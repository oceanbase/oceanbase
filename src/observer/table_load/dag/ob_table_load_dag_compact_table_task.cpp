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

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/dag/ob_table_load_dag_compact_table_task.h"
#include "observer/table_load/ob_table_load_schema.h"
#include "observer/table_load/ob_table_load_store_ctx.h"
#include "observer/table_load/ob_table_load_store_table_ctx.h"
#include "observer/table_load/plan/ob_table_load_merge_op.h"
#include "storage/direct_load/ob_direct_load_multiple_heap_table_compactor.h"
#include "storage/direct_load/ob_direct_load_multiple_sstable_compactor.h"
#include "storage/direct_load/ob_direct_load_range_splitter.h"
#include "storage/direct_load/ob_direct_load_table_store.h"

namespace oceanbase
{
namespace observer
{
using namespace share;
using namespace storage;

/**
 * ObTableLoadDagCompactTableOpTask
 */

ObTableLoadDagCompactTableOpTask::ObTableLoadDagCompactTableOpTask(ObTableLoadDag *dag,
                                                                   ObTableLoadOp *op)
  : ObITask(TASK_TYPE_DIRECT_LOAD_COMPACT_TABLE_OP), ObTableLoadDagOpTaskBase(dag, op)
{
}

int ObTableLoadDagCompactTableOpTask::process()
{
  int ret = OB_SUCCESS;
  ObTableLoadCompactDataOp *op = static_cast<ObTableLoadCompactDataOp *>(op_);
  FLOG_INFO("[DIRECT_LOAD_OP] compact table op start", KP(op));
  op->start_time_ = ObTimeUtil::current_time();

  ObDirectLoadTableStore &table_store = op->op_ctx_->table_store_;
  if (table_store.empty()) {
    op->end_time_ = op->start_time_;
    FLOG_INFO("[DIRECT_LOAD_OP] compact table op skip", KP(op));
  } else if (table_store.get_table_data_desc().row_flag_.uncontain_hidden_pk_) {
    // compact_task -> op_finish_task -> [next_op_task]
    ObTableLoadCompactHeapTableTask *compact_task = nullptr;
    ObTableLoadDagCompactTableOpFinishTask *op_finish_task = nullptr;
    if (OB_ISNULL(op->heap_table_compactor_ =
                    OB_NEWx(ObTableLoadDagParallelHeapTableCompactor, &op->op_ctx_->allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObTableLoadDagParallelHeapTableCompactor", KR(ret));
    } else if (OB_FAIL(op->heap_table_compactor_->init(dag_->store_ctx_, op->op_ctx_))) {
      LOG_WARN("fail to init heap table compactor", KR(ret));
    }
    // 创建task
    else if (OB_FAIL(dag_->alloc_task(compact_task, dag_))) {
      LOG_WARN("failed to alloc task", KR(ret));
    } else if (OB_FAIL(dag_->alloc_task(op_finish_task, dag_, op_))) {
      LOG_WARN("failed to alloc task", KR(ret));
    } else if (OB_FAIL(compact_task->init(op))) {
      LOG_WARN("fail to init task", KR(ret));
    }
    // 建立依赖关系
    else if (OB_FAIL(compact_task->add_child(*op_finish_task))) {
      LOG_WARN("fail to add child", KR(ret));
    } else if (OB_FAIL(op_finish_task->deep_copy_children(get_child_nodes()))) {
      LOG_WARN("fail to deep copy children", KR(ret));
    }
    // 加入dag
    else if (OB_FAIL(dag_->add_task(*compact_task))) {
      LOG_WARN("fail to add task", KR(ret));
    } else if (OB_FAIL(dag_->add_task(*op_finish_task))) {
      LOG_WARN("fail to add task", KR(ret));
    }
  } else {
    // compact_task -> op_finish_task -> [next_op_task]
    ObTableLoadCompactSSTableTask *compact_task = nullptr;
    ObTableLoadDagCompactTableOpFinishTask *op_finish_task = nullptr;
    if (OB_ISNULL(op->sstable_compactor_ =
                    OB_NEWx(ObTableLoadDagParallelSSTableCompactor, &op->op_ctx_->allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObTableLoadDagParallelSSTableCompactor", KR(ret));
    } else if (OB_FAIL(op->sstable_compactor_->init(dag_->store_ctx_, op->op_ctx_))) {
      LOG_WARN("fail to init heap table compactor", KR(ret));
    } else if (OB_FAIL(op->sstable_compactor_->prepare_compact())) {
      LOG_WARN("fail to prepare compact", KR(ret));
    }
    // 创建task
    else if (OB_FAIL(dag_->alloc_task(compact_task, dag_))) {
      LOG_WARN("failed to alloc task", KR(ret));
    } else if (OB_FAIL(dag_->alloc_task(op_finish_task, dag_, op_))) {
      LOG_WARN("failed to alloc task", KR(ret));
    } else if (OB_FAIL(compact_task->init(op))) {
      LOG_WARN("fail to init task", KR(ret));
    }
    // 建立依赖关系
    else if (OB_FAIL(compact_task->add_child(*op_finish_task))) {
      LOG_WARN("fail to add child", KR(ret));
    } else if (OB_FAIL(op_finish_task->deep_copy_children(get_child_nodes()))) {
      LOG_WARN("fail to deep copy children", KR(ret));
    }
    // 加入dag
    else if (OB_FAIL(dag_->add_task(*compact_task))) {
      LOG_WARN("fail to add task", KR(ret));
    } else if (OB_FAIL(dag_->add_task(*op_finish_task))) {
      LOG_WARN("fail to add task", KR(ret));
    }
  }
  return ret;
}

/**
 * ObTableLoadDagCompactTableOpFinishTask
 */

ObTableLoadDagCompactTableOpFinishTask::ObTableLoadDagCompactTableOpFinishTask(ObTableLoadDag *dag,
                                                                               ObTableLoadOp *op)
  : ObITask(TASK_TYPE_DIRECT_LOAD_COMPACT_TABLE_OP_FINISH), ObTableLoadDagOpTaskBase(dag, op)
{
}

int ObTableLoadDagCompactTableOpFinishTask::process()
{
  int ret = OB_SUCCESS;
  ObTableLoadCompactDataOp *op = static_cast<ObTableLoadCompactDataOp *>(op_);
  if (nullptr != op->sstable_compactor_) {
    if (OB_FAIL(op->sstable_compactor_->close())) {
      LOG_WARN("fail to close sstable compactor", KR(ret));
    }
  } else if (nullptr != op->heap_table_compactor_) {
    if (OB_FAIL(op->heap_table_compactor_->close())) {
      LOG_WARN("fail to close heap table compactor", KR(ret));
    }
  }

  op->end_time_ = ObTimeUtil::current_time();
  FLOG_INFO("[DIRECT_LOAD_OP] compact table op finish", KP(op), "time_cost",
            op->end_time_ - op->start_time_);

  reset_op(op);
  return ret;
}

void ObTableLoadDagCompactTableOpFinishTask::reset_op(ObTableLoadCompactDataOp *op)
{
  if (OB_NOT_NULL(op)) {
    OB_DELETEx(ObTableLoadDagParallelSSTableCompactor, &op->op_ctx_->allocator_, op->sstable_compactor_);
    OB_DELETEx(ObTableLoadDagParallelHeapTableCompactor, &op->op_ctx_->allocator_, op->heap_table_compactor_);
  }
}

/**
 * ObTableLoadSSTableSplitRangeTask
 */

ObTableLoadSSTableSplitRangeTask::ObTableLoadSSTableSplitRangeTask(ObTableLoadDag *dag)
  : ObITask(TASK_TYPE_DIRECT_LOAD_COMPACT_SSTABLE_SPLIT_RANGE),
    ObTableLoadDagTaskBase(dag),
    op_ctx_(nullptr),
    compact_ctx_(nullptr),
    is_inited_(false)
{
}

int ObTableLoadSSTableSplitRangeTask::init(ObTableLoadTableOpCtx *op_ctx,
                                           ObTableLoadDagParallelCompactTabletCtx *compact_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_UNLIKELY(op_ctx == nullptr || compact_ctx == nullptr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(op_ctx), KP(compact_ctx));
  } else {
    op_ctx_ = op_ctx;
    compact_ctx_ = compact_ctx;
    is_inited_ = true;
  }
  return ret;
}

int ObTableLoadSSTableSplitRangeTask::process()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    const int64_t merge_count_per_round = store_ctx_->merge_count_per_round_;
    const int64_t merge_sstable_count =
      MIN(compact_ctx_->sstables_.count() - merge_count_per_round + 1, merge_count_per_round);
    ObDirectLoadTableHandleArray sstable_array;
    // sort sstable
    ObTableLoadDagSSTableCompare compare;
    lib::ob_sort(compact_ctx_->sstables_.begin(), compact_ctx_->sstables_.end(), compare);
    // collect merged sstables
    for (int64_t i = 0; OB_SUCC(ret) && i < merge_sstable_count; ++i) {
      ObDirectLoadTableHandle sstable;
      if (OB_FAIL(compact_ctx_->sstables_.get_table(i, sstable))) {
        LOG_WARN("fail to get table", KR(ret), K(i));
      } else if (OB_FAIL(sstable_array.add(sstable))) {
        LOG_WARN("fail to push back sstable", KR(ret));
      }
    }
    // split range
    if (OB_SUCC(ret)) {
      ObDirectLoadMultipleSSTableRangeSplitter range_splitter;
      if (OB_FAIL(range_splitter.init(sstable_array, op_ctx_->table_store_.get_table_data_desc(),
                                      &op_ctx_->store_table_ctx_->schema_->datum_utils_))) {
        LOG_WARN("fail to init range splitter", KR(ret));
      } else if (OB_FAIL(range_splitter.split_range(compact_ctx_->ranges_, store_ctx_->thread_cnt_,
                                                    compact_ctx_->range_allocator_))) {
        LOG_WARN("fail to split range", KR(ret));
      } else if (OB_FAIL(compact_ctx_->set_parallel_merge_param(merge_sstable_count,
                                                                compact_ctx_->ranges_.count()))) {
        LOG_WARN("fail to set parallel merge param", KR(ret));
      }
    }
  }
  return ret;
}

ObTableLoadSSTableMergeRangeTask::ObTableLoadSSTableMergeRangeTask(ObTableLoadDag *dag)
  : ObITask(TASK_TYPE_DIRECT_LOAD_COMPACT_SSTABLE_MERGE_RANGE),
    ObTableLoadDagTaskBase(dag),
    op_ctx_(nullptr),
    compact_ctx_(nullptr),
    total_thread_cnt_(0),
    curr_thread_idx_(0),
    is_inited_(false)
{
}

int ObTableLoadSSTableMergeRangeTask::init(ObTableLoadTableOpCtx *op_ctx,
                                           ObTableLoadDagParallelCompactTabletCtx *compact_ctx,
                                           const int64_t total_thread_cnt,
                                           const int64_t curr_thread_idx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_UNLIKELY(op_ctx == nullptr || compact_ctx == nullptr || total_thread_cnt <= 0 ||
                         curr_thread_idx >= total_thread_cnt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(op_ctx), KP(compact_ctx), K(total_thread_cnt),
             K(curr_thread_idx));
  } else {
    op_ctx_ = op_ctx;
    compact_ctx_ = compact_ctx;
    total_thread_cnt_ = total_thread_cnt;
    curr_thread_idx_ = curr_thread_idx;
    is_inited_ = true;
  }
  return ret;
}

int ObTableLoadSSTableMergeRangeTask::process()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    const ObDirectLoadMultipleDatumRow *datum_row = nullptr;
    ObDirectLoadTableHandleArray table_array;
    ObDirectLoadMultipleSSTable *sstable = nullptr;
    ObDirectLoadTableHandle sstable_handle;
    if (OB_FAIL(init_scan_merge())) {
      LOG_WARN("fail to init scan merge", KR(ret));
    } else if (OB_FAIL(init_sstable_builder())) {
      LOG_WARN("fail to init sstable builder", KR(ret));
    }
    while (OB_SUCC(ret)) {
      if (OB_FAIL(dag_->check_status())) {
        LOG_WARN("fail to check status", KR(ret));
      } else if (OB_FAIL(scan_merge_.get_next_row(datum_row))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to get next row", KR(ret));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else if (OB_FAIL(sstable_builder_.append_row(*datum_row))) {
        LOG_WARN("fail to append row", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(sstable_builder_.close())) {
        LOG_WARN("fail to close sstable builder", KR(ret));
      } else {
        ObMutexGuard guard(compact_ctx_->mutex_);
        if (OB_FAIL(sstable_builder_.get_tables(table_array, store_ctx_->table_mgr_))) {
          LOG_WARN("fail to get tables", KR(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_UNLIKELY(1 != table_array.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected table array count", KR(ret), K(table_array));
        } else if (OB_FAIL(table_array.get_table(0, sstable_handle))) {
          LOG_WARN("fail to get table", KR(ret));
        } else if (compact_ctx_->range_sstables_.at(curr_thread_idx_).is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected range idx", KR(ret), K(curr_thread_idx_));
        } else {
          compact_ctx_->range_sstables_.at(curr_thread_idx_) = sstable_handle;
          compact_ctx_->range_sstable_count_++;
        }
      }
    }
  }
  return ret;
}

int ObTableLoadSSTableMergeRangeTask::generate_next_task(ObITask *&next_task)
{
  int ret = OB_SUCCESS;
  next_task = nullptr;
  if (curr_thread_idx_ == total_thread_cnt_ - 1) {
    ret = OB_ITER_END;
  } else {
    ObTableLoadSSTableMergeRangeTask *task = nullptr;
    if (OB_FAIL(dag_->alloc_task(task, dag_))) {
      LOG_WARN("failed to alloc task", KR(ret));
    } else if (OB_ISNULL(task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("task is null", KR(ret));
    } else if (OB_FAIL(
                 task->init(op_ctx_, compact_ctx_, total_thread_cnt_, curr_thread_idx_ + 1))) {
      LOG_WARN("fail to init task", KR(ret));
    } else {
      next_task = task;
    }
  }
  return ret;
}

int ObTableLoadSSTableMergeRangeTask::init_scan_merge()
{
  int ret = OB_SUCCESS;
  ObDirectLoadMultipleSSTableScanMergeParam scan_merge_param;
  scan_merge_param.table_data_desc_ = op_ctx_->table_store_.get_table_data_desc();
  scan_merge_param.datum_utils_ = &op_ctx_->store_table_ctx_->schema_->datum_utils_;
  scan_merge_param.dml_row_handler_ = op_ctx_->dml_row_handler_;
  for (int64_t i = 0; OB_SUCC(ret) && i < compact_ctx_->merge_sstable_count_; ++i) {
    ObDirectLoadTableHandle sstable;
    if (OB_FAIL(compact_ctx_->sstables_.get_table(i, sstable))) {
      LOG_WARN("fail to get table", KR(ret), K(i));
    } else if (OB_FAIL(sstable_array_.add(sstable))) {
      LOG_WARN("fail to push back sstable", KR(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(scan_merge_.init(scan_merge_param, sstable_array_,
                                 compact_ctx_->ranges_.at(curr_thread_idx_)))) {
      LOG_WARN("fail to init sstable scan merge", KR(ret));
    }
  }
  return ret;
}

int ObTableLoadSSTableMergeRangeTask::init_sstable_builder()
{
  int ret = OB_SUCCESS;
  ObDirectLoadMultipleSSTableBuildParam build_param;
  build_param.tablet_id_ = compact_ctx_->tablet_id_;
  build_param.table_data_desc_ = op_ctx_->table_store_.get_table_data_desc();
  build_param.datum_utils_ = &op_ctx_->store_table_ctx_->schema_->datum_utils_;
  build_param.file_mgr_ = store_ctx_->tmp_file_mgr_;
  build_param.extra_buf_ = reinterpret_cast<char *>(1); // unuse, delete in future
  build_param.extra_buf_size_ = 4096;
  if (OB_FAIL(sstable_builder_.init(build_param))) {
    LOG_WARN("fail to init sstable builder", KR(ret));
  }
  return ret;
}

ObTableLoadSSTableCompactTask::ObTableLoadSSTableCompactTask(ObTableLoadDag *dag)
  : ObITask(TASK_TYPE_DIRECT_LOAD_COMPACT_SSTABLE_COMPACT),
    ObTableLoadDagTaskBase(dag),
    op_ctx_(nullptr),
    compact_ctx_(nullptr),
    is_inited_(false)
{
}

int ObTableLoadSSTableCompactTask::init(ObTableLoadTableOpCtx *op_ctx,
                                        ObTableLoadDagParallelCompactTabletCtx *compact_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_UNLIKELY(op_ctx == nullptr || compact_ctx == nullptr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(op_ctx), KP(compact_ctx));
  } else {
    op_ctx_ = op_ctx;
    compact_ctx_ = compact_ctx;
    is_inited_ = true;
  }
  return ret;
}

int ObTableLoadSSTableCompactTask::process()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    ObDirectLoadTableHandle sstable_handle;
    if (OB_FAIL(compact_sstable(sstable_handle))) {
      LOG_WARN("fail to compact sstable", KR(ret));
    } else if (OB_FAIL(apply_merged_sstable(sstable_handle))) {
      LOG_WARN("fail to apply merged sstable", KR(ret));
    }
  }
  return ret;
}

int ObTableLoadSSTableCompactTask::compact_sstable(ObDirectLoadTableHandle &result_sstable)
{
  int ret = OB_SUCCESS;
  ObDirectLoadMultipleSSTableCompactor compactor;
  ObDirectLoadMultipleSSTableCompactParam compact_param;
  compact_param.tablet_id_ = compact_ctx_->tablet_id_;
  compact_param.table_data_desc_ = op_ctx_->table_store_.get_table_data_desc();
  compact_param.datum_utils_ = &op_ctx_->store_table_ctx_->schema_->datum_utils_;
  if (OB_FAIL(compactor.init(compact_param))) {
    LOG_WARN("fail to init sstable compactor", KR(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < compact_ctx_->range_sstables_.count(); ++i) {
    ObDirectLoadTableHandle sstable_handle = compact_ctx_->range_sstables_.at(i);
    if (OB_FAIL(compactor.add_table(sstable_handle))) {
      LOG_WARN("fail to add table", KR(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(compactor.compact())) {
      LOG_WARN("fail to do compact", KR(ret));
    } else if (OB_FAIL(compactor.get_table(result_sstable, store_ctx_->table_mgr_))) {
      LOG_WARN("fail to get table", KR(ret));
    }
  }
  return ret;
}

int ObTableLoadSSTableCompactTask::apply_merged_sstable(
  const ObDirectLoadTableHandle &merged_sstable)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!merged_sstable.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(merged_sstable));
  } else if (OB_UNLIKELY(compact_ctx_->merge_sstable_count_ <= 0 ||
                         compact_ctx_->range_count_ <= 0 ||
                         compact_ctx_->range_count_ != compact_ctx_->range_sstable_count_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected tablet ctx", KR(ret), KPC(this), K(compact_ctx_->merge_sstable_count_),
             K(compact_ctx_->range_count_), K(compact_ctx_->range_sstable_count_));
  } else {
    ObDirectLoadTableHandleArray result_sstables;
    LOG_INFO("parallel merge apply merged", K(compact_ctx_->merge_sstable_count_),
             K(compact_ctx_->sstables_.count()));
    if (OB_FAIL(result_sstables.add(merged_sstable))) {
      LOG_WARN("fail to push back sstable", KR(ret));
    }
    // old_sstables_里的table_handle都被reset了, 这里直接清空数组
    compact_ctx_->old_sstables_.reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < compact_ctx_->sstables_.count(); ++i) {
      ObDirectLoadTableHandle &sstable_handle = compact_ctx_->sstables_.at(i);
      if (i < compact_ctx_->merge_sstable_count_) {
        if (OB_FAIL(compact_ctx_->old_sstables_.push_back(sstable_handle))) {
          LOG_WARN("fail to push back", KR(ret));
        }
      } else {
        if (OB_FAIL(result_sstables.add(sstable_handle))) {
          LOG_WARN("fail to push back sstable", KR(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(compact_ctx_->sstables_.assign(result_sstables))) {
        LOG_WARN("fail to assign sstables", KR(ret));
      } else {
        // clear merge ctx
        compact_ctx_->merge_sstable_count_ = 0;
        compact_ctx_->range_count_ = 0;
        compact_ctx_->range_sstable_count_ = 0;
        compact_ctx_->range_sstables_.reset();
        compact_ctx_->ranges_.reset();
        compact_ctx_->range_allocator_.reset();
      }
    }
  }
  return ret;
}

/**
 * ObTableLoadCompactSSTableClearTask
 */

ObTableLoadCompactSSTableClearTask::ObTableLoadCompactSSTableClearTask(
  ObTableLoadDag *dag, ObTableLoadDagParallelCompactTabletCtx *compact_ctx,
  const int64_t thread_idx)
  : ObITask(TASK_TYPE_DIRECT_LOAD_COMPACT_SSTABLE_CLEAR),
    ObTableLoadDagTaskBase(dag),
    compact_ctx_(compact_ctx),
    thread_idx_(thread_idx)
{
}

int ObTableLoadCompactSSTableClearTask::generate_next_task(ObITask *&next_task)
{
  int ret = OB_SUCCESS;
  next_task = nullptr;
  const int64_t next_thread_idx = thread_idx_ + 1;
  if (next_thread_idx >= store_ctx_->thread_cnt_) {
    ret = OB_ITER_END;
  } else {
    ObTableLoadCompactSSTableClearTask *task = nullptr;
    if (OB_FAIL(dag_->alloc_task(task, dag_, compact_ctx_, next_thread_idx))) {
      LOG_WARN("failed to alloc task", KR(ret));
    } else {
      next_task = task;
    }
  }
  return ret;
}

int ObTableLoadCompactSSTableClearTask::process()
{
  int ret = OB_SUCCESS;
  const int64_t table_cnt_pre_thread =
    compact_ctx_->old_sstables_.count() / store_ctx_->thread_cnt_;
  const int64_t remain_table_cnt = compact_ctx_->old_sstables_.count() % store_ctx_->thread_cnt_;
  const int64_t start_idx = thread_idx_ * table_cnt_pre_thread + MIN(thread_idx_, remain_table_cnt);
  const int64_t table_cnt = table_cnt_pre_thread + (thread_idx_ < remain_table_cnt ? 1 : 0);
  for (int64_t i = 0; i < table_cnt; ++i) {
    compact_ctx_->old_sstables_.at(start_idx + i).reset();
  }
  return ret;
}

/**
 * ObTableLoadCompactSSTableTask
 */

ObTableLoadCompactSSTableTask::ObTableLoadCompactSSTableTask(ObTableLoadDag *dag)
  : ObITask(TASK_TYPE_DIRECT_LOAD_COMPACT_SSTABLE),
    ObTableLoadDagTaskBase(dag),
    op_(nullptr),
    is_inited_(false)
{
}

int ObTableLoadCompactSSTableTask::init(ObTableLoadCompactDataOp *op)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_UNLIKELY(op == nullptr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(op));
  } else {
    op_ = op;
    is_inited_ = true;
  }
  return ret;
}

int ObTableLoadCompactSSTableTask::post_generate_next_task(ObITask *&next_task)
{
  int ret = OB_SUCCESS;
  next_task = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    ObTableLoadCompactSSTableTask *sstable_task = nullptr;
    FOREACH_X(it, op_->sstable_compactor_->get_tablet_ctx_map(), OB_SUCC(ret))
    {
      ObTableLoadDagParallelCompactTabletCtx *tablet_ctx = it->second;
      if (tablet_ctx->sstables_.count() > store_ctx_->merge_count_per_round_) {
        ObTableLoadSSTableSplitRangeTask *split_range_task = nullptr;
        ObTableLoadSSTableMergeRangeTask *merge_range_task = nullptr;
        ObTableLoadSSTableCompactTask *compact_task = nullptr;
        ObTableLoadCompactSSTableClearTask *clear_task = nullptr;
        if (OB_FAIL(dag_->alloc_task(split_range_task, dag_))) {
          LOG_WARN("failed to alloc task", KR(ret));
        } else if (OB_FAIL(dag_->alloc_task(merge_range_task, dag_))) {
          LOG_WARN("failed to alloc task", KR(ret));
        } else if (OB_FAIL(dag_->alloc_task(compact_task, dag_))) {
          LOG_WARN("failed to alloc task", KR(ret));
        } else if (OB_FAIL(dag_->alloc_task(clear_task, dag_, tablet_ctx, 0/*thread_idx*/))) {
          LOG_WARN("failed to alloc task", KR(ret));
        } else if (sstable_task == nullptr && OB_FAIL(dag_->alloc_task(sstable_task, dag_))) {
          LOG_WARN("failed to alloc task", KR(ret));
        } else if (OB_FAIL(split_range_task->init(op_->op_ctx_, tablet_ctx))) {
          LOG_WARN("fail to init split range task", KR(ret));
        } else if (OB_FAIL(merge_range_task->init(op_->op_ctx_, tablet_ctx, store_ctx_->thread_cnt_,
                                                  0 /*curr_thread_idx*/))) {
          LOG_WARN("fail to init merge range task", KR(ret));
        } else if (OB_FAIL(compact_task->init(op_->op_ctx_, tablet_ctx))) {
          LOG_WARN("fail to init compact task", KR(ret));
        }
        // 建立依赖关系: split_range_task -> merge_range_task -> compact_task -> clear_task -> sstable_task
        else if (OB_FAIL(split_range_task->add_child(*merge_range_task))) {
          LOG_WARN("fail to add child", KR(ret));
        } else if (OB_FAIL(merge_range_task->add_child(*compact_task))) {
          LOG_WARN("fail to add child", KR(ret));
        } else if (OB_FAIL(compact_task->add_child(*clear_task))) {
          LOG_WARN("fail to add child", KR(ret));
        } else if (OB_FAIL(clear_task->add_child(*sstable_task))) {
          LOG_WARN("fail to add child", KR(ret));
        }
        // 添加task
        else if (OB_FAIL(dag_->add_task(*split_range_task))) {
          LOG_WARN("fail to add task", KR(ret));
        } else if (OB_FAIL(dag_->add_task(*merge_range_task))) {
          LOG_WARN("fail to add task", KR(ret));
        } else if (OB_FAIL(dag_->add_task(*compact_task))) {
          LOG_WARN("fail to add task", KR(ret));
        } else if (OB_FAIL(dag_->add_task(*clear_task))) {
          LOG_WARN("fail to add task", KR(ret));
        }
      }
    }
    if (OB_SUCC(ret) && sstable_task) {
      if (OB_FAIL(sstable_task->init(op_))) {
        LOG_WARN("fail to init compact task", KR(ret));
      } else {
        next_task = sstable_task;
      }
    }
  }
  if (OB_SUCC(ret) && next_task == nullptr) {
    ret = OB_ITER_END;
  }
  return ret;
}

ObTableLoadHeapTableCompactTask::ObTableLoadHeapTableCompactTask(ObTableLoadDag *dag)
  : ObITask(TASK_TYPE_DIRECT_LOAD_COMPACT_HEAP_TABLE_COMPACT),
    ObTableLoadDagTaskBase(dag),
    op_(nullptr),
    table_cnt_(-1),
    total_thread_cnt_(-1),
    curr_thread_idx_(-1),
    is_inited_(false)
{
}

int ObTableLoadHeapTableCompactTask::init(ObTableLoadCompactDataOp *op,
                                          const int64_t table_cnt, const int64_t total_thread_cnt,
                                          const int64_t curr_thread_idx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_UNLIKELY(op == nullptr || table_cnt < 0 ||
                         total_thread_cnt <= 0 || curr_thread_idx >= total_thread_cnt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(op), K(table_cnt), K(curr_thread_idx),
             K(total_thread_cnt));
  } else {
    op_ = op;
    table_cnt_ = table_cnt;
    total_thread_cnt_ = total_thread_cnt;
    curr_thread_idx_ = curr_thread_idx;
    is_inited_ = true;
  }
  return ret;
}

int ObTableLoadHeapTableCompactTask::process()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    int ret = OB_SUCCESS;
    ObDirectLoadMultipleHeapTableCompactor compactor;
    ObDirectLoadMultipleHeapTableCompactParam compact_param;
    compact_param.table_data_desc_ = op_->op_ctx_->table_store_.get_table_data_desc();
    compact_param.file_mgr_ = store_ctx_->tmp_file_mgr_;
    if (OB_FAIL(compact_param.file_mgr_->alloc_dir(compact_param.index_dir_id_))) {
      LOG_WARN("fail to alloc dir", KR(ret));
    } else if (OB_FAIL(compactor.init(compact_param))) {
      LOG_WARN("fail to init compactor", KR(ret));
    } else {
      int64_t start_idx = curr_thread_idx_ * table_cnt_;
      int64_t end_idx = start_idx + table_cnt_;
      while (OB_SUCC(ret) && start_idx < end_idx) {
        ObDirectLoadTableHandle table_handle;
        if (OB_FAIL(op_->heap_table_compactor_->tables_handle_.get_table(start_idx, table_handle))) {
          LOG_WARN("fail to get table", KR(ret));
        } else if (OB_FAIL(compactor.add_table(table_handle))) {
          LOG_WARN("fail to add table", KR(ret));
        } else {
          start_idx++;
        }
      }
      if (OB_SUCC(ret)) {
        ObDirectLoadTableHandle table_handle;
        if (OB_FAIL(compactor.compact())) {
          LOG_WARN("fail to do compact", KR(ret));
        } else {
          ObDirectLoadTableHandle table_handle;
          if (OB_FAIL(compactor.get_table(table_handle, store_ctx_->table_mgr_))) {
            LOG_WARN("fail to get table", KR(ret));
          } else {
            ObMutexGuard guard(op_->heap_table_compactor_->mutex_);
            if (OB_FAIL(op_->heap_table_compactor_->result_tables_handle_.add(table_handle))) {
              LOG_WARN("fail to add result sstable", KR(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObTableLoadHeapTableCompactTask::generate_next_task(ObITask *&next_task)
{
  int ret = OB_SUCCESS;
  next_task = nullptr;
  if (curr_thread_idx_ == total_thread_cnt_ - 1) {
    ret = OB_ITER_END;
  } else {
    ObTableLoadHeapTableCompactTask *task = nullptr;
    if (OB_FAIL(dag_->alloc_task(task, dag_))) {
      LOG_WARN("failed to alloc task", KR(ret));
    } else if (OB_ISNULL(task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("task is null", KR(ret));
    } else if (OB_FAIL(task->init(op_, table_cnt_, total_thread_cnt_,
                                  curr_thread_idx_ + 1))) {
      LOG_WARN("fail to init task", KR(ret));
    } else {
      next_task = task;
    }
  }
  return ret;
}

ObTableLoadCompactHeapTableTask::ObTableLoadCompactHeapTableTask(ObTableLoadDag *dag)
  : ObITask(TASK_TYPE_DIRECT_LOAD_COMPACT_HEAP_TABLE),
    ObTableLoadDagTaskBase(dag),
    op_(nullptr),
    is_inited_(false)
{
}

int ObTableLoadCompactHeapTableTask::init(ObTableLoadCompactDataOp *op)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_UNLIKELY(op == nullptr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(op));
  } else {
    op_ = op;
    is_inited_ = true;
  }
  return ret;
}

int ObTableLoadCompactHeapTableTask::post_generate_next_task(ObITask *&next_task)
{
  int ret = OB_SUCCESS;
  next_task = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    ObDirectLoadTableStore *table_store = &op_->op_ctx_->table_store_;
    if (op_->heap_table_compactor_->result_tables_handle_.empty()) {
      FOREACH_X(it, *table_store, OB_SUCC(ret))
      {
        if (OB_FAIL(op_->heap_table_compactor_->tables_handle_.assign(*it->second))) {
          LOG_WARN("failed to assign", KR(ret));
        }
      }
      if (OB_SUCC(ret)) {
        // 清空table_store, 以便在compact过程中能释放磁盘空间
        table_store->clear();
      }
    } else {
      op_->heap_table_compactor_->tables_handle_.reset();
      if (OB_FAIL(op_->heap_table_compactor_->tables_handle_.assign(op_->heap_table_compactor_->result_tables_handle_))) {
        LOG_WARN("failed to assign", KR(ret));
      } else {
        op_->heap_table_compactor_->result_tables_handle_.reset();
      }
    }
    if (OB_SUCC(ret)) {
      if (op_->heap_table_compactor_->tables_handle_.count() > store_ctx_->merge_count_per_round_) {
        ObTableLoadDagHeapTableCompare compare;
        lib::ob_sort(op_->heap_table_compactor_->tables_handle_.begin(), op_->heap_table_compactor_->tables_handle_.end(), compare);
        int task_cnt = store_ctx_->thread_cnt_;
        int table_cnt =
          min(op_->heap_table_compactor_->tables_handle_.count() / task_cnt, store_ctx_->merge_count_per_round_);
        for (int i = table_cnt * task_cnt; OB_SUCC(ret) && i < op_->heap_table_compactor_->tables_handle_.count();
             i++) {
          if (OB_FAIL(op_->heap_table_compactor_->result_tables_handle_.add(op_->heap_table_compactor_->tables_handle_.at(i)))) {
            LOG_WARN("failed to add", KR(ret));
          }
        }
        if (OB_SUCC(ret)) {
          ObTableLoadHeapTableCompactTask *compact_task = nullptr;
          ObTableLoadCompactHeapTableTask *heap_table_task = nullptr;
          if (OB_FAIL(dag_->alloc_task(compact_task, dag_))) {
            LOG_WARN("failed to alloc task", KR(ret));
          } else if (OB_FAIL(dag_->alloc_task(heap_table_task, dag_))) {
            LOG_WARN("failed to alloc task", KR(ret));
          } else if (OB_FAIL(compact_task->init(op_, table_cnt, task_cnt,
                                                0 /*curr_thread_idx_*/))) {
            LOG_WARN("fail to init compact task", KR(ret));
          } else if (OB_FAIL(heap_table_task->init(op_))) {
            LOG_WARN("fail to init heap table task", KR(ret));
          } else if (OB_FAIL(compact_task->add_child(*heap_table_task))) {
            LOG_WARN("fail to add child", KR(ret));
          } else if (OB_FAIL(dag_->add_task(*compact_task))) {
            LOG_WARN("fail to add task", KR(ret));
          } else {
            next_task = heap_table_task;
          }
        }
      }
    }
  }

  if (OB_SUCC(ret) && next_task == nullptr) {
    ret = OB_ITER_END;
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
