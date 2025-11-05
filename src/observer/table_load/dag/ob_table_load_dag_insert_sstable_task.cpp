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

#include "observer/table_load/dag/ob_table_load_dag_insert_sstable_task.h"
#include "observer/table_load/dag/ob_table_load_dag.h"
#include "observer/table_load/dag/ob_table_load_dag_parallel_merger.h"
#include "observer/table_load/ob_table_load_store_ctx.h"
#include "observer/table_load/plan/ob_table_load_merge_op.h"
#include "storage/ddl/ob_cg_macro_block_writer.h"
#include "storage/ddl/ob_tablet_slice_row_iterator.h"
#include "storage/direct_load/ob_direct_load_i_merge_task.h"
#include "storage/direct_load/ob_direct_load_insert_table_ctx.h"

namespace oceanbase
{
namespace observer
{
using namespace share;
using namespace storage;
using namespace common;

/**
 * ObTableLoadDagInsertSSTableOpTask
 */

ObTableLoadDagInsertSSTableOpTask::ObTableLoadDagInsertSSTableOpTask(ObTableLoadDag *dag,
                                                                     ObTableLoadOp *op)
  : ObITask(TASK_TYPE_DIRECT_LOAD_INSERT_SSTABLE_OP), ObTableLoadDagOpTaskBase(dag, op)
{
}

int ObTableLoadDagInsertSSTableOpTask::process()
{
  int ret = OB_SUCCESS;
  ObTableLoadInsertSSTableOp *op = static_cast<ObTableLoadInsertSSTableOp *>(op_);
  FLOG_INFO("[DIRECT_LOAD_OP] insert sstable op start", KP(op));
  op->start_time_ = ObTimeUtil::current_time();

  // insert_task -> clear_task -> op_finish_task -> [next_op_task]
  ObTableLoadInsertSSTableTask *insert_task = nullptr;
  ObTableLoadDagInsertSSTableClearTask *clear_task = nullptr;
  ObTableLoadDagInsertSSTableOpFinishTask *op_finish_task = nullptr;
  if (OB_ISNULL(op->parallel_merger_ =
                  OB_NEWx(ObTableLoadDagParallelMerger, &op->op_ctx_->allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to new ObTableLoadDagParallelMerger", KR(ret));
  } else if (OB_FAIL(op->parallel_merger_->init_merge_task(store_ctx_, op->op_ctx_))) {
    LOG_WARN("fail to init merge task", KR(ret));
  }
  // alloc task
  else if (OB_FAIL(dag_->alloc_task(insert_task, dag_, op->parallel_merger_))) {
    LOG_WARN("failed to alloc task", KR(ret));
  } else if (OB_FAIL(dag_->alloc_task(clear_task, dag_, op->parallel_merger_, 0 /*thread_idx*/))) {
    LOG_WARN("failed to alloc task", KR(ret));
  } else if (OB_FAIL(dag_->alloc_task(op_finish_task, dag_, op_))) {
    LOG_WARN("failed to alloc task", KR(ret));
  }
  // 依赖关系
  else if (OB_FAIL(insert_task->add_child(*clear_task))) {
    LOG_WARN("failed to add child", KR(ret));
  } else if (OB_FAIL(clear_task->add_child(*op_finish_task))) {
    LOG_WARN("failed to add child", KR(ret));
  } else if (OB_FAIL(op_finish_task->deep_copy_children(get_child_nodes()))) {
    LOG_WARN("fail to deep copy children", KR(ret));
  }
  // 添加task
  else if (OB_FAIL(dag_->add_task(*insert_task))) {
    LOG_WARN("failed to add task", KR(ret));
  } else if (OB_FAIL(dag_->add_task(*clear_task))) {
    LOG_WARN("failed to add task", KR(ret));
  } else if (OB_FAIL(dag_->add_task(*op_finish_task))) {
    LOG_WARN("fail to add task", KR(ret));
  }
  return ret;
}

/**
 * ObTableLoadDagInsertSSTableOpFinishTask
 */

ObTableLoadDagInsertSSTableOpFinishTask::ObTableLoadDagInsertSSTableOpFinishTask(
  ObTableLoadDag *dag, ObTableLoadOp *op)
  : ObITask(TASK_TYPE_DIRECT_LOAD_INSERT_SSTABLE_OP_FINISH), ObTableLoadDagOpTaskBase(dag, op)
{
}

int ObTableLoadDagInsertSSTableOpFinishTask::process()
{
  int ret = OB_SUCCESS;
  ObTableLoadInsertSSTableOp *op = static_cast<ObTableLoadInsertSSTableOp *>(op_);

  op->end_time_ = ObTimeUtil::current_time();
  FLOG_INFO("[DIRECT_LOAD_OP] insert sstable op finish", KP(op), "time_cost",
            op->end_time_ - op->start_time_);

  reset_op(op);
  return ret;
}

void ObTableLoadDagInsertSSTableOpFinishTask::reset_op(ObTableLoadInsertSSTableOp *op)
{
  if (OB_NOT_NULL(op)) {
    OB_DELETEx(ObTableLoadDagParallelMerger, &op->op_ctx_->allocator_, op->parallel_merger_);
  }
}

/**
 * ObTableLoadDagInsertSSTableTaskBase
 */

int ObTableLoadDagInsertSSTableTaskBase::handle_merge_task_finish(
  ObITask *parent_task, ObDirectLoadIMergeTask *merge_task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == parent_task || nullptr == merge_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(parent_task), KP(merge_task));
  } else {
    ObDirectLoadTabletMergeCtx *tablet_merge_ctx = merge_task->get_merge_ctx();
    bool is_ready = false;
    if (OB_FAIL(tablet_merge_ctx->inc_finish_count(ret, is_ready))) {
      LOG_WARN("fail to inc finish count", K(ret));
    } else if (is_ready) {
      const ObTabletID &tablet_id = tablet_merge_ctx->get_insert_tablet_ctx()->get_tablet_id();
      const ObDirectLoadInsertTableResult &insert_table_result = tablet_merge_ctx->get_insert_tablet_ctx()->get_insert_table_result();
      FLOG_INFO("tablet merge task all finished", K(tablet_merge_ctx->get_tablet_id()),
               K(tablet_id), K(insert_table_result));

      ObArray<ObITask *> write_macro_block_tasks;
      if (OB_FAIL(dag_->generate_tablet_write_macro_block_tasks(
                   tablet_id, write_macro_block_tasks, parent_task))) {
        LOG_WARN("fail to generate tablet write macro block tasks", K(ret));
      } else if (OB_FAIL(dag_->batch_add_task(write_macro_block_tasks))) {
        LOG_WARN("fail to batch add task", K(ret));
      }
    }
  }
  return ret;
}

/**
 * ObTableLoadInsertSSTableTask
 */

ObTableLoadInsertSSTableTask::ObTableLoadInsertSSTableTask(
  ObTableLoadDag *dag, ObTableLoadDagParallelMerger *parallel_merger)
  : ObITask(TASK_TYPE_DIRECT_LOAD_INSERT_SSTABLE),
    ObTableLoadDagInsertSSTableTaskBase(dag, parallel_merger)
{
}

int ObTableLoadInsertSSTableTask::process()
{
  int ret = OB_SUCCESS;
  ObDirectLoadIMergeTask *merge_task = nullptr;
  if (OB_FAIL(parallel_merger_->get_next_merge_task(merge_task))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("fail to get next merge task", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
  } else {
    ObITask *task = nullptr;
    if (dag_->get_ddl_table_schema().table_item_.is_column_store_ &&
        !(is_incremental_minor_direct_load(dag_->get_direct_load_type()))) {
      ObTableLoadMemoryFriendWriteMacroBlockTask *write_task = nullptr;
      if (OB_FAIL(dag_->alloc_task(write_task, this, merge_task))) {
        LOG_WARN("fail to alloc task", K(ret));
      } else {
        task = write_task;
      }
    } else {
      ObTableLoadMacroBlockWriteTask *write_task = nullptr;
      if (OB_FAIL(dag_->alloc_task(write_task, this, merge_task))) {
        LOG_WARN("fail to alloc task", K(ret));
      } else {
        task = write_task;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(task->deep_copy_children(get_child_nodes()))) {
        LOG_WARN("fail to deep copy children", K(ret));
      } else if (OB_FAIL(dag_->add_task(*task))) {
        LOG_WARN("fail to add task", K(ret));
      }
    }
  }
  return ret;
}

/**
 * ObTableLoadDagInsertSSTableClearTask
 */

ObTableLoadDagInsertSSTableClearTask::ObTableLoadDagInsertSSTableClearTask(
  ObTableLoadDag *dag, ObTableLoadDagParallelMerger *parallel_merger, const int64_t thread_idx)
  : ObITask(TASK_TYPE_DIRECT_LOAD_INSERT_SSTABLE_CLEAR),
    ObTableLoadDagInsertSSTableTaskBase(dag, parallel_merger),
    thread_idx_(thread_idx)
{
}

ObTableLoadDagInsertSSTableClearTask::ObTableLoadDagInsertSSTableClearTask(
  ObTableLoadDagInsertSSTableTaskBase *parent, const int64_t thread_idx)
  : ObITask(TASK_TYPE_DIRECT_LOAD_INSERT_SSTABLE_CLEAR),
    ObTableLoadDagInsertSSTableTaskBase(parent),
    thread_idx_(thread_idx)
{
}

int ObTableLoadDagInsertSSTableClearTask::generate_next_task(share::ObITask *&next_task)
{
  int ret = OB_SUCCESS;
  next_task = nullptr;
  const int64_t next_thread_idx = thread_idx_ + 1;
  if (next_thread_idx >= store_ctx_->thread_cnt_) {
    ret = OB_ITER_END;
  } else {
    ObTableLoadDagInsertSSTableClearTask *clear_task = nullptr;
    if (OB_FAIL(dag_->alloc_task(clear_task, this, next_thread_idx))) {
      LOG_WARN("fail to alloc task", K(ret));
    } else {
      next_task = clear_task;
    }
  }
  return ret;
}

int ObTableLoadDagInsertSSTableClearTask::process()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(parallel_merger_->prepare_clear_table())) {
    LOG_WARN("fail to prepare clear table", K(ret));
  } else if (OB_FAIL(parallel_merger_->clear_table(store_ctx_->thread_cnt_, thread_idx_))) {
    LOG_WARN("fail to clear table", K(ret));
  }
  return ret;
}

/**
 * ObTableLoadMemoryFriendWriteMacroBlockPipeline
 */

ObTableLoadMemoryFriendWriteMacroBlockPipeline::ObTableLoadMemoryFriendWriteMacroBlockPipeline()
  : ObDDLMemoryFriendWriteMacroBlockPipeline(TASK_TYPE_DIRECT_LOAD_WRITE_MACRO_BLOCK_PIPELINE),
    is_inited_(false),
    row_iterator_(nullptr),
    chunk_(),
    batch_datum_rows_write_op_(this),
    cg_row_file_writer_op_(this)
{
}

ObTableLoadMemoryFriendWriteMacroBlockPipeline::ObTableLoadMemoryFriendWriteMacroBlockPipeline(
  ObITabletSliceRowIterator *row_iterator)
  : ObDDLMemoryFriendWriteMacroBlockPipeline(TASK_TYPE_DIRECT_LOAD_WRITE_MACRO_BLOCK_PIPELINE),
    is_inited_(false),
    row_iterator_(row_iterator),
    chunk_(),
    batch_datum_rows_write_op_(this),
    cg_row_file_writer_op_(this)
{
}

ObTableLoadMemoryFriendWriteMacroBlockPipeline::~ObTableLoadMemoryFriendWriteMacroBlockPipeline()
{
  reset();
}

int ObTableLoadMemoryFriendWriteMacroBlockPipeline::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("the ObTableLoadMemoryFriendWriteMacroBlockPipeline has been initialized", K(ret));
  } else if (OB_ISNULL(row_iterator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected row iterator is null", KR(ret));
  } else {
    const ObTabletID tablet_id = row_iterator_->get_tablet_id();
    const int64_t slice_idx = row_iterator_->get_slice_idx();
    ObDDLIndependentDag *ddl_dag = static_cast<ObDDLIndependentDag *>(dag_);
    ObDDLSlice *ddl_slice = nullptr;
    ObDDLTabletContext *tablet_context = nullptr;
    bool is_slice_created = false;
    if (OB_FAIL(ddl_dag->get_tablet_context(tablet_id, tablet_context))) {
      LOG_WARN("fail to get tablet context", K(ret), K(tablet_id));
    } else if (OB_UNLIKELY(nullptr == tablet_context)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet context is null", K(ret), K(tablet_id));
    } else if (OB_FAIL(
                 tablet_context->get_or_create_slice(slice_idx, ddl_slice, is_slice_created))) {
      LOG_WARN("fail to get or create slice", K(ret), K(tablet_id), K(slice_idx));
    }
    if (FAILEDx(batch_datum_rows_write_op_.init(tablet_id, slice_idx))) {
      LOG_WARN("fail to initialize batch datum rows write op", K(ret), K(tablet_id), K(slice_idx));
    } else if (OB_FAIL(add_op(&batch_datum_rows_write_op_))) {
      LOG_WARN("fail to add batch datum rows write op", K(ret));
    } else if (OB_FAIL(cg_row_file_writer_op_.init(tablet_id, slice_idx,
                                                   ObBatchDatumRowsWriteOp::MAX_BATCH_SIZE))) {
      LOG_WARN("fail to initialize cg row file writer op", K(ret), K(tablet_id), K(slice_idx));
    } else if (OB_FAIL(add_op(&cg_row_file_writer_op_))) {
      LOG_WARN("fail to add cg row file writer op", K(ret));
    } else if (OB_FAIL(ObDDLMemoryFriendWriteMacroBlockPipeline::init(ddl_slice))) {
      LOG_WARN("fail to initialize ddl memory friend write macro block pipeline", K(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObTableLoadMemoryFriendWriteMacroBlockPipeline::get_next_chunk(ObChunk *&chunk)
{
  int ret = OB_SUCCESS;
  chunk = nullptr;
  chunk_.reset();
  const ObDatumRow *datum_row_ptr = nullptr;
  if (!is_inited_ && OB_FAIL(init())) {
    LOG_WARN("fail to init", KR(ret));
  } else if (OB_FAIL(row_iterator_->get_next_row(datum_row_ptr))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("fail to get next row", K(ret));
    } else {
      ret = OB_SUCCESS;
      chunk_.set_end_chunk();
      chunk = &chunk_;
    }
  } else {
    chunk_.type_ = ObChunk::DATUM_ROW;
    chunk_.datum_row_ = const_cast<ObDatumRow *>(datum_row_ptr);
    chunk = &chunk_;
  }
  return ret;
}

void ObTableLoadMemoryFriendWriteMacroBlockPipeline::postprocess(int &ret)
{
  if (OB_UNLIKELY(OB_ITER_END != ret)) {
    FLOG_INFO("ret code not expected", K(ret), KPC(this), KPC(dag_));
    // 旁路的写入任务不支持挂起, 修改错误码
    if (OB_DAG_TASK_IS_SUSPENDED == ret) {
      ret = OB_ERR_UNEXPECTED;
    }
  } else {
    ret = OB_SUCCESS;
    if (OB_FAIL(set_remain_block())) {
      LOG_WARN("fail to set remain block", K(ret));
    } else {
      LOG_INFO("the ObTableLoadMemoryFriendWriteMacroBlockPipeline has ret code iter end", K(ret));
    }
  }
}

void ObTableLoadMemoryFriendWriteMacroBlockPipeline::reset()
{
  is_inited_ = false;
  if (OB_LIKELY(nullptr != row_iterator_)) {
    ObMemAttr attr(MTL_ID(), "TLD_SliceIter");
    OB_DELETE(ObITabletSliceRowIterator, attr, row_iterator_);
    row_iterator_ = nullptr;
  }
}

/**
 * ObTableLoadMemoryFriendWriteMacroBlockTask
 */

ObTableLoadMemoryFriendWriteMacroBlockTask::ObTableLoadMemoryFriendWriteMacroBlockTask(
  ObTableLoadDagInsertSSTableTaskBase *parent, ObDirectLoadIMergeTask *merge_task)
  : ObTableLoadDagInsertSSTableTaskBase(parent), merge_task_(merge_task)
{
}

int ObTableLoadMemoryFriendWriteMacroBlockTask::init()
{
  int ret = OB_SUCCESS;
  if (nullptr == row_iterator_ && OB_FAIL(merge_task_->init_iterator(row_iterator_))) {
    LOG_WARN("fail to init iterator", KR(ret));
  } else if (OB_FAIL(ObTableLoadMemoryFriendWriteMacroBlockPipeline::init())) {
    LOG_WARN("fail to init", KR(ret));
  }
  return ret;
}

int ObTableLoadMemoryFriendWriteMacroBlockTask::generate_next_task(share::ObITask *&next_task)
{
  int ret = OB_SUCCESS;
  next_task = nullptr;
  ObDirectLoadIMergeTask *merge_task = nullptr;
  if (OB_FAIL(parallel_merger_->get_next_merge_task(merge_task))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("fail to get next iter", KR(ret));
    }
  } else {
    ObTableLoadMemoryFriendWriteMacroBlockTask *write_task = nullptr;
    if (OB_FAIL(dag_->alloc_task(write_task, this, merge_task))) {
      LOG_WARN("fail to alloc task", K(ret));
    } else {
      next_task = write_task;
    }
  }
  return ret;
}

void ObTableLoadMemoryFriendWriteMacroBlockTask::postprocess(int &ret)
{
  ObTableLoadMemoryFriendWriteMacroBlockPipeline::postprocess(ret);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(handle_merge_task_finish(this, merge_task_))) {
      LOG_WARN("fail to handle merge task finish", K(ret));
    }
  }
}

/**
 * ObTableLoadMacroBlockWriteTask
 */

ObTableLoadMacroBlockWriteTask::ObTableLoadMacroBlockWriteTask(
  ObTableLoadDagInsertSSTableTaskBase *parent, ObDirectLoadIMergeTask *merge_task)
  : share::ObITask(TASK_TABLE_LOAD_MACRO_BLOCK_WRITE_TASK),
    ObTableLoadDagInsertSSTableTaskBase(parent),
    merge_task_(merge_task)
{
}

int ObTableLoadMacroBlockWriteTask::process()
{
  int ret = OB_SUCCESS;
  ObITabletSliceRowIterator *row_iter = nullptr;
  ObITabletSliceWriter *storage_writer = nullptr;
  ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "TLMBWTask"));
  if (OB_FAIL(merge_task_->init_iterator(row_iter))) {
    LOG_WARN("fail to init iterator", KR(ret));
  } else {
    const ObTabletID tablet_id = row_iter->get_tablet_id();
    const int64_t slice_idx = row_iter->get_slice_idx();
    ObWriteMacroParam writer_param;
    writer_param.is_sorted_table_load_ = true;
    if (OB_FAIL(ObDDLUtil::fill_writer_param(tablet_id, slice_idx, -1 /*cg_idx*/, dag_,
                                             ObTabletSliceBufferTempFileWriter::ObDDLRowBuffer::DEFAULT_MAX_BATCH_SIZE,
                                             writer_param))) {
      LOG_WARN("fail to fill writer param", K(ret), K(tablet_id), K(slice_idx), K(dag_));
    } else if (OB_FAIL(ObDDLUtil::alloc_storage_macro_block_writer(writer_param, allocator,
                                                                   storage_writer))) {
      LOG_WARN("fail to alloc storage macro block writer", K(ret), K(writer_param));
    }
  }
  if (OB_SUCC(ret)) {
    const ObDatumRow *datum_row = nullptr;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(dag_->check_status())) {
        LOG_WARN("fail to check status", K(ret));
      } else if (OB_FAIL(row_iter->get_next_row(datum_row))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to get next row", KR(ret));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else if (OB_FAIL(storage_writer->append_row(*datum_row))) {
        LOG_WARN("fail to append row", K(ret), KPC(datum_row));
      }
    }
    if (FAILEDx(storage_writer->close())) {
      LOG_WARN("fail to close storage writer", K(ret));
    }
  }
  if (OB_LIKELY(nullptr != row_iter)) {
    ObMemAttr attr(MTL_ID(), "TLD_SliceIter");
    OB_DELETE(ObITabletSliceRowIterator, attr, row_iter);
  }
  if (OB_LIKELY(nullptr != storage_writer)) {
    storage_writer->~ObITabletSliceWriter();
    storage_writer = nullptr;
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(handle_merge_task_finish(this, merge_task_))) {
      LOG_WARN("fail to handle merge task finish", K(ret));
    }
  }
  return ret;
}

// start private functions
int ObTableLoadMacroBlockWriteTask::generate_next_task(ObITask *&next_task)
{
  int ret = OB_SUCCESS;
  next_task = nullptr;
  ObDirectLoadIMergeTask *merge_task = nullptr;
  if (OB_FAIL(parallel_merger_->get_next_merge_task(merge_task))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("fail to get next iter", KR(ret));
    }
  } else {
    ObTableLoadMacroBlockWriteTask *write_task = nullptr;
    if (OB_FAIL(dag_->alloc_task(write_task, this, merge_task))) {
      LOG_WARN("fail to alloc task", K(ret));
    } else {
      next_task = write_task;
    }
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
