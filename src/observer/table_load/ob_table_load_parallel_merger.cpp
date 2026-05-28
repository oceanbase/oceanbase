/**
 * Copyright (c) 2024 OceanBase
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

#include "observer/table_load/ob_table_load_parallel_merger.h"
#include "observer/table_load/ob_table_load_merge_rescan_op.h"
#include "observer/table_load/ob_table_load_merge_table_op.h"
#include "observer/table_load/ob_table_load_schema.h"
#include "observer/table_load/ob_table_load_service.h"
#include "observer/table_load/ob_table_load_store_ctx.h"
#include "observer/table_load/ob_table_load_store_table_ctx.h"
#include "observer/table_load/ob_table_load_table_ctx.h"
#include "observer/table_load/ob_table_load_task.h"
#include "observer/table_load/ob_table_load_task_scheduler.h"

namespace oceanbase
{
namespace observer
{
ObTableLoadParallelMerger::ObTableLoadParallelMerger()
  : store_ctx_(nullptr),
    op_(nullptr),
    running_thread_cnt_(0),
    has_error_(false),
    is_stop_(false),
    is_inited_(false)
{
}

ObTableLoadParallelMerger::~ObTableLoadParallelMerger() {}

int ObTableLoadParallelMerger::init_merge_ctx(ObTableLoadMergeTableBaseOp *op)
{
  int ret = OB_SUCCESS;
  ObTableLoadStoreTableCtx *store_table_ctx = op->merge_table_ctx_->store_table_ctx_;
  ObDirectLoadMergeParam param;
  param.table_id_ = store_table_ctx->table_id_;
  param.rowkey_column_num_ = store_table_ctx->schema_->rowkey_column_count_;
  param.column_count_ = store_table_ctx->schema_->store_column_count_;
  param.col_descs_ = &store_table_ctx->schema_->column_descs_;
  param.datum_utils_ = &store_table_ctx->schema_->datum_utils_;
  param.lob_column_idxs_ = &store_table_ctx->schema_->lob_column_idxs_;
  param.merge_mode_ = op->merge_table_ctx_->merge_mode_;
  param.use_batch_mode_ = op->merge_table_ctx_->use_batch_mode_;
  param.dml_row_handler_ = op->merge_table_ctx_->dml_row_handler_;
  param.insert_table_ctx_ = op->merge_table_ctx_->insert_table_ctx_;
  param.trans_param_ = op->merge_phase_ctx_->trans_param_;
  param.file_mgr_ = op->store_ctx_->tmp_file_mgr_;
  param.ctx_ = op->ctx_;
  if (OB_FAIL(merge_ctx_.init(param, store_table_ctx->ls_partition_ids_))) {
    LOG_WARN("fail to init merge ctx", KR(ret), K(param));
  }
  return ret;
}

int ObTableLoadParallelMerger::init_merge_task(ObTableLoadMergeTableBaseOp *op)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadParallelMerger init twice", KR(ret), KP(this));
  } else if (OB_ISNULL(op)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(op));
  } else if (OB_FAIL(init_merge_ctx(op))) {
    LOG_WARN("fail to init merge ctx", KR(ret));
  } else {
    ObDirectLoadTableStore *table_store = op->merge_table_ctx_->table_store_;
    const int64_t thread_cnt = op->store_ctx_->thread_cnt_;
    if (op->merge_table_ctx_->is_del_lob_) {
      if (OB_FAIL(merge_ctx_.build_del_lob_task(*table_store, thread_cnt))) {
        LOG_WARN("fail to build del lob task", KR(ret));
      }
    } else {
      if (OB_FAIL(merge_ctx_.build_merge_task(*table_store, thread_cnt))) {
        LOG_WARN("fail to build merge task", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(task_iter_.init(&merge_ctx_))) {
        LOG_WARN("fail to init task iter", KR(ret));
      } else {
        // merge_task会持有table的引用计数, 这里可以先清空
        table_store->clear();
        store_ctx_ = op->store_ctx_;
        op_ = op;
        is_inited_ = true;
      }
    }
  }
  return ret;
}

int ObTableLoadParallelMerger::init_rescan_task(ObTableLoadMergeTableBaseOp *op)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadParallelMerger init twice", KR(ret), KP(this));
  } else if (OB_ISNULL(op)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(op));
  } else if (OB_FAIL(init_merge_ctx(op))) {
    LOG_WARN("fail to init merge ctx", KR(ret));
  } else if (OB_FAIL(merge_ctx_.build_rescan_task(op->store_ctx_->thread_cnt_))) {
    LOG_WARN("fail to build rescan task", KR(ret));
  } else if (OB_FAIL(task_iter_.init(&merge_ctx_))) {
    LOG_WARN("fail to init task iter", KR(ret));
  } else {
    store_ctx_ = op->store_ctx_;
    op_ = op;
    is_inited_ = true;
  }
  return ret;
}

int ObTableLoadParallelMerger::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadParallelMerger not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(0 != running_thread_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected running thread count not zero", KR(ret), K(running_thread_cnt_));
  } else {
    const int64_t thread_count = store_ctx_->task_scheduler_->get_thread_count();
    ObTableLoadTableCtx *ctx = store_ctx_->ctx_;
    running_thread_cnt_ = thread_count;
    for (int64_t i = 0; OB_SUCC(ret) && i < thread_count; ++i) {
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
      else if (OB_FAIL(store_ctx_->task_scheduler_->add_task(i, task))) {
        LOG_WARN("fail to add task", KR(ret), K(i), KPC(task));
      }
      if (OB_FAIL(ret)) {
        if (nullptr != task) {
          ctx->free_task(task);
          task = nullptr;
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
    has_error_ = true;
  }
  return ret;
}

void ObTableLoadParallelMerger::stop()
{
  is_stop_ = true;
  ObMutexGuard guard(mutex_);
  ObDirectLoadIMergeTask *merge_task = nullptr;
  DLIST_FOREACH_NORET(merge_task, running_task_list_) { merge_task->stop(); }
}

int ObTableLoadParallelMerger::get_next_merge_task(ObDirectLoadIMergeTask *&merge_task)
{
  int ret = OB_SUCCESS;
  merge_task = nullptr;
  if (OB_UNLIKELY(is_stop_ || has_error_)) {
    ret = OB_ITER_END;
  } else {
    ObMutexGuard guard(mutex_);
    if (OB_FAIL(task_iter_.get_next_task(merge_task))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to get next merger", KR(ret));
      }
    } else {
      abort_unless(running_task_list_.add_last(merge_task));
    }
  }
  return ret;
}

int ObTableLoadParallelMerger::handle_merge_task_finish(ObDirectLoadIMergeTask *merge_task,
                                                        int ret_code)
{
  int ret = OB_SUCCESS;
  // 先把任务从running队列移除
  {
    ObMutexGuard guard(mutex_);
    abort_unless(OB_NOT_NULL(running_task_list_.remove(merge_task)));
  }
  // 任务可能执行成功或失败
  ObDirectLoadTabletMergeCtx *tablet_merge_ctx = merge_task->get_merge_ctx();
  bool is_ready = false;
  if (OB_ISNULL(tablet_merge_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet merge ctx is null", KR(ret), KP(tablet_merge_ctx));
  } else if (OB_FAIL(tablet_merge_ctx->inc_finish_count(ret_code, is_ready))) {
    LOG_WARN("fail to inc finish count", KR(ret));
  } else if (is_ready) {
    ObDirectLoadInsertTabletContext *insert_tablet_ctx = tablet_merge_ctx->get_insert_tablet_ctx();
    if (op_->merge_table_ctx_->need_calc_range_ &&
        OB_FAIL(insert_tablet_ctx->calc_range(store_ctx_->thread_cnt_))) {
      LOG_WARN("fail to calc range", KR(ret));
    } else if (op_->merge_table_ctx_->need_close_insert_tablet_ctx_ &&
               OB_FAIL(insert_tablet_ctx->close())) {
      LOG_WARN("fail to close", KR(ret));
    } else {
      // 清理
      tablet_merge_ctx->reset();
    }
  }
  return ret;
}

int ObTableLoadParallelMerger::handle_merge_thread_finish(int ret_code)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ret_code)) {
    has_error_ = true;
  }
  const int64_t running_thread_cnt = ATOMIC_SAF(&running_thread_cnt_, 1);
  if (0 == running_thread_cnt) {
    if (OB_UNLIKELY(is_stop_ || has_error_)) {
    } else {
      // cleanup merge ctx
      merge_ctx_.reset();
      if (OB_FAIL(op_->on_success())) {
        LOG_WARN("fail to handle success", KR(ret));
      }
    }
  }
  return ret;
}

class ObTableLoadParallelMerger::MergeTaskProcessor : public ObITableLoadTaskProcessor
{
public:
  MergeTaskProcessor(ObTableLoadTask &task, ObTableLoadTableCtx *ctx,
                     ObTableLoadParallelMerger *parallel_merger)
    : ObITableLoadTaskProcessor(task), ctx_(ctx), parallel_merger_(parallel_merger)
  {
    ctx_->inc_ref_count();
  }

  virtual ~MergeTaskProcessor() { ObTableLoadService::put_ctx(ctx_); }
  int process() override
  {
    int ret = OB_SUCCESS;
    int tmp_ret = OB_SUCCESS;
    ObDirectLoadIMergeTask *merge_task = nullptr;
    while (OB_SUCC(ret)) {
      merge_task = nullptr;
      if (OB_FAIL(parallel_merger_->get_next_merge_task(merge_task))) {
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
        if (OB_TMP_FAIL(parallel_merger_->handle_merge_task_finish(merge_task, ret))) {
          LOG_WARN("fail to handle merge task finish", KR(ret));
          ret = COVER_SUCC(tmp_ret);
        }
      }
    }
    return ret;
  }

private:
  ObTableLoadTableCtx *ctx_;
  ObTableLoadParallelMerger *parallel_merger_;
};

class ObTableLoadParallelMerger::MergeTaskCallback : public ObITableLoadTaskCallback
{
public:
  MergeTaskCallback(ObTableLoadTableCtx *ctx, ObTableLoadParallelMerger *parallel_merger)
    : ctx_(ctx), parallel_merger_(parallel_merger)
  {
    ctx_->inc_ref_count();
  }
  virtual ~MergeTaskCallback() { ObTableLoadService::put_ctx(ctx_); }
  void callback(int ret_code, ObTableLoadTask *task) override
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(parallel_merger_->handle_merge_thread_finish(ret_code))) {
      LOG_WARN("fail to handle merge thread finish", KR(ret));
    }
    if (OB_FAIL(ret)) {
      ctx_->store_ctx_->set_status_error(ret);
    }
    ctx_->free_task(task);
  }

private:
  ObTableLoadTableCtx *ctx_;
  ObTableLoadParallelMerger *parallel_merger_;
};

} // namespace observer
} // namespace oceanbase
