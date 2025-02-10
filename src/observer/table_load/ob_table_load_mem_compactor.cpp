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

#include "observer/table_load/ob_table_load_mem_compactor.h"
#include "observer/table_load/ob_table_load_merge_mem_sort_op.h"
#include "observer/table_load/ob_table_load_schema.h"
#include "observer/table_load/ob_table_load_service.h"
#include "observer/table_load/ob_table_load_stat.h"
#include "observer/table_load/ob_table_load_store_ctx.h"
#include "observer/table_load/ob_table_load_store_table_ctx.h"
#include "observer/table_load/ob_table_load_table_ctx.h"
#include "observer/table_load/ob_table_load_task.h"
#include "observer/table_load/ob_table_load_task_scheduler.h"
#include "storage/direct_load/ob_direct_load_external_table.h"
#include "storage/direct_load/ob_direct_load_mem_loader.h"
#include "storage/direct_load/ob_direct_load_mem_sample.h"

namespace oceanbase
{
namespace observer
{
using namespace common;
using namespace common::hash;
using namespace storage;
using namespace table;
using namespace blocksstable;
using namespace sql;

class ObTableLoadMemCompactor::SampleTaskProcessor : public ObITableLoadTaskProcessor
{
public:
  SampleTaskProcessor(ObTableLoadTask &task, ObTableLoadTableCtx *ctx,
                      ObDirectLoadMemContext *mem_ctx)
    : ObITableLoadTaskProcessor(task), ctx_(ctx), mem_ctx_(mem_ctx)
  {
    ctx_->inc_ref_count();
  }
  virtual ~SampleTaskProcessor() { ObTableLoadService::put_ctx(ctx_); }
  int process() override
  {
    int ret = OB_SUCCESS;
    storage::ObDirectLoadMemSample sample(ctx_, mem_ctx_);
    if (OB_FAIL(sample.do_sample())) {
      LOG_WARN("fail to do sample", KR(ret));
    }
    return ret;
  }

private:
  ObTableLoadTableCtx *const ctx_;
  ObDirectLoadMemContext *mem_ctx_;
};

class ObTableLoadMemCompactor::LoadTaskProcessor : public ObITableLoadTaskProcessor
{
public:
  LoadTaskProcessor(ObTableLoadTask &task, ObTableLoadTableCtx *ctx,
                    ObDirectLoadMemContext *mem_ctx)
    : ObITableLoadTaskProcessor(task), ctx_(ctx), mem_ctx_(mem_ctx)
  {
    ctx_->inc_ref_count();
  }
  virtual ~LoadTaskProcessor() { ObTableLoadService::put_ctx(ctx_); }
  int process() override
  {
    OB_TABLE_LOAD_STATISTICS_TIME_COST(INFO, table_compactor_time_us);
    int ret = OB_SUCCESS;
    while (OB_SUCC(ret) && OB_LIKELY(!mem_ctx_->has_error_)) {
      ObDirectLoadMemWorker *loader = nullptr;
      mem_ctx_->mem_loader_queue_.pop_one(loader);
      if (loader == nullptr) {
        break;
      }
      if (OB_FAIL(loader->work())) {
        LOG_WARN("fail to compact", KR(ret));
      }
      loader->~ObDirectLoadMemWorker();
      loader = nullptr;
    }
    ATOMIC_AAF(&(mem_ctx_->finish_load_thread_cnt_), 1);
    return ret;
  }

private:
  ObTableLoadTableCtx *const ctx_;
  ObDirectLoadMemContext *mem_ctx_;
};

class ObTableLoadMemCompactor::DumpTaskProcessor : public ObITableLoadTaskProcessor
{
public:
  DumpTaskProcessor(ObTableLoadTask &task, ObTableLoadTableCtx *ctx,
                    ObDirectLoadMemContext *mem_ctx)
    : ObITableLoadTaskProcessor(task), ctx_(ctx), mem_ctx_(mem_ctx)
  {
    ctx_->inc_ref_count();
  }
  virtual ~DumpTaskProcessor() { ObTableLoadService::put_ctx(ctx_); }
  int process() override
  {
    int ret = OB_SUCCESS;
    void *tmp = nullptr;
    while (OB_SUCC(ret) && OB_LIKELY(!mem_ctx_->has_error_)) {
      if (OB_FAIL(mem_ctx_->mem_dump_queue_.pop(tmp))) {
        LOG_WARN("fail to pop", KR(ret));
      } else {
        if (tmp != nullptr) {
          ObDirectLoadMemDump *mem_dump = (ObDirectLoadMemDump *)tmp;
          if (OB_FAIL(mem_dump->do_dump())) {
            LOG_WARN("fail to dump mem", KR(ret));
          }
          mem_dump->~ObDirectLoadMemDump();
          ob_free(mem_dump);
        } else {
          if (OB_FAIL(mem_ctx_->mem_dump_queue_.push(nullptr))) {
            LOG_WARN("fail to push", KR(ret));
          } else {
            break;
          }
        }
      }
    }
    return ret;
  }

private:
  ObTableLoadTableCtx *const ctx_;
  ObDirectLoadMemContext *mem_ctx_;
};

class ObTableLoadMemCompactor::CompactTaskCallback : public ObITableLoadTaskCallback
{
public:
  CompactTaskCallback(ObTableLoadTableCtx *ctx, ObTableLoadMemCompactor *compactor)
    : ctx_(ctx), compactor_(compactor)
  {
    ctx_->inc_ref_count();
  }
  virtual ~CompactTaskCallback() { ObTableLoadService::put_ctx(ctx_); }
  void callback(int ret_code, ObTableLoadTask *task) override
  {
    int ret = ret_code;
    if (OB_SUCC(ret)) {
      if (OB_FAIL(compactor_->handle_compact_thread_finish())) {
        LOG_WARN("fail to handle compact thread finish", KR(ret));
      }
    }
    if (OB_FAIL(ret)) {
      compactor_->set_has_error();
      ctx_->store_ctx_->set_status_error(ret);
    }
    ctx_->free_task(task);
    OB_TABLE_LOAD_STATISTICS_PRINT_AND_RESET();
  }

private:
  ObTableLoadTableCtx *const ctx_;
  ObTableLoadMemCompactor *const compactor_;
};

class ObTableLoadMemCompactor::FinishTaskProcessor : public ObITableLoadTaskProcessor
{
public:
  FinishTaskProcessor(ObTableLoadTask &task, ObTableLoadTableCtx *ctx,
                      ObTableLoadMemCompactor *compactor)
    : ObITableLoadTaskProcessor(task), ctx_(ctx), compactor_(compactor)
  {
    ctx_->inc_ref_count();
  }
  virtual ~FinishTaskProcessor() { ObTableLoadService::put_ctx(ctx_); }
  int process() override { return compactor_->finish(); }

private:
  ObTableLoadTableCtx *const ctx_;
  ObTableLoadMemCompactor *const compactor_;
};

class ObTableLoadMemCompactor::FinishTaskCallback : public ObITableLoadTaskCallback
{
public:
  FinishTaskCallback(ObTableLoadTableCtx *ctx) : ctx_(ctx) { ctx_->inc_ref_count(); }
  virtual ~FinishTaskCallback() { ObTableLoadService::put_ctx(ctx_); }
  void callback(int ret_code, ObTableLoadTask *task) override
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(ret_code)) {
      ctx_->store_ctx_->set_status_error(ret);
    }
    ctx_->free_task(task);
    OB_TABLE_LOAD_STATISTICS_PRINT_AND_RESET();
  }

private:
  ObTableLoadTableCtx *const ctx_;
};

ObTableLoadMemCompactor::ObTableLoadMemCompactor()
  : store_ctx_(nullptr),
    store_table_ctx_(nullptr),
    op_(nullptr),
    allocator_("TLD_MemC"),
    task_scheduler_(nullptr),
    finish_thread_cnt_(0),
    is_inited_(false)
{
  allocator_.set_tenant_id(MTL_ID());
}

ObTableLoadMemCompactor::~ObTableLoadMemCompactor() { reset(); }

void ObTableLoadMemCompactor::reset()
{
  is_inited_ = false;
  // 先把sample线程停了
  mem_ctx_.has_error_ = true;
  if (nullptr != task_scheduler_) {
    task_scheduler_->stop();
    task_scheduler_->wait();
    task_scheduler_->~ObITableLoadTaskScheduler();
    allocator_.free(task_scheduler_);
    task_scheduler_ = nullptr;
  }
  store_ctx_ = nullptr;
  store_table_ctx_ = nullptr;
  op_ = nullptr;
  mem_ctx_.reset();
  finish_thread_cnt_ = 0;
  // 分配器最后reset
  allocator_.reset();
}

int ObTableLoadMemCompactor::init(ObTableLoadMergeMemSortOp *op)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadMemCompactor init twice", KR(ret), KP(this));
  } else if (OB_ISNULL(op)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(op));
  } else {
    store_ctx_ = op->store_ctx_;
    store_table_ctx_ = op->merge_table_ctx_->store_table_ctx_;
    op_ = op;

    if (OB_UNLIKELY(store_ctx_->thread_cnt_ < 2)) {
      // 排序至少需要两个线程
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid args", KR(ret), K(store_ctx_->thread_cnt_));
    } else {
      mem_ctx_.table_data_desc_ = op_->merge_table_ctx_->table_store_->get_table_data_desc();
      mem_ctx_.datum_utils_ = &op_->merge_table_ctx_->store_table_ctx_->schema_->datum_utils_;
      mem_ctx_.dml_row_handler_ = op_->merge_table_ctx_->dml_row_handler_;
      mem_ctx_.file_mgr_ = store_ctx_->tmp_file_mgr_;
      mem_ctx_.table_mgr_ = store_ctx_->table_mgr_;
      mem_ctx_.dup_action_ = store_ctx_->ctx_->param_.dup_action_;

      mem_ctx_.exe_mode_ = store_ctx_->ctx_->param_.exe_mode_;
      mem_ctx_.merge_count_per_round_ = store_ctx_->merge_count_per_round_;
      mem_ctx_.max_mem_chunk_count_ = store_ctx_->max_mem_chunk_count_;
      mem_ctx_.mem_chunk_size_ = store_ctx_->mem_chunk_size_;
      mem_ctx_.heap_table_mem_chunk_size_ = store_ctx_->heap_table_mem_chunk_size_;

      mem_ctx_.total_thread_cnt_ = store_ctx_->thread_cnt_;
      mem_ctx_.dump_thread_cnt_ =
        MAX(mem_ctx_.total_thread_cnt_ / 3, 1); //暂时先写成1/3，后续再优化
      mem_ctx_.load_thread_cnt_ = mem_ctx_.total_thread_cnt_ - mem_ctx_.dump_thread_cnt_;
    }

    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(mem_ctx_.load_thread_cnt_ <= 0)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("mem load thread cannot be zero", KR(ret), K(mem_ctx_.total_thread_cnt_),
                 K(mem_ctx_.dump_thread_cnt_), K(mem_ctx_.load_thread_cnt_));
      } else if (OB_FAIL(mem_ctx_.init())) {
        LOG_WARN("fail to init compactor ctx", KR(ret));
      } else if (OB_FAIL(init_scheduler())) {
        LOG_WARN("fail to init_scheduler", KR(ret));
      } else {
        is_inited_ = true;
      }
    }
  }
  return ret;
}

int ObTableLoadMemCompactor::init_scheduler()
{
  int ret = OB_SUCCESS;
  // 初始化task_scheduler_
  if (OB_ISNULL(task_scheduler_ = OB_NEWx(ObTableLoadTaskThreadPoolScheduler, (&allocator_),
                                          1 /*thread_count*/, store_ctx_->ctx_->param_.table_id_,
                                          "MemSample", store_ctx_->ctx_->session_info_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to new ObTableLoadTaskThreadPoolScheduler", KR(ret));
  } else if (OB_FAIL(task_scheduler_->init())) {
    LOG_WARN("fail to init task scheduler", KR(ret));
  } else if (OB_FAIL(task_scheduler_->start())) {
    LOG_WARN("fail to start task scheduler", KR(ret));
  }
  return ret;
}

int ObTableLoadMemCompactor::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadMemCompactor not init", KR(ret), KP(this));
  } else {
    if (OB_FAIL(construct_compactors())) {
      LOG_WARN("fail to construct compactors", KR(ret));
    } else if (OB_FAIL(start_compact())) {
      LOG_WARN("fail to start compact", KR(ret));
    }
  }
  return ret;
}

int ObTableLoadMemCompactor::construct_compactors()
{
  int ret = OB_SUCCESS;
  ObDirectLoadTableStore *table_store = op_->merge_table_ctx_->table_store_;
  if (OB_UNLIKELY(table_store->empty() || !table_store->is_external_table())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected not external table", KR(ret), KPC(table_store));
  } else {
    // 根据table_store构造任务
    FOREACH_X(it, *table_store, OB_SUCC(ret))
    {
      ObDirectLoadTableHandleArray *table_handle_array = it->second;
      for (int64_t i = 0; OB_SUCC(ret) && i < table_handle_array->count(); ++i) {
        if (OB_FAIL(add_tablet_table(table_handle_array->at(i)))) {
          LOG_WARN("fail to add tablet table", KR(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      // 清空table_store, 以便在排序过程中能释放磁盘空间
      table_store->clear();
    }
  }
  return ret;
}

int ObTableLoadMemCompactor::add_tablet_table(const ObDirectLoadTableHandle &table_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!table_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(table_handle));
  } else {
    ObDirectLoadMemLoader *loader = nullptr;
    if (OB_ISNULL(loader =
                    OB_NEWx(ObDirectLoadMemLoader, (&allocator_), store_ctx_->ctx_, &mem_ctx_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObDirectLoadMemLoader", KR(ret));
    } else if (OB_FAIL(loader->add_table(table_handle))) {
      LOG_WARN("fail to add table", KR(ret));
    } else if (OB_FAIL(mem_ctx_.mem_loader_queue_.push(loader))) {
      LOG_WARN("fail to push", KR(ret));
    }
    if (OB_FAIL(ret)) {
      if (nullptr != loader) {
        loader->~ObDirectLoadMemLoader();
        allocator_.free(loader);
        loader = nullptr;
      }
    }
  }
  return ret;
}

int ObTableLoadMemCompactor::start_load()
{
  int ret = OB_SUCCESS;
  ObTableLoadTableCtx *ctx = store_ctx_->ctx_;
  for (int64_t i = 0; OB_SUCC(ret) && i < mem_ctx_.load_thread_cnt_; ++i) {
    ObTableLoadTask *task = nullptr;
    // 1. 分配task
    if (OB_FAIL(ctx->alloc_task(task))) {
      LOG_WARN("fail to alloc task", KR(ret));
    }
    // 2. 设置processor
    else if (OB_FAIL(task->set_processor<LoadTaskProcessor>(ctx, &mem_ctx_))) {
      LOG_WARN("fail to set load task processor", KR(ret));
    }
    // 3. 设置callback
    else if (OB_FAIL(task->set_callback<CompactTaskCallback>(ctx, this))) {
      LOG_WARN("fail to set compact task callback", KR(ret));
    }
    // 4. 把task放入调度器
    else if (OB_FAIL(store_ctx_->task_scheduler_->add_task(i, task))) {
      LOG_WARN("fail to add task", KR(ret), KPC(task));
    }
    if (OB_FAIL(ret)) {
      if (nullptr != task) {
        ctx->free_task(task);
      }
    }
  }
  return ret;
}

int ObTableLoadMemCompactor::start_dump()
{
  int ret = OB_SUCCESS;
  ObTableLoadTableCtx *ctx = store_ctx_->ctx_;
  const int64_t dump_thread_start_idx = mem_ctx_.load_thread_cnt_;
  for (int64_t i = 0; OB_SUCC(ret) && i < mem_ctx_.dump_thread_cnt_; ++i) {
    ObTableLoadTask *task = nullptr;
    // 1. 分配task
    if (OB_FAIL(ctx->alloc_task(task))) {
      LOG_WARN("fail to alloc task", KR(ret));
    }
    // 2. 设置processor
    else if (OB_FAIL(task->set_processor<DumpTaskProcessor>(ctx, &mem_ctx_))) {
      LOG_WARN("fail to set dump task processor", KR(ret));
    }
    // 3. 设置callback
    else if (OB_FAIL(task->set_callback<CompactTaskCallback>(ctx, this))) {
      LOG_WARN("fail to set compactor task callback", KR(ret));
    }
    // 4. 把task放入调度器
    else if (OB_FAIL(store_ctx_->task_scheduler_->add_task(dump_thread_start_idx + i, task))) {
      LOG_WARN("fail to add task", KR(ret), KPC(task));
    }
    if (OB_FAIL(ret)) {
      if (nullptr != task) {
        ctx->free_task(task);
      }
    }
  }
  return ret;
}

int ObTableLoadMemCompactor::start_sample()
{
  int ret = OB_SUCCESS;
  ObTableLoadTableCtx *ctx = store_ctx_->ctx_;
  ObTableLoadTask *task = nullptr;
  // 1. 分配task
  if (OB_FAIL(ctx->alloc_task(task))) {
    LOG_WARN("fail to alloc task", KR(ret));
  }
  // 2. 设置processor
  else if (OB_FAIL(task->set_processor<SampleTaskProcessor>(ctx, &mem_ctx_))) {
    LOG_WARN("fail to set sample task processor", KR(ret));
  }
  // 3. 设置callback
  else if (OB_FAIL(task->set_callback<CompactTaskCallback>(ctx, this))) {
    LOG_WARN("fail to set compactor task callback", KR(ret));
  }
  // 4. 把task放入调度器
  else if (OB_FAIL(task_scheduler_->add_task(0, task))) {
    LOG_WARN("fail to add task", KR(ret), KPC(task));
  }
  if (OB_FAIL(ret)) {
    if (nullptr != task) {
      ctx->free_task(task);
    }
  }
  return ret;
}

int ObTableLoadMemCompactor::start_finish()
{
  int ret = OB_SUCCESS;
  ObTableLoadTableCtx *ctx = store_ctx_->ctx_;
  ObTableLoadTask *task = nullptr;
  // 1. 分配task
  if (OB_FAIL(ctx->alloc_task(task))) {
    LOG_WARN("fail to alloc task", KR(ret));
  }
  // 2. 设置processor
  else if (OB_FAIL(task->set_processor<FinishTaskProcessor>(ctx, this))) {
    LOG_WARN("fail to set finish task processor", KR(ret));
  }
  // 3. 设置callback
  else if (OB_FAIL(task->set_callback<FinishTaskCallback>(ctx))) {
    LOG_WARN("fail to set finish task callback", KR(ret));
  }
  // 4. 把task放入调度器
  else if (OB_FAIL(store_ctx_->task_scheduler_->add_task(0, task))) {
    LOG_WARN("fail to add task", KR(ret), KPC(task));
  }
  if (OB_FAIL(ret)) {
    if (nullptr != task) {
      ctx->free_task(task);
    }
  }
  return ret;
}

int ObTableLoadMemCompactor::start_compact()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(start_load())) {
    LOG_WARN("fail to start load", KR(ret));
  } else if (OB_FAIL(start_sample())) {
    LOG_WARN("fail to start sample", KR(ret));
  } else if (OB_FAIL(start_dump())) {
    LOG_WARN("fail to start dump", KR(ret));
  }
  if (OB_FAIL(ret)) {
    set_has_error();
  }
  return ret;
}

void ObTableLoadMemCompactor::stop()
{
  set_has_error(); //先设置为error，因为stop的场景就是error
  if (nullptr != task_scheduler_) {
    task_scheduler_->stop();
  }
}

int ObTableLoadMemCompactor::handle_compact_thread_finish()
{
  int ret = OB_SUCCESS;
  const int64_t finish_thread_cnt = ATOMIC_AAF(&finish_thread_cnt_, 1);
  if (finish_thread_cnt == mem_ctx_.total_thread_cnt_ + 1 /*sample thread*/) {
    if (OB_LIKELY(!mem_ctx_.has_error_) && OB_FAIL(start_finish())) {
      LOG_WARN("fail to start finish", KR(ret));
    }
  }
  return ret;
}

int ObTableLoadMemCompactor::finish()
{
  int ret = OB_SUCCESS;
  ObDirectLoadTableStore *table_store = op_->merge_table_ctx_->table_store_;
  table_store->clear();
  table_store->set_table_data_desc(mem_ctx_.table_data_desc_);
  table_store->set_multiple_sstable();
  if (OB_FAIL(table_store->add_tables(mem_ctx_.tables_handle_))) {
    LOG_WARN("fail to add tables", KR(ret));
  } else {
    mem_ctx_.reset();
    if (OB_FAIL(op_->on_success())) {
      LOG_WARN("fail to handle success", KR(ret));
    }
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
