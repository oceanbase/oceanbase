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
#include "observer/table_load/ob_table_load_error_row_handler.h"
#include "observer/table_load/ob_table_load_service.h"
#include "observer/table_load/ob_table_load_stat.h"
#include "observer/table_load/ob_table_load_store_ctx.h"
#include "observer/table_load/ob_table_load_table_ctx.h"
#include "observer/table_load/ob_table_load_task.h"
#include "observer/table_load/ob_table_load_task_scheduler.h"
#include "observer/table_load/ob_table_load_trans_store.h"
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
  SampleTaskProcessor(ObTableLoadTask &task, ObTableLoadTableCtx *ctx, ObDirectLoadMemContext *mem_ctx)
    : ObITableLoadTaskProcessor(task), ctx_(ctx), mem_ctx_(mem_ctx)
  {
    ctx_->inc_ref_count();
  }
  virtual ~SampleTaskProcessor()
  {
    ObTableLoadService::put_ctx(ctx_);
  }
  int process() override
  {
    int ret = OB_SUCCESS;
    storage::ObDirectLoadMemSample sample(mem_ctx_);
    if (OB_FAIL(sample.do_sample())) {
      LOG_WARN("fail to do sample", KR(ret));
    }
    return ret;
  }
private:
  ObTableLoadTableCtx * const ctx_;
  ObDirectLoadMemContext *mem_ctx_;
};

class ObTableLoadMemCompactor::CompactTaskProcessor : public ObITableLoadTaskProcessor
{
public:
  CompactTaskProcessor(ObTableLoadTask &task,
                       ObTableLoadTableCtx *ctx,
                       ObDirectLoadMemContext *mem_ctx)
    : ObITableLoadTaskProcessor(task),
      ctx_(ctx),
      mem_ctx_(mem_ctx)
  {
    ctx_->inc_ref_count();
  }
  virtual ~CompactTaskProcessor()
  {
    ObTableLoadService::put_ctx(ctx_);
  }
  int process() override
  {
    OB_TABLE_LOAD_STATISTICS_TIME_COST(INFO, table_compactor_time_us);
    int ret = OB_SUCCESS;
    while (OB_SUCC(ret) && !(mem_ctx_->has_error_)) {
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
    ATOMIC_AAF(&(mem_ctx_->finish_compact_count_), 1);
    return ret;
  }
private:
  ObTableLoadTableCtx * const ctx_;
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
  virtual ~CompactTaskCallback()
  {
    ObTableLoadService::put_ctx(ctx_);
  }
  void callback(int ret_code, ObTableLoadTask *task) override
  {
    int ret = ret_code;
    if (OB_SUCC(ret)) {
      if (OB_FAIL(compactor_->handle_compact_task_finish(ret_code))) {
        LOG_WARN("fail to handle_compact_task_finish", KR(ret));
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
  FinishTaskProcessor(ObTableLoadTask &task,
                      ObTableLoadTableCtx *ctx,
                      ObTableLoadMemCompactor *compactor)
    : ObITableLoadTaskProcessor(task),
    ctx_(ctx), compactor_(compactor)
  {
    ctx_->inc_ref_count();
  }
  virtual ~FinishTaskProcessor()
  {
    ObTableLoadService::put_ctx(ctx_);
  }
  int process() override
  {
    return compactor_->finish();
  }
private:
  ObTableLoadTableCtx *const ctx_;
  ObTableLoadMemCompactor *const compactor_;
};

class ObTableLoadMemCompactor::FinishTaskCallback : public ObITableLoadTaskCallback
{
public:
  FinishTaskCallback(ObTableLoadTableCtx *ctx, ObTableLoadMemCompactor *compactor)
    : ctx_(ctx), compactor_(compactor)
  {
    ctx_->inc_ref_count();
  }
  virtual ~FinishTaskCallback()
  {
    ObTableLoadService::put_ctx(ctx_);
  }
  void callback(int ret_code, ObTableLoadTask *task) override
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(ret_code)) {
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

class ObTableLoadMemCompactor::MemDumpTaskProcessor : public ObITableLoadTaskProcessor
{
public:
  MemDumpTaskProcessor(ObTableLoadTask &task,
                      ObTableLoadTableCtx *ctx,
                      ObDirectLoadMemContext *mem_ctx)
    : ObITableLoadTaskProcessor(task),
    ctx_(ctx),
    mem_ctx_(mem_ctx)
  {
    ctx_->inc_ref_count();
  }
  virtual ~MemDumpTaskProcessor()
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

ObTableLoadMemCompactor::ObTableLoadMemCompactor()
  : store_ctx_(nullptr),
    param_(nullptr),
    allocator_("TLD_MemC"),
    finish_task_count_(0),
    task_scheduler_(nullptr),
    parallel_merge_cb_(this)
{
}

ObTableLoadMemCompactor::~ObTableLoadMemCompactor()
{
  reset();
}

void ObTableLoadMemCompactor::reset()
{
  store_ctx_ = nullptr;
  param_ = nullptr;
  finish_task_count_ = 0;

  if (nullptr != task_scheduler_) {
    task_scheduler_->stop();
    task_scheduler_->wait();
    task_scheduler_->~ObITableLoadTaskScheduler();
    allocator_.free(task_scheduler_);
    task_scheduler_ = nullptr;
  }
  mem_ctx_.reset();
  allocator_.reset();
}

int ObTableLoadMemCompactor::inner_init()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  store_ctx_ = compact_ctx_->store_ctx_;
  param_ = &(store_ctx_->ctx_->param_);
  allocator_.set_tenant_id(tenant_id);
  if (OB_FAIL(init_scheduler())) {
    LOG_WARN("fail to init_scheduler", KR(ret));
  } else {
    mem_ctx_.mem_dump_task_count_ = param_->session_count_ / 3; //暂时先写成1/3，后续再优化
    if (mem_ctx_.mem_dump_task_count_ == 0)  {
      mem_ctx_.mem_dump_task_count_ = 1;
    }
    mem_ctx_.table_data_desc_ = store_ctx_->table_data_desc_;
    mem_ctx_.datum_utils_ = &(store_ctx_->ctx_->schema_.datum_utils_);
    mem_ctx_.need_sort_ = param_->need_sort_;
    mem_ctx_.mem_load_task_count_ = param_->session_count_;
    mem_ctx_.column_count_ = param_->column_count_;
    mem_ctx_.dml_row_handler_ = store_ctx_->error_row_handler_;
    mem_ctx_.file_mgr_ = store_ctx_->tmp_file_mgr_;
    mem_ctx_.dup_action_ = param_->dup_action_;
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(mem_ctx_.init())) {
      LOG_WARN("fail to init compactor ctx", KR(ret));
    } else if (OB_FAIL(parallel_merge_ctx_.init(store_ctx_))) {
      LOG_WARN("fail to init parallel merge ctx", KR(ret));
    }
  }

  return ret;
}

int ObTableLoadMemCompactor::init_scheduler()
{
  int ret = OB_SUCCESS;
  // 初始化task_scheduler_
  if (OB_ISNULL(task_scheduler_ = OB_NEWx(ObTableLoadTaskThreadPoolScheduler, (&allocator_), 1,
                                          param_->table_id_, "MemCompact"))) {
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
  ObSEArray<ObTableLoadTransStore *, 64> trans_store_array;
  if (OB_FAIL(store_ctx_->get_committed_trans_stores(trans_store_array))) {
    LOG_WARN("fail to get committed trans stores", KR(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < trans_store_array.count(); ++i) {
    ObTableLoadTransStore *trans_store = trans_store_array.at(i);
    for (int64_t j = 0; OB_SUCC(ret) && j < trans_store->session_store_array_.count(); ++j) {
      const ObTableLoadTransStore::SessionStore *session_store = trans_store->session_store_array_.at(j);
      for (int64_t k = 0; OB_SUCC(ret) && k < session_store->partition_table_array_.count(); ++k) {
        ObIDirectLoadPartitionTable *table = session_store->partition_table_array_.at(k);
        if (OB_FAIL(add_tablet_table(table))) {
          LOG_WARN("fail to add tablet table", KR(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    store_ctx_->clear_committed_trans_stores();
  }
  return ret;
}

int ObTableLoadMemCompactor::add_tablet_table(ObIDirectLoadPartitionTable *table)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(table));
  } else {
    ObDirectLoadMemLoader *loader = nullptr;
    if (OB_FAIL(create_mem_loader(loader))) {
      LOG_WARN("fail to create tablet table compactor", KR(ret));
    } else if (OB_FAIL(mem_ctx_.mem_loader_queue_.push(loader))) {
      LOG_WARN("fail to push", KR(ret));
    } else if (OB_FAIL(loader->add_table(table))) {
      LOG_WARN("fail to add table", KR(ret));
    }
  }
  return ret;
}

int ObTableLoadMemCompactor::create_mem_loader(ObDirectLoadMemLoader *&mem_loader)
{
  int ret = OB_SUCCESS;
  mem_loader = nullptr;
  if (OB_ISNULL(mem_loader =
                  OB_NEWx(ObDirectLoadMemLoader, (&allocator_), &mem_ctx_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to new ObDirectLoadMemLoader", KR(ret));
  }
  if (OB_FAIL(ret)) {
    if (nullptr != mem_loader) {
      mem_loader->~ObDirectLoadMemLoader();
      allocator_.free(mem_loader);
      mem_loader = nullptr;
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
    LOG_WARN("fail to set compactor task processor", KR(ret));
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

int ObTableLoadMemCompactor::start_dump()
{
  int ret = OB_SUCCESS;
  ObTableLoadTableCtx *ctx = store_ctx_->ctx_;
  for (int64_t i = 0; OB_SUCC(ret) && i < mem_ctx_.mem_dump_task_count_; i ++) {
    ObTableLoadTask *task = nullptr;
    // 1. 分配task
    if (OB_FAIL(ctx->alloc_task(task))) {
      LOG_WARN("fail to alloc task", KR(ret));
    }
    // 2. 设置processor
    else if (OB_FAIL(task->set_processor<MemDumpTaskProcessor>(ctx, &mem_ctx_))) {
      LOG_WARN("fail to set compactor task processor", KR(ret));
    }
    // 3. 设置callback
    else if (OB_FAIL(task->set_callback<CompactTaskCallback>(ctx, this))) {
      LOG_WARN("fail to set compactor task callback", KR(ret));
    }
    // 4. 把task放入调度器
    else if (OB_FAIL(store_ctx_->task_scheduler_->add_task(param_->session_count_ - mem_ctx_.mem_dump_task_count_ + i, task))) {
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
    LOG_WARN("fail to set compactor task processor", KR(ret));
  }
  // 3. 设置callback
  else if (OB_FAIL(task->set_callback<FinishTaskCallback>(ctx, this))) {
    LOG_WARN("fail to set compactor task callback", KR(ret));
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

int ObTableLoadMemCompactor::start_load()
{
  int ret = OB_SUCCESS;
  ObTableLoadTableCtx *ctx = store_ctx_->ctx_;
  for (int32_t session_id = 1; OB_SUCC(ret) && session_id <= get_compact_task_count(); ++session_id) {
    ObTableLoadTask *task = nullptr;
    // 1. 分配task
    if (OB_FAIL(ctx->alloc_task(task))) {
      LOG_WARN("fail to alloc task", KR(ret));
    }
    // 2. 设置processor
    else if (OB_FAIL(task->set_processor<CompactTaskProcessor>(ctx, &mem_ctx_))) {
      LOG_WARN("fail to set compact task processor", KR(ret));
    }
    // 3. 设置callback
    else if (OB_FAIL(task->set_callback<CompactTaskCallback>(ctx, this))) {
      LOG_WARN("fail to set compact task callback", KR(ret));
    }
    // 4. 把task放入调度器
    else if (OB_FAIL(store_ctx_->task_scheduler_->add_task(session_id - 1, task))) {
      LOG_WARN("fail to add task", KR(ret), K(session_id), KPC(task));
    }
    if (OB_FAIL(ret)) {
      if (nullptr != task) {
        ctx->free_task(task);
      }
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
  if (nullptr != task_scheduler_) {
    task_scheduler_->stop();
    task_scheduler_->wait();
  }
  set_has_error(); //先设置为error，因为stop的场景就是error
}

int64_t ObTableLoadMemCompactor::get_compact_task_count() const
{
  return param_->session_count_ - mem_ctx_.mem_dump_task_count_;
}

int ObTableLoadMemCompactor::handle_compact_task_finish(int ret_code)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ret_code)) {
  } else {
    const int64_t finish_task_count = ATOMIC_AAF(&finish_task_count_, 1);
    int64_t task_to_wait = param_->session_count_ + 1; // one for sample task
    if (task_to_wait == finish_task_count) {
      if (OB_LIKELY(!(mem_ctx_.has_error_)) && OB_FAIL(start_finish())) {
        LOG_WARN("fail to start finish", KR(ret));
      }
    }
  }
  return ret;
}

int ObTableLoadMemCompactor::finish()
{
  int ret = OB_SUCCESS;
  if (mem_ctx_.table_data_desc_.is_heap_table_) {
    if (OB_FAIL(build_result_for_heap_table())) {
      LOG_WARN("fail to build result", KR(ret));
    } else if (OB_FAIL(compact_ctx_->handle_table_compact_success())) {
      LOG_WARN("fail to handle_table_compact_success", KR(ret));
    }
  } else {
    if (OB_FAIL(add_table_to_parallel_merge_ctx())) {
      LOG_WARN("fail to build result", KR(ret));
    } else if (OB_FAIL(start_parallel_merge())) {
      LOG_WARN("fail to start parallel merge", KR(ret));
    }
  }
  if (OB_SUCC(ret)) {
    mem_ctx_.reset(); // mem_ctx的tables已经copy，需要提前释放
  }
  return ret;
}

int ObTableLoadMemCompactor::build_result_for_heap_table()
{
  int ret = OB_SUCCESS;
  ObTableLoadTableCompactResult &result = compact_ctx_->result_;
  for (int64_t i = 0; OB_SUCC(ret) && i < mem_ctx_.tables_.count(); i++) {
    ObIDirectLoadPartitionTable *table = mem_ctx_.tables_.at(i);
    ObDirectLoadExternalTable *external_table = nullptr;
    ObDirectLoadExternalTable *copied_external_table = nullptr;
    if (OB_ISNULL(external_table = dynamic_cast<ObDirectLoadExternalTable *>(table))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected table", KR(ret), K(i), KPC(table));
    } else if (OB_ISNULL(copied_external_table =
                           OB_NEWx(ObDirectLoadExternalTable, (&result.allocator_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new external table", KR(ret));
    } else if (OB_FAIL(copied_external_table->copy(*external_table))) {
      LOG_WARN("fail to copy external table", KR(ret));
    } else if (OB_FAIL(result.add_table(copied_external_table))) {
      LOG_WARN("fail to add tablet sstable", KR(ret));
    }
    if (OB_FAIL(ret)) {
      if (nullptr != copied_external_table) {
        copied_external_table->~ObDirectLoadExternalTable();
        copied_external_table = nullptr;
      }
    }
  }
  return ret;
}

int ObTableLoadMemCompactor::add_table_to_parallel_merge_ctx()
{
  int ret = OB_SUCCESS;
  ObTableLoadTableCompactResult &result = compact_ctx_->result_;
  for (int64_t i = 0; OB_SUCC(ret) && i < mem_ctx_.tables_.count(); i++) {
    ObIDirectLoadPartitionTable *table = mem_ctx_.tables_.at(i);
    ObDirectLoadMultipleSSTable *sstable = nullptr;
    if (OB_ISNULL(sstable = dynamic_cast<ObDirectLoadMultipleSSTable *>(table))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected table", KR(ret), K(i), KPC(table));
    } else if (OB_FAIL(parallel_merge_ctx_.add_tablet_sstable(sstable))) {
      LOG_WARN("fail to add tablet sstable", KR(ret));
    }
  }
  return ret;
}

int ObTableLoadMemCompactor::start_parallel_merge()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(parallel_merge_ctx_.start(&parallel_merge_cb_))) {
    LOG_WARN("fail to start parallel merge", KR(ret));
  }
  return ret;
}

int ObTableLoadMemCompactor::ParallelMergeCb::on_success()
{
  return compactor_->handle_merge_success();
}

int ObTableLoadMemCompactor::handle_merge_success()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(build_result())) {
    LOG_WARN("fail to build result", KR(ret));
  } else {
    ret = compact_ctx_->handle_table_compact_success();
  }
  return ret;
}

int ObTableLoadMemCompactor::build_result()
{
  int ret = OB_SUCCESS;
  ObTableLoadTableCompactResult &result = compact_ctx_->result_;
  const ObTableLoadParallelMergeCtx::TabletCtxMap &tablet_ctx_map =
    parallel_merge_ctx_.get_tablet_ctx_map();
  for (ObTableLoadParallelMergeCtx::TabletCtxIterator tablet_ctx_iter = tablet_ctx_map.begin();
       OB_SUCC(ret) && tablet_ctx_iter != tablet_ctx_map.end(); ++tablet_ctx_iter) {
    ObTableLoadParallelMergeTabletCtx *tablet_ctx = tablet_ctx_iter->second;
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ctx->sstables_.size(); ++i) {
      ObDirectLoadMultipleSSTable *sstable = tablet_ctx->sstables_.at(i);
      ObDirectLoadMultipleSSTable *copied_sstable = nullptr;
      if (OB_ISNULL(copied_sstable = OB_NEWx(ObDirectLoadMultipleSSTable, (&result.allocator_)))) {
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
  return ret;
}

} // namespace observer
} // namespace oceanbase
