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

#include "observer/table_load/ob_table_load_multiple_heap_table_compactor.h"
#include "observer/table_load/ob_table_load_merge_mem_sort_op.h"
#include "observer/table_load/ob_table_load_schema.h"
#include "observer/table_load/ob_table_load_service.h"
#include "observer/table_load/ob_table_load_stat.h"
#include "observer/table_load/ob_table_load_store_ctx.h"
#include "observer/table_load/ob_table_load_store_table_ctx.h"
#include "observer/table_load/ob_table_load_table_ctx.h"
#include "observer/table_load/ob_table_load_task.h"
#include "observer/table_load/ob_table_load_task_scheduler.h"
#include "storage/direct_load/ob_direct_load_multiple_heap_table_compactor.h"
#include "storage/direct_load/ob_direct_load_multiple_heap_table_sorter.h"

namespace oceanbase
{
namespace observer
{
using namespace common;
using namespace common::hash;
using namespace storage;
using namespace table;
using namespace blocksstable;

/**
 * ObTableLoadMultipleHeapTableCompactCompare
 */

ObTableLoadMultipleHeapTableCompactCompare::ObTableLoadMultipleHeapTableCompactCompare()
  : result_code_(OB_SUCCESS)
{
}

ObTableLoadMultipleHeapTableCompactCompare::~ObTableLoadMultipleHeapTableCompactCompare() {}

bool ObTableLoadMultipleHeapTableCompactCompare::operator()(const ObDirectLoadTableHandle lhs,
                                                            const ObDirectLoadTableHandle rhs)
{
  int ret = OB_SUCCESS;
  int cmp_ret = 0;
  if (OB_UNLIKELY(!lhs.is_valid() || !lhs.get_table()->is_multiple_heap_table() ||
                  !rhs.is_valid() || !rhs.get_table()->is_multiple_heap_table())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(lhs), K(rhs));
  } else {
    ObDirectLoadMultipleHeapTable *lhs_multi_sstable =
      static_cast<ObDirectLoadMultipleHeapTable *>(lhs.get_table());
    ObDirectLoadMultipleHeapTable *rhs_multi_sstable =
      static_cast<ObDirectLoadMultipleHeapTable *>(rhs.get_table());
    cmp_ret = lhs_multi_sstable->get_meta().index_entry_count_ -
              rhs_multi_sstable->get_meta().index_entry_count_;
  }
  if (OB_FAIL(ret)) {
    result_code_ = ret;
  }
  return cmp_ret < 0;
}

/**
 * ObTableLoadMultipleHeapTableCompactor
 */

class ObTableLoadMultipleHeapTableCompactor::CompactTaskProcessor : public ObITableLoadTaskProcessor
{
public:
  CompactTaskProcessor(ObTableLoadTask &task, ObTableLoadTableCtx *ctx,
                       ObDirectLoadMemContext *mem_ctx)
    : ObITableLoadTaskProcessor(task),
      ctx_(ctx),
      mem_ctx_(mem_ctx),
      index_dir_id_(-1),
      data_dir_id_(-1)
  {
    ctx_->inc_ref_count();
  }
  virtual ~CompactTaskProcessor()
  {
    heap_table_array_.reset();
    ObTableLoadService::put_ctx(ctx_);
  }
  int process() override
  {
    OB_TABLE_LOAD_STATISTICS_TIME_COST(INFO, table_compactor_time_us);
    int ret = OB_SUCCESS;
    // alloc dir id
    if (OB_FAIL(mem_ctx_->file_mgr_->alloc_dir(index_dir_id_))) {
      LOG_WARN("fail to alloc dir", KR(ret));
    } else if (OB_FAIL(mem_ctx_->file_mgr_->alloc_dir(data_dir_id_))) {
      LOG_WARN("fail to alloc dir", KR(ret));
    }
    // build heap table array
    while (OB_SUCC(ret) && !(mem_ctx_->has_error_)) {
      ObDirectLoadMemWorker *worker = nullptr;
      mem_ctx_->mem_loader_queue_.pop_one(worker);
      if (worker == nullptr) {
        break;
      }
      ObDirectLoadMultipleHeapTableSorter *sorter =
        static_cast<ObDirectLoadMultipleHeapTableSorter *>(worker);
      sorter->set_work_param(ctx_, index_dir_id_, data_dir_id_, &heap_table_array_);
      if (OB_FAIL(sorter->work())) {
        LOG_WARN("fail to compact", KR(ret));
      }
      worker->~ObDirectLoadMemWorker();
    }
    // compact heap table array
    if (OB_SUCC(ret) && !heap_table_array_.empty()) {
      // TODO(suzhi.yt) optimize single heap table
      if (OB_FAIL(compact_heap_table())) {
        LOG_WARN("fail to compact heap table", KR(ret));
      }
    }
    return ret;
  }

private:
  int compact_heap_table()
  {
    int ret = OB_SUCCESS;
    ObDirectLoadTableHandleArray empty_tables_handle;
    ObDirectLoadTableHandleArray *curr_round = &heap_table_array_;
    ObDirectLoadTableHandleArray *next_round = &empty_tables_handle;
    ObDirectLoadMultipleHeapTableCompactParam heap_table_compact_param;
    ObDirectLoadMultipleHeapTableCompactor heap_table_compactor;
    heap_table_compact_param.table_data_desc_ = mem_ctx_->table_data_desc_;
    heap_table_compact_param.file_mgr_ = mem_ctx_->file_mgr_;
    heap_table_compact_param.index_dir_id_ = index_dir_id_;
    if (OB_FAIL(heap_table_compactor.init(heap_table_compact_param))) {
      LOG_WARN("fail to init heap table compactor", KR(ret));
    }
    while (OB_SUCC(ret) && curr_round->count() > mem_ctx_->merge_count_per_round_) {
      if (OB_FAIL(compact_heap_table_one_round(heap_table_compactor, curr_round, next_round))) {
        LOG_WARN("fail to compact heap table one round", KR(ret));
      } else {
        heap_table_compactor.reuse();
        std::swap(curr_round, next_round);
        ATOMIC_AAF(&ctx_->job_stat_->store_.compact_stage_consume_tmp_files_,
                   mem_ctx_->merge_count_per_round_ - 1);
      }
    }
    if (OB_SUCC(ret)) {
      abort_unless(curr_round->count() > 0 &&
                   curr_round->count() <= mem_ctx_->merge_count_per_round_);
      for (int64_t i = 0; OB_SUCC(ret) && i < curr_round->count(); ++i) {
        ObDirectLoadTableHandle heap_table_handle;
        if (OB_FAIL(curr_round->get_table(i, heap_table_handle))) {
          LOG_WARN("fail to get table", KR(ret));
        } else if (OB_FAIL(heap_table_compactor.add_table(heap_table_handle))) {
          LOG_WARN("fail to add table", KR(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(heap_table_compactor.compact())) {
          LOG_WARN("fail to do compact", KR(ret));
        } else if (OB_FAIL(mem_ctx_->add_tables_from_table_compactor(heap_table_compactor))) {
          LOG_WARN("fail to add table from table compactor", KR(ret));
        } else {
          ATOMIC_AAF(&ctx_->job_stat_->store_.compact_stage_consume_tmp_files_,
                     curr_round->count());
        }
      }
    }
    // release heap table array
    curr_round->reset();
    next_round->reset();
    return ret;
  }
  int compact_heap_table_one_round(ObDirectLoadMultipleHeapTableCompactor &heap_table_compactor,
                                   ObDirectLoadTableHandleArray *curr_round,
                                   ObDirectLoadTableHandleArray *next_round)
  {
    int ret = OB_SUCCESS;
    // sort heap table array
    ObTableLoadMultipleHeapTableCompactCompare compare;
    lib::ob_sort(curr_round->begin(), curr_round->end(), compare);
    // compact top merge_count_per_round heap table
    for (int64_t i = 0; OB_SUCC(ret) && i < mem_ctx_->merge_count_per_round_; ++i) {
      ObDirectLoadTableHandle heap_table_handle;
      if (OB_FAIL(curr_round->get_table(i, heap_table_handle))) {
        LOG_WARN("fail to get table", KR(ret));
      } else if (OB_FAIL(heap_table_compactor.add_table(heap_table_handle))) {
        LOG_WARN("fail to add table", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      ObDirectLoadTableHandle compacted_table_handle;
      if (OB_FAIL(heap_table_compactor.compact())) {
        LOG_WARN("fail to do compact", KR(ret));
      } else if (OB_FAIL(
                   heap_table_compactor.get_table(compacted_table_handle, mem_ctx_->table_mgr_))) {
        LOG_WARN("fail to get table", KR(ret));
      } else if (OB_FAIL(next_round->add(compacted_table_handle))) {
        LOG_WARN("fail to push back", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      for (int64_t i = mem_ctx_->merge_count_per_round_; OB_SUCC(ret) && i < curr_round->count();
           ++i) {
        ObDirectLoadTableHandle heap_table_handle;
        if (OB_FAIL(curr_round->get_table(i, heap_table_handle))) {
          LOG_WARN("fail to get table", KR(ret));
        } else if (OB_FAIL(next_round->add(heap_table_handle))) {
          LOG_WARN("fail to add table", KR(ret));
        }
      }
      if (OB_FAIL(ret)) {
        // clear next round
        next_round->reset();
      }
    }
    if (OB_SUCC(ret)) {
      // clear current round
      curr_round->reset();
    }
    return ret;
  }

private:
  ObTableLoadTableCtx *const ctx_;
  ObDirectLoadMemContext *mem_ctx_;
  int64_t index_dir_id_;
  int64_t data_dir_id_;
  ObDirectLoadTableHandleArray heap_table_array_;
};

class ObTableLoadMultipleHeapTableCompactor::CompactTaskCallback : public ObITableLoadTaskCallback
{
public:
  CompactTaskCallback(ObTableLoadTableCtx *ctx, ObTableLoadMultipleHeapTableCompactor *compactor)
    : ctx_(ctx), compactor_(compactor)
  {
    ctx_->inc_ref_count();
  }
  virtual ~CompactTaskCallback() { ObTableLoadService::put_ctx(ctx_); }
  void callback(int ret_code, ObTableLoadTask *task) override
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(compactor_->handle_compact_task_finish(ret_code))) {
      LOG_WARN("fail to handle compact task finish", KR(ret));
    }
    if (OB_FAIL(ret)) {
      ctx_->store_ctx_->set_status_error(ret);
    }
    ctx_->free_task(task);
    OB_TABLE_LOAD_STATISTICS_PRINT_AND_RESET();
  }

private:
  ObTableLoadTableCtx *const ctx_;
  ObTableLoadMultipleHeapTableCompactor *const compactor_;
};

ObTableLoadMultipleHeapTableCompactor::ObTableLoadMultipleHeapTableCompactor()
  : store_ctx_(nullptr),
    store_table_ctx_(nullptr),
    op_(nullptr),
    allocator_("TLD_MemC"),
    is_inited_(false)
{
  allocator_.set_tenant_id(MTL_ID());
}

ObTableLoadMultipleHeapTableCompactor::~ObTableLoadMultipleHeapTableCompactor() { reset(); }

void ObTableLoadMultipleHeapTableCompactor::reset()
{
  is_inited_ = false;
  store_ctx_ = nullptr;
  store_table_ctx_ = nullptr;
  op_ = nullptr;
  mem_ctx_.reset();
  allocator_.reset();
}

int ObTableLoadMultipleHeapTableCompactor::init(ObTableLoadMergeMemSortOp *op)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadMultipleHeapTableCompactor init twice", KR(ret), KP(this));
  } else if (OB_ISNULL(op)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(op));
  } else {
    store_ctx_ = op->store_ctx_;
    store_table_ctx_ = op->merge_table_ctx_->store_table_ctx_;
    op_ = op;

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
    mem_ctx_.dump_thread_cnt_ = 0;
    mem_ctx_.load_thread_cnt_ = mem_ctx_.total_thread_cnt_;

    if (OB_FAIL(mem_ctx_.init())) {
      LOG_WARN("fail to init compactor ctx", KR(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObTableLoadMultipleHeapTableCompactor::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadMultipleHeapTableCompactor not init", KR(ret), KP(this));
  } else {
    if (OB_FAIL(construct_compactors())) {
      LOG_WARN("fail to construct compactors", KR(ret));
    } else if (OB_FAIL(start_sort())) {
      LOG_WARN("fail to start sort", KR(ret));
    }
  }
  if (OB_FAIL(ret)) {
    set_has_error();
  }
  return ret;
}

int ObTableLoadMultipleHeapTableCompactor::construct_compactors()
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

int ObTableLoadMultipleHeapTableCompactor::add_tablet_table(
  const ObDirectLoadTableHandle &table_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!table_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(table_handle));
  } else {
    ObDirectLoadMultipleHeapTableSorter *sorter = nullptr;
    if (OB_ISNULL(sorter =
                    OB_NEWx(ObDirectLoadMultipleHeapTableSorter, (&allocator_), &mem_ctx_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObDirectLoadMultipleHeapTableSorter", KR(ret));
    } else if (OB_FAIL(sorter->add_table(table_handle))) {
      LOG_WARN("fail to add table", KR(ret));
    } else if (OB_FAIL(mem_ctx_.mem_loader_queue_.push(sorter))) {
      LOG_WARN("fail to push", KR(ret));
    }
    if (OB_FAIL(ret)) {
      if (nullptr != sorter) {
        sorter->~ObDirectLoadMultipleHeapTableSorter();
        allocator_.free(sorter);
        sorter = nullptr;
      }
    }
  }
  return ret;
}

int ObTableLoadMultipleHeapTableCompactor::start_sort()
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
    else if (OB_FAIL(task->set_processor<CompactTaskProcessor>(ctx, &mem_ctx_))) {
      LOG_WARN("fail to set compact task processor", KR(ret));
    }
    // 3. 设置callback
    else if (OB_FAIL(task->set_callback<CompactTaskCallback>(ctx, this))) {
      LOG_WARN("fail to set compact task callback", KR(ret));
    }
    // 4. 把task放入调度器
    else if (OB_FAIL(store_ctx_->task_scheduler_->add_task(i, task))) {
      LOG_WARN("fail to add task", KR(ret), K(i), KPC(task));
    }
    if (OB_FAIL(ret)) {
      if (nullptr != task) {
        ctx->free_task(task);
      }
    }
  }
  return ret;
}

void ObTableLoadMultipleHeapTableCompactor::stop()
{
  set_has_error(); //先设置为error，因为stop的场景就是error
}

int ObTableLoadMultipleHeapTableCompactor::handle_compact_task_finish(int ret_code)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ret_code)) {
    set_has_error();
  } else {
    const int64_t finish_load_thread_cnt = ATOMIC_AAF(&mem_ctx_.finish_load_thread_cnt_, 1);
    if (finish_load_thread_cnt == mem_ctx_.load_thread_cnt_) {
      if (OB_LIKELY(!(mem_ctx_.has_error_)) && OB_FAIL(finish())) {
        LOG_WARN("fail to start finish", KR(ret));
      }
    }
  }
  return ret;
}

int ObTableLoadMultipleHeapTableCompactor::finish()
{
  int ret = OB_SUCCESS;
  ObDirectLoadTableStore *table_store = op_->merge_table_ctx_->table_store_;
  // 将排序后的数据保存到table_store
  table_store->clear();
  table_store->set_table_data_desc(mem_ctx_.table_data_desc_);
  table_store->set_multiple_heap_table();
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
