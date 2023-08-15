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
#include "observer/table_load/ob_table_load_error_row_handler.h"
#include "observer/table_load/ob_table_load_service.h"
#include "observer/table_load/ob_table_load_stat.h"
#include "observer/table_load/ob_table_load_store_ctx.h"
#include "observer/table_load/ob_table_load_table_ctx.h"
#include "observer/table_load/ob_table_load_task.h"
#include "observer/table_load/ob_table_load_task_scheduler.h"
#include "observer/table_load/ob_table_load_trans_store.h"
#include "storage/direct_load/ob_direct_load_multiple_heap_table_builder.h"
#include "storage/direct_load/ob_direct_load_multiple_heap_table_compactor.h"

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

ObTableLoadMultipleHeapTableCompactCompare::~ObTableLoadMultipleHeapTableCompactCompare()
{
}

bool ObTableLoadMultipleHeapTableCompactCompare::operator()(
  const ObDirectLoadMultipleHeapTable *lhs,
  const ObDirectLoadMultipleHeapTable *rhs)
{
  int ret = OB_SUCCESS;
  int cmp_ret = 0;
  if (OB_UNLIKELY(nullptr == lhs || nullptr == rhs || !lhs->is_valid() || !rhs->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(lhs), K(rhs));
  } else {
    cmp_ret = lhs->get_meta().index_entry_count_ - rhs->get_meta().index_entry_count_;
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
  CompactTaskProcessor(ObTableLoadTask &task,
                       ObTableLoadTableCtx *ctx,
                       ObDirectLoadMemContext *mem_ctx)
    : ObITableLoadTaskProcessor(task),
      ctx_(ctx),
      mem_ctx_(mem_ctx),
      index_dir_id_(-1),
      data_dir_id_(-1),
      heap_table_allocator_("TLD_MHTCompact")
  {
    ctx_->inc_ref_count();
    heap_table_allocator_.set_tenant_id(MTL_ID());
  }
  virtual ~CompactTaskProcessor()
  {
    for (int64_t i = 0; i < heap_table_array_.count(); ++i) {
      ObDirectLoadMultipleHeapTable *heap_table = heap_table_array_.at(i);
      heap_table->~ObDirectLoadMultipleHeapTable();
    }
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
        dynamic_cast<ObDirectLoadMultipleHeapTableSorter *>(worker);
      if (OB_ISNULL(sorter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected worker", KR(ret), KPC(worker));
      } else if (FALSE_IT(sorter->set_work_param(index_dir_id_, data_dir_id_, heap_table_array_,
                                                 heap_table_allocator_))) {
      } else if (OB_FAIL(sorter->work())) {
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
    ObArray<ObDirectLoadMultipleHeapTable *> tmp_heap_table_array;
    ObArray<ObDirectLoadMultipleHeapTable *> *curr_round = &heap_table_array_;
    ObArray<ObDirectLoadMultipleHeapTable *> *next_round = &tmp_heap_table_array;
    ObDirectLoadMultipleHeapTableCompactParam heap_table_compact_param;
    ObDirectLoadMultipleHeapTableCompactor heap_table_compactor;
    heap_table_compact_param.table_data_desc_ = mem_ctx_->table_data_desc_;
    heap_table_compact_param.file_mgr_ = mem_ctx_->file_mgr_;
    heap_table_compact_param.index_dir_id_ = index_dir_id_;
    if (OB_FAIL(heap_table_compactor.init(heap_table_compact_param))) {
      LOG_WARN("fail to init heap table compactor", KR(ret));
    }
    while (OB_SUCC(ret) &&
           curr_round->count() > mem_ctx_->table_data_desc_.merge_count_per_round_) {
      if (OB_FAIL(compact_heap_table_one_round(heap_table_compactor, curr_round, next_round))) {
        LOG_WARN("fail to compact heap table one round", KR(ret));
      } else {
        heap_table_compactor.reuse();
        std::swap(curr_round, next_round);
      }
    }
    if (OB_SUCC(ret)) {
      abort_unless(curr_round->count() > 0 &&
                   curr_round->count() <= mem_ctx_->table_data_desc_.merge_count_per_round_);
      for (int64_t i = 0; OB_SUCC(ret) && i < curr_round->count(); ++i) {
        ObDirectLoadMultipleHeapTable *heap_table = curr_round->at(i);
        if (OB_FAIL(heap_table_compactor.add_table(heap_table))) {
          LOG_WARN("fail to add table", KR(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(heap_table_compactor.compact())) {
          LOG_WARN("fail to do compact", KR(ret));
        } else if (OB_FAIL(mem_ctx_->add_tables_from_table_compactor(heap_table_compactor))) {
          LOG_WARN("fail to add table from table compactor", KR(ret));
        }
      }
    }
    // release heap table array
    for (int64_t i = 0; i < curr_round->count(); ++i) {
      ObDirectLoadMultipleHeapTable *heap_table = curr_round->at(i);
      heap_table->~ObDirectLoadMultipleHeapTable();
    }
    curr_round->reset();
    for (int64_t i = 0; i < next_round->count(); ++i) {
      ObDirectLoadMultipleHeapTable *heap_table = next_round->at(i);
      heap_table->~ObDirectLoadMultipleHeapTable();
    }
    next_round->reset();
    return ret;
  }
  int compact_heap_table_one_round(ObDirectLoadMultipleHeapTableCompactor &heap_table_compactor,
                                   ObArray<ObDirectLoadMultipleHeapTable *> *curr_round,
                                   ObArray<ObDirectLoadMultipleHeapTable *> *next_round)
  {
    int ret = OB_SUCCESS;
    // sort heap table array
    ObTableLoadMultipleHeapTableCompactCompare compare;
    std::sort(curr_round->begin(), curr_round->end(), compare);
    // compact top merge_count_per_round heap table
    for (int64_t i = 0; OB_SUCC(ret) && i < mem_ctx_->table_data_desc_.merge_count_per_round_;
         ++i) {
      ObDirectLoadMultipleHeapTable *heap_table = curr_round->at(i);
      if (OB_FAIL(heap_table_compactor.add_table(heap_table))) {
        LOG_WARN("fail to add table", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      ObIDirectLoadPartitionTable *compacted_table = nullptr;
      ObDirectLoadMultipleHeapTable *compacted_heap_table = nullptr;
      if (OB_FAIL(heap_table_compactor.compact())) {
        LOG_WARN("fail to do compact", KR(ret));
      } else if (OB_FAIL(heap_table_compactor.get_table(compacted_table, allocator_))) {
        LOG_WARN("fail to get table", KR(ret));
      } else if (OB_ISNULL(compacted_heap_table =
                             dynamic_cast<ObDirectLoadMultipleHeapTable *>(compacted_table))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected table", KR(ret), KPC(compacted_table));
      } else if (OB_FAIL(next_round->push_back(compacted_heap_table))) {
        LOG_WARN("fail to push back", KR(ret));
      }
      if (OB_FAIL(ret)) {
        if (nullptr != compacted_table) {
          compacted_table->~ObIDirectLoadPartitionTable();
          allocator_.free(compacted_table);
          compacted_table = nullptr;
        }
      }
    }
    if (OB_SUCC(ret)) {
      for (int64_t i = mem_ctx_->table_data_desc_.merge_count_per_round_;
           OB_SUCC(ret) && i < curr_round->count(); ++i) {
        ObDirectLoadMultipleHeapTable *heap_table = curr_round->at(i);
        if (OB_FAIL(next_round->push_back(heap_table))) {
          LOG_WARN("fail to push back", KR(ret));
        }
      }
      if (OB_FAIL(ret)) {
        // clear next round
        ObDirectLoadMultipleHeapTable *compacted_heap_table = next_round->at(0);
        compacted_heap_table->~ObDirectLoadMultipleHeapTable();
        next_round->reset();
      }
    }
    if (OB_SUCC(ret)) {
      // clear current round
      for (int64_t i = 0; i < mem_ctx_->table_data_desc_.merge_count_per_round_; ++i) {
        ObDirectLoadMultipleHeapTable *heap_table = curr_round->at(i);
        heap_table->~ObDirectLoadMultipleHeapTable();
      }
      curr_round->reset();
    }
    return ret;
  }
private:
  ObTableLoadTableCtx * const ctx_;
  ObDirectLoadMemContext *mem_ctx_;
  int64_t index_dir_id_;
  int64_t data_dir_id_;
  ObArenaAllocator heap_table_allocator_;
  ObArray<ObDirectLoadMultipleHeapTable *> heap_table_array_;
};

class ObTableLoadMultipleHeapTableCompactor::CompactTaskCallback : public ObITableLoadTaskCallback
{
public:
  CompactTaskCallback(ObTableLoadTableCtx *ctx, ObTableLoadMultipleHeapTableCompactor *compactor)
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
    param_(nullptr),
    allocator_("TLD_MemC"),
    finish_task_count_(0)
{
}

ObTableLoadMultipleHeapTableCompactor::~ObTableLoadMultipleHeapTableCompactor()
{
  reset();
}

void ObTableLoadMultipleHeapTableCompactor::reset()
{
  store_ctx_ = nullptr;
  param_ = nullptr;
  finish_task_count_ = 0;
  mem_ctx_.reset();
  allocator_.reset();
}

int ObTableLoadMultipleHeapTableCompactor::inner_init()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  store_ctx_ = compact_ctx_->store_ctx_;
  param_ = &(store_ctx_->ctx_->param_);
  allocator_.set_tenant_id(tenant_id);

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

  if (OB_SUCC(ret)) {
    if (OB_FAIL(mem_ctx_.init())) {
      LOG_WARN("fail to init compactor ctx", KR(ret));
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
    } else if (OB_FAIL(start_compact())) {
      LOG_WARN("fail to start compact", KR(ret));
    }
  }
  return ret;
}

int ObTableLoadMultipleHeapTableCompactor::construct_compactors()
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

int ObTableLoadMultipleHeapTableCompactor::add_tablet_table(ObIDirectLoadPartitionTable *table)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(table));
  } else {
    ObDirectLoadMultipleHeapTableSorter *sorter = nullptr;
    if (OB_FAIL(create_heap_table_sorter(sorter))) {
      LOG_WARN("fail to create tablet table compactor", KR(ret));
    } else if (OB_FAIL(mem_ctx_.mem_loader_queue_.push(sorter))) {
      LOG_WARN("fail to push", KR(ret));
    } else if (OB_FAIL(sorter->add_table(table))) {
      LOG_WARN("fail to add table", KR(ret));
    }
  }
  return ret;
}

int ObTableLoadMultipleHeapTableCompactor::create_heap_table_sorter(ObDirectLoadMultipleHeapTableSorter *&heap_table_sorter)
{
  int ret = OB_SUCCESS;
  heap_table_sorter = nullptr;
  if (OB_ISNULL(heap_table_sorter =
                  OB_NEWx(ObDirectLoadMultipleHeapTableSorter, (&allocator_), &mem_ctx_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to new ObDirectLoadMultipleHeapTableSorter", KR(ret));
  } else if (OB_FAIL(heap_table_sorter->init())) {
    LOG_WARN("fail to init sorter", KR(ret));
  }
  if (OB_FAIL(ret)) {
    if (nullptr != heap_table_sorter) {
      heap_table_sorter->~ObDirectLoadMultipleHeapTableSorter();
      allocator_.free(heap_table_sorter);
      heap_table_sorter = nullptr;
    }
  }
  return ret;
}


int ObTableLoadMultipleHeapTableCompactor::start_sort()
{
  int ret = OB_SUCCESS;
  ObTableLoadTableCtx *ctx = store_ctx_->ctx_;
  for (int32_t session_id = 1; OB_SUCC(ret) && session_id <= param_->session_count_; ++session_id) {
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

int ObTableLoadMultipleHeapTableCompactor::start_compact()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(start_sort())) {
    LOG_WARN("fail to start load", KR(ret));
  }
  if (OB_FAIL(ret)) {
    set_has_error();
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
    const int64_t task_to_wait = param_->session_count_;
    const int64_t finish_task_count = ATOMIC_AAF(&finish_task_count_, 1);
    if (task_to_wait == finish_task_count) {
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
  if (OB_FAIL(build_result_for_heap_table())) {
    LOG_WARN("fail to build result", KR(ret));
  } else if (OB_FAIL(compact_ctx_->handle_table_compact_success())) {
    LOG_WARN("fail to handle_table_compact_success", KR(ret));
  }
  if (OB_SUCC(ret)) {
    mem_ctx_.reset(); // mem_ctx的tables已经copy，需要提前释放
  }
  return ret;
}

int ObTableLoadMultipleHeapTableCompactor::build_result_for_heap_table()
{
  int ret = OB_SUCCESS;
  ObTableLoadTableCompactResult &result = compact_ctx_->result_;
  for (int64_t i = 0; OB_SUCC(ret) && i < mem_ctx_.tables_.count(); i++) {
    ObIDirectLoadPartitionTable *table = mem_ctx_.tables_.at(i);
    ObDirectLoadMultipleHeapTable *multi_heap_sstable = nullptr;
    ObDirectLoadMultipleHeapTable *copied_multi_heap_sstable = nullptr;
    if (OB_ISNULL(multi_heap_sstable = dynamic_cast<ObDirectLoadMultipleHeapTable *>(table))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected table", KR(ret), K(i), KPC(table));
    } else if (OB_ISNULL(copied_multi_heap_sstable =
                           OB_NEWx(ObDirectLoadMultipleHeapTable, (&result.allocator_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new external table", KR(ret));
    } else if (OB_FAIL(copied_multi_heap_sstable->copy(*multi_heap_sstable))) {
      LOG_WARN("fail to copy external table", KR(ret));
    } else if (OB_FAIL(result.add_table(copied_multi_heap_sstable))) {
      LOG_WARN("fail to add tablet sstable", KR(ret));
    }
    if (OB_FAIL(ret)) {
      if (nullptr != copied_multi_heap_sstable) {
        copied_multi_heap_sstable->~ObDirectLoadMultipleHeapTable();
        copied_multi_heap_sstable = nullptr;
      }
    }
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
