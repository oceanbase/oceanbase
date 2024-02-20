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

#include "observer/table_load/ob_table_load_general_table_compactor.h"
#include "observer/table_load/ob_table_load_service.h"
#include "observer/table_load/ob_table_load_stat.h"
#include "observer/table_load/ob_table_load_store_ctx.h"
#include "observer/table_load/ob_table_load_table_ctx.h"
#include "observer/table_load/ob_table_load_task.h"
#include "observer/table_load/ob_table_load_task_scheduler.h"
#include "observer/table_load/ob_table_load_trans_store.h"
#include "storage/direct_load/ob_direct_load_external_table_compactor.h"
#include "storage/direct_load/ob_direct_load_sstable_compactor.h"

namespace oceanbase
{
namespace observer
{
using namespace blocksstable;
using namespace common;
using namespace common::hash;
using namespace lib;
using namespace storage;
using namespace table;

class ObTableLoadGeneralTableCompactor::CompactTaskProcessor : public ObITableLoadTaskProcessor
{
public:
  CompactTaskProcessor(ObTableLoadTask &task, ObTableLoadTableCtx *ctx,
                       ObTableLoadGeneralTableCompactor *compactor)
    : ObITableLoadTaskProcessor(task), ctx_(ctx), compactor_(compactor)
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
    CompactorTask *compactor_task = nullptr;
    while (OB_SUCC(ret)) {
      compactor_task = nullptr;
      if (OB_FAIL(compactor_->get_next_compactor_task(compactor_task))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to get next compactor task", KR(ret));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else if (OB_FAIL(compactor_task->process())) {
        LOG_WARN("fail to process compactor task", KR(ret));
      }
      if (nullptr != compactor_task) {
        compactor_->handle_compactor_task_finish(compactor_task);
      }
    }
    return ret;
  }
private:
  ObTableLoadTableCtx *const ctx_;
  ObTableLoadGeneralTableCompactor *const compactor_;
};

class ObTableLoadGeneralTableCompactor::CompactTaskCallback : public ObITableLoadTaskCallback
{
public:
  CompactTaskCallback(ObTableLoadTableCtx *ctx, ObTableLoadGeneralTableCompactor *compactor)
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
    if (OB_FAIL(compactor_->handle_compact_thread_finish(ret_code))) {
      LOG_WARN("fail to handle compact thread finish", KR(ret));
    }
    if (OB_FAIL(ret)) {
      ctx_->store_ctx_->set_status_error(ret);
    }
    ctx_->free_task(task);
    OB_TABLE_LOAD_STATISTICS_PRINT_AND_RESET();
  }
private:
  ObTableLoadTableCtx *const ctx_;
  ObTableLoadGeneralTableCompactor *const compactor_;
};

/**
 * CompactorTask
 */

ObTableLoadGeneralTableCompactor::CompactorTask::CompactorTask(
  ObIDirectLoadTabletTableCompactor *table_compactor)
  : table_compactor_(table_compactor)
{
}

ObTableLoadGeneralTableCompactor::CompactorTask::~CompactorTask()
{
}

int ObTableLoadGeneralTableCompactor::CompactorTask::add_table(ObIDirectLoadPartitionTable *table)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == table_compactor_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null table compactor", KR(ret), KP(table_compactor_));
  } else if (OB_UNLIKELY(nullptr == table)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(table));
  } else if (OB_FAIL(table_compactor_->add_table(table))) {
    LOG_WARN("fail to add table", KR(ret));
  }
  return ret;
}

int ObTableLoadGeneralTableCompactor::CompactorTask::process()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == table_compactor_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null table compactor", KR(ret), KP(table_compactor_));
  } else if (OB_FAIL(table_compactor_->compact())) {
    LOG_WARN("fail to do compact", KR(ret));
  }
  return ret;
}

void ObTableLoadGeneralTableCompactor::CompactorTask::stop()
{
  if (OB_NOT_NULL(table_compactor_)) {
    table_compactor_->stop();
  }
}

/**
 * CompactorTaskIter
 */

ObTableLoadGeneralTableCompactor::CompactorTaskIter::CompactorTaskIter()
  : pos_(0)
{
  compactor_task_array_.set_tenant_id(MTL_ID());
}

ObTableLoadGeneralTableCompactor::CompactorTaskIter::~CompactorTaskIter()
{
  reset();
}

void ObTableLoadGeneralTableCompactor::CompactorTaskIter::reset()
{
  for (int64_t i = 0; i < compactor_task_array_.count(); ++i) {
    CompactorTask *compactor_task = compactor_task_array_.at(i);
    compactor_task->~CompactorTask();
  }
  compactor_task_array_.reset();
}

int ObTableLoadGeneralTableCompactor::CompactorTaskIter::add(CompactorTask *compactor_task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == compactor_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(compactor_task));
  } else if (OB_FAIL(compactor_task_array_.push_back(compactor_task))) {
    LOG_WARN("fail to push back", KR(ret));
  }
  return ret;
}

int ObTableLoadGeneralTableCompactor::CompactorTaskIter::get_next_compactor_task(
  CompactorTask *&compactor_task)
{
  int ret = OB_SUCCESS;
  if (pos_ >= compactor_task_array_.count()) {
    ret = OB_ITER_END;
  } else {
    compactor_task = compactor_task_array_.at(pos_++);
  }
  return ret;
}

/**
 * ObTableLoadGeneralTableCompactor
 */

ObTableLoadGeneralTableCompactor::ObTableLoadGeneralTableCompactor()
  : store_ctx_(nullptr),
    param_(nullptr),
    allocator_("TLD_GeneralTC"),
    running_thread_count_(0),
    has_error_(false),
    is_stop_(false)
{
  allocator_.set_tenant_id(MTL_ID());
  all_compactor_array_.set_tenant_id(MTL_ID());
}

ObTableLoadGeneralTableCompactor::~ObTableLoadGeneralTableCompactor()
{
  reset();
}

void ObTableLoadGeneralTableCompactor::reset()
{
  abort_unless(compacting_list_.is_empty());
  store_ctx_ = nullptr;
  param_ = nullptr;
  for (int64_t i = 0; i < all_compactor_array_.count(); ++i) {
    ObIDirectLoadTabletTableCompactor *compactor = all_compactor_array_.at(i);
    compactor->~ObIDirectLoadTabletTableCompactor();
    allocator_.free(compactor);
  }
  all_compactor_array_.reset();
  compactor_task_iter_.reset();
  allocator_.reset();
  has_error_ = false;
  is_stop_ = false;
}

int ObTableLoadGeneralTableCompactor::inner_init()
{
  int ret = OB_SUCCESS;
  store_ctx_ = compact_ctx_->store_ctx_;
  param_ = &store_ctx_->ctx_->param_;
  return ret;
}

int ObTableLoadGeneralTableCompactor::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadGeneralTableCompactor not init", KR(ret), KP(this));
  } else {
    if (OB_FAIL(construct_compactors())) {
      LOG_WARN("fail to construct compactors", KR(ret));
    } else if (OB_FAIL(start_compact())) {
      LOG_WARN("fail to start compact", KR(ret));
    }
  }
  return ret;
}

void ObTableLoadGeneralTableCompactor::stop()
{
  LOG_WARN_RET(OB_SUCCESS, "LOAD TABLE COMPACT STOP");
  is_stop_ = true;
  // 遍历合并中的任务队列, 调用stop
  ObMutexGuard guard(mutex_);
  CompactorTask *compactor_task = nullptr;
  DLIST_FOREACH_NORET(compactor_task, compacting_list_)
  {
    compactor_task->stop();
  }
}

int ObTableLoadGeneralTableCompactor::construct_compactors()
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("TLD_Tmp");
  CompactorTaskMap *compactor_task_map_array = nullptr;
  ObArray<ObTableLoadTransStore *> trans_store_array;
  allocator.set_tenant_id(MTL_ID());
  trans_store_array.set_block_allocator(ModulePageAllocator(allocator));
  if (OB_FAIL(store_ctx_->get_committed_trans_stores(trans_store_array))) {
    LOG_WARN("fail to get committed trans stores", KR(ret));
  } else if (OB_ISNULL(compactor_task_map_array = static_cast<CompactorTaskMap *>(
                         allocator.alloc(sizeof(CompactorTaskMap) * param_->write_session_count_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", KR(ret));
  } else {
    new (compactor_task_map_array) CompactorTaskMap[param_->write_session_count_];
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < param_->write_session_count_; ++i) {
    CompactorTaskMap &compactor_map = compactor_task_map_array[i];
    if (OB_FAIL(compactor_map.create(1024, "TLD_CT_Map", "TLD_CT_Map", MTL_ID()))) {
      LOG_WARN("fail to create compactor map", KR(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < trans_store_array.count(); ++i) {
    ObTableLoadTransStore *trans_store = trans_store_array.at(i);
    for (int64_t j = 0; OB_SUCC(ret) && j < trans_store->session_store_array_.count(); ++j) {
      const ObTableLoadTransStore::SessionStore *session_store =
        trans_store->session_store_array_.at(j);
      CompactorTaskMap &compactor_map = compactor_task_map_array[session_store->session_id_ - 1];
      for (int64_t k = 0; OB_SUCC(ret) && k < session_store->partition_table_array_.count(); ++k) {
        ObIDirectLoadPartitionTable *table = session_store->partition_table_array_.at(k);
        if (OB_FAIL(add_tablet_table(session_store->session_id_, compactor_map, table))) {
          LOG_WARN("fail to add tablet table", KR(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    store_ctx_->clear_committed_trans_stores();
  }
  if (nullptr != compactor_task_map_array) {
    for (int64_t i = 0; i < param_->write_session_count_; ++i) {
      CompactorTaskMap *compactor_map = compactor_task_map_array + i;
      compactor_map->~CompactorTaskMap();
    }
  }
  return ret;
}

int ObTableLoadGeneralTableCompactor::add_tablet_table(int32_t session_id,
                                                       CompactorTaskMap &compactor_task_map,
                                                       ObIDirectLoadPartitionTable *table)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(table));
  } else {
    const ObTabletID &tablet_id = table->get_tablet_id();
    CompactorTask *compactor_task = nullptr;
    if (OB_FAIL(compactor_task_map.get_refactored(tablet_id, compactor_task))) {
      if (OB_UNLIKELY(OB_HASH_NOT_EXIST != ret)) {
        LOG_WARN("fail to get refactored", KR(ret));
      } else {
        ret = OB_SUCCESS;
        if (OB_FAIL(create_tablet_compactor_task(session_id, tablet_id, compactor_task))) {
          LOG_WARN("fail to create tablet compactor task", KR(ret));
        } else if (OB_FAIL(compactor_task_map.set_refactored(tablet_id, compactor_task))) {
          LOG_WARN("fail to set refactored", KR(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(compactor_task->add_table(table))) {
        LOG_WARN("fail to add table", KR(ret));
      }
    }
  }
  return ret;
}

int ObTableLoadGeneralTableCompactor::create_tablet_table_compactor(
  int32_t session_id, const ObTabletID &tablet_id,
  ObIDirectLoadTabletTableCompactor *&table_compactor)
{
  int ret = OB_SUCCESS;
  table_compactor = nullptr;
  if (store_ctx_->ctx_->schema_.is_heap_table_) {
    ObDirectLoadExternalTableCompactParam compact_param;
    compact_param.tablet_id_ = tablet_id;
    compact_param.table_data_desc_ = store_ctx_->table_data_desc_;
    ObDirectLoadExternalTableCompactor *external_table_compactor = nullptr;
    if (OB_ISNULL(external_table_compactor =
                    OB_NEWx(ObDirectLoadExternalTableCompactor, (&allocator_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObDirectLoadExternalTableCompactor", KR(ret));
    } else if (OB_FAIL(external_table_compactor->init(compact_param))) {
      LOG_WARN("fail to init external table compactor", KR(ret));
    }
    table_compactor = external_table_compactor;
  } else if (!param_->need_sort_) {
    ObDirectLoadSSTableCompactParam compact_param;
    compact_param.tablet_id_ = tablet_id;
    compact_param.table_data_desc_ = store_ctx_->table_data_desc_;
    compact_param.datum_utils_ = &(store_ctx_->ctx_->schema_.datum_utils_);
    ObDirectLoadSSTableCompactor *sstable_compactor = nullptr;
    if (OB_ISNULL(sstable_compactor = OB_NEWx(ObDirectLoadSSTableCompactor, (&allocator_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObDirectLoadSSTableCompactor", KR(ret));
    } else if (OB_FAIL(sstable_compactor->init(compact_param))) {
      LOG_WARN("fail to init sstable compactor", KR(ret));
    }
    table_compactor = sstable_compactor;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected table compactor", KR(ret));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(all_compactor_array_.push_back(table_compactor))) {
      LOG_WARN("fail to push back", KR(ret));
    }
  }
  if (OB_FAIL(ret)) {
    if (nullptr != table_compactor) {
      table_compactor->~ObIDirectLoadTabletTableCompactor();
      allocator_.free(table_compactor);
      table_compactor = nullptr;
    }
  }
  return ret;
}

int ObTableLoadGeneralTableCompactor::create_tablet_compactor_task(int32_t session_id,
                                                                   const ObTabletID &tablet_id,
                                                                   CompactorTask *&compactor_task)
{
  int ret = OB_SUCCESS;
  compactor_task = nullptr;
  ObIDirectLoadTabletTableCompactor *table_compactor = nullptr;
  if (OB_FAIL(create_tablet_table_compactor(session_id, tablet_id, table_compactor))) {
    LOG_WARN("fail to create tablet table compactor", KR(ret));
  } else if (OB_ISNULL(compactor_task = OB_NEWx(CompactorTask, (&allocator_), table_compactor))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to new CompactorTask", KR(ret));
  } else if (OB_FAIL(compactor_task_iter_.add(compactor_task))) {
    LOG_WARN("fail to add compactor task", KR(ret));
  }
  if (OB_FAIL(ret)) {
    if (nullptr != compactor_task) {
      compactor_task->~CompactorTask();
      compactor_task = nullptr;
    }
  }
  return ret;
}

int ObTableLoadGeneralTableCompactor::start_compact()
{
  int ret = OB_SUCCESS;
  const int64_t thread_count = store_ctx_->task_scheduler_->get_thread_count();
  ObTableLoadTableCtx *ctx = store_ctx_->ctx_;
  running_thread_count_ = thread_count;
  for (int32_t thread_idx = 0; OB_SUCC(ret) && thread_idx < thread_count; ++thread_idx) {
    ObTableLoadTask *task = nullptr;
    // 1. 分配task
    if (OB_FAIL(ctx->alloc_task(task))) {
      LOG_WARN("fail to alloc task", KR(ret));
    }
    // 2. 设置processor
    else if (OB_FAIL(task->set_processor<CompactTaskProcessor>(ctx, this))) {
      LOG_WARN("fail to set compact task processor", KR(ret));
    }
    // 3. 设置callback
    else if (OB_FAIL(task->set_callback<CompactTaskCallback>(ctx, this))) {
      LOG_WARN("fail to set compact task callback", KR(ret));
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

int ObTableLoadGeneralTableCompactor::get_next_compactor_task(CompactorTask *&compactor_task)
{
  int ret = OB_SUCCESS;
  compactor_task = nullptr;
  if (OB_UNLIKELY(is_stop_ || has_error_)) {
    ret = OB_ITER_END;
  } else {
    ObMutexGuard guard(mutex_);
    if (OB_FAIL(compactor_task_iter_.get_next_compactor_task(compactor_task))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to get next compactor task", KR(ret));
      }
    } else {
      OB_ASSERT(compacting_list_.add_last(compactor_task));
    }
  }
  return ret;
}

void ObTableLoadGeneralTableCompactor::handle_compactor_task_finish(CompactorTask *compactor_task)
{
  ObMutexGuard guard(mutex_);
  OB_ASSERT(OB_NOT_NULL(compacting_list_.remove(compactor_task)));
}

int ObTableLoadGeneralTableCompactor::handle_compact_thread_finish(int ret_code)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ret_code)) {
    has_error_ = true;
  }
  const int64_t running_thread_count = ATOMIC_SAF(&running_thread_count_, 1);
  if (0 == running_thread_count) {
    if (OB_UNLIKELY(is_stop_ || has_error_)) {
    } else {
      LOG_INFO("LOAD TABLE COMPACT COMPLETED");
      if (OB_FAIL(build_result())) {
        LOG_WARN("fail to build result", KR(ret));
      } else if (OB_FAIL(compact_ctx_->handle_table_compact_success())) {
        LOG_WARN("fail to handle table compact success", KR(ret));
      }
    }
  }
  return ret;
}

int ObTableLoadGeneralTableCompactor::build_result()
{
  int ret = OB_SUCCESS;
  ObTableLoadTableCompactResult &result = compact_ctx_->result_;
  // get tables from table compactor
  for (int64_t i = 0; OB_SUCC(ret) && i < all_compactor_array_.count(); ++i) {
    ObIDirectLoadTabletTableCompactor *table_compactor = all_compactor_array_.at(i);
    ObIDirectLoadPartitionTable *table = nullptr;
    if (OB_FAIL(table_compactor->get_table(table, result.allocator_))) {
      LOG_WARN("fail to get table from table compactor", KR(ret), KPC(table_compactor));
    } else if (OB_FAIL(result.add_table(table))) {
      LOG_WARN("fail to add table", KR(ret));
    }
    if (OB_FAIL(ret)) {
      if (nullptr != table) {
        table->~ObIDirectLoadPartitionTable();
        result.allocator_.free(table);
      }
    }
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
