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

#include "observer/table_load/ob_table_load_store.h"
#include "observer/table_load/ob_table_load_merger.h"
#include "observer/table_load/ob_table_load_service.h"
#include "observer/table_load/ob_table_load_stat.h"
#include "observer/table_load/ob_table_load_store_ctx.h"
#include "observer/table_load/ob_table_load_store_trans.h"
#include "observer/table_load/ob_table_load_store_trans_px_writer.h"
#include "observer/table_load/ob_table_load_table_ctx.h"
#include "observer/table_load/ob_table_load_task.h"
#include "observer/table_load/ob_table_load_task_scheduler.h"
#include "observer/table_load/ob_table_load_trans_store.h"
#include "observer/table_load/ob_table_load_utils.h"
#include "storage/direct_load/ob_direct_load_insert_table_ctx.h"
#include "share/stat/ob_opt_stat_monitor_manager.h"
#include "storage/blocksstable/ob_sstable.h"
#include "share/table/ob_table_load_dml_stat.h"

namespace oceanbase
{
namespace observer
{
using namespace common;
using namespace table;
using namespace transaction;

ObTableLoadStore::ObTableLoadStore(ObTableLoadTableCtx *ctx)
  : ctx_(ctx), param_(ctx->param_), store_ctx_(ctx->store_ctx_), is_inited_(false)
{
}

int ObTableLoadStore::init_ctx(
  ObTableLoadTableCtx *ctx,
  const ObTableLoadArray<ObTableLoadLSIdAndPartitionId> &partition_id_array,
  const ObTableLoadArray<ObTableLoadLSIdAndPartitionId> &target_partition_id_array)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid agrs", KR(ret));
  } else if (OB_FAIL(ctx->init_store_ctx(partition_id_array, target_partition_id_array))) {
    LOG_WARN("fail to init store ctx", KR(ret));
  }
  return ret;
}

void ObTableLoadStore::abort_ctx(ObTableLoadTableCtx *ctx, bool &is_stopped)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ctx->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC(ctx));
    is_stopped = true;
  } else if (OB_UNLIKELY(nullptr == ctx->store_ctx_ || !ctx->store_ctx_->is_valid())) {
    // store ctx not init, do nothing
    is_stopped = true;
  } else {
    LOG_INFO("store abort");
    // 1. mark status abort, speed up background task exit
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = ctx->store_ctx_->set_status_abort())) {
      LOG_WARN("fail to set store status abort", KR(tmp_ret));
    }
    // 2. disable heart beat check
    ctx->store_ctx_->set_enable_heart_beat_check(false);
    // 3. mark all active trans abort
    if (OB_SUCCESS != (tmp_ret = abort_active_trans(ctx))) {
      LOG_WARN("fail to abort active trans", KR(tmp_ret));
    }
    ctx->store_ctx_->insert_table_ctx_->cancel();
    ctx->store_ctx_->merger_->stop();
    ctx->store_ctx_->task_scheduler_->stop();
    is_stopped = ctx->store_ctx_->task_scheduler_->is_stopped() && (0 == ATOMIC_LOAD(&ctx->store_ctx_->px_writer_count_));
  }
}

int ObTableLoadStore::abort_active_trans(ObTableLoadTableCtx *ctx)
{
  int ret = OB_SUCCESS;
  ObArray<ObTableLoadTransId> trans_id_array;
  trans_id_array.set_tenant_id(MTL_ID());
  if (OB_FAIL(ctx->store_ctx_->get_active_trans_ids(trans_id_array))) {
    LOG_WARN("fail to get active trans ids", KR(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < trans_id_array.count(); ++i) {
    const ObTableLoadTransId &trans_id = trans_id_array.at(i);
    ObTableLoadStoreTrans *trans = nullptr;
    if (OB_FAIL(ctx->store_ctx_->get_trans(trans_id, trans))) {
      if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
        LOG_WARN("fail to get trans", KR(ret), K(trans_id));
      } else {
        ret = OB_SUCCESS;
      }
    } else if (OB_FAIL(trans->set_trans_status_abort())) {
      LOG_WARN("fail to set trans status abort", KR(ret));
    }
    if (OB_NOT_NULL(trans)) {
      ctx->store_ctx_->put_trans(trans);
      trans = nullptr;
    }
  }
  return ret;
}

int ObTableLoadStore::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadCoordinator init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!ctx_->is_valid()) || OB_ISNULL(store_ctx_) ||
             OB_UNLIKELY(!store_ctx_->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC_(ctx), KPC_(store_ctx));
  } else if (THIS_WORKER.is_timeout_ts_valid() && OB_UNLIKELY(THIS_WORKER.is_timeout())) {
    ret = OB_TIMEOUT;
    LOG_WARN("worker timeouted", KR(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObTableLoadStore::pre_begin()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadStore not init", KR(ret), KP(this));
  } else {
    LOG_INFO("store pre begin");
    // do nothing
  }

  return ret;
}

int ObTableLoadStore::confirm_begin()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadStore not init", KR(ret), KP(this));
  } else {
    LOG_INFO("store confirm begin");
    store_ctx_->heart_beat(); // init heart beat
    store_ctx_->set_enable_heart_beat_check(true);
    if (ObDirectLoadMethod::is_incremental(param_.method_)) {
      if (OB_FAIL(open_insert_table_ctx())) {
        LOG_WARN("fail to open insert_table_ctx", KR(ret));
      }
    } else {
      if (OB_FAIL(ctx_->store_ctx_->set_status_loading())) {
        LOG_WARN("fail to set store status loading", KR(ret));
      }
    }
  }

  return ret;
}

int ObTableLoadStore::open_insert_table_ctx()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadStore not init", KR(ret));
  } else {
    for (int32_t session_id = 1; OB_SUCC(ret) && session_id <= ctx_->param_.session_count_; ++session_id) {
      ObTableLoadTask *task = nullptr;
      if (OB_FAIL(ctx_->alloc_task(task))) {
        LOG_WARN("fail to alloc task", KR(ret));
      } else if (OB_FAIL(task->set_processor<OpenInsertTabletTaskProcessor>(ctx_))) {
        LOG_WARN("fail to set flush task processor", KR(ret));
      } else if (OB_FAIL(task->set_callback<OpenInsertTabletTaskCallback>(ctx_))) {
        LOG_WARN("fail to set flush task callback", KR(ret));
      } else if (OB_FAIL(store_ctx_->task_scheduler_->add_task(session_id - 1, task))) {
        LOG_WARN("fail to add task", KR(ret), K(session_id), KPC(task));
      }
      if (OB_FAIL(ret)) {
        if (nullptr != task) {
          ctx_->free_task(task);
        }
      }
    }
  }

  return ret;
}

class ObTableLoadStore::OpenInsertTabletTaskProcessor : public ObITableLoadTaskProcessor
{
public:
  OpenInsertTabletTaskProcessor(ObTableLoadTask &task, ObTableLoadTableCtx *ctx)
    : ObITableLoadTaskProcessor(task), ctx_(ctx)
  {
    ctx_->inc_ref_count();
  }
  virtual ~OpenInsertTabletTaskProcessor()
  {
    ObTableLoadService::put_ctx(ctx_);
  }
  int process() override
  {
    OB_TABLE_LOAD_STATISTICS_TIME_COST(INFO, store_open_tablet_time_us);
    int ret = OB_SUCCESS;
    while (OB_SUCC(ret)) {
      ObTabletID tablet_id;
      ObDirectLoadInsertTabletContext *tablet_ctx = nullptr;
      if (OB_FAIL(ctx_->store_ctx_->get_next_insert_tablet_ctx(tablet_id))) {
        if (OB_UNLIKELY(ret != OB_ITER_END)) {
          LOG_WARN("fail to get next insert tablet context", KR(ret));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else if (OB_FAIL(ctx_->store_ctx_->insert_table_ctx_->get_tablet_context(tablet_id, tablet_ctx))) {
        LOG_WARN("fail to get tablet context", KR(ret), K(tablet_id));
      } else {
        bool is_finish = false;
        while (OB_SUCC(ret)) {
          if (THIS_WORKER.is_timeout_ts_valid() && OB_UNLIKELY(THIS_WORKER.is_timeout())) {
            ret = OB_TIMEOUT;
            LOG_WARN("worker timeouted", KR(ret));
          } else if (OB_FAIL(ctx_->store_ctx_->check_status(ObTableLoadStatusType::INITED))) {
            LOG_WARN("fail to check status", KR(ret));
          } else if (OB_FAIL(tablet_ctx->open())) {
            LOG_WARN("fail to open tablet context", KR(ret), K(tablet_id));
            if (ret == OB_EAGAIN || ret == OB_MINOR_FREEZE_NOT_ALLOW) {
              LOG_WARN("retry to open tablet context", K(tablet_id));
              ret = OB_SUCCESS;
            }
          } else {
            ctx_->store_ctx_->handle_open_insert_tablet_ctx_finish(is_finish);
            break;
          }
        }
        if (OB_SUCC(ret)) {
          if (is_finish && OB_FAIL(ctx_->store_ctx_->set_status_loading())) {
            LOG_WARN("fail to set store status loading", KR(ret));
          }
        }
      }
    }

    return ret;
  }
private:
  ObTableLoadTableCtx * const ctx_;
};

class ObTableLoadStore::OpenInsertTabletTaskCallback : public ObITableLoadTaskCallback
{
public:
  OpenInsertTabletTaskCallback(ObTableLoadTableCtx *ctx)
    : ctx_(ctx)
  {
    ctx_->inc_ref_count();
  }
  virtual ~OpenInsertTabletTaskCallback()
  {
    ObTableLoadService::put_ctx(ctx_);
  }
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
  ObTableLoadTableCtx * const ctx_;
};

/**
 * merge
 */

class ObTableLoadStore::MergeTaskProcessor : public ObITableLoadTaskProcessor
{
public:
  MergeTaskProcessor(ObTableLoadTask &task, ObTableLoadTableCtx *ctx)
    : ObITableLoadTaskProcessor(task), ctx_(ctx)
  {
    ctx_->inc_ref_count();
  }
  virtual ~MergeTaskProcessor()
  {
    ObTableLoadService::put_ctx(ctx_);
  }
  int process() override
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(ctx_->store_ctx_->merger_->start())) {
      LOG_WARN("fail to start merger", KR(ret));
    }
    return ret;
  }
private:
  ObTableLoadTableCtx * const ctx_;
};

class ObTableLoadStore::MergeTaskCallback : public ObITableLoadTaskCallback
{
public:
  MergeTaskCallback(ObTableLoadTableCtx *ctx) : ctx_(ctx)
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
    if (OB_FAIL(ret_code)) {
      ctx_->store_ctx_->set_status_error(ret);
    }
    ctx_->free_task(task);
  }
private:
  ObTableLoadTableCtx * const ctx_;
};

int ObTableLoadStore::pre_merge(
  const ObTableLoadArray<ObTableLoadTransId> &committed_trans_id_array)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadStore not init", KR(ret), KP(this));
  } else {
    LOG_INFO("store pre merge");
    ObArenaAllocator allocator("TLD_Tmp");
    bool trans_exist = false;
    ObTableLoadArray<ObTableLoadTransId> store_committed_trans_id_array;
    allocator.set_tenant_id(MTL_ID());
    // 1. 冻结状态, 防止后续继续创建trans
    if (OB_FAIL(store_ctx_->set_status_frozen())) {
      LOG_WARN("fail to set store status frozen", KR(ret));
    }
    // 2. 检查当前是否还有trans没有结束
    else if (OB_FAIL(store_ctx_->check_exist_trans(trans_exist))) {
      LOG_WARN("fail to check exist trans", KR(ret));
    } else if (OB_UNLIKELY(trans_exist)) {
      ret = OB_ENTRY_EXIST;
      LOG_WARN("trans already exist", KR(ret));
    } else if (!ctx_->param_.px_mode_) {
      // 3. 检查数据一致性
      if (OB_FAIL(
                store_ctx_->get_committed_trans_ids(store_committed_trans_id_array, allocator))) {
        LOG_WARN("fail to get committed trans ids", KR(ret));
      } else if (OB_UNLIKELY(committed_trans_id_array.count() !=
                            store_committed_trans_id_array.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected committed trans count", KR(ret), K(committed_trans_id_array),
                K(store_committed_trans_id_array));
      } else {
        lib::ob_sort(store_committed_trans_id_array.begin(), store_committed_trans_id_array.end());
        for (int64_t i = 0; OB_SUCC(ret) && i < committed_trans_id_array.count(); ++i) {
          if (OB_UNLIKELY(committed_trans_id_array[i] != store_committed_trans_id_array[i])) {
            ret = OB_ITEM_NOT_MATCH;
            LOG_WARN("committed trans id not match", KR(ret), K(i), K(committed_trans_id_array[i]),
                    K(store_committed_trans_id_array[i]));
          }
        }
      }
    }
  }
  return ret;
}

int ObTableLoadStore::start_merge()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadStore not init", KR(ret), KP(this));
  } else {
    LOG_INFO("store start merge");
    if (OB_FAIL(store_ctx_->set_status_merging())) {
      LOG_WARN("fail to set store status merging", KR(ret));
    } else {
      ObTableLoadTask *task = nullptr;
      // 1. 分配task
      if (OB_FAIL(ctx_->alloc_task(task))) {
        LOG_WARN("fail to alloc task", KR(ret));
      }
      // 2. 设置processor
      else if (OB_FAIL(task->set_processor<MergeTaskProcessor>(ctx_))) {
        LOG_WARN("fail to set merge task processor", KR(ret));
      }
      // 3. 设置callback
      else if (OB_FAIL(task->set_callback<MergeTaskCallback>(ctx_))) {
        LOG_WARN("fail to set merge task callback", KR(ret));
      }
      // 4. 把task放入调度器
      else if (OB_FAIL(store_ctx_->task_scheduler_->add_task(0, task))) {
        LOG_WARN("fail to add task", KR(ret), KPC(task));
      }
      if (OB_FAIL(ret)) {
        if (nullptr != task) {
          ctx_->free_task(task);
        }
      }
    }
  }
  return ret;
}

int ObTableLoadStore::commit(ObTableLoadResultInfo &result_info,
                             ObTableLoadSqlStatistics &sql_statistics,
                             ObTableLoadDmlStat &dml_stats,
                             ObTxExecResult &trans_result)
{
  int ret = OB_SUCCESS;
  sql_statistics.reset();
  dml_stats.reset();
  trans_result.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadStore not init", KR(ret), KP(this));
  } else {
    LOG_INFO("store commit");
    ObTransService *txs = nullptr;
    ObMutexGuard guard(store_ctx_->get_op_lock());
    if (OB_ISNULL(MTL(ObTransService *))) {
      ret = OB_ERR_SYS;
      LOG_WARN("trans service is null", KR(ret));
    } else if (OB_FAIL(store_ctx_->check_status(ObTableLoadStatusType::MERGED))) {
      LOG_WARN("fail to check store status", KR(ret));
    } else if (OB_FAIL(store_ctx_->insert_table_ctx_->commit(dml_stats, sql_statistics))) {
      LOG_WARN("fail to commit insert table", KR(ret));
    } else if (ctx_->schema_.has_autoinc_column_ && OB_FAIL(store_ctx_->commit_autoinc_value())) {
      LOG_WARN("fail to commit sync auto increment value", KR(ret));
    }
    // 全量旁路导入的dml_stat在执行节点更新
    // 增量旁路导入的dml_stat收集到协调节点在事务中更新
    else if (ObDirectLoadMethod::is_full(param_.method_) &&
               OB_FAIL(ObOptStatMonitorManager::update_dml_stat_info_from_direct_load(dml_stats.dml_stat_array_))) {
      LOG_WARN("fail to update dml stat info", KR(ret));
    } else if (ObDirectLoadMethod::is_full(param_.method_) && FALSE_IT(dml_stats.reset())) {
    } else if (ObDirectLoadMethod::is_incremental(param_.method_) &&
               OB_FAIL(txs->get_tx_exec_result(*ctx_->session_info_->get_tx_desc(), trans_result))) {
    } else if (OB_FAIL(store_ctx_->set_status_commit())) {
      LOG_WARN("fail to set store status commit", KR(ret));
    } else {
      store_ctx_->set_enable_heart_beat_check(false);
      result_info = store_ctx_->result_info_;
    }
  }
  return ret;
}

int ObTableLoadStore::get_status(ObTableLoadStatusType &status, int &error_code)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadStore not init", KR(ret), KP(this));
  } else {
    LOG_INFO("store get status");
    store_ctx_->get_status(status, error_code);
  }
  return ret;
}

int ObTableLoadStore::heart_beat()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadStore not init", KR(ret), KP(this));
  } else {
    LOG_DEBUG("store heart beat");
    store_ctx_->heart_beat();
  }
  return ret;
}

int ObTableLoadStore::pre_start_trans(const ObTableLoadTransId &trans_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadStore not init", KR(ret), KP(this));
  } else {
    LOG_INFO("store pre start trans", K(trans_id));
    ObTableLoadStoreTrans *trans = nullptr;
    if (OB_FAIL(store_ctx_->start_trans(trans_id, trans))) {
      LOG_WARN("fail to start trans", KR(ret), K(trans_id));
    }
    if (OB_NOT_NULL(trans)) {
      store_ctx_->put_trans(trans);
      trans = nullptr;
    }
  }
  return ret;
}

int ObTableLoadStore::confirm_start_trans(const ObTableLoadTransId &trans_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadStore not init", KR(ret), KP(this));
  } else {
    LOG_INFO("store confirm start trans", K(trans_id));
    ObTableLoadStoreTrans *trans = nullptr;
    if (OB_FAIL(store_ctx_->get_trans(trans_id, trans))) {
      LOG_WARN("fail to get trans", KR(ret));
    } else if (OB_FAIL(trans->set_trans_status_running())) {
      LOG_WARN("fail to set trans status running", KR(ret));
    }
    if (OB_NOT_NULL(trans)) {
      store_ctx_->put_trans(trans);
      trans = nullptr;
    }
  }
  return ret;
}

int ObTableLoadStore::pre_finish_trans(const ObTableLoadTransId &trans_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadStore not init", KR(ret), KP(this));
  } else {
    LOG_INFO("store pre finish trans", K(trans_id));
    ObTableLoadStoreTrans *trans = nullptr;
    if (OB_FAIL(store_ctx_->get_trans(trans_id, trans))) {
      LOG_WARN("fail to get trans", KR(ret));
    } else if (OB_FAIL(flush(trans))) {
      LOG_WARN("fail to flush", KR(ret));
    }
    if (OB_NOT_NULL(trans)) {
      store_ctx_->put_trans(trans);
      trans = nullptr;
    }
  }
  return ret;
}

int ObTableLoadStore::confirm_finish_trans(const ObTableLoadTransId &trans_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadStore not init", KR(ret), KP(this));
  } else {
    LOG_INFO("store confirm finish trans", K(trans_id));
    ObTableLoadStoreTrans *trans = nullptr;
    if (OB_FAIL(store_ctx_->get_trans(trans_id, trans))) {
      LOG_WARN("fail to get trans", KR(ret));
    } else if (OB_FAIL(store_ctx_->commit_trans(trans))) {
      LOG_WARN("fail to commit trans", KR(ret));
    }
    if (OB_NOT_NULL(trans)) {
      store_ctx_->put_trans(trans);
      trans = nullptr;
    }
  }
  return ret;
}

int ObTableLoadStore::abandon_trans(const ObTableLoadTransId &trans_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadStore not init", KR(ret), KP(this));
  } else {
    LOG_INFO("store abandon trans", K(trans_id));
    ObTableLoadStoreTrans *trans = nullptr;
    if (OB_FAIL(store_ctx_->get_trans(trans_id, trans))) {
      if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
        LOG_WARN("fail to get trans", KR(ret));
      } else {
        ret = OB_SUCCESS;
      }
    } else if (OB_UNLIKELY(trans_id != trans->get_trans_ctx()->trans_id_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid trans id", KR(ret), K(trans_id), KPC(trans));
    } else if (OB_FAIL(trans->set_trans_status_abort())) {
      LOG_WARN("fail to set trans status abort", KR(ret));
    } else if (OB_FAIL(store_ctx_->abort_trans(trans))) {
      LOG_WARN("fail to abort trans", KR(ret));
    } else if (OB_FAIL(clean_up_trans(trans))) {
      LOG_WARN("fail to clean up trans", KR(ret));
    }
    if (OB_NOT_NULL(trans)) {
      store_ctx_->put_trans(trans);
      trans = nullptr;
    }
  }
  return ret;
}

class ObTableLoadStore::CleanUpTaskProcessor : public ObITableLoadTaskProcessor
{
public:
  CleanUpTaskProcessor(ObTableLoadTask &task, ObTableLoadTableCtx *ctx,
                       ObTableLoadStoreTrans *trans, ObTableLoadTransStoreWriter *store_writer,
                       int32_t session_id)
    : ObITableLoadTaskProcessor(task),
      ctx_(ctx),
      trans_(trans),
      store_writer_(store_writer),
      session_id_(session_id)
  {
    ctx_->inc_ref_count();
    trans_->inc_ref_count();
    store_writer_->inc_ref_count();
  }
  virtual ~CleanUpTaskProcessor()
  {
    trans_->put_store_writer(store_writer_);
    ctx_->store_ctx_->put_trans(trans_);
    ObTableLoadService::put_ctx(ctx_);
  }
  int process() override
  {
    int ret = OB_SUCCESS;
    store_writer_->clean_up(session_id_);
    return ret;
  }
private:
  ObTableLoadTableCtx * const ctx_;
  ObTableLoadStoreTrans * const trans_;
  ObTableLoadTransStoreWriter * const store_writer_;
  const int32_t session_id_;
};

class ObTableLoadStore::CleanUpTaskCallback : public ObITableLoadTaskCallback
{
public:
  CleanUpTaskCallback(ObTableLoadTableCtx *ctx) : ctx_(ctx)
  {
    ctx_->inc_ref_count();
  }
  virtual ~CleanUpTaskCallback()
  {
    ObTableLoadService::put_ctx(ctx_);
  }
  void callback(int ret_code, ObTableLoadTask *task) override
  {
    ctx_->free_task(task);
  }
private:
  ObTableLoadTableCtx * const ctx_;
};

int ObTableLoadStore::clean_up_trans(ObTableLoadStoreTrans *trans)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("store clean up trans");
  ObTableLoadTransStoreWriter *store_writer = nullptr;
  // 取出当前store_writer
  if (OB_FAIL(trans->get_store_writer(store_writer))) {
    LOG_WARN("fail to get store writer", KR(ret));
  } else {
    for (int32_t session_id = 1; OB_SUCC(ret) && session_id <= param_.write_session_count_;
         ++session_id) {
      ObTableLoadTask *task = nullptr;
      // 1. 分配task
      if (OB_FAIL(ctx_->alloc_task(task))) {
        LOG_WARN("fail to alloc task", KR(ret));
      }
      // 2. 设置processor
      else if (OB_FAIL(task->set_processor<CleanUpTaskProcessor>(ctx_, trans, store_writer,
                                                                 session_id))) {
        LOG_WARN("fail to set clean up task processor", KR(ret));
      }
      // 3. 设置callback
      else if (OB_FAIL(task->set_callback<CleanUpTaskCallback>(ctx_))) {
        LOG_WARN("fail to set clean up task callback", KR(ret));
      }
      // 4. 把task放入调度器
      else if (OB_FAIL(store_ctx_->task_scheduler_->add_task(session_id - 1, task))) {
        LOG_WARN("fail to add task", KR(ret), K(session_id), KPC(task));
      }
      if (OB_FAIL(ret)) {
        if (nullptr != task) {
          ctx_->free_task(task);
        }
      }
    }
  }
  if (OB_NOT_NULL(store_writer)) {
    trans->put_store_writer(store_writer);
    store_writer = nullptr;
  }
  return ret;
}

/**
 * get trans status
 */

int ObTableLoadStore::get_trans_status(const ObTableLoadTransId &trans_id,
                                       ObTableLoadTransStatusType &trans_status,
                                       int &error_code)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadStore not init", KR(ret), KP(this));
  } else {
    LOG_INFO("store get trans status");
    ObTableLoadTransCtx *trans_ctx = nullptr;
    if (OB_FAIL(store_ctx_->get_trans_ctx(trans_id, trans_ctx))) {
      LOG_WARN("fail to get trans ctx", KR(ret), K(trans_id));
    } else {
      trans_ctx->get_trans_status(trans_status, error_code);
    }
  }
  return ret;
}

/**
 * write
 */

class ObTableLoadStore::WriteTaskProcessor : public ObITableLoadTaskProcessor
{
public:
  WriteTaskProcessor(ObTableLoadTask &task, ObTableLoadTableCtx *ctx, ObTableLoadStoreTrans *trans,
                     ObTableLoadTransStoreWriter *store_writer, int32_t session_id)
    : ObITableLoadTaskProcessor(task),
      ctx_(ctx),
      trans_(trans),
      store_writer_(store_writer),
      session_id_(session_id)
  {
    ctx_->inc_ref_count();
    trans_->inc_ref_count();
    store_writer_->inc_ref_count();
  }
  virtual ~WriteTaskProcessor()
  {
    trans_->put_store_writer(store_writer_);
    ctx_->store_ctx_->put_trans(trans_);
    ObTableLoadService::put_ctx(ctx_);
  }
  int set_row_array(const ObTableLoadTabletObjRowArray &row_array)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(row_array_.assign(row_array))) {
      LOG_WARN("fail to assign row array", KR(ret));
    }
    return ret;
  }
  int process() override
  {
    OB_TABLE_LOAD_STATISTICS_TIME_COST(INFO, store_write_time_us);
    int ret = OB_SUCCESS;
    if (OB_SUCC(trans_->check_trans_status(ObTableLoadTransStatusType::RUNNING)) ||
        OB_SUCC(trans_->check_trans_status(ObTableLoadTransStatusType::FROZEN))) {
      if (OB_FAIL(store_writer_->write(session_id_, row_array_))) {
        LOG_WARN("fail to write store", KR(ret));
      }
    }
    return ret;
  }
private:
  ObTableLoadTableCtx * const ctx_;
  ObTableLoadStoreTrans * const trans_;
  ObTableLoadTransStoreWriter * const store_writer_;
  const int32_t session_id_;
  ObTableLoadTabletObjRowArray row_array_;
};

class ObTableLoadStore::WriteTaskCallback : public ObITableLoadTaskCallback
{
public:
  WriteTaskCallback(ObTableLoadTableCtx *ctx, ObTableLoadStoreTrans *trans,
                    ObTableLoadTransStoreWriter *store_writer)
    : ctx_(ctx), trans_(trans), store_writer_(store_writer)
  {
    ctx_->inc_ref_count();
    trans_->inc_ref_count();
    store_writer_->inc_ref_count();
  }
  virtual ~WriteTaskCallback()
  {
    trans_->put_store_writer(store_writer_);
    ctx_->store_ctx_->put_trans(trans_);
    ObTableLoadService::put_ctx(ctx_);
  }
  void callback(int ret_code, ObTableLoadTask *task) override
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(ret_code)) {
      trans_->set_trans_status_error(ret);
    }
    ctx_->free_task(task);
  }
private:
  ObTableLoadTableCtx * const ctx_;
  ObTableLoadStoreTrans * const trans_;
  ObTableLoadTransStoreWriter * const store_writer_; // 为了保证接收完本次写入结果之后再让store的引用归零
};

int ObTableLoadStore::write(const ObTableLoadTransId &trans_id, int32_t session_id,
                            uint64_t sequence_no, const ObTableLoadTabletObjRowArray &row_array)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadStore not init", KR(ret), KP(this));
  } else {
    LOG_DEBUG("store write");
    ObTableLoadStoreTrans *trans = nullptr;
    ObTableLoadTransStoreWriter *store_writer = nullptr;
    ObTableLoadMutexGuard guard;
    // 取出当前trans
    if (OB_FAIL(store_ctx_->get_trans(trans_id, trans))) {
      LOG_WARN("fail to get trans", KR(ret));
    }
    else if (OB_FAIL(trans->check_trans_status(ObTableLoadTransStatusType::RUNNING))) {
      LOG_WARN("fail to check trans status", KR(ret));
    }
    // 取出store_writer
    else if (OB_FAIL(trans->get_store_writer(store_writer))) {
      LOG_WARN("fail to get store writer", KR(ret));
    //} else if (OB_FAIL(store_writer->advance_sequence_no(session_id, partition_id, sequence_no, guard))) {
    //  if (OB_UNLIKELY(OB_ENTRY_EXIST != ret)) {
    //    LOG_WARN("fail to advance sequence no", KR(ret), K(session_id));
    //  } else {
    //    ret = OB_SUCCESS;
    //  }
    } else {
      ObTableLoadTask *task = nullptr;
      WriteTaskProcessor *processor = nullptr;
      // 1. 分配task
      if (OB_FAIL(ctx_->alloc_task(task))) {
        LOG_WARN("fail to alloc task", KR(ret));
      }
      // 2. 设置processor
      else if (OB_FAIL(task->set_processor<WriteTaskProcessor>(ctx_, trans, store_writer,
                                                               session_id))) {
        LOG_WARN("fail to set write task processor", KR(ret));
      } else if (OB_ISNULL(processor = dynamic_cast<WriteTaskProcessor *>(task->get_processor()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null processor", KR(ret));
      } else if (OB_FAIL(processor->set_row_array(row_array))) {
        LOG_WARN("fail to set objs", KR(ret));
      }
      // 3. 设置callback
      else if (OB_FAIL(task->set_callback<WriteTaskCallback>(ctx_, trans, store_writer))) {
        LOG_WARN("fail to set write task callback", KR(ret));
      }
      // 4. 把task放入调度器
      else if (OB_FAIL(store_ctx_->task_scheduler_->add_task(session_id - 1, task))) {
        LOG_WARN("fail to add task", KR(ret), K(session_id), KPC(task));
      }
      if (OB_FAIL(ret)) {
        if (nullptr != task) {
          ctx_->free_task(task);
        }
      }
    }
    if (OB_NOT_NULL(trans)) {
      if (OB_NOT_NULL(store_writer)) {
        trans->put_store_writer(store_writer);
        store_writer = nullptr;
      }
      store_ctx_->put_trans(trans);
      trans = nullptr;
    }
  }
  return ret;
}

/**
 * flush
 */

class ObTableLoadStore::FlushTaskProcessor : public ObITableLoadTaskProcessor
{
public:
  FlushTaskProcessor(ObTableLoadTask &task, ObTableLoadTableCtx *ctx, ObTableLoadStoreTrans *trans,
                     ObTableLoadTransStoreWriter *store_writer, int32_t session_id)
    : ObITableLoadTaskProcessor(task),
      ctx_(ctx),
      trans_(trans),
      store_writer_(store_writer),
      session_id_(session_id)
  {
    ctx_->inc_ref_count();
    trans_->inc_ref_count();
    store_writer_->inc_ref_count();
  }
  virtual ~FlushTaskProcessor()
  {
    trans_->put_store_writer(store_writer_);
    ctx_->store_ctx_->put_trans(trans_);
    ObTableLoadService::put_ctx(ctx_);
  }
  int process() override
  {
    OB_TABLE_LOAD_STATISTICS_TIME_COST(INFO, store_flush_time_us);
    int ret = OB_SUCCESS;
    if (OB_SUCC(trans_->check_trans_status(ObTableLoadTransStatusType::FROZEN))) {
      if (OB_FAIL(store_writer_->flush(session_id_))) {
        LOG_WARN("fail to flush store", KR(ret));
      }
    }
    return ret;
  }
private:
  ObTableLoadTableCtx * const ctx_;
  ObTableLoadStoreTrans * const trans_;
  ObTableLoadTransStoreWriter * const store_writer_;
  const int32_t session_id_;
};

class ObTableLoadStore::FlushTaskCallback : public ObITableLoadTaskCallback
{
public:
  FlushTaskCallback(ObTableLoadTableCtx *ctx, ObTableLoadStoreTrans *trans,
                    ObTableLoadTransStoreWriter *store_writer)
    : ctx_(ctx), trans_(trans), store_writer_(store_writer)
  {
    ctx_->inc_ref_count();
    trans_->inc_ref_count();
    store_writer_->inc_ref_count();
  }
  virtual ~FlushTaskCallback()
  {
    trans_->put_store_writer(store_writer_);
    ctx_->store_ctx_->put_trans(trans_);
    ObTableLoadService::put_ctx(ctx_);
  }
  void callback(int ret_code, ObTableLoadTask *task) override
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(ret_code)) {
      trans_->set_trans_status_error(ret);
    }
    ctx_->free_task(task);
    OB_TABLE_LOAD_STATISTICS_PRINT_AND_RESET();
  }
private:
  ObTableLoadTableCtx * const ctx_;
  ObTableLoadStoreTrans * const trans_;
  ObTableLoadTransStoreWriter * const store_writer_; // 为了保证接收完本次写入结果之后再让store的引用归零
};

int ObTableLoadStore::flush(ObTableLoadStoreTrans *trans)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadStore not init", KR(ret), KP(this));
  } else {
    LOG_DEBUG("store flush");
    ObTableLoadTransStoreWriter *store_writer = nullptr;
    // 取出当前store_writer
    if (OB_FAIL(trans->get_store_writer(store_writer))) {
      LOG_WARN("fail to get store writer", KR(ret));
    }
    // after get store writer, avoid early commit
    else if (OB_FAIL(trans->set_trans_status_frozen())) {
      LOG_WARN("fail to freeze trans", KR(ret));
    } else {
      for (int32_t session_id = 1; OB_SUCC(ret) && session_id <= param_.write_session_count_; ++session_id) {
        ObTableLoadTask *task = nullptr;
        // 1. 分配task
        if (OB_FAIL(ctx_->alloc_task(task))) {
          LOG_WARN("fail to alloc task", KR(ret));
        }
        // 2. 设置processor
        else if (OB_FAIL(task->set_processor<FlushTaskProcessor>(
                  ctx_, trans, store_writer, session_id))) {
          LOG_WARN("fail to set flush task processor", KR(ret));
        }
        // 3. 设置callback
        else if (OB_FAIL(task->set_callback<FlushTaskCallback>(ctx_, trans, store_writer))) {
          LOG_WARN("fail to set flush task callback", KR(ret));
        }
        // 4. 把task放入调度器
        else if (OB_FAIL(store_ctx_->task_scheduler_->add_task(session_id - 1, task))) {
          LOG_WARN("fail to add task", KR(ret), K(session_id), KPC(task));
        }
        if (OB_FAIL(ret)) {
          if (nullptr != task) {
            ctx_->free_task(task);
          }
        }
      }
    }
    if (OB_NOT_NULL(store_writer)) {
      trans->put_store_writer(store_writer);
      store_writer = nullptr;
    }
  }
  return ret;
}

int ObTableLoadStore::px_start_trans(const ObTableLoadTransId &trans_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadStore not init", KR(ret), KP(this));
  } else {
    LOG_INFO("store px start trans", K(trans_id));
    ObTableLoadStoreTrans *trans = nullptr;
    if (OB_FAIL(store_ctx_->start_trans(trans_id, trans))) {
      LOG_WARN("fail to start trans", KR(ret), K(trans_id));
    } else if (OB_FAIL(trans->set_trans_status_running())) {
      LOG_WARN("fail to set trans status running", KR(ret));
    } else {
      LOG_DEBUG("succeed to start trans", K(trans_id));
    }
    if (OB_NOT_NULL(trans)) {
      store_ctx_->put_trans(trans);
      trans = nullptr;
    }
  }
  return ret;
}

int ObTableLoadStore::px_finish_trans(const ObTableLoadTransId &trans_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadStore not init", KR(ret), KP(this));
  } else {
    LOG_INFO("store px finish trans", K(trans_id));
    ObTableLoadStoreTrans *trans = nullptr;
    if (OB_FAIL(store_ctx_->get_segment_trans(trans_id.segment_id_, trans))) {
      LOG_WARN("fail to get segment trans", KR(ret));
    } else if (OB_UNLIKELY(trans_id != trans->get_trans_id())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected trans id", KR(ret), K(trans_id), KPC(trans));
    } else if (OB_FAIL(px_flush(trans))) {
      LOG_WARN("fail to do px flush", KR(ret));
    } else if (OB_FAIL(store_ctx_->commit_trans(trans))) {
      LOG_WARN("fail to commit trans", KR(ret));
    } else {
      LOG_DEBUG("succeed to commit trans", K(trans_id));
    }
    if (OB_NOT_NULL(trans)) {
      store_ctx_->put_trans(trans);
      trans = nullptr;
    }
  }
  return ret;
}

int ObTableLoadStore::px_get_trans_writer(const ObTableLoadTransId &trans_id,
                                          ObTableLoadStoreTransPXWriter &writer)
{
  int ret = OB_SUCCESS;
  writer.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadStore not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!trans_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(trans_id));
  } else {
    ObTableLoadStoreTrans *trans = nullptr;
    ObTableLoadTransStoreWriter *store_writer = nullptr;
    if (OB_FAIL(store_ctx_->get_segment_trans(trans_id.segment_id_, trans))) {
      LOG_WARN("fail to get segment trans", KR(ret));
    } else if (OB_UNLIKELY(trans_id != trans->get_trans_id())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected trans id", KR(ret), K(trans_id), KPC(trans));
    } else if (OB_FAIL(trans->get_store_writer(store_writer))) {
      LOG_WARN("fail to get store writer", KR(ret));
    } else if (OB_FAIL(writer.init(store_ctx_, trans, store_writer))) {
      LOG_WARN("fail to init writer", KR(ret));
    }
    if (OB_NOT_NULL(trans)) {
      if (OB_NOT_NULL(store_writer)) {
        trans->put_store_writer(store_writer);
        store_writer = nullptr;
      }
      store_ctx_->put_trans(trans);
      trans = nullptr;
    }
  }
  return ret;
}

int ObTableLoadStore::px_flush(ObTableLoadStoreTrans *trans)
{
  int ret = OB_SUCCESS;
  int32_t session_id = 1;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadStore not init", KR(ret), KP(this));
  } else {
    ObTableLoadTransStoreWriter *store_writer = nullptr;
    if (OB_FAIL(trans->get_store_writer(store_writer))) {
      LOG_WARN("fail to get store writer", KR(ret));
    }
    // after get store writer, avoid early commit
    else if (OB_FAIL(trans->set_trans_status_frozen())) {
      LOG_WARN("fail to freeze trans", KR(ret));
    } else if (OB_FAIL(store_writer->flush(session_id))) {
      LOG_WARN("fail to flush store", KR(ret));
    } else {
      LOG_DEBUG("succeed to flush store");
    }
    if (OB_NOT_NULL(store_writer)) {
      trans->put_store_writer(store_writer);
      store_writer = nullptr;
    }
  }
  return ret;
}

int ObTableLoadStore::px_abandon_trans(ObTableLoadTableCtx *ctx, const ObTableLoadTransId &trans_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ctx->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC(ctx));
  } else if (OB_UNLIKELY(nullptr == ctx->store_ctx_ || !ctx->store_ctx_->is_valid())) {
    // store ctx not init, do nothing
  } else {
    LOG_INFO("store px abandon trans", K(trans_id));
    ObTableLoadStoreCtx *store_ctx = ctx->store_ctx_;
    ObTableLoadStoreTrans *trans = nullptr;
    if (OB_FAIL(store_ctx->get_segment_trans(trans_id.segment_id_, trans))) {
      if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
        LOG_WARN("fail to get segment trans", KR(ret));
      } else {
        ret = OB_SUCCESS;
      }
    } else if (OB_UNLIKELY(trans_id != trans->get_trans_id())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected trans id", KR(ret), K(trans_id), KPC(trans));
    } else if (OB_FAIL(trans->set_trans_status_abort())) {
      LOG_WARN("fail to set trans status abort", KR(ret));
    } else if (OB_FAIL(store_ctx->abort_trans(trans))) {
      LOG_WARN("fail to abort trans", KR(ret));
    } else if (OB_FAIL(px_clean_up_trans(trans))) {
      LOG_WARN("fail to clean up trans", KR(ret));
    }
    if (OB_NOT_NULL(trans)) {
      store_ctx->put_trans(trans);
      trans = nullptr;
    }
  }
  return ret;
}

int ObTableLoadStore::px_clean_up_trans(ObTableLoadStoreTrans *trans)
{
  int ret = OB_SUCCESS;
  int32_t session_id = 1;
  if (OB_ISNULL(trans)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(trans));
  } else {
    ObTableLoadTransStoreWriter *store_writer = nullptr;
    if (OB_FAIL(trans->get_store_writer(store_writer))) {
      LOG_WARN("fail to get store writer", KR(ret));
    } else if (OB_FAIL(store_writer->clean_up(session_id))) {
      LOG_WARN("fail to clean up store writer", KR(ret));
    }
    if (OB_NOT_NULL(store_writer)) {
      trans->put_store_writer(store_writer);
      store_writer = nullptr;
    }
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
