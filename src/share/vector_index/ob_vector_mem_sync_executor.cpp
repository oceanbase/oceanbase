/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SHARE
#include "ob_vector_mem_sync_executor.h"
#include "ob_plugin_vector_index_scheduler.h"
#include "share/vector_index/ob_plugin_vector_index_service.h"
#include "share/vector_index/ob_plugin_vector_index_utils.h"
#include "share/vector_index/ob_vector_index_i_task_executor.h"
#include "share/vector_index/ob_vector_index_util.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "storage/ls/ob_ls.h"
#include "logservice/ob_log_handler.h"
#include "rootserver/ddl_task/ob_ddl_task.h"
#include "lib/thread/thread_mgr.h"
#include "common/ob_role.h"
#include "observer/omt/ob_tenant_config_mgr.h"

namespace oceanbase
{
using namespace storage;
namespace share
{

// ==================== ObVecMemSyncLogCb Implementation ====================
ObVecMemSyncLogCb::ObVecMemSyncLogCb()
  : executor_(nullptr),
    log_buffer_(nullptr),
    is_callback_invoked_(false),
    is_success_(false)
{
}

ObVecMemSyncLogCb::~ObVecMemSyncLogCb()
{
  destroy();
}

void ObVecMemSyncLogCb::reset()
{
  ATOMIC_SET(&is_callback_invoked_, false);
  ATOMIC_SET(&is_success_, false);
}

void ObVecMemSyncLogCb::destroy()
{
  if (OB_NOT_NULL(log_buffer_)) {
    ob_free(log_buffer_);
    log_buffer_ = nullptr;
  }
}

int ObVecMemSyncLogCb::on_success()
{
  ATOMIC_SET(&is_success_, true);
  if (OB_NOT_NULL(executor_)) {
    executor_->handle_submit_callback(true);
  }
  ATOMIC_SET(&is_callback_invoked_, true);
  return OB_SUCCESS;
}

int ObVecMemSyncLogCb::on_failure()
{
  if (OB_NOT_NULL(executor_)) {
    executor_->handle_submit_callback(false);
  }
  ATOMIC_SET(&is_callback_invoked_, true);
  return OB_SUCCESS;
}

// ==================== ObVecMemSyncTask Implementation ====================

int ObVecMemSyncTask::init(uint64_t tenant_id, ObLSID ls_id, ObVecIndexAsyncTaskCtx *task_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_ISNULL(task_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id), KP(task_ctx));
  } else {
    tenant_id_ = tenant_id;
    ls_id_ = ls_id;
    ctx_ = task_ctx;
    task_type_ = ObVecIndexAsyncTaskType::OB_VECTOR_ASYNC_MEM_SYNC_TASK;
    ls_ = task_ctx->get_ls();
    allocator_.set_tenant_id(tenant_id);
    is_inited_ = true;
  }
  return ret;
}

int ObVecMemSyncTask::do_work()
{
  int ret = OB_SUCCESS;
  CONSUMER_GROUP_FUNC_GUARD(ObFunctionType::PRIO_VECTOR_HIGH);
  ObTraceIdGuard trace_guard(ctx_->task_status_.trace_id_);
  LOG_INFO("[VEC_ASYNC_TASK] start do_work", K(ret), K(ctx_->task_status_), K(ls_id_));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObVecMemSyncTask not init", KR(ret));
  } else if (OB_ISNULL(ls_) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", KR(ret), KP(ls_), KP(ctx_));
  } else {
    bool need_stop = false;
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
    bool is_leader = false;

    if (OB_FAIL(ObPluginVectorIndexUtils::get_ls_leader_flag(ls_id_, is_leader))) {
      LOG_WARN("memdata sync fail to get ls leader flag", KR(ret), K(ls_id_));
    } else if (!tenant_config.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail get tenant_config", KR(ret), K(tenant_id_));
    } else if (!is_leader && !tenant_config->load_vector_index_on_follower) {
      need_stop = true;
      common::ObSpinLockGuard ctx_guard(ctx_->lock_);
      ctx_->task_status_.status_ = ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_CANCEL;
      LOG_INFO("no need to load memdata to ls follower", K(ret), K(tenant_config->load_vector_index_on_follower));
    }

    if (OB_FAIL(ret) || need_stop) {
    } else if (OB_FAIL(process_one())) {
        LOG_WARN("fail to refresh memdata", KR(ret), KPC(ctx_));
    }

  }
  LOG_INFO("[VEC_ASYNC_TASK] end do_work", K(ret), K(ctx_->task_status_));
  return ret;
}

int ObVecMemSyncTask::process_one()
{
  int ret = OB_SUCCESS;
  int64_t start_time = ObTimeUtil::current_time();
  ObPluginVectorIndexAdapterGuard adpt_guard;
  ObPluginVectorIndexAdapterGuard new_adpt_guard;
  ObPluginVectorIndexMgr *mgr = nullptr;
  ObPluginVectorIndexService *vector_index_service = MTL(ObPluginVectorIndexService *);

  if (OB_ISNULL(vector_index_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("vector index service is null", KR(ret));
  } else if (OB_FAIL(vector_index_service->acquire_vector_index_mgr(ls_id_, mgr))) {
    LOG_WARN("fail to acquire vector index mgr", KR(ret), K(ls_id_));
  } else if (OB_ISNULL(mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mgr is null", KR(ret));
  } else if (OB_FAIL(ObPluginVectorIndexUtils::get_task_read_snapshot(ls_id_, read_snapshot_))) {
    LOG_WARN("memdata sync fail to get task read snapshot", KR(ret), K(ls_id_), KPC(ctx_));
  } else if (OB_FAIL(mgr->get_adapter_inst_guard(ctx_->task_status_.tablet_id_, adpt_guard))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_EAGAIN;
    }
    LOG_WARN("memdata sync fail to get adapter instance guard", KR(ret), K(ls_id_), KPC(ctx_));
  } else {
    common::ObSpinLockGuard ctx_guard(adpt_guard.get_adatper()->get_reload_lock());
    if (mgr->get_ls_leader() && adpt_guard.get_adatper()->get_reload_finish()) {
      // do nothing
    } else {
      int64_t est_mem = 0;
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(ObVecIndexAsyncTaskUtil::estimate_task_memory(ctx_, est_mem))) {
        LOG_WARN("fail to estimate merge task memory", K(tmp_ret), KPC(ctx_));
      }
      if (OB_NOT_NULL(ctx_)) {
        common::ObSpinLockGuard ctx_guard(ctx_->lock_);
        ctx_->task_status_.target_scn_ = read_snapshot_;
        ctx_->task_status_.task_info_.task_estimate_memory_ = est_mem;
      }
      if (OB_FAIL(ObPluginVectorIndexUtils::refresh_memdata(ls_id_,
                                                            adpt_guard.get_adatper(),
                                                            read_snapshot_,
                                                            allocator_,
                                                            ctx_))) {
        LOG_WARN("memdata sync fail to refresh memdata", KR(ret), K(ls_id_), KPC(ctx_));
      } else if (mgr->get_ls_leader()) {
        adpt_guard.get_adatper()->set_reload_finish(true);
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(mgr->get_adapter_inst_guard(ctx_->task_status_.tablet_id_, new_adpt_guard))) {
    LOG_WARN("memdata sync fail to get adapter instance", KR(ret), K(ls_id_), KPC(ctx_));
  }

  // Update adapter statistics
  if (OB_SUCC(ret)) {
    if (OB_NOT_NULL(new_adpt_guard.get_adatper())) {
      new_adpt_guard.get_adatper()->sync_succ();
    } else if (OB_NOT_NULL(adpt_guard.get_adatper())) {
      adpt_guard.get_adatper()->sync_succ();
    }
  } else {
    if (OB_NOT_NULL(new_adpt_guard.get_adatper())) {
      new_adpt_guard.get_adatper()->sync_fail(ret);
    } else if (OB_NOT_NULL(adpt_guard.get_adatper())) {
      adpt_guard.get_adatper()->sync_fail(ret);
    }
  }
  if (OB_NOT_NULL(ctx_)) {
    common::ObSpinLockGuard ctx_guard(ctx_->lock_);
    ctx_->task_status_.ret_code_ = ret;
  }
  int64_t cost = ObTimeUtil::current_time() - start_time;
  LOG_INFO("memdata sync finish process one", K(cost), K(allocator_.used()), K(allocator_.total()),
           K(ls_id_), KPC(ctx_));
  allocator_.reset();

  return ret;
}

// ==================== ObVecMemSyncExecutor Implementation ====================

int ObVecMemSyncExecutor::init(uint64_t tenant_id, ObLSHandle &ls_handle)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_ISNULL(ls_handle.get_ls())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_handle));
  } else if (OB_FAIL(ObVecITaskExecutor::init(tenant_id, ls_handle))) {
    LOG_WARN("fail to init base class", KR(ret), K(tenant_id));
  } else {
    cb_.executor_ = this;
    is_inited_ = true;
  }
  return ret;
}

int ObVecMemSyncExecutor::load_triggered_task(const ObVecIndexTaskStatus &task_row)
{
  int ret = OB_SUCCESS;
  bool need_submit = false;
  bool need_fallback_to_waiting_map = false;
  ObPluginVectorIndexMgr *index_ls_mgr = nullptr;
  ObVectorIndexTabletIDArray tablet_id_array;
  ObVectorIndexTableIDArray table_id_array;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("mem sync executor not inited", KR(ret));
  } else if (!check_operation_allow()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("mem sync not allowed due to data version", KR(ret));
  } else if (OB_FAIL(get_index_ls_mgr(index_ls_mgr))) {
    LOG_WARN("fail to get index ls mgr", KR(ret), K(tenant_id_), K(ls_handle_));
  } else if (OB_ISNULL(index_ls_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null index_ls_mgr", KR(ret));
  } else {
    ObTabletID tablet_id(task_row.tablet_id_);
    int64_t table_id = task_row.table_id_;
    // Try to submit clog directly for immediate follower sync.
    {
      common::ObSpinLockGuard ctx_guard(logging_lock_);
      if (is_logging_) {
        need_fallback_to_waiting_map = true;
      } else {
        if (OB_FAIL(tablet_id_array.push_back(tablet_id))) {
          LOG_WARN("fail to push tablet id", KR(ret), K(tablet_id));
        } else if (OB_FAIL(table_id_array.push_back(table_id))) {
          LOG_WARN("fail to push table id", KR(ret), K(table_id));
        } else {
          need_submit = true;
        }
      }
    }
    if (OB_SUCC(ret) && need_fallback_to_waiting_map) {
      int tmp_ret = index_ls_mgr->get_mem_sync_info().add_task_to_waiting_map(tablet_id, table_id);
      if (OB_SUCCESS != tmp_ret && OB_HASH_EXIST != tmp_ret) {
        ret = tmp_ret;
        LOG_WARN("fail to add tablet to waiting map", KR(ret), K(tablet_id), K(table_id));
      } else {
        LOG_INFO("triggered mem sync task: logging in progress, fallback to waiting map",
                 K(tablet_id), K(table_id));
      }
    }
    if (OB_SUCC(ret) && need_submit) {
      if (OB_FAIL(submit_log_(tablet_id_array, table_id_array))) {
        LOG_WARN("fail to submit mem sync clog for triggered task", KR(ret), K(tablet_id));
        // Clog submission failed, fallback to waiting_map so normal cycle picks it up.
        int tmp_ret = index_ls_mgr->get_mem_sync_info().add_task_to_waiting_map(tablet_id, table_id);
        if (OB_SUCCESS != tmp_ret && OB_HASH_EXIST != tmp_ret) {
          LOG_WARN("fail to add tablet to waiting map after clog failure",
                   K(tmp_ret), K(tablet_id), K(table_id));
        } else {
          ret = OB_SUCCESS;  // degraded successfully, do not report error to caller
        }
      } else {
        LOG_INFO("triggered mem sync task: clog submitted", K(tablet_id), K(table_id));
      }
    }
  }
  return ret;
}

bool ObVecMemSyncExecutor::check_operation_allow()
{
  int ret = OB_SUCCESS;
  bool bret = true;
  uint64_t tenant_data_version = 0;

  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id_, tenant_data_version))) {
    bret = false;
    LOG_WARN("get tenant data version failed", KR(ret), K(tenant_id_));
    // Mixed-version compat: align with the legacy ObPluginVectorIndexLoadScheduler
    // version floor (4_3_3_0). Mem sync runs purely in-memory during the upgrade
    // window — inner-table reads/writes are short-circuited at lower layers.
  } else if (tenant_data_version < DATA_VERSION_4_3_3_0) {
    bret = false;
    LOG_DEBUG("vector mem sync can not work with data version less than 4_3_3_0", K(tenant_data_version));
  }
  return bret;
}

int ObVecMemSyncExecutor::check_and_set_thread_pool()
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexMgr *index_ls_mgr = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("vector index load task not inited", K(ret));
  } else if (OB_ISNULL(vector_index_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), K_(tenant_id));
  } else if (OB_FAIL(get_index_ls_mgr(index_ls_mgr))) {
    LOG_WARN("fail to get index ls mgr", K(ret), K_(tenant_id), K_(ls_handle));
  } else if (0 == index_ls_mgr->get_complete_adapter_map().size()) {
    // no vector index exist on this LS, skip creating thread pool
  } else {
    ObVecIndexAsyncTaskHandler &thread_pool_handle = vector_index_service_->get_vec_async_task_handle();
    common::ObSpinLockGuard init_guard(thread_pool_handle.lock_);
    if (thread_pool_handle.get_tg_id() != INVALID_TG_ID) {
      // already initialized
    } else if (OB_FAIL(thread_pool_handle.init())) {
      LOG_WARN("fail to init vec async task handle", K(ret), K_(tenant_id));
    } else if (OB_FAIL(thread_pool_handle.start())) {
      LOG_WARN("fail to start thread pool", K(ret), K_(tenant_id));
    }
  }
  return ret;
}

// reuse code from ObPluginVectorIndexLoadScheduler::log_tablets_need_memdata_sync
int ObVecMemSyncExecutor::log_tablets_need_memdata_sync(ObPluginVectorIndexMgr *index_ls_mgr)
{
  int ret = OB_SUCCESS;
  bool need_submit_log = false;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
  bool is_leader = index_ls_mgr->get_ls_leader();
  ObVectorIndexTabletIDArray tablet_id_array;
  ObVectorIndexTableIDArray table_id_array;
  if (!need_refresh_) {
    need_refresh_ = index_ls_mgr->get_and_clear_need_refresh_memdata();
  }
  if (!tenant_config.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail get tenant_config", KR(ret), K_(tenant_id));
  } else if (need_refresh_ && !tenant_config->load_vector_index_on_follower && !is_leader) {
    need_refresh_ = false;
  }
  if (OB_SUCC(ret)) {
    common::ObSpinLockGuard ctx_guard(logging_lock_);
    if (is_logging_) {
      FLOG_INFO("vector index memdata sync is logging");
    } else {
      // follower just refresh adapter statistics, leader submit log need memdata sync
      RWLock::RLockGuard lock_guard(index_ls_mgr->get_adapter_map_lock());
      FOREACH_X(iter, index_ls_mgr->get_complete_adapter_map(), OB_SUCC(ret)) {
        ObPluginVectorIndexAdaptor *adapter = iter->second;
        bool need_sync = false;
        bool can_read_index = false;
        if (OB_ISNULL(adapter)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get null adapter", KR(ret), K_(tenant_id), K_(ls_handle));
        } else if (iter->first != adapter->get_inc_tablet_id()) {
          // do nothing
        } else if (OB_INVALID_ID == adapter->get_inc_table_id()) {
          // do nothing
        } else if (tablet_id_array.count() >= ObVecMemSyncLogCb::VECTOR_INDEX_MAX_SYNC_COUNT) {
          // do nothing, wait for next schedule
        } else if (OB_FAIL(adapter->check_snapshot_table_can_read_index(can_read_index))) {
          LOG_WARN("fail to check snapshot table can read index", KR(ret),
              K_(tenant_id), K_(ls_handle), KPC(adapter));
          ret = OB_SUCCESS;
        } else if (!can_read_index) {
          LOG_INFO("snapshot table not ready, skip memdata sync", K_(tenant_id),
              K_(ls_handle), KPC(adapter));
        } else if (!need_refresh_ && OB_FAIL(adapter->check_need_sync_to_follower_or_do_opt_task(index_ls_mgr, is_leader, need_sync))) {
          LOG_WARN("fail to check need memdata sync", KR(ret), K_(tenant_id), K_(ls_handle));
        } else if ((need_refresh_ || need_sync) && is_leader) {
          if (OB_FAIL(tablet_id_array.push_back(iter->first))) {
            LOG_WARN("fail to push tablet id", KR(ret));
          } else if (OB_FAIL(table_id_array.push_back(adapter->get_inc_table_id()))) {
            LOG_WARN("fail to push table id", KR(ret));
          } else {
            need_submit_log = true;
          }
        }
      }
    }
  }
  // Submit log or add to waiting map
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (need_submit_log) {
    if (need_refresh_) {
      if (OB_FAIL(index_ls_mgr->get_mem_sync_info().add_task_to_waiting_map(index_ls_mgr->get_complete_adapter_map()))) {
        TRANS_LOG(WARN, "fail to add complete adaptor to waiting map",KR(ret), K(tenant_id_));
      }
    } else if (tenant_config->load_vector_index_on_follower) {
      if (OB_FAIL(submit_log_(tablet_id_array, table_id_array))) {
        TRANS_LOG(WARN, "fail to submit vector index memdata sync log",KR(ret), K(tenant_id_), K(ls_handle_));
      } else {
        TRANS_LOG(INFO, "submit vector index memdata sync log success", KR(ret), K(need_refresh_), K(tenant_id_), K(ls_handle_));
      }
    }
  } else if (!is_leader && need_refresh_) {
    if (OB_FAIL(index_ls_mgr->get_mem_sync_info().add_task_to_waiting_map(index_ls_mgr->get_complete_adapter_map()))) {
      TRANS_LOG(WARN, "fail to add complete adaptor to waiting map",KR(ret), K(tenant_id_));
    }
  }
  if (OB_SUCC(ret)) {
    need_refresh_ = false;
  }
  return ret;
}

int ObVecMemSyncExecutor::load_task(uint64_t &task_trace_base_num)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexMgr *index_ls_mgr = nullptr;
  bool need_skip = false;
  bool need_sync = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("mem sync executor not init", KR(ret));
  } else if (!check_operation_allow()) {
    // skip
    need_skip = true;
  } else if (OB_FAIL(get_index_ls_mgr(index_ls_mgr))) {
    LOG_WARN("fail to get index ls mgr", K(ret), K_(tenant_id), K_(ls_handle));
  } else if (OB_ISNULL(index_ls_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index ls mgr is null", KR(ret));
  } else if (index_ls_mgr->get_async_task_opt().is_stop()) {
    need_skip = true;
    LOG_TRACE("skip loading mem sync task because ls task option is stopped",
              K_(tenant_id), K_(ls_handle));
  } else if (OB_FAIL(log_tablets_need_memdata_sync(index_ls_mgr))) {
    LOG_WARN("fail to log tablets need memdata sync", KR(ret), K_(tenant_id), K_(ls_handle));
  } else if (OB_FAIL(index_ls_mgr->check_need_mem_data_sync_task(need_sync, false))) {
    LOG_WARN("fail to check need mem data sync task", K(ret), K_(ls_handle));
  }

  ObArray<ObVecIndexAsyncTaskCtx*> task_ctx_array;
  // load task ctx from processing map to task_ctx_array
  if (OB_SUCC(ret) && !need_skip && need_sync) {
    VectorIndexMemSyncMap &processing_map = index_ls_mgr->get_mem_sync_info().get_processing_map();
    ObVecIndexAsyncTaskOption &task_opt = index_ls_mgr->get_async_task_opt();
    ObIAllocator *allocator = task_opt.get_allocator();
    FOREACH_X(iter, processing_map, OB_SUCC(ret)) {
      ObTabletID tablet_id = iter->first;
      ObVecIndexAsyncTaskCtx *task_ctx_tmp = iter->second;
      ObVecIndexAsyncTaskCtx *task_ctx = nullptr;
      char *task_ctx_buf = nullptr;
      bool inc_new_task = false;
      bool task_ctx_in_map = false;
      int64_t new_task_id = OB_INVALID_ID;
      common::ObCurTraceId::TraceId new_trace_id;
      if (OB_ISNULL(task_ctx_tmp)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr task_ctx_tmp", K(ret), K(tablet_id));
      } else if (OB_NOT_NULL(scheduler_) && scheduler_->is_task_disabled(
                     ObVecIndexAsyncTaskType::OB_VECTOR_ASYNC_MEM_SYNC_TASK, tablet_id.id())) {
        LOG_TRACE("[VEC_IDX_TASK_DISABLED] skip generating disabled task",
                  K_(tenant_id), K(tablet_id), "task_type", "MEM_SYNC");
      } else if (OB_ISNULL(task_ctx_buf =
                     static_cast<char *>(allocator->alloc(sizeof(ObVecIndexAsyncTaskCtx))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc task ctx buf", KR(ret));
      } else if (FALSE_IT(task_ctx = new(task_ctx_buf) ObVecIndexAsyncTaskCtx())) {
      } else if (FALSE_IT(task_ctx->task_status_ =
                     task_ctx_tmp->task_status_)) {
      } else if (OB_FAIL(ObVecIndexAsyncTaskUtil::fetch_new_task_id(tenant_id_, new_task_id))) {
        LOG_WARN("fail to fetch new task id", K(ret), K(tenant_id_));
      } else if (OB_FAIL(ObVecIndexAsyncTaskUtil::fetch_new_trace_id(++task_trace_base_num, allocator, new_trace_id))) {
        LOG_WARN("fail to fetch new trace id", K(ret), K(tablet_id));
      } else {
        LOG_INFO("[VEC_ASYNC_TASK] task loaded with PREPARE status",
                 K(ret), K(tablet_id), K(new_trace_id), K(new_task_id));
        task_ctx->tenant_id_ = tenant_id_;
        task_ctx->ls_handle_ = ls_handle_;
        task_ctx->task_status_.tenant_id_ = tenant_id_;
        task_ctx->task_status_.task_type_ = ObVecIndexAsyncTaskType::OB_VECTOR_ASYNC_MEM_SYNC_TASK;
        task_ctx->task_status_.trigger_type_ = ObVecIndexAsyncTaskTriggerType::OB_VEC_TRIGGER_AUTO;
        task_ctx->task_status_.status_ = ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_PREPARE;
        task_ctx->task_status_.exec_addr_ = GCTX.self_addr();
        task_ctx->task_status_.task_id_ = new_task_id;
        task_ctx->task_status_.trace_id_ = new_trace_id;
        task_ctx->allocator_.set_tenant_id(tenant_id_);
        if (OB_FAIL(index_ls_mgr->get_async_task_opt().add_task_ctx(tablet_id, task_ctx, inc_new_task))) {
          LOG_WARN("fail to add task ctx", KR(ret));
        } else if (FALSE_IT(task_ctx_in_map = inc_new_task)) {
        } else if (inc_new_task && OB_FAIL(task_ctx_array.push_back(task_ctx))) {
          int tmp_ret = OB_SUCCESS;
          if (OB_TMP_FAIL(ObVecIndexAsyncTaskUtil::clear_task_ctx(index_ls_mgr->get_async_task_opt(), task_ctx))) {
            LOG_WARN("fail to clear task ctx", KR(tmp_ret), K(task_ctx));
          }
          task_ctx_in_map = false;
          task_ctx = nullptr;
          LOG_WARN("fail to push back task status", KR(ret));
        }
      }
      if (OB_NOT_NULL(task_ctx) && (!OB_SUCC(ret) || !inc_new_task)) {
        if (task_ctx_in_map) {
          int tmp_ret = ObVecIndexAsyncTaskUtil::clear_task_ctx(index_ls_mgr->get_async_task_opt(), task_ctx);
          if (OB_SUCCESS != tmp_ret) {
            LOG_WARN("fail to clear task ctx after load task failed", K(tmp_ret), K(ret), K(task_ctx));
          }
        } else {
          task_ctx->~ObVecIndexAsyncTaskCtx();
          allocator->free(task_ctx);
        }
        task_ctx = nullptr;
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(insert_new_task(task_ctx_array))) {
      LOG_WARN("fail to insert new tasks", KR(ret));
    }
    // clear on fail
    if (OB_FAIL(ret) && !task_ctx_array.empty()) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(ObVecIndexAsyncTaskUtil::clear_task_ctxs(index_ls_mgr->get_async_task_opt(), task_ctx_array))) {
        LOG_WARN("fail to clear task ctx", KR(tmp_ret));
      }
    }

    if (OB_SUCC(ret)) {
      index_ls_mgr->get_mem_sync_info().get_processing_map().reuse();
      index_ls_mgr->get_mem_sync_info().get_processing_allocator().reset();
    }
    if (task_ctx_array.count() > 0) {
      LOG_INFO("finish load async task", KR(ret), K(ls_handle_.get_ls()->get_ls_id()), K(task_ctx_array.count()));
    }
  }
  return ret;
}

// reuse code from ObPluginVectorIndexLoadScheduler::submit_log_
int ObVecMemSyncExecutor::submit_log_(
    ObVectorIndexTabletIDArray &tablet_id_array,
    ObVectorIndexTableIDArray &table_id_array)
{
  int ret = OB_SUCCESS;
  common::ObSpinLockGuard ctx_guard(logging_lock_);

  if (is_logging_) {
    FLOG_INFO("vector index memdata sync is logging");
  } else if (OB_ISNULL(cb_.executor_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("executor is null", KR(ret));
  } else if (tablet_id_array.count() == 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get empty tablet id array", KR(ret));
  } else {
    ObVectorIndexSyncLog ls_log(tablet_id_array, table_id_array);
    const int64_t sync_task_count = tablet_id_array.count();
    palf::LSN lsn;
    SCN base_scn = SCN::min_scn();
    SCN scn;
    logservice::ObLogBaseHeader base_header(
        logservice::ObLogBaseType::VEC_INDEX_LOG_BASE_TYPE,
        logservice::ObReplayBarrierType::NO_NEED_BARRIER);

    uint32_t log_size = base_header.get_serialize_size() + ls_log.get_serialize_size();
    if (log_size > ObVecMemSyncLogCb::VECTOR_INDEX_SYNC_LOG_MAX_LENGTH) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("log size is too large", KR(ret), K(log_size), K(tablet_id_array.count()));
    } else if (OB_ISNULL(cb_.log_buffer_)) {
      cb_.log_buffer_ = static_cast<char *>(ob_malloc(
          ObVecMemSyncLogCb::VECTOR_INDEX_SYNC_LOG_MAX_LENGTH,
          ObMemAttr(tenant_id_, "VEC_INDEX_LOG")));
      if (OB_ISNULL(cb_.log_buffer_)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc vec index memdata sync log buffer", KR(ret), K(log_size));
      }
    }

    int64_t pos = 0;
    ObLS *ls = ls_handle_.get_ls();
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(ls)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls is null", KR(ret), K_(ls_handle));
    } else if (OB_FAIL(base_header.serialize(cb_.log_buffer_,
                                             ObVecMemSyncLogCb::VECTOR_INDEX_SYNC_LOG_MAX_LENGTH,
                                             pos))) {
      TRANS_LOG(WARN, "ObVectorIndexSyncLog serialize base header error",
        KR(ret), KP(cb_.log_buffer_), K(pos), K(sync_task_count), K(log_size));
    } else if (OB_FAIL(ls_log.serialize(cb_.log_buffer_,
                                        ObVecMemSyncLogCb::VECTOR_INDEX_SYNC_LOG_MAX_LENGTH,
                                        pos))) {
      TRANS_LOG(WARN, "ObVectorIndexSyncLog serialize vec index memdata sync log error",
        KR(ret), KP(cb_.log_buffer_), K(pos), K(sync_task_count), K(log_size));
    } else if (OB_FAIL(ls->get_log_handler()->append(cb_.log_buffer_,
                                                       pos,
                                                       base_scn,
                                                       false,
                                                       false,
                                                       &cb_,
                                                       lsn,
                                                       scn))) {
      cb_.reset();
      TRANS_LOG(WARN, "vector index memdata sync log submit error",
        KR(ret), KP(cb_.log_buffer_), K(pos), K(sync_task_count), K(log_size));
    } else {
      is_logging_ = true;
      TRANS_LOG(INFO, "submit vector index memdata sync log success",
        K(tenant_id_), K(ls->get_ls_id()), K(sync_task_count), K(log_size),
        K(base_scn), K(lsn), K(scn), K(tablet_id_array.count()));
    }
  }
  return ret;
}

int ObVecMemSyncExecutor::handle_submit_callback(const bool success)
{
  int ret = OB_SUCCESS;
  common::ObSpinLockGuard ctx_guard(logging_lock_);
  is_logging_ = false;
  LOG_INFO("submit vector index memdata sync log callback",
           K_(tenant_id), K(ls_handle_), K(success));
  return ret;
}

} // namespace share
} // namespace oceanbase
