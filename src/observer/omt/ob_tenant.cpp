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

#define USING_LOG_PREFIX SERVER_OMT
#include "ob_tenant.h"

#include "share/ob_define.h"
#include "lib/container/ob_vector.h"
#include "lib/time/ob_time_utility.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/stat/ob_session_stat.h"
#include "share/config/ob_server_config.h"
#include "sql/engine/px/ob_px_admission.h"
#include "share/interrupt/ob_global_interrupt_call.h"
#include "ob_th_worker.h"
#include "ob_multi_tenant.h"
#include "observer/ob_server_struct.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_schema_utils.h"
#include "share/resource_manager/ob_resource_manager.h"
#include "sql/engine/px/ob_px_target_mgr.h"
#include "logservice/palf/palf_options.h"
#include "sql/dtl/ob_dtl_fc_server.h"
#include "observer/mysql/ob_mysql_request_manager.h"
#include "observer/ob_srv_deliver.h"
#include "observer/ob_srv_network_frame.h"
#include "storage/tx/wrs/ob_tenant_weak_read_service.h"
#include "sql/engine/ob_tenant_sql_memory_manager.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "lib/worker.h"
#include "ob_tenant_mtl_helper.h"
#include "storage/ob_file_system_router.h"
#include "storage/slog/ob_storage_logger.h"
#include "storage/slog/ob_storage_logger_manager.h"
#include "storage/ob_file_system_router.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "storage/shared_storage/ob_disk_space_manager.h"
#endif
#include "common/ob_smart_var.h"
#include "rpc/obmysql/ob_sql_nio_server.h"
#include "rpc/obrpc/ob_rpc_stat.h"
#include "rpc/obrpc/ob_rpc_packet.h"
#include "lib/container/ob_array.h"
#include "share/rc/ob_tenant_module_init_ctx.h"
#include "share/resource_manager/ob_cgroup_ctrl.h"
#include "sql/engine/px/ob_px_worker.h"
#include "lib/thread/protected_stack_allocator.h"
#include "lib/stat/ob_diagnostic_info_guard.h"

using namespace oceanbase::lib;
using namespace oceanbase::common;
using namespace oceanbase::omt;
using namespace oceanbase::rpc;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::storage;
using namespace oceanbase::sql::dtl;
using namespace oceanbase::obrpc;

#define GET_OTHER_TSI_ADDR(var_name, addr) \
const int64_t var_name##_offset = ((int64_t)addr - (int64_t)pthread_self()); \
decltype(*addr) var_name = *(decltype(addr))(thread_base + var_name##_offset);

#define EXPAND_INTERVAL (1 * 1000 * 1000)
#define SHRINK_INTERVAL (1 * 1000 * 1000)
#define SLEEP_INTERVAL (60 * 1000 * 1000)

extern "C" {
int ob_pthread_create(void **ptr, void *(*start_routine) (void *), void *arg);
int ob_pthread_tryjoin_np(void *ptr);
}
void MultiLevelReqCnt::atomic_inc(const int32_t level)
{
  if (level < 0 || level >= MAX_REQUEST_LEVEL) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "unexpected level", K(level));
  } else {
    ATOMIC_INC(&cnt_[level]);
  }
}

int ObPxPools::init(uint64_t tenant_id)
{
  static int PX_POOL_COUNT = 128; // 128 groups, generally enough
  int ret = OB_SUCCESS;
  tenant_id_ = tenant_id;
  ObMemAttr attr(tenant_id, "PxPoolBkt");
  if (OB_FAIL(pool_map_.create(PX_POOL_COUNT, attr, attr))) {
    LOG_WARN("fail init pool map", K(ret));
  }
  return ret;
}

int ObPxPools::get_or_create(int64_t group_id, ObPxPool *&pool)
{
  int ret = OB_SUCCESS;
  if (!pool_map_.created()) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(pool_map_.get_refactored(group_id, pool))) {
    if (OB_HASH_NOT_EXIST == ret) {
      if (OB_FAIL(create_pool(group_id, pool))) {
        LOG_WARN("fail create pool", K(ret), K(group_id));
      }
    } else {
      LOG_WARN("fail get group id from hashmap", K(ret), K(group_id));
    }
  }
  return ret;
}

int ObPxPools::create_pool(int64_t group_id, ObPxPool *&pool)
{
  static constexpr uint64_t MAX_TASKS_PER_CPU = 1;
  int ret = OB_SUCCESS;
  common::SpinWLockGuard g(lock_);
  if (OB_FAIL(pool_map_.get_refactored(group_id, pool))) {
    if (OB_HASH_NOT_EXIST == ret) {
      pool = OB_NEW(ObPxPool, ObMemAttr(tenant_id_, "PxPool"));
      if (OB_ISNULL(pool)) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
      } else {
        pool->set_tenant_id(tenant_id_);
        pool->set_group_id(group_id);
        pool->set_run_wrapper(MTL_CTX());
        if (OB_FAIL(pool->start())) {
          LOG_WARN("fail startup px pool", K(group_id), K(tenant_id_), K(ret));
        } else if (OB_FAIL(pool_map_.set_refactored(group_id, pool))) {
          LOG_WARN("fail set pool to hashmap", K(group_id), K(ret));
        }
      }
    } else {
      LOG_WARN("fail get group id from hashmap", K(ret), K(group_id));
    }
  }
  return ret;
}

int ObPxPools::thread_recycle()
{
  int ret = OB_SUCCESS;
  common::SpinWLockGuard g(lock_);
  ThreadRecyclePoolFunc recycle_pool_func;
  if (OB_FAIL(pool_map_.foreach_refactored(recycle_pool_func))) {
    LOG_WARN("failed to do foreach", K(ret));
  }
  return ret;
}

int ObPxPools::ThreadRecyclePoolFunc::operator() (common::hash::HashMapPair<int64_t, ObPxPool*> &kv)
{
  int ret = OB_SUCCESS;
  int64_t &group_id = kv.first;
  ObPxPool *pool = kv.second;
  if (NULL == pool) {
    LOG_WARN("pool is null", K(group_id));
  } else {
    IGNORE_RETURN pool->thread_recycle();
  }
  return ret;
}

int ObPxPools::StopPoolFunc::operator() (common::hash::HashMapPair<int64_t, ObPxPool*> &kv)
{
  int ret = OB_SUCCESS;
  int64_t &group_id = kv.first;
  ObPxPool *pool = kv.second;
  if (NULL == pool) {
    LOG_WARN("pool is null", K(group_id));
  } else {
    pool->stop();
    LOG_INFO("DEL_POOL_STEP_1: mark px pool stop succ!", K(group_id));
  }
  return ret;
}

int ObPxPools::DeletePoolFunc::operator() (common::hash::HashMapPair<int64_t, ObPxPool*> &kv)
{
  int ret = OB_SUCCESS;
  int64_t &group_id = kv.first;
  ObPxPool *pool = kv.second;
  if (NULL == pool) {
    LOG_WARN("pool is null", K(group_id));
  } else {
    pool->wait();
    LOG_INFO("DEL_POOL_STEP_2: wait pool empty succ!", K(group_id));
    pool->destroy();
    LOG_INFO("DEL_POOL_STEP_3: pool destroy succ!", K(group_id), K(pool->get_queue_size()));
    common::ob_delete(pool);
  }
  return ret;
}

void ObPxPools::mtl_stop(ObPxPools *&pools)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(pools)) {
    // ignore ret
    // pools will be null if it's creating tenant and failed.
    LOG_WARN("pools is null");
  } else {
    common::SpinWLockGuard g(pools->lock_);
    StopPoolFunc stop_pool_func;
    if (OB_FAIL(pools->pool_map_.foreach_refactored(stop_pool_func))) {
      LOG_WARN("failed to do foreach", K(ret));
    }
  }
}

void ObPxPools::destroy()
{
  int ret = OB_SUCCESS;
  common::SpinWLockGuard g(lock_);
  DeletePoolFunc free_pool_func;
  if (OB_FAIL(pool_map_.foreach_refactored(free_pool_func))) {
    LOG_WARN("failed to do foreach", K(ret));
  } else {
    pool_map_.destroy();
    tenant_id_ = OB_INVALID_ID;
  }
}

int ObPxPool::submit(const RunFuncT &func)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    queue_.set_limit(common::ObServerConfig::get_instance().tenant_task_queue_size);
    is_inited_ = true;
  }
  disable_recycle();
  ATOMIC_INC(&concurrency_);
  if (ATOMIC_LOAD(&active_threads_) < ATOMIC_LOAD(&concurrency_)) {
    ret = OB_SIZE_OVERFLOW;
  } else {
    Task *t = OB_NEW(Task, ObMemAttr(tenant_id_, "PxTask"), func);
    if (OB_ISNULL(t)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (OB_FAIL(queue_.push(static_cast<ObLink*>(t), 0))) {
      LOG_ERROR("px push queue failed", K(ret));
    }
  }
  if (ret != OB_SUCCESS) {
    ATOMIC_DEC(&concurrency_);
  }
  enable_recycle();
  return ret;
}

void ObPxPool::handle(ObLink *task)
{
  Task *t  = static_cast<Task*>(task);
  if (t == nullptr) {
    LOG_ERROR_RET(OB_INVALID_ARGUMENT, "px task is invalid");
  } else {
    bool need_exec = true;
    t->func_(need_exec);
    OB_DELETE(Task, "PxTask", t);
  }
  ATOMIC_DEC(&concurrency_);
}

void ObPxPool::set_px_thread_name()
{
  char buf[32];
  snprintf(buf, 32, "PX_G%ld", group_id_);
  ob_get_tenant_id() = tenant_id_;
  lib::set_thread_name(buf);
}

void ObPxPool::run(int64_t idx)
{
  ATOMIC_INC(&active_threads_);
  set_thread_idx(idx);
  // Create worker for current thread.
  ObPxWorker worker;
  Worker::set_worker_to_thread_local(&worker);
  run1();
}


void ObPxPool::run1()
{
  int ret = OB_SUCCESS;
  set_px_thread_name();
  ObTLTaGuard ta_guard(tenant_id_);
  common::ObBackGroundSessionGuard backgroud_session_guard(tenant_id_, group_id_);
  ObLocalDiagnosticInfo::set_thread_name(ob_get_tenant_id(), "PxWorker");
  auto *pm = common::ObPageManager::thread_local_instance();
  if (OB_LIKELY(nullptr != pm)) {
    pm->set_tenant_ctx(tenant_id_, common::ObCtxIds::DEFAULT_CTX_ID);
  }
  //ObTaTLCacheGuard ta_guard(tenant_id_);
  CLEAR_INTERRUPTABLE();
  ObCgroupCtrl *cgroup_ctrl = GCTX.cgroup_ctrl_;
  LOG_INFO("run px pool", K(group_id_), K(tenant_id_), K_(active_threads));
  SET_GROUP_ID(group_id_);

	if (!is_inited_) {
    queue_.set_limit(common::ObServerConfig::get_instance().tenant_task_queue_size);
    is_inited_ = true;
  }

  ObLink *task = nullptr;
  int64_t idle_time = 0;
  while (!Thread::current().has_set_stop()) {
	  if (!is_inited_) {
      ob_usleep(10 * 1000L);
    } else {
      if (OB_SUCC(queue_.pop(task, QUEUE_WAIT_TIME))) {
        handle(task);
        idle_time = 0; // reset recycle timer
      } else {
        idle_time += QUEUE_WAIT_TIME;
        // if idle for more than 10 min, exit thread
        try_recycle(idle_time);
      }
    }
  }
}

void ObPxPool::try_recycle(int64_t idle_time)
{
  // recycle thread policy:
  // 1. first N threads reserved for first 10 min idle period
  // 2. no thread reserved after 1 hour idle period
  //
  // impl. note: must ensure active_threads_ > concurrency_, otherwise may hang task
  const int N = 8;
  if ((idle_time > 10LL * 60 * 1000 * 1000 && get_thread_count() >= N)
      || idle_time > 60LL * 60 * 1000 * 1000) {
    if (OB_SUCCESS == recycle_lock_.trylock()) {
      if (ATOMIC_LOAD(&active_threads_) > ATOMIC_LOAD(&concurrency_)) {
        ATOMIC_DEC(&active_threads_);
        // when thread marked as stopped,
        // it will exit the event loop and recycled by background deamon
        Thread::current().stop();
      }
      recycle_lock_.unlock();
    }
  }
}

void ObPxPool::stop()
{
  int ret = OB_SUCCESS;
  Threads::stop();
  ObLink *task = nullptr;
  bool need_exec = false;
  while (OB_SUCC(queue_.pop(task, QUEUE_WAIT_TIME))) {
    Task *t  = static_cast<Task*>(task);
    if (OB_NOT_NULL(t)) {
      t->func_(need_exec);
      OB_DELETE(Task, "PxTask", t);
    }
    ATOMIC_DEC(&concurrency_);
  }
}

ObResourceGroup::ObResourceGroup(uint64_t group_id, ObTenant* tenant, share::ObCgroupCtrl *cgroup_ctrl):
  ObResourceGroupNode(group_id),
  workers_lock_(tenant->workers_lock_),
  inited_(false),
  recv_req_cnt_(0),
  shrink_(false),
  token_change_ts_(0),
  nesting_worker_cnt_(0),
  tenant_(tenant),
  cgroup_ctrl_(cgroup_ctrl)
{
}

int ObResourceGroup::init()
{
  int ret = OB_SUCCESS;
  if (nullptr == tenant_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("group init failed");
  } else if (FALSE_IT(multi_level_queue_.set_limit(common::ObServerConfig::get_instance().tenant_task_queue_size))) {
    LOG_WARN("multi level queue set limit failed", K(ret), K(tenant_->id()), K(group_id_), K(*this));
  } else {
    req_queue_.set_limit(common::ObServerConfig::get_instance().tenant_task_queue_size);
    inited_ = true;
  }
  return ret;
}

void ObResourceGroup::update_queue_size()
{
  req_queue_.set_limit(common::ObServerConfig::get_instance().tenant_task_queue_size);
}

int ObResourceGroup::acquire_level_worker(int32_t level)
{
  int ret = OB_SUCCESS;
  ObTenantSwitchGuard guard(tenant_);

  if (level <= 0 || level > MAX_REQUEST_LEVEL) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected level", K(level), K(tenant_->id()));
  } else {
    ObThWorker *w = nullptr;
    if (OB_FAIL(create_worker(w, tenant_, group_id_, level, true /*ignore max worker limit*/, this))) {
      LOG_WARN("create worker failed", K(ret));
    } else if (!nesting_workers_.add_last(&w->worker_node_)) {
      OB_ASSERT(false);
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("add worker to list fail", K(ret));
    }
  }
  return ret;
}


int ObResourceGroup::acquire_more_worker(int64_t num, int64_t &succ_num, bool force)
{
  int ret = OB_SUCCESS;
  ObTenantSwitchGuard guard(tenant_);

  const auto need_num = num;
  succ_num = 0;

  while (OB_SUCC(ret) && need_num > succ_num) {
    ObThWorker *w = nullptr;
    if (OB_FAIL(create_worker(w, tenant_, group_id_, INT32_MAX, force, this))) {
      LOG_WARN("create worker failed", K(ret));
    } else if (!workers_.add_last(&w->worker_node_)) {
      OB_ASSERT(false);
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("add worker to list fail", K(ret));
    } else {
      succ_num++;
    }
  }

  if (need_num != num ||  // Reach worker count bound,
      succ_num != need_num  // or can't allocate enough worker.
     ) {
    if (TC_REACH_TIME_INTERVAL(10000000)) {
      LOG_WARN("Alloc group worker less than lack", K(num), K(need_num), K(succ_num));
    }
  }

  return ret;
}

inline bool is_dbms_job_group(int64_t group_id)
{
  return share::OBCG_DBMS_SCHED_JOB == group_id || share::OBCG_OLAP_ASYNC_JOB == group_id;
}

void ObResourceGroup::check_worker_count()
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(workers_lock_.trylock())) {
    if ((is_resource_manager_group(group_id_) || is_dbms_job_group(group_id_))
      && nesting_worker_cnt_ < (MAX_REQUEST_LEVEL - GROUP_MULTI_LEVEL_THRESHOLD)) {
      for (int level = GROUP_MULTI_LEVEL_THRESHOLD + nesting_worker_cnt_; OB_SUCC(ret) && level < MAX_REQUEST_LEVEL; level++) {
        if (OB_SUCC(acquire_level_worker(level))) {
          nesting_worker_cnt_ = nesting_worker_cnt_ + 1;
        }
      }
    }
    int64_t now = ObTimeUtility::current_time();
    bool enable_dynamic_worker = true;
    int64_t threshold = 3 * 1000;
    {
      ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_->id()));
      enable_dynamic_worker = tenant_config.is_valid() ? tenant_config->_ob_enable_dynamic_worker : true;
      threshold = tenant_config.is_valid() ? tenant_config->_stall_threshold_for_dynamic_worker : 3 * 1000;
    }
    int64_t blocking_cnt = 0;
    DLIST_FOREACH_REMOVESAFE(wnode, workers_) {
      const auto w = static_cast<ObThWorker*>(wnode->get_data());
      if (w->has_set_stop()) {
        workers_.remove(wnode);
        destroy_worker(w);
      } else if (w->has_req_flag()
                 && 0 != w->blocking_ts()
                 && now - w->blocking_ts() >= threshold
                 && enable_dynamic_worker) {
        ++blocking_cnt;
      }
    }

    int64_t token = 0;
    bool is_group_critical = share::ObCgSet::instance().is_group_critical(group_id_) || is_resource_manager_group(group_id_);
    if (is_group_critical) {
      token = 1 + blocking_cnt;
      token = std::min(token, max_worker_cnt());
      token = std::max(token, min_worker_cnt());
    } else {
      int64_t queue_size = req_queue_.size() + multi_level_queue_.get_total_size();
      if (queue_size == 0) {
        token = 0;
      } else {
        token = max(1 + blocking_cnt, min(workers_.get_size() + queue_size, min_worker_cnt()));
        token = std::min(token, max_worker_cnt());
      }
    }

    int64_t succ_num = 0L;
    int64_t shrink_ts = (token == 0 && workers_.get_size() == 1) ? SLEEP_INTERVAL : SHRINK_INTERVAL;
    int64_t diff = token < min_worker_cnt() ? token - workers_.get_size() : min_worker_cnt() - workers_.get_size();
    if (OB_UNLIKELY(diff > 0)) {
      token_change_ts_ = now;
      ATOMIC_STORE(&shrink_, false);
      acquire_more_worker(diff, succ_num, /* force */ true);
      LOG_INFO("worker thread created", K(tenant_->id()), K(group_id_), K(token));
    } else if (OB_UNLIKELY(workers_.get_size() < token) &&
               OB_LIKELY(ObMallocAllocator::get_instance()->get_tenant_remain(tenant_->id()) >
                         ObMallocAllocator::get_instance()->get_tenant_limit(tenant_->id()) * 0.05)) {
      ATOMIC_STORE(&shrink_, false);
      if (OB_LIKELY(now - token_change_ts_ >= EXPAND_INTERVAL)) {
        token_change_ts_ = now;
        acquire_more_worker(1, succ_num);
        LOG_INFO("worker thread created", K(tenant_->id()), K(group_id_), K(token));
      }
    } else if (OB_UNLIKELY(workers_.get_size() > token) && OB_LIKELY(now - token_change_ts_ >= shrink_ts)) {
      token_change_ts_ = now;
      ATOMIC_STORE(&shrink_, true);
      LOG_INFO("worker thread began to shrink", K(tenant_->id()), K(group_id_), K(token));
    }
    IGNORE_RETURN workers_lock_.unlock();
  }
}

void ObResourceGroup::check_worker_count(ObThWorker &w)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ATOMIC_LOAD(&shrink_))
      && OB_LIKELY(ATOMIC_BCAS(&shrink_, true, false))) {
    w.stop();
    LOG_INFO("worker thread exit", K(tenant_->id()), K(workers_.get_size()));
  }
}

int ObResourceGroup::clear_worker()
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(workers_lock_);

  int tmp_ret = OB_SUCCESS;
  const int64_t timeout = 10 * 1000;
  ObLink* task = nullptr;
  rpc::ObRequest *req = nullptr;
  while (req_queue_.size() > 0) {
    if (OB_TMP_FAIL(req_queue_.pop(task, timeout))) {
      LOG_WARN("req queue pop task fail", K(tmp_ret), K(&req_queue_));
    } else if (NULL != task) {
      req = static_cast<rpc::ObRequest*>(task);
      on_translate_fail(req, OB_TENANT_NOT_IN_SERVER);
    } else {
      LOG_ERROR("req queue pop successfully but task is NULL");
    }
  }

  for (int32_t level = 0; level < MULTI_LEVEL_QUEUE_SIZE; level++) {
    while (multi_level_queue_.get_size(level) > 0) {
      if (OB_TMP_FAIL(multi_level_queue_.pop(task, level, timeout))) {
        LOG_WARN("req queue pop task fail", K(tmp_ret), K(&multi_level_queue_));
      } else if (NULL != task) {
        req = static_cast<rpc::ObRequest*>(task);
        on_translate_fail(req, OB_TENANT_NOT_IN_SERVER);
      } else {
        LOG_ERROR("multi level queue pop successfully but task is NULL");
      }
    }
  }


  while (nesting_workers_.get_size() > 0) {
    int ret = OB_SUCCESS;
    DLIST_FOREACH_REMOVESAFE(wnode, nesting_workers_) {
      ObThWorker *w = static_cast<ObThWorker*>(wnode->get_data());
      nesting_workers_.remove(wnode);
      destroy_worker(w);
    }
    if (REACH_TIME_INTERVAL(10 * 1000L * 1000L)) {
      LOG_INFO(
          "Tenant has some group nesting workers need stop",
          K(tenant_->id()),
          "group nesting workers", nesting_workers_.get_size(),
          "group id", get_group_id());
    }
  }
  while (workers_.get_size() > 0) {
    int ret = OB_SUCCESS;
    DLIST_FOREACH_REMOVESAFE(wnode, workers_) {
      const auto w = static_cast<ObThWorker*>(wnode->get_data());
      workers_.remove(wnode);
      destroy_worker(w);
    }
    if (REACH_TIME_INTERVAL(10 * 1000L * 1000L)) {
      LOG_INFO(
          "Tenant has some group workers need stop",
          K(tenant_->id()),
          "group workers", workers_.get_size(),
          "group id", get_group_id());
    }
    ob_usleep(10L * 1000L);
  }
  return ret;
}

int ObResourceGroup::get_throttled_time(int64_t &throttled_time)
{
  int ret = OB_SUCCESS;
  int64_t current_throttled_time_us = -1;
  if (OB_FAIL(GCTX.cgroup_ctrl_->get_throttled_time(tenant_->id(), current_throttled_time_us, group_id_))) {
    LOG_WARN("get throttled time failed", K(ret), K(tenant_->id()), K(group_id_));
  } else if (current_throttled_time_us > 0) {
    throttled_time = current_throttled_time_us - throttled_time_us_;
    throttled_time_us_ = current_throttled_time_us;
  }
  return ret;
}

int GroupMap::create_and_insert_group(uint64_t group_id, ObTenant *tenant, ObCgroupCtrl *cgroup_ctrl, ObResourceGroup *&group)
{
  int ret = OB_SUCCESS;
  if (nullptr == tenant
   || nullptr == cgroup_ctrl) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    const int64_t alloc_size = sizeof(ObResourceGroup);
    ObResourceGroup *buf = nullptr;
    if (nullptr == (buf = (ObResourceGroup*)ob_malloc(alloc_size, ObMemAttr(tenant->id(), "ResourceGroup")))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      group = new(buf)ObResourceGroup(group_id, tenant, cgroup_ctrl);
      if (OB_FAIL(group->init())) {
        LOG_ERROR("group init failed", K(ret), K(group_id));
      } else if (OB_FAIL(err_code_map(insert(group)))) {
        LOG_WARN("groupmap insert group failed", K(ret), K(group->get_group_id()), K(tenant->id()));
      }
      if (OB_SUCCESS != ret) {
        group->~ObResourceGroup();
        ob_free(group);
      } else {
        group->check_worker_count();
      }
    }
  }
  return ret;
}

void GroupMap::wait_group()
{
  int ret = OB_SUCCESS;
  ObResourceGroupNode* iter = NULL;
  while (nullptr != (iter = quick_next(iter))) {
    ObResourceGroup *group = static_cast<ObResourceGroup*>(iter);
    if (OB_FAIL(group->clear_worker())) {
      LOG_ERROR("group clear worker failed", K(ret));
    }
  }
}

void GroupMap::destroy_group()
{
  int ret = OB_SUCCESS;
  ObResourceGroupNode* iter = NULL;
  while (nullptr != (iter = quick_next(iter))) {
    ObResourceGroup *group = static_cast<ObResourceGroup*>(iter);
    if (OB_SUCC(err_code_map(del(iter, iter)))) {
      group->~ObResourceGroup();
      ob_free(group);
      iter = NULL;
    } else {
      LOG_ERROR("drop group failed", K(ret));
    }
  }
}

int GroupMap::err_code_map(int err)
{
  int ret = OB_SUCCESS;
  switch (err) {
    case 0:          ret = OB_SUCCESS; break;
    case -ENOENT:    ret = OB_ENTRY_NOT_EXIST; break;
    case -EAGAIN:    ret = OB_EAGAIN; break;
    case -ENOMEM:    ret = OB_ALLOCATE_MEMORY_FAILED; break;
    case -EEXIST:    ret = OB_ENTRY_EXIST; break;
    case -EOVERFLOW: ret = OB_SIZE_OVERFLOW; break;
    default:         ret = OB_ERROR;
  }
  return ret;
}

int64_t RpcStatInfo::to_string(char *buf, const int64_t len) const
{
  int64_t pos = 0;
  int ret = OB_SUCCESS;
  struct PcodeDcount{
    obrpc::ObRpcPacketCode pcode_;
    int64_t dcount_;
    bool operator <(const PcodeDcount &other) const { return dcount_ > other.dcount_; }
    int64_t to_string(char* buf, const int64_t len) const { UNUSED(buf); UNUSED(len); return 0L; }
  };
  SMART_VAR(ObArray<PcodeDcount>, pd_array) {
    ObRpcPacketSet &set = ObRpcPacketSet::instance();
    for (int64_t pcode_idx = 0; (OB_SUCCESS == ret) && (pcode_idx < ObRpcPacketSet::THE_PCODE_COUNT); pcode_idx++) {
      PcodeDcount pd_item;
      RpcStatItem item;
      if (OB_FAIL(rpc_stat_srv_.get(pcode_idx, item))) {
        //continue
      } else if (item.dcount_ != 0) {
        pd_item.pcode_ = set.pcode_of_idx(pcode_idx);
        pd_item.dcount_ = item.dcount_;
        if (OB_FAIL(pd_array.push_back(pd_item))) {
          //break
        }
      }
    }
    if (OB_SUCC(ret) && pd_array.size() > 0) {
      std::make_heap(pd_array.begin(), pd_array.end());
      std::sort_heap(pd_array.begin(), pd_array.end());
      for (int i = 0; i < min(5, pd_array.size()); i++) {
        databuff_printf(buf, len, pos, " pcode=0x%x:cnt=%ld",
          pd_array.at(i).pcode_, pd_array.at(i).dcount_);
      }
    }
  }
  for (int64_t pcode_idx = 0; pcode_idx < ObRpcPacketSet::THE_PCODE_COUNT; pcode_idx++) {
    RpcStatPiece piece;
    piece.reset_dcount_ = true;
    rpc_stat_srv_.add(pcode_idx, piece);
  }
  return pos;
}


ObTenant::ObTenant(const int64_t id,
                   const int64_t epoch,
                   const int64_t times_of_workers,
                   ObCgroupCtrl &cgroup_ctrl)
    : ObTenantBase(id, epoch, true),
      meta_lock_(),
      tenant_meta_(),
      shrink_(0),
      total_worker_cnt_(0),
      gc_thread_(nullptr),
      has_created_(false),
      stopped_(0),
      wait_mtl_finished_(false),
      req_queue_(),
      multi_level_queue_(nullptr),
      recv_hp_rpc_cnt_(0),
      recv_np_rpc_cnt_(0),
      recv_lp_rpc_cnt_(0),
      recv_mysql_cnt_(0),
      recv_task_cnt_(0),
      recv_sql_task_cnt_(0),
      recv_large_req_cnt_(0),
      pause_cnt_(0),
      resume_cnt_(0),
      recv_retry_on_lock_rpc_cnt_(0),
      recv_retry_on_lock_mysql_cnt_(0),
      tt_large_quries_(0),
      pop_normal_cnt_(0),
      group_map_(group_map_buf_, sizeof(group_map_buf_)),
      lock_(),
      rpc_stat_info_(nullptr),
      mtl_init_ctx_(nullptr),
      workers_lock_(common::ObLatchIds::TENANT_WORKER_LOCK),
      cgroup_ctrl_(cgroup_ctrl),
      disable_user_sched_(false),
      token_usage_(.0),
      token_usage_check_ts_(0),
      token_change_ts_(0),
      ctx_(nullptr),
      st_metrics_(),
      sql_limiter_(),
      worker_us_(0),
      cpu_time_us_(0)
{
  token_usage_check_ts_ = ObTimeUtility::current_time();
  lock_.set_diagnose(true);
}

ObTenant::~ObTenant() {}

int ObTenant::init_ctx()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(CREATE_ENTITY(ctx_, this))) {
    LOG_WARN("create tenant ctx failed", K(ret));
  } else if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(ret));
  }
  return ret;
}

int ObTenant::init(const ObTenantMeta &meta)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(ObTenantBase::init(&cgroup_ctrl_))) {
    LOG_WARN("fail to init tenant base", K(ret));
  } else if (FALSE_IT(req_queue_.set_limit(GCONF.tenant_task_queue_size))) {
  } else if (OB_ISNULL(multi_level_queue_ = OB_NEW(ObMultiLevelQueue, ObMemAttr(id_, "MulLevelQueue")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc ObMultiLevelQueue failed", K(ret), K(*this));
  } else if (FALSE_IT(multi_level_queue_->set_limit(common::ObServerConfig::get_instance().tenant_task_queue_size))) {
  } else if (OB_ISNULL(rpc_stat_info_ = OB_NEW(RpcStatInfo, ObMemAttr(id_, "RpcStatInfo"), id_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc RpcStatInfo failed", K(ret), K(*this));
  } else if (OB_FAIL(construct_mtl_init_ctx(meta, mtl_init_ctx_))) {
    LOG_WARN("construct_mtl_init_ctx failed", KR(ret), K(*this));
  } else {
    ObTenantBase::mtl_init_ctx_ = mtl_init_ctx_;
    tenant_meta_ = meta;
    set_unit_min_cpu(meta.unit_.config_.min_cpu());
    set_unit_max_cpu(meta.unit_.config_.max_cpu());
    const int64_t memory_size = static_cast<double>(tenant_meta_.unit_.config_.memory_size());
    set_unit_memory_size(memory_size);
    const int64_t data_disk_size = tenant_meta_.unit_.config_.data_disk_size();
    set_unit_data_disk_size(data_disk_size);
    constexpr static int64_t MINI_MEM_UPPER = 1L<<30; // 1G
    update_mini_mode(memory_size <= MINI_MEM_UPPER);

    if (!is_virtual_tenant_id(id_)) {
      if (OB_FAIL(create_tenant_module())) {
        // do nothing
      } else if (OB_FAIL(OB_PX_TARGET_MGR.add_tenant(id_))) {
        LOG_WARN("add tenant into px target mgr failed", K(ret), K(id_));
      } else if (OB_FAIL(G_RES_MGR.get_col_mapping_rule_mgr().add_tenant(id_))) {
        LOG_WARN("add tenant into res col maping rule mgr failed", K(ret), K(id_));
      }
    } else {
      disable_user_sched(); // disable_user_sched for virtual tenant
    }
  }

  if (OB_SUCC(ret)) {
    int64_t succ_cnt = 0L;
    if (OB_FAIL(acquire_more_worker(2, succ_cnt))) {
      LOG_WARN("create worker in init failed", K(ret), K(succ_cnt));
    } else {
      // there must be 2 workers.
      static_cast<ObThWorker*>(workers_.get_first()->get_data())->set_priority_limit(QQ_HIGH);
      static_cast<ObThWorker*>(workers_.get_last()->get_data())->set_priority_limit(QQ_NORMAL);
      for (int level = MULTI_LEVEL_THRESHOLD; OB_SUCC(ret) && level < MAX_REQUEST_LEVEL; level++) {
        if (OB_SUCC(acquire_level_worker(1, succ_cnt, level))) {
          succ_cnt = 0L;
        }
      }
      timeup();
    }
  }

  if (OB_FAIL(ret)) {
    LOG_ERROR("fail to create tenant module", K(ret));
  } else {
    start();
  }

  return ret;
}

int ObTenant::construct_mtl_init_ctx(const ObTenantMeta &meta, share::ObTenantModuleInitCtx *&ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx = OB_NEW(share::ObTenantModuleInitCtx, ObMemAttr(id_, "ModuleInitCtx")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc ObTenantModuleInitCtx failed", K(ret));
  } else if (OB_FAIL(OB_FILE_SYSTEM_ROUTER.get_tenant_clog_dir(id_, mtl_init_ctx_->tenant_clog_dir_))) {
    LOG_ERROR("get_tenant_clog_dir failed", K(ret));
  } else {
    mtl_init_ctx_->palf_options_.disk_options_.log_disk_usage_limit_size_ = meta.unit_.config_.log_disk_size();
    mtl_init_ctx_->palf_options_.disk_options_.log_disk_utilization_threshold_ = 80;
    mtl_init_ctx_->palf_options_.disk_options_.log_disk_utilization_limit_threshold_ = 95;
    mtl_init_ctx_->palf_options_.disk_options_.log_disk_throttling_percentage_ = 100;
    mtl_init_ctx_->palf_options_.disk_options_.log_disk_throttling_maximum_duration_ = 2 * 60 * 60 * 1000 * 1000L;//2h
    mtl_init_ctx_->palf_options_.disk_options_.log_writer_parallelism_ = 3;
    ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
    if (OB_UNLIKELY(!tenant_config.is_valid())) {
      ret = is_virtual_tenant_id(id_) ? OB_SUCCESS : OB_ENTRY_NOT_EXIST;
    } else {
      mtl_init_ctx_->palf_options_.disk_options_.log_writer_parallelism_ = tenant_config->_log_writer_parallelism;
      mtl_init_ctx_->palf_options_.enable_log_cache_ = tenant_config->_enable_log_cache;
    }
    LOG_INFO("construct_mtl_init_ctx success", "palf_options", mtl_init_ctx_->palf_options_.disk_options_);
  }
  return ret;
}
bool ObTenant::is_hidden()
{
  TCRLockGuard guard(meta_lock_);
  return tenant_meta_.super_block_.is_hidden_;
}

ObTenantCreateStatus ObTenant::get_create_status()
{
  TCRLockGuard guard(meta_lock_);
  return tenant_meta_.create_status_;
}
void ObTenant::set_create_status(const ObTenantCreateStatus status)
{
  TCWLockGuard guard(meta_lock_);
  LOG_INFO("set create status",
      "tenant_id", id_,
      "unit_id", tenant_meta_.unit_.unit_id_,
      "new_status", status,
      "old_status", tenant_meta_.create_status_,
      K_(tenant_meta));
  tenant_meta_.create_status_ = status;
}

ObTenantMeta ObTenant::get_tenant_meta()
{
  TCRLockGuard guard(meta_lock_);
  return tenant_meta_;
}

ObUnitInfoGetter::ObTenantConfig ObTenant::get_unit()
{
  TCRLockGuard guard(meta_lock_);
  return tenant_meta_.unit_;
}

uint64_t ObTenant::get_unit_id()
{
  TCRLockGuard guard(meta_lock_);
  return tenant_meta_.unit_.unit_id_;
}

ObTenantSuperBlock ObTenant::get_super_block()
{
  TCRLockGuard guard(meta_lock_);
  return tenant_meta_.super_block_;
}

void ObTenant::set_tenant_unit(const ObUnitInfoGetter::ObTenantConfig &unit)
{
  TCWLockGuard guard(meta_lock_);
  tenant_meta_.unit_ = unit;
}

void ObTenant::set_tenant_super_block(const ObTenantSuperBlock &super_block)
{
  TCWLockGuard guard(meta_lock_);
  tenant_meta_.super_block_ = super_block;
}

Worker::CompatMode ObTenant::get_compat_mode() const
{
  TCRLockGuard guard(meta_lock_);
  return tenant_meta_.unit_.mode_;
}

void ObTenant::set_unit_status(const ObUnitInfoGetter::ObUnitStatus status)
{
  TCWLockGuard guard(meta_lock_);
  LOG_INFO("set unit status",
      "tenant_id", id_,
      "unit_id", tenant_meta_.unit_.unit_id_,
      "new_status", ObUnitInfoGetter::get_unit_status_str(status),
      "old_status", ObUnitInfoGetter::get_unit_status_str(tenant_meta_.unit_.unit_status_),
      K_(tenant_meta));
  tenant_meta_.unit_.unit_status_ = status;
}

ObUnitInfoGetter::ObUnitStatus  ObTenant::get_unit_status()
{
  TCRLockGuard guard(meta_lock_);
  return tenant_meta_.unit_.unit_status_;
}

void ObTenant::mark_tenant_is_removed()
{
  TCWLockGuard guard(meta_lock_);
  LOG_INFO("mark tenant is removed",
      "tenant_id", id_,
      "unit_id", tenant_meta_.unit_.unit_id_,
      K_(tenant_meta));
  tenant_meta_.unit_.is_removed_ = true;
}

ERRSIM_POINT_DEF(CREATE_MTL_MODULE_FAIL)
// 初始化租户各子模块，保证初始化同步执行，因为依赖线程局部变量和栈上变量
int ObTenant::create_tenant_module()
{
  int ret = OB_SUCCESS;
  const uint64_t &tenant_id = id_;
  const double max_cpu = static_cast<double>(tenant_meta_.unit_.config_.max_cpu());
  // set tenant ctx to thread_local
  ObTenantSwitchGuard guard(this);
  // set tenant init param
  FLOG_INFO("begin create mtl module>>>>", K(tenant_id), K(MTL_ID()));

  bool mtl_init = false;
  if (OB_FAIL(ObTenantBase::create_mtl_module())) {
    LOG_ERROR("create mtl module failed", K(tenant_id), K(ret));
  } else if (CREATE_MTL_MODULE_FAIL) {
    ret = CREATE_MTL_MODULE_FAIL;
    LOG_ERROR("create_tenant_module failed because of tracepoint CREATE_MTL_MODULE_FAIL",
              K(tenant_id), K(ret));
  } else if (FALSE_IT(ObTenantEnv::set_tenant(this))) {
    // 上面通过ObTenantSwitchGuard中会创建一个新的TenantBase线程局部变量，而不是存TenantBase的指针，
    // 目的是通过MTL()访问时减少一次内存跳转，但是设置的时mtl模块的指针还是nullptr, 所以在mtl创建完成时
    // 还需要设置一次。
  } else if (FALSE_IT(mtl_init = true)) {
  } else if (OB_FAIL(ObTenantBase::init_mtl_module())) {
    LOG_ERROR("init mtl module failed", K(tenant_id), K(ret));
  } else if (OB_FAIL(ObTenantBase::start_mtl_module())) {
    LOG_ERROR("start mtl module failed", K(tenant_id), K(ret));
  } else if (OB_FAIL(update_thread_cnt(max_cpu))) {
    LOG_ERROR("update mtl module thread cnt fail", K(tenant_id), K(ret));
  }


  FLOG_INFO("finish create mtl module>>>>", K(tenant_id), K(MTL_ID()), K(ret));

  if (OB_FAIL(ret)) {
    if (mtl_init) {
      ObTenantBase::stop_mtl_module();
      ObTenantBase::wait_mtl_module();
    }
    ObTenantBase::destroy_mtl_module();
  }

  return ret;
}

void ObTenant::sleep_and_warn(ObTenant* tenant)
{
  ob_usleep(10_ms);
  const int64_t ts = ObTimeUtility::current_time() - tenant->stopped_;
  if (ts >= 3L * 60 * 1000 * 1000 && TC_REACH_TIME_INTERVAL(3L * 60 * 1000 * 1000)) {
    LOG_ERROR_RET(OB_SUCCESS, "tenant destructed for too long time.", K_(tenant->id), K(ts));
  }
}

void* ObTenant::wait(void* t)
{
  int ret = OB_SUCCESS;
  ObTenant* tenant = (ObTenant*)t;
  ob_get_tenant_id() = tenant->id_;
  lib::set_thread_name("UnitGC");
  lib::Thread::update_loop_ts();
  tenant->handle_retry_req(true);
  while (tenant->req_queue_.size() > 0
    || (tenant->multi_level_queue_ != nullptr && tenant->multi_level_queue_->get_total_size() > 0)) {
    sleep_and_warn(tenant);
  }
  while (tenant->workers_.get_size() > 0) {
    if (OB_SUCC(tenant->workers_lock_.trylock())) {
      DLIST_FOREACH_REMOVESAFE(wnode, tenant->workers_) {
        const auto w = static_cast<ObThWorker*>(wnode->get_data());
        tenant->workers_.remove(wnode);
        destroy_worker(w);
      }
      IGNORE_RETURN tenant->workers_lock_.unlock();
      if (REACH_TIME_INTERVAL(10_s)) {
        LOG_INFO(
            "Tenant has some workers need stop", K_(tenant->id),
            "workers", tenant->workers_.get_size(),
            K_(tenant->req_queue));
      }
    }
    sleep_and_warn(tenant);
  }
  LOG_INFO("start remove nesting", K(tenant->nesting_workers_.get_size()), K_(tenant->id));
  while (tenant->nesting_workers_.get_size() > 0) {
    int ret = OB_SUCCESS;
    if (OB_SUCC(tenant->workers_lock_.trylock())) {
      DLIST_FOREACH_REMOVESAFE(wnode, tenant->nesting_workers_) {
        auto w = static_cast<ObThWorker*>(wnode->get_data());
        tenant->nesting_workers_.remove(wnode);
        destroy_worker(w);
      }
      IGNORE_RETURN tenant->workers_lock_.unlock();
      if (REACH_TIME_INTERVAL(10_s)) {
        LOG_INFO(
            "Tenant has some nesting workers need stop",
            K_(tenant->id),
            "nesting workers", tenant->nesting_workers_.get_size(),
            K_(tenant->req_queue));
      }
    }
    sleep_and_warn(tenant);
  }
  LOG_INFO("finish remove nesting", K(tenant->nesting_workers_.get_size()), K_(tenant->id));
  LOG_INFO("start remove group_map", K_(tenant->id));
  tenant->group_map_.wait_group();
  LOG_INFO("finish remove group_map", K_(tenant->id));
  if (!is_virtual_tenant_id(tenant->id_) && !tenant->wait_mtl_finished_) {
    ObTenantSwitchGuard guard(tenant);
    tenant->stop_mtl_module();
    OB_PX_TARGET_MGR.delete_tenant(tenant->id_);
    G_RES_MGR.get_col_mapping_rule_mgr().drop_tenant(tenant->id_);
    tenant->wait_mtl_module();
    tenant->wait_mtl_finished_ = true;
  }
  LOG_INFO("finish waiting", K_(tenant->id));
  return nullptr;
}

int ObTenant::try_wait()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ATOMIC_LOAD(&gc_thread_))) {
    if (!ATOMIC_BCAS(&has_created_, false, true)) {
      // there will be double-try_wait when kill -15 or failure of locking,
      // so we have to tolerate that and return OB_SUCCESS although it is not correct.
      // ret = OB_ERR_UNEXPECTED;
      LOG_WARN("try_wait again after wait successfully, there may be `kill -15` or failure of locking", K(id_), K(wait_mtl_finished_));
    } else {
      // it may takes too much time for killing session after remove_tenant, we should recalculate.
      ATOMIC_STORE(&stopped_, ObTimeUtility::current_time()); // update, it is not 0 before here.
      if (OB_FAIL(ob_pthread_create(&gc_thread_, wait, this))) {
        ATOMIC_STORE(&has_created_, false);
        LOG_ERROR("tenant gc thread create failed", K(ret), K(errno), K(id_));
      } else {
        ret = OB_EAGAIN;
        LOG_INFO("tenant pthread_create gc thread successfully", K(id_), K(gc_thread_));
      }
    }
  } else {
    if (OB_FAIL(ob_pthread_tryjoin_np(gc_thread_))) {
      LOG_WARN("tenant pthread_tryjoin_np failed", K(errno), K(id_));
    } else {
      ATOMIC_STORE(&gc_thread_, nullptr); // avoid try_wait again after wait success
      LOG_INFO("tenant pthread_tryjoin_np successfully", K(id_));
    }
    const int64_t ts = ObTimeUtility::current_time() - stopped_;
    // only warn for one time in all tenant.
    if (ts >= 3L * 60 * 1000 * 1000 && REACH_TIME_INTERVAL(3L * 60 * 1000 * 1000)) {
      LOG_ERROR_RET(OB_SUCCESS, "tenant destructed for too long time.", K_(id), K(ts));
    }
  }
  return ret;
}

void __attribute__((weak)) print_all_thread(const char* desc, uint64_t tenant_id)
{
  UNUSED(desc);
  UNUSED(tenant_id);
}

void ObTenant::destroy()
{
  int tmp_ret = OB_SUCCESS;
  if (ctx_ != nullptr) {
    DESTROY_ENTITY(ctx_);
    ctx_ = nullptr;
  }
  group_map_.destroy_group();
  ObTenantSwitchGuard guard(this);
  print_all_thread("TENANT_BEFORE_DESTROY", id_);
  destroy_mtl_module();
  ObTenantBase::destroy();

  if (nullptr != multi_level_queue_) {
    common::ob_delete(multi_level_queue_);
    multi_level_queue_ = nullptr;
  }
  if (nullptr != rpc_stat_info_) {
    common::ob_delete(rpc_stat_info_);
    rpc_stat_info_ = nullptr;
  }
  if (nullptr != mtl_init_ctx_) {
    common::ob_delete(mtl_init_ctx_);
    mtl_init_ctx_ = nullptr;
  }

  if (!cgroup_ctrl_.is_valid()) {
    // do nothing
  } else if (OB_TMP_FAIL(cgroup_ctrl_.remove_cgroup(id_))) {
    LOG_WARN_RET(tmp_ret, "remove tenant cgroup failed", K(tmp_ret), K_(id));
  }
}

void ObTenant::set_unit_max_cpu(double cpu)
{
  int tmp_ret = OB_SUCCESS;
  unit_max_cpu_ = cpu;
  if (!cgroup_ctrl_.is_valid() || is_meta_tenant(id_)) {
    // do nothing
  } else if (OB_TMP_FAIL(cgroup_ctrl_.set_cpu_cfs_quota(id_, is_sys_tenant(id_) ? -1 : cpu))) {
    _LOG_WARN_RET(tmp_ret, "set tenant cpu cfs quota failed, tenant_id=%lu, cpu=%.2f", id_, cpu);
  }
}

void ObTenant::set_unit_min_cpu(double cpu)
{
  int tmp_ret = OB_SUCCESS;
  unit_min_cpu_ = cpu;
  if (!cgroup_ctrl_.is_valid()) {
    // do nothing
  } else if (OB_TMP_FAIL(cgroup_ctrl_.set_cpu_shares(id_, cpu))) {
    _LOG_WARN_RET(tmp_ret, "set tenant cpu shares failed, tenant_id=%lu, cpu=%.2f", id_, cpu);
  }
}

int64_t ObTenant::cpu_quota_concurrency() const
{
  ObTenantConfigGuard tenant_config(TENANT_CONF(id_));
  return static_cast<int64_t>((tenant_config.is_valid() ? tenant_config->cpu_quota_concurrency : 4));
}

int64_t ObTenant::min_worker_cnt() const
{
  ObTenantConfigGuard tenant_config(TENANT_CONF(id_));
  return 2 + std::max(1L, static_cast<int64_t>(unit_min_cpu() * (tenant_config.is_valid() ? tenant_config->cpu_quota_concurrency : 4)));
}

int64_t ObTenant::max_worker_cnt() const
{
  return std::max(tenant_meta_.unit_.config_.memory_size() / 20 / (GCONF.stack_size + (3 << 20) + (512 << 10)),
                  150L);
}

int ObTenant::get_new_request(
    ObThWorker &w,
    int64_t timeout,
    rpc::ObRequest *&req)
{
  int ret = OB_SUCCESS;
  ObLink* task = nullptr;

  req = nullptr;
  int wk_level = 0;
  Thread::WaitGuard guard(Thread::WAIT_IN_TENANT_QUEUE);
  if (w.is_group_worker()) {
    w.set_large_query(false);
    w.set_curr_request_level(0);
    wk_level = w.get_worker_level();
    ObResourceGroup *group = static_cast<ObResourceGroup *>(w.get_group());
    if (wk_level < 0 || wk_level >= MAX_REQUEST_LEVEL) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected level", K(wk_level), K(id_));
    } else if (wk_level >= MAX_REQUEST_LEVEL - 1) {
      ret = group->multi_level_queue_.pop_timeup(task, wk_level, timeout);
      if ((ret == OB_SUCCESS && nullptr == task) || ret == OB_ENTRY_NOT_EXIST) {
        ret = OB_ENTRY_NOT_EXIST;
        usleep(10 * 1000L);
      } else if (ret == OB_SUCCESS){
        rpc::ObRequest *tmp_req = static_cast<rpc::ObRequest*>(task);
        LOG_WARN("req is timeout and discard", "tenant_id", id_, K(tmp_req));
      } else {
        LOG_ERROR("pop queue err", "tenant_id", id_, K(ret));
      }
    } else if (w.is_level_worker()) {
      ret = group->multi_level_queue_.pop(task, wk_level, timeout);
    } else {
      for (int32_t level = MAX_REQUEST_LEVEL - 1; level >= GROUP_MULTI_LEVEL_THRESHOLD; level--) {
        IGNORE_RETURN group->multi_level_queue_.try_pop(task, level);
        if (nullptr != task) {
          ret = OB_SUCCESS;
          break;
        }
      }
      if (nullptr == task) {
        ret = group->req_queue_.pop(task, timeout);
      }
    }
  } else {
    w.set_large_query(false);
    w.set_curr_request_level(0);
    wk_level = w.get_worker_level();
    if (wk_level < 0 || wk_level >= MAX_REQUEST_LEVEL) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected level", K(wk_level), K(id_));
    } else if (wk_level >= MAX_REQUEST_LEVEL - 1) {
      ret = multi_level_queue_->pop_timeup(task, wk_level, timeout);
      if ((ret == OB_SUCCESS && nullptr == task) || ret == OB_ENTRY_NOT_EXIST) {
        ret = OB_ENTRY_NOT_EXIST; // If the pop comes out and finds that there is not enough time, then push the front back, ret is succ,
                                  // But because of this situation, the subsequent processing strategy should be the same as the original queue itself is empty.
                                  // So set ret to be the same as the queue empty situation, that is, set to entry not exist
        ob_usleep(10 * 1000L);
      } else if (ret == OB_SUCCESS){
        rpc::ObRequest *tmp_req = static_cast<rpc::ObRequest*>(task);
        LOG_WARN("req is timeout and discard", "tenant_id", id_, K(tmp_req));
      } else {
        LOG_ERROR("pop queue err", "tenant_id", id_, K(ret));
      }
    } else if (w.is_level_worker()) {
      ret = multi_level_queue_->pop(task, wk_level, timeout);
    } else {
      if (w.is_default_worker()) {
        for (int32_t level = MAX_REQUEST_LEVEL - 1; level >= 1; level--) { // Level 0 threads also need to look at the requests of non-level 0 queues first
          IGNORE_RETURN multi_level_queue_->try_pop(task, level);
          if (nullptr != task) {
            ret = OB_SUCCESS;
            break;
          }
        }
      }
      if (OB_ISNULL(task)) {
        if (OB_UNLIKELY(w.is_high_priority())) {
          // We must ensure at least one worker can process the highest
          // priority task.
          ret = req_queue_.pop_high(task, timeout);
        } else if (OB_UNLIKELY(w.is_normal_priority())) {
          // We must ensure at least number of tokens of workers which don't
          // process low priority task.
          ret = req_queue_.pop_normal(task, timeout);
        } else {
          // If large requests exist and this worker doesn't have LQT but
          // can acquire, do it.
          ATOMIC_INC(&pop_normal_cnt_);
          ret = req_queue_.pop(task, timeout);
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (nullptr == req && nullptr != task) {
      req = static_cast<rpc::ObRequest*>(task);
    }
    if (nullptr != req) {
      if (w.is_group_worker() && req->large_retry_flag()) {
        w.set_large_query();
      }
      if (req->get_type() == ObRequest::OB_RPC) {
        using obrpc::ObRpcPacket;
        const ObRpcPacket &pkt
          = static_cast<const ObRpcPacket&>(req->get_packet());
        w.set_curr_request_level(pkt.get_request_level());
      }
    }
  }
  return ret;
}

using oceanbase::obrpc::ObRpcPacket;
inline bool is_high_prio(const ObRpcPacket &pkt)
{
  return pkt.get_priority() < 5;
}

inline bool is_normal_prio(const ObRpcPacket &pkt)
{
  return pkt.get_priority() == 5;
}

inline bool is_low_prio(const ObRpcPacket &pkt)
{
  return pkt.get_priority() > 5 && pkt.get_priority() < 10;
}

inline bool is_ddl(const ObRpcPacket &pkt)
{
  return pkt.get_priority() == 10;
}

inline bool is_warmup(const ObRpcPacket &pkt)
{
  return pkt.get_priority() == 11;
}

int ObTenant::recv_group_request(ObRequest &req, int64_t group_id)
{
  int ret = OB_SUCCESS;
  int64_t now = ObTimeUtility::current_time();
  req.set_enqueue_timestamp(now);
  ObResourceGroup* group = nullptr;
  ObResourceGroupNode* node = nullptr;
  ObResourceGroupNode key(group_id);
  int req_level = 0;
  if (OB_SUCC(GroupMap::err_code_map(group_map_.get(&key, node)))) {
    group = static_cast<ObResourceGroup*>(node);
  } else if (OB_FAIL(group_map_.create_and_insert_group(group_id, this,  &cgroup_ctrl_, group))) {
    if (OB_ENTRY_EXIST == ret && OB_SUCC(GroupMap::err_code_map(group_map_.get(&key, node)))) {
      group = static_cast<ObResourceGroup*>(node);
    } else {
      LOG_WARN("failed to create and insert group", K(ret), K(group_id), K(id_));
    }
  } else {
    LOG_INFO("create group successfully", K_(id), K(group_id), K(group));
  }
  if (OB_SUCC(ret)) {
    if (req.get_type() == ObRequest::OB_RPC) {
      using obrpc::ObRpcPacket;
      const ObRpcPacket &pkt
          = static_cast<const ObRpcPacket&>(req.get_packet());
      req_level = min(pkt.get_request_level(), MAX_REQUEST_LEVEL - 1);
    }
    if (req_level < 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected level", K(req_level), K(id_), K(group_id));
    } else if ((is_resource_manager_group(group_id) || is_dbms_job_group(group_id)) && req_level >= GROUP_MULTI_LEVEL_THRESHOLD) {
      group->recv_level_rpc_cnt_.atomic_inc(req_level);
      if (OB_FAIL(group->multi_level_queue_.push(req, req_level, 0))) {
        LOG_WARN("push request to queue fail", K(req_level), K(id_), K(group_id));
      }
    } else {
      group->atomic_inc_recv_cnt();
      if (OB_FAIL(group->req_queue_.push(&req, 0))) {
        LOG_ERROR("push request to queue fail", K(id_), K(group_id));
      }
    }
    int tmp_ret = OB_SUCCESS;
    if (!share::ObCgSet::instance().is_group_critical(group_id) && 0 == group->workers_.get_size()) {
      if (OB_SUCCESS == (tmp_ret = group->workers_lock_.trylock())) {
        if (0 == group->workers_.get_size()) {
          int64_t succ_num = 0L;
          group->token_change_ts_ = now;
          ATOMIC_STORE(&group->shrink_, false);
          group->acquire_more_worker(1, succ_num, /* force */ true);
          LOG_INFO("worker thread created", K(id()), K(group->group_id_));
        }
        IGNORE_RETURN group->workers_lock_.unlock();
      } else {
        LOG_WARN("failed to lock group workers", K(ret), K(id_), K(group_id));
      }
    }
  }
  return ret;
}

int ObTenant::recv_request(ObRequest &req)
{
  int ret = OB_SUCCESS;
  int req_level = 0;
  if (has_stopped()) {
    ret = OB_TENANT_NOT_IN_SERVER;
    LOG_WARN("receive request but tenant has already stopped", K(ret), K(id_));
  } else if (0 != req.get_group_id()) {
    if (OB_FAIL(recv_group_request(req, req.get_group_id()))) {
      LOG_ERROR("recv group request failed", K(ret), K(id_), K(req.get_group_id()));
    }
  } else {
    // Request would been pushed into corresponding queue by rule.
    //
    //   1. RPC with high or normal priority goes into quick queue.
    //   2. RPC with low priority, usually trivial task, goes into normal queue with low priority.
    //   3. SQL goes into normal queue with normal priority.
    //   4. Server task, session close task, goes into normal queue with high priority.
    //
    req.set_enqueue_timestamp(ObTimeUtility::current_time());
    req.set_trace_point(ObRequest::OB_EASY_REQUEST_TENANT_RECEIVED);
    switch (req.get_type()) {
      case ObRequest::OB_RPC: {
        using obrpc::ObRpcPacket;
        const ObRpcPacket& pkt = static_cast<const ObRpcPacket&>(req.get_packet());
        req_level = min(pkt.get_request_level(), MAX_REQUEST_LEVEL - 1); // Requests that exceed the limit are pushed to the highest-level queue
        if (req_level < 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("unexpected level", K(req_level), K(id_));
        } else if (req_level >= MULTI_LEVEL_THRESHOLD) {
          recv_level_rpc_cnt_.atomic_inc(req_level);
          if (OB_FAIL(multi_level_queue_->push(req, req_level, 0))) {
            LOG_WARN("push request to queue fail", K(ret), K(this));
          }
        } else {
          // (0,5) High priority
          //  [5,10) Normal priority
          //  10 is the low priority used by ddl and should not appear here
          //  11 Ultra-low priority for preheating
          if (is_high_prio(pkt)) {  // the less number the higher priority
            ATOMIC_INC(&recv_hp_rpc_cnt_);
            if (OB_FAIL(req_queue_.push(&req, QQ_HIGH))) {
              if (REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
                LOG_WARN("push request to queue fail", K(ret), K(*this));
              }
            }
          } else if (req.is_retry_on_lock())  {
            ATOMIC_INC(&recv_retry_on_lock_rpc_cnt_);
            if (OB_FAIL(req_queue_.push(&req, QQ_NORMAL))) {
              LOG_WARN("push request to QQ_NORMAL queue fail", K(ret), K(this));
            }
          } else if (pkt.is_kv_request()) {
            // the same as sql request, kv request use q4
            ATOMIC_INC(&recv_np_rpc_cnt_);
            if (OB_FAIL(req_queue_.push(&req, RQ_NORMAL))) {
              LOG_WARN("push kv request to queue fail", K(ret), K(this));
            }
          } else if (is_normal_prio(pkt) || is_low_prio(pkt)) {
            ATOMIC_INC(&recv_np_rpc_cnt_);
            if (OB_FAIL(req_queue_.push(&req, QQ_LOW))) {
              LOG_WARN("push request to queue fail", K(ret), K(this));
            }
          } else if (is_ddl(pkt)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("priority 10 should not come here", K(ret));
          } else if (is_warmup(pkt)) {
            ATOMIC_INC(&recv_lp_rpc_cnt_);
            if (OB_FAIL(req_queue_.push(&req, RQ_LOW))) {
              LOG_WARN("push request to queue fail", K(ret), K(this));
            }
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("unexpected priority", K(ret), K(pkt.get_priority()));
          }
        }
        break;
      }
      case ObRequest::OB_MYSQL: {
        if (req.is_retry_on_lock()) {
          ATOMIC_INC(&recv_retry_on_lock_mysql_cnt_);
          if (OB_FAIL(req_queue_.push(&req, RQ_HIGH))) {
            LOG_WARN("push request to RQ_HIGH queue fail", K(ret), K(this));
          }
        } else {
          ATOMIC_INC(&recv_mysql_cnt_);
          if (OB_FAIL(req_queue_.push(&req, RQ_NORMAL))) {
            LOG_WARN("push request to queue fail", K(ret), K(this));
          }
        }
        break;
      }
      case ObRequest::OB_TASK:
      case ObRequest::OB_TS_TASK: {
        ATOMIC_INC(&recv_task_cnt_);
        if (OB_FAIL(req_queue_.push(&req, RQ_HIGH))) {
          LOG_WARN("push request to queue fail", K(ret), K(this));
        }
        break;
      }
      case ObRequest::OB_SQL_TASK: {
        ATOMIC_INC(&recv_sql_task_cnt_);
        if (OB_FAIL(req_queue_.push(&req, RQ_NORMAL))) {
          LOG_WARN("push request to queue fail", K(ret), K(this));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("unknown request type", K(ret));
        break;
      }
    }
  }

  if (OB_SUCC(ret)) {
    EVENT_INC(REQUEST_ENQUEUE_COUNT);
  }

  if (OB_SIZE_OVERFLOW == ret || (GCONF._faststack_req_queue_size_threshold.get_value() > 0 &&
      req_queue_.size() >= GCONF._faststack_req_queue_size_threshold.get_value())) {
    IGNORE_RETURN faststack();
  }

  return ret;
}

int ObTenant::recv_large_request(rpc::ObRequest &req)
{
  int ret = OB_SUCCESS;
  req.set_enqueue_timestamp(ObTimeUtility::current_time());
  if (has_stopped()) {
    ret = OB_TENANT_NOT_IN_SERVER;
    LOG_WARN("receive large request but tenant has already stopped", K(ret), "tenant_id", id_);
  } else if (0 != req.get_group_id()) {
    if (OB_FAIL(recv_group_request(req, req.get_group_id()))) {
      LOG_WARN("tenant receive large retry request fail", K(ret),
          "tenant_id", id_, "group_id", req.get_group_id());
    }
  } else if (OB_FAIL(recv_group_request(req, OBCG_LQ))){
    LOG_ERROR("recv large request failed", "tenant_id", id_);
  } else {
    EVENT_INC(REQUEST_ENQUEUE_COUNT);
  }
  return ret;
}

int ObTenant::push_retry_queue(rpc::ObRequest &req, const uint64_t timestamp)
{
  int ret = OB_SUCCESS;
  if (has_stopped()) {
    ret = OB_IN_STOP_STATE;
    LOG_WARN("receive retry request but tenant has already stopped", K(ret), K(id_));
  } else if (OB_FAIL(retry_queue_.push(req, timestamp))) {
    LOG_ERROR("push retry queue failed", K(ret), K(id_));
  }
  return ret;
}

int ObTenant::timeup()
{
  int ret = OB_SUCCESS;
  ObLDHandle handle;
  if (!has_stopped() && OB_SUCC(try_rdlock(handle))) {
    // it may fail during drop tenant, try next time.
    if (!has_stopped()) {
      check_group_worker_count();
      check_worker_count();
      update_token_usage();
      handle_retry_req();
      update_queue_size();
    }
    IGNORE_RETURN unlock(handle);
  }
  return OB_SUCCESS;
}

void ObTenant::print_throttled_time()
{
  class ThrottledTimeLog
  {
  public:
    ThrottledTimeLog(ObTenant *tenant) : tenant_(tenant)
    {}
    ~ThrottledTimeLog()
    {}
    int64_t to_string(char *buf, const int64_t len) const
    {
      int64_t pos = 0;
      int tmp_ret = OB_SUCCESS;
      int64_t tenant_throttled_time = 0;
      int64_t group_throttled_time = 0;
      ObResourceGroupNode *iter = NULL;
      ObResourceGroup *group = nullptr;
      ObCgSet &set = ObCgSet::instance();
      while (NULL != (iter = tenant_->group_map_.quick_next(iter))) {
        group = static_cast<ObResourceGroup *>(iter);
        if (!is_resource_manager_group(group->group_id_)) {
          if (OB_TMP_FAIL(group->get_throttled_time(group_throttled_time))) {
            LOG_WARN_RET(tmp_ret, "get throttled time failed", K(tmp_ret), K(group));
          } else {
            tenant_throttled_time += group_throttled_time;
            databuff_printf(buf,
                len,
                pos,
                "group_id: %ld, group: %s, throttled_time: %ld;",
                group->group_id_,
                set.name_of_id(group->group_id_),
                group_throttled_time);
          }
        }
      }

      share::ObGroupName g_name;
      ObRefHolder<ObTenantIOManager> tenant_holder;
      if (OB_TMP_FAIL(OB_IO_MANAGER.get_tenant_io_manager(tenant_->id_, tenant_holder))) {
        LOG_WARN_RET(tmp_ret, "get tenant io manager failed", K(tmp_ret), K(tenant_->id_));
      } else {
        const uint64_t MODE_CNT = static_cast<uint64_t>(ObIOMode::MAX_MODE) + 1;
        for (int64_t i = 0; i < tenant_holder.get_ptr()->get_group_num(); i++) {
          uint64_t group_config_index = i * MODE_CNT;
          if (!tenant_holder.get_ptr()->get_io_config().group_configs_.at(group_config_index).deleted_) {
            uint64_t group_id = tenant_holder.get_ptr()->get_io_config().group_configs_.at(group_config_index).group_id_;
            if (OB_TMP_FAIL(tenant_holder.get_ptr()->get_throttled_time(group_id, group_throttled_time))) {
              LOG_WARN_RET(tmp_ret, "get throttled time failed", K(tmp_ret), K(group_id));
            } else if (OB_TMP_FAIL(tenant_->cgroup_ctrl_.get_group_info_by_group_id(tenant_->id_, group_id, g_name))) {
              LOG_WARN_RET(tmp_ret, "get group_name by id failed", K(tmp_ret), K(group_id));
            } else {
              tenant_throttled_time += group_throttled_time;
              databuff_printf(buf,
                  len,
                  pos,
                  "group_id: %ld, group: %.*s, throttled_time: %ld;",
                  group_id,
                  g_name.get_value().length(),
                  g_name.get_value().ptr(),
                  group_throttled_time);
            }
          }
        }
      }
      databuff_printf(
          buf, len, pos, "tenant_id: %lu, tenant_throttled_time: %ld;", tenant_->id_, tenant_throttled_time);
      return pos;
    }
    ObTenant *tenant_;
  };
  ThrottledTimeLog throttled_time_log(this);
  LOG_INFO("dump throttled time info", K(id_), K(throttled_time_log));
}

void ObTenant::regist_threads_to_cgroup()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  // set cgroup configs
  if (OB_TMP_FAIL(cgroup_ctrl_.set_cpu_shares(id_, unit_min_cpu_, OB_INVALID_GROUP_ID))) {
    LOG_WARN_RET(tmp_ret, "set tenant cpu shares failed", K(tmp_ret), K_(id), K_(unit_min_cpu));
  } else if (is_meta_tenant(id_)) {
    // do nothing
  } else if (OB_TMP_FAIL(
                 cgroup_ctrl_.set_cpu_cfs_quota(id_, is_sys_tenant(id_) ? -1 : unit_max_cpu_, OB_INVALID_GROUP_ID))) {
    LOG_WARN_RET(tmp_ret, "set tenant cpu cfs quota failed", K(tmp_ret), K_(id), K_(unit_max_cpu));
  }

  if (OB_SUCC(thread_list_lock_.trylock())) {
    DLIST_FOREACH_REMOVESAFE(thread_list_node_, thread_list_)
    {
      Thread *thread = thread_list_node_->get_data();
      char *thread_base = (char *)thread->get_pthread();
      Worker *worker = nullptr;
      if (OB_NOT_NULL(thread_base)) {
        GET_OTHER_TSI_ADDR(worker, &Worker::self_);
        if (OB_NOT_NULL(worker) && OB_NOT_NULL(GCTX.cgroup_ctrl_) && GCTX.cgroup_ctrl_->is_valid() &&
            OB_FAIL(GCTX.cgroup_ctrl_->add_thread_to_cgroup_(thread->get_tid(), id_, worker->get_group_id()))) {
          LOG_WARN("regist thread to cgroup failed",
              K(ret),
              K(thread->get_tid()),
              K(id_),
              KP(worker),
              K(worker->get_group_id()));
        }
      }
    }
    LOG_INFO("regist threads to cgroup from thread list", K(ret), K(id_), K(thread_list_.get_size()));
    thread_list_lock_.unlock();
  }
}

void ObTenant::handle_retry_req(bool need_clear)
{
  int ret = OB_SUCCESS;
  ObLink* task = nullptr;
  ObRequest *req = NULL;
  // even if ret != OB_SUCCESS, the loop must continue to pop all requests
  while (OB_SUCC(retry_queue_.pop(task, need_clear))) {
    // if pop returns OB_SUCCESS, then the task must not be NULL.
    req = static_cast<rpc::ObRequest*>(task);
    if (req->large_retry_flag()) {
      if (OB_FAIL(recv_large_request(*req))) {
        LOG_WARN("tenant patrol push req into large_query queue fail, "
            "and the req well be destroyed", "tenant_id", id_, "req", *req, K(ret));
        on_translate_fail(req, ret);
      }
    } else {
      if (OB_FAIL(recv_request(*req))) {
        LOG_WARN("tenant patrol push req into common queue fail, "
            "and the req well be destroyed", "tenant_id", id_, "req", *req, K(ret));
        on_translate_fail(req, ret);
      }
    }
  }
}

void ObTenant::update_queue_size()
{
  ObResourceGroupNode* iter = NULL;
  ObResourceGroup* group = nullptr;
  while (NULL != (iter = group_map_.quick_next(iter))) {
    group = static_cast<ObResourceGroup*>(iter);
    group->update_queue_size();
  }
  req_queue_.set_limit(common::ObServerConfig::get_instance().tenant_task_queue_size);
  if (nullptr != multi_level_queue_) {
    multi_level_queue_->set_limit(common::ObServerConfig::get_instance().tenant_task_queue_size);
  }
}

void ObTenant::check_worker_count()
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(workers_lock_.trylock())) {
    int64_t token = 3;
    int64_t now = ObTimeUtility::current_time();
    bool enable_dynamic_worker = true;
    int64_t threshold = 3 * 1000;
    {
      ObTenantConfigGuard tenant_config(TENANT_CONF(id_));
      enable_dynamic_worker = tenant_config.is_valid() ? tenant_config->_ob_enable_dynamic_worker : true;
      threshold = tenant_config.is_valid() ? tenant_config->_stall_threshold_for_dynamic_worker : 3 * 1000;
    }
    // assume that high priority and normal priority were busy.
    DLIST_FOREACH_REMOVESAFE(wnode, workers_) {
      const auto w = static_cast<ObThWorker*>(wnode->get_data());
      if (w->has_set_stop()) {
        workers_.remove(wnode);
        destroy_worker(w);
      } else if (w->has_req_flag()
                 && 0 != w->blocking_ts()
                 && now - w->blocking_ts() >= threshold
                 && w->is_default_worker()
                 && enable_dynamic_worker) {
        ++token;
      }
    }
    int64_t succ_num = 0L;
    token = std::max(token, min_worker_cnt());
    token = std::min(token, max_worker_cnt());
    if (OB_UNLIKELY(workers_.get_size() < min_worker_cnt())) {
      const auto diff = min_worker_cnt() - workers_.get_size();
      token_change_ts_ = now;
      ATOMIC_STORE(&shrink_, false);
      acquire_more_worker(diff, succ_num, /* force */ true);
      LOG_INFO("worker thread created", K(id_), K(token));
    } else if (OB_UNLIKELY(token > workers_.get_size())
               && OB_LIKELY(ObMallocAllocator::get_instance()->get_tenant_remain(id_) > ObMallocAllocator::get_instance()->get_tenant_limit(id_) * 0.05)) {
      ATOMIC_STORE(&shrink_, false);
      if (OB_LIKELY(now - token_change_ts_ >= EXPAND_INTERVAL)) {
        token_change_ts_ = now;
        acquire_more_worker(1, succ_num);
        LOG_INFO("worker thread created", K(id_), K(token));
      }
    } else if (OB_UNLIKELY(token < workers_.get_size())
               && OB_LIKELY(now - token_change_ts_ >= SHRINK_INTERVAL)) {
      token_change_ts_ = now;
      ATOMIC_STORE(&shrink_, true);
      LOG_INFO("worker thread began to shrink", K(id_), K(token));
    }
    IGNORE_RETURN workers_lock_.unlock();
  }

  if (GCONF._enable_new_sql_nio && GCONF._enable_tenant_sql_net_thread &&
      (is_sys_tenant(id_) || is_user_tenant(id_))) {
    GCTX.net_frame_->reload_tenant_sql_thread_config(id_);
  }
}

void ObTenant::check_group_worker_count()
{
  ObResourceGroupNode* iter = NULL;
  ObResourceGroup* group = nullptr;
  while (NULL != (iter = group_map_.quick_next(iter))) {
    group = static_cast<ObResourceGroup*>(iter);
    group->check_worker_count();
  }
}

void ObTenant::check_worker_count(ObThWorker &w)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(w.is_default_worker())
      && OB_UNLIKELY(ATOMIC_LOAD(&shrink_))
      && OB_LIKELY(ATOMIC_BCAS(&shrink_, true, false))) {
    w.stop();
    LOG_INFO("worker thread exit", K(id_), K(workers_.get_size()));
  }
}

int ObTenant::acquire_level_worker(int64_t num, int64_t &succ_num, int32_t level)
{
  int ret = OB_SUCCESS;
  ObTenantSwitchGuard guard(this);

  const auto need_num = num;
  succ_num = 0;

  if (level <= 0 || level > MAX_REQUEST_LEVEL) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected level", K(level), K(id_));
  } else {
    while (OB_SUCC(ret) && need_num > succ_num) {
      ObThWorker *w = nullptr;
      if (OB_FAIL(create_worker(w, this, 0, level, true))) {
        LOG_WARN("create worker failed", K(ret));
      } else if (!nesting_workers_.add_last(&w->worker_node_)) {
        OB_ASSERT(false);
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("add worker to list fail", K(ret));
      } else {
        succ_num++;
      }
    }
  }

  if (need_num != num ||  // Reach worker count bound,
      succ_num != need_num  // or can't allocate enough worker.
     ) {
    if (TC_REACH_TIME_INTERVAL(10000000)) {
      LOG_WARN("Alloc level worker less than lack", K(num), K(need_num), K(succ_num));
    }
  }

  return ret;
}

// This interface is unnecessary after adding htap
int ObTenant::acquire_more_worker(int64_t num, int64_t &succ_num, bool force)
{
  int ret = OB_SUCCESS;
  succ_num = 0;

  ObTenantSwitchGuard guard(this);
  while (OB_SUCC(ret) && num > succ_num) {
    ObThWorker *w = nullptr;
    if (OB_FAIL(create_worker(w, this, 0, 0, force))) {
      LOG_WARN("create worker failed", K(ret));
    } else if (!workers_.add_last(&w->worker_node_)) {
      OB_ASSERT(false);
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("add worker to list fail", K(ret));
    } else {
      succ_num++;
    }
  }

  return ret;
}

void ObTenant::lq_end(ObThWorker &w)
{
  int ret = OB_SUCCESS;
  if (w.is_lq_yield()) {
    if (OB_FAIL(SET_GROUP_ID(share::OBCG_DEFAULT))) {
      LOG_WARN("move thread from lq group failed", K(ret), K(id_));
    } else {
      w.set_lq_yield(false);
    }
  }
}

void ObTenant::lq_wait(ObThWorker &w)
{
  int64_t last_query_us = ObTimeUtility::current_time() - w.get_last_wakeup_ts();
  ObResourceGroup *group = static_cast<ObResourceGroup *>(w.get_group());
  int64_t lq_group_worker_cnt = group->workers_.get_size();
  int64_t default_group_worker_cnt = workers_.get_size();
  double large_query_percentage = GCONF.large_query_worker_percentage / 100.0;
  int64_t wait_us = static_cast<int64_t>(last_query_us * lq_group_worker_cnt /
                                        (default_group_worker_cnt * large_query_percentage) -
                                         last_query_us);
  wait_us = std::min(wait_us, min(100 * 1000, w.get_timeout_remain()));
  if (wait_us > 10 * 1000) {
    usleep(wait_us);
    w.set_last_wakeup_ts(ObTimeUtility::current_time());
  }
}

int ObTenant::lq_yield(ObThWorker &w)
{
  int ret = OB_SUCCESS;
  ATOMIC_INC(&tt_large_quries_);
  if (!cgroup_ctrl_.is_valid() && w.is_group_worker()) {
    if (w.get_group_id() == share::OBCG_LQ) {
      lq_wait(w);
    }
  } else if (w.is_lq_yield()) {
    // avoid duplicate change group
  } else if (OB_FAIL(SET_GROUP_ID(share::OBCG_LQ))) {
    LOG_WARN("move thread to lq group failed", K(ret), K(id_));
  } else {
    w.set_lq_yield();
  }
  return ret;
}

// thread unsafe
void ObTenant::update_token_usage()
{
  int ret = OB_SUCCESS;
  const auto now = ObTimeUtility::current_time();
  const auto duration = static_cast<double>(now - token_usage_check_ts_);
  if (duration >= 1000 * 1000 && OB_SUCC(workers_lock_.trylock())) {  // every second
    ObResourceGroupNode* iter = NULL;
    ObResourceGroup* group = nullptr;
    int64_t idle_us = 0;
    token_usage_check_ts_ = now;
    DLIST_FOREACH_REMOVESAFE(wnode, workers_) {
      const auto w = static_cast<ObThWorker*>(wnode->get_data());
      idle_us += ATOMIC_SET(&w->idle_us_, 0);
    }
    DLIST_FOREACH_REMOVESAFE(wnode, nesting_workers_) {
      const auto w = static_cast<ObThWorker*>(wnode->get_data());
      idle_us += ATOMIC_SET(&w->idle_us_, 0);
    }
    while (OB_NOT_NULL(iter = group_map_.quick_next(iter))) {
      group = static_cast<ObResourceGroup*>(iter);
      DLIST_FOREACH_REMOVESAFE(wnode, group->workers_) {
        const auto w = static_cast<ObThWorker*>(wnode->get_data());
        idle_us += ATOMIC_SET(&w->idle_us_, 0);
      }
    }
    workers_lock_.unlock();
    const auto total_us = duration * total_worker_cnt_;
    token_usage_ = std::max(.0, 1.0 * (total_us - idle_us) / total_us);
    IGNORE_RETURN ATOMIC_FAA(&worker_us_, total_us - idle_us);
  }

  if (OB_NOT_NULL(GCTX.cgroup_ctrl_) && GCTX.cgroup_ctrl_->is_valid()) {
    //do nothing
  } else if (duration >= 1000 * 1000 && OB_SUCC(thread_list_lock_.trylock())) {  // every second
    int64_t cpu_time_inc = 0;
    DLIST_FOREACH_REMOVESAFE(thread_list_node_, thread_list_)
    {
      Thread *thread = thread_list_node_->get_data();
      int64_t inc = 0;
      if (OB_SUCC(thread->get_cpu_time_inc(inc))) {
        cpu_time_inc += inc;
      }
    }
    thread_list_lock_.unlock();
    IGNORE_RETURN ATOMIC_FAA(&cpu_time_us_, cpu_time_inc);
  }
}

void ObTenant::periodically_check()
{
  int ret = OB_SUCCESS;
  WITH_ENTITY(ctx_) {
    check_parallel_servers_target();
    check_resource_manager_plan();
    check_dtl();
    check_px_thread_recycle();
  }
}

void ObTenant::check_resource_manager_plan()
{
  int ret = OB_SUCCESS;
  ObString plan;
  ObString up_plan;
  ObResourcePlanManager &plan_mgr = G_RES_MGR.get_plan_mgr();
  ObResourceMappingRuleManager &rule_mgr = G_RES_MGR.get_mapping_rule_mgr();
  ObResourceColMappingRuleManager &col_rule_mgr = G_RES_MGR.get_col_mapping_rule_mgr();
  char data[OB_MAX_RESOURCE_PLAN_NAME_LENGTH];
  ObDataBuffer allocator(data, OB_MAX_RESOURCE_PLAN_NAME_LENGTH);
  if (OB_SYS_TENANT_ID != id_ && OB_MAX_RESERVED_TENANT_ID >= id_) {
    // Except for system rental outside, internal tenants do not use resource plan for internal isolation
  } else if (OB_FAIL(ObSchemaUtils::get_tenant_varchar_variable(
              id_,
              SYS_VAR_RESOURCE_MANAGER_PLAN,
              allocator,
              plan))) {
    LOG_WARN("fail get tenant variable", K(id_), K(plan), K(ret));
    // skip
  } else if (OB_FAIL(ob_simple_low_to_up(allocator, plan, up_plan))) {
    LOG_WARN("plan change to upper string failed", K(ret));
  } else if (OB_FAIL(rule_mgr.refresh_group_mapping_rule(id_, up_plan))) {
    LOG_WARN("refresh group id name mapping rule fail."
             "Tenant resource isolation may not work",
             K(id_), K(up_plan), K(ret));
  } else if (OB_FAIL(plan_mgr.refresh_resource_plan(id_, up_plan))) {
    LOG_WARN("refresh resource plan fail."
             "Tenant resource isolation may not work",
             K(id_), K(up_plan), K(ret));
  } else if (OB_FAIL(rule_mgr.refresh_resource_mapping_rule(id_, up_plan))) {
    LOG_WARN("refresh resource mapping rule fail."
             "Tenant resource isolation may not work",
             K(id_), K(up_plan), K(ret));
  } else if (OB_FAIL(col_rule_mgr.refresh_resource_column_mapping_rule(id_, get<ObPlanCache*>(),
                                                                       up_plan))) {
    LOG_WARN("refresh resource column mapping rule fail."
             "Tenant resource isolation may not work",
             K(id_), K(up_plan), K(ret));
  }
}

void ObTenant::check_dtl()
{
  int ret = OB_SUCCESS;
  if (is_virtual_tenant_id(id_)) {
    // Except for system rentals, internal tenants do not allocate px threads
  } else {
    ObTenantSwitchGuard guard(this);
    auto tenant_dfc = MTL(ObTenantDfc*);
    if (OB_NOT_NULL(tenant_dfc)) {
      tenant_dfc->check_dtl(id_);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to switch to tenant", K(id_), K(ret));
    }
  }
}

void ObTenant::check_das()
{
  int ret = OB_SUCCESS;
  if (!is_virtual_tenant_id(id_)) {
    ObTenantSwitchGuard guard(this);
    if (OB_ISNULL(MTL(ObDataAccessService *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get das ptr", K(MTL_ID()));
    } else {
      double min_cpu = .0;
      double max_cpu = .0;
      if (OB_FAIL(GCTX.omt_->get_tenant_cpu(MTL_ID(), min_cpu, max_cpu))) {
        LOG_WARN("failed to set das task max concurrency", K(MTL_ID()));
      } else {
        MTL(ObDataAccessService *)->set_max_concurrency(min_cpu);
      }
    }
  }
}

void ObTenant::check_parallel_servers_target()
{
  int ret = OB_SUCCESS;
  int64_t val = 0;
  if (is_virtual_tenant_id(id_)) {
    // Except for system rentals, internal tenants do not allocate px threads
  } else if (OB_FAIL(ObSchemaUtils::get_tenant_int_variable(
              id_,
              SYS_VAR_PARALLEL_SERVERS_TARGET,
              val))) {
    LOG_WARN("fail read tenant variable", K_(id), K(ret));
  } else if (OB_FAIL(OB_PX_TARGET_MGR.set_parallel_servers_target(id_, val))) {
    LOG_WARN("set parallel_servers_target failed", K(ret), K(id_), K(val));
  }
}

void ObTenant::check_px_thread_recycle()
{
  int ret = OB_SUCCESS;
  if (is_virtual_tenant_id(id_)) {
    // Except for system rentals, internal tenants do not allocate px threads
  } else {
    ObTenantSwitchGuard guard(this);
    auto px_pools = MTL(ObPxPools*);
    if (OB_NOT_NULL(px_pools)) {
      px_pools->thread_recycle();
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to switch to tenant", K(id_), K(ret));
    }
  }
}
