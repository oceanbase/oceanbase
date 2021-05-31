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
#include "ob_th_worker.h"
#include "ob_worker_pool.h"
#include "ob_multi_tenant.h"
#include "observer/ob_server_struct.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_schema_utils.h"
#include "share/resource_manager/ob_resource_manager.h"

using namespace oceanbase::lib;
using namespace oceanbase::common;
using namespace oceanbase::omt;
using namespace oceanbase::rpc;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

void MultiLevelReqCnt::atomic_inc(const int32_t level)
{
  if (level < 0 || level >= MAX_REQUEST_LEVEL) {
    LOG_WARN("unexpected level", K(level));
  } else {
    ATOMIC_INC(&cnt_[level]);
  }
}

int ObPxPools::init(uint64_t tenant_id)
{
  static int PX_POOL_COUNT = 128;  // 128 groups, generally enough
  int ret = OB_SUCCESS;
  tenant_id_ = tenant_id;
  if (OB_FAIL(pool_map_.create(PX_POOL_COUNT, "PxPoolBkt", "PxPoolNode"))) {
    LOG_WARN("fail init pool map", K(ret));
  }
  return ret;
}

int ObPxPools::get_or_create(int64_t group_id, ObPxPool*& pool)
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

int ObPxPools::create_pool(int64_t group_id, ObPxPool*& pool)
{
  static constexpr uint64_t MAX_TASKS_PER_CPU = 1;
  int ret = OB_SUCCESS;
  common::SpinWLockGuard g(lock_);
  if (OB_FAIL(pool_map_.get_refactored(group_id, pool))) {
    if (OB_HASH_NOT_EXIST == ret) {
      pool = OB_NEW(ObPxPool, common::ObModIds::OMT_TENANT);
      if (OB_ISNULL(pool)) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
      } else {
        pool->set_tenant_id(tenant_id_);
        pool->set_group_id(group_id);
        pool->set_thread_max_tasks(MAX_TASKS_PER_CPU);
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

int ObPxPools::delete_pool(int64_t group_id)
{
  int ret = OB_SUCCESS;
  ObPxPool* pool = nullptr;
  common::SpinWLockGuard g(lock_);
  if (OB_FAIL(pool_map_.get_refactored(group_id, pool))) {
    LOG_WARN("fail delete group pool", K(group_id), K(ret));
  } else {
    pool->stop();
    LOG_INFO("DEL_POOL_STEP_1: mark px pool stop succ!", K(group_id));
    pool->wait();
    LOG_INFO("DEL_POOL_STEP_2: wait pool empty succ!", K(group_id));
    pool->destroy();
    LOG_INFO("DEL_POOL_STEP_3: pool destroy succ!", K(group_id));
    common::ob_delete(pool);
  }
  return ret;
}

void ObPxPools::destroy()
{
  common::SpinWLockGuard g(lock_);
  pool_map_.destroy();
  tenant_id_ = OB_INVALID_ID;
}

void ObPxPool::run1()
{
  lib::set_thread_name("PxPoolTh", get_thread_idx());
  auto* pm = common::ObPageManager::thread_local_instance();
  if (OB_LIKELY(nullptr != pm)) {
    pm->set_tenant_ctx(tenant_id_, common::ObCtxIds::WORK_AREA);
  }
  ObCgroupCtrl* cgroup_ctrl = GCTX.cgroup_ctrl_;
  LOG_INFO("XXXXX: run px pool", K(group_id_), K(tenant_id_));
  if (nullptr != cgroup_ctrl && OB_LIKELY(cgroup_ctrl->is_valid())) {
    pid_t pid = static_cast<pid_t>(syscall(__NR_gettid));
    cgroup_ctrl->add_thread_to_cgroup(tenant_id_, group_id_, pid);
    LOG_INFO("set pid to group succ", K(tenant_id_), K(group_id_), K(pid));
  }
  while (!Thread::current().has_set_stop()) {
    this_routine::usleep(10L * 1000L * 1000L);
  }
}

void ObResourceGroup::init(int64_t token_cnt)
{
  req_queue_.set_limit(common::ObServerConfig::get_instance().tenant_task_queue_size);
  set_token_cnt(token_cnt);
  set_min_token_cnt(token_cnt);
  inited_ = true;
}

int ObResourceGroup::acquire_more_worker(int64_t num, int64_t& succ_num)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  const auto need_num = num;
  succ_num = 0;

  while (OB_SUCC(ret) && need_num > succ_num) {
    ObThWorker* w = worker_pool_->alloc();
    if (w) {
      w->reset();
      w->set_tidx(workers_.get_size() + 2000);
      w->set_tenant(tenant_);
      w->set_group(this);
      w->set_group_id(group_id_);
      w->activate();
      if (!workers_.add_last(&w->worker_node_)) {
        OB_ASSERT(false);
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("add worker to list fail", K(ret));
      } else {
        tmp_ret = cgroup_ctrl_->add_thread_to_cgroup(tenant_->id(), group_id_, w->get_tid());
        if (OB_SUCCESS != tmp_ret) {
          LOG_WARN("add thread to cgroup failed", K(tmp_ret), "tenant", w->get_tenant()->id());
        }
        succ_num++;
      }
    } else {
      ret = OB_SIZE_OVERFLOW;
    }
  }

  if (need_num != num ||    // Reach worker count bound,
      succ_num != need_num  // or can't allocate enough worker.
  ) {
    if (TC_REACH_TIME_INTERVAL(10000000)) {
      LOG_WARN("Alloc group worker less than lack", K(num), K(need_num), K(succ_num));
    }
  }

  return ret;
}

void ObResourceGroup::calibrate_token_count()
{
  int ret = OB_SUCCESS;
  const auto current_time = ObTimeUtility::current_time();
  if (current_time - last_calibrate_token_ts_ > CALIBRATE_TOKEN_INTERVAL) {
    int64_t wait_worker = 0;
    int64_t active_workers = 0;
    DLIST_FOREACH_REMOVESAFE(wnode, workers_)
    {
      const auto w = static_cast<ObThWorker*>(wnode->get_data());
      if (w->is_active()) {
        active_workers++;
        if (!w->has_req_flag()) {
          wait_worker++;
        }
      }
    }
    if (tenant_->sug_token_cnt() !=
        min_token_cnt_) {  // If the user manually adjusts the tenant specifications, the dynamic token adjustment alone
                           // cannot respond quickly, and it needs to be adjusted forcibly
      set_token_cnt(tenant_->sug_token_cnt());
      set_min_token_cnt(tenant_->sug_token_cnt());
    }
    if (last_pop_req_cnt_ != 0 && pop_req_cnt_ == last_pop_req_cnt_ && token_cnt_ == ass_token_cnt_) {
      set_token_cnt(token_cnt_ + 1);
    }
    if (wait_worker > active_workers / 2) {
      set_token_cnt(max(token_cnt_ - 1, min_token_cnt_));
    }
    last_calibrate_token_ts_ = current_time;
    last_pop_req_cnt_ = pop_req_cnt_;
  }
}

void ObResourceGroup::check_worker_count()
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(workers_lock_.trylock())) {
    DLIST_FOREACH_REMOVESAFE(wnode, workers_)
    {
      const auto w = static_cast<ObThWorker*>(wnode->get_data());
      const auto active_inactive_ts = w->get_active_inactive_ts();
      const auto sojourn_time = ObTimeUtility::current_time() - active_inactive_ts;
      if (w->is_active()) {
        // w->set_tidx(active_workers);
      } else if (w->is_waiting_active() && sojourn_time > PRESERVE_INACTIVE_WORKER_TIME) {
        const auto active_inactive_ts = w->get_active_inactive_ts();
        const auto sojourn_time = ObTimeUtility::current_time() - active_inactive_ts;
        if (sojourn_time > PRESERVE_INACTIVE_WORKER_TIME) {
          workers_.remove(wnode);
          w->reset();
          worker_pool_->free(w);
        }
      }
    }
    const auto diff = token_cnt_ - ass_token_cnt_;
    if (diff > 0) {
      int64_t succ_num = 0L;
      acquire_more_worker(diff, succ_num);
      ass_token_cnt_ += succ_num;
    } else if (diff < 0) {
      // ret = OB_NEED_WAIT;
    }
    IGNORE_RETURN workers_lock_.unlock();
  }
}

void ObResourceGroup::check_worker_count(ObThWorker& w)
{
  int ret = OB_SUCCESS;
  if (ass_token_cnt_ != token_cnt_ && OB_SUCC(workers_lock_.trylock())) {
    const auto diff = token_cnt_ - ass_token_cnt_;
    int tmp_ret = OB_SUCCESS;
    if (diff > 0) {
      int64_t succ_num = 0L;
      acquire_more_worker(diff, succ_num);
      ass_token_cnt_ += succ_num;
    } else if (diff < 0) {
      ass_token_cnt_--;
      w.set_inactive();
      if (cgroup_ctrl_->is_valid() &&
          OB_SUCCESS != (tmp_ret = cgroup_ctrl_->remove_thread_from_cgroup(tenant_->id(), w.get_tid()))) {
        LOG_WARN("remove thread from cgroup failed", K(tmp_ret), "tenant:", tenant_->id(), K_(group_id));
      }
    }
    IGNORE_RETURN workers_lock_.unlock();
  }
}

ObTenant::ObTenant(
    const int64_t id, const int64_t times_of_workers, ObWorkerPool& worker_pool, ObCgroupCtrl& cgroup_ctrl)
    : ObTenantBase(id),
      times_of_workers_(times_of_workers),
      unit_max_cpu_(0),
      unit_min_cpu_(0),
      slice_(0),
      slice_remain_(0),
      slice_remain_lock_(),
      slice_remain_clear_flag_(true),
      acc_min_slice_(0),
      acc_max_slice_(0),
      significance_(0),
      token_cnt_(0),
      ass_token_cnt_(0),
      lq_tokens_(0),
      used_lq_tokens_(0),
      last_calibrate_worker_ts_(0),
      last_calibrate_token_ts_(0),
      last_pop_normal_cnt_(0),
      nesting_worker_has_init_(1),
      stopped_(true),
      use_group_map_(true),
      req_queue_(),
      large_req_queue_(),
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
      actives_(0),
      tt_large_quries_(0),
      pop_normal_cnt_(0),
      worker_pool_(worker_pool),
      lock_(),
      group_map_(nullptr),
      cgroup_ctrl_(cgroup_ctrl),
      disable_user_sched_(false),
      idle_us_(0),
      token_usage_(.0),
      token_usage_check_ts_(0),
      dynamic_modify_token_(true),
      compat_mode_(share::ObWorker::CompatMode::INVALID),
      ctx_(nullptr),
      px_pool_is_running_(false),
      st_metrics_(),
      sql_limiter_()
{
  token_usage_check_ts_ = ObTimeUtility::current_time();
  lock_.set_diagnose(true);
}

ObTenant::~ObTenant()
{}

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

int ObTenant::init()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  req_queue_.set_limit(common::ObServerConfig::get_instance().tenant_task_queue_size);

  if (NULL == (multi_level_queue_ = OB_NEW(ObMultiLevelQueue, ObModIds::OMT_TENANT))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc ObMultiLevelQueue failed", K(ret), K(*this));
  } else if (OB_FAIL(multi_level_queue_->init(common::ObServerConfig::get_instance().tenant_task_queue_size))) {
    LOG_WARN("ObMultiLevelQueue init failed", K(ret), K_(id), K(*this));
  }
  stopped_ = false;
  if (cgroup_ctrl_.is_valid() && OB_SUCCESS != (tmp_ret = cgroup_ctrl_.create_tenant_cgroup(id_))) {
    // Failure of cgroup does not affect tenant creation
    LOG_WARN("create tenant cgroup failed", K(tmp_ret), K_(id));
  } else if (cgroup_ctrl_.is_valid() && use_group_map_) {
    int64_t size = MAX_RESOURCE_GROUP * sizeof(common::SpHashNode);
    void* obj_buf = nullptr;
    void* buf = nullptr;
    if (nullptr != (obj_buf = common::ob_malloc(sizeof(GroupMap), ObModIds::OMT_TENANT)) &&
        nullptr != (buf = common::ob_malloc(size, ObModIds::OMT_TENANT)) &&
        nullptr != (group_map_ = new (obj_buf) GroupMap(buf, size))) {
      group_map_->init();
    } else {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc memory failed", K(ret));
      if (nullptr != group_map_) {
        common::ob_delete(group_map_);
      }
      if (nullptr != buf) {
        ob_free(buf);
      }
      if (nullptr != obj_buf) {
        ob_free(obj_buf);
      }
    }
  }
  if (OB_SUCCESS == ret) {
    if (OB_FAIL(ObTenantBase::init())) {
      LOG_ERROR("tenant base init failed", K_(id), K(ret));
    }
  }
  return ret;
}

void ObTenant::stop()
{
  stopped_ = true;  // don't receive new request.
}

void ObTenant::wait()
{
  while (has_task()) {
    {
      ObMutexGuard guard(lq_waiting_workers_lock_);
      DLIST_FOREACH_NORET(wnode, lq_waiting_workers_)
      {
        const auto w = static_cast<ObThWorker*>(wnode->get_data());
        resume_it(*w);
      }
    }
    usleep(100L * 1000L);
  }
  while (workers_.get_size() > 0) {
    int ret = OB_SUCCESS;
    if (OB_SUCC(workers_lock_.trylock())) {
      DLIST_FOREACH_REMOVESAFE(wnode, workers_)
      {
        const auto w = static_cast<ObThWorker*>(wnode->get_data());
        w->set_inactive();
        if (w->is_waiting_active()) {
          w->reset();
          workers_.remove(wnode);
          worker_pool_.free(w);
        }
      }
      IGNORE_RETURN workers_lock_.unlock();
      if (REACH_TIME_INTERVAL(10 * 1000L * 1000L)) {
        LOG_INFO("Tenant has some workers need stop", K_(id), "workers", workers_.get_size(), K_(req_queue));
      }
    }
    usleep(10L * 1000L);
  }
  LOG_WARN("start remove nesting", K(nesting_workers_.get_size()), K_(id));
  while (nesting_workers_.get_size() > 0) {
    int ret = OB_SUCCESS;
    if (OB_SUCC(workers_lock_.trylock())) {
      DLIST_FOREACH_REMOVESAFE(wnode, nesting_workers_)
      {
        const auto w = static_cast<ObThWorker*>(wnode->get_data());
        w->set_inactive();
        if (w->is_waiting_active()) {
          w->reset();
          nesting_workers_.remove(wnode);
          worker_pool_.free(w);
          nesting_worker_has_init_--;
        }
      }
      IGNORE_RETURN workers_lock_.unlock();
      if (REACH_TIME_INTERVAL(10 * 1000L * 1000L)) {
        LOG_INFO("Tenant has some nesting workers need stop",
            K_(id),
            "nesting workers",
            nesting_workers_.get_size(),
            K_(req_queue));
      }
    }
    usleep(10L * 1000L);
  }
  LOG_WARN("finish remove nesting", K(nesting_workers_.get_size()), K_(id));

  if (nullptr != group_map_ && group_map_->is_inited()) {
    LOG_WARN("start remove group_map", K_(id));
    while (!group_map_->is_empty()) {
      ObResourceGroup* iter = NULL;
      if (nullptr != (iter = group_map_->quick_next(iter))) {
        while (iter->workers_.get_size() > 0) {
          int ret = OB_SUCCESS;
          if (OB_SUCC(workers_lock_.trylock())) {
            DLIST_FOREACH_REMOVESAFE(wnode, iter->workers_)
            {
              const auto w = static_cast<ObThWorker*>(wnode->get_data());
              w->set_inactive();
              if (w->is_waiting_active()) {
                w->reset();
                iter->workers_.remove(wnode);
                iter->worker_pool_->free(w);
              }
            }
            IGNORE_RETURN workers_lock_.unlock();
            if (REACH_TIME_INTERVAL(10 * 1000L * 1000L)) {
              LOG_INFO("Tenant has some group workers need stop",
                  K_(id),
                  "group workers",
                  iter->workers_.get_size(),
                  "group type",
                  iter->get_group_id());
            }
          }
          usleep(10L * 1000L);
        }
        ObResourceGroup cur_key(iter->get_group_id(), nullptr, nullptr, nullptr);
        ObResourceGroup* cur_group = NULL;
        if (0 == group_map_->del(&cur_key, cur_group)) {
          destroy_group(cur_group);
        }
      }
    }
    common::ob_delete(group_map_);
    LOG_WARN("finish remove group_map", K_(id));
  }
}

void ObTenant::destroy()
{
  int tmp_ret = OB_SUCCESS;
  if (ctx_ != nullptr) {
    DESTROY_ENTITY(ctx_);
    ctx_ = nullptr;
  }
  if (cgroup_ctrl_.is_valid() && OB_SUCCESS != (tmp_ret = cgroup_ctrl_.remove_tenant_cgroup(id_))) {
    LOG_WARN("remove tenant cgroup failed", K(tmp_ret), K_(id));
  }
  ObTenantBase::destory();
}

void ObTenant::set_unit_max_cpu(double cpu)
{
  int tmp_ret = OB_SUCCESS;
  unit_max_cpu_ = cpu;
  const double default_cfs_period_us = 100000.0;
  int32_t cfs_quota_us = static_cast<int32_t>(default_cfs_period_us * cpu);
  if (cgroup_ctrl_.is_valid() && OB_SUCCESS != (tmp_ret = cgroup_ctrl_.set_cpu_cfs_quota(id_, cfs_quota_us))) {
    LOG_WARN("set cpu cfs quota failed", K(tmp_ret), K_(id), K(cfs_quota_us));
  }
}
void ObTenant::set_unit_min_cpu(double cpu)
{
  int tmp_ret = OB_SUCCESS;
  unit_min_cpu_ = cpu;
  const double default_cpu_shares = 1024.0;
  int32_t cpu_shares = static_cast<int32_t>(default_cpu_shares * cpu);
  if (cgroup_ctrl_.is_valid() && OB_SUCCESS != (tmp_ret = cgroup_ctrl_.set_cpu_shares(id_, cpu_shares))) {
    LOG_WARN("set cpu shares failed", K(tmp_ret), K_(id), K(cpu_shares));
  }
}

void ObTenant::set_token(const int64_t token)
{
  if (token >= 0) {
    token_cnt_ = token;
  }
}

void ObTenant::set_sug_token(const int64_t token)
{
  if (token >= 0) {
    sug_token_cnt_ = token;
    if (!dynamic_modify_token_ || token_cnt_ == 0L) {
      token_cnt_ = sug_token_cnt_;
    }

    const auto lq_pctg = GCONF.large_query_worker_percentage.get();
    const auto lq_token = static_cast<double>(sug_token_cnt_) * lq_pctg / 100.0;
    const auto final_lq_token = std::max(1L, static_cast<int64_t>(lq_token));
    ATOMIC_SET(&lq_tokens_, final_lq_token);
  }
}

int64_t ObTenant::worker_count_bound() const
{
  // All max_cpu in unit won't beyond this node's cpu count, so worker
  // bound of all tenant in this node wont't exceeds number of the
  // node's workers too.
  return static_cast<int64_t>(unit_max_cpu_ * static_cast<int>(times_of_workers_));
}

int ObTenant::get_new_request(ObThWorker& w, int64_t timeout, rpc::ObRequest*& req)
{
  int ret = OB_SUCCESS;
  int wk_level = 0;
  ObLink* task = nullptr;

  req = nullptr;
  if (w.get_group() != nullptr) {
    w.set_large_query(false);
    w.set_curr_request_level(0);
    wk_level = w.get_worker_level();
    if (OB_SUCC(w.get_group()->req_queue_.pop(task, timeout))) {
      w.get_group()->atomic_inc_pop_cnt();
      EVENT_INC(REQUEST_DEQUEUE_COUNT);
      if (nullptr == req && nullptr != task) {
        req = static_cast<rpc::ObRequest*>(task);
        if (req->large_retry_flag()) {
          w.set_large_query();
        }
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
        ret = OB_ENTRY_NOT_EXIST;  // If the pop comes out and finds that there is not enough time, then push the front
                                   // back, ret is succ, But because of this situation, the subsequent processing
                                   // strategy should be the same as the original queue itself is empty. So set ret to
                                   // be the same as the queue empty situation, that is, set to entry not exist
        usleep(10 * 1000L);
      } else if (ret == OB_SUCCESS) {
        rpc::ObRequest* tmp_req = static_cast<rpc::ObRequest*>(task);
        LOG_WARN("req is timeout and discard", "tenant_id", id_, K(tmp_req));
      } else {
        LOG_ERROR("pop queue err", "tenant_id", id_, K(ret));
      }
    } else if (wk_level > 0) {
      ret = multi_level_queue_->pop(task, wk_level, timeout);
    } else {
      const bool only_high_high_prio = w.ObWorker::get_tidx() == 1 && workers_.get_size() > 2;
      const bool only_high_prio = w.ObWorker::get_tidx() == 0 && workers_.get_size() > 1;

      if (!only_high_high_prio && !only_high_prio) {
        for (int32_t level = MAX_REQUEST_LEVEL - 1; level >= 1;
             level--) {  // Level 0 threads also need to look at the requests of non-level 0 queues first
          IGNORE_RETURN multi_level_queue_->try_pop(task, level);
          if (nullptr != task) {
            ret = OB_SUCCESS;
            break;
          }
        }
      }

      if (nullptr == task) {
        if (OB_UNLIKELY(only_high_high_prio)) {
          // We must ensure at least one worker can process the highest
          // priority task.
          ret = req_queue_.do_pop(task, QQ_HIGH + 1, timeout);
        } else if (OB_UNLIKELY(only_high_prio)) {
          // We must ensure at least number of tokens of workers which don't
          // process low priority task.
          ret = req_queue_.pop_high(task, timeout);
        } else {
          // If large requests exist and this worker doesn't have LQT but
          // can acquire, do it.
          ATOMIC_INC(&pop_normal_cnt_);
          if (large_req_queue_.size() > 0 && !w.has_lq_token() && acquire_lq_token()) {
            w.set_lq_token();
          }
          if (OB_LIKELY(!w.has_lq_token())) {
            ret = req_queue_.pop(task, 0L);
          }
          if (OB_UNLIKELY(nullptr == task)) {
            // If large query flag is set, we prefer large query.
            if (OB_SUCC(large_req_queue_.pop(task))) {
              w.set_large_query();
            } else {
              // Ignore return code from large queue and get request from
              // normal queue.
              ret = req_queue_.pop(task, timeout);
            }
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      EVENT_INC(REQUEST_DEQUEUE_COUNT);
      if (nullptr == req && nullptr != task) {
        req = static_cast<rpc::ObRequest*>(task);
      }
      if (nullptr != req && req->get_type() == ObRequest::OB_RPC) {
        using obrpc::ObRpcPacket;
        const ObRpcPacket& pkt = static_cast<const ObRpcPacket&>(req->get_packet());
        w.set_curr_request_level(pkt.get_request_level());
      }
    }

    if (w.has_lq_token() && (nullptr == req || !w.large_query())) {
      if (w.has_lq_token()) {
        release_lq_token();
      }
      w.set_lq_token(false);
    }
  }

  return ret;
}

using oceanbase::obrpc::ObRpcPacket;
inline bool is_high_prio(const ObRpcPacket& pkt)
{
  return pkt.get_priority() < 5;
}

inline bool is_normal_prio(const ObRpcPacket& pkt)
{
  return pkt.get_priority() == 5;
}

inline bool is_low_prio(const ObRpcPacket& pkt)
{
  return pkt.get_priority() > 5 && pkt.get_priority() < 10;
}

inline bool is_ddl(const ObRpcPacket& pkt)
{
  return pkt.get_priority() == 10;
}

inline bool is_warmup(const ObRpcPacket& pkt)
{
  return pkt.get_priority() == 11;
}

ObResourceGroup* ObTenant::create_group(
    int32_t group_id, ObTenant* tenant, ObWorkerPool* worker_pool, ObCgroupCtrl* cgroup_ctrl)
{
  const int64_t alloc_size = sizeof(ObResourceGroup);
  ObResourceGroup* p = (ObResourceGroup*)ob_malloc(alloc_size, ObModIds::OMT_TENANT);
  return (NULL == p) ? NULL : new (p) ObResourceGroup(group_id, tenant, worker_pool, cgroup_ctrl);
}

void ObTenant::destroy_group(ObResourceGroup* p)
{
  ob_free(p);
}

int ObTenant::recv_request(ObRequest& req)
{
  int ret = OB_SUCCESS;
  int req_level = 0;
  if (stopped_) {
    ret = OB_IN_STOP_STATE;
    LOG_WARN("receive request but tenant has already stopped", K(ret), K(id_));
  } else if (nullptr != group_map_ && group_map_->is_inited()) {
    req.set_enqueue_timestamp(ObTimeUtility::current_time());
    int32_t group_id = req.get_group_id();
    ObResourceGroup* group = nullptr;
    ObResourceGroup key(req.get_group_id(), nullptr, nullptr, nullptr);
    if (0 == group_map_->get(&key, group)) {
      // do nothing
    } else if (nullptr == (group = create_group(group_id, this, &worker_pool_, &cgroup_ctrl_))) {
      LOG_ERROR("create group failed", "group_id", group_id, "tenant", group->get_tenant()->id());
    } else if (0 != group_map_->insert(group)) {
      destroy_group(group);
      LOG_ERROR("groupmap insert group failed", "group_id", group->get_group_id(), "tenant", group->get_tenant()->id());
    } else {
      group->init(sug_token_cnt_);
    }
    if ((nullptr != group) && group->is_inited()) {
      group->atomic_inc_recv_cnt();
      if (OB_FAIL(group->req_queue_.push(&req, 0))) {
        LOG_ERROR("push request to queue fail", K(ret), K(this));
      }
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
    if (req.get_type() == ObRequest::OB_RPC) {
      using obrpc::ObRpcPacket;
      const ObRpcPacket& pkt = static_cast<const ObRpcPacket&>(req.get_packet());
      req_level = min(pkt.get_request_level(),
          MAX_REQUEST_LEVEL - 1);  // Requests that exceed the limit are pushed to the highest-level queue
      if (req_level < 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("unexpected level", K(req_level), K(id_));
      } else if (req_level > 1) {
        recv_level_rpc_cnt_.atomic_inc(req_level);
        if (OB_FAIL(multi_level_queue_->push(req, req_level, 0))) {
          LOG_WARN("push request to queue fail", K(ret), K(this));
        }
      } else {
        // (0,5) High priority
        //  (5,10) Normal priority
        //  10 is the low priority used by ddl and should not appear here
        //  11 Ultra-low priority for preheating
        if (is_high_prio(pkt)) {  // the less number the higher priority
          ATOMIC_INC(&recv_hp_rpc_cnt_);
          if (OB_FAIL(req_queue_.push(&req, QQ_HIGH))) {
            if (REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
              LOG_WARN("push request to queue fail", K(ret), K(*this));
            }
          }
        } else if (req.is_retry_on_lock()) {
          ATOMIC_INC(&recv_retry_on_lock_rpc_cnt_);
          if (OB_FAIL(req_queue_.push(&req, QQ_PRIOR_TO_NORMAL))) {
            LOG_WARN("push request to QQ_PRIOR_TO_NORMAL queue fail", K(ret), K(this));
          }
        } else if (is_normal_prio(pkt) || is_low_prio(pkt)) {
          ATOMIC_INC(&recv_np_rpc_cnt_);
          if (OB_FAIL(req_queue_.push(&req, QQ_NORMAL))) {
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
    } else if (req.get_type() == ObRequest::OB_MYSQL) {
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

    } else if (req.get_type() == ObRequest::OB_TASK || req.get_type() == ObRequest::OB_GTS_TASK) {
      ATOMIC_INC(&recv_task_cnt_);
      if (OB_FAIL(req_queue_.push(&req, RQ_HIGH))) {
        LOG_WARN("push request to queue fail", K(ret), K(this));
      }
    } else if (req.get_type() == ObRequest::OB_SQL_TASK) {
      ATOMIC_INC(&recv_sql_task_cnt_);
      if (OB_FAIL(req_queue_.push(&req, RQ_NORMAL))) {
        LOG_WARN("push request to queue fail", K(ret), K(this));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unknown request type", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    ObTenantStatEstGuard guard(id_);
    EVENT_INC(REQUEST_ENQUEUE_COUNT);
  }

  return ret;
}

int ObTenant::recv_large_request(rpc::ObRequest& req)
{
  int ret = OB_SUCCESS;
  req.set_enqueue_timestamp(ObTimeUtility::current_time());
  if (nullptr != group_map_ && group_map_->is_inited()) {
    req.set_large_retry_flag(true);
    if (OB_FAIL(recv_request(req))) {
      LOG_WARN("tenant receive large retry request fail", K(ret));
    }
  } else {
    ATOMIC_INC(&recv_large_req_cnt_);
    if (OB_FAIL(large_req_queue_.push(&req))) {
      LOG_WARN("push large request queue fail", K(req), K(ret));
    } else {
      ObTenantStatEstGuard guard(id_);
      EVENT_INC(REQUEST_ENQUEUE_COUNT);
    }
  }
  return ret;
}

int ObTenant::push_retry_queue(rpc::ObRequest& req, const uint64_t timestamp)
{
  return retry_queue_.push(req, timestamp);
}

int ObTenant::timeup()
{
  int ret = OB_SUCCESS;
  if (cgroup_ctrl_.is_valid() && use_group_map_) {
    calibrate_group_token_count();
    check_group_worker_count();
  } else {
    check_worker_count();
    update_token_usage();
    calibrate_worker_count();
    handle_retry_req();
    calibrate_token_count();
  }
  return ret;
}

void ObTenant::handle_retry_req()
{
  int ret = OB_SUCCESS;
  ObLink* task = nullptr;
  ObRequest* req = NULL;
  while (OB_SUCC(retry_queue_.pop(task))) {
    req = static_cast<rpc::ObRequest*>(task);
    if (OB_FAIL(recv_large_request(*req))) {
      LOG_ERROR("tenant patrol push req fail", "tenant", id_);
      break;
    }
  }
}

void ObTenant::calibrate_token_count()
{
  if (dynamic_modify_token_) {
    int ret = OB_SUCCESS;
    const auto current_time = ObTimeUtility::current_time();
    if (current_time - last_calibrate_token_ts_ > CALIBRATE_TOKEN_INTERVAL && OB_SUCC(workers_lock_.trylock())) {
      int64_t wait_worker = 0;
      int64_t active_workers = 0;
      DLIST_FOREACH_REMOVESAFE(wnode, workers_)
      {
        const auto w = static_cast<ObThWorker*>(wnode->get_data());
        if (w->is_active()) {
          active_workers++;
          if (!w->has_req_flag()) {
            wait_worker++;
          }
        }
      }
      if (sug_token_cnt_ > token_cnt_) {
        set_token(sug_token_cnt_);
      }
      if (last_pop_normal_cnt_ != 0 && pop_normal_cnt_ == last_pop_normal_cnt_) {
        set_token(min(token_cnt_ + 1, worker_count_bound()));
      }
      if (wait_worker > active_workers / 2) {
        set_token(max(token_cnt_ - 1, sug_token_cnt_));
      }
      last_calibrate_token_ts_ = current_time;
      last_pop_normal_cnt_ = pop_normal_cnt_;
      IGNORE_RETURN workers_lock_.unlock();
    }
  }
}

void ObTenant::calibrate_group_token_count()
{
  ObResourceGroup* iter = NULL;
  while (nullptr != group_map_ && group_map_->is_inited() && NULL != (iter = group_map_->quick_next(iter))) {
    iter->calibrate_token_count();
  }
}

void ObTenant::calibrate_worker_count()
{
  int ret = OB_SUCCESS;
  const auto current_time = ObTimeUtility::current_time();
  if (current_time - last_calibrate_worker_ts_ > CALIBRATE_WORKER_INTERVAL) {
    if (OB_SUCC(workers_lock_.trylock())) {
      int active_workers = 0;
      DLIST_FOREACH_REMOVESAFE(wnode, workers_)
      {
        const auto w = static_cast<ObThWorker*>(wnode->get_data());
        if (w->is_active()) {
          active_workers++;
        }
      }
      int64_t new_ass_token_cnt = active_workers - lq_waiting_workers_.get_size();
      LOG_INFO("tenant calibrate worker", K_(id), K_(ass_token_cnt), K(new_ass_token_cnt));
      if (new_ass_token_cnt > 0) {
        ass_token_cnt_ = new_ass_token_cnt;
      }
      last_calibrate_worker_ts_ = current_time;
      IGNORE_RETURN workers_lock_.unlock();
    }
  }
}

int ObTenant::check_worker_count()
{
  int ret = OB_SUCCESS;
  if (nesting_worker_has_init_ < MAX_REQUEST_LEVEL && OB_SUCC(workers_lock_.trylock())) {
    LOG_WARN("tenant acquire nesting worker", K(nesting_worker_has_init_));
    int64_t need_cnt = 1L;
    int64_t succ_cnt = 0L;
    for (int level = nesting_worker_has_init_; level < MAX_REQUEST_LEVEL; level++, nesting_worker_has_init_++) {
      if (OB_SUCCESS != acquire_level_worker(need_cnt, succ_cnt, level) || succ_cnt != need_cnt) {
        break;
      }
      succ_cnt = 0L;
    }
    IGNORE_RETURN workers_lock_.unlock();
    LOG_WARN("tenant acquire nesting worker done", K(nesting_worker_has_init_));
  }
  if (OB_SUCC(workers_lock_.trylock())) {
    // 1. update active workers count
    // 2. remove inactive workers
    // 3. update worker TIDX(tenant index)
    auto active_workers = 0;
    DLIST_FOREACH_REMOVESAFE(wnode, workers_)
    {
      const auto w = static_cast<ObThWorker*>(wnode->get_data());
      if (w->get_worker_level() == 0) {
        const auto active_inactive_ts = w->get_active_inactive_ts();
        const auto sojourn_time = ObTimeUtility::current_time() - active_inactive_ts;
        if (w->is_active()) {
          w->set_tidx(active_workers);
          active_workers++;
        } else if (w->is_waiting_active() && sojourn_time > PRESERVE_INACTIVE_WORKER_TIME) {
          // Sojourn time may not correct before it's not atomic when we
          // get active status and get active_inactive_ts. There are two
          // exceptions:
          //
          //   1. If worker is changing status from inactive to active,
          //      time we get is its total inactive time but we treat it
          //      as its active time. It's OK since we do nothing with
          //      its sojourn time when worker is detected as active.
          //
          //   2. If worker is change status from active to inactive,
          //      then time we get is its total active time but we treat
          //      it as its inactive time. Because we need use worker's
          //      sojourn time to judge when to put inactive worker into
          //      global worker pool, recalculate its sojourn time is
          //      necessary to exclude this case.
          //
          // BTW, With lock of workers it's safe worker won't change
          // status from inactive to active but not opposite.
          const auto active_inactive_ts = w->get_active_inactive_ts();
          const auto sojourn_time = ObTimeUtility::current_time() - active_inactive_ts;
          if (sojourn_time > PRESERVE_INACTIVE_WORKER_TIME) {
            workers_.remove(wnode);
            w->reset();
            worker_pool_.free(w);
          }
        }
      }
    }
    actives_ = active_workers;

    const auto diff = token_cnt_ - ass_token_cnt_;
    if (diff > 0) {
      int64_t succ_num = 0L;
      acquire_more_worker(diff, succ_num);
      // diff is the number of fail workers.
      ass_token_cnt_ += succ_num;
    } else if (diff < 0) {
      ret = OB_NEED_WAIT;
    }

    IGNORE_RETURN workers_lock_.unlock();
  } else {
    ret = OB_EAGAIN;
  }
  return ret;
}

int ObTenant::check_group_worker_count()
{
  int ret = OB_SUCCESS;
  ObResourceGroup* iter = NULL;
  while (nullptr != group_map_ && group_map_->is_inited() && NULL != (iter = group_map_->quick_next(iter))) {
    iter->check_worker_count();
  }
  return ret;
}

int ObTenant::check_worker_count(ObThWorker& w)
{
  int ret = OB_SUCCESS;
  if (nesting_worker_has_init_ < MAX_REQUEST_LEVEL && OB_SUCC(workers_lock_.trylock())) {
    LOG_WARN("thread acquire nesting worker", K(w.get_tidx()), K(nesting_worker_has_init_));
    int64_t need_cnt = 1L;
    int64_t succ_cnt = 0L;
    for (int level = nesting_worker_has_init_; level < MAX_REQUEST_LEVEL; level++, nesting_worker_has_init_++) {
      if (OB_SUCCESS != acquire_level_worker(need_cnt, succ_cnt, level) || succ_cnt != need_cnt) {
        break;
      }
      succ_cnt = 0L;
    }
    IGNORE_RETURN workers_lock_.unlock();
    LOG_WARN("thread acquire nesting worker", K(w.get_tidx()), K(nesting_worker_has_init_));
  }
  if (ass_token_cnt_ != token_cnt_ && OB_SUCC(workers_lock_.trylock())) {
    const auto diff = token_cnt_ - ass_token_cnt_;
    int tmp_ret = OB_SUCCESS;
    // ass_token_cnt_ maybe change before having acquired lock so we
    // check diff once more.
    if (diff > 0) {
      int64_t succ_num = 0L;
      acquire_more_worker(diff, succ_num);
      // acquire_count is the number of fail workers.
      ass_token_cnt_ += succ_num;
    } else if (diff < 0 && w.get_tidx() > 1) {
      // tidx == 0 ==> process_high_prio_task()
      // tidx == 1 ==> process_high_high_prio_task()
      //
      // These two workers mustn't be set inactive, since there may be
      // no available workers process there types of task.
      ass_token_cnt_--;
      ret = OB_NEED_WAIT;
      if (w.has_lq_token()) {
        release_lq_token();
      }
      w.set_lq_token(false);
      w.set_inactive();
      if (cgroup_ctrl_.is_valid() &&
          OB_SUCCESS != (tmp_ret = cgroup_ctrl_.remove_thread_from_cgroup(id_, w.get_tid()))) {
        LOG_WARN("remove thread from cgroup failed", K(tmp_ret), K_(id));
      }
    }
    IGNORE_RETURN workers_lock_.unlock();
  } else {
    ret = OB_EAGAIN;
  }
  return ret;
}

int ObTenant::acquire_level_worker(int64_t num, int64_t& succ_num, int32_t level)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  const auto need_num = num;
  succ_num = 0;

  if (level <= 0 || level > MAX_REQUEST_LEVEL) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected level", K(level), K(id_));
  } else {
    while (OB_SUCC(ret) && need_num > succ_num) {
      ObThWorker* w = worker_pool_.alloc();
      if (w) {
        w->reset();
        w->set_tidx(nesting_workers_.get_size() + 1000);
        w->set_worker_level(level);
        w->set_tenant(this);
        w->activate();
        if (!nesting_workers_.add_last(&w->worker_node_)) {
          OB_ASSERT(false);
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("add worker to list fail", K(ret));
        } else {
          if (cgroup_ctrl_.is_valid() &&
              OB_SUCCESS != (tmp_ret = cgroup_ctrl_.add_thread_to_cgroup(id_, w->get_tid()))) {
            LOG_WARN("add thread to cgroup failed", K(tmp_ret), K_(id));
          }
          succ_num++;
        }
      } else {
        ret = OB_SIZE_OVERFLOW;
      }
    }
  }

  if (need_num != num ||    // Reach worker count bound,
      succ_num != need_num  // or can't allocate enough worker.
  ) {
    if (TC_REACH_TIME_INTERVAL(10000000)) {
      LOG_WARN("Alloc level worker less than lack", K(num), K(need_num), K(succ_num));
    }
  }

  return ret;
}

int ObTenant::acquire_more_worker(int64_t num, int64_t& succ_num)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  const auto bound = worker_count_bound();
  const auto current = workers_.get_size();
  const auto need_num = std::min(num, bound - current);
  succ_num = 0;

  // If tenant has inactive workers, wake up them first.
  DLIST_FOREACH_X(wnode, workers_, need_num > succ_num)
  {
    const auto w = static_cast<ObThWorker*>(wnode->get_data());
    if (!w->is_active()) {
      w->activate();
      succ_num++;
    }
  }

  while (OB_SUCC(ret) && need_num > succ_num) {
    ObThWorker* w = worker_pool_.alloc();
    if (w) {
      w->reset();
      w->set_tidx(workers_.get_size());
      w->set_worker_level(0);
      w->set_tenant(this);
      w->activate();
      if (!workers_.add_last(&w->worker_node_)) {
        OB_ASSERT(false);
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("add worker to list fail", K(ret));
      } else {
        if (cgroup_ctrl_.is_valid() && OB_SUCCESS != (tmp_ret = cgroup_ctrl_.add_thread_to_cgroup(id_, w->get_tid()))) {
          LOG_WARN("add thread to cgroup failed", K(tmp_ret), K_(id));
        }
        succ_num++;
      }
    } else {
      ret = OB_SIZE_OVERFLOW;
    }
  }

  if (need_num != num ||    // Reach worker count bound,
      succ_num != need_num  // or can't allocate enough worker.
  ) {
    if (TC_REACH_TIME_INTERVAL(10000000)) {
      LOG_WARN("Alloc worker less than lack", K(num), K(need_num), K(succ_num));
    }
  }

  return ret;
}

int ObTenant::link_worker(ObWorker& w)
{
  return workers_.add_last(&w.worker_node_);
}
void ObTenant::unlink_worker(ObWorker& w)
{
  workers_.remove(&w.worker_node_);
}
int ObTenant::link_lq_waiting_worker(ObWorker& w)
{
  return lq_waiting_workers_.add_last(&w.lq_waiting_worker_node_);
}
void ObTenant::unlink_lq_waiting_worker(ObWorker& w)
{
  lq_waiting_workers_.remove(&w.lq_waiting_worker_node_);
}

void ObTenant::try_unlink_lq_waiting_worker_with_lock(ObWorker& w)
{
  if (w.lq_waiting_worker_node_.get_next() != nullptr) {
    ObMutexGuard guard(lq_waiting_workers_lock_);
    unlink_lq_waiting_worker(w);
  }
}

// Called by worker self.
void ObTenant::check_paused_worker(ObThWorker& w)
{
  // Check whether there are waiting large queries and large query token is
  // enough, if so wake up that query.
  const auto lq_waitings = lq_waiting_workers_.get_size();
  // Only normal priority workers can wakeup awaiting LQ workers.
  // Worker with tidx == 0/1 only process high priority tasks.
  if (lq_waitings > 0 && w.get_tidx() > 1 && w.is_active()) {
    if (w.has_lq_token() || acquire_lq_token()) {
      w.set_lq_token(true);
    }
    if (w.has_lq_token() || req_queue_.size() == 0) {
      ObMutexGuard guard(lq_waiting_workers_lock_);
      auto* node = lq_waiting_workers_.remove_first();
      if (nullptr != node) {
        OB_ASSERT(node->get_data());
        auto& nw = *static_cast<ObThWorker*>(node->get_data());
        nw.set_lq_token(w.has_lq_token());
        resume_it(nw);
        // LQT is transfer to the waked up worker, so just flag no LQT
        // for current worker.
        w.set_lq_token(false);
        w.set_inactive();
      } else {
        // lq_token would preserved if this worker has so that worker
        // will prefer to process large query rather than normal
        // query.
      }
    }
  }
}

// called each checkpoint for worker of this tenant.
int ObTenant::lq_check_status(ObThWorker& w)
{
  // Only normal priority workers take part in schedule.
  if (w.get_tidx() <= 1) {
    // High priority workers
    if (!w.has_lq_token()) {
      if (acquire_lq_token()) {
        w.set_lq_token();
      }
    }
  } else {
    ATOMIC_INC(&tt_large_quries_);

    bool has_newborn = false;  // has newborn worker?
    bool has_waiting_workers = lq_waiting_workers_.get_size() > 0;

    if (!w.has_lq_token() && !acquire_lq_token()) {
      // Exceeds lq_token count.
      //
      // It's allowed to execute a large query even though it doesn't
      // own a lq_token if there's no normal request waiting for.
      if (get_request_queue_length() > 0) {
        const int64_t cnt = 1;
        int64_t succ_cnt = 0;
        ObMutexGuard guard(workers_lock_);
        acquire_more_worker(cnt, succ_cnt);
        if (succ_cnt > 0) {
          has_newborn = true;
        }
      }
    } else {
      w.set_lq_token();
    }

    // If there isn't another newborn worker being woken up, then
    // has_newborn would set false and we need ensure tenant has enough
    // worker processes its requests. It has two situations.
    //
    //   1. lq_token hasn't been acquired and no new worker available.
    //   2. lq_token has been acquired.
    //
    // Under either condition, we should choose a LARGE QUERY worker to
    // run. The oldest waiting worker will be woken up, or current one
    // if there's no waiting worker.
    if (w.has_lq_token()) {    // worker has lq token, allow to execute
    } else if (has_newborn) {  // Execution token has transferred a
      // newborn worker.
      ObMutexGuard guard(lq_waiting_workers_lock_);
      link_lq_waiting_worker(w);
      pause_it(w);
    } else if (has_waiting_workers) {  // No newborn worker but has
      // waiting workers.
      ObMutexGuard guard(lq_waiting_workers_lock_);
      auto* node = lq_waiting_workers_.remove_first();
      if (nullptr != node) {
        auto& nw = *static_cast<ObThWorker*>(node->get_data());
        nw.set_lq_token(w.has_lq_token());
        resume_it(nw);
        w.set_lq_token(false);
        pause_it(w);
        link_lq_waiting_worker(w);
        has_newborn = true;
      }
    } else {  // no lq token and no newborn worker and no waiting
      // workers, allow to execute
    }
  }
  return w.is_active() ? OB_EAGAIN : OB_SUCCESS;
}

bool ObTenant::has_task() const
{
  bool result = false;
  if (!result) {
    result = req_queue_.size() > 0 || large_req_queue_.size() > 0 || lq_waiting_workers_.get_size() > 0;
  }
  return result;
}

int64_t ObTenant::get_request_queue_length() const
{
  return req_queue_.size();
}

int64_t ObTenant::waiting_count() const
{
  // TODO: add waiting workers with paused task.
  return req_queue_.size();
}

// thread unsafe
void ObTenant::update_token_usage()
{
  const auto now = ObTimeUtility::current_time();
  const auto duration = static_cast<double>(now - token_usage_check_ts_);
  if (duration > 1000 * 1000) {  // every second
    token_usage_check_ts_ = now;
    const auto idle_us = static_cast<double>(ATOMIC_TAS(&idle_us_, 0));
    const auto tokens = static_cast<double>(token_cnt());
    const auto total_us = duration * (tokens + nesting_worker_has_init_ - 1);
    token_usage_ = (total_us - idle_us) / duration;
    token_usage_ = std::max(.0, token_usage_);
  }
}

void ObTenant::periodically_check()
{
  int ret = OB_SUCCESS;
  WITH_ENTITY(ctx_)
  {
    check_parallel_max_servers();
    check_parallel_servers_target();
    check_resource_manager_plan();
  }
}

void ObTenant::check_resource_manager_plan()
{
  int ret = OB_SUCCESS;
  ObString plan_name;
  ObResourcePlanManager& plan_mgr = G_RES_MGR.get_plan_mgr();
  ObResourceMappingRuleManager& rule_mgr = G_RES_MGR.get_mapping_rule_mgr();
  char data[OB_MAX_RESOURCE_PLAN_NAME_LENGTH];
  ObDataBuffer allocator(data, OB_MAX_RESOURCE_PLAN_NAME_LENGTH);
  if (!cgroup_ctrl_.is_valid()) {
    // The cgroup is not initialized successfully, no need to refresh the resource manager plan
  } else if (OB_SYS_TENANT_ID != id_ && OB_MAX_RESERVED_TENANT_ID >= id_) {
    // Except for system rental outside, internal tenants do not use resource plan for internal isolation
  } else if (OB_FAIL(ObSchemaUtils::get_tenant_varchar_variable(
                 id_, SYS_VAR_RESOURCE_MANAGER_PLAN, allocator, plan_name))) {
    LOG_WARN("fail get tenant variable", K(id_), K(plan_name), K(ret));
    // skip
  } else if (OB_FAIL(plan_mgr.refresh_resource_plan(id_, plan_name))) {
    LOG_WARN("refresh resource plan fail."
             "Tenant resource isolation may not work",
        K(id_),
        K(plan_name),
        K(ret));
  } else if (OB_FAIL(rule_mgr.refresh_resource_mapping_rule(id_, plan_name))) {
    LOG_WARN("refresh resource mapping rule fail."
             "Tenant resource isolation may not work",
        K(id_),
        K(plan_name),
        K(ret));
  }
}

void ObTenant::check_parallel_max_servers()
{
  /*
    int ret = OB_SUCCESS;
    static constexpr uint64_t MAX_TASKS_PER_CPU = 1;
    int64_t parallel_max_servers = 0;

    int64_t px_cpus = 0; // Native tenant-level px thread pool size
    bool px_enabled = false;
    bool skip_check = false;

    if (OB_SYS_TENANT_ID != id_ && OB_MAX_RESERVED_TENANT_ID >= id_) {
      skip_check = true; // Except for system rentals, internal tenants do not allocate px threads
    } else {
      if (OB_FAIL(ObSchemaUtils::get_tenant_int_variable(
                  id_,
                  SYS_VAR_PARALLEL_MAX_SERVERS,
                  parallel_max_servers))) {
        // Px_cpus failed to be brushed, you cannot disable px directly, otherwise it will cause the pool to be
    destroyed skip_check = true; } else { px_cpus = parallel_max_servers; px_enabled = (px_cpus > 0);
      }
    }

    LOG_TRACE("check cpu",
              "tenant_id", id_,
              K(skip_check),
              K(px_enabled),
              K_(unit_min_cpu),
              K(parallel_max_servers),
              K(px_cpus));

    if (!skip_check) {
      auto px_pool = MTL_GET(ObPxPool*);
      auto px_pool_stat = MTL_GET(sql::ObPxPoolStat*);
      auto tenant_dfc = MTL_GET(ObTenantDfc*);

      if (OB_ISNULL(px_pool) || OB_ISNULL(px_pool_stat) || OB_ISNULL(tenant_dfc)) {
        LOG_ERROR("null ptr, disable update px thread count", KP(px_pool), KP(px_pool_stat));
      } else if (px_enabled) {
        if (px_pool_is_running_) {
          LOG_TRACE("Check number of CPUs using by PX pool.",
                    "tenant_id", id_,
                    K(px_cpus),
                    "prev_px_cpus", px_pool_stat->get_pool_size());
          // Check number of CPUs using by PX pool.
          // All stack memory is temporarily counted on 500 tenants
          //px_pool->set_tenant_id(id_);
          if (OB_FAIL(px_pool->set_thread_count(px_cpus))) {
            LOG_WARN("set PX pool thread count fail", "tenant_id", id_, K(px_cpus), K(ret));
          } else {
            if (px_cpus != px_pool_stat->get_pool_size()) {
              LOG_INFO("update PX pool size success", "tenant_id", id_, K(px_cpus));
            }
            px_pool_stat->set_pool_size(px_cpus);
            tenant_dfc->calc_max_buffer(px_cpus);
          }
        } else {
          // Initialize and start PX pool if it isn't running.
          if (OB_FAIL(px_pool->init())) {
            LOG_WARN("initialize PX pool fail", "tenant_id", id_, K(ret));
          } else {
            px_pool->set_cgroup_ctrl(&cgroup_ctrl_);
            px_pool->set_thread_max_tasks(MAX_TASKS_PER_CPU);
            if (OB_FAIL(px_pool->set_thread_count(px_cpus))) {
              LOG_WARN("set PX pool thread count fail", "tenant_id", id_, K(px_cpus), K(ret));
            } else if (OB_FAIL(px_pool->start())) {
              px_pool->destroy();
              LOG_WARN("start PX pool fail", "tenant_id", id_, K(ret));
            } else {
              px_pool_is_running_ = true;
              px_pool_stat->set_pool_size(px_cpus);
              tenant_dfc->calc_max_buffer(px_cpus);
              LOG_INFO("start PX pool success", "tenant_id", id_, K(px_cpus));
            }
          }
        }
      } else {
        // Destroy running PX pool if PX has been disabled.
        if (px_pool_is_running_) {
          px_pool_stat->set_pool_size(0);
          px_pool->set_thread_count(0);
          px_pool->stop();
          px_pool->wait();
          px_pool->destroy();
          px_pool_is_running_ = false;
        }
      }
    }
  */
}

void ObTenant::check_parallel_servers_target()
{
  int ret = OB_SUCCESS;
  int64_t val = 0;
  auto px_pool_stat = MTL_GET(sql::ObPxPoolStat*);
  if (OB_SYS_TENANT_ID != id_ && OB_MAX_RESERVED_TENANT_ID >= id_) {
    // Except for system rentals, internal tenants do not allocate px threads
  } else if (OB_ISNULL(px_pool_stat)) {
    LOG_WARN("null ptr unexpected", KP(px_pool_stat));
  } else if (OB_FAIL(ObSchemaUtils::get_tenant_int_variable(id_, SYS_VAR_PARALLEL_SERVERS_TARGET, val))) {
    LOG_WARN("fail read tenant variable", K_(id), K(ret));
  } else {
    px_pool_stat->set_target(val);
  }
}
