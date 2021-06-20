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
#include "ob_th_worker.h"

#include "share/ob_define.h"
#include "lib/time/ob_time_utility.h"
#include "lib/oblog/ob_trace_log.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/stat/ob_session_stat.h"
#include "lib/allocator/ob_page_manager.h"
#include "lib/rc/context.h"
#include "lib/thread/ob_thread_name.h"
#include "lib/coro/routine.h"
#include "ob_tenant.h"
#include "ob_worker_processor.h"
#include "share/config/ob_server_config.h"
#include "observer/ob_server.h"
#include "storage/memtable/ob_lock_wait_mgr.h"
#include "sql/session/ob_sql_session_info.h"

using namespace oceanbase;
using namespace oceanbase::lib;
using namespace oceanbase::common;
using namespace oceanbase::omt;
using namespace oceanbase::rpc;
using namespace oceanbase::rpc::frame;

ObThWorker::ObThWorker(ObIWorkerProcessor& procor)
    : procor_(procor),
      is_inited_(false),
      tenant_(nullptr),
      group_(nullptr),
      run_cond_(),
      pause_flag_(false),
      large_query_(false),
      query_start_time_(0),
      last_check_time_(0),
      can_retry_(true),
      need_retry_(false),
      retry_in_place_(false),
      ws_(WStatus::STOPPED),
      active_(false),
      waiting_active_(false),
      active_inactive_ts_(0L),
      lq_token_(false)
{}

ObThWorker::~ObThWorker()
{}

int ObThWorker::init()
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("the ObThWorker has been inited, ", K(ret));
  } else if (OB_FAIL(run_cond_.init(ObWaitEventIds::TH_WORKER_COND_WAIT))) {
    LOG_ERROR("init run cond fail, ", K(ret));
  } else {
    is_inited_ = true;
  }

  return ret;
}

void ObThWorker::destroy()
{
  if (is_inited_) {
    run_cond_.destroy();
    is_inited_ = false;
  }
}

class ObDiagTenantGuard {
public:
  ObDiagTenantGuard(ObThWorker& worker, uint64_t tenant_id) : worker_(worker)
  {
    if (OB_DIAG_TENANT_ID == tenant_id) {
      worker.set_rpc_tenant(tenant_id);
      LOG_INFO("set_rpc_tenant", K(tenant_id));
    }
  }
  ~ObDiagTenantGuard()
  {
    worker_.reset_rpc_tenant();
  }

private:
  ObThWorker& worker_;
};

// by other thread
void ObThWorker::resume()
{
  ObThreadCondGuard guard(run_cond_);
  pause_flag_ = false;
  run_cond_.signal();
}

void ObThWorker::activate()
{
  ObThreadCondGuard guard(run_cond_);
  active_inactive_ts_ = ObTimeUtility::current_time();
  active_ = true;
  run_cond_.signal();
}

void ObThWorker::wait_active()
{
  bool has_reset_pm = false;
  auto* pm = common::ObPageManager::thread_local_instance();
  ObThreadCondGuard guard(run_cond_);
  while (OB_UNLIKELY(!active_)) {
    if (pm != nullptr && !has_reset_pm) {
      auto& pmc = common::ObPageManagerCenter::get_instance();
      if (pmc.has_register(*pm)) {
        pmc.unregister_pm(*pm);
      }
      pm->reset();
      has_reset_pm = true;
    }
    waiting_active_ = true;
    IGNORE_RETURN run_cond_.wait();
    waiting_active_ = false;
  }
  ws_ = WStatus::IDLE;
}

inline void ObThWorker::wait_runnable()
{
  int64_t wait_us = std::max(0L, get_timeout_remain());
  if (OB_UNLIKELY(!tenant_->has_stopped()) && wait_us > 0 && pause_flag_) {
    ObThreadCondGuard guard(run_cond_);
    while (!tenant_->has_stopped() && wait_us > 0 && pause_flag_) {
      run_status_ = RS_PAUSED;
      WAIT_BEGIN(OMT_WAIT, wait_us, 0, 0, 0);
      NG_TRACE(wait_start);
      IGNORE_RETURN run_cond_.wait_us(wait_us);
      NG_TRACE(wait_end);
      WAIT_END(OMT_WAIT);
      wait_us = std::max(0L, get_timeout_remain());
    }
  }
  // LQ Worker being woken up maybe has 2 reasons generally.
  //
  // 1. Scheduler thinks it should run, e.g. LQ token is enough or
  //    tenant is deleting.
  // 2. The task has reached its timeout and this worker should run
  //    the cleanup process ASAP.
  //
  // In the second condition, current worker may be still in the
  // waiting queue. So that we need try to remove it from the queue by
  // invoking this function.
  tenant_->try_unlink_lq_waiting_worker_with_lock(*this);
  pause_flag_ = false;
  run_status_ = RS_RUN;
}

// Check only before user request starts
ObThWorker::Status ObThWorker::check_qtime_throttle()
{
  Status st = WS_NOWAIT;
  if (!OB_ISNULL(tenant_)) {
    const int64_t curr_time = ObClockGenerator::getClock();
    auto& st_metrics = tenant_->get_sql_throttle_metrics();
    if (st_current_priority_ != -1 && st_current_priority_ <= st_metrics.priority_) {
      if ((st_metrics.queue_time_ >= .0) && (get_query_start_time() - get_query_enqueue_time() >=
                                                static_cast<int64_t>(st_metrics.queue_time_ * 1000000L))) {
        st = WS_OUT_OF_THROTTLE;
        LOG_WARN("query is throttled",
            "queue_time_threshold(s)",
            st_metrics.queue_time_,
            "query_enqueue_time",
            get_query_enqueue_time(),
            "query_start_time",
            get_query_start_time());
      }
    }
  }
  return st;
}

// Periodic inspection
ObThWorker::Status ObThWorker::check_throttle()
{
  Status st = WS_NOWAIT;
  if (!OB_ISNULL(tenant_) && !OB_ISNULL(session_) && !static_cast<sql::ObSQLSessionInfo*>(session_)->is_inner()) {
    const int64_t curr_time = ObTimeUtility::current_time();
    auto& st_metrics = tenant_->get_sql_throttle_metrics();
    if (st_current_priority_ != -1 && st_current_priority_ <= st_metrics.priority_) {
      if ((st_metrics.rt_ >= .0) &&
          (curr_time - get_query_start_time() >= static_cast<int64_t>(st_metrics.rt_ * 1000000L))) {
        st = WS_OUT_OF_THROTTLE;
        LOG_WARN("query is throttled",
            "rt_threshold(s)",
            st_metrics.rt_,
            "query_start_time",
            get_query_start_time(),
            "current_time",
            curr_time);
      }
    }
  }
  return st;
}

ObThWorker::Status ObThWorker::check_rate_limiter()
{
  Status st = WS_NOWAIT;
  if (!OB_ISNULL(tenant_)) {
    auto& st_rate_limiter = tenant_->get_sql_rate_limiter();
    if (st_rate_limiter.rate() <= 0) {
      // do nothing
    } else if (OB_EAGAIN == st_rate_limiter.try_acquire()) {
      st = WS_OUT_OF_THROTTLE;
    }
  }
  return st;
}

// by self thread
ObThWorker::Status ObThWorker::check_wait()
{
  const int64_t threshold = GCONF.large_query_threshold;
  const int64_t curr_time = ObTimeUtility::current_time();
  Status st = WS_NOWAIT;
  if (OB_UNLIKELY(tenant_->has_stopped())) {
    st = WS_INVALID;
  } else if (OB_UNLIKELY(!tenant_->user_sched_enabled())) {
  } else if (OB_UNLIKELY(true == get_disable_wait_flag())) {
  } else if (this->get_curr_request_level() > 1) {
  } else if (this->get_group() != nullptr) {
  } else if (curr_time > last_check_time_ + WORKER_CHECK_PERIOD) {
    st = check_throttle();
    if (st != WS_OUT_OF_THROTTLE) {
      if (OB_UNLIKELY(curr_time > get_query_start_time() + threshold)) {
        large_query_ = true;
        tenant_->lq_check_status(*this);
        wait_runnable();
      } else {
        // no need to reset large query flag
        // large_query_ = false;
      }
    }
    last_check_time_ = curr_time;
  }
  return st;
}

inline void ObThWorker::process_request(rpc::ObRequest& req)
{
  // reset retry flags
  can_retry_ = true;
  need_retry_ = false;
  int ret = OB_SUCCESS;
  reset_sql_throttle_current_priority();
  ObDiagTenantGuard diag_guard(*this, tenant_ ? tenant_->id() : OB_SYS_TENANT_ID);
  set_req_flag(true);
  if (retry_in_place_ && (nullptr != tenant_) && tenant_->id() > 1000) {
    do {
      need_retry_ = false;
      if (OB_FAIL(procor_.process(req))) {
        LOG_WARN("process request fail", K(ret));
      }
      if (need_retry_) {
        LOG_WARN("force reprocess request", "tenant", tenant_->id());
      }
    } while (need_retry_);
  } else {
    memtable::get_global_lock_wait_mgr().setup(req.get_lock_wait_node(), req.get_receive_timestamp());
    if (OB_FAIL(procor_.process(req))) {
      LOG_WARN("process request fail", K(ret));
    }

    bool need_wait = false;
    bool wait_succ = memtable::get_global_lock_wait_mgr().post_process(need_retry_, need_wait);
    // Return code maybe lost, but we don't care.
    if (need_retry_) {
      int32_t retry_times = req.get_retry_times();
      req.set_retry_times(retry_times + 1);
      if (need_wait) {
        if (!wait_succ) {
          if (OB_FAIL(tenant_->recv_request(req))) {
            LOG_WARN("tenant receive retry_on_lock request fail, retry with current worker", K(ret));
          }
        }
      } else if (retry_times) {
        if (retry_times == 1) {
          LOG_WARN("tenant push retry request to wait queue", "tenant", tenant_->id(), K(req));
        }
        uint64_t curr_timestamp = ObTimeUtility::current_time();
        uint64_t delta_us = curr_timestamp - req.get_receive_timestamp();
        uint64_t timestamp = curr_timestamp + min(delta_us, 100 * 1000L);
        if (OB_FAIL(tenant_->push_retry_queue(req, timestamp))) {
          LOG_WARN(
              "tenant schedule retry_on_lock request fail, retry with current worker", "tenant", tenant_->id(), K(ret));
        }
      } else if (OB_FAIL(tenant_->recv_large_request(req))) {
        LOG_WARN("tenant receive large request fail, "
                 "retry with current worker",
            K(ret));
      }

      if (OB_FAIL(ret)) {
        can_retry_ = false;
        need_retry_ = false;
        if (OB_FAIL(procor_.process(req))) {
          LOG_WARN("request retry with current worker fail", K(ret));
        }
      }
    }
  }
  // TODO@: confirm 8K is enough
  if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
    auto* pm = common::ObPageManager::thread_local_instance();
    const int64_t pm_hold = (pm != nullptr) ? pm->get_hold() : 0;
    _OB_LOG(INFO,
        "worker= %ld thd_flag=%d total=%ld used=%ld pm_hold=%ld",
        ObWorker::get_tidx(),
        has_req_flag(),
        get_allocator().total(),
        get_allocator().used(),
        pm_hold);
  }
  set_req_flag(false);
}

void ObThWorker::set_th_worker_thread_name(uint64_t tenant_id)
{
  // fix compile issue
  UNUSED(tenant_id);

  static __thread uint64_t serving_tenant_id = 0;
  char buf[32];
  if (serving_tenant_id != tenant_->id()) {
    snprintf(buf, 32, "TNT_L%d_", get_worker_level());
    lib::set_thread_name(buf, tenant_->id());
    serving_tenant_id = tenant_->id();
  }
}

void ObThWorker::worker(int64_t& tenant_id, int64_t& req_recv_timestamp, int32_t& worker_level)
{
  int ret = OB_SUCCESS;
  lib::set_thread_name("OMT_FREE", ObWorker::get_tidx());
  Worker::self_ = this;
  ObWorker::self_ = this;
  int64_t wait_start_time = 0;
  int64_t wait_end_time = 0;

  this_thread::set_monopoly();
  th_created();

  // Avoid adding and deleting entities from the root node for every request, the parameters are meaningless
  CREATE_WITH_TEMP_ENTITY(RESOURCE_OWNER, OB_SERVER_TENANT_ID)
  {
    CREATE_WITH_TEMP_ENTITY(TABLE_SPACE, combine_id(1, 1))
    {
      auto* pm = common::ObPageManager::thread_local_instance();
      if (this->get_worker_level() == INT32_MAX) {
        this->set_worker_level(0);
      }
      while (!has_set_stop()) {
        wait_active();
        worker_level = this->get_worker_level();  // Update backtrace printing parameters
        if (nullptr != this->tenant_) {
          tenant_id = this->tenant_->id();  // Update backtrace printing parameters
        }
        if (OB_UNLIKELY(has_set_stop())) {
          // empty
        } else if (OB_ISNULL(tenant_)) {
          LOG_ERROR("invalid status, unexpected", K(tenant_));
        } else {
          if (OB_LIKELY(pm != nullptr)) {
            if (pm->get_used() != 0) {
              LOG_ERROR("page manager's used should be 0, unexpected!!!", KP(pm));
            } else {
              // Ignore the above warning
              ret = pm->set_tenant_ctx(tenant_->id(), ObCtxIds::DEFAULT_CTX_ID);
            }
          }
          set_th_worker_thread_name(tenant_->id());
          lib::ContextTLOptGuard guard(true);
          lib::ContextParam param;
          param.set_mem_attr(tenant_->id(), ObModIds::OB_SQL_EXECUTOR, ObCtxIds::DEFAULT_CTX_ID)
              .set_properties(lib::USE_TL_PAGE_OPTIONAL)
              .set_ablock_size(lib::INTACT_MIDDLE_AOBJECT_SIZE);
          CREATE_WITH_TEMP_CONTEXT(param)
          {
            class AllocatorGuard {
            public:
              AllocatorGuard(ObIAllocator** allocator) : allocator_(allocator)
              {
                *allocator_ = &CURRENT_CONTEXT.get_arena_allocator();
              }
              ~AllocatorGuard()
              {
                *allocator_ = nullptr;
              }

            private:
              ObIAllocator** allocator_;
            } allocator_guard(&allocator_);
            const uint64_t owner_id =
                (!is_virtual_tenant_id(tenant_->id()) || is_virtual_tenant_for_memory(tenant_->id()))
                    ? tenant_->id()
                    : OB_SERVER_TENANT_ID;
            CREATE_WITH_TEMP_ENTITY(RESOURCE_OWNER, owner_id)
            {
              WITH_ENTITY(&tenant_->ctx())
              {
                ObTenantStatEstGuard guard(tenant_->id());
                set_compatibility_mode(tenant_->get_compat_mode());

                // get request from queue and process it
                rpc::ObRequest* req = NULL;
                wait_start_time = ObTimeUtility::current_time();

                /// get request from tenant
                {
                  ObWaitEventGuard wait_guard(ObWaitEventIds::OMT_IDLE, 0, wait_start_time, 0, 0);
                  ret = tenant_->get_new_request(*this, REQUEST_WAIT_TIME, req);
                  wait_end_time = ObTimeUtility::current_time();
                }

                if (OB_SUCC(ret)) {
                  if (OB_LIKELY(nullptr != req)) {
                    req_recv_timestamp = req->get_receive_timestamp();  // Update backtrace printing parameters
                    EVENT_ADD(REQUEST_QUEUE_TIME, wait_end_time - req->get_enqueue_timestamp());
                    if (req->ez_req_) {
                      req->set_push_pop_diff(wait_end_time);
                    }
                    query_start_time_ = wait_end_time;
                    query_enqueue_time_ = req->get_enqueue_timestamp();
                    last_check_time_ = wait_end_time;
                    process_request(*req);
                    query_enqueue_time_ = INT64_MAX;
                    query_start_time_ = INT64_MAX;
                  } else {
                    ret = OB_ERR_UNEXPECTED;
                    LOG_ERROR("got NULL request from tenant", K(tenant_), K(ret), K(req));
                  }
                } else if (OB_ENTRY_NOT_EXIST == ret) {
                  // timeout while waiting for request from tenant request queue
                  ret = OB_SUCCESS;
                }
                tenant_->add_idle_time(wait_end_time - wait_start_time);
                if (this->get_worker_level() == 0 && this->get_group() == nullptr) {
                  tenant_->check_worker_count(*this);
                  tenant_->check_paused_worker(*this);
                } else if (this->get_group() != nullptr) {
                  group_->check_worker_count(*this);
                }
              }
            }
          }
        }
      }
    }
  }

  th_destroy();
}

void ObThWorker::run(int64_t idx)
{
  UNUSED(idx);
  // The information that needs to be printed in the backtrace is placed in the parameter
  int64_t tenant_id = -1;
  int64_t req_recv_timestamp = -1;
  int32_t worker_level = -1;
  this->worker(tenant_id, req_recv_timestamp, worker_level);
}

int ObThWorker::check_large_query_quota()
{
  // We just always return fail that the task will retry after current
  // process is done.
  int ret = OB_SUCCESS;
  if (OB_ISNULL(tenant_)) {
    // Back ground thread may also check large query quota, whereas we
    // always return success
  } else if (tenant_->id() >= OB_SERVER_TENANT_ID && tenant_->id() <= OB_MAX_RESERVED_TENANT_ID) {
    // do nothing, these tenants don't support large query schedule.
  } else if (this->get_curr_request_level() > 1) {
    // do nothing, level request not support large query retry
  } else if (tenant_->user_sched_enabled() && can_retry_ && !large_query()) {
    // if current query is not served by large_query worker (!large_query())
    // evict it back to large query queue
    need_retry_ = true;
    ret = OB_EAGAIN;
  }
  return ret;
}

void ObThWorker::th_created()
{
  procor_.th_created();
}

void ObThWorker::th_destroy()
{
  procor_.th_destroy();
}

int ObThWorker::check_status()
{
  int ret = OB_SUCCESS;
  if (nullptr != session_) {
    session_->is_terminate(ret);
  }

  if (OB_SUCC(ret)) {
    if (is_timeout()) {
      ret = OB_TIMEOUT;
    } else {
      if (WS_OUT_OF_THROTTLE == check_wait()) {
        ret = OB_KILLED_BY_THROTTLING;
      }
    }
  }
  return ret;
}
