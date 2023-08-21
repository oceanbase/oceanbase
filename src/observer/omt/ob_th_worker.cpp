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
#include "ob_tenant.h"
#include "ob_worker_processor.h"
#include "share/config/ob_server_config.h"
#include "observer/ob_server.h"
#include "storage/memtable/ob_lock_wait_mgr.h"
#include "sql/session/ob_sql_session_info.h"

using namespace oceanbase;
using namespace oceanbase::lib;
using namespace oceanbase::common;
using namespace oceanbase::observer;
using namespace oceanbase::omt;
using namespace oceanbase::rpc;
using namespace oceanbase::rpc::frame;

namespace oceanbase
{
namespace memtable
{
extern TLOCAL(bool, TLOCAL_NEED_WAIT_IN_LOCK_WAIT_MGR);
}

namespace omt
{
int create_worker(ObThWorker* &worker, ObTenant *tenant, int32_t group_id,
                  int32_t level, bool force, ObResourceGroup *group)
{
  int ret = OB_SUCCESS;
  if (!force && tenant->total_worker_cnt() >= tenant->max_worker_cnt()) {
    ret = OB_RESOURCE_OUT;
    LOG_WARN("create worker fail", K(ret), K(tenant->id()), K(group_id), K(level),
                                    K(tenant->total_worker_cnt()), K(tenant->max_worker_cnt()));
  } else if (OB_ISNULL(worker = OB_NEW(ObThWorker,
                                       ObMemAttr(0 == GET_TENANT_ID() ? OB_SERVER_TENANT_ID : GET_TENANT_ID(),
                                       "OMT_Worker",
                                       ObCtxIds::DEFAULT_CTX_ID, OB_NORMAL_ALLOC)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("create worker fail", K(ret), K(tenant->id()), K(group_id), K(level));
  } else if (OB_FAIL(worker->init())) {
    LOG_ERROR("init worker fail", K(ret), K(tenant->id()), K(group_id), K(level));
    ob_delete(worker);
    worker = nullptr;
  } else {
    worker->reset();
    worker->set_tenant(tenant);
    worker->set_group_id(group_id);
    worker->set_worker_level(level);
    worker->set_group(group);
    if (OB_FAIL(worker->start())) {
      ob_delete(worker);
      worker = nullptr;
      LOG_ERROR("worker start failed", K(ret), K(tenant->id()), K(group_id), K(level));
    } else {
      ++tenant->total_worker_cnt_;
    }
  }
  return ret;
}

int destroy_worker(ObThWorker *worker)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(worker)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(worker), K(ret));
  } else {
    auto* tenant = worker->get_tenant();
    worker->stop();
    worker->wait();
    worker->destroy();
    ob_delete(worker);
    --tenant->total_worker_cnt_;
  }
  return ret;
}
}// end of namespace omt
}// end of namespace oceanbase

ObThWorker::ObThWorker()
    : procor_(ObServer::get_instance().get_net_frame().get_xlator(), ObServer::get_instance().get_self()),
      is_inited_(false), tenant_(nullptr),
      group_(nullptr), run_cond_(),
      pause_flag_(false), large_query_(false),
      priority_limit_(RQ_LOW), is_lq_yield_(false),
      query_start_time_(0), last_check_time_(0),
      can_retry_(true), need_retry_(false),
      has_add_to_cgroup_(false), last_wakeup_ts_(0), blocking_ts_(nullptr),
      idle_us_(0)
{
}

ObThWorker::~ObThWorker()
{
}

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

// by other thread
void ObThWorker::resume()
{
  ObThreadCondGuard guard(run_cond_);
  pause_flag_ = false;
  run_cond_.signal();
}


RLOCAL(uint64_t, serving_tenant_id);

// Check only before user request starts
ObThWorker::Status ObThWorker::check_qtime_throttle()
{
  Status st = WS_NOWAIT;
  if (!OB_ISNULL(tenant_)) {
    auto &st_metrics = tenant_->get_sql_throttle_metrics();
    if (st_current_priority_ != -1 && st_current_priority_ <= st_metrics.priority_) {
      if ((st_metrics.queue_time_ >= .0) &&
          (get_query_start_time() - get_query_enqueue_time() >=
           static_cast<int64_t>(st_metrics.queue_time_ * 1000000L))) {
        st = WS_OUT_OF_THROTTLE;
        LOG_WARN_RET(OB_ERROR, "query is throttled",
                 "queue_time_threshold(s)", st_metrics.queue_time_,
                 "query_enqueue_time", get_query_enqueue_time(),
                 "query_start_time", get_query_start_time());
      }
    }
  }
  return st;
}

// Periodic inspection
ObThWorker::Status ObThWorker::check_throttle()
{
  Status st = WS_NOWAIT;
  if (!OB_ISNULL(tenant_) && !OB_ISNULL(session_) &&
      !static_cast<sql::ObSQLSessionInfo*>(session_)->is_inner()) {
    const int64_t curr_time = common::ObClockGenerator::getClock();
    auto &st_metrics = tenant_->get_sql_throttle_metrics();
    if (st_current_priority_ != -1 && st_current_priority_ <= st_metrics.priority_) {
      if ((st_metrics.rt_ >= .0) &&
         (curr_time - get_query_start_time() >=
          static_cast<int64_t>(st_metrics.rt_ * 1000000L))) {
        st = WS_OUT_OF_THROTTLE;
        LOG_WARN_RET(OB_ERR_UNEXPECTED, "query is throttled",
                 "rt_threshold(s)", st_metrics.rt_,
                 "query_start_time", get_query_start_time(),
                 "current_time", curr_time);
      }
    }
  }
  return st;
}

ObThWorker::Status ObThWorker::check_rate_limiter()
{
  Status st = WS_NOWAIT;
  if (!OB_ISNULL(tenant_)) {
    auto &st_rate_limiter = tenant_->get_sql_rate_limiter();
    if (st_rate_limiter.rate() <= 0)  {
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
  const int64_t curr_time = common::ObClockGenerator::getClock();
  Status st = WS_NOWAIT;
  if (OB_UNLIKELY(tenant_->has_stopped())) {
    st = WS_INVALID;
  } else if (OB_UNLIKELY(!tenant_->user_sched_enabled())) {
  } else if (OB_UNLIKELY(true == get_disable_wait_flag())) {
  } else if (this->get_curr_request_level() >= MULTI_LEVEL_THRESHOLD) {
  } else if (this->is_group_worker() && this->get_group_id() != share::OBCG_LQ) {
  } else if (curr_time > last_check_time_ + WORKER_CHECK_PERIOD) {
    st = check_throttle();
    if (st != WS_OUT_OF_THROTTLE) {
      if (OB_UNLIKELY(curr_time > get_query_start_time() + threshold)) {
        tenant_->lq_yield(*this);
      }
    }
    last_check_time_ = curr_time;
  }
  return st;
}

inline void ObThWorker::process_request(rpc::ObRequest &req)
{
  // reset retry flags
  can_retry_ = true;
  need_retry_ = false;
  bool need_wait_lock = false;
  int ret = OB_SUCCESS;
  reset_sql_throttle_current_priority();
  set_req_flag(&req);

  memtable::TLOCAL_NEED_WAIT_IN_LOCK_WAIT_MGR = false;
  MTL(memtable::ObLockWaitMgr*)->setup(req.get_lock_wait_node(), req.get_receive_timestamp());
  if (OB_FAIL(procor_.process(req))) {
    LOG_WARN("process request fail", K(ret));
  }
  bool wait_succ = MTL(memtable::ObLockWaitMgr*)->post_process(need_retry_, need_wait_lock);
  if (OB_LIKELY(wait_succ)) {
    need_retry_ = false;
  }
  // need_retry_ can be set in procor_.process() via THIS_WORKER.set_need_retry()
  if (OB_UNLIKELY(need_retry_)) {
    int32_t retry_times = req.get_retry_times();
    req.set_retry_times(retry_times + 1);
    if (need_wait_lock) {
      if (!wait_succ) {
        if (OB_FAIL(tenant_->recv_request(req))) {
          LOG_WARN("tenant receive retry_on_lock request fail, retry with current worker", K(ret));
        }
      }
    } else if (retry_times) {
      if (retry_times == 1) {
        LOG_WARN("tenant push retry request to wait queue", "tenant", tenant_->id(), K(req));
      }
      uint64_t curr_timestamp = common::ObClockGenerator::getClock();
      uint64_t delta_us = curr_timestamp - req.get_receive_timestamp();
      uint64_t timestamp = curr_timestamp + min(delta_us, 100 * 1000UL);
      if (OB_FAIL(tenant_->push_retry_queue(req, timestamp))) {
        LOG_WARN("tenant schedule retry_on_lock request fail, retry with current worker","tenant", tenant_->id(), K(ret));
      }
    } else if (OB_FAIL(tenant_->recv_large_request(req))) {
      LOG_WARN("tenant receive large request fail, "
               "retry with current worker", K(ret));
    }

    if (OB_FAIL(ret)) {
      can_retry_ = false;
      need_retry_ = false;
      if (OB_FAIL(procor_.process(req))) {
        LOG_WARN("request retry with current worker fail", K(ret));
      }
    }
  }

  set_req_flag(NULL);
  reset_rpc_tenant();
}

void ObThWorker::set_th_worker_thread_name()
{
  char buf[32];
  if (serving_tenant_id != tenant_->id()) {
    serving_tenant_id = tenant_->id();
    snprintf(buf, 32, "L%d_G%d", get_worker_level(), get_group_id());
    lib::set_thread_name(buf);
  }
}

void ObThWorker::worker(int64_t &tenant_id, int64_t &req_recv_timestamp, int32_t &worker_level)
{
  int ret = OB_SUCCESS;
  Worker::set_worker_to_thread_local(static_cast<lib::Worker*>(this));
  int64_t wait_start_time = 0;
  int64_t wait_end_time = 0;
  procor_.th_created();
  blocking_ts_ = &Thread::blocking_ts_;

  ObTLTaGuard ta_guard(tenant_->id());
  // Avoid adding and deleting entities from the root node for every request, the parameters are meaningless
  CREATE_WITH_TEMP_ENTITY(RESOURCE_OWNER, OB_SERVER_TENANT_ID) {
    auto *pm = common::ObPageManager::thread_local_instance();
    if (this->get_worker_level() == INT32_MAX) {
      this->set_worker_level(0);
    }
    while (!has_set_stop()) {
      worker_level = get_worker_level();
      if (OB_NOT_NULL(tenant_)) {
        tenant_id = tenant_->id();
      }
      if (OB_NOT_NULL(GCTX.cgroup_ctrl_) && OB_LIKELY(GCTX.cgroup_ctrl_->is_valid()) && !has_add_to_cgroup_) {
        if (OB_SUCC(GCTX.cgroup_ctrl_->add_self_to_cgroup(tenant_->id(), get_group_id()))) {
          has_add_to_cgroup_ = true;
        }
      }
      if (OB_NOT_NULL(pm)) {
        if (pm->get_used() != 0) {
          LOG_ERROR("page manager's used should be 0, unexpected!!!", KP(pm));
        } else {
          // Ignore the above warning
          ret = pm->set_tenant_ctx(tenant_->id(), ObCtxIds::DEFAULT_CTX_ID);
        }
      }
      CLEAR_INTERRUPTABLE();
      set_th_worker_thread_name();
      lib::ContextTLOptGuard guard(true);
      lib::ContextParam param;
      param.set_mem_attr(tenant_->id(), ObModIds::OB_SQL_EXECUTOR, ObCtxIds::DEFAULT_CTX_ID)
        .set_page_size(!lib::is_mini_mode() ?
            OB_MALLOC_BIG_BLOCK_SIZE : OB_MALLOC_MIDDLE_BLOCK_SIZE)
        .set_properties(lib::USE_TL_PAGE_OPTIONAL)
        .set_ablock_size(lib::INTACT_MIDDLE_AOBJECT_SIZE);
      CREATE_WITH_TEMP_CONTEXT(param) {
        const uint64_t owner_id =
          (!is_virtual_tenant_id(tenant_->id()) || is_virtual_tenant_for_memory(tenant_->id())) ?
          tenant_->id() : OB_SERVER_TENANT_ID;
        CREATE_WITH_TEMP_ENTITY(RESOURCE_OWNER, owner_id) {
          class AllocatorGuard {
          public:
            AllocatorGuard(ObIAllocator **allocator)
              : allocator_(allocator)
            {
              *allocator_ = &CURRENT_CONTEXT->get_arena_allocator();
            }
            ~AllocatorGuard()
            {
              *allocator_ = nullptr;
            }
          private:
            ObIAllocator **allocator_;
          } allocator_guard(&allocator_);
          WITH_ENTITY(&tenant_->ctx()) {
            ObTenantStatEstGuard guard(tenant_->id());
            set_compatibility_mode(tenant_->get_compat_mode());
            // get request from queue and process it
            rpc::ObRequest *req = NULL;
            wait_start_time = ObTimeUtility::current_time();
            /// get request from tenant
            {
              ObWaitEventGuard wait_guard(ObWaitEventIds::OMT_IDLE, 0, wait_start_time, 0, 0);
              ret = tenant_->get_new_request(*this, is_level_worker() ? NESTING_REQUEST_WAIT_TIME : REQUEST_WAIT_TIME, req);
              wait_end_time = ObTimeUtility::current_time();
            }
            if (OB_SUCC(ret)) {
              if (OB_NOT_NULL(req)) {
                req_recv_timestamp = req->get_receive_timestamp();
                EVENT_ADD(REQUEST_QUEUE_TIME, wait_end_time - req->get_enqueue_timestamp());
                req->set_push_pop_diff(wait_end_time);
                query_start_time_ = wait_end_time;
                query_enqueue_time_ = req->get_enqueue_timestamp();
                last_check_time_ = wait_end_time;
                set_last_wakeup_ts(query_start_time_);
                set_rpc_stat_srv(&(tenant_->rpc_stat_info_->rpc_stat_srv_));
                process_request(*req);
                query_enqueue_time_ = INT64_MAX;
                query_start_time_ = INT64_MAX;
              } else {
                ret = OB_ERR_UNEXPECTED;
                LOG_ERROR(
                    "got NULL request from tenant",
                    K(tenant_), K(ret), K(req));
              }
            } else if (OB_ENTRY_NOT_EXIST == ret) {
              // timeout while waiting for request from tenant request queue
              ret = OB_SUCCESS;
            }
            IGNORE_RETURN ATOMIC_FAA(&idle_us_, (wait_end_time - wait_start_time));
            if (this->get_worker_level() == 0 && !is_group_worker()) {
              tenant_->check_worker_count(*this);
              tenant_->lq_end(*this);
            } else if (this->is_group_worker()) {
              group_->check_worker_count(*this);
            }
          }
        }
      }
    }
  }
  procor_.th_destroy();
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
  } else if (tenant_->id() >= OB_SERVER_TENANT_ID
             && tenant_->id() <= OB_MAX_RESERVED_TENANT_ID) {
    // do nothing, these tenants don't support large query schedule.
  } else if (this->get_curr_request_level() > 1) {
    // do nothing, level request not support large query retry
  } else if (
      tenant_->user_sched_enabled() &&
      can_retry_ &&
      !large_query()) {
    // if current query is not served by large_query worker (!large_query())
    // evict it back to large query queue
    need_retry_ = true;
    ret = OB_EAGAIN;
  }
  return ret;
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
