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
 *
 * Fetch Log Stream
 */

#define USING_LOG_PREFIX OBLOG_FETCHER

#include "ob_log_ls_fetch_stream.h"
#include "lib/allocator/ob_malloc.h"

#include "lib/container/ob_se_array_iterator.h"   // begin

#include "ob_log_config.h"                        // ObLogFetcherConfig
#include "ob_log_rpc.h"                           // IObLogRpc
#include "ob_ls_worker.h"                         // IObLSWorker
#include "ob_log_part_progress_controller.h"      // PartProgressController
#include "ob_log_trace_id.h"                      // ObLogTraceIdGuard

using namespace oceanbase::common;
using namespace oceanbase::obrpc;

namespace oceanbase
{
namespace logfetcher
{
#define IS_WARN_LOG_LEVEL(arg) \
  if (OB_TIMEOUT == arg \
  || OB_ERR_OUT_OF_LOWER_BOUND == arg \
  || OB_TENANT_NOT_EXIST == arg \
  || OB_TENANT_NOT_IN_SERVER == arg \
  || OB_IN_STOP_STATE == arg \
  || OB_SERVER_IS_INIT == arg) \

int64_t FetchStream::g_rpc_timeout = ObLogFetcherConfig::default_fetch_log_rpc_timeout_sec * _SEC_;
int64_t FetchStream::g_dml_progress_limit = ObLogFetcherConfig::default_progress_limit_sec_for_dml * _SEC_;
int64_t FetchStream::g_ddl_progress_limit = ObLogFetcherConfig::default_progress_limit_sec_for_ddl * _SEC_;
int64_t FetchStream::g_blacklist_survival_time = ObLogFetcherConfig::default_blacklist_survival_time_sec * _SEC_;
int64_t FetchStream::g_check_switch_server_interval = ObLogFetcherConfig::default_check_switch_server_interval_sec * _SEC_;
bool FetchStream::g_print_rpc_handle_info = ObLogFetcherConfig::default_print_rpc_handle_info;
bool FetchStream::g_print_stream_dispatch_info = ObLogFetcherConfig::default_print_stream_dispatch_info;
int64_t FetchStream::g_schedule_time = ObLogFetcherConfig::default_timer_task_wait_time_msec * _MSEC_;

const char *FetchStream::print_state(State state)
{
  const char *str = "UNKNOWN";

  switch (state) {
    case IDLE:
      str = "IDLE";
      break;
    case FETCH_LOG:
      str = "FETCH_LOG";
      break;
    default:
      str = "UNKNOWN";
      break;
  }

  return str;
}

FetchStream::FetchStream() : fetch_log_arpc_(*this)
{
  reset();
}

FetchStream::~FetchStream()
{
  reset();
}

void FetchStream::reset()
{
  is_inited_ = false;
  // Wait for asynchronous RPC to end before clearing data
  fetch_log_arpc_.stop();
  self_tenant_id_ = OB_INVALID_TENANT_ID;

  state_ = State::IDLE;
  stype_ = FETCH_STREAM_TYPE_HOT;
  ls_fetch_ctx_ = nullptr;
  svr_.reset();
  rpc_ = NULL;
  stream_worker_ = NULL;
  rpc_result_pool_ = NULL;
  progress_controller_ = NULL;
  log_handler_ = NULL;

  upper_limit_ = OB_INVALID_TIMESTAMP;
  last_switch_server_tstamp_ = 0;
  fetch_log_arpc_.reset();

  last_stat_time_ = OB_INVALID_TIMESTAMP;
  cur_stat_info_.reset();
  last_stat_info_.reset();

  FSListNode::reset();
}

int FetchStream::init(
    const uint64_t source_tenant_id,
    const uint64_t self_tenant_id,
    LSFetchCtx &ls_fetch_ctx,
    const FetchStreamType stream_type,
    IObLogRpc &rpc,
    IObLSWorker &stream_worker,
    IFetchLogARpcResultPool &rpc_result_pool,
    PartProgressController &progress_controller,
    ILogFetcherHandler &log_handler)
{
  int ret = OB_SUCCESS;
  reset();

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("FetchStream has been inited twice", KR(ret), K(source_tenant_id), K(ls_fetch_ctx), KPC(this));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == source_tenant_id)
      || OB_UNLIKELY(! is_fetch_stream_type_valid(stream_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(source_tenant_id), K(stream_type), K(ls_fetch_ctx), KPC(this));
  } else if (OB_FAIL(fetch_log_arpc_.init(source_tenant_id, self_tenant_id, rpc, stream_worker, rpc_result_pool))) {
    LOG_ERROR("FetchLogARpc init failed", KR(ret), K(source_tenant_id));
  } else {
    ls_fetch_ctx_ = &ls_fetch_ctx;
    self_tenant_id_ = self_tenant_id;
    stype_ = stream_type;
    rpc_ = &rpc;
    stream_worker_ = &stream_worker;
    rpc_result_pool_ = &rpc_result_pool;
    progress_controller_ = &progress_controller;
    log_handler_ = &log_handler;

    is_inited_ = true;
  }

  return ret;
}

int FetchStream::prepare_to_fetch_logs(
    LSFetchCtx &task,
    const common::ObAddr &svr)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("FetchStream has not been inited", KR(ret));
  } else if (OB_UNLIKELY(task.get_fetch_stream_type() != stype_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid LS task, stream type does not match", KR(ret), K(stype_),
        K(task.get_fetch_stream_type()), K(task));
  } else if (OB_ISNULL(stream_worker_)) {
    ret = OB_INVALID_ERROR;
    LOG_ERROR("invalid stream worker", KR(ret), K(stream_worker_));
  // We mush set server before fetch log
  } else if (OB_FAIL(fetch_log_arpc_.set_server(svr))) {
    LOG_ERROR("fetch_log_arpc_ set_server failed", KR(ret), K(svr));
  } else {
    // Note:
    // We should set svr before dispatch_in_fetch_stream
    svr_ = svr;
    // Mark to start fetching logs
    task.dispatch_in_fetch_stream(svr, *this);

    LOG_INFO("[STAT] [FETCH_STREAM] [PREPARE_TO_FETCH]", "fetch_task", &task, "fetch_task", task, K(svr),
        "fetch_stream", this,
        "fetch_stream", *this);

    // For the fetch log stream task, it should be immediately assigned to a worker thread for processing
    if (OB_FAIL(stream_worker_->dispatch_stream_task(*this, "DispatchServer"))) {
      LOG_ERROR("dispatch stream task fail", KR(ret));
    } else {
      // Note: You cannot continue to manipulate this data structure afterwards !
    }
  }

  return ret;
}

int FetchStream::handle(volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  bool print_stream_dispatch_info = ATOMIC_LOAD(&g_print_stream_dispatch_info);
  ClientFetchingMode fetching_mode = ClientFetchingMode::FETCHING_MODE_UNKNOWN;

  if (print_stream_dispatch_info) {
    LOG_INFO("[STAT] [FETCH_STREAM] begin handle", "fetch_stream", this,
        "fetch_stream", *this, "LS_CTX", *ls_fetch_ctx_);
  } else {
    LOG_TRACE("[STAT] [FETCH_STREAM] begin handle", "fetch_stream", this,
        "fetch_stream", *this, "LS_CTX", *ls_fetch_ctx_);
  }
  if (OB_ISNULL(ls_fetch_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ls fetch ctx is invalid in fetchstream", KR(ret), KPC(this));
  } else if (ls_fetch_ctx_->is_discarded()) {
    // Copy the structure to avoid recycling
    const logservice::TenantLSID tls_id = ls_fetch_ctx_->get_tls_id();
    KickOutInfo kick_out_info;
    kick_out_info.kick_out_reason_ = KickOutReason::DISCARDED;

    if (OB_FAIL(kick_out_task_(kick_out_info))) {
      LOG_ERROR("kick out task failed", KR(ret), K(kick_out_info));
    } else {
      LOG_INFO("LSFetchCtx is discarded, dispatch fetch task success", K(tls_id));
    }
  } else if (FALSE_IT(fetching_mode = ls_fetch_ctx_->get_fetching_mode())) {
  } else if (is_integrated_fetching_mode(fetching_mode)) {
    if (IDLE == state_) {
      if (OB_FAIL(handle_idle_task_(stop_flag))) {
        if (OB_IN_STOP_STATE != ret) {
          LOG_ERROR("handle IDLE task fail", KR(ret));
        }
      }
    } else if (FETCH_LOG == state_) {
      if (OB_FAIL(handle_fetch_log_task_(stop_flag))) {
        if (OB_IN_STOP_STATE != ret) {
          LOG_ERROR("handle FETCH_LOG task fail", KR(ret));
        }
      }
    } else {
      ret = OB_INVALID_ERROR;
      LOG_ERROR("invalid state", KR(ret), K(state_));
    }
  } else if (is_direct_fetching_mode(fetching_mode)) {
    if (OB_FAIL(handle_fetch_archive_task_(stop_flag))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("handle fetch archive task failed", KR(ret));
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid fetching mode", KR(ret), K(fetching_mode));
  }

  // Note: The following can no longer continue the operation, there may be concurrency issues ！
  return ret;
}

#ifdef ERRSIM
ERRSIM_POINT_DEF(FAILED_TO_SCHEDULE_FETCH_STREAM);
#endif

int FetchStream::schedule(int timer_id)
{
  int ret = OB_SUCCESS;
#ifdef ERRSIM
  if (OB_FAIL(FAILED_TO_SCHEDULE_FETCH_STREAM)) {
    LOG_ERROR("ERRSIM: failed to schedule fetch stream");
  } else {
#endif
  ret = TG_SCHEDULE(timer_id, *this, g_schedule_time, false);
#ifdef ERRSIM
  }
#endif
  return ret;
}

// The purpose of a timed task is to assign itself to a worker thread
void FetchStream::runTimerTask()
{
  int ret = OB_SUCCESS;
  static int64_t max_dispatch_time = 0;
  int64_t start_time = get_timestamp();
  int64_t end_time = 0;

  LOG_TRACE("[STAT] [WAKE_UP_STREAM_TASK]", "task", this, "task", *this);

  if (OB_ISNULL(stream_worker_)) {
    LOG_ERROR("invalid stream worker", K(stream_worker_));
    ret = OB_INVALID_ERROR;
  // should never fail
  } else if (OB_FAIL(stream_worker_->dispatch_stream_task(*this, "TimerWakeUp", true))) {
    LOG_ERROR("dispatch stream task fail", KR(ret), K(this));
  } else {
    ATOMIC_STORE(&end_time, get_timestamp());
    max_dispatch_time = std::max(max_dispatch_time, ATOMIC_LOAD(&end_time) - start_time);

    if (REACH_TIME_INTERVAL_THREAD_LOCAL(STAT_INTERVAL)) {
      LOG_INFO("[STAT] [FETCH_STREAM_TIMER_TASK]", K(max_dispatch_time));
    }
  }
}

void FetchStream::configure(const ObLogFetcherConfig &config)
{
  int64_t fetch_log_rpc_timeout_sec = config.fetch_log_rpc_timeout_sec;
  int64_t dml_progress_limit_sec = config.progress_limit_sec_for_dml;
  int64_t ddl_progress_limit_sec = config.progress_limit_sec_for_ddl;
  int64_t blacklist_survival_time_sec = config.blacklist_survival_time_sec;
  int64_t check_switch_server_interval_sec = config.check_switch_server_interval_sec;
  bool print_rpc_handle_info = config.print_rpc_handle_info;
  bool print_stream_dispatch_info = config.print_stream_dispatch_info;
  int64_t timer_task_wait_time_msec = config.timer_task_wait_time_msec;

  ATOMIC_STORE(&g_rpc_timeout, fetch_log_rpc_timeout_sec * _SEC_);
  LOG_INFO("[CONFIG]", K(fetch_log_rpc_timeout_sec));
  ATOMIC_STORE(&g_dml_progress_limit, dml_progress_limit_sec * _SEC_);
  LOG_INFO("[CONFIG]", K(dml_progress_limit_sec));
  ATOMIC_STORE(&g_ddl_progress_limit, ddl_progress_limit_sec * _SEC_);
  LOG_INFO("[CONFIG]", K(ddl_progress_limit_sec));
  ATOMIC_STORE(&g_blacklist_survival_time, blacklist_survival_time_sec * _SEC_);
  LOG_INFO("[CONFIG]", K(blacklist_survival_time_sec));
  ATOMIC_STORE(&g_check_switch_server_interval, check_switch_server_interval_sec * _SEC_);
  LOG_INFO("[CONFIG]", K(check_switch_server_interval_sec));
  ATOMIC_STORE(&g_print_rpc_handle_info, print_rpc_handle_info);
  LOG_INFO("[CONFIG]", K(print_rpc_handle_info));
  ATOMIC_STORE(&g_print_stream_dispatch_info, print_stream_dispatch_info);
  LOG_INFO("[CONFIG]", K(print_stream_dispatch_info));
  ATOMIC_STORE(&g_schedule_time, timer_task_wait_time_msec * _MSEC_);
  LOG_INFO("[CONFIG]", K(timer_task_wait_time_msec));
}

void FetchStream::do_stat(int64_t &traffic)
{
  ObByteLockGuard lock_guard(stat_lock_);

  int ret = OB_SUCCESS;
  int64_t cur_time = get_timestamp();
  int64_t delta_time = cur_time - last_stat_time_;
  double delta_second = static_cast<double>(delta_time) / static_cast<double>(_SEC_);

  if (last_stat_time_ <= 0) {
    last_stat_time_ = cur_time;
    last_stat_info_ = cur_stat_info_;
  } else if (delta_second <= 0) {
    // Statistics are too frequent, ignore the statistics here, otherwise the following will lead to divide by zero error
    LOG_TRACE("fetch stream stat too frequently", K(delta_time), K(delta_second),
        K(last_stat_time_), K(this));
  } else {
    FetchStatInfoPrinter fsi_printer(cur_stat_info_, last_stat_info_, delta_second);

    if (nullptr != ls_fetch_ctx_) {
      _LOG_INFO("[STAT] [FETCH_STREAM] stream=%s(%p:%s)(%s)(FETCHED_LOG:%s) %s", to_cstring(svr_), this,
          print_fetch_stream_type(stype_),
          to_cstring(ls_fetch_ctx_->get_tls_id()),
          SIZE_TO_STR(ls_fetch_ctx_->get_fetched_log_size()),
          to_cstring(fsi_printer));
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("ls_fetch_ctx_ is NULL", KR(ret), "fs", *this);
    }
    traffic = fsi_printer.get_traffic();
    last_stat_time_ = cur_time;
    last_stat_info_ = cur_stat_info_;
  }
}

void FetchStream::handle_when_leave_(const char *leave_reason) const
{
  // Note: This function only prints logs and cannot access any data members, except global members
  // Because of the multi-threaded problem
  bool print_stream_dispatch_info = ATOMIC_LOAD(&g_print_stream_dispatch_info);
  if (print_stream_dispatch_info) {
    // No data members can be accessed in when print log, only the address is printed
    LOG_INFO("[STAT] [FETCH_STREAM] leave stream", "fetch_stream", this, K(leave_reason));
  } else {
    LOG_TRACE("[STAT] [FETCH_STREAM] leave stream", "fetch_stream", this, K(leave_reason));
  }
}

int FetchStream::handle_idle_task_(volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(IDLE != state_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_ERROR("state does not match IDLE", KR(ret), K(state_));
  } else if (OB_ISNULL(ls_fetch_ctx_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("ls_fetch_ctx_ is NULL", KR(ret), K(ls_fetch_ctx_));
  } else if (! ls_fetch_ctx_->is_in_fetching_log()) {
    handle_when_leave_("LSNotFetchLogState");
  } else {
    const char *discard_reason = "HandleIdle";

    // First discard the old request
    fetch_log_arpc_.discard_request(discard_reason);

    // First ready to fetch log by asynchronous RPC request
    if (OB_FAIL(prepare_rpc_request_())) {
      LOG_ERROR("prepare rpc request fail", KR(ret));
    } else {
      bool need_fetch_log = false;

      // Update upper limit, prepare for fetching logs
      if (OB_FAIL(check_need_fetch_log_with_upper_limit_(need_fetch_log))) {
        LOG_ERROR("check need fetch log with upper limit failed", KR(ret));
      } else if (! need_fetch_log) {
        // If you don't need to fetch the log, you will go into hibernation
        // No further manipulation of the data structure !!!!
        if (OB_FAIL(hibernate_())) {
          LOG_ERROR("hibernate fail", KR(ret));
        }
      } else {
        // Go to fetch log status
        switch_state(FETCH_LOG);

        // launch an asynchronous fetch log RPC
        bool rpc_send_succeed = false;
        const palf::LSN &next_lsn = ls_fetch_ctx_->get_next_lsn();

        if (OB_FAIL(async_fetch_log_(next_lsn, rpc_send_succeed))) {
          LOG_ERROR("async fetch log fail", KR(ret));
        } else if (rpc_send_succeed) {
          // Asynchronous fetch log RPC success, wait for RPC callback, after that can not continue to manipulate any data structure
          // Note: You cannot continue to manipulate any data structures afterwards !!!!!
          handle_when_leave_("AsyncRpcSendSucc");
        } else {
          // RPC failure, directly into the FETCH_LOG processing process
          // Note: You cannot continue to manipulate any data structures afterwards !!!!!
          ret = handle(stop_flag);
        }
      }
    }
  }

  return ret;
}

int FetchStream::dispatch_fetch_task_(LSFetchCtx &task,
    KickOutReason dispatch_reason)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(stream_worker_)) {
    LOG_ERROR("invalid stream worker", K(stream_worker_));
    ret = OB_INVALID_ERROR;
  } else if (OB_ISNULL(ls_fetch_ctx_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("ls_fetch_ctx_ is NULL", KR(ret), K(ls_fetch_ctx_));
  } else {
    const ClientFetchingMode fetching_mode = ls_fetch_ctx_->get_fetching_mode();
    // The server is not blacklisted when the stream is actively switch and when the partition is discarded, but is blacklisted in all other cases.
    if (is_integrated_fetching_mode(fetching_mode) && need_add_into_blacklist_(dispatch_reason)) {
      // Get the total time of the current partition of the server service at this time
      int64_t svr_start_fetch_tstamp = OB_INVALID_TIMESTAMP;

      if (OB_FAIL(task.get_cur_svr_start_fetch_tstamp(svr_, svr_start_fetch_tstamp))) {
        LOG_ERROR("get_cur_svr_start_fetch_tstamp fail", KR(ret), "tls_id", task.get_tls_id(),
            K_(svr), K(svr_start_fetch_tstamp));
      } else {
        int64_t svr_service_time = get_timestamp() - svr_start_fetch_tstamp;
        int64_t cur_survival_time = ATOMIC_LOAD(&g_blacklist_survival_time);
        int64_t survival_time = cur_survival_time;
        // Server add into blacklist
        if (OB_FAIL(task.add_into_blacklist(svr_, svr_service_time, survival_time))) {
          LOG_ERROR("task add into blacklist fail", KR(ret), K(task), K_(svr),
              "svr_service_time", TVAL_TO_STR(svr_service_time),
              "survival_time", TVAL_TO_STR(survival_time));
        }
      }
    }

    if (OB_SUCCESS == ret) {
      const char *dispatch_reason_str = print_switch_reason(dispatch_reason);

      // Note:
      // We must set_not_in_fetching_log state before dispatch_fetch_task, because when LsFetchCtx is not
      // fetchStream module, eg, IdlePool, LsFetchCtx cannot try to fetch log.
      // Reference: handle_idle_task_() - is_in_fetching_log()
      ls_fetch_ctx_->set_not_in_fetching_log();

      if (OB_FAIL(stream_worker_->dispatch_fetch_task(task, dispatch_reason_str))) {
        // Assignment of fetch log tasks
        LOG_ERROR("dispatch fetch task fail", KR(ret), K(task),
                  "dispatch_reason", dispatch_reason_str);
      } else {
        // You cannot continue with the task afterwards
      }
    }
  }

  return ret;
}

int FetchStream::get_upper_limit(int64_t &upper_limit_ns)
{
  int ret = OB_SUCCESS;
  int64_t min_progress = OB_INVALID_TIMESTAMP;
  int64_t global_upper_limit = OB_INVALID_TIMESTAMP;

  if (OB_ISNULL(progress_controller_)) {
    LOG_ERROR("invalid progress controller", K(progress_controller_));
    ret = OB_INVALID_ERROR;
  }
  //  Get global minimum progress
  else if (OB_FAIL(progress_controller_->get_min_progress(min_progress))) {
    LOG_ERROR("get_min_progress fail", KR(ret), KPC(progress_controller_));
  } else if (OB_UNLIKELY(OB_INVALID_TIMESTAMP == min_progress)) {
    ret = OB_INVALID_ERROR;
    LOG_ERROR("current min progress is invalid", KR(ret), K(min_progress), KPC(progress_controller_));
  } else {
    // DDL partition is not limited by progress limit, here upper limit is set to a future value
    if (FETCH_STREAM_TYPE_SYS_LS == stype_) {
      upper_limit_ns = min_progress + ATOMIC_LOAD(&g_ddl_progress_limit) * NS_CONVERSION;
    } else {
      // Other partition are limited by progress limit
      upper_limit_ns = min_progress + ATOMIC_LOAD(&g_dml_progress_limit) * NS_CONVERSION;
    }

    global_upper_limit = progress_controller_->get_global_upper_limit();
    if (OB_INVALID_TIMESTAMP != global_upper_limit) {
      const int64_t log_progress = ls_fetch_ctx_->get_progress();
      if (log_progress < global_upper_limit) {
        upper_limit_ns = INT64_MAX - 1;
      } else {
        upper_limit_ns = std::min(upper_limit_ns, global_upper_limit);
      }
    }
  }

  return ret;
}

int FetchStream::check_need_fetch_log_(const int64_t limit, bool &need_fetch_log)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(limit <= 0)) {
    LOG_ERROR("invalid upper limit", K(limit));
    ret = OB_INVALID_ARGUMENT;
  } else {
    LSFetchCtx *task = ls_fetch_ctx_;

    need_fetch_log = false;

    // Iterate through all tasks, as long as there is a task less than upper limit, then you need to continue to fetch logs
    if (NULL != task) {
      int64_t part_progress = task->get_progress();
      if (OB_UNLIKELY(OB_INVALID_TIMESTAMP == part_progress)) {
        LOG_ERROR("fetch task progress is invalid", K(part_progress), KPC(task));
        ret = OB_ERR_UNEXPECTED;
      } else {
        need_fetch_log = (part_progress < limit);
      }
    }
  }

  return ret;
}

int FetchStream::check_need_fetch_log_with_upper_limit_(bool &need_fetch_log)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_upper_limit(upper_limit_))) {
    LOG_ERROR("get upper limit failed", KR(ret), "tls_id", ls_fetch_ctx_->get_tls_id());
  } else if (OB_FAIL(check_need_fetch_log_(upper_limit_, need_fetch_log))) {
    LOG_ERROR("check need fetch log failed", KR(ret), K(upper_limit_));
  }
  return ret;
}

int FetchStream::hibernate_()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(stream_worker_)) {
    LOG_ERROR("invalid stream worker", K(stream_worker_));
    ret = OB_INVALID_ERROR;
  } else if (OB_FAIL(stream_worker_->hibernate_stream_task(*this, "FetchStream"))) {
    LOG_ERROR("hibernate_stream_task fail", KR(ret));
  } else {
    // Note: You can't continue to manipulate the structure after that, there are concurrency issues!!!
    handle_when_leave_("Hibernate");
  }

  return ret;
}

int FetchStream::prepare_rpc_request_()
{
  int ret = OB_SUCCESS;

  // TODO: Currently, every time a RPC request is prepared, the default value is used, find a way to optimize
  int64_t rpc_timeout = ATOMIC_LOAD(&g_rpc_timeout);

  if (OB_ISNULL(ls_fetch_ctx_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("ls_fetch_ctx_ is NULL", KR(ret), K(ls_fetch_ctx_));
  } else {
    const logservice::TenantLSID &tls_id = ls_fetch_ctx_->get_tls_id();

    if (OB_FAIL(fetch_log_arpc_.prepare_request(tls_id.get_ls_id(), rpc_timeout))) {
      LOG_ERROR("prepare request for rpc fail", KR(ret), K(rpc_timeout));
    }
  }

  return ret;
}

int FetchStream::async_fetch_log_(
    const palf::LSN &req_start_lsn,
    bool &rpc_send_succeed)
{
  int ret = OB_SUCCESS;
  const int64_t client_progress = ls_fetch_ctx_->get_progress();

  rpc_send_succeed = false;

  // Launch an asynchronous RPC
  if (OB_FAIL(fetch_log_arpc_.async_fetch_log(req_start_lsn, client_progress, upper_limit_, rpc_send_succeed))) {
    LOG_ERROR("async_fetch_log fail", KR(ret), K(req_start_lsn), K(upper_limit_), K(fetch_log_arpc_));
  } else {
    // Asynchronous RPC execution succeeded
    // Note: You cannot continue to manipulate any data structures afterwards !!!!
  }

  return ret;
}

void FetchStream::print_handle_info_(
    FetchLogARpcResult &result,
    const int64_t handle_rpc_time,
    const int64_t read_log_time,
    const int64_t decode_log_entry_time,
    const bool rpc_is_flying,
    const bool is_stream_valid,
    const char *stream_invalid_reason,
    const KickOutInfo &kickout_info,
    const TransStatInfo &tsi,
    const bool need_stop_request)
{
  bool print_rpc_handle_info = ATOMIC_LOAD(&g_print_rpc_handle_info);
  LSFetchCtx::LSProgress min_progress;
  logservice::TenantLSID min_tls_id;

  if (print_rpc_handle_info) {
    LOG_INFO("handle rpc result by fetch stream",
        "fetch_stream", this,
        "upper_limit", NTS_TO_STR(upper_limit_),
        K(need_stop_request),
        "rpc_stop_upon_result", result.rpc_stop_upon_result_,
        "rpc_stop_reason", FetchLogARpc::print_rpc_stop_reason(result.rpc_stop_reason_),
        K(rpc_is_flying), K(is_stream_valid), K(stream_invalid_reason), K(kickout_info),
        "resp", result.resp_, K(handle_rpc_time), K(read_log_time), K(decode_log_entry_time),
        K(tsi), K(min_progress), K(min_tls_id));
  } else {
    LOG_TRACE("handle rpc result by fetch stream",
        "fetch_stream", this,
        "upper_limit", NTS_TO_STR(upper_limit_),
        K(need_stop_request),
        "rpc_stop_upon_result", result.rpc_stop_upon_result_,
        "rpc_stop_reason", FetchLogARpc::print_rpc_stop_reason(result.rpc_stop_reason_),
        K(rpc_is_flying), K(is_stream_valid), K(stream_invalid_reason), K(kickout_info),
        "resp", result.resp_, K(handle_rpc_time), K(read_log_time), K(decode_log_entry_time),
        K(tsi), K(min_progress), K(min_tls_id));
  }
}

bool FetchStream::has_new_fetch_task_() const
{
  // If the queue of the fetch log task pool is not empty, it marks that there is a new task to be processed
  return false;
}

int FetchStream::process_result_(
    FetchLogARpcResult &result,
    volatile bool &stop_flag,
    const bool rpc_is_flying,
    KickOutInfo &kickout_info,
    bool &need_hibernate,
    bool &is_stream_valid)
{
  int ret = OB_SUCCESS;
  int64_t start_handle_time = get_timestamp();
  int64_t handle_rpc_time = 0;
  int64_t read_log_time = 0;
  int64_t decode_log_entry_time = 0;
  int64_t flush_time = 0;
  TransStatInfo tsi;
  bool need_stop_request = false;
  const char *stream_invalid_reason = NULL;

  // Process each result, set the corresponding trace id
  ObLogTraceIdGuard trace_guard(result.trace_id_);

  // Process the log results and make appropriate decisions based on the results
  if (OB_FAIL(handle_fetch_log_result_(result,
      stop_flag,
      is_stream_valid,
      stream_invalid_reason,
      kickout_info,
      need_hibernate,
      read_log_time,
      decode_log_entry_time,
      tsi,
      flush_time))) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("handle fetch log result fail", KR(ret), K(result), K(kickout_info), K(fetch_log_arpc_));
    }
  }
  // If the current fetching stream is invalid, you need to discard the request
  else if (! is_stream_valid) {
    // If the current fetch log stream is invalid, you need to reopen the stream
    fetch_log_arpc_.discard_request(stream_invalid_reason, is_stream_valid/*is_normal_discard*/);
  }
  // The log stream is valid and ready to continue processing the next RPC packet
  else {
    // If a new LS task comes in, notify RPC to stop continuing to fetch logs
    // Avoid starvation of new LS
    need_stop_request = (rpc_is_flying && has_new_fetch_task_());

    // Mark the request as finished
    // After you stop the request, you still need to continue iterating through the results until all the results are iterated through
    if (need_stop_request && (OB_FAIL(fetch_log_arpc_.mark_request_stop()))) {
      LOG_ERROR("fetch log rpc mar request stop fail", KR(ret), K(this),
          K(fetch_log_arpc_));
    }
    // Update RPC request parameters
    else if (OB_FAIL(update_rpc_request_params_())) {
      LOG_ERROR("update rpc request params fail", KR(ret));
    } else {
      // success
    }
  }

  if (OB_SUCCESS == ret) {
    handle_rpc_time = get_timestamp() - start_handle_time;

    // Update statistical information
    update_fetch_stat_info_(result, handle_rpc_time, read_log_time,
        decode_log_entry_time, flush_time, tsi);

    // Print processing information
    print_handle_info_(result, handle_rpc_time, read_log_time, decode_log_entry_time,
        rpc_is_flying, is_stream_valid, stream_invalid_reason, kickout_info,
        tsi, need_stop_request);
  }

  return ret;
}

int FetchStream::handle_fetch_log_task_(volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(ls_fetch_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid ls_fetch_ctx_ while handle_fetch_log_task_", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(FETCH_LOG != state_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_ERROR("state does not match which is not FETCH_LOG", KR(ret), K(state_));
  } else {
    bool need_hibernate = false;
    bool rpc_is_flying = false;
    bool is_stream_valid = true;
    FetchLogARpcResult *result = NULL;
    KickOutInfo kickout_info;

    // Whether the log stream is taken over by RPC, default is false
    bool stream_been_taken_over_by_rpc = false;

    // Continuously iterate through the fetch log results while the current fetch log stream is continuously active, and then process
    while (! stop_flag
        && OB_SUCC(ret)
        && is_stream_valid
        && OB_SUCC(fetch_log_arpc_.next_result(result, rpc_is_flying))) {
      need_hibernate = false;

      if (OB_ISNULL(result)) {
        LOG_ERROR("invalid result", K(result));
        ret = OB_INVALID_ERROR;
      }
      // Processing results
      else if (OB_FAIL(process_result_(
          *result,
          stop_flag,
          rpc_is_flying,
          kickout_info,
          need_hibernate,
          is_stream_valid))) {
        if (OB_IN_STOP_STATE != ret) {
          LOG_ERROR("process result fail", KR(ret), K(result), KPC(result), K(kickout_info), K(this), KPC(this));
        }
      } else {
        // Processing success
      }

      // Recycling result
      if (NULL != result) {
        fetch_log_arpc_.revert_result(result);
        result = NULL;
      }
    } // while

    if (stop_flag) {
      ret = OB_IN_STOP_STATE;
    }

    if (OB_ITER_END == ret) {
      // Iterate through all the results
      ret = OB_SUCCESS;

      if (rpc_is_flying) {
        // The RPC is still running, the fetch log stream is taken over by the RPC callback thread
        // Note: No further manipulation of any data structures can be performed subsequently
        stream_been_taken_over_by_rpc = true;
      } else {
        // The RPC is not running, it is still the current thread that is responsible for that fetch log stream
        stream_been_taken_over_by_rpc = false;
      }
    }

    // Fetching missing log RPC failed, need retry
    if (OB_NEED_RETRY == ret) {
      ret = OB_SUCCESS;
    }

    // Final unified processing results
    if (OB_SUCCESS == ret) {
      if (stream_been_taken_over_by_rpc) {
        // The fetch log stream is taken over by the RPC callback, maintains the FETCH_LOG state, and exits unconditionally
        // Note: You cannot continue to manipulate any data structures afterwards !!!!!
        handle_when_leave_("RpcTakeOver");
      } else {
        // The current thread is still responsible for this fetch log stream
        // Entering IDLE state
        switch_state(IDLE);

        // kickout task if need_kick_out
        if (kickout_info.need_kick_out()) {
          if (OB_FAIL(kick_out_task_(kickout_info))) {
            LOG_ERROR("kick_out_task_ failed", KR(ret), K(kickout_info), K(result), KPC(result), K(this), KPC(this));
          }
        // Hibernate the task if it needs to be hibernated
        // Note: No more data structures can be accessed afterwards, there is a concurrency scenario !!!!
        } else if (need_hibernate) {
          if (OB_FAIL(hibernate_())) {
            LOG_ERROR("hibernate fail", KR(ret));
          }
        } else {
          // No hibernation required, then recursive processing of IDLE tasks
          // Note: no more data structures can be accessed afterwards, there is a concurrency scenario !!!!
          // TODO note
          ret = handle(stop_flag);
        }
      }
    }
  }

  return ret;
}

int FetchStream::read_group_entry_(
    const palf::LogGroupEntry &group_entry,
    const palf::LSN &group_start_lsn,
    const char *buffer,
    KickOutInfo &kick_out_info,
    TransStatInfo &tsi,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  ObTaskId trace_id(*ObCurTraceId::get_trace_id());

  if (OB_ISNULL(ls_fetch_ctx_) || OB_ISNULL(log_handler_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ls_fetch_ctx_ or log_handler_ is nullptr, unexpected", KR(ret), K(ls_fetch_ctx_), K(log_handler_));
  } else {
    const uint64_t tenant_id = ls_fetch_ctx_->get_tls_id().get_tenant_id();
    const share::ObLSID &ls_id = ls_fetch_ctx_->get_tls_id().get_ls_id();
    const int64_t proposal_id = ls_fetch_ctx_->get_proposal_id();

    if (OB_FAIL(log_handler_->handle_group_entry(tenant_id, ls_id, proposal_id, group_start_lsn, group_entry, buffer,
            static_cast<void *>(ls_fetch_ctx_),
            kick_out_info, tsi, stop_flag))) {
      if (OB_NEED_RETRY == ret) {
        LOG_INFO("LogHander handle_group_entry failed, need_retry", KR(ret), K(tenant_id), K(ls_id), K(proposal_id),
            K(group_start_lsn), K(group_entry));
      } else if (OB_IN_STOP_STATE == ret) {
        LOG_INFO("LogHander handle_group_entry is stopped", KR(ret), K(tenant_id), K(ls_id), K(proposal_id),
            K(group_start_lsn), K(group_entry));
      } else {
        LOG_ERROR("LogHander handle_group_entry failed", KR(ret), K(tenant_id), K(ls_id), K(proposal_id),
            K(group_start_lsn), K(group_entry));
        if (OB_NOT_NULL(ls_fetch_ctx_)) {
          ls_fetch_ctx_->handle_error(ls_id, IObLogErrHandler::ErrType::SUBMIT_LOG, trace_id, group_start_lsn, ret, "%s");
        }
      }
    }
  }

  return ret;
}

void FetchStream::update_fetch_stat_info_(
    const int64_t fetch_log_cnt,
    const int64_t fetch_log_size,
    const int64_t fetch_and_read_time,
    const int64_t fetch_log_time,
    const int64_t flush_time,
    const TransStatInfo &tsi)
{
  ObByteLockGuard lock_guard(stat_lock_);

  FetchStatInfo &fsi = cur_stat_info_;
  fsi.fetch_log_rpc_cnt_++;
  fsi.fetch_log_cnt_ += fetch_log_cnt;
  fsi.fetch_log_size_ += fetch_log_size;
  fsi.handle_rpc_read_log_time_ += fetch_and_read_time - fetch_log_time;
  fsi.handle_rpc_flush_time_ += flush_time;
  fsi.fetch_log_rpc_time_ += fetch_log_time;
  fsi.tsi_.update(tsi);
}

int FetchStream::handle_fetch_archive_task_(volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  static const int64_t UPDATE_FETCH_STATE_INTERVAL = 100;
  bool need_fetch_log = true;
  LOG_TRACE("handle_fetch_archive_task_ begin", K(svr_), "tls_id", ls_fetch_ctx_->get_tls_id());

  if (OB_ISNULL(ls_fetch_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ls fetch ctx is null", KR(ret), KPC(this));
  } else if (! is_direct_fetching_mode(ls_fetch_ctx_->get_fetching_mode())) {
    const ClientFetchingMode mode = ls_fetch_ctx_->get_fetching_mode();
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("fetching mode of ls fetch ctx doesn't match", KR(ret), K(mode), "tls_id", ls_fetch_ctx_->get_tls_id());
  } else if (OB_FAIL(check_need_fetch_log_with_upper_limit_(need_fetch_log))) {
    LOG_ERROR("get upper limit failed", KR(ret), KPC(this), K(need_fetch_log), "tls_id", ls_fetch_ctx_->get_tls_id());
  } else if (! need_fetch_log) {
    if (OB_FAIL(hibernate_())) {
      LOG_ERROR("hibernate_ failed", KR(ret), KPC(this));
    } else {
      // exit, not allowed to manipulate ls_fetch_ctx afterwards
    }
  } else {
    KickOutInfo kick_out_info;
    TransStatInfo tsi;
    const logservice::TenantLSID &tls_id = ls_fetch_ctx_->get_tls_id();
    int64_t fetched_group_entry_cnt = 0;
    int64_t fetched_group_entry_size = 0;
    int64_t start_handle_timestamp = get_timestamp();
    int64_t start_fetch_remote_timestamp = OB_INVALID_TIMESTAMP;
    int64_t fetch_remote_time = 0;

    while (OB_SUCC(ret) && need_fetch_log) {
      palf::LogGroupEntry log_group_entry;
      palf::LSN lsn;
      const char *buf = NULL;
      int64_t buf_size = 0;
      start_fetch_remote_timestamp = get_timestamp();

      if (! ls_fetch_ctx_->is_remote_iter_inited() && OB_FAIL(ls_fetch_ctx_->init_remote_iter())) {
        LOG_ERROR("init remote iter when handle fetch archive task failed", KR(ret), KPC(ls_fetch_ctx_));
      } else if (OB_FAIL(ls_fetch_ctx_->get_next_remote_group_entry(log_group_entry,
          lsn, buf, buf_size))) {
        // reset iter on OB_ITER_END because remote_iter become invalid when meet OB_ITER_END
        if (OB_INVALID_DATA == ret) {
          LOG_WARN("get invalid data, retry", KR(ret), KPC(ls_fetch_ctx_));
        } else if (OB_ITER_END != ret && OB_NEED_RETRY != ret) {
          LOG_ERROR("get next group entry failed", KR(ret), KPC(ls_fetch_ctx_));
        } else if (OB_NEED_RETRY == ret) {
          int tmp_ret = OB_SUCCESS;
          const logservice::TenantLSID &tls_id = ls_fetch_ctx_->get_tls_id();
          if (OB_TMP_FAIL(set_(kick_out_info, tls_id, KickOutReason::FETCH_LOG_FAIL_IN_DIRECT_MODE))) {
            LOG_WARN("set kickout info failed", KR(tmp_ret), K(kick_out_info), K(tls_id));
          }
        }
        // retry on fetch remote log failure anyway
        // for all scenario above, no need to fetch log and need to reset remote iterator.
        need_fetch_log = false;
        ls_fetch_ctx_->reset_remote_iter();
        ret = OB_SUCCESS;
      } else if (FALSE_IT(fetch_remote_time += get_timestamp() - start_fetch_remote_timestamp)) {
      } else if (OB_FAIL(ls_fetch_ctx_->append_log(buf, buf_size))) {
        LOG_ERROR("append log failed", KR(ret), K(buf), K(buf_size));
      } else if (OB_FAIL(read_group_entry_(log_group_entry, lsn, buf, kick_out_info, tsi, stop_flag))) {
        if (OB_IN_STOP_STATE != ret && OB_NEED_RETRY != ret) {
          LOG_ERROR("read group entry failed when handling fetch archive task", KR(ret), K(log_group_entry),
              K(lsn), K(kick_out_info), KPC(ls_fetch_ctx_));
        } else if (OB_NEED_RETRY == ret) {
          LOG_WARN("read_group_entry failed, retry", KR(ret), K(log_group_entry), K(lsn), K(tls_id));
          // reset remote iter to fetch log that match the next_lsn in progress next round,
          // otherwise incorrect log may be fetched.
          ls_fetch_ctx_->reset_remote_iter();
          // reset memory storage to prevent the remain logentry in mem_storage from
          // disruppting the iteration of log group entry.
          ls_fetch_ctx_->reset_memory_storage();
          need_fetch_log = false;
          ret = OB_SUCCESS;
        }
      } else if (OB_FAIL(ls_fetch_ctx_->update_progress(log_group_entry, lsn))) {
        LOG_ERROR("ls fetch ctx update progress failed", KR(ret), K(log_group_entry), K(lsn), K(tls_id));
      }

      // dispatch part trans task & update stats
      if (OB_SUCC(ret)) {
        const int64_t submit_ts = log_group_entry.get_scn().get_val_for_logservice();

        if (submit_ts > upper_limit_) {
          check_need_fetch_log_with_upper_limit_(need_fetch_log);
        }
        fetched_group_entry_size += log_group_entry.get_serialize_size();
        // update fetch state every 100 group entries
        if ((++fetched_group_entry_cnt % UPDATE_FETCH_STATE_INTERVAL) == 0) {
          int64_t flush_time = 0;
          const int64_t read_log_time = get_timestamp() - start_handle_timestamp;

          if (OB_FAIL(update_fetch_task_state_(kick_out_info, stop_flag, flush_time))) {
            LOG_ERROR("update fetch task state failed", KR(ret), K(kick_out_info), K(tls_id));
          } else {
            update_fetch_stat_info_(fetched_group_entry_cnt, fetched_group_entry_size,
              read_log_time, fetch_remote_time, flush_time, tsi);
            if (kick_out_info.need_kick_out()) {
              need_fetch_log = false;
            }
          }
          fetch_remote_time = 0;
          fetched_group_entry_cnt = 0;
          fetched_group_entry_size = 0;
          tsi.reset();
          start_handle_timestamp = get_timestamp();
        }
      }
    }

    // when exit from loop, there could still be some fetch tasks to be synchronized
    if (OB_SUCC(ret)) {
      int64_t flush_time = 0;
      const int64_t read_log_time = get_timestamp() - start_handle_timestamp;
      if (OB_FAIL(update_fetch_task_state_(kick_out_info, stop_flag, flush_time))) {
        LOG_ERROR("update fetch task state failed at loop end", KR(ret), K(kick_out_info));
      } else {
        update_fetch_stat_info_(fetched_group_entry_cnt, fetched_group_entry_size,
            read_log_time, fetch_remote_time, flush_time, tsi);
      }
    }

    // rewrite ret code when ret equals OB_NEED_RETRY.
    if (OB_NEED_RETRY == ret) {
      ret = OB_SUCCESS;
    }

    if (OB_SUCC(ret)) {
      if (kick_out_info.need_kick_out()) {
        if (OB_FAIL(kick_out_task_(kick_out_info))) {
          LOG_ERROR("kick out task failed", KR(ret), K(kick_out_info));
        }
      } else {
        if (OB_FAIL(hibernate_())) {
          LOG_ERROR("hibernate failed", KR(ret));
        }
      }
    }
  }

  return ret;
}

void FetchStream::update_fetch_stat_info_(
    FetchLogARpcResult &result,
    const int64_t handle_rpc_time,
    const int64_t read_log_time,
    const int64_t decode_log_entry_time,
    const int64_t flush_time,
    const TransStatInfo &tsi)
{
  ObByteLockGuard lock_guard(stat_lock_);

  FetchStatInfo &fsi = cur_stat_info_;
  const ObRpcResultCode &rcode = result.rcode_;
  const ObCdcLSFetchLogResp &resp = result.resp_;
  const ObCdcFetchStatus &fetch_status = resp.get_fetch_status();

  // No statistics on failed RPCs
  if (OB_SUCCESS == rcode.rcode_ && OB_SUCCESS == resp.get_err()) {
    fsi.fetch_log_cnt_ += resp.get_log_num();
    fsi.fetch_log_size_ += resp.get_pos();

    fsi.fetch_log_rpc_cnt_++;
    fsi.fetch_log_rpc_time_ += result.rpc_time_;
    fsi.fetch_log_rpc_to_svr_net_time_ += fetch_status.l2s_net_time_;
    fsi.fetch_log_rpc_svr_queue_time_ += fetch_status.svr_queue_time_;
    fsi.fetch_log_rpc_svr_process_time_ += fetch_status.ext_process_time_;
    fsi.fetch_log_rpc_callback_time_ += result.rpc_callback_time_;
    fsi.handle_rpc_time_ += handle_rpc_time;
    fsi.handle_rpc_read_log_time_ += read_log_time;
    fsi.handle_rpc_flush_time_ += flush_time;
    fsi.read_log_decode_log_entry_time_ += decode_log_entry_time;
    fsi.tsi_.update(tsi);

    // RPC stops immediately and is a single round of RPC
    if (result.rpc_stop_upon_result_) {
      fsi.single_rpc_cnt_++;

      switch (result.rpc_stop_reason_) {
        case FetchLogARpc::REACH_UPPER_LIMIT:
          fsi.reach_upper_limit_rpc_cnt_++;
          break;

        case FetchLogARpc::REACH_MAX_LOG:
          fsi.reach_max_log_id_rpc_cnt_++;
          break;

        case FetchLogARpc::FETCH_NO_LOG:
          fsi.no_log_rpc_cnt_++;
          break;

        case FetchLogARpc::REACH_MAX_RPC_RESULT:
          fsi.reach_max_result_rpc_cnt_++;
          break;

        default:
          break;
      }
    }
  }
}

int FetchStream::handle_fetch_log_result_(
    FetchLogARpcResult &result,
    volatile bool &stop_flag,
    bool &is_stream_valid,
    const char *&stream_invalid_reason,
    KickOutInfo &kickout_info,
    bool &need_hibernate,
    int64_t &read_log_time,
    int64_t &decode_log_entry_time,
    TransStatInfo &tsi,
    int64_t &flush_time)
{
  int ret = OB_SUCCESS;
  const ObRpcResultCode &rcode = result.rcode_;
  const ObCdcLSFetchLogResp &resp = result.resp_;

  is_stream_valid = true;
  stream_invalid_reason = NULL;
  need_hibernate = false;

  read_log_time = 0;
  decode_log_entry_time = 0;

  if (OB_SUCCESS != rcode.rcode_ || OB_SUCCESS != resp.get_err()) {
    is_stream_valid = false;
    stream_invalid_reason = "FetchLogFail";
    if (OB_FAIL(handle_fetch_log_error_(rcode, resp, kickout_info))) {
      LOG_ERROR("handle fetch log error fail", KR(ret), K(rcode), K(resp), K(kickout_info));
    }
  } else {
    // Read all log entries
    if (OB_FAIL(read_log_(resp, stop_flag, kickout_info, read_log_time, decode_log_entry_time,
        tsi))) {
      if (OB_LOG_NOT_SYNC == ret) {
        // The stream is out of sync and needs to be reopened
        // Note: This error code is handled uniformly below, and the following logic must be handled to
      } else if (OB_IN_STOP_STATE != ret && OB_NEED_RETRY != ret) {
        LOG_ERROR("read log fail", KR(ret), K(resp));
      }
    }
    // Check the feedback array
    else if (OB_FAIL(check_feedback_(resp, kickout_info))) {
      LOG_ERROR("check feed back fail", KR(ret), K(resp));
    } // Update the status of the fetch log task
    else if (OB_FAIL(update_fetch_task_state_(kickout_info, stop_flag, flush_time))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("update fetch task state fail", KR(ret), K(kickout_info));
      }
    } else {
      // success
    }

    // The error code is handled uniformly here
    if (OB_LOG_NOT_SYNC == ret) {
      // Stream out of sync, need to reopen stream
      is_stream_valid = false;
      stream_invalid_reason = "LogNotSync";
      ret = OB_SUCCESS;
    } else if ((OB_NEED_RETRY == ret) || (OB_IN_STOP_STATE == ret)) {
      // 1. OB_NEED_RETRY: handle_group_entry may return
      // 2. OB_IN_STOP_STATE: handle_group_entry may return
      // ...
      is_stream_valid = false;
      stream_invalid_reason = "NeedRetry";

      if (OB_UNLIKELY(ls_fetch_ctx_->is_discarded())) {
        kickout_info.kick_out_reason_ = DISCARDED;
        LOG_INFO("[STAT] [FETCH_STREAM] [RECYCLE_FETCH_TASK]", KPC(ls_fetch_ctx_));
      }
      ret = OB_SUCCESS;
    } else if (OB_SUCCESS == ret) {
      // Kick out the partitions that need to be kicked out and reopen the stream next time
      if (kickout_info.need_kick_out()) {
        is_stream_valid = false;
        stream_invalid_reason = "KickOutLS";
      } else {
        // All LS read logs normally
        is_stream_valid = true;

        // When the fetched log is empty, it needs to sleep for a while
        if (resp.get_log_num() <= 0) {
          need_hibernate = true;
        }

        // TODO: Here we check the upper limit to achieve dynamic adjustment of the upper limit interval
      }
    }
  }

  return ret;
}

bool FetchStream::check_need_switch_server_()
{
  bool bool_ret = false;
  const int64_t check_switch_server_interval = ATOMIC_LOAD(&g_check_switch_server_interval);
  const int64_t cur_time = get_timestamp();

  if ((check_switch_server_interval <= 0)
      || (cur_time - last_switch_server_tstamp_) >= check_switch_server_interval) {
    bool_ret = true;
    last_switch_server_tstamp_ = cur_time;
  }

  return bool_ret;
}

int FetchStream::update_rpc_request_params_()
{
  int ret = OB_SUCCESS;
  int64_t rpc_timeout = ATOMIC_LOAD(&g_rpc_timeout);

  // Update local upper limit to keep in sync with RPC
  if (OB_FAIL(get_upper_limit(upper_limit_))) {
    LOG_ERROR("update upper limit fail", KR(ret));
  }
  // Update fetch log request parameters
  else if (OB_FAIL(fetch_log_arpc_.update_request(
      upper_limit_,
      rpc_timeout))) {
    LOG_ERROR("update fetch log request fail", KR(ret), K(fetch_log_arpc_),
        K(upper_limit_), K(rpc_timeout));
  }

  return ret;
}

int FetchStream::handle_fetch_log_error_(
    const ObRpcResultCode &rcode,
    const obrpc::ObCdcLSFetchLogResp &resp,
    KickOutInfo &kickout_info)
{
  int ret = OB_SUCCESS;
  bool need_kick_out = false;
  KickOutReason kick_out_reason = NONE;
  ObTaskId trace_id(*ObCurTraceId::get_trace_id());

  // RPC failure, need switch server
  if (OB_SUCCESS != rcode.rcode_) {
    need_kick_out = true;
    kick_out_reason = FETCH_LOG_FAIL_ON_RPC;
    if (OB_NOT_NULL(ls_fetch_ctx_)) {
      ls_fetch_ctx_->handle_error(ls_fetch_ctx_->get_tls_id().get_ls_id(),
                                IObLogErrHandler::ErrType::FETCH_LOG,
                                trace_id,
                                ls_fetch_ctx_->get_next_lsn(),
                                rcode.rcode_,
                                "%s");
      IS_WARN_LOG_LEVEL(rcode.rcode_) {
        LOG_WARN("fetch log fail on rpc, need_switch_server", K(svr_), K(rcode), "fetch_stream", this);
      } else {
        LOG_ERROR("fetch log fail on rpc, need_switch_server", K(svr_), K(rcode), "fetch_stream", this);
      }
    }
  }
  // server return error
  else if (OB_SUCCESS != resp.get_err()) {
    // Other errors, switch server directly
    need_kick_out = true;
    kick_out_reason = FETCH_LOG_FAIL_ON_SERVER;
    if (OB_NOT_NULL(ls_fetch_ctx_)) {
      ls_fetch_ctx_->handle_error(ls_fetch_ctx_->get_tls_id().get_ls_id(),
                                IObLogErrHandler::ErrType::FETCH_LOG,
                                trace_id,
                                ls_fetch_ctx_->get_next_lsn(),
                                resp.get_err(),
                                "%s");
      IS_WARN_LOG_LEVEL(resp.get_err()) {
        LOG_WARN("fetch log fail on server, need_switch_server", "fetch_stream", this, K(svr_),
                        "svr_err", resp.get_err(), "svr_debug_err", resp.get_debug_err(),
                        K(rcode), K(resp));
      } else {
        LOG_ERROR("fetch log fail on server, need_switch_server", "fetch_stream", this, K(svr_),
                        "svr_err", resp.get_err(), "svr_debug_err", resp.get_debug_err(),
                        K(rcode), K(resp));
      }
    }
  } else {
    need_kick_out = false;
  }

  if (OB_SUCC(ret) && need_kick_out) {
    kickout_info.kick_out_reason_ = kick_out_reason;
    // Take down from the linklist and reset the linklist node information
    if (OB_ISNULL(ls_fetch_ctx_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("ls_fetch_ctx_ shoule not be null", KR(ret), K(kickout_info), KPC(this));
    } else {
      ls_fetch_ctx_->reset_list_node();
      // Distribute the log fetching task to the next server's log fetching stream at handle_fetch_log_task_
    }
  }

  return ret;
}

bool FetchStream::need_add_into_blacklist_(const KickOutReason reason)
{
  bool bool_ret = false;

  if ((NEED_SWITCH_SERVER == reason) ||
      (DISCARDED == reason) ||
      (ARCHIVE_ITER_END_BUT_LS_NOT_EXIST_IN_PALF == reason)) {
    bool_ret = false;
  } else {
    bool_ret = true;
  }

  return bool_ret;
}

bool FetchStream::exist_(KickOutInfo &kick_out_info,
    const logservice::TenantLSID &tls_id)
{
  return tls_id == kick_out_info.tls_id_;
}

int FetchStream::set_(KickOutInfo &kick_out_info,
    const logservice::TenantLSID &tls_id,
    const KickOutReason kick_out_reason)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! tls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("tls_ls is not valid", KR(ret));
  } else if (KickOutReason::NONE != kick_out_info.kick_out_reason_) {
    ret = OB_ENTRY_EXIST;
  } else {
    kick_out_info.reset(tls_id, kick_out_reason);
  }

  return ret;
}

int FetchStream::read_log_(
    const obrpc::ObCdcLSFetchLogResp &resp,
    volatile bool &stop_flag,
    KickOutInfo &kick_out_info,
    int64_t &read_log_time,
    int64_t &decode_log_entry_time,
    TransStatInfo &tsi)
{
  int ret = OB_SUCCESS;
  const char *buf = resp.get_log_entry_buf();
  const int64_t len = resp.get_pos();
  const int64_t log_cnt = resp.get_log_num();
  int64_t pos = 0;
  int64_t start_read_time = get_timestamp();
  read_log_time = 0;
  decode_log_entry_time = 0;
  // TODO for Debug remove
  LOG_TRACE("redo_log_debug", K(resp), "tls_id", ls_fetch_ctx_->get_tls_id());

  if (OB_ISNULL(buf)) {
    LOG_ERROR("invalid response log buf", K(buf), K(resp));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(ls_fetch_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid ls_fetch_ctx", KR(ret), K(ls_fetch_ctx_));
  } else if (0 == log_cnt) {
    // Ignore 0 logs
    LOG_TRACE("fetch 0 log", K_(svr), "fetch_status", resp.get_fetch_status());
  } else if (OB_FAIL(ls_fetch_ctx_->append_log(buf, len))) {
    LOG_ERROR("append log to LSFetchCtx failed", KR(ret), KPC(ls_fetch_ctx_), K(resp));
  } else {
    // Iterate through all log entries
    for (int64_t idx = 0; OB_SUCC(ret) && (idx < log_cnt); ++idx) {
      int64_t begin_time = get_timestamp();
      palf::LSN group_start_lsn;
      palf::LogGroupEntry group_entry;
      palf::MemPalfBufferIterator entry_iter;
      const char *buffer = nullptr;

      if (OB_FAIL(ls_fetch_ctx_->get_next_group_entry(group_entry, group_start_lsn, buffer))) {
        if (OB_ITER_END != ret) {
          LOG_ERROR("get next_group_entry failed", KR(ret), K_(ls_fetch_ctx), K(resp));
          if (OB_CHECKSUM_ERROR == ret || OB_INVALID_DATA == ret)  {
            OB_ASSERT(ret);
          }
        } else if (idx < log_cnt - 1) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("group_entry iterate end unexpected", KR(ret), K_(ls_fetch_ctx), K(resp));
        } else { /* group_entry iter end */ }
      } else {
        // GroupLogEntry deserialize time
        decode_log_entry_time += (get_timestamp() - begin_time);

        if (OB_FAIL(read_group_entry_(group_entry, group_start_lsn, buffer, kick_out_info, tsi, stop_flag))) {
          if (OB_IN_STOP_STATE != ret && OB_NEED_RETRY != ret) {
            LOG_ERROR("read group entry failed", KR(ret), KPC_(ls_fetch_ctx));
          }

          // If failed, reset memory storage
          ls_fetch_ctx_->reset_memory_storage();
        }

        // update log process
        if (OB_SUCC(ret)) {
          if (OB_FAIL(ls_fetch_ctx_->update_progress(group_entry, group_start_lsn))) {
            LOG_ERROR("ls_fetch_ctx_ update_progress failed", KR(ret), K(group_entry), K(group_start_lsn));
          }
        }
      }
    }
  }

  if (OB_SUCCESS == ret) {
    read_log_time = get_timestamp() - start_read_time;
  }

  return ret;
}

int FetchStream::alloc_fetch_log_srpc(FetchLogSRpc *&fetch_log_srpc)
{
  int ret = OB_SUCCESS;
  void *buf = ob_malloc(sizeof(FetchLogSRpc), ObModIds::OB_LOG_FETCH_LOG_SRPC);

  if (OB_ISNULL(buf)) {
    LOG_ERROR("alloc memory for FetchLogSRpc fail", K(sizeof(FetchLogSRpc)));
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_ISNULL(fetch_log_srpc = new(buf) FetchLogSRpc())) {
    LOG_ERROR("construct fetch log srpc fail", K(buf));
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    // success
  }
  return ret;
}

void FetchStream::free_fetch_log_srpc(FetchLogSRpc *fetch_log_srpc)
{
  if (NULL != fetch_log_srpc) {
    fetch_log_srpc->~FetchLogSRpc();
    ob_free(fetch_log_srpc);
    fetch_log_srpc = NULL;
  }
}

int FetchStream::kick_out_task_(const KickOutInfo &kick_out_info)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(ls_fetch_ctx_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("ls_fetch_ctx_ is NULL", KR(ret), K(ls_fetch_ctx_));
  } else if (OB_FAIL(dispatch_fetch_task_(*ls_fetch_ctx_, kick_out_info.kick_out_reason_))) {
    LOG_ERROR("dispatch fetch task fail", KR(ret), K(kick_out_info));
  } else {}

  return ret;
}

KickOutReason FetchStream::get_feedback_reason_(const Feedback &feedback) const
{
  // Get KickOutReason based on feedback
  KickOutReason reason = NONE;
  switch (feedback) {
    case FeedbackType::LAGGED_FOLLOWER:
      reason = LAGGED_FOLLOWER;
      break;

    case FeedbackType::LOG_NOT_IN_THIS_SERVER:
      reason = LOG_NOT_IN_THIS_SERVER;
      break;

    case FeedbackType::LS_OFFLINED:
      reason = LS_OFFLINED;
      break;

    case FeedbackType::ARCHIVE_ITER_END_BUT_LS_NOT_EXIST_IN_PALF:
      reason = ARCHIVE_ITER_END_BUT_LS_NOT_EXIST_IN_PALF;
      break;

    default:
      reason = NONE;
      break;
  }

  return reason;
}

int FetchStream::check_feedback_(const obrpc::ObCdcLSFetchLogResp &resp,
    KickOutInfo &kick_out_info)
{
  int ret = OB_SUCCESS;
  const Feedback &feedback = resp.get_feedback_type();
  KickOutReason reason = get_feedback_reason_(feedback);

  // Kick out all the LS in the feedback, but not the NONE
  if (reason != NONE) {
    if (OB_ISNULL(ls_fetch_ctx_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("ls_fetch_ctx_ is NULL", KR(ret), K(ls_fetch_ctx_));
    } else {
      const logservice::TenantLSID &tls_id = ls_fetch_ctx_->get_tls_id();

      if (OB_FAIL(set_(kick_out_info, tls_id, reason))) {
        if (OB_ENTRY_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_ERROR("check feedback, set kick out info fail", KR(ret), K(feedback), K(kick_out_info));
        }
      }
    }
  }

  return ret;
}

int FetchStream::update_fetch_task_state_(KickOutInfo &kick_out_info,
    volatile bool &stop_flag,
    int64_t &flush_time)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(ls_fetch_ctx_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("ls_fetch_ctx_ is NULL", KR(ret), K(ls_fetch_ctx_));
  } else {
    LSFetchCtx *task = ls_fetch_ctx_;
    const bool need_check_switch_server = check_need_switch_server_();

    // If the task is deleted, it is kicked out directly
    if (OB_UNLIKELY(task->is_discarded())) {
      LOG_INFO("[STAT] [FETCH_STREAM] [RECYCLE_FETCH_TASK]", "fetch_task", task,
          "fetch_stream", this, KPC(task));
      if (OB_FAIL(set_(kick_out_info, ls_fetch_ctx_->get_tls_id(), DISCARDED))) {
        if (OB_ENTRY_EXIST == ret) {
          // Already exists, ignore
          ret = OB_SUCCESS;
        } else {
          LOG_ERROR("check task discard, set into kick out info fail", KR(ret),
              K(task->get_tls_id()), K(kick_out_info));
        }
      }
    } else {
      // Check if the progress is greater than the upper limit, and update the touch timestamp if it is greater than the upper limit
      // Avoid progress of partitions is not updated that progress larger than upper_limit, which will be misjudged as progress timeout in the future
      if (OB_SUCCESS == ret) {
        task->update_touch_tstamp_if_progress_beyond_upper_limit(upper_limit_);
      }

      // Update each LS's progress to the global
      if (OB_SUCCESS == ret && OB_FAIL(publish_progress_(*task))) {
        LOG_ERROR("update progress fail", KR(ret), K(task), KPC(task));
      }

      if (is_integrated_fetching_mode(ls_fetch_ctx_->get_fetching_mode())) {
        // Check if the server list needs to be updated
        if (OB_SUCCESS == ret && task->need_update_svr_list()) {
          const ObLogFetcherConfig *cfg = nullptr;

          if (OB_FAIL(ls_fetch_ctx_->get_fetcher_config(cfg))) {
            LOG_ERROR("get_fetcher_config_ failed", KR(ret));
          } else {
            const bool need_print_info = (cfg->print_ls_server_list_update_info != 0);
            if (OB_FAIL(task->update_svr_list(need_print_info))) {
              LOG_ERROR("update svr list fail", KR(ret), KPC(task));
            }
          }
        }

        // Check if the log fetch timeout on the current server, and add the timeout tasks to the kick out collection
        if (OB_SUCCESS == ret && OB_FAIL(check_fetch_timeout_(*task, kick_out_info))) {
          LOG_ERROR("check fetch timeout fail", KR(ret), K(task), KPC(task), K(kick_out_info));
        }

        // Periodically check if there is a server with a higher level of excellence at this time, and if so, add the task to the kick out set for active flow cutting
        if (need_check_switch_server) {
          if (OB_SUCCESS == ret && OB_FAIL(check_switch_server_(*task, kick_out_info))) {
            LOG_ERROR("check switch server fail", KR(ret), K(task), KPC(task), K(kick_out_info));
          }
        }
      }

      // Synchronize data to parser
      // 1. synchronize the data generated by the read log to downstream
      // 2. Synchronize progress to downstream using heartbeat task
      if (OB_SUCCESS == ret) {
        int64_t begin_flush_time = get_timestamp();
        if (OB_FAIL(task->sync(stop_flag))) {
          if (OB_IN_STOP_STATE != ret) {
            LOG_ERROR("sync data to parser fail", KR(ret), KPC(task));
          }
        } else {
          flush_time += get_timestamp() - begin_flush_time;
        }
      }
      // end
    }
  }

  return ret;
}

int FetchStream::publish_progress_(LSFetchCtx &task)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(progress_controller_)) {
    LOG_ERROR("invalid progress controller", K(progress_controller_));
    ret = OB_INVALID_ARGUMENT;
  } else {
    int64_t part_progress = task.get_progress();
    int64_t part_progress_id = task.get_progress_id();

    if (OB_UNLIKELY(OB_INVALID_TIMESTAMP == part_progress)) {
      LOG_ERROR("invalid part progress", K(part_progress), K(task));
      ret = OB_INVALID_ERROR;
    } else if (OB_FAIL(progress_controller_->update_progress(part_progress_id,
        part_progress))) {
      LOG_ERROR("update progress by progress controller fail", KR(ret),
          K(part_progress_id), K(part_progress));
    }
  }
  return ret;
}

int FetchStream::check_fetch_timeout_(LSFetchCtx &task, KickOutInfo &kick_out_info)
{
  int ret = OB_SUCCESS;
  bool is_fetch_timeout = false;
  int64_t fetcher_resume_tstamp = OB_INVALID_TIMESTAMP;

  if (OB_ISNULL(stream_worker_)) {
    ret = OB_INVALID_ERROR;
    LOG_ERROR("invalid stream worker", KR(ret), K(stream_worker_));
  } else {
    fetcher_resume_tstamp = stream_worker_->get_fetcher_resume_tstamp();

    if (OB_FAIL(task.check_fetch_timeout(svr_, upper_limit_, fetcher_resume_tstamp, is_fetch_timeout))) {
      LOG_ERROR("check fetch timeout fail", KR(ret), K_(svr),
          K(upper_limit_), "fetcher_resume_tstamp", TS_TO_STR(fetcher_resume_tstamp), K(task));
    } else if (is_fetch_timeout) {
      KickOutReason reason = PROGRESS_TIMEOUT;
      // If the partition fetch log times out, add it to the kick out collection
      if (OB_FAIL(set_(kick_out_info, task.get_tls_id(), reason))) {
        if (OB_ENTRY_EXIST == ret) {
          // Already exists, ignore
          ret = OB_SUCCESS;
        } else {
          LOG_ERROR("set into kick out info fail", KR(ret), K(task.get_tls_id()), K(kick_out_info),
              K(reason));
        }
      }
    }
  }

  return ret;
}

int FetchStream::check_switch_server_(LSFetchCtx &task, KickOutInfo &kick_out_info)
{
  int ret = OB_SUCCESS;
  const char *branch_str = nullptr;

  if (exist_(kick_out_info, task.get_tls_id())) {
    // Do not check for LS already located in kick_out_info
    branch_str = "exist_kick_out_info";
  } else if (task.need_switch_server(svr_)) {
    branch_str = "need_switch_server";
    LOG_TRACE("exist higher priority server, need switch server", KR(ret), "key", task.get_tls_id(),
             K_(svr));
    // If need to switch the stream, add it to the kick out collection
    if (OB_FAIL(set_(kick_out_info, task.get_tls_id(), NEED_SWITCH_SERVER))) {
      if (OB_ENTRY_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_ERROR("set into kick out info fail", KR(ret), K(task.get_tls_id()), K(kick_out_info));
      }
    }
  } else {
    // do nothing
    branch_str = "no_need_switch_server";
  }

  LOG_TRACE("check_switch_server", "ls_id", ls_fetch_ctx_->get_tls_id(), K(branch_str));
  return ret;
}

}
}
