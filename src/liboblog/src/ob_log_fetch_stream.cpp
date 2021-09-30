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

#define USING_LOG_PREFIX OBLOG_FETCHER

#include "ob_log_fetch_stream.h"

#include "lib/container/ob_se_array_iterator.h"   // begin
#include "lib/allocator/page_arena.h"             // ObArenaAllocator

#include "ob_log_config.h"                        // ObLogConfig
#include "ob_log_rpc.h"                           // IObLogRpc
#include "ob_log_svr_finder.h"                    // IObLogSvrFinder
#include "ob_log_fetcher_heartbeat_worker.h"      // IObLogFetcherHeartbeatWorker
#include "ob_log_stream_worker.h"                 // IObLogStreamWorker
#include "ob_log_part_progress_controller.h"      // PartProgressController
#include "ob_log_trace_id.h"                      // ObLogTraceIdGuard

using namespace oceanbase::common;
using namespace oceanbase::obrpc;

namespace oceanbase
{
namespace liboblog
{

int64_t FetchStream::g_stream_max_part_count = ObLogConfig::default_stream_max_partition_count;
int64_t FetchStream::g_stream_life_time = ObLogConfig::default_stream_life_time_sec * _SEC_;
int64_t FetchStream::g_rpc_timeout = ObLogConfig::default_fetch_log_rpc_timeout_sec * _SEC_;
int64_t FetchStream::g_fetch_log_cnt_per_part_per_round = ObLogConfig::default_fetch_log_cnt_per_part_per_round;
int64_t FetchStream::g_dml_progress_limit = ObLogConfig::default_progress_limit_sec_for_dml * _SEC_;
int64_t FetchStream::g_blacklist_survival_time = ObLogConfig::default_blacklist_survival_time_sec * _SEC_;
int64_t FetchStream::g_check_switch_server_interval = ObLogConfig::default_check_switch_server_interval_min * _MIN_;
bool FetchStream::g_print_rpc_handle_info = ObLogConfig::default_print_rpc_handle_info;
bool FetchStream::g_print_stream_dispatch_info = ObLogConfig::default_print_stream_dispatch_info;

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
  // Wait for asynchronous RPC to end before clearing data
  fetch_log_arpc_.stop();

  state_ = State::IDLE;
  stype_ = FETCH_STREAM_TYPE_HOT;
  svr_.reset();
  rpc_ = NULL;
  svr_finder_ = NULL;
  heartbeater_ = NULL;
  stream_worker_ = NULL;
  rpc_result_pool_ = NULL;
  progress_controller_ = NULL;

  upper_limit_ = OB_INVALID_TIMESTAMP;
  need_open_stream_ = false;
  stream_seq_.reset();
  last_feedback_tstamp_ = OB_INVALID_TIMESTAMP;
  last_switch_server_tstamp_ = 0;
  fetch_task_pool_.reset();
  fetch_log_arpc_.reset();

  last_stat_time_ = OB_INVALID_TIMESTAMP;
  cur_stat_info_.reset();
  last_stat_info_.reset();

  FSListNode::reset();
}

void FetchStream::reset(const common::ObAddr &svr,
    const FetchStreamType stream_type,
    IObLogRpc &rpc,
    IObLogSvrFinder &svr_finder,
    IObLogFetcherHeartbeatWorker &heartbeater,
    IObLogStreamWorker &stream_worker,
    IFetchLogARpcResultPool &rpc_result_pool,
    PartProgressController &progress_controller)
{
  reset();

  svr_ = svr;
  stype_ = stream_type;
  rpc_ = &rpc;
  svr_finder_ = &svr_finder;
  heartbeater_ = &heartbeater;
  stream_worker_ = &stream_worker;
  rpc_result_pool_ = &rpc_result_pool;
  progress_controller_ = &progress_controller;

  fetch_log_arpc_.reset(svr, rpc, stream_worker, rpc_result_pool);
}

int FetchStream::add_fetch_task(PartFetchCtx &task)
{
  int ret = OB_SUCCESS;
  bool is_pool_idle = false;

  if (OB_UNLIKELY(task.get_fetch_stream_type() != stype_)) {
    LOG_ERROR("invalid part task, stream type does not match", K(stype_),
        K(task.get_fetch_stream_type()), K(task));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(stream_worker_)) {
    LOG_ERROR("invalid stream worker", K(stream_worker_));
    ret = OB_INVALID_ERROR;
  } else {
    // Mark to start fetching logs
    task.dispatch_in_fetch_stream(svr_, *this);

    if (OB_FAIL(fetch_task_pool_.push(task, is_pool_idle))) {
      if (OB_SIZE_OVERFLOW != ret) {
        LOG_ERROR("push task into pool fail", KR(ret), K(task));
      }
    } else {
      LOG_DEBUG("[STAT] [FETCH_STREAM] [ADD_FETCH_TASK]", "fetch_task", &task,
          "fetch_stream", this, K(is_pool_idle), "fetch_task", task,
          "fetch_stream", *this);

      if (is_pool_idle) {
        // If the task pool is in IDLE state before the task is inserted, then no thread is processing the current fetch log stream,
        // and the fetch log stream task is marked as new.
        // For a new fetch log stream task, it should be immediately assigned to a worker thread for processing
        if (OB_FAIL(stream_worker_->dispatch_stream_task(*this, "EmptyStream"))) {
          LOG_ERROR("dispatch stream task fail", KR(ret));
        } else {
          // Note: You cannot continue to manipulate this data structure afterwards !!!!!
        }
      }
    }
  }

  return ret;
}

int FetchStream::handle(volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  bool print_stream_dispatch_info = ATOMIC_LOAD(&g_print_stream_dispatch_info);

  if (print_stream_dispatch_info) {
    LOG_INFO("[STAT] [FETCH_STREAM] begin handle", "fetch_stream", this,
        "fetch_stream", *this);
  } else {
    LOG_DEBUG("[STAT] [FETCH_STREAM] begin handle", "fetch_stream", this,
        "fetch_stream", *this);
  }

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
    LOG_ERROR("invalid state", K(state_));
    ret = OB_INVALID_ERROR;
  }

  // Note: The following can no longer continue the operation, there may be concurrency issues ！！！！

  return ret;
}

// The purpose of a timed task is to assign itself to a worker thread
void FetchStream::process_timer_task()
{
  int ret = OB_SUCCESS;
  static int64_t max_dispatch_time = 0;
  int64_t start_time = get_timestamp();
  int64_t end_time = 0;

  LOG_DEBUG("[STAT] [WAKE_UP_STREAM_TASK]", "task", this, "task", *this);

  if (OB_ISNULL(stream_worker_)) {
    LOG_ERROR("invalid stream worker", K(stream_worker_));
    ret = OB_INVALID_ERROR;
  } else if (OB_FAIL(stream_worker_->dispatch_stream_task(*this, "TimerWakeUp"))) {
    LOG_ERROR("dispatch stream task fail", KR(ret), K(this));
  } else {
    ATOMIC_STORE(&end_time, get_timestamp());
    max_dispatch_time = std::max(max_dispatch_time, ATOMIC_LOAD(&end_time) - start_time);

    if (REACH_TIME_INTERVAL(STAT_INTERVAL)) {
      LOG_INFO("[STAT] [FETCH_STREAM_TIMER_TASK]", K(max_dispatch_time));
    }
  }
}

void FetchStream::configure(const ObLogConfig & config)
{
  int64_t stream_max_partition_count = config.stream_max_partition_count;
  int64_t stream_life_time_sec = config.stream_life_time_sec;
  int64_t fetch_log_rpc_timeout_sec = config.fetch_log_rpc_timeout_sec;
  int64_t fetch_log_cnt_per_part_per_round = config.fetch_log_cnt_per_part_per_round;
  int64_t dml_progress_limit_sec = config.progress_limit_sec_for_dml;
  int64_t blacklist_survival_time_sec = config.blacklist_survival_time_sec;
  int64_t check_switch_server_interval_min = config.check_switch_server_interval_min;
  bool print_rpc_handle_info = config.print_rpc_handle_info;
  bool print_stream_dispatch_info = config.print_stream_dispatch_info;

  ATOMIC_STORE(&g_stream_max_part_count, stream_max_partition_count);
  LOG_INFO("[CONFIG]", K(stream_max_partition_count));
  ATOMIC_STORE(&g_stream_life_time, stream_life_time_sec * _SEC_);
  LOG_INFO("[CONFIG]", K(stream_life_time_sec));
  ATOMIC_STORE(&g_rpc_timeout, fetch_log_rpc_timeout_sec * _SEC_);
  LOG_INFO("[CONFIG]", K(fetch_log_rpc_timeout_sec));
  ATOMIC_STORE(&g_fetch_log_cnt_per_part_per_round, fetch_log_cnt_per_part_per_round);
  LOG_INFO("[CONFIG]", K(fetch_log_cnt_per_part_per_round));
  ATOMIC_STORE(&g_dml_progress_limit, dml_progress_limit_sec * _SEC_);
  LOG_INFO("[CONFIG]", K(dml_progress_limit_sec));
  ATOMIC_STORE(&g_blacklist_survival_time, blacklist_survival_time_sec * _SEC_);
  LOG_INFO("[CONFIG]", K(blacklist_survival_time_sec));
  ATOMIC_STORE(&g_check_switch_server_interval, check_switch_server_interval_min * _MIN_);
  LOG_INFO("[CONFIG]", K(check_switch_server_interval_min));
  ATOMIC_STORE(&g_print_rpc_handle_info, print_rpc_handle_info);
  LOG_INFO("[CONFIG]", K(print_rpc_handle_info));
  ATOMIC_STORE(&g_print_stream_dispatch_info, print_stream_dispatch_info);
  LOG_INFO("[CONFIG]", K(print_stream_dispatch_info));
}

void FetchStream::do_stat()
{
  ObByteLockGuard lock_guard(stat_lock_);

  int64_t cur_time = get_timestamp();
  int64_t delta_time = cur_time - last_stat_time_;
  double delta_second = static_cast<double>(delta_time) / static_cast<double>(_SEC_);

  if (last_stat_time_ <= 0) {
    last_stat_time_ = cur_time;
    last_stat_info_ = cur_stat_info_;
  } else if (delta_second <= 0) {
    // Statistics are too frequent, ignore the statistics here, otherwise the following will lead to divide by zero error
    LOG_DEBUG("fetch stream stat too frequently", K(delta_time), K(delta_second),
        K(last_stat_time_), K(this));
  } else {
    FetchStatInfoPrinter fsi_printer(cur_stat_info_, last_stat_info_, delta_second);

    _LOG_INFO("[STAT] [FETCH_STREAM] stream=%s(%p:%s) part=%ld(queue=%ld) %s", to_cstring(svr_), this,
        print_fetch_stream_type(stype_), fetch_task_pool_.total_count(),
        fetch_task_pool_.queued_count(), to_cstring(fsi_printer));

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
    LOG_DEBUG("[STAT] [FETCH_STREAM] leave stream", "fetch_stream", this, K(leave_reason));
  }
}

int FetchStream::handle_idle_task_(volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  bool pool_become_idle = false;
  bool task_list_changed = false;

  if (OB_UNLIKELY(IDLE != state_)) {
    LOG_ERROR("state does not match IDLE", K(state_));
    ret = OB_STATE_NOT_MATCH;
  }
  // Update task pool status and prepare task list
  else if (OB_FAIL(fetch_task_pool_.update(pool_become_idle, task_list_changed))) {
    LOG_ERROR("task pool prepare task fail", KR(ret));
  }
  // If the task pool changes to IDLE state, no further operations will be performed
  else if (pool_become_idle) {
    // Note: You must not continue to manipulate any data structures here, as another thread may have taken over the fetch log stream.
    // See the add_fetch_task() implementation for details
    handle_when_leave_("StreamBecomeEmpty");
  } else {
    bool open_stream_succeed = false;

    // Only when the task list has changed, or the active setting needs to open the stream, then the open stream operation needs to be performed
    if (! task_list_changed && ! need_open_stream_) {
      open_stream_succeed = true;
    } else {
      int64_t part_count = 0;
      const char *discard_reason = task_list_changed ? "TaskListChangedOnIdle" : "ForceOpenStream";

      open_stream_succeed = false;

      // First discard the old request
      fetch_log_arpc_.discard_request(discard_reason);

      if (OB_FAIL(open_stream_(open_stream_succeed, part_count))) {
        LOG_ERROR("open stream fail", KR(ret));
      } else if (open_stream_succeed) {
        // Open stream successfully, ready to fetch log by asynchronous RPC request
        if (OB_FAIL(prepare_rpc_request_(part_count))) {
          LOG_ERROR("prepare rpc request fail", KR(ret), K(part_count));
        }
      }
    }

    if (OB_SUCCESS == ret) {
      // Failed to open stream, kick out all partitions and start over
      if (! open_stream_succeed) {
        if (OB_FAIL(kick_out_all_(OPEN_STREAM_FAIL))) {
          LOG_ERROR("kick out all partition fail after open stream error", KR(ret));
        } else {
          // Recursively call the handle function to re-prepare the task
          // Note: You cannot continue to manipulate any data structures after handle, there are concurrency scenarios
          ret = handle(stop_flag);
        }
      } else {
        bool need_fetch_log = false;

        // Update upper limit, prepare for fetching logs
        if (OB_FAIL(get_upper_limit(upper_limit_))) {
          LOG_ERROR("update upper limit fail", KR(ret));
        }
        // Check need to fetch logs
        else if (OB_FAIL(check_need_fetch_log_(upper_limit_, need_fetch_log))) {
          LOG_ERROR("check need fetch log fail", KR(ret), K(upper_limit_));
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
          if (OB_FAIL(async_fetch_log_(rpc_send_succeed))) {
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
  }
  return ret;
}

int FetchStream::open_stream_(bool &rpc_succeed, int64_t &part_count)
{
  int ret = OB_SUCCESS;
  OpenStreamSRpc open_stream_rpc;
  FetchTaskList &task_list = fetch_task_pool_.get_task_list();
  int64_t stream_life_time = ATOMIC_LOAD(&g_stream_life_time);
  int64_t rpc_timeout = ATOMIC_LOAD(&g_rpc_timeout);

  ObLogTraceIdGuard trace_guard;

  rpc_succeed = false;
  need_open_stream_ = true; // Default requires open stream
  part_count = 0;

  if (OB_ISNULL(rpc_)) {
    LOG_ERROR("invalid rpc handler", K(rpc_));
    ret = OB_INVALID_ERROR;
  }
  // Execute synchronous RPC
  else if (OB_FAIL(open_stream_rpc.open_stream(*rpc_, svr_, rpc_timeout,
      task_list, stream_seq_, stream_life_time))) {
    LOG_ERROR("launch open stream rpc fail", KR(ret), K(svr_), K(rpc_timeout),
        K(task_list), K(stream_seq_), K(stream_life_time));
  } else {
    // Checking RPC return values
    const ObRpcResultCode &rcode = open_stream_rpc.get_result_code();
    const ObLogOpenStreamResp &resp = open_stream_rpc.get_resp();
    const ObLogOpenStreamReq &req = open_stream_rpc.get_req();

    // RPC failure
    if (OB_SUCCESS != rcode.rcode_) {
      LOG_ERROR("open stream fail on rpc", K_(svr), K(rcode), K(req), K(resp));
    } else if (OB_SUCCESS != resp.get_err()) {
      // server return error
      LOG_ERROR("open stream fail on server", K_(svr), "svr_err", resp.get_err(),
          "svr_debug_err", resp.get_debug_err(), K(rcode), K(req), K(resp));
    } else {
      // Ending the old stream
      LOG_DEBUG("[STAT] [FETCH_STREAM] [CLOSE_STREAM]", "fetch_stream", this, K_(stream_seq));

      // Open new stream
      LOG_DEBUG("[STAT] [FETCH_STREAM] [OPEN_STREAM]", "fetch_stream", this,
          "stream_seq", resp.get_stream_seq(),
          "part_count", task_list.count());

      // Open stream successfully
      need_open_stream_ = false;
      rpc_succeed = true;
      stream_seq_ = resp.get_stream_seq();
      last_feedback_tstamp_ = OB_INVALID_TIMESTAMP;
      part_count = task_list.count();
    }
  }

  return ret;
}

int FetchStream::kick_out_all_(KickOutReason kick_out_reason)
{
  int ret = OB_SUCCESS;
  FetchTaskList list;

  // Kick all partitions in the list from the task pool and return the task list
  if (OB_FAIL(fetch_task_pool_.kick_out_task_list(list))) {
    LOG_ERROR("kick out task list from task pool fail", KR(ret));
  } else {
    PartFetchCtx *task = list.head();

    while (OB_SUCCESS == ret && NULL != task) {
      PartFetchCtx *next = task->get_next();

      // Take down from the linklist and reset the linklist node information
      task->reset_list_node();

      // Distribute the log fetching task to the next server's log fetching stream
      if (OB_FAIL(dispatch_fetch_task_(*task, kick_out_reason))) {
        LOG_ERROR("dispatch fetch task fail", KR(ret), KPC(task));
      } else {
        task = next;
      }
    }

    if (OB_SUCCESS == ret) {
      list.reset();
    }
  }

  return ret;
}

int FetchStream::dispatch_fetch_task_(PartFetchCtx &task,
    KickOutReason dispatch_reason)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(stream_worker_)) {
    LOG_ERROR("invalid stream worker", K(stream_worker_));
    ret = OB_INVALID_ERROR;
  } else {
    // The server is not blacklisted when the stream is actively switch and when the partition is discarded, but is blacklisted in all other cases.
    if (need_add_into_blacklist_(dispatch_reason)) {
      // Get the total time of the current partition of the server service at this time
      int64_t svr_start_fetch_tstamp = OB_INVALID_TIMESTAMP;

      if (OB_FAIL(task.get_cur_svr_start_fetch_tstamp(svr_, svr_start_fetch_tstamp))) {
        LOG_ERROR("get_cur_svr_start_fetch_tstamp fail", KR(ret), "pkey", task.get_pkey(),
            K(svr_), K(svr_start_fetch_tstamp));
      } else {
        int64_t svr_service_time = get_timestamp() - svr_start_fetch_tstamp;
        int64_t cur_survival_time = ATOMIC_LOAD(&g_blacklist_survival_time);
        int64_t survival_time = cur_survival_time;
        // Server add into blacklist
        if (OB_FAIL(task.add_into_blacklist(svr_, svr_service_time, survival_time))) {
          LOG_ERROR("task add into blacklist fail", KR(ret), K(task), K(svr_),
              "svr_service_time", TVAL_TO_STR(svr_service_time),
              "survival_time", TVAL_TO_STR(survival_time));
        }
      }
    }

    if (OB_SUCCESS == ret) {
      const char *dispatch_reason_str = print_kick_out_reason_(dispatch_reason);
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

int FetchStream::get_upper_limit(int64_t &upper_limit_us)
{
  int ret = OB_SUCCESS;
  int64_t min_progress = OB_INVALID_TIMESTAMP;

  if (OB_ISNULL(progress_controller_)) {
    LOG_ERROR("invalid progress controller", K(progress_controller_));
    ret = OB_INVALID_ERROR;
  }
  //  Get global minimum progress
  else if (OB_FAIL(progress_controller_->get_min_progress(min_progress))) {
    LOG_ERROR("get_min_progress fail", KR(ret), KPC(progress_controller_));
  } else if (OB_UNLIKELY(OB_INVALID_TIMESTAMP == min_progress)) {
    LOG_ERROR("current min progress is invalid", K(min_progress), KPC(progress_controller_));
    ret = OB_INVALID_ERROR;
  } else {
    // DDL partition is not limited by progress limit, here upper limit is set to a future value
    if (FETCH_STREAM_TYPE_DDL == stype_) {
      upper_limit_us = min_progress + _YEAR_;
    } else {
      // Other partition are limited by progress limit
      upper_limit_us = min_progress + ATOMIC_LOAD(&g_dml_progress_limit);
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
    PartFetchCtx *task = fetch_task_pool_.get_task_list().head();

    need_fetch_log = false;

    // Iterate through all tasks, as long as there is a task less than upper limit, then you need to continue to fetch logs
    while (! need_fetch_log && OB_SUCCESS == ret && NULL != task) {
      int64_t part_progress = task->get_progress();
      if (OB_UNLIKELY(OB_INVALID_TIMESTAMP == part_progress)) {
        LOG_ERROR("fetch task progress is invalid", K(part_progress), KPC(task));
        ret = OB_ERR_UNEXPECTED;
      } else {
        need_fetch_log = (part_progress < limit);
      }

      if (OB_SUCCESS == ret) {
        task = task->get_next();
      }
    }
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

int FetchStream::prepare_rpc_request_(const int64_t part_count)
{
  int ret = OB_SUCCESS;

  // TODO: Currently, every time a RPC request is prepared, the default value is used, find a way to optimize
  bool need_feed_back = false;
  int64_t rpc_timeout = ATOMIC_LOAD(&g_rpc_timeout);
  int64_t fetch_log_cnt_per_part_per_round = ATOMIC_LOAD(&g_fetch_log_cnt_per_part_per_round);

  if (OB_FAIL(fetch_log_arpc_.prepare_request(stream_seq_, part_count,
      fetch_log_cnt_per_part_per_round,
      need_feed_back,
      rpc_timeout))) {
    LOG_ERROR("prepare request for rpc fail", KR(ret), K(stream_seq_), K(part_count),
        K(fetch_log_cnt_per_part_per_round), K(need_feed_back), K(rpc_timeout));
  }

  return ret;
}

int FetchStream::async_fetch_log_(bool &rpc_send_succeed)
{
  int ret = OB_SUCCESS;

  rpc_send_succeed = false;

  // Launch an asynchronous RPC
  if (OB_FAIL(fetch_log_arpc_.async_fetch_log(stream_seq_,
      upper_limit_,
      rpc_send_succeed))) {
    LOG_ERROR("async_fetch_log fail", KR(ret), K(stream_seq_), K(upper_limit_),
        K(fetch_log_arpc_));
  } else {
    // Asynchronous RPC execution succeeded
    // Note: You cannot continue to manipulate any data structures afterwards !!!!
  }
  return ret;
}

void FetchStream::print_handle_info_(FetchLogARpcResult &result,
    const int64_t handle_rpc_time,
    const int64_t read_log_time,
    const int64_t decode_log_entry_time,
    const bool rpc_is_flying,
    const bool is_stream_valid,
    const char *stream_invalid_reason,
    PartFetchCtx *min_progress_task,
    const TransStatInfo &tsi,
    const bool need_stop_request)
{
  bool print_rpc_handle_info = ATOMIC_LOAD(&g_print_rpc_handle_info);
  PartFetchCtx::PartProgress min_progress;
  ObPartitionKey min_pkey;
  if (NULL != min_progress_task) {
    min_progress_task->get_progress_struct(min_progress);
    min_pkey = min_progress_task->get_pkey();
  }

  if (print_rpc_handle_info) {
    LOG_INFO("handle rpc result by fetch stream",
        "fetch_stream", this, K_(stream_seq),
        K_(fetch_task_pool),
        "upper_limit", TS_TO_STR(upper_limit_),
        K(need_stop_request),
        "rpc_stop_upon_result", result.rpc_stop_upon_result_,
        "rpc_stop_reason", FetchLogARpc::print_rpc_stop_reason(result.rpc_stop_reason_),
        K(rpc_is_flying), K(is_stream_valid), K(stream_invalid_reason),
        "resp", result.resp_, K(handle_rpc_time), K(read_log_time), K(decode_log_entry_time),
        K(tsi), K(min_progress), K(min_pkey));
  } else {
    LOG_DEBUG("handle rpc result by fetch stream",
        "fetch_stream", this, K_(stream_seq),
        K_(fetch_task_pool),
        "upper_limit", TS_TO_STR(upper_limit_),
        K(need_stop_request),
        "rpc_stop_upon_result", result.rpc_stop_upon_result_,
        "rpc_stop_reason", FetchLogARpc::print_rpc_stop_reason(result.rpc_stop_reason_),
        K(rpc_is_flying), K(is_stream_valid), K(stream_invalid_reason),
        "resp", result.resp_, K(handle_rpc_time), K(read_log_time), K(decode_log_entry_time),
        K(tsi), K(min_progress), K(min_pkey));
  }
}

bool FetchStream::has_new_fetch_task_() const
{
  // If the queue of the fetch log task pool is not empty, it marks that there is a new task to be processed
  return fetch_task_pool_.queued_count() > 0;
}

int FetchStream::process_result_(FetchLogARpcResult &result,
    volatile bool &stop_flag,
    const bool rpc_is_flying,
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
  PartFetchCtx *min_progress_task = NULL;
  bool need_stop_request = false;
  const char *stream_invalid_reason = NULL;

  // Process each result, set the corresponding trace id
  ObLogTraceIdGuard trace_guard(result.trace_id_);

  // Process the log results and make appropriate decisions based on the results
  if (OB_FAIL(handle_fetch_log_result_(result,
      stop_flag,
      is_stream_valid,
      stream_invalid_reason,
      need_hibernate,
      read_log_time,
      decode_log_entry_time,
      tsi,
      flush_time,
      min_progress_task))) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("handle fetch log result fail", KR(ret), K(result), K(fetch_log_arpc_));
    }
  }
  // 如果当前取日志流无效，需要重新开流
  else if (! is_stream_valid) {
    // If the current fetch log stream is invalid, you need to reopen the stream
    fetch_log_arpc_.discard_request(stream_invalid_reason);
    need_open_stream_ = true;
  }
  // The log stream is valid and ready to continue processing the next RPC packet
  else {
    // If a new partition task comes in, notify RPC to stop continuing to fetch logs
    // Avoid starvation of new partitions
    need_stop_request = (rpc_is_flying && has_new_fetch_task_());

    // Mark the request as finished
    // After you stop the request, you still need to continue iterating through the results until all the results are iterated through
    if (need_stop_request && (OB_FAIL(fetch_log_arpc_.mark_request_stop(stream_seq_)))) {
      LOG_ERROR("fetch log rpc mar request stop fail", KR(ret), K(this), K(stream_seq_),
          K(fetch_log_arpc_), K(fetch_task_pool_));
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
        rpc_is_flying, is_stream_valid, stream_invalid_reason, min_progress_task, tsi,
        need_stop_request);
  }

  return ret;
}

int FetchStream::handle_fetch_log_task_(volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(FETCH_LOG != state_)) {
    LOG_ERROR("state does not match which is not FETCH_LOG", K(state_));
    ret = OB_STATE_NOT_MATCH;
  } else {
    bool need_hibernate = false;
    bool rpc_is_flying = false;
    bool is_stream_valid = true;
    FetchLogARpcResult *result = NULL;

    // Whether the log stream is taken over by RPC, default is false
    bool stream_been_taken_over_by_rpc = false;

    // Continuously iterate through the fetch log results while the current fetch log stream is continuously active, and then process
    while (! stop_flag
        && OB_SUCCESS == ret
        && is_stream_valid
        && OB_SUCC(fetch_log_arpc_.next_result(result, rpc_is_flying))) {
      need_hibernate = false;

      if (OB_ISNULL(result)) {
        LOG_ERROR("invalid result", K(result));
        ret = OB_INVALID_ERROR;
      }
      // Processing results
      else if (OB_FAIL(process_result_(*result, stop_flag, rpc_is_flying, need_hibernate,
          is_stream_valid))) {
        if (OB_IN_STOP_STATE != ret) {
          LOG_ERROR("process result fail", KR(ret), K(result), KPC(result), K(this), KPC(this));
        }
      } else {
        // Processing success
      }

      // Recycling result
      if (NULL != result) {
        fetch_log_arpc_.revert_result(result);
        result = NULL;
      }
    }

    if (stop_flag) {
      ret = OB_IN_STOP_STATE;
    }

    if (OB_ITER_END == ret) {
      // Iterate through all results
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

    // Final unified processing results
    if (OB_SUCCESS == ret) {
      if (stream_been_taken_over_by_rpc) {
        // The fetch log stream is taken over by the RPC callback, maintains the FETCH_LOG state, and exits unconditionally
        // Note: You cannot continue to manipulate any data structures afterwards !!!!!
        handle_when_leave_("RpcTaskOver");
      } else {
        // The current thread is still responsible for this fetch log stream
        // Entering IDLE state
        switch_state(IDLE);

        // Hibernate the task if it needs to be hibernated
        // Note: No more data structures can be accessed afterwards, there is a concurrency scenario !!!!
        if (need_hibernate) {
          if (OB_FAIL(hibernate_())) {
            LOG_ERROR("hibernate fail", KR(ret));
          }
        } else {
          // No hibernation required, then recursive processing of IDLE tasks
          // Note: no more data structures can be accessed afterwards, there is a concurrency scenario !!!!
          ret = handle(stop_flag);
        }
      }
    }
  }

  return ret;
}

void FetchStream::update_fetch_stat_info_(FetchLogARpcResult &result,
    const int64_t handle_rpc_time,
    const int64_t read_log_time,
    const int64_t decode_log_entry_time,
    const int64_t flush_time,
    const TransStatInfo &tsi)
{
  ObByteLockGuard lock_guard(stat_lock_);

  FetchStatInfo &fsi = cur_stat_info_;
  const ObRpcResultCode &rcode = result.rcode_;
  const ObLogStreamFetchLogResp &resp = result.resp_;
  const ObFetchStatus &fetch_status = resp.get_fetch_status();

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

int FetchStream::handle_fetch_log_result_(FetchLogARpcResult &result,
    volatile bool &stop_flag,
    bool &is_stream_valid,
    const char *&stream_invalid_reason,
    bool &need_hibernate,
    int64_t &read_log_time,
    int64_t &decode_log_entry_time,
    TransStatInfo &tsi,
    int64_t &flush_time,
    PartFetchCtx *&min_progress_task)
{
  int ret = OB_SUCCESS;
  const ObStreamSeq &seq = result.seq_;
  const ObRpcResultCode &rcode = result.rcode_;
  const ObLogStreamFetchLogResp &resp = result.resp_;

  is_stream_valid = true;
  stream_invalid_reason = NULL;
  need_hibernate = false;

  read_log_time = 0;
  decode_log_entry_time = 0;

  if (OB_SUCCESS != rcode.rcode_ || OB_SUCCESS != resp.get_err()) {
    is_stream_valid = false;
    stream_invalid_reason = "FetchLogFail";
    if (OB_FAIL(handle_fetch_log_error_(seq, rcode, resp))) {
      LOG_ERROR("handle fetch log error fail", KR(ret), K(seq), K(rcode), K(resp));
    }
  } else {
    // A collection of log taking tasks that need to be kicked out
    ObArenaAllocator allocator;
    KickOutTaskSet kick_out_set(allocator);

    // Read all log entries
    if (OB_FAIL(read_log_(resp, stop_flag, kick_out_set, read_log_time, decode_log_entry_time,
        tsi))) {
      if (OB_LOG_NOT_SYNC == ret) {
        // The stream is out of sync and needs to be reopened
        // Note: This error code is handled uniformly below, and the following logic must be handled to
      } else if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("read log fail", KR(ret), K(resp));
      }
    }
    // Check the feedback array
    else if (OB_FAIL(check_feedback_(resp, kick_out_set))) {
      LOG_ERROR("check feed back fail", KR(ret), K(resp));
    }
    // Check to fetch the log heartbeat array
    else if (OB_FAIL(check_fetch_log_heartbeat_(resp, kick_out_set))) {
      if (OB_LOG_NOT_SYNC == ret) {
        // Stream out of sync, need to reopen stream
      } else {
        LOG_ERROR("check fetch log heartbeat fail", KR(ret), K(resp), K(kick_out_set));
      }
    }
    // Update the status of the fetch log task
    else if (OB_FAIL(update_fetch_task_state_(kick_out_set, stop_flag, min_progress_task,
          flush_time))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("update fetch task state fail", KR(ret), K(kick_out_set));
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
    } else if (OB_SUCCESS == ret) {
      // Kick out the partitions that need to be kicked out and reopen the stream next time
      if (kick_out_set.count() > 0) {
        is_stream_valid = false;
        stream_invalid_reason = "KickOutPartition";
        if (OB_FAIL(kick_out_task_(kick_out_set))) {
          LOG_ERROR("kick out task fail", KR(ret), K(kick_out_set));
        }
      } else {
        // All partitions read logs normally
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

bool FetchStream::check_need_feedback_()
{
  bool bool_ret = false;
  const int64_t feedback_interval = TCONF.stream_feedback_interval_sec * _SEC_;
  const int64_t cur_time = get_timestamp();

  if (OB_INVALID_TIMESTAMP == last_feedback_tstamp_               // First request must feedback
      || feedback_interval <= 0                                   // Request a feedback every time
      || (cur_time - last_feedback_tstamp_) >= feedback_interval) // Periodic feedback
  {
    bool_ret = true;
    last_feedback_tstamp_ = cur_time;
  }

  return bool_ret;
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
  int64_t fetch_log_cnt_per_part_per_round = ATOMIC_LOAD(&g_fetch_log_cnt_per_part_per_round);
  int64_t rpc_timeout = ATOMIC_LOAD(&g_rpc_timeout);
  const bool need_feed_back = check_need_feedback_();

  // Update local upper limit to keep in sync with RPC
  if (OB_FAIL(get_upper_limit(upper_limit_))) {
    LOG_ERROR("update upper limit fail", KR(ret));
  }
  // Update fetch log request parameters
  else if (OB_FAIL(fetch_log_arpc_.update_request(stream_seq_,
      upper_limit_,
      fetch_log_cnt_per_part_per_round,
      need_feed_back,
      rpc_timeout))) {
    LOG_ERROR("update fetch log request fail", KR(ret), K(fetch_log_arpc_), K(stream_seq_),
        K(upper_limit_), K(fetch_log_cnt_per_part_per_round), K(need_feed_back), K(rpc_timeout));
  }

  return ret;
}

int FetchStream::handle_fetch_log_error_(const ObStreamSeq &seq,
    const ObRpcResultCode &rcode,
    const ObLogStreamFetchLogResp &resp)
{
  int ret = OB_SUCCESS;
  bool need_kick_out_all = false;
  KickOutReason kick_out_reason = NONE;

  // RPC failure, need switch server
  if (OB_SUCCESS != rcode.rcode_) {
    need_kick_out_all = true;
    kick_out_reason = FETCH_LOG_FAIL_ON_RPC;
    LOG_ERROR("fetch log fail on rpc", K_(svr), K(rcode), "fetch_stream", this, K(seq));
  }
  // server return error
  else if (OB_SUCCESS != resp.get_err()) {
    // If the stream does not exist, the stream is reopened without switching servers
    if (OB_STREAM_NOT_EXIST == resp.get_err()) {
      need_kick_out_all = false;
      LOG_WARN("fetch log fail on server, stream not exist", K_(svr), "svr_err", resp.get_err(),
          "svr_debug_err", resp.get_debug_err(), K(seq), K(rcode), K(resp));
    } else {
      // Other errors, switch server directly
      need_kick_out_all = true;
      kick_out_reason = FETCH_LOG_FAIL_ON_SERVER;
      LOG_ERROR("fetch log fail on server", "fetch_stream", this, K_(svr),
          "svr_err", resp.get_err(), "svr_debug_err", resp.get_debug_err(),
          K(seq), K(rcode), K(resp));
    }
  } else {
    need_kick_out_all = false;
  }

  if (OB_SUCCESS == ret && need_kick_out_all) {
    if (OB_FAIL(kick_out_all_(kick_out_reason))) {
      LOG_ERROR("kick out all fail", KR(ret));
    }
  }

  return ret;
}

bool FetchStream::need_add_into_blacklist_(const KickOutReason reason)
{
  bool bool_ret = false;

  if ((NEED_SWITCH_SERVER == reason) || (DISCARDED == reason)) {
    bool_ret = false;
  } else {
    bool_ret = true;
  }

  return bool_ret;
}

const char *FetchStream::print_kick_out_reason_(const KickOutReason reason)
{
  const char *str = "NONE";
  switch (reason) {
    case OPEN_STREAM_FAIL:
      str = "OpenStreamFail";
      break;

    case FETCH_LOG_FAIL_ON_RPC:
      str = "FetchLogFailOnRpc";
      break;

    case FETCH_LOG_FAIL_ON_SERVER:
      str = "FetchLogFailOnServer";
      break;

    case MISSING_LOG_OPEN_STREAM_FAIL:
      str = "MissingLogOpenStreamFail";
      break;

    case MISSING_LOG_FETCH_FAIL:
      str = "MissingLogFetchFail";
      break;

    case LAGGED_FOLLOWER:
      str = "LAGGED_FOLLOWER";
      break;

    case LOG_NOT_IN_THIS_SERVER:
      str = "LOG_NOT_IN_THIS_SERVER";
      break;

    case PARTITION_OFFLINED:
      str = "PARTITION_OFFLINED";
      break;

    case PROGRESS_TIMEOUT:
      str = "PROGRESS_TIMEOUT";
      break;

    case PROGRESS_TIMEOUT_ON_LAGGED_REPLICA:
      str = "PROGRESS_TIMEOUT_ON_LAGGED_REPLICA";
      break;

    case NEED_SWITCH_SERVER:
      str = "NeedSwitchServer";
      break;

    case DISCARDED:
      str = "Discarded";
      break;

    default:
      str = "NONE";
      break;
  }

  return str;
}

bool FetchStream::exist_(KickOutTaskSet &kick_out_set, const common::ObPartitionKey &pkey)
{
  KickOutTask task(pkey);
  return OB_HASH_EXIST == kick_out_set.exist_refactored(task);
}

int FetchStream::set_(KickOutTaskSet &kick_out_set,
    const common::ObPartitionKey &pkey,
    KickOutReason kick_out_reason)
{
  KickOutTask task(pkey, kick_out_reason);
  return kick_out_set.set_refactored(task);
}

int FetchStream::read_log_(const ObLogStreamFetchLogResp &resp,
    volatile bool &stop_flag,
    KickOutTaskSet &kick_out_set,
    int64_t &read_log_time,
    int64_t &decode_log_entry_time,
    TransStatInfo &tsi)
{
  int ret = OB_SUCCESS;
  const char *buf = resp.get_log_entry_buf();
  const int64_t len = resp.get_pos();
  const int64_t log_cnt = resp.get_log_num();
  int64_t pos = 0;
  clog::ObLogEntry log_entry;
  int64_t start_read_time = get_timestamp();

  read_log_time = 0;
  decode_log_entry_time = 0;

  if (OB_ISNULL(buf)) {
    LOG_ERROR("invalid response log buf", K(buf), K(resp));
    ret = OB_ERR_UNEXPECTED;
  } else if (0 == log_cnt) {
    // Ignore 0 logs
    LOG_DEBUG("fetch 0 log", K_(svr), K_(stream_seq), "fetch_status", resp.get_fetch_status());
  } else {
    // Iterate through all log entries
    for (int64_t idx = 0; OB_SUCCESS == ret && (idx < log_cnt); ++idx) {
      int64_t begin_time = get_timestamp();
      // liboblog ignores the batch commit flag when checking checksum
      bool ignore_batch_commit_flag_when_check_integrity = true;

      log_entry.reset();

      // Deserialize log_entry
      if (OB_FAIL(log_entry.deserialize(buf, len, pos))) {
        LOG_ERROR("deserialize log entry fail", KR(ret), K(buf), K(len), K(pos),
            K_(svr), K_(stream_seq));
      }
      // Checking Integrity
      else if (OB_UNLIKELY(!log_entry.check_integrity(ignore_batch_commit_flag_when_check_integrity))) {
        LOG_ERROR("log entry check integrity fail", K(log_entry), K_(svr), K_(stream_seq),
            K(ignore_batch_commit_flag_when_check_integrity));
        ret = OB_INVALID_DATA;
      } else {
        // ObLogEntry deserialize time
        decode_log_entry_time += (get_timestamp() - begin_time);

        const clog::ObLogEntryHeader &header = log_entry.get_header();
        const ObPartitionKey &pkey = header.get_partition_key();
        uint64_t log_id = header.get_log_id();
        int64_t tstamp = header.get_submit_timestamp();
        PartFetchCtx *task = NULL;
        IObLogPartTransResolver::ObLogMissingInfo missing_logs;
        TransStatInfo local_tsi;
        const bool need_filter_pg_no_missing_redo_trans = false;
        IObLogPartTransResolver::ObAggreLogIndexArray log_indexs;

        // Filtering partition logs that need to be kicked out
        if (exist_(kick_out_set, pkey)) {
          LOG_INFO("ignore partition log entry which need kick out from current stream",
              K(pkey), K(log_id), K(tstamp), K_(svr), K_(stream_seq));
        }
        // Get the corresponding fetch log task
        else if (OB_FAIL(fetch_task_pool_.get_task(pkey, task))) {
          LOG_ERROR("get task from pool fail", KR(ret), K(pkey), K(log_entry), K_(stream_seq));
        } else if (OB_ISNULL(task)) {
          LOG_ERROR("invalid task", K(task), K(pkey), K(log_entry), K_(stream_seq));
          ret = OB_ERR_UNEXPECTED;
        }
        // The fetch log task is responsible for parsing the logs
        else if (OB_FAIL(task->read_log(log_entry, missing_logs, local_tsi, need_filter_pg_no_missing_redo_trans,
                log_indexs, stop_flag))) {
          if (OB_LOG_NOT_SYNC != ret && OB_IN_STOP_STATE != ret && OB_ITEM_NOT_SETTED != ret) {
            LOG_ERROR("fetch task read log fail", KR(ret), K(log_entry), K(missing_logs));
          } else if (OB_ITEM_NOT_SETTED == ret) {
            ret = OB_SUCCESS;

            // Handling missing redo log scenarios
            KickOutReason fail_reason = NONE;
            if (OB_FAIL(handle_missing_log_(*task, log_entry, missing_logs, stop_flag,
                fail_reason))) {
              // Processing failure, need to kick out, add to kick out set
              if (OB_NEED_RETRY == ret) {
                ret = OB_SUCCESS;
                if (OB_FAIL(set_(kick_out_set, pkey, fail_reason))) {
                  LOG_ERROR("add task into kick out set fail", KR(ret), K(pkey), K(kick_out_set),
                      K(fail_reason));
                }
              } else if (OB_IN_STOP_STATE != ret) {
                LOG_ERROR("handle missing log fail", KR(ret), KPC(task), K(missing_logs),
                    K(log_entry));
              }
            }
          }
        } else {
          // Update transaction statistics
          tsi.update(local_tsi);
        }
      }
    }
  }

  if (OB_SUCCESS == ret) {
    read_log_time = get_timestamp() - start_read_time;
  }

  return ret;
}

int FetchStream::handle_missing_log_(PartFetchCtx &task,
    const clog::ObLogEntry &prepare_log_entry,
    const IObLogPartTransResolver::ObLogMissingInfo &org_missing_logs,
    volatile bool &stop_flag,
    KickOutReason &fail_reason)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(org_missing_logs.get_missing_log_count() <= 0)) {
    LOG_ERROR("empty missing log", K(org_missing_logs));
    ret = OB_INVALID_ARGUMENT;
  } else {
    const ObLogIdArray &missing_logs = org_missing_logs.missing_log_ids_;
    int64_t stream_life_time = DEFAULT_MISSING_LOG_STREAM_LIFE_TIME;
    const ObPartitionKey &pkey = task.get_pkey();

    // The upper limit of the missing log stream is the prepare log timestamp
    int64_t upper_limit = prepare_log_entry.get_header().get_submit_timestamp();

    int64_t fetched_missing_log_cnt = 0;
    // Keep reading logs and consuming missing log arrays
    while (! stop_flag && OB_SUCCESS == ret && missing_logs.count() > fetched_missing_log_cnt) {
      ObStreamSeq seq;
      bool open_stream_succeed = false;

      // Start each time with the next missing log to be fetched
      uint64_t start_log_id = missing_logs.at(fetched_missing_log_cnt);

      // Open streams for individual partitions based on minimum log ID
      if (OB_FAIL(open_stream_(pkey, start_log_id, seq, stream_life_time,
          open_stream_succeed))) {
        LOG_ERROR("open stream fail", KR(ret), K(pkey), K(start_log_id), K(stream_life_time));
      } else if (! open_stream_succeed) {
        //  Open stream failed, need to switch server
        ret = OB_NEED_RETRY;
        fail_reason = MISSING_LOG_OPEN_STREAM_FAIL;
      }
      // Keep fetching the missing log on the current stream
      else if (OB_FAIL(fetch_missing_log_(task, seq, missing_logs, org_missing_logs.missing_trans_ids_,
          fetched_missing_log_cnt, upper_limit))) {
        if (OB_STREAM_NOT_EXIST == ret) {
          // Stream does not exist, need to reopen stream
          ret = OB_SUCCESS;
        } else if (OB_NEED_RETRY == ret) {
          // Fetching logs failed, need to switch server
          fail_reason = MISSING_LOG_FETCH_FAIL;
        } else {
          LOG_ERROR("fetch missing log fail", KR(ret), K(seq), K(task), K(missing_logs),
              K(fetched_missing_log_cnt), K(upper_limit));
        }
      }
    }

    if (stop_flag) {
      ret = OB_IN_STOP_STATE;
    }

    if (OB_SUCCESS == ret) {
      if (OB_UNLIKELY(missing_logs.count() > fetched_missing_log_cnt)) {
        LOG_ERROR("missing log not consumed all", K(missing_logs), K(fetched_missing_log_cnt));
        ret = OB_ERR_UNEXPECTED;
      } else {
        TransStatInfo tsi;
        // Missing log processing is complete, prepare log needs to be parsed again
        IObLogPartTransResolver::ObLogMissingInfo missing_info;
        const bool need_filter_pg_no_missing_redo_trans = true;

        if (OB_FAIL(task.read_log(prepare_log_entry, missing_info, tsi, need_filter_pg_no_missing_redo_trans,
                org_missing_logs.log_indexs_, stop_flag))) {
          if (OB_IN_STOP_STATE != ret) {
            LOG_ERROR("fetch task read log fail", KR(ret), K(prepare_log_entry), K(task),
                K(need_filter_pg_no_missing_redo_trans), "log_indexs", org_missing_logs.log_indexs_);
          }
        }
      }
    }
  }
  return ret;
}

int FetchStream::open_stream_(const common::ObPartitionKey &pkey,
    const uint64_t start_log_id,
    obrpc::ObStreamSeq &seq,
    const int64_t stream_life_time,
    bool &rpc_succeed)
{
  int ret = OB_SUCCESS;
  OpenStreamSRpc open_stream_rpc;
  int64_t rpc_timeout = ATOMIC_LOAD(&g_rpc_timeout);

  ObLogTraceIdGuard trace_guard;

  rpc_succeed = false;

  if (OB_ISNULL(rpc_)) {
    LOG_ERROR("invalid rpc handler", K(rpc_));
    ret = OB_INVALID_ERROR;
  }
  // Execute synchronous RPC
  else if (OB_FAIL(open_stream_rpc.open_stream(*rpc_, svr_, rpc_timeout, pkey, start_log_id,
      seq, stream_life_time))) {
    LOG_ERROR("launch open stream rpc fail", KR(ret), K(svr_), K(rpc_timeout), K(pkey),
        K(start_log_id), K(stream_life_time));
  } else {
    // Checking RPC return values
    const ObRpcResultCode &rcode = open_stream_rpc.get_result_code();
    const ObLogOpenStreamResp &resp = open_stream_rpc.get_resp();
    const ObLogOpenStreamReq &req = open_stream_rpc.get_req();

    // RPC failure
    if (OB_SUCCESS != rcode.rcode_) {
      LOG_ERROR("open stream fail on rpc", K_(svr), K(rcode), K(req), K(resp));
    } else if (OB_SUCCESS != resp.get_err()) {
      // server return error
      LOG_ERROR("open stream fail on server", K_(svr), "svr_err", resp.get_err(),
          "svr_debug_err", resp.get_debug_err(), K(rcode), K(req), K(resp));
    } else {
      rpc_succeed = true;
      seq = resp.get_stream_seq();
    }
  }
  return ret;
}

int FetchStream::fetch_missing_log_(PartFetchCtx &task,
    const obrpc::ObStreamSeq &seq,
    const ObLogIdArray &missing_logs,
    const IObLogPartTransResolver::ObTransIDArray &missing_log_trans_id_array,
    int64_t &fetched_missing_log_cnt,
    const int64_t upper_limit)
{
  int ret = OB_SUCCESS;
  // Use synchronous RPC method to fetch MISSING LOG
  FetchLogSRpc *fetch_log_srpc = NULL;
  int64_t rpc_timeout = ATOMIC_LOAD(&g_rpc_timeout);

  if (OB_ISNULL(rpc_)) {
    LOG_ERROR("invalid rpc handler", K(rpc_));
    ret = OB_INVALID_ERROR;
  } else if (OB_FAIL(alloc_fetch_log_srpc_(fetch_log_srpc))) {
    LOG_ERROR("alloc fetch log srpc fail", KR(ret));
  } else if (OB_ISNULL(fetch_log_srpc)) {
    LOG_ERROR("invalid fetch_log_srpc", K(fetch_log_srpc));
    ret = OB_ERR_UNEXPECTED;
  } else {
    while (OB_SUCCESS == ret && missing_logs.count() > fetched_missing_log_cnt) {
      uint64_t max_log_id = missing_logs.at(missing_logs.count() - 1);
      uint64_t min_log_id = missing_logs.at(fetched_missing_log_cnt);

      // Maximum number of logs fetched
      int64_t fetch_log_cnt = max_log_id - min_log_id + 1;

      // Launch rpc
      if (OB_FAIL(fetch_log_srpc->fetch_log(*rpc_, svr_, rpc_timeout, seq, upper_limit,
          fetch_log_cnt, false))) {
        LOG_ERROR("launch fetch log fail", KR(ret), K(seq), K(upper_limit), K(fetch_log_cnt));
      } else {
        const obrpc::ObRpcResultCode &rcode = fetch_log_srpc->get_result_code();
        const obrpc::ObLogStreamFetchLogResp &resp = fetch_log_srpc->get_resp();

        // RPC failure, need to switch server
        if (OB_SUCCESS != rcode.rcode_) {
          LOG_ERROR("fetch log fail on rpc", K_(svr), K(rcode), K(seq));
          ret = OB_NEED_RETRY;
        }
        // server return fail
        else if (OB_SUCCESS != resp.get_err()) {
          // Stream does not exist, reopen the stream
          if (OB_STREAM_NOT_EXIST == resp.get_err()) {
            LOG_WARN("fetch missing log fail on server, stream not exist", K_(svr),
                "svr_err", resp.get_err(), "svr_debug_err", resp.get_debug_err(),
                K(seq), K(rcode), K(resp));
            ret = OB_STREAM_NOT_EXIST;
          } else {
            // Other error, need to switch server
            LOG_ERROR("fetch missing log fail on server", K_(svr), "svr_err", resp.get_err(),
                "svr_debug_err", resp.get_debug_err(), K(seq), K(rcode), K(resp));
            ret = OB_NEED_RETRY;
          }
        }
        // Fetch log successfully
        else if (OB_FAIL(read_missing_log_(task, resp, missing_logs, missing_log_trans_id_array, fetched_missing_log_cnt))) {
          LOG_ERROR("read missing log fail", KR(ret), K(resp), K(missing_logs), K(missing_log_trans_id_array),
              K(fetched_missing_log_cnt), K(task));
        }
      }
    }
  }

  if (NULL != fetch_log_srpc) {
    free_fetch_log_srpc_(fetch_log_srpc);
    fetch_log_srpc = NULL;
  }

  return ret;
}

int FetchStream::alloc_fetch_log_srpc_(FetchLogSRpc *&fetch_log_srpc)
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

void FetchStream::free_fetch_log_srpc_(FetchLogSRpc *fetch_log_srpc)
{
  if (NULL != fetch_log_srpc) {
    fetch_log_srpc->~FetchLogSRpc();
    ob_free(fetch_log_srpc);
    fetch_log_srpc = NULL;
  }
}

int FetchStream::read_missing_log_(PartFetchCtx &task,
    const ObLogStreamFetchLogResp &resp,
    const ObLogIdArray &missing_logs,
    const IObLogPartTransResolver::ObTransIDArray &missing_log_trans_id_array,
    int64_t &fetched_missing_log_cnt)
{
  int ret = OB_SUCCESS;
  const char *buf = resp.get_log_entry_buf();
  int64_t len = resp.get_pos();
  int64_t pos = 0;
  const int64_t log_cnt = resp.get_log_num();
  clog::ObLogEntry log_entry;
  // liboblog should ignore the batch commit flag when checking checksum
  bool ignore_batch_commit_flag_when_check_integrity = true;

  if (OB_UNLIKELY(missing_logs.count() <= fetched_missing_log_cnt)) {
    LOG_ERROR("invalid missing_logs", K(missing_logs), K(fetched_missing_log_cnt));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(buf)) {
    LOG_ERROR("invalid response log buf", K(buf), K(resp));
    ret = OB_ERR_UNEXPECTED;
  } else if (0 == log_cnt) {
    // Ignore 0 logs
    // TODO: Adopt a policy to handle 0 log scenarios, or switch server if there are always 0 logs
    LOG_INFO("fetch 0 missing log", K_(svr), K_(stream_seq),
        "fetch_status", resp.get_fetch_status(), K(missing_logs), K(fetched_missing_log_cnt));
  } else {
    // Iterate through all log entries and select the required redo logs
    // Since missing_logs are sorted from smallest to largest, just compare the logs in order
    for (int64_t idx = 0;
        OB_SUCCESS == ret && (idx < log_cnt) && missing_logs.count() > fetched_missing_log_cnt;
        ++idx) {
      uint64_t next_missing_log_id = missing_logs.at(fetched_missing_log_cnt);
      log_entry.reset();

      // Deserialize log_entry
      if (OB_FAIL(log_entry.deserialize(buf, len, pos))) {
        LOG_ERROR("deserialize log entry fail", KR(ret), K(buf), K(len), K(pos),
            K_(svr), K_(stream_seq));
      }
      // Check intergrity
      else if (OB_UNLIKELY(!log_entry.check_integrity(ignore_batch_commit_flag_when_check_integrity))) {
        LOG_ERROR("log entry check integrity fail", K(log_entry), K_(svr), K_(stream_seq),
            K(ignore_batch_commit_flag_when_check_integrity));
        ret = OB_INVALID_DATA;
      }
      // Check partition key
      else if (OB_UNLIKELY(log_entry.get_header().get_partition_key() != task.get_pkey())) {
        LOG_ERROR("invalid log, partition key does not match",
            "log_pkey", log_entry.get_header().get_partition_key(),
            "asked_pkey", task.get_pkey(), K(resp));
        ret = OB_INVALID_DATA;
      } else if (OB_UNLIKELY(log_entry.get_header().get_log_id() > next_missing_log_id)) {
        // The log ID should not be larger than the next missing log
        LOG_ERROR("log id is greater than next missing log id",
            "log_id", log_entry.get_header().get_log_id(),
            K(next_missing_log_id), K(log_entry), K(missing_logs), K(fetched_missing_log_cnt));
        ret = OB_ERR_UNEXPECTED;
      } else if (log_entry.get_header().get_log_id() < next_missing_log_id) {
        // Filtering unwanted logs
      }
      // Read the next missing log
      else if (OB_FAIL(task.read_missing_redo(log_entry, missing_log_trans_id_array))) {
        LOG_ERROR("read missing redo fail", KR(ret), K(log_entry), K(missing_log_trans_id_array), K(next_missing_log_id));
      } else {
        // success
        fetched_missing_log_cnt++;
      }
    }
  }
  return ret;
}

int FetchStream::kick_out_task_(const KickOutTaskSet &kick_out_set)
{
  int ret = OB_SUCCESS;

  KickOutTaskSet::const_iterator_t iter = kick_out_set.begin();

  // Iterate through the collection, remove the partition fetch log task from the task pool, and assign it to another stream
  for (; (OB_SUCCESS == ret && iter != kick_out_set.end()); ++iter) {
    PartFetchCtx *task = NULL;
    if (OB_FAIL(fetch_task_pool_.kick_out_task((*iter).pkey_, task))) {
      LOG_ERROR("kick out task from pool fail", KR(ret), K(*iter), K(kick_out_set));
    } else if (OB_ISNULL(task)) {
      LOG_ERROR("invalid task", K(task));
      ret = OB_INVALID_ERROR;
    } else if (OB_FAIL(dispatch_fetch_task_(*task,
          (*iter).kick_out_reason_))) {
      LOG_ERROR("dispatch fetch task fail", KR(ret), KPC(task), K((*iter).kick_out_reason_));
    } else {
      task = NULL;
    }
  }

  return ret;
}

FetchStream::KickOutReason FetchStream::get_feedback_reason_(const Feedback &feedback) const
{
  // Get KickOutReason based on feedback
  KickOutReason reason = NONE;
  switch (feedback.feedback_type_) {
    case ObLogStreamFetchLogResp::LAGGED_FOLLOWER:
      reason = LAGGED_FOLLOWER;
      break;

    case ObLogStreamFetchLogResp::LOG_NOT_IN_THIS_SERVER:
      reason = LOG_NOT_IN_THIS_SERVER;
      break;

    case ObLogStreamFetchLogResp::PARTITION_OFFLINED:
      reason = PARTITION_OFFLINED;
      break;

    default:
      reason = NONE;
      break;
  }

  return reason;
}

int FetchStream::check_feedback_(const ObLogStreamFetchLogResp &resp,
    KickOutTaskSet &kick_out_set)
{
  int ret = OB_SUCCESS;
  int64_t feedback_cnt = resp.get_feedback_array().count();

  for (int64_t idx = 0; OB_SUCCESS == ret && (idx < feedback_cnt); ++idx) {
    const Feedback &feedback = resp.get_feedback_array().at(idx);
    KickOutReason reason = get_feedback_reason_(feedback);

    // Kick out all the partitions in the feedback, but not the NONE
    if (reason != NONE) {
      if (OB_FAIL(set_(kick_out_set, feedback.pkey_, reason))) {
        if (OB_HASH_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_ERROR("set pkey into kick out set fail", KR(ret), K(feedback), K(kick_out_set));
        }
      }
    }
  }
  return ret;
}

int FetchStream::check_fetch_log_heartbeat_(const ObLogStreamFetchLogResp &resp,
    KickOutTaskSet &kick_out_set)
{
  int ret = OB_SUCCESS;
  int64_t hb_cnt = resp.get_hb_array().count();
  typedef obrpc::ObLogStreamFetchLogResp::FetchLogHeartbeatItem Heartbeat;

  // Update the progress of each partition
  for (int64_t idx = 0; OB_SUCCESS == ret && (idx < hb_cnt); ++idx) {
    const Heartbeat &hb = resp.get_hb_array().at(idx);
    PartFetchCtx *task = NULL;

    if (exist_(kick_out_set, hb.pkey_)) {
      // Ignore the partitions that will be kicked out
    } else if (OB_FAIL(fetch_task_pool_.get_task(hb.pkey_, task))) {
      LOG_ERROR("get task from pool fail", KR(ret), K(hb.pkey_), K_(stream_seq));
    } else if (OB_ISNULL(task)) {
      LOG_ERROR("invalid task", K(task), K(hb.pkey_), K_(stream_seq));
      ret = OB_ERR_UNEXPECTED;
    }
    // Update progress based on log heartbeat
    else if (OB_FAIL(task->update_log_heartbeat(hb.next_log_id_, hb.heartbeat_ts_))) {
      if (OB_LOG_NOT_SYNC != ret) {
        LOG_ERROR("update log heartbeat fail", KR(ret), K(hb), KPC(task));
      }
    } else {
      // success
    }
  }
  return ret;
}

int FetchStream::update_fetch_task_state_(KickOutTaskSet &kick_out_set,
    volatile bool &stop_flag,
    PartFetchCtx *&min_progress_task,
    int64_t &flush_time)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(heartbeater_) || OB_ISNULL(svr_finder_)) {
    LOG_ERROR("invalid handlers", K(heartbeater_), K(svr_finder_));
    ret = OB_INVALID_ERROR;
  } else {
    FetchTaskList &task_list = fetch_task_pool_.get_task_list();
    PartFetchCtx *task = task_list.head();
    int64_t min_progress = OB_INVALID_TIMESTAMP;
    bool need_check_switch_server = check_need_switch_server_();

    // Check each of the fetch log tasks and update their status
    while (OB_SUCCESS == ret && NULL != task) {
      // If the task is deleted, it is kicked out directly
      if (OB_UNLIKELY(task->is_discarded())) {
        LOG_INFO("[STAT] [FETCH_STREAM] [RECYCLE_FETCH_TASK]", "fetch_task", task,
            "fetch_stream", this, KPC(task));
        if (OB_FAIL(set_(kick_out_set, task->get_pkey(), DISCARDED))) {
          if (OB_HASH_EXIST == ret) {
            // Already exists, ignore
            ret = OB_SUCCESS;
          } else {
            LOG_ERROR("set into kick out set fail", KR(ret), K(task->get_pkey()), K(kick_out_set));
          }
        }
      } else {
        // Periodic update of leader information
        if (task->need_update_leader_info()) {
          if (OB_FAIL(task->update_leader_info(*svr_finder_))) {
            LOG_ERROR("update leader info fail", KR(ret), KPC(task));
          }
        }

        // Update heartbeat progress for needed tasks
        if (task->need_heartbeat(upper_limit_)) {
          if (OB_FAIL(task->update_heartbeat_info(*heartbeater_, *svr_finder_))) {
            LOG_ERROR("update heartbeat info fail", KR(ret), KPC(task));
          }
        }

        // Check if the progress is greater than the upper limit, and update the touch timestamp if it is greater than the upper limit
        // Avoid progress of partitions is not updated that progress larger than upper_limit, which will be misjudged as progress timeout in the future
        if (OB_SUCCESS == ret) {
          task->update_touch_tstamp_if_progress_beyond_upper_limit(upper_limit_);
        }

        // Update each partition's progress to the global
        if (OB_SUCCESS == ret && OB_FAIL(publish_progress_(*task))) {
          LOG_ERROR("update progress fail", KR(ret), K(task), KPC(task));
        }

        // Check if the server list needs to be updated
        if (OB_SUCCESS == ret && task->need_update_svr_list()) {
          bool need_print_info = (TCONF.print_partition_server_list_update_info != 0);
          if (OB_FAIL(task->update_svr_list(*svr_finder_, need_print_info))) {
            LOG_ERROR("update svr list fail", KR(ret), K(svr_finder_), KPC(task));
          }
        }

        // Check if the log fetch timeout on the current server, and add the timeout tasks to the kick out collection
        if (OB_SUCCESS == ret && OB_FAIL(check_fetch_timeout_(*task, kick_out_set))) {
          LOG_ERROR("check fetch timeout fail", KR(ret), K(task), KPC(task), K(kick_out_set));
        }

        // Periodically check if there is a server with a higher level of excellence at this time, and if so, add the task to the kick out set for active flow cutting
        if (need_check_switch_server) {
          if (OB_SUCCESS == ret && OB_FAIL(check_switch_server_(*task, kick_out_set))) {
            LOG_ERROR("check switch server fail", KR(ret), K(task), KPC(task), K(kick_out_set));
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


        if (OB_SUCCESS == ret) {
          int64_t progress = task->get_progress();
          if (OB_INVALID_TIMESTAMP == min_progress || progress < min_progress) {
            min_progress_task = task;
            min_progress = progress;
          }
        }
      }

      if (OB_SUCCESS == ret) {
        task = task->get_next();
      }
    }
  }

  return ret;
}

int FetchStream::publish_progress_(PartFetchCtx &task)
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

int FetchStream::check_fetch_timeout_(PartFetchCtx &task, KickOutTaskSet &kick_out_set)
{
  int ret = OB_SUCCESS;
  bool is_fetch_timeout = false;
  bool is_fetch_timeout_on_lagged_replica = false;
  // For lagging replica, the timeout of partition
  int64_t fetcher_resume_tstamp = OB_INVALID_TIMESTAMP;

  if (OB_ISNULL(stream_worker_)) {
    LOG_ERROR("invalid stream worker", K(stream_worker_));
    ret = OB_INVALID_ERROR;
  } else {
    fetcher_resume_tstamp = stream_worker_->get_fetcher_resume_tstamp();

    if (OB_FAIL(task.check_fetch_timeout(svr_, upper_limit_, fetcher_resume_tstamp,
        is_fetch_timeout, is_fetch_timeout_on_lagged_replica))) {
      LOG_ERROR("check fetch timeout fail", KR(ret), K(svr_),
          K(upper_limit_), "fetcher_resume_tstamp", TS_TO_STR(fetcher_resume_tstamp), K(task));
    } else if (is_fetch_timeout) {
      KickOutReason reason = is_fetch_timeout_on_lagged_replica ? PROGRESS_TIMEOUT_ON_LAGGED_REPLICA : PROGRESS_TIMEOUT;
      // If the partition fetch log times out, add it to the kick out collection
      if (OB_FAIL(set_(kick_out_set, task.get_pkey(), reason))) {
        if (OB_HASH_EXIST == ret) {
          // Already exists, ignore
          ret = OB_SUCCESS;
        } else {
          LOG_ERROR("set into kick out set fail", KR(ret), K(task.get_pkey()), K(kick_out_set),
              K(reason));
        }
      }
    }
  }

  return ret;
}

int FetchStream::check_switch_server_(PartFetchCtx &task, KickOutTaskSet &kick_out_set)
{
  int ret = OB_SUCCESS;

  if (exist_(kick_out_set, task.get_pkey())) {
    // Do not check for partitions already located in kick_out_set
  } else if (task.need_switch_server(svr_)) {
    LOG_DEBUG("exist higher priority server, need switch server", KR(ret), "pkey", task.get_pkey(),
             "cur_svr", svr_);
    // If you need to switch the stream, add it to the kick out collection
    if (OB_FAIL(set_(kick_out_set, task.get_pkey(), NEED_SWITCH_SERVER))) {
      if (OB_HASH_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_ERROR("set into kick out set fail", KR(ret), K(task.get_pkey()), K(kick_out_set));
      }
    }
  } else {
    // do nothing
  }

  return ret;
}

/////////////////////////////// FetchTaskPool /////////////////////////////

const char *FetchStream::FetchTaskPool::print_state(State state)
{
  const char *str = "UNKNOWN";
  switch (state) {
    case IDLE:
      str = "IDLE";
      break;
    case READY:
      str = "READY";
      break;
    case HANDLING:
      str = "HANDLING";
      break;
    default:
      str = "UNKNOWN";
      break;
  }
  return str;
}

FetchStream::FetchTaskPool::FetchTaskPool() :
    state_(IDLE),
    map_(),
    queue_(),
    total_cnt_(0),
    list_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(queue_.init(ObModIds::OB_LOG_FETCH_STREAM_PART_TASK_QUEUE))) {
    LOG_ERROR("init task queue fail", KR(ret));
  } else if (OB_FAIL(map_.init(ObModIds::OB_LOG_FETCH_STREAM_PART_TASK_MAP))) {
    LOG_ERROR("init task map fail", KR(ret));
  }
}

FetchStream::FetchTaskPool::~FetchTaskPool()
{
  reset();

  queue_.destroy();
  (void)map_.destroy();
}

void FetchStream::FetchTaskPool::reset()
{
  state_ = IDLE;
  map_.reset();
  queue_.reset();
  total_cnt_ = 0;
  list_.reset();
}

int FetchStream::FetchTaskPool::push(PartFetchCtx &task, bool &is_pool_idle)
{
  int ret = OB_SUCCESS;
  const ObPartitionKey &pkey = task.get_pkey();

  // First increase the total number of tasks, and if the number of tasks has reached the limit, return directly
  if (OB_FAIL(inc_total_cnt_(1))) {
    if (OB_SIZE_OVERFLOW != ret) {
      LOG_ERROR("inc_total_cnt_ fail", KR(ret));
    }
  }
  // Insert into MAP first
  else if (OB_FAIL(map_.insert(pkey, &task))) {
    LOG_ERROR("insert task into map fail", KR(ret), K(pkey), K(task));
  }
  // Push queue
  else if (OB_FAIL(queue_.push(&task))) {
    LOG_ERROR("push task into queue fail", KR(ret), K(&task));
    (void)map_.erase(pkey);
  } else {
    is_pool_idle = false;

    // Unconditionally set to READY state
    if ((ATOMIC_LOAD(&state_) != READY)) {
      // Verify that the previous state is IDLE
      is_pool_idle = (ATOMIC_SET(&state_, READY) == IDLE);
    }
  }
  return ret;
}

int FetchStream::FetchTaskPool::kick_out_task_list(FetchTaskList &task_list)
{
  int ret = OB_SUCCESS;
  PartFetchCtx *task = list_.head();

  // Remove the corresponding Task from the Map
  while (OB_SUCCESS == ret && NULL != task) {
    if (OB_FAIL(map_.erase(task->get_pkey()))) {
      LOG_ERROR("erase task from map fail", KR(ret), "pkey", task->get_pkey(), KPC(task));
    } else {
      task = task->get_next();
    }
  }

  if (OB_SUCCESS == ret) {
    int64_t part_count = list_.count();

    // Modify the total number of fetch log tasks
    if (part_count > 0 && OB_FAIL(dec_total_cnt_(part_count))) {
      LOG_ERROR("decrease total cnt fail", KR(ret), K(part_count));
    }
  }

  if (OB_SUCCESS == ret) {
    // Copy Fetch Log Task List
    task_list = list_;

    // Reset the list of fetch log tasks
    list_.reset();
  }

  return ret;
}

int FetchStream::FetchTaskPool::kick_out_task(const common::ObPartitionKey &pkey,
    PartFetchCtx *&task)
{
  int ret = OB_SUCCESS;

  task = NULL;
  // Delete from Map
  if (OB_FAIL(map_.erase(pkey, task))) {
    LOG_ERROR("erase from map fail", KR(ret), K(pkey));
  } else if (OB_ISNULL(task)) {
    LOG_ERROR("invalid task", K(task), K(pkey));
    ret = OB_ERR_UNEXPECTED;
  } else {
    // Delete from list
    list_.erase(*task);

    // Modify the total number of fetch log tasks
    if (OB_FAIL(dec_total_cnt_(1))) {
      LOG_ERROR("decrease total count fail", KR(ret));
    } else {
      // success
    }
  }

  return ret;
}

int FetchStream::FetchTaskPool::update(bool &pool_become_idle, bool &task_list_changed)
{
  int ret = OB_SUCCESS;
  task_list_changed = false;
  pool_become_idle = false;

  do {
    // First set the status to HANDLING
    (void)ATOMIC_SET(&state_, HANDLING);

    // Then the data in the queue is moved into the linklist
    while (OB_SUCCESS == ret) {
      PartFetchCtx *task = NULL;

      if (OB_FAIL(queue_.pop(task))) {
        if (OB_EAGAIN != ret) {
          LOG_ERROR("pop task from queue fail", KR(ret));
        }
      } else if (OB_ISNULL(task)) {
        LOG_ERROR("invalid task", K(task));
        ret = OB_ERR_UNEXPECTED;
      } else {
        list_.add_head(*task);
        task_list_changed = true;
      }
    }

    if (OB_EAGAIN == ret) {
      ret = OB_SUCCESS;
    }

    if (OB_SUCCESS == ret) {
      // The queue has been emptied above, if the linklist is also empty, there is no task; try to set the status to IDLE
      // Only when the status is successfully changed from HANDLING to IDLE, it really means that both the queue and the linklist are empty, otherwise a new task may be inserted
      // Note: When the state is changed from HANDLING to IDLE, no further operations can be performed on any data structure, as a thread may have already taken over the data structure.
      if (list_.count() <= 0) {
        if (HANDLING == ATOMIC_CAS(&state_, HANDLING, IDLE)) {
          pool_become_idle = true;
        }
      }
    }
  } while (OB_SUCCESS == ret && list_.count() <= 0 && ! pool_become_idle);

  return ret;
}

int FetchStream::FetchTaskPool::get_task(const common::ObPartitionKey &pkey,
    PartFetchCtx *&task) const
{
  return map_.get(pkey, task);
}

int FetchStream::FetchTaskPool::inc_total_cnt_(const int64_t cnt)
{
  int ret = OB_SUCCESS;
  int64_t max_part_count = ATOMIC_LOAD(&FetchStream::g_stream_max_part_count);
  int64_t cur_cnt = ATOMIC_LOAD(&total_cnt_);
  int64_t old_cnt = 0;
  int64_t new_cnt = 0;

  do {
    old_cnt = cur_cnt;
    new_cnt = (old_cnt + cnt);

    // When increasing the number of partitions, check if the upper limit is reached
    if (cnt > 0 && old_cnt >= max_part_count) {
      ret = OB_SIZE_OVERFLOW;
    }
  } while (OB_SUCCESS == ret
      && (old_cnt != (cur_cnt = ATOMIC_CAS(&total_cnt_, old_cnt, new_cnt))));

  return ret;
}

int FetchStream::FetchTaskPool::dec_total_cnt_(const int64_t cnt)
{
  return inc_total_cnt_(-cnt);
}

}
}
