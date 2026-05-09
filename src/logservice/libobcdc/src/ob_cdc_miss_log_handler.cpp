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
 * OBCDC header file
 * This file defines interface of OBCDC
 */

#define USING_LOG_PREFIX OBLOG_FETCHER


#include "ob_cdc_miss_log_handler.h"
#include "ob_log_config.h"
#include "ob_log_trace_id.h"            // ObLogTraceIdGuard
#include "ob_log_utils.h"
#include "lib/thread/threads.h"
#include "lib/random/ob_random.h"
#include "logservice/ipalf/ipalf_log_group_entry.h"
#include "logservice/ipalf/ipalf_log_entry.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace libobcdc
{

////////////////////////////// File-scope helpers //////////////////////////////

// Core logic for deserializing and resolving a batch of miss log entries from
// the RPC response buffer. Extracted as a free function so that MissLogBatchReader
// (worker thread) can call it without accessing ObCDCMissLogHandler private members.
static int do_read_batch_misslog(
    LSFetchCtx &ls_fetch_ctx,
    const obrpc::ObCdcLSFetchLogResp &resp,
    int64_t &fetched_missing_log_cnt,
    logfetcher::TransStatInfo &tsi,
    IObCDCPartTransResolver::MissingLogInfo &missing_info)
{
  int ret = OB_SUCCESS;
  LOG_INFO("read_batch_misslog begin", "tls_id", ls_fetch_ctx.get_tls_id(), K(resp), K(fetched_missing_log_cnt));

  const int64_t total_misslog_cnt = missing_info.get_total_misslog_cnt();
  const char *buf = resp.get_log_entry_buf();
  const int64_t len = resp.get_pos();
  int64_t pos = 0;
  const int64_t log_cnt = resp.get_log_num();
  const ObLogLSNArray &org_misslog_arr = missing_info.get_miss_redo_lsn_arr();
  int64_t start_ts = get_timestamp();

  if (OB_UNLIKELY(log_cnt <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("expected valid log count from FetchLogSRpc for misslog", KR(ret), K(resp));
  } else {
    for (int64_t idx = 0; OB_SUCC(ret) && idx < log_cnt; idx++) {
      if (fetched_missing_log_cnt >= total_misslog_cnt) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("fetched_missing_log_cnt is more than total_misslog_cnt", KR(ret),
            K(fetched_missing_log_cnt), K(missing_info), K(idx), K(resp));
      } else {
        palf::LSN misslog_lsn;
        bool enable_logservice = ls_fetch_ctx.get_logservice_model();
        ipalf::ILogEntry miss_log_entry(enable_logservice);
        misslog_lsn.reset();
        miss_log_entry.reset();
        IObCDCPartTransResolver::MissingLogInfo tmp_miss_info;
        tmp_miss_info.set_resolving_miss_log();

        if (org_misslog_arr.count() == fetched_missing_log_cnt) {
          if (OB_FAIL(missing_info.get_miss_record_or_state_log_lsn(misslog_lsn))) {
            if (OB_ENTRY_NOT_EXIST == ret) {
              LOG_ERROR("expect valid miss-record_log_lsn", KR(ret), K(missing_info), K(fetched_missing_log_cnt), K(ls_fetch_ctx));
            } else {
              LOG_ERROR("get_miss_record_or_state_log_lsn failed", KR(ret), K(missing_info), K(fetched_missing_log_cnt), K(ls_fetch_ctx));
            }
          }
        } else if (OB_FAIL(org_misslog_arr.at(fetched_missing_log_cnt, misslog_lsn))) {
          LOG_ERROR("get misslog_lsn fail", KR(ret), K(fetched_missing_log_cnt),
              K(idx), K(org_misslog_arr), K(resp));
        }

        if (FAILEDx(miss_log_entry.deserialize(misslog_lsn, buf, len, pos))) {
          LOG_ERROR("deserialize miss_log_entry fail", KR(ret), K(len), K(pos));
        } else if (OB_FAIL(ls_fetch_ctx.read_miss_tx_log(miss_log_entry, misslog_lsn, tsi, tmp_miss_info))) {
          LOG_ERROR("read_miss_log fail", KR(ret), K(miss_log_entry),
              K(misslog_lsn), K(fetched_missing_log_cnt), K(idx), K(tmp_miss_info));
        } else {
          fetched_missing_log_cnt++;
          missing_info.set_last_misslog_progress(tmp_miss_info.get_last_misslog_progress());
        }
      }
    }
  }

  int64_t read_batch_missing_cost = get_timestamp() - start_ts;
  const int64_t handle_miss_progress = missing_info.get_last_misslog_progress();
  ObCStringHelper helper;
  _LOG_INFO("[MISS_LOG][READ_MISSLOG][PART_TRANS_ID=%s][COST=%ld][PROGRESS_CNT=%ld/%ld][PROGRESS_SCN=%ld(%s)]",
      helper.convert(missing_info.get_part_trans_id()),
      read_batch_missing_cost,
      fetched_missing_log_cnt,
      org_misslog_arr.count(),
      handle_miss_progress,
      NTS_TO_STR(handle_miss_progress));

  return ret;
}

////////////////////////////// MissLogBatchReader //////////////////////////////
//
// Lightweight worker thread for async batch processing in handle_miss_redo_log_.
// Decouples the heavy read_batch_misslog (deserialize + resolve) from the RPC loop,
// so that RPC round-trips and log processing run in parallel.
//
// Lifecycle: created on stack in handle_miss_redo_log_, destroyed when it returns.
//
// Usage pattern (main thread):
//   reader.init();
//   while (more_batches) {
//     fetch_miss_log_with_retry_(batch_N, srpc_A);  // blocking RPC, overlaps with worker
//     reader.wait_task();                            // ensure previous batch done
//     reader.post_task(srpc_A.resp);                 // hand off to worker
//     swap(srpc_A, srpc_B);                          // free srpc_A for next RPC
//   }
//   reader.wait_task();                              // wait for last batch
//   reader.destroy();

class MissLogBatchReader : public lib::Threads
{
public:
  MissLogBatchReader()
    : lib::Threads(1),
      task_cond_(ObCond::SPIN_WAIT_NUM, ObWaitEventIds::CDC_COMMON_COND_WAIT),
      done_cond_(ObCond::SPIN_WAIT_NUM, ObWaitEventIds::CDC_COMMON_COND_WAIT),
      ls_fetch_ctx_(nullptr), resp_(nullptr),
      fetched_cnt_(nullptr), tsi_(nullptr), missing_info_(nullptr),
      has_task_(false), task_done_(true),
      task_ret_(OB_SUCCESS), started_(false)
  {}

  ~MissLogBatchReader() { destroy(); }

  int init()
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(started_)) {
      ret = OB_INIT_TWICE;
    } else if (OB_FAIL(lib::Threads::start())) {
      LOG_WARN("[MISS_LOG][PIPELINE] create batch reader thread failed", KR(ret));
    } else {
      started_ = true;
      LOG_INFO("[MISS_LOG][PIPELINE] batch reader thread started");
    }
    return ret;
  }

  void destroy()
  {
    if (started_) {
      lib::Threads::stop();
      task_cond_.signal();
      lib::Threads::destroy();
      started_ = false;
      LOG_INFO("[MISS_LOG][PIPELINE] batch reader thread stopped");
    }
  }

  void post_task(
      LSFetchCtx &ls_fetch_ctx,
      const obrpc::ObCdcLSFetchLogResp &resp,
      int64_t &fetched_cnt,
      logfetcher::TransStatInfo &tsi,
      IObCDCPartTransResolver::MissingLogInfo &missing_info)
  {
    ls_fetch_ctx_ = &ls_fetch_ctx;
    resp_ = &resp;
    fetched_cnt_ = &fetched_cnt;
    tsi_ = &tsi;
    missing_info_ = &missing_info;
    task_ret_ = OB_SUCCESS;
    ATOMIC_SET(&task_done_, false);
    ATOMIC_SET(&has_task_, true);
    task_cond_.signal();
  }

  int wait_task()
  {
    while (!ATOMIC_LOAD(&task_done_) && !has_set_stop()) {
      done_cond_.wait();
    }
    return task_ret_;
  }

  bool has_pending_task() const { return !ATOMIC_LOAD(&task_done_); }

  void run(int64_t idx) override
  {
    UNUSED(idx);
    lib::set_thread_name("MissLogReader");
    while (!has_set_stop()) {
      while (!ATOMIC_LOAD(&has_task_) && !has_set_stop()) {
        task_cond_.wait();
      }
      if (has_set_stop()) {
        break;
      }
      ObLogTraceIdGuard trace_guard;
      task_ret_ = do_read_batch_misslog(*ls_fetch_ctx_, *resp_, *fetched_cnt_, *tsi_, *missing_info_);
      ATOMIC_SET(&has_task_, false);
      ATOMIC_SET(&task_done_, true);
      done_cond_.signal();
    }
  }

private:
  common::ObCond task_cond_;
  common::ObCond done_cond_;

  LSFetchCtx *ls_fetch_ctx_;
  const obrpc::ObCdcLSFetchLogResp *resp_;
  int64_t *fetched_cnt_;
  logfetcher::TransStatInfo *tsi_;
  IObCDCPartTransResolver::MissingLogInfo *missing_info_;

  volatile bool has_task_ CACHE_ALIGNED;
  volatile bool task_done_ CACHE_ALIGNED;
  int task_ret_;

  bool started_;
};
////////////////////////////// Parallel read helper //////////////////////////////

static int do_read_batch_misslog_for_worker(
    LSFetchCtx &ls_fetch_ctx,
    const obrpc::ObCdcLSFetchLogResp &resp,
    const ObLogLSNArray &miss_arr,
    const int64_t rpc_start_idx,
    logfetcher::TransStatInfo &tsi,
    int64_t &max_progress)
{
  int ret = OB_SUCCESS;
  const char *buf = resp.get_log_entry_buf();
  const int64_t len = resp.get_pos();
  int64_t pos = 0;
  const int64_t log_cnt = resp.get_log_num();
  const bool enable_logservice = ls_fetch_ctx.get_logservice_model();

  for (int64_t idx = 0; OB_SUCC(ret) && idx < log_cnt; idx++) {
    const int64_t arr_idx = rpc_start_idx + idx;
    palf::LSN misslog_lsn;

    if (OB_UNLIKELY(arr_idx >= miss_arr.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("[MISS_LOG][PARALLEL] arr_idx out of bounds", KR(ret),
          K(arr_idx), "miss_arr_cnt", miss_arr.count());
    } else {
      misslog_lsn = miss_arr.at(arr_idx);
      ipalf::ILogEntry miss_log_entry(enable_logservice);
      miss_log_entry.reset();
      IObCDCPartTransResolver::MissingLogInfo tmp_miss_info;
      tmp_miss_info.set_resolving_miss_log();

      if (OB_FAIL(miss_log_entry.deserialize(misslog_lsn, buf, len, pos))) {
        LOG_ERROR("[MISS_LOG][PARALLEL] deserialize fail", KR(ret), K(misslog_lsn), K(arr_idx));
      } else if (OB_FAIL(ls_fetch_ctx.read_miss_tx_log(
          miss_log_entry, misslog_lsn, tsi, tmp_miss_info))) {
        LOG_ERROR("[MISS_LOG][PARALLEL] read_miss_tx_log fail", KR(ret), K(misslog_lsn), K(arr_idx));
      } else {
        const int64_t progress = tmp_miss_info.get_last_misslog_progress();
        if (progress > max_progress) {
          max_progress = progress;
        }
      }
    }
  }

  return ret;
}

////////////////////////////// MissLogParallelFetcher //////////////////////////////

class MissLogParallelFetcher : public lib::Threads
{
  static const int64_t DEFAULT_BATCH_SIZE = 200;

  struct WorkerCtx {
    FetchLogSRpc *srpc_;
    ObArrayImpl<obrpc::ObCdcLSFetchMissLogReq::MissLogParam> batch_;
    int64_t fetched_cnt_;
    int64_t max_progress_;
    int64_t fetched_bytes_;
    int64_t rpc_count_;
    volatile bool done_ CACHE_ALIGNED;
    int ret_code_;
    common::ObAddr svr_;
    bool need_change_server_;

    WorkerCtx()
      : srpc_(nullptr), batch_(), fetched_cnt_(0), max_progress_(0),
        fetched_bytes_(0), rpc_count_(0),
        done_(false), ret_code_(OB_SUCCESS), svr_(), need_change_server_(false) {}
    ~WorkerCtx() {}
  };

public:
  MissLogParallelFetcher()
    : lib::Threads(1),
      n_workers_(0), n_constructed_(0), workers_(nullptr),
      miss_arr_(nullptr), total_cnt_(0), task_(nullptr), stop_flag_(nullptr),
      next_batch_idx_(0), batch_size_(DEFAULT_BATCH_SIZE),
      done_count_(0), done_cond_(ObCond::SPIN_WAIT_NUM, ObWaitEventIds::CDC_COMMON_COND_WAIT),
      process_lock_(0), has_error_(0),
      inited_(false) {}

  ~MissLogParallelFetcher() { destroy(); }

  int init(const int64_t n_workers, const uint64_t tenant_id,
           const ObLogLSNArray &miss_arr, const int64_t total_cnt,
           MissLogTask &task, volatile bool &stop_flag)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(inited_)) {
      ret = OB_INIT_TWICE;
    } else {
      n_workers_ = n_workers;
      miss_arr_ = &miss_arr;
      total_cnt_ = total_cnt;
      task_ = &task;
      stop_flag_ = &stop_flag;
      ATOMIC_SET(&next_batch_idx_, 0);
      ATOMIC_SET(&done_count_, 0);
      ATOMIC_SET(&process_lock_, 0);
      ATOMIC_SET(&has_error_, 0);

      void *buf = ob_cdc_malloc(sizeof(WorkerCtx) * n_workers_, "MissLogPF", tenant_id);
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("[MISS_LOG][PARALLEL] alloc WorkerCtx failed", KR(ret), K(n_workers_));
      } else {
        MEMSET(buf, 0, sizeof(WorkerCtx) * n_workers_);
        workers_ = static_cast<WorkerCtx *>(buf);
        for (int64_t i = 0; OB_SUCC(ret) && i < n_workers_; i++) {
          new (&workers_[i]) WorkerCtx();
          n_constructed_ = i + 1;
          void *sbuf = ob_cdc_malloc(sizeof(FetchLogSRpc), ObModIds::OB_LOG_FETCH_LOG_SRPC, tenant_id);
          if (OB_ISNULL(sbuf)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_ERROR("[MISS_LOG][PARALLEL] alloc FetchLogSRpc failed", KR(ret), K(i));
          } else {
            workers_[i].srpc_ = new(sbuf) FetchLogSRpc();
          }
        }

        if (OB_SUCC(ret)) {
          for (int64_t i = 0; i < n_workers_; i++) {
            common::ObAddr worker_svr;
            if (OB_SUCCESS == task.ls_fetch_ctx_.next_server(worker_svr)
                && worker_svr.is_valid()) {
              workers_[i].svr_ = worker_svr;
            } else {
              workers_[i].svr_ = task.svr_;
            }
          }
          set_thread_count(n_workers_);
          if (OB_FAIL(lib::Threads::start())) {
            LOG_ERROR("[MISS_LOG][PARALLEL] start threads failed", KR(ret), K(n_workers_));
          } else {
            inited_ = true;
            const int64_t total_batches = (total_cnt_ + batch_size_ - 1) / batch_size_;
            LOG_INFO("[MISS_LOG][PARALLEL] fetcher started",
                K(n_workers_), K(total_cnt_), K_(batch_size), K(total_batches));
          }
        }
      }
    }
    return ret;
  }

  void destroy()
  {
    if (inited_) {
      lib::Threads::stop();
      done_cond_.signal();
      lib::Threads::destroy();
      inited_ = false;
    }
    if (OB_NOT_NULL(workers_)) {
      for (int64_t i = 0; i < n_constructed_; i++) {
        if (OB_NOT_NULL(workers_[i].srpc_)) {
          workers_[i].srpc_->~FetchLogSRpc();
          ob_cdc_free(workers_[i].srpc_);
        }
        workers_[i].~WorkerCtx();
      }
      ob_cdc_free(workers_);
      workers_ = nullptr;
      n_constructed_ = 0;
    }
  }

  int wait_all(int64_t &total_fetched, int64_t &max_progress)
  {
    int ret = OB_SUCCESS;
    static const int64_t STAT_INTERVAL_US = 10 * _SEC_;
    int64_t last_stat_ts = get_timestamp();
    const int64_t start_ts = last_stat_ts;

    int64_t last_fetched = 0;
    int64_t last_bytes = 0;

    while (ATOMIC_LOAD(&done_count_) < n_workers_ && !(*stop_flag_)) {
      done_cond_.timedwait(100000);
      const int64_t now = get_timestamp();
      if (now - last_stat_ts >= STAT_INTERVAL_US) {
        const int64_t interval_us = now - last_stat_ts;
        last_stat_ts = now;
        int64_t sum_fetched = 0;
        int64_t sum_bytes = 0;
        int64_t sum_rpcs = 0;
        int64_t n_done = 0;
        int64_t n_error = 0;
        for (int64_t i = 0; i < n_workers_; i++) {
          sum_fetched += workers_[i].fetched_cnt_;
          sum_bytes += workers_[i].fetched_bytes_;
          sum_rpcs += workers_[i].rpc_count_;
          if (ATOMIC_LOAD(&workers_[i].done_)) { n_done++; }
          if (OB_SUCCESS != workers_[i].ret_code_) { n_error++; }
        }
        const int64_t elapsed_s = (now - start_ts) / _SEC_;
        const double interval_s = static_cast<double>(interval_us) / _SEC_;
        const int64_t delta_fetched = sum_fetched - last_fetched;
        const int64_t delta_bytes = sum_bytes - last_bytes;
        const int64_t fetch_rate = interval_s > 0 ? static_cast<int64_t>(delta_fetched / interval_s) : 0;
        const int64_t byte_rate = interval_s > 0 ? static_cast<int64_t>(delta_bytes / interval_s) : 0;
        last_fetched = sum_fetched;
        last_bytes = sum_bytes;
        _LOG_INFO("[MISS_LOG][PARALLEL][PROGRESS] elapsed=%lds fetched=%ld/%ld(%.1f%%) "
            "traffic=%s rpcs=%ld rate=%ld logs/s %s/s workers_done=%ld/%ld errors=%ld",
            elapsed_s, sum_fetched, total_cnt_,
            total_cnt_ > 0 ? (100.0 * sum_fetched / total_cnt_) : 0.0,
            SIZE_TO_STR(sum_bytes), sum_rpcs,
            fetch_rate, SIZE_TO_STR(byte_rate),
            n_done, n_workers_, n_error);
      }
    }

    total_fetched = 0;
    max_progress = 0;
    for (int64_t i = 0; i < n_workers_; i++) {
      if (!ATOMIC_LOAD(&workers_[i].done_)) {
        continue;
      }
      if (OB_SUCCESS != workers_[i].ret_code_ && OB_SUCCESS == ret) {
        ret = workers_[i].ret_code_;
      }
      total_fetched += workers_[i].fetched_cnt_;
      if (workers_[i].max_progress_ > max_progress) {
        max_progress = workers_[i].max_progress_;
      }
    }
    return ret;
  }

  void run(int64_t idx) override
  {
    set_cdc_thread_name("MissLogPF", idx);
    ObLogTraceIdGuard trace_guard;

    WorkerCtx &w = workers_[idx];
    const int64_t total_batches = (total_cnt_ + batch_size_ - 1) / batch_size_;
    LOG_INFO("[MISS_LOG][PARALLEL][WORKER_START]", K(idx), K(total_batches), K_(batch_size));

    while (!has_set_stop() && !(*stop_flag_) && OB_SUCCESS == w.ret_code_
        && !ATOMIC_LOAD(&has_error_)) {
      const int64_t batch_idx = ATOMIC_FAA(&next_batch_idx_, 1);
      if (batch_idx >= total_batches) {
        break;
      }

      const int64_t batch_start = batch_idx * batch_size_;
      const int64_t batch_end = std::min(batch_start + batch_size_, total_cnt_);
      int64_t send_idx = batch_start;

      while (send_idx < batch_end && !has_set_stop() && !(*stop_flag_)
          && OB_SUCCESS == w.ret_code_ && !ATOMIC_LOAD(&has_error_)) {
        w.batch_.reuse();
        int ret = OB_SUCCESS;
        for (int64_t i = send_idx; OB_SUCC(ret) && i < batch_end; i++) {
          obrpc::ObCdcLSFetchMissLogReq::MissLogParam p;
          p.miss_lsn_ = miss_arr_->at(i);
          if (OB_FAIL(w.batch_.push_back(p))) {
            LOG_ERROR("[MISS_LOG][PARALLEL] push_back failed", KR(ret), K(idx), K(i));
          }
        }
        if (OB_FAIL(ret)) { w.ret_code_ = ret; break; }

        if (OB_FAIL(ObCDCMissLogHandler::get_instance().fetch_miss_log_with_retry_(
            *task_, w.batch_, *w.srpc_, *stop_flag_,
            &w.svr_, &w.need_change_server_, &process_lock_))) {
          w.ret_code_ = ret;
          if (OB_IN_STOP_STATE != ret) {
            LOG_ERROR("[MISS_LOG][PARALLEL] fetch failed", KR(ret), K(idx), K(batch_idx));
          }
          break;
        }

        const obrpc::ObCdcLSFetchLogResp &resp = w.srpc_->get_resp();
        const int64_t rcnt = resp.get_log_num();

        const palf::LSN next_lsn = resp.get_next_miss_lsn();
        const int64_t bcnt = w.batch_.count();
        bool lsn_ok = false;
        if (bcnt == rcnt) {
          lsn_ok = (w.batch_.at(bcnt - 1).miss_lsn_ == next_lsn);
        } else if (bcnt > rcnt) {
          lsn_ok = (w.batch_.at(rcnt).miss_lsn_ == next_lsn);
        }
        if (!lsn_ok) {
          w.ret_code_ = OB_ERR_UNEXPECTED;
          LOG_ERROR("[MISS_LOG][PARALLEL] next_miss_lsn mismatch", K(idx),
              K(next_lsn), K(bcnt), K(rcnt), "ret", w.ret_code_);
          break;
        }

        {
          // Serialize log deserialization across workers via spinlock.
          // The RPC fetch phase runs in parallel, but deserialization must be serial
          // because it updates shared state (miss_arr_ and send_idx).
          // Workers spin with PAUSE() while waiting; has_error_/stop_flag checks
          // ensure prompt exit on failure.
          ObLogTraceIdGuard trace_guard;
          while (!ATOMIC_BCAS(&process_lock_, 0, 1)) {
            PAUSE();
            if (ATOMIC_LOAD(&has_error_) || has_set_stop() || *stop_flag_) {
              w.ret_code_ = OB_IN_STOP_STATE;
              break;
            }
          }
          if (OB_SUCCESS != w.ret_code_) break;
          ret = do_read_batch_misslog_for_worker(
              task_->ls_fetch_ctx_, resp, *miss_arr_, send_idx,
              task_->tsi_, w.max_progress_);
          ATOMIC_SET(&process_lock_, 0);
        }
        if (OB_FAIL(ret)) {
          w.ret_code_ = ret;
          LOG_ERROR("[MISS_LOG][PARALLEL] process failed", KR(ret), K(idx), K(send_idx));
          break;
        }

        w.fetched_cnt_ += rcnt;
        w.fetched_bytes_ += resp.get_pos();
        w.rpc_count_++;
        send_idx += rcnt;
      }
    }

    if (OB_SUCCESS != w.ret_code_) {
      ATOMIC_SET(&has_error_, 1);
    }
    ATOMIC_SET(&w.done_, true);
    ATOMIC_INC(&done_count_);
    done_cond_.signal();
    ObCStringHelper helper;
    _LOG_INFO("[MISS_LOG][PARALLEL][WORKER_END] idx=%ld fetched=%ld rpcs=%ld "
        "traffic=%s svr=%s ret_code=%d",
        idx, w.fetched_cnt_, w.rpc_count_,
        SIZE_TO_STR(w.fetched_bytes_),
        helper.convert(w.svr_), w.ret_code_);
  }

private:
  int64_t n_workers_;
  int64_t n_constructed_;
  WorkerCtx *workers_;
  const ObLogLSNArray *miss_arr_;
  int64_t total_cnt_;
  MissLogTask *task_;
  volatile bool *stop_flag_;

  int64_t next_batch_idx_ CACHE_ALIGNED;
  int64_t batch_size_;
  volatile int64_t done_count_ CACHE_ALIGNED;
  common::ObCond done_cond_;
  volatile int64_t process_lock_ CACHE_ALIGNED;
  volatile int64_t has_error_ CACHE_ALIGNED;
  bool inited_;
};

int64_t ObCDCMissLogHandler::g_rpc_timeout = TCONF.fetch_log_rpc_timeout_sec * _SEC_;
const int64_t ObCDCMissLogHandler::RETRY_LOG_PRINT_INTERVAL = 30 * _SEC_;
const int64_t ObCDCMissLogHandler::RETRY_TIMEOUT = 1 * _HOUR_;
const int64_t ObCDCMissLogHandler::MAX_RPC_TIMEOUT = 5 * _MIN_;

MissLogTask::MissLogTask(
    const common::ObAddr &orig_svr,
    IObLogRpc &rpc,
    LSFetchCtx &ls_fetch_ctx,
    IObCDCPartTransResolver::MissingLogInfo &missing_info,
    logfetcher::TransStatInfo &tsi)
    : svr_(), rpc_(rpc), ls_fetch_ctx_(ls_fetch_ctx), missing_info_(missing_info),
      need_change_server_(false), disable_server_change_(false), tsi_(tsi)
{
  svr_ = orig_svr;
}

void MissLogTask::reset()
{
  svr_.reset();
  need_change_server_ = false;
  disable_server_change_ = false;
}

int MissLogTask::try_change_server(const int64_t timeout, volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  const int64_t end_time = get_timestamp() + timeout;

  if (OB_UNLIKELY(disable_server_change_)) {
    need_change_server_ = false;
  } else if (OB_UNLIKELY(need_change_server_)) {
    common::ObAddr req_svr;
    int64_t retry_times = 0;
    bool is_timeout = false;
    const static int64_t RETRY_LOG_PRINT_INTERVAL = 30 * _SEC_;
    do {
      if (OB_FAIL(ls_fetch_ctx_.next_server(req_svr))) {
        LOG_WARN("get next_server failed", KR(ret), K_(ls_fetch_ctx));
      } else if (OB_UNLIKELY(!req_svr.is_valid())) {
        ret = OB_NEED_RETRY;
        LOG_WARN("next_server is not valid, need_retry", KR(ret), K(req_svr), K_(ls_fetch_ctx));
      } else {
        ObCStringHelper helper;
        _LOG_INFO("[MISS_LOG][CHANGE_SVR][FROM=%s][TO=%s][PART_TRANS_ID=%s]",
            helper.convert(svr_), helper.convert(req_svr), helper.convert(get_part_trans_id()));
        svr_ = req_svr;
        need_change_server_ = false;
      }

      if (OB_FAIL(ret)) {
        is_timeout = get_timestamp() >= end_time;
        if (is_timeout) {
          LOG_ERROR("[MISS_LOG][NEXT_SERVER]RETRY_GET_NEXT_SERVER TIMEOUT", KR(ret), K(timeout), KPC(this));
          ret = OB_TIMEOUT;
        } else {
          ++retry_times;
          if (TC_REACH_TIME_INTERVAL(RETRY_LOG_PRINT_INTERVAL)) {
            LOG_WARN("[MISS_LOG][NEXT_SERVER]RETRY_GET_NEXT_SERVER",
                "tls_id", ls_fetch_ctx_.get_tls_id(), K(retry_times),
                "end_time", TS_TO_STR(end_time));
          }
          ob_usleep(10 * _MSEC_);
        }
      }
    } while (OB_FAIL(ret) && ! is_timeout && ! stop_flag);

    if (OB_UNLIKELY(stop_flag)) {
      ret = OB_IN_STOP_STATE;
      LOG_WARN("handle miss log task stop", KR(ret), KPC(this));
    }
  }

  return ret;
}

ObCDCMissLogHandler& ObCDCMissLogHandler::get_instance()
{
  static ObCDCMissLogHandler instance;
  return instance;
}

void ObCDCMissLogHandler::configure(const ObLogConfig &config)
{
  g_rpc_timeout = config.fetch_log_rpc_timeout_sec * _SEC_;
  LOG_INFO("[MISS_LOG][CONFIG]", "rpc_timeout", TVAL_TO_STR(g_rpc_timeout));
}

// TODO add metrics
int ObCDCMissLogHandler::handle_log_miss(
    const common::ObAddr &cur_svr,
    IObLogRpc *rpc,
    LSFetchCtx &ls_fetch_ctx,
    IObCDCPartTransResolver::MissingLogInfo &missing_info,
    logfetcher::TransStatInfo &tsi,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(rpc)
      || OB_UNLIKELY(missing_info.is_empty()
      || ! cur_svr.is_valid()
      || missing_info.get_part_trans_id().get_tls_id() != ls_fetch_ctx.get_tls_id())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid args to handle miss log", KR(ret), K(cur_svr), K(missing_info), K(rpc));
  } else {
    MissLogTask misslog_task(cur_svr, *rpc, ls_fetch_ctx, missing_info, tsi);

    if (OB_FAIL(handle_miss_log_task_(misslog_task, stop_flag))) {
      if (OB_IN_STOP_STATE != ret) {
        int debug_err = ret;
        // overwrite ret
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("handle_miss_log_task_ failed", KR(ret), KR(debug_err), K(misslog_task), K(stop_flag));
      }
    }
  }

  return ret;
}

int ObCDCMissLogHandler::handle_miss_log_task_(MissLogTask &misslog_task, volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  ObTraceIdGuard guard(*common::ObCurTraceId::get_trace_id());
  const int64_t start_ts = get_timestamp();
  misslog_task.missing_info_.set_resolving_miss_log();
  FetchLogSRpc *fetch_log_srpc = nullptr;
  // Allocate a second FetchLogSRpc for pipelined redo log fetching:
  // while processing the current batch response, the next batch RPC is already in-flight.
  FetchLogSRpc *fetch_log_srpc_next = nullptr;

  if (OB_FAIL(alloc_fetch_log_srpc_(misslog_task.get_tenant_id(), fetch_log_srpc))) {
    LOG_ERROR("alloc fetch_log_srpc fail", KR(ret));
  } else if (OB_ISNULL(fetch_log_srpc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid fetch_log_srpc", KR(ret), K(misslog_task));
  } else if (OB_FAIL(alloc_fetch_log_srpc_(misslog_task.get_tenant_id(), fetch_log_srpc_next))) {
    LOG_ERROR("alloc fetch_log_srpc_next fail", KR(ret));
  } else if (OB_ISNULL(fetch_log_srpc_next)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid fetch_log_srpc_next", KR(ret), K(misslog_task));
  } else if (OB_FAIL(handle_miss_record_or_state_log_(
      misslog_task,
      *fetch_log_srpc,
      stop_flag))) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("handle_miss_record_or_state_log_ failed", KR(ret), K(misslog_task));
    }
  } else if (OB_FAIL(handle_miss_redo_log_(
      misslog_task,
      *fetch_log_srpc,
      *fetch_log_srpc_next,
      stop_flag))) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("handle_miss_redo_log_ failed", KR(ret), K(misslog_task));
    }
  }

  if (stop_flag) {
    ret = OB_IN_STOP_STATE;
  } else {
    const int64_t cost_time = get_timestamp() - start_ts;
    LOG_INFO("[MISS_LOG][HANDLE_DONE]", KR(ret),
        K(cost_time), "cost_time", TVAL_TO_STR(cost_time),
        K(misslog_task));
  }

  if (OB_NOT_NULL(fetch_log_srpc)) {
    free_fetch_log_srpc_(fetch_log_srpc);
    fetch_log_srpc = nullptr;
  }
  if (OB_NOT_NULL(fetch_log_srpc_next)) {
    free_fetch_log_srpc_(fetch_log_srpc_next);
    fetch_log_srpc_next = nullptr;
  }
  return ret;
}

int ObCDCMissLogHandler::handle_miss_record_or_state_log_(
    MissLogTask &misslog_task,
    FetchLogSRpc &fetch_log_srpc,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;

  if (misslog_task.missing_info_.has_miss_record_or_state_log()) {
    int64_t miss_record_cnt = 1;
    ObArrayImpl<obrpc::ObCdcLSFetchMissLogReq::MissLogParam> batched_misslog_lsn_arr;
    palf::LSN misslog_lsn;
    const int64_t start_ts = get_timestamp();
    LOG_INFO("[MISS_LOG][FETCH_RECORD_OR_STATE][BEGIN]", KR(ret), "part_trans_id", misslog_task.get_part_trans_id());

    while (OB_SUCC(ret) && ! stop_flag && misslog_task.missing_info_.has_miss_record_or_state_log()) {
      misslog_lsn.reset();
      batched_misslog_lsn_arr.reuse();
      ObCdcLSFetchMissLogReq::MissLogParam param;

      if (OB_FAIL(misslog_task.missing_info_.get_miss_record_or_state_log_lsn(misslog_lsn))) {
        LOG_ERROR("get_miss_record_or_state_log_lsn failed", KR(ret), K(misslog_task), K(misslog_lsn));
      } else {
        param.miss_lsn_ = misslog_lsn;

        if (OB_FAIL(batched_misslog_lsn_arr.push_back(param))) {
          LOG_ERROR("push_back miss_record_or_state_log_lsn into batched_misslog_lsn_arr failed", KR(ret), K(param));
        } else if (OB_FAIL(fetch_miss_log_with_retry_(
            misslog_task,
            batched_misslog_lsn_arr,
            fetch_log_srpc,
            stop_flag))) {
          if (OB_IN_STOP_STATE != ret) {
            LOG_ERROR("fetch_miss_log_with_retry_ failed", KR(ret), K(batched_misslog_lsn_arr), K(misslog_task));
          }
        } else {
          const obrpc::ObRpcResultCode &rcode = fetch_log_srpc.get_result_code();
          const obrpc::ObCdcLSFetchLogResp &resp = fetch_log_srpc.get_resp();

          if (OB_FAIL(rcode.rcode_) || OB_FAIL(resp.get_err())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("fetch log fail on rpc", KR(ret), K(rcode), K(resp), K(batched_misslog_lsn_arr));
          } else if (resp.get_log_num() < 1) {
            LOG_INFO("fetch_miss_log_rpc doesn't fetch log, retry", K(misslog_lsn));
          } else if (OB_UNLIKELY(resp.get_log_num() > 1)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("expect only one misslog while fetching miss_record_or_state_log", K(resp));
          } else if (OB_UNLIKELY(resp.get_next_miss_lsn() != misslog_lsn)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("fetched log not match miss_log_lsn", KR(ret), K(misslog_lsn), K(resp));
          } else {
            misslog_task.missing_info_.reset_miss_record_or_state_log_lsn();
            bool enable_logservice = misslog_task.ls_fetch_ctx_.get_logservice_model();
            ipalf::ILogEntry miss_log_entry(enable_logservice);
            miss_log_entry.reset();
            const char *buf = resp.get_log_entry_buf();
            const int64_t len = resp.get_pos();
            int64_t pos = 0;

            if (OB_FAIL(miss_log_entry.deserialize(misslog_lsn, buf, len, pos))) {
              LOG_ERROR("deserialize log_entry of miss_record_or_state_log failed", KR(ret), K(misslog_lsn), KP(buf), K(len), K(pos));
            } else if (OB_FAIL(misslog_task.ls_fetch_ctx_.read_miss_tx_log(miss_log_entry, misslog_lsn, misslog_task.tsi_, misslog_task.missing_info_))) {
              if (OB_ITEM_NOT_SETTED == ret) {
                miss_record_cnt ++;
                ret = OB_SUCCESS;
                LOG_INFO("[MISS_LOG][FETCH_RECORD_OR_STATE] found new miss_record_or_state_log",
                    "tls_id", misslog_task.ls_fetch_ctx_.get_tls_id(), K(misslog_lsn), K(miss_record_cnt), K(misslog_task));
              } else {
                LOG_ERROR("[MISS_LOG][FETCH_RECORD_OR_STATE] read miss_log failed", KR(ret), K(miss_log_entry), K(misslog_lsn), K(misslog_task));
              }
            }
          }
        }
      }
    }

    if (stop_flag) {
      ret = OB_IN_STOP_STATE;
    }

    if (OB_SUCC(ret)) {
      LOG_INFO("[MISS_LOG][FETCH_RECORD_OR_STATE][END]",
          "part_trans_id", misslog_task.missing_info_.get_part_trans_id(),
          K(miss_record_cnt),
          "miss_redo_cnt", misslog_task.missing_info_.get_miss_redo_lsn_arr().count(),
          "cost", get_timestamp() - start_ts);
    }
  }

  return ret;
}

// Pipelined miss redo log fetching:
//
// Decouples the RPC loop (main thread) from response processing (worker thread).
// After receiving an RPC response, the main thread:
//   1. Verifies response metadata (lightweight — only checks log_num and next_miss_lsn)
//   2. Hands off the response to MissLogBatchReader for async processing
//   3. Immediately proceeds to fetch the next batch (blocking RPC with retry)
//
// This overlaps the full RPC round-trip of batch N+1 with the processing of batch N.
//
// Timeline:
//   Main thread:  [fetch_with_retry(0)] → [post resp_0] → [fetch_with_retry(1)] → [wait_worker(0)] [post resp_1] → ...
//   Worker thread:                         [read_batch(0)] ─────────────────────────────────────────  [read_batch(1)] ...
//                                                           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//                                                           overlapped with fetch_with_retry(1)
//
// Uses two FetchLogSRpc objects (ping-pong): while the worker processes batch N's
// response from srpc_A, the main thread uses srpc_B for batch N+1's RPC.
//
// For direct-fetch mode (no network RPC), the pipeline still works but provides no
// benefit since there is no network latency to hide.
// If worker thread creation fails, falls back to synchronous processing.

int ObCDCMissLogHandler::handle_miss_redo_log_(
    MissLogTask &misslog_task,
    FetchLogSRpc &fetch_log_srpc,
    FetchLogSRpc &fetch_log_srpc_next,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(misslog_task.missing_info_.sort_and_unique_missing_log_lsn())) {
    LOG_ERROR("sort_and_unique_missing_log_lsn failed", KR(ret), K(misslog_task));
  } else {
    const int64_t total_misslog_cnt = misslog_task.missing_info_.get_total_misslog_cnt();
    int64_t fetched_missing_log_cnt = 0;
    const int64_t start_ts = get_timestamp();
    const int64_t parallelism = TCONF.miss_log_discrete_fetch_parallelism;
    const ClientFetchingMode fetching_mode = misslog_task.ls_fetch_ctx_.get_fetching_mode();

    LOG_INFO("[MISS_LOG][FETCH_REDO][BEGIN]",
        "part_trans_id", misslog_task.get_part_trans_id(),
        K(total_misslog_cnt), K(parallelism));

    // direct-fetch mode must keep miss-log fetching serial.
    // Multiple workers would concurrently access the same LSFetchCtx and update
    // its shared archive source / locate-info state through direct-fetch
    // callbacks, but that path has no thread-safe protection today.
    const bool allow_parallel_discrete_fetch =
        !is_direct_fetching_mode(fetching_mode)
        && parallelism > 1
        && total_misslog_cnt > parallelism;

    if (allow_parallel_discrete_fetch) {
      ret = handle_miss_redo_log_parallel_(
          misslog_task, fetched_missing_log_cnt, total_misslog_cnt, stop_flag);
    } else {
      if (is_direct_fetching_mode(fetching_mode) && parallelism > 1) {
        LOG_INFO("[MISS_LOG] direct-fetch mode disables parallel discrete fetch",
            K(total_misslog_cnt), K(parallelism), K(fetching_mode));
      }
      ret = handle_miss_redo_log_discrete_(
          misslog_task, fetch_log_srpc, fetch_log_srpc_next,
          fetched_missing_log_cnt, total_misslog_cnt, stop_flag);
    }

    LOG_INFO("[MISS_LOG][FETCH_REDO][END]", KR(ret),
        "part_trans_id", misslog_task.missing_info_.get_part_trans_id(),
        K(total_misslog_cnt), K(fetched_missing_log_cnt),
        K(parallelism),
        "cost", get_timestamp() - start_ts);
  }

  return ret;
}

// Original discrete-fetch path with pipeline worker.
int ObCDCMissLogHandler::handle_miss_redo_log_discrete_(
    MissLogTask &misslog_task,
    FetchLogSRpc &fetch_log_srpc,
    FetchLogSRpc &fetch_log_srpc_next,
    int64_t &fetched_missing_log_cnt,
    const int64_t total_misslog_cnt,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  int64_t send_idx = 0;
  ObArrayImpl<obrpc::ObCdcLSFetchMissLogReq::MissLogParam> batched_misslog_lsn_arr;

  MissLogBatchReader reader;
  const int init_ret = reader.init();
  bool use_pipeline = (OB_SUCCESS == init_ret);
  if (!use_pipeline) {
    LOG_WARN("[MISS_LOG][PIPELINE] worker init failed, fallback to synchronous mode", "init_ret", init_ret);
  }

  FetchLogSRpc *cur_srpc = &fetch_log_srpc;
  FetchLogSRpc *next_srpc = &fetch_log_srpc_next;

  while (OB_SUCC(ret) && ! stop_flag && send_idx < total_misslog_cnt) {
    batched_misslog_lsn_arr.reuse();

    if (OB_FAIL(build_batch_misslog_lsn_arr_(
        send_idx,
        misslog_task.missing_info_,
        batched_misslog_lsn_arr))) {
      LOG_ERROR("build_batch_misslog_lsn_arr_ failed", KR(ret), K(send_idx), K(misslog_task));
    } else if (OB_FAIL(fetch_miss_log_with_retry_(
        misslog_task,
        batched_misslog_lsn_arr,
        *cur_srpc,
        stop_flag))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("fetch_miss_log_with_retry_ failed", KR(ret), K(batched_misslog_lsn_arr), K(misslog_task));
      }
    }

    if (OB_SUCC(ret) && ! stop_flag) {
      const obrpc::ObRpcResultCode &rcode = cur_srpc->get_result_code();
      const obrpc::ObCdcLSFetchLogResp &resp = cur_srpc->get_resp();

      if (OB_FAIL(rcode.rcode_) || OB_FAIL(resp.get_err())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("fetch log fail on rpc", KR(ret), K(rcode), K(resp), K(batched_misslog_lsn_arr));
      } else {
        bool is_next_miss_lsn_match = false;
        palf::LSN next_miss_lsn = resp.get_next_miss_lsn();
        const int64_t batch_cnt = batched_misslog_lsn_arr.count();
        const int64_t resp_log_cnt = resp.get_log_num();

        if (batch_cnt == resp_log_cnt) {
          is_next_miss_lsn_match = (batched_misslog_lsn_arr.at(batch_cnt - 1).miss_lsn_ == next_miss_lsn);
        } else if (batch_cnt > resp_log_cnt) {
          is_next_miss_lsn_match = (batched_misslog_lsn_arr.at(resp_log_cnt).miss_lsn_ == next_miss_lsn);
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("too many misslog fetched", KR(ret), K(next_miss_lsn), K(batch_cnt),
              K(resp_log_cnt), K(resp), K(misslog_task));
        }

        if (OB_SUCC(ret)) {
          if (!is_next_miss_lsn_match) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("misslog fetched is not match batched_misslog_lsn_arr requested", KR(ret),
                K(next_miss_lsn), K(batch_cnt), K(resp_log_cnt), K(batched_misslog_lsn_arr), K(resp), K(misslog_task));
          } else if (use_pipeline) {
            if (reader.has_pending_task()) {
              const int proc_ret = reader.wait_task();
              if (OB_UNLIKELY(OB_SUCCESS != proc_ret)) {
                ret = proc_ret;
                LOG_ERROR("previous batch processing failed", KR(ret), K(misslog_task));
              }
            }

            if (OB_SUCC(ret)) {
              reader.post_task(
                  misslog_task.ls_fetch_ctx_,
                  resp,
                  fetched_missing_log_cnt,
                  misslog_task.tsi_,
                  misslog_task.missing_info_);
              send_idx += resp_log_cnt;
              std::swap(cur_srpc, next_srpc);
            }
          } else {
            if (OB_FAIL(read_batch_misslog_(
                misslog_task.ls_fetch_ctx_,
                resp,
                fetched_missing_log_cnt,
                misslog_task.tsi_,
                misslog_task.missing_info_))) {
              LOG_ERROR("read_batch_misslog failed", KR(ret), K(fetched_missing_log_cnt), K(misslog_task));
            } else {
              send_idx += resp_log_cnt;
            }
          }
        }
      }
    }
  }

  if (use_pipeline && reader.has_pending_task()) {
    const int proc_ret = reader.wait_task();
    if (OB_SUCC(ret)) {
      ret = proc_ret;
    }
  }
  reader.destroy();

  return ret;
}

// Parallel discrete-fetch path.
int ObCDCMissLogHandler::handle_miss_redo_log_parallel_(
    MissLogTask &misslog_task,
    int64_t &fetched_missing_log_cnt,
    const int64_t total_misslog_cnt,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_direct_fetching_mode(misslog_task.ls_fetch_ctx_.get_fetching_mode()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("[MISS_LOG][PARALLEL] direct-fetch mode should not enter parallel path",
        KR(ret), K(misslog_task));
  } else {
    const int64_t parallelism = std::min(
        TCONF.miss_log_discrete_fetch_parallelism.get(), total_misslog_cnt);
    const uint64_t tenant_id = misslog_task.get_tenant_id();
    const ObLogLSNArray &miss_arr = misslog_task.missing_info_.get_miss_redo_lsn_arr();

    MissLogParallelFetcher fetcher;
    if (OB_FAIL(fetcher.init(parallelism, tenant_id, miss_arr,
            total_misslog_cnt, misslog_task, stop_flag))) {
      LOG_ERROR("[MISS_LOG][PARALLEL] init failed", KR(ret), K(parallelism));
    } else {
      int64_t max_progress = 0;
      ret = fetcher.wait_all(fetched_missing_log_cnt, max_progress);
      if (OB_SUCC(ret) && max_progress > 0) {
        misslog_task.missing_info_.set_last_misslog_progress(max_progress);
      }
    }

    fetcher.destroy();
  }

  if (stop_flag) {
    ret = OB_IN_STOP_STATE;
  }

  return ret;
}

// split all miss_logs by batch
int ObCDCMissLogHandler::build_batch_misslog_lsn_arr_(
    const int64_t fetched_log_idx,
    IObCDCPartTransResolver::MissingLogInfo &missing_log_info,
    ObIArray<obrpc::ObCdcLSFetchMissLogReq::MissLogParam> &batched_misslog_lsn_arr)
{
  int ret = OB_SUCCESS;

  int64_t batched_cnt = 0;
  // Increase batch size: actual batch is bounded by server-side FETCH_BUF_LEN (17MB),
  // so a larger client-side limit allows small entries to be batched more efficiently.
  static int64_t MAX_MISSLOG_CNT_PER_RPC = 4096;
  const ObLogLSNArray &miss_redo_or_state_log_arr = missing_log_info.get_miss_redo_lsn_arr();
  const int64_t miss_log_cnt = miss_redo_or_state_log_arr.count();

  if (OB_UNLIKELY(0 < batched_misslog_lsn_arr.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid batched_misslog_lsn_arr", KR(ret), K(batched_misslog_lsn_arr));
  } else {

    // fetched_log_idx is log_count that already fetched after last batch rpc
    // for miss_redo_or_state_log_arr with 100 miss_log and MAX_MISSLOG_CNT_PER_RPC = 10
    // fetched_log_idx start from 0, if fetched 8 miss_log in one rpc, then fetched_log_idx is 8,
    // and for next batch, miss_redo_or_state_log_arr.at(8) is the 9th miss_log as expected.
    for (int idx = fetched_log_idx; OB_SUCC(ret) && batched_cnt < MAX_MISSLOG_CNT_PER_RPC && idx < miss_log_cnt; idx++) {
      const palf::LSN &lsn = miss_redo_or_state_log_arr.at(idx);
      ObCdcLSFetchMissLogReq::MissLogParam param;
      param.miss_lsn_ = lsn;
      if (OB_FAIL(batched_misslog_lsn_arr.push_back(param))) {
        LOG_ERROR("push_back missing_log lsn into batched_misslog_lsn_arr failed", KR(ret), K(idx),
            K(fetched_log_idx), K(miss_redo_or_state_log_arr), K(batched_misslog_lsn_arr), K(param));
      } else {
        batched_cnt++;
      }
    }
  }
  ObCStringHelper helper;
  _LOG_INFO("[MISS_LOG][BATCH_MISSLOG][PART_TRANS_ID=%s][PROGRESS=%ld/%ld][BATCH_SIZE=%ld]",
        helper.convert(missing_log_info.get_part_trans_id()),
        fetched_log_idx,
        miss_log_cnt,
        batched_misslog_lsn_arr.count());

  return ret;
}

// read batched misslog — delegates to file-scope do_read_batch_misslog()
int ObCDCMissLogHandler::read_batch_misslog_(
    LSFetchCtx &ls_fetch_ctx,
    const obrpc::ObCdcLSFetchLogResp &resp,
    int64_t &fetched_missing_log_cnt,
    logfetcher::TransStatInfo &tsi,
    IObCDCPartTransResolver::MissingLogInfo &missing_info)
{
  return do_read_batch_misslog(ls_fetch_ctx, resp, fetched_missing_log_cnt, tsi, missing_info);
}

int ObCDCMissLogHandler::alloc_fetch_log_srpc_(const uint64_t tenant_id, FetchLogSRpc *&fetch_log_srpc)
{
  int ret = OB_SUCCESS;
  void *buf = ob_cdc_malloc(sizeof(FetchLogSRpc), ObModIds::OB_LOG_FETCH_LOG_SRPC, tenant_id);

  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc memory for FetchLogSRpc fail", KR(ret), K(tenant_id), K(sizeof(FetchLogSRpc)));
  } else if (OB_ISNULL(fetch_log_srpc = new(buf) FetchLogSRpc())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("construct fetch miss log srpc fail", KR(ret), K(tenant_id), K(buf));
  } else {
    // success
  }

  return ret;
}

void ObCDCMissLogHandler::free_fetch_log_srpc_(FetchLogSRpc *fetch_log_srpc)
{
  if (OB_NOT_NULL(fetch_log_srpc)) {
    fetch_log_srpc->~FetchLogSRpc();
    ob_cdc_free(fetch_log_srpc);
    fetch_log_srpc = nullptr;
  }
}

int ObCDCMissLogHandler::fetch_miss_log_with_retry_(
    MissLogTask &misslog_task,
    const ObIArray<obrpc::ObCdcLSFetchMissLogReq::MissLogParam> &miss_log_array,
    FetchLogSRpc &fetch_srpc,
    volatile bool &stop_flag,
    ObAddr *worker_svr,
    bool *worker_need_change,
    volatile int64_t *svr_change_lock)
{
  int ret = OB_SUCCESS;
  ObLogTraceIdGuard trace_guard;
  int64_t fetch_log_timeout = g_rpc_timeout;

  const int64_t start_ts = get_timestamp();
  const int64_t end_ts = start_ts + RETRY_TIMEOUT;
  int64_t cur_ts = start_ts;
  int64_t try_cnt = 0;
  bool rpc_succ = false;
  bool rpc_fetch_no_log = false;
  bool is_fatal_err = false;
  bool need_backoff = false;
  const bool test_mode_misslog_errsim = (1 == TCONF.test_mode_on && 1 == TCONF.test_fetch_missing_errsim);
  int test_mode_fail_max_cnt = test_mode_misslog_errsim ? 2 : 0;
  int test_mode_fail_cnt = 0;

  static const int64_t BASE_RETRY_INTERVAL_US = 100 * _MSEC_;
  static const int64_t MAX_RETRY_INTERVAL_US = 30 * _SEC_;

  while (! stop_flag && ! rpc_succ && ! is_fatal_err && cur_ts < end_ts) {
    bool has_valid_feedback = false;
    need_backoff = false;
    try_cnt ++;
    if (OB_FAIL(fetch_miss_log_(
        miss_log_array,
        fetch_log_timeout,
        misslog_task,
        fetch_srpc,
        stop_flag,
        worker_svr,
        worker_need_change,
        svr_change_lock))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("fetch_miss_log failed", KR(ret), K(misslog_task));
        need_backoff = true;
      }
    } else {
      const obrpc::ObRpcResultCode &rcode = fetch_srpc.get_result_code();
      const obrpc::ObCdcLSFetchLogResp &resp = fetch_srpc.get_resp();

      if (OB_SUCCESS != rcode.rcode_ || OB_SUCCESS != resp.get_err()) {
        if (OB_TIMEOUT == rcode.rcode_ || OB_TIMEOUT == resp.get_err() || OB_TIMEOUT == resp.get_debug_err()) {
          ret = OB_TIMEOUT;
        } else {
          if (OB_SUCCESS != rcode.rcode_) {
            ret = rcode.rcode_;
          } else if (OB_SUCCESS != resp.get_err()) {
            ret = resp.get_err();
          } else if (OB_SUCCESS != resp.get_debug_err()) {
            ret = resp.get_debug_err();
          }

          if (OB_ERR_OUT_OF_LOWER_BOUND == ret) {
            rpc_fetch_no_log = true;
            is_fatal_err = true;
            LOG_ERROR("[MISS_LOG] fetch log fail on rpc, log has been recycled", KR(ret), K(rpc_fetch_no_log), K(rcode), K(resp), K(misslog_task));
          } else {
            need_backoff = true;
            LOG_WARN("[MISS_LOG] fetch log fail on rpc", KR(ret), K(rpc_fetch_no_log), K(rcode), K(resp), K(misslog_task));
          }
        }
      } else if (OB_UNLIKELY(test_mode_fail_cnt++ < test_mode_fail_max_cnt)) {
        ret = OB_TIMEOUT;
        LOG_INFO("mock fetch log fail in test mode", KR(ret), K(test_mode_fail_cnt), K(test_mode_fail_max_cnt), "end_time", TS_TO_STR(end_ts));
      } else if (FALSE_IT(check_feedback_(resp, has_valid_feedback, rpc_fetch_no_log))) {
      } else if (OB_UNLIKELY(resp.get_log_num() <= 0)) {
        need_backoff = true;
        LOG_WARN("[MISS_LOG][EMPTY_RPC_RESPONSE], need retry", K(resp), K(misslog_task));
      } else {
        rpc_succ = true;
      }

      if (OB_TIMEOUT == ret) {
        ObCStringHelper helper;
        const int64_t max_rpc_timeout = std::max(MAX_RPC_TIMEOUT, g_rpc_timeout);
        const int64_t new_fetch_log_timeout = std::min(max_rpc_timeout, fetch_log_timeout * 2);
        _LOG_INFO("[MISS_LOG][FETCH_TIMEOUT][ADJUST_FETCH_MISSLOG_TIMEOUT][FROM=%s][TO=%s][rpc_rcode=%s][rpc_response=%s]",
            TVAL_TO_STR(fetch_log_timeout), TVAL_TO_STR(new_fetch_log_timeout), helper.convert(rcode), helper.convert(resp));
        fetch_log_timeout = new_fetch_log_timeout;
      }
    }

    if (OB_UNLIKELY(! rpc_succ || has_valid_feedback)) {
      const uint64_t cluster_version = GET_MIN_CLUSTER_VERSION();
      const bool should_change = (cluster_version >= MOCK_CLUSTER_VERSION_4_2_1_7  && cluster_version < CLUSTER_VERSION_4_2_2_0)
          || (cluster_version >= MOCK_CLUSTER_VERSION_4_2_5_0 && cluster_version < CLUSTER_VERSION_4_3_0_0)
          || cluster_version >= CLUSTER_VERSION_4_3_4_0;
      if (OB_NOT_NULL(worker_need_change)) {
        *worker_need_change = should_change;
      } else if (!misslog_task.disable_server_change_) {
        misslog_task.need_change_server_ = should_change;
      }

      if (rpc_fetch_no_log) {
        LOG_WARN("[MISS_LOG][FETCH_NO_LOG]", KR(ret), K(misslog_task), "rpc_response", fetch_srpc.get_resp(), "rpc_request", fetch_srpc.get_req());
      }
    }

    if (need_backoff && ! stop_flag && ! is_fatal_err) {
      const int64_t capped_shift = std::min(try_cnt - 1, (int64_t)15);
      const int64_t backoff_ceil = std::min(MAX_RETRY_INTERVAL_US, BASE_RETRY_INTERVAL_US * (1L << capped_shift));
      const int64_t sleep_us = common::ObRandom::rand(0, backoff_ceil);
      if (sleep_us > 0) {
        _LOG_INFO("[MISS_LOG][RETRY_BACKOFF][try_cnt=%ld][sleep=%ldus][backoff_ceil=%ldus]",
            try_cnt, sleep_us, backoff_ceil);
        ob_usleep(static_cast<uint32_t>(std::min(sleep_us, (int64_t)INT32_MAX)));
      }
    }

    cur_ts = get_timestamp();
  } // end while

  if (stop_flag) {
    ret = OB_IN_STOP_STATE;
  } else if (!rpc_succ && !is_fatal_err) {
    ret = OB_TIMEOUT;
    LOG_WARN("[MISS_LOG] fetch_miss_log_with_retry exhausted",
        K(try_cnt), "elapsed_us", cur_ts - start_ts, K(misslog_task));
  }

  const obrpc::ObCdcLSFetchLogResp &final_resp = fetch_srpc.get_resp();
  ObCStringHelper helper;
  _LOG_INFO("[MISS_LOG][FETCH_MISSLOG][ret=%d][PART_TRANS_ID=%s][SVR=%s][COST=%ld]"
      "[REQ_CNT=%ld][RESP_CNT=%ld][RESP_SIZE=%s]"
      "[FETCH_STATUS=%s][FETCH_ROUND=%ld][FATAL=%d]", ret,
      helper.convert(misslog_task.get_part_trans_id()),
      helper.convert(misslog_task.svr_),
      cur_ts - start_ts,
      miss_log_array.count(),
      final_resp.get_log_num(),
      SIZE_TO_STR(final_resp.get_pos()),
      helper.convert(final_resp.get_fetch_status()),
      try_cnt,
      is_fatal_err);

  return ret;
}

int ObCDCMissLogHandler::fetch_miss_log_(
    const ObIArray<obrpc::ObCdcLSFetchMissLogReq::MissLogParam> &miss_log_array,
    const int64_t timeout,
    MissLogTask &misslog_task,
    FetchLogSRpc &fetch_srpc,
    volatile bool &stop_flag,
    ObAddr *worker_svr,
    bool *worker_need_change,
    volatile int64_t *svr_change_lock)
{
  int ret = OB_SUCCESS;
  const ClientFetchingMode fetching_mode = misslog_task.ls_fetch_ctx_.get_fetching_mode();
  const int64_t last_handle_progress = misslog_task.get_handle_progress();
  LSFetchCtx &ls_fetch_ctx = misslog_task.ls_fetch_ctx_;

  if (OB_UNLIKELY(! is_fetching_mode_valid(fetching_mode))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("fetching mode is not valid", KR(ret), K(fetching_mode));
  } else if (is_integrated_fetching_mode(fetching_mode)) {
    if (OB_NOT_NULL(worker_svr) && OB_NOT_NULL(worker_need_change)) {
      if (*worker_need_change) {
        bool lock_acquired = false;
        if (OB_NOT_NULL(svr_change_lock)) {
          while (!ATOMIC_BCAS(svr_change_lock, 0, 1)) {
            PAUSE();
            if (stop_flag) {
              ret = OB_IN_STOP_STATE;
              break;
            }
          }
          if (OB_SUCC(ret)) {
            lock_acquired = true;
          }
        }
        if (OB_SUCC(ret)) {
          common::ObAddr new_svr;
          int tmp_ret = ls_fetch_ctx.next_server(new_svr);
          if (OB_SUCCESS == tmp_ret && new_svr.is_valid()) {
            ObCStringHelper helper;
            _LOG_INFO("[MISS_LOG][PARALLEL][CHANGE_SVR][FROM=%s][TO=%s]",
                helper.convert(*worker_svr), helper.convert(new_svr));
            *worker_svr = new_svr;
          }
        }
        if (lock_acquired) {
          ATOMIC_SET(svr_change_lock, 0);
        }
        *worker_need_change = false;
      }
    } else if (OB_FAIL(misslog_task.try_change_server(timeout, stop_flag))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("get fetch_miss_log server failed", KR(ret), K(ls_fetch_ctx));
      }
    }

    if (OB_SUCC(ret)) {
      const common::ObAddr &rpc_svr = OB_NOT_NULL(worker_svr) ? *worker_svr : misslog_task.svr_;
      if (OB_FAIL(fetch_srpc.fetch_log(
          misslog_task.rpc_,
          ls_fetch_ctx.get_tls_id().get_tenant_id(),
          ls_fetch_ctx.get_tls_id().get_ls_id(),
          miss_log_array,
          rpc_svr,
          timeout,
          last_handle_progress))) {
        LOG_ERROR("fetch_misslog_rpc exec failed", KR(ret), K(miss_log_array), K(last_handle_progress), K(misslog_task));
        ret = OB_NEED_RETRY;
      }
    }
  } else if (is_direct_fetching_mode(fetching_mode)) {
    // mock FetchLogSRpc here
    if (OB_FAIL(fetch_miss_log_direct_(miss_log_array, timeout, fetch_srpc, ls_fetch_ctx))) {
      LOG_ERROR("fetch missing log direct failed", KR(ret), K(miss_log_array), K(misslog_task));
      // rewrite ret code to make sure that cdc wouldn't exit because fetch_missing_log_direct_ failed.
      ret = OB_NEED_RETRY;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid fetching mode", KR(ret), K(fetching_mode), K(misslog_task));
  }

  return ret;
}

int ObCDCMissLogHandler::fetch_miss_log_direct_(
    const ObIArray<obrpc::ObCdcLSFetchMissLogReq::MissLogParam> &miss_log_array,
    const int64_t timeout,
    FetchLogSRpc &fetch_log_srpc,
    LSFetchCtx &ls_fetch_ctx)
{
  int ret = OB_SUCCESS;
  ObCdcLSFetchLogResp *resp = NULL;
  LSFetchCtxGetSourceFunctor get_source_func(ls_fetch_ctx);
  LSFetchCtxUpdateSourceFunctor update_source_func(ls_fetch_ctx);
  const int64_t current_progress = ls_fetch_ctx.get_progress();
  const int64_t tenant_id = ls_fetch_ctx.get_tls_id().get_tenant_id();
  const ObLSID &ls_id = ls_fetch_ctx.get_tls_id().get_ls_id();
  archive::LargeBufferPool *buffer_pool = NULL;
  logservice::ObLogExternalStorageHandler *log_ext_handler = NULL;
  ObRpcResultCode rcode;
  SCN cur_scn;
  const int64_t start_fetch_ts = get_timestamp();
  const int64_t time_upper_limit = start_fetch_ts + timeout;
  bool stop_fetch = false;
  bool is_timeout = false;

  if (OB_ISNULL(resp = OB_NEW(ObCdcLSFetchLogResp, "FetchMissResp"))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc ObCdcLSFetchLogResp failed", KR(ret));
  } else if (OB_FAIL(cur_scn.convert_from_ts(current_progress/1000L))) {
    LOG_ERROR("convert log progress to scn failed", KR(ret), K(current_progress));
  } else if (OB_FAIL(ls_fetch_ctx.get_large_buffer_pool(buffer_pool))) {
    LOG_ERROR("get large buffer pool when fetching missing log failed", KR(ret), K(ls_fetch_ctx));
  } else if (OB_FAIL(ls_fetch_ctx.get_log_ext_handler(log_ext_handler))) {
    LOG_ERROR("get log ext handler when fetching missing log failed", KR(ret), K(ls_fetch_ctx));
  } else {
    int64_t fetched_cnt = 0;
    const int64_t arr_cnt = miss_log_array.count();
    resp->set_next_miss_lsn(miss_log_array.at(0).miss_lsn_);
    while (OB_SUCC(ret) && !stop_fetch) {
      bool retry_on_err = false;
      while (OB_SUCC(ret) && fetched_cnt < arr_cnt && !is_timeout) {
        const int64_t start_fetch_entry_ts = get_timestamp();
        const ObCdcLSFetchMissLogReq::MissLogParam &param = miss_log_array.at(fetched_cnt);
        const LSN &missing_lsn = param.miss_lsn_;
        const char *buf;
        int64_t buf_size = 0;
        const bool enable_logservice = ls_fetch_ctx.get_logservice_model();
        ipalf::ILogEntry log_entry(enable_logservice);
        palf::LSN lsn;
        logservice::ObRemoteILogEntryIterator entry_iter(get_source_func, update_source_func);
        resp->set_next_miss_lsn(missing_lsn);

        if (get_timestamp() > time_upper_limit) {
          is_timeout = true;
        } else if (OB_FAIL(entry_iter.init(tenant_id, ls_id, cur_scn, missing_lsn,
            LSN(palf::LOG_MAX_LSN_VAL), buffer_pool, log_ext_handler, archive::ARCHIVE_FILE_DATA_BUF_SIZE, enable_logservice))) {
          LOG_WARN("remote entry iter init failed", KR(ret));
        } else if (OB_FAIL(entry_iter.next(log_entry, lsn, buf, buf_size))) {
          retry_on_err =true;
          LOG_WARN("log entry iter failed to iterate", KR(ret));
        } else {
          if (OB_UNLIKELY(missing_lsn != lsn)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("direct fetch missing_lsn not match", KR(ret), K(tenant_id), K(ls_id),
                K(missing_lsn), K(lsn));
          } else {
            const int64_t entry_size = log_entry.get_serialize_size(lsn);
            int64_t pos = 0;
            resp->inc_log_fetch_time(get_timestamp() - start_fetch_entry_ts);

            if (! resp->has_enough_buffer(entry_size)) {
              ret = OB_BUF_NOT_ENOUGH;
            } else {
              int64_t remain_size = 0;
              char *remain_buf = resp->get_remain_buf(remain_size);
              if (OB_ISNULL(remain_buf)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("remain buffer is null", KR(ret));
              }
              else if (OB_FAIL(log_entry.serialize(lsn, remain_buf, remain_size, pos))) {
                LOG_WARN("missing LogEntry serialize failed", KR(ret), K(remain_size),
                    K(pos), K(missing_lsn), K(log_entry));
              }
              else {
                // TODO: there is an issue in entry_iter.next(), the returned buffer is not as expected
                // MEMCPY(remain_buf, buf, buf_size);
                resp->log_entry_filled(entry_size);
                fetched_cnt++;
              }
            }
          }
        }
      }// while

      if (OB_SUCC(ret)) {
        stop_fetch = true;
      } else if (OB_BUF_NOT_ENOUGH == ret) {
        stop_fetch = true;
        ret = OB_SUCCESS;
      } else if (retry_on_err) {
        ret = OB_SUCCESS;
      }
    } // while
    resp->set_l2s_net_time(0);
    resp->set_svr_queue_time(0);
    resp->set_process_time(get_timestamp() - start_fetch_ts);
  }
  // regard resp not null as sending rpc successfully
  if (OB_NOT_NULL(resp)) {
    resp->set_debug_err(ret);
    if (OB_FAIL(ret)) {
      resp->set_err(OB_ERR_SYS);
    } else {
      resp->set_err(OB_SUCCESS);
    }
    ret = OB_SUCCESS;
    rcode.rcode_ = OB_SUCCESS;
  } else {
    rcode.rcode_ = ret;
    sprintf(rcode.msg_, "failed to allocate fetchlogresp");
  }
  fetch_log_srpc.set_resp(rcode, resp);

  if (OB_NOT_NULL(resp)) {
    OB_DELETE(ObCdcLSFetchLogResp, "FetchMissResp", resp);
    resp = NULL;
  }

  return ret;
}

void ObCDCMissLogHandler::check_feedback_(
    const obrpc::ObCdcLSFetchLogResp &resp,
    bool &has_valid_feedback,
    bool &rpc_fetch_no_log)
{
  const obrpc::FeedbackType& feedback = resp.get_feedback_type();
  has_valid_feedback = (feedback != obrpc::FeedbackType::INVALID_FEEDBACK);
  rpc_fetch_no_log = (obrpc::FeedbackType::LOG_NOT_IN_THIS_SERVER == feedback);

  if (has_valid_feedback) {
    const char *rpc_feedback_info = nullptr;
    switch (feedback) {
      case obrpc::FeedbackType::LAGGED_FOLLOWER:
        rpc_feedback_info = "fetching log on lagged follower";
        break;
      case obrpc::FeedbackType::LOG_NOT_IN_THIS_SERVER:
        rpc_feedback_info = "log not in this server, may be recycled";
        break;
      case obrpc::FeedbackType::LS_OFFLINED:
        rpc_feedback_info = "fetching log on offline logstream";
        break;
      case obrpc::FeedbackType::ARCHIVE_ITER_END_BUT_LS_NOT_EXIST_IN_PALF:
        rpc_feedback_info = "archive log iter end but no valid palf on this server";
        break;
      default:
        rpc_feedback_info = "unknown feedback type";
        break;
    }
    LOG_INFO("[MISS_LOG][RPC_FEEDBACK]", K(rpc_fetch_no_log), K(resp), KCSTRING(rpc_feedback_info));
  }
}

} // namespace libobcdc
} // namespace oceanbase
