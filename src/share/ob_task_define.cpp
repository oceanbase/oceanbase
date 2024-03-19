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

#define USING_LOG_PREFIX SHARE

#include "ob_task_define.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/oblog/ob_syslog_rate_limiter.h"
#include "lib/oblog/ob_log.h"
#include <numeric>
#include "lib/thread/thread_pool.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/thread/ob_thread_name.h"
#include "share/ob_define.h"
#include "share/config/ob_server_config.h"

using namespace oceanbase::lib;
using namespace oceanbase::common;

namespace oceanbase {
namespace common {
void allow_next_syslog(int64_t count)
{
  share::ObTaskController::get().allow_next_syslog(count);
}
} // common

namespace share {

class ObSyslogPerErrLimiter : public common::ObISyslogRateLimiter
{
public:
  const static int64_t REFRESH_INTERVAL_US = 1000000;
  ObSyslogPerErrLimiter() : inited_(false), refresh_thread_(*this)
  {
    refresh_thread_.set_thread_count(1);
    set_rate(GCONF.diag_syslog_per_error_limit);
  }

  virtual ~ObSyslogPerErrLimiter() { destroy(); }
  int init();
  void stop()
  {
    if (inited_) {
      refresh_thread_.stop();
    }
  }
  void wait()
  {
    if (inited_) {
      refresh_thread_.wait();
    }
  }
  void destroy()
  {
    if (inited_) {
      refresh_thread_.destroy();
      inited_ = false;
    }
  }


  virtual int do_acquire(int64_t permits, int log_level, int errcode) override;
  virtual bool is_force_allows() const override { return false; }
  virtual void reset_force_allows() override {}

  static ObSyslogPerErrLimiter &instance()
  {
    static ObSyslogPerErrLimiter g_instance;
    return g_instance;
  }

private:
  void per_err_logs(int &first_thread_cnt, int &left_cnt)
  {
    int total_cnt = std::min(static_cast<int32_t>(rate_), INT32_MAX);
    first_thread_cnt = std::min(std::max(total_cnt / 2, 24), 48);
    left_cnt = std::max(0, total_cnt - first_thread_cnt);
  }

  class RefreshThread : public lib::ThreadPool
  {
  public:
    RefreshThread(ObSyslogPerErrLimiter &limiter) : limiter_(limiter) {}
    virtual void run1() override
    {
      lib::set_thread_name("LogLimiterRefresh");
      while (!has_set_stop()) {
        ob_usleep(100000);
        limiter_.refresh();
      }
    }
  private:
    ObSyslogPerErrLimiter &limiter_;
  };

  struct PerErrorItem
  {
    int errcode_;
    volatile int first_tid_;
    volatile int64_t first_log_time_;
    volatile int tid_cnt_;
    volatile int cnt_;
    PerErrorItem * next_;

    // Only fields need printed add here.
    TO_STRING_KV("errcode", errcode_, "dropped", cnt_, "tid", first_tid_);

    PerErrorItem() : errcode_(0), first_tid_(0), first_log_time_(0),
    tid_cnt_(0), cnt_(0), next_(NULL)
    {
    }
  };

  inline PerErrorItem &locate_perr(int errcode)
  {
    PerErrorItem *p = buckets_[static_cast<uint32_t>(errcode) % bucket_cnt_];
    while (NULL != p && p->errcode_ != errcode) {
      p = p->next_;
    }
    if (NULL == p) { // for unknown error code.
      p = &errors_[ARRAYSIZEOF(errors_) - 1];
    }
    return *p;
  }


  void refresh();

private:
  bool inited_;
  const static int64_t bucket_cnt_ = 6151; // prime greater than error count.
  static_assert(bucket_cnt_ > ARRAYSIZEOF(g_all_ob_errnos), "too small bucket cnt");
  PerErrorItem *buckets_[bucket_cnt_];
  PerErrorItem errors_[ARRAYSIZEOF(g_all_ob_errnos) + 1]; // 1 for unknown error.

  ObSEArray<PerErrorItem, 16> refresh_errlogs_;
  RefreshThread refresh_thread_;
};

class ObLogRateLimiter : public common::ObSyslogSimpleRateLimiter
{
  friend class ObTaskController;
public:
  ObLogRateLimiter(ObISyslogRateLimiter *wdiag_log_limiter)
      : wdiag_log_limiter_(wdiag_log_limiter)
  {
  }

  bool is_force_allows() const override
  {
    return OB_UNLIKELY(allows_ > 0);
  }
  void reset_force_allows() override
  {
    if (is_force_allows()) {
      allows_--;
    }
  }

  int do_acquire(int64_t permits, int log_level, int errcode) override {
    return NULL != wdiag_log_limiter_ && log_level == OB_LOG_LEVEL_WARN
        ? wdiag_log_limiter_->do_acquire(1, log_level, errcode)
        : ObSyslogSimpleRateLimiter::do_acquire(permits, log_level, errcode);
  }

private:
  ObISyslogRateLimiter *wdiag_log_limiter_;
  RLOCAL_STATIC(int64_t, allows_);
};

_RLOCAL(int64_t, ObLogRateLimiter::allows_);

// class ObTaskController
ObTaskController ObTaskController::instance_;

ObTaskController::ObTaskController()
    : limiters_(), rate_pctgs_(), log_rate_limit_(LOG_RATE_LIMIT)
{}

ObTaskController::~ObTaskController()
{
  destroy();
}

int ObTaskController::init()
{
  int ret = OB_SUCCESS;
  ObSyslogPerErrLimiter::instance().set_rate(GCONF.diag_syslog_per_error_limit);
  OZ(ObSyslogPerErrLimiter::instance().init());
  for (int i = 0; OB_SUCC(ret) && i < MAX_TASK_ID; i++) {
    limiters_[i] = OB_NEW(ObLogRateLimiter, ObModIds::OB_LOG, &ObSyslogPerErrLimiter::instance());
    if (nullptr == limiters_[i]) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      break;
    }
  }
  if (OB_SUCC(ret)) {
#define LOG_PCTG(ID, PCTG)                      \
    do {                                        \
      set_log_rate_pctg<ID>(PCTG);              \
      get_limiter(ID)->set_name(#ID);           \
    } while (0)

    // Set percentage of each task here.
    // @NOTE: Inquire @yongle.xh before any change.
    LOG_PCTG(ObTaskType::GENERIC, 100.0);  // default limiter
    LOG_PCTG(ObTaskType::USER_REQUEST, 100.0);  // default limiter
    LOG_PCTG(ObTaskType::DATA_MAINTAIN, 100.0);  // default limiter
    LOG_PCTG(ObTaskType::ROOT_SERVICE, 100.0);  // default limiter
    LOG_PCTG(ObTaskType::SCHEMA, 100.0);  // default limiter

#undef LOG_PCTG

    calc_log_rate();
    ObLogger::set_default_limiter(*limiters_[toUType(ObTaskType::GENERIC)]);
    ObLogger::set_tl_type(0);
  } else {
    destroy();
  }

  return ret;
}

void ObTaskController::stop()
{
  ObSyslogPerErrLimiter::instance().stop();
}

void ObTaskController::wait()
{
  ObSyslogPerErrLimiter::instance().wait();
}

void ObTaskController::destroy()
{
  stop();
  wait();
  ObSyslogPerErrLimiter::instance().destroy();
  for (int i = 0; i < MAX_TASK_ID; i++) {
    if (nullptr != limiters_[i]) {
      ob_delete(limiters_[i]);
      limiters_[i] = nullptr;
    }
  }
}

void ObTaskController::switch_task(ObTaskType task_id)
{
  ObLogger::set_tl_limiter(*limiters_[toUType(task_id)]);
  ObLogger::set_tl_type(static_cast<int32_t>(toUType(task_id)));
}

void ObTaskController::allow_next_syslog(int64_t count)
{
  ObLogRateLimiter::allows_ += count;
}

void ObTaskController::set_log_rate_limit(int64_t limit)
{
  if (limit != log_rate_limit_) {
    log_rate_limit_ = limit;
    calc_log_rate();
  }
}

void ObTaskController::set_diag_per_error_limit(int64_t cnt)
{
  ObSyslogPerErrLimiter::instance().set_rate(cnt);
}

void ObTaskController::calc_log_rate()
{
  const double total = std::accumulate(
      rate_pctgs_, rate_pctgs_ + MAX_TASK_ID, .0);
  for (int i = 0; total > 0 && i < MAX_TASK_ID; i++) {
    limiters_[i]->set_rate(
        static_cast<int64_t>(
            rate_pctgs_[i]/total * static_cast<double>(log_rate_limit_)));
  }
}

ObTaskController &ObTaskController::get()
{
  return instance_;
}

ObTaskController::RateLimiter *ObTaskController::get_limiter(ObTaskType id)
{
  return limiters_[toUType(id)];
};

int ObSyslogPerErrLimiter::init()
{
  int ret = OB_SUCCESS;
  int first_tid_cnt = 0;
  int left_cnt = 0;
  per_err_logs(first_tid_cnt, left_cnt);
  for (int64_t i = 0; i < ARRAYSIZEOF(g_all_ob_errnos); i++) {
    int errcode = g_all_ob_errnos[i];
    PerErrorItem &perr = errors_[i];
    perr.errcode_ = errcode;
    perr.tid_cnt_ = first_tid_cnt;
    perr.cnt_ = left_cnt;

    int pos = static_cast<uint32_t>(errcode) % bucket_cnt_;
    perr.next_ = buckets_[pos];
    buckets_[pos] = &perr;
  }
  PerErrorItem &perr = errors_[ARRAYSIZEOF(errors_) - 1];
  perr.errcode_ = INT32_MIN;
  perr.tid_cnt_ = first_tid_cnt;
  perr.cnt_ = left_cnt;


  OZ(refresh_thread_.init());
  OZ(refresh_thread_.start());

  if (OB_SUCC(ret)) {
    inited_ = true;
  }
  return ret;
}

int ObSyslogPerErrLimiter::do_acquire(int64_t permits, int log_level, int errcode)
{
  UNUSED(permits);
  int ret = OB_SUCCESS;
  if (log_level == OB_LOG_LEVEL_WARN && inited_ && rate_ > 0) {
    PerErrorItem &perr = locate_perr(errcode);
    int mytid = static_cast<int32_t>(GETTID());
    int tid = ATOMIC_LOAD(&perr.first_tid_);
    int tidcnt = -1;
    if (0 == tid) {
      ATOMIC_SET(&perr.first_log_time_, ObTimeUtil::fast_current_time());
      MEM_BARRIER();
      if (ATOMIC_CAS(&perr.first_tid_, 0, mytid)) {
        tid = mytid;
      }
    }
    if (tid == mytid) {
      tidcnt= ATOMIC_AAF(&perr.tid_cnt_, -1);
    }
    if (tidcnt < 0) {
      // We continuously issue permints, instead of spread to one second.
      if (ATOMIC_AAF(&perr.cnt_, -1) < 0) {
        ret = OB_EAGAIN;
      }
    }
  }

  return ret;
}

void ObSyslogPerErrLimiter::refresh()
{
  int ret = OB_SUCCESS;
  int64_t cur_time = ObTimeUtility::current_time();

  int first_tid_cnt = 0;
  int left_cnt = 0;
  per_err_logs(first_tid_cnt, left_cnt);

  refresh_errlogs_.reuse();
  PerErrorItem cur;

  for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(errors_); i++) {
    auto &perr = errors_[i];
    int tid = ATOMIC_LOAD(&perr.first_tid_);
    MEM_BARRIER();
    if (tid > 0) {
      int64_t log_time = ATOMIC_LOAD(&perr.first_log_time_);
      if (log_time > 0 && cur_time - log_time> REFRESH_INTERVAL_US) {
        cur.errcode_ = perr.errcode_;
        cur.first_tid_ = tid;
        cur.first_log_time_ = log_time;
        cur.tid_cnt_ = ATOMIC_LOAD(&perr.tid_cnt_);
        cur.cnt_ = ATOMIC_LOAD(&perr.cnt_);
        ATOMIC_AAF(&perr.tid_cnt_, first_tid_cnt - cur.tid_cnt_);
        ATOMIC_AAF(&perr.cnt_, left_cnt - cur.cnt_);
        ATOMIC_SET(&perr.first_tid_, 0);
        if (cur.cnt_ < 0) {
          cur.cnt_ = -cur.cnt_;
          OZ(refresh_errlogs_.push_back(cur));
        }
      }
    }
  }
  if (!refresh_errlogs_.empty()) {
    ObTaskController::get().allow_next_syslog();
    LOG_WARN("Throttled WDIAG logs in last second",
             "details {error code, dropped logs, earliest tid}",
             refresh_errlogs_);
  }
}

}  // share
}  // oceanbase
