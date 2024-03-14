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

/**
 * ObOccamTimeGuard/ObOccamTimeGuardDetectHung follows the Occam's razor principle and value semantics.
 * It only requires the minimum necessary information, and then things will be done.
 *
 * Occam’s razor, also spelled Ockham’s razor, also called law of economy or law of parsimony,
 * principle stated by the Scholastic philosopher William of Ockham (1285–1347/49) that
 * “plurality should not be posited without necessity.”
 * The principle gives precedence to simplicity: of two competing theories,
 * the simpler explanation of an entity is to be preferred.
 * The principle is also expressed as “Entities are not to be multiplied beyond necessity.”
 **/

#ifndef OCEANBASE_LIB_TASK_OB_EASY_TIME_GUARD_H
#define OCEANBASE_LIB_TASK_OB_EASY_TIME_GUARD_H

#include "lib/atomic/ob_atomic.h"
#include "lib/thread_local/ob_tsi_utils.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/literals/ob_literals.h"
#include "share/ob_define.h"
#include "share/ob_occam_thread_pool.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace common
{

namespace occam_thread_pool
{
class ObOccamThread;
}

namespace occam
{

// this happens when compile, no runtime cost, zero overhead.
template <typename T, size_t S>
inline constexpr const T *get_file_name_without_dir(const T (& str)[S], size_t i = S - 1)
{
  static_assert(S > 0, "file name char array length must greater than 0.");
  return (str[i] == '/' || str[i] == '\\') ? &str[i + 1] : (i > 0 ? get_file_name_without_dir(str, i - 1) : 0);
}

template <typename T>
inline constexpr const T *get_file_name_without_dir(T (& str)[1]) { return &str[0]; }

class ObOccamTimeGuardDetectHung;
#define THREAD_HUNG_DETECTOR ObThreadHungDetector::get_instance()

class ObThreadHungDetector
{
  friend class ObOccamTimeGuardDetectHung;
public:
  static constexpr int64_t POW_OF_2 = 12;
  static constexpr int64_t MAX_THREAD_NUM = 1LL << POW_OF_2;// 4096
  static constexpr int64_t MAX_THREAD_NUM_MASK = MAX_THREAD_NUM - 1;
private:
  ObThreadHungDetector() : back_thread_(nullptr) {}
  ~ObThreadHungDetector()
  {
    destroy();
  }
public:
  int init() {
    int ret = OB_SUCCESS;
    back_thread_ = (occam::ObOccamThread*)ob_malloc(sizeof(occam::ObOccamThread), "OccamTimeGuard");
    if (back_thread_ == nullptr) {
      OCCAM_LOG(ERROR, "malloc back thread failed");
    } else {
      new(back_thread_) occam::ObOccamThread();
      for (int64_t idx = 0; idx < MAX_THREAD_NUM; ++idx) {
        new(&points_[idx]) ClickPoint();
      }
      if (OB_FAIL(back_thread_->init_and_start([this]() {
        while(true) {
          IGNORE_RETURN lib::Thread::update_loop_ts();
          if (!back_thread_->is_stopped()) {
            for (int64_t idx = 0; idx < MAX_THREAD_NUM; ++idx) {
              ClickPoint point = points_[idx].atomic_copy();
              int64_t cur_ts = ObTimeUtility::fast_current_time();
              if (point.timeout_ts_ != 0 && cur_ts > point.timeout_ts_) {
                int64_t allow_print_ts = ATOMIC_LOAD(&next_print_ts_[idx]);
                if (cur_ts > allow_print_ts) {
                  int64_t next_allow_print_ts = point.timeout_ts_ + 30_s;
                  next_print_ts_[idx] = next_allow_print_ts;// only this thread read it, so no need atomic write
                  OCCAM_LOG_RET(WARN, OB_ERR_UNEXPECTED, "thread maybe hung!", K(point), K(idx), KTIMERANGE(next_allow_print_ts, HOUR, MSECOND));
                } else {
                  OCCAM_LOG(DEBUG, "this thread has been repoted hung, wait for next print time",
                                K(point), K(idx), KTIME(allow_print_ts));
                }
              }
            }
            ob_usleep(static_cast<uint32_t>(500_ms));
          } else {
            OCCAM_LOG(INFO, "thread hung detect thread is stopped");
            break;
          }
        }
      }, false))) {
        OCCAM_LOG(ERROR, "init back thread failed");
      }
    }
    return ret;
  }
  void stop()
  {
    if (back_thread_ != nullptr) {
      back_thread_->stop();
    }
  }
  void wait()
  {
    if (back_thread_ != nullptr) {
      back_thread_->wait();
    }
  }
  void destroy()
  {
    stop();
    wait();
    if (back_thread_ != nullptr) {
      back_thread_->destroy();
      ob_free(back_thread_);
      back_thread_ = nullptr;
    }
  }
  static ObThreadHungDetector &get_instance() { static ObThreadHungDetector d; return d; }
  struct ClickPoint
  {
    ClickPoint() : thread_id_(-1), timeout_ts_(0), file_(nullptr),
                   func_name_(nullptr), last_click_ts_(0), version_(0), last_click_line_(0) {}
    int64_t to_string(char *buf, const int64_t buf_len) const
    {
      int64_t pos = 0;
      common::databuff_printf(buf, buf_len, pos, "[thread id=%ld, ", thread_id_);
      common::databuff_printf(buf, buf_len, pos, "timeout ts=%s, ", common::ObTime2Str::ob_timestamp_str_range<HOUR, MSECOND>(timeout_ts_));
      common::databuff_printf(buf, buf_len, pos, "last click point=\"%s:%s:%d\", ",
                                                 file_, func_name_, last_click_line_);
      common::databuff_printf(buf, buf_len, pos, "last click ts=%s]", common::ObTime2Str::ob_timestamp_str_range<HOUR, MSECOND>(last_click_ts_));
      return pos;
    }
    void reset()
    {
      ++version_;
      MEM_BARRIER();
      timeout_ts_ = 0;
      file_ = nullptr;
      func_name_ = nullptr;
      last_click_ts_ = 0;
      last_click_line_ = 0;
      MEM_BARRIER();
      ++version_;
      version_ = 0;
      thread_id_ = -1;
    }
    ClickPoint atomic_copy()// sequential consistency
    {
      int64_t read_version = -1;
      ClickPoint click_point;
      do {
        if (OB_UNLIKELY(read_version != -1)) {// not first time read, there is conflict read/write
          PAUSE();// avoid busy-while
        }
        read_version = version_;
        MEM_BARRIER();
        click_point.thread_id_ = thread_id_;
        click_point.timeout_ts_ = timeout_ts_;
        click_point.file_ =file_;
        click_point.func_name_ = func_name_;
        click_point.last_click_ts_ = last_click_ts_;
        click_point.last_click_line_ = last_click_line_;
        MEM_BARRIER();
      } while ((read_version != version_) || (read_version & 1));// double check to ensure consistency
      return click_point;
    }
    int64_t thread_id_;
    int64_t timeout_ts_;
    const char *file_;
    const char *func_name_;
    int64_t last_click_ts_;
    int64_t version_;// sequence lock flag
    uint16_t last_click_line_;
  } CACHE_ALIGNED;
  ClickPoint points_[MAX_THREAD_NUM];// used in both user thread and backgroud scan thread
  int64_t next_print_ts_[MAX_THREAD_NUM] = { 0 };// used in both user thread and backgroud scan thread
  occam::ObOccamThread *back_thread_;
private:
  class ClickPointIdx// thread local only
  {
  public:
    ClickPointIdx() : idx_(-1) {}
    ~ClickPointIdx()
    {
      if (idx_ != -1) {
        THREAD_HUNG_DETECTOR.points_[idx_].reset();
        idx_ = -1;
      }
    }
    int64_t get_idx()
    {
      if (OB_UNLIKELY(idx_ == -1)) {
        int64_t start_idx = (GETTID() & MAX_THREAD_NUM_MASK);
        for (int64_t idx = start_idx; idx < start_idx + MAX_THREAD_NUM; ++idx) {
          ClickPoint &point = THREAD_HUNG_DETECTOR.points_[idx & MAX_THREAD_NUM_MASK];
          if (ATOMIC_LOAD(&point.thread_id_) == -1) {
            int64_t thread_id = GETTID();
            if (-1 == ATOMIC_CAS(&point.thread_id_, -1, thread_id)) {
              idx_ = idx & MAX_THREAD_NUM_MASK;
              OCCAM_LOG(INFO, "init point thread id with",
                           KP(&point), K(idx_), K(point), K(thread_id));
              break;
            }
          }
        }
      }
      return idx_;
    }
  private:
    int64_t idx_;
  };
  RLOCAL_STATIC(ClickPointIdx, click_point_idx);
};

class ObOccamTimeGuard
{
public:
  ObOccamTimeGuard(const uint64_t warn_threshold,
                   const char *file,
                   const char *func,
                   const char *mod)
  :warn_threshold_(warn_threshold),
  idx_(0),
  last_click_ts_(common::ObTimeUtility::current_time()),
  file_(file),
  func_name_(func),
  log_mod_(mod)
  {
    static_assert(CAPACITY > 0, "CAPACITY must greater than 0");
  }
  ~ObOccamTimeGuard()
  {
    int64_t total_cost = 0;
    for (int64_t idx = 0; idx < idx_; ++idx) {
      total_cost += click_poinsts_[idx];
    }
    total_cost += common::ObTimeUtility::current_time() - last_click_ts_;
    if (OB_UNLIKELY(total_cost >= warn_threshold_)) {
      constexpr int buffer_size = 256;
      char strbuffer[buffer_size] = { 0 };
      int n = snprintf(strbuffer, buffer_size, "cost too much time:%s:%s, ",
                       file_,
                       func_name_);
      if (n >= buffer_size) {
        snprintf(&strbuffer[buffer_size - 6], 6, "..., ");
      }
      ::oceanbase::common::OB_PRINT(log_mod_, OB_LOG_LEVEL_DIRECT_NO_ERRCODE(WARN), OB_SUCCESS, strbuffer, LOG_KVS(K(*this)));
    }
  }
  void reuse()
  {
    idx_ = 0;
    last_click_ts_ = common::ObTimeUtility::current_time();
  }
  bool is_timeout()
  {
    int64_t total_cost = 0;
    for (int64_t idx = 0; idx < idx_; ++idx) {
      total_cost += click_poinsts_[idx];
    }
    total_cost += common::ObTimeUtility::current_time() - last_click_ts_;
    return total_cost > warn_threshold_;
  }
  int64_t get_total_time() const
  {
    int64_t total_cost = 0;
    for (int64_t idx = 0; idx < idx_; ++idx) {
      total_cost += click_poinsts_[idx];
    }
    total_cost += common::ObTimeUtility::fast_current_time() - last_click_ts_;
    return total_cost;
  }
  bool click(const uint16_t line)
  {
    if (OB_LIKELY(idx_ < CAPACITY)) {
      int64_t now = common::ObTimeUtility::current_time();
      line_array_[idx_] = static_cast<uint16_t>(line);
      click_poinsts_[idx_] = now - last_click_ts_;
      last_click_ts_ = now;
      ++idx_;
    }
    return true;
  }
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    fmt_ts_to_meaningful_str(buf, buf_len, pos, "|threshold", warn_threshold_);
    int64_t start_click_ts = last_click_ts_;
    for (int64_t idx = 0; idx < idx_; ++idx) {
      start_click_ts -= click_poinsts_[idx];
    }
    common::databuff_printf(buf, buf_len, pos, "start at %s|", common::ObTime2Str::ob_timestamp_str_range<HOUR, MSECOND>(start_click_ts));
    int64_t total_cost = 0;
    for (int64_t idx = 0; idx < idx_; ++idx) {
      fmt_ts_to_meaningful_str(buf, buf_len, pos, line_array_[idx], click_poinsts_[idx]);
      total_cost += click_poinsts_[idx];
    }
    total_cost += common::ObTimeUtility::current_time() - last_click_ts_;
    fmt_ts_to_meaningful_str(buf, buf_len, pos, "total", total_cost);
    if (pos != 0 && pos < buf_len) {
      pos -= 1;
    }
    return pos;
  }
protected:
  void fmt_ts_to_meaningful_str(char *buf,
                                const int64_t buf_len,
                                int64_t &pos,
                                const uint16_t line,
                                const int64_t ts) const
  {
    if (line != UINT16_MAX) {
      common::databuff_printf(buf, buf_len, pos, "%d", line);
    } else {
      common::databuff_printf(buf, buf_len, pos, "end");
    }
    if (ts < 1_ms) {
      common::databuff_printf(buf, buf_len, pos, "=%ldus|", ts);
    } else if (ts < 1_s) {
      common::databuff_printf(buf, buf_len, pos, "=%.2lfms|", double(ts) / 1_ms);
    } else {
      common::databuff_printf(buf, buf_len, pos, "=%.2lfs|", double(ts) / 1_s);
    }
  }
  void fmt_ts_to_meaningful_str(char *buf,
                                const int64_t buf_len,
                                int64_t &pos,
                                const char *lvalue,
                                const int64_t ts) const
  {
    common::databuff_printf(buf, buf_len, pos, "%s", lvalue);
    if (ts < 1_ms) {
      common::databuff_printf(buf, buf_len, pos, "=%ldus|", ts);
    } else if (ts < 1_s) {
      common::databuff_printf(buf, buf_len, pos, "=%.2lfms|", double(ts) / 1_ms);
    } else {
      common::databuff_printf(buf, buf_len, pos, "=%.2lfs|", double(ts) / 1_s);
    }
  }
protected:
  static constexpr int64_t CAPACITY = 16;
protected:
  const uint64_t warn_threshold_;
  uint32_t idx_;
  int64_t last_click_ts_;
  const char * const file_;
  const char * const func_name_;
  const char * const log_mod_;
  uint16_t line_array_[CAPACITY];
  uint64_t click_poinsts_[CAPACITY];
};

class ObOccamFastTimeGuard// must used in same thread
{
public:
  ObOccamFastTimeGuard(const uint32_t warn_threshold,
                       const char *file,
                       const char *func,
                       const char *mod)
  :warn_threshold_(warn_threshold),
  idx_(0),
  last_click_ts_(common::ObTscTimestamp::get_instance().current_time()),
  file_(file),
  func_name_(func),
  log_mod_(mod)
  {
    static_assert(CAPACITY > 0, "CAPACITY must greater than 0");
  }
  ~ObOccamFastTimeGuard()
  {
    int64_t total_cost = 0;
    for (int64_t idx = 0; idx < idx_; ++idx) {
      total_cost += click_poinsts_[idx];
    }
    total_cost += common::ObTscTimestamp::get_instance().current_time() - last_click_ts_;
    if (OB_UNLIKELY(total_cost >= warn_threshold_)) {
      constexpr int buffer_size = 256;
      char strbuffer[buffer_size] = { 0 };
      int n = snprintf(strbuffer, buffer_size, "cost too much time:%s:%s, ",
                       file_,
                       func_name_);
      if (n >= buffer_size) {
        snprintf(&strbuffer[buffer_size - 6], 6, "..., ");
      }
      ::oceanbase::common::OB_PRINT(log_mod_, OB_LOG_LEVEL_DIRECT_NO_ERRCODE(WARN), OB_SUCCESS, strbuffer, LOG_KVS(K(*this)));
    }
  }
  bool is_timeout()
  {
    int64_t total_cost = 0;
    for (int64_t idx = 0; idx < idx_; ++idx) {
      total_cost += click_poinsts_[idx];
    }
    total_cost += common::ObTscTimestamp::get_instance().current_time() - last_click_ts_;
    return total_cost > warn_threshold_;
  }
  bool click(const uint16_t line)
  {
    if (OB_LIKELY(idx_ < CAPACITY)) {
      int64_t now = common::ObTscTimestamp::get_instance().current_time();
      line_array_[idx_] = static_cast<uint16_t>(line);
      click_poinsts_[idx_] = static_cast<uint32_t>(now - last_click_ts_);
      last_click_ts_ = now;
      ++idx_;
    }
    return true;
  }
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    fmt_ts_to_meaningful_str(buf, buf_len, pos, "|threshold", warn_threshold_);
    int64_t start_click_ts = common::ObTimeUtility::current_time();
    for (int64_t idx = 0; idx < idx_; ++idx) {
      start_click_ts -= click_poinsts_[idx];
    }
    common::databuff_printf(buf, buf_len, pos, "start at %s|", common::ObTime2Str::ob_timestamp_str_range<HOUR, MSECOND>(start_click_ts));
    int64_t total_cost = 0;
    for (int64_t idx = 0; idx < idx_; ++idx) {
      fmt_ts_to_meaningful_str(buf, buf_len, pos, line_array_[idx], click_poinsts_[idx]);
      total_cost += click_poinsts_[idx];
    }
    total_cost += common::ObTscTimestamp::get_instance().current_time() - last_click_ts_;
    fmt_ts_to_meaningful_str(buf, buf_len, pos, "total", total_cost);
    if (pos != 0 && pos < buf_len) {
      pos -= 1;
    }
    return pos;
  }
protected:
  void fmt_ts_to_meaningful_str(char *buf,
                                const int64_t buf_len,
                                int64_t &pos,
                                const uint16_t line,
                                const int64_t ts) const
  {
    if (line != UINT16_MAX) {
      common::databuff_printf(buf, buf_len, pos, "%d", line);
    } else {
      common::databuff_printf(buf, buf_len, pos, "end");
    }
    if (ts < 1_ms) {
      common::databuff_printf(buf, buf_len, pos, "=%ldus|", ts);
    } else if (ts < 1_s) {
      common::databuff_printf(buf, buf_len, pos, "=%.2lfms|", double(ts) / 1_ms);
    } else {
      common::databuff_printf(buf, buf_len, pos, "=%.2lfs|", double(ts) / 1_s);
    }
  }
  void fmt_ts_to_meaningful_str(char *buf,
                                const int64_t buf_len,
                                int64_t &pos,
                                const char *lvalue,
                                const int64_t ts) const
  {
    common::databuff_printf(buf, buf_len, pos, "%s", lvalue);
    if (ts < 1_ms) {
      common::databuff_printf(buf, buf_len, pos, "=%ldus|", ts);
    } else if (ts < 1_s) {
      common::databuff_printf(buf, buf_len, pos, "=%.2lfms|", double(ts) / 1_ms);
    } else {
      common::databuff_printf(buf, buf_len, pos, "=%.2lfs|", double(ts) / 1_s);
    }
  }
protected:
  static constexpr int64_t CAPACITY = 16;
protected:
  const uint32_t warn_threshold_;
  uint32_t idx_;
  int64_t last_click_ts_;
  const char * const file_;
  const char * const func_name_;
  const char * const log_mod_;
  uint16_t line_array_[CAPACITY];
  uint32_t click_poinsts_[CAPACITY];
};

class ObOccamTimeGuardDetectHung : public ObOccamFastTimeGuard
{
public:
  ObOccamTimeGuardDetectHung(const uint32_t warn_threshold,
                             const uint32_t hung_threshold,
                             const char *file,
                             const char *func,
                             const char *mod,
                             const uint16_t line) :
    ObOccamFastTimeGuard(warn_threshold, file, func, mod),
    g_idx_(-1),
    saved_timeout_ts_(0),
    saved_file_(0),
    saved_func_name_(nullptr),
    saved_last_click_ts_(0),
    saved_last_click_line_(0) {
    g_idx_ = ObThreadHungDetector::click_point_idx.get_idx();
    if (OB_LIKELY(g_idx_ != -1)) {
      // copy to stack
      ObThreadHungDetector::ClickPoint &g_point = THREAD_HUNG_DETECTOR.points_[g_idx_];
      if (OB_LIKELY(g_point.timeout_ts_ != 0)) {
        saved_timeout_ts_ = g_point.timeout_ts_;
        saved_file_ = g_point.file_;
        saved_func_name_ = g_point.func_name_;
        saved_last_click_ts_ = g_point.last_click_ts_;
        saved_last_click_line_ = g_point.last_click_line_;
      }
      int64_t cur_ts = common::ObTimeUtility::current_time();
      int64_t hung_timeout_ts = cur_ts + hung_threshold;
      // recover global slot
      ++g_point.version_;
      MEM_BARRIER();// sequence lock, design for single-thread-write, multi-thread-read
      g_point.timeout_ts_ = hung_timeout_ts;
      g_point.file_ = file_;
      g_point.func_name_ = func_name_;
      g_point.last_click_ts_ = last_click_ts_;
      g_point.last_click_line_ = line;
      MEM_BARRIER();
      ++g_point.version_;
    } else {
      OCCAM_LOG_RET(WARN, OB_ERR_UNEXPECTED, "this time guard will not detect thread hung, cause global slot is not enough",
                   K(file), K(func), K(line), K(lbt()));
    }
  }
  ~ObOccamTimeGuardDetectHung() {
    if (OB_LIKELY(g_idx_ != -1)) {
      ObThreadHungDetector::ClickPoint &g_point = THREAD_HUNG_DETECTOR.points_[g_idx_];
      if (saved_timeout_ts_ != 0) {
        ++g_point.version_;
        MEM_BARRIER();
        g_point.timeout_ts_ = saved_timeout_ts_;
        g_point.file_ = saved_file_;
        g_point.func_name_ = saved_func_name_;
        g_point.last_click_ts_ = saved_last_click_ts_;
        g_point.last_click_line_ = saved_last_click_line_;
        MEM_BARRIER();
        ++g_point.version_;
      } else {
        ++g_point.version_;
        MEM_BARRIER();
        g_point.timeout_ts_ = 0;
        MEM_BARRIER();
        ++g_point.version_;
      }
      ATOMIC_STORE(&THREAD_HUNG_DETECTOR.next_print_ts_[g_idx_], 0);
    }
  }
  bool click(const uint16_t line) {
    ObOccamFastTimeGuard::click(line);
    if (OB_LIKELY(g_idx_ != -1)) {
      ObThreadHungDetector::ClickPoint &g_point = THREAD_HUNG_DETECTOR.points_[g_idx_];
      ++g_point.version_;
      MEM_BARRIER();
      g_point.last_click_ts_ = last_click_ts_;
      g_point.last_click_line_ = line;
      MEM_BARRIER();
      ++g_point.version_;
    }
    return true;
  }
private:
  int64_t g_idx_;
  int64_t saved_timeout_ts_;
  const char *saved_file_;
  const char *saved_func_name_;
  int64_t saved_last_click_ts_;
  uint16_t saved_last_click_line_;
};

struct TimeGuardFactory
{
  static ObOccamTimeGuard make_guard(const int64_t threshold1,
                                     const char *file,
                                     const char *func,
                                     const int64_t line,
                                     const char *mod) {
    UNUSED(line);
    return ObOccamTimeGuard(static_cast<uint32_t>(threshold1), file, func, mod);
  }
  static ObOccamTimeGuardDetectHung make_guard(const int64_t threshold1,
                                               const int64_t threshold2,
                                               const char *file,
                                               const char *func,
                                               const int64_t line,
                                               const char *mod) {
    return ObOccamTimeGuardDetectHung(static_cast<uint32_t>(threshold1),
        static_cast<uint32_t>(threshold2), file, func, mod, line);
  }
};

#define TIMEGUARD_INIT(mod, ...) auto __time_guard__ = oceanbase::common::occam::TimeGuardFactory::\
                                                       make_guard(__VA_ARGS__,\
                                                                  oceanbase::common::occam::get_file_name_without_dir(__FILE__),\
                                                                  __PRETTY_FUNCTION__,\
                                                                  __LINE__,\
                                                                  "["#mod"] ")
#define CLICK() ({ static_assert(__LINE__ >= 0 && __LINE__ <= UINT16_MAX, "line num greater than 65535"); (__time_guard__.click(__LINE__));})
#define CLICK_FAIL(stmt) (CLICK(), OB_FAIL(stmt))
#define CLICK_TMP_FAIL(stmt) (CLICK(), OB_TMP_FAIL(stmt))

}// namespace occam
}// namespace common
}// namespace oceanbase

#endif
