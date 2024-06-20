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

#ifndef _OCEABASE_COMMON_OB_COMMON_UTILITY_H_
#define _OCEABASE_COMMON_OB_COMMON_UTILITY_H_
#include <sys/time.h>
#include "lib/ob_define.h"
#include "lib/time/ob_tsc_timestamp.h"
namespace oceanbase
{
namespace common
{
extern const char *print_server_role(const common::ObServerRole server_role);

//@brief recursive function call should use this function to check if recursion is too deep
//to avoid stack overflow, default reserved statck size is 1M
extern int64_t get_reserved_stack_size();
extern void set_reserved_stack_size(int64_t reserved_size);
extern int check_stack_overflow(
    bool &is_overflow,
    int64_t reserved_stack_size = get_reserved_stack_size(),
    int64_t *used_size = nullptr);
extern int get_stackattr(void *&stackaddr, size_t &stacksize);
extern void set_stackattr(void *stackaddr, size_t stacksize);

// return OB_SIZE_OVERFLOW if stack overflow
inline int check_stack_overflow(void)
{
  bool overflow = false;
  int ret = check_stack_overflow(overflow);
  return OB_LIKELY(OB_SUCCESS == ret) && OB_UNLIKELY(overflow) ? OB_SIZE_OVERFLOW : ret;
}

/**
 * @brief The ObFatalErrExtraInfoGuard class is used for printing extra info, when fatal error happens.
 *        The of pointer of the class is maintained on thread local.
 */
class ObFatalErrExtraInfoGuard
{
public:
  explicit ObFatalErrExtraInfoGuard();
  virtual ~ObFatalErrExtraInfoGuard();
  static const ObFatalErrExtraInfoGuard *get_thd_local_val_ptr();
  virtual int64_t to_string(char* buf, const int64_t buf_len) const;
private:
  static ObFatalErrExtraInfoGuard *&get_val();
  ObFatalErrExtraInfoGuard *last_;
};

inline int64_t get_cur_ts()
{
  return OB_TSC_TIMESTAMP.fast_current_time();
}

class ObBasicTimeGuard
{
public:
  struct ClickInfo
  {

    static bool compare(const ClickInfo &left, const ClickInfo &right)
    {
      return left.seq_ < right.seq_;
    }
    int32_t seq_;
    int32_t cost_time_;
    const char* mod_;
  };
  explicit ObBasicTimeGuard(const char *owner, const char *start, const char *end)
    : enable_(tl_enable_time_guard), owner_(owner), start_(start), end_(end)
  {
    if (OB_LIKELY(enable_)) {
      last_time_guard_ = tl_time_guard;
      if (NULL != tl_time_guard) {
        tl_time_guard->click(start_);
      }
      tl_time_guard = this;
    } else {
      last_time_guard_ = NULL;
    }
    start_ts_ = get_cur_ts();
    last_ts_ = start_ts_;
    click_count_ = 0;
    MEMSET(click_infos_, 0, sizeof(click_infos_));
  }
  ~ObBasicTimeGuard()
  {
    if (OB_LIKELY(enable_)) {
      tl_time_guard = last_time_guard_;
      if (NULL != tl_time_guard) {
        tl_time_guard->click(end_);
      }
    }
  }
  static ObBasicTimeGuard *get_tl_time_guard()
  {
    return tl_time_guard;
  }
  static void time_guard_click(const char *mod)
  {
    if (OB_LIKELY(tl_enable_time_guard) && NULL != tl_time_guard) {
      tl_time_guard->click(mod);
    }
  }
  int64_t get_start_ts() const
  {
    return start_ts_;
  }
  int64_t get_diff() const
  {
    return get_cur_ts() - start_ts_;
  }
  int64_t to_string(char *buf, const int64_t buf_len) const;
private:
  void click(const char *mod)
  {
    const int64_t cur_ts = get_cur_ts();
    const int64_t cost_time = cur_ts - last_ts_;
    last_ts_ = cur_ts;
    int64_t index = 0;
    bool record_click = true;
    if (OB_LIKELY(click_count_ < MAX_CLICK_COUNT)) {
      index = click_count_;
    } else {
      int64_t min_cost_time = cost_time;
      for (int32_t i = 0; i < MAX_CLICK_COUNT; ++i) {
        if (click_infos_[i].cost_time_ < min_cost_time) {
          index = i;
          min_cost_time = click_infos_[i].cost_time_;
        }
      }
      record_click = cost_time > min_cost_time;
    }
    if (record_click) {
      click_infos_[index].seq_ = (int32_t)click_count_++;
      click_infos_[index].cost_time_ = (int32_t)cost_time;
      click_infos_[index].mod_ = mod;
    }
  }
public:
  static __thread bool tl_enable_time_guard;
protected:
  static const int64_t MAX_CLICK_COUNT = 16;
  static __thread ObBasicTimeGuard *tl_time_guard;
private:
  const bool enable_;
  ObBasicTimeGuard *last_time_guard_;
  int64_t start_ts_;
  int64_t last_ts_;
  int64_t click_count_;
  const char *owner_;
  const char *start_;
  const char *end_;
  ClickInfo click_infos_[MAX_CLICK_COUNT];
};

#define BASIC_TIME_GUARD_CLICK(mod) ObBasicTimeGuard::time_guard_click(mod)

#define BASIC_TIME_GUARD(name, owner) ObBasicTimeGuard name(owner, owner"#start", owner"#end");


} // end of namespace common
} // end of namespace oceanbase
#endif /* _OCEABASE_COMMON_OB_COMMON_UTILITY_H_ */
