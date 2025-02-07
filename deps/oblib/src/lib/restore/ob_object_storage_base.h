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

#ifndef SRC_LIBRARY_SRC_LIB_RESTORE_OB_OBJECT_STORAGE_BASE_H_
#define SRC_LIBRARY_SRC_LIB_RESTORE_OB_OBJECT_STORAGE_BASE_H_

#include "lib/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/utility/utility.h"

namespace oceanbase
{
namespace common
{

static int64_t OB_STORAGE_MAX_IO_TIMEOUT_US = 20LL * 1000LL * 1000LL;  // 20s
// disable the retry mechanism
static constexpr int64_t DO_NOT_RETRY = 0;

// interface class, used for COS,
// as COS cannot utilize OB's code directly, to avoid compilation errors
template<typename OutcomeType_>
class ObStorageIORetryStrategyBase
{
public:
  using RetType = OutcomeType_;

  ObStorageIORetryStrategyBase(const int64_t timeout_us)
      : start_time_us_(0),  // cannot use virtual func `current_time_us` in constructor
        timeout_us_(timeout_us < 0 ? OB_STORAGE_MAX_IO_TIMEOUT_US : timeout_us) // 0 means never retry
  {}

  // A virtual destructor can lead to link failures, temporarily commented out
  // virtual ~ObStorageIORetryStrategyBase() {}
  void reuse(const int64_t timeout_us)
  {
    start_time_us_ = current_time_us();
    timeout_us_ = (timeout_us < 0 ? OB_STORAGE_MAX_IO_TIMEOUT_US : timeout_us);
  }
  virtual int64_t current_time_us() const = 0;

  bool should_retry(const RetType &outcome, const int64_t attempted_retries) const
  {
    bool bret = should_retry_impl_(outcome, attempted_retries);
    if (bret && is_timeout_(attempted_retries)) {
      bret = false;
    }
    return bret;
  }

  virtual uint32_t calc_delay_time_us(
      const RetType &outcome, const int64_t attempted_retries) const
  {
    static const uint32_t base_delay_us = 25 * 1000; // 25ms
    return base_delay_us * (1 << attempted_retries);
  }

  virtual void log_error(
      const RetType &outcome, const int64_t attempted_retries) const = 0;

protected:
  virtual bool is_timeout_(const int64_t attempted_retries) const = 0;

  virtual bool should_retry_impl_(
      const RetType &outcome, const int64_t attempted_retries) const = 0;

protected:
  int64_t start_time_us_;
  int64_t timeout_us_;
};

// interface class, used for OSS & S3,
template<typename OutcomeType_>
class ObStorageIORetryStrategy : public ObStorageIORetryStrategyBase<OutcomeType_>
{
public:
  using typename ObStorageIORetryStrategyBase<OutcomeType_>::RetType;
  using ObStorageIORetryStrategyBase<OutcomeType_>::start_time_us_;
  using ObStorageIORetryStrategyBase<OutcomeType_>::timeout_us_;
  using ObStorageIORetryStrategyBase<OutcomeType_>::calc_delay_time_us;

  ObStorageIORetryStrategy(const int64_t timeout_us)
      : ObStorageIORetryStrategyBase<OutcomeType_>(timeout_us)
  {
    start_time_us_ = ObTimeUtility::current_time();
  }
  virtual ~ObStorageIORetryStrategy() {}

  virtual int64_t current_time_us() const override
  {
    return ObTimeUtility::current_time();
  }

protected:
  virtual bool is_timeout_(const int64_t attempted_retries) const override
  {
    bool bret = false;
    const int64_t cur_time_us = current_time_us();
    if (cur_time_us >= start_time_us_ + timeout_us_) {
      OB_LOG_RET(WARN, OB_TIMEOUT, "request reach time limit", K(start_time_us_),
          K(timeout_us_), K(cur_time_us), K(attempted_retries));
      bret = true;
    }
    return bret;
  }
};

template<typename FuncType, typename... Args>
using FuncRetType = typename std::result_of<FuncType(Args...)>::type;

template<typename FuncType, typename... Args>
FuncRetType<FuncType, Args...> execute_until_timeout(
    ObStorageIORetryStrategyBase<FuncRetType<FuncType, Args...>> &retry_strategy,
    FuncType func,
    Args... args)
{
  int64_t retries = 0;
  bool should_retry_flag = true;
  // func_ret may be pointer, so use {} construct it
  FuncRetType<FuncType, Args...> func_ret {};
  do {
    func_ret = func(args...);
    if (!retry_strategy.should_retry(func_ret, retries)) {
      should_retry_flag = false;
    } else {
      // if should_retry, log the current error
      retry_strategy.log_error(func_ret, retries);
      uint32_t sleep_time_us = retry_strategy.calc_delay_time_us(func_ret, retries);
      ::usleep(sleep_time_us);
    }
    retries++;
  } while (should_retry_flag);

  return func_ret;
}

class ObObjectStorageTenantGuard
{
public:
  ObObjectStorageTenantGuard(const uint64_t tenant_id, const int64_t timeout_us);
  virtual ~ObObjectStorageTenantGuard();

  static uint64_t get_tenant_id();
  static int64_t get_timeout_us();

private:
  static thread_local uint64_t tl_tenant_id_;
  uint64_t old_tenant_id_;

  static thread_local int64_t tl_timeout_us_;
  int64_t old_timeout_us_;
};

} // common
} // oceanbase

#endif // SRC_LIBRARY_SRC_LIB_RESTORE_OB_OBJECT_STORAGE_BASE_H_
