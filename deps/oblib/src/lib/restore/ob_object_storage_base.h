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
// time
constexpr int64_t MS_US = 1000LL;
constexpr int64_t S_US = 1000LL * MS_US;
constexpr int64_t MIN_US = 60LL * S_US;
constexpr int64_t HOUR_US = 60LL * MIN_US;
constexpr int64_t DAY_US = 24LL * HOUR_US;

// size
constexpr int64_t KB = 1024LL;
constexpr int64_t MB = 1024LL * KB;
constexpr int64_t GB = 1024LL * MB;

} // common

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
        timeout_us_(timeout_us < 0 ? OB_STORAGE_MAX_IO_TIMEOUT_US : timeout_us), // 0 means never retry
        delay_us_(BASE_DELAY_US)
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
      const RetType &outcome, const int64_t attempted_retries)
  {
    if (attempted_retries != 0 && delay_us_ < MAX_DELAY_US) {
      delay_us_ = delay_us_ * DELAY_EXPONENT;
    }
    return MIN(delay_us_, MAX_DELAY_US);
  }

  virtual void log_error(
      const RetType &outcome, const int64_t attempted_retries) const = 0;

protected:
  virtual bool is_timeout_(const int64_t attempted_retries) const = 0;

  virtual bool should_retry_impl_(
      const RetType &outcome, const int64_t attempted_retries) const = 0;

protected:
  static const uint32_t BASE_DELAY_US = 25 * 1000;        // 25ms
  static const uint32_t MAX_DELAY_US = 5 * 1000 * 1000LL; // 5s
  static const uint32_t DELAY_EXPONENT = 3;
  int64_t start_time_us_;
  int64_t timeout_us_;
  int64_t delay_us_;
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
    Args && ... args)
{
  int64_t retries = 0;
  bool should_retry_flag = true;
  // func_ret may be pointer, so use {} construct it
  FuncRetType<FuncType, Args...> func_ret {};
  do {
    func_ret = func(std::forward<Args>(args)...);
    if (!retry_strategy.should_retry(func_ret, retries)) {
      should_retry_flag = false;
    } else {
      // if should_retry, log the current error
      uint32_t sleep_time_us = retry_strategy.calc_delay_time_us(func_ret, retries);
      retry_strategy.log_error(func_ret, retries);
      ob_usleep(sleep_time_us);
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
