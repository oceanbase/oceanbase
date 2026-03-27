/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_TASK_DEFINE_H
#define OB_TASK_DEFINE_H

#include <cstdint>
#include <type_traits>

namespace oceanbase {
namespace lib
{
class ObRateLimiter;
}
namespace share {
enum class ObTaskType {
  GENERIC,
  USER_REQUEST,
  DATA_MAINTAIN,
  ROOT_SERVICE,
  SCHEMA,
  MAX
};

template<typename E>
constexpr typename std::underlying_type<E>::type
toUType(E enumerator) noexcept
{
  return static_cast<typename std::underlying_type<E>::type>(enumerator);
}

// Not support change log rate percentage of each task dynamically.
class ObTaskController {
  using RateLimiter = lib::ObRateLimiter;
  static constexpr int MAX_TASK_ID = toUType(ObTaskType::MAX);
  static constexpr int64_t LOG_RATE_LIMIT = 10 << 20;

public:
  ObTaskController();
  virtual ~ObTaskController();

  int init();
  void stop();
  void wait();
  void destroy();
  void switch_task(ObTaskType task_id);
  void allow_next_syslog();

  void set_log_rate_limit(int64_t limit);

  void set_diag_per_error_limit(int64_t cnt);

  static ObTaskController &get();

private:
  template <ObTaskType id>
  void set_log_rate_pctg(double pctg)
  {
    if (id != ObTaskType::MAX) {
      rate_pctgs_[toUType(id)] = pctg;
    }
  }

  void calc_log_rate();

  RateLimiter *get_limiter(ObTaskType id);

private:
  RateLimiter *limiters_[MAX_TASK_ID];
  double rate_pctgs_[MAX_TASK_ID];
  int64_t log_rate_limit_;

  static ObTaskController instance_;
};

}  // share
}  // oceanbase


#endif /* OB_TASK_DEFINE_H */
