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

#ifndef OB_RUNNING_MODE_H_
#define OB_RUNNING_MODE_H_

#include "lib/ob_define.h"
namespace oceanbase
{
namespace lib
{
extern bool mtl_is_mini_mode();

struct ObRunningModeConfig
{
  static const int64_t MIN_MEM;
  static const int64_t MINI_MEM_LOWER;
  static const int64_t MINI_MEM_UPPER;
  static const int64_t MINI_CPU_UPPER;
  bool mini_mode_ = false;
  bool mini_cpu_mode_ = false;
  int64_t memory_limit_ = 0;
  bool use_ipv6_ = false;
  static ObRunningModeConfig &instance();
private:
  ObRunningModeConfig() = default;
};

inline ObRunningModeConfig &ObRunningModeConfig::instance()
{
  static ObRunningModeConfig instance;
  return instance;
}

inline bool is_mini_mode()
{
  return ObRunningModeConfig::instance().mini_mode_ || mtl_is_mini_mode();
}

inline bool is_mini_cpu_mode()
{
  return ObRunningModeConfig::instance().mini_cpu_mode_;
}

inline double mini_mode_resource_ratio()
{
  int64_t memory_limit = ObRunningModeConfig::instance().memory_limit_;
  int64_t upper = ObRunningModeConfig::instance().MINI_MEM_UPPER;
  double ratio = 1.0;
  if (0 == memory_limit || memory_limit >= upper) {
    ratio = 1.0;
  } else {
    ratio = (double)memory_limit / upper;
  }
  return ratio;
}

inline void update_mini_mode(int64_t memory_limit, int64_t cpu_cnt)
{
  ObRunningModeConfig::instance().memory_limit_ = memory_limit;
  ObRunningModeConfig::instance().mini_mode_ = (memory_limit < lib::ObRunningModeConfig::MINI_MEM_UPPER);
  ObRunningModeConfig::instance().mini_cpu_mode_ = (cpu_cnt <= lib::ObRunningModeConfig::MINI_CPU_UPPER);
}

inline bool use_ipv6()
{
  return ObRunningModeConfig::instance().use_ipv6_;
}

inline void enable_use_ipv6()
{
  ObRunningModeConfig::instance().use_ipv6_ = true;
}

} //lib
} //oceanbase

extern "C" {
  bool use_ipv6_c();
} /* extern "C" */

#endif // OB_RUNNING_MODE_H_
