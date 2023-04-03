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
struct ObRunningModeConfig
{
  static const int64_t MINI_MEM_LOWER;
  static const int64_t MINI_MEM_UPPER;
  bool mini_mode_ = false;
  int64_t memory_limit_ = 0;
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
  return ObRunningModeConfig::instance().mini_mode_;
}

inline void update_mini_mode(int64_t memory_limit)
{
  ObRunningModeConfig::instance().memory_limit_ = memory_limit;
  ObRunningModeConfig::instance().mini_mode_ = (memory_limit < lib::ObRunningModeConfig::MINI_MEM_UPPER);
}

} //lib
} //oceanbase
#endif // OB_RUNNING_MODE_H_
