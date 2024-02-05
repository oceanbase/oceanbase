/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#pragma once

#include "lib/task/ob_timer.h"

namespace oceanbase
{
namespace rootserver
{
class ObMViewTimerTask : public common::ObTimerTask
{
public:
  ObMViewTimerTask() = default;
  virtual ~ObMViewTimerTask() = default;

  int schedule_task(const int64_t delay, bool repeate = false, bool immediate = false);
  void cancel_task();
  void wait_task();
};

} // namespace rootserver
} // namespace oceanbase
