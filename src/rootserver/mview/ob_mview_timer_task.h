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
#include "share/scn.h"

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

  static int need_schedule_major_refresh_mv_task(const uint64_t tenant_id,
                                                 bool &need_schedule);
  static int need_push_major_mv_merge_scn(const uint64_t tenant_id,
                                          bool &need_push,
                                          share::SCN &latest_merge_scn,
                                          share::SCN &major_mv_merge_scn);
  static int check_mview_last_refresh_scn(const uint64_t tenant_id,
                                          const share::SCN &tenant_mv_merge_scn);
};

} // namespace rootserver
} // namespace oceanbase
