/**
 * Copyright (c) 2023 OceanBase
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

#include "lib/container/ob_array.h"
#include "rootserver/mview/ob_mview_timer_task.h"

namespace oceanbase
{
namespace rootserver
{
class ObMViewMaintenanceTask : public ObMViewTimerTask
{
public:
  ObMViewMaintenanceTask();
  virtual ~ObMViewMaintenanceTask();
  DISABLE_COPY_ASSIGN(ObMViewMaintenanceTask);

  // for Service
  int init();
  int start();
  void stop();
  void wait();
  void destroy();

  // for TimerTask
  void runTimerTask() override;

private:
  static const int64_t MVIEW_MAINTENANCE_INTERVAL = 24LL * 3600 * 1000 * 1000; // 1day
  static const int64_t MVIEW_MAINTENANCE_SCHED_INTERVAL = 10LL * 1000 * 1000; // 10s
  static const int64_t MVIEW_NUM_FETCH_PER_SCHED = 1000;
  static const int64_t MVREF_STATS_NUM_PURGE_PER_SCHED = 1000;

  enum class StatusType
  {
    PREPARE = 0,
    GC_MVIEW = 1,
    SUCCESS = 100,
    FAIL = 101,
  };

private:
  static bool is_retry_ret_code(int ret_code);
  void switch_status(StatusType new_status, int ret_code);

  int prepare();
  int gc_mview();
  int finish();

  void cleanup();
  int drop_mview(uint64_t mview_id);

private:
  uint64_t tenant_id_;
  int64_t round_;
  StatusType status_;
  int error_code_;
  uint64_t last_fetch_mview_id_;
  ObArray<uint64_t> mview_ids_;
  int64_t mview_idx_;
  uint64_t gc_mview_id_;
  int64_t fetch_mview_num_;
  int64_t gc_mview_num_;
  int64_t gc_stats_num_;
  int64_t start_time_;
  int64_t start_gc_mview_time_;
  int64_t cost_us_;
  int64_t prepare_cost_us_;
  int64_t gc_mview_cost_us_;
  bool fetch_finish_;
  bool in_sched_;
  bool is_stop_;
  bool is_inited_;
};

} // namespace rootserver
} // namespace oceanbase
