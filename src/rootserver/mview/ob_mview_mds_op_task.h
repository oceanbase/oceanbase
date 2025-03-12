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
#include "rootserver/mview/ob_mview_timer_task.h"

namespace oceanbase
{
namespace rootserver
{
class ObMViewMdsOpTask : public ObMViewTimerTask
{
public:
  ObMViewMdsOpTask();
  virtual ~ObMViewMdsOpTask();
  DISABLE_COPY_ASSIGN(ObMViewMdsOpTask);
  // for Service
  int init();
  int start();
  void stop();
  void wait();
  void destroy();
  // for TimerTask
  void runTimerTask() override;
  int update_mview_mds_op();
  static const int64_t MVIEW_MDS_OP_INTERVAL = 5 * 1000 * 1000; // 5s
private:
  bool is_inited_;
  bool in_sched_;
  bool is_stop_;
  uint64_t tenant_id_;
  int64_t last_sched_ts_;
};

} // namespace rootserver
} // namespace oceanbase
