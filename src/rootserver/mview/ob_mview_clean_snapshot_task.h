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
#include "lib/mysqlclient/ob_isql_client.h"
#include "rootserver/mview/ob_mview_timer_task.h"

namespace oceanbase
{
namespace rootserver
{
class ObMViewCleanSnapshotTask : public ObMViewTimerTask
{
public:
  ObMViewCleanSnapshotTask();
  virtual ~ObMViewCleanSnapshotTask();
  DISABLE_COPY_ASSIGN(ObMViewCleanSnapshotTask);
  // for Service
  int init();
  int start();
  void stop();
  void wait();
  void destroy();
  // for TimerTask
  void runTimerTask() override;
  // TODO: increase the scheduling interval
  static const int64_t MVIEW_CLEAN_SNAPSHOT_INTERVAL = 60 * 1000 * 1000; // 1min
private:
  int get_table_id_(ObISQLClient &sql_client, const uint64_t tablet_id, uint64_t &table_id);
  int is_mv_container_table_(const uint64_t table_id, bool &is_container);
private:
  bool is_inited_;
  bool in_sched_;
  bool is_stop_;
  uint64_t tenant_id_;
};

} // namespace rootserver
} // namespace oceanbase
