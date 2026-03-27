/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_SHARE_ASH_REFRESH_TASK_H_
#define _OB_SHARE_ASH_REFRESH_TASK_H_

#include "lib/task/ob_timer.h"
#include "share/wr/ob_wr_snapshot_rpc_processor.h"
// refersh task update its state and do decision making
// every ASH_REFRESH_INTERVAL
constexpr int64_t ASH_REFRESH_INTERVAL = 120 * 1000L * 1000L;  // 120s

namespace oceanbase
{
namespace share
{

class ObAshRefreshTask : public common::ObTimerTask
{
public:
  ObAshRefreshTask(): is_inited_(false), last_scheduled_snapshot_time_(OB_INVALID_TIMESTAMP), prev_write_pos_(0), prev_sched_time_(0) {}
  virtual ~ObAshRefreshTask() = default;
  static ObAshRefreshTask &get_instance();
  int start();
  virtual void runTimerTask() override;
private:
  bool require_snapshot_ahead();
  bool check_tenant_can_do_wr_task(uint64_t tenant_id);
  obrpc::ObWrRpcProxy wr_proxy_;
  bool is_inited_;
  int64_t last_scheduled_snapshot_time_;
  int64_t prev_write_pos_;
  int64_t prev_sched_time_;

};
}
}
#endif /* _OB_SHARE_ASH_REFRESH_TASK_H_ */
//// end of header file