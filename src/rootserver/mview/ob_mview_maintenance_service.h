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

#include "logservice/ob_log_base_type.h"
#include "rootserver/mview/ob_mlog_maintenance_task.h"
#include "rootserver/mview/ob_mview_maintenance_task.h"
#include "rootserver/mview/ob_mview_refresh_stats_maintenance_task.h"
#include "share/scn.h"

namespace oceanbase
{
namespace rootserver
{
class ObMViewMaintenanceService : public logservice::ObIReplaySubHandler,
                                  public logservice::ObICheckpointSubHandler,
                                  public logservice::ObIRoleChangeSubHandler
{
public:
  ObMViewMaintenanceService();
  virtual ~ObMViewMaintenanceService();
  DISABLE_COPY_ASSIGN(ObMViewMaintenanceService);

  // for MTL
  static int mtl_init(ObMViewMaintenanceService *&service);
  int init();
  int start();
  void stop();
  void wait();
  void destroy();

  // for replay, do nothing
  int replay(const void *buffer, const int64_t nbytes, const palf::LSN &lsn,
             const share::SCN &scn) override final
  {
    UNUSED(buffer);
    UNUSED(nbytes);
    UNUSED(lsn);
    UNUSED(scn);
    return OB_SUCCESS;
  }

  // for checkpoint, do nothing
  share::SCN get_rec_scn() override final { return share::SCN::max_scn(); }
  int flush(share::SCN &rec_scn) override final
  {
    UNUSED(rec_scn);
    return OB_SUCCESS;
  }

  // for role change
  void switch_to_follower_forcedly() override final;
  int switch_to_leader() override final;
  int switch_to_follower_gracefully() override final;
  int resume_leader() override final;

private:
  int inner_switch_to_leader();
  int inner_switch_to_follower();

private:
  ObMLogMaintenanceTask mlog_maintenance_task_;
  ObMViewMaintenanceTask mview_maintenance_task_;
  ObMViewRefreshStatsMaintenanceTask mvref_stats_maintenance_task_;
  bool is_inited_;
};

} // namespace rootserver
} // namespace oceanbase
