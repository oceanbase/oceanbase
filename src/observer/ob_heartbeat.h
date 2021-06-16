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

#ifndef OCEANBASE_OBSERVER_OB_HEARTBEAT_H_
#define OCEANBASE_OBSERVER_OB_HEARTBEAT_H_

#include "observer/ob_lease_state_mgr.h"

#include "lib/task/ob_timer.h"
#include "share/ob_lease_struct.h"
#include "observer/ob_server_struct.h"

namespace oceanbase {
namespace observer {

class ObServerSchemaUpdater;

class ObHeartBeatProcess : public observer::IHeartBeatProcess {
public:
  ObHeartBeatProcess(
      const ObGlobalContext& gctx, ObServerSchemaUpdater& schema_updater, ObLeaseStateMgr& lease_state_mgr);
  virtual ~ObHeartBeatProcess();

  int init();
  virtual int init_lease_request(share::ObLeaseRequest& lease_request);
  virtual int do_heartbeat_event(const share::ObLeaseResponse& lease_response);

  int update_lease_info();
  int try_update_infos();

private:
  class ObZoneLeaseInfoUpdateTask : public common::ObTimerTask {
  public:
    explicit ObZoneLeaseInfoUpdateTask(ObHeartBeatProcess& hb_process);
    virtual ~ObZoneLeaseInfoUpdateTask();

    virtual void runTimerTask();

  private:
    ObHeartBeatProcess& hb_process_;
  };

  int try_start_merge(const int64_t broadcast_version);
  int try_reload_config(const int64_t config_version);
  int try_reload_time_zone_info(const int64_t time_zone_info_version);

private:
  bool inited_;
  ObZoneLeaseInfoUpdateTask update_task_;
  share::ObZoneLeaseInfo zone_lease_info_;
  int64_t newest_lease_info_version_;

  const ObGlobalContext& gctx_;
  ObServerSchemaUpdater& schema_updater_;
  ObLeaseStateMgr& lease_state_mgr_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObHeartBeatProcess);
};

}  // end of namespace observer
}  // end of namespace oceanbase

#endif
