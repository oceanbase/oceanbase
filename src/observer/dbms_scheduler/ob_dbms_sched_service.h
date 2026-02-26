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

#ifndef OCEANBASE_ROOTSERVER_OB_DBMS_SCHEDULER_SERVICE_H
#define OCEANBASE_ROOTSERVER_OB_DBMS_SCHEDULER_SERVICE_H

#include "share/ob_define.h"
#include "logservice/ob_log_base_type.h"                        //ObIRoleChangeSubHandler ObICheckpointSubHandler ObIReplaySubHandler
#include "observer/dbms_scheduler/ob_dbms_sched_job_master.h"
#include "rootserver/ob_primary_ls_service.h" // ObTenantThreadHelper

namespace oceanbase
{
namespace rootserver
{
class ObDBMSSchedService : public ObTenantThreadHelper,
                           public logservice::ObICheckpointSubHandler,
                           public logservice::ObIReplaySubHandler
{
public:
  ObDBMSSchedService()
      : tenant_id_(OB_INVALID_TENANT_ID),
        job_master_()
  {}
  virtual ~ObDBMSSchedService()
  {
    destroy();
  }

  static int mtl_init(ObDBMSSchedService *&dbms_sched_service);
  int init();
  int start();
  virtual void do_work() override;
  void stop();
  void wait();
  void destroy();
  bool is_leader() { return job_master_.is_leader(); }
  bool is_stop() { return job_master_.is_stop(); }

public:
  // for replay, do nothing
  int replay(const void *buffer, const int64_t nbytes, const palf::LSN &lsn, const share::SCN &scn) override
  {
    UNUSED(buffer);
    UNUSED(nbytes);
    UNUSED(lsn);
    UNUSED(scn);
    return OB_SUCCESS;
  }
  // for checkpoint, do nothing
  virtual share::SCN get_rec_scn() override
  {
    return share::SCN::max_scn();
  }
  virtual int flush(share::SCN &scn) override
  {
    return OB_SUCCESS;
  }

  // for role change
  virtual void switch_to_follower_forcedly() override;
  virtual int switch_to_leader() override;
  virtual int switch_to_follower_gracefully() override;
  virtual int resume_leader() override;

private:
  uint64_t tenant_id_;
  dbms_scheduler::ObDBMSSchedJobMaster job_master_;
};
}  // namespace rootserver
}  // namespace oceanbase

#endif /* !OCEANBASE_ROOTSERVER_OB_DBMS_SCHEDULER_SERVICE_H */
