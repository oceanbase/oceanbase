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

#include "lib/task/ob_timer.h"
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
  class ObOrphanJobCleanTask : public common::ObTimerTask
  {
  public:
    ObOrphanJobCleanTask() : service_(nullptr) {}
    void set_service(ObDBMSSchedService *service) { service_ = service; }
    virtual void runTimerTask() override;
  private:
    ObDBMSSchedService *service_;
  };

  ObDBMSSchedService()
      : tenant_id_(OB_INVALID_TENANT_ID),
        in_orphan_job_clean_(false),
        orphan_job_clean_scheduled_(false),
        job_master_(),
        orphan_job_clean_task_()
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
  void trigger_orphan_job_clean();

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
  int schedule_orphan_job_clean_task_();
  void cancel_orphan_job_clean_task_();
  void wait_orphan_job_clean_task_();
  int run_orphan_job_clean_task_();
  uint64_t tenant_id_;
  bool in_orphan_job_clean_;
  bool orphan_job_clean_scheduled_;
  dbms_scheduler::ObDBMSSchedJobMaster job_master_;
  ObOrphanJobCleanTask orphan_job_clean_task_;
};
}  // namespace rootserver
}  // namespace oceanbase

#endif /* !OCEANBASE_ROOTSERVER_OB_DBMS_SCHEDULER_SERVICE_H */
