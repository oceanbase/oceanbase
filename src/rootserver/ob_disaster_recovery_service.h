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

#ifndef OCEANBASE_ROOTSERVER_OB_DISASTER_RECOVERY_SERVICE_H_
#define OCEANBASE_ROOTSERVER_OB_DISASTER_RECOVERY_SERVICE_H_

#include "common/ob_role.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "logservice/ob_log_base_type.h"
#include "ob_tenant_thread_helper.h"
#include "share/scn.h"

namespace oceanbase
{

namespace rootserver
{

class ObDRService : public ObTenantThreadHelper,
                    public logservice::ObICheckpointSubHandler,
                    public logservice::ObIReplaySubHandler
{
public:
  const static int64_t CHECK_AND_CLEAN_TASK_INTERVAL = 10L * 1000000L; // 10s

public:
  ObDRService() : inited_(false), tenant_id_(OB_INVALID_TENANT_ID) {}
  virtual ~ObDRService() {}
public:
  int init();
  void destroy();
  virtual void do_work() override;
  DEFINE_MTL_FUNC(ObDRService)

public:
  virtual share::SCN get_rec_scn() override { return share::SCN::max_scn();}
  virtual int flush(share::SCN &scn) override { return OB_SUCCESS; }
  int replay(const void *buffer, const int64_t nbytes, const palf::LSN &lsn, const share::SCN &scn) override
  {
    UNUSED(buffer);
    UNUSED(nbytes);
    UNUSED(lsn);
    UNUSED(scn);
    return OB_SUCCESS;
  }
private:
  int check_inner_stat_() const;

  int do_dr_service_work_();

  bool is_dr_worker_thread_(const uint64_t thread_idx) { return 0 == thread_idx; }

  int adjust_idle_time_(
      int64_t &idle_time_us);

  // use dr_worker_ check dr task
  // @params[in] tenant_id, target tenant_id
  int try_tenant_disaster_recovery_(
      const uint64_t tenant_id,
      const int64_t service_epoch_to_check);

  // use dr_mgr_ manage dr task
  // @params[in] tenant_id,       target tenant_id
  // @params[in] need_clean_task, whether need to clean task
  int manage_dr_tasks_(
      const uint64_t tenant_id,
      const bool need_clean_task,
      const int64_t service_epoch_to_check);

  // check whether the current thread can provide services
  int check_and_update_service_epoch_(
      int64_t &service_epoch);

  // update tenant service_epoch value for proposal_id
  int update_tenant_service_epoch_(
      ObMySQLTransaction &trans,
      const int64_t proposal_id);

  // get the current tenant's 1 LS role and proposal_id
  int get_role_and_proposal_id_(
      common::ObRole &role,
      int64_t &proposal_id);

  // set service_epoch value to check of worker and mgr
  int get_service_epoch_to_check_(
      const uint64_t execute_task_tenant,
      const int64_t epoch_of_service_thread,
      int64_t &service_epoch_to_check);

  // get tenant_id array to execute task
  int get_tenant_ids_(
      ObIArray<uint64_t> &tenant_ids);

  // according to last_check_ts, check whether need to clean task
  int check_need_clean_task_(
      int64_t &last_check_ts,
      bool &need_clean_task);

  int ensure_service_epoch_exist_();

private:
  bool inited_;
  uint64_t tenant_id_;
};

} // end namespace rootserver
} // end namespace oceanbase
#endif // OCEANBASE_ROOTSERVER_OB_DISASTER_RECOVERY_SERVICE_H_
