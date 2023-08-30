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

#ifndef OCEANBASE_ROOTSERVER_OB_TENANT_BALANCE_SERVICE_H
#define OCEANBASE_ROOTSERVER_OB_TENANT_BALANCE_SERVICE_H
#include "lib/thread/ob_reentrant_thread.h"//ObRsReentrantThread
#include "share/ob_thread_mgr.h" //OBTGDefIDEnum
#include "share/unit/ob_unit_info.h"//ObUnit::Status && SimpleUnitGroup
#include "lib/thread/thread_mgr_interface.h"          // TGRunnable
#include "lib/lock/ob_thread_cond.h"//ObThreadCond
#include "rootserver/ob_tenant_thread_helper.h"//ObTenantTheadHelper
#include "share/ls/ob_ls_status_operator.h"//ObLSStatusInfoArray
#include "share/ob_balance_define.h"

namespace oceanbase
{
namespace obrpc
{
class  ObSrvRpcProxy;
}
namespace common
{
class ObMySQLProxy;
class ObMySQLTransaction;
}
namespace share
{
class ObBalanceJob;
class ObBalanceTask;
class ObLSTableOperator;
namespace schema
{
class ObMultiVersionSchemaService;
class ObTenantSchema;
}
}
namespace rootserver
{

/*description:
 * only one thread in threadpool
 * the service process expand, shrink and partition balance
 */
class ObTenantBalanceService : public ObTenantThreadHelper,
                           public logservice::ObICheckpointSubHandler,
                           public logservice::ObIReplaySubHandler
{
public:
  ObTenantBalanceService():inited_(false), loaded_(false), tenant_id_(OB_INVALID_TENANT_ID),
                           primary_zone_num_(OB_INVALID_COUNT), ls_array_(),
                           unit_group_array_() {}
  virtual ~ObTenantBalanceService() {}
  int init();
  void destroy();
  virtual void do_work() override;
  DEFINE_MTL_FUNC(ObTenantBalanceService)

public:
  virtual share::SCN get_rec_scn() override { return share::SCN::max_scn();}
  virtual int flush(share::SCN &) override { return OB_SUCCESS; }
  int replay(const void *buffer, const int64_t nbytes, const palf::LSN &lsn, const share::SCN &) override
  {
    UNUSED(buffer);
    UNUSED(nbytes);
    UNUSED(lsn);
    return OB_SUCCESS;
  }
  static int gather_stat_primary_zone_num_and_units(
      const uint64_t &tenant_id,
      int64_t &primary_zone_num,
      ObIArray<share::ObSimpleUnitGroup> &unit_group_array);
  static int gather_ls_status_stat(const uint64_t &tenant_id, share::ObLSStatusInfoArray &ls_array);
  static int is_ls_balance_finished(const uint64_t &tenant_id, bool &is_finished);

private:
  static int is_primary_tenant_ls_balance_finished_(const uint64_t &tenant_id, bool &is_finished);
  static int is_standby_tenant_ls_balance_finished_(const uint64_t &tenant_id, bool &is_finished);
  //load current unit group and primary zone
  int gather_stat_();
  //process current job
  int try_process_current_job(int64_t &job_cnt);
  //accordint to primary_zone and unit group
  int ls_balance_(int64_t &job_cnt);
  // according balance group strategy
  int partition_balance_(bool enable_transfer = false);
  //if job finish success, job cnt is zero or one
  int try_finish_current_job_(const share::ObBalanceJob &job, int64_t &job_cnt);
  /* description: check current job need cancel
  current ls group and primary zone not match with job
  */
  int check_ls_job_need_cancel_(const share::ObBalanceJob &job,
                                bool &need_cancel,
                                common::ObSqlString &abort_comment);
  void reset();
  int persist_job_and_task_(const share::ObBalanceJob &job,
                            ObArray<share::ObBalanceTask> &tasks);
  int construct_dependency_of_each_task_(ObArray<share::ObBalanceTask> &tasks);
  int lock_and_check_balance_job_(common::ObMySQLTransaction &trans, const uint64_t tenant_id);
  int try_update_job_comment_(const share::ObBalanceJob &job, const common::ObSqlString &comment);
  int try_do_partition_balance_(int64_t &last_partition_balance_time);
  int try_statistic_balance_group_status_(
      int64_t &last_statistic_bg_stat_time,
      int64_t &last_statistic_schema_version,
      share::ObTransferTaskID &last_statistic_max_transfer_task_id);
  int get_active_unit_num_(int64_t &active_unit_num) const;
private:
  bool inited_;
  bool loaded_;
  uint64_t tenant_id_;
  int64_t primary_zone_num_;
  share::ObLSStatusInfoArray ls_array_;
  ObArray<share::ObSimpleUnitGroup> unit_group_array_;
};
}
}


#endif /* !OCEANBASE_ROOTSERVER_OB_TENANT_BALANCE_SERVICE_H */
