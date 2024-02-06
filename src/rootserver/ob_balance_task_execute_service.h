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

#ifndef OCEANBASE_ROOTSERVER_OB_BALANCE_TASK_EXECUTE_H
#define OCEANBASE_ROOTSERVER_OB_BALANCE_TASK_EXECUTE_H
#include "lib/thread/ob_reentrant_thread.h"//ObRsReentrantThread
#include "lib/thread/thread_mgr_interface.h"          // TGRunnable
#include "lib/lock/ob_thread_cond.h"//ObThreadCond
#include "rootserver/ob_tenant_thread_helper.h"//ObTenantTheadHelper
#include "share/balance/ob_balance_task_table_operator.h"//ObBalanceTask
#include "share/ob_thread_mgr.h" //OBTGDefIDEnum
#include "share/ob_balance_define.h"  // ObBalanceJobID, ObBalanceTaskID
#include "share/ls/ob_ls_i_life_manager.h"//ObLSStatus

namespace oceanbase
{

namespace common
{
class ObMySQLProxy;
class ObMySQLTransaction;
class ObTabletID;
class ObISQLClient;
}
namespace share
{
class ObBalanceJob;
namespace schema
{
class ObSchemaGetterGuard;
}
}
namespace rootserver
{

/*description:
 * only one thread in threadpool
 * the service process expand, shrink and partition balance
 */
class ObBalanceTaskExecuteService : public ObTenantThreadHelper,
                           public logservice::ObICheckpointSubHandler,
                           public logservice::ObIReplaySubHandler
{
public:
  ObBalanceTaskExecuteService():inited_(false), tenant_id_(OB_INVALID_TENANT_ID),
                        sql_proxy_(NULL), task_array_(), task_comment_(){}
  virtual ~ObBalanceTaskExecuteService() {}
  int init();
  void destroy();
  virtual void do_work() override;
  DEFINE_MTL_FUNC(ObBalanceTaskExecuteService)

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
private:
  int load_all_balance_task_();
  int execute_task_();
  int get_balance_job_task_for_update_(const share::ObBalanceTask &task,
      share::ObBalanceJob &job,  share::ObBalanceTask &task_in_trans,
      ObMySQLTransaction &trans);
  int finish_task_(
      const share::ObBalanceTask &task,
      const share::ObBalanceTaskStatus finish_task_status,
      ObMySQLTransaction &trans);
  int update_task_status_(const share::ObBalanceTask &task,
                   const share::ObBalanceJobStatus &job_status,
                   ObMySQLTransaction &trans);
  int process_current_task_status_(const share::ObBalanceTask &task, ObMySQLTransaction &trans,
                                   bool &skip_next_status);
  int cancel_current_task_status_(const share::ObBalanceTask &task, ObMySQLTransaction &trans, bool &skip_next_status);
  int cancel_other_init_task_(const share::ObBalanceTask &task, ObMySQLTransaction &trans);
  int process_init_task_(const share::ObBalanceTask &task, ObMySQLTransaction &trans);
  int wait_ls_to_target_status_(const share::ObLSID &ls_id, const share::ObLSStatus ls_status, bool &skip_next_status);
  int wait_alter_ls_(const share::ObBalanceTask &task, bool &skip_next_status);
  int set_ls_to_merge_(const share::ObBalanceTask &task, ObMySQLTransaction &trans);
  int set_ls_to_dropping_(const share::ObLSID &ls_id, ObMySQLTransaction &trans);
  int execute_transfer_in_trans_(const share::ObBalanceTask &task,
                                 ObMySQLTransaction &trans,
                                 bool &all_part_transferred);
  int get_and_update_merge_ls_part_list_(
      ObMySQLTransaction &trans,
      const share::ObBalanceTask &task,
      bool &all_part_transferred);
  int wait_tenant_ready_();
  int try_update_task_comment_(const share::ObBalanceTask &task,
  const common::ObSqlString &comment, ObISQLClient &sql_client);
private:
  bool inited_;
  uint64_t tenant_id_;
  common::ObMySQLProxy *sql_proxy_;
  ObArray<share::ObBalanceTask> task_array_;
  ObSqlString task_comment_;
};
}
}


#endif /* !OCEANBASE_ROOTSERVER_OB_BALANCE_TASK_EXECUTE_H */
