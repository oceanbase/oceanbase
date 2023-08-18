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

#ifndef OCEANBASE_ROOTSERVER_OB_LS_RECOVERY_SERVICE_H
#define OCEANBASE_ROOTSERVER_OB_LS_RECOVERY_SERVICE_H

#include "common/ob_member_list.h" // common::ObMemberList
#include "lib/thread/ob_reentrant_thread.h"//ObRsReentrantThread
#include "lib/utility/ob_print_utils.h" //TO_STRING_KV
#include "share/ob_tenant_role.h"//ObTenantRole
#include "share/ob_tenant_info_proxy.h"//ObAllTenantInfo
#include "share/ls/ob_ls_recovery_stat_operator.h" // ObLSRecoveryStatOperator
#include "lib/lock/ob_spin_rwlock.h" //lock
#include "rootserver/ob_rs_async_rpc_proxy.h" //ObGetLSReplayedScnProxy

namespace oceanbase {
namespace common
{
class ObMySQLProxy;
class ObMySQLTransaction;
}
namespace share
{
class ObLSID;
class SCN;
struct ObLSRecoveryStat;
}
namespace storage
{
class ObLS;
}
namespace rootserver
{

/*description:
 * Collect the information of each log stream under the user tenant: the minimum
 * standby machine-readable timestamp of the majority, the minimum standby
 * machine-readable timestamp of all replicas, the synchronization point, etc.
 * Statistics for the syslog stream are not in this thread.*/
class ObLSRecoveryReportor : public share::ObReentrantThread
{
public:
 ObLSRecoveryReportor()
     : is_inited_(false),
       tenant_id_(common::OB_INVALID_TENANT_ID),
       sql_proxy_(nullptr) {}
 ~ObLSRecoveryReportor() {}
 static int mtl_init(ObLSRecoveryReportor *&ka);
 int init();
 void destroy();
 int start();
 void stop();
 void wait();
 void wakeup();
 virtual int blocking_run() {
   BLOCKING_RUN_IMPLEMENT();
 }
 virtual void run2() override;
 //description: update ls recovery
 int update_ls_recovery(storage::ObLS *ls, common::ObMySQLProxy *sql_proxy);

 static int get_readable_scn(const share::ObLSID &id, share::SCN &read_scn);
 static int update_sys_ls_recovery_stat_and_tenant_info(share::ObLSRecoveryStat &ls_recovery_stat,
      const share::ObTenantRole &tenant_role,
      const bool update_readable_scn,
      common::ObMySQLTransaction &trans);
 static int update_tenant_info_in_trans(const share::ObAllTenantInfo &old_tenant_info, common::ObMySQLTransaction &trans);
private:
  static int get_sync_point_(const share::ObLSID &id, share::SCN &scn, share::SCN &read_scn);
  int update_ls_recovery_stat_();
  void idle_some_time_();

public:
 TO_STRING_KV(K_(is_inited), K_(tenant_id));
private:
  bool is_inited_;
  uint64_t tenant_id_;
  common::ObMySQLProxy *sql_proxy_;
private:
  //更新受控回放到replayservice
  int update_replayable_point_();
  int update_replayable_point_from_tenant_info_();
  int update_replayable_point_from_meta_();
  int submit_tenant_refresh_schema_task_();
  DISALLOW_COPY_AND_ASSIGN(ObLSRecoveryReportor);
};

} // namespace rootserver
} // namespace oceanbase


#endif /* !OCEANBASE_ROOTSERVER_OB_LS_RECOVERY_SERVICE_H */
