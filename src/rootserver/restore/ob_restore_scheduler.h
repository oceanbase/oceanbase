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

#ifndef OCEANBASE_ROOTSERVER_OB_RESTORE_SCHEDULER_H_
#define OCEANBASE_ROOTSERVER_OB_RESTORE_SCHEDULER_H_

#include "rootserver/restore/ob_restore_util.h"
#include "share/backup/ob_backup_struct.h"
#include "share/ob_rpc_struct.h"
#include "share/ob_upgrade_utils.h"
#include "ob_restore_common_util.h"

namespace oceanbase
{
namespace share
{
class SCN;
class ObLSTableOperator;
struct ObLSAttr;
struct ObHisRestoreJobPersistInfo;
}
namespace rootserver
{
class ObRestoreService;
// Running in a single thread.
// schedule restore job, register to sys ls of meta tenant
class ObRestoreScheduler
{
public:
  static const int64_t MAX_RESTORE_TASK_CNT = 10000;
public:
  ObRestoreScheduler();
  virtual ~ObRestoreScheduler();
  int init(ObRestoreService &restore_service);
  void do_work();
  void destroy();
public:
  static int reset_schema_status(const uint64_t tenant_id, common::ObMySQLProxy *sql_proxy);

public:
  static int assign_pool_list(const char *str,
                       common::ObIArray<common::ObString> &pool_list);

private:
  int process_restore_job(const share::ObPhysicalRestoreJob &job);
  int process_sys_restore_job(const share::ObPhysicalRestoreJob &job);
  int try_recycle_job(const share::ObPhysicalRestoreJob &job);

  int restore_tenant(const share::ObPhysicalRestoreJob &job_info);
  int restore_upgrade(const share::ObPhysicalRestoreJob &job_info);
  int restore_pre(const share::ObPhysicalRestoreJob &job_info);

  int post_check(const share::ObPhysicalRestoreJob &job_info);
  int restore_finish(const share::ObPhysicalRestoreJob &job_info);
  int tenant_restore_finish(const share::ObPhysicalRestoreJob &job_info);
  int restore_init_ls(const share::ObPhysicalRestoreJob &job_info);
  int restore_wait_to_consistent_scn(const share::ObPhysicalRestoreJob &job_info);
  int check_tenant_replay_to_consistent_scn(const uint64_t tenant_id, const share::SCN &scn, bool &is_replay_finish);
  int set_restore_to_target_scn_(common::ObMySQLTransaction &sql_client, const share::ObPhysicalRestoreJob &job_info, const share::SCN &scn);
  int restore_wait_ls_finish(const share::ObPhysicalRestoreJob &job_info);
  int restore_wait_tenant_finish(const share::ObPhysicalRestoreJob &job_info);

  int fill_create_tenant_arg(const share::ObPhysicalRestoreJob &job_info,
                             const ObSqlString &pool_list,
                             obrpc::ObCreateTenantArg &arg);
  int convert_tde_parameters(const share::ObPhysicalRestoreJob &job_info);
  int restore_root_key(const share::ObPhysicalRestoreJob &job_info);
  int restore_keystore(const share::ObPhysicalRestoreJob &job_info);

  int check_locality_valid(const share::schema::ZoneLocalityIArray &locality);
  int try_update_job_status(
      common::ObISQLClient &sql_client,
      int return_ret,
      const share::ObPhysicalRestoreJob &job,
      share::PhysicalRestoreMod mod = share::PHYSICAL_RESTORE_MOD_RS);
  void record_rs_event(const share::ObPhysicalRestoreJob &job,
                       const share::PhysicalRestoreStatus next_status);
  share::PhysicalRestoreStatus get_next_status(int return_ret, share::PhysicalRestoreStatus current_status);
  share::PhysicalRestoreStatus get_sys_next_status(share::PhysicalRestoreStatus current_status);
  
  int fill_restore_statistics(const share::ObPhysicalRestoreJob &job_info);
private:
  int create_all_ls_(const share::schema::ObTenantSchema &tenant_schema,
      const common::ObIArray<share::ObLSAttr> &ls_attr_array);
  int wait_all_ls_created_(const share::schema::ObTenantSchema &tenant_schema,
      const share::ObPhysicalRestoreJob &job);
  int finish_create_ls_(const share::schema::ObTenantSchema &tenant_schema,
      const common::ObIArray<share::ObLSAttr> &ls_attr_array);
  int check_all_ls_restore_finish_(const uint64_t tenant_id, TenantRestoreStatus &tenant_restore_status);
  int check_all_ls_restore_to_consistent_scn_finish_(const uint64_t tenant_id, TenantRestoreStatus &tenant_restore_status);
  int try_get_tenant_restore_history_(const share::ObPhysicalRestoreJob &job_info,
                                      share::ObHisRestoreJobPersistInfo &history_info,
                                      bool &restore_tenant_exist);
  int check_tenant_can_restore_(const uint64_t tenant_id);
  int may_update_restore_concurrency_(const uint64_t new_tenant_id,
      const share::ObPhysicalRestoreJob &job_info);
  int reset_restore_concurrency_(const uint64_t new_tenant_id, const share::ObPhysicalRestoreJob &job_info);
  int update_restore_concurrency_(const common::ObString &tenant_name, const uint64_t tenant_id,
      const int64_t restore_concurrency);
private:
  bool inited_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
  common::ObMySQLProxy *sql_proxy_;
  obrpc::ObCommonRpcProxy *rpc_proxy_;
  obrpc::ObSrvRpcProxy *srv_rpc_proxy_;
  share::ObLSTableOperator *lst_operator_;
  ObRestoreService *restore_service_;
  common::ObAddr self_addr_;
  uint64_t tenant_id_;
  DISALLOW_COPY_AND_ASSIGN(ObRestoreScheduler);
};




} // end namespace rootserver
} // end namespace oceanbase

#endif // OCEANBASE_ROOTSERVER_OB_RESTORE_SCHEDULER_H_
