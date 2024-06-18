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

#ifndef __OB_RS_TENANT_CLONE_UTIL_H__
#define __OB_RS_TENANT_CLONE_UTIL_H__

#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "share/restore/ob_tenant_clone_table_operator.h" //ObCancelCloneJobReason

namespace oceanbase
{
namespace share
{
class ObTenantSnapshotID;
class ObTenantSnapItem;
class ObCloneJob;
class SCN;
}
namespace obrpc
{
class ObCloneTenantArg;
}
namespace rootserver
{
class ObTenantCloneUtil
{
public:
  static int check_source_tenant_has_clone_job(common::ObISQLClient &sql_client,
                                               const uint64_t source_tenant_id,
                                               bool &has_job);
  static int check_clone_tenant_exist(common::ObISQLClient &sql_client,
                                      const ObString &clone_tenant_name,
                                      bool &is_exist);
  static int fill_clone_job(const int64_t job_id,
                            const obrpc::ObCloneTenantArg &arg,
                            const uint64_t source_tenant_id,
                            const ObString &source_tenant_name,
                            const share::ObTenantSnapItem &snapshot_item,
                            share::ObCloneJob &clone_job);
  static int record_clone_job(common::ObISQLClient &sql_client,
                              const share::ObCloneJob &clone_job);
  static int update_resource_pool_id_of_clone_job(common::ObISQLClient &sql_client,
                                                  const int64_t job_id,
                                                  const uint64_t resource_pool_id);
  static int update_snapshot_info_for_fork_job(common::ObISQLClient &sql_client,
                                               const int64_t job_id,
                                               const share::ObTenantSnapshotID tenant_snapshot_id,
                                               const ObString &tenant_snapshot_name);
  static int update_restore_scn_for_fork_job(common::ObISQLClient &sql_client,
                                             const int64_t job_id,
                                             const share::SCN &restore_scn);
  static int insert_user_tenant_clone_job(common::ObISQLClient &sql_client,
                                          const ObString &clone_tenant_name,
                                          const uint64_t user_tenant_id);
  static int recycle_clone_job(common::ObISQLClient &sql_client,
                               const share::ObCloneJob &job);
  static int notify_clone_scheduler(const uint64_t tenant_id);
  static int release_clone_tenant_resource_of_clone_job(const share::ObCloneJob &clone_job);
  static int release_source_tenant_resource_of_clone_job(common::ObISQLClient &sql_client,
                                                         const share::ObCloneJob &clone_job);
  static int get_clone_job_failed_message(common::ObISQLClient &sql_client,
                                          const int64_t job_id,
                                          const uint64_t tenant_id,
                                          ObIAllocator &allocator,
                                          ObString &err_msg);
  //attention: This function is called by the user executing "cancel clone" sql.
  static int cancel_clone_job_by_name(
         common::ObISQLClient &sql_client,
         const ObString &clone_tenant_name,
         bool &clone_already_finish,
         const ObCancelCloneJobReason &reason);
  // cancel clone job by source tenant id, this will be called by
  // standby tenant iterating multi-source log(upgrade, transfer, alter_ls)
  // @params[in]  sql_client, the client to use
  // @params[in]  source tenant id, to identify clone job's source tenant id
  // @params[in]  reason, reason to cancel
  // @params[out] clone_already_finish, whether job already finished
  static int cancel_clone_job_by_source_tenant_id(
         common::ObISQLClient &sql_client,
         const uint64_t source_tenant_id,
         const ObCancelCloneJobReason &reason,
         bool &clone_already_finish);
  static void try_to_record_clone_status_change_rs_event(
         const ObCloneJob &clone_job,
         const share::ObTenantCloneStatus &prev_clone_status,
         const share::ObTenantCloneStatus &cur_clone_status,
         const int ret_code,
         const ObCancelCloneJobReason &reason);
private:
  // inner cancel clone job
  // @params[in]  clone_op, operator to use
  // @params[in]  clone_job, which job to cancel
  // @params[in]  reason, the reason to cancel clone job
  // @params[out] clone_already_finish, whether clone job already finished
  static int inner_cancel_clone_job_(
         ObTenantCloneTableOperator &clone_op,
         const ObCloneJob &clone_job,
         const ObCancelCloneJobReason &reason,
         bool &clone_already_finish);

  // construct data version to record
  // @params[in]  tenant_id, which tenant clone job
  // @params[out] data_version, tenant data version
  // @params[out] min_cluster_version, min_cluster_version
  static int construct_data_version_to_record_(
         const uint64_t tenant_id,
         uint64_t &data_version,
         uint64_t &min_cluster_version);
};


}
}


#endif /* __OB_RS_TENANT_CLONE_UTIL_H__ */
