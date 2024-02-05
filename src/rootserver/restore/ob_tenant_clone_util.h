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
  static int cancel_clone_job(common::ObISQLClient &sql_client,
                              const ObString &clone_tenant_name,
                              bool &clone_already_finish);
};


}
}


#endif /* __OB_RS_TENANT_CLONE_UTIL_H__ */
