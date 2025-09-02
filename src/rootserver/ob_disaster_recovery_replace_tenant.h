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

#ifndef OCEANBASE_ROOTSERVER_OB_DISASTER_RECOVERY_REPLACE_TENANT_H_
#define OCEANBASE_ROOTSERVER_OB_DISASTER_RECOVERY_REPLACE_TENANT_H_
#ifdef OB_BUILD_SHARED_LOG_SERVICE
#ifdef OB_BUILD_SHARED_STORAGE
#include "sql/resolver/cmd/ob_alter_system_stmt.h"

namespace oceanbase
{

namespace rootserver
{

class ObDRReplaceTenant
{
public:
  const static int64_t REPLACE_TENANT_TIMEOUT = 10L * 60L * 1000L * 1000L; // 10 minutes
  const static int64_t RETRY_INTERVAL_US = 100L * 1000L; // 100ms

  ObDRReplaceTenant() : inited_(false),
                        ctx_(),
                        stmt_(),
                        new_server_id_(OB_INVALID_ID),
                        new_unit_id_(OB_INVALID_ID),
                        new_unit_config_id_(OB_INVALID_ID),
                        new_resource_pool_id_(OB_INVALID_ID),
                        shared_storage_info_(),
                        storage_dest_(),
                        unit_config_() {}
  virtual ~ObDRReplaceTenant() {}

public:
  int init(const sql::ObReplaceTenantStmt &stmt);
  int do_replace_tenant();

private:
  int check_inner_stat_() const;
  int check_and_init_for_replace_tenant_();
  int basic_check_for_replace_tenant_();
  int init_and_check_logservice_access_point_();
  int check_new_zone_();
  int set_in_replace_sys_();
  int set_finish_replace_sys_();
  int check_cluster_id_();
  int check_and_init_ss_info_();
  int create_unit_();
  int replace_sys_tenant_();
  int replace_sys_tenant_ls_(const share::ObLSID &ls_id);
  int wait_ls_has_leader_(const share::ObLSID &ls_id);
  int wait_sslog_ls_ready_();
  int wait_sys_tenant_ready_();
  int check_member_list_can_replace_(const common::ObMemberList &member_list,
                                     const share::ObLSID &ls_id);
  int check_can_replace_sys_ls_(const share::ObLSID &ls_id,
                                palf::LogConfigVersion &config_version,
                                common::ObMemberList &member_list);
  int load_and_check_data_version_();
  int get_and_init_server_id_();
  int check_server_alive_(const common::ObAddr &server, bool &alive);
  int correct_inner_table_();
  int correct_all_zone_(common::ObMySQLTransaction &trans);
  int correct_zone_info_in_ss_(common::ObMySQLTransaction &trans);
  int correct_all_server_(common::ObMySQLTransaction &trans);
  int create_new_unit_(common::ObMySQLTransaction &trans, share::ObUnitTableOperator &unit_operator);
  int create_new_unit_config_(common::ObMySQLTransaction &trans, share::ObUnitTableOperator &unit_operator);
  int create_new_resource_pool_(common::ObMySQLTransaction &trans, share::ObUnitTableOperator &unit_operator);
  int correct_unit_related_table_(common::ObMySQLTransaction &trans);
  int correct_max_resource_id_(common::ObMySQLTransaction &trans);
  int record_history_table_(common::ObMySQLTransaction &trans);
  int clean_up_on_failure_();
private:
  bool inited_;
  ObTimeoutCtx ctx_;
  sql::ObReplaceTenantStmt stmt_;
  uint64_t new_server_id_;
  uint64_t new_unit_id_;
  uint64_t new_unit_config_id_;
  uint64_t new_resource_pool_id_;
  share::ObServerInfoInTable::ObBuildVersion build_version_;
  obrpc::ObAdminStorageArg shared_storage_info_;
  share::ObBackupDest storage_dest_;
  share::ObUnitConfig unit_config_;
};

} // end namespace rootserver
} // end namespace oceanbase

#endif // OCEANBASE_ROOTSERVER_OB_DISASTER_RECOVERY_REPLACE_TENANT_H_

#endif
#endif