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

#ifndef OCEANBASE_UPGRADE_EXECUTOR_H_
#define OCEANBASE_UPGRADE_EXECUTOR_H_

#include "lib/thread/ob_async_task_queue.h"
#include "share/ob_upgrade_utils.h"
#include "share/ob_check_stop_provider.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/ob_rpc_struct.h"
#include "rootserver/ob_rs_job_table_operator.h"
#include "rootserver/ob_root_inspection.h"

namespace oceanbase
{
namespace rootserver
{
class ObUpgradeExecutor;

class ObUpgradeTask: public share::ObAsyncTask
{
public:
  explicit ObUpgradeTask(ObUpgradeExecutor &upgrade_executor)
           : upgrade_executor_(&upgrade_executor), arg_()
  {}
  virtual ~ObUpgradeTask() {}
  int init(const obrpc::ObUpgradeJobArg &arg);
  virtual int64_t get_deep_copy_size() const;
  share::ObAsyncTask *deep_copy(char *buf, const int64_t buf_size) const;
  virtual int process();
private:
  ObUpgradeExecutor *upgrade_executor_;
  obrpc::ObUpgradeJobArg arg_;
};

class ObUpgradeExecutor : public share::ObCheckStopProvider
{
public:
  ObUpgradeExecutor();
  ~ObUpgradeExecutor() {}
  int init(share::schema::ObMultiVersionSchemaService &schema_service,
           rootserver::ObRootInspection &root_inspection,
           common::ObMySQLProxy &sql_proxy,
           obrpc::ObSrvRpcProxy &rpc_proxy,
           obrpc::ObCommonRpcProxy &common_proxy);

  int execute(const obrpc::ObUpgradeJobArg &arg);
  int can_execute();
  int check_stop() const;
  bool check_execute() const;

  void start();
  int stop();
private:
  int check_inner_stat_() const;
  int set_execute_mark_();

  int run_upgrade_post_job_(const common::ObIArray<uint64_t> &tenant_ids,
                            const int64_t version);

  /*-----upgrade all cmd----*/
  int run_upgrade_begin_action_(const common::ObIArray<uint64_t> &tenant_ids);
  int run_upgrade_system_variable_job_(const common::ObIArray<uint64_t> &tenant_ids);
  int run_upgrade_system_table_job_(const common::ObIArray<uint64_t> &tenant_ids);
  int run_upgrade_virtual_schema_job_(const common::ObIArray<uint64_t> &tenant_ids);
  int run_upgrade_system_package_job_();
  int run_upgrade_all_post_action_(const common::ObIArray<uint64_t> &tenant_ids);
  int run_upgrade_inspection_job_(const common::ObIArray<uint64_t> &tenant_ids);
  int run_upgrade_end_action_(const common::ObIArray<uint64_t> &tenant_ids);

  int run_upgrade_all_(const common::ObIArray<uint64_t> &tenant_ids);
  /*-----upgrade all cmd----*/

  int run_upgrade_begin_action_(
      const uint64_t tenant_id,
      common::hash::ObHashMap<uint64_t, share::SCN> &tenants_sys_ls_target_scn);
  int upgrade_system_table_(const uint64_t tenant_id);
  int check_table_schema_(const uint64_t tenant_id,
                          const share::schema::ObTableSchema &hard_code_table);
  int upgrade_mysql_system_package_job_();
#ifdef OB_BUILD_ORACLE_PL
  int upgrade_oracle_system_package_job_();
#endif
  int run_upgrade_all_post_action_(const uint64_t tenant_id);
  int run_upgrade_end_action_(const uint64_t tenant_id);

  int check_schema_sync_(const uint64_t tenant_id);
  int check_schema_sync_(
      obrpc::ObTenantSchemaVersions &primary_schema_versions,
      obrpc::ObTenantSchemaVersions &standby_schema_versions,
      bool &schema_sync);
  int construct_tenant_ids_(
      const common::ObIArray<uint64_t> &src_tenant_ids,
      common::ObIArray<uint64_t> &dst_tenant_ids);
  rootserver::ObRsJobType convert_to_job_type_(
      const obrpc::ObUpgradeJobArg::Action &action);

  int fill_extra_info_(
      const uint64_t tenant_id,
      const int64_t specified_version,
      const uint64_t current_data_version,
      const int64_t buf_len,
      char *buf);
private:
  bool inited_;
  bool stopped_;
  bool execute_;
  common::SpinRWLock rwlock_;
  common::ObMySQLProxy *sql_proxy_;
  obrpc::ObSrvRpcProxy *rpc_proxy_;
  obrpc::ObCommonRpcProxy *common_rpc_proxy_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
  rootserver::ObRootInspection *root_inspection_;
  share::ObUpgradeProcesserSet upgrade_processors_;
  DISALLOW_COPY_AND_ASSIGN(ObUpgradeExecutor);
};
}//end rootserver
}//end oceanbase
#endif // OCEANBASE_UPGRADE_EXECUTOR_H
