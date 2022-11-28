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

namespace oceanbase
{
namespace rootserver
{
class ObUpgradeExecutor;

class ObUpgradeTask: public share::ObAsyncTask
{
public:
  explicit ObUpgradeTask(ObUpgradeExecutor &upgrade_executor,
                         const obrpc::ObUpgradeJobArg::Action action,
                         const int64_t version)
           : upgrade_executor_(&upgrade_executor), action_(action), version_(version)
  {}
  virtual ~ObUpgradeTask() {}
  virtual int64_t get_deep_copy_size() const;
  share::ObAsyncTask *deep_copy(char *buf, const int64_t buf_size) const;
  virtual int process();
private:
  ObUpgradeExecutor *upgrade_executor_;
  obrpc::ObUpgradeJobArg::Action action_;
  int64_t version_;
};

class ObUpgradeExecutor : public share::ObCheckStopProvider
{
public:
  ObUpgradeExecutor();
  ~ObUpgradeExecutor() {}
  int init(share::schema::ObMultiVersionSchemaService &schema_service,
           common::ObMySQLProxy &sql_proxy,
           obrpc::ObSrvRpcProxy &rpc_proxy,
           obrpc::ObCommonRpcProxy &common_proxy);

  int execute(const obrpc::ObUpgradeJobArg::Action action,
              const int64_t version);
  int can_execute();
  int check_stop() const;
  bool check_execute() const;

  void start();
  int stop();
private:
  int check_inner_stat_() const;
  int set_execute_mark_();

  int run_upgrade_post_job_(const int64_t version);
  int run_upgrade_system_variable_job_();
  int run_upgrade_system_table_job_();

  int upgrade_system_table_(const uint64_t tenant_id);
  int check_table_schema_(const uint64_t tenant_id,
                          const share::schema::ObTableSchema &hard_code_table);

  int check_schema_sync_();
  int check_schema_sync_(
      obrpc::ObTenantSchemaVersions &primary_schema_versions,
      obrpc::ObTenantSchemaVersions &standby_schema_versions,
      bool &schema_sync);
private:
  bool inited_;
  bool stopped_;
  bool execute_;
  common::SpinRWLock rwlock_;
  common::ObMySQLProxy *sql_proxy_;
  obrpc::ObSrvRpcProxy *rpc_proxy_;
  obrpc::ObCommonRpcProxy *common_rpc_proxy_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
  share::ObUpgradeProcesserSet upgrade_processors_;
  DISALLOW_COPY_AND_ASSIGN(ObUpgradeExecutor);
};
}//end rootserver
}//end oceanbase
#endif // OCEANBASE_UPGRADE_EXECUTOR_H
