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

#ifndef OCEANBASE_SCHEMA_SPLIT_EXECUTOR_H_
#define OCEANBASE_SCHEMA_SPLIT_EXECUTOR_H_

#include "lib/thread/ob_async_task_queue.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "rootserver/ob_schema_spliter.h"

namespace oceanbase {
namespace rootserver {
class ObSchemaSplitExecutor;

class ObSchemaSplitTask : public share::ObAsyncTask {
public:
  explicit ObSchemaSplitTask(ObSchemaSplitExecutor& schema_split_executor, ObRsJobType type)
      : schema_split_executor_(&schema_split_executor), type_(type)
  {}
  virtual ~ObSchemaSplitTask()
  {}
  virtual int64_t get_deep_copy_size() const;
  share::ObAsyncTask* deep_copy(char* buf, const int64_t buf_size) const;
  virtual int process();

private:
  ObSchemaSplitExecutor* schema_split_executor_;
  ObRsJobType type_;
};

class ObSchemaSplitExecutor {
public:
  ObSchemaSplitExecutor();
  ~ObSchemaSplitExecutor()
  {}
  int init(share::schema::ObMultiVersionSchemaService& schema_service, common::ObMySQLProxy* sql_proxy,
      obrpc::ObSrvRpcProxy& rpc_proxy, ObServerManager& server_mgr);

  int execute();
  int execute(ObRsJobType type);
  int can_execute();
  int check_stop();

  void start();
  int stop();

private:
  int set_execute_mark();

  int do_execute();

  int pre_check(bool& has_done);
  int create_tables();
  int check_schema_sync();
  int migrate_and_check();
  int finish_schema_split();
  int check_after_schema_split();

  int set_enable_sys_table_ddl(bool value);
  int set_enable_ddl(bool value);
  int check_all_server();

  int do_execute_v2();
  int finish_schema_split_v2(const uint64_t tenant_id);
  int check_schema_sync(obrpc::ObTenantSchemaVersions& primary_schema_versions,
      obrpc::ObTenantSchemaVersions& standby_schema_versions, bool& schema_sync);
  int migrate_core_table_schema();

private:
  bool inited_;
  bool stopped_;
  bool execute_;
  common::SpinRWLock rwlock_;
  common::ObMySQLProxy* sql_proxy_;
  obrpc::ObSrvRpcProxy* rpc_proxy_;
  ObServerManager* server_mgr_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  DISALLOW_COPY_AND_ASSIGN(ObSchemaSplitExecutor);
};
}  // namespace rootserver
}  // namespace oceanbase
#endif  // OCEANBASE_SCHEMA_SPLIT_EXECUTOR_H
