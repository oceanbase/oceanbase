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

#ifndef OB_SCHEMA_REVISE_EXECUTOR_H_
#define OB_SCHEMA_REVISE_EXECUTOR_H_

#include "lib/thread/ob_async_task_queue.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/schema/ob_multi_version_schema_service.h"

namespace oceanbase {
namespace common {
class ObMySQLProxy;
}
namespace share {
namespace schema {
class ObMultiVersionSchemaService;
}
}  // namespace share
namespace rootserver {
class ObSchemaReviseExecutor;
class ObDDLService;
class ObSchemaReviseTask : public share::ObAsyncTask {
public:
  explicit ObSchemaReviseTask(ObSchemaReviseExecutor& executor) : executor_(&executor)
  {}
  virtual ~ObSchemaReviseTask() = default;
  virtual int64_t get_deep_copy_size() const;
  share::ObAsyncTask* deep_copy(char* buf, const int64_t buf_size) const;
  virtual int process() override;

private:
  ObSchemaReviseExecutor* executor_;
};

class ObSchemaReviseExecutor {
public:
  ObSchemaReviseExecutor();
  ~ObSchemaReviseExecutor() = default;
  int init(share::schema::ObMultiVersionSchemaService& schema_service, ObDDLService& ddl_service,
      common::ObMySQLProxy& sql_proxy, obrpc::ObSrvRpcProxy& rpc_proxy, ObServerManager& server_mgr);
  int execute();
  int can_execute();
  void start();
  int stop();

private:
  int set_execute_mark();
  int check_stop();
  int check_schema_sync();
  int check_standby_dml_sync(const obrpc::ObClusterTenantStats& primary_tenant_stats,
      const obrpc::ObClusterTenantStats& standby_tenant_stats, bool& schema_sync);
  int compute_check_cst_count_by_tenant(const uint64_t tenant_id, int64_t& check_constraint_count);
  int generate_constraint_schema(const uint64_t tenant_id, ObIAllocator& allocator,
      share::schema::ObSchemaGetterGuard& schema_guard, ObSArray<share::schema::ObConstraint>& csts);
  int replace_constraint_column_info_in_inner_table(
      const uint64_t tenant_id, ObSArray<share::schema::ObConstraint>& csts, bool is_history);
  int do_schema_revise();
  int do_inner_table_revise();
  int do_inner_table_revise_by_tenant(
      const uint64_t tenant_id, ObIAllocator& allocator, share::schema::ObSchemaGetterGuard& schema_guard);
  int flush_schema_kv_cache();

private:
  bool is_inited_;
  bool is_stopped_;
  bool execute_;
  common::SpinRWLock rwlock_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  ObDDLService* ddl_service_;
  common::ObMySQLProxy* sql_proxy_;
  obrpc::ObSrvRpcProxy* rpc_proxy_;
  ObServerManager* server_mgr_;
  DISALLOW_COPY_AND_ASSIGN(ObSchemaReviseExecutor);
};

}  // end namespace rootserver
}  // end namespace oceanbase

#endif  // OB_SCHEMA_REVISE_EXECUTOR_H_
