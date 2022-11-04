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

#ifndef OB_CREATE_INNER_SCHEMA_EXECUTOR_H_
#define OB_CREATE_INNER_SCHEMA_EXECUTOR_H_

#include "lib/thread/ob_async_task_queue.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/schema/ob_multi_version_schema_service.h"

namespace oceanbase
{
namespace common
{
class ObMySQLProxy;
}
namespace obrpc
{
class ObCommonRpcProxy;
}
namespace share
{
namespace schema
{
class ObMultiVersionSchemaService;
}
}
namespace rootserver
{
class ObCreateInnerSchemaExecutor;
class ObDDLService;
class ObCreateInnerSchemaTask : public share::ObAsyncTask
{
public:
  explicit ObCreateInnerSchemaTask(ObCreateInnerSchemaExecutor &executor)
    : executor_(&executor)
  {}
  virtual ~ObCreateInnerSchemaTask() = default;
  virtual int64_t get_deep_copy_size() const;
  share::ObAsyncTask *deep_copy(char *buf, const int64_t buf_size) const;
  virtual int process() override;
private:
  ObCreateInnerSchemaExecutor *executor_;
};

class ObCreateInnerSchemaExecutor
{
public:
  ObCreateInnerSchemaExecutor();
  ~ObCreateInnerSchemaExecutor() = default;
  int init(share::schema::ObMultiVersionSchemaService &schema_service,
           common::ObMySQLProxy &sql_proxy,
           obrpc::ObCommonRpcProxy &rpc_proxy);
  int execute();
  int can_execute();
  void start();
  int stop();
  static int ur_exists(
      share::schema::ObSchemaGetterGuard &schema_guard, 
      uint64_t tenant_id, 
      const uint64_t ur_id, 
      bool &exists);
  static int add_inner_role(
      uint64_t tenant_id,
      share::schema::ObSchemaGetterGuard &schema_guard,
      common::ObMySQLProxy *sql_proxy);
  static int check_and_create_inner_keysore(
      share::schema::ObSchemaGetterGuard &schema_guard, 
      int64_t tenant_id,
      obrpc::ObCommonRpcProxy *rpc_proxy);
  static int add_password_and_lock_security_sys_users(
      uint64_t tenant_id,
      share::schema::ObSchemaGetterGuard &schema_guard,
      common::ObMySQLProxy *sql_proxy);
  static int check_and_create_default_profile(
      share::schema::ObSchemaGetterGuard &schema_guard, 
      int64_t tenant_id, 
      obrpc::ObCommonRpcProxy *rpc_proxy);
  static int add_audit_user(
      uint64_t tenant_id, 
      share::schema::ObSchemaGetterGuard &schema_guard,
      common::ObMySQLProxy *sql_proxy);
  static int do_create_inner_schema_by_tenant(
      uint64_t tenant_id,
      oceanbase::lib::Worker::CompatMode compat_mode,
      share::schema::ObSchemaGetterGuard &schema_guard,
      common::ObMySQLProxy *sql_proxy,
      obrpc::ObCommonRpcProxy *rpc_proxy);
private:
  int set_execute_mark();
  int do_create_inner_schema();
  int check_stop();
  private:
  bool is_inited_;
  bool is_stopped_;
  bool execute_;
  common::SpinRWLock rwlock_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
  common::ObMySQLProxy *sql_proxy_;
  obrpc::ObCommonRpcProxy *rpc_proxy_;
  DISALLOW_COPY_AND_ASSIGN(ObCreateInnerSchemaExecutor);
};

}  // end namespace rootserver
}  // end namespace oceanbase

#endif  // OB_CREATE_INNER_SCHEMA_EXECUTOR_H_
