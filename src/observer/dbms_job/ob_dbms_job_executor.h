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

#ifndef SRC_OBSERVER_DBMS_JOB_EXECUTOR_H_
#define SRC_OBSERVER_DBMS_JOB_EXECUTOR_H_

#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/allocator/ob_mod_define.h"
#include "share/schema/ob_multi_version_schema_service.h"

namespace oceanbase
{
namespace sql
{
class ObExecEnv;
}
namespace dbms_job
{
class ObDBMSJobInfo;
class ObDBMSJobUtils;
class ObDBMSJobExecutor
{
public:
  ObDBMSJobExecutor() : inited_(false), sql_proxy_(NULL), schema_service_(NULL) {}

  virtual ~ObDBMSJobExecutor() {}

  int init(
    common::ObMySQLProxy *sql_proxy, share::schema::ObMultiVersionSchemaService *schema_service);

  int init_session(
    sql::ObSQLSessionInfo &session,
    share::schema::ObSchemaGetterGuard &schema_guard,
    const common::ObString &tenant_name, uint64_t tenant_id,
    const common::ObString &database_name, uint64_t database_id,
    const share::schema::ObUserInfo* user_info,
    sql::ObExecEnv &exec_env);

  int init_env(ObDBMSJobInfo &job_info, sql::ObSQLSessionInfo &session);

  int run_dbms_job(uint64_t tenant_id, uint64_t job_id);
  int run_dbms_job(uint64_t tenant_id, ObDBMSJobInfo &job_info, ObIAllocator &allocator);

private:
  bool inited_;
  ObDBMSJobUtils job_utils_;
  common::ObMySQLProxy *sql_proxy_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
};

}
}
#endif /* SRC_OBSERVER_DBMS_JOB_EXECUTOR_H_ */

