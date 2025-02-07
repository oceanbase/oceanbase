/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_ROOTSERVER_OB_FLUSH_NCOMP_DLL_TASK_H_
#define OCEANBASE_ROOTSERVER_OB_FLUSH_NCOMP_DLL_TASK_H_

#include "share/schema/ob_schema_struct.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"

namespace oceanbase
{

namespace share
{

static const char *async_flush_ncomp_dll = "ASYNC_FLUSH_NCOMP_DLL_V2_EXPIRED_DATA";
static const char *async_flush_ncomp_dll_for_425 = "ASYNC_FLUSH_NCOMP_DLL_EXPIRED_DATA";

class ObFlushNcompDll
{
public:
  static int create_flush_ncomp_dll_job(
      const schema::ObSysVariableSchema &sys_variable,
      const uint64_t tenant_id,
      const bool is_enabled,
      common::ObMySQLTransaction &trans);
  static int create_flush_ncomp_dll_job_for_425(
      const schema::ObSysVariableSchema &sys_variable,
      const uint64_t tenant_id,
      const bool is_enabled,
      common::ObMySQLTransaction &trans);
private:
  static int check_flush_ncomp_dll_job_exists(ObMySQLTransaction &trans,
                                              const uint64_t tenant_id,
                                              const ObString &job_name,
                                              bool &is_job_exists);
  static int get_job_id(const uint64_t tenant_id,
                        ObMySQLTransaction &trans,
                        int64_t &job_id);
  static int get_job_action(ObSqlString &job_action);
  static int create_flush_ncomp_dll_job_common(const schema::ObSysVariableSchema &sys_variable,
                                                const uint64_t tenant_id,
                                                const bool is_enabled,
                                                ObMySQLTransaction &trans,
                                                const ObSqlString &job_action,
                                                const ObString &job_name);
};

}
}//end namespace oceanbase

#endif