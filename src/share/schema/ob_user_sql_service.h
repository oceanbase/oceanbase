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

#ifndef OCEANBASE_SHARE_SCHEMA_OB_USER_SQL_SERVICE_H_
#define OCEANBASE_SHARE_SCHEMA_OB_USER_SQL_SERVICE_H_

#include "ob_ddl_sql_service.h"

namespace oceanbase
{
namespace common
{
class ObISQLClient;
}
namespace share
{
class ObDMLSqlSplicer;

namespace schema
{
class ObUserInfo;

class ObUserSqlService : public ObDDLSqlService
{
public:
  ObUserSqlService(ObSchemaService &schema_service)
    : ObDDLSqlService(schema_service) {}
  virtual ~ObUserSqlService() {}

  virtual int create_user(
      const ObUserInfo &user,
      const int64_t new_schema_version,
      const common::ObString *ddl_stmt_str,
      common::ObISQLClient &sql_client);
  virtual int alter_user(
      const ObUserInfo &user,
      const int64_t new_schema_version,
      const common::ObString *ddl_stmt_str,
      common::ObISQLClient &sql_client);
  virtual int drop_user(
      const uint64_t tenant_id,
      const uint64_t user_id,
      const uint64_t new_schema_version,
      const common::ObString *ddl_stmt_str,
      common::ObISQLClient &sql_client,
      share::schema::ObSchemaGetterGuard &schema_guard);
  virtual int rename_user(const ObUserInfo &user_info,
                          const int64_t new_schema_version,
                          const common::ObString *ddl_stmt_str,
                          common::ObISQLClient &sql_client);
  virtual int set_passwd(const ObUserInfo &user_info,
                         const int64_t new_schema_version,
                         const common::ObString *ddl_stmt_str,
                         common::ObISQLClient &sql_client);
  virtual int alter_role(const ObUserInfo &user_info,
                         const int64_t new_schema_version,
                         const common::ObString *ddl_stmt_str,
                         common::ObISQLClient &sql_client);
  virtual int set_passwd_impl(const ObUserInfo &user_info,
                              const int64_t new_schema_version,
                              const common::ObString *ddl_stmt_str,
                              common::ObISQLClient &sql_client,
                              const ObSchemaOperationType type);
  virtual int set_max_connections(const ObUserInfo &user_info,
                                  const int64_t new_schema_version,
                                  const ObString *ddl_stmt_str,
                                  ObISQLClient &sql_client);
  virtual int alter_user_require(const ObUserInfo &user_info,
                                 const int64_t new_schema_version,
                                 const common::ObString *ddl_stmt_str,
                                 common::ObISQLClient &sql_client);
  virtual int grant_revoke_user(const ObUserInfo &user_info,
                                const int64_t new_schema_version,
                                const common::ObString *ddl_stmt_str,
                                common::ObISQLClient &sql_client,
                                const bool is_from_inner_sql);
  virtual int lock_user(const ObUserInfo &user_info,
                        const int64_t new_schema_version,
                        const common::ObString *ddl_stmt_str,
                        common::ObISQLClient &sql_client);
  virtual int update_user_schema_version(const uint64_t tenant_id,
                                         const common::ObArray<ObUserInfo> &user_infos,
                                         const common::ObString *ddl_stmt_str,
                                         common::ObISQLClient &sql_client);
  virtual int alter_user_profile(const ObUserInfo &user_info,
                                 const common::ObString *ddl_stmt_str,
                                 common::ObISQLClient &sql_client);
  static int add_user_history(const ObUserInfo &user, 
                              const int64_t schema_version,
                              common::ObISQLClient &sql_client,
                              const bool is_from_inner_sql);
                              
  static int gen_user_dml(const uint64_t exec_tenant_id, 
                          const ObUserInfo &user, 
                          share::ObDMLSqlSplicer &dml,
                          const bool is_from_inner_sql);
  virtual int replace_user(
      const ObUserInfo &user,
      const int64_t new_schema_version,
      const common::ObString *ddl_stmt_str,
      common::ObISQLClient &sql_client,
      const ObSchemaOperationType type);

private:
  int drop_user_delete_role_grantee_map(
      const uint64_t tenant_id,
      bool is_role,
      const uint64_t new_schema_version,
      const ObUserInfo *user,
      const ObString *ddl_stmt_str,
      ObISQLClient &sql_client,
      share::schema::ObSchemaGetterGuard &schema_guard);
private:
  DISALLOW_COPY_AND_ASSIGN(ObUserSqlService);
};


} //end of namespace share
} //end of namespace schema
} //end of namespace oceanbase

#endif //OCEANBASE_SHARE_SCHEMA_OB_USER_SQL_SERVICE_H_
