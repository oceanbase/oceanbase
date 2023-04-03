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

#ifndef OCEANBASE_SHARE_SCHEMA_OB_PRIV_SQL_SERVICE_H_
#define OCEANBASE_SHARE_SCHEMA_OB_PRIV_SQL_SERVICE_H_

#include "share/schema/ob_ddl_sql_service.h"
#include "share/schema/ob_priv_type.h"
#include "share/ob_priv_common.h"
#include "share/ob_dml_sql_splicer.h"

namespace oceanbase
{
namespace share
{
class ObDMLSqlSplicer;
namespace schema
{
struct ObOriginalDBKey;
struct ObTablePrivSortKey;

class ObPrivSqlService : public ObDDLSqlService
{
public:
  ObPrivSqlService(ObSchemaService &schema_service)
    : ObDDLSqlService(schema_service) {}
  virtual ~ObPrivSqlService() {}

  virtual int grant_database(
      const ObOriginalDBKey &db_priv_key,
      const ObPrivSet priv_set,
      const int64_t new_schema_version,
      const common::ObString *ddl_stmt_str,
      common::ObISQLClient &sql_client);

  virtual int revoke_database(
      const ObOriginalDBKey &db_priv_key,
      const ObPrivSet priv_set,
      const int64_t new_schema_version,
      const common::ObString *ddl_stmt_str,
      common::ObISQLClient &sql_client);

  virtual int delete_db_priv(
      const ObOriginalDBKey &org_db_key,
      const int64_t new_schema_version,
      common::ObISQLClient &sql_client);

  virtual int grant_table_ora_only(
    const ObString *ddl_stmt_str,
    ObISQLClient &sql_client,
    const share::ObRawObjPrivArray &obj_priv_array,
    const uint64_t option,
    const ObObjPrivSortKey &obj_priv_key,
    const int64_t new_schema_version_ora,
    const bool is_deleted,
    bool is_delete_all);

  virtual int grant_table(
      const ObTablePrivSortKey &table_priv_key,
      const ObPrivSet priv_set,
      const int64_t new_schema_version,
      const common::ObString *ddl_stmt_str,
      common::ObISQLClient &sql_client,
      const share::ObRawObjPrivArray &obj_priv_array,
      const uint64_t option,
      const ObObjPrivSortKey &obj_priv_key,
      const int64_t new_schema_version_ora,
      const bool is_grant,
      bool is_revoke_all_ora);
 
 virtual int revoke_table(
      const ObTablePrivSortKey &table_priv_key,
      const ObPrivSet priv_set,
      const int64_t new_schema_version,
      const common::ObString *ddl_stmt_str,
      ObISQLClient &sql_client,
      const int64_t new_schema_version_ora,
      const ObObjPrivSortKey &obj_priv_key,
      const share::ObRawObjPrivArray &obj_priv_array,
      bool is_revoke_all);

  virtual int revoke_table_ora(
      const ObObjPrivSortKey &obj_priv_key,
      const share::ObRawObjPrivArray &obj_priv_array,
      const int64_t new_schema_version,
      const common::ObString *ddl_stmt_str,
      ObISQLClient &sql_client,
      bool is_revoke_all);
      
  virtual int delete_table_priv(
      const ObTablePrivSortKey &table_priv_key,
      const int64_t new_schema_version,
      common::ObISQLClient &sql_client);
  
  virtual int alter_user_default_role(
      const share::schema::ObUserInfo &user_info,
      const int64_t new_schema_version,
      const ObString *ddl_stmt_str,
      common::ObIArray<uint64_t> &role_id_array, 
      common::ObIArray<uint64_t> &disable_flag_array,
      common::ObISQLClient &sql_client);

  virtual int grant_revoke_role(
      const uint64_t tenant_id,
      const share::schema::ObUserInfo &user_info,
      const common::ObIArray<uint64_t> &role_ids,
      const share::schema::ObUserInfo *specified_role_info,
      const int64_t new_schema_version,
      const common::ObString *ddl_stmt_str,
      common::ObISQLClient &sql_client,
      const bool is_grant,
      share::schema::ObSchemaGetterGuard &schema_guard,
      const uint64_t option);
  
  virtual int grant_sys_priv_to_ur(
      const uint64_t tenant_id,
      const uint64_t grantee_id,
      const uint64_t option,
      const ObRawPrivArray &priv_array,
      const int64_t new_schema_version,
      const common::ObString *ddl_stmt_str,
      common::ObISQLClient &sql_client,
      const bool is_grant,
      const bool is_revoke_all);

  virtual int delete_obj_priv(
      const ObObjPriv &obj_priv,
      const int64_t new_schema_version,
      ObISQLClient &sql_client);

private:
  int log_obj_priv_operation(
      const ObObjPrivSortKey &obj_priv_key,
      const int64_t new_schema_version,
      const ObSchemaOperationType op_type,
      const ObString *ddl_stmt_str,
      ObISQLClient &sql_client);

  int add_db_priv_history(
      const ObOriginalDBKey &db_priv_key,
      const ObPrivSet &priv_set,
      const int64_t schema_version,
      common::ObISQLClient &sql_client);
  
  int add_table_priv_history(
      const ObTablePrivSortKey &table_priv_key,
      const ObPrivSet &priv_set,
      const int64_t schema_version,
      common::ObISQLClient &sql_client);

  int gen_db_priv_dml(
      const uint64_t exec_tenant_id,
      const ObOriginalDBKey &db_priv_key,
      const ObPrivSet &priv_set,
      share::ObDMLSqlSplicer &dml);
  
  int gen_table_priv_dml(
      const uint64_t exec_tenant_id,
      const ObTablePrivSortKey &table_priv_key,
      const ObPrivSet &priv_set,
      share::ObDMLSqlSplicer &dml);

  int gen_grant_sys_priv_dml(
      const uint64_t exec_tenant_id,
      const uint64_t tenant_id,
      const uint64_t grantee_id,
      const uint64_t option,
      const ObRawPriv &raw_priv,
      ObDMLSqlSplicer &dml);

  int insert_objauth(
      const uint64_t exec_tenant_id,
      const ObObjPrivSortKey &obj_priv_key, 
      const int64_t new_schema_version,
      const share::ObRawObjPrivArray &obj_priv_array,
      const bool is_deleted,
      const uint64_t option,
      share::ObDMLExecHelper &exec,
      ObDMLSqlSplicer &dml);

  int gen_obj_priv_dml_ora(
    const uint64_t exec_tenant_id,
    const ObObjPrivSortKey &obj_priv_key,
    share::ObRawObjPriv &raw_obj_priv,
    const uint64_t option,
    ObDMLSqlSplicer &dml,
    bool is_deleted);

private:
  DISALLOW_COPY_AND_ASSIGN(ObPrivSqlService);
};

} //end of namespace schema
} //end of namespace share
} //end of namespace oceanbase

#endif //OCEANBASE_SHARE_SCHEMA_OB_PRIV_SQL_SERVICE_H_
