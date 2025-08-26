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

#define USING_LOG_PREFIX RS
#include "rootserver/ob_objpriv_mysql_ddl_operator.h"
#include "rootserver/ob_ddl_operator.h"
#include "src/share/schema/ob_priv_sql_service.h"
#include "rootserver/ob_ddl_sql_generator.h"

namespace oceanbase
{
namespace rootserver
{

int ObObjPrivMysqlDDLOperator::grant_object(
                               const ObObjMysqlPrivSortKey &object_priv_key,
                               const ObPrivSet priv_set,
                               common::ObMySQLTransaction &trans,
                               const uint64_t option,
                               const bool gen_ddl_stmt,
                               const common::ObString &grantor,
                               const common::ObString &grantor_host)
{
  int ret = OB_SUCCESS;
  share::ObRawObjPrivArray new_obj_priv_array;
  const uint64_t tenant_id = object_priv_key.tenant_id_;
  ObSchemaGetterGuard schema_guard;
  ObSchemaService *schema_sql_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_sql_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schama service_impl and schema manage must not null",
        "schema_service_impl", schema_sql_service, K(ret));
  } else if (!object_priv_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("object_priv_key is invalid", K(object_priv_key), K(ret));
  } else if (0 == priv_set) {
    //do nothing
  } else if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret));
  } else {
    ObPrivSet new_priv = priv_set;
    ObPrivSet object_priv_set = OB_PRIV_SET_EMPTY;
    if (OB_FAIL(schema_guard.get_obj_mysql_priv_set(object_priv_key, object_priv_set))) {
      LOG_WARN("get object priv set failed", K(ret));
    } else {
      bool need_flush = true;
      new_priv |= object_priv_set;
      need_flush = (new_priv != object_priv_set);
      if (need_flush) {
        ObSqlString ddl_stmt_str;
        ObString ddl_sql;
        const ObUserInfo *user_info = NULL;
        ObNeedPriv need_priv;
        need_priv.table_ = object_priv_key.object_name_;
        need_priv.priv_level_ = OB_PRIV_OBJECT_LEVEL;
        need_priv.priv_set_ = (~object_priv_set) & new_priv;
        need_priv.obj_type_ = static_cast<share::schema::ObObjectType>(object_priv_key.object_type_);
        if (OB_FAIL(schema_guard.get_user_info(tenant_id, object_priv_key.user_id_, user_info))) {
          LOG_WARN("get user info failed", K(object_priv_key), K(ret));
        } else if (OB_ISNULL(user_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("user not exist", K(object_priv_key), K(ret));
        } else if (gen_ddl_stmt == true && OB_FAIL(ObDDLSqlGenerator::gen_object_priv_sql(
            obrpc::ObAccountArg(user_info->get_user_name_str(), user_info->get_host_name_str()),
            need_priv, true, /*is_grant*/ ddl_stmt_str))) {
          LOG_WARN("gen_object_priv_sql failed", K(ret), K(need_priv));
        } else if (FALSE_IT(ddl_sql = ddl_stmt_str.string())) {
        } else {
          int64_t new_schema_version = OB_INVALID_VERSION;
          int64_t new_schema_version_ora = OB_INVALID_VERSION;
          if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
            LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
          } else if (OB_FAIL(schema_sql_service->get_priv_sql_service().grant_object(
                object_priv_key, new_priv, new_schema_version, &ddl_sql, trans, option, true,
                grantor, grantor_host))) {
            LOG_WARN("priv sql service grant object failed", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObObjPrivMysqlDDLOperator::revoke_object(
                               const ObObjMysqlPrivSortKey &object_priv_key,
                               const ObPrivSet priv_set,
                               common::ObMySQLTransaction &trans,
                               bool report_error,
                               const bool gen_ddl_stmt,
                               const common::ObString &grantor,
                               const common::ObString &grantor_host)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = object_priv_key.tenant_id_;
  ObSchemaGetterGuard schema_guard;
  ObSchemaService *schema_sql_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_sql_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schama service_impl and schema manage must not null",
        "schema_service_impl", schema_sql_service,
        K(ret));
  } else if (!object_priv_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("db_priv_key is invalid", K(object_priv_key), K(ret));
  } else if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret));
  } else {
    ObPrivSet object_priv_set = OB_PRIV_SET_EMPTY;
    if (OB_FAIL(schema_guard.get_obj_mysql_priv_set(object_priv_key, object_priv_set))) {
      LOG_WARN("get object priv set failed", K(ret));
    } else if (OB_PRIV_SET_EMPTY == object_priv_set) {
      if (report_error) {
        ret = OB_ERR_CANNOT_REVOKE_PRIVILEGES_YOU_DID_NOT_GRANT;
        LOG_WARN("No such grant to revoke", K(object_priv_key), K(object_priv_set), K(ret));
      }
    } else if (0 == priv_set) {
      // do-nothing
    } else {
      ObPrivSet new_priv = object_priv_set & (~priv_set);
      /* If there is an intersection between the existing permissions and the permissions that require revoke */
      if ((object_priv_set & priv_set) != 0) {
        ObSqlString ddl_stmt_str;
        ObString ddl_sql;
        const ObUserInfo *user_info = NULL;
        ObNeedPriv need_priv;
        need_priv.table_ = object_priv_key.object_name_;
        need_priv.priv_level_ = OB_PRIV_OBJECT_LEVEL;
        need_priv.priv_set_ = object_priv_set & priv_set;
        need_priv.obj_type_ = static_cast<ObObjectType>(object_priv_key.object_type_);
        int64_t new_schema_version = OB_INVALID_VERSION;
        int64_t new_schema_version_ora = OB_INVALID_VERSION;
        if (OB_FAIL(schema_guard.get_user_info(tenant_id, object_priv_key.user_id_, user_info))) {
          LOG_WARN("get user info failed", K(object_priv_key), K(ret));
        } else if (OB_ISNULL(user_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("user not exist", K(object_priv_key), K(ret));
        } else if (gen_ddl_stmt == true && OB_FAIL(ObDDLSqlGenerator::gen_object_priv_sql(
                   obrpc::ObAccountArg(user_info->get_user_name_str(), user_info->get_host_name_str()),
                                       need_priv,
                                       false, /*is_grant*/
                                       ddl_stmt_str))) {
          LOG_WARN("gen_object_priv_sql failed", K(ret), K(need_priv));
        } else if (FALSE_IT(ddl_sql = ddl_stmt_str.string())) {
        } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
          LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
        } else if (OB_FAIL(schema_sql_service->get_priv_sql_service().revoke_object(
                   object_priv_key, new_priv, new_schema_version, &ddl_sql, trans, grantor, grantor_host))) {
          LOG_WARN("Failed to revoke object", K(object_priv_key), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObObjPrivMysqlDDLOperator::drop_obj_mysql_privs(
                   const uint64_t tenant_id,
                   const ObString &obj_name,
                   const uint64_t obj_type,
                   ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;

  OZ (schema_service_.get_tenant_schema_guard(tenant_id, schema_guard));
  OZ (drop_obj_mysql_privs(tenant_id, obj_name, obj_type, trans, schema_service_, schema_guard));

  return ret;
}

int ObObjPrivMysqlDDLOperator::drop_obj_mysql_privs(
                               const uint64_t tenant_id,
                               const ObString &obj_name,
                               const uint64_t obj_type,
                               ObMySQLTransaction &trans,
                               ObMultiVersionSchemaService &schema_service,
                               ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_sql_service = schema_service.get_schema_service();
  ObArray<const ObObjMysqlPriv *> obj_privs;

  CK (OB_NOT_NULL(schema_sql_service));
  OZ (schema_guard.get_obj_mysql_priv_with_obj_name(tenant_id, obj_name, obj_type, obj_privs, true));
  for (int64_t i = 0; OB_SUCC(ret) && i < obj_privs.count(); ++i) {
    const ObObjMysqlPriv *obj_priv = obj_privs.at(i);
    int64_t new_schema_version = OB_INVALID_VERSION;

    if (OB_ISNULL(obj_priv)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("obj_priv priv is NULL", K(ret), K(obj_priv));
    } else {
      OZ (schema_service.gen_new_schema_version(tenant_id, new_schema_version));
      OZ (schema_sql_service->get_priv_sql_service().delete_obj_mysql_priv(
                *obj_priv, new_schema_version, trans));
    }
  }
  return ret;
}

}
}