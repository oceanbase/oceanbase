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

#define USING_LOG_PREFIX SHARE_SCHEMA
#include "ob_user_sql_service.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/schema/ob_priv_type.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "sql/ob_sql_utils.h"

namespace oceanbase
{
using namespace common;
namespace share
{
namespace schema
{

int ObUserSqlService::create_user(
    const ObUserInfo &user,
    const int64_t new_schema_version,
    const ObString *ddl_stmt_str,
    ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  const ObSchemaOperationType type = OB_DDL_CREATE_USER;
  if (!user.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Input arguments error", K(user), K(ret));
  } else if (OB_FAIL(replace_user(user, new_schema_version,
                                  ddl_stmt_str, sql_client,
                                  type))) {
    LOG_WARN("failed to replace user", K(ret), K(user));
  }
  return ret;
}
 
int ObUserSqlService::alter_user(
    const ObUserInfo &user,
    const int64_t new_schema_version,
    const ObString *ddl_stmt_str,
    ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  const ObSchemaOperationType type = OB_DDL_ALTER_USER;
  if (!user.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Input arguments error", K(user), K(ret));
  } else if (OB_FAIL(replace_user(user, new_schema_version,
                                  ddl_stmt_str, sql_client,
                                  type))) {
    LOG_WARN("failed to replace user", K(ret), K(user));
  }
  return ret;
}
 
int ObUserSqlService::replace_user(
    const ObUserInfo &user,
    const int64_t new_schema_version,
    const ObString *ddl_stmt_str,
    ObISQLClient &sql_client,
    const ObSchemaOperationType type)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = user.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  if (!user.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Input arguments error", K(user), K(ret));
  } else {
    int64_t affected_rows = 0;
    ObDMLExecHelper exec(sql_client, exec_tenant_id);
    ObDMLSqlSplicer dml;
    if (OB_FAIL(gen_user_dml(exec_tenant_id, user, dml, false))) {
      LOG_WARN("gen_user_dml failed", K(ret));
    }
    // insert into __all_user
    if (FAILEDx(exec.exec_replace(OB_ALL_USER_TNAME, dml, affected_rows))) {
      LOG_WARN("execute insert failed", K(ret));
    } else if (!is_single_row(affected_rows) && !is_double_row(affected_rows)) {
      // It may replace __all_user while reply schema in standby cluster.
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("affected_rows unexpected", K(affected_rows), K(ret));
    }

    // insert into __all_user_history
    if (FAILEDx(add_user_history(user, new_schema_version, sql_client, false))) {
      LOG_WARN("add_user_history failed", K(user), K(new_schema_version), K(ret));
    }

    // log operations
    if (OB_SUCC(ret)) {
      ObSchemaOperation priv_operation;
      priv_operation.tenant_id_ = user.get_tenant_id();
      priv_operation.user_id_ = user.get_user_id();
      priv_operation.op_type_ = type;
      priv_operation.schema_version_ = new_schema_version;
      priv_operation.ddl_stmt_str_ = ddl_stmt_str ? *ddl_stmt_str : ObString();
      if (OB_FAIL(log_operation(priv_operation, sql_client))) {
        LOG_WARN("Failed to log operation", K(ret));
      }
    }
  }
  return ret;
}

int ObUserSqlService::drop_user_delete_role_grantee_map(
  const uint64_t tenant_id,
  bool is_role,
  const uint64_t new_schema_version,
  const ObUserInfo *user,
  const ObString *ddl_stmt_str,
  ObISQLClient &sql_client,
  ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  CK (NULL != user);
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  const uint64_t user_id = user->get_user_id();
  const ObUserInfo *tmp_user = NULL;
  int64_t affected_rows = 0;
  common::ObSEArray<uint64_t, 8> schema_id_array = is_role
      ? user->get_grantee_id_array()
      : user->get_role_id_array();
  const bool is_need_update = schema_id_array.count() > 0;
  if (is_need_update) {
    common::ObArray<ObUserInfo> user_infos; // used to update related users' schema version
    ObSqlString del_sql; // from __all_tenant_role_grantee_map
    ObSqlString insert_sql; // insert into __all_tenant_role_grantee_map_history
    bool is_first = true;
    // delete row from __all_tenant_role_grantee_map
    if (is_role) {
      if (OB_FAIL(del_sql.append_fmt("DELETE FROM %s WHERE TENANT_ID = %lu and ROLE_ID = %lu and GRANTEE_ID IN (",
          OB_ALL_TENANT_ROLE_GRANTEE_MAP_TNAME,
          ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
          ObSchemaUtils::get_extract_schema_id(exec_tenant_id, user_id)))) {
        LOG_WARN("append table name failed, ", K(ret), K(tenant_id), K(user_id));
      }
    } else if (FAILEDx(del_sql.append_fmt("DELETE FROM %s WHERE TENANT_ID = %lu and GRANTEE_ID = %lu and ROLE_ID IN (",
        OB_ALL_TENANT_ROLE_GRANTEE_MAP_TNAME,
        ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
        ObSchemaUtils::get_extract_schema_id(exec_tenant_id, user_id)))) {
      LOG_WARN("append table name failed, ", K(ret), K(tenant_id), K(user_id));
    }

    // insert new row into __all_tenant_role_grantee_map_history
    if (FAILEDx(insert_sql.append_fmt("INSERT INTO %s VALUES ", OB_ALL_TENANT_ROLE_GRANTEE_MAP_HISTORY_TNAME))) {
      LOG_WARN("append table name failed, ", K(ret));
    }

    // generate user_infos, del_sql, insert_sql
    for (int64_t i = 0; OB_SUCC(ret) && i < schema_id_array.count(); i++) {
      const uint64_t id = schema_id_array.at(i);
      // collect user infos
      tmp_user = NULL;
      if (OB_FAIL(schema_guard.get_user_info(tenant_id, id, tmp_user))) {
        LOG_WARN("failed to get user info", K(ret), K(tenant_id), K(id));
      } else if (NULL == tmp_user) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("user info is null", K(ret), K(id));
      } else if (OB_FAIL(user_infos.push_back(*tmp_user))) {
        LOG_WARN("fail to push back", K(ret), KPC(tmp_user));
      }

      // generate delete sql stmt
      if (OB_SUCC(ret) && !is_first) {
        if (OB_FAIL(del_sql.append_fmt(", "))) {
          LOG_WARN("append sql failed, ", K(ret));
        }
      }
      if (FAILEDx(del_sql.append_fmt("%lu", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, id)))) {
        LOG_WARN("append sql failed, ", K(ret), K(id));
      }

      // genereate insert sql stmt
      if (OB_SUCC(ret) && !is_first) {
        if (OB_FAIL(insert_sql.append_fmt(", "))) {
          LOG_WARN("append sql failed, ", K(ret));
        }
      }
      const int64_t is_deleted = 1;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(insert_sql.append_fmt("(now(6), now(6), %lu, %lu, %lu, %ld, %ld, %lu, %lu)",
          ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id), 
          ObSchemaUtils::get_extract_schema_id(exec_tenant_id, is_role ? id : user_id), 
          ObSchemaUtils::get_extract_schema_id(exec_tenant_id, is_role ? user_id : id),
          new_schema_version, 
          is_deleted,
          static_cast<uint64_t>(0), /* admin option. xinqi.zlm to do */
          static_cast<uint64_t>(0)/* disable flag. xinqi.zlm to do */))) {
        LOG_WARN("append sql failed, ", K(ret));
      }
      is_first = false;
    }
    // delete from __all_tenant_role_grantee_map
    if (OB_SUCC(ret)) {
      if (OB_FAIL(del_sql.append_fmt(")"))) {
        LOG_WARN("append sql failed, ", K(ret));
      } else if (OB_FAIL(sql_client.write(exec_tenant_id, del_sql.ptr(), affected_rows))) {
        LOG_WARN("execute sql failed,  ", "sql", del_sql.ptr(), K(ret));
      } else if (schema_id_array.count() != affected_rows) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("del affected_rows is not expected", K(ret), K(affected_rows), K(schema_id_array.count()));
      }
    }
    // insert into __all_tenant_role_grantee_map_history
    if (OB_SUCC(ret)) {
      affected_rows = 0;
      if (OB_FAIL(sql_client.write(exec_tenant_id, insert_sql.ptr(), affected_rows))) {
        LOG_WARN("execute sql failed,  ", "sql", insert_sql.ptr(), K(ret));
      } else if (schema_id_array.count() != affected_rows) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("insert affected_rows is not expected", K(ret), K(affected_rows), K(schema_id_array.count()));
      }
    }
    // update related users' schema version
    if (FAILEDx((update_user_schema_version(tenant_id,
        user_infos,
        ddl_stmt_str,
        sql_client)))) {
      LOG_WARN("Failed to grant or revoke user", K(exec_tenant_id), K(ret));
    }
  }
  return ret;
}

int ObUserSqlService::drop_proxy_info(ObISQLClient &sql_client,
                                      const uint64_t tenant_id,
                                      const uint64_t client_user_id,
                                      const uint64_t proxy_user_id,
                                      const uint64_t new_schema_version)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  ObDMLExecHelper exec(sql_client, exec_tenant_id);
  int64_t affected_rows = 0;
  if (OB_FAIL(dml.add_pk_column("tenant_id", 0))
      || OB_FAIL(dml.add_pk_column("client_user_id", client_user_id))
      || OB_FAIL(dml.add_pk_column("proxy_user_id", proxy_user_id))) {
    LOG_WARN("add pk column failed", K(ret));
  } else if (OB_FAIL(exec.exec_delete(OB_ALL_USER_PROXY_INFO_TNAME, dml, affected_rows))) {
    LOG_WARN("exec delete failed", K(ret));
  } else if (OB_UNLIKELY(affected_rows > 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("affected_rows expected to be less than 2", K(affected_rows), K(ret));
  } else if (OB_FAIL(dml.add_pk_column("schema_version", new_schema_version))
            || OB_FAIL(dml.add_column("is_deleted", 1))) {
    LOG_WARN("add column failed", K(ret));
  } else if (OB_FAIL(exec.exec_insert(OB_ALL_USER_PROXY_INFO_HISTORY_TNAME, dml, affected_rows))) {
    LOG_WARN("exec insert failed", K(ret));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("affected_rows expected to be one", K(affected_rows), K(ret));
  }
  return ret;
}

int ObUserSqlService::drop_proxy_role_info(ObISQLClient &sql_client,
                                      const uint64_t tenant_id,
                                      const uint64_t client_user_id,
                                      const uint64_t proxy_user_id,
                                      const uint64_t role_id,
                                      const uint64_t new_schema_version)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  ObDMLExecHelper exec(sql_client, exec_tenant_id);
  int64_t affected_rows = 0;
  if (OB_FAIL(dml.add_pk_column("tenant_id", 0))
      || OB_FAIL(dml.add_pk_column("client_user_id", client_user_id))
      || OB_FAIL(dml.add_pk_column("proxy_user_id", proxy_user_id))
      || OB_FAIL(dml.add_pk_column("role_id", role_id))) {
    LOG_WARN("add pk column failed", K(ret));
  } else if (OB_FAIL(exec.exec_delete(OB_ALL_USER_PROXY_ROLE_INFO_TNAME, dml, affected_rows))) {
    LOG_WARN("exec delete failed", K(ret));
  } else if (OB_UNLIKELY(affected_rows > 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("affected_rows expected to be less than 2", K(affected_rows), K(ret));
  } else if (OB_FAIL(dml.add_pk_column("schema_version", new_schema_version))
            || OB_FAIL(dml.add_column("is_deleted", 1))) {
    LOG_WARN("add column failed", K(ret));
  } else if (OB_FAIL(exec.exec_insert(OB_ALL_USER_PROXY_ROLE_INFO_HISTORY_TNAME, dml, affected_rows))) {
    LOG_WARN("exec insert failed", K(ret));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("affected_rows expected to be one", K(affected_rows), K(ret));
  }
  return ret;
}

int ObUserSqlService::drop_user_delete_proxy_user_info(const uint64_t tenant_id,
                                                        const bool is_role,
                                                        const uint64_t new_schema_version,
                                                        const ObUserInfo &user,
                                                        const ObString *ddl_stmt_str,
                                                        ObISQLClient &sql_client,
                                                        ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  common::ObArray<ObUserInfo> user_infos_to_update;
  if (is_role) { //drop role
    common::ObSEArray<uint64_t, 8> schema_id_array = user.get_grantee_id_array();
    for (int64_t i = 0; OB_SUCC(ret) && i < schema_id_array.count(); i++) {
      const ObUserInfo *grantee = NULL;
      if (OB_FAIL(schema_guard.get_user_info(tenant_id, schema_id_array.at(i), grantee))) {
        LOG_WARN("get user info failed", K(ret));
      } else if (OB_ISNULL(grantee)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error", K(ret));
      } else if (grantee->is_role()) {
        //do nothing
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < grantee->get_proxied_user_info_cnt(); j++) {
          const ObProxyInfo *proxy_info = grantee->get_proxied_user_info_by_idx(j);
          bool found = false;
          if (OB_ISNULL(proxy_info)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected error", K(ret));
          }
          for (int64_t k = 0; OB_SUCC(ret) && !found && k < proxy_info->role_id_cnt_; k++) {
            uint64_t role_id = proxy_info->get_role_id_by_idx(k);
            if (user.get_user_id() == proxy_info->get_role_id_by_idx(k)) {
              found = true;
              OZ (drop_proxy_role_info(sql_client, tenant_id, grantee->get_user_id(),
                                                  proxy_info->user_id_, user.get_user_id(), new_schema_version));
            }
            if (OB_SUCC(ret) && found) {
              const ObUserInfo *user_info = NULL;
              if (OB_FAIL(schema_guard.get_user_info(tenant_id, proxy_info->user_id_, user_info))) {
                LOG_WARN("get user info failed", K(ret));
              } else if (OB_ISNULL(user_info)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unexpected error", K(ret));
              } else if (OB_FAIL(user_infos_to_update.push_back(*user_info))) {
                LOG_WARN("push back failed", K(ret));
              }
            }
          }
        }
      }
    }
  } else { //drop user
    for (int64_t i = 0; OB_SUCC(ret) && i < user.get_proxy_user_info_cnt(); i++) {
      const ObProxyInfo *proxy_info = user.get_proxy_user_info_by_idx(i);
      if (OB_ISNULL(proxy_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error", K(ret));
      } else if (OB_FAIL(drop_proxy_info(sql_client, tenant_id, proxy_info->user_id_,
                                                                user.get_user_id(), new_schema_version))) {
        LOG_WARN("drop proxy info failed", K(ret));
      } else {
        const ObUserInfo *user_info = NULL;
        if (proxy_info->user_id_ == user.get_user_id()) { //u1 proxy u1, when droping u1, do not update u1
          //do nothing
        } else if (OB_FAIL(schema_guard.get_user_info(tenant_id, proxy_info->user_id_, user_info))) {
          LOG_WARN("get user info failed", K(ret));
        } else if (OB_ISNULL(user_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected error", K(ret));
        } else if (OB_FAIL(user_infos_to_update.push_back(*user_info))) {
          LOG_WARN("push back failed", K(ret));
        }
        for (int64_t j = 0; OB_SUCC(ret) && j < proxy_info->role_id_cnt_; j++) {
          if (OB_FAIL(drop_proxy_role_info(sql_client, tenant_id, proxy_info->user_id_, user.get_user_id(),
                                                          proxy_info->get_role_id_by_idx(j), new_schema_version))) {
            LOG_WARN("drop proxy role info failed", K(ret));
          }
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < user.get_proxied_user_info_cnt(); i++) {
      const ObProxyInfo *proxy_info = user.get_proxied_user_info_by_idx(i);
      if (OB_ISNULL(proxy_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error", K(ret));
      } else if (user.get_user_id() == proxy_info->user_id_) {
        //alread dropped before
      } else if (OB_FAIL(drop_proxy_info(sql_client, tenant_id, user.get_user_id(), proxy_info->user_id_,
                                                                 new_schema_version))) {
        LOG_WARN("drop proxy info failed", K(ret));
      } else {
        const ObUserInfo *user_info = NULL;
        if (proxy_info->user_id_ == user.get_user_id()) { //u1 proxy u1, when droping u1, do not update u1
          //do nothing
        } else if (OB_FAIL(schema_guard.get_user_info(tenant_id, proxy_info->user_id_, user_info))) {
          LOG_WARN("get user info failed", K(ret));
        } else if (OB_ISNULL(user_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected error", K(ret));
        } else if (OB_FAIL(user_infos_to_update.push_back(*user_info))) {
          LOG_WARN("push back failed", K(ret));
        }
        for (int64_t j = 0; OB_SUCC(ret) && j < proxy_info->role_id_cnt_; j++) {
          if (OB_FAIL(drop_proxy_role_info(sql_client, tenant_id, user.get_user_id(), proxy_info->user_id_,
                                                            proxy_info->get_role_id_by_idx(j), new_schema_version))) {
            LOG_WARN("drop proxy role info failed", K(ret));
          }
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (user_infos_to_update.empty()) {
  } else if (OB_FAIL(update_user_proxy_info(tenant_id,
                                                user_infos_to_update,
                                                NULL,
                                                sql_client))) {
    LOG_WARN("Failed to grant or revoke user", K(ret));
  }
  return ret;
}

int ObUserSqlService::drop_user(
    const uint64_t tenant_id,
    const uint64_t user_id,
    const uint64_t new_schema_version,
    const ObString *ddl_stmt_str,
    ObISQLClient &sql_client,
    ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  static const int64_t IS_DELETED = 1;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);

  if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == user_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid arguments", K(tenant_id), K(user_id), K(ret));
  } else {
    int64_t affected_rows = 0;
    ObDMLExecHelper exec(sql_client, exec_tenant_id);
    ObDMLSqlSplicer dml;
    if (OB_SUCC(ret)) {
      if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                                 exec_tenant_id, tenant_id)))
          || OB_FAIL(dml.add_pk_column("user_id", ObSchemaUtils::get_extract_schema_id(
                                                  exec_tenant_id, user_id)))
          || OB_FAIL(dml.add_gmt_modified())) {
        LOG_WARN("add column failed", K(ret));
      }
    }

    // delete from __all_user table
    if (FAILEDx(exec.exec_delete(OB_ALL_USER_TNAME, dml, affected_rows))) {
      LOG_WARN("execute sql failed", K(ret));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("affected_rows expect to 1, ", K(affected_rows), K(ret));
    }

    // mark delete __all_user_history
    if (OB_SUCC(ret)) {
      if (OB_FAIL(dml.add_pk_column("schema_version", new_schema_version))
          || OB_FAIL(dml.add_column("is_deleted", IS_DELETED))) {
        LOG_WARN("add column failed", K(ret));
      } else if (OB_FAIL(exec.exec_replace(OB_ALL_USER_HISTORY_TNAME, dml, affected_rows))) {
        LOG_WARN("execute sql failed", K(ret));
      } else if (!is_single_row(affected_rows)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected_rows expect to 1, ", K(affected_rows), K(ret));
      }
    }

    // log operation
    if (OB_SUCC(ret)) {
      ObSchemaOperation priv_operation;
      priv_operation.tenant_id_ = tenant_id;
      priv_operation.user_id_ = user_id;
      priv_operation.op_type_ = OB_DDL_DROP_USER;
      priv_operation.schema_version_ = new_schema_version;
      priv_operation.ddl_stmt_str_ = ddl_stmt_str ? *ddl_stmt_str : ObString();
      if (OB_FAIL(log_operation(priv_operation, sql_client))) {
        LOG_WARN("Failed to log operation", K(ret));
      }
    }
  }
  // deal with related <role, grantee>
  // 1. remove <role, grantee> of dropping user
  // 2. If dropping user is:
  //    1). role: update related grantees' schema version
  //    2). grantee: update related roles' schema version
  const ObUserInfo *user = NULL;
  lib::Worker::CompatMode cmp_mode = lib::Worker::CompatMode::INVALID;
  if (FAILEDx(schema_guard.get_user_info(tenant_id, user_id, user))) {
    LOG_WARN("failed to get user info", K(ret), K(tenant_id), K(user_id));
  } else if (NULL == user) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("user info is null", K(ret), K(user_id));
  } else if (OB_FAIL(schema_guard.get_tenant_compat_mode(tenant_id, cmp_mode))) {
    LOG_WARN("fail to get compat mode", K(ret));
  } else {
    const bool is_role = user->is_role() || (lib::Worker::CompatMode::MYSQL == cmp_mode);

    OZ (drop_user_delete_role_grantee_map(tenant_id, is_role, new_schema_version,
                                          user, ddl_stmt_str, sql_client, schema_guard));
    if (OB_SUCC(ret) && is_role) {
      OZ (drop_user_delete_role_grantee_map(tenant_id, false, new_schema_version,
                                            user, ddl_stmt_str, sql_client, schema_guard));
    }

    OZ (drop_user_delete_proxy_user_info(tenant_id, is_role, new_schema_version,
                                          *user, NULL, sql_client, schema_guard));
  }

  return ret;
}

int ObUserSqlService::rename_user(
    const ObUserInfo &user_info,
    const int64_t new_schema_version,
    const ObString *ddl_stmt_str,
    ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = user_info.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  const uint64_t user_id = user_info.get_user_id();
  const ObString &new_user_name = user_info.get_user_name_str();
  const ObString &new_host_name = user_info.get_host_name_str();
  ObSqlString sql_string;
  if (OB_INVALID_ID == tenant_id
     || OB_INVALID_ID == user_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(tenant_id), K(user_id), K(ret));
  } else {
    int64_t affected_rows = 0;
    ObDMLExecHelper exec(sql_client, exec_tenant_id);
    ObDMLSqlSplicer dml;
    if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                               exec_tenant_id, tenant_id)))
        || OB_FAIL(dml.add_pk_column("user_id", ObSchemaUtils::get_extract_schema_id(
                                                exec_tenant_id, user_id)))
        || OB_FAIL(dml.add_column("user_name", new_user_name))
        || OB_FAIL(dml.add_column("host", new_host_name))
        || OB_FAIL(dml.add_gmt_modified())) {
      LOG_WARN("add column failed", K(ret));
    }

    // udpate __all_user table
    if (FAILEDx(exec.exec_update(OB_ALL_USER_TNAME, dml, affected_rows))) {
      LOG_WARN("execute update sql fail", K(ret));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("update should affect only 1 row", K(affected_rows), K(ret));
    }

    // update __all_user history table
    if (FAILEDx(add_user_history(user_info, new_schema_version, sql_client, false))) {
      LOG_WARN("add_user_history failed", K(user_info), K(new_schema_version), K(ret));
    }

    // log operation
    if (OB_SUCC(ret)) {
      ObSchemaOperation priv_operation;
      priv_operation.schema_version_ = new_schema_version;
      priv_operation.tenant_id_ = tenant_id;
      priv_operation.user_id_ = user_id;
      priv_operation.op_type_ = OB_DDL_RENAME_USER;
      priv_operation.ddl_stmt_str_ = ddl_stmt_str ? *ddl_stmt_str : ObString();
      if (OB_FAIL(log_operation(priv_operation, sql_client))) {
        LOG_WARN("Failed to log operation", K(ret));
      }
    }
  }
  return ret;
}

int ObUserSqlService::alter_role(
    const ObUserInfo &user_info,
    const int64_t new_schema_version,
    const ObString *ddl_stmt_str,
    ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(set_passwd_impl(user_info, 
                              new_schema_version, 
                              ddl_stmt_str, 
                              sql_client, 
                              OB_DDL_ALTER_ROLE))) {
    LOG_WARN("fail to set_passwd_impl");
  }
  return ret;
}

int ObUserSqlService::set_passwd(
    const ObUserInfo &user_info,
    const int64_t new_schema_version,
    const ObString *ddl_stmt_str,
    ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(set_passwd_impl(user_info, 
                              new_schema_version, 
                              ddl_stmt_str, 
                              sql_client, 
                              OB_DDL_SET_PASSWD))) {
    LOG_WARN("fail to set_passwd_impl");
  }
  return ret;
}

int ObUserSqlService::set_passwd_impl(
    const ObUserInfo &user_info,
    const int64_t new_schema_version,
    const ObString *ddl_stmt_str,
    ObISQLClient &sql_client,
    const ObSchemaOperationType type)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = user_info.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  const uint64_t user_id = user_info.get_user_id();
  ObSqlString sql_string;
  if (OB_INVALID_ID == tenant_id
     || OB_INVALID_ID == user_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid id", K(tenant_id), K(user_id), K(ret));
  } else {
    int64_t affected_rows = 0;
    ObDMLExecHelper exec(sql_client, exec_tenant_id);
    ObDMLSqlSplicer dml;
    if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                               exec_tenant_id, tenant_id)))
        || OB_FAIL(dml.add_pk_column("user_id", ObSchemaUtils::get_extract_schema_id(
                                                exec_tenant_id, user_id)))
        || OB_FAIL(dml.add_column("passwd", user_info.get_passwd()))
        || OB_FAIL(dml.add_time_column("password_last_changed",
                                      user_info.get_password_last_changed()))
        || OB_FAIL(dml.add_gmt_modified())) {
      LOG_WARN("add column failed", K(ret));
    }

    // udpate __all_user table
    if (FAILEDx(exec.exec_update(OB_ALL_USER_TNAME, dml, affected_rows))) {
      LOG_WARN("execute update sql fail", K(ret));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("update should affect only 1 row", K(affected_rows), K(ret));
    }

    // update __all_user history table
    if (FAILEDx(add_user_history(user_info, new_schema_version, sql_client, false))) {
      LOG_WARN("add_user_history failed", K(user_info), K(new_schema_version), K(ret));
    }

    // log operation
    if (OB_SUCC(ret)) {
      ObSchemaOperation priv_operation;
      priv_operation.schema_version_ = new_schema_version;
      priv_operation.tenant_id_ = tenant_id;
      priv_operation.user_id_ = user_id;
      priv_operation.op_type_ = type;
      priv_operation.ddl_stmt_str_ = ddl_stmt_str ? *ddl_stmt_str : ObString();
      if (OB_FAIL(log_operation(priv_operation, sql_client))) {
        LOG_WARN("Failed to log operation", K(ret));
      }
    }
  }

  return ret;
}

int ObUserSqlService::set_max_connections(
    const ObUserInfo &user_info,
    const int64_t new_schema_version,
    const ObString *ddl_stmt_str,
    ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = user_info.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  const uint64_t user_id = user_info.get_user_id();
  ObSqlString sql_string;
  if (OB_INVALID_ID == tenant_id
     || OB_INVALID_ID == user_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid id", K(tenant_id), K(user_id), K(ret));
  } else {
    int64_t affected_rows = 0;
    ObDMLExecHelper exec(sql_client, exec_tenant_id);
    ObDMLSqlSplicer dml;
    if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                               exec_tenant_id, tenant_id)))
        || OB_FAIL(dml.add_pk_column("user_id", ObSchemaUtils::get_extract_schema_id(
                                                exec_tenant_id, user_id)))
        || OB_FAIL(dml.add_column("max_connections", user_info.get_max_connections()))
        || OB_FAIL(dml.add_column("max_user_connections", user_info.get_max_user_connections()))
        || OB_FAIL(dml.add_gmt_modified())) {
      LOG_WARN("add column failed", K(ret));
    }

    // udpate __all_user table
    if (FAILEDx(exec.exec_update(OB_ALL_USER_TNAME, dml, affected_rows))) {
      LOG_WARN("execute update sql fail", K(ret));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("update should affect only 1 row", K(affected_rows), K(ret));
    }

    // update __all_user history table
    if (FAILEDx(add_user_history(user_info, new_schema_version, sql_client, false))) {
      LOG_WARN("add_user_history failed", K(user_info), K(new_schema_version), K(ret));
    }

    // log operation
    if (OB_SUCC(ret)) {
      ObSchemaOperation priv_operation;
      priv_operation.schema_version_ = new_schema_version;
      priv_operation.tenant_id_ = tenant_id;
      priv_operation.user_id_ = user_id;
      priv_operation.op_type_ = OB_DDL_ALTER_USER;
      priv_operation.ddl_stmt_str_ = ddl_stmt_str ? *ddl_stmt_str : ObString();
      if (OB_FAIL(log_operation(priv_operation, sql_client))) {
        LOG_WARN("Failed to log operation", K(ret));
      }
    }
  }

  return ret;
}

int ObUserSqlService::alter_user_require(
    const ObUserInfo &user_info,
    const int64_t new_schema_version,
    const ObString *ddl_stmt_str,
    ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = user_info.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  const uint64_t user_id = user_info.get_user_id();
  ObSqlString sql_string;
  if (OB_INVALID_ID == tenant_id
     || OB_INVALID_ID == user_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid id", K(tenant_id), K(user_id), K(ret));
  } else {
    int64_t affected_rows = 0;
    ObDMLExecHelper exec(sql_client, exec_tenant_id);
    ObDMLSqlSplicer dml;
    if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                               exec_tenant_id, tenant_id)))
        || OB_FAIL(dml.add_pk_column("user_id", ObSchemaUtils::get_extract_schema_id(
                                                exec_tenant_id, user_id)))
        || OB_FAIL(dml.add_column("ssl_type", user_info.get_ssl_type()))
        || OB_FAIL(dml.add_column("ssl_cipher", user_info.get_ssl_cipher()))
        || OB_FAIL(dml.add_column("x509_issuer", user_info.get_x509_issuer()))
        || OB_FAIL(dml.add_column("x509_subject", user_info.get_x509_subject()))
        || OB_FAIL(dml.add_gmt_modified())) {
      LOG_WARN("add column failed", K(ret));
    }

    // udpate __all_user table
    if (FAILEDx(exec.exec_update(OB_ALL_USER_TNAME, dml, affected_rows))) {
      LOG_WARN("execute update sql fail", K(ret));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("update should affect only 1 row", K(affected_rows), K(ret));
    }

    // update __all_user history table
    if (FAILEDx(add_user_history(user_info, new_schema_version, sql_client, false))) {
      LOG_WARN("add_user_history failed", K(user_info), K(new_schema_version), K(ret));
    }

    // log operation
    if (OB_SUCC(ret)) {
      ObSchemaOperation priv_operation;
      priv_operation.schema_version_ = new_schema_version;
      priv_operation.tenant_id_ = tenant_id;
      priv_operation.user_id_ = user_id;
      priv_operation.op_type_ = OB_DDL_ALTER_USER_REQUIRE;
      priv_operation.ddl_stmt_str_ = ddl_stmt_str ? *ddl_stmt_str : ObString();
      if (OB_FAIL(log_operation(priv_operation, sql_client))) {
        LOG_WARN("Failed to log operation", K(ret));
      }
    }
  }

  return ret;
}

int ObUserSqlService::grant_revoke_user(
    const ObUserInfo &user_info,
    const int64_t new_schema_version,
    const ObString *ddl_stmt_str,
    ObISQLClient &sql_client,
    const bool is_from_inner_sql)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = user_info.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  const uint64_t user_id = user_info.get_user_id();
  ObSqlString sql_string;
  if (OB_INVALID_ID == tenant_id
     || OB_INVALID_ID == user_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid arguments", K(tenant_id), K(user_id), K(ret));
  } else {
    int64_t affected_rows = 0;
    ObDMLExecHelper exec(sql_client, exec_tenant_id);
    ObDMLSqlSplicer dml;
    if (OB_FAIL(gen_user_dml(exec_tenant_id, user_info, dml, is_from_inner_sql))) {
      LOG_WARN("gen_user_dml failed", K(user_info), K(ret));
    }

    // insert into __all_user
    if (FAILEDx(exec.exec_update(OB_ALL_USER_TNAME, dml, affected_rows))) {
      LOG_WARN("execute insert failed", K(ret));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("affected_rows unexpected", K(affected_rows), K(ret));
    }

    // insert into __all_user_history
    if (FAILEDx(add_user_history(user_info, new_schema_version, sql_client, is_from_inner_sql))) {
      LOG_WARN("add_user_history failed", K(user_info), K(new_schema_version), K(ret));
    }

    // log operation
    if (OB_SUCC(ret)) {
      ObSchemaOperation priv_operation;
      priv_operation.schema_version_ = new_schema_version;
      priv_operation.tenant_id_ = tenant_id;
      priv_operation.user_id_ = user_id;
      priv_operation.op_type_ = OB_DDL_GRANT_REVOKE_USER;
      priv_operation.ddl_stmt_str_ = ddl_stmt_str ? *ddl_stmt_str : ObString();
      if (OB_FAIL(log_operation(priv_operation, sql_client))) {
        LOG_WARN("Failed to log operation", K(ret));
      }
    }
  }

  return ret;
}

int ObUserSqlService::alter_user_profile(
    const ObUserInfo &user_info,
    const ObString *ddl_stmt_str,
    ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = user_info.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  const uint64_t user_id = user_info.get_user_id();
  const uint64_t profile_id = user_info.get_profile_id();

  if (!is_valid_tenant_id(tenant_id)
      || !is_valid_id(user_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid arguments", K(tenant_id), K(user_id), K(ret));
  } else {
    int64_t affected_rows = 0;
    ObDMLExecHelper exec(sql_client, exec_tenant_id);
    ObDMLSqlSplicer dml;
    if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                               exec_tenant_id, tenant_id)))
        || OB_FAIL(dml.add_pk_column("user_id", ObSchemaUtils::get_extract_schema_id(
                                                exec_tenant_id, user_id)))
        || OB_FAIL(dml.add_column("profile_id", ObSchemaUtils::get_extract_schema_id(
                                                exec_tenant_id, profile_id)))
        || OB_FAIL(dml.add_gmt_modified())) {
      LOG_WARN("add column failed", K(ret));
    }

    // udpate __all_user table
    if (FAILEDx(exec.exec_update(OB_ALL_USER_TNAME, dml, affected_rows))) {
      LOG_WARN("execute update sql fail", K(ret));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("update should affect only 1 row", K(affected_rows), K(ret));
    }

    // update __all_user history table
    if (FAILEDx(add_user_history(user_info, user_info.get_schema_version(), sql_client, false))) {
      LOG_WARN("add_user_history failed", K(user_info), K(ret));
    }

    // log operation
    if (OB_SUCC(ret)) {
      ObSchemaOperation priv_operation;
      priv_operation.schema_version_ = user_info.get_schema_version();
      priv_operation.tenant_id_ = tenant_id;
      priv_operation.user_id_ = user_id;
      priv_operation.op_type_ = OB_DDL_ALTER_USER_PROFILE;
      priv_operation.ddl_stmt_str_ = ddl_stmt_str ? *ddl_stmt_str : ObString();
      if (OB_FAIL(log_operation(priv_operation, sql_client))) {
        LOG_WARN("Failed to log operation", K(ret));
      }
    }
  }

  return ret;
}

int ObUserSqlService::lock_user(
    const ObUserInfo &user_info,
    const int64_t new_schema_version,
    const ObString *ddl_stmt_str,
    ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = user_info.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  const uint64_t user_id = user_info.get_user_id();
  const bool locked = user_info.get_is_locked();

  if (OB_INVALID_ID == tenant_id
      || OB_INVALID_ID == user_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid arguments", K(tenant_id), K(user_id), K(ret));
  } else {
    int64_t affected_rows = 0;
    ObDMLExecHelper exec(sql_client, exec_tenant_id);
    ObDMLSqlSplicer dml;
    if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                               exec_tenant_id, tenant_id)))
        || OB_FAIL(dml.add_pk_column("user_id", ObSchemaUtils::get_extract_schema_id(
                                                exec_tenant_id, user_id)))
        || OB_FAIL(dml.add_column("is_locked", locked))
        || OB_FAIL(dml.add_gmt_modified())) {
      LOG_WARN("add column failed", K(ret));
    }

    // udpate __all_user table
    if (FAILEDx(exec.exec_update(OB_ALL_USER_TNAME, dml, affected_rows))) {
      LOG_WARN("execute update sql fail", K(ret));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("update should affect only 1 row", K(affected_rows), K(ret));
    }

    // update __all_user history table
    if (FAILEDx(add_user_history(user_info, new_schema_version, sql_client, false))) {
      LOG_WARN("add_user_history failed", K(user_info), K(new_schema_version), K(ret));
    }

    // log operation
    if (OB_SUCC(ret)) {
      ObSchemaOperation priv_operation;
      priv_operation.schema_version_ = new_schema_version;
      priv_operation.tenant_id_ = tenant_id;
      priv_operation.user_id_ = user_id;
      priv_operation.op_type_ = OB_DDL_LOCK_USER;
      priv_operation.ddl_stmt_str_ = ddl_stmt_str ? *ddl_stmt_str : ObString();
      if (OB_FAIL(log_operation(priv_operation, sql_client))) {
        LOG_WARN("Failed to log operation", K(ret));
      }
    }
  }

  return ret;
}

int ObUserSqlService::add_user_history(
    const ObUserInfo &user_info,
    const int64_t schema_version,
    common::ObISQLClient &sql_client,
    const bool is_from_inner_sql)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = user_info.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  ObDMLExecHelper exec(sql_client, exec_tenant_id);
  ObDMLSqlSplicer dml;
  int64_t affected_rows = 0;
  if (OB_FAIL(gen_user_dml(exec_tenant_id, user_info, dml, is_from_inner_sql))) {
    LOG_WARN("gen_user_dml failed", K(user_info), K(ret));
  } else {
    const int64_t is_deleted = 0;
    if (OB_FAIL(dml.add_pk_column("schema_version", schema_version))
        || OB_FAIL(dml.add_column("is_deleted", is_deleted))) {
      LOG_WARN("add column failed", K(ret));
    } else if (OB_FAIL(exec.exec_replace(OB_ALL_USER_HISTORY_TNAME, dml, affected_rows))) {
      LOG_WARN("execute update sql fail", K(ret));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("update should affect only 1 row", K(affected_rows), K(ret));
    }
  }
  return ret;
}

int ObUserSqlService::gen_user_dml(
    const uint64_t exec_tenant_id,
    const ObUserInfo &user,
    ObDMLSqlSplicer &dml,
    const bool is_from_inner_sql)
{
  int ret = OB_SUCCESS;
  const bool is_ssl_support = (user.get_ssl_type() != ObSSLType::SSL_TYPE_NOT_SPECIFIED);
  LOG_INFO("gen_user_dml", K(is_ssl_support), K(user), K(is_from_inner_sql));
  uint64_t compat_version = 0;
  bool is_oracle_mode = false;
  if (OB_FAIL(GET_MIN_DATA_VERSION(user.get_tenant_id(), compat_version))) {
    LOG_WARN("fail to get data version", KR(ret), K(user.get_tenant_id()));
  } else if (OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_tenant_id(user.get_tenant_id(), is_oracle_mode))) {
    LOG_WARN("fail to check is oracle mode", K(ret));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                             exec_tenant_id, user.get_tenant_id())))
      || OB_FAIL(dml.add_pk_column("user_id", ObSchemaUtils::get_extract_schema_id(
                                              exec_tenant_id,user.get_user_id())))
      || OB_FAIL(dml.add_column("user_name", ObHexEscapeSqlStr(user.get_user_name())))
      || OB_FAIL(dml.add_column("host", ObHexEscapeSqlStr(user.get_host_name())))
      || OB_FAIL(dml.add_column("passwd", user.get_passwd()))
      || OB_FAIL(dml.add_column("info", user.get_info()))
      || OB_FAIL(dml.add_column("PRIV_ALTER", user.get_priv(OB_PRIV_ALTER) ? 1 : 0))
      || OB_FAIL(dml.add_column("PRIV_CREATE", user.get_priv(OB_PRIV_CREATE) ? 1 : 0))
      || OB_FAIL(dml.add_column("PRIV_CREATE_USER", user.get_priv(OB_PRIV_CREATE_USER) ? 1 : 0))
      || OB_FAIL(dml.add_column("PRIV_DELETE", user.get_priv(OB_PRIV_DELETE) ? 1 : 0))
      || OB_FAIL(dml.add_column("PRIV_DROP", user.get_priv(OB_PRIV_DROP) ? 1 : 0))
      || OB_FAIL(dml.add_column("PRIV_GRANT_OPTION", user.get_priv(OB_PRIV_GRANT) ? 1 : 0))
      || OB_FAIL(dml.add_column("PRIV_INSERT", user.get_priv(OB_PRIV_INSERT) ? 1 : 0))
      || OB_FAIL(dml.add_column("PRIV_UPDATE", user.get_priv(OB_PRIV_UPDATE) ? 1 : 0))
      || OB_FAIL(dml.add_column("PRIV_SELECT", user.get_priv(OB_PRIV_SELECT) ? 1 : 0))
      || OB_FAIL(dml.add_column("PRIV_INDEX", user.get_priv(OB_PRIV_INDEX) ? 1 : 0))
      || OB_FAIL(dml.add_column("PRIV_CREATE_VIEW", user.get_priv(OB_PRIV_CREATE_VIEW) ? 1 : 0))
      || OB_FAIL(dml.add_column("PRIV_SHOW_VIEW", user.get_priv(OB_PRIV_SHOW_VIEW) ? 1 : 0))
      || OB_FAIL(dml.add_column("PRIV_SHOW_DB", user.get_priv(OB_PRIV_SHOW_DB) ? 1 : 0))
      || OB_FAIL(dml.add_column("PRIV_SUPER", user.get_priv(OB_PRIV_SUPER) ? 1 : 0))
      || OB_FAIL(dml.add_column("PRIV_PROCESS", user.get_priv(OB_PRIV_PROCESS) ? 1 : 0))
      || OB_FAIL(dml.add_column("IS_LOCKED", user.get_is_locked() ? 1 : 0))
      || OB_FAIL(dml.add_column("PRIV_CREATE_SYNONYM", user.get_priv(OB_PRIV_CREATE_SYNONYM) ? 1 : 0))
      || OB_FAIL(dml.add_column("PRIV_FILE", user.get_priv(OB_PRIV_FILE) ? 1 : 0))
      || OB_FAIL(dml.add_column("PRIV_ALTER_TENANT", user.get_priv(OB_PRIV_ALTER_TENANT) ? 1 : 0))
      || OB_FAIL(dml.add_column("PRIV_ALTER_SYSTEM", user.get_priv(OB_PRIV_ALTER_SYSTEM) ? 1 : 0))
      || OB_FAIL(dml.add_column("PRIV_CREATE_RESOURCE_POOL", 
                                user.get_priv(OB_PRIV_CREATE_RESOURCE_POOL) ? 1 : 0))
      || OB_FAIL(dml.add_column("PRIV_CREATE_RESOURCE_UNIT", 
                                user.get_priv(OB_PRIV_CREATE_RESOURCE_UNIT) ? 1 : 0))
      || OB_FAIL(dml.add_column("max_connections", user.get_max_connections()))
      || OB_FAIL(dml.add_column("max_user_connections", user.get_max_user_connections()))
      || OB_FAIL(dml.add_column("PRIV_REPL_SLAVE", user.get_priv(OB_PRIV_REPL_SLAVE) ? 1 : 0))
      || OB_FAIL(dml.add_column("PRIV_REPL_CLIENT", user.get_priv(OB_PRIV_REPL_CLIENT) ? 1 : 0))
      || (is_ssl_support && OB_FAIL(dml.add_column("SSL_TYPE", user.get_ssl_type())))
      || (is_ssl_support && OB_FAIL(dml.add_column("SSL_CIPHER", user.get_ssl_cipher())))
      || (is_ssl_support && OB_FAIL(dml.add_column("X509_ISSUER", user.get_x509_issuer())))
      || (is_ssl_support && OB_FAIL(dml.add_column("X509_SUBJECT", user.get_x509_subject())))
      || OB_FAIL(dml.add_column("TYPE", user.is_role() ? 1 : 0))
      || OB_FAIL(dml.add_column("profile_id", ObSchemaUtils::get_extract_schema_id(
                                              exec_tenant_id, user.get_profile_id())))
      || OB_FAIL(dml.add_time_column("password_last_changed", user.get_password_last_changed()))
      || OB_FAIL(dml.add_gmt_modified())) {
    LOG_WARN("add column failed", K(ret));
  } else if (!is_from_inner_sql && compat_version < DATA_VERSION_4_2_0_0) {
    if (1 == user.get_priv(OB_PRIV_DROP_DATABASE_LINK) ||
        1 == user.get_priv(OB_PRIV_CREATE_DATABASE_LINK)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("some column of user info is not empty when MIN_DATA_VERSION is below DATA_VERSION_4_2_0_0", K(ret), K(user.get_priv(OB_PRIV_DROP_DATABASE_LINK)), K(user.get_priv(OB_PRIV_CREATE_DATABASE_LINK)));
    }
  } else if (OB_FAIL(dml.add_column("PRIV_DROP_DATABASE_LINK", user.get_priv(OB_PRIV_DROP_DATABASE_LINK) ? 1 : 0))) {
    LOG_WARN("add  PRIV_DROP_DATABASE_LINK column failed", K(user.get_priv(OB_PRIV_DROP_DATABASE_LINK)), K(ret));
  } else if (OB_FAIL(dml.add_column("PRIV_CREATE_DATABASE_LINK", user.get_priv(OB_PRIV_CREATE_DATABASE_LINK) ? 1 : 0))) {
    LOG_WARN("add  PRIV_CREATE_DATABASE_LINK column failed", K(user.get_priv(OB_PRIV_CREATE_DATABASE_LINK)), K(ret));
  }
  int64_t priv_others = 0;
  if (OB_SUCC(ret)) {
    if ((user.get_priv_set() & OB_PRIV_EXECUTE) != 0) { priv_others |= 1; }
    if ((user.get_priv_set() & OB_PRIV_ALTER_ROUTINE) != 0) { priv_others |= 2; }
    if ((user.get_priv_set() & OB_PRIV_CREATE_ROUTINE) != 0) { priv_others |= 4; }
    if ((user.get_priv_set() & OB_PRIV_CREATE_TABLESPACE) != 0) { priv_others |= 8; }
    if ((user.get_priv_set() & OB_PRIV_SHUTDOWN) != 0) { priv_others |= 16; }
    if ((user.get_priv_set() & OB_PRIV_RELOAD) != 0) { priv_others |= 32; }
  }
  if (OB_FAIL(ret)) {
  } else if (!sql::ObSQLUtils::is_data_version_ge_422_or_431(compat_version)) {
    if (priv_others != 0) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("some column of user info is not empty when MIN_DATA_VERSION is below DATA_VERSION_4_3_1_0 or DATA_VERSION_4_2_2_0", K(ret), K(user.get_priv(OB_PRIV_EXECUTE)), K(user.get_priv(OB_PRIV_ALTER_ROUTINE)), K(user.get_priv(OB_PRIV_CREATE_ROUTINE)));
    }
  } else if (OB_FAIL(dml.add_column("PRIV_OTHERS", priv_others))) {
    LOG_WARN("add PRIV_OTHERS column failed", K(priv_others), K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (!sql::ObSQLUtils::is_data_version_ge_423_or_432(compat_version)) {
    if (user.get_flags() != 0) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("some column of user info is not empty when MIN_DATA_VERSION is below DATA_VERSION_4_2_3_0 or 4_3_2_0", K(ret), K(user.get_flags()));
    }
  } else if (OB_FAIL(dml.add_column("flags", user.get_flags()))) {
    LOG_WARN("add flags column failed", K(user.get_flags()), K(ret));
  }
  return ret;
}

int ObUserSqlService::update_user_proxy_info(const uint64_t tenant_id,
                                              const common::ObArray<ObUserInfo> &user_infos,
                                              const ObString *ddl_stmt_str,
                                              ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  int64_t new_schema_version = OB_INVALID_VERSION;
  if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid arguments", K(tenant_id), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < user_infos.count(); i++) {
      const ObUserInfo &user_info = user_infos.at(i);
      if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, OB_INVALID_VERSION, new_schema_version))) {
        LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
      } else if (OB_FAIL(replace_user(user_info, new_schema_version, ddl_stmt_str, sql_client, OB_DDL_GRANT_REVOKE_USER))) {
        LOG_WARN("update user failed failed", K(user_info), K(new_schema_version), K(ret));
      } else {
        ddl_stmt_str = NULL;
      }
    }
  }

  return ret;
}

int ObUserSqlService::update_user_schema_version(
    const uint64_t tenant_id,
    const common::ObArray<ObUserInfo> &user_infos,
    const ObString *ddl_stmt_str,
    ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  UNUSED(ddl_stmt_str);
  if (OB_INVALID_ID == tenant_id
      || user_infos.count() < 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid arguments", K(tenant_id), K(ret));
  } else {
    // update __all_user history table
    for (int64_t i = 0; OB_SUCC(ret) && i < user_infos.count(); i++) {
      int64_t new_schema_version = OB_INVALID_VERSION;
      const ObUserInfo &user_info = user_infos.at(i);
      if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, OB_INVALID_VERSION, new_schema_version))) {
        LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
      } else if (OB_FAIL(add_user_history(user_info, new_schema_version, sql_client, false))) {
        LOG_WARN("add_user_history failed", K(user_info), K(new_schema_version), K(ret));
      }
      // log operation
      if (OB_SUCC(ret)) {
        ObSchemaOperation priv_operation;
        priv_operation.schema_version_ = new_schema_version;
        priv_operation.tenant_id_ = tenant_id;
        priv_operation.user_id_ = user_info.get_user_id();
        priv_operation.op_type_ = OB_DDL_MODIFY_USER_SCHEMA_VERSION;
        //priv_operation.ddl_stmt_str_ = ddl_stmt_str ? *ddl_stmt_str : ObString();
        priv_operation.ddl_stmt_str_ = ObString();
        if (OB_FAIL(log_operation(priv_operation, sql_client))) {
          LOG_WARN("Failed to log operation", K(ret));
        }
      }
    }
  }

  return ret;
}


} //end of schema
} //end of share
} //end of oceanbase
