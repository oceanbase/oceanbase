/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX RS

#include "rootserver/ob_password_ddl_service.h"

#include "lib/utility/ob_tracepoint.h"
#include "rootserver/ob_ddl_sql_generator.h"
#include "rootserver/ob_password_ddl_operator.h"

namespace oceanbase
{
using namespace common;
using namespace obrpc;
using namespace share;
using namespace share::schema;
namespace rootserver
{

ObPasswordDDLService::ObPasswordDDLService(ObDDLService *ddl_service) : ddl_service_(ddl_service) {}

ObPasswordDDLService::~ObPasswordDDLService() {}

int ObPasswordDDLService::set_passwd(const ObSetPasswdArg &arg)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = arg.tenant_id_;
  uint64_t user_id = OB_INVALID_ID;
  const ObString &user_name = arg.user_;
  const ObString &host_name = arg.host_;
  const ObString &passwd = arg.passwd_;
  const bool modify_max_connections = arg.modify_max_connections_;
  const uint64_t max_connections_per_hour = arg.max_connections_per_hour_;
  const uint64_t max_user_connections = arg.max_user_connections_;
  const ObSSLType ssl_type = arg.ssl_type_;
  const ObString &plugin = arg.plugin_;
  const bool retain_current_password = arg.retain_current_password_;
  const bool discard_old_password = arg.discard_old_password_;
  const ObPasswordExpireOption password_expire_option = arg.password_expire_option_;
  const int64_t password_lifetime_us = arg.password_lifetime_us_;
  const int64_t password_last_changed = arg.password_last_changed_;
  ObSchemaGetterGuard schema_guard;
  bool is_oracle_mode = false;
  if (OB_ISNULL(ddl_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ddl service is null", K(ret));
  } else if (OB_FAIL(ddl_service_->check_inner_stat())) {
    LOG_WARN("ddl service is not initialized", K(ret));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid set password argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_tenant_id(tenant_id, is_oracle_mode))) {
    LOG_WARN("failed to check oracle mode", K(ret), K(tenant_id));
  } else if (OB_FAIL(ddl_service_->get_tenant_schema_guard_with_version_in_inner_table(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret), K(tenant_id));
  } else {
    ObSqlString ddl_stmt_str;
    ObString ddl_sql;
    if (OB_FAIL(schema_guard.get_user_id(tenant_id, user_name, host_name, user_id))) {
      LOG_WARN("failed to get user id", K(ret), K(tenant_id));
    } else if (OB_INVALID_ID == user_id) {
      ret = OB_USER_NOT_EXIST;
      LOG_WARN("user does not exist", K(ret), K(tenant_id));
    } else if (ObSSLType::SSL_TYPE_NOT_SPECIFIED == ssl_type) {
      if (modify_max_connections) {
        if (OB_FAIL(ObDDLSqlGenerator::gen_set_max_connections_sql(ObAccountArg(user_name, host_name),
                                                                   max_connections_per_hour,
                                                                   max_user_connections,
                                                                   ddl_stmt_str,
                                                                   is_oracle_mode))) {
          LOG_WARN("generate set max connections sql failed", K(ret), K(tenant_id), K(user_id));
        } else if (FALSE_IT(ddl_sql = ddl_stmt_str.string())) {
        } else if (OB_FAIL(ddl_service_->set_max_connection_in_trans(tenant_id,
                                                                     user_id,
                                                                     max_connections_per_hour,
                                                                     max_user_connections,
                                                                     &ddl_sql,
                                                                     schema_guard))) {
          LOG_WARN("set max connections failed", K(ret), K(tenant_id), K(user_id));
        }
      } else {
        if (OB_FAIL(ObDDLSqlGenerator::gen_set_passwd_sql(ObAccountArg(user_name, host_name),
                                                          passwd,
                                                          ddl_stmt_str,
                                                          plugin,
                                                          is_oracle_mode,
                                                          retain_current_password,
                                                          discard_old_password,
                                                          arg.modify_password_,
                                                          password_expire_option,
                                                          password_lifetime_us))) {
          LOG_WARN("generate set password sql failed", K(ret), K(tenant_id), K(user_id));
        } else if (FALSE_IT(ddl_sql = ddl_stmt_str.string())) {
        } else if (OB_FAIL(set_passwd_in_trans(tenant_id,
                                               user_id,
                                               passwd,
                                               &ddl_sql,
                                               schema_guard,
                                               plugin,
                                               retain_current_password,
                                               discard_old_password,
                                               password_expire_option,
                                               password_lifetime_us,
                                               password_last_changed))) {
          LOG_WARN("set password failed", K(ret), K(tenant_id), K(user_id));
        }
      }
    } else {
      if (OB_FAIL(ObDDLSqlGenerator::gen_alter_user_require_sql(ObAccountArg(user_name, host_name), arg, ddl_stmt_str))) {
        LOG_WARN("generate alter user require sql failed", K(ret), K(tenant_id), K(user_id));
      } else if (FALSE_IT(ddl_sql = ddl_stmt_str.string())) {
      } else if (OB_FAIL(ddl_service_->alter_user_require_in_trans(tenant_id, user_id, arg, &ddl_sql, schema_guard))) {
        LOG_WARN("alter user require failed", K(ret), K(tenant_id), K(user_id));
      }
    }
  }
  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_PASSWORD_DDL_BEFORE_COMMIT);
int ObPasswordDDLService::set_passwd_in_trans(const uint64_t tenant_id,
                                              const uint64_t user_id,
                                              const ObString &new_passwd,
                                              const ObString *ddl_stmt_str,
                                              ObSchemaGetterGuard &schema_guard,
                                              const ObString &plugin,
                                              const bool retain_current_password,
                                              const bool discard_old_password,
                                              const ObPasswordExpireOption password_expire_option,
                                              const int64_t password_lifetime_us,
                                              const int64_t password_last_changed)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ddl_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ddl service is null", K(ret));
  } else if (OB_FAIL(ddl_service_->check_inner_stat())) {
    LOG_WARN("ddl service is not initialized", K(ret));
  } else {
    ObDDLSQLTransaction trans(&ddl_service_->get_schema_service());
    if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == user_id) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("tenant id or user id is invalid", K(ret), K(tenant_id), K(user_id));
    } else if (OB_FAIL(ddl_service_->get_tenant_schema_guard_with_version_in_inner_table(tenant_id, schema_guard))) {
      LOG_WARN("failed to get schema guard with version in inner table", K(ret), K(tenant_id));
    } else {
      int64_t refreshed_schema_version = 0;
      if (OB_FAIL(schema_guard.get_schema_version(tenant_id, refreshed_schema_version))) {
        LOG_WARN("failed to get tenant schema version", K(ret), K(tenant_id));
      } else if (OB_FAIL(trans.start(&ddl_service_->get_sql_proxy(), tenant_id, refreshed_schema_version))) {
        LOG_WARN("start transaction failed", K(ret), K(tenant_id), K(refreshed_schema_version));
      } else {
        ObPasswordDDLOperator ddl_operator(ddl_service_->get_schema_service());
        if (OB_FAIL(ddl_operator.set_passwd(tenant_id,
                                            user_id,
                                            new_passwd,
                                            ddl_stmt_str,
                                            trans,
                                            plugin,
                                            retain_current_password,
                                            discard_old_password,
                                            password_expire_option,
                                            password_lifetime_us,
                                            password_last_changed))) {
          LOG_WARN("set password failed", K(ret), K(tenant_id), K(user_id));
        }
#ifdef ERRSIM
        if (OB_SUCC(ret) && OB_FAIL(EVENT_CALL(ERRSIM_PASSWORD_DDL_BEFORE_COMMIT))) {
          LOG_WARN("injected password DDL failure before commit", KR(ret), K(tenant_id));
        }
#endif
        if (trans.is_started()) {
          int temp_ret = OB_SUCCESS;
          if (OB_SUCCESS != (temp_ret = trans.end(OB_SUCC(ret)))) {
            LOG_WARN("end transaction failed", K(temp_ret), "is_commit", OB_SUCCESS == ret);
            ret = OB_SUCC(ret) ? temp_ret : ret;
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ddl_service_->publish_schema(tenant_id))) {
        LOG_WARN("publish schema failed", K(ret), K(tenant_id));
      }
    }
  }
  return ret;
}

} // end namespace rootserver
} // end namespace oceanbase
