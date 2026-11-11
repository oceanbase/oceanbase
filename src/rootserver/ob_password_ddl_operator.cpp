/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX RS

#include "rootserver/ob_password_ddl_operator.h"

#include "lib/encrypt/ob_encrypted_helper.h"
#include "lib/time/ob_time_utility.h"
#include "share/schema/ob_schema_service_sql_impl.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;
namespace rootserver
{

ObPasswordDDLOperator::ObPasswordDDLOperator(ObMultiVersionSchemaService &schema_service)
    : schema_service_(schema_service)
{}

ObPasswordDDLOperator::~ObPasswordDDLOperator() {}

int ObPasswordDDLOperator::calc_old_password_info(const bool discard_old_password,
                                                  const bool retain_current_password,
                                                  const ObString &plugin,
                                                  const ObUserInfo *user_info,
                                                  ObString &old_password,
                                                  int64_t &old_password_start_time)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(user_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("user info is null", K(ret));
  } else {
    const ObString &current_passwd = user_info->get_passwd_str();
    const int64_t current_ts = ObTimeUtility::current_time();
    old_password = user_info->get_old_password_str();
    old_password_start_time = user_info->get_old_password_start_time();
    if (discard_old_password) {
      old_password = ObString::make_empty_string();
      old_password_start_time = OB_INVALID_TIMESTAMP;
    } else if (retain_current_password) {
      old_password = current_passwd;
      old_password_start_time = current_ts;
    } else if (!ObEncryptedHelper::is_same_auth_plugin(plugin, user_info->get_plugin())) {
      // Discard the old password when the authentication plugin changes.
      old_password = ObString::make_empty_string();
      old_password_start_time = OB_INVALID_TIMESTAMP;
    } else {
      old_password = user_info->get_old_password_str();
      old_password_start_time = user_info->get_old_password_start_time();
    }
  }
  return ret;
}

int ObPasswordDDLOperator::set_passwd(const uint64_t tenant_id,
                                      const uint64_t user_id,
                                      const ObString &passwd,
                                      const ObString *ddl_stmt_str,
                                      ObMySQLTransaction &trans,
                                      const ObString &plugin,
                                      const bool retain_current_password,
                                      const bool discard_old_password,
                                      const ObPasswordExpireOption password_expire_option,
                                      int64_t password_lifetime_us,
                                      const int64_t password_last_changed)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  ObSchemaService *schema_sql_service = schema_service_.get_schema_service();
  if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == user_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id and user_id must not be null", K(tenant_id), K(user_id), K(ret));
  } else if (OB_ISNULL(schema_sql_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema service must not be null", KP(schema_sql_service), K(ret));
  } else if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret), K(tenant_id));
  } else {
    const ObUserInfo *user_info = nullptr;
    if (OB_FAIL(schema_guard.get_user_info(tenant_id, user_id, user_info))) {
      LOG_WARN("failed to get user info", K(ret), K(tenant_id), K(user_id));
    } else if (OB_ISNULL(user_info)) {
      ret = OB_ERR_USER_NOT_EXIST;
      LOG_WARN("user does not exist", K(ret), K(tenant_id), K(user_id));
    } else {
      int64_t new_schema_version = OB_INVALID_VERSION;
      ObUserInfo new_user_info;
      ObString old_password;
      int64_t old_password_start_time = OB_INVALID_TIMESTAMP;
      if (static_cast<int64_t>(PASSWORD_EXPIRE_NONE) == password_lifetime_us) {
        password_lifetime_us = user_info->get_password_lifetime();
      }
      const bool password_expired = PASSWORD_EXPIRE_NOW == password_expire_option
                                    || (password_last_changed == user_info->get_password_last_changed()
                                        && user_info->get_password_expired());
      if (OB_FAIL(calc_old_password_info(discard_old_password,
                                         retain_current_password,
                                         plugin,
                                         user_info,
                                         old_password,
                                         old_password_start_time))) {
        LOG_WARN("failed to calculate old password info", K(ret), K(tenant_id), K(user_id));
      } else if (OB_FAIL(new_user_info.assign(*user_info))) {
        LOG_WARN("assign user info failed", K(ret), K(tenant_id), K(user_id));
      } else if (OB_FAIL(new_user_info.set_passwd(passwd))) {
        LOG_WARN("set password failed", K(ret), K(tenant_id), K(user_id));
      } else if (OB_FALSE_IT(new_user_info.set_password_last_changed(password_last_changed))) {
      } else if (OB_FALSE_IT(new_user_info.set_password_expired(password_expired))) {
      } else if (!plugin.empty() && OB_FAIL(new_user_info.set_plugin(plugin))) {
        LOG_WARN("set plugin failed", K(ret), K(tenant_id), K(user_id));
      } else if (OB_FAIL(new_user_info.set_old_password(old_password))) {
        LOG_WARN("set old password failed", K(ret), K(tenant_id), K(user_id));
      } else if (OB_FALSE_IT(new_user_info.set_old_password_start_time(old_password_start_time))) {
      } else if (OB_FALSE_IT(new_user_info.set_password_lifetime(password_lifetime_us))) {
      } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
        LOG_WARN("failed to generate schema version", K(ret), K(tenant_id));
      } else if (OB_FAIL(schema_sql_service->get_user_sql_service().set_passwd(new_user_info,
                                                                               new_schema_version,
                                                                               ddl_stmt_str,
                                                                               trans))) {
        LOG_WARN("set password in inner table failed", K(ret), K(tenant_id), K(user_id));
      }
    }
  }
  return ret;
}

} // end namespace rootserver
} // end namespace oceanbase
