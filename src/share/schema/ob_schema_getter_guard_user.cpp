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

#define USING_LOG_PREFIX SHARE_SCHEMA

#include "share/schema/ob_schema_getter_guard.h"
#include "lib/encrypt/ob_encrypted_helper.h"
#include "lib/encrypt/ob_sha256_crypt.h"
#include "lib/net/ob_net_util.h"
#include "lib/allocator/ob_allocator.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/privilege_check/ob_ora_priv_check.h"
#include "lib/encrypt/ob_caching_sha2_cache_mgr.h"

namespace oceanbase
{
using namespace common;
using namespace observer;

namespace share
{
namespace schema
{

int64_t combine_default_value(int64_t value, int64_t default_value)
{
  return ObProfileSchema::DEFAULT_VALUE == value ? default_value : value;
}

int ObSchemaGetterGuard::get_user_profile_failed_login_limits(
    const uint64_t tenant_id,
    const uint64_t user_id,
    int64_t &failed_login_limit_num,
    int64_t &failed_login_limit_time)
{
  int ret = OB_SUCCESS;
  const ObUserInfo *user_info = nullptr;
  const ObProfileSchema *profile_info = nullptr;
  const ObProfileSchema *default_profile = nullptr;
  uint64_t profile_id = OB_INVALID_ID;

  if (OB_FAIL(get_user_info(tenant_id, user_id, user_info))) {
    LOG_WARN("fail to get user id", KR(ret), K(tenant_id), K(user_id));
  } else if (OB_ISNULL(user_info)) {
    ret = OB_USER_NOT_EXIST;
    LOG_WARN("user not exist", KR(ret), K(tenant_id), K(user_id));
  } else {
    uint64_t default_profile_id = OB_ORACLE_TENANT_INNER_PROFILE_ID;
    profile_id = user_info->get_profile_id();
    if (OB_FAIL(get_profile_schema_by_id(user_info->get_tenant_id(),
                                         is_valid_id(profile_id) ? profile_id : default_profile_id,
                                         profile_info))) {
       LOG_WARN("fail to get profile info", KR(ret), KPC(user_info));
    } else if (OB_FAIL(get_profile_schema_by_id(user_info->get_tenant_id(),
                                                default_profile_id,
                                                default_profile))) {
      LOG_WARN("fail to get profile info", KR(ret), KPC(user_info));
    } else {
      failed_login_limit_num = combine_default_value(profile_info->get_failed_login_attempts(),
                                                     default_profile->get_failed_login_attempts());
      failed_login_limit_time = combine_default_value(profile_info->get_password_lock_time(),
                                                      default_profile->get_password_lock_time());
    }
  }

  return ret;
}

// only use in oracle mode
int ObSchemaGetterGuard::get_user_password_expire_times(
    const uint64_t tenant_id,
    const uint64_t user_id,
    int64_t &password_last_change,
    int64_t &password_life_time,
    int64_t &password_grace_time)
{
  int ret = OB_SUCCESS;
  const ObUserInfo *user_info = nullptr;
  const ObProfileSchema *profile_info = nullptr;
  const ObProfileSchema *default_profile = nullptr;
  uint64_t profile_id = OB_INVALID_ID;

  if (OB_FAIL(get_user_info(tenant_id, user_id, user_info))) {
    LOG_WARN("fail to get user id", KR(ret), K(tenant_id), K(user_id));
  } else if (OB_ISNULL(user_info)) {
    ret = OB_USER_NOT_EXIST;
    LOG_WARN("user not exist", KR(ret), K(tenant_id), K(user_id));
  } else {
    uint64_t default_profile_id = OB_ORACLE_TENANT_INNER_PROFILE_ID;
    profile_id = user_info->get_profile_id();
    password_last_change = user_info->get_password_last_changed();
    if (!is_valid_id(profile_id)) {
      profile_id = OB_ORACLE_TENANT_INNER_PROFILE_ID;
    }
    if (OB_FAIL(get_profile_schema_by_id(user_info->get_tenant_id(),
                                         is_valid_id(profile_id) ? profile_id : default_profile_id,
                                         profile_info))) {
       LOG_WARN("fail to get profile info", KR(ret), KPC(user_info));
    } else if (OB_FAIL(get_profile_schema_by_id(user_info->get_tenant_id(),
                                                default_profile_id,
                                                default_profile))) {
      LOG_WARN("fail to get profile info", KR(ret), KPC(user_info));
    } else {
      password_life_time = combine_default_value(profile_info->get_password_life_time(),
                                                 default_profile->get_password_life_time());
      password_grace_time = combine_default_value(profile_info->get_password_grace_time(),
                                                  default_profile->get_password_grace_time());
    }
    password_life_time = (password_life_time == -1) ? INT64_MAX : password_life_time;
    password_grace_time = (password_grace_time == -1) ? INT64_MAX : password_grace_time;
  }

  return ret;
}

int ObSchemaGetterGuard::get_user_info(
    const uint64_t tenant_id,
    const uint64_t user_id,
    const ObUserInfo *&user_info)
{
  int ret = OB_SUCCESS;
  user_info = NULL;

  LOG_TRACE("begin to get user schema", K(user_id));

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == user_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(user_id), KR(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(get_schema(USER_SCHEMA,
                                tenant_id,
                                user_id,
                                user_info))) {
    LOG_WARN("get user schema failed", KR(ret), K(tenant_id), K(user_id));
  }

  return ret;
}

int ObSchemaGetterGuard::get_user_info(const uint64_t tenant_id,
                                       const ObString &user_name,
                                       const ObString &host_name,
                                       const ObUserInfo *&user_info)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  user_info = NULL;

  const ObSimpleUserSchema *simple_user = NULL;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), KR(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->get_user_schema(tenant_id,
                                          user_name,
                                          host_name,
                                          simple_user))) {
    LOG_WARN("get simple user failed", KR(ret), K(tenant_id), K(user_name));
  } else if (NULL == simple_user) {
    LOG_INFO("user not exist", K(tenant_id), K(user_name));
  } else if (OB_FAIL(get_schema(USER_SCHEMA,
                                simple_user->get_tenant_id(),
                                simple_user->get_user_id(),
                                user_info,
                                simple_user->get_schema_version()))) {
    LOG_WARN("get user schema failed", KR(ret), K(tenant_id), KPC(simple_user));
  } else if (OB_ISNULL(user_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", KR(ret), K(tenant_id), K(user_name));
  }
  return ret;
}

int ObSchemaGetterGuard::get_user_info(const uint64_t tenant_id,
                                       const ObString &user_name,
                                       ObIArray<const ObUserInfo *> &users_info)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), KR(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else {
    const int64_t DEFAULT_SAME_USERNAME_COUNT = 4;
    ObSEArray<const ObSimpleUserSchema *, DEFAULT_SAME_USERNAME_COUNT> simple_users;
    if (OB_FAIL(mgr->get_user_schema(tenant_id, user_name, simple_users))) {
      LOG_WARN("get simple user failed", KR(ret), K(tenant_id), K(user_name));
    } else if (simple_users.empty()) {
      LOG_INFO("user not exist", K(tenant_id), K(user_name));
    } else {
      const ObUserInfo *user_info = NULL;
      for (int64_t i = 0; i < simple_users.count() && OB_SUCC(ret); ++i) {
        const ObSimpleUserSchema *&simple_user = simple_users.at(i);
        if (OB_FAIL(get_schema(USER_SCHEMA,
                               simple_user->get_tenant_id(),
                               simple_user->get_user_id(),
                               user_info,
                               simple_user->get_schema_version()))) {
          LOG_WARN("get user schema failed", K(tenant_id), KPC(simple_user), KR(ret));
        } else if (OB_ISNULL(user_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL ptr", KR(ret), KP(user_info));
        } else if (OB_FAIL(users_info.push_back(user_info))) {
          LOG_WARN("failed to push back user_info", KPC(user_info), K(users_info), KR(ret));
        } else {
          user_info = NULL;
        }
      }
    }
  }
  return ret;
}

int ObSchemaGetterGuard::add_role_id_recursively(
  const uint64_t tenant_id,
  uint64_t role_id,
  ObSessionPrivInfo &s_priv,
  common::ObIArray<uint64_t> &enable_role_id_array)
{
  int ret = OB_SUCCESS;
  const ObUserInfo *role_info = NULL;

  if (!has_exist_in_array(enable_role_id_array, role_id)) {
    /* 1. put itself */
    OZ (enable_role_id_array.push_back(role_id));
    /* 2. get role recursively */
    OZ (get_user_info(tenant_id, role_id, role_info));
    if (OB_SUCC(ret) && role_info != NULL) {
      const ObSEArray<uint64_t, 8> &role_id_array = role_info->get_role_id_array();
      for (int i = 0; OB_SUCC(ret) && i < role_id_array.count(); ++i) {
        OZ (add_role_id_recursively(tenant_id, role_info->get_role_id_array().at(i), s_priv, enable_role_id_array));
      }
    }
  }
  return ret;
}

int ObSchemaGetterGuard::check_activate_all_role_var(const uint64_t tenant_id, bool &activate_all_role) {
  int ret = OB_SUCCESS;
  const ObSysVarSchema *session_var = NULL;
  ObObj session_obj;
  ObArenaAllocator alloc(ObModIds::OB_TEMP_VARIABLES);
  activate_all_role = false;
  if (OB_FAIL(get_tenant_system_variable(tenant_id,
                                         SYS_VAR_ACTIVATE_ALL_ROLES_ON_LOGIN,
                                         session_var))) {
    LOG_WARN("fail to get tenant var schema", K(ret));
  } else if (OB_ISNULL(session_var)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get charset_var or collation_var", K(ret));
  } else if (OB_FAIL(session_var->get_value(&alloc, NULL, session_obj))) {
    LOG_WARN("fail to get charset var value", K(ret));
  } else {
    activate_all_role = !!(session_obj.get_int());
  }
  return ret;
}

int ObSchemaGetterGuard::is_user_empty_passwd(const ObUserLoginInfo &login_info, bool &is_empty_passwd_account) {
  int ret = OB_SUCCESS;
  lib::Worker::CompatMode compat_mode = lib::Worker::CompatMode::INVALID;
  uint64_t tenant_id = OB_INVALID_ID;
  is_empty_passwd_account = false;
  if (OB_FAIL(get_tenant_id(login_info.tenant_name_,tenant_id))) {
    LOG_WARN("Invalid tenant", "tenant_name", login_info.tenant_name_, KR(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(get_tenant_compat_mode(tenant_id, compat_mode))) {
    LOG_WARN("fail to get tenant compat mode", K(ret));
  } else {
    const int64_t DEFAULT_SAME_USERNAME_COUNT = 4;
    ObSEArray<const ObUserInfo *, DEFAULT_SAME_USERNAME_COUNT> users_info;
    if (OB_FAIL(get_user_info(tenant_id, login_info.user_name_, users_info))) {
      LOG_WARN("get user info failed", KR(ret), K(tenant_id), K(login_info));
    } else if (users_info.empty()) {
      ret = OB_PASSWORD_WRONG;
      LOG_WARN("No tenant user", K(login_info), KR(ret));
    } else {
      const ObUserInfo *user_info = NULL;
      const ObUserInfo *matched_user_info = NULL;
      for (int64_t i = 0; i < users_info.count() && OB_SUCC(ret); ++i) {
        user_info = users_info.at(i);
        if (NULL == user_info) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("user info is null", K(login_info), KR(ret));
        } else if (user_info->is_role() && lib::Worker::CompatMode::ORACLE == compat_mode) {
          ret = OB_PASSWORD_WRONG;
          LOG_INFO("password error", "tenant_name", login_info.tenant_name_,
              "user_name", login_info.user_name_,
              "client_ip_", login_info.client_ip_, KR(ret));
        } else if (!obsys::ObNetUtil::is_match(login_info.client_ip_, user_info->get_host_name_str())) {
          LOG_TRACE("account not matched, try next", KPC(user_info), K(login_info));
        } else {
          matched_user_info = user_info;
          if (0 == login_info.passwd_.length() && 0 == user_info->get_passwd_str().length()) {
            is_empty_passwd_account = true;
            break;
          }
        }
      }
    }
  }
  return ret;
}

// for user authentication
int ObSchemaGetterGuard::check_user_access(
    const ObUserLoginInfo &login_info,
    ObSessionPrivInfo &s_priv,
    common::ObIArray<uint64_t> &enable_role_id_array,
    SSL *ssl_st,
    const ObUserInfo *&sel_user_info)
{
  int ret = OB_SUCCESS;
  lib::Worker::CompatMode compat_mode = lib::Worker::CompatMode::INVALID;
  sel_user_info = NULL;
  if (OB_FAIL(get_tenant_id(login_info.tenant_name_, s_priv.tenant_id_))) {
    LOG_WARN("Invalid tenant", "tenant_name", login_info.tenant_name_, KR(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(s_priv.tenant_id_))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(s_priv), K_(tenant_id));
  } else if (OB_FAIL(get_tenant_compat_mode(s_priv.tenant_id_, compat_mode))) {
    LOG_WARN("fail to get tenant compat mode", K(ret));
  } else {
    const int64_t DEFAULT_SAME_USERNAME_COUNT = 4;
    ObSEArray<const ObUserInfo *, DEFAULT_SAME_USERNAME_COUNT> users_info;
    if (OB_FAIL(get_user_info(s_priv.tenant_id_, login_info.user_name_, users_info))) {
      LOG_WARN("get user info failed", KR(ret), K(s_priv.tenant_id_), K(login_info));
    } else if (users_info.empty()) {
      ret = OB_PASSWORD_WRONG;
      LOG_WARN("No tenant user", K(login_info), KR(ret));
    } else {
      bool is_found = false;
      const ObUserInfo *user_info = NULL;
      const ObUserInfo *matched_user_info = NULL;
      for (int64_t i = 0; i < users_info.count() && OB_SUCC(ret) && !is_found; ++i) {
        user_info = users_info.at(i);
        if (NULL == user_info) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("user info is null", K(login_info), KR(ret));
        } else if (user_info->is_role() && lib::Worker::CompatMode::ORACLE == compat_mode) {
          ret = OB_PASSWORD_WRONG;
          LOG_INFO("password error", "tenant_name", login_info.tenant_name_,
              "user_name", login_info.user_name_,
              "client_ip_", login_info.client_ip_, KR(ret));
        } else if (!obsys::ObNetUtil::is_match(login_info.client_ip_, user_info->get_host_name_str())) {
          LOG_TRACE("account not matched, try next", KPC(user_info), K(login_info));
        } else {
          matched_user_info = user_info;
          if (0 == login_info.passwd_.length() && 0 == user_info->get_passwd_str().length()) {
            //passed
            is_found = true;
          } else if (0 == login_info.passwd_.length() || 0 == user_info->get_passwd_str().length()) {
            ret = OB_PASSWORD_WRONG;
            LOG_WARN("password error", KR(ret), K(login_info.passwd_.length()),
                     K(user_info->get_passwd_str().length()));
          } else {
            // Get user's authentication plugin
            ObString plugin = user_info->get_plugin_str();

            // Dispatch to different authentication methods based on plugin
            if (ObEncryptedHelper::is_native_password_plugin(plugin)) {
              // mysql_native_password authentication
              char stored_stage2_hex[SCRAMBLE_LENGTH] = {0};
              ObString stored_stage2_trimed;
              ObString stored_stage2_hex_str;
              if (user_info->get_passwd_str().length() < SCRAMBLE_LENGTH *2 + 1) {
                ret = OB_NOT_IMPLEMENT;
                LOG_WARN("Currently hash method other than MySQL 4.1 hash is not implemented.",
                         "hash str length", user_info->get_passwd_str().length());
              } else {
                //trim the leading '*'
                stored_stage2_trimed.assign_ptr(user_info->get_passwd_str().ptr() + 1,
                                                user_info->get_passwd_str().length() - 1);
                stored_stage2_hex_str.assign_buffer(stored_stage2_hex, SCRAMBLE_LENGTH);
                stored_stage2_hex_str.set_length(SCRAMBLE_LENGTH);
                //first, we restore the stored, displayable stage2 hash to its hex form
                ObEncryptedHelper::displayable_to_hex(stored_stage2_trimed, stored_stage2_hex_str);
                //then, we call the mysql validation logic.
                if (OB_FAIL(ObEncryptedHelper::check_login(login_info.passwd_,
                                                           login_info.scramble_str_,
                                                           stored_stage2_hex_str,
                                                           is_found))) {
                  LOG_WARN("Failed to check login", K(login_info), KR(ret));
                } else if (!is_found) {
                  LOG_INFO("password error", "tenant_name", login_info.tenant_name_,
                           "user_name", login_info.user_name_,
                           "client_ip", login_info.client_ip_,
                           "host_name", user_info->get_host_name_str());
                } else {
                  //found it
                }
              }
            } else if (ObEncryptedHelper::is_caching_sha2_password_plugin(plugin)) {
              // caching_sha2_password authentication
              LOG_DEBUG("caching_sha2_password authentication",
                        K(login_info.user_name_), K(login_info.passwd_.length()),
                        K(login_info.is_passwd_plaintext_));

              if (login_info.is_passwd_plaintext_) {
                // Full authentication mode: verify with plaintext password
                if (OB_FAIL(ObSha256Crypt::check_sha256_password(
                        login_info.passwd_,
                        login_info.scramble_str_,
                        user_info->get_passwd_str(),
                        is_found))) {
                  LOG_WARN("Failed to check caching_sha2_password with plaintext", K(ret), K(login_info));
                } else if (!is_found) {
                  LOG_INFO("caching_sha2_password full authentication failed",
                           "tenant_name", login_info.tenant_name_,
                           "user_name", login_info.user_name_,
                           "client_ip", login_info.client_ip_,
                           "host_name", user_info->get_host_name_str());
                } else {
                  // Full authentication succeeded; generate and cache double SHA256 digest
                  unsigned char digest_buf[OB_SHA256_DIGEST_LENGTH];
                  int tmp_ret = OB_SUCCESS;

                  if (OB_SUCCESS != (tmp_ret = ObSha256Crypt::generate_sha2_digest_for_cache(
                                                              login_info.passwd_.ptr(),
                                                              login_info.passwd_.length(),
                                                              digest_buf,
                                                              OB_SHA256_DIGEST_LENGTH))) {
                    LOG_WARN("failed to generate sha2 digest for cache", K(tmp_ret));
                    // Cache failure does not affect authentication success, continue
                  } else if (OB_SUCCESS != (tmp_ret = ObCachingSha2CacheMgr::get_instance().put_digest(
                                                      login_info.user_name_,
                                                      user_info->get_host_name_str(),
                                                      s_priv.tenant_id_,
                                                      user_info->get_password_last_changed(),
                                                      digest_buf,
                                                      OB_SHA256_DIGEST_LENGTH))) {
                    LOG_WARN("failed to put digest to cache", K(tmp_ret),
                             K(login_info.user_name_), K(user_info->get_host_name_str()));
                    // Cache failure does not affect authentication success, continue
                  } else {
                    LOG_INFO("successfully cached sha2 digest for fast auth",
                             K(login_info.user_name_),
                             K(user_info->get_host_name_str()),
                             K(s_priv.tenant_id_),
                             K(user_info->get_password_last_changed()));
                  }

                  // Clear sensitive data
                  MEMSET(digest_buf, 0, sizeof(digest_buf));
                }
              } else {
                // Fast authentication mode: verify with scramble response
                // Fast authentication is already done at connect time, no need to repeat in this phase
                is_found = true;
              }
            } else {
              // Unsupported authentication plugin
              ret = OB_NOT_SUPPORTED;
              LOG_WARN("Unsupported authentication plugin", K(ret), K(plugin), K(login_info.user_name_));
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (matched_user_info != NULL
            && matched_user_info->get_is_locked()
            && !sql::ObOraSysChecker::is_super_user(matched_user_info->get_user_id())) {
          if (is_found) {
            s_priv.user_id_ = matched_user_info->get_user_id();
          }
          ret = OB_ERR_USER_IS_LOCKED;
          LOG_WARN("User is locked", KR(ret));
        } else if (!is_found) {
          user_info = NULL;
          ret = OB_PASSWORD_WRONG;
          LOG_INFO("password error", "tenant_name", login_info.tenant_name_,
                   "user_name", login_info.user_name_,
                   "client_ip_", login_info.client_ip_, KR(ret));
        } else if (OB_FAIL(check_ssl_access(*user_info, ssl_st))) {
          LOG_WARN("check_ssl_access failed", "tenant_name", login_info.tenant_name_,
                   "user_name", login_info.user_name_,
                   "client_ip_", login_info.client_ip_, KR(ret));
        }
      }
      const ObUserInfo *proxied_user_info = NULL;
      uint64_t proxied_info_idx = OB_INVALID_INDEX;
      if (OB_SUCC(ret) && compat_mode == lib::Worker::CompatMode::ORACLE) {
        if (!login_info.proxied_user_name_.empty()) {
          users_info.reuse();
          if (OB_FAIL(get_user_info(s_priv.tenant_id_, login_info.proxied_user_name_, users_info))) {
            LOG_WARN("get user info failed", KR(ret), K(s_priv.tenant_id_), K(login_info));
          } else if (users_info.count() <= 0) {
            ret = OB_PASSWORD_WRONG;
            LOG_WARN("proxy user not existed", K(ret));
          } else if (OB_ISNULL(proxied_user_info = users_info.at(0))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected error", K(ret));
          } else {
            is_found = false;
            for (int64_t i = 0; OB_SUCC(ret) && !is_found && i < proxied_user_info->get_proxied_user_info_cnt(); i++) {
              const ObProxyInfo *proxied_info = proxied_user_info->get_proxied_user_info_by_idx(i);
              if (OB_ISNULL(proxied_info)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unexpected error", K(ret));
              } else if (proxied_info->user_id_ == user_info->get_user_id()) {
                is_found = true;
                proxied_info_idx = i;
              }
            }
            if (OB_FAIL(ret)) {
            } else if (!is_found) {
              ret = OB_PASSWORD_WRONG;
              LOG_WARN("proxy user not existed", KR(ret), K(user_info->get_user_id()), KPC(proxied_user_info));
              proxied_user_info = NULL;
            } else {
              s_priv.proxy_user_name_ = user_info->get_user_name_str();
              s_priv.proxy_host_name_ = user_info->get_host_name_str();
            }
          }
        }
      }

      if (OB_SUCC(ret) && proxied_user_info!= NULL) {
        if (proxied_user_info->get_is_locked()
            && !sql::ObOraSysChecker::is_super_user(proxied_user_info->get_user_id())) {
          ret = OB_ERR_USER_IS_LOCKED;
          LOG_WARN("User is locked", KR(ret));
        }
      }

      if (OB_SUCC(ret)) {
        s_priv.tenant_id_ = user_info->get_tenant_id();
        if (proxied_user_info != NULL) {
          s_priv.user_id_ = proxied_user_info->get_user_id();
          s_priv.proxy_user_id_ = user_info->get_user_id();
          s_priv.user_name_ = proxied_user_info->get_user_name_str();
          s_priv.host_name_ = proxied_user_info->get_host_name_str();
          s_priv.proxy_user_name_ = user_info->get_user_name_str();
          s_priv.proxy_host_name_ = user_info->get_host_name_str();
          s_priv.user_priv_set_ = proxied_user_info->get_priv_set();
          sel_user_info = proxied_user_info;
        } else {
          s_priv.user_id_ = user_info->get_user_id();
          s_priv.proxy_user_id_ = OB_INVALID_ID;
          s_priv.user_id_ = user_info->get_user_id();
          s_priv.user_name_ = user_info->get_user_name_str();
          s_priv.host_name_ = user_info->get_host_name_str();
          s_priv.proxy_user_name_ = ObString();
          s_priv.proxy_host_name_ = ObString();
          s_priv.user_priv_set_ = user_info->get_priv_set();
          sel_user_info = user_info;
        }
        s_priv.db_ = login_info.db_;
        // load role privx
        if (OB_SUCC(ret)) {
          bool activate_all_role = false;
          CK (user_info->get_role_id_array().count() ==
              user_info->get_role_id_option_array().count());

          if (OB_SUCC(ret) && lib::Worker::CompatMode::MYSQL == compat_mode) {
            if (OB_FAIL(check_activate_all_role_var(user_info->get_tenant_id(), activate_all_role))) {
              LOG_WARN("fail to check activate all role", K(ret));
            }
          }

          ObSEArray<uint64_t, 8> role_id_array;
          ObSEArray<uint64_t, 8> role_id_option_array;
          if (proxied_user_info != NULL) {
            CK (proxied_user_info->get_role_id_array().count() ==
                proxied_user_info->get_role_id_option_array().count());
            OZ (role_id_array.assign(proxied_user_info->get_role_id_array()));
            OZ (role_id_option_array.assign(proxied_user_info->get_role_id_option_array()));
          } else {
            CK (user_info->get_role_id_array().count() ==
                user_info->get_role_id_option_array().count());
            OZ (role_id_array.assign(user_info->get_role_id_array()));
            OZ (role_id_option_array.assign(user_info->get_role_id_option_array()));
          }
          if (OB_SUCC(ret) && compat_mode == lib::Worker::CompatMode::ORACLE && proxied_user_info != NULL) {
            const ObProxyInfo *proxied_info = NULL;
            if (OB_UNLIKELY(proxied_info_idx < 0 || proxied_info_idx >= proxied_user_info->get_proxied_user_info_cnt())) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected error", K(ret));
            } else if (OB_ISNULL(proxied_info = proxied_user_info->get_proxied_user_info_by_idx(proxied_info_idx))
                      || OB_UNLIKELY(proxied_info->user_id_ != user_info->get_user_id())) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected error", K(ret));
            } else {
              ObArray<uint64_t> new_role_id_array;
              ObArray<uint64_t> new_role_id_option_array;
              if (OB_FAIL(sql::ObSQLUtils::get_proxy_can_activate_role(role_id_array,
                                                                  role_id_option_array,
                                                                  *proxied_info,
                                                                  new_role_id_array,
                                                                  new_role_id_option_array))) {
                LOG_WARN("get proxy can activate role failed", K(ret));
              } else {
                role_id_array.reuse();
                role_id_option_array.reuse();
                for (int64_t i = 0; OB_SUCC(ret) && i < new_role_id_array.count(); i++) {
                  const ObUserInfo *role_info = NULL;
                  if (OB_FAIL(get_user_info(s_priv.tenant_id_, new_role_id_array.at(i), role_info))) {
                    LOG_WARN("failed to get role ids", KR(ret), K(new_role_id_array.at(i)));
                  } else if (NULL == role_info) {
                    ret = OB_ERR_UNEXPECTED;
                    LOG_WARN("role info is null", KR(ret), K(new_role_id_array.at(i)));
                  } else if (!role_info->get_passwd_str().empty()) {
                    //do nothing
                  } else {
                    OZ (role_id_array.push_back(new_role_id_array.at(i)));
                    OZ (role_id_option_array.push_back(new_role_id_option_array.at(i)));
                  }
                }
              }
            }
          }
          for (int i = 0; OB_SUCC(ret) && i < role_id_array.count(); ++i) {
            const ObUserInfo *role_info = NULL;
            if (OB_FAIL(get_user_info(s_priv.tenant_id_, role_id_array.at(i), role_info))) {
              LOG_WARN("failed to get role ids", KR(ret), K(role_id_array.at(i)));
            } else if (NULL == role_info) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("role info is null", KR(ret), K(role_id_array.at(i)));
            } else if (lib::Worker::CompatMode::ORACLE == compat_mode) {
              s_priv.user_priv_set_ |= role_info->get_priv_set();
              if (user_info->get_disable_option(role_id_option_array.at(i)) == 0) {
                OZ (add_role_id_recursively(s_priv.tenant_id_,
                                            role_id_array.at(i),
                                            s_priv,
                                            enable_role_id_array));
              }
            } else {
              if (activate_all_role
                  || user_info->get_disable_option(user_info->get_role_id_option_array().at(i)) == 0) {
                OZ (enable_role_id_array.push_back(role_id_array.at(i)));
              }
            }
          }
          if (lib::Worker::CompatMode::ORACLE == compat_mode) {
            OZ (add_role_id_recursively(user_info->get_tenant_id(),
                                        OB_ORA_PUBLIC_ROLE_ID,
                                        s_priv,
                                        enable_role_id_array));
          }
        }

        //check db access and db existence
        if (!login_info.db_.empty()
            && OB_FAIL(check_db_access(s_priv, enable_role_id_array, login_info.db_, s_priv.db_priv_set_))) {
          LOG_WARN("Database access deined", K(login_info), KR(ret));
        } else { }
      }
    }
  }
  return ret;
}

int ObSchemaGetterGuard::check_ssl_access(const ObUserInfo &user_info, SSL *ssl_st)
{
  int ret = OB_SUCCESS;
  switch (user_info.get_ssl_type()) {
    case ObSSLType::SSL_TYPE_NOT_SPECIFIED:
    case ObSSLType::SSL_TYPE_NONE: {
      //do nothing
      break;
    }
    case ObSSLType::SSL_TYPE_ANY: {
      if (NULL == ssl_st) {
        ret = OB_PASSWORD_WRONG;
        LOG_WARN("not use ssl", KR(ret));
      }
      break;
    }
    case ObSSLType::SSL_TYPE_X509: {
      X509 *cert = NULL;
      int64_t verify_result = 0;
      if (NULL == ssl_st
          || (X509_V_OK != (verify_result = SSL_get_verify_result(ssl_st)))
          || (NULL == (cert = SSL_get_peer_certificate(ssl_st)))) {
        ret = OB_PASSWORD_WRONG;
        LOG_WARN("X509 check failed", KP(ssl_st), K(verify_result), KR(ret));
      }
      X509_free(cert);
      break;
    }
    case ObSSLType::SSL_TYPE_SPECIFIED: {
      X509 *cert = NULL;
      int64_t verify_result = 0;
      char *x509_issuer = NULL;
      char *x509_subject = NULL;
      if (NULL == ssl_st
          || (X509_V_OK != (verify_result = SSL_get_verify_result(ssl_st)))
          || (NULL == (cert = SSL_get_peer_certificate(ssl_st)))) {
        ret = OB_PASSWORD_WRONG;
        LOG_WARN("X509 check failed", KP(ssl_st), K(verify_result), KR(ret));
      }


      if (OB_SUCC(ret)
          && !user_info.get_ssl_cipher_str().empty()
          && user_info.get_ssl_cipher_str().compare(SSL_get_cipher(ssl_st)) != 0) {
        ret = OB_PASSWORD_WRONG;
        LOG_WARN("X509 cipher check failed", "expect", user_info.get_ssl_cipher_str(),
                 "receive", SSL_get_cipher(ssl_st), KR(ret));
      }

      if (OB_SUCC(ret) && !user_info.get_x509_issuer_str().empty()) {
        x509_issuer = X509_NAME_oneline(X509_get_issuer_name(cert), 0, 0);
        if (user_info.get_x509_issuer_str().compare(x509_issuer) != 0) {
          ret = OB_PASSWORD_WRONG;
          LOG_WARN("x509 issue check failed", "expect", user_info.get_x509_issuer_str(),
                   "receive", x509_issuer, KR(ret));
        }
      }

      if (OB_SUCC(ret) && !user_info.get_x509_subject_str().empty()) {
        x509_subject = X509_NAME_oneline(X509_get_subject_name(cert), 0, 0);
        if (user_info.get_x509_subject_str().compare(x509_subject) != 0) {
          ret = OB_PASSWORD_WRONG;
          LOG_WARN("x509 subject check failed", "expect", user_info.get_x509_subject_str(),
                   "receive", x509_subject, KR(ret));
        }
      }

      OPENSSL_free(x509_issuer);
      OPENSSL_free(x509_subject);
      X509_free(cert);
      break;
    }
    default: {
      ret = OB_PASSWORD_WRONG;
      LOG_WARN("unknonw type", K(user_info), KR(ret));
      break;
    }
  }

  if (OB_FAIL(ret)) {
    LOG_TRACE("fail to check_ssl_access", K(user_info), KR(ret));
  }
  return ret;
}

int ObSchemaGetterGuard::get_session_priv_info(const uint64_t tenant_id,
                                               const uint64_t user_id,
                                               const ObString &database_name,
                                               ObSessionPrivInfo &session_priv)
{
  int ret = OB_SUCCESS;
  const ObUserInfo *user_info = NULL;
  if (OB_FAIL(get_user_info(tenant_id,
                            user_id,
                            user_info))) {
    LOG_WARN("failed to get user info", KR(ret), K(tenant_id), K(user_id));
  } else if (NULL == user_info) {
    ret = OB_USER_NOT_EXIST;
    LOG_WARN("user info is null", KR(ret), K(user_id));
  } else {
    const ObSchemaMgr *mgr = NULL;
    ObOriginalDBKey db_priv_key(tenant_id,
                                user_info->get_user_id(),
                                database_name);
    ObPrivSet db_priv_set = OB_PRIV_SET_EMPTY;
    if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
      LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id));
    } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
      LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
    } else if (OB_FAIL(mgr->priv_mgr_.get_db_priv_set(db_priv_key, db_priv_set))) {
      LOG_WARN("get db priv set failed", KR(ret), K(db_priv_key));
    } else {
      session_priv.tenant_id_ = tenant_id;
      session_priv.user_id_ = user_info->get_user_id();
      session_priv.user_name_ = user_info->get_user_name_str();
      session_priv.host_name_ = user_info->get_host_name_str();
      session_priv.db_ = database_name;
      session_priv.user_priv_set_ = user_info->get_priv_set();
      session_priv.db_priv_set_ = db_priv_set;
    }
  }
  return ret;
}

int ObSchemaGetterGuard::get_user_infos_with_tenant_id(
    const uint64_t tenant_id,
    common::ObIArray<const ObUserInfo *> &user_infos)
{
  int ret = OB_SUCCESS;
  user_infos.reset();

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(get_user_schemas_in_tenant(tenant_id,
                                                user_infos))) {
    LOG_WARN("get user schemas in tenant failed", KR(ret), K(tenant_id));
  }
  return ret;
}

} // end namespace schema
} // end namespace share
} // end namespace oceanbase
