/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE_SCHEMA

#include "share/schema/ob_schema_getter_guard.h"
#include "lib/encrypt/ob_encrypted_helper.h"
#include "lib/encrypt/ob_sha256_crypt.h"
#include "lib/net/ob_addr.h"
#include "lib/net/ob_net_util.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/time/ob_time_utility.h"
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

// MySQL sorts exact IPs before CIDR ranges and dotted subnet-mask ranges.
enum class ObSchemaGetterGuard::ObHostMatchType
{
  INVALID = 0,
  SUBNET_MASK = 1,
  CIDR = 2,
  EXACT_IP = 3
};

struct ObSchemaGetterGuard::ObHostMatchRange
{
  ObHostMatchRange() : type_(ObHostMatchType::INVALID), address_(), mask_() {}

  ObHostMatchType type_;
  ObAddr address_;
  ObAddr mask_;
};

bool ObSchemaGetterGuard::has_host_wildcard(const ObString &host_name)
{
  return OB_NOT_NULL(host_name.find('%')) || OB_NOT_NULL(host_name.find('_'));
}

bool ObSchemaGetterGuard::parse_host_mask_bits(const ObString &mask_str, int64_t &mask_bits)
{
  bool is_valid = !mask_str.empty();
  mask_bits = 0;
  for (int64_t i = 0; i < mask_str.length() && is_valid; ++i) {
    const char ch = mask_str.ptr()[i];
    if (ch < '0' || ch > '9') {
      is_valid = false;
    } else {
      mask_bits = mask_bits * 10 + ch - '0';
      // Network-mask notation cannot be used for IPv6 addresses.
      is_valid = mask_bits <= 32;
    }
  }
  return is_valid && mask_bits > 0;
}

bool ObSchemaGetterGuard::parse_ipv4_subnet_mask(const ObString &mask_str, ObAddr &mask)
{
  bool is_valid = false;
  const int64_t MAX_IPV4_MASK_BITS = 32;
  ObAddr expected_mask;
  if (mask.set_ip_addr(mask_str, 0) && mask.using_ipv4()) {
    for (int64_t mask_bits = 1; mask_bits <= MAX_IPV4_MASK_BITS && !is_valid; ++mask_bits) {
      if (expected_mask.as_mask(mask_bits, ObAddr::IPV4)
          && expected_mask.is_equal_except_port(mask)) {
        is_valid = true;
      }
    }
  }
  return is_valid;
}

bool ObSchemaGetterGuard::build_host_match_range(const ObString &host_name,
                                                 ObHostMatchRange &range)
{
  bool is_valid = false;
  const char *slash = host_name.find('/');
  if (OB_ISNULL(slash)) {
    if (range.address_.set_ip_addr(host_name, 0)) {
      const int64_t mask_bits = range.address_.using_ipv4() ? 32 : 128;
      is_valid = range.mask_.as_mask(mask_bits, range.address_.get_version());
      if (is_valid) {
        range.type_ = ObHostMatchType::EXACT_IP;
      }
    }
  } else if (slash > host_name.ptr() && slash + 1 < host_name.ptr() + host_name.length()) {
    ObString host_ip;
    ObString mask_str;
    int64_t mask_bits = 0;
    host_ip.assign_ptr(host_name.ptr(), slash - host_name.ptr());
    mask_str.assign_ptr(slash + 1, host_name.ptr() + host_name.length() - slash - 1);
    if (range.address_.set_ip_addr(host_ip, 0) && range.address_.using_ipv4()) {
      if (OB_NOT_NULL(mask_str.find('.'))) {
        is_valid = parse_ipv4_subnet_mask(mask_str, range.mask_);
        if (is_valid) {
          range.type_ = ObHostMatchType::SUBNET_MASK;
        }
      } else if (parse_host_mask_bits(mask_str, mask_bits)) {
        is_valid = range.mask_.as_mask(mask_bits, range.address_.get_version());
        if (is_valid) {
          range.type_ = ObHostMatchType::CIDR;
        }
      }
    }
  }
  return is_valid;
}

bool ObSchemaGetterGuard::is_host_match_range_subset(const ObHostMatchRange &lhs,
                                                     const ObHostMatchRange &rhs)
{
  bool is_subset = false;
  if (lhs.address_.get_version() == rhs.address_.get_version()
      && lhs.mask_.get_version() == rhs.mask_.get_version()) {
    ObAddr lhs_mask = lhs.mask_;
    ObAddr lhs_network = lhs.address_;
    ObAddr rhs_network = rhs.address_;
    lhs_mask.as_subnet(rhs.mask_);
    lhs_network.as_subnet(rhs.mask_);
    rhs_network.as_subnet(rhs.mask_);
    is_subset = lhs_mask.is_equal_except_port(rhs.mask_)
                && lhs_network.is_equal_except_port(rhs_network);
  }
  return is_subset;
}

bool ObSchemaGetterGuard::is_auth_user_host_match(const ObString &client_ip,
                                                  const ObString &host_name,
                                                  const bool enable_host_match_priority)
{
  bool is_match = false;
  if (!enable_host_match_priority || OB_ISNULL(host_name.find('/'))) {
    // Keep the legacy behavior before the feature is enabled and for non-range Hosts.
    is_match = obsys::ObNetUtil::is_match(client_ip, host_name);
  } else {
    ObHostMatchRange host_range;
    ObAddr client_address;
    if (build_host_match_range(host_name, host_range)
        && client_address.set_ip_addr(client_ip, 0)
        && client_address.get_version() == host_range.address_.get_version()) {
      // MySQL range matching requires (client_ip & mask) to equal host_ip.
      client_address.as_subnet(host_range.mask_);
      is_match = client_address.is_equal_except_port(host_range.address_);
    }
  }
  return is_match;
}

int64_t ObSchemaGetterGuard::get_host_pattern_sort(const ObString &host_name)
{
  const int64_t LITERAL_HOST_SORT = 128;
  const int64_t MAX_PATTERN_HOST_SORT = 127;
  int64_t sort_value = host_name.empty() ? 0 : LITERAL_HOST_SORT;
  // Literal hosts sort before patterns, while '%' is the least-specific nonempty pattern.
  // A later first wildcard makes a host pattern more specific.
  for (int64_t i = 0; i < host_name.length() && LITERAL_HOST_SORT == sort_value; ++i) {
    if ('%' == host_name.ptr()[i] || '_' == host_name.ptr()[i]) {
      if (0 == i && '%' == host_name.ptr()[i] && 1 == host_name.length()) {
        sort_value = 1;
      } else {
        sort_value = i + 2;
        if (sort_value > MAX_PATTERN_HOST_SORT) {
          sort_value = MAX_PATTERN_HOST_SORT;
        }
      }
    }
  }
  return sort_value;
}

// Returns 1 if lhs is more specific, 0 if lhs should not replace rhs, and -1 if incomparable.
int ObSchemaGetterGuard::compare_auth_user_host_specificity(const ObString &lhs,
                                                            const ObString &rhs)
{
  int compare_result = -1;
  ObHostMatchRange lhs_range;
  ObHostMatchRange rhs_range;
  const bool lhs_is_range = build_host_match_range(lhs, lhs_range);
  const bool rhs_is_range = build_host_match_range(rhs, rhs_range);
  const bool lhs_has_wildcard = has_host_wildcard(lhs);
  const bool rhs_has_wildcard = has_host_wildcard(rhs);
  const int64_t lhs_pattern_sort = get_host_pattern_sort(lhs);
  const int64_t rhs_pattern_sort = get_host_pattern_sort(rhs);

  if (0 == lhs.compare(rhs)) {
    compare_result = 0;
  } else if (lhs_is_range && rhs_is_range) {
    if (lhs_range.type_ != rhs_range.type_) {
      if (static_cast<int64_t>(lhs_range.type_) > static_cast<int64_t>(rhs_range.type_)) {
        compare_result = 1;
      } else {
        compare_result = 0;
      }
    } else {
      const bool lhs_subset_rhs = is_host_match_range_subset(lhs_range, rhs_range);
      const bool rhs_subset_lhs = is_host_match_range_subset(rhs_range, lhs_range);
      if (lhs_subset_rhs && rhs_subset_lhs) {
        compare_result = 0;
      } else if (lhs_subset_rhs) {
        compare_result = 1;
      } else if (rhs_subset_lhs) {
        compare_result = 0;
      }
    }
  } else if (lhs_is_range && rhs_has_wildcard) {
    compare_result = 1;
  } else if (rhs_is_range && lhs_has_wildcard) {
    compare_result = 0;
  } else if (!lhs_is_range && !rhs_is_range && lhs_pattern_sort > rhs_pattern_sort) {
    compare_result = 1;
  } else if (!lhs_is_range && !rhs_is_range && lhs_pattern_sort <= rhs_pattern_sort) {
    compare_result = 0;
  }
  return compare_result;
}

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

int ObSchemaGetterGuard::get_user_password_rollover_time(
    const uint64_t tenant_id,
    const uint64_t user_id,
    int64_t &password_rollover_time)
{
  int ret = OB_SUCCESS;
  const ObUserInfo *user_info = NULL;
  const ObProfileSchema *profile_info = NULL;
  const ObProfileSchema *default_profile = NULL;
  uint64_t profile_id = OB_INVALID_ID;
  password_rollover_time = 0;
  if (OB_FAIL(get_user_info(tenant_id, user_id, user_info))) {
    LOG_WARN("fail to get user id", KR(ret), K(tenant_id), K(user_id));
  } else if (OB_ISNULL(user_info)) {
    ret = OB_USER_NOT_EXIST;
    LOG_WARN("user not exist", KR(ret), K(tenant_id), K(user_id));
  } else {
    uint64_t default_profile_id = OB_ORACLE_TENANT_INNER_PROFILE_ID;
    profile_id = user_info->get_profile_id();
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
      password_rollover_time = combine_default_value(profile_info->get_password_rollover_time(),
                                                     default_profile->get_password_rollover_time());
      password_rollover_time = (password_rollover_time == -1) ? 0 : password_rollover_time;
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
    const uint64_t auth_user_id = login_info.auth_user_id_;
    const ObUserInfo *user_info = NULL;
    if (!is_valid_id(auth_user_id)) {
      ret = OB_PASSWORD_WRONG;
      LOG_WARN("invalid authentication user id", KR(ret), K(auth_user_id));
    } else if (OB_FAIL(get_user_info(tenant_id, auth_user_id, user_info))) {
      LOG_WARN("get user info failed", KR(ret), K(tenant_id), K(auth_user_id));
    } else if (NULL == user_info) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("user info is null", K(login_info), KR(ret));
    } else if (user_info->get_user_name_str() != login_info.user_name_) {
      ret = OB_PASSWORD_WRONG;
      LOG_WARN("authentication user does not match login info", KR(ret), K(auth_user_id));
    } else if (user_info->is_role() && lib::Worker::CompatMode::ORACLE == compat_mode) {
      ret = OB_PASSWORD_WRONG;
      LOG_INFO("password error", "tenant_name", login_info.tenant_name_,
          "user_name", login_info.user_name_,
          "client_ip_", login_info.client_ip_, KR(ret));
    } else if (0 == login_info.passwd_.length() && 0 == user_info->get_passwd_str().length()) {
      is_empty_passwd_account = true;
    }
  }
  return ret;
}

// for user authentication
int ObSchemaGetterGuard::check_user_access(const ObUserLoginInfo &login_info,
                                           ObSessionPrivInfo &s_priv,
                                           common::ObIArray<uint64_t> &enable_role_id_array,
                                           SSL *ssl_st,
                                           const ObUserInfo *&sel_user_info)
{
  int ret = OB_SUCCESS;
  lib::Worker::CompatMode compat_mode = lib::Worker::CompatMode::INVALID;
  sel_user_info = NULL;
  bool is_oracle_mode = false;
  uint64_t auth_user_id = login_info.auth_user_id_;
  // Table API legacy login has no auth_user_id and an empty client_ip.
  const bool is_legacy_table_login = OB_INVALID_ID == auth_user_id && login_info.client_ip_.empty();
  if (OB_FAIL(get_tenant_id(login_info.tenant_name_, s_priv.tenant_id_))) {
    LOG_WARN("Invalid tenant", "tenant_name", login_info.tenant_name_, KR(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(s_priv.tenant_id_))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(s_priv), K_(tenant_id));
  } else if (OB_FAIL(get_tenant_compat_mode(s_priv.tenant_id_, compat_mode))) {
    LOG_WARN("fail to get tenant compat mode", K(ret));
  } else if (is_legacy_table_login
             && OB_FAIL(get_user_id(s_priv.tenant_id_,
                                    login_info.user_name_,
                                    ObString::make_string("%"),
                                    auth_user_id))) {
    LOG_WARN("fail to resolve legacy authentication user", KR(ret), K(s_priv.tenant_id_));
  } else {
    const int64_t DEFAULT_SAME_USERNAME_COUNT = 4;
    const ObUserInfo *user_info = nullptr;
    bool pwd_match = false;
    ObSEArray<const ObUserInfo *, DEFAULT_SAME_USERNAME_COUNT> proxied_users_info;
    const ObUserInfo *proxied_user_info = nullptr;
    uint64_t proxied_info_idx = OB_INVALID_INDEX;
    is_oracle_mode = (compat_mode == lib::Worker::CompatMode::ORACLE);
    if (OB_INVALID_ID == auth_user_id) {
      ret = OB_PASSWORD_WRONG;
      LOG_WARN("invalid authentication user id", KR(ret), K(auth_user_id), K(login_info.auth_user_id_));
    } else if (OB_FAIL(get_user_info(s_priv.tenant_id_, auth_user_id, user_info))) {
      LOG_WARN("get user info failed", KR(ret), K(s_priv.tenant_id_), K(auth_user_id));
    } else if (OB_ISNULL(user_info)) {
      ret = OB_PASSWORD_WRONG;
      LOG_WARN("authentication user does not exist", KR(ret), K(s_priv.tenant_id_), K(auth_user_id));
    } else if (user_info->get_user_name_str() != login_info.user_name_) {
      ret = OB_PASSWORD_WRONG;
      LOG_WARN("authentication user does not match login info", KR(ret), K(auth_user_id));
    } else {
      if (OB_FAIL(verify_user_password_authentication(user_info, login_info, compat_mode, s_priv.tenant_id_, pwd_match))) {
        LOG_WARN("Failed to verify user password authentication", K(login_info), KR(ret));
      } else if (pwd_match) {
        // do nothing
      } else if (user_info->get_old_password_start_time() != OB_INVALID_TIMESTAMP) {
        // try dual password
        bool dual_password_valid = true;
        if (is_oracle_mode) {
          int64_t password_rollover_time = 0;
          if (OB_FAIL(get_user_password_rollover_time(s_priv.tenant_id_,
                                                      user_info->get_user_id(),
                                                      password_rollover_time))) {
            LOG_WARN("fail to get password rollover time", KR(ret), K(s_priv.tenant_id_), KPC(user_info));
          } else if (password_rollover_time > 0) {
            const int64_t start_ts = user_info->get_old_password_start_time();
            int64_t expire_ts = start_ts + password_rollover_time;
            expire_ts = (expire_ts < start_ts) ? INT64_MAX : expire_ts;
            dual_password_valid = (ObTimeUtility::current_time() < expire_ts);
          }
        }
        if (OB_SUCC(ret) && dual_password_valid) {
          ObUserInfo temp_user_info;
          if (OB_FAIL(temp_user_info.assign(*user_info))) {
            LOG_WARN("failed to assign user info", KR(ret));
          } else if (OB_FALSE_IT(temp_user_info.set_passwd(user_info->get_old_password_str()))) {
          } else if (OB_FALSE_IT(temp_user_info.set_password_last_changed(user_info->get_old_password_start_time()))) {
          } else if (OB_FAIL(verify_user_password_authentication(&temp_user_info,
                                                                 login_info,
                                                                 compat_mode,
                                                                 s_priv.tenant_id_,
                                                                 pwd_match))) {
            LOG_WARN("Failed to verify user password authentication", K(login_info), KR(ret));
          } else if (pwd_match) {
            // do nothing
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (user_info->get_is_locked()
            && !sql::ObOraSysChecker::is_super_user(user_info->get_user_id())) {
          if (pwd_match) {
            s_priv.user_id_ = user_info->get_user_id();
          }
          ret = OB_ERR_USER_IS_LOCKED;
          LOG_WARN("User is locked", KR(ret));
        } else if (!pwd_match) {
          user_info = nullptr;
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
      if (OB_SUCC(ret) && compat_mode == lib::Worker::CompatMode::ORACLE) {
        if (!login_info.proxied_user_name_.empty()) {
          if (OB_FAIL(get_user_info(s_priv.tenant_id_, login_info.proxied_user_name_,
                                    proxied_users_info))) {
            LOG_WARN("get user info failed", KR(ret), K(s_priv.tenant_id_), K(login_info));
          } else if (proxied_users_info.count() <= 0) {
            ret = OB_PASSWORD_WRONG;
            LOG_WARN("proxy user not existed", K(ret));
          } else if (OB_ISNULL(proxied_user_info = proxied_users_info.at(0))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected error", K(ret));
          } else {
            pwd_match = false;
            for (int64_t i = 0; OB_SUCC(ret) && !pwd_match && i < proxied_user_info->get_proxied_user_info_cnt(); i++) {
              const ObProxyInfo *proxied_info = proxied_user_info->get_proxied_user_info_by_idx(i);
              if (OB_ISNULL(proxied_info)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unexpected error", K(ret));
              } else if (proxied_info->user_id_ == auth_user_id) {
                pwd_match = true;
                proxied_info_idx = i;
              }
            }
            if (OB_FAIL(ret)) {
            } else if (!pwd_match) {
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

        if (OB_SUCC(ret) && lib::Worker::CompatMode::ORACLE == compat_mode) {
          OZ (check_ora_restricted_session(s_priv, enable_role_id_array));
        }
      }
    }
  }
  return ret;
}

int ObSchemaGetterGuard::verify_user_password_authentication(
    const ObUserInfo *user_info,
    const ObUserLoginInfo &login_info,
    lib::Worker::CompatMode compat_mode,
    uint64_t tenant_id,
    bool &is_found)
{
  int ret = OB_SUCCESS;
  is_found = false;

  if (NULL == user_info) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("user info is null", K(login_info), KR(ret));
  } else if (user_info->is_role() && lib::Worker::CompatMode::ORACLE == compat_mode) {
    ret = OB_PASSWORD_WRONG;
    LOG_INFO("password error", "tenant_name", login_info.tenant_name_,
             "user_name", login_info.user_name_,
             "client_ip_", login_info.client_ip_, KR(ret));
  } else {
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
        // [mysql_native_password authentication]
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
        // [caching_sha2_password authentication]
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
                                                    tenant_id,
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
                       K(tenant_id),
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

int ObSchemaGetterGuard::try_caching_sha2_fast_auth_verify(const ObUserLoginInfo &login_info,
                                                           const common::ObString &host_name,
                                                           uint64_t tenant_id,
                                                           int64_t password_last_changed_timestamp,
                                                           bool &fast_auth_success_flag)
{
  int ret = OB_SUCCESS;
  fast_auth_success_flag = false;
  ObCachingSha2Handle cache_handle;
  ObString user_name = login_info.user_name_;
  int cache_ret = ObCachingSha2CacheMgr::get_instance().get_digest(user_name,
                                                                   host_name,
                                                                   tenant_id,
                                                                   password_last_changed_timestamp,
                                                                   cache_handle);
  if (OB_SUCCESS == cache_ret && OB_NOT_NULL(cache_handle.digest_)) {
    // Cache hit, try fast auth
    LOG_TRACE("caching_sha2_password: cache hit, attempting fast auth", K(user_name), K(host_name));
    ObString cached_digest(cache_handle.digest_->get_digest_len(), cache_handle.digest_->get_digest());
    if (OB_FAIL(ObSha256Crypt::verify_fast_auth_scramble(login_info.passwd_,
                                                         ObString(SCRAMBLE_LENGTH, login_info.scramble_str_.ptr()),
                                                         cached_digest,
                                                         fast_auth_success_flag))) {
      LOG_WARN("Failed to verify fast auth scramble", K(ret), K(login_info));
    }
  } else {
    // Cache miss, fast_auth_success_flag remains false
    if (OB_ENTRY_NOT_EXIST != cache_ret && OB_SUCCESS != cache_ret) {
      LOG_WARN("failed to get digest from cache", K(cache_ret), K(user_name), K(host_name));
      ret = cache_ret;
    }
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
