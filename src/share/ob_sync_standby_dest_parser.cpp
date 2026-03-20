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

#define USING_LOG_PREFIX SHARE

#include "share/ob_sync_standby_dest_parser.h"
#include "share/ob_log_restore_proxy.h"
#include "stdlib.h"
namespace oceanbase
{
namespace share
{
ObSyncStandbyDestStruct::ObSyncStandbyDestStruct()
  : restore_source_service_attr_(), net_timeout_(OB_DEFAULT_NET_TIMEOUT), health_check_time_(OB_DEFAULT_HEALTH_CHECK_TIME)
{
}

void ObSyncStandbyDestStruct::reset()
{
  restore_source_service_attr_.reset();
  net_timeout_ = OB_DEFAULT_NET_TIMEOUT;
  health_check_time_ = OB_DEFAULT_HEALTH_CHECK_TIME;
}

bool ObSyncStandbyDestStruct::is_valid() const
{
  return restore_source_service_attr_.is_valid() && net_timeout_ >= 0 && health_check_time_ >= 0;
}

int ObSyncStandbyDestStruct::assign(const ObSyncStandbyDestStruct &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    if (OB_FAIL(restore_source_service_attr_.assign(other.restore_source_service_attr_))) {
      LOG_WARN("failed to assign restore_source_service_attr_", KR(ret), K(other));
    } else {
      net_timeout_ = other.net_timeout_;
      health_check_time_ = other.health_check_time_;
    }
  }
  return ret;
}

int ObSyncStandbyDestStruct::parse_standby_dest_from_str(const ObString &standby_dest_str)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(parse_from_str(standby_dest_str, ","))) {
    LOG_WARN("failed to parse standby dest from str", KR(ret), K(standby_dest_str));
  }
  return ret;
}

int ObSyncStandbyDestStruct::do_parse_key_value(const char *key, const char *value, bool &not_exist)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(key) || OB_ISNULL(value)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(key), KP(value));
  } else if (0 == STRCASECMP(key, OB_STR_NET_TIMEOUT)) {
    if (OB_FAIL(set_service_net_timeout(value, true/*from_inner_table*/))) {
      LOG_WARN("failed to set service net timeout", KR(ret), K(key), K(value));
    }
  } else if (0 == STRCASECMP(key, OB_STR_HEALTH_CHECK_TIME)) {
    if (OB_FAIL(set_service_health_check_time(value, true/*from_inner_table*/))) {
      LOG_WARN("failed to set service health check time", KR(ret), K(key), K(value));
    }
  } else if (OB_FAIL(restore_source_service_attr_.do_parse_key_value(key, value, not_exist))) {
    LOG_WARN("failed to parse key value", KR(ret), K(key), K(value));
  }
  return ret;
}

int ObSyncStandbyDestStruct::gen_standby_dest_str(ObSqlString &str) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("sync standby dest is not valid", KR(ret), K(*this));
  } else if (OB_FAIL(restore_source_service_attr_.gen_service_attr_str(str))) {
    LOG_WARN("failed to gen service attr str", KR(ret), K(*this));
  } else if (OB_FAIL(str.append_fmt(",%s=%ld", OB_STR_NET_TIMEOUT, net_timeout_))) {
    LOG_WARN("failed to append net timeout", KR(ret), K(*this), K(net_timeout_));
  } else if (OB_FAIL(str.append_fmt(",%s=%ld", OB_STR_HEALTH_CHECK_TIME, health_check_time_))) {
    LOG_WARN("failed to append health check time", KR(ret), K(*this), K(health_check_time_));
  }
  return ret;
}

int ObSyncStandbyDestStruct::set_service_time_config_(
    const char *config_value,
    const bool from_inner_table,
    const char *config_name,
    int64_t &target_value)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == config_value || 0 == STRLEN(config_value))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid service time config", KR(ret), K(config_name), K(config_value));
  } else if (from_inner_table) {
    char *str_end = nullptr;
    target_value = ::strtol(config_value, &str_end, 0);
    if (OB_ISNULL(str_end) || '\0' != *str_end) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid service time config", KR(ret), K(config_name),
          KCSTRING(config_value), K(target_value), KCSTRING(str_end));
    }
  } else {
    bool valid = false;
    target_value = common::ObConfigTimeParser::get(config_value, valid);
    if (!valid) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid service time config", KR(ret), K(config_name), K(config_value));
    }
  }
  return ret;
}

int ObSyncStandbyDestStruct::set_service_net_timeout(const char *net_timeout, const bool from_inner_table)
{
  return set_service_time_config_(net_timeout, from_inner_table, OB_STR_NET_TIMEOUT, net_timeout_);
}

int ObSyncStandbyDestStruct::set_service_health_check_time(const char *health_check_time, const bool from_inner_table)
{
  return set_service_time_config_(
      health_check_time,
      from_inner_table,
      OB_STR_HEALTH_CHECK_TIME,
      health_check_time_);
}

const int64_t ObSyncStandbyDestParser::OB_MIN_USER_NET_TIMEOUT = 10_s;
const int64_t ObSyncStandbyDestParser::OB_MAX_USER_NET_TIMEOUT = 1200_s;

ObSyncStandbyDestParser::ObSyncStandbyDestParser(ObSyncStandbyDestStruct &sync_standby_dest)
  : ObServiceConfigParser(sync_standby_dest.restore_source_service_attr_),
    sync_standby_dest_struct_(sync_standby_dest)
{
}

int ObSyncStandbyDestParser::do_parse_token_(char *token, char *value)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(token) || OB_ISNULL(value)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(token), KP(value));
  } else if (0 == STRCASECMP(token, OB_STR_NET_TIMEOUT)) {
    if (OB_FAIL(do_parse_net_timeout_(token, value))) {
      LOG_WARN("failed to do parse net timeout", KR(ret), K(token), K(value));
    }
  } else if (0 == STRCASECMP(token, OB_STR_HEALTH_CHECK_TIME)) {
    if (OB_FAIL(do_parse_health_check_time_(token, value))) {
      LOG_WARN("failed to do parse health check time", KR(ret), K(token), K(value));
    }
  } else if (OB_FAIL(ObServiceConfigParser::do_parse_token_(token, value))) {
    LOG_WARN("failed to do parse service config token", KR(ret), K(token), K(value));
  }
  return ret;
}

int ObSyncStandbyDestParser::do_parse_net_timeout_(const common::ObString &name, const common::ObString &value)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(name.empty() || value.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(name), K(value));
  } else if (OB_FAIL(sync_standby_dest_struct_.set_service_net_timeout(value.ptr(),
          false/*from_inner_table*/))) {
    LOG_WARN("failed to set service net timeout", KR(ret), K(name), K(value));
  } else if (sync_standby_dest_struct_.get_service_net_timeout() < OB_MIN_USER_NET_TIMEOUT
      || sync_standby_dest_struct_.get_service_net_timeout() > OB_MAX_USER_NET_TIMEOUT) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("net_timeout is out of range", KR(ret), K(name), K(value),
        "net_timeout_us", sync_standby_dest_struct_.get_service_net_timeout(),
        K(OB_MIN_USER_NET_TIMEOUT), K(OB_MAX_USER_NET_TIMEOUT));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT,
        "invalid net_timeout value, net_timeout should be between 10s and 1200s");
  }
  return ret;
}

int ObSyncStandbyDestParser::do_parse_health_check_time_(const common::ObString &name, const common::ObString &value)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(name.empty() || value.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(name), K(value));
  } else if (OB_FAIL(sync_standby_dest_struct_.set_service_health_check_time(value.ptr(),
          false/*from_inner_table*/))) {
    LOG_WARN("failed to set service health check time", KR(ret), K(name), K(value));
  }
  if (OB_FAIL(ret)) {
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "set health_check_time config, please check the length and format of health_check_time");
  }
  return ret;
}

int ObSyncStandbyDestParser::check_sync_standby_dest_connectivity_(const uint64_t user_tenant_id)
{
  int ret = OB_SUCCESS;
  ObRestoreSourceServiceAttr &service_attr = sync_standby_dest_struct_.restore_source_service_attr_;
  if (OB_UNLIKELY(!is_user_tenant(user_tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid user tenant id", KR(ret), K(user_tenant_id));
  } else {
    SMART_VAR(ObLogRestoreProxyUtil, proxy) {
      if (OB_FAIL(service_attr.init_for_first_connection(user_tenant_id, false/*for_verify*/, proxy))) {
        LOG_WARN("failed to init for first connection", KR(ret), K(service_attr), K(user_tenant_id));
      }
    }
  }
  return ret;
}

int ObSyncStandbyDestParser::parse_from(const common::ObString &value, const uint64_t user_tenant_id,
   const ObCompatibilityMode &compat_mode)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObServiceConfigParser::parse_from(value))) {
    LOG_WARN("failed to parse from", KR(ret), K(value));
  } else if (FALSE_IT(sync_standby_dest_struct_.restore_source_service_attr_.user_.mode_ = compat_mode)) {
  } else if (OB_FAIL(check_sync_standby_dest_connectivity_(user_tenant_id))) {
    LOG_WARN("failed to check sync standby dest connectivity", KR(ret), K(user_tenant_id));
  }
  return ret;
}

} // namespace share
} // namespace oceanbase