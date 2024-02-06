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
#include "lib/utility/ob_defer.h"
#include "share/backup/ob_backup_config.h"
#include "ob_log_restore_struct.h"


using namespace oceanbase;
using namespace common;
using namespace share;

ObRestoreSourceServiceUser::ObRestoreSourceServiceUser()
  : user_name_(),
    tenant_name_(),
    mode_(ObCompatibilityMode::OCEANBASE_MODE),
    tenant_id_(OB_INVALID_TENANT_ID),
    cluster_id_(OB_INVALID_CLUSTER_ID)
{
  user_name_[0] = '\0';
  tenant_name_[0] = '\0';
}

void ObRestoreSourceServiceUser::reset()
{
  user_name_[0] = '\0';
  tenant_name_[0] = '\0';
  mode_ = ObCompatibilityMode::OCEANBASE_MODE;
  tenant_id_ = OB_INVALID_TENANT_ID;
  cluster_id_ = OB_INVALID_CLUSTER_ID;
}

bool ObRestoreSourceServiceUser::is_valid() const
{
  return STRLEN(user_name_) != 0
      && STRLEN(tenant_name_) != 0
      && (ObCompatibilityMode::OCEANBASE_MODE != mode_)
      && (tenant_id_ != OB_INVALID_TENANT_ID)
      && (cluster_id_ != OB_INVALID_CLUSTER_ID);
}

bool ObRestoreSourceServiceUser::operator== (const ObRestoreSourceServiceUser &other) const
{
  return (STRLEN(this->user_name_) == STRLEN(other.user_name_))
         && (STRLEN(this->tenant_name_) == STRLEN(other.tenant_name_))
         && (0 == STRCMP(this->user_name_, other.user_name_))
         && (0 == STRCMP(this->tenant_name_, other.tenant_name_))
         && this->mode_ == other.mode_
         && this->tenant_id_ == other.tenant_id_
         && this->cluster_id_ == other.cluster_id_;
}

int ObRestoreSourceServiceUser::assign(const ObRestoreSourceServiceUser &user)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!user.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(user));
  } else if (OB_FAIL(databuff_printf(user_name_, sizeof(user_name_), "%s", user.user_name_))) {
    LOG_WARN("user_name_ assign failed", K(user));
  } else if (OB_FAIL(databuff_printf(tenant_name_, sizeof(tenant_name_), "%s", user.tenant_name_))) {
    LOG_WARN("tenant_name_ assign failed", K(user));
  } else {
    mode_ = user.mode_;
    tenant_id_ = user.tenant_id_;
    cluster_id_ = user.cluster_id_;
  }
  return ret;
}

ObRestoreSourceServiceAttr::ObRestoreSourceServiceAttr()
  : addr_(),
    user_()
{
  encrypt_passwd_[0] = '\0';
}

void ObRestoreSourceServiceAttr::reset()
{
  addr_.reset();
  user_.reset();
  encrypt_passwd_[0] = '\0';
}

/*
   parse service attr from string
   eg: "ip_list=127.0.0.1:1001;127.0.0.1:1002,USER=restore_user@primary_tenant,PASSWORD=xxxxxx,TENANT_ID=1002,
   CLUSTER_ID=10001,COMPATIBILITY_MODE=MYSQL,IS_ENCRYPTED=true"
*/
int ObRestoreSourceServiceAttr::parse_service_attr_from_str(ObSqlString &value)
{
  int ret = OB_SUCCESS;
  char tmp_str[OB_MAX_BACKUP_DEST_LENGTH + 1] = { 0 };
  char *token = nullptr;
  char *saveptr = nullptr;

  if (OB_UNLIKELY(value.empty() || value.length() > OB_MAX_BACKUP_DEST_LENGTH)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("log restore source attr value is invalid");
  } else if (OB_FAIL(databuff_printf(tmp_str, sizeof(tmp_str), "%.*s", static_cast<int>(value.length()), value.ptr()))) {
    LOG_WARN("fail to print attr value", K(value));
  } else {
    token = tmp_str;
    for (char *str = token; OB_SUCC(ret); str = nullptr) {
      token = ::STRTOK_R(str, ",", &saveptr);
      if (nullptr == token) {
        break;
      } else if (OB_FAIL(do_parse_sub_service_attr(token))) {
        LOG_WARN("fail to parse service attr str", K(token));
      }
    }
  }
  return ret;
}

int ObRestoreSourceServiceAttr::do_parse_sub_service_attr(const char *sub_value)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(sub_value) || OB_UNLIKELY(0 == STRLEN(sub_value) || STRLEN(sub_value) > OB_MAX_BACKUP_DEST_LENGTH)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("log restore source service attr sub value is invalid", K(sub_value));
  } else {
    char tmp_str[OB_MAX_BACKUP_DEST_LENGTH + 1] = { 0 };
    char *token = nullptr;
    char *saveptr = nullptr;
    if (OB_FAIL(databuff_printf(tmp_str, sizeof(tmp_str), "%s", sub_value))) {
      LOG_WARN("fail to print sub_value", K(sub_value));
    } else if (OB_ISNULL(token = ::STRTOK_R(tmp_str, "=", &saveptr))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("fail to split sub_value str", K(token), KP(tmp_str));
    } else if (OB_FALSE_IT(str_tolower(token, strlen(token)))) {
    } else if (0 == STRCASECMP(token, OB_STR_IP_LIST)) {
      if (OB_FAIL(parse_ip_port_from_str(saveptr, ";"/*delimiter*/))) {
        LOG_WARN("fail to parse ip list from str", K(token), K(saveptr));
      }
    } else if (0 == STRCASECMP(token, OB_STR_USER)) {
      if (OB_FAIL(set_service_user_config(saveptr))) {
        LOG_WARN("fail to set restore service user and tenant", K(token), K(saveptr));
      }
    } else if (0 == STRCASECMP(token, OB_STR_TENANT_ID)) {
      if (OB_FAIL(set_service_tenant_id(saveptr))) {
        LOG_WARN("fail to set restore service user and tenant", K(token), K(saveptr));
      }
    } else if (0 == STRCASECMP(token, OB_STR_CLUSTER_ID)) {
      if (OB_FAIL(set_service_cluster_id(saveptr))) {
        LOG_WARN("fail to set restore service user and tenant", K(token), K(saveptr));
      }
    } else if (0 == STRCASECMP(token, OB_COMPATIBILITY_MODE)) {
      if (OB_FAIL(set_service_compatibility_mode(saveptr))) {
        LOG_WARN("fail to set restore service compatibility mode", K(token), K(saveptr));
      }
    } else if (0 == STRCASECMP(token, OB_STR_PASSWORD)) {
      if (OB_FAIL(set_service_passwd_no_encrypt(saveptr))) {
        LOG_WARN("fail to set restore service passwd", K(token), K(saveptr));
      }
    } else if (0 == STRCASECMP(token, OB_STR_IS_ENCRYPTED)) {
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("log restore source service do not have this config", K(token));
    }
  }
  return ret;
}

int ObRestoreSourceServiceAttr::set_service_user_config(const char *user_tenant)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(user_tenant) || OB_UNLIKELY(0 == STRLEN(user_tenant)
      || STRLEN(user_tenant) > OB_MAX_RESTORE_USER_AND_TENANT_LEN)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("log restore source service user is invalid");
  } else {
    char *user_name = nullptr;
    char *tenant_name = nullptr;
    char tmp_str[OB_MAX_RESTORE_USER_AND_TENANT_LEN + 1] = { 0 };
    if (OB_FAIL(databuff_printf(tmp_str, sizeof(tmp_str), "%s", user_tenant))) {
      LOG_WARN("fail to print user_tenant", K(user_tenant));
    } else if (OB_ISNULL(user_name = ::STRTOK_R(tmp_str, "@", &tenant_name))) {
      LOG_WARN("fail to split user_tenant", K(tmp_str));
    } else if (OB_FAIL(set_service_user(user_name, tenant_name))) {
      LOG_WARN("fail to set service user", K(user_name), K(tenant_name));
    }
  }
  return ret;
}

int ObRestoreSourceServiceAttr::set_service_user(const char *user, const char *tenant)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(user) || OB_ISNULL(tenant) || OB_UNLIKELY(0 == STRLEN(user) || 0 == STRLEN(tenant)
      || STRLEN(user) > OB_MAX_USER_NAME_LENGTH || STRLEN(tenant) > OB_MAX_ORIGINAL_NANE_LENGTH)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("log restore source service user name or tenant name is invalid");
  } else if (OB_FAIL(databuff_printf(user_.user_name_, sizeof(user_.user_name_), "%s", user))) {
    LOG_WARN("fail to print user name", K(user));
  } else if (OB_FAIL(databuff_printf(user_.tenant_name_, sizeof(user_.tenant_name_), "%s", tenant))) {
    LOG_WARN("fail to print tenant name", K(tenant));
  }
  LOG_DEBUG("set service user", K(user), K(tenant), K(user_));
  return ret;
}

int ObRestoreSourceServiceAttr::set_service_tenant_id(const char *tenant_id_str)
{
  int ret = OB_SUCCESS;
  char *p_end = nullptr;
  if (OB_FAIL(ob_strtoull(tenant_id_str, p_end, user_.tenant_id_))) {
    LOG_WARN("fail to set service tenant id from string", K(tenant_id_str));
  }
  return ret;
}

int ObRestoreSourceServiceAttr::set_service_cluster_id(const char *cluster_id_str)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ob_atoll(cluster_id_str, user_.cluster_id_))) {
    LOG_WARN("fail to set service cluster id from string", K(cluster_id_str));
  }
  return ret;
}

int ObRestoreSourceServiceAttr::set_service_compatibility_mode(const char *compat_mode)
{
  int ret = OB_SUCCESS;
  char token[OB_MAX_COMPAT_MODE_STR_LEN + 1] = { 0 };
  if (OB_ISNULL(compat_mode) || OB_UNLIKELY(0 == STRLEN(compat_mode)
      || STRLEN(compat_mode) > OB_MAX_COMPAT_MODE_STR_LEN)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("log restore source service compat_mode is invalid");
  } else if (OB_FAIL(databuff_printf(token, sizeof(token), "%s", compat_mode))) {
    LOG_WARN("fail to print compat mode", K(compat_mode));
  } else if (OB_FALSE_IT(str_toupper(token, STRLEN(token)))) {
  } else if ((0 == STRCASECMP(token, "MYSQL"))) {
    user_.mode_ = ObCompatibilityMode::MYSQL_MODE;
  } else if ((0 == STRCASECMP(token, "ORACLE"))) {
    user_.mode_ = ObCompatibilityMode::ORACLE_MODE;
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid compatibility mode", K(compat_mode), K(ret));
  }
  return ret;
}

int ObRestoreSourceServiceAttr::set_service_passwd_to_encrypt(const char *passwd)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(passwd) || OB_UNLIKELY(0 == STRLEN(passwd) || STRLEN(passwd) > OB_MAX_PASSWORD_LENGTH)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument");
#ifdef OB_BUILD_TDE_SECURITY
  } else if (OB_FAIL(set_encrypt_password_key_(passwd))) {
    LOG_WARN("encrypt password failed");
  }
#else
  } else if (OB_FAIL(databuff_printf(encrypt_passwd_, sizeof(encrypt_passwd_), "%s", passwd))) {
    LOG_WARN("fail to print encrypt password");
  }
#endif
  LOG_INFO("set service password success");
  return ret;
}

int ObRestoreSourceServiceAttr::set_service_passwd_no_encrypt(const char *passwd)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(passwd) || OB_UNLIKELY(0 == STRLEN(passwd) || STRLEN(passwd) > OB_MAX_PASSWORD_LENGTH)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument");
  } else if (OB_FAIL(databuff_printf(encrypt_passwd_, sizeof(encrypt_passwd_), "%s", passwd))) {
    LOG_WARN("fail to print encrypt password");
  }
  return ret;
}

// 127.0.0.1:1000;127.0.0.1:1001;127.0.0.1:1002 ==> 127.0.0.1:1000 127.0.0.1:1001 127.0.0.1:1002
int ObRestoreSourceServiceAttr::parse_ip_port_from_str(const char *ip_list, const char *delimiter)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ip_list) || OB_ISNULL(delimiter) || OB_UNLIKELY(STRLEN(ip_list) > OB_MAX_RESTORE_SOURCE_IP_LIST_LEN)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("log restore source service ip list is invalid");
  }

  char tmp_str[OB_MAX_RESTORE_SOURCE_IP_LIST_LEN + 1] = { 0 };
  char *token = nullptr;
  char *saveptr = nullptr;
  if (OB_FAIL(databuff_printf(tmp_str, sizeof(tmp_str), "%s", ip_list))) {
    LOG_WARN("fail to get ip list", K(ip_list));
  } else {
    token = tmp_str;
    for (char *str = token; OB_SUCC(ret); str = nullptr) {
      token = ::STRTOK_R(str, delimiter, &saveptr);
      if (nullptr == token) {
        break;
      } else {
        ObAddr addr;
        if (OB_FAIL(addr.parse_from_string(ObString(token)))) {
          LOG_WARN("fail to parse addr", K(addr), K(token));
        } else if (!addr.is_valid()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("service addr is invalid", K(addr), K(ip_list));
        } else if (OB_FAIL(addr_.push_back(addr))){
          LOG_WARN("fail to push addr", K(addr));
        }
      }
    }
  }
  return ret;
}

bool ObRestoreSourceServiceAttr::is_valid() const
{
  return service_user_is_valid()
      && service_host_is_valid()
      && service_password_is_valid();
}

bool ObRestoreSourceServiceAttr::service_user_is_valid() const
{
  return user_.is_valid();
}

bool ObRestoreSourceServiceAttr::service_host_is_valid() const
{
   return !addr_.empty();
}

bool ObRestoreSourceServiceAttr::service_password_is_valid() const
{
   return strlen(encrypt_passwd_) != 0;
}

int ObRestoreSourceServiceAttr::gen_config_items(common::ObIArray<BackupConfigItemPair> &items) const
{
  int ret = OB_SUCCESS;
  BackupConfigItemPair config;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguement", KPC(this));
  }

  ObSqlString tmp;
  // gen ip list config
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(config.key_.assign(OB_STR_IP_LIST))) {
    LOG_WARN("failed to assign ip list key");
  } else if (OB_FAIL(get_ip_list_str_(tmp))) {
    LOG_WARN("failed to get ip list str");
  } else if (OB_FAIL(config.value_.assign(tmp.ptr()))) {
    LOG_WARN("failed to assign ip list value");
  } else if(OB_FAIL(items.push_back(config))) {
     LOG_WARN("failed to push service source attr config", K(config));
  }

  // gen user config
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(config.key_.assign(OB_STR_USER))) {
    LOG_WARN("failed to assign user key");
  } else if (OB_FAIL(get_user_str_(tmp))) {
    LOG_WARN("failed to get user str");
  } else if (OB_FAIL(config.value_.assign(tmp.ptr()))) {
    LOG_WARN("failed to assign user value");
  } else if (OB_FAIL(items.push_back(config))) {
    LOG_WARN("failed to push service source attr config", K(config));
  }

  // gen password config
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(config.key_.assign(OB_STR_PASSWORD))) {
    LOG_WARN("failed to assign password key");
  } else if (OB_FAIL(get_password_str_(tmp))) {
    LOG_WARN("failed to get password str");
  } else if (OB_FAIL(config.value_.assign(tmp.ptr()))) {
    LOG_WARN("failed to assign password value");
  } else if (OB_FAIL(items.push_back(config))) {
    LOG_WARN("failed to push service source attr config", K(config));
  }

  // gen tenant id
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(config.key_.assign(OB_STR_TENANT_ID))) {
    LOG_WARN("failed to assign tenant id key");
  } else if (OB_FAIL(get_tenant_id_str_(tmp))) {
    LOG_WARN("failed to get tenant id str");
  } else if (OB_FAIL(config.value_.assign(tmp.ptr()))) {
    LOG_WARN("failed to assign tenant id value");
  } else if (OB_FAIL(items.push_back(config))) {
    LOG_WARN("failed to push service source attr config", K(config));
  }

  // gen cluster id
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(config.key_.assign(OB_STR_CLUSTER_ID))) {
    LOG_WARN("failed to assign cluster id key");
  } else if (OB_FAIL(get_cluster_id_str_(tmp))) {
    LOG_WARN("failed to get cluster id str");
  } else if (OB_FAIL(config.value_.assign(tmp.ptr()))) {
    LOG_WARN("failed to assign cluster id value");
  } else if (OB_FAIL(items.push_back(config))) {
    LOG_WARN("failed to push service source attr config", K(config));
  }

  // gen compatibility mode
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(config.key_.assign(OB_COMPATIBILITY_MODE))) {
    LOG_WARN("failed to assign compatibility mode");
  } else if (OB_FAIL(get_compatibility_mode_str_(tmp))) {
    LOG_WARN("failed to get compatibility mode str");
  } else if (OB_FAIL(config.value_.assign(tmp.ptr()))) {
    LOG_WARN("failed to assign compatibility mode value");
  } else if (OB_FAIL(items.push_back(config))) {
    LOG_WARN("failed to push service source attr config", K(config));
  }

  // gen is encrypted
   if (OB_FAIL(ret)) {
  } else if (OB_FAIL(config.key_.assign(OB_STR_IS_ENCRYPTED))) {
    LOG_WARN("failed to assign encrypted key");
  } else if (OB_FAIL(get_is_encrypted_str_(tmp))) {
    LOG_WARN("failed to get is encrypted str");
  } else if (OB_FAIL(config.value_.assign(tmp.ptr()))) {
    LOG_WARN("failed to assign encrypted value");
  } else if (OB_FAIL(items.push_back(config))) {
    LOG_WARN("failed to push service source attr config", K(config));
  }

  return ret;
}

int ObRestoreSourceServiceAttr::gen_service_attr_str(char *buf, const int64_t buf_size) const
{
  int ret = OB_SUCCESS;
  ObSqlString str;
  ObSqlString ip_str;
  ObSqlString user_str;
  ObSqlString passwd_str;
  ObSqlString compat_str;
  ObSqlString is_encrypted_str;

  if (OB_UNLIKELY(!is_valid() || OB_ISNULL(buf) || buf_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid service attr argument", KP(buf), K(buf_size));
  } else if (OB_FAIL(get_ip_list_str_(ip_str))) {
    LOG_WARN("get ip list str failed");
  } else if (OB_FAIL(get_user_str_(user_str))) {
    LOG_WARN("get user str failed");
  } else if (OB_FAIL(get_password_str_(passwd_str))) {
    LOG_WARN("get password str failed");
  } else if (OB_FAIL(get_compatibility_mode_str_(compat_str))) {
    LOG_WARN("get compatibility mode str failed");
  } else if (OB_FAIL(get_is_encrypted_str_(is_encrypted_str))) {
    LOG_WARN("get encrypted str failed");
  } else if (OB_FAIL(str.assign_fmt("IP_LIST=%s,USER=%s,PASSWORD=%s,TENANT_ID=%ld,CLUSTER_ID=%ld,COMPATIBILITY_MODE=%s,IS_ENCRYPTED=%s",
    ip_str.ptr(), user_str.ptr(), passwd_str.ptr(), user_.tenant_id_, user_.cluster_id_, compat_str.ptr(), is_encrypted_str.ptr()))) {
    LOG_WARN("fail to assign str", K(compat_str), K(is_encrypted_str));
  } else if (OB_FAIL(databuff_printf(buf, buf_size, "%.*s", static_cast<int>(str.length()), str.ptr()))) {
    LOG_WARN("fail to print str", K(str));
  }
  return ret;
}

int ObRestoreSourceServiceAttr::get_ip_list_str_(char *buf, const int64_t buf_size) const
{
  int ret = OB_SUCCESS;
  ObSqlString str;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ip list argument", KP(buf), K(buf_size));
  } else if (OB_FAIL(get_ip_list_str_(str))) {
    LOG_WARN("get ip list str failed");
  } else if (OB_FAIL(databuff_printf(buf, buf_size, "%.*s", static_cast<int>(str.length()), str.ptr()))) {
    LOG_WARN("fail to print str", K(str));
  }
  return ret;
}

int ObRestoreSourceServiceAttr::get_ip_list_str_(ObSqlString &str) const
{
  int ret = OB_SUCCESS;

  ARRAY_FOREACH_N(addr_, idx, cnt) {
    char ip_str[MAX_IP_PORT_LENGTH] = { 0 };
    const ObAddr ip = addr_.at(idx);
    if (OB_FAIL(ip.ip_port_to_string(ip_str, sizeof(ip_str)))) {
      LOG_WARN("fail to convert ip port to string", K(ip), K(ip_str));
    } else {
      if (0 == idx && (OB_FAIL(str.assign_fmt("%s", ip_str)))) {
        LOG_WARN("fail to assign ip str", K(str) ,K(ip), K(ip_str));
      } else if ( 0 != idx && OB_FAIL(str.append_fmt(";%s", ip_str))) {
        LOG_WARN("fail to append ip str", K(str), K(ip), K(ip_str));
      }
    }
  }
  LOG_DEBUG("get ip list str", K(str));
  return ret;
}

int ObRestoreSourceServiceAttr::get_user_str_(char *buf, const int64_t buf_size) const
{
  int ret = OB_SUCCESS;
  ObSqlString res_str;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid user str argument", KP(buf), K(buf_size));
  } else if (OB_FAIL(get_user_str_(res_str))) {
    LOG_WARN("fail to get user str");
  } else if (OB_FAIL(databuff_printf(buf, buf_size, "%.*s", static_cast<int>(res_str.length()), res_str.ptr()))) {
    LOG_WARN("fail to print str");
  }
  return ret;
}

int ObRestoreSourceServiceAttr::get_user_str_(ObSqlString &user_str) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(user_str.assign(user_.user_name_))) {
    LOG_WARN("fail to assign user name" ,K(user_.user_name_));
  } else if (OB_FAIL(user_str.append_fmt("@%s",user_.tenant_name_))) {
    LOG_WARN("fail to assign tenant name", K(user_.user_name_));
  }
  return ret;
}

int ObRestoreSourceServiceAttr::get_password_str_(char *buf, const int64_t buf_size) const
{
  int ret = OB_SUCCESS;
  ObSqlString passwd_str;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid password str argument", KP(buf), K(buf_size));
  } else if (OB_FAIL(get_password_str_(passwd_str))) {
    LOG_WARN("fail to get password str");
  } else if (OB_FAIL(databuff_printf(buf, buf_size, "%.*s", static_cast<int>(passwd_str.length()), passwd_str.ptr()))) {
    LOG_WARN("fail to print str");
  }
  return ret;
}

int ObRestoreSourceServiceAttr::get_password_str_(ObSqlString &passwd_str) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(passwd_str.assign(encrypt_passwd_))) {
     LOG_WARN("fail to assign password");
  }
  return ret;
}

int ObRestoreSourceServiceAttr::get_tenant_id_str_(char *buf, const int64_t buf_size) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id str argument", KP(buf), K(buf_size));
  } else if (OB_FAIL(databuff_printf(buf, buf_size, "%ld", user_.tenant_id_))) {
    LOG_WARN("fail to print tenant id str", K(user_.tenant_id_));
  }
  return ret;
}

int ObRestoreSourceServiceAttr::get_tenant_id_str_(ObSqlString &tenant_str) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(tenant_str.assign_fmt("%ld", user_.tenant_id_))) {
    LOG_WARN("fail to assign tenant id str", K(user_.tenant_id_));
  }
  return ret;
}

int ObRestoreSourceServiceAttr::get_cluster_id_str_(char *buf, const int64_t buf_size) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid cluster id str argument", KP(buf), K(buf_size));
  } else if (OB_FAIL(databuff_printf(buf, buf_size, "%ld", user_.cluster_id_))) {
    LOG_WARN("fail to print cluster id str", K(user_.cluster_id_));
  }
  return ret;
}

int ObRestoreSourceServiceAttr::get_cluster_id_str_(ObSqlString &cluster_str) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(cluster_str.assign_fmt("%ld", user_.cluster_id_))) {
    LOG_WARN("fail to assign cluster id str", K(user_.cluster_id_));
  }
  return ret;
}

int ObRestoreSourceServiceAttr::get_compatibility_mode_str_(char *buf, const int64_t buf_size) const
{
  int ret = OB_SUCCESS;
  ObSqlString tmp_str;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid compatibilidy mode str argument", KP(buf), K(buf_size));
  } else if (OB_FAIL(get_compatibility_mode_str_(tmp_str))) {
    LOG_WARN("fail to get compatibility mode str");
  } else if (OB_FAIL(databuff_printf(buf, buf_size, "%.*s", static_cast<int>(tmp_str.length()), tmp_str.ptr()))) {
    LOG_WARN("fail to print compatibility mode str", K(tmp_str));
  }
  return ret;
}

int ObRestoreSourceServiceAttr::get_compatibility_mode_str_(ObSqlString &compatibility_str) const
{
  int ret = OB_SUCCESS;
  const char *compat_mode = "INVALID";
  if (ObCompatibilityMode::MYSQL_MODE == user_.mode_) {
    compat_mode = "MYSQL";
  } else if (ObCompatibilityMode::ORACLE_MODE == user_.mode_) {
    compat_mode = "ORACLE";
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("compatibility mode is invalid", K(user_.mode_));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(compatibility_str.assign_fmt("%s", compat_mode))) {
    LOG_WARN("fail to assign compatibility mode", K(compat_mode));
  }
  return ret;
}

int ObRestoreSourceServiceAttr::get_is_encrypted_str_(char *buf, const int64_t buf_size) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid encrypted str argument", KP(buf), K(buf_size));
#ifdef OB_BUILD_TDE_SECURITY
  } else if (OB_FAIL(databuff_printf(buf, buf_size, "%s", "true"))) {
    LOG_WARN("fail to print is_encrypted str");
  }
#else
  } else if (OB_FAIL(databuff_printf(buf, buf_size, "%s", "false"))) {
    LOG_WARN("fail to print encrypted str");
  }
#endif
  return ret;
}

int ObRestoreSourceServiceAttr::get_is_encrypted_str_(ObSqlString &encrypted_str) const
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_TDE_SECURITY
  if (OB_FAIL(encrypted_str.assign("true"))) {
    LOG_WARN("fail to print is_encrypted str");
  }
#else
  if (OB_FAIL(encrypted_str.assign("false"))) {
    LOG_WARN("fail to print str");
  }
#endif
  return ret;
}

#ifdef OB_BUILD_TDE_SECURITY
int ObRestoreSourceServiceAttr::set_encrypt_password_key_(const char *passwd)
{
  int ret = OB_SUCCESS;
  char encrypted_key[OB_MAX_BACKUP_ENCRYPTKEY_LENGTH] = { 0 };
  char serialize_buf[OB_MAX_BACKUP_SERIALIZEKEY_LENGTH] = { 0 };
  int64_t serialize_pos = 0;
  int64_t key_len = 0;

  if (OB_ISNULL(passwd) || OB_UNLIKELY(0 == STRLEN(passwd))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("password is empty, shouldn't encrypt");
  } else if (OB_FAIL(ObEncryptionUtil::encrypt_sys_data(OB_SYS_TENANT_ID, passwd, strlen(passwd), encrypted_key,
    OB_MAX_BACKUP_ENCRYPTKEY_LENGTH, key_len))) {
    LOG_WARN("failed to encrypt authorization key");
  } else if (OB_FAIL(hex_print(encrypted_key, key_len, serialize_buf, sizeof(serialize_buf), serialize_pos))) {
    LOG_WARN("failed to serialize encrypted key", K(encrypted_key));
  } else if (serialize_pos >= sizeof(serialize_buf) || serialize_pos >= sizeof(encrypt_passwd_)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("encode error", K(serialize_pos), K(sizeof(serialize_buf)), K(sizeof(encrypt_passwd_)));
  } else if (FALSE_IT(serialize_buf[serialize_pos] = '\0')) {
  } else if (OB_FAIL(databuff_printf(encrypt_passwd_, sizeof(encrypt_passwd_), "%s", serialize_buf))) {
    LOG_WARN("failed to get encrypted key", K(serialize_buf));
  }
  return ret;
}

int ObRestoreSourceServiceAttr::get_decrypt_password_key_(char *unencrypt_key, const int64_t buf_size) const
{
  int ret = OB_SUCCESS;
  int64_t key_len = 0;
  char deserialize_buf[OB_MAX_BACKUP_SERIALIZEKEY_LENGTH] = { 0 };
  char decrypt_key[OB_MAX_BACKUP_ACCESSKEY_LENGTH] = { 0 };
  int64_t buf_len = strlen(encrypt_passwd_);
  int64_t deserialize_size = 0;

  if (OB_ISNULL(unencrypt_key) || OB_UNLIKELY((0 == buf_len) || (buf_size <= 0) || (buf_size < OB_MAX_PASSWORD_LENGTH))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get decrypt password, parameter is invalid", K(ret));
  } else if (OB_FAIL(hex_to_cstr(encrypt_passwd_, buf_len,
      deserialize_buf, sizeof(deserialize_buf), deserialize_size))) {
    LOG_WARN("failed to get cstr from hex", K(buf_len), K(sizeof(deserialize_buf)));
  } else if (OB_FAIL(ObEncryptionUtil::decrypt_sys_data(OB_SYS_TENANT_ID, deserialize_buf, deserialize_size,
      decrypt_key, sizeof(decrypt_key), key_len))) {
    LOG_WARN("failed to decrypt authorization key", K(ret), K(deserialize_buf), K(deserialize_size));
  } else if (key_len >= sizeof(decrypt_key) || (key_len) >= buf_size) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("decrypt key size overflow", K(ret), K(key_len), K(sizeof(decrypt_key)), K(sizeof(unencrypt_key)));
  } else if (FALSE_IT(decrypt_key[key_len] = '\0')) {
  } else if (OB_FAIL(databuff_printf(unencrypt_key, buf_size, "%s", decrypt_key))) {
    LOG_WARN("failed to set unencrypt key", K(ret));
  }
  return ret;
}
#endif

int ObRestoreSourceServiceAttr::get_password(char *passwd, const int64_t buf_size) const
{
  int ret = OB_SUCCESS;
  char tmp_passwd[OB_MAX_PASSWORD_LENGTH + 1] = { 0 };
  if (OB_ISNULL(passwd) || OB_UNLIKELY((buf_size <= 0) || (buf_size < OB_MAX_PASSWORD_LENGTH))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid parameter when get password", K(passwd), K(STRLEN(passwd)));
#ifdef OB_BUILD_TDE_SECURITY
  } else if (OB_FAIL(get_decrypt_password_key_(tmp_passwd, sizeof(tmp_passwd)))) {
    LOG_WARN("failed to get decrypt password key");
  } else if (OB_FAIL(databuff_printf(passwd, buf_size, "%s", tmp_passwd))) {
    LOG_WARN("failed to print encrypt_passwd_ key", K(encrypt_passwd_));
  }
#else
  } else if (OB_FAIL(databuff_printf(passwd, buf_size, "%s", encrypt_passwd_))) {
    LOG_WARN("failed to print encrypt_passwd_ key", K(encrypt_passwd_));
  }
#endif
  return ret;
}

bool ObRestoreSourceServiceAttr::compare_addr_(common::ObArray<common::ObAddr> addr) const
{
  int bret = true;
  int ret = OB_SUCCESS;
  if (addr.size() != this->addr_.size()) {
    bret = false;
  } else {
    ARRAY_FOREACH_N(addr, idx, cnt) {
      bool tmp_cmp = false;
      ARRAY_FOREACH_N(this->addr_, idx1, cnt1) {
        if (addr.at(idx) == this->addr_.at(idx1)) {
          tmp_cmp = true;
        }
      }
      if (!tmp_cmp) {
        bret = false;
        break;
      }
    }
  }
  return bret;
}

int ObRestoreSourceServiceAttr::check_restore_source_is_self_(bool &is_self, uint64_t tenant_id) const
{
  int ret = OB_SUCCESS;
  is_self = false;
  int64_t curr_cluster_id = GCONF.cluster_id;

  // assume that cluster id is managed by
  if (tenant_id == user_.tenant_id_ && curr_cluster_id == user_.cluster_id_) {
    is_self = true;
    LOG_WARN("set standby itself as log restore source is not allowed");
  }
  LOG_INFO("check restore is self succ", K(tenant_id), K(user_.tenant_id_), K(curr_cluster_id), K(user_.cluster_id_));
  return ret;
}

bool ObRestoreSourceServiceAttr::operator==(const ObRestoreSourceServiceAttr &other) const
{
  return (this->user_ == other.user_)
         && (STRLEN(this->encrypt_passwd_) == STRLEN(other.encrypt_passwd_))
         && (0 == STRCMP(this->encrypt_passwd_, other.encrypt_passwd_))
         && compare_addr_(other.addr_);
}

int ObRestoreSourceServiceAttr::assign(const ObRestoreSourceServiceAttr &attr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!attr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(attr));
  } else if (FALSE_IT(addr_.reset())) {
  } else if (OB_FAIL(addr_.assign(attr.addr_))) {
    LOG_WARN("addr_ assign failed", K(attr));
  } else if (OB_FAIL(user_.assign(attr.user_))) {
    LOG_WARN("user_ assign failed", K(attr));
  } else if (OB_FAIL(databuff_printf(encrypt_passwd_, sizeof(attr.encrypt_passwd_), "%s", attr.encrypt_passwd_))) {
    LOG_WARN("passwd_ assign failed", K(attr));
  }
  return ret;
}

ObRestoreSourceLocationPrimaryAttr::ObRestoreSourceLocationPrimaryAttr()
  : tenant_id_(OB_INVALID_TENANT_ID),
    cluster_id_(OB_INVALID_CLUSTER_ID)
{
  location_.reset();
}

int ObRestoreSourceLocationPrimaryAttr::parse_location_attr_from_str(const common::ObString &value)
{
  int ret = OB_SUCCESS;
  char tmp_str[OB_MAX_BACKUP_DEST_LENGTH] = { 0 };
  char *token = nullptr;
  char *saveptr = nullptr;
  char *p_end = nullptr;

  if (value.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("location log restore source is empty");
  } else if (OB_FAIL(databuff_printf(tmp_str, sizeof(tmp_str), "%.*s", static_cast<int>(value.length()), value.ptr()))) {
    LOG_WARN("fail to set config value", K(ret), K(value));
  } else {
    token = tmp_str;
    for (char *str = token; OB_SUCC(ret); str = nullptr) {
      token = ::STRTOK_R(str, ",", &saveptr);
      if (nullptr == token) {
        break;
      } else if (OB_FAIL(do_parse_sub_config_(token))) {
        LOG_WARN("fail to do parse location restore source sub config from string", K(token));
      }
    }
  }
  return ret;
}

int ObRestoreSourceLocationPrimaryAttr::do_parse_sub_config_(const char *sub_value)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sub_value) || OB_UNLIKELY(0 == STRLEN(sub_value) || STRLEN(sub_value) > OB_MAX_BACKUP_DEST_LENGTH)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("location log restore source primary attr sub value is invalid", K(sub_value));
  } else {
    char tmp_str[OB_MAX_BACKUP_DEST_LENGTH + 1] = { 0 };
    char *token = nullptr;
    char *saveptr = nullptr;
    if (OB_FAIL(databuff_printf(tmp_str, sizeof(tmp_str), "%s", sub_value))) {
      LOG_WARN("fail to print sub_value", K(sub_value));
    } else if (OB_ISNULL(token = ::STRTOK_R(tmp_str, "=", &saveptr))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("fail to split sub_value str", K(token), KP(tmp_str));
    } else if (0 == STRCASECMP(token, OB_STR_LOCATION)) {
      if (OB_FAIL(set_location_path_(saveptr))) {
        LOG_WARN("fail to do parse location path", K(token), K(saveptr));
      }
    } else if (0 == STRCASECMP(token, OB_STR_CLUSTER_ID)) {
      if (OB_FAIL(set_location_cluster_id_(saveptr))) {
        LOG_WARN("fail to do parse location primary cluster id", K(token), K(saveptr));
      }
    } else if (0 == STRCASECMP(token, OB_STR_TENANT_ID)) {
      if (OB_FAIL(set_location_tenant_id_(saveptr))) {
        LOG_WARN("fail to do parse location primary tenant id", K(token), K(saveptr));
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("location log restore source does not support this sub config", K(token));
    }
  }
  return ret;
}

int ObRestoreSourceLocationPrimaryAttr::set_location_path_(const char *path)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(path) || OB_UNLIKELY(0 == STRLEN(path) || STRLEN(path) > OB_MAX_BACKUP_DEST_LENGTH)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("location path is invalid", K(path));
  } else if (OB_FAIL(location_.assign(path))) {
    LOG_WARN("fail to assign location path", K(path));
  }
  return ret;
}

int ObRestoreSourceLocationPrimaryAttr::set_location_cluster_id_(const char *cluster_id_str)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cluster_id_str)
    || OB_UNLIKELY(0 == STRLEN(cluster_id_str) || STRLEN(cluster_id_str) > OB_MAX_BACKUP_DEST_LENGTH)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("location cluster id str is invalid", K(cluster_id_str));
  } else if (OB_FAIL(ob_atoll(cluster_id_str, cluster_id_))) {
    LOG_WARN("fail to set location primary cluster id from string", K(cluster_id_str));
  }
  return ret;
}

int ObRestoreSourceLocationPrimaryAttr::set_location_tenant_id_(const char *tenant_id_str)
{
  int ret = OB_SUCCESS;
  char *p_end = nullptr;
  if (OB_ISNULL(tenant_id_str) || OB_UNLIKELY(0 == STRLEN(tenant_id_str))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("location tenant id is invalid", K(tenant_id_str));
  } else if (OB_FAIL(ob_strtoull(tenant_id_str, p_end, tenant_id_))) {
    LOG_WARN("fail to set location primary tenant id from string", K(tenant_id_str));
  }
  return ret;
}

bool ObRestoreSourceLocationPrimaryAttr::is_valid()
{
  return tenant_id_ != OB_INVALID_TENANT_ID
         && cluster_id_ != OB_INVALID_CLUSTER_ID
         && location_.is_valid();
}