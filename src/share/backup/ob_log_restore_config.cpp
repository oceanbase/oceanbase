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
#include "ob_backup_config.h"
#include "ob_log_restore_config.h"
#include "share/restore/ob_log_restore_source_mgr.h"  // ObLogRestoreSourceMgr
#include "share/ob_log_restore_proxy.h"  // ObLogRestoreProxyUtil

using namespace oceanbase;
using namespace share;
using namespace common;

int ObLogRestoreSourceLocationConfigParser::update_inner_config_table(common::ObISQLClient &trans)
{
  int ret = OB_SUCCESS;
  ObBackupDestMgr dest_mgr;
  ObLogRestoreSourceMgr restore_source_mgr;
  ObAllTenantInfo tenant_info;

  if (!type_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid type", KR(ret), KPC(this));
  } else if (OB_FAIL(restore_source_mgr.init(tenant_id_, &trans))) {
    LOG_WARN("failed to init restore_source_mgr", KR(ret), KPC(this));
  } else if (is_empty_) {
    if (OB_FAIL(restore_source_mgr.delete_source())) {
      LOG_WARN("failed to delete restore source", KR(ret), KPC(this));
    }
  } else {
    if (!archive_dest_.is_dest_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid restore source", KR(ret), KPC(this));
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "log_restore_source");
    } else if (OB_FAIL(archive_dest_.gen_path_config_items(config_items_))) {
      LOG_WARN("fail to gen archive config items", KR(ret), KPC(this));
    } else {
      ObString key_string = ObString::make_string(config_items_.at(0).key_.ptr());
      ObString path_string = ObString::make_string(OB_STR_PATH);
      ObString value_string = ObString::make_string(config_items_.at(0).value_.ptr());

      if (1 != config_items_.count()
          || path_string != key_string
          || config_items_.at(0).value_.empty()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid archive source", KR(ret), KPC(this));
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "set log_restore_source");
      } else if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(tenant_id_, &trans,
                                                                true /* for update */, tenant_info))) {
        LOG_WARN("failed to load tenant info", KR(ret), K_(tenant_id));
      } else if (OB_FAIL(restore_source_mgr.add_location_source(tenant_info.get_recovery_until_scn(),
                                                                value_string))) {
        LOG_WARN("failed to add log restore source", KR(ret), K(tenant_info), K(value_string), KPC(this));
      }
    }
  }

  return ret;
}

int ObLogRestoreSourceLocationConfigParser::check_before_update_inner_config(
    obrpc::ObSrvRpcProxy &rpc_proxy,
    common::ObISQLClient &trans)
{
  int ret = OB_SUCCESS;

  if (is_empty_) {
  } else if (!type_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid parser", KR(ret), KPC(this));
  } else {
    share::ObBackupStore backup_store;
    share::ObBackupFormatDesc desc;
    if (OB_FAIL(backup_store.init(archive_dest_.dest_))) {
      LOG_WARN("backup store init failed", K_(archive_dest_.dest));
    } else if (OB_FAIL(backup_store.read_format_file(desc))) {
      LOG_WARN("backup store read format file failed", K_(archive_dest_.dest));
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "access the log restore source location");
    } else if (OB_UNLIKELY(! desc.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("backup store desc is invalid");
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "access the log restore source location");
    } else if (GCONF.cluster_id == desc.cluster_id_ && tenant_id_ == desc.tenant_id_) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("set standby itself as log restore source is not allowed");
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "set standby itself as log restore source");
    }
  }
  //TODO (wenjinyu.wjy) need support access permission check
  //
  return ret;
}

int ObLogRestoreSourceLocationConfigParser::do_parse_sub_config_(const common::ObString &config_str)
{
  int ret = OB_SUCCESS;
  const char *target= nullptr;
  if (config_str.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty log restore source is not allowed", KR(ret), K(config_str));
  } else {
    char tmp_str[OB_MAX_BACKUP_DEST_LENGTH] = { 0 };
    char *token = nullptr;
    char *saveptr = nullptr;
    char *p_end = nullptr;
    if (OB_FAIL(databuff_printf(tmp_str, sizeof(tmp_str), "%.*s", config_str.length(), config_str.ptr()))) {
      LOG_WARN("fail to set config value", KR(ret), K(config_str));
    } else if (OB_ISNULL(token = ::STRTOK_R(tmp_str, "=", &saveptr))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to split config str", K(ret), KP(token));
    } else if (OB_FALSE_IT(str_tolower(token, strlen(token)))) {
    } else if (0 == STRCASECMP(token, OB_STR_LOCATION)) {
      if (OB_FAIL(do_parse_log_archive_dest_(token, saveptr))) {
        LOG_WARN("fail to do parse log archive dest", KR(ret), K(token), K(saveptr));
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("log restore source does not has this config", KR(ret), K(token));
    }
  }
  return ret;
}

int ObLogRestoreSourceServiceConfigParser::parse_from(const common::ObSqlString &value)
{
  int ret = OB_SUCCESS;
  char tmp_str[OB_MAX_BACKUP_DEST_LENGTH] = { 0 };
  char *token = nullptr;
  char *saveptr = nullptr;
  is_empty_ = false;

  if (value.empty()) {
    is_empty_ = true;
  } else if (value.length() > OB_MAX_BACKUP_DEST_LENGTH) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("config value is too long");
  } else if (OB_FAIL(databuff_printf(tmp_str, sizeof(tmp_str), "%.*s", static_cast<int>(value.length()), value.ptr()))) {
    LOG_WARN("fail to set config value", K(value));
  } else {
    token = tmp_str;
    for (char *str = token; OB_SUCC(ret); str = nullptr) {
      token = ::STRTOK_R(str, " ", &saveptr);
      if (nullptr == token) {
        break;
      } else if (OB_FAIL(do_parse_sub_config_(token))) {
        LOG_WARN("fail to do parse log restore source server sub config");
      }
    }
  }
  return ret;
}

int ObLogRestoreSourceServiceConfigParser::update_inner_config_table(common::ObISQLClient &trans)
{
  int ret = OB_SUCCESS;
  ObLogRestoreSourceMgr restore_source_mgr;
  ObAllTenantInfo tenant_info;

  if (!type_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid type", KPC(this));
  } else if (OB_FAIL(restore_source_mgr.init(tenant_id_, &trans))) {
    LOG_WARN("failed to init restore_source_mgr", KPC(this));
  } else if (is_empty_) {
    if (OB_FAIL(restore_source_mgr.delete_source())) {
      LOG_WARN("failed to delete restore source", KPC(this));
    }
  } else if (!service_attr_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid restore source", KPC(this));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "log_restore_source");
  } else if (OB_FAIL(service_attr_.gen_config_items(config_items_))) {
    LOG_WARN("fail to gen restore source service config items", KPC(this));
  } else {
    /*
    eg: 开源版本 "ip_list=127.0.0.1:1001;127.0.0.1:1002,USER=restore_user@primary_tenant,PASSWORD=xxxxxxx(密码),TENANT_ID=1002,CLUSTER_ID=10001,COMPATIBILITY_MODE=MYSQL,IS_ENCRYPTED=false"
      非开源版本 "ip_list=127.0.0.1:1001;127.0.0.1:1002,USER=restore_user@primary_tenant,PASSWORD=xxxxxxx(加密后密码),TENANT_ID=1002,CLUSTER_ID=10001,COMPATIBILITY_MODE=MYSQL,IS_ENCRYPTED=true"
    */
    char value_string[OB_MAX_BACKUP_DEST_LENGTH] = { 0 };

    if (config_items_.empty() || OB_MAX_RESTORE_SOURCE_SERVICE_CONFIG_LEN != config_items_.count()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid restore source", KPC(this));
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "log_restore_source");
    } else if (OB_FAIL(service_attr_.gen_service_attr_str(value_string, OB_MAX_BACKUP_DEST_LENGTH))) {
      LOG_WARN("failed gen service attr str", K_(tenant_id));
    } else if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(tenant_id_, &trans,
                                                            true /* for update */, tenant_info))) {
      LOG_WARN("failed to load tenant info", K_(tenant_id));
    } else if (OB_FAIL(restore_source_mgr.add_service_source(tenant_info.get_recovery_until_scn(),
                                                            value_string))) {
      LOG_WARN("failed to add log restore source", K(tenant_info), K(value_string), KPC(this));
    }
  }
  return ret;
}

int ObLogRestoreSourceServiceConfigParser::check_before_update_inner_config(obrpc::ObSrvRpcProxy &rpc_proxy, common::ObISQLClient &trans)
{
  int ret = OB_SUCCESS;
  ObCompatibilityMode compat_mode = ObCompatibilityMode::OCEANBASE_MODE;
  if (is_empty_) {
  } else if (!type_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid parser", KR(ret), KPC(this));
  } else if (OB_FAIL(check_before_update_inner_config(false /* for_verify */, compat_mode))) {
    LOG_WARN("fail to check before update inner config");
  }
  return ret;
}

int ObLogRestoreSourceServiceConfigParser::check_before_update_inner_config(
    const bool for_verify,
    ObCompatibilityMode &compat_mode)
{
  int ret = OB_SUCCESS;
  compat_mode = ObCompatibilityMode::OCEANBASE_MODE;
  bool source_is_self = false;

  SMART_VAR(ObLogRestoreProxyUtil, proxy) {
    if (is_empty_) {
    } else if (OB_FAIL(construct_restore_sql_proxy_(proxy))) {
      LOG_WARN("failed to construct restore sql proxy", KR(ret));
    } else if (!for_verify && OB_FAIL(service_attr_.check_restore_source_is_self_(source_is_self, tenant_id_))) {
      LOG_WARN("check restore source is self failed");
    } else if (source_is_self) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("set tenant itself as log restore source is not allowed");
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "set tenant itself as log restore source is");
    } else if (OB_FAIL(proxy.get_compatibility_mode(service_attr_.user_.tenant_id_, service_attr_.user_.mode_))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("get primary compatibility mode failed", K(tenant_id_), K(service_attr_.user_.tenant_id_));
    } else if (for_verify && OB_FAIL(proxy.check_begin_lsn(service_attr_.user_.tenant_id_))) {
      LOG_WARN("check_begin_lsn failed", K(tenant_id_), K(service_attr_.user_.tenant_id_));
    } else {
      compat_mode = service_attr_.user_.mode_;
      LOG_INFO("check_before_update_inner_config success", K(tenant_id_), K(service_attr_), K(compat_mode));
    }
  }
  return ret;
}

int ObLogRestoreSourceServiceConfigParser::construct_restore_sql_proxy_(ObLogRestoreProxyUtil &log_restore_proxy)
{
  int ret = OB_SUCCESS;
  char passwd[OB_MAX_PASSWORD_LENGTH + 1] = { 0 }; //unencrypted password
  ObSqlString user_and_tenant;
  if (is_empty_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else if (!type_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid parser", KPC(this));
  } else if (0 == STRLEN(service_attr_.encrypt_passwd_) ||
             0 == STRLEN(service_attr_.user_.user_name_) ||
             0 == STRLEN(service_attr_.user_.tenant_name_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to parse log restore source config, please check the config parameters");
    LOG_USER_ERROR(OB_INVALID_ARGUMENT,
        "parse log restore source config, please check the config parameters");
  } else if (OB_FAIL(service_attr_.get_password(passwd, sizeof(passwd)))) {
    LOG_WARN("get servcie attr password failed");
  } else if (OB_FAIL(service_attr_.get_user_str_(user_and_tenant))) {
    LOG_WARN("get user str failed", K(service_attr_.user_.user_name_),
             K(service_attr_.user_.tenant_name_));
  } else if (OB_FAIL(log_restore_proxy.try_init(tenant_id_ /*standby*/, service_attr_.addr_,
                                    user_and_tenant.ptr(), passwd))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("proxy connect to primary db failed", K(service_attr_.addr_),
             K(user_and_tenant));
  } else if (OB_FAIL(log_restore_proxy.get_tenant_id(service_attr_.user_.tenant_name_,
                                         service_attr_.user_.tenant_id_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get primary tenant id failed", K(tenant_id_),
             K(service_attr_.user_));
  } else if (OB_FAIL(log_restore_proxy.get_cluster_id(service_attr_.user_.tenant_id_,
                                          service_attr_.user_.cluster_id_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get primary cluster id failed", K(tenant_id_),
             K(service_attr_.user_.tenant_id_));
  }
  return ret;
}

int ObLogRestoreSourceServiceConfigParser::get_compatibility_mode(common::ObCompatibilityMode &compatibility_mode)
{
  int ret = OB_SUCCESS;
  compatibility_mode = OCEANBASE_MODE;
  if (is_empty_) {
  } else if (!type_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid parser", KPC(this));
  } else if (!service_attr_.user_.is_valid()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KPC(this));
  } else {
    compatibility_mode = service_attr_.user_.mode_;
  }
  return ret;
}

int ObLogRestoreSourceServiceConfigParser::
    get_primary_server_addr(const common::ObSqlString &value,
                            uint64_t &primary_tenant_id,
                            uint64_t &primary_cluster_id,
                            ObIArray<common::ObAddr> &addr_list) {
  int ret = OB_SUCCESS;
  if (OB_FAIL(value.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(value));
  } else if (OB_FAIL(parse_from(value))) {
    LOG_WARN("failed to parse from value", KR(ret), K(value));
  } else {
    SMART_VAR(ObLogRestoreProxyUtil, proxy) {
      if (OB_FAIL(construct_restore_sql_proxy_(proxy))) {
        LOG_WARN("failed to construct restore sql proxy", KR(ret));
      } else if (OB_FAIL(proxy.get_server_addr(service_attr_.user_.tenant_id_, addr_list))) {
        LOG_WARN("failed to get server addr", KR(ret), K(tenant_id_), K(addr_list), K(service_attr_));
      } else if (OB_UNLIKELY(0 >= addr_list.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("addr list is empty", K(primary_tenant_id), K(value));
      } else {
        primary_tenant_id = service_attr_.user_.tenant_id_;
        primary_cluster_id = service_attr_.user_.cluster_id_;
      }
      LOG_INFO("get primary server info", K(primary_tenant_id), K(primary_cluster_id), K(addr_list));
    }
  }
  return ret;
}

int ObLogRestoreSourceServiceConfigParser::do_parse_sub_config_(const common::ObString &config_str)
{
  int ret = OB_SUCCESS;
  if (config_str.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty log restore source sub config is not allowed", K(config_str));
  } else {
    char tmp_str[OB_MAX_BACKUP_DEST_LENGTH] = { 0 };
    char *token = nullptr;
    char *saveptr = nullptr;
    if (OB_FAIL(databuff_printf(tmp_str, sizeof(tmp_str), "%.*s", config_str.length(), config_str.ptr()))) {
      LOG_WARN("fail to set config value", K(config_str));
    } else if (OB_ISNULL(token = ::STRTOK_R(tmp_str, "=", &saveptr))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to split config str", KP(token));
    } else if (OB_FALSE_IT(str_tolower(token, strlen(token)))) {
    } else if (0 == STRCASECMP(token, OB_STR_SERVICE)) {
      if (OB_FAIL(do_parse_restore_service_host_(token, saveptr))) {
        LOG_WARN("fail to do parse restore service host", K(token), K(saveptr));
      }
    } else if (0 == STRCASECMP(token, OB_STR_USER)) {
      if (OB_FAIL(do_parse_restore_service_user_(token, saveptr))) {
        LOG_WARN("fail to do parse restore service user", K(token), K(saveptr));
      }
    } else if (0 == STRCASECMP(token, OB_STR_PASSWORD)) {
      if (OB_FAIL(do_parse_restore_service_passwd_(token, saveptr))) {
        LOG_WARN("fail to do parse restore service passwd", K(token), K(saveptr));
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("log restore source does not has this config", K(token));
    }
  }

  return ret;
}

int ObLogRestoreSourceServiceConfigParser::do_parse_restore_service_host_(const common::ObString &name, const
common::ObString &value)
{
  int ret = OB_SUCCESS;
  if (name.empty() || value.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid log restore source service host config", K(name), K(value));
  } else if (OB_FAIL(service_attr_.parse_ip_port_from_str(value.ptr(), ";" /*delimiter*/))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("parse restore source service host failed", K(name), K(value));
  }
  if (OB_FAIL(ret)) {
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "set ip list config, please check the length and format of ip list");
  }
  return ret;
}

int ObLogRestoreSourceServiceConfigParser::do_parse_restore_service_user_(const common::ObString &name, const
common::ObString &value)
{
  int ret = OB_SUCCESS;
  if (name.empty() || value.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid log restore source service user config", K(name), K(value));
  } else {
    char tmp_str[OB_MAX_BACKUP_DEST_LENGTH] = { 0 };
    char *user_token = nullptr;
    char *tenant_token = nullptr;
    if (OB_FAIL(databuff_printf(tmp_str, sizeof(tmp_str), "%.*s", value.length(), value.ptr()))) {
      LOG_WARN("fail to set user config value", K(value));
    } else if (OB_FAIL(service_attr_.set_service_user_config(tmp_str))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("parse restore source service user failed", K(name), K(value));
    }
  }
  if (OB_FAIL(ret)) {
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "set user config, please check the length and format of username, tenant name");
  }
  return ret;
}

int ObLogRestoreSourceServiceConfigParser::do_parse_restore_service_passwd_(const common::ObString &name, const
common::ObString &value)
{
  int ret = OB_SUCCESS;
  if (name.empty() || value.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid log restore source service password config", K(name), K(value));
  } else if (OB_FAIL(service_attr_.set_service_passwd_to_encrypt(value.ptr()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("parse restore source service password failed", K(name), K(value));
  }
  if (OB_FAIL(ret)) {
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "set password config, please check the length and format of password");
  }
  return ret;
}
