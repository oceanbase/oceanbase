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

#include "share/config/ob_server_config.h"
#include "share/ob_cluster_version.h"
#include "share/ob_task_define.h"
#include "lib/ob_define.h"
#include "lib/string/ob_string.h"
#include "observer/omt/ob_tenant_config_mgr.h"
namespace oceanbase
{
namespace common
{
// for compat, cluster_version str("a.b.c"/"a.b.c.0") which is less than "3.2.3" will be parsed as "a.b.0.c".
static int parse_version(const char *str, uint64_t *versions, const int64_t size)
{
  int ret = OB_SUCCESS;
  int64_t i = 0;
  char buf[64] = {0};
  char *ptr = buf;
  const char *delim = ".";
  char *saveptr = NULL;
  char *token = NULL;
  const int64_t VERSION_ITEM = 4;
  const int64_t LAST_VERSION_ITEM = VERSION_ITEM - 1;

  if (NULL == str || NULL == versions || VERSION_ITEM > size) {
    COMMON_LOG(WARN, "invalid argument", KP(str), KP(versions), K(size));
    ret = OB_INVALID_ARGUMENT;
  } else if (strlen(str) >= sizeof(buf)) {
    COMMON_LOG(WARN, "invalid version", "version", str);
    ret = OB_INVALID_ARGUMENT;
  } else {
    strncpy(buf, str, sizeof(buf) - 1);
    for (i = 0; i < size; i++) {
      if (NULL != (token = strtok_r(ptr, delim, &saveptr))) {
        versions[i] = atoi(token);
      } else {
        break;
      }
      ptr = NULL;
    }
    if (VERSION_ITEM < i || LAST_VERSION_ITEM > i) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "invalid package version", KR(ret), "version", str, K(i), K(VERSION_ITEM));
    } else if (i == LAST_VERSION_ITEM) {
      // padding cluster_versions with 0
      versions[VERSION_ITEM - 1] = 0;
    }
    // cluster version str which is less than "3.2.3"
    if (OB_FAIL(ret)) {
    } else if (versions[ObClusterVersion::MAJOR_POS] < 3
               || (3 == versions[ObClusterVersion::MAJOR_POS]
                   && versions[ObClusterVersion::MINOR_POS] < 2)
               || (3 == versions[ObClusterVersion::MAJOR_POS]
                   && 2 == versions[ObClusterVersion::MINOR_POS]
                   && versions[ObClusterVersion::MAJOR_PATCH_POS] < 3)) {
      if (OB_UNLIKELY(0 != versions[ObClusterVersion::MINOR_PATCH_POS])) {
        ret = OB_INVALID_ARGUMENT;
        COMMON_LOG(WARN, "invalid package version", KR(ret), "version", str);
      } else {
        // convert "a.b.c" or "a.b.c.0" to "a.b.0.c"
        versions[ObClusterVersion::MINOR_PATCH_POS] = versions[ObClusterVersion::MAJOR_PATCH_POS];
        versions[ObClusterVersion::MAJOR_PATCH_POS] = 0;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(
        versions[ObClusterVersion::MAJOR_POS] > OB_VSN_MAJOR_MASK
        || versions[ObClusterVersion::MINOR_POS] > OB_VSN_MINOR_MASK
        || versions[ObClusterVersion::MAJOR_PATCH_POS] > OB_VSN_MAJOR_PATCH_MASK
        || versions[ObClusterVersion::MINOR_PATCH_POS] > OB_VSN_MINOR_PATCH_MASK)) {
      ret = OB_SIZE_OVERFLOW;
      COMMON_LOG(WARN, "invalid package version",
                 KR(ret), "version", str,
                 "major", versions[ObClusterVersion::MAJOR_POS],
                 "minor", versions[ObClusterVersion::MINOR_POS],
                 "major_patch", versions[ObClusterVersion::MAJOR_PATCH_POS],
                 "minor_patch", versions[ObClusterVersion::MINOR_PATCH_POS]);
    }
  }
  return ret;
}

ObClusterVersion::ObClusterVersion()
  : is_inited_(false), config_(NULL),
    tenant_config_mgr_(NULL), cluster_version_(0), data_version_(0)
{
  cluster_version_ = cal_version(DEF_MAJOR_VERSION,
                                 DEF_MINOR_VERSION,
                                 DEF_MAJOR_PATCH_VERSION,
                                 DEF_MINOR_PATCH_VERSION);
}

int ObClusterVersion::init(const uint64_t cluster_version)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(ERROR, "cluster version init twice", KR(ret), K(cluster_version));
  } else if (!check_version_valid_(cluster_version)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid cluster version", KR(ret), K(cluster_version));
  } else {
    ATOMIC_SET(&cluster_version_, cluster_version);
    COMMON_LOG(INFO, "cluster version inited success", K(cluster_version));
    is_inited_ = true;
  }

  return ret;
}

int ObClusterVersion::init(
    const ObServerConfig *config,
    const omt::ObTenantConfigMgr *tenant_config_mgr)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(ERROR, "cluster version init twice", KR(ret), KP(config));
  } else if (OB_ISNULL(config) || OB_ISNULL(tenant_config_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", KR(ret), KP(config), KP(tenant_config_mgr));
  } else if (OB_FAIL(refresh_cluster_version(config->min_observer_version.str()))) {
    COMMON_LOG(WARN, "refresh cluster version error", KR(ret));
  } else {
    config_ = config;
    tenant_config_mgr_ = tenant_config_mgr;
    COMMON_LOG(INFO, "cluster version inited success", K_(cluster_version));
    is_inited_ = true;
  }

  return ret;
}

void ObClusterVersion::destroy()
{
  if (is_inited_) {
    is_inited_ = false;
    config_ = NULL;
    cluster_version_ = 0;
    data_version_ = 0;
  }
}

int64_t ObClusterVersion::to_string(char *buf, const int64_t buf_len) const
{
  const uint64_t version = ATOMIC_LOAD(&cluster_version_);
  return print_vsn(buf, buf_len, version);
}

int64_t ObClusterVersion::print_vsn(char *buf, const int64_t buf_len, uint64_t version)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  const uint32_t major = OB_VSN_MAJOR(version);
  const uint16_t minor = OB_VSN_MINOR(version);
  const uint8_t major_patch = OB_VSN_MAJOR_PATCH(version);
  const uint8_t minor_patch = OB_VSN_MINOR_PATCH(version);
  if (OB_UNLIKELY(!check_version_valid_(version))) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(ERROR, "invalid cluster version", KR(ret), K(version), K(lbt()));
  } else if (major < 3
             || (3 == major && minor < 2)
             || (3 == major && 2 == minor && 0 == major_patch && minor_patch < 3)) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%lu(%u, %u, %u)",
                version, major, minor, minor_patch))) {
      COMMON_LOG(WARN, "fail to print vsn", KR(ret), K(version));
    }
  } else {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%lu(%u, %u, %u, %u)",
                version, major, minor, major_patch, minor_patch))) {
      COMMON_LOG(WARN, "fail to print vsn", KR(ret), K(version));
    }
  }
  if (OB_FAIL(ret)) {
    pos = OB_INVALID_INDEX;
  }
  return pos;
}

int64_t ObClusterVersion::print_version_str(char *buf, const int64_t buf_len, uint64_t version)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  const uint32_t major = OB_VSN_MAJOR(version);
  const uint16_t minor = OB_VSN_MINOR(version);
  const uint8_t major_patch = OB_VSN_MAJOR_PATCH(version);
  const uint8_t minor_patch = OB_VSN_MINOR_PATCH(version);
  if (OB_UNLIKELY(!check_version_valid_(version))) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(ERROR, "invalid cluster version", K(version), K(lbt()));
  } else if (major < 3
             || (3 == major && minor < 2)
             || (3 == major && 2 == minor && 0 == major_patch && minor_patch < 3)) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%u.%u.%u",
                major, minor, minor_patch))) {
      COMMON_LOG(WARN, "fail to print version str", KR(ret), K(version));
    }
  } else {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%u.%u.%u.%u",
                major, minor, major_patch, minor_patch))) {
      COMMON_LOG(WARN, "fail to print version str", KR(ret), K(version));
    }
  }
  if (OB_FAIL(ret)) {
    pos = OB_INVALID_INDEX;
  }
  return pos;
}

int ObClusterVersion::refresh_cluster_version(const char *verstr)
{
  int ret = OB_SUCCESS;
  uint64_t items[MAX_VERSION_ITEM] = {0};
  if (NULL == verstr) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(parse_version(verstr, items, MAX_VERSION_ITEM))) {
    // do nothing
  } else {
    const uint64_t version = cal_version(items[ObClusterVersion::MAJOR_POS],
                                         items[ObClusterVersion::MINOR_POS],
                                         items[ObClusterVersion::MAJOR_PATCH_POS],
                                         items[ObClusterVersion::MINOR_PATCH_POS]);
    ATOMIC_SET(&cluster_version_, version);
    COMMON_LOG(INFO, "refresh cluster version", "cluster_version", *this);
  }

  return ret;
}

int ObClusterVersion::reload_config()
{
  int ret = OB_SUCCESS;

  if (NULL == config_) {
    COMMON_LOG(WARN, "config is null", KP_(config));
    ret = OB_ERR_UNEXPECTED;
  } else {
    ret = refresh_cluster_version(config_->min_observer_version.str());
  }

  return ret;
}

void ObClusterVersion::update_cluster_version(const uint64_t cluster_version)
{
  ATOMIC_SET(&cluster_version_, cluster_version);
}

void ObClusterVersion::update_data_version(const uint64_t data_version)
{
  ATOMIC_SET(&data_version_, data_version);
}

int ObClusterVersion::get_tenant_data_version(
    const uint64_t tenant_id,
    uint64_t &data_version)
{
  int ret = OB_SUCCESS;
  data_version = 0;
  if (OB_UNLIKELY(0 != data_version_)) {
    // only work for unittest
    data_version = ATOMIC_LOAD(&cluster_version_);
  } else  if (OB_ISNULL(tenant_config_mgr_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "tenant_config is null", KR(ret), KP(tenant_config_mgr_));
  } else {
    // wont't fallback or retry
    omt::ObTenantConfigGuard tenant_config(tenant_config_mgr_->get_tenant_config_with_lock(tenant_id));
    if (tenant_config.is_valid() && tenant_config->compatible.value_updated()) {
      data_version = tenant_config->compatible;
    } else if (is_sys_tenant(tenant_id)
               || is_meta_tenant(tenant_id)) {
      // For sys/meta tenant, circular dependency problem may exist when load tenant config from inner tables.
      // For safety, data_version will fallback to last barrier data version until actual tenant config is loaded.
      data_version = LAST_BARRIER_DATA_VERSION;
      if (REACH_TIME_INTERVAL(60 * 1000 * 1000L)) {
        share::ObTaskController::get().allow_next_syslog();
        COMMON_LOG(INFO, "tenant data version fallback to last barrier version", K(tenant_id), K(data_version));
      }
    } else {
      // For user tenant
      ret = OB_ENTRY_NOT_EXIST;
      COMMON_LOG(WARN, "tenant compatible version is not refreshed", KR(ret), K(tenant_id));
    }
  }
  return ret;
}

int ObClusterVersion::tenant_need_upgrade(
    const uint64_t tenant_id,
    bool &need_upgrade)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  if (OB_FAIL(get_tenant_data_version(tenant_id, data_version))) {
    COMMON_LOG(WARN, "fail to get tenant data version", KR(ret), K(tenant_id));
  } else {
    need_upgrade = (data_version < DATA_CURRENT_VERSION);
  }
  return ret;
}

int ObClusterVersion::is_valid(const char *verstr)
{
  int ret = OB_SUCCESS;
  uint64_t items[MAX_VERSION_ITEM] = {0};
  if (NULL == verstr) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(parse_version(verstr, items, MAX_VERSION_ITEM))) {
    COMMON_LOG(WARN, "invalid version", "version_str", verstr);
  }
  return ret;
}

int ObClusterVersion::get_version(const common::ObString &verstr, uint64_t &version)
{
  int ret = OB_SUCCESS;
  char buf[OB_CLUSTER_VERSION_LENGTH];
  version = 0;

  if (OB_FAIL(databuff_printf(buf, OB_CLUSTER_VERSION_LENGTH, "%.*s", verstr.length(), verstr.ptr()))) {
    COMMON_LOG(WARN, "failed to print version", K(ret), K(verstr));
  } else if (OB_FAIL(get_version(buf, version))) {
    COMMON_LOG(WARN, "failed to get version", K(ret), K(buf));
  }

  return ret;
}

int ObClusterVersion::get_version(const char *verstr, uint64_t &version)
{
  int ret = OB_SUCCESS;
  uint64_t items[MAX_VERSION_ITEM] = {0};
  if (NULL == verstr) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(parse_version(verstr, items, MAX_VERSION_ITEM))) {
    COMMON_LOG(WARN, "invalid version", "version_str", verstr);
  } else {
    version = cal_version(items[ObClusterVersion::MAJOR_POS],
                          items[ObClusterVersion::MINOR_POS],
                          items[ObClusterVersion::MAJOR_PATCH_POS],
                          items[ObClusterVersion::MINOR_PATCH_POS]);
  }
  return ret;
}

bool ObClusterVersion::check_version_valid_(const uint64_t version)
{
  bool bret = true;
  const uint32_t major = OB_VSN_MAJOR(version);
  const uint16_t minor = OB_VSN_MINOR(version);
  const uint8_t major_patch = OB_VSN_MAJOR_PATCH(version);
  const uint8_t minor_patch = OB_VSN_MINOR_PATCH(version);
  if (major < 3 || (3 == major && minor < 2)) {
    // cluster_version is less than "3.2":
    // - should be "a.b.0.c";
    bret = (0 == major_patch);
  } else if (3 == major && 2 == minor)  {
    // cluster_version's prefix is "3.2":
    // - cluster_version == 3.2.0.0/1/2
    // - cluster_version >= 3.2.3.x
    bret = (0 == major_patch && minor_patch <= 2) || (major_patch >= 3);
  } else {
    // cluster_version is greator than "3.2"
    bret = true;
  }
  return bret;
}

ObClusterVersion &ObClusterVersion::get_instance()
{
  static ObClusterVersion cluster_version;
  return cluster_version;
}

} // end namespace common
} // end namespace oceanbase
