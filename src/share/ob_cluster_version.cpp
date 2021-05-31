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
#include "lib/string/ob_string.h"
namespace oceanbase {
namespace common {
static int parse_version(const char* str, int* versions, const int64_t size)
{
  int ret = OB_SUCCESS;
  int64_t i = 0;
  char buf[64] = {0};
  char* ptr = buf;
  const char* delim = ".";
  char* saveptr = NULL;
  char* token = NULL;
  const int64_t VERSION_ITEM = 3;

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
        // COMMON_LOG(INFO, "token", K(versions[i]));
      } else {
        break;
      }
      ptr = NULL;
    }
    if (VERSION_ITEM != i) {
      COMMON_LOG(WARN, "invalid package version", "version", str, K(i), K(VERSION_ITEM));
      ret = OB_ERR_UNEXPECTED;
    }
  }

  return ret;
}

ObClusterVersion::ObClusterVersion() : is_inited_(false), config_(NULL), cluster_version_(0)
{
  cluster_version_ = cal_version(DEF_MAJOR_VERSION, DEF_MINOR_VERSION, DEF_PATCH_VERSION);
}

uint64_t cal_version(const uint64_t major, const uint64_t minor, const uint64_t patch)
{
  return (major << 32) + (minor << 16) + patch;
}

int ObClusterVersion::init(const uint64_t cluster_version)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    COMMON_LOG(ERROR, "cluster version init twice", K(cluster_version));
    ret = OB_INIT_TWICE;
  } else {
    ATOMIC_SET(&cluster_version_, cluster_version);
    COMMON_LOG(INFO, "cluster version inited success", K(cluster_version));
    is_inited_ = true;
  }

  return ret;
}

int ObClusterVersion::init(const ObServerConfig* config)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    COMMON_LOG(ERROR, "cluster version init twice", KP(config));
    ret = OB_INIT_TWICE;
  } else if (NULL == config) {
    COMMON_LOG(WARN, "invalid argument", KP(config));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(refresh_cluster_version(config->min_observer_version.str()))) {
    COMMON_LOG(WARN, "refresh cluster version error", K(ret));
  } else {
    config_ = config;
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
  }
}

int64_t ObClusterVersion::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  const uint64_t version = ATOMIC_LOAD(&cluster_version_);
  const uint16_t major = static_cast<const uint16_t>((version >> 32) & 0xffff);
  const uint16_t minor = static_cast<const uint16_t>((version >> 16) & 0xffff);
  const uint16_t patch = static_cast<const uint16_t>(version & 0xffff);
  databuff_printf(buf, buf_len, pos, "%lu(%u, %u, %u)", version, major, minor, patch);
  return pos;
}

int64_t ObClusterVersion::print_vsn(char* buf, const int64_t buf_len, uint64_t version)
{
  int64_t pos = 0;
  const uint16_t major = OB_VSN_MAJOR(version);
  const uint16_t minor = OB_VSN_MINOR(version);
  const uint16_t patch = OB_VSN_PATCH(version);
  databuff_printf(buf, buf_len, pos, "%lu(%u, %u, %u)", version, major, minor, patch);
  return pos;
}

int64_t ObClusterVersion::print_version_str(char* buf, const int64_t buf_len, uint64_t version)
{
  int64_t pos = 0;
  const uint16_t major = OB_VSN_MAJOR(version);
  const uint16_t minor = OB_VSN_MINOR(version);
  const uint16_t patch = OB_VSN_PATCH(version);
  databuff_printf(buf, buf_len, pos, "%u.%u.%u", major, minor, patch);
  return pos;
}

int ObClusterVersion::refresh_cluster_version(const char* verstr)
{
  int ret = OB_SUCCESS;
  int items[MAX_VERSION_ITEM] = {0};
  if (NULL == verstr) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(parse_version(verstr, items, MAX_VERSION_ITEM))) {
    // do nothing
  } else {
    const uint64_t version = cal_version(items[0], items[1], items[2]);
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

uint64_t ObClusterVersion::get_cluster_version()
{
  return ATOMIC_LOAD(&cluster_version_);
}

void ObClusterVersion::update_cluster_version(const uint64_t cluster_version)
{
  ATOMIC_SET(&cluster_version_, cluster_version);
}

int ObClusterVersion::is_valid(const char* verstr)
{
  int ret = OB_SUCCESS;
  int items[MAX_VERSION_ITEM] = {0};
  if (NULL == verstr) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(parse_version(verstr, items, MAX_VERSION_ITEM))) {
    COMMON_LOG(WARN, "invalid version", "version_str", verstr);
  }
  return ret;
}

int ObClusterVersion::get_version(const common::ObString& verstr, uint64_t& version)
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

int ObClusterVersion::get_version(const char* verstr, uint64_t& version)
{
  int ret = OB_SUCCESS;
  int items[MAX_VERSION_ITEM] = {0};
  if (NULL == verstr) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(parse_version(verstr, items, MAX_VERSION_ITEM))) {
    COMMON_LOG(WARN, "invalid version", "version_str", verstr);
  } else {
    version = cal_version(items[0], items[1], items[2]);
  }
  return ret;
}

ObClusterVersion& ObClusterVersion::get_instance()
{
  static ObClusterVersion cluster_version;
  return cluster_version;
}

}  // end namespace common
}  // end namespace oceanbase
