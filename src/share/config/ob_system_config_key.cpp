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

#include "share/config/ob_system_config_key.h"

namespace oceanbase
{
namespace common
{
const char *ObSystemConfigKey::DEFAULT_VALUE = "ANY";

int ObSystemConfigKey::set_varchar(const ObString &key, const char *strvalue)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(strvalue)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "check varchar value failed", K(ret));
  } else if (key == "zone") {
    if (OB_FAIL(zone_.assign(strvalue))) {
      OB_LOG(WARN, "zone assign failed", K(strvalue), K(ret));
    }
  } else if (key == "svr_type") {
    strncpy(server_type_, strvalue, sizeof(server_type_));
    server_type_[OB_SERVER_TYPE_LENGTH - 1] = '\0';
  } else if (key == "svr_ip") {
    strncpy(server_ip_, strvalue, sizeof(server_ip_));
    server_ip_[OB_MAX_SERVER_ADDR_SIZE - 1] = '\0';
  } else {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(ERROR, "unknown sys config column name", "name", key.ptr(), K(ret));
  }
  return ret;
}

void ObSystemConfigKey::set_version(const int64_t version)
{
  version_ = version;
}

int64_t ObSystemConfigKey::get_version() const
{
  return version_;
}

int ObSystemConfigKey::set_int(const ObString &key, int64_t intval)
{
  int ret = OB_SUCCESS;
  if (key == "svr_port") {
    server_port_ = intval;
  } else {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(ERROR, "unknown sys config column name", "name", key.ptr(), K(ret));
  }
  return ret;
}

int64_t ObSystemConfigKey::to_string(char *buf, const int64_t len) const
{
  return snprintf(buf, len, "name: [%s] zone: [%s] "
                  "server_type: [%s] server_ip: [%s] server_port: [%ld] version: [%ld]",
                  name_, zone_.ptr(), server_type_, server_ip_, server_port_, version_);
}

} // end of namespace common
} // end of namespace oceanbase
