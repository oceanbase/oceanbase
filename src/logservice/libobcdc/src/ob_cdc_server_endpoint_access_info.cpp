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
 *
 * ObCDCServerEndpiontAccessInfo Impl
 */

#define USING_LOG_PREFIX OBLOG

#include "ob_cdc_server_endpoint_access_info.h"

#include "lib/net/ob_net_util.h"
#include "ob_log_utils.h"

namespace oceanbase
{
namespace libobcdc
{
using namespace oceanbase::common;

int ObCDCEndpoint::init(const char *tenant_endpoint)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(tenant_endpoint)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid tenant_endpoint", KR(ret));
  } else {
    const int64_t tenant_endpoint_str_len = strlen(tenant_endpoint);
    char tenant_endpoint_str[tenant_endpoint_str_len + 1];
    tenant_endpoint_str[tenant_endpoint_str_len] = '\0';
    MEMCPY(tenant_endpoint_str, tenant_endpoint, tenant_endpoint_str_len);

    if (OB_UNLIKELY(tenant_endpoint_str_len <= 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("tenant_endpoint_str_len too long", KR(ret),
          K(tenant_endpoint_str_len), KCSTRING(tenant_endpoint_str));
    } else {
      const char *delimiter = ":";
      const int64_t expected_res_cnt = 2; // expect hostname and port
      const char *split_res[2];
      int64_t split_res_cnt = 0;

      if (OB_FAIL(split(
          tenant_endpoint_str,
          delimiter,
          expected_res_cnt,
          split_res,
          split_res_cnt))) {
        LOG_ERROR("split tenant_endpoint_str failed", KR(ret),
            KCSTRING(tenant_endpoint_str), K(delimiter), K(expected_res_cnt), K(split_res_cnt));
      } else if (OB_UNLIKELY(split_res_cnt != expected_res_cnt)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_ERROR("split res cnt not as expected", KR(ret),
            KCSTRING(tenant_endpoint_str), K(split_res_cnt), K(expected_res_cnt));
      } else if (OB_ISNULL(split_res[0]) || OB_ISNULL(split_res[1])) {
        ret = OB_INVALID_ARGUMENT;
        LOG_ERROR("split res content not as expected",
            KCSTRING(tenant_endpoint_str), K(split_res_cnt), K(expected_res_cnt), K(split_res[0]), K(split_res[1]));
      } else if (OB_FAIL(c_str_to_int(split_res[1], port_))) {
        LOG_ERROR("convert port_str to port failed", KR(ret), KCSTRING(tenant_endpoint_str), K(split_res[1]), K_(port));
      } else {
        const char *host = split_res[0];
        const int host_len = strlen(host);
        if (OB_UNLIKELY(MAX_HOSTNAME_LEN < host_len)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_ERROR("invalid tenant_endpoint hostname", KR(ret),
            KCSTRING(tenant_endpoint_str), K(host), K(host_len), K(MAX_HOSTNAME_LEN));
        } else {
          MEMCPY(host_, host, host_len);
          if (OB_FAIL(check_domain_or_addr_())) {
            LOG_ERROR("check_domain_or_addr_ failed", KR(ret), KPC(this));
          } else {
            LOG_INFO("resolve tenant endpoint", KPC(this));
          }
        }
      }
    }
  }

  return ret;
}

ObCDCEndpoint &ObCDCEndpoint::operator=(const ObCDCEndpoint &other)
{
  if (this == &other) {
    return *this;
  } else {
    reset();
    MEMCPY(host_, other.get_host(), strlen(other.get_host()));
    port_ = other.get_port();
    return *this;
  }
}

ObAddr ObCDCEndpoint::get_addr() const
{
  int ret = OB_SUCCESS;
  ObAddr addr;
  char *address;

  if (OB_ISNULL(address = obsys::ObNetUtil::get_addr_by_hostname(host_))) {
    LOG_WARN("invalid addr of hostname", KPC(this));
  } else {
    addr.set_ip_addr(address, port_);
  }

  return addr;
}

bool ObCDCEndpoint::is_valid() const
{
  return get_addr().is_valid();
}

int ObCDCEndpoint::check_domain_or_addr_()
{
  int ret = OB_SUCCESS;
  char *addr;

  if (OB_ISNULL(addr = obsys::ObNetUtil::get_addr_by_hostname(host_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid addr of hostname");
  } else {
    is_domain_ = (0 != strcmp(addr, host_));
  }

  return ret;
}

int ObAccessInfo::init(const char *user, const char *password)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(user) || OB_ISNULL(password)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid user or password", KR(ret));
  } else if (OB_UNLIKELY(strlen(user) > OB_MAX_USER_NAME_LENGTH
      || strlen(password) > OB_MAX_PASSWORD_LENGTH)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("user or password is too long",
        "user_length", strlen(user),
        "max_user_length", OB_MAX_USER_NAME_LENGTH,
        "password_length", strlen(password),
        "max_password_length", OB_MAX_PASSWORD_LENGTH);
  } else {
    MEMCPY(user_, user, strlen(user));
    MEMCPY(password_, password, strlen(password));
  }

  return ret;
}

} // namespace libobcdc
} // namespace oceanbase
