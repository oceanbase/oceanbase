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
 * ObCDCEndpointProvider Impl
 */

#define USING_LOG_PREFIX OBLOG

#include "ob_cdc_tenant_endpoint_provider.h"

#include "ob_log_mysql_connector.h"         // ObLogMySQLConnector
#include "ob_log_config.h"                  // TCONF

namespace oceanbase
{
namespace libobcdc
{
ObCDCEndpointProvider::ObCDCEndpointProvider()
    : is_inited_(false),
      refresh_lock_(ObLatchIds::OBCDC_SQLSERVER_LOCK),
      sql_svr_list_(),
      endpoint_list_(),
      svr_blacklist_()
{}

ObCDCEndpointProvider::~ObCDCEndpointProvider()
{
  destroy();
}

int ObCDCEndpointProvider::init(const char *tenant_endpoint_str)
{
  int ret = OB_SUCCESS;
  const char *sql_server_blacklist = TCONF.sql_server_blacklist.str();
  const bool is_sql_server = true;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("CDCEndpointProvider already inited", KR(ret));
  } else if (OB_FAIL(svr_blacklist_.init(sql_server_blacklist, is_sql_server))) {
    LOG_ERROR("svr_blacklist_ init fail", KR(ret), K(sql_server_blacklist), K(is_sql_server));
  } else if (OB_FAIL(parse_tenant_endpoint_list_(tenant_endpoint_str))) {
    LOG_ERROR("parse_tenant_endpoint_list_ failed", KR(ret));
  } else if (OB_FAIL(init_sql_svr_list_())) {
    LOG_ERROR("init_sql_svr_list_ failed", KR(ret), KCSTRING(tenant_endpoint_str), K_(endpoint_list));
  } else {
    LOG_INFO("init EndpointProvider", KCSTRING(tenant_endpoint_str), K_(endpoint_list));
    is_inited_ = true;
  }

  return ret;
}

void ObCDCEndpointProvider::destroy()
{
  if (IS_INIT) {
    is_inited_ = false;
    sql_svr_list_.reset();
    endpoint_list_.reset();
    svr_blacklist_.destroy();
  }
}

void ObCDCEndpointProvider::configure(const ObLogConfig &cfg)
{
  const char *sql_server_blacklist = cfg.sql_server_blacklist.str();
  LOG_INFO("[CONFIG]", K(sql_server_blacklist));

  svr_blacklist_.refresh(sql_server_blacklist);
}

int ObCDCEndpointProvider::refresh_server_list(ObIArray<ObAddr> &server_list)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_LIKELY(server_list.count() > 0)) {
    SpinWLockGuard guard(refresh_lock_);
    if (OB_FAIL(sql_svr_list_.assign(server_list))) {
      LOG_ERROR("assgin server_list failed", KR(ret), K(server_list), K_(sql_svr_list));
    } else {
      LOG_INFO("[ENDPOINT], refresh to server_list", K_(sql_svr_list));
    }
  }

  return ret;
}

int64_t ObCDCEndpointProvider::get_server_count() const
{
  SpinRLockGuard guard(refresh_lock_);
  return is_inited_ ? sql_svr_list_.count() : 0;
}

int ObCDCEndpointProvider::get_server(const int64_t svr_idx, ObAddr &server)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (svr_idx < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid server index", K(svr_idx));
  } else {
    SpinRLockGuard guard(refresh_lock_);
    if (OB_UNLIKELY(svr_idx > sql_svr_list_.count())) {
      ret = OB_INDEX_OUT_OF_RANGE;
      LOG_ERROR("index out of range", KR(ret), K(svr_idx), K(sql_svr_list_));
    } else if (OB_FAIL(sql_svr_list_.at(svr_idx, server))) {
      LOG_ERROR("get server from sql_svr_list_ failed", KR(ret), K(svr_idx), K_(sql_svr_list), K_(endpoint_list));
    }
  }

  return ret;
}

int ObCDCEndpointProvider::get_tenant_ids(ObIArray<uint64_t> &tenant_ids)
{
  UNUSED(tenant_ids);
  return OB_SUCCESS;
}

int ObCDCEndpointProvider::get_tenant_servers(const uint64_t tenant_id, common::ObIArray<ObAddr> &tenant_servers)
{
  UNUSED(tenant_id);
  UNUSED(tenant_servers);
  return OB_SUCCESS;
}

int ObCDCEndpointProvider::parse_tenant_endpoint_list_(const char *tenant_endpoint_str)
{
  int ret = OB_SUCCESS;
  const int64_t str_len = strlen(tenant_endpoint_str);
  char tenant_endpoint_str_copy[str_len + 1];

  if (OB_ISNULL(tenant_endpoint_str) ||(1 >= str_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid tenant_endpoint_str", KR(ret), K(str_len));
  } else {
    MEMSET(tenant_endpoint_str_copy, '\0', sizeof(char) * (str_len + 1));
    MEMCPY(tenant_endpoint_str_copy, tenant_endpoint_str, str_len);
    ObCDCEndpointList endpoint_list;
    const char *delimiter = "|";
    char *p = nullptr;
    char *endpoint_ptr = strtok_r(tenant_endpoint_str_copy, delimiter, &p);

    while (OB_SUCC(ret) && OB_NOT_NULL(endpoint_ptr)) {
      ObCDCEndpoint endpoint;
      if (OB_FAIL(endpoint.init(endpoint_ptr))) {
        LOG_ERROR("init endpoint failed", KR(ret), K(endpoint_ptr));
      } else if (OB_FAIL(endpoint_list.push_back(endpoint))) {
        LOG_ERROR("push_back endpoint failed", KR(ret), K(endpoint), KCSTRING(tenant_endpoint_str), K(endpoint_list));
      } else {
        LOG_INFO("[TENANT_ENDPOINT]", K(endpoint));
      }
      endpoint_ptr = strtok_r(nullptr, delimiter, &p);
    }

    SpinWLockGuard guard(refresh_lock_);
    if (FAILEDx(endpoint_list_.assign(endpoint_list))) {
      LOG_ERROR("assign endpoint_list failed", KR(ret),
          "expected_endpoint_list", endpoint_list,
          "cur_endpoint_list", endpoint_list_);
    }
  }
  LOG_INFO("parse_tenant_endpoint_list_", KR(ret), KCSTRING(tenant_endpoint_str), K_(endpoint_list));

  return ret;
}

int ObCDCEndpointProvider::init_sql_svr_list_()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(sql_svr_list_.count() > 0 || endpoint_list_.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid sql_svr_list_ or endpoint_list_ while init_sql_svr_list_",
        KR(ret), K_(sql_svr_list), K_(endpoint_list));
  } else {
    const int64_t endpoint_count = endpoint_list_.count();

    for (int i = 0; OB_SUCC(ret) && i < endpoint_count; i++) {
      const ObCDCEndpoint &endpoint = endpoint_list_.at(i);
      ObAddr addr = endpoint.get_addr();
      if (OB_UNLIKELY(! addr.is_valid())) {
        // keep ret = OB_SUCCESS
        LOG_WARN("invalid tenant_endpoint", KR(ret), K(endpoint));
      } else if (OB_FAIL(sql_svr_list_.push_back(addr))) {
        LOG_ERROR("push_back addr into sql_svr_list failed", KR(ret), K(endpoint), K(addr));
      } else {
        LOG_INFO("[ENDPOINT_INIT]", K(endpoint));
      }
    }
  }

  const int64_t sql_svr_count = sql_svr_list_.count();

  if (sql_svr_count <= 0) {
    ret = OB_EMPTY_RESULT;
    LOG_WARN("[ENDPOINT_INIT] NOT FOUND VALID ENDPOINT, PLEASE CHECK CONFIG 'tenant_endpoint'", KR(ret), K_(endpoint_list));
  } else {
    LOG_INFO("[ENDPOINT_INIT]", "TOTAL", sql_svr_count);
  }

  return ret;
}

} // namespace libobcdc
} // namespace oceanbase
