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
#include "lib/string/ob_sql_string.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "config/ob_server_config.h"
#include "share/ob_inner_config_root_addr.h"
#include "observer/ob_server_struct.h"
#include "common/ob_timeout_ctx.h"
#include "rootserver/ob_root_utils.h"

namespace oceanbase
{
using namespace common;
namespace share
{
int ObInnerConfigRootAddr::check_inner_stat() const
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(config_) || OB_ISNULL(proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is null", KR(ret), KP_(config), KP_(proxy));
  }
  return ret;
}

int ObInnerConfigRootAddr::init(ObMySQLProxy &sql_proxy, ObServerConfig &config)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(ObRootAddrAgent::init(config))) {
    LOG_WARN("init root addr agent failed", K(ret));
  } else {
    proxy_ = &sql_proxy;
    inited_ = true;
  }
  return ret;
}

// update root server list if %addr_list not same with config_->rootservice_list
int ObInnerConfigRootAddr::store(const ObIAddrList &addr_list, const ObIAddrList &readonly_addr_list,
                                 const bool force, const common::ObClusterRole cluster_role,
                                 const int64_t timestamp)
{
  // TODO: Currently there is no local cache of the read copy location of __all_core_table
  UNUSED(readonly_addr_list);
  UNUSED(cluster_role);
  UNUSED(timestamp);
  UNUSED(force);
  int ret = OB_SUCCESS;
  bool need_update = false;
  char addr_buf[MAX_IP_PORT_LENGTH] = "";
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (addr_list.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "addr count", addr_list.count());
  } else {
    if (addr_list.count() != config_->rootservice_list.size()) {
      need_update = true;
    } else {
      ObAddr addr;
      for (int64_t i = 0; OB_SUCC(ret) && !need_update
          && i < config_->rootservice_list.size(); ++i) {
        addr.reset();
        int64_t sql_port = 0;
        if (OB_FAIL(config_->rootservice_list.get(
            static_cast<int32_t>(i), addr_buf, sizeof(addr_buf)))) {
          LOG_WARN("rootservice_list get failed", K(i), K(ret));
        } else if (OB_FAIL(parse_rs_addr(addr_buf, addr, sql_port))) {
          LOG_WARN("parse_rs_addr failed", KCSTRING(addr_buf), K(ret));
        } else {
          bool found = false;
          FOREACH_CNT_X(a, addr_list, !found) {
            if (addr == a->get_server() && sql_port == a->get_sql_port()) {
              found = true;
            }
          }
          if (!found) {
            need_update = true;
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (need_update) {
      ObSqlString sql;
      if (OB_FAIL(sql.assign("ALTER SYSTEM SET rootservice_list = '"))) {
        LOG_WARN("assign sql failed", K(ret));
      } else if (OB_FAIL(format_rootservice_list(addr_list, sql))) {
        LOG_WARN("fail to format rootservice list", KR(ret), K(addr_list), K(sql));
      }

      int64_t affected_rows = 0;
      ObTimeoutCtx ctx;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(sql.append_fmt("'"))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(rootserver::ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
        LOG_WARN("fail to get timeout ctx", K(ret), K(ctx));
      } else if (OB_FAIL(proxy_->write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows))) {
        LOG_WARN("execute sql failed", K(ret), K(sql));
      } else {
        LOG_INFO("ALTER SYSTEM SET rootservice_list succeed", K(addr_list), K(force));
      }
    }
  }

  return ret;
}

int ObInnerConfigRootAddr::format_rootservice_list(const ObIAddrList &addr_list, ObSqlString &str)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < addr_list.count(); ++i) {
    char ip_buf[OB_IP_STR_BUFF] = "";
    const ObRootAddr &rs = addr_list.at(i);
    ObAddr addr = rs.get_server();
    // If it is not the last RS, add a separator at the end
    const bool need_append_delimiter = (i != addr_list.count() - 1);

    if (OB_UNLIKELY(!addr.ip_to_string(ip_buf, sizeof(ip_buf)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("convert ip to string failed", KR(ret), K(rs));
    } else if (OB_FAIL(str.append_fmt("%s%s%s:%d:%ld%s",
        addr.using_ipv4() ? "" : "[",
        ip_buf,
        addr.using_ipv4() ? "" : "]",
        addr.get_port(),
        rs.get_sql_port(),
        need_append_delimiter ? ";" : ""))) {
      LOG_WARN("fail to append_fmt", KR(ret), K(str));
    }
  }
  return ret;
}

int ObInnerConfigRootAddr::fetch(
    ObIAddrList &addr_list,
    ObIAddrList &readonly_addr_list)
{
  int ret = OB_SUCCESS;
  char addr_buf[MAX_IP_PORT_LENGTH] = "";
  const int64_t local_cluster_id = GCONF.cluster_id;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else {
    addr_list.reuse();
    // TODO: The location of the read-only copy of __all_core_table is not cached locally, so it cannot be read
    readonly_addr_list.reuse();
    ObAddr addr;
    ObRootAddr rs_addr;
    for (int64_t i = 0; OB_SUCC(ret) && i < config_->rootservice_list.size(); ++i) {
      addr.reset();
      int64_t sql_port = 0;
      if (OB_FAIL(config_->rootservice_list.get(
          static_cast<int>(i), addr_buf, sizeof(addr_buf)))) {
        LOG_WARN("get rs failed", K(ret), K(i));
      } else if (OB_FAIL(parse_rs_addr(addr_buf, addr, sql_port))) {
        LOG_WARN("parse_rs_addr failed", K(addr_buf), K(ret));
      } else if (OB_FAIL(rs_addr.init(addr, FOLLOWER, sql_port))) {
       LOG_WARN("failed to simple init rs_addr", KR(ret), K(addr), K(sql_port)); 
      } else if (OB_FAIL(addr_list.push_back(rs_addr))) {
        LOG_WARN("add rs address to array failed", K(ret));
      }
    }
    LOG_TRACE("fetch addr_list &readonly_addr_list", K(ret),
        K(addr_list), K(readonly_addr_list));
  }
  return ret;
}

int ObInnerConfigRootAddr::parse_rs_addr(char *addr_buf, ObAddr &addr, int64_t &sql_port)
{
  int ret = OB_SUCCESS;
  if (nullptr == addr_buf) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(addr_buf));
  } else {
    int64_t rpc_port = 0;
    bool use_ipv6 = false;
    char *ip_str = nullptr;
    char *port_str = nullptr;
    char *sql_port_str = nullptr;
    char *save_ptr = nullptr;

    /*
     * ipv4格式: a.b.c.d:port1:port2, proxy只使用一个port
     * ipv6格式: [a:b:c:d:e:f:g:h]:port1:port2, proxy只使用一个port
     *
     */
    if ('[' != addr_buf[0]) { /* ipv4 */
      if (OB_ISNULL(ip_str = strtok_r(addr_buf, ":", &save_ptr))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("parse ipv4 failed", K(addr_buf), K(ret));
      } else if (OB_ISNULL(port_str = strtok_r(nullptr, ":", &save_ptr))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("parse port failed", K(addr_buf), K(ret));
      }
    } else {                  /* ipv6 */
      use_ipv6 = true;
      ip_str = addr_buf + 1;
      port_str = strrchr(addr_buf, ']');
      if (OB_NOT_NULL(port_str)) {
        *(port_str++) = '\0';
        if (':' != *port_str) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("parse port failed", K(addr_buf), K(ret));
        } else {
          port_str++;
          if (OB_ISNULL(port_str = strtok_r(port_str, ":", &save_ptr))) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("parse port failed", K(addr_buf), K(ret));
          }
        }
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("parse ipv6 failed", K(addr_buf), K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      // for compatible with obproxy
      // observer rs list format-->ip:rpc_port:sql_port
      // obporoxy rs list format-->ip:sql_port
      if (OB_ISNULL(sql_port_str = strtok_r(nullptr, ":", &save_ptr))) {
        sql_port_str = port_str;
        port_str = nullptr;
        LOG_INFO("only has one port, used for obproxy", K(sql_port_str), K(addr_buf));
      } else if (OB_NOT_NULL(strtok_r(nullptr, ":", &save_ptr))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("addr in rs_list not in right format", K(addr_buf), K(ret));
      }
      if (OB_SUCC(ret)) {
        if (nullptr != port_str) {
          rpc_port = atoi(port_str);
        } else {
          rpc_port = 1; // fake rpc port, for obproxy, will never be used
        }
        sql_port = atoi(sql_port_str);
        if (!addr.set_ip_addr(ip_str, static_cast<int32_t>(rpc_port))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("set ip address failed", K(ret), K(ip_str), K(rpc_port));
        }
      }
    }
  }
  return ret;
}

} // end namespace share
} // end oceanbase
