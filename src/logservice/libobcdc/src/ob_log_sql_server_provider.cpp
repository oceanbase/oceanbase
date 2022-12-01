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
 * ObCDCRSSQLServerProvider impl
 * Get the RootsServer list from ConfigURL or RSLIST
 */

#define USING_LOG_PREFIX OBLOG

#include "ob_log_sql_server_provider.h"

#include "share/ob_define.h"
#include "share/ob_web_service_root_addr.h"   // fetch_rs_list_from_url
#include "share/location_cache/ob_location_struct.h" //ObLSReplicaLocation

using namespace oceanbase::common;
using namespace oceanbase::share;
namespace oceanbase
{
namespace libobcdc
{

const char *ObLogSQLServerProvider::DEFAULT_VALUE_OF_RS_CONF = "|";

ObLogSQLServerProvider::ObLogSQLServerProvider() : inited_(false),
                                                   is_using_rs_list_(false),
                                                   server_list_(ObModIds::OB_LOG_SERVER_PROVIDER, OB_MALLOC_NORMAL_BLOCK_SIZE),
                                                   refresh_lock_(ObLatchIds::OBCDC_SQLSERVER_LOCK),
                                                   rs_conf_lock_(),
                                                   svr_blacklist_()
{
  (void)memset(rs_list_, 0, sizeof(char) * MAX_CONFIG_LENGTH);
  (void)memset(config_url_, 0, sizeof(char) * MAX_CONFIG_LENGTH);
}

ObLogSQLServerProvider::~ObLogSQLServerProvider()
{
  destroy();
}

int ObLogSQLServerProvider::init(const char *config_url, const char *rs_list)
{
  int ret = OB_SUCCESS;
  const char *sql_server_blacklist = TCONF.sql_server_blacklist.str();
  const bool is_sql_server = true;

  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
  } else if (OB_FAIL(svr_blacklist_.init(sql_server_blacklist, is_sql_server))) {
    LOG_ERROR("svr_blacklist_ init fail", KR(ret), K(sql_server_blacklist), K(is_sql_server));
  // try inited by rs list if rs_list if not empty otherwise use cluster_url, will exist if can't init by rs_list or cluster_url
  } else if (OB_FAIL(check_rs_list_valid_(rs_list))) {
   LOG_ERROR("failed to verify rslist", KR(ret), K(rs_list));
  } else {
    if (is_using_rs_list_) {
      if (OB_FAIL(init_by_rs_list_conf_(rs_list))) {
        LOG_ERROR("failed to init sql server provider by rs_list, please check rs_list is valid or not!", KR(ret), K(rs_list));
      }
    } else {
      if (OB_FAIL(init_by_cluster_url_(config_url))) {
        LOG_ERROR("failed to init sql server provider by cluster url", KR(ret), K(config_url));
      }
    }
  }

  if (OB_SUCC(ret)) {
    inited_ = true;
  }

  return ret;
}

void ObLogSQLServerProvider::destroy()
{
  inited_ = false;
  is_using_rs_list_ = false;
  (void)memset(rs_list_, 0, sizeof(char) * MAX_CONFIG_LENGTH);
  (void)memset(config_url_, 0, sizeof(char) * MAX_CONFIG_LENGTH);
  server_list_.destroy();
  svr_blacklist_.destroy();
}

void ObLogSQLServerProvider::configure(const ObLogConfig &cfg)
{
  const char *sql_server_blacklist = cfg.sql_server_blacklist.str();
  // reload rs_list
  const char *rs_list = cfg.rootserver_list.str();
  LOG_INFO("[CONFIG]", K(sql_server_blacklist), K(rs_list));

  check_rs_list_valid_(rs_list);

  svr_blacklist_.refresh(sql_server_blacklist);
}

int ObLogSQLServerProvider::prepare_refresh()
{
  return OB_SUCCESS;
}

int ObLogSQLServerProvider::end_refresh()
{
  return OB_SUCCESS;
}

int ObLogSQLServerProvider::check_rs_list_valid_(const char *rs_list)
{
  int ret = OB_SUCCESS;
  int64_t pos_rs_list = 0;

  if (OB_ISNULL(rs_list) || OB_UNLIKELY(MAX_CONFIG_LENGTH <= strlen(rs_list)) || OB_UNLIKELY(0 == strlen(rs_list))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invald rs list", KR(ret), K(rs_list));
  } else if (0 == strcmp(rs_list, DEFAULT_VALUE_OF_RS_CONF)) {
    is_using_rs_list_ = false;
  } else {
    is_using_rs_list_ = true;
    // add lock to protect rs_list_ for write;
    ObSmallSpinLockGuard<ObByteLock> guard(rs_conf_lock_);
    if (OB_FAIL(databuff_printf(rs_list_, sizeof(rs_list_), pos_rs_list, "%s", rs_list))) {
      LOG_ERROR("copy rs_list fail", KR(ret), "buf_size", sizeof(rs_list_), "rs_list length", strlen(rs_list),
          K(pos_rs_list), K(rs_list), K_(is_using_rs_list));
    }
  }

  return ret;
}

int ObLogSQLServerProvider::init_by_rs_list_conf_(const char *rs_list)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(rs_list) || OB_UNLIKELY(0 == strcmp(rs_list, DEFAULT_VALUE_OF_RS_CONF))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, may not init by rs_list", KR(ret), K(rs_list), K(DEFAULT_VALUE_OF_RS_CONF));
  } else if (OB_FAIL(refresh_by_rs_list_conf_(rs_list, server_list_))) {
    LOG_ERROR("failed to refresh serverlist by rs_list config", KR(ret), K(rs_list));
  } else{
    LOG_INFO("init sql server provider by rs list success", K(rs_list), K_(server_list));
  }

  return ret;
}

int ObLogSQLServerProvider::init_by_cluster_url_(const char *config_url)
{
  int ret = OB_SUCCESS;
  int64_t pos_cluster_url = 0;

  if (OB_ISNULL(config_url) || OB_UNLIKELY(0 == strcmp(config_url, DEFAULT_VALUE_OF_RS_CONF))) {
    LOG_ERROR("invalid arguments", K(config_url), K(DEFAULT_VALUE_OF_RS_CONF));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(databuff_printf(config_url_, sizeof(config_url_), pos_cluster_url, "%s", config_url))) {
    LOG_ERROR("copy config_url fail", KR(ret), "buf_size", sizeof(config_url),
        "config_url_len", strlen(config_url), K(pos_cluster_url), K(config_url));
  } else if (OB_FAIL(refresh_until_success_(config_url, server_list_))) {
    LOG_ERROR("refresh server list fail", KR(ret), K(config_url));
  } else {
    LOG_INFO("init sql server provider by cluster_url success", K(config_url), K_(server_list));
  }

  return ret;
}

int ObLogSQLServerProvider::refresh_by_rs_list_conf_(const char *rs_list, ServerList &server_list)
{
  OB_ASSERT(NULL != rs_list);
  int ret = OB_SUCCESS;

  if (OB_FAIL(parse_rs_list_(rs_list, server_list))) {
    LOG_WARN("failed to parse rslist", KR(ret), K(rs_list));
  } else if (0 == server_list.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("found empty rs list", KR(ret), K(rs_list));
  } else if (OB_FAIL(get_svr_list_based_on_blacklist_(server_list))) {
    LOG_ERROR("failed to get svr after filter by sql svr blacklis", KR(ret), K(rs_list), K(server_list));
  } else {
    // success
  }

  return ret;
}

int ObLogSQLServerProvider::parse_rs_list_(const char *rs_list, ServerList &server_list)
{
  int ret = OB_SUCCESS;
  static const char *rs_delimiter = ";";
  static const char *param_delemiter = ":";
  static const int64_t expected_rs_param_num = 3; // expect rs param: ip, rpc_port, sql_port

  char rs_list_copy[MAX_CONFIG_LENGTH];
  if (OB_FAIL(get_copy_of_rs_list_conf_(rs_list_copy))) {
    LOG_ERROR("failed to get copy of rs list conf str", KR(ret), K_(rs_list), K(rs_list_copy));
  } else {
    LOG_DEBUG("get copy of rs_list conf str", K_(rs_list), K(rs_list_copy));
    char *rs_ptr = NULL;
    char *p = NULL;

    rs_ptr = strtok_r(rs_list_copy, rs_delimiter, &p);
    while (OB_SUCC(ret) && rs_ptr != NULL) {
      int64_t rs_param_cnt = 0;
      const char *rs_param_res[3];

      if (OB_ISNULL(rs_ptr) || (0 == strlen(rs_ptr))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid rs info str", KR(ret), K(rs_list), K(rs_ptr));
      } else if (OB_FAIL(split(rs_ptr, param_delemiter, expected_rs_param_num, rs_param_res, rs_param_cnt))) {
        LOG_WARN("failed to split rs_info_str", KR(ret), K(rs_ptr));
      } else if (expected_rs_param_num != rs_param_cnt) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid rs param count", KR(ret), K(rs_ptr), K(rs_list), K(expected_rs_param_num));
      } else {
        const char *ip = rs_param_res[0];
        int64_t rpc_port_64 = -1;
        int64_t sql_port = -1;
        if (OB_ISNULL(rs_param_res[1]) || OB_ISNULL(rs_param_res[2]) || (0 == rs_param_res[0]) || (0 == rs_param_res[1])) {
          ret = OB_INVALID_ARGUMENT;
          LOG_ERROR("invalid rs param", KR(ret), K(rs_list), K(rs_param_res[1]), K(rs_param_res[2]));
        } else if (OB_FAIL(c_str_to_int(rs_param_res[1], rpc_port_64))) {
          LOG_ERROR("fail to convert rpc_port_str to rpc_port_int64", KR(ret), K(rs_param_res[1]), K(rpc_port_64));
        } else if (OB_FAIL(c_str_to_int(rs_param_res[2], sql_port))) {
          LOG_ERROR("fail to convert sql_port_str to sql_port_int64", KR(ret), K(rs_param_res[2]), K(sql_port));
        } else {
          ObAddr addr;
          ObRootAddr rs_addr;
          addr.reset();
          rs_addr.reset();
          int32_t rpc_port_32 = static_cast<int32_t>(rpc_port_64);

          if (addr.set_ip_addr(ip, rpc_port_32)) {
            if (OB_FAIL(rs_addr.init(addr, FOLLOWER, sql_port))) {
              LOG_WARN("failed to init addr", KR(ret), K(addr), K(sql_port));
            } else if (!rs_addr.is_valid()) {
              LOG_WARN("invalid rs addr, will ignore this server", K(rs_list), K(ip), K(rpc_port_32), K(sql_port));
            } else if (OB_FAIL(server_list.push_back(rs_addr))) {
              LOG_ERROR("failed to pushback rs server to server list", KR(ret));
            }
          } else {
            LOG_WARN("invalid ip address for rs list config, will ignore this server", K(rs_list), K(ip), K(rpc_port_32), K(sql_port));
          }
        }
      }
      rs_ptr = strtok_r(NULL, rs_delimiter, &p);
    }
  }

  return ret;
}

int ObLogSQLServerProvider::get_copy_of_rs_list_conf_(char *rs_list_copy)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(rs_list_) || OB_UNLIKELY(MAX_CONFIG_LENGTH <= strlen(rs_list_)) || OB_UNLIKELY(0 == strlen(rs_list_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invald rs list", KR(ret), K_(rs_list));
  } else {
    ObSmallSpinLockGuard<ObByteLock> guard(rs_conf_lock_);
    (void)memset(rs_list_copy, 0, sizeof(char) * MAX_CONFIG_LENGTH);
    int64_t pos = 0;
    if (OB_FAIL(databuff_printf(rs_list_copy, sizeof(rs_list_), pos, "%s", rs_list_))) {
      LOG_WARN("failed to copy rs_list_ to rs_list_copy", KR(ret), K_(rs_list), K(rs_list_copy));
    }
  }
  return ret;
}

int ObLogSQLServerProvider::refresh_until_success_(const char *url, ServerList &server_list)
{
  int ret = OB_SUCCESS;
  int64_t check_count = TCONF.test_mode_on ? TCONF.test_mode_block_sqlserver_count : -1; // Test mode to mock the number of failed times of sqlServer refresh rs list
  while (OB_FAIL(refresh_by_cluster_url_(url, server_list)) || check_count-- > 0)
  {
    LOG_WARN("refresh rs list fail, will retry until success", KR(ret), K(url));
    ob_usleep(FETCH_RS_RETRY_INTERVAL_ON_INIT_FAIL);
  }
  return ret;
}

int ObLogSQLServerProvider::refresh_by_cluster_url_(const char *url, ServerList &server_list)
{
  OB_ASSERT(NULL != url);

  int ret = OB_SUCCESS;
  int64_t timeout_ms = REFRESH_SERVER_LIST_TIMEOUT_MS;
  ServerList readonly_server_list; //for compatible, not used
  ObClusterRole cluster_role;
  if (OB_FAIL(ObWebServiceRootAddr::fetch_rs_list_from_url(NULL, url, timeout_ms, server_list, readonly_server_list,
                                                           cluster_role))) {
    LOG_ERROR("fetch_rs_list_from_url fail", KR(ret), K(url), K(timeout_ms));
  } else if (OB_FAIL(get_svr_list_based_on_blacklist_(server_list))) {
    LOG_ERROR("failed to get svr after filter by sql svr blacklist", KR(ret),K(url));
  }

  return ret;
}

int ObLogSQLServerProvider::get_svr_list_based_on_blacklist_(ServerList &server_list)
{
  int ret = OB_SUCCESS;
  const int64_t svr_count_before_filter = server_list.count();
  ObArray<ObAddr> remove_svrs;

  if (OB_FAIL(filter_by_svr_blacklist_(server_list, remove_svrs))) {
    LOG_ERROR("filter_by_svr_blacklist_ fail", KR(ret), K(server_list), K(remove_svrs));
  } else {
    const int64_t svr_count_after_filter = server_list.count();
    LOG_INFO("[SQL_SERVER_PROVIDER] refresh server list succ", "server_count", svr_count_after_filter,
      "remove_server_count", svr_count_before_filter - svr_count_after_filter, K(remove_svrs));

    for (int64_t index = 0; index < svr_count_after_filter; index++) {
      ObRootAddr &addr = server_list.at(index);

      _LOG_INFO("[SQL_SERVER_PROVIDER] server[%ld/%ld]=%s  role=%ld  sql_port=%ld",
          index, svr_count_after_filter, to_cstring(addr.get_server()),
          static_cast<int64_t>(addr.get_role()), addr.get_sql_port());
    }
  }
  return ret;
}

int ObLogSQLServerProvider::filter_by_svr_blacklist_(ServerList &server_list,
    common::ObArray<common::ObAddr> &remove_svrs)
{
  int ret = OB_SUCCESS;
  bool has_done = false;
  const int64_t svr_blacklist_cnt = svr_blacklist_.count();
  int64_t svr_remove_cnt = 0;

  for (int64_t svr_idx = server_list.count() - 1; OB_SUCC(ret) && ! has_done && svr_idx >= 0; --svr_idx) {
    const ObAddr &svr = server_list.at(svr_idx).get_server();
    const int64_t svr_count = server_list.count();

    if (1 == svr_count) {
      // Retain, do not dispose
      has_done = true;
    } else if (svr_remove_cnt >= svr_blacklist_cnt) {
      // Based on the fact that the svr blacklist has been cleared
      has_done = true;
    } else {
      if (svr_blacklist_.is_exist(svr)) {
        if (OB_FAIL(remove_svrs.push_back(svr))) {
          LOG_ERROR("remove_svrs push_back fail", KR(ret), K(svr));
        } else if (OB_FAIL(server_list.remove(svr_idx))) {
          LOG_ERROR("remove svr item fail", KR(ret), K(svr_idx), K(svr), K(server_list));
        } else {
          ++svr_remove_cnt;
        }
      } else {
        // do nothing
      }
    }
  }

  return ret;
}

int ObLogSQLServerProvider::get_server(
    const int64_t svr_idx,
    common::ObAddr &server)
{
  int ret = OB_SUCCESS;
  if (! inited_) {
    ret = OB_NOT_INIT;
  } else if (svr_idx < 0) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    // add read lock
    SpinRLockGuard guard(refresh_lock_);
    ObRootAddr addr;

    if (svr_idx >= server_list_.count()) {
      // out of svr count, need retry
      ret = OB_ENTRY_NOT_EXIST;
    } else if (OB_FAIL(server_list_.at(svr_idx, addr))) {
      LOG_ERROR("get server from server list fail", KR(ret), K(svr_idx));
    } else {
      server = addr.get_server();
      // Set SQL port
      server.set_port((int32_t)addr.get_sql_port());
    }

    _LOG_DEBUG("[SQL_SERVER_PROVIDER] get_server(%ld/%ld)=>%s ret=%d",
        svr_idx, server_list_.count(), to_cstring(server), ret);
  }

  return ret;
}

int64_t ObLogSQLServerProvider::get_server_count() const
{
  SpinRLockGuard guard(refresh_lock_);
  return inited_ ? server_list_.count() : 0;
}

int ObLogSQLServerProvider::get_tenant_ids(common::ObIArray<uint64_t> &tenant_ids)
{
  int ret = OB_SUCCESS;
  tenant_ids.reset();

  if (OB_FAIL(tenant_ids.push_back(OB_SYS_TENANT_ID))) {
    LOG_ERROR("push_back sys_tenant_id into tenant_ids for svr_provider failed", KR(ret));
  }

  return ret;
}

int ObLogSQLServerProvider::get_tenant_servers(
    const uint64_t tenant_id,
    common::ObIArray<common::ObAddr> &tenant_servers)
{
  int ret = OB_SUCCESS;
  tenant_servers.reset();

  if (OB_UNLIKELY(OB_SYS_TENANT_ID != tenant_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("SQL_SERVER_PROVIDER expect only provide RS Server", KR(ret), K(tenant_id));
  } else {
    SpinRLockGuard guard(refresh_lock_);

    for (int svr_idx = 0; OB_SUCC(ret) && svr_idx < server_list_.count(); svr_idx++) {
      ObRootAddr root_addr;
      if (OB_FAIL(server_list_.at(svr_idx, root_addr))) {
        LOG_ERROR("get_server failed", KR(ret), K(svr_idx), K_(server_list));
      } else if (OB_UNLIKELY(! root_addr.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("get invalid server from server_list_", KR(ret), K(svr_idx), K(root_addr), K_(server_list));
      } else {
        common::ObAddr svr_addr;
        svr_addr = root_addr.get_server();
        svr_addr.set_port((int32_t)root_addr.get_sql_port());

        if (OB_FAIL(tenant_servers.push_back(svr_addr))) {
          LOG_ERROR("push_back svr_addr to tenant_servers failed", KR(ret),
              K(svr_idx), K(root_addr), K(svr_addr), K(tenant_servers), K_(server_list));
        }
      }
    }
  }

  return ret;
}

int ObLogSQLServerProvider::refresh_server_list(void)
{
  return OB_SUCCESS;
}

int ObLogSQLServerProvider::call_refresh_server_list(void)
{
  int ret = OB_SUCCESS;
  ServerList new_server_list;

  if (! inited_) {
    ret = OB_NOT_INIT;
  } else {
    if (is_using_rs_list_) {
      // should refresh by rslist
      if (OB_FAIL(refresh_by_rs_list_conf_(rs_list_, new_server_list))) {
        LOG_ERROR("refresh server list by rootserver_list failed", KR(ret), K_(rs_list));
      }
    } else {
      if (OB_FAIL(refresh_by_cluster_url_(config_url_, new_server_list))) {
        LOG_ERROR("refresh server list by config url fail", KR(ret), K_(config_url));
      }
    }

    if (OB_SUCC(ret)) {
      // write lock to modify server_list
      SpinWLockGuard guard(refresh_lock_);
      server_list_ = new_server_list;
    }
  }

  return ret;
}

} // namespace libobcdc
} // namespace oceanbase
