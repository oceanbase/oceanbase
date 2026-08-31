/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX RS

#include "ob_backup_server_disk_space_filter.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "rootserver/ob_root_utils.h"
#include "share/config/ob_server_config.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace rootserver
{

ObBackupServerDiskSpaceFilter::ObBackupServerDiskSpaceFilter()
  : is_inited_(false),
    sql_proxy_(nullptr)
{
}

int ObBackupServerDiskSpaceFilter::init(ObMySQLProxy *sql_proxy)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("disk space filter init twice", K(ret));
  } else if (OB_ISNULL(sql_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(sql_proxy));
  } else {
    sql_proxy_ = sql_proxy;
    is_inited_ = true;
  }
  return ret;
}

int ObBackupServerDiskSpaceFilter::query_all_disk_stats(ObIArray<RawDiskStat> &raw_stats)
{
  int ret = OB_SUCCESS;
  raw_stats.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("disk space filter not init", K(ret));
  } else {
    ObTimeoutCtx ctx;
    const char *sql = "SELECT svr_ip, svr_port, data_disk_capacity, data_disk_in_use "
                      "FROM oceanbase.__all_virtual_server";
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
        LOG_WARN("fail to get timeout ctx", K(ret));
      } else if (OB_FAIL(sql_proxy_->read(res, OB_SYS_TENANT_ID, sql))) {
        LOG_WARN("fail to execute disk stat sql", K(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", K(ret));
      } else {
        while (OB_SUCC(ret) && OB_SUCC(result->next())) {
          RawDiskStat stat;
          ObString svr_ip_str;
          int64_t svr_port = 0;
          EXTRACT_VARCHAR_FIELD_MYSQL(*result, "svr_ip", svr_ip_str);
          EXTRACT_INT_FIELD_MYSQL(*result, "svr_port", svr_port, int64_t);
          EXTRACT_INT_FIELD_MYSQL(*result, "data_disk_capacity", stat.capacity_, int64_t);
          EXTRACT_INT_FIELD_MYSQL(*result, "data_disk_in_use", stat.in_use_, int64_t);
          if (OB_SUCC(ret)) {
            if (!stat.server_.set_ip_addr(svr_ip_str, static_cast<int32_t>(svr_port))) {
              ret = OB_INVALID_DATA;
              LOG_WARN("fail to set server addr", K(ret), K(svr_ip_str), K(svr_port));
            } else if (OB_FAIL(raw_stats.push_back(stat))) {
              LOG_WARN("fail to push disk stat", K(ret));
            }
          }
        }
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
      }
    }
  }
  return ret;
}

int ObBackupServerDiskSpaceFilter::apply_disk_filter(
    const ObIArray<ObBackupServer> &input_servers,
    const ObIArray<RawDiskStat> &raw_stats,
    ObIArray<ObBackupServer> &filtered_servers)
{
  int ret = OB_SUCCESS;
  filtered_servers.reset();
  if (input_servers.empty()) {
    // nothing to do
  } else {
    const int64_t limit = GCONF._backup_server_disk_limit_percentage;
    int64_t excluded_count = 0;
    ObArray<ServerWithDiskInfo> passing;

    for (int64_t i = 0; OB_SUCC(ret) && i < input_servers.count(); ++i) {
      const ObBackupServer &bs = input_servers.at(i);
      int64_t used_pct = 0;
      bool disk_ok = true;

      // linear scan to match server — cluster server count bounded
      for (int64_t j = 0; j < raw_stats.count(); ++j) {
        if (raw_stats.at(j).server_ == bs.server_) {
          const int64_t cap = raw_stats.at(j).capacity_;
          const int64_t in_use = raw_stats.at(j).in_use_;
          used_pct = (cap > 0) ? (in_use * 100 / cap) : 100;
          disk_ok = (used_pct < limit);
          if (!disk_ok) {
            ++excluded_count;
            if (REACH_TIME_INTERVAL(60 * 1000 * 1000)) {
              LOG_WARN("[BACKUP] server disk usage exceeds backup limit",
                       "server", bs.server_, K(used_pct), K(limit), K(cap), K(in_use));
            }
          }
          break;
        }
      }
      // server absent from __all_virtual_server: treated as ok with used_pct=0

      if (OB_SUCC(ret) && disk_ok) {
        ServerWithDiskInfo info;
        info.server_ = bs;
        info.used_percentage_ = used_pct;
        if (OB_FAIL(passing.push_back(info))) {
          LOG_WARN("fail to push server info", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      ob_sort(passing.begin(), passing.end());
      for (int64_t i = 0; OB_SUCC(ret) && i < passing.count(); ++i) {
        if (OB_FAIL(filtered_servers.push_back(passing.at(i).server_))) {
          LOG_WARN("fail to push filtered server", K(ret));
        }
      }
      if (filtered_servers.empty() && excluded_count > 0) {
        LOG_WARN("[BACKUP] all servers filtered due to disk space limit",
                 K(excluded_count), K(limit));
      }
    }
  }
  return ret;
}

} // namespace rootserver
} // namespace oceanbase
