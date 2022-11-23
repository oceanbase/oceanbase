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

#define USING_LOG_PREFIX SERVER

#include "ob_inner_table_monitor.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_sql_string.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "common/ob_role_mgr.h"
#include "share/config/ob_server_config.h"
#include "share/ob_common_rpc_proxy.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "rootserver/ob_root_service.h"
namespace oceanbase
{
using namespace common;
using namespace share;

namespace rootserver
{

ObInnerTableMonitor::ObInnerTableMonitor()
  : inited_(false),
    rs_proxy_(NULL),
    sql_proxy_(NULL),
    root_service_(NULL)
{
}

int ObInnerTableMonitor::init(ObMySQLProxy &mysql_proxy,
                              obrpc::ObCommonRpcProxy &rs_proxy,
                              ObRootService &root_service) {
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice");
  } else {
    sql_proxy_ = &mysql_proxy;
    rs_proxy_ = &rs_proxy;
    root_service_ = &root_service;
    inited_ = true;
  }
  return ret;
}

int ObInnerTableMonitor::check_inner_stat() const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is null", K(ret));
  } else if (OB_ISNULL(rs_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("common_proxy_ is null", K(ret));
  } else if (OB_ISNULL(root_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("root_service is null", K(ret));
  }
  return ret;
}

int ObInnerTableMonitor::get_all_tenants_from_stats(
    ObIArray<uint64_t> &tenant_ids)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    sqlclient::ObMySQLResult *result = NULL;
    ObSqlString sql;
    if (OB_FAIL(check_inner_stat())) {
      LOG_WARN("check inner stat failed", K(ret));
    } else if (OB_FAIL(sql.append_fmt("SELECT distinct tenant_id FROM %s",
        OB_ALL_TENANT_HISTORY_TNAME))) {
      LOG_WARN("assign sql string failed", K(ret));
    } else if (OB_FAIL(sql_proxy_->read(res, sql.ptr()))) {
      LOG_WARN("execute sql failed", K(sql), K(ret));
    } else if (NULL == (result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get result failed", K(ret));
    }
  
    while (OB_SUCC(ret)) {
      if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("get next result failed", K(ret));
        }
      } else {
        int64_t tenant_id = 0;
        if (OB_FAIL(result->get_int("tenant_id", tenant_id))) {
          LOG_WARN("get tenant id failed", K(ret));
        } else if (OB_FAIL(tenant_ids.push_back(tenant_id))) {
          LOG_WARN("push back tenant id failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObInnerTableMonitor::purge_inner_table_history()
{
  bool ret = OB_SUCCESS;
  ObSEArray<uint64_t, 16> tenant_ids;
  if (OB_FAIL(get_all_tenants_from_stats(tenant_ids))) {
    LOG_WARN("get_all_tenants_from_stats failed", K(ret));
  } else if (OB_FAIL(purge_recyclebin_objects(tenant_ids))) {
    LOG_WARN("purge recyclebin objects failed", K(ret));
  }
  return ret;
}

int ObInnerTableMonitor::check_cancel() const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check inner stat failed", K(ret));
  } else if (!root_service_->in_service()) {
    ret = OB_RS_SHUTDOWN;
    LOG_WARN("root service is shutdown", K(ret));
  }
  return ret;
}

int ObInnerTableMonitor::purge_recyclebin_objects(ObIArray<uint64_t> &tenant_ids)
{
  int ret = OB_SUCCESS;
  const int64_t current_time = ObTimeUtility::current_time();
  obrpc::Int64 expire_time = current_time - GCONF.schema_history_expire_time;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check inner stat failed", K(ret));
  } else {
    const int64_t SLEEP_INTERVAL = 100 * 1000; //100ms
    const int64_t SLEEP_TIMES = 100;
    const int64_t PURGE_EACH_TIME = 10;
    obrpc::Int64 affected_rows = 0;
    bool is_tenant_finish = false;
    obrpc::ObPurgeRecycleBinArg arg;
    //ignore ret
    for (int i = 0; i < tenant_ids.count() && OB_SUCCESS == check_cancel(); ++i) {
      is_tenant_finish = false;
      affected_rows = 0;
      arg.tenant_id_ = tenant_ids.at(i);
      arg.purge_num_ = PURGE_EACH_TIME;
      arg.expire_time_ = expire_time;
      LOG_INFO("start purge recycle objects of tenant", K(arg));
      int retry_cnt = 0;
      while (!is_tenant_finish && OB_SUCCESS == check_cancel()) {
        // In case of holding DDL thread in long time, Each tenant only purge 10 recycle objects in one round.
        int64_t start_time = ObTimeUtility::current_time();
        if (OB_FAIL(rs_proxy_->purge_expire_recycle_objects(arg, affected_rows))) {
          LOG_WARN("purge reyclebin objects failed", K(ret),
              K(current_time), K(expire_time), K(affected_rows), K(arg), K(retry_cnt));
          if (retry_cnt < 3) {
            is_tenant_finish = false;
            ++retry_cnt;
          } else {
            LOG_WARN("retry purge recyclebin object of tenant failed, ignore it", K(retry_cnt), K(ret), K(arg));
            is_tenant_finish = true;
          }
        } else {
          retry_cnt = 0;
          is_tenant_finish = PURGE_EACH_TIME == affected_rows ? false : true;
        }
        int64_t cost_time = ObTimeUtility::current_time() - start_time;
        LOG_INFO("purge recycle objects", K(ret), K(cost_time),
            K(expire_time), K(current_time), K(affected_rows), K(is_tenant_finish));
        //sleep 10s so that will not block the rs DDL thread
        int i = 0;
        while (OB_SUCCESS == check_cancel() && i < SLEEP_TIMES) {
          ob_usleep(SLEEP_INTERVAL);
          ++i;
        }
      }
    }
  }
  return ret;
}

} //namespace rootserver
} //namespace oceanbase
