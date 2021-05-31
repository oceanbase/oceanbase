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

#define USING_LOG_PREFIX RS_LB

#include "ob_unit_load_history_table_operator.h"

namespace oceanbase {
namespace rootserver {
using namespace common;
using namespace share;

int ObUnitLoadHistoryTableOperator::init(common::ObMySQLProxy& proxy, const common::ObAddr& self_addr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObEventHistoryTableOperator::init(proxy))) {
  } else {
    const bool is_rs_event = true;
    set_addr(self_addr, is_rs_event);
    set_event_table(share::OB_ALL_UNIT_LOAD_HISTORY_TNAME);
  }
  return ret;
}

ObUnitLoadHistoryTableOperator& ObUnitLoadHistoryTableOperator::get_instance()
{
  static ObUnitLoadHistoryTableOperator instance;
  return instance;
}

int ObUnitLoadHistoryTableOperator::add_load(int64_t tenant_id, ObResourceWeight& weight, UnitStat& us)
{
  int ret = common::OB_SUCCESS;
  int64_t event_ts = 0;
  if (!is_inited()) {
    ret = common::OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(gen_event_ts(event_ts))) {
    LOG_WARN("gen_event_ts failed", K(ret));
  } else {
    share::ObDMLSqlSplicer dml;
    char rs_ip_buf[common::MAX_IP_ADDR_LENGTH];
    char ip_buf[common::MAX_IP_ADDR_LENGTH];
    const ObAddr& unit_addr = us.info_.unit_.server_;
    const ObAddr& rs_addr = get_addr();
    // rs_svr_ip, rs_svr_port

    (void)rs_addr.ip_to_string(rs_ip_buf, common::MAX_IP_ADDR_LENGTH);
    // svr_ip, svr_port
    (void)unit_addr.ip_to_string(ip_buf, common::MAX_IP_ADDR_LENGTH);
    if (OB_FAIL(dml.add_gmt_create(event_ts)) || OB_FAIL(dml.add_column("tenant_id", tenant_id)) ||
        OB_FAIL(dml.add_column("zone", us.info_.unit_.zone_)) || OB_FAIL(dml.add_column("svr_ip", ip_buf)) ||
        OB_FAIL(dml.add_column("svr_port", unit_addr.get_port())) ||
        OB_FAIL(dml.add_column("unit_id", us.info_.unit_.unit_id_)) ||
        OB_FAIL(dml.add_column("rs_svr_ip", rs_ip_buf)) || OB_FAIL(dml.add_column("rs_svr_port", rs_addr.get_port())) ||
        OB_FAIL(dml.add_column("`load`", us.get_load())) || OB_FAIL(dml.add_column("cpu_weight", weight.cpu_weight_)) ||
        OB_FAIL(dml.add_column("disk_weight", weight.disk_weight_)) ||
        OB_FAIL(dml.add_column("memory_weight", weight.memory_weight_)) ||
        OB_FAIL(dml.add_column("iops_weight", weight.iops_weight_)) ||
        OB_FAIL(dml.add_column("cpu_usage_rate", us.get_cpu_usage())) ||
        OB_FAIL(dml.add_column("disk_usage_rate", us.get_disk_usage())) ||
        OB_FAIL(dml.add_column("memory_usage_rate", us.get_memory_usage())) ||
        OB_FAIL(dml.add_column("iops_usage_rate", us.get_iops_usage()))) {
      LOG_WARN("add column failed", K(ret));
    }
    if (OB_SUCC(ret)) {
      common::ObSqlString sql;
      if (OB_FAIL(dml.splice_insert_sql(share::OB_ALL_UNIT_LOAD_HISTORY_TNAME, sql))) {
        LOG_WARN("splice_insert_sql failed", K(ret));
      } else if (OB_FAIL(add_task(sql))) {
        LOG_WARN("add_task failed", K(sql), K(ret));
      } else {
        LOG_INFO("add sql to task", K(sql));
      }
    }
  }
  return ret;
}

int ObUnitLoadHistoryTableOperator::async_delete()
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "not init", K(ret));
  } else {
    const int64_t now = ObTimeUtility::current_time();
    ObSqlString sql;
    const bool is_delete = true;
    if (OB_SUCCESS == ret) {
      const int64_t load_delete_timestap = now - UNIT_LOAD_HISTORY_DELETE_TIME;
      sql.reset();
      if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE gmt_create < usec_to_time(%ld) LIMIT 1024",
              share::OB_ALL_UNIT_LOAD_HISTORY_TNAME,
              load_delete_timestap))) {
        SHARE_LOG(WARN, "assign_fmt failed", K(ret));
      } else if (OB_FAIL(add_task(sql, is_delete))) {
        SHARE_LOG(WARN, "add_task failed", K(sql), K(is_delete), K(ret));
      }
    }
  }
  return ret;
}

}  // end namespace rootserver
}  // end namespace oceanbase
