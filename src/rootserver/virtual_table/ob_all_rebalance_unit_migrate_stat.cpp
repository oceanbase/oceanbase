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

#define USING_LOG_PREFIX RS
#include "ob_all_rebalance_unit_migrate_stat.h"

using namespace oceanbase::rootserver;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

ObAllRebalanceUnitMigrateStat::ObAllRebalanceUnitMigrateStat(ObUnitManager& unit_mgr,
    ObILeaderCoordinator& leader_coordinator, ObServerManager& server_mgr, ObZoneManager& zone_mgr)
    : server_balance_plan_(unit_mgr, leader_coordinator, server_mgr, zone_mgr),
      schema_guard_(),
      src_ip_buf_(),
      dst_ip_buf_(),
      task_(),
      inited_(false)
{}

ObAllRebalanceUnitMigrateStat::~ObAllRebalanceUnitMigrateStat()
{}

int ObAllRebalanceUnitMigrateStat::init(common::ObMySQLProxy& proxy, common::ObServerConfig& server_config,
    share::schema::ObMultiVersionSchemaService& schema_service, ObRootBalancer& root_balancer)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret));
  } else if (OB_FAIL(server_balance_plan_.init(proxy, server_config, schema_service, root_balancer))) {
    LOG_WARN("fail to init server balance plan", K(ret));
  } else if (OB_FAIL(schema_service.get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard_))) {
    LOG_WARN("fail to get schema guard", K(ret));
  } else {
    inited_ = true;
  }
  return ret;
}

int ObAllRebalanceUnitMigrateStat::get_table_schema(uint64_t tid)
{
  int ret = OB_SUCCESS;
  const uint64_t table_id = combine_id(OB_SYS_TENANT_ID, tid);
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAllRebalanceTenantStat not init", K(ret), K(inited_));
  } else if (OB_FAIL(schema_guard_.get_table_schema(table_id, table_schema_))) {
    LOG_WARN("fail to get table schema", K(table_id), K(ret));
  } else if (NULL == table_schema_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table_schema is null", K(ret));
  }
  return ret;
}

int ObAllRebalanceUnitMigrateStat::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(get_table_schema(OB_ALL_VIRTUAL_REBALANCE_UNIT_MIGRATE_STAT_TID))) {
    LOG_WARN("fail to get table schema", K(ret));
  } else if (OB_FAIL(server_balance_plan_.generate_server_balance_plan())) {
    LOG_WARN("fail to generate server balance plan", K(ret));
  } else {
  }  // no more to do
  return ret;
}

int ObAllRebalanceUnitMigrateStat::inner_get_next_row(common::ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ret = server_balance_plan_.get_next_task(task_);
    if (OB_ITER_END == ret) {
      // to the end
    } else if (OB_SUCC(ret)) {
      ObArray<Column> columns;
      if (OB_FAIL(get_full_row(task_, columns))) {
        LOG_WARN("fail to get full row", K(ret));
      } else if (OB_FAIL(project_row(columns, cur_row_))) {
        LOG_WARN("fail to project row", K(ret));
      } else {
        row = &cur_row_;
      }
    } else {
      LOG_WARN("fail to get next task", K(ret));
    }
  }
  return ret;
}

int ObAllRebalanceUnitMigrateStat::get_full_row(const ServerBalancePlanTask& task, common::ObIArray<Column>& columns)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (NULL == table_schema_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("virtual table schema ptr is null", K(ret), KP(table_schema_));
  } else {
    if (!task.src_addr_.ip_to_string(src_ip_buf_, sizeof(src_ip_buf_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to execute ip to string", K(ret));
    } else if (!task.dst_addr_.ip_to_string(dst_ip_buf_, sizeof(dst_ip_buf_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail ot exeucte ip to string", K(ret));
    } else {
      ADD_COLUMN(set_int, table_schema_, "unit_id", task.unit_id_, columns);
      ADD_COLUMN(set_varchar, table_schema_, "zone", task.zone_.str(), columns);
      ADD_COLUMN(set_varchar, table_schema_, "src_svr_ip", src_ip_buf_, columns);
      ADD_COLUMN(set_int, table_schema_, "src_svr_port", task.src_addr_.get_port(), columns);
      ADD_COLUMN(set_varchar, table_schema_, "dst_svr_ip", dst_ip_buf_, columns);
      ADD_COLUMN(set_int, table_schema_, "dst_svr_port", task.dst_addr_.get_port(), columns);
    }
  }
  return ret;
}
