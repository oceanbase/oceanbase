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
#include "ob_all_rebalance_unit_distribution_stat.h"

using namespace oceanbase::rootserver;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

ObAllRebalanceUnitDistributionStat::ObAllRebalanceUnitDistributionStat(ObUnitManager& unit_mgr,
    ObILeaderCoordinator& leader_coordinator, ObServerManager& server_mgr, ObZoneManager& zone_mgr)
    : server_balance_plan_(unit_mgr, leader_coordinator, server_mgr, zone_mgr),
      schema_guard_(),
      svr_ip_buf_(),
      unit_info_(),
      inited_(false)
{}

ObAllRebalanceUnitDistributionStat::~ObAllRebalanceUnitDistributionStat()
{}

int ObAllRebalanceUnitDistributionStat::init(common::ObMySQLProxy& proxy, common::ObServerConfig& server_config,
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

int ObAllRebalanceUnitDistributionStat::get_table_schema(uint64_t tid)
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

int ObAllRebalanceUnitDistributionStat::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(get_table_schema(OB_ALL_VIRTUAL_REBALANCE_UNIT_DISTRIBUTION_STAT_TID))) {
    LOG_WARN("fail to get table schema", K(ret));
  } else if (OB_FAIL(server_balance_plan_.generate_server_balance_plan())) {
    LOG_WARN("fail to generate server balance plan", K(ret));
  } else {
  }  // no more to do
  return ret;
}

int ObAllRebalanceUnitDistributionStat::inner_get_next_row(common::ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ret = server_balance_plan_.get_next_unit_distribution(unit_info_);
    if (OB_ITER_END == ret) {
      // to the end
    } else if (OB_SUCC(ret)) {
      ObArray<Column> columns;
      if (OB_FAIL(get_full_row(unit_info_, columns))) {
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

int ObAllRebalanceUnitDistributionStat::get_full_row(
    const share::ObUnitInfo& unit_info, common::ObIArray<Column>& columns)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (NULL == table_schema_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("virtual table schema ptr is null", K(ret), KP(table_schema_));
  } else {
    if (!unit_info.unit_.server_.ip_to_string(svr_ip_buf_, sizeof(svr_ip_buf_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to execute ip to string", K(ret));
    } else {
      ADD_COLUMN(set_int, table_schema_, "unit_id", unit_info.unit_.unit_id_, columns);
      ADD_COLUMN(set_int, table_schema_, "tenant_id", unit_info.pool_.tenant_id_, columns);
      ADD_COLUMN(set_varchar, table_schema_, "svr_ip", svr_ip_buf_, columns);
      ADD_COLUMN(set_int, table_schema_, "svr_port", unit_info.unit_.server_.get_port(), columns);
      ADD_COLUMN(set_varchar, table_schema_, "zone", unit_info.unit_.zone_.str(), columns);
      ADD_COLUMN(set_double, table_schema_, "max_cpu", unit_info.config_.max_cpu_, columns);
      ADD_COLUMN(set_double, table_schema_, "min_cpu", unit_info.config_.min_cpu_, columns);
      ADD_COLUMN(set_int, table_schema_, "max_memory", unit_info.config_.max_memory_, columns);
      ADD_COLUMN(set_int, table_schema_, "min_memory", unit_info.config_.min_memory_, columns);
      ADD_COLUMN(set_int, table_schema_, "max_iops", unit_info.config_.max_iops_, columns);
      ADD_COLUMN(set_int, table_schema_, "min_iops", unit_info.config_.min_iops_, columns);
      ADD_COLUMN(set_int, table_schema_, "max_disk_size", unit_info.config_.max_disk_size_, columns);
      ADD_COLUMN(set_int, table_schema_, "max_session_num", unit_info.config_.max_session_num_, columns);
    }
  }
  return ret;
}
