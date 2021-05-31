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
#include "ob_all_rebalance_tenant_stat.h"
#include "rootserver/ob_root_utils.h"
#include "rootserver/ob_unit_manager.h"
#include "rootserver/ob_balance_group_container.h"

#include "share/schema/ob_multi_version_schema_service.h"

using namespace oceanbase::rootserver;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::rootserver::balancer;

ObAllRebalanceTenantStat::ObAllRebalanceTenantStat()
    : inited_(false),
      schema_service_(NULL),
      tenant_balance_stat_(),
      all_tenants_(),
      cur_tenant_idx_(-1),
      cur_zone_idx_(-1),
      schema_guard_(),
      table_schema_(NULL),
      columns_()
{}

ObAllRebalanceTenantStat::~ObAllRebalanceTenantStat()
{}

int ObAllRebalanceTenantStat::init(share::schema::ObMultiVersionSchemaService& schema_service, ObUnitManager& unit_mgr,
    ObServerManager& server_mgr, share::ObPartitionTableOperator& pt_operator,
    share::ObRemotePartitionTableOperator& remote_pt_operator, ObZoneManager& zone_mgr, ObRebalanceTaskMgr& task_mgr,
    share::ObCheckStopProvider& check_stop_provider)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObAllRebalanceTenantStat already inited", K(ret));
  } else if (OB_FAIL(tenant_balance_stat_.init(unit_mgr,
                 server_mgr,
                 pt_operator,
                 remote_pt_operator,
                 zone_mgr,
                 task_mgr,
                 check_stop_provider,
                 unit_mgr.get_sql_proxy()))) {
    LOG_WARN("failed to init tenant_balance_stat", K(ret));
  } else if (OB_FAIL(schema_service.get_schema_guard(schema_guard_))) {
    LOG_WARN("fail to get schema guard", K(ret));
  } else {
    schema_service_ = &schema_service;
    inited_ = true;
  }
  return ret;
}

int ObAllRebalanceTenantStat::inner_open()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(tenant_balance_stat_.reuse_replica_count_mgr())) {
    LOG_WARN("reuse_replica_count_mgr failed", K(ret));
  } else if (OB_FAIL(get_all_tenant())) {
    LOG_WARN("fail to get all tenant", K(ret));
  } else if (OB_FAIL(get_table_schema(OB_ALL_VIRTUAL_REBALANCE_TENANT_STAT_TID))) {
    LOG_WARN("fail to get table schema", K(ret));
  } else {
    cur_tenant_idx_ = -1;
    cur_zone_idx_ = -1;
  }
  return ret;
}

int ObAllRebalanceTenantStat::inner_get_next_row(common::ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    if (OB_FAIL(next())) {
    } else if (OB_FAIL(get_row())) {
    } else {
      row = &cur_row_;
    }
  }
  return ret;
}

int ObAllRebalanceTenantStat::next()
{
  int ret = OB_SUCCESS;
  bool loop = false;
  do {
    loop = false;
    ++cur_zone_idx_;
    if (cur_zone_idx_ >= tenant_balance_stat_.all_zone_unit_.count()) {
      ++cur_tenant_idx_;
      if (cur_tenant_idx_ >= all_tenants_.count()) {
        ret = OB_ITER_END;
      } else {
        if (OB_FAIL(init_tenant_balance_stat(all_tenants_.at(cur_tenant_idx_)))) {
        } else {
          cur_zone_idx_ = -1;
          loop = true;
        }
      }
    }
  } while (loop);
  return ret;
}

int ObAllRebalanceTenantStat::get_row()
{
  int ret = OB_SUCCESS;
  columns_.reuse();
  if (OB_FAIL(get_full_row(table_schema_, columns_))) {
    LOG_WARN("fail to get full row", K(ret));
  } else if (OB_FAIL(project_row(columns_, cur_row_))) {
    LOG_WARN("fail to project row", K_(columns), K(ret));
  }
  return ret;
}

int ObAllRebalanceTenantStat::get_full_row(const share::schema::ObTableSchema* table, common::ObIArray<Column>& columns)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (NULL == table) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table is null", K(ret));
  } else if (cur_zone_idx_ >= tenant_balance_stat_.all_zone_unit_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected branch");
  } else {
    const ZoneUnit& zu = tenant_balance_stat_.all_zone_unit_.at(cur_zone_idx_);
    // primary key
    ADD_COLUMN(set_int, table, "tenant_id", tenant_balance_stat_.tenant_id_, columns);
    ADD_COLUMN(set_varchar, table, "zone", zu.zone_.str(), columns);
    // other columns
    ADD_COLUMN(set_double, table, "cpu_weight", tenant_balance_stat_.resource_weight_.cpu_weight_, columns);
    ADD_COLUMN(set_double, table, "disk_weight", tenant_balance_stat_.resource_weight_.disk_weight_, columns);
    ADD_COLUMN(set_double, table, "iops_weight", tenant_balance_stat_.resource_weight_.iops_weight_, columns);
    ADD_COLUMN(set_double, table, "memory_weight", tenant_balance_stat_.resource_weight_.memory_weight_, columns);

    ADD_COLUMN(set_double, table, "load_imbalance", zu.load_imbalance_, columns);
    ADD_COLUMN(set_double, table, "load_avg", zu.load_avg_, columns);
    ADD_COLUMN(set_double, table, "cpu_imbalance", zu.cpu_imbalance_, columns);
    ADD_COLUMN(set_double, table, "cpu_avg", zu.cpu_avg_, columns);
    ADD_COLUMN(set_double, table, "disk_imbalance", zu.disk_imbalance_, columns);
    ADD_COLUMN(set_double, table, "disk_avg", zu.disk_avg_, columns);
    ADD_COLUMN(set_double, table, "iops_imbalance", zu.iops_imbalance_, columns);
    ADD_COLUMN(set_double, table, "iops_avg", zu.iops_avg_, columns);
    ADD_COLUMN(set_double, table, "memory_imbalance", zu.memory_imbalance_, columns);
    ADD_COLUMN(set_double, table, "memory_avg", zu.memory_avg_, columns);
  }
  return ret;
}

int ObAllRebalanceTenantStat::get_all_tenant()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAllRebalanceTenantStat not init", K(ret), K(inited_));
  } else if (OB_FAIL(schema_guard_.get_tenant_ids(all_tenants_))) {
    LOG_WARN("fail to get_tenant_ids", K(ret));
  } else {
  }  // no more to do
  return ret;
}

int ObAllRebalanceTenantStat::get_table_schema(uint64_t tid)
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
  } else {
  }
  return ret;
}

int ObAllRebalanceTenantStat::init_tenant_balance_stat(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", K(ret));
  } else {
    tenant_balance_stat_.reuse();
    common::ObArenaAllocator allocator(ObModIds::OB_RS_PARTITION_BALANCER);
    TenantSchemaGetter stat_finder(tenant_id);
    ObLeaderBalanceGroupContainer balance_group_container(schema_guard_, stat_finder, allocator);
    if (OB_FAIL(balance_group_container.init(tenant_id))) {
      LOG_WARN("fail to init balance group container", K(ret), K(tenant_id));
    } else if (OB_FAIL(balance_group_container.build())) {
      LOG_WARN("fail to build balance index builder", K(ret));
    } else if (OB_FAIL(tenant_balance_stat_.gather_stat(
                   tenant_id, &schema_guard_, balance_group_container.get_hash_index()))) {
      LOG_WARN("gather tenant balance statistics failed", K(ret), K(tenant_id));
    } else {
    }
  }
  return ret;
}
