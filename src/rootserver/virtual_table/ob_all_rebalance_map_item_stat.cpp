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
#include "ob_all_rebalance_map_item_stat.h"
#include "rootserver/ob_root_utils.h"
#include "rootserver/ob_partition_disk_balancer.h"
#include "rootserver/ob_partition_balancer.h"
#include "rootserver/ob_balance_group_data.h"
#include "share/schema/ob_multi_version_schema_service.h"

using namespace oceanbase::rootserver;
using namespace oceanbase::rootserver::balancer;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

ObAllRebalanceMapItemStat::ObAllRebalanceMapItemStat()
    : inited_(false),
      schema_service_(NULL),
      tenant_balance_stat_(),
      index_map_(),
      all_tenants_(),
      tenant_maps_(),
      cur_tenant_idx_(-1),
      cur_map_idx_(-1),
      cur_map_size_(-1),
      cur_zone_idx_(-1),
      cur_item_idx_(-1),
      schema_guard_(),
      table_schema_(NULL),
      columns_()
{}

ObAllRebalanceMapItemStat::~ObAllRebalanceMapItemStat()
{}

int ObAllRebalanceMapItemStat::init(share::schema::ObMultiVersionSchemaService& schema_service, ObUnitManager& unit_mgr,
    ObServerManager& server_mgr, share::ObPartitionTableOperator& pt_operator,
    share::ObRemotePartitionTableOperator& remote_pt_operator, ObZoneManager& zone_mgr, ObRebalanceTaskMgr& task_mgr,
    share::ObCheckStopProvider& check_stop_provider)
{
  int ret = OB_SUCCESS;
  const int64_t HASH_MAP_SIZE = 100 * 1024;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObAllRebalanceMapItemStat already inited", K(ret));
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
  } else if (OB_FAIL(index_map_.init(HASH_MAP_SIZE))) {
    LOG_WARN("fail to create", K(ret));
  } else {
    schema_service_ = &schema_service;
    inited_ = true;
  }
  return ret;
}

int ObAllRebalanceMapItemStat::inner_open()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(tenant_balance_stat_.reuse_replica_count_mgr())) {
    LOG_WARN("reuse_replica_count_mgr failed", K(ret));
  } else if (OB_FAIL(get_all_tenant())) {
    LOG_WARN("fail to get all maps", K(ret));
  } else if (OB_FAIL(get_table_schema(OB_ALL_VIRTUAL_REBALANCE_MAP_ITEM_STAT_TID))) {
    LOG_WARN("fail to get table schema", K(ret));
  } else {
    cur_tenant_idx_ = -1;
    cur_map_idx_ = -1;
    cur_map_size_ = -1;
    cur_zone_idx_ = -1;
    cur_item_idx_ = -1;
  }
  return ret;
}

int ObAllRebalanceMapItemStat::inner_get_next_row(common::ObNewRow*& row)
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

// sequence: items->zones->maps->tenants
int ObAllRebalanceMapItemStat::next()
{
  int ret = OB_SUCCESS;
  bool try_again = false;
  do {
    try_again = false;
    ++cur_item_idx_;
    if (cur_item_idx_ >= cur_map_size_) {
      // end of current zone, move to next zone
      if (OB_FAIL(next_zone())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail move to next zone", K(ret));
        }
      } else if (OB_FAIL(rebuild_map())) {
        LOG_WARN("fail build map", K(ret));
      }
      cur_item_idx_ = -1;
      try_again = true;
    }
  } while (try_again && OB_SUCC(ret));
  return ret;
}

int ObAllRebalanceMapItemStat::next_zone()
{
  int ret = OB_SUCCESS;
  bool try_again = false;
  do {
    try_again = false;
    cur_zone_idx_++;
    if (cur_zone_idx_ >= tenant_balance_stat_.all_zone_unit_.count()) {
      // end of current map, move to next map
      if (OB_FAIL(next_map())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail move to next map", K(ret));
        }
      }
      cur_zone_idx_ = -1;
      try_again = true;
    }
  } while (try_again && OB_SUCC(ret));
  return ret;
}

int ObAllRebalanceMapItemStat::next_map()
{
  int ret = OB_SUCCESS;
  bool try_again = false;
  do {
    try_again = false;
    cur_map_idx_++;
    if (cur_map_idx_ >= tenant_maps_.count()) {
      // end of current tenant, move to next tenant
      if (OB_FAIL(next_tenant())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail move to next tenant", K(ret));
        }
      }
      cur_map_idx_ = -1;
      try_again = true;
    }
  } while (try_again && OB_SUCC(ret));
  return ret;
}

int ObAllRebalanceMapItemStat::next_tenant()
{
  int ret = OB_SUCCESS;
  ++cur_tenant_idx_;
  if (cur_tenant_idx_ >= all_tenants_.count()) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(init_tenant_balance_stat(all_tenants_.at(cur_tenant_idx_)))) {
    LOG_WARN("fail init tenant balance stat", K_(cur_tenant_idx), K_(cur_map_idx), K(ret));
  }
  return ret;
}

int ObAllRebalanceMapItemStat::init_tenant_balance_stat(uint64_t tenant_id)
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
    tenant_maps_.reuse();
    index_map_.reuse();
    TenantSchemaGetter stat_finder(tenant_id);
    ObLeaderBalanceGroupContainer balance_group_container(schema_guard_, stat_finder, allocator_);
    if (OB_FAIL(balance_group_container.init(tenant_id))) {
      LOG_WARN("fail to init balance group container", K(ret), K(tenant_id));
    } else if (OB_FAIL(balance_group_container.build())) {
      LOG_WARN("fail to build balance index builder", K(ret));
    } else if (OB_FAIL(tenant_balance_stat_.gather_stat(
                   tenant_id, &schema_guard_, balance_group_container.get_hash_index()))) {
      LOG_WARN("gather tenant balance statistics failed", K(ret), K(tenant_id));
    } else {
      // do it
      balancer::ITenantStatFinder& stat_finder = tenant_balance_stat_;
      balancer::ObPartitionBalanceGroupContainer container(schema_guard_, stat_finder, allocator_);
      if (OB_FAIL(container.init(tenant_id))) {
        LOG_WARN("fail to init", K(ret));
      } else if (OB_FAIL(container.build())) {
        LOG_WARN("fail to build", K(ret));
      } else if (OB_FAIL(tenant_maps_.assign(container.get_square_id_map_array()))) {
        LOG_WARN("fail to assign tenant maps", K(ret));
      } else if (OB_FAIL(index_map_.assign(container.get_hash_index()))) {
        LOG_WARN("fail to assign index map", K(ret));
      }
    }
  }
  return ret;
}

int ObAllRebalanceMapItemStat::rebuild_map()
{
  int ret = OB_SUCCESS;
  if (cur_map_idx_ < 0 || cur_map_idx_ >= tenant_maps_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected map idx", K_(cur_map_idx), "count", tenant_maps_.count(), K(ret));
  } else if (cur_zone_idx_ < 0 || cur_zone_idx_ >= tenant_balance_stat_.all_zone_unit_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected zone idx", K_(cur_zone_idx), "count", tenant_balance_stat_.all_zone_unit_.count(), K(ret));
  } else {
    SquareIdMap& map = *tenant_maps_.at(cur_map_idx_);
    ObPartitionUnitProvider unit_provider(tenant_balance_stat_.all_zone_unit_.at(cur_zone_idx_), tenant_balance_stat_);
    ObZone& zone = tenant_balance_stat_.all_zone_unit_.at(cur_zone_idx_).zone_;
    DynamicAverageDiskBalancer balancer(map, tenant_balance_stat_, unit_provider, zone, schema_guard_, index_map_);
    if (OB_FAIL(calc_leader_balance_statistic(zone, index_map_, map))) {
      LOG_WARN("fail to calc leader balance statistic", K(ret));
    } else if (!map.is_valid()) {
      LOG_WARN("map is invalid and can not do balance. check detail info by query inner table",
          "inner_table",
          OB_ALL_VIRTUAL_REBALANCE_MAP_STAT_TNAME,
          K(map),
          K(ret));
    } else if (OB_FAIL(balancer.balance())) {  // fill unit_id and dest_unit_id
      if (OB_ENTRY_NOT_EXIST == ret) {
        LOG_INFO("unit not determined for some replica. ignore balance map for now", K(map), K(ret));
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail balance map", K(map), K(ret));
      }
    }
    cur_map_size_ = map.size();
    cur_item_idx_ = -1;
  }
  return ret;
}

int ObAllRebalanceMapItemStat::get_row()
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

int ObAllRebalanceMapItemStat::calc_leader_balance_statistic(
    const common::ObZone& zone, const HashIndexCollection& hash_index_collection, SquareIdMap& id_map)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (zone.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (id_map.ignore_leader_balance()) {
    // bypass
  } else if (OB_FAIL(id_map.update_leader_balance_info(zone, hash_index_collection))) {
    LOG_WARN("fail to update leader balance info", K(ret));
  }
  return ret;
}

int ObAllRebalanceMapItemStat::get_full_row(
    const share::schema::ObTableSchema* table, common::ObIArray<Column>& columns)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (NULL == table) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table is null", K(ret));
  } else if (cur_map_idx_ >= tenant_maps_.count() || cur_map_idx_ < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected branch", K_(cur_map_idx), K(ret));
  } else if (cur_zone_idx_ >= tenant_balance_stat_.all_zone_unit_.count() || cur_zone_idx_ < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected branch", K_(cur_zone_idx), K(ret));
  } else {
    const balancer::SquareIdMap& map = *tenant_maps_.at(cur_map_idx_);
    balancer::SquareIdMap::Item item;
    cur_map_size_ = map.size();
    if (cur_item_idx_ >= cur_map_size_ || cur_item_idx_ < 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected branch", K_(cur_item_idx), K(ret));
    } else if (OB_FAIL(map.get(cur_item_idx_, item))) {
      LOG_WARN("fail get item from map", K(map), K_(cur_item_idx), K(ret));
    } else {
      uint64_t tenant_id = map.get_tenant_id();
      const ObZone& zone = tenant_balance_stat_.all_zone_unit_.at(cur_zone_idx_).zone_;
      if (tenant_id != tenant_balance_stat_.get_tenant_id()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant id not match", K(tenant_id), "other", tenant_balance_stat_.get_tenant_id(), K(ret));
      } else {
        ADD_COLUMN(set_int, table, "tenant_id", map.get_tenant_id(), columns);
        ADD_COLUMN(set_varchar, table, "zone", zone.str(), columns);
        ADD_COLUMN(set_int, table, "tablegroup_id", item.get_tablegroup_id(), columns);
        ADD_COLUMN(set_int, table, "table_id", item.get_table_id(), columns);
        ADD_COLUMN(set_int, table, "map_type", map.get_map_type(), columns);
        ADD_COLUMN(set_int, table, "row_size", map.get_row_size(), columns);
        ADD_COLUMN(set_int, table, "col_size", map.get_col_size(), columns);
        ADD_COLUMN(set_int, table, "part_idx", item.get_partition_idx(), columns);
        ADD_COLUMN(set_int, table, "designated_role", (item.is_designated_leader() ? 1 : 2), columns);
        ADD_COLUMN(set_int, table, "unit_id", item.get_unit_id(), columns);
        ADD_COLUMN(set_int, table, "dest_unit_id", item.get_dest_unit_id(), columns);
      }
    }
  }
  return ret;
}

int ObAllRebalanceMapItemStat::get_all_tenant()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAllRebalanceMapItemStat not init", K(ret), K(inited_));
  } else if (OB_FAIL(schema_guard_.get_tenant_ids(all_tenants_))) {
    LOG_WARN("fail to get_tenant_ids", K(ret));
  }
  return ret;
}

int ObAllRebalanceMapItemStat::get_table_schema(uint64_t tid)
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
