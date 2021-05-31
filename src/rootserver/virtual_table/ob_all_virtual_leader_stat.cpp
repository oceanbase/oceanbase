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
#include "ob_all_virtual_leader_stat.h"
#include "observer/ob_server_struct.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "rootserver/ob_root_utils.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

namespace oceanbase {
namespace rootserver {
int ObAllVirtualLeaderStat::init(
    rootserver::ObLeaderCoordinator& leader_coordinator, share::schema::ObMultiVersionSchemaService& schema_service)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObAllVirtualLeaderStat init twice", K(ret));
  } else if (OB_FAIL(partition_info_container_.init(GCTX.pt_operator_, &schema_service, &leader_coordinator))) {
    LOG_WARN("fail to init partition info container", K(ret));
  } else {
    leader_coordinator_ = &leader_coordinator;
    schema_service_ = &schema_service;
    is_inited_ = true;
  }
  return ret;
}

int ObAllVirtualLeaderStat::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAllVirtualLeaderStat not init", K(ret), K(is_inited_));
  } else if (OB_FAIL(schema_service_->get_schema_guard(schema_guard_))) {
    LOG_WARN("fail to get schema guard", K(ret));
  } else if (OB_FAIL(get_all_tenant())) {
    LOG_WARN("fail to get all tenant", K(ret));
  } else if (OB_FAIL(get_table_schema(OB_ALL_VIRTUAL_LEADER_STAT_TID))) {
    LOG_WARN("fail to get table schema", K(ret));
  } else {
  }  // no more to do
  return ret;
}

int ObAllVirtualLeaderStat::get_all_tenant()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAllVirtualLeaderStat not init", K(ret), K(is_inited_));
  } else if (OB_FAIL(schema_guard_.get_tenant_ids(all_tenants_))) {
    LOG_WARN("fail to get_tenant_ids", K(ret));
  } else {
  }  // no more to do
  return ret;
}

int ObAllVirtualLeaderStat::get_table_schema(uint64_t table_id)
{
  int ret = OB_SUCCESS;
  const uint64_t my_table_id = combine_id(OB_SYS_TENANT_ID, table_id);
  if (OB_UNLIKELY(!is_inited_)) {
    LOG_WARN("ObAllVirtualLeaderStat not init", K(ret));
  } else if (OB_FAIL(schema_guard_.get_table_schema(my_table_id, table_schema_))) {
    LOG_WARN("fail to get table schema", K(table_id), K(my_table_id), K(ret));
  } else if (NULL == table_schema_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(ret));
  } else {
  }  // no more to do
  return ret;
}

int ObAllVirtualLeaderStat::inner_get_next_row(common::ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAllVirtualLeaderStat not init", K(ret));
  } else if (OB_FAIL(next())) {
    LOG_WARN("fail to go on next", K(ret));
  } else if (OB_FAIL(get_row())) {
    LOG_WARN("fail to get row", K(ret));
  } else {
    row = &cur_row_;
  }
  return ret;
}

int ObAllVirtualLeaderStat::build_partition_group_candidate_leader_array()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAllVirtualLeaderStat not init", K(ret));
  } else if (partition_group_idx_ >= tenant_partition_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition group idx out of range",
        K(ret),
        K(partition_group_idx_),
        "tenant partition group cnt",
        tenant_partition_.count());
  } else {
    ObLeaderCoordinator::PartitionArray& partitions = *tenant_partition_.at(partition_group_idx_);
    ObLeaderCoordinator::CandidateLeaderInfoMap leader_info_map(*leader_coordinator_);
    const int64_t MAP_BUCKET_NUM = 64;
    bool is_ignore_switch_percent = false;
    const common::ObArray<common::ObZone> exclude_zones;
    const common::ObArray<common::ObAddr> exclude_servers;
    if (OB_FAIL(leader_info_map.create(MAP_BUCKET_NUM, ObModIds::OB_RS_LEADER_COORDINATOR))) {
      LOG_WARN("fail to create leader info map", K(ret));
    } else if (OB_FAIL(leader_coordinator_->build_partition_statistic(partitions,
                   tenant_unit_,
                   exclude_zones,
                   exclude_servers,
                   leader_info_map,
                   is_ignore_switch_percent))) {
      LOG_WARN("fail to build partition statistic", K(ret));
    } else if (OB_FAIL(leader_coordinator_->update_leader_candidates(partitions, leader_info_map))) {
      LOG_WARN("fail to update leader candidates", K(ret));
    } else {
      candidate_leader_info_array_.reset();
      candidate_leader_info_idx_ = -1;  // reset back to -1 when a new array is constructed
      for (ObLeaderCoordinator::CandidateLeaderInfoMap::const_iterator iter = leader_info_map.begin();
           OB_SUCC(ret) && iter != leader_info_map.end();
           ++iter) {
        const ObLeaderCoordinator::CandidateLeaderInfo& info = iter->second;
        if (OB_FAIL(candidate_leader_info_array_.push_back(info))) {
          LOG_WARN("fail to push back candidate leader info array", K(ret));
        } else {
        }  // ok
      }
    }
  }
  return ret;
}

int ObAllVirtualLeaderStat::construct_tenant_partition()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAllVirtualLeaderStat not init", K(ret));
  } else if (cur_tenant_idx_ >= all_tenants_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant idx out of range", K(ret), K(cur_tenant_idx_), "all tenants count", all_tenants_.count());
  } else if (OB_FAIL(do_construct_tenant_partition(all_tenants_.at(cur_tenant_idx_)))) {
    LOG_WARN("fail to do construct tenant partition", K(ret), "tenant id", all_tenants_.at(cur_tenant_idx_));
  } else {
  }  // no more to do
  return ret;
}

int ObAllVirtualLeaderStat::do_construct_tenant_partition(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else {
    candidate_leader_info_idx_ = -1;
    candidate_leader_info_array_.reset();
    partition_group_idx_ = -1;
    tenant_partition_.reset();
    inner_allocator_.reset();
    tenant_unit_.reset();
    if (OB_FAIL(partition_info_container_.build_tenant_partition_info(tenant_id))) {
      LOG_WARN("fail to build tenants partition info", K(ret));
    } else if (OB_FAIL(leader_coordinator_->build_tenant_partition(
                   schema_guard_, partition_info_container_, tenant_id, inner_allocator_, tenant_partition_))) {
      LOG_WARN("fail to build tenant partition", K(ret));
    } else if (OB_FAIL(leader_coordinator_->get_tenant_unit(tenant_id, tenant_unit_))) {
      LOG_WARN("fail to get tenant unit", K(ret), K(tenant_id));
    } else if (OB_FAIL(leader_coordinator_->build_dm_partition_candidates(tenant_partition_))) {
      LOG_WARN("fail to build dm partition candidates", K(ret));
    }
  }
  return ret;
}

int ObAllVirtualLeaderStat::next()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAllVirtualLeaderStat not init", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      if (OB_FAIL(next_candidate_leader())) {
        LOG_WARN("fail to move to next candidate leader", K(ret));
      } else if (!reach_candidate_leader_array_end()) {
        break;  // do not reach candidate leader array end, ok
      } else if (OB_FAIL(next_partition_group())) {
        LOG_WARN("fail to move to next partition group", K(ret));
      } else if (!reach_tenant_end()) {
        if (OB_FAIL(build_partition_group_candidate_leader_array())) {
          LOG_WARN("fail to build partition group candidate leader array", K(ret));
        } else {
        }  // no more
      } else if (OB_FAIL(next_tenant())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to move to next tenant", K(ret));
        } else {
        }  // iter end
      } else if (OB_FAIL(construct_tenant_partition())) {
        LOG_WARN("fail to construct tenant partition", K(ret));
      } else {
      }  // no more to do
    }
  }
  return ret;
}

int ObAllVirtualLeaderStat::next_candidate_leader()
{
  int ret = OB_SUCCESS;
  ++candidate_leader_info_idx_;
  return ret;
}

bool ObAllVirtualLeaderStat::reach_candidate_leader_array_end()
{
  bool reach_end = false;
  if (tenant_partition_.count() <= 0) {
    reach_end = true;
  } else if (candidate_leader_info_idx_ >= candidate_leader_info_array_.count()) {
    reach_end = true;
  } else {
    reach_end = false;
  }
  return reach_end;
}

int ObAllVirtualLeaderStat::next_partition_group()
{
  int ret = OB_SUCCESS;
  ++partition_group_idx_;
  return ret;
}

bool ObAllVirtualLeaderStat::reach_tenant_end()
{
  bool reach_end = false;
  if (partition_group_idx_ >= tenant_partition_.count()) {
    reach_end = true;
  } else {
    reach_end = false;
  }
  return reach_end;
}

int ObAllVirtualLeaderStat::next_tenant()
{
  int ret = OB_SUCCESS;
  ++cur_tenant_idx_;
  if (cur_tenant_idx_ >= all_tenants_.count()) {
    ret = OB_ITER_END;
  } else {
  }  // no more to do
  return ret;
}

int ObAllVirtualLeaderStat::get_row()
{
  int ret = OB_SUCCESS;
  columns_.reuse();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAllVirtualLeaderStat not init", K(ret));
  } else if (OB_FAIL(get_full_row(table_schema_, columns_))) {
    LOG_WARN("fail to get full row", K(ret));
  } else if (OB_FAIL(project_row(columns_, cur_row_))) {
    LOG_WARN("fail to project row", K(columns_), K(ret));
  } else {
  }  // no more to do
  return ret;
}

int ObAllVirtualLeaderStat::get_full_row(const share::schema::ObTableSchema* table, common::ObIArray<Column>& columns)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(NULL == table)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, table is null", K(ret), KP(table));
  } else {
    const ObLeaderCoordinator::CandidateLeaderInfo& info = candidate_leader_info_array_.at(candidate_leader_info_idx_);
    const ObLeaderCoordinator::PartitionArray& partitions = *tenant_partition_.at(partition_group_idx_);
    ObPartitionKey pkey;
    bool small_tenant = false;
    char ip_buf[MAX_IP_ADDR_LENGTH];
    if (partitions.count() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partitions is empty", K(ret), "partition count", partitions.count());
    } else if (OB_FAIL(partitions.at(0).get_partition_key(pkey))) {
      LOG_WARN("get_partition_key failed", K(ret), "partition", partitions.at(0));
    } else if (!pkey.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pkey is invalid", K(ret), K(pkey));
    } else if (OB_FAIL(ObTenantUtils::check_small_tenant(pkey.get_tenant_id(), small_tenant))) {
      LOG_WARN("fail to check small tenant", K(ret), "tenant_id", pkey.get_tenant_id());
    } else if (!info.server_addr_.ip_to_string(ip_buf, MAX_IP_ADDR_LENGTH)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to format server ip", K(ret));
    } else {
      // primary key
      ADD_COLUMN(set_int, table, "tenant_id", pkey.get_tenant_id(), columns);
      ADD_COLUMN(set_int, table, "tablegroup_id", partitions.at(0).tg_id_, columns);
      ADD_COLUMN(set_int, table, "partition_id", (small_tenant ? 0 : pkey.get_partition_id()), columns);
      ADD_COLUMN(set_varchar, table, "svr_ip", ip_buf, columns);
      ADD_COLUMN(set_int, table, "svr_port", info.server_addr_.get_port(), columns);
      // other columns
      bool primary_zone_null = (partitions.get_primary_zone().size() <= 0);
      bool region_null = (info.region_score_.region_.size() <= 0);
      bool zone_null = (info.zone_score_.zone_.size() <= 0);
      ADD_COLUMN(
          set_varchar, table, "primary_zone", (primary_zone_null ? "" : partitions.get_primary_zone().ptr()), columns);
      ADD_COLUMN(set_varchar, table, "region", (region_null ? "" : info.region_score_.region_.ptr()), columns);
      ADD_COLUMN(set_int, table, "region_score", info.region_score_.region_score_, columns);
      ADD_COLUMN(set_int, table, "not_merging", static_cast<int64_t>(info.not_merging_), columns);
      ADD_COLUMN(set_int, table, "candidate_count", info.candidate_count_, columns);
      ADD_COLUMN(set_int, table, "is_candidate", static_cast<int64_t>(info.is_candidate_), columns);
      ADD_COLUMN(set_int, table, "migrate_out_or_transform_count", info.migrate_out_or_transform_count_, columns);
      ADD_COLUMN(set_int, table, "in_normal_unit_count", info.in_normal_unit_count_, columns);
      ADD_COLUMN(set_varchar, table, "zone", (zone_null ? "" : info.zone_score_.zone_.ptr()), columns);
      ADD_COLUMN(set_int, table, "zone_score", info.zone_score_.zone_score_, columns);
      ADD_COLUMN(set_int, table, "original_leader_count", info.in_normal_unit_count_, columns);
      ADD_COLUMN(set_int, table, "random_score", info.random_score_, columns);
    }
  }
  return ret;
}

}  // namespace rootserver
}  // namespace oceanbase
