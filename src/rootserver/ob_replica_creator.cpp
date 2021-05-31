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
#include "ob_replica_creator.h"
#include "rootserver/ob_zone_manager.h"
#include "rootserver/ob_alloc_replica_strategy.h"
#include "rootserver/ob_partition_creator.h"
#include "rootserver/ob_balance_group_container.h"
#include "lib/container/ob_se_array_iterator.h"
#include "lib/container/ob_array_iterator.h"
#include "share/ob_zone_info.h"
#include "share/ob_replica_info.h"
#include "share/ob_partition_modify.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_table_schema.h"
#include "share/partition_table/ob_replica_filter.h"
#include "share/partition_table/ob_partition_table_iterator.h"
#include "share/partition_table/ob_partition_table_operator.h"
#include "ob_server_manager.h"
#include "ob_unit_manager.h"
#include "ob_root_utils.h"
using namespace oceanbase::common;
using namespace oceanbase::rootserver;
using namespace oceanbase::rootserver::balancer;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

ObReplicaCreator::ObReplicaCreator()
    : inited_(false),
      schema_service_(NULL),
      zone_mgr_(NULL),
      unit_mgr_(NULL),
      server_mgr_(NULL),
      pt_operator_(NULL),
      check_stop_provider_(NULL)
{}

int ObReplicaCreator::init(share::schema::ObMultiVersionSchemaService& schema_service, ObUnitManager& unit_mgr,
    ObServerManager& server_mgr, share::ObPartitionTableOperator& pt_operator, ObZoneManager& zone_mgr,
    share::ObCheckStopProvider& check_stop_provider)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
  } else {
    schema_service_ = &schema_service;
    zone_mgr_ = &zone_mgr;
    unit_mgr_ = &unit_mgr;
    server_mgr_ = &server_mgr;
    pt_operator_ = &pt_operator;
    check_stop_provider_ = &check_stop_provider;
    inited_ = true;
  }
  return ret;
}

int ObReplicaCreator::alloc_tablegroup_partitions_for_create(const share::schema::ObTablegroupSchema& schema,
    const obrpc::ObCreateTableMode create_mode, common::ObIArray<ObPartitionAddr>& addr,
    ObIArray<TenantUnitRepCnt*>& ten_unit_arr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(create_mode <= obrpc::OB_CREATE_TABLE_MODE_INVALID ||
                         create_mode >= obrpc::OB_CREATE_TABLE_MODE_MAX)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(create_mode));
  } else {
    addr.reuse();
    const int64_t part_num = schema.get_all_part_num();
    if (OB_FAIL(addr.reserve(part_num))) {
      LOG_WARN("fail to reserve addr array", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < part_num; ++i) {
        if (OB_FAIL(addr.push_back(ObPartitionAddr()))) {
          LOG_WARN("fail to add empty tablegroup partition address", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      const uint64_t tenant_id = schema.get_tenant_id();
      share::schema::ObSchemaGetterGuard schema_guard;
      ObArenaAllocator allocator(ObModIds::OB_RS_PARTITION_TABLE_TEMP);
      TenantSchemaGetter stat_finder(tenant_id);
      ObSinglePtBalanceContainer balance_container(schema, schema_guard, stat_finder, allocator);
      common::ObSEArray<common::ObZone, 7> high_priority_zone_array;
      common::ObSEArray<share::ObRawPrimaryZoneUtil::ZoneScore, MAX_ZONE_NUM> zone_score_array;
      ObArray<int64_t> partition_ids;
      ObPartIdsGenerator id_gen(schema);
      bool is_non_part_table = false;
      if (!(schema.get_all_part_num() > 1 || !schema.has_self_partition())) {
        is_non_part_table = true;
      } else {
      }
      if (OB_UNLIKELY(nullptr == schema_service_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schema service ptr is null", K(ret), KP(schema_service_));
      } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
        LOG_WARN("fail to get schema guard", K(ret));
      } else if (OB_FAIL(build_single_pt_balance_container(
                     schema_guard, schema, balance_container, high_priority_zone_array, zone_score_array))) {
        LOG_WARN("fail to get build single pt balance container", K(ret), "tg_id", schema.get_tablegroup_id());
      } else if (OB_FAIL(process_replica_in_logonly_unit(schema, addr, create_mode, schema_guard))) {
        LOG_WARN("fail to process logonly replica", K(ret), "tg_id", schema.get_tablegroup_id());
      } else if (OB_FAIL(get_new_partitions(balance_container,
                     high_priority_zone_array,
                     schema,
                     addr,
                     create_mode,
                     ten_unit_arr,
                     is_non_part_table))) {
        LOG_WARN("fail to get new partitions", K(ret), "tg_id", schema.get_tablegroup_id());
      }
      if (OB_SUCC(ret)) {
        if (is_non_part_table && GCTX.is_primary_cluster() && ten_unit_arr.count() > 0) {
          if (OB_FAIL(set_non_part_leader(high_priority_zone_array, zone_score_array, schema, ten_unit_arr, addr))) {
            LOG_WARN("fail to set non part leader", K(ret));
          }
        } else {
          if (OB_FAIL(id_gen.gen(partition_ids))) {
            LOG_WARN("fail to generate phy_partition_id", K(ret), "tg_id", schema.get_tablegroup_id());
          } else if (OB_FAIL(set_initial_leader(
                         balance_container, high_priority_zone_array, schema, partition_ids, addr, schema_guard))) {
            LOG_WARN("fail to set initial leader", K(ret), "tg_id", schema.get_tablegroup_id());
          }
        }
      }
    }
  }
  return ret;
}

/* the address distribution strategy for new partition replicas:
 * when the new table is included in a table group, the corresponding
 * partition replicas are aligned to the partition replicas
 * already exist of this tablegroup.
 */
int ObReplicaCreator::alloc_partitions_for_create(const ObTableSchema& table, obrpc::ObCreateTableMode create_mode,
    ObITablePartitionAddr& addr, ObIArray<TenantUnitRepCnt*>& ten_unit_arr)
{
  int ret = OB_SUCCESS;
  RS_TRACE(alloc_replica_begin);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!table.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table", K(ret), K(table));
  } else if (create_mode == obrpc::OB_CREATE_TABLE_MODE_INVALID) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid create_mode", K(ret), K(create_mode));
  } else {
    addr.reuse();
    const int64_t part_num = table.get_all_part_num();
    if (OB_FAIL(addr.reserve(part_num))) {
      LOG_WARN("array reserve failed", K(ret), K(part_num));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < part_num; ++i) {
        if (OB_FAIL(addr.push_back(ObPartitionAddr()))) {
          LOG_WARN("add empty partition address failed", K(ret), K(i));
        }
      }
    }
    if (OB_SUCC(ret)) {
      const uint64_t tenant_id = table.get_tenant_id();
      share::schema::ObSchemaGetterGuard schema_guard;
      ObArenaAllocator allocator(ObModIds::OB_RS_PARTITION_TABLE_TEMP);
      TenantSchemaGetter stat_finder(tenant_id);
      ObSinglePtBalanceContainer balance_container(table, schema_guard, stat_finder, allocator);
      common::ObSEArray<common::ObZone, 7> high_priority_zone_array;
      common::ObSEArray<share::ObRawPrimaryZoneUtil::ZoneScore, MAX_ZONE_NUM> zone_score_array;
      ObArray<int64_t> partition_ids;
      ObPartIdsGenerator id_gen(table);
      bool is_non_part_table = false;
      if (table.has_self_partition() && table.get_all_part_num() <= 1 &&
          table.get_duplicate_scope() == ObDuplicateScope::DUPLICATE_SCOPE_NONE &&
          share::OB_ALL_DUMMY_TID != extract_pure_id(table.get_table_id()) &&
          OB_INVALID_ID == table.get_tablegroup_id() && rootserver::ObTenantUtils::is_balance_target_schema(table)) {
        is_non_part_table = true;
      } else {
      }
      if (OB_INVALID_ID != table.get_tablegroup_id()) {
        if (OB_FAIL(get_pg_partitions(table, addr))) {
          LOG_WARN("get partition group partitions failed", K(ret), K(table));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_UNLIKELY(nullptr == schema_service_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schema service ptr is null", K(ret), KP(schema_service_));
      } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
        LOG_WARN("fail to get schema guard", K(ret));
      } else if (OB_FAIL(build_single_pt_balance_container(
                     schema_guard, table, balance_container, high_priority_zone_array, zone_score_array))) {
        LOG_WARN("fail to build single pt balance container", K(ret));
      } else if (OB_FAIL(process_replica_in_logonly_unit(table, addr, create_mode, schema_guard))) {
        LOG_WARN("fail to process logonly replica in logonly unit", K(ret));
      } else if (OB_FAIL(get_new_partitions(balance_container,
                     high_priority_zone_array,
                     table,
                     addr,
                     create_mode,
                     ten_unit_arr,
                     is_non_part_table))) {
        LOG_WARN("alloc new partitions failed", K(ret), K(table));
      }
      if (OB_SUCC(ret)) {
        if (is_non_part_table && GCTX.is_primary_cluster() && ten_unit_arr.count() > 0) {
          if (OB_FAIL(set_non_part_leader(high_priority_zone_array, zone_score_array, table, ten_unit_arr, addr))) {
            LOG_WARN("fail to set non part leader", K(ret));
          }
        } else {
          if (OB_FAIL(id_gen.gen(partition_ids))) {
            LOG_WARN("fail to generate phy_partition_id", K(ret));
          } else if (OB_FAIL(set_initial_leader(
                         balance_container, high_priority_zone_array, table, partition_ids, addr, schema_guard))) {
            LOG_WARN("set initial leader failed", K(ret), K(table), K(partition_ids));
          }
        }
      }
    }
  }
  RS_TRACE(alloc_replica_end);
  return ret;
}

int ObReplicaCreator::recalc_partition_initial_leader_stat(ObPartitionAddr& partition_addr, bool& has_leader)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    bool is_valid = false;
    has_leader = false;
    FOREACH_CNT_X(r, partition_addr, !has_leader && OB_SUCCESS == ret)
    {
      if (r->initial_leader_) {
        if (OB_FAIL(server_mgr_->check_server_valid_for_partition(r->addr_, is_valid))) {
          LOG_WARN("fail to check server stopped", "server", r->addr_, K(ret));
        } else if (is_valid) {
          has_leader = true;
        } else {
          r->initial_leader_ = false;
        }
      }
    }
  }
  return ret;
}

int ObReplicaCreator::generate_balance_group_index(const share::schema::ObPartitionSchema& partition_schema,
    const common::ObPartitionKey& pkey, const bool small_tenant,
    common::ObSEArray<common::ObZone, 7>& high_priority_zone_array,
    const balancer::ObSinglePtBalanceContainer& balance_index_container, common::ObZone& balance_group_zone)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (small_tenant || partition_schema.get_all_part_num() <= 0) {
    balance_group_zone.reset();
  } else {
    HashIndexMapItem balance_index;
    int tmp_ret = balance_index_container.get_hash_index().get_partition_index(pkey, balance_index);
    if (OB_ENTRY_NOT_EXIST == tmp_ret) {
      // by pass
    } else if (OB_SUCCESS != tmp_ret) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get partition index failed", K(ret));
    } else if (OB_FAIL(balance_index.get_balance_group_zone(high_priority_zone_array, balance_group_zone))) {
      LOG_WARN("fail to get balance group zone", K(ret));
    }
  }
  return ret;
}

int ObReplicaCreator::set_partition_initial_leader(ObPartitionAddr& paddr,
    const share::schema::ObPartitionSchema& partition_schema, const common::ObPartitionKey& pkey,
    const bool small_tenant, common::ObSEArray<common::ObZone, 7>& high_priority_zone_array,
    const balancer::ObSinglePtBalanceContainer& balance_index_container,
    rootserver::ObRandomZoneSelector& random_selector, share::schema::ObSchemaGetterGuard& schema_guard)
{
  int ret = OB_SUCCESS;
  uint64_t tg_id = OB_INVALID_ID;
  const int64_t phy_part_id = pkey.get_partition_id();
  const int64_t assign_part_id = (small_tenant ? 0 : phy_part_id);
  common::ObZone balance_group_zone;
  common::ObZone first_primary_zone;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(pkey));
  } else if (small_tenant) {
    tg_id = partition_schema.get_tenant_id();
  } else if (OB_INVALID_ID == partition_schema.get_tablegroup_id()) {
    tg_id = partition_schema.get_table_id();
  } else {
    tg_id = partition_schema.get_tablegroup_id();
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(random_selector.update_score(tg_id, assign_part_id))) {
    LOG_WARN("fail to update random zone selector score", K(ret));
  } else if (OB_FAIL(generate_balance_group_index(partition_schema,
                 pkey,
                 small_tenant,
                 high_priority_zone_array,
                 balance_index_container,
                 balance_group_zone))) {
    LOG_WARN("fail to generate balance group index", K(ret));
  } else if (OB_FAIL(get_partition_schema_first_primary_zone(small_tenant,
                 partition_schema,
                 random_selector,
                 paddr,
                 balance_group_zone,
                 first_primary_zone,
                 schema_guard))) {
    LOG_WARN("fail to get first primary zone", K(ret));
  } else {
    ObReplicaAddr* leader = NULL;
    int64_t max_score = INT64_MIN;
    int64_t max_memstore_percent = 0;
    FOREACH_CNT_X(r, paddr, OB_SUCCESS == ret)
    {
      int64_t score = 0;
      bool is_valid = false;
      if (OB_FAIL(server_mgr_->check_server_valid_for_partition(r->addr_, is_valid))) {
        LOG_WARN("fail to check server valid", "server", r->addr_, K(ret));
      } else if (is_valid) {
        // a D replica cannot be designated to be a leader by default
        if (r->zone_ == first_primary_zone && common::REPLICA_TYPE_FULL == r->replica_type_ &&
            100 == r->get_memstore_percent()) {
          leader = &(*r);
          break;
        } else if (OB_FAIL(random_selector.get_zone_score(r->zone_, score))) {
          LOG_WARN("get zone score failed", K(ret), "zone", r->zone_);
        } else if (r->get_memstore_percent() > max_memstore_percent && common::REPLICA_TYPE_FULL == r->replica_type_) {
          max_memstore_percent = r->get_memstore_percent();
          max_score = score;
          leader = &(*r);
        } else if (r->get_memstore_percent() == max_memstore_percent && common::REPLICA_TYPE_FULL == r->replica_type_) {
          if (score > max_score) {
            max_memstore_percent = r->get_memstore_percent();
            max_score = score;
            leader = &(*r);
          }
        }
      }
    }
    if (OB_FAIL(ret)) {
      // failed
    } else if (OB_ISNULL(leader)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("no active server found", K(ret));
    } else {
      leader->initial_leader_ = true;
    }
  }
  return ret;
}

int ObReplicaCreator::set_initial_leader(balancer::ObSinglePtBalanceContainer& pt_balance_container,
    common::ObSEArray<common::ObZone, 7>& high_priority_zone_array, const ObPartitionSchema& partition_schema,
    const ObIArray<int64_t>& partition_ids, ObITablePartitionAddr& addr,
    share::schema::ObSchemaGetterGuard& schema_guard)
{
  // try to set initial leader in one zone.
  int ret = OB_SUCCESS;
  ObRandomZoneSelector random_selector;
  bool small_tenant = false;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(addr.count() != partition_ids.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(addr.count()), K(partition_ids.count()), K(ret));
  } else if (OB_FAIL(random_selector.init(*zone_mgr_))) {
    LOG_WARN("fail to init random zone selector", K(ret));
  } else if (OB_FAIL(ObTenantUtils::check_small_tenant(partition_schema.get_tenant_id(), small_tenant))) {
    LOG_WARN("fail to check small tenant", K(ret));
  } else {
    for (int64_t partition_idx = 0; OB_SUCC(ret) && partition_idx < addr.count(); ++partition_idx) {
      common::ObZone balance_group_zone;
      ObPartitionAddr* paddr = &addr.at(partition_idx);
      bool has_leader = false;
      const int64_t phy_part_id = partition_ids.at(partition_idx);
      common::ObPartitionKey pkey;
      if (OB_FAIL(recalc_partition_initial_leader_stat(*paddr, has_leader))) {
        LOG_WARN("fail to recalc partition initial leader stat", K(ret));
      } else if (has_leader) {
        // bypass
      } else if (OB_FAIL(
                     pkey.init(partition_schema.get_table_id(), phy_part_id, partition_schema.get_partition_cnt()))) {
        LOG_WARN("fail to init partition key", K(ret));
      } else if (OB_FAIL(set_partition_initial_leader(*paddr,
                     partition_schema,
                     pkey,
                     small_tenant,
                     high_priority_zone_array,
                     pt_balance_container,
                     random_selector,
                     schema_guard))) {
        LOG_WARN("fail to set partition initial leader stat", K(ret));
      }
    }
  }
  return ret;
}

int ObReplicaCreator::set_non_part_leader(common::ObSEArray<common::ObZone, 7>& high_priority_zone_array,
    common::ObSEArray<share::ObRawPrimaryZoneUtil::ZoneScore, MAX_ZONE_NUM>& zone_score_array,
    const share::schema::ObPartitionSchema& partition_schema, ObIArray<share::TenantUnitRepCnt*>& ten_unit_arr,
    ObITablePartitionAddr& addr)
{
  int ret = OB_SUCCESS;
  ObPartitionAddr* paddr = NULL;
  ObArray<common::ObZone> alive_zone;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (addr.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("addr count is unexpected", KR(ret), K(partition_schema), K(high_priority_zone_array));
  } else if (high_priority_zone_array.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("high priority zone array count is null", KR(ret));
  } else if (FALSE_IT(paddr = &addr.at(0))) {  // one partition only
    // never be here
  } else if (OB_ISNULL(paddr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("paddr is null", KR(ret), K(addr));
  } else if (OB_FAIL(get_alive_zone_list(high_priority_zone_array, zone_score_array, *paddr, alive_zone))) {
    LOG_WARN("fail to get alive zone list", KR(ret), K(high_priority_zone_array), K(*paddr), K(partition_schema));
  } else if (alive_zone.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("alive_zone is null", KR(ret), K(high_priority_zone_array), K(*paddr), K(partition_schema));
  } else {
    std::sort(alive_zone.begin(), alive_zone.end());
    int64_t zone_id1 = 0;
    const bool is_new_tablegroup = is_new_tablegroup_id(partition_schema.get_tablegroup_id());
    if (ten_unit_arr.count() > 0) {
      if (!is_new_tablegroup) {
        zone_id1 = ten_unit_arr.at(0)->unit_rep_cnt_.index_num_;
      } else {
        zone_id1 = ten_unit_arr.at(0)->non_table_cnt_;
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ten_unit_arr is null", K(ret));
    }
    if (OB_SUCC(ret)) {
      // the algrithm to calculate leader shall match that of leader coordinator
      uint32_t offset_base = 0;
      const uint64_t tenant_id = partition_schema.get_tenant_id();
      offset_base = murmurhash2(&tenant_id, sizeof(tenant_id), offset_base);
      offset_base = fnv_hash2(&tenant_id, sizeof(tenant_id), offset_base);
      const int64_t offset = (int64_t)offset_base + zone_id1;
      const int64_t tmp_zone_id = offset % alive_zone.count();
      const int64_t zone_id = (tmp_zone_id + tenant_id) % alive_zone.count();
      bool has_leader = false;
      if (OB_FAIL(recalc_partition_initial_leader_stat(*paddr, has_leader))) {
        LOG_WARN("fail to recalc partition initial leader stat", K(ret));
      } else if (has_leader) {
      } else {
        ObReplicaAddr* leader = NULL;
        FOREACH_CNT_X(r, *paddr, OB_SUCCESS == ret)
        {
          if (r->zone_ == alive_zone.at(zone_id)) {
            bool is_valid = false;
            if (OB_FAIL(server_mgr_->check_server_valid_for_partition(r->addr_, is_valid))) {
              LOG_WARN("failt to check server valid for partition", KR(ret), K(*r));
            } else if (is_valid) {
              // a D replica cannot by designated to be a leader by default
              if (common::REPLICA_TYPE_FULL == r->replica_type_ && 0 < r->get_memstore_percent()) {
                leader = &(*r);
                break;
              } else {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("set leader fail", K(ret), K(zone_id));
              }
            }
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_ISNULL(leader)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("no active server found", K(ret));
          } else {
            leader->initial_leader_ = true;
            set_ten_unit_arr(ten_unit_arr, *paddr, *leader, is_new_tablegroup);
            LOG_INFO("finish set leader", KR(ret), K(*leader), K(addr.at(0)));
          }
        }
      }
    }
  }
  return ret;
}

int ObReplicaCreator::get_alive_zone_list(common::ObSEArray<common::ObZone, 7>& high_priority_zone_array,
    common::ObSEArray<share::ObRawPrimaryZoneUtil::ZoneScore, MAX_ZONE_NUM>& zone_score_array,
    const ObPartitionAddr& paddr, ObIArray<common::ObZone>& alive_zone)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < paddr.count() && OB_SUCC(ret); ++i) {
    const ObReplicaAddr& replica_addr = paddr.at(i);
    if (has_exist_in_array(high_priority_zone_array, replica_addr.zone_)) {
      if (OB_FAIL(alive_zone.push_back(replica_addr.zone_))) {
        LOG_WARN("fail to push back alive zone", KR(ret), K(high_priority_zone_array), K(paddr), K(i));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (zone_score_array.count() <= 0) {
      // nothing todo
    } else if (0 == alive_zone.count()) {
      // has no unit on primary_zone, try to designate the leader to another zone
      CmpZoneScore cmp;
      std::sort(zone_score_array.begin(), zone_score_array.end(), cmp);
      int64_t first_zone_score = zone_score_array.at(0).zone_score_;
      for (int64_t j = 1; j < zone_score_array.count() && OB_SUCC(ret); ++j) {
        const share::ObRawPrimaryZoneUtil::ZoneScore& zone_score = zone_score_array.at(j);
        if (first_zone_score == zone_score.zone_score_) {
          if (OB_FAIL(gen_alive_zone_list(paddr, zone_score, alive_zone))) {
            LOG_WARN("fail to gen alive zone list", KR(ret), K(zone_score), K(alive_zone));
          }
        } else {
          if (0 == alive_zone.count()) {
            if (OB_FAIL(gen_alive_zone_list(paddr, zone_score, alive_zone))) {
              LOG_WARN("fail to gen alive zone list", KR(ret), K(zone_score), K(alive_zone));
            }
          } else {
            break;
          }
        }
      }
    } else {
      // nothing todo
    }
  }
  return ret;
}

int ObReplicaCreator::gen_alive_zone_list(const ObPartitionAddr& paddr,
    const share::ObRawPrimaryZoneUtil::ZoneScore& zone_score, ObIArray<common::ObZone>& alive_zone)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < paddr.count() && OB_SUCC(ret); ++i) {
    const ObReplicaAddr& replica_addr = paddr.at(i);
    bool is_valid = false;
    if (zone_score.zone_ != replica_addr.zone_) {
      // nothing todo
    } else if (OB_FAIL(server_mgr_->check_server_valid_for_partition(replica_addr.addr_, is_valid))) {
      LOG_WARN("fail to check server valid for partiton", KR(ret), K(replica_addr));
    } else if (is_valid) {
      if (common::REPLICA_TYPE_FULL == replica_addr.replica_type_ && 0 < replica_addr.get_memstore_percent()) {
        if (OB_FAIL(alive_zone.push_back(replica_addr.zone_))) {
          LOG_WARN("fail to push back zone", KR(ret), K(replica_addr.zone_));
        }
      }
    }
  }
  return ret;
}

void ObReplicaCreator::set_ten_unit_arr(ObIArray<share::TenantUnitRepCnt*>& ten_unit_arr, const ObPartitionAddr& paddr,
    const ObReplicaAddr& leader, const bool is_new_tablegroup)
{
  for (int64_t j = 0; j < paddr.count(); ++j) {
    const ObReplicaAddr replica_addr = paddr.at(j);
    for (int64_t i = 0; i < ten_unit_arr.count(); ++i) {
      if (replica_addr.unit_id_ == ten_unit_arr.at(i)->unit_id_) {
        if (replica_addr.replica_type_ == REPLICA_TYPE_FULL && replica_addr.get_memstore_percent() == 100) {
          ten_unit_arr.at(i)->unit_rep_cnt_.f_replica_cnt_++;
        } else if (replica_addr.replica_type_ == REPLICA_TYPE_FULL && replica_addr.get_memstore_percent() == 0) {
          ten_unit_arr.at(i)->unit_rep_cnt_.d_replica_cnt_++;
        } else if (replica_addr.replica_type_ == REPLICA_TYPE_LOGONLY) {
          ten_unit_arr.at(i)->unit_rep_cnt_.l_replica_cnt_++;
        } else if (replica_addr.replica_type_ == REPLICA_TYPE_READONLY) {
          ten_unit_arr.at(i)->unit_rep_cnt_.r_replica_cnt_++;
        }
      }
    }
  }
  for (int64_t i = 0; i < ten_unit_arr.count(); ++i) {
    if (leader.unit_id_ == ten_unit_arr.at(i)->unit_id_) {
      ten_unit_arr.at(i)->unit_rep_cnt_.leader_cnt_++;
    }
    if (!is_new_tablegroup) {
      ten_unit_arr.at(i)->unit_rep_cnt_.index_num_++;
      ten_unit_arr.at(i)->non_table_cnt_++;
    } else {
      ten_unit_arr.at(i)->non_table_cnt_++;
    }
    const int64_t start = ObTimeUtility::current_time();
    ten_unit_arr.at(i)->now_time_ = start;
  }
}

int ObReplicaCreator::get_partition_schema_first_primary_zone(const bool small_tenant,
    const share::schema::ObPartitionSchema& partition_schema, const ObRandomZoneSelector& random_selector,
    const common::ObIArray<rootserver::ObReplicaAddr>& replica_addrs, const common::ObZone& balance_group_zone,
    ObZone& first_primary_zone, share::schema::ObSchemaGetterGuard& guard)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = partition_schema.get_tenant_id();
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!balance_group_zone.is_empty()) {
    first_primary_zone = balance_group_zone;  // balance group zone valid
  } else {
    if (!small_tenant) {
      if (OB_FAIL(partition_schema.get_first_primary_zone_inherit(
              guard, random_selector, replica_addrs, first_primary_zone))) {
        LOG_WARN("fail to get first primary zone inherit", K(ret));
      }
    } else {
      const share::schema::ObTenantSchema* tenant_schema = nullptr;
      if (OB_FAIL(guard.get_tenant_info(tenant_id, tenant_schema))) {
        LOG_WARN("fail to get tenant info", K(ret), K(tenant_id));
      } else if (OB_UNLIKELY(nullptr == tenant_schema)) {
        ret = OB_TENANT_NOT_EXIST;
        LOG_WARN("tenant schema ptr is null", K(ret), K(tenant_id));
      } else if (OB_FAIL(tenant_schema->get_first_primary_zone(random_selector, replica_addrs, first_primary_zone))) {
        LOG_WARN("fail to get first primary zone", K(ret));
      }
    }
  }
  return ret;
}

// this func will help to filter the unit with a type of logonly
int ObReplicaCreator::tenant_online_unit_without_logonly(share::schema::ObSchemaGetterGuard& schema_guard,
    const uint64_t tenant_id, ZoneUnitArray& all_zone_unit, ZoneUnitPtrArray& all_zone_unit_ptr)
{
  all_zone_unit.reuse();
  all_zone_unit_ptr.reuse();
  int ret = OB_SUCCESS;
  ObArray<uint64_t> rs_pool;
  ObArray<ObUnitInfo> unit_array;
  UnitArray tmp;
  const share::schema::ObTenantSchema* tenant_schema = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_schema))) {
    LOG_WARN("fail to get tenant schema", K(ret), K(tenant_id));
  } else if (OB_UNLIKELY(NULL == tenant_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant schema is null", K(ret));
  } else if (OB_FAIL(unit_mgr_->get_active_unit_infos_by_tenant(*tenant_schema, unit_array))) {
    LOG_WARN("fail to get unit infos by tenant", K(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("rs stop", K(ret));
  } else {
    // algrithm: the units got from the get_active_unit_infos_by_tenant are unsorted.
    //            this algrithm helps to push these units into all_zone_unit accumulated by zone
    // attention: zone_units from all_zone_unit are not sorted by zone
    FOREACH_X(u, unit_array, OB_SUCCESS == ret)
    {
      int64_t idx = 0;
      if (REPLICA_TYPE_LOGONLY == u->unit_.replica_type_) {
        // nothing todo
        continue;
      }
      for (; idx < all_zone_unit.count(); ++idx) {
        if (all_zone_unit.at(idx).count() > 0 && all_zone_unit.at(idx).at(0).unit_.zone_ == u->unit_.zone_) {
          break;
        }
      }
      if (idx >= all_zone_unit.count()) {
        if (OB_FAIL(all_zone_unit.push_back(tmp))) {
          LOG_WARN("add unit array failed", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(all_zone_unit.at(idx).push_back(*u))) {
          LOG_WARN("add unit failed", K(ret));
        }
      }
    }
  }

  // copy the unit ptr of all_zone_unit zone by zone
  // reason: the following routine keeps trying to delete elements from
  //         the all_zone_unit_ptr array, we suppose the performance is good since
  //         all operations are executed on pointers.
  UnitPtrArray unit_ptr_array;
  all_zone_unit_ptr.reserve(all_zone_unit.count());
  FOREACH_CNT_X(zu, all_zone_unit, OB_SUCC(ret))
  {
    unit_ptr_array.reuse();
    unit_ptr_array.reserve(zu->count());
    FOREACH_CNT_X(up, *zu, OB_SUCC(ret))
    {
      if (OB_FAIL(unit_ptr_array.push_back(&(*up)))) {
        LOG_WARN("unit_ptr_array push_back failed", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(all_zone_unit_ptr.push_back(unit_ptr_array))) {
      LOG_WARN("all_zone_unit_ptr push_back failed", K(ret));
    }
  }

  // function:  acquire all units of one tenant
  // algrithm:  traverse all unit zone by zone and check whether the server is available,
  //            if the server is not available, delete the unit from the corresponding zone unit
  // attention: for the server which is not available, we cannot distribute any replica on it.
  //            so if all serves of one zone are not available, then we cannot distribte any replica
  //            on this zone. if the number of all distributed replica is less than majority,
  //            this routine shall fail.
  if (OB_SUCC(ret)) {
    ObArray<int64_t> zone_ptr_to_remove;
    int64_t zone_idx = -1;  // record the zone to by removed
    FOREACH_X(zu, all_zone_unit_ptr, OB_SUCCESS == ret)
    {
      int64_t cnt = zu->count();
      zone_idx++;
      ObArray<int64_t> unit_to_remove;
      for (int64_t i = 0; OB_SUCC(ret) && i < cnt; ++i) {
        ObUnit* u = &zu->at(i)->unit_;
        bool is_valid = false;
        if (OB_FAIL(server_mgr_->check_server_valid_for_partition(u->server_, is_valid))) {
          LOG_WARN("check server valid for partition", KR(ret), "server", u->server_);
        } else if (is_valid) {
          // unit usable, don't remove
        } else if (OB_FAIL(unit_to_remove.push_back(i))) {
          LOG_WARN("push to array failed", K(ret), K(i));
        } else {
          LOG_INFO("need to remove unit", K(ret), K(i), K(*u));
        }
        LOG_INFO("choose unit candidata", K(i), "server", u->server_, K(is_valid), "unit", *u);
      }
      if (OB_SUCC(ret) && unit_to_remove.count() > 0) {
        for (int64_t i = unit_to_remove.count() - 1; OB_SUCC(ret) && i >= 0; i--) {
          int64_t idx = unit_to_remove.at(i);
          if (OB_FAIL(zu->remove(idx))) {
            LOG_WARN("remove idx failed", K(ret), K(cnt), K(idx));
          }
        }
      }
      if (OB_SUCC(ret) && 0 == zu->count()) {
        if (OB_FAIL(zone_ptr_to_remove.push_back(zone_idx))) {
          LOG_WARN("fail push idx to remove array", K(zone_idx), K(ret));
        } else {
          LOG_WARN("zone is empty, remove it", K(zone_idx));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (zone_ptr_to_remove.count() > 0) {
        int64_t cnt = zone_ptr_to_remove.count();
        for (int64_t i = cnt - 1; OB_SUCC(ret) && i >= 0; i--) {
          int64_t idx = zone_ptr_to_remove.at(i);
          if (OB_FAIL(all_zone_unit_ptr.remove(idx))) {
            LOG_WARN("remove idx failed", K(ret), K(cnt), K(i));
          }
        }
      }
    }
  }

  return ret;
}

// this function has a inherent drawback in implementation:
// the basic principle to check majority is to check if the
// distributed replica number are no less than majority, however we
// count the unit number instead
int ObReplicaCreator::check_majority(share::schema::ObSchemaGetterGuard& schema_guard, const uint64_t tenant_id,
    const int64_t paxos_replica_num, const common::ObIArray<share::ObZoneReplicaAttrSet>& zone_locality,
    obrpc::ObCreateTableMode create_mode)
{
  int ret = OB_SUCCESS;
  const share::schema::ObTenantSchema* tenant_schema = NULL;
  ObArray<ObUnitInfo> unit_array;

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(paxos_replica_num <= 0 || OB_INVALID_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("paxos replica num error", K(ret), K(paxos_replica_num), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_schema))) {
    LOG_WARN("fail to get tenant schema", K(ret), K(tenant_id));
  } else if (OB_UNLIKELY(NULL == tenant_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant schema is null", K(ret), K(tenant_id));
  } else if (OB_FAIL(unit_mgr_->get_active_unit_infos_by_tenant(*tenant_schema, unit_array))) {
    LOG_WARN("fail to get unit infos by tenant", K(ret));
  } else {
    int64_t full_unit_num = 0;
    int64_t logonly_unit_num = 0;
    int64_t total_unit_num = 0;

    for (int64_t i = 0; OB_SUCC(ret) && i < unit_array.count(); ++i) {
      const ObUnit& unit = unit_array.at(i).unit_;
      bool is_valid = false;
      if (OB_FAIL(server_mgr_->check_server_valid_for_partition(unit.server_, is_valid))) {
        LOG_WARN("fail to get server active", K(ret));
      } else if (!is_valid) {
        // ignore the unit inactive
      } else if (REPLICA_TYPE_FULL == unit.replica_type_) {
        ++full_unit_num;
      } else if (REPLICA_TYPE_LOGONLY == unit.replica_type_) {
        ++logonly_unit_num;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit replica type unexpected", K(ret), K(unit));
      }
    }
    int64_t specific_replica_num = 0;
    int64_t logonly_replica_num = 0;
    int64_t except_l_specific_num = 0;
    for (int64_t i = 0; i < zone_locality.count(); ++i) {
      const share::ObZoneReplicaAttrSet& zone_set = zone_locality.at(i);
      specific_replica_num += zone_set.get_specific_replica_num();
      logonly_replica_num += zone_set.get_logonly_replica_num();
    }

    if (OB_FAIL(ret)) {
    } else if (obrpc::OB_CREATE_TABLE_MODE_STRICT == create_mode) {
      total_unit_num = full_unit_num + logonly_unit_num;
      except_l_specific_num = specific_replica_num - logonly_replica_num;
      if (total_unit_num < specific_replica_num || full_unit_num < except_l_specific_num) {
        ret = OB_MACHINE_RESOURCE_NOT_ENOUGH;
        LOG_WARN("don't has enough unit for partition allocate",
            K(ret),
            K(total_unit_num),
            K(specific_replica_num),
            K(full_unit_num),
            K(except_l_specific_num));
      }
    } else {
      total_unit_num = full_unit_num + logonly_unit_num;
      if (full_unit_num <= 0 || total_unit_num < majority(paxos_replica_num)) {
        ret = OB_MACHINE_RESOURCE_NOT_ENOUGH;
        LOG_WARN("don't has enough unit for partition allocate",
            K(ret),
            K(total_unit_num),
            K(paxos_replica_num),
            K(full_unit_num));
      }
    }
  }
  return ret;
}

int ObReplicaCreator::try_compensate_readonly_all_server(share::schema::ObSchemaGetterGuard& schema_guard,
    const share::schema::ObPartitionSchema& schema, share::schema::ZoneLocalityIArray& zone_locality) const
{
  int ret = OB_SUCCESS;
  bool compensate_readonly_all_server = false;
  if (OB_FAIL(schema.check_is_duplicated(schema_guard, compensate_readonly_all_server))) {
    LOG_WARN("fail to check duplicate scope cluter", K(ret), K(schema));
  } else if (compensate_readonly_all_server) {
    common::ObArray<share::ReplicaAttr> readonly_set;
    if (OB_FAIL(readonly_set.push_back(ReplicaAttr(ObLocalityDistribution::ALL_SERVER_CNT, 100 /*percent*/)))) {
      LOG_WARN("fail to push back", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < zone_locality.count(); ++i) {
        share::ObZoneReplicaAttrSet& locality_set = zone_locality.at(i);
        if (OB_FAIL(locality_set.replica_attr_set_.set_readonly_replica_attr_array(readonly_set))) {
          LOG_WARN("fail to set readonly replica attr array", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObReplicaCreator::get_locality_info(
    const share::schema::ObPartitionSchema& schema, share::schema::ZoneLocalityIArray& zone_locality) const
{
  int ret = OB_SUCCESS;
  share::schema::ObSchemaGetterGuard schema_guard;
  const uint64_t tenant_id = schema.get_tenant_id();
  if (!inited_) {
    ret = OB_NOT_INIT;
    RS_LOG(WARN, "not init", K(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    RS_LOG(WARN, "fail to get schema guard", K(ret));
  } else if (OB_FAIL(schema.get_zone_replica_attr_array_inherit(schema_guard, zone_locality))) {
    RS_LOG(WARN, "fail get zone replica num array", K(ret));
  } else if (OB_FAIL(try_compensate_readonly_all_server(schema_guard, schema, zone_locality))) {
    LOG_WARN("fail to try compensate readonly all server", K(ret));
  } else {
    LOG_INFO("xxx", K(zone_locality));
  }
  return ret;
}

int ObReplicaCreator::build_single_pt_balance_container(share::schema::ObSchemaGetterGuard& schema_guard,
    const share::schema::ObPartitionSchema& partition_schema,
    balancer::ObSinglePtBalanceContainer& pt_balance_container,
    common::ObIArray<common::ObZone>& high_priority_zone_array,
    common::ObSEArray<share::ObRawPrimaryZoneUtil::ZoneScore, MAX_ZONE_NUM>& zone_score_array)
{
  int ret = OB_SUCCESS;
  common::ObZone integrated_zone;
  ObSEArray<share::ObRawPrimaryZoneUtil::RegionScore, MAX_ZONE_NUM> region_score_array;
  share::ObRawPrimaryZoneUtil raw_primary_zone_util(*zone_mgr_);
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(
                 ObPrimaryZoneUtil::get_pg_integrated_primary_zone(schema_guard, partition_schema, integrated_zone))) {
    LOG_WARN("fail to get pg integrated primary zone", K(ret));
  } else if (OB_FAIL(raw_primary_zone_util.build(integrated_zone, zone_score_array, region_score_array))) {
    LOG_WARN("fail to build raw primary zone util", K(ret));
  } else if (OB_FAIL(
                 raw_primary_zone_util.generate_high_priority_zone_array(zone_score_array, high_priority_zone_array))) {
    LOG_WARN("fail to generate high priority zone array", K(ret));
  } else if (OB_FAIL(pt_balance_container.init(partition_schema.get_all_part_num()))) {
    LOG_WARN("fail to init partition schema balance index builder", K(ret));
  } else if (OB_FAIL(pt_balance_container.build())) {
    LOG_WARN("fail to build index builder", K(ret));
  }
  return ret;
}

// invoked by primary cluster to create partitioins
int ObReplicaCreator::do_get_new_partitions(ObCreateTableReplicaByLocality& addr_allocator,
    const share::schema::ObPartitionSchema& partition_schema, ObITablePartitionAddr& addr,
    const obrpc::ObCreateTableMode create_mode, ObIArray<TenantUnitRepCnt*>& ten_unit_arr, const bool is_non_part_table)
{
  int ret = OB_SUCCESS;
  // FIXME:()
  // no other requirement to create partition whose schema in delaying deletion,
  // the check_dropped_schema is set to false by default
  bool check_dropped_schema = false;
  ObPartitionKeyIter pkey_iter(partition_schema.get_table_id(), partition_schema, check_dropped_schema);
  ARRAY_FOREACH_X(addr, partition_idx, partition_cnt, OB_SUCC(ret))
  {
    ObPartitionAddr& paddr = addr.at(partition_idx);
    ObReplicaAddr replica_addr;
    common::ObPartitionKey pkey;
    if (OB_FAIL(addr_allocator.prepare_for_next_partition(paddr))) {
      LOG_WARN("fail prepare variables for next partition", K(ret));
    } else if (OB_FAIL(pkey_iter.next_partition_key_v2(pkey))) {
      LOG_WARN("fail to get next partition key", K(ret), K(partition_idx), K(partition_cnt));
    } else {
      while (OB_SUCC(ret)) {
        if (OB_FAIL(addr_allocator.get_next_replica(pkey, ten_unit_arr, is_non_part_table, replica_addr))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          } else if (OB_MACHINE_RESOURCE_NOT_ENOUGH == ret && obrpc::OB_CREATE_TABLE_MODE_STRICT != create_mode) {
            LOG_WARN("ingore create replica failure in non-strict mode",
                K(create_mode),
                K(partition_idx),
                "tenant_id",
                partition_schema.get_tenant_id());
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("fail get next replia for current partition", K(ret));
          }
        } else if (OB_FAIL(paddr.push_back(replica_addr))) {
          LOG_WARN("add replica address failed", K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        ret = addr_allocator.fill_all_rest_server_with_replicas(paddr);
      }
    }
  }
  return ret;
}

// invoked by standby cluster to create partitioins
int ObReplicaCreator::get_new_partitions(balancer::ObSinglePtBalanceContainer& pt_balance_container,
    common::ObSEArray<common::ObZone, 7>& high_priority_zone_array,
    const share::schema::ObPartitionSchema& partition_schema, const ObIArray<common::ObPartitionKey>& keys,
    ObITablePartitionAddr& addr, const obrpc::ObCreateTableMode create_mode, ObIArray<TenantUnitRepCnt*>& ten_unit_arr,
    share::schema::ObSchemaGetterGuard& schema_guard, const bool is_non_part_table)
{
  int ret = OB_SUCCESS;
  ZoneUnitPtrArray all_zone_units_alive;
  ObArray<share::ObZoneReplicaAttrSet> zone_locality;
  common::ObArray<common::ObZone> zone_list;
  const uint64_t tenant_id = partition_schema.get_tenant_id();
  ZoneUnitArray unit_pool;
  ObAliveZoneUnitsProvider zone_units_provider;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(init_addr_allocator_parameter(partition_schema,
                 create_mode,
                 zone_list,
                 unit_pool,
                 zone_locality,
                 all_zone_units_alive,
                 schema_guard))) {
    LOG_WARN("failed to init addr allocator parameter", K(ret));
  } else if (OB_FAIL(zone_units_provider.init(all_zone_units_alive))) {
    LOG_WARN("fail to init zone unit provider", K(ret));
  } else {
    LOG_INFO("get all unit", K(unit_pool), K(all_zone_units_alive));
    // partition_schema.get_table_id() is used to by a seed to generate a random start offset
    // for locality distribution, the algrithm shall be the same as that of ObLocalityChecker
    // and ObRereplication
    ObCreateTableReplicaByLocality addr_allocator(*zone_mgr_,
        zone_units_provider,
        zone_locality,
        create_mode,
        zone_list,
        partition_schema.get_table_id(),
        &pt_balance_container,
        &high_priority_zone_array);
    if (OB_FAIL(addr_allocator.init())) {
      LOG_WARN("fail init replica addr allocator", K(ret));
    } else if (OB_FAIL(
                   do_get_new_partitions(addr_allocator, keys, addr, create_mode, ten_unit_arr, is_non_part_table))) {
      LOG_WARN("fail to get new partitions", K(ret));
    }
  }
  return ret;
}

// invoked by standby cluster to create partitioins
int ObReplicaCreator::do_get_new_partitions(ObCreateTableReplicaByLocality& addr_allocator,
    const ObIArray<common::ObPartitionKey>& keys, ObITablePartitionAddr& addr,
    const obrpc::ObCreateTableMode create_mode, ObIArray<TenantUnitRepCnt*>& ten_unit_arr, const bool is_non_part_table)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(addr.count() != keys.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("addr count not equal to key count", K(ret), "addr_count", addr.count(), "peky_count", keys.count());
  }
  ARRAY_FOREACH_X(addr, partition_idx, partition_cnt, OB_SUCC(ret))
  {
    ObPartitionAddr& paddr = addr.at(partition_idx);
    ObReplicaAddr replica_addr;
    const common::ObPartitionKey& pkey = keys.at(partition_idx);
    if (OB_FAIL(addr_allocator.prepare_for_next_partition(paddr))) {
      LOG_WARN("fail prepare variables for next partition", K(ret));
    } else {
      while (OB_SUCC(ret)) {
        if (OB_FAIL(addr_allocator.get_next_replica(pkey, ten_unit_arr, is_non_part_table, replica_addr))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          } else if (OB_MACHINE_RESOURCE_NOT_ENOUGH == ret && obrpc::OB_CREATE_TABLE_MODE_STRICT != create_mode) {
            LOG_WARN("ingore create replica failure in non-strict mode", K(create_mode), K(partition_idx), K(pkey));
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("fail get next replia for current partition", K(ret));
          }
        } else if (OB_FAIL(paddr.push_back(replica_addr))) {
          LOG_WARN("add replica address failed", K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        ret = addr_allocator.fill_all_rest_server_with_replicas(paddr);
      }
    }
  }
  return ret;
}

// invoked by primary cluster to create partitioins
int ObReplicaCreator::get_new_partitions(balancer::ObSinglePtBalanceContainer& pt_balance_container,
    common::ObSEArray<common::ObZone, 7>& high_priority_zone_array,
    const share::schema::ObPartitionSchema& partition_schema, ObITablePartitionAddr& addr,
    const obrpc::ObCreateTableMode create_mode, ObIArray<TenantUnitRepCnt*>& ten_unit_arr, const bool is_non_part_table)
{
  int ret = OB_SUCCESS;
  ZoneUnitPtrArray all_zone_units_alive;
  ObArray<share::ObZoneReplicaAttrSet> zone_locality;
  common::ObArray<common::ObZone> zone_list;
  ZoneUnitArray unit_pool;
  const uint64_t tenant_id = partition_schema.get_tenant_id();
  share::schema::ObSchemaGetterGuard schema_guard;

  ObAliveZoneUnitsProvider zone_units_provider;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(nullptr == schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service ptr is null", K(ret), KP(schema_service_));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    RS_LOG(WARN, "fail to get schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(init_addr_allocator_parameter(partition_schema,
                 create_mode,
                 zone_list,
                 unit_pool,
                 zone_locality,
                 all_zone_units_alive,
                 schema_guard))) {
    LOG_WARN("failed to init addr allocator parameter", K(ret));
  } else if (OB_FAIL(zone_units_provider.init(all_zone_units_alive))) {
    LOG_WARN("fail to init zone unit provider", K(ret));
  } else {
    LOG_INFO("get all unit", K(unit_pool), K(all_zone_units_alive));
    // partition_schema.get_table_id() is used to by a seed to generate a random start offset
    // for locality distribution, the algrithm shall be the same as that of ObLocalityChecker
    // and ObRereplication
    ObCreateTableReplicaByLocality addr_allocator(*zone_mgr_,
        zone_units_provider,
        zone_locality,
        create_mode,
        zone_list,
        partition_schema.get_table_id(),
        &pt_balance_container,
        &high_priority_zone_array);
    if (OB_FAIL(addr_allocator.init())) {
      LOG_WARN("fail init replica addr allocator", K(ret));
    } else if (OB_FAIL(do_get_new_partitions(
                   addr_allocator, partition_schema, addr, create_mode, ten_unit_arr, is_non_part_table))) {
      LOG_WARN("fail to get new partitions", K(ret));
    }
  }
  return ret;
}
int ObReplicaCreator::init_addr_allocator_parameter(const share::schema::ObPartitionSchema& partition_schema,
    const obrpc::ObCreateTableMode create_mode, ObIArray<common::ObZone>& zone_list, ZoneUnitArray& unit_pool,
    ObIArray<share::ObZoneReplicaAttrSet>& zone_locality, ZoneUnitPtrArray& all_zone_units_alive,
    share::schema::ObSchemaGetterGuard& schema_guard)
{
  int ret = OB_SUCCESS;
  int64_t paxos_replica_num = OB_INVALID_COUNT;
  const uint64_t tenant_id = partition_schema.get_tenant_id();
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (OB_UNLIKELY(nullptr == schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service ptr is null", K(ret), KP(schema_service_));
  } else if (OB_FAIL(get_locality_info(partition_schema, zone_locality))) {
    LOG_WARN("fail to get locality info", K(ret));
  } else if (OB_FAIL(partition_schema.get_paxos_replica_num(schema_guard, paxos_replica_num))) {
    LOG_WARN("fail to get paxos replica num", K(ret));
  } else if (OB_FAIL(check_majority(schema_guard, tenant_id, paxos_replica_num, zone_locality, create_mode))) {
  } else if (OB_FAIL(partition_schema.get_zone_list(schema_guard, zone_list))) {
    LOG_WARN("fail to get zone list", K(ret));
  } else if (OB_FAIL(tenant_online_unit_without_logonly(schema_guard, tenant_id, unit_pool, all_zone_units_alive))) {
    LOG_WARN("get tenant all unit failed", K(ret), K(tenant_id));
    LOG_WARN("fail to check majority", K(ret));
  } else if (OB_FAIL(ObLocalityTaskHelp::filter_logonly_task(tenant_id, *unit_mgr_, schema_guard, zone_locality))) {
    LOG_WARN("fail to filter logonly task", K(ret), K(zone_locality));
  }
  return ret;
}

int ObReplicaCreator::get_pg_partitions(const ObTableSchema& table, ObITablePartitionAddr& addr)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard guard;
  ObArray<const ObSimpleTableSchemaV2*> table_schemas;
  const ObSimpleTableSchemaV2* max_part_num_table = NULL;
  int64_t paxos_replica_num = OB_INVALID_COUNT;
  const uint64_t tenant_id = table.get_tenant_id();
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!table.is_valid() || OB_INVALID_ID == table.get_tablegroup_id() ||
             table.get_all_part_num() != addr.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(table), "addr count", addr.count());
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id, guard))) {
    LOG_WARN("get schema guard failed", K(ret));
  } else if (OB_FAIL(guard.get_table_schemas_in_tablegroup(tenant_id, table.get_tablegroup_id(), table_schemas))) {
    LOG_WARN("get table group tables failed", GETK(table, tenant_id), GETK(table, tablegroup_id), K(ret));
  } else if (OB_FAIL(table.get_paxos_replica_num(guard, paxos_replica_num))) {
    LOG_WARN("fail to get table paxos replica num", K(ret));
  } else if (OB_UNLIKELY(paxos_replica_num <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("paxos replica num error", K(ret), K(paxos_replica_num), "table_id", table.get_table_id());
  } else {
    // find table with max partition num in tablegroup
    FOREACH_CNT_X(table_schema, table_schemas, OB_SUCCESS == ret)
    {
      if (NULL == *table_schema) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL table schema", K(ret));
      } else if ((*table_schema)->has_self_partition()) {
        if (NULL == max_part_num_table ||
            (*table_schema)->get_all_part_num() > max_part_num_table->get_all_part_num()) {
          max_part_num_table = *table_schema;
        }
      } else {
      }  // no partition, go on check next
    }
  }

  // prepare filter, only align in_member_list & in_service replica with in pg
  ObReplicaFilterHolder filter;
  ObArray<uint64_t> unit_ids;
  if (OB_SUCC(ret)) {
    // check if all servers are in service, filter the servers not in service
    if (OB_FAIL(filter.set_only_in_service_server(*server_mgr_))) {
      LOG_WARN("set only in service filter failed", K(ret));
    } else if (filter.filter_block_server(*server_mgr_)) {
      LOG_WARN("filter block server fail", K(ret));
    } else if (OB_FAIL(filter.set_in_member_list())) {
      LOG_WARN("set in member list filter failed", K(ret));
    } else if (OB_FAIL(unit_mgr_->get_logonly_unit_ids(unit_ids))) {
      LOG_WARN("fail to get logonly unit ids", K(ret));
    } else if (OB_FAIL(filter.set_filter_unit(unit_ids))) {
      LOG_WARN("fail to filt unit", K(ret));
    }
  }

  // current implementation: replication aligned according the the partition group
  //                         only for the paxos replicas
  // TODO: non-paxos replicas replication also need to take the partition
  //       group align into consideration.
  ObArray<int64_t> replica_not_enough_ids;
  if (OB_SUCCESS == ret && NULL != max_part_num_table) {
    ObArenaAllocator allocator(ObModIds::OB_RS_PARTITION_TABLE_TEMP);
    ObArray<ObPartitionInfo> partitions;
    if (OB_FAIL(partitions.reserve(max_part_num_table->get_all_part_num()))) {
      LOG_WARN("array reserve failed", K(ret), "count", max_part_num_table->get_all_part_num());
    } else if (OB_FAIL(table_all_partition(allocator, max_part_num_table->get_table_id(), partitions))) {
      LOG_WARN("get table all partition failed", K(ret), "table_id", max_part_num_table->get_table_id());
    }

    for (int idx = 0; OB_SUCCESS == ret && idx < partitions.count() && idx < addr.count(); ++idx) {
      ObPartitionAddr& paddr = addr.at(idx);
      ObPartitionInfo& partition = partitions.at(idx);
      if (OB_FAIL(partition.filter(filter))) {
        LOG_WARN("filter partition fail", K(ret));
      } else if (OB_FAIL(set_same_addr_ignore_logonly(partition, guard, table, paxos_replica_num, paddr))) {
        LOG_WARN("set partition info to partition address failed", K(ret));
      } else if (paddr.count() < paxos_replica_num) {
        if (OB_FAIL(replica_not_enough_ids.push_back(idx))) {
          LOG_WARN("push_back failed", K(ret));
        }
      } else {
      }  // enougu paddr
    }
  }

  // if some partition addr don't have enough replica addr, choose pg_partition with enough replica
  if (OB_SUCCESS == ret && replica_not_enough_ids.count() > 0) {
    ObArenaAllocator allocator(ObModIds::OB_RS_PARTITION_TABLE_TEMP);
    ObPartitionInfo partition;
    partition.set_allocator(&allocator);
    FOREACH_CNT_X(id, replica_not_enough_ids, OB_SUCCESS == ret)
    {
      ObPartitionAddr& paddr = addr.at(*id);
      FOREACH_CNT_X(table_schema, table_schemas, OB_SUCCESS == ret)
      {
        partition.reuse();
        // table schema pointer validity checked in previous iteration, not need check again.
        if (*id >= (*table_schema)->get_all_part_num()) {
          continue;
        } else if (OB_FAIL(pt_operator_->get((*table_schema)->get_table_id(), *id, partition))) {
          LOG_WARN("pt_operator get failed", "table_id", (*table_schema)->get_table_id(), "partition_id", *id, K(ret));
        } else if (OB_FAIL(partition.filter(filter))) {
          LOG_WARN("partition filter failed", K(ret));
        } else if (partition.replica_count() >= paxos_replica_num) {
          if (OB_FAIL(set_same_addr_ignore_logonly(partition, guard, table, paxos_replica_num, paddr))) {
            LOG_WARN("set partition info to partition address failed", K(ret));
          } else {
            break;
          }
        }
      }
    }
  }

  return ret;
}

int ObReplicaCreator::check_all_partition_allocated(
    ObITablePartitionAddr& addr, const int64_t partition_num, const int64_t replica_num, bool& allocated) const
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (partition_num <= 0 || replica_num <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(partition_num), K(replica_num));
  } else {
    int64_t partition_idx = 0;
    allocated = (addr.count() == partition_num);
    FOREACH_CNT_X(a, addr, allocated)
    {
      ++partition_idx;
      if (a->count() < replica_num) {
        if (a->count() > 0) {
          LOG_INFO("partition do not allocate enough replica from partition group",
              K(partition_num),
              K(partition_idx),
              "allocated",
              *a,
              K(replica_num));
        }
        allocated = false;
      }
    }
  }
  return ret;
}

// try to distribute partition addr for table schema based on sample partition info
// replica_num: paxos replicas number
// sample_info: logonly replicas on logonly unit are filtered already
// only replicas on non logonly unit are concerned
int ObReplicaCreator::set_same_addr_ignore_logonly(const share::ObPartitionInfo& sample_info,
    share::schema::ObSchemaGetterGuard& schema_guard, const share::schema::ObTableSchema& table_schema,
    const int64_t replica_num, ObPartitionAddr& paddr)
{
  int ret = OB_SUCCESS;
  ObArray<ObZoneReplicaAttrSet> zone_locality;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!sample_info.is_valid() || replica_num <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(sample_info), K(replica_num));
  } else if (OB_FAIL(table_schema.get_zone_replica_attr_array_inherit(schema_guard, zone_locality))) {
    LOG_WARN("fail to get zone replica num array", K(ret));
  } else if (OB_FAIL(ObLocalityTaskHelp::filter_logonly_task(
                 table_schema.get_tenant_id(), *unit_mgr_, schema_guard, zone_locality))) {
    LOG_WARN("fail to filter logonly unit", K(ret), K(zone_locality));
  } else {
    LOG_DEBUG("set same addr", K(ret), K(sample_info), K(replica_num));
    for (int64_t i = 0; OB_SUCC(ret) && i < zone_locality.count(); ++i) {
      const ObZoneReplicaAttrSet& zone_attr_set = zone_locality.at(i);
      ZoneReplicaDistTask zone_task_set;
      bool is_zone_task_set_valid = true;
      if (OB_FAIL(zone_task_set.generate(ZoneReplicaDistTask::ReplicaNature::PAXOS, zone_attr_set, paddr))) {
        LOG_WARN("fail to generate zone task set", K(ret));
      } else if (OB_FAIL(zone_task_set.check_valid(is_zone_task_set_valid))) {
        LOG_WARN("fail to check valid", K(ret));
      } else if (!is_zone_task_set_valid) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("zone task set invalid", K(ret));
      } else {
        for (int64_t index = 0; OB_SUCC(ret) && index < sample_info.get_replicas_v2().count(); ++index) {
          bool has_leader = false;
          bool has_this_task = false;
          FOREACH_CNT(a, paddr)
          {
            if (a->initial_leader_) {
              has_leader = true;
            }
          }
          const ObPartitionReplica& replica = sample_info.get_replicas_v2().at(index);
          bool is_sample_replica = false;
          share::ObUnitInfo unit_info;
          if (!replica.is_in_service()) {
            is_sample_replica = false;
          } else if (!ObReplicaTypeCheck::is_paxos_replica_V2(replica.replica_type_)) {
            is_sample_replica = false;
          } else if (REPLICA_TYPE_FULL == replica.replica_type_) {
            is_sample_replica = true;
          } else if (OB_FAIL(unit_mgr_->get_unit_info_by_id(replica.unit_id_, unit_info))) {
            LOG_WARN("fail to get unit info by id", K(ret), K(replica));
          } else if (REPLICA_TYPE_FULL == unit_info.pool_.replica_type_) {
            is_sample_replica = true;
          } else {  // need to filter the logonly replca on the logonly unit
            is_sample_replica = false;
          }
          if (OB_FAIL(ret)) {
          } else if (!is_sample_replica) {
            // bypass
          } else if (OB_FAIL(zone_task_set.check_has_task(
                         replica.zone_, replica.replica_type_, replica.get_memstore_percent(), has_this_task))) {
            LOG_WARN("fail to check has task", K(ret));
          } else if (!has_this_task) {
            // pass, since does not have this task
          } else if (OB_INVALID_ID == replica.unit_id_) {
            // pass when unit id is invalid
          } else if (OB_FAIL(zone_task_set.erase_task(
                         replica.zone_, replica.get_memstore_percent(), replica.replica_type_))) {
            LOG_WARN("fail to erase",
                K(ret),
                "zone",
                replica.zone_,
                "memstore_percent",
                replica.get_memstore_percent(),
                "replica_type",
                replica.replica_type_);
          } else {
            ObReplicaAddr raddr;
            raddr.unit_id_ = replica.unit_id_;
            raddr.addr_ = replica.server_;
            raddr.zone_ = replica.zone_;
            raddr.replica_type_ = replica.replica_type_;
            raddr.initial_leader_ = !has_leader && sample_info.is_leader_like(index);
            if (OB_FAIL(raddr.set_memstore_percent(replica.get_memstore_percent()))) {
              LOG_WARN("fail to set memstore percent", K(ret));
            } else if (OB_FAIL(paddr.push_back(raddr))) {
              LOG_WARN("fail to add replica address", K(ret));
            } else {
              LOG_INFO("set same addr by partition group", K(raddr));
            }
          }
        }
      }
    }
  }
  return ret;
}

// get all replicas of this partition
int ObReplicaCreator::partition_all_replica(
    const uint64_t table_id, const uint64_t partition_id, share::ObPartitionInfo& partition_info)
{
  int ret = OB_SUCCESS;
  const ObTableSchema* table = NULL;
  const ObTablegroupSchema* tablegroup = NULL;
  const uint64_t tenant_id = extract_tenant_id(table_id);
  ObReplicaFilterHolder filter;
  ObSchemaGetterGuard schema_guard;
  ObArenaAllocator allocator(ObModIds::OB_RS_PARTITION_TABLE_TEMP);
  partition_info.reuse();
  partition_info.set_allocator(&allocator);
  partition_info.set_table_id(table_id);
  partition_info.set_partition_id(partition_id);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == table_id || OB_INVALID_ID == partition_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(table_id), K(partition_id));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret));
  } else if (is_tablegroup_id(table_id)) {
    if (OB_FAIL(schema_guard.get_tablegroup_schema(table_id, tablegroup))) {
      LOG_WARN("failed to get tablegroup schema", K(ret), K(table_id));
    }
  } else if (OB_FAIL(schema_guard.get_table_schema(table_id, table))) {
    LOG_WARN("get table schema failed", K(ret), KT(table_id));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(table) && OB_ISNULL(tablegroup)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema should not be NULL", K(ret), KT(table_id));
  } else if (OB_FAIL(filter.set_replica_status(REPLICA_STATUS_NORMAL))) {
    LOG_WARN("set in member list filter failed", K(ret));
  } else if (OB_FAIL(filter.set_only_in_service_server(*server_mgr_))) {
    LOG_WARN("set only in service filter failed", K(ret));
  } else if (OB_FAIL(pt_operator_->get(table_id, partition_id, partition_info))) {
    LOG_WARN("fail to get partition", K(ret));
  } else if (OB_FAIL(partition_info.filter(filter))) {
    LOG_WARN("fail to filter replica", K(ret), K(partition_info));
  }
  return ret;
}

// get all paxos replicas of all partitions for this table schema
int ObReplicaCreator::table_all_partition(
    common::ObIAllocator& allocator, const uint64_t table_id, common::ObIArray<share::ObPartitionInfo>& parts)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = extract_tenant_id(table_id);
  const ObTableSchema* table = NULL;
  ObReplicaFilterHolder filter;
  ObSchemaGetterGuard schema_guard;
  ObArray<uint64_t> unit_ids;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(table_id));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(table_id, table))) {
    LOG_WARN("get table schema failed", K(ret), KT(table_id));
  } else if (NULL == table) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema should not be NULL", K(ret), KT(table_id));
  } else if (OB_FAIL(filter.set_in_member_list())) {
    LOG_WARN("set in member list filter failed", K(ret));
  } else if (OB_FAIL(filter.set_only_in_service_server(*server_mgr_))) {
    LOG_WARN("set only in service filter failed", K(ret));
  } else if (OB_FAIL(unit_mgr_->get_logonly_unit_ids(unit_ids))) {
    LOG_WARN("fail to get logonly unit ids", K(ret));
  } else if (OB_FAIL(filter.set_filter_unit(unit_ids))) {
    LOG_WARN("fail to filt unit", K(ret));
  } else {
    const int64_t part_num = table->get_all_part_num();
    ObTablePartitionIterator iter;
    parts.reuse();
    if (OB_FAIL(iter.init(table_id, schema_guard, *pt_operator_))) {
      LOG_WARN("init table partition iterator failed", K(ret), K(part_num));
    } else if (OB_FAIL(parts.reserve(part_num))) {
      LOG_WARN("array reserve failed", K(ret), "count", part_num);
    } else {
      ObPartitionInfo info;
      info.set_allocator(&allocator);
      while (OB_SUCCESS == ret && OB_SUCC(iter.next(info))) {
        if (OB_FAIL(info.filter(filter))) {
          LOG_WARN("filter failed", K(ret));
        } else if (OB_FAIL(parts.push_back(info))) {
          LOG_WARN("push partition info to array failed", K(ret));
        }
        info.reuse();
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("iterator table partition failed", K(ret), K(iter));
      }
    }
  }
  return ret;
}

// FIXME: support non-template sub partition
int ObReplicaCreator::alloc_partitions_for_split(
    const ObPartitionSchema& table, const ObPartitionSchema& inc_table, ObITablePartitionAddr& addr)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = table.get_tenant_id();
  RS_TRACE(alloc_replica_begin);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!table.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table", K(ret), K(table));
  } else {
    addr.reuse();
    int64_t inc_part_num = inc_table.get_partition_num();
    if (inc_part_num <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("new_partition_num must bigger than origin table",
          K(ret),
          "origin part num",
          inc_table.get_partition_num(),
          "new part num",
          table.get_partition_num());
    } else if (PARTITION_LEVEL_TWO == table.get_part_level()) {
      inc_part_num *= table.get_sub_part_option().get_part_num();
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(addr.reserve(inc_part_num))) {
      LOG_WARN("array reserve failed", K(ret), K(inc_part_num));
    }
  }
  // try to find the replica addresses of the original partition so as to designate addr for
  // the new partitions, check if the replica number is no more than majority at once
  ObPartitionInfo partition_info;
  share::schema::ObSchemaGetterGuard schema_guard;
  ObArenaAllocator allocator(ObModIds::OB_RS_PARTITION_TABLE_TEMP);
  ObPartition** partition_array = inc_table.get_part_array();
  uint64_t last_partition_id = OB_INVALID_ID;
  int64_t new_partition_idx = 0;
  int64_t part_num = inc_table.get_partition_num();
  ObArray<share::ObZoneReplicaAttrSet> zone_locality;
  int64_t paxos_replica_num = OB_INVALID_COUNT;
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(partition_array)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid new schema", K(ret), K(inc_table));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret));
  } else if (OB_FAIL(get_locality_info(table, zone_locality))) {
    LOG_WARN("fail to get locality info", K(ret));
  } else if (OB_FAIL(table.get_paxos_replica_num(schema_guard, paxos_replica_num))) {
    LOG_WARN("fail to get paxos replica num", K(ret));
  } else if (OB_FAIL(check_majority(
                 schema_guard, tenant_id, paxos_replica_num, zone_locality, obrpc::OB_CREATE_TABLE_MODE_STRICT))) {
    LOG_WARN("fail to check majority", K(ret));
  }
  for (int64_t i = 0; i < part_num && OB_SUCC(ret); i++) {
    if (OB_ISNULL(partition_array[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid partition array", K(ret), K(i), K(inc_table));
    } else if (1 != partition_array[i]->get_source_part_ids().count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid source part ids", K(ret), "source_part", partition_array[i]->get_source_part_ids());
    } else if (last_partition_id == partition_array[i]->get_source_part_ids().at(0)) {
      // only the first partition in split need to construct the partition_info, no need to construct
      // for other partitions
    } else if (OB_FAIL(partition_all_replica(
                   table.get_table_id(), partition_array[i]->get_source_part_ids().at(0), partition_info))) {
      LOG_WARN("fail to get partition all replica", K(ret), K(table), K(inc_table));
    } else {
      last_partition_id = partition_array[i]->get_source_part_ids().at(0);
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(addr.push_back(ObPartitionAddr()))) {
        LOG_WARN("add empty partition address failed", K(ret), K(i));
      } else if (OB_FAIL(set_same_addr(partition_info, addr.at(new_partition_idx)))) {
        LOG_WARN("fail to set same addr", K(ret), K(new_partition_idx), K(partition_info));
      } else {
        LOG_INFO("alloc partition for split", K(partition_info), "part index", addr.at(new_partition_idx));
        new_partition_idx++;
      }
    }
  }  // end for
  RS_TRACE(alloc_replica_end);
  return ret;
}

int ObReplicaCreator::set_same_addr(const share::ObPartitionInfo& sample_info, ObPartitionAddr& paddr)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!sample_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(sample_info));
  } else {
    ObReplicaAddr raddr;
    FOREACH_CNT_X(r, sample_info.get_replicas_v2(), OB_SUCC(ret))
    {
      if (r->replica_status_ != REPLICA_STATUS_NORMAL || OB_INVALID_ID == r->unit_id_) {
        continue;
      }
      raddr.reset();
      raddr.unit_id_ = r->unit_id_;
      raddr.zone_ = r->zone_;
      raddr.addr_ = r->server_;
      raddr.replica_type_ = r->replica_type_;
      if (r->is_leader_like()) {
        raddr.initial_leader_ = true;
      } else {
        raddr.initial_leader_ = false;
      }
      if (OB_FAIL(raddr.set_memstore_percent(r->get_memstore_percent()))) {
        LOG_WARN("fail to set memstore percent", K(ret));
      } else if (OB_FAIL(paddr.push_back(raddr))) {
        LOG_WARN("fail to push back", K(ret));
      } else {
        LOG_INFO("set same addr by sample partition", K(ret));
      }
    }
  }
  return ret;
}

int ObReplicaCreator::process_replica_in_logonly_unit_per_partition(
    const ObIArray<share::ObZoneReplicaAttrSet>& zone_locality, const ObIArray<ObUnitInfo>& logonly_units,
    ObPartitionAddr& paddr, const obrpc::ObCreateTableMode create_mode)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < zone_locality.count(); ++i) {
      const ObZoneReplicaAttrSet& zone_attr_set = zone_locality.at(i);
      ZoneReplicaDistTask zone_task_set;
      bool is_zone_task_set_valid = true;
      if (OB_FAIL(zone_task_set.generate(ZoneReplicaDistTask::ReplicaNature::PAXOS, zone_attr_set, paddr))) {
        LOG_WARN("fail to generate zone task set", K(ret));
      } else if (OB_FAIL(zone_task_set.check_valid(is_zone_task_set_valid))) {
        LOG_WARN("fail to check valid", K(ret));
      } else if (!is_zone_task_set_valid) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("zone task set invalid", K(ret));
      } else {
        for (int64_t i = 0; i < logonly_units.count() && OB_SUCC(ret); ++i) {
          const ObUnitInfo& unit_info = logonly_units.at(i);
          bool has_this_task = false;
          bool valid = false;
          // designated memstore percent to 100 for logonly replica
          const int64_t logonly_memstore_percent = 100;
          if (OB_FAIL(zone_task_set.check_has_task(
                  unit_info.unit_.zone_, REPLICA_TYPE_LOGONLY, logonly_memstore_percent, has_this_task))) {
            LOG_WARN("fail to check has task", K(ret));
          } else if (!has_this_task) {
            // pass, since does not have this task
          } else if (OB_FAIL(zone_task_set.erase_task(
                         unit_info.unit_.zone_, logonly_memstore_percent, REPLICA_TYPE_LOGONLY))) {
            LOG_WARN("fail to erase task", K(ret), "zone", unit_info.unit_.zone_, "replica_type", REPLICA_TYPE_LOGONLY);
          } else if (OB_FAIL(server_mgr_->check_server_valid_for_partition(unit_info.unit_.server_, valid))) {
            LOG_WARN("fail to check server valid", KR(ret), "server", unit_info.unit_.server_);
          } else if (valid || (!valid && obrpc::OB_CREATE_TABLE_MODE_STRICT != create_mode)) {
            ObReplicaAddr raddr;
            raddr.unit_id_ = unit_info.unit_.unit_id_;
            raddr.addr_ = unit_info.unit_.server_;
            raddr.zone_ = unit_info.unit_.zone_;
            raddr.replica_type_ = REPLICA_TYPE_LOGONLY;
            raddr.initial_leader_ = false;  // not a leader
            if (OB_FAIL(raddr.set_memstore_percent(logonly_memstore_percent))) {
              LOG_WARN("fail to set memstore percent", K(ret));
            } else if (OB_FAIL(paddr.push_back(raddr))) {
              LOG_WARN("fail to push back", K(ret));
            } else {
              LOG_INFO("alloc replica for logonly locality", K(raddr));
            }
          } else {
            ret = OB_MACHINE_RESOURCE_NOT_ENOUGH;
            LOG_WARN("fail to alloc logonly replica", K(ret), "server", unit_info.unit_.server_);
          }
        }
      }
    }
  }
  return ret;
}

int ObReplicaCreator::process_replica_in_logonly_unit(const share::schema::ObPartitionSchema& partition_schema,
    ObITablePartitionAddr& addr, const obrpc::ObCreateTableMode create_mode,
    share::schema::ObSchemaGetterGuard& schema_guard)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = partition_schema.get_tenant_id();
  ObArray<ObUnitInfo> logonly_units;
  ObArray<share::ObZoneReplicaAttrSet> zone_locality;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(get_locality_info(partition_schema, zone_locality))) {
    LOG_WARN("fail to get locality info", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || zone_locality.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(zone_locality));
  } else if (OB_FAIL(unit_mgr_->get_logonly_unit_by_tenant(schema_guard, tenant_id, logonly_units))) {
    LOG_WARN("fail to get logonly unit", K(ret), K(tenant_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < addr.count(); ++i) {
      ObPartitionAddr& paddr = addr.at(i);
      if (OB_FAIL(process_replica_in_logonly_unit_per_partition(zone_locality, logonly_units, paddr, create_mode))) {
        LOG_WARN("fail to process replica in logonly unit per partition", K(ret));
      }
    }
  }
  return ret;
}

int ObReplicaCreator::standby_alloc_partitions_for_split(const share::schema::ObTableSchema& table,
    const common::ObIArray<int64_t>& source_part_ids, const common::ObIArray<int64_t>& partition_ids,
    ObITablePartitionAddr& addr)
{
  int ret = OB_SUCCESS;
  RS_TRACE(alloc_replica_begin);
  int64_t inc_part_num = partition_ids.count();
  share::schema::ObSchemaGetterGuard schema_guard;
  ObPartitionInfo partition_info;
  addr.reuse();
  int64_t paxos_replica_num = OB_INVALID_COUNT;
  ObArray<share::ObZoneReplicaAttrSet> zone_locality;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!table.is_valid() || 0 == inc_part_num || 1 != source_part_ids.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table", K(ret), K(table), K(partition_ids), K(source_part_ids));
  } else if (OB_FAIL(addr.reserve(inc_part_num))) {
    LOG_WARN("array reserve failed", K(ret), K(inc_part_num));
  } else if (OB_FAIL(partition_all_replica(table.get_table_id(), source_part_ids.at(0), partition_info))) {
    LOG_WARN("fail to get all partition", KR(ret), K(table), K(source_part_ids));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(table.get_tenant_id(), schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret));
  } else if (OB_FAIL(get_locality_info(table, zone_locality))) {
    LOG_WARN("fail to get zone locality info", KR(ret));
  } else if (OB_FAIL(table.get_paxos_replica_num(schema_guard, paxos_replica_num))) {
    LOG_WARN("fail to get psxos replica num", KR(ret));
  } else if (OB_FAIL(check_majority(schema_guard,
                 table.get_tenant_id(),
                 paxos_replica_num,
                 zone_locality,
                 obrpc::OB_CREATE_TABLE_MODE_LOOSE))) {
    LOG_WARN("fail to check majority", K(ret));
  }
  for (int64_t i = 0; i < inc_part_num && OB_SUCC(ret); i++) {
    if (OB_FAIL(addr.push_back(ObPartitionAddr()))) {
      LOG_WARN("add empty partition address failed", K(ret), K(i));
    } else if (OB_FAIL(set_same_addr(partition_info, addr.at(i)))) {
      LOG_WARN("fail to set same addr", K(ret), K(i), K(partition_info));
    } else {
      LOG_INFO("alloc partition for split", K(partition_info), K(i));
    }
  }
  RS_TRACE(alloc_replica_end);
  return ret;
}
int ObReplicaCreator::alloc_table_partitions_for_standby(const share::schema::ObTableSchema& schema,
    const common::ObIArray<ObPartitionKey>& keys, obrpc::ObCreateTableMode create_mode, ObITablePartitionAddr& addr,
    share::schema::ObSchemaGetterGuard& guard)
{
  ObArray<int64_t> partition_ids;
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(create_mode <= obrpc::OB_CREATE_TABLE_MODE_INVALID ||
                         create_mode >= obrpc::OB_CREATE_TABLE_MODE_MAX)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(create_mode));
  } else if (!schema.has_self_partition()) {
    // table with attributes of binding true shall never be here
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("schema must have self partition", K(ret));
  } else {
    addr.reuse();
    const int64_t part_num = keys.count();
    if (OB_FAIL(addr.reserve(part_num))) {
      LOG_WARN("fail to reserve addr array", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < part_num; ++i) {
        if (OB_FAIL(addr.push_back(ObPartitionAddr()))) {
          LOG_WARN("fail to add empty tablegroup partition address", K(ret));
        } else if (OB_FAIL(partition_ids.push_back(keys.at(i).get_partition_id()))) {
          LOG_WARN("failed to push back partition key", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      bool is_non_part_table = false;
      ObArray<share::TenantUnitRepCnt*> ten_unit_arr;
      const uint64_t tenant_id = schema.get_tenant_id();
      ObArenaAllocator allocator(ObModIds::OB_RS_PARTITION_TABLE_TEMP);
      TenantSchemaGetter stat_finder(tenant_id);
      ObSinglePtBalanceContainer balance_container(schema, guard, stat_finder, allocator);
      common::ObSEArray<common::ObZone, 7> high_priority_zone_array;
      common::ObSEArray<share::ObRawPrimaryZoneUtil::ZoneScore, MAX_ZONE_NUM> zone_score_array;
      if (OB_INVALID_ID != schema.get_tablegroup_id() && addr.count() == schema.get_all_part_num()) {
        // the branch when the addr.count() is not equal to part_num shall never by here
        if (OB_FAIL(get_pg_partitions(schema, addr))) {
          LOG_WARN("get partition group partitions failed", K(ret), K(schema));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_UNLIKELY(nullptr == schema_service_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("schema service ptr is null", K(ret), KP(schema_service_));
        } else if (OB_FAIL(build_single_pt_balance_container(
                       guard, schema, balance_container, high_priority_zone_array, zone_score_array))) {
          LOG_WARN("fail to get build single pt balance container", K(ret), "tg_id", schema.get_tablegroup_id());
        } else if (OB_FAIL(process_replica_in_logonly_unit(schema, addr, create_mode, guard))) {
          LOG_WARN("fail to process logonly replica", K(ret), "schema_id", schema.get_table_id());
        } else if (OB_FAIL(get_new_partitions(balance_container,
                       high_priority_zone_array,
                       schema,
                       keys,
                       addr,
                       create_mode,
                       ten_unit_arr,
                       guard,
                       is_non_part_table))) {
          LOG_WARN("failed to get new partitions", K(ret));
        } else if (OB_FAIL(set_initial_leader(
                       balance_container, high_priority_zone_array, schema, partition_ids, addr, guard))) {
          LOG_WARN("set initial leader failed", K(ret), K(schema), K(partition_ids));
        }
      }
    }
  }
  return ret;
}

int ObReplicaCreator::alloc_tablegroup_partitions_for_standby(const share::schema::ObTablegroupSchema& schema,
    const common::ObIArray<ObPartitionKey>& keys, obrpc::ObCreateTableMode create_mode, ObITablePartitionAddr& addr,
    share::schema::ObSchemaGetterGuard& guard)
{
  ObArray<int64_t> partition_ids;
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(create_mode <= obrpc::OB_CREATE_TABLE_MODE_INVALID ||
                         create_mode >= obrpc::OB_CREATE_TABLE_MODE_MAX)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(create_mode));
  } else {
    addr.reuse();
    const int64_t part_num = keys.count();
    if (OB_FAIL(addr.reserve(part_num))) {
      LOG_WARN("fail to reserve addr array", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < part_num; ++i) {
        if (OB_FAIL(addr.push_back(ObPartitionAddr()))) {
          LOG_WARN("fail to add empty tablegroup partition address", K(ret));
        } else if (OB_FAIL(partition_ids.push_back(keys.at(i).get_partition_id()))) {
          LOG_WARN("failed to push back partition key", K(ret));
        }
      }
    }
    bool is_non_part_table = false;
    ObArray<share::TenantUnitRepCnt*> ten_unit_arr;
    if (OB_SUCC(ret)) {
      const uint64_t tenant_id = schema.get_tenant_id();
      ObArenaAllocator allocator(ObModIds::OB_RS_PARTITION_TABLE_TEMP);
      TenantSchemaGetter stat_finder(tenant_id);
      ObSinglePtBalanceContainer balance_container(schema, guard, stat_finder, allocator);
      common::ObSEArray<common::ObZone, 7> high_priority_zone_array;
      common::ObSEArray<share::ObRawPrimaryZoneUtil::ZoneScore, MAX_ZONE_NUM> zone_score_array;
      if (OB_UNLIKELY(nullptr == schema_service_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schema service ptr is null", K(ret), KP(schema_service_));
      } else if (OB_FAIL(build_single_pt_balance_container(
                     guard, schema, balance_container, high_priority_zone_array, zone_score_array))) {
        LOG_WARN("fail to get build single pt balance container", K(ret), "tg_id", schema.get_tablegroup_id());
      } else if (OB_FAIL(process_replica_in_logonly_unit(schema, addr, create_mode, guard))) {
        LOG_WARN("fail to process logonly replica", K(ret), "schema_id", schema.get_table_id());
      } else if (OB_FAIL(get_new_partitions(balance_container,
                     high_priority_zone_array,
                     schema,
                     keys,
                     addr,
                     create_mode,
                     ten_unit_arr,
                     guard,
                     is_non_part_table))) {
        LOG_WARN("failed to get new partitions", K(ret));
      } else if (OB_FAIL(set_initial_leader(
                     balance_container, high_priority_zone_array, schema, partition_ids, addr, guard))) {
        LOG_WARN("set initial leader failed", K(ret), K(schema), K(partition_ids));
      }
    }
  }
  return ret;
}
