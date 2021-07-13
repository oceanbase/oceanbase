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

#ifndef _OB_REPLICA_CREATOR_H
#define _OB_REPLICA_CREATOR_H 1
#include "share/ob_define.h"
#include "lib/list/ob_dlist.h"
#include "lib/container/ob_iarray.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_se_array.h"
#include "common/ob_unit_info.h"
#include "common/ob_partition_key.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_part_mgr_util.h"
#include "share/ob_rpc_struct.h"
#include "ob_replica_addr.h"
#include "ob_root_utils.h"
#include "ob_balancer_interface.h"
#include "ob_balance_group_container.h"
#include "share/ob_unit_replica_counter.h"
namespace oceanbase {
namespace common {
class ModulePageArena;
}
namespace share {
class ObPartitionTableOperator;
class ObSplitInfo;
namespace schema {
class ObMultiVersionSchemaService;
class ObSchemaGetterGuard;
}  // namespace schema
}  // namespace share
namespace rootserver {
class ObUnitManager;
class ObServerManager;
class ObZoneManager;
class ObRandomZoneSelector;
class ObCreateTableReplicaByLocality;

namespace balancer {
class ObSinglePtBalanceContainer;
class TenantSchemaGetter;
}  // namespace balancer

// To create replicas when creating tables
class ObReplicaCreator {
public:
  ObReplicaCreator();
  virtual ~ObReplicaCreator()
  {}
  int init(share::schema::ObMultiVersionSchemaService& schema_service, ObUnitManager& unit_mgr,
      ObServerManager& server_mgr, share::ObPartitionTableOperator& pt_operator, ObZoneManager& zone_mgr,
      share::ObCheckStopProvider& check_stop_provider);

  int alloc_tablegroup_partitions_for_create(const share::schema::ObTablegroupSchema& tablegroup_schema,
      const obrpc::ObCreateTableMode create_mode, common::ObIArray<ObPartitionAddr>& tablegroup_addr,
      ObIArray<share::TenantUnitRepCnt*>& ten_unit_arr);
  // add partition for create table
  int alloc_partitions_for_create(const share::schema::ObTableSchema& table, obrpc::ObCreateTableMode create_mode,
      ObITablePartitionAddr& addr, ObIArray<share::TenantUnitRepCnt*>& ten_unit_arr);
  // add partition for add partition
  template <typename SCHEMA>
  int alloc_partitions_for_add(const SCHEMA& table, const SCHEMA& inc_table, const obrpc::ObCreateTableMode create_mode,
      ObIArray<share::TenantUnitRepCnt*>& ten_unit_arr, ObITablePartitionAddr& addr);
  // add partition for split
  int alloc_partitions_for_split(const share::schema::ObPartitionSchema& table,
      const share::schema::ObPartitionSchema& inc_table, ObITablePartitionAddr& addr);
  int standby_alloc_partitions_for_split(const share::schema::ObTableSchema& table,
      const common::ObIArray<int64_t>& source_part_ids, const common::ObIArray<int64_t>& dest_partition_ids,
      ObITablePartitionAddr& addr);
  int alloc_table_partitions_for_standby(const share::schema::ObTableSchema& table,
      const common::ObIArray<ObPartitionKey>& keys, obrpc::ObCreateTableMode create_mode, ObITablePartitionAddr& addr,
      share::schema::ObSchemaGetterGuard& guard);
  int alloc_tablegroup_partitions_for_standby(const share::schema::ObTablegroupSchema& table_group,
      const common::ObIArray<ObPartitionKey>& keys, obrpc::ObCreateTableMode create_mode, ObITablePartitionAddr& addr,
      share::schema::ObSchemaGetterGuard& guard);

public:
  // types and constants
  typedef common::ObArray<share::ObUnitInfo> UnitArray;
  typedef common::ObSEArray<UnitArray, common::MAX_ZONE_NUM> ZoneUnitArray;
  typedef common::ObArray<share::ObUnitInfo*> UnitPtrArray;
  typedef common::ObSEArray<UnitPtrArray, common::MAX_ZONE_NUM> ZoneUnitPtrArray;

private:
  struct CmpZoneScore {
    bool operator()(share::ObRawPrimaryZoneUtil::ZoneScore& left, share::ObRawPrimaryZoneUtil::ZoneScore& right)
    {
      bool bool_ret = false;
      if (left.zone_score_ < right.zone_score_) {
        bool_ret = true;  // sorted asc by zone_score_
      } else {
        bool_ret = false;
      }
      return bool_ret;
    }
  };
  // function members
  // return OB_CANCELED if stop, else return OB_SUCCESS
  int check_stop() const
  {
    return check_stop_provider_->check_stop();
  }
  int build_single_pt_balance_container(share::schema::ObSchemaGetterGuard& schema_guard,
      const share::schema::ObPartitionSchema& partition_schema,
      balancer::ObSinglePtBalanceContainer& pt_balance_container,
      common::ObIArray<common::ObZone>& high_priority_zone_array,
      common::ObSEArray<share::ObRawPrimaryZoneUtil::ZoneScore, MAX_ZONE_NUM>& zone_score_array);
  int get_pg_partitions(const share::schema::ObTableSchema& table, ObITablePartitionAddr& addr);
  int init_addr_allocator_parameter(const share::schema::ObPartitionSchema& partition_schema,
      const obrpc::ObCreateTableMode create_mode, ObIArray<common::ObZone>& zone_list, ZoneUnitArray& unit_pool,
      ObIArray<share::ObZoneReplicaAttrSet>& zone_locality, ZoneUnitPtrArray& all_zone_units_alive,
      share::schema::ObSchemaGetterGuard& schema_guard);
  // invoked by primary cluster when create partitions
  int get_new_partitions(balancer::ObSinglePtBalanceContainer& pt_balance_container,
      common::ObSEArray<common::ObZone, 7>& high_priority_zone_array,
      const share::schema::ObPartitionSchema& partition_schema, ObITablePartitionAddr& addr,
      const obrpc::ObCreateTableMode create_mode, ObIArray<share::TenantUnitRepCnt*>& ten_unit_arr,
      const bool is_non_part_table);
  int do_get_new_partitions(ObCreateTableReplicaByLocality& addr_allocator,
      const share::schema::ObPartitionSchema& partition_schema, ObITablePartitionAddr& addr,
      const obrpc::ObCreateTableMode create_mode, ObIArray<share::TenantUnitRepCnt*>& ten_unit_arr,
      const bool is_non_part_table);
  // invokded by standby cluster when create partitions
  int get_new_partitions(balancer::ObSinglePtBalanceContainer& pt_balance_container,
      common::ObSEArray<common::ObZone, 7>& high_priority_zone_array,
      const share::schema::ObPartitionSchema& partition_schema, const ObIArray<common::ObPartitionKey>& keys,
      ObITablePartitionAddr& addr, const obrpc::ObCreateTableMode create_mode,
      ObIArray<share::TenantUnitRepCnt*>& ten_unit_arr, share::schema::ObSchemaGetterGuard& schema_guard,
      const bool is_non_part_table);
  int do_get_new_partitions(ObCreateTableReplicaByLocality& addr_allocator,
      const ObIArray<common::ObPartitionKey>& keys, ObITablePartitionAddr& addr,
      const obrpc::ObCreateTableMode create_mode, ObIArray<share::TenantUnitRepCnt*>& ten_unit_arr,
      const bool is_non_part_table);
  int recalc_partition_initial_leader_stat(ObPartitionAddr& partition_addr, bool& has_leader);
  int set_initial_leader(balancer::ObSinglePtBalanceContainer& pt_balance_container,
      common::ObSEArray<common::ObZone, 7>& high_priority_zone_array,
      const share::schema::ObPartitionSchema& partition_schema, const common::ObIArray<int64_t>& partition_ids,
      ObITablePartitionAddr& addr, share::schema::ObSchemaGetterGuard& schema_guard);
  int set_non_part_leader(common::ObSEArray<common::ObZone, 7>& high_priority_zone_array,
      common::ObSEArray<share::ObRawPrimaryZoneUtil::ZoneScore, MAX_ZONE_NUM>& zone_score_array,
      const share::schema::ObPartitionSchema& partition_schema, ObIArray<share::TenantUnitRepCnt*>& ten_unit_arr,
      ObITablePartitionAddr& addr);
  int get_alive_zone_list(common::ObSEArray<common::ObZone, 7>& high_priority_zone_array,
      common::ObSEArray<share::ObRawPrimaryZoneUtil::ZoneScore, MAX_ZONE_NUM>& zone_score_array,
      const ObPartitionAddr& paddr, ObIArray<common::ObZone>& alive_zone);
  int gen_alive_zone_list(const ObPartitionAddr& paddr, const share::ObRawPrimaryZoneUtil::ZoneScore& zone_score,
      ObIArray<common::ObZone>& alive_zone);
  void set_ten_unit_arr(ObIArray<share::TenantUnitRepCnt*>& ten_unit_arr, const ObPartitionAddr& paddr,
      const ObReplicaAddr& leader, const bool is_new_tablegroup);
  int set_partition_initial_leader(ObPartitionAddr& paddr, const share::schema::ObPartitionSchema& partition_schema,
      const common::ObPartitionKey& pkey, const bool small_tenant,
      common::ObSEArray<common::ObZone, 7>& high_priority_zone_array,
      const balancer::ObSinglePtBalanceContainer& balance_index_container,
      rootserver::ObRandomZoneSelector& random_selector, share::schema::ObSchemaGetterGuard& schema_guard);
  int generate_balance_group_index(const share::schema::ObPartitionSchema& partition_schema,
      const common::ObPartitionKey& pkey, const bool small_tenant,
      common::ObSEArray<common::ObZone, 7>& high_priority_zone_array,
      const balancer::ObSinglePtBalanceContainer& balance_index_container, common::ObZone& balance_group_zone);
  int get_partition_schema_first_primary_zone(const bool small_tenant,
      const share::schema::ObPartitionSchema& partition_schema, const rootserver::ObRandomZoneSelector& random_selector,
      const common::ObIArray<rootserver::ObReplicaAddr>& replica_addrs, const common::ObZone& balance_group_zone,
      common::ObZone& first_primary_zone, share::schema::ObSchemaGetterGuard& schema_guard);
  int try_compensate_readonly_all_server(share::schema::ObSchemaGetterGuard& schema_guard,
      const share::schema::ObPartitionSchema& schema, share::schema::ZoneLocalityIArray& zone_locality) const;
  int get_locality_info(
      const share::schema::ObPartitionSchema& partition_schema, share::schema::ZoneLocalityIArray& zone_locality) const;
  int check_all_partition_allocated(
      ObITablePartitionAddr& addr, const int64_t partition_num, const int64_t replica_num, bool& allocated) const;
  // Get table partitions, empty partition also returned an empty partition info.
  int table_all_partition(
      common::ObIAllocator& allocator, const uint64_t table_id, common::ObIArray<share::ObPartitionInfo>& parts);
  int partition_all_replica(const uint64_t table_id, const uint64_t partition_id, share::ObPartitionInfo& part);

  int set_same_addr_ignore_logonly(const share::ObPartitionInfo& info, share::schema::ObSchemaGetterGuard& schema_guard,
      const share::schema::ObTableSchema& table, const int64_t replica_num, ObPartitionAddr& addr);
  int set_same_addr(const share::ObPartitionInfo& sample_info, ObPartitionAddr& paddr);

  // Get tenant all online (has heartbeat with rs) unit grouped by zone.
  int tenant_online_unit_without_logonly(share::schema::ObSchemaGetterGuard& schema_guard, const uint64_t tenant_id,
      ZoneUnitArray& all_zone_unit, ZoneUnitPtrArray& all_zone_unit_ptr);
  int check_majority(share::schema::ObSchemaGetterGuard& schema_guard, const uint64_t tenant_id,
      const int64_t paxos_replica_num, const common::ObIArray<share::ObZoneReplicaAttrSet>& zone_locality,
      obrpc::ObCreateTableMode create_mode);

  int process_replica_in_logonly_unit(const share::schema::ObPartitionSchema& partition_schema,
      common::ObIArray<ObPartitionAddr>& tablegroup_addr, const obrpc::ObCreateTableMode create_mode,
      share::schema::ObSchemaGetterGuard& schema_guard);

  int process_replica_in_logonly_unit_per_partition(const common::ObIArray<share::ObZoneReplicaAttrSet>& zone_locality,
      const common::ObIArray<share::ObUnitInfo>& logonly_units, ObPartitionAddr& paddr,
      const obrpc::ObCreateTableMode create_mode);

private:
  // data members
  bool inited_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  ObZoneManager* zone_mgr_;
  ObUnitManager* unit_mgr_;
  ObServerManager* server_mgr_;
  share::ObPartitionTableOperator* pt_operator_;
  share::ObCheckStopProvider* check_stop_provider_;

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObReplicaCreator);
};

// @table: original table
// @inc_table: contains only the new partition, its sub-partition array is empty
//             and need to be filled from the original table schema
// TODO: support non-template subpartition
template <typename SCHEMA>
int ObReplicaCreator::alloc_partitions_for_add(const SCHEMA& table, const SCHEMA& inc_table,
    const obrpc::ObCreateTableMode create_mode, ObIArray<share::TenantUnitRepCnt*>& ten_unit_arr,
    ObITablePartitionAddr& addr)
{
  int ret = OB_SUCCESS;
  RS_TRACE(alloc_replica_begin);
  SCHEMA schema_for_add;
  if (!inited_) {
    ret = OB_NOT_INIT;
    RS_LOG(WARN, "not inited", K(ret));
  } else if (!table.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    RS_LOG(WARN, "invalid table", K(ret), K(table));
  } else if (OB_FAIL(schema_for_add.assign(table))) {
    RS_LOG(WARN, "fail to assign table", K(ret));
  } else if (FALSE_IT(schema_for_add.reset_dropped_partition())) {
  } else if (OB_FAIL(schema_for_add.try_assign_part_array(inc_table))) {
    RS_LOG(WARN, "fail to try assign part array", K(ret));
  } else if (OB_FAIL(schema_for_add.try_assign_def_subpart_array(table))) {
    RS_LOG(WARN, "fail to try assign subpart array", K(ret));
  } else {
    int64_t partition_num = schema_for_add.get_partition_num();
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_num; i++) {
      share::schema::ObPartition* partition = schema_for_add.get_part_array()[i];
      if (OB_ISNULL(partition)) {
        ret = OB_ERR_UNEXPECTED;
        RS_LOG(WARN, "the partition is null", K(ret));
      } else if (OB_INVALID_ID == partition->get_part_id()) {
        int64_t max_used_part_id = table.get_part_option().get_max_used_part_id();
        if (0 > max_used_part_id) {
          ret = OB_ERR_UNEXPECTED;
          RS_LOG(WARN, "max_used_part_id less 0 when adding part", K(ret));
        } else {
          partition->set_part_id(max_used_part_id - partition_num + i + 1);
        }
      } else {
        int64_t subpartition_num = partition->get_subpartition_num();
        for (int64_t j = 0; j < subpartition_num; j++) {
          share::schema::ObSubPartition* subpart = partition->get_subpart_array()[j];
          if (OB_ISNULL(subpart)) {
            ret = OB_ERR_UNEXPECTED;
            RS_LOG(WARN, "the subpart_array[j] is null", K(ret), K(j));
          } else if (OB_INVALID_ID == subpart->get_sub_part_id()) {
            int64_t max_used_subpart_id = partition->get_max_used_sub_part_id();
            if (0 > max_used_subpart_id) {
              ret = OB_ERR_UNEXPECTED;
              RS_LOG(WARN, "max_used_subpart_id less 0 when adding part", K(ret));
            } else {
              subpart->set_sub_part_id(max_used_subpart_id - subpartition_num + j + 1);
            }
          }
        }
      }
    }
    addr.reuse();
    int64_t inc_part_num = inc_table.get_part_option().get_part_num();
    if (OB_SUCC(ret) && share::schema::PARTITION_LEVEL_TWO == table.get_part_level()) {
      if (!table.is_sub_part_template()) {
        inc_part_num = 0;
        for (int64_t i = 0; i < inc_table.get_partition_num(); i++) {
          if (OB_ISNULL(inc_table.get_part_array()[i])) {
            ret = OB_ERR_UNEXPECTED;
            RS_LOG(WARN, "part_array[i] is null", K(ret), K(i));
          } else {
            inc_part_num += inc_table.get_part_array()[i]->get_subpartition_num();
          }
        }
      } else {
        inc_part_num *= table.get_sub_part_option().get_part_num();
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(addr.reserve(inc_part_num))) {
      RS_LOG(WARN, "array reserve failed", K(ret), K(inc_part_num));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < inc_part_num; ++i) {
        if (OB_FAIL(addr.push_back(ObPartitionAddr()))) {
          RS_LOG(WARN, "add empty partition address failed", K(ret), K(i));
        }
      }
    }
    if (OB_SUCC(ret)) {
      const uint64_t tenant_id = table.get_tenant_id();
      share::schema::ObSchemaGetterGuard schema_guard;
      ObArenaAllocator allocator(ObModIds::OB_RS_PARTITION_TABLE_TEMP);
      balancer::TenantSchemaGetter stat_finder(tenant_id);
      balancer::ObSinglePtBalanceContainer balance_container(schema_for_add, schema_guard, stat_finder, allocator);
      common::ObSEArray<common::ObZone, 7> high_priority_zone_array;
      common::ObSEArray<share::ObRawPrimaryZoneUtil::ZoneScore, MAX_ZONE_NUM> zone_score_array;
      ObArray<int64_t> partition_ids;
      share::schema::ObPartIdsGeneratorForAdd<share::schema::ObPartitionSchema> gen(table, inc_table);
      const bool is_non_part_table = false;
      if (OB_UNLIKELY(nullptr == schema_service_)) {
        ret = OB_ERR_UNEXPECTED;
        RS_LOG(WARN, "schema service ptr is null", K(ret), KP(schema_service_));
      } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
        RS_LOG(WARN, "fail to get schema guard", K(ret));
      } else if (OB_FAIL(build_single_pt_balance_container(
                     schema_guard, table, balance_container, high_priority_zone_array, zone_score_array))) {
        RS_LOG(WARN, "fail to build single pt balance container", K(ret));
      } else if (OB_FAIL(process_replica_in_logonly_unit(table, addr, create_mode, schema_guard))) {
        RS_LOG(WARN, "fail to process logonly replica in logonly unit", K(ret));
      } else if (OB_FAIL(get_new_partitions(balance_container,
                     high_priority_zone_array,
                     schema_for_add,
                     addr,
                     create_mode,
                     ten_unit_arr,
                     is_non_part_table))) {
        RS_LOG(WARN, "fail to get new partitions", K(ret));
      } else if (OB_FAIL(gen.gen(partition_ids))) {
        RS_LOG(WARN, "fail to generate part ids", K(ret));
      } else if (OB_FAIL(set_initial_leader(
                     balance_container, high_priority_zone_array, table, partition_ids, addr, schema_guard))) {
        RS_LOG(WARN, "fail to set initial leader", K(ret));
      }
    }
  }
  RS_TRACE(alloc_replica_end);
  return ret;
}

}  // end namespace rootserver
}  // end namespace oceanbase

#endif /* _OB_REPLICA_CREATOR_H */
