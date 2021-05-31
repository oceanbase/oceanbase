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

#include "ob_leader_coordinator.h"

#include "lib/allocator/page_arena.h"
#include "lib/net/ob_addr.h"
#include "lib/time/ob_time_utility.h"
#include "lib/profile/ob_trace_id.h"
#include "lib/container/ob_array_iterator.h"
#include "lib/container/ob_se_array_iterator.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/string/ob_sql_string.h"
#include "lib/stat/ob_diagnose_info.h"
#include "share/ob_srv_rpc_proxy.h"
#include "share/partition_table/ob_partition_table_operator.h"
#include "share/partition_table/ob_partition_table_iterator.h"
#include "share/partition_table/ob_replica_filter.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_part_mgr_util.h"
#include "share/schema/ob_schema_mgr.h"
#include "share/ob_rpc_struct.h"
#include "share/ob_leader_election_waiter.h"
#include "share/config/ob_server_config.h"
#include "share/ob_multi_cluster_util.h"
#include "observer/ob_server_struct.h"
#include "rootserver/ob_root_utils.h"
#include "rootserver/ob_server_manager.h"
#include "rootserver/ob_zone_manager.h"
#include "rootserver/ob_rebalance_task_mgr.h"
#include "rootserver/ob_unit_manager.h"
#include "rootserver/ob_daily_merge_scheduler.h"
#include "rootserver/ob_rs_async_rpc_proxy.h"
#include "rootserver/ob_rs_event_history_table_operator.h"
#include "rootserver/ob_root_service.h"

namespace oceanbase {
using namespace common;
using namespace common::hash;
using namespace obrpc;
using namespace share;
using namespace share::schema;
namespace rootserver {
using namespace balancer;

int ObRandomZoneSelector::init(ObZoneManager& zone_mgr)
{
  int ret = OB_SUCCESS;
  if (is_inited()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else {
    ObSEArray<ObZone, DEFAULT_ZONE_CNT> zones;
    if (OB_FAIL(zone_mgr.get_zone(zones))) {
      LOG_WARN("get all zones failed", K(ret));
    } else if (zones.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("empty zones", K(ret));
    } else {
      const int64_t init_score = 0;
      FOREACH_CNT_X(z, zones, OB_SUCC(ret))
      {
        if (OB_FAIL(zones_.push_back(std::make_pair(*z, init_score)))) {
          LOG_WARN("add zone failed", K(ret));
        }
      }
      std::sort(zones_.begin(), zones_.end());
    }
  }
  return ret;
}

int ObRandomZoneSelector::update_score(const uint64_t tg_id, const int64_t partition_id)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tg_id || 0 == tg_id || partition_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tg_id), K(partition_id));
  } else {
    // simple combine murmur hash and fnv hash for better randomize
    uint32_t score = 0;
    score = murmurhash2(&tg_id, sizeof(tg_id), score);
    if (extract_tenant_id(tg_id) != 0  // valid tablegroup_id or table_id
        && partition_id > 0) {
      score = murmurhash2(&partition_id, sizeof(partition_id), score);
    }
    score = fnv_hash2(&tg_id, sizeof(tg_id), score);
    if (extract_tenant_id(tg_id) != 0  // valid tablegroup_id or table_id
        && partition_id > 0) {
      score = fnv_hash2(&partition_id, sizeof(partition_id), score);
    }

    FOREACH(z, zones_)
    {
      z->second = 0;
    }
    zones_.at(score % zones_.count()).second = zones_.count();
    for (int64_t i = 1; i < zones_.count(); ++i) {
      const int64_t pos = score % (zones_.count() - i);
      int64_t idx = 0;
      FOREACH(z, zones_)
      {
        if (0 == z->second) {
          if (idx == pos) {
            z->second = zones_.count() - i;
          }
          ++idx;
        }
      }
    }
  }
  return ret;
}

int ObRandomZoneSelector::get_zone_score(const ObZone& zone, int64_t& score) const
{
  int ret = OB_ENTRY_NOT_EXIST;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (zone.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid zone", K(ret), K(zone));
  } else {
    FOREACH_CNT_X(z, zones_, OB_FAIL(ret))
    {
      if (z->first == zone) {
        score = z->second;
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int PartitionInfoContainer::init(share::ObPartitionTableOperator* pt_operator,
    share::schema::ObMultiVersionSchemaService* schema_service, ObLeaderCoordinator* host)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(nullptr == pt_operator || nullptr == schema_service || nullptr == host)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(partition_info_map_.create(
                 PART_INFO_MAP_SIZE, ObModIds::OB_RS_LEADER_COORDINATOR, ObModIds::OB_RS_LEADER_COORDINATOR))) {
    LOG_WARN("fail to create partition info map", K(ret));
  } else {
    pt_operator_ = pt_operator;
    schema_service_ = schema_service;
    host_ = host;
    is_inited_ = true;
  }
  return ret;
}

int PartitionInfoContainer::build_tenant_partition_info(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_UNLIKELY(nullptr == pt_operator_ || nullptr == schema_service_ || nullptr == host_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pt_operator OR schema_service OR host ptr is null", K(ret));
  } else {
    partition_info_map_.destroy();
    ObTenantPartitionIterator tenant_partition_iter;
    const bool ignore_row_checksum = true;
    if (OB_FAIL(partition_info_map_.create(
            PART_INFO_MAP_SIZE, ObModIds::OB_RS_LEADER_COORDINATOR, ObModIds::OB_RS_LEADER_COORDINATOR))) {
      LOG_WARN("fail to create partition info map", K(ret));
    } else if (OB_FAIL(tenant_partition_iter.init(*pt_operator_, *schema_service_, tenant_id, ignore_row_checksum))) {
      LOG_WARN("fail to init tenant partition init", K(ret));
    } else {
      ObPartitionInfo partition_info;
      while (OB_SUCC(ret) && OB_SUCC(tenant_partition_iter.next(partition_info))) {
        host_->update_last_run_timestamp();  // save current time for thread checker.
        const uint64_t table_id = partition_info.get_table_id();
        const int64_t partition_id = partition_info.get_partition_id();
        const int64_t partition_cnt = 0;
        const int32_t overwrite = 0;
        common::ObPartitionKey pkey;
        if (OB_FAIL(host_->remove_lost_replicas(partition_info))) {
          LOG_WARN("fail to remove lost replicas", K(ret));
        } else if (OB_FAIL(pkey.init(table_id, partition_id, partition_cnt))) {
          LOG_WARN("fail to init partition key", K(ret));
        } else if (OB_FAIL(partition_info_map_.set_refactored(pkey, partition_info, overwrite))) {
          LOG_WARN("fail to set partition info map", K(ret));
        } else {
        }  // good
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int PartitionInfoContainer::build_partition_group_info(
    const uint64_t tenant_id, const uint64_t partition_entity_id, const int64_t partition_id)
{
  int ret = OB_SUCCESS;
  share::schema::ObSchemaGetterGuard schema_guard;
  const share::schema::ObPartitionSchema* partition_schema = nullptr;
  int64_t partition_idx = -1;
  common::ObArray<uint64_t> partition_entity_ids;
  partition_info_map_.destroy();
  const share::schema::ObSchemaType schema_type =
      (is_new_tablegroup_id(partition_entity_id) ? share::schema::ObSchemaType::TABLEGROUP_SCHEMA
                                                 : share::schema::ObSchemaType::TABLE_SCHEMA);
  bool check_dropped_partition = true;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == partition_entity_id || partition_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(partition_entity_id), K(partition_id));
  } else if (OB_UNLIKELY(nullptr == pt_operator_ || nullptr == schema_service_ || nullptr == host_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pt_operator OR schema_service OR host ptr is null", K(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret));
  } else if (OB_FAIL(ObPartMgrUtils::get_partition_schema(
                 schema_guard, partition_entity_id, schema_type, partition_schema))) {
    LOG_WARN("fail to get partition schema", K(ret));
  } else if (OB_UNLIKELY(nullptr == partition_schema)) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("partition schema ptr is null", K(ret), K(partition_entity_id));
  } else if (!partition_schema->has_self_partition()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition entity has no physical partition", K(ret), K(partition_entity_id));
  } else if (OB_FAIL(ObPartMgrUtils::get_partition_idx_by_id(
                 *partition_schema, check_dropped_partition, partition_id, partition_idx))) {
    LOG_WARN("fail to get partition idx", K(ret), K(partition_entity_id), K(partition_id));
  } else if (OB_FAIL(ObLeaderCoordinator::get_same_tablegroup_partition_entity_ids(
                 schema_guard, partition_entity_id, partition_entity_ids))) {
    LOG_WARN("fail to get same tabelgroup ids", K(ret));
  } else if (OB_FAIL(partition_info_map_.create(
                 PART_INFO_MAP_SIZE, ObModIds::OB_RS_LEADER_COORDINATOR, ObModIds::OB_RS_LEADER_COORDINATOR))) {
    LOG_WARN("fail to create partition info map", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_entity_ids.count(); ++i) {
      const ObPartitionSchema* partition_schema = NULL;
      const uint64_t this_partition_entity_id = partition_entity_ids.at(i);
      const share::schema::ObSchemaType this_schema_type =
          (is_new_tablegroup_id(this_partition_entity_id) ? share::schema::ObSchemaType::TABLEGROUP_SCHEMA
                                                          : share::schema::ObSchemaType::TABLE_SCHEMA);
      if (OB_FAIL(ObPartMgrUtils::get_partition_schema(
              schema_guard, this_partition_entity_id, this_schema_type, partition_schema))) {
        LOG_WARN("fail to get partition schema", K(ret), K(this_partition_entity_id));
      } else if (NULL == partition_schema) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("partition schema not exist", K(ret), K(this_partition_entity_id));
      } else if (!partition_schema->has_self_partition()) {
        // bypass
      } else {
        bool check_dropped_schema = true;
        ObPartitionKeyIter iter(this_partition_entity_id, *partition_schema, check_dropped_schema);
        int64_t j = 0;
        int64_t phy_part_id = -1;
        while (OB_SUCC(ret) && OB_SUCC(iter.next_partition_id_v2(phy_part_id))) {
          ObArenaAllocator allocator(ObModIds::OB_RS_PARTITION_TABLE_TEMP);
          share::ObPartitionInfo part_info;
          part_info.set_allocator(&allocator);
          common::ObPartitionKey pkey;
          const int32_t overwrite = 0;
          if (j != partition_idx) {
            // bypass
          } else if (OB_FAIL(pt_operator_->get(this_partition_entity_id, phy_part_id, part_info))) {
            LOG_WARN("fail to get partition info", K(ret), K(this_partition_entity_id), K(phy_part_id));
          } else if (OB_FAIL(host_->remove_lost_replicas(part_info))) {
            LOG_WARN("fail to remove lost replicas", K(ret));
          } else if (OB_FAIL(pkey.init(this_partition_entity_id, phy_part_id, 0 /*part cnt*/))) {
            LOG_WARN("fail to init partition key", K(ret));
          } else if (OB_FAIL(partition_info_map_.set_refactored(pkey, part_info, overwrite))) {
            LOG_WARN("fail to set partition info map", K(ret));
          } else {
          }  // good
          ++j;
        }
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
      }
    }
  }
  return ret;
}

int PartitionInfoContainer::get(
    const uint64_t partition_entity_id, const int64_t phy_partition_id, const share::ObPartitionInfo*& partition_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == partition_entity_id || phy_partition_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(partition_entity_id), K(phy_partition_id));
  } else {
    ObPartitionKey pkey;
    if (OB_FAIL(pkey.init(partition_entity_id, phy_partition_id, 0 /* part cnt ignore*/))) {
      LOG_WARN("pkey init", K(ret), K(partition_entity_id), K(phy_partition_id));
    } else {
      const share::ObPartitionInfo* my_part_info = nullptr;
      my_part_info = partition_info_map_.get(pkey);
      if (nullptr != my_part_info) {
        partition_info = my_part_info;
      } else {
        ret = OB_ENTRY_NOT_EXIST;
      }
    }
  }
  return ret;
}

ObLeaderCoordinator::ServerReplicaMsgContainer::ServerReplicaMsgContainer(
    common::ObIAllocator& allocator, ObLeaderCoordinator& host)
    : inner_allocator_(allocator),
      host_(host),
      server_leader_info_(),
      server_replica_counter_map_(),
      zone_replica_counter_map_(),
      valid_zone_array_(),
      valid_zone_total_leader_cnt_(0),
      pg_cnt_(0),
      inited_(false)
{}

ObLeaderCoordinator::ServerReplicaMsgContainer::~ServerReplicaMsgContainer()
{
  ObAllServerLeaderMsg::iterator iter = server_leader_info_.begin();
  for (; iter != server_leader_info_.end(); ++iter) {
    ObServerLeaderMsg* ptr = iter->second;
    if (nullptr != ptr) {
      destroy_and_free_server_leader_msg(ptr);
      ptr = nullptr;
    }
  }
}

int ObLeaderCoordinator::ServerReplicaMsgContainer::init(const int64_t pg_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (pg_cnt <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(pg_cnt));
  } else if (OB_FAIL(server_leader_info_.create(
                 2 * MAX_SERVER_COUNT, ObModIds::OB_RS_LEADER_COORDINATOR, ObModIds::OB_RS_LEADER_COORDINATOR))) {
    LOG_WARN("fail to create server leader info", K(ret));
  } else if (OB_FAIL(server_replica_counter_map_.create(
                 2 * MAX_SERVER_COUNT, ObModIds::OB_RS_LEADER_COORDINATOR, ObModIds::OB_RS_LEADER_COORDINATOR))) {
    LOG_WARN("fail to create server replica counter map", K(ret));
  } else if (OB_FAIL(zone_replica_counter_map_.create(
                 2 * MAX_ZONE_NUM, ObModIds::OB_RS_LEADER_COORDINATOR, ObModIds::OB_RS_LEADER_COORDINATOR))) {
    LOG_WARN("fail to create zone replica counter map", K(ret));
  } else {
    pg_cnt_ = pg_cnt;
    inited_ = true;
  }
  return ret;
}

void ObLeaderCoordinator::ServerReplicaMsgContainer::destroy_and_free_server_leader_msg(ObServerLeaderMsg* msg)
{
  if (nullptr != msg) {
    while (!msg->leader_partition_list_.is_empty()) {
      ObPartitionMsg* part_msg = msg->leader_partition_list_.remove_first();
      if (nullptr != part_msg) {
        part_msg->~ObPartitionMsg();
        inner_allocator_.free(part_msg);
        part_msg = nullptr;
      }
    }
    msg->~ObServerLeaderMsg();
    inner_allocator_.free(msg);
    msg = nullptr;  // make no sense
  }
}

int ObLeaderCoordinator::ServerReplicaMsgContainer::collect_replica(
    Partition* partition_ptr, const share::ObPartitionReplica& replica)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(nullptr == partition_ptr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(partition_ptr));
  } else if (!replica.in_member_list_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("replica not in member list should not be here", K(ret), K(replica));
  } else {
    const bool is_leader = replica.is_leader_like();
    const common::ObZone& zone = replica.zone_;
    const common::ObAddr& server = replica.server_;
    // 0. collect server leader info map
    if (is_leader) {
      ObServerLeaderMsg* server_leader_msg = nullptr;
      int tmp_ret = server_leader_info_.get_refactored(server, server_leader_msg);
      if (OB_HASH_NOT_EXIST == tmp_ret) {
        const int32_t overwrite = 0;
        void* raw_ptr = nullptr;
        if (nullptr == (raw_ptr = inner_allocator_.alloc(sizeof(ObServerLeaderMsg)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to allocate memory", K(ret));
        } else if (FALSE_IT(server_leader_msg = new (raw_ptr) ObServerLeaderMsg())) {
          // Implicit expect_leader_cnt is 0 in the ObServerleaderMsg constructor
        } else if (FALSE_IT(server_leader_msg->zone_ = zone)) {
          // never be here
        } else if (OB_FAIL(server_leader_info_.set_refactored(server, server_leader_msg, overwrite))) {
          LOG_WARN("fail to set refactored", K(ret));
        }
        if (OB_FAIL(ret)) {
          if (nullptr != server_leader_msg) {
            destroy_and_free_server_leader_msg(server_leader_msg);
            server_leader_msg = nullptr;
          } else if (nullptr != raw_ptr) {
            inner_allocator_.free(raw_ptr);
            raw_ptr = nullptr;
          }
        }
      } else if (OB_SUCCESS == tmp_ret) {
        // bypass
      } else {
        ret = tmp_ret;
        LOG_WARN("fail to get refactored", K(ret));
      }

      if (OB_SUCC(ret) && nullptr != server_leader_msg) {
        ++server_leader_msg->curr_leader_cnt_;
        void* raw_ptr = nullptr;
        ObPartitionMsg* part_msg = nullptr;
        if (nullptr == (raw_ptr = inner_allocator_.alloc(sizeof(ObPartitionMsg)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to allocate memory", K(ret));
        } else if (FALSE_IT(part_msg = new (raw_ptr) ObPartitionMsg())) {
          // shall never by here
        } else if (!server_leader_msg->leader_partition_list_.add_last(part_msg)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to add to partition list", K(ret));
        } else {
          part_msg->partition_msg_ = partition_ptr;
        }
        if (OB_FAIL(ret)) {
          if (nullptr != part_msg) {
            part_msg->~ObPartitionMsg();
            inner_allocator_.free(part_msg);
            part_msg = nullptr;
          } else if (nullptr != raw_ptr) {
            inner_allocator_.free(raw_ptr);
            raw_ptr = nullptr;
          }
        }
      }
    }
    // 1. collect server replica counter
    if (OB_SUCC(ret)) {
      ServerReplicaCounter* sr_counter = nullptr;
      if (OB_UNLIKELY(!server.is_valid() || zone.is_empty())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(ret), K(server), K(zone));
      } else if (nullptr == (sr_counter = const_cast<ServerReplicaCounter*>(server_replica_counter_map_.get(server)))) {
        // not on valid server, no need to process
      } else {
        sr_counter->leader_cnt_ = is_leader ? sr_counter->leader_cnt_ + 1 : sr_counter->leader_cnt_;
        sr_counter->full_replica_cnt_ = common::REPLICA_TYPE_FULL == replica.replica_type_
                                            ? sr_counter->full_replica_cnt_ + 1
                                            : sr_counter->full_replica_cnt_;
      }
    }
  }
  return ret;
}

int ObLeaderCoordinator::ServerReplicaMsgContainer::check_need_balance_by_leader_cnt(bool& need_balance)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ServerReplicaMsgContainer not init", K(ret));
  } else {
    for (ServerReplicaCounterMap::iterator iter = server_replica_counter_map_.begin();
         OB_SUCC(ret) && iter != server_replica_counter_map_.end();
         ++iter) {
      const common::ObZone& zone = iter->second.zone_;
      ZoneReplicaCounter* zr_counter = nullptr;
      zr_counter = const_cast<ZoneReplicaCounter*>(zone_replica_counter_map_.get(zone));
      if (OB_UNLIKELY(nullptr == zr_counter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("zr_counter ptr is null", K(ret));
      } else {
        if (zr_counter->max_full_replica_cnt_ < iter->second.full_replica_cnt_) {
          zr_counter->max_full_replica_cnt_ = iter->second.full_replica_cnt_;
        }
        if (zr_counter->min_full_replica_cnt_ > iter->second.full_replica_cnt_) {
          zr_counter->min_full_replica_cnt_ = iter->second.full_replica_cnt_;
        }
        if (zr_counter->max_leader_cnt_ < iter->second.leader_cnt_) {
          zr_counter->max_leader_cnt_ = iter->second.leader_cnt_;
        }
        if (zr_counter->min_leader_cnt_ > iter->second.leader_cnt_) {
          zr_counter->min_leader_cnt_ = iter->second.leader_cnt_;
        }
        if (zr_counter->max_exp_leader_cnt_ <= iter->second.leader_cnt_) {
          zr_counter->curr_max_leader_server_cnt_++;
        }
        zr_counter->total_leader_cnt_ += iter->second.leader_cnt_;
        valid_zone_total_leader_cnt_ += iter->second.leader_cnt_;
      }
    }

    int64_t zone_max_leader_cnt = 0;
    int64_t zone_min_leader_cnt = INT64_MAX;
    bool full_replica_balance = true;
    int64_t zone_count = 0;
    need_balance = false;
    if (OB_SUCC(ret)) {
      for (ZoneReplicaCounterMap::iterator iter = zone_replica_counter_map_.begin();
           OB_SUCC(ret) && iter != zone_replica_counter_map_.end();
           ++iter) {
        ZoneReplicaCounter& zr_counter = iter->second;
        if (zr_counter.max_full_replica_cnt_ - zr_counter.min_full_replica_cnt_ > 1) {
          full_replica_balance = false;
          // The full replica in the zone is not balanced, temporarily not balanced
          need_balance = false;
          LOG_INFO("full replica is not enough, do not balance", K(zr_counter));
          break;
        } else if (zr_counter.max_leader_cnt_ - zr_counter.min_leader_cnt_ > 1) {
          // The number of leaders in the zone is unbalanced and needs to be readjusted
          need_balance = true;
        } else {
          if (zone_max_leader_cnt < zr_counter.total_leader_cnt_) {
            zone_max_leader_cnt = zr_counter.total_leader_cnt_;
          }
          if (zone_min_leader_cnt > zr_counter.total_leader_cnt_) {
            zone_min_leader_cnt = zr_counter.total_leader_cnt_;
          }
        }
        zone_count++;
      }
    }

    if (OB_SUCC(ret) && full_replica_balance && !need_balance && zone_count == zone_replica_counter_map_.size()) {
      // The number of leaders between zones is unbalanced and needs to be adjusted
      if (zone_max_leader_cnt - zone_min_leader_cnt > 1) {
        need_balance = true;
      }
    }

    if (OB_SUCC(ret) && full_replica_balance && !need_balance) {
      if (pg_cnt_ < valid_zone_total_leader_cnt_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("leader count unexpected",
            K(ret),
            "total_pg",
            pg_cnt_,
            "valid_zone_total_leader_count",
            valid_zone_total_leader_cnt_);
      } else if (pg_cnt_ > valid_zone_total_leader_cnt_) {
        // Not all leaders are in the valid zone, and leader balancing needs to be done
        need_balance = true;
      } else {
        // Leaders are all in the valid zone
      }
    }
  }
  return ret;
}

int ObLeaderCoordinator::ServerReplicaMsgContainer::check_and_build_available_zones(const int64_t balance_group_id,
    const common::ObIArray<common::ObZone>& primary_zone_array, const common::ObIArray<share::ObUnit>& tenant_unit,
    bool& need_balance)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ServerReplicaMsgContainer not init", K(ret));
  } else if (OB_UNLIKELY(primary_zone_array.count() <= 0)) {
    need_balance = false;  // has no available primary zone
    LOG_INFO("has no available valid primary zone, ignore leader count balance");
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_unit.count(); ++i) {
      const share::ObUnit& unit = tenant_unit.at(i);
      bool is_active = true;
      bool is_stopped = false;
      if (OB_UNLIKELY(nullptr == host_.server_mgr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("host_.server_mgr ptr is null", K(ret));
      } else if (OB_FAIL(host_.server_mgr_->check_server_active(unit.server_, is_active))) {
        LOG_WARN("fail to check server active", K(ret), "server", unit.server_);
      } else if (OB_FAIL(host_.server_mgr_->check_server_stopped(unit.server_, is_stopped))) {
        LOG_WARN("fail to check server stopped", K(ret), "server", unit.server_);
      } else if (is_active && !is_stopped && has_exist_in_array(primary_zone_array, unit.zone_)) {
        const ZoneReplicaCounter* zrc_ptr = zone_replica_counter_map_.get(unit.zone_);
        if (nullptr == zrc_ptr) {
          ZoneReplicaCounter zone_replica_counter;
          if (OB_FAIL(zone_replica_counter_map_.set_refactored(unit.zone_, zone_replica_counter))) {
            LOG_WARN("fail to set refactored", K(ret), "zone", unit.zone_);
          } else if (OB_FAIL(valid_zone_array_.push_back(unit.zone_))) {
            LOG_WARN("fail to push back", K(ret));
          }
        } else {
          // already exist, no need to push in
        }
      } else {
        // not active or unit not in primary zone
      }
    }
    if (OB_FAIL(ret)) {
      // failed
    } else if (valid_zone_array_.count() <= 0) {
      need_balance = false;
      LOG_INFO("no valid server in valid primary zone", K(primary_zone_array));
    } else {
      const int64_t available_zone_cnt = valid_zone_array_.count();
      const int64_t total_leader_cnt = pg_cnt_;
      // min leader cnt
      const int64_t min_l_cnt = total_leader_cnt / available_zone_cnt;
      // max leader cnt
      const int64_t max_l_cnt = (min_l_cnt * available_zone_cnt == total_leader_cnt) ? (min_l_cnt) : (min_l_cnt + 1);
      // max zone cnt
      const int64_t max_z_cnt = total_leader_cnt - (min_l_cnt * available_zone_cnt);
      // min zone cnt
      const int64_t min_z_cnt = available_zone_cnt - max_z_cnt;
      int64_t seed = balance_group_id;
      // Increase randomness with balance group id

      for (int64_t i = 0; OB_SUCC(ret) && i < valid_zone_array_.count(); ++i) {
        const common::ObZone& this_zone = valid_zone_array_.at(i);
        ZoneReplicaCounter* zrc = nullptr;
        zrc = const_cast<ZoneReplicaCounter*>(zone_replica_counter_map_.get(this_zone));
        if (nullptr == zrc) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected zrs, ptr is null", K(ret));
        } else {
          const int64_t idx = seed % available_zone_cnt;
          zrc->seed_ = seed++;
          zrc->expected_leader_cnt_ = (idx < min_z_cnt ? min_l_cnt : max_l_cnt);
        }
      }
    }
  }
  return ret;
}

int ObLeaderCoordinator::ServerReplicaMsgContainer::check_and_build_available_servers(
    const int64_t balance_group_id, const common::ObIArray<share::ObUnit>& tenant_unit, bool& need_balance)
{
  int ret = OB_SUCCESS;
  need_balance = true;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ServerReplicaMsgContainer not init", K(ret));
  } else {
    UNUSED(balance_group_id);
    for (int64_t i = 0; need_balance && OB_SUCC(ret) && i < tenant_unit.count(); ++i) {
      const share::ObUnit& unit = tenant_unit.at(i);
      const common::ObZone& zone = unit.zone_;
      ServerReplicaCounter sr_counter;
      sr_counter.zone_ = zone;
      ZoneReplicaCounter* zr_counter = nullptr;
      zr_counter = const_cast<ZoneReplicaCounter*>(zone_replica_counter_map_.get(zone));
      const int32_t overwrite = 0;
      bool is_active = false;
      bool is_stop = false;
      if (nullptr == zr_counter) {
        // bypass, since this unit not in available primary zone
      } else if (unit.migrate_from_server_.is_valid()) {
        need_balance = false;  // unit in migrating, ignore leader balance
      } else if (OB_UNLIKELY(nullptr == (host_.server_mgr_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("server mgr ptr is null", K(ret));
      } else if (OB_FAIL(host_.server_mgr_->check_server_active(unit.server_, is_active))) {
        LOG_WARN("fail to check server active", K(ret), "server", unit.server_);
      } else if (OB_FAIL(host_.server_mgr_->check_server_stopped(unit.server_, is_stop))) {
        LOG_WARN("fail to check server stop", K(ret), "server", unit.server_);
      } else if (!is_active || is_stop) {
        // bypass
      } else if (OB_FAIL(server_replica_counter_map_.set_refactored(unit.server_, sr_counter, overwrite))) {
        LOG_WARN("fail to set refactored", K(ret));
      } else {
        zr_counter->available_server_cnt_++;
      }
    }
    for (ServerReplicaCounterMap::iterator iter = server_replica_counter_map_.begin();
         need_balance && OB_SUCC(ret) && iter != server_replica_counter_map_.end();
         ++iter) {
      const common::ObAddr& server = iter->first;
      const common::ObZone& zone = iter->second.zone_;
      void* raw_ptr = nullptr;
      ObServerLeaderMsg* server_leader_msg = nullptr;
      ZoneReplicaCounter* zr_counter = nullptr;
      const int32_t overwrite = 0;
      if (nullptr == (zr_counter = const_cast<ZoneReplicaCounter*>(zone_replica_counter_map_.get(zone)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("zr counte ptr null unexpected", K(ret), K(zone), K(server));
      } else if (nullptr == (raw_ptr = (inner_allocator_.alloc(sizeof(ObServerLeaderMsg))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory", K(ret));
      } else if (FALSE_IT(server_leader_msg = new (raw_ptr) ObServerLeaderMsg())) {
        // shall never be here
      } else if (FALSE_IT(server_leader_msg->zone_ = zone)) {
        // never be here
      } else if (OB_FAIL(server_leader_info_.set_refactored(server, server_leader_msg, overwrite))) {
        LOG_WARN("fail to set refactored", K(ret));
      } else if (zr_counter->available_server_cnt_ <= 0) {
        need_balance = false;
        LOG_INFO("no valid server in valid primary zone", K(zone), K(server));
      } else {
        const int64_t available_server_cnt = zr_counter->available_server_cnt_;
        const int64_t total_leader_cnt = zr_counter->expected_leader_cnt_;
        const int64_t min_l_cnt = total_leader_cnt / available_server_cnt;
        zr_counter->min_exp_leader_cnt_ = min_l_cnt;
        zr_counter->max_exp_leader_cnt_ =
            (min_l_cnt * available_server_cnt == total_leader_cnt) ? (min_l_cnt) : (min_l_cnt + 1);
        zr_counter->max_leader_server_cnt_ = total_leader_cnt - min_l_cnt * available_server_cnt;
        zr_counter->min_leader_server_cnt_ = available_server_cnt - zr_counter->max_leader_server_cnt_;
      }
      if (OB_FAIL(ret)) {
        if (nullptr != server_leader_msg) {
          destroy_and_free_server_leader_msg(server_leader_msg);
          server_leader_msg = nullptr;
        } else if (nullptr != raw_ptr) {
          inner_allocator_.free(raw_ptr);
          raw_ptr = nullptr;
        }
      }
    }
  }
  return ret;
}

int ObLeaderCoordinator::ServerReplicaMsgContainer::check_and_build_available_zones_and_servers(
    const int64_t balance_group_id, const common::ObIArray<common::ObZone>& primary_zone_array,
    const common::ObIArray<share::ObUnit>& tenant_unit, bool& need_balance)
{
  int ret = OB_SUCCESS;
  need_balance = true;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ServerReplicaMsgContainer not init", K(ret));
  } else if (OB_UNLIKELY(primary_zone_array.count() <= 0)) {
    need_balance = false;  // has no available primary zone
  } else if (OB_FAIL(
                 check_and_build_available_zones(balance_group_id, primary_zone_array, tenant_unit, need_balance))) {
    LOG_WARN("fail to check and build available zones", K(ret));
  } else if (!need_balance) {
    // ignore no need to balance
  } else if (OB_FAIL(check_and_build_available_servers(balance_group_id, tenant_unit, need_balance))) {
    LOG_WARN("fail to check and build available servers", K(ret));
  }
  return ret;
}

ObLeaderCoordinator::LcBalanceGroupContainer::LcBalanceGroupContainer(ObLeaderCoordinator& host,
    share::schema::ObSchemaGetterGuard& schema_guard, balancer::ITenantStatFinder& stat_finder,
    common::ObIAllocator& allocator)
    : ObLeaderBalanceGroupContainer(schema_guard, stat_finder, allocator),
      host_(host),
      bg_info_map_(),
      inner_allocator_(),
      inited_(false)
{}

ObLeaderCoordinator::LcBalanceGroupContainer::~LcBalanceGroupContainer()
{
  LcBgInfoMap::iterator iter = bg_info_map_.begin();
  for (; iter != bg_info_map_.end(); ++iter) {
    LcBalanceGroupInfo* bg_info = iter->second;
    if (nullptr != bg_info) {
      bg_info->~LcBalanceGroupInfo();
      inner_allocator_.free(bg_info);
      bg_info = nullptr;
    }
  }
}

int ObLeaderCoordinator::LcBalanceGroupContainer::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice error", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(bg_info_map_.create(BG_INFO_MAP_SIZE, "LcBgContainer"))) {
    LOG_WARN("fail to create bg info map", K(ret));
  } else if (OB_FAIL(pkey_to_tenant_partition_map_.create(BG_INFO_MAP_SIZE, "LcBgContainer"))) {
    LOG_WARN("fail to create map", K(ret));
  } else if (OB_FAIL(inner_allocator_.init(ObMallocAllocator::get_instance(), OB_MALLOC_MIDDLE_BLOCK_SIZE))) {
    LOG_WARN("fail to init inner allocator", K(ret));
  } else if (OB_FAIL(ObLeaderBalanceGroupContainer::init(tenant_id))) {
    LOG_WARN("fail to init base class", K(ret));
  } else {
    inner_allocator_.set_label("LcBgContainer");
    inited_ = true;
  }
  return ret;
}

int ObLeaderCoordinator::LcBalanceGroupContainer::build_base_info()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(ObLeaderBalanceGroupContainer::build())) {
    LOG_WARN("fail to build base info", K(ret));
  } else {
  }  // no others
  return ret;
}

int ObLeaderCoordinator::LcBalanceGroupContainer::collect_balance_group_array_index(
    const common::ObIArray<PartitionArray*>& tenant_partition)
{
  int ret = OB_SUCCESS;
  common::ObPartitionKey pkey;
  for (int64_t i = 0; OB_SUCC(ret) && i < tenant_partition.count(); ++i) {
    PartitionArray* pa_ptr = tenant_partition.at(i);
    if (OB_UNLIKELY(nullptr == pa_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pa_ptr is null", K(ret));
    } else {
      ObAddr curr_leader;
      for (int64_t j = 0; OB_SUCC(ret) && j < pa_ptr->count(); ++j) {
        pkey.reset();
        const Partition& partition = pa_ptr->at(j);
        if (OB_FAIL(partition.get_partition_key(pkey))) {
          LOG_WARN("fail to get partition key", K(ret), K(pkey));
        } else {
          HashIndexMapItem bg_index;
          int tmp_ret = get_hash_index().get_partition_index(pkey, bg_index);
          if (OB_ENTRY_NOT_EXIST == tmp_ret) {
            // not bad, this part in not the anchor
          } else if (OB_SUCCESS != tmp_ret) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("fail to get partition index", K(ret), K(pkey));
          } else if (OB_UNLIKELY(!bg_index.is_valid())) {  // OB_SUCCESS
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("balance group index is not valid", K(ret), K(pkey), K(bg_index));
          } else if (OB_FAIL(collect_balance_group_single_index(bg_index.balance_group_id_, i, bg_index))) {
            LOG_WARN("fail to collect balance group single element", K(ret), K(pkey), K(bg_index));
          } else if (OB_FAIL(partition.get_leader(curr_leader))) {
            LOG_WARN("fail to get leader addr", K(ret));
          } else {
            pa_ptr->set_anchor_pos(j);  // easy to find
            pa_ptr->set_advised_leader(curr_leader);
            break;
          }
        }
      }
    }
  }
  return ret;
}

int ObLeaderCoordinator::LcBalanceGroupContainer::build_balance_groups_leader_info(
    share::schema::ObSchemaGetterGuard& schema_guard, const uint64_t tenant_id,
    const common::ObIArray<PartitionArray*>& tenant_partition, const common::ObIArray<share::ObUnit>& tenant_unit)
{
  int ret = OB_SUCCESS;
  common::ObArray<common::ObZone> excluded_zone_array;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(collect_balance_group_array_index(tenant_partition))) {
    LOG_WARN("fail to collect balance group elements", K(ret));
  } else if (OB_FAIL(host_.get_excluded_zone_array(excluded_zone_array))) {
    LOG_WARN("fail to get merging zone array", K(ret));
  } else {
    for (LcBgInfoMap::iterator iter = bg_info_map_.begin(); OB_SUCC(ret) && iter != bg_info_map_.end(); ++iter) {
      LcBalanceGroupInfo* bg_info = iter->second;
      if (OB_UNLIKELY(nullptr == bg_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("bg info ptr is null", K(ret), KP(bg_info));
      } else if (OB_FAIL(build_single_bg_leader_info(
                     schema_guard, excluded_zone_array, tenant_id, *bg_info, tenant_partition, tenant_unit))) {
        LOG_WARN("fail to calculate single bg rebalance condition", K(ret));
      }
    }
  }
  return ret;
}

int ObLeaderCoordinator::LcBalanceGroupContainer::get_balance_group_valid_primary_zone(
    const common::ObIArray<PartitionArray*>& tenant_partition,
    const common::ObIArray<common::ObZone>& excluded_zone_array, LcBalanceGroupInfo& bg_info,
    common::ObSEArray<common::ObZone, 7>& primary_zone_array, common::ObZone& sample_zone)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawPrimaryZoneUtil::ZoneScore, 7> zone_score_array;
  ObSEArray<ObRawPrimaryZoneUtil::RegionScore, 7> region_score_array;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (bg_info.pg_idx_array_.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("bg info unexpected", K(ret));
  } else {
    const int64_t idx = bg_info.pg_idx_array_.at(0).bg_array_idx_;
    PartitionArray* pa_ptr = nullptr;
    int64_t anchor_pos = -1;
    common::ObPartitionKey pkey;
    if (OB_UNLIKELY(idx >= tenant_partition.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pg idx greater than tenant partition count", K(ret), "array_cnt", tenant_partition.count(), K(idx));
    } else if (OB_UNLIKELY(nullptr == (pa_ptr = tenant_partition.at(idx)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pa_ptr is null", K(ret));
    } else if (FALSE_IT(anchor_pos = pa_ptr->get_anchor_pos())) {
      // shall never be here
    } else if (anchor_pos < 0 || anchor_pos >= pa_ptr->count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("anchor pos unexpected", K(ret));
    } else if (OB_FAIL(pa_ptr->at(anchor_pos).get_partition_key(pkey))) {
      LOG_WARN("fail to get partition key", K(ret));
    } else if (OB_FAIL(host_.build_zone_region_score_array(
                   pa_ptr->get_primary_zone(), zone_score_array, region_score_array))) {
      LOG_WARN("fail to build zone region score array", K(ret));
    } else if (zone_score_array.count() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("zone score array count unexpected", K(ret));
    } else {
      sample_zone = pa_ptr->get_primary_zone();
      ObRawPrimaryZoneUtil::ZoneScore& my_sample = zone_score_array.at(0);
      for (int64_t i = 0; OB_SUCC(ret) && i < zone_score_array.count(); ++i) {
        const ObRawPrimaryZoneUtil::ZoneScore& this_score = zone_score_array.at(i);
        if (my_sample.zone_score_ == this_score.zone_score_) {
          common::ObZone this_zone = this_score.zone_;
          if (has_exist_in_array(excluded_zone_array, this_zone)) {
            // exist in merging zone array, bypass
          } else if (OB_FAIL(primary_zone_array.push_back(this_zone))) {
            LOG_WARN("fail to push back", K(ret));
          }
        } else {
          break;
        }
      }
      if (OB_SUCC(ret)) {
        std::sort(primary_zone_array.begin(), primary_zone_array.end());
      }
    }
  }
  return ret;
}

int ObLeaderCoordinator::LcBalanceGroupContainer::get_pg_locality_and_valid_primary_zone(const PartitionArray& pa,
    share::schema::ObSchemaGetterGuard& schema_guard, const common::ObPartitionKey& pkey,
    const common::ObZone& sample_primary_zone, common::ObIArray<share::ObZoneReplicaAttrSet>& zone_locality,
    bool& need_balance)
{
  int ret = OB_SUCCESS;
  ObSEArray<share::ObZoneReplicaAttrSet, 7> actual_zone_locality;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(pkey));
  } else if (!is_tablegroup_id(pkey.get_table_id())) {
    // normal table
    const ObSimpleTableSchemaV2* table_schema = nullptr;
    if (OB_FAIL(schema_guard.get_table_schema(pkey.get_table_id(), table_schema))) {
      LOG_WARN("fail to get table schema", K(ret), "table_id", pkey.get_table_id());
    } else if (OB_UNLIKELY(nullptr == table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("get invalid table schema", K(ret), "table_id", pkey.get_table_id());
    } else if (OB_FAIL(table_schema->get_zone_replica_attr_array_inherit(schema_guard, actual_zone_locality))) {
      LOG_WARN("fail to get zone locality", K(ret), "table_id", pkey.get_table_id());
    }
  } else {
    const ObTablegroupSchema* tg_schema = nullptr;
    if (OB_FAIL(schema_guard.get_tablegroup_schema(pkey.get_table_id(), tg_schema))) {
      LOG_WARN("fail to get tablegroup schema", K(ret), "tg_id", pkey.get_table_id());
    } else if (OB_UNLIKELY(nullptr == tg_schema)) {
      ret = OB_TABLEGROUP_NOT_EXIST;
      LOG_WARN("get invalid tg schema", K(ret), "tg_id", pkey.get_table_id());
    } else if (OB_FAIL(tg_schema->get_zone_replica_attr_array_inherit(schema_guard, actual_zone_locality))) {
      LOG_WARN("fail to get zone locality", K(ret), "tg_id", pkey.get_table_id());
    }
  }
  if (OB_SUCC(ret)) {
    need_balance = true;
    zone_locality.reset();
    if (OB_FAIL(
            ObLocalityUtil::generate_designated_zone_locality(false /*duplicate table compensate readonly all server*/,
                pa.get_tablegroup_id(),
                pkey,
                get_hash_index(),
                actual_zone_locality,
                zone_locality))) {
      LOG_WARN("fail to get generate designated zone locality", K(ret));
    } else if (sample_primary_zone != pa.get_primary_zone()) {
      need_balance = false;
    }
  }
  return ret;
}

int ObLeaderCoordinator::LcBalanceGroupContainer::build_valid_zone_locality_map(
    const common::ObIArray<common::ObZone>& primary_zone_array,
    const common::ObIArray<share::ObZoneReplicaAttrSet>& zone_locality,
    common::hash::ObHashMap<common::ObZone, const share::ObZoneReplicaAttrSet*>& locality_map)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(locality_map.create(
                 2 * MAX_ZONE_NUM, ObModIds::OB_RS_LEADER_COORDINATOR, ObModIds::OB_RS_LEADER_COORDINATOR))) {
    LOG_WARN("fail to create locality map", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < zone_locality.count(); ++i) {
      const int32_t overwrite = 0;
      const share::ObZoneReplicaAttrSet& replica_set = zone_locality.at(i);
      if (!has_exist_in_array(primary_zone_array, replica_set.zone_)) {
        // bypass, not in primary zone array,
      } else if (replica_set.get_full_replica_num() <= 0) {
        // bypass,
      } else if (OB_FAIL(locality_map.set_refactored(replica_set.zone_, &replica_set, overwrite))) {
        LOG_WARN("fail to set refactored", K(ret), "zone", replica_set.zone_);
      }
    }
  }
  return ret;
}

int ObLeaderCoordinator::LcBalanceGroupContainer::check_need_balance_by_locality_distribution(
    const common::ObIArray<share::ObZoneReplicaAttrSet>& zone_locality,
    const common::ObIArray<common::ObZone>& valid_zone_array, PartitionArray& partition_array,
    ServerReplicaMsgContainer& server_replica_msg_container, bool& need_balance)
{
  int ret = OB_SUCCESS;
  int64_t anchor_pos = -1;
  Partition* partition_ptr = nullptr;
  share::ObPartitionInfo* part_info = nullptr;
  common::ObPartitionKey pkey;
  common::hash::ObHashMap<common::ObZone, const share::ObZoneReplicaAttrSet*> locality_map;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (FALSE_IT(anchor_pos = partition_array.get_anchor_pos())) {
    // shall never be here
  } else if (anchor_pos < 0 || anchor_pos >= partition_array.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("anchor pos unexpected", K(ret), K(anchor_pos), "array_cnt", partition_array.count());
  } else if (OB_UNLIKELY(nullptr == (partition_ptr = &partition_array.at(anchor_pos)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition ptr is nullptr", K(ret), KP(partition_ptr));
  } else if (OB_FAIL(partition_ptr->get_partition_key(pkey))) {
    LOG_WARN("fail to get partition key", K(ret), K(pkey));
  } else if (OB_UNLIKELY(valid_zone_array.count() <= 0)) {
    LOG_INFO("empty available primary zone, ignore leader balance", K(pkey));
    need_balance = false;
  } else if (OB_UNLIKELY(nullptr == (part_info = const_cast<share::ObPartitionInfo*>(partition_ptr->info_)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("part info ptr is null", K(ret), KP(part_info));
  } else if (OB_FAIL(build_valid_zone_locality_map(valid_zone_array, zone_locality, locality_map))) {
    LOG_WARN("fail to build valid zone locality map", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < part_info->get_replicas_v2().count(); ++i) {
      const ObPartitionReplica& replica = part_info->get_replicas_v2().at(i);
      if (!replica.in_member_list_) {
        // bypass, replica not in member list, ignore
      } else if (OB_FAIL(server_replica_msg_container.collect_replica(partition_ptr, replica))) {
        LOG_WARN("fail to collect replica", K(ret), K(replica));
      } else if (common::REPLICA_TYPE_FULL != replica.replica_type_) {
        // bypass
      } else if (nullptr == (server_replica_msg_container.get_server_replica_counter_map().get(replica.server_))) {
        // bypass
      } else {
        (void)locality_map.erase_refactored(replica.zone_);  // ignore ret
      }
    }
    if (OB_SUCC(ret)) {  // Ensure that the full replica in valid_zone_array is complete
      need_balance = true;
      if (locality_map.size() > 0) {
        need_balance = false;
        LOG_INFO("partition replica not enough, postpone leader balance", K(pkey), K(valid_zone_array));
      }
    }
  }
  return ret;
}

int ObLeaderCoordinator::LcBalanceGroupContainer::check_need_balance_by_leader_cnt(
    ServerReplicaMsgContainer& server_replica_msg_container, bool& need_balance)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(server_replica_msg_container.check_need_balance_by_leader_cnt(need_balance))) {
    LOG_WARN("fail to check need balance by leader cnt", K(ret));
  }
  return ret;
}

int ObLeaderCoordinator::LcBalanceGroupContainer::generate_leader_balance_plan(
    ServerReplicaMsgContainer& server_replica_msg_container, LcBalanceGroupInfo& bg_info,
    const common::ObIArray<PartitionArray*>& tenant_partition)
{
  int ret = OB_SUCCESS;
  UNUSED(bg_info);
  ObAllPartitionSwitchMsg switch_plan_map;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(switch_plan_map.create(
                 BG_INFO_MAP_SIZE, ObModIds::OB_RS_LEADER_COORDINATOR, ObModIds::OB_RS_LEADER_COORDINATOR))) {
    LOG_WARN("fail to create switch plan map", K(ret));
  } else if (OB_FAIL(do_generate_leader_balance_plan(server_replica_msg_container.get_server_leader_info(),
                 switch_plan_map,
                 server_replica_msg_container.get_server_replica_counter_map(),
                 server_replica_msg_container.get_zone_replica_counter_map()))) {
    LOG_WARN("fail to do generate leader balance plan", K(ret));
  } else {
    // generate plan
    for (ObAllPartitionSwitchMsg::iterator iter = switch_plan_map.begin();
         OB_SUCC(ret) && iter != switch_plan_map.end();
         ++iter) {
      common::ObPartitionKey& pkey = iter->first;
      ObPartitionSwitchMsg* switch_msg = iter->second;
      int64_t index = -1;
      if (OB_FAIL(pkey_to_tenant_partition_map_.get_refactored(pkey, index))) {
        LOG_WARN("fail to get refactored", K(ret), K(pkey));
      } else if (index < 0 || index >= tenant_partition.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get index", K(ret));
      } else if (OB_UNLIKELY(nullptr == switch_msg)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("switch msg ptr is null", K(ret));
      } else {
        tenant_partition.at(index)->set_advised_leader(switch_msg->final_leader_addr_);
      }
    }
  }
  // either success or failure are free memory
  for (ObAllPartitionSwitchMsg::iterator iter = switch_plan_map.begin(); iter != switch_plan_map.end(); ++iter) {
    ObPartitionSwitchMsg* switch_msg = iter->second;
    if (nullptr != switch_msg) {
      switch_msg->~ObPartitionSwitchMsg();
      inner_allocator_.free(switch_msg);
      switch_msg = nullptr;
    }
  }
  return ret;
}

int ObLeaderCoordinator::LcBalanceGroupContainer::do_generate_leader_balance_plan(
    ObAllServerLeaderMsg& server_leader_info, ObAllPartitionSwitchMsg& switch_plan_map,
    ServerReplicaMsgContainer::ServerReplicaCounterMap& server_replica_counter_map,
    ServerReplicaMsgContainer::ZoneReplicaCounterMap& zone_replica_counter_map)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    NewLeaderStrategy new_leader_strategy(
        inner_allocator_, server_leader_info, switch_plan_map, server_replica_counter_map, zone_replica_counter_map);
    if (OB_FAIL(new_leader_strategy.init())) {
      LOG_WARN("fail to init new leader strategy", K(ret));
    } else if (OB_FAIL(new_leader_strategy.execute())) {
      LOG_WARN("fail to do new leader strategy", K(ret));
    }
  }
  return ret;
}

int ObLeaderCoordinator::LcBalanceGroupContainer::build_single_bg_leader_info(
    share::schema::ObSchemaGetterGuard& schema_guard, const common::ObIArray<common::ObZone>& excluded_zone_array,
    const uint64_t tenant_id, LcBalanceGroupInfo& bg_info, const common::ObIArray<PartitionArray*>& tenant_partition,
    const common::ObIArray<share::ObUnit>& tenant_unit)
{
  int ret = OB_SUCCESS;
  ServerReplicaMsgContainer server_replica_msg_container(inner_allocator_, host_);
  common::ObSEArray<common::ObZone, 7> primary_zone_array;
  common::ObZone sample_primary_zone;
  bool need_balance = true;
  bool is_shrinking = false;
  int tmp_ret = OB_SUCCESS;
  UNUSED(tenant_id);
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(server_replica_msg_container.init(bg_info.pg_cnt_))) {
    LOG_WARN("fail to server replica msg container", K(ret));
  } else if (OB_FAIL(get_balance_group_valid_primary_zone(
                 tenant_partition, excluded_zone_array, bg_info, primary_zone_array, sample_primary_zone))) {
    LOG_WARN("fail to get balance group valid primary_zone", K(ret));
  } else if (OB_FAIL(host_.unit_mgr_->check_tenant_pools_in_shrinking(tenant_id, is_shrinking))) {
    LOG_WARN("fail to check tenant pools in shrinking", K(ret), K(tenant_id));
  } else if (is_shrinking) {
    // Don't do leader coordinate during the shrinking process
  } else if (OB_FAIL(server_replica_msg_container.check_and_build_available_zones_and_servers(
                 bg_info.balance_group_id_, primary_zone_array, tenant_unit, need_balance))) {
    LOG_WARN("fail to construct available servers", K(ret));
  } else {
    // Calculate whether the partition of the index already exists on the corresponding locality
    common::ObPartitionKey pkey;
    common::ObSEArray<share::ObZoneReplicaAttrSet, 7> zone_locality;
    // If the primary zone array is not the same between the balance groups,
    // the leader balance will not be performed
    pkey_to_tenant_partition_map_.reuse();
    for (int64_t i = 0; need_balance && OB_SUCC(ret) && i < bg_info.pg_idx_array_.count(); ++i) {
      const int64_t idx = bg_info.pg_idx_array_.at(i).bg_array_idx_;
      PartitionArray* pa_ptr = nullptr;
      int64_t anchor_pos = -1;
      const int32_t overwrite = 0;
      pkey.reset();
      zone_locality.reset();
      if (OB_UNLIKELY(idx >= tenant_partition.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pg idx greater than tenant partition count", K(ret), "array_cnt", tenant_partition.count(), K(idx));
      } else if (OB_UNLIKELY(nullptr == (pa_ptr = tenant_partition.at(idx)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pa_ptr is null", K(ret));
      } else if (FALSE_IT(anchor_pos = pa_ptr->get_anchor_pos())) {
        // shall never be here
      } else if (anchor_pos < 0 || anchor_pos >= pa_ptr->count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("anchor pos unexpected", K(ret));
      } else if (OB_FAIL(pa_ptr->at(anchor_pos).get_partition_key(pkey))) {
        LOG_WARN("fail to get partition key", K(ret));
      } else if (OB_FAIL(get_pg_locality_and_valid_primary_zone(
                     *pa_ptr, schema_guard, pkey, sample_primary_zone, zone_locality, need_balance))) {
        LOG_WARN("fail to get pg locality", K(ret));
      } else if (OB_FAIL(check_need_balance_by_locality_distribution(
                     zone_locality, primary_zone_array, *pa_ptr, server_replica_msg_container, need_balance))) {
        LOG_WARN("fail to check need balance by locality", K(ret));
      } else if (OB_FAIL(pkey_to_tenant_partition_map_.set_refactored(pkey, idx, overwrite))) {
        LOG_WARN("fail to set refactored", K(ret), K(pkey));
      }
    }
    if (OB_FAIL(ret)) {
      // failed,
    } else if (!need_balance) {
      // no need to balance
    } else if (OB_FAIL(check_need_balance_by_leader_cnt(server_replica_msg_container, need_balance))) {
      LOG_WARN("fail to check need balance by leader cnt", K(ret));
    } else if (!need_balance) {
      // no need to balance
    } else if (OB_SUCCESS !=
               (tmp_ret = generate_leader_balance_plan(server_replica_msg_container, bg_info, tenant_partition))) {
      // ignor the ret.
      // Ensure that the leader balance between each balanced group is not affected
      LOG_WARN("fail to generate leader balance plan",
          KR(tmp_ret),
          K(bg_info),
          K(bg_info.pg_idx_array_.count()),
          K(tenant_id),
          K(pkey),
          K(zone_locality));
    }
  }
  return ret;
}

int ObLeaderCoordinator::LcBalanceGroupContainer::collect_balance_group_single_index(
    const int64_t balance_group_id, const int64_t bg_array_idx, const HashIndexMapItem& balance_group_item)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (balance_group_id < 0 || bg_array_idx < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(balance_group_id), K(bg_array_idx));
  } else {
    void* ptr = nullptr;
    LcBalanceGroupInfo* bg_info = nullptr;
    int tmp_ret = bg_info_map_.get_refactored(balance_group_id, bg_info);
    if (OB_HASH_NOT_EXIST == tmp_ret) {
      if (nullptr == (ptr = inner_allocator_.alloc(sizeof(LcBalanceGroupInfo)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory", K(ret));
      } else if (nullptr == (bg_info = new (ptr) LcBalanceGroupInfo(inner_allocator_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to construct LcBalanceGroupInfo", K(ret));
      } else {
        bg_info->pg_cnt_ = balance_group_item.group_count_;
        bg_info->balance_group_id_ = balance_group_id;
        const int32_t overwrite = 0;
        if (OB_FAIL(bg_info->pg_idx_array_.init(bg_info->pg_cnt_))) {
          LOG_WARN("fail to init pg idx array", K(ret), "pg_cnt", bg_info->pg_cnt_);
        } else if (bg_info->pg_idx_array_.count() >= bg_info->pg_cnt_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("pg idx array count unexpected",
              K(ret),
              K(balance_group_id),
              "pg_idx_array_count",
              bg_info->pg_idx_array_.count(),
              "pg_cnt",
              bg_info->pg_cnt_);
        } else if (OB_FAIL(
                       bg_info->pg_idx_array_.push_back(LcBgPair(bg_array_idx, balance_group_item.in_group_index_)))) {
          LOG_WARN("fail to push back", K(ret));
        } else if (OB_FAIL(bg_info_map_.set_refactored(balance_group_id, bg_info, overwrite))) {
          LOG_WARN("fail to set refacotred", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
        if (nullptr != bg_info) {
          bg_info->~LcBalanceGroupInfo();
          inner_allocator_.free(bg_info);
          bg_info = nullptr;
        } else if (nullptr != ptr) {
          inner_allocator_.free(ptr);
          ptr = nullptr;
        }
      }
    } else if (OB_SUCCESS == tmp_ret) {
      if (OB_UNLIKELY(nullptr == bg_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("bg info ptr is null", K(ret));
      } else if (bg_info->pg_idx_array_.count() >= bg_info->pg_cnt_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pg idx array count unexpected",
            K(ret),
            K(balance_group_id),
            "pg_idx_array_count",
            bg_info->pg_idx_array_.count(),
            "pg_cnt",
            bg_info->pg_cnt_);
      } else if (OB_FAIL(
                     bg_info->pg_idx_array_.push_back(LcBgPair(bg_array_idx, balance_group_item.in_group_index_)))) {
        LOG_WARN("fail to push back", K(ret));
      }
    } else {
      ret = tmp_ret;
      LOG_WARN("fail to get bg info map", K(ret), K(balance_group_id));
    }
  }
  return ret;
}

int ObLeaderCoordinator::Partition::assign(const ObLeaderCoordinator::Partition& other)
{
  int ret = OB_SUCCESS;
  table_id_ = other.table_id_;
  tg_id_ = other.tg_id_;
  info_ = other.info_;
  if (OB_FAIL(copy_assign(candidates_, other.candidates_))) {
    LOG_WARN("failed to assign candidates", K(ret));
  } else if (OB_FAIL(copy_assign(prep_candidates_, other.prep_candidates_))) {
    LOG_WARN("failed to assign prep candidates", K(ret));
  } else if (OB_FAIL(copy_assign(candidate_status_array_, other.candidate_status_array_))) {
    LOG_WARN("fail to assign candidate status list", K(ret));
  }
  return ret;
}

void ObLeaderCoordinator::Partition::reuse()
{
  table_id_ = 0;
  tg_id_ = 0;
  info_ = nullptr;
  candidates_.reuse();
  prep_candidates_.reuse();
  candidate_status_array_.reuse();
}

int ObLeaderCoordinator::Partition::check_replica_can_be_elected(const ObAddr& server, bool& can_be_elected) const
{
  int ret = OB_SUCCESS;
  can_be_elected = true;
  // treat replica not exist not normal replica
  const ObPartitionReplica* replica = NULL;
  if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), K(ret));
  } else if (OB_UNLIKELY(nullptr == info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("info_ ptr is null", K(ret));
  } else if (OB_FAIL(info_->find(server, replica))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      can_be_elected = false;
    } else {
      LOG_WARN("find server replica failed", K(ret));
    }
  } else if (NULL == replica) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL replica", K(ret));
  } else {
    if (REPLICA_STATUS_NORMAL != replica->replica_status_) {
      can_be_elected = false;
    }
    if (!ObReplicaTypeCheck::is_can_elected_replica(replica->replica_type_)) {
      can_be_elected = false;
    }
  }
  return ret;
}

int ObLeaderCoordinator::Partition::get_leader(ObAddr& leader) const
{
  int ret = OB_SUCCESS;
  leader.reset();
  const ObPartitionReplica* leader_replica = NULL;
  if (OB_UNLIKELY(nullptr == info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("info_ ptr is null", K(ret));
  } else if (OB_FAIL(info_->find_leader_by_election(leader_replica))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_LEADER_NOT_EXIST;
      LOG_WARN("leader replica not exist", K(ret));
    } else {
      LOG_WARN("find leader replica failed", K(ret));
    }
  } else if (NULL == leader_replica) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL replica", K(ret));
  } else {
    leader = leader_replica->server_;
  }
  return ret;
}

int ObLeaderCoordinator::Partition::get_partition_key(ObPartitionKey& pkey) const
{
  int ret = OB_SUCCESS;
  pkey.reset();
  if (OB_UNLIKELY(nullptr == info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("info_ ptr is null", K(ret));
  } else if (info_->get_replicas_v2().count() <= 0) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("replicas is empty", "partition_info", *info_, K(ret));
  } else {
    const ObPartitionReplica& replica = info_->get_replicas_v2().at(0);
    pkey = ObPartitionKey(replica.table_id_, replica.partition_id_, replica.partition_cnt_);
  }
  return ret;
}

int ObLeaderCoordinator::GetLeaderCandidatesAsyncV2Operator::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(index_map_.create(2 * MAX_CANDIDATE_INFO_ARRAY_SIZE,
                 ObModIds::OB_RS_LEADER_COORDINATOR,
                 ObModIds::OB_RS_LEADER_COORDINATOR))) {
    LOG_WARN("fail to init index map", K(ret));
  } else if (OB_FAIL(candidate_info_array_.init(MAX_CANDIDATE_INFO_ARRAY_SIZE))) {
    LOG_WARN("init candidate info array", K(ret));
  } else {
    inited_ = true;
  }
  return ret;
}

void ObLeaderCoordinator::GetLeaderCandidatesAsyncV2Operator::clear_after_wait()
{
  async_proxy_.reuse();
  part_index_matrix_.reuse();
  send_requests_cnt_ = 0;
}

bool ObLeaderCoordinator::GetLeaderCandidatesAsyncV2Operator::reach_send_to_wait_threshold() const
{
  return send_requests_cnt_ >= SEND_TO_WAIT_THRESHOLD;
}

bool ObLeaderCoordinator::GetLeaderCandidatesAsyncV2Operator::reach_accumulate_threshold(
    const PartIndexInfo& info) const
{
  return info.part_index_array_.count() >= ACCUMULATE_THRESHOLD;
}

int ObLeaderCoordinator::GetLeaderCandidatesAsyncV2Operator::accumulate_request(
    common::ObArray<common::ObAddr>& prep_candidates, const common::ObAddr& dest_server, PartIndexInfo& index_info,
    const common::ObPartitionKey& pkey, const int64_t first_level_idx, const int64_t second_level_idx)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!pkey.is_valid()) || first_level_idx < 0 || second_level_idx < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(pkey), K(first_level_idx), K(second_level_idx));
  } else if (index_info.part_index_array_.count() != index_info.arg_.partitions_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("part_idx cnt is not equal to partition cnt",
        K(ret),
        "part_idx cnt",
        index_info.part_index_array_.count(),
        "partition cnt",
        index_info.arg_.partitions_.count());
  } else if (OB_FAIL(index_info.part_index_array_.push_back(PartIndex(first_level_idx, second_level_idx)))) {
    LOG_WARN("fail to push partition index", K(ret), K(first_level_idx), K(second_level_idx));
  } else if (OB_FAIL(index_info.arg_.partitions_.push_back(pkey))) {
    // if this fails, this process stream fails,
    // no need to pop the part_idx above
    LOG_WARN("fail to push partition", K(ret), K(first_level_idx), K(second_level_idx));
  } else if (index_info.arg_.prep_candidates_.count() <= 0) {
    if (OB_FAIL(index_info.arg_.prep_candidates_.assign(prep_candidates))) {
      LOG_WARN("fail to assign", K(ret));
    } else {
      index_info.dest_server_ = dest_server;
    }
  } else {
  }  // arg prep candidates already assign, ignore

  return ret;
}

int ObLeaderCoordinator::GetLeaderCandidatesAsyncV2Operator::process_request(const common::ObPartitionKey& pkey,
    const common::ObAddr& dest_server, common::ObArray<common::ObAddr>& prep_candidates, const int64_t first_part_idx,
    const int64_t second_part_idx, common::ObIArray<PartitionArray*>& partition_arrays)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!pkey.is_valid() || !dest_server.is_valid() || first_part_idx < 0 || second_part_idx < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(pkey), K(dest_server), K(first_part_idx), K(second_part_idx));
  } else if (OB_UNLIKELY(prep_candidates.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument to prep candidates", K(ret));
  } else {
    int64_t ci_array_idx = -1;
    int tmp_ret = index_map_.get_refactored(dest_server, ci_array_idx);
    if (OB_SUCCESS == tmp_ret) {
      if (OB_FAIL(process_request_server_exist(
              ci_array_idx, pkey, dest_server, prep_candidates, first_part_idx, second_part_idx, partition_arrays))) {
        LOG_WARN("fail to process requests server exist", K(ret), K(pkey), K(dest_server));
      }
    } else if (OB_HASH_NOT_EXIST == tmp_ret) {
      if (OB_FAIL(process_request_server_not_exist(
              pkey, dest_server, prep_candidates, first_part_idx, second_part_idx, partition_arrays))) {
        LOG_WARN("fail to process requests server not exist", K(ret), K(pkey), K(dest_server));
      }
    } else {
      ret = tmp_ret;
      LOG_WARN("fail to get from index map", K(ret), K(dest_server));
    }
  }
  return ret;
}

int ObLeaderCoordinator::GetLeaderCandidatesAsyncV2Operator::process_request_server_exist(const int64_t ci_array_idx,
    const common::ObPartitionKey& pkey, const common::ObAddr& dest_server,
    common::ObArray<common::ObAddr>& prep_candidates, const int64_t first_part_idx, const int64_t second_part_idx,
    common::ObIArray<PartitionArray*>& partition_arrays)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (ci_array_idx >= candidate_info_array_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ci array idx unexpected", K(ret), K(ci_array_idx));
  } else if (OB_UNLIKELY(!pkey.is_valid() || !dest_server.is_valid() || first_part_idx < 0 || second_part_idx < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(pkey), K(dest_server), K(first_part_idx), K(second_part_idx));
  } else if (OB_UNLIKELY(prep_candidates.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument to prep candidates", K(ret));
  } else {
    PartIndexInfo& this_info = candidate_info_array_.at(ci_array_idx);
    if ((!ObLeaderCoordinator::prep_candidates_match(prep_candidates, this_info.arg_.prep_candidates_) &&
            this_info.arg_.prep_candidates_.count() > 0) ||
        reach_accumulate_threshold(this_info)) {
      if (OB_FAIL(send_requests(this_info, dest_server, partition_arrays))) {
        LOG_WARN("fail to send requests", K(ret), K(dest_server));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(accumulate_request(prep_candidates, dest_server, this_info, pkey, first_part_idx, second_part_idx))) {
        LOG_WARN("fail to accumulate request", K(ret));
      }
    }
  }
  return ret;
}

int ObLeaderCoordinator::GetLeaderCandidatesAsyncV2Operator::process_request_server_not_exist(
    const common::ObPartitionKey& pkey, const common::ObAddr& dest_server,
    common::ObArray<common::ObAddr>& prep_candidates, const int64_t first_part_idx, const int64_t second_part_idx,
    common::ObIArray<PartitionArray*>& partition_arrays)
{
  int ret = OB_SUCCESS;
  const int32_t overwrite_flag = 0;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!pkey.is_valid() || !dest_server.is_valid() || first_part_idx < 0 || second_part_idx < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(dest_server), K(first_part_idx), K(second_part_idx));
  } else if (candidate_info_array_.count() < MAX_CANDIDATE_INFO_ARRAY_SIZE) {
    PartIndexInfo tmp_info;
    const int64_t idx = candidate_info_array_.count();
    if (OB_FAIL(candidate_info_array_.push_back(tmp_info))) {
      LOG_WARN("fail to push back");
    } else if (OB_FAIL(index_map_.set_refactored(dest_server, idx, overwrite_flag))) {
      LOG_WARN("fail to set refactored", K(ret));
    } else if (OB_FAIL(process_request_server_exist(
                   idx, pkey, dest_server, prep_candidates, first_part_idx, second_part_idx, partition_arrays))) {
      LOG_WARN("fail to process request server not exist", K(ret));
    }
  } else {
    int64_t idx = static_cast<int64_t>(pkey.hash());
    // Pick one at random
    idx = (idx < 0 ? -idx : idx);
    idx = idx % candidate_info_array_.count();
    const common::ObAddr prev_dest = candidate_info_array_.at(idx).dest_server_;
    if (OB_FAIL(index_map_.erase_refactored(prev_dest))) {
      LOG_WARN("fail to erase refactored", K(ret));
    } else if (OB_FAIL(index_map_.set_refactored(dest_server, idx, overwrite_flag))) {
      LOG_WARN("fail to set refactored", K(ret));
    } else if (OB_FAIL(send_requests(candidate_info_array_.at(idx), prev_dest, partition_arrays))) {
      LOG_WARN("fail to send request", K(ret));
    } else if (OB_FAIL(accumulate_request(prep_candidates,
                   dest_server,
                   candidate_info_array_.at(idx),
                   pkey,
                   first_part_idx,
                   second_part_idx))) {
      LOG_WARN("fail to accumulate request", K(ret));
    }
  }
  return ret;
}

int ObLeaderCoordinator::GetLeaderCandidatesAsyncV2Operator::finally_send_requests(
    common::ObIArray<PartitionArray*>& partition_arrays)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < candidate_info_array_.count(); ++i) {
      PartIndexInfo& index_info = candidate_info_array_.at(i);
      const common::ObAddr& dest_server = index_info.dest_server_;
      if (!dest_server.is_valid()) {
        // bypass, since this already processed before
      } else if (OB_FAIL(send_requests(index_info, dest_server, partition_arrays))) {
        LOG_WARN("fail to send requests", K(ret));
      } else if (reach_send_to_wait_threshold()) {
        if (OB_FAIL(wait())) {
          LOG_WARN("fail to wait proxy", K(ret));
        } else if (OB_FAIL(fill_partitions_leader_candidates(partition_arrays))) {
          LOG_WARN("fail to fill partitions leader candidates", K(ret));
        } else {
          clear_after_wait();
        }
      } else {
      }  // go on
    }
  }
  return ret;
}

int ObLeaderCoordinator::GetLeaderCandidatesAsyncV2Operator::send_requests(
    PartIndexInfo& this_info, const common::ObAddr& dest_server, common::ObIArray<PartitionArray*>& partition_arrays)
{
  int ret = OB_SUCCESS;
  int64_t rpc_timeout = GCONF.get_leader_candidate_rpc_timeout;
  if (rpc_timeout > MAX_RPC_TIMEOUT || rpc_timeout < MIN_RPC_TIMEOUT) {
    rpc_timeout = RPC_TIMEOUT;
  } else {
  }  // use this configuration

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!dest_server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(dest_server));
  } else if (this_info.part_index_array_.count() != this_info.arg_.partitions_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("part index cnt is not equal to partition cnt",
        K(ret),
        "part_idx cnt",
        this_info.part_index_array_.count(),
        "partition cnt",
        this_info.arg_.partitions_.count());
  } else if (dest_server != this_info.dest_server_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dest server not match", K(ret), "left_server", dest_server, "right_server", this_info.dest_server_);
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < this_info.part_index_array_.count(); ++i) {
      const int64_t first_level_idx = this_info.part_index_array_.at(i).first_level_idx_;
      const int64_t second_level_idx = this_info.part_index_array_.at(i).second_level_idx_;
      PartitionArray* partition_array = nullptr;
      if (first_level_idx >= partition_arrays.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("part index is illegal", K(ret), K(first_level_idx), "partitions cnt", partition_arrays.count());
      } else if (nullptr == (partition_array = partition_arrays.at(first_level_idx))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("partition array ptr is null", K(ret));
      } else if (second_level_idx >= partition_array->count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("part index is illegal", K(ret), K(second_level_idx), "partitions count", partition_array->count());
      } else if (OB_FAIL(
                     partition_array->at(second_level_idx).prep_candidates_.assign(this_info.arg_.prep_candidates_))) {
        LOG_WARN("fail to assign candidates", K(ret), K(i), K(first_level_idx), K(second_level_idx));
      } else {
      }  // do nothing
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(part_index_matrix_.push_back(this_info.part_index_array_))) {
      LOG_WARN("fail to push to part_index_matrix", K(ret));
    } else if (OB_FAIL(async_proxy_.call(dest_server, rpc_timeout, this_info.arg_))) {
      LOG_WARN("fail to send rpc request", K(ret), K(dest_server), K(this_info));
    } else {
      send_requests_cnt_++;
      // clear this info requests are send
      this_info.reuse();
    }
  }
  return ret;
}

int ObLeaderCoordinator::GetLeaderCandidatesAsyncV2Operator::wait()
{
  return async_proxy_.wait();
}

int ObLeaderCoordinator::GetLeaderCandidatesAsyncV2Operator::fill_partitions_leader_candidates(
    common::ObIArray<PartitionArray*>& partition_arrays)
{
  int ret = OB_SUCCESS;
  if (async_proxy_.get_results().count() != part_index_matrix_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("results cnt isn't equal to part_index_matrix cnt",
        K(ret),
        "result cnt",
        async_proxy_.get_results().count(),
        "part_index_matrix cnt",
        part_index_matrix_.count());
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < part_index_matrix_.count(); ++i) {
      const PartIndexArray& part_index_array = part_index_matrix_.at(i);
      const obrpc::ObGetLeaderCandidatesResult* result = async_proxy_.get_results().at(i);
      if (OB_UNLIKELY(NULL == result)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("leader candidates result is NULL", K(ret), KP(result));
      } else if (part_index_array.count() != result->candidates_.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("part index cnt is not equal to candidates cnt",
            K(ret),
            "part index cnt",
            part_index_array.count(),
            "candidates cnt",
            result->candidates_.count());
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < part_index_array.count(); ++j) {
          const int64_t first_level_idx = part_index_array.at(j).first_level_idx_;
          const int64_t second_level_idx = part_index_array.at(j).second_level_idx_;
          PartitionArray* partition_array = nullptr;
          if (first_level_idx >= partition_arrays.count()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("part index is illegal", K(ret), K(first_level_idx), "partitions cnt", partition_arrays.count());
          } else if (nullptr == (partition_array = partition_arrays.at(first_level_idx))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("partition array ptr is null", K(ret));
          } else if (second_level_idx >= partition_array->count()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN(
                "part index is illegal", K(ret), K(second_level_idx), "partitions count", partition_array->count());
          } else if (OB_FAIL(partition_array->at(second_level_idx).candidates_.assign(result->candidates_.at(j)))) {
            LOG_WARN("fail to assign candidates",
                K(ret),
                K(i),
                K(j),
                K(first_level_idx),
                K(second_level_idx),
                K(part_index_array));
          } else if (result->candidate_status_array_.count() <= 0) {
            // Compatible processing, compatible with the version without candidate status array
          } else if (OB_FAIL(partition_array->at(second_level_idx)
                                 .candidate_status_array_.assign(result->candidate_status_array_.at(j)))) {
            LOG_WARN("fail to assign candidate status list",
                K(ret),
                K(i),
                K(j),
                K(first_level_idx),
                K(second_level_idx),
                K(part_index_array));
          } else {
          }  // do nothing
        }
      }
    }
  }
  return ret;
}

void ObLeaderCoordinator::SwitchLeaderListAsyncOperator::reuse()
{
  async_proxy_.reuse();
  arg_.reset();
  send_requests_cnt_ = 0;
  saved_cur_leader_.reset();
  saved_new_leader_.reset();
}

bool ObLeaderCoordinator::SwitchLeaderListAsyncOperator::reach_accumulate_threshold() const
{
  return arg_.partition_key_list_.count() >= ACCUMULATE_THRESHOLD;
}

bool ObLeaderCoordinator::SwitchLeaderListAsyncOperator::reach_send_to_wait_threshold() const
{
  return send_requests_cnt_ >= SEND_TO_WAIT_THRESHOLD;
}

int ObLeaderCoordinator::SwitchLeaderListAsyncOperator::accumulate_request(
    const common::ObPartitionKey& pkey, const bool is_sys_tg_partition)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(pkey));
  } else if (OB_FAIL(arg_.partition_key_list_.push_back(pkey))) {
    LOG_WARN("fail to push partition key", K(ret), K(pkey));
  } else if (OB_FAIL(is_sys_tg_partitions_.push_back(is_sys_tg_partition))) {
    LOG_WARN("fail to push back", K(ret));
  } else {
  }  // do nothing
  return ret;
}

int ObLeaderCoordinator::SwitchLeaderListAsyncOperator::send_requests(
    common::ObAddr& dest_server, common::ObAddr& dest_leader, ExpectedLeaderWaitOperator& leader_wait_operator)
{
  int ret = OB_SUCCESS;
  ObLeaderElectionWaiter::ExpectedLeader expected_leader;

  if (!dest_server.is_valid() || !dest_leader.is_valid()) {
    if (arg_.partition_key_list_.count() <= 0) {  // this is good
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(dest_server), K(dest_leader));
    }
  } else {
    arg_.leader_addr_ = dest_leader;
    if (arg_.partition_key_list_.count() != is_sys_tg_partitions_.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("array count not match",
          K(ret),
          "left_count",
          arg_.partition_key_list_.count(),
          "right_count",
          is_sys_tg_partitions_.count());
    } else if (OB_FAIL(async_proxy_.call(dest_server, RPC_TIMEOUT, arg_))) {
      LOG_WARN("fail to send rpc request", K(ret), K(dest_server), K(arg_));
    } else {
      ObPartitionKey& part_key = arg_.partition_key_list_.at(0);
      LOG_INFO("switch leader list",
          K(dest_server),
          K(dest_leader),
          "tenant_id",
          part_key.get_tenant_id(),
          "part_id",
          part_key.get_partition_id(),
          "count",
          arg_.partition_key_list_.count());
      for (int64_t i = 0; OB_SUCC(ret) && i < arg_.partition_key_list_.count(); ++i) {
        expected_leader.reset();
        expected_leader.partition_ = arg_.partition_key_list_.at(i);
        expected_leader.exp_leader_ = arg_.leader_addr_;
        expected_leader.old_leader_ = dest_server;
        if (OB_FAIL(leader_wait_operator.append_expected_leader(expected_leader, is_sys_tg_partitions_.at(i)))) {
          LOG_WARN("append expected leader failed", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        send_requests_cnt_++;
        // clear arg_ after requests are send
        arg_.reset();
        is_sys_tg_partitions_.reset();
      }
    }
  }
  return ret;
}

int ObLeaderCoordinator::SwitchLeaderListAsyncOperator::wait()
{
  return async_proxy_.wait();
}

int ObLeaderCoordinator::ExpectedLeaderWaitOperator::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(new_leader_counter_.create(MAX_SERVER_CNT, ObModIds::OB_RS_LEADER_COORDINATOR))) {
    LOG_WARN("fail to create leader wait operator new leader counter", K(ret));
  } else if (OB_FAIL(old_leader_counter_.create(MAX_SERVER_CNT, ObModIds::OB_RS_LEADER_COORDINATOR))) {
    LOG_WARN("fail to create leader wait operator old leader counter", K(ret));
  } else {
    tenant_id_ = tenant_id;
    inited_ = true;
  }
  return ret;
}

int ObLeaderCoordinator::ExpectedLeaderWaitOperator::check_can_switch_more_leader(
    const common::ObAddr& new_leader, const common::ObAddr& old_leader, bool& can_switch)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!new_leader.is_valid() || !old_leader.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(new_leader), K(old_leader));
  } else {
    int64_t count = 0;
    int tmp_ret = new_leader_counter_.get_refactored(new_leader, count);
    if (OB_HASH_NOT_EXIST == tmp_ret) {
      can_switch = true;
    } else if (OB_SUCCESS == tmp_ret) {
      if (count < GCONF.wait_leader_batch_count) {
        can_switch = true;
      } else {
        can_switch = false;
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
    }
    if (OB_FAIL(ret)) {
    } else if (!can_switch) {
    } else {
      tmp_ret = old_leader_counter_.get_refactored(old_leader, count);
      if (OB_HASH_NOT_EXIST == tmp_ret) {
        can_switch = true;
      } else if (OB_SUCCESS == tmp_ret) {
        if (count < GCONF.wait_leader_batch_count) {
          can_switch = true;
        } else {
          can_switch = false;
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
      }
    }
  }
  return ret;
}

int ObLeaderCoordinator::ExpectedLeaderWaitOperator::check_can_switch_more_new_leader(
    const common::ObAddr& new_leader, bool& can_switch)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!new_leader.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(new_leader));
  } else {
    int64_t count = 0;
    int tmp_ret = new_leader_counter_.get_refactored(new_leader, count);
    if (OB_HASH_NOT_EXIST == tmp_ret) {
      can_switch = true;
    } else if (OB_SUCCESS == tmp_ret) {
      if (count < GCONF.wait_leader_batch_count) {
        can_switch = true;
      } else {
        can_switch = false;
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
    }
  }
  return ret;
}

int ObLeaderCoordinator::ExpectedLeaderWaitOperator::check_can_switch_more_old_leader(
    const common::ObAddr& old_leader, bool& can_switch)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!old_leader.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(old_leader));
  } else {
    int64_t count = 0;
    int tmp_ret = old_leader_counter_.get_refactored(old_leader, count);
    if (OB_HASH_NOT_EXIST == tmp_ret) {
      can_switch = true;
    } else if (OB_SUCCESS == tmp_ret) {
      if (count < GCONF.wait_leader_batch_count) {
        can_switch = true;
      } else {
        can_switch = false;
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
    }
  }
  return ret;
}

int ObLeaderCoordinator::ExpectedLeaderWaitOperator::do_wait()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    host_.update_last_run_timestamp();  // save current time for thread checker.
    if (sys_tg_expected_leaders_.count() > 0) {
      const bool is_sys = true;
      const int64_t timeout = generate_wait_leader_timeout(is_sys);
      if (timeout <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("timeout shouldn't be less than or equal to 0", K(ret), K(timeout));
      } else if (OB_FAIL(sys_leader_waiter_.wait(sys_tg_expected_leaders_, timeout))) {
        LOG_WARN("wait_elect_leaders failed", K(ret), K(timeout));
      }
      if (OB_SUCCESS == ret || OB_WAIT_ELEC_LEADER_TIMEOUT == ret) {
        wait_ret_ = (OB_SUCCESS == wait_ret_) ? ret : wait_ret_;
        ret = OB_SUCCESS;
      }
      LOG_INFO("wait sys leader finish", K(ret), K_(tenant_id));
      sys_tg_expected_leaders_.reuse();
    }
    if (OB_SUCC(ret) && non_sys_tg_expected_leaders_.count() > 0) {
      const bool is_sys = false;
      const int64_t timeout = generate_wait_leader_timeout(is_sys);
      if (timeout <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("timeout shouldn't be less than or equal to 0", K(ret), K(timeout));
      } else if (OB_FAIL(user_leader_waiter_.tenant_user_partition_wait(
                     tenant_id_, non_sys_tg_expected_leaders_, timeout))) {
        LOG_WARN("wait elect leaders failed", K(ret), K(timeout));
      }
      if (OB_SUCCESS == ret || OB_WAIT_ELEC_LEADER_TIMEOUT == ret) {
        wait_ret_ = (OB_SUCCESS == wait_ret_) ? ret : wait_ret_;
        ret = OB_SUCCESS;
      }
      LOG_INFO("wait non sys leader finish", K(ret), K_(tenant_id));
      non_sys_tg_expected_leaders_.reuse();
    }
  }
  return ret;
}

int ObLeaderCoordinator::ExpectedLeaderWaitOperator::wait()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (accumulate_cnt_ <= 0) {
    // by pass
  } else if (OB_SYS_TENANT_ID != tenant_id_) {
    if (OB_FAIL(idling_.idle())) {
      LOG_WARN("idle failed", K(ret));
    }
  } else {
    if (OB_FAIL(do_wait())) {
      LOG_WARN("do wait failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    accumulate_cnt_ = 0;
    new_leader_counter_.reuse();
    old_leader_counter_.reuse();
  }
  return ret;
}

int ObLeaderCoordinator::ExpectedLeaderWaitOperator::finally_wait()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(do_wait())) {
    LOG_WARN("do wait failed", K(ret));
  } else {
    accumulate_cnt_ = 0;
    new_leader_counter_.reuse();
    old_leader_counter_.reuse();
  }
  return ret;
}

int ObLeaderCoordinator::ExpectedLeaderWaitOperator::append_expected_leader(
    share::ObLeaderElectionWaiter::ExpectedLeader& expected_leader, const bool is_sys_tg_partition)
{
  int ret = OB_SUCCESS;
  if (is_sys_tg_partition) {
    if (OB_FAIL(sys_tg_expected_leaders_.push_back(expected_leader))) {
      LOG_WARN("fail to push expected leader", K(ret), K(expected_leader));
    }
  } else {
    if (OB_FAIL(non_sys_tg_expected_leaders_.push_back(expected_leader))) {
      LOG_WARN("fail to push expected leader", K(ret), K(expected_leader));
    }
  }
  if (OB_SUCC(ret)) {
    const common::ObAddr& new_leader = expected_leader.exp_leader_;
    int64_t count = 0;
    ret = new_leader_counter_.get_refactored(new_leader, count);
    if (OB_HASH_NOT_EXIST == ret) {
      // add a new k/v pair
      int overwrite = 0;
      if (OB_FAIL(new_leader_counter_.set_refactored(new_leader, 1, overwrite))) {
        LOG_WARN("fail to set refactored", K(ret));
      }
    } else if (OB_SUCCESS == ret) {
      // inc the k/v pair already exist
      int overwrite = 1;
      if (OB_FAIL(new_leader_counter_.set_refactored(new_leader, count + 1, overwrite))) {
        LOG_WARN("fail to set refactored", K(ret));
      }
    } else {
      LOG_WARN("fail to get k/v from new leader counter", K(ret), K(new_leader));
    }
  }

  if (OB_SUCC(ret)) {
    const common::ObAddr& old_leader = expected_leader.old_leader_;
    int64_t count = 0;
    ret = old_leader_counter_.get_refactored(old_leader, count);
    if (OB_HASH_NOT_EXIST == ret) {
      // add a new k/v pair
      int overwrite = 0;
      if (OB_FAIL(old_leader_counter_.set_refactored(old_leader, 1, overwrite))) {
        LOG_WARN("fail to set refacorted", K(ret));
      }
    } else if (OB_SUCCESS == ret) {
      // inc the k/v pair already exist
      int overwrite = 1;
      if (OB_FAIL(old_leader_counter_.set_refactored(old_leader, count + 1, overwrite))) {
        LOG_WARN("fail to set refactored", K(ret));
      }
    } else {
      LOG_WARN("fail to get k/v from new leader counter", K(ret), K(old_leader));
    }
  }

  if (OB_SUCC(ret)) {
    ++accumulate_cnt_;
  }
  return ret;
}

int ObLeaderCoordinator::ExpectedLeaderWaitOperator::get_wait_ret() const
{
  return wait_ret_;
}

int64_t ObLeaderCoordinator::ExpectedLeaderWaitOperator::generate_wait_leader_timeout(const bool is_sys)
{
  // I don't think this will overflow
  const int64_t cnt = (is_sys ? sys_tg_expected_leaders_.count() : non_sys_tg_expected_leaders_.count());
  int64_t timeout = cnt * WAIT_LEADER_US_PER_PARTITION;
  if (timeout < ObLeaderCoordinator::WAIT_SWITCH_LEADER_TIMEOUT) {
    timeout = ObLeaderCoordinator::WAIT_SWITCH_LEADER_TIMEOUT;
  } else if (timeout > 4 * ObLeaderCoordinator::WAIT_SWITCH_LEADER_TIMEOUT) {
    timeout = 4 * ObLeaderCoordinator::WAIT_SWITCH_LEADER_TIMEOUT;
  }
  return timeout;
}

int ObLeaderCoordinator::ExpectedLeaderWaitOperator::do_get_non_sys_part_leader_cnt(
    const uint64_t sql_tenant_id, const common::ObIArray<common::ObZone>& zones, int64_t& leader_cnt)
{
  int ret = OB_SUCCESS;
  ObRootService* root_service = NULL;
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    sqlclient::ObMySQLResult* result = NULL;
    if (OB_UNLIKELY(!inited_)) {
      ret = OB_NOT_INIT;
      LOG_WARN("not init", K(ret));
    } else if (NULL == (root_service = GCTX.root_service_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rootservice pointer is null");
    } else if (OB_UNLIKELY(OB_INVALID_ID == sql_tenant_id)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid tenant id", K(ret), K(sql_tenant_id));
    } else if (OB_UNLIKELY(zones.count() <= 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret));
    } else {
      ObSqlString sql;
      ObMySQLProxy& sql_proxy = root_service->get_sql_proxy();
      if (OB_FAIL(sql.append_fmt("SELECT floor(COUNT(*)) as count FROM %s "
                                 "WHERE tenant_id = %lu AND ROLE = 1 AND ZONE IN (",
              OB_ALL_TENANT_META_TABLE_TNAME,
              tenant_id_))) {
        LOG_WARN("fail to append fmt", K(ret));
      } else {
        bool first = true;
        const int64_t idx = 0;
        for (int64_t i = 0; OB_SUCC(ret) && i < zones.count(); ++i) {
          if (OB_FAIL(sql.append_fmt("%s\'%s\'", first ? "" : ", ", zones.at(i).ptr()))) {
            LOG_WARN("fail to append", K(ret));
          } else {
            first = false;
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(sql.append_fmt(")"))) {
          LOG_WARN("fail to append", K(ret));
        } else if (OB_FAIL(sql_proxy.read(res, sql_tenant_id, sql.ptr()))) {
          LOG_WARN("fail to read result", K(ret));
        } else if (OB_UNLIKELY(NULL == (result = res.get_result()))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get result failed", K(ret));
        } else if (OB_FAIL(result->next())) {
          LOG_WARN("fail to get result", K(ret));
        } else if (OB_FAIL(result->get_int(idx, leader_cnt))) {
          LOG_WARN("fail to get leader cnt", K(ret));
        } else {
          LOG_INFO("remaining leader on zones", K(leader_cnt), K(zones), "tenant_id", sql_tenant_id);
        }
      }
    }
  }
  return ret;
}

int ObLeaderCoordinator::ExpectedLeaderWaitOperator::get_non_sys_part_leader_cnt(
    const common::ObIArray<common::ObZone>& zones, int64_t& leader_cnt)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(OB_SYS_TENANT_ID == tenant_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sys tenant should not be here", K(ret));
  } else if (OB_UNLIKELY(zones.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(do_get_non_sys_part_leader_cnt(tenant_id_, zones, leader_cnt))) {
    LOG_WARN("fail to get non sys part leader cnt", K(ret));
  }
  return ret;
}

ObLeaderCoordinator::ObLeaderCoordinator()
    : ObRsReentrantThread(true),
      inited_(false),
      pt_operator_(NULL),
      schema_service_(NULL),
      server_mgr_(NULL),
      srv_rpc_proxy_(NULL),
      zone_mgr_(NULL),
      rebalance_task_mgr_(NULL),
      unit_mgr_(NULL),
      config_(NULL),
      idling_(stop_),
      lock_(),
      leader_stat_(),
      leader_stat_lock_(),
      switch_info_stat_(),
      daily_merge_scheduler_(NULL),
      last_switch_turn_stat_(LAST_SWITCH_TURN_SUCCEED),
      sys_balanced_(false),
      partition_info_container_(),
      is_in_merging_(false)
{}

ObLeaderCoordinator::~ObLeaderCoordinator()
{}

int ObLeaderCoordinator::init(ObPartitionTableOperator& pt_operator, ObMultiVersionSchemaService& schema_service,
    ObServerManager& server_mgr, ObSrvRpcProxy& srv_rpc_proxy, ObZoneManager& zone_mgr,
    ObRebalanceTaskMgr& rebalance_task_mgr, ObUnitManager& unit_mgr, ObServerConfig& config,
    ObDailyMergeScheduler& daily_merge_scheduler)
{
  int ret = OB_SUCCESS;
  static const int64_t leader_coordinator_thread_cnt = 1;

  ObLatchWGuard guard(lock_, ObLatchIds::LEADER_COORDINATOR_LOCK);
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(create(leader_coordinator_thread_cnt, "LeaderCoord"))) {
    LOG_WARN("create leader coordinator thread failed", K(ret), K(leader_coordinator_thread_cnt));
  } else if (OB_FAIL(switch_info_stat_.init(&srv_rpc_proxy))) {
    LOG_WARN("failed to init switch info stat", K(ret));
  } else if (OB_FAIL(partition_info_container_.init(&pt_operator, &schema_service, this))) {
    LOG_WARN("fail to init partition info container", K(ret));
  } else {
    pt_operator_ = &pt_operator;
    schema_service_ = &schema_service;
    server_mgr_ = &server_mgr;
    srv_rpc_proxy_ = &srv_rpc_proxy;
    zone_mgr_ = &zone_mgr;
    rebalance_task_mgr_ = &rebalance_task_mgr;
    unit_mgr_ = &unit_mgr;
    config_ = &config;
    daily_merge_scheduler_ = &daily_merge_scheduler;
    leader_stat_.reset();
    last_switch_turn_stat_ = LAST_SWITCH_TURN_SUCCEED;
    sys_balanced_ = false;
    inited_ = true;
  }
  return ret;
}

int ObLeaderCoordinator::get_auto_leader_switch_idle_interval(int64_t& idle_interval_us) const
{
  int ret = OB_SUCCESS;
  idle_interval_us = GCONF.auto_leader_switch_interval;
  if (switch_info_stat_.is_doing_smooth_switch()) {
    int64_t expect_ts = 0;
    const int64_t now = ObTimeUtility::current_time();
    const int64_t switch_leader_duration_time = GCONF.merger_switch_leader_duration_time;
    if (OB_FAIL(switch_info_stat_.get_expect_switch_ts(switch_leader_duration_time, expect_ts))) {
      LOG_WARN("failed to get expect switch ts", K(ret));
    } else {
      const int64_t min_switch_interval = ObServerSwitchLeaderInfoStat::MIN_SWITCH_INTERVAL;
      const int64_t should_sleep_time = std::max(min_switch_interval, expect_ts - now);
      idle_interval_us = std::min(should_sleep_time, idle_interval_us);
    }
  } else if (is_in_merging_) {
    const int64_t merger_check_interval_twice = 2 * GCONF.merger_check_interval;
    idle_interval_us = std::max(merger_check_interval_twice, idle_interval_us);
  }
  return ret;
}

void ObLeaderCoordinator::run3()
{
  int ret = OB_SUCCESS;

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    LOG_INFO("leader coordinate start");
    // clear server leader stat
    {
      ObLatchWGuard guard(leader_stat_lock_, ObLatchIds::LEADER_STAT_LOCK);
      leader_stat_.reuse();
    }
    sys_balanced_ = false;
    // restart smooth switch leader
    {
      int64_t smooth_switch_start_time = 0;
      if (OB_FAIL(fetch_smooth_switch_start_time(smooth_switch_start_time))) {
        LOG_WARN("failed to fetch_smooth_switch_start_time", K(ret));
      } else if (0 != smooth_switch_start_time) {
        ObLatchWGuard guard(lock_, ObLatchIds::LEADER_COORDINATOR_LOCK);
        LOG_INFO("need to continue do smooth switch", K(smooth_switch_start_time));
        if (OB_FAIL(switch_info_stat_.start_new_smooth_switch(smooth_switch_start_time))) {
          LOG_WARN("failed to start new smooth switch", K(ret));
        }
      }
    }

    while (!stop_) {
      update_last_run_timestamp();
      ObCurTraceId::init(GCONF.self_addr_);
      DEBUG_SYNC(BEFORE_AUTO_COORDINATE);
      int64_t idle_interval_us = 0;
      THIS_WORKER.set_timeout_ts(INT64_MAX);
      if (OB_SUCC(ret)) {
        ObLatchWGuard guard(lock_, ObLatchIds::LEADER_COORDINATOR_LOCK);

        if (OB_FAIL(auto_coordinate())) {
          LOG_WARN("failed to to smooth coordinate", K(ret));
        } else if (OB_FAIL(get_auto_leader_switch_idle_interval(idle_interval_us))) {
          LOG_WARN("fail to get auto leader switch idle interval", K(ret));
        }

        if (OB_FAIL(ret)) {
          idle_interval_us = ObServerSwitchLeaderInfoStat::MIN_SWITCH_INTERVAL;
        }
      }  // end of lock

      LOG_INFO("sleep for next smooth_switch_turn", K(ret), K(idle_interval_us));
      idling_.set_idle_interval_us(idle_interval_us);
      if (OB_FAIL(idling_.idle())) {  // allow overwrite ret
        break;
      }
    }
    LOG_INFO("leader coordinate stop");
  }

  {
    ObLatchWGuard guard(lock_, ObLatchIds::LEADER_COORDINATOR_LOCK);
    switch_info_stat_.clear();
    sys_balanced_ = false;
  }
}

void ObLeaderCoordinator::wakeup()
{
  if (!inited_) {
    LOG_WARN("not init");
  } else {
    idling_.wakeup();
  }
}

void ObLeaderCoordinator::stop()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!stop_) {
    ObReentrantThread::stop();
    wakeup();
  }
}

// must be locked by caller
int ObLeaderCoordinator::start_smooth_coordinate()
{
  int ret = OB_SUCCESS;
  int64_t smooth_switch_start_time = ObTimeUtility::current_time();

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(switch_info_stat_.start_new_smooth_switch(smooth_switch_start_time))) {
    LOG_WARN("failed to start new smooth switch", K(ret));
  } else {
    LOG_INFO("succ to start smooth coordinate", K(smooth_switch_start_time));
    wakeup();
  }
  return ret;
}

int ObLeaderCoordinator::fetch_smooth_switch_start_time(int64_t& smooth_switch_start_time)
{
  int ret = OB_SUCCESS;
  common::ObArray<share::ObZoneInfo> infos;
  smooth_switch_start_time = 0;

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(zone_mgr_->get_zone(infos))) {
    LOG_WARN("failed to get zone", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < infos.count(); ++i) {
      share::ObZoneInfo& info = infos.at(i);
      LOG_INFO("fetch_smooth_switch_start_time", K(info));
      if (info.is_merging_ && info.merge_status_ == ObZoneInfo::MERGE_STATUS_IDLE) {
        if (info.merge_start_time_ > smooth_switch_start_time) {
          smooth_switch_start_time = info.merge_start_time_;
        }
      }
    }

    if (smooth_switch_start_time + GCONF.merger_switch_leader_duration_time < ObTimeUtility::current_time()) {
      // already timeout, no need to do smooth switch
      smooth_switch_start_time = 0;
    }
  }

  return ret;
}

int ObLeaderCoordinator::is_doing_smooth_coordinate(bool& is_doing)
{
  int ret = OB_SUCCESS;

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    is_doing = switch_info_stat_.is_doing_smooth_switch();
  }
  return ret;
}

int ObLeaderCoordinator::is_last_switch_turn_succ(bool& is_succ)
{
  int ret = OB_SUCCESS;
  ObLatchWGuard guard(lock_, ObLatchIds::LEADER_COORDINATOR_LOCK);

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    is_succ = (last_switch_turn_stat_ == LAST_SWITCH_TURN_SUCCEED);
  }
  return ret;
}

int ObLeaderCoordinator::coordinate()
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> tenant_ids;
  ObArray<ObAddr> excluded_servers;
  ObArray<ObZone> excluded_zones;
  common::ObArray<common::ObAddr> server_list;
  ObZone empty_zone;
  const int64_t switch_leader_duration_time = GCONF.merger_switch_leader_duration_time;
  ObLatchWGuard guard(lock_, ObLatchIds::LEADER_COORDINATOR_LOCK);

  switch_info_stat_.clear_switch_limited();
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!config_->enable_auto_leader_switch) {
    LOG_INFO("enable_auto_leader_switch is disable, skip do coordinate");
  } else {
    if (OB_FAIL(switch_info_stat_.prepare_next_turn(stop_, *server_mgr_, switch_leader_duration_time))) {
      LOG_WARN("failed to prepare for new turn", K(ret));
    } else if (OB_FAIL(get_tenant_ids(tenant_ids))) {
      LOG_WARN("get_tenant_ids failed", K(ret));
    } else if (OB_FAIL(coordinate_helper(partition_info_container_, excluded_zones, excluded_servers, tenant_ids))) {
      LOG_WARN("coordinate failed", K(ret));
    } else if (OB_FAIL(update_leader_stat())) {
      LOG_WARN("update_leader_stat failed", K(ret));
    }
  }
  return ret;
}

// no lock, need to lock lock_ by caller
int ObLeaderCoordinator::auto_coordinate()
{
  int ret = OB_SUCCESS;
  common::ObArray<common::ObAddr> server_list;
  ObZone empty_zone;
  ObArray<uint64_t> tenant_ids;
  ObArray<ObAddr> excluded_servers;
  ObArray<ObZone> excluded_zones;
  bool force_finish_smooth_switch = false;
  const int64_t switch_leader_duration_time = GCONF.merger_switch_leader_duration_time;

  DEBUG_SYNC(BEFORE_AUTO_COORDINATE);
  switch_info_stat_.clear_switch_limited();
  if (!config_->enable_auto_leader_switch) {
    LOG_INFO("enable_auto_leader_switch is disabled, skip auto_coordinate");
  } else {
    if (OB_FAIL(switch_info_stat_.prepare_next_turn(stop_, *server_mgr_, switch_leader_duration_time))) {
      LOG_WARN("failed to prepare for new turn", K(ret));
    } else if (OB_FAIL(get_tenant_ids(tenant_ids))) {
      LOG_WARN("get_tenant_ids failed", K(ret));
    } else if (OB_FAIL(coordinate_helper(partition_info_container_, excluded_zones, excluded_servers, tenant_ids))) {
      LOG_WARN("coordinate failed", K(ret));
    } else if (OB_FAIL(update_leader_stat())) {
      LOG_WARN("update_leader_stat failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (switch_info_stat_.get_is_switch_limited()) {
      last_switch_turn_stat_ = LAST_SWITCH_TURN_LIMITED;
    } else {
      last_switch_turn_stat_ = LAST_SWITCH_TURN_SUCCEED;
    }
    LOG_INFO("auto_coordinate stat",
        K_(last_switch_turn_stat),
        "is_doing_smooth_switch",
        switch_info_stat_.is_doing_smooth_switch());
  } else {
    last_switch_turn_stat_ = LAST_SWITCH_TURN_FAILED;
    LOG_INFO("auto_coordinate fail",
        K_(last_switch_turn_stat),
        "is_doing_smooth_switch",
        switch_info_stat_.is_doing_smooth_switch());
  }

  if (switch_info_stat_.is_doing_smooth_switch()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_FAIL(ret)) {
      if (OB_SUCCESS != (tmp_ret = switch_info_stat_.need_force_finish_smooth_switch(
                             switch_leader_duration_time, force_finish_smooth_switch))) {
        LOG_WARN("failed to check need_force_finish_smooth_switch", K(tmp_ret));
      }
    }

    if (OB_SUCC(ret) || (OB_SUCCESS == tmp_ret && force_finish_smooth_switch)) {
      if (OB_FAIL(switch_info_stat_.finish_smooth_turn(force_finish_smooth_switch))) {
        LOG_WARN("failed to finish smooth turn", K(ret));
      }
    }

    if (!switch_info_stat_.is_doing_smooth_switch()) {
      LOG_INFO("finish do smooth switch, wake up daily merge scheduler");
      daily_merge_scheduler_->wakeup();
    }
  }
  return ret;
}

int ObLeaderCoordinator::coordinate_tenants(const ObArray<ObZone>& excluded_zones,
    const ObIArray<ObAddr>& excluded_servers, const ObIArray<uint64_t>& tenant_ids, const bool force)
{
  int ret = OB_SUCCESS;
  ObLatchWGuard guard(lock_, ObLatchIds::LEADER_COORDINATOR_LOCK);
  LOG_INFO("do coordinate_tenants", K(excluded_zones), K(excluded_servers), K(tenant_ids), K(force));

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (tenant_ids.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_ids is empty", K(tenant_ids), K(ret));
  } else {
    PartitionInfoContainer partition_info_container;
    if (OB_FAIL(partition_info_container.init(pt_operator_, schema_service_, this))) {
      LOG_WARN("fail to init partition info container", K(ret));
    } else if (OB_FAIL(
                   coordinate_helper(partition_info_container, excluded_zones, excluded_servers, tenant_ids, force))) {
      LOG_WARN("coordinate helper failed", K(excluded_zones), K(excluded_servers), K(tenant_ids), K(force), K(ret));
    }
  }
  return ret;
}

int ObLeaderCoordinator::coordinate_partition_group(const uint64_t partition_entity_id, const int64_t partition_id,
    const ObIArray<ObAddr>& excluded_servers, const ObArray<ObZone>& excluded_zones)
{
  int ret = OB_SUCCESS;
  bool small_tenant = false;
  const uint64_t tenant_id = extract_tenant_id(partition_entity_id);
  const int64_t begin = ObTimeUtility::current_time();
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == partition_entity_id || OB_INVALID_ID == tenant_id || partition_id < 0 ||
             excluded_servers.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KT(partition_entity_id), K(tenant_id), K(partition_id), K(excluded_servers));
  } else if (OB_FAIL(ObTenantUtils::check_small_tenant(tenant_id, small_tenant))) {
    LOG_WARN("check small tenant failed", K(ret), K(tenant_id));
  } else if (OB_UNLIKELY(NULL == GCTX.root_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rootservice ptr is null", K(ret));
  } else {
    const bool force = true;
    if (small_tenant) {
      // coordinate full tenant partitions for small tenant
      ObArray<uint64_t> tenant_ids;
      if (OB_FAIL(tenant_ids.push_back(tenant_id))) {
        LOG_WARN("push tenant_id to array failed", K(ret));
      } else if (OB_FAIL(coordinate_tenants(excluded_zones, excluded_servers, tenant_ids, force))) {
        LOG_WARN("coordinate tenant failed", K(ret), K(excluded_servers), K(tenant_ids), K(force));
      }
    } else {
      ObLatchWGuard guard(lock_, ObLatchIds::LEADER_COORDINATOR_LOCK);
      ObSchemaGetterGuard schema_guard;
      ObArenaAllocator partition_allocator(ObModIds::OB_RS_LEADER_COORDINATOR_PA);
      TenantPartition tenant_partition;
      ObArray<uint64_t> partition_entity_ids;
      const ObPartitionSchema* partition_schema = nullptr;
      int64_t partition_idx = -1;
      PartitionInfoContainer partition_info_container;
      CursorContainer cursor_container;
      TenantUnit tenant_unit;
      const int64_t dummy_cnt = 1;
      const share::schema::ObSchemaType schema_type =
          (is_new_tablegroup_id(partition_entity_id) ? share::schema::ObSchemaType::TABLEGROUP_SCHEMA
                                                     : share::schema::ObSchemaType::TABLE_SCHEMA);
      bool check_dropped_partition = true;
      if (OB_FAIL(partition_info_container.init(pt_operator_, schema_service_, this))) {
        LOG_WARN("fail to init partition info container", K(ret));
      } else if (OB_FAIL(partition_info_container.build_partition_group_info(
                     tenant_id, partition_entity_id, partition_id))) {
        LOG_WARN("fail to build partition group info", K(ret));
      } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
        LOG_WARN("get schema guard failed", K(ret));
      } else if (OB_FAIL(ObPartMgrUtils::get_partition_schema(
                     schema_guard, partition_entity_id, schema_type, partition_schema))) {
      } else if (NULL == partition_schema) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("partition schema is nullptr", K(ret), KT(partition_entity_id));
      } else if (!partition_schema->has_self_partition()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("has no phy partition", K(ret), K(partition_entity_id));
      } else if (OB_FAIL(ObPartMgrUtils::get_partition_idx_by_id(
                     *partition_schema, check_dropped_partition, partition_id, partition_idx))) {
        LOG_WARN("fail to get partition idx", K(ret), K(partition_entity_id), K(partition_id));
      } else if (partition_idx < 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected partition idx", K(ret), K(partition_idx));
      } else if (OB_FAIL(get_same_tablegroup_partition_entity_ids(
                     schema_guard, partition_entity_id, partition_entity_ids))) {
        LOG_WARN("fail to get same tablegroup table ids", K(ret));
      } else if (OB_FAIL(build_tg_partition(schema_guard,
                     partition_info_container,
                     partition_entity_ids,
                     small_tenant,
                     partition_allocator,
                     tenant_partition,
                     partition_idx))) {
        LOG_WARN("build partition group partition failed",
            K(ret),
            K(partition_entity_ids),
            K(small_tenant),
            K(partition_id));
      } else if (!tenant_partition.empty() && !tenant_partition.at(0)->empty()) {
        if (nullptr == tenant_partition.at(0)->at(0).info_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("info_ ptr is null", K(ret));
        } else if (combine_id(OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID) ==
                   tenant_partition.at(0)->at(0).info_->get_table_id()) {
          if (OB_FAIL(move_all_core_last(tenant_partition))) {
            LOG_WARN("move all core table last failed", K(ret));
          }
        }
      }
      if (OB_SUCC(ret)) {
        ExpectedLeaderWaitOperator leader_wait_operator(
            *pt_operator_, GCTX.root_service_->get_sql_proxy(), stop_, *this);
        if (OB_FAIL(leader_wait_operator.init(tenant_id))) {
          LOG_WARN("fail to init leader wait operator", K(ret));
        } else if (OB_FAIL(get_tenant_unit(tenant_id, tenant_unit))) {
          LOG_WARN("fail to get tenant unit", K(ret));
        } else if (OB_FAIL(build_leader_switch_cursor_container(tenant_id,
                       tenant_unit,
                       tenant_partition,
                       cursor_container,
                       excluded_zones,
                       excluded_servers,
                       force))) {
          LOG_WARN("fail to build leader switch cursor array", K(ret));
        } else if (OB_FAIL(coordinate_partition_arrays(
                       leader_wait_operator, tenant_partition, tenant_unit, cursor_container, force))) {
          LOG_WARN("coordinate partition arrays failed", K(ret), K(force));
        }
      }
    }
  }
  LOG_INFO("coordinate partition group leader finish",
      K(ret),
      "time_used",
      ObTimeUtility::current_time() - begin,
      KT(partition_entity_id),
      K(partition_id),
      K(excluded_servers));
  return ret;
}

int ObLeaderCoordinator::get_same_tablegroup_partition_entity_ids(share::schema::ObSchemaGetterGuard& schema_guard,
    const uint64_t partition_entity_id, common::ObIArray<uint64_t>& partition_entity_ids)
{
  int ret = OB_SUCCESS;
  partition_entity_ids.reset();
  if (!is_tablegroup_id(partition_entity_id)) {
    const share::schema::ObSimpleTableSchemaV2* table_schema = nullptr;
    if (OB_FAIL(schema_guard.get_table_schema(partition_entity_id, table_schema))) {
      LOG_WARN("fail to get table schema", K(ret), "table_id", partition_entity_id);
    } else if (OB_UNLIKELY(nullptr == table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("table schema ptr is null", K(ret), "table_id", partition_entity_id);
    } else if (OB_INVALID_ID != table_schema->get_tablegroup_id()) {
      if (OB_FAIL(schema_guard.get_table_ids_in_tablegroup(
              table_schema->get_tenant_id(), table_schema->get_tablegroup_id(), partition_entity_ids))) {
        LOG_WARN("get table ids of tablegroup failed",
            K(ret),
            "tenant_id",
            table_schema->get_tenant_id(),
            "tablegroup_id",
            table_schema->get_tablegroup_id());
      }
    } else {
      if (OB_FAIL(partition_entity_ids.push_back(partition_entity_id))) {
        LOG_WARN("array push back failed", K(ret));
      }
    }
  } else {
    // tablegroup
    if (OB_FAIL(partition_entity_ids.push_back(partition_entity_id))) {
      LOG_WARN("array push back failed", K(ret));
    }
  }
  return ret;
}

int ObLeaderCoordinator::get_leader_stat(ServerLeaderStat& leader_stat)
{
  int ret = OB_SUCCESS;
  ObLatchRGuard guard(leader_stat_lock_, ObLatchIds::LEADER_STAT_LOCK);
  leader_stat.reuse();

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(leader_stat.assign(leader_stat_))) {
    LOG_WARN("leader_stat assign failed", K(ret));
  }
  return ret;
}

int ObLeaderCoordinator::get_tenant_ids(ObArray<uint64_t>& tenant_ids)
{
  return ObTenantUtils::get_tenant_ids(schema_service_, tenant_ids);
}

int ObLeaderCoordinator::build_leader_balance_info(share::schema::ObSchemaGetterGuard& schema_guard,
    const uint64_t tenant_id, LcBalanceGroupContainer& balance_group_container, TenantPartition& tenant_partition,
    TenantUnit& tenant_unit)
{
  int ret = OB_SUCCESS;
  bool small_tenant = false;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(ObTenantUtils::check_small_tenant(tenant_id, small_tenant))) {
    LOG_WARN("fail to check small tenant", K(tenant_id), K(ret));
  } else if (small_tenant) {
    // small tenant, do not build balance group leader info
  } else if (OB_FAIL(balance_group_container.build_balance_groups_leader_info(
                 schema_guard, tenant_id, tenant_partition, tenant_unit))) {
    LOG_WARN("fail to calcualte balance groups rebalance condition", K(ret));
  }
  return ret;
}

int ObLeaderCoordinator::coordinate_helper(PartitionInfoContainer& partition_info_container,
    const ObArray<ObZone>& excluded_zones, const ObIArray<ObAddr>& excluded_servers,
    const ObIArray<uint64_t>& tenant_ids, const bool force)
{
  int ret = OB_SUCCESS;
  int bak_ret = OB_SUCCESS;
  ObArenaAllocator partition_allocator(ObModIds::OB_RS_LEADER_COORDINATOR_PA);
  TenantPartition tenant_partition;
  const ObTenantSchema* tenant_schema = NULL;
  int64_t start_ts = ObTimeUtility::current_time();

  LOG_INFO("start doing leader coordinate");
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (tenant_ids.count() <= 0) {
    // excluded_zones and excluded_servers can be empty, so don't check them
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_ids is empty", K(tenant_ids), K(ret));
  } else if (OB_UNLIKELY(NULL == GCTX.root_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("gctx root service ptr is null", K(ret));
  } else {
    ObArenaAllocator allocator(ObModIds::OB_RS_LEADER_COORDINATOR_PA);
    ObArray<uint64_t> new_tenant_ids;
    if (OB_FAIL(move_sys_tenant_to_last(tenant_ids, new_tenant_ids))) {
      LOG_WARN("move_sys_tenant_to_last failed", K(tenant_ids), K(ret));
    } else {
      FOREACH_CNT_X(tenant_id, new_tenant_ids, OB_SUCCESS == ret)
      {
        CursorContainer cursor_container;
        TenantUnit tenant_unit;
        allocator.reset();
        ExpectedLeaderWaitOperator leader_wait_operator(
            *pt_operator_, GCTX.root_service_->get_sql_proxy(), stop_, *this);
        TenantSchemaGetter stat_finder(*tenant_id);
        ObSchemaGetterGuard schema_guard;
        LcBalanceGroupContainer balance_group_container(*this, schema_guard, stat_finder, allocator);
        int64_t tenant_start_ts = ObTimeUtility::current_time();
        LOG_INFO("start do leader coordinate of tenant", "tenant_id", *tenant_id);
        tenant_partition.reuse();
        partition_allocator.reuse();
        if (OB_FAIL(check_cancel())) {
          LOG_WARN("check_cancel failed, rs is stopped", K(ret));
        } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(*tenant_id, schema_guard))) {
          LOG_WARN("get_schema_guard failed", K(ret));
        } else if (OB_FAIL(schema_guard.get_tenant_info(*tenant_id, tenant_schema))) {
          LOG_WARN("fail to get tenant info", K(ret), K(*tenant_id));
        } else if (OB_UNLIKELY(NULL == tenant_schema)) {
          // ignore tenant already deleted
          LOG_DEBUG("tenant is already deleted", K(*tenant_id));
        } else if (OB_INVALID_ID == *tenant_id) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid tenant id", K(ret), "tenant_id", *tenant_id);
        } else if (OB_FAIL(partition_info_container.build_tenant_partition_info(*tenant_id))) {
          LOG_WARN("fail to build tenant partition info", K(ret));
        } else if (OB_FAIL(balance_group_container.init(*tenant_id))) {
          LOG_WARN("fail to init balance index builder", K(ret));
        } else if (OB_FAIL(balance_group_container.build_base_info())) {
          LOG_WARN("fail to build balance index builder", K(ret), K(tenant_id));
        } else if (OB_FAIL(build_tenant_partition(
                       schema_guard, partition_info_container, *tenant_id, partition_allocator, tenant_partition))) {
          LOG_WARN("fail to build tenant partition", "tenant_id", *tenant_id, K(ret));
        } else if (OB_FAIL(get_tenant_unit(*tenant_id, tenant_unit))) {
          LOG_WARN("fail to get tenant unit", K(ret));
        } else if (OB_FAIL(build_leader_balance_info(
                       schema_guard, *tenant_id, balance_group_container, tenant_partition, tenant_unit))) {
          LOG_WARN("fail to build leader balance info", K(ret));
        } else if (OB_FAIL(build_leader_switch_cursor_container(*tenant_id,
                       tenant_unit,
                       tenant_partition,
                       cursor_container,
                       excluded_zones,
                       excluded_servers,
                       force))) {
          LOG_WARN("fail to build leader switch cursor array", K(ret));
        } else if (OB_FAIL(leader_wait_operator.init(*tenant_id))) {
          LOG_WARN("fail to init leader wait operator", K(ret));
        } else if (OB_FAIL(coordinate_partition_arrays(
                       leader_wait_operator, tenant_partition, tenant_unit, cursor_container, force))) {
          LOG_WARN("coordinate partition arrays failed", K(excluded_zones), K(excluded_servers), K(force), K(ret));
        } else {
        }                    // no more to do
        if (OB_FAIL(ret)) {  // print a warn log for this tenant
          LOG_WARN("fail to leader coordinator on specific tenant", K(ret), "tenant_id", *tenant_id);
          bak_ret = OB_SUCCESS == bak_ret ? ret : bak_ret;
          ret = OB_SUCCESS;  // rewrite to SUCC, so that tenants can go on switch
        }
        int64_t tenant_cost_time = ObTimeUtility::current_time() - tenant_start_ts;
        LOG_INFO("finish do leader coordinate of tenant", K(force), K(tenant_cost_time), "tenant_id", *tenant_id);
      }
      ret = OB_SUCCESS == ret ? bak_ret : ret;
    }
  }

  int64_t cost_time = ObTimeUtility::current_time() - start_ts;
  LOG_INFO("finish doing leader coordinate", K(ret), K(cost_time));
  return ret;
}

int ObLeaderCoordinator::do_build_normal_tenant_partition(share::schema::ObSchemaGetterGuard& schema_guard,
    const ObIArray<const IPartitionEntity*>& partition_entity_array, PartitionInfoContainer& partition_info_container,
    common::ObIAllocator& partition_allocator, TenantPartition& tenant_partition)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> table_ids;
  TablegroupPartition tg_partition;
  uint64_t prev_tg_id = OB_INVALID_ID;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    const bool small_tenant = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_entity_array.count(); ++i) {
      const IPartitionEntity* entity = partition_entity_array.at(i);
      if (OB_UNLIKELY(NULL == entity)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("partition entity is null", K(ret));
      } else if (!entity->has_self_partition()) {
        // has no partitions, bypass
      } else if (OB_INVALID_ID != prev_tg_id && entity->get_tablegroup_id() == prev_tg_id) {
        if (OB_FAIL(table_ids.push_back(entity->get_partition_entity_id()))) {
          LOG_WARN("push_back failed", K(ret));
        }
      } else {
        if (table_ids.count() > 0) {
          if (OB_FAIL(append_tg_partition(schema_guard,
                  partition_info_container,
                  table_ids,
                  small_tenant,
                  partition_allocator,
                  tenant_partition))) {
            LOG_WARN("fail to append tg partition", K(table_ids), K(ret));
          }
        }
        if (OB_FAIL(ret)) {
        } else {
          table_ids.reuse();
          if (OB_FAIL(table_ids.push_back(entity->get_partition_entity_id()))) {
            LOG_WARN("push_back failed", K(ret));
          } else {
            prev_tg_id = entity->get_tablegroup_id();
          }
        }
      }
    }
    if (OB_SUCCESS == ret && table_ids.count() > 0) {
      if (OB_FAIL(append_tg_partition(schema_guard,
              partition_info_container,
              table_ids,
              small_tenant,
              partition_allocator,
              tenant_partition))) {
        LOG_WARN("fail to append tg partition", K(table_ids), K(ret));
      }
    }
  }
  return ret;
}

int ObLeaderCoordinator::do_build_small_tenant_partition(const uint64_t tenant_id,
    share::schema::ObSchemaGetterGuard& schema_guard, const ObIArray<const IPartitionEntity*>& partition_entity_array,
    PartitionInfoContainer& partition_info_container, common::ObIAllocator& partition_allocator,
    TenantPartition& tenant_partition)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else {
    const bool small_tenant = true;
    ObArray<uint64_t> sys_tg_tables;
    ObArray<uint64_t> non_sys_tg_tables;
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_entity_array.count(); ++i) {
      const IPartitionEntity* entity = partition_entity_array.at(i);
      if (OB_UNLIKELY(NULL == entity)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table schema ptr is null", K(ret));
      } else if (!entity->has_self_partition()) {
        // has no partitions, bypass
      } else if (combine_id(tenant_id, OB_SYS_TABLEGROUP_ID) == entity->get_tablegroup_id()) {
        if (OB_FAIL(sys_tg_tables.push_back(entity->get_partition_entity_id()))) {
          LOG_WARN("fail to push back", K(ret));
        }
      } else {
        if (OB_FAIL(non_sys_tg_tables.push_back(entity->get_partition_entity_id()))) {
          LOG_WARN("fail to push back", K(ret));
        }
      }
    }
    if (OB_SUCC(ret) && sys_tg_tables.count() > 0) {
      if (OB_FAIL(append_tg_partition(schema_guard,
              partition_info_container,
              sys_tg_tables,
              small_tenant,
              partition_allocator,
              tenant_partition))) {
        LOG_WARN("fail to append to partition", K(ret));
      }
    }
    if (OB_SUCC(ret) && non_sys_tg_tables.count() > 0) {
      if (OB_FAIL(append_tg_partition(schema_guard,
              partition_info_container,
              non_sys_tg_tables,
              small_tenant,
              partition_allocator,
              tenant_partition))) {
        LOG_WARN("fail to append to partition", K(ret));
      }
    }
  }
  return ret;
}

int ObLeaderCoordinator::build_tenant_partition(ObSchemaGetterGuard& schema_guard,
    PartitionInfoContainer& partition_info_container, const uint64_t tenant_id,
    common::ObIAllocator& partition_allocator, TenantPartition& tenant_partition)
{
  int ret = OB_SUCCESS;
  ObArray<const IPartitionEntity*> partition_entity_array;
  bool small_tenant = false;
  ObArenaAllocator allocator(ObModIds::OB_RS_LEADER_COORDINATOR_PA);
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(ret));
  } else if (OB_FAIL(get_tenant_partition_entity_array(schema_guard, tenant_id, allocator, partition_entity_array))) {
    LOG_WARN("get tenant entity failed", K(tenant_id), K(ret));
  } else if (OB_FAIL(ObTenantUtils::check_small_tenant(tenant_id, small_tenant))) {
    LOG_WARN("fail to check small tenant", K(tenant_id), K(ret));
  } else if (small_tenant) {
    LOG_INFO("small tenant", K(tenant_id));
    if (OB_FAIL(do_build_small_tenant_partition(tenant_id,
            schema_guard,
            partition_entity_array,
            partition_info_container,
            partition_allocator,
            tenant_partition))) {
      LOG_WARN("fail to build small tenant partition", K(ret));
    }
  } else {
    LOG_INFO("not a small tenant", K(tenant_id));
    if (OB_FAIL(do_build_normal_tenant_partition(
            schema_guard, partition_entity_array, partition_info_container, partition_allocator, tenant_partition))) {
      LOG_WARN("fail ot build normal tenant partition", K(ret));
    }
  }

  // move all_core_table partition to last position of last partition_array
  if (OB_SUCCESS == ret && OB_SYS_TENANT_ID == tenant_id) {
    if (OB_FAIL(move_all_core_last(tenant_partition))) {
      LOG_WARN("move_all_core_last failed", K(ret));
    }
  }
  return ret;
}

int ObLeaderCoordinator::get_tenant_partition_entity_array(share::schema::ObSchemaGetterGuard& schema_guard,
    const uint64_t tenant_id, common::ObIAllocator& allocator, ObArray<const IPartitionEntity*>& partition_entity_array)
{
  int ret = OB_SUCCESS;
  partition_entity_array.reset();
  ObArray<const share::schema::ObSimpleTableSchemaV2*> table_schemas;
  ObArray<const share::schema::ObSimpleTablegroupSchema*> tg_schemas;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", K(tenant_id), K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schemas_in_tenant(tenant_id, table_schemas))) {
    LOG_WARN("fail to get table schemas", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_tablegroup_schemas_in_tenant(tenant_id, tg_schemas))) {
    LOG_WARN("fail to get tablegroup schemas", K(ret), K(tenant_id));
  } else if (OB_FAIL(check_cancel())) {
    LOG_WARN("rs stoped", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < table_schemas.count(); ++i) {
      const ObSimpleTableSchemaV2* table_schema = table_schemas.at(i);
      SATableEntity* partition_entity = nullptr;
      void* ptr = nullptr;
      if (OB_UNLIKELY(nullptr == table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table schema ptr is null", K(ret));
      } else if (nullptr == (ptr = allocator.alloc(sizeof(SATableEntity)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory", K(ret));
      } else if (nullptr == (partition_entity = new (ptr) SATableEntity(*table_schema))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to construct partition entity", K(ret));
      } else if (OB_FAIL(partition_entity_array.push_back(partition_entity))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < tg_schemas.count(); ++i) {
      const ObSimpleTablegroupSchema* tg_schema = tg_schemas.at(i);
      TablegroupEntity* partition_entity = nullptr;
      void* ptr = nullptr;
      if (OB_UNLIKELY(nullptr == tg_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table schema ptr is null", K(ret));
      } else if (nullptr == (ptr = allocator.alloc(sizeof(TablegroupEntity)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory", K(ret));
      } else if (nullptr == (partition_entity = new (ptr) TablegroupEntity(*tg_schema))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to construct partition entity", K(ret));
      } else if (OB_FAIL(partition_entity_array.push_back(partition_entity))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
    std::sort(partition_entity_array.begin(), partition_entity_array.end(), partition_entity_cmp);
  }
  return ret;
}

int ObLeaderCoordinator::append_tg_partition(ObSchemaGetterGuard& schema_guard,
    PartitionInfoContainer& partition_info_container, const ObArray<uint64_t>& partition_entity_ids,
    const bool small_tenant, common::ObIAllocator& partition_allocator, TenantPartition& tenant_partition)
{
  int ret = OB_SUCCESS;
  TablegroupPartition tg_partition;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (partition_entity_ids.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", "ids_count", partition_entity_ids.count(), K(ret));
  } else if (OB_FAIL(build_tg_partition(schema_guard,
                 partition_info_container,
                 partition_entity_ids,
                 small_tenant,
                 partition_allocator,
                 tg_partition,
                 ALL_PARTITIONS))) {
    LOG_WARN("fail to build tg partition", K(partition_entity_ids), K(ret));
  } else {
    FOREACH_CNT_X(partition_array, tg_partition, OB_SUCCESS == ret)
    {
      if (OB_FAIL(tenant_partition.push_back(*partition_array))) {
        LOG_WARN("push_back failed", K(ret));
      }
    }
  }
  return ret;
}

int ObLeaderCoordinator::build_tg_partition(ObSchemaGetterGuard& schema_guard,
    PartitionInfoContainer& partition_info_container, const ObIArray<uint64_t>& partition_entity_ids,
    const bool small_tenant, common::ObIAllocator& partition_allocator, TablegroupPartition& tg_partition,
    const int64_t partition_idx)
{
  int ret = OB_SUCCESS;
  int64_t partition_array_capacity = 0;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (partition_entity_ids.count() <= 0 || (ALL_PARTITIONS != partition_idx && partition_idx < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", "ids_cnt", partition_entity_ids.count(), K(partition_idx), K(ret));
  } else if (OB_FAIL(calculate_partition_array_capacity(
                 schema_guard, partition_entity_ids, small_tenant, partition_idx, partition_array_capacity))) {
    LOG_WARN("fail to calcuate partition array_capacity", K(ret), K(small_tenant), K(partition_entity_ids));
  } else if (partition_array_capacity <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid partition array capacity", K(ret));
  } else {
    Partition partition;
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_entity_ids.count(); ++i) {
      const ObPartitionSchema* partition_schema = NULL;
      const share::schema::ObSchemaType schema_type =
          (is_new_tablegroup_id(partition_entity_ids.at(i)) ? share::schema::ObSchemaType::TABLEGROUP_SCHEMA
                                                            : share::schema::ObSchemaType::TABLE_SCHEMA);
      if (OB_FAIL(check_cancel())) {
        LOG_WARN("rs is stopped", K(ret));
      } else if (OB_FAIL(ObPartMgrUtils::get_partition_schema(
                     schema_guard, partition_entity_ids.at(i), schema_type, partition_schema))) {
        LOG_WARN("fail to get partition schema", K(ret), "entity_id", partition_entity_ids.at(i));
      } else if (nullptr == partition_schema) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("partition schema not exist", "entity_id", partition_entity_ids.at(i), K(ret));
      } else if (!partition_schema->has_self_partition()) {
        // bypass
      } else {
        bool check_dropped_schema = true;
        ObPartitionKeyIter iter(partition_entity_ids.at(i), *partition_schema, check_dropped_schema);
        int j = 0;
        int64_t phy_part_id = -1;
        while (OB_SUCC(ret) && OB_SUCC(iter.next_partition_id_v2(phy_part_id))) {
          partition.reuse();
          if (ALL_PARTITIONS != partition_idx && j != partition_idx) {
            // do nothing, since partition idx not match
          } else if (OB_FAIL(fill_partition_tg_id(schema_guard, partition_entity_ids.at(i), small_tenant, partition))) {
            LOG_WARN("fail to fill partition tg_id", K(ret), "table_id", partition_entity_ids.at(i));
          } else if (OB_FAIL(partition_info_container.get(partition_entity_ids.at(i), phy_part_id, partition.info_))) {
            if (OB_ENTRY_NOT_EXIST == ret) {
              ret = OB_SUCCESS;  // rewrite to succ, since schema guard may not match
              LOG_WARN(
                  "partition not exist", K(ret), "partition_id", phy_part_id, "table_id", partition_entity_ids.at(i));
            } else {
              LOG_WARN("fail to get partition info from container",
                  K(ret),
                  "table_id",
                  partition_entity_ids.at(i),
                  K(phy_part_id));
            }
          } else if (OB_FAIL(append_to_tg_partition(schema_guard,
                         (small_tenant || ALL_PARTITIONS != partition_idx),
                         j,
                         tg_partition,
                         partition,
                         partition_allocator,
                         partition_array_capacity))) {
            LOG_WARN("fail to append partition", K(ret), "partition idx", j);
          } else {
          }  // do nothing
          ++j;
        }
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
      }
    }
  }
  return ret;
}

int ObLeaderCoordinator::fill_partition_tg_id(share::schema::ObSchemaGetterGuard& schema_guard,
    const uint64_t partition_entity_id, const bool small_tenant, Partition& partition)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == partition_entity_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(partition_entity_id));
  } else {
    partition.table_id_ = partition_entity_id;
    if (small_tenant) {
      partition.tg_id_ = extract_tenant_id(partition_entity_id);
    } else if (!is_tablegroup_id(partition_entity_id)) {
      const share::schema::ObSimpleTableSchemaV2* table_schema = nullptr;
      if (OB_FAIL(schema_guard.get_table_schema(partition_entity_id, table_schema))) {
        LOG_WARN("fail to get table schema", K(ret), "table_id", partition_entity_id);
      } else if (OB_UNLIKELY(nullptr == table_schema)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("schema not exist", K(ret), "table_id", partition_entity_id);
      } else if (0 != table_schema->get_tablegroup_id() && OB_INVALID_ID != table_schema->get_tablegroup_id()) {
        partition.tg_id_ = table_schema->get_tablegroup_id();
      } else {
        partition.tg_id_ = partition_entity_id;
      }
    } else {
      partition.tg_id_ = partition_entity_id;
    }
  }
  return ret;
}

int ObLeaderCoordinator::append_to_tg_partition(share::schema::ObSchemaGetterGuard& schema_guard,
    const bool is_single_partition_group, const int64_t partition_idx, TablegroupPartition& tg_partition,
    const Partition& partition, common::ObIAllocator& partition_allocator, const int64_t partition_array_capacity)
{
  int ret = OB_SUCCESS;
  PartitionArray* partitions = NULL;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(partition_idx < 0) || OB_UNLIKELY(partition_array_capacity <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(partition_idx), K(partition_array_capacity));
  } else if (OB_UNLIKELY(nullptr == partition.info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("info ptr is null", K(ret));
  } else if (partition.info_->get_replicas_v2().count() <= 0) {
    // maybe the table has already been deleted, don't set ret, ignore it
    LOG_WARN("partition has no replicas", "partition_info", *partition.info_, K(ret));
  } else {
    if (is_single_partition_group) {
      // when small tenant or a specific partition id,
      // only one partition group is needed.
      if (tg_partition.count() > 0) {
        if (OB_UNLIKELY(NULL == (partitions = tg_partition.at(0)))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected error, partition array null", K(ret));
        } else if (OB_FAIL(partitions->push_back(partition))) {
          LOG_WARN("fail to push back partition", K(ret));
        } else {
        }  // do nothing
      } else {
        if (OB_FAIL(alloc_partition_array(
                schema_guard, partition.table_id_, partition_array_capacity, partition_allocator, partitions))) {
          LOG_WARN("fail to alloc partition array", K(ret));
        } else if (OB_UNLIKELY(NULL == partitions)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected error, partition array null", K(ret));
        } else if (OB_FAIL(partitions->push_back(partition))) {
          LOG_WARN("push back partition failed", K(ret));
        } else if (OB_FAIL(tg_partition.push_back(partitions))) {
          LOG_WARN("push back partition array failed", K(ret));
        } else {
        }  // do nothing
      }
    } else {
      // when not a small tenant or all partition group,
      // append to tg_partition with a offset partition_idx
      while (OB_SUCC(ret) && tg_partition.count() <= partition_idx) {
        if (OB_FAIL(alloc_partition_array(
                schema_guard, partition.table_id_, partition_array_capacity, partition_allocator, partitions))) {
          LOG_WARN("fail to alloc partition array", K(ret));
        } else if (OB_UNLIKELY(NULL == partitions)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected error, partition array null", K(ret));
        } else if (OB_FAIL(tg_partition.push_back(partitions))) {
          LOG_WARN("array push back failed", K(ret));
        } else {
        }  // do nothing
      }
      if (OB_SUCC(ret)) {
        if (OB_UNLIKELY(NULL == (partitions = tg_partition.at(partition_idx)))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected error, partition array null", K(ret));
        } else if (OB_FAIL(tg_partition.at(partition_idx)->push_back(partition))) {
          LOG_WARN("fail to push partition", K(ret));
        } else {
        }  // do nothing
      }
    }
  }
  return ret;
}

int ObLeaderCoordinator::move_all_core_last(TenantPartition& tenant_partition)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (tenant_partition.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_partition is empty", K(tenant_partition), K(ret));
  } else {
    PartitionArray* partitions = tenant_partition.at(0);
    ObPartitionKey pkey;
    if (OB_UNLIKELY(NULL == partitions) || OB_UNLIKELY(partitions->count() <= 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("partitions is empty", KP(partitions), K(ret));
    } else if (OB_FAIL(partitions->at(0).get_partition_key(pkey))) {
      LOG_WARN("get_partition_key failed", "partition", partitions->at(0), K(ret));
    } else if (!pkey.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pkey is invalid", K(pkey), K(ret));
    } else if (combine_id(OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID) != pkey.get_table_id()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("first partition in partitions not all_core_table partition", K(ret), K(pkey.get_table_id()));
    } else {
      if (partitions->count() > 1) {
        // move partition to last position of partitions
        Partition partition;
        if (OB_FAIL(partition.assign(partitions->at(0)))) {
          LOG_WARN("failed to assign partition", K(ret));
        } else if (OB_FAIL(partitions->at(0).assign(partitions->at(partitions->count() - 1)))) {
          LOG_WARN("failed to assign partitions.at(0)", K(ret));
        } else if (OB_FAIL(partitions->at(partitions->count() - 1).assign(partition))) {
          LOG_WARN("failed to assign partitions.at(partitions.count() - 1)", K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        if (tenant_partition.count() > 1) {
          // move first pg to last position of pgs
          const int64_t last_pos = tenant_partition.count() - 1;
          PartitionArray* temp_partitions = tenant_partition.at(0);
          tenant_partition.at(0) = tenant_partition.at(last_pos);
          tenant_partition.at(last_pos) = temp_partitions;
        }
      }
    }
  }
  return ret;
}

int ObLeaderCoordinator::get_tenant_unit(const uint64_t tenant_id, TenantUnit& tenant_unit)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(tenant_id), K(ret));
  } else {
    ObArray<uint64_t> pool_ids;
    if (OB_FAIL(unit_mgr_->get_pool_ids_of_tenant(tenant_id, pool_ids))) {
      LOG_WARN("get_pool_ids_of_tenant failed", K(tenant_id), K(ret));
    } else {
      ObArray<ObUnitInfo> unit_infos;
      FOREACH_CNT_X(pool_id, pool_ids, OB_SUCCESS == ret)
      {
        unit_infos.reuse();
        if (OB_FAIL(unit_mgr_->get_unit_infos_of_pool(*pool_id, unit_infos))) {
          LOG_WARN("get_unit_infos_of_pool failed", "pool_id", *pool_id, K(ret));
        } else {
          FOREACH_CNT_X(unit_info, unit_infos, OB_SUCCESS == ret)
          {
            if (OB_FAIL(tenant_unit.push_back(unit_info->unit_))) {
              LOG_WARN("push_back failed", K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObLeaderCoordinator::build_pre_switch_pg_index_array(const uint64_t tenant_id, TenantUnit& tenant_unit,
    common::ObArray<PartitionArray*>& partition_arrays, common::ObIArray<PreSwitchPgInfo>& pre_switch_index_array,
    const ObZoneList& excluded_zones, const common::ObIArray<common::ObAddr>& excluded_servers)
{
  int ret = OB_SUCCESS;
  CandidateLeaderInfoMap leader_info_map(*this);
  const int64_t MAP_BUCKET_NUM = 64;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id || partition_arrays.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("partition_arrays is empty", K(ret), K(tenant_id), K(partition_arrays));
  } else if (OB_FAIL(leader_info_map.create(MAP_BUCKET_NUM, ObModIds::OB_RS_LEADER_COORDINATOR))) {
    LOG_WARN("fail to create leader info map", K(ret));
  } else {
    pre_switch_index_array.reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_arrays.count(); ++i) {
      common::ObSEArray<common::ObAddr, 7> candidate_leaders;
      PartitionArray* partition_array = partition_arrays.at(i);
      bool need_switch = true;
      ObPartitionKey pkey;
      bool ignore_switch_percent = false;
      ObAddr cur_leader;
      bool same_leader = false;
      if (OB_UNLIKELY(NULL == partition_array)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("partition array is null", K(ret), KP(partition_array));
      } else if (OB_FAIL(check_cancel())) {
        LOG_WARN("check cancel failed, maybe rs is stopped", K(ret));
      } else if (OB_UNLIKELY(partition_array->count() <= 0)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("partition array is empty", K(ret));
      } else if (OB_FAIL(partition_array->at(0).get_partition_key(pkey))) {
        LOG_WARN("fail to get partition key", K(ret));
      } else if (!pkey.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pkey is invalid", K(ret), K(pkey));
      } else if (OB_FAIL(build_partition_statistic(*partition_array,
                     tenant_unit,
                     excluded_zones,
                     excluded_servers,
                     leader_info_map,
                     ignore_switch_percent))) {
        LOG_WARN("fail to build partition statistic", K(ret));
      } else if (OB_FAIL(get_cur_partition_leader(*partition_array, cur_leader, same_leader))) {
        LOG_WARN("fail to get cur partition leader", K(ret));
      } else if (!same_leader) {
        ignore_switch_percent = true;
        LOG_INFO("leaders not in same server, force switch",
            K(tenant_id),
            "first_pk",
            pkey,
            "part_id",
            pkey.get_partition_id());
      } else {
        if (OB_FAIL(choose_leader(
                tenant_id, partition_array->get_tablegroup_id(), leader_info_map, candidate_leaders, cur_leader))) {
          LOG_WARN("fail to choose leader", K(ret));
        } else if (candidate_leaders.count() <= 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("candidate leaders count must not 0", K(ret), K(candidate_leaders.count()));
        } else if (has_exist_in_array(candidate_leaders, cur_leader)) {
          need_switch = false;  // cur leader already in candidate leaders, no need to switch
        }
      }
      if (OB_FAIL(ret) || !need_switch) {
      } else if (OB_FAIL(pre_switch_index_array.push_back(PreSwitchPgInfo(i)))) {
        LOG_WARN("fail to push back", K(ret));
      }
      if (OB_FAIL(ret)) {
      } else if (need_switch) {
        // do nothing
      } else if (OB_FAIL(count_leader(*partition_array, false /*is_new_leader*/))) {
        LOG_WARN("fail to count leader", K(ret));
      }
    }
  }
  return ret;
}

int ObLeaderCoordinator::do_build_leader_switch_cursor_container(const uint64_t tenant_id, TenantUnit& tenant_unit,
    const common::ObIArray<PreSwitchPgInfo>& pre_switch_index_array, common::ObArray<PartitionArray*>& partition_arrays,
    const ObZoneList& excluded_zones, const common::ObIArray<common::ObAddr>& excluded_servers,
    CursorContainer& cursor_container)
{
  int ret = OB_SUCCESS;
  CandidateLeaderInfoMap leader_info_map(*this);
  const int64_t MAP_BUCKET_NUM = 4096;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id || partition_arrays.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("partition_arrays is empty", K(ret), K(tenant_id), K(partition_arrays));
  } else if (OB_FAIL(leader_info_map.create(MAP_BUCKET_NUM, ObModIds::OB_RS_LEADER_COORDINATOR))) {
    LOG_WARN("fail to create leader info map", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < pre_switch_index_array.count(); ++i) {
      const PreSwitchPgInfo& pg_info = pre_switch_index_array.at(i);
      common::ObSEArray<common::ObAddr, 7> candidate_leaders;
      PartitionArray* partition_array = nullptr;
      bool need_switch = true;
      ObPartitionKey pkey;
      bool ignore_switch_percent = false;
      ObAddr cur_leader;
      bool same_leader = false;
      if (OB_FAIL(check_cancel())) {
        LOG_WARN("check cancel failed, maybe rs is stopped", K(ret));
      } else if (!pg_info.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid pg info", K(ret));
      } else if (pg_info.index_ >= partition_arrays.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pg info index unexpected",
            K(ret),
            "pg_index",
            pg_info.index_,
            "partition array count",
            partition_arrays.count());
      } else if (nullptr == (partition_array = partition_arrays.at(pg_info.index_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("partition array ptr is null",
            K(ret),
            "pg_index",
            pg_info.index_,
            "partition array count",
            partition_arrays.count());
      } else if (OB_UNLIKELY(partition_array->count() <= 0)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("partition array is empty", K(ret));
      } else if (OB_FAIL(partition_array->at(0).get_partition_key(pkey))) {
        LOG_WARN("fail to get partition key", K(ret));
      } else if (!pkey.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pkey is invalid", K(ret), K(pkey));
      } else if (OB_FAIL(build_partition_statistic(*partition_array,
                     tenant_unit,
                     excluded_zones,
                     excluded_servers,
                     leader_info_map,
                     ignore_switch_percent))) {
        LOG_WARN("fail to build partition statistic", K(ret));
      } else if (OB_FAIL(update_leader_candidates(*partition_array, leader_info_map))) {
        LOG_WARN("fail to update leader candidate", K(ret));
      } else if (OB_FAIL(get_cur_partition_leader(*partition_array, cur_leader, same_leader))) {
        LOG_WARN("fail to get cur partition leader", K(ret));
      } else if (!same_leader) {
        ignore_switch_percent = true;
        LOG_INFO("leaders not in same server, force switch",
            K(tenant_id),
            "first_pk",
            pkey,
            "part_id",
            pkey.get_partition_id());
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(choose_leader(tenant_id,
                     partition_array->get_tablegroup_id(),
                     leader_info_map,
                     candidate_leaders,
                     cur_leader))) {
        LOG_WARN("fail to choose leader", K(ret));
      } else if (candidate_leaders.count() <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("candidate leaders count shall not be 0", K(ret));
      } else if (same_leader && has_exist_in_array(candidate_leaders, cur_leader)) {
        need_switch = false;  // cur leader already in candidate leaders, no need to switch
      }
      if (OB_FAIL(ret) || !need_switch) {
      } else if (candidate_leaders.count() <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("candidate leaders count shall not be 0", K(ret));
      } else {
        const int64_t count = candidate_leaders.count();
        const int64_t idx = std::abs(static_cast<int64_t>(get_random()) % count);
        ObAddr& advised_leader = candidate_leaders.at(idx);
        PartitionArrayCursor cursor(
            0, partition_array->count(), pg_info.index_, cur_leader, advised_leader, ignore_switch_percent);
        if (OB_FAIL(cursor_container.cursor_array_.push_back(cursor))) {
          LOG_WARN("fail to push back", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (need_switch) {
        // do nothing
      } else if (OB_FAIL(count_leader(*partition_array, false /*is_new_leader*/))) {
        LOG_WARN("fail to count leader", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(cursor_container.reorganize())) {
        LOG_WARN("fail to reorganize/sort the array", K(ret));
      }
    }
  }
  return ret;
}

int ObLeaderCoordinator::build_leader_switch_cursor_container(const uint64_t tenant_id, TenantUnit& tenant_unit,
    common::ObArray<PartitionArray*>& partition_arrays, CursorContainer& cursor_container,
    const ObZoneList& excluded_zones, const common::ObIArray<common::ObAddr>& excluded_servers, const bool force)
{
  int ret = OB_SUCCESS;
  const bool enable_auto_leader_switch = config_->enable_auto_leader_switch;
  common::ObArray<PreSwitchPgInfo> pre_switch_index_array;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id || partition_arrays.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("partition_arrays is empty", K(ret), K(tenant_id), K(partition_arrays));
  } else if (!force && !enable_auto_leader_switch) {
    // no need to switch leader, when auto leader switch is off
  } else if (OB_FAIL(pre_switch_index_array.reserve(partition_arrays.count()))) {
    LOG_WARN("fail to reserve index array", K(ret));
  } else if (OB_FAIL(build_pre_switch_pg_index_array(
                 tenant_id, tenant_unit, partition_arrays, pre_switch_index_array, excluded_zones, excluded_servers))) {
    LOG_WARN("fail to pre switch pg index array", K(ret));
  } else if (pre_switch_index_array.count() <= 0) {
    // bypass, no need to switch
  } else if (OB_FAIL(build_pg_array_leader_candidates(pre_switch_index_array, partition_arrays))) {
    LOG_WARN("fail ot build pg array leader candidates", K(ret));
  } else if (OB_FAIL(do_build_leader_switch_cursor_container(tenant_id,
                 tenant_unit,
                 pre_switch_index_array,
                 partition_arrays,
                 excluded_zones,
                 excluded_servers,
                 cursor_container))) {
    LOG_WARN("fail to do build leader switch cursor container", K(ret));
  }
  return ret;
}

int ObLeaderCoordinator::build_pg_array_leader_candidates(
    const common::ObIArray<PreSwitchPgInfo>& pre_switch_index_array, common::ObArray<PartitionArray*>& partition_arrays)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(partition_arrays.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_UNLIKELY(nullptr == srv_rpc_proxy_ || nullptr == config_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("svr_rpc_proxy_ ptr is null", K(ret));
  } else {
    GetLeaderCandidatesAsyncV2Operator async_rpc_operator(
        *srv_rpc_proxy_, &ObSrvRpcProxy::get_leader_candidates_async_v2, *config_);
    if (OB_FAIL(async_rpc_operator.init())) {
      LOG_WARN("fail to init get candidate rpc operator", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < pre_switch_index_array.count(); ++i) {
        const PreSwitchPgInfo& pg_info = pre_switch_index_array.at(i);
        PartitionArray* partition_array = nullptr;
        if (OB_FAIL(check_cancel())) {
          LOG_WARN("fail to check cancel, maybe rs is stopped", K(ret));
        } else if (OB_UNLIKELY(pg_info.index_ >= partition_arrays.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected pg info index",
              K(ret),
              "pg_index",
              pg_info.index_,
              "partition array count",
              partition_arrays.count());
        } else if (nullptr == (partition_array = partition_arrays.at(pg_info.index_))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("partition array ptr is null", K(ret), "pg_index", pg_info.index_);
        } else if (OB_FAIL(build_single_pg_leader_candidates(
                       async_rpc_operator, pg_info.index_, *partition_array, partition_arrays))) {
          LOG_WARN("fail to build single pg leader candidates v2", K(ret));
        }
      }  // end of for
    }

    // Maybe a few requests have been hoarded and have not been sent out
    // need to deal with them here.
    if (OB_SUCC(ret)) {
      if (OB_FAIL(async_rpc_operator.finally_send_requests(partition_arrays))) {
        LOG_WARN("fail to batch send rpc request", K(ret));
      }
    }
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = async_rpc_operator.wait())) {
      LOG_WARN("proxy wait failed", K(ret), K(tmp_ret));
      ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
    } else if (OB_SUCC(ret)) {  // wait() SUCC and the above process SUCC
      if (OB_FAIL(async_rpc_operator.fill_partitions_leader_candidates(partition_arrays))) {
        LOG_WARN("fail to fill partitions leader candidates", K(ret));
      }
    } else {
    }  // wait() SUCC however the above process FAIL, do nothing
  }
  return ret;
}

int ObLeaderCoordinator::coordinate_partition_arrays(ExpectedLeaderWaitOperator& leader_wait_operator,
    ObArray<PartitionArray*>& partition_arrays, TenantUnit& tenant_unit, CursorContainer& cursor_container,
    const bool force)
{
  int ret = OB_SUCCESS;
  // excluded_zones and excluded_servers can be empty
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (partition_arrays.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("partition_arrays is empty", K(partition_arrays), K(ret));
  } else {
    ConcurrentSwitchLeaderStrategy switch_strategy(*this, partition_arrays, cursor_container);
    if (OB_FAIL(switch_strategy.execute(tenant_unit, leader_wait_operator, force))) {
      LOG_WARN("fail to execute switch", K(ret));
    } else if (OB_FAIL(leader_wait_operator.finally_wait())) {
      LOG_WARN("leader wait operator try wait failed", K(ret));
    }
    ret = (OB_SUCCESS == ret) ? leader_wait_operator.get_wait_ret() : ret;
  }
  return ret;
}

int ObLeaderCoordinator::try_update_switch_leader_event(const uint64_t tenant_id, const uint64_t tg_id,
    const int64_t partition_id, const common::ObAddr& advised_leader, const common::ObAddr& current_leader)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id) || OB_UNLIKELY(partition_id < 0) ||
             OB_UNLIKELY(!advised_leader.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(partition_id), K(advised_leader));
  } else {
    ROOTSERVICE_EVENT_ADD("leader_coordinator",
        "switch_leader",
        "current_rs",
        config_->self_addr_,
        "tenant_id",
        tenant_id,
        "tablegroup_id",
        tg_id,
        "partition_id",
        partition_id,
        "advised_leader",
        advised_leader,
        "current_leader",
        current_leader);
  }
  return ret;
}

uint32_t ObLeaderCoordinator::get_random()
{
  const int64_t min = 0;
  const int64_t max = INT32_MAX;
  return static_cast<uint32_t>(ObRandom::rand(min, max));
}

int ObLeaderCoordinator::get_cur_partition_leader(
    const ObIArray<Partition>& partitions, common::ObAddr& leader, bool& has_same_leader)
{
  int ret = OB_SUCCESS;
  ObAddr first_leader;
  ObAddr tmp_leader;

  leader.reset();
  has_same_leader = true;

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(partitions.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("partitions must not empty", K(ret), K(partitions));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && has_same_leader && i < partitions.count(); ++i) {
      if (OB_FAIL(partitions.at(i).get_leader(tmp_leader))) {
        if (OB_LEADER_NOT_EXIST != ret) {
          LOG_WARN("failed to get leader", K(ret));
        } else {
          ret = OB_SUCCESS;
          has_same_leader = false;
        }
      } else if (0 == i) {
        first_leader = tmp_leader;
      } else if (tmp_leader != first_leader) {
        has_same_leader = false;
      }
    }
  }

  if (OB_SUCC(ret) && has_same_leader) {
    if (!first_leader.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("leader must not valid", K(ret));
    } else {
      leader = first_leader;
    }
  }

  if (OB_FAIL(ret)) {
    has_same_leader = false;
  }
  return ret;
}

int ObLeaderCoordinator::update_candidate_leader_info(const Partition& partition,
    const share::ObPartitionReplica& replica, const int64_t replica_index, const TenantUnit& tenant_unit,
    CandidateLeaderInfo& info, int64_t& zone_migrate_out_or_transform_count, bool& is_ignore_switch_percent)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(nullptr == partition.info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("info ptr is null", K(ret));
  } else if (replica_index >= partition.info_->replica_count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(replica_index), "replica_count", partition.info_->replica_count());
  } else {
    if (replica.is_original_leader_) {
      ++info.original_leader_count_;
    }
    if (partition.info_->is_leader_like(replica_index)) {
      ++info.cur_leader_count_;
    }

    bool is_in_normal_unit = false;
    if (OB_FAIL(check_in_normal_unit(replica, tenant_unit, is_in_normal_unit))) {
      LOG_WARN("failed to check in normal unit", K(ret));
    } else if (is_in_normal_unit) {
      const ObPartitionReplica* leader_replica = NULL;
      int tmp_ret = partition.info_->find_leader_by_election(leader_replica);
      if (OB_ENTRY_NOT_EXIST == tmp_ret) {
        // Partitions without a leader are not recorded in normal_unit_count
      } else if (OB_SUCCESS != tmp_ret) {
        // get leader failed
      } else if (NULL == leader_replica) {
        // bypass, do not record into normal_unit_count
      } else {
        ++info.in_normal_unit_count_;
      }
    } else if (replica.is_leader_by_election()) {
      // When the main unit is not in the normal unit,
      // it needs to be forced to switch
      is_ignore_switch_percent = true;
    }

    if (OB_SUCC(ret)) {
      ObPartitionKey pkey;
      bool has = false;
      if (OB_FAIL(partition.get_partition_key(pkey))) {
        LOG_WARN("fail to get partition", K(ret), "partition", partition);
      } else if (OB_FAIL(rebalance_task_mgr_->has_in_migrate_or_transform_task_info(pkey, replica.server_, has))) {
        LOG_WARN("check has in migrating out rebalance task failed", K(ret), K(pkey), "server", replica.server_);
      } else if (has) {
        info.migrate_out_or_transform_count_++;
        zone_migrate_out_or_transform_count++;
      } else {
      }  // do nothing
    }
  }
  return ret;
}

int ObLeaderCoordinator::build_partition_statistic(const PartitionArray& partitions, const TenantUnit& tenant_unit,
    const ObZoneList& excluded_zones, const common::ObIArray<common::ObAddr>& excluded_servers,
    CandidateLeaderInfoMap& leader_info_map, bool& is_ignore_switch_percent)
{
  int ret = OB_SUCCESS;
  CandidateLeaderInfo info;
  const int64_t bucket_num = 64;
  const int overwrite_flag = 1;
  is_ignore_switch_percent = false;
  ObRandomZoneSelector random_selector;
  ObSEArray<ObRawPrimaryZoneUtil::ZoneScore, MAX_ZONE_NUM, ObNullAllocator> zone_score_array;
  // we deem region num is no more than zone num, so max zone num is enough
  ObSEArray<ObRawPrimaryZoneUtil::RegionScore, MAX_ZONE_NUM, ObNullAllocator> region_score_array;
  // Used to count the number of migrate on the zone,
  // which is the sum of the migrate count of all observers on the zone
  ZoneMigrateCountMap zone_migrate_count_map;
  const common::ObAddr& advised_leader = partitions.get_advised_leader();

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(partitions.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid partitions", K(ret));
  } else if (OB_UNLIKELY(nullptr == partitions.at(0).info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("info ptr is null", K(ret));
  } else if (OB_FAIL(random_selector.init(*zone_mgr_))) {
    LOG_WARN("init random zone selector failed", K(ret));
  } else if (OB_FAIL(random_selector.update_score(  // small tenant partition_id filled 0
                 partitions.at(0).tg_id_,
                 (extract_tenant_id(partitions.at(0).tg_id_) > 0 ? partitions.at(0).info_->get_partition_id() : 0)))) {
    LOG_WARN("update random zone selector score failed", K(ret));
  } else if (OB_FAIL(leader_info_map.reuse())) {
    LOG_WARN("failed to create leader info map", K(ret));
  } else if (OB_FAIL(zone_migrate_count_map.create(bucket_num, ObModIds::OB_RS_LEADER_COORDINATOR))) {
    LOG_WARN("failed to create zone migrate count map", K(ret));
  } else if (OB_FAIL(
                 build_zone_region_score_array(partitions.get_primary_zone(), zone_score_array, region_score_array))) {
    LOG_WARN("fail to build zone region score array", K(ret), "primary zone", partitions.get_primary_zone());
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < partitions.count(); ++i) {
      const ObPartitionInfo::ReplicaArray& replicas = partitions.at(i).info_->get_replicas_v2();
      for (int64_t j = 0; OB_SUCC(ret) && j < replicas.count(); ++j) {
        const ObPartitionReplica& replica = replicas.at(j);
        const ObZone& replica_zone = replica.zone_;
        int64_t zone_migrate_out_or_transform_cnt = 0;
        if (!replica.is_valid() || replica_zone.is_empty()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("replica must be valid, cannot build partition statistic", K(ret), K(replica));
        } else if (!ObReplicaTypeCheck::is_can_elected_replica(replica.replica_type_)) {
          // Cannot be selected as leader, skip this type of replica
        } else if (OB_FAIL(leader_info_map.locate_candidate_leader_info(advised_leader,
                       replica,
                       zone_score_array,
                       region_score_array,
                       random_selector,
                       excluded_zones,
                       excluded_servers,
                       info))) {
          LOG_WARN("fail to locate candidate leader info", K(ret));
        } else if (OB_FAIL(zone_migrate_count_map.locate(replica_zone, zone_migrate_out_or_transform_cnt))) {
          LOG_WARN("fail to locate zone migrate or transform cnt", K(ret));
        } else if (OB_FAIL(update_candidate_leader_info(partitions.at(i),
                       replica,
                       j,
                       tenant_unit,
                       info,
                       zone_migrate_out_or_transform_cnt,
                       is_ignore_switch_percent))) {
          LOG_WARN("fail to update candidate leader info", K(ret));
        } else if (OB_FAIL(leader_info_map.set_refactored(replica.server_, info, overwrite_flag))) {
          LOG_WARN("failed to set leader info", K(ret));
        } else if (OB_FAIL(zone_migrate_count_map.set_refactored(
                       replica_zone, zone_migrate_out_or_transform_cnt, overwrite_flag))) {
          LOG_WARN("fail to set map", K(ret));
        }
      }  // end for replicas
    }    // end for partitons
    for (CandidateLeaderInfoMap::iterator iter = leader_info_map.begin(); OB_SUCC(ret) && iter != leader_info_map.end();
         ++iter) {
      const ObZone& zone = iter->second.zone_;
      int64_t zone_cnt = 0;
      if (OB_FAIL(zone_migrate_count_map.get_refactored(zone, zone_cnt))) {
        LOG_WARN("fail to get from map", K(ret));
      } else if (zone_cnt < 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("zone candidate count value unexpected", K(ret), K(zone_cnt));
      } else {
        iter->second.zone_migrate_out_or_transform_count_ = zone_cnt;
      }
    }
  }
  return ret;
}

int ObLeaderCoordinator::build_zone_region_score_array(const ObZone& primary_zone,
    common::ObIArray<share::ObRawPrimaryZoneUtil::ZoneScore>& zone_score_array,
    common::ObIArray<share::ObRawPrimaryZoneUtil::RegionScore>& region_score_array) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(nullptr == zone_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("zone_mgr_ ptr is null", K(ret));
  } else {
    ObRawPrimaryZoneUtil builder(*zone_mgr_);
    if (OB_FAIL(builder.build(primary_zone, zone_score_array, region_score_array))) {
      LOG_WARN("fail to build zone region array", K(ret), K(primary_zone));
    } else {
    }  // good
  }
  return ret;
}

int ObLeaderCoordinator::CandidateLeaderInfoMap::build_candidate_basic_statistic(const common::ObAddr& server_addr,
    const common::ObAddr& advised_leader, const common::ObIArray<share::ObRawPrimaryZoneUtil::ZoneScore>& zone_score,
    const common::ObIArray<share::ObRawPrimaryZoneUtil::RegionScore>& region_score,
    const ObRandomZoneSelector& random_selector, const ObZoneList& excluded_zones,
    const common::ObIArray<common::ObAddr>& excluded_servers, CandidateLeaderInfo& candidate_leader_info)
{
  int ret = OB_SUCCESS;
  // advised leader may be invalid, no need to check valid
  if (OB_UNLIKELY(!server_addr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(server_addr));
  } else if (OB_FAIL(build_candidate_zone_region_score(server_addr, zone_score, region_score, candidate_leader_info))) {
    LOG_WARN("fail to build candidate zone regioin score", K(ret));
  } else if (OB_FAIL(build_candidate_server_zone_status(
                 server_addr, excluded_zones, excluded_servers, candidate_leader_info))) {
    LOG_WARN("fail to build server zone status", K(ret));
  } else if (OB_FAIL(build_candidate_balance_group_score(server_addr, advised_leader, candidate_leader_info))) {
    LOG_WARN("fail to build candidate balance group score", K(ret));
  } else if (OB_FAIL(build_candidate_server_random_score(server_addr, random_selector, candidate_leader_info))) {
    LOG_WARN("fail to build candidate server random score", K(ret));
  } else {
    candidate_leader_info.server_addr_ = server_addr;
  }
  return ret;
}

int ObLeaderCoordinator::CandidateLeaderInfoMap::build_candidate_balance_group_score(
    const common::ObAddr& server_addr, const common::ObAddr& advised_leader, CandidateLeaderInfo& candidate_leader_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!server_addr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(server_addr));
  } else {
    if (server_addr == advised_leader) {
      candidate_leader_info.balance_group_score_ = 0;
    } else {
      candidate_leader_info.balance_group_score_ = INT64_MAX;
    }
  }
  return ret;
}

int ObLeaderCoordinator::CandidateLeaderInfoMap::build_candidate_server_random_score(const common::ObAddr& server_addr,
    const ObRandomZoneSelector& random_selector, CandidateLeaderInfo& candidate_leader_info)
{
  int ret = OB_SUCCESS;
  ObZone zone;
  int64_t score = INT64_MAX;
  if (OB_UNLIKELY(NULL == host_.server_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("server mgr null", K(ret), KP(host_.server_mgr_));
  } else if (OB_FAIL(host_.server_mgr_->get_server_zone(server_addr, zone))) {
    LOG_WARN("get server zone failed", K(ret), K(server_addr));
  } else if (OB_FAIL(random_selector.get_zone_score(zone, score))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      candidate_leader_info.random_score_ = INT64_MAX;
    } else {
      LOG_WARN("fail to get zone score", K(ret));
    }
  } else {
    candidate_leader_info.random_score_ = score;
  }
  return ret;
}

int ObLeaderCoordinator::CandidateLeaderInfoMap::build_candidate_server_zone_status(const common::ObAddr& server_addr,
    const ObZoneList& excluded_zones, const common::ObIArray<common::ObAddr>& excluded_servers,
    CandidateLeaderInfo& info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!server_addr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_UNLIKELY(NULL == host_.server_mgr_ || NULL == host_.zone_mgr_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("leade coordinator not init", K(ret));
  } else {
    HEAP_VAR(ObZoneInfo, zone_info)
    {
      ObServerStatus server_status;
      bool zone_active = false;
      if (OB_FAIL(host_.server_mgr_->get_server_zone(server_addr, zone_info.zone_))) {
        LOG_WARN("get server status failed", K(ret), K(server_addr));
      } else if (OB_FAIL(host_.zone_mgr_->get_zone(zone_info))) {
        LOG_WARN("fail to check zone active", K(ret));
      } else if (OB_FAIL(host_.server_mgr_->get_server_status(server_addr, server_status))) {
        LOG_WARN("fail to get server status", K(ret), K(server_addr));
      } else if (OB_FAIL(host_.zone_mgr_->check_zone_active(server_status.zone_, zone_active))) {
        LOG_WARN("fail to check zone active", K(ret));
      } else {
        info.is_candidate_ = !server_status.is_stopped() && zone_active;
        info.not_excluded_ = !has_exist_in_array(excluded_servers, server_status.server_) &&
                             !has_exist_in_array(excluded_zones, server_status.zone_);
        info.not_merging_ =
            !zone_info.is_merging_ || zone_info.suspend_merging_ || !host_.zone_mgr_->is_stagger_merge();
        info.start_service_ = (server_status.start_service_time_ > 0);
      }
    }
  }
  return ret;
}

int ObLeaderCoordinator::CandidateLeaderInfoMap::build_candidate_zone_region_score(const common::ObAddr& server_addr,
    const common::ObIArray<share::ObRawPrimaryZoneUtil::ZoneScore>& zone_score_array,
    const common::ObIArray<share::ObRawPrimaryZoneUtil::RegionScore>& region_score_array,
    CandidateLeaderInfo& candidate_leader_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!server_addr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(server_addr));
  } else if (OB_UNLIKELY(NULL == host_.server_mgr_ || NULL == host_.zone_mgr_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("leader coordinator not init", K(ret));
  } else {
    common::ObZone server_zone;
    common::ObRegion server_region;
    share::ObServerStatus server_status;
    HEAP_VAR(share::ObZoneInfo, zone_info)
    {
      if (OB_FAIL(host_.server_mgr_->get_server_status(server_addr, server_status))) {
        LOG_WARN("fail to get server status", K(ret));
      } else {
        server_zone = server_status.zone_;
        zone_info.zone_ = server_zone;
        if (OB_FAIL(host_.zone_mgr_->get_zone(zone_info))) {
          LOG_WARN("fail to get zone", K(ret), K(server_zone));
        } else if (OB_FAIL(server_region.assign(zone_info.region_.info_.ptr()))) {
          LOG_WARN("fail to assign region", K(ret));
        } else {
        }  // got zone and region
      }
      if (OB_SUCC(ret)) {
        for (int64_t i = 0; i < zone_score_array.count(); ++i) {
          if (server_zone == zone_score_array.at(i).zone_) {
            candidate_leader_info.zone_score_ = zone_score_array.at(i);
            break;
          } else {
          }  // go on find
        }
        for (int64_t i = 0; i < region_score_array.count(); ++i) {
          if (server_region == region_score_array.at(i).region_) {
            candidate_leader_info.region_score_ = region_score_array.at(i);
            break;
          } else {
          }  // go on find
        }
      }
    }
  }
  return ret;
}

int ObLeaderCoordinator::CandidateLeaderInfoMap::locate_candidate_leader_info(const common::ObAddr& advised_leader,
    const share::ObPartitionReplica& replica,
    const common::ObIArray<share::ObRawPrimaryZoneUtil::ZoneScore>& zone_score,
    const common::ObIArray<share::ObRawPrimaryZoneUtil::RegionScore>& region_score,
    const ObRandomZoneSelector& random_selector, const ObZoneList& excluded_zones,
    const common::ObIArray<common::ObAddr>& excluded_servers, CandidateLeaderInfo& candidate_leader_info)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(get_refactored(replica.server_, candidate_leader_info))) {
    // got it, good
  } else if (OB_HASH_NOT_EXIST == ret) {
    ret = OB_SUCCESS;
    candidate_leader_info.reset();
    candidate_leader_info.zone_ = replica.zone_;
    candidate_leader_info.partition_id_ = replica.partition_id_;
    if (OB_FAIL(build_candidate_basic_statistic(replica.server_,
            advised_leader,
            zone_score,
            region_score,
            random_selector,
            excluded_zones,
            excluded_servers,
            candidate_leader_info))) {
      LOG_WARN("fail to build zone region score", K(ret));
    }
  } else {
    LOG_WARN("fail to get leader info from raw map", K(ret));
  }
  return ret;
}

int ObLeaderCoordinator::ZoneMigrateCountMap::locate(const common::ObZone& zone, int64_t& count)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(get_refactored(zone, count))) {
    // got it, good
  } else if (OB_HASH_NOT_EXIST == ret) {
    ret = OB_SUCCESS;
    count = 0;
  } else {
    LOG_WARN("fail to get count from map", K(ret), K(zone));
  }
  return ret;
}

int ObLeaderCoordinator::get_high_score_regions(
    const common::ObIArray<share::ObRawPrimaryZoneUtil::RegionScore>& region_score_array,
    common::ObIArray<common::ObRegion>& high_score_regions)
{
  int ret = OB_SUCCESS;
  high_score_regions.reset();
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (region_score_array.count() <= 0) {
    // bypass
  } else {
    const share::ObRawPrimaryZoneUtil::RegionScore& sample = region_score_array.at(0);
    for (int64_t i = 0; OB_SUCC(ret) && i < region_score_array.count(); ++i) {
      const share::ObRawPrimaryZoneUtil::RegionScore& this_one = region_score_array.at(i);
      if (this_one.region_score_ != sample.region_score_) {
        break;
      } else {
        if (OB_FAIL(high_score_regions.push_back(this_one.region_))) {
          LOG_WARN("fail to push back", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObLeaderCoordinator::get_partition_prep_candidates(const common::ObAddr& leader,
    const common::ObIArray<share::ObRawPrimaryZoneUtil::ZoneScore>& zone_score_array,
    const common::ObIArray<share::ObRawPrimaryZoneUtil::RegionScore>& region_score_array, Partition& partition,
    ObIArray<common::ObAddr>& prep_partitions)
{
  int ret = OB_SUCCESS;
  common::ObZone leader_zone;
  common::ObRegion leader_region;
  common::ObArray<common::ObRegion> high_score_regions;
  prep_partitions.reset();
  UNUSED(zone_score_array);
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!leader.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(leader));
  } else if (OB_UNLIKELY(NULL == server_mgr_ || NULL == zone_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("server mgr ptr is null", K(ret), KP(server_mgr_));
  } else if (OB_UNLIKELY(nullptr == partition.info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("info ptr is null", K(ret));
  } else if (OB_FAIL(get_high_score_regions(region_score_array, high_score_regions))) {
    LOG_WARN("fail to get high score regions", K(ret));
  } else if (OB_FAIL(server_mgr_->get_server_zone(leader, leader_zone))) {
    LOG_WARN("fail to get leader zone", K(ret), K(leader));
  } else if (OB_FAIL(zone_mgr_->get_region(leader_zone, leader_region))) {
    LOG_WARN("fail to get leader region", K(ret), K(leader_zone));
  } else if (!has_exist_in_array(high_score_regions, leader_region)) {
    // The region where the leader is located is not in the region with the highest priority,
    // so fill all the full replicas in the member list into the prep_partitions array.
    for (int64_t i = 0; OB_SUCC(ret) && i < partition.info_->get_replicas_v2().count(); ++i) {
      const share::ObPartitionReplica& replica = partition.info_->get_replicas_v2().at(i);
      if (replica.is_in_service() && REPLICA_TYPE_FULL == replica.replica_type_) {
        if (OB_FAIL(prep_partitions.push_back(replica.server_))) {
          LOG_WARN("fail to push back", K(ret));
        }
      } else {
      }  // not a full replica in member list
    }
  } else {
    // The region where the leader is located is included in the regions with the highest priority,
    // and the server in the regions with the highest priority is filled into the prep_partitions array.
    for (int64_t i = 0; OB_SUCC(ret) && i < partition.info_->get_replicas_v2().count(); ++i) {
      const share::ObPartitionReplica& replica = partition.info_->get_replicas_v2().at(i);
      common::ObZone this_zone;
      common::ObRegion this_region;
      if (!replica.is_in_service() || REPLICA_TYPE_FULL != replica.replica_type_) {
        // not in member list or not a full replica
      } else if (OB_FAIL(server_mgr_->get_server_zone(replica.server_, this_zone))) {
        LOG_WARN("fail to get server zone", K(ret), "server", replica.server_);
      } else if (OB_FAIL(zone_mgr_->get_region(this_zone, this_region))) {
        LOG_WARN("fail to get server region", K(ret), K(this_zone));
      } else if (!has_exist_in_array(high_score_regions, this_region)) {
        // bypass, since this region do no exist in high score regions
      } else if (OB_FAIL(prep_partitions.push_back(replica.server_))) {
        LOG_WARN("fail to push back", K(ret));
      } else {
      }  // no more to do
    }
    if (OB_FAIL(ret)) {
    } else if (prep_partitions.count() > 0) {
      // already has member in prep partitions
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < partition.info_->get_replicas_v2().count(); ++i) {
        const share::ObPartitionReplica& replica = partition.info_->get_replicas_v2().at(i);
        if (replica.is_in_service() && REPLICA_TYPE_FULL == replica.replica_type_) {
          if (OB_FAIL(prep_partitions.push_back(replica.server_))) {
            LOG_WARN("fail to push back", K(ret));
          }
        } else {
        }  // not a full replica in member list
      }
    }
  }
  return ret;
}

bool ObLeaderCoordinator::prep_candidates_match(
    ObArray<common::ObAddr>& prev_prep_candidates, ObSArray<common::ObAddr>& this_prep_candidates)
{
  bool match = true;
  if (prev_prep_candidates.count() != this_prep_candidates.count()) {
    match = false;
  } else {
    std::sort(prev_prep_candidates.begin(), prev_prep_candidates.end());
    std::sort(this_prep_candidates.begin(), this_prep_candidates.end());
    for (int64_t i = 0; i < prev_prep_candidates.count() && match; ++i) {
      common::ObAddr& prev_one = prev_prep_candidates.at(i);
      common::ObAddr& this_one = this_prep_candidates.at(i);
      match = (prev_one == this_one);
    }
  }
  return match;
}

int ObLeaderCoordinator::build_single_pg_leader_candidates(GetLeaderCandidatesAsyncV2Operator& async_rpc_operator,
    const int64_t in_array_index, PartitionArray& partitions, common::ObIArray<PartitionArray*>& partition_arrays)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawPrimaryZoneUtil::ZoneScore, MAX_ZONE_NUM, ObNullAllocator> zone_score_array;
  ObSEArray<ObRawPrimaryZoneUtil::RegionScore, MAX_ZONE_NUM, ObNullAllocator> region_score_array;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(partitions.count() <= 0 || in_array_index < 0 || in_array_index >= partition_arrays.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument",
        K(ret),
        K(partitions),
        K(in_array_index),
        "partition arrays count",
        partition_arrays.count());
  } else if (OB_FAIL(
                 build_zone_region_score_array(partitions.get_primary_zone(), zone_score_array, region_score_array))) {
    LOG_WARN("fail to build zone region score array", K(ret), "primary zone", partitions.get_primary_zone());
  } else {
    common::ObArray<common::ObAddr> this_prep_candidates;
    for (int64_t i = 0; OB_SUCC(ret) && i < partitions.count(); ++i) {
      Partition& partition = partitions.at(i);
      ObPartitionKey pkey;
      ObAddr this_leader;
      if (OB_FAIL(partition.get_partition_key(pkey))) {
        LOG_WARN("fail to get partiiton key", K(ret), K(partition));
      } else if (OB_UNLIKELY(!pkey.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pkey of this partition is invalid", K(ret), K(pkey), K(partition));
      } else if (OB_FAIL(partition.get_leader(this_leader))) {
        if (OB_LEADER_NOT_EXIST != ret) {
          LOG_WARN("fail to get partition leader", K(ret), K(partition));
        } else {
          ret = OB_SUCCESS;  // rewrite ret to OB_SUCCESS, and record warn log
          LOG_WARN("partition doesn't have leader", K(pkey));
        }
      } else if (OB_UNLIKELY(!this_leader.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("leader of this partition is invalid", K(ret), K(partition), K(this_leader));
      } else if (OB_FAIL(get_partition_prep_candidates(
                     this_leader, zone_score_array, region_score_array, partition, this_prep_candidates))) {
        LOG_WARN("fail to get partition prep_candidates", K(ret), K(partition));
      } else if (this_prep_candidates.count() <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected prep candidates", K(ret), K(partition));
      } else {
        if (OB_FAIL(async_rpc_operator.process_request(
                pkey, this_leader, this_prep_candidates, in_array_index, i, partition_arrays))) {
          LOG_WARN("fail to process get leader candidate request", K(ret), K(pkey), K(this_leader));
        } else if (async_rpc_operator.reach_send_to_wait_threshold()) {
          if (OB_FAIL(async_rpc_operator.wait())) {
            LOG_WARN("proxy wait failed", K(ret));
          } else if (OB_FAIL(async_rpc_operator.fill_partitions_leader_candidates(partition_arrays))) {
            LOG_WARN("fail to fill partitons leader candidates", K(ret));
          } else {
            async_rpc_operator.clear_after_wait();
          }
        }
      }
    }  // end of for
  }
  return ret;
}

int ObLeaderCoordinator::update_specific_candidate_statistic(const common::ObAddr& server, const common::ObZone& zone,
    const Partition& partition, CandidateLeaderInfo& info, CandidateLeaderInfoMap& leader_info_map,
    ZoneCandidateCountMap& zone_candidate_count_map)
{
  int ret = OB_SUCCESS;
  const int overwrite_flag = 1;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!server.is_valid() || zone.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(server), K(zone));
  } else {
    bool in_blacklist = false;
    int64_t zone_candidate_count = 0;
    if (OB_FAIL(leader_info_map.get_refactored(server, info))) {
      LOG_WARN("failed to get leader info from map", K(ret), K(server));
    } else if (OB_FAIL(check_in_election_revoke_blacklist(server, partition, in_blacklist))) {
      LOG_WARN("fail to check in election revoke blacklist", K(ret), K(server));
    } else {
      ++info.candidate_count_;
      info.in_revoke_blacklist_count_ =
          in_blacklist ? info.in_revoke_blacklist_count_ + 1 : info.in_revoke_blacklist_count_;
      if (OB_FAIL(leader_info_map.set_refactored(server, info, overwrite_flag))) {
        LOG_WARN("failed to set leader info", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_SUCC(zone_candidate_count_map.get_refactored(zone, zone_candidate_count))) {
        // got it, good
      } else if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        zone_candidate_count = 0;
      }
    }
    if (OB_SUCC(ret)) {
      ++zone_candidate_count;
      if (OB_FAIL(zone_candidate_count_map.set_refactored(zone, zone_candidate_count, overwrite_flag))) {
        LOG_WARN("fail to set zone candidate", K(ret));
      }
    }
  }
  return ret;
}

int ObLeaderCoordinator::check_in_election_revoke_blacklist(
    const common::ObAddr& server, const Partition& partition, bool& in_blacklist)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(!server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(server));
  } else if (partition.candidate_status_array_.count() <= 0) {
    // Compatible processing, compatible with the version without candidate_status_array
    in_blacklist = false;
  } else if (partition.candidates_.count() != partition.candidate_status_array_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("array count unexpected",
        K(ret),
        "candidate_cnt",
        partition.candidates_.count(),
        "status_cnt",
        partition.candidate_status_array_.count());
  } else {
    in_blacklist = false;
    for (int64_t i = 0; i < partition.candidates_.count(); ++i) {
      const common::ObAddr& this_server = partition.candidates_.at(i);
      if (server == this_server) {
        in_blacklist = partition.candidate_status_array_.at(i).get_in_black_list();
        break;  // server found, break here
      } else {
        // server not match, bypass
      }
    }
  }
  return ret;
}

int ObLeaderCoordinator::update_leader_candidates(
    const ObIArray<Partition>& partitions, CandidateLeaderInfoMap& leader_info_map)
{
  int ret = OB_SUCCESS;
  CandidateLeaderInfo info;
  ObArray<ObAddr> server_list;
  ZoneCandidateCountMap zone_candidate_count_map;
  const int64_t bucket_num = 64;

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(partitions.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("partitions is empty", K(partitions), K(ret));
  } else if (!leader_info_map.created()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("leader_info_map not created", K(ret));
  } else if (OB_FAIL(server_list.reserve(leader_info_map.size()))) {
    LOG_WARN("failed to reserve server list", K(ret));
  } else if (OB_FAIL(zone_candidate_count_map.create(bucket_num, ObModIds::OB_RS_LEADER_COORDINATOR))) {
    LOG_WARN("failed to create zone candidate count map", K(ret));
  } else {
    for (CandidateLeaderInfoMap::const_iterator iter = leader_info_map.begin();
         OB_SUCC(ret) && iter != leader_info_map.end();
         ++iter) {
      const ObAddr server = iter->first;
      if (OB_FAIL(server_list.push_back(server))) {
        LOG_WARN("failed to add server list", K(ret));
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < server_list.count(); ++i) {
      const int overwrite_flag = 1;
      if (OB_FAIL(leader_info_map.get_refactored(server_list.at(i), info))) {
        LOG_WARN("failed to get leader info from map", K(ret), K(server_list.at(i)));
      } else {
        info.candidate_count_ = 0;
        info.in_revoke_blacklist_count_ = 0;
        if (OB_FAIL(leader_info_map.set_refactored(server_list.at(i), info, overwrite_flag))) {
          LOG_WARN("failed to set leader info", K(ret));
        }
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < partitions.count(); ++i) {
      if (OB_UNLIKELY(nullptr == partitions.at(i).info_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("info ptr is null", K(ret));
      } else {
        const ObPartitionInfo::ReplicaArray& replicas = partitions.at(i).info_->get_replicas_v2();
        for (int64_t j = 0; OB_SUCC(ret) && j < replicas.count(); ++j) {
          const ObPartitionReplica& replica = replicas.at(j);
          const ObZone& replica_zone = replica.zone_;
          if (!replica.is_valid() || replica_zone.is_empty()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("replica must be valid, cannot build partition statistic", K(ret), K(replica));
          } else if (has_exist_in_array(partitions.at(i).candidates_, replica.server_)) {
            if (OB_FAIL(update_specific_candidate_statistic(replica.server_,
                    replica_zone,
                    partitions.at(i),
                    info,
                    leader_info_map,
                    zone_candidate_count_map))) {
              LOG_WARN("fail to update specific cnadidate statistic", K(ret));
            }
          } else {
          }  // ignore
        }
      }
    }
    for (CandidateLeaderInfoMap::iterator iter = leader_info_map.begin(); OB_SUCC(ret) && iter != leader_info_map.end();
         ++iter) {
      const ObZone& zone = iter->second.zone_;
      int64_t zone_candidate_count = 0;
      ret = zone_candidate_count_map.get_refactored(zone, zone_candidate_count);
      if (OB_SUCCESS == ret) {
        if (zone_candidate_count < 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("zone candidate count value unexpected", K(ret), K(zone_candidate_count));
        } else {
          iter->second.zone_candidate_count_ = zone_candidate_count;
        }
      } else if (OB_HASH_NOT_EXIST == ret) {
        iter->second.zone_candidate_count_ = 0;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to get from map", K(ret));
      }
    }
  }
  return ret;
}

int ObLeaderCoordinator::choose_leader(const uint64_t tenant_id, const uint64_t tablegroup_id,
    const CandidateLeaderInfoMap& leader_info_map, common::ObIArray<common::ObAddr>& candidate_leaders,
    const common::ObAddr& cur_leader)
{
  int ret = OB_SUCCESS;

  candidate_leaders.reset();
  bool is_all_server_candidates_num_same = true;
  int64_t partition_id = 0;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!leader_info_map.created() || OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(tenant_id), K(leader_info_map.created()));
  } else if (OB_LOG_NEED_TO_PRINT(INFO)) {
    int64_t first_candidates_num = -1;
    for (CandidateLeaderInfoMap::const_iterator iter = leader_info_map.begin();
         OB_SUCC(ret) && iter != leader_info_map.end();
         ++iter) {
      const CandidateLeaderInfo& info = iter->second;
      if (first_candidates_num < 0) {
        first_candidates_num = info.candidate_count_;
      } else if (first_candidates_num != info.candidate_count_) {
        is_all_server_candidates_num_same = false;
        break;
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObArray<CandidateLeaderInfo> candidate_array;
    for (CandidateLeaderInfoMap::const_iterator iter = leader_info_map.begin();
         OB_SUCC(ret) && iter != leader_info_map.end();
         ++iter) {
      if (OB_FAIL(candidate_array.push_back(iter->second))) {
        LOG_WARN("fail to push back", K(ret));
      } else {
      }
    }
    ChooseLeaderCmp cmp_operator(tenant_id, tablegroup_id);
    std::sort(candidate_array.begin(), candidate_array.end(), cmp_operator);
    if (OB_FAIL(cmp_operator.get_ret())) {
      LOG_WARN("fail to choose leader", K(ret));
    } else if (OB_UNLIKELY(candidate_array.count() <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("no candidate exists", K(ret));
    } else if (OB_SYS_TENANT_ID == tenant_id) {
      const CandidateLeaderInfo& sample_candidate = candidate_array.at(0);
      for (int64_t i = 0; i < candidate_array.count(); ++i) {
        const CandidateLeaderInfo& cmp_candidate = candidate_array.at(i);
        if (cmp_candidate.region_score_ == sample_candidate.region_score_ &&
            cmp_candidate.zone_candidate_count_ == sample_candidate.zone_candidate_count_ &&
            cmp_candidate.start_service_ == sample_candidate.start_service_ &&
            cmp_candidate.is_candidate_ == sample_candidate.is_candidate_) {
          if (cmp_candidate.server_addr_ == config_->self_addr_) {
            const bool self_in_candidate = true;
            switch_info_stat_.set_self_in_candidate(self_in_candidate);
            break;
          } else {
          }  // go on check
        } else {
          break;
        }
      }
    } else {
    }  // do nothing for other tenant_id
    if (OB_FAIL(ret)) {
    } else if (candidate_array.count() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("no candidate exists", K(ret));
    } else if (OB_FAIL(candidate_leaders.push_back(candidate_array.at(0).server_addr_))) {
      LOG_WARN("fail to push back candidate", K(ret));
    } else {
      bool zone_active = false;
      ObServerStatus server_status;
      for (CandidateLeaderInfoMap::const_iterator iter = leader_info_map.begin();
           OB_SUCC(ret) && iter != leader_info_map.end();
           ++iter) {
        const ObAddr server = iter->first;
        const CandidateLeaderInfo& info = iter->second;
        partition_id = info.partition_id_;
        if (OB_FAIL(server_mgr_->get_server_status(server, server_status))) {
          LOG_WARN("get_server_status failed", "server", server, K(ret));
        } else if (OB_FAIL(zone_mgr_->check_zone_active(server_status.zone_, zone_active))) {
          LOG_WARN("check_zone_active failed", "zone", server_status.zone_, K(ret));
        }
        if (OB_SUCC(ret) && candidate_array.at(0).server_addr_ != cur_leader) {
          if (is_all_server_candidates_num_same) {
            LOG_INFO("choose leader info",
                K(tenant_id),
                K(tablegroup_id),
                K(server),
                K(info),
                "is_stopped",
                server_status.is_stopped(),
                "is_zone_active",
                zone_active);
          } else {
            LOG_INFO("choose leader info with not same candidate num",
                K(tenant_id),
                K(tablegroup_id),
                K(server),
                K(info),
                "is_stopped",
                server_status.is_stopped(),
                "is_zone_active",
                zone_active);
          }
        }
      }
      if (candidate_array.at(0).server_addr_ != cur_leader) {
        LOG_INFO("choose leader succ",
            K(tenant_id),
            K(tablegroup_id),
            K(partition_id),
            "candidate leader",
            candidate_array.at(0).server_addr_,
            "curr leader",
            cur_leader);
      }
    }
  }
  return ret;
}

int ObLeaderCoordinator::check_in_normal_unit(
    const ObPartitionReplica& replica, const TenantUnit& tenant_unit, bool& in_normal_unit)
{
  int ret = OB_SUCCESS;
  in_normal_unit = false;

  if (!replica.is_valid() || tenant_unit.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(replica), "tenant_unit count", tenant_unit.count(), K(ret));
  } else {
    if (OB_INVALID_ID != replica.unit_id_) {
      FOREACH_CNT_X(unit, tenant_unit, !in_normal_unit)
      {
        if (replica.unit_id_ == unit->unit_id_ && replica.server_ == unit->server_ &&
            !unit->migrate_from_server_.is_valid() && ObUnit::UNIT_STATUS_ACTIVE == unit->status_) {
          in_normal_unit = true;
        }
      }
    }
  }
  return ret;
}

int ObLeaderCoordinator::remove_lost_replicas(ObPartitionInfo& partition_info)
{
  int ret = OB_SUCCESS;
  ObReplicaFilterHolder filter;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(filter.set_only_alive_server(*server_mgr_))) {
    LOG_WARN("set filter failed", K(ret));
  } else if (OB_FAIL(filter.set_replica_status(REPLICA_STATUS_NORMAL))) {
    LOG_WARN("set filter failed", K(ret));
  } else if (OB_FAIL(filter.set_in_member_list())) {
    LOG_WARN("set in member list", K(ret));
  } else if (!partition_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid partition_info", K(partition_info), K(ret));
  } else if (OB_FAIL(partition_info.filter(filter))) {
    LOG_WARN("partition_info filter failed", K(ret));
  }
  return ret;
}

bool ObLeaderCoordinator::partition_entity_cmp(const IPartitionEntity* l, const IPartitionEntity* r)
{
  bool cmp_ret = false;
  int ret = OB_SUCCESS;
  if (NULL == l || NULL == r) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", "left", OB_P(l), "right", OB_P(r), K(ret));
  } else {
    if (l->get_tablegroup_id() == r->get_tablegroup_id()) {
      cmp_ret = (l->get_partition_entity_id() < r->get_partition_entity_id());
    } else {
      cmp_ret = (l->get_tablegroup_id() < r->get_tablegroup_id());
    }
  }
  return cmp_ret;
}

int ObLeaderCoordinator::check_cancel() const
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (stop_) {
    ret = OB_CANCELED;
  }
  return ret;
}

int ObLeaderCoordinator::move_sys_tenant_to_last(
    const ObIArray<uint64_t>& tenant_ids, ObIArray<uint64_t>& new_tenant_ids)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (tenant_ids.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_ids is empty", K(tenant_ids), K(ret));
  } else if (OB_FAIL(new_tenant_ids.assign(tenant_ids))) {
    LOG_WARN("assign failed", K(tenant_ids), K(ret));
  } else {
    bool has_sys_tenant = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < new_tenant_ids.count(); ++i) {
      if (OB_SYS_TENANT_ID == new_tenant_ids.at(i)) {
        has_sys_tenant = true;
        if (OB_FAIL(new_tenant_ids.remove(i))) {
          LOG_WARN("failed to remove sys tenant id", K(ret), K(i), K(new_tenant_ids));
        }
        break;
      }
    }
    if (OB_SUCC(ret) && has_sys_tenant) {
      if (OB_FAIL(new_tenant_ids.push_back(OB_SYS_TENANT_ID))) {
        LOG_WARN("failed to add sys tenant id", K(ret));
      }
    }
  }
  return ret;
}

int ObLeaderCoordinator::count_leader(const PartitionArray& partitions, const bool is_new_leader)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(partitions.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("partitions is empty", K(partitions), K(ret));
  } else {
    FOREACH_CNT_X(partition, partitions, OB_SUCCESS == ret)
    {
      if (OB_FAIL(count_leader(*partition, is_new_leader))) {
        LOG_WARN("count_leader failed", "partition", *partition, K(ret));
      }
    }
  }
  return ret;
}

int ObLeaderCoordinator::count_leader(const Partition& partition, const bool is_new_leader)
{
  int ret = OB_SUCCESS;
  ObAddr leader;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!partition.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid partition", K(partition), K(ret));
  } else if (OB_FAIL(partition.get_leader(leader))) {
    if (OB_LEADER_NOT_EXIST == ret && !is_new_leader) {
      LOG_INFO("partition has not leader, skip count leader");
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("get_leader failed", K(ret));
    }
  } else if (!leader.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("leader is invalid", K(leader), K(ret));
  } else if (OB_FAIL(switch_info_stat_.inc_leader_count(leader, is_new_leader))) {
    LOG_WARN("count_leader failed", K(leader), K(ret));
  }
  return ret;
}

int ObLeaderCoordinator::update_leader_stat()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    {
      ObLatchWGuard guard(leader_stat_lock_, ObLatchIds::LEADER_STAT_LOCK);
      if (OB_FAIL(switch_info_stat_.copy_leader_stat(leader_stat_))) {
        LOG_WARN("leader_stat assign failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(switch_info_stat_.update_server_leader_cnt_status(server_mgr_))) {
        LOG_WARN("fail to update server leader cnt status", K(ret), KP(server_mgr_));
      }
    }
  }
  return ret;
}

int ObLeaderCoordinator::alloc_partition_array(share::schema::ObSchemaGetterGuard& schema_guard,
    const uint64_t partition_entity_id, const int64_t partition_array_capacity,
    common::ObIAllocator& partition_allocator, PartitionArray*& partitions)
{
  int ret = OB_SUCCESS;
  void* ptr = NULL;
  PartitionArray* tmp_partitions = NULL;
  const share::schema::ObPartitionSchema* partition_schema = NULL;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(partition_array_capacity <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(partition_array_capacity));
  } else if (NULL == (ptr = partition_allocator.alloc(sizeof(PartitionArray)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail to allocate memory for partition array", K(ret));
  } else {
    tmp_partitions = new (ptr) PartitionArray(partition_allocator);
    ObZone primary_zone;
    const share::schema::ObSchemaType schema_type =
        (is_new_tablegroup_id(partition_entity_id) ? share::schema::ObSchemaType::TABLEGROUP_SCHEMA
                                                   : share::schema::ObSchemaType::TABLE_SCHEMA);
    if (OB_UNLIKELY(NULL == tmp_partitions)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, may constrcut PartitionArray failed", K(ret));
    } else if (OB_FAIL(tmp_partitions->init(partition_array_capacity))) {
      LOG_WARN("fail to init partition array", K(ret), K(partition_array_capacity));
    } else if (OB_FAIL(ObPartMgrUtils::get_partition_schema(
                   schema_guard, partition_entity_id, schema_type, partition_schema))) {
      LOG_WARN("fail to get partition schema", K(ret), K(partition_entity_id));
    } else if (OB_UNLIKELY(nullptr == partition_schema)) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("table schema ptr is null", K(ret), K(partition_entity_id));
    } else if (!partition_schema->has_self_partition()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("has no physical partition", K(ret), K(partition_entity_id));
    } else if (OB_FAIL(ObPrimaryZoneUtil::get_pg_integrated_primary_zone(
                   schema_guard, partition_entity_id, primary_zone))) {
      LOG_WARN("fail to assign primary zone to parray", K(ret));
    } else {
      tmp_partitions->set_primary_zone(primary_zone);
      if (!is_tablegroup_id(partition_entity_id)) {
        const share::schema::ObSimpleTableSchemaV2* table_schema = nullptr;
        if (OB_FAIL(schema_guard.get_table_schema(partition_entity_id, table_schema))) {
          LOG_WARN("fail to get table schema", K(ret), "table_id", partition_entity_id);
        } else if (OB_UNLIKELY(nullptr == table_schema)) {
          ret = OB_TABLE_NOT_EXIST;
          LOG_WARN("table schema ptr is null", K(ret), "table_id", partition_entity_id);
        } else {
          tmp_partitions->set_tablegroup_id(table_schema->get_tablegroup_id());
        }
      } else {
        tmp_partitions->set_tablegroup_id(partition_entity_id);
      }
      if (OB_SUCC(ret)) {
        partitions = tmp_partitions;
      }
    }
  }
  return ret;
}

int ObLeaderCoordinator::calculate_partition_array_capacity(ObSchemaGetterGuard& schema_guard,
    const ObIArray<uint64_t>& partition_entity_ids, const bool small_tenant, const int64_t partition_id,
    int64_t& partition_array_capacity)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("leader coordinator not init", K(ret));
  } else if (partition_entity_ids.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", "ids_item_cnt", partition_entity_ids.count(), K(ret));
  } else if (small_tenant && ALL_PARTITIONS == partition_id) {
    // cal partition array capacity when small tenant with all partitions
    partition_array_capacity = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_entity_ids.count(); ++i) {
      const ObPartitionSchema* partition_schema = nullptr;
      const share::schema::ObSchemaType schema_type =
          (is_new_tablegroup_id(partition_entity_ids.at(i)) ? share::schema::ObSchemaType::TABLEGROUP_SCHEMA
                                                            : share::schema::ObSchemaType::TABLE_SCHEMA);
      int64_t part_num = 0;
      if (OB_FAIL(ObPartMgrUtils::get_partition_schema(
              schema_guard, partition_entity_ids.at(i), schema_type, partition_schema))) {
        LOG_WARN("fail to get partition schema", K(ret), "entity_id", partition_entity_ids.at(i));
      } else if (NULL == partition_schema) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("partition schema not exist", "entity_id", partition_entity_ids.at(i), K(ret));
      } else if (!partition_schema->has_self_partition()) {
        // bypass
      } else if (OB_FAIL(partition_schema->get_all_partition_num(true, part_num))) {
        LOG_WARN("fail to get all partition num", K(ret), KPC(partition_schema));
      } else {
        partition_array_capacity += part_num;
      }
    }
  } else {
    partition_array_capacity = partition_entity_ids.count();
  }
  return ret;
}

ObServerSwitchLeaderInfoStat::ServerSwitchLeaderInfo::ServerSwitchLeaderInfo()
    : server_(), new_leader_count_(0), leader_count_(0), partition_count_(0), is_switch_limited_(false)
{}

void ObServerSwitchLeaderInfoStat::ServerSwitchLeaderInfo::reset()
{
  server_.reset();
  new_leader_count_ = 0;
  leader_count_ = 0;
  partition_count_ = 0;
  is_switch_limited_ = false;
}

ObServerSwitchLeaderInfoStat::ObServerSwitchLeaderInfoStat()
    : is_inited_(false),
      info_map_(),
      srv_rpc_proxy_(NULL),
      avg_partition_count_(0),
      is_switch_limited_(false),
      smooth_switch_start_time_(0),
      switch_percent_(0),
      self_in_candidate_(false)
{}

void ObServerSwitchLeaderInfoStat::clear()
{
  info_map_.reuse();
  avg_partition_count_ = 0;
  is_switch_limited_ = false;
  smooth_switch_start_time_ = 0;
  switch_percent_ = 0;
  self_in_candidate_ = false;
  LOG_INFO("clear ObServerSwitchLeaderInfoStat");
}

int ObServerSwitchLeaderInfoStat::init(obrpc::ObSrvRpcProxy* srv_rpc_proxy)
{
  int ret = OB_SUCCESS;
  const int64_t bucket_num = 64;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret));
  } else if (NULL == srv_rpc_proxy) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(srv_rpc_proxy));
  } else if (OB_FAIL(info_map_.create(bucket_num, ObModIds::OB_RS_LEADER_COORDINATOR))) {
    LOG_WARN("failed to create info map", K(ret));
  } else {
    switch_percent_ = 0;
    srv_rpc_proxy_ = srv_rpc_proxy;
    avg_partition_count_ = 0;
    is_switch_limited_ = false;
    smooth_switch_start_time_ = 0;
    self_in_candidate_ = false;
    is_inited_ = true;
    LOG_INFO("succeed to init ObServerSwitchLeaderInfoStat");
  }

  return ret;
}

int ObServerSwitchLeaderInfoStat::start_new_smooth_switch(const int64_t smooth_switch_start_time)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (smooth_switch_start_time <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(smooth_switch_start_time));
  } else {
    if (0 != smooth_switch_start_time_) {
      LOG_INFO("old smooth switch leader is not finished, start new",
          K(smooth_switch_start_time_),
          K(smooth_switch_start_time));
    }
    LOG_INFO("start new smooth switch", K(smooth_switch_start_time));
    smooth_switch_start_time_ = smooth_switch_start_time;
    switch_percent_ = 0;
  }
  return ret;
}

int ObServerSwitchLeaderInfoStat::check_smooth_switch_condition(
    common::ObIArray<common::ObAddr>& candidate_leaders, bool& can_switch_more)
{
  int ret = OB_SUCCESS;
  bool my_can_switch = false;
  for (int64_t i = candidate_leaders.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
    if (OB_FAIL(can_switch_more_leader(candidate_leaders.at(i), my_can_switch))) {
      LOG_WARN("fail to check can switch more leader", K(ret));
    } else if (my_can_switch) {
      // this candidate still available, do nothing
    } else if (OB_FAIL(candidate_leaders.remove(i))) {
      LOG_WARN("fail to remove candidate leader", K(ret), K(i));
    } else {
    }
  }
  if (OB_SUCC(ret)) {
    if (candidate_leaders.count() <= 0) {
      can_switch_more = false;
      set_switch_limited();
      LOG_INFO("switch leader limited");
    } else {
      can_switch_more = true;
    }
  }
  return ret;
}

int ObServerSwitchLeaderInfoStat::can_switch_more_leader(const common::ObAddr& server, bool& can_switch)
{
  int ret = OB_SUCCESS;
  ServerSwitchLeaderInfo info;
  can_switch = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (0 == smooth_switch_start_time_) {
    can_switch = true;
  } else if (OB_FAIL(info_map_.get_refactored(server, info))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      can_switch = true;
      LOG_INFO("server not exist in map, set can_switch=true", K(server));
    } else {
      LOG_WARN("failed to get info map", K(ret), K(server));
    }
  } else if (info.is_switch_limited_) {
    can_switch = false;
    LOG_INFO("this server switch limit", K(server));
  } else {
    const int64_t switch_cnt_lmt = info.partition_count_ * switch_percent_ / ALL_SWITCH_PERCENT + 1;
    if (switch_percent_ >= ALL_SWITCH_PERCENT || info.new_leader_count_ < switch_cnt_lmt ||
        info.new_leader_count_ < SMOOTH_SWITCH_LEADER_CNT_LIMIT) {
      can_switch = true;
    } else {
      can_switch = false;
      info.is_switch_limited_ = true;
      LOG_INFO(
          "this server switch limit", K(server), K(switch_cnt_lmt), "leader count in this server", info.leader_count_);
      const int overwrite = 1;
      if (OB_FAIL(info_map_.set_refactored(server, info, overwrite))) {
        LOG_WARN("fail to set map with overwrite", K(ret), K(server));
      } else {
      }  // do nothing more
    }
  }
  return ret;
}

int ObServerSwitchLeaderInfoStat::inc_leader_count(const common::ObAddr& leader, const bool is_new_leader)
{
  int ret = OB_SUCCESS;
  ServerSwitchLeaderInfo info;
  const int overwrite_flag = 1;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(info_map_.get_refactored(leader, info))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      info.reset();
      info.server_ = leader;
      // if no partition count saved, use avg partition count
      info.partition_count_ = avg_partition_count_;
    } else {
      LOG_WARN("failed to get leader info from map", K(ret), K(leader));
    }
  }

  if (OB_SUCC(ret)) {
    ++info.leader_count_;
    if (is_new_leader) {
      ++info.new_leader_count_;
    }
    if (OB_FAIL(info_map_.set_refactored(leader, info, overwrite_flag))) {
      LOG_WARN("failed to set leader info", K(ret));
    }
  }

  return ret;
}

int ObServerSwitchLeaderInfoStat::prepare_next_turn(
    const volatile bool& is_stop, ObServerManager& server_mgr, const int64_t switch_leader_duration_time)
{
  int ret = OB_SUCCESS;
  common::ObArray<common::ObAddr> server_list;
  ObZone empty_zone;
  const int64_t now = ObTimeUtility::current_time();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (smooth_switch_start_time_ < 0) {
    ret = OB_ERR_SYS;
    LOG_WARN("smooth_switch_start_time_ must not less than 0", K(smooth_switch_start_time_));
  } else if (0 == smooth_switch_start_time_ || 0 == switch_leader_duration_time) {
    info_map_.reuse();
    switch_percent_ = ALL_SWITCH_PERCENT;
    LOG_INFO("prepare next turn, clear info map for not smooth switch",
        K(smooth_switch_start_time_),
        K(switch_leader_duration_time));
  } else {
    if (OB_FAIL(server_mgr.get_alive_servers(empty_zone, server_list))) {
      LOG_WARN("failed to get alive servers", K(ret));
    } else if (OB_FAIL(prepare_next_smooth_turn(is_stop, server_list, switch_leader_duration_time, now))) {
      LOG_WARN("failed to update switch percent", K(ret), K(switch_leader_duration_time), K(server_list));
    }
  }
  return ret;
}

int ObServerSwitchLeaderInfoStat::prepare_next_smooth_turn(const volatile bool& is_stop,
    const common::ObIArray<common::ObAddr>& server_list, const int64_t switch_leader_duration_time, const int64_t now)
{
  int ret = OB_SUCCESS;
  ServerSwitchLeaderInfo info;
  const int overwrite_flag = 1;
  int64_t partition_count = 0;
  int64_t switch_percent = 0;
  const int64_t reserve_time_per_turn = switch_leader_duration_time / TIMES_IN_SMOOTH_SWITCH;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (switch_leader_duration_time <= 0 || server_list.count() <= 0 || now <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(switch_leader_duration_time), K(server_list.count()));
  } else if (smooth_switch_start_time_ <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("cannot prepare smooth turn with invalid smooth_switch_start_time", K(ret), K_(smooth_switch_start_time));
  } else {
    info_map_.reuse();
    const int64_t used_switch_time = now - smooth_switch_start_time_;

    if (switch_leader_duration_time <= 0 || used_switch_time + reserve_time_per_turn > switch_leader_duration_time) {
      switch_percent_ = ALL_SWITCH_PERCENT;
    } else {
      // the first division of 1000 in time is to avoid the risk of int64 overflow
      int64_t denominator = switch_leader_duration_time / 1000;
      int64_t numerator = (used_switch_time + reserve_time_per_turn) / 1000;
      if (denominator <= 0) {
        denominator = 1;
      }
      switch_percent = ALL_SWITCH_PERCENT * numerator / denominator;
      if (switch_percent > ALL_SWITCH_PERCENT) {
        switch_percent = ALL_SWITCH_PERCENT;
      }
      if (switch_percent > switch_percent_) {
        switch_percent_ = switch_percent;
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < server_list.count(); ++i) {
      info.reset();
      info.server_ = server_list.at(i);
      if (is_stop) {
        ret = OB_CANCELED;
        LOG_WARN("rs is stopped", K(ret));
      } else if (OB_FAIL(get_server_partition_count(info.server_, info.partition_count_))) {
        LOG_WARN("failed to get server partition count", K(ret), K(info.server_));
      } else {
        partition_count += info.partition_count_;
        if (OB_FAIL(info_map_.set_refactored(info.server_, info, overwrite_flag))) {
          LOG_WARN("failed to set leader info map", K(ret));
        } else {
          LOG_INFO("fetch partition count", "server", info.server_, "partition_count", info.partition_count_);
        }
      }
    }

    if (OB_SUCC(ret)) {
      avg_partition_count_ = partition_count / server_list.count();
    }
  }

  LOG_INFO("prepare_next_smooth_turn",
      K(ret),
      K(switch_percent_),
      "cal_switch_percent",
      switch_percent,
      K(is_stop),
      K_(avg_partition_count),
      K(switch_leader_duration_time));
  return ret;
}

int ObServerSwitchLeaderInfoStat::finish_smooth_turn(const bool force_finish)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (0 == smooth_switch_start_time_) {
    ret = OB_ERR_SYS;
    LOG_ERROR("only smooth switch leader could be finished", K(smooth_switch_start_time_));
  } else if (!is_switch_limited_ || force_finish) {
    LOG_INFO("finish smooth switch", K(smooth_switch_start_time_), K(force_finish));
    smooth_switch_start_time_ = 0;
  }
  return ret;
}

int ObServerSwitchLeaderInfoStat::update_server_leader_cnt_status(ObServerManager* server_mgr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == server_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(server_mgr));
  } else if (OB_FAIL(server_mgr->update_leader_cnt_status(info_map_))) {
    LOG_WARN("fail to update leader cnt status", K(ret));
  } else {
  }  // no more to do
  return ret;
}

int ObServerSwitchLeaderInfoStat::copy_leader_stat(
    common::ObArray<ObILeaderCoordinator::ServerLeaderCount>& leader_stat)
{
  int ret = OB_SUCCESS;
  ObILeaderCoordinator::ServerLeaderCount server_leader_count;

  leader_stat.reset();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    for (ServerSwitchLeaderInfoMap::const_iterator iter = info_map_.begin(); OB_SUCC(ret) && iter != info_map_.end();
         ++iter) {
      const ObAddr& server = iter->first;
      const ServerSwitchLeaderInfo& info = iter->second;

      server_leader_count.reset();
      server_leader_count.count_ = info.leader_count_;
      server_leader_count.server_ = server;
      if (OB_FAIL(leader_stat.push_back(server_leader_count))) {
        LOG_WARN("failed to add leader stat", K(ret));
      }
    }

    std::sort(leader_stat.begin(), leader_stat.end());
  }
  return ret;
}

int ObServerSwitchLeaderInfoStat::need_force_finish_smooth_switch(
    const int64_t merger_switch_leader_duration_time, bool& need_force) const
{
  int ret = OB_SUCCESS;
  const int64_t now = ObTimeUtility::current_time();
  int64_t switch_timeout = MIN_SWITCH_TIMEOUT;
  need_force = false;

  if (switch_timeout < 2 * merger_switch_leader_duration_time) {
    switch_timeout = 2 * merger_switch_leader_duration_time;
  }

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (0 == smooth_switch_start_time_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not in smooth switch, no need to force finish", K(ret));
  } else if (now >= smooth_switch_start_time_ + switch_timeout) {
    need_force = true;
    LOG_WARN("smooth switch leader is timeout twice merger_switch_leader_duration_time,"
             "force it finish",
        K(now),
        K(smooth_switch_start_time_),
        K(merger_switch_leader_duration_time),
        K(switch_timeout));
  }
  return ret;
}

int ObServerSwitchLeaderInfoStat::get_expect_switch_ts(
    const int64_t switch_leader_duration_time, int64_t& expect_ts) const
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (0 == smooth_switch_start_time_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not in smooth switch, no need to force finish", K(ret));
  } else {
    // the first division of 1000 in time is to avoid the risk of int64 overflow
    const int64_t expect_cost_time = (switch_leader_duration_time / 1000) * switch_percent_ / ALL_SWITCH_PERCENT * 1000;
    expect_ts = smooth_switch_start_time_ + expect_cost_time;
  }
  return ret;
}

int ObServerSwitchLeaderInfoStat::get_server_partition_count(common::ObAddr& server, int64_t& partition_count)
{
  int ret = OB_SUCCESS;
  ObGetPartitionCountResult result;
  const int64_t rpc_timeout = 2 * 1000 * 1000;  // 2s

  if (NULL == srv_rpc_proxy_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(srv_rpc_proxy_->to(server).timeout(rpc_timeout).get_partition_count(result))) {
    LOG_WARN("failed to get partition count", K(ret), K(server));
  } else if (result.partition_count_ < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition count must not less than 0", K(ret), K(result));
  } else {
    partition_count = result.partition_count_;
  }

  return ret;
}

ObLeaderCoordinator::CandidateLeaderInfo::CandidateLeaderInfo()
    : server_addr_(),
      zone_(),
      balance_group_score_(INT64_MAX),
      original_leader_count_(0),
      cur_leader_count_(0),
      zone_candidate_count_(0),
      candidate_count_(0),
      in_normal_unit_count_(0),
      zone_migrate_out_or_transform_count_(0),
      migrate_out_or_transform_count_(0),
      zone_score_(),
      region_score_(),
      is_candidate_(true),
      not_merging_(true),
      not_excluded_(true),
      random_score_(INT64_MAX),
      start_service_(false),
      in_revoke_blacklist_count_(0)
{}

void ObLeaderCoordinator::CandidateLeaderInfo::reset()
{
  *this = CandidateLeaderInfo();
}

bool ObLeaderCoordinator::ChooseLeaderCmp::operator()(const CandidateLeaderInfo& left, const CandidateLeaderInfo& right)
{
  // is_candidate
  // true has a higher priority than false
  bool bool_ret = false;
  if (left.is_candidate_ == right.is_candidate_) {
    bool_ret = cmp_region_score(left, right);
  } else if (left.is_candidate_ && !right.is_candidate_) {
    bool_ret = true;
  } else if (!left.is_candidate_ && right.is_candidate_) {
    bool_ret = false;
  } else {
    ret_ = OB_ERR_UNEXPECTED;
  }
  return bool_ret;
}

bool ObLeaderCoordinator::ChooseLeaderCmp::cmp_region_score(
    const CandidateLeaderInfo& left, const CandidateLeaderInfo& right)
{
  bool bool_ret = false;
  // The lower the region score, the higher the priority
  if (left.region_score_.region_score_ < right.region_score_.region_score_) {
    bool_ret = true;
  } else if (left.region_score_.region_score_ > right.region_score_.region_score_) {
    bool_ret = false;
  } else {
    if (OB_SYS_TENANT_ID == tenant_id_) {
      bool_ret = cmp_start_service(left, right);
    } else if (combine_id(tenant_id_, OB_SYS_TABLEGROUP_ID) == tablegroup_id_) {
      bool_ret = cmp_start_service(left, right);
    } else {
      bool_ret = cmp_not_merging(left, right);
    }
  }
  return bool_ret;
}

bool ObLeaderCoordinator::ChooseLeaderCmp::cmp_not_merging(
    const CandidateLeaderInfo& left, const CandidateLeaderInfo& right)
{
  bool bool_ret = false;
  // not mergeing
  // true has a higher priority than false
  if (left.not_merging_ == right.not_merging_) {
    bool_ret = cmp_start_service(left, right);
  } else if (left.not_merging_ && !right.not_merging_) {
    bool_ret = true;
  } else if (!left.not_merging_ && right.not_merging_) {
    bool_ret = false;
  } else {
    ret_ = OB_ERR_UNEXPECTED;
  }
  return bool_ret;
}

bool ObLeaderCoordinator::ChooseLeaderCmp::cmp_start_service(
    const CandidateLeaderInfo& left, const CandidateLeaderInfo& right)
{
  bool bool_ret = false;
  // start service
  // true has a higher priority than false
  /*
  if (left.start_service_ == right.start_service_) {
    bool_ret = cmp_candidate_cnt(left, right);
  } else if (left.start_service_ && !right.start_service_) {
    bool_ret = true;
  } else if (!left.start_service_ && right.start_service_) {
    bool_ret = false;
  } else {
    ret_ = OB_ERR_UNEXPECTED;
  }
  */
  // Ignore it for now, the start service time is currently unstable
  bool_ret = cmp_candidate_cnt(left, right);
  return bool_ret;
}

bool ObLeaderCoordinator::ChooseLeaderCmp::cmp_candidate_cnt(
    const CandidateLeaderInfo& left, const CandidateLeaderInfo& right)
{
  bool bool_ret = false;
  if (left.zone_candidate_count_ > right.zone_candidate_count_) {
    bool_ret = true;
  } else if (left.zone_candidate_count_ < right.zone_candidate_count_) {
    bool_ret = false;
  } else {
    bool_ret = cmp_in_revoke_blacklist(left, right);
  }
  return bool_ret;
}

bool ObLeaderCoordinator::ChooseLeaderCmp::cmp_in_revoke_blacklist(
    const CandidateLeaderInfo& left, const CandidateLeaderInfo& right)
{
  bool bool_ret = false;
  if (left.in_revoke_blacklist_count_ < right.in_revoke_blacklist_count_) {
    bool_ret = true;
  } else if (left.in_revoke_blacklist_count_ > right.in_revoke_blacklist_count_) {
    bool_ret = false;
  } else {
    bool_ret = cmp_not_excluded(left, right);
  }
  return bool_ret;
}

bool ObLeaderCoordinator::ChooseLeaderCmp::cmp_not_excluded(
    const CandidateLeaderInfo& left, const CandidateLeaderInfo& right)
{
  bool bool_ret = false;
  // not_excluded
  // true has a higher priority than false
  if (left.not_excluded_ == right.not_excluded_) {
    bool_ret = cmp_migrate_out_cnt(left, right);
  } else if (left.not_excluded_ && !right.not_excluded_) {
    bool_ret = true;
  } else if (!left.not_excluded_ && right.not_excluded_) {
    bool_ret = false;
  } else {
    ret_ = OB_ERR_UNEXPECTED;
  }
  return bool_ret;
}

bool ObLeaderCoordinator::ChooseLeaderCmp::cmp_migrate_out_cnt(
    const CandidateLeaderInfo& left, const CandidateLeaderInfo& right)
{
  bool bool_ret = false;
  // The smaller the migrate out count, the higher the priority
  if (left.zone_migrate_out_or_transform_count_ < right.zone_migrate_out_or_transform_count_) {
    bool_ret = true;
  } else if (left.zone_migrate_out_or_transform_count_ > right.zone_migrate_out_or_transform_count_) {
    bool_ret = false;
  } else {
    if (left.migrate_out_or_transform_count_ < right.migrate_out_or_transform_count_) {
      bool_ret = true;
    } else if (left.migrate_out_or_transform_count_ > right.migrate_out_or_transform_count_) {
      bool_ret = false;
    } else {
      bool_ret = cmp_in_normal_unit(left, right);
    }
  }
  return bool_ret;
}

bool ObLeaderCoordinator::ChooseLeaderCmp::cmp_in_normal_unit(
    const CandidateLeaderInfo& left, const CandidateLeaderInfo& right)
{
  bool bool_ret = false;
  // The larger the in normal unit count, the higher the priority
  if (left.in_normal_unit_count_ > right.in_normal_unit_count_) {
    bool_ret = true;
  } else if (left.in_normal_unit_count_ < right.in_normal_unit_count_) {
    bool_ret = false;
  } else {
    bool_ret = cmp_primary_zone(left, right);
  }
  return bool_ret;
}

bool ObLeaderCoordinator::ChooseLeaderCmp::cmp_primary_zone(
    const CandidateLeaderInfo& left, const CandidateLeaderInfo& right)
{
  bool bool_ret = false;
  // The smaller the zone score, the higher the priority
  if (left.zone_score_.zone_score_ < right.zone_score_.zone_score_) {
    bool_ret = true;
  } else if (left.zone_score_.zone_score_ > right.zone_score_.zone_score_) {
    bool_ret = false;
  } else {
    bool_ret = cmp_balance_group_score(left, right);
  }
  return bool_ret;
}

bool ObLeaderCoordinator::ChooseLeaderCmp::cmp_balance_group_score(
    const CandidateLeaderInfo& left, const CandidateLeaderInfo& right)
{
  bool bool_ret = false;
  // The smaller the balance group zone score, the higher the priority
  if (left.balance_group_score_ < right.balance_group_score_) {
    bool_ret = true;
  } else if (left.balance_group_score_ > right.balance_group_score_) {
    bool_ret = false;
  } else {
    bool_ret = cmp_original_leader(left, right);
  }
  return bool_ret;
}

bool ObLeaderCoordinator::ChooseLeaderCmp::cmp_original_leader(
    const CandidateLeaderInfo& left, const CandidateLeaderInfo& right)
{
  bool bool_ret = false;
  // The larger the original leader count, the higher the priority
  if (left.original_leader_count_ > right.original_leader_count_) {
    bool_ret = true;
  } else if (left.original_leader_count_ < right.original_leader_count_) {
    bool_ret = false;
  } else {
    bool_ret = cmp_random_score(left, right);
  }
  return bool_ret;
}

bool ObLeaderCoordinator::ChooseLeaderCmp::cmp_random_score(
    const CandidateLeaderInfo& left, const CandidateLeaderInfo& right)
{
  bool bool_ret = false;
  if (left.random_score_ > right.random_score_) {
    bool_ret = true;
  } else if (left.random_score_ < right.random_score_) {
    bool_ret = false;
  } else {
    bool_ret = cmp_server_addr(left, right);
  }
  return bool_ret;
}

bool ObLeaderCoordinator::ChooseLeaderCmp::cmp_server_addr(
    const CandidateLeaderInfo& left, const CandidateLeaderInfo& right)
{
  bool bool_ret = false;
  // The smaller the server add, the higher the priority
  if (left.server_addr_ < right.server_addr_) {
    bool_ret = true;
  } else {
    bool_ret = false;
  }
  return bool_ret;
}

int ObLeaderCoordinator::check_daily_merge_switch_leader_by_tenant(PartitionInfoContainer& partition_info_container,
    const uint64_t tenant_id, const common::ObIArray<common::ObZone>& zone_list, common::ObIArray<bool>& results)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator partition_allocator(ObModIds::OB_RS_LEADER_COORDINATOR_PA);
  TenantPartition tenant_partition;
  share::schema::ObSchemaGetterGuard schema_guard;
  const share::schema::ObTenantSchema* tenant_schema = NULL;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLeaderCoordinator not init", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || OB_SYS_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(check_cancel())) {
    LOG_WARN("fail to check cancel, rs is stopped", K(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_schema))) {
    LOG_WARN("fail to get tenant schema", K(ret), K(tenant_id));
  } else if (OB_UNLIKELY(NULL == tenant_schema)) {
    LOG_INFO("this tenant is deleted during check daily merge switch leader", K(tenant_id));
  } else if (OB_FAIL(build_tenant_partition(
                 schema_guard, partition_info_container, tenant_id, partition_allocator, tenant_partition))) {
    LOG_WARN("fail to build tenant partition", K(ret), K(tenant_id));
  } else if (OB_FAIL(check_dm_pg_switch_leader_by_tenant(tenant_partition, tenant_id, zone_list, results))) {
    if (OB_PARTITION_NOT_EXIST == ret || OB_NOT_MASTER == ret) {
      // do not print log and return this ret to the upper layer
    } else {
      LOG_WARN("fail to check daily merge pg switch leader", K(ret), K(tenant_id));
    }
  } else {
  }  // no more to do
  return ret;
}

int ObLeaderCoordinator::build_dm_partition_candidates(TenantPartition& tenant_partition)
{
  int ret = OB_SUCCESS;
  common::ObArray<PreSwitchPgInfo> pre_switch_index_array;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLeaderCoordinator not init", K(ret));
  } else if (OB_FAIL(pre_switch_index_array.reserve(tenant_partition.count()))) {
    LOG_WARN("fail to reserve index array", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_partition.count(); ++i) {
      if (OB_FAIL(check_cancel())) {
        LOG_WARN("fail to check cancel, maybe rs is stopped", K(ret));
      } else if (OB_FAIL(pre_switch_index_array.push_back(PreSwitchPgInfo(i)))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(build_pg_array_leader_candidates(pre_switch_index_array, tenant_partition))) {
      LOG_WARN("fail to build pg array leader candidates", K(ret));
    }
  }
  return ret;
}

int ObLeaderCoordinator::check_dm_pg_switch_leader_by_tenant(TenantPartition& tenant_partition,
    const uint64_t tenant_id, const common::ObIArray<common::ObZone>& zone_list, common::ObIArray<bool>& results)
{
  int ret = OB_SUCCESS;
  int bak_ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLeaderCoordinator not init", K(ret));
  } else if (OB_UNLIKELY(OB_SYS_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (tenant_partition.count() <= 0) {
    // The number of partition groups under this tenant is 0,
    // it is considered that it can be cut away,
    // and return true
    for (int64_t i = 0; OB_SUCC(ret) && i < zone_list.count(); ++i) {
      if (OB_FAIL(results.push_back(true))) {
        LOG_WARN("fail to push back", K(ret));
      } else {
      }  // no more to do
    }
  } else if (OB_FAIL(build_dm_partition_candidates(tenant_partition))) {
    if (OB_NOT_MASTER == ret || OB_PARTITION_NOT_EXIST == ret) {
      bak_ret = ret;
      ret = OB_SUCCESS;  // rewrite to OB_SUCCESS
    } else {
      LOG_WARN("fail to check daily merge switch leader by pg", K(ret));
    }
  } else {
    bool need_continue = true;
    common::ObArray<bool> intermediate_results;
    FOREACH_CNT_X(ppartition_array, tenant_partition, need_continue && OB_SUCC(ret))
    {
      intermediate_results.reset();
      PartitionArray* partition_array = *ppartition_array;
      if (NULL == partition_array) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("partition array is null", K(ret), KP(partition_array));
      } else if (OB_FAIL(check_cancel())) {
        LOG_WARN("fail to check cancel, maybe rs is stopped", K(ret));
      } else if (OB_SYS_TABLEGROUP_ID == extract_pure_id(partition_array->get_tablegroup_id())) {
        // Tenant-level system tables do not switch the leader during rotation and consolidation,
        // no need to check
        for (int64_t i = 0; i < zone_list.count() && OB_SUCC(ret); ++i) {
          if (OB_FAIL(results.push_back(true /*can switch out*/))) {
            LOG_WARN("fail to push back", KR(ret), K(zone_list), K(tenant_id));
          }
        }
      } else if (OB_FAIL(check_daily_merge_switch_leader_by_pg(*partition_array, zone_list, intermediate_results))) {
        LOG_WARN("fail to check daily merge switch leader by pg", K(ret));
      } else if (OB_FAIL(update_daily_merge_switch_leader_result(intermediate_results, results))) {
        LOG_WARN("fail to update daily merge switch leader result", K(ret));
      } else if (OB_FAIL(check_daily_merge_switch_leader_need_continue(results, need_continue))) {
        LOG_WARN("fail to check daily merge switch leader need continue", K(ret));
      } else if (!need_continue) {
        if (partition_array->count() > 0) {
          dump_partition_candidates_info(tenant_id, *partition_array);
        }
        // all elements in results are false, no need to check any more
      } else {
      }  // no more to do
    }
  }
  ret = (OB_SUCCESS == ret) ? bak_ret : ret;
  return ret;
}

void ObLeaderCoordinator::dump_partition_candidates_info(const uint64_t tenant_id, PartitionArray& partition_array)
{
  int tmp_ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTenantSchema* tenant_schema = NULL;
  if (OB_UNLIKELY(!inited_)) {
    tmp_ret = OB_NOT_INIT;
    LOG_WARN("ObLeaderCoordinator not init", K(tmp_ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
    tmp_ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tmp_ret), K(tenant_id));
  } else if (OB_SUCCESS != (tmp_ret = schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("fail to get schema guard", K(tmp_ret));
  } else if (OB_SUCCESS != (tmp_ret = schema_guard.get_tenant_info(tenant_id, tenant_schema))) {
    LOG_WARN("fail to get tenant schema", K(tmp_ret), K(tenant_id));
  } else if (OB_UNLIKELY(NULL == tenant_schema)) {
    LOG_INFO("tenant may be deleted", K(tenant_id));
  } else {
    int64_t full_replica_num = tenant_schema->get_full_replica_num();
    if (full_replica_num <= 0) {
      tmp_ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant full replica num unexpected", K(tmp_ret), K(full_replica_num));
    } else {
      LOG_INFO("tenant full replica num", K(tenant_id), K(full_replica_num));
      for (int64_t i = 0; i < partition_array.count(); ++i) {
        const Partition& this_partition = partition_array.at(i);
        const int64_t replica_num = this_partition.prep_candidates_.count();
        if (OB_UNLIKELY(nullptr == this_partition.info_)) {
          LOG_WARN("info ptr is null", K(tmp_ret));
        } else if (this_partition.candidates_.count() < replica_num) {
          // Recorded to rs event history
          dump_partition_candidates_to_rs_event(tenant_id, this_partition);
          // Print a log
          LOG_INFO("partition candidate",
              K(tenant_id),
              "table_id",
              this_partition.table_id_,
              "partition_id",
              this_partition.info_->get_partition_id(),
              "candidates",
              this_partition.candidates_,
              "prep_candidates",
              this_partition.prep_candidates_);
          // No need to print out all the partitions, just type a sample for troubleshooting
          break;
        }
      }
    }
  }
}

// Record the current candidate replica information of the partition in the rs event history table
int ObLeaderCoordinator::dump_candidate_info_to_rs_event(const uint64_t tenant_id, const Partition& partition)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  const int64_t buf_len = common::MAX_ROOTSERVICE_EVENT_EXTRA_INFO_LENGTH;
  char candidate_buf[common::MAX_ROOTSERVICE_EVENT_EXTRA_INFO_LENGTH] = {0};
  bool is_first = true;
  constexpr int64_t addr_len = common::MAX_IP_ADDR_LENGTH;
  char addr_string[addr_len + 1] = {0};
  // Record the partition's candidate
  for (int64_t i = 0; i < partition.candidates_.count() && OB_SUCC(ret); ++i) {
    if (partition.candidates_.at(i).to_string(addr_string, addr_len) > addr_len) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("fail to format candidate addr", K(ret));
    } else if (OB_FAIL(databuff_printf(candidate_buf, buf_len, pos, "%s%s", (is_first ? "" : ";"), addr_string))) {
      if (OB_SIZE_OVERFLOW == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("fail to format candidate info", K(ret), K(partition));
      }
    } else {
      is_first = false;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(nullptr == partition.info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("info ptr is null", K(ret));
  } else {
    const char* NOP_NAME = "";
    const char* NOP_VALUE = "";
    ROOTSERVICE_EVENT_ADD("leader_coordinator",
        "check_daily_merge_switch_leader",
        "tenant_id",
        tenant_id,
        "table_id",
        partition.table_id_,
        "partition_id",
        partition.info_->get_partition_id(),
        NOP_NAME,
        NOP_VALUE,
        NOP_NAME,
        NOP_VALUE,
        NOP_NAME,
        NOP_VALUE,
        candidate_buf);
  }
  return ret;
}

int ObLeaderCoordinator::construct_non_candidate_info_sql(
    const Partition& partition, const common::ObIArray<common::ObAddr>& non_candidates, common::ObSqlString& sql_string)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == partition.info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("info ptr is null", K(ret));
  } else if (non_candidates.count() > 0) {
    sql_string.reset();
    if (OB_FAIL(sql_string.append_fmt("SELECT "
                                      "svr_ip, "
                                      "svr_port, "
                                      "table_id, "
                                      "partition_idx, "
                                      "partition_cnt, "
                                      "role, "
                                      "is_candidate, "
                                      "membership_version, "
                                      "log_id, "
                                      "locality, "
                                      "system_score, "
                                      "is_tenant_active, "
                                      "on_revoke_blacklist, "
                                      "on_loop_blacklist, "
                                      "replica_type, "
                                      "server_status, "
                                      "is_clog_disk_full, "
                                      "is_offline"
                                      " FROM %s WHERE table_id = %ld and partition_idx = %ld and (",
            share::OB_ALL_VIRTUAL_ELECTION_PRIORITY_TNAME,
            partition.info_->get_table_id(),
            partition.info_->get_partition_id()))) {
      LOG_WARN("fail to append fmt", K(ret));
    } else {
      bool is_first = true;
      const int64_t ip_len = common::MAX_IP_ADDR_LENGTH;
      char ip_string[ip_len + 1] = {0};
      for (int64_t i = 0; OB_SUCC(ret) && i < non_candidates.count(); ++i) {
        if (!non_candidates.at(i).ip_to_string(ip_string, ip_len)) {
          ret = OB_SIZE_OVERFLOW;
          LOG_WARN("fail to format ip string", K(ret));
        } else if (OB_FAIL(sql_string.append_fmt("%s(svr_ip = '%s' and svr_port = %d)",
                       is_first ? "" : "or ",
                       ip_string,
                       non_candidates.at(i).get_port()))) {
          LOG_WARN("fail to append fmt", K(ret));
        } else {
          is_first = false;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(sql_string.append(")"))) {
        LOG_WARN("fail to append", K(ret));
      } else {
      }  // construct ok, no more to do
    }
  }
  return ret;
}

int ObLeaderCoordinator::get_single_candidate_election_priority(
    sqlclient::ObMySQLResult& res, common::ObIArray<CandidateElectionPriority>& election_priority_array)
{
  int ret = OB_SUCCESS;
  CandidateElectionPriority election_priority;
  int64_t idx = 0;
  int64_t table_id = 0;
  int64_t part_id = 0;
  int64_t part_cnt = 0;
  ObString svr_ip;
  int64_t svr_port = 0;
  GET_COL_IGNORE_NULL(res.get_varchar, idx++, svr_ip);
  GET_COL_IGNORE_NULL(res.get_int, idx++, svr_port);
  GET_COL_IGNORE_NULL(res.get_int, idx++, table_id);
  GET_COL_IGNORE_NULL(res.get_int, idx++, part_id);
  GET_COL_IGNORE_NULL(res.get_int, idx++, part_cnt);
  GET_COL_IGNORE_NULL(res.get_int, idx++, election_priority.role_);
  GET_COL_IGNORE_NULL(res.get_bool, idx++, election_priority.is_candidate_);
  GET_COL_IGNORE_NULL(res.get_int, idx++, election_priority.membership_version_);
  GET_COL_IGNORE_NULL(res.get_uint, idx++, election_priority.log_id_);
  GET_COL_IGNORE_NULL(res.get_uint, idx++, election_priority.locality_);
  GET_COL_IGNORE_NULL(res.get_int, idx++, election_priority.sys_score_);
  GET_COL_IGNORE_NULL(res.get_bool, idx++, election_priority.is_tenant_active_);
  GET_COL_IGNORE_NULL(res.get_bool, idx++, election_priority.on_revoke_blacklist_);
  GET_COL_IGNORE_NULL(res.get_bool, idx++, election_priority.on_loop_blacklist_);
  GET_COL_IGNORE_NULL(res.get_int, idx++, election_priority.replica_type_);
  GET_COL_IGNORE_NULL(res.get_int, idx++, election_priority.server_status_);
  GET_COL_IGNORE_NULL(res.get_bool, idx++, election_priority.is_clog_disk_full_);
  GET_COL_IGNORE_NULL(res.get_bool, idx++, election_priority.is_offline_);
  if (!election_priority.addr_.set_ip_addr(svr_ip, static_cast<int32_t>(svr_port))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to set addr", K(ret), K(svr_ip), K(svr_port));
  } else if (OB_FAIL(election_priority.pkey_.init(static_cast<uint64_t>(table_id), part_id, part_cnt))) {
    LOG_WARN("fail to init pkey", K(ret), K(table_id), K(part_id), K(part_cnt));
  } else if (OB_FAIL(election_priority_array.push_back(election_priority))) {
    LOG_WARN("fail to push back", K(ret));
  } else {
  }  // no more to do
  return ret;
}

int ObLeaderCoordinator::do_dump_non_candidate_info_to_rs_event(const Partition& partition,
    const common::ObIArray<common::ObAddr>& non_candidates, const common::ObSqlString& sql_string)
{
  int ret = OB_SUCCESS;
  ObRootService* root_service = NULL;
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    sqlclient::ObMySQLResult* result;
    ObArray<CandidateElectionPriority> election_priority_array;
    if (NULL == (root_service = GCTX.root_service_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rootservice pointer null", K(ret));
    } else if (OB_FAIL(root_service->get_sql_proxy().read(res, sql_string.ptr()))) {
      LOG_WARN("fail to read result", K(ret));
    } else if (OB_UNLIKELY(NULL == (result = res.get_result()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get result", K(ret));
    } else {
      while (OB_SUCC(ret)) {
        ret = result->next();
        if (OB_SUCCESS == ret) {
          if (OB_FAIL(get_single_candidate_election_priority(*result, election_priority_array))) {
            LOG_WARN("fail to get single candidate election priority", K(ret));
          } else {
          }  // no more to do
        } else if (OB_ITER_END == ret) {
          break;
        } else {
          LOG_WARN("fail to get next", K(ret));
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
      if (OB_SUCCESS == ret) {
        // The way to read the virtual table to obtain the non_candidate information,
        // due to server downtime and other reasons, may not be able to obtain all the non_candidate.
        // Record these non_candidates that are not obtained into the rs event history table,
        // and identified as "REPLICA_NOT_EXIST"
        if (OB_FAIL(dump_exist_info_to_rs_event(election_priority_array))) {
          LOG_WARN("fail to dump exist non candidate info", K(ret));
        } else if (OB_FAIL(dump_non_exist_info_to_rs_event(partition, election_priority_array, non_candidates))) {
          LOG_WARN("fail to dump non exist candidate info", K(ret));
        } else {
        }  // no more to do
      }
    }
  }
  return ret;
}

int ObLeaderCoordinator::dump_exist_info_to_rs_event(
    const common::ObIArray<CandidateElectionPriority>& election_priority_array)
{
  int ret = OB_SUCCESS;
  const char* NOP_NAME = "";
  const char* NOP_VALUE = "";
  for (int64_t i = 0; OB_SUCC(ret) && i < election_priority_array.count(); ++i) {
    int64_t pos = 0;
    const int64_t buf_len = common::MAX_ROOTSERVICE_EVENT_EXTRA_INFO_LENGTH;
    char election_priority_buf[common::MAX_ROOTSERVICE_EVENT_EXTRA_INFO_LENGTH] = {0};
    const CandidateElectionPriority& candidate_election = election_priority_array.at(i);
    int tmp_ret = OB_SUCCESS;  // Each replica does not affect each other
    if (OB_SUCCESS != (tmp_ret = databuff_printf(election_priority_buf,
                           buf_len,
                           pos,
                           "role:%ld, is_candidate:%d, membership_version:%ld, log_id:%lu, "
                           "locality:%lu, sys_score:%ld, is_tenant_active:%d, on_revoke_blacklist:%d, "
                           "on_loop_blacklist:%d, server_status:%ld, is_clog_disk_full:%d, is_offline:%d",
                           candidate_election.role_,
                           static_cast<int32_t>(candidate_election.is_candidate_),
                           candidate_election.membership_version_,
                           candidate_election.log_id_,
                           candidate_election.locality_,
                           candidate_election.sys_score_,
                           static_cast<int32_t>(candidate_election.is_tenant_active_),
                           static_cast<int32_t>(candidate_election.on_revoke_blacklist_),
                           static_cast<int32_t>(candidate_election.on_loop_blacklist_),
                           candidate_election.server_status_,
                           static_cast<int32_t>(candidate_election.is_clog_disk_full_),
                           static_cast<int32_t>(candidate_election.is_offline_)))) {
      LOG_WARN("fail to format info to election priority buf", K(tmp_ret));
    } else {
      ROOTSERVICE_EVENT_ADD("leader_coordinator",
          "non_candidate_info",
          "tenant_id",
          candidate_election.pkey_.get_tenant_id(),
          "table_id",
          candidate_election.pkey_.get_table_id(),
          "partition_id",
          candidate_election.pkey_.get_partition_id(),
          "server_addr",
          candidate_election.addr_,
          NOP_NAME,
          NOP_VALUE,
          NOP_NAME,
          NOP_VALUE,
          election_priority_buf);
    }
  }
  return ret;
}

int ObLeaderCoordinator::dump_non_exist_info_to_rs_event(const Partition& partition,
    const common::ObIArray<CandidateElectionPriority>& election_priority_array,
    const common::ObIArray<common::ObAddr>& non_candidates)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == partition.info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("info ptr is null", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < non_candidates.count(); ++i) {
      const common::ObAddr& addr = non_candidates.at(i);
      bool exist = false;
      for (int64_t j = 0; !exist && j < election_priority_array.count(); ++j) {
        if (addr == election_priority_array.at(j).addr_) {
          exist = true;
        } else {
        }  // go on to check next
      }
      if (!exist) {
        const char* COMMENT_NAME = "comment";
        const char* COMMENT_VALUE = "replica_temporary_not_exist";
        ROOTSERVICE_EVENT_ADD("leader_coordinator",
            "non_candidate_info",
            "tenant_id",
            partition.info_->get_tenant_id(),
            "table_id",
            partition.info_->get_table_id(),
            "partition_id",
            partition.info_->get_partition_id(),
            "server_addr",
            addr,
            COMMENT_NAME,
            COMMENT_VALUE);
      } else {
      }  // exist in election_priority_array, has been record by dump_exist_info_to_rs_event
    }
  }
  return ret;
}

// Record the replica of the partition that is not in the candidate list in the rs event history table,
// Go back and read the virtual table to get the current state of these replicas
int ObLeaderCoordinator::dump_non_candidate_info_to_rs_event(const uint64_t tenant_id, const Partition& partition)
{
  int ret = OB_SUCCESS;
  UNUSED(tenant_id);
  common::ObArray<common::ObAddr> non_candidates;
  common::ObSqlString sql_string;
  if (OB_UNLIKELY(nullptr == partition.info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("info ptr is null", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < partition.info_->replica_count(); ++i) {
      const ObPartitionReplica& replica = partition.info_->get_replicas_v2().at(i);
      if (replica.is_in_service() && common::REPLICA_TYPE_FULL == replica.replica_type_) {
        const common::ObAddr& server = replica.server_;
        if (!has_exist_in_array(partition.candidates_, server)) {
          if (OB_FAIL(non_candidates.push_back(server))) {
            LOG_WARN("fail to push back", K(ret));
          } else {
          }  // no more to do
        } else {
        }  // exist in candidate array, ignore
      } else {
      }  // ignore the any other replica
    }
    if (OB_FAIL(ret)) {
    } else if (non_candidates.count() <= 0) {
    } else if (OB_FAIL(construct_non_candidate_info_sql(partition, non_candidates, sql_string))) {
      LOG_WARN("fail to construct non candidate info sql", K(ret));
    } else if (OB_FAIL(do_dump_non_candidate_info_to_rs_event(partition, non_candidates, sql_string))) {
      LOG_WARN("fail to do dump candidate info to rs event", K(ret));
    } else {
    }  // no more to do
  }
  return ret;
}

// Both the current candidate replica of the partition
// and the replica that cannot become a candidate will be written to the rs event history table
void ObLeaderCoordinator::dump_partition_candidates_to_rs_event(const uint64_t tenant_id, const Partition& partition)
{
  int tmp_ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
    tmp_ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(tenant_id), K(tmp_ret));
  } else if (OB_SUCCESS != (tmp_ret = dump_candidate_info_to_rs_event(tenant_id, partition))) {
    LOG_WARN("fail to dump candidate info to rs event", K(tmp_ret));
  } else if (OB_SUCCESS != (tmp_ret = dump_non_candidate_info_to_rs_event(tenant_id, partition))) {
    LOG_WARN("fail to dump non candidate info to rs event", K(tmp_ret));
  } else {
  }  // no more to do
}

int ObLeaderCoordinator::check_daily_merge_switch_leader_by_pg(
    PartitionArray& partition_array, const common::ObIArray<common::ObZone>& zone_list, common::ObIArray<bool>& results)
{
  int ret = OB_SUCCESS;
  CandidateZoneInfoMap leader_zone_map;
  ObSEArray<ObRawPrimaryZoneUtil::ZoneScore, MAX_ZONE_NUM, ObNullAllocator> zone_score_array;
  // we deem region num is on more than zone num, so max zone num is enough
  ObSEArray<ObRawPrimaryZoneUtil::RegionScore, MAX_ZONE_NUM, ObNullAllocator> region_score_array;
  const int64_t bucket_num = 64;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLeaderCoordinator not init", K(ret));
  } else if (OB_UNLIKELY(partition_array.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("partition array is empty", K(ret), "partition count", partition_array.count());
  } else if (OB_UNLIKELY(zone_list.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("zone list is empty", K(ret), "zone list count", zone_list.count());
  } else if (OB_FAIL(leader_zone_map.create(bucket_num, ObModIds::OB_RS_LEADER_COORDINATOR))) {
    LOG_WARN("fail to create leader info map", K(ret));
  } else if (OB_FAIL(build_zone_region_score_array(
                 partition_array.get_primary_zone(), zone_score_array, region_score_array))) {
    LOG_WARN("fail to build zone region score array", K(ret), "primary zone", partition_array.get_primary_zone());
  } else if (OB_FAIL(build_candidate_zone_info_map(region_score_array, partition_array, leader_zone_map))) {
    LOG_WARN("fail to build candidate zone info map", K(ret));
  } else if (OB_FAIL(do_check_daily_merge_switch_leader_by_pg(partition_array, leader_zone_map, zone_list, results))) {
    LOG_WARN("fail to do check daily merge switch leader by pg", K(ret));
  } else {
  }  // no more to do
  return ret;
}

int ObLeaderCoordinator::update_candidate_zone_info_map(
    const common::ObIArray<share::ObRawPrimaryZoneUtil::RegionScore>& region_score_array,
    const common::ObAddr& candidate, CandidateZoneInfoMap& candidate_zone_map)
{
  int ret = OB_SUCCESS;
  common::ObRegion server_region;
  HEAP_VAR(share::ObZoneInfo, zone_info)
  {
    CandidateZoneInfo candidate_zone_info;
    if (OB_UNLIKELY(!inited_)) {
      ret = OB_NOT_INIT;
      LOG_WARN("ObLeaderCoordinator not init", K(ret));
    } else if (OB_UNLIKELY(!candidate.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid region score array or candidate", K(ret), K(candidate));
    } else if (OB_FAIL(server_mgr_->get_server_zone(candidate, zone_info.zone_))) {
      LOG_WARN("fail to get server zone", K(ret), K(candidate));
    } else if (OB_FAIL(zone_mgr_->get_zone(zone_info))) {
      LOG_WARN("fail to get zone info", K(ret));
    } else if (OB_FAIL(server_region.assign(zone_info.region_.info_.ptr()))) {
      LOG_WARN("fail to assign region", K(ret));
    } else {
      const int overwrite_flag = 1;
      ret = candidate_zone_map.get_refactored(zone_info.zone_, candidate_zone_info);
      if (OB_SUCCESS == ret) {
        ++candidate_zone_info.candidate_count_;
      } else if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        candidate_zone_info.zone_ = zone_info.zone_;
        candidate_zone_info.candidate_count_ = 1;  // init count 1
        candidate_zone_info.region_score_ = INT64_MAX;
        for (int64_t i = 0; i < region_score_array.count(); ++i) {
          if (server_region == region_score_array.at(i).region_) {
            candidate_zone_info.region_score_ = region_score_array.at(i).region_score_;
            break;
          } else {
            candidate_zone_info.region_score_ = INT64_MAX;
          }
        }
      } else {
        LOG_WARN("fail to get from map", K(ret));
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(candidate_zone_map.set_refactored(zone_info.zone_, candidate_zone_info, overwrite_flag))) {
        LOG_WARN("fail to set refactored", K(ret));
      } else {
      }  // no more to do
    }
  }
  return ret;
}

int ObLeaderCoordinator::build_candidate_zone_info_map(
    const common::ObIArray<share::ObRawPrimaryZoneUtil::RegionScore>& region_score_array,
    const PartitionArray& partition_array, CandidateZoneInfoMap& candidate_zone_map)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLeaderCoordinator not init", K(ret));
  } else if (OB_UNLIKELY(partition_array.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid region score array or partition array", K(ret), "partition_array count", partition_array.count());
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_array.count(); ++i) {
      const common::ObIArray<common::ObAddr>& candidates = partition_array.at(i).candidates_;
      for (int64_t j = 0; OB_SUCC(ret) && j < candidates.count(); ++j) {
        if (OB_FAIL(update_candidate_zone_info_map(region_score_array, candidates.at(j), candidate_zone_map))) {
          LOG_WARN("fail to update candidate zone info map", K(ret));
        } else {
        }  // no more to do
      }
    }
  }
  return ret;
}

int ObLeaderCoordinator::do_check_daily_merge_switch_leader_by_pg(const PartitionArray& partition_array,
    const CandidateZoneInfoMap& candidate_zone_map, const common::ObIArray<common::ObZone>& zone_list,
    common::ObIArray<bool>& results)
{
  int ret = OB_SUCCESS;
  ObArray<CandidateZoneInfo> candidate_zone_info_array;
  for (CandidateZoneInfoMap::const_iterator iter = candidate_zone_map.begin();
       OB_SUCC(ret) && iter != candidate_zone_map.end();
       ++iter) {
    const CandidateZoneInfo& candidate_zone_info = iter->second;
    if (OB_FAIL(candidate_zone_info_array.push_back(candidate_zone_info))) {
      LOG_WARN("fail to push back", K(ret));
    } else {
    }  // no more to do
  }
  if (OB_FAIL(ret)) {
  } else if (candidate_zone_info_array.count() <= 0) {
    // No owner will result in no elements in the candidate array,
    // Allow daily merge to be carried out without an owner
    const bool can_switch_out = true;
    for (int64_t i = 0; OB_SUCC(ret) && i < zone_list.count(); ++i) {
      if (OB_FAIL(results.push_back(can_switch_out))) {
        LOG_WARN("fail to push back", K(ret));
      } else {
      }  // no more to do
    }
  } else {
    CandidateZoneInfoCmp cmp_operator;
    std::sort(candidate_zone_info_array.begin(), candidate_zone_info_array.end(), cmp_operator);
    if (OB_FAIL(cmp_operator.get_ret())) {
      LOG_WARN("fail to do candidate zone info array sort", K(ret), K(candidate_zone_info_array));
    } else {
      bool can_switch_out = false;
      CandidateZoneInfo high_candidate = candidate_zone_info_array.at(0);
      for (int64_t i = 0; !can_switch_out && OB_SUCC(ret) && i < candidate_zone_info_array.count(); ++i) {
        const CandidateZoneInfo& this_info = candidate_zone_info_array.at(i);
        const ObZone& this_zone = this_info.zone_;
        // The zone_list is the excluded list.
        // If there is a zone outside the excluded list in the same level candidate,
        // we think that the leader can be cut away.
        // The interface is currently redundant, and the return value results does not need to be an array.
        if (cmp_operator.same_level(high_candidate, this_info)) {
          HEAP_VAR(ObZoneInfo, zone_info)
          {
            zone_info.zone_ = this_zone;
            if (OB_FAIL(zone_mgr_->get_zone(zone_info))) {
              LOG_WARN("fail to get zone info", K(ret));
            } else if (!zone_info.is_merging_ && !has_exist_in_array(zone_list, this_zone) &&
                       this_info.candidate_count_ >= partition_array.count()) {
              can_switch_out = true;
            } else {
              high_candidate = this_info;
            }
          }
        } else {
          // enough, no candidate with same level exists any more
          break;
        }
      }
      // The current interface is redundant, just return a bool type,
      // and currently return a bool array with the same length as the zone list
      if (OB_SUCC(ret)) {
        for (int64_t i = 0; OB_SUCC(ret) && i < zone_list.count(); ++i) {
          if (OB_FAIL(results.push_back(can_switch_out))) {
            LOG_WARN("fail to push back", K(ret));
          } else {
          }  // no more to do
        }
        if (!can_switch_out) {
          LOG_INFO("candidate zone infos", K(candidate_zone_info_array));
        }
      }
    }
  }
  return ret;
}

int ObLeaderCoordinator::update_daily_merge_switch_leader_result(
    const common::ObIArray<bool>& intermediate_results, common::ObIArray<bool>& final_results)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLeaderCoordinator not init", K(ret));
  } else if (intermediate_results.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "intermediate result count", intermediate_results.count());
  } else {
    if (final_results.count() == 0) {
      // The first update result, the final original result is empty, copy it directly
      for (int64_t i = 0; OB_SUCC(ret) && i < intermediate_results.count(); ++i) {
        if (OB_FAIL(final_results.push_back(intermediate_results.at(i)))) {
          LOG_WARN("fail to push back", K(ret));
        } else {
        }  // no more to do
      }
    } else if (final_results.count() != intermediate_results.count()) {
      // Not the first time to update the result,
      // the width of the final and intermediate results must be equal,
      // otherwise an error will be reported
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument",
          K(ret),
          "final results count",
          final_results.count(),
          "intermediate result count",
          intermediate_results.count());
    } else {
      for (int64_t i = 0; i < final_results.count(); ++i) {
        final_results.at(i) = final_results.at(i) && intermediate_results.at(i);
      }
    }
  }
  return ret;
}

int ObLeaderCoordinator::check_daily_merge_switch_leader(
    const common::ObIArray<common::ObZone>& zone_list, common::ObIArray<bool>& results)
{
  int ret = OB_SUCCESS;
  results.reset();
  ObArray<uint64_t> tenant_ids;
  PartitionInfoContainer partition_info_container;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLeaderCoordinator not init", K(ret));
  } else if (OB_UNLIKELY(zone_list.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "zone list count", zone_list.count());
  } else if (OB_FAIL(partition_info_container.init(pt_operator_, schema_service_, this))) {
    LOG_WARN("fail to init partition info container", K(ret));
  } else if (OB_FAIL(init_daily_merge_switch_leader_result(zone_list, results))) {
    LOG_WARN("fail to init daily merge switch leader result", K(ret));
  } else if (OB_FAIL(get_daily_merge_tenant_ids(tenant_ids))) {
    LOG_WARN("fail to get tenant ids", K(ret));
  } else if (tenant_ids.count() <= 0) {
    // good, no tenant need to check
  } else {
    bool need_continue = true;
    common::ObArray<bool> intermediate_results;
    int bak_ret = OB_SUCCESS;
    for (int64_t i = 0; need_continue && OB_SUCC(ret) && i < tenant_ids.count(); ++i) {
      intermediate_results.reset();
      const uint64_t tenant_id = tenant_ids.at(i);
      if (OB_FAIL(check_daily_merge_switch_leader_need_continue(results, need_continue))) {
        LOG_WARN("fail to check daily merge switch leader need continue", K(ret));
      } else if (!need_continue) {
        // all elements in results are false, no need to check any more
      } else if (OB_FAIL(partition_info_container.build_tenant_partition_info(tenant_id))) {
        LOG_WARN("fail to build tenant partition info", K(ret), K(tenant_id));
      } else if (OB_FAIL(check_daily_merge_switch_leader_by_tenant(
                     partition_info_container, tenant_id, zone_list, intermediate_results))) {
        if (OB_PARTITION_NOT_EXIST == ret || OB_NOT_MASTER == ret) {
          bak_ret = ret;
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to check daily merge switch leader by tenant", K(ret));
        }
      } else if (OB_FAIL(update_daily_merge_switch_leader_result(intermediate_results, results))) {
        LOG_WARN("fail to update daily merge switch leader result", K(ret));
      } else {
      }  // no more to do
    }
    if (OB_SUCCESS == ret) {
      if (OB_PARTITION_NOT_EXIST == bak_ret || OB_NOT_MASTER == bak_ret) {
        ret = OB_EAGAIN;
      } else if (OB_SUCCESS != bak_ret) {  // this shall never arrive now
        ret = bak_ret;
      } else {
      }  // no more to do
    }
  }
  return ret;
}

// During the daily merge, the system tenant is not cut to the master operation
// and the returned result does not contain the system tenant id
int ObLeaderCoordinator::get_daily_merge_tenant_ids(common::ObIArray<uint64_t>& tenant_ids)
{
  int ret = OB_SUCCESS;
  common::ObArray<uint64_t> tmp_tenant_ids;
  tenant_ids.reset();
  ObSchemaGetterGuard schema_guard;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLeaderCoordinator not init", K(ret));
  } else if (OB_FAIL(get_tenant_ids(tmp_tenant_ids))) {
    LOG_WARN("fail to get tenant ids", K(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret));
  } else {
    const ObTenantSchema* tenant_schema = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < tmp_tenant_ids.count(); ++i) {
      uint64_t this_tenant_id = tmp_tenant_ids.at(i);
      if (OB_SYS_TENANT_ID == this_tenant_id) {
        // filter the sys tenant id
      } else if (OB_FAIL(schema_guard.get_tenant_info(this_tenant_id, tenant_schema))) {
        LOG_WARN("fail to get tenant schema", K(ret), K(this_tenant_id));
      } else if (OB_UNLIKELY(NULL == tenant_schema)) {
        LOG_INFO("tenant may be deleted, ignore and go on", K(this_tenant_id));
      } else if (tenant_schema->get_full_replica_num() <= 1) {
        // Tenants with only one full replica need to be ignored and cannot be rotated and merged.
      } else if (OB_FAIL(tenant_ids.push_back(this_tenant_id))) {
        LOG_WARN("fail to push back", K(ret));
      } else {
      }  // no more to do
    }
  }
  return ret;
}

int ObLeaderCoordinator::init_daily_merge_switch_leader_result(
    const common::ObIArray<common::ObZone>& zone_list, common::ObIArray<bool>& results)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLeaderCoordinator not init", K(ret));
  } else if (OB_UNLIKELY(zone_list.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "zone list count", zone_list.count());
  } else {
    const int64_t zone_list_count = zone_list.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < zone_list_count; ++i) {
      if (OB_FAIL(results.push_back(true))) {
        LOG_WARN("fail to push back", K(ret));
      } else {
      }  // no more to do
    }
  }
  return ret;
}

// As long as at least one element in results is true, need_continue is true
// When all elements in results are false, need_continue is false,
// When results is empty, it is the initial state, and need continue returns true
int ObLeaderCoordinator::check_daily_merge_switch_leader_need_continue(
    const common::ObIArray<bool>& results, bool& check_switch_leader_need_continue)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLeaderCoordinator not init", K(ret));
  } else if (results.count() <= 0) {
    check_switch_leader_need_continue = true;
  } else {
    for (int64_t i = 0; i < results.count(); ++i) {
      if (results.at(i)) {
        check_switch_leader_need_continue = true;
        break;
      } else {
        // go on check next
        check_switch_leader_need_continue = false;
      }
    }
  }
  return ret;
}

int ObLeaderCoordinator::CursorContainer::reorganize()
{
  int ret = OB_SUCCESS;
  std::sort(cursor_array_.begin(), cursor_array_.end());
  return ret;
}

bool ObLeaderCoordinator::PartitionArrayCursor::operator<(const PartitionArrayCursor& that) const
{
  bool bool_ret = false;
  if (cur_leader_ < that.cur_leader_) {
    bool_ret = true;
  } else if (cur_leader_ == that.cur_leader_) {
    if (advised_leader_ < that.advised_leader_) {
      bool_ret = true;
    } else {
      bool_ret = false;
    }
  } else {
    bool_ret = false;
  }
  return bool_ret;
}

bool ObLeaderCoordinator::CandidateZoneInfoCmp::operator()(
    const CandidateZoneInfo& left, const CandidateZoneInfo& right)
{
  bool bool_ret = false;
  if (left.region_score_ == right.region_score_) {
    if (cmp_candidate_cnt(left.candidate_count_, right.candidate_count_) >= 0) {
      bool_ret = true;
    } else {
      bool_ret = false;
    }
  } else if (left.region_score_ < right.region_score_) {
    bool_ret = true;
  } else {
    bool_ret = false;
  }
  return bool_ret;
}

int64_t ObLeaderCoordinator::CandidateZoneInfoCmp::cmp_candidate_cnt(const int64_t left_cnt, const int64_t right_cnt)
{
  int64_t cmp = OB_SUCCESS;
  if (left_cnt == right_cnt) {
    cmp = 0;
  } else {
    // Temporarily modified to be strictly consistent
    if (left_cnt > right_cnt) {
      cmp = 1;
    } else {
      cmp = -1;
    }
  }
  return cmp;
}

bool ObLeaderCoordinator::CandidateZoneInfoCmp::same_level(
    const CandidateZoneInfo& left, const CandidateZoneInfo& right)
{
  return left.region_score_ == right.region_score_ &&
         0 == cmp_candidate_cnt(left.candidate_count_, right.candidate_count_);
}

int ObLeaderCoordinator::SwitchLeaderStrategy::check_tenant_on_server(
    const TenantUnit& tenant_unit, const ObAddr& server, bool& tenant_on_server)
{
  int ret = OB_SUCCESS;
  tenant_on_server = false;
  if (tenant_unit.count() <= 0 || !server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_unit is empty", K(server), K(tenant_unit), K(server));
  } else {
    FOREACH_CNT_X(unit, tenant_unit, !tenant_on_server)
    {
      if (server == unit->server_ || server == unit->migrate_from_server_) {
        tenant_on_server = true;
      }
    }
  }
  return ret;
}

int ObLeaderCoordinator::get_excluded_zone_array(common::ObIArray<common::ObZone>& excluded_zone_array) const
{
  int ret = OB_SUCCESS;
  common::ObArray<share::ObZoneInfo> infos;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(nullptr == zone_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("zone mgr ptr is null", K(ret));
  } else if (OB_FAIL(zone_mgr_->get_zone(infos))) {
    LOG_WARN("fail to get zone", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < infos.count(); ++i) {
      share::ObZoneInfo& info = infos.at(i);
      if ((info.is_merging_ && !info.suspend_merging_ && zone_mgr_->is_stagger_merge()) ||
          ObZoneStatus::ACTIVE != info.status_) {
        if (OB_FAIL(excluded_zone_array.push_back(info.zone_))) {
          LOG_WARN("fail to push back", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObLeaderCoordinator::SwitchLeaderStrategy::check_before_coordinate_partition(const Partition& partition,
    const TenantUnit& tenant_unit, const bool force, const ObPartitionKey& part_key, ObAddr& advised_leader,
    ObAddr& cur_leader, bool& need_switch)
{
  int ret = OB_SUCCESS;
  cur_leader.reset();
  need_switch = (force || host_.config_->enable_auto_leader_switch);
  // check whether current leader exist
  if (need_switch) {
    if (OB_UNLIKELY(nullptr == partition.info_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("info ptr is null", K(ret));
    } else if (OB_FAIL(partition.get_leader(cur_leader))) {
      if (OB_LEADER_NOT_EXIST != ret) {
        LOG_WARN("get_leader failed", K(ret));
      } else {
        ret = OB_SUCCESS;
        need_switch = false;
        LOG_WARN("partition don't have leader, can't switch", "partition", *partition.info_);
      }
    } else if (!cur_leader.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cur_leader is invalid", K(cur_leader), K(ret));
    } else {
      if (cur_leader == advised_leader) {
        need_switch = false;
      }
    }
  }

  // check is normal replica
  if (OB_SUCCESS == ret && need_switch) {
    bool can_be_elected = false;
    if (OB_FAIL(partition.check_replica_can_be_elected(advised_leader, can_be_elected))) {
      LOG_WARN("check_normal_replica failed", "partition", partition, K(advised_leader), K(ret));
    } else if (can_be_elected) {
      // advised leader is a normal one, do nothing
    } else {
      // Try switch to the same zone if no normal replica found in advised server,
      need_switch = false;
      ObZone zone;
      ObZone cur_leader_zone;
      if (OB_FAIL(host_.server_mgr_->get_server_zone(advised_leader, zone))) {
        LOG_WARN("get sever zone failed", K(ret), K(advised_leader));
      } else if (OB_FAIL(host_.server_mgr_->get_server_zone(cur_leader, cur_leader_zone))) {
        LOG_WARN("get server zone failed", K(ret), K(cur_leader));
      } else {
        FOREACH_CNT_X(r, partition.info_->get_replicas_v2(), OB_SUCC(ret))
        {
          if (REPLICA_STATUS_NORMAL == r->replica_status_ &&
              ObReplicaTypeCheck::is_can_elected_replica(r->replica_type_) && r->zone_ == zone &&
              r->server_ != cur_leader) {
            need_switch = true;
            LOG_INFO("no normal replica found in advise server, same zone server found",
                "partition",
                *partition.info_,
                K(advised_leader),
                "new_advised_leader",
                r->server_);
            advised_leader = r->server_;
            break;
          }
        }
      }
      if (OB_SUCC(ret) && !need_switch && zone != cur_leader_zone) {
        LOG_WARN("no normal replica found in advise server zone, can't switch leader",
            "partition",
            *partition.info_,
            K(advised_leader));
      }
    }
  }

  // check is in leader candidates
  if (OB_SUCCESS == ret && need_switch) {
    need_switch = has_exist_in_array(partition.candidates_, advised_leader);
    if (!need_switch) {
      LOG_WARN("advised_leader not in leader candidates, can't switch",
          K(advised_leader),
          "leader candidates",
          partition.candidates_,
          "partition",
          *partition.info_);
    }
  }

  // if tenant has not unit on advised_leader, don't switch leader to it
  if (OB_SUCCESS == ret && need_switch && !force) {
    bool tenant_on_server = false;
    if (OB_FAIL(check_tenant_on_server(tenant_unit, advised_leader, tenant_on_server))) {
      LOG_WARN("check tenant on server failed", K(tenant_unit), K(advised_leader), K(ret));
    } else {
      need_switch = tenant_on_server;
      if (!need_switch) {
        LOG_WARN("tenant not on advised_leader, can't switch", K(tenant_unit), K(advised_leader));
      }
    }
  }

  // check is __all_core_table partition need switch (need switch RS)
  if (OB_SUCCESS == ret && need_switch && !force) {
    if (part_key.get_table_id() == combine_id(OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID)) {
      int64_t high_priority_task_cnt = 0;
      int64_t low_priority_task_cnt = 0;
      if (OB_FAIL(host_.rebalance_task_mgr_->get_tenant_task_info_cnt(
              OB_SYS_TENANT_ID, high_priority_task_cnt, low_priority_task_cnt))) {
        LOG_WARN("get system tenant rebalance task cnt failed", K(ret));
      } else {
        bool has_rebalance_task = (0 != high_priority_task_cnt || 0 != low_priority_task_cnt);
        if (host_.switch_info_stat_.get_self_in_candidate() && (has_rebalance_task || !host_.sys_balanced_)) {
          need_switch = false;
          LOG_INFO("system tenant has rebalance task, or sys tenant not balanced, "
                   "don't switch __all_core_table leader right now",
              K(high_priority_task_cnt),
              K(low_priority_task_cnt));
        }
      }
    }
  }
  return ret;
}

int ObLeaderCoordinator::SwitchLeaderStrategy::coordinate_partitions_per_tg(TenantUnit& tenant_unit,
    PartitionArray& partition_array, PartitionArrayCursor& cursor, const bool force,
    ExpectedLeaderWaitOperator& leader_wait_operator, SwitchLeaderListAsyncOperator& async_rpc_operator,
    bool& do_switch_leader)
{
  int ret = OB_SUCCESS;
  ObPartitionKey first_part_key;
  uint64_t first_tg_id = OB_INVALID_ID;
  if (OB_UNLIKELY(tenant_unit.count() <= 0) || OB_UNLIKELY(partition_array.count() <= 0) ||
      OB_UNLIKELY(NULL == host_.srv_rpc_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument",
        K(ret),
        "partition count",
        partition_array.count(),
        "tenant_unit count",
        tenant_unit.count(),
        KP(host_.srv_rpc_proxy_));
  } else if (cursor.part_idx_ >= partition_array.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should not be here, when partition array switch finish", K(ret));
  } else if (OB_FAIL(partition_array.at(cursor.part_idx_).get_partition_key(first_part_key))) {
    LOG_WARN("fail to get partition key", K(ret));
  } else if (FALSE_IT(first_tg_id = partition_array.at(cursor.part_idx_).tg_id_)) {
    // never be here
  } else {
    do_switch_leader = false;
    ObAddr cur_leader;
    ObAddr new_leader;
    bool need_switch = false;
    const uint64_t tenant_id = extract_tenant_id(partition_array.get_tablegroup_id());
    const uint64_t pure_tg_id = extract_pure_id(partition_array.get_tablegroup_id());
    const bool is_sys_tg = (pure_tg_id == OB_SYS_TABLEGROUP_ID || tenant_id == OB_SYS_TENANT_ID);
    for (int64_t i = cursor.part_idx_; OB_SUCC(ret) && i < partition_array.count(); ++i) {
      new_leader = cursor.advised_leader_;
      ObPartitionKey part_key;
      bool can_switch = false;
      if (OB_FAIL(host_.check_cancel())) {
        LOG_WARN("rs is stopped", K(ret));
      } else if (OB_FAIL(partition_array.at(i).get_partition_key(part_key))) {
        LOG_WARN("fail to get partition key", K(ret));
      } else if (OB_FAIL(check_before_coordinate_partition(
                     partition_array.at(i), tenant_unit, force, part_key, new_leader, cur_leader, need_switch))) {
        LOG_WARN("failed to check before coordinate partition", K(ret), K(i));
      } else if (!need_switch) {
        const bool is_new_leader = false;
        ++cursor.part_idx_;
        // Push up the corresponding value of the cursor
        if (OB_FAIL(host_.count_leader(partition_array.at(i), is_new_leader))) {
          LOG_WARN("count_leader failed", "partition", partition_array.at(i), K(ret));
        }
      } else if (!new_leader.is_valid() || !cur_leader.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid leaders", K(ret), K(new_leader), K(cur_leader));
      } else if (OB_FAIL(leader_wait_operator.check_can_switch_more_leader(new_leader, cur_leader, can_switch))) {
        LOG_WARN("fail to check can switch more leader", K(ret));
      } else if (!can_switch) {
        break;
      } else {
        do_switch_leader = true;
        ++cursor.part_idx_;
        // The partition cuts the main point and pushes the corresponding value of the cursor larger
        if (((async_rpc_operator.get_saved_cur_leader().is_valid() &&
                 async_rpc_operator.get_saved_new_leader().is_valid()) &&
                (async_rpc_operator.get_saved_cur_leader() != cur_leader ||
                    async_rpc_operator.get_saved_new_leader() != new_leader)) ||
            (async_rpc_operator.reach_accumulate_threshold())) {
          if (OB_FAIL(async_rpc_operator.send_requests(async_rpc_operator.get_saved_cur_leader(),
                  async_rpc_operator.get_saved_new_leader(),
                  leader_wait_operator))) {
            LOG_WARN("fail to batch send rpc request",
                K(ret),
                "saved_cur_leader",
                async_rpc_operator.get_saved_cur_leader(),
                "saved_new_leader",
                async_rpc_operator.get_saved_new_leader());
          } else if (async_rpc_operator.reach_send_to_wait_threshold()) {
            if (OB_FAIL(async_rpc_operator.wait())) {
              LOG_WARN("proxy wait failed", K(ret));
            } else {
              async_rpc_operator.reuse();
            }
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(host_.switch_info_stat_.inc_leader_count(new_leader, true /*is_new_leader*/))) {
          LOG_WARN("count leader failed", K(ret), "partition", partition_array.at(i));
        } else if (OB_FAIL(async_rpc_operator.accumulate_request(part_key, is_sys_tg))) {
          LOG_WARN("fail to accumulate request", K(ret), K(part_key));
        } else {
          async_rpc_operator.set_saved_cur_leader(cur_leader);
          async_rpc_operator.set_saved_new_leader(new_leader);
        }
      }
    }  // end of for
    if (OB_SUCC(ret) && do_switch_leader) {
      (void)host_.try_update_switch_leader_event(first_part_key.get_tenant_id(),
          first_tg_id,
          first_part_key.get_partition_id(),
          cursor.advised_leader_,
          async_rpc_operator.get_saved_cur_leader());
    }
  }
  return ret;
}

int ObLeaderCoordinator::ConcurrentSwitchLeaderStrategy::init_cursor_queue_map()
{
  int ret = OB_SUCCESS;
  const common::ObZone zone;  // empty zone means all zone
  int64_t alive_count = 0;
  int64_t not_alive_count = 0;
  int64_t bucket_num = 0;
  if (OB_UNLIKELY(NULL == host_.server_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("host server mgr ptr is null", K(ret));
  } else if (OB_FAIL(host_.server_mgr_->get_server_count(zone, alive_count, not_alive_count))) {
    LOG_WARN("fail to get server count", K(ret));
  } else if (FALSE_IT(bucket_num = (2 * (alive_count + not_alive_count)))) {
    // will never be here
  } else if (OB_FAIL(cursor_queue_map_.create(bucket_num, ObModIds::OB_RS_LEADER_COORDINATOR))) {
    LOG_WARN("fail to create dst cursor list map", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < cursor_container_.cursor_array_.count(); ++i) {
      PartitionArrayCursor& cursor = cursor_container_.cursor_array_.at(i);
      CursorLink* cursor_link = NULL;
      void* tmp_ptr = NULL;
      if (NULL == (tmp_ptr = link_node_allocator_.alloc(sizeof(CursorLink)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory", K(ret));
      } else if (NULL == (cursor_link = new (tmp_ptr) CursorLink())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to construct cursor node", K(ret));
      } else if (FALSE_IT(cursor_link->cursor_ptr_ = &cursor)) {
        // will never be here
      } else {
        CursorQueue* cursor_queue = NULL;
        int tmp_ret = cursor_queue_map_.get_refactored(cursor.advised_leader_, cursor_queue);
        if (OB_HASH_NOT_EXIST == tmp_ret) {
          CursorQueue* new_queue = NULL;
          void* tmp_ptr = NULL;
          if (NULL == (tmp_ptr = link_node_allocator_.alloc(sizeof(CursorQueue)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to allocate memory", K(ret));
          } else if (NULL == (new_queue = new (tmp_ptr) CursorQueue())) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to allocate memory", K(ret));
          } else if (!new_queue->cursor_list_[NOT_START_LIST].add_last(cursor_link)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("fail to add cursor to list", K(ret));
          } else if (OB_FAIL(cursor_queue_map_.set_refactored(cursor.advised_leader_, new_queue, 0 /*overwrite*/))) {
            LOG_WARN("fail to set refactored", K(ret));
          }
        } else if (OB_SUCCESS == tmp_ret) {
          if (OB_UNLIKELY(NULL == cursor_queue)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("cursor queue ptr is null", K(ret));
          } else if (!cursor_queue->cursor_list_[NOT_START_LIST].add_last(cursor_link)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("fail to add cursor to list", K(ret));
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to get cursor queue from map", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObLeaderCoordinator::ConcurrentSwitchLeaderStrategy::try_remove_cursor_queue_map(
    CursorLink* cursor_link, const LinkType link_type)
{
  int ret = OB_SUCCESS;
  if (NULL == cursor_link || link_type >= LINK_TYPE_MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(cursor_link), K(link_type));
  } else if (NULL == cursor_link->cursor_ptr_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cursor_ptr is null", K(ret));
  } else {
    const common::ObAddr& key = cursor_link->cursor_ptr_->advised_leader_;
    CursorQueue* cursor_queue = NULL;
    if (OB_FAIL(cursor_queue_map_.get_refactored(key, cursor_queue))) {
      LOG_WARN("fail to get from map");
    } else if (NULL == cursor_queue) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cursor queue ptr is null", K(ret));
    } else {
      (void)cursor_queue->cursor_list_[link_type].remove(cursor_link);
      if (!cursor_queue->empty()) {
        // ignore
      } else {
        (void)cursor_queue_map_.erase_refactored(key);
      }
    }
  }
  return ret;
}

bool ObLeaderCoordinator::ConcurrentSwitchLeaderStrategy::is_switch_finished()
{
  return cursor_queue_map_.size() <= 0;
}

int ObLeaderCoordinator::ConcurrentSwitchLeaderStrategy::get_cursor_link_from_list(
    ExpectedLeaderWaitOperator& leader_wait_operator, CursorList& cursor_list, CursorLink*& cursor_link,
    bool& no_leader_pa)
{
  int ret = OB_SUCCESS;
  bool found = false;
  cursor_link = NULL;
  for (CursorLink* node = cursor_list.get_first(); !found && OB_SUCC(ret) && node != cursor_list.get_header();
       node = node->get_next()) {
    PartitionArrayCursor* cursor = NULL;
    no_leader_pa = true;
    PartitionArray* partition_array = NULL;
    if (NULL == node) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("node ptr is null", K(ret));
    } else if (NULL == (cursor = node->cursor_ptr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cursor ptr is null", K(ret));
    } else if (cursor->switch_finish()) {
      // In fact, this group of partition array has been cut master, we return it to the upper layer,
      // and the upper layer will perform the subsequent operations completed by cut leader
      no_leader_pa = false;
      found = true;
      cursor_link = node;
    } else if (cursor->array_idx_ >= partition_arrays_.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cursor array idx unexpected", K(ret));
    } else if (NULL == (partition_array = (partition_arrays_.at(cursor->array_idx_)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition array ptr is null", K(ret));
    } else {
      for (int64_t i = cursor->part_idx_; !found && OB_SUCC(ret) && i < cursor->part_cnt_; ++i) {
        Partition& partition = partition_array->at(i);
        const common::ObAddr& new_leader = cursor->advised_leader_;
        common::ObAddr old_leader;
        int tmp_ret = partition.get_leader(old_leader);
        if (OB_LEADER_NOT_EXIST == tmp_ret) {
          // next partition
        } else if (OB_SUCCESS == tmp_ret) {
          no_leader_pa = false;
          // Reorganizing the remaining partitions in pg is not all no-master
          bool can_switch = false;
          if (OB_FAIL(leader_wait_operator.check_can_switch_more_leader(new_leader, old_leader, can_switch))) {
            LOG_WARN("fail to check can switch more leader", K(ret));
          } else if (!can_switch) {
            break;
          } else {
            found = true;
            cursor_link = node;
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to get leader", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (!no_leader_pa) {
      } else {
        // Return this all no-master pg
        found = true;
        cursor_link = node;
      }
    }
  }
  return ret;
}

int ObLeaderCoordinator::ConcurrentSwitchLeaderStrategy::get_next_cursor_link(
    ExpectedLeaderWaitOperator& leader_wait_operator, CursorLink*& cursor_link, LinkType& link_type, bool& no_leader_pa)
{
  int ret = OB_SUCCESS;
  cursor_link = NULL;
  link_type = LINK_TYPE_MAX;
  bool found = false;
  for (CursorQueueMap::iterator iter = cursor_queue_map_.begin();
       !found && OB_SUCC(ret) && iter != cursor_queue_map_.end();
       ++iter) {
    const common::ObAddr& new_leader = iter->first;
    bool can_switch = false;
    if (OB_FAIL(leader_wait_operator.check_can_switch_more_new_leader(new_leader, can_switch))) {
      LOG_WARN("fail to check can switch more new leader", K(ret), K(new_leader));
    } else if (!can_switch) {
      // bypass
    } else {
      for (int32_t i = 0; !found && OB_SUCC(ret) && i < LINK_TYPE_MAX; ++i) {
        if (OB_UNLIKELY(NULL == iter->second)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("iter second is invalid", K(ret));
        } else {
          CursorList& cursor_list = iter->second->cursor_list_[i];
          if (OB_FAIL(get_cursor_link_from_list(leader_wait_operator, cursor_list, cursor_link, no_leader_pa))) {
            LOG_WARN("fail to get cursor link from list", K(ret));
          } else if (NULL == cursor_link) {
            // next
          } else {
            found = true;
            link_type = (LinkType)(i);
          }
        }
      }
    }
  }
  return ret;
}

int ObLeaderCoordinator::ConcurrentSwitchLeaderStrategy::try_reconnect_cursor_queue(
    CursorLink* cursor_link, const LinkType link_type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == cursor_link || link_type >= LINK_TYPE_MAX)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(cursor_link), K(link_type));
  } else if (START_SWITCH_LIST == link_type) {
    if (NULL == cursor_link->cursor_ptr_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cursor ptr is null", K(ret));
    } else if (cursor_link->cursor_ptr_->switch_finish()) {
      if (OB_FAIL(try_remove_cursor_queue_map(cursor_link, link_type))) {
        LOG_WARN("fail to try remove cursor queue map", K(ret));
      }
    } else {
    }  // Not finished, keep it in the start switch list
  } else if (NOT_START_LIST == link_type) {
    if (NULL == cursor_link->cursor_ptr_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cursor ptr is null", K(ret));
    } else if (cursor_link->cursor_ptr_->switch_finish()) {
      if (OB_FAIL(try_remove_cursor_queue_map(cursor_link, link_type))) {
        LOG_WARN("fail to try remove cursor queue map", K(ret));
      }
    } else if (!cursor_link->cursor_ptr_->switch_start()) {
      // Still not starting to cut, keep it in the not start list
    } else {
      CursorQueue* cursor_queue = NULL;
      const common::ObAddr& key = cursor_link->cursor_ptr_->advised_leader_;
      if (OB_FAIL(cursor_queue_map_.get_refactored(key, cursor_queue))) {
        LOG_WARN("fail to get from cursor queue map", K(ret), K(key));
      } else if (OB_UNLIKELY(NULL == cursor_queue)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cursor queue ptr is null", K(ret));
      } else {
        (void)cursor_queue->cursor_list_[NOT_START_LIST].remove(cursor_link);
        if (!cursor_queue->cursor_list_[START_SWITCH_LIST].add_last(cursor_link)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to add to new list", K(ret));
        }
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("link type unexpected", K(ret), K(link_type));
  }
  return ret;
}

int ObLeaderCoordinator::ConcurrentSwitchLeaderStrategy::execute(
    TenantUnit& tenant_unit, ExpectedLeaderWaitOperator& leader_wait_operator, const bool force)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_cursor_queue_map())) {
    LOG_WARN("fail to init", K(ret));
  } else {
    SwitchLeaderListAsyncOperator async_rpc_operator(
        *(host_.srv_rpc_proxy_), &ObSrvRpcProxy::switch_leader_list_async, *(host_.config_));
    while (OB_SUCC(ret) && !is_switch_finished() && OB_SUCC(host_.check_cancel())) {
      bool no_leader_pa = false;
      CursorLink* cursor_link = NULL;
      LinkType link_type = LINK_TYPE_MAX;
      PartitionArray* array = NULL;
      bool need_switch = false;
      bool do_switch_leader = false;
      if (OB_FAIL(get_next_cursor_link(leader_wait_operator, cursor_link, link_type, no_leader_pa))) {
        LOG_WARN("fail to get next cursor node", K(ret));
      } else if (NULL == cursor_link) {
        // Did not get the partition array that can cut the leader,
        // this wave cut master has reached the upper limit
        if (OB_FAIL(async_rpc_operator.send_requests(async_rpc_operator.get_saved_cur_leader(),
                async_rpc_operator.get_saved_new_leader(),
                leader_wait_operator))) {
          LOG_WARN("fail to send requests",
              K(ret),
              "saved_cur_leader",
              async_rpc_operator.get_saved_cur_leader(),
              "saved_new_leader",
              async_rpc_operator.get_saved_new_leader());
        } else if (OB_FAIL(async_rpc_operator.wait())) {
          LOG_WARN("fail to wait switch leader", K(ret));
        } else if (FALSE_IT(async_rpc_operator.reuse())) {
          // shall never be here
        } else if (OB_FAIL(leader_wait_operator.wait())) {
          LOG_WARN("fail to wait leader", K(ret));
        }
      } else if (LINK_TYPE_MAX == link_type) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected link type", K(ret), K(link_type));
      } else if (no_leader_pa) {  // Get a set of partition array without leader, don't cut, throw away
        need_switch = false;
        if (OB_FAIL(try_remove_cursor_queue_map(cursor_link, link_type))) {
          LOG_WARN("fail to try remove cursor queue map", K(ret));
        }
      } else if (NULL == cursor_link->cursor_ptr_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cursor_ptr_ is null", K(ret));
      } else if (cursor_link->cursor_ptr_->switch_finish()) {
        need_switch = false;
        if (OB_FAIL(try_remove_cursor_queue_map(cursor_link, link_type))) {
          LOG_WARN("fail to try remove cursor queue map", K(ret));
        }
      } else if (cursor_link->cursor_ptr_->array_idx_ >= partition_arrays_.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("array idex value unexpected",
            K(ret),
            "array_idx",
            cursor_link->cursor_ptr_->array_idx_,
            "array_count",
            partition_arrays_.count());
      } else if (NULL == (array = partition_arrays_.at(cursor_link->cursor_ptr_->array_idx_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("partition array ptr is null", K(ret));
      } else if (cursor_link->cursor_ptr_->switch_start()) {
        need_switch = true;
      } else if (cursor_link->cursor_ptr_->ignore_switch_percent_ || force) {
        need_switch = true;
      } else {
        ObSEArray<common::ObAddr, 1> candidate_leaders;
        bool can_switch_more = false;
        if (OB_FAIL(candidate_leaders.push_back(cursor_link->cursor_ptr_->advised_leader_))) {
          LOG_WARN("fail to push back", K(ret));
        } else if (OB_FAIL(host_.switch_info_stat_.check_smooth_switch_condition(candidate_leaders, can_switch_more))) {
          LOG_WARN("fail to check smooth switch condition", K(ret));
        } else if (!can_switch_more) {
          if (OB_FAIL(host_.count_leader(*array, false /*is_new_leader*/))) {
            LOG_WARN("fail to count leader", K(ret));
          } else {
            need_switch = false;
            if (OB_FAIL(try_remove_cursor_queue_map(cursor_link, link_type))) {
              LOG_WARN("fail to try remove cursor queue map", K(ret));
            }
          }
        } else {
          need_switch = true;
        }
      }
      if (OB_FAIL(ret) || !need_switch) {
      } else if (NULL == array || NULL == cursor_link || NULL == cursor_link->cursor_ptr_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("variable ptr is null", K(ret));
      } else if (OB_FAIL(coordinate_partitions_per_tg(tenant_unit,
                     *array,
                     *(cursor_link->cursor_ptr_),
                     force,
                     leader_wait_operator,
                     async_rpc_operator,
                     do_switch_leader))) {
        LOG_WARN("fail to coordinate partition per tg", K(ret));
      } else if (!do_switch_leader) {
        // can't cut, this turn has reached the upper limit
        if (OB_FAIL(async_rpc_operator.send_requests(async_rpc_operator.get_saved_cur_leader(),
                async_rpc_operator.get_saved_new_leader(),
                leader_wait_operator))) {
          LOG_WARN("fail to send requests",
              K(ret),
              "saved_cur_leader",
              async_rpc_operator.get_saved_cur_leader(),
              "saved_new_leader",
              async_rpc_operator.get_saved_new_leader());
        } else if (OB_FAIL(async_rpc_operator.wait())) {
          LOG_WARN("fail to wait switch leader", K(ret));
        } else if (FALSE_IT(async_rpc_operator.reuse())) {
          // shall never be here
        } else if (OB_FAIL(leader_wait_operator.wait())) {
          LOG_WARN("fail to wait leader", K(ret));
        }
      } else {
        if (OB_FAIL(try_reconnect_cursor_queue(cursor_link, link_type))) {
          LOG_WARN("fail to try reconnect cursor link", K(ret));
        }
      }
    }
    // need to try send_requests since async_rpc_operator may accumulate rpc request
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(async_rpc_operator.send_requests(async_rpc_operator.get_saved_cur_leader(),
                   async_rpc_operator.get_saved_new_leader(),
                   leader_wait_operator))) {
      LOG_WARN("fail to batch send rpc request",
          K(ret),
          "saved_cur_leader",
          async_rpc_operator.get_saved_cur_leader(),
          "saved_new_leader",
          async_rpc_operator.get_saved_new_leader());
    }
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = async_rpc_operator.wait())) {
      LOG_WARN("proxy wait failed", K(ret), K(tmp_ret));
      ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
    } else {
    }  // wait SUCC, do nothing
  }
  return ret;
}

int64_t ObLeaderCoordinator::get_schedule_interval() const
{
  int64 time_us = idling_.get_idle_interval_us();
  if (time_us < 4 * ObLeaderCoordinator::WAIT_SWITCH_LEADER_TIMEOUT) {
    time_us = 4 * ObLeaderCoordinator::WAIT_SWITCH_LEADER_TIMEOUT;
    // The maximum interval time to wait for a successful leader switch
  }
  return time_us;
}

// new leader strategy
ObLeaderCoordinator::NewLeaderStrategy::NewLeaderStrategy(ObIAllocator& allocator,
    ObAllServerLeaderMsg& all_server_leader_msg, ObAllPartitionSwitchMsg& all_part_switch_msg,
    ServerReplicaMsgContainer::ServerReplicaCounterMap& server_replica_counter_map,
    ServerReplicaMsgContainer::ZoneReplicaCounterMap& zone_replica_counter_map)
    : allocator_(allocator),
      all_server_leader_msg_(all_server_leader_msg),
      all_part_switch_msg_(all_part_switch_msg),
      server_replica_counter_map_(server_replica_counter_map),
      zone_replica_counter_map_(zone_replica_counter_map),
      global_map_info_(),
      first_local_map_info_(),
      second_local_map_info_(),
      self_allocator_(),
      inited_(false)
{}

ObLeaderCoordinator::NewLeaderStrategy::~NewLeaderStrategy()
{}

int ObLeaderCoordinator::NewLeaderStrategy::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(global_map_info_.create(2 * MAX_SERVER_COUNT, "GloMapInfo"))) {
    LOG_WARN("fail to create global map info", K(ret));
  } else if (OB_FAIL(first_local_map_info_.create(2 * MAX_SERVER_COUNT, "FirLocMapInfo"))) {
    LOG_WARN("fail to create first local map info", K(ret));
  } else if (OB_FAIL(second_local_map_info_.create(2 * MAX_SERVER_COUNT, "SecLocMapInfo"))) {
    LOG_WARN("fail to create second local map info", K(ret));
  } else {
    inited_ = true;
  }
  return ret;
}

int ObLeaderCoordinator::NewLeaderStrategy::get_sorted_server(ObIArray<common::ObAddr>& server_arr)
{
  int ret = OB_SUCCESS;
  // Priority push the server with curr_leader_cnt_ greater than max_leader_cnt_ into the queue
  for (ObAllServerLeaderMsg::iterator iter = all_server_leader_msg_.begin();
       iter != all_server_leader_msg_.end() && OB_SUCC(ret);
       ++iter) {
    if (OB_ISNULL(iter->second)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("server leader msg is NULL", K(ret));
    } else {
      const common::ObZone& zone = iter->second->zone_;
      ZoneReplicaCounter zr_counter;
      if (OB_FAIL((zone_replica_counter_map_.get_refactored(zone, zr_counter)))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          if (iter->second->curr_leader_cnt_ > 0) {
            if (OB_FAIL(server_arr.push_back(iter->first))) {
              LOG_WARN("fail to push back server", K(ret), "server", iter->first);
            }
          }
        } else {
          LOG_WARN("zr counte ptr null unexpected", K(ret), K(zone));
        }
      } else if (zr_counter.max_exp_leader_cnt_ < iter->second->curr_leader_cnt_) {
        if (OB_FAIL(server_arr.push_back(iter->first))) {
          LOG_WARN("fail to push back server", K(ret), "server", iter->first);
        }
      }
    }
  }
  return ret;
}

// First cut the exp_max_leader larger than exp_max_leader into exp_max_leader
int ObLeaderCoordinator::NewLeaderStrategy::switch_to_exp_max_leader(const ObIArray<common::ObAddr>& server_arr)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < server_arr.count() && OB_SUCC(ret); ++i) {
    const common::ObAddr& server = server_arr.at(i);
    const bool to_exp_max_leader = true;
    bool is_balance = false;
    int64_t tmp_max_round = 0;
    ObServerLeaderMsg* server_leader_msg = nullptr;
    if (OB_FAIL(all_server_leader_msg_.get_refactored(server, server_leader_msg))) {
      LOG_WARN("fail to get server leader msg", KR(ret), K(server));
    } else if (OB_ISNULL(server_leader_msg)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("server leader msg is null", KR(ret), K(server));
    } else {
      const int64_t max_round = server_leader_msg->curr_leader_cnt_;
      while (!is_balance && tmp_max_round < max_round) {
        if (OB_FAIL(execute_new_leader_strategy(server, to_exp_max_leader, is_balance))) {
          LOG_WARN("fail to do new leader strateg", KR(ret), K(server));
        } else {
          tmp_max_round++;
        }
      }
      if (!is_balance) {
        LOG_INFO("server leader is not balance",
            K(server),
            K(to_exp_max_leader),
            K(server_leader_msg->curr_leader_cnt_),
            K(server_leader_msg->zone_));
      }
      LOG_INFO("finish switch to exp max leader", KR(ret), K(server), K(tmp_max_round));
    }
  }
  return ret;
}

int ObLeaderCoordinator::NewLeaderStrategy::execute()
{
  int ret = OB_SUCCESS;
  ObArray<common::ObAddr> server_arr;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(get_sorted_server(server_arr))) {
    LOG_WARN("fail to init server leader msg", K(ret));
  } else if (OB_FAIL(switch_to_exp_max_leader(server_arr))) {
    LOG_WARN("fail to switch to exp max leader", KR(ret), K(server_arr));
  } else {
    for (ObAllServerLeaderMsg::iterator iter = all_server_leader_msg_.begin();
         iter != all_server_leader_msg_.end() && OB_SUCC(ret);
         ++iter) {
      bool need_execute = false;
      ObServerLeaderMsg* server_leader_msg = iter->second;
      const common::ObAddr& server = iter->first;
      const bool to_exp_max_leader = false;
      ZoneReplicaCounter zr_counter;
      if (OB_ISNULL(server_leader_msg)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("server leader msg is null", K(ret));
      } else if (OB_FAIL(zone_replica_counter_map_.get_refactored(server_leader_msg->zone_, zr_counter))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to get follower zr counter", K(ret), "zone", server_leader_msg->zone_);
        }
      } else {
      }                    // nothing todo
      if (OB_FAIL(ret)) {  // nothing todo
      } else if (OB_FAIL(check_do_need_execute_new_strategy(server_leader_msg, need_execute))) {
        LOG_WARN("fail to check do need execute new strategy", K(ret));
      } else if (need_execute) {
        LOG_INFO("start do new leader strategy",
            "zone",
            server_leader_msg->zone_,
            "server",
            server,
            "server_cnt",
            all_server_leader_msg_.size(),
            "valid_server_cnt",
            server_replica_counter_map_.size(),
            "valid_zone_cnt",
            zone_replica_counter_map_.size(),
            "curr_leader_cnt",
            server_leader_msg->curr_leader_cnt_,
            K(zr_counter));
        bool is_balance = false;
        if (OB_FAIL(execute_new_leader_strategy(server, to_exp_max_leader, is_balance))) {
          LOG_WARN("fail to do new_leader strategy", K(ret), K(server), K(to_exp_max_leader));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(zone_replica_counter_map_.get_refactored(server_leader_msg->zone_, zr_counter))) {
          if (OB_HASH_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("fail to get zr counter", KR(ret), "zone", server_leader_msg->zone_);
          }
        } else {
        }  // nothing todo
      }
      LOG_INFO("finish new leader strategy",
          KR(ret),
          "zone",
          server_leader_msg->zone_,
          "server",
          server,
          "server_cnt",
          all_server_leader_msg_.size(),
          "curr_leader_cnt",
          server_leader_msg->curr_leader_cnt_,
          "valid_server_cnt",
          server_replica_counter_map_.size(),
          "valid_zone_cnt",
          zone_replica_counter_map_.size(),
          K(zr_counter));
    }
  }
  return ret;
}

int ObLeaderCoordinator::NewLeaderStrategy::check_do_need_execute_new_strategy(
    ObServerLeaderMsg* server_leader_msg, bool& need_execute)
{
  int ret = OB_SUCCESS;
  need_execute = false;
  ZoneReplicaCounter zr_counter;
  common::ObZone zone;
  if (OB_ISNULL(server_leader_msg)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("server leader msg is null", KR(ret));
  } else if (FALSE_IT(zone = server_leader_msg->zone_)) {
    // never be here
  } else if (OB_FAIL((zone_replica_counter_map_.get_refactored(zone, zr_counter)))) {
    if (OB_HASH_NOT_EXIST == ret) {
      if (server_leader_msg->curr_leader_cnt_ > 0) {
        need_execute = true;
        // all the leaders on the server are moved away
      }
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("zr counte ptr null unexpected", K(ret), K(zone));
    }
  } else if (zr_counter.total_leader_cnt_ > zr_counter.expected_leader_cnt_) {
    if (server_leader_msg->curr_leader_cnt_ > zr_counter.min_exp_leader_cnt_) {
      need_execute = true;
    }
  } else if (zr_counter.curr_max_leader_server_cnt_ <= zr_counter.max_leader_server_cnt_) {
    if (server_leader_msg->curr_leader_cnt_ > zr_counter.max_exp_leader_cnt_) {
      need_execute = true;
    } else {
      // nothing todo
    }
  } else {
    if (server_leader_msg->curr_leader_cnt_ > zr_counter.min_exp_leader_cnt_) {
      need_execute = true;
    } else {
      // nothing todo
    }
  }
  return ret;
}

int ObLeaderCoordinator::NewLeaderStrategy::check_can_do_switch_leader(ObServerLeaderMsg* leader_server_leader_msg,
    ObServerLeaderMsg* follower_server_leader_msg, const ObZone& follower_zone, bool& can_do_switch)
{
  int ret = OB_SUCCESS;
  can_do_switch = false;
  ZoneReplicaCounter follower_zr_counter;
  ZoneReplicaCounter leader_zr_counter;
  if (OB_FAIL(zone_replica_counter_map_.get_refactored(follower_zone, follower_zr_counter))) {
    if (OB_HASH_NOT_EXIST == ret) {
      // not the invalid zone , Can't become leader
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get follower zr counter", K(ret), K(follower_zone));
    }
  } else if (OB_ISNULL(leader_server_leader_msg) || OB_ISNULL(follower_server_leader_msg)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("follower or leader server leader msg is NULL", K(ret));
  } else if (OB_FAIL(zone_replica_counter_map_.get_refactored(leader_server_leader_msg->zone_, leader_zr_counter))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      // The absence of zone means that the leader above must exceed max_exp_cnt
      // Exceeding max_exp_cnt does not consider the number of leaders on the zone
      if (follower_server_leader_msg->curr_leader_cnt_ + 1 <= follower_zr_counter.max_exp_leader_cnt_) {
        can_do_switch = true;
      }
    } else {
      LOG_WARN("fail to get leadre zr counter", K(ret), "zone", follower_server_leader_msg->zone_);
    }
  } else if (leader_server_leader_msg->curr_leader_cnt_ > leader_zr_counter.max_exp_leader_cnt_) {
    if (follower_server_leader_msg->curr_leader_cnt_ + 1 <= follower_zr_counter.max_exp_leader_cnt_) {
      can_do_switch = true;
    } else {
      // nothing todo
    }
  } else {
    if (follower_zr_counter.curr_max_leader_server_cnt_ < follower_zr_counter.max_leader_server_cnt_) {
      if (follower_server_leader_msg->curr_leader_cnt_ + 1 <= follower_zr_counter.max_exp_leader_cnt_) {
        can_do_switch = true;
      } else {
        // nothing todo
      }
    } else {
      if (follower_server_leader_msg->curr_leader_cnt_ + 1 <= follower_zr_counter.min_exp_leader_cnt_) {
        can_do_switch = true;
      } else {
        // nothing todo
      }
    }
  }
  return ret;
}

int ObLeaderCoordinator::NewLeaderStrategy::execute_new_leader_strategy(
    const common::ObAddr& addr, const bool to_exp_max_leader, bool& is_balance)
{
  int ret = OB_SUCCESS;
  is_balance = false;
  bool has_switch_leader = false;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!addr.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("addr is valid", K(ret));
  } else if (OB_FAIL(push_first_server_in_map(addr))) {
    LOG_WARN("fail to push first server in map", K(ret));
  } else {
    ObSerMapInfo* parent_map_info = &first_local_map_info_;
    ObSerMapInfo* children_map_info = &second_local_map_info_;
    ObSerMapInfo* tmp_map_info = nullptr;
    while ((!is_balance) && OB_SUCC(ret)) {
      if (OB_ISNULL(parent_map_info) || OB_ISNULL(children_map_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("parent map info or children map info is null", K(ret));
      } else if (OB_FAIL(do_one_round_leader_switch(
                     *parent_map_info, *children_map_info, addr, to_exp_max_leader, has_switch_leader, is_balance))) {
        LOG_WARN("fail to do one round leader switch", K(ret));
      } else {
        tmp_map_info = parent_map_info;
        parent_map_info = children_map_info;
        children_map_info = tmp_map_info;
      }
      if ((parent_map_info->size() == 0 && children_map_info->size() == 0) || has_switch_leader) {
        break;
      }
    }
    parent_map_info = nullptr;
    children_map_info = nullptr;
    tmp_map_info = nullptr;
    if (OB_SUCC(ret)) {
      if (OB_FAIL(all_map_reuse())) {
        LOG_WARN("fail to map reuse", K(ret));
      }
    }
  }
  return ret;
}

int ObLeaderCoordinator::NewLeaderStrategy::all_map_reuse()
{
  int ret = OB_SUCCESS;
  for (ObSerMapInfo::iterator iter = first_local_map_info_.begin(); OB_SUCC(ret) && iter != first_local_map_info_.end();
       iter++) {
    if (OB_FAIL(do_map_reuse(iter->first, first_local_map_info_))) {
      LOG_WARN("fail to do map reuse", K(ret), "addr", iter->first);
    }
  }
  for (ObSerMapInfo::iterator iter = second_local_map_info_.begin();
       OB_SUCC(ret) && iter != second_local_map_info_.end();
       iter++) {
    if (OB_FAIL(do_map_reuse(iter->first, second_local_map_info_))) {
      LOG_WARN("fail to do map reuse", K(ret), "addr", iter->first);
    }
  }
  for (ObSerMapInfo::iterator iter = global_map_info_.begin(); OB_SUCC(ret) && iter != global_map_info_.end(); iter++) {
    if (OB_FAIL(do_map_reuse(iter->first, global_map_info_))) {
      LOG_WARN("fail to do map reuse", K(ret), "addr", iter->first);
    }
  }
  global_map_info_.reuse();
  first_local_map_info_.reuse();
  second_local_map_info_.reuse();
  self_allocator_.reuse();
  return ret;
}

int ObLeaderCoordinator::NewLeaderStrategy::do_map_reuse(const ObAddr& addr, ObSerMapInfo& map_info)
{
  int ret = OB_SUCCESS;
  ObServerMsg* server_msg = nullptr;
  int tmp_ret = map_info.get_refactored(addr, server_msg);
  if (OB_SUCCESS == tmp_ret) {
    if (nullptr != server_msg) {
      server_msg->~ObServerMsg();
      self_allocator_.free(server_msg);
      server_msg = nullptr;
    }
  } else {
    ret = tmp_ret;
    LOG_WARN("fail to get server msg", K(ret));
  }
  return ret;
}

int ObLeaderCoordinator::NewLeaderStrategy::push_first_server_in_map(const ObAddr& original_addr)
{
  int ret = OB_SUCCESS;
  void* buff = self_allocator_.alloc(sizeof(ObServerMsg));
  ObServerMsg* first_server_msg = nullptr;
  if (OB_ISNULL(buff)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc failed", K(ret));
  } else if (nullptr == (first_server_msg = new (buff) ObServerMsg())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail create", K(ret));
  } else {
    // Push the first node to be balanced into the map,
    // because every time you push a node,
    // you need to check whether the node is already in the map
    //
    // It is not allowed to put the server into the map,
    // and the node is the end mark, and the parent is NULL
    first_server_msg->self_addr_ = original_addr;
    first_server_msg->parent_node_ = nullptr;
    first_server_msg->head_ = nullptr;
    first_server_msg->partition_cnt_ = 0;
    if (OB_FAIL(first_local_map_info_.set_refactored(original_addr, first_server_msg, 0 /*not overwrite*/))) {
      LOG_WARN("fail to set refactored", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
    if (nullptr != first_server_msg) {
      first_server_msg->~ObServerMsg();
      self_allocator_.free(first_server_msg);
      first_server_msg = nullptr;
    } else if (nullptr != buff) {
      self_allocator_.free(buff);
      buff = nullptr;
    }
  }
  return ret;
}

int ObLeaderCoordinator::NewLeaderStrategy::do_one_round_leader_switch(ObSerMapInfo& parent_map_info,
    ObSerMapInfo& children_map_info, const ObAddr& original_addr, const bool to_exp_max_leader, bool& has_switch_leader,
    bool& is_balance)
{
  int ret = OB_SUCCESS;
  is_balance = false;
  ObArray<ObServerMsg*> erase_addr_arr;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObSerMapInfo::iterator iter = parent_map_info.begin();
    // First traverse all nodes of this layer for the first time
    for (; iter != parent_map_info.end() && OB_SUCC(ret) && !is_balance && !has_switch_leader; ++iter) {
      if (OB_ISNULL(iter->second)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("server info is null", K(ret));
      } else {
        if (OB_FAIL(first_traversal_server_leader_info(
                iter->second, original_addr, to_exp_max_leader, has_switch_leader, is_balance))) {
          // Each traversal tries to cut away the leader of 1 partition,
          // and update all_server_leader_msg_ and other information
          LOG_WARN("fail to traversal first server leader msg", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (is_balance || has_switch_leader) {
        // nothing todo
      } else if (OB_FAIL(get_erase_addr(parent_map_info, erase_addr_arr, original_addr))) {
        LOG_WARN("fail to get erase addr", K(ret));
      } else if (OB_FAIL(erase_addr(erase_addr_arr))) {
        LOG_WARN("fail to erase addr", K(ret));
      } else {
        if (children_map_info.size() != 0) {
          // Ensure child is empty
          children_map_info.reuse();
        }
        iter = parent_map_info.begin();
        // If it is not balanced, perform a second traversal of all nodes in this layer
        for (; iter != parent_map_info.end() && OB_SUCC(ret); ++iter) {
          if (OB_FAIL(second_traversal_server_leader_info(
                  parent_map_info, children_map_info, original_addr, iter->second))) {
            // In the second traversal,
            // partitions that cannot be directly cut away will be pushed into the linked list.
            LOG_WARN("fail traversal second server leader msg", K(ret));
          }
        }
      }
    }
  }
  if (OB_SUCC(ret) && !is_balance && !has_switch_leader) {
    // Put the server in parent_map into global_map
    ObSerMapInfo::iterator iter = parent_map_info.begin();
    for (; iter != parent_map_info.end() && OB_SUCC(ret); ++iter) {
      if (OB_ISNULL(iter->second)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("push an exist map", K(ret));
      } else if (OB_FAIL(global_map_info_.set_refactored(iter->first, iter->second, 0 /*overwrite*/))) {
        LOG_WARN("fail to set global map info", K(ret), "server", iter->first);
      }
    }
    parent_map_info.reuse();
  }
  return ret;
}

int ObLeaderCoordinator::NewLeaderStrategy::first_traversal_server_leader_info(ObServerMsg* server_msg,
    const ObAddr& original_addr, const bool to_exp_max_leader, bool& has_switch_leader, bool& is_balance)
{
  int ret = OB_SUCCESS;
  is_balance = false;
  has_switch_leader = false;
  ObServerLeaderMsg* first_server_leader_msg = nullptr;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(server_msg)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("server msg is NULL", K(ret));
  } else if (OB_FAIL(all_server_leader_msg_.get_refactored(server_msg->self_addr_, first_server_leader_msg))) {
    LOG_WARN("fail to get server leader msg", K(ret));
  } else if (OB_ISNULL(first_server_leader_msg)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("server_leader_msg is NULL", K(ret));
  } else {
    DLIST_FOREACH_REMOVESAFE_X(partition_leader_msg,
        first_server_leader_msg->leader_partition_list_,
        OB_SUCC(ret) && !has_switch_leader && !is_balance)
    {
      if (OB_ISNULL(partition_leader_msg)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("partition leader msg is NULL", K(ret));
      } else if (OB_FAIL(check_leader_can_switch(
                     first_server_leader_msg, partition_leader_msg, has_switch_leader, server_msg))) {
        LOG_WARN("fail to check leader can switch", K(ret));
      } else if (OB_FAIL(check_server_already_balance(original_addr, to_exp_max_leader, is_balance))) {
        LOG_WARN("fail to check server already balance", K(ret));
      }
    }
  }
  return ret;
}

int ObLeaderCoordinator::NewLeaderStrategy::second_traversal_server_leader_info(ObSerMapInfo& parent_map_info,
    ObSerMapInfo& children_map_info, const ObAddr& original_addr, ObServerMsg* parent_server_msg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    // Re-acquire server_msg, it may have been updated,
    // and the rest are leaders that cannot be cut away directly
    ObServerLeaderMsg* original_server_leader_msg = nullptr;
    ObServerLeaderMsg* current_server_leader_msg = nullptr;
    if (OB_FAIL(all_server_leader_msg_.get_refactored(original_addr, original_server_leader_msg))) {
      LOG_WARN("fail to get orginal server leader msg", K(ret));
    } else if (OB_ISNULL(original_server_leader_msg)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("server_leader_msg is NULL", K(ret));
    } else if (OB_ISNULL(parent_server_msg)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("parent server msg is null", K(ret));
    } else if (OB_FAIL(
                   all_server_leader_msg_.get_refactored(parent_server_msg->self_addr_, current_server_leader_msg))) {
      LOG_WARN("fail to get server leader msg", K(ret));
    } else {
      const ObZone& original_zone = original_server_leader_msg->zone_;
      ZoneReplicaCounter original_zr_counter;
      int tmp_leader_ret = zone_replica_counter_map_.get_refactored(original_zone, original_zr_counter);
      int64_t max_exp_leader_cnt = 0;
      int64_t min_exp_leader_cnt = 0;
      if (tmp_leader_ret == OB_SUCCESS) {
        max_exp_leader_cnt = original_zr_counter.max_leader_server_cnt_;
        min_exp_leader_cnt = original_zr_counter.min_leader_server_cnt_;
      } else if (OB_HASH_NOT_EXIST == tmp_leader_ret) {
        max_exp_leader_cnt = 0;
        min_exp_leader_cnt = 0;
      } else {
        ret = tmp_leader_ret;
        LOG_WARN("fail to get original zr counter", KR(ret), K(original_zone));
      }
      if (OB_SUCC(ret)) {
        // the leader_partition stored in curr_server_leader_msg is not directly cut
        DLIST_FOREACH_REMOVESAFE_X(
            partition_leader_msg, current_server_leader_msg->leader_partition_list_, OB_SUCC(ret))
        {
          if (OB_ISNULL(partition_leader_msg)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("partition leader msg is NULL", K(ret));
          } else if (OB_FAIL(update_server_map(partition_leader_msg,
                         parent_server_msg,
                         parent_map_info,
                         children_map_info,
                         parent_server_msg->self_addr_))) {
            LOG_WARN("fail to update server map", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObLeaderCoordinator::NewLeaderStrategy::check_leader_can_switch(ObServerLeaderMsg* leader_server_leader_msg,
    ObPartitionMsg* partition_leader_msg, bool& has_switch_leader, ObServerMsg* server_msg)
{
  int ret = OB_SUCCESS;
  ObPartitionKey part_key;
  has_switch_leader = false;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(partition_leader_msg) || OB_ISNULL(server_msg)) {
    LOG_WARN("partition leader msg or server msg is NULL",
        K(ret),
        "leader_msg",
        partition_leader_msg,
        "server_msg",
        server_msg);
  } else if (OB_ISNULL(partition_leader_msg->partition_msg_)) {
    LOG_WARN("partition_msg is NULL", K(ret));
  } else if (OB_ISNULL(partition_leader_msg->partition_msg_->info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition is NULL", K(ret));
  } else {
    // Traverse all the servers where the replicas are located
    const common::ObIArray<ObPartitionReplica>& partition_replica_arr =
        partition_leader_msg->partition_msg_->info_->get_replicas_v2();
    const ObAddr& leader_addr = server_msg->self_addr_;
    for (int64_t i = 0; i < partition_replica_arr.count() && OB_SUCC(ret) && !has_switch_leader; ++i) {
      const ObAddr& follower_addr = partition_replica_arr.at(i).server_;
      bool can_switch_leader = false;
      if (follower_addr != leader_addr) {
        // The address is not equal to the current leader
        ObServerLeaderMsg* follower_server_leader_msg = NULL;
        ServerReplicaCounter follower_sr_counter;
        int tmp_ret = server_replica_counter_map_.get_refactored(follower_addr, follower_sr_counter);
        if (OB_HASH_NOT_EXIST == tmp_ret) {
          // The server is not in server_replica_counter_map_,
          // it may be in the stop state and cannot become the leader
        } else if (OB_SUCCESS != tmp_ret) {
          ret = tmp_ret;
          LOG_WARN("fail to get refactored", K(ret));
        } else if (OB_FAIL(all_server_leader_msg_.get_refactored(follower_addr, follower_server_leader_msg))) {
          LOG_WARN("fail to get follower_server_leader_msg", K(ret));
        } else if (OB_ISNULL(follower_server_leader_msg)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("server follower map is NULL", K(ret));
        } else if (OB_FAIL(check_can_switch_leader(leader_server_leader_msg,
                       follower_server_leader_msg,
                       partition_replica_arr.at(i),
                       can_switch_leader))) {
          LOG_WARN("fail to can switch leader", K(ret));
        } else if (can_switch_leader) {
          if (OB_FAIL(change_server_leader_info(
                  follower_server_leader_msg, partition_leader_msg, leader_server_leader_msg, follower_addr))) {
            LOG_WARN("fail to change server link msg", K(ret));
          } else if (OB_FAIL(change_parent_leader_info(server_msg))) {
            LOG_WARN("fail to change parent leader msg", K(ret));
          } else {
            has_switch_leader = true;
          }
        } else {
          // nothing todo
        }
      } else {
        // nothing todo
      }
    }
  }
  return ret;
}

int ObLeaderCoordinator::NewLeaderStrategy::check_can_switch_leader(ObServerLeaderMsg* leader_server_leader_msg,
    ObServerLeaderMsg* follower_server_leader_msg, const ObPartitionReplica& partition_replica, bool& can_switch_leader)
{
  int ret = OB_SUCCESS;
  can_switch_leader = false;
  bool can_do_switch = false;
  if (OB_FAIL(check_can_do_switch_leader(
          leader_server_leader_msg, follower_server_leader_msg, partition_replica.zone_, can_do_switch))) {
    LOG_WARN("fail to check can do switch leader", K(ret), "zone", partition_replica.zone_, K(can_do_switch));
  } else if (can_do_switch) {
    if (partition_replica.is_in_service() && REPLICA_TYPE_FULL == partition_replica.replica_type_) {
      can_switch_leader = true;
    } else {
      // nothing todo
    }
  } else {
    // nothing todo
  }
  return ret;
}

int ObLeaderCoordinator::NewLeaderStrategy::change_server_leader_info(ObServerLeaderMsg* follower_server_leader_msg,
    ObPartitionMsg* partition_leader_msg, ObServerLeaderMsg* leader_server_leader_msg, const ObAddr& follower_addr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    if (OB_ISNULL(leader_server_leader_msg) || OB_ISNULL(follower_server_leader_msg)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("follower or leader server leader msg is NULL", K(ret));
    } else if (OB_FAIL(change_partition_leader_address(
                   follower_server_leader_msg, leader_server_leader_msg, partition_leader_msg, follower_addr))) {
      LOG_WARN("fail to change partition leader address", K(ret));
    }
  }
  return ret;
}

int ObLeaderCoordinator::NewLeaderStrategy::change_parent_leader_info(ObServerMsg* server_msg)
{
  int ret = OB_SUCCESS;
  ObServerMsg* tmp_server_msg = server_msg;
  if (OB_ISNULL(tmp_server_msg)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tmp_server msg is NULL", K(ret));
  } else {
    while (!OB_ISNULL(tmp_server_msg->parent_node_) && tmp_server_msg->partition_cnt_ > 0) {
      ObPartitionMsg* partition_msg = tmp_server_msg->head_;
      ObServerLeaderMsg* tmp_follower_server_leader_msg = nullptr;
      ObServerLeaderMsg* leader_server_leader_msg = nullptr;
      if (OB_ISNULL(partition_msg)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("partition msg is NULL", K(ret));
      } else if (FALSE_IT(tmp_server_msg->head_ = partition_msg->next_part_)) {
        // never be here
      } else if (FALSE_IT(tmp_server_msg->partition_cnt_--)) {
        // never be here
      } else if (OB_FAIL(all_server_leader_msg_.get_refactored(
                     tmp_server_msg->self_addr_, tmp_follower_server_leader_msg))) {
        LOG_WARN("fail to get tmp follower server leader msg", K(ret));
      } else if (OB_ISNULL(tmp_follower_server_leader_msg)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tmp follower server leader msg is NULL", K(ret));
      } else if (tmp_server_msg->parent_node_->self_addr_ == tmp_server_msg->self_addr_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("server is error", K(ret));
      } else if (OB_FAIL(all_server_leader_msg_.get_refactored(
                     tmp_server_msg->parent_node_->self_addr_, leader_server_leader_msg))) {
        LOG_WARN("fail to get leader server leader msg", K(ret));
      } else if (OB_FAIL(change_server_leader_info(tmp_follower_server_leader_msg,
                     partition_msg,
                     leader_server_leader_msg,
                     tmp_server_msg->self_addr_))) {
        LOG_WARN("fail to change server map msg", K(ret));
      } else {
        tmp_server_msg = tmp_server_msg->parent_node_;
      }
    }
  }
  return ret;
}

int ObLeaderCoordinator::NewLeaderStrategy::get_erase_addr(
    ObSerMapInfo& parent_map_info, ObIArray<ObServerMsg*>& erase_addr_arr, const common::ObAddr& original_addr)
{
  int ret = OB_SUCCESS;
  // Remove the partition_cnt of 0 from the map,
  // Because there is no push to the global at this time,
  // need to traverse the parent_map
  for (ObSerMapInfo::iterator iter = parent_map_info.begin(); iter != parent_map_info.end() && OB_SUCC(ret); ++iter) {
    ObServerMsg* server_msg = iter->second;
    if (iter->first == original_addr) {
      // nothing todo
    } else if (OB_ISNULL(server_msg)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("server msg is null", K(ret), "server", iter->first);
    } else if (0 == server_msg->partition_cnt_ && nullptr == server_msg->head_) {
      // If partition_key is already 0, delete the node from the map
      if (OB_FAIL(erase_addr_arr.push_back(server_msg))) {
        LOG_WARN("fail to push back erase_addr_arr", K(ret), "server", server_msg->self_addr_);
      }
    } else if ((0 != server_msg->partition_cnt_ && nullptr == server_msg->head_) ||
               (0 == server_msg->partition_cnt_ && nullptr != server_msg->head_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("server msg partition cnt and head are not match",
          K(ret),
          K(server_msg->partition_cnt_),
          K(server_msg->head_));
    }
  }
  for (ObSerMapInfo::iterator iter = global_map_info_.begin(); iter != global_map_info_.end() && OB_SUCC(ret); ++iter) {
    ObServerMsg* server_msg = iter->second;
    if (iter->first == original_addr) {
      // nothing todo
    } else if (OB_ISNULL(server_msg)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("server msg is null", K(ret), "server", iter->first);
    } else if (0 == server_msg->partition_cnt_ && nullptr == server_msg->head_) {
      // If partition_key is already 0, delete the node from the map
      if (OB_FAIL(erase_addr_arr.push_back(server_msg))) {
        LOG_WARN("fail to push back erase_addr_arr", K(ret), "server", server_msg->self_addr_);
      }
    } else if ((0 != server_msg->partition_cnt_ && nullptr == server_msg->head_) ||
               (0 == server_msg->partition_cnt_ && nullptr != server_msg->head_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("server msg partition cnt and head are not match",
          K(ret),
          K(server_msg->partition_cnt_),
          K(server_msg->head_));
    }
  }
  return ret;
}

int ObLeaderCoordinator::NewLeaderStrategy::erase_addr(const ObIArray<ObServerMsg*>& erase_addr_arr)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < erase_addr_arr.count() && OB_SUCC(ret); ++i) {
    ObServerMsg* server_msg = erase_addr_arr.at(i);
    if (OB_ISNULL(server_msg)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("server msg is NULL", K(ret));
    } else {
      int global_tmp_ret = global_map_info_.erase_refactored(server_msg->self_addr_);
      int first_tmp_ret = first_local_map_info_.erase_refactored(server_msg->self_addr_);
      int second_tmp_ret = second_local_map_info_.erase_refactored(server_msg->self_addr_);
      if (OB_HASH_NOT_EXIST != global_tmp_ret && OB_SUCCESS != global_tmp_ret) {
        ret = global_tmp_ret;
      } else if (OB_HASH_NOT_EXIST != first_tmp_ret && OB_SUCCESS != first_tmp_ret) {
        ret = first_tmp_ret;
      } else if (OB_HASH_NOT_EXIST != second_tmp_ret && OB_SUCCESS != second_tmp_ret) {
        ret = second_tmp_ret;
      } else {
        // nothing todo
      }
      if (nullptr != server_msg) {
        server_msg->~ObServerMsg();
        self_allocator_.free(server_msg);
        server_msg = nullptr;
      }
    }
  }
  return ret;
}

int ObLeaderCoordinator::NewLeaderStrategy::change_partition_leader_address(
    ObServerLeaderMsg* follower_server_leader_msg, ObServerLeaderMsg* leader_server_leader_msg,
    ObPartitionMsg* partition_leader_msg, const ObAddr& follower_addr)
// Only the last time it was found that the leader can be directly migrated to its follower
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(leader_server_leader_msg) || OB_ISNULL(follower_server_leader_msg) ||
             OB_ISNULL(partition_leader_msg)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("leader or follower server leader msg is NULL",
        K(ret),
        K(leader_server_leader_msg),
        K(follower_server_leader_msg),
        K(partition_leader_msg));
  } else if (OB_ISNULL(partition_leader_msg->partition_msg_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition msg is NULL", K(ret));
  } else {
    leader_server_leader_msg->curr_leader_cnt_--;
    follower_server_leader_msg->curr_leader_cnt_++;
    ObPartitionKey leader_partition_key;
    // 1. Update all_server_leader_msg_, update the partition_list of leader and follower
    if (OB_FAIL(partition_leader_msg->partition_msg_->get_partition_key(leader_partition_key))) {
      LOG_WARN("fail to get partition key", K(ret));
    } else if (OB_ISNULL(leader_server_leader_msg->leader_partition_list_.remove(partition_leader_msg))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to erase leader partition", K(ret));
    } else if (!follower_server_leader_msg->leader_partition_list_.add_last(partition_leader_msg)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to set follower partition", K(ret));
    } else if (OB_FAIL(update_partition_switch_info(leader_partition_key, partition_leader_msg, follower_addr))) {
      LOG_WARN("fail to update partition switch info", K(ret));
    } else if (OB_FAIL(update_zone_replica_counter(leader_server_leader_msg, follower_server_leader_msg))) {
      LOG_WARN("fail to update zone replica counter", K(ret));
    }
  }
  return ret;
}

int ObLeaderCoordinator::NewLeaderStrategy::update_partition_switch_info(
    ObPartitionKey& leader_partition_key, ObPartitionMsg* partition_leader_msg, const ObAddr& follower_addr)
{
  int ret = OB_SUCCESS;
  // 2. Update all_part_swtich_msg, which partition leader will finally be put on which server
  ObPartitionSwitchMsg* part_switch_msg = nullptr;
  if (OB_FAIL(all_part_switch_msg_.get_refactored(leader_partition_key, part_switch_msg))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      void* buff = allocator_.alloc(sizeof(ObPartitionSwitchMsg));
      if (OB_ISNULL(buff)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc failed", K(ret));
      } else if (nullptr == (part_switch_msg = new (buff) ObPartitionSwitchMsg())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail create", K(ret));
      } else if (OB_FAIL(partition_leader_msg->partition_msg_->get_leader(part_switch_msg->original_leader_addr_))) {
        LOG_WARN("fail to get leader", K(ret));
      } else if (FALSE_IT(part_switch_msg->final_leader_addr_ = follower_addr)) {
        // never be here
      } else if (OB_FAIL(
                     all_part_switch_msg_.set_refactored(leader_partition_key, part_switch_msg, 0 /*not overwrite*/))) {
        LOG_WARN("fail to set all partition switch msg", K(ret));
      }
      if (OB_FAIL(ret)) {
        if (part_switch_msg != nullptr) {
          part_switch_msg->~ObPartitionSwitchMsg();
          allocator_.free(part_switch_msg);
          part_switch_msg = nullptr;
        } else if (buff != nullptr) {
          allocator_.free(buff);
          buff = nullptr;
        }
      }
    } else {
      LOG_WARN("fail to get partition switch msg", K(ret));
    }
  } else {
    if (part_switch_msg->original_leader_addr_ == follower_addr) {
      // Migrate back to the original address, delete the partition from the map
      if (OB_FAIL(all_part_switch_msg_.erase_refactored(leader_partition_key))) {
        LOG_WARN("fail to erase all part switch msg", K(ret));
      } else if (nullptr != part_switch_msg) {
        part_switch_msg->~ObPartitionSwitchMsg();
        allocator_.free(part_switch_msg);
        part_switch_msg = nullptr;
      }
    } else {
      part_switch_msg->final_leader_addr_ = follower_addr;
    }
  }
  return ret;
}

int ObLeaderCoordinator::NewLeaderStrategy::update_zone_replica_counter(
    ObServerLeaderMsg* leader_server_leader_msg, ObServerLeaderMsg* follower_server_leader_msg)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(leader_server_leader_msg) || OB_ISNULL(follower_server_leader_msg)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("leader or follower server leader msg is null", K(ret));
  } else {
    const ObZone& leader_zone = leader_server_leader_msg->zone_;
    const ObZone& follower_zone = follower_server_leader_msg->zone_;
    ZoneReplicaCounter leader_zr_counter;
    ZoneReplicaCounter follower_zr_counter;
    int tmp_leader_ret = zone_replica_counter_map_.get_refactored(leader_zone, leader_zr_counter);
    int tmp_follower_ret = zone_replica_counter_map_.get_refactored(follower_zone, follower_zr_counter);
    ret = ((tmp_leader_ret == OB_SUCCESS || tmp_leader_ret == OB_HASH_NOT_EXIST)
               ? ((tmp_follower_ret == OB_SUCCESS || tmp_follower_ret == OB_HASH_NOT_EXIST) ? ret : tmp_follower_ret)
               : tmp_leader_ret);
    if (OB_FAIL(ret)) {
      LOG_WARN("fail to get leader or follower zone replica", K(ret), K(leader_zone), K(follower_zone));
    } else {
      if (tmp_leader_ret != OB_HASH_NOT_EXIST) {
        leader_zr_counter.total_leader_cnt_--;
        if (leader_server_leader_msg->curr_leader_cnt_ < leader_zr_counter.max_exp_leader_cnt_ &&
            leader_zr_counter.curr_max_leader_server_cnt_ > 0) {
          leader_zr_counter.curr_max_leader_server_cnt_--;
        }
        if (OB_FAIL(zone_replica_counter_map_.set_refactored(leader_zone, leader_zr_counter, 1 /*overwrite*/))) {
          LOG_WARN("fail to set leader zone replica", K(ret), K(leader_zone));
        }
      }
      if (OB_SUCC(ret)) {
        if (tmp_follower_ret != OB_HASH_NOT_EXIST) {
          follower_zr_counter.total_leader_cnt_++;
          if (follower_server_leader_msg->curr_leader_cnt_ >= follower_zr_counter.max_exp_leader_cnt_ &&
              follower_zr_counter.curr_max_leader_server_cnt_ < follower_zr_counter.available_server_cnt_) {
            follower_zr_counter.curr_max_leader_server_cnt_++;
          }
          if (OB_FAIL(zone_replica_counter_map_.set_refactored(follower_zone, follower_zr_counter, 1 /*overwrite*/))) {
            LOG_WARN("fail to set follower zone replica", K(ret), K(follower_zone));
          }
        }
      }
    }
  }
  return ret;
}

int ObLeaderCoordinator::NewLeaderStrategy::check_server_already_balance(
    const ObAddr& addr, const bool to_exp_max_leader, bool& is_balance)
{
  int ret = OB_SUCCESS;
  is_balance = false;
  ObServerLeaderMsg* server_leader_msg = nullptr;
  ObZone zone;
  ZoneReplicaCounter zr_counter;
  if (OB_FAIL(all_server_leader_msg_.get_refactored(addr, server_leader_msg))) {
    LOG_WARN("fail to get server leader msg", K(ret));
  } else if (OB_ISNULL(server_leader_msg)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("server leader msg is NULL", K(ret));
  } else if (FALSE_IT(zone = server_leader_msg->zone_)) {
    // never be here
  } else if (OB_FAIL(zone_replica_counter_map_.get_refactored(zone, zr_counter))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      if (server_leader_msg->curr_leader_cnt_ == 0) {
        is_balance = true;
      }
    } else {
      LOG_WARN("fail to get zr counter", K(ret), K(zone));
    }
  } else if (to_exp_max_leader) {
    // Only cut the leader into max_leader
    if (server_leader_msg->curr_leader_cnt_ <= zr_counter.max_exp_leader_cnt_) {
      is_balance = true;
    }
  } else {
    if ((zr_counter.total_leader_cnt_ <= zr_counter.expected_leader_cnt_ &&
            zr_counter.curr_max_leader_server_cnt_ <= zr_counter.max_leader_server_cnt_ &&
            server_leader_msg->curr_leader_cnt_ <= zr_counter.max_exp_leader_cnt_) ||
        server_leader_msg->curr_leader_cnt_ <= zr_counter.min_exp_leader_cnt_) {
      is_balance = true;
    }
  }
  return ret;
}

int ObLeaderCoordinator::NewLeaderStrategy::update_server_map(ObPartitionMsg* partition_leader_msg,
    ObServerMsg* parent_server_msg, ObSerMapInfo& parent_map_info, ObSerMapInfo& children_map_info,
    const ObAddr& curr_leader_addr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(partition_leader_msg)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition leader msg is NULL", K(ret));
  } else if (OB_ISNULL(partition_leader_msg->partition_msg_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition msg is NULL", K(ret));
  } else if (OB_ISNULL(partition_leader_msg->partition_msg_->info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition info is NULL", K(ret));
  } else {
    const common::ObIArray<ObPartitionReplica>& partition_replica_arr =
        partition_leader_msg->partition_msg_->info_->get_replicas_v2();
    for (int64_t i = 0; i < partition_replica_arr.count() && OB_SUCC(ret); ++i) {
      if (partition_replica_arr.at(i).server_ != curr_leader_addr) {
        if (OB_FAIL(do_update_server_map(partition_replica_arr.at(i).server_,
                partition_leader_msg,  // parent
                parent_server_msg,
                parent_map_info,
                children_map_info))) {
          LOG_WARN("fail do update server list", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObLeaderCoordinator::NewLeaderStrategy::do_update_server_map(const ObAddr& follower_addr,
    ObPartitionMsg* partition_leader_msg, ObServerMsg* parent_server_msg, ObSerMapInfo& parent_map_info,
    ObSerMapInfo& children_map_info)
{
  int ret = OB_SUCCESS;
  // 1. Check if it is in msp
  bool in_map = false;
  bool in_leader_map = false;
  ObServerMsg* server_msg = nullptr;
  // need to not have both global_map and parent_map and push in leader_map,
  // because parent_map is not updated to global at this time
  int global_tmp_ret = global_map_info_.get_refactored(follower_addr, server_msg);
  int parent_tmp_ret = parent_map_info.get_refactored(follower_addr, server_msg);
  if (OB_HASH_NOT_EXIST == global_tmp_ret && OB_HASH_NOT_EXIST == parent_tmp_ret) {
  } else if (OB_SUCCESS == global_tmp_ret || OB_SUCCESS == parent_tmp_ret) {
    in_map = true;
    // There may be a possibility of success and failure,
    // but since in_map is true, subsequent operations will not be entered and can be ignored
  } else {
    ret = (global_tmp_ret == OB_SUCCESS ? parent_tmp_ret : global_tmp_ret);
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(server_replica_counter_map_.get(follower_addr))) {
      // nothing todo
    } else {
      in_leader_map = true;
    }
  }
  // 2.Check whether it is in the local_map.
  //   If it is, it is not allowed to continue to add,
  //   otherwise it is added to the map
  if (!in_map && in_leader_map && OB_SUCC(ret)) {
    if (OB_FAIL(
            do_update_children_server_map(children_map_info, partition_leader_msg, parent_server_msg, follower_addr))) {
      LOG_WARN("fail to do update children server map", K(ret));
    }
  }
  return ret;
}

int ObLeaderCoordinator::NewLeaderStrategy::do_update_children_server_map(ObSerMapInfo& children_map_info,
    ObPartitionMsg* partition_leader_msg, ObServerMsg* parent_server_msg, const ObAddr& follower_addr)
{
  int ret = OB_SUCCESS;
  ObServerMsg* server_msg = nullptr;
  if (OB_ISNULL(parent_server_msg)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parent server msg is NULL", K(ret));
  } else if (OB_ISNULL(partition_leader_msg)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition_leader_msgis NULL", K(ret));
  } else if (OB_FAIL(children_map_info.get_refactored(follower_addr, server_msg))) {
    // not in map
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      void* buff = self_allocator_.alloc(sizeof(ObServerMsg));
      if (OB_ISNULL(buff)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc failed", K(ret));
      } else if (nullptr == (server_msg = new (buff) ObServerMsg())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail create", K(ret));
      } else {
        server_msg->self_addr_ = follower_addr;
        server_msg->parent_node_ = parent_server_msg;
        partition_leader_msg->next_part_ = nullptr;
        // Make sure that the next_part of msg inserted for the first time is empty
        server_msg->head_ = partition_leader_msg;  // first push
        server_msg->partition_cnt_++;
        if (OB_FAIL(children_map_info.set_refactored(follower_addr, server_msg, 0 /*not overwrite*/))) {
          LOG_WARN("fail to set server partition map local", K(ret));
        } else {
          // nothing todo
        }
      }
      if (OB_FAIL(ret)) {
        if (nullptr != server_msg) {
          server_msg->~ObServerMsg();
          self_allocator_.free(server_msg);
          server_msg = nullptr;
        } else if (nullptr != buff) {
          self_allocator_.free(buff);
          buff = nullptr;
        }
      }
    } else {
      LOG_WARN("fail to get server partition map local", K(ret));
    }
  } else {
    // In the map, it is not allowed to add partition to it
  }
  return ret;
}

bool ObLeaderCoordinator::TablegroupEntity::has_self_partition() const
{
  return tg_schema_.get_binding();
}

uint64_t ObLeaderCoordinator::TablegroupEntity::get_partition_entity_id() const
{
  return tg_schema_.get_tablegroup_id();
}

uint64_t ObLeaderCoordinator::TablegroupEntity::get_tablegroup_id() const
{
  return tg_schema_.get_tablegroup_id();
}

}  // end namespace rootserver
}  // end namespace oceanbase
