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

#include "ob_shrink_resource_pool_checker.h"
#include "lib/container/ob_array.h"
#include "lib/allocator/page_arena.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/ob_srv_rpc_proxy.h"
#include "share/ob_replica_info.h"
#include "share/partition_table/ob_partition_table_operator.h"
#include "share/partition_table/ob_partition_table_iterator.h"
#include "share/partition_table/ob_partition_info.h"
#include "share/partition_table/ob_replica_filter.h"
#include "share/schema/ob_schema_mgr.h"
#include "observer/ob_server_struct.h"
#include "ob_leader_coordinator.h"
#include "ob_unit_manager.h"
#include "ob_alloc_replica_strategy.h"
#include "ob_balance_info.h"

namespace oceanbase {
using namespace common;
using namespace share;
using namespace share::schema;
namespace rootserver {

int ObShrinkResourcePoolChecker::ZoneReplicas::assign(const ObShrinkResourcePoolChecker::ZoneReplicas& other)
{
  int ret = OB_SUCCESS;
  zone_ = other.zone_;
  if (OB_FAIL(copy_assign(in_pool_paxos_replicas_, other.in_pool_paxos_replicas_))) {
    LOG_WARN("fail to copy assign", K(ret));
  } else if (OB_FAIL(copy_assign(not_in_pool_paxos_replicas_, other.not_in_pool_paxos_replicas_))) {
    LOG_WARN("fail to copy assign", K(ret));
  } else if (OB_FAIL(copy_assign(in_pool_non_paxos_replicas_, other.in_pool_non_paxos_replicas_))) {
    LOG_WARN("fail to copy assign", K(ret));
  } else if (OB_FAIL(copy_assign(not_in_pool_non_paxos_replicas_, other.not_in_pool_non_paxos_replicas_))) {
    LOG_WARN("fail to copy assign", K(ret));
  } else {
  }  // good
  return ret;
}

int ObShrinkResourcePoolChecker::check_stop()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObShrinkResourcePoolChecker not init", K(ret), K(is_inited_));
  } else if (is_stop_) {
    ret = OB_CANCELED;
  } else {
  }  // do nothing
  return ret;
}

int ObShrinkResourcePoolChecker::init(share::schema::ObMultiVersionSchemaService* schema_service,
    rootserver::ObLeaderCoordinator* leader_coordinator, obrpc::ObSrvRpcProxy* srv_rpc_proxy, common::ObAddr& addr,
    rootserver::ObUnitManager* unit_mgr, share::ObPartitionTableOperator* pt_operator, TenantBalanceStat* tenant_stat,
    ObRebalanceTaskMgr* rebalance_task_mgr, ObServerManager* server_mgr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObShrinkResourcePoolChecker not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(NULL == leader_coordinator) || OB_UNLIKELY(NULL == schema_service) ||
             OB_UNLIKELY(NULL == srv_rpc_proxy) || OB_UNLIKELY(NULL == unit_mgr) || OB_UNLIKELY(!addr.is_valid()) ||
             OB_UNLIKELY(NULL == pt_operator) || OB_UNLIKELY(NULL == tenant_stat) ||
             OB_UNLIKELY(NULL == rebalance_task_mgr) || OB_UNLIKELY(NULL == server_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument",
        K(ret),
        KP(leader_coordinator),
        KP(schema_service),
        KP(srv_rpc_proxy),
        KP(unit_mgr),
        K(addr),
        KP(pt_operator),
        KP(tenant_stat),
        KP(rebalance_task_mgr),
        KP(server_mgr));
  } else {
    leader_coordinator_ = leader_coordinator;
    schema_service_ = schema_service;
    srv_rpc_proxy_ = srv_rpc_proxy;
    unit_mgr_ = unit_mgr;
    self_ = addr;
    pt_operator_ = pt_operator;
    tenant_stat_ = tenant_stat;
    rebalance_task_mgr_ = rebalance_task_mgr;
    server_mgr_ = server_mgr;
    is_inited_ = true;
  }
  return ret;
}

int ObShrinkResourcePoolChecker::try_shrink_tenant_resource_pools(int64_t& task_cnt)
{
  int ret = OB_SUCCESS;
  ObArray<share::ObResourcePool> pools;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObShrinkResourcePoolChecker not init", K(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("balancer stop", K(ret));
  } else if (OB_INVALID_ID == tenant_stat_->tenant_id_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected tenant id", K(ret), "tenant_id", tenant_stat_->tenant_id_);
  } else if (OB_FAIL(unit_mgr_->get_pools_of_tenant(tenant_stat_->tenant_id_, pools))) {
    LOG_WARN("fail to get resource pools", K(ret), "tenant_id", tenant_stat_->tenant_id_);
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < pools.count() && task_cnt <= ObRebalanceTaskMgr::ONCE_ADD_TASK_CNT; ++i) {
      const share::ObResourcePool& pool = pools.at(i);
      bool in_shrinking = true;
      const uint64_t pool_id = pool.resource_pool_id_;
      if (OB_INVALID_ID == pool_id) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid pool id", K(ret), K(pool_id));
      } else if (OB_FAIL(unit_mgr_->check_pool_in_shrinking(pool_id, in_shrinking))) {
        LOG_WARN("fail to check pool in shrinking", K(ret));
      } else if (!in_shrinking) {
        // not in shrinking, ignore and check next
      } else if (OB_FAIL(shrink_single_resource_pool(pool, task_cnt))) {
        LOG_WARN("fail to shrink single resource pool", K(ret), K(pool_id));
      } else {
      }  // no more to do
    }
  }
  return ret;
}

// shrink resource pool operation depends on the unit assign
int ObShrinkResourcePoolChecker::shrink_single_resource_pool(const share::ObResourcePool& pool, int64_t& task_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObShrinkResourcePoolChecker not init", K(ret));
  } else if (OB_UNLIKELY(!pool.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(pool));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("balancer stop", K(ret));
  } else if (OB_UNLIKELY(NULL == tenant_stat_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant_stat_ ptr is null", K(ret), KP(tenant_stat_));
  } else if (pool.tenant_id_ != tenant_stat_->tenant_id_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN(
        "tenant id not match", K(ret), "pool_tenant_id", pool.tenant_id_, "tenant_stat_id", tenant_stat_->tenant_id_);
  } else {
    const ObIArray<common::ObZone>& zone_list = pool.zone_list_;
    TenantBalanceStat& ts = *tenant_stat_;
    FOREACH_X(p, ts.sorted_partition_, OB_SUCC(ret))
    {
      if (OB_FAIL(check_stop())) {
        LOG_WARN("balancer stop", K(ret));
      } else if (task_cnt > ObRebalanceTaskMgr::ONCE_ADD_TASK_CNT) {
        break;
      } else if (!(*p)->can_balance()) {
        LOG_WARN("partition can't balance now, shrink until leader elected", "partition", *(*p));
      } else if (OB_FAIL(regulate_single_partition(*(*p), zone_list, task_cnt))) {
        LOG_WARN("fail to regulate single partition", K(ret));
      } else {
      }  // no more to do
    }
  }
  return ret;
}

int ObShrinkResourcePoolChecker::regulate_single_partition(
    const Partition& partition, const common::ObIArray<common::ObZone>& zone_list, int64_t& task_cnt)
{
  int ret = OB_SUCCESS;
  ObArray<ZoneReplicas> all_zone_replicas;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObShrinkResourcePoolChecker not init", K(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("balancer stop", K(ret));
  } else if (OB_UNLIKELY(NULL == tenant_stat_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant_stat_ ptr is null", K(ret), KP(tenant_stat_));
  } else {
    FOR_BEGIN_END_E(r, partition, tenant_stat_->all_replica_, OB_SUCCESS == ret)
    {
      if (!has_exist_in_array(zone_list, r->zone_)) {
        // not exist in zone list
      } else if (share::REPLICA_STATUS_NORMAL != r->replica_status_) {
        // do not process the replica not in normal status
      } else {
        int64_t index = 0;
        for (; index < all_zone_replicas.count(); ++index) {
          if (all_zone_replicas.at(index).zone_ == r->zone_) {
            break;
          } else {
          }  // go on to check next;
        }
        if (index >= all_zone_replicas.count()) {
          ZoneReplicas zone_replicas;
          zone_replicas.zone_ = r->zone_;
          if (OB_FAIL(all_zone_replicas.push_back(zone_replicas))) {
            LOG_WARN("fail to push back", K(ret));
          } else {
          }  // no more to do
        }
        if (OB_FAIL(ret)) {
        } else if (index >= all_zone_replicas.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("index greater than all zone replicas count",
              K(ret),
              K(index),
              "all_zone_replicas_count",
              all_zone_replicas.count());
        } else if (OB_UNLIKELY(NULL == r->unit_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("replica unit ptr is null", K(ret));
        } else if (r->zone_ != all_zone_replicas.at(index).zone_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("zone not match", K(ret), "left_zone", r->zone_, "right_zone", all_zone_replicas.at(index).zone_);
        } else if (r->unit_->in_pool_ && r->is_in_service() &&
                   ObReplicaTypeCheck::is_paxos_replica_V2(r->replica_type_)) {
          if (OB_FAIL(all_zone_replicas.at(index).in_pool_paxos_replicas_.push_back(r))) {
            LOG_WARN("fail to push back", K(ret));
          }
        } else if (r->unit_->in_pool_ && r->is_in_service() &&
                   !ObReplicaTypeCheck::is_paxos_replica_V2(r->replica_type_)) {
          if (OB_FAIL(all_zone_replicas.at(index).in_pool_non_paxos_replicas_.push_back(r))) {
            LOG_WARN("fail to push back", K(ret));
          }
        } else if (!r->unit_->in_pool_ && r->is_in_service() &&
                   ObReplicaTypeCheck::is_paxos_replica_V2(r->replica_type_)) {
          if (OB_FAIL(all_zone_replicas.at(index).not_in_pool_paxos_replicas_.push_back(r))) {
            LOG_WARN("fail to push back", K(ret));
          }
        } else {
          if (OB_FAIL(all_zone_replicas.at(index).not_in_pool_non_paxos_replicas_.push_back(r))) {
            LOG_WARN("fail to push back", K(ret));
          }
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < all_zone_replicas.count(); ++i) {
      const ZoneReplicas& zone_replicas = all_zone_replicas.at(i);
      if (OB_FAIL(regulate_single_partition_by_zone(partition, zone_replicas, task_cnt))) {
        LOG_WARN("fail to regulate single partition by zone", K(ret), K(zone_replicas));
      } else {
      }  // no more to do
    }
  }
  return ret;
}

int ObShrinkResourcePoolChecker::transfer_position_for_not_in_pool_paxos_replicas(const Partition& partition,
    const ObIArray<const Replica*>& not_in_pool_paxos_replicas,
    const ObIArray<const Replica*>& in_pool_non_paxos_replicas, int64_t& task_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObShrinkResourcePoolChecker not init", K(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("balancer stop", K(ret));
  } else if (OB_UNLIKELY(NULL == tenant_stat_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant_stat_ ptr is null", K(ret), KP(tenant_stat_));
  } else if (OB_UNLIKELY(NULL == rebalance_task_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rebalance_task_mgr_ ptr is null", K(ret), KP(rebalance_task_mgr_));
  } else if (not_in_pool_paxos_replicas.count() <= 0) {
    // no paxos replia without resource pool exists, by pass
  } else if (in_pool_non_paxos_replicas.count() <= 0) {
    // no extra unit for paxos replica even deleting all non-paxos replicas
    LOG_INFO("volumn not enough for not in pool paxos", K(partition));
  } else {
    // has a paxos replica not in pool while we have a unit for this replica
    const Replica* r = in_pool_non_paxos_replicas.at(0);
    if (OB_UNLIKELY(NULL == r)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("replica ptr is null", K(ret), KP(r));
    } else if (OB_ISNULL(r->server_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("replica server is null", K(ret), "server", r->server_);
    } else if (!is_non_paxos_replica(r->replica_type_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("replica type not match", K(ret), "replica_type", r->replica_type_);
    } else if (r->is_in_blacklist(ObRebalanceTaskType::REMOVE_NON_PAXOS_REPLICA, r->server_->server_, tenant_stat_)) {
      ret = OB_SUCCESS;
      LOG_WARN("replica do remove no paxos task in black list", "replica", *r);
    } else {
      ObRemoveNonPaxosTask task;
      ObRemoveNonPaxosTaskInfo task_info;
      common::ObArray<ObRemoveNonPaxosTaskInfo> task_info_array;
      OnlineReplica remove_member;
      remove_member.member_ = ObReplicaMember(r->server_->server_, r->member_time_us_, r->replica_type_);
      remove_member.zone_ = r->zone_;
      const char* comment = balancer::TRANSFER_POSITION_FOR_NOT_IN_POOL_PAXOS_REPLICA;
      ObPartitionKey pkey;
      if (OB_FAIL(tenant_stat_->gen_partition_key(partition, pkey))) {
        LOG_WARN("fail ot gen partition key", K(ret), K(partition), "replica", *r);
      } else if (OB_FAIL(task_info.build(remove_member, pkey))) {
        LOG_WARN("fail to build remove non paxos replica task info", K(ret));
      } else if (OB_FAIL(task_info_array.push_back(task_info))) {
        LOG_WARN("fail to push back", K(ret));
      } else if (OB_FAIL(task.build(task_info_array, remove_member.member_.get_server(), comment))) {
        LOG_WARN("fail to build remove member task", K(ret));
      } else if (OB_FAIL(rebalance_task_mgr_->add_task(task, task_cnt))) {
        LOG_WARN("fail to add task", K(ret));
      } else {
        LOG_INFO("add task to remove member", K(task), K(task_cnt), K(partition));
      }
    }
  }
  return ret;
}

int ObShrinkResourcePoolChecker::remove_not_in_pool_non_paxos_replicas(
    const Partition& partition, const ObIArray<const Replica*>& not_in_pool_non_paxos_replicas, int64_t& task_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObShrinkResourcePoolChecker not init", K(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("balancer stop", K(ret));
  } else if (OB_UNLIKELY(NULL == tenant_stat_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant_stat_ ptr is null", K(ret), KP(tenant_stat_));
  } else if (OB_UNLIKELY(NULL == rebalance_task_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rebalance_task_mgr_ ptr is null", K(ret), KP(rebalance_task_mgr_));
  } else if (OB_UNLIKELY(NULL == server_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("server_mgr_ ptr is null", K(ret), KP(server_mgr_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < not_in_pool_non_paxos_replicas.count(); ++i) {
      share::ObServerStatus server_status;
      const Replica* r = not_in_pool_non_paxos_replicas.at(i);
      if (OB_UNLIKELY(NULL == r)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("replica ptr is null", K(ret), KP(r));
      } else if (!is_non_paxos_replica(r->replica_type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("replica type not match", K(ret), "replica_type", r->replica_type_);
      } else if (OB_UNLIKELY(NULL == r->server_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("server ptr is null", K(ret));
      } else if (OB_FAIL(server_mgr_->get_server_status(r->server_->server_, server_status))) {
        LOG_WARN("fail to get server status", K(ret), "server", r->server_->server_);
      } else if (r->is_in_blacklist(ObRebalanceTaskType::REMOVE_NON_PAXOS_REPLICA, r->server_->server_, tenant_stat_)) {
        ret = OB_SUCCESS;
        LOG_WARN("replica do remove non paxos task in black list", "replica", *r);
      } else if (server_status.is_alive()) {
        ObRemoveNonPaxosTask task;
        ObRemoveNonPaxosTaskInfo task_info;
        common::ObArray<ObRemoveNonPaxosTaskInfo> task_info_array;
        OnlineReplica remove_member;
        remove_member.member_ = ObReplicaMember(r->server_->server_, r->member_time_us_, r->replica_type_);
        remove_member.zone_ = r->zone_;
        const char* comment = balancer::REMOVE_NOT_IN_POOL_NON_PAXOS_REPLICA;
        ObPartitionKey pkey;
        if (OB_FAIL(tenant_stat_->gen_partition_key(partition, pkey))) {
          LOG_WARN("fail to gen partition key", K(ret), K(partition), "replica", *r);
        } else if (OB_FAIL(task_info.build(remove_member, pkey))) {
          LOG_WARN("fail to build remove non paxos replica task info", K(ret));
        } else if (OB_FAIL(task_info_array.push_back(task_info))) {
          LOG_WARN("fail to push back", K(ret));
        } else if (OB_FAIL(task.build(task_info_array, remove_member.member_.get_server(), comment))) {
          LOG_WARN("fail to build remove member task", K(ret));
        } else if (OB_FAIL(rebalance_task_mgr_->add_task(task, task_cnt))) {
          LOG_WARN("fail to add task", K(ret));
        } else {
          LOG_INFO("add task to remove member", K(task), K(task_cnt), K(partition));
        }
      } else if (server_status.is_permanent_offline() ||
                 (server_status.is_temporary_offline() &&
                     server_status.last_hb_time_ + NOT_IN_POOL_NON_PAXOS_SAFE_REMOVE_INTERVAL <
                         ObTimeUtility::current_time())) {
        // remove the record from partition table directly
        if (OB_UNLIKELY(NULL == pt_operator_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("pt_operator_ is null", K(ret), KP(pt_operator_));
        } else if (OB_FAIL(pt_operator_->remove(partition.table_id_, partition.partition_id_, r->server_->server_))) {
          LOG_WARN("fail to remove", K(ret));
        } else {
        }  // no more to do
      } else {
      }  // temporary offline and not reach safe removing interval, do nothing
    }
  }
  return ret;
}

int ObShrinkResourcePoolChecker::regulate_single_partition_by_zone(
    const Partition& partition, const ZoneReplicas& zone_replicas, int64_t& task_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObShrinkResourcePoolChecker not init", K(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("balancer stop", K(ret));
  } else if (OB_UNLIKELY(!zone_replicas.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(zone_replicas));
  } else {
    const ObIArray<const Replica*>& in_pool_paxos_replicas = zone_replicas.in_pool_paxos_replicas_;
    const ObIArray<const Replica*>& not_in_pool_paxos_replicas = zone_replicas.not_in_pool_paxos_replicas_;
    const ObIArray<const Replica*>& in_pool_non_paxos_replicas = zone_replicas.in_pool_non_paxos_replicas_;
    const ObIArray<const Replica*>& not_in_pool_non_paxos_replicas = zone_replicas.not_in_pool_non_paxos_replicas_;
    if (OB_FAIL(remove_not_in_pool_non_paxos_replicas(partition, not_in_pool_non_paxos_replicas, task_cnt))) {
      LOG_WARN("fail to remove non in pool non_paxos_replicas", K(ret));
    } else if (OB_FAIL(transfer_position_for_not_in_pool_paxos_replicas(
                   partition, not_in_pool_paxos_replicas, in_pool_non_paxos_replicas, task_cnt))) {
      LOG_WARN("fail to transfer position for not in pool paxos replicas", K(ret));
    } else {
    }  // no more to do
  }
  return ret;
}

int ObShrinkResourcePoolChecker::check_shrink_resource_pool_finished()
{
  int ret = OB_SUCCESS;
  LOG_INFO("start check shrink resource pool");
  ObArray<uint64_t> tenant_ids;
  share::schema::ObSchemaGetterGuard schema_guard;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObShrinkResourcePoolChecker not init", K(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("balancer stop", K(ret));
  } else if (OB_FAIL(leader_coordinator_->get_tenant_ids(tenant_ids))) {
    LOG_WARN("fail to fetch all tenant ids", K(ret));
  } else if (OB_FAIL(schema_service_->get_schema_guard(schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret));
  } else {
    int bak_ret = OB_SUCCESS;
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.count(); ++i) {
      const uint64_t tenant_id = tenant_ids.at(i);
      LOG_INFO("start check shrink resource pool", K(tenant_id));
      if (OB_INVALID_ID == tenant_id) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid tenant_id", K(ret), K(tenant_id));
      } else if (OB_FAIL(check_shrink_resource_pool_finished_by_tenant(schema_guard, tenant_id))) {
        LOG_WARN("fail to check shrink resource pool finish", K(ret));
      } else {
      }  // no more to do
      bak_ret = (OB_SUCCESS == ret ? bak_ret : ret);
      ret = OB_SUCCESS;  // rewrite ret to OB_SUCCESS
    }
    ret = bak_ret;
  }
  LOG_INFO("finish check shrink resource pool", K(ret));
  return ret;
}

int ObShrinkResourcePoolChecker::commit_shrink_resource_pool(const uint64_t pool_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObShrinkResourcePoolChecker not init", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == pool_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(pool_id));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("balancer stop", K(ret));
  } else if (OB_FAIL(unit_mgr_->commit_shrink_resource_pool(pool_id))) {
    LOG_WARN("fail to shrink resource pool", K(ret), K(pool_id));
  } else {
  }  // no more to do
  return ret;
}

int ObShrinkResourcePoolChecker::check_shrink_resource_pool_finished_by_tenant(
    share::schema::ObSchemaGetterGuard& schema_guard, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> pool_ids;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObShrinkResourcePoolChecker not init", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("balancer stop", K(ret));
  } else if (OB_FAIL(unit_mgr_->get_pool_ids_of_tenant(tenant_id, pool_ids))) {
    LOG_WARN("fail to get resource pools", K(ret), K(tenant_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < pool_ids.count(); ++i) {
      const uint64_t pool_id = pool_ids.at(i);
      bool in_shrinking = true;
      bool is_finished = true;
      if (OB_INVALID_ID == pool_id) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid pool id", K(ret), K(pool_id));
      } else if (OB_FAIL(unit_mgr_->check_pool_in_shrinking(pool_id, in_shrinking))) {
        LOG_WARN("fail to check pool in shrinking", K(ret));
      } else if (!in_shrinking) {
        // not in shrinking, ignore
      } else if (OB_FAIL(check_single_pool_shrinking_finished(schema_guard, tenant_id, pool_id, is_finished))) {
        LOG_WARN("fail to check pool shrinking finished", K(ret));
      } else if (OB_FAIL(check_stop())) {
        LOG_WARN("balancer stop", K(ret));
      } else if (!is_finished) {
        // not finished, ignore
      } else if (OB_FAIL(commit_shrink_resource_pool(pool_id))) {
        LOG_WARN("fail to do commit shrink resource pool", K(ret));
      } else {
      }  // no more to do
    }
  }
  return ret;
}

int ObShrinkResourcePoolChecker::extract_units_servers_and_ids(
    const ObIArray<share::ObUnit>& units, ObIArray<common::ObAddr>& servers, ObIArray<uint64_t>& unit_ids)
{
  int ret = OB_SUCCESS;
  servers.reset();
  unit_ids.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < units.count(); ++i) {
    if (OB_FAIL(servers.push_back(units.at(i).server_))) {
      LOG_WARN("fail to push back", K(ret));
    } else if (OB_FAIL(unit_ids.push_back(units.at(i).unit_id_))) {
      LOG_WARN("fail to push back", K(ret));
    } else {
    }  // no more to do
  }
  return ret;
}

int ObShrinkResourcePoolChecker::check_single_pool_shrinking_finished(share::schema::ObSchemaGetterGuard& schema_guard,
    const uint64_t tenant_id, const uint64_t pool_id, bool& is_finished)
{
  int ret = OB_SUCCESS;
  ObArray<share::ObUnit> units;
  ObArray<common::ObAddr> servers;
  ObArray<uint64_t> unit_ids;
  common::ObArray<uint64_t> table_ids;
  common::ObArray<uint64_t> tablegroup_ids;
  LOG_INFO("start check resource pool shrinking finish", K(tenant_id), K(pool_id));
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObShrinkResourcePoolChecker not init", K(ret), K(is_inited_));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("balancer stop", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == pool_id || OB_INVALID_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(pool_id), K(tenant_id));
  } else if (OB_FAIL(unit_mgr_->get_deleting_units_of_pool(pool_id, units))) {
    LOG_WARN("fail to get deleting units of pool", K(ret), K(pool_id));
  } else if (OB_FAIL(extract_units_servers_and_ids(units, servers, unit_ids))) {
    LOG_WARN("fail to extract unit servers", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_ids_in_tenant(tenant_id, table_ids))) {
    LOG_WARN("fail to get table ids in tenant", K(ret));
  } else if (OB_FAIL(schema_guard.get_tablegroup_ids_in_tenant(tenant_id, tablegroup_ids))) {
    LOG_WARN("fail to get tablegroup ids in tenant", K(ret));
  } else {
    is_finished = true;
    for (int64_t i = 0; is_finished && OB_SUCC(ret) && i < table_ids.count(); ++i) {
      const share::schema::ObSimpleTableSchemaV2* table_schema = NULL;
      if (OB_FAIL(check_stop())) {
        LOG_WARN("root balancer stop", K(ret));
      } else if (OB_FAIL(schema_guard.get_table_schema(table_ids.at(i), table_schema))) {
        LOG_WARN("fail to get table schema", K(ret), "table_id", table_ids.at(i));
      } else if (OB_UNLIKELY(NULL == table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table not exist", K(ret), "table_id", table_ids.at(i));
      } else if (table_schema->has_self_partition()) {
        if (OB_FAIL(check_single_partition_entity_finished(
                schema_guard, table_ids.at(i), servers, unit_ids, is_finished))) {
          LOG_WARN("fail to check single table", K(ret), "table_id", table_ids.at(i));
        }
      } else {
      }  // has no partition, ignore
    }
    for (int64_t i = 0; is_finished && OB_SUCC(ret) && i < tablegroup_ids.count(); ++i) {
      const share::schema::ObSimpleTablegroupSchema* tg_schema = nullptr;
      if (OB_FAIL(check_stop())) {
        LOG_WARN("root balancer stop", K(ret));
      } else if (OB_FAIL(schema_guard.get_tablegroup_schema(tablegroup_ids.at(i), tg_schema))) {
        LOG_WARN("fail to get tablegroup schema", K(ret), "tg_id", tablegroup_ids.at(i));
      } else if (OB_UNLIKELY(nullptr == tg_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablegroup not exist", K(ret), "tg_id", tablegroup_ids.at(i));
      } else if (tg_schema->has_self_partition()) {
        if (OB_FAIL(check_single_partition_entity_finished(
                schema_guard, tablegroup_ids.at(i), servers, unit_ids, is_finished))) {
          LOG_WARN("fail to check single tablegroup", K(ret), "tg_id", tablegroup_ids.at(i));
        }
      } else {
      }  // partition group has no
    }
  }
  return ret;
}

int ObShrinkResourcePoolChecker::check_single_partition_entity_finished(
    share::schema::ObSchemaGetterGuard& schema_guard, const uint64_t schema_id, const ObIArray<common::ObAddr>& servers,
    const ObIArray<uint64_t>& unit_ids, bool& is_finished)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObShrinkResourcePoolChecker not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(OB_INVALID_ID == schema_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(schema_id));
  } else {
    ObArenaAllocator allocator;
    ObTablePartitionIterator iter;
    const bool need_fetch_faillist = true;
    iter.set_need_fetch_faillist(need_fetch_faillist);
    ObReplicaFilterHolder partition_filter;
    if (OB_FAIL(partition_filter.set_replica_status(REPLICA_STATUS_NORMAL))) {
      LOG_WARN("fail to set in member list filter", K(ret));
    } else if (OB_FAIL(iter.init(schema_id, schema_guard, *pt_operator_))) {
      LOG_WARN("fail to init table partition iterator", K(ret));
    } else {
      is_finished = true;
      ObPartitionInfo info;
      info.set_allocator(&allocator);
      while (is_finished && OB_SUCC(ret) && OB_SUCC(iter.next(info))) {
        bool has_replica = false;
        if (OB_FAIL(info.filter(partition_filter))) {
          LOG_WARN("fail to filter partition", K(ret));
        } else if (OB_FAIL(check_partition_replicas_exist_on_servers_and_units(info, servers, unit_ids, has_replica))) {
          LOG_WARN("fail to check", K(ret));
        } else if (has_replica) {
          // shrink pool not finished since valid partition still on these servers.
          is_finished = false;
        } else {
        }  // good, and go on to check next partition
        info.reuse();
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ObShrinkResourcePoolChecker::check_partition_replicas_exist_on_servers_and_units(
    const share::ObPartitionInfo& partition_info, const ObIArray<common::ObAddr>& servers,
    const ObIArray<uint64_t>& unit_ids, bool& has_replica)
{
  int ret = OB_SUCCESS;
  has_replica = false;
  for (int64_t i = 0; !has_replica && OB_SUCC(ret) && i < partition_info.replica_count(); ++i) {
    const ObPartitionReplica& replica = partition_info.get_replicas_v2().at(i);
    const ObAddr& server = replica.server_;
    const uint64_t unit_id = replica.unit_id_;
    if (!has_exist_in_array(servers, server) && !has_exist_in_array(unit_ids, unit_id)) {
      // good, this replica do not exist on servers
    } else {
      has_replica = true;
      LOG_INFO("replica still in unit which is in deleting status", K(unit_id), K(server), K(replica));
    }
  }
  return ret;
}

bool ObShrinkResourcePoolChecker::is_non_paxos_replica(const int32_t replica_type)
{
  return (REPLICA_TYPE_READONLY == replica_type);
}

bool ObShrinkResourcePoolChecker::is_paxos_replica_V2(const int32_t replica_type)
{
  return (REPLICA_TYPE_FULL == replica_type || REPLICA_TYPE_LOGONLY == replica_type);
}

}  // end namespace rootserver
}  // namespace oceanbase
