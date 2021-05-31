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
#include "ob_rereplication.h"

#include "lib/container/ob_array.h"
#include "lib/container/ob_array_iterator.h"
#include "lib/container/ob_se_array.h"
#include "lib/container/ob_se_array_iterator.h"
#include "share/partition_table/ob_partition_table_operator.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_table_schema.h"
#include "share/ob_multi_cluster_util.h"
#include "observer/ob_server_struct.h"
#include "ob_rebalance_task_mgr.h"
#include "ob_rebalance_task_executor.h"
#include "ob_zone_manager.h"
#include "ob_alloc_replica_strategy.h"
#include "ob_root_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::rootserver;
using namespace oceanbase::share;

ObRereplication::ObRereplication()
    : inited_(false),
      schema_service_(NULL),
      zone_mgr_(NULL),
      task_mgr_(NULL),
      pt_operator_(NULL),
      tenant_stat_(NULL),
      check_stop_provider_(NULL),
      unit_mgr_(NULL)
{}

int ObRereplication::init(share::schema::ObMultiVersionSchemaService& schema_service, ObZoneManager& zone_mgr,
    share::ObPartitionTableOperator& pt_operator, ObRebalanceTaskMgr& task_mgr, TenantBalanceStat& tenant_stat,
    share::ObCheckStopProvider& check_stop_provider, ObUnitManager& unit_mgr)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
  } else {
    pt_operator_ = &pt_operator;
    task_mgr_ = &task_mgr;
    zone_mgr_ = &zone_mgr;
    tenant_stat_ = &tenant_stat;
    check_stop_provider_ = &check_stop_provider;
    schema_service_ = &schema_service;
    unit_mgr_ = &unit_mgr;
    inited_ = true;
  }
  return ret;
}

int ObRereplication::check_inner_stat()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!tenant_stat_->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "tenant stat valid", false);
  }
  return ret;
}

/* operations of adding replicas should be scheduled when replicas are not enough,
 * the function replicate_enough_replica is implemented to achieve this goal.
 * replicate_enought_replica is one of the operations to align the replica distribution to
 * partition locality, it is only used to add replicas to the specific zone/region based on
 * the locality. others like replica type transform are implemented by other functions.
 */
int ObRereplication::replicate_enough_replica(
    const balancer::HashIndexCollection& hash_index_collection, int64_t& task_cnt)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  // the basic logic to add replicas
  // (1) first:  add paxos replicas based on the table group distribution
  // (2) second: add readonly replica randomly -- even though this is no an elegant way.
  //

  if (task_cnt <= 0) {
    if (OB_FAIL(replicate_enough_paxos_replica(hash_index_collection, task_cnt))) {
      LOG_WARN("fail to replicate paxos replica", K(ret));
      tmp_ret = ret;
    }
  }
  if (task_cnt <= 0) {
    if (OB_FAIL(replicate_enough_specific_logonly_replica(hash_index_collection, task_cnt))) {
      LOG_WARN("fail to replicate enough specific logonly replica", K(ret));
      tmp_ret = ret;
    }
  }
  if (task_cnt <= 0) {
    if (OB_FAIL(replicate_enough_readonly_replica(hash_index_collection, task_cnt))) {
      LOG_WARN("fail to replica enough readonly replica", K(ret));
      tmp_ret = ret;
    }
  }
  ret = tmp_ret;
  if (OB_FAIL(ret)) {
    LOG_WARN("fail to replicate enough replica", KR(ret));
  } else {
    LOG_INFO("replicate enough replica success", K(task_cnt));
  }
  return ret;
}

int ObRereplication::process_only_in_memberlist_replica(int64_t& task_cnt)
{
  int ret = OB_SUCCESS;
  TenantBalanceStat& ts = *tenant_stat_;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!ts.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "tenant stat valid", ts.is_valid());
  } else if (OB_UNLIKELY(nullptr == task_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task mgr ptr is null", K(ret));
  }
  FOREACH_X(p, ts.sorted_partition_, OB_SUCCESS == ret)
  {
    if (OB_FAIL(check_stop())) {
      LOG_WARN("balancer stop", K(ret));
      break;
    }
    if ((*p)->in_physical_restore()) {
      continue;
    }
    FOR_BEGIN_END(r, *(*p), ts.all_replica_)
    {
      const bool ignore_is_in_spliting = true;
      if (!r->need_rebuild_only_in_member_list()) {
        // nothing todo
      } else if (nullptr == r->server_) {
        LOG_WARN("server of this replica is null ignore", "replica", *r, "partition", *(*p));
      } else if (!ts.has_leader_while_member_change(*(*p), r->server_->server_, ignore_is_in_spliting)) {
        LOG_WARN("cannot remove member, may lost leader", "replica", *r, "partition", *(*p));
      } else if ((OB_ALL_CORE_TABLE_TID != extract_pure_id((*p)->table_id_)) ||
                 (OB_ALL_CORE_TABLE_TID == extract_pure_id((*p)->table_id_) &&
                     (r->server_->permanent_offline_ || r->server_->alive_ ||
                         (!r->server_->alive_ && !ts.has_replica_locality(*r))))) {
        ObAddr leader;
        bool valid_leader = false;
        ObRemoveMemberTaskInfo task_info;
        ObRemoveMemberTask task;
        common::ObArray<ObRemoveMemberTaskInfo> task_info_array;
        OnlineReplica remove_member;
        remove_member.member_ = ObReplicaMember(r->server_->server_, r->member_time_us_, r->replica_type_);
        remove_member.zone_ = r->zone_;
        const char* comment = balancer::REMOVE_ONLY_IN_MEMBER_LIST_REPLICA;
        ObPartitionKey pkey;
        int64_t quorum = 0;
        if (OB_FAIL(ts.get_remove_replica_quorum_size(*(*p), r->zone_, r->replica_type_, quorum))) {
          LOG_WARN("fail to get quorum size", K(ret), "partition", *(*p));
        } else if (OB_FAIL(ts.gen_partition_key(*(*p), pkey))) {
          LOG_WARN("fail to generate partition key", K(ret), "partition", *(*p));
        } else if (OB_FAIL(task_info.build(remove_member, pkey, quorum, (*p)->get_quorum()))) {
          LOG_WARN("fail to build remove member task info", K(ret));
        } else if (OB_FAIL(tenant_stat_->get_leader_addr(*(*p), leader, valid_leader))) {
          LOG_WARN("fail to get leader addr", K(ret), K(task_info));
        } else if (!valid_leader) {
          LOG_WARN("leader not finded, just skip", K(ret));
        } else if (!leader.is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("leader is invalid", K(ret), K(leader));
        } else if (OB_FAIL(task_info_array.push_back(task_info))) {
          LOG_WARN("fail to push back", K(ret));
        } else if (OB_FAIL(task.build(task_info_array, leader, comment))) {
          LOG_WARN("fail to build remove member task", K(ret));
        } else if (OB_FAIL(task_mgr_->add_task(task))) {
          LOG_WARN("fail to add task", K(ret));
        } else {
          task_cnt += 1;
        }
      }
    }
  }  // end ts.sorted_partition_ iterate
  return ret;
}

int ObRereplication::replicate_enough_specific_logonly_replica(
    const balancer::HashIndexCollection& hash_index_collection, int64_t& task_cnt)
{
  int ret = OB_SUCCESS;
  TenantBalanceStat& ts = *tenant_stat_;
  const uint64_t tenant_id = ts.tenant_id_;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!ts.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "tenant stat valid", ts.is_valid());
  }
  const Partition* prev = NULL;
  bool lost_primary_partition = false;
  FOREACH_X(p, ts.sorted_partition_, OB_SUCCESS == ret)
  {
    if (OB_FAIL(check_stop())) {
      LOG_WARN("balancer stop", K(ret));
      break;
    } else {
      // is_primary() guarantees partitions from the same partition group can be migrated in one task,
      // regardless the ONCE_ADD_TASK_CNT restriction
      if ((*p)->is_primary() && task_cnt > ObRebalanceTaskMgr::ONCE_ADD_TASK_CNT) {
        break;
      }
    }

    if (!(*p)->can_balance()) {
      LOG_WARN("partition can't replicate", "partition", *(*p));
      continue;
    } else if (!(*p)->is_valid_quorum()) {
      // the quorum value of a partition is introduced from 1.4.71,
      // it is filled by updating partition meta table, if the quorum value in partition meta table
      // is legal, then ignore it and go on to check the next partition
      LOG_WARN("quorum is invalid, maybe not report yet", K(ret), KPC(*p));
      continue;
    }

    if ((*p)->is_primary()) {
      lost_primary_partition = true;
    }
    // update partition group stat if tablegroup changed
    if (NULL == prev || !is_same_tg(*prev, *(*p))) {
      if (OB_FAIL(ts.update_tg_pg_stat(p - ts.sorted_partition_.begin()))) {
        LOG_WARN("update tenant group stat failed", K(ret));
        break;
      }
      prev = *p;
    }
    ObZoneLogonlyUnitProvider all_zone_unit_provider(ts.all_zone_unit_);
    ObArray<share::ObZoneReplicaAttrSet> zone_locality;
    ObArray<common::ObZone> zone_list;
    share::schema::ObSchemaGetterGuard guard;
    ObAddSpecificReplicaByLocality addr_allocator(*zone_mgr_, ts, all_zone_unit_provider, zone_locality, zone_list);
    if (OB_ISNULL(schema_service_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema_service is null", K(ret));
    } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id, guard))) {
      LOG_WARN("get schema guard failed", K(ret));
    } else if (OB_FAIL(get_locality_info(guard, *(*p), hash_index_collection, zone_locality, zone_list))) {
      LOG_WARN("fail get locality info", K(tenant_id), "table_id", (*p)->table_id_, K(ret));
    } else if (OB_FAIL(addr_allocator.init())) {
      LOG_WARN("fail init replica addr allocator", K(ret));
    } else if (OB_FAIL(addr_allocator.prepare_for_next_partition(*(*p), (*p)->is_primary()))) {
      LOG_WARN("fail prepare variables for next partition", K(ret));
    } else {
      ObReplicaAddr replica_addr;
      do {

        if (OB_FAIL(addr_allocator.get_next_replica(p, replica_addr))) {
          if (OB_LIKELY(OB_ITER_END == ret)) {
            ret = OB_SUCCESS;
            break;
          } else {
            LOG_WARN("fail get next replia for current partition", K(ret), "partition", *(*p));
          }
        } else {
          LOG_INFO("find dest unit for partition replica", "partition", *(*p), K(replica_addr));

          // replicate partition group partitions and update dest unit stat
          do {
            if (OB_FAIL(check_stop())) {
              LOG_WARN("balancer stop", K(ret));
              break;
            } else {
              ObAddReplicaTask task;
              ObAddTaskInfo task_info;
              common::ObArray<ObAddTaskInfo> task_info_array;
              ObPartitionKey key;
              OnlineReplica dst;
              ObReplicaMember src;
              ObReplicaMember empty_member;
              const char* comment = balancer::REPLICATE_ENOUGH_SPECIFIC_REPLICA;
              dst.member_ = ObReplicaMember(replica_addr.addr_,
                  ObTimeUtility::current_time(),
                  replica_addr.replica_type_,
                  replica_addr.get_memstore_percent());
              dst.unit_id_ = replica_addr.unit_id_;
              dst.zone_ = replica_addr.zone_;
              common::ObRegion dst_region = DEFAULT_REGION_NAME;
              if (OB_FAIL(zone_mgr_->get_region(dst.zone_, dst_region))) {
                dst_region = DEFAULT_REGION_NAME;
              }
              dst.member_.set_region(dst_region);
              int64_t transmit_data_size = 0;
              int64_t quorum = 0;
              bool exist = false;
              bool schema_changed = true;
              int64_t cluster_id = OB_INVALID_ID;
              bool is_restore = (*p)->in_physical_restore();

              if (OB_FAIL(ret)) {
                // failed
              } else if (OB_FAIL(ts.check_table_schema_changed(guard, (*p)->table_id_, schema_changed))) {
                LOG_WARN("fail to check if schema changed", K(ret));
              } else if (schema_changed) {
                ret = OB_EAGAIN;
                LOG_WARN("schema changed, generate task next round", K(ret));
              } else if (OB_FAIL(ts.check_valid_replica_exist_on_dest_server(*(*p), dst, exist))) {
                LOG_WARN("fail to check valid paxos replica exist", K(ret), K(dst), "partition", *(*p));
              } else if (exist) {
                // a valid replica exists in the target zone,
                // ignore it and go on to process the next partition
              } else if (OB_FAIL(ts.gen_partition_key(*(*p), key))) {
                LOG_WARN("generate partition key failed", K(ret), "partition", *(*p));
              } else if (OB_FAIL(ts.choose_data_source(*(*p),
                             dst.member_,
                             empty_member,
                             false,
                             src,
                             transmit_data_size,
                             ObRebalanceTaskType::ADD_REPLICA,
                             cluster_id))) {
                LOG_WARN("set task src failed", K(ret), K(task));
              } else if (OB_FAIL(ts.get_add_replica_quorum_size(
                             *(*p), replica_addr.zone_, replica_addr.replica_type_, quorum))) {
                LOG_WARN("fail to get quorum size", K(ret), "partition", *(*p));
              } else if (FALSE_IT(task_info.set_transmit_data_size(transmit_data_size))) {
                // nop
              } else if (FALSE_IT(task_info.set_cluster_id(cluster_id))) {
                // nothing todo
              } else if (OB_FAIL(task_info.build(dst, key, src, quorum, is_restore))) {
                LOG_WARN("fail to build add task info", K(ret));
              } else if (OB_FAIL(task_info_array.push_back(task_info))) {
                LOG_WARN("fail to push back", K(ret));
              } else if (OB_FAIL(task.build(task_info_array, dst.member_.get_server(), comment))) {
                LOG_WARN("fail to build add task", K(ret), K(key));
              } else if (OB_FAIL(task_mgr_->add_task(task, task_cnt))) {
                LOG_WARN("add task failed", K(ret), K(task));
              } else {
              }  // nothing more in this branch

              if (OB_SUCC(ret)) {
                // process others partitions in the same partition group
                __typeof__(p) next = p + 1;
                if (next >= ts.sorted_partition_.end() || (*next)->partition_id_ != (*p)->partition_id_ ||
                    (*next)->is_primary() || !(*next)->can_balance() || !(*next)->is_valid_quorum()) {
                  break;
                }
                p = next;
              }
            }
          } while (OB_SUCC(ret) && lost_primary_partition);
        }
      } while (0);  // one partition for one iteration only
      if (OB_MACHINE_RESOURCE_NOT_ENOUGH == ret) {
        LOG_WARN("fail to replicate partition, because of machine not enough, reset result_code",
            K(ret),
            "partition",
            *(*p));
        ret = OB_SUCCESS;
      }
    }
    DEBUG_SYNC(END_PRODUCE_PG_REPLICATE_TASK);
  }  // end ts.sorted_partition_ iterate
  return ret;
}

int ObRereplication::replicate_enough_paxos_replica(
    const balancer::HashIndexCollection& hash_index_collection, int64_t& task_cnt)
{
  LOG_INFO("start to replicate enough paxos replica", K(task_cnt), "tenant_id", tenant_stat_->tenant_id_);
  int ret = OB_SUCCESS;
  TenantBalanceStat& ts = *tenant_stat_;
  const uint64_t tenant_id = ts.tenant_id_;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!ts.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "tenant stat valid", ts.is_valid());
  }
  const Partition* prev = NULL;
  bool lost_primary_partition = false;
  FOREACH_X(p, ts.sorted_partition_, OB_SUCCESS == ret)
  {
    if (OB_FAIL(check_stop())) {
      LOG_WARN("balancer stop", K(ret));
      break;
    } else {
      // is_primary() guarantees partitions from the same partition group can be migrated in one task,
      // regardless the ONCE_ADD_TASK_CNT restriction
      if ((*p)->is_primary() && task_cnt > ObRebalanceTaskMgr::ONCE_ADD_TASK_CNT) {
        break;
      }
    }

    if (!(*p)->can_balance()) {
      LOG_WARN("partition can't balance", "partition", *(*p));
      continue;
    } else if (!(*p)->is_valid_quorum()) {
      // the quorum value of a partition is introduced from 1.4.71,
      // it is filled by updating partition meta table, if the quorum value in partition meta table
      // is legal, then ignore it and go on to check the next partition
      LOG_WARN("quorum is invalid, maybe not report yet", K(ret), KPC(*p));
      continue;
    }
    if (!(*p)->can_rereplicate()) {
      LOG_WARN("partition can't do replicate", "partition", *(*p));
      continue;
    }
    if ((*p)->is_primary()) {
      lost_primary_partition = true;
    }
    // update partition group stat if tablegroup changed
    if (NULL == prev || !is_same_tg(*prev, *(*p))) {
      if (OB_FAIL(ts.update_tg_pg_stat(p - ts.sorted_partition_.begin()))) {
        LOG_WARN("update tenant group stat failed", K(ret));
        break;
      }
      prev = *p;
    }
    ObZoneUnitsWithoutLogonlyProvider all_zone_unit_provider(ts.all_zone_unit_);
    ObZoneLogonlyUnitProvider logonly_zone_unit_provider(ts.all_zone_unit_);
    ObArray<share::ObZoneReplicaAttrSet> zone_locality;
    ObArray<common::ObZone> zone_list;
    schema::ObSchemaGetterGuard guard;
    ObAddPaxosReplicaByLocality addr_allocator(
        *zone_mgr_, ts, all_zone_unit_provider, logonly_zone_unit_provider, zone_locality, zone_list);
    if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id, guard))) {
      LOG_WARN("get schema guard failed", K(ret));
    } else if (OB_FAIL(get_locality_info(guard, *(*p), hash_index_collection, zone_locality, zone_list))) {
      LOG_WARN("fail get locality info", K(tenant_id), "table_id", (*p)->table_id_, K(ret));
    } else if (ObLocalityTaskHelp::filter_logonly_task(ts.tenant_id_, *unit_mgr_, guard, zone_locality)) {
      LOG_WARN("fail to filter logonly task", K(ret));
    } else if (OB_FAIL(addr_allocator.init())) {
      LOG_WARN("fail init replica addr allocator", K(ret));
    } else if (OB_FAIL(addr_allocator.prepare_for_next_partition(*(*p), (*p)->is_primary()))) {
      LOG_WARN("fail prepare variables for next partition", K(ret));
    } else {
      LOG_DEBUG("filter logonly task", K(ret), "table_id", (*p)->table_id_, K(zone_locality));
      ObReplicaAddr replica_addr;
      do {
        if (OB_FAIL(addr_allocator.get_next_replica((p), replica_addr))) {
          if (OB_LIKELY(OB_ITER_END == ret)) {
            ret = OB_SUCCESS;
            break;
          } else {
            LOG_WARN("fail get next replia for current partition", K(ret), "partition", *(*p));
          }
        } else {
          LOG_INFO("find dest unit for partition replica", "partition", *(*p), K(replica_addr));

          // replicate partition group partitions and update dest unit stat
          do {
            if (OB_FAIL(check_stop())) {
              LOG_WARN("balancer stop", K(ret));
              break;
            } else {
              ObAddReplicaTask task;
              ObAddTaskInfo task_info;
              common::ObArray<ObAddTaskInfo> task_info_array;
              ObPartitionKey key;
              OnlineReplica dst;
              ObReplicaMember src;
              ObReplicaMember empty_member;
              const char* comment = balancer::REPLICATE_ENOUGH_REPLICA;
              dst.member_ = ObReplicaMember(replica_addr.addr_,
                  ObTimeUtility::current_time(),
                  replica_addr.replica_type_,
                  replica_addr.get_memstore_percent());
              dst.unit_id_ = replica_addr.unit_id_;
              dst.zone_ = replica_addr.zone_;
              common::ObRegion dst_region = DEFAULT_REGION_NAME;
              if (OB_SUCCESS != zone_mgr_->get_region(dst.zone_, dst_region)) {
                dst_region = DEFAULT_REGION_NAME;
              }
              dst.member_.set_region(dst_region);
              int64_t transmit_data_size = 0;
              int64_t quorum = 0;
              bool exist = false;
              bool schema_changed = true;
              int64_t cluster_id = OB_INVALID_ID;
              bool is_restore = (*p)->in_physical_restore();
              ObClusterType cluster_type = ObClusterInfoGetter::get_cluster_type_v2();

              if (OB_FAIL(ret)) {
                // failed
              } else if (OB_FAIL(ts.check_table_schema_changed(guard, (*p)->table_id_, schema_changed))) {
                LOG_WARN("fail to check if schema changed", K(ret));
              } else if (schema_changed) {
                ret = OB_EAGAIN;
                LOG_WARN("schema changed, generate task next round", K(ret));
              } else if (OB_FAIL(ts.check_valid_replica_exist_on_dest_server(*(*p), dst, exist))) {
                LOG_WARN("fail to check valid paxos replica exist", K(ret), K(dst), "partition", *(*p));
              } else if (exist) {
                // a valid replica exists in the target zone,
                // ignore it and go on to process the next partition
              } else if (OB_FAIL(ts.gen_partition_key(*(*p), key))) {
                LOG_WARN("generate partition key failed", K(ret), "partition", *(*p));
              } else if (OB_FAIL(ts.choose_data_source(*(*p),
                             dst.member_,
                             empty_member,
                             false,
                             src,
                             transmit_data_size,
                             ObRebalanceTaskType::ADD_REPLICA,
                             cluster_id))) {
                LOG_WARN("set task src failed", K(ret), K(task));
              } else if (OB_FAIL(ts.get_add_replica_quorum_size(
                             *(*p), replica_addr.zone_, replica_addr.replica_type_, quorum))) {
                LOG_WARN("fail to get quorum size", K(ret), "partition", *(*p));
              } else if (FALSE_IT(task_info.set_transmit_data_size(transmit_data_size))) {
                // nop
              } else if (FALSE_IT(task_info.set_cluster_id(cluster_id))) {
                // nop
              } else if (OB_FAIL(task_info.build(dst, key, src, quorum, is_restore))) {
                LOG_WARN("fail to build add task info", K(ret));
              } else if (OB_FAIL(task_info_array.push_back(task_info))) {
                LOG_WARN("fail to push back", K(ret));
              } else if (OB_FAIL(task.build(task_info_array, dst.member_.get_server(), comment))) {
                LOG_WARN("fail to build add task", K(ret), K(key));
              } else if (OB_FAIL(task_mgr_->add_task(task, task_cnt))) {
                LOG_WARN("add task failed", K(ret), K(task));
              } else {
              }  // nothing more in this branch

              if (OB_SUCC(ret)) {
                // handles tablegroup
                __typeof__(p) next = p + 1;
                if (next >= ts.sorted_partition_.end() || (*next)->partition_id_ != (*p)->partition_id_ ||
                    (*next)->is_primary() || !(*next)->can_rereplicate() || !(*next)->is_valid_quorum()) {
                  break;
                }
                p = next;
              }
            }
          } while (OB_SUCC(ret) && lost_primary_partition);
        }
      } while (0);  // one partition for one iteration only
      if (OB_MACHINE_RESOURCE_NOT_ENOUGH == ret) {
        LOG_WARN("fail to replicate partition, because of machine not enough, reset result_code",
            K(ret),
            "partition",
            *(*p));
        ret = OB_SUCCESS;
      }
    }
    DEBUG_SYNC(END_PRODUCE_PG_REPLICATE_TASK);
  }  // end ts.sorted_partition_ iterate
  return ret;
}

int ObRereplication::replicate_enough_readonly_replica(
    const balancer::HashIndexCollection& hash_index_collection, int64_t& task_cnt)
{
  int ret = OB_SUCCESS;
  TenantBalanceStat& ts = *tenant_stat_;
  const uint64_t tenant_id = ts.tenant_id_;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!ts.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "tenant stat valid", ts.is_valid());
  }
  const Partition* prev = NULL;
  bool lost_primary_partition = false;
  FOREACH_X(p, ts.sorted_partition_, OB_SUCCESS == ret)
  {
    if (OB_FAIL(check_stop())) {
      LOG_WARN("balancer stop", K(ret));
      break;
    } else {
      if ((*p)->is_primary() && task_cnt > ObRebalanceTaskMgr::ONCE_ADD_TASK_CNT) {
        break;
      }
    }

    if (!(*p)->can_balance()) {
      LOG_WARN("partition can't replicate", "partition", *(*p));
      continue;
    } else if (!(*p)->is_valid_quorum()) {
      // the quorum value of a partition is introduced from 1.4.71,
      // it is filled by updating partition meta table, if the quorum value in partition meta table
      // is legal, then ignore it and go on to check the next partition
      LOG_WARN("quorum is invalid, maybe not report yet", K(ret), KPC(*p));
      continue;
    }

    if (!ts.can_rereplicate_readonly(*(*p))) {
      LOG_WARN("partition can not rereplcate readonly", KPC(*p));
      continue;
    }

    if ((*p)->is_primary()) {
      lost_primary_partition = true;
    }

    if (NULL == prev || !is_same_tg(*prev, *(*p))) {
      if (OB_FAIL(ts.update_tg_pg_stat(p - ts.sorted_partition_.begin()))) {
        LOG_WARN("update tenant group stat failed", K(ret));
        break;
      }
      prev = *p;
    }

    if (OB_SUCC(ret)) {
      ObZoneUnitsWithoutLogonlyProvider all_zone_unit_provider(ts.all_zone_unit_);
      ObArray<share::ObZoneReplicaAttrSet> zone_locality;
      ObArray<common::ObZone> zone_list;
      share::schema::ObSchemaGetterGuard guard;
      ObAddReadonlyReplicaByLocality addr_allocator(*zone_mgr_, ts, all_zone_unit_provider, zone_locality, zone_list);
      if (OB_ISNULL(schema_service_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schema_service is null", K(ret));
      } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id, guard))) {
        LOG_WARN("get schema guard failed", K(ret));
      } else if (OB_FAIL(get_locality_info(guard, *(*p), hash_index_collection, zone_locality, zone_list))) {
        LOG_WARN("fail get locality info", K(tenant_id), "table_id", (*p)->table_id_, K(ret));
      } else if (OB_FAIL(addr_allocator.init())) {
        LOG_WARN("fail init replica addr allocator", "partition", *(*p), K(ret));
      } else if (OB_FAIL(addr_allocator.prepare_for_next_partition(*(*p), (*p)->is_primary()))) {
        LOG_WARN("fail prepare variables for next partition", "partition", *(*p), K(ret));
      } else {
        ObReplicaAddr replica_addr;
        do {
          if (OB_FAIL(addr_allocator.get_next_replica(p, replica_addr))) {
            if (OB_LIKELY(OB_ITER_END == ret)) {
              ret = OB_SUCCESS;
              break;
            } else {
              LOG_WARN("fail get next replia for current partition", K(ret), "partition", *(*p));
            }
          } else {
            LOG_INFO("find dest unit for partition readonly replica", "partition", *(*p), K(replica_addr));
            do {
              if (OB_FAIL(check_stop())) {
                LOG_WARN("balancer stop", K(ret));
                break;
              } else {
                ObAddReplicaTask task;
                ObAddTaskInfo task_info;
                common::ObArray<ObAddTaskInfo> task_info_array;
                ObPartitionKey key;
                OnlineReplica dst;
                ObReplicaMember src;
                ObReplicaMember empty_member;
                const char* comment = balancer::REPLICATE_ENOUGH_REPLICA;
                dst.member_ = ObReplicaMember(replica_addr.addr_,
                    ObTimeUtility::current_time(),
                    replica_addr.replica_type_,
                    replica_addr.get_memstore_percent());
                dst.unit_id_ = replica_addr.unit_id_;
                dst.zone_ = replica_addr.zone_;
                common::ObRegion dst_region = DEFAULT_REGION_NAME;
                if (OB_SUCCESS != zone_mgr_->get_region(dst.zone_, dst_region)) {
                  dst_region = DEFAULT_REGION_NAME;
                }
                dst.member_.set_region(dst_region);
                int64_t quorum = 0;
                int64_t transmit_data_size = 0;
                bool exist = false;
                bool schema_changed = true;
                int64_t cluster_id = OB_INVALID_ID;
                bool is_restore = (*p)->in_physical_restore();
                if (OB_FAIL(ts.check_table_schema_changed(guard, (*p)->table_id_, schema_changed))) {
                  LOG_WARN("fail to check if schema changed", K(ret));
                } else if (schema_changed) {
                  ret = OB_EAGAIN;
                  LOG_WARN("schema changed, generate task next round", K(ret));
                } else if (OB_FAIL(ts.check_valid_replica_exist_on_dest_server(*(*p), dst, exist))) {
                  LOG_WARN("fail to check valid readonly exist on dest server", K(ret));
                } else if (exist) {
                  // a valid replica exists in the target zone,
                  // ignore it and go on to process the next partition
                } else if (OB_FAIL(ts.gen_partition_key(*(*p), key))) {
                  LOG_WARN("generate partition key failed", K(ret), "partition", *(*p));
                } else if (OB_FAIL(ts.choose_data_source(*(*p),
                               dst.member_,
                               empty_member,
                               false,
                               src,
                               transmit_data_size,
                               ObRebalanceTaskType::ADD_REPLICA,
                               cluster_id))) {
                  LOG_WARN("set task src failed", K(ret), K(task));
                } else if (OB_FAIL(ts.get_add_replica_quorum_size(
                               *(*p), replica_addr.zone_, replica_addr.replica_type_, quorum))) {
                  LOG_WARN("fail to get quorum size", K(ret), K(quorum), "partition", *(*p));
                } else if (FALSE_IT(task_info.set_transmit_data_size(transmit_data_size))) {
                  // nop
                } else if (FALSE_IT(task_info.set_cluster_id(cluster_id))) {
                  // nothing todo
                } else if (OB_FAIL(task_info.build(dst, key, src, quorum, is_restore))) {
                  LOG_WARN("fail to build add task info", K(ret));
                } else if (OB_FAIL(task_info_array.push_back(task_info))) {
                  LOG_WARN("fail to push back", K(ret));
                } else if (OB_FAIL(task.build(task_info_array, dst.member_.get_server(), comment))) {
                  LOG_WARN("fail to build add task", K(ret), K(key));
                } else if (OB_FAIL(task_mgr_->add_task(task, task_cnt))) {
                  LOG_WARN("add task failed", K(ret), K(task));
                }

                if (OB_SUCC(ret)) {
                  // process the partitions in the same partition group
                  __typeof__(p) next = p + 1;
                  if (next >= ts.sorted_partition_.end() || (*next)->partition_id_ != (*p)->partition_id_ ||
                      (*next)->is_primary() || !ts.can_rereplicate_readonly(*(*next)) || !(*next)->is_valid_quorum() ||
                      !(*next)->can_balance()) {
                    break;
                  }
                  p = next;
                }
              }
            } while (OB_SUCC(ret) && lost_primary_partition);
          }
        } while (0);  // on partition for one iteration only
        if (OB_MACHINE_RESOURCE_NOT_ENOUGH == ret) {
          LOG_WARN("fail to replicate partition, because of machine not enough, reset result_code",
              K(ret),
              "partition",
              *(*p));
          ret = OB_SUCCESS;
        }
      }
    }
  }  // end ts.sorted_partition_ iterate
  return ret;
}

int ObRereplication::get_readonly_all_server_compensation_mode(
    share::schema::ObSchemaGetterGuard& guard, const Partition& partition, bool& compensate_readonly_all_server) const
{
  int ret = OB_SUCCESS;
  const uint64_t schema_id = partition.table_id_;
  if (OB_UNLIKELY(OB_INVALID_ID == schema_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "partition", partition);
  } else if (is_new_tablegroup_id(schema_id)) {
    compensate_readonly_all_server = false;
  } else {
    const share::schema::ObSimpleTableSchemaV2* table_schema = nullptr;
    if (OB_FAIL(guard.get_table_schema(schema_id, table_schema))) {
      LOG_WARN("fail to get table schema", K(ret), K(schema_id));
    } else if (OB_UNLIKELY(nullptr == table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("table schema ptr is null", K(ret), K(schema_id));
    } else if (OB_FAIL(table_schema->check_is_duplicated(guard, compensate_readonly_all_server))) {
      LOG_WARN("fail to check duplicate scope cluter", K(ret), K(schema_id));
    }
  }
  return ret;
}

int ObRereplication::get_locality_info(share::schema::ObSchemaGetterGuard& guard, const Partition& partition,
    const balancer::HashIndexCollection& hash_index_collection, share::schema::ZoneLocalityIArray& zone_locality,
    common::ObIArray<common::ObZone>& zone_list) const
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    common::ObSEArray<share::ObZoneReplicaAttrSet, 7> actual_zone_locality;
    const uint64_t schema_id = partition.table_id_;
    bool locality_changed = true;
    const schema::ObPartitionSchema* partition_schema = nullptr;
    bool compensate_readonly_all_server = false;
    const schema::ObSchemaType schema_type = (is_new_tablegroup_id(schema_id) ? schema::ObSchemaType::TABLEGROUP_SCHEMA
                                                                              : schema::ObSchemaType::TABLE_SCHEMA);
    if (OB_UNLIKELY(nullptr == tenant_stat_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant stat is null", K(ret));
    } else if (OB_FAIL(tenant_stat_->check_table_locality_changed(guard, schema_id, locality_changed))) {
      LOG_WARN("fail to check if schema changed", K(ret));
    } else if (locality_changed) {
      ret = OB_EAGAIN;
      LOG_WARN("locality changed, generate task next round", K(ret));
    } else if (OB_FAIL(schema::ObPartMgrUtils::get_partition_schema(guard, schema_id, schema_type, partition_schema))) {
      LOG_WARN("fail to get partition schema", K(ret), K(schema_id));
    } else if (OB_UNLIKELY(nullptr == partition_schema)) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("partition schema not exist", K(schema_id), K(ret));
    } else if (OB_FAIL(partition_schema->get_zone_list(guard, zone_list))) {
      LOG_WARN("fail to get zone list", K(ret), "table_id", schema_id);
    } else if (OB_FAIL(partition_schema->get_zone_replica_attr_array_inherit(guard, actual_zone_locality))) {
      LOG_WARN("fail to get zone replica attr array", K(ret), K(schema_id));
    } else if (OB_FAIL(get_readonly_all_server_compensation_mode(guard, partition, compensate_readonly_all_server))) {
      LOG_WARN("fail to get readonly all server compensation mode", K(ret));
    } else if (OB_FAIL(ObLocalityUtil::generate_designated_zone_locality(compensate_readonly_all_server,
                   partition.tablegroup_id_,
                   partition.all_pg_idx_,
                   hash_index_collection,
                   *tenant_stat_,
                   actual_zone_locality,
                   zone_locality))) {
      LOG_WARN("fail to generate designated zone locality", K(ret));
    }
  }
  return ret;
}

int ObRereplication::remove_permanent_offline_replicas(int64_t& task_cnt)
{
  const int64_t start = ObTimeUtility::current_time();
  int ret = OB_SUCCESS;
  TenantBalanceStat& ts = *tenant_stat_;
  ObArray<ObRemoveMemberTask> remove_member_tasks;
  bool check_pg_leader = false;
  const Partition* prev = NULL;
  // remove the replica which is in split with minor freeze not finished
  const bool ignore_is_in_spliting = true;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!ts.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "tenant stat valid", ts.is_valid());
  }
  FOREACH_X(p, ts.sorted_partition_, OB_SUCCESS == ret)
  {
    if (OB_FAIL(check_stop())) {
      LOG_WARN("balancer stop", K(ret));
    } else if (OB_ISNULL(tenant_stat_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant_stat is null", K(ret));
    } else if (OB_ISNULL(task_mgr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("task_mgr is null", K(ret));
    } else if (OB_ISNULL(p) || OB_ISNULL(*p)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition is null", K(ret));
    } else if (!(*p)->is_valid_quorum()) {
      // the quorum value of a partition is introduced from 1.4.71,
      // it is filled by updating partition meta table, if the quorum value in partition meta table
      // is legal, then ignore it and go on to check the next partition
      LOG_WARN("quorum is invalid, maybe not report yet", K(ret), KPC(*p));
      continue;
    } else {
      // when try to remove member of the partitions that are in the same partition group,
      // it is essential to check leader again
      if (NULL == prev || !is_same_pg(*prev, *(*p))) {
        check_pg_leader = false;
      }
      prev = *p;

      FOR_BEGIN_END_E(r, *(*p), ts.all_replica_, OB_SUCCESS == ret)
      {
        if (r->is_in_service() && ObReplicaTypeCheck::is_paxos_replica_V2(r->replica_type_) &&
            !(*p)->in_physical_restore() && r->server_->permanent_offline_) {
          if (!ts.has_leader_while_member_change(*(*p), r->server_->server_, ignore_is_in_spliting)) {
            LOG_WARN("can't remove member, may lost leaser", "partition", *(*p), "replica", *r);
          } else if (r->is_in_blacklist(ObRebalanceTaskType::MEMBER_CHANGE, r->server_->server_, tenant_stat_)) {
            ret = OB_REBALANCE_TASK_CANT_EXEC;
            LOG_WARN("task in black list", K(*r));
          } else {
            ObAddr leader;
            ObRemoveMemberTaskInfo task_info;
            OnlineReplica remove_member;
            remove_member.member_ = ObReplicaMember(r->server_->server_, r->member_time_us_, r->replica_type_);
            remove_member.zone_ = r->zone_;
            const char* comment = balancer::REMOVE_PERMANENT_OFFLINE_REPLICA;
            ObPartitionKey pkey;
            int64_t quorum = 0;
            bool valid_leader = false;
            if (OB_FAIL(ts.get_remove_replica_quorum_size(*(*p), r->zone_, r->replica_type_, quorum))) {
              LOG_WARN("fail to get quorum size", K(ret), "partition", *(*p));
            } else if (OB_FAIL(ts.gen_partition_key(*(*p), pkey))) {
              LOG_WARN("fail to generate partition key", K(ret), "partition", *(*p));
            } else if (OB_FAIL(task_info.build(remove_member, pkey, quorum, (*p)->get_quorum()))) {
              LOG_WARN("fail to build remove member task info", K(ret));
            } else if (OB_FAIL(tenant_stat_->get_leader_addr(*(*p), leader, valid_leader))) {
              LOG_WARN("fail to get leader addr", K(ret), K(task_info));
            } else if (!valid_leader) {
              LOG_WARN("leader not finded, just skip", K(ret));
            } else if (!leader.is_valid()) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("leader is invalid", K(ret), K(leader));
            } else if (OB_FAIL(ObRebalanceTaskUtil::build_batch_remove_member_task(
                           *task_mgr_, leader, task_info, comment, remove_member_tasks, check_pg_leader))) {
              LOG_WARN("fail to build batch_remove_member_task", K(ret), KPC((*p)), K(task_info));
            } else {
              task_cnt += 1;
              // one task for eache partition
              break;
            }  // no more to do
          }
        } else {
        }  // go on next replica
      }
      // ignore single partition error
      if (OB_FAIL(ret)) {
        LOG_WARN("partition remove permanent offline member failed", K(ret), KPC(*p));
        ret = OB_SUCCESS;
      }
    }
  }
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; i < remove_member_tasks.count(); i++) {
      if (OB_FAIL(task_mgr_->add_task(remove_member_tasks.at(i)))) {
        LOG_WARN("fail to add task", K(ret));
      }
    }
  }
  LOG_INFO("do delete permanent offline member", K(ret), "cost", ObTimeUtility::current_time() - start);
  return ret;
}

/* replicate_to_unit is used to migrate replicas whose server is inactive
 * and who is still in the member list of its leader
 *
 * note:if a replica is destroyed before migration,
 *        no corresponding replica infos are filled in all_replica_ array,
 *        in which case, we cannot migrate all replicas by replicate_to_unit.
 */
int ObRereplication::replicate_to_unit(int64_t& task_cnt)
{
  int ret = OB_SUCCESS;
  TenantBalanceStat& ts = *tenant_stat_;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!ts.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "tenant stat valid", ts.is_valid());
  }
  FOREACH_X(p, ts.sorted_partition_, OB_SUCCESS == ret)
  {
    if (OB_FAIL(check_stop())) {
      LOG_WARN("balancer stop", K(ret));
      break;
    } else {
      if ((*p)->is_primary() && task_cnt > ObRebalanceTaskMgr::ONCE_ADD_TASK_CNT) {
        break;
      }
    }
    if (!(*p)->can_balance()) {
      LOG_WARN("partition can't replicate", "partition", *(*p));
      continue;
    } else if (!(*p)->is_valid_quorum()) {
      // the quorum value of a partition is introduced from 1.4.71,
      // it is filled by updating partition meta table, if the quorum value in partition meta table
      // is legal, then ignore it and go on to check the next partition
      LOG_WARN("quorum is invalid, maybe not report yet", K(ret), KPC(*p));
      continue;
    }
    FOR_BEGIN_END_E(r, *(*p), ts.all_replica_, OB_SUCCESS == ret)
    {
      if (r->replica_status_ == REPLICA_STATUS_NORMAL && r->unit_->in_pool_ && !r->server_->online_ &&
          r->server_->server_ != r->unit_->info_.unit_.server_ && r->unit_->server_->online_ &&
          !r->unit_->server_->blocked_ && ts.has_leader_while_member_change(*(*p), r->server_->server_) &&
          !r->is_in_blacklist(ObRebalanceTaskType::MIGRATE_REPLICA, r->unit_->info_.unit_.server_, tenant_stat_)) {
        LOG_INFO("try to replicate replica, because server and unit location is different"
                 " and server not online",
            "partition",
            *(*p),
            "replica_type",
            r->replica_type_,
            "replica_status",
            r->replica_status_,
            "from_server",
            r->server_->server_,
            "dest_server",
            r->unit_->info_.unit_.server_);
        ObMigrateReplicaTask task;
        ObMigrateTaskInfo task_info;
        common::ObArray<ObMigrateTaskInfo> task_info_array;
        ObPartitionKey key;
        OnlineReplica dst;
        ObReplicaMember data_src;
        ObReplicaMember src =
            ObReplicaMember(r->server_->server_, r->member_time_us_, r->replica_type_, r->get_memstore_percent());
        ;
        const char* comment = balancer::REPLICATE_PERMANENT_OFFLINE_REPLICA;
        dst.member_ = ObReplicaMember(
            r->unit_->info_.unit_.server_, ObTimeUtility::current_time(), r->replica_type_, r->get_memstore_percent());
        dst.unit_id_ = r->unit_->info_.unit_.unit_id_;
        dst.zone_ = r->zone_;
        dst.member_.set_region(r->region_);
        int64_t quorum = 0;
        int64_t transmit_data_size = 0;
        int64_t cluster_id = OB_INVALID_ID;
        bool is_restore = (*p)->in_physical_restore();
        const obrpc::MigrateMode migrate_mode = obrpc::MigrateMode::MT_LOCAL_FS_MODE;
        if (OB_FAIL(ts.gen_partition_key(*(*p), key))) {
          LOG_WARN("generate partition key failed", K(ret), "partition", *(*p));
        } else if (OB_FAIL(ts.choose_data_source(*(*p),
                       dst.member_,
                       src,
                       false,
                       data_src,
                       transmit_data_size,
                       ObRebalanceTaskType::MIGRATE_REPLICA,
                       cluster_id))) {
          LOG_WARN("set task src failed", K(ret), K(task));
        } else if (OB_FAIL(ts.get_migrate_replica_quorum_size(*(*p), quorum))) {
          LOG_WARN("fail to get quorum size", K(ret));
        } else if (FALSE_IT(task_info.set_transmit_data_size(transmit_data_size))) {
          // nop
        } else if (OB_FAIL(task_info.build(migrate_mode, dst, key, src, data_src, quorum, is_restore))) {
          LOG_WARN("fail to build migrate task", K(ret), K(key));
        } else if (OB_FAIL(task_info_array.push_back(task_info))) {
          LOG_WARN("fail to push back", K(ret));
        } else if (OB_FAIL(task.build(migrate_mode,
                       task_info_array,
                       dst.member_.get_server(),
                       ObRebalanceTaskPriority::LOW_PRI,
                       comment))) {
          LOG_WARN("fail to build add task", K(ret), K(key));
        } else if (OB_FAIL(task_mgr_->add_task(task, task_cnt))) {
          LOG_WARN("add task failed", K(ret), K(task));
        } else {
          break;
        }
      }
    }
  }
  return ret;
}
