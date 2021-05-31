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
#include "ob_partition_group_coordinator.h"

#include "lib/container/ob_array.h"
#include "lib/container/ob_array_iterator.h"
#include "lib/container/ob_se_array.h"
#include "lib/container/ob_se_array_iterator.h"
#include "observer/ob_server_struct.h"
#include "share/ob_multi_cluster_util.h"
#include "share/partition_table/ob_partition_table_operator.h"
#include "ob_rebalance_task_mgr.h"
#include "ob_root_utils.h"
#include "ob_locality_checker.h"

using namespace oceanbase::common;
using namespace oceanbase::rootserver;
using namespace oceanbase::share;

ObPartitionGroupCoordinator::ObPartitionGroupCoordinator()
    : inited_(false),
      task_mgr_(NULL),
      tenant_stat_(NULL),
      check_stop_provider_(NULL),
      two_paxos_zones_(),
      logonly_units_()
{}

int ObPartitionGroupCoordinator::init(
    ObRebalanceTaskMgr& task_mgr, TenantBalanceStat& tenant_stat, share::ObCheckStopProvider& check_stop_provider)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
  } else {
    task_mgr_ = &task_mgr;
    tenant_stat_ = &tenant_stat;
    check_stop_provider_ = &check_stop_provider;
    two_paxos_zones_.reset();
    logonly_units_.reset();
    inited_ = true;
  }
  return ret;
}

int ObPartitionGroupCoordinator::prepare()
{
  int ret = OB_SUCCESS;
  TenantBalanceStat& ts = *tenant_stat_;
  for (int64_t i = 0; i < ts.all_zone_paxos_info_.count() && OB_SUCC(ret); i++) {
    const ZonePaxosInfo& zone_paxos_info = ts.all_zone_paxos_info_.at(i);
    if (zone_paxos_info.full_replica_num_ == 1 && zone_paxos_info.logonly_replica_num_ == 1) {
      if (OB_FAIL(two_paxos_zones_.push_back(zone_paxos_info.zone_))) {
        LOG_WARN("fail to push back", K(ret), K(zone_paxos_info));
      }
    } else {
    }
  }

  for (int64_t i = 0; i < ts.all_zone_unit_.count() && OB_SUCC(ret); i++) {
    const ZoneUnit& zu = ts.all_zone_unit_.at(i);
    for (int64_t j = 0; j < zu.all_unit_.count() && OB_SUCC(ret); j++) {
      UnitStat* u = zu.all_unit_.at(j);
      if (REPLICA_TYPE_LOGONLY == u->info_.unit_.replica_type_) {
        if (OB_FAIL(logonly_units_.push_back(u))) {
          LOG_WARN("fail to push back", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObPartitionGroupCoordinator::normal_pg_member_coordinate(
    common::ObArray<Partition*>::iterator& p, Partition& primary_partition, int64_t& task_cnt)
{
  int ret = OB_SUCCESS;
  TenantBalanceStat& ts = *tenant_stat_;
  bool primary_partition_migrated = false;
  int64_t tmp_task_cnt = 0;
  if (!primary_partition.can_balance()) {
    LOG_INFO("primary partition has no leader", K(primary_partition));
  } else if (!(*p)->is_valid_quorum()) {
    LOG_WARN("quorum not report yet, just skip", K(ret), KPC(*p));
  } else if (OB_FAIL(try_coordinate_primary_by_unit(primary_partition, primary_partition_migrated, tmp_task_cnt))) {
    LOG_WARN("fail to coordiante logonly replica", K(ret), K(primary_partition));
  } else if (primary_partition_migrated) {
    task_cnt += tmp_task_cnt;
  } else if (!can_do_coordinate(primary_partition)) {
    LOG_WARN("can't do coordinate", K(ret), K(primary_partition));
  } else {
    do {
      int64_t tmp_task_cnt = 0;
      __typeof__(p) next = p + 1;
      if (next >= ts.sorted_partition_.end() || (*next)->partition_idx_ != (*p)->partition_idx_ ||
          (*next)->is_primary()) {
        break;
      }
      p = next;
      if (!(*p)->is_valid_quorum()) {
        LOG_WARN("quorum not report yet, just skip", K(ret), KPC(*p));
      } else if (OB_FAIL(
                     coordinate_partition_member(primary_partition, *(*p), primary_partition_migrated, tmp_task_cnt))) {
        LOG_WARN("coordinate partition member failed", K(ret), K(primary_partition), "partition", *(*p));
      } else {
        task_cnt += tmp_task_cnt;
        if (primary_partition_migrated) {
          break;
        }
      }
    } while (OB_SUCC(ret));
  }
  return ret;
}

// aligned step:
// 1. Using the first partition of pg as a template, called the primary partition,
//   first solve the alignment of the primary partition;
// 1.1 try_coordinate_primary_by_unit: Ensure that the replica of type L is stored on the logonly unit,
//    and if there is a logonly unit, the logonly replica must be on the logonlg unit;
// 1.2 In the case where the primary partition is already aligned, solve the alignment of the remaining partitions
// 2.1 migrate_primary_partition: For each replica in pg,
//                               check whether the destination OBS can migrate in occasionally when aligning,
//                               if not, try to migrate back
// 2.2 Solve the alignment of each type by type without moving back
// 2.2.1 There are two cases of paxos copy alignment.
//      One is that there are two replicas of paxos in the zone;
//      the other is that there is only one replica of paxos in the zone;
// 2.2.2 Solve alignment issues for other replica types
int ObPartitionGroupCoordinator::coordinate_pg_member(int64_t& task_cnt)
{
  int ret = OB_SUCCESS;
  TenantBalanceStat& ts = *tenant_stat_;
  task_cnt = 0;
  two_paxos_zones_.reset();
  logonly_units_.reset();

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!ts.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "tenant stat valid", ts.is_valid());
  } else if (OB_FAIL(prepare())) {
    LOG_WARN("fail to prepare", K(ret));
  } else {
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
      if ((*p)->is_primary()) {
        const uint64_t partition_entity_id = (*p)->table_id_;
        Partition& primary_partition = *(*p);
        if (OB_FAIL(normal_pg_member_coordinate(p, primary_partition, task_cnt))) {
          LOG_WARN("fail to do normal pg member coordinate", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObPartitionGroupCoordinator::try_coordinate_primary_by_unit(
    const Partition& primary_partition, bool& primary_partition_migrated, int64_t& task_count)
{
  int ret = OB_SUCCESS;
  task_count = 0;
  TenantBalanceStat& ts = *tenant_stat_;
  primary_partition_migrated = false;
  bool can_add_task = false;
  for (int64_t i = 0; i < logonly_units_.count() && OB_SUCC(ret) && !primary_partition_migrated; i++) {
    const Replica* replica_on_logonly = NULL;
    const Replica* logonly_replica = NULL;
    FOR_BEGIN_END_E(ppr, primary_partition, ts.all_replica_, OB_SUCC(ret))
    {
      if (!ppr->is_in_service()) {
        // nothing todo
      } else if (ppr->zone_ != logonly_units_.at(i)->info_.unit_.zone_) {
        // nothing todo
      } else {
        if (REPLICA_TYPE_LOGONLY == ppr->replica_type_) {
          logonly_replica = ppr;
        }
        if (REPLICA_TYPE_LOGONLY == ppr->unit_->info_.unit_.replica_type_) {
          replica_on_logonly = ppr;
        }
      }
    }  // end  FOR_BEGIN_END_E(ppr
    // Check for alignment
    if (OB_FAIL(ret)) {
      // nothing todo
    } else if (OB_ISNULL(logonly_replica)) {
      // Lack of logonly replica; waiting to make up the replica
      if (!OB_ISNULL(replica_on_logonly)) {
        // Try to move the replica on the logonly unit
        LOG_WARN("need coordiante because non_logonly replica in logonly unit",
            K(primary_partition),
            K(*replica_on_logonly));
        primary_partition_migrated = true;
        Replica dest;
        if (OB_FAIL(get_random_dest(primary_partition, *replica_on_logonly, dest))) {
          LOG_WARN("fail to get random dest", K(ret));
          if (OB_ENTRY_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("fail to get random dest", K(ret));
          }
        } else if (OB_FAIL(do_migrate_replica(primary_partition, *replica_on_logonly, dest, can_add_task))) {
          LOG_WARN("fail to migrate replica",
              K(ret),
              "partition_id",
              primary_partition.partition_id_,
              "table_id",
              primary_partition.table_id_,
              "replica",
              *replica_on_logonly);
        } else if (can_add_task) {
          task_count++;
        }
      }
    } else if (OB_ISNULL(replica_on_logonly)) {
      primary_partition_migrated = true;
      Replica dest_replica;
      dest_replica.zone_ = logonly_units_.at(i)->info_.unit_.zone_;
      dest_replica.server_ = logonly_units_.at(i)->server_;
      dest_replica.unit_ = logonly_units_.at(i);
      LOG_WARN("need coordiante becase logonly_replica not in logonly unit",
          K(primary_partition),
          K(*logonly_replica),
          K(dest_replica));
      // Empty unit, logonly replica needs to be migrated
      if (OB_FAIL(do_migrate_replica(primary_partition, *logonly_replica, dest_replica, can_add_task))) {
        LOG_WARN("fail to migreate replica", K(ret));
      } else if (can_add_task) {
        task_count++;
      }
    } else if (logonly_replica != replica_on_logonly) {
      LOG_WARN("need coordiante becase logonly_replica not in logonly unit",
          K(primary_partition),
          K(*replica_on_logonly),
          K(*logonly_replica));
      primary_partition_migrated = true;
      Replica dest;
      // Try to move away first, if the migration fails, need to delete first and then migrate
      if (OB_FAIL(get_random_dest(primary_partition, *replica_on_logonly, dest))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          LOG_WARN("fail to get random dest", K(ret));
        } else {
          // Priority to keep the replica of F
          if (REPLICA_TYPE_FULL == replica_on_logonly->replica_type_) {
            if (OB_FAIL(try_remove_and_migrate(
                    primary_partition, *replica_on_logonly, *logonly_replica, *logonly_replica, can_add_task))) {
              LOG_WARN("fail to remove and migrate", K(ret));
            } else if (can_add_task) {
              task_count++;
            }
          } else if (OB_FAIL(try_remove_and_migrate(primary_partition,
                         *logonly_replica,
                         *replica_on_logonly,
                         *replica_on_logonly,
                         can_add_task))) {
            LOG_WARN("fail to remove and migrate", K(ret));
          } else if (can_add_task) {
            task_count++;
          }
        }
      } else if (OB_FAIL(do_migrate_replica(primary_partition, *replica_on_logonly, dest, can_add_task))) {
        LOG_WARN("fail to do migrare replica", K(ret), K(primary_partition), K(*replica_on_logonly), K(dest));
      } else if (can_add_task) {
        task_count++;
      }
    } else {
      // nothing todo
    }
  }  // end for (int64_t i = 0; i < logonly_zones
  return ret;
}

int ObPartitionGroupCoordinator::coordinate_partition_member(
    const Partition& primary_partition, Partition& partition, bool& primary_partition_migrated, int64_t& task_count)
{
  int ret = OB_SUCCESS;
  task_count = 0;
  TenantBalanceStat& ts = *tenant_stat_;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!ts.is_valid() || OB_INVALID_ID == primary_partition.table_id_ || OB_INVALID_ID == partition.table_id_ ||
             primary_partition.tablegroup_id_ != partition.tablegroup_id_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "tenant stat valid", ts.is_valid(), K(primary_partition), K(partition));
  }
  // ppr : primary partition replica
  primary_partition_migrated = false;
  bool skip_coordinate_paxos_replica = false;
  int64_t tmp_task_count = 0;
  // 1. Align the replica of the paxos type first
  // 2. Then align replicas of non-paxos types
  if (OB_SUCC(ret)) {
    // Check whether ppr needs to be migrated back.
    // Need to iterate all the partitions until a suitable location is found for migration back
    if (OB_FAIL(migrate_primary_partition(primary_partition, partition, primary_partition_migrated, tmp_task_count))) {
      LOG_WARN("migrate primary partition", K(ret), K(primary_partition), K(partition));
    } else if (primary_partition_migrated) {
      task_count += tmp_task_count;
      // nothing todo
    } else if (!can_do_coordinate(partition)) {
      LOG_WARN("fail to to coordinate", K(partition));
    } else if (OB_FAIL(coordinate_paxos_replica(
                   primary_partition, partition, tmp_task_count, skip_coordinate_paxos_replica))) {
      LOG_WARN("fail to process replica in same addr", K(ret));
    } else if (0 == tmp_task_count  // Do only one replica at a time
               && !skip_coordinate_paxos_replica) {
      if (OB_FAIL(coordinate_non_paxos_replica(primary_partition, partition, tmp_task_count))) {
        LOG_WARN("fail to process replica in same zone", K(ret), K(primary_partition), K(partition));
      } else {
        task_count += tmp_task_count;
      }
    } else {
      task_count += tmp_task_count;
    }
  }
  return ret;
}

// Try to find an available machine on the same server/zone in the partition for migration;
// Check whether the primary partition needs to find the correct unit to migrate.
// In addition, if there is a block, need to migrate back or find the correct unit to migrate.
int ObPartitionGroupCoordinator::migrate_primary_partition(
    const Partition& primary_partition, const Partition& partition, bool& primary_partition_migrated, int64_t& task_cnt)

{
  int ret = OB_SUCCESS;
  Replica dest;
  primary_partition_migrated = false;
  task_cnt = 0;
  TenantBalanceStat& ts = *tenant_stat_;
  bool can_add_task = false;
  FOR_BEGIN_END_E(ppr, primary_partition, ts.all_replica_, OB_SUCC(ret) && !primary_partition_migrated)
  {
    if (!ppr->is_in_service()) {
      // nothing todo
    } else if (!ppr->server_->need_rebalance()) {
      // nothing todo
    } else {
      primary_partition_migrated = true;
      if (OB_FAIL(get_migrate_dest(primary_partition, *ppr, partition, dest))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to get migrate dest addr", K(ret), K(primary_partition), K(*ppr), K(partition));
        }
      } else if (OB_FAIL(do_migrate_replica(primary_partition, *ppr, dest, can_add_task))) {
        LOG_WARN("fail to build migrate task", KR(ret));
      } else if (can_add_task) {
        task_cnt++;
        LOG_INFO("migrate primary partition back", K(ret), K(primary_partition), K(dest));
      }
    }
  }
  return ret;
}

// Only handle the alignment of the paxos replica of the zone containing two replicas of paxos
int ObPartitionGroupCoordinator::coordinate_paxos_replica(const Partition& primary_partition,
    const Partition& partition, int64_t& task_count, bool& skip_coordinate_paxos_replica)
{
  int ret = OB_SUCCESS;
  task_count = 0;
  skip_coordinate_paxos_replica = false;
  if (OB_FAIL(ret)) {
    // nothing todo
  } else if (two_paxos_zones_.count() <= 0) {
    // nothing todo
  } else if (OB_FAIL(coordinate_specific_paxos_replica(
                 primary_partition, partition, task_count, skip_coordinate_paxos_replica))) {
    LOG_WARN("fail to coornate sepcific paxos replica", K(ret), K(two_paxos_zones_));
  }
  if (OB_FAIL(ret) || task_count > 0 || skip_coordinate_paxos_replica) {
    // nothing todo
  } else if (OB_FAIL(coordinate_normal_paxos_replica(
                 primary_partition, partition, task_count, skip_coordinate_paxos_replica))) {
    LOG_WARN("fail to coordinate paxos replica", K(ret));
  }
  return ret;
}

// If there are two replicas of paxos on the zone, first check whether the replica of F is aligned.
// If the replica of F is not aligned, it needs to be migrated for alignment,
// or it needs to be deleted and then migrated for alignment
// Secondly, under the premise that the replica of F is aligned, the alignment of the replica of L is performed
int ObPartitionGroupCoordinator::coordinate_specific_paxos_replica(const Partition& primary_partition,
    const Partition& partition, int64_t& task_count, bool& skip_coordinate_paxos_replica)
{
  int ret = OB_SUCCESS;
  skip_coordinate_paxos_replica = false;
  task_count = 0;
  TenantBalanceStat& ts = *tenant_stat_;
  bool can_add_task = false;
  for (int64_t i = 0; i < two_paxos_zones_.count() && OB_SUCC(ret) && task_count == 0; i++) {
    const ObZone& zone = two_paxos_zones_.at(i);
    Replica* ppr_full_replica = NULL;
    Replica* ppr_logonly_replica = NULL;
    FOR_BEGIN_END_E(ppr, primary_partition, ts.all_replica_, OB_SUCC(ret) && 0 == task_count)
    {
      if (!ppr->is_in_service()) {
        // not a normal replica
      } else if (!ppr->is_valid_paxos_member() || ppr->zone_ != zone) {
        // nothing todo
      } else if (REPLICA_TYPE_LOGONLY == ppr->replica_type_) {
        if (REPLICA_TYPE_LOGONLY != ppr->unit_->info_.unit_.replica_type_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("primary partition not coordinate now", K(ret), K(*ppr), "unit", ppr->unit_->info_);
        } else if (OB_ISNULL(ppr_logonly_replica)) {
          ppr_logonly_replica = ppr;
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("there are two logonly replica in zone", K(ret), K(zone), K(primary_partition));
        }
      } else if (REPLICA_TYPE_FULL == ppr->replica_type_) {
        if (REPLICA_TYPE_LOGONLY == ppr->unit_->info_.unit_.replica_type_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("primary partition not coordinate now", K(ret), K(*ppr), "unit", ppr->unit_->info_);
        } else if (OB_ISNULL(ppr_full_replica)) {
          ppr_full_replica = ppr;
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("there are two full replica in zone", K(ret), K(zone), K(primary_partition));
        }
      }
    }  // end FOR_BEGIN_END_E

    Replica* pr_full_replica = NULL;
    Replica* pr_logonly_replica = NULL;
    Replica* pr_barrier_full = NULL;
    Replica* pr_barrier_logonly = NULL;
    FOR_BEGIN_END_E(pr, partition, ts.all_replica_, OB_SUCC(ret) && 0 == task_count)
    {
      if (!pr->is_in_service()) {
        // not a normal replica
      } else if (!pr->is_valid_paxos_member() || pr->zone_ != zone) {
        // nothing todo
      } else if (REPLICA_TYPE_LOGONLY == pr->replica_type_) {
        if (OB_ISNULL(pr_logonly_replica)) {
          pr_logonly_replica = pr;
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("there are two logonly replica in zone", K(ret), K(zone), K(partition));
        }
      } else if (REPLICA_TYPE_FULL == pr->replica_type_) {
        if (OB_ISNULL(pr_full_replica)) {
          pr_full_replica = pr;
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("there are two full replica in zone", K(ret), K(zone), K(partition));
        }
      }
      if (!OB_ISNULL(ppr_full_replica) && pr->server_->server_ == ppr_full_replica->server_->server_) {
        pr_barrier_full = pr;
      }
      if (!OB_ISNULL(ppr_logonly_replica) && pr->server_->server_ == ppr_logonly_replica->server_->server_) {
        pr_barrier_logonly = pr;
      }
    }  // end FOR_BEGIN_END_E(pr, partition

    // Start to check whether it needs to be migrated or deleted
    // First check whether the replica of F needs coordinate
    // Second, check whether the replica of L needs coordinate
    if (OB_FAIL(ret)) {
      // nothing todo
    } else if (OB_ISNULL(ppr_full_replica) || OB_ISNULL(pr_full_replica)) {
      // nothing todo
    } else if (ppr_full_replica->server_->server_ == pr_full_replica->server_->server_) {
      // nothing todo
    } else {
      // need to migrate
      if (OB_ISNULL(pr_barrier_full)) {
        skip_coordinate_paxos_replica = true;
        // try migrate
        if (OB_FAIL(do_migrate_replica(partition, *pr_full_replica, *ppr_full_replica, can_add_task))) {
          LOG_WARN("fail to do migrate replica", K(ret));
        } else if (can_add_task) {
          task_count++;
        }
      } else if (OB_FAIL(try_remove_and_migrate(
                     partition, *pr_full_replica, *pr_barrier_full, *ppr_full_replica, can_add_task))) {
        LOG_WARN("fail to delete and migrate", K(ret));
        skip_coordinate_paxos_replica = true;
      } else if (can_add_task) {
        skip_coordinate_paxos_replica = true;
        task_count++;
      }
    }  // end check FULL replica

    // begin to check logonly replica
    if (OB_FAIL(ret) || skip_coordinate_paxos_replica) {
      // nothing todo
    } else if (OB_ISNULL(ppr_logonly_replica) || OB_ISNULL(pr_logonly_replica)) {
      // nothing todo
    } else if (ppr_logonly_replica->server_->server_ == pr_logonly_replica->server_->server_) {
      // nothing todo
    } else {
      // need to migrate
      if (OB_ISNULL(pr_barrier_logonly)) {
        skip_coordinate_paxos_replica = true;
        // try migrate
        if (OB_FAIL(do_migrate_replica(partition, *pr_logonly_replica, *ppr_logonly_replica, can_add_task))) {
          LOG_WARN("fail to do migrate replica", K(ret));
        } else if (can_add_task) {
          task_count++;
        }
      } else if (OB_FAIL(try_remove_and_migrate(
                     partition, *pr_logonly_replica, *pr_barrier_logonly, *ppr_logonly_replica, can_add_task))) {
        LOG_WARN("fail to delete and migrate", K(ret));
        skip_coordinate_paxos_replica = true;
      } else if (can_add_task) {
        skip_coordinate_paxos_replica = true;
        task_count++;
      }
    }  // end check logonly replica
  }    // end for (int64_t i = 0; i < two_paxos_zones_.count()
  return ret;
}

int ObPartitionGroupCoordinator::coordinate_normal_paxos_replica(const Partition& primary_partition,
    const Partition& partition, int64_t& task_count, bool& skip_coordinate_paxos_replica)
{
  int ret = OB_SUCCESS;
  task_count = 0;
  skip_coordinate_paxos_replica = false;
  TenantBalanceStat& ts = *tenant_stat_;
  bool can_add_task = false;
  // Handling of paxos type replicas separately
  FOR_BEGIN_END_E(ppr, primary_partition, ts.all_replica_, OB_SUCC(ret) && 0 == task_count)
  {
    if (!ppr->is_valid_paxos_member()) {
      continue;
    } else if (!ppr->server_->can_migrate_in() || !ppr->unit_->in_pool_) {
      skip_coordinate_paxos_replica = true;
      continue;
    } else if (has_exist_in_array(two_paxos_zones_, ppr->zone_)) {
      // nothing todo
    } else {
      Replica* paxos_replica = NULL;
      Replica* same_addr = NULL;
      FOR_BEGIN_END_E(pr, partition, ts.all_replica_, OB_SUCC(ret) && 0 == task_count)
      {
        if (!pr->is_in_service()) {
          continue;
        }
        if (pr->zone_ == ppr->zone_ && pr->is_valid_paxos_member() && pr->server_->can_migrate_in()) {
          paxos_replica = &(*pr);
        }
        if (pr->server_->server_ == ppr->server_->server_) {
          same_addr = &(*pr);
        }
      }  // FOR_BEGIN_END_E(pr
      if (OB_SUCC(ret) && 0 == task_count) {
        if (NULL == paxos_replica) {
          // Missing replica of paxos; waiting for additional replicas of other disaster recovery processes
          // nothing to do
        } else if (NULL == same_addr) {
          skip_coordinate_paxos_replica = true;
          // There is an unaligned replica of paxos type, which needs to be migrated;
          //(it may also need to do type conversion, but we only do one operation in one step)
          if (OB_FAIL(do_migrate_replica(partition, *paxos_replica, *ppr, can_add_task))) {
            LOG_WARN("fail to migrate replica", K(ret), K(partition), "target", *paxos_replica, "dest replica", *ppr);
          } else if (can_add_task) {
            task_count++;
          }
        } else if (same_addr == paxos_replica) {
          // An aligned replica of the paxos type exists, but type conversion may be required
          if (ppr->replica_type_ == paxos_replica->replica_type_) {
            // nothing todo
          } else {
            // Type conversion can be moved outside to do. Avoid comparing locality
          }
        } else {
          skip_coordinate_paxos_replica = true;
          // There are two replicas, for example, ppr is located on machine A,
          // but partition R is located on A, and F is located on machine B
          // Need to delete first, then migrate
          if (OB_FAIL(do_remove_and_migrate(partition, *paxos_replica, *ppr, can_add_task))) {
            LOG_WARN(
                "fail to do remove and migrate", K(ret), K(partition), "target", *paxos_replica, "dest replica", *ppr);
          } else if (can_add_task) {
            task_count++;
          }
        }
      }
    }
  }
  return ret;
}

int ObPartitionGroupCoordinator::coordinate_non_paxos_replica(
    const Partition& primary_partition, Partition& partition, int64_t& task_count)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  }
  ObArray<Replica*> ppr_coordinated_replica;  // Record the aligned ppr
  ObArray<Replica*> pr_coordinated_replica;   // Record the aligned pr
  task_count = 0;
  //(1) On the same OBS, see if there is a replica, if there are and the types are the same, do nothing,
  //    otherwise perform type conversion
  //(2) In the same zone, check to see if there is no replica of the same type, and if there is one, perform a
  // migration.
  //    Otherwise, do a type conversion first.(Need to exclude aligned replicas).
  if (OB_SUCC(ret)) {
    if (OB_FAIL(process_non_paxos_replica_in_same_addr(
            primary_partition, partition, ppr_coordinated_replica, pr_coordinated_replica, task_count))) {
      LOG_WARN("fail to process replica in same addr", K(ret));
    } else if (task_count == 0  // Do only one replica at a time
               && OB_FAIL(process_non_paxos_replica_in_same_zone(
                      primary_partition, partition, ppr_coordinated_replica, pr_coordinated_replica, task_count))) {
      LOG_WARN("fail to process replica in same zone", K(ret), K(primary_partition), K(partition));
    }
  }
  return ret;
}

int ObPartitionGroupCoordinator::process_non_paxos_replica_in_same_zone(const Partition& primary_partition,
    Partition& partition, ObIArray<Replica*>& ppr_coordinated_replica, ObIArray<Replica*>& pr_coordinated_replica,
    int64_t& task_count)
{
  int ret = OB_SUCCESS;
  TenantBalanceStat& ts = *tenant_stat_;
  bool can_add_task = false;
  FOR_BEGIN_END_E(ppr, primary_partition, ts.all_replica_, OB_SUCC(ret) && 0 == task_count)
  {
    if (!ppr->is_in_service() || !ppr->server_->active_ || !ppr->server_->online_ || !ppr->unit_->in_pool_ ||
        ppr->server_->server_ != ppr->unit_->info_.unit_.server_) {
      continue;
    } else if (ObReplicaTypeCheck::is_paxos_replica_V2(ppr->replica_type_)) {
      continue;
    } else if (has_exist_in_array(ppr_coordinated_replica, &(*ppr))) {
      continue;
    }
    bool found_addr = false;
    FOR_BEGIN_END_E(pr, partition, ts.all_replica_, !found_addr && OB_SUCCESS == ret)
    {
      if (!pr->is_in_service() || has_exist_in_array(pr_coordinated_replica, &(*pr))) {
        continue;
      } else if (ObReplicaTypeCheck::is_paxos_replica_V2(pr->replica_type_)) {
        continue;
      } else if (pr->zone_ == ppr->zone_) {
        // need to migrate;
        found_addr = true;
        if (OB_FAIL(do_migrate_replica(partition, *pr, *ppr, can_add_task))) {
          LOG_WARN("fail to do migrate", K(ret), K(partition), K(primary_partition));
        } else if (OB_FAIL(pr_coordinated_replica.push_back(&(*pr)))) {
          LOG_WARN("fail to push back", K(ret));
        } else if (can_add_task) {
          task_count++;
        }
      }
    }
  }
  return ret;
}

int ObPartitionGroupCoordinator::process_non_paxos_replica_in_same_addr(const Partition& primary_partition,
    Partition& partition, ObIArray<Replica*>& ppr_coordinated_replica, ObIArray<Replica*>& pr_coordinated_replica,
    int64_t& task_count)
{
  int ret = OB_SUCCESS;
  TenantBalanceStat& ts = *tenant_stat_;
  FOR_BEGIN_END_E(ppr, primary_partition, ts.all_replica_, OB_SUCC(ret) && 0 == task_count)
  {
    if (!ppr->is_in_service() || !ppr->server_->can_migrate_in() || !ppr->unit_->in_pool_ ||
        ppr->server_->server_ != ppr->unit_->info_.unit_.server_) {
      continue;
    } else if (ObReplicaTypeCheck::is_paxos_replica_V2(ppr->replica_type_)) {
      continue;
    }
    FOR_BEGIN_END_E(pr, partition, ts.all_replica_, OB_SUCCESS == ret)
    {
      if (pr->is_in_service() && ppr->server_->server_ == pr->server_->server_) {
        if (pr->replica_type_ == ppr->replica_type_) {
          // nothing todo
        } else if (ObReplicaTypeCheck::is_paxos_replica_V2(pr->replica_type_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to be here", K(ret), K(primary_partition), K(*pr), K(partition));
        } else {
          // Don't do type conversion here
          // For the time being, the situation where there are three types of replicas in a zone will not be processed
          // If there are three types of replicas in a zone,
          // except for the paxos type, the other two types should also be aligned.
          // At this time, the problem of type conversion may be involved.
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(pr_coordinated_replica.push_back(pr))) {
            LOG_WARN("fail to push back coordinated replica", K(ret), K(*pr));
          } else if (OB_FAIL(ppr_coordinated_replica.push_back(&(*ppr)))) {
            LOG_WARN("fail to push back", K(ret));
          }
        }
      }  // if server=server
    }    // end FOR_BEGIN_END_E(pr, partition
  }      // end FOR_BEGIN_END_E(ppr, primary_partition
  return ret;
}

// 14x new deployment method: F/L@zone
//(1) Check whether a zone has only one paxos type;
//    if a zone has multiple replicas of paxos, it will affect the coordinate
// NOTE: In the pg coordinate stage, type conversion is not considered.
//      In such a situation, skip all
//(2) If there is a unit migration operation, do not coordinate;
//    wait for the unit migration to be completed
bool ObPartitionGroupCoordinator::can_do_coordinate(const Partition& partition)
{
  bool bret = true;
  int ret = OB_SUCCESS;
  TenantBalanceStat& ts = *tenant_stat_;
  if (!ts.is_valid()) {
    bret = false;
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid tenant stat", K(ret));
  } else {
    FOR_BEGIN_END_E(pr, partition, ts.all_replica_, OB_SUCC(ret) && bret)
    {
      if (!pr->is_in_service()) {
        continue;
      }
      if (pr->server_->server_ != pr->unit_->info_.unit_.server_) {
        bret = false;
        LOG_INFO("can not do coordiante now, partition have unit balance task todo",
            K(partition),
            "unit",
            pr->unit_->info_.unit_,
            "replica_addr",
            pr->server_->server_);
      }
    }
  }
  return bret;
  ;
}

// If the replica that you want to delete is an L replica,
// need to determine whether there is a lack of PAXOS replicas.
// If the paxos replica is not enough, you cannot delete
int ObPartitionGroupCoordinator::try_remove_and_migrate(const Partition& partition, const Replica& to_migrate_replica,
    const Replica& to_delete_replica, const Replica& migrate_dest_in_ppr, bool& can_add_task)
{
  int ret = OB_SUCCESS;
  TenantBalanceStat& ts = *tenant_stat_;
  can_add_task = false;
  if (!ts.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid tenant stat", K(ret));
  } else if (ObReplicaTypeCheck::is_paxos_replica_V2(to_delete_replica.replica_type_)) {
    if (partition.valid_member_cnt_ < partition.schema_replica_cnt_) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("already lost paxos replica, refuse to delete one", K(ret), K(partition));
    }
  }
  if (OB_FAIL(ret)) {
    // nothing todo
  } else if (OB_FAIL(do_remove_replica(partition, to_delete_replica))) {
    LOG_WARN("fail to remove replica", K(ret));
  } else if (OB_FAIL(do_migrate_replica(partition, to_migrate_replica, migrate_dest_in_ppr, can_add_task))) {
    LOG_WARN("fail to migrate replica", K(ret));
  }
  return ret;
}

// partition: objects to be deleted and migrated;
// First find the replica on the machine where the ppr in the partition is located, and delete it;
// then migrate the replica to the machine where the ppr is located;
int ObPartitionGroupCoordinator::do_remove_and_migrate(
    const Partition& partition, const Replica& replica, const Replica& ppr, bool& can_add_task)
{
  int ret = OB_SUCCESS;
  TenantBalanceStat& ts = *tenant_stat_;
  can_add_task = false;
  if (!ts.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid tenant stat", K(ret));
  } else {
    // find replica in ppr->server in partition and remove it
    FOR_BEGIN_END_E(pr, partition, ts.all_replica_, OB_SUCC(ret))
    {
      if (pr->is_in_service() && pr->server_->server_ == ppr.server_->server_) {
        if (ObReplicaTypeCheck::is_paxos_replica_V2(pr->replica_type_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN(
              "should not remove paxos member in partition group", K(ret), K(partition), K(replica), K(ppr), K(*pr));
        } else if (OB_FAIL(do_remove_replica(partition, *pr))) {
          LOG_WARN("fail to remove replica", K(ret), K(partition), K(replica), K(ppr));
        } else if (OB_FAIL(do_migrate_replica(partition, replica, ppr, can_add_task))) {
          LOG_WARN("fail to migrate replica", K(ret), K(partition), K(replica), K(ppr));
        }
        break;
      }
    }
  }
  return ret;
}

int ObPartitionGroupCoordinator::do_remove_replica(const Partition& partition, const Replica& replica)
{
  int ret = OB_SUCCESS;
  TenantBalanceStat& ts = *tenant_stat_;
  ObPartitionKey key;
  OnlineReplica remove_member;
  remove_member.member_ = ObReplicaMember(replica.server_->server_, replica.member_time_us_, replica.replica_type_);
  remove_member.zone_ = replica.zone_;
  const char* comment = balancer::PG_COORDINATE_REMOVE_REPLICA;

  if (!ts.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid tenant stat", K(ret));
  } else if (OB_FAIL(ts.gen_partition_key(partition, key))) {
    LOG_WARN("fail to gen partition key", K(ret), K(partition));
  } else if (ObReplicaTypeCheck::is_paxos_replica_V2(replica.replica_type_)) {
    ObRemoveMemberTask task;
    ObRemoveMemberTaskInfo task_info;
    common::ObArray<ObRemoveMemberTaskInfo> task_info_array;
    int64_t quorum = 0;
    if (replica.is_in_blacklist(ObRebalanceTaskType::MEMBER_CHANGE, replica.server_->server_, tenant_stat_)) {
      ret = OB_REBALANCE_TASK_CANT_EXEC;
      LOG_WARN("replica member change frequent failed, now in black list", K(replica));
    } else if (OB_FAIL(ts.get_remove_replica_quorum_size(partition, replica.zone_, replica.replica_type_, quorum))) {
      LOG_WARN("fail to get quorum size", K(ret));
    } else if (OB_FAIL(task_info.build(remove_member, key, quorum, partition.get_quorum()))) {
      LOG_WARN("fail to build remove member task info", K(ret));
    } else if (OB_FAIL(task_info_array.push_back(task_info))) {
      LOG_WARN("fail to push back", K(ret));
    } else if (OB_FAIL(task.build(task_info_array, remove_member.member_.get_server(), comment))) {
      LOG_WARN("fail to build remove member task", K(ret));
    } else if (OB_FAIL(task_mgr_->add_task(task))) {
      LOG_WARN("fail to add task", K(ret), K(replica));
    } else {
      LOG_INFO("add task to remove member for coordinate", K(replica), K(partition));
    }
  } else {
    ObRemoveNonPaxosTask task;
    ObRemoveNonPaxosTaskInfo task_info;
    common::ObArray<ObRemoveNonPaxosTaskInfo> task_info_array;
    if (replica.is_in_blacklist(
            ObRebalanceTaskType::REMOVE_NON_PAXOS_REPLICA, replica.server_->server_, tenant_stat_)) {
      ret = OB_REBALANCE_TASK_CANT_EXEC;
      LOG_WARN("task in black list", K(replica));
    } else if (OB_FAIL(task_info.build(remove_member, key))) {
      LOG_WARN("fail to build remove non paxos replica task info", K(ret), K(key));
    } else if (OB_FAIL(task_info_array.push_back(task_info))) {
      LOG_WARN("fail to push back", K(ret));
    } else if (OB_FAIL(task.build(task_info_array, remove_member.member_.get_server(), comment))) {
      LOG_WARN("fail to build remove non paxos replcia", K(ret), K(partition), K(remove_member));
    } else if (OB_FAIL(task_mgr_->add_task(task))) {
      LOG_WARN("add task failed", K(ret), K(task));
    } else {
      LOG_INFO("add remove replica task for partition group coordinate", K(task));
    }
  }
  return ret;
}

// Migrate replica to the machine where dest_replica is located
// Don't worry about the migration target is the leader.
// When executing, the leader will be moved first.
int ObPartitionGroupCoordinator::do_migrate_replica(
    const Partition& partition, const Replica& replica, const Replica& dest_replica, bool& can_add_task)
{
  int ret = OB_SUCCESS;
  TenantBalanceStat& ts = *tenant_stat_;
  can_add_task = false;
  if (!ts.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid tenant stat", K(ret));
  } else if (!ts.has_leader_while_member_change(partition, replica.server_->server_)) {
    // Ignore the error code,
    // otherwise it will fail to initiate the migration replica task due to
    // the inability to align the standby database for a single zone and multiple units,
    // affecting the swtich_over operation
    LOG_WARN("can not do migrate, may no leader", K(ret), K(partition), K(replica), K(dest_replica));
  } else if (replica.is_in_blacklist(
                 ObRebalanceTaskType::MIGRATE_REPLICA, dest_replica.server_->server_, tenant_stat_)) {
    ret = OB_REBALANCE_TASK_CANT_EXEC;
    LOG_INFO("replica in black list, wait", K(replica), K(dest_replica));
  } else {
    ObMigrateReplicaTask task;
    ObMigrateTaskInfo task_info;
    common::ObArray<ObMigrateTaskInfo> task_info_array;
    ObPartitionKey key;
    ObReplicaMember data_source;
    ObReplicaMember src = ObReplicaMember(
        replica.server_->server_, replica.member_time_us_, replica.replica_type_, replica.get_memstore_percent());
    const char* comment = balancer::PG_COORDINATE_MIGRATE;
    OnlineReplica dst;
    dst.member_ = ObReplicaMember(dest_replica.server_->server_,
        ObTimeUtility::current_time(),
        replica.replica_type_,
        replica.get_memstore_percent());
    dst.zone_ = dest_replica.zone_;
    dst.member_.set_region(dest_replica.region_);
    dst.unit_id_ = dest_replica.unit_->info_.unit_.unit_id_;
    int64_t quorum = 0;
    int64_t transmit_data_size = 0;
    int64_t cluster_id = OB_INVALID_ID;
    bool is_restore = partition.in_physical_restore();
    const obrpc::MigrateMode migrate_mode = obrpc::MigrateMode::MT_LOCAL_FS_MODE;
    if (OB_FAIL(ts.gen_partition_key(partition, key))) {
      LOG_WARN("fail to gen partition key", K(ret), K(partition));
    } else if (OB_FAIL(ts.choose_data_source(partition,
                   dst.member_,
                   src,
                   false,
                   data_source,
                   transmit_data_size,
                   ObRebalanceTaskType::MIGRATE_REPLICA,
                   cluster_id))) {
      LOG_WARN("fail to choose data source", K(ret), K(partition), K(replica));
    } else if (OB_FAIL(ts.get_migrate_replica_quorum_size(partition, quorum))) {
      LOG_WARN("fail to get quorum size", K(ret));
    } else if (FALSE_IT(task_info.set_transmit_data_size(transmit_data_size))) {
      // nothing todo
    } else if (OB_FAIL(task_info.build(migrate_mode, dst, key, src, data_source, quorum, is_restore))) {
      LOG_WARN("fail to build migrate task info", K(ret));
    } else if (OB_FAIL(task_info_array.push_back(task_info))) {
      LOG_WARN("fail to push back", K(ret));
    } else if (OB_FAIL(task.build(migrate_mode,
                   task_info_array,
                   dst.member_.get_server(),
                   ObRebalanceTaskPriority::LOW_PRI,
                   comment))) {
      LOG_WARN("fail to build migrate task", K(ret), K(key), K(partition));
    } else if (OB_FAIL(task_mgr_->add_task(task))) {
      LOG_WARN("add task failed", K(ret), K(task));
    } else {
      can_add_task = true;
      LOG_INFO("add migrate task for partition group coordinate", K(task));
    }
  }
  return ret;
}

// Choose a migration destination for ppr
// First select the replica location of the same type in this zone; secondly,
// choose a replica location in this zone at will
int ObPartitionGroupCoordinator::get_migrate_dest(
    const Partition& primary_partition, const Replica& ppr, const Partition& partition, Replica& dest)
{
  int ret = OB_SUCCESS;
  if (ppr.server_->can_migrate_in()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ppr));
  } else {
    Replica* same_type_replica = NULL;
    Replica* same_zone_repilca = NULL;
    TenantBalanceStat& ts = *tenant_stat_;
    bool find = false;
    FOR_BEGIN_END_E(pr, partition, ts.all_replica_, OB_SUCC(ret))
    {
      if (pr->is_in_service() && pr->zone_ == ppr.zone_) {
        if (pr->server_->server_ == ppr.server_->server_) {  // ppr->server has been blocked
          // nothing todo
        } else if (pr->unit_->info_.unit_.replica_type_ != ppr.unit_->info_.unit_.replica_type_) {
          // If the unit types are not the same, can't migrate back and wait for the fault to be resolved
          // nothing todo
        } else if (pr->server_->can_migrate_in() && !ts.has_replica(primary_partition, pr->server_->server_)) {
          same_zone_repilca = &(*pr);
          if (pr->replica_type_ == ppr.replica_type_) {
            same_type_replica = &(*pr);
          }
        }
      }
    }  // end FOR_BEGIN_END_E(pr,
    if (OB_SUCC(ret) && (same_type_replica != NULL || same_zone_repilca != NULL)) {
      if (same_type_replica != NULL) {
        dest = *same_type_replica;
        find = true;
      } else {
        dest = *same_zone_repilca;
        find = true;
      }
    }
    // The above logic may not find a suitable location, need to find a unit to move in
    if (OB_FAIL(ret) || find) {
      // nothing todo
    } else if (OB_FAIL(get_random_dest(primary_partition, ppr, dest))) {
      LOG_WARN("fail to get random dest", K(ret));
    }
  }
  return ret;
}

int ObPartitionGroupCoordinator::get_random_dest(const Partition& primary_partition, const Replica& ppr, Replica& dest)
{
  int ret = OB_SUCCESS;
  TenantBalanceStat& ts = *tenant_stat_;
  bool find = false;
  for (int64_t i = 0; i < ts.all_zone_unit_.count() && OB_SUCC(ret) && !find; i++) {
    const ZoneUnit& zu = ts.all_zone_unit_.at(i);
    if (zu.zone_ != ppr.zone_) {
      // nothing todo
    } else {
      for (int64_t j = 0; j < zu.all_unit_.count() && OB_SUCC(ret); j++) {
        UnitStat* us = zu.all_unit_.at(j);
        if (!us->server_->can_migrate_in() || ts.has_replica(primary_partition, us->server_->server_) ||
            (REPLICA_TYPE_LOGONLY == us->info_.unit_.replica_type_ &&
                ppr.unit_->info_.unit_.replica_type_ != us->info_.unit_.replica_type_)) {
          // nothing todo
        } else {
          find = true;
          dest.unit_ = us;
          dest.server_ = us->server_;
          dest.zone_ = ppr.zone_;
          dest.region_ = ppr.region_;
        }
      }
    }
  }
  if (OB_SUCC(ret) && !find) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("fail to find dest for replica", K(primary_partition), K(ppr));
  }
  return ret;
}
