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
#include "ob_migrate_unit.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_array_iterator.h"
#include "lib/container/ob_se_array.h"
#include "lib/container/ob_se_array_iterator.h"
#include "ob_rebalance_task_mgr.h"
#include "ob_unit_manager.h"
#include "ob_balance_info.h"
#include "ob_root_utils.h"
#include "ob_root_utils.h"
#include "observer/ob_server_struct.h"
#include "share/ob_multi_cluster_util.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::rootserver;
ObMigrateUnit::ObMigrateUnit()
    : inited_(false), unit_mgr_(NULL), task_mgr_(NULL), tenant_stat_(NULL), check_stop_provider_(NULL)
{}

int ObMigrateUnit::init(ObUnitManager& unit_mgr, ObRebalanceTaskMgr& task_mgr, TenantBalanceStat& tenant_stat,
    share::ObCheckStopProvider& check_stop_provider)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
  } else {
    unit_mgr_ = &unit_mgr;
    task_mgr_ = &task_mgr;
    tenant_stat_ = &tenant_stat;
    check_stop_provider_ = &check_stop_provider;

    inited_ = true;
  }
  return ret;
}

/* note:
 * rereplication.replicate_to_unit is used to migrate unit when disaster recovery
 * migrate_unit.migrate_to_unit is used to migrate unit when load balance
 */
int ObMigrateUnit::migrate_to_unit(int64_t& task_cnt)
{
  int ret = OB_SUCCESS;
  TenantBalanceStat& ts = *tenant_stat_;
  bool small_tenant = false;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!ts.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "tenant stat valid", ts.is_valid());
  } else if (OB_FAIL(ObTenantUtils::check_small_tenant(ts.tenant_id_, small_tenant))) {
    LOG_WARN("fail to check small tenant", K(ret), "tenant_id", ts.tenant_id_);
  } else {
  }  // no more to do in this branch
  // the guard partition when is going to be migrated in one partition group
  const Partition* first_migrate_p = NULL;
  const Replica* first_migrate_r = NULL;
  common::ObAddr hint_data_src;
  common::ObArray<ObMigrateTaskInfo> task_info_array;
  FOREACH_X(p, ts.sorted_partition_, OB_SUCCESS == ret)
  {
    if (OB_FAIL(check_stop())) {
      LOG_WARN("balancer stop", K(ret));
    } else if ((*p)->is_primary() && task_cnt > ObRebalanceTaskMgr::ONCE_ADD_TASK_CNT) {
      break;
    } else if (!(*p)->can_balance()) {
      LOG_WARN("partition can't migrate", "partition", *(*p));
    } else if (!(*p)->is_valid_quorum()) {
      // quorum is introduced from the version 1.4.71, it is updated by partition meta reports,
      // an invalid value of quorum means it is not reported yet, we shall ignore the invalid value and
      // go on to process the next partition
      LOG_WARN("quorum not report yet, just skip", K(ret), K(*p));
      continue;
    } else {
      FOR_BEGIN_END_E(r, *(*p), ts.all_replica_, OB_SUCCESS == ret)
      {
        if (r->replica_status_ == REPLICA_STATUS_NORMAL && r->unit_->in_pool_ &&
            r->server_->server_ != r->unit_->info_.unit_.server_ && r->unit_->server_->online_ &&
            !r->unit_->server_->blocked_ && ts.has_leader_while_member_change(*(*p), r->server_->server_) &&
            !r->is_in_blacklist(ObRebalanceTaskType::MIGRATE_REPLICA, r->unit_->info_.unit_.server_, tenant_stat_)) {
          LOG_INFO("try to migrate replica, because server and unit location is different",
              "partition",
              *(*p),
              "from_server",
              r->server_->server_,
              "dest_server",
              r->unit_->info_.unit_.server_);
          ObMigrateTaskInfo task_info;
          ObPartitionKey key;
          OnlineReplica dst;
          ObReplicaMember data_src;
          ObReplicaMember src =
              ObReplicaMember(r->server_->server_, r->member_time_us_, r->replica_type_, r->get_memstore_percent());
          dst.member_ = ObReplicaMember(r->unit_->info_.unit_.server_,
              ObTimeUtility::current_time(),
              r->replica_type_,
              r->get_memstore_percent());
          dst.zone_ = r->zone_;
          dst.member_.set_region(r->region_);
          dst.unit_id_ = r->unit_->info_.unit_.unit_id_;
          int64_t quorum = 0;
          int64_t transmit_data_size = 0;
          int64_t cluster_id = OB_INVALID_ID;
          bool do_accumulated = false;
          const obrpc::MigrateMode migrate_mode = obrpc::MigrateMode::MT_LOCAL_FS_MODE;
          const bool same_pg_with_first_migrate_p = (NULL != first_migrate_p && NULL != first_migrate_r &&
                                                     (is_same_pg(*first_migrate_p, *(*p)) || small_tenant));
          bool is_restore = (*p)->in_physical_restore();
          if (same_pg_with_first_migrate_p &&
              first_migrate_r->unit_->info_.unit_.server_ != r->unit_->info_.unit_.server_) {
            // ignore when the two destinations are different
          } else if (OB_FAIL(ts.gen_partition_key(*(*p), key))) {
            LOG_WARN("generate partition key failed", K(ret), "partition", *(*p));
          } else if (OB_FAIL(ts.choose_data_source(*(*p),
                         dst.member_,
                         src,
                         false,
                         data_src,
                         transmit_data_size,
                         ObRebalanceTaskType::MIGRATE_REPLICA,
                         cluster_id,
                         (same_pg_with_first_migrate_p ? &hint_data_src : NULL)))) {
            LOG_WARN("set task src failed", K(ret));
          } else if (OB_FAIL(ts.get_migrate_replica_quorum_size(*(*p), quorum))) {
            LOG_WARN("fail to get quorum size", K(ret));
          } else if (FALSE_IT(task_info.set_transmit_data_size(transmit_data_size))) {
            // nop
          } else if (OB_FAIL(task_info.build(migrate_mode,
                         dst,
                         key,
                         src,
                         data_src,
                         quorum,
                         is_restore,
                         r->unit_->info_.unit_.is_manual_migrate()))) {
            LOG_WARN("fail to build add task", K(ret), K(key));
          } else if (OB_FAIL(try_accumulate_task_info(small_tenant,
                         task_info_array,
                         task_info,
                         *p,
                         r,
                         first_migrate_p,
                         first_migrate_r,
                         task_cnt,
                         do_accumulated))) {
            LOG_WARN("fail to accumulate task info", K(ret));
          } else if (do_accumulated) {
            // accumulate a replica of this partition, break in this loop and check next partition
            hint_data_src = data_src.get_server();
            break;
          } else {
          }  // do not accumulate, go on for next replica in this partition
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (task_info_array.count() > 0) {
      ObMigrateReplicaTask task;
      obrpc::MigrateMode migrate_mode = obrpc::MigrateMode::MT_MAX;
      if (NULL == first_migrate_r) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null first migrate replica", K(ret));
      } else if (FALSE_IT(migrate_mode = obrpc::MigrateMode::MT_LOCAL_FS_MODE)) {
        // shall never by here
      } else if (OB_FAIL(task.build(migrate_mode,
                     task_info_array,
                     first_migrate_r->unit_->info_.unit_.server_,
                     ObRebalanceTaskPriority::LOW_PRI,
                     (first_migrate_r->unit_->info_.unit_.is_manual_migrate() ? balancer::MANUAL_MIGRATE_UNIT
                                                                              : balancer::MIGRATE_TO_NEW_SERVER)))) {
        LOG_WARN("fail to build migrate task", K(ret));
      } else if (OB_FAIL(task_mgr_->add_task(task, task_cnt))) {
        LOG_WARN("fail to add task", K(ret));
      } else {
      }  // no more to do
    }
  }
  return ret;
}

// check if the unit migration is finished
// we need to guarantee there are no replicas on the source server
int ObMigrateUnit::unit_migrate_finish(int64_t& task_cnt)
{
  int ret = OB_SUCCESS;
  TenantBalanceStat& ts = *tenant_stat_;
  LOG_INFO("start check unit migrate finish");
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!ts.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "tenant stat valid", ts.is_valid());
  }
  FOREACH_X(r, ts.all_replica_, OB_SUCCESS == ret)
  {
    if (r->server_->server_ != r->unit_->info_.unit_.server_ && (r->is_in_service() || r->only_in_member_list())) {
      r->unit_->outside_replica_cnt_++;
    }
    if (r->server_->server_ == r->unit_->info_.unit_.server_ && (r->is_in_service() || r->only_in_member_list())) {
      r->unit_->inside_replica_cnt_++;
    }
  }

  FOREACH_X(zu, ts.all_zone_unit_, OB_SUCCESS == ret)
  {
    FOREACH_X(u, zu->all_unit_, OB_SUCCESS == ret)
    {
      if ((*u)->in_pool_) {
        if ((*u)->outside_replica_cnt_ == 0 && (*u)->info_.unit_.migrate_from_server_.is_valid()) {
          task_cnt++;
          if (OB_FAIL(unit_mgr_->finish_migrate_unit((*u)->info_.unit_.unit_id_))) {
            LOG_WARN("set unit migrate finish failed", K(ret), "unit_id", (*u)->info_.unit_.unit_id_);
          } else {
            LOG_INFO("finish migrate unit", K(ret), "unit_id", (*u)->info_.unit_.unit_id_);
          }
        }
      }
    }
  }

  return ret;
}

int ObMigrateUnit::try_accumulate_task_info(const bool small_tenant,
    common::ObIArray<ObMigrateTaskInfo>& task_info_array, const ObMigrateTaskInfo& task_info,
    const Partition* cur_partition, const Replica* cur_replica, const Partition*& first_migrate_p,
    const Replica*& first_migrate_r, int64_t& task_cnt, bool& do_accumulated)
{
  int ret = OB_SUCCESS;
  ObMigrateReplicaTask task;
  TenantBalanceStat& ts = *tenant_stat_;
  if (OB_UNLIKELY(NULL == cur_partition || NULL == cur_replica)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(cur_partition), KP(cur_replica));
  } else if (NULL == first_migrate_p) {
    // first_migrate_p is not set before
    first_migrate_p = cur_partition;
    first_migrate_r = cur_replica;
    if (OB_FAIL(task_info_array.push_back(task_info))) {
      LOG_WARN("fail to push back", K(ret));
    } else {
      do_accumulated = true;
    }
  } else if (OB_ISNULL(first_migrate_p) || OB_ISNULL(cur_partition)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("first or cur partition is null", K(ret), K(first_migrate_p), K(cur_partition));
  } else {
    if (is_same_pg(*first_migrate_p, *cur_partition) || small_tenant) {
      // no accumulation on standby cluster, since in standby cluster we divide all partition
      // into two different categories, private partitions and non-private partitons, the membership
      // change protocal for there two categories are different, so we do not do batch migration for
      // standby cluster
      if (NULL == first_migrate_r) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL first mgirate replica", K(ret), KP(first_migrate_r));
      } else if (first_migrate_r->unit_->info_.unit_.server_ != cur_replica->unit_->info_.unit_.server_ ||
                 (GCTX.is_standby_cluster() && OB_SYS_TENANT_ID != ts.tenant_id_)) {
        // destinations are different
        // no accumulation on standby cluster
        do_accumulated = false;
      } else {
        // the same destination, push the task info back to this array
        if (OB_FAIL(task_info_array.push_back(task_info))) {
          LOG_WARN("fail to push back", K(ret));
        } else {
          do_accumulated = true;
        }
      }
    } else {
      // execute the previous task and start to accumulate the new one
      if (NULL == first_migrate_r) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null first migrate replica", K(ret), KP(first_migrate_r));
      } else if (task_info_array.count() <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("task info array count unexpected", K(ret));
      } else {
        const common::ObZone& zone = first_migrate_r->unit_->info_.unit_.zone_;
        const obrpc::MigrateMode migrate_mode = obrpc::MigrateMode::MT_LOCAL_FS_MODE;
        if (OB_FAIL(task.build(migrate_mode,
                task_info_array,
                first_migrate_r->unit_->info_.unit_.server_,
                ObRebalanceTaskPriority::LOW_PRI,
                (first_migrate_r->unit_->info_.unit_.is_manual_migrate() ? balancer::MANUAL_MIGRATE_UNIT
                                                                         : balancer::MIGRATE_TO_NEW_SERVER)))) {
          LOG_WARN("fail to build migrate task", K(ret));
        } else if (OB_FAIL(task_mgr_->add_task(task, task_cnt))) {
          LOG_WARN("fail to add task", K(ret));
        } else {  // finish the previous batch migration task
          task_info_array.reset();
          if (OB_FAIL(task_info_array.push_back(task_info))) {
            LOG_WARN("fail to push back", K(ret));
          } else {
            first_migrate_p = cur_partition;
            first_migrate_r = cur_replica;
            do_accumulated = true;
          }
        }
      }
    }
  }
  return ret;
}
