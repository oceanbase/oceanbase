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
#include "ob_single_zone_mode_migrate_replica.h"
#include "ob_balance_info.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_array_iterator.h"
#include "lib/container/ob_se_array.h"
#include "lib/container/ob_se_array_iterator.h"
#include "share/partition_table/ob_partition_table_operator.h"
#include "share/config/ob_server_config.h"
#include "share/ob_multi_cluster_util.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/ob_replica_info.h"
#include "ob_rebalance_task.h"
#include "ob_rebalance_task_mgr.h"
#include "ob_root_utils.h"
#include "rootserver/ob_unit_load_history_table_operator.h"
#include "rootserver/ob_zone_manager.h"
#include "rootserver/ob_partition_disk_balancer.h"
#include "rootserver/ob_balance_group_container.h"
#include "rootserver/ob_balance_group_data.h"
#include "rootserver/ob_server_manager.h"
#include "rootserver/ob_unit_manager.h"
#include "observer/ob_server_struct.h"
#include "ob_alloc_replica_strategy.h"

using namespace oceanbase::common;
using namespace oceanbase::rootserver;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

ObSingleZoneModeMigrateReplica::ObSingleZoneModeMigrateReplica()
    : inited_(false),
      config_(NULL),
      schema_service_(NULL),
      pt_operator_(NULL),
      task_mgr_(NULL),
      zone_mgr_(NULL),
      tenant_stat_(NULL),
      origin_tenant_stat_(NULL),
      check_stop_provider_(NULL)
{}

int ObSingleZoneModeMigrateReplica::init(common::ObServerConfig& cfg,
    share::schema::ObMultiVersionSchemaService& schema_service, share::ObPartitionTableOperator& pt_operator,
    ObRebalanceTaskMgr& task_mgr, ObZoneManager& zone_mgr, TenantBalanceStat& tenant_stat,
    share::ObCheckStopProvider& check_stop_provider)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
  } else {
    config_ = &cfg;
    schema_service_ = &schema_service;
    pt_operator_ = &pt_operator;
    task_mgr_ = &task_mgr;
    zone_mgr_ = &zone_mgr;
    origin_tenant_stat_ = &tenant_stat;
    tenant_stat_ = &tenant_stat;
    check_stop_provider_ = &check_stop_provider;

    inited_ = true;
  }
  return ret;
}

int ObSingleZoneModeMigrateReplica::migrate_replica(int64_t& task_cnt)
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
    const Partition* first_migrate_p = NULL;  // guard partition for partition group migration
    common::ObArray<ObMigrateTaskInfo> task_info_array;
    Replica dest_replica;
    FOREACH_X(p, ts.sorted_partition_, OB_SUCCESS == ret)
    {
      if (OB_FAIL(check_stop())) {
        LOG_WARN("balancer stop", K(ret));
      } else if (is_inner_table((*p)->table_id_)) {
        // FIXME: may have bugs
      } else {  // user table
        if ((*p)->is_primary() && task_cnt > ObRebalanceTaskMgr::ONCE_ADD_TASK_CNT) {
          break;
        } else if (OB_FAIL(migrate_not_inner_table_replica(
                       *(*p), first_migrate_p, dest_replica, small_tenant, task_info_array, task_cnt))) {
          LOG_WARN("fail to try migrate replica", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (task_info_array.count() > 0) {
        ObMigrateReplicaTask task;
        if (OB_FAIL(task.build(obrpc::MigrateMode::MT_SINGLE_ZONE_MODE,
                task_info_array,
                dest_replica.server_->server_,
                ObRebalanceTaskPriority::LOW_PRI,
                (dest_replica.unit_->info_.unit_.is_manual_migrate() ? balancer::MANUAL_MIGRATE_UNIT
                                                                     : balancer::MIGRATE_TO_NEW_SERVER)))) {
          LOG_WARN("fail to build migrate task", K(ret));
        } else if (OB_FAIL(task_mgr_->add_task(task, task_cnt))) {
          LOG_WARN("fail to add task", K(ret));
        } else {
        }  // no more to do
      }
    }
  }
  return ret;
}

int ObSingleZoneModeMigrateReplica::migrate_not_inner_table_replica(const Partition& p,
    const Partition*& first_migrate_p, Replica& dest_replica, bool& small_tenant,
    common::ObIArray<ObMigrateTaskInfo>& task_info_array, int64_t& task_cnt)
{
  int ret = OB_SUCCESS;
  Replica dest;
  TenantBalanceStat& ts = *tenant_stat_;
  FOR_BEGIN_END_E(pr, p, ts.all_replica_, OB_SUCC(ret))
  {
    if (!pr->is_in_service()) {
      // bypass
    } else if (nullptr == pr->server_) {
      // bypass
    } else if (!ts.has_leader_while_member_change(p, pr->server_->server_)) {
      // bypass
    } else if (pr->server_->is_stopped()) {  // process the replicas only on stopped server
      bool do_accumulated = false;
      ObReplicaMember data_source;
      if (OB_ISNULL(first_migrate_p)) {  // find a destination randomly
        if (OB_FAIL(get_random_dest(p, *pr, dest_replica, dest))) {
          if (OB_ENTRY_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("fail to get migrate dest addr", K(ret), K(p), K(*pr));
          }
        }
      } else {
        // partitions with the same destination for partition group or small tenant migration
        if (is_same_pg(*first_migrate_p, p) || small_tenant) {
          if (!OB_ISNULL(first_migrate_p)) {
            dest = dest_replica;
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("first partition is NULL", K(ret), K(first_migrate_p));
          }
        } else if (OB_FAIL(get_random_dest(p, *pr, dest_replica, dest))) {
          if (OB_ENTRY_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("fail to get migrate dest addr", K(ret), K(p), K(*pr));
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (do_migrate_replica(
                p, *pr, dest, first_migrate_p, data_source, small_tenant, task_info_array, task_cnt, do_accumulated)) {
          LOG_WARN("fail to build migrate task");
        } else if (do_accumulated) {
          break;
        } else {
        }  // do not accumulate, go on for next replica int this partition
      }
    } else {
    }  // nothing todo
  }
  return ret;
}

int ObSingleZoneModeMigrateReplica::get_random_dest(
    const Partition& partition, const Replica& pr, Replica& dest_replica, Replica& dest)
{
  int ret = OB_SUCCESS;
  TenantBalanceStat& ts = *tenant_stat_;
  bool find = false;
  for (int64_t i = 0; i < ts.all_zone_unit_.count() && OB_SUCC(ret) && !find; i++) {
    const ZoneUnit& zu = ts.all_zone_unit_.at(i);
    if (zu.zone_ != pr.zone_) {
      // nothing todo
    } else {
      for (int64_t j = 0; j < zu.all_unit_.count() && OB_SUCC(ret); j++) {
        UnitStat* us = zu.all_unit_.at(j);
        if (!us->server_->can_migrate_in() ||
            (REPLICA_TYPE_LOGONLY == us->info_.unit_.replica_type_ &&
                pr.unit_->info_.unit_.replica_type_ != us->info_.unit_.replica_type_)) {
          // nothing todo
        } else {
          find = true;
          dest.unit_ = us;
          dest.server_ = us->server_;
          dest.zone_ = pr.zone_;
          dest.region_ = pr.region_;
          dest_replica = dest;
        }
      }
    }
  }
  if (OB_SUCC(ret) && !find) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("fail to find dest for replica", K(partition), K(pr));
  }
  return ret;
}

int ObSingleZoneModeMigrateReplica::do_migrate_replica(const Partition& partition, const Replica& replica,
    const Replica& dest_replica, const Partition*& first_migrate_p, ObReplicaMember& data_source, bool& small_tenant,
    common::ObIArray<ObMigrateTaskInfo>& task_info_array, int64_t& task_cnt, bool& do_accumulated)
{
  int ret = OB_SUCCESS;
  TenantBalanceStat& ts = *tenant_stat_;
  if (!ts.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid tenant stat", K(ret));
  } else if (partition.is_in_spliting()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition is in spliting", K(ret));
  } else if (replica.is_in_blacklist(
                 ObRebalanceTaskType::MIGRATE_REPLICA, dest_replica.server_->server_, tenant_stat_)) {
    ret = OB_REBALANCE_TASK_CANT_EXEC;
    LOG_INFO("replica in black list, wait", K(replica), K(dest_replica));
  } else {
    ObMigrateTaskInfo task_info;
    ObPartitionKey key;
    ObReplicaMember src = ObReplicaMember(
        replica.server_->server_, replica.member_time_us_, replica.replica_type_, replica.get_memstore_percent());
    const char* comment = balancer::SINGLE_ZONE_STOP_SERVER;
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
    data_source = src;
    if (OB_FAIL(ts.gen_partition_key(partition, key))) {
      LOG_WARN("fail to gen partition key", K(ret), K(partition));
    } else if (OB_FAIL(ts.get_migrate_replica_quorum_size(partition, quorum))) {
      LOG_WARN("fail to get quorum size", K(ret));
    } else if (FALSE_IT(task_info.set_transmit_data_size(transmit_data_size))) {
      // nothing todo
    } else if (OB_FAIL(task_info.build(obrpc::MigrateMode::MT_SINGLE_ZONE_MODE,
                   dst,
                   key,
                   src,
                   data_source,
                   quorum,
                   replica.unit_->info_.unit_.is_manual_migrate()))) {
      LOG_WARN("fail to build migrate task info", K(ret));
    } else if (OB_FAIL(try_accumulate_task_info(small_tenant,
                   task_info_array,
                   task_info,
                   &partition,
                   first_migrate_p,
                   dest_replica,
                   task_cnt,
                   do_accumulated))) {
      LOG_WARN("fail to accumulate task info", K(ret));
    } else {
    }  // do not accumulate, go on for next replica in this partition
  }
  return ret;
}

int ObSingleZoneModeMigrateReplica::try_accumulate_task_info(const bool small_tenant,
    common::ObIArray<ObMigrateTaskInfo>& task_info_array, const ObMigrateTaskInfo& task_info,
    const Partition* cur_partition, const Partition*& first_migrate_p, const Replica& dest_replica_migrate,
    int64_t& task_cnt, bool& do_accumulated)
{
  int ret = OB_SUCCESS;
  ObMigrateReplicaTask task;
  TenantBalanceStat& ts = *tenant_stat_;
  if (OB_UNLIKELY(NULL == cur_partition)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(cur_partition));
  } else if (NULL == first_migrate_p) {
    // first_migrate_ is new
    first_migrate_p = cur_partition;
    if (OB_FAIL(task_info_array.push_back(task_info))) {
      LOG_WARN("fail to push back", K(ret));
    } else {
      do_accumulated = true;
    }
  } else {
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(first_migrate_p) || OB_ISNULL(cur_partition)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("first or cur partition is null", K(ret), K(first_migrate_p), K(cur_partition));
      } else if (is_same_pg(*first_migrate_p, *cur_partition) || small_tenant) {
        // cannot do batch migration for private partition and non-private partition on standby cluster,
        // even they are in the same partition group
        if ((GCTX.is_standby_cluster() && OB_SYS_TENANT_ID != ts.tenant_id_)) {
          // not the same destination, ignore
          // not batch on standby cluster
          do_accumulated = false;
        } else {
          // the same destination,push the task info back to the array task
          if (OB_FAIL(task_info_array.push_back(task_info))) {
            LOG_WARN("fail to push back", K(ret));
          } else {
            do_accumulated = true;
          }
        }
      } else {
        // partition not in the same partition group, first execute the previous task,
        // then accumulate
        if (OB_FAIL(task.build(obrpc::MigrateMode::MT_SINGLE_ZONE_MODE,
                task_info_array,
                dest_replica_migrate.server_->server_,
                ObRebalanceTaskPriority::LOW_PRI,
                (dest_replica_migrate.unit_->info_.unit_.is_manual_migrate() ? balancer::MANUAL_MIGRATE_UNIT
                                                                             : balancer::MIGRATE_TO_NEW_SERVER)))) {
          LOG_WARN("fail to build migrate task", K(ret));
        } else if (OB_FAIL(task_mgr_->add_task(task, task_cnt))) {
          LOG_WARN("fail to add task", K(ret));
        } else {
          task_info_array.reset();
          if (OB_FAIL(task_info_array.push_back(task_info))) {
            LOG_WARN("fail to push back", K(ret));
          } else {
            first_migrate_p = cur_partition;
            do_accumulated = true;
          }
        }
      }
    }
  }
  return ret;
}
