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

#define USING_LOG_PREFIX RS_LB
#include "ob_partition_balancer.h"
#include "ob_balance_info.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_array_iterator.h"
#include "lib/container/ob_se_array.h"
#include "lib/container/ob_se_array_iterator.h"
#include "share/partition_table/ob_partition_table_operator.h"
#include "share/config/ob_server_config.h"
#include "share/ob_multi_cluster_util.h"
#include "ob_rebalance_task.h"
#include "ob_rebalance_task_mgr.h"
#include "ob_root_utils.h"
#include "rootserver/ob_unit_load_history_table_operator.h"
#include "rootserver/ob_zone_manager.h"
#include "rootserver/ob_partition_disk_balancer.h"
#include "rootserver/ob_balance_group_container.h"
#include "rootserver/ob_balance_group_data.h"
#include "observer/ob_server_struct.h"
using namespace oceanbase::common;
using namespace oceanbase::rootserver;
using namespace oceanbase::share;
using namespace oceanbase::rootserver::balancer;

int ObPartitionUnitProvider::find_unit(int64_t all_tg_idx, int64_t part_idx, uint64_t& unit_id) const
{
  int ret = OB_SUCCESS;
  const ObZone& zone = zone_unit_.zone_;
  if (OB_FAIL(stat_finder_.get_primary_partition_unit(zone, all_tg_idx, part_idx, unit_id))) {
    LOG_WARN("fail find unit id", K(zone), K(all_tg_idx), K(part_idx), K(ret));
  }
  return ret;
}

int64_t ObPartitionUnitProvider::count() const
{
  return zone_unit_.all_unit_.count();
}

uint64_t ObPartitionUnitProvider::get_unit_id(int64_t unit_idx) const
{
  uint64_t unit_id = OB_INVALID_ID;
  if (unit_idx >= 0 && unit_idx < count()) {
    UnitStat* us = zone_unit_.all_unit_.at(unit_idx);
    if (OB_ISNULL(us)) {
      LOG_WARN("unexpected. unit stat should not be null", K(unit_idx), K(*us));
    } else {
      unit_id = us->info_.unit_.unit_id_;
    }
  }
  return unit_id;
}

int ObPartitionUnitProvider::get_units(ObIArray<UnitStat*>& unit_stat) const
{
  int ret = OB_SUCCESS;
  ARRAY_FOREACH_X(zone_unit_.all_unit_, idx, cnt, OB_SUCC(ret))
  {
    UnitStat* us = zone_unit_.all_unit_.at(idx);
    if (OB_ISNULL(us)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected. unit stat should not be null", K(idx), K(*us));
    } else {
      if (OB_FAIL(unit_stat.push_back(us))) {
        LOG_WARN("fail push back unit id", K(idx), K(cnt), K(ret));
      }
    }
  }
  return ret;
}

int ObPartitionUnitProvider::get_unit_by_id(const uint64_t unit_id, UnitStat*& unit_stat) const
{
  int ret = OB_SUCCESS;
  bool find = false;
  if (OB_INVALID_ID == unit_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid unit id", K(ret));
  } else {
    ARRAY_FOREACH_X(zone_unit_.all_unit_, idx, cnt, OB_SUCC(ret) && !find)
    {
      UnitStat* us = zone_unit_.all_unit_.at(idx);
      if (OB_ISNULL(us)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected. unit stat should not be null", K(idx), K(*us));
      } else if (unit_id == us->get_unit_id()) {
        unit_stat = us;
        find = true;
      } else {
      }  // go on next
    }
    if (OB_FAIL(ret)) {
    } else if (!find) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("unit not exist here", K(ret), K(unit_id));
    } else {
    }  // good
  }
  return ret;
}

int ObPartitionUnitProvider::get_avg_load(double& avg_load) const
{
  int ret = OB_SUCCESS;
  avg_load = zone_unit_.get_avg_load();
  return ret;
}

ObPartitionBalancer::ObPartitionBalancer()
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

int ObPartitionBalancer::init(common::ObServerConfig& cfg, share::schema::ObMultiVersionSchemaService& schema_service,
    share::ObPartitionTableOperator& pt_operator, ObRebalanceTaskMgr& task_mgr, ObZoneManager& zone_mgr,
    TenantBalanceStat& tenant_stat, share::ObCheckStopProvider& check_stop_provider)
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

// Make replicas of the same table group but different partition groups spread evenly across all units as much as
// possible
int ObPartitionBalancer::partition_balance(
    int64_t& task_cnt, const balancer::HashIndexCollection& hash_index_collection)
{
  int ret = OB_SUCCESS;
  TenantBalanceStat& origin_ts = *origin_tenant_stat_;
  TenantBalanceStat ts;
  uint64_t tenant_id = origin_ts.tenant_id_;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!origin_ts.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "tenant stat valid", origin_ts.is_valid());
  } else if (OB_ISNULL(origin_ts.schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema guard should not be NULL if origin_ts.is_valid true", K(ret));
  } else if (OB_FAIL(origin_ts.gather_filter_stat(ts, hash_index_collection))) {
    LOG_WARN("fail to gather stat", K(ret));
  } else {
    tenant_stat_ = &ts;
  }

  FOREACH_X(zu, ts.all_zone_unit_, OB_SUCCESS == ret)
  {
    if (zu->active_unit_cnt_ <= 0) {
      continue;
    }
    ObPartitionUnitProvider unit_provider(*zu, ts);
    balancer::ITenantStatFinder& stat_finder = ts;
    common::ObArenaAllocator allocator(ObModIds::OB_RS_PARTITION_BALANCER);
    ObPartitionBalanceGroupContainer balance_group_container(*ts.schema_guard_, stat_finder, allocator);
    bool skip_disk_balance_if_count_balanced = false;
    if (OB_FAIL(balance_group_container.init(tenant_id))) {
      LOG_WARN("fail get all maps from collection", K(ret), K(tenant_id));
    } else if (OB_FAIL(balance_group_container.build())) {
      LOG_WARN("fail to build balance group container", K(ret));
    } else if (OB_FAIL(balance_group_container.calc_leader_balance_statistic(zu->zone_))) {
      LOG_WARN("fail to calc leader balance statistic", K(ret));
    }
    common::ObIArray<SquareIdMap*>& maps = balance_group_container.get_square_id_map_array();
    ARRAY_FOREACH_X(maps, idx, cnt, OB_SUCC(ret))
    {
      balancer::SquareIdMap& map = *maps.at(idx);
      balancer::PartitionLeaderCountBalancer balancer(
          map, unit_provider, zu->zone_, *ts.schema_guard_, stat_finder, balance_group_container.get_hash_index());
      if (!map.is_valid()) {
        LOG_WARN("map is invalid and can not do balance. check detail info by query inner table",
            "inner_table",
            OB_ALL_VIRTUAL_REBALANCE_MAP_STAT_TNAME,
            K(map),
            K(ret));
      } else if (OB_FAIL(balancer.balance())) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          LOG_INFO("unit not determined for some replica. ignore balance map for now", K(map), K(ret));
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail balance map by count", K(map), K(ret));
        }
      } else if (map.need_balance()) {
        // When the number of maps is not balanced,
        // skip the disk balancing for the maps whose numbers are already balanced
        skip_disk_balance_if_count_balanced = true;
      }
    }
    ARRAY_FOREACH_X(maps, idx, cnt, OB_SUCC(ret))
    {
      balancer::SquareIdMap& map = *maps.at(idx);
      bool count_balanced = !map.need_balance();
      if (skip_disk_balance_if_count_balanced && count_balanced) {
        continue;
      }
      balancer::DynamicAverageDiskBalancer balancer(
          map, stat_finder, unit_provider, zu->zone_, *ts.schema_guard_, balance_group_container.get_hash_index());
      if (!map.is_valid()) {
        LOG_WARN("map is invalid and can not do balance. check detail info by query inner table",
            "inner_table",
            OB_ALL_VIRTUAL_REBALANCE_MAP_STAT_TNAME,
            K(map),
            K(ret));
      } else if (OB_FAIL(balancer.balance())) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          LOG_INFO("unit not determined for some replica. ignore balance map for now", K(map), K(ret));
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail balance map", K(map), K(ret));
        }
      } else {

        map.dump2("dump balance result");

        // map balanced
        // Traverse the map item and calculate the group based on the item
        FOREACH_X(item, map, OB_SUCC(ret))
        {
          uint64_t src_unit_id = item->unit_id_;
          uint64_t dst_unit_id = item->dest_unit_id_;
          UnitStatMap::Item* s = NULL;
          UnitStatMap::Item* d = NULL;
          UnitStat* src = NULL;
          UnitStat* dest = NULL;
          if (src_unit_id == dst_unit_id) {
            continue;
          } else if (OB_FAIL(ts.unit_stat_map_.locate(src_unit_id, s))) {
            LOG_WARN("locate unit stat failed", K(ret), K(src_unit_id));
          } else if (OB_FAIL(ts.unit_stat_map_.locate(dst_unit_id, d))) {
            LOG_WARN("locate unit stat failed", K(ret), K(dst_unit_id));
          } else if (NULL == s || NULL == d) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("null unit stat", K(src_unit_id), KP(s), K(dst_unit_id), K(d), K(ret));
          } else {
            src = &s->v_;
            dest = &d->v_;
            LOG_INFO("plan to migrate pg", "from", src_unit_id, "to", dst_unit_id);
          }
          if (OB_SUCC(ret)) {
            int64_t tg_idx = item->all_tg_idx_;
            if (tg_idx < 0 || tg_idx >= ts.all_tg_.count()) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected tg_idx value", K(tg_idx), "max", ts.all_tg_.count(), K(ret));
            } else if (!src->server_->can_migrate_out() || !dest->server_->can_migrate_in()) {
              LOG_WARN("skip migrate, may source can;t migrate out or dest can't migrate in");
              // server cannot move in/out temporarily, skip
              // Note: When generating the map,
              // it does not depend on whether the server where the unit is located can move in and out,
              // the algorithm is correct
            } else {
              TableGroup& tg = ts.all_tg_.at(tg_idx);
              FOR_BEGIN_END_E(pg, tg, ts.all_pg_, OB_SUCC(ret))
              {
                if (pg->partition_idx_ == item->part_idx_ && can_migrate_pg_by_rule(*pg, src, dest)) {
                  if (OB_FAIL(migrate_pg(*pg, src, dest, task_cnt, map.get_comment()))) {
                    LOG_WARN("migrate partition group failed", K(ret));
                  }
                }
              }
            }
          }
        }  // for each item in map, try migrate pg
      }
    }
  }
  return ret;
}

// Determine whether pg can be migrated from src to dest;
// Judge through primary_partition;
// As long as the primary_partition can generate the task of migrating from src to dest,
// it is considered that the entire pg can be migrated;
// Because pg coordinate will try its best to align the replica of the entire pg with the primary partition
// In addition, if the migrated replica is F,
// it needs to be able to find another F replica to prevent the situation where the master cannot be cut.
bool ObPartitionBalancer::can_migrate_pg_by_rule(const PartitionGroup& pg, UnitStat* src, UnitStat* dest)
{
  bool bret = false;
  int ret = OB_SUCCESS;
  TenantBalanceStat& ts = *tenant_stat_;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!ts.is_valid() || OB_INVALID_ID == pg.table_id_ || NULL == src || NULL == dest) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "tenant stat valid", ts.is_valid(), K(pg), KP(src), KP(dest));
  } else if (OB_FAIL(ObCanMigratePartitionChecker::can_migrate_out(*zone_mgr_, *src, bret))) {
    LOG_WARN("fail check can migrate partition out of max load unit", K(ret));
  }

  FOR_BEGIN_END_E(p, pg, ts.sorted_partition_, OB_SUCCESS == ret && bret)
  {
    if (!(*p)->is_primary()) {
    } else {
      if (!(*p)->can_balance()) {
        bret = false;
        LOG_INFO("partition is in split, can not balance", "partition", *(*p));
      } else if (!ts.has_leader_while_member_change(*(*p), src->info_.unit_.server_)) {
        bret = false;
        LOG_WARN("partition will no leader in migrating", "partition", *(*p));
      } else if (ts.has_replica(*(*p), dest->info_.unit_.server_)) {
        bret = false;
        LOG_DEBUG("partition already exist in dest unit", K(pg), "replica", *(*p), "dest", dest->info_.unit_.server_);
      } else {
        bret = true;
      }
      break;
    }
  }
  return bret;
}

int ObPartitionBalancer::migrate_pg(
    const PartitionGroup& pg, UnitStat* src, UnitStat* dest, int64_t& task_cnt, const char* comment)
{
  int ret = OB_SUCCESS;
  TenantBalanceStat& ts = *tenant_stat_;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!ts.is_valid() || OB_INVALID_ID == pg.table_id_ || NULL == src || NULL == dest || OB_ISNULL(comment)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "tenant stat valid", ts.is_valid(), K(pg), KP(src), KP(dest), K(comment));
  }
  ObMigrateReplicaTask task;
  common::ObArray<ObMigrateTaskInfo> task_info_array;
  common::ObAddr hint_data_src;
  FOR_BEGIN_END_E(p, pg, ts.sorted_partition_, OB_SUCCESS == ret)
  {
    if (!(*p)->is_valid_quorum()) {
      // The quorum of partition was introduced in 1.4.7.1.
      // It depends on the report and may be an illegal value.
      // skip it first to avoid affecting the generation of subsequent tasks.
      LOG_WARN("quorum not report yet, just skip", K(ret), K(*p));
      continue;
    }
    FOR_BEGIN_END(r, *(*p), ts.all_replica_)
    {
      if (!r->is_in_service()) {
        continue;
      }
      if (r->need_skip_pg_balance()) {
        continue;
        // In the case of R@all, the R in pg does not need to be migrated
      }
      if (r->is_in_blacklist(ObRebalanceTaskType::MIGRATE_REPLICA, dest->info_.unit_.server_, tenant_stat_)) {
        LOG_WARN("migrate task frequent failed, need wait");
        continue;
      }

      if (r->unit_ == src) {  // FIXME : check unit_id?
        if (dest->info_.unit_.server_ == r->server_->server_) {
          if (OB_FAIL(pt_operator_->set_unit_id(
                  (*p)->table_id_, (*p)->partition_id_, r->server_->server_, dest->info_.unit_.unit_id_))) {
            LOG_WARN("set replica unit id failed",
                K(ret),
                "partition",
                *(*p),
                "server",
                r->server_->server_,
                "unit_id",
                dest->info_.unit_.unit_id_);
          }
        } else {
          ObMigrateTaskInfo task_info;
          ObPartitionKey key;
          ObReplicaMember data_src;
          ObReplicaMember src =
              ObReplicaMember(r->server_->server_, r->member_time_us_, r->replica_type_, r->get_memstore_percent());
          OnlineReplica dst;
          dst.member_ = ObReplicaMember(
              dest->info_.unit_.server_, ObTimeUtility::current_time(), r->replica_type_, r->get_memstore_percent());
          dst.unit_id_ = dest->info_.unit_.unit_id_;
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
                         cluster_id,
                         (hint_data_src.is_valid() ? &hint_data_src : NULL)))) {
            LOG_WARN("set task src failed", K(ret), K(task));
          } else if (OB_FAIL(ts.get_migrate_replica_quorum_size(*(*p), quorum))) {
            LOG_WARN("fail to get quorum size", K(ret), "partition", *(*p));
          } else if (FALSE_IT(task_info.set_transmit_data_size(transmit_data_size))) {
            // nothint todo
          } else if (OB_FAIL(task_info.build(migrate_mode, dst, key, src, data_src, quorum, is_restore))) {
            LOG_WARN("fail to build migrate task info", K(ret));
          } else if (OB_FAIL(task_info_array.push_back(task_info))) {
            LOG_WARN("fail to push back", K(ret));
          } else {
          }  // do nothing
          if (OB_SUCC(ret)) {
            r->unit_ = dest;
            hint_data_src = data_src.get_server();
          }
        }
        break;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(check_stop())) {
        LOG_WARN("balancer stop", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && task_info_array.count() > 0) {
    const uint64_t table_id = task_info_array.at(0).get_partition_key().get_table_id();
    const obrpc::MigrateMode migrate_mode = obrpc::MigrateMode::MT_LOCAL_FS_MODE;
    if (OB_FAIL(task.build(
            migrate_mode, task_info_array, dest->info_.unit_.server_, ObRebalanceTaskPriority::LOW_PRI, comment))) {
      LOG_WARN("fail to build migrate task", K(ret));
    } else if (OB_FAIL(task_mgr_->add_task(task, task_cnt))) {
      LOG_WARN("fail to add task", K(ret));
    } else {
    }  // no more to do
  }
  if (OB_SUCC(ret)) {
    ZoneUnit* zu = NULL;
    if (OB_FAIL(ts.locate_zone(src->info_.unit_.zone_, zu))) {
      LOG_WARN("fail locate zone", K(ret), "unit", src->info_.unit_);
    } else {
      src->load_ = src->calc_load(zu->resource_weight_, src->load_factor_);
      src->tg_pg_cnt_ -= 1;

      dest->load_ = dest->calc_load(zu->resource_weight_, dest->load_factor_);
      dest->tg_pg_cnt_ += 1;
    }
  }
  return ret;
}
