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
#include "ob_unit_balancer.h"

#include "lib/container/ob_array.h"
#include "lib/container/ob_array_iterator.h"
#include "lib/container/ob_se_array.h"
#include "lib/container/ob_se_array_iterator.h"
#include "share/partition_table/ob_partition_table_operator.h"
#include "share/config/ob_server_config.h"
#include "share/ob_multi_cluster_util.h"
#include "ob_rebalance_task_mgr.h"
#include "ob_root_utils.h"
#include "rootserver/ob_balance_group_data.h"
#include "rootserver/ob_zone_manager.h"

using namespace oceanbase::common;
using namespace oceanbase::rootserver;
using namespace oceanbase::share;
using namespace oceanbase::obrpc;

ObUnitBalancer::ObUnitBalancer()
    : inited_(false),
      disable_random_behavior_(false),
      config_(NULL),
      pt_operator_(NULL),
      task_mgr_(NULL),
      zone_mgr_(NULL),
      tenant_stat_(NULL),
      origin_tenant_stat_(NULL),
      check_stop_provider_(NULL)
{}

int ObUnitBalancer::init(common::ObServerConfig& cfg, share::ObPartitionTableOperator& pt_operator,
    ObRebalanceTaskMgr& task_mgr, ObZoneManager& zone_mgr, TenantBalanceStat& tenant_stat,
    share::ObCheckStopProvider& check_stop_provider)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
  } else {
    config_ = &cfg;
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

// On the basis of partition_group_balance,
// according to the load of the unit where the copy is located
// Balance the load on the unit by migrating the partition group
int ObUnitBalancer::unit_load_balance_v2(int64_t& task_cnt, const balancer::HashIndexCollection& hash_index_collection,
    const common::hash::ObHashSet<uint64_t>& processed_tids)
{
  int ret = OB_SUCCESS;
  TenantBalanceStat& origin_ts = *origin_tenant_stat_;
  TenantBalanceStat ts;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!origin_ts.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "tenant stat valid", origin_ts.is_valid());
  } else if (OB_FAIL(origin_ts.gather_filter_stat(ts, hash_index_collection))) {
    LOG_WARN("fail to gather filter stat", K(ret));
  } else {
    tenant_stat_ = &ts;
  }

  // Do load balance zone by zone
  ObZone zone;  // the zone balanced in this round
  double tolerance = static_cast<double>(config_->balancer_tolerance_percentage) / 100;
  int64_t task_cnt_old = task_cnt;
  // Although the first zone is not balanced, it may not be balanced anymore,
  // need to try the next zone
  const int64_t idx = ObConfigPartitionBalanceStrategyFuncChecker::DISK_UTILIZATION_ONLY;
  const bool disk_only_balance_strategy =
      (0 == ObString::make_string(GCONF._partition_balance_strategy)
                .case_compare(ObConfigPartitionBalanceStrategyFuncChecker::balance_strategy[idx]));
  FOREACH_X(
      zu, ts.all_zone_unit_, OB_SUCC(ret) && task_cnt == task_cnt_old && config_->balancer_tolerance_percentage < 100)
  {
    UnitStat* max_u = NULL;
    UnitStat* min_u = NULL;
    bool need_balance = false;
    if (OB_SUCC(ret) && zu->active_unit_cnt_ > 0) {
      FOREACH(u, zu->all_unit_)
      {
        if (NULL == max_u && (*u)->server_->can_migrate_out()) {
          max_u = *u;
        }
        if (NULL == min_u && (*u)->server_->can_migrate_in()) {
          min_u = *u;
        }

        if (NULL != max_u && (*u)->get_load() > max_u->get_load() && (*u)->server_->can_migrate_out()) {
          max_u = *u;
        } else if (NULL != min_u && (*u)->get_load() < min_u->get_load() && (*u)->server_->can_migrate_in()) {
          min_u = *u;
        }
        LOG_DEBUG("find max-min unit, current unit info: ",
            "zone",
            zu->zone_,
            "cur_unit_id",
            (*u)->get_unit_id(),
            "can_in",
            (*u)->server_->can_migrate_in(),
            "can_out",
            (*u)->server_->can_migrate_out(),
            KP(*u),
            "load",
            (*u)->get_load());
      }

      // If the load of a unit in a certain zone deviates too much from the mean of the zone,
      // it needs to be balanced
      if (NULL == max_u || NULL == min_u) {
        LOG_DEBUG("can not find avaliable migrate in/out unit", KP(max_u), KP(min_u));
      } else if (min_u == max_u) {
        LOG_DEBUG("can not find two different migrate in/out unit",
            KP(max_u),
            KP(min_u),
            "server",
            max_u->server_->server_,
            "zone",
            zu->zone_);
      } else {
        LOG_INFO("found max-min unit",
            "zone",
            zu->zone_,
            "max_unit_id",
            max_u->get_unit_id(),
            "min_unit_id",
            min_u->get_unit_id(),
            "max_load",
            max_u->get_load(),
            "min_load",
            min_u->get_load(),
            "avg_load",
            zu->get_avg_load(),
            K(tolerance));
        if (max_u->get_load() > zu->get_avg_load() + tolerance || min_u->get_load() < zu->get_avg_load() - tolerance) {
          need_balance = true;
          zone = zu->zone_;
          LOG_INFO("zone need balance", "tenant_id", ts.tenant_id_, KP(max_u), KP(min_u), K(zone));
        }
      }
    }
    // Migrate pg from max_u to min_u
    if (OB_SUCC(ret) && need_balance) {

      if (OB_FAIL(ts.gather_unit_balance_stat(*zu, *max_u))) {
        LOG_WARN("fail gather unit balance stat for max load unit", K(*max_u), K(ret));
      }
      // The smallest unit of migration is partition group, traverse sorted_pg sequentially
      FOREACH_X(pg, ts.sorted_pg_, OB_SUCCESS == ret)
      {
        if (OB_FAIL(check_stop())) {
          LOG_WARN("balancer stop", K(ret));
          break;
        }
        // Migrate at most ONCE_ADD_TASK_CNT pg in one round
        if (task_cnt > ObRebalanceTaskMgr::ONCE_ADD_TASK_CNT) {
          break;
        }

        if (combine_id(OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID) == pg->table_id_) {
          continue;
        }

        if (combine_id(ts.get_tenant_id(), OB_SYS_TABLEGROUP_ID) == pg->tablegroup_id_) {
          continue;
        }

        if (!ts.sorted_partition_.at(pg->begin_)->can_balance()) {
          LOG_WARN("partition can't do unit load balance", "partition", *(ts.sorted_partition_.at(pg->begin_)));
          continue;
        }
        // Update the tg_pg_cnt_ and schema_tg_pg_cnt_ values of the current pg corresponding
        // to zu for the next calculation
        if (OB_FAIL(ts.update_tg_pg_stat(pg->begin_))) {
          LOG_WARN("update tenant group stat failed", K(ret));
          break;
        }

        if (OB_SUCC(ret)) {
          print_pg_distribution();
          int tmp_ret = processed_tids.exist_refactored(pg->table_id_);
          if (OB_HASH_NOT_EXIST == tmp_ret) {
            // good, already processed
          } else {
            // HASH EXIST or other mistake, ignore this pg
            continue;
          }
        }

        FOR_BEGIN_END_E(r, *ts.sorted_partition_.at(pg->begin_), ts.all_replica_, OB_SUCC(ret))
        {
          if (r->unit_ == max_u && r->is_in_service() && r->zone_ == zu->zone_ && r->server_->can_migrate_out()) {
            if (zu->get_pg_count() <= 1 || disk_only_balance_strategy) {
              if (can_migrate_pg_by_rule(*pg, max_u, min_u) && can_migrate_pg_by_load(*zu, *max_u, *min_u, *pg)) {
                double src_load = max_u->get_load();
                double dest_load = min_u->get_load();
                if (OB_FAIL(migrate_pg(*pg, max_u, min_u, task_cnt, balancer::UNIT_PG_BALANCE))) {
                  LOG_WARN("migrate partition group failed", K(ret));
                } else {
                  LOG_INFO("unit load balance. migrate pg",
                      "zone",
                      zone,
                      "zu_pg_count",
                      zu->get_pg_count(),
                      "tablegroup_id",
                      pg->tablegroup_id_,
                      "leading_table_id",
                      pg->table_id_,
                      "partition_idx",
                      pg->partition_idx_,
                      "src_unit",
                      max_u->info_.unit_.unit_id_,
                      "dest_unit",
                      min_u->info_.unit_.unit_id_,
                      "unit_avg_load",
                      zu->get_avg_load(),
                      "tolerance",
                      tolerance,
                      "weight",
                      zu->resource_weight_,
                      "old_src_unit_load",
                      src_load,
                      "new_src_unit_load",
                      max_u->get_load(),
                      "old_dest_unit_load",
                      dest_load,
                      "new_dest_unit_load",
                      min_u->get_load());
                }
              }
            }
          }
          if (r->unit_ == max_u) {
            break;  // already found pg in max_u
          }
        }  // end loop pg replicas
      }    // end loop all_tg
    }      // need balance
  }        // end loop all_zone_unit
  return ret;
}

bool ObUnitBalancer::can_migrate_pg_by_load(ZoneUnit& zu, UnitStat& max_u, UnitStat& min_u, PartitionGroup& pg)
{
  bool fesible = true;

  double tolerance = static_cast<double>(config_->balancer_tolerance_percentage) / 100;

  if (config_->balancer_tolerance_percentage >= 100) {
    fesible = false;
  }

  if (fesible) {
    if (max_u.get_load() < zu.get_avg_load() + tolerance) {
      fesible = false;
    }
  }

  // PARTITION FILTER
  if (fesible) {
    if (pg.load_factor_.get_weighted_sum(zu.resource_weight_) <= 0) {
      fesible = false;
    }
  }

  // TODO:
  // Add more strategies to determine whether it is appropriate to migrate this pg
  //

  if (fesible) {
    double max_load = max_u.get_load_if_minus(zu.resource_weight_, pg.load_factor_);
    double min_load = min_u.get_load_if_plus(zu.resource_weight_, pg.load_factor_);
    if (max_load < zu.get_avg_load()) {
      fesible = false;
    } else if (min_load >= zu.get_avg_load() + tolerance) {
      fesible = false;
    }
  }

  return fesible;
}

int ObUnitBalancer::print_pg_distribution()
{
  int ret = OB_SUCCESS;
#if 0
  TenantBalanceStat &ts = *tenant_stat_;
  FOREACH_X(zu, ts.all_zone_unit_, OB_SUCC(ret)) {
    if (zu->active_unit_cnt_ <= 0) {
      continue;
    }
    ObArray<int64_t> unit_array; // unit id
    ObArray<int64_t> cnt_array; // Corresponding to the number of pg on each unit 
    const int64_t avg_pg_cnt_lower = zu->get_pg_count() / zu->active_unit_cnt_;
    const int64_t avg_pg_cnt_upper = avg_pg_cnt_lower +
        (zu->get_pg_count() % zu->active_unit_cnt_ == 0 ? 0 : 1);
    FOREACH_X(u, zu->all_unit_, OB_SUCC(ret)) {
      if (OB_FAIL(unit_array.push_back((*u)->info_.unit_.unit_id_))) {
        LOG_WARN("fail push back", K(ret));
      } else if (OB_FAIL(cnt_array.push_back((*u)->tg_pg_cnt_))) {
        LOG_WARN("fail push back", K(ret));
      }
    }
    LOG_INFO("zone pg distribution info",
             "zone", zu->zone_,
             "units", unit_array,
             "pg_count", cnt_array,
             K(avg_pg_cnt_lower),
             K(avg_pg_cnt_upper));
  }
#endif
  return ret;
}

// Determine whether pg can be migrated from src to dest;
// Judge by primary_partition;
// as long as primary_partition can generate tasks to migrate from src to dest,
// it is considered that the entire pg can be migrated;
// Because pg coordinate will try its best to align the replica of the entire pg with the primary partition;
// In addition, if the migrated replica is F,
// it needs to be able to find another F replica to prevent the situation where the master cannot be cut.
bool ObUnitBalancer::can_migrate_pg_by_rule(const PartitionGroup& pg, UnitStat* src, UnitStat* dest)
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
      if (!ts.has_leader_while_member_change(*(*p), src->info_.unit_.server_)) {
        bret = false;
        LOG_WARN("partition will no leader in migrating", "partition", *(*p));
      } else if (ts.has_replica(*(*p), dest->info_.unit_.server_)) {
        bret = false;
        LOG_WARN("can not migrate partition group",
            K(pg),
            "partition",
            *(*p),
            "src_unit",
            src->info_.unit_,
            "dest_unit",
            dest->info_.unit_);
      } else {
        bret = true;
      }
      break;
    }
  }
  return bret;
}

int ObUnitBalancer::migrate_pg(
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
      LOG_WARN("quorum is invalid, maybe not report yet", K(ret), KPC(*p));
      continue;
    }
    FOR_BEGIN_END(r, *(*p), ts.all_replica_)
    {
      if (!r->is_in_service()) {
        continue;
      }
      if (r->is_in_blacklist(ObRebalanceTaskType::MIGRATE_REPLICA, dest->info_.unit_.server_, tenant_stat_)) {
        LOG_WARN("the replica task in black list, choose another replica");
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
          const obrpc::MigrateMode migrate_mode = obrpc::MigrateMode::MT_LOCAL_FS_MODE;
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
          const ObClusterType cluster_type = ObClusterInfoGetter::get_cluster_type_v2();
          bool is_restore = (*p)->in_physical_restore();
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
      src->load_factor_ -= pg.load_factor_;
      src->load_ = src->calc_load(zu->resource_weight_, src->load_factor_);
      src->tg_pg_cnt_ -= 1;

      dest->load_factor_ += pg.load_factor_;
      dest->load_ = dest->calc_load(zu->resource_weight_, dest->load_factor_);
      dest->tg_pg_cnt_ += 1;
    }
  }
  return ret;
}
