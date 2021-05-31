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

#include "ob_server_balance_plan.h"
#include "share/ob_zone_info.h"
#include "rootserver/ob_zone_manager.h"
#include "observer/ob_server_struct.h"
namespace oceanbase {
using namespace common;
using namespace share;
using namespace share::schema;
namespace rootserver {

int ServerManager::get_servers_of_zone(const common::ObZone& zone, ObServerArray& server_list) const
{
  int ret = OB_SUCCESS;
  server_list.reuse();
  SpinRLockGuard guard(server_status_rwlock_);
  for (int64_t i = 0; OB_SUCC(ret) && i < server_statuses_.count(); ++i) {
    if ((server_statuses_[i].zone_ == zone || zone.is_empty())) {
      ret = server_list.push_back(server_statuses_[i].server_);
      if (OB_FAIL(ret)) {
        LOG_WARN("push back to server_list failed", K(ret));
      }
    }
  }
  return ret;
}

int ServerManager::get_server_status(const common::ObAddr& server, ObServerStatus& server_status) const
{
  int ret = OB_SUCCESS;
  if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), K(ret));
  } else {
    SpinRLockGuard guard(server_status_rwlock_);
    ObServerStatus* status_ptr = NULL;
    if (OB_FAIL(find_server_status(server, status_ptr))) {
      LOG_WARN("find failed", K(server), K(ret));
    } else if (NULL == status_ptr) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("status_ptr is null", "status_ptr", OB_P(status_ptr), K(ret));
    } else {
      server_status = *status_ptr;
    }
  }
  return ret;
}

int ServerManager::clone(ObServerManager& server_mgr)
{
  int ret = OB_SUCCESS;
  const common::ObZone zone;
  common::ObArray<share::ObServerStatus> server_statuses;
  if (OB_FAIL(server_mgr.get_server_statuses(zone, server_statuses))) {
    LOG_WARN("fail to get server statuses", K(ret));
  } else {
    server_statuses_.reset();
    SpinWLockGuard guard(server_status_rwlock_);
    if (OB_FAIL(append(server_statuses_, server_statuses))) {
      LOG_WARN("fail to append", K(ret));
    }
  }
  return ret;
}

int ServerManager::reduce_disk_use(const common::ObAddr& server, const int64_t size)
{
  int ret = OB_SUCCESS;
  if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), K(ret));
  } else {
    SpinWLockGuard guard(server_status_rwlock_);
    ObServerStatus* status_ptr = NULL;
    if (OB_FAIL(find_server_status(server, status_ptr))) {
      LOG_WARN("find failed", K(server), K(ret));
    } else if (NULL == status_ptr) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("status_ptr is null", "status_ptr", OB_P(status_ptr), K(ret));
    } else {
      status_ptr->resource_info_.disk_in_use_ -= size;
    }
  }
  return ret;
}

int ServerManager::find_server_status(const ObAddr& server, ObServerStatus*& status) const
{
  int ret = OB_SUCCESS;
  status = NULL;
  if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), K(ret));
  } else {
    bool find = false;
    for (int64_t i = 0; i < server_statuses_.count() && !find; ++i) {
      if (server_statuses_[i].server_ == server) {
        status = const_cast<ObServerStatus*>(&server_statuses_[i]);
        find = true;
      }
    }
    if (!find) {
      ret = OB_ENTRY_NOT_EXIST;
      // we print info log here, because sometime this is normal(such as add server)
      LOG_INFO("server not exist", K(server), K(ret));
    }
  }
  return ret;
}

int ServerManager::add_disk_use(const common::ObAddr& server, const int64_t size)
{
  int ret = OB_SUCCESS;
  if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), K(ret));
  } else {
    SpinWLockGuard guard(server_status_rwlock_);
    ObServerStatus* status_ptr = NULL;
    if (OB_FAIL(find_server_status(server, status_ptr))) {
      LOG_WARN("find failed", K(server), K(ret));
    } else if (NULL == status_ptr) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("status_ptr is null", "status_ptr", OB_P(status_ptr), K(ret));
    } else {
      status_ptr->resource_info_.disk_in_use_ += size;
    }
  }
  return ret;
}

int ObServerBalancePlan::init(common::ObMySQLProxy& proxy, common::ObServerConfig& server_config,
    share::schema::ObMultiVersionSchemaService& schema_service, ObRootBalancer& root_balancer)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret), K(is_inited_));
  } else if (NULL == GCTX.pt_operator_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pt operator ptr is null", K(ret));
  } else if (OB_FAIL(unit_stat_getter_.init(*GCTX.pt_operator_, schema_service, stop_checker_))) {
    LOG_WARN("fail to init unit stat getter", K(ret));
  } else if (OB_FAIL(unit_stat_mgr_.init(schema_service, fake_unit_mgr_, unit_stat_getter_))) {
    LOG_WARN("fail to init unit stat mgr", K(ret));
  } else if (OB_FAIL(fake_unit_mgr_.init(proxy, server_config, leader_coordinator_, schema_service, root_balancer))) {
    LOG_WARN("fail to init fake unit mgr", K(ret));
  } else if (OB_FAIL(server_balancer_.init(schema_service,
                 fake_unit_mgr_,
                 leader_coordinator_,
                 zone_mgr_,
                 fake_server_mgr_,
                 unit_stat_mgr_,
                 tenant_stat_))) {
    LOG_WARN("fail to init server balancer", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObServerBalancePlan::generate_server_balance_plan()
{
  int ret = OB_SUCCESS;
  int64_t zone_count = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(fake_server_mgr_.clone(server_mgr_))) {
    LOG_WARN("fail to clone", K(ret));
  } else if (OB_FAIL(clone_fake_unit_mgr())) {
    LOG_WARN("fail to clone fake unit mgr", K(ret));
  } else if (OB_FAIL(unit_stat_mgr_.gather_stat())) {
    LOG_WARN("fail to gather stat", K(ret));
  } else if (OB_FAIL(zone_mgr_.get_zone_count(zone_count))) {
    LOG_WARN("fail to get zone count", K(ret));
  } else if (zone_count <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("zone count unexpected", K(ret), K(zone_count));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < zone_count; ++i) {
      HEAP_VAR(share::ObZoneInfo, zone_info)
      {
        if (OB_FAIL(zone_mgr_.get_zone(i, zone_info))) {
          LOG_WARN("fail to get zone", K(ret));
        } else if (OB_FAIL(server_balancer_.generate_server_balance_plan(zone_info.zone_))) {
          LOG_WARN("fail to generate server balance", K(ret));
        } else {
        }  // no more to do
      }
    }
    if (OB_SUCC(ret)) {
      plan_generated_ = true;
    }
  }
  return ret;
}

int ObServerBalancePlan::clone_fake_unit_mgr()
{
  int ret = OB_SUCCESS;
  {
    SpinRLockGuard guard(unit_mgr_.get_lock());
    if (OB_FAIL(fake_unit_mgr_.load())) {
      LOG_WARN("fail to clone fake unit mgr", K(ret));
    }
  }
  return ret;
}

int ObServerBalancePlan::get_next_task(ServerBalancePlanTask& task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!plan_generated_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("balance plan not generated", K(ret));
  } else {
    ret = server_balancer_.get_next_task(task);
  }
  return ret;
}

int ObServerBalancePlan::get_next_unit_distribution(share::ObUnitInfo& unit)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!plan_generated_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("balance plan not generated", K(ret));
  } else {
    ret = server_balancer_.get_next_unit_distribution(unit);
  }
  return ret;
}

int UnitManager::migrate_unit(const uint64_t unit_id, const common::ObAddr& dst, const bool is_manual)
{
  int ret = OB_SUCCESS;
  ObUnit* unit = NULL;
  share::ObResourcePool* pool = NULL;
  UNUSED(is_manual);
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (OB_INVALID_ID == unit_id || !dst.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(unit_id), K(dst), K(ret));
  } else if (OB_FAIL(get_unit_by_id(unit_id, unit))) {
    LOG_WARN("get_unit_by_id failed", K(unit_id), K(ret));
  } else if (NULL == unit) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unit is null", KP(unit), K(ret));
  } else if (unit->server_ == dst) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("cannot migrate to original server", K(ret), "unit", *unit, K(dst));
  } else if (ObUnit::UNIT_STATUS_ACTIVE != unit->status_) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("cannot do unit balance with unit in deleting", K(ret), K(unit_id));
  } else if (unit->migrate_from_server_.is_valid()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("cannot do unit balance with unit in migrating", K(ret), K(unit_id));
  } else if (OB_FAIL(get_resource_pool_by_id(unit->resource_pool_id_, pool))) {
    LOG_WARN("get resource pool failed", K(ret), "pool_id", unit->resource_pool_id_);
  } else if (NULL == pool) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pool is null", K(ret), KP(pool));
  } else {
    ObAddr src = unit->server_;
    unit->server_ = dst;
    ObUnitLoad load;
    if (OB_FAIL(delete_unit_load(src, unit_id))) {
      LOG_WARN("delete_unit_load failed", K(src), K(unit_id), K(ret));
    } else if (OB_FAIL(gen_unit_load(unit, load))) {
      LOG_WARN("gen_unit_load failed", "unit", *unit, K(ret));
    } else if (OB_FAIL(insert_unit_load(dst, load))) {
      LOG_WARN("insert_unit_load failed", K(dst), K(ret));
    } else {
    }  // no more to do
  }
  return ret;
}

int UnitManager::end_migrate_unit(const uint64_t unit_id, const EndMigrateOp end_migrate_op)
{
  int ret = OB_SUCCESS;
  UNUSED(unit_id);
  UNUSED(end_migrate_op);
  return ret;
}

int ServerBalancer::generate_server_balance_plan(common::ObZone& zone)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(zone.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(zone));
  } else if (OB_FAIL(do_generate_server_balance_plan(zone))) {
    LOG_WARN("fail to generate server balance plan", K(ret), K(zone));
  }
  return ret;
}

int ServerBalancer::get_all_zone_units(const common::ObZone& zone, common::ObIArray<share::ObUnitInfo>& units)
{
  int ret = OB_SUCCESS;
  units.reset();
  ObArray<uint64_t> unit_ids;
  if (OB_UNLIKELY(NULL == unit_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unit mgr ptr is null", K(ret));
  } else if (OB_FAIL(unit_mgr_->get_unit_ids(unit_ids))) {
    LOG_WARN("fail to get unit ids", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < unit_ids.count(); ++i) {
      ObUnit* unit = NULL;
      share::ObUnitConfig* unit_config = NULL;
      share::ObResourcePool* pool = NULL;
      if (OB_FAIL(unit_mgr_->get_unit_by_id(unit_ids.at(i), unit))) {
        LOG_WARN("fail to get unit by id", K(ret));
      } else if (OB_UNLIKELY(NULL == unit)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit ptr is null", K(ret));
      } else if (zone != unit->zone_) {
        // pass
      } else if (OB_FAIL(unit_mgr_->get_resource_pool_by_id(unit->resource_pool_id_, pool))) {
        LOG_WARN("fail to get resource pool by id", K(ret));
      } else if (OB_UNLIKELY(NULL == pool)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pool ptr is null", K(ret), KP(pool));
      } else if (OB_FAIL(unit_mgr_->get_unit_config_by_id(pool->unit_config_id_, unit_config))) {
        LOG_WARN("fail to get unit config by id", K(ret));
      } else if (OB_UNLIKELY(NULL == unit_config)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit config ptr is null", K(ret), KP(unit_config));
      } else {
        share::ObUnitInfo unit_info;
        unit_info.unit_ = *unit;
        unit_info.config_ = *unit_config;
        if (OB_FAIL(unit_info.pool_.assign(*pool))) {
          LOG_WARN("fail to assign", K(ret));
        } else if (OB_FAIL(units.push_back(unit_info))) {
          LOG_WARN("fail to push back", K(ret));
        } else {
        }  // no more to do
      }
    }
  }
  return ret;
}

int ServerBalancer::check_generate_balance_plan_finished(
    common::ObArray<share::ObUnitInfo>& before, common::ObArray<share::ObUnitInfo>& after, bool& finish)
{
  int ret = OB_SUCCESS;
  if (before.count() != after.count()) {
    finish = false;
  } else {
    finish = true;
    ObArray<share::ObUnitInfo*> before_ptr;
    ObArray<share::ObUnitInfo*> after_ptr;
    for (int64_t i = 0; OB_SUCC(ret) && i < before.count(); ++i) {
      if (OB_FAIL(before_ptr.push_back(&before.at(i)))) {
        LOG_WARN("fail to push back", K(ret));
      } else if (after_ptr.push_back(&after.at(i))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      UnitIdCmp cmp;
      std::sort(before_ptr.begin(), before_ptr.end(), cmp);
      if (OB_FAIL(cmp.get_ret())) {
        LOG_WARN("fail to sort", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      UnitIdCmp cmp;
      std::sort(after_ptr.begin(), after_ptr.end(), cmp);
      if (OB_FAIL(cmp.get_ret())) {
        LOG_WARN("fail to sort", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && finish && i < before_ptr.count(); ++i) {
      share::ObUnit& before_ins = before_ptr.at(i)->unit_;
      share::ObUnit& after_ins = after_ptr.at(i)->unit_;
      if (before_ins.unit_id_ != after_ins.unit_id_) {
        finish = false;
      } else if (before_ins.server_ != after_ins.server_) {
        finish = false;
      } else {
      }  // go on next
    }
  }
  return ret;
}

int ServerBalancer::do_generate_server_balance_plan(const common::ObZone& zone)
{
  int ret = OB_SUCCESS;
  ObArray<share::ObUnitInfo> units_before;
  ObArray<share::ObUnitInfo> units_after;
  if (OB_UNLIKELY(zone.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(zone));
  } else if (OB_FAIL(get_all_zone_units(zone, units_before))) {
    LOG_WARN("fail to get all zone units", K(ret));
  } else if (units_before.count() <= 0) {
    // bypass
  } else {
    const int64_t max_times = units_before.count() * units_before.count();
    bool finish = false;
    for (int64_t i = 0; !finish && OB_SUCC(ret) && i < max_times; ++i) {
      if (OB_FAIL(get_all_zone_units(zone, units_before))) {
        LOG_WARN("fail to get all zone units", K(ret));
      } else if (OB_FAIL(rebalance_servers_v2(zone))) {
        LOG_WARN("fail to rebalance", K(ret));
      } else if (OB_FAIL(get_all_zone_units(zone, units_after))) {
        LOG_WARN("fail to get all zone units", K(ret));
      } else if (OB_FAIL(check_generate_balance_plan_finished(units_before, units_after, finish))) {
        LOG_WARN("fail to check generate balance plan finished", K(ret));
      } else if (finish) {
        if (OB_FAIL(append(unit_distribution_, units_before))) {
          LOG_WARN("fail to append", K(ret));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (!finish) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cannot finish in certain balance round", K(ret), K(max_times));
    } else {
      LOG_INFO("ServerBalancer generate balance finish", K(zone));
    }
  }
  return ret;
}

int ServerBalancer::get_next_task(ServerBalancePlanTask& task)
{
  int ret = OB_SUCCESS;
  if (task_iter_idx_ >= task_array_.count()) {
    ret = OB_ITER_END;
  } else {
    task = task_array_.at(task_iter_idx_);
    task_iter_idx_++;
  }
  return ret;
}

int ServerBalancer::get_next_unit_distribution(share::ObUnitInfo& unit)
{
  int ret = OB_SUCCESS;
  if (unit_iter_idx_ >= unit_distribution_.count()) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(unit.assign(unit_distribution_.at(unit_iter_idx_)))) {
    LOG_WARN("fail to assign", K(ret));
  } else {
    unit_iter_idx_++;
  }
  return ret;
}

int ServerBalancer::execute_migrate_unit(const share::ObUnit& unit, const ObServerBalancer::UnitMigrateStat& unit_task)
{
  int ret = OB_SUCCESS;
  ServerBalancePlanTask task;
  task.unit_id_ = unit.unit_id_;
  task.zone_ = unit.zone_;
  task.resource_pool_id_ = unit.resource_pool_id_;
  task.src_addr_ = unit.server_;
  task.dst_addr_ = unit_task.arranged_pos_;
  ObUnitStat unit_stat;
  if (OB_UNLIKELY(NULL == unit_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unit mgr ptr is null", K(ret));
  } else if (OB_FAIL(unit_mgr_->migrate_unit(unit.unit_id_, task.dst_addr_, false))) {
    LOG_WARN("fail to migrate unit", K(ret));
  } else if (OB_FAIL(unit_mgr_->end_migrate_unit(unit.unit_id_, ObUnitManager::COMMIT))) {
    LOG_WARN("fail to end migrate unit", K(ret));
  } else if (OB_FAIL(task_array_.push_back(task))) {
    LOG_WARN("fail to push back", K(ret));
  } else if (OB_UNLIKELY(NULL == unit_stat_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unit stat mgr ptr is null", K(ret));
  } else if (OB_FAIL(unit_stat_mgr_->get_unit_stat(unit.unit_id_, unit_stat))) {
    LOG_WARN("fail to get unit stat", K(ret));
  } else if (unit.replica_type_ == common::REPLICA_TYPE_LOGONLY) {
    // Logonly unit, disk usage does not need to be adjusted
  } else if (OB_UNLIKELY(NULL == server_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("server mgr ptr is null", K(ret));
  } else if (OB_FAIL(((ServerManager*)server_mgr_)->reduce_disk_use(task.src_addr_, unit_stat.required_size_))) {
    LOG_WARN("fail to reduce disk use", K(ret), "server", task.src_addr_);
  } else if (OB_FAIL(((ServerManager*)server_mgr_)->add_disk_use(task.dst_addr_, unit_stat.required_size_))) {
    LOG_WARN("fail to add disk use", K(ret), "server", task.dst_addr_);
  }
  return ret;
}

int ServerBalancer::do_migrate_unit_task(const common::ObIArray<ObServerBalancer::UnitMigrateStat>& unit_stat_array)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < unit_stat_array.count(); ++i) {
    const ObUnit* unit = unit_stat_array.at(i).unit_load_.unit_;
    if (OB_UNLIKELY(NULL == unit)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unit ptr is null", K(ret));
    } else if (OB_FAIL(execute_migrate_unit(*unit, unit_stat_array.at(i)))) {
      LOG_WARN("fail to execute migrate unit", K(ret));
    } else {
    }  // no more to do
  }
  return ret;
}

int ServerBalancer::do_migrate_unit_task(const common::ObIArray<ObServerBalancer::UnitMigrateStat*>& unit_stat_array)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < unit_stat_array.count(); ++i) {
    const ObServerBalancer::UnitMigrateStat* unit_stat = unit_stat_array.at(i);
    const ObUnit* unit = NULL;
    if (OB_UNLIKELY(NULL == unit_stat)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unit stat ptr is null", K(ret), KP(unit_stat));
    } else if (OB_UNLIKELY(NULL == (unit = unit_stat->unit_load_.unit_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unit ptr is null", K(ret));
    } else if (OB_FAIL(execute_migrate_unit(*unit, *unit_stat))) {
      LOG_WARN("fail to execute migrate unit in ServerBalancer", K(ret));
    } else {
    }  // no more to do
  }
  return ret;
}
}  // namespace rootserver
}  // namespace oceanbase
