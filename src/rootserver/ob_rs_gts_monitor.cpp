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

#include "ob_rs_gts_monitor.h"

#include "lib/hash_func/murmur_hash.h"
#include "common/ob_region.h"
#include "rootserver/ob_unit_manager.h"
#include "rootserver/ob_server_manager.h"
#include "rootserver/ob_zone_manager.h"
#include "rootserver/ob_rs_gts_task_mgr.h"
#include "rootserver/ob_rs_event_history_table_operator.h"
#include "share/ob_server_status.h"
#include "share/ob_rpc_struct.h"

namespace oceanbase {
using namespace common;
using namespace lib;
using namespace obrpc;
using namespace share;
namespace rootserver {

int ObRsGtsInfoCollector::init(common::ObMySQLProxy* sql_proxy)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(nullptr == sql_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(gts_table_operator_.init(sql_proxy))) {
    LOG_WARN("fail to init gts table operator", K(ret));
  } else if (OB_FAIL(unit_info_map_.create(UNIT_MAP_BUCKET_CNT, ObModIds::OB_RS_GTS_MANAGER))) {
    LOG_WARN("fail to create map", K(ret));
  } else {
    inited_ = true;
  }
  return ret;
}

void ObRsGtsInfoCollector::reuse()
{
  gts_info_array_.reset();
  unit_info_array_.reset();
  unit_info_map_.reuse();
}

int ObRsGtsInfoCollector::append_unit_info_array(
    common::ObIArray<UnitInfoStruct>& dst_array, const common::ObIArray<share::ObUnitInfo>& src_array)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < src_array.count(); ++i) {
    UnitInfoStruct info;
    if (OB_FAIL(info.assign(src_array.at(i)))) {
      LOG_WARN("fail to assign", K(ret));
    } else if (OB_FAIL(dst_array.push_back(info))) {
      LOG_WARN("fail to push back", K(ret));
    }
  }
  return ret;
}

int ObRsGtsInfoCollector::gather_stat()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    reuse();
    common::ObArray<uint64_t> pool_ids;
    const uint64_t tenant_id = OB_GTS_TENANT_ID;
    if (OB_FAIL(gts_table_operator_.get_gts_infos(gts_info_array_))) {
      LOG_WARN("fail to get gts infos", K(ret));
    } else if (OB_FAIL(unit_mgr_.get_pool_ids_of_tenant(tenant_id, pool_ids))) {
      LOG_WARN("fail to get pool ids of tenant", K(ret));
    } else {
      common::ObArray<share::ObUnitInfo> tmp_unit_info_array;
      for (int64_t i = 0; OB_SUCC(ret) && i < pool_ids.count(); ++i) {
        const uint64_t pool_id = pool_ids.at(i);
        tmp_unit_info_array.reset();
        if (OB_FAIL(unit_mgr_.get_unit_infos_of_pool(pool_id, tmp_unit_info_array))) {
          LOG_WARN("fail to get unit infos of pool", K(ret));
        } else if (OB_FAIL(ObRsGtsInfoCollector::append_unit_info_array(unit_info_array_, tmp_unit_info_array))) {
          LOG_WARN("fail to append", K(ret));
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < unit_info_array_.count(); ++i) {
        UnitInfoStruct& unit_info = unit_info_array_.at(i);
        if (unit_info.unit_.server_.is_valid()) {
          if (OB_FAIL(unit_info_map_.set_refactored(unit_info.unit_.server_, &unit_info))) {
            LOG_WARN("fail to set refactored", K(ret));
          }
        }
        if (OB_SUCC(ret) && unit_info.unit_.migrate_from_server_.is_valid()) {
          if (OB_FAIL(unit_info_map_.set_refactored(unit_info.unit_.migrate_from_server_, &unit_info))) {
            LOG_WARN("fail to set refactored", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObRsGtsInfoCollector::get_unit_info(const common::ObAddr& server, UnitInfoStruct*& unit_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(server));
  } else {
    unit_info = nullptr;
    int tmp_ret = unit_info_map_.get_refactored(server, unit_info);
    if (OB_SUCCESS == tmp_ret) {
      // good, got it
    } else if (OB_HASH_NOT_EXIST == tmp_ret) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get unit info from inner hash table", K(ret));
    }
  }
  return ret;
}

int ObRsGtsUnitDistributor::init(common::ObMySQLProxy* sql_proxy)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObGtsUnitDistributor init twice", K(ret));
  } else if (OB_UNLIKELY(nullptr == sql_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(sql_proxy));
  } else if (OB_FAIL(rs_gts_info_collector_.init(sql_proxy))) {
    LOG_WARN("fail to init rs gts info collector", K(ret));
  } else {
    sql_proxy_ = sql_proxy;
    inited_ = true;
  }
  return ret;
}

int ObRsGtsUnitDistributor::distribute_unit_for_server_status_change()
{
  int ret = OB_SUCCESS;
  rs_gts_info_collector_.reuse();
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("gts unit monitor is stopped", K(ret));
  } else if (OB_FAIL(rs_gts_info_collector_.gather_stat())) {
    LOG_WARN("fail to gather stat", K(ret));
  } else {
    LOG_INFO("distribute gts unit for server status change");
    SpinWLockGuard unit_mgr_lock_guard(unit_mgr_.get_lock());
    common::ObArray<common::ObAddr> excluded_servers;
    for (int64_t i = 0; OB_SUCC(ret) && i < rs_gts_info_collector_.get_unit_info_array().count(); ++i) {
      const share::ObUnitInfo& unit_info = rs_gts_info_collector_.get_unit_info_array().at(i);
      ObServerStatus status;
      if (OB_FAIL(check_stop())) {
        LOG_WARN("gts unit monitor is stopped", K(ret));
      } else if (ObUnit::UNIT_STATUS_ACTIVE != unit_info.unit_.status_) {
        // bypass, do not process the unit in deleting status
      } else if (OB_FAIL(server_mgr_.get_server_status(unit_info.unit_.server_, status))) {
        LOG_WARN("fail to get server status", K(ret), "server", unit_info.unit_.server_);
      } else if (status.is_active()) {
        // bypass
      } else {  // not alive or in deleting
        if (unit_info.unit_.migrate_from_server_.is_valid()) {
          // this unit is in migration, try to migrate back here
          const common::ObAddr& migrate_from_server = unit_info.unit_.migrate_from_server_;
          ObServerStatus migrate_from_status;
          if (OB_FAIL(server_mgr_.get_server_status(migrate_from_server, migrate_from_status))) {
            LOG_WARN("fail to get server status", K(ret), K(migrate_from_server));
          } else if (!migrate_from_status.is_active()) {
            // not alive or in deleting, cannot migrate back
          } else if (OB_FAIL(unit_mgr_.cancel_migrate_unit(unit_info.unit_, true /*is_gts*/))) {
            LOG_WARN("fail to migrate unit back", K(ret), "unit", unit_info.unit_);
          }
        } else {
          // the server containing gts units is not alive or in deleting. try to migrate out
          excluded_servers.reset();
          common::ObAddr dst_server;
          if (OB_FAIL(unit_mgr_.get_excluded_servers(unit_info.unit_, excluded_servers))) {
            LOG_WARN("fail to get excluded servers", K(ret), "unit", unit_info.unit_);
          } else if (OB_FAIL(unit_mgr_.choose_server_for_unit(
                         unit_info.config_, unit_info.unit_.zone_, excluded_servers, dst_server))) {
            LOG_WARN("fail to choose server for unit", K(ret));
          } else if (OB_FAIL(unit_mgr_.migrate_unit(unit_info.unit_.unit_id_, dst_server, false /* is_manual*/))) {
            LOG_WARN("fail to migrate unit", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObRsGtsUnitDistributor::unit_migrate_finish()
{
  int ret = OB_SUCCESS;
  rs_gts_info_collector_.reuse();
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("gts unit monitor is stopped", K(ret));
  } else if (OB_FAIL(rs_gts_info_collector_.gather_stat())) {
    LOG_WARN("fail to gather stat", K(ret));
  } else {
    LOG_INFO("check migrate unit finish");
    for (int64_t i = 0; OB_SUCC(ret) && i < rs_gts_info_collector_.get_gts_info_array().count(); ++i) {
      const common::ObGtsInfo& gts_info = rs_gts_info_collector_.get_gts_info_array().at(i);
      for (int64_t j = 0; OB_SUCC(ret) && j < gts_info.member_list_.get_member_number(); ++j) {
        common::ObAddr this_server;
        UnitInfoStruct* unit_info = nullptr;
        if (OB_FAIL(gts_info.member_list_.get_server_by_index(j, this_server))) {
          LOG_WARN("fail to get get info member list", K(ret));
        } else if (OB_FAIL(rs_gts_info_collector_.get_unit_info(this_server, unit_info))) {
          if (OB_ENTRY_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("fail to get unit info", K(ret), K(this_server));
          }
        } else if (OB_UNLIKELY(nullptr == unit_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unit info ptr is nullptr", K(ret));
        } else if (this_server != unit_info->unit_.server_) {
          unit_info->inc_outside_replica_cnt();
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(check_stop())) {
        LOG_WARN("gts unit monitor is stopped", K(ret));
      } else if (gts_info.standby_.is_valid()) {
        const common::ObAddr& this_server = gts_info.standby_;
        UnitInfoStruct* unit_info = nullptr;
        if (OB_FAIL(rs_gts_info_collector_.get_unit_info(gts_info.standby_, unit_info))) {
          if (OB_ENTRY_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("fail to get unit info", K(ret), "standby", gts_info.standby_);
          }
        } else if (OB_UNLIKELY(nullptr == unit_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unit info ptr is nullptr", K(ret));
        } else if (this_server != unit_info->unit_.server_) {
          unit_info->inc_outside_replica_cnt();
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < rs_gts_info_collector_.get_unit_info_array().count(); ++i) {
      const UnitInfoStruct& unit_info = rs_gts_info_collector_.get_unit_info_array().at(i);
      ObServerStatus status;
      if (OB_FAIL(check_stop())) {
        LOG_WARN("gts unit monitor is stopped", K(ret));
      } else if (ObUnit::UNIT_STATUS_ACTIVE != unit_info.unit_.status_) {
        // bypass, do not process the unit in deleting status
      } else if (unit_info.unit_.migrate_from_server_.is_valid() && 0 == unit_info.get_outside_replica_cnt()) {
        if (OB_FAIL(unit_mgr_.finish_migrate_unit(unit_info.unit_.unit_id_))) {
          LOG_WARN("fail to finish migrate unit", K(ret), "unit_id", unit_info.unit_.unit_id_);
        } else {
          LOG_INFO("finish migrate gts unit", K(ret), "unit_id", unit_info.unit_.unit_id_);
        }
      }
    }
  }
  return ret;
}

int ObRsGtsUnitDistributor::check_shrink_resource_pool()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = OB_GTS_TENANT_ID;
  ObArray<uint64_t> pool_ids;
  rs_gts_info_collector_.reuse();
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("gts unit monitor is stopped", K(ret));
  } else if (OB_FAIL(rs_gts_info_collector_.gather_stat())) {
    LOG_WARN("fail to gather stat", K(ret));
  } else if (OB_FAIL(unit_mgr_.get_pool_ids_of_tenant(tenant_id, pool_ids))) {
    LOG_WARN("fail to get resource pools", K(ret), K(tenant_id));
  } else {
    LOG_INFO("check shrink gts resource pool");
    for (int64_t i = 0; OB_SUCC(ret) && i < pool_ids.count(); ++i) {
      const uint64_t pool_id = pool_ids.at(i);
      bool in_shrinking = true;
      bool is_finished = true;
      if (OB_INVALID_ID == pool_id) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pool id unexpected", K(ret), K(pool_id));
      } else if (OB_FAIL(unit_mgr_.check_pool_in_shrinking(pool_id, in_shrinking))) {
        LOG_WARN("fail to check pool in shrinking", K(ret));
      } else if (!in_shrinking) {
        // not a shrink pool, bypass
      } else if (OB_FAIL(check_single_pool_shrinking_finished(tenant_id, pool_id, is_finished))) {
        LOG_WARN("fail to check single pool shrinking finished", K(ret), K(pool_id));
      } else if (OB_FAIL(check_stop())) {
        LOG_WARN("gts unit monitor stop", K(ret));
      } else if (!is_finished) {
        // not finish
      } else if (OB_FAIL(commit_shrink_resource_pool(pool_id))) {
        LOG_WARN("fail to commit shrink resource pool", K(ret), K(ret));
      }
    }
  }
  return ret;
}

int ObRsGtsUnitDistributor::check_single_pool_shrinking_finished(
    const uint64_t tenant_id, const uint64_t pool_id, bool& is_finished)
{
  int ret = OB_SUCCESS;
  common::ObArray<share::ObUnit> deleting_units;
  common::ObArray<common::ObAddr> deleting_servers;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(OB_GTS_TENANT_ID != tenant_id || OB_INVALID_ID == pool_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(pool_id));
  } else if (OB_FAIL(unit_mgr_.get_deleting_units_of_pool(pool_id, deleting_units))) {
    LOG_WARN("fail to get deleting units of pool", K(ret), K(pool_id));
  } else {
    is_finished = true;
    for (int64_t i = 0; OB_SUCC(ret) && i < deleting_units.count(); ++i) {
      const common::ObAddr& this_server = deleting_units.at(i).server_;
      if (OB_FAIL(deleting_servers.push_back(this_server))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
    for (int64_t i = 0; is_finished && OB_SUCC(ret) && i < rs_gts_info_collector_.get_gts_info_array().count(); ++i) {
      const common::ObGtsInfo& gts_info = rs_gts_info_collector_.get_gts_info_array().at(i);
      for (int64_t j = 0; is_finished && OB_SUCC(ret) && j < gts_info.member_list_.get_member_number(); ++j) {
        common::ObAddr this_server;
        if (OB_FAIL(gts_info.member_list_.get_server_by_index(j, this_server))) {
          LOG_WARN("fail to get server by index", K(ret), K(j));
        } else if (OB_UNLIKELY(!this_server.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("this server is unexpected", K(ret));
        } else if (has_exist_in_array(deleting_servers, this_server)) {
          is_finished = false;
        } else {
          // this server not on deleting unit
        }
      }
      if (OB_SUCC(ret) && is_finished) {
        const common::ObAddr& this_server = gts_info.standby_;
        if (!this_server.is_valid()) {
          // bypass
        } else if (has_exist_in_array(deleting_servers, this_server)) {
          is_finished = false;
        }
      }
    }
  }
  return ret;
}

int ObRsGtsUnitDistributor::commit_shrink_resource_pool(const uint64_t pool_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObShrinkResourcePoolChecker not init", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == pool_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(pool_id));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("gts unit monitor is stopped", K(ret));
  } else if (OB_FAIL(unit_mgr_.commit_shrink_resource_pool(pool_id))) {
    LOG_WARN("fail to commit shrink resource pool", K(ret), K(pool_id));
  }
  return ret;
}

int ObRsGtsUnitDistributor::check_stop() const
{
  int ret = OB_SUCCESS;
  if (stop_) {
    ret = OB_CANCELED;
  }
  return ret;
}

bool ObGtsReplicaTaskKey::is_valid() const
{
  return OB_INVALID_ID != gts_id_;
}

bool ObGtsReplicaTaskKey::operator==(const ObGtsReplicaTaskKey& that) const
{
  return gts_id_ == that.gts_id_;
}

ObGtsReplicaTaskKey& ObGtsReplicaTaskKey::operator=(const ObGtsReplicaTaskKey& that)
{
  gts_id_ = that.gts_id_;
  hash_value_ = that.hash_value_;
  return (*this);
}

uint64_t ObGtsReplicaTaskKey::hash() const
{
  return hash_value_;
}

int ObGtsReplicaTaskKey::init(const uint64_t gts_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == gts_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(gts_id));
  } else {
    gts_id_ = gts_id;
    hash_value_ = inner_hash();
  }
  return ret;
}

int ObGtsReplicaTaskKey::init(const ObGtsReplicaTaskKey& that)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!that.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    gts_id_ = that.gts_id_;
    hash_value_ = inner_hash();
  }
  return ret;
}

uint64_t ObGtsReplicaTaskKey::inner_hash() const
{
  uint64_t hash_val = 0;
  hash_val = murmurhash(&gts_id_, sizeof(gts_id_), hash_val);
  return hash_val;
}

int GtsMigrateReplicaTask::check_before_execute(rootserver::ObServerManager& server_mgr,
    rootserver::ObUnitManager& unit_mgr, share::ObGtsTableOperator& gts_table_operator, bool& can_execute)
{
  int ret = OB_SUCCESS;
  ObGtsInfo this_gts_info;
  UNUSED(unit_mgr);
  bool is_active = false;
  if (OB_FAIL(gts_table_operator.get_gts_info(gts_info_.gts_id_, this_gts_info))) {
    LOG_WARN("fail to get gts info", K(ret));
  } else if (this_gts_info.epoch_id_ != gts_info_.epoch_id_) {
    can_execute = false;
    LOG_INFO("membership epoch id changed, cannot execute in this turn",
        "curr_epoch_id",
        this_gts_info.epoch_id_,
        "prev_epoch_id",
        gts_info_.epoch_id_);
  } else if (OB_UNLIKELY(!dst_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dst server is invalid", K(ret), K(dst_));
  } else if (OB_FAIL(server_mgr.check_server_active(dst_, is_active))) {
    LOG_WARN("fail to check server active", K(ret));
  } else if (is_active) {
    can_execute = true;
  }
  return ret;
}

int GtsMigrateReplicaTask::try_remove_migrate_src(
    rootserver::ObServerManager& server_mgr, obrpc::ObSrvRpcProxy& rpc_proxy)
{
  int ret = OB_SUCCESS;
  bool sent = false;
  for (int64_t i = 0; !sent && OB_SUCC(ret) && i < gts_info_.member_list_.get_member_number(); ++i) {
    common::ObAddr this_server;
    bool is_alive = false;
    if (OB_FAIL(gts_info_.member_list_.get_server_by_index(i, this_server))) {
      LOG_WARN("fail to get server by index", K(ret));
    } else if (src_ == this_server) {
      // bypass,
    } else if (OB_FAIL(server_mgr.check_server_alive(this_server, is_alive))) {
      LOG_WARN("fail to check server alive", K(ret), K(this_server));
    } else if (!is_alive) {
      // by pass
    } else {
      obrpc::ObHaGtsChangeMemberResponse response;
      obrpc::ObHaGtsChangeMemberRequest gts_mc_arg;
      gts_mc_arg.set(gts_info_.gts_id_, src_);
      if (OB_FAIL(rpc_proxy.to(this_server).ha_gts_change_member(gts_mc_arg, response))) {
        LOG_WARN("fail to fail to change gts member", K(ret));
      } else if (OB_FAIL(response.get_ret_value())) {
        LOG_WARN("fail to do ha gts change member", K(ret));
      }
      sent = true;
    }
  }
  return ret;
}

int GtsMigrateReplicaTask::execute(rootserver::ObServerManager& server_mgr,
    share::ObGtsTableOperator& gts_table_operator, obrpc::ObSrvRpcProxy& rpc_proxy)
{
  int ret = OB_SUCCESS;
  bool do_update = false;
  if (!dst_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(gts_table_operator.try_update_standby(gts_info_, dst_, do_update))) {
    LOG_WARN("fail to try update standby", K(ret));
  } else if (MigrateType::MT_MIGRATE_REPLICA == migrate_type_ ||
             MigrateType::MT_SPREAD_REPLICA_AMONG_ZONE == migrate_type_) {
    if (!do_update) {
      // standby not update, maybe modified by others
    } else if (OB_FAIL(try_remove_migrate_src(server_mgr, rpc_proxy))) {
      LOG_WARN("fail to try remove migrate rpc", K(ret));
    }
  } else if (MigrateType::MT_MIGRATE_STANDBY == migrate_type_) {
    // no more to do for migrate standby
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected migrate type", K(ret), K(migrate_type_));
  }
  ROOTSERVICE_EVENT_ADD("gts_balancer",
      "migrate_gts_replica",
      "gts_id",
      gts_info_.gts_id_,
      "migrate_type",
      migrate_type_,
      "src",
      src_,
      "dst",
      dst_,
      "result",
      ret);
  return ret;
}

int GtsMigrateReplicaTask::clone_new(void* ptr, ObGtsReplicaTask*& output_ptr) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == ptr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(ptr));
  } else {
    GtsMigrateReplicaTask* gts_task = new (ptr) GtsMigrateReplicaTask;
    if (OB_UNLIKELY(nullptr == gts_task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to construct gts task", K(ret));
    } else if (OB_FAIL(gts_task->assign(*this))) {
      LOG_WARN("fail to assign to gts task", K(ret));
    } else {
      output_ptr = gts_task;
    }
  }
  return ret;
}

int GtsMigrateReplicaTask::init(const common::ObGtsInfo& gts_info, const common::ObAddr& src, const common::ObAddr& dst,
    const GtsMigrateReplicaTask::MigrateType migrate_type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!gts_info.is_valid() || !src.is_valid() || !dst.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(gts_info), K(src), K(dst));
  } else if (OB_FAIL(gts_info_.assign(gts_info))) {
    LOG_WARN("fail to assign gts info", K(ret), K(gts_info));
  } else if (OB_FAIL(task_key_.init(gts_info.gts_id_))) {
    LOG_WARN("fail to init task key", K(ret));
  } else {
    src_ = src;
    dst_ = dst;
    migrate_type_ = migrate_type;
  }
  return ret;
}

int GtsMigrateReplicaTask::assign(const GtsMigrateReplicaTask& that)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObGtsReplicaTask::assign(that))) {
    LOG_WARN("fail to assign base class", K(ret));
  } else if (OB_FAIL(gts_info_.assign(that.gts_info_))) {
    LOG_WARN("fail to assign gts info", K(ret));
  } else {
    src_ = that.src_;
    dst_ = that.dst_;
    migrate_type_ = that.migrate_type_;
  }
  return ret;
}

int GtsAllocStandbyTask::check_before_execute(rootserver::ObServerManager& server_mgr,
    rootserver::ObUnitManager& unit_mgr, share::ObGtsTableOperator& gts_table_operator, bool& can_execute)
{
  int ret = OB_SUCCESS;
  ObGtsInfo this_gts_info;
  UNUSED(unit_mgr);
  bool is_active = false;
  if (OB_FAIL(gts_table_operator.get_gts_info(gts_info_.gts_id_, this_gts_info))) {
    LOG_WARN("fail to get gts info", K(ret));
  } else if (this_gts_info.epoch_id_ != gts_info_.epoch_id_) {
    can_execute = false;
    LOG_INFO("memship epoch id changed, cannot execute in this turn",
        "curr_epoch_id",
        this_gts_info.epoch_id_,
        "prev_epoch_id",
        gts_info_.epoch_id_);
  } else if (OB_UNLIKELY(!new_standby_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("new standby is invalid", K(ret), K(new_standby_));
  } else if (OB_FAIL(server_mgr.check_server_active(new_standby_, is_active))) {
    LOG_WARN("fail to check server active", K(ret), K(new_standby_));
  } else if (is_active) {
    can_execute = true;
  }
  return ret;
}

int GtsAllocStandbyTask::execute(rootserver::ObServerManager& server_mgr, share::ObGtsTableOperator& gts_table_operator,
    obrpc::ObSrvRpcProxy& rpc_proxy)
{
  int ret = OB_SUCCESS;
  UNUSED(server_mgr);
  UNUSED(rpc_proxy);
  bool do_update = false;  // indeed dummy in this func
  if (OB_UNLIKELY(!new_standby_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(new_standby_));
  } else if (OB_FAIL(gts_table_operator.try_update_standby(gts_info_, new_standby_, do_update))) {
    LOG_WARN("fail to try update standby", K(ret));
  }
  ROOTSERVICE_EVENT_ADD(
      "gts_balancer", "alloc_gts_standby", "gts_id", gts_info_.gts_id_, "new_standby", new_standby_, "result", ret);
  return ret;
}

int GtsAllocStandbyTask::clone_new(void* ptr, ObGtsReplicaTask*& output_ptr) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == ptr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(ptr));
  } else {
    GtsAllocStandbyTask* gts_task = new (ptr) GtsAllocStandbyTask;
    if (OB_UNLIKELY(nullptr == gts_task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to construct gts task", K(ret));
    } else if (OB_FAIL(gts_task->assign(*this))) {
      LOG_WARN("fail to assign to gts task", K(ret));
    } else {
      output_ptr = gts_task;
    }
  }
  return ret;
}

int GtsAllocStandbyTask::init(const common::ObGtsInfo& gts_info, const common::ObAddr& new_standby)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!gts_info.is_valid() || !new_standby.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(gts_info_.assign(gts_info))) {
    LOG_WARN("fail to assign gts info", K(ret));
  } else if (OB_FAIL(task_key_.init(gts_info.gts_id_))) {
    LOG_WARN("fail to init task key", K(ret));
  } else {
    new_standby_ = new_standby;
  }
  return ret;
}

int GtsAllocStandbyTask::assign(const GtsAllocStandbyTask& that)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObGtsReplicaTask::assign(that))) {
    LOG_WARN("fail to assign base class", K(ret));
  } else if (OB_FAIL(gts_info_.assign(that.gts_info_))) {
    LOG_WARN("fail to assign gts info", K(ret));
  } else {
    new_standby_ = that.new_standby_;
  }
  return ret;
}

int ObRsGtsReplicaTaskGenerator::init(common::ObMySQLProxy* sql_proxy)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(nullptr == sql_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(rs_gts_info_collector_.init(sql_proxy))) {
    LOG_WARN("fail to init rs gts info collector", K(ret));
  } else {
    sql_proxy_ = sql_proxy;
    inited_ = true;
  }
  return ret;
}

void ObRsGtsReplicaTaskGenerator::reuse()
{
  for (int64_t i = 0; i < output_task_array_.count(); ++i) {
    ObGtsReplicaTask* task = output_task_array_.at(i);
    if (nullptr != task) {
      task->~ObGtsReplicaTask();
    }
  }
  output_task_array_.reset();
  allocator_.reset();
  rs_gts_info_collector_.reuse();
}

int ObRsGtsReplicaTaskGenerator::get_excluded_servers(
    const common::ObGtsInfo& gts_info, common::ObIArray<common::ObAddr>& excluded_servers)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!gts_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(gts_info));
  } else {
    excluded_servers.reset();
    const common::ObMemberList& member_list = gts_info.member_list_;
    for (int64_t i = 0; OB_SUCC(ret) && i < member_list.get_member_number(); ++i) {
      ObAddr server;
      UnitInfoStruct* unit_info = nullptr;
      if (OB_FAIL(member_list.get_server_by_index(i, server))) {
        LOG_WARN("fail to get server by index", K(ret));
      } else if (OB_FAIL(rs_gts_info_collector_.get_unit_info(server, unit_info))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_SUCCESS;  // may have replica not in any unit, ignore them
        } else {
          LOG_WARN("fail to get refactored", K(ret), K(server));
        }
      } else if (OB_UNLIKELY(nullptr == unit_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit info ptr is null", K(ret));
      } else if (OB_FAIL(excluded_servers.push_back(unit_info->unit_.server_))) {
        LOG_WARN("fail to push back", K(ret));
      } else if (!unit_info->unit_.migrate_from_server_.is_valid()) {
        // bypass
      } else if (OB_FAIL(excluded_servers.push_back(unit_info->unit_.migrate_from_server_))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
  }
  return ret;
}

int ObRsGtsReplicaTaskGenerator::get_zone_replica_cnts(
    const common::ObGtsInfo& gts_info, const bool count_standby, common::ObIArray<ZoneReplicaCnt>& zone_replica_cnts)
{
  int ret = OB_SUCCESS;
  common::ObArray<common::ObZone> zone_list;
  if (OB_UNLIKELY(!gts_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(gts_info));
  } else if (OB_FAIL(zone_mgr_.get_zone(zone_list))) {
    LOG_WARN("fail to get zone", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < zone_list.count(); ++i) {
      common::ObRegion this_region;
      const common::ObZone& this_zone = zone_list.at(i);
      if (OB_FAIL(zone_mgr_.get_region(this_zone, this_region))) {
        LOG_WARN("fail to get region", K(ret), K(this_zone));
      } else if (this_region != gts_info.region_) {
        // bypass
      } else {
        ZoneReplicaCnt zone_replica_cnt;
        zone_replica_cnt.zone_ = this_zone;
        zone_replica_cnt.cnt_ = 0;
        if (OB_FAIL(zone_replica_cnts.push_back(zone_replica_cnt))) {
          LOG_WARN("fail to push back", K(ret));
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < gts_info.member_list_.get_member_number(); ++i) {
      common::ObAddr this_server;
      common::ObZone this_zone;
      UnitInfoStruct* unit_info = nullptr;
      if (OB_FAIL(gts_info.member_list_.get_server_by_index(i, this_server))) {
        LOG_WARN("fail to get server by index", K(ret));
      } else if (OB_FAIL(rs_gts_info_collector_.get_unit_info(this_server, unit_info))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_SUCCESS;  // not in unit, ignore them
        } else {
          LOG_WARN("fail to get unit info", K(ret), K(this_server));
        }
      } else if (OB_UNLIKELY(nullptr == unit_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit info ptr is null", K(ret));
      } else if (OB_FAIL(server_mgr_.get_server_zone(this_server, this_zone))) {
        LOG_WARN("fail to get server zone", K(ret));
      } else {
        bool find = false;
        for (int64_t j = 0; !find && OB_SUCC(ret) && j < zone_replica_cnts.count(); ++j) {
          ZoneReplicaCnt& this_instance = zone_replica_cnts.at(j);
          if (this_instance.zone_ == this_zone) {
            find = true;
            ++this_instance.cnt_;
          }
        }
        if (!find) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("zone not found", K(ret), K(this_zone));
        }
      }
    }
    if (OB_SUCC(ret) && count_standby) {
      const common::ObAddr& this_server = gts_info.standby_;
      common::ObZone this_zone;
      UnitInfoStruct* unit_info = nullptr;
      if (!this_server.is_valid()) {
        // invalid standby, bypass
      } else if (OB_FAIL(server_mgr_.get_server_zone(this_server, this_zone))) {
        LOG_WARN("fail to get server zone", K(ret));
      } else if (OB_FAIL(rs_gts_info_collector_.get_unit_info(this_server, unit_info))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_SUCCESS;  // not in unit, ignore them
        } else {
          LOG_WARN("fail to get unit info", K(ret), K(this_server));
        }
      } else if (OB_UNLIKELY(nullptr == unit_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit info ptr is null", K(ret));
      } else {
        bool find = false;
        for (int64_t j = 0; !find && OB_SUCC(ret) && j < zone_replica_cnts.count(); ++j) {
          ZoneReplicaCnt& this_instance = zone_replica_cnts.at(j);
          if (this_instance.zone_ == this_zone) {
            find = true;
            ++this_instance.cnt_;
          }
        }
        if (!find) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("zone not found", K(ret), K(this_zone));
        }
      }
    }
  }
  return ret;
}

int ObRsGtsReplicaTaskGenerator::pick_new_gts_replica(const common::ObGtsInfo& gts_info, common::ObAddr& new_server)
{
  int ret = OB_SUCCESS;
  common::ObArray<common::ObAddr> excluded_servers;
  common::ObArray<ZoneReplicaCnt> zone_replica_cnts;
  if (OB_UNLIKELY(!gts_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(get_excluded_servers(gts_info, excluded_servers))) {
    LOG_WARN("fail to excluded servers", K(ret));
  } else if (OB_FAIL(get_zone_replica_cnts(gts_info, true /*count standby*/, zone_replica_cnts))) {
    LOG_WARN("fail to get zone replica cnts", K(ret));
  } else {
    std::sort(zone_replica_cnts.begin(), zone_replica_cnts.end());
    bool find = false;
    new_server.reset();
    for (int64_t i = 0; !find && OB_SUCC(ret) && i < zone_replica_cnts.count(); ++i) {
      const ZoneReplicaCnt& zone_replica_cnt = zone_replica_cnts.at(i);
      for (int64_t j = 0; !find && OB_SUCC(ret) && j < rs_gts_info_collector_.get_unit_info_array().count(); ++j) {
        const UnitInfoStruct& unit_info = rs_gts_info_collector_.get_unit_info_array().at(j);
        bool is_active = false;
        bool in_service = false;
        bool is_stopped = false;
        if (ObUnit::UNIT_STATUS_ACTIVE != unit_info.unit_.status_) {
          // bypass
        } else if (zone_replica_cnt.zone_ != unit_info.unit_.zone_) {
          // bypass
        } else if (has_exist_in_array(excluded_servers, unit_info.unit_.server_)) {
          // in excluded servers
        } else if (OB_FAIL(server_mgr_.check_server_active(unit_info.unit_.server_, is_active))) {
          LOG_WARN("fail to check server active", K(ret));
        } else if (!is_active) {
          // bypass, not active
        } else if (OB_FAIL(server_mgr_.check_in_service(unit_info.unit_.server_, in_service))) {
          LOG_WARN("fail to check in service", K(ret));
        } else if (!in_service) {
          // bypass, not in service
        } else if (OB_FAIL(server_mgr_.is_server_stopped(unit_info.unit_.server_, is_stopped))) {
          LOG_WARN("fail to check is server stopped", K(ret));
        } else if (is_stopped) {
          // bypass, server stopped
        } else {
          find = true;
          new_server = unit_info.unit_.server_;
        }
      }
    }
    // when find is false,no new server is selected,and the new_server value is invalid
  }
  return ret;
}

int ObRsGtsReplicaTaskGenerator::construct_alloc_standby_task(
    const common::ObGtsInfo& gts_info, const common::ObAddr& new_standby, GtsAllocStandbyTask*& new_task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!gts_info.is_valid() || !new_standby.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(gts_info), K(new_standby));
  } else {
    void* ptr = allocator_.alloc(sizeof(GtsAllocStandbyTask));
    if (OB_UNLIKELY(nullptr == ptr)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", K(ret));
    } else if (OB_UNLIKELY(nullptr == (new_task = new (ptr) GtsAllocStandbyTask))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("new task construct unexpected", K(ret));
    } else if (OB_FAIL(new_task->init(gts_info, new_standby))) {
      LOG_WARN("fail to init gts alloc standby task", K(ret));
    }
  }
  return ret;
}

int ObRsGtsReplicaTaskGenerator::construct_migrate_replica_task(const common::ObGtsInfo& gts_info,
    const common::ObAddr& src, const common::ObAddr& dst, const GtsMigrateReplicaTask::MigrateType migrate_type,
    GtsMigrateReplicaTask*& new_task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!gts_info.is_valid() || !src.is_valid() || !dst.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(gts_info), K(src), K(dst));
  } else {
    void* ptr = allocator_.alloc(sizeof(GtsMigrateReplicaTask));
    if (OB_UNLIKELY(nullptr == ptr)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", K(ret));
    } else if (OB_UNLIKELY(nullptr == (new_task = new (ptr) GtsMigrateReplicaTask))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("new task construct unexpected", K(ret));
    } else if (OB_FAIL(new_task->init(gts_info, src, dst, migrate_type))) {
      LOG_WARN("fail to init migrate replica task", K(ret));
    }
  }
  return ret;
}

int ObRsGtsReplicaTaskGenerator::try_alloc_standby_task(
    const common::ObGtsInfo& gts_info, ObGtsReplicaTask*& gts_replica_task)
{
  int ret = OB_SUCCESS;
  bool need_alloc = false;
  bool is_active = false;
  bool in_service = false;
  gts_replica_task = nullptr;
  GtsAllocStandbyTask* new_task = nullptr;
  common::ObAddr new_standby;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!gts_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (!gts_info.standby_.is_valid()) {
    need_alloc = true;
  } else if (OB_FAIL(server_mgr_.check_server_active(gts_info.standby_, is_active))) {
    LOG_WARN("fail to check server active", K(ret), "server", gts_info.standby_);
  } else if (OB_FAIL(server_mgr_.check_in_service(gts_info.standby_, in_service))) {
    LOG_WARN("fail to check in service", K(ret), "server", gts_info.standby_);
  } else if (!is_active || !in_service) {
    need_alloc = true;
  }
  if (OB_FAIL(ret)) {
    // bypass
  } else if (!need_alloc) {
    // bypass
  } else if (OB_FAIL(pick_new_gts_replica(gts_info, new_standby))) {
    LOG_WARN("fail to pick new gts replica", K(ret));
  } else if (!new_standby.is_valid()) {
    // bypass, no new standby found
  } else if (OB_FAIL(construct_alloc_standby_task(gts_info, new_standby, new_task))) {
    LOG_WARN("fail to construct alloc standby task", K(ret));
  } else if (OB_UNLIKELY(nullptr == new_task)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("new task ptr is null", K(ret));
  } else {
    gts_replica_task = new_task;
  }
  return ret;
}

int ObRsGtsReplicaTaskGenerator::check_can_migrate(
    const common::ObAddr& myself, const common::ObGtsInfo& gts_info, bool& can_migrate)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!myself.is_valid() || !gts_info.is_valid())) {
    LOG_WARN("invalid argument", K(ret), K(myself), K(gts_info));
  } else {
    int64_t valid_m_cnt = 0;
    can_migrate = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < gts_info.member_list_.get_member_number(); ++i) {
      common::ObAddr this_server;
      bool in_service = false;
      bool is_active = false;
      if (OB_FAIL(gts_info.member_list_.get_server_by_index(i, this_server))) {
        LOG_WARN("fail to get server by index", K(ret));
      } else if (this_server == myself) {
        // bypass
      } else if (OB_FAIL(server_mgr_.check_in_service(this_server, in_service))) {
        LOG_WARN("fail to check in service", K(ret), K(this_server));
      } else if (!in_service) {
        // bypass
      } else if (OB_FAIL(server_mgr_.check_server_active(this_server, is_active))) {
        LOG_WARN("fail to check server active", K(ret), K(this_server));
      } else if (!is_active) {
        // bypass
      } else {
        ++valid_m_cnt;
      }
    }
    if (OB_FAIL(ret)) {
      // failed
    } else if (valid_m_cnt >= (GTS_QUORUM - 1)) {
      can_migrate = true;
    }
  }
  return ret;
}

int ObRsGtsReplicaTaskGenerator::try_generate_spread_replica_between_zone_task(const common::ObGtsInfo& gts_info,
    const common::ObZone& src_zone, const common::ObZone& dst_zone, ObGtsReplicaTask*& gts_replica_task)
{
  int ret = OB_SUCCESS;
  common::ObArray<common::ObAddr> excluded_servers;
  const ObIArray<UnitInfoStruct>& unit_info_array = rs_gts_info_collector_.get_unit_info_array();
  const common::ObMemberList& member_list = gts_info.member_list_;
  common::ObAddr src_server;
  common::ObAddr dst_server;
  gts_replica_task = nullptr;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!gts_info.is_valid() || src_zone.is_empty() || dst_zone.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(gts_info), K(src_zone), K(dst_zone));
  } else if (src_zone == dst_zone) {
    // bypass
  } else if (OB_FAIL(get_excluded_servers(gts_info, excluded_servers))) {
    LOG_WARN("fail to get excluded servers", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < unit_info_array.count() && !dst_server.is_valid(); ++i) {
      bool is_active = false;
      bool in_service = false;
      bool is_stopped = false;
      const UnitInfoStruct& this_u = unit_info_array.at(i);
      if (this_u.unit_.zone_ != dst_zone) {
        // bypass, since zone not match
      } else if (has_exist_in_array(excluded_servers, this_u.unit_.server_)) {
        // bypass, since the unit has replica
      } else if (has_exist_in_array(excluded_servers, this_u.unit_.migrate_from_server_)) {
        // bypass, since the unit has replica
      } else if (OB_FAIL(server_mgr_.check_server_active(this_u.unit_.server_, is_active))) {
        LOG_WARN("fail to check server active", K(ret));
      } else if (!is_active) {
        // bypass, not active
      } else if (OB_FAIL(server_mgr_.check_in_service(this_u.unit_.server_, in_service))) {
        LOG_WARN("fail to check in service", K(ret));
      } else if (!in_service) {
        // bypass, not in service
      } else if (OB_FAIL(server_mgr_.is_server_stopped(this_u.unit_.server_, is_stopped))) {
        LOG_WARN("fail to check is server stopped", K(ret));
      } else if (is_stopped) {
        // bypass, server stopped
      } else {
        dst_server = this_u.unit_.server_;
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < member_list.get_member_number() && !src_server.is_valid(); ++i) {
      ObAddr this_server;
      ObZone this_zone;
      if (OB_FAIL(member_list.get_server_by_index(i, this_server))) {
        LOG_WARN("fail to get server by index", K(ret));
      } else if (OB_FAIL(server_mgr_.get_server_zone(this_server, this_zone))) {
        LOG_WARN("fail to get servre zone", K(ret), K(this_server));
      } else if (this_zone != src_zone) {
        // bypass, since zone not match
      } else {
        src_server = this_server;
      }
    }
    if (OB_SUCC(ret) && src_server.is_valid() && dst_server.is_valid()) {
      GtsMigrateReplicaTask* new_task = nullptr;
      if (OB_FAIL(construct_migrate_replica_task(gts_info,
              src_server,
              dst_server,
              GtsMigrateReplicaTask::MigrateType::MT_SPREAD_REPLICA_AMONG_ZONE,
              new_task))) {
        LOG_WARN("fail to construct migrate replica task", K(ret));
      } else if (OB_UNLIKELY(nullptr == new_task)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("new task ptr is null", K(ret));
      } else {
        gts_replica_task = new_task;
      }
    }
  }
  return ret;
}

int ObRsGtsReplicaTaskGenerator::try_generate_spread_replica_among_zone_task(
    const common::ObGtsInfo& gts_info, ObGtsReplicaTask*& gts_replica_task)
{
  int ret = OB_SUCCESS;
  gts_replica_task = nullptr;
  common::ObArray<ZoneReplicaCnt> zone_replica_cnts;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!gts_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(gts_info));
  } else if (gts_info.member_list_.get_member_number() != GTS_QUORUM) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should not be here", K(ret), K(gts_info));
  } else if (OB_FAIL(get_zone_replica_cnts(gts_info, false /*count standby*/, zone_replica_cnts))) {
    LOG_WARN("fail to get zone replica cnts", K(ret));
  } else if (zone_replica_cnts.count() <= 0) {
    // bypass
  } else {
    // sorted by cnt asc
    std::sort(zone_replica_cnts.begin(), zone_replica_cnts.end());
    const int64_t array_cnt = zone_replica_cnts.count();
    const int64_t max_replica_cnt = zone_replica_cnts.at(array_cnt - 1).cnt_;
    const common::ObZone& src_zone = zone_replica_cnts.at(array_cnt - 1).zone_;
    for (int64_t i = 0; nullptr == gts_replica_task && OB_SUCC(ret) && i < zone_replica_cnts.count(); ++i) {
      const ZoneReplicaCnt& this_ins = zone_replica_cnts.at(i);
      const common::ObZone& dst_zone = this_ins.zone_;
      if (max_replica_cnt - this_ins.cnt_ <= 1) {
        // by pass
      } else if (OB_FAIL(
                     try_generate_spread_replica_between_zone_task(gts_info, src_zone, dst_zone, gts_replica_task))) {
        LOG_WARN("fail to try generate spread replica between zone task", K(ret));
      } else if (nullptr != gts_replica_task) {
        LOG_INFO("generate a gts spread replica task", "task_info", *gts_replica_task);
      }
    }
  }
  return ret;
}

int ObRsGtsReplicaTaskGenerator::try_generate_migrate_replica_task(
    const common::ObGtsInfo& gts_info, ObGtsReplicaTask*& gts_replica_task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!gts_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(gts_info));
  } else if (gts_info.member_list_.get_member_number() != GTS_QUORUM) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should not be here", K(ret), K(gts_info));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < gts_info.member_list_.get_member_number(); ++i) {
      ObAddr this_server;
      UnitInfoStruct* unit_info = nullptr;
      GtsMigrateReplicaTask* new_task = nullptr;
      ObServerStatus server_status;
      bool need_migrate = false;         // if this replica need to be migrated
      bool migrate_beyond_unit = false;  // migrate inside the unit or between units
      bool can_migrate = true;
      if (OB_FAIL(gts_info.member_list_.get_server_by_index(i, this_server))) {
        LOG_WARN("fail to get server by index", K(ret));
      } else if (OB_FAIL(rs_gts_info_collector_.get_unit_info(this_server, unit_info))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          need_migrate = true;  // replica not on unit, need migrate
          migrate_beyond_unit = true;
        } else {
          LOG_WARN("fail to unit info", K(ret), K(this_server));
        }
      } else if (nullptr == unit_info) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit info ptr is null", K(ret), KP(unit_info));
      } else if (OB_FAIL(server_mgr_.get_server_status(unit_info->unit_.server_, server_status))) {
        LOG_WARN("fail to get server status", K(ret), "server", unit_info->unit_.server_);
      } else if (ObUnit::UNIT_STATUS_ACTIVE != unit_info->unit_.status_) {
        need_migrate = true;  // replica on deleting unit, need migrate
        migrate_beyond_unit = true;
      } else if (unit_info->unit_.server_ != this_server) {
        need_migrate = true;  // replica on migrate unit, need migrate
        migrate_beyond_unit = false;
      }

      common::ObAddr src_server;
      common::ObAddr dst_server;
      if (OB_FAIL(ret) || !need_migrate) {
        // bypass
      } else if (OB_FAIL(check_can_migrate(this_server, gts_info, can_migrate))) {
        LOG_WARN("fail to check can migrate", K(ret), K(this_server), K(gts_info));
      } else if (!can_migrate) {
        // check next replica
      } else if (migrate_beyond_unit) {
        src_server = this_server;
        if (OB_FAIL(pick_new_gts_replica(gts_info, dst_server))) {
          LOG_WARN("fail to pick new gts replica", K(ret));
        } else if (!dst_server.is_valid()) {
          can_migrate = false;
        }
      } else {
        src_server = unit_info->unit_.migrate_from_server_;
        dst_server = unit_info->unit_.server_;
      }

      if (OB_FAIL(ret) || !can_migrate || !need_migrate) {
      } else if (OB_FAIL(construct_migrate_replica_task(gts_info,
                     src_server,
                     dst_server,
                     GtsMigrateReplicaTask::MigrateType::MT_MIGRATE_REPLICA,
                     new_task))) {
        LOG_WARN("fail to construct migrate replica task", K(ret));
      } else if (OB_UNLIKELY(nullptr == new_task)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("new task ptr is null", K(ret));
      } else {
        gts_replica_task = new_task;
        break;  // one task for one gts instance once
      }
    }
  }
  return ret;
}

int ObRsGtsReplicaTaskGenerator::inner_generate_gts_replica_task(
    const common::ObGtsInfo& gts_info, ObGtsReplicaTask*& gts_replica_task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!gts_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(gts_info));
  } else if (gts_info.member_list_.get_member_number() != GTS_QUORUM) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should not be here", K(ret), K(gts_info));
  } else if (OB_FAIL(try_alloc_standby_task(gts_info, gts_replica_task))) {
    LOG_WARN("fail to try alloc standby task", K(ret));
  } else if (nullptr != gts_replica_task) {
    // got a alloc standby task
  } else if (OB_FAIL(try_generate_migrate_replica_task(gts_info, gts_replica_task))) {
    LOG_WARN("fail to try generate migrate replica task", K(ret));
  } else if (nullptr != gts_replica_task) {
    // got a gts replica task
  } else if (OB_FAIL(try_generate_spread_replica_among_zone_task(gts_info, gts_replica_task))) {
    LOG_WARN("fail to try generate spread replica among zone task", K(ret));
  }
  return ret;
}

/* 1 when the member number in member list is less than QUORUM, we try to
 *   allocate a standby
 *   1.1 if standby is invalid,allocated a standby
 *   1.2 if standby is valid,check if the server holding the standby is available
 * 2 when the member number in member list is greater than QUORUM, print ERROR
 * 3 when the member number in member list is equal to QUORUM, do as follows
 */
int ObRsGtsReplicaTaskGenerator::try_generate_gts_replica_task(
    const common::ObGtsInfo& gts_info, ObGtsReplicaTask*& gts_replica_task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!gts_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(gts_info));
  } else {
    gts_replica_task = nullptr;
    const common::ObMemberList& member_list = gts_info.member_list_;
    if (member_list.get_member_number() < GTS_QUORUM) {
      if (OB_FAIL(try_alloc_standby_task(gts_info, gts_replica_task))) {
        LOG_WARN("fail to try alloc standby task", K(ret));
      }
    } else if (member_list.get_member_number() > GTS_QUORUM) {
      LOG_ERROR("member list member num unexpected", K(gts_info));
    } else {
      if (OB_FAIL(inner_generate_gts_replica_task(gts_info, gts_replica_task))) {
        LOG_WARN("fail to inner genreate gts replica task", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (nullptr != gts_replica_task) {
        // bypass
      } else if (OB_FAIL(try_generate_migrate_standby_task(gts_info, gts_replica_task))) {
        LOG_WARN("fail to try generate migrate standby task", K(ret), K(gts_info));
      }
    }
    if (OB_SUCC(ret)) {
      if (nullptr != gts_replica_task) {
        // bypass
      } else if (OB_FAIL(try_generate_server_or_zone_stopped_task(gts_info, gts_replica_task))) {
        LOG_WARN("fail to try generate server or zone stopped task", K(ret));
      }
    }
  }
  return ret;
}

int ObRsGtsReplicaTaskGenerator::try_generate_server_or_zone_stopped_task(
    const common::ObGtsInfo& gts_info, ObGtsReplicaTask*& gts_replica_task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!gts_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(gts_info));
  } else {
    gts_replica_task = nullptr;
    const common::ObMemberList& member_list = gts_info.member_list_;
    if (member_list.get_member_number() < GTS_QUORUM) {
      // bypass
    } else {
      common::ObArray<MemberCandStat> member_cand_stat_array;
      for (int64_t i = 0; OB_SUCC(ret) && i < member_list.get_member_number(); ++i) {
        ObAddr this_server;
        ObZone this_zone;
        bool is_server_stopped = false;
        bool is_zone_active = false;
        if (OB_FAIL(member_list.get_server_by_index(i, this_server))) {
          LOG_WARN("fail to get server by index", K(ret));
        } else if (OB_FAIL(server_mgr_.get_server_zone(this_server, this_zone))) {
          LOG_WARN("fail to get server zone", K(ret), K(this_server));
        } else if (OB_FAIL(server_mgr_.check_server_stopped(this_server, is_server_stopped))) {
          LOG_WARN("fail to check server stopped", K(ret), K(this_server));
        } else if (OB_FAIL(zone_mgr_.check_zone_active(this_zone, is_zone_active))) {
          LOG_WARN("fail to check zone active", K(ret), K(this_zone));
        } else if (!is_server_stopped && is_zone_active) {
          // bypass, the member of this server/zone is active
        } else {
          MemberCandStat member_cand_stat(this_server, this_zone);
          if (OB_FAIL(member_cand_stat_array.push_back(member_cand_stat))) {
            LOG_WARN("fail to push back array", K(ret));
          }
        }
      }
      if (OB_FAIL(ret)) {
        // bypass
      } else if (member_cand_stat_array.count() <= 1) {
        // bypass, more than one member is in the illegal server/zone
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < member_cand_stat_array.count(); ++i) {
          const MemberCandStat& this_member = member_cand_stat_array.at(i);
          bool can_migrate = false;
          common::ObAddr new_server;
          GtsMigrateReplicaTask* new_task = nullptr;
          if (OB_FAIL(check_can_migrate(this_member.server_, gts_info, can_migrate))) {
            LOG_WARN("fail to check can migrate", K(ret), K(gts_info));
          } else if (!can_migrate) {
            // bypass
          } else if (OB_FAIL(pick_new_gts_replica(gts_info, new_server))) {
            LOG_WARN("fail to pick new gts replica", K(ret));
          } else if (!new_server.is_valid()) {
            // bypass
          } else if (OB_FAIL(construct_migrate_replica_task(gts_info,
                         this_member.server_ /*src*/,
                         new_server /*dst*/,
                         GtsMigrateReplicaTask::MigrateType::MT_MIGRATE_REPLICA,
                         new_task))) {
            LOG_WARN("fail to construct migrate replica task", K(ret));
          } else if (OB_UNLIKELY(nullptr == new_task)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("new task ptr is null", K(ret));
          } else {
            gts_replica_task = new_task;
            break;  // one task for an instance every time
          }
        }
      }
    }
  }
  return ret;
}

int ObRsGtsReplicaTaskGenerator::try_generate_migrate_standby_task(
    const common::ObGtsInfo& gts_info, ObGtsReplicaTask*& gts_replica_task)
{
  int ret = OB_SUCCESS;
  gts_replica_task = nullptr;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!gts_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (!gts_info.standby_.is_valid()) {
    // bypass
  } else {
    UnitInfoStruct* unit_info = nullptr;
    GtsMigrateReplicaTask* new_task = nullptr;
    ObServerStatus server_status;
    bool need_migrate = false;         // if this replica need to be migrated
    bool migrate_beyond_unit = false;  // migrate inside the unit or between units
    if (OB_FAIL(rs_gts_info_collector_.get_unit_info(gts_info.standby_, unit_info))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        need_migrate = true;  // replica not on unit, need migrate
        migrate_beyond_unit = true;
      } else {
        LOG_WARN("fail to get refactored", K(ret));
      }
    } else if (nullptr == unit_info) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unit info ptr is null", K(ret));
    } else if (OB_FAIL(server_mgr_.get_server_status(unit_info->unit_.server_, server_status))) {
      LOG_WARN("fail to get server status", K(ret), "server", unit_info->unit_.server_);
    } else if (ObUnit::UNIT_STATUS_ACTIVE != unit_info->unit_.status_) {
      need_migrate = true;
      migrate_beyond_unit = true;
    } else if (unit_info->unit_.server_ != gts_info.standby_) {
      need_migrate = true;
      migrate_beyond_unit = false;
    }

    common::ObAddr src_server;
    common::ObAddr dst_server;
    bool can_migrate = true;
    if (OB_FAIL(ret) || !need_migrate) {
      // bypass
    } else if (migrate_beyond_unit) {
      src_server = gts_info.standby_;
      if (OB_FAIL(pick_new_gts_replica(gts_info, dst_server))) {
        LOG_WARN("fail to pick new gts replica", K(ret));
      } else if (!dst_server.is_valid()) {
        can_migrate = false;
      }
    } else {
      src_server = unit_info->unit_.migrate_from_server_;
      dst_server = unit_info->unit_.server_;
    }

    if (OB_FAIL(ret) || !can_migrate || !need_migrate) {
    } else if (OB_FAIL(construct_migrate_replica_task(gts_info,
                   src_server,
                   dst_server,
                   GtsMigrateReplicaTask::MigrateType::MT_MIGRATE_STANDBY,
                   new_task))) {
      LOG_WARN("fail to constrcut migrate replica task", K(ret));
    } else if (OB_UNLIKELY(nullptr == new_task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("new task ptr is null", K(ret));
    } else {
      gts_replica_task = new_task;
    }
  }
  return ret;
}

int ObRsGtsReplicaTaskGenerator::output_gts_replica_task_array(
    common::ObIArray<const ObGtsReplicaTask*>& output_task_array)
{
  int ret = OB_SUCCESS;
  output_task_array.reset();
  reuse();
  if (OB_FAIL(rs_gts_info_collector_.gather_stat())) {
    LOG_WARN("fail to gather stat", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < rs_gts_info_collector_.get_gts_info_array().count(); ++i) {
      const common::ObGtsInfo& gts_info = rs_gts_info_collector_.get_gts_info_array().at(i);
      ObGtsReplicaTask* gts_replica_task = nullptr;
      if (OB_FAIL(try_generate_gts_replica_task(gts_info, gts_replica_task))) {
        LOG_WARN("fail to try generate gts replica task", K(ret));
      } else if (nullptr == gts_replica_task) {
        // bypass
      } else if (OB_FAIL(output_task_array_.push_back(gts_replica_task))) {
        LOG_WARN("fail to push back", K(ret));
      } else if (OB_FAIL(output_task_array.push_back(gts_replica_task))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
  }
  return ret;
}

int ObRsGtsReplicaTaskGenerator::check_stop()
{
  int ret = OB_SUCCESS;
  if (stop_) {
    ret = OB_CANCELED;
  }
  return ret;
}

ObRsGtsMonitor::ObRsGtsMonitor(rootserver::ObUnitManager& unit_mgr, rootserver::ObServerManager& server_mgr,
    rootserver::ObZoneManager& zone_mgr, rootserver::ObRsGtsTaskMgr& gts_task_mgr)
    : inited_(false),
      idling_(stop_),
      gts_unit_distributor_(unit_mgr, server_mgr, zone_mgr, stop_),
      gts_replica_task_generator_(unit_mgr, server_mgr, zone_mgr, stop_),
      gts_task_mgr_(gts_task_mgr),
      unit_mgr_(unit_mgr),
      server_mgr_(server_mgr),
      zone_mgr_(zone_mgr),
      sql_proxy_(nullptr)
{}

int ObRsGtsMonitor::init(common::ObMySQLProxy* sql_proxy)
{
  int ret = OB_SUCCESS;
  static const int64_t rs_gts_monitor_thread_cnt = 1;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(nullptr == sql_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(sql_proxy));
  } else if (OB_FAIL(gts_unit_distributor_.init(sql_proxy))) {
    LOG_WARN("fail to init gts unit distributor", K(ret));
  } else if (OB_FAIL(gts_replica_task_generator_.init(sql_proxy))) {
    LOG_WARN("fail to init gts replica task generator", K(ret));
  } else if (OB_FAIL(create(rs_gts_monitor_thread_cnt, "RSGTSMon"))) {
    LOG_WARN("fail to create rs gts monitor thread", K(ret));
  } else {
    sql_proxy_ = sql_proxy;
    inited_ = true;
  }
  return ret;
}

void ObRsGtsMonitor::run3()
{
  LOG_INFO("rs gts monitor start");
  int err = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    err = OB_NOT_INIT;
    LOG_ERROR("not inited", K(err));
  } else {
    common::ObArray<const ObGtsReplicaTask*> output_task_array;
    while (!stop_) {
      if (OB_SUCCESS != (err = gts_unit_distributor_.distribute_unit_for_server_status_change())) {
        LOG_WARN("fail to distribute unit for server status change", K(err));
      }
      if (OB_SUCCESS != (err = gts_replica_task_generator_.output_gts_replica_task_array(output_task_array))) {
        LOG_WARN("fail to output gts replica task array", K(err));
      } else {
        for (int64_t i = 0; OB_SUCCESS == err && i < output_task_array.count(); ++i) {
          const ObGtsReplicaTask* this_task = output_task_array.at(i);
          if (nullptr == this_task) {
            // ignore
          } else {
            int tmp_ret = gts_task_mgr_.push_task(this_task);
            if (OB_SUCCESS == tmp_ret) {
              // good
            } else if (OB_ENTRY_EXIST == tmp_ret) {
              // bypass
            } else {
              err = OB_ERR_UNEXPECTED;
              LOG_WARN("fail to push back task", K(err));
            }
          }
        }
      }
      if (OB_SUCCESS != (err = gts_unit_distributor_.unit_migrate_finish())) {
        LOG_WARN("fail to check unit migrate finish", K(err));
      }
      if (OB_SUCCESS != (err = gts_unit_distributor_.check_shrink_resource_pool())) {
        LOG_WARN("fail to check shrink resource pool", K(err));
      }
      idling_.idle();
    }
  }
}

int ObRsGtsMonitor::check_gts_replica_enough_when_stop_server(
    const common::ObIArray<common::ObAddr>& servers_need_stopped, bool& can_stop)
{
  int ret = OB_SUCCESS;
  ObGtsTableOperator gts_table_operator;
  common::ObArray<common::ObGtsInfo> gts_info_array;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(servers_need_stopped.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(servers_need_stopped));
  } else if (OB_UNLIKELY(nullptr == sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy ptr is null, unexpected", K(ret), KP(sql_proxy_));
  } else if (OB_FAIL(gts_table_operator.init(sql_proxy_))) {
    LOG_WARN("fail to init gts table operator", K(ret));
  } else if (OB_FAIL(gts_table_operator.get_gts_infos(gts_info_array))) {
    LOG_WARN("fail to get gts info array", K(ret));
  } else {
    can_stop = true;
    for (int64_t i = 0; can_stop && OB_SUCC(ret) && i < gts_info_array.count(); ++i) {
      const ObGtsInfo& this_gts_info = gts_info_array.at(i);
      int64_t valid_m_cnt = 0;
      for (int64_t j = 0; OB_SUCC(ret) && j < this_gts_info.member_list_.get_member_number(); ++j) {
        common::ObAddr this_server;
        bool is_active = false;
        bool in_service = false;
        bool is_stopped = false;
        if (OB_FAIL(this_gts_info.member_list_.get_server_by_index(j, this_server))) {
          LOG_WARN("fail to get server by index", K(ret));
        } else if (has_exist_in_array(servers_need_stopped, this_server)) {
          // bypass
        } else if (OB_FAIL(server_mgr_.check_server_active(this_server, is_active))) {
          LOG_WARN("fail to check server active", K(ret), K(this_server));
        } else if (OB_FAIL(server_mgr_.check_in_service(this_server, in_service))) {
          LOG_WARN("fail to check in service", K(ret), K(this_server));
        } else if (OB_FAIL(server_mgr_.is_server_stopped(this_server, is_stopped))) {
          LOG_WARN("fail to check is server stopped", K(ret), K(this_server));
        } else if (is_active && in_service && !is_stopped) {
          ++valid_m_cnt;
        }
      }
      if (OB_FAIL(ret)) {
        // bypass
      } else if (valid_m_cnt < GTS_QUORUM - 1) {
        can_stop = false;
        LOG_INFO(
            "cannot stop server, since gts instance replica not enough", K(this_gts_info), K(servers_need_stopped));
      }
    }
  }
  return ret;
}

int ObRsGtsMonitor::check_gts_replica_enough_when_stop_zone(const common::ObZone& zone_need_stopped, bool& can_stop)
{
  int ret = OB_SUCCESS;
  ObGtsTableOperator gts_table_operator;
  common::ObArray<common::ObGtsInfo> gts_info_array;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(zone_need_stopped.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(zone_need_stopped));
  } else if (OB_UNLIKELY(nullptr == sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy ptr is null, unexpected", K(ret), KP(sql_proxy_));
  } else if (OB_FAIL(gts_table_operator.init(sql_proxy_))) {
    LOG_WARN("fail to init gts table operator", K(ret));
  } else if (OB_FAIL(gts_table_operator.get_gts_infos(gts_info_array))) {
    LOG_WARN("fail to get gts info array", K(ret));
  } else {
    can_stop = true;
    for (int64_t i = 0; can_stop && OB_SUCC(ret) && i < gts_info_array.count(); ++i) {
      const ObGtsInfo& this_gts_info = gts_info_array.at(i);
      int64_t valid_m_cnt = 0;
      for (int64_t j = 0; OB_SUCC(ret) && j < this_gts_info.member_list_.get_member_number(); ++j) {
        common::ObAddr this_server;
        common::ObZone this_zone;
        bool is_active = false;
        bool in_service = false;
        bool is_stopped = false;
        if (OB_FAIL(this_gts_info.member_list_.get_server_by_index(j, this_server))) {
          LOG_WARN("fail to get server by index", K(ret));
        } else if (OB_FAIL(server_mgr_.get_server_zone(this_server, this_zone))) {
          LOG_WARN("fail to get server zone", K(ret), K(this_server));
        } else if (this_zone == zone_need_stopped) {
          // bypass
        } else if (OB_FAIL(server_mgr_.check_server_active(this_server, is_active))) {
          LOG_WARN("fail to check server active", K(ret), K(this_server));
        } else if (OB_FAIL(server_mgr_.check_in_service(this_server, in_service))) {
          LOG_WARN("fail to check in service", K(ret), K(this_server));
        } else if (OB_FAIL(server_mgr_.is_server_stopped(this_server, is_stopped))) {
          LOG_WARN("fail to check is server stopped", K(ret), K(this_server));
        } else if (is_active && in_service && !is_stopped) {
          ++valid_m_cnt;
        }
      }
      if (OB_FAIL(ret)) {
        // bypass
      } else if (valid_m_cnt < GTS_QUORUM - 1) {
        can_stop = false;
        LOG_INFO("cannot stop zone, since gts instance replica not enough", K(this_gts_info), K(zone_need_stopped));
      }
    }
  }
  return ret;
}

void ObRsGtsMonitor::wakeup()
{
  if (OB_UNLIKELY(!inited_)) {
    LOG_ERROR("not init");
  } else {
    idling_.wakeup();
  }
}

void ObRsGtsMonitor::stop()
{
  if (OB_UNLIKELY(!inited_)) {
    LOG_ERROR("not init");
  } else {
    ObRsReentrantThread::stop();
    wakeup();
  }
}

}  // namespace rootserver
}  // namespace oceanbase
