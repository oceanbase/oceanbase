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
#include "ob_disaster_recovery_info.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_array_iterator.h"
#include "lib/container/ob_se_array.h"
#include "lib/container/ob_se_array_iterator.h"
#include "ob_unit_manager.h"
#include "ob_zone_manager.h"
#include "share/ob_all_server_tracer.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::rootserver;

int DRServerStatInfo::init(
    const common::ObAddr &server,
    const bool alive,
    const bool active,
    const bool permanent_offline,
    const bool block,
    const bool stopped)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(server));
  } else {
    server_ = server;
    alive_ = alive;
    active_ = active;
    permanent_offline_ = permanent_offline;
    block_ = block;
    stopped_ = stopped;
  }
  return ret;
}

int DRServerStatInfo::assign(
    const DRServerStatInfo &that)
{
  int ret = OB_SUCCESS;
  this->server_ = that.server_;
  this->alive_ = that.alive_;
  this->active_ = that.active_;
  this->permanent_offline_ = that.permanent_offline_;
  this->block_ = that.block_;
  this->stopped_ = that.stopped_;
  return ret;
}

int DRUnitStatInfo::init(
    const uint64_t unit_id,
    const bool in_pool,
    const share::ObUnitInfo &unit_info,
    DRServerStatInfo *server_stat,
    const int64_t outside_replica_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == unit_id
                  || !unit_info.is_valid()
                  || nullptr == server_stat)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(unit_id), K(unit_info), KP(server_stat));
  } else {
    unit_id_ = unit_id;
    in_pool_ = in_pool;
    if (OB_FAIL(unit_info_.assign(unit_info))) {
      LOG_WARN("fail to assign", KR(ret));
    } else {
      server_stat_ = server_stat;
      outside_replica_cnt_ = outside_replica_cnt;
    }
  }
  return ret;
}

int DRUnitStatInfo::assign(
    const DRUnitStatInfo &that)
{
  int ret = OB_SUCCESS;
  this->unit_id_ = that.unit_id_;
  this->in_pool_ = that.in_pool_;
  if (OB_FAIL(this->unit_info_.assign(that.unit_info_))) {
    LOG_WARN("fail to assign", KR(ret));
  } else {
    this->server_stat_ = that.server_stat_;
    this->outside_replica_cnt_ = that.outside_replica_cnt_;
  }
  return ret;
}

int DRLSInfo::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (is_meta_tenant(resource_tenant_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("resource tenant id shall be user/sys tenant", KR(ret), K(resource_tenant_id_));
  } else if (OB_UNLIKELY(nullptr == schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service ptr is null", KR(ret), KP(schema_service_));
  } else if (OB_FAIL(unit_stat_info_map_.init(
          UNIT_MAP_BUCKET_NUM))) {
    LOG_WARN("fail to init unit stat info map", KR(ret));
  } else if (OB_FAIL(server_stat_info_map_.init(
          SERVER_MAP_BUCKET_NUM))) {
    LOG_WARN("fail to init server stat info map", KR(ret));
  } else if (OB_FAIL(gather_server_unit_stat())) {
    LOG_WARN("fail to gather server unit stat", KR(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(
          OB_SYS_TENANT_ID, sys_schema_guard_))) {
    LOG_WARN("fail to get tenant schema guard", KR(ret));
  } else {
    inited_ = true;
  }
  return ret;
}

int DRLSInfo::get_tenant_id(
    uint64_t &tenant_id) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    tenant_id = ls_status_info_.tenant_id_;
  }
  return ret;
}

int DRLSInfo::get_ls_id(
    uint64_t &tenant_id,
    ObLSID &ls_id) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    tenant_id = ls_status_info_.tenant_id_;
    ls_id = ls_status_info_.ls_id_;
  }
  return ret;
}

int DRLSInfo::get_replica_cnt(
    int64_t &replica_cnt) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    const int64_t my_cnt = inner_ls_info_.replica_count();
    if (my_cnt != server_stat_array_.count()
        || my_cnt != unit_stat_array_.count()
        || my_cnt != unit_in_group_stat_array_.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("DRLS stat unexpected", KR(ret),
               "replica_count", my_cnt,
               "server_stat_count", server_stat_array_.count(),
               "unit_stat_count", unit_stat_array_.count(),
               "unit_in_group_stat_count", unit_in_group_stat_array_.count());
    } else {
      replica_cnt = my_cnt;
    }
  }
  return ret;
}

int DRLSInfo::append_replica_server_unit_stat(
    DRServerStatInfo *server_stat_info,
    DRUnitStatInfo *unit_stat_info,
    DRUnitStatInfo *unit_in_group_stat_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(server_stat_info)
             || OB_ISNULL(unit_stat_info)
             || OB_ISNULL(unit_in_group_stat_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument",
             KR(ret),
             KP(server_stat_info),
             KP(unit_stat_info),
             KP(unit_in_group_stat_info));
  } else {
    if (OB_FAIL(server_stat_array_.push_back(server_stat_info))) {
      LOG_WARN("fail to push back", KR(ret));
    } else if (OB_FAIL(unit_stat_array_.push_back(unit_stat_info))) {
      LOG_WARN("fail to push back", KR(ret));
    } else if (OB_FAIL(unit_in_group_stat_array_.push_back(unit_in_group_stat_info))) {
      LOG_WARN("fail to push back", KR(ret));
    }
  }
  return ret;
}

int DRLSInfo::get_replica_stat(
    const int64_t index,
    share::ObLSReplica *&ls_replica,
    DRServerStatInfo *&server_stat_info,
    DRUnitStatInfo *&unit_stat_info,
    DRUnitStatInfo *&unit_in_group_stat_info)
{
  int ret = OB_SUCCESS;
  int64_t replica_cnt = 0;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(get_replica_cnt(replica_cnt))) {
    LOG_WARN("fail to get replica cnt", KR(ret));
  } else if (OB_UNLIKELY(index >= replica_cnt || index < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(index), K(replica_cnt));
  } else {
    ls_replica = &(inner_ls_info_.get_replicas().at(index));
    server_stat_info = server_stat_array_.at(index);
    unit_stat_info = unit_stat_array_.at(index);
    unit_in_group_stat_info = unit_in_group_stat_array_.at(index);
  }
  return ret;
}

int DRLSInfo::get_ls_status_info(
    const share::ObLSStatusInfo *&ls_status_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    ls_status_info = &ls_status_info_;
  }
  return ret;
}

int DRLSInfo::get_inner_ls_info(share::ObLSInfo &inner_ls_info) const
{
  int ret = OB_SUCCESS;
  inner_ls_info.reset();
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!inner_ls_info_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K_(inner_ls_info));
  } else if (OB_FAIL(inner_ls_info.assign(inner_ls_info_))) {
    LOG_WARN("fail to assign ls info", KR(ret), K_(inner_ls_info));
  }
  return ret;
}

int DRLSInfo::fill_servers()
{
  int ret = OB_SUCCESS;
  common::ObZone zone;
  ObArray<ObServerInfoInTable> servers_info;
  if (OB_FAIL(SVR_TRACER.get_servers_info(zone, servers_info))) {
    LOG_WARN("fail to get all servers_info", KR(ret));
  } else if (OB_ISNULL(zone_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("zone_mgr_ is null", KR(ret), KP(zone_mgr_));
  } else {
    server_stat_info_map_.reuse();
    FOREACH_X(s, servers_info, OB_SUCC(ret)) {
      ServerStatInfoMap::Item *item = nullptr;
      bool zone_active = false;
      if (OB_ISNULL(s)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("server ptr is null", KR(ret));
      } else {
        const ObAddr &server = s->get_server();
        const ObZone &zone = s->get_zone();
        if (OB_FAIL(zone_mgr_->check_zone_active(zone, zone_active))) {
          LOG_WARN("fail to check zone active", KR(ret), "zone", zone);
        } else if (OB_FAIL(server_stat_info_map_.locate(server, item))) {
          LOG_WARN("fail to locate server status", KR(ret), "server", server);
        } else if (OB_UNLIKELY(nullptr == item)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("item ptr is null", KR(ret), "server", server);
        } else if (OB_FAIL(item->v_.init(
                server,
                s->is_alive(),
                s->is_active(),
                s->is_permanent_offline(),
                s->is_migrate_in_blocked(),
                (s->is_stopped() || !zone_active)))) {
          LOG_WARN("fail to init server item", KR(ret));
        }
      }
    }
  }
  return ret;
}

int DRLSInfo::fill_units()
{
  int ret = OB_SUCCESS;
  common::ObArray<share::ObUnitInfo> unit_info_array;
  if (OB_UNLIKELY(nullptr == unit_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unit mgr ptr is null", KR(ret), KP(unit_mgr_));
  } else if (OB_UNLIKELY(OB_INVALID_ID == resource_tenant_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(resource_tenant_id_));
  } else if (OB_FAIL(unit_mgr_->get_all_unit_infos_by_tenant(
          resource_tenant_id_, unit_info_array))) {
    LOG_WARN("fail to get all unit infos by tenant", KR(ret), K(resource_tenant_id_));
  } else {
    FOREACH_X(u, unit_info_array, OB_SUCC(ret)) {
      UnitStatInfoMap::Item *item = nullptr;
      ServerStatInfoMap::Item *server_item = nullptr;
      if (OB_UNLIKELY(nullptr == u)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("u ptr is null", KR(ret));
      } else if (OB_FAIL(unit_stat_info_map_.locate(u->unit_.unit_id_, item))) {
        LOG_WARN("fail to locate unit", KR(ret), "unit_id", u->unit_.unit_id_);
      } else if (OB_FAIL(server_stat_info_map_.locate(u->unit_.server_, server_item))) {
        LOG_WARN("fail to locate server", KR(ret), "server", u->unit_.server_);
      } else if (OB_UNLIKELY(nullptr == item || nullptr == server_item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("item ptr is null", KR(ret));
      } else if (OB_FAIL(item->v_.init(
              u->unit_.unit_id_,
              true, /*in pool*/
              *u,
              &server_item->v_,
              0 /*outside_replica_cnt*/))) {
        LOG_WARN("fail to init unit item", KR(ret));
      }
    }
  }
  return ret;
}

int DRLSInfo::gather_server_unit_stat()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(fill_servers())) {
    LOG_WARN("fail to fill servers", KR(ret));
  } else if (OB_FAIL(fill_units())) {
    LOG_WARN("fail to fill units", KR(ret));
  }
  return ret;
}

void DRLSInfo::reset_last_disaster_recovery_ls()
{
  zone_locality_array_.reset();
  inner_ls_info_.reset();
  ls_status_info_.reset();
  server_stat_array_.reset();
  unit_stat_array_.reset();
  unit_in_group_stat_array_.reset();
  schema_replica_cnt_ = 0;
  schema_full_replica_cnt_ = 0;
  member_list_cnt_ = 0;
  paxos_replica_number_ = 0;
  has_leader_ = false;
}

int DRLSInfo::construct_filtered_ls_info_to_use_(
    const share::ObLSInfo &input_ls_info,
    share::ObLSInfo &output_ls_info,
    const bool &filter_readonly_replicas_with_flag)
{
  int ret = OB_SUCCESS;
  output_ls_info.reset();
  const ObLSReplica *leader_replica = NULL;
  if (OB_UNLIKELY(!input_ls_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(input_ls_info));
  } else if (OB_FAIL(output_ls_info.init(
                         input_ls_info.get_tenant_id(),
                         input_ls_info.get_ls_id()))) {
    LOG_WARN("fail to init ls info", KR(ret), K(input_ls_info));
  } else if (OB_FAIL(input_ls_info.find_leader(leader_replica))) {
    LOG_WARN("fail to find leader replica in input_ls_info", KR(ret), K(input_ls_info));
  } else if (OB_ISNULL(leader_replica)) {
    ret = OB_LEADER_NOT_EXIST;
    LOG_WARN("no leader in input_ls_info", KR(ret), K(input_ls_info));
  } else {
    uint64_t tenant_id = input_ls_info.get_tenant_id();
    ObLSID ls_id = input_ls_info.get_ls_id();
    for (int64_t i = 0; OB_SUCC(ret) && i < input_ls_info.get_replicas().count(); i++) {
      const ObLSReplica &ls_replica = input_ls_info.get_replicas().at(i);
      // filter replicas not in member/learner list
      if (ls_replica.get_in_member_list()) {
        if (OB_FAIL(output_ls_info.add_replica(ls_replica))) {
          LOG_WARN("fail to add full replica to new ls_info", KR(ret), K(ls_replica));
        }
      } else if (ls_replica.get_in_learner_list()) {
        if (!filter_readonly_replicas_with_flag) {
          if (OB_FAIL(output_ls_info.add_replica(ls_replica))) {
            LOG_WARN("fail to add read only replica to new ls_info", KR(ret), K(ls_replica));
          }
        } else {
          // filter learner replicas with flag
          common::ObMember learner_in_learner_list;
          if (OB_FAIL(leader_replica->get_learner_list().get_learner_by_addr(ls_replica.get_server(), learner_in_learner_list))) {
            LOG_WARN("fail to get learner from leader learner_list", KR(ret), KPC(leader_replica), K(ls_replica));
          } else if (learner_in_learner_list.is_migrating()) {
            LOG_TRACE("replica is filtered because it is a garbage replica generated by migrating",
                      K(input_ls_info), K(ls_replica), KPC(leader_replica), K(learner_in_learner_list));
          } else if (OB_FAIL(output_ls_info.add_replica(ls_replica))) {
            LOG_WARN("fail to add read only replica to new ls_info", KR(ret), K(ls_replica));
          }
        }
      } else {
        LOG_TRACE("replica is filtered because it is not in member/learner list",
                  K(input_ls_info), K(ls_replica), KPC(leader_replica));
      }
    }
  }
  return ret;
}

int DRLSInfo::build_disaster_ls_info(
    const share::ObLSInfo &ls_info,
    const share::ObLSStatusInfo &ls_status_info,
    const bool &filter_readonly_replicas_with_flag)
{
  int ret = OB_SUCCESS;

  reset_last_disaster_recovery_ls();
  const share::schema::ObTenantSchema *tenant_schema = nullptr;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("DRWorker not init", KR(ret));
  } else if (OB_ISNULL(schema_service_) || OB_ISNULL(unit_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service ptr is null", KR(ret), KP(schema_service_), KP(unit_mgr_));
  } else if (resource_tenant_id_ != gen_user_tenant_id(ls_info.get_tenant_id())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant id not match", KR(ret), K(resource_tenant_id_),
             "ls_tenant_id", ls_info.get_tenant_id());
  } else if (OB_FAIL(construct_filtered_ls_info_to_use_(ls_info, inner_ls_info_, filter_readonly_replicas_with_flag))) {
    LOG_WARN("fail to filter not in member/learner list replicas and learner_with_flag replicas",
             KR(ret), K(ls_info), K(filter_readonly_replicas_with_flag));
  } else if (OB_FAIL(ls_status_info_.assign(ls_status_info))) {
    LOG_WARN("fail to assign ls_status_info", KR(ret));
  } else if (OB_FAIL(sys_schema_guard_.get_tenant_info(
          inner_ls_info_.get_tenant_id(),
          tenant_schema))) {
    LOG_WARN("fail to get tenant schema", KR(ret),
             "tenant_id", inner_ls_info_.get_tenant_id());
  } else if (OB_ISNULL(tenant_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant schema ptr is null", KR(ret),
             KP(tenant_schema), K(inner_ls_info_), K(ls_info));
  } else if (OB_FAIL(tenant_schema->get_zone_replica_attr_array_inherit(
          sys_schema_guard_,
          zone_locality_array_))) {
    LOG_WARN("fail to get zone replica attr array", KR(ret));
  } else if (OB_FAIL(tenant_schema->get_paxos_replica_num(
          sys_schema_guard_,
          schema_replica_cnt_))) {
    LOG_WARN("fail to get paxos replica num", KR(ret));
  } else if (FALSE_IT(schema_full_replica_cnt_ = tenant_schema->get_full_replica_num())) {
    // shall never be here
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < inner_ls_info_.get_replicas().count(); ++i) {
      ServerStatInfoMap::Item *server = nullptr;
      UnitStatInfoMap::Item *unit = nullptr;
      UnitStatInfoMap::Item *unit_in_group = nullptr;
      share::ObUnitInfo unit_info;
      share::ObLSReplica &ls_replica = inner_ls_info_.get_replicas().at(i);
      if (!ls_replica.get_in_member_list() && !ls_replica.get_in_learner_list()) {
        LOG_INFO("replica is neither in member list nor in learner list", K(ls_replica));
      } else if (OB_FAIL(server_stat_info_map_.locate(ls_replica.get_server(), server))) {
        LOG_WARN("fail to locate server", KR(ret), "server", ls_replica.get_server());
      } else if (OB_FAIL(unit_stat_info_map_.locate(ls_replica.get_unit_id(), unit))) {
        LOG_WARN("fail to locate unit", KR(ret), "unit_id", ls_replica.get_unit_id());
      } else {
        if (0 == ls_status_info.unit_group_id_) {
          unit_in_group = unit;
        } else if (OB_FAIL(unit_mgr_->get_unit_in_group(
                ls_replica.get_tenant_id(),
                ls_status_info.unit_group_id_,
                ls_replica.get_zone(),
                unit_info))) {
          LOG_WARN("fail to get unit in group", KR(ret), K(ls_replica));
        } else if (OB_FAIL(unit_stat_info_map_.locate(unit_info.unit_.unit_id_, unit_in_group))) {
          LOG_WARN("fail to locate unit", KR(ret), "unit_id", unit_info.unit_.unit_id_);
        }

        if (OB_SUCC(ret)) {
          if (OB_ISNULL(server) || OB_ISNULL(unit) || OB_ISNULL(unit_in_group)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unit or server ptr is null", KR(ret), KP(server), KP(unit), K(ls_replica));
          } else if (OB_FAIL(append_replica_server_unit_stat(
                  &server->v_, &unit->v_, &unit_in_group->v_))) {
            LOG_WARN("fail to append replica server/unit stat", KR(ret),
                     "server_stat_info", server->v_, "unit_stat_info", unit->v_);
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      const ObLSReplica *leader_replica = nullptr;
      int tmp_ret = inner_ls_info_.find_leader(leader_replica);
      if (OB_SUCCESS == tmp_ret && nullptr != leader_replica) {
        paxos_replica_number_ = leader_replica->get_paxos_replica_number();
        member_list_cnt_ = leader_replica->get_member_list().count();
        has_leader_ = true;
      }
    }
  }
  return ret;
}

int DRLSInfo::get_leader(
    common::ObAddr &leader_addr) const
{
  int ret = OB_SUCCESS;
  const ObLSReplica *leader_replica = nullptr;
  if (OB_FAIL(inner_ls_info_.find_leader(leader_replica))) {
    LOG_WARN("fail to find leader", KR(ret));
  } else if (OB_UNLIKELY(nullptr == leader_replica)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("leader replica ptr is null", KR(ret));
  } else {
    leader_addr = leader_replica->get_server();
  }
  return ret;
}

int DRLSInfo::get_leader_and_member_list(
    common::ObAddr &leader_addr,
    common::ObMemberList &member_list,
    GlobalLearnerList &learner_list)
{
  int ret = OB_SUCCESS;
  const ObLSReplica *leader_replica = nullptr;
  member_list.reset();
  learner_list.reset();
  if (OB_FAIL(inner_ls_info_.find_leader(leader_replica))) {
    LOG_WARN("fail to find leader", KR(ret));
  } else if (OB_ISNULL(leader_replica)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("leader replica ptr is null", KR(ret), KP(leader_replica));
  } else {
    leader_addr = leader_replica->get_server();
    // construct member list
    FOREACH_CNT_X(m, leader_replica->get_member_list(), OB_SUCC(ret)) {
      if (OB_ISNULL(m)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid SimpleMember", KR(ret), KP(m));
      } else if (OB_FAIL(member_list.add_member(ObMember(m->get_server(), m->get_timestamp())))) {
        LOG_WARN("fail to add server to member list", KR(ret), KPC(m));
      }
    }
    // construct learner list
    for (int64_t index = 0; OB_SUCC(ret) && index < leader_replica->get_learner_list().get_member_number(); ++index) {
      ObMember learner;
      if (OB_FAIL(leader_replica->get_learner_list().get_member_by_index(index, learner))) {
        LOG_WARN("fail to get learner by index", KR(ret), K(index));
      } else if (OB_FAIL(learner_list.add_learner(learner))) {
        LOG_WARN("fail to add learner to learner list", KR(ret), K(learner));
      }
    }
  }
  return ret;
}
