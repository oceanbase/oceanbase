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


#include "ob_system_admin_util.h"
#include "observer/ob_srv_network_frame.h"
#include "ob_root_service.h"
#include "logservice/leader_coordinator/table_accessor.h"
#include "rootserver/freeze/ob_major_freeze_helper.h"
#include "share/ob_cluster_event_history_table_operator.h"//CLUSTER_EVENT_INSTANCE
namespace oceanbase
{
using namespace common;
using namespace common::hash;
using namespace share;
using namespace share::schema;
using namespace obrpc;

namespace rootserver
{

int ObSystemAdminUtil::check_service() const
{
  int ret = OB_SUCCESS;
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    ret = ctx_.rs_status_->in_service()? OB_SUCCESS : OB_CANCELED;
  }
  return ret;
}

int ObAdminSwitchReplicaRole::execute(const ObAdminSwitchReplicaRoleArg &arg)
{
  LOG_INFO("execute switch replica role request", K(arg));
  ObArenaAllocator allocator(ObModIds::OB_RS_PARTITION_TABLE_TEMP);
  int ret = OB_SUCCESS;
  const ObLSID ls_id(arg.ls_id_);
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  ObLSInfo ls_info;
  auto get_tenant_id_by_name = [this](const ObAdminSwitchReplicaRoleArg &arg, uint64_t &tenant_id) -> int {
    int ret = OB_SUCCESS;
    ObSchemaGetterGuard schema_guard;
    ObString tenant_name;
    tenant_name.assign_ptr(arg.tenant_name_.ptr(),
        static_cast<int32_t>(strlen(arg.tenant_name_.ptr())));
    if (tenant_name.empty()) {
      tenant_id = OB_INVALID_TENANT_ID;
    } else if (OB_FAIL(ctx_.schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
      LOG_WARN("get schema manager failed", KR(ret));
    } else if (OB_FAIL(schema_guard.get_tenant_id(tenant_name, tenant_id))
        || OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
      ret = OB_TENANT_NOT_EXIST;
      LOG_WARN("tenant not exist", K(tenant_name), KR(ret));
    }
    return ret;
  };
  auto check_server_valid = [](const ObAddr &server) -> int {
    int ret = OB_SUCCESS;
    const char *columns[1] = {"status"};
    constexpr int64_t buffer_size = 128;
    char where_condition[buffer_size] = {0};
    char ip_str_buffer[buffer_size] = {0};
    if (!server.ip_to_string(ip_str_buffer, buffer_size)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ip to string failed", K(ip_str_buffer), K(server));
    } else {
      if (OB_FAIL(databuff_printf(where_condition, buffer_size, "where svr_ip='%s' and svr_port=%d", ip_str_buffer, server.get_port()))) {
        LOG_WARN("fail to create where confition", K(ip_str_buffer), K(server));
      } else {
        ObStringHolder server_status;
        if (OB_FAIL(ObTableAccessHelper::read_single_row(OB_SYS_TENANT_ID, columns, OB_ALL_SERVER_TNAME, where_condition, server_status))) {
          if (OB_ITER_END == ret) {
            ret = OB_ENTRY_NOT_EXIST;
            LOG_USER_ERROR(OB_ENTRY_NOT_EXIST, "server not in cluster");
            LOG_WARN("server not in __all_server table", K(server), KR(ret));
          } else {
            LOG_WARN("fail to read all_server table", K(server), KR(ret));
          }
        } else if (server_status.get_ob_string().compare("ACTIVE") != 0) {
          ret = OB_OP_NOT_ALLOW;
          LOG_USER_ERROR(OB_OP_NOT_ALLOW, "server not active");
          LOG_WARN("server status not valid", K(server), K(server_status));
        }
      }
    }
    return ret;
  };
  auto update_ls_election_reference_info_table = [](const ObAdminSwitchReplicaRoleArg &arg, const int64_t tenant_id, const ObLSInfo &info) -> int {
    int ret = OB_SUCCESS;
    ObSwitchLeaderArg switch_leader_arg(arg.ls_id_, arg.role_, tenant_id, arg.server_);
    const ObLSReplica *ls_replica = nullptr;
    if (switch_leader_arg.ls_id_ < 0 ||
        OB_INVALID_TENANT_ID == switch_leader_arg.tenant_id_ ||
        !switch_leader_arg.dest_server_.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(ret), K(switch_leader_arg));
    } else if (switch_leader_arg.role_ == ObRole::LEADER) {
      logservice::coordinator::LsElectionReferenceInfoRow row(tenant_id, share::ObLSID(arg.ls_id_));
      if (OB_FAIL(row.change_manual_leader(arg.server_))) {
        LOG_WARN("fail to change manual leader in __all_ls_election_reference_info", K(ret), K(arg));
      } else {
        LOG_INFO("successfully to change manual leader in __all_ls_election_reference_info", K(ret), K(arg));
      }
    } else if (switch_leader_arg.role_ == ObRole::FOLLOWER) {
      logservice::coordinator::LsElectionReferenceInfoRow row(tenant_id, share::ObLSID(arg.ls_id_));
      if (OB_FAIL(row.add_server_to_blacklist(arg.server_, logservice::coordinator::InsertElectionBlacklistReason::SWITCH_REPLICA))) {
        LOG_WARN("fail to add remove member info in __all_ls_election_reference_info", K(ret), K(arg));
      } else {
        LOG_INFO("successfully to add remove member info in __all_ls_election_reference_info", K(ret), K(arg));
      }
    } else if (switch_leader_arg.role_ == ObRole::INVALID_ROLE) {
      logservice::coordinator::LsElectionReferenceInfoRow row(tenant_id, share::ObLSID(arg.ls_id_));
      if (OB_FAIL(row.change_manual_leader(ObAddr()))) {
        LOG_WARN("fail to change manual leader in __all_ls_election_reference_info", K(ret), K(arg));
      } else if (OB_FAIL(row.delete_server_from_blacklist(arg.server_))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          LOG_WARN("fail to del remove member info in __all_ls_election_reference_info", K(ret), K(arg));
        } else {
          ret = OB_SUCCESS;
        }
      }
      if (OB_SUCC(ret)) {
        LOG_INFO("successfully to reset server status in __all_ls_election_reference_info", K(ret), K(arg));
      }
    }
    return ret;
  };
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), KR(ret));
  } else if (!ls_id.is_valid()) {// 表示需要改变server上或者zone中所有日志流的状态
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "switch server's role or zone's role");
  } else if (!arg.server_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("server must set", K(arg), KR(ret));
  } else if (OB_FAIL(check_server_valid(arg.server_))) {
    LOG_WARN("check server valid state failed", K(arg), KR(ret));
  } else if (OB_FAIL(get_tenant_id_by_name(arg, tenant_id))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      LOG_USER_ERROR(OB_ENTRY_NOT_EXIST, "invalid tenant");
    }
    LOG_WARN("fail to convert tenant name to id", K(arg), KR(ret));
  } else if (OB_ISNULL(GCTX.lst_operator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.lst_operator_ is NULL", K(arg), KR(ret), K(tenant_id));
  } else if (OB_FAIL(GCTX.lst_operator_->get(GCONF.cluster_id, tenant_id,
                     ls_id, share::ObLSTable::DEFAULT_MODE, ls_info))) {
    LOG_WARN("get ls info from GCTX.lst_operator_ failed", K(arg), KR(ret), K(tenant_id));
  } else if (OB_FAIL(update_ls_election_reference_info_table(arg, tenant_id, ls_info))) {
    LOG_WARN("fail to update ls election reference info", K(arg), KR(ret), K(tenant_id));
  } else {
    int tmp_ret = OB_SUCCESS;//ignore ret
    if (OB_TMP_FAIL(ObRootUtils::try_notify_switch_ls_leader(ctx_.rpc_proxy_, ls_info,
          obrpc::ObNotifySwitchLeaderArg::SwitchLeaderComment::MANUAL_SWITCH))) {
      LOG_WARN("failed to notify switch ls leader", KR(ret), K(ls_info));
    }
  }
  LOG_INFO("switch leader done", KR(ret), K(arg), K(tenant_id), K(ls_info));
  return ret;
}

int ObAdminSwitchReplicaRole::alloc_tenant_id_set(common::hash::ObHashSet<uint64_t> &tenant_id_set)
{
  int ret = OB_SUCCESS;
  if (tenant_id_set.created()) {
    if(OB_FAIL(tenant_id_set.clear())) {
      LOG_WARN("clear tenant id set failed", KR(ret));
    }
  } else if (OB_FAIL(tenant_id_set.create(TENANT_BUCKET_NUM))) {
    LOG_WARN("create tenant id set failed", LITERAL_K(TENANT_BUCKET_NUM), KR(ret));
  }
  return ret;
}

template<typename T>
int ObAdminSwitchReplicaRole::convert_set_to_array(const common::hash::ObHashSet<T> &set,
    ObArray<T> &array)
{
  int ret = common::OB_SUCCESS;
  array.reuse();
  if (!set.created()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("set not created", "set created", set.created(), KR(ret));
  } else if (OB_FAIL(array.reserve(set.size()))) {
    LOG_WARN("array reserver failed", "capacity", set.size(), KR(ret));
  } else {
    for (typename common::hash::ObHashSet<T>::const_iterator iter = set.begin();
        OB_SUCCESS == ret && iter != set.end(); ++iter) {
      if (OB_FAIL(array.push_back(iter->first))) {
        LOG_WARN("push_back failed", KR(ret));
      }
    }
  }
  return ret;
}

int ObAdminSwitchReplicaRole::get_tenants_of_zone(const ObZone &zone,
    common::hash::ObHashSet<uint64_t> &tenant_id_set)
{
  int ret = OB_SUCCESS;
  ObArray<ObAddr> server_array;
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (zone.is_empty() || !tenant_id_set.created()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(zone),
        "tenant_id_set created", tenant_id_set.created(), KR(ret));
  } else if (OB_FAIL(SVR_TRACER.get_alive_servers(zone, server_array))) {
    LOG_WARN("get alive servers failed", K(zone), KR(ret));
  } else {
    FOREACH_CNT_X(server, server_array, OB_SUCCESS == ret) {
      if (OB_FAIL(ctx_.unit_mgr_->get_tenants_of_server(*server, tenant_id_set))) {
        LOG_WARN("get tenants of server failed", "server", *server, KR(ret));
      }
    }
  }

  return ret;
}

int ObAdminSwitchReplicaRole::get_switch_replica_tenants(const ObZone &zone, const ObAddr &server,
    const uint64_t &tenant_id, ObArray<uint64_t> &tenant_ids)
{
  int ret = OB_SUCCESS;
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (zone.is_empty() && !server.is_valid() && OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("zone, server and tenant_id are all invalid",
        K(zone), K(server), K(tenant_id), KR(ret));
  } else if (OB_INVALID_ID != tenant_id) {
    if (OB_FAIL(tenant_ids.push_back(tenant_id))) {
      LOG_WARN("push back tenant id failed", KR(ret));
    }
  } else if (server.is_valid() || !zone.is_empty()) {
    ObHashSet<uint64_t> tenant_id_set;
    if (OB_FAIL(alloc_tenant_id_set(tenant_id_set))) {
      LOG_WARN("alloc tenant id set failed", KR(ret));
    } else {
      if (server.is_valid()) {
        if (OB_FAIL(ctx_.unit_mgr_->get_tenants_of_server(server, tenant_id_set))) {
          LOG_WARN("get tenants of server failed", K(server), KR(ret));
        }
      } else {
        if (OB_FAIL(get_tenants_of_zone(zone, tenant_id_set))) {
          LOG_WARN("get tenants of zone failed", K(zone), KR(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(convert_set_to_array(tenant_id_set, tenant_ids))) {
        LOG_WARN("convert set to array failed", KR(ret));
      }
    }
  }

  return ret;
}

int ObAdminCallServer::get_server_list(const ObServerZoneArg &arg, ObIArray<ObAddr> &server_list)
{
  int ret = OB_SUCCESS;
  server_list.reset();
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), KR(ret));
  } else if (arg.server_.is_valid()) {
    bool is_alive = false;
    if (OB_FAIL(SVR_TRACER.check_server_alive(arg.server_, is_alive))) {
      LOG_WARN("fail to check server alive", KR(ret), "server", arg.server_);
    } else if (!is_alive) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("server is not alive", KR(ret), "server", arg.server_);
    } else if (OB_FAIL(server_list.push_back(arg.server_))) {
      LOG_WARN("push back server failed", KR(ret));
    }
  } else {
    bool zone_exist = true;
    if (!arg.zone_.is_empty() && OB_FAIL(ctx_.zone_mgr_->check_zone_exist(arg.zone_, zone_exist))) {
      LOG_WARN("fail to check zone exist", KR(ret));
    } else if (!zone_exist) {
      ret = OB_ZONE_INFO_NOT_EXIST;
      LOG_WARN("zone info not exist", KR(ret), K(arg.zone_));
    } else if (OB_FAIL(SVR_TRACER.get_alive_servers(arg.zone_, server_list))) {
      LOG_WARN("get alive servers failed", KR(ret), K(arg));
    }
  }
  return ret;
}

int ObAdminCallServer::call_all(const ObServerZoneArg &arg)
{
  int ret = OB_SUCCESS;
  ObArray<ObAddr> server_list;
  if (OB_FAIL(get_server_list(arg, server_list))) {
    LOG_WARN("get server list failed", K(ret), K(arg));
  } else {
    FOREACH_CNT(server, server_list) {
      int tmp_ret = call_server(*server);
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("call server failed", KR(ret), "server", *server);
        ret = OB_SUCCESS == ret ? tmp_ret : ret;
      }
    }
  }
  return ret;
}

int ObAdminReportReplica::execute(const obrpc::ObAdminReportReplicaArg &arg)
{
  LOG_INFO("execute report request", K(arg));
  int ret = OB_SUCCESS;
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), KR(ret));
  } else if (OB_FAIL(call_all(arg))) {
    LOG_WARN("execute report replica failed", KR(ret), K(arg));
  }
  return ret;
}

int ObAdminReportReplica::call_server(const ObAddr &server)
{
  int ret = OB_SUCCESS;
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), KR(ret));
  } else if (OB_FAIL(ctx_.rpc_proxy_->to(server).report_replica())) {
    LOG_WARN("request server report replica failed", KR(ret), K(server));
  }
  return ret;
}

int ObAdminRecycleReplica::execute(const obrpc::ObAdminRecycleReplicaArg &arg)
{
  LOG_INFO("execute recycle request", K(arg));
  int ret = OB_SUCCESS;
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), KR(ret));
  } else if (OB_FAIL(call_all(arg))) {
    LOG_WARN("execute recycle replica failed", KR(ret), K(arg));
  }
  return ret;
}

int ObAdminRecycleReplica::call_server(const ObAddr &server)
{
  int ret = OB_SUCCESS;
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), KR(ret));
  } else if (OB_FAIL(ctx_.rpc_proxy_->to(server).recycle_replica())) {
    LOG_WARN("request server recycle replica failed", KR(ret), K(server));
  }
  return ret;
}

int ObAdminClearLocationCache::execute(const obrpc::ObAdminClearLocationCacheArg &arg)
{
  LOG_INFO("execute clear location cache request", K(arg));
  int ret = OB_SUCCESS;
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), KR(ret));
  } else if (OB_FAIL(call_all(arg))) {
    LOG_WARN("execute clear location cache failed", KR(ret), K(arg));
  }
  return ret;
}

int ObAdminClearLocationCache::call_server(const ObAddr &server)
{
  int ret = OB_SUCCESS;
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), KR(ret));
  } else if (OB_FAIL(ctx_.rpc_proxy_->to(server).clear_location_cache())) {
    LOG_WARN("request clear location cache failed", KR(ret), K(server));
  }
  return ret;
}

int ObAdminReloadUnit::execute()
{
  LOG_INFO("execute reload unit request");
  int ret = OB_SUCCESS;
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(ctx_.unit_mgr_->load())) {
    LOG_WARN("unit manager load failed", KR(ret));
  }
  LOG_INFO("finish execute reload unit request", KR(ret));
  return ret;
}

int ObAdminReloadServer::execute()
{
  LOG_INFO("execute reload server request");
  int ret = OB_SUCCESS;
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(ctx_.server_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx_.server_mgr_ is null", KR(ret), KP(ctx_.server_mgr_));
  } else if (OB_FAIL(ctx_.server_mgr_->load_server_manager())) {
    LOG_WARN("build server status failed", KR(ret));
  }
  return ret;
}

int ObAdminReloadZone::execute()
{
  LOG_INFO("execute reload zone request");
  int ret = OB_SUCCESS;
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(ctx_.zone_mgr_->reload())) {
    LOG_ERROR("zone manager reload failed", KR(ret));
  }
  return ret;
}

int ObAdminClearMergeError::execute(const obrpc::ObAdminMergeArg &arg)
{
  LOG_INFO("execute clear merge error request", K(arg));
  int ret = OB_SUCCESS;
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), KR(ret));
  } else {
    ObTenantAdminMergeParam param;
    param.transport_ = GCTX.net_frame_->get_req_transport();
    if (arg.affect_all_ || arg.affect_all_user_ || arg.affect_all_meta_) {
      if ((true == arg.affect_all_ && true == arg.affect_all_user_) ||
          (true == arg.affect_all_ && true == arg.affect_all_meta_) ||
          (true == arg.affect_all_user_ && true == arg.affect_all_meta_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("only one of affect_all,affect_all_user,affect_all_meta can be true",
                 KR(ret), "affect_all", arg.affect_all_, "affect_all_user", 
                 arg.affect_all_user_, "affect_all_meta", arg.affect_all_meta_);
      } else {
        if (arg.affect_all_) {
          param.need_all_ = true;
        } else if (arg.affect_all_user_) {
          param.need_all_user_ = true;
        } else {
          param.need_all_meta_ = true;
        }
      }
    } else if (OB_FAIL(param.tenant_array_.assign(arg.tenant_ids_))) {
      LOG_WARN("fail to assign tenant_ids", KR(ret), K(arg));
    }
    if (FAILEDx(ObMajorFreezeHelper::clear_merge_error(param))) {
      LOG_WARN("fail to clear merge error", KR(ret), K(param));
    }
  }
  return ret;
}

int ObAdminZoneFastRecovery::execute(const obrpc::ObAdminRecoveryArg &arg)
{
  LOG_INFO("execute zone fast recovery admin request", K(arg));
  int ret = OB_SUCCESS;
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), KR(ret));
  } else {
    switch (arg.type_) {
      case ObAdminRecoveryArg::SUSPEND_RECOVERY:
        if (OB_FAIL(ctx_.zone_mgr_->update_recovery_status(
                arg.zone_, share::ObZoneInfo::RECOVERY_STATUS_SUSPEND))) {
          LOG_WARN("fail to update zone fast recovery status", KR(ret));
        }
        break;
      case ObAdminRecoveryArg::RESUME_RECOVERY:
        if (OB_FAIL(ctx_.zone_mgr_->update_recovery_status(
                arg.zone_, share::ObZoneInfo::RECOVERY_STATUS_NORMAL))) {
          LOG_WARN("fail to update zone fast recovery status", KR(ret));
        }
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("arg type unexpected", KR(ret), "type", arg.type_);
        break;
    }
  }
  return ret;
}

int ObAdminMerge::execute(const obrpc::ObAdminMergeArg &arg)
{
  LOG_INFO("execute merge admin request", K(arg));
  int ret = OB_SUCCESS;
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), KR(ret));
  } else {
    switch(arg.type_) {
      case ObAdminMergeArg::START_MERGE: {
        /* if (OB_FAIL(ctx_.daily_merge_scheduler_->manual_start_merge(arg.zone_))) {
          LOG_WARN("start merge zone failed", K(ret), K(arg));
        }*/
        break;
      }
      case ObAdminMergeArg::SUSPEND_MERGE: {
        ObTenantAdminMergeParam param;
        param.transport_ = GCTX.net_frame_->get_req_transport();
        if (arg.affect_all_ || arg.affect_all_user_ || arg.affect_all_meta_) {
          if ((true == arg.affect_all_ && true == arg.affect_all_user_) ||
              (true == arg.affect_all_ && true == arg.affect_all_meta_) ||
              (true == arg.affect_all_user_ && true == arg.affect_all_meta_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("only one of affect_all,affect_all_user,affect_all_meta can be true",
                     KR(ret), "affect_all", arg.affect_all_, "affect_all_user", 
                     arg.affect_all_user_, "affect_all_meta", arg.affect_all_meta_);
          } else {
            if (arg.affect_all_) {
              param.need_all_ = true;
            } else if (arg.affect_all_user_) {
              param.need_all_user_ = true;
            } else {
              param.need_all_meta_ = true;
            }
          }
        } else if (OB_FAIL(param.tenant_array_.assign(arg.tenant_ids_))) {
          LOG_WARN("fail to assign tenant_ids", KR(ret), K(arg));
        }
        if (FAILEDx(ObMajorFreezeHelper::suspend_merge(param))) {
          LOG_WARN("fail to suspend merge", KR(ret), K(param));
        }
        break;
      }
      case ObAdminMergeArg::RESUME_MERGE: {
        ObTenantAdminMergeParam param;
        param.transport_ = GCTX.net_frame_->get_req_transport();
        if (arg.affect_all_ || arg.affect_all_user_ || arg.affect_all_meta_) {
          if ((true == arg.affect_all_ && true == arg.affect_all_user_) ||
              (true == arg.affect_all_ && true == arg.affect_all_meta_) ||
              (true == arg.affect_all_user_ && true == arg.affect_all_meta_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("only one of affect_all,affect_all_user,affect_all_meta can be true",
                     KR(ret), "affect_all", arg.affect_all_, "affect_all_user", 
                     arg.affect_all_user_, "affect_all_meta", arg.affect_all_meta_);
          } else {
            if (arg.affect_all_) {
              param.need_all_ = true;
            } else if (arg.affect_all_user_) {
              param.need_all_user_ = true;
            } else {
              param.need_all_meta_ = true;
            }
          }
        } else if (OB_FAIL(param.tenant_array_.assign(arg.tenant_ids_))) {
          LOG_WARN("fail to assign tenant_ids", KR(ret), K(arg));
        }
        if (FAILEDx(ObMajorFreezeHelper::resume_merge(param))) {
          LOG_WARN("fail to resume merge", KR(ret), K(param));
        }
        break;
      }
      default: {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("unsupported merge admin type", "type", arg.type_, KR(ret));
      }
    }
  }
  return ret;
}

int ObAdminClearRoottable::execute(const obrpc::ObAdminClearRoottableArg &arg)
{
  int ret = OB_NOT_SUPPORTED;
  UNUSED(arg);
  return ret;
}

//FIXME: flush schemas of all tenants
int ObAdminRefreshSchema::execute(const obrpc::ObAdminRefreshSchemaArg &arg)
{
  LOG_INFO("execute refresh schema", K(arg));
  int ret = OB_SUCCESS;
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), KR(ret));
  } else if (OB_FAIL(ctx_.ddl_service_->refresh_schema(OB_SYS_TENANT_ID))) {
    LOG_WARN("refresh schema failed", KR(ret));
  } else {
    if (OB_FAIL(ctx_.schema_service_->get_tenant_schema_version(OB_SYS_TENANT_ID, schema_version_))) {
      LOG_WARN("fail to get schema version", KR(ret));
     } else if (OB_FAIL(ctx_.schema_service_->get_refresh_schema_info(schema_info_))) {
       LOG_WARN("fail to get refresh schema info", KR(ret), K(schema_info_));
     } else if (!schema_info_.is_valid()) {
       schema_info_.set_schema_version(schema_version_);
     }
     if (OB_FAIL(ret)) {
     } else if (OB_FAIL(call_all(arg))) {
      LOG_WARN("execute notify refresh schema failed", KR(ret), K(arg));
    }
  }
  return ret;
}

int ObAdminRefreshSchema::call_server(const ObAddr &server)
{
  int ret = OB_SUCCESS;
  ObTimeoutCtx ctx;
  if (OB_UNLIKELY(!ctx_.is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", KR(ret), K(server));
  } else if (OB_ISNULL(GCTX.srv_rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.srv_rpc_proxy_ is null", KR(ret));
  } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, GCONF.rpc_timeout))) {
    LOG_WARN("fail to set timeout ctx", KR(ret));
  } else {
    ObSwitchSchemaArg arg;
    arg.schema_info_ = schema_info_;
    ObArray<int> return_code_array;
    ObSwitchSchemaProxy proxy(*GCTX.srv_rpc_proxy_, &ObSrvRpcProxy::switch_schema);
    int tmp_ret = OB_SUCCESS;
    const int64_t timeout_ts = ctx.get_timeout(0);
    if (OB_FAIL(proxy.call(server, timeout_ts, arg))) {
      LOG_WARN("notify switch schema failed", KR(ret), K(server), K_(schema_version), K_(schema_info));
    }
    if (OB_TMP_FAIL(proxy.wait_all(return_code_array))) {
      ret = OB_SUCC(ret) ? tmp_ret : ret;
      LOG_WARN("fail to wait all", KR(ret), KR(tmp_ret), K(server));
    } else if (OB_FAIL(ret)) {
    } else if (OB_FAIL(proxy.check_return_cnt(return_code_array.count()))) {
      LOG_WARN("fail to check return cnt", KR(ret), K(server), "return_cnt", return_code_array.count());
    } else if (OB_UNLIKELY(1 != return_code_array.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("return_code_array count shoud be 1", KR(ret), K(server), "return_cnt", return_code_array.count());
    } else {
      ret = return_code_array.at(0);
    }
  }
  return ret;
}

int ObAdminRefreshMemStat::execute(const ObAdminRefreshMemStatArg &arg)
{
  LOG_INFO("execute refresh memory stat");
  int ret = OB_SUCCESS;
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(call_all(arg))) {
   LOG_WARN("execute notify refresh memory stat failed", KR(ret));
  }
  return ret;
}

int ObAdminRefreshMemStat::call_server(const ObAddr &server)
{
  int ret = OB_SUCCESS;
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), KR(ret));
  } else if (OB_FAIL(ctx_.rpc_proxy_->to(server).refresh_memory_stat())) {
    LOG_WARN("notify refresh memory stat failed", KR(ret), K(server));
  }
  return ret;
}

int ObAdminWashMemFragmentation::execute(const ObAdminWashMemFragmentationArg &arg)
{
  LOG_INFO("execute sync wash fragment");
  int ret = OB_SUCCESS;
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(call_all(arg))) {
    LOG_WARN("execute notify sync wash fragment failed", K(ret));
  }
  return ret;
}

int ObAdminWashMemFragmentation::call_server(const ObAddr &server)
{
  int ret = OB_SUCCESS;
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), K(ret));
  } else if (OB_FAIL(ctx_.rpc_proxy_->to(server).wash_memory_fragmentation())) {
    LOG_WARN("notify sync wash fragment failed", K(ret), K(server));
  }
  return ret;
}

int ObAdminSetConfig::verify_config(obrpc::ObAdminSetConfigArg &arg)
{
  int ret = OB_SUCCESS;
  void *ptr = nullptr, *cfg_ptr = nullptr;
  ObServerConfigChecker *cfg = nullptr;
  ObTenantConfigChecker *tenant_cfg = nullptr;
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), KR(ret));
  }
  FOREACH_X(item, arg.items_, OB_SUCCESS == ret) {
    if (item->name_.is_empty()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("empty config name", "item", *item, KR(ret));
    } else {
      ObConfigItem *ci = nullptr;
      ObString config_name(item->name_.size(), item->name_.ptr());
      bool is_default_table_organization_config = (0 == config_name.case_compare(DEFAULT_TABLE_ORGANIZATION));
      if (OB_SYS_TENANT_ID != item->exec_tenant_id_ || item->tenant_name_.size() > 0) {
        // tenants(user or sys tenants) modify tenant level configuration
        item->want_to_set_tenant_config_ = true;
        if (nullptr == tenant_cfg) {
          if (OB_ISNULL(cfg_ptr = ob_malloc(sizeof(ObTenantConfigChecker),
                        ObModIds::OB_RS_PARTITION_TABLE_TEMP))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to alloc memory", KR(ret));
          } else if (OB_ISNULL(tenant_cfg = new (cfg_ptr) ObTenantConfigChecker())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("new tenant_cfg failed", KR(ret));
          }
        } // if

        if (OB_SUCC(ret)) {
          ObConfigItem * const *ci_ptr = tenant_cfg->get_container().get(
                                          ObConfigStringKey(item->name_.ptr()));
          if (OB_ISNULL(ci_ptr)) {
            ret = OB_ERR_SYS_CONFIG_UNKNOWN;
            LOG_WARN("can't found config item", KR(ret), "item", *item);
          } else {
            ci = *ci_ptr;
            share::schema::ObSchemaGetterGuard schema_guard;
            const char *const NAME_ALL = "all";
            const char *const NAME_ALL_USER = "all_user";
            const char *const NAME_ALL_META = "all_meta";
            if (OB_ISNULL(GCTX.schema_service_)) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("invalid argument", KR(ret), KP(GCTX.schema_service_));
            } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
              LOG_WARN("fail to get sys tenant schema guard", KR(ret));
            } else if (OB_SYS_TENANT_ID == item->exec_tenant_id_ &&
                      (0 == item->tenant_name_.str().case_compare(NAME_ALL) ||
                       0 == item->tenant_name_.str().case_compare(NAME_ALL_USER) ||
                       0 == item->tenant_name_.str().case_compare(NAME_ALL_META))) {
              common::ObArray<uint64_t> tenant_ids;
              if (OB_FAIL(schema_guard.get_tenant_ids(tenant_ids))) {
                LOG_WARN("get_tenant_ids failed", KR(ret));
              } else {
                using FUNC_TYPE = bool (*) (const uint64_t);
                FUNC_TYPE condition_func = nullptr;
                if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_2_1_0) {
                  if (0 == item->tenant_name_.str().case_compare(NAME_ALL_USER) || 
                      0 == item->tenant_name_.str().case_compare(NAME_ALL_META)) {
                    ret = OB_NOT_SUPPORTED;
                    LOG_WARN("all_user/all_meta are not supported when min_cluster_version is less than 4.2.1.0", 
                             KR(ret), "tenant_name", item->tenant_name_);
                  } else {
                    condition_func = is_not_virtual_tenant_id;
                  }
                } else {
                  if (0 == item->tenant_name_.str().case_compare(NAME_ALL) || 
                      0 == item->tenant_name_.str().case_compare(NAME_ALL_USER)) {
                    condition_func = is_user_tenant;
                  } else {
                    condition_func = is_meta_tenant;
                  }
                }
                if (OB_SUCC(ret) && (nullptr != condition_func)) {
                  const ObTenantSchema *tenant_schema = nullptr;
                  for (const uint64_t tenant_id: tenant_ids) {
                    if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_schema))) {
                      LOG_WARN("fail to get tenant info", KR(ret), K(tenant_id));
                    } else if (OB_ISNULL(tenant_schema)) {
                      ret = OB_ERR_UNEXPECTED;
                      LOG_WARN("tenant_schema is null", KR(ret), K(tenant_id));
                    } else if (condition_func(tenant_id) &&
                              (is_default_table_organization_config ? !tenant_schema->is_oracle_tenant() : true) &&
                              OB_FAIL(item->tenant_ids_.push_back(tenant_id))) {
                      LOG_WARN("add tenant_id failed", K(tenant_id), KR(ret));
                      break;
                    }
                  } // for
                }
              }
            } else if (OB_SYS_TENANT_ID == item->exec_tenant_id_
                       && item->tenant_name_ == ObFixedLengthString<common::OB_MAX_TENANT_NAME_LENGTH + 1>("seed")) {
              uint64_t tenant_id = OB_PARAMETER_SEED_ID;
              if (OB_FAIL(item->tenant_ids_.push_back(tenant_id))) {
                LOG_WARN("add seed tenant_id failed", KR(ret));
                break;
              }
            } else {
              uint64_t tenant_id = OB_INVALID_TENANT_ID;
              const ObTenantSchema *tenant_schema = nullptr;
              if (OB_SYS_TENANT_ID != item->exec_tenant_id_) {
                tenant_id = item->exec_tenant_id_;
              } else {
                if (OB_FAIL(schema_guard.get_tenant_id(
                                   ObString(item->tenant_name_.ptr()), tenant_id))
                                   || OB_INVALID_ID == tenant_id) {
                  ret = OB_ERR_INVALID_TENANT_NAME;
                  LOG_WARN("get_tenant_id failed", KR(ret), "tenant", item->tenant_name_);
                }
              }
              if (OB_FAIL(ret)) {
              } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_schema))) {
                LOG_WARN("fail to get tenant info", KR(ret), K(tenant_id));
              } else if (OB_ISNULL(tenant_schema)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("tenant_schema is null", KR(ret), K(tenant_id));
              } else if ((is_default_table_organization_config ? 
                          !tenant_schema->is_oracle_tenant() && OB_SYS_TENANT_ID != tenant_id
                          : true) && OB_FAIL(item->tenant_ids_.push_back(tenant_id))) {
                LOG_WARN("add tenant_id failed", K(tenant_id), KR(ret));
              }
            } // else
          } // else
        } // if
      } else {
        // sys tenant try to modify configration(cluster level or sys tenant level)
        if (nullptr == cfg) {
          if (OB_ISNULL(ptr = ob_malloc(sizeof(ObServerConfigChecker),
                                      ObModIds::OB_RS_PARTITION_TABLE_TEMP))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to alloc memory", KR(ret));
          } else if (OB_ISNULL(cfg = new (ptr) ObServerConfigChecker)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("new cfg failed", KR(ret));
          }
        } // if
        if (OB_SUCC(ret) && nullptr == tenant_cfg) {
          if (OB_ISNULL(cfg_ptr = ob_malloc(sizeof(ObTenantConfigChecker),
                        ObModIds::OB_RS_PARTITION_TABLE_TEMP))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to alloc memory", KR(ret));
          } else if (OB_ISNULL(tenant_cfg = new (cfg_ptr) ObTenantConfigChecker())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("new tenant_cfg failed", KR(ret));
          }
        } // if

        if (OB_SUCC(ret)) {
          ObConfigItem * const *sys_ci_ptr = cfg->get_container().get(
                                             ObConfigStringKey(item->name_.ptr()));
          ObConfigItem * const *tenant_ci_ptr = tenant_cfg->get_container().get(
                                                ObConfigStringKey(item->name_.ptr()));
          if (OB_NOT_NULL(sys_ci_ptr)) {
            ci = *sys_ci_ptr;
          } else if (OB_NOT_NULL(tenant_ci_ptr)) {
            ci = *tenant_ci_ptr;
            item->want_to_set_tenant_config_ = true;
            if (is_default_table_organization_config) {
              // sys tenant try to modify default_table_organization
            } else if (OB_FAIL(item->tenant_ids_.push_back(OB_SYS_TENANT_ID))) {
              LOG_WARN("add tenant_id failed", KR(ret));
            }
          } else {
            ret = OB_ERR_SYS_CONFIG_UNKNOWN;
            LOG_WARN("can't found config item", KR(ret), "item", *item);
          }
        } // if
      } // else

      if (OB_SUCC(ret)) {
        const char *err = NULL;
        if (ci->is_not_editable() && !arg.is_inner_) {
          ret = OB_INVALID_CONFIG; //TODO: specific report not editable
          LOG_WARN("config is not editable", "item", *item, KR(ret));
        } else if (!ci->check_unit(item->value_.ptr())) {
          ret = OB_INVALID_CONFIG;
          LOG_ERROR("invalid config", "item", *item, KR(ret));
        } else if (!ci->set_value_unsafe(item->value_.ptr())) {
          ret = OB_INVALID_CONFIG;
          LOG_WARN("invalid config", "item", *item, KR(ret));
        } else if (!ci->check()) {
          ret = OB_INVALID_CONFIG;
          LOG_WARN("invalid value range", "item", *item, KR(ret));
        } else if (!ctx_.root_service_->check_config(*ci, err)) {
          ret = OB_INVALID_CONFIG;
          LOG_WARN("invalid value range", "item", *item, KR(ret));
        }
        if (OB_FAIL(ret)) {
          if (nullptr != err) {
            LOG_USER_ERROR(OB_INVALID_CONFIG, err);
          }
        }
      } // if
    } // else
  } // FOREACH_X

  if (nullptr != cfg) {
    cfg->~ObServerConfigChecker();
    ob_free(cfg);
    cfg = nullptr;
    ptr = nullptr;
  } else if (nullptr != ptr) {
    ob_free(ptr);
    ptr = nullptr;
  }
  if (nullptr != tenant_cfg) {
    tenant_cfg->~ObTenantConfigChecker();
    ob_free(tenant_cfg);
    tenant_cfg = nullptr;
    cfg_ptr = nullptr;
  } else if (nullptr != cfg_ptr) {
    ob_free(cfg_ptr);
    cfg_ptr = nullptr;
  }
  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_UPDATE_MIN_CONFIG_VERSION_ERROR);
int ObAdminSetConfig::update_config(obrpc::ObAdminSetConfigArg &arg, int64_t new_version)
{
  int ret = OB_SUCCESS;
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), KR(ret));
  } else if (OB_ISNULL(GCTX.config_mgr_)
             || OB_ISNULL(GCTX.sql_proxy_)
             || OB_ISNULL(ctx_.zone_mgr_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(GCTX.config_mgr_),
             KP(GCTX.sql_proxy_), KP(ctx_.zone_mgr_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < arg.items_.count(); ++i) {
      const ObAdminSetConfigItem &item = arg.items_.at(i);
      char svr_ip[OB_MAX_SERVER_ADDR_SIZE] = "ANY";
      int64_t svr_port = 0;
      if (item.server_.is_valid()) {
        if (false == item.server_.ip_to_string(svr_ip, sizeof(svr_ip))) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("convert server addr to ip failed", KR(ret), "server", item.server_);
        } else {
          svr_port = item.server_.get_port();
          ObAddr addr;
          bool is_server_exist = false;
          if (false == addr.set_ip_addr(svr_ip, static_cast<int32_t>(svr_port))){
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("set addr fail", KR(ret), "svr_ip", svr_ip, K(svr_port));
          } else if (OB_FAIL(SVR_TRACER.is_server_exist(addr, is_server_exist))) {
            LOG_WARN("check server exist fail", K(addr));
          } else if (!is_server_exist) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("server is not exist", KR(ret), K(addr));
            LOG_USER_ERROR(OB_INVALID_ARGUMENT, "server");
          }
        } // else
      } // if

      if (OB_FAIL(ret)) {
      } else if (!item.zone_.is_empty()) {
        HEAP_VAR(ObZoneInfo, zone_info) {
          zone_info.zone_ = item.zone_;
          if (OB_FAIL(ObZoneTableOperation::load_zone_info(*GCTX.sql_proxy_, zone_info, true/*check_zone_exist*/))) {
            LOG_WARN("fail to get zone info", KR(ret), K(zone_info));
            if (OB_ZONE_INFO_NOT_EXIST == ret) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("zone is not exist", KR(ret), "zone", item.zone_, K(zone_info));
              LOG_USER_ERROR(OB_INVALID_ARGUMENT, "zone");
            }
          }
        }
      }

      if (OB_FAIL(ret)) {
      } else {
        if (OB_UNLIKELY(ERRSIM_UPDATE_MIN_CONFIG_VERSION_ERROR)) {
          LOG_INFO("ERRSIM here, new config version is setted to 0");
          new_version = 1;
        } else {
          new_version = std::max(new_version + 1, ObTimeUtility::current_time());
        }
      }
      if (OB_FAIL(ret)) {
      } else if (item.want_to_set_tenant_config_) {
        // try update tenant config
        if (OB_FAIL(update_tenant_config_(item, svr_ip, svr_port, new_version))) {
          LOG_WARN("fail to update tenant config", KR(ret), K(item), K(svr_ip), K(svr_port), K(new_version));
        }
      } else {
        // try update sys config
        if (OB_FAIL(update_sys_config_(item, svr_ip, svr_port, new_version))) {
          LOG_WARN("fail to update sys config", KR(ret), K(item), K(svr_ip), K(svr_port), K(new_version));
        }
      }
    } // end for each item
  }

  return ret;
}

int ObAdminSetConfig::update_tenant_config_(
    const obrpc::ObAdminSetConfigItem &item,
    const char *svr_ip,
    const int64_t svr_port,
    const int64_t new_version)
{
  int ret = OB_SUCCESS;
  share::schema::ObSchemaGetterGuard schema_guard;
  const share::schema::ObSimpleTenantSchema *tenant_schema = NULL;
  if (OB_UNLIKELY(!item.want_to_set_tenant_config_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("not expected to update tenant config", KR(ret), K(item));
  } else if (OB_ISNULL(svr_ip)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(svr_ip));
  } else if (OB_FAIL(GSCHEMASERVICE.get_tenant_schema_guard(
                 OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("fail to get sys tenant schema guard", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < item.tenant_ids_.count(); ++i) {
      uint64_t tenant_id = item.tenant_ids_.at(i);
      const char *table_name = (ObAdminSetConfig::OB_PARAMETER_SEED_ID == tenant_id ?
                                OB_ALL_SEED_PARAMETER_TNAME : OB_TENANT_PARAMETER_TNAME);
      tenant_id = (ObAdminSetConfig::OB_PARAMETER_SEED_ID == tenant_id ? OB_SYS_TENANT_ID : tenant_id);
      uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id);
      if (OB_FAIL(schema_guard.get_tenant_info(exec_tenant_id, tenant_schema))) {
        LOG_WARN("failed to get tenant ids", KR(ret), K(exec_tenant_id));
      } else if (OB_ISNULL(tenant_schema)) {
        ret = OB_TENANT_NOT_EXIST;
        LOG_WARN("tenant not exist", KR(ret), K(tenant_id));
      } else if (!tenant_schema->is_normal()) {
        //tenant not normal, maybe tenant not ready, cannot add tenant config
        LOG_INFO("tenant is not normal in schema, just skip", K(tenant_id), KPC(tenant_schema));
      } else if (OB_FAIL(inner_update_tenant_config_(
                             item, tenant_id, table_name, svr_ip, svr_port, new_version))) {
        LOG_WARN("fail to inner update tenant config", KR(ret), K(item), K(tenant_id),
                 K(table_name), K(svr_ip), K(svr_port), K(new_version));
      }
      // set config_version to config_version_map and trigger parameter update
      if (OB_FAIL(ret)) {
      } else if (ObAdminSetConfig::OB_PARAMETER_SEED_ID == tenant_id) {
      } else if (OB_FAIL(OTC_MGR.set_tenant_config_version(tenant_id,
                                                           new_version))) {
        LOG_WARN("failed to set tenant config version", K(tenant_id), KR(ret),
                 K(item), K(new_version));
      } else if (GCTX.omt_->has_tenant(tenant_id) &&
                 OB_FAIL(OTC_MGR.got_version(tenant_id, new_version))) {
        LOG_WARN("failed to got version", K(tenant_id), KR(ret), K(item));
      } else {
        LOG_INFO("got new tenant config version", K(new_version), K(tenant_id), K(item));
      }
    } // end for each tenant
    // try to broadcast config is changed to all server
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(construct_arg_and_broadcast_tenant_config_map())) {
      LOG_WARN("fail to construct arg and broadcast tenant config map", KR(ret));
    }
  }
  return ret;
}

int ObAdminSetConfig::inner_update_tenant_config_(
    const obrpc::ObAdminSetConfigItem &item,
    const uint64_t tenant_id,
    const char *table_name,
    const char *svr_ip,
    const int64_t svr_port,
    const int64_t new_version)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  ObConfigItem *ci = nullptr;
  // tenant not exist in RS, use SYS instead
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(OB_SYS_TENANT_ID));
  if (OB_UNLIKELY(!item.want_to_set_tenant_config_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("not expected to update tenant config", KR(ret), K(item));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))
             || OB_UNLIKELY(0 != ObString(table_name).case_compare(OB_ALL_SEED_PARAMETER_TNAME)
                            && 0 != ObString(table_name).case_compare(OB_TENANT_PARAMETER_TNAME))
             || OB_ISNULL(svr_ip)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(table_name), KP(svr_ip));
  } else if (!tenant_config.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get tenant config", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(tenant_config->get_container().get(
                          ObConfigStringKey(item.name_.ptr())))) {
    ret = OB_ERR_SYS_CONFIG_UNKNOWN;
    LOG_WARN("can't found config item", KR(ret), K(tenant_id), K(item));
  } else {
    ci = *(tenant_config->get_container().get(
                          ObConfigStringKey(item.name_.ptr())));
    const ObString compatible_cfg(COMPATIBLE);
    if (OB_FAIL(build_dml_before_update_(tenant_id, item, *ci, svr_ip, svr_port,
                    table_name, new_version, dml))) {
      LOG_WARN("fail to build dml", KR(ret), K(tenant_id), K(item), K(svr_ip), K(svr_port),
               K(table_name), K(new_version));
    } else if (0 == compatible_cfg.case_compare(item.name_.ptr())) {
      // update config named compatible
      if (OB_FAIL(inner_update_tenant_config_for_compatible_(
              tenant_id, &item, svr_ip, svr_port, table_name, dml, new_version))) {
        LOG_WARN("fail to update compatible", KR(ret), K(tenant_id),
                 K(svr_ip), K(svr_port), K(table_name), K(new_version));
      }
    } else {
      // update config named not compatible
      if (OB_FAIL(inner_update_tenant_config_for_others_(
              tenant_id, svr_ip, svr_port, item, table_name, dml, new_version))) {
        LOG_WARN("fail to update other configs", KR(ret), K(tenant_id),
                 K(svr_ip), K(svr_port), K(table_name), K(new_version));
      }
    }
  }
  return ret;
}

int ObAdminSetConfig::inner_update_tenant_config_for_others_(
    const uint64_t tenant_id,
    const char *svr_ip,
    const uint64_t svr_port,
    const obrpc::ObAdminSetConfigItem &item,
    const char *table_name,
    share::ObDMLSqlSplicer &dml,
    const uint64_t new_version)
{
  ObMySQLTransaction trans;
  int ret = OB_SUCCESS;
  uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id);
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))
      || OB_UNLIKELY(!is_valid_tenant_id(exec_tenant_id))
      || OB_ISNULL(GCTX.sql_proxy_)
      || OB_ISNULL(svr_ip)
      || OB_ISNULL(table_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(exec_tenant_id), KP(GCTX.sql_proxy_),
             KP(svr_ip), KP(table_name));
  } else if (OB_FAIL(trans.start(GCTX.sql_proxy_, exec_tenant_id))) {
    LOG_WARN("fail to start trans", KR(ret), K(exec_tenant_id));
  } else if (OB_FAIL(check_with_lock_before_update_(
                         trans, svr_ip, svr_port, tenant_id, exec_tenant_id,
                         item, table_name, new_version))) {
    LOG_WARN("fail to lock line and check vonfig version", KR(ret), K(svr_ip),
             K(tenant_id), K(exec_tenant_id), K(svr_port), K(item), K(table_name), K(new_version));
  } else {
    int64_t affected_rows = 0;
    ObDMLExecHelper exec(trans, exec_tenant_id);
    if (OB_FAIL(exec.exec_insert_update(table_name, dml, affected_rows))) {
      LOG_WARN("execute insert update failed", K(tenant_id), KR(ret), K(item));
    } else if (is_zero_row(affected_rows) || affected_rows > 2) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected affected rows", K(tenant_id), K(affected_rows), KR(ret));
    }
  }
  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(trans.end(OB_SUCC(ret)))) {
      LOG_WARN("trans end failed", KR(tmp_ret), K(ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }
  return ret;
}

int ObAdminSetConfig::update_sys_config_(
    const obrpc::ObAdminSetConfigItem &item,
    const char *svr_ip,
    const int64_t svr_port,
    const int64_t new_version)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  const char *table_name = OB_ALL_SYS_PARAMETER_TNAME;
  ObConfigItem *const *ci_ptr = GCONF.get_container().get(
                                   ObConfigStringKey(item.name_.ptr()));
  ObMySQLTransaction trans;
  if (OB_UNLIKELY(item.want_to_set_tenant_config_)
      || OB_UNLIKELY(OB_SYS_TENANT_ID != item.exec_tenant_id_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("not expected to update tenant config", KR(ret), K(item));
  } else if (OB_ISNULL(svr_ip)
             || OB_ISNULL(GCTX.sql_proxy_)
             || OB_ISNULL(ctx_.zone_mgr_)
             || OB_ISNULL(GCTX.config_mgr_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(svr_ip), KP(GCTX.sql_proxy_),
             KP(ctx_.zone_mgr_), KP(GCTX.config_mgr_));
  } else if (OB_ISNULL(ci_ptr)) {
    ret = OB_ERR_SYS_CONFIG_UNKNOWN;
    LOG_WARN("can't found config item", KR(ret), K(item));
  } else if (OB_FAIL(build_dml_before_update_(
                       OB_SYS_TENANT_ID, item, **ci_ptr, svr_ip, svr_port,
                       table_name, new_version, dml))) {
    LOG_WARN("fail to build dml", KR(ret), K(item), K(svr_ip),
             K(svr_port), KP(table_name), K(new_version));
  } else if (OB_FAIL(trans.start(GCTX.sql_proxy_, OB_SYS_TENANT_ID))) {
    LOG_WARN("fail to start trans", KR(ret));
  } else if (OB_FAIL(check_with_lock_before_update_(
                         trans, svr_ip, svr_port, OB_SYS_TENANT_ID, OB_SYS_TENANT_ID/*exec_tenant_id*/,
                         item, table_name, new_version))) {
    LOG_WARN("fail to lock line and check vonfig version", KR(ret), K(svr_ip),
             K(svr_port), K(item), K(table_name), K(new_version));
  } else {
    int64_t affected_rows = 0;
    ObDMLExecHelper exec(trans, OB_SYS_TENANT_ID);
    if (OB_FAIL(exec.exec_insert_update(table_name, dml, affected_rows))) {
      LOG_WARN("execute insert update failed", KR(ret), K(item));
    } else if (is_zero_row(affected_rows) || affected_rows > 2) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected affected rows", K(affected_rows), KR(ret));
    // try to update table directly and try best to update infos in memory
    // The reason why to reload zone_manager is to make sure lease_info_version is newest in memory,
    // old heartbeat renew_lease will send lease_info_version in zone_manager to other observers
    // to trigger config-update
    } else if (OB_FAIL(ObZoneTableOperation::update_global_config_version_with_lease(trans, new_version))) {
      LOG_WARN("fail to update global config version with lease", KR(ret), K(new_version));
    }
  }
  if (trans.is_started()) {
    if (OB_TMP_FAIL(trans.end(OB_SUCC(ret)))) {
      LOG_WARN("trans end failed", KR(tmp_ret), K(ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }
  // try update zone manager to let heartbeat response trigger remote observer refresh config
  // ignore ret because RS may not in service now
  if (OB_FAIL(ret)) {
  } else if (OB_TMP_FAIL(ctx_.zone_mgr_->reload())) {
    LOG_WARN("fail to reload zone manager", KR(tmp_ret));
  }
  // try update local memory and trigger remote server to refresh this change
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(GCTX.config_mgr_->got_version(new_version))) {
    LOG_WARN("config mgr got version failed", KR(ret), K(new_version));
  } else {
    LOG_INFO("got new sys config version", K(new_version), K(item));
  }
  // try broadcast sys config version is changed
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(construct_arg_and_broadcast_global_config_version_(new_version))) {
    LOG_WARN("fail to construct arg and broadcast global config version",
             KR(ret), K(new_version));
  } else {
    LOG_INFO("success to broadcast global config version", KR(ret), K(new_version));
  }
  return ret;
}

int ObAdminSetConfig::build_dml_before_update_(
    const uint64_t tenant_id,
    const obrpc::ObAdminSetConfigItem &item,
    const ObConfigItem &config_item,
    const char *svr_ip,
    const int64_t svr_port,
    const char *table_name,
    const int64_t new_version,
    share::ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  dml.reset();
  if (OB_ISNULL(svr_ip)
      || OB_UNLIKELY(!is_valid_tenant_id(tenant_id))
      || OB_ISNULL(table_name)
      || OB_UNLIKELY(0 != ObString(table_name).case_compare(OB_ALL_SEED_PARAMETER_TNAME)
                     && 0 != ObString(table_name).case_compare(OB_TENANT_PARAMETER_TNAME)
                     && 0 != ObString(table_name).case_compare(OB_ALL_SYS_PARAMETER_TNAME))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id), K(item), KP(svr_ip), K(svr_port),
             KP(table_name), K(new_version));
  } else if (0 == ObString(table_name).case_compare(OB_TENANT_PARAMETER_TNAME)
             && OB_FAIL(dml.add_pk_column("tenant_id", tenant_id))) {
    // __all_seed_parameteri and all_sys_parameter does not have column 'tenant_id'
    LOG_WARN("add column failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(dml.add_pk_column("zone", item.zone_.ptr()))
            || OB_FAIL(dml.add_pk_column("svr_type", print_server_role(OB_SERVER)))
            || OB_FAIL(dml.add_pk_column("svr_ip", svr_ip))
            || OB_FAIL(dml.add_pk_column("svr_port", svr_port))
            || OB_FAIL(dml.add_pk_column("name", item.name_.ptr()))
            || OB_FAIL(dml.add_column("value", item.value_.ptr()))
            || OB_FAIL(dml.add_column("info", item.comment_.ptr()))
            || OB_FAIL(dml.add_column("config_version", new_version))) {
    LOG_WARN("add column failed", KR(ret), K(item), K(new_version), K(svr_ip), K(svr_port));
  } else if (OB_FAIL(dml.get_values().append_fmt("usec_to_time(%ld)", new_version))) {
    LOG_WARN("append valued failed", KR(ret), K(new_version));
  } else if (OB_FAIL(dml.add_column(false, "gmt_modified"))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.add_column("section", config_item.section()))
             || OB_FAIL(dml.add_column("scope", config_item.scope()))
             || OB_FAIL(dml.add_column("source", config_item.source()))
             || OB_FAIL(dml.add_column("edit_level", config_item.edit_level()))
             || OB_FAIL(dml.add_column("data_type", config_item.data_type()))) {
    LOG_WARN("add column failed", KR(ret));
  }
  return ret;
}

int ObAdminSetConfig::check_with_lock_before_update_(
    ObMySQLTransaction &trans,
    const char *svr_ip,
    const int64_t svr_port,
    const uint64_t tenant_id,
    const uint64_t exec_tenant_id,
    const obrpc::ObAdminSetConfigItem &item,
    const char *table_name,
    const int64_t new_version)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(svr_ip)
      || OB_ISNULL(table_name)
      || OB_UNLIKELY(!is_valid_tenant_id(tenant_id))
      || OB_UNLIKELY(!is_valid_tenant_id(exec_tenant_id))
      || OB_UNLIKELY(0 != ObString(table_name).case_compare(OB_ALL_SEED_PARAMETER_TNAME)
                     && 0 != ObString(table_name).case_compare(OB_TENANT_PARAMETER_TNAME)
                     && 0 != ObString(table_name).case_compare(OB_ALL_SYS_PARAMETER_TNAME))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id), K(exec_tenant_id), K(item),
             KP(svr_ip), K(svr_port), KP(table_name), K(new_version));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ObSqlString sql_string;
      ObSqlString sub_string;
      sqlclient::ObMySQLResult *result = NULL;
      if (0 == ObString(table_name).case_compare(OB_TENANT_PARAMETER_TNAME)
          && sub_string.assign_fmt("AND tenant_id = %lu ", tenant_id)) {
        LOG_WARN("fail to build sub sql string", KR(ret), K(tenant_id), K(table_name));
      } else if (OB_FAIL(sql_string.assign_fmt(
              "SELECT config_version AS current_config_version "
              "FROM %s "
              "WHERE zone = '%s' AND svr_type = '%s' AND svr_ip = '%s' "
              "AND svr_port = %ld AND name = '%s' %.*s"
              "FOR UPDATE",
              table_name, item.zone_.ptr(), print_server_role(OB_SERVER),
              svr_ip, svr_port, item.name_.ptr(), static_cast<int>(sub_string.length()), sub_string.ptr()))) {
        LOG_WARN("assign sql string failed", KR(ret), K(item), K(table_name), K(svr_ip),
                 K(svr_port), K(sub_string));
      } else if (OB_FAIL(trans.read(res, exec_tenant_id, sql_string.ptr()))) {
        LOG_WARN("fail to execute sql", KR(ret), K(exec_tenant_id), K(sql_string));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get sql result", KR(ret), KP(result));
      } else {
        if (OB_FAIL(result->next())) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("get result next failed", KR(ret), K(sql_string));
          }
        } else {
          uint64_t current_config_version = 0;
          EXTRACT_INT_FIELD_MYSQL(*result, "current_config_version", current_config_version, uint64_t);
          if (OB_FAIL(ret)) {
            LOG_WARN("failed to get result", KR(ret), K(sql_string));
          } else if (new_version <= current_config_version) {
            ret = OB_NEED_RETRY;
            LOG_WARN("config_version is promoted already, no need to update", K(tenant_id), K(item),
                     K(current_config_version), K(new_version));
          }
        }
      }
    }
  }
  return ret;
}

int ObAdminSetConfig::construct_arg_and_broadcast_tenant_config_map()
{
  int ret = OB_SUCCESS;
  share::ObLeaseResponse tmp_lease_response;
  ObBroadcastConfigVersionArg broadcast_arg;
  if (OB_FAIL(OTC_MGR.get_lease_response(tmp_lease_response))) {
    LOG_WARN("fail to get lease response", KR(ret));
  } else if (OB_FAIL(broadcast_arg.init_by_tenant_config_version_map(tmp_lease_response.tenant_config_version_))) {
    LOG_WARN("fail to construct boradcast arg by tenant config version map", KR(ret), K(tmp_lease_response));
  } else if (OB_FAIL(broadcast_config_version_(broadcast_arg))) {
    LOG_WARN("fail to broadcast config version to all servers", KR(ret), K(broadcast_arg));
  }
  return ret;
}

int ObAdminSetConfig::construct_arg_and_broadcast_global_config_version_(
    const int64_t new_version)
{
  int ret = OB_SUCCESS;
  ObBroadcastConfigVersionArg broadcast_arg;
  if (OB_UNLIKELY(0 >= new_version)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(new_version));
  } else if (OB_FAIL(broadcast_arg.init_by_global_config_version(new_version))) {
    LOG_WARN("fail to construct broadcast arg", KR(ret), K(new_version));
  } else if (OB_FAIL(broadcast_config_version_(broadcast_arg))) {
    LOG_WARN("fail to broadcast config version to all servers", KR(ret), K(broadcast_arg));
  }
  return ret;
}

int ObAdminSetConfig::broadcast_config_version_(
    const obrpc::ObBroadcastConfigVersionArg &broadcast_arg)
{
  LOG_INFO("try to broadcast config version", K(broadcast_arg));
  int ret = OB_SUCCESS;
  ObZone empty_zone; // to get all server
  common::ObArray<common::ObAddr> server_list;
  common::ObArray<std::pair<common::ObAddr, int>> failed_list;
  const int64_t rpc_timeout = GCONF.rpc_timeout;
  int tmp_ret = OB_SUCCESS;
  int64_t start_time_us = ObTimeUtility::current_time();

  if (OB_UNLIKELY(!broadcast_arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(broadcast_arg));
  } else if (OB_FAIL(SVR_TRACER.get_servers_of_zone(empty_zone, server_list))) {
    LOG_WARN("fail to get all server from SVR_TRACER", KR(ret), K(empty_zone));
  } else if (OB_ISNULL(GCTX.srv_rpc_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(GCTX.srv_rpc_proxy_));
  } else {
    for (int64_t index = 0; index < server_list.count(); ++index) {
      if (OB_SUCCESS != (tmp_ret = GCTX.srv_rpc_proxy_->to(server_list.at(index)).by(OB_SYS_TENANT_ID)
                                                       .timeout(rpc_timeout).broadcast_config_version(broadcast_arg))) {
        LOG_WARN("fail to broadcast config version", KR(tmp_ret), K(server_list.at(index)), K(index), K(broadcast_arg));
        if (OB_SUCCESS != (tmp_ret = failed_list.push_back(std::make_pair(server_list.at(index), tmp_ret)))) {
          LOG_WARN("fail to add failed infos", KR(tmp_ret), K(server_list.at(index)), K(index));
        }
      } else {
        LOG_INFO("success to broadcast config version", K(server_list.at(index)), K(broadcast_arg));
      }
    }
  }
  int64_t cost_time_us = ObTimeUtility::current_time() - start_time_us;
  LOG_INFO("finish broadcast config version and lease info version", K(broadcast_arg),
           K(cost_time_us), "total_count", server_list.count(),
           "failed_count", failed_list.count(), K(failed_list));
  return ret;
}

int ObAdminSetConfig::inner_update_tenant_config_for_compatible_(
    const uint64_t tenant_id,
    const obrpc::ObAdminSetConfigItem *item,
    const char *svr_ip,
    const int64_t svr_port,
    const char *table_name,
    share::ObDMLSqlSplicer &dml,
    const int64_t new_version)
{
  int ret = OB_SUCCESS;
  bool need_to_update = true;
  const uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id);
  ObMySQLTransaction trans;
  if (OB_ISNULL(item) || OB_ISNULL(svr_ip) || OB_ISNULL(table_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(tenant_id), K(item), K(svr_ip), K(svr_port),
             K(table_name), K(new_version));
  } else if (OB_FAIL(trans.start(ctx_.sql_proxy_, exec_tenant_id))) {
    LOG_WARN("fail to start trans", KR(ret), K(exec_tenant_id));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ObSqlString sql_string;
      sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(sql_string.assign_fmt(
              "SELECT value as compatible FROM %s WHERE tenant_id = %lu "
              "and "
              "zone = '%s' and svr_type = '%s' and svr_ip = '%s' "
              "and "
              "svr_port = %ld and name = '%s' FOR UPDATE",
              table_name, tenant_id, item->zone_.ptr(),
              print_server_role(OB_SERVER), svr_ip, svr_port, COMPATIBLE))) {
        LOG_WARN("assign sql string failed", K(ret));
      } else if (OB_FAIL(trans.read(res, exec_tenant_id, sql_string.ptr()))) {
        LOG_WARN("fail to execute sql", K(ret), K(exec_tenant_id),
                 K(sql_string));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get sql result", K(ret), KP(result));
      } else {
        if (OB_FAIL(result->next())) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("get result next failed", KR(ret), K(sql_string));
          }
        } else {
          ObString old_compatible_str;
          uint64_t old_compatible_val = 0;
          uint64_t new_compatible_val = 0;
          EXTRACT_VARCHAR_FIELD_MYSQL(*result, "compatible",
                                      old_compatible_str);
          if (OB_FAIL(ret)) {
            LOG_WARN("failed to get result", KR(ret), K(sql_string));
          } else if (OB_FAIL(ObClusterVersion::get_version(
                         old_compatible_str, old_compatible_val))) {
            LOG_WARN("parse version failed", KR(ret), K(old_compatible_str));
          } else if (OB_FAIL(ObClusterVersion::get_version(
                         item->value_.ptr(), new_compatible_val))) {
            LOG_WARN("parse version failed", KR(ret), K(item->value_.ptr()));
          } else if (new_compatible_val <= old_compatible_val) {
            need_to_update = false;
            LOG_INFO("[COMPATIBLE] [DATA_VERSION] no need to update", K(tenant_id), 
                     "old_data_version", DVP(old_compatible_val),
                     "new_data_version", DVP(new_compatible_val));
          }
        }
      }
    }
  }
  if (OB_SUCC(ret) && need_to_update) {
    int64_t affected_rows = 0;
    ObDMLExecHelper exec(trans, exec_tenant_id);
    if (OB_FAIL(exec.exec_insert_update(table_name, dml, affected_rows))) {
      LOG_WARN("execute insert update failed", K(tenant_id), KR(ret), "item",
               *item);
    } else if (is_zero_row(affected_rows) || affected_rows > 2) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected affected rows", K(tenant_id), K(affected_rows),
               KR(ret));
    }
  }
  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(trans.end(OB_SUCC(ret)))) {
      LOG_WARN("trans end failed", KR(tmp_ret), K(ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }

  return ret;
}

int ObAdminSetConfig::execute(obrpc::ObAdminSetConfigArg &arg)
{
  LOG_INFO("execute set config request", K(arg));
  DEBUG_SYNC(BEFORE_EXECUTE_ADMIN_SET_CONFIG);
  int ret = OB_SUCCESS;
  int64_t config_version = 0;
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!arg.is_valid() || OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), KR(ret), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(verify_config(arg))) {
    LOG_WARN("verify config failed", KR(ret), K(arg));
  } else {
    HEAP_VAR(ObGlobalInfo, global_info) {
      if (OB_FAIL(ObZoneTableOperation::load_global_info(*(GCTX.sql_proxy_), global_info))) {
        LOG_WARN("fail to load global info from __all_zone", KR(ret));
      } else if (FALSE_IT(config_version = global_info.config_version_)) {
        // shall never be here
      } else if (OB_FAIL(ctx_.root_service_->set_config_pre_hook(arg))) {
        LOG_WARN("fail to process pre hook", K(arg), KR(ret));
      } else if (OB_FAIL(update_config(arg, config_version))) {
        LOG_WARN("update config failed", KR(ret), K(arg));
      } else if (OB_ISNULL(ctx_.root_service_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error inner stat", KR(ret), K(ctx_.root_service_));
      } else if (OB_FAIL(ctx_.root_service_->set_config_post_hook(arg))) {
        LOG_WARN("fail to set config callback", KR(ret));
      } else {
        LOG_INFO("set config succ", K(arg));
      }
    }
  }
  return ret;
}

int ObAdminMigrateUnit::execute(const ObAdminMigrateUnitArg &arg)
{
  int ret = OB_SUCCESS;
  LOG_INFO("execute migrate unit request", K(arg));
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), KR(ret));
  } else {
    const uint64_t unit_id = arg.unit_id_;
    const ObAddr &dst = arg.destination_;
    if (OB_FAIL(ctx_.unit_mgr_->admin_migrate_unit(unit_id, dst, arg.is_cancel_))) {
      LOG_WARN("migrate unit failed", K(unit_id), K(dst), KR(ret));
    } else {
      ctx_.root_balancer_->wakeup();
    }
  }
  return ret;
}

int ObAdminUpgradeVirtualSchema::execute()
{
  int ret = OB_SUCCESS;
  LOG_INFO("execute upgrade virtual schema request");
  int64_t upgrade_cnt = 0;
  ObSchemaGetterGuard schema_guard;
  ObArray<uint64_t> tenant_ids;
  if (OB_UNLIKELY(!ctx_.is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(ctx_.root_inspection_) || OB_ISNULL(ctx_.ddl_service_) || OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is null", KR(ret), KP(ctx_.root_inspection_), KP(ctx_.ddl_service_), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(ctx_.ddl_service_->get_tenant_schema_guard_with_version_in_inner_table(
             OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("get_schema_guard failed", KR(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_ids(tenant_ids))) {
    LOG_WARN("fail to get tenant ids", KR(ret));
  } else {
    share::ObTenantRole tenant_role;
    FOREACH(tenant_id, tenant_ids) { // ignore ret
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(ObAllTenantInfoProxy::get_tenant_role(GCTX.sql_proxy_, *tenant_id, tenant_role))) {
        LOG_WARN("fail to get tenant role", KR(ret), KP(GCTX.sql_proxy_), K(*tenant_id));
      } else if (tenant_role.is_invalid()) {
        tmp_ret = OB_NEED_WAIT;
        LOG_WARN("tenant role is not ready, need wait", KR(ret), K(*tenant_id), KR(tmp_ret), K(tenant_role));
      } else if (tenant_role.is_restore()) {
        tmp_ret = OB_OP_NOT_ALLOW;
        LOG_WARN("restore tenant cannot upgrade virtual schema", KR(ret), K(*tenant_id), KR(tmp_ret), K(tenant_role));
      } else if (tenant_role.is_standby()) {
        // skip
      } else if (tenant_role.is_primary()) {
        if (OB_TMP_FAIL(execute(*tenant_id, upgrade_cnt))) {
          LOG_WARN("fail to execute upgrade virtual table by tenant", KR(ret), K(*tenant_id), KR(tmp_ret), K(tenant_role));
        }
      } else {
        // Currently, clone tenant is not available, but it may be added later.
        tmp_ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unknown tenant_role", KR(ret), K(*tenant_id), KR(tmp_ret), K(tenant_role));
      }
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }
  if (OB_SUCC(ret) && upgrade_cnt > 0) {
    // if schema upgraded, inspect schema again
    int tmp_ret = ctx_.root_inspection_->check_all();
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("root inspection failed", KR(tmp_ret));
    }
  }
  return ret;
}

int ObAdminUpgradeVirtualSchema::execute(
    const uint64_t tenant_id,
    int64_t &upgrade_cnt)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t start_ts = ObTimeUtility::current_time();
  ObArrayArray<ObTableSchema> hard_code_tables;
  if (OB_UNLIKELY(!ctx_.is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(
             is_virtual_tenant_id(tenant_id)
             || OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(ctx_.root_inspection_)
             || OB_ISNULL(ctx_.ddl_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is null", KR(ret), KP(ctx_.root_inspection_), KP(ctx_.ddl_service_));
  } else if (OB_FAIL(generate_hard_code_schemas_(tenant_id, hard_code_tables))) {
    LOG_WARN("failed to generate all hard code tables", KR(ret), K(tenant_id));
  } else if (OB_FAIL(drop_not_exist_tables_(tenant_id, hard_code_tables))) {
    LOG_WARN("failed to drop not exist tables", KR(ret), K(tenant_id));
  }
  // upgrade tables in hard code tables
  for (int64_t i = 0; OB_SUCC(ret) && i < hard_code_tables.count(); i++) {
    ObIArray<ObTableSchema> &tables = hard_code_tables.at(i);
    if (OB_FAIL(batch_upgrade_(tenant_id, tables, upgrade_cnt))) {
      LOG_WARN("failed to batch upgrade virtual schema", KR(ret), K(tenant_id));
    }
  }
  LOG_INFO("[UPGRADE] upgrade virtual schema", KR(ret), K(tenant_id),
      "cost", ObTimeUtility::current_time() - start_ts);
  return ret;
}

int ObAdminUpgradeVirtualSchema::drop_not_exist_tables_(
    const uint64_t &tenant_id,
    const ObArrayArray<ObTableSchema> &hard_code_tables)
{
  int ret = OB_SUCCESS;
  int64_t start_ts = ObTimeUtility::current_time();
  LOG_INFO("[UPGRADE] upgrade virtual schema: drop not exist tables begin", K(tenant_id));
  ObHashSet<uint64_t> table_id_set;
  ObSchemaGetterGuard schema_guard;
  ObArray<uint64_t> tids;
  if (OB_ISNULL(ctx_.ddl_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is NULL", KR(ret), KP(ctx_.ddl_service_));
  } else if (OB_FAIL(generate_table_id_set_(hard_code_tables, table_id_set))) {
    LOG_WARN("failed to generate table id set", KR(ret));
  } else if (OB_FAIL(ctx_.ddl_service_->get_tenant_schema_guard_with_version_in_inner_table(tenant_id, schema_guard))) {
    LOG_WARN("get_schema_guard failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_table_ids_in_tenant(tenant_id, tids))) {
    LOG_WARN("get_table_ids_in_tenant failed", KR(ret), K(tenant_id));
  } else {
    LOG_INFO("begin to check table exist", KR(ret), K(tenant_id));
    FOREACH_CNT_X(tid, tids, OB_SUCC(ret)) {
      const ObTableSchema *in_mem_table = NULL;
      if (!is_inner_table(*tid) || is_sys_table(*tid)) {
        continue;
      } else if (OB_HASH_EXIST == table_id_set.exist_refactored(*tid)) {
        // table exists in hard_code_tables, do nothing
      } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, *tid, in_mem_table))) {
        LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(*tid));
      } else if (OB_ISNULL(in_mem_table)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("table not exist", KR(ret), K(tenant_id), K(*tid));
      } else if (OB_FAIL(ctx_.ddl_service_->drop_inner_table(NULL, *in_mem_table))) {
        LOG_WARN("drop table schema failed", KR(ret), K(tenant_id), KPC(in_mem_table));
      } else if (OB_FAIL(ctx_.ddl_service_->refresh_schema(tenant_id))) {
        LOG_WARN("refresh_schema failed", KR(ret), K(tenant_id));
      } else {
        LOG_INFO("drop not exist table", KR(ret), K(*tid));
      }
    }
  }
  LOG_INFO("[UPGRADE] upgrade virtual schema: drop not exist tables end", KR(ret), K(tenant_id),
      "cost", ObTimeUtility::current_time() - start_ts);
  return ret;
}

int ObAdminUpgradeVirtualSchema::generate_hard_code_schemas_(
    const int64_t &tenant_id,
    ObArrayArray<ObTableSchema> &hard_code_tables)
{
  int ret = OB_SUCCESS;
  int64_t start_ts = ObTimeUtility::current_time();
  LOG_INFO("[UPGRADE] upgrade virtual schema: generate hard_code_tables begin", KR(ret), K(tenant_id));
  const schema_create_func *creator_ptr_array[] = {
    share::virtual_table_schema_creators,
    share::virtual_table_index_schema_creators,
    share::sys_view_schema_creators, NULL };
  ObTableSchema table_schema;
    int64_t array_idx = 0;
  ObArray<ObTableSchema> empty_schemas;
  for (const schema_create_func **creator_ptr_ptr = creator_ptr_array;
      OB_SUCC(ret) && OB_NOT_NULL(*creator_ptr_ptr); ++creator_ptr_ptr, ++array_idx) {
    if (OB_FAIL(hard_code_tables.push_back(empty_schemas))) {
      LOG_WARN("failed to push_back empty_schema array", KR(ret));
    }
    for (const schema_create_func *creator_ptr = *creator_ptr_ptr;
        OB_SUCC(ret) && OB_NOT_NULL(*creator_ptr); ++creator_ptr) {
      table_schema.reset();
      bool exist = false;
      if (OB_FAIL((*creator_ptr)(table_schema))) {
        LOG_WARN("create table schema failed", KR(ret));
      } else if (!is_sys_tenant(tenant_id)
          && OB_FAIL(ObSchemaUtils::construct_tenant_space_full_table(
              tenant_id, table_schema))) {
        LOG_WARN("fail to construct tenant space table", KR(ret), K(tenant_id));
      } else if (OB_FAIL(ObSysTableChecker::is_inner_table_exist(
              tenant_id, table_schema, exist))) {
        LOG_WARN("fail to check inner table exist",
            KR(ret), K(tenant_id), K(table_schema));
      } else if (!exist) {
        // skip
      } else if (is_sys_table(table_schema.get_table_id())) {
        // only check and upgrade virtual table && sys views
      } else if (OB_FAIL(hard_code_tables.push_back(array_idx, table_schema))) {
        LOG_WARN("push_back failed", KR(ret), K(tenant_id));
      }
    }
  }
  LOG_INFO("[UPGRADE] upgrade virtual schema: generate hard_code_tables end", KR(ret), K(tenant_id),
      "cost", ObTimeUtility::current_time() - start_ts);
  return ret;
}
int ObAdminUpgradeVirtualSchema::generate_table_id_set_(
    const ObArrayArray<ObTableSchema> &hard_code_tables,
    common::hash::ObHashSet<uint64_t> &table_ids)
{
  int ret = OB_SUCCESS;
  int64_t capacity = 0;
  for (int64_t i = 0; i < hard_code_tables.count(); i++) {
    capacity += hard_code_tables.count(i);
  }
  if (OB_FAIL(table_ids.create(hash::cal_next_prime(capacity)))) {
    LOG_WARN("failed to create table ids set", KR(ret), K(capacity));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < hard_code_tables.count(); i++) {
      const ObIArray<ObTableSchema> &tables = hard_code_tables.at(i);
      for (int64_t j = 0; OB_SUCC(ret) && j < tables.count(); j++) {
        const ObTableSchema &table = tables.at(j);
        const uint64_t table_id = table.get_table_id();
        if (OB_FAIL(table_ids.set_refactored(table_id))) {
          LOG_WARN("failed to add table_id to set", KR(ret), K(table_id));
        }
      }
    }
    if (OB_SUCC(ret) && table_ids.size() != capacity) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("duplicated table_id in hard_code_tables", KR(ret), K(table_ids));
    }
  }
  return ret;
}

int ObAdminUpgradeVirtualSchema::batch_upgrade_(const uint64_t tenant_id,
    ObIArray<share::schema::ObTableSchema> &hard_code_tables,
    int64_t &upgrade_cnt)
{
  int ret = OB_SUCCESS;
  int64_t start_ts = ObTimeUtility::current_time();
  int64_t count = 0;
  LOG_INFO("[UPGRADE] batch upgrade virtual schema begin", KR(ret), K(tenant_id));
  if (OB_UNLIKELY(!ctx_.is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(ctx_.ddl_service_) || OB_ISNULL(ctx_.schema_service_) || OB_ISNULL(ctx_.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is null", KR(ret), KP(ctx_.root_inspection_), KP(ctx_.ddl_service_),
        KP(ctx_.schema_service_), KP(ctx_.sql_proxy_));
  } else {
    int tmp_ret = OB_SUCCESS;
    ObArray<int64_t> upgrade_table_idx;
    ObDDLSQLTransaction trans(ctx_.schema_service_,
        true/*need_end_signal*/, true/*enable_query_stash*/);
    ObSchemaGetterGuard schema_guard;
    int64_t refreshed_schema_version = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < hard_code_tables.count(); i++) {
      const share::schema::ObTableSchema &hard_code_table = hard_code_tables.at(i);
      if (OB_FAIL(ObSysTableInspection::check_table_schema(tenant_id, hard_code_table))) {
        if (OB_SCHEMA_ERROR != ret) {
          LOG_WARN("check table schema failed", KR(ret), K(tenant_id), K(hard_code_table));
        } else {
          LOG_INFO("table schema need upgrade", K(tenant_id), K(hard_code_table));
          if (OB_FAIL(upgrade_table_idx.push_back(i))) {
            LOG_WARN("failed push upgrade table idx", KR(ret), K(tenant_id), K(hard_code_table), K(i));
          }
        }
      }
    }
    if (FAILEDx(ctx_.ddl_service_->get_tenant_schema_guard_with_version_in_inner_table(
            tenant_id, schema_guard))) {
      LOG_WARN("get schema guard in inner table failed", KR(ret), K(tenant_id));
    } else if (OB_FAIL(schema_guard.get_schema_version(tenant_id, refreshed_schema_version))) {
      LOG_WARN("failed to get refreshed_schema_version", KR(ret), K(tenant_id));
    } else if (OB_FAIL(trans.start(ctx_.sql_proxy_, tenant_id, refreshed_schema_version))) {
      LOG_WARN("failed to start trans", KR(ret), K(tenant_id), K(refreshed_schema_version));
    } else if (OB_FAIL(batch_upgrade_inner_tables_(trans, tenant_id, schema_guard, hard_code_tables,
            upgrade_table_idx))) {
      LOG_WARN("failed to batch upgrade inner tables", KR(ret), K(tenant_id), K(upgrade_table_idx));
    }
    if (trans.is_started()) {
      if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
        LOG_WARN_RET(tmp_ret, "trans end failed", "is_commit", OB_SUCCESS == ret, K(tmp_ret));
        ret = (OB_SUCC(ret)) ? tmp_ret : ret;
      }
    }
    if (OB_TMP_FAIL(ctx_.ddl_service_->refresh_schema(tenant_id))) {
      LOG_WARN_RET(tmp_ret, "refresh schema failed", KR(tmp_ret), K(tenant_id));
      ret = OB_SUCC(ret)? tmp_ret: ret;
    }
    count = upgrade_table_idx.count();
    upgrade_cnt += upgrade_table_idx.count();
    DEBUG_SYNC(AFTER_UPGRADE_VIRTUAL_SCHEMA_REFRESH_SCHEMA);
  }
  LOG_INFO("[UPGRADE] batch upgrade virtual schema end", KR(ret), K(tenant_id), K(count),
      "cost", ObTimeUtility::current_time() - start_ts);
  return ret;
}

int ObAdminUpgradeVirtualSchema::batch_upgrade_inner_tables_(
    ObDDLSQLTransaction &trans, 
    const uint64_t &tenant_id,
    share::schema::ObSchemaGetterGuard &schema_guard,
    ObIArray<share::schema::ObTableSchema> &hard_code_tables,
    const ObIArray<int64_t> &upgrade_idxs)
{
  int ret = OB_SUCCESS;
  int64_t start_ts = ObTimeUtility::current_time();
  if (OB_UNLIKELY(!ctx_.is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || is_virtual_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(ctx_.ddl_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is null", KR(ret), KP(ctx_.ddl_service_));
  } else if (upgrade_idxs.empty()) {
    // upgrade_idxs is empty, do nothing
  } else {
    int tmp_ret = OB_SUCCESS;
    ObHashSet<uint64_t> dropped_table_ids;
    if (OB_FAIL(dropped_table_ids.create(hash::cal_next_prime(upgrade_idxs.count())))) {
      LOG_WARN("failed to create dropped_table_ids set", KR(ret));
    }
    FOREACH_CNT_X(it, upgrade_idxs, OB_SUCC(ret)) {
      if (*it < 0 || *it >= hard_code_tables.count()) {
        ret = OB_ERR_UNDEFINED;
        LOG_WARN("upgrade idx out of range", KR(ret), K(*it), K(hard_code_tables.count()));
      } else {
        share::schema::ObTableSchema &table = hard_code_tables.at(*it);
        if (OB_FAIL(check_and_drop_inner_table_(trans, tenant_id, table, dropped_table_ids))) {
          LOG_WARN("failed to drop inner table", KR(ret), K(tenant_id), K(table), K(*it), K(dropped_table_ids));
        }
      }
    }
    // double check to avoid dropped user table or sys table
    FOREACH_X(it, dropped_table_ids, OB_SUCC(ret)) {
      const uint64_t table_id = it->first;
      if (!common::is_inner_table(table_id) || common::is_sys_table(table_id)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("dropped table is unexpected", KR(ret), K(table_id));
      }
    }
    FOREACH_CNT_X(it, upgrade_idxs, OB_SUCC(ret)) {
      if (*it < 0 || *it >= hard_code_tables.count()) {
        ret = OB_ERR_UNDEFINED;
        LOG_WARN("upgrade idx out of range", KR(ret), K(*it), K(hard_code_tables.count()));
      } else {
        share::schema::ObTableSchema &table = hard_code_tables.at(*it);
        if (OB_FAIL(ctx_.ddl_service_->add_table_schema(&trans, table, schema_guard))) {
          LOG_WARN("failed to create inner table", KR(ret), K(tenant_id), K(table), K(*it));
        }
      }
    }
  }
  LOG_INFO("[UPGRADE] upgrade virtual schema: upgrade tables", KR(ret), K(tenant_id),
      "cost", ObTimeUtility::current_time() - start_ts);
  return ret;
}

bool ObAdminUpgradeVirtualSchema::check_table_dropped(const share::schema::ObSimpleTableSchemaV2 &table,
      const common::hash::ObHashSet<uint64_t> &dropped_table_ids)
{
  bool dropped = false;
  if (dropped_table_ids.exist_refactored(table.get_table_id()) == OB_HASH_EXIST) {
    dropped = true;
  } else if (table.is_index_table()) {
    const uint64_t data_table_id = table.get_data_table_id();
    if (dropped_table_ids.exist_refactored(data_table_id) == OB_HASH_EXIST) {
      dropped = true;
    }
  }
  return dropped;
}

int ObAdminUpgradeVirtualSchema::check_and_drop_inner_table_(
    rootserver::ObDDLSQLTransaction &trans,
    const uint64_t &tenant_id,
    share::schema::ObTableSchema &table,
    ObHashSet<uint64_t> &dropped_table_ids)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *exist_schema = NULL;
  ObSchemaGetterGuard schema_guard;
  if (OB_UNLIKELY(!ctx_.is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(is_virtual_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(
             !table.is_valid()
             || is_sys_table(table.get_table_id())
             || table.get_tenant_id() != tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table", KR(ret), K(tenant_id), K(table));
  } else if (OB_ISNULL(ctx_.ddl_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ddl service is null", KR(ret));
  }
  LOG_INFO("[UPGRADE] drop inner table begin", KR(ret), K(table.get_table_id()), K(table.is_index_table()));
  // 1. check table name duplicated
  if (FAILEDx(ctx_.ddl_service_->get_tenant_schema_guard_with_version_in_inner_table(
      tenant_id, schema_guard))) {
    LOG_WARN("get schema guard in inner table failed", KR(ret), K(tenant_id));
  } else {
    ObArenaAllocator allocator;
    ObString index_name;
    if (table.is_index_table()) {
      // In the early version, table name of oracle virtual table index is not right
      // (data_table_id is mysql virtual table id), which may cause we can't find duplicate index
      // with table name and duplicate name conflict occur.
      //
      // ETC:
      // OB_ALL_VIRTUAL_PLAN_CACHE_STAT_ORA_TID = 15034;
      // OB_ALL_VIRTUAL_PLAN_CACHE_STAT_ORA_ALL_VIRTUAL_PLAN_CACHE_STAT_I1_TID = 19998;
      // OB_ALL_VIRTUAL_PLAN_CACHE_STAT_ALL_VIRTUAL_PLAN_CACHE_STAT_I1_TNAME = "__idx_1099511638779_all_virtual_plan_cache_stat_i1";
      // OB_ALL_VIRTUAL_PLAN_CACHE_STAT_ORA_ALL_VIRTUAL_PLAN_CACHE_STAT_I1_TNAME = "__idx_1099511642810_all_virtual_plan_cache_stat_i1";
      //
      // For oracle virtual table index which data_table_id is (1 << 40) | 15034 = 1099511642810,
      // but it use OB_ALL_VIRTUAL_PLAN_CACHE_STAT_ALL_VIRTUAL_PLAN_CACHE_STAT_I1_TNAME as table name.
      if (OB_FAIL(table.generate_origin_index_name())) {
        LOG_WARN("fail to generate origin index name", KR(ret), K(table));
      } else if (OB_FAIL(ObTableSchema::build_index_table_name(
                 allocator, table.get_data_table_id(),
                 table.get_origin_index_name_str(), index_name))) {
        LOG_WARN("fail to build index table name", KR(ret), K(table));
      }
    }
    if (FAILEDx(schema_guard.get_table_schema(
                tenant_id,
                table.get_database_id(),
                table.is_index_table() ? index_name : table.get_table_name(),
                table.is_index_table(),
                exist_schema))) {
      LOG_WARN("get table schema failed", KR(ret), K(tenant_id), "table", table.get_table_name());
      if (OB_TABLE_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      }
    } else if (OB_ISNULL(exist_schema)) {
      // no duplicate table name
    } else if (check_table_dropped(*exist_schema, dropped_table_ids)) {
      // the table is dropped in this trans

      // avoid drop priv in oracle
    } else if (OB_FAIL(ctx_.ddl_service_->drop_inner_table(&trans, *exist_schema, false/*delete_priv*/))) {
      LOG_WARN("get table schema failed", KR(ret), K(tenant_id),
               "table", table.get_table_name(), "table_id", table.get_table_id());
    } else if (OB_FAIL(dropped_table_ids.set_refactored(exist_schema->get_table_id()))) {
      LOG_WARN("failed to add table_id to dropped_table_id set", KR(ret), KPC(exist_schema));
    }
  }
  // 2. try drop table first
  exist_schema = NULL;
  if (OB_FAIL(ret)) {
  } else if (check_table_dropped(table, dropped_table_ids)) {
    // the table is dropped in this trans
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id,
                                            table.get_table_id(),
                                            exist_schema))) {
    LOG_WARN("get table schema failed", KR(ret), "table", table.get_table_name(),
             "table_id", table.get_table_id());
    if (OB_TABLE_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    }
  } else if (OB_ISNULL(exist_schema)) {
    // missed table

    // in oracle mode, drop table will drop priv on it cascade
    // here the exist_schema will be upgraded, and it will be dropped and created again.
    // then the priv on it will be lost
    // so do not delete the priv on it here.
  } else if (OB_FAIL(ctx_.ddl_service_->drop_inner_table(&trans, *exist_schema, false/*delete_priv*/))) {
    LOG_WARN("drop table schema failed", KR(ret), "table_schema", *exist_schema);
  } else if (OB_FAIL(dropped_table_ids.set_refactored(table.get_table_id()))) {
    LOG_WARN("failed to add table_id to dropped_table_id set", KR(ret), K(table));
  }
  return ret;
}

int ObAdminUpgradeCmd::execute(const Bool &upgrade)
{
  int ret = OB_SUCCESS;
  // set enable_upgrade_mode
  HEAP_VAR(ObAdminSetConfigItem, item) {
    obrpc::ObAdminSetConfigArg set_config_arg;
    set_config_arg.is_inner_ = true;
    const char *enable_upgrade_name = "enable_upgrade_mode";
    ObAdminSetConfig admin_set_config(ctx_);
    char min_server_version[OB_SERVER_VERSION_LENGTH] = {'\0'};
    uint64_t cluster_version = GET_MIN_CLUSTER_VERSION();

    if (OB_INVALID_INDEX == ObClusterVersion::print_version_str(
        min_server_version, OB_SERVER_VERSION_LENGTH, cluster_version)) {
       ret = OB_INVALID_ARGUMENT;
       LOG_WARN("fail to print version str", KR(ret), K(cluster_version));
    } else if (OB_FAIL(item.name_.assign(enable_upgrade_name))) {
      LOG_WARN("assign enable_upgrade_mode config name failed", KR(ret));
    } else if (OB_FAIL(item.value_.assign((upgrade ? "true" : "false")))) {
      LOG_WARN("assign enable_upgrade_mode config value failed", KR(ret));
    } else if (OB_FAIL(set_config_arg.items_.push_back(item))) {
      LOG_WARN("add enable_upgrade_mode config item failed", KR(ret));
    } else {
      const char *upgrade_stage_name = "_upgrade_stage";
      obrpc::ObUpgradeStage stage = upgrade ?
                                    obrpc::OB_UPGRADE_STAGE_PREUPGRADE :
                                    obrpc::OB_UPGRADE_STAGE_NONE;
      if (OB_FAIL(item.name_.assign(upgrade_stage_name))) {
        LOG_WARN("assign _upgrade_stage config name failed", KR(ret), K(upgrade));
      } else if (OB_FAIL(item.value_.assign(obrpc::get_upgrade_stage_str(stage)))) {
        LOG_WARN("assign _upgrade_stage config value failed", KR(ret), K(stage), K(upgrade));
      } else if (OB_FAIL(set_config_arg.items_.push_back(item))) {
        LOG_WARN("add _upgrade_stage config item failed", KR(ret), K(stage), K(upgrade));
      }
    }
    share::ObServerInfoInTable::ObBuildVersion build_version;
    if (FAILEDx(admin_set_config.execute(set_config_arg))) {
      LOG_WARN("execute set config failed", KR(ret));
    } else if (OB_FAIL(observer::ObService::get_build_version(build_version))) {
      LOG_WARN("fail to get build version", KR(ret));
    } else {
      CLUSTER_EVENT_SYNC_ADD("UPGRADE",
                             upgrade ? "BEGIN_UPGRADE" : "END_UPGRADE",
                             "cluster_version", min_server_version,
                             "build_version", build_version.ptr());
      LOG_INFO("change upgrade parameters",
               "enable_upgrade_mode", upgrade,
               "in_major_version_upgrade_mode", GCONF.in_major_version_upgrade_mode());

    }
  }
  return ret;
}

int ObAdminRollingUpgradeCmd::execute(const obrpc::ObAdminRollingUpgradeArg &arg)
{
  int ret = OB_SUCCESS;
  uint64_t max_server_id = 0;
  HEAP_VAR(ObAdminSetConfigItem, item) {
    obrpc::ObAdminSetConfigArg set_config_arg;
    set_config_arg.is_inner_ = true;
    const char *upgrade_stage_name = "_upgrade_stage";
    ObAdminSetConfig admin_set_config(ctx_);
    char ori_min_server_version[OB_SERVER_VERSION_LENGTH] = {'\0'};
    char min_server_version[OB_SERVER_VERSION_LENGTH] = {'\0'};
    uint64_t ori_cluster_version = GET_MIN_CLUSTER_VERSION();

    if (!arg.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arg", KR(ret), K(arg));
    } else if (OB_INVALID_INDEX == ObClusterVersion::print_version_str(
               ori_min_server_version, OB_SERVER_VERSION_LENGTH, ori_cluster_version)) {
       ret = OB_INVALID_ARGUMENT;
       LOG_WARN("fail to print version str", KR(ret), K(ori_cluster_version));
    } else if (OB_FAIL(item.name_.assign(upgrade_stage_name))) {
      LOG_WARN("assign _upgrade_stage config name failed", KR(ret), K(arg));
    } else if (OB_FAIL(item.value_.assign(obrpc::get_upgrade_stage_str(arg.stage_)))) {
      LOG_WARN("assign _upgrade_stage config value failed", KR(ret), K(arg));
    } else if (OB_FAIL(set_config_arg.items_.push_back(item))) {
      LOG_WARN("add _upgrade_stage config item failed", KR(ret), K(arg));
    } else if (obrpc::OB_UPGRADE_STAGE_POSTUPGRADE == arg.stage_) {
      // wait min_observer_version to report to inner table
      ObTimeoutCtx ctx;
      if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, GCONF.rpc_timeout))) {
        LOG_WARN("fail to set default timeout", KR(ret));
      } else {
        const int64_t CHECK_INTERVAL = 100 * 1000L; // 100ms
        while (OB_SUCC(ret)) {
          uint64_t min_observer_version = 0;
          if (ctx.is_timeouted()) {
            ret = OB_TIMEOUT;
            LOG_WARN("wait min_server_version report to inner table failed",
                     KR(ret), "abs_timeout", ctx.get_abs_timeout());
          } else if (OB_FAIL(SVR_TRACER.get_min_server_version(
                     min_server_version, min_observer_version))) {
            LOG_WARN("failed to get the min server version", KR(ret));
          } else if (min_observer_version > CLUSTER_CURRENT_VERSION) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("min_observer_version is larger than CLUSTER_CURRENT_VERSION",
                     KR(ret), "min_server_version", min_server_version,
                     K(min_observer_version), "CLUSTER_CURRENT_VERSION", CLUSTER_CURRENT_VERSION);
          } else if (min_observer_version < CLUSTER_CURRENT_VERSION) {
            if (REACH_TIME_INTERVAL(1 * 1000 * 1000L)) { // 1s
              LOG_INFO("min_observer_version is not reported yet, just wait",
                       KR(ret), "min_server_version", min_server_version,
                       K(min_observer_version), "CLUSTER_CURRENT_VERSION", CLUSTER_CURRENT_VERSION);
            }
            ob_usleep(CHECK_INTERVAL);
          } else {
            break;
          }
        } // end while
      }
      if (OB_FAIL(ret) || GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_3_4_0) {
      } else if (OB_FAIL(ObServerTableOperator::get_clusters_max_server_id(max_server_id))) {
        LOG_WARN("fail to get max server id", KR(ret));
      } else if (OB_UNLIKELY(!is_valid_server_index(max_server_id))) {
        ret = OB_OP_NOT_ALLOW;
        LOG_WARN("max_server_id should be a valid server index", KR(ret), K(max_server_id));
        LOG_USER_ERROR(OB_OP_NOT_ALLOW, "max server id in the cluster cannot be larget than MAX_SERVER_COUNT, UPGRADE is");
      }
      // end rolling upgrade, should raise min_observer_version
      const char *min_obs_version_name = "min_observer_version";
      if (FAILEDx(item.name_.assign(min_obs_version_name))) {
        LOG_WARN("assign min_observer_version config name failed",
                 KR(ret), K(min_obs_version_name));
      } else if (OB_FAIL(item.value_.assign(min_server_version))) {
        LOG_WARN("assign min_observer_version config value failed",
                 KR(ret), K(min_server_version));
      } else if (OB_FAIL(set_config_arg.items_.push_back(item))) {
        LOG_WARN("add min_observer_version config item failed", KR(ret), K(item));
      }
    }
    if (FAILEDx(admin_set_config.execute(set_config_arg))) {
      LOG_WARN("execute set config failed", KR(ret));
    } else {
      share::ObServerInfoInTable::ObBuildVersion build_version;
      if (OB_FAIL(observer::ObService::get_build_version(build_version))) {
        LOG_WARN("fail to get build version", KR(ret));
      } else if (obrpc::OB_UPGRADE_STAGE_POSTUPGRADE != arg.stage_) {
        CLUSTER_EVENT_SYNC_ADD("UPGRADE", "BEGIN_ROLLING_UPGRADE",
                               "cluster_version", ori_min_server_version,
                               "build_version", build_version.ptr());
      } else {
        CLUSTER_EVENT_SYNC_ADD("UPGRADE", "END_ROLLING_UPGRADE",
                               "cluster_version", min_server_version,
                               "ori_cluster_version", ori_min_server_version,
                               "build_version", build_version.ptr());
      }
      LOG_INFO("change upgrade parameters", KR(ret), "_upgrade_stage", arg.stage_);
    }
  }
  return ret;
}

DEFINE_ENUM_FUNC(ObInnerJob, inner_job, OB_INNER_JOB_DEF);

int ObAdminRunJob::execute(const ObRunJobArg &arg)
{
  int ret = OB_SUCCESS;
  ObInnerJob job = INVALID_INNER_JOB;
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), KR(ret));
  } else if (INVALID_INNER_JOB == (job = get_inner_job_value(arg.job_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid inner job", K(arg), KR(ret));
  } else {
    switch(job) {
      case CHECK_PARTITION_TABLE: {
        ObAdminCheckPartitionTable job_executor(ctx_);
        if (OB_FAIL(job_executor.execute(arg))) {
          LOG_WARN("execute job failed", K(arg), KR(ret));
        }
        break;
      }
      case ROOT_INSPECTION: {
        ObAdminRootInspection job_executor(ctx_);
        if (OB_FAIL(job_executor.execute(arg))) {
          LOG_WARN("execute job failed", K(arg), KR(ret));
        }
        break;
      }
      case UPGRADE_STORAGE_FORMAT_VERSION:
      case STOP_UPGRADE_STORAGE_FORMAT_VERSION: {
        ObAdminUpgradeStorageFormatVersionExecutor job_executor(ctx_);
        if (OB_FAIL(job_executor.execute(arg))) {
          LOG_WARN("fail to execute upgrade storage format version job", KR(ret));
        }
        break;
      }
      case CREATE_INNER_SCHEMA: {
        ObAdminCreateInnerSchema job_executor(ctx_);
        if (OB_FAIL(job_executor.execute(arg))) {
          LOG_WARN("execute job failed", KR(ret));
        }
        break;
      }
      case IO_CALIBRATION: {
        ObAdminIOCalibration job_executor(ctx_);
        if (OB_FAIL(job_executor.execute(arg))) {
          LOG_WARN("execute job failed", KR(ret));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("not known job", K(job), KR(ret));
        break;
      }
    }
  }
  return ret;
}

int ObAdminCheckPartitionTable::execute(const obrpc::ObRunJobArg &arg)
{
  UNUSEDx(arg);
  return OB_NOT_SUPPORTED;
}

int ObAdminCheckPartitionTable::call_server(const ObAddr &server)
{
  int ret = OB_SUCCESS;
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), KR(ret));
  } else if (OB_FAIL(ctx_.rpc_proxy_->to(server).check_partition_table())) {
    LOG_WARN("request check partition table failed", KR(ret), K(server));
  }
  return ret;
}

int ObAdminCreateInnerSchema::execute(const obrpc::ObRunJobArg &arg)
{
  int ret = OB_SUCCESS;
  LOG_INFO("execute create inner role request", KR(ret));
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(arg));
  } else if (CREATE_INNER_SCHEMA != get_inner_job_value(arg.job_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("job to run not create inner role", KR(ret), K(arg));
  } else if (OB_UNLIKELY(nullptr == ctx_.root_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("root service ptr is null", KR(ret));
  } else if (OB_FAIL(ctx_.root_service_->submit_create_inner_schema_task())) {
    LOG_WARN("fail to submit create inner role task", KR(ret));
  }
  return ret;
}

int ObAdminIOCalibration::execute(const obrpc::ObRunJobArg &arg)
{
  int ret = OB_SUCCESS;
  LOG_INFO("execute io calibration quest", KR(ret));
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(arg));
  } else if (IO_CALIBRATION != get_inner_job_value(arg.job_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected job type", KR(ret), K(arg));
  } else if (OB_FAIL(call_all(arg))) {
    LOG_WARN("call all server failed", K(ret), K(arg));
  }
  return ret;
}

int ObAdminIOCalibration::call_server(const common::ObAddr &server)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ctx_.is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), K(ret));
  } else if (OB_FAIL(ctx_.rpc_proxy_->to(server).execute_io_benchmark())) {
    LOG_WARN("request io calibration failed", KR(ret), K(server));
  }
  return ret;
}

int ObAdminRefreshIOCalibration::execute(const obrpc::ObAdminRefreshIOCalibrationArg &arg)
{
  int ret = OB_SUCCESS;
  ObArray<ObAddr> server_list;
  if (OB_UNLIKELY(!ctx_.is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else if (OB_FAIL(get_server_list(arg, server_list))) {
    LOG_WARN("get server list failed", K(ret), K(arg));
  } else if (arg.only_refresh_) {
    // do nothing
  } else {
    ObIOAbility io_ability;
    for (int64_t i = 0; OB_SUCC(ret) && i < arg.calibration_list_.count(); ++i) {
      const ObIOBenchResult &item = arg.calibration_list_.at(i);
      if (OB_FAIL(io_ability.add_measure_item(item))) {
        LOG_WARN("add item failed", K(ret), K(item));
      }
    }
    if (OB_SUCC(ret)) {
      if (arg.calibration_list_.count() > 0 && !io_ability.is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid calibration list", K(ret), K(arg), K(io_ability));
      }
    }
    if (OB_SUCC(ret)) {
      ObMySQLTransaction trans;
      if (OB_FAIL(trans.start(ctx_.sql_proxy_, OB_SYS_TENANT_ID))) {
        LOG_WARN("start transaction failed", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < server_list.count(); ++i) {
          if (OB_FAIL(ObIOCalibration::get_instance().write_into_table(trans, server_list.at(i), io_ability))) {
            LOG_WARN("write io ability failed", K(ret), K(io_ability), K(server_list.at(i)));
          }
        }
        bool is_commit = OB_SUCCESS == ret;
        int tmp_ret = trans.end(is_commit);
        if (OB_UNLIKELY(OB_SUCCESS != tmp_ret)) {
          LOG_WARN("end transaction failed", K(tmp_ret), K(is_commit));
          ret = OB_SUCC(ret) ? tmp_ret : ret;
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObRefreshIOCalibrationArg refresh_arg;
    refresh_arg.storage_name_ = arg.storage_name_;
    refresh_arg.only_refresh_ = arg.only_refresh_;
    if (OB_FAIL(refresh_arg.calibration_list_.assign(arg.calibration_list_))) {
      LOG_WARN("assign calibration list failed", K(ret), K(arg.calibration_list_));
    } else {
      int64_t succ_count = 0;
      FOREACH_CNT(server, server_list) {
        int tmp_ret = ctx_.rpc_proxy_->to(*server).refresh_io_calibration(refresh_arg);
        if (OB_UNLIKELY(OB_SUCCESS != tmp_ret)) {
          LOG_WARN("request io calibration failed", KR(tmp_ret), K(*server), K(refresh_arg));
        } else {
          ++succ_count;
        }
      }
      if (server_list.count() != succ_count) {
        ret = OB_PARTIAL_FAILED;
        LOG_USER_ERROR(OB_PARTIAL_FAILED, "Partial failed");
      }
    }
  }
  LOG_INFO("admin refresh io calibration", K(ret), K(arg), K(server_list));
  return ret;
}

int ObAdminRefreshIOCalibration::call_server(const common::ObAddr &server)
{
  // should never go here
  UNUSED(server);
  return OB_NOT_SUPPORTED;
}

int ObAdminRootInspection::execute(const obrpc::ObRunJobArg &arg)
{
  int ret = OB_SUCCESS;
  LOG_INFO("execute root inspection request", K(arg));
  ObAddr rs_addr;
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), KR(ret));
  } else if (ROOT_INSPECTION != get_inner_job_value(arg.job_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("job to run not root inspection", K(arg), KR(ret));
  } else if (OB_ISNULL(GCTX.rs_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.rs_mgr_ is null", KR(ret), KP(GCTX.rs_mgr_));
  } else if (OB_FAIL(GCTX.rs_mgr_->get_master_root_server(rs_addr))) {
    LOG_WARN("fail to get master root server", KR(ret));
  } else if (OB_UNLIKELY(!rs_addr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("rs_addr is invalid", KR(ret), K(rs_addr));
  } else if (!ctx_.root_inspection_->is_inited()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("root_inspection not inited", KR(ret));
  } else if (!arg.zone_.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("root inspection can't execute by zone", K(arg), KR(ret));
  } else if (arg.server_.is_valid() && arg.server_ != rs_addr) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("only rs can execute root inspection", K(arg),
        "rs", rs_addr, KR(ret));
  } else if (OB_FAIL(ctx_.root_inspection_->check_all())) {
    LOG_WARN("root_inspection check_all failed", KR(ret));
  }

  return ret;
}

int ObAdminUpgradeStorageFormatVersionExecutor::execute(const obrpc::ObRunJobArg &arg)
{
  int ret = OB_SUCCESS;
  ObInnerJob job = INVALID_INNER_JOB;
  LOG_INFO("execute upgrade storage format version request", K(arg));
  if (OB_UNLIKELY(!ctx_.is_inited())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ObAdminUpgradeStorageFormatVersionExecutor has not been inited", KR(ret));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(arg));
  } else {
    job = get_inner_job_value(arg.job_);
    if (UPGRADE_STORAGE_FORMAT_VERSION == job) {
      if (OB_ISNULL(ctx_.root_service_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, root service must not be NULL", KR(ret));
      } else if (OB_FAIL(ctx_.root_service_->submit_upgrade_storage_format_version_task())) {
        LOG_WARN("fail to submit upgrade storage format version task", KR(ret));
      }
    } else if (STOP_UPGRADE_STORAGE_FORMAT_VERSION == job) {
      if (OB_ISNULL(ctx_.upgrade_storage_format_executor_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("executor is null", KR(ret));
      } else if (OB_FAIL(ctx_.upgrade_storage_format_executor_->stop())) {
        LOG_WARN("fail to stop upgrade_storage_format task", KR(ret));
      } else {
        ctx_.upgrade_storage_format_executor_->start();
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid job type", KR(ret), K(job));
    }
  }
  return ret;
}

int ObAdminFlushCache::execute(const obrpc::ObAdminFlushCacheArg &arg)
{
  int ret = OB_SUCCESS;
  int64_t tenant_num = arg.tenant_ids_.count();
  ObSEArray<ObAddr, 8> server_list;
  ObFlushCacheArg fc_arg;
  // fine-grained plan evict only will pass this way.
  // This because fine-grained plan evict must specify tenant
  // if tenant num is 0, flush all tenant, else, flush appointed tenant
  if (tenant_num != 0) { //flush appointed tenant
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_num; ++i) {
      //get tenant server list;
      if (OB_FAIL(get_tenant_servers(arg.tenant_ids_.at(i), server_list))) {
        LOG_WARN("fail to get tenant servers", "tenant_id", arg.tenant_ids_.at(i));
      } else {
        //call tenant servers;
        fc_arg.is_all_tenant_ = false;
        fc_arg.cache_type_ = arg.cache_type_;
        fc_arg.ns_type_ = arg.ns_type_;
        // fine-grained plan evict args
        if (arg.is_fine_grained_) {
          fc_arg.sql_id_ = arg.sql_id_;
          fc_arg.is_fine_grained_ = arg.is_fine_grained_;
          fc_arg.schema_id_ = arg.schema_id_;
          for(int64_t j=0; OB_SUCC(ret) && j<arg.db_ids_.count(); j++) {
            if (OB_FAIL(fc_arg.push_database(arg.db_ids_.at(j)))) {
              LOG_WARN("fail to add db ids", KR(ret));
            }
          }
        }
        for (int64_t j = 0; OB_SUCC(ret) && j < server_list.count(); ++j) {
          fc_arg.tenant_id_ = arg.tenant_ids_.at(i);
          LOG_INFO("flush server cache", K(fc_arg), K(server_list.at(j)));
          if (OB_FAIL(call_server(server_list.at(j), fc_arg))) {
            LOG_WARN("fail to call tenant server",
                     "tenant_id", arg.tenant_ids_.at(i),
                     "server addr", server_list.at(j));
          }
        }
      }
      server_list.reset();
    }
  } else { // flush all tenant
    //get all server list, server_mgr_.get_alive_servers
    if (OB_FAIL(get_all_servers(server_list))) {
      LOG_WARN("fail to get all servers", KR(ret));
    } else {
      fc_arg.is_all_tenant_ = true;
      fc_arg.tenant_id_ = common::OB_INVALID_TENANT_ID;
      fc_arg.cache_type_ = arg.cache_type_;
      fc_arg.ns_type_ = arg.ns_type_;
      for (int64_t j = 0; OB_SUCC(ret) && j < server_list.count(); ++j) {
        LOG_INFO("flush server cache", K(fc_arg), K(server_list.at(j)));
        if (OB_FAIL(call_server(server_list.at(j), fc_arg))) {
          LOG_WARN("fail to call tenant server",
                   "server addr", server_list.at(j));
        }
      }
    }
  }
  return ret;
}

#ifdef OB_BUILD_SPM
int ObAdminLoadBaseline::execute(const obrpc::ObLoadPlanBaselineArg &arg)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObAddr, 8> server_list;
  if (OB_FAIL(get_tenant_servers(arg.tenant_id_, server_list))) {
    LOG_WARN("fail to get tenant servers", "tenant_id", arg.tenant_id_, KR(ret));
  } else {
    //call tenant servers;
    for (int64_t j = 0; OB_SUCC(ret) && j < server_list.count(); ++j) {
      if (OB_FAIL(call_server(server_list.at(j), arg))) {
        LOG_WARN("fail to call tenant server",
                 "tenant_id", arg.tenant_id_,
                 "server addr", server_list.at(j),
                 KR(ret));
      }
    }
  }
  server_list.reset();
  return ret;
}

int ObAdminLoadBaseline::call_server(const common::ObAddr &server,
                                     const obrpc::ObLoadPlanBaselineArg &arg)
{
  int ret = OB_SUCCESS;
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), KR(ret));
  } else if (OB_FAIL(ctx_.rpc_proxy_->to(server)
                                     .by(arg.tenant_id_)
                                     .as(arg.tenant_id_)
                                     .load_baseline(arg))) {
    LOG_WARN("request server load baseline failed", KR(ret), K(server));
  }

  return ret;
}

int ObAdminLoadBaselineV2::execute(const obrpc::ObLoadPlanBaselineArg &arg, uint64_t &total_load_count)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObAddr, 8> server_list;
  if (OB_FAIL(get_tenant_servers(arg.tenant_id_, server_list))) {
    LOG_WARN("fail to get tenant servers", "tenant_id", arg.tenant_id_, KR(ret));
  } else {
    //call tenant servers;
    for (int64_t j = 0; OB_SUCC(ret) && j < server_list.count(); ++j) {
      obrpc::ObLoadBaselineRes res;
      if (OB_FAIL(call_server(server_list.at(j), arg, res))) {
        LOG_WARN("fail to call tenant server",
                 "tenant_id", arg.tenant_id_,
                 "server addr", server_list.at(j),
                 KR(ret));
      } else {
        total_load_count += res.load_count_;
      }
    }
  }
  server_list.reset();
  return ret;
}

int ObAdminLoadBaselineV2::call_server(const common::ObAddr &server,
                                     const obrpc::ObLoadPlanBaselineArg &arg,
                                     obrpc::ObLoadBaselineRes &res)
{
  int ret = OB_SUCCESS;
  int64_t timeout = THIS_WORKER.get_timeout_remain();
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), KR(ret));
  } else if (OB_UNLIKELY(0 >= timeout)) {
    ret = OB_TIMEOUT;
    LOG_WARN("query timeout is reached", K(timeout));
  } else if (OB_FAIL(ctx_.rpc_proxy_->to(server)
                                     .by(arg.tenant_id_)
                                     .as(arg.tenant_id_)
                                     .timeout(timeout)
                                     .load_baseline_v2(arg, res))) {
    LOG_WARN("request server load baseline failed", KR(ret), K(server));
  }
  return ret;
}
#endif

int ObTenantServerAdminUtil::get_tenant_servers(const uint64_t tenant_id, common::ObIArray<ObAddr> &servers)
{
  int ret = OB_SUCCESS;
  ObUnitTableOperator ut_operator;
  servers.reset();
  // sys tenant, get all servers directly
  if (OB_SYS_TENANT_ID == tenant_id) {
    if (OB_FAIL(get_all_servers(servers))) {
      LOG_WARN("fail to get all servers", KR(ret));
    }
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(ut_operator.init(*GCTX.sql_proxy_))) {
    LOG_WARN("fail to init unit table operator", KR(ret), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(ut_operator.get_alive_servers_by_tenant(tenant_id, servers))) {
    LOG_WARN("fail to get tenant servers", KR(ret), K(tenant_id));
  }

  return ret;
}

int ObTenantServerAdminUtil::get_all_servers(common::ObIArray<ObAddr> &servers)
{
  int ret = OB_SUCCESS;
  ObZone empty_zone;
  if (OB_FAIL(SVR_TRACER.get_alive_servers(empty_zone, servers))) {
    //if zone is empty, get all servers
    LOG_WARN("fail to get all servers", KR(ret));
  }
  return ret;
}

int ObAdminFlushCache::call_server(const common::ObAddr &server, const obrpc::ObFlushCacheArg &arg)
{
  int ret = OB_SUCCESS;
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), KR(ret));
  } else if (OB_FAIL(ctx_.rpc_proxy_->to(server).flush_cache(arg))) {
    LOG_WARN("request server flush cache failed", KR(ret), K(server));
  }
  return ret;
}

int ObAdminSetTP::execute(const obrpc::ObAdminSetTPArg &arg)
{
  LOG_INFO("start execute set_tp request", K(arg));
  int ret = OB_SUCCESS;
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), KR(ret));
  } else if (OB_FAIL(call_all(arg))) {
    LOG_WARN("execute report replica failed", KR(ret), K(arg));
  }
  LOG_INFO("end execute set_tp request", K(arg));
  return ret;
}

int ObAdminSetTP::call_server(const ObAddr &server)
{
  int ret = OB_SUCCESS;
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), KR(ret));
  } else if (OB_FAIL(ctx_.rpc_proxy_->to(server).set_tracepoint(arg_))) {
    LOG_WARN("request server report replica failed", KR(ret), K(server));
  }
  return ret;
}

int ObAdminSyncRewriteRules::execute(const obrpc::ObSyncRewriteRuleArg &arg)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObAddr, 8> server_list;
  if (OB_FAIL(get_tenant_servers(arg.tenant_id_, server_list))) {
    LOG_WARN("fail to get tenant servers", "tenant_id", arg.tenant_id_, KR(ret));
  } else {
    //call tenant servers;
    for (int64_t j = 0; OB_SUCC(ret) && j < server_list.count(); ++j) {
      if (OB_FAIL(call_server(server_list.at(j), arg))) {
        LOG_WARN("fail to call tenant server",
                 "tenant_id", arg.tenant_id_,
                 "server addr", server_list.at(j),
                 KR(ret));
      }
    }
  }
  server_list.reset();
  return ret;
}

int ObAdminSyncRewriteRules::call_server(const common::ObAddr &server,
                                         const obrpc::ObSyncRewriteRuleArg &arg)
{
  int ret = OB_SUCCESS;
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), KR(ret));
  } else if (OB_ISNULL(ctx_.rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(ctx_.rpc_proxy_->to(server)
                                     .by(arg.tenant_id_)
                                     .as(arg.tenant_id_)
                                     .sync_rewrite_rules(arg))) {
    LOG_WARN("request server sync rewrite rules failed", KR(ret), K(server));
  }
  return ret;
}

} // end namespace rootserver
} // end namespace oceanbase
