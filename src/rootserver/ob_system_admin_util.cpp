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

#include "io/easy_connection.h"
#include "lib/list/ob_dlist.h"
#include "lib/ob_errno.h"
#include "lib/string/ob_string_holder.h"
#include "logservice/leader_coordinator/failure_event.h"
#include "logservice/leader_coordinator/ob_failure_detector.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/ob_table_access_helper.h"
#include "share/rc/ob_tenant_base.h"
#define USING_LOG_PREFIX RS

#include "ob_system_admin_util.h"

#include "lib/time/ob_time_utility.h"
#include "lib/container/ob_array_iterator.h"
#include "share/ob_srv_rpc_proxy.h"
#include "share/ob_rpc_struct.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/config/ob_server_config.h"
#include "share/config/ob_config_manager.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/ob_cluster_version.h"
#include "share/ob_upgrade_utils.h"
#include "share/ob_share_util.h" // ObShareUtil
#include "storage/ob_file_system_router.h"
#include "observer/ob_server_struct.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "observer/omt/ob_multi_tenant.h"
#include "observer/ob_srv_network_frame.h"
#include "ob_server_manager.h"
#include "ob_ddl_operator.h"
#include "ob_zone_manager.h"
#include "ob_ddl_service.h"
#include "ob_unit_manager.h"
#include "ob_root_inspection.h"
#include "ob_root_service.h"
#include "storage/ob_file_system_router.h"
#include "logservice/leader_coordinator/table_accessor.h"
#include "rootserver/freeze/ob_major_freeze_helper.h"
#include "share/ob_cluster_event_history_table_operator.h"//CLUSTER_EVENT_INSTANCE
#include "observer/ob_service.h"
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
    } else if (OB_UNLIKELY(return_code_array.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("return_code_array is empty", KR(ret), K(server));
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
            if (OB_FAIL(ctx_.ddl_service_->get_tenant_schema_guard_with_version_in_inner_table(OB_SYS_TENANT_ID, schema_guard))) {
              LOG_WARN("get_schema_guard failed", KR(ret));
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
                  for (const uint64_t tenant_id: tenant_ids) {
                    if (condition_func(tenant_id) &&
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
              if (OB_SUCC(ret) && OB_FAIL(item->tenant_ids_.push_back(tenant_id))) {
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
            if (OB_FAIL(item->tenant_ids_.push_back(OB_SYS_TENANT_ID))) {
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
        } else if (!ci->set_value(item->value_.ptr())) {
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

int ObAdminSetConfig::update_config(obrpc::ObAdminSetConfigArg &arg, int64_t new_version)
{
  int ret = OB_SUCCESS;
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), KR(ret));
  } else {
    FOREACH_X(item, arg.items_, OB_SUCCESS == ret) {
      char svr_ip[OB_MAX_SERVER_ADDR_SIZE] = "ANY";
      int64_t svr_port = 0;
      if (item->server_.is_valid()) {
        if (false == item->server_.ip_to_string(svr_ip, sizeof(svr_ip))) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("convert server addr to ip failed", KR(ret), "server", item->server_);
        } else {
          svr_port = item->server_.get_port();
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
      } else if (!item->zone_.is_empty()) {
        bool is_zone_exist = false;
        if (OB_FAIL(ctx_.zone_mgr_->check_zone_exist(item->zone_, is_zone_exist))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("check zone exist fail", KR(ret), "zone", item->zone_);
        } else if(!is_zone_exist) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("zone is not exist", KR(ret), "zone", item->zone_);
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "zone");
        }
      }

      if (OB_FAIL(ret)) {
      } else if (item->tenant_ids_.size() > 0 || item->want_to_set_tenant_config_) {
        // tenant config
        ObDMLSqlSplicer dml;
        share::schema::ObSchemaGetterGuard schema_guard;
        const share::schema::ObSimpleTenantSchema *tenant_schema = NULL;
        if (OB_FAIL(GSCHEMASERVICE.get_tenant_schema_guard(
                OB_SYS_TENANT_ID, schema_guard))) {
          LOG_WARN("fail to get sys tenant schema guard", KR(ret));
        } else {
          for (uint64_t tenant_id : item->tenant_ids_) {
            const char *table_name = (ObAdminSetConfig::OB_PARAMETER_SEED_ID == tenant_id ?
                                      OB_ALL_SEED_PARAMETER_TNAME : OB_TENANT_PARAMETER_TNAME);
            tenant_id = (ObAdminSetConfig::OB_PARAMETER_SEED_ID == tenant_id ? OB_SYS_TENANT_ID : tenant_id);
            uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id);
            dml.reset();
            if (OB_FAIL(schema_guard.get_tenant_info(exec_tenant_id, tenant_schema))) {
              LOG_WARN("failed to get tenant ids", KR(ret), K(exec_tenant_id));
            } else if (OB_ISNULL(tenant_schema)) {
              ret = OB_TENANT_NOT_EXIST;
              LOG_WARN("tenant not exist", KR(ret), K(tenant_id));
            } else if (!tenant_schema->is_normal()) {
              //tenant not normal, maybe tenant not ready, cannot add tenant config
            } else if (OB_FAIL(dml.add_pk_column("tenant_id", tenant_id))
                || OB_FAIL(dml.add_pk_column("zone", item->zone_.ptr()))
                || OB_FAIL(dml.add_pk_column("svr_type", print_server_role(OB_SERVER)))
                || OB_FAIL(dml.add_pk_column(K(svr_ip)))
                || OB_FAIL(dml.add_pk_column(K(svr_port)))
                || OB_FAIL(dml.add_pk_column("name", item->name_.ptr()))
                || OB_FAIL(dml.add_column("data_type", "varchar"))
                || OB_FAIL(dml.add_column("value", item->value_.ptr()))
                || OB_FAIL(dml.add_column("info", item->comment_.ptr()))
                || OB_FAIL(dml.add_column("config_version", new_version))) {
              LOG_WARN("add column failed", KR(ret));
            } else if (OB_FAIL(dml.get_values().append_fmt("usec_to_time(%ld)", new_version))) {
              LOG_WARN("append valued failed", KR(ret));
            } else if (OB_FAIL(dml.add_column(false, "gmt_modified"))) {
              LOG_WARN("add column failed", KR(ret));
            } else {
              int64_t affected_rows = 0;
              ObDMLExecHelper exec(*ctx_.sql_proxy_, exec_tenant_id);
              ObConfigItem *ci = nullptr;
              // tenant not exist in RS, use SYS instead
              omt::ObTenantConfigGuard tenant_config(TENANT_CONF(OB_SYS_TENANT_ID));
              if (!tenant_config.is_valid()) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("failed to get tenant config",K(tenant_id),  KR(ret));
              } else if (OB_ISNULL(tenant_config->get_container().get(
                                          ObConfigStringKey(item->name_.ptr())))) {
                ret = OB_ERR_SYS_CONFIG_UNKNOWN;
                LOG_WARN("can't found config item", KR(ret), K(tenant_id), "item", *item);
              } else {
                ci = *(tenant_config->get_container().get(
                                      ObConfigStringKey(item->name_.ptr())));
                if (OB_FAIL(dml.add_column("section", ci->section()))
                            || OB_FAIL(dml.add_column("scope", ci->scope()))
                            || OB_FAIL(dml.add_column("source", ci->source()))
                            || OB_FAIL(dml.add_column("edit_level", ci->edit_level()))) {
                  LOG_WARN("add column failed", KR(ret));
                } else if (OB_FAIL(exec.exec_insert_update(table_name,
                                                          dml, affected_rows))) {
                  LOG_WARN("execute insert update failed", K(tenant_id), KR(ret), "item", *item);
                } else if (is_zero_row(affected_rows) || affected_rows > 2) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("unexpected affected rows", K(tenant_id), K(affected_rows), KR(ret));
                }
              }
            }
            if (OB_FAIL(ret)) {
              break;
            }
          } // for
        }
      } else {
        // sys config
        ObDMLSqlSplicer dml;
        dml.reset();
        if (OB_SYS_TENANT_ID != item->exec_tenant_id_) {
          uint64_t tenant_id = item->exec_tenant_id_;
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected tenant_id", K(tenant_id), KR(ret));
        } else if (OB_FAIL(dml.add_pk_column("zone", item->zone_.ptr()))
                  || OB_FAIL(dml.add_pk_column("svr_type", print_server_role(OB_SERVER)))
                  || OB_FAIL(dml.add_pk_column(K(svr_ip)))
                  || OB_FAIL(dml.add_pk_column(K(svr_port)))
                  || OB_FAIL(dml.add_pk_column("name", item->name_.ptr()))
                  || OB_FAIL(dml.add_column("data_type", "varchar"))
                  || OB_FAIL(dml.add_column("value", item->value_.ptr()))
                  || OB_FAIL(dml.add_column("info", item->comment_.ptr()))
                  || OB_FAIL(dml.add_column("config_version", new_version))) {
          LOG_WARN("add column failed", KR(ret));
        } else if (OB_FAIL(dml.get_values().append_fmt("usec_to_time(%ld)", new_version))) {
          LOG_WARN("append valued failed", KR(ret));
        } else if (OB_FAIL(dml.add_column(false, "gmt_modified"))) {
          LOG_WARN("add column failed", KR(ret));
        } else {
          int64_t affected_rows = 0;
          ObDMLExecHelper exec(*ctx_.sql_proxy_, OB_SYS_TENANT_ID);
          ObConfigItem *ci = nullptr;
          ObConfigItem *const *ci_ptr = GCONF.get_container().get(
                                         ObConfigStringKey(item->name_.ptr()));
          if (OB_ISNULL(ci_ptr)) {
            ret = OB_ERR_SYS_CONFIG_UNKNOWN;
            LOG_WARN("can't found config item", KR(ret), "item", *item);
          } else {
            ci = *ci_ptr;
            if (OB_FAIL(dml.add_column("section", ci->section()))
                        || OB_FAIL(dml.add_column("scope", ci->scope()))
                        || OB_FAIL(dml.add_column("source", ci->source()))
                        || OB_FAIL(dml.add_column("edit_level", ci->edit_level()))) {
              LOG_WARN("add column failed", KR(ret));
            } else if (OB_FAIL(exec.exec_insert_update(OB_ALL_SYS_PARAMETER_TNAME,
                                                       dml, affected_rows))) {
              LOG_WARN("execute insert update failed", KR(ret), "item", *item);
            } else if (is_zero_row(affected_rows) || affected_rows > 2) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected affected rows", K(affected_rows), KR(ret));
            }
          } // else
        } // else
      } // else sys config
    } // FOREACH_X
  }

  if (OB_SUCC(ret)) {
    FOREACH_X(item, arg.items_, OB_SUCCESS == ret) {
      if (item->tenant_ids_.size() > 0) {
        for (uint64_t tenant_id : item->tenant_ids_) {
          if (ObAdminSetConfig::OB_PARAMETER_SEED_ID == tenant_id) {
          } else if (OB_FAIL(OTC_MGR.set_tenant_config_version(tenant_id, new_version))) {
            LOG_WARN("failed to set tenant config version", K(tenant_id), KR(ret));
          } else if(GCTX.omt_->has_tenant(tenant_id) && OB_FAIL(OTC_MGR.got_version(tenant_id, new_version))) {
            LOG_WARN("failed to got version", K(tenant_id), KR(ret));
          }
          if (OB_FAIL(ret)) {
            break;
          }
        } // for
      } else {
        if (OB_FAIL(ctx_.zone_mgr_->update_config_version(new_version))) {
          LOG_WARN("set new config version failed", KR(ret), K(new_version));
        } else if (OB_FAIL(ctx_.config_mgr_->got_version(new_version))) {
          LOG_WARN("config mgr got version failed", KR(ret), K(new_version));
        }
      }
    } // FOREACH_X
  } // if
  return ret;
}

int ObAdminSetConfig::execute(obrpc::ObAdminSetConfigArg &arg)
{
  LOG_INFO("execute set config request", K(arg));
  int ret = OB_SUCCESS;
  int64_t config_version = 0;
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), KR(ret));
  } else if (OB_FAIL(verify_config(arg))) {
    LOG_WARN("verify config failed", KR(ret), K(arg));
  } else if (OB_FAIL(ctx_.zone_mgr_->get_config_version(config_version))) {
    LOG_WARN("get_config_version failed", KR(ret));
  } else {
    const int64_t now = ObTimeUtility::current_time();
    const int64_t new_version = std::max(config_version + 1, now);
    if (OB_FAIL(ctx_.root_service_->set_config_pre_hook(arg))) {
      LOG_WARN("fail to process pre hook", K(arg), KR(ret));
    } else if (OB_FAIL(update_config(arg, new_version))) {
      LOG_WARN("update config failed", KR(ret), K(arg));
    } else if (OB_ISNULL(ctx_.root_service_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error inner stat", KR(ret), K(ctx_.root_service_));
    } else if (OB_FAIL(ctx_.root_service_->set_config_post_hook(arg))) {
      LOG_WARN("fail to set config callback", KR(ret));
    } else {
      LOG_INFO("get new config version", K(new_version), K(arg));
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
  } else if (GCTX.is_standby_cluster()) {
    // standby cluster cannot upgrade virtual schema independently,
    // need to get these information from the primary cluster
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("upgrade virtual schema in standby cluster not allow", KR(ret));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "upgrade virtual schema in standby cluster");
  } else if (OB_ISNULL(ctx_.root_inspection_)
             || OB_ISNULL(ctx_.ddl_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is null", KR(ret), KP(ctx_.root_inspection_), KP(ctx_.ddl_service_));
  } else if (OB_FAIL(ctx_.ddl_service_->get_tenant_schema_guard_with_version_in_inner_table(
             OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("get_schema_guard failed", KR(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_ids(tenant_ids))) {
    LOG_WARN("fail to get tenant ids", KR(ret));
  } else {
    FOREACH(tenant_id, tenant_ids) { // ignore ret
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = execute(*tenant_id, upgrade_cnt))) {
        LOG_WARN("fail to execute upgrade virtual table by tenant", KR(tmp_ret), K(*tenant_id));
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
  }

  const schema_create_func *creator_ptr_array[] = {
        share::virtual_table_schema_creators,
        share::sys_view_schema_creators, NULL };
  ObArray<ObTableSchema> hard_code_tables;
  ObTableSchema table_schema;

  for (const schema_create_func **creator_ptr_ptr = creator_ptr_array;
       OB_SUCC(ret) && OB_NOT_NULL(*creator_ptr_ptr); ++creator_ptr_ptr) {
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
      } else if (OB_FAIL(hard_code_tables.push_back(table_schema))) {
        LOG_WARN("push_back failed", KR(ret), K(tenant_id));
      }
    }
  }

  // remove tables not exist on hard code tables
  ObSchemaGetterGuard schema_guard;
  ObArray<uint64_t> tids;
  if (FAILEDx(ctx_.ddl_service_->get_tenant_schema_guard_with_version_in_inner_table(tenant_id, schema_guard))) {
    LOG_WARN("get_schema_guard failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_table_ids_in_tenant(tenant_id, tids))) {
    LOG_WARN("get_table_ids_in_tenant failed", KR(ret), K(tenant_id));
  } else {
    FOREACH_CNT_X(tid, tids, OB_SUCC(ret)) {
      const ObTableSchema *in_mem_table = NULL;
      if (!is_inner_table(*tid) || is_sys_table(*tid)) {
        continue;
      } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, *tid, in_mem_table))) {
        LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(*tid));
      } else if (OB_ISNULL(in_mem_table)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("table not exist", KR(ret), K(tenant_id), K(*tid));
      } else {
        bool exist = false;
        FOREACH_CNT_X(hard_code_table, hard_code_tables, OB_SUCC(ret) && !exist) {
          if (OB_ISNULL(hard_code_table)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("hard code table is null", KR(ret), K(tenant_id));
          } else if (in_mem_table->get_table_id() == hard_code_table->get_table_id()) {
            exist = true;
          }
        }
        if (!exist) {
          if (FAILEDx(ctx_.ddl_service_->drop_inner_table(*in_mem_table))) {
            LOG_WARN("drop table schema failed", KR(ret), K(tenant_id), KPC(in_mem_table));
          } else if (OB_FAIL(ctx_.ddl_service_->refresh_schema(tenant_id))) {
            LOG_WARN("refresh_schema failed", KR(ret), K(tenant_id));
          }
        }
      }
    }
  }

  // upgrade tables
  FOREACH_CNT_X(hard_code_table, hard_code_tables, OB_SUCC(ret)) {
    if (OB_ISNULL(hard_code_table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("hard code table is null", KR(ret), K(tenant_id));
    } else if (OB_FAIL(ctx_.root_inspection_->check_table_schema(tenant_id, *hard_code_table))) {
      if (OB_SCHEMA_ERROR != ret) {
        LOG_WARN("check table schema failed", KR(ret), K(tenant_id), K(*hard_code_table));
      } else {
        LOG_INFO("table schema need upgrade", K(tenant_id), K(*hard_code_table));
        if (OB_FAIL(upgrade_(tenant_id, *hard_code_table))) {
          LOG_WARN("upgrade failed", KR(ret), K(tenant_id), K(*hard_code_table));
        } else {
          LOG_INFO("update table schema success", K(tenant_id), K(*hard_code_table));
          upgrade_cnt++;
        }
      }
    }
  }
  return ret;
}

int ObAdminUpgradeVirtualSchema::upgrade_(
    const uint64_t tenant_id,
    share::schema::ObTableSchema &table)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *exist_schema = NULL;
  ObSchemaGetterGuard schema_guard;
  if (OB_UNLIKELY(!ctx_.is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(
             is_virtual_tenant_id(tenant_id)
             || OB_INVALID_TENANT_ID == tenant_id)) {
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
    } else if (OB_FAIL(ctx_.ddl_service_->drop_inner_table(*exist_schema))) {
      LOG_WARN("get table schema failed", KR(ret), K(tenant_id),
               "table", table.get_table_name(), "table_id", table.get_table_id());
    } else if (OB_FAIL(ctx_.ddl_service_->get_tenant_schema_guard_with_version_in_inner_table(
               tenant_id, schema_guard))) {
      LOG_WARN("get schema guard in inner table failed", KR(ret), K(tenant_id));
    }
  }
  // 2. try drop table first
  exist_schema = NULL;
  if (FAILEDx(schema_guard.get_table_schema(tenant_id,
                                            table.get_table_id(),
                                            exist_schema))) {
    LOG_WARN("get table schema failed", KR(ret), "table", table.get_table_name(),
             "table_id", table.get_table_id());
    if (OB_TABLE_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    }
  } else if (OB_ISNULL(exist_schema)) {
    // missed table
  } else if (OB_FAIL(ctx_.ddl_service_->drop_inner_table(*exist_schema))) {
    LOG_WARN("drop table schema failed", KR(ret), "table_schema", *exist_schema);
  } else if (OB_FAIL(ctx_.ddl_service_->get_tenant_schema_guard_with_version_in_inner_table(
             tenant_id, schema_guard))) {
    LOG_WARN("get schema guard in inner table failed", KR(ret), K(tenant_id));
  }
  // 3. create table
  if (FAILEDx(ctx_.ddl_service_->add_table_schema(table, schema_guard))) {
    LOG_WARN("add table schema failed", KR(ret), K(tenant_id), K(table));
  } else if (OB_FAIL(ctx_.ddl_service_->refresh_schema(tenant_id))) {
    LOG_WARN("refresh schema failed", KR(ret), K(tenant_id));
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
      // end rolling upgrade, should raise min_observer_version
      const char *min_obs_version_name = "min_observer_version";
      if (OB_FAIL(SVR_TRACER.get_min_server_version(min_server_version))) {
        LOG_WARN("failed to get the min server version", KR(ret));
      } else if (OB_FAIL(item.name_.assign(min_obs_version_name))) {
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
        LOG_USER_ERROR(OB_PARTIAL_FAILED);
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
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), KR(ret));
  } else if (OB_FAIL(ctx_.rpc_proxy_->to(server)
                                     .by(arg.tenant_id_)
                                     .as(arg.tenant_id_)
                                     .load_baseline_v2(arg, res))) {
    LOG_WARN("request server load baseline failed", KR(ret), K(server));
  }
  return ret;
}
#endif

int ObTenantServerAdminUtil::get_tenant_servers(const uint64_t tenant_id, common::ObIArray<ObAddr> &servers)
{
  int ret = OB_SUCCESS;
  // sys tenant, get all servers directly
  if (OB_SYS_TENANT_ID == tenant_id) {
    if (OB_FAIL(get_all_servers(servers))) {
      LOG_WARN("fail to get all servers", KR(ret));
    }
  } else {
    ObArray<uint64_t> pool_ids;
    if (OB_ISNULL(ctx_.unit_mgr_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ctx_.unit_mgr_), KR(ret));
    } else if (!SVR_TRACER.has_build() || !ctx_.unit_mgr_->check_inner_stat()) {
      ret = OB_SERVER_IS_INIT;
      LOG_WARN("server manager or unit manager hasn't built",
               "unit_mgr built", ctx_.unit_mgr_->check_inner_stat(), KR(ret));
    } else if (OB_FAIL(ctx_.unit_mgr_->get_pool_ids_of_tenant(tenant_id, pool_ids))) {
      LOG_WARN("get_pool_ids_of_tenant failed", K(tenant_id), KR(ret));
    } else {
      ObArray<ObUnitInfo> unit_infos;
      for (int64_t i = 0; OB_SUCC(ret) && i < pool_ids.count(); ++i) {
        unit_infos.reuse();
        if (OB_FAIL(ctx_.unit_mgr_->get_unit_infos_of_pool(pool_ids.at(i), unit_infos))) {
          LOG_WARN("get_unit_infos_of_pool failed", "pool_id", pool_ids.at(i), KR(ret));
        } else {
          for (int64_t j = 0; OB_SUCC(ret) && j < unit_infos.count(); ++j) {
            bool is_alive = false;
            const ObUnit &unit = unit_infos.at(j).unit_;
            if (OB_FAIL(SVR_TRACER.check_server_alive(unit.server_, is_alive))) {
              LOG_WARN("check_server_alive failed", "server", unit.server_, KR(ret));
            } else if (is_alive) {
              if (OB_FAIL(servers.push_back(unit.server_))) {
                LOG_WARN("push_back failed", KR(ret));
              }
            }
            if (OB_SUCC(ret)) {
              if (unit.migrate_from_server_.is_valid()) {
                if (OB_FAIL(SVR_TRACER.check_server_alive(
                    unit.migrate_from_server_, is_alive))) {
                  LOG_WARN("check_server_alive failed", "server",
                      unit.migrate_from_server_, KR(ret));
                } else if (is_alive) {
                  if (OB_FAIL(servers.push_back(unit.migrate_from_server_))) {
                    LOG_WARN("push_back failed", KR(ret));
                  }
                }
              }
            }
          } // for unit infos end
        }
      } // for pool ids end
    }
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
