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

#include "lib/time/ob_time_utility.h"
#include "lib/container/ob_array_iterator.h"
#include "share/ob_srv_rpc_proxy.h"
#include "share/ob_rpc_struct.h"
#include "share/partition_table/ob_partition_table_operator.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/config/ob_server_config.h"
#include "share/config/ob_config_manager.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/partition_table/ob_partition_table_iterator.h"
#include "share/ob_cluster_version.h"
#include "share/ob_upgrade_utils.h"
#include "share/ob_multi_cluster_util.h"
#include "observer/ob_server_struct.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "observer/omt/ob_multi_tenant.h"
#include "ob_server_manager.h"
#include "ob_daily_merge_scheduler.h"
#include "ob_leader_coordinator.h"
#include "ob_ddl_operator.h"
#include "ob_zone_manager.h"
#include "ob_ddl_service.h"
#include "ob_unit_manager.h"
#include "ob_root_inspection.h"
#include "ob_lost_replica_checker.h"
#include "ob_root_service.h"
namespace oceanbase {
using namespace common;
using namespace common::hash;
using namespace share;
using namespace share::schema;
using namespace obrpc;

namespace rootserver {

int ObSystemAdminUtil::check_service() const
{
  int ret = OB_SUCCESS;
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ret = ctx_.rs_status_->in_service() ? OB_SUCCESS : OB_CANCELED;
  }
  return ret;
}

int ObSystemAdminUtil::wait_switch_leader(const ObIArray<ObPartitionKey>& keys, const ObIArray<ObAddr>& new_leaders)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::OB_RS_PARTITION_TABLE_TEMP);
  ObPartitionInfo info;
  info.set_allocator(&allocator);

  int64_t start_time = ObTimeUtility::current_time();
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(check_service())) {
    LOG_WARN("check_service failed", K(ret));
  } else if (keys.count() <= 0 || new_leaders.count() <= 0 || keys.count() != new_leaders.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", "keys count", keys.count(), "new_leaders count", new_leaders.count(), K(ret));
  }

  while (OB_SUCC(ret)) {
    bool wait_all_leader_success = true;
    for (int64_t i = 0; OB_SUCC(ret) && i < keys.count(); ++i) {
      info.reuse();
      const ObPartitionKey& key = keys.at(i);
      if (OB_FAIL(ctx_.pt_->get(key.get_table_id(), key.get_partition_id(), info))) {
        LOG_WARN("get partition info failed", K(ret), K(key));
      } else {
        const ObPartitionReplica* r = NULL;
        if (OB_FAIL(info.find_leader_by_election(r))) {
          if (OB_ENTRY_NOT_EXIST == ret) {
            // should continue if some partition has not leader
            ret = OB_SUCCESS;
            wait_all_leader_success = false;
            break;
          } else {
            LOG_WARN("find leader failed", K(ret));
          }
        } else if (NULL == r) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL replica", K(ret));
        } else {
          if (new_leaders.at(i) == r->server_) {
            continue;
          } else {
            wait_all_leader_success = false;
            break;
          }
        }
      }
    }
    if (wait_all_leader_success) {
      break;
    }
    const int64_t now = ObTimeUtility::current_time();
    if (OB_FAIL(ret)) {
    } else if (now >= start_time + WAIT_LEADER_SWITCH_TIMEOUT_US) {
      ret = OB_WAIT_LEADER_SWITCH_TIMEOUT;
      LOG_WARN("wait leader switch timeout", K(keys), K(new_leaders), K(ret));
    } else {
      usleep(static_cast<int>(std::min(
          static_cast<int64_t>(WAIT_LEADER_SWITCH_INTERVAL_US), start_time + WAIT_LEADER_SWITCH_TIMEOUT_US - now)));
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(check_service())) {
        LOG_WARN("check_service failed", K(ret));
      }
    }
  }

  if (OB_FAIL(ret)) {
    LOG_WARN("wait leader switch failed", K(ret), K(keys), K(new_leaders));
  }

  return ret;
}

int ObSystemAdminUtil::wait_switch_leader(const ObPartitionKey& key, const ObAddr& new_leader)
{
  int ret = OB_SUCCESS;
  ObArray<ObPartitionKey> partition_keys;
  ObArray<ObAddr> new_leaders;
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!key.is_valid() || !new_leader.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(key), K(new_leader), K(ret));
  } else if (OB_FAIL(partition_keys.push_back(key))) {
    LOG_WARN("push back partition key failed", K(ret));
  } else if (OB_FAIL(new_leaders.push_back(new_leader))) {
    LOG_WARN("push back new leader failed", K(ret));
  } else if (OB_FAIL(wait_switch_leader(partition_keys, new_leaders))) {
    LOG_WARN("wait switch leader failed", K(ret), K(partition_keys), K(new_leaders));
  }

  return ret;
}

int ObAdminSwitchReplicaRole::execute(const ObAdminSwitchReplicaRoleArg& arg)
{
  LOG_INFO("execute switch replica role request", K(arg));
  ObArenaAllocator allocator(ObModIds::OB_RS_PARTITION_TABLE_TEMP);
  ObPartitionInfo info;
  info.set_allocator(&allocator);
  int ret = OB_SUCCESS;
  bool is_alive = false;
  const ObPartitionKey& key = arg.partition_key_;
  int64_t replica_partition_cnt = OB_INVALID_COUNT;
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (!arg.partition_key_.is_valid()) {
    ObSchemaGetterGuard schema_guard;
    ObString tenant_name;
    uint64_t tenant_id = OB_INVALID_ID;
    tenant_name.assign_ptr(arg.tenant_name_.ptr(), static_cast<int32_t>(strlen(arg.tenant_name_.ptr())));
    if (tenant_name.empty()) {
      tenant_id = OB_INVALID_ID;
    } else if (OB_FAIL(ctx_.schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
      LOG_WARN("get schema manager failed", K(ret));
    } else if (OB_FAIL(schema_guard.get_tenant_id(tenant_name, tenant_id)) || OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
      ret = OB_TENANT_NOT_EXIST;
      LOG_WARN("tenant not exist", K(tenant_name), K(ret));
    }

    if (OB_SUCC(ret)) {
      if (arg.server_.is_valid()) {
        if (OB_FAIL(switch_replica_by_server(arg.server_, tenant_id, arg.role_))) {
          LOG_WARN("switch replica by server failed", "server", arg.server_, K(tenant_id), "role", arg.role_, K(ret));
        }
      } else {
        if (arg.zone_.is_empty()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("zone is empty", "zone", arg.zone_, K(ret));
        } else if (OB_FAIL(switch_replica_by_zone(arg.zone_, tenant_id, arg.role_))) {
          LOG_WARN("switch replica by zone failed", "zone", arg.zone_, K(tenant_id), "role", arg.role_, K(ret));
        }
      }
    }
  } else if (!arg.server_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("server must set", K(arg), K(ret));
  } else if (OB_FAIL(ctx_.server_mgr_->check_server_alive(arg.server_, is_alive))) {
    LOG_WARN("check_server_alive failed", "server", arg.server_, K(ret));
  } else if (!is_alive) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sever not alive", K(arg), K(ret));
  } else if (OB_FAIL(ctx_.pt_->get(key.get_table_id(), key.get_partition_id(), info))) {
    LOG_WARN("get partition table failed", K(ret), K(key));
  } else if (OB_FAIL(info.get_partition_cnt(replica_partition_cnt))) {
    LOG_WARN("fail to get partition cnt", K(ret));
  } else if (arg.partition_key_.get_partition_cnt() != replica_partition_cnt) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("partition_cnt is invalid", K(ret), K(arg), K(replica_partition_cnt));
  } else {
    switch (arg.role_) {
      case LEADER: {
        // ignore partition table get error to set leader when no leader exist.
        ObAddr rpc_server;
        bool replica_exist = false;
        bool normal_replica = false;
        int64_t index = -1;
        FOREACH_CNT(r, info.get_replicas_v2())
        {
          index++;
          if (info.is_leader_like(index)) {
            rpc_server = r->server_;
          }
          if (arg.server_ == r->server_) {
            replica_exist = true;
            if (REPLICA_STATUS_NORMAL == r->replica_status_ &&
                ObReplicaTypeCheck::is_can_elected_replica(r->replica_type_)) {
              normal_replica = true;
            }
          }
        }
        if (!rpc_server.is_valid()) {
          LOG_WARN("partition has no leader");
          // If no leader found, send switch_leader rpc to destination server,
          // to set leader when no leader exist.
          rpc_server = arg.server_;
        }

        if (!replica_exist) {
          ret = OB_PARTITION_NOT_EXIST;
          LOG_WARN("replica not exist", K(info), K(arg), K(ret));
        } else if (!normal_replica) {
          ret = OB_STATE_NOT_MATCH;
          LOG_WARN("replica not normal replica", K(info), K(arg), K(ret));
        } else {
          ObSwitchLeaderArg switch_leader_arg;
          switch_leader_arg.partition_key_ = arg.partition_key_;
          switch_leader_arg.leader_addr_ = arg.server_;
          if (OB_FAIL(ctx_.rpc_proxy_->to(rpc_server).switch_leader(switch_leader_arg))) {
            LOG_WARN("switch leader rpc failed", K(ret), K(rpc_server), K(switch_leader_arg));
          }
        }
        break;
      }
      case FOLLOWER: {  // need to switch other replica to leader
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(switch_replica_to_follower(arg.server_, info))) {
          LOG_WARN("switch_replica_to_follower failed", "server", arg.server_, K(info), K(ret));
        }
        break;
      }
      default: {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("unknown role", "role", arg.role_, K(ret));
      }
    }
  }
  return ret;
}

int ObAdminSwitchReplicaRole::alloc_tenant_id_set(common::hash::ObHashSet<uint64_t>& tenant_id_set)
{
  int ret = OB_SUCCESS;
  if (tenant_id_set.created()) {
    if (OB_FAIL(tenant_id_set.clear())) {
      LOG_WARN("clear tenant id set failed", K(ret));
    }
  } else if (OB_FAIL(tenant_id_set.create(TENANT_BUCKET_NUM))) {
    LOG_WARN("create tenant id set failed", LITERAL_K(TENANT_BUCKET_NUM), K(ret));
  }
  return ret;
}

template <typename T>
int ObAdminSwitchReplicaRole::convert_set_to_array(const common::hash::ObHashSet<T>& set, ObArray<T>& array)
{
  int ret = common::OB_SUCCESS;
  array.reuse();
  if (!set.created()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("set not created", "set created", set.created(), K(ret));
  } else if (OB_FAIL(array.reserve(set.size()))) {
    LOG_WARN("array reserver failed", "capacity", set.size(), K(ret));
  } else {
    for (typename common::hash::ObHashSet<T>::const_iterator iter = set.begin(); OB_SUCCESS == ret && iter != set.end();
         ++iter) {
      if (OB_FAIL(array.push_back(iter->first))) {
        LOG_WARN("push_back failed", K(ret));
      }
    }
  }
  return ret;
}

int ObAdminSwitchReplicaRole::get_partition_infos_by_tenant(
    const ObAddr& server, const uint64_t& tenant_id, ObIArray<ObPartitionInfo>& partition_infos)
{
  int ret = OB_SUCCESS;
  ObTenantPartitionIterator tenant_partition_iter;
  partition_infos.reuse();
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!server.is_valid() || OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(server), K(tenant_id), K(ret));
  } else if (OB_FAIL(tenant_partition_iter.init(*ctx_.pt_, *ctx_.schema_service_, tenant_id, true))) {
    LOG_WARN("init tenant partition iter failed", K(tenant_id), K(ret));
  } else {
    ObPartitionInfo partition_info;
    while (OB_SUCC(ret)) {
      partition_info.reuse();
      const ObPartitionReplica* replica = NULL;
      if (OB_FAIL(tenant_partition_iter.next(partition_info))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("iterate partition failed", K(ret));
        }
      } else if (OB_FAIL(partition_info.find(server, replica))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          // do nothing
        } else {
          LOG_WARN("find server replica failed", K(server), K(ret));
        }
      } else if (OB_FAIL(partition_infos.push_back(partition_info))) {
        LOG_WARN("push back partition info failed", K(ret));
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }

  return ret;
}

int ObAdminSwitchReplicaRole::get_partition_infos_by_server(
    const ObAddr& server, ObIArray<ObPartitionInfo>& partition_infos)
{
  int ret = OB_SUCCESS;
  common::hash::ObHashSet<uint64_t> tenant_id_set;
  ObArray<ObPartitionInfo> tenant_partition_infos;
  partition_infos.reuse();
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), K(ret));
  } else if (OB_FAIL(alloc_tenant_id_set(tenant_id_set))) {
    LOG_WARN("alloc tenant id set failed", K(ret));
  } else if (OB_FAIL(ctx_.unit_mgr_->get_tenants_of_server(server, tenant_id_set))) {
    LOG_WARN("get tenants of server failed", K(server), K(ret));
  } else {
    FOREACH_X(tenant_id, tenant_id_set, OB_SUCCESS == ret)
    {
      tenant_partition_infos.reuse();
      if (OB_FAIL(get_partition_infos_by_tenant(server, tenant_id->first, tenant_partition_infos))) {
        LOG_WARN("get partition infos by tenant failed", K(server), "tenant_id", tenant_id->first, K(ret));
      } else {
        FOREACH_CNT_X(tenant_partition_info, tenant_partition_infos, OB_SUCCESS == ret)
        {
          if (OB_FAIL(partition_infos.push_back(*tenant_partition_info))) {
            LOG_WARN("push_back failed", K(ret));
          }
        }
      }
    }
  }

  return ret;
}

int ObAdminSwitchReplicaRole::get_tenants_of_zone(const ObZone& zone, common::hash::ObHashSet<uint64_t>& tenant_id_set)
{
  int ret = OB_SUCCESS;
  ObArray<ObAddr> server_array;
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (zone.is_empty() || !tenant_id_set.created()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(zone), "tenant_id_set created", tenant_id_set.created(), K(ret));
  } else if (OB_FAIL(ctx_.server_mgr_->get_alive_servers(zone, server_array))) {
    LOG_WARN("get alive servers failed", K(zone), K(ret));
  } else {
    FOREACH_CNT_X(server, server_array, OB_SUCCESS == ret)
    {
      if (OB_FAIL(ctx_.unit_mgr_->get_tenants_of_server(*server, tenant_id_set))) {
        LOG_WARN("get tenants of server failed", "server", *server, K(ret));
      }
    }
  }

  return ret;
}

int ObAdminSwitchReplicaRole::switch_replica_to_leader(const ObAddr& server, const ObPartitionInfo& partition_info)
{
  OB_ASSERT(server.is_valid());
  int ret = OB_SUCCESS;
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!server.is_valid() || !partition_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(server), K(partition_info), K(ret));
  } else {
    ObAddr rpc_server;
    ObSwitchLeaderArg switch_leader_arg;

    const ObPartitionReplica* leader = NULL;
    if (OB_FAIL(partition_info.find_leader_by_election(leader))) {
      LOG_WARN("fail to find leader", K(ret), K(partition_info));
    } else if (OB_ISNULL(leader)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get valid leader", K(ret), K(leader));
    } else {
      rpc_server = leader->server_;
    }
    if (!rpc_server.is_valid()) {
      LOG_WARN("partition has no leader");
      rpc_server = server;
    }
    FOREACH_CNT(r, partition_info.get_replicas_v2())
    {
      if (server == r->server_) {
        switch_leader_arg.partition_key_ = r->partition_key();
        switch_leader_arg.leader_addr_ = server;
        break;
      }
    }
    if (OB_FAIL(ctx_.rpc_proxy_->to(rpc_server).switch_leader(switch_leader_arg))) {
      LOG_WARN("switch leader rpc failed", K(ret), K(rpc_server), K(switch_leader_arg));
    }
  }

  return ret;
}

int ObAdminSwitchReplicaRole::switch_replica_to_follower(const ObAddr& server, const ObPartitionInfo& partition_info)
{
  int ret = OB_SUCCESS;
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!server.is_valid() || !partition_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(server), K(partition_info), K(ret));
  } else if (partition_info.get_replicas_v2().count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("partition has not replica", K(partition_info), K(ret));
  } else {
    bool find = false;
    bool need_switch = false;
    for (int64_t i = 0; i < partition_info.get_replicas_v2().count() && !find; i++) {
      if (server == partition_info.get_replicas_v2().at(i).server_) {
        find = true;
        if (partition_info.is_leader_like(i)) {
          need_switch = true;
        }
      }
    }

    if (!find) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("replica not exist in partition table", K(server), K(partition_info), K(ret));
    } else if (!need_switch) {
      // no need to set ret, just ignore this request
      LOG_WARN("replica not leader, no need to switch to follower", K(server), K(partition_info));
    } else {
      obrpc::ObGetLeaderCandidatesArg arg;
      obrpc::ObGetLeaderCandidatesResult result;
      const ObPartitionReplica& replica = partition_info.get_replicas_v2().at(0);
      const ObAddr cur_leader = server;
      ObAddr new_leader;
      ObPartitionKey pkey(replica.table_id_, replica.partition_id_, replica.partition_cnt_);
      if (OB_FAIL(arg.partitions_.push_back(pkey))) {
        LOG_WARN("push partition key failed", K(ret), K(pkey));
      } else if (OB_FAIL(ctx_.rpc_proxy_->to(cur_leader).get_leader_candidates(arg, result))) {
        LOG_WARN("get leader candidates failed", K(ret), K(cur_leader), K(arg));
      } else if (result.candidates_.empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("no candidate leader", K(ret), K(cur_leader), K(arg));
      } else {
        FOREACH_CNT_X(candidate, result.candidates_.at(0), OB_SUCCESS == ret)
        {
          const ObPartitionReplica* r = NULL;
          if (OB_FAIL(partition_info.find(*candidate, r))) {
            if (OB_ENTRY_NOT_EXIST == ret) {
              ret = OB_SUCCESS;
            } else {
              LOG_WARN("find server replica failed", K(ret));
            }
          } else if (NULL == r) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("NULL replica", K(ret));
          } else {
            if (r->server_ != cur_leader && REPLICA_STATUS_NORMAL == r->replica_status_ &&
                ObReplicaTypeCheck::is_can_elected_replica(r->replica_type_)) {
              bool is_alive = false;
              if (OB_FAIL(ctx_.server_mgr_->check_server_alive(r->server_, is_alive))) {
                LOG_WARN("check_server_alive failed", "server", r->server_, K(ret));
              } else if (is_alive) {
                new_leader = r->server_;
                break;
              }
            }
          }
        }
        if (OB_FAIL(ret)) {
        } else if (!new_leader.is_valid()) {
          ret = OB_ERR_SYS;
          LOG_WARN("can't select a new leader", K(partition_info), K(result), K(server), K(ret));
        } else {
          ObSwitchLeaderArg switch_leader_arg;
          switch_leader_arg.partition_key_ = pkey;
          switch_leader_arg.leader_addr_ = new_leader;
          int temp_ret = 0;
          if (OB_FAIL(ctx_.rpc_proxy_->to(cur_leader).switch_leader(switch_leader_arg))) {
            LOG_WARN("switch leader rpc failed", K(ret), K(cur_leader), K(switch_leader_arg));
          } else if (OB_SUCCESS != (temp_ret = wait_switch_leader(pkey, new_leader))) {
            LOG_WARN("wait switch leader failed", K(pkey), K(new_leader), K(temp_ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObAdminSwitchReplicaRole::get_switch_replica_tenants(
    const ObZone& zone, const ObAddr& server, const uint64_t& tenant_id, ObArray<uint64_t>& tenant_ids)
{
  int ret = OB_SUCCESS;
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (zone.is_empty() && !server.is_valid() && OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("zone, server and tenant_id are all invalid", K(zone), K(server), K(tenant_id), K(ret));
  } else if (OB_INVALID_ID != tenant_id) {
    if (OB_FAIL(tenant_ids.push_back(tenant_id))) {
      LOG_WARN("push back tenant id failed", K(ret));
    }
  } else if (server.is_valid() || !zone.is_empty()) {
    ObHashSet<uint64_t> tenant_id_set;
    if (OB_FAIL(alloc_tenant_id_set(tenant_id_set))) {
      LOG_WARN("alloc tenant id set failed", K(ret));
    } else {
      if (server.is_valid()) {
        if (OB_FAIL(ctx_.unit_mgr_->get_tenants_of_server(server, tenant_id_set))) {
          LOG_WARN("get tenants of server failed", K(server), K(ret));
        }
      } else {
        if (OB_FAIL(get_tenants_of_zone(zone, tenant_id_set))) {
          LOG_WARN("get tenants of zone failed", K(zone), K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(convert_set_to_array(tenant_id_set, tenant_ids))) {
        LOG_WARN("convert set to array failed", K(ret));
      }
    }
  }

  return ret;
}

int ObAdminSwitchReplicaRole::switch_server_to_leader(const ObAddr& server, const uint64_t& tenant_id)
{
  int ret = OB_SUCCESS;
  ObArray<ObPartitionInfo> partition_infos;
  const uint64_t all_core_table_id = combine_id(OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID);
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!server.is_valid()) {
    // tenant_id can be invalid
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), K(ret));
  } else {
    if (OB_INVALID_ID == tenant_id) {
      if (OB_FAIL(get_partition_infos_by_server(server, partition_infos))) {
        LOG_WARN("get partition infos by server failed", K(server), K(ret));
      }
    } else {
      if (OB_FAIL(get_partition_infos_by_tenant(server, tenant_id, partition_infos))) {
        LOG_WARN("get partition infos by tenant failed", K(server), K(tenant_id), K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObArray<ObPartitionKey> partition_keys;
    ObArray<ObAddr> new_leaders;
    ObPartitionInfo* all_core_table_partition_info = NULL;
    FOREACH_CNT_X(partition_info, partition_infos, OB_SUCCESS == ret)
    {
      if (all_core_table_id == partition_info->get_table_id()) {
        all_core_table_partition_info = partition_info;
      } else if (OB_FAIL(switch_replica_to_leader(server, *partition_info))) {
        LOG_WARN("switch replica to leader failed", K(server), "partition_info", *partition_info, K(ret));
      } else {
        int first_index = 0;
        if (partition_info->replica_count() <= 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid partition replica count", "partition_info", *partition_info, K(ret));
        } else if (OB_FAIL(
                       partition_keys.push_back(partition_info->get_replicas_v2().at(first_index).partition_key()))) {
          LOG_WARN("push_back partition key failed", K(ret));
        } else if (OB_FAIL(new_leaders.push_back(server))) {
          LOG_WARN("push back new_leaders failed", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = wait_switch_leader(partition_keys, new_leaders))) {
        LOG_WARN("wait switch leader failed", K(partition_keys), K(new_leaders), K(ret));
      }
    }

    // deal with all core table
    if (OB_SUCCESS == ret && NULL != all_core_table_partition_info) {
      if (OB_FAIL(switch_replica_to_leader(server, *all_core_table_partition_info))) {
        LOG_WARN("switch replica to leader to all core table partition failed",
            K(server),
            "partition_info",
            *all_core_table_partition_info,
            K(ret));
      } else {
        const int first_index = 0;
        if (all_core_table_partition_info->replica_count() <= 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid partition replica count", "partition_info", *all_core_table_partition_info, K(ret));
        } else {
          int tmp_ret = OB_SUCCESS;
          const ObPartitionKey& partition_key =
              all_core_table_partition_info->get_replicas_v2().at(first_index).partition_key();
          if (OB_SUCCESS != (tmp_ret = wait_switch_leader(partition_key, server))) {
            LOG_WARN("wait switch leader failed", K(partition_key), K(server), K(tmp_ret));
          }
        }
      }
    }
  }

  return ret;
}

int ObAdminSwitchReplicaRole::switch_zone_to_leader(const ObZone& zone, const uint64_t& tenant_id)
{
  int ret = OB_SUCCESS;
  ObArray<ObZone> excluded_zones;
  ObArray<ObAddr> excluded_servers;
  ObArray<uint64_t> tenant_ids;
  HEAP_VAR(ObGlobalInfo, global_info)
  {
    ObArray<ObZoneInfo> infos;
    if (!ctx_.is_inited()) {
      ret = OB_NOT_INIT;
      LOG_WARN("not init", K(ret));
    } else if (zone.is_empty()) {
      // tenant_id can be invalid
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(zone), K(ret));
    } else if (OB_FAIL(ctx_.zone_mgr_->get_snapshot(global_info, infos))) {
      LOG_WARN("get_snapshot failed", K(ret));
    } else {
      FOREACH_CNT_X(info, infos, OB_SUCCESS == ret)
      {
        if (zone != info->zone_) {
          if (OB_FAIL(excluded_zones.push_back(info->zone_))) {
            LOG_WARN("push back zone failed", K(ret));
          }
        }
      }
      if (OB_SUCC(ret)) {
        const bool force = true;
        ObAddr invalid_server;
        if (OB_FAIL(get_switch_replica_tenants(zone, invalid_server, tenant_id, tenant_ids))) {
          LOG_WARN("get tenants of zone failed", K(zone), K(invalid_server), K(tenant_id), K(ret));
        } else if (OB_FAIL(ctx_.leader_coordinator_->coordinate_tenants(
                       excluded_zones, excluded_servers, tenant_ids, force))) {
          LOG_WARN("switch zone replica role to follower failed",
              K(excluded_zones),
              K(excluded_servers),
              K(tenant_ids),
              K(force),
              K(ret));
        }
      }
    }
  }
  return ret;
}

int ObAdminSwitchReplicaRole::switch_replica_by_server(
    const ObAddr& server, const uint64_t& tenant_id, const ObRole role)
{
  int ret = OB_SUCCESS;
  bool is_alive = false;
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!server.is_valid()) {  // invalid tenant_id means all tenant
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("server is invalid", K(server), K(ret));
  } else if (OB_FAIL(ctx_.server_mgr_->check_server_alive(server, is_alive))) {
    LOG_WARN("check_server_alive failed", K(server), K(ret));
  } else if (!is_alive) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("server not alive", K(server), K(ret));
  } else if (is_strong_leader(role)) {
    if (OB_FAIL(switch_server_to_leader(server, tenant_id))) {
      LOG_WARN("switch server to leader failed", K(server), K(tenant_id), K(ret));
    }
  } else if (is_follower(role)) {
    const bool force = true;
    ObArray<uint64_t> tenant_ids;
    ObArray<ObZone> excluded_zones;
    ObArray<ObAddr> excluded_servers;
    ObZone invalid_zone;
    if (OB_FAIL(get_switch_replica_tenants(invalid_zone, server, tenant_id, tenant_ids))) {
      LOG_WARN("get tenants of server failed", K(invalid_zone), K(server), K(tenant_id), K(ret));
    } else if (OB_FAIL(excluded_servers.push_back(server))) {
      LOG_WARN("push back server failed", K(ret));
    } else if (OB_FAIL(
                   ctx_.leader_coordinator_->coordinate_tenants(excluded_zones, excluded_servers, tenant_ids, force))) {
      LOG_WARN("switch server replica role to follower failed", K(server), K(tenant_ids), K(force), K(ret));
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("invalid role type", K(ret), K(role));
  }
  return ret;
}

int ObAdminSwitchReplicaRole::switch_replica_by_zone(const ObZone& zone, const uint64_t& tenant_id, const ObRole role)
{
  int ret = OB_SUCCESS;
  bool zone_exist = false;
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (zone.is_empty()) {
    // tenant_id can be invalid
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("zone is empty", K(zone), K(ret));
  } else if (OB_FAIL(ctx_.zone_mgr_->check_zone_exist(zone, zone_exist))) {
    LOG_WARN("check_zone_exist failed", K(zone), K(ret));
  } else if (!zone_exist) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("zone is not exist", K(zone), K(ret));
  } else if (is_strong_leader(role)) {
    if (OB_FAIL(switch_zone_to_leader(zone, tenant_id))) {
      LOG_WARN("switch zone to leader failed", K(zone), K(tenant_id), K(ret));
    }
  } else if (is_follower(role)) {
    ObArray<uint64_t> tenant_ids;
    ObArray<ObZone> excluded_zones;
    ObArray<ObAddr> excluded_servers;
    ObAddr invalid_server;
    const bool force = true;
    if (OB_FAIL(get_switch_replica_tenants(zone, invalid_server, tenant_id, tenant_ids))) {
      LOG_WARN("get coordinate_tenants failed", K(zone), K(invalid_server), K(tenant_id), K(ret));
    } else if (OB_FAIL(excluded_zones.push_back(zone))) {
      LOG_WARN("push back zone failed", K(ret));
    } else if (OB_FAIL(
                   ctx_.leader_coordinator_->coordinate_tenants(excluded_zones, excluded_servers, tenant_ids, force))) {
      LOG_WARN("switch zone replica role to follower failed", K(zone), K(tenant_ids), K(force), K(ret));
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("invalid role type", K(ret), K(role));
  }
  return ret;
}

int ObAdminChangeReplica::execute(const ObAdminChangeReplicaArg& arg)
{
  int ret = OB_SUCCESS;
  LOG_INFO("execute change replica request", K(arg));
  ObPartitionInfo info;
  ObArenaAllocator allocator(ObModIds::OB_RS_PARTITION_TABLE_TEMP);
  info.set_allocator(&allocator);
  ObTypeTransformTask task;
  const ObPartitionSchema* schema = NULL;
  ObSchemaGetterGuard schema_guard;
  const ObSchemaType schema_type =
      (is_new_tablegroup_id(arg.partition_key_.get_table_id()) ? ObSchemaType::TABLEGROUP_SCHEMA
                                                               : ObSchemaType::TABLE_SCHEMA);
  const uint64_t tenant_id = arg.partition_key_.get_tenant_id();
  if (OB_FAIL(ctx_.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret));
  } else if (OB_FAIL(ObPartMgrUtils::get_partition_schema(
                 schema_guard, arg.partition_key_.get_table_id(), schema_type, schema))) {
    LOG_WARN("get table schema failed", K(ret), "table", arg.partition_key_.get_table_id());
  } else if (OB_ISNULL(schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid table schema", K(ret), K(arg));
  } else if (OB_FAIL(ctx_.pt_->get(arg.partition_key_.get_table_id(), arg.partition_key_.get_partition_id(), info))) {
    LOG_WARN("fail to get partition info", K(ret), K(arg));
  } else if (OB_FAIL(update_partition_info(schema_guard, schema, info))) {
    LOG_WARN("fail to update partition info", K(ret), K(info));
  }
  if (OB_FAIL(ret)) {
  } else if (!arg.force_cmd_ && OB_FAIL(check_parameters(arg, info))) {
    LOG_WARN("invalid argument", K(arg));
  } else if (OB_FAIL(build_task(arg, info, task, arg.force_cmd_))) {
    LOG_INFO("fail to build task", K(ret), K(arg), K(info));
  } else if (OB_FAIL(ctx_.rebalance_task_mgr_->add_task(task))) {
    LOG_WARN("fail to add task", K(ret), K(task));
  }
  return ret;
}

int ObAdminChangeReplica::build_task(
    const ObAdminChangeReplicaArg& arg, const ObPartitionInfo& info, ObTypeTransformTask& task, bool force_cmd)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObPartitionReplica* leader = NULL;
  int64_t quorum = 0;
  if (OB_FAIL(info.find_leader_by_election(leader))) {
    LOG_WARN("fail to get leader", K(ret), K(info));
  } else if (OB_ISNULL(leader)) {
    ret = OB_LEADER_NOT_EXIST;
    LOG_WARN("leader not exist", K(ret), K(info));
  } else if (leader->quorum_ <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("quorum is invalid", K(ret), KPC(leader));
  } else {
    quorum = leader->quorum_;
  }
  if (OB_FAIL(ret)) {
  } else {
    ObTypeTransformTaskInfo task_info;
    common::ObArray<ObTypeTransformTaskInfo> task_info_array;
    ObPartitionKey key = arg.partition_key_;
    ObReplicaMember data_source;
    ObReplicaMember src_member;
    const char* comment = balancer::ADMIN_TYPE_TRANSFORM;
    OnlineReplica dst;
    dst.member_ = ObReplicaMember(arg.member_.get_server(),
        ObTimeUtility::current_time(),
        arg.member_.get_replica_type(),
        arg.member_.get_memstore_percent());
    bool is_alive = false;
    int64_t data_version = 0;
    ObDataSourceCandidateChecker type_checker(arg.member_.get_replica_type());
    bool find = false;
    FOREACH_CNT_X(r, info.get_replicas_v2(), OB_SUCCESS == ret)
    {
      if (r->server_ == arg.member_.get_server()) {
        dst.unit_id_ = r->unit_id_;
        dst.zone_ = r->zone_;
        common::ObRegion dst_region = DEFAULT_REGION_NAME;
        if (OB_FAIL(ctx_.zone_mgr_->get_region(dst.zone_, dst_region))) {
          dst_region = DEFAULT_REGION_NAME;
        }
        dst.member_.set_region(dst_region);
        src_member = ObReplicaMember(r->server_, r->member_time_us_, r->replica_type_, r->get_memstore_percent());
      } else if (OB_FAIL(ctx_.server_mgr_->check_server_alive(r->server_, is_alive))) {
        LOG_WARN("check_server_alive failed", "server", r->server_, K(ret));
      } else if (is_alive && type_checker.is_candidate(r->replica_type_) && r->data_version_ > data_version) {
        find = true;
        data_source = ObReplicaMember(r->server_, r->member_time_us_, r->replica_type_, r->get_memstore_percent());
        data_version = r->data_version_;
      }
    }
    if (OB_SUCC(ret) && !find) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("fail to find data source replica", K(ret), K(arg), K(info));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(task_info.build(dst, key, src_member, data_source, quorum, false, true))) {
      LOG_WARN("fail to build type transform", K(ret), K(key));
    } else if (OB_FAIL(task_info_array.push_back(task_info))) {
      LOG_WARN("fail to push back", K(ret));
    } else if (OB_FAIL(task.build(task_info_array, dst.member_.get_server(), comment, force_cmd))) {
      LOG_WARN("fail to build transform task", K(ret));
    }
  }
  return ret;
}

int ObAdminChangeReplica::check_parameters(const ObAdminChangeReplicaArg& arg, const ObPartitionInfo& info)
{
  int ret = OB_SUCCESS;
  HEAP_VAR(ObZoneInfo, zone_info)
  {
    ObPartitionReplica* replica = NULL;
    if (OB_FAIL(check_replica_argument(arg.partition_key_,
            arg.member_.get_server(),
            arg.member_.get_server(),
            zone_info.zone_,
            info,
            replica,
            ObRebalanceTaskType::TYPE_TRANSFORM))) {
      LOG_WARN("fail to check arg info", K(ret), K(arg), K(info));
    } else if (ctx_.zone_mgr_->get_zone(replica->zone_, zone_info)) {
      LOG_WARN("fail to get zone info", K(ret), K(replica), K(arg));
    } else if (common::REPLICA_TYPE_READONLY != arg.member_.get_replica_type() &&
               static_cast<int64_t>(ObZoneType::ZONE_TYPE_READONLY) == zone_info.zone_type_.value_) {
      // only readonly replicas are permitted on readonly zone
      ret = OB_OP_NOT_ALLOW;
      ObAddr dst = replica->server_;
      LOG_WARN("change replica to non-readonly replica in readonly zone is not allowed", K(ret), K(dst), K(zone_info));
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "change replica to non-readonly replica in readonly zone");
    }
  }
  if (OB_FAIL(ret)) {
  } else if (info.in_physical_restore() || info.in_standby_restore()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("in physical restore or standby restore, change replica not allowed", K(ret), K(arg));
  }
  return ret;
}

int ObAdminDropReplica::execute(const ObAdminDropReplicaArg& arg)
{
  int ret = OB_SUCCESS;
  LOG_INFO("execute drop replica request", K(arg));
  ObPartitionInfo info;
  ObArenaAllocator allocator(ObModIds::OB_RS_PARTITION_TABLE_TEMP);
  info.set_allocator(&allocator);
  ObPartitionReplica* dest_replica = NULL;
  const ObPartitionSchema* schema = NULL;
  ObSchemaGetterGuard schema_guard;
  const ObSchemaType schema_type =
      (is_new_tablegroup_id(arg.partition_key_.get_table_id()) ? ObSchemaType::TABLEGROUP_SCHEMA
                                                               : ObSchemaType::TABLE_SCHEMA);
  const uint64_t tenant_id = arg.partition_key_.get_tenant_id();
  if (OB_FAIL(ctx_.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret));
  } else if (OB_FAIL(ObPartMgrUtils::get_partition_schema(
                 schema_guard, arg.partition_key_.get_table_id(), schema_type, schema))) {
    LOG_WARN("get table schema failed", K(ret), "table", arg.partition_key_.get_table_id());
  } else if (OB_ISNULL(schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid table schema", K(ret), K(arg));
  } else if (OB_FAIL(ctx_.pt_->get(arg.partition_key_.get_table_id(), arg.partition_key_.get_partition_id(), info))) {
    LOG_WARN("fail to get partition info", K(ret), K(arg));
  } else if (OB_FAIL(update_partition_info(schema_guard, schema, info))) {
    LOG_WARN("fail to update partition info", K(ret), K(info));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(check_replica_argument(arg.partition_key_,
                 arg.server_,
                 arg.server_,
                 arg.zone_,
                 info,
                 dest_replica,
                 ObRebalanceTaskType::REMOVE_NON_PAXOS_REPLICA))) {
    LOG_WARN("invalid argument", K(arg));
    if (arg.force_cmd_) {
      ret = OB_SUCCESS;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(dest_replica)) {
      const ObPartitionReplica* tmp = NULL;
      if (OB_FAIL(info.find(arg.server_, tmp))) {
        LOG_WARN("fail to find server", K(ret), K(arg));
        if (OB_FAIL(ctx_.root_service_->report_single_replica(arg.partition_key_))) {
          LOG_WARN("fail to force report replica", K(ret));
        } else {
          ret = OB_EAGAIN;
          LOG_WARN("force obs report replica, try it later;");
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(before_process(arg, info))) {
        LOG_WARN("fail to before precess", K(ret), K(arg), K(info));
      } else if (OB_FAIL(build_task(arg, info, dest_replica, arg.force_cmd_))) {
        LOG_INFO("fail to build task", K(ret), K(arg), K(dest_replica));
      }
    }
  }
  return ret;
}

int ObAdminDropReplica::wait_remove_member_finish(const ObAdminDropReplicaArg& arg, const OnlineReplica& remove_member)
{
  int ret = OB_SUCCESS;
  const int64_t now = ObTimeUtility::current_time();
  int64_t timeout_us = 10 * 1000 * 1000;  // 10s
  timeout_us = std::min(THIS_WORKER.get_timeout_remain(), timeout_us);
  int64_t abs_timeout_us = now + timeout_us;
  bool in_ml = true;
  while (OB_SUCC(ret) && in_ml && ObTimeUtility::current_time() < abs_timeout_us) {
    ObArenaAllocator allocator;
    ObPartitionInfo info;
    info.set_allocator(&allocator);
    const ObPartitionReplica* leader = nullptr;
    if (OB_UNLIKELY(nullptr == ctx_.pt_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pt operator null in ctx is null", K(ret));
    } else if (OB_FAIL(ctx_.pt_->get(arg.partition_key_.get_table_id(), arg.partition_key_.get_partition_id(), info))) {
      LOG_WARN("fail to get partition info", K(ret), K(arg));
    } else if (OB_FAIL(info.find_leader_by_election(leader))) {
      LOG_WARN("fail to get leader", K(ret), K(info));
    } else if (OB_UNLIKELY(nullptr == leader)) {
      ret = OB_LEADER_NOT_EXIST;
      LOG_WARN("leader not exist", K(ret), K(info));
    } else {
      const ObPartitionReplica::MemberList& member_list = leader->member_list_;
      const common::ObAddr& server = remove_member.get_server();
      bool found = false;
      for (int64_t i = 0; !found && i < member_list.count(); ++i) {
        const ObPartitionReplica::Member& this_member = member_list.at(i);
        if (server == this_member.server_) {
          found = true;
        }
      }
      if (found) {
        usleep(500 * 1000 /*500ms*/);
      } else {
        in_ml = false;
      }
    }
  }
  if (OB_FAIL(ret)) {
    // bypass
  } else if (in_ml) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("still in leader member list after wait", K(ret));
  } else {
    // good, wait remove member succeed
  }
  return ret;
}

int ObAdminDropReplica::do_remove_paxos_replica(
    const ObPartitionInfo& info, const OnlineReplica& remove_member, const common::ObPartitionKey& key, bool force_cmd)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    ObRemoveMemberTask task;
    ObRemoveMemberTaskInfo task_info;
    common::ObArray<ObRemoveMemberTaskInfo> task_info_array;
    const char* comment = balancer::ADMIN_REMOVE_MEMBER;
    const ObPartitionReplica* leader = NULL;
    if (OB_FAIL(info.find_leader_by_election(leader))) {
      LOG_WARN("fail to get leader", K(ret), K(info));
    } else if (OB_ISNULL(leader)) {
      ret = OB_LEADER_NOT_EXIST;
      LOG_WARN("leader not exist", K(ret), K(info));
    } else if (leader->quorum_ <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("quorum is invalid", K(ret), KPC(leader));
    } else if (OB_FAIL(task_info.build(remove_member, key, leader->quorum_, leader->quorum_))) {
      LOG_WARN("fail to build remove task info", K(ret), K(key), K(remove_member));
    } else if (OB_FAIL(task_info_array.push_back(task_info))) {
      LOG_WARN("fail to push back", K(ret));
    } else if (OB_FAIL(task.build(task_info_array, remove_member.member_.get_server(), comment, force_cmd))) {
      LOG_WARN("fail to build remove member task", K(ret), K(key), K(remove_member));
    } else if (OB_FAIL(ctx_.rebalance_task_mgr_->add_task(task))) {
      LOG_WARN("fail to add task", K(ret), K(task));
    }
  }
  return ret;
}

int ObAdminDropReplica::do_remove_non_paxos_replica(
    const OnlineReplica& remove_member, const common::ObPartitionKey& key, bool force_cmd)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    ObRemoveNonPaxosTask task;
    ObRemoveNonPaxosTaskInfo task_info;
    common::ObArray<ObRemoveNonPaxosTaskInfo> task_info_array;
    const char* comment = balancer::ADMIN_REMOVE_REPLICA;
    if (OB_FAIL(task_info.build(remove_member, key))) {
      LOG_WARN("fail to build remove non paxos replica task info", K(ret), K(key), K(remove_member));
    } else if (OB_FAIL(task_info_array.push_back(task_info))) {
      LOG_WARN("fail to push back", K(ret));
    } else if (OB_FAIL(task.build(task_info_array, remove_member.member_.get_server(), comment, force_cmd))) {
      LOG_WARN("fail to build remove non paxos replcia", K(ret), K(key), K(remove_member));
    } else if (OB_FAIL(ctx_.rebalance_task_mgr_->add_task(task))) {
      LOG_WARN("fail to add task", K(ret), K(task));
    }
  }
  return ret;
}

int ObAdminDropReplica::build_task(
    const ObAdminDropReplicaArg& arg, const ObPartitionInfo& info, const ObPartitionReplica* replica, bool force_cmd)
{
  int ret = OB_SUCCESS;
  OnlineReplica remove_member;
  bool is_remove_member = false;
  if (OB_ISNULL(replica) || !arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(replica), K(arg), K(replica));
  } else {
    remove_member.member_ = ObReplicaMember(arg.server_, replica->member_time_us_, replica->replica_type_);
    remove_member.zone_ = replica->zone_;
    remove_member.unit_id_ = replica->unit_id_;
    ObPartitionKey key = arg.partition_key_;
    is_remove_member = ObReplicaTypeCheck::is_paxos_replica_V2(replica->replica_type_);
    if (is_remove_member) {
      if (OB_FAIL(do_remove_paxos_replica(info, remove_member, key, force_cmd))) {
        LOG_WARN("fail to remove paxos replica", K(ret), K(key), K(remove_member));
      }
    } else {
      if (OB_FAIL(do_remove_non_paxos_replica(remove_member, key, force_cmd))) {
        LOG_WARN("fail to do remove non paxos replica", K(ret), K(key), K(remove_member));
      }
    }
  }
  return ret;
}

int ObAdminDropReplica::before_process(const ObAdminDropReplicaArg& arg, const ObPartitionInfo& info)
{
  int ret = OB_SUCCESS;
  int64_t index = -1;
  ObAddr leader;
  ObAddr alternate_leader;
  // check if this operation need to switch leader
  FOREACH_CNT_X(r, info.get_replicas_v2(), OB_SUCCESS == ret)
  {
    index++;
    const ObPartitionReplica& replica = info.get_replicas_v2().at(index);
    if (replica.is_leader_by_election()) {
      leader = r->server_;
    } else if (r->is_in_service() && ObReplicaTypeCheck::is_can_elected_replica(r->replica_type_)) {
      if (!alternate_leader.is_valid()) {
        bool is_alive = false;
        if (OB_FAIL(ctx_.server_mgr_->check_server_alive(r->server_, is_alive))) {
          LOG_WARN("check_server_alive failed", "server", r->server_, K(ret));
        } else if (is_alive) {
          alternate_leader = r->server_;
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (!leader.is_valid()) {
      // no leader found, don't need to do switch leader, ignore it
      LOG_WARN("no valid leader exist", K(arg));
    } else if (leader == arg.server_) {
      LOG_INFO("try to drop leader replica", K(arg));
      if (!alternate_leader.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("leader need to switch, but has no alternate", K(arg), K(ret));
      } else {
        ObSwitchLeaderArg switch_leader_arg;
        switch_leader_arg.partition_key_ = arg.partition_key_;
        switch_leader_arg.leader_addr_ = alternate_leader;
        if (OB_FAIL(ctx_.rpc_proxy_->to(leader).switch_leader(switch_leader_arg))) {
          LOG_WARN("switch leader failed", K(ret), K(leader), K(switch_leader_arg));
        } else if (OB_FAIL(wait_switch_leader(arg.partition_key_, alternate_leader))) {
          LOG_WARN("wait switch leader failed", K(ret), K(alternate_leader), K(arg));
        }
      }
    }
  }
  return ret;
}

// generate flag replica
int ObSystemAdminUtil::update_partition_info(
    ObSchemaGetterGuard& schema_guard, const ObPartitionSchema* schema, ObPartitionInfo& partition)
{
  int ret = OB_SUCCESS;
  bool has_flag_replica = false;
  const ObPartitionReplica* leader = NULL;
  ObArray<share::ObZoneReplicaNumSet> zone_locality;
  if (OB_ISNULL(schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(schema));
  } else if (OB_FAIL(partition.find_leader_by_election(leader))) {
    LOG_WARN("fail to find leader", K(ret), K(partition));
  } else if (OB_ISNULL(leader)) {
    // nothing todo
    LOG_WARN("get invalid leader", K(ret), K(partition), K(leader));
  } else {
    has_flag_replica = false;
    FOREACH_CNT_X(m, leader->member_list_, OB_SUCC(ret))
    {
      bool found = (m->server_ == leader->server_);
      FOREACH_CNT_X(r, partition.get_replicas_v2(), !found)
      {
        if (r->server_ == m->server_) {
          found = true;
        }
      }
      if (!found) {
        has_flag_replica = true;
        ObPartitionReplica r;
        ObZone zone;
        r.member_time_us_ = m->timestamp_;
        r.server_ = m->server_;
        r.table_id_ = partition.get_table_id();
        r.partition_id_ = partition.get_partition_id();
        r.partition_cnt_ = 0;
        r.unit_id_ = 0;
        r.role_ = FOLLOWER;
        r.in_member_list_ = true;
        r.replica_status_ = REPLICA_STATUS_FLAG;
        // set the replica type to full and memstore_percent to 100 temporarily
        r.replica_type_ = REPLICA_TYPE_FULL;
        r.data_version_ = -1;
        if (OB_FAIL(r.set_memstore_percent(100))) {
          LOG_WARN("fail to set memstore percent", K(ret));
        } else if (OB_FAIL(ctx_.server_mgr_->get_server_zone(m->server_, zone))) {
          LOG_WARN("fail to get zone info", K(ret), K(*m), K(partition));
        } else if (FALSE_IT(r.zone_ = zone)) {
          // shall never be here
        } else if (OB_FAIL(partition.add_replica(r))) {
          LOG_WARN("fail to push back to partition", K(ret), K(partition), K(r));
        }
      }
    }
  }
  if (OB_FAIL(ret) || !has_flag_replica) {
    // no need to check here
  } else if (OB_FAIL(schema->get_zone_replica_attr_array_inherit(schema_guard, zone_locality))) {
    LOG_WARN("fail to get zone replica attr array inherit", K(ret));
  } else {
    FOREACH_CNT_X(r, partition.get_replicas_v2(), OB_SUCC(ret))
    {
      if (ObReplicaTypeCheck::is_paxos_replica_V2(r->replica_type_) && REPLICA_STATUS_FLAG != r->replica_status_) {
        for (int64_t i = 0; OB_SUCC(ret) && i < zone_locality.count(); ++i) {
          share::ObReplicaAttrSet& replica_set = zone_locality.at(i).replica_attr_set_;
          const common::ObIArray<common::ObZone>& zone_set = zone_locality.at(i).zone_set_;
          if (!has_exist_in_array(zone_set, r->zone_)) {
            // bypass
          } else if (REPLICA_TYPE_FULL == r->replica_type_) {
            if (!replica_set.has_this_replica(r->replica_type_, r->get_memstore_percent())) {
              // bypass
            } else if (OB_FAIL(replica_set.sub_full_replica_num(ReplicaAttr(1, r->get_memstore_percent())))) {
              LOG_WARN("fail to sub full replica num", K(ret));
            }
            break;
          } else if (REPLICA_TYPE_LOGONLY == r->replica_type_) {
            if (!replica_set.has_this_replica(r->replica_type_, r->get_memstore_percent())) {
              // bypass
            } else if (OB_FAIL(replica_set.sub_logonly_replica_num(ReplicaAttr(1, r->get_memstore_percent())))) {
              LOG_WARN("fail to sub logonly replica num", K(ret));
            }
          } else {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("not support replica type", K(ret), K(partition));
          }
        }
      }
    }
    FOREACH_CNT_X(r, partition.get_replicas_v2(), OB_SUCC(ret))
    {
      if (REPLICA_STATUS_FLAG == r->replica_status_) {
        for (int64_t i = 0; OB_SUCC(ret) && i < zone_locality.count(); ++i) {
          share::ObReplicaAttrSet& replica_set = zone_locality.at(i).replica_attr_set_;
          const common::ObIArray<common::ObZone>& zone_set = zone_locality.at(i).zone_set_;
          if (!has_exist_in_array(zone_set, r->zone_)) {
            // bypass
          } else if (replica_set.has_this_replica(REPLICA_TYPE_FULL, 100)) {
            r->replica_type_ = REPLICA_TYPE_FULL;
            if (OB_FAIL(r->set_memstore_percent(100))) {
              LOG_WARN("fail to set memstore percent", K(ret));
            } else if (OB_FAIL(replica_set.sub_full_replica_num(ReplicaAttr(1, 100)))) {
              LOG_WARN("fail to sub full replica num", K(ret));
            }
            break;
          } else if (replica_set.has_this_replica(REPLICA_TYPE_FULL, 0)) {
            r->replica_type_ = REPLICA_TYPE_FULL;
            if (OB_FAIL(r->set_memstore_percent(0))) {
              LOG_WARN("fail to set memstore percent", K(ret));
            } else if (OB_FAIL(replica_set.sub_full_replica_num(ReplicaAttr(1, 100)))) {
              LOG_WARN("fail to sub full replica num", K(ret));
            }
            break;
          } else if (replica_set.has_this_replica(REPLICA_TYPE_LOGONLY, 100)) {
            r->replica_type_ = REPLICA_TYPE_LOGONLY;
            if (OB_FAIL(r->set_memstore_percent(100))) {
              LOG_WARN("fail to set memstore percent", K(ret));
            } else if (OB_FAIL(replica_set.sub_logonly_replica_num(ReplicaAttr(1, 100)))) {
              LOG_WARN("fail to sub logonly replica num", K(ret));
            }
            break;
          } else {
            LOG_WARN("redundant replica in member list", K(ret), K(*r));
          }
        }
      }
    }
  }
  return ret;
}

int ObSystemAdminUtil::check_replica_argument(const ObPartitionKey& pkey, const ObAddr& dest, const ObAddr& src,
    const ObZone& dest_zone, const ObPartitionInfo& partition, ObPartitionReplica*& replica,
    const ObRebalanceTaskType& task_type)
{
  int ret = OB_SUCCESS;
  int64_t replica_partition_cnt = OB_INVALID_COUNT;
  bool is_alive = false;
  replica = NULL;
  ObServerStatus src_status;
  if (!pkey.is_valid() || !dest.is_valid() || !partition.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(pkey), K(dest), K(partition));
  } else if (partition.get_replicas_v2().count() <= 0) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("partition not exist in partition_table", K(ret), K(pkey), K(partition));
  } else if (OB_FAIL(const_cast<ObPartitionInfo*>(&partition)->get_partition_cnt(replica_partition_cnt))) {
    LOG_WARN("fail to get partition cnt", K(ret));
  } else if (pkey.get_partition_cnt() != replica_partition_cnt) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("partition_cnt is invalid", K(ret), K(pkey), K(replica_partition_cnt));
  } else if (OB_FAIL(ctx_.server_mgr_->check_server_alive(dest, is_alive))) {
    LOG_WARN("check_server_alive failed", K(ret), "server", dest);
  } else if (!is_alive) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("server is not alive", K(ret), "server", dest);
  } else if (OB_FAIL(ctx_.server_mgr_->get_server_status(src, src_status))) {
    LOG_WARN("fail to get server status", K(ret), K(src));
  } else {
    FOREACH_CNT(r, partition.get_replicas_v2())
    {
      if (r->is_in_service() && r->server_ == dest) {
        replica = const_cast<ObPartitionReplica*>(r);
        break;
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (ObRebalanceTaskType::MIGRATE_REPLICA == task_type || ObRebalanceTaskType::ADD_REPLICA == task_type) {
      ret = OB_ENTRY_NOT_EXIST;
      FOREACH_CNT(r, partition.get_replicas_v2())
      {
        if (src == r->server_) {
          ret = OB_SUCCESS;
          if (!r->is_in_service()) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("source replica not valid in migrate/add replica", K(ret), K(pkey), K(src), K(partition));
          }
          break;
        }
      }
      if (OB_ENTRY_NOT_EXIST == ret) {
        LOG_WARN("fail to find source replica, refuse to migrate/add", K(ret), K(pkey), K(src), K(partition));
      }
    }

    if (OB_SUCC(ret) &&
        (ObRebalanceTaskType::TYPE_TRANSFORM == task_type || ObRebalanceTaskType::MEMBER_CHANGE == task_type ||
            ObRebalanceTaskType::REMOVE_NON_PAXOS_REPLICA == task_type)) {
      ret = OB_ENTRY_NOT_EXIST;
      FOREACH_CNT(r, partition.get_replicas_v2())
      {
        if (dest == r->server_) {
          ret = OB_SUCCESS;
          if (!r->is_in_service()) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("dest replica is not in service", K(ret), K(pkey), K(src), K(partition));
          }
          break;
        }
      }
      if (OB_ENTRY_NOT_EXIST == ret) {
        LOG_WARN("fail to find destination replica, refuse to do type_transform/drop",
            K(ret),
            K(dest),
            K(pkey),
            K(partition));
      }
    }

    if (OB_SUCC(ret) &&
        (ObRebalanceTaskType::MIGRATE_REPLICA == task_type || ObRebalanceTaskType::ADD_REPLICA == task_type)) {
      FOREACH_CNT_X(r, partition.get_replicas_v2(), OB_SUCC(ret))
      {
        if (dest == r->server_ && r->is_in_service()) {
          ret = OB_ENTRY_EXIST;
          LOG_WARN("replica already exist, refuse to do migrate/add", K(ret), K(dest), K(pkey), K(partition));
          break;
        }
      }
      if (OB_SUCC(ret) && ObRebalanceTaskType::MIGRATE_REPLICA == task_type) {
        if (!dest_zone.is_empty() && dest_zone != src_status.zone_) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("can't move replica cross zone", K(dest_zone), K(src_status), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObAdminMigrateReplica::execute(const ObAdminMigrateReplicaArg& arg)
{
  int ret = OB_SUCCESS;
  LOG_INFO("execute migrate replica request", K(arg));
  ObPartitionInfo info;
  ObArenaAllocator allocator(ObModIds::OB_RS_PARTITION_TABLE_TEMP);
  info.set_allocator(&allocator);
  const ObPartitionSchema* schema = NULL;
  ObSchemaGetterGuard schema_guard;
  const ObSchemaType schema_type =
      (is_new_tablegroup_id(arg.partition_key_.get_table_id()) ? ObSchemaType::TABLEGROUP_SCHEMA
                                                               : ObSchemaType::TABLE_SCHEMA);
  const uint64_t tenant_id = arg.partition_key_.get_tenant_id();
  if (OB_FAIL(ctx_.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret));
  } else if (OB_FAIL(ObPartMgrUtils::get_partition_schema(
                 schema_guard, arg.partition_key_.get_table_id(), schema_type, schema))) {
    LOG_WARN("get table schema failed", K(ret), "table", arg.partition_key_.get_table_id());
  } else if (OB_ISNULL(schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid table schema", K(ret), K(arg));
  } else if (OB_FAIL(ctx_.pt_->get(arg.partition_key_.get_table_id(), arg.partition_key_.get_partition_id(), info))) {
    LOG_WARN("fail to get partition info", K(ret), K(arg));
  } else if (OB_FAIL(update_partition_info(schema_guard, schema, info))) {
    LOG_WARN("fail to update partition info", K(ret), K(info));
  }

  if (OB_FAIL(ret)) {
  } else if (!arg.force_cmd_ && OB_FAIL(check_parameters(arg, info))) {
    LOG_WARN("invalid argument", K(arg));
  } else if (OB_FAIL(build_task(arg, info, arg.force_cmd_))) {
    LOG_INFO("fail to build task", K(ret), K(arg), K(info));
  }
  return ret;
}

int ObAdminMigrateReplica::build_task(const ObAdminMigrateReplicaArg& arg, const ObPartitionInfo& info, bool force_cmd)
{
  int ret = OB_SUCCESS;
  int64_t quorum = OB_INVALID_COUNT;
  ObUnit unit;
  ObReplicaMember src_member;
  ObReplicaMember data_source;
  OnlineReplica dst;
  ObZone dest_zone;
  int64_t transmit_data_size = 0;
  const ObPartitionReplica* src = NULL;
  bool is_alive = false;
  const ObTableSchema* schema = NULL;
  ObSchemaGetterGuard schema_guard;
  const ObPartitionReplica* leader = NULL;
  const uint64_t tenant_id = arg.partition_key_.get_tenant_id();
  if (OB_FAIL(info.find_leader_by_election(leader))) {
    LOG_WARN("fail to get leader", K(ret), K(info));
  } else if (OB_ISNULL(leader)) {
    ret = OB_LEADER_NOT_EXIST;
    LOG_WARN("leader not exist", K(ret), K(info));
  } else if (leader->quorum_ <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("quorum is invalid", K(ret), KPC(leader));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ctx_.unit_mgr_->get_unit(arg.partition_key_.get_tenant_id(), arg.dest_, unit))) {
    LOG_WARN("get_unit failed", "tenant_id", arg.partition_key_.get_tenant_id(), "server", arg.dest_, K(ret));
  } else if (OB_FAIL(ctx_.server_mgr_->get_server_zone(arg.dest_, dest_zone))) {
    LOG_WARN("fail to get server zone", K(ret), K(arg.dest_));
  } else if (OB_FAIL(info.find(arg.src_, src))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("find server replica failed", K(ret), "server", arg.src_, K(info));
  } else if (OB_ISNULL(src)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("src is null", K(ret), K(arg));
  } else {
    if (force_cmd) {
      int64_t paxos_num = OB_INVALID_COUNT;
      if (OB_FAIL(ctx_.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
        LOG_WARN("failed to get schema guard", K(ret));
      } else if (OB_FAIL(schema_guard.get_table_schema(arg.partition_key_.get_table_id(), schema))) {
        LOG_WARN("get table schema failed", K(ret), "table", arg.partition_key_.get_table_id());
      } else if (OB_ISNULL(schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid table schema", K(ret), K(arg));
      } else if (OB_FAIL(schema->get_paxos_replica_num(schema_guard, paxos_num))) {
        LOG_WARN("fail to get paxos replica num", K(ret), K(arg));
      } else if (paxos_num <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid paxos num", K(ret), K(paxos_num));
      } else {
        quorum = min(leader->quorum_ + 1, paxos_num);
      }
    } else {
      quorum = leader->quorum_;
    }
    const int64_t now = ObTimeUtility::current_time();
    src_member = ObReplicaMember(src->server_, src->member_time_us_, src->replica_type_, src->get_memstore_percent());
    dst.member_ = ObReplicaMember(arg.dest_, now, src->replica_type_, src->get_memstore_percent());
    dst.unit_id_ = unit.unit_id_;
    dst.zone_ = dest_zone;
    common::ObRegion dst_region = DEFAULT_REGION_NAME;
    if (OB_FAIL(ctx_.zone_mgr_->get_region(dst.zone_, dst_region))) {
      dst_region = DEFAULT_REGION_NAME;
    }
    dst.member_.set_region(dst_region);
    transmit_data_size = src->data_size_;
  }
  if (OB_FAIL(ret)) {
    // bypass
  } else if (arg.is_copy_) {
    // the first candidate as the data source is its source replica,
    // and we need to check the liveness of the source server
  } else if (OB_FAIL(ctx_.server_mgr_->check_server_alive(src_member.get_server(), is_alive))) {
    LOG_WARN("fail to check server is alive", K(ret), K(src_member));
  } else if (is_alive) {
    data_source = src_member;
  } else {
    // select a proper data source
    int64_t data_version = 0;
    FOREACH_CNT_X(r, info.get_replicas_v2(), OB_SUCCESS == ret)
    {
      if (r->server_ == src_member.get_server() || !r->is_in_service() ||
          !ObReplicaTypeCheck::can_as_data_source(dst.member_.get_replica_type(), r->replica_type_)) {
        continue;
      }

      is_alive = false;
      if (OB_FAIL(ctx_.server_mgr_->check_server_alive(r->server_, is_alive))) {
        LOG_WARN("check_server_alive failed", "server", r->server_, K(ret));
      } else if (is_alive && r->data_version_ > data_version) {
        data_source = ObReplicaMember(r->server_, r->member_time_us_, r->replica_type_, r->get_memstore_percent());
        data_version = r->data_version_;
      }
    }  // end FOREACH_CNT_X
    if (OB_FAIL(ret)) {
    } else if (!data_source.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("migrate source not found in partition table", K(arg), K(info), K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    const char* comment = NULL;
    if (arg.is_copy_) {
      comment = balancer::ADMIN_ADD_REPLICA;
    } else {
      comment = balancer::ADMIN_MIGRATE_REPLICA;
    }
    if (arg.is_copy_) {
      ObAddReplicaTask task;
      ObAddTaskInfo task_info;
      common::ObArray<ObAddTaskInfo> task_info_array;
      task_info.set_transmit_data_size(transmit_data_size);
      task_info.set_cluster_id(GCONF.cluster_id);
      if (OB_FAIL(task_info.build(dst, arg.partition_key_, src_member, quorum, false, true))) {
        LOG_WARN("fail to build add task", K(ret), K(arg));
      } else if (OB_FAIL(task_info_array.push_back(task_info))) {
        LOG_WARN("fail to push back", K(ret));
      } else if (OB_FAIL(task.build(task_info_array, dst.member_.get_server(), comment, force_cmd))) {
        LOG_WARN("fail to build add task", K(ret));
      } else if (OB_FAIL(ctx_.rebalance_task_mgr_->add_task(task))) {
        LOG_WARN("fail to add task", K(ret), K(task));
      }
    } else {
      ObMigrateReplicaTask task;
      ObMigrateTaskInfo task_info;
      common::ObArray<ObMigrateTaskInfo> task_info_array;
      task_info.set_transmit_data_size(transmit_data_size);
      task_info.set_cluster_id(GCONF.cluster_id);
      const obrpc::MigrateMode migrate_mode = obrpc::MigrateMode::MT_LOCAL_FS_MODE;
      if (OB_FAIL(
              task_info.build(migrate_mode, dst, arg.partition_key_, src_member, data_source, quorum, false, true))) {
        LOG_WARN("fail to build migrate task", K(ret), K(arg));
      } else if (OB_FAIL(task_info_array.push_back(task_info))) {
        LOG_WARN("fail to push back", K(ret));
      } else if (OB_FAIL(task.build(migrate_mode,
                     task_info_array,
                     dst.member_.get_server(),
                     ObRebalanceTaskPriority::HIGH_PRI,
                     comment,
                     force_cmd))) {
        LOG_WARN("fail to build migrate task", K(ret));
      } else if (OB_FAIL(ctx_.rebalance_task_mgr_->add_task(task))) {
        LOG_WARN("fail to add task", K(ret), K(task));
      }
    }
  }
  return ret;
}

int ObAdminMigrateReplica::check_parameters(const ObAdminMigrateReplicaArg& arg, const ObPartitionInfo& info)
{
  int ret = OB_SUCCESS;
  ObUnit unit;
  ObZone dest_zone;
  ObPartitionReplica* replica = NULL;
  ObRebalanceTaskType task_type =
      arg.is_copy_ ? ObRebalanceTaskType::ADD_REPLICA : ObRebalanceTaskType::MIGRATE_REPLICA;
  if (OB_FAIL(ctx_.server_mgr_->get_server_zone(arg.dest_, dest_zone))) {
    LOG_WARN("failed to get zone", K(ret), K(arg));
  } else if (OB_FAIL(check_replica_argument(
                 arg.partition_key_, arg.dest_, arg.src_, dest_zone, info, replica, task_type))) {
    LOG_WARN("fail to check arg info", K(ret), K(arg), K(info));
  } else if (OB_FAIL(ctx_.unit_mgr_->get_unit(arg.partition_key_.get_tenant_id(), arg.dest_, unit))) {
    LOG_WARN("get_unit failed", "tenant_id", arg.partition_key_.get_tenant_id(), "server", arg.dest_, K(ret));
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("tenant does not have unit on server",
          "tenant_id",
          arg.partition_key_.get_tenant_id(),
          "server",
          arg.dest_,
          K(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "add replica to server without unit");
    }
  } else if (ObUnit::UNIT_STATUS_ACTIVE != unit.status_) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("cannot migrate replica to a unit which is being deleted", K(ret));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "migrate replica to a unit which is being deleted");
  } else if (info.in_physical_restore() || info.in_standby_restore()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("in physical restore or standby restore, migrate replica not allowed", K(ret), K(arg));
  }
  return ret;
}

int ObAdminCallServer::call_all(const ObServerZoneArg& arg)
{
  int ret = OB_SUCCESS;
  ObArray<ObAddr> server_list;
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (arg.server_.is_valid()) {
    bool is_alive = false;
    if (OB_FAIL(ctx_.server_mgr_->check_server_alive(arg.server_, is_alive))) {
      LOG_WARN("fail to check server alive", K(ret), "server", arg.server_);
    } else if (!is_alive) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("server is not alive", K(ret), "server", arg.server_);
    } else if (OB_FAIL(server_list.push_back(arg.server_))) {
      LOG_WARN("push back server failed", K(ret));
    }
  } else {
    if (OB_FAIL(ctx_.server_mgr_->get_alive_servers(arg.zone_, server_list))) {
      LOG_WARN("get alive servers failed", K(ret), K(arg));
    }
  }

  if (OB_SUCC(ret)) {
    FOREACH_CNT(server, server_list)
    {
      int tmp_ret = call_server(*server);
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("call server failed", K(ret), "server", *server);
        ret = OB_SUCCESS == ret ? tmp_ret : ret;
      }
    }
  }
  return ret;
}

int ObAdminReportReplica::execute(const obrpc::ObAdminReportReplicaArg& arg)
{
  LOG_INFO("execute report request", K(arg));
  int ret = OB_SUCCESS;
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(call_all(arg))) {
    LOG_WARN("execute report replica failed", K(ret), K(arg));
  }
  return ret;
}

int ObAdminReportReplica::call_server(const ObAddr& server)
{
  int ret = OB_SUCCESS;
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), K(ret));
  } else if (OB_FAIL(ctx_.rpc_proxy_->to(server).report_replica())) {
    LOG_WARN("request server report replica failed", K(ret), K(server));
  }
  return ret;
}

int ObAdminRecycleReplica::execute(const obrpc::ObAdminRecycleReplicaArg& arg)
{
  LOG_INFO("execute recycle request", K(arg));
  int ret = OB_SUCCESS;
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(call_all(arg))) {
    LOG_WARN("execute recycle replica failed", K(ret), K(arg));
  }
  return ret;
}

int ObAdminRecycleReplica::call_server(const ObAddr& server)
{
  int ret = OB_SUCCESS;
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), K(ret));
  } else if (OB_FAIL(ctx_.rpc_proxy_->to(server).recycle_replica())) {
    LOG_WARN("request server recycle replica failed", K(ret), K(server));
  }
  return ret;
}

int ObAdminClearLocationCache::execute(const obrpc::ObAdminClearLocationCacheArg& arg)
{
  LOG_INFO("execute clear location cache request", K(arg));
  int ret = OB_SUCCESS;
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(call_all(arg))) {
    LOG_WARN("execute clear location cache failed", K(ret), K(arg));
  }
  return ret;
}

int ObAdminClearLocationCache::call_server(const ObAddr& server)
{
  int ret = OB_SUCCESS;
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), K(ret));
  } else if (OB_FAIL(ctx_.rpc_proxy_->to(server).clear_location_cache())) {
    LOG_WARN("request clear location cache failed", K(ret), K(server));
  }
  return ret;
}

int ObAdminReloadGts::execute()
{
  LOG_INFO("execute reload gts request");
  int ret = OB_SUCCESS;
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(ctx_.rs_gts_manager_->load())) {
    LOG_WARN("rs gts manager load failed", K(ret));
  }
  LOG_INFO("finish execute reload gts request", K(ret));
  return ret;
}

int ObAdminReloadUnit::execute()
{
  LOG_INFO("execute reload unit request");
  int ret = OB_SUCCESS;
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(ctx_.unit_mgr_->load())) {
    LOG_WARN("unit manager load failed", K(ret));
  }
  LOG_INFO("finish execute reload unit request", K(ret));
  return ret;
}

int ObAdminReloadServer::execute()
{
  LOG_INFO("execute reload server request");
  int ret = OB_SUCCESS;
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(ctx_.server_mgr_->load_server_manager())) {
    LOG_WARN("build server status failed", K(ret));
  }
  return ret;
}

int ObAdminReloadZone::execute()
{
  LOG_INFO("execute reload zone request");
  int ret = OB_SUCCESS;
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(ctx_.zone_mgr_->reload())) {
    LOG_ERROR("zone manager reload failed", K(ret));
  }
  return ret;
}

int ObAdminClearMergeError::execute()
{
  LOG_INFO("execute clear merge error request");
  int ret = OB_SUCCESS;
  bool merge_error_set = false;
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(ctx_.zone_mgr_->is_merge_error(merge_error_set))) {
    LOG_WARN("is_merge_error failed", K(ret));
  } else if (!merge_error_set) {
    LOG_INFO("merge error flag not set, no need clear");
  } else {
    const int64_t merge_error = 0;
    if (OB_FAIL(ctx_.zone_mgr_->set_merge_error(merge_error))) {
      LOG_WARN("set merge error failed", K(merge_error), K(ret));
    }
  }

  // always wakeup daily merge scheduler
  ctx_.daily_merge_scheduler_->wakeup();

  return ret;
}

int ObAdminMerge::execute(const obrpc::ObAdminMergeArg& arg)
{
  LOG_INFO("execute merge admin request", K(arg));
  int ret = OB_SUCCESS;
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else {
    switch (arg.type_) {
      case ObAdminMergeArg::START_MERGE: {
        if (arg.zone_.is_empty()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("empty zone", K(arg), K(ret));
        } else if (OB_FAIL(ctx_.daily_merge_scheduler_->manual_start_merge(arg.zone_))) {
          LOG_WARN("start merge zone failed", K(ret), K(arg));
        }
        break;
      }
      case ObAdminMergeArg::SUSPEND_MERGE: {
        if (OB_FAIL(ctx_.zone_mgr_->suspend_merge(arg.zone_))) {
          LOG_WARN("suspend merge failed", K(ret), K(arg));
        }
        break;
      }
      case ObAdminMergeArg::RESUME_MERGE: {
        if (OB_FAIL(ctx_.zone_mgr_->resume_merge(arg.zone_))) {
          LOG_WARN("resume merge failed", K(ret), K(arg));
        }
        break;
      }
      default: {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("unsupported merge admin type", "type", arg.type_, K(ret));
      }
    }
  }
  return ret;
}

int ObAdminClearRoottable::execute(const obrpc::ObAdminClearRoottableArg& arg)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> all_tenants;
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else {
    ObSchemaGetterGuard schema_guard;
    if (OB_FAIL(ctx_.schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
      LOG_WARN("get_schema_guard failed", K(ret));
    } else if (!arg.tenant_name_.is_empty()) {
      uint64_t tenant_id = OB_INVALID_ID;
      if (OB_FAIL(schema_guard.get_tenant_id(ObString::make_string(arg.tenant_name_.ptr()), tenant_id)) ||
          OB_INVALID_ID == tenant_id) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("tenant not found", K(arg), K(ret));
      } else if (OB_FAIL(all_tenants.push_back(tenant_id))) {
        LOG_WARN("push back tenant id failed", K(ret));
      }
    } else {
      if (OB_FAIL(ctx_.leader_coordinator_->get_tenant_ids(all_tenants))) {
        LOG_WARN("get all tenant id failed", K(ret));
      }
    }
  }

  // delete partition table
  ObDDLOperator ddl_operator(*ctx_.schema_service_, *ctx_.sql_proxy_);
  if (OB_SUCC(ret)) {
    bool include_sys_tenant = false;
    FOREACH_CNT_X(t, all_tenants, OB_SUCCESS == ret)
    {
      if (OB_SYS_TENANT_ID == *t) {
        include_sys_tenant = true;
        continue;
      }
      if (OB_FAIL(ddl_operator.clear_tenant_partition_table(*t, *ctx_.sql_proxy_))) {
        LOG_WARN("clear tenant partition table failed", K(ret), "tenant_id", *t);
      }
    }
    if (OB_SUCC(ret)) {
      // delete system tenant's partition at last
      if (include_sys_tenant) {
        if (OB_FAIL(ddl_operator.clear_tenant_partition_table(OB_SYS_TENANT_ID, *ctx_.sql_proxy_))) {
          LOG_WARN("clear sys tenant partition table failed", "tenant_id", OB_SYS_TENANT_ID, K(ret));
        } else {
          // clear inmemory partition table
          ctx_.pt_->get_inmemory_table().reuse();
        }
      }
    }

    // force all server report
    if (OB_SUCC(ret)) {
      ObAdminReportReplicaArg report_arg;
      ObAdminReportReplica admin_report(ctx_);
      if (OB_FAIL(admin_report.execute(report_arg))) {
        LOG_WARN("force all server report failed", K(report_arg), K(ret));
      }
    }
  }

  return ret;
}

// FIXME: flush schemas of all tenants
int ObAdminRefreshSchema::execute(const obrpc::ObAdminRefreshSchemaArg& arg)
{
  LOG_INFO("execute refresh schema", K(arg));
  int ret = OB_SUCCESS;
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(ctx_.ddl_service_->refresh_schema(OB_SYS_TENANT_ID))) {
    LOG_WARN("refresh schema failed", K(ret));
  } else {
    if (OB_FAIL(ctx_.schema_service_->get_tenant_schema_version(OB_SYS_TENANT_ID, schema_version_))) {
      LOG_WARN("fail to get schema version", K(ret));
    } else if (OB_FAIL(ctx_.schema_service_->get_refresh_schema_info(schema_info_))) {
      LOG_WARN("fail to get refresh schema info", K(ret), K(schema_info_));
    } else if (!schema_info_.is_valid()) {
      schema_info_.set_schema_version(schema_version_);
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(call_all(arg))) {
      LOG_WARN("execute notify refresh schema failed", K(ret), K(arg));
    }
  }
  return ret;
}

int ObAdminRefreshSchema::call_server(const ObAddr& server)
{
  int ret = OB_SUCCESS;
  ObSwitchSchemaArg arg;
  arg.schema_info_ = schema_info_;
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), K(ret));
  } else if (OB_FAIL(ctx_.rpc_proxy_->to(server).switch_schema(arg))) {
    LOG_WARN("notify switch schema failed", K(ret), K(server), K_(schema_version), K_(schema_info));
  }
  return ret;
}

int ObAdminRefreshMemStat::execute(const ObAdminRefreshMemStatArg& arg)
{
  LOG_INFO("execute refresh memory stat");
  int ret = OB_SUCCESS;
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(call_all(arg))) {
    LOG_WARN("execute notify refresh memory stat failed", K(ret));
  }
  return ret;
}

int ObAdminRefreshMemStat::call_server(const ObAddr& server)
{
  int ret = OB_SUCCESS;
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), K(ret));
  } else if (OB_FAIL(ctx_.rpc_proxy_->to(server).refresh_memory_stat())) {
    LOG_WARN("notify refresh memory stat failed", K(ret), K(server));
  }
  return ret;
}

int ObAdminSetConfig::verify_config(obrpc::ObAdminSetConfigArg& arg)
{
  int ret = OB_SUCCESS;
  void *ptr = nullptr, *cfg_ptr = nullptr;
  ObServerConfigChecker* cfg = nullptr;
  ObTenantConfigChecker* tenant_cfg = nullptr;
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  }
  FOREACH_X(item, arg.items_, OB_SUCCESS == ret)
  {
    if (item->name_.is_empty()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("empty config name", "item", *item, K(ret));
    } else {
      ObConfigItem* ci = nullptr;
      if (OB_SYS_TENANT_ID != item->exec_tenant_id_ || item->tenant_name_.size() > 0) {
        // tenants(user or sys tenants) modify tenant level configuration
        if (nullptr == tenant_cfg) {
          if (OB_ISNULL(cfg_ptr = ob_malloc(sizeof(ObTenantConfigChecker), ObModIds::OB_RS_PARTITION_TABLE_TEMP))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to alloc memory", K(ret));
          } else if (OB_ISNULL(tenant_cfg = new (cfg_ptr) ObTenantConfigChecker())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("new tenant_cfg failed", K(ret));
          }
        }  // if

        if (OB_SUCC(ret)) {
          ObConfigItem* const* ci_ptr = tenant_cfg->get_container().get(ObConfigStringKey(item->name_.ptr()));
          if (OB_ISNULL(ci_ptr)) {
            ret = OB_ERR_SYS_CONFIG_UNKNOWN;
            LOG_WARN("can't found config item", K(ret), "item", *item);
          } else {
            ci = *ci_ptr;
            schema::ObSchemaGetterGuard schema_guard;
            if (OB_FAIL(ctx_.ddl_service_->get_tenant_schema_guard_with_version_in_inner_table(
                    OB_SYS_TENANT_ID, schema_guard))) {
              LOG_WARN("get_schema_guard failed", K(ret));
            } else if (OB_SYS_TENANT_ID == item->exec_tenant_id_ &&
                       item->tenant_name_ == ObFixedLengthString<common::OB_MAX_TENANT_NAME_LENGTH + 1>("all")) {
              common::ObArray<uint64_t> tenant_ids;
              if (OB_FAIL(schema_guard.get_tenant_ids(tenant_ids))) {
                LOG_WARN("get_tenant_ids failed", K(ret));
              } else {
                for (const uint64_t tenant_id : tenant_ids) {
                  if (!is_virtual_tenant_id(tenant_id) && OB_FAIL(item->tenant_ids_.push_back(tenant_id))) {
                    LOG_WARN("add tenant_id failed", K(tenant_id), K(ret));
                    break;
                  }
                }  // for
              }
            } else if (OB_SYS_TENANT_ID == item->exec_tenant_id_ &&
                       item->tenant_name_ == ObFixedLengthString<common::OB_MAX_TENANT_NAME_LENGTH + 1>("seed")) {
              uint64_t tenant_id = OB_PARAMETER_SEED_ID;
              if (OB_FAIL(item->tenant_ids_.push_back(tenant_id))) {
                LOG_WARN("add seed tenant_id failed", K(ret));
                break;
              }
            } else {
              uint64_t tenant_id = OB_INVALID_TENANT_ID;
              if (OB_SYS_TENANT_ID != item->exec_tenant_id_) {
                tenant_id = item->exec_tenant_id_;
              } else {
                if (OB_FAIL(schema_guard.get_tenant_id(ObString(item->tenant_name_.ptr()), tenant_id)) ||
                    OB_INVALID_ID == tenant_id) {
                  ret = OB_ERR_INVALID_TENANT_NAME;
                  LOG_WARN("get_tenant_id failed", K(ret), "tenant", item->tenant_name_);
                }
              }
              if (OB_SUCC(ret) && OB_FAIL(item->tenant_ids_.push_back(tenant_id))) {
                LOG_WARN("add tenant_id failed", K(tenant_id), K(ret));
              }
            }  // else
          }    // else
        }      // if
      } else {
        // sys tenant try to modify configration(cluster level or sys tenant level)
        if (nullptr == cfg) {
          if (OB_ISNULL(ptr = ob_malloc(sizeof(ObServerConfigChecker), ObModIds::OB_RS_PARTITION_TABLE_TEMP))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to alloc memory", K(ret));
          } else if (OB_ISNULL(cfg = new (ptr) ObServerConfigChecker)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("new cfg failed", K(ret));
          }
        }  // if
        if (OB_SUCC(ret) && nullptr == tenant_cfg) {
          if (OB_ISNULL(cfg_ptr = ob_malloc(sizeof(ObTenantConfigChecker), ObModIds::OB_RS_PARTITION_TABLE_TEMP))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to alloc memory", K(ret));
          } else if (OB_ISNULL(tenant_cfg = new (cfg_ptr) ObTenantConfigChecker())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("new tenant_cfg failed", K(ret));
          }
        }  // if

        if (OB_SUCC(ret)) {
          ObConfigItem* const* sys_ci_ptr = cfg->get_container().get(ObConfigStringKey(item->name_.ptr()));
          ObConfigItem* const* tenant_ci_ptr = tenant_cfg->get_container().get(ObConfigStringKey(item->name_.ptr()));
          if (OB_NOT_NULL(sys_ci_ptr)) {
            ci = *sys_ci_ptr;
          } else if (OB_NOT_NULL(tenant_ci_ptr)) {
            ci = *tenant_ci_ptr;
            if (OB_FAIL(item->tenant_ids_.push_back(OB_SYS_TENANT_ID))) {
              LOG_WARN("add tenant_id failed", K(ret));
            }
          } else {
            ret = OB_ERR_SYS_CONFIG_UNKNOWN;
            LOG_WARN("can't found config item", K(ret), "item", *item);
          }
        }  // if
      }    // else

      if (OB_SUCC(ret)) {
        const char* err = NULL;
        if (ci->is_not_editable() && !arg.is_inner_) {
          ret = OB_INVALID_CONFIG;  // TODO: specific report not editable
          LOG_WARN("config is not editable", "item", *item, K(ret));
        } else if (!ci->set_value(item->value_.ptr())) {
          ret = OB_INVALID_CONFIG;
          LOG_WARN("invalid config", "item", *item, K(ret));
        } else if (!ci->check()) {
          ret = OB_INVALID_CONFIG;
          LOG_WARN("invalid value range", "item", *item, K(ret));
        } else if (!ctx_.root_service_->check_config(*ci, err)) {
          ret = OB_INVALID_CONFIG;
          LOG_WARN("invalid value range", "item", *item, K(ret));
        }
        if (OB_FAIL(ret)) {
          if (nullptr != err) {
            LOG_USER_ERROR(OB_INVALID_CONFIG, err);
          }
        }
      }  // if
    }    // else
  }      // FOREACH_X

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

int ObAdminSetConfig::update_config(obrpc::ObAdminSetConfigArg& arg, int64_t new_version)
{
  int ret = OB_SUCCESS;
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else {
    FOREACH_X(item, arg.items_, OB_SUCCESS == ret)
    {
      char svr_ip[OB_MAX_SERVER_ADDR_SIZE] = "ANY";
      int64_t svr_port = 0;
      if (item->server_.is_valid()) {
        if (false == item->server_.ip_to_string(svr_ip, sizeof(svr_ip))) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("convert server addr to ip failed", K(ret), "server", item->server_);
        } else {
          svr_port = item->server_.get_port();
          ObAddr addr;
          bool is_server_exist = false;
          if (false == addr.set_ip_addr(svr_ip, static_cast<int32_t>(svr_port))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("set addr fail", K(ret), "svr_ip", svr_ip, K(svr_port));
          } else if (OB_FAIL(ctx_.server_mgr_->is_server_exist(addr, is_server_exist))) {
            LOG_WARN("check server exist fail", K(addr));
          } else if (!is_server_exist) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("server is not exist", K(ret), K(addr));
            LOG_USER_ERROR(OB_INVALID_ARGUMENT, "server");
          }
        }  // else
      }    // if

      if (OB_FAIL(ret)) {
      } else if (!item->zone_.is_empty()) {
        bool is_zone_exist = false;
        if (OB_FAIL(ctx_.zone_mgr_->check_zone_exist(item->zone_, is_zone_exist))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("check zone exist fail", K(ret), "zone", item->zone_);
        } else if (!is_zone_exist) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("zone is not exist", K(ret), "zone", item->zone_);
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "zone");
        }
      }

      if (OB_FAIL(ret)) {
      } else if (item->tenant_ids_.size() > 0) {
        // tenant config
        ObDMLSqlSplicer dml;
        for (uint64_t tenant_id : item->tenant_ids_) {
          const char* table_name = (ObAdminSetConfig::OB_PARAMETER_SEED_ID == tenant_id ? OB_ALL_SEED_PARAMETER_TNAME
                                                                                        : OB_TENANT_PARAMETER_TNAME);
          tenant_id = (ObAdminSetConfig::OB_PARAMETER_SEED_ID == tenant_id ? OB_SYS_TENANT_ID : tenant_id);
          dml.reset();
          if (OB_FAIL(dml.add_pk_column("zone", item->zone_.ptr())) ||
              OB_FAIL(dml.add_pk_column("svr_type", print_server_role(OB_SERVER))) ||
              OB_FAIL(dml.add_pk_column(K(svr_ip))) || OB_FAIL(dml.add_pk_column(K(svr_port))) ||
              OB_FAIL(dml.add_pk_column("name", item->name_.ptr())) ||
              OB_FAIL(dml.add_column("data_type", "varchar")) || OB_FAIL(dml.add_column("value", item->value_.ptr())) ||
              OB_FAIL(dml.add_column("info", item->comment_.ptr())) ||
              OB_FAIL(dml.add_column("config_version", new_version))) {
            LOG_WARN("add column failed", K(ret));
          } else if (OB_FAIL(dml.get_values().append_fmt("usec_to_time(%ld)", new_version))) {
            LOG_WARN("append valued failed", K(ret));
          } else if (OB_FAIL(dml.add_column(false, "gmt_modified"))) {
            LOG_WARN("add column failed", K(ret));
          } else {
            int64_t affected_rows = 0;
            ObDMLExecHelper exec(*ctx_.sql_proxy_, tenant_id);
            ObConfigItem* ci = nullptr;
            // tenant not exist in RS, use SYS instead
            omt::ObTenantConfigGuard tenant_config(TENANT_CONF(OB_SYS_TENANT_ID));
            if (!tenant_config.is_valid()) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("failed to get tenant config", K(tenant_id), K(ret));
            } else if (OB_ISNULL(tenant_config->get_container().get(ObConfigStringKey(item->name_.ptr())))) {
              ret = OB_ERR_SYS_CONFIG_UNKNOWN;
              LOG_WARN("can't found config item", K(ret), K(tenant_id), "item", *item);
            } else {
              ci = *(tenant_config->get_container().get(ObConfigStringKey(item->name_.ptr())));
              if (OB_FAIL(dml.add_column("section", ci->section())) || OB_FAIL(dml.add_column("scope", ci->scope())) ||
                  OB_FAIL(dml.add_column("source", ci->source())) ||
                  OB_FAIL(dml.add_column("edit_level", ci->edit_level()))) {
                LOG_WARN("add column failed", K(ret));
              } else if (OB_FAIL(exec.exec_insert_update(table_name, dml, affected_rows))) {
                LOG_WARN("execute insert update failed", K(tenant_id), K(ret), "item", *item);
              } else if (is_zero_row(affected_rows) || affected_rows > 2) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unexpected affected rows", K(tenant_id), K(affected_rows), K(ret));
              }
            }
          }
          if (OB_FAIL(ret)) {
            break;
          }
        }  // for
      } else {
        // sys config
        ObDMLSqlSplicer dml;
        dml.reset();
        if (OB_SYS_TENANT_ID != item->exec_tenant_id_) {
          uint64_t tenant_id = item->exec_tenant_id_;
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected tenant_id", K(tenant_id), K(ret));
        } else if (OB_FAIL(dml.add_pk_column("zone", item->zone_.ptr())) ||
                   OB_FAIL(dml.add_pk_column("svr_type", print_server_role(OB_SERVER))) ||
                   OB_FAIL(dml.add_pk_column(K(svr_ip))) || OB_FAIL(dml.add_pk_column(K(svr_port))) ||
                   OB_FAIL(dml.add_pk_column("name", item->name_.ptr())) ||
                   OB_FAIL(dml.add_column("data_type", "varchar")) ||
                   OB_FAIL(dml.add_column("value", item->value_.ptr())) ||
                   OB_FAIL(dml.add_column("info", item->comment_.ptr())) ||
                   OB_FAIL(dml.add_column("config_version", new_version))) {
          LOG_WARN("add column failed", K(ret));
        } else if (OB_FAIL(dml.get_values().append_fmt("usec_to_time(%ld)", new_version))) {
          LOG_WARN("append valued failed", K(ret));
        } else if (OB_FAIL(dml.add_column(false, "gmt_modified"))) {
          LOG_WARN("add column failed", K(ret));
        } else {
          int64_t affected_rows = 0;
          ObDMLExecHelper exec(*ctx_.sql_proxy_, OB_SYS_TENANT_ID);
          ObConfigItem* ci = nullptr;
          ObConfigItem* const* ci_ptr = GCONF.get_container().get(ObConfigStringKey(item->name_.ptr()));
          if (OB_ISNULL(ci_ptr)) {
            ret = OB_ERR_SYS_CONFIG_UNKNOWN;
            LOG_WARN("can't found config item", K(ret), "item", *item);
          } else {
            ci = *ci_ptr;
            if (OB_FAIL(dml.add_column("section", ci->section())) || OB_FAIL(dml.add_column("scope", ci->scope())) ||
                OB_FAIL(dml.add_column("source", ci->source())) ||
                OB_FAIL(dml.add_column("edit_level", ci->edit_level()))) {
              LOG_WARN("add column failed", K(ret));
            } else if (OB_FAIL(exec.exec_insert_update(OB_ALL_SYS_PARAMETER_TNAME, dml, affected_rows))) {
              LOG_WARN("execute insert update failed", K(ret), "item", *item);
            } else if (is_zero_row(affected_rows) || affected_rows > 2) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected affected rows", K(affected_rows), K(ret));
            }
          }  // else
        }    // else
      }      // else sys config
    }        // FOREACH_X
  }

  if (OB_SUCC(ret)) {
    FOREACH_X(item, arg.items_, OB_SUCCESS == ret)
    {
      if (item->tenant_ids_.size() > 0) {
        for (uint64_t tenant_id : item->tenant_ids_) {
          if (ObAdminSetConfig::OB_PARAMETER_SEED_ID == tenant_id) {
          } else if (OB_FAIL(OTC_MGR.set_tenant_config_version(tenant_id, new_version))) {
            LOG_WARN("failed to set tenant config version", K(tenant_id), K(ret));
          } else if (GCTX.omt_->has_tenant(tenant_id) && OB_FAIL(OTC_MGR.got_version(tenant_id, new_version))) {
            LOG_WARN("failed to got version", K(tenant_id), K(ret));
          }
          if (OB_FAIL(ret)) {
            break;
          }
        }  // for
      } else {
        if (OB_FAIL(ctx_.zone_mgr_->update_config_version(new_version))) {
          LOG_WARN("set new config version failed", K(ret), K(new_version));
        } else if (OB_FAIL(ctx_.config_mgr_->got_version(new_version))) {
          LOG_WARN("config mgr got version failed", K(ret), K(new_version));
        }
      }
    }  // FOREACH_X
  }    // if
  return ret;
}

int ObAdminSetConfig::execute(obrpc::ObAdminSetConfigArg& arg)
{
  LOG_INFO("execute set config request", K(arg));
  int ret = OB_SUCCESS;
  int64_t config_version = 0;
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(verify_config(arg))) {
    LOG_WARN("verify config failed", K(ret), K(arg));
  } else if (OB_FAIL(ctx_.zone_mgr_->get_config_version(config_version))) {
    LOG_WARN("get_config_version failed", K(ret));
  } else {
    const int64_t now = ObTimeUtility::current_time();
    const int64_t new_version = std::max(config_version + 1, now);
    if (OB_FAIL(ctx_.root_service_->set_config_pre_hook(arg))) {
      LOG_WARN("fail to process pre hook", K(arg), K(ret));
    } else if (OB_FAIL(update_config(arg, new_version))) {
      LOG_WARN("update config failed", K(ret), K(arg));
    } else if (OB_ISNULL(ctx_.root_service_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error inner stat", K(ret), K(ctx_.root_service_));
    } else if (OB_FAIL(ctx_.root_service_->set_config_post_hook(arg))) {
      LOG_WARN("fail to set config callback", K(ret));
    } else {
      LOG_INFO("get new config version", K(new_version), K(arg));
    }
  }
  return ret;
}

int ObAdminMigrateUnit::execute(const ObAdminMigrateUnitArg& arg)
{
  int ret = OB_SUCCESS;
  LOG_INFO("execute migrate unit request", K(arg));
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else {
    const uint64_t unit_id = arg.unit_id_;
    const ObAddr& dst = arg.destination_;
    if (OB_FAIL(ctx_.unit_mgr_->admin_migrate_unit(unit_id, dst, ctx_.rebalance_task_mgr_, arg.is_cancel_))) {
      LOG_WARN("migrate unit failed", K(unit_id), K(dst), K(ret));
    } else {
      ctx_.root_balancer_->wakeup();
    }
  }
  return ret;
}

int ObAdminUpgradeVirtualSchema::execute()
{
  int ret = OB_SUCCESS;
  int64_t upgrade_cnt = 0;
  const schema_create_func* creator_ptr_array[] = {
      share::virtual_table_schema_creators, share::sys_view_schema_creators, NULL};
  LOG_INFO("execute upgrade virtual schema request");
  ObArray<ObTableSchema> hard_code_tables;
  ObTableSchema table_schema;

  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (GCTX.is_standby_cluster()) {
    // standby cluster cannot upgrade virtual schema independently,
    // need to get these information from the primary cluster
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("upgrade virtual schema in standby cluster not allow", K(ret));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "upgrade virtual schema in standby cluster");
  }

  for (const schema_create_func** creator_ptr_ptr = creator_ptr_array; OB_SUCCESS == ret && NULL != *creator_ptr_ptr;
       ++creator_ptr_ptr) {
    for (const schema_create_func* creator_ptr = *creator_ptr_ptr; OB_SUCCESS == ret && NULL != *creator_ptr;
         ++creator_ptr) {
      table_schema.reset();
      if (OB_FAIL((*creator_ptr)(table_schema))) {
        LOG_WARN("create table schema failed", K(ret));
      } else {
        // only check and upgrade virtual table && sys views
        if (is_sys_table(table_schema.get_table_id())) {
          continue;
        } else if (OB_FAIL(hard_code_tables.push_back(table_schema))) {
          LOG_WARN("push_back failed", K(ret));
        }
      }
    }
  }

  // remove tables not exist on hard code tables
  ObSchemaGetterGuard schema_guard;
  ObArray<const ObTableSchema*> in_mem_tables;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ctx_.ddl_service_->get_tenant_schema_guard_with_version_in_inner_table(
                 OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("get_schema_guard failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schemas_in_tenant(OB_SYS_TENANT_ID, in_mem_tables))) {
    LOG_WARN("get_table_schemas_in_tenant failed", "tenant_id", static_cast<uint64_t>(OB_SYS_TENANT_ID), K(ret));
  } else {
    FOREACH_CNT_X(in_mem_table, in_mem_tables, OB_SUCCESS == ret)
    {
      if (!is_inner_table((*in_mem_table)->get_table_id()) || is_sys_table((*in_mem_table)->get_table_id())) {
        continue;
      }
      bool exist = false;
      FOREACH_CNT_X(hard_code_table, hard_code_tables, OB_SUCCESS == ret && !exist)
      {
        if ((*in_mem_table)->get_table_id() == hard_code_table->get_table_id()) {
          exist = true;
          // process the virtual table index migration overlapping problems
          // originally, MySQL virtual table index id is under 19999(OB_MAX_VIRTUAL_TABLE_ID-1),
          // then migrate to 14999(OB_MAX_MYSQL_VIRTUAL_TABLE_ID-1)
          // 19999 is used as the Oracle virtual table index ID
          if (combine_id(OB_SYS_TENANT_ID, OB_MAX_VIRTUAL_TABLE_ID - 1) == hard_code_table->get_table_id()) {
            exist = false;
          }
        }
      }
      if (!exist) {
        if (OB_FAIL(ctx_.ddl_service_->drop_inner_table(**in_mem_table))) {
          LOG_WARN("drop table schema failed", K(ret), "table_schema", **in_mem_table);
        } else if (OB_FAIL(ctx_.ddl_service_->refresh_schema(OB_SYS_TENANT_ID))) {
          LOG_WARN("refresh_schema failed", K(ret));
        }
      }
    }
  }

  // upgrade tables
  FOREACH_CNT_X(hard_code_table, hard_code_tables, OB_SUCCESS == ret)
  {
    ret = ctx_.root_inspection_->check_table_schema(*hard_code_table);
    if (OB_FAIL(ret)) {
      if (OB_SCHEMA_ERROR != ret) {
        LOG_WARN("check table schema failed", K(ret), "table_schema", *hard_code_table);
      } else {
        LOG_INFO("table schema need upgrade", "table_schema", *hard_code_table);
        if (OB_FAIL(upgrade(*hard_code_table))) {
          LOG_WARN("upgrade failed", "hard_code_table", *hard_code_table, K(ret));
        } else {
          LOG_INFO("update table schema success", "table_schema", *hard_code_table);
          upgrade_cnt++;
        }
      }
    }
  }

  if (OB_SUCCESS == ret && upgrade_cnt > 0) {
    // if schema upgraded, inspect schema again
    int tmp_ret = ctx_.root_inspection_->check_all();
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("root inspection failed", K(tmp_ret));
    }
  }

  return ret;
}

int ObAdminUpgradeVirtualSchema::upgrade(share::schema::ObTableSchema& table)
{
  int ret = OB_SUCCESS;
  const ObTableSchema* exist_schema = NULL;
  ObSchemaGetterGuard schema_guard;
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!table.is_valid() || is_sys_table(table.get_table_id())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table", K(table), K(ret));
  } else if (OB_FAIL(ctx_.ddl_service_->get_tenant_schema_guard_with_version_in_inner_table(
                 OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("get schema guard in inner table failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(table.get_tenant_id(),
                 table.get_database_id(),
                 table.get_table_name(),
                 table.is_index_table(),
                 exist_schema))) {
    // check if the table_name is occupied by others
    LOG_WARN("get table schema failed", K(ret), "table", table.get_table_name());
    if (OB_TABLE_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    }
  } else if (OB_NOT_NULL(exist_schema)) {
    // name modification except virtual table should first delete the old table,
    // then create the new one to make virtual table upgrade valid
    if (OB_FAIL(ctx_.ddl_service_->drop_inner_table(*exist_schema))) {
      LOG_WARN("get table schema failed", K(ret), "table", table.get_table_name(), "table_id", table.get_table_id());
    }
  }

  // rebuild the inner table
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ctx_.ddl_service_->add_table_schema(table))) {
    LOG_WARN("add table schema failed", K(ret), K(table));
  } else if (OB_FAIL(ctx_.ddl_service_->refresh_schema(OB_SYS_TENANT_ID))) {
    LOG_WARN("refresh schema failed", K(ret));
  }

  return ret;
}

int ObAdminUpgradeVirtualSchema::execute(const share::schema::ObSchemaOperation& operation)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!is_virtual_table(operation.table_id_) && !is_sys_view(operation.table_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid operation", K(ret), K(operation));
  } else if (OB_FAIL(ctx_.ddl_service_->get_tenant_schema_guard_with_version_in_inner_table(
                 OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret));
  } else if (share::schema::OB_DDL_CREATE_TABLE == operation.op_type_ ||
             share::schema::OB_DDL_CREATE_VIEW == operation.op_type_ ||
             share::schema::OB_DDL_CREATE_INDEX == operation.op_type_) {
    // need to get the table schema from hard code, may not exist in memory
    bool is_exist = false;
    if (OB_FAIL(schema_guard.check_table_exist(operation.table_id_, is_exist))) {
      LOG_WARN("failed to check table exist", K(ret), K(operation));
    } else if (is_exist) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("can not process exist schema", K(ret), K(operation));
    } else {
      // tables to be added shall never exist in memory
      const schema_create_func* creator_ptr_array[] = {
          share::virtual_table_schema_creators, share::sys_view_schema_creators, NULL};
      ObArray<ObTableSchema> hard_code_tables;
      ObTableSchema hard_code_table;
      bool found = false;
      for (const schema_create_func** creator_ptr_ptr = creator_ptr_array; OB_SUCC(ret) && NULL != *creator_ptr_ptr;
           ++creator_ptr_ptr) {
        for (const schema_create_func* creator_ptr = *creator_ptr_ptr; OB_SUCC(ret) && NULL != *creator_ptr;
             ++creator_ptr) {
          hard_code_table.reset();
          if (OB_FAIL((*creator_ptr)(hard_code_table))) {
            LOG_WARN("create table schema failed", K(ret));
          } else if (operation.table_id_ == hard_code_table.get_table_id()) {
            found = true;
            ObTableSchema new_schema;
            if (OB_FAIL(new_schema.assign(hard_code_table))) {
              LOG_WARN("failed to assign table schema", K(ret), K(hard_code_table));
            } else if (OB_FAIL(ctx_.ddl_service_->add_table_schema(new_schema))) {
              LOG_WARN("failed to add table schema", K(ret), K(operation), K(new_schema));
            }
            break;
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (!found) {
        // did not find in hard code
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("failed to find schema", K(ret), K(operation));
      }
    }
  } else if (share::schema::OB_DDL_DROP_TABLE == operation.op_type_ ||
             share::schema::OB_DDL_DROP_VIEW == operation.op_type_ ||
             share::schema::OB_DDL_DROP_INDEX == operation.op_type_) {
    // need to find in the memory, the table to be deleted should be fetched from the memory,
    // whether it exists in hard code is not determined
    const ObTableSchema* table_schema = NULL;
    if (OB_FAIL(schema_guard.get_table_schema(operation.table_id_, table_schema))) {
      LOG_WARN("failed to get table schema", K(ret), K(operation));
    } else if (OB_FAIL(ctx_.ddl_service_->drop_inner_table(*table_schema))) {
      LOG_WARN("failed to drop inner table", K(ret), K(table_schema));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected operation type", K(ret), K(operation));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ctx_.ddl_service_->refresh_schema(OB_SYS_TENANT_ID))) {
      LOG_WARN("failed to refresh schema", K(ret));
    }
  }
  return ret;
}
int ObAdminUpgradeCmd::execute(const Bool& upgrade)
{
  int ret = OB_SUCCESS;
  char min_server_version[OB_SERVER_VERSION_LENGTH];
  uint64_t current_version = 0;
  if (OB_FAIL(ctx_.server_mgr_->get_min_server_version(min_server_version))) {
    LOG_WARN("failed to get the min server version", K(ret));
  } else if (OB_FAIL(ObClusterVersion::get_version(min_server_version, current_version))) {
    LOG_WARN("fail to parse current version", K(ret), K(min_server_version));
  } else {
    // set min_observer_version and enable_upgrade_mode
    ObAdminSetConfigItem min_obs_version_item;
    ObAdminSetConfigItem enable_upgrade_item;
    obrpc::ObAdminSetConfigArg set_config_arg;
    set_config_arg.is_inner_ = true;
    const char* min_obs_version_name = "min_observer_version";
    const char* enable_upgrade_name = "enable_upgrade_mode";
    ObAdminSetConfig admin_set_config(ctx_);

    if (OB_FAIL(min_obs_version_item.name_.assign(min_obs_version_name))) {
      LOG_WARN("assign min_observer_version config name failed", K(ret));
    } else if (OB_FAIL(enable_upgrade_item.name_.assign(enable_upgrade_name))) {
      LOG_WARN("assign enable_upgrade_mode config name failed", K(ret));
    } else if (OB_FAIL(min_obs_version_item.value_.assign(min_server_version))) {
      LOG_WARN("assign min_observer_version config value failed", K(ret));
    } else if (OB_FAIL(enable_upgrade_item.value_.assign((upgrade ? "true" : "false")))) {
      LOG_WARN("assign enable_upgrade_mode config value failed", K(ret));
    } else if (OB_FAIL(set_config_arg.items_.push_back(min_obs_version_item))) {
      LOG_WARN("add min_observer_version config item failed", K(ret));
    } else if (OB_FAIL(set_config_arg.items_.push_back(enable_upgrade_item))) {
      LOG_WARN("add enable_upgrade_mode config item failed", K(ret));
    } else if (current_version >= CLUSTER_VERSION_2250) {
      ObAdminSetConfigItem upgrade_stage_item;
      const char* upgrade_stage_name = "_upgrade_stage";
      obrpc::ObUpgradeStage stage = upgrade ? obrpc::OB_UPGRADE_STAGE_PREUPGRADE : obrpc::OB_UPGRADE_STAGE_NONE;
      if (OB_FAIL(upgrade_stage_item.name_.assign(upgrade_stage_name))) {
        LOG_WARN("assign _upgrade_stage config name failed", K(ret), K(upgrade));
      } else if (OB_FAIL(upgrade_stage_item.value_.assign(obrpc::get_upgrade_stage_str(stage)))) {
        LOG_WARN("assign _upgrade_stage config value failed", K(ret), K(stage), K(upgrade));
      } else if (OB_FAIL(set_config_arg.items_.push_back(upgrade_stage_item))) {
        LOG_WARN("add _upgrade_stage config item failed", K(ret), K(stage), K(upgrade));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (admin_set_config.execute(set_config_arg)) {
      LOG_WARN("execute set config failed", K(ret));
    } else {
      LOG_INFO("change upgrade parameters",
          "min_observer_version",
          min_server_version,
          "enable_upgrade_mode",
          upgrade,
          "in_major_version_upgrade_mode",
          GCONF.in_major_version_upgrade_mode());
    }
  }
  return ret;
}

int ObAdminRollingUpgradeCmd::execute(const obrpc::ObAdminRollingUpgradeArg& arg)
{
  int ret = OB_SUCCESS;
  ObAdminSetConfigItem upgrade_stage_item;
  obrpc::ObAdminSetConfigArg set_config_arg;
  set_config_arg.is_inner_ = true;
  const char* upgrade_stage_name = "_upgrade_stage";
  ObAdminSetConfig admin_set_config(ctx_);

  if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(arg));
  } else if (OB_FAIL(upgrade_stage_item.name_.assign(upgrade_stage_name))) {
    LOG_WARN("assign _upgrade_stage config name failed", K(ret), K(arg));
  } else if (OB_FAIL(upgrade_stage_item.value_.assign(obrpc::get_upgrade_stage_str(arg.stage_)))) {
    LOG_WARN("assign _upgrade_stage config value failed", K(ret), K(arg));
  } else if (OB_FAIL(set_config_arg.items_.push_back(upgrade_stage_item))) {
    LOG_WARN("add _upgrade_stage config item failed", K(ret), K(arg));
  } else if (admin_set_config.execute(set_config_arg)) {
    LOG_WARN("execute set config failed", K(ret));
  } else {
    LOG_INFO("change upgrade parameters", K(ret), "_upgrade_stage", arg.stage_);
  }
  return ret;
}

DEFINE_ENUM_FUNC(ObInnerJob, inner_job, OB_INNER_JOB_DEF);

int ObAdminRunJob::execute(const ObRunJobArg& arg)
{
  int ret = OB_SUCCESS;
  ObInnerJob job = INVALID_INNER_JOB;
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (INVALID_INNER_JOB == (job = get_inner_job_value(arg.job_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid inner job", K(arg), K(ret));
  } else {
    switch (job) {
      case CHECK_PARTITION_TABLE: {
        ObAdminCheckPartitionTable job_executor(ctx_);
        if (OB_FAIL(job_executor.execute(arg))) {
          LOG_WARN("execute job failed", K(arg), K(ret));
        }
        break;
      }
      case ROOT_INSPECTION: {
        ObAdminRootInspection job_executor(ctx_);
        if (OB_FAIL(job_executor.execute(arg))) {
          LOG_WARN("execute job failed", K(arg), K(ret));
        }
        break;
      }
      case UPGRADE_STORAGE_FORMAT_VERSION: {
        ObAdminUpgradeStorageFormatVersionExecutor job_executor(ctx_);
        if (OB_FAIL(job_executor.execute(arg))) {
          LOG_WARN("fail to execute upgrade storage format version job", K(ret));
        }
        break;
      }
      case CREATE_SEQUENCE_TABLE:
      case CREATE_META_TABLE:
      case CREATE_GC_TABLE:
      case SEQUENCE_MIGRATION:
      case ROLLBACK_METATABLE_MIGRATION:
      case METATABLE_MIGRATION:
      case BUILD_GC_PARTITION:
      case SCHEMA_SPLIT:
      case MIGRATE_PARTITION_META_TABLE:
      case STOP_MIGRATE_PARTITION_META_TABLE: {
        ret = OB_NOT_SUPPORTED;
        break;
      }
      case STOP_SCHEMA_SPLIT:
      case SCHEMA_SPLIT_V2: {
        ObAdminSchemaSplitExecutor job_executor(ctx_);
        if (OB_FAIL(job_executor.execute(arg))) {
          LOG_WARN("execute job failed", K(arg), K(ret));
        }
        break;
      }
      case STATISTIC_PRIMARY_ZONE_ENTITY_COUNT: {
        ObAdminStatisticPrimaryZoneEntityCount job_executor(ctx_);
        if (OB_FAIL(job_executor.execute(arg))) {
          LOG_WARN("execute job failed", K(ret));
        }
        break;
      }
      case CREATE_HA_GTS_UTIL: {
        ObAdminCreateHaGtsUtil job_executor(ctx_);
        if (OB_FAIL(job_executor.execute(arg))) {
          LOG_WARN("execute job failed", K(ret));
        }
        break;
      }
      case CREATE_INNER_SCHEMA: {
        ObAdminCreateInnerSchema job_executor(ctx_);
        if (OB_FAIL(job_executor.execute(arg))) {
          LOG_WARN("execute job failed", K(ret));
        }
        break;
      }
      case SCHEMA_REVISE: {
        ObAdminSchemaRevise job_executor(ctx_);
        if (OB_FAIL(job_executor.execute(arg))) {
          LOG_WARN("execute job failed", K(ret));
        }
        break;
      }
      case UPDATE_TABLE_SCHEMA_VERSION: {
        ObAdminUpdateTableSchemaVersion job_executor(ctx_);
        if (OB_FAIL(job_executor.execute(arg))) {
          LOG_WARN("execute job failed", K(ret));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("not known job", K(job), K(ret));
        break;
      }
    }
  }
  return ret;
}

int ObAdminUpdateTableSchemaVersion::execute(const obrpc::ObRunJobArg& arg)
{
  int ret = OB_SUCCESS;
  LOG_INFO("execute update table schema version request", K(arg));
  ObUpdateTableSchemaVersionArg schema_arg;
  schema_arg.tenant_id_ = OB_SYS_TENANT_ID;
  schema_arg.table_id_ = 0;
  schema_arg.schema_version_ = OB_INVALID_SCHEMA_VERSION;

  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (GCTX.is_standby_cluster()) {
    // no need to check schema validation for standby cluster
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("upgrade table schema in standby cluster not allow", KR(ret));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "upgrade table schema version in standby cluster");
  } else if (OB_FAIL(ctx_.root_service_->update_table_schema_version(schema_arg))) {
    LOG_WARN("fail to update table schema version", KR(ret));
  } else {
    LOG_INFO("update table schema version success");
  }
  return ret;
}

int ObAdminCheckPartitionTable::execute(const obrpc::ObRunJobArg& arg)
{
  int ret = OB_SUCCESS;
  LOG_INFO("execute check partition table request", K(arg));
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (!arg.server_.is_valid() && arg.zone_.is_empty()) {
    // rs check lost replica
    ctx_.root_service_->submit_lost_replica_checker_task();
  } else if (OB_FAIL(call_all(arg))) {
    LOG_WARN("execute check partition table failed", K(ret), K(arg));
  }
  return ret;
}

int ObAdminCheckPartitionTable::call_server(const ObAddr& server)
{
  int ret = OB_SUCCESS;
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), K(ret));
  } else if (OB_FAIL(ctx_.rpc_proxy_->to(server).check_partition_table())) {
    LOG_WARN("request check partition table failed", K(ret), K(server));
  }
  return ret;
}

int ObAdminStatisticPrimaryZoneEntityCount::execute(const obrpc::ObRunJobArg& arg)
{
  int ret = OB_SUCCESS;
  LOG_INFO("execute statistic primary zone entity count request", K(ret));
  const bool enable_ddl = GCONF.enable_ddl;
  const bool enable_sys_table_ddl = GCONF.enable_sys_table_ddl;
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (enable_ddl || enable_sys_table_ddl) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("statistic primary zone entity is not allowed when enable ddl");
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "statistc primary zone entity when enable ddl");
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else if (STATISTIC_PRIMARY_ZONE_ENTITY_COUNT != get_inner_job_value(arg.job_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("job to run not statistic primary zone entity count", K(ret), K(arg));
  } else if (OB_UNLIKELY(nullptr == ctx_.root_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ddl service ptr is null", K(ret));
  } else if (OB_FAIL(ctx_.root_service_->submit_statistic_primary_zone_count())) {
    LOG_WARN("fail to statistic primary zone entity", K(ret));
  }
  return ret;
}

int ObAdminCreateHaGtsUtil::execute(const obrpc::ObRunJobArg& arg)
{
  int ret = OB_SUCCESS;
  LOG_INFO("execute create ha gts util request", K(ret));
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else if (CREATE_HA_GTS_UTIL != get_inner_job_value(arg.job_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("job to run not create ha gts util", K(ret), K(arg));
  } else if (OB_UNLIKELY(nullptr == ctx_.root_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("root service ptr is null", K(ret));
  } else if (OB_FAIL(ctx_.root_service_->submit_create_ha_gts_util())) {
    LOG_WARN("fail to submit create ha gts util", K(ret));
  }
  return ret;
}

int ObAdminCreateInnerSchema::execute(const obrpc::ObRunJobArg& arg)
{
  int ret = OB_SUCCESS;
  LOG_INFO("execute create inner role request", K(ret));
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else if (CREATE_INNER_SCHEMA != get_inner_job_value(arg.job_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("job to run not create inner role", K(ret), K(arg));
  } else if (OB_UNLIKELY(nullptr == ctx_.root_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("root service ptr is null", K(ret));
  } else if (OB_FAIL(ctx_.root_service_->submit_create_inner_schema_task())) {
    LOG_WARN("fail to submit create inner role task", K(ret));
  }
  return ret;
}

int ObAdminSchemaRevise::execute(const obrpc::ObRunJobArg& arg)
{
  int ret = OB_SUCCESS;
  LOG_INFO("execute schema revise request", K(ret));
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else if (SCHEMA_REVISE != get_inner_job_value(arg.job_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("job to run not schema revise", K(ret), K(arg));
  } else if (OB_UNLIKELY(nullptr == ctx_.root_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("root service ptr is null", K(ret));
  } else if (OB_FAIL(ctx_.root_service_->submit_schema_revise_task())) {
    LOG_WARN("fail to submit schema revise task", K(ret));
  }
  return ret;
}

int ObAdminRootInspection::execute(const obrpc::ObRunJobArg& arg)
{
  int ret = OB_SUCCESS;
  LOG_INFO("execute root inspection request", K(arg));
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (ROOT_INSPECTION != get_inner_job_value(arg.job_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("job to run not root inspection", K(arg), K(ret));
  } else if (!ctx_.server_mgr_->is_inited()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("server_mgr_ not inited", K(ret));
  } else if (!ctx_.root_inspection_->is_inited()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("root_inspection not inited", K(ret));
  } else if (!arg.zone_.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("root inspection can't execute by zone", K(arg), K(ret));
  } else if (arg.server_.is_valid() && arg.server_ != ctx_.server_mgr_->get_rs_addr()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("only rs can execute root inspection", K(arg), "rs", ctx_.server_mgr_->get_rs_addr(), K(ret));
  } else if (OB_FAIL(ctx_.root_inspection_->check_all())) {
    LOG_WARN("root_inspection check_all failed", K(ret));
  }

  return ret;
}

int ObAdminSchemaSplitExecutor::execute(const obrpc::ObRunJobArg& arg)
{
  int ret = OB_SUCCESS;
  ObInnerJob job = INVALID_INNER_JOB;
  LOG_INFO("execute schema split request", K(arg));
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else {
    job = get_inner_job_value(arg.job_);
    if (SCHEMA_SPLIT == job) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("not supported", K(ret));
    } else if (SCHEMA_SPLIT_V2 == job) {
      if (OB_ISNULL(ctx_.root_service_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("rootservice is null", K(ret));
      } else if (OB_FAIL(ctx_.root_service_->submit_schema_split_task_v2())) {
        LOG_WARN("submit schema split task failed", K(ret));
      }
    } else if (STOP_SCHEMA_SPLIT == job) {
      if (OB_ISNULL(ctx_.schema_split_executor_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("executor is null", K(ret));
      } else if (OB_FAIL(ctx_.schema_split_executor_->stop())) {
        LOG_WARN("fail to stop schema split task", K(ret));
      } else {
        ctx_.schema_split_executor_->start();
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid job type", K(ret), K(job));
    }
  }
  return ret;
}

int ObAdminUpgradeStorageFormatVersionExecutor::execute(const obrpc::ObRunJobArg& arg)
{
  int ret = OB_SUCCESS;
  ObInnerJob job = INVALID_INNER_JOB;
  LOG_INFO("execute upgrade storage format version request", K(arg));
  if (OB_UNLIKELY(!ctx_.is_inited())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ObAdminUpgradeStorageFormatVersionExecutor has not been inited", K(ret));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(arg));
  } else {
    job = get_inner_job_value(arg.job_);
    if (UPGRADE_STORAGE_FORMAT_VERSION == job) {
      if (OB_ISNULL(ctx_.root_service_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, root service must not be NULL", K(ret));
      } else if (OB_FAIL(ctx_.root_service_->submit_upgrade_storage_format_version_task())) {
        LOG_WARN("fail to submit upgrade storage format version task", K(ret));
      }
    } else if (STOP_UPGRADE_STORAGE_FORMAT_VERSION == job) {
      if (OB_ISNULL(ctx_.upgrade_storage_format_executor_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("executor is null", K(ret));
      } else if (OB_FAIL(ctx_.upgrade_storage_format_executor_->stop())) {
        LOG_WARN("fail to stop upgrade_storage_format task", K(ret));
      } else {
        ctx_.upgrade_storage_format_executor_->start();
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid job type", K(ret), K(job));
    }
  }
  return ret;
}

int ObAdminFlushCache::execute(const obrpc::ObAdminFlushCacheArg& arg)
{
  int ret = OB_SUCCESS;
  int64_t tenant_num = arg.tenant_ids_.count();
  ObSEArray<ObAddr, 8> server_list;
  ObFlushCacheArg fc_arg;
  // if tenant num is 0, flush all tenant, else, flush appointed tenant
  if (tenant_num != 0) {  // flush appointed tenant
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_num; ++i) {
      // get tenant server list;
      if (OB_FAIL(get_tenant_servers(arg.tenant_ids_.at(i), server_list))) {
        LOG_WARN("fail to get tenant servers", "tenant_id", arg.tenant_ids_.at(i));
      } else {
        // call tenant servers;
        fc_arg.is_all_tenant_ = false;
        fc_arg.cache_type_ = arg.cache_type_;
        for (int64_t j = 0; OB_SUCC(ret) && j < server_list.count(); ++j) {
          fc_arg.tenant_id_ = arg.tenant_ids_.at(i);
          LOG_INFO("flush server cache", K(fc_arg), K(server_list.at(j)));
          if (OB_FAIL(call_server(server_list.at(j), fc_arg))) {
            LOG_WARN(
                "fail to call tenant server", "tenant_id", arg.tenant_ids_.at(i), "server addr", server_list.at(j));
          }
        }
      }
      server_list.reset();
    }
  } else {  // flush all tenant
    // get all server list, server_mgr_.get_alive_servers
    if (OB_FAIL(get_all_servers(server_list))) {
      LOG_WARN("fail to get all servers", K(ret));
    } else {
      fc_arg.is_all_tenant_ = true;
      fc_arg.tenant_id_ = common::OB_INVALID_TENANT_ID;
      fc_arg.cache_type_ = arg.cache_type_;
      for (int64_t j = 0; OB_SUCC(ret) && j < server_list.count(); ++j) {
        LOG_INFO("flush server cache", K(fc_arg), K(server_list.at(j)));
        if (OB_FAIL(call_server(server_list.at(j), fc_arg))) {
          LOG_WARN("fail to call tenant server", "server addr", server_list.at(j));
        }
      }
    }
  }
  return ret;
}

int ObAdminLoadBaseline::execute(const obrpc::ObAdminLoadBaselineArg& arg)
{
  int ret = OB_SUCCESS;
  int64_t tenant_num = arg.tenant_ids_.count();
  ObSEArray<ObAddr, 8> server_list;
  ObLoadBaselineArg lb_arg;
  // if tenant num is 0, load all tenant baseline,
  // else, load baseline from appointed tenant
  if (tenant_num != 0) {
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_num; ++i) {
      // get tenant server list;
      if (OB_FAIL(get_tenant_servers(arg.tenant_ids_.at(i), server_list))) {
        LOG_WARN("fail to get tenant servers", "tenant_id", arg.tenant_ids_.at(i));
      } else {
        // call tenant servers;
        lb_arg.is_all_tenant_ = false;
        for (int64_t j = 0; OB_SUCC(ret) && j < server_list.count(); ++j) {
          lb_arg.tenant_id_ = arg.tenant_ids_.at(i);
          lb_arg.sql_id_ = arg.sql_id_;
          lb_arg.fixed_ = arg.fixed_;
          lb_arg.enabled_ = arg.enabled_;
          LOG_INFO("load baseline from plan cache", K(lb_arg), K(server_list.at(j)));
          if (OB_FAIL(call_server(server_list.at(j), lb_arg))) {
            LOG_WARN(
                "fail to call tenant server", "tenant_id", arg.tenant_ids_.at(i), "server addr", server_list.at(j));
          }
        }
      }
      server_list.reset();
    }
  } else {  // load baseline from all tenant
    // get all server list, server_mgr_.get_alive_servers
    if (OB_FAIL(get_all_servers(server_list))) {
      LOG_WARN("fail to get all servers", K(ret));
    } else {
      lb_arg.is_all_tenant_ = true;
      lb_arg.tenant_id_ = common::OB_INVALID_TENANT_ID;
      for (int64_t j = 0; OB_SUCC(ret) && j < server_list.count(); ++j) {
        LOG_INFO("load baseline from plan cache", K(lb_arg), K(server_list.at(j)));
        if (OB_FAIL(call_server(server_list.at(j), lb_arg))) {
          LOG_WARN("fail to call tenant server", "server addr", server_list.at(j));
        }
      }
    }
  }
  return ret;
}

int ObAdminLoadBaseline::call_server(const common::ObAddr& server, const obrpc::ObLoadBaselineArg& arg)
{
  int ret = OB_SUCCESS;
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), K(ret));
  } else if (OB_FAIL(ctx_.rpc_proxy_->to(server).load_baseline(arg))) {
    LOG_WARN("request server load baseline failed", K(ret), K(server));
  }

  return ret;
}

int ObTenantServerAdminUtil::get_tenant_servers(const uint64_t tenant_id, common::ObIArray<ObAddr>& servers)
{
  int ret = OB_SUCCESS;
  // sys tenant, get all servers directly
  if (OB_SYS_TENANT_ID == tenant_id) {
    if (OB_FAIL(get_all_servers(servers))) {
      LOG_WARN("fail to get all servers", K(ret));
    }
  } else {
    ObArray<uint64_t> pool_ids;
    if (OB_ISNULL(ctx_.server_mgr_) || OB_ISNULL(ctx_.unit_mgr_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ctx_.server_mgr_), K(ctx_.unit_mgr_), K(ret));
    } else if (!ctx_.server_mgr_->has_build() || !ctx_.unit_mgr_->check_inner_stat()) {
      ret = OB_SERVER_IS_INIT;
      LOG_WARN("server manager or unit manager hasn't built",
          "server_mgr built",
          ctx_.server_mgr_->has_build(),
          "unit_mgr built",
          ctx_.unit_mgr_->check_inner_stat(),
          K(ret));
    } else if (OB_FAIL(ctx_.unit_mgr_->get_pool_ids_of_tenant(tenant_id, pool_ids))) {
      LOG_WARN("get_pool_ids_of_tenant failed", K(tenant_id), K(ret));
    } else {
      ObArray<ObUnitInfo> unit_infos;
      for (int64_t i = 0; OB_SUCC(ret) && i < pool_ids.count(); ++i) {
        unit_infos.reuse();
        if (OB_FAIL(ctx_.unit_mgr_->get_unit_infos_of_pool(pool_ids.at(i), unit_infos))) {
          LOG_WARN("get_unit_infos_of_pool failed", "pool_id", pool_ids.at(i), K(ret));
        } else {
          for (int64_t j = 0; OB_SUCC(ret) && j < unit_infos.count(); ++j) {
            bool is_alive = false;
            const ObUnit& unit = unit_infos.at(j).unit_;
            if (OB_FAIL(ctx_.server_mgr_->check_server_alive(unit.server_, is_alive))) {
              LOG_WARN("check_server_alive failed", "server", unit.server_, K(ret));
            } else if (is_alive) {
              if (OB_FAIL(servers.push_back(unit.server_))) {
                LOG_WARN("push_back failed", K(ret));
              }
            }
            if (OB_SUCC(ret)) {
              if (unit.migrate_from_server_.is_valid()) {
                if (OB_FAIL(ctx_.server_mgr_->check_server_alive(unit.migrate_from_server_, is_alive))) {
                  LOG_WARN("check_server_alive failed", "server", unit.migrate_from_server_, K(ret));
                } else if (is_alive) {
                  if (OB_FAIL(servers.push_back(unit.migrate_from_server_))) {
                    LOG_WARN("push_back failed", K(ret));
                  }
                }
              }
            }
          }  // for unit infos end
        }
      }  // for pool ids end
    }
  }

  return ret;
}

int ObTenantServerAdminUtil::get_all_servers(common::ObIArray<ObAddr>& servers)
{
  int ret = OB_SUCCESS;
  ObZone empty_zone;
  if (OB_ISNULL(ctx_.server_mgr_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ctx_.server_mgr_), K(ret));
  } else if (OB_FAIL(ctx_.server_mgr_->get_alive_servers(empty_zone, servers))) {
    // if zone is empty, get all servers
    LOG_WARN("fail to get all servers", K(ret));
  }
  return ret;
}

int ObAdminFlushCache::call_server(const common::ObAddr& server, const obrpc::ObFlushCacheArg& arg)
{
  int ret = OB_SUCCESS;
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), K(ret));
  } else if (OB_FAIL(ctx_.rpc_proxy_->to(server).flush_cache(arg))) {
    LOG_WARN("request server flush cache failed", K(ret), K(server));
  }
  return ret;
}

int ObAdminSetTP::execute(const obrpc::ObAdminSetTPArg& arg)
{
  LOG_INFO("execute report request", K(arg));
  int ret = OB_SUCCESS;
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(call_all(arg))) {
    LOG_WARN("execute report replica failed", K(ret), K(arg));
  }
  return ret;
}

int ObAdminSetTP::call_server(const ObAddr& server)
{
  int ret = OB_SUCCESS;
  if (!ctx_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), K(ret));
  } else if (OB_FAIL(ctx_.rpc_proxy_->to(server).set_tracepoint(arg_))) {
    LOG_WARN("request server report replica failed", K(ret), K(server));
  }
  return ret;
}

}  // end namespace rootserver
}  // end namespace oceanbase
