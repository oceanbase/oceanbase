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

#define USING_LOG_PREFIX SHARE_PT

#include "ob_rpc_partition_table.h"

#include "rpc/obrpc/ob_rpc_proxy.h"
#include "common/ob_timeout_ctx.h"
#include "share/config/ob_server_config.h"
#include "share/ob_rs_mgr.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/ob_common_rpc_proxy.h"
#include "observer/ob_server_struct.h"
#include "storage/ob_partition_service.h"
namespace oceanbase {
using namespace common;
using namespace obrpc;

namespace share {
ObRpcPartitionTable::ObRpcPartitionTable(ObIPartPropertyGetter& prop_getter)
    : ObIPartitionTable(prop_getter), is_inited_(false), rpc_proxy_(NULL), rs_mgr_(NULL), config_(NULL)
{}

ObRpcPartitionTable::~ObRpcPartitionTable()
{}

int ObRpcPartitionTable::init(ObCommonRpcProxy& rpc_proxy, ObRsMgr& rs_mgr, ObServerConfig& config)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("rpc partition table is already inited", K(ret));
  } else {
    rpc_proxy_ = &rpc_proxy;
    rs_mgr_ = &rs_mgr;
    config_ = &config;
    is_inited_ = true;
  }
  return ret;
}

int ObRpcPartitionTable::get(const uint64_t table_id, const int64_t partition_id, ObPartitionInfo& partition_info,
    const bool need_fetch_faillist, const int64_t cluster_id)
{
  int ret = OB_SUCCESS;
  UNUSED(need_fetch_faillist);
  ObAddr rs_addr;
  partition_info.set_table_id(table_id);
  partition_info.set_partition_id(partition_id);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("rpc partition table not inited", K(ret));
  } else if (OB_INVALID_ID != cluster_id) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("get location with cluster_id not supported", K(ret), K(cluster_id));
  } else if (combine_id(OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID) != table_id ||
             ALL_CORE_TABLE_PARTITION_ID != partition_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KT(table_id), K(partition_id), K(ret));
  } else if (OB_FAIL(rs_mgr_->get_master_root_server(rs_addr))) {
    LOG_WARN("get master root service failed", K(ret));
  } else {
    int64_t timeout_us = 0;
    if (OB_FAIL(get_timeout(timeout_us))) {
      LOG_WARN("get timeout failed", K(ret));
    } else if (timeout_us <= 0) {
      ret = OB_TIMEOUT;
      LOG_WARN("process timeout", K(timeout_us));
    } else if (OB_FAIL(rpc_proxy_->to_addr(rs_addr)
                           .by(OB_LOC_CORE_TENANT_ID)
                           .timeout(timeout_us)
                           .get_root_partition(partition_info))) {  // Get the cache of RS directly
      LOG_WARN("fetch root partition through rpc failed", K(rs_addr), K(ret));
      if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_2260) {
        if (OB_FAIL(fetch_root_partition_v2(partition_info))) {
          LOG_WARN("fail to fetch root partition", K(ret));
        }
      } else {
        if (OB_FAIL(fetch_root_partition_v1(partition_info))) {
          LOG_WARN("fail to fetch root partition", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObRpcPartitionTable::batch_fetch_partition_infos(const common::ObIArray<common::ObPartitionKey>& keys,
    common::ObIAllocator& allocator, common::ObArray<ObPartitionInfo*>& partitions, const int64_t cluster_id)
{
  int ret = OB_SUCCESS;
  if (1 != keys.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid keys cnt", K(ret), "cnt", keys.count());
  } else if (OB_INVALID_ID != cluster_id) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("get location with cluster_id not supported", K(ret), K(cluster_id));
  } else {
    ObPartitionInfo* partition = NULL;
    const ObPartitionKey& key = keys.at(0);
    if (OB_FAIL(ObPartitionInfo::alloc_new_partition_info(allocator, partition))) {
      LOG_WARN("fail to alloc partition", K(ret), K(key));
    } else if (OB_FAIL(get(key.get_table_id(), key.get_partition_id(), *partition))) {
      LOG_WARN("fail to get partition", K(ret), K(key));
    } else if (OB_FAIL(partitions.push_back(partition))) {
      LOG_WARN("fail to push back partition", K(ret), K(key));
    }
  }
  return ret;
}

int ObRpcPartitionTable::fetch_root_partition_v1(ObPartitionInfo& partition_info)
{
  int ret = OB_SUCCESS;
  const ObPartitionReplica* leader = NULL;
  bool need_retry_all_server = false;
  int tmp_ret = OB_SUCCESS;
  if (OB_SUCCESS != (tmp_ret = fetch_root_partition_from_rs_list_v1(partition_info))) {
    LOG_WARN("fail to fetch root partition from rs_list", K(tmp_ret));
    need_retry_all_server = true;
  } else if (OB_SUCCESS != (tmp_ret = partition_info.find_leader_v2(leader)) || OB_ISNULL(leader)) {
    LOG_WARN("fail to find leader, retry all server", K(tmp_ret), K(partition_info));
    need_retry_all_server = true;
  }
  if (need_retry_all_server) {
    if (OB_FAIL(fetch_root_partition_from_all_server_v1(partition_info))) {
      LOG_WARN("fail to request all server from core table", K(ret));
    }
  }
  return ret;
}

int ObRpcPartitionTable::fetch_root_partition_v2(ObPartitionInfo& partition_info)
{
  int ret = OB_SUCCESS;
  const ObPartitionReplica* leader = NULL;
  bool need_retry = false;
  int tmp_ret = OB_SUCCESS;
  ObArray<ObAddr> accessed_server_list;
  if (OB_SUCCESS != (tmp_ret = fetch_root_partition_from_rs_list_v2(accessed_server_list, partition_info))) {
    LOG_WARN("fail to fetch root partition from rootservice_list", K(tmp_ret));
    need_retry = true;
  } else if (OB_SUCCESS != (tmp_ret = partition_info.find_leader_v2(leader)) || OB_ISNULL(leader)) {
    LOG_WARN("fail to find leader from rootservice_list, retry partition service", K(tmp_ret), K(partition_info));
    need_retry = true;
  } else {
    LOG_INFO("fetch root partition from rootservice_list succes", K(partition_info));
  }
  if (need_retry) {
    need_retry = false;
    if (OB_FAIL(fetch_root_partition_from_ps_v2(accessed_server_list, partition_info))) {
      LOG_WARN("fail to fetch root partition from partition service", KR(ret));
      need_retry = true;
    } else if (OB_SUCCESS != (tmp_ret = partition_info.find_leader_v2(leader)) || OB_ISNULL(leader)) {
      LOG_WARN("fail to find leader from partition service, retry all server", K(tmp_ret), K(partition_info));
      need_retry = true;
    } else {
      LOG_INFO("fetch root partition from partition service success", K(partition_info));
    }
  }
  if (need_retry) {
    if (OB_FAIL(fetch_root_partition_from_all_server_v2(accessed_server_list, partition_info))) {
      LOG_WARN("fail to request all server from all_server_list", K(ret));
    } else if (OB_SUCCESS != (tmp_ret = partition_info.find_leader_v2(leader)) || OB_ISNULL(leader)) {
      LOG_WARN("fail to find leader from all_server_list, retry all server", K(tmp_ret), K(partition_info));
    } else {
      LOG_INFO("fetch root partition from all_server_list success", K(partition_info));
    }
  }
  return ret;
}

int ObRpcPartitionTable::fetch_root_partition_from_rs_list_v2(
    ObIArray<ObAddr>& rs_list, ObPartitionInfo& partition_info)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(rs_mgr_->get_all_rs_list(rs_list))) {
    LOG_WARN("fail to get rs list", K(ret));
  } else if (OB_FAIL(rs_mgr_->do_detect_master_rs_v3(rs_list, partition_info))) {
    LOG_WARN("fail to get root partition info", K(ret), K(rs_list));
  } else {
    LOG_INFO("fetch root partition from rs_list success", K(partition_info), K(rs_list));
  }
  return ret;
}

int ObRpcPartitionTable::fetch_root_partition_from_all_server_v2(
    const ObIArray<ObAddr>& rs_list, ObPartitionInfo& partition)
{
  int ret = OB_SUCCESS;
  ObArray<ObAddr> obs_list;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(GCTX.config_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid config", K(ret));
  } else {
    bool split_end = false;
    ObString sub_string;
    ObString trimed_string;
    ObString all_server_list(strlen(GCTX.config_->all_server_list.str()), GCTX.config_->all_server_list.str());
    char buf[OB_IP_PORT_STR_BUFF];
    ObAddr addr;
    while (!split_end && OB_SUCCESS == ret) {
      sub_string = all_server_list.split_on(',');
      if (sub_string.empty() && NULL == sub_string.ptr()) {
        split_end = true;
        sub_string = all_server_list;
      }
      trimed_string = sub_string.trim();
      if (trimed_string.empty()) {
        // nothing todo
      } else if (0 > snprintf(buf, OB_IP_PORT_STR_BUFF, "%.*s", trimed_string.length(), trimed_string.ptr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to snprintf", K(ret), K(trimed_string));
      } else if (OB_FAIL(addr.parse_from_cstring(buf))) {
        LOG_WARN("fail to parser addr from cstring", K(ret));
      } else if (has_exist_in_array(rs_list, addr)) {
        // nothing todo
      } else if (OB_FAIL(obs_list.push_back(addr))) {
        LOG_WARN("fail to push back", K(ret), K(addr));
      }
    }  // end while
  }    // end else
  if (OB_SUCC(ret)) {
    if (OB_FAIL(rs_mgr_->do_detect_master_rs_v3(obs_list, partition))) {
      LOG_WARN("fail to get root partition from obs", K(ret), K(obs_list));
    } else {
      LOG_INFO("fetch root partition from server list success", K(partition), K(obs_list));
    }
  }
  return ret;
}

int ObRpcPartitionTable::fetch_root_partition_from_rs_list_v1(ObPartitionInfo& partition_info)
{
  int ret = OB_SUCCESS;
  ObArray<ObAddr> rs_list;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(rs_mgr_->get_all_rs_list(rs_list))) {
    LOG_WARN("fail to get rs list", K(ret));
  } else if (OB_FAIL(fetch_root_partition_from_obs_v1(rs_list, partition_info))) {
    LOG_WARN("fail to get root partition info", K(ret), K(rs_list));
  } else {
    LOG_INFO("fetch root partition from rs_list success", K(partition_info), K(rs_list));
  }
  return ret;
}

int ObRpcPartitionTable::fetch_root_partition_from_all_server_v1(ObPartitionInfo& partition)
{
  int ret = OB_SUCCESS;
  ObArray<ObAddr> obs_list;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(GCTX.config_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid config", K(ret));
  } else {
    bool split_end = false;
    ObString sub_string;
    ObString trimed_string;
    ObString all_server_list(strlen(GCTX.config_->all_server_list.str()), GCTX.config_->all_server_list.str());
    char buf[OB_IP_PORT_STR_BUFF];
    ObAddr addr;
    while (!split_end && OB_SUCCESS == ret) {
      sub_string = all_server_list.split_on(',');
      if (sub_string.empty() && NULL == sub_string.ptr()) {
        split_end = true;
        sub_string = all_server_list;
      }
      trimed_string = sub_string.trim();
      if (trimed_string.empty()) {
        // nothing todo
      } else if (0 > snprintf(buf, OB_IP_PORT_STR_BUFF, "%.*s", trimed_string.length(), trimed_string.ptr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to snprintf", K(ret), K(trimed_string));
      } else if (OB_FAIL(addr.parse_from_cstring(buf))) {
        LOG_WARN("fail to parser addr from cstring", K(ret));
      } else if (OB_FAIL(obs_list.push_back(addr))) {
        LOG_WARN("fail to push back", K(ret), K(addr));
      }
    }  // end while
  }    // end else
  if (OB_SUCC(ret)) {
    if (OB_FAIL(fetch_root_partition_from_obs_v1(obs_list, partition))) {
      LOG_WARN("fail to get root partition from obs", K(ret), K(obs_list));
    } else {
      LOG_INFO("fetch root partition from server list success", K(partition), K(obs_list));
    }
  }
  return ret;
}

int ObRpcPartitionTable::fetch_root_partition_from_obs_v1(
    const ObIArray<ObAddr>& obs_list, ObPartitionInfo& partition_info)
{
  int ret = OB_SUCCESS;
  partition_info.reuse();
  int64_t timeout_us = 0;
  ObGetRootserverRoleResult result;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    for (int64_t i = 0; i < obs_list.count() && OB_SUCC(ret); i++) {
      const ObAddr& addr = obs_list.at(i);
      result.reset();
      if (OB_FAIL(get_timeout(timeout_us))) {
        LOG_WARN("get timeout failed", K(ret));
      } else if (timeout_us <= 0) {
        ret = OB_TIMEOUT;
        LOG_WARN("process timeout", K(timeout_us));
      } else if (OB_FAIL(rpc_proxy_->to_addr(addr)
                             .by(OB_LOC_CORE_TENANT_ID)
                             .timeout(timeout_us)
                             .get_root_server_status(result))) {
        LOG_WARN("fail to get root server status", K(ret), K(addr));
        ret = OB_SUCCESS;
      } else if (!result.replica_.is_valid()) {
        // nothing todo
      } else if (OB_FAIL(partition_info.add_replica(result.replica_))) {
        LOG_WARN("fail to get add replica", K(ret), "replica", result.replica_, K(partition_info));
      } else {
        LOG_INFO("fetch root partition from obs", "replica", result.replica_, K(addr));
      }
    }  // end for
  }
  return ret;
}

int ObRpcPartitionTable::prefetch_by_table_id(const uint64_t tenant_id, const uint64_t start_table_id,
    const int64_t start_partition_id, ObIArray<ObPartitionInfo>& partition_infos, const bool need_fetch_faillist)
{
  int ret = OB_SUCCESS;
  UNUSED(need_fetch_faillist);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_SYS_TENANT_ID != tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(tenant_id), K(ret));
  } else if (start_table_id != combine_id(OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID) ||
             start_partition_id != ALL_CORE_TABLE_PARTITION_ID) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, need to be __all_core_table", K(start_table_id), K(start_partition_id));
  } else {
    const uint64_t table_id = combine_id(OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID);
    const int64_t partition_id = ALL_CORE_TABLE_PARTITION_ID;
    ObPartitionInfo partition_info;
    if (OB_FAIL(get(table_id, partition_id, partition_info))) {
      LOG_WARN("get failed", KT(table_id), K(partition_id), K(ret));
    } else {
      partition_info.reset_row_checksum();
      if (OB_FAIL(partition_infos.push_back(partition_info))) {
        LOG_WARN("push_back failed", K(ret));
      }
    }
  }
  return ret;
}

int ObRpcPartitionTable::prefetch(const uint64_t tenant_id, const uint64_t start_table_id,
    const int64_t start_partition_id, ObIArray<ObPartitionInfo>& partition_infos, bool ignore_row_checksum,
    const bool need_fetch_faillist)
{
  int ret = OB_SUCCESS;
  UNUSED(ignore_row_checksum);
  UNUSED(need_fetch_faillist);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_SYS_TENANT_ID != tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(tenant_id), K(ret));
  } else if (OB_INVALID_ID == start_table_id || start_partition_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KT(start_table_id), K(start_partition_id));
  } else if (start_table_id > combine_id(OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID) ||
             (start_table_id == combine_id(OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID) &&
                 start_partition_id >= ALL_CORE_TABLE_PARTITION_ID)) {
    // do nothing
  } else {
    const uint64_t table_id = combine_id(OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID);
    const int64_t partition_id = ALL_CORE_TABLE_PARTITION_ID;
    ObPartitionInfo partition_info;
    if (OB_FAIL(get(table_id, partition_id, partition_info))) {
      LOG_WARN("get failed", KT(table_id), K(partition_id), K(ret));
    } else {
      partition_info.reset_row_checksum();
      if (OB_FAIL(partition_infos.push_back(partition_info))) {
        LOG_WARN("push_back failed", K(ret));
      }
    }
  }
  return ret;
}

int ObRpcPartitionTable::prefetch(const uint64_t pt_table_id, const int64_t pt_partition_id,
    const uint64_t start_table_id, const int64_t start_partition_id, ObIArray<ObPartitionInfo>& partition_infos,
    const bool need_fetch_faillist)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (pt_table_id != combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_CORE_META_TABLE_TID) || pt_partition_id != 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid pt_table_id or pt_partition_id", KT(pt_table_id), K(pt_partition_id), K(ret));
  } else if (OB_INVALID_ID == start_table_id || start_partition_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(start_table_id), K(start_partition_id));
  } else if (OB_FAIL(prefetch(
                 OB_SYS_TENANT_ID, start_table_id, start_partition_id, partition_infos, false, need_fetch_faillist))) {
    LOG_WARN("prefetch failed", "tenant_id", OB_SYS_TENANT_ID, KT(start_table_id), K(start_partition_id), K(ret));
  }
  return ret;
}

int ObRpcPartitionTable::batch_execute(const ObIArray<ObPartitionReplica>& replicas)
{
  int ret = OB_SUCCESS;
  if (replicas.count() <= 0) {
    // nothing to do
  } else {
    FOREACH_CNT_X(replica, replicas, OB_SUCC(ret))
    {
      if (OB_ISNULL(replica)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invali replica", K(ret), K(replica));
      } else {
        if (replica->is_remove_) {
          if (OB_FAIL(remove(replica->table_id_, replica->partition_id_, GCTX.self_addr_))) {
            LOG_WARN("fail to remove replica", K(ret), "table_id", replica->table_id_);
          }
        } else {
          if (OB_FAIL(update(*replica))) {
            LOG_WARN("fail to update replica", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObRpcPartitionTable::batch_report_partition_role(
    const common::ObIArray<share::ObPartitionReplica>& pkey_array, const ObRole new_role)
{
  UNUSED(pkey_array);
  UNUSED(new_role);
  return OB_NOT_SUPPORTED;
}

int ObRpcPartitionTable::batch_report_with_optimization(
    const ObIArray<ObPartitionReplica>& replicas, const bool with_role)
{
  UNUSED(replicas);
  UNUSED(with_role);
  return OB_NOT_SUPPORTED;
}

int ObRpcPartitionTable::update(const ObPartitionReplica& replica)
{
  int ret = OB_SUCCESS;
  ObAddr rs_addr;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("rpc partition table is not inited", K(ret));
  } else if (!replica.is_valid() || combine_id(OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID) != replica.table_id_ ||
             ALL_CORE_TABLE_PARTITION_ID != replica.partition_id_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(replica), K(ret));
  } else if (OB_FAIL(rs_mgr_->get_master_root_server(rs_addr))) {
    LOG_WARN("get master root service failed", K(ret));
  } else {
    int64_t timeout_us = 0;
    if (OB_FAIL(get_timeout(timeout_us))) {
      LOG_WARN("get timeout failed", K(ret));
    } else if (timeout_us <= 0) {
      ret = OB_TIMEOUT;
      LOG_WARN("process timeout", K(ret), K(timeout_us));
    } else if (OB_FAIL(rpc_proxy_->to(rs_addr).timeout(timeout_us).report_root_partition(replica))) {
      LOG_WARN("report root partition through rpc failed", K(replica), K(rs_addr), K(ret));
    }
  }

  LOG_INFO("update root partition replica", K(replica), K(ret));
  return ret;
}

int ObRpcPartitionTable::remove(const uint64_t table_id, const int64_t partition_id, const common::ObAddr& server)
{
  int ret = OB_SUCCESS;
  ObAddr rs_addr;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("rpc partition table is not inited", K(ret));
  } else if (combine_id(OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID) != table_id ||
             ALL_CORE_TABLE_PARTITION_ID != partition_id || !server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KT(table_id), K(partition_id), K(ret));
  } else if (OB_FAIL(rs_mgr_->get_master_root_server(rs_addr))) {
    LOG_WARN("get master root service failed", K(ret));
  } else {
    int64_t timeout_us = 0;
    if (OB_FAIL(get_timeout(timeout_us))) {
      LOG_WARN("get timeout failed", K(ret));
    } else if (timeout_us <= 0) {
      ret = OB_TIMEOUT;
      LOG_WARN("process timeout", K(ret), K(timeout_us));
    } else if (OB_FAIL(rpc_proxy_->to(rs_addr).timeout(timeout_us).remove_root_partition(server))) {
      LOG_WARN("remove root partition through rpc failed", K(rs_addr), K(ret));
    }
  }
  return ret;
}

int ObRpcPartitionTable::update_rebuild_flag(
    const uint64_t table_id, const int64_t partition_id, const common::ObAddr& server, const bool rebuild)
{
  int ret = OB_SUCCESS;
  ObAddr rs_addr;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("rpc partition table is not inited", K(ret));
  } else if (combine_id(OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID) != table_id ||
             ALL_CORE_TABLE_PARTITION_ID != partition_id || !server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KT(table_id), K(partition_id), K(server), K(ret));
  } else if (OB_FAIL(rs_mgr_->get_master_root_server(rs_addr))) {
    LOG_WARN("get master root service failed", K(ret));
  } else {
    int64_t timeout_us = 0;
    if (OB_FAIL(get_timeout(timeout_us))) {
      LOG_WARN("get timeout failed", K(ret));
    } else if (timeout_us <= 0) {
      ret = OB_TIMEOUT;
      LOG_WARN("process timeout", K(ret), K(timeout_us));
    } else {
      if (rebuild) {
        if (OB_FAIL(rpc_proxy_->to(rs_addr).timeout(timeout_us).rebuild_root_partition(server))) {
          LOG_WARN("rebuild root partition through rpc failed", K(rs_addr), K(server), K(ret));
        }
      } else {
        if (OB_FAIL(rpc_proxy_->to(rs_addr).timeout(timeout_us).clear_rebuild_root_partition(server))) {
          LOG_WARN("clear rebuild root partition through rpc failed", K(rs_addr), K(server), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObRpcPartitionTable::update_fail_list(const uint64_t table_id, const int64_t partition_id,
    const common::ObAddr& server, const ObPartitionReplica::FailList& fail_list)
{
  int ret = OB_SUCCESS;
  UNUSED(table_id);
  UNUSED(partition_id);
  UNUSED(server);
  UNUSED(fail_list);
  ret = OB_NOT_SUPPORTED;
  LOG_WARN("rpc partition table set_fail_list is not supported", K(ret));
  return ret;
}

int ObRpcPartitionTable::handover_partition(
    const common::ObPGKey& pg_key, const common::ObAddr& src_addr, const common::ObAddr& dest_addr)
{
  SHARE_PT_LOG(INFO,
      "handover partition will do nothing, maybe fast migrate __all_core_table",
      K(pg_key),
      K(src_addr),
      K(dest_addr));
  return OB_SUCCESS;  // do nothing
}

int ObRpcPartitionTable::replace_partition(
    const ObPartitionReplica& replica, const common::ObAddr& src_addr, const common::ObAddr& dest_addr)
{
  SHARE_PT_LOG(INFO,
      "replace partition will do nothing, maybe fast recover __all_core_table",
      K(replica),
      K(src_addr),
      K(dest_addr));
  return OB_SUCCESS;  // do nothing
}

int ObRpcPartitionTable::set_unit_id(
    const uint64_t table_id, const int64_t partition_id, const ObAddr& server, const uint64_t unit_id)
{
  int ret = OB_SUCCESS;
  UNUSED(table_id);
  UNUSED(partition_id);
  UNUSED(server);
  UNUSED(unit_id);
  ret = OB_NOT_SUPPORTED;
  LOG_WARN("rpc partition table set_unit_id is not supported", K(ret));
  return ret;
}

int ObRpcPartitionTable::set_original_leader(
    const uint64_t table_id, const int64_t partition_id, const bool is_original_leader)
{
  int ret = OB_SUCCESS;
  UNUSED(table_id);
  UNUSED(partition_id);
  UNUSED(is_original_leader);
  ret = OB_NOT_SUPPORTED;
  LOG_WARN("rpc partition table set_original_leader is not supported", K(ret));
  return ret;
}

int ObRpcPartitionTable::get_timeout(int64_t& timeout)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    timeout = config_->rpc_timeout;
    if (ObTimeoutCtx::get_ctx().is_timeout_set()) {
      timeout = ObTimeoutCtx::get_ctx().get_timeout();
    }
  }
  return ret;
}

int ObRpcPartitionTable::fetch_root_partition_from_ps_v2(ObIArray<ObAddr>& server_list, ObPartitionInfo& partition_info)
{
  int ret = OB_SUCCESS;
  ObPartitionKey part_key(combine_id(OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID), 0, 1);

  ObAddr leader;
  ObRole role;
  ObMemberList member_list;
  ObChildReplicaList children_list;
  ObReplicaType replica_type = common::ObReplicaType::REPLICA_TYPE_MAX;
  ObReplicaProperty property;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(GCTX.par_ser_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected", KR(ret));
  } else if (OB_FAIL(GCTX.par_ser_->get_curr_leader_and_memberlist(
                 part_key, leader, role, member_list, children_list, replica_type, property))) {
    LOG_WARN("fail to get curr leader and member list", KR(ret));
  }

  bool need_retry = true;
  // there are leaders in partition service but not exist in the server list
  if (OB_SUCC(ret) && leader.is_valid() && !has_exist_in_array(server_list, leader)) {
    int64_t timeout_us = 0;
    if (OB_FAIL(get_timeout(timeout_us))) {
      LOG_WARN("fail to get timeout", KR(ret));
    } else if (timeout_us <= 0) {
      ret = OB_TIMEOUT;
      LOG_WARN("process timeout", K(timeout_us));
    } else if (OB_FAIL(rpc_proxy_->to_addr(leader)
                           .by(OB_LOC_CORE_TENANT_ID)
                           .timeout(timeout_us)
                           .get_root_partition(partition_info))) {
      LOG_WARN("fail to get root partition", KR(ret), K(leader));
      int tmp_ret = server_list.push_back(leader);
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("fail to push back", KR(ret), K(leader));
      }
    } else {
      need_retry = false;
    }
  }

  ObArray<ObAddr> retry_server_list;
  if (OB_FAIL(ret) || !need_retry) {
    // nothing todo
  } else {
    ObAddr server;
    for (int64_t i = 0; i < member_list.get_member_number() && OB_SUCC(ret); i++) {
      server.reset();
      if (OB_FAIL(member_list.get_server_by_index(i, server))) {
        LOG_WARN("fail to get server", KR(ret), K(i));
      } else if (has_exist_in_array(server_list, server)) {
        // nothing todo
      } else if (OB_FAIL(retry_server_list.push_back(server))) {
        LOG_WARN("fail to push back", KR(ret), K(server));
      } else if (OB_FAIL(server_list.push_back(server))) {
        LOG_WARN("fail to push back", KR(ret), K(server));
      }
      // ignore ret;
      ret = OB_SUCCESS;
    }  // end for

    // push back to children server list
    for (int64_t i = 0; i < children_list.count() && OB_SUCC(ret); i++) {
      const ObReplicaMember& member = children_list.at(i);
      if (has_exist_in_array(server_list, member.get_server())) {
        // nothing todo
      } else if (OB_FAIL(retry_server_list.push_back(server))) {
        LOG_WARN("fail to push back", KR(ret), K(server));
      } else if (OB_FAIL(server_list.push_back(server))) {
        LOG_WARN("fail to push back", KR(ret), K(server));
      }
    }
  }

  // try to acquire rs list infos
  if (OB_FAIL(ret) || 0 >= retry_server_list.count()) {
    // nothing todo
  } else if (OB_FAIL(rs_mgr_->do_detect_master_rs_v3(retry_server_list, partition_info))) {
    LOG_WARN("fail to detect master rs", KR(ret), K(retry_server_list));
  }
  return ret;
}
}  // end namespace share
}  // end namespace oceanbase
