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

#define USING_LOG_PREFIX SHARE

#include "share/ob_rs_mgr.h"

#include "common/ob_role_mgr.h"
#include "share/ob_common_rpc_proxy.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "share/config/ob_server_config.h"
#include "share/ob_rpc_struct.h"
#include "share/partition_table/ob_partition_table_operator.h"
#include "observer/ob_server_struct.h"
#include "lib/profile/ob_trace_id.h"

namespace oceanbase {
using namespace common;
using namespace obrpc;

namespace share {
ObUnifiedAddrAgent::ObUnifiedAddrAgent(void) : is_inited_(false)
{
  for (int64_t i = 0; i < AGENT_NUM; ++i) {
    agents_[i] = NULL;
  }
}

bool ObUnifiedAddrAgent::is_valid()
{
  return (NULL != agents_[0]);
}

int ObUnifiedAddrAgent::init(ObMySQLProxy& sql_proxy, ObServerConfig& config)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(ObRootAddrAgent::init(config))) {
    LOG_WARN("init root addr agent failed", K(ret));
  } else if (OB_FAIL(web_service_root_addr_.init(config))) {
    LOG_WARN("init web service root address failed", K(ret));
  } else if (OB_FAIL(inner_config_root_addr_.init(sql_proxy, config))) {
    LOG_WARN("init inner config root address failed", K(ret));
  } else {
    config_ = &config;
    is_inited_ = true;
    if (OB_FAIL(reload())) {
      LOG_WARN("reload failed", K(ret));
      is_inited_ = false;
    }
  }

  return ret;
}

int ObUnifiedAddrAgent::reload()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    STATIC_ASSERT(ARRAYSIZEOF(agents_) >= 2, "too small agent array");
    if (NULL != config_->obconfig_url.str() && strlen(config_->obconfig_url.str()) > 0) {
      agents_[0] = &inner_config_root_addr_;
      agents_[1] = &web_service_root_addr_;
    } else {
      agents_[0] = &inner_config_root_addr_;
      agents_[1] = NULL;
    }
  }
  return ret;
}

int ObUnifiedAddrAgent::delete_cluster(const int64_t cluster_id)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    int tmp_ret = OB_SUCCESS;
    for (int64_t i = 0; i < ARRAYSIZEOF(agents_); ++i) {
      if (NULL != agents_[i]) {
        if (OB_SUCCESS != (tmp_ret = agents_[i]->delete_cluster(cluster_id))) {
          LOG_WARN("store rs list failed", "agent", i, K(tmp_ret), K(cluster_id));
          // continue storing for others agents, while error happen.
          ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
        } else {
          LOG_INFO("delete cluster succeed", "agent", i, K(cluster_id));
        }
      }
    }
  }
  return ret;
}

int ObUnifiedAddrAgent::store(const ObIAddrList& addr_list, const ObIAddrList& readonly_addr_list, const bool force,
    const common::ObClusterType cluster_type, const int64_t timestamp)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (addr_list.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "addr count", addr_list.count());
  } else {
    int tmp_ret = OB_SUCCESS;
    for (int64_t i = 0; i < ARRAYSIZEOF(agents_); ++i) {
      if (NULL != agents_[i]) {
        if (OB_SUCCESS !=
            (tmp_ret = agents_[i]->store(addr_list, readonly_addr_list, force, cluster_type, timestamp))) {
          LOG_WARN("store rs list failed", "agent", i, K(tmp_ret), K(addr_list), K(force));
          // continue storing for others agents, while error happen.
          if (&web_service_root_addr_ != agents_[i]) {
            // ignore the error code of configserver
            ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
          }
          // ret = OB_SUCCESS == ret ? ret : tmp_ret;
        } else {
          LOG_INFO("store rs list succeed", "agent", i, K(addr_list), K(force));
        }
      }
    }
  }

  return ret;
}

int ObUnifiedAddrAgent::fetch(ObIAddrList& addr_list, ObIAddrList& readonly_addr_list, ObClusterType& cluster_type)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    // fetch rs list from agents, return success if one of the agents fetch success
    for (int64_t i = 0; i < ARRAYSIZEOF(agents_); ++i) {
      if (NULL != agents_[i]) {
        addr_list.reset();
        if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_2260 && &web_service_root_addr_ == agents_[i]) {
          // OB is no need to get RS_LIST from configserver.
          ret = OB_NOT_SUPPORTED;
          LOG_DEBUG("can not get owner cluster rs list from all cluster", K(ret), K(i));
        } else if (OB_FAIL(agents_[i]->fetch(addr_list, readonly_addr_list, cluster_type))) {
          LOG_WARN("fetch rs list failed", "agent", i, K(ret));
        } else if (0 < addr_list.count()) {
          break;
        }
      }
    }
  }
  return ret;
}

int ObUnifiedAddrAgent::fetch_remote_rslist(const int64_t cluster_id, ObIAddrList& addr_list,
    ObIAddrList& readonly_addr_list, common::ObClusterType& cluster_type)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    for (int64_t i = 0; i < ARRAYSIZEOF(agents_); ++i) {
      if (NULL != agents_[i]) {
        addr_list.reset();
        readonly_addr_list.reset();
        if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_2260 && &web_service_root_addr_ == agents_[i] &&
            config_->cluster_id == cluster_id) {
          // OB is no need to get RS_LIST from configserver.
          ret = OB_NOT_SUPPORTED;
          LOG_DEBUG("can not get owner cluster rs list from all cluster", K(ret), K(i), K(cluster_id));
        } else if (OB_FAIL(agents_[i]->fetch_remote_rslist(cluster_id, addr_list, readonly_addr_list, cluster_type))) {
          LOG_WARN("fetch rs list failed", "agent", i, K(ret));
        } else if (0 < addr_list.count()) {
          break;
        }
      }
    }
  }
  return ret;
}
int ObUnifiedAddrAgent::fetch_rslist_by_agent_idx(const int64_t index, const int64_t cluster_id, ObIAddrList& addr_list,
    ObIAddrList& readonly_addr_list, common::ObClusterType& cluster_type)
{
  int ret = OB_SUCCESS;
  addr_list.reset();
  readonly_addr_list.reset();
  cluster_type = common::INVALID_CLUSTER_TYPE;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (index >= ARRAYSIZEOF(agents_) || OB_INVALID_CLUSTER_ID == cluster_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("index out of range or cluster id is invalid", K(ret), K(index), K(cluster_id));
  } else if (OB_NOT_NULL(agents_[index])) {
    if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_2260 && &web_service_root_addr_ == agents_[index] &&
        config_->cluster_id == cluster_id) {
      // OB is no need to get RS_LIST from configserver.
      ret = OB_NOT_SUPPORTED;
      LOG_DEBUG("can not get owner cluster rs list from all cluster", K(ret), K(index), K(cluster_id));
    } else if (OB_FAIL(agents_[index]->fetch_remote_rslist(cluster_id, addr_list, readonly_addr_list, cluster_type))) {
      if (OB_NOT_SUPPORTED == ret) {
      } else {
        LOG_WARN("failed to get remote rslist", K(ret), K(cluster_id));
      }
    }
  }
  return ret;
}

ObRsMgr::ObRsMgr() : inited_(false), rpc_proxy_(NULL), config_(NULL), addr_agent_(), root_addr_agent_(addr_agent_)
{}

ObRsMgr::~ObRsMgr()
{}

int ObRsMgr::init(obrpc::ObCommonRpcProxy* rpc_proxy, ObServerConfig* config, ObMySQLProxy* sql_proxy)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (NULL == rpc_proxy || NULL == config || NULL == sql_proxy) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(rpc_proxy), KP(config), KP(sql_proxy));
  } else {
    rpc_proxy_ = rpc_proxy;
    config_ = config;
    if (OB_FAIL(addr_agent_.init(*sql_proxy, *config))) {
      LOG_WARN("init addr agent failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    inited_ = true;
    RsList rs_list;
    int tmp_ret = get_all_rs_list(rs_list);
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("update rs list failed", KR(tmp_ret));
    } else {
      if (!rs_list.empty()) {
        ObLockGuard<ObSpinLock> lock_guard(lock_);
        master_rs_ = rs_list.at(0);
      }
    }
    LOG_INFO("ObRsMgr init successfully! master rootserver", K_(master_rs));
  }
  return ret;
}

int ObRsMgr::get_master_root_server(ObAddr& addr) const
{
  int ret = OB_SUCCESS;
  ObLockGuard<ObSpinLock> lock_guard(lock_);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    addr = master_rs_;
  }
  return ret;
}

// it is no need to set the leader first, because there is no role status in rootservice_list
// if get rs_list from rootservice_list, it is always to access old RS first, and it does not bring much optimization
// when RS is refreshed, it will be stored naturally.
int ObRsMgr::get_all_rs_list(common::ObIArray<common::ObAddr>& list)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRootAddr, MAX_ZONE_NUM> tmp_new_list;
  ObSEArray<ObRootAddr, MAX_ZONE_NUM> tmp_new_readonly_list;
  ObClusterType cluster_type;

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  }
  for (int64_t i = 0; i < addr_agent_.get_agent_num(); ++i) {
    tmp_new_list.reset();
    tmp_new_readonly_list.reset();
    if (OB_FAIL(addr_agent_.fetch_rslist_by_agent_idx(
            i, GCONF.cluster_id, tmp_new_list, tmp_new_readonly_list, cluster_type))) {
      if (OB_NOT_SUPPORTED == ret) {
      } else {
        LOG_WARN("failed to get rslist by agent idx", K(ret), K(i));
      }
    } else if (0 >= tmp_new_list.count()) {
      LOG_INFO("get emtpty rs list");
      // nothing todo
    } else {
      for (int64_t i = 0; i < tmp_new_list.count() && OB_SUCC(ret); i++) {
        if (has_exist_in_array(list, tmp_new_list.at(i).server_)) {
          // nothing
        } else if (OB_FAIL(list.push_back(tmp_new_list.at(i).server_))) {
          LOG_WARN("failed to push back server", KR(ret), K(i), K(tmp_new_list));
        }
      }
    }
    if (OB_FAIL(ret)) {
      ret = OB_SUCCESS;  // ignore fail
    }
  }  // end for

  if OB_FAIL (ret) {
  } else if (OB_UNLIKELY(0 >= list.count())) {
    ret = OB_EMPTY_RESULT;
    LOG_WARN("get empty rs list", KR(ret));
  }
  return ret;
}

int ObRsMgr::renew_master_rootserver()
{
  int ret = OB_SUCCESS;
  if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_2260) {
    ret = renew_master_rootserver_v3();
  } else {
    ret = renew_master_rootserver_v2();
  }
  return ret;
}

int ObRsMgr::renew_master_rootserver_v2()
{
  int ret = OB_SUCCESS;
  RsList rs_list;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(get_all_rs_list(rs_list))) {
    LOG_WARN("fail to get all rs list", KR(ret));
  } else if (OB_FAIL(do_detect_master_rs_v2(rs_list))) {
    LOG_WARN("fail to do detect master rs", KR(ret), K(rs_list));
  }
  return ret;
}

int ObRsMgr::do_detect_master_rs_v2(common::ObIArray<common::ObAddr>& rs_list)
{
  int ret = OB_SUCCESS;

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (rs_list.empty()) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("empty rootservice list", K(ret));
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("[begin detect_master_rs]", "rs_list", rs_list);
    // continue detect next address here, so do not check ret in loop.
    ObGetRootserverRoleResult result;
    const int64_t cluster_id = GCONF.cluster_id;
    bool has_rs = false;
    FOREACH_CNT(server, rs_list)
    {
      result.reset();
      if (OB_FAIL(do_detect_master_rs_v3(*server, cluster_id, result))) {
        // LOG_WARN("detect master rootservice failed", K(ret), "server", *server);
      } else {
        has_rs = true;
        ObLockGuard<ObSpinLock> lock_guard(lock_);
        master_rs_ = result.replica_.server_;
        LOG_INFO("new master rootserver found", "rootservice", master_rs_);
        break;
      }
    }
    if (has_rs) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObRsMgr::renew_master_rootserver_v3()
{
  int ret = OB_SUCCESS;
  ObPartitionInfo partition_info;
  if (OB_ISNULL(ObCurTraceId::get_trace_id())) {
    // Prevent the current trace_id from being overwritten
    ObCurTraceId::init(GCONF.self_addr_);
  }
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(GCTX.pt_operator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid partition table operator", KR(ret));
  } else if (OB_FAIL(GCTX.pt_operator_->get(combine_id(OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID),
                 ObIPartitionTable::ALL_CORE_TABLE_PARTITION_ID,
                 partition_info))) {
    LOG_WARN("fail to get", KR(ret));
  } else if (partition_info.replica_count() <= 0) {
    // nothing todo
  } else {
    for (int64_t i = 0; i < partition_info.replica_count() && OB_SUCC(ret); i++) {
      const ObPartitionReplica& replica = partition_info.get_replicas_v2().at(i);
      if (replica.is_strong_leader()) {
        ObLockGuard<ObSpinLock> lock_guard(lock_);
        master_rs_ = replica.server_;
        LOG_INFO("new master rootserver found", "rootservice", master_rs_);
        break;
      }
    }
  }
  return ret;
}

int ObRsMgr::do_detect_master_rs_v3(const ObIArray<ObAddr>& server_list, ObPartitionInfo& partition_info)
{
  int ret = OB_SUCCESS;

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (server_list.empty()) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("empty rootservice list", K(ret));
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("[begin detect_master_rs]", K(server_list));
    RsList real_rs_list;
    // continue detect next address here, so do not check ret in loop.
    ObAddr rootserver;
    const int64_t cluster_id = GCONF.cluster_id;
    bool has_rs = false;
    int tmp_ret = OB_SUCCESS;
    ObGetRootserverRoleResult result;
    FOREACH_CNT(server, server_list)
    {
      result.reset();
      if (ObTimeoutCtx::get_ctx().is_timeout_set() && ObTimeoutCtx::get_ctx().is_timeouted()) {
        ret = OB_TIMEOUT;
        LOG_WARN("detect master rs timeout", KR(ret));
        break;
      } else if (OB_FAIL(do_detect_master_rs_v3(*server, cluster_id, result))) {
        // LOG_WARN("detect master rootservice failed", K(ret), "server", *server);
      } else {
        // if RS exists, return the memroy data of RS directly.
        if (OB_FAIL(partition_info.assign(result.partition_info_))) {
          LOG_WARN("fail to assign", KR(ret), K(result));
        }
        break;
      }
      // if RS not exists, retrun the replica information of __all_core_table.
      if (OB_ENTRY_NOT_EXIST == ret) {
        // nothing todo
      } else if (OB_RS_NOT_MASTER == ret) {
        if (OB_SUCCESS != (tmp_ret = partition_info.get_replicas_v2().push_back(result.replica_))) {
          LOG_WARN("fail to push back", KR(ret));
        }
      }
    }
  }
  return ret;
}

int ObRsMgr::do_detect_master_rs_v3(
    const ObAddr& dst_server, const int64_t cluster_id, ObGetRootserverRoleResult& result)
{
  int ret = OB_SUCCESS;
  result.reset();
  result.role_ = ObRoleMgr::OB_SLAVE;
  result.zone_.reset();
  ObCurTraceId::Guard guard(GCTX.self_addr_);
  int64_t timeout = DETECT_MASTER_TIMEOUT;
  if (ObTimeoutCtx::get_ctx().is_timeout_set()) {
    timeout = ObTimeoutCtx::get_ctx().get_timeout();
  }
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!dst_server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(dst_server));
  } else {
    ObCurTraceId::Guard guard(GCTX.self_addr_);
    if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_2220) {
      if (OB_FAIL(rpc_proxy_->to_addr(dst_server).timeout(timeout).get_root_server_status(result))) {
        LOG_WARN("failed to get rootserver role", KR(ret), K(dst_server));
      }
    } else {
      if (OB_FAIL(rpc_proxy_->to_addr(dst_server)
                      .timeout(timeout)
                      .dst_cluster_id(cluster_id)
                      .get_master_root_server(result))) {
        LOG_WARN("fail to get rootserver role", KR(ret), K(dst_server), K(cluster_id));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (ObRoleMgr::OB_MASTER != result.role_) {
    ret = OB_RS_NOT_MASTER;
    LOG_WARN("rootserver role is not master",
        K(ret),
        "rootserver",
        result.replica_.server_,
        K(dst_server),
        "zone",
        result.zone_,
        "role",
        result.role_,
        K(result));
    if (status::INVALID == result.status_) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("core table not exist in server", KR(ret), K(result));
    }
  } else {
    LOG_INFO("get rootserver success", K(result));
  }
  return ret;
}

// TODO add a interface to standby cluster for automatically find primary cluster
int ObRsMgr::get_primary_cluster_master_rs(common::ObAddr& addr)
{
  int ret = OB_NOT_SUPPORTED;
  UNUSED(addr);
  return ret;
}

int ObRsMgr::get_remote_cluster_master_rs(const int64_t cluster_id, common::ObAddr& addr)
{
  int ret = OB_SUCCESS;
  LOG_INFO("start get remote cluster ", K(cluster_id));
  ObSEArray<ObRootAddr, MAX_ZONE_NUM> new_list;
  ObSEArray<ObRootAddr, MAX_ZONE_NUM> new_readonly_list;
  ObClusterType cluster_type;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    bool found = false;
    ObGetRootserverRoleResult result;
    for (int64_t i = 0; i < addr_agent_.get_agent_num(); ++i) {
      if (OB_FAIL(addr_agent_.fetch_rslist_by_agent_idx(i, cluster_id, new_list, new_readonly_list, cluster_type))) {
        LOG_WARN("fetch rs list failed", K(ret), K(cluster_id), K(i));
      } else {
        found = false;
        for (int64_t i = 0; i < new_list.count(); ++i) {
          const ObAddr& dst_server = new_list.at(i).server_;
          result.reset();
          if (OB_FAIL(do_detect_master_rs_v3(dst_server, cluster_id, result))) {
          } else {
            addr = result.replica_.server_;
            LOG_INFO("new master rootserver found", "rootservice", addr, K(cluster_id));
            found = true;
            break;
          }
        }
        if (OB_SUCC(ret) && found) {
          break;
        }
      }
    }
  }
  return ret;
}

int ObRsMgr::force_set_master_rs(const ObAddr master_rs)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!master_rs.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(master_rs));
  } else {
    ObLockGuard<ObSpinLock> lock_guard(lock_);
    master_rs_ = master_rs;
    LOG_INFO("force set rs list", K(master_rs));
  }
  return ret;
}
int ObRsMgr::fetch_rs_list(ObIAddrList& addr_list, ObIAddrList& readonly_addr_list)
{
  int ret = OB_SUCCESS;
  ObClusterType cluster_type;  // no used
  common::ObAddr master_rs;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(addr_agent_.fetch(addr_list, readonly_addr_list, cluster_type))) {
    LOG_WARN("failed to fetch addr list", KR(ret));
  } else if (OB_UNLIKELY(0 == addr_list.count())) {
    // directly return success
    LOG_WARN("failed to get rs list", KR(ret), K(addr_list));
  } else if (OB_FAIL(get_master_root_server(master_rs))) {
    LOG_WARN("failed to get master root server", KR(ret), K(master_rs));
  } else {
    // it is no role status in rootservice_list. accordion to master_rs, set role status.
    // if master_rs is not found in rootservice_list, set the first to leader
    bool found = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < addr_list.count() && !found; ++i) {
      if (master_rs == addr_list.at(i).server_) {
        addr_list.at(i).role_ = common::ObRole::LEADER;
        found = true;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (!found) {
      addr_list.at(0).role_ = common::ObRole::LEADER;
    }
  }
  return ret;
}

}  // namespace share
}  // namespace oceanbase
