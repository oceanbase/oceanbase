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

#include "lib/profile/ob_trace_id.h"
#include "common/ob_role_mgr.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "share/ob_rpc_struct.h"
#include "share/ob_srv_rpc_proxy.h"
#include "share/ob_share_util.h"
#include "share/config/ob_server_config.h"
#include "observer/ob_server_struct.h"
#include "share/ls/ob_ls_table_operator.h"
#include "storage/tx_storage/ob_ls_handle.h" //ObLSHandle
#include "storage/tx_storage/ob_ls_service.h"   // ObLSService

namespace oceanbase
{
using namespace common;
using namespace obrpc;

namespace share
{
ObUnifiedAddrAgent::ObUnifiedAddrAgent(void)
  : is_inited_(false)
{
  for (int64_t i = 0; i < MAX_AGENT_NUM; ++i) {
    agents_[i] = NULL;
  }
}

bool ObUnifiedAddrAgent::is_valid()
{
  return (NULL != agents_[0]);
}

int ObUnifiedAddrAgent::check_inner_stat() const
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(config_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is null", KR(ret));
  }
  return ret;
}

int ObUnifiedAddrAgent::init(ObMySQLProxy &sql_proxy, ObServerConfig &config)
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
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else {
    STATIC_ASSERT(ARRAYSIZEOF(agents_) >= MAX_AGENT_NUM, "too small agent array");
    if (NULL != config_->obconfig_url.str() && strlen(config_->obconfig_url.str()) > 0) {
      agents_[INNER_CONFIG_AGENT] = &inner_config_root_addr_;
      agents_[WEB_SERVICE_AGENT] = &web_service_root_addr_;
    } else {
      agents_[INNER_CONFIG_AGENT] = &inner_config_root_addr_;
      agents_[WEB_SERVICE_AGENT] = NULL;
    }
  }
  return ret;
}

int ObUnifiedAddrAgent::store(const ObIAddrList &addr_list, const ObIAddrList &readonly_addr_list,
                              const bool force, const common::ObClusterRole cluster_role,
                              const int64_t timestamp)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (addr_list.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "addr count", addr_list.count());
  } else {
    int tmp_ret = OB_SUCCESS;
    for (int64_t i = 0; i < ARRAYSIZEOF(agents_); ++i) {
      if (NULL != agents_[i]) {
        if (OB_SUCCESS != (tmp_ret = agents_[i]->store(addr_list, readonly_addr_list, force,
                                                      cluster_role, timestamp))) {
          LOG_WARN("store rs list failed", "agent", i, K(tmp_ret), K(addr_list), K(force));
          // continue storing for others agents, while error happen.
          if (&web_service_root_addr_ != agents_[i]) {
            //ignore the error code of configserver
            ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
          }
          //ret = OB_SUCCESS == ret ? ret : tmp_ret;
        } else {
          LOG_INFO("store rs list succeed", "agent", i, K(addr_list), K(force));
        }
      }
    }
  }

  return ret;
}

int ObUnifiedAddrAgent::fetch(
    ObIAddrList &addr_list,
    ObIAddrList &readonly_addr_list)
{
  int ret = OB_SUCCESS;
  const int64_t agent_idx = INNER_CONFIG_AGENT;
  addr_list.reset();
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_ISNULL(agents_[agent_idx])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("agent is null", KR(ret), K(agent_idx));
  } else if (OB_FAIL(agents_[agent_idx]->fetch(addr_list, readonly_addr_list))) {
    LOG_WARN("fetch rs list failed", KR(ret), K(agent_idx));
  }
  return ret;
}

////////////////////
int ObRsMgr::ObRemoteClusterIdGetter::operator() (
    common::hash::HashMapPair<int64_t, common::ObAddr> &entry)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(cluster_id_list_.push_back(entry.first))) {
    LOG_WARN("fail to push back cluster id", KR(ret), "cluster_id", entry.first);
  }
  return ret;
}


/////////////////////

ObRsMgr::ObRsMgr()
  : inited_(false),
    srv_rpc_proxy_(NULL),
    config_(NULL),
    addr_agent_(),
    lock_(common::ObLatchIds::MASTER_RS_CACHE_LOCK),
    master_rs_(),
    remote_master_rs_map_()
{
}

ObRsMgr::~ObRsMgr()
{
}

int ObRsMgr::check_inner_stat() const
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_ISNULL(srv_rpc_proxy_) || OB_ISNULL(config_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is null", KR(ret), KP_(srv_rpc_proxy), KP_(config));
  }
  return ret;
}

int ObRsMgr::init(
    obrpc::ObSrvRpcProxy *srv_rpc_proxy,
    ObServerConfig *config,
    ObMySQLProxy *sql_proxy)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (NULL == srv_rpc_proxy || NULL == config || NULL == sql_proxy) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(srv_rpc_proxy), KP(config), KP(sql_proxy));
  } else if (OB_FAIL(addr_agent_.init(*sql_proxy, *config))) {
    LOG_WARN("init addr agent failed", KR(ret));
  } else if (OB_FAIL(remote_master_rs_map_.create(
          MAX_CLUSTER_IDX_VALUE,
          "RemMasterMap", "RemMasterMap"))) {
    LOG_WARN("fail to create remote master rs map", KR(ret));
  } else {
    srv_rpc_proxy_ = srv_rpc_proxy;
    config_ = config;
    inited_ = true;

    // try init master_rs_
    ObSEArray<ObAddr, OB_MAX_MEMBER_NUMBER> rs_list;
    int tmp_ret = get_all_rs_list_from_configure_(rs_list);
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("update rs list failed", KR(tmp_ret));
    } else if (!rs_list.empty()) {
      ObLockGuard<ObSpinLock> lock_guard(lock_);
      master_rs_ = rs_list.at(0);
    }
    LOG_INFO("ObRsMgr init successfully! master rootserver", K_(master_rs));
  }
  return ret;
}

int ObRsMgr::get_master_root_server(const int64_t cluster_id, ObAddr &addr) const
{
  int ret = OB_SUCCESS;
  const int64_t local_cluster_id = GCONF.cluster_id;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check inner stat faild", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_CLUSTER_ID == cluster_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("cluster id is invalid", KR(ret), K(cluster_id));
  } else if (cluster_id == local_cluster_id) {
    ObLockGuard<ObSpinLock> lock_guard(lock_);
    addr = master_rs_;
  } else if (OB_FAIL(remote_master_rs_map_.get_refactored(cluster_id, addr))) {
    ret = (OB_HASH_NOT_EXIST == ret) ? OB_ENTRY_NOT_EXIST : ret;
    LOG_WARN("remote master root server does't exist", KR(ret), K(cluster_id));
  }
  return ret;
}

int ObRsMgr::get_master_root_server(ObAddr &addr) const
{
  int ret = OB_SUCCESS;
  const int64_t local_cluster_id = GCONF.cluster_id;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check inner stat faild", KR(ret));
  } else if (OB_FAIL(get_master_root_server(local_cluster_id, addr))) {
    LOG_WARN("failed to get root server", KR(ret), K(local_cluster_id));
  }
  return ret;
}


int ObRsMgr::force_set_master_rs(const ObAddr &master_rs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check inner stat faild", KR(ret));
  } else if (OB_UNLIKELY(!master_rs.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(master_rs));
  } else {
    ObLockGuard<ObSpinLock> lock_guard(lock_);
    master_rs_ = master_rs;
    LOG_INFO("[RS_MGR] force set rs list", K(master_rs));
  }
  return ret;
}

//it is no need to set the leader first, because there is no role status in rootservice_list
//if get rs_list from rootservice_list, it is always to access old RS first, and it does not bring much optimization
//when RS is refreshed, it will be stored naturally.
int ObRsMgr::get_all_rs_list_from_configure_(common::ObIArray<common::ObAddr> &server_list)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRootAddr, OB_MAX_MEMBER_NUMBER> rs_list;
  ObSEArray<ObRootAddr, OB_MAX_MEMBER_NUMBER> readonly_list; // not used
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check inner stat faild", KR(ret));
  } else if (OB_FAIL(addr_agent_.fetch(
             rs_list,
             readonly_list))) {
    LOG_WARN("failed to get rslist by agent idx", KR(ret));
  } else if (OB_UNLIKELY(0 >= rs_list.count())) {
    ret = OB_EMPTY_RESULT;
    LOG_WARN("get empty rs list", KR(ret));
  } else if (OB_FAIL(convert_addr_array(rs_list, server_list))) {
    LOG_WARN("fail to convert addr array", KR(ret), K(rs_list));
  }
  return ret;
}

int ObRsMgr::renew_master_rootserver()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check inner stat faild", KR(ret));
  } else if (OB_FAIL(renew_master_rootserver(GCONF.cluster_id))) {
    LOG_WARN("failed to renew master rootserver", KR(ret));
  }
  return ret;
}

int ObRsMgr::renew_master_rootserver(const int64_t cluster_id)
{
  int ret = OB_SUCCESS;
  ObLSInfo ls_info;
  ObAddr leader;
  bool leader_exist = false;
  if (OB_ISNULL(ObCurTraceId::get_trace_id())) {
    //Prevent the current trace_id from being overwritten
    ObCurTraceId::init(GCONF.self_addr_);
  }
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check inner stat faild", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_CLUSTER_ID == cluster_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("cluster id is invalid", KR(ret), K(cluster_id));
  } else if (OB_ISNULL(GCTX.lst_operator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid partition table operator", KR(ret));
  } else if (OB_FAIL(GCTX.lst_operator_->get(cluster_id,
                                        OB_SYS_TENANT_ID,
                                        SYS_LS,
                                        share::ObLSTable::DEFAULT_MODE,
                                        ls_info))) {
    LOG_WARN("get root log stream failed",
             KR(ret), K(cluster_id),
             "tenant_id", OB_SYS_TENANT_ID,
             "ls_id", SYS_LS);
  }
  for (int64_t i = 0; i < ls_info.get_replicas().count() && OB_SUCC(ret); i++) {
    const ObLSReplica &replica = ls_info.get_replicas().at(i);
    if (replica.is_strong_leader()) {
      leader_exist = true;
      leader = replica.get_server();
      break;
    }
  }
  if (OB_SUCC(ret)) {
    if (!leader_exist) {
      ret = OB_RS_NOT_MASTER;
      LOG_WARN("no leader finded", KR(ret), K(leader_exist), K(ls_info));
    } else if (OB_UNLIKELY(!leader.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to find leader replica", KR(ret), K(ls_info), K(leader));
    } else if (cluster_id == GCONF.cluster_id) {
      ObLockGuard<ObSpinLock> lock_guard(lock_);
      master_rs_ = leader;
    } else if (OB_FAIL(remote_master_rs_map_.set_refactored(cluster_id, leader, 1 /*overwrite*/))) {
      LOG_WARN("fail to set remote master rs", KR(ret), K(cluster_id), K(leader));
    }
    ObTaskController::get().allow_next_syslog();
    LOG_INFO("[RS_MGR] new master rootserver found", "rootservice", leader, K(cluster_id));
  }
  return ret;
}

int ObRsMgr::construct_initial_server_list(
    const bool check_ls_service,
    common::ObIArray<common::ObAddr> &server_list)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  }
  // case 1: get rs from ObRsMgr master_rs_
  if (OB_SUCC(ret)) {
    ObAddr rs_addr;
    if (OB_SUCCESS != (tmp_ret = get_master_root_server(rs_addr))) {
      LOG_WARN("get master root service failed", KR(tmp_ret));
    } else if (rs_addr.is_valid()
               && OB_FAIL(server_list.push_back(rs_addr))) {
      LOG_WARN("fail to push back addr", KR(ret), K(rs_addr));
    }
  }
  // case 2: get rs_list from local configure
  if (OB_SUCC(ret)) {
    ObSEArray<ObAddr, OB_MAX_MEMBER_NUMBER> rs_list;
    if (OB_SUCCESS != (tmp_ret = get_all_rs_list_from_configure_(rs_list))) {
      LOG_WARN("fail to get all rs list", KR(tmp_ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < rs_list.count(); i++) {
        ObAddr &addr = rs_list.at(i);
        if (!has_exist_in_array(server_list, addr)
            && OB_FAIL(server_list.push_back(addr))) {
          LOG_WARN("fail to push back addr", KR(ret), K(addr));
        }
      }
    }
  }
  // case 3: try get sys_ls's member_list from ObLSService
  // For RTO scene, get_paxos_member_list() may be blocked and related threads will be hang.
  // To avoid thread hang when refreshing ls locations by rpc, we don't use member_list of sys tenant's ls.
  if (OB_SUCC(ret) && check_ls_service) {
    MTL_SWITCH(OB_SYS_TENANT_ID) {
      ObMemberList member_list;
      ObLSService *ls_svr = nullptr;
      ObLSHandle ls_handle;
      int64_t paxos_replica_number = 0;
      if (OB_ISNULL(ls_svr = MTL(ObLSService*))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("MTL ObLSService failed", KR(ret), "tenant_id", OB_SYS_TENANT_ID, K(MTL_ID()));
      } else if (OB_FAIL(ls_svr->get_ls(SYS_LS, ls_handle, ObLSGetMod::RS_MOD))) {
        if (OB_LS_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("get ls handle failed", KR(ret), "log_stream_id", SYS_LS.id());
        }
      } else if (OB_ISNULL(ls_handle.get_ls())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ls_handle.get_ls() is nullptr", KR(ret));
      } else if (OB_SUCCESS != (tmp_ret = ls_handle.get_ls()->get_paxos_member_list(member_list, paxos_replica_number))) {
        LOG_WARN("get member_list from ObLS failed", KR(tmp_ret), "tenant_id", OB_SYS_TENANT_ID,
                 "log_stream_id", SYS_LS.id(), K(ls_handle));
      }
      if (OB_SUCC(ret)) {
        ObAddr addr;
        for (int64_t i = 0; OB_SUCC(ret) && i < member_list.get_member_number(); i++) {
          if (OB_FAIL(member_list.get_server_by_index(i, addr))) {
            LOG_WARN("fail to get server", KR(ret), K(i), K(member_list));
          } else if (!has_exist_in_array(server_list, addr)
                    && OB_FAIL(server_list.push_back(addr))) {
            LOG_WARN("fail to push back addr", KR(ret), K(addr));
          }
        }
      }
    } else {
      if (OB_TENANT_NOT_IN_SERVER == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("switch tenant fail", KR(ret), "tenant_id", OB_SYS_TENANT_ID);
      }
    }

  }
  return ret;
}

int ObRsMgr::renew_remote_master_rootserver()
{
  return OB_NOT_SUPPORTED;
}

int ObRsMgr::remove_unused_remote_master_rs_(const ObIArray<int64_t> &remote_cluster_id_list)
{
  int ret = OB_SUCCESS;
  ObRemoteClusterIdGetter getter;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check inner stat faild", KR(ret));
  } else if (OB_FAIL(remote_master_rs_map_.foreach_refactored(getter))) {
    LOG_WARN("fail to get cluster id list", KR(ret));
  } else {
    const ObIArray<int64_t> &cluster_id_list = getter.get_cluster_id_list();
    ObAddr leader;
    for (int64_t i = 0; OB_SUCC(ret) && i < cluster_id_list.count(); i++) {
      const int64_t cluster_id = cluster_id_list.at(i);
      if (has_exist_in_array(remote_cluster_id_list, cluster_id)) {
        // do nothing
      } else if (OB_FAIL(remote_master_rs_map_.erase_refactored(cluster_id, &leader))) {
        LOG_WARN("fail to erase remote master rs", KR(ret), K(cluster_id));
      } else {
        LOG_INFO("[RS_MGR] remove remote master rs", K(cluster_id), K(leader));
      }
    }  // end for
  }
  return ret;
}

int ObRsMgr::convert_addr_array(
    const ObIAddrList &root_addr_list,
    ObIArray<ObAddr> &addr_list)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < root_addr_list.count(); i++) {
    const ObRootAddr &root_addr = root_addr_list.at(i);
    if (OB_FAIL(addr_list.push_back(root_addr.get_server()))) {
      LOG_WARN("fail to push back addr", KR(ret), K(root_addr));
    }
  }
  return ret;
}

}//namespace share
}//namespace oceanbase
