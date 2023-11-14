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

#include "ob_rpc_ls_table.h"           // for declarations of functions in this cpp
#include "share/config/ob_server_config.h"     // for ObServerConfig
#include "share/ob_rs_mgr.h"                   // for ObRpcLSTable
#include "share/ob_common_rpc_proxy.h"         // for ObCommonRpcProxy
#include "rootserver/ob_rs_async_rpc_proxy.h"//async
#include "share/ob_rpc_struct.h"//ObDetectMasterRsArg, ObRemoveSysLsArg
#include "share/ob_share_util.h"//ObShareUtil
#include "share/ob_srv_rpc_proxy.h"//ObSrvRpcProxy
#include "lib/mysqlclient/ob_mysql_proxy.h"//ObMySQLProxy
#include "share/ob_share_util.h" // ObShareUtil
#include "share/resource_manager/ob_cgroup_ctrl.h" //CGID_DEF

namespace oceanbase
{
using namespace common;
using namespace obrpc;

namespace share
{
ObRpcLSTable::ObRpcLSTable()
  : ObLSTable(),
    is_inited_(false),
    rpc_proxy_(NULL),
    rs_mgr_(NULL),
    srv_rpc_proxy_(NULL),
    sql_proxy_(NULL)
{
}

ObRpcLSTable::~ObRpcLSTable()
{
}

int ObRpcLSTable::check_inner_stat_() const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(rs_mgr_) || OB_ISNULL(rpc_proxy_)
             || OB_ISNULL(srv_rpc_proxy_) || OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is null", KR(ret), KP_(rpc_proxy), KP_(rs_mgr), KP_(srv_rpc_proxy), KP_(sql_proxy));
  }
  return ret;
}

int ObRpcLSTable::init(
    ObCommonRpcProxy &rpc_proxy,
    obrpc::ObSrvRpcProxy &srv_rpc_proxy,
    ObRsMgr &rs_mgr,
    common::ObMySQLProxy &sql_proxy)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else {
    rpc_proxy_ = &rpc_proxy;
    srv_rpc_proxy_ = &srv_rpc_proxy;
    rs_mgr_ = &rs_mgr;
    sql_proxy_ = &sql_proxy;
    is_inited_ = true;
  }
  return ret;
}

int ObRpcLSTable::get(
    const int64_t cluster_id,
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    const ObLSTable::Mode mode,
    ObLSInfo &ls_info)
{
  int ret = OB_SUCCESS;
  const int64_t local_cluster_id = GCONF.cluster_id;
  if (OB_FAIL(ls_info.init(tenant_id, ls_id))) {
    LOG_WARN("fail to init ls info", KR(ret), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(!is_sys_tenant(tenant_id)
             || !ls_id.is_sys_ls()
             || ObLSTable::INNER_TABLE_ONLY_MODE == mode)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument for sys tenant's sys ls",
             KR(ret), KT(tenant_id), K(ls_id), K(mode));
  } else if (local_cluster_id == cluster_id) {
    if (OB_FAIL(get_ls_info_(ls_info))) {
      LOG_WARN("failed to get ls info", KR(ret));
    }
  } else if (OB_FAIL(get_ls_info_across_cluster_(cluster_id, ls_info))) {
    LOG_WARN("failed to get ls info", KR(ret), K(cluster_id));
  }
  return ret;
}

int ObRpcLSTable::get_ls_info_(ObLSInfo &ls_info)
{
  int ret = OB_SUCCESS;
  const int64_t local_cluster_id = GCONF.cluster_id;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (!ls_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_info));
  } else {
    int tmp_ret = OB_SUCCESS;
    ObArray<ObAddr> rs_list;
    bool need_retry = false;
    const ObLSReplica *leader = NULL;
    const bool check_ls_service = true;
    /*
     * case 1: get rs from ObRsMgr
     * case 2: get rs_list from local configure
     * case 3: try get __all_core_table's member_list from ObLSService
     */
    if (OB_FAIL(rs_mgr_->construct_initial_server_list(check_ls_service, rs_list))) {
      LOG_WARN("fail to construct initial server list", KR(ret));
    } else if (OB_FAIL(do_detect_master_rs_ls_(local_cluster_id, rs_list, ls_info))) {
      need_retry = true;
      LOG_WARN("fail to detect master rs, try use all_server_list",
               KR(ret), K(local_cluster_id), K(rs_list));
    } else if (OB_SUCCESS != (tmp_ret = ls_info.find_leader(leader))
               || OB_ISNULL(leader)) {
      need_retry = true;
      LOG_INFO("leader doesn't exist, try use all_server_list", KR(tmp_ret), K(ls_info));
    } else {
      LOG_INFO("fetch root ls success", K(local_cluster_id), K(ls_info), K(rs_list));
    }
    // case 4: try use all_server_list from local configure
    if (need_retry) { // overwrite ret
      ObArray<ObAddr> server_list;
      if (OB_FAIL(ObShareUtil::parse_all_server_list(rs_list, server_list))) {
        LOG_WARN("fail to construct all server list", KR(ret), K(rs_list));
      } else if (server_list.empty()) {
        // server_list is empty, do nothing
        LOG_INFO("server_list is empty, do nothing", KR(ret), K(server_list));
      } else if (OB_FAIL(do_detect_master_rs_ls_(local_cluster_id, server_list, ls_info))) {
        LOG_WARN("fail to detect master rs", KR(ret), K(local_cluster_id), K(server_list));
      } else {
        LOG_INFO("fetch root ls success", K(local_cluster_id), K(ls_info), K(server_list));
      }
    }
  }
  return ret;
}

int ObRpcLSTable::get_ls_info_across_cluster_(const int64_t cluster_id, ObLSInfo &ls_info)
{
  UNUSEDx(cluster_id, ls_info);
  int ret = OB_NOT_SUPPORTED;
  return ret;
}

int ObRpcLSTable::update(
    const ObLSReplica &replica,
    const bool inner_table_only)
{
  int ret = OB_SUCCESS;
  ObAddr rs_addr;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (!replica.is_valid()
      || !is_sys_tenant(replica.get_tenant_id())
      || !replica.get_ls_id().is_sys_ls()
      || inner_table_only) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(replica), K(inner_table_only));
  } else if(OB_FAIL(rs_mgr_->get_master_root_server(rs_addr))) {
    LOG_WARN("get master root service failed", KR(ret));
  } else {
    int64_t timeout_us = 0;
    if (OB_FAIL(get_timeout_(timeout_us))) {
      LOG_WARN("get timeout failed", KR(ret));
    } else if (timeout_us <= 0) {
      ret = OB_TIMEOUT;
      LOG_WARN("process timeout", KR(ret), K(timeout_us));
    } else if (OB_FAIL(rpc_proxy_->to(rs_addr).timeout(timeout_us).report_sys_ls(replica))) {
      LOG_WARN("report sys_tenant's ls through rpc failed", K(replica), K(rs_addr), KR(ret));
    }
  }
  LOG_INFO("update sys_tenant's ls replica", KR(ret), K(replica));
  return ret;
}

int ObRpcLSTable::get_timeout_(int64_t &timeout)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else {
    timeout = GCONF.rpc_timeout;
    ObTimeoutCtx ctx;
    if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, timeout))) {
      LOG_WARN("fail to set default timeout ctx", KR(ret));
    } else {
      timeout = ctx.get_timeout();
    }
  }
  return ret;
}

int ObRpcLSTable::do_detect_master_rs_ls_(
    const int64_t cluster_id,
    const ObIArray<ObAddr> &initial_server_list,
    ObLSInfo &ls_info)
{
  int ret = OB_SUCCESS;
  ObArray<ObAddr> server_list;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check inner stat faild", KR(ret));
  } else if (initial_server_list.empty()) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("empty rootservice list", KR(ret));
  } else if (OB_FAIL(server_list.assign(initial_server_list))) {
    LOG_WARN("fail to assign server list", KR(ret), K(initial_server_list));
  } else {
    int64_t start_idx = 0;
    int64_t end_idx = server_list.count() - 1;
    const ObLSReplica *leader = NULL;
    // reset ls_info to get a new one
    if (OB_FAIL(ls_info.init(OB_SYS_TENANT_ID, SYS_LS))) {
      LOG_WARN("fail to init ls info", KR(ret));
    }
    while (OB_SUCC(ret)
           && start_idx <= end_idx
           && end_idx < server_list.count()
           && OB_ISNULL(leader)) {
      LOG_TRACE("[RPC_LS] do detect master rs", K(cluster_id), K(start_idx), K(end_idx), K(server_list));
      if (OB_FAIL(do_detect_master_rs_ls_(cluster_id, start_idx, end_idx,
                                                 server_list, ls_info))) {
        LOG_WARN("fail to detect master rs", KR(ret), K(cluster_id),
                  K(start_idx), K(end_idx), K(server_list));
      } else {
        int tmp_ret = ls_info.find_leader(leader);
        if (OB_SUCCESS == tmp_ret && OB_NOT_NULL(leader)) {
          LOG_TRACE("[RPC_LS] get master rs", KR(ret), K(cluster_id), "addr", leader->get_server());
        }
        start_idx = end_idx + 1;
        end_idx = server_list.count() - 1;
      }
    }
  }
  return ret;
}

int ObRpcLSTable::do_detect_master_rs_ls_(
    const int64_t cluster_id,
    const int64_t start_idx,
    const int64_t end_idx,
    ObIArray<ObAddr> &server_list,
    ObLSInfo &ls_info)
{
  int ret = OB_SUCCESS;
  ObTimeoutCtx ctx;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check inner stat faild", KR(ret));
  } else if (start_idx < 0 || start_idx > end_idx || end_idx >= server_list.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("start_idx/end_idx is invalid", KR(ret),
             K(start_idx), K(end_idx), "list_cnt", server_list.count());
  } else {
    ObArray<ObAddr> tmp_addrs;
    ObTimeoutCtx ctx;
    int64_t timeout = GCONF.rpc_timeout;  // default value is 2s
    int tmp_ret = share::ObShareUtil::set_default_timeout_ctx(ctx, timeout);
    timeout = max(timeout, ctx.get_timeout());  // at least 2s

    rootserver::ObDetectMasterRsLSProxy proxy(
        *srv_rpc_proxy_, &obrpc::ObSrvRpcProxy::detect_master_rs_ls);
    ObDetectMasterRsArg arg;
    for (int64_t i = start_idx; OB_SUCC(ret) && i <= end_idx; i++) {
      ObAddr &addr = server_list.at(i);
      if (!addr.is_valid()) {
        // TODO: @wanhong.wwh: need check when addr is not valid
      } else if (OB_FAIL(tmp_addrs.push_back(addr))) {
        LOG_WARN("fail to push back addr", KR(ret), K(addr));
      } else if (OB_FAIL(arg.init(addr, cluster_id))) {
        LOG_WARN("fail to init arg", KR(ret), K(addr), K(cluster_id));
      } else if (OB_TMP_FAIL(proxy.call(addr, timeout, cluster_id,
                 OB_SYS_TENANT_ID, share::OBCG_DETECT_RS, arg))) {
        LOG_WARN("fail to send rpc", KR(tmp_ret), K(cluster_id), K(addr), K(timeout), K(arg));
      }
    }

    ObArray<int> return_ret_array;
    if (OB_TMP_FAIL(proxy.wait_all(return_ret_array))) { // ignore ret
      LOG_WARN("wait batch result failed", KR(tmp_ret), KR(ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    } else if (OB_FAIL(ret)) {
    } else {
      // don't use arg/dest here because call() may has failure.
      // !use_invalid_addr means count of args_/dest_/results_/return_rets are matched.
      bool leader_exist = false;
      const bool use_invalid_addr = (OB_SUCCESS != proxy.check_return_cnt(return_ret_array.count()));
      ObAddr invalid_addr;
      for (int64_t i = 0; OB_SUCC(ret) && i < proxy.get_results().count(); i++) {
        const ObAddr &addr = use_invalid_addr ? invalid_addr : proxy.get_dests().at(i);
        const ObDetectMasterRsLSResult *result = proxy.get_results().at(i);
        if (!use_invalid_addr && OB_SUCCESS != return_ret_array.at(i)) {
          LOG_WARN("fail to get result by rpc, just ignore", "tmp_ret", return_ret_array.at(i), K(addr));
        } else if (OB_ISNULL(result) || !result->is_valid()) {
          // return fail
        } else if (OB_FAIL(deal_with_result_ls_(*result, leader_exist, server_list, ls_info))) {
          LOG_WARN("fail to deal with result", KR(ret), K(addr), KPC(result));
        } else {
          LOG_TRACE("detect master rs", KR(ret), K(addr), KPC(result));
        }
      } // end for

      if (use_invalid_addr || proxy.get_results().count() <= 0) {
        LOG_WARN("Detect master rs may be failed", KR(ret), K(tmp_addrs));
      }
    }
  }
  return ret;
}

int ObRpcLSTable::deal_with_result_ls_(
    const ObDetectMasterRsLSResult &result,
    bool &leader_exist,
    ObIArray<ObAddr> &server_list,
    ObLSInfo &ls_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check inner stat faild", KR(ret));
  } else {// try expand server_list
    const ObAddr &master_rs = result.get_master_rs();
    const ObRole role = result.get_role();
    const ObLSReplica &replica = result.get_replica();
    if (master_rs.is_valid() && !has_exist_in_array(server_list, master_rs)) {
      if (OB_FAIL(server_list.push_back(master_rs))) {
        LOG_WARN("fail to push back addr", KR(ret), K(result));
      }
    }
    ObIArray<ObLSReplica> &replicas = ls_info.get_replicas();
    if (OB_FAIL(ret)) {
    } else if (ObRole::INVALID_ROLE == role) {// do nothing
      // invalid role means target server don't have this logstream
    } else if (ObRole::FOLLOWER == role) {// try construct ls_info with follower's replicas
      if (!leader_exist && OB_FAIL(replicas.push_back(replica))) {
        LOG_WARN("fail to push back replica", KR(ret), K(result));
      }
    } else if (ObRole::LEADER == role) {
      bool need_assign = false;
      if (!leader_exist) {
        need_assign = true;
      } else {
        const ObLSReplica *leader = NULL;
        if (OB_FAIL(ls_info.find_leader(leader))) {
          LOG_WARN("fail to find leader", KR(ret), K(ls_info));
        } else if (OB_ISNULL(leader) || ObRole::LEADER != leader->get_role()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("leader is invalid", KR(ret), KPC(leader));
        } else if (replica.get_proposal_id() > leader->get_proposal_id()) {
          need_assign = true;
        }
      }
      if (OB_SUCC(ret) && need_assign) {
        if (OB_FAIL(replicas.assign(result.get_ls_info().get_replicas()))) {
          LOG_WARN("fail to assign replicas", KR(ret), K(result));
        }
      }
      leader_exist = true;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("role is invalid", KR(ret), K(result));
    }
  }
  return ret;
}

int ObRpcLSTable::remove(
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    const ObAddr &server,
    const bool inner_table_only)
{
  int ret = OB_SUCCESS;
  ObAddr rs_addr;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(!is_sys_tenant(tenant_id)
      || !ls_id.is_sys_ls()
      || !server.is_valid())
      || inner_table_only) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id),
             K(ls_id), K(server), K(inner_table_only));
  } else if(OB_FAIL(rs_mgr_->get_master_root_server(rs_addr))) {
    LOG_WARN("get master root service failed", KR(ret));
  } else {
    int64_t timeout_us = 0;
    obrpc::ObRemoveSysLsArg arg(server);
    if (OB_FAIL(get_timeout_(timeout_us))) {
      LOG_WARN("get timeout failed", KR(ret));
    } else if (timeout_us <= 0) {
      ret = OB_TIMEOUT;
      LOG_WARN("process timeout", KR(ret), K(timeout_us));
    } else if (OB_FAIL(rpc_proxy_->to(rs_addr).timeout(timeout_us).remove_sys_ls(arg))) {
      LOG_WARN("remove sys_tenant's ls through rpc failed", KR(ret), K(rs_addr), K(arg));
    }
  }
  LOG_INFO("remove sys_tenant's ls replica", KR(ret), K(server));
  return ret;
}

}//end namespace share
}//end namespace oceanbase
