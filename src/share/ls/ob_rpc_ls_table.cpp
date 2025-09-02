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
#include "share/ob_common_rpc_proxy.h"         // for ObCommonRpcProxy
#include "storage/tx_storage/ob_ls_service.h" // ObLSService
#include "rootserver/ob_rs_async_rpc_proxy.h"//async

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
                      || (!ls_id.is_sys_ls() && !ls_id.is_sslog_ls()) // only surport sys tenant SSLOG LS and SYS LS
                      || ObLSTable::INNER_TABLE_ONLY_MODE == mode)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument for sys tenant's sys ls",
             KR(ret), KT(tenant_id), K(ls_id), K(mode));
  } else if (local_cluster_id == cluster_id) {
    if (OB_FAIL(get_ls_info_(ls_id, ls_info))) {
      LOG_WARN("failed to get ls info", KR(ret), K(ls_id));
    }
  } else if (OB_FAIL(get_ls_info_across_cluster_(cluster_id, ls_info))) {
    LOG_WARN("failed to get ls info", KR(ret), K(cluster_id));
  }
  return ret;
}

int ObRpcLSTable::do_detect_sslog_ls_(
    const common::ObIArray<common::ObAddr> &server_list,
    ObLSInfo &ls_info)
{
  int ret = OB_SUCCESS;
  ObTimeoutCtx ctx;
  int64_t timeout = GCONF.rpc_timeout;
  int tmp_ret = share::ObShareUtil::set_default_timeout_ctx(ctx, timeout);
  timeout = max(timeout, ctx.get_timeout());
  ObDetectSSlogLSArg arg;
  ObArray<int> return_ret_array;
  if (OB_UNLIKELY(server_list.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(server_list));
  } else if (OB_FAIL(ls_info.init(OB_SYS_TENANT_ID, SSLOG_LS))) { // reset ls_info to get a new one
    LOG_WARN("fail to init ls info", KR(ret));
  } else if (OB_ISNULL(srv_rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("src_rpc_proxy is nullptr", KR(ret), KP(srv_rpc_proxy_));
  } else {
    rootserver::ObDetectSSlogLSProxy proxy(*srv_rpc_proxy_, &obrpc::ObSrvRpcProxy::detect_sslog_ls);
    for (int64_t i = 0; OB_SUCC(ret) && i < server_list.count(); i++) {
      const ObAddr &addr = server_list.at(i);
      if (OB_UNLIKELY(!addr.is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", KR(ret), K(addr));
      } else if (OB_FAIL(arg.init(addr))) {
        LOG_WARN("fail to init arg", KR(ret), K(addr));
      } else if (OB_TMP_FAIL(proxy.call(addr, timeout, GCONF.cluster_id, OB_SYS_TENANT_ID, share::OBCG_DETECT_RS, arg))) {
        // same as sys tenant 1 LS, use OBCG_DETECT_RS thread pool
        LOG_WARN("fail to send rpc", KR(tmp_ret), K(addr), K(timeout), K(arg));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_TMP_FAIL(proxy.wait_all(return_ret_array))) {
      LOG_WARN("wait batch result failed", KR(tmp_ret), KR(ret));
    } else {
      common::ObArray<ObLSReplica> leader_replicas;
      const bool ret_count_match = (OB_SUCCESS == proxy.check_return_cnt(return_ret_array.count()));
      for (int64_t i = 0; OB_SUCC(ret) && i < proxy.get_results().count(); i++) {
        const ObDetectSSlogLSResult *result = proxy.get_results().at(i);
        if (ret_count_match && OB_SUCCESS != return_ret_array.at(i)) {
          LOG_WARN("fail to get result by rpc, just ignore", "return_ret", return_ret_array.at(i));
        } else if (OB_ISNULL(result)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("nullptr result", KR(ret), KP(result));
        } else if (OB_UNLIKELY(!result->has_sslog() || !result->is_valid())) {
          LOG_INFO("server has no sslog LS, invalid replica, skip", KPC(result));
        } else {
          ObLSReplica tmp_replica;
          if (OB_FAIL(tmp_replica.assign(result->get_replica()))) {
            LOG_WARN("fail to assign replica", KR(ret), KPC(result));
          } else if (is_strong_leader(tmp_replica.get_role()) && OB_FAIL(leader_replicas.push_back(tmp_replica))) {
            LOG_WARN("fail to push back replica", KR(ret), K(tmp_replica));
          } else if (is_follower(tmp_replica.get_role())) {
            tmp_replica.update_to_follower_role();
            if (OB_FAIL(ls_info.add_replica(tmp_replica))) {
              LOG_WARN("fail to add replica", KR(ret), K(tmp_replica));
            }
            // Here, we use update_to_follower_role() to set replica's proposal_id to 0 and role to follower.
            // In rpc mode, only get SSLOG LS info needs to do this , SYS LS does not need to.
            // For SYS LS, we record it's info in RS memory, when updating RS memory and meta table, the same treatment has been done for the follower.
            // Getting SYS LS info via RPC is equivalent to reading the memory of RS, so no further processing is required.

            // Next, let's explain why we need to set replica's proposal_id to 0 and role to follower:
            //   Since RS LS info is reported asynchronously, it may not be accurate enough.
            //   In some scenarios, it is possible that follower has a larger proposal_id. for exeample:
            //     1. A is leader, proposal_id is 1.
            //     2. B become leader, proposal_id is 2, B later became follower.
            //     3. C is new leader, but has no report yet.
            //   When looking for a leader, we can’t just judge whether the role is the leader, need find the leader has max proposal_id among replicas.
            //   But in normal cases, proposal_id is equal in all reported replica, which unable to make the above judgment.
            //   So RS made adjustments to LS replica proposal_id which recorded in RS memory or meta table, only the leader's proposal_id is valid,
            //   the follower's proposal_id is set to 0.
            //   Attention:
            //     In the above situation, RS cannot get a correct leader because the correct leader has not yet reported.
            //     Here it depends on the subsequent report of the new leader.
          }
        }
      }
      LOG_TRACE("before update_replica_status", KR(ret), K(leader_replicas), K(ls_info));
      if (FAILEDx(check_sslog_leader_replicas_(leader_replicas, ls_info))) {
        LOG_WARN("failed to check sslog leader replicas", KR(ret), K(leader_replicas), K(ls_info));
      } else if (OB_FAIL(ls_info.update_replica_status())) {
        LOG_WARN("update replica status failed", KR(ret), K(ls_info));
      }
      LOG_TRACE("aftr update_replica_status", KR(ret), K(ls_info));
    }
  }
  return ret;
}

int ObRpcLSTable::check_sslog_leader_replicas_(
    common::ObArray<ObLSReplica> &leader_replicas,
    ObLSInfo &ls_info)
{
  /*
  When multiple members are returned as leaders in a query, it is necessary to judge based on proposal_id.
  The member with the max proposal_id is the leader, and all other members need to be set to follower.
  */
  int ret = OB_SUCCESS;
  int64_t max_proposal_id = palf::INVALID_PROPOSAL_ID;
  int64_t idx = OB_INVALID_INDEX; // -1, max leader proposal_id position
  for (int64_t index = 0; OB_SUCC(ret) && index < leader_replicas.count(); index++) {
    const ObLSReplica &replica = leader_replicas.at(index);
    if (palf::INVALID_PROPOSAL_ID == max_proposal_id || replica.get_proposal_id() > max_proposal_id) {
      max_proposal_id = replica.get_proposal_id();
      idx = index;
    }
  }
  for (int64_t index = 0; OB_SUCC(ret) && index < leader_replicas.count(); index++) {
    if (index != idx) {
      // set all other members to follower
      leader_replicas.at(index).update_to_follower_role();
    }
    if (OB_FAIL(ls_info.add_replica(leader_replicas.at(index)))) {
      LOG_WARN("fail to add replica", KR(ret), K(index), K(leader_replicas));
    }
  }
  return ret;
}

int ObRpcLSTable::get_ls_info_(
    const ObLSID &ls_id,
    ObLSInfo &ls_info)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t local_cluster_id = GCONF.cluster_id;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(!ls_info.is_valid()
                      || (!ls_id.is_sys_ls() && !ls_id.is_sslog_ls()))) { // only surport sys tenant SSLOG LS and SYS LS
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id), K(ls_info));
  } else {
    ObArray<ObAddr> server_list;
    bool need_retry = false;
    const ObLSReplica *leader = NULL;
    /*
      case 1. Get rs from ObRsMgr
      case 2. Get rs_list from local configure
      case 3. Try get SYS LS member_list or SSLOG LS member_list and learner_list from ObLSService
            a. For SSLOG LS, we did not record the LS information in the RS memory, so need to ask all members(member_list and learner_list)
            b. For SYS LS, RS memory contains all reported LS information, So only need to find the location of RS(in member_list).
     */
    if (OB_FAIL(rs_mgr_->construct_initial_server_list(true/*check_ls_service*/, ls_id, server_list))) {
      LOG_WARN("fail to construct initial server list", KR(ret));
    } else if (ls_id.is_sslog_ls() && OB_FAIL(do_detect_sslog_ls_(server_list, ls_info))) {
      need_retry = true;
      LOG_WARN("fail to detect sslog ls, try use all_server_list", KR(ret), K(server_list));
    } else if (ls_id.is_sys_ls() && OB_FAIL(do_detect_master_rs_ls_(local_cluster_id, server_list, ls_info))) {
      need_retry = true;
      LOG_WARN("fail to detect master rs, try use all_server_list", KR(ret), K(local_cluster_id), K(server_list));
    } else if (OB_TMP_FAIL(ls_info.find_leader(leader)) || OB_ISNULL(leader)) {
      need_retry = true;
      LOG_INFO("leader doesn't exist, try use all_server_list", KR(tmp_ret), K(ls_id), K(ls_info));
    } else {
      LOG_INFO("fetch ls success", K(local_cluster_id), K(ls_id), K(ls_info), K(server_list));
    }
    // case 4: try use all_server_list from local configure
    if (need_retry) { // overwrite ret
      ObArray<ObAddr> all_server_list;
      ObArray<ObAddr> empty_list;
      const ObArray<ObAddr> &excluded_list = ls_id.is_sys_ls() ? server_list : empty_list;
      // For SSLOG LS, need to request all servers to get results, for SYS LS, only need to find RS.
      if (OB_FAIL(ObShareUtil::parse_all_server_list(excluded_list, all_server_list))) {
        LOG_WARN("fail to construct all server list", KR(ret), K(excluded_list));
      } else if (all_server_list.empty()) {
        // all_server_list is empty, do nothing
        LOG_INFO("all_server_list is empty, do nothing", KR(ret), K(all_server_list));
      } else if (ls_id.is_sslog_ls() && OB_FAIL(do_detect_sslog_ls_(all_server_list, ls_info))) {
        LOG_WARN("fail to detect sslog ls", KR(ret), K(all_server_list));
      } else if (ls_id.is_sys_ls() && OB_FAIL(do_detect_master_rs_ls_(local_cluster_id, all_server_list, ls_info))) {
        LOG_WARN("fail to detect master rs", KR(ret), K(local_cluster_id), K(all_server_list));
      } else {
        LOG_INFO("fetch ls success", K(local_cluster_id), K(ls_id), K(ls_info), K(all_server_list));
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
      || (!replica.get_ls_id().is_sys_ls() && !replica.get_ls_id().is_sslog_ls())
      || inner_table_only) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(replica), K(inner_table_only));
  } else if (is_sys_tenant(replica.get_tenant_id()) && replica.get_ls_id().is_sslog_ls()) {
    LOG_INFO("sslog ls no need process", K(replica));
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
      || (!ls_id.is_sys_ls() && !ls_id.is_sslog_ls())
      || !server.is_valid())
      || inner_table_only) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id),
             K(ls_id), K(server), K(inner_table_only));
  } else if (is_sys_tenant(tenant_id) && ls_id.is_sslog_ls()) {
    LOG_INFO("sslog ls no need process", K(tenant_id), K(ls_id));
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
