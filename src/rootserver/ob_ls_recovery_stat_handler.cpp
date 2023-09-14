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

#include "ob_ls_recovery_stat_handler.h" // ObLSRecoveryStatHandler
#include "lib/utility/ob_macro_utils.h" // OB_FAIL
#include "lib/oblog/ob_log_module.h"  // LOG_*
#include "lib/utility/ob_print_utils.h" // TO_STRING_KV
#include "logservice/ob_log_service.h" // ObLogService
#include "rootserver/ob_tenant_info_loader.h" // ObTenantInfoLoader
#include "rootserver/ob_ls_recovery_reportor.h" // ObLSRecoveryReportor
#include "rootserver/ob_ls_service_helper.h"//ObLSServiceHelper

namespace oceanbase
{
namespace rootserver
{
int ObLSRecoveryStatHandler::init(const uint64_t tenant_id, ObLS *ls)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLSRecoveryStatHandler init twice", KR(ret), K_(is_inited));
  } else if (OB_ISNULL(ls) || !is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(ls), K(tenant_id));
  } else {
    ls_ = ls;
    tenant_id_ = tenant_id;
    is_inited_ = true;
    LOG_INFO("ObLSRecoveryStatHandler init success", K(this));
  }

  return ret;
}

void ObLSRecoveryStatHandler::reset()
{
  is_inited_ = false;
  ls_ = NULL;
  tenant_id_ = OB_INVALID_TENANT_ID;
}

int ObLSRecoveryStatHandler::check_inner_stat_()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(ls_) || !is_valid_tenant_id(tenant_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Member variables is not init", KR(ret), KP(ls_), K_(tenant_id));
  }
  return ret;
}

int ObLSRecoveryStatHandler::get_ls_replica_readable_scn(share::SCN &readable_scn)
{
  int ret = OB_SUCCESS;
  readable_scn = SCN::min_scn();
  share::SCN readable_scn_to_increase = SCN::min_scn();

  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("inner stat error", KR(ret), K(is_inited_));
  } else if (OB_FAIL(ls_->get_max_decided_scn(readable_scn))) {
    LOG_WARN("failed to get_max_decided_scn", KR(ret), KPC_(ls));
  } else if (FALSE_IT(readable_scn_to_increase = readable_scn)) {
  } else if (OB_FAIL(increase_ls_replica_readable_scn_(readable_scn_to_increase))) {
    if (OB_NOT_MASTER == ret) {
      // if not master, do not increase_ls_replica_readable_scn
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to increase_ls_replica_readable_scn_", KR(ret), K(readable_scn_to_increase), KPC_(ls));
    }
  } else {
    readable_scn = readable_scn_to_increase;
  }

  return ret;
}

int ObLSRecoveryStatHandler::increase_ls_replica_readable_scn_(SCN &readable_scn)
{
  int ret = OB_SUCCESS;
  SCN sync_scn = SCN::min_scn();
  int64_t first_proposal_id = palf::INVALID_PROPOSAL_ID;
  int64_t second_proposal_id = palf::INVALID_PROPOSAL_ID;
  common::ObRole first_role;
  common::ObRole second_role;
  logservice::ObLogService *ls_svr = MTL(logservice::ObLogService*);
  rootserver::ObTenantInfoLoader *tenant_info_loader = MTL(rootserver::ObTenantInfoLoader*);
  SCN replayable_scn = SCN::base_scn();

  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("inner stat error", KR(ret), K_(is_inited));
  } else if (OB_ISNULL(ls_svr) || OB_ISNULL(tenant_info_loader)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pointer is null", KR(ret), KP(ls_svr), KP(tenant_info_loader));
  } else if (OB_FAIL(ls_svr->get_palf_role(ls_->get_ls_id(), first_role, first_proposal_id))) {
    LOG_WARN("failed to get first role", KR(ret), K(ls_->get_ls_id()), KP(ls_svr), KPC_(ls));
  } else if (!is_strong_leader(first_role)) {
    ret = OB_NOT_MASTER;
    // Since the follower replica also call this function, return OB_NOT_MASTER does not LOG_WARN
  } else if (!readable_scn.is_valid_and_not_min()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(readable_scn));
  // scn get order: read_scn before replayable_scn before sync_scn
  } else if (OB_FAIL(tenant_info_loader->get_replayable_scn(replayable_scn))) {
    LOG_WARN("failed to get replayable_scn", KR(ret));
  } else if (OB_FAIL(ObLSServiceHelper::get_ls_replica_sync_scn(MTL_ID(), ls_->get_ls_id(), sync_scn))) {
    LOG_WARN("failed to get ls sync scn", KR(ret), "tenant_id", MTL_ID());
  } else if (OB_FAIL(ls_svr->get_palf_role(ls_->get_ls_id(), second_role, second_proposal_id))) {
    LOG_WARN("failed to get second role", KR(ret), K(ls_->get_ls_id()), KP(ls_svr), KPC_(ls));
  } else if (!(first_proposal_id == second_proposal_id
             && first_role == second_role)) {
    ret = OB_NOT_MASTER;
    LOG_WARN("not leader", KR(ret), K(first_proposal_id), K(second_proposal_id), K(first_role),
                           K(second_role), KPC_(ls));
  } else {
    if (sync_scn < replayable_scn && readable_scn == sync_scn
        && sync_scn.is_valid_and_not_min() && replayable_scn.is_valid_and_not_min()
        && readable_scn.is_valid_and_not_min()) {
      // two scenarios
      // 1. when sync scn is pushed forward in switchover
      // 2. wait offline LS
      readable_scn = replayable_scn;
    }
  }

  return ret;
}

int ObLSRecoveryStatHandler::get_ls_level_recovery_stat(ObLSRecoveryStat &ls_recovery_stat)
{
  int ret = OB_SUCCESS;
  share::SCN sync_scn = SCN::min_scn();
  share::SCN readable_scn = SCN::min_scn();
  logservice::ObLogService *ls_svr = MTL(logservice::ObLogService*);
  common::ObRole role;
  int64_t first_proposal_id = palf::INVALID_PROPOSAL_ID;
  int64_t second_proposal_id = palf::INVALID_PROPOSAL_ID;
  ls_recovery_stat.reset();

  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("inner stat error", KR(ret), K_(is_inited));
  } else if (OB_ISNULL(ls_svr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("pointer is null", KR(ret), KP(ls_svr));
  } else if (OB_FAIL(ls_svr->get_palf_role(ls_->get_ls_id(), role, first_proposal_id))) {
    LOG_WARN("failed to get first role", KR(ret), K(ls_->get_ls_id()), KPC_(ls));
  } else if (!is_strong_leader(role)) {
    ret = OB_NOT_MASTER;
    LOG_TRACE("not leader", KR(ret), K(role), KPC_(ls));
  } else if (OB_FAIL(do_get_ls_level_readable_scn_(readable_scn))) {
    LOG_WARN("failed to do_get_ls_level_readable_scn_", KR(ret), KPC_(ls));
  // scn get order: read_scn before replayable_scn before sync_scn
   } else if (OB_FAIL(ObLSServiceHelper::get_ls_replica_sync_scn(MTL_ID(), ls_->get_ls_id(), sync_scn))) {
    LOG_WARN("failed to get ls sync scn", KR(ret), "tenant_id", MTL_ID());
  } else if (OB_FAIL(ls_recovery_stat.init_only_recovery_stat(tenant_id_, ls_->get_ls_id(),
                                                              sync_scn, readable_scn))) {
    LOG_WARN("failed to init ls recovery stat", KR(ret), KPC_(ls), K_(tenant_id), K(sync_scn), K(readable_scn));
  } else if (OB_FAIL(ls_svr->get_palf_role(ls_->get_ls_id(), role, second_proposal_id))) {
    LOG_WARN("failed to get palf role again", KR(ret), K(role), KPC_(ls));
  } else if (first_proposal_id != second_proposal_id || !is_strong_leader(role)) {
    ret = OB_EAGAIN;
    LOG_INFO("role changed, try again", KR(ret), K(role),
              K(first_proposal_id), K(second_proposal_id), KPC_(ls));
  }

  return ret;
}

int ObLSRecoveryStatHandler::do_get_ls_level_readable_scn_(SCN &read_scn)
{
  int ret = OB_SUCCESS;
  palf::AccessMode access_mode;
  int64_t unused_mode_version = 0;
  share::SCN majority_min_readable_scn = SCN::min_scn();
  read_scn = SCN::min_scn();

  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("inner stat error", KR(ret), K_(is_inited));
    // scn get order: read_scn before replayable_scn before sync_scn
  } else if (OB_FAIL(ls_->get_max_decided_scn(read_scn))) {
    LOG_WARN("failed to get_max_decided_scn", KR(ret), KPC_(ls));
  } else if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_2_0_0) {
    //Before the cluster version is pushed up, the majority is not counted,
    //and this RPC is only supported in version 4.2
  } else if (OB_FAIL(get_majority_readable_scn_(read_scn /* leader_readable_scn */, majority_min_readable_scn))) {
    LOG_WARN("failed to get_majority_readable_scn_", KR(ret), K(read_scn), KPC_(ls));
  } else {
    read_scn = majority_min_readable_scn;
  }

  LOG_TRACE("do_get_ls_level_readable_scn_ finished", KR(ret), KPC_(ls), K(read_scn),
      K(majority_min_readable_scn));

  return ret;
}

int ObLSRecoveryStatHandler::construct_new_member_list_(
    const common::ObMemberList &member_list_ori,
    const common::GlobalLearnerList &degraded_list,
    const int64_t paxos_replica_number_ori,
    ObIArray<common::ObAddr> &member_list_new,
    int64_t &paxos_replica_number_new)
{
  int ret = OB_SUCCESS;
  bool found_me = false;
  member_list_new.reset();
  paxos_replica_number_new = paxos_replica_number_ori;
  if (OB_UNLIKELY(0 >= member_list_ori.get_member_number()
        || 0 > degraded_list.get_member_number()
        || 0 >= paxos_replica_number_ori)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(member_list_ori), K(degraded_list), K(paxos_replica_number_ori));
  } else {
    common::ObMember member;

    for (int64_t i = 0; OB_SUCC(ret) && i < member_list_ori.get_member_number(); ++i) {
      member.reset();
      if (OB_FAIL(member_list_ori.get_member_by_index(i, member))) {
        LOG_WARN("get_member_by_index failed", KR(ret), K(i));
      } else if (OB_UNLIKELY(!member.is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("member is invalid", KR(ret), K(member));
      } else if (degraded_list.contains(member.get_server())) {
        paxos_replica_number_new--;
        // do not count degraded member
      } else if (OB_FAIL(member_list_new.push_back(member.get_server()))) {
        LOG_WARN("fail to push back member_list_new", KR(ret), K(member), K(member_list_new));
      } else if (member.get_server() == GCTX.self_addr()) {
        found_me = true;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (!found_me) {
      ret = OB_EAGAIN;
      LOG_WARN("current leader degraded, try again", KR(ret), K(member_list_ori), K(degraded_list),
          K(paxos_replica_number_ori), K(member_list_new), K(paxos_replica_number_new));
    }
  }
  return ret;
}

int ObLSRecoveryStatHandler::get_palf_stat_(
    palf::PalfStat &palf_stat)
{
  int ret = OB_SUCCESS;
  palf_stat.reset();
  logservice::ObLogService *log_service = NULL;
  palf::PalfHandleGuard palf_handle_guard;

  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("inner stat error", KR(ret), K_(is_inited));
  } else if (OB_ISNULL(log_service = MTL(logservice::ObLogService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get MTL log_service", KR(ret), K_(tenant_id), KPC_(ls));
  } else if (OB_FAIL(log_service->open_palf(ls_->get_ls_id(), palf_handle_guard))) {
    LOG_WARN("failed to open palf", KR(ret), K_(tenant_id), KPC_(ls));
  } else if (OB_FAIL(palf_handle_guard.stat(palf_stat))) {
    LOG_WARN("get palf_stat failed", KR(ret), KPC_(ls));
  }

  return ret;
}

int ObLSRecoveryStatHandler::get_latest_palf_stat_(
    palf::PalfStat &palf_stat)
{
  int ret = OB_SUCCESS;
  palf_stat.reset();
  common::ObMemberList ob_member_list_latest;
  int64_t paxos_replica_number_latest = 0;

  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("inner stat error", KR(ret), K_(is_inited));
  } else if (OB_FAIL(get_palf_stat_(palf_stat))) {
    LOG_WARN("get palf_stat failed", KR(ret), KPC_(ls));
  } else if (OB_FAIL(ls_->get_paxos_member_list(ob_member_list_latest, paxos_replica_number_latest))) {
    LOG_WARN("get latest paxos member_list failed", KR(ret), KPC_(ls));
  } else if (!ob_member_list_latest.member_addr_equal(palf_stat.paxos_member_list_)
      || paxos_replica_number_latest != palf_stat.paxos_replica_num_) {
    ret = OB_EAGAIN;
    LOG_WARN("palf_stat is not latest, try again", KR(ret), KPC_(ls), K(ob_member_list_latest),
        K(paxos_replica_number_latest), K(palf_stat));
  }

  return ret;
}

int ObLSRecoveryStatHandler::get_majority_readable_scn_(
    const share::SCN &leader_readable_scn,
    share::SCN &majority_min_readable_scn)
{
  int ret = OB_SUCCESS;
  majority_min_readable_scn = leader_readable_scn;
  palf::PalfStat palf_stat_first;
  palf::PalfStat palf_stat_second;
  ObArray<common::ObAddr> member_list_new;
  int64_t paxos_replica_number_new = 0;

  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("inner stat error", KR(ret), K_(is_inited));
  } else if (!leader_readable_scn.is_valid_and_not_min()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(leader_readable_scn));
  } else if (OB_FAIL(get_latest_palf_stat_(palf_stat_first))) {
    LOG_WARN("get latest palf_stat failed", KR(ret), KPC_(ls));
  } else if (OB_FAIL(construct_new_member_list_(palf_stat_first.paxos_member_list_,
          palf_stat_first.degraded_list_,
          palf_stat_first.paxos_replica_num_,
          member_list_new,
          paxos_replica_number_new))) {
    LOG_WARN("construct_new_member_list failed", KR(ret), KPC_(ls), K(palf_stat_first));
  } else if (OB_FAIL(do_get_majority_readable_scn_(member_list_new,
          leader_readable_scn, rootserver::majority(paxos_replica_number_new), majority_min_readable_scn))) {
    LOG_WARN("do_get_majority_readable_scn_ failed", KR(ret), K(member_list_new), K(leader_readable_scn),
        K(paxos_replica_number_new), K(palf_stat_first), K(majority_min_readable_scn));
  } else if (OB_FAIL(get_latest_palf_stat_(palf_stat_second))) {
    LOG_WARN("get latest palf_stat failed", KR(ret), KPC_(ls));
  } else if (palf_stat_first.config_version_ != palf_stat_second.config_version_) {
    ret = OB_EAGAIN;
    LOG_WARN("config_version changed, try again", KR(ret), K(palf_stat_first), K(palf_stat_second));
  }

  return ret;
}

int ObLSRecoveryStatHandler::do_get_majority_readable_scn_(
    const ObIArray<common::ObAddr> &ob_member_list,
    const share::SCN &leader_readable_scn,
    const int64_t majority_cnt,
    share::SCN &majority_min_readable_scn)
{
  int ret = OB_SUCCESS;
  majority_min_readable_scn = SCN::min_scn();
  const common::ObAddr self_addr = GCTX.self_addr();
  ObTimeoutCtx ctx;
  ObSEArray<ObAddr, 3> inactive_members;
  obrpc::ObGetLSReplayedScnArg arg;
  const int64_t need_query_member_cnt = majority_cnt - 1;

  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("inner stat error", KR(ret), K_(is_inited));
  } else if (!is_user_tenant(tenant_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument. only support for user tenant", KR(ret), K_(tenant_id));
  } else if (OB_ISNULL(GCTX.server_tracer_) || OB_ISNULL(GCTX.srv_rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pointer is null", KR(ret), KP(GCTX.server_tracer_), KP(GCTX.srv_rpc_proxy_));
  } else if (!leader_readable_scn.is_valid_and_not_min()
      || ob_member_list.count() <= 0
      || !self_addr.is_valid()
      || 0 >= majority_cnt
      || 0 > need_query_member_cnt) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(leader_readable_scn), K(self_addr),
        K(majority_cnt), K(need_query_member_cnt), K(ob_member_list));
  } else if (0 == need_query_member_cnt) {
    ret = OB_SUCCESS;
    majority_min_readable_scn = leader_readable_scn;
    LOG_INFO("single replica, majority_min_readable_scn = leader_readable_scn", KR(ret),
        K(ob_member_list), K(leader_readable_scn));
  } else if (OB_FAIL(arg.init(tenant_id_, ls_->get_ls_id()))) {
    LOG_WARN("failed to init arg", KR(ret), K_(tenant_id), KPC_(ls));
  } else {
    int tmp_ret = OB_SUCCESS;
    ObGetLSReplayedScnProxy proxy(
        *GCTX.srv_rpc_proxy_, &obrpc::ObSrvRpcProxy::get_ls_replayed_scn);
    int64_t rpc_count = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < ob_member_list.count(); ++i) {
      const auto member = ob_member_list.at(i);

      bool alive = true;
      int64_t trace_time = 0;
      if (OB_FAIL(rootserver::ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
        LOG_WARN("fail to get timeout ctx", KR(ret), K(ctx));
      } else if (OB_UNLIKELY(!member.is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("member is invalid", KR(ret), K(member));
      } else if (self_addr == member) {
        //skip myself
      } else if (OB_FAIL(GCTX.server_tracer_->is_alive(member, alive, trace_time))) {
        LOG_WARN("check server alive failed", KR(ret), K(member));
      } else if (!alive) {
        //not send to alive
        if (OB_FAIL(inactive_members.push_back(member))) {
          LOG_WARN("fail to push back inactive_members", KR(ret), K(member), K(inactive_members));
        }
        // use meta rpc process thread
      } else if (OB_TMP_FAIL(proxy.call(member, ctx.get_timeout(), gen_meta_tenant_id(tenant_id_), arg))) {
        LOG_WARN("failed to send rpc", KR(tmp_ret), K(member), K(i), K(ctx), K_(tenant_id), K(arg), K(ob_member_list));
      } else {
        rpc_count++;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (need_query_member_cnt > rpc_count) {
      // If the number of alive servers is not enough for a majority, send to majority servers
      for (int64_t i = 0; OB_SUCC(ret) && i < inactive_members.count(); ++i) {
        if (OB_FAIL(rootserver::ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
          LOG_WARN("fail to get timeout ctx", KR(ret), K(ctx));
        } else if (OB_TMP_FAIL(proxy.call(inactive_members.at(i), ctx.get_timeout(), tenant_id_, arg))) {
          LOG_WARN("failed to send rpc", KR(tmp_ret), K(i), K(ctx), K_(tenant_id), K(arg), K(inactive_members));
        } else {
          rpc_count++;
        }
      }
    }

    //get result
    ObArray<int> return_code_array;
    if (OB_SUCCESS != (tmp_ret = proxy.wait_all(return_code_array))) {
      LOG_WARN("wait all batch result failed", KR(ret), KR(tmp_ret));
      ret = OB_SUCCESS == ret ? tmp_ret : ret;
    } else if (OB_FAIL(ret)) {
    } else if (OB_FAIL(calc_majority_min_readable_scn_(
            leader_readable_scn,
            majority_cnt,
            return_code_array,
            proxy,
            rpc_count,
            majority_min_readable_scn))) {
      LOG_WARN("failed to calc_majority_min_readable_scn", KR(ret), K(leader_readable_scn),
          K(ob_member_list), K(return_code_array), K(rpc_count));
    }
  }

  return ret;
}

int ObLSRecoveryStatHandler::calc_majority_min_readable_scn_(
    const SCN &leader_readable_scn,
    const int64_t majority_cnt,
    const ObIArray<int> &return_code_array,
    const ObGetLSReplayedScnProxy &proxy,
    const int64_t rpc_count,
    SCN &majority_min_readable_scn)
{
  int ret = OB_SUCCESS;
  ObArray<SCN> readable_scn_list;
  majority_min_readable_scn = SCN::max_scn();
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("inner stat error", KR(ret), K_(is_inited));
  } else if (!leader_readable_scn.is_valid_and_not_min() || 0 >= majority_cnt || 0 >= rpc_count) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(leader_readable_scn), K(majority_cnt), K(rpc_count));
  } else if (rpc_count != return_code_array.count() ||
      rpc_count != proxy.get_results().count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("rpc count not equal to result count", KR(ret),
        K(rpc_count), K(return_code_array), "arg count",
        proxy.get_args().count(), K(proxy.get_results().count()));
  } else if (OB_FAIL(readable_scn_list.push_back(leader_readable_scn))) {
    LOG_WARN("failed to push back", KR(ret), K(leader_readable_scn), K(readable_scn_list));
  } else if (OB_FAIL(ret)) {
  } else {
    ObGetLSReplayedScnRes res;
    int tmp_ret = OB_SUCCESS;

    for (int64_t i = 0; OB_SUCC(ret) && i < return_code_array.count(); ++i) {
      tmp_ret = return_code_array.at(i);
      // skip error server
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("send rpc is failed", KR(tmp_ret), K(i), K(return_code_array));
      } else {
        const auto *result = proxy.get_results().at(i);
        if (OB_ISNULL(result)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("result is null", KR(ret), K(i), K(return_code_array));
        } else if (!result->get_cur_readable_scn().is_valid_and_not_min()) {
          LOG_WARN("not valid scn", KR(ret), K(i), KPC(result));
          // skip this server
        } else if (OB_FAIL(readable_scn_list.push_back(result->get_cur_readable_scn()))) {
          LOG_WARN("failed to push back", KR(ret), K(i), KPC(result), K(readable_scn_list));
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (readable_scn_list.count() < majority_cnt) {
      ret = OB_EAGAIN;
      LOG_WARN("can not get majority readable_scn count", KR(ret), K(majority_cnt), K(readable_scn_list), K(return_code_array));
    } else {
      (void)std::sort(readable_scn_list.begin(), readable_scn_list.end(), std::greater<share::SCN>());
      for (int64_t i = 0; OB_SUCC(ret) && i < readable_scn_list.count() && i < majority_cnt; ++i) {
        if (majority_min_readable_scn > readable_scn_list.at(i)) {
          majority_min_readable_scn = readable_scn_list.at(i);
        }
      }
      LOG_TRACE("calculate majority min readable_scn finished", KR(ret), K(leader_readable_scn),
          K(majority_min_readable_scn), K(readable_scn_list), K(majority_cnt), K(return_code_array));
    }
  }
  return ret;
}

}
}
