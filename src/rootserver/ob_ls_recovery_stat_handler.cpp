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

int ObLSReplicaReadableSCN::init(const common::ObAddr &server, const share::SCN &readable_scn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("server is invalid", KR(ret), K(server));
  } else {
    server_ = server;
    readable_scn_ = readable_scn;
  }
  return ret;
}
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
    replicas_scn_.set_tenant_id(tenant_id);
    replicas_scn_.set_label("LSReadableSCN");
    LOG_INFO("ObLSRecoveryStatHandler init success", K(this));
  }

  return ret;
}

void ObLSRecoveryStatHandler::reset()
{
  is_inited_ = false;
  ls_ = NULL;
  tenant_id_ = OB_INVALID_TENANT_ID;
  SpinWLockGuard guard(lock_);
  readable_scn_in_inner_.reset();
  config_version_in_inner_.reset();
  extra_server_.reset();
  config_version_.reset();
  replicas_scn_.reset();
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


int ObLSRecoveryStatHandler::get_all_replica_min_readable_scn(share::SCN &readable_scn)
{
  int ret = OB_SUCCESS;
  palf::PalfStat palf_stat_first;
  palf::PalfStat palf_stat_second;
  readable_scn = SCN::max_scn();
  logservice::ObLogService *ls_svr = MTL(logservice::ObLogService*);
  int64_t first_proposal_id = 0;
  ObRole first_role;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("inner stat error", KR(ret), K_(is_inited));
  } else if (OB_FAIL(get_latest_palf_stat_(palf_stat_first))) {
    LOG_WARN("get latest palf_stat failed", KR(ret), KPC_(ls));
  } else if (OB_FAIL(ls_svr->get_palf_role(ls_->get_ls_id(), first_role, first_proposal_id))) {
    LOG_WARN("failed to get first role", KR(ret), K(ls_->get_ls_id()), KP(ls_svr), KPC_(ls));
  } else if (!is_strong_leader(first_role)) {
    ret = OB_NOT_MASTER;
    LOG_WARN("not master, need retry", KR(ret), K(ls_->get_ls_id()));
  } else {
    ObMemberList &paxos_member_list = palf_stat_first.paxos_member_list_;
    SpinRLockGuard guard(lock_);
    if (palf_stat_first.config_version_ != config_version_) {
      ret = OB_NEED_RETRY;
      LOG_WARN("config version not match", KR(ret), K(config_version_), K(palf_stat_first));
    } else if (replicas_scn_.count() < paxos_member_list.get_member_number()) {
      ret = OB_NEED_RETRY;
      LOG_WARN("not enough replica", KR(ret), K(replicas_scn_), K(paxos_member_list));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < paxos_member_list.get_member_number(); ++i) {
      ObAddr member;
      int64_t index = 0;
      if (OB_FAIL(paxos_member_list.get_server_by_index(i, member))) {
        LOG_WARN("failed to get server by index", KR(ret), K(i));
      }
      for (; OB_SUCC(ret) && index < replicas_scn_.count(); ++index) {
        if (replicas_scn_.at(index).get_server() == member) {
          readable_scn = SCN::min(readable_scn, replicas_scn_.at(index).get_readable_scn());
          break;
        }
      }
      if (OB_SUCC(ret) && index >= replicas_scn_.count()) {
        ret = OB_NEED_RETRY;
        LOG_WARN("replica has no readable scn", KR(ret), K(member), K(replicas_scn_));
      }
    }
    //TODO maybe need consider readable scn in inner table
    ObLSID ls_id = ls_->get_ls_id();
    LOG_INFO("all ls readable scn", K(ls_id), K(readable_scn), K(replicas_scn_));
  }
  if (FAILEDx(get_latest_palf_stat_(palf_stat_second))) {
    LOG_WARN("get latest palf_stat failed", KR(ret), KPC_(ls));
  } else if (palf_stat_first.config_version_ != palf_stat_second.config_version_) {
    ret = OB_EAGAIN;
    LOG_WARN("config_version changed, try again", KR(ret), K(palf_stat_first), K(palf_stat_second));
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
  } else if (OB_FAIL(tenant_info_loader->get_global_replayable_scn(replayable_scn))) {
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
  ls_recovery_stat.reset();
  palf::PalfStat palf_stat_first;
  palf::PalfStat palf_stat_second;

  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("inner stat error", KR(ret), K_(is_inited));
  } else if (OB_FAIL(get_latest_palf_stat_(palf_stat_first))) {
    LOG_WARN("failed to get lastest palf stat", KR(ret));
  } else if (!is_strong_leader(palf_stat_first.role_)) {
    ret = OB_NOT_MASTER;
    LOG_TRACE("not leader", KR(ret), K(palf_stat_first));
  } else if (OB_FAIL(do_get_ls_level_readable_scn_(readable_scn))) {
    LOG_WARN("failed to do_get_ls_level_readable_scn_", KR(ret), KPC_(ls));
  // scn get order: read_scn before replayable_scn before sync_scn
   } else if (OB_FAIL(ObLSServiceHelper::get_ls_replica_sync_scn(MTL_ID(), ls_->get_ls_id(), sync_scn))) {
    LOG_WARN("failed to get ls sync scn", KR(ret), "tenant_id", MTL_ID());
  } else if (OB_FAIL(ls_recovery_stat.init_only_recovery_stat(tenant_id_, ls_->get_ls_id(),
                                                              sync_scn, readable_scn,
                                                              palf_stat_first.config_version_))) {
    LOG_WARN("failed to init ls recovery stat", KR(ret), K_(tenant_id), K(sync_scn), K(readable_scn),
    K(palf_stat_first), "ls_id", ls_->get_ls_id());
  } else if (OB_FAIL(get_latest_palf_stat_(palf_stat_second))) {
    LOG_WARN("failed to get lastest palf stat", KR(ret));
  } else if (palf_stat_first.config_version_ != palf_stat_second.config_version_
  || !is_strong_leader(palf_stat_second.role_)) {
    ret = OB_EAGAIN;
    LOG_INFO("role changed, try again", KR(ret), K(palf_stat_first), K(palf_stat_second));
  }

  return ret;
}

int ObLSRecoveryStatHandler::set_add_replica_server(
    const common::ObAddr &server)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("server is invalid", KR(ret), K(server));
  } else {
    SpinWLockGuard guard(lock_);
    extra_server_ = server;
  }

  return ret;
}

int ObLSRecoveryStatHandler::do_get_ls_level_readable_scn_(SCN &read_scn)
{
  int ret = OB_SUCCESS;

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

int ObLSRecoveryStatHandler::try_reload_and_fix_config_version_(const palf::LogConfigVersion &current_version)
{
  int ret = OB_SUCCESS;
  bool need_update = false;
  share::SCN readable_scn;
  const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id_);
  uint64_t tenant_data_version = 0;
  ObLSRecoveryStatOperator op;
  ObLSID ls_id;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_UNLIKELY(!current_version.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("config_version is invalid", KR(ret), K(current_version));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else if (FALSE_IT(ls_id = ls_->get_ls_id())) {
    //can not be there
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(meta_tenant_id, tenant_data_version))) {
    LOG_WARN("failed to get min data version", KR(ret), K(tenant_id_), K(meta_tenant_id));
  } else if (tenant_data_version < MOCK_DATA_VERSION_4_2_4_0) {
    //内部表config_version的汇报最开始是在4300版本上提交的
    //后面patch到424版本上，由于是在4300分支的第一个版本号提交
    //所以版本号判断直接小于等于424即可
    need_update = false;
    LOG_INFO("not ready to load and update config version", KR(ret), K(tenant_data_version));
  } else {
    SpinRLockGuard guard(lock_);
    if (current_version != config_version_in_inner_) {
      need_update = true;
      FLOG_INFO("config version not match, need update",
          K(config_version_in_inner_), K(current_version), K(ls_id));
    }
  }
  if (OB_SUCC(ret) && need_update) {
    if (OB_FAIL(op.update_ls_config_version(tenant_id_, ls_id, current_version,
    *GCTX.sql_proxy_, readable_scn))) {
      LOG_WARN("failed to update ls config version", KR(ret), K(tenant_id_), K(ls_id), K(current_version));
      //set invalid config version
      SpinWLockGuard guard(lock_);
      config_version_in_inner_.reset();
    } else {
      SpinWLockGuard guard(lock_);
      readable_scn_in_inner_ = readable_scn;
      config_version_in_inner_ = current_version;
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

int ObLSRecoveryStatHandler::gather_replica_readable_scn()
{
  int ret = OB_SUCCESS;
  ObArray<ObLSReplicaReadableSCN> replicas_scn;
  palf::PalfStat palf_stat_first;
  palf::PalfStat palf_stat_second;
  ObArray<common::ObAddr> addr_list;

  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("inner stat error", KR(ret), K_(is_inited));
  } else if (OB_FAIL(get_latest_palf_stat_(palf_stat_first))) {
    LOG_WARN("get latest palf_stat failed", KR(ret), KPC_(ls));
  } else if (!is_strong_leader(palf_stat_first.role_)) {
    ret = OB_NOT_MASTER;
    LOG_TRACE("not leader", KR(ret), K(palf_stat_first));
  } else if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_3_0_0) {
    ret = OB_NEED_WAIT;
    LOG_WARN("not ready to gather replica readable scn", KR(ret));
  } else {
    common::ObMember member;
    common::ObMemberList &member_list = palf_stat_first.paxos_member_list_;
    for (int64_t i = 0; OB_SUCC(ret) && i < member_list.get_member_number(); ++i) {
      if (OB_FAIL(member_list.get_member_by_index(i, member))) {
        LOG_WARN("failed to get member by index", KR(ret), K(i));
      } else if (OB_FAIL(addr_list.push_back(member.get_server()))) {
        LOG_WARN("failed to push back member", KR(ret), K(member));
      }
    }
    if (OB_SUCC(ret)) {
      SpinWLockGuard guard(lock_);
      if (extra_server_.is_valid() && OB_FAIL(addr_list.push_back(extra_server_))) {
        LOG_WARN("failed to push back member", KR(ret), K(extra_server_));
      }
    }
  }
  if (FAILEDx(do_get_each_replica_readable_scn_(addr_list, replicas_scn))) {
    LOG_WARN("failed to get each replica readable", KR(ret), K(addr_list));
  } else if (OB_FAIL(get_palf_stat_(palf_stat_second))) {
    LOG_WARN("failed to get palf stat", KR(ret));
  } else if (palf_stat_second.config_version_ != palf_stat_first.config_version_) {
    ret = OB_EAGAIN;
    LOG_WARN("config version change", KR(ret), K(palf_stat_second), K(palf_stat_first));
  } else {
    SpinWLockGuard guard(lock_);
    ObLSID ls_id = ls_->get_ls_id();
    config_version_ = palf_stat_second.config_version_;
    replicas_scn_.reset();
    if (OB_FAIL(replicas_scn_.assign(replicas_scn))) {
      LOG_WARN("failed to replicas scn", KR(ret), K(replicas_scn));
    }
    const int64_t PRINT_INTERVAL = 1 * 1000 * 1000L;
    if (REACH_TENANT_TIME_INTERVAL(PRINT_INTERVAL)) {
      LOG_INFO("ls readable scn in memory", KR(ret), K(ls_id), K(replicas_scn_));
    } else {
      LOG_TRACE("ls readable scn in memory", KR(ret), K(ls_id), K(replicas_scn_));
    }
  }
  if (is_strong_leader(palf_stat_second.role_)) {
    //优先把正确的config_version更新到内部表和内存中
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(try_reload_and_fix_config_version_(palf_stat_second.config_version_))) {
      ret = OB_SUCC(ret) ? tmp_ret : ret;
      LOG_WARN("failed to try reload and fix config version", KR(tmp_ret), KR(ret), K(palf_stat_second));
    }
  }
  return ret;
}

int ObLSRecoveryStatHandler::do_get_each_replica_readable_scn_(
    const ObIArray<common::ObAddr> &ob_member_list,
    ObArray<ObLSReplicaReadableSCN> &replicas_scn)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("inner stat error", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(0 >= ob_member_list.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ob_member_list));
  } else {
    obrpc::ObGetLSReplayedScnArg arg;
    ObGetLSReplayedScnProxy proxy(
        *GCTX.srv_rpc_proxy_, &obrpc::ObSrvRpcProxy::get_ls_replayed_scn);
    ObTimeoutCtx ctx;
    int tmp_ret = OB_SUCCESS;
    ObArray<int> return_code_array;
    ObLSReplicaReadableSCN replica_scn;
    if (OB_FAIL(arg.init(tenant_id_, ls_->get_ls_id(), false))) {
      LOG_WARN("failed to init arg", KR(ret), K_(tenant_id), KPC_(ls));
    } else if (OB_FAIL(rootserver::ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
      LOG_WARN("fail to get timeout ctx", KR(ret), K(ctx));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < ob_member_list.count(); ++i) {
      const ObAddr &member = ob_member_list.at(i);
      if (GCTX.self_addr() == member) {
        SCN self_readable_scn;
        if (OB_FAIL(ls_->get_max_decided_scn(self_readable_scn))) {
          LOG_WARN("failed to get max decide scn", KR(ret));
        } else if (OB_FAIL(replica_scn.init(member, self_readable_scn))) {
          LOG_WARN("failed to init replica scn", KR(ret), K(member), K(self_readable_scn));
        } else if (OB_FAIL(replicas_scn.push_back(replica_scn))) {
          LOG_WARN("failed to push back replica scn", KR(ret), K(replica_scn));
        }
      } else if (OB_TMP_FAIL(proxy.call(member, ctx.get_timeout(), tenant_id_, arg))) {
        LOG_WARN("failed to send rpc", KR(ret), K(member), K(i), K(ctx),
            K_(tenant_id), K(arg), K(ob_member_list));
      }
    }
    if (OB_TMP_FAIL(proxy.wait_all(return_code_array))) {
      LOG_WARN("wait all batch result failed", KR(ret), KR(tmp_ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    } else if (OB_SUCC(ret) && return_code_array.count() != proxy.get_results().count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("return code array not match with result", KR(ret), K(return_code_array),
          K(proxy.get_results()));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < return_code_array.count(); ++i) {
      if (OB_TMP_FAIL(return_code_array.at(i))) {
        LOG_WARN("send rpc is failed", KR(tmp_ret), K(i), K(return_code_array));
      } else {
        const ObGetLSReplayedScnRes *result = proxy.get_results().at(i);
        if (OB_ISNULL(result)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("result is null", KR(ret), K(i), K(return_code_array));
        } else if (OB_FAIL(replica_scn.init(result->get_server(), result->get_cur_readable_scn()))) {
          LOG_WARN("failed to init replica scn", KR(ret), KPC(result));
        } else if (OB_FAIL(replicas_scn.push_back(replica_scn))) {
          LOG_WARN("failed to push back", KR(ret), K(replica_scn));
        }
      }
    }
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
  } else if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_3_0_0) {
    //use readable in memory
    if (OB_FAIL(do_get_majority_readable_scn_V2_(member_list_new,
    rootserver::majority(paxos_replica_number_new), palf_stat_first.config_version_,
    majority_min_readable_scn))) {
      LOG_WARN("failed to get majority readable scn", KR(ret), K(palf_stat_first),
          K(paxos_replica_number_new), K(member_list_new));
    }
  } else if (OB_FAIL(do_get_majority_readable_scn_(member_list_new,
          leader_readable_scn, rootserver::majority(paxos_replica_number_new), majority_min_readable_scn))) {
    LOG_WARN("do_get_majority_readable_scn_ failed", KR(ret), K(member_list_new), K(leader_readable_scn),
        K(paxos_replica_number_new), K(palf_stat_first), K(majority_min_readable_scn));
  }
  if (FAILEDx(get_latest_palf_stat_(palf_stat_second))) {
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
  } else if (OB_FAIL(arg.init(tenant_id_, ls_->get_ls_id(), false))) {
    LOG_WARN("failed to init arg", KR(ret), K_(tenant_id), KPC_(ls));
  } else {
    int tmp_ret = OB_SUCCESS;
    ObGetLSReplayedScnProxy proxy(
        *GCTX.srv_rpc_proxy_, &obrpc::ObSrvRpcProxy::get_ls_replayed_scn);
    int64_t rpc_count = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < ob_member_list.count(); ++i) {
      const common::ObAddr &member = ob_member_list.at(i);

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
    if (OB_TMP_FAIL(proxy.wait_all(return_code_array))) {
      LOG_WARN("wait all batch result failed", KR(ret), KR(tmp_ret));
      ret = OB_SUCCESS == ret ? tmp_ret : ret;
    } else if (OB_FAIL(ret)) {
    } else if (OB_FAIL(calc_majority_min_readable_scn_(
            leader_readable_scn,
            majority_cnt,
            return_code_array,
            proxy,
            majority_min_readable_scn))) {
      LOG_WARN("failed to calc_majority_min_readable_scn", KR(ret), K(leader_readable_scn),
          K(ob_member_list), K(return_code_array));
    }
  }

  return ret;
}
int ObLSRecoveryStatHandler::do_get_majority_readable_scn_V2_(
    const ObIArray<common::ObAddr> &ob_member_list,
    const int64_t need_query_member_cnt,
    const palf::LogConfigVersion &config_version,
    share::SCN &majority_min_readable_scn)
{
  int ret = OB_SUCCESS;
  majority_min_readable_scn = SCN::min_scn();
  ObArray<SCN> replica_readble_scn;

  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("inner stat error", KR(ret), K_(is_inited));
  } else if ( ob_member_list.count() <= 0
      || 0 >= need_query_member_cnt
      || !config_version.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(need_query_member_cnt),
    K(ob_member_list), K(config_version));
  } else {
    SpinRLockGuard guard(lock_);
    if (config_version_ != config_version) {
      ret = OB_NEED_RETRY;
      LOG_WARN("config version not match, need retry", KR(ret), K(config_version),
      K(config_version_));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < replicas_scn_.count(); ++i) {
      ObAddr &server = replicas_scn_.at(i).get_server();
      SCN readable_scn= replicas_scn_.at(i).get_readable_scn();
      if (has_exist_in_array(ob_member_list, server)) {
        if (OB_FAIL(replica_readble_scn.push_back(readable_scn))) {
          LOG_WARN("failed to push back", KR(ret), K(i), K(server), K(readable_scn));
        }
      }
    }
  }
  if (FAILEDx(do_calc_majority_min_readable_scn_(need_query_member_cnt,
  replica_readble_scn, majority_min_readable_scn))) {
    LOG_WARN("failed to calc majority readable scn", KR(ret),
    K(need_query_member_cnt), K(replica_readble_scn));
  }
  return ret;
}

int ObLSRecoveryStatHandler::calc_majority_min_readable_scn_(
    const SCN &leader_readable_scn,
    const int64_t majority_cnt,
    const ObIArray<int> &return_code_array,
    const ObGetLSReplayedScnProxy &proxy,
    SCN &majority_min_readable_scn)
{
  int ret = OB_SUCCESS;
  majority_min_readable_scn = SCN::max_scn();
  ObArray<SCN> readable_scn_list;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("inner stat error", KR(ret), K_(is_inited));
  } else if (!leader_readable_scn.is_valid_and_not_min() || 0 >= majority_cnt) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(leader_readable_scn), K(majority_cnt));
  } else if (return_code_array.count() != proxy.get_results().count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("rpc count not equal to result count", KR(ret),
             K(return_code_array), K(proxy.get_results().count()));
  } else if (OB_FAIL(readable_scn_list.push_back(leader_readable_scn))) {
    LOG_WARN("failed to push back", KR(ret), K(leader_readable_scn), K(readable_scn_list));
  } else {
    ObGetLSReplayedScnRes res;
    int tmp_ret = OB_SUCCESS;
    // don't use arg/dest here because call() may has failure.
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
    if (FAILEDx(do_calc_majority_min_readable_scn_(majority_cnt,
    readable_scn_list, majority_min_readable_scn))) {
      LOG_WARN("do calc majority min readable scn", KR(ret), K(majority_cnt), K(readable_scn_list));
    }
  }
  return ret;
}
  int ObLSRecoveryStatHandler::do_calc_majority_min_readable_scn_(
    const int64_t majority_cnt,
    ObArray<SCN> &readable_scn_list,
    share::SCN &majority_min_readable_scn)
{
  int ret = OB_SUCCESS;
  majority_min_readable_scn.set_max();
  if (OB_UNLIKELY(0 >= majority_cnt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(majority_cnt));
  } else if (readable_scn_list.count() < majority_cnt) {
    ret = OB_EAGAIN;
    LOG_WARN("can not get majority readable_scn count", KR(ret),
             K(majority_cnt), K(readable_scn_list));
  } else {
    (void)lib::ob_sort(readable_scn_list.begin(), readable_scn_list.end(),
                    std::greater<share::SCN>());
    for (int64_t i = 0;
         OB_SUCC(ret) && i < readable_scn_list.count() && i < majority_cnt;
         ++i) {
      if (majority_min_readable_scn > readable_scn_list.at(i)) {
        majority_min_readable_scn = readable_scn_list.at(i);
      }
    }
    LOG_TRACE("calculate majority min readable_scn finished", KR(ret),
               K(majority_min_readable_scn),
              K(readable_scn_list), K(majority_cnt));
  }
  return ret;
}
}
}
