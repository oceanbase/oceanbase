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

#include "ob_ls_recovery_stat_handler.h"
#include "logservice/ob_log_service.h" // ObLogService
#include "rootserver/ob_tenant_info_loader.h" // ObTenantInfoLoader
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

int ObLSRecoveryGuard::init(const uint64_t tenant_id, const share::ObLSID &ls_id,
      const int64_t &timeout)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(ls_recovery_stat_) || is_valid_tenant_id(tenant_id_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("already init", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))
             || !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(ls_id));
  } else if (skip_check_member_list_change_(tenant_id)) {
    //meta and sys no transfer, no need report and wait readable_scn
    //primary tenant no need check member list change
    ls_recovery_stat_ = NULL;
    tenant_id_ = tenant_id;
  } else {
    MTL_SWITCH(tenant_id) {
      ObLSService *ls_svr = MTL(ObLSService *);
      storage::ObLS *ls = NULL;

      if (OB_ISNULL(ls_svr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ls service is null", KR(ret));
      } else if (OB_FAIL(ls_svr->get_ls(ls_id, ls_handle_, storage::ObLSGetMod::RS_MOD))) {
        LOG_WARN("failed to get ls", KR(ret), K(ls_id));
      } else if (OB_ISNULL(ls = ls_handle_.get_ls())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("ls is NULL", KR(ret), K(ls_handle_));
      } else {
        ObLSRecoveryStatHandler* ls_recovery_stat = ls->get_ls_recovery_stat_handler();
        if (OB_ISNULL(ls_recovery_stat)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to get ls recovery stat", KR(ret), K(tenant_id),
              "ls_id", ls->get_ls_id());
        } else if (OB_FAIL(ls_recovery_stat->inc_ref(timeout))) {
          LOG_WARN("failed to inc ref", KR(ret), K(timeout), K(tenant_id));
        } else {
          ls_recovery_stat_ = ls_recovery_stat;
          tenant_id_ = tenant_id;
        }
      }
    }
  }
  return ret;
}

ObLSRecoveryGuard::~ObLSRecoveryGuard()
{
  if (OB_ISNULL(ls_recovery_stat_)) {
    //not init, nothing todo
  } else {
    ls_recovery_stat_->reset_add_replica_server();
    ls_recovery_stat_->dec_ref();
    LOG_TRACE("release ls recovery stat guard", K(tenant_id_), KPC(ls_recovery_stat_));
    ls_recovery_stat_ = NULL;
  }
}

bool ObLSRecoveryGuard::skip_check_member_list_change_(const uint64_t tenant_id)
{
  bool bret = false;
  if (!is_user_tenant(tenant_id)) {
    bret = true;
    LOG_INFO("not user tenant, no need to check member list change", K(tenant_id));
  } else {
    int ret = OB_SUCCESS;//use to MTL_SWITCH
    MTL_SWITCH(tenant_id) {
      if (MTL_TENANT_ROLE_CACHE_IS_PRIMARY()) {
        bret = true;
        LOG_INFO("is primary tenant, no need check readable_scn");
      }
    }
  }
  return bret;
}

int ObLSRecoveryGuard::check_can_add_member(const ObAddr &server, const int64_t timeout)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!server.is_valid() || 0 >= timeout)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(server), K(timeout));
  } else if (skip_check_member_list_change_(tenant_id_)) {
    //if not user tenant, no need to check and add member
  } else if (OB_ISNULL(ls_recovery_stat_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls recovery stat is null, not init", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(ls_recovery_stat_->set_add_replica_server(server))) {
    LOG_WARN("failed to set add replica server", KR(ret), K(server));
  } else if (OB_FAIL(ls_recovery_stat_->wait_server_readable_scn(server, timeout))) {
    LOG_WARN("failed to wait readable scn", KR(ret), K(server));
  }
  return ret;
}

int ObLSRecoveryGuard::check_can_change_member(const ObMemberList &new_member_list,
      const int64_t paxos_replica_num, const int64_t timeout)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!new_member_list.is_valid() || 0 >= paxos_replica_num || 0 >= timeout)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(new_member_list), K(paxos_replica_num), K(timeout));
  } else if (skip_check_member_list_change_(tenant_id_)) {
    //if not user tenant, no need to check and add member
  } else if (OB_ISNULL(ls_recovery_stat_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls recovery stat is null, not init", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(ls_recovery_stat_->wait_can_change_member_list(new_member_list,
                     paxos_replica_num, timeout))) {
    LOG_WARN("failed to check can change member", KR(ret), K(new_member_list), K(paxos_replica_num), K(timeout));
  }
  return ret;
}

int ObLSRecoveryStatHandler::init(const uint64_t tenant_id, ObLS *ls)
{
  int ret = OB_SUCCESS;
  reset(true);
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLSRecoveryStatHandler init twice", KR(ret), K_(is_inited));
  } else if (OB_ISNULL(ls) || !is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(ls), K(tenant_id));
  } else {
    ls_ = ls;
    tenant_id_ = tenant_id;
    SpinWLockGuard guard(lock_);
    last_dump_ts_ = ObTimeUtility::current_time();
    is_inited_ = true;
    LOG_INFO("ObLSRecoveryStatHandler init success", K(this));
  }
  return ret;
}

void ObLSRecoveryStatHandler::reset(const bool is_init)
{
  is_inited_ = false;
  ls_ = NULL;
  tenant_id_ = OB_INVALID_TENANT_ID;
  SpinWLockGuard guard(lock_);
  readable_scn_upper_limit_.reset();
  config_version_in_inner_.reset();
  extra_server_.reset();
  config_version_.reset();
  replicas_scn_.reset();
  last_dump_ts_ = OB_INVALID_TIMESTAMP;
  if (is_init) {
    ATOMIC_SET(&ref_cnt_, 0);
  } else if (0 != ATOMIC_LOAD(&ref_cnt_)) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "has reference", K(ref_cnt_));
    ref_cond_.signal();
  }
}

void ObLSRecoveryStatHandler::dec_ref()
{
  ATOMIC_CAS(&ref_cnt_, 1, 0);
  ref_cond_.signal();
}

int ObLSRecoveryStatHandler::inc_ref(const int64_t timeout)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check inner stat error", KR(ret));
  } else {
    int64_t curr_timeout = timeout;
    const int64_t TIME_WAIT = 100 * 1000;
    int tmp_ret = OB_SUCCESS;
    ret = OB_EAGAIN;
    do {
      if (0 == ATOMIC_CAS(&ref_cnt_, 0, 1)) {
        ret = OB_SUCCESS;
      } else if (curr_timeout > 0) {
        LOG_INFO("wait for inc ref", K(curr_timeout), K(timeout));
        if (OB_TMP_FAIL(ref_cond_.timedwait(TIME_WAIT))) {
          LOG_WARN("failed to timedwait", KR(ret), KR(tmp_ret));
        }
        curr_timeout -= TIME_WAIT;
      }
    } while (curr_timeout > 0 && OB_EAGAIN == ret);
  }
  return ret;
}

int ObLSRecoveryStatHandler::set_inner_readable_scn(const palf::LogConfigVersion &config_version,
      const share::SCN &readable_scn, bool check_inner_config_valid)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check inner stat error", KR(ret));
  } else if (OB_UNLIKELY(!config_version.is_valid() || !readable_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(config_version), K(readable_scn));
  } else {
    SpinWLockGuard guard(lock_);
    if (check_inner_config_valid && (!config_version_in_inner_.is_valid()
          || config_version_in_inner_ != config_version)) {
      //如果需要校验config_version，只能校验config_version相等
      //不能无脑推高config_version_in_inner_,这个值要严格和内部表保持一致
      //如果本地统计的config_version大于config_version_in_inner_
      //readable_scn_upper_limit_也没必要推高，总是要先把config_version更新成功后，才会更新内部表成功
      ret = OB_NEED_RETRY;
      LOG_WARN("config version in inner is invalid, can not update upper limit readable_scn",
          KR(ret), K(check_inner_config_valid), K(config_version_in_inner_));
    } else if (config_version_in_inner_.is_valid()
        && config_version_in_inner_ > config_version) {
      //内存中的config_version不会回退，但是内存中存储的readable_scn_upper_limit可能会回退，
      //可能会由于统计不到某些副本导致可读点回退，但是内存不回退，但是也不报错
      ret = OB_NEED_RETRY;
      LOG_WARN("config version not match or readable_scn fallback", KR(ret), K(config_version),
          K(config_version_in_inner_), K(readable_scn_upper_limit_), K(readable_scn));
    } else {
      config_version_in_inner_ = config_version;
      const int64_t PRINT_INTERVAL = 1 * 1000 * 1000;
      if (readable_scn_upper_limit_ > readable_scn && REACH_THREAD_TIME_INTERVAL(PRINT_INTERVAL)) {
        const ObLSID ls_id = ls_->get_ls_id();
        LOG_INFO("readable scn fallback", K(ls_id), K(readable_scn_upper_limit_), K(readable_scn));
      }
      readable_scn_upper_limit_ = SCN::max(readable_scn_upper_limit_, readable_scn);
    }
  }
  return ret;
}
int ObLSRecoveryStatHandler::reset_inner_readable_scn()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check inner stat error", KR(ret));
  } else {
    SpinWLockGuard guard(lock_);
    config_version_in_inner_.reset();
    readable_scn_upper_limit_.reset();
  }
  return ret;
}

int ObLSRecoveryStatHandler::wait_can_change_member_list(
    const ObMemberList &new_member_list, const int64_t paxos_replica_num,
    const int64_t timeout)
{
  int ret = OB_SUCCESS;
  const int64_t now = ObTimeUtility::current_time();
  if (OB_UNLIKELY(!new_member_list.is_valid() || 0 >= paxos_replica_num || 0 >= timeout)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(new_member_list), K(paxos_replica_num), K(timeout));
  } else if (OB_FAIL(wait_func_with_timeout_(timeout, new_member_list, paxos_replica_num))) {
    LOG_WARN("failed to wait func with timeout", KR(ret), K(new_member_list), K(paxos_replica_num), K(timeout));
  }
  LOG_INFO("finish wait for change member_list", KR(ret), K(new_member_list), K(paxos_replica_num),
      K(timeout), "cost",  ObTimeUtility::current_time() - now);
  return ret;
}

int ObLSRecoveryStatHandler::check_inner_stat_() {
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
  int64_t first_proposal_id = 0;
  ObRole first_role;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("inner stat error", KR(ret), K_(is_inited));
  } else if (OB_FAIL(get_latest_palf_stat_(palf_stat_first))) {
    LOG_WARN("get latest palf_stat failed", KR(ret), KPC_(ls));
  } else if (!is_strong_leader(palf_stat_first.role_)) {
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
    LOG_INFO("all ls readable scn", KR(ret), K(ls_id), K(readable_scn), K(replicas_scn_),
        "member_list", paxos_member_list, "replicas config_version", config_version_,
        "current_config_version", palf_stat_first.config_version_);
  }
  if (FAILEDx(get_latest_palf_stat_(palf_stat_second))) {
    LOG_WARN("get latest palf_stat failed", KR(ret), KPC_(ls));
  } else if (palf_stat_first.config_version_ != palf_stat_second.config_version_) {
    ret = OB_NEED_RETRY;
    LOG_WARN("config_version changed, try again", KR(ret), K(palf_stat_first), K(palf_stat_second));
  }
  return ret;
}

int ObLSRecoveryStatHandler::wait_server_readable_scn(
    const common::ObAddr &server, const int64_t timeout)
{
  int ret = OB_SUCCESS;
  int64_t now = ObTimeUtility::current_time();
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_UNLIKELY(!server.is_valid() || 0 >= timeout)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(server), K(timeout));
  } else {
    if (OB_FAIL(wait_func_with_timeout_(timeout, server))) {
      LOG_WARN("failed wait func with timeout", KR(ret), K(server), K(timeout));
    }
  }
  LOG_INFO("finish wait for add member", KR(ret), K(server),
      K(timeout), "cost",  ObTimeUtility::current_time() - now);

  return ret;
}

int ObLSRecoveryStatHandler::check_member_change_valid_(
    const common::ObAddr &server, bool &is_valid)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_UNLIKELY(!server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(server));
  } else {
    SCN readable_scn;
    is_valid = false;
    const int64_t TIME_WAIT = 100 * 1000;
    bool found = false;
    SpinRLockGuard guard(lock_);
    for (int64_t i = 0; i < replicas_scn_.count() && OB_SUCC(ret) && !found;
         ++i) {
      if (replicas_scn_.at(i).get_server() == server) {
        found = true;
        readable_scn = replicas_scn_.at(i).get_readable_scn();
        if (readable_scn >= readable_scn_upper_limit_) {
          is_valid = true;
          LOG_INFO("can change member list", K(server), K(readable_scn),
              K(readable_scn_upper_limit_), "ls_id", ls_->get_ls_id());
        } else if (REACH_THREAD_TIME_INTERVAL(TIME_WAIT)) {
          LOG_INFO("server readable scn is not larger enough", K(server),
                   K(readable_scn), K(readable_scn_upper_limit_), "ls_id", ls_->get_ls_id());
        }
      }
    }  // end for
    if (OB_SUCC(ret) && !found && REACH_THREAD_TIME_INTERVAL(TIME_WAIT)) {
      is_valid = false;
      LOG_INFO("cannot find server readable scn", K(server), K(replicas_scn_),
          K(readable_scn_upper_limit_), "ls_id", ls_->get_ls_id());
    }
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
    ret = OB_NEED_RETRY;
    LOG_INFO("role changed, try again", KR(ret), K(palf_stat_first), K(palf_stat_second));
  }

  return ret;
}

int ObLSRecoveryStatHandler::set_add_replica_server(
    const common::ObAddr &server)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("inner stat error", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("server is invalid", KR(ret), K(server));
  } else {
    SpinWLockGuard guard(lock_);
    extra_server_ = server;
  }

  return ret;
}


void ObLSRecoveryStatHandler::reset_add_replica_server()
{
  SpinWLockGuard guard(lock_);
  extra_server_.reset();
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

int ObLSRecoveryStatHandler::try_reload_and_fix_config_version_(
    const palf::LogConfigVersion &current_version)
{
  int ret = OB_SUCCESS;
  bool need_update = false;
  share::SCN readable_scn;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_UNLIKELY(!current_version.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("config_version is invalid", KR(ret), K(current_version));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else {
    SpinRLockGuard guard(lock_);
    if (config_version_in_inner_.is_valid() && config_version_in_inner_ > current_version) {
      ret = OB_NEED_RETRY;
      LOG_WARN("config version is fallback", KR(ret), K(current_version), K(config_version_in_inner_));
    } else if (current_version == config_version_in_inner_) {
      need_update = false;
      LOG_DEBUG("config version not change", KR(ret), K(current_version));
    } else {
      need_update = true;
      FLOG_INFO("config version not match, need update",
                K(config_version_in_inner_), K(current_version), "ls_id",
                ls_->get_ls_id());
    }
  }
  if (OB_SUCC(ret) && need_update) {
    ObLSRecoveryStatOperator op;
    ObLSID ls_id = ls_->get_ls_id();
    int tmp_ret = OB_SUCCESS;
    if (OB_FAIL(op.update_ls_config_version(tenant_id_, ls_id, current_version,
    *GCTX.sql_proxy_, readable_scn))) {
      LOG_WARN("failed to update ls config version", KR(ret), K(tenant_id_), K(ls_id), K(current_version));
      if (OB_TMP_FAIL(reset_inner_readable_scn())) {
        LOG_ERROR("failed to reset config version", KR(ret), KR(tmp_ret));
      }
    } else if (OB_FAIL(set_inner_readable_scn(current_version, readable_scn, false))) {
      LOG_ERROR("failed to set readable_scn", KR(ret), K(current_version), K(readable_scn));
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

int ObLSRecoveryStatHandler::check_can_use_new_version_(bool &is_valid_use)
{
  int ret = OB_SUCCESS;
  is_valid_use = false;
  const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id_);
  uint64_t tenant_data_version = 0;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(meta_tenant_id, tenant_data_version))) {
    LOG_WARN("failed to get min data version", KR(ret), K(tenant_id_), K(meta_tenant_id));
  } else if (tenant_data_version < MOCK_DATA_VERSION_4_2_4_0) {
    //由于config_version在430版本已经存在了，所以兼容性判断为小于等于4240不支持
    is_valid_use = false;
    LOG_INFO("not ready to to use new version", KR(ret), K(tenant_data_version));
  } else {
    is_valid_use = true;
  }
  return ret;
}

int ObLSRecoveryStatHandler::construct_addr_list_(
    const palf::PalfStat &palf_stat,
    ObIArray<common::ObAddr> &addr_list)
{
  int ret = OB_SUCCESS;
  addr_list.reset();
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("inner stat error", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!palf_stat.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("palf stat is invalid", KR(ret), K(palf_stat));
  } else {
    common::ObMember member;
    const common::ObMemberList &member_list = palf_stat.paxos_member_list_;
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
  return ret;
}

int ObLSRecoveryStatHandler::dump_all_replica_readable_scn_(const bool force_dump)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("inner stat error", KR(ret), K_(is_inited));
  } else {
    ObLSID ls_id = ls_->get_ls_id();
    SpinRLockGuard guard(lock_);
    const int64_t PRINT_INTERVAL = 5 * 1000 * 1000L;
    const int64_t now = ObTimeUtility::current_time();
    if (force_dump || now - last_dump_ts_ > PRINT_INTERVAL) {
      LOG_INFO("ls readable scn in memory", K(ls_id), K(replicas_scn_),
          K(last_dump_ts_), K(force_dump));
      last_dump_ts_ = now;
    } else {
      LOG_TRACE("ls readable scn in memory", KR(ret), K(ls_id),
          K(readable_scn_upper_limit_), K(replicas_scn_));
    }
  }
  return ret;
}

int ObLSRecoveryStatHandler::check_member_change_valid_(
    const ObMemberList &new_member_list, const int64_t paxos_replica_num, bool &is_valid)
{
  int ret = OB_SUCCESS;
  SCN readable_scn;
  palf::PalfStat palf_stat;
  ObArray<ObAddr> addr_list;
  is_valid = false;
  if (OB_UNLIKELY(!new_member_list.is_valid() || 0 >= paxos_replica_num)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(new_member_list), K(paxos_replica_num));
  } else if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("inner stat error", KR(ret), K_(is_inited));
  } else if (OB_FAIL(get_palf_stat_(palf_stat))) {
    LOG_WARN("failed to get palf stat", KR(ret));
  } else if (OB_FAIL(new_member_list.get_addr_array(addr_list))) {
    LOG_WARN("failed to get addr array", KR(ret), K(new_member_list));
  } else if (OB_FAIL(do_get_majority_readable_scn_V2_(addr_list,
     rootserver::majority(paxos_replica_num), palf_stat.config_version_, readable_scn))) {
    LOG_WARN("failed to get majority readable scn", KR(ret), K(addr_list),
    K(paxos_replica_num), K(palf_stat));
  } else {
    SpinRLockGuard guard(lock_);
    if (readable_scn >= readable_scn_upper_limit_) {
      is_valid = true;
      LOG_INFO("can change member list", K(new_member_list), K(paxos_replica_num),
         "new readable_scn", readable_scn, "current readable_scn", readable_scn_upper_limit_,
         "ls_id", ls_->get_ls_id(), K(palf_stat));
    } else if (REACH_THREAD_TIME_INTERVAL(1 * 1000 * 1000L)) {
      LOG_INFO("can not change member list",  K(new_member_list), K(paxos_replica_num),
          K(readable_scn), K(readable_scn_upper_limit_),
          "ls_id", ls_->get_ls_id(), K(palf_stat));
    }
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
  int tmp_ret = OB_SUCCESS;
  bool is_valid_use = false;

  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("inner stat error", KR(ret), K_(is_inited));
  } else if (OB_FAIL(get_latest_palf_stat_(palf_stat_first))) {
    LOG_WARN("get latest palf_stat failed", KR(ret), KPC_(ls));
  } else if (!is_strong_leader(palf_stat_first.role_)) {
    ret = OB_NOT_MASTER;
    LOG_TRACE("not leader", KR(ret), K(palf_stat_first));
  } else if (OB_FAIL(check_can_use_new_version_(is_valid_use))) {
    LOG_WARN("failed to check can use new version", KR(ret));
  } else if (!is_valid_use) {
    ret = OB_NEED_RETRY;
    LOG_WARN("not valid to use new version", KR(ret));
  } else if (OB_FAIL(construct_addr_list_(palf_stat_first, addr_list))) {
    LOG_WARN("failed to construct addr list", KR(ret), K(palf_stat_first));
  } else if (OB_FAIL(do_get_each_replica_readable_scn_(addr_list, replicas_scn))) {
    LOG_WARN("failed to get each replica readable", KR(ret), K(addr_list));
  } else if (OB_FAIL(get_palf_stat_(palf_stat_second))) {
    LOG_WARN("failed to get palf stat", KR(ret));
  } else if (palf_stat_second.config_version_ != palf_stat_first.config_version_) {
    ret = OB_NEED_RETRY;
    LOG_WARN("config version change", KR(ret), K(palf_stat_second), K(palf_stat_first));
  } else {
    SpinWLockGuard guard(lock_);
    config_version_ = palf_stat_second.config_version_;
    if (OB_FAIL(replicas_scn_.assign(replicas_scn))) {
      LOG_WARN("failed to replicas scn", KR(ret), K(replicas_scn));
    }
  }
  if (is_strong_leader(palf_stat_second.role_) && is_valid_use) {
    if (OB_TMP_FAIL(try_reload_and_fix_config_version_(palf_stat_second.config_version_))) {
      ret = OB_SUCC(ret) ? tmp_ret : ret;
      LOG_WARN("failed to try reload and fix config version",
          KR(tmp_ret), KR(ret), K(palf_stat_second));
    }
  }
  if (OB_NOT_MASTER != ret) {
    if (OB_TMP_FAIL(dump_all_replica_readable_scn_(OB_FAIL(ret)))) {
      LOG_WARN("failed to dump replica readable scn", KR(ret), KR(tmp_ret));
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
  } else if (OB_ISNULL(GCTX.srv_rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rpc proxy is null", KR(ret));
  } else {
    obrpc::ObGetLSReplayedScnArg arg;
    ObGetLSReplayedScnProxy proxy(
        *GCTX.srv_rpc_proxy_, &obrpc::ObSrvRpcProxy::get_ls_replayed_scn);
    ObTimeoutCtx ctx;
    int tmp_ret = OB_SUCCESS;
    ObArray<int> return_code_array;
    ObLSReplicaReadableSCN replica_scn;
    int group_id = share::OBCG_DBA_COMMAND;
    if (OB_FAIL(arg.init(tenant_id_, ls_->get_ls_id(), false))) {
      LOG_WARN("failed to init arg", KR(ret), K_(tenant_id), KPC_(ls));
    } else if (OB_FAIL(rootserver::ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
      LOG_WARN("fail to get timeout ctx", KR(ret), K(ctx));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < ob_member_list.count(); ++i) {
      const ObAddr &member = ob_member_list.at(i);
      if (OB_TMP_FAIL(proxy.call(member, ctx.get_timeout(),
              GCONF.cluster_id, tenant_id_, group_id, arg))) {
        LOG_WARN("failed to send rpc", KR(ret), KR(tmp_ret), K(member), K(i), K(ctx),
            K_(tenant_id), K(arg), K(ob_member_list), K(group_id));
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
  bool is_valid_to_use = false;

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
  } else if (OB_FAIL(check_can_use_new_version_(is_valid_to_use))) {
    LOG_WARN("failed to check can use new version", KR(ret));
  } else if (is_valid_to_use) {
    //use readable in memory
    if (OB_FAIL(do_get_majority_readable_scn_V2_(member_list_new,
    rootserver::majority(paxos_replica_number_new), palf_stat_first.config_version_,
    majority_min_readable_scn))) {
      LOG_WARN("failed to get majority readable scn", KR(ret), K(palf_stat_first),
          K(paxos_replica_number_new), K(member_list_new));
    } else if (OB_FAIL(set_inner_readable_scn(palf_stat_first.config_version_,
            majority_min_readable_scn, true))) {
      //尝试更新内存中的可读点
      LOG_WARN("failed to set inner readable scn", KR(ret), K(palf_stat_first),
        K(majority_min_readable_scn));
    }
  } else if (OB_FAIL(do_get_majority_readable_scn_(member_list_new,
          leader_readable_scn, rootserver::majority(paxos_replica_number_new), majority_min_readable_scn))) {
    LOG_WARN("do_get_majority_readable_scn_ failed", KR(ret), K(member_list_new), K(leader_readable_scn),
        K(paxos_replica_number_new), K(palf_stat_first), K(majority_min_readable_scn));
  }
  if (FAILEDx(get_latest_palf_stat_(palf_stat_second))) {
    LOG_WARN("get latest palf_stat failed", KR(ret), KPC_(ls));
  } else if (palf_stat_first.config_version_ != palf_stat_second.config_version_) {
    ret = OB_NEED_RETRY;
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
  } else if (ob_member_list.count() <= 0
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
      const ObAddr &server = replicas_scn_.at(i).get_server();
      const SCN &readable_scn = replicas_scn_.at(i).get_readable_scn();
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
        const ObGetLSReplayedScnRes *result = proxy.get_results().at(i);
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
