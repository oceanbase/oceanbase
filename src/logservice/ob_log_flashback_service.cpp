// Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#define USING_LOG_PREFIX CLOG
#include "ob_log_flashback_service.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/scn.h"

namespace oceanbase
{

namespace logservice
{

ObLogFlashbackService::ObLogFlashbackService() :
    is_inited_(false),
    lock_(),
    self_(),
    location_adapter_(nullptr),
    rpc_proxy_(nullptr),
    sql_proxy_(nullptr)
  {}

ObLogFlashbackService::~ObLogFlashbackService()
{
  destroy();
}

int ObLogFlashbackService::init(const common::ObAddr &self,
                                logservice::ObLocationAdapter *location_adapter,
                                obrpc::ObLogServiceRpcProxy *rpc_proxy,
                                common::ObMySQLProxy *sql_proxy)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(WARN, "init twice", K(ret), K(self));
  } else if (false == self.is_valid() || OB_ISNULL(rpc_proxy) ||
      OB_ISNULL(sql_proxy) || OB_ISNULL(location_adapter)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(self), KP(rpc_proxy));
  } else {
    self_ = self;
    flashback_op_array_.reset();
    location_adapter_ = location_adapter;
    rpc_proxy_ = rpc_proxy;
    sql_proxy_ = sql_proxy;
    is_inited_ = true;
  }
  return ret;
}

void ObLogFlashbackService::destroy()
{
  is_inited_ = false;
  self_.reset();
  flashback_op_array_.reset();
  location_adapter_ = nullptr;
  rpc_proxy_ = nullptr;
  sql_proxy_ = nullptr;
}

int ObLogFlashbackService::flashback(const uint64_t tenant_id, const SCN &flashback_scn, const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  #define COMMON_LOG_INFO K(ret), K_(self), K(tenant_id), K(flashback_scn), K(timeout_us)
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_INVALID_TENANT_ID == tenant_id ||
             !flashback_scn.is_valid() ||
             timeout_us <= 0) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", COMMON_LOG_INFO);
  } else if (true == is_meta_tenant(tenant_id) || true == is_sys_tenant(tenant_id)) {
    ret = OB_NOT_SUPPORTED;
    CLOG_LOG(WARN, "can't flashback meta tenant/sys tenant", COMMON_LOG_INFO);
  } else if (OB_SUCC(lock_.trylock())) {
    // 1. get ls list
    // 2. wait replicas of all ls are sync
    // 3. get mode_version and access_mode of all ls
    // 4. change access_mode of all ls to FLASHBACK
    // 5. send Flashback msg and wait all ls replicas returns OB_SUCCESS
    FLOG_INFO("[FLASHBACK_STAGE 0] start flashback", COMMON_LOG_INFO);
    common::ObTimeGuard time_guard("flashback", 1 * 1000 * 1000);
    share::ObLSStatusInfoArray ls_array;
    ChangeModeOpArray stop_mode_op_array, flashback_mode_op_array;
    const int64_t begin_time_us = common::ObTimeUtility::current_time();
    int64_t curr_time_us = 0;
    #define REMAIN_TIMEOUT (timeout_us - (common::ObTimeUtility::current_time() - begin_time_us))
    if (OB_FAIL(get_ls_list_(tenant_id, ls_array))) {
      CLOG_LOG(WARN, "get_ls_list_ failed", COMMON_LOG_INFO);
    } else if (FALSE_IT(time_guard.click("get_ls_list")) ||
        OB_FAIL(get_and_change_access_mode_(tenant_id, flashback_scn, palf::AccessMode::PREPARE_FLASHBACK,
        ls_array, REMAIN_TIMEOUT, stop_mode_op_array))) {
      CLOG_LOG(WARN, "get_and_change_access_mode_ failed", COMMON_LOG_INFO);
    } else if (FALSE_IT(time_guard.click("change_to_prepare_flshback_mode")) ||
        OB_FAIL(wait_all_ls_replicas_log_sync_(tenant_id, flashback_scn, ls_array, REMAIN_TIMEOUT))) {
      CLOG_LOG(WARN, "wait_all_ls_replicas_log_sync_ failed", COMMON_LOG_INFO);
    } else if (FALSE_IT(time_guard.click("wait_log_sync")) ||
        OB_FAIL(get_and_change_access_mode_(tenant_id, flashback_scn, palf::AccessMode::FLASHBACK,
        ls_array, REMAIN_TIMEOUT, flashback_mode_op_array))) {
      CLOG_LOG(WARN, "get_and_change_access_mode_ failed", COMMON_LOG_INFO);
    } else if (FALSE_IT(time_guard.click("change_to_flashback_mode")) ||
        OB_FAIL(do_flashback_(tenant_id, flashback_scn, flashback_mode_op_array, REMAIN_TIMEOUT))) {
      CLOG_LOG(WARN, "do_flashback_ failed", COMMON_LOG_INFO);
    } else {
      time_guard.click("flashback_op");
    }
    #undef REMAIN_TIMEOUT
    FLOG_INFO("[FLASHBACK_STAGE] flashback finish", COMMON_LOG_INFO, K(time_guard));
    (void) lock_.unlock();
  } else {
    CLOG_LOG(WARN, "can't flashback bacause another flashback operation is doing", COMMON_LOG_INFO);
  }
  #undef COMMON_LOG_INFO
  return ret;
}

int ObLogFlashbackService::get_ls_list_(const uint64_t tenant_id,
                                        share::ObLSStatusInfoArray &ls_array)
{
  int ret = OB_SUCCESS;
  share::ObLSStatusOperator ls_status_op;
  ls_array.reset();
  if (!is_user_tenant(tenant_id)|| OB_ISNULL(sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("only support flashback user tenant", KR(ret), K(tenant_id), KP(sql_proxy_));
  } else if (OB_FAIL(ls_status_op.get_all_ls_status_by_order_for_switch_tenant(
                                  tenant_id,
                                  false/* ignore_need_create_abort */,
                                  ls_array,
                                  *sql_proxy_))) {
    LOG_WARN("fail to get_all_ls_status_by_order_for_switch_tenant", KR(ret), K(tenant_id), KP(sql_proxy_));
  }
  return ret;
}

int ObLogFlashbackService::wait_all_ls_replicas_log_sync_(
    const uint64_t tenant_id,
    const SCN &flashback_scn,
    const share::ObLSStatusInfoArray &ls_array,
    const int64_t timeout_us) const
{
  int ret = OB_SUCCESS;
  #define COMMON_LOG_INFO K(ret), K_(self), K(tenant_id), K(flashback_scn)
  // 1. constructs ls operator list
  // 2. motivates all ls operator until success, timeout or fail
  CheckLogOpArray check_log_op_array;
  if (OB_FAIL(construct_ls_operator_list_(tenant_id, flashback_scn, ls_array, check_log_op_array))) {
    CLOG_LOG(WARN, "construct_ls_operator_list_ failed", COMMON_LOG_INFO);
  } else {
    palf::TimeoutChecker not_timeout(timeout_us);
    while (OB_SUCC(not_timeout())) {
      if (OB_SUCC(motivate_ls_operator_list_once_(check_log_op_array))) {
        FLOG_INFO("[FLASHBACK_STAGE 2] wait_all_ls_replicas_log_sync_ success", COMMON_LOG_INFO);
        break;
      } else if (OB_EAGAIN == ret) {
        if (REACH_TIME_INTERVAL(500 * 1000)) {
          CLOG_LOG(WARN, "wait_all_ls_replicas_log_sync_ eagain", COMMON_LOG_INFO, K(check_log_op_array));
        }
        ::usleep(10 * 1000);
      } else {
        CLOG_LOG(WARN, "wait_all_ls_replicas_log_sync_ fail", COMMON_LOG_INFO, K(check_log_op_array));
      }
    }
    // returns user-friendly error code
    if (OB_TIMEOUT == ret) {
      ret = OB_OP_NOT_ALLOW;
      CheckLogOpArray failed_ls_ops;
      for (int64_t i = 0; i < check_log_op_array.count(); i++) {
        CheckLSLogSyncOperator &op = check_log_op_array.at(i);
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS == op.ret_) {
        } else if (OB_SUCCESS != (tmp_ret = failed_ls_ops.push_back(op))) {
          CLOG_LOG(WARN, "push_back failed", K(ret), K_(self), K(op));
        }
      }
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "logs of some replicas may be not sync, role transition");
      CLOG_LOG(WARN, "logs of some replicas may be not sync, check these replicas", K(ret),
          K_(self), K(failed_ls_ops));
    }
  }
  #undef COMMON_LOG_INFO
  return ret;
}

int ObLogFlashbackService::get_and_change_access_mode_(
    const uint64_t tenant_id,
    const SCN &flashback_scn,
    const palf::AccessMode &dst_mode,
    const share::ObLSStatusInfoArray &ls_array,
    const int64_t timeout_us,
    ChangeModeOpArray &change_mode_op_array)
{
  int ret = OB_SUCCESS;
  #define COMMON_LOG_INFO K(ret), K_(self), K(tenant_id), K(dst_mode), K(flashback_scn)
  // 1. constructs ls operator list
  // 2. motivates all ls operator until success or fail
  if (OB_FAIL(construct_ls_operator_list_(tenant_id, flashback_scn, ls_array, change_mode_op_array))) {
    CLOG_LOG(WARN, "construct_ls_operator_list_ failed", COMMON_LOG_INFO);
  } else {
    for (int64_t i = 0; i < change_mode_op_array.count(); i++) {
      ChangeAccessModeOperator &op = change_mode_op_array.at(i);
      op.dst_mode_ = dst_mode;
    }
    palf::TimeoutChecker not_timeout(timeout_us);
    while (OB_SUCC(not_timeout())) {
      if (OB_SUCC(motivate_ls_operator_list_once_(change_mode_op_array))) {
        if (palf::AccessMode::PREPARE_FLASHBACK == dst_mode) {
          FLOG_INFO("[FLASHBACK_STAGE 1] change to PREPARE_FLASHBACK mode success", COMMON_LOG_INFO);
        } else {
          FLOG_INFO("[FLASHBACK_STAGE 3] change to FLASHBACK mode success", COMMON_LOG_INFO);
        }
        break;
      } else if (OB_EAGAIN == ret) {
        if (REACH_TIME_INTERVAL(500 * 1000)) {
          // display sync process
          CLOG_LOG(WARN, "get_and_change_access_mode_ eagain", COMMON_LOG_INFO, K(change_mode_op_array));
        }
        ::usleep(10 * 1000);
      } else {
        CLOG_LOG(WARN, "get_and_change_access_mode_ fail", COMMON_LOG_INFO, K(change_mode_op_array));
      }
    }
  }
  #undef COMMON_LOG_INFO
  return ret;
}

int ObLogFlashbackService::do_flashback_(
    const uint64_t tenant_id,
    const SCN &flashback_scn,
    const ChangeModeOpArray &mode_op_array,
    const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  #define COMMON_LOG_INFO K(ret), K_(self), K(tenant_id), K(flashback_scn)
  // 1. constructs ls operator list
  // 2. motivates all ls operator until success or fail
  // FlashbackOpArray flashback_op_array;
  if (OB_FAIL(cast_ls_operator_list_(mode_op_array, flashback_op_array_))) {
    CLOG_LOG(WARN, "construct_ls_operator_list_ failed", COMMON_LOG_INFO);
  } else {
    palf::TimeoutChecker not_timeout(timeout_us);
    while (OB_SUCC(not_timeout())) {
      if (OB_SUCC(motivate_ls_operator_list_once_(flashback_op_array_))) {
        FLOG_INFO("[FLASHBACK_STAGE 4] do_flashback_ success", COMMON_LOG_INFO);
        break;
      } else if (OB_EAGAIN == ret) {
        if (REACH_TIME_INTERVAL(500 * 1000)) {
          // display sync process
          CLOG_LOG(WARN, "do_flashback_ eagain", COMMON_LOG_INFO, K_(flashback_op_array));
        }
        ::usleep(50 * 1000);
      } else {
        CLOG_LOG(INFO, "do_flashback_ fail", COMMON_LOG_INFO, K_(flashback_op_array));
      }
    }
    // returns user-friendly error code
    if (OB_TIMEOUT == ret) {
      ret = OB_OP_NOT_ALLOW;
      FlashbackOpArray failed_ls_ops;
      for (int64_t i = 0; i < flashback_op_array_.count(); i++) {
        ExecuteFlashbackOperator &op = flashback_op_array_.at(i);
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS == op.ret_) {
        } else if (OB_SUCCESS != (tmp_ret = failed_ls_ops.push_back(op))) {
          CLOG_LOG(WARN, "push_back failed", K(ret), K_(self), K(op));
        }
      }
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "logs of some replicas may have not finished flashback, role transition");
      CLOG_LOG(WARN, "logs of some replicas may have not finished flashback, check these replicas",
          K(ret), K_(self), K(failed_ls_ops));
    }
  }
  #undef COMMON_LOG_INFO
  return ret;
}

template<typename T=ObLogFlashbackService::BaseLSOperator>
int ObLogFlashbackService::construct_ls_operator_list_(
    const uint64_t tenant_id,
    const SCN &flashback_scn,
    const share::ObLSStatusInfoArray &ls_array,
    common::ObArray<T> &ls_operator_array) const
{
  int ret = OB_SUCCESS;
  ls_operator_array.reset();
  for (int i = 0; i < ls_array.count(); i++) {
    const share::ObLSStatusInfo &ls_status = ls_array.at(i);
    const share::ObLSID &ls_id = ls_status.ls_id_;
    T op(tenant_id, ls_id, self_, flashback_scn, location_adapter_, rpc_proxy_);
    if (false == op.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(WARN, "FlashbackService Operator invalid", K(ret), K_(self), K(ls_id), K(op));
    } else if (OB_FAIL(ls_operator_array.push_back(op))) {
      CLOG_LOG(WARN, "push_back failed", K(ret), K_(self), K(ls_id));
    }
  }
  CLOG_LOG(INFO, "construct_ls_operator_list_ finish", K(ret), K_(self), K(flashback_scn),
      K(ls_array), K(ls_operator_array));
  return ret;
}

template<typename SRC_T, typename DST_T>
int ObLogFlashbackService::cast_ls_operator_list_(
    const common::ObArray<SRC_T> &src_array,
    common::ObArray<DST_T> &dst_array) const
{
  int ret = OB_SUCCESS;
  dst_array.reset();
  for (int i = 0; i < src_array.count(); i++) {
    DST_T op(src_array.at(i));
    if (false == op.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(WARN, "FlashbackService Operator invalid", K(ret), K_(self), K(op));
    } else if (OB_FAIL(dst_array.push_back(op))) {
      CLOG_LOG(WARN, "push_back failed", K(ret), K_(self));
    }
  }
  return ret;
}

template<typename T=ObLogFlashbackService::BaseLSOperator>
int ObLogFlashbackService::motivate_ls_operator_list_once_(common::ObArray<T> &ls_operator_array) const
{
  int ret = OB_SUCCESS;
  bool all_success = true;
  bool any_fail = false;
  int64_t any_fail_idx = -1;
  for (int64_t i = 0; i < ls_operator_array.count(); i++) {
    T &op = ls_operator_array.at(i);
    if (false == op.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(ERROR, "invalid operator", K(ret), K(op));
      break;
    } else if (OB_SUCC(op.switch_state())) {
      continue;
    } else if (OB_EAGAIN == ret) {
      all_success = false;
      continue;
    } else {
      CLOG_LOG(WARN, "CheckLSLogSyncOperator switch_state failed", K(ret), K(op));
      any_fail = true;
      all_success = false;
      any_fail_idx = i;
      break;
    }
  }
  if (true == all_success) {
  } else if (true == any_fail) {
    const T &op = ls_operator_array.at(any_fail_idx);
    CLOG_LOG(WARN, "motivate_ls_operator_list_once_ failed", K(ret), K(op));
  } else {
    ret = OB_EAGAIN;
  }
  return ret;
}

int ObLogFlashbackService::BaseLSOperator::update_leader_()
{
  int ret = OB_SUCCESS;
  leader_.reset();
  if (OB_FAIL(location_adapter_->nonblock_get_leader(tenant_id_, ls_id_.id(), leader_))) {
    CLOG_LOG(INFO, "nonblock_get_leader failed", K(ret), K_(ls_id));
    location_adapter_->nonblock_renew_leader(tenant_id_, ls_id_.id());
  }
  return ret;
}

int ObLogFlashbackService::BaseLSOperator::get_leader_palf_stat_(palf::PalfStat &palf_stat)
{
  int ret = OB_SUCCESS;
  const int64_t CONN_TIMEOUT_US = GCONF.rpc_timeout;
  if (false == leader_.is_valid() && OB_FAIL(update_leader_())) {
    ret = OB_EAGAIN;
  } else {
    const bool is_to_leader = true;
    LogGetPalfStatReq get_palf_stat_req(self_, ls_id_.id(), is_to_leader);
    LogGetPalfStatResp get_palf_stat_resp;
    if (OB_FAIL(rpc_proxy_->to(leader_).timeout(CONN_TIMEOUT_US).trace_time(true).
        max_process_handler_time(CONN_TIMEOUT_US).by(tenant_id_).
        get_palf_stat(get_palf_stat_req, get_palf_stat_resp))) {
      CLOG_LOG(WARN, "get_palf_stat failed", K(ret), KPC(this), K(get_palf_stat_req));
      // send RPC fail or not master, need renew leader
      leader_.reset();
      ret = OB_EAGAIN;
    } else {
      palf_stat = get_palf_stat_resp.palf_stat_;
    }
  }
  return ret;
}

int ObLogFlashbackService::BaseLSOperator::get_leader_list_(common::ObMemberList &member_list,
                                                            common::GlobalLearnerList &learner_list)
{
  int ret = OB_SUCCESS;
  palf::PalfStat palf_stat;
  if (OB_FAIL(get_leader_palf_stat_(palf_stat))) {
    CLOG_LOG(WARN, "get_leader_palf_stat_ failed", K(ret), KPC(this), K(palf_stat));
  } else {
    member_list = palf_stat.paxos_member_list_;
    learner_list = palf_stat.learner_list_;
  }
  return ret;
}

int ObLogFlashbackService::CheckLSLogSyncOperator::switch_state()
{
  int ret = OB_SUCCESS;
  palf::PalfStat palf_stat;
  if (false == this->is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "invalid operator", K(ret), KPC(this));
  } else if (false == leader_.is_valid() && OB_FAIL(update_leader_())) {
    ret = OB_EAGAIN;
  } else if (false == has_get_member_list_ && OB_FAIL(get_leader_palf_stat_(palf_stat))) {
    // eagain
  } else if (palf_stat.is_valid() && palf::AccessMode::FLASHBACK == palf_stat.access_mode_) {
    // ls has been changed to FLASHBACK mode, don't need to check log sync
  } else {
    has_get_member_list_ = true;
    member_list_ = (palf_stat.is_valid())? palf_stat.paxos_member_list_: member_list_;
    learner_list_ = (palf_stat.is_valid())? palf_stat.learner_list_: learner_list_;
    int64_t unsync_member_cnt = 0;
    int64_t unused_cnt = 0;
    int tmp_ret_member = check_list_log_sync(member_list_, log_sync_memberlist_, unsync_member_cnt);
    int tmp_ret_learner = check_list_log_sync(learner_list_, log_sync_learnerlist_, unused_cnt);
    ret = (OB_SUCCESS == tmp_ret_member && OB_SUCCESS == tmp_ret_learner)? OB_SUCCESS: OB_EAGAIN;
    // Note: end_scn of some log stream may be smaller than flasback_scn in switchover scenario,
    //       becauase flashback_scn is the end_scn of the log stream whose end_scn is the greatest.
    //       Therefore, we skip log streams whose logs are committed entirely and its end_scn is
    //       smaller than flashback_scn. It's safe because these log streams must be in PREPARE_FLASHBACK
    //       mode (its end_scn will not increase)
    if (unsync_member_cnt == member_list_.get_member_number()) {
      ret = OB_SUCCESS;
      CLOG_LOG(INFO, "end_scn of all members are smaller than flashback_scn",
          K(ret), KPC(this), K(unsync_member_cnt));
    }
  }
  if (OB_SUCC(ret)) {
    CLOG_LOG(INFO, "check_ls_log_sync success", K(ret), KPC(this));
  } else if (OB_EAGAIN == ret) {
    if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
      CLOG_LOG(WARN, "check_ls_log_sync eagain", K(ret), KPC(this));
    }
  }
  ret_ = ret;
  return ret;
}

int ObLogFlashbackService::ChangeAccessModeOperator::switch_state()
{
  int ret = OB_SUCCESS;
  // 1. get access_mode
  // 2. change_access_mode
  const int64_t CONN_TIMEOUT_US = GCONF.rpc_timeout;
  LogGetPalfStatReq get_mode_req(self_, ls_id_.id(), true);
  LogGetPalfStatResp get_mode_resp;
  if (false == this->is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "invalid operator", K(ret), KPC(this));
  } else if (false == leader_.is_valid() && OB_FAIL(update_leader_())) {
    ret = OB_EAGAIN;
  } else if (OB_FAIL(rpc_proxy_->to(leader_).timeout(CONN_TIMEOUT_US).trace_time(true).
                      max_process_handler_time(CONN_TIMEOUT_US).by(tenant_id_).
                      get_palf_stat(get_mode_req, get_mode_resp))) {
    CLOG_LOG(WARN, "get_palf_stat failed", K(ret), KPC(this), K(get_mode_req));
    leader_.reset();
    ret = OB_EAGAIN;
  } else if (FALSE_IT(mode_version_ = get_mode_resp.palf_stat_.mode_version_)) {
  } else if (dst_mode_ == get_mode_resp.palf_stat_.access_mode_ ||
      palf::AccessMode::FLASHBACK == get_mode_resp.palf_stat_.access_mode_) {
    // access_mode has been changed to dst_mode, skip
  } else {
    LogChangeAccessModeCmd change_mode_cmd(self_, ls_id_.id(), mode_version_, dst_mode_, SCN::min_scn());
    if (OB_FAIL(rpc_proxy_->to(leader_).timeout(CONN_TIMEOUT_US).trace_time(true).
        max_process_handler_time(CONN_TIMEOUT_US).by(tenant_id_).
        send_change_access_mode_cmd(change_mode_cmd, NULL))) {
      CLOG_LOG(WARN, "send_change_access_mode_cmd failed", K(ret), KPC(this), K(change_mode_cmd));
    }
    ret = OB_EAGAIN;
  }
  if (OB_SUCC(ret)) {
    CLOG_LOG(INFO, "change_access_mode success", K(ret), KPC(this));
  } else if (OB_EAGAIN == ret) {
    if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
      CLOG_LOG(WARN, "change_access_mode eagain", K(ret), KPC(this));
    }
  }
  ret_ = ret;
  return ret;
}

int ObLogFlashbackService::ExecuteFlashbackOperator::switch_state()
{
  int ret = OB_SUCCESS;
  common::ObSpinLockGuard guard(lock_);
  if (false == this->is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "invalid operator", K(ret), KPC(this));
  } else if (false == leader_.is_valid() && OB_FAIL(update_leader_())) {
    ret = OB_EAGAIN;
  } else if (true == has_get_member_list_ ||
      (false == has_get_member_list_ && OB_SUCC(get_leader_list_(member_list_, learner_list_)))) {
    has_get_member_list_ = true;
    const int tmp_ret_member = flashback_list_(member_list_, flashbacked_memberlist_);
    const int tmp_ret_learner = flashback_list_(learner_list_, flashbacked_learnerlist_);
    ret = (OB_SUCCESS == tmp_ret_member && OB_SUCCESS == tmp_ret_learner)? OB_SUCCESS: OB_EAGAIN;
  }
  if (OB_SUCC(ret)) {
    CLOG_LOG(INFO, "do_flashback success", K(ret), KPC(this));
  } else if (OB_EAGAIN == ret) {
    if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
      CLOG_LOG(WARN, "do_flashback eagain", K(ret), KPC(this));
    }
  }
  ret_ = ret;
  return ret;
}

int ObLogFlashbackService::ExecuteFlashbackOperator::handle_flashback_resp(const LogFlashbackMsg &resp)
{
  int ret = OB_SUCCESS;
  common::ObSpinLockGuard guard(lock_);
  if (false == this->is_valid() || false == has_get_member_list_) {
    CLOG_LOG(WARN, "can not handle_flashback_resp", K(ret), KPC(this), K(resp));
  } else if (false == resp.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), KPC(this), K(resp));
  } else if (true == member_list_.contains(resp.src_)) {
    (void) flashbacked_memberlist_.add_server(resp.src_);
  } else if (true == learner_list_.contains(resp.src_)) {
    (void) flashbacked_learnerlist_.add_server(resp.src_);
  } else {
    CLOG_LOG(WARN, "flashbacked member is not in memberlist", K(ret), KPC(this), K(resp));
  }
  return ret;
}

int ObLogFlashbackService::handle_flashback_resp(const LogFlashbackMsg &resp)
{
  int ret = OB_SUCCESS;
  bool has_find_ls = false;
  for (int64_t i = 0; i < flashback_op_array_.count(); i++) {
    ExecuteFlashbackOperator &op = flashback_op_array_.at(i);
    if (resp.ls_id_ == op.ls_id_.id() && resp.mode_version_ == op.mode_version_) {
      ret = op.handle_flashback_resp(resp);
      has_find_ls = true;
      break;
    }
  }
  if (false == has_find_ls) {
    CLOG_LOG(WARN, "do not find ls in flashback_op_array", K(ret), K(resp), K_(flashback_op_array));
  } else {
    CLOG_LOG(INFO, "handle_flashback_resp finished", K(ret), K(resp), K_(flashback_op_array));
  }
  return ret;
}

}//end of namespace logservice
}//end of namespace oceanbase
