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


#include "logservice/palf/election/message/election_message.h"
#include "ob_role.h"
#include "share/ob_occam_time_guard.h"
#include "election_proposer.h"
#include "common/ob_clock_generator.h"
#include "election_impl.h"
#include "lib/ob_errno.h"
#include "lib/string/ob_string_holder.h"
#include "logservice/palf/election/interface/election_msg_handler.h"
#include "logservice/palf/election/utils/election_common_define.h"
#include "logservice/palf/election/utils/election_member_list.h"
#include "logservice/palf/palf_callback.h"

namespace oceanbase
{
namespace palf
{
namespace election
{

struct ResponseChecker
{
  template <typename ResponseMsg>
  static bool check_ballot_and_restart_counter_valid_and_accepted(const ResponseMsg &msg,
                                                                  ElectionProposer *proposer,
                                                                  const LogPhase phase)
  {
    ELECT_TIME_GUARD(500_ms);
    #define PRINT_WRAPPER K(msg), K(*proposer)
    bool ret = false;
    // 1. 检查restart counter
    if (OB_UNLIKELY(msg.get_restart_counter() != proposer->restart_counter_)) {
      LOG_PHASE(INFO, phase, "receive old restart counter response");
      assert(msg.get_restart_counter() < proposer->restart_counter_);
    // 2. 检查ballot number
    } else if (OB_UNLIKELY(msg.get_ballot_number() < proposer->ballot_number_)) {
      LOG_PHASE(WARN, phase, "receive old ballot response");
    // 3. ballot number更大的时候更新ballot number，若自己仍然是leader，触发leader prepare
    } else if (msg.get_ballot_number() > proposer->ballot_number_) {
      assert(!msg.is_accepted());
      proposer->advance_ballot_number_and_reset_related_states_(msg.get_ballot_number(),
                                                                "receive reject message");
      if (proposer->check_leader()) {
        // 在check leader后可能卡住，做leader prepare时就已经不再是leader了
        // 但是没有关系，正确性是由prepare阶段保证的，check_leader的意义在于尽量避免无谓的leader prepare流程
        LOG_PHASE(WARN, phase, "leader message is rejected cause ballot number", K(msg.get_ballot_number()), K(proposer->prepare_success_ballot_));
        proposer->advance_ballot_number_and_reset_related_states_(proposer->ballot_number_ + 1,
                                                                  "retry leader prepare");
        proposer->prepare(ObRole::LEADER);
      }
    // 4. 检查消息是否因为其他原因被拒绝
    } else if (!msg.is_accepted()) {
      LOG_PHASE(WARN, phase, "request is rejected");
    } else {
      ret = true;
    }
    return ret;
    #undef PRINT_WRAPPER
  }
};

ElectionProposer::ElectionProposer(ElectionImpl *election)
:role_(ObRole::FOLLOWER),
ballot_number_(INVALID_VALUE),
prepare_success_ballot_(INVALID_VALUE),
switch_source_leader_ballot_(INVALID_VALUE),
restart_counter_(INVALID_VALUE),
p_election_(election),
last_do_prepare_ts_(INVALID_VALUE),
last_dump_proposer_info_ts_(INVALID_VALUE),
last_dump_election_msg_count_state_ts_(INVALID_VALUE),
record_lease_interval_(INVALID_VALUE) {}

int ElectionProposer::init(const int64_t restart_counter)
{
  ELECT_TIME_GUARD(500_ms);
  #define PRINT_WRAPPER K(*this)
  int ret = OB_SUCCESS;
  if (CLICK_FAIL(memberlist_with_states_.init())) {
    LOG_INIT(ERROR, "init memberlist with states failed");
  } else {
    restart_counter_ = restart_counter;
  }
  return ret;
  #undef PRINT_WRAPPER
}

int ElectionProposer::set_member_list(const MemberList &new_member_list)
{
  ELECT_TIME_GUARD(500_ms);
  #define PRINT_WRAPPER K(*this), K(new_member_list)
  int ret = OB_SUCCESS;
  // 检查旧的成员组的信息是否一致
  const MemberList &current_member_list = memberlist_with_states_.get_member_list();
  if (current_member_list.is_valid()) {
    if (new_member_list.get_membership_version() < current_member_list.get_membership_version()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_SET_MEMBER(ERROR, "new memberlsit's membership version is not greater than current");
    } else if (check_leader()) {
      if (!memberlist_with_states_.is_synced_with_majority()) {
        ret = OB_OP_NOT_ALLOW;
        LOG_SET_MEMBER(WARN, "current membership version is not sync with majority yet, change member list not allowed now");
      }
    }
  }
  if (OB_SUCC(ret)) {
    MemberList old_list = memberlist_with_states_.get_member_list();
    if (CLICK_FAIL(memberlist_with_states_.set_member_list(new_member_list))) {
      LOG_SET_MEMBER(WARN, "set new member list failed");
    } else {
      if (old_list.get_addr_list().empty() && new_member_list.get_addr_list().count() == 1) {// 单副本第一次设置成员列表
        prepare(ObRole::FOLLOWER);
      }
      if (old_list.only_membership_version_different(new_member_list)) {
        LOG_SET_MEMBER(INFO, "advance membership version");
      } else {
        LOG_SET_MEMBER(INFO, "member list is changed");
        p_election_->event_recorder_.report_member_list_changed_event(old_list, memberlist_with_states_.get_member_list());
      }
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

int ElectionProposer::change_leader_to(const ObAddr &dest_addr)
{
  ELECT_TIME_GUARD(500_ms);
  #define PRINT_WRAPPER K(*this), K(dest_addr), K(redirect_addr)
  int ret = OB_SUCCESS;
  int idx = 0;
  ObAddr redirect_addr = dest_addr;
  const ObArray<ObAddr> &addr_list = memberlist_with_states_.get_member_list().get_addr_list();
  for (; idx < addr_list.count(); ++idx) {
    if (dest_addr == addr_list[idx]) {
      break;
    }
  }
  if (OB_UNLIKELY(idx == addr_list.count())) {
    LOG_CHANGE_LEADER(WARN, "the dest addr is not in member list, change leader to myself");
    redirect_addr = p_election_->get_self_addr();
  }
  if (OB_UNLIKELY(!check_leader())) {
    ret = OB_NOT_MASTER;
    LOG_CHANGE_LEADER(WARN, "follower cannot do change leader");
  } else {
    inner_change_leader_to(redirect_addr);
    LOG_CHANGE_LEADER(INFO, "direct change leader", K(lbt()));
  }
  return ret;
  #undef PRINT_WRAPPER
}

bool ElectionProposer::leader_revoke_if_lease_expired_(RoleChangeReason reason)
{
  ELECT_TIME_GUARD(500_ms);
  #define PRINT_WRAPPER K(*this)
  bool ret_bool = false;
  if (role_ == ObRole::LEADER && !check_leader()) {
    (p_election_->role_change_cb_)(p_election_, ObRole::LEADER, ObRole::FOLLOWER, reason);
    role_ = ObRole::FOLLOWER;
    memberlist_with_states_.clear_prepare_and_accept_states();
    leader_lease_and_epoch_.reset();
    ret_bool = true;
  }
  return ret_bool;
  #undef PRINT_WRAPPER
}

bool ElectionProposer::leader_takeover_if_lease_valid_(RoleChangeReason reason)
{
  ELECT_TIME_GUARD(500_ms);
  #define PRINT_WRAPPER K(*this)
  bool ret_bool = false;
  int ret = common::OB_SUCCESS;
  if (role_ != ObRole::LEADER && check_leader()) {
    (p_election_->role_change_cb_)(p_election_, ObRole::FOLLOWER, ObRole::LEADER, reason);
    highest_priority_cache_.reset();
    role_ = ObRole::LEADER;
    propose();
    ret_bool = true;
  }
  return ret_bool;
  #undef PRINT_WRAPPER
}

void ElectionProposer::
     advance_ballot_number_and_reset_related_states_(const int64_t new_ballot_number,
                                                     const char *reason)
{
  ELECT_TIME_GUARD(500_ms);
  assert(new_ballot_number >= ballot_number_);
  ELECT_LOG(INFO, "advance ballot number", K(new_ballot_number), K(reason), K(*this));
  ballot_number_ = new_ballot_number;
  memberlist_with_states_.clear_prepare_and_accept_states();
  // 如果在切主过程中推大ballot number需要清理切主流程中的相关状态
  switch_source_leader_ballot_ = INVALID_VALUE;
  switch_source_leader_addr_.reset();
}

int ElectionProposer::register_renew_lease_task_()
{
  ELECT_TIME_GUARD(500_ms);
  #define PRINT_WRAPPER KR(ret), K(*this)
  int ret = OB_SUCCESS;
  // 如果续约不够快很多case不能及时切主，所以当MAX_TST设置的比较大时，也让续约间隔设置的短一些
  if (CLICK_FAIL(p_election_->timer_->schedule_task_repeat(renew_lease_task_handle_,
                                                           std::min(int64_t(500_ms), CALCULATE_RENEW_LEASE_INTERVAL()),
                                                           [this]() {
    int ret = OB_SUCCESS;
    LockGuard lock_guard(p_election_->lock_);
    // 周期性打印选举的状态
    if (ObClockGenerator::getCurrentTime() > last_dump_proposer_info_ts_ + 3_s) {
      last_dump_proposer_info_ts_ = ObClockGenerator::getCurrentTime();
      ELECT_LOG(INFO, "dump proposer info", K(*this));
    }
    // 周期性打印选举的消息收发统计信息
    if (ObClockGenerator::getCurrentTime() > last_dump_election_msg_count_state_ts_ + 10_s) {
      last_dump_election_msg_count_state_ts_ = ObClockGenerator::getCurrentTime();
      char ls_id_buffer[32] = {0};
      auto pretend_to_be_ls_id = [ls_id_buffer](const int64_t id) mutable {
        int64_t pos = 0;
        databuff_printf(ls_id_buffer, 32, pos, "{id:%ld}", id);
        return ls_id_buffer;
      };// 在日志打印时与ls_id的格式保持一致
      ELECT_LOG(INFO, "dump message count", "ls_id", pretend_to_be_ls_id(p_election_->id_), "self_addr", p_election_->self_addr_, "state", p_election_->msg_counter_);
    }
    // 判断是否可以进行续约
    if (!p_election_->is_running_) {
      LOG_RENEW_LEASE(WARN, "election is stopped, this renew lease task should be stopped");
    } else if (role_ == ObRole::FOLLOWER) {
      // 在FOLLOWER上仍然需要执行该定时任务，以周期性打印选举状态
    } else if (role_ != ObRole::LEADER) {
      LOG_RENEW_LEASE(ERROR, "unexpected role status");
    } else if (OB_UNLIKELY(leader_revoke_if_lease_expired_(RoleChangeReason::LeaseExpiredToRevoke))) {
      LOG_RENEW_LEASE(WARN, "leader lease expired, leader revoked");
    } else if (prepare_success_ballot_ != ballot_number_) {// 需要进行leader prepare推大用于续约的ballot number
      LOG_RENEW_LEASE(INFO, "prepare_success_ballot_ not same as ballot_number_, maybe in Leader Prepare phase, gie up renew lease this time");
    } else {
      propose();
    }
    return false;
  }))) {
    LOG_INIT(ERROR, "regist renew lease task failed");
  }
  return ret;
  #undef PRINT_WRAPPER
}

// 这个接口是在外部未加锁的状态下调用的
int ElectionProposer::reschedule_or_register_prepare_task_after_(const int64_t delay_us)
{
  ELECT_TIME_GUARD(500_ms);
  #define PRINT_WRAPPER KR(ret), K(delay_us)
  int ret = OB_SUCCESS;
  CLICK();
  if (delay_us < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_NONE(ERROR, "invalid argument");
  } else if (devote_task_handle_.is_running()) {
    if (CLICK_FAIL(devote_task_handle_.reschedule_after(delay_us))) {
      LOG_RENEW_LEASE(ERROR, "reschedule devote task failed");
    }
  } else if (CLICK_FAIL(p_election_->timer_->schedule_task_repeat_spcifiy_first_delay(devote_task_handle_,
                                                                                      delay_us,
                                                                                      CALCULATE_MAX_ELECT_COST_TIME(),
                                                                                      [this, delay_us]() {
    int ret = OB_SUCCESS;
    LockGuard lock_guard(p_election_->lock_);
    if (check_leader()) {// Leader不应该靠定时任务主动做Prepare，只能被动触发Prepare
      LOG_RENEW_LEASE(INFO, "leader not allow do prepare in timer task before lease expired, this log may printed when message delay too large", K(*this));
    } else {
      if (role_ == ObRole::LEADER) {
        role_ = ObRole::FOLLOWER;
      }
      this->prepare(role_);// 只有Follower可以走到这里
    }
    return false;
  }))) {
    LOG_INIT(ERROR, "first time register devote task failed");
  }
  return ret;
  #undef PRINT_WRAPPER
}

int ElectionProposer::start()
{
  #define PRINT_WRAPPER K(*this)
  ELECT_TIME_GUARD(500_ms);
  int ret = OB_SUCCESS;
  if (CLICK_FAIL(reschedule_or_register_prepare_task_after_(3_s))) {
    LOG_INIT(ERROR, "first time register devote task failed");
  } else if (CLICK_FAIL(register_renew_lease_task_())) {
    LOG_INIT(ERROR, "first time register renew lease task failed");
  }
  return ret;
  #undef PRINT_WRAPPER
}

void ElectionProposer::stop()
{
  #define PRINT_WRAPPER K(*this)
  ELECT_TIME_GUARD(3_s);
  devote_task_handle_.stop_and_wait();
  renew_lease_task_handle_.stop_and_wait();
  LockGuard lock_guard(p_election_->lock_);
  leader_lease_and_epoch_.reset();
  if (leader_revoke_if_lease_expired_(RoleChangeReason::StopToRevoke)) {
    LOG_DESTROY(INFO, "leader revoke because election is stopped");
  }
   #undef PRINT_WRAPPER
}

void ElectionProposer::prepare(const ObRole role)
{
  ELECT_TIME_GUARD(500_ms);
  #define PRINT_WRAPPER KR(ret), K(role), K(*this)
  int ret = OB_SUCCESS;
  int64_t cur_ts = ObClockGenerator::getCurrentTime();
  LogPhase phase = role == ObRole::LEADER ? LogPhase::RENEW_LEASE : LogPhase::ELECT_LEADER;
  if (memberlist_with_states_.get_member_list().get_addr_list().empty()) {
    LOG_PHASE(INFO, phase, "memberlist is empty, give up do prepare this time");
  } else if (!is_self_in_memberlist_()) {
    LOG_PHASE(INFO, phase, "self is not in memberlist, give up do prepare");
  } else if (role == ObRole::FOLLOWER && cur_ts - last_do_prepare_ts_ < CALCULATE_MAX_ELECT_COST_TIME() / 2) {// 若这是一个一乎动作，且距离上一次一呼百应的时间点过近，该次一乎调度无效
    LOG_PHASE(INFO, phase, "the prepare action just happened, need wait next time");
  } else {
    last_do_prepare_ts_ = cur_ts;
    // 1. Leader prepare不推ballot number，Follower prepare需要推大自己的ballot number再进行
    if (role == ObRole::FOLLOWER) {
      (void) advance_ballot_number_and_reset_related_states_(ballot_number_ + 1, "do prepare");
      LOG_PHASE(INFO, phase, "do prepare");
    } else if (role == ObRole::LEADER) {
      LOG_PHASE(INFO, phase, "do leader prepare");
    } else {
      LOG_PHASE(ERROR, phase, "unexpected code path");
      abort();
    }
    // 2. 获取本地的优先级
    ElectionPrepareRequestMsg prepare_req(p_election_->id_,
                                          p_election_->get_self_addr(),
                                          restart_counter_,
                                          ballot_number_,
                                          p_election_->get_ls_biggest_min_cluster_version_ever_seen_(),
                                          p_election_->inner_priority_seed_,
                                          p_election_->get_membership_version_());
    (void) p_election_->refresh_priority_();
    if (CLICK_FAIL(prepare_req.set(p_election_->get_priority_(),
                                   role))) {
      LOG_PHASE(ERROR, phase, "create prepare request failed");
    // 3. 广播消息
    } else if (CLICK_FAIL(p_election_->broadcast_(prepare_req,
                                              memberlist_with_states_.
                                              get_member_list().
                                              get_addr_list()))) {
      LOG_PHASE(ERROR, phase, "broadcast prepare request failed");
    } else {
      LOG_PHASE(INFO, phase, "broadcast prepare request");
    }
  }
  #undef PRINT_WRAPPER
}

void ElectionProposer::on_prepare_request(const ElectionPrepareRequestMsg &prepare_req,
                                          bool *need_register_devote_task)
{
  ELECT_TIME_GUARD(500_ms);
  #define PRINT_WRAPPER KR(ret), K(prepare_req), K(*this)
  int ret = OB_SUCCESS;
  // 0. 拒绝旧消息、过滤本轮次消息、根据新消息推大轮次
  if (prepare_req.get_ballot_number() <= ballot_number_) {
    if (prepare_req.get_ballot_number() < ballot_number_) {// 对于旧消息发送拒绝响应
      ElectionPrepareResponseMsg prepare_res_reject(p_election_->get_self_addr(),
                                                    p_election_->get_ls_biggest_min_cluster_version_ever_seen_(),
                                                    prepare_req);
      prepare_res_reject.set_rejected(ballot_number_);
      if (CLICK_FAIL(p_election_->send_(prepare_res_reject))) {
        LOG_ELECT_LEADER(ERROR, "create prepare request failed");
      } else {
        LOG_ELECT_LEADER(INFO, "send reject response cause prepare message ballot too small");
      }
    } else {// 对于本轮次消息，需要过滤，否则无限循环
      LOG_ELECT_LEADER(INFO, "has been send prepare request in this ballot, give up this time");
    }
  } else if (!is_self_in_memberlist_()) {
    LOG_ELECT_LEADER(INFO, "self is not in memberlist, give up do prepare");
  } else {// 对于新的消息，推大本机选举轮次
    LOG_ELECT_LEADER(INFO, "receive bigger ballot prepare request");
    (void) advance_ballot_number_and_reset_related_states_(prepare_req.get_ballot_number(),
                                                           "receive bigger ballot prepare request");
      // 1. 忽略leader prepare消息，不触发一呼百应
    if (static_cast<ObRole>(prepare_req.get_role()) == ObRole::LEADER) {
      LOG_ELECT_LEADER(INFO, "proposer ignore leader prepare");
    } else if (static_cast<ObRole>(prepare_req.get_role()) != ObRole::FOLLOWER) {
      // 非candidate prepare是非预期的
      LOG_ELECT_LEADER(ERROR, "unexpected code path");
    // 2. 尝试一呼百应
    } else if (memberlist_with_states_.get_member_list().get_addr_list().empty()) {
      LOG_ELECT_LEADER(INFO, "memberlist is empty, give up do prepare this time");
    } else {
      (void) p_election_->refresh_priority_();
      ElectionPrepareRequestMsg prepare_followed_req(p_election_->id_,
                                                     p_election_->get_self_addr(),
                                                     restart_counter_,
                                                     ballot_number_,
                                                     p_election_->get_ls_biggest_min_cluster_version_ever_seen_(),
                                                     p_election_->inner_priority_seed_,
                                                     p_election_->get_membership_version_());
      if (CLICK_FAIL(prepare_followed_req.set(p_election_->get_priority_(),
                                              role_))) {
        LOG_ELECT_LEADER(ERROR, "create prepare request failed");
      } else if (CLICK_FAIL(p_election_->broadcast_(prepare_followed_req,
                                                    memberlist_with_states_.get_member_list()
                                                                           .get_addr_list()))) {
        LOG_ELECT_LEADER(ERROR, "broadcast prepare request failed");
      } else {
        last_do_prepare_ts_ = ObClockGenerator::getCurrentTime();
        if (role_ == ObRole::LEADER) {
          LOG_ELECT_LEADER(INFO, "join elect leader phase as leader");
        } else if (role_ == ObRole::FOLLOWER) {
          LOG_ELECT_LEADER(INFO, "join elect leader phase as follower");
        }
        *need_register_devote_task = true;
      }
    }
  }
  #undef PRINT_WRAPPER
}

void ElectionProposer::on_prepare_response(const ElectionPrepareResponseMsg &prepare_res)
{
  ELECT_TIME_GUARD(500_ms);
  #define PRINT_WRAPPER KR(ret), K(prepare_res), K(*this)
  int ret = OB_SUCCESS;
  LogPhase phase = role_ == ObRole::LEADER ? LogPhase::RENEW_LEASE : LogPhase::ELECT_LEADER;
  LOG_PHASE(INFO, phase, "handle prepare response");
  const Lease &res_lease = prepare_res.get_lease();
  // 1. 检查消息的有效性
  if (!ResponseChecker::check_ballot_and_restart_counter_valid_and_accepted(prepare_res, this, phase)) {
    LOG_PHASE(WARN, phase, "receive invalid or rejected response");
  // 2. 检查是否具备进入accept阶段的条件
  } else if (!(res_lease.is_empty() ||// 1. 对方目前没有lease
              res_lease.get_owner() == p_election_->get_self_addr() ||// 2. lease是本proposer发出的
              (res_lease.get_owner() == switch_source_leader_addr_ &&// 3. 对方的lease是切主源端的
              res_lease.get_ballot_number() == switch_source_leader_ballot_))) {// 处于旧主的ballot内
    LOG_PHASE(INFO, phase, "peer lease still valid, not count");
  // 3. 记录对方的应答，计入多数派中
  } else if (CLICK_FAIL(memberlist_with_states_.record_prepare_ok(prepare_res))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      LOG_PHASE(WARN, phase, "peer maybe not in memberlist anymore");
    } else if (OB_ELECTION_BELOW_MAJORITY == ret) {
      LOG_PHASE(INFO, phase, "response not reach majority, waiting more...");
    } else if (OB_ELECTION_OVER_MAJORITY == ret) {
      LOG_PHASE(INFO, phase, "has been send accept message this ballot");
    } else {
      LOG_PHASE(ERROR, phase, "unexpected error code");
    }
  // 4. 若应答刚好达到多数派，进入accept阶段
  } else {
    prepare_success_ballot_ = max(ballot_number_, prepare_success_ballot_);
    propose();
    LOG_PHASE(INFO, phase, "do propose");
  }
  #undef PRINT_WRAPPER
}

void ElectionProposer::propose()
{
  ELECT_TIME_GUARD(500_ms);
  #define PRINT_WRAPPER KR(ret), K(*this)
  int ret = OB_SUCCESS;
  int64_t current_ts = get_monotonic_ts();
  int64_t new_lease_interval = CALCULATE_LEASE_INTERVAL();
  LogPhase phase = role_ == ObRole::LEADER ? LogPhase::RENEW_LEASE : LogPhase::ELECT_LEADER;
  if (new_lease_interval != record_lease_interval_) {
    LOG_PHASE(INFO, phase, "lease interval changed", K(record_lease_interval_), K(new_lease_interval));
    record_lease_interval_ = new_lease_interval;
  }
  ElectionAcceptRequestMsg accept_req(p_election_->id_,
                                      p_election_->get_self_addr(),
                                      restart_counter_,
                                      prepare_success_ballot_,
                                      p_election_->get_ls_biggest_min_cluster_version_ever_seen_(),
                                      current_ts,
                                      new_lease_interval,
                                      memberlist_with_states_.get_member_list()
                                                             .get_membership_version());
  if (CLICK_FAIL(p_election_->broadcast_(accept_req, memberlist_with_states_.get_member_list()
                                                                            .get_addr_list()))) {
    LOG_PHASE(ERROR, phase, "broadcast accept request failed", K(accept_req));
  }
  #undef PRINT_WRAPPER
}

void ElectionProposer::on_accept_response(const ElectionAcceptResponseMsg &accept_res)
{
  ELECT_TIME_GUARD(500_ms);
  #define PRINT_WRAPPER KR(ret), K(accept_res), K(new_lease_end), K(*this)
  int ret = OB_SUCCESS;
  int64_t new_lease_end = 0;
  LogPhase phase = role_ == ObRole::LEADER ? LogPhase::RENEW_LEASE : LogPhase::ELECT_LEADER;
  // 1. 检查消息的有效性
  if (!ResponseChecker::check_ballot_and_restart_counter_valid_and_accepted(accept_res, this, LogPhase::RENEW_LEASE)) {
    LOG_PHASE(WARN, phase, "receive invalid or rejected response");
  // 2. 记录多数派
  } else if (CLICK_FAIL(memberlist_with_states_.record_accept_ok(accept_res))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      LOG_PHASE(WARN, phase, "peer maybe not in memberlist anymore");
    } else if (OB_ELECTION_BELOW_MAJORITY == ret) {
      LOG_PHASE(INFO, phase, "response not reach majority, waiting more...");
    } else {
      LOG_PHASE(ERROR, phase, "unexpected error code");
    }
  // 3. 拿到新的lease
  } else if (CLICK_FAIL(memberlist_with_states_.get_majority_promised_not_vote_ts(new_lease_end))) {
    LOG_PHASE(ERROR, phase, "get majority promised not vote ts failed");
  // 4. 更新lease
  } else {
    // 4.1 如果当前lease已经失效，中间可能出现过其他的leader，而本副本还未感知，需要先卸任
    if (OB_UNLIKELY(leader_revoke_if_lease_expired_(RoleChangeReason::LeaseExpiredToRevoke))) {
      LOG_PHASE(WARN, phase, "lease is expired, need revoke before takeover");
    }
    // 4.2 更新lease和允许成员变更的版本号
    int64_t record_leader_lease_end;
    int64_t exposed_epoch;// not used
    leader_lease_and_epoch_.get(record_leader_lease_end, exposed_epoch);
    if (new_lease_end > record_leader_lease_end) {// 需要更新
      leader_lease_and_epoch_.set_lease_and_epoch_if_lease_expired_or_just_set_lease(new_lease_end, prepare_success_ballot_);
    }
    // 如果4.1步骤检查lease没有过期，但是在4.3步骤之前lease刚好过期，是有可能发生的，但是如果4.3步骤执行成功
    // 可以推出，4.1-4.3步骤之间不存在其他leader上任，因为在4.3步骤上任成功之前多数派都是被lease锁住的
    // 4.3 若之前的角色是Follower且当前lease有效则尝试上任
    if (switch_source_leader_ballot_ != INVALID_VALUE) {
      if (switch_source_leader_ballot_ != ballot_number_ - 1) {
        LOG_CHANGE_LEADER(WARN, "self ballot number is advanced, change leader failed");
      } else if (leader_takeover_if_lease_valid_(RoleChangeReason::ChangeLeaderToBeLeader)) {
        LOG_CHANGE_LEADER(INFO, "change leader, new leader takeover success");
        switch_source_leader_ballot_ = INVALID_VALUE;
        switch_source_leader_addr_.reset();
      }
    } else {
      if (leader_takeover_if_lease_valid_(RoleChangeReason::DevoteToBeLeader)) {
        LOG_ELECT_LEADER(INFO, "decentralized voting, leader takeover success");
      }
    }
  }
  // 5. 检查follower的优先级是否高于Leader，尝试触发切主
  if (role_ == ObRole::LEADER && accept_res.get_sender() != p_election_->self_addr_) {
    ObStringHolder higher_than_leader_reason;
    ObStringHolder higher_than_cached_msg_reason;
    (void) p_election_->refresh_priority_();
    ElectionAcceptResponseMsg mock_self_accept_response_msg(p_election_->self_addr_,
                                                            p_election_->inner_priority_seed_,
                                                            p_election_->get_membership_version_(),
                                                            p_election_->get_ls_biggest_min_cluster_version_ever_seen_(),
                                                            ElectionAcceptRequestMsg(p_election_->id_,
                                                                                     p_election_->self_addr_,
                                                                                     restart_counter_,
                                                                                     ballot_number_,
                                                                                     p_election_->get_ls_biggest_min_cluster_version_ever_seen_(),
                                                                                     0,
                                                                                     record_lease_interval_,
                                                                                     p_election_->get_membership_version_()));
    if (CLICK_FAIL(mock_self_accept_response_msg.set_accepted(ballot_number_, p_election_->priority_))) {
      LOG_CHANGE_LEADER(ERROR, "construct mock acceptor response failed");
    } else if (p_election_->is_rhs_message_higher_(mock_self_accept_response_msg,
                                                   accept_res,
                                                   higher_than_leader_reason,
                                                   false,
                                                   LogPhase::RENEW_LEASE)) {// 比Leader的优先级要高，此时不比较IP
      highest_priority_cache_.check_expired();
      if (!highest_priority_cache_.cached_msg_.is_valid()) {
        highest_priority_cache_.set(accept_res, higher_than_leader_reason);
        LOG_CHANGE_LEADER(INFO, "follower priority is higher than leader", K(higher_than_leader_reason));
      } else if (p_election_->is_rhs_message_higher_(highest_priority_cache_.cached_msg_, accept_res, higher_than_cached_msg_reason, true, LogPhase::RENEW_LEASE)) {// 比缓存的最高消息的优先级要高， 此时比较IP
        LOG_CHANGE_LEADER(INFO, "highest_priority_cache_ will be replaced", K(highest_priority_cache_), K(higher_than_cached_msg_reason), K(higher_than_leader_reason));
        highest_priority_cache_.set(accept_res, higher_than_cached_msg_reason);
      } else if (highest_priority_cache_.cached_msg_.get_sender() == accept_res.get_sender()) {// 缓存的最高优先级的消息的副本第二次响应Leader时触发切主
        if (CLICK_FAIL(prepare_change_leader_to_(highest_priority_cache_.cached_msg_.get_sender(),
                                                 higher_than_leader_reason))) {
          LOG_CHANGE_LEADER(WARN, "fail to prepare change leader", K(highest_priority_cache_), K(higher_than_leader_reason));
        }
      }
    }
  }
  #undef PRINT_WRAPPER
}

int ElectionProposer::prepare_change_leader_to_(const ObAddr &dest_addr, const ObStringHolder &reason)
{
  ELECT_TIME_GUARD(50_ms);
  #define PRINT_WRAPPER KR(ret), K(dest_addr), K(reason), K(*this)
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(p_election_->priority_) && p_election_->priority_->has_fatal_failure()) {// FATAL failure的切主绕过RCS，不等上层做准备
    const ObAddr &dest_addr = highest_priority_cache_.cached_msg_.get_sender();
    p_election_->event_recorder_.report_directly_change_leader_event(dest_addr, reason);
    if (CLICK_FAIL(change_leader_to(dest_addr))) {
      LOG_CHANGE_LEADER(WARN, "call direct change leader failed when meet fatal failure", K(highest_priority_cache_), K(dest_addr));
    } else {
      LOG_CHANGE_LEADER(INFO, "call direct change leader success when meet fatal failure", K(highest_priority_cache_), K(dest_addr));
    }
  } else if (!p_election_->prepare_change_leader_cb_.is_valid()) {
    LOG_CHANGE_LEADER(ERROR, "prepare_change_leader_cb_ is not valid", K(reason));
  } else {
    p_election_->event_recorder_.report_prepare_change_leader_event(dest_addr, reason);
    if (CLICK_FAIL(p_election_->prepare_change_leader_cb_(p_election_->id_, dest_addr))) {
      LOG_CHANGE_LEADER(ERROR, "commit change leader task failed", K(reason));
    } else {
      LOG_CHANGE_LEADER(INFO, "commit change leader task success", K(reason));
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

void ElectionProposer::inner_change_leader_to(const ObAddr &dst)
{
  ELECT_TIME_GUARD(500_ms);
  #define PRINT_WRAPPER KR(ret), K(change_leader_msg), K(dst), K(*this)
  int ret = OB_SUCCESS;
  // 发出切主消息要带上旧主的ballot number
  int64_t switch_source_leader_ballot = ballot_number_;
  // 旧主要推高自己的ballot number，把飘在空中的accept_res都挡住，否则有正确性问题
  advance_ballot_number_and_reset_related_states_(ballot_number_ + 1,
                                                  "change leader");
  leader_lease_and_epoch_.reset();
  ElectionChangeLeaderMsg change_leader_msg(p_election_->id_,
                                            p_election_->get_self_addr(),
                                            restart_counter_,
                                            ballot_number_,
                                            p_election_->get_ls_biggest_min_cluster_version_ever_seen_(),
                                            switch_source_leader_ballot,
                                            p_election_->get_membership_version_());
  if (OB_LIKELY(leader_revoke_if_lease_expired_(RoleChangeReason::ChangeLeaderToRevoke))) {
    LOG_CHANGE_LEADER(INFO, "change leader, old leader revoke");
    p_election_->event_recorder_.report_change_leader_to_revoke_event(dst);
  } else {
    LOG_CHANGE_LEADER(ERROR, "change leader, old leader revoke failed, which should not happen");
  }
  change_leader_msg.set_receiver(dst);
  if (CLICK_FAIL(p_election_->send_(change_leader_msg))) {
    LOG_CHANGE_LEADER(ERROR, "send change leader msg failed");
  } else {
    LOG_CHANGE_LEADER(INFO, "change leader, old leader revoke");
  }
  #undef PRINT_WRAPPER
}

void ElectionProposer::on_change_leader(const ElectionChangeLeaderMsg &change_leader_msg)
{
  ELECT_TIME_GUARD(500_ms);
  // 新主收到切主消息后，进行一次Leader Prepare
  #define PRINT_WRAPPER K(change_leader_msg), K(*this)
  LOG_CHANGE_LEADER(INFO, "handle change leader message");
  bool accept = false;
  if (change_leader_msg.get_sender() == p_election_->get_self_addr()) {// 自己切给自己的
    if (change_leader_msg.get_ballot_number() == ballot_number_) {
      accept = true;
    } else {
      LOG_CHANGE_LEADER_RET(WARN, OB_ERR_UNEXPECTED, "change leader to self msg's ballot number not expected");
    }
  } else {// 别人切给自己的
    if (change_leader_msg.get_ballot_number() > ballot_number_) {
      accept = true;
    } else {
      LOG_CHANGE_LEADER_RET(WARN, OB_ERR_UNEXPECTED, "change leader msg's ballot number is too small");
    }
  }
  if (!accept) {
    LOG_CHANGE_LEADER_RET(WARN, OB_ERR_UNEXPECTED, "change leader msg not accepted");
  } else if (change_leader_msg.get_membership_version() > memberlist_with_states_.
                                                     get_member_list().
                                                     get_membership_version()) {
    LOG_CHANGE_LEADER_RET(WARN, OB_ERR_UNEXPECTED, "change leader msg's membership version is larger than self");
  } else {
    advance_ballot_number_and_reset_related_states_(change_leader_msg.get_ballot_number(),
                                                    "receive change leader message");
    switch_source_leader_ballot_ = change_leader_msg.get_old_ballot_number();
    switch_source_leader_addr_ = change_leader_msg.get_sender();
    prepare(ObRole::LEADER);
    LOG_CHANGE_LEADER(INFO, "receive change leader msg, do leader prepare");
  }
  #undef PRINT_WRAPPER
}

int64_t ElectionProposer::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (OB_NOT_NULL(p_election_)) {
    common::databuff_printf(buf, buf_len, pos, "{ls_id:{id:%ld}", p_election_->id_);
    common::databuff_printf(buf, buf_len, pos, ", addr:%s", to_cstring(p_election_->get_self_addr()));
  }
  common::databuff_printf(buf, buf_len, pos, ", role:%s", obj_to_string(role_));
  common::databuff_printf(buf, buf_len, pos, ", ballot_number:%ld", ballot_number_);
  if (prepare_success_ballot_ != INVALID_VALUE) {
    common::databuff_printf(buf, buf_len, pos, ", prepare_success_ballot:%ld", prepare_success_ballot_);
  }
  int64_t lease = record_lease_interval_;
  if (lease > 1_s) {
    common::databuff_printf(buf, buf_len, pos, ", lease_interval:%.2lfs",lease * 1.0 / 1_s);
  } else if (lease > 1_ms) {
    common::databuff_printf(buf, buf_len, pos, ", lease_interval:%.2lfms", lease * 1.0 / 1_ms);
  } else {
    common::databuff_printf(buf, buf_len, pos, ", lease_interval:%ldus", lease);
  }
  common::databuff_printf(buf, buf_len, pos, ", memberlist_with_states:%s",
                                                to_cstring(memberlist_with_states_));
  if (leader_lease_and_epoch_.is_valid()) {// 非有效的leader不打印lease信息
    common::databuff_printf(buf, buf_len, pos, ", lease_and_epoch:%s",
                                                  to_cstring(leader_lease_and_epoch_));
  }
  if (switch_source_leader_ballot_ != INVALID_VALUE) {// 该变量与切主相关，只在切主过程中打印该变量
    common::databuff_printf(buf, buf_len, pos, ", switch_source_leader_ballot:%ld",
                                                  switch_source_leader_ballot_);
  }
  if (switch_source_leader_addr_.is_valid()) {// 该变量与切主相关，只在切主过程中打印该变量
    common::databuff_printf(buf, buf_len, pos, ", switch_source_leader_addr:%s",
                                                  to_cstring(switch_source_leader_addr_));
  }
  common::databuff_printf(buf, buf_len, pos, ", priority_seed:0x%lx", (unsigned long)p_election_->inner_priority_seed_);
  common::databuff_printf(buf, buf_len, pos, ", restart_counter:%ld", restart_counter_);
  common::databuff_printf(buf, buf_len, pos, ", last_do_prepare_ts:%s", ObTime2Str::ob_timestamp_str_range<YEAR, USECOND>(last_do_prepare_ts_));
  if (OB_NOT_NULL(p_election_)) {
    common::databuff_printf(buf, buf_len, pos, ", self_priority:%s", p_election_->priority_ == nullptr ? "NULL" : to_cstring(*(p_election_->priority_)));
  }
  common::databuff_printf(buf, buf_len, pos, ", p_election:0x%lx}", (unsigned long)p_election_);
  return pos;
}

int ElectionProposer::revoke(const RoleChangeReason &reason)
{
  ELECT_TIME_GUARD(500_ms);
  #define PRINT_WRAPPER K(*this)
  int ret = OB_SUCCESS;
  if (!check_leader()) {
    ret = OB_NOT_MASTER;
    LOG_NONE(WARN, "i am not leader, but someone ask me to revoke", K(lbt()));
  }
  leader_lease_and_epoch_.reset();
  if (!leader_revoke_if_lease_expired_(reason)) {
    LOG_NONE(WARN, "somethig wrong when revoke", K(lbt()));
  }
  return ret;
  #undef PRINT_WRAPPER
}

bool ElectionProposer::is_self_in_memberlist_() const
{
  bool ret = false;
  const ObArray<ObAddr> &addr_list = memberlist_with_states_.get_member_list().get_addr_list();
  for (int64_t idx = 0; idx < addr_list.count(); ++idx) {
    if (addr_list[idx] == p_election_->self_addr_) {
      ret = true;
      break;
    }
  }
  return ret;
}

}
}
}
