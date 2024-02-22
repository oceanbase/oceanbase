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

#include "lib/list/ob_dlist.h"
#include "observer/ob_server.h"
#include "share/ob_errno.h"
#include "share/ob_occam_time_guard.h"
#include "election_impl.h"
#include "lib/ob_errno.h"
#include "logservice/palf/election/interface/election.h"
#include "share/rc/ob_tenant_base.h"
#include "logservice/palf/election/utils/election_common_define.h"
namespace oceanbase
{
using namespace share;
namespace palf
{
namespace election
{

int64_t MAX_TST = 1_s;
int64_t INIT_TS = -1;
ObOccamTimer GLOBAL_REPORT_TIMER;

void DefaultRoleChangeCallBack::operator()(ElectionImpl *election,
                                           common::ObRole before,
                                           common::ObRole after,
                                           RoleChangeReason reason)
{
  const char *before_str = obj_to_string(before);
  const char *after_str = obj_to_string(after);
  const char *reason_str = obj_to_string(reason);
  ELECT_LOG(INFO, "this is default role change callback", K(obj_to_string(before)),
                                                             K(obj_to_string(after)),
                                                             K(obj_to_string(reason)),
                                                             KPC(election));
  switch (reason) {
  case RoleChangeReason::DevoteToBeLeader:
    election->event_recorder_.report_decentralized_to_be_leader_event(election->proposer_.memberlist_with_states_);
    break;
  case RoleChangeReason::ChangeLeaderToBeLeader:
    election->event_recorder_.report_change_leader_to_takeover_event(election->proposer_.switch_source_leader_addr_);
    break;
  case RoleChangeReason::LeaseExpiredToRevoke:
    election->event_recorder_.report_leader_lease_expired_event(election->proposer_.memberlist_with_states_);
    break;
  default:
    break;
  }
}

ElectionImpl::ElectionImpl()
    : is_inited_(false),
      is_running_(false),
      lock_(common::ObLatchIds::ELECTION_LOCK),
      proposer_(this),
      acceptor_(this),
      priority_(nullptr),
      msg_handler_(nullptr),
      ls_biggest_min_cluster_version_ever_seen_(0),
      event_recorder_(id_, self_addr_, timer_)
{}

ElectionImpl::~ElectionImpl()
{
  #define PRINT_WRAPPER K(*this)
  if (is_running_) {
    stop();
  }
  is_inited_ = false;
  LOG_DESTROY(INFO, "election destroyed");
  #undef PRINT_WRAPPER
}

int ElectionImpl::init_and_start(const int64_t id,
                                 common::ObOccamTimer *election_timer,
                                 ElectionMsgSender *msg_handler,
                                 const common::ObAddr &self_addr,
                                 const uint64_t inner_priority_seed,/*smaller value has higher priority*/
                                 const int64_t restart_counter,
                                 const ObFunction<int(const int64_t, const ObAddr &)> &prepare_change_leader_cb,
                                 const ObFunction<void(ElectionImpl *, common::ObRole, common::ObRole, RoleChangeReason)> &role_change_cb)
{
  ELECT_TIME_GUARD(500_ms);
  #define PRINT_WRAPPER KR(ret), K(*this), KP(msg_handler)
  int ret = OB_SUCCESS;
  CHECK_ELECTION_ARGS(msg_handler, election_timer);
  LockGuard lock_guard(lock_);
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_INIT(ERROR, "init proposer twice");
  } else if (CLICK_FAIL(proposer_.init(restart_counter))) {
    LOG_INIT(ERROR, "init proposer failed");
  } else if (CLICK_FAIL(prepare_change_leader_cb_.assign(prepare_change_leader_cb))) {
    LOG_INIT(ERROR, "assign call back failed");
  } else if (CLICK_FAIL(role_change_cb_.assign(role_change_cb))) {
    LOG_INIT(ERROR, "assign call back failed");
  } else {
    timer_ = election_timer;
    id_ = id;
    msg_handler_ = msg_handler;
    self_addr_ = self_addr;
    inner_priority_seed_ = inner_priority_seed;
    if (CLICK_FAIL(proposer_.start())) {
      LOG_INIT(ERROR, "proposer start failed");
    } else if (CLICK_FAIL(acceptor_.start())) {
      LOG_INIT(ERROR, "start acceptor failed");
    } else {
      if (inner_priority_seed_ & static_cast<uint64_t>(PRIORITY_SEED_BIT::SEED_NOT_NORMOL_REPLICA_BIT)) {
        event_recorder_.set_need_report(false);
      }
      is_inited_ = true;
      is_running_ = true;
      LOG_INIT(INFO, "election init and start");
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

void ElectionImpl::stop()
{
  ELECT_TIME_GUARD(3_s);
  #define PRINT_WRAPPER KR(ret), K(*this)
  int ret = OB_SUCCESS;
  bool can_stop = false;
  {
    LockGuard lock_guard(lock_);
    priority_ = nullptr;
    if (OB_UNLIKELY(!is_inited_ || !is_running_)) {
      ret = OB_NOT_RUNNING;
      LOG_DESTROY(WARN, "election is not running or not inited");
    } else {
      can_stop = true;
      is_running_ = false;
      LOG_DESTROY(INFO, "election stopped");
    }
  }
  if (can_stop) {
    // stop的时候会停定时任务，如果定时任务已经开始则要等定时任务结束
    // 在定时任务中加了election的锁，如果在锁的保护下停定时任务则有可能死锁
    proposer_.stop();
    acceptor_.stop();
  }
  #undef PRINT_WRAPPER
}

int ElectionImpl::can_set_memberlist(const palf::LogConfigVersion &new_config_version) const
{
  ELECT_TIME_GUARD(500_ms);
  #define PRINT_WRAPPER KR(ret), K(*this), K(new_config_version)
  int ret = common::OB_SUCCESS;
  CHECK_ELECTION_ARGS(new_config_version);
  LockGuard lock_guard(lock_);
  CHECK_ELECTION_INIT();
  if (CLICK_FAIL(proposer_.can_set_memberlist(new_config_version))) {
    LOG_SET_MEMBER(WARN, "can_set_memberlist failed");
  }
  return ret;
  #undef PRINT_WRAPPER
}

int ElectionImpl::set_memberlist(const MemberList &new_memberlist)
{
  ELECT_TIME_GUARD(500_ms);
  #define PRINT_WRAPPER KR(ret), K(*this), K(new_memberlist)
  int ret = common::OB_SUCCESS;
  CHECK_ELECTION_ARGS(new_memberlist);
  LockGuard lock_guard(lock_);
  CHECK_ELECTION_INIT();
  if (CLICK_FAIL(proposer_.set_member_list(new_memberlist))) {
    LOG_SET_MEMBER(WARN, "update memberlist with states failed");
  }
  return ret;
  #undef PRINT_WRAPPER
}

int ElectionImpl::change_leader_to(const common::ObAddr &dest_addr)
{
  ELECT_TIME_GUARD(500_ms);
  #define PRINT_WRAPPER KR(ret), K(*this), K(dest_addr)
  int ret = common::OB_SUCCESS;
  CHECK_ELECTION_ARGS(dest_addr);
  LockGuard lock_guard(lock_);
  CHECK_ELECTION_INIT();
  if (CLICK_FAIL(proposer_.change_leader_to(dest_addr))) {
    LOG_CHANGE_LEADER(WARN, "change leader to failed");
  } else {
    LOG_CHANGE_LEADER(INFO, "change leader to", K(lbt()));
  }
  return ret;
  #undef PRINT_WRAPPER
}

int ElectionImpl::set_priority(ElectionPriority *priority)
{
  ELECT_TIME_GUARD(500_ms);
  #define PRINT_WRAPPER KR(ret), K(*this), KPC(priority)
  int ret = OB_SUCCESS;
  CHECK_ELECTION_ARGS(priority);
  LockGuard lock_guard(lock_);
  if (OB_NOT_NULL(priority_)) {
    ret = OB_ENTRY_EXIST;
    LOG_NONE(ERROR, "priority has been setted");
  } else {
    priority_ = priority;
  }
  return ret;
  #undef PRINT_WRAPPER
}

int ElectionImpl::reset_priority()
{
  ELECT_TIME_GUARD(500_ms);
  LockGuard lock_guard(lock_);
  CHECK_ELECTION_INIT();
  priority_ = nullptr;
  return OB_SUCCESS;
}

void ElectionImpl::handle_message_base_(const ElectionMsgBase &message_base)
{
  ls_biggest_min_cluster_version_ever_seen_.try_advance(message_base.get_ls_biggest_min_cluster_version_ever_seen().version_);
}

int ElectionImpl::handle_message(const ElectionPrepareRequestMsg &msg)
{
  const_cast<ElectionPrepareRequestMsg &>(msg).set_process_ts();
  ELECT_TIME_GUARD(500_ms);
  #define PRINT_WRAPPER KR(ret), K(*this), K(msg), K(need_register_devote_task)
  int ret = common::OB_SUCCESS;
  bool need_register_devote_task = false;
  {
    LockGuard lock_guard(lock_);
    handle_message_base_(msg);
    msg_counter_.add_received_count(msg);
    CHECK_ELECTION_INIT_AND_START();
    if (msg.get_sender() != self_addr_) {
      proposer_.on_prepare_request(msg, &need_register_devote_task);
    }
    acceptor_.on_prepare_request(msg);
  }
  if (need_register_devote_task) {
    if (CLICK_FAIL(proposer_.reschedule_or_register_prepare_task_after_(CALCULATE_MAX_ELECT_COST_TIME()))) {
      LOG_NONE(ERROR, "register devote task failed");
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

int ElectionImpl::handle_message(const ElectionAcceptRequestMsg &msg)
{
  const_cast<ElectionAcceptRequestMsg &>(msg).set_process_ts();
  ELECT_TIME_GUARD(500_ms);
  #define PRINT_WRAPPER KR(ret), K(msg), K(us_to_expired)
  int ret = common::OB_SUCCESS;
  int64_t us_to_expired = 0;
  {
    LockGuard lock_guard(lock_);
    handle_message_base_(msg);
    msg_counter_.add_received_count(msg);
    CHECK_ELECTION_INIT_AND_START();
    if (msg.get_ballot_number() > proposer_.ballot_number_) {
      proposer_.advance_ballot_number_and_reset_related_states_(msg.get_ballot_number(),
                                                                "receive bigger accept request");
    }
    acceptor_.on_accept_request(msg, &us_to_expired);
  }
  if (OB_LIKELY(us_to_expired > 0)) {
    if (us_to_expired - CALCULATE_TRIGGER_ELECT_WATER_MARK() < 0) {
      LOG_NONE(WARN, "reschedule devote task in invalid us", K(us_to_expired - 2 * MAX_TST));
    } else if (CLICK_FAIL(proposer_.reschedule_or_register_prepare_task_after_(us_to_expired - CALCULATE_TRIGGER_ELECT_WATER_MARK()))) {
      LOG_NONE(ERROR, "register devote task failed");
    } else {
      LOG_NONE(DEBUG, "reschedule devote task after", K(us_to_expired - 2 * MAX_TST));
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

int ElectionImpl::handle_message(const ElectionPrepareResponseMsg &msg)
{
  const_cast<ElectionPrepareResponseMsg &>(msg).set_process_ts();
  ELECT_TIME_GUARD(500_ms);
  int ret = OB_SUCCESS;
  LockGuard lock_guard(lock_);
  handle_message_base_(msg);
  msg_counter_.add_received_count(msg);
  CHECK_ELECTION_INIT_AND_START();
  proposer_.on_prepare_response(msg);
  return ret;
}

int ElectionImpl::handle_message(const ElectionAcceptResponseMsg &msg)
{
  const_cast<ElectionAcceptResponseMsg &>(msg).set_process_ts();
  ELECT_TIME_GUARD(500_ms);
  int ret = OB_SUCCESS;
  LockGuard lock_guard(lock_);
  handle_message_base_(msg);
  msg_counter_.add_received_count(msg);
  CHECK_ELECTION_INIT_AND_START();
  proposer_.on_accept_response(msg);
  return ret;
}

int ElectionImpl::handle_message(const ElectionChangeLeaderMsg &msg)
{
  const_cast<ElectionChangeLeaderMsg &>(msg).set_process_ts();
  ELECT_TIME_GUARD(500_ms);
  int ret = OB_SUCCESS;
  LockGuard lock_guard(lock_);
  handle_message_base_(msg);
  msg_counter_.add_received_count(msg);
  CHECK_ELECTION_INIT_AND_START();
  proposer_.on_change_leader(msg);
  return ret;
}

const common::ObAddr &ElectionImpl::get_self_addr() const
{
  return self_addr_;
}

ElectionPriority *ElectionImpl::get_priority_() const { return priority_; }

void ElectionImpl::refresh_priority_()
{
  ELECT_TIME_GUARD(500_ms);
  int ret = OB_SUCCESS;
  if (inner_priority_seed_ & static_cast<uint64_t>(PRIORITY_SEED_BIT::SEED_NOT_NORMOL_REPLICA_BIT)) {
    // do nothing
  } else if (OB_ISNULL(priority_)) {
    ELECT_LOG(INFO, "priority is null", K(*this));
  } else if (CLICK_FAIL(priority_->refresh())) {
    ELECT_LOG(WARN, "refresh priority failed", KR(ret), K(*this));
  }
}

bool ElectionImpl::is_member_list_valid_() const
{
  ELECT_TIME_GUARD(500_ms);
  return proposer_.memberlist_with_states_.get_member_list().is_valid();
}

LogConfigVersion ElectionImpl::get_membership_version_() const
{
  ELECT_TIME_GUARD(500_ms);
  return proposer_.memberlist_with_states_.get_member_list().get_membership_version();
}

int ElectionImpl::broadcast_(const ElectionPrepareRequestMsg &msg,
                             const common::ObArray<common::ObAddr> &list) const
{
  ELECT_TIME_GUARD(500_ms);
  #define PRINT_WRAPPER KR(ret), K(*this), K(msg)
  int ret = common::OB_SUCCESS;
  if (CLICK_FAIL(msg_handler_->broadcast(msg, list))) {
    LOG_NONE(WARN, "broadcast msg failed");
  } else {
    for (int64_t idx = 0; idx < list.count(); ++idx) {
      const_cast<election::ElectionPrepareRequestMsg *>(&msg)->set_receiver(list.at(idx));
      msg_counter_.add_send_count(msg);
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

int ElectionImpl::broadcast_(const ElectionAcceptRequestMsg &msg,
                             const common::ObArray<common::ObAddr> &list) const
{
  ELECT_TIME_GUARD(500_ms);
  #define PRINT_WRAPPER KR(ret), K(*this), K(msg)
  int ret = common::OB_SUCCESS;
  if (CLICK_FAIL(msg_handler_->broadcast(msg, list))) {
    LOG_NONE(WARN, "broadcast msg failed");
  } else {
    for (int64_t idx = 0; idx < list.count(); ++idx) {
      const_cast<election::ElectionAcceptRequestMsg *>(&msg)->set_receiver(list.at(idx));
      msg_counter_.add_send_count(msg);
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

int ElectionImpl::send_(const ElectionPrepareResponseMsg &msg) const
{
  ELECT_TIME_GUARD(500_ms);
  #define PRINT_WRAPPER KR(ret), K(*this), K(msg)
  int ret = common::OB_SUCCESS;
  if (CLICK_FAIL(msg_handler_->send(msg))) {
    LOG_NONE(WARN, "send msg failed");
  } else {
    msg_counter_.add_send_count(msg);
  }
  return ret;
  #undef PRINT_WRAPPER
}

int ElectionImpl::send_(const ElectionAcceptResponseMsg &msg) const
{
  ELECT_TIME_GUARD(500_ms);
  #define PRINT_WRAPPER KR(ret), K(*this), K(msg)
  int ret = common::OB_SUCCESS;
  if (CLICK_FAIL(msg_handler_->send(msg))) {
    LOG_NONE(WARN, "send msg failed");
  } else {
    msg_counter_.add_send_count(msg);
  }
  return ret;
  #undef PRINT_WRAPPER
}

int ElectionImpl::send_(const ElectionChangeLeaderMsg &msg) const
{
  ELECT_TIME_GUARD(500_ms);
  #define PRINT_WRAPPER KR(ret), K(*this), K(msg)
  int ret = common::OB_SUCCESS;
  if (CLICK_FAIL(msg_handler_->send(msg))) {
    LOG_NONE(WARN, "send msg failed");
  } else {
    msg_counter_.add_send_count(msg);
  }
  return ret;
  #undef PRINT_WRAPPER
}

int ElectionImpl::temporarily_downgrade_protocol_priority(const int64_t time_us, const char *reason)
{
  #define PRINT_WRAPPER KR(ret), K(*this), K(time_us), K(reason)
  ELECT_TIME_GUARD(500_ms);
  int ret = OB_SUCCESS;
  LockGuard lock_guard(lock_);
  CHECK_ELECTION_INIT();
  temporarily_downgrade_priority_info_.downgrade_expire_ts_ = get_monotonic_ts() + time_us;
  temporarily_downgrade_priority_info_.interval_ = time_us;
  temporarily_downgrade_priority_info_.reason_ = reason;
  LOG_NONE(WARN, "temporarily downgrade protocol priority");
  return ret;
  #undef PRINT_WRAPPER
}

int ElectionImpl::add_inner_priority_seed_bit(const PRIORITY_SEED_BIT new_bit)
{
  #define PRINT_WRAPPER KR(ret), K(*this), K(new_bit)
  int ret = OB_SUCCESS;
  ELECT_TIME_GUARD(500_ms);
  LockGuard lock_guard(lock_);
  CLICK();
  CHECK_ELECTION_INIT();
  if (inner_priority_seed_ & static_cast<uint64_t>(new_bit)) {
    ret = OB_ENTRY_EXIST;
    LOG_NONE(WARN, "set inner priority seed with new bit failed, caus it already exist");
  } else {
    inner_priority_seed_ |= static_cast<uint64_t>(new_bit);
  }
  return ret;
  #undef PRINT_WRAPPER
}

int ElectionImpl::clear_inner_priority_seed_bit(const PRIORITY_SEED_BIT old_bit)
{
  #define PRINT_WRAPPER KR(ret), K(*this), K(old_bit)
  int ret = OB_SUCCESS;
  ELECT_TIME_GUARD(500_ms);
  LockGuard lock_guard(lock_);
  CLICK();
  CHECK_ELECTION_INIT();
  if (!(inner_priority_seed_ | static_cast<uint64_t>(old_bit))) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_NONE(WARN, "clear inner priority seed with old bit failed, caus it not exist");
  } else {
    inner_priority_seed_ &= (~static_cast<uint64_t>(old_bit));
  }
  return ret;
  #undef PRINT_WRAPPER
}

int ElectionImpl::set_inner_priority_seed(const uint64_t seed)
{
  int ret = OB_SUCCESS;
  ELECT_TIME_GUARD(500_ms);
  LockGuard lock_guard(lock_);
  CLICK();
  inner_priority_seed_ = seed;
  return ret;
}

uint64_t ElectionImpl::get_ls_biggest_min_cluster_version_ever_seen_() const
{
  #define PRINT_WRAPPER K(*this)
  int ret = OB_SUCCESS;
  uint64_t ls_biggest_min_cluster_version_ever_seen = 0;
  if (observer::ObServer::get_instance().is_arbitration_mode()) {
#ifdef OB_BUILD_ARBITRATION
    if (CLUSTER_CURRENT_VERSION < ls_biggest_min_cluster_version_ever_seen_.version_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_NONE(ERROR, "ls_biggest_min_cluster_version_ever_seen_ greater than arb binary version");
    } else if (ls_biggest_min_cluster_version_ever_seen_.version_ == 0) {
      LOG_NONE(WARN, "ls_biggest_min_cluster_version_ever_seen_ not setted yet");
    }
    ls_biggest_min_cluster_version_ever_seen = ls_biggest_min_cluster_version_ever_seen_.version_;
#endif
  } else {
    ls_biggest_min_cluster_version_ever_seen = std::max(GET_MIN_CLUSTER_VERSION(),
                                                        ls_biggest_min_cluster_version_ever_seen_.version_);
  }
  return ls_biggest_min_cluster_version_ever_seen;
  #undef PRINT_WRAPPER
}

uint64_t ElectionImpl::generate_inner_priority_seed_() const
{
  uint64_t priority_seed = inner_priority_seed_;
  if (get_monotonic_ts() < temporarily_downgrade_priority_info_.downgrade_expire_ts_) {
    priority_seed |= (uint64_t)PRIORITY_SEED_BIT::SEED_TEMORARILY_DOWNGRADE_PRIORIY_BIT;
  }
  return priority_seed;
}

}
}
}
