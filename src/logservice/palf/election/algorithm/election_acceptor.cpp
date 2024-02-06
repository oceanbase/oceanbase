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

#include "share/ob_occam_time_guard.h"
#include "election_acceptor.h"
#include "common/ob_clock_generator.h"
#include "election_impl.h"
#include "lib/net/ob_addr.h"
#include "logservice/palf/election/interface/election_priority.h"
#include "logservice/palf/election/utils/election_common_define.h"
#include "logservice/palf/election/utils/election_event_recorder.h"

namespace oceanbase
{
namespace palf
{
namespace election
{

#define CHECK_SILENCE()\
do {\
  if (ATOMIC_LOAD(&INIT_TS) < 0) {\
    ELECT_LOG_RET(ERROR, common::OB_ERROR, "INIT_TS is less than 0, may not call GLOBAL_INIT_ELECTION_MODULE yet!", K(*this));\
    return;\
  } else if (OB_UNLIKELY(get_monotonic_ts() < ATOMIC_LOAD(&INIT_TS) + MAX_LEASE_TIME)) {\
    ELECT_LOG(INFO, "keep silence for safty, won't send response", K(*this));\
    return;\
  }\
} while(0)

template <typename Type>
struct ResponseType {};

template <>
struct ResponseType<ElectionPrepareRequestMsg> { using type = ElectionPrepareResponseMsg; };

template <>
struct ResponseType<ElectionAcceptRequestMsg> { using type = ElectionAcceptResponseMsg; };

class RequestChecker
{
private:
public:
  template <typename RequestMsg, typename Acceptor>
  static bool check_ballot_valid(const RequestMsg &msg, Acceptor *p_acceptor, const LogPhase phase)
  {
    ELECT_TIME_GUARD(500_ms);
    #define PRINT_WRAPPER K(msg), K(*p_acceptor)
    bool ret = false;
    if (OB_UNLIKELY(msg.get_ballot_number() < p_acceptor->ballot_number_)) {
      using T = typename ResponseType<RequestMsg>::type;
      T reject_msg = create_reject_message_(p_acceptor->p_election_->get_self_addr(),
                                            p_acceptor->p_election_->inner_priority_seed_,
                                            p_acceptor->p_election_->get_membership_version_(),
                                            p_acceptor->p_election_->get_ls_biggest_min_cluster_version_ever_seen_(),
                                            msg);
      reject_msg.set_rejected(p_acceptor->ballot_number_);
      p_acceptor->p_election_->send_(reject_msg);
      LOG_PHASE(WARN, phase, "receive old ballot request, refused");
    } else {
      ret = true;
    }
    return ret;
    #undef PRINT_WRAPPER
  }
private:
  static ElectionPrepareResponseMsg create_reject_message_(const common::ObAddr &addr,
                                                           const uint64_t inner_priority_seed,
                                                           const LogConfigVersion &membership_version,
                                                           const LsBiggestMinClusterVersionEverSeen &version,
                                                           const ElectionPrepareRequestMsg &msg)
  {
    UNUSED(inner_priority_seed),
    UNUSED(membership_version);
    return ElectionPrepareResponseMsg(addr, version, msg);
  }
  static ElectionAcceptResponseMsg create_reject_message_(const common::ObAddr &addr,
                                                          const uint64_t inner_priority_seed,
                                                          const LogConfigVersion &membership_version,
                                                          const LsBiggestMinClusterVersionEverSeen &version,
                                                          const ElectionAcceptRequestMsg &msg)
  {
    return ElectionAcceptResponseMsg(addr, inner_priority_seed, membership_version, version, msg);
  }
};

ElectionAcceptor::ElectionAcceptor(ElectionImpl *p_election) :
ballot_number_(INVALID_VALUE),
ballot_of_time_window_(INVALID_VALUE),
is_time_window_opened_(false),
p_election_(p_election),
last_time_window_open_ts_(INVALID_VALUE),
last_dump_acceptor_info_ts_(INVALID_VALUE) {}

void ElectionAcceptor::advance_ballot_number_and_reset_related_states_(const int64_t new_ballot_number,
                                                                       const LogPhase phase)
{
  ELECT_TIME_GUARD(500_ms);
  #define PRINT_WRAPPER K(new_ballot_number), K(*this)
  if (new_ballot_number > ballot_number_) {
    ballot_number_ = new_ballot_number;
    ballot_of_time_window_ = ballot_number_;
    reset_time_window_states_(phase);
  } else {
    LOG_PHASE_RET(ERROR, OB_INVALID_ARGUMENT, phase, "invalid argument");
  }
  #undef PRINT_WRAPPER
}

int ElectionAcceptor::start()
{
  ObAddr last_record_lease_owner;
  bool last_record_lease_valid_state = false;
  int ret = OB_SUCCESS;
  return p_election_->timer_->schedule_task_repeat(time_window_task_handle_,
                                                   250_ms,
                                                   [this,
                                                    last_record_lease_owner,
                                                    last_record_lease_valid_state]() mutable {
    ELECT_TIME_GUARD(500_ms);
    #define PRINT_WRAPPER KR(ret), K(*this)
    int ret = OB_SUCCESS;
    
    LockGuard lock_guard(p_election_->lock_);
    // 周期性打印选举的状态
    if (ObClockGenerator::getCurrentTime() > last_dump_acceptor_info_ts_ + 3_s) {
      last_dump_acceptor_info_ts_ = ObClockGenerator::getCurrentTime();
      ELECT_LOG(INFO, "dump acceptor info", K(*this));
    }
    // 当acceptor的Lease有效状态发生变化时需要打印日志以及汇报事件
    bool lease_valid_state = !lease_.is_expired();
    if (last_record_lease_valid_state != lease_valid_state) {// 当记录的lease有效状态与当前Lease的有效状态不符时
      if (lease_valid_state) {// Lease从无效变有效，意味着该acceptor【可能】见证了一个新的Leader诞生
        LOG_ELECT_LEADER(INFO, "witness new leader");
      } else {// Lease从有效变无效，意味着本副本可能与Leader失去了网络连接
        LOG_RENEW_LEASE(WARN, "lease expired");
        p_election_->event_recorder_.report_acceptor_lease_expired_event(lease_);
        lease_.reset();
      }
    }
    // 当acceptor发现Lease的owner发生变化的时候需要打印日志以及汇报事件
    if (last_record_lease_owner != lease_.get_owner()) {
      if (last_record_lease_owner.is_valid() && lease_.get_owner().is_valid()) {
        LOG_CHANGE_LEADER(INFO, "lease owner changed");
        p_election_->event_recorder_.report_acceptor_witness_change_leader_event(last_record_lease_owner, lease_.get_owner());
      }
      last_record_lease_owner = lease_.get_owner();
    }
    if (is_time_window_opened_) {
      ElectionPrepareResponseMsg prepare_res_accept(p_election_->get_self_addr(),
                                                    p_election_->get_ls_biggest_min_cluster_version_ever_seen_(),
                                                    highest_priority_prepare_req_);
      bool can_vote = false;
      if (last_record_lease_valid_state && !lease_valid_state) {// 这个定时任务可能是被延迟致lease到期时触发的，为了在lease到期的第一时间投票
        can_vote = true;
        LOG_ELECT_LEADER(INFO, "vote when lease expired");
      } else if (ObClockGenerator::getCurrentTime() - last_time_window_open_ts_ >= CALCULATE_TIME_WINDOW_SPAN_TS()) {
        can_vote = true;
      } else {
        LOG_ELECT_LEADER(INFO, "can't vote now", K(last_record_lease_valid_state),
                         K(lease_valid_state), K(CALCULATE_TIME_WINDOW_SPAN_TS()),
                         KTIME_(last_time_window_open_ts));
      }
      if (can_vote) {
        // 1. 定时任务触发时要检查是否重新开启过窗口，避免提前关闭新的窗口，同时检查是否有这一轮记录的prepare请求
        if (ballot_of_time_window_ == highest_priority_prepare_req_.get_ballot_number() &&
            ballot_of_time_window_ > ballot_number_) {
          // 1.1 要投票了，可以推大ballot number了
          ballot_number_ = ballot_of_time_window_;
          // 1.2 若lease过期需要重置lease，因为proposer没办法判断lease的有效性
          if (lease_.is_expired()) {// 若Lease过期，则返回空Lease（Lease是否过期只能在本机上进行判断）
            lease_.reset();
          }
          // 1.3 构造prepare ok消息
          prepare_res_accept.set_accepted(ballot_number_, lease_);
          if (CLICK_FAIL(p_election_->send_(prepare_res_accept))) {
            LOG_ELECT_LEADER(ERROR, "fail to send prepare ok", K(prepare_res_accept));
          } else {
            p_election_->event_recorder_.report_vote_event(prepare_res_accept.get_receiver(), vote_reason_);
            LOG_ELECT_LEADER(INFO, "time window closed, send vote", K(prepare_res_accept));
          }
        } else {
          LOG_ELECT_LEADER(ERROR, "give up sending prepare response, casuse ballot number not match", K(prepare_res_accept));
        }
        is_time_window_opened_ = false;// 发出消息后，关闭时间窗口
      }
    }
    last_record_lease_valid_state = lease_valid_state;
    #undef PRINT_WRAPPER
    return false;
  });
}

void ElectionAcceptor::stop()
{
  ELECT_TIME_GUARD(3_s);
  time_window_task_handle_.stop_and_wait();
}

void ElectionAcceptor::reset_time_window_states_(const LogPhase phase)
{
  ELECT_TIME_GUARD(500_ms);
  #define PRINT_WRAPPER K(*this)
  if (is_time_window_opened_) {
    is_time_window_opened_ = false;// 推大ballot number的时候要关闭时间窗口
    highest_priority_prepare_req_.reset();
    vote_reason_.reset();
    LOG_PHASE(INFO, phase, "time window closed, not vote");
  }
  #undef PRINT_WRAPPER
}

void ElectionAcceptor::on_prepare_request(const ElectionPrepareRequestMsg &prepare_req)
{
  ELECT_TIME_GUARD(500_ms);
  #define PRINT_WRAPPER KR(ret), K(prepare_req), K(*this)
  CHECK_SILENCE();// 启动后的要维持一段静默时间，acceptor假装看不到任何消息，以维护lease的正确语义
  int ret = OB_SUCCESS;
  LogPhase phase = (prepare_req.get_role() == common::ObRole::FOLLOWER ? LogPhase::ELECT_LEADER : LogPhase::RENEW_LEASE);
  LOG_PHASE(INFO, phase, "handle prepare request");
  if (OB_UNLIKELY(false == p_election_->is_member_list_valid_())) {
    LOG_PHASE(INFO, phase, "ignore prepare when member_list is invalid");
  } else if (prepare_req.get_membership_version() < p_election_->get_membership_version_()) {
    LOG_PHASE(INFO, phase, "ignore lower membership version request");
  } else if (OB_LIKELY(RequestChecker::check_ballot_valid(prepare_req, this, phase))) {
    // 0. 收到leader prepare的时候无须比较优先级，直接返回投票结果
    if (prepare_req.get_role() == common::ObRole::LEADER) {
      if (prepare_req.get_ballot_number() <= ballot_number_) {
        LOG_PHASE(WARN, phase, "leader prepare message's ballot number is smaller than self");
      } else {
        advance_ballot_number_and_reset_related_states_(prepare_req.get_ballot_number(), phase);
        ElectionPrepareResponseMsg prepare_res_accept(p_election_->get_self_addr(),
                                                      p_election_->get_ls_biggest_min_cluster_version_ever_seen_(),
                                                      prepare_req);
        prepare_res_accept.set_accepted(ballot_number_, lease_);
        if (CLICK_FAIL(p_election_->msg_handler_->send(prepare_res_accept))) {
          LOG_PHASE(WARN, phase, "send prepare response to leader prepare failed");
        } else {
          LOG_PHASE(INFO, phase, "receive valid leader prepare message, send vote to him");
        }
      }
    } else {
      // 1. 遇到比时间窗口更大的ballot number的时候需要关闭当前的时间窗口
      if (is_time_window_opened_ && prepare_req.get_ballot_number() > ballot_of_time_window_) {
        reset_time_window_states_(phase);
      }
      // 2. 若时间窗口未开启，需要开启时间窗口
      if (!is_time_window_opened_ && prepare_req.get_ballot_number() > ballot_of_time_window_) {
        ballot_of_time_window_ = prepare_req.get_ballot_number();
        highest_priority_prepare_req_.reset();
        LOG_PHASE(DEBUG, phase, "advance ballot_of_time_window_");
        int64_t ballot_of_time_window_when_registered = ballot_of_time_window_;
        int64_t timewindow_span = 0;
        if (!lease_.is_expired()) {// 当前Lease有效时，如果有效的时间超过一个最大单程消息延迟，则窗口关闭时机以Lease到期时间为准
          timewindow_span = std::max(lease_.get_lease_end_ts() - get_monotonic_ts(), CALCULATE_TIME_WINDOW_SPAN_TS() / 2);
        } else {// 否则视为普通的无主选举流程，窗口需要覆盖两个最大单程消息延迟
          timewindow_span = CALCULATE_TIME_WINDOW_SPAN_TS();
        }
        if (CLICK_FAIL(time_window_task_handle_.reschedule_after(timewindow_span))) {
          LOG_PHASE(ERROR, phase, "open time window failed");
        } else {
          is_time_window_opened_ = true;// 定时任务注册成功，打开时间窗口
          last_time_window_open_ts_ = ObClockGenerator::getCurrentTime();
          LOG_PHASE(INFO, phase, "open time window success", K(timewindow_span));
        }
      }
      // 3. 成员版本号是第一优先级，在成员版本号不小于自己的基础上要比较成员版本号的大小，否则将导致分票
      if (OB_SUCC(ret) && is_time_window_opened_) {// 在时间窗口内，进行计票
        if (prepare_req.get_ballot_number() != ballot_of_time_window_) {
          LOG_PHASE(INFO, phase, "prepare request's ballot is not same as time window, just ignore");
        } else if (!highest_priority_prepare_req_.is_valid()) {
          highest_priority_prepare_req_ = prepare_req;
          LOG_PHASE(INFO, phase, "highest priority prepare message will be replaced casuse cached highest prioriy message is invalid");
          vote_reason_.assign("the only request");
        } else if (prepare_req.get_membership_version() > highest_priority_prepare_req_.get_membership_version()) {
          highest_priority_prepare_req_ = prepare_req;
          LOG_PHASE(INFO, phase, "highest priority prepare message will be replaced casuse new message's membership version is higher");
          vote_reason_.assign("membership_version is higher");
        } else if (prepare_req.get_membership_version() < highest_priority_prepare_req_.get_membership_version()) {
          LOG_PHASE(INFO, phase, "prepare message's membership version not less than self, but not greater than cached highest priority prepare message");
        } else {
          // 4. 比较消息和缓存的最高优先级之间的高低
          if (p_election_->is_rhs_message_higher_(highest_priority_prepare_req_, prepare_req, vote_reason_, true, LogPhase::ELECT_LEADER)) {
            LOG_PHASE(INFO, phase, "highest priority prepare request will be replaced", K(vote_reason_));
            highest_priority_prepare_req_ = prepare_req;
          } else {
            LOG_PHASE(INFO, phase, "ignore prepare request, cause it has lower priority", K(vote_reason_));
          }
        }
      }
    }
  }
  #undef PRINT_WRAPPER
}

void ElectionAcceptor::on_accept_request(const ElectionAcceptRequestMsg &accept_req,
                                         int64_t *us_to_expired)
{
  ELECT_TIME_GUARD(500_ms);
  #define PRINT_WRAPPER KR(ret), K(accept_req), K(*this)
  CHECK_SILENCE();// 启动后的要维持一段静默时间，acceptor假装看不到任何消息，以维护lease的语义
  int ret = OB_SUCCESS;
  if (OB_LIKELY(RequestChecker::check_ballot_valid(accept_req,
                                                   this,
                                                   LogPhase::RENEW_LEASE))) {
    // 1. 推大ballot number，防止accept lease的ballot number回退
    if (accept_req.get_ballot_number() > ballot_number_) {
      advance_ballot_number_and_reset_related_states_(accept_req.get_ballot_number(), LogPhase::RENEW_LEASE);
    }
    // 2. 无条件更新Lease
    lease_.update_from(accept_req);
    *us_to_expired = lease_.get_lease_end_ts() - get_monotonic_ts();
    // 3. 构造accept ok消息
    ElectionAcceptResponseMsg accept_res_accept(p_election_->get_self_addr(),
                                                p_election_->inner_priority_seed_,
                                                p_election_->get_membership_version_(),
                                                p_election_->get_ls_biggest_min_cluster_version_ever_seen_(),
                                                accept_req);
    (void) p_election_->refresh_priority_();
    if (CLICK_FAIL(accept_res_accept.set_accepted(ballot_number_,
                                                  p_election_->get_priority_()))) {
      LOG_RENEW_LEASE(ERROR, "fail to copy priority", K(accept_res_accept));
    } else if (CLICK_FAIL(p_election_->send_(accept_res_accept))) {
      LOG_RENEW_LEASE(ERROR, "fail to send msg", K(accept_res_accept));
    }
  }
  #undef PRINT_WRAPPER
}

int64_t ElectionAcceptor::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (OB_ISNULL(p_election_)) {
    common::databuff_printf(buf, buf_len, pos, "{p_election:NULL");
  } else {
    common::databuff_printf(buf, buf_len, pos, "{ls_id:{id:%ld}", p_election_->id_);
    common::databuff_printf(buf, buf_len, pos, ", addr:%s", to_cstring(p_election_->get_self_addr()));
  }
  common::databuff_printf(buf, buf_len, pos, ", ballot_number:%ld", ballot_number_);
  common::databuff_printf(buf, buf_len, pos, ", ballot_of_time_window:%ld", ballot_of_time_window_);
  common::databuff_printf(buf, buf_len, pos, ", lease:%s", to_cstring(lease_));
  common::databuff_printf(buf, buf_len, pos, ", is_time_window_opened:%s", to_cstring(is_time_window_opened_));
  common::databuff_printf(buf, buf_len, pos, ", vote_reason:%s", to_cstring(vote_reason_));
  common::databuff_printf(buf, buf_len, pos, ", last_time_window_open_ts:%s", ObTime2Str::ob_timestamp_str_range<YEAR, USECOND>(last_time_window_open_ts_));
  if (highest_priority_prepare_req_.is_valid()) {
    common::databuff_printf(buf, buf_len, pos, ", highest_priority_prepare_req:%s",
                                                  to_cstring(highest_priority_prepare_req_));
  }
  common::databuff_printf(buf, buf_len, pos, ", p_election:0x%lx}", (unsigned long)p_election_);
  return pos;
}

}
}
}
