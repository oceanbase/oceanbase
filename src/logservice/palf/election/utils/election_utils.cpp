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
#include "lib/ob_define.h"
#include "lib/oblog/ob_log_module.h"
#include "share/ob_occam_time_guard.h"
#include "share/rc/ob_tenant_base.h"
#include "election_utils.h"
#include "election_common_define.h"
#include "election_args_checker.h"
#include "logservice/palf/election/message/election_message.h"

namespace oceanbase
{
namespace palf
{
namespace election
{
using namespace share;

OB_SERIALIZE_MEMBER(Lease, owner_, lease_end_ts_, ballot_number_);

MemberListWithStates::MemberListWithStates()
:p_impl_(nullptr)
{
}

int MemberListWithStates::init()
{
  #define PRINT_WRAPPER KR(ret), K(*this)
  int ret = OB_SUCCESS;
  p_impl_ = (PImpl *)mtl_malloc(sizeof(PImpl), "Election");
  if (OB_ISNULL(p_impl_)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_INIT(ERROR, "alloc memory failed");
  } else {
    new(p_impl_) PImpl();
  }
  return ret;
  #undef PRINT_WRAPPER
}

MemberListWithStates::~MemberListWithStates()
{
  if (OB_NOT_NULL(p_impl_)) {
    p_impl_->~PImpl();
    mtl_free(p_impl_);
    p_impl_ = nullptr;
  }
}

int MemberListWithStates::set_member_list(const MemberList &new_member_list)
{
  ELECT_TIME_GUARD(500_ms);
  #define PRINT_WRAPPER K(new_member_list), K(*this)
  int ret = OB_SUCCESS;
  bool only_membership_version_different = false;
  if (p_impl_->member_list_.get_replica_num() != new_member_list.get_replica_num()) {
  } else if (p_impl_->member_list_.get_addr_list().count() != new_member_list.get_addr_list().count()) {
  } else {
    int64_t compare_idx = 0;
    for (; compare_idx < new_member_list.get_addr_list().count(); ++compare_idx) {
      if (new_member_list.get_addr_list()[compare_idx] != p_impl_->member_list_.get_addr_list()[compare_idx]) {
        break;
      }
    }
    if (compare_idx == new_member_list.get_addr_list().count()) {
      only_membership_version_different = true;
    }
  }
  if (only_membership_version_different) {
    p_impl_->member_list_.set_membership_version(new_member_list.get_membership_version());
  } else {
    PImpl *p_impl = (PImpl*)mtl_malloc(sizeof(PImpl), "Election");
    if (p_impl == nullptr) {
      LOG_SET_MEMBER(ERROR, "alloc memory failed");
    } else {
      new(p_impl) PImpl();
      if (CLICK_FAIL(p_impl->member_list_.assign(new_member_list))) {
        LOG_SET_MEMBER(ERROR, "fail to assign new member list");
      } else if (CLICK_FAIL(init_array_(p_impl->prepare_ok_,
                                    new_member_list.get_replica_num(),
                                    false))) {
        LOG_SET_MEMBER(ERROR, "fail to init prepare ok list");
      } else if (CLICK_FAIL(init_array_(p_impl->accept_ok_promise_not_vote_before_local_ts_,
                                    new_member_list.get_replica_num(),
                                    int64_t(0)))) {
        LOG_SET_MEMBER(ERROR, "fail to init accept ok list");
      } else if (CLICK_FAIL(init_array_(p_impl->follower_renew_lease_success_membership_version_,
                                    new_member_list.get_replica_num(), LogConfigVersion()))) {
        LOG_SET_MEMBER(ERROR, "fail to init follower_renew_lease_success_membership_version_ list");
      } else {
        MemberList &old_member_list = p_impl_->member_list_;
        PImpl &old_data = *p_impl_;
        PImpl &new_data = *p_impl;
        for (int64_t i = 0; i < old_data.member_list_.get_addr_list().count(); ++i) {
          for (int64_t j = 0; j < new_data.member_list_.get_addr_list().count(); ++j) {
            if (old_data.member_list_.get_addr_list()[i] == new_data.member_list_.get_addr_list()[j]) {
              new_data.prepare_ok_[j] = old_data.prepare_ok_[i];
              new_data.accept_ok_promise_not_vote_before_local_ts_[j] = old_data.accept_ok_promise_not_vote_before_local_ts_[i];
              new_data.follower_renew_lease_success_membership_version_[j] = old_data.follower_renew_lease_success_membership_version_[i];
            }
          }
        }
        swap(p_impl_, p_impl);
        LOG_SET_MEMBER(INFO, "set new memberlist success", K(old_member_list));
      }
    }
    if (OB_NOT_NULL(p_impl)) {
      p_impl->~PImpl();
      mtl_free(p_impl);
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

void MemberListWithStates::clear_prepare_and_accept_states()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_array_(p_impl_->prepare_ok_,
                          p_impl_->member_list_.get_replica_num(),
                          false))) {
    abort();
  } else if (OB_FAIL(init_array_(p_impl_->accept_ok_promise_not_vote_before_local_ts_,
                                 p_impl_->member_list_.get_replica_num(),
                                 int64_t(0)))) {
    abort();
  }
}

int MemberListWithStates::record_prepare_ok(const ElectionPrepareResponseMsg &prepare_res)
{
  ELECT_TIME_GUARD(500_ms);
  #define PRINT_WRAPPER KR(ret), K(*this)
  int ret = OB_SUCCESS;
  int64_t idx = 0;
  if (CLICK_FAIL(get_server_idx_in_memberlist_(prepare_res.get_sender(), idx))) {
    LOG_ELECT_LEADER(WARN, "not find server index in member list");
  } else {
    p_impl_->prepare_ok_[idx] = true;
    ObFunction<bool(const bool &)> f = [](const bool &v) { return v; };
    int64_t ok_count = count_if(p_impl_->prepare_ok_, f);
    if (ok_count < (p_impl_->member_list_.get_replica_num() / 2 + 1)) {
      ret = OB_ELECTION_BELOW_MAJORITY;
    } else if (ok_count > (p_impl_->member_list_.get_replica_num() / 2 + 1)) {
      ret = OB_ELECTION_OVER_MAJORITY;
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

int MemberListWithStates::record_accept_ok(const ElectionAcceptResponseMsg &accept_res)
{
  ELECT_TIME_GUARD(500_ms);
  int ret = OB_SUCCESS;
  int64_t idx = 0;
  if (OB_ISNULL(p_impl_)) {
    ret = OB_NOT_INIT;
    ELECT_LOG(ERROR, "p_impl is nullptr", KR(ret), KP_(p_impl));
  } else if (CLICK_FAIL(get_server_idx_in_memberlist_(accept_res.get_sender(), idx))) {
    ELECT_LOG(WARN, "not find server index in member list", KR(ret), K(*this));
  } else {
    int64_t &write_ts = p_impl_->accept_ok_promise_not_vote_before_local_ts_[idx];
    write_ts = max(write_ts,
                   accept_res.get_lease_started_ts_on_proposer() + accept_res.get_lease_interval());
    LogConfigVersion &write_version = p_impl_->follower_renew_lease_success_membership_version_[idx];
    if (!write_version.is_valid()) {
      write_version = accept_res.get_responsed_membership_version();
    } else {
      write_version = max(write_version, accept_res.get_responsed_membership_version());
    }
    ObFunction<bool(const int64_t&)> f = [](const int64_t &v) { return v != 0; };
    int64_t ok_count = count_if(p_impl_->accept_ok_promise_not_vote_before_local_ts_, f);
    if (ok_count < (p_impl_->member_list_.get_replica_num() / 2 + 1)) {
      ret = OB_ELECTION_BELOW_MAJORITY;
    }
    // 这里不能返回OVER_MAJORITY，因为在续约阶段，所有的accept_ok_promise_not_vote_before_local_ts_都是
    // 有效的，并且这里不判断OVER_MAJORITY的另一个好处是，每一次返回accept ok都可能把leader lease向后推进
  }
  return ret;
}

int MemberListWithStates::get_majority_promised_not_vote_ts(int64_t &ts) const
{
  ELECT_TIME_GUARD(500_ms);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(p_impl_)) {
    ret = OB_NOT_INIT;
    ELECT_LOG(ERROR, "p_impl_ is nullptr", KR(ret), KP_(p_impl));
  } else if (CLICK_FAIL(get_sorted_majority_one_desc(
                     p_impl_->accept_ok_promise_not_vote_before_local_ts_,
                     ts))) {
    ELECT_LOG(ERROR, "connot get majority promised not vote ts", KR(ret), K(*this));
  }
  return ret;
}

bool MemberListWithStates::is_synced_with_majority() const
{
  bool ret_bool = false;
  if (OB_ISNULL(p_impl_)) {
    ELECT_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "p_impl_ is nullptr", KP_(p_impl));
  } else if (!p_impl_->member_list_.get_membership_version().is_valid()) {
    ret_bool = true;
  } else {
    LogConfigVersion version = p_impl_->member_list_.get_membership_version();
    ObFunction<bool(const LogConfigVersion&)> f = [version](const LogConfigVersion &v) { return v == version; };
    int64_t sync_count = count_if(p_impl_->follower_renew_lease_success_membership_version_, f);
    if (sync_count >= (p_impl_->member_list_.get_replica_num() / 2 + 1)) {
      ret_bool = true;
    } else {
      ELECT_LOG_RET(WARN, OB_ERR_UNEXPECTED, "membership version not sync with majority yet", K(*this), K(version));
    }
  }
  return ret_bool;
}

const MemberList &MemberListWithStates::get_member_list() const
{
  return p_impl_->member_list_;
}

int MemberListWithStates::get_server_idx_in_memberlist_(const ObAddr &addr, int64_t &idx)
{
  int ret = OB_SUCCESS;
  idx = 0;
  if (OB_ISNULL(p_impl_)) {
    ret = OB_NOT_INIT;
    ELECT_LOG(ERROR, "p_impl_ is nullptr", KR(ret), KP_(p_impl));
  } else {
    const ObArray<ObAddr> &addr_list = p_impl_->member_list_.get_addr_list();
    for(; idx < addr_list.count(); ++idx) {
      if (addr_list.at(idx) == addr) {
        break;
      }
    }
    if (idx == addr_list.count()) {
      ret = OB_ENTRY_NOT_EXIST;
      ELECT_LOG(WARN, "this address not found in memberlist", KR(ret), K(addr), K(*this));
    }
  }
  return ret;
}

int64_t MemberListWithStates::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (OB_ISNULL(p_impl_)) {
    common::databuff_printf(buf, buf_len, pos, "{p_impl:NULL}");
  } else {
    common::databuff_printf(buf, buf_len, pos, "{member_list:%s, ", to_cstring(p_impl_->member_list_));
    print_array_in_pretty_way_(buf, buf_len, pos, p_impl_->prepare_ok_, "prepare_ok", false);
    common::ObArray<int64_t> &v = p_impl_->accept_ok_promise_not_vote_before_local_ts_;
    bool is_synced = true;
    for (int64_t idx = 0; idx < v.count(); ++idx) {
      if (v[idx] != v[0]) {
        is_synced = false;
        break;
      }
    }
    if (is_synced && !v.empty()) {
      int64_t map_wall_clock_ts = v[0] - get_monotonic_ts() + ObClockGenerator::getCurrentTime();
      common::databuff_printf(buf, buf_len, pos, "accept_ok_promised_ts:%s, ",
                                                 v[0] != 0 ? common::ObTime2Str::ob_timestamp_str_range<HOUR, MSECOND>(map_wall_clock_ts) : "invalid");
    } else {
      common::databuff_printf(buf, buf_len, pos, "accept_ok_promised_ts:[");
      for (int64_t idx = 0; idx < v.count(); ++idx) {
        int64_t map_wall_clock_ts = v[idx] - get_monotonic_ts() + ObClockGenerator::getCurrentTime();
        if (idx == v.count() - 1) {
          common::databuff_printf(buf, buf_len, pos, "%s]", v[idx] != 0 ?
            common::ObTime2Str::ob_timestamp_str_range<HOUR, MSECOND>(map_wall_clock_ts) : "invalid");
        } else {
          common::databuff_printf(buf, buf_len, pos, "%s, ", v[idx] != 0 ?
            common::ObTime2Str::ob_timestamp_str_range<HOUR, MSECOND>(map_wall_clock_ts) : "invalid");
        }
      }
    }
    print_array_in_pretty_way_(buf, buf_len, pos, p_impl_->follower_renew_lease_success_membership_version_, "follower_promise_membership_version", true);
  }
  return pos;
}

Lease::Lease() : lease_end_ts_(INVALID_VALUE), ballot_number_(INVALID_VALUE) {}

Lease::Lease(const Lease &rhs)
{
  int64_t lease_end_ts = 0;
  int64_t ballot_number = 0;
  common::ObAddr owner;
  {
    TCRLockGuard guard(rhs.lock_);
    lease_end_ts = rhs.lease_end_ts_;
    ballot_number = rhs.ballot_number_;
    owner = rhs.owner_;
  }
  {
    TCWLockGuard guard(lock_);
    lease_end_ts_ = lease_end_ts;
    ballot_number_ = ballot_number;
    owner_ = owner;
  }
}

Lease &Lease::operator=(const Lease &rhs)
{
  new(this) Lease(rhs);
  return *this;
}

const ObAddr &Lease::get_owner() const {
  TCRLockGuard guard(lock_);
  return owner_;
}

void Lease::update_from(const ElectionAcceptRequestMsg &accept_req)
{
  TCWLockGuard guard(lock_);
  owner_ = accept_req.get_sender();
  lease_end_ts_ = max(lease_end_ts_,
                      int64_t(get_monotonic_ts() + accept_req.get_lease_interval()));
  assert(accept_req.get_ballot_number() >= ballot_number_);
  ballot_number_ = accept_req.get_ballot_number();
}

void Lease::get_owner_and_ballot(ObAddr &owner, int64_t &ballot) const
{
  TCRLockGuard guard(lock_);
  owner = owner_;
  ballot = ballot_number_;
}

bool Lease::is_expired() const
{ // is_expired()只支持在本地判断
  TCRLockGuard guard(lock_);
  return get_monotonic_ts() > lease_end_ts_;
}

int64_t Lease::get_ballot_number() const {
  TCRLockGuard guard(lock_);
  return ballot_number_;
}

void Lease::reset()
{
  TCWLockGuard guard(lock_);
  owner_.reset();
  lease_end_ts_ = -1;
}

bool Lease::is_empty() const
{ // is_empty()支持在任意机器上判断
  TCRLockGuard guard(lock_);
  return !owner_.is_valid() && lease_end_ts_ == -1;
}

int64_t Lease::get_lease_end_ts() const
{
  TCRLockGuard guard(lock_);
  return lease_end_ts_;
}

ElectionMsgCounter::AddrMsgCounter::MsgCounter *ElectionMsgCounter::AddrMsgCounter::find(const common::ObAddr &addr)
{
  MsgCounter *msg_counter = nullptr;
  for (int64_t idx = 0; idx < idx_ && OB_ISNULL(msg_counter); ++idx) {
    if (addr == addr_mapper_[idx].element<0>()) {
      msg_counter = &addr_mapper_[idx].element<1>();
    }
  }
  return msg_counter;
}

ElectionMsgCounter::AddrMsgCounter::MsgCounter *ElectionMsgCounter::AddrMsgCounter::find_or_reuse_item_(const common::ObAddr &addr)
{
  MsgCounter *msg_counter = nullptr;
  for (int64_t idx = 0; idx < idx_ && OB_ISNULL(msg_counter); ++idx) {
    if (addr == addr_mapper_[idx].element<0>()) {
      msg_counter = &addr_mapper_[idx].element<1>();
    }
  }
  if (OB_ISNULL(msg_counter)) {// not found, create new item
    int64_t new_item_idx = idx_;
    if (idx_ < MAP_SIZE) {// use new buffer
      ++idx_;
    } else {// reuse oldest buffer
      static_assert(MAP_SIZE > 0, "MAP_SIZE should greater than 0");
      new_item_idx = 0;
      int64_t oldest_interactive_ts = addr_mapper_[new_item_idx].element<1>().get_latest_ts();
      for (int64_t idx = 1; idx < MAP_SIZE; ++idx) {
        if (addr_mapper_[idx].element<1>().get_latest_ts() < oldest_interactive_ts) {
          oldest_interactive_ts = addr_mapper_[idx].element<1>().get_latest_ts();
          new_item_idx = idx;
        }
      }
    }
    addr_mapper_[new_item_idx].element<0>() = addr;
    addr_mapper_[new_item_idx].element<1>().reset();
    msg_counter = &addr_mapper_[new_item_idx].element<1>();
  }
  return msg_counter;
}

void ElectionMsgCounter::AddrMsgCounter::add_send_count(const ElectionMsgBase &msg)
{
  find_or_reuse_item_(msg.get_receiver())->add_send_count(msg.get_msg_type());
}

void ElectionMsgCounter::AddrMsgCounter::add_received_count(const ElectionMsgBase &msg)
{
  find_or_reuse_item_(msg.get_sender())->add_received_count(msg.get_msg_type());
}

int64_t ElectionMsgCounter::AddrMsgCounter::MsgCounter::to_string(char *buf, const int64_t buf_len, MsgCounter *old) const
{
  int64_t pos = 0;
  bool diff_rec[static_cast<int>(ElectionMsgType::INVALID_TYPE)] = { false };
  bool diff_send[static_cast<int>(ElectionMsgType::INVALID_TYPE)] = { false };
  if (OB_NOT_NULL(old)) {
    for (int64_t idx = 0; idx < static_cast<int>(ElectionMsgType::INVALID_TYPE); ++idx) {
      if (counter_mapper_[idx].receive_count_ != old->counter_mapper_[idx].receive_count_) {
        diff_rec[idx] = true;
      }
      if (counter_mapper_[idx].send_count_ != old->counter_mapper_[idx].send_count_) {
        diff_send[idx] = true;
      }
    }
  }
  databuff_printf(buf, buf_len, pos, "send:[");
  for (int64_t idx = 0; idx < static_cast<int>(ElectionMsgType::INVALID_TYPE); ++idx) {
    if (diff_send[idx]) {
      databuff_printf(buf, buf_len, pos, "(%ld), ", counter_mapper_[idx].send_count_);
    } else {
      databuff_printf(buf, buf_len, pos, "%ld, ", counter_mapper_[idx].send_count_);
    }
  }
  pos -= pos > 0 ? 2 : 0;
  databuff_printf(buf, buf_len, pos, "], rec:[");
  for (int64_t idx = 0; idx < static_cast<int>(ElectionMsgType::INVALID_TYPE); ++idx) {
    if (diff_rec[idx]) {
      databuff_printf(buf, buf_len, pos, "(%ld), ", counter_mapper_[idx].receive_count_);
    } else {
      databuff_printf(buf, buf_len, pos, "%ld, ", counter_mapper_[idx].receive_count_);
    }
  }
  pos -= pos > 0 ? 2 : 0;
  databuff_printf(buf, buf_len, pos, "], ");
  databuff_printf(buf, buf_len, pos, "last_send:%s, ", common::ObTime2Str::ob_timestamp_str(get_latest_send_ts()));
  databuff_printf(buf, buf_len, pos, "last_rec:%s", common::ObTime2Str::ob_timestamp_str(get_latest_rec_ts()));
  return pos;
}

int64_t ElectionMsgCounter::to_string(char *buf, const int64_t buf_len) const// mark the diff between cur and old every time print
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "|\tmatch:[");
  for (int64_t idx = 0; idx < static_cast<int>(ElectionMsgType::INVALID_TYPE); ++idx) {
    databuff_printf(buf, buf_len, pos, "%s, ", msg_type_to_string(static_cast<ElectionMsgType>(idx)));
  }
  pos -= pos > 2 ? 2 : 0;
  databuff_printf(buf, buf_len, pos, "]");
  for (int64_t idx = 0; idx < p_cur_mapper_->idx_; ++idx) {
    databuff_printf(buf, buf_len, pos, "|\t%s:", to_cstring(p_cur_mapper_->addr_mapper_[idx].element<0>()));
    pos += p_cur_mapper_->addr_mapper_[idx].element<1>().to_string(buf + pos, buf_len - pos, p_old_mapper_->find(p_cur_mapper_->addr_mapper_[idx].element<0>()));
  }
  *p_old_mapper_ = *p_cur_mapper_;
  return pos;
}

}// namespace election
}// namespace palf
}// namesapce oceanbase
