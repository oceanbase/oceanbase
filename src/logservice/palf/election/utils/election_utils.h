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

#ifndef LOGSERVICE_PALF_ELECTION_UTILS_OB_ELECTION_UTILS_H
#define LOGSERVICE_PALF_ELECTION_UTILS_OB_ELECTION_UTILS_H

#include "common/ob_clock_generator.h"
#include "lib/lock/ob_tc_rwlock.h"
#include "lib/net/ob_addr.h"
#include "lib/container/ob_array.h"
#include "lib/function/ob_function.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/container/ob_tuple.h"
#include "logservice/palf/election/utils/election_common_define.h"
#include "logservice/palf/election/interface/election.h"

namespace oceanbase
{
namespace palf
{
namespace election
{

class ElectionMsgBase;
class ElectionPrepareResponseMsg;
class ElectionAcceptRequestMsg;
class ElectionAcceptResponseMsg;

enum class ElectionMsgType
{
  PREPARE_REQUEST = 0,
  PREPARE_RESPONSE,
  ACCEPT_REQUEST,
  ACCEPT_RESPONSE,
  CHANGE_LEADER,
  INVALID_TYPE,
};

inline const char *msg_type_to_string(const ElectionMsgType &type)
{
  const char *ret = "unknown type";
  switch (type)
  {
  case ElectionMsgType::PREPARE_REQUEST:
    ret = "Prepare Request";
    break;
  case ElectionMsgType::PREPARE_RESPONSE:
    ret = "Prepare Response";
    break;
  case ElectionMsgType::ACCEPT_REQUEST:
    ret = "Accept Request";
    break;
  case ElectionMsgType::ACCEPT_RESPONSE:
    ret = "Accept Response";
    break;
  case ElectionMsgType::CHANGE_LEADER:
    ret = "Change Leader";
    break;
  case ElectionMsgType::INVALID_TYPE:
    ret = "InValid Type";
    break;
  default:
    break;
  }
  return ret;
}

template <typename T>
inline void swap(T &v1, T&v2) {
  T temp_v;
  temp_v = v1;
  v1 = v2;
  v2 = temp_v;
}

template <typename T>
inline T max(const T &v1, const T &v2)
{
  return v1 > v2 ? v1 : v2;
}

template <typename T>
inline int64_t count_if(const common::ObArray<T> &array, ObFunction<bool(const T&)> func)
{
  int64_t count = 0;
  for (int64_t idx = 0; idx < array.count(); ++idx) {
    if (func(array.at(idx))) {
      ++count;
    }
  }
  return count;
}

template <typename T>
inline void bubble_sort_desc(common::ObArray<T> &array)
{
  if(array.count() >= 2 ) {
    for (int64_t i = 0; i < array.count() - 1; ++i) {
      for (int64_t j = 0; j < array.count() - i - 1; ++j) {
        if (array.at(j) < array.at(j + 1)) {
          swap(array.at(j), array.at(j + 1));
        }
      }
    }
  }
}

template <typename T>
inline int get_sorted_majority_one_desc(const common::ObArray<T> &array, T &majority_element)
{
  ELECT_TIME_GUARD(500_ms);
  int ret = common::OB_SUCCESS;
  common::ObArray<T> temp_array;
  for (int64_t idx = 0; idx < array.count() && OB_SUCC(ret); ++idx) {
    if (CLICK_FAIL(temp_array.push_back(array.at(idx)))) {
      ELECT_LOG(ERROR, "assign temp array failed", KR(ret));
    }
  }
  if (OB_SUCC(ret)) {
    (void) bubble_sort_desc(temp_array);
    majority_element = temp_array[temp_array.count() / 2];// 从高到低排序取第n/2 + 1个值
  }
  return ret;
}

template <typename T>
inline const char *obj_to_string(const T &v);

template <>
inline const char *obj_to_string<common::ObRole>(const common::ObRole &v)
{
  const char *ret = "unknown value";
  switch (v) {
  case common::ObRole::LEADER:
    ret = "Leader";
    break;
  case common::ObRole::FOLLOWER:
    ret = "Follower";
    break;
  default:
    break;
  }
  return ret;
}

template <>
inline const char *obj_to_string<RoleChangeReason>(const RoleChangeReason &v)
{
  const char *ret = "unknown value";
  switch (v) {
  case RoleChangeReason::DevoteToBeLeader:
    ret = "decentralized voting, self to be leader";
    break;
  case RoleChangeReason::ChangeLeaderToBeLeader:
    ret = "self change leader to be leader";
    break;
  case RoleChangeReason::LeaseExpiredToRevoke:
    ret = "renew lease failed, lease expired, self to be follower";
    break;
  case RoleChangeReason::ChangeLeaderToRevoke:
    ret = "self change leader to be follower";
    break;
  case RoleChangeReason::StopToRevoke:
    ret = "election stopped to revoke";
    break;
  case RoleChangeReason::AskToRevoke:
    ret = "someone asking election to revoke";
    break;
  default:
    break;
  }
  return ret;
}

class TimeSpanWrapper
{
public:
  TimeSpanWrapper(const int64_t ts) : ts_(ts) {}
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    if (ts_ <= 0) {
      common::databuff_printf(buf, buf_len, pos, "invalid");
    } else {
      int64_t diff = ts_ - get_monotonic_ts();
      int64_t expired_time_point =  common::ObTimeUtility::fast_current_time() + diff;
      double time_span = diff * 1.0 / 1_s;
      common::databuff_printf(buf, buf_len, pos, "{span_from_now:%.3lfs, expired_time_point:%s}",
                              time_span,
                              common::ObTime2Str::
                              ob_timestamp_str_range<HOUR, MSECOND>(expired_time_point));
    }
    return pos;
  }
  int64_t get_current_ts_likely() const
  {
    int64_t diff = ts_ - get_monotonic_ts();
    int64_t expired_time_point =  common::ObTimeUtility::fast_current_time() + diff;
    return expired_time_point;
  }
private:
  const int64_t ts_;
};

class Lease
{
  OB_UNIS_VERSION(1);
public:
  Lease();
  Lease(const Lease &rhs);
  Lease &operator=(const Lease &rhs);
  bool is_expired() const;
  void reset();
  bool is_empty() const;
  const ObAddr &get_owner() const;
  int64_t get_ballot_number() const;
  int64_t get_lease_end_ts() const;
  void get_owner_and_ballot(ObAddr &owner, int64_t &ballot) const;
  void update_from(const ElectionAcceptRequestMsg &accept_req);
  //int64_t to_string(char *buf, const int64_t buf_len) const;
  #define LEASE_END "lease_end_ts", TimeSpanWrapper(lease_end_ts_)
  TO_STRING_KV(K_(owner), LEASE_END, K_(ballot_number));
  #undef LEASE_END
private:
  common::ObAddr owner_; // 发出Lease的Proposer
  int64_t lease_end_ts_; // 即Lease End
  int64_t ballot_number_; // 即paxos中的proposal id
  RWLock lock_;
};

class EventRecorder;

class MemberListWithStates
{
  friend class EventRecorder;
public:
  MemberListWithStates();
  int init();
  MemberListWithStates(const MemberListWithStates&) = delete;
  MemberListWithStates &operator=(const MemberListWithStates&) = delete;
  ~MemberListWithStates();
  int set_member_list(const MemberList &new_member_list);
  void clear_prepare_and_accept_states();
  int record_prepare_ok(const ElectionPrepareResponseMsg &prepare_res);
  int record_accept_ok(const ElectionAcceptResponseMsg &accept_res);
  bool is_synced_with_majority() const;
  int get_majority_promised_not_vote_ts(int64_t &ts) const;
  const MemberList &get_member_list() const;
  int64_t to_string(char *buf, const int64_t buf_len) const;
private:
  template <typename T>
  int init_array_(common::ObArray<T> &v, const int64_t size, T default_value);
  template <typename T>
  void print_array_in_pretty_way_(char *buf, const int64_t buf_len, int64_t &pos,
                                  const common::ObArray<T> &array, const char *key, const bool is_end) const;
  int get_server_idx_in_memberlist_(const common::ObAddr &addr, int64_t &idx);
private:
  struct PImpl
  {
    PImpl()
    {
      prepare_ok_.set_attr(
        ObMemAttr(OB_SERVER_TENANT_ID, "PrepareOk"));
      accept_ok_promise_not_vote_before_local_ts_.set_attr(
        ObMemAttr(OB_SERVER_TENANT_ID, "acceptOK"));
      follower_renew_lease_success_membership_version_.set_attr(
        ObMemAttr(OB_SERVER_TENANT_ID, "followerRenew"));
    }
    MemberList member_list_;
    common::ObArray<bool> prepare_ok_;
    common::ObArray<int64_t> accept_ok_promise_not_vote_before_local_ts_;
    common::ObArray<LogConfigVersion> follower_renew_lease_success_membership_version_;
  } *p_impl_;
};

template <typename T>
int MemberListWithStates::init_array_(common::ObArray<T> &v, const int64_t size, T default_value)
{
  ELECT_TIME_GUARD(500_ms);
  int ret = OB_SUCCESS;
  if (v.count() == size) {
    for (int64_t idx = 0; idx < size; ++idx) {
      v[idx] = default_value;
    }
  } else {
    v.reset();
    for (int64_t idx = 0; idx < size && OB_SUCC(ret); ++idx) {
      if (CLICK_FAIL(v.push_back(default_value))) {
        ELECT_LOG(ERROR, "push value to v failed", KR(ret), K(*this));
      }
    }
  }
  if (CLICK_FAIL(ret)) {
    v.reset();
  }
  return ret;
}

template <typename T>
void MemberListWithStates::print_array_in_pretty_way_(char *buf, const int64_t buf_len, int64_t &pos, const common::ObArray<T> &array, const char *key, const bool is_end) const
{
  bool is_synced = true;
  for (int64_t idx = 0; idx < array.count(); ++idx) {
    if (array[idx] != array[0]) {
      is_synced = false;
      break;
    }
  }
  if (!is_end) {
    if (is_synced && !array.empty()) {
      common::databuff_printf(buf, buf_len, pos, "%s:%s, ", key, to_cstring(array[0]));
    } else {
      common::databuff_printf(buf, buf_len, pos, "%s:%s, ", key, to_cstring(array));
    }
  } else {
    if (is_synced && !array.empty()) {
      common::databuff_printf(buf, buf_len, pos, "%s:%s}", key, to_cstring(array[0]));
    } else {
      common::databuff_printf(buf, buf_len, pos, "%s:%s}", key, to_cstring(array));
    }
  }
};

// 从排查问题的经验来看，选举模块已经稳定，没有发生因为自身问题导致的无主
// 但测试环境中还是经常会出现无主报错，这通常是其他原因引起的，例如：
// 1. palf工作线程卡住，无法处理消息
// 2. observer内存耗尽，可以发出但是无法接收消息
// 3. server处于stop状态，但是没有完全退出，可以发出但是无法接收消息
// 4. 多数派core，无法选主
// 5. 其他由于observer内部其他模块异常导致的消息收发问题。
//
// 尽管真实的网络环境正常，但由于observer内部的问题，以上情况对于选举模块来说等同于网络分区/单向网络连通
// 设计以下数据结构用来记录消息收发的情况，用于快速提供信息排查问题：
// 1. 发过多少消息，什么类型，分别发给了哪些server，最后一次给他们发出消息的时间戳是多少
// 2. 收到多少消息，什么类型，分别来自哪些server，最后一次收到他们发来消息的时间戳是多少
class ElectionMsgCounter
{
  // 按IP地址分类
  class AddrMsgCounter
  {
    // 按消息类型分类
    struct MsgCounter
    {
      // 统计消息收发数量以及最后一次收发的时间戳
      struct Counter
      {
        Counter() : send_count_(0), receive_count_(0), last_send_ts_(0), last_received_ts_(0) {}
        void add_send_count() { ++send_count_; last_send_ts_ = ObClockGenerator::getCurrentTime(); }
        void add_received_count() { ++receive_count_; last_received_ts_ = ObClockGenerator::getCurrentTime(); }
        void reset() { new (this)Counter(); }
        int64_t send_count_;
        int64_t receive_count_;
        int64_t last_send_ts_;
        int64_t last_received_ts_;
      };
      void add_send_count(ElectionMsgType type) { counter_mapper_[static_cast<int>(type)].add_send_count(); }
      void add_received_count(ElectionMsgType type) { counter_mapper_[static_cast<int>(type)].add_received_count(); }
      int64_t get_latest_ts() const {
        return max(get_latest_rec_ts(), get_latest_send_ts());
      }
      int64_t get_latest_rec_ts() const {
        int64_t latest_ts = 0;
        for (int64_t idx = 0; idx < static_cast<int>(ElectionMsgType::INVALID_TYPE); ++idx) {
          latest_ts = max(latest_ts, counter_mapper_[idx].last_received_ts_);
        }
        return latest_ts;
      }
      int64_t get_latest_send_ts() const {
        int64_t latest_ts = 0;
        for (int64_t idx = 0; idx < static_cast<int>(ElectionMsgType::INVALID_TYPE); ++idx) {
          latest_ts = max(latest_ts, counter_mapper_[idx].last_send_ts_);
        }
        return latest_ts;
      }
      bool is_valid() const { return get_latest_ts() != 0; }
      void reset() {
        for (int64_t idx = 0; idx < static_cast<int>(ElectionMsgType::INVALID_TYPE); ++idx) {
          counter_mapper_[idx].reset();
        }
      }
      int64_t to_string(char *buf, const int64_t buf_len, MsgCounter *old) const;
      Counter counter_mapper_[static_cast<int>(ElectionMsgType::INVALID_TYPE)];
    };
  public:
    static constexpr int64_t MAP_SIZE = 7;
    AddrMsgCounter() : idx_(0) {}
    void add_send_count(const ElectionMsgBase &msg);
    void add_received_count(const ElectionMsgBase &msg);
    MsgCounter *find(const common::ObAddr &addr);
    MsgCounter *find_or_reuse_item_(const common::ObAddr &addr);
    int idx_;
    // 按副本IP分类, 维护最近通信的MAP_SIZE个消息收发记录
    ObTuple<common::ObAddr, MsgCounter> addr_mapper_[MAP_SIZE];
  };
public:
  ElectionMsgCounter() : p_cur_mapper_(&mapper_[0]), p_old_mapper_(&mapper_[1]) {}
  void add_send_count(const ElectionMsgBase &msg) { return p_cur_mapper_->add_send_count(msg); }
  void add_received_count(const ElectionMsgBase &msg) { return p_cur_mapper_->add_received_count(msg); }
  int64_t to_string(char *buf, const int64_t buf_len) const;// mark the diff between cur and old every time print
private:
  mutable AddrMsgCounter *p_cur_mapper_;
  mutable AddrMsgCounter *p_old_mapper_;
  AddrMsgCounter mapper_[2];
};

}// namespace election
}// namespace palf
}// namesapce oceanbase

#endif
