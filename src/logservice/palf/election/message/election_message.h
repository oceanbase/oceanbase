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

#ifndef LOGSERVICE_PALF_ELECTION_MESSAGE_OB_ELECTION_MESSAGE_H
#define LOGSERVICE_PALF_ELECTION_MESSAGE_OB_ELECTION_MESSAGE_H

#include "common/ob_clock_generator.h"
#include "lib/net/ob_addr.h"
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/oblog/ob_log_time_fmt.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_unify_serialize.h"
#include "lib/utility/serialization.h"
#include "logservice/palf/election/utils/election_common_define.h"
#include "logservice/palf/election/utils/election_utils.h"
#include "logservice/palf/election/interface/election_priority.h"
#include "logservice/palf/election/interface/election.h"

namespace oceanbase
{
namespace palf
{
namespace election
{

struct LsBiggestMinClusterVersionEverSeen {// this is for maintain min_cluster_version on arb server
  LsBiggestMinClusterVersionEverSeen() : version_(0) {}
  LsBiggestMinClusterVersionEverSeen(const uint64_t version)
  : version_(version) {};
  void try_advance(uint64_t new_version) {
    if (version_ < new_version) {
      version_ = new_version;
      ELECT_LOG(INFO, "advance ls ever seen biggest min_cluster_verson");
    }
  }
  int64_t to_string(char *buf, const int64_t len) const {
    int64_t pos = 0;
    databuff_printf(buf, len, pos, "%u.", OB_VSN_MAJOR(version_));
    databuff_printf(buf, len, pos, "%u.", OB_VSN_MINOR(version_));
    databuff_printf(buf, len, pos, "%u.", OB_VSN_MAJOR_PATCH(version_));
    databuff_printf(buf, len, pos, "%u", OB_VSN_MINOR_PATCH(version_));
    return pos;
  }
  uint64_t version_;
};

struct CompatHelper {
  static constexpr int64_t BIT_MASK_NOT_LESS_THAN_4_2 = (1LL << 63);// this bit is used for mark message needed seralized in lazy-mode, and for compat reason
  static void set_msg_flag_not_less_than_4_2(const ElectionMsgBase &msg);
  static bool fetch_msg_flag_not_less_than_4_2(const ElectionMsgBase &msg);
};

struct ElectionMsgDebugTs
{
  OB_UNIS_VERSION(1);
public:
  ElectionMsgDebugTs() :
  src_construct_ts_(0),
  src_serialize_ts_(0),
  dest_deserialize_ts_(0),
  dest_process_ts_(0) {}
  #define PROCESS_DELAY "process_delay", dest_process_ts_ - src_construct_ts_
  TO_STRING_KV(KTIMERANGE_(src_construct_ts, MINUTE, USECOND),
               KTIMERANGE_(src_serialize_ts, MINUTE, USECOND),
               KTIMERANGE_(dest_deserialize_ts, MINUTE, USECOND),
               KTIMERANGE_(dest_process_ts, MINUTE, USECOND),
               PROCESS_DELAY);
  #undef PROCESS_DELAY
  int64_t src_construct_ts_;
  int64_t src_serialize_ts_;
  int64_t dest_deserialize_ts_;
  int64_t dest_process_ts_;
};
OB_SERIALIZE_MEMBER_TEMP(inline, ElectionMsgDebugTs, src_construct_ts_, src_serialize_ts_,
                         dest_deserialize_ts_, dest_process_ts_);

void print_debug_ts_if_reach_warn_threshold(const ElectionMsgBase &msg, const int64_t warn_threshold);

class ElectionMsgBase
{
  friend class CompatHelper;
  OB_UNIS_VERSION(1);
public:
  ElectionMsgBase();// default constructor is required by deserialization, but not actually worked
  ElectionMsgBase(const int64_t id,
                  const common::ObAddr &self_addr,
                  const int64_t restart_counter,
                  const int64_t ballot_number,
                  const LsBiggestMinClusterVersionEverSeen &version,
                  const ElectionMsgType msg_type);
  void reset();
  void set_receiver(const common::ObAddr &addr);
  int64_t get_restart_counter() const;
  int64_t get_ballot_number() const;
  const common::ObAddr &get_sender() const;
  const common::ObAddr &get_receiver() const;
  const LsBiggestMinClusterVersionEverSeen &get_ls_biggest_min_cluster_version_ever_seen() const;
  ElectionMsgType get_msg_type() const;
  bool is_valid() const;
  ElectionMsgDebugTs get_debug_ts() const;
  void set_process_ts();
  int64_t get_id() const;
  #define MSG_TYPE "msg_type", msg_type_to_string(static_cast<ElectionMsgType>(msg_type_))
  TO_STRING_KV(MSG_TYPE, K_(id), K_(sender), K_(receiver), K_(restart_counter),
               K_(ballot_number), K_(debug_ts), K_(biggest_min_cluster_version_ever_seen));
  #undef MSG_TYPE
protected:
  int64_t id_;
  common::ObAddr sender_;
  common::ObAddr receiver_;
  int64_t restart_counter_;
  int64_t ballot_number_;
  LsBiggestMinClusterVersionEverSeen biggest_min_cluster_version_ever_seen_;
  int64_t msg_type_;
  ElectionMsgDebugTs debug_ts_;
};
OB_SERIALIZE_MEMBER_TEMP(inline, ElectionMsgBase, id_, sender_, receiver_,
                         restart_counter_, ballot_number_, msg_type_, debug_ts_,
                         biggest_min_cluster_version_ever_seen_.version_);

class ElectionPrepareRequestMsgMiddle : public ElectionMsgBase
{
  OB_UNIS_VERSION(1);
public:
  ElectionPrepareRequestMsgMiddle();// default constructor is required by deserialization, but not actually worked
  ElectionPrepareRequestMsgMiddle(const int64_t id,
                                  const common::ObAddr &self_addr,
                                  const int64_t restart_counter,
                                  const int64_t ballot_number,
                                  const LsBiggestMinClusterVersionEverSeen &version,
                                  const uint64_t inner_priority_seed,
                                  const LogConfigVersion membership_version);
  int set(const ElectionPriority *priority, const common::ObRole role);
  const char *get_priority_buffer() const;
  bool is_buffer_valid() const;
  common::ObRole get_role() const;
  uint64_t get_inner_priority_seed() const;
  LogConfigVersion get_membership_version() const;
  #define BASE "BASE", *(static_cast<const ElectionMsgBase*>(this))
  #define ROLE "role", obj_to_string(static_cast<common::ObRole>(role_))
  TO_STRING_KV(KP(this), BASE, ROLE, K_(is_buffer_valid), K_(inner_priority_seed),
               K_(membership_version));
  #undef ROLE
  #undef BASE
protected:
  int64_t role_;
  bool is_buffer_valid_;
  uint64_t inner_priority_seed_;
  LogConfigVersion membership_version_;
  unsigned char priority_buffer_[PRIORITY_BUFFER_SIZE];
};
OB_SERIALIZE_MEMBER_TEMP(inline, (ElectionPrepareRequestMsgMiddle, ElectionMsgBase), role_,
                         is_buffer_valid_, membership_version_, priority_buffer_, inner_priority_seed_);

// design wrapper class to record serialize/deserialize time, for debugging
class ElectionPrepareRequestMsg : public ElectionPrepareRequestMsgMiddle
{
public:
  ElectionPrepareRequestMsg() : ElectionPrepareRequestMsgMiddle() {}// default constructor is required by deserialization, but not actually worked
  ElectionPrepareRequestMsg(const int64_t id,
                            const common::ObAddr &self_addr,
                            const int64_t restart_counter,
                            const int64_t ballot_number,
                            const LsBiggestMinClusterVersionEverSeen &version,
                            const uint64_t inner_priority_seed,
                            const LogConfigVersion membership_version) :
  ElectionPrepareRequestMsgMiddle(id, self_addr, restart_counter, ballot_number, version, inner_priority_seed, membership_version) {}
  int deserialize(const char* buf, const int64_t data_len, int64_t& pos) {
    int ret = OB_SUCCESS;
    if (OB_FAIL(ElectionPrepareRequestMsgMiddle::deserialize(buf, data_len, pos))) {
      ELECT_LOG(WARN, "deserialize failed", KR(ret));
    }
    debug_ts_.dest_deserialize_ts_ = ObClockGenerator::getRealClock();
    print_debug_ts_if_reach_warn_threshold(*this, MSG_DELAY_WARN_THRESHOLD);
    return ret;
  }
  int64_t get_serialize_size() const {
    if (debug_ts_.src_serialize_ts_ == 0) {// cause get_serialize_size maybe call more than once
      const_cast<int64_t&>(debug_ts_.src_serialize_ts_) = ObClockGenerator::getRealClock();
      print_debug_ts_if_reach_warn_threshold(*this, MSG_DELAY_WARN_THRESHOLD);
    }
    return ElectionPrepareRequestMsgMiddle::get_serialize_size();
  }
};

class ElectionPrepareResponseMsgMiddle : public ElectionMsgBase
{
  OB_UNIS_VERSION(1);
public:
  ElectionPrepareResponseMsgMiddle();// default constructor is required by deserialization, but not actually worked
  ElectionPrepareResponseMsgMiddle(const common::ObAddr &self_addr,
                                   const LsBiggestMinClusterVersionEverSeen &version,
                                   const ElectionPrepareRequestMsgMiddle &request);
  void set_accepted(const int64_t ballot_number, const Lease lease);
  void set_rejected(const int64_t ballot_number);
  bool is_accepted() const;
  const Lease &get_lease() const;
  ElectionMsgDebugTs get_request_debug_ts() const;
  #define BASE "BASE", *(static_cast<const ElectionMsgBase*>(this))
  TO_STRING_KV(BASE, K_(accepted), K_(lease), K_(request_debug_ts));
  #undef BASE
private:
  bool accepted_;// 请求是否被接受
  Lease lease_;// may be "EMPTY"
  ElectionMsgDebugTs request_debug_ts_;
};
OB_SERIALIZE_MEMBER_TEMP(inline, (ElectionPrepareResponseMsgMiddle, ElectionMsgBase), accepted_,
                         lease_, request_debug_ts_);

// design wrapper class to record serialize/deserialize time, for debugging
class ElectionPrepareResponseMsg : public ElectionPrepareResponseMsgMiddle
{
public:
  ElectionPrepareResponseMsg() : ElectionPrepareResponseMsgMiddle() {}// default constructor is required by deserialization, but not actually worked
  ElectionPrepareResponseMsg(const common::ObAddr &self_addr,
                             const LsBiggestMinClusterVersionEverSeen &version,
                             const ElectionPrepareRequestMsgMiddle &request) :
  ElectionPrepareResponseMsgMiddle(self_addr, version, request) {}
  int deserialize(const char* buf, const int64_t data_len, int64_t& pos) {
    int ret = OB_SUCCESS;
    if (OB_FAIL(ElectionPrepareResponseMsgMiddle::deserialize(buf, data_len, pos))) {
      ELECT_LOG(WARN, "deserialize failed", KR(ret));
    }
    debug_ts_.dest_deserialize_ts_ = ObClockGenerator::getRealClock();
    print_debug_ts_if_reach_warn_threshold(*this, MSG_DELAY_WARN_THRESHOLD);
    return ret;
  }
  int64_t get_serialize_size() const {
    if (debug_ts_.src_serialize_ts_ == 0) {// cause get_serialize_size maybe call more than once
      const_cast<int64_t&>(debug_ts_.src_serialize_ts_) = ObClockGenerator::getRealClock();
      print_debug_ts_if_reach_warn_threshold(*this, MSG_DELAY_WARN_THRESHOLD);
    }
    return ElectionPrepareResponseMsgMiddle::get_serialize_size();
  }
};

class ElectionAcceptRequestMsgMiddle : public ElectionMsgBase
{
  OB_UNIS_VERSION(1);
public:
  ElectionAcceptRequestMsgMiddle();// default constructor is required by deserialization, but not actually worked
  ElectionAcceptRequestMsgMiddle(const int64_t id,
                                 const common::ObAddr &self_addr,
                                 const int64_t restart_counter,
                                 const int64_t ballot_number,
                                 const LsBiggestMinClusterVersionEverSeen &version,
                                 const int64_t lease_start_ts_on_proposer,
                                 const int64_t lease_interval,
                                 const LogConfigVersion membership_version);
  int64_t get_lease_start_ts_on_proposer() const;
  int64_t get_lease_interval() const;
  LogConfigVersion get_membership_version() const;
  bool not_less_than_4_2() const { return flag_not_less_than_4_2_; }
  #define BASE "BASE", *(static_cast<const ElectionMsgBase*>(this))
  TO_STRING_KV(BASE, K_(lease_start_ts_on_proposer), K_(lease_interval), K_(membership_version));
  #undef BASE
protected:
  int64_t lease_start_ts_on_proposer_;
  int64_t lease_interval_;
  LogConfigVersion membership_version_;
  bool flag_not_less_than_4_2_;
};
OB_SERIALIZE_MEMBER_TEMP(inline, (ElectionAcceptRequestMsgMiddle, ElectionMsgBase),
                         lease_start_ts_on_proposer_, lease_interval_, membership_version_);

// design wrapper class to record serialize/deserialize time, for debugging
class ElectionAcceptRequestMsg : public ElectionAcceptRequestMsgMiddle
{
public:
  ElectionAcceptRequestMsg() : ElectionAcceptRequestMsgMiddle() {}// default constructor is required by deserialization, but not actually worked
  ElectionAcceptRequestMsg(const int64_t id,
                           const common::ObAddr &self_addr,
                           const int64_t restart_counter,
                           const int64_t ballot_number,
                           const LsBiggestMinClusterVersionEverSeen &version,
                           const int64_t lease_start_ts_on_proposer,
                           const int64_t lease_interval,
                           const LogConfigVersion membership_version) :
  ElectionAcceptRequestMsgMiddle(id, self_addr, restart_counter, ballot_number, version,
                                 lease_start_ts_on_proposer, lease_interval, membership_version) {}
  int deserialize(const char* buf, const int64_t data_len, int64_t& pos) {
    int ret = OB_SUCCESS;
    if (OB_FAIL(ElectionAcceptRequestMsgMiddle::deserialize(buf, data_len, pos))) {
      ELECT_LOG(WARN, "deserialize failed", KR(ret));
    }
    flag_not_less_than_4_2_ = CompatHelper::fetch_msg_flag_not_less_than_4_2(*this);
    debug_ts_.dest_deserialize_ts_ = ObClockGenerator::getRealClock();
    print_debug_ts_if_reach_warn_threshold(*this, MSG_DELAY_WARN_THRESHOLD);
    return ret;
  }
  int64_t get_serialize_size() const {
    if (debug_ts_.src_serialize_ts_ == 0) {// cause get_serialize_size maybe call more than once
      const_cast<int64_t&>(debug_ts_.src_serialize_ts_) = ObClockGenerator::getRealClock();
      print_debug_ts_if_reach_warn_threshold(*this, MSG_DELAY_WARN_THRESHOLD);
    }
    if (flag_not_less_than_4_2_) {
      CompatHelper::set_msg_flag_not_less_than_4_2(*this);
    }
    return ElectionAcceptRequestMsgMiddle::get_serialize_size();
  }
};

struct ElectionPriorityAdaptivedSerializationBuffer
{
  ElectionPriorityAdaptivedSerializationBuffer(unsigned char *buf)
  : priority_buffer_(buf), buffer_used_size_(0) { memset(priority_buffer_, 0, PRIORITY_BUFFER_SIZE); }
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const {// serialize all buffer
    int ret = OB_SUCCESS;
    if (OB_FAIL(serialization::encode(buf, buf_len, pos, buffer_used_size_))) {
      ELECT_LOG(ERROR, "fail to serialize buffer size", K(pos), KR(ret), K_(buffer_used_size));
    } else {
      for (int64_t idx = 0; idx < buffer_used_size_ && OB_SUCC(ret); ++idx) {
        if (OB_FAIL(serialization::encode(buf, buf_len, pos, priority_buffer_[idx]))) {
          ELECT_LOG(ERROR, "fail to serialize buffer", K(idx), K(pos), KR(ret), K_(buffer_used_size));
        }
      }
    }
    return ret;
  }
  int deserialize(const char *buf, const int64_t buf_len, int64_t &pos) {
    int ret = OB_SUCCESS;
    if (OB_FAIL(serialization::decode(buf, buf_len, pos, buffer_used_size_))) {
      ELECT_LOG(ERROR, "fail to serialize buffer size", K(pos), KR(ret), K_(buffer_used_size));
    } else if (buffer_used_size_ > PRIORITY_BUFFER_SIZE) {
      ret = OB_SIZE_OVERFLOW;
      ELECT_LOG(ERROR, "too big deserialized buffer size", K(pos), KR(ret), K_(buffer_used_size));
    } else {
      for (int64_t idx = 0; idx < buffer_used_size_ && OB_SUCC(ret); ++idx) {
        if (OB_FAIL(serialization::decode(buf, buf_len, pos, priority_buffer_[idx]))) {
          ELECT_LOG(ERROR, "fail to serialize buffer", K(idx), K(pos), KR(ret), K_(buffer_used_size));
        }
      }
    }
    return ret;
  }
  int64_t get_serialize_size() const {
    int64_t size = 0;
    size += serialization::encoded_length(buffer_used_size_);
    for (int64_t idx =0; idx < buffer_used_size_; ++idx) {
      size += serialization::encoded_length(priority_buffer_[idx]);
    }
    return size;
  }
  unsigned char *priority_buffer_;
  int64_t buffer_used_size_;
};
class ElectionAcceptResponseMsgMiddle : public ElectionMsgBase
{
  OB_UNIS_VERSION(1);
public:
  ElectionAcceptResponseMsgMiddle();// default constructor is required by deserialization, but not actually worked
  ElectionAcceptResponseMsgMiddle(const common::ObAddr &self_addr,
                                  const uint64_t inner_priority_seed,
                                  const LogConfigVersion &membership_version,
                                  const LsBiggestMinClusterVersionEverSeen &version,
                                  const ElectionAcceptRequestMsgMiddle &request);
  int set_accepted(const int64_t ballot_number, const ElectionPriority *priority);
  void set_rejected(const int64_t ballot_number);
  bool is_accepted() const;
  bool is_buffer_valid() const;
  int64_t get_lease_started_ts_on_proposer() const;
  int64_t get_lease_interval() const;
  const char *get_priority_buffer() const;
  uint64_t get_inner_priority_seed() const;
  LogConfigVersion get_responsed_membership_version() const;
  LogConfigVersion get_membership_version() const;
  ElectionMsgDebugTs get_request_debug_ts() const;
  #define BASE "BASE", *(static_cast<const ElectionMsgBase*>(this))
  TO_STRING_KV(BASE, K_(lease_started_ts_on_proposer), K_(lease_interval), 
               K_(reserved_flag), K_(accepted), K_(flag_not_less_than_4_2),
               K_(is_buffer_valid), K_(responsed_membership_version), K_(inner_priority_seed),
               K_(membership_version), K_(request_debug_ts));
  #undef BASE
protected:
  int64_t lease_started_ts_on_proposer_;
  int64_t lease_interval_;
  int64_t reserved_flag_;
  bool accepted_;
  bool is_buffer_valid_;
  uint64_t inner_priority_seed_;
  LogConfigVersion responsed_membership_version_;
  LogConfigVersion membership_version_;
  ElectionMsgDebugTs request_debug_ts_;
  unsigned char priority_buffer_[PRIORITY_BUFFER_SIZE];
  ElectionPriorityAdaptivedSerializationBuffer fixed_buffer_;
  bool flag_not_less_than_4_2_;
};

// design wrapper class to record serialize/deserialize time, for debugging
class ElectionAcceptResponseMsg : public ElectionAcceptResponseMsgMiddle
{
public:
  ElectionAcceptResponseMsg() : ElectionAcceptResponseMsgMiddle() {}// default constructor is required by deserialization, but not actually worked
  ElectionAcceptResponseMsg(const common::ObAddr &self_addr,
                            const uint64_t inner_priority_seed,
                            const LogConfigVersion &membership_version,
                            const LsBiggestMinClusterVersionEverSeen &version,
                            const ElectionAcceptRequestMsgMiddle &request) :
  ElectionAcceptResponseMsgMiddle(self_addr, inner_priority_seed, membership_version, version, request) {}
  int deserialize(const char* buf, const int64_t data_len, int64_t& pos) {
    int ret = OB_SUCCESS;
    if (OB_FAIL(ElectionAcceptResponseMsgMiddle::deserialize(buf, data_len, pos))) {
      ELECT_LOG(WARN, "deserialize failed", KR(ret));
    }
    debug_ts_.dest_deserialize_ts_ = ObClockGenerator::getRealClock();
    print_debug_ts_if_reach_warn_threshold(*this, MSG_DELAY_WARN_THRESHOLD);
    return ret;
  }
  int64_t get_serialize_size() const {
    if (debug_ts_.src_serialize_ts_ == 0) {// cause get_serialize_size maybe call more than once
      const_cast<int64_t&>(debug_ts_.src_serialize_ts_) = ObClockGenerator::getRealClock();
      print_debug_ts_if_reach_warn_threshold(*this, MSG_DELAY_WARN_THRESHOLD);
    }
    return ElectionAcceptResponseMsgMiddle::get_serialize_size();
  }
};

class ElectionChangeLeaderMsgMiddle : public ElectionMsgBase
{
  OB_UNIS_VERSION(1);
public:
  ElectionChangeLeaderMsgMiddle();// default constructor is required by deserialization, but not actually worked
  ElectionChangeLeaderMsgMiddle(const int64_t id,
                                const common::ObAddr &self_addr,
                                const int64_t restart_counter,
                                const int64_t get_ballot_number,
                                const LsBiggestMinClusterVersionEverSeen &version,
                                int64_t switch_source_leader_ballot,
                                const LogConfigVersion membership_version);
  int64_t get_old_ballot_number() const;
  LogConfigVersion get_membership_version() const;
  #define BASE "BASE", *(static_cast<const ElectionMsgBase*>(this))
  TO_STRING_KV(BASE, K_(switch_source_leader_ballot), K_(membership_version));
  #undef BASE
private:
  int64_t switch_source_leader_ballot_;
  LogConfigVersion membership_version_;
};
OB_SERIALIZE_MEMBER_TEMP(inline, (ElectionChangeLeaderMsgMiddle, ElectionMsgBase),
                         switch_source_leader_ballot_, membership_version_);

// design wrapper class to record serialize/deserialize time, for debugging
class ElectionChangeLeaderMsg : public ElectionChangeLeaderMsgMiddle
{
public:
  ElectionChangeLeaderMsg() : ElectionChangeLeaderMsgMiddle() {}// default constructor is required by deserialization, but not actually worked
  ElectionChangeLeaderMsg(const int64_t id,
                          const common::ObAddr &self_addr,
                          const int64_t restart_counter,
                          const int64_t get_ballot_number,
                          const LsBiggestMinClusterVersionEverSeen &version,
                          int64_t switch_source_leader_ballot,
                          const LogConfigVersion membership_version) :
  ElectionChangeLeaderMsgMiddle(id, self_addr, restart_counter, get_ballot_number, version,
                                switch_source_leader_ballot, membership_version) {}
  int deserialize(const char* buf, const int64_t data_len, int64_t& pos) {
    int ret = OB_SUCCESS;
    if (OB_FAIL(ElectionChangeLeaderMsgMiddle::deserialize(buf, data_len, pos))) {
      ELECT_LOG(WARN, "deserialize failed", KR(ret));
    }
    debug_ts_.dest_deserialize_ts_ = ObClockGenerator::getRealClock();
    print_debug_ts_if_reach_warn_threshold(*this, MSG_DELAY_WARN_THRESHOLD);
    return ret;
  }
  int64_t get_serialize_size() const {
    if (debug_ts_.src_serialize_ts_ == 0) {// cause get_serialize_size maybe call more than once
      const_cast<int64_t&>(debug_ts_.src_serialize_ts_) = ObClockGenerator::getRealClock();
      print_debug_ts_if_reach_warn_threshold(*this, MSG_DELAY_WARN_THRESHOLD);
    }
    return ElectionChangeLeaderMsgMiddle::get_serialize_size();
  }
};

}// namespace election
}// namespace palf
}// namesapce oceanbase

#endif