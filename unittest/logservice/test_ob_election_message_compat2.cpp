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
#include <gtest/gtest.h>
#include <gmock/gmock.h>
#define private public
#define protected public
#include "share/ob_cluster_version.h"
#include "lib/list/ob_dlist.h"
#include "logservice/palf/election/interface/election_msg_handler.h"
#include <algorithm>
#include <chrono>
#include "logservice/palf/election/interface/election.h"
#include "logservice/palf/log_meta_info.h"
#define UNITTEST
#include "logservice/palf/election/utils/election_common_define.h"
#include "logservice/palf/election/algorithm/election_impl.h"
#include "logservice/leader_coordinator/election_priority_impl/election_priority_impl.h"
#include "share/ob_occam_timer.h"
#include "share/rc/ob_tenant_base.h"
#include "mock_logservice_container/mock_election_user.h"
#include <iostream>
#include <vector>
#include "observer/ob_server.h"

using namespace oceanbase::obrpc;
using namespace std;

#define SUCC_(stmt) ASSERT_EQ((stmt), OB_SUCCESS)
#define FAIL_(stmt) ASSERT_EQ((stmt), OB_FAIL)
#define TRUE_(stmt) ASSERT_EQ((stmt), true)
#define FALSE_(stmt) ASSERT_EQ((stmt), false)

namespace oceanbase {
namespace palf
{
namespace election
{
int EventRecorder::report_event_(ElectionEventType, const common::ObString &)
{
  return OB_SUCCESS;
}
}
}
namespace unittest {

using namespace common;
using namespace palf;
using namespace election;
using namespace std;
using namespace logservice::coordinator;

class TestElectionMsgCompat2 : public ::testing::Test {
public:
  TestElectionMsgCompat2() {}
  ~TestElectionMsgCompat2() {}
  virtual void SetUp() {}
  virtual void TearDown() {}
};

namespace election
{
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
  OB_UNIS_VERSION(1);
public:
  ElectionMsgBase();// default constructor is required by deserialization, but not actually worked
  ElectionMsgBase(const int64_t id,
                  const common::ObAddr &self_addr,
                  const int64_t restart_counter,
                  const int64_t ballot_number,
                  const ElectionMsgType msg_type);
  void reset();
  void set_receiver(const common::ObAddr &addr);
  int64_t get_restart_counter() const;
  int64_t get_ballot_number() const;
  const common::ObAddr &get_sender() const;
  const common::ObAddr &get_receiver() const;
  ElectionMsgType get_msg_type() const;
  bool is_valid() const;
  ElectionMsgDebugTs get_debug_ts() const;
  void set_process_ts();
  int64_t get_id() const;
  #define MSG_TYPE "msg_type", msg_type_to_string(static_cast<ElectionMsgType>(msg_type_))
  TO_STRING_KV(MSG_TYPE, K_(id), K_(sender), K_(receiver), K_(restart_counter),
               K_(ballot_number), K_(debug_ts));
  #undef MSG_TYPE
protected:
  int64_t id_;
  common::ObAddr sender_;
  common::ObAddr receiver_;
  int64_t restart_counter_;
  int64_t ballot_number_;
  int64_t msg_type_;
  ElectionMsgDebugTs debug_ts_;
};
OB_SERIALIZE_MEMBER_TEMP(inline, ElectionMsgBase, id_, sender_, receiver_,
                         restart_counter_, ballot_number_, msg_type_, debug_ts_);

class ElectionPrepareRequestMsgMiddle : public ElectionMsgBase
{
  OB_UNIS_VERSION(1);
public:
  ElectionPrepareRequestMsgMiddle();// default constructor is required by deserialization, but not actually worked
  ElectionPrepareRequestMsgMiddle(const int64_t id,
                                  const common::ObAddr &self_addr,
                                  const int64_t restart_counter,
                                  const int64_t ballot_number,
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
                            const uint64_t inner_priority_seed,
                            const LogConfigVersion membership_version) :
  ElectionPrepareRequestMsgMiddle(id, self_addr, restart_counter, ballot_number, inner_priority_seed, membership_version) {}
  int deserialize(const char* buf, const int64_t data_len, int64_t& pos) {
    int ret = OB_SUCCESS;
    if (OB_FAIL(ElectionPrepareRequestMsgMiddle::deserialize(buf, data_len, pos))) {
      ELECT_LOG(WARN, "deserialize failed", KR(ret));
    }
    debug_ts_.dest_deserialize_ts_ = ObClockGenerator::getRealClock();
    //print_debug_ts_if_reach_warn_threshold(*this, MSG_DELAY_WARN_THRESHOLD);
    return ret;
  }
  int64_t get_serialize_size() const {
    if (debug_ts_.src_serialize_ts_ == 0) {// cause get_serialize_size maybe call more than once
      const_cast<int64_t&>(debug_ts_.src_serialize_ts_) = ObClockGenerator::getRealClock();
      //print_debug_ts_if_reach_warn_threshold(*this, MSG_DELAY_WARN_THRESHOLD);
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
                             const ElectionPrepareRequestMsgMiddle &request) :
  ElectionPrepareResponseMsgMiddle(self_addr, request) {}
  int deserialize(const char* buf, const int64_t data_len, int64_t& pos) {
    int ret = OB_SUCCESS;
    if (OB_FAIL(ElectionPrepareResponseMsgMiddle::deserialize(buf, data_len, pos))) {
      ELECT_LOG(WARN, "deserialize failed", KR(ret));
    }
    debug_ts_.dest_deserialize_ts_ = ObClockGenerator::getRealClock();
    //print_debug_ts_if_reach_warn_threshold(*this, MSG_DELAY_WARN_THRESHOLD);
    return ret;
  }
  int64_t get_serialize_size() const {
    if (debug_ts_.src_serialize_ts_ == 0) {// cause get_serialize_size maybe call more than once
      const_cast<int64_t&>(debug_ts_.src_serialize_ts_) = ObClockGenerator::getRealClock();
      //print_debug_ts_if_reach_warn_threshold(*this, MSG_DELAY_WARN_THRESHOLD);
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
                                 const int64_t lease_start_ts_on_proposer,
                                 const int64_t lease_interval,
                                 const LogConfigVersion membership_version);
  int64_t get_lease_start_ts_on_proposer() const;
  int64_t get_lease_interval() const;
  LogConfigVersion get_membership_version() const;
  #define BASE "BASE", *(static_cast<const ElectionMsgBase*>(this))
  TO_STRING_KV(BASE, K_(lease_start_ts_on_proposer), K_(lease_interval), K_(membership_version));
  #undef BASE
private:
  int64_t lease_start_ts_on_proposer_;
  int64_t lease_interval_;
  LogConfigVersion membership_version_;
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
                           const int64_t lease_start_ts_on_proposer,
                           const int64_t lease_interval,
                           const LogConfigVersion membership_version) :
  ElectionAcceptRequestMsgMiddle(id, self_addr, restart_counter, ballot_number,
                                 lease_start_ts_on_proposer, lease_interval, membership_version) {}
  int deserialize(const char* buf, const int64_t data_len, int64_t& pos) {
    int ret = OB_SUCCESS;
    if (OB_FAIL(ElectionAcceptRequestMsgMiddle::deserialize(buf, data_len, pos))) {
      ELECT_LOG(WARN, "deserialize failed", KR(ret));
    }
    debug_ts_.dest_deserialize_ts_ = ObClockGenerator::getRealClock();
    //print_debug_ts_if_reach_warn_threshold(*this, MSG_DELAY_WARN_THRESHOLD);
    return ret;
  }
  int64_t get_serialize_size() const {
    if (debug_ts_.src_serialize_ts_ == 0) {// cause get_serialize_size maybe call more than once
      const_cast<int64_t&>(debug_ts_.src_serialize_ts_) = ObClockGenerator::getRealClock();
      //print_debug_ts_if_reach_warn_threshold(*this, MSG_DELAY_WARN_THRESHOLD);
    }
    return ElectionAcceptRequestMsgMiddle::get_serialize_size();
  }
};

class ElectionAcceptResponseMsgMiddle : public ElectionMsgBase
{
  OB_UNIS_VERSION(1);
public:
  ElectionAcceptResponseMsgMiddle();// default constructor is required by deserialization, but not actually worked
  ElectionAcceptResponseMsgMiddle(const common::ObAddr &self_addr,
                                  const uint64_t inner_priority_seed,
                                  const LogConfigVersion &membership_version,
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
               KTIMERANGE_(process_request_ts, MINUTE, USECOND), K_(accepted),
               K_(is_buffer_valid), K_(responsed_membership_version), K_(inner_priority_seed),
               K_(membership_version), K_(request_debug_ts));
  #undef BASE
protected:
  int64_t lease_started_ts_on_proposer_;
  int64_t lease_interval_;
  int64_t process_request_ts_;
  bool accepted_;
  bool is_buffer_valid_;
  uint64_t inner_priority_seed_;
  LogConfigVersion responsed_membership_version_;
  LogConfigVersion membership_version_;
  ElectionMsgDebugTs request_debug_ts_;
  unsigned char priority_buffer_[PRIORITY_BUFFER_SIZE];
};
OB_SERIALIZE_MEMBER_TEMP(inline, (ElectionAcceptResponseMsgMiddle, ElectionMsgBase),
                         lease_started_ts_on_proposer_, lease_interval_, process_request_ts_,
                         accepted_, is_buffer_valid_, responsed_membership_version_, membership_version_,
                         request_debug_ts_, priority_buffer_, inner_priority_seed_);

// design wrapper class to record serialize/deserialize time, for debugging
class ElectionAcceptResponseMsg : public ElectionAcceptResponseMsgMiddle
{
public:
  ElectionAcceptResponseMsg() : ElectionAcceptResponseMsgMiddle() {}// default constructor is required by deserialization, but not actually worked
  ElectionAcceptResponseMsg(const common::ObAddr &self_addr,
                            const uint64_t inner_priority_seed,
                            const LogConfigVersion &membership_version,
                            const ElectionAcceptRequestMsgMiddle &request) :
  ElectionAcceptResponseMsgMiddle(self_addr, inner_priority_seed, membership_version, request) {}
  int deserialize(const char* buf, const int64_t data_len, int64_t& pos) {
    int ret = OB_SUCCESS;
    if (OB_FAIL(ElectionAcceptResponseMsgMiddle::deserialize(buf, data_len, pos))) {
      ELECT_LOG(WARN, "deserialize failed", KR(ret));
    }
    debug_ts_.dest_deserialize_ts_ = ObClockGenerator::getRealClock();
    //print_debug_ts_if_reach_warn_threshold(*this, MSG_DELAY_WARN_THRESHOLD);
    return ret;
  }
  int64_t get_serialize_size() const {
    if (debug_ts_.src_serialize_ts_ == 0) {// cause get_serialize_size maybe call more than once
      const_cast<int64_t&>(debug_ts_.src_serialize_ts_) = ObClockGenerator::getRealClock();
      //print_debug_ts_if_reach_warn_threshold(*this, MSG_DELAY_WARN_THRESHOLD);
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
                          int64_t switch_source_leader_ballot,
                          const LogConfigVersion membership_version) :
  ElectionChangeLeaderMsgMiddle(id, self_addr, restart_counter, get_ballot_number,
                                switch_source_leader_ballot, membership_version) {}
  int deserialize(const char* buf, const int64_t data_len, int64_t& pos) {
    int ret = OB_SUCCESS;
    if (OB_FAIL(ElectionChangeLeaderMsgMiddle::deserialize(buf, data_len, pos))) {
      ELECT_LOG(WARN, "deserialize failed", KR(ret));
    }
    debug_ts_.dest_deserialize_ts_ = ObClockGenerator::getRealClock();
    //print_debug_ts_if_reach_warn_threshold(*this, MSG_DELAY_WARN_THRESHOLD);
    return ret;
  }
  int64_t get_serialize_size() const {
    if (debug_ts_.src_serialize_ts_ == 0) {// cause get_serialize_size maybe call more than once
      const_cast<int64_t&>(debug_ts_.src_serialize_ts_) = ObClockGenerator::getRealClock();
      //print_debug_ts_if_reach_warn_threshold(*this, MSG_DELAY_WARN_THRESHOLD);
    }
    return ElectionChangeLeaderMsgMiddle::get_serialize_size();
  }
};
}// namespace election

namespace election
{

using namespace common;
using namespace share;

// this is important debug info when meet Lease Expired ERROR! which is a high frequecy error in election
void print_debug_ts_if_reach_warn_threshold(const ElectionMsgBase &msg, const int64_t warn_threshold)
{
  #define PRINT_WRAPPER K(msg), K(req_debug_ts), K(diff), K(max_diff), K(warn_threshold), K(recaculated_warn_threshold)
  int64_t diff = 0;
  ElectionMsgDebugTs req_debug_ts;
  ElectionMsgDebugTs res_debug_ts;
  int64_t recaculated_warn_threshold = warn_threshold;
  // this must be true, or some terrible things happend, and can't be handle, abort is the best way
  OB_ASSERT(msg.get_msg_type() >= ElectionMsgType::PREPARE_REQUEST && msg.get_msg_type() < ElectionMsgType::INVALID_TYPE);
  if (msg.get_msg_type() == ElectionMsgType::PREPARE_RESPONSE) {
    recaculated_warn_threshold += CALCULATE_TIME_WINDOW_SPAN_TS();
    req_debug_ts = static_cast<const ElectionPrepareResponseMsg &>(msg).get_request_debug_ts();
    res_debug_ts = msg.get_debug_ts();
  } else if (msg.get_msg_type() == ElectionMsgType::ACCEPT_RESPONSE) {
    req_debug_ts = static_cast<const ElectionAcceptResponseMsg &>(msg).get_request_debug_ts();
    res_debug_ts = msg.get_debug_ts();
  } else {
    req_debug_ts = msg.get_debug_ts();
  }
  int64_t max_diff = std::max({req_debug_ts.src_construct_ts_, req_debug_ts.src_serialize_ts_,
                              req_debug_ts.dest_deserialize_ts_, req_debug_ts.dest_process_ts_,
                              res_debug_ts.src_construct_ts_, res_debug_ts.src_serialize_ts_,
                              res_debug_ts.dest_deserialize_ts_, res_debug_ts.dest_process_ts_}) - req_debug_ts.src_construct_ts_;
  if (req_debug_ts.src_serialize_ts_ != 0 && (diff = std::abs(req_debug_ts.src_serialize_ts_ - req_debug_ts.src_construct_ts_)) > recaculated_warn_threshold) {
    LOG_NONE_RET(WARN, OB_ERR_UNEXPECTED, "request serialize in src too delay");
  } else if (req_debug_ts.dest_deserialize_ts_ != 0 && (diff = std::abs(req_debug_ts.dest_deserialize_ts_ - req_debug_ts.src_construct_ts_)) > recaculated_warn_threshold) {
    LOG_NONE_RET(WARN, OB_ERR_UNEXPECTED, "request deserialize in dest too delay");
  } else if (req_debug_ts.dest_process_ts_ != 0 && (diff = std::abs(req_debug_ts.dest_process_ts_ - req_debug_ts.src_construct_ts_)) > recaculated_warn_threshold) {
    LOG_NONE_RET(WARN, OB_ERR_UNEXPECTED, "request process in dest too delay");
  } else if (res_debug_ts.src_construct_ts_ != 0 && (diff = std::abs(res_debug_ts.src_construct_ts_ - req_debug_ts.src_construct_ts_)) > recaculated_warn_threshold) {
    LOG_NONE_RET(WARN, OB_ERR_UNEXPECTED, "response construct in src too delay");
  } else if (res_debug_ts.src_serialize_ts_ != 0 && (diff = std::abs(res_debug_ts.src_serialize_ts_ - req_debug_ts.src_construct_ts_)) > recaculated_warn_threshold) {
    LOG_NONE_RET(WARN, OB_ERR_UNEXPECTED, "response serialize in src too delay");
  } else if (res_debug_ts.dest_deserialize_ts_ != 0 && (diff = std::abs(res_debug_ts.dest_deserialize_ts_ - req_debug_ts.src_construct_ts_)) > recaculated_warn_threshold) {
    LOG_NONE_RET(WARN, OB_ERR_UNEXPECTED, "response deserialize in dest too delay");
  } else if (res_debug_ts.dest_process_ts_ != 0 && (diff = std::abs(res_debug_ts.dest_process_ts_ - req_debug_ts.src_construct_ts_)) > recaculated_warn_threshold) {
    LOG_NONE_RET(WARN, OB_ERR_UNEXPECTED, "response process in dest too delay");
  } else if (max_diff > recaculated_warn_threshold) {
    LOG_NONE_RET(WARN, OB_ERR_UNEXPECTED, "max_diff too delay");
  }
  return;
  #undef PRINT_WRAPPER
}

ElectionMsgBase::ElectionMsgBase() :
id_(INVALID_VALUE),
restart_counter_(INVALID_VALUE),
ballot_number_(INVALID_VALUE),
msg_type_(static_cast<int64_t>(ElectionMsgType::INVALID_TYPE)) {}

ElectionMsgBase::ElectionMsgBase(const int64_t id,
                                 const common::ObAddr &self_addr,
                                 const int64_t restart_counter,
                                 const int64_t ballot_number,
                                 const ElectionMsgType msg_type) :
id_(id),
sender_(self_addr),
restart_counter_(restart_counter),
ballot_number_(ballot_number),
msg_type_(static_cast<int64_t>(msg_type)) {
  debug_ts_.src_construct_ts_ = ObClockGenerator::getRealClock();
}

void ElectionMsgBase::reset()
{
  sender_.reset();
  receiver_.reset();
  restart_counter_ = INVALID_VALUE;
  ballot_number_ = INVALID_VALUE;
  msg_type_ = static_cast<int64_t>(ElectionMsgType::INVALID_TYPE);
}

void ElectionMsgBase::set_receiver(const common::ObAddr &addr) { receiver_ = addr; }

int64_t ElectionMsgBase::get_restart_counter() const { return restart_counter_; }

int64_t ElectionMsgBase::get_ballot_number() const { return ballot_number_; }

bool ElectionMsgBase::is_valid() const
{
  return sender_.is_valid() && receiver_.is_valid() && restart_counter_ != INVALID_VALUE && ballot_number_ != INVALID_VALUE &&
         msg_type_ >= static_cast<int64_t>(ElectionMsgType::PREPARE_REQUEST) &&
         msg_type_ <= static_cast<int64_t>(ElectionMsgType::CHANGE_LEADER);
}

const common::ObAddr &ElectionMsgBase::get_sender() const { return sender_; }

const common::ObAddr &ElectionMsgBase::get_receiver() const { return receiver_; }

ElectionMsgType ElectionMsgBase::get_msg_type() const
{
  return static_cast<ElectionMsgType>(msg_type_);
}

ElectionMsgDebugTs ElectionMsgBase::get_debug_ts() const { return debug_ts_; }

void ElectionMsgBase::set_process_ts()
{
  debug_ts_.dest_process_ts_ = ObClockGenerator::getRealClock();
  print_debug_ts_if_reach_warn_threshold(*this, MSG_DELAY_WARN_THRESHOLD);
}

int64_t ElectionMsgBase::get_id() const { return id_; }

ElectionPrepareRequestMsgMiddle::ElectionPrepareRequestMsgMiddle(const int64_t id,
                                                                 const common::ObAddr &self_addr,
                                                                 const int64_t restart_counter,
                                                                 const int64_t ballot_number,
                                                                 const uint64_t inner_priority_seed,
                                                                 const LogConfigVersion membership_version) :
ElectionMsgBase(id,
                self_addr,
                restart_counter,
                ballot_number,
                ElectionMsgType::PREPARE_REQUEST),
role_(ObRole::INVALID_ROLE),
is_buffer_valid_(false),
inner_priority_seed_(inner_priority_seed),
membership_version_(membership_version)
{ memset(priority_buffer_, 0, PRIORITY_BUFFER_SIZE); }

ElectionPrepareRequestMsgMiddle::ElectionPrepareRequestMsgMiddle() :
ElectionMsgBase(),
role_(ObRole::INVALID_ROLE),
is_buffer_valid_(false),
inner_priority_seed_(static_cast<uint64_t>(PRIORITY_SEED_BIT::DEFAULT_SEED))
{
  memset(priority_buffer_, 0, PRIORITY_BUFFER_SIZE);
}

int ElectionPrepareRequestMsgMiddle::set(const ElectionPriority *priority,
                                   const common::ObRole role) {
  ELECT_TIME_GUARD(500_ms);
  int ret = common::OB_SUCCESS;
  role_ = static_cast<int64_t>(role);
  // create_buffer_and_serialize_priority(priority_buffer_, buffer_length_, priority);
  if (OB_NOT_NULL(priority)) {
    int64_t pos = 0;
    if (CLICK_FAIL(priority->serialize((char*)priority_buffer_, PRIORITY_BUFFER_SIZE, pos))) {
      ELECT_LOG(ERROR, "fail to serialize priority");
    } else {
      is_buffer_valid_ = true;
    }
  }
  return ret;
}

bool ElectionPrepareRequestMsgMiddle::is_buffer_valid() const { return is_buffer_valid_; }

const char *ElectionPrepareRequestMsgMiddle::get_priority_buffer() const { return (char*)priority_buffer_; }

common::ObRole ElectionPrepareRequestMsgMiddle::get_role() const { return static_cast<common::ObRole>(role_); }

LogConfigVersion ElectionPrepareRequestMsgMiddle::get_membership_version() const { return membership_version_; }

uint64_t ElectionPrepareRequestMsgMiddle::get_inner_priority_seed() const { return inner_priority_seed_; }

ElectionPrepareResponseMsgMiddle::ElectionPrepareResponseMsgMiddle() :
ElectionMsgBase(),
accepted_(false) {}

ElectionPrepareResponseMsgMiddle::
ElectionPrepareResponseMsgMiddle(const ObAddr &self_addr,
                                 const ElectionPrepareRequestMsgMiddle &request) :
ElectionMsgBase(request.get_id(),
                self_addr,
                request.get_restart_counter(),
                INVALID_VALUE,
                ElectionMsgType::PREPARE_RESPONSE),
accepted_(false) {
  set_receiver(request.get_sender());
  request_debug_ts_ = request.get_debug_ts();
  print_debug_ts_if_reach_warn_threshold(*this, MSG_DELAY_WARN_THRESHOLD);
}

void ElectionPrepareResponseMsgMiddle::set_accepted(const int64_t ballot_number, const Lease lease) {
  ballot_number_ = ballot_number;
  lease_ = lease;
  accepted_ = true;
}

bool ElectionPrepareResponseMsgMiddle::is_accepted() const { return accepted_; }

void ElectionPrepareResponseMsgMiddle::set_rejected(const int64_t ballot_number) {
  ballot_number_ = ballot_number;
  accepted_ = false;
}

const Lease &ElectionPrepareResponseMsgMiddle::get_lease() const { return lease_; }

ElectionAcceptRequestMsgMiddle::ElectionAcceptRequestMsgMiddle() :
ElectionMsgBase(),
lease_start_ts_on_proposer_(0),
lease_interval_(0) {}

ElectionMsgDebugTs ElectionPrepareResponseMsgMiddle::get_request_debug_ts() const { return request_debug_ts_; }

ElectionAcceptRequestMsgMiddle::ElectionAcceptRequestMsgMiddle(const int64_t id,
                                                               const ObAddr &self_addr,
                                                               const int64_t restart_counter,
                                                               const int64_t ballot_number,
                                                               const int64_t lease_start_ts_on_proposer,
                                                               const int64_t lease_interval,
                                                               const LogConfigVersion membership_version) :
ElectionMsgBase(id,
                self_addr,
                restart_counter,
                ballot_number,
                ElectionMsgType::ACCEPT_REQUEST),
lease_start_ts_on_proposer_(lease_start_ts_on_proposer),
lease_interval_(lease_interval),
membership_version_(membership_version) {}

int64_t ElectionAcceptRequestMsgMiddle::get_lease_start_ts_on_proposer() const
{
  return lease_start_ts_on_proposer_;
}

int64_t ElectionAcceptRequestMsgMiddle::get_lease_interval() const { return lease_interval_; }

ElectionAcceptResponseMsgMiddle::ElectionAcceptResponseMsgMiddle() :
ElectionMsgBase(),
lease_started_ts_on_proposer_(0),
lease_interval_(0),
accepted_(false),
is_buffer_valid_(false),
inner_priority_seed_(static_cast<uint64_t>(PRIORITY_SEED_BIT::DEFAULT_SEED))
{
  memset(priority_buffer_, 0, PRIORITY_BUFFER_SIZE);
}

LogConfigVersion ElectionAcceptRequestMsgMiddle::get_membership_version() const { return membership_version_; }

ElectionAcceptResponseMsgMiddle::
ElectionAcceptResponseMsgMiddle(const ObAddr &self_addr,
                                const uint64_t inner_priority_seed,
                                const LogConfigVersion &membership_version,
                                const ElectionAcceptRequestMsgMiddle &request) :
ElectionMsgBase(request.get_id(),
                self_addr,
                request.get_restart_counter(),
                INVALID_VALUE,
                ElectionMsgType::ACCEPT_RESPONSE),
lease_started_ts_on_proposer_(request.get_lease_start_ts_on_proposer()),
lease_interval_(request.get_lease_interval()),
accepted_(false),
is_buffer_valid_(false),
inner_priority_seed_(inner_priority_seed),
responsed_membership_version_(request.get_membership_version()),
membership_version_(membership_version)
{
  set_receiver(request.get_sender());
  memset(priority_buffer_, 0, PRIORITY_BUFFER_SIZE);
  request_debug_ts_ = request.get_debug_ts();
}

int ElectionAcceptResponseMsgMiddle::set_accepted(const int64_t ballot_number,
                                                  const ElectionPriority *priority) {
  ELECT_TIME_GUARD(500_ms);
  int ret = common::OB_SUCCESS;
  ballot_number_ = ballot_number;
  accepted_ = true;
  int64_t pos = 0;
  if (OB_NOT_NULL(priority)) {
    if (CLICK_FAIL(priority->serialize((char*)priority_buffer_, PRIORITY_BUFFER_SIZE, pos))) {
      ELECT_LOG(ERROR, "fail to serialize priority");
    } else {
      is_buffer_valid_ = true;
    }
  }
  return ret;
}

void ElectionAcceptResponseMsgMiddle::set_rejected(const int64_t ballot_number) {
  ballot_number_ = ballot_number;
  accepted_ = false;
}

bool ElectionAcceptResponseMsgMiddle::is_accepted() const { return accepted_; }

int64_t ElectionAcceptResponseMsgMiddle::get_lease_started_ts_on_proposer() const
{
  return lease_started_ts_on_proposer_;
}

int64_t ElectionAcceptResponseMsgMiddle::get_lease_interval() const { return lease_interval_; }

bool ElectionAcceptResponseMsgMiddle::is_buffer_valid() const { return is_buffer_valid_; }

const char *ElectionAcceptResponseMsgMiddle::get_priority_buffer() const { return (char*)priority_buffer_; }

LogConfigVersion ElectionAcceptResponseMsgMiddle::get_responsed_membership_version() const { return responsed_membership_version_; }

LogConfigVersion ElectionAcceptResponseMsgMiddle::get_membership_version() const { return membership_version_; }

ElectionMsgDebugTs ElectionAcceptResponseMsgMiddle::get_request_debug_ts() const { return request_debug_ts_; }

uint64_t ElectionAcceptResponseMsgMiddle::get_inner_priority_seed() const { return inner_priority_seed_; }

ElectionChangeLeaderMsgMiddle::ElectionChangeLeaderMsgMiddle() :
ElectionMsgBase(),
switch_source_leader_ballot_(INVALID_VALUE) {}

ElectionChangeLeaderMsgMiddle::ElectionChangeLeaderMsgMiddle(const int64_t id,
                                                             const ObAddr &self_addr,
                                                             const int64_t restart_counter,
                                                             const int64_t ballot_number,
                                                             int64_t switch_source_leader_ballot,
                                                             const LogConfigVersion membership_version) :
ElectionMsgBase(id,
                self_addr,
                restart_counter,
                ballot_number,
                ElectionMsgType::CHANGE_LEADER),
switch_source_leader_ballot_(switch_source_leader_ballot),
membership_version_(membership_version) {}

LogConfigVersion ElectionChangeLeaderMsgMiddle::get_membership_version() const { return membership_version_; }

int64_t ElectionChangeLeaderMsgMiddle::get_old_ballot_number() const { return switch_source_leader_ballot_; }

}// namespace election
/***********************************************************************/

TEST_F(TestElectionMsgCompat2, old_to_new) {
  constexpr int64_t BUFFER_SIZE = 2048;
  char buffer[BUFFER_SIZE] = {0};
  LogConfigVersion config_version;
  config_version.generate(1, 1);
  int64_t pos = 0;
  unittest::ElectionAcceptRequestMsg msg_request_old(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 1), 1, 1, LsBiggestMinClusterVersionEverSeen(CLUSTER_VERSION_4_1_0_0), 123, 234, config_version);
  palf::election::ElectionAcceptRequestMsg msg_request_new;
  ASSERT_EQ(msg_request_old.serialize(buffer, BUFFER_SIZE, pos), OB_SUCCESS);
  int64_t pos2 = 0;
  ASSERT_EQ(msg_request_new.deserialize(buffer, BUFFER_SIZE, pos2), OB_SUCCESS);
  ASSERT_EQ(pos, pos2);
  ASSERT_EQ(msg_request_new.flag_not_less_than_4_2_, false);

  ElectionPriorityImpl priority;
  unittest::ElectionAcceptResponseMsg msg_response_old(ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 1), 1, config_version, LsBiggestMinClusterVersionEverSeen(CLUSTER_VERSION_4_1_0_0), msg_request_old);
  palf::election::ElectionAcceptResponseMsg msg_response_new;
  ASSERT_EQ(msg_response_old.set_accepted(1, &priority), OB_SUCCESS);
  pos = 0;
  ASSERT_EQ(msg_response_old.serialize(buffer, BUFFER_SIZE, pos), OB_SUCCESS);
  pos2 = 0;
  ASSERT_EQ(msg_response_new.deserialize(buffer, BUFFER_SIZE, pos2), OB_SUCCESS);
  ASSERT_EQ(pos, pos2);
  ASSERT_EQ(msg_response_new.flag_not_less_than_4_2_, false);
}

TEST_F(TestElectionMsgCompat2, new_to_old_fake_new) {
  observer::ObServer::get_instance().gctx_.startup_mode_ = observer::NORMAL_MODE;
  oceanbase::common::ObClusterVersion::get_instance().cluster_version_ = CLUSTER_VERSION_4_1_0_0;
  constexpr int64_t BUFFER_SIZE = 2048;
  char buffer[BUFFER_SIZE] = {0};
  LogConfigVersion config_version;
  config_version.generate(1, 1);
  int64_t pos = 0;
  ASSERT_EQ(observer::ObServer::get_instance().is_arbitration_mode(), false);
  ASSERT_EQ(GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_2_0_0, true);
  palf::election::ElectionAcceptRequestMsg msg_request_new(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 1), 1, 1, LsBiggestMinClusterVersionEverSeen(CLUSTER_VERSION_4_1_0_0), 123, 234, config_version);
  ASSERT_EQ(msg_request_new.flag_not_less_than_4_2_, false);
  unittest::ElectionAcceptRequestMsg msg_request_old;
  int64_t serialize_size = msg_request_new.get_serialize_size();
  ASSERT_EQ(msg_request_new.serialize(buffer, BUFFER_SIZE, pos), OB_SUCCESS);
  ASSERT_EQ(serialize_size, pos);
  int64_t pos2 = 0;
  ASSERT_EQ(msg_request_old.deserialize(buffer, BUFFER_SIZE, pos2), OB_SUCCESS);
  ASSERT_EQ(pos, pos2);

  ElectionPriorityImpl priority;
  palf::election::ElectionAcceptResponseMsg msg_response_new(ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 1), 1, config_version, LsBiggestMinClusterVersionEverSeen(CLUSTER_VERSION_4_1_0_0), msg_request_old);
  ASSERT_EQ(msg_response_new.flag_not_less_than_4_2_, false);
  unittest::ElectionAcceptResponseMsg msg_response_old;
  ASSERT_EQ(msg_response_new.set_accepted(1, &priority), OB_SUCCESS);
  pos = 0;
  serialize_size = msg_response_new.get_serialize_size();
  ASSERT_EQ(msg_response_new.serialize(buffer, BUFFER_SIZE, pos), OB_SUCCESS);
  ASSERT_EQ(serialize_size, pos);
  pos2 = 0;
  ASSERT_EQ(msg_response_old.deserialize(buffer, BUFFER_SIZE, pos2), OB_SUCCESS);
  ASSERT_EQ(pos, pos2);
}

TEST_F(TestElectionMsgCompat2, new_to_new_real_new) {
  observer::ObServer::get_instance().gctx_.startup_mode_ = observer::NORMAL_MODE;
  oceanbase::common::ObClusterVersion::get_instance().cluster_version_ = CLUSTER_VERSION_4_2_0_0;
  constexpr int64_t BUFFER_SIZE = 2048;
  char buffer[BUFFER_SIZE] = {0};
  LogConfigVersion config_version;
  config_version.generate(1, 1);
  int64_t pos = 0;
  ASSERT_EQ(observer::ObServer::get_instance().is_arbitration_mode(), false);
  ASSERT_EQ(GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_2_0_0, false);
  palf::election::ElectionAcceptRequestMsg msg_request_src(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 1), 1, 1, LsBiggestMinClusterVersionEverSeen(CLUSTER_VERSION_4_1_0_0), 123, 234, config_version);
  ASSERT_EQ(msg_request_src.flag_not_less_than_4_2_, true);
  palf::election::ElectionAcceptRequestMsg msg_request_dst;
  int64_t serialize_size = msg_request_src.get_serialize_size();
  ASSERT_EQ(msg_request_src.serialize(buffer, BUFFER_SIZE, pos), OB_SUCCESS);
  ASSERT_EQ(serialize_size, pos);
  int64_t pos2 = 0;
  ASSERT_EQ(msg_request_dst.deserialize(buffer, BUFFER_SIZE, pos2), OB_SUCCESS);
  ASSERT_EQ(pos, pos2);
  ASSERT_EQ(msg_request_dst.flag_not_less_than_4_2_, true);

  ElectionPriorityImpl priority;
  palf::election::ElectionAcceptResponseMsg msg_response_src(ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 1), 1, config_version,  LsBiggestMinClusterVersionEverSeen(CLUSTER_VERSION_4_1_0_0), msg_request_dst);
  ASSERT_EQ(msg_response_src.flag_not_less_than_4_2_, true);
  palf::election::ElectionAcceptResponseMsg msg_response_dst;
  ASSERT_EQ(msg_response_src.set_accepted(1, &priority), OB_SUCCESS);
  pos = 0;
  serialize_size = msg_response_src.get_serialize_size();
  ASSERT_EQ(msg_response_src.serialize(buffer, BUFFER_SIZE, pos), OB_SUCCESS);
  ASSERT_EQ(serialize_size, pos);
  pos2 = 0;
  ASSERT_EQ(msg_response_dst.deserialize(buffer, BUFFER_SIZE, pos2), OB_SUCCESS);
  ASSERT_EQ(pos, pos2);
  ASSERT_EQ(msg_response_dst.flag_not_less_than_4_2_, true);
}

}
}

int main(int argc, char **argv)
{
  system("rm -rf test_ob_election_message_compat2.log");
  oceanbase::common::ObClockGenerator::init();
  oceanbase::common::ObLogger &logger = oceanbase::common::ObLogger::get_logger();
  logger.set_file_name("test_ob_election_message_compat2.log", false);
  logger.set_log_level(OB_LOG_LEVEL_DEBUG);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
