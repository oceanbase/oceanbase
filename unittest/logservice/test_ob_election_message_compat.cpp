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

class TestElectionMsgCompat : public ::testing::Test {
public:
  TestElectionMsgCompat() {}
  ~TestElectionMsgCompat() {}
  virtual void SetUp() {}
  virtual void TearDown() {}
};

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

class ElectionPrepareRequestMsgMiddleOld : public oceanbase::unittest::ElectionMsgBase
{
  OB_UNIS_VERSION(1);
public:
  ElectionPrepareRequestMsgMiddleOld();// default constructor is required by deserialization, but not actually worked
  ElectionPrepareRequestMsgMiddleOld(const int64_t id,
                                     const common::ObAddr &self_addr,
                                     const int64_t restart_counter,
                                     const int64_t ballot_number,
                                     const LogConfigVersion membership_version);
  int set(const ElectionPriority *priority, const common::ObRole role);
  const char *get_priority_buffer() const;
  bool is_buffer_valid() const;
  common::ObRole get_role() const;
  LogConfigVersion get_membership_version() const;
  #define BASE "BASE", *(static_cast<const ElectionMsgBase*>(this))
  #define ROLE "role", obj_to_string(static_cast<common::ObRole>(role_))
  TO_STRING_KV(KP(this), BASE, ROLE, K_(is_buffer_valid), K_(membership_version));
  #undef ROLE
  #undef BASE
protected:
  int64_t role_;
  bool is_buffer_valid_;
  LogConfigVersion membership_version_;
  unsigned char priority_buffer_[PRIORITY_BUFFER_SIZE];
};
OB_SERIALIZE_MEMBER_TEMP(inline, (ElectionPrepareRequestMsgMiddleOld, ElectionMsgBase), role_,
                         is_buffer_valid_, membership_version_, priority_buffer_);

ElectionPrepareRequestMsgMiddleOld::ElectionPrepareRequestMsgMiddleOld(const int64_t id,
                                                                 const common::ObAddr &self_addr,
                                                                 const int64_t restart_counter,
                                                                 const int64_t ballot_number,
                                                                 const LogConfigVersion membership_version) :
ElectionMsgBase(id,
                self_addr,
                restart_counter,
                ballot_number,
                ElectionMsgType::PREPARE_REQUEST),
role_(ObRole::INVALID_ROLE),
is_buffer_valid_(false),
membership_version_(membership_version)
{ memset(priority_buffer_, 0, PRIORITY_BUFFER_SIZE); }

ElectionPrepareRequestMsgMiddleOld::ElectionPrepareRequestMsgMiddleOld() :
ElectionMsgBase(),
role_(ObRole::INVALID_ROLE),
is_buffer_valid_(false) { memset(priority_buffer_, 0, PRIORITY_BUFFER_SIZE); }

int ElectionPrepareRequestMsgMiddleOld::set(const ElectionPriority *priority,
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

bool ElectionPrepareRequestMsgMiddleOld::is_buffer_valid() const { return is_buffer_valid_; }

const char *ElectionPrepareRequestMsgMiddleOld::get_priority_buffer() const { return (char*)priority_buffer_; }

common::ObRole ElectionPrepareRequestMsgMiddleOld::get_role() const { return static_cast<common::ObRole>(role_); }

LogConfigVersion ElectionPrepareRequestMsgMiddleOld::get_membership_version() const { return membership_version_; }

// design wrapper class to record serialize/deserialize time, for debugging
class ElectionPrepareRequestMsgOld : public ElectionPrepareRequestMsgMiddleOld
{
public:
  ElectionPrepareRequestMsgOld() : ElectionPrepareRequestMsgMiddleOld() {}// default constructor is required by deserialization, but not actually worked
  ElectionPrepareRequestMsgOld(const int64_t id,
                            const common::ObAddr &self_addr,
                            const int64_t restart_counter,
                            const int64_t ballot_number,
                            const LogConfigVersion membership_version) :
  ElectionPrepareRequestMsgMiddleOld(id, self_addr, restart_counter, ballot_number, membership_version) {}
  int deserialize(const char* buf, const int64_t data_len, int64_t& pos) {
    int ret = ElectionPrepareRequestMsgMiddleOld::deserialize(buf, data_len, pos);
    debug_ts_.dest_deserialize_ts_ = ObClockGenerator::getRealClock();
    // print_debug_ts_if_reach_warn_threshold(*this, MSG_DELAY_WARN_THRESHOLD);
    return ret;
  }
  int64_t get_serialize_size() const {
    if (debug_ts_.src_serialize_ts_ == 0) {// cause get_serialize_size maybe call more than once
      const_cast<int64_t&>(debug_ts_.src_serialize_ts_) = ObClockGenerator::getRealClock();
      // print_debug_ts_if_reach_warn_threshold(*this, MSG_DELAY_WARN_THRESHOLD);
    }
    return ElectionPrepareRequestMsgMiddleOld::get_serialize_size();
  }
};

TEST_F(TestElectionMsgCompat, old_new_msg_serialize) {
  LogConfigVersion config_version;
  config_version.generate(1, 1);
  ElectionPrepareRequestMsgOld prepare_msg_old1(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 1), 1, 1, config_version);
  ElectionPrepareRequestMsg prepare_msg_new1;
  ElectionPrepareRequestMsg prepare_msg_new2(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 1), 1, 1, LsBiggestMinClusterVersionEverSeen(CLUSTER_VERSION_4_1_0_0), (1 << 10), config_version);
  ElectionPrepareRequestMsgOld prepare_msg_old2;
  constexpr int64_t buffer_size = 2_KB;
  char buffer[buffer_size];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, prepare_msg_old1.serialize(buffer, buffer_size, pos));
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, prepare_msg_new1.deserialize(buffer, buffer_size, pos));
  ELECT_LOG(INFO, "test msg from old to new", K(prepare_msg_old1), K(prepare_msg_new1));
  ASSERT_EQ((uint64_t)PRIORITY_SEED_BIT::DEFAULT_SEED, prepare_msg_new1.get_inner_priority_seed());
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, prepare_msg_new2.serialize(buffer, buffer_size, pos));
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, prepare_msg_old2.deserialize(buffer, buffer_size, pos));
  ELECT_LOG(INFO, "test msg from new to old", K(prepare_msg_new2), K(prepare_msg_old2));
}

}
}

int main(int argc, char **argv)
{
  system("rm -rf test_ob_election_message_compat.log");
  oceanbase::common::ObClockGenerator::init();
  oceanbase::common::ObLogger &logger = oceanbase::common::ObLogger::get_logger();
  logger.set_file_name("test_ob_election_message_compat.log", false);
  logger.set_log_level(OB_LOG_LEVEL_TRACE);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
