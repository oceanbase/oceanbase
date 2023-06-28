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

#define private public
#include "logservice/palf/log_meta_info.h"            // LogPrepareMeta...
#include "logservice/palf/palf_options.h"
#include "share/scn.h"
#undef private
#include <gtest/gtest.h>

namespace oceanbase
{
using namespace common;
using namespace palf;
constexpr int64_t palf::LogConfigMeta::LOG_CONFIG_META_VERSION;
constexpr int64_t palf::LogConfigMeta::LOG_CONFIG_META_VERSION_INC;
namespace unittest
{

TEST(TestLogMetaInfos, test_log_voted_for)
{
  common::ObAddr addr(ObAddr::IPV4, "127.0.0.1", 1234);
  LogVotedFor voted_for(addr);
  PALF_LOG(INFO, "voted_for" , K(voted_for));
  char buf[1024];
  int64_t pos = 0;
  EXPECT_EQ(OB_SUCCESS, voted_for.serialize(buf, 1024, pos));
  LogVotedFor voted_for1;
  pos = 0;
  EXPECT_EQ(OB_SUCCESS, voted_for1.deserialize(buf, 1024, pos));
  ObAddr addr2;
  addr2.version_ = ObAddr::IPV4;
  addr2.ip_.v4_ = voted_for1.voted_for_[0];
  addr2.port_ = voted_for1.voted_for_[1];
  EXPECT_EQ(addr, addr2);
  ObAddr addr_v6;
  addr_v6.version_ = ObAddr::IPV6;
  addr_v6.set_ipv6_addr("fe80::754d:f834:4606:13", 1234);
  LogVotedFor voted_for_v6(addr_v6);
  pos = 0;
  voted_for_v6.serialize(buf, 1024, pos);
  LogVotedFor voted_for_v6_1;
  pos = 0;
  voted_for_v6_1.deserialize(buf, 1024, pos);
  ObAddr addr_v6_1;
  addr_v6_1.version_ = ObAddr::IPV6;
  memcpy(addr_v6_1.ip_.v6_, voted_for_v6.voted_for_, 2*sizeof(int64_t));
  addr_v6_1.port_ = voted_for_v6.voted_for_[2];
  EXPECT_EQ(addr_v6_1, addr_v6);
  PALF_LOG(INFO, "voted_for_v6", K(voted_for_v6), K(addr_v6));
}

TEST(TestLogMetaInfos, test_log_prepare_meta)
{
  static const int64_t BUFSIZE = 1 << 21;
  char buf[BUFSIZE];
  int64_t proposal_id = INVALID_PROPOSAL_ID;
  proposal_id = 1;
  LogPrepareMeta log_prepare_meta1;
  // Test invalid argument
  EXPECT_FALSE(log_prepare_meta1.is_valid());
  common::ObAddr addr(ObAddr::IPV4, "127.0.0.1", 1234);
  LogVotedFor voted_for(addr);
  EXPECT_EQ(OB_SUCCESS, log_prepare_meta1.generate(voted_for, proposal_id));
  EXPECT_TRUE(log_prepare_meta1.is_valid());

  // Test serialize and deserialize
  EXPECT_TRUE(log_prepare_meta1.is_valid());
  int64_t pos = 0;
  EXPECT_EQ(OB_SUCCESS, log_prepare_meta1.serialize(buf, BUFSIZE, pos));
  EXPECT_EQ(pos, log_prepare_meta1.get_serialize_size());
  pos = 0;
  LogPrepareMeta log_prepare_meta2;
  EXPECT_EQ(OB_SUCCESS, log_prepare_meta2.deserialize(buf, BUFSIZE, pos));
  EXPECT_EQ(log_prepare_meta1.log_proposal_id_, log_prepare_meta1.log_proposal_id_);
}

TEST(TestLogMetaInfos, test_log_config_meta)
{
  //test LogLockMeta
  LogLockMeta lock_meta;
  EXPECT_EQ(false, lock_meta.is_valid());
  lock_meta.version_ = LogLockMeta::LOG_LOCK_META_VERSION;
  EXPECT_EQ(true, lock_meta.is_valid());
  EXPECT_EQ(false, lock_meta.is_lock_owner_valid());

  lock_meta.lock_owner_ = 1;
  lock_meta.lock_type_ = LOCK_NOTHING;
  EXPECT_EQ(false, lock_meta.is_valid());
  EXPECT_EQ(OB_INVALID_ARGUMENT, lock_meta.generate(1, LOCK_NOTHING));
  EXPECT_EQ(OB_INVALID_ARGUMENT, lock_meta.generate(-1, LOCK_PAXOS_MEMBER_CHANGE));
  EXPECT_EQ(OB_SUCCESS, lock_meta.generate(1, LOCK_PAXOS_MEMBER_CHANGE));
  EXPECT_EQ(true, lock_meta.is_locked());
  EXPECT_EQ(true, lock_meta.is_lock_owner_valid());

  lock_meta.unlock();
  EXPECT_EQ(false, lock_meta.is_locked());
  EXPECT_EQ(LOCK_NOTHING, lock_meta.lock_type_);
  EXPECT_EQ(1, lock_meta.lock_owner_);
  EXPECT_EQ(true, lock_meta.version_ == LogLockMeta::LOG_LOCK_META_VERSION);
  EXPECT_EQ(true, lock_meta.is_lock_owner_valid());

  lock_meta.reset_as_unlocked();
  EXPECT_EQ(false, lock_meta.is_locked());
  EXPECT_EQ(LOCK_NOTHING, lock_meta.lock_type_);
  EXPECT_EQ(-1, lock_meta.lock_owner_);
  EXPECT_EQ(true, lock_meta.version_ == LogLockMeta::LOG_LOCK_META_VERSION);
  EXPECT_EQ(false, lock_meta.is_lock_owner_valid());

  LogLockMeta new_lock_meta;
  new_lock_meta = lock_meta;
  EXPECT_EQ(true, new_lock_meta == lock_meta);

  const int64_t SER_BUF_SIZE = 512;
  char ser_buf[SER_BUF_SIZE] = {0};
  int64_t pos = 0;
  EXPECT_EQ(OB_SUCCESS, lock_meta.serialize(ser_buf, SER_BUF_SIZE, pos));
  int64_t new_pos = 0;
  EXPECT_EQ(OB_SUCCESS, new_lock_meta.deserialize(ser_buf, pos, new_pos));
  EXPECT_EQ(pos, new_pos);
  EXPECT_EQ(new_lock_meta, lock_meta);
  //end of test LogLockMeta

  static const int64_t BUFSIZE = 1 << 21;
  ObAddr addr1(ObAddr::IPV4, "127.0.0.1", 4096);
  ObAddr addr2(ObAddr::IPV4, "127.0.0.1", 4097);
  ObAddr addr3(ObAddr::IPV4, "127.0.0.1", 4098);
  ObAddr addr4(ObAddr::IPV4, "127.0.0.1", 4099);
  ObMember member1(addr1, 1);
  ObMember member2(addr2, 1);
  ObMember learner1(addr3, 1);
  ObMember learner2(addr4, 1);
  LSN prev_lsn; prev_lsn.val_ = 1;
  int64_t prev_log_proposal_id = 1;
  int64_t prev_config_seq = 1;
  int64_t prev_replica_num = 1;
  ObMemberList prev_member_list;
  prev_member_list.add_member(member1);
  int64_t curr_config_seq = 1;
  int64_t curr_replica_num = 1;
  LSN curr_lsn; curr_lsn.val_ = 1;
  int64_t curr_log_proposal_id = 1;
  ObMemberList curr_member_list;
  curr_member_list.add_member(member2);
  common::GlobalLearnerList prev_learner_list;
  prev_learner_list.add_learner(learner1);
  common::GlobalLearnerList curr_learner_list;
  curr_learner_list.add_learner(learner2);

  LogConfigVersion prev_config_version;
  LogConfigVersion curr_config_version;
  LogConfigInfoV2 prev_config_info;
  LogConfigInfoV2 curr_config_info;
  LogConfigInfo old_config_info;
  LogConfigInfoV2 new_config_info;

  // log barrier
  const int64_t barrier_log_proposal_id = 3;
  const LSN barrier_lsn = LSN(300);
  const int64_t barrier_mode_pid = 4;

  // Test default version
  LogConfigMeta default_config_meta;
  EXPECT_EQ(OB_SUCCESS, default_config_meta.generate_for_default(curr_log_proposal_id, prev_config_info, curr_config_info));
  EXPECT_TRUE(default_config_meta.is_valid());
  EXPECT_EQ(palf::LogConfigMeta::LOG_CONFIG_META_VERSION_INC, default_config_meta.version_);

  // Test invalid argument
  LogConfigMeta log_config_meta;
  EXPECT_EQ(OB_INVALID_ARGUMENT, log_config_meta.generate(curr_log_proposal_id, prev_config_info, curr_config_info,
      barrier_log_proposal_id, barrier_lsn, barrier_mode_pid));
  EXPECT_FALSE(log_config_meta.is_valid());

  EXPECT_EQ(OB_SUCCESS, prev_config_version.generate(prev_log_proposal_id, prev_config_seq));
  EXPECT_EQ(OB_SUCCESS, curr_config_version.generate(curr_log_proposal_id, curr_config_seq));
  lock_meta.reset();
  EXPECT_EQ(OB_INVALID_ARGUMENT, prev_config_info.generate(prev_member_list, prev_replica_num, prev_learner_list, prev_config_version, lock_meta));
  lock_meta.reset_as_unlocked();
  ASSERT_EQ(OB_SUCCESS, prev_config_info.generate(prev_member_list, prev_replica_num, prev_learner_list, prev_config_version, lock_meta));
  EXPECT_EQ(OB_SUCCESS, prev_config_info.generate(prev_member_list, prev_replica_num, prev_learner_list, prev_config_version));
  EXPECT_EQ(OB_SUCCESS, curr_config_info.generate(curr_member_list, curr_replica_num, curr_learner_list, curr_config_version));
  EXPECT_TRUE(curr_config_info.is_valid());
  EXPECT_TRUE(prev_config_info.is_valid());

  // test lists overlap
//  {
//    LogConfigInfo invalid_info = curr_config_info; //TODO(yaoying): fix it
//    invalid_info.learnerlist_.add_learner(member2);
//    EXPECT_FALSE(invalid_info.is_valid());
//  }

  EXPECT_EQ(OB_SUCCESS, old_config_info.generate(curr_member_list, curr_replica_num, curr_learner_list, curr_config_version));
  EXPECT_EQ(OB_SUCCESS, new_config_info.generate(old_config_info));
  EXPECT_EQ(true, new_config_info.lock_meta_.is_valid());
  EXPECT_EQ(false, new_config_info.lock_meta_.is_locked());

  // test basic serialization
  {
    char buf[BUFSIZE];
    LogConfigMeta log_config_meta1;
    EXPECT_EQ(OB_SUCCESS, log_config_meta1.generate(curr_log_proposal_id, prev_config_info, curr_config_info,
        barrier_log_proposal_id, barrier_lsn, barrier_mode_pid));
    EXPECT_TRUE(log_config_meta1.is_valid());

    // Test serialzie and deserialize
    int64_t pos = 0;
    EXPECT_EQ(OB_SUCCESS, log_config_meta1.serialize(buf, BUFSIZE, pos));
    EXPECT_EQ(pos, log_config_meta1.get_serialize_size());
    pos = 0;
    LogConfigMeta log_config_meta2;
    EXPECT_EQ(OB_SUCCESS, log_config_meta2.deserialize(buf, BUFSIZE, pos));
    EXPECT_TRUE(log_config_meta1.proposal_id_ == log_config_meta2.proposal_id_);
    EXPECT_TRUE(log_config_meta1.prev_ ==
                log_config_meta2.prev_);
    EXPECT_TRUE(log_config_meta1.curr_ ==
                log_config_meta2.curr_);
    EXPECT_TRUE(log_config_meta1.prev_log_proposal_id_ == log_config_meta2.prev_log_proposal_id_);
    EXPECT_TRUE(log_config_meta1.prev_lsn_ == log_config_meta2.prev_lsn_);
    EXPECT_TRUE(log_config_meta1.prev_mode_pid_ == log_config_meta2.prev_mode_pid_);
    PALF_LOG(INFO, "trace", K(log_config_meta1), K(log_config_meta2));
  }
  // test compatibility (new code deserializes old data)
  {
    char buf[BUFSIZE];
    LogConfigMeta log_config_meta1;
    EXPECT_EQ(OB_SUCCESS, log_config_meta1.generate(curr_log_proposal_id, prev_config_info, curr_config_info,
        barrier_log_proposal_id, barrier_lsn, barrier_mode_pid));
    EXPECT_TRUE(log_config_meta1.is_valid());
    // assign old version
    log_config_meta1.version_ = LogConfigMeta::LOG_CONFIG_META_VERSION;

    int64_t pos = 0;
    EXPECT_EQ(OB_SUCCESS, log_config_meta1.serialize(buf, BUFSIZE, pos));
    EXPECT_EQ(pos, log_config_meta1.get_serialize_size());
    pos = 0;
    LogConfigMeta log_config_meta2;
    EXPECT_EQ(OB_SUCCESS, log_config_meta2.deserialize(buf, BUFSIZE, pos));
    EXPECT_TRUE(log_config_meta1.proposal_id_ == log_config_meta2.proposal_id_);
    EXPECT_TRUE(log_config_meta1.prev_ ==
                log_config_meta2.prev_);
    EXPECT_TRUE(log_config_meta1.curr_ ==
                log_config_meta2.curr_);
    EXPECT_EQ(log_config_meta2.prev_log_proposal_id_, INVALID_PROPOSAL_ID);
    EXPECT_FALSE(log_config_meta2.prev_lsn_.is_valid());
    EXPECT_EQ(log_config_meta2.prev_mode_pid_, INVALID_PROPOSAL_ID);
    PALF_LOG(INFO, "trace", K(log_config_meta1), K(log_config_meta2));
  }
}

TEST(TestLogMetaInfos, test_log_config_info_convert)
{
  static const int64_t BUFSIZE = 1 << 21;
  char buf[BUFSIZE];
  ObAddr addr1(ObAddr::IPV4, "127.0.0.1", 4096);
  ObAddr addr2(ObAddr::IPV4, "127.0.0.1", 4097);
  ObAddr addr3(ObAddr::IPV4, "127.0.0.1", 4098);
  ObAddr addr4(ObAddr::IPV4, "127.0.0.1", 4099);
  ObAddr addr5(ObAddr::IPV4, "127.0.0.1", 4100);
  ObAddr addr6(ObAddr::IPV4, "127.0.0.1", 4101);
  ObAddr addr7(ObAddr::IPV4, "127.0.0.1", 4102);
  ObAddr addr8(ObAddr::IPV4, "127.0.0.1", 4103);
  int64_t curr_config_seq = 1;
  LogConfigVersion curr_config_version;
  int64_t curr_log_proposal_id = INVALID_PROPOSAL_ID; curr_log_proposal_id = 1;
  EXPECT_EQ(OB_SUCCESS, curr_config_version.generate(curr_log_proposal_id, curr_config_seq));
  // 2F1A, 2 degraded learners
  {
    LogConfigInfoV2 curr_config_info_v2;

    LogConfigInfo &curr_config_info = curr_config_info_v2.config_;
    int64_t log_sync_replica_num = 2;
    ObMemberList log_sync_member_list;
    log_sync_member_list.add_member(ObMember(addr1, 1));
    log_sync_member_list.add_member(ObMember(addr2, 1));
    common::ObMember arb_replica = ObMember(addr3, 1);
    ObMemberList expected_paxos_memberlist = log_sync_member_list;
    expected_paxos_memberlist.add_member(arb_replica);
    const int64_t expected_paxos_replica_num = log_sync_replica_num + 1;

    common::GlobalLearnerList curr_learner_list;
    curr_learner_list.add_learner(ObMember(addr4, 1));
    curr_config_info.degraded_learnerlist_.add_learner(ObMember(addr5, 1));
    curr_config_info.degraded_learnerlist_.add_learner(ObMember(addr6, 1));
    GlobalLearnerList expected_learner_list;
    expected_learner_list.append(curr_learner_list);
    expected_learner_list.append(curr_config_info.degraded_learnerlist_);
    
    EXPECT_EQ(OB_SUCCESS, curr_config_info.generate(log_sync_member_list, log_sync_replica_num, curr_learner_list, curr_config_version));
    curr_config_info.arbitration_member_ = arb_replica;

    common::ObMemberList result_memberlist;
    int64_t result_replica_num;
    GlobalLearnerList result_learners;
    EXPECT_EQ(OB_SUCCESS, curr_config_info_v2.convert_to_complete_config(result_memberlist, result_replica_num, result_learners));
    EXPECT_EQ(result_replica_num, expected_paxos_replica_num);
    EXPECT_TRUE(result_memberlist.member_addr_equal(expected_paxos_memberlist));
    EXPECT_EQ(3, result_memberlist.get_member_number());
    EXPECT_TRUE(result_learners.learner_addr_equal(expected_learner_list));
    EXPECT_EQ(3, result_learners.get_member_number());
  }
}

TEST(TestLogMetaInfos, test_log_mode_meta)
{
  static const int64_t BUFSIZE = 1 << 21;
  char buf[BUFSIZE];
  LogModeMeta log_mode_meta1;
  LSN lsn; lsn.val_ = 1;
  ObAddr addr(ObAddr::IPV4, "127.0.0.1", 4096);

  share::SCN invalid_scn;
  // Test invalid argument
  EXPECT_FALSE(log_mode_meta1.is_valid());
  EXPECT_EQ(OB_INVALID_ARGUMENT, log_mode_meta1.generate(1, 1, AccessMode::INVALID_ACCESS_MODE, share::SCN::min_scn()));
  EXPECT_EQ(OB_INVALID_ARGUMENT, log_mode_meta1.generate(1, 1, AccessMode::APPEND, invalid_scn));
  EXPECT_EQ(OB_INVALID_ARGUMENT, log_mode_meta1.generate(1, INVALID_PROPOSAL_ID, AccessMode::APPEND, share::SCN::min_scn()));
  EXPECT_EQ(OB_INVALID_ARGUMENT, log_mode_meta1.generate(INVALID_PROPOSAL_ID, 1, AccessMode::APPEND, share::SCN::min_scn()));
  EXPECT_EQ(OB_SUCCESS, log_mode_meta1.generate(1, 1, AccessMode::APPEND, share::SCN::min_scn()));
  EXPECT_TRUE(log_mode_meta1.is_valid());

  // Test serialize and deserialize
  int64_t pos = 0;
  EXPECT_EQ(OB_SUCCESS, log_mode_meta1.serialize(buf, BUFSIZE, pos));
  EXPECT_EQ(pos, log_mode_meta1.get_serialize_size());
  pos = 0;
  LogModeMeta log_mode_meta2;
  EXPECT_EQ(OB_SUCCESS, log_mode_meta2.deserialize(buf, BUFSIZE, pos));
  const bool equal = (log_mode_meta1.mode_version_ == log_mode_meta2.mode_version_ &&
                      log_mode_meta1.proposal_id_ == log_mode_meta2.proposal_id_ &&
                      log_mode_meta1.access_mode_ == log_mode_meta2.access_mode_ &&
                      log_mode_meta1.ref_scn_ == log_mode_meta2.ref_scn_);
  EXPECT_TRUE(equal);
}

TEST(TestLogMetaInfos, test_log_snapshot_meta)
{
  static const int64_t BUFSIZE = 1 << 21;
  char buf[BUFSIZE];
  LogSnapshotMeta log_snapshot_meta1;
  LSN lsn; lsn.val_ = 1;
  ObAddr addr(ObAddr::IPV4, "127.0.0.1", 4096);

  // Test invalid argument
  EXPECT_FALSE(log_snapshot_meta1.is_valid());
  EXPECT_EQ(OB_SUCCESS, log_snapshot_meta1.generate(lsn));
  EXPECT_TRUE(log_snapshot_meta1.is_valid());

  // Test serialize and deserialize
  int64_t pos = 0;
  EXPECT_EQ(OB_SUCCESS, log_snapshot_meta1.serialize(buf, BUFSIZE, pos));
  EXPECT_EQ(pos, log_snapshot_meta1.get_serialize_size());
  pos = 0;
  LogSnapshotMeta log_snapshot_meta2;
  EXPECT_EQ(OB_SUCCESS, log_snapshot_meta2.deserialize(buf, BUFSIZE, pos));
  EXPECT_EQ(log_snapshot_meta1.base_lsn_,
            log_snapshot_meta2.base_lsn_);
}

TEST(TestLogReplicaPropertyMeta, test_log_replica_property_meta)
{
  static const int64_t BUFSIZE = 1 << 21;
  char buf[BUFSIZE];
  LogReplicaPropertyMeta replica_meta;
  LogReplicaPropertyMeta replica_meta1;
  EXPECT_FALSE(replica_meta.is_valid());
  EXPECT_EQ(OB_SUCCESS, replica_meta.generate(true, LogReplicaType::NORMAL_REPLICA));
  replica_meta.reset();
  EXPECT_FALSE(replica_meta.is_valid());
  EXPECT_EQ(OB_SUCCESS, replica_meta.generate(false, LogReplicaType::NORMAL_REPLICA));
  replica_meta1 = replica_meta;
  EXPECT_EQ(replica_meta.version_, replica_meta1.version_);
  EXPECT_EQ(replica_meta.allow_vote_, replica_meta1.allow_vote_);

  int64_t pos = 0;
  EXPECT_EQ(OB_SUCCESS, replica_meta.generate(true, LogReplicaType::NORMAL_REPLICA));
  EXPECT_EQ(OB_SUCCESS, replica_meta.serialize(buf, BUFSIZE, pos));
  EXPECT_EQ(pos, replica_meta.get_serialize_size());
  pos = 0;
  EXPECT_EQ(OB_SUCCESS, replica_meta1.deserialize(buf, BUFSIZE, pos));
  EXPECT_EQ(replica_meta.version_, replica_meta1.version_);
  EXPECT_EQ(replica_meta.allow_vote_, replica_meta1.allow_vote_);
}

TEST(TestLogMetaInfos, test_log_config_version)
{
  LogConfigVersion cv1;
  EXPECT_FALSE(cv1.is_valid());
  EXPECT_EQ(OB_INVALID_ARGUMENT, cv1.generate(0, -1));
  EXPECT_EQ(OB_NOT_INIT, cv1.inc_update_version(1));
  EXPECT_EQ(OB_SUCCESS, cv1.generate(1, 1));
  EXPECT_EQ(OB_INVALID_ARGUMENT, cv1.inc_update_version(0));
  EXPECT_EQ(OB_SUCCESS, cv1.inc_update_version(1));
  EXPECT_EQ(2, cv1.config_seq_);
  cv1.reset();
  EXPECT_FALSE(cv1.is_valid());
  {
    LogConfigVersion cv2;
    LogConfigVersion cv3;
    EXPECT_FALSE(cv2 < cv3);
    EXPECT_FALSE(cv2 > cv3);
    EXPECT_TRUE(cv2 <= cv3);
    EXPECT_TRUE(cv2 >= cv3);
  }
  {
    LogConfigVersion cv2;
    LogConfigVersion cv3;
    EXPECT_EQ(OB_SUCCESS, cv2.generate(1, 1));
    EXPECT_TRUE(cv2 > cv3);
    EXPECT_TRUE(cv2 >= cv3);
    EXPECT_FALSE(cv2 == cv3);
  }
  {
    LogConfigVersion cv2;
    LogConfigVersion cv3;
    EXPECT_EQ(OB_SUCCESS, cv2.generate(1, 1));
    EXPECT_EQ(OB_SUCCESS, cv3.generate(1, 1));
    EXPECT_TRUE(cv2 == cv3);
    EXPECT_EQ(OB_SUCCESS, cv2.inc_update_version(1));
    EXPECT_TRUE(cv2 > cv3);
    EXPECT_EQ(OB_SUCCESS, cv3.inc_update_version(2));
    EXPECT_TRUE(cv2 < cv3);
  }
}

} // end of unittest
} // end of oceanbase

int main(int args, char **argv)
{
  OB_LOGGER.set_file_name("test_log_meta_infos.log", true);
  OB_LOGGER.set_log_level("INFO");
  PALF_LOG(INFO, "begin unittest::test_log_meta_infos");
  ::testing::InitGoogleTest(&args, argv);
  oceanbase::ObClusterVersion::get_instance().update_data_version(DATA_CURRENT_VERSION);
  return RUN_ALL_TESTS();
}

