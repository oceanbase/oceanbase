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
#include "logservice/palf/log_meta_entry.h"              // LogMetaEntry
#include "logservice/palf/log_meta_info.h"               // LogPrepareMeta...
#include "logservice/palf/log_meta.h"                    // LogMeta
#include "logservice/palf/palf_options.h"

#include <gtest/gtest.h>

namespace oceanbase
{
namespace unittest
{

using namespace common;
using namespace palf;

TEST(TestLogMetaEntry, test_log_meta_entry)
{
  int64_t proposal_id = INVALID_PROPOSAL_ID;
  proposal_id = 1;
  ObAddr addr(ObAddr::IPV4, "127.0.0.1", 4096);
  // Stream meta

  // Prepare meta
  LogPrepareMeta log_prepare_meta1;
  EXPECT_EQ(OB_SUCCESS, log_prepare_meta1.generate(LogVotedFor(), proposal_id));

  // Membership meta
  LogConfigMeta log_config_meta1;
  ObAddr addr1(ObAddr::IPV4, "127.0.0.1", 4096);
  ObAddr addr2(ObAddr::IPV4, "127.0.0.1", 4097);
  ObAddr addr3(ObAddr::IPV4, "127.0.0.1", 4098);
  ObAddr addr4(ObAddr::IPV4, "127.0.0.1", 4099);
  ObMember learner1(addr3, 1);
  ObMember learner2(addr4, 1);
  ObMember member1(addr1, 1);
  ObMember member2(addr2, 1);
  LSN prev_lsn; prev_lsn.val_ = 1;
  int64_t prev_log_proposal_id = INVALID_PROPOSAL_ID; prev_log_proposal_id = 1;
  int64_t prev_config_seq = 1;
  int64_t prev_replica_num = 1;
  ObMemberList prev_member_list;
  prev_member_list.add_member(member1);
  int64_t curr_config_seq = 1;
  int64_t curr_replica_num = 1;
  LSN curr_lsn; curr_lsn.val_ = 1;
  int64_t curr_log_proposal_id = INVALID_PROPOSAL_ID; curr_log_proposal_id = 1;
  ObMemberList curr_member_list;
  curr_member_list.add_member(member2);
  common::GlobalLearnerList prev_learner_list;
  prev_learner_list.add_learner(learner1);
  common::GlobalLearnerList curr_learner_list;
  curr_learner_list.add_learner(learner2);

  LogConfigInfoV2 prev_config_info;
  LogConfigInfoV2 curr_config_info;
  LogConfigVersion prev_config_version;
  LogConfigVersion curr_config_version;

  EXPECT_EQ(OB_SUCCESS, prev_config_version.generate(prev_log_proposal_id, prev_config_seq));
  EXPECT_EQ(OB_SUCCESS, curr_config_version.generate(curr_log_proposal_id, curr_config_seq));
  EXPECT_EQ(OB_SUCCESS, prev_config_info.generate(prev_member_list, prev_replica_num, prev_learner_list, prev_config_version));
  EXPECT_EQ(OB_SUCCESS, curr_config_info.generate(curr_member_list, curr_replica_num, prev_learner_list, curr_config_version));
  EXPECT_EQ(OB_SUCCESS, log_config_meta1.generate(curr_log_proposal_id, prev_config_info, curr_config_info,
      curr_log_proposal_id, LSN(0), curr_log_proposal_id));

  // Snapshot meta
  LogSnapshotMeta log_snapshot_meta1;
  LSN lsn; lsn.val_ = 1;
  EXPECT_EQ(OB_SUCCESS, log_snapshot_meta1.generate(lsn));

  LogReplicaPropertyMeta replica_meta1;
  replica_meta1.generate(true, LogReplicaType::NORMAL_REPLICA);

  LogMeta log_meta1;
  // Test invalid
  EXPECT_FALSE(log_meta1.is_valid());

  const int64_t BUFSIZE = 1 << 21;
  char buf[BUFSIZE];
  int64_t pos = 0;
  // Test serialize and deserialize
  EXPECT_EQ(OB_SUCCESS, log_meta1.update_log_config_meta(log_config_meta1));
  EXPECT_EQ(OB_SUCCESS, log_meta1.serialize(buf, BUFSIZE, pos));
  EXPECT_EQ(pos, log_meta1.get_serialize_size());

  LogMetaEntryHeader log_meta_entry_header;
  EXPECT_EQ(OB_SUCCESS, log_meta_entry_header.generate(buf, pos));
  EXPECT_TRUE(log_meta_entry_header.check_integrity(buf, pos));

  // Test invalid argument
  LogMetaEntry log_meta_entry1;
  EXPECT_FALSE(log_meta_entry1.is_valid());
  EXPECT_EQ(OB_SUCCESS, log_meta_entry1.generate(log_meta_entry_header, buf));
  EXPECT_TRUE(log_meta_entry1.is_valid());

  // Test serialize and deserialize
  char buf1[BUFSIZE];
  pos = 0;
  EXPECT_EQ(OB_SUCCESS, log_meta_entry1.serialize(buf1, BUFSIZE, pos));
  EXPECT_EQ(pos, log_meta_entry1.get_serialize_size());
  EXPECT_TRUE(log_meta_entry1.check_integrity());
  LogMetaEntry log_meta_entry2;
  pos = 0;
  EXPECT_EQ(OB_SUCCESS, log_meta_entry2.deserialize(buf1, BUFSIZE, pos));
  EXPECT_TRUE(log_meta_entry2.check_integrity());
}

} // end of unittest
} // end of oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_log_meta_entry.log", true);
  OB_LOGGER.set_log_level("INFO");
  PALF_LOG(INFO, "begin unittest::test_log_meta_entry");
  ::testing::InitGoogleTest(&argc, argv);
  oceanbase::ObClusterVersion::get_instance().update_data_version(DATA_CURRENT_VERSION);
  return RUN_ALL_TESTS();
}
