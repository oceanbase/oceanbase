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

#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "logservice/palf/log_define.h"
#define private public
#include "logservice/palf/log_meta.h"
#undef private
#include <gtest/gtest.h>

namespace oceanbase
{
namespace unittest
{
using namespace common;
using namespace palf;

TEST(TestLogMeta, test_log_meta)
{
  int64_t proposal_id = INVALID_PROPOSAL_ID;
  proposal_id = 1;
  ObAddr addr(ObAddr::IPV4, "127.0.0.1", 4096);

  // Prepare meta
  LogPrepareMeta log_prepare_meta1;
  EXPECT_EQ(OB_SUCCESS, log_prepare_meta1.generate(LogVotedFor(), proposal_id));

  // Membership meta
  LogConfigMeta log_config_meta1;
  ObAddr addr1(ObAddr::IPV4, "127.0.0.1", 4096);
  ObAddr addr2(ObAddr::IPV4, "127.0.0.1", 4097);
  ObMember member1(addr1, 1);
  ObMember member2(addr2, 1);
  ObAddr addr3(ObAddr::IPV4, "127.0.0.1", 4098);
  ObAddr addr4(ObAddr::IPV4, "127.0.0.1", 4099);
  ObMember learner1(addr3, 1);
  ObMember learner2(addr4, 1);
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

  LogConfigVersion prev_config_version;
  LogConfigVersion curr_config_version;
  EXPECT_EQ(OB_INVALID_ARGUMENT, prev_config_version.generate(INVALID_PROPOSAL_ID, -1));
  EXPECT_EQ(OB_SUCCESS, prev_config_version.generate(prev_log_proposal_id, prev_config_seq));
  EXPECT_EQ(OB_SUCCESS, curr_config_version.generate(curr_log_proposal_id, curr_config_seq));
  EXPECT_TRUE(curr_config_version.is_valid());

  LogConfigInfoV2 prev_config_info;
  EXPECT_EQ(OB_INVALID_ARGUMENT, prev_config_info.generate(prev_member_list, -1, prev_learner_list, prev_config_version));
  EXPECT_EQ(OB_SUCCESS, prev_config_info.generate(prev_member_list, prev_replica_num, prev_learner_list, prev_config_version));
  LogConfigInfoV2 curr_config_info;
  EXPECT_EQ(OB_SUCCESS, curr_config_info.generate(curr_member_list, curr_replica_num, curr_learner_list, curr_config_version));
  EXPECT_TRUE(curr_config_info.is_valid());
  EXPECT_EQ(OB_SUCCESS, log_config_meta1.generate(curr_log_proposal_id, prev_config_info, curr_config_info,
      curr_log_proposal_id, LSN(0), curr_log_proposal_id));
  EXPECT_TRUE(log_config_meta1.is_valid());

  // Snapshot meta
  LogSnapshotMeta log_snapshot_meta1;
  LSN lsn; lsn.val_ = 1;
  EXPECT_EQ(OB_INVALID_ARGUMENT, log_snapshot_meta1.generate(LSN()));
  EXPECT_EQ(OB_SUCCESS, log_snapshot_meta1.generate(lsn));
  EXPECT_TRUE(log_snapshot_meta1.is_valid());

  // replica property meta
  LogReplicaPropertyMeta replica_meta1;
  EXPECT_EQ(OB_SUCCESS, replica_meta1.generate(true, LogReplicaType::NORMAL_REPLICA));

  const int64_t init_log_proposal_id(0);
  LogConfigMeta log_config_meta;
  LogConfigInfoV2 init_config_info;
  LogConfigVersion init_config_version;
  EXPECT_EQ(OB_SUCCESS, init_config_version.generate(init_log_proposal_id, 0));
  EXPECT_EQ(OB_SUCCESS, init_config_info.generate(init_config_version));
  log_config_meta.version_ = LogConfigMeta::LOG_CONFIG_META_VERSION_INC;
  log_config_meta.proposal_id_ = init_log_proposal_id;
  log_config_meta.curr_ = init_config_info;
  log_config_meta.prev_ = init_config_info;
  LogMeta log_meta1;
  log_meta1.update_log_prepare_meta(log_prepare_meta1);
  EXPECT_EQ(OB_SUCCESS, log_meta1.update_log_config_meta(log_config_meta));
  // Test invalid
  EXPECT_FALSE(log_meta1.is_valid());

  const int64_t BUFSIZE = 1 << 21;
  char buf[BUFSIZE];
  int64_t pos = 0;
  // Test serialize and deserialize
  EXPECT_EQ(OB_SUCCESS, log_meta1.serialize(buf, BUFSIZE, pos));
  EXPECT_EQ(pos, log_meta1.get_serialize_size());
  pos = 0;
  LogMeta log_meta2;
  EXPECT_EQ(OB_SUCCESS, log_meta2.deserialize(buf, BUFSIZE, pos));
  EXPECT_EQ(log_prepare_meta1.log_proposal_id_,
            log_meta2.get_log_prepare_meta().log_proposal_id_);
}

TEST(TestLogMeta, test_log_meta_generate)
{
  LogMeta meta1, meta2;
  LSN prev_lsn(10000), lsn(20000);
  int64_t init_pid(2);
  share::SCN init_scn;
  init_scn.convert_for_logservice(10);
  int64_t init_cksum(10);
  PalfBaseInfo base_info;
  LogInfo log_info;
  log_info.log_id_ = 1;
  log_info.scn_ = init_scn;
  log_info.log_proposal_id_ = init_pid;
  log_info.accum_checksum_ = init_cksum;
  // invalid lsn
  log_info.lsn_ = lsn;
  base_info.curr_lsn_ = prev_lsn;
  base_info.prev_log_info_ = log_info;
  EXPECT_EQ(OB_INVALID_ARGUMENT, meta1.generate_by_palf_base_info(base_info, AccessMode::APPEND, palf::NORMAL_REPLICA));
  // valid lsn
  log_info.lsn_ = prev_lsn;
  base_info.curr_lsn_ = lsn;
  base_info.prev_log_info_ = log_info;
  EXPECT_EQ(OB_SUCCESS, meta1.generate_by_palf_base_info(base_info, AccessMode::APPEND, palf::NORMAL_REPLICA));
  EXPECT_EQ(meta1.log_prepare_meta_.log_proposal_id_, base_info.prev_log_info_.log_proposal_id_);
  EXPECT_EQ(meta1.log_config_meta_.proposal_id_, base_info.prev_log_info_.log_proposal_id_);
  EXPECT_EQ(meta1.log_config_meta_.curr_.config_.config_version_.proposal_id_, base_info.prev_log_info_.log_proposal_id_);
  EXPECT_EQ(meta1.log_config_meta_.prev_.config_.config_version_.proposal_id_, base_info.prev_log_info_.log_proposal_id_);
  EXPECT_EQ(meta1.log_mode_meta_.proposal_id_, base_info.prev_log_info_.log_proposal_id_);
  EXPECT_EQ(meta1.log_mode_meta_.mode_version_, base_info.prev_log_info_.log_proposal_id_);
  EXPECT_EQ(meta1.log_snapshot_meta_.base_lsn_, base_info.curr_lsn_);
  EXPECT_EQ(meta1.log_snapshot_meta_.prev_log_info_, base_info.prev_log_info_);
}
}
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_log_meta.log", true);
  OB_LOGGER.set_log_level("INFO");
  PALF_LOG(INFO, "begin unittest::test_log_meta");
  ::testing::InitGoogleTest(&argc, argv);
  oceanbase::ObClusterVersion::get_instance().update_data_version(DATA_CURRENT_VERSION);
  return RUN_ALL_TESTS();
}
