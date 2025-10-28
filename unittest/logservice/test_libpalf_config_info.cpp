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
#define private public
#include "close_modules/shared_log_service/logservice/libpalf/libpalf_proposor_member_info.h"
#undef private

namespace oceanbase
{
using namespace common;
using namespace palf;
using namespace libpalf;
namespace unittest
{

// Helper function to create ObMember
ObMember create_member(const char* ip, int32_t port, int64_t timestamp = 0, int64_t flag = 0)
{
  ObAddr addr;
  if (timestamp == 0) {
    timestamp = ObTimeUtility::current_time();
  }
  addr.set_ip_addr(ip, port);
  return ObMember(addr, timestamp, flag);
}

TEST(TestLibPalfConfigInfo, test_apply_config_change_basic)
{
  PALF_LOG(INFO, "test_apply_config_change_basic");
  LibPalfMemberInfo member_info;
  MemberChangeArgs args;
  bool is_already_finished = false;

  // Test basic functionality with empty args
  int ret = member_info.apply_config_change(args, is_already_finished);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(is_already_finished);
}

TEST(TestLibPalfConfigInfo, test_apply_config_change_add_unique_members)
{
  PALF_LOG(INFO, "test_apply_config_change_add_unique_members");
  LibPalfMemberInfo member_info;
  MemberChangeArgs args;
  bool is_already_finished = false;

  // Initialize with some existing members
  ObMember existing_member1 = create_member("127.0.0.1", 1001, 1);
  ObMember existing_member2 = create_member("127.0.0.1", 1002, 1);
  ObMember existing_learner1 = create_member("127.0.0.2", 1001, 1);

  // Add existing members using direct method (simulating initial state)
  ASSERT_EQ(OB_SUCCESS, member_info.full_memberlist_.add_member(existing_member1));
  ASSERT_EQ(OB_SUCCESS, member_info.full_memberlist_.add_member(existing_member2));
  ASSERT_EQ(OB_SUCCESS, member_info.learner_list_.add_learner(existing_learner1));

  // Create new unique members to add
  ObMember new_member1 = create_member("127.0.0.1", 1003, 2);
  ObMember new_member2 = create_member("127.0.0.2", 1002, 2);

  // Add new unique members to full_memberlist
  ASSERT_EQ(OB_SUCCESS, args.append_full_member_list_op(MEMBER_CHANGE_OP_ADD, new_member1));
  ASSERT_EQ(OB_SUCCESS, args.append_full_member_list_op(MEMBER_CHANGE_OP_ADD, new_member2));

  // Apply config change - should succeed
  int ret = member_info.apply_config_change(args, is_already_finished);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_FALSE(is_already_finished); // Should have made changes

  // Verify members were added (2 existing + 2 new = 4 total)
  ASSERT_EQ(4, member_info.get_full_memberlist().get_member_number());
  ASSERT_EQ(1, member_info.get_learner_list().get_member_number());
}

TEST(TestLibPalfConfigInfo, test_apply_config_change_add_duplicate_members)
{
  PALF_LOG(INFO, "test_apply_config_change_add_duplicate_members");
  LibPalfMemberInfo member_info;
  MemberChangeArgs args;
  bool is_already_finished = false;

  // Initialize with existing members
  ObMember existing_member1 = create_member("127.0.0.1", 1001, 1);
  ObMember existing_member2 = create_member("127.0.0.1", 1002, 1);

  ASSERT_EQ(OB_SUCCESS, member_info.full_memberlist_.add_member(existing_member1));
  ASSERT_EQ(OB_SUCCESS, member_info.full_memberlist_.add_member(existing_member2));

  // Create duplicate members (same server as existing)
  ObMember duplicate_member1 = create_member("127.0.0.1", 1001, 1); // Same as existing_member1
  ObMember duplicate_member2 = create_member("127.0.0.1", 1001, 1, 2); // Same as existing_member1

  // Add duplicate members to full_memberlist
  ASSERT_EQ(OB_SUCCESS, args.append_full_member_list_op(MEMBER_CHANGE_OP_ADD, duplicate_member1));
  ASSERT_EQ(OB_SUCCESS, member_info.apply_config_change(args, is_already_finished));
  ASSERT_TRUE(is_already_finished);
  args.reset();
  ASSERT_EQ(OB_SUCCESS, args.append_full_member_list_op(MEMBER_CHANGE_OP_ADD, duplicate_member2));
  ASSERT_EQ(OB_SUCCESS, member_info.apply_config_change(args, is_already_finished));
  ASSERT_TRUE(is_already_finished);
  args.reset();
  ASSERT_EQ(OB_SUCCESS, args.append_learner_list_op(MEMBER_CHANGE_OP_ADD, duplicate_member1));
  ASSERT_EQ(OB_INVALID_ARGUMENT, member_info.apply_config_change(args, is_already_finished));
  // Verify no new members were added due to failure (still 2 existing)
  ASSERT_EQ(2, member_info.get_full_memberlist().get_member_number());
}

TEST(TestLibPalfConfigInfo, test_apply_config_change_add_cross_list_duplicates)
{
  PALF_LOG(INFO, "test_apply_config_change_add_cross_list_duplicates");
  LibPalfMemberInfo member_info;
  MemberChangeArgs args;
  bool is_already_finished = false;

  // Initialize with existing members
  ObMember existing_member1 = create_member("127.0.0.1", 1001, 1);
  ObMember existing_learner1 = create_member("127.0.0.2", 1001, 1);

  ASSERT_EQ(OB_SUCCESS, member_info.full_memberlist_.add_member(existing_member1));
  ASSERT_EQ(OB_SUCCESS, member_info.learner_list_.add_learner(existing_learner1));

  // Create members with same server for both lists (cross-list duplicates)
  ObMember new_member1 = create_member("127.0.0.2", 1001, 2); // Same server as existing_learner1
  ObMember new_learner1 = create_member("127.0.0.1", 1001, 2); // Same server as existing_member1

  // Add member to full_memberlist and learner to learner_list
  ASSERT_EQ(OB_SUCCESS, args.append_full_member_list_op(MEMBER_CHANGE_OP_ADD, new_member1));
  // Apply config change - should fail due to cross-list duplicates
  ASSERT_EQ(OB_INVALID_ARGUMENT, member_info.apply_config_change(args, is_already_finished));

  args.reset();
  ASSERT_EQ(OB_SUCCESS, args.append_learner_list_op(MEMBER_CHANGE_OP_ADD, new_learner1));
  // Apply config change - should fail due to cross-list duplicates
  ASSERT_EQ(OB_INVALID_ARGUMENT, member_info.apply_config_change(args, is_already_finished));
}

TEST(TestLibPalfConfigInfo, test_apply_config_change_add_duplicate_learners)
{
  PALF_LOG(INFO, "test_apply_config_change_add_duplicate_learners");
  LibPalfMemberInfo member_info;
  MemberChangeArgs args;
  bool is_already_finished = false;

  // Create duplicate learners (same server)
  ObMember learner1 = create_member("127.0.0.1", 1001, 1, 2);
  ObMember learner2 = create_member("127.0.0.1", 1001, 1);
  ObMember learner3 = create_member("127.0.0.1", 1001, 2, 2); // Same server as learner1 with flag 2

  ASSERT_EQ(OB_SUCCESS, member_info.learner_list_.add_learner(learner1));
  // Add duplicate learners to learner_list
  ASSERT_EQ(OB_SUCCESS, args.append_learner_list_op(MEMBER_CHANGE_OP_REMOVE, learner1));
  ASSERT_EQ(OB_SUCCESS, args.append_learner_list_op(MEMBER_CHANGE_OP_ADD, learner2));

  // Apply config change - should fail due to duplicates
  ASSERT_EQ(OB_SUCCESS, member_info.apply_config_change(args, is_already_finished));
  ASSERT_FALSE(is_already_finished);

  args.reset();
  ASSERT_EQ(OB_SUCCESS, args.append_learner_list_op(MEMBER_CHANGE_OP_ADD, learner3));
  ASSERT_EQ(OB_INVALID_ARGUMENT, member_info.apply_config_change(args, is_already_finished));
}

TEST(TestLibPalfConfigInfo, test_apply_config_change_set_member_with_duplicates)
{
  PALF_LOG(INFO, "test_apply_config_change_set_member_with_duplicates");
  LibPalfMemberInfo member_info;
  MemberChangeArgs args;
  bool is_already_finished = false;

  // Create member list with duplicates
  ObMemberList member_list;
  ObMember member1 = create_member("127.0.0.1", 1001, 1);
  ObMember member2 = create_member("127.0.0.1", 1002, 1); // Same server as member1

  ASSERT_EQ(OB_SUCCESS, member_list.add_member(member1));
  ASSERT_EQ(OB_SUCCESS, member_list.add_member(member2));

  // Set member list with duplicates
  ASSERT_EQ(OB_SUCCESS, args.append_full_member_list_op(MEMBER_CHANGE_OP_SET, member_list));

  // Apply config change - should fail due to duplicates
  ASSERT_EQ(OB_SUCCESS, member_info.apply_config_change(args, is_already_finished));
  ASSERT_FALSE(is_already_finished);
  args.reset();
  ObMemberList member_list2;
  ObMember member3 = create_member("127.0.0.1", 1001, 2);
  ObMember member4 = create_member("127.0.0.1", 1002, 2); // Same server as member1

  ASSERT_EQ(OB_SUCCESS, member_list2.add_member(member3));
  ASSERT_EQ(OB_SUCCESS, member_list2.add_member(member4));

  ASSERT_EQ(OB_SUCCESS, args.append_full_member_list_op(MEMBER_CHANGE_OP_SET, member_list2));
  ASSERT_EQ(OB_SUCCESS, member_info.apply_config_change(args, is_already_finished));
  // set member will ignore and reset migrate flag, thus the second set op is set already finished and op success
  ASSERT_FALSE(is_already_finished);
  args.reset();
  ObMemberList member_list3;
  ObMember member5 = create_member("127.0.0.1", 1001, 2, 2);
  ObMember member6 = create_member("127.0.0.1", 1002, 2, 2); // Same server as member1

  ASSERT_EQ(OB_SUCCESS, member_list3.add_member(member5));
  ASSERT_EQ(OB_SUCCESS, member_list3.add_member(member6));

  ASSERT_EQ(OB_SUCCESS, args.append_full_member_list_op(MEMBER_CHANGE_OP_SET, member_list3));
  ASSERT_EQ(OB_SUCCESS, member_info.apply_config_change(args, is_already_finished));
  // set member will ignore and reset migrate flag, thus the second set op is set already finished and op success
  ASSERT_TRUE(is_already_finished);
}

TEST(TestLibPalfConfigInfo, test_apply_config_change_set_learner_with_duplicates)
{
  PALF_LOG(INFO, "test_apply_config_change_set_learner_with_duplicates");
  LibPalfMemberInfo member_info;
  MemberChangeArgs args;
  bool is_already_finished = false;


  ObMember learner1 = create_member("127.0.0.1", 1001, 1, 2);
  ObMember learner2 = create_member("127.0.0.1", 1002, 1, 2);
  ASSERT_EQ(OB_SUCCESS, member_info.learner_list_.add_learner(learner1));
  ASSERT_EQ(OB_SUCCESS, member_info.learner_list_.add_learner(learner2));
  // Create learner list with duplicates
  GlobalLearnerList learner_list;
  ObMember learner3 = create_member("127.0.0.1", 1001, 1);
  ObMember learner4 = create_member("127.0.0.1", 1002, 1); // Same server as learner1
  ASSERT_EQ(OB_SUCCESS, learner_list.add_learner(learner3));
  ASSERT_EQ(OB_SUCCESS, learner_list.add_learner(learner4));

  // Set learner list with duplicates
  ASSERT_EQ(OB_SUCCESS, args.append_learner_list_op(MEMBER_CHANGE_OP_SET, learner_list));

  // Apply config change - should success for set operation
  ASSERT_EQ(OB_SUCCESS, member_info.apply_config_change(args, is_already_finished));
  ASSERT_FALSE(is_already_finished);
  args.reset();

  ASSERT_EQ(OB_SUCCESS, args.append_learner_list_op(MEMBER_CHANGE_OP_ADD, learner1));
  ASSERT_EQ(OB_INVALID_ARGUMENT, member_info.apply_config_change(args, is_already_finished));
}

TEST(TestLibPalfConfigInfo, test_apply_config_change_mixed_operations_with_duplicates)
{
  LibPalfMemberInfo member_info;
  MemberChangeArgs args;
  bool is_already_finished = false;

  // Initialize with existing members
  ObMember existing_member1 = create_member("127.0.0.1", 1001, 1);
  ObMember existing_learner1 = create_member("127.0.0.2", 1001, 1);

  ASSERT_EQ(OB_SUCCESS, member_info.full_memberlist_.add_member(existing_member1));
  ASSERT_EQ(OB_SUCCESS, member_info.learner_list_.add_learner(existing_learner1));

  // Create members with same server for different operations
  ObMember new_member1 = create_member("127.0.0.2", 1001, 2); // Same server as existing_learner1
  ObMember new_learner1 = create_member("127.0.0.1", 1001, 2); // Same server as existing_member1

  // Add member to full_memberlist and learner to learner_list
  ASSERT_EQ(OB_SUCCESS, args.append_full_member_list_op(MEMBER_CHANGE_OP_ADD, new_member1));
  ASSERT_EQ(OB_SUCCESS, args.append_learner_list_op(MEMBER_CHANGE_OP_ADD, new_learner1));

  // Apply config change - should fail due to cross-list duplicates
  ASSERT_EQ(OB_INVALID_ARGUMENT, member_info.apply_config_change(args, is_already_finished));
}

TEST(TestLibPalfConfigInfo, test_apply_config_change_learner_to_member_conversion)
{
  LibPalfMemberInfo member_info;
  MemberChangeArgs args;
  bool is_already_finished = false;

  // Initialize with existing learner
  ObMember existing_learner = create_member("127.0.0.1", 1001, 1, 2);
  ASSERT_EQ(OB_SUCCESS, member_info.learner_list_.add_learner(existing_learner));

  // Try to add the same server as a member (learner to member conversion)
  ObMember new_member = create_member("127.0.0.1", 1001, 1); // Same server as existing_learner

  // Add member to full_memberlist
  ASSERT_EQ(OB_SUCCESS, args.append_full_member_list_op(MEMBER_CHANGE_OP_ADD, new_member));
  ASSERT_EQ(OB_SUCCESS, args.append_learner_list_op(MEMBER_CHANGE_OP_REMOVE, existing_learner));

  // Apply config change - should fail due to cross-list duplicates
  ASSERT_EQ(OB_SUCCESS, member_info.apply_config_change(args, is_already_finished));

  // Verify no changes were made due to failure (still 1 learner, 0 members)
  ASSERT_EQ(1, member_info.full_memberlist_.get_member_number());
  ASSERT_EQ(0, member_info.learner_list_.get_member_number());
  args.reset();
  ASSERT_EQ(OB_SUCCESS, args.append_learner_list_op(MEMBER_CHANGE_OP_ADD, existing_learner));
  ASSERT_EQ(OB_INVALID_ARGUMENT, member_info.apply_config_change(args, is_already_finished));
}

}
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_libpalf_config_info.log", true);
  OB_LOGGER.set_log_level("INFO");
  PALF_LOG(INFO, "begin unittest::test_libpalf_config_info");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}