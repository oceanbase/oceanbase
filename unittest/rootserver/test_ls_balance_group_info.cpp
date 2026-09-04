/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 */

#define USING_LOG_PREFIX RS

#include <gtest/gtest.h>
#include "lib/allocator/page_arena.h"
#include "share/schema/ob_table_schema.h"

#define private public
#include "rootserver/balance/ob_ls_balance_group_info.h"
#undef private

namespace oceanbase
{

bool share::schema::ObSimpleTableSchemaV2::is_valid() const { return true; }

namespace rootserver
{
using namespace common;
using namespace share;

namespace
{
static const int64_t BALANCED_LS_NUM = 3;

bool contains_table(const ObTransferPartList &part_list, const uint64_t table_id)
{
  bool found = false;
  for (int64_t i = 0; !found && i < part_list.count(); ++i) {
    found = (table_id == part_list.at(i).table_id());
  }
  return found;
}
} // namespace

class TestLSBalanceGroupInfo : public ::testing::Test
{
public:
  TestLSBalanceGroupInfo()
      : allocator_(),
        src_ls_bg_info_(allocator_),
        dst_ls_bg_info_(allocator_)
  {
  }

  void SetUp() override
  {
    allocator_.set_tenant_id(OB_SERVER_TENANT_ID);
    ASSERT_EQ(OB_SUCCESS, src_ls_bg_info_.init(ObLSID(1), BALANCED_LS_NUM));
    ASSERT_EQ(OB_SUCCESS, dst_ls_bg_info_.init(ObLSID(2), BALANCED_LS_NUM));
  }

  int append_part_groups(
      ObLSBalanceGroupInfo &ls_bg_info,
      const int64_t *data_sizes,
      const int64_t part_group_count,
      const uint64_t first_table_id)
  {
    int ret = OB_SUCCESS;
    schema::ObSimpleTableSchemaV2 table_schema;
    table_schema.set_database_id(1);
    for (int64_t i = 0; OB_SUCC(ret) && i < part_group_count; ++i) {
      const uint64_t table_id = first_table_id + i;
      ObTransferPartInfo part(table_id, table_id);
      if (OB_FAIL(ls_bg_info.append_part_into_balance_group(
          ObBalanceGroupID::NON_PART_BG_ID,
          table_schema,
          part,
          data_sizes[i],
          table_id,
          0 /* balance_weight */))) {
        LOG_WARN("append part group failed", KR(ret), K(table_id), K(data_sizes[i]));
      }
    }
    return ret;
  }

  ObBalanceGroupInfo *get_bg_info(ObLSBalanceGroupInfo &ls_bg_info)
  {
    ObBalanceGroupInfo *bg_info = nullptr;
    EXPECT_EQ(OB_SUCCESS, ls_bg_info.bg_map_.get_refactored(
        ObBalanceGroupID::NON_PART_BG_ID, bg_info));
    return bg_info;
  }

protected:
  ObArenaAllocator allocator_;
  ObLSBalanceGroupInfo src_ls_bg_info_;
  ObLSBalanceGroupInfo dst_ls_bg_info_;
};

TEST_F(TestLSBalanceGroupInfo, keep_historical_selection_at_threshold)
{
  const int64_t data_sizes[] = {1, 14, 14, 10};
  ASSERT_EQ(OB_SUCCESS, append_part_groups(
      src_ls_bg_info_, data_sizes, ARRAYSIZEOF(data_sizes), 1001));

  ObTransferPartList part_list;
  ASSERT_EQ(OB_SUCCESS, src_ls_bg_info_.transfer_out_by_factor(
      dst_ls_bg_info_, 0.25f, part_list));

  ObBalanceGroupInfo *src_bg_info = get_bg_info(src_ls_bg_info_);
  ObBalanceGroupInfo *dst_bg_info = get_bg_info(dst_ls_bg_info_);
  ASSERT_NE(nullptr, src_bg_info);
  ASSERT_NE(nullptr, dst_bg_info);
  ASSERT_EQ(1, part_list.count());
  EXPECT_TRUE(contains_table(part_list, 1004));
  EXPECT_FALSE(contains_table(part_list, 1001));
  EXPECT_EQ(3, src_bg_info->get_part_groups_count());
  EXPECT_EQ(29, src_bg_info->get_part_groups_data_size());
  EXPECT_EQ(1, dst_bg_info->get_part_groups_count());
  EXPECT_EQ(10, dst_bg_info->get_part_groups_data_size());
}

TEST_F(TestLSBalanceGroupInfo, calculate_threshold_without_float_rounding)
{
  const int64_t data_sizes[] = {1, 2, 3, 3, 4, 4, 4};
  ASSERT_EQ(OB_SUCCESS, append_part_groups(
      src_ls_bg_info_, data_sizes, ARRAYSIZEOF(data_sizes), 1001));

  ObTransferPartList part_list;
  ASSERT_EQ(OB_SUCCESS, src_ls_bg_info_.transfer_out_by_factor(
      dst_ls_bg_info_, 0.1f, part_list));

  ObBalanceGroupInfo *src_bg_info = get_bg_info(src_ls_bg_info_);
  ObBalanceGroupInfo *dst_bg_info = get_bg_info(dst_ls_bg_info_);
  ASSERT_NE(nullptr, src_bg_info);
  ASSERT_NE(nullptr, dst_bg_info);
  ASSERT_EQ(1, part_list.count());
  EXPECT_TRUE(contains_table(part_list, 1001));
  EXPECT_FALSE(contains_table(part_list, 1007));
  EXPECT_EQ(6, src_bg_info->get_part_groups_count());
  EXPECT_EQ(20, src_bg_info->get_part_groups_data_size());
  EXPECT_EQ(1, dst_bg_info->get_part_groups_count());
  EXPECT_EQ(1, dst_bg_info->get_part_groups_data_size());
}

TEST_F(TestLSBalanceGroupInfo, keep_historical_selection_when_all_sizes_are_zero)
{
  const int64_t data_sizes[] = {0, 0, 0, 0};
  ASSERT_EQ(OB_SUCCESS, append_part_groups(
      src_ls_bg_info_, data_sizes, ARRAYSIZEOF(data_sizes), 1001));

  ObTransferPartList part_list;
  ASSERT_EQ(OB_SUCCESS, src_ls_bg_info_.transfer_out_by_factor(
      dst_ls_bg_info_, 0.25f, part_list));

  ObBalanceGroupInfo *src_bg_info = get_bg_info(src_ls_bg_info_);
  ObBalanceGroupInfo *dst_bg_info = get_bg_info(dst_ls_bg_info_);
  ASSERT_NE(nullptr, src_bg_info);
  ASSERT_NE(nullptr, dst_bg_info);
  ASSERT_EQ(1, part_list.count());
  EXPECT_TRUE(contains_table(part_list, 1004));
  EXPECT_FALSE(contains_table(part_list, 1001));
  EXPECT_EQ(3, src_bg_info->get_part_groups_count());
  EXPECT_EQ(1, dst_bg_info->get_part_groups_count());
  EXPECT_EQ(0, src_bg_info->get_part_groups_data_size());
  EXPECT_EQ(0, dst_bg_info->get_part_groups_data_size());
}

TEST_F(TestLSBalanceGroupInfo, replace_only_oversized_selection_with_nonempty_dest)
{
  const int64_t historical_dst_size[] = {1000};
  const int64_t src_data_sizes[] = {1, 20, 30, 100};
  ASSERT_EQ(OB_SUCCESS, append_part_groups(
      dst_ls_bg_info_, historical_dst_size, ARRAYSIZEOF(historical_dst_size), 9001));
  ASSERT_EQ(OB_SUCCESS, append_part_groups(
      src_ls_bg_info_, src_data_sizes, ARRAYSIZEOF(src_data_sizes), 1001));

  ObTransferPartList part_list;
  ASSERT_EQ(OB_SUCCESS, src_ls_bg_info_.transfer_out_by_factor(
      dst_ls_bg_info_, 0.25f, part_list));

  ObBalanceGroupInfo *src_bg_info = get_bg_info(src_ls_bg_info_);
  ObBalanceGroupInfo *dst_bg_info = get_bg_info(dst_ls_bg_info_);
  ASSERT_NE(nullptr, src_bg_info);
  ASSERT_NE(nullptr, dst_bg_info);
  ASSERT_EQ(1, part_list.count());
  EXPECT_TRUE(contains_table(part_list, 1001));
  EXPECT_FALSE(contains_table(part_list, 1004));
  EXPECT_FALSE(contains_table(part_list, 9001));
  EXPECT_EQ(3, src_bg_info->get_part_groups_count());
  EXPECT_EQ(150, src_bg_info->get_part_groups_data_size());
  EXPECT_EQ(2, dst_bg_info->get_part_groups_count());
  EXPECT_EQ(1001, dst_bg_info->get_part_groups_data_size());
  EXPECT_EQ(1151, src_bg_info->get_part_groups_data_size()
      + dst_bg_info->get_part_groups_data_size());
}

TEST_F(TestLSBalanceGroupInfo, repeated_transfer_returns_best_effort_selection)
{
  const int64_t data_sizes[] = {10, 20, 30, 40, 50, 60, 70, 80, 90};
  ASSERT_EQ(OB_SUCCESS, append_part_groups(
      src_ls_bg_info_, data_sizes, ARRAYSIZEOF(data_sizes), 1001));

  ObTransferPartList first_round;
  ASSERT_EQ(OB_SUCCESS, src_ls_bg_info_.transfer_out_by_factor(
      dst_ls_bg_info_, 0.3f, first_round));
  ObBalanceGroupInfo *src_bg_info = get_bg_info(src_ls_bg_info_);
  ObBalanceGroupInfo *dst_bg_info = get_bg_info(dst_ls_bg_info_);
  ASSERT_NE(nullptr, src_bg_info);
  ASSERT_NE(nullptr, dst_bg_info);
  ASSERT_EQ(3, first_round.count());
  EXPECT_TRUE(contains_table(first_round, 1001));
  EXPECT_TRUE(contains_table(first_round, 1002));
  EXPECT_TRUE(contains_table(first_round, 1007));
  EXPECT_EQ(6, src_bg_info->get_part_groups_count());
  EXPECT_EQ(350, src_bg_info->get_part_groups_data_size());
  EXPECT_EQ(3, dst_bg_info->get_part_groups_count());
  EXPECT_EQ(100, dst_bg_info->get_part_groups_data_size());

  ObTransferPartList second_round;
  ASSERT_EQ(OB_SUCCESS, src_ls_bg_info_.transfer_out_by_factor(
      dst_ls_bg_info_, 0.3f, second_round));
  ASSERT_EQ(3, second_round.count());
  EXPECT_TRUE(contains_table(second_round, 1003));
  EXPECT_TRUE(contains_table(second_round, 1004));
  EXPECT_TRUE(contains_table(second_round, 1005));
  EXPECT_EQ(3, src_bg_info->get_part_groups_count());
  EXPECT_EQ(230, src_bg_info->get_part_groups_data_size());
  EXPECT_EQ(6, dst_bg_info->get_part_groups_count());
  // The second-round threshold is ceil(350 * 3 / 9) = 117, but the
  // smallest feasible three PGs total 120. The algorithm is best effort.
  EXPECT_EQ(220, dst_bg_info->get_part_groups_data_size());

  ObTransferPartList third_round;
  ASSERT_EQ(OB_SUCCESS, src_ls_bg_info_.transfer_out_by_factor(
      dst_ls_bg_info_, 0.3f, third_round));
  EXPECT_TRUE(third_round.empty());
  EXPECT_EQ(3, src_bg_info->get_part_groups_count());
  EXPECT_EQ(230, src_bg_info->get_part_groups_data_size());
  EXPECT_EQ(6, dst_bg_info->get_part_groups_count());
  EXPECT_EQ(220, dst_bg_info->get_part_groups_data_size());
  EXPECT_EQ(450, src_bg_info->get_part_groups_data_size()
      + dst_bg_info->get_part_groups_data_size());
}

TEST_F(TestLSBalanceGroupInfo, invalid_and_empty_inputs_do_not_mutate_state)
{
  ObLSBalanceGroupInfo uninitialized_src(allocator_);
  ObLSBalanceGroupInfo uninitialized_dst(allocator_);
  ObTransferPartList part_list;
  EXPECT_EQ(OB_NOT_INIT, uninitialized_src.transfer_out_by_factor(
      dst_ls_bg_info_, 0.5f, part_list));
  EXPECT_EQ(OB_INVALID_ARGUMENT, src_ls_bg_info_.transfer_out_by_factor(
      uninitialized_dst, 0.5f, part_list));
  EXPECT_EQ(OB_INVALID_ARGUMENT, src_ls_bg_info_.transfer_out_by_factor(
      dst_ls_bg_info_, 0.0f, part_list));
  EXPECT_EQ(OB_INVALID_ARGUMENT, src_ls_bg_info_.transfer_out_by_factor(
      dst_ls_bg_info_, -0.1f, part_list));
  EXPECT_TRUE(part_list.empty());
  EXPECT_EQ(0, src_ls_bg_info_.bg_map_.size());
  EXPECT_EQ(0, dst_ls_bg_info_.bg_map_.size());

  EXPECT_EQ(OB_SUCCESS, src_ls_bg_info_.transfer_out_by_factor(
      dst_ls_bg_info_, 0.5f, part_list));
  EXPECT_TRUE(part_list.empty());
  EXPECT_EQ(0, src_ls_bg_info_.bg_map_.size());
  EXPECT_EQ(0, dst_ls_bg_info_.bg_map_.size());

  ObArray<ObPartGroupInfo *> selected_part_groups;
  EXPECT_EQ(OB_INVALID_ARGUMENT,
      src_ls_bg_info_.try_reduce_selected_data_size_by_swap_(
          0, nullptr, nullptr, selected_part_groups));
  ObPartGroupInfo *largest_part_group = nullptr;
  int64_t largest_index = OB_INVALID_INDEX;
  EXPECT_EQ(OB_INVALID_ARGUMENT,
      src_ls_bg_info_.get_largest_part_group_from_array_(
          selected_part_groups, largest_part_group, largest_index));
  EXPECT_EQ(nullptr, largest_part_group);
  EXPECT_EQ(OB_INVALID_INDEX, largest_index);

  const int64_t data_sizes[] = {10, 20};
  ASSERT_EQ(OB_SUCCESS, append_part_groups(
      src_ls_bg_info_, data_sizes, ARRAYSIZEOF(data_sizes), 1001));
  ObBalanceGroupInfo *src_bg_info = get_bg_info(src_ls_bg_info_);
  ASSERT_NE(nullptr, src_bg_info);
  ASSERT_EQ(OB_SUCCESS, src_bg_info->get_largest_part_group(largest_part_group));
  ASSERT_NE(nullptr, largest_part_group);
  EXPECT_EQ(OB_INVALID_ARGUMENT,
      src_bg_info->swap_for_smallest_pg(largest_part_group, *src_bg_info));
  EXPECT_EQ(2, src_bg_info->get_part_groups_count());
  EXPECT_EQ(30, src_bg_info->get_part_groups_data_size());

  // Initialize the temporary objects before destruction to avoid expected
  // OB_NOT_INIT diagnostics from destroying never-created hash maps.
  ASSERT_EQ(OB_SUCCESS, uninitialized_src.init(ObLSID(3), BALANCED_LS_NUM));
  ASSERT_EQ(OB_SUCCESS, uninitialized_dst.init(ObLSID(4), BALANCED_LS_NUM));
}

} // namespace rootserver
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
