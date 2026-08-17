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

#define USING_LOG_PREFIX RS

#include <gtest/gtest.h>
#include "rootserver/balance/ob_balance_group_info.h"
#include "rootserver/balance/ob_ls_balance_group_info.h"
#define private public
#include "rootserver/ob_ls_balance_helper.h"
#undef private
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
namespace rootserver
{
using namespace common;
using namespace share;

namespace
{
int append_part(ObBalanceGroupInfo &bg_info,
    const uint64_t table_id,
    const uint64_t part_object_id,
    const int64_t data_size,
    const uint64_t part_group_uid)
{
  ObTransferPartInfo part(table_id, part_object_id);
  return bg_info.append_part(part, data_size, part_group_uid);
}

bool contains_table(const ObTransferPartList &part_list, const uint64_t table_id)
{
  bool found = false;
  for (int64_t i = 0; !found && i < part_list.count(); ++i) {
    found = (table_id == part_list.at(i).table_id());
  }
  return found;
}

int64_t sum_linear_data_size(const ObTransferPartList &part_list)
{
  int64_t data_size = 0;
  for (int64_t i = 0; i < part_list.count(); ++i) {
    data_size += 10 * (part_list.at(i).table_id() - 1000);
  }
  return data_size;
}

int append_redundant_normal_ls(
    ObLSBalanceTaskHelper &helper,
    const int64_t redundant_ls_count)
{
  int ret = OB_SUCCESS;
  ObUnitGroupBalanceInfo balance_info(
      ObSimpleUnitGroup(1, ObUnit::UNIT_STATUS_DELETING), 1);
  for (int64_t i = 0; OB_SUCC(ret) && i < redundant_ls_count; ++i) {
    ObLSStatusInfo ls_status;
    if (OB_FAIL(ls_status.init(
        OB_SYS_TENANT_ID,
        ObLSID(100 + i),
        1,
        OB_LS_NORMAL,
        1,
        ObZone(),
        ObLSFlag()))) {
      LOG_WARN("failed to init LS status", KR(ret), K(i));
    } else if (OB_FAIL(balance_info.add_ls_status_info(ls_status))) {
      LOG_WARN("failed to add LS status", KR(ret), K(ls_status));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(helper.unit_group_balance_array_.push_back(balance_info))) {
    LOG_WARN("failed to add unit group balance info", KR(ret), K(balance_info));
  }
  return ret;
}

int append_redundant_dup_ls(
    ObLSBalanceTaskHelper &helper,
    const int64_t redundant_dup_ls_count)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < redundant_dup_ls_count; ++i) {
    ObLSFlag flag;
    ObLSStatusInfo ls_status;
    flag.set_duplicate();
    if (OB_FAIL(ls_status.init(
        OB_SYS_TENANT_ID,
        ObLSID(1000 + i),
        1,
        OB_LS_NORMAL,
        1,
        ObZone(),
        flag))) {
      LOG_WARN("failed to init duplicate LS status", KR(ret), K(i));
    } else if (OB_FAIL(helper.dup_ls_stat_array_.push_back(ls_status))) {
      LOG_WARN("failed to add duplicate LS status", KR(ret), K(ls_status));
    }
  }
  return ret;
}

int check_need_load_tablet_size(
    const char *balance_strategy,
    const int64_t redundant_ls_count,
    const int64_t redundant_dup_ls_count,
    bool &need_load_tablet_size)
{
  int ret = OB_SUCCESS;
  ObLSBalanceTaskHelper helper;
  need_load_tablet_size = false;
  if (OB_FAIL(helper.tenant_ls_bg_info_.init(OB_SYS_TENANT_ID))) {
    LOG_WARN("failed to init tenant LS balance group info", KR(ret));
  } else if (OB_FAIL(helper.job_.init(
      OB_SYS_TENANT_ID,
      ObBalanceJobID(1),
      ObBalanceJobType(ObBalanceJobType::BALANCE_JOB_LS),
      ObBalanceJobStatus(ObBalanceJobStatus::BALANCE_JOB_STATUS_DOING),
      1,
      2,
      ObString(),
      ObString(balance_strategy)))) {
    LOG_WARN("failed to init balance job", KR(ret), KCSTRING(balance_strategy));
  } else if (OB_FAIL(append_redundant_normal_ls(helper, redundant_ls_count))) {
    LOG_WARN("failed to append redundant normal LS", KR(ret), K(redundant_ls_count));
  } else if (OB_FAIL(append_redundant_dup_ls(helper, redundant_dup_ls_count))) {
    LOG_WARN("failed to append redundant duplicate LS", KR(ret), K(redundant_dup_ls_count));
  } else {
    need_load_tablet_size = helper.need_load_tablet_size_();
  }
  return ret;
}
} // namespace

TEST(TestLSBalanceGroupInfo, select_by_data_size_without_changing_part_group_count)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObBalanceGroupInfo bg_info(ObBalanceGroupID(1, 1), allocator);

  // Eight PGs. PG 1 contains two related parts and its aggregate size is 10.
  ASSERT_EQ(OB_SUCCESS, append_part(bg_info, 1001, 1001, 4, 1));
  ASSERT_EQ(OB_SUCCESS, append_part(bg_info, 1002, 1002, 6, 1));
  ASSERT_EQ(OB_SUCCESS, append_part(bg_info, 2001, 2001, 20, 2));
  ASSERT_EQ(OB_SUCCESS, append_part(bg_info, 3001, 3001, 30, 3));
  ASSERT_EQ(OB_SUCCESS, append_part(bg_info, 4001, 4001, 40, 4));
  ASSERT_EQ(OB_SUCCESS, append_part(bg_info, 5001, 5001, 50, 5));
  ASSERT_EQ(OB_SUCCESS, append_part(bg_info, 6001, 6001, 60, 6));
  ASSERT_EQ(OB_SUCCESS, append_part(bg_info, 7001, 7001, 1000, 7));
  ASSERT_EQ(OB_SUCCESS, append_part(bg_info, 8001, 8001, 1000, 8));

  const int64_t original_part_group_count = bg_info.get_part_group_count();
  const int64_t original_data_size = bg_info.get_data_size();
  ASSERT_EQ(8, original_part_group_count);
  ASSERT_EQ(2210, original_data_size);

  ObTransferPartList part_list;
  int64_t popped_part_count = 0;
  int64_t popped_data_size = 0;
  ASSERT_EQ(OB_SUCCESS,
      bg_info.pop_back(2, 553, part_list, popped_part_count, popped_data_size));

  EXPECT_EQ(2, original_part_group_count - bg_info.get_part_group_count());
  // Two PGs contain three parts because PG 1 has two related parts.
  EXPECT_EQ(3, popped_part_count);
  EXPECT_EQ(3, part_list.count());
  // Historical suffix selection would choose 2000; optimized selection chooses 10 + 20.
  EXPECT_EQ(30, popped_data_size);
  EXPECT_LE(popped_data_size, 553);
  EXPECT_EQ(original_data_size - popped_data_size, bg_info.get_data_size());
  EXPECT_TRUE(contains_table(part_list, 1001));
  EXPECT_TRUE(contains_table(part_list, 1002));
  EXPECT_TRUE(contains_table(part_list, 2001));
}

TEST(TestLSBalanceGroupInfo, split_ls_repeated_transfer_avoids_oversized_part_groups)
{
  ObLSBalanceGroupInfo ls_bg_info;
  const ObBalanceGroupID bg_id(2, 1);
  ASSERT_EQ(OB_SUCCESS, ls_bg_info.init(ObLSID(1)));

  for (int64_t i = 0; i < 9; ++i) {
    ObTransferPartInfo part(1001 + i, 1001 + i);
    ASSERT_EQ(OB_SUCCESS,
        ls_bg_info.append_part_into_balance_group(bg_id, part, 10 * (i + 1), i + 1));
  }

  ObTransferPartList first_round;
  ASSERT_EQ(OB_SUCCESS, ls_bg_info.transfer_out_by_factor(0.3f, first_round));
  EXPECT_EQ(3, first_round.count());
  EXPECT_EQ(100, sum_linear_data_size(first_round));
  EXPECT_LE(sum_linear_data_size(first_round), 150);
  EXPECT_FALSE(contains_table(first_round, 1009));

  ObTransferPartList second_round;
  ASSERT_EQ(OB_SUCCESS, ls_bg_info.transfer_out_by_factor(0.3f, second_round));
  EXPECT_EQ(3, second_round.count());
  // The original split ratio gives a threshold of ceil((450 - 100) * 3 / 9)
  // = 117. It cannot be reached with the remaining PGs, so choose the smallest
  // possible three PGs (30 + 40 + 50) instead of moving an oversized suffix.
  EXPECT_EQ(120, sum_linear_data_size(second_round));
  EXPECT_TRUE(contains_table(second_round, 1003));
  EXPECT_TRUE(contains_table(second_round, 1004));
  EXPECT_TRUE(contains_table(second_round, 1005));
  EXPECT_FALSE(contains_table(second_round, 1009));

  // The original PG count remains nine, so the third round must leave three
  // PGs in place and transfer nothing.
  ObTransferPartList third_round;
  ASSERT_EQ(OB_SUCCESS, ls_bg_info.transfer_out_by_factor(0.3f, third_round));
  EXPECT_EQ(0, third_round.count());
}

TEST(TestLSBalanceGroupInfo, keep_historical_selection_when_size_is_balanced)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObBalanceGroupInfo bg_info(ObBalanceGroupID(3, 1), allocator);
  const int64_t data_sizes[] = {1, 2, 9, 10};

  for (int64_t i = 0; i < 4; ++i) {
    ASSERT_EQ(OB_SUCCESS,
        append_part(bg_info, 1001 + i, 1001 + i, data_sizes[i], i + 1));
  }

  ObTransferPartList part_list;
  int64_t popped_part_count = 0;
  int64_t popped_data_size = 0;
  ASSERT_EQ(OB_SUCCESS,
      bg_info.pop_back(2, 19, part_list, popped_part_count, popped_data_size));
  EXPECT_EQ(2, popped_part_count);
  EXPECT_EQ(19, popped_data_size);
  EXPECT_TRUE(contains_table(part_list, 1003));
  EXPECT_TRUE(contains_table(part_list, 1004));
  EXPECT_FALSE(contains_table(part_list, 1001));
  EXPECT_FALSE(contains_table(part_list, 1002));
}

TEST(TestLSBalanceTaskHelper, load_tablet_size_only_for_partial_ls_split)
{
  bool need_load_tablet_size = false;

  // Expansion always splits source LSs to create new destination LSs.
  ASSERT_EQ(OB_SUCCESS, check_need_load_tablet_size(
      LS_BALANCE_BY_EXPAND, 0, 0, need_load_tablet_size));
  EXPECT_TRUE(need_load_tablet_size);

  // Non-integer R/T produces a partial normal-LS split, with or without duplicate LSs.
  ASSERT_EQ(OB_SUCCESS, check_need_load_tablet_size(
      LS_BALANCE_BY_SHRINK, 1, 0, need_load_tablet_size));
  EXPECT_TRUE(need_load_tablet_size);
  ASSERT_EQ(OB_SUCCESS, check_need_load_tablet_size(
      LS_BALANCE_BY_SHRINK, 3, 2, need_load_tablet_size));
  EXPECT_TRUE(need_load_tablet_size);

  // Duplicate-only shrink and integer R/T shrink only merge whole LSs.
  ASSERT_EQ(OB_SUCCESS, check_need_load_tablet_size(
      LS_BALANCE_BY_SHRINK, 0, 2, need_load_tablet_size));
  EXPECT_FALSE(need_load_tablet_size);
  ASSERT_EQ(OB_SUCCESS, check_need_load_tablet_size(
      LS_BALANCE_BY_SHRINK, 2, 0, need_load_tablet_size));
  EXPECT_FALSE(need_load_tablet_size);
  ASSERT_EQ(OB_SUCCESS, check_need_load_tablet_size(
      LS_BALANCE_BY_SHRINK, 4, 2, need_load_tablet_size));
  EXPECT_FALSE(need_load_tablet_size);

  // ALTER and MIGRATE do not select partitions for an LS split.
  ASSERT_EQ(OB_SUCCESS, check_need_load_tablet_size(
      LS_BALANCE_BY_ALTER, 3, 0, need_load_tablet_size));
  EXPECT_FALSE(need_load_tablet_size);
  ASSERT_EQ(OB_SUCCESS, check_need_load_tablet_size(
      LS_BALANCE_BY_MIGRATE, 3, 0, need_load_tablet_size));
  EXPECT_FALSE(need_load_tablet_size);
}

} // namespace rootserver
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  oceanbase::share::ObTenantBase tenant_base(oceanbase::OB_SYS_TENANT_ID);
  int ret = tenant_base.init();
  if (oceanbase::OB_SUCCESS == ret) {
    oceanbase::share::ObTenantEnv::set_tenant(&tenant_base);
  }
  testing::InitGoogleTest(&argc, argv);
  const int test_ret = oceanbase::OB_SUCCESS == ret ? RUN_ALL_TESTS() : 1;
  oceanbase::share::ObTenantEnv::set_tenant(nullptr);
  tenant_base.destroy();
  return test_ret;
}
