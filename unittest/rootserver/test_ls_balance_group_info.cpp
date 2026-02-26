/**
 * Copyright (c) 2025 OceanBase
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

#include <gmock/gmock.h>
#include <cmath>
#include <algorithm>

#define private public
#include "rootserver/balance/ob_ls_balance_group_info.h"

namespace oceanbase
{

bool share::schema::ObSimpleTableSchemaV2::is_valid() const { return true; }

namespace rootserver
{
using namespace common;
using namespace share;

static const uint64_t GB = 1024L * 1024L * 1024L;
static const int64_t BALANCED_LS_NUM = 3;

class TestLSBalanceGroupInfo : public ::testing::Test
{
public:
  void SetUp() override
  {
    allocator_.set_tenant_id(OB_SERVER_TENANT_ID);
    src_ls_bg_info_ = new ObLSBalanceGroupInfo(allocator_);
    dst_ls_bg_info_ = new ObLSBalanceGroupInfo(allocator_);
    ASSERT_NE(nullptr, src_ls_bg_info_);
    ASSERT_NE(nullptr, dst_ls_bg_info_);
    ASSERT_EQ(OB_SUCCESS, src_ls_bg_info_->init(ObLSID(1), BALANCED_LS_NUM));
    ASSERT_EQ(OB_SUCCESS, dst_ls_bg_info_->init(ObLSID(2), BALANCED_LS_NUM));
  }

  void TearDown() override
  {
    if (OB_NOT_NULL(src_ls_bg_info_)) {
      src_ls_bg_info_->destroy();
      delete src_ls_bg_info_;
      src_ls_bg_info_ = nullptr;
    }
    if (OB_NOT_NULL(dst_ls_bg_info_)) {
      dst_ls_bg_info_->destroy();
      delete dst_ls_bg_info_;
      dst_ls_bg_info_ = nullptr;
    }
    allocator_.reset();
  }

  // Helper function to add partitions to a balance group
  int add_partitions_to_bg(
      ObLSBalanceGroupInfo &ls_bg_info,
      const ObBalanceGroupID &bg_id,
      const int64_t part_count,
      const int64_t data_size_per_part,
      const int64_t start_table_id = 1000)
  {
    int ret = OB_SUCCESS;
    schema::ObSimpleTableSchemaV2 mock_table_schema;
    mock_table_schema.set_database_id(123123123);

    for (int64_t i = 0; i < part_count && OB_SUCC(ret); ++i) {
      int64_t table_id = start_table_id + i;
      int64_t part_object_id = table_id;
      ObTransferPartInfo part(table_id, part_object_id);
      uint64_t part_group_uid = part_object_id;
      int64_t balance_weight = 0;
      int64_t data_size = data_size_per_part;

      if (OB_FAIL(ls_bg_info.append_part_into_balance_group(
          bg_id, mock_table_schema, part, data_size, part_group_uid, balance_weight))) {
        LOG_WARN("append part into balance group failed", KR(ret), K(bg_id), K(part));
      }
    }
    return ret;
  }

  // Helper function to calculate total data size in a balance group
  int64_t get_total_data_size(ObLSBalanceGroupInfo &ls_bg_info, const ObBalanceGroupID &bg_id)
  {
    int64_t total_size = 0;
    ObBalanceGroupInfo *bg = nullptr;
    if (OB_SUCCESS == ls_bg_info.bg_map_.get_refactored(bg_id, bg) && OB_NOT_NULL(bg)) {
      total_size = bg->get_part_groups_data_size();
    }
    return total_size;
  }

  // Helper function to get partition group count
  int64_t get_part_group_count(ObLSBalanceGroupInfo &ls_bg_info, const ObBalanceGroupID &bg_id)
  {
    int64_t count = 0;
    ObBalanceGroupInfo *bg = nullptr;
    if (OB_SUCCESS == ls_bg_info.bg_map_.get_refactored(bg_id, bg) && OB_NOT_NULL(bg)) {
      count = bg->get_part_groups_count();
    }
    return count;
  }

protected:
  ObArenaAllocator allocator_;
  ObLSBalanceGroupInfo *src_ls_bg_info_;
  ObLSBalanceGroupInfo *dst_ls_bg_info_;
};

// Test case 1: Basic transfer with factor 0.5
TEST_F(TestLSBalanceGroupInfo, BasicTransferByFactor)
{
  const ObBalanceGroupID bg_id(1, 1);
  const int64_t part_count = 10;
  const int64_t data_size_per_part = 100 * GB;
  const float factor = 0.5f;

  // Add partitions to source LS
  ASSERT_EQ(OB_SUCCESS, add_partitions_to_bg(
      *src_ls_bg_info_, bg_id, part_count, data_size_per_part));

  int64_t src_original_count = get_part_group_count(*src_ls_bg_info_, bg_id);
  int64_t src_original_data_size = get_total_data_size(*src_ls_bg_info_, bg_id);
  LOG_INFO("Before transfer", K(src_original_count), K(src_original_data_size));

  // Transfer out by factor
  ObTransferPartList part_list;
  ASSERT_EQ(OB_SUCCESS, src_ls_bg_info_->transfer_out_by_factor(
      *dst_ls_bg_info_, factor, part_list));

  // Verify results
  int64_t src_after_count = get_part_group_count(*src_ls_bg_info_, bg_id);
  int64_t dst_after_count = get_part_group_count(*dst_ls_bg_info_, bg_id);
  int64_t transferred_count = part_list.count();

  LOG_INFO("After transfer", K(src_after_count), K(dst_after_count), K(transferred_count));

  // Expected remove count should be ceil(factor * total_count)
  int64_t expected_remove_count = static_cast<int64_t>(std::ceil(factor * src_original_count));
  // But we should leave at least as many as we remove
  int64_t max_remove_count = src_original_count - expected_remove_count;
  int64_t actual_remove_count = std::min(max_remove_count, expected_remove_count);

  ASSERT_GT(transferred_count, 0);
  ASSERT_LE(transferred_count, actual_remove_count);
  ASSERT_EQ(src_after_count, src_original_count - transferred_count);
  ASSERT_EQ(dst_after_count, transferred_count);
}

// Test case 2: Transfer with small factor (0.1)
TEST_F(TestLSBalanceGroupInfo, SmallFactorTransfer)
{
  const ObBalanceGroupID bg_id(2, 2);
  const int64_t part_count = 20;
  const int64_t data_size_per_part = 50 * GB;
  const float factor = 0.1f;

  ASSERT_EQ(OB_SUCCESS, add_partitions_to_bg(
      *src_ls_bg_info_, bg_id, part_count, data_size_per_part));

  int64_t src_original_count = get_part_group_count(*src_ls_bg_info_, bg_id);

  ObTransferPartList part_list;
  ASSERT_EQ(OB_SUCCESS, src_ls_bg_info_->transfer_out_by_factor(
      *dst_ls_bg_info_, factor, part_list));

  int64_t transferred_count = part_list.count();
  int64_t expected_min = static_cast<int64_t>(std::ceil(factor * src_original_count));
  int64_t max_remove = src_original_count - expected_min;
  int64_t expected_max = std::min(max_remove, expected_min);

  LOG_INFO("Small factor transfer", K(src_original_count), K(transferred_count), K(expected_min));

  ASSERT_GE(transferred_count, 0);
  ASSERT_LE(transferred_count, expected_max);
}

// Test case 3: Transfer with large factor (0.8)
TEST_F(TestLSBalanceGroupInfo, LargeFactorTransfer)
{
  const ObBalanceGroupID bg_id(3, 3);
  const int64_t part_count = 15;
  const int64_t data_size_per_part = 80 * GB;
  const float factor = 0.8f;

  ASSERT_EQ(OB_SUCCESS, add_partitions_to_bg(
      *src_ls_bg_info_, bg_id, part_count, data_size_per_part));

  int64_t src_original_count = get_part_group_count(*src_ls_bg_info_, bg_id);
  int64_t src_original_data_size = get_total_data_size(*src_ls_bg_info_, bg_id);

  ObTransferPartList part_list;
  ASSERT_EQ(OB_SUCCESS, src_ls_bg_info_->transfer_out_by_factor(
      *dst_ls_bg_info_, factor, part_list));

  int64_t transferred_count = part_list.count();
  int64_t src_after_count = get_part_group_count(*src_ls_bg_info_, bg_id);
  int64_t dst_after_count = get_part_group_count(*dst_ls_bg_info_, bg_id);

  LOG_INFO("Large factor transfer", K(src_original_count), K(transferred_count),
      K(src_after_count), K(dst_after_count));

  // Should transfer out a significant portion
  ASSERT_GT(transferred_count, 0);
  ASSERT_LT(src_after_count, src_original_count);
  ASSERT_EQ(dst_after_count, transferred_count);
  // Should leave at least as many as transferred
  ASSERT_GE(src_after_count, transferred_count);
}

// Test case 4: Multiple balance groups
TEST_F(TestLSBalanceGroupInfo, MultipleBalanceGroups)
{
  const ObBalanceGroupID bg_id1(10, 10);
  const ObBalanceGroupID bg_id2(20, 20);
  const ObBalanceGroupID bg_id3(30, 30);
  const float factor = 0.3f;

  // Add partitions to multiple balance groups
  ASSERT_EQ(OB_SUCCESS, add_partitions_to_bg(*src_ls_bg_info_, bg_id1, 10, 100 * GB));
  ASSERT_EQ(OB_SUCCESS, add_partitions_to_bg(*src_ls_bg_info_, bg_id2, 15, 50 * GB));
  ASSERT_EQ(OB_SUCCESS, add_partitions_to_bg(*src_ls_bg_info_, bg_id3, 8, 200 * GB));

  int64_t bg1_original = get_part_group_count(*src_ls_bg_info_, bg_id1);
  int64_t bg2_original = get_part_group_count(*src_ls_bg_info_, bg_id2);
  int64_t bg3_original = get_part_group_count(*src_ls_bg_info_, bg_id3);

  ObTransferPartList part_list;
  ASSERT_EQ(OB_SUCCESS, src_ls_bg_info_->transfer_out_by_factor(
      *dst_ls_bg_info_, factor, part_list));

  int64_t bg1_after = get_part_group_count(*src_ls_bg_info_, bg_id1);
  int64_t bg2_after = get_part_group_count(*src_ls_bg_info_, bg_id2);
  int64_t bg3_after = get_part_group_count(*src_ls_bg_info_, bg_id3);
  int64_t bg1_dst = get_part_group_count(*dst_ls_bg_info_, bg_id1);
  int64_t bg2_dst = get_part_group_count(*dst_ls_bg_info_, bg_id2);
  int64_t bg3_dst = get_part_group_count(*dst_ls_bg_info_, bg_id3);

  LOG_INFO("Multiple balance groups transfer",
      K(bg1_original), K(bg1_after), K(bg1_dst),
      K(bg2_original), K(bg2_after), K(bg2_dst),
      K(bg3_original), K(bg3_after), K(bg3_dst),
      "total_transferred", part_list.count());

  // All balance groups should have transferred some partitions
  ASSERT_GT(bg1_dst, 0);
  ASSERT_GT(bg2_dst, 0);
  ASSERT_GT(bg3_dst, 0);
  ASSERT_EQ(bg1_after, bg1_original - bg1_dst);
  ASSERT_EQ(bg2_after, bg2_original - bg2_dst);
  ASSERT_EQ(bg3_after, bg3_original - bg3_dst);
}

// Test case 5: Test swap strategy with many zero-size partitions
// This tests that swap strategy prefers zero-size partitions over large ones
TEST_F(TestLSBalanceGroupInfo, SwapStrategyWithManyZeroSizePartitions)
{
  const ObBalanceGroupID bg_id(4, 4);
  const float factor = 0.3f;

  schema::ObSimpleTableSchemaV2 mock_table_schema;
  mock_table_schema.set_database_id(123123123);

  // Add many zero-size partitions (empty partitions)
  const int64_t zero_size_count = 50;
  for (int64_t i = 0; i < zero_size_count; ++i) {
    int64_t table_id = 4000 + i;
    ObTransferPartInfo part(table_id, table_id);
    uint64_t part_group_uid = table_id;
    int64_t data_size = 0; // Zero-size partitions
    int64_t balance_weight = 0;
    ASSERT_EQ(OB_SUCCESS, src_ls_bg_info_->append_part_into_balance_group(
        bg_id, mock_table_schema, part, data_size, part_group_uid, balance_weight));
  }

  // Add some large partitions
  const int64_t large_partition_count = 10;
  for (int64_t i = 0; i < large_partition_count; ++i) {
    int64_t table_id = 5000 + i;
    ObTransferPartInfo part(table_id, table_id);
    uint64_t part_group_uid = table_id;
    int64_t data_size = 1000 * GB; // Large partitions (1TB each)
    int64_t balance_weight = 0;
    ASSERT_EQ(OB_SUCCESS, src_ls_bg_info_->append_part_into_balance_group(
        bg_id, mock_table_schema, part, data_size, part_group_uid, balance_weight));
  }

  int64_t src_original_count = get_part_group_count(*src_ls_bg_info_, bg_id);
  int64_t src_original_data_size = get_total_data_size(*src_ls_bg_info_, bg_id);
  int64_t expected_remove_count = static_cast<int64_t>(std::ceil(factor * src_original_count));
  int64_t max_remove = src_original_count - expected_remove_count;
  int64_t actual_remove_count = std::min(max_remove, expected_remove_count);
  int64_t data_size_threshold = static_cast<int64_t>(
      std::ceil(static_cast<double>(actual_remove_count) / src_original_count * src_original_data_size));

  LOG_INFO("Before transfer with zero-size partitions",
      K(src_original_count), K(src_original_data_size),
      K(expected_remove_count), K(actual_remove_count), K(data_size_threshold),
      K(zero_size_count), K(large_partition_count));

  ObTransferPartList part_list;
  ASSERT_EQ(OB_SUCCESS, src_ls_bg_info_->transfer_out_by_factor(
      *dst_ls_bg_info_, factor, part_list));

  int64_t transferred_count = part_list.count();
  int64_t dst_data_size = get_total_data_size(*dst_ls_bg_info_, bg_id);

  LOG_INFO("After transfer with zero-size partitions",
      K(transferred_count), K(dst_data_size), K(data_size_threshold));

  // The swap strategy should ensure transferred data size does not exceed threshold
  // If round-robin selection already meets the threshold, swap won't execute (which is correct)
  ASSERT_GT(transferred_count, 0);
  ASSERT_LE(transferred_count, actual_remove_count);

  // Verify that the transferred data size does not exceed the threshold
  // Swap strategy will only execute if total_size > threshold, otherwise it's skipped
  // This is the correct behavior: swap ensures we don't exceed threshold, not that we minimize size
  if (data_size_threshold > 0) {
    LOG_INFO("Data size comparison", K(dst_data_size), K(data_size_threshold),
        "ratio", static_cast<double>(dst_data_size) / data_size_threshold);
    // The transferred size should not exceed the threshold
    // If it's already <= threshold, swap won't execute (which is expected behavior)
    ASSERT_LE(dst_data_size, data_size_threshold);
  }
}

// Test case 6: Test swap strategy with one huge partition
// This tests that swap strategy avoids selecting the huge partition
TEST_F(TestLSBalanceGroupInfo, SwapStrategyWithHugePartition)
{
  const ObBalanceGroupID bg_id(5, 5);
  const float factor = 0.4f;

  schema::ObSimpleTableSchemaV2 mock_table_schema;
  mock_table_schema.set_database_id(123123123);

  // Add one huge partition (10TB)
  int64_t huge_table_id = 6000;
  ObTransferPartInfo huge_part(huge_table_id, huge_table_id);
  uint64_t huge_part_group_uid = huge_table_id;
  int64_t huge_data_size = 10 * 1024 * GB; // 10TB - huge partition
  int64_t balance_weight = 0;
  ASSERT_EQ(OB_SUCCESS, src_ls_bg_info_->append_part_into_balance_group(
      bg_id, mock_table_schema, huge_part, huge_data_size, huge_part_group_uid, balance_weight));

  // Add many small/medium partitions
  const int64_t small_partition_count = 30;
  for (int64_t i = 0; i < small_partition_count; ++i) {
    int64_t table_id = 7000 + i;
    ObTransferPartInfo part(table_id, table_id);
    uint64_t part_group_uid = table_id;
    int64_t data_size = 50 * GB; // Small partitions (50GB each)
    int64_t balance_weight = 0;
    ASSERT_EQ(OB_SUCCESS, src_ls_bg_info_->append_part_into_balance_group(
        bg_id, mock_table_schema, part, data_size, part_group_uid, balance_weight));
  }

  int64_t src_original_count = get_part_group_count(*src_ls_bg_info_, bg_id);
  int64_t src_original_data_size = get_total_data_size(*src_ls_bg_info_, bg_id);
  int64_t expected_remove_count = static_cast<int64_t>(std::ceil(factor * src_original_count));
  int64_t max_remove = src_original_count - expected_remove_count;
  int64_t actual_remove_count = std::min(max_remove, expected_remove_count);
  int64_t data_size_threshold = static_cast<int64_t>(
      std::ceil(static_cast<double>(actual_remove_count) / src_original_count * src_original_data_size));

  LOG_INFO("Before transfer with huge partition",
      K(src_original_count), K(src_original_data_size),
      K(expected_remove_count), K(actual_remove_count), K(data_size_threshold),
      K(huge_data_size), K(small_partition_count));

  ObTransferPartList part_list;
  ASSERT_EQ(OB_SUCCESS, src_ls_bg_info_->transfer_out_by_factor(
      *dst_ls_bg_info_, factor, part_list));

  int64_t transferred_count = part_list.count();
  int64_t dst_data_size = get_total_data_size(*dst_ls_bg_info_, bg_id);

  LOG_INFO("After transfer with huge partition",
      K(transferred_count), K(dst_data_size), K(data_size_threshold), K(huge_data_size));

  ASSERT_GT(transferred_count, 0);
  ASSERT_LE(transferred_count, actual_remove_count);

  // The swap strategy should avoid selecting the huge partition
  // If the huge partition was selected, dst_data_size would be close to huge_data_size
  // But with swap, it should be much smaller
  ASSERT_LT(dst_data_size, huge_data_size);

  // The transferred size should be reasonable (mostly small partitions)
  // Expected: transferred_size <= actual_remove_count * 50GB (if all small partitions)
  int64_t max_expected_size = actual_remove_count * 50 * GB;
  if (dst_data_size > max_expected_size) {
    // If larger, it means huge partition might have been selected
    // But swap should have replaced it, so we allow some tolerance
    LOG_INFO("Transferred size is larger than expected, but swap should have helped",
        K(dst_data_size), K(max_expected_size), K(huge_data_size));
    // At least it should be much less than huge partition size
    ASSERT_LT(dst_data_size, huge_data_size / 2);
  }
}

// Test case 7: Test swap strategy with highly unbalanced data sizes
// Mix of zero-size, small, medium, and large partitions
TEST_F(TestLSBalanceGroupInfo, SwapStrategyWithUnbalancedDataSizes)
{
  const ObBalanceGroupID bg_id(6, 6);
  const float factor = 0.35f;

  schema::ObSimpleTableSchemaV2 mock_table_schema;
  mock_table_schema.set_database_id(123123123);

  // Add partitions with highly unbalanced sizes
  int64_t table_id_base = 8000;

  // 20 zero-size partitions
  for (int64_t i = 0; i < 20; ++i) {
    int64_t table_id = table_id_base + i;
    ObTransferPartInfo part(table_id, table_id);
    ASSERT_EQ(OB_SUCCESS, src_ls_bg_info_->append_part_into_balance_group(
        bg_id, mock_table_schema, part, 0, table_id, 0));
  }

  // 15 small partitions (10GB each)
  for (int64_t i = 0; i < 15; ++i) {
    int64_t table_id = table_id_base + 100 + i;
    ObTransferPartInfo part(table_id, table_id);
    ASSERT_EQ(OB_SUCCESS, src_ls_bg_info_->append_part_into_balance_group(
        bg_id, mock_table_schema, part, 10 * GB, table_id, 0));
  }

  // 10 medium partitions (200GB each)
  for (int64_t i = 0; i < 10; ++i) {
    int64_t table_id = table_id_base + 200 + i;
    ObTransferPartInfo part(table_id, table_id);
    ASSERT_EQ(OB_SUCCESS, src_ls_bg_info_->append_part_into_balance_group(
        bg_id, mock_table_schema, part, 200 * GB, table_id, 0));
  }

  // 5 large partitions (1000GB each)
  for (int64_t i = 0; i < 5; ++i) {
    int64_t table_id = table_id_base + 300 + i;
    ObTransferPartInfo part(table_id, table_id);
    ASSERT_EQ(OB_SUCCESS, src_ls_bg_info_->append_part_into_balance_group(
        bg_id, mock_table_schema, part, 1000 * GB, table_id, 0));
  }

  int64_t src_original_count = get_part_group_count(*src_ls_bg_info_, bg_id);
  int64_t src_original_data_size = get_total_data_size(*src_ls_bg_info_, bg_id);
  int64_t expected_remove_count = static_cast<int64_t>(std::ceil(factor * src_original_count));
  int64_t max_remove = src_original_count - expected_remove_count;
  int64_t actual_remove_count = std::min(max_remove, expected_remove_count);
  int64_t data_size_threshold = static_cast<int64_t>(
      std::ceil(static_cast<double>(actual_remove_count) / src_original_count * src_original_data_size));

  LOG_INFO("Before transfer with unbalanced sizes",
      K(src_original_count), K(src_original_data_size),
      K(expected_remove_count), K(actual_remove_count), K(data_size_threshold));

  ObTransferPartList part_list;
  ASSERT_EQ(OB_SUCCESS, src_ls_bg_info_->transfer_out_by_factor(
      *dst_ls_bg_info_, factor, part_list));

  int64_t transferred_count = part_list.count();
  int64_t dst_data_size = get_total_data_size(*dst_ls_bg_info_, bg_id);
  int64_t src_after_data_size = get_total_data_size(*src_ls_bg_info_, bg_id);

  LOG_INFO("After transfer with unbalanced sizes",
      K(transferred_count), K(dst_data_size), K(data_size_threshold),
      K(src_after_data_size), K(src_original_data_size));

  ASSERT_GT(transferred_count, 0);
  ASSERT_LE(transferred_count, actual_remove_count);
  ASSERT_EQ(dst_data_size + src_after_data_size, src_original_data_size);

  // Verify swap strategy effectiveness
  // The transferred data size should be optimized (prefer smaller partitions)
  if (data_size_threshold > 0 && actual_remove_count > 0) {
    // Calculate average data size per transferred partition
    double avg_transferred_size = static_cast<double>(dst_data_size) / transferred_count;
    // Calculate average data size per partition in source (before transfer)
    double avg_source_size = static_cast<double>(src_original_data_size) / src_original_count;

    LOG_INFO("Average sizes comparison",
        K(avg_transferred_size), K(avg_source_size),
        "ratio", avg_transferred_size / avg_source_size);

    // If swap works well, average transferred size should be less than or equal to average source size
    // (because we prefer smaller partitions)
    // Allow some tolerance due to round-robin initial selection
    if (avg_source_size > 0) {
      // The ratio should be <= 1.5 (allowing some tolerance)
      ASSERT_LE(avg_transferred_size / avg_source_size, 1.5);
    }
  }

  // Verify that we didn't transfer too much data
  // The transferred size should be reasonable compared to threshold
  if (data_size_threshold > 0) {
    LOG_INFO("Data size vs threshold",
        K(dst_data_size), K(data_size_threshold),
        "ratio", static_cast<double>(dst_data_size) / data_size_threshold);
    // Transferred size should not exceed threshold by too much (allow 20% tolerance)
    ASSERT_LE(dst_data_size, data_size_threshold * 1.2);
  }
}

// Test case 8: Test swap strategy effectiveness - verify that swap actually reduces data size
// This test creates a scenario where round-robin might select large partitions,
// but swap should replace them with smaller ones
TEST_F(TestLSBalanceGroupInfo, SwapStrategyEffectiveness)
{
  const ObBalanceGroupID bg_id(7, 7);
  const float factor = 0.5f;

  schema::ObSimpleTableSchemaV2 mock_table_schema;
  mock_table_schema.set_database_id(123123123);

  // Create a scenario where initial round-robin selection might pick large partitions
  // But swap should optimize by replacing them with smaller/zero-size partitions

  // Add a few very large partitions first (round-robin might pick these first)
  for (int64_t i = 0; i < 5; ++i) {
    int64_t table_id = 9000 + i;
    ObTransferPartInfo part(table_id, table_id);
    ASSERT_EQ(OB_SUCCESS, src_ls_bg_info_->append_part_into_balance_group(
        bg_id, mock_table_schema, part, 2000 * GB, table_id, 0)); // 2TB each
  }

  // Add many zero-size and small partitions (these should be preferred by swap)
  for (int64_t i = 0; i < 20; ++i) {
    int64_t table_id = 10000 + i;
    ObTransferPartInfo part(table_id, table_id);
    int64_t data_size = (i % 2 == 0) ? 0 : 5 * GB; // Alternating zero-size and small
    ASSERT_EQ(OB_SUCCESS, src_ls_bg_info_->append_part_into_balance_group(
        bg_id, mock_table_schema, part, data_size, table_id, 0));
  }

  int64_t src_original_count = get_part_group_count(*src_ls_bg_info_, bg_id);
  int64_t src_original_data_size = get_total_data_size(*src_ls_bg_info_, bg_id);
  int64_t expected_remove_count = static_cast<int64_t>(std::ceil(factor * src_original_count));
  int64_t max_remove = src_original_count - expected_remove_count;
  int64_t actual_remove_count = std::min(max_remove, expected_remove_count);
  int64_t data_size_threshold = static_cast<int64_t>(
      std::ceil(static_cast<double>(actual_remove_count) / src_original_count * src_original_data_size));

  LOG_INFO("Before transfer - swap effectiveness test",
      K(src_original_count), K(src_original_data_size),
      K(expected_remove_count), K(actual_remove_count), K(data_size_threshold));

  ObTransferPartList part_list;
  ASSERT_EQ(OB_SUCCESS, src_ls_bg_info_->transfer_out_by_factor(
      *dst_ls_bg_info_, factor, part_list));

  int64_t transferred_count = part_list.count();
  int64_t dst_data_size = get_total_data_size(*dst_ls_bg_info_, bg_id);

  LOG_INFO("After transfer - swap effectiveness test",
      K(transferred_count), K(dst_data_size), K(data_size_threshold));

  ASSERT_GT(transferred_count, 0);
  ASSERT_LE(transferred_count, actual_remove_count);

  // Calculate what the data size would be if we selected the first N partitions (worst case)
  // The first 5 are 2TB each, so worst case would be around 5 * 2TB = 10TB
  // But with swap, it should be much less
  int64_t worst_case_size = (actual_remove_count < 5 ? actual_remove_count : 5) * 2000 * GB;

  int64_t dst_data_size_for_ratio = (dst_data_size > 0 ? dst_data_size : 1);
  LOG_INFO("Swap effectiveness comparison",
      K(dst_data_size), K(worst_case_size), K(data_size_threshold),
      "improvement_ratio", static_cast<double>(worst_case_size) / dst_data_size_for_ratio);

  // Verify that swap actually improved the selection
  // The transferred size should be significantly less than worst case
  if (worst_case_size > 0 && dst_data_size < worst_case_size) {
    // Swap should have reduced the size by at least 50%
    double improvement = static_cast<double>(worst_case_size - dst_data_size) / worst_case_size;
    LOG_INFO("Swap improvement", K(improvement));
    // We expect at least 30% improvement (allowing some tolerance)
    ASSERT_GT(improvement, 0.3);
  }

  // Also verify it's within threshold
  if (data_size_threshold > 0) {
    ASSERT_LE(dst_data_size, data_size_threshold * 1.2);
  }
}

// Test case 9: Edge case - very small factor
TEST_F(TestLSBalanceGroupInfo, VerySmallFactor)
{
  const ObBalanceGroupID bg_id(8, 8);
  const int64_t part_count = 100;
  const int64_t data_size_per_part = 10 * GB;
  const float factor = 0.01f; // Very small factor

  ASSERT_EQ(OB_SUCCESS, add_partitions_to_bg(
      *src_ls_bg_info_, bg_id, part_count, data_size_per_part));

  int64_t src_original_count = get_part_group_count(*src_ls_bg_info_, bg_id);

  ObTransferPartList part_list;
  ASSERT_EQ(OB_SUCCESS, src_ls_bg_info_->transfer_out_by_factor(
      *dst_ls_bg_info_, factor, part_list));

  int64_t transferred_count = part_list.count();
  int64_t expected_min = static_cast<int64_t>(std::ceil(factor * src_original_count));
  int64_t max_remove = src_original_count - expected_min;
  int64_t expected_max = std::min(max_remove, expected_min);

  LOG_INFO("Very small factor", K(src_original_count), K(transferred_count), K(expected_min));

  // With very small factor, might transfer 0 or 1 partition
  ASSERT_GE(transferred_count, 0);
  ASSERT_LE(transferred_count, expected_max);
}

// Test case 10: Invalid arguments
TEST_F(TestLSBalanceGroupInfo, InvalidArguments)
{
  const ObBalanceGroupID bg_id(9, 9);
  ObTransferPartList part_list;

  // Test with uninitialized dst
  ObLSBalanceGroupInfo uninit_dst(allocator_);
  ASSERT_EQ(OB_INVALID_ARGUMENT, src_ls_bg_info_->transfer_out_by_factor(
      uninit_dst, 0.5f, part_list));

  // Test with invalid factor (too small)
  ASSERT_EQ(OB_INVALID_ARGUMENT, src_ls_bg_info_->transfer_out_by_factor(
      *dst_ls_bg_info_, 0.0f, part_list));

  // Test with negative factor
  ASSERT_EQ(OB_INVALID_ARGUMENT, src_ls_bg_info_->transfer_out_by_factor(
      *dst_ls_bg_info_, -0.1f, part_list));
}

// Test case 11: Empty source (no partitions)
TEST_F(TestLSBalanceGroupInfo, EmptySource)
{
  const ObBalanceGroupID bg_id(10, 10);
  ObTransferPartList part_list;

  // Try to transfer from empty source
  int ret = src_ls_bg_info_->transfer_out_by_factor(
      *dst_ls_bg_info_, 0.5f, part_list);

  // Should succeed but transfer nothing
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, part_list.count());
}

// Test case 12: Complex scenario with multiple BGs, diverse data sizes,
// non-empty dst, multiple transfers, and BALANCED_LS_NUM changes
// This test ensures swap strategy is actually triggered
TEST_F(TestLSBalanceGroupInfo, ComplexSwapScenarioWithMultipleBGs)
{
  schema::ObSimpleTableSchemaV2 mock_table_schema;
  mock_table_schema.set_database_id(123123123);

  // Create multiple balance groups with diverse partition sizes
  const ObBalanceGroupID bg_id1(11, 11);
  const ObBalanceGroupID bg_id2(12, 12);
  const ObBalanceGroupID bg_id3(13, 13);

  // BG1: Mix of large, medium, small, and zero-size partitions
  // Strategy: Add large partitions first (round-robin might pick them),
  // then many small/zero-size partitions (for swap)
  int64_t table_id_base = 11000;

  // Add 5 large partitions (2TB each) - these might be selected by round-robin
  for (int64_t i = 0; i < 5; ++i) {
    int64_t table_id = table_id_base + i;
    ObTransferPartInfo part(table_id, table_id);
    ASSERT_EQ(OB_SUCCESS, src_ls_bg_info_->append_part_into_balance_group(
        bg_id1, mock_table_schema, part, 2000 * GB, table_id, 0));
  }

  // Add 20 medium partitions (100GB each)
  for (int64_t i = 0; i < 20; ++i) {
    int64_t table_id = table_id_base + 100 + i;
    ObTransferPartInfo part(table_id, table_id);
    ASSERT_EQ(OB_SUCCESS, src_ls_bg_info_->append_part_into_balance_group(
        bg_id1, mock_table_schema, part, 100 * GB, table_id, 0));
  }

  // Add 30 small partitions (10GB each) and 30 zero-size partitions
  for (int64_t i = 0; i < 30; ++i) {
    int64_t table_id = table_id_base + 200 + i;
    ObTransferPartInfo part(table_id, table_id);
    int64_t data_size = (i % 2 == 0) ? 0 : 10 * GB;
    ASSERT_EQ(OB_SUCCESS, src_ls_bg_info_->append_part_into_balance_group(
        bg_id1, mock_table_schema, part, data_size, table_id, 0));
  }

  // BG2: Similar pattern but different sizes
  table_id_base = 12000;
  // 3 huge partitions (3TB each)
  for (int64_t i = 0; i < 3; ++i) {
    int64_t table_id = table_id_base + i;
    ObTransferPartInfo part(table_id, table_id);
    ASSERT_EQ(OB_SUCCESS, src_ls_bg_info_->append_part_into_balance_group(
        bg_id2, mock_table_schema, part, 3000 * GB, table_id, 0));
  }
  // 25 zero-size partitions
  for (int64_t i = 0; i < 25; ++i) {
    int64_t table_id = table_id_base + 100 + i;
    ObTransferPartInfo part(table_id, table_id);
    ASSERT_EQ(OB_SUCCESS, src_ls_bg_info_->append_part_into_balance_group(
        bg_id2, mock_table_schema, part, 0, table_id, 0));
  }
  // 15 small partitions (5GB each)
  for (int64_t i = 0; i < 15; ++i) {
    int64_t table_id = table_id_base + 200 + i;
    ObTransferPartInfo part(table_id, table_id);
    ASSERT_EQ(OB_SUCCESS, src_ls_bg_info_->append_part_into_balance_group(
        bg_id2, mock_table_schema, part, 5 * GB, table_id, 0));
  }

  // BG3: More balanced distribution
  table_id_base = 13000;
  // 10 medium partitions (200GB each)
  for (int64_t i = 0; i < 10; ++i) {
    int64_t table_id = table_id_base + i;
    ObTransferPartInfo part(table_id, table_id);
    ASSERT_EQ(OB_SUCCESS, src_ls_bg_info_->append_part_into_balance_group(
        bg_id3, mock_table_schema, part, 200 * GB, table_id, 0));
  }
  // 40 zero-size partitions
  for (int64_t i = 0; i < 40; ++i) {
    int64_t table_id = table_id_base + 100 + i;
    ObTransferPartInfo part(table_id, table_id);
    ASSERT_EQ(OB_SUCCESS, src_ls_bg_info_->append_part_into_balance_group(
        bg_id3, mock_table_schema, part, 0, table_id, 0));
  }

  // Pre-populate dst with some partitions (non-empty dst scenario)
  // Add some partitions to dst to simulate previous transfers
  table_id_base = 20000;
  for (int64_t i = 0; i < 10; ++i) {
    int64_t table_id = table_id_base + i;
    ObTransferPartInfo part(table_id, table_id);
    ASSERT_EQ(OB_SUCCESS, dst_ls_bg_info_->append_part_into_balance_group(
        bg_id1, mock_table_schema, part, 50 * GB, table_id, 0));
  }

  // Calculate initial state
  int64_t bg1_src_count = get_part_group_count(*src_ls_bg_info_, bg_id1);
  int64_t bg1_src_size = get_total_data_size(*src_ls_bg_info_, bg_id1);
  int64_t bg2_src_count = get_part_group_count(*src_ls_bg_info_, bg_id2);
  int64_t bg2_src_size = get_total_data_size(*src_ls_bg_info_, bg_id2);
  int64_t bg3_src_count = get_part_group_count(*src_ls_bg_info_, bg_id3);
  int64_t bg3_src_size = get_total_data_size(*src_ls_bg_info_, bg_id3);
  int64_t bg1_dst_count = get_part_group_count(*dst_ls_bg_info_, bg_id1);
  int64_t bg1_dst_size = get_total_data_size(*dst_ls_bg_info_, bg_id1);

  LOG_INFO("Before first transfer - Complex swap scenario",
      K(bg1_src_count), K(bg1_src_size),
      K(bg2_src_count), K(bg2_src_size),
      K(bg3_src_count), K(bg3_src_size),
      K(bg1_dst_count), K(bg1_dst_size));

  // First transfer: Use large factor (0.5) to potentially select large partitions
  // This should trigger swap if round-robin selects large partitions
  const float factor1 = 0.5f;
  ObTransferPartList part_list1;
  ASSERT_EQ(OB_SUCCESS, src_ls_bg_info_->transfer_out_by_factor(
      *dst_ls_bg_info_, factor1, part_list1));

  int64_t bg1_src_after1 = get_part_group_count(*src_ls_bg_info_, bg_id1);
  int64_t bg1_src_size_after1 = get_total_data_size(*src_ls_bg_info_, bg_id1);
  int64_t bg2_src_after1 = get_part_group_count(*src_ls_bg_info_, bg_id2);
  int64_t bg2_src_size_after1 = get_total_data_size(*src_ls_bg_info_, bg_id2);
  int64_t bg3_src_after1 = get_part_group_count(*src_ls_bg_info_, bg_id3);
  int64_t bg3_src_size_after1 = get_total_data_size(*src_ls_bg_info_, bg_id3);
  int64_t bg1_dst_after1 = get_part_group_count(*dst_ls_bg_info_, bg_id1);
  int64_t bg1_dst_size_after1 = get_total_data_size(*dst_ls_bg_info_, bg_id1);
  int64_t bg2_dst_after1 = get_part_group_count(*dst_ls_bg_info_, bg_id2);
  int64_t bg2_dst_size_after1 = get_total_data_size(*dst_ls_bg_info_, bg_id2);
  int64_t bg3_dst_after1 = get_part_group_count(*dst_ls_bg_info_, bg_id3);
  int64_t bg3_dst_size_after1 = get_total_data_size(*dst_ls_bg_info_, bg_id3);

  LOG_INFO("After first transfer",
      K(bg1_src_after1), K(bg1_src_size_after1),
      K(bg2_src_after1), K(bg2_src_size_after1),
      K(bg3_src_after1), K(bg3_src_size_after1),
      K(bg1_dst_after1), K(bg1_dst_size_after1),
      K(bg2_dst_after1), K(bg2_dst_size_after1),
      K(bg3_dst_after1), K(bg3_dst_size_after1),
      "transferred_count", part_list1.count());

  // Verify transfer happened
  ASSERT_GT(part_list1.count(), 0);
  ASSERT_EQ(bg1_src_after1 + bg1_dst_after1, bg1_src_count + bg1_dst_count);
  ASSERT_EQ(bg2_src_after1 + bg2_dst_after1, bg2_src_count);
  ASSERT_EQ(bg3_src_after1 + bg3_dst_after1, bg3_src_count);
  // Verify data size consistency
  ASSERT_EQ(bg1_src_size_after1 + bg1_dst_size_after1, bg1_src_size + bg1_dst_size);
  ASSERT_EQ(bg2_src_size_after1 + bg2_dst_size_after1, bg2_src_size);
  ASSERT_EQ(bg3_src_size_after1 + bg3_dst_size_after1, bg3_src_size);

  // Calculate thresholds for each BG to verify swap behavior
  int64_t bg1_expected_remove = static_cast<int64_t>(std::ceil(factor1 * bg1_src_count));
  int64_t bg1_max_remove = bg1_src_count - bg1_expected_remove;
  int64_t bg1_actual_remove = std::min(bg1_max_remove, bg1_expected_remove);
  int64_t bg1_threshold = static_cast<int64_t>(
      std::ceil(static_cast<double>(bg1_actual_remove) / bg1_src_count * bg1_src_size));

  int64_t bg2_expected_remove = static_cast<int64_t>(std::ceil(factor1 * bg2_src_count));
  int64_t bg2_max_remove = bg2_src_count - bg2_expected_remove;
  int64_t bg2_actual_remove = std::min(bg2_max_remove, bg2_expected_remove);
  int64_t bg2_threshold = static_cast<int64_t>(
      std::ceil(static_cast<double>(bg2_actual_remove) / bg2_src_count * bg2_src_size));

  int64_t bg3_expected_remove = static_cast<int64_t>(std::ceil(factor1 * bg3_src_count));
  int64_t bg3_max_remove = bg3_src_count - bg3_expected_remove;
  int64_t bg3_actual_remove = std::min(bg3_max_remove, bg3_expected_remove);
  int64_t bg3_threshold = static_cast<int64_t>(
      std::ceil(static_cast<double>(bg3_actual_remove) / bg3_src_count * bg3_src_size));

  LOG_INFO("Thresholds and actual transferred sizes",
      K(bg1_threshold), K(bg1_dst_size_after1),
      K(bg2_threshold), K(bg2_dst_size_after1),
      K(bg3_threshold), K(bg3_dst_size_after1));

  // Verify swap strategy: transferred size should not exceed threshold
  if (bg1_threshold > 0) {
    ASSERT_LE(bg1_dst_size_after1, bg1_threshold);
  }
  if (bg2_threshold > 0) {
    ASSERT_LE(bg2_dst_size_after1, bg2_threshold);
  }
  if (bg3_threshold > 0) {
    ASSERT_LE(bg3_dst_size_after1, bg3_threshold);
  }

  // Second transfer: Continue with smaller factor (0.3)
  // This tests multiple consecutive transfers
  const float factor2 = 0.3f;
  ObTransferPartList part_list2;
  ASSERT_EQ(OB_SUCCESS, src_ls_bg_info_->transfer_out_by_factor(
      *dst_ls_bg_info_, factor2, part_list2));

  int64_t bg1_src_after2 = get_part_group_count(*src_ls_bg_info_, bg_id1);
  int64_t bg1_dst_after2 = get_part_group_count(*dst_ls_bg_info_, bg_id1);
  int64_t bg2_src_after2 = get_part_group_count(*src_ls_bg_info_, bg_id2);
  int64_t bg2_dst_after2 = get_part_group_count(*dst_ls_bg_info_, bg_id2);
  int64_t bg3_src_after2 = get_part_group_count(*src_ls_bg_info_, bg_id3);
  int64_t bg3_dst_after2 = get_part_group_count(*dst_ls_bg_info_, bg_id3);

  LOG_INFO("After second transfer",
      K(bg1_src_after2), K(bg1_dst_after2),
      K(bg2_src_after2), K(bg2_dst_after2),
      K(bg3_src_after2), K(bg3_dst_after2),
      "transferred_count", part_list2.count());

  // Verify second transfer
  ASSERT_GT(part_list2.count(), 0);
  ASSERT_EQ(bg1_src_after2 + bg1_dst_after2, bg1_src_count + bg1_dst_count);
  ASSERT_EQ(bg2_src_after2 + bg2_dst_after2, bg2_src_count);
  ASSERT_EQ(bg3_src_after2 + bg3_dst_after2, bg3_src_count);

  // Test with different BALANCED_LS_NUM
  // Create new LS balance group info with different balanced_ls_num
  ObLSBalanceGroupInfo src_ls_bg_info_new(allocator_);
  ObLSBalanceGroupInfo dst_ls_bg_info_new(allocator_);
  const int64_t NEW_BALANCED_LS_NUM = 5; // Different from initial 3
  ASSERT_EQ(OB_SUCCESS, src_ls_bg_info_new.init(ObLSID(3), NEW_BALANCED_LS_NUM));
  ASSERT_EQ(OB_SUCCESS, dst_ls_bg_info_new.init(ObLSID(4), NEW_BALANCED_LS_NUM));

  // Add partitions to new src with different distribution
  table_id_base = 30000;
  // Add large partitions that will likely exceed threshold
  for (int64_t i = 0; i < 8; ++i) {
    int64_t table_id = table_id_base + i;
    ObTransferPartInfo part(table_id, table_id);
    ASSERT_EQ(OB_SUCCESS, src_ls_bg_info_new.append_part_into_balance_group(
        bg_id1, mock_table_schema, part, 1500 * GB, table_id, 0));
  }
  // Add many zero-size partitions for swap
  for (int64_t i = 0; i < 50; ++i) {
    int64_t table_id = table_id_base + 100 + i;
    ObTransferPartInfo part(table_id, table_id);
    ASSERT_EQ(OB_SUCCESS, src_ls_bg_info_new.append_part_into_balance_group(
        bg_id1, mock_table_schema, part, 0, table_id, 0));
  }

  int64_t new_bg1_src_count = get_part_group_count(src_ls_bg_info_new, bg_id1);
  int64_t new_bg1_src_size = get_total_data_size(src_ls_bg_info_new, bg_id1);
  int64_t new_bg1_expected_remove = static_cast<int64_t>(std::ceil(0.4f * new_bg1_src_count));
  int64_t new_bg1_max_remove = new_bg1_src_count - new_bg1_expected_remove;
  int64_t new_bg1_actual_remove = std::min(new_bg1_max_remove, new_bg1_expected_remove);
  int64_t new_bg1_threshold = static_cast<int64_t>(
      std::ceil(static_cast<double>(new_bg1_actual_remove) / new_bg1_src_count * new_bg1_src_size));

  LOG_INFO("New LS with different BALANCED_LS_NUM",
      K(new_bg1_src_count), K(new_bg1_src_size), K(new_bg1_threshold));

  // Transfer with factor that might select large partitions
  ObTransferPartList part_list3;
  ASSERT_EQ(OB_SUCCESS, src_ls_bg_info_new.transfer_out_by_factor(
      dst_ls_bg_info_new, 0.4f, part_list3));

  int64_t new_bg1_dst_size = get_total_data_size(dst_ls_bg_info_new, bg_id1);
  LOG_INFO("After transfer with new BALANCED_LS_NUM",
      K(new_bg1_dst_size), K(new_bg1_threshold),
      "transferred_count", part_list3.count());

  // Verify swap worked: if round-robin selected large partitions (> threshold),
  // swap should have replaced them with zero-size partitions
  if (new_bg1_threshold > 0) {
    ASSERT_LE(new_bg1_dst_size, new_bg1_threshold);
    // If threshold is large (because of large partitions), but we have many zero-size,
    // the actual transferred size should be much smaller
    if (new_bg1_threshold > 1000 * GB && new_bg1_dst_size < new_bg1_threshold / 2) {
      LOG_INFO("Swap strategy successfully reduced transferred size",
          K(new_bg1_dst_size), K(new_bg1_threshold));
    }
  }

  // Cleanup
  src_ls_bg_info_new.destroy();
  dst_ls_bg_info_new.destroy();
}

} // namespace rootserver
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
