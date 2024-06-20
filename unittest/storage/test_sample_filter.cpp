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
#define protected public
#include "storage/access/ob_sample_filter.h"
namespace oceanbase
{
using namespace blocksstable;
namespace unittest
{
class ObRowSampleFilterTest : public ::testing::Test
{
public:
  void SetUp()
  {
    sql::ObPushdownOperator *op = nullptr;
    ObPushdownSampleFilterNode *filter_node = nullptr;
    void *buf = nullptr;
    ASSERT_NE(nullptr, buf = allocator_.alloc(sizeof(ObSampleFilterExecutor)));
    sample_executor_ = new (buf) ObSampleFilterExecutor(allocator_, *filter_node, *op);

    SampleInfo sample_info;
    sample_info.method_ = SampleInfo::ROW_SAMPLE;
    sample_info.percent_ = 40.0;
    sample_info.seed_ = 0;
    ASSERT_EQ(OB_SUCCESS, sample_executor_->init(sample_info, false, &allocator_));
    EXPECT_EQ(4, sample_executor_->interval_infos_[0].interval_length_);
  }

  void TearDown()
  {
    sample_executor_->reset();
    allocator_.free(sample_executor_);
    sample_executor_ = nullptr;
  }

  void check_sample_range(
      const int64_t start,
      const int64_t end,
      ObMicroIndexInfo &index_info,
      const bool result)
  {
    printf("check range: [%ld, %ld]\n", start, end);
    ObIndexBlockRowHeader row_header;
    index_info.row_header_ = &row_header;
    row_header.row_count_ = end - start + 1;
    index_info.set_filter_constant_type(sql::ObBoolMaskType::PROBABILISTIC);
    ASSERT_EQ(OB_SUCCESS, sample_executor_->check_range_filtered(index_info, start));
    EXPECT_EQ(result, index_info.is_filter_always_false());
  }

public:
  ObArenaAllocator allocator_;
  ObSampleFilterExecutor *sample_executor_;
};

TEST_F(ObRowSampleFilterTest, test_filter_single_row)
{
  bool result[20] = {0, 1, 0, 0,
                     0, 0, 1, 1,
                     0, 0, 1, 0,
                     0, 1, 1, 0,
                     1, 1, 0, 0};
  bool filtered = false;
  for(int row_num = 0; row_num < 20; row_num++) {
    sample_executor_->check_single_row_filtered(row_num, filtered);
    EXPECT_EQ(result[row_num], !filtered);
  }
}

TEST_F(ObRowSampleFilterTest, test_filter_range)
{
  ObMicroIndexInfo index_info;
  check_sample_range(0, 0, index_info, true);
  check_sample_range(2, 5, index_info, true);
  check_sample_range(8, 9, index_info, true);
  check_sample_range(11, 12, index_info, true);
  check_sample_range(15, 15, index_info, true);
  check_sample_range(18, 19, index_info, true);

  check_sample_range(0, 2, index_info, false);
  check_sample_range(0, 4, index_info, false);
  check_sample_range(5, 6, index_info, false);
  check_sample_range(5, 9, index_info, false);
  check_sample_range(7, 9, index_info, false);
  check_sample_range(12, 13, index_info, false);
  check_sample_range(15, 16, index_info, false);
  check_sample_range(17, 17, index_info, false);
  check_sample_range(0, 19, index_info, false);
}

TEST_F(ObRowSampleFilterTest, test_set_sample_bitmap)
{
  bool result[20] = {0, 1, 0, 0,
                     0, 0, 1, 1,
                     0, 0, 1, 0,
                     0, 1, 1, 0,
                     1, 1, 0, 0};
  ObBitmap *result_bitmap = nullptr;
  ASSERT_EQ(OB_SUCCESS, sample_executor_->init_bitmap(20, result_bitmap));
  ASSERT_EQ(OB_SUCCESS, sample_executor_->set_sample_bitmap(0, 20, *result_bitmap));
  for (int row_num = 0; row_num < 20; row_num++) {
    EXPECT_EQ(result[row_num], result_bitmap->test(row_num));
  }
}
} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  printf("start running test\n");
  return RUN_ALL_TESTS();
}
