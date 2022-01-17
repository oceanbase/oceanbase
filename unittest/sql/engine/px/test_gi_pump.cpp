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

#define USING_LOG_PREFIX SQL_EXE
#include <gtest/gtest.h>
#define private public
#define protected public
#include <stdarg.h>

#include "lib/utility/ob_tracepoint.h"
#include "sql/ob_sql_init.h"
#include "ob_fake_partition_location_cache.h"
#include "ob_fake_partition_service.h"
#include "ob_fake_partition_location_cache.h"
#include "ob_fake_partition_service.h"
#include "sql/engine/px/ob_granule_util.h"
#undef protected
#undef private

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::storage;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

#define PUSH_ITEM(array, item, times) \
  for (int64_t i = 0; i < times; ++i) \
  array.push_back(item)

class ObGiPumpTest : public ::testing::Test {
public:
  const static int64_t TEST_PARTITION_COUNT = 5;
  const static int64_t TEST_SPLIT_TASK_COUNT = 8;

  ObGiPumpTest();
  virtual ~ObGiPumpTest();
  virtual void SetUp();
  virtual void TearDown();
  void TestGISplitTaskCase(int64_t case_idx);

private:
  // disallow copy
  ObGiPumpTest(const ObGiPumpTest& other);
  ObGiPumpTest& operator=(const ObGiPumpTest& other);

public:
  // data members
  ObSEArray<ObPartitionArray, 32> pkeys_array_;
  ObFakePartitionServiceForGI partition_service_;
  ObSEArray<uint64_t, 32> total_macros_by_case_;
  ObSEArray<uint64_t, 32> empty_partition_count_by_case_;
};

ObGiPumpTest::ObGiPumpTest()
{}

ObGiPumpTest::~ObGiPumpTest()
{}

void ObGiPumpTest::SetUp()
{
  /*
   * first type data: all partition micro-blocks empty
   * case 1        partition id    1     2     3     4     5
   *       micro-blocks num        0     0     0     0     0
   * second type data: some partition's micro-blocks are empty
   * case 2        partition id   11    12    13    14    15
   *      micro-blocks num         0     0   300    16     0
   * case 3        partition id   21    22    23    24    25
   *      micro-blocks num         0    16     0    16     0
   * case 4        partition id   31    32    33    34    35
   *      micro-blocks num         0    57    16     0    16
   * case 5        partition id   41    42    43    44    45
   *      micro-blocks num      1021     1     2     1     0
   * third type data: all micro-blocks are not empty
   * case 6        partition id   51    52    53    54    55
   *      micro-blocks num      1021     1   312     1  1021
   * case 7        partition id   61    62    63    64    65
   *      micro-blocks num         1     1    12     1  1021
   * case 8        partition id   71    72    73    74    75
   *      micro-blocks num         1     1    1   2044     1
   * */

  ObPartitionArray pkeys;
  ObPartitionKey key1;
  ObPartitionKey key2;
  ObPartitionKey key3;
  ObPartitionKey key4;
  ObPartitionKey key5;
  /*                 case result 1                      */
  ObFakePartitionServiceForGI::ObGIResult case_1;
  case_1.macros_count_.push_back(0);
  case_1.macros_count_.push_back(0);
  case_1.macros_count_.push_back(0);
  case_1.macros_count_.push_back(0);
  case_1.macros_count_.push_back(0);
  key1.init(1, 1, 10);
  key2.init(1, 2, 10);
  key3.init(1, 3, 10);
  key4.init(1, 4, 10);
  key5.init(1, 5, 10);
  pkeys.push_back(key1);
  pkeys.push_back(key2);
  pkeys.push_back(key3);
  pkeys.push_back(key4);
  pkeys.push_back(key5);
  pkeys_array_.push_back(pkeys);
  partition_service_.result_set_.push_back(case_1);
  total_macros_by_case_.push_back(0);
  empty_partition_count_by_case_.push_back(5);
  /*                 case result 2                      */
  ObFakePartitionServiceForGI::ObGIResult case_2;
  pkeys.reset();
  case_2.macros_count_.push_back(0);
  case_2.macros_count_.push_back(0);
  case_2.macros_count_.push_back(300);
  case_2.macros_count_.push_back(16);
  case_2.macros_count_.push_back(0);
  key1.init(1, 11, 10);
  key2.init(1, 12, 10);
  key3.init(1, 13, 10);
  key4.init(1, 14, 10);
  key5.init(1, 15, 10);
  pkeys.push_back(key1);
  pkeys.push_back(key2);
  pkeys.push_back(key3);
  pkeys.push_back(key4);
  pkeys.push_back(key5);
  pkeys_array_.push_back(pkeys);
  partition_service_.result_set_.push_back(case_2);
  total_macros_by_case_.push_back(316);
  empty_partition_count_by_case_.push_back(3);
  /*                 case result 3                      */
  ObFakePartitionServiceForGI::ObGIResult case_3;
  pkeys.reset();
  case_3.macros_count_.push_back(0);
  case_3.macros_count_.push_back(16);
  case_3.macros_count_.push_back(0);
  case_3.macros_count_.push_back(16);
  case_3.macros_count_.push_back(0);
  key1.init(1, 21, 10);
  key2.init(1, 22, 10);
  key3.init(1, 23, 10);
  key4.init(1, 24, 10);
  key5.init(1, 25, 10);
  pkeys.push_back(key1);
  pkeys.push_back(key2);
  pkeys.push_back(key3);
  pkeys.push_back(key4);
  pkeys.push_back(key5);
  pkeys_array_.push_back(pkeys);
  partition_service_.result_set_.push_back(case_3);
  total_macros_by_case_.push_back(16 + 16);
  empty_partition_count_by_case_.push_back(3);
  /*                 case result 4                      */
  ObFakePartitionServiceForGI::ObGIResult case_4;
  pkeys.reset();
  case_4.macros_count_.push_back(0);
  case_4.macros_count_.push_back(57);
  case_4.macros_count_.push_back(16);
  case_4.macros_count_.push_back(0);
  case_4.macros_count_.push_back(16);
  key1.init(1, 31, 10);
  key2.init(1, 32, 10);
  key3.init(1, 33, 10);
  key4.init(1, 34, 10);
  key5.init(1, 35, 10);
  pkeys.push_back(key1);
  pkeys.push_back(key2);
  pkeys.push_back(key3);
  pkeys.push_back(key4);
  pkeys.push_back(key5);
  pkeys_array_.push_back(pkeys);
  partition_service_.result_set_.push_back(case_4);
  total_macros_by_case_.push_back(57 + 16 + 16);
  empty_partition_count_by_case_.push_back(2);
  /*                 case result 5                      */
  ObFakePartitionServiceForGI::ObGIResult case_5;
  pkeys.reset();
  case_5.macros_count_.push_back(1021);
  case_5.macros_count_.push_back(1);
  case_5.macros_count_.push_back(2);
  case_5.macros_count_.push_back(1);
  case_5.macros_count_.push_back(0);
  key1.init(1, 41, 10);
  key2.init(1, 42, 10);
  key3.init(1, 43, 10);
  key4.init(1, 44, 10);
  key5.init(1, 45, 10);
  pkeys.push_back(key1);
  pkeys.push_back(key2);
  pkeys.push_back(key3);
  pkeys.push_back(key4);
  pkeys.push_back(key5);
  pkeys_array_.push_back(pkeys);
  partition_service_.result_set_.push_back(case_5);
  total_macros_by_case_.push_back(1021 + 1 + 2 + 1);
  empty_partition_count_by_case_.push_back(1);
  /*                 case result 6                      */
  ObFakePartitionServiceForGI::ObGIResult case_6;
  pkeys.reset();
  case_6.macros_count_.push_back(1021);
  case_6.macros_count_.push_back(1);
  case_6.macros_count_.push_back(312);
  case_6.macros_count_.push_back(1);
  case_6.macros_count_.push_back(1021);
  key1.init(1, 51, 10);
  key2.init(1, 52, 10);
  key3.init(1, 53, 10);
  key4.init(1, 54, 10);
  key5.init(1, 55, 10);
  pkeys.push_back(key1);
  pkeys.push_back(key2);
  pkeys.push_back(key3);
  pkeys.push_back(key4);
  pkeys.push_back(key5);
  pkeys_array_.push_back(pkeys);
  partition_service_.result_set_.push_back(case_6);
  total_macros_by_case_.push_back(1021 + 1 + 312 + 1 + 1021);
  empty_partition_count_by_case_.push_back(0);
  /*                 case result 7                      */
  ObFakePartitionServiceForGI::ObGIResult case_7;
  pkeys.reset();
  case_7.macros_count_.push_back(1);
  case_7.macros_count_.push_back(1);
  case_7.macros_count_.push_back(12);
  case_7.macros_count_.push_back(1);
  case_7.macros_count_.push_back(1021);
  key1.init(1, 61, 10);
  key2.init(1, 62, 10);
  key3.init(1, 63, 10);
  key4.init(1, 64, 10);
  key5.init(1, 65, 10);
  pkeys.push_back(key1);
  pkeys.push_back(key2);
  pkeys.push_back(key3);
  pkeys.push_back(key4);
  pkeys.push_back(key5);
  pkeys_array_.push_back(pkeys);
  partition_service_.result_set_.push_back(case_7);
  total_macros_by_case_.push_back(1 + 1 + 12 + 1 + 1021);
  empty_partition_count_by_case_.push_back(0);
  /*                 case result 8                      */
  ObFakePartitionServiceForGI::ObGIResult case_8;
  pkeys.reset();
  case_8.macros_count_.push_back(1);
  case_8.macros_count_.push_back(1);
  case_8.macros_count_.push_back(1);
  case_8.macros_count_.push_back(2044);
  case_8.macros_count_.push_back(1);
  key1.init(1, 71, 10);
  key2.init(1, 72, 10);
  key3.init(1, 73, 10);
  key4.init(1, 74, 10);
  key5.init(1, 75, 10);
  pkeys.push_back(key1);
  pkeys.push_back(key2);
  pkeys.push_back(key3);
  pkeys.push_back(key4);
  pkeys.push_back(key5);
  pkeys_array_.push_back(pkeys);
  partition_service_.result_set_.push_back(case_8);
  total_macros_by_case_.push_back(1 + 1 + 1 + 2044 + 1);
  empty_partition_count_by_case_.push_back(0);
}

void ObGiPumpTest::TearDown()
{}

// execute case
TEST_F(ObGiPumpTest, split_task_test)
{
  int64_t case_count = ObGiPumpTest::TEST_SPLIT_TASK_COUNT;
  for (int64_t i = 0; i < case_count; ++i) {
    // TestGISplitTaskCase(i);
  }
}

TEST_F(ObGiPumpTest, task_count_test)
{
  int ret = OB_SUCCESS;
  /*
   * There are many factors that affect the number of tasks.
   * Construct a special combination of test values.
   * The default micro-block size is 2M, each
   *
   *
   * */
  int64_t total_macros_count = 0;
  int64_t total_task_count = 0;
  ObParallelBlockRangeTaskParams params;
  params.parallelism_ = 3;
  params.expected_task_load_ = 128;
  params.max_task_count_per_thread_ = 100;
  params.min_task_count_per_thread_ = 13;
  params.min_task_access_size_ = 2;  //(2M)

  /*                 case 1                   */
  // The minimum task is 1 macro block
  total_macros_count = 1;
  if (OB_FAIL(ObGranuleUtil::compute_task_count(params, total_macros_count, total_task_count))) {
    LOG_WARN("compute task count failed", K(ret));
  }
  ASSERT_TRUE(total_task_count == 1);
  total_macros_count = 2;
  if (OB_FAIL(ObGranuleUtil::compute_task_count(params, total_macros_count, total_task_count))) {
    LOG_WARN("compute task count failed", K(ret));
  }
  ASSERT_TRUE(total_task_count == 2);
  total_macros_count = 3;
  if (OB_FAIL(ObGranuleUtil::compute_task_count(params, total_macros_count, total_task_count))) {
    LOG_WARN("compute task count failed", K(ret));
  }
  ASSERT_TRUE(total_task_count == 3);
  // The minimum task must be 30M, so only one task should be drawn out here
  params.min_task_access_size_ = 30;
  total_macros_count = 3;
  if (OB_FAIL(ObGranuleUtil::compute_task_count(params, total_macros_count, total_task_count))) {
    LOG_WARN("compute task count failed", K(ret));
  }
  ASSERT_TRUE(total_task_count == 1);

  /*                 case 2                   */
  //[13,100]
  total_macros_count = 1300;
  params.min_task_access_size_ = 2;
  params.parallelism_ = 100;
  if (OB_FAIL(ObGranuleUtil::compute_task_count(params, total_macros_count, total_task_count))) {
    LOG_WARN("compute task count failed", K(ret));
  }
  ASSERT_TRUE(total_task_count == 1300);
  total_macros_count = 340;
  params.parallelism_ = 10;
  if (OB_FAIL(ObGranuleUtil::compute_task_count(params, total_macros_count, total_task_count))) {
    LOG_WARN("compute task count failed", K(ret));
  }
  ASSERT_TRUE(total_task_count == 130);
  total_macros_count = 340;
  params.parallelism_ = 10;
  params.expected_task_load_ = 2;
  if (OB_FAIL(ObGranuleUtil::compute_task_count(params, total_macros_count, total_task_count))) {
    LOG_WARN("compute task count failed", K(ret));
  }
  ASSERT_TRUE(total_task_count == 340);

  /*                 case 3                   */
  // Falling down to calculate the number of tasks
  // based on the minimum amount of data that must be scanned for each task
  total_macros_count = 1300;
  params.min_task_access_size_ = 20;
  params.parallelism_ = 100;
  if (OB_FAIL(ObGranuleUtil::compute_task_count(params, total_macros_count, total_task_count))) {
    LOG_WARN("compute task count failed", K(ret));
  }
  ASSERT_TRUE(total_task_count == 1300 * 2 / 20);
  total_macros_count = 1300;
  params.min_task_access_size_ = 43;
  params.parallelism_ = 100;
  if (OB_FAIL(ObGranuleUtil::compute_task_count(params, total_macros_count, total_task_count))) {
    LOG_WARN("compute task count failed", K(ret));
  }
  ASSERT_TRUE(total_task_count == 1300 * 2 / 43);

  params.parallelism_ = 2;
  params.expected_task_load_ = 128;
  params.max_task_count_per_thread_ = 100;
  params.min_task_count_per_thread_ = 13;
  params.min_task_access_size_ = 2;  //(2M)
  if (OB_FAIL(ObGranuleUtil::compute_task_count(params, total_macros_count, total_task_count))) {
    LOG_WARN("compute task count failed", K(ret));
  }
  ASSERT_TRUE(total_task_count == 26);
}

// test ObGranuleUtil::compute_total_task_count
TEST_F(ObGiPumpTest, compute_total_task_count)
{
  int ret = OB_SUCCESS;
  int64_t total_data_size = 0;
  int64_t total_task_count = 0;
  ObParallelBlockRangeTaskParams params;
  // The degree of parallel is 3
  // expect each task load 128M
  // lower_bound = 3*128*13
  // upper_bound = 3*128*100
  params.parallelism_ = 3;
  params.expected_task_load_ = 128;
  params.max_task_count_per_thread_ = 100;
  params.min_task_count_per_thread_ = 13;
  params.min_task_access_size_ = 2;  //(2M)

  /*                 case 1                   */
  // each size is 0
  total_data_size = 0;
  if (OB_FAIL(ObGranuleUtil::compute_total_task_count(params, total_data_size, total_task_count))) {
    LOG_WARN("compute task count failed", K(ret));
  }
  ASSERT_TRUE(total_task_count == 0);
  total_data_size = 128 * 20;  // 128*20 < 3*128*13
  if (OB_FAIL(ObGranuleUtil::compute_total_task_count(params, total_data_size, total_task_count))) {
    LOG_WARN("compute task count failed", K(ret));
  }
  // 3*13
  ASSERT_TRUE(total_task_count == 39);
  total_data_size = 128 * 20 + 30;
  if (OB_FAIL(ObGranuleUtil::compute_total_task_count(params, total_data_size, total_task_count))) {
    LOG_WARN("compute task count failed", K(ret));
  }
  ASSERT_TRUE(total_task_count == 39);

  /*                 case 2                   */
  // [13,100]
  total_data_size = 3 * 128 * 13 + 128 * 10;
  if (OB_FAIL(ObGranuleUtil::compute_total_task_count(params, total_data_size, total_task_count))) {
    LOG_WARN("compute task count failed", K(ret));
  }
  ASSERT_TRUE(total_task_count == (3 * 13 + 10));
  total_data_size = 3 * 128 * 13 + 128 * 10 + 50;
  if (OB_FAIL(ObGranuleUtil::compute_total_task_count(params, total_data_size, total_task_count))) {
    LOG_WARN("compute task count failed", K(ret));
  }
  ASSERT_TRUE(total_task_count == (3 * 13 + 10));

  /*                 case 3                   */
  // data amount is greater than upper_bound
  total_data_size = 3 * 128 * 100 + 128 * 1;
  if (OB_FAIL(ObGranuleUtil::compute_total_task_count(params, total_data_size, total_task_count))) {
    LOG_WARN("compute task count failed", K(ret));
  }
  ASSERT_TRUE(total_task_count == (3 * 100));

  total_data_size = 3 * 128 * 100 + 128 * 13;
  if (OB_FAIL(ObGranuleUtil::compute_total_task_count(params, total_data_size, total_task_count))) {
    LOG_WARN("compute task count failed", K(ret));
  }
  ASSERT_TRUE(total_task_count == (3 * 100));
}

// test ObGranuleUtil::compute_task_count_each_partition
TEST_F(ObGiPumpTest, compute_task_count_each_partition)
{

  int ret = OB_SUCCESS;
  // Test whether the task cnt obtained by dividing each partition is correct in different situations
  // case1:
  int64_t total_size = 0;
  int64_t total_task_cnt = 0;  // invalid argument
  ObSEArray<int64_t, 4> size_each_partition;
  ObSEArray<int64_t, 4> task_cnt_each_partition;
  size_each_partition.push_back(0);
  size_each_partition.push_back(0);
  size_each_partition.push_back(0);
  size_each_partition.push_back(0);
  if (OB_FAIL(ObGranuleUtil::compute_task_count_each_partition(
          total_size, total_task_cnt, size_each_partition, task_cnt_each_partition))) {
    LOG_WARN("compute task count each partition failed", K(ret));
  }
  for (int i = 0; i < size_each_partition.count() && OB_SUCC(ret); i++) {
    ASSERT_TRUE(task_cnt_each_partition.at(i) == 1);
  }
  // case2:
  size_each_partition.reset();
  task_cnt_each_partition.reset();
  total_size = 400;
  total_task_cnt = 4;
  size_each_partition.push_back(100);
  size_each_partition.push_back(100);
  size_each_partition.push_back(100);
  size_each_partition.push_back(100);
  if (OB_FAIL(ObGranuleUtil::compute_task_count_each_partition(
          total_size, total_task_cnt, size_each_partition, task_cnt_each_partition))) {
    LOG_WARN("compute task count each partition failed", K(ret));
  }
  for (int i = 0; i < size_each_partition.count() && OB_SUCC(ret); i++) {
    ASSERT_TRUE(task_cnt_each_partition.at(i) == 1);
  }
  // case3:
  size_each_partition.reset();
  task_cnt_each_partition.reset();
  total_size = 400;
  total_task_cnt = 20;
  size_each_partition.push_back(0);
  size_each_partition.push_back(175);
  size_each_partition.push_back(205);
  size_each_partition.push_back(20);
  ObSEArray<int64_t, 4> checks;
  checks.push_back(1);
  checks.push_back(8);
  checks.push_back(10);
  checks.push_back(1);
  if (OB_FAIL(ObGranuleUtil::compute_task_count_each_partition(
          total_size, total_task_cnt, size_each_partition, task_cnt_each_partition))) {
    LOG_WARN("compute task count each partition failed", K(ret));
  }
  for (int i = 0; i < size_each_partition.count() && OB_SUCC(ret); i++) {
    LOG_INFO("each task cnt for partition", K(task_cnt_each_partition.at(i)), K(checks.at(i)));
    ASSERT_TRUE(task_cnt_each_partition.at(i) == checks.at(i));
  }
}

int main(int argc, char** argv)
{
  OB_LOGGER.set_log_level("TRACE");
  // oceanbase::common::ObLogger::get_logger().set_log_level("TRACE");
  init_sql_factories();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
