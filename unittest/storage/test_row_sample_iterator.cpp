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
#include <stdio.h>
#include "storage/ob_row_sample_iterator.h"
#include "mockcontainer/mock_ob_iterator.h"

namespace oceanbase
{
using namespace common;
using namespace storage;
namespace unittest
{
class TestObRowSampleIterator : public ::testing::Test
{
public:
  void check_result(const char *input, const char *output, const SampleInfo &sample_info);
  void check_iters(ObQueryRowIterator &iter1, ObQueryRowIterator &iter2);
};

void TestObRowSampleIterator::check_result(const char *input, const char *output, const SampleInfo &sample_info)
{
  ObMockQueryRowIterator input_iter;
  ObMockQueryRowIterator expect_iter;
  ObRowSampleIterator row_sample_iterator(sample_info);
  ASSERT_EQ(OB_SUCCESS, input_iter.from(input));
  ASSERT_EQ(OB_SUCCESS, expect_iter.from(output));
  ASSERT_EQ(OB_SUCCESS, row_sample_iterator.open(input_iter));
  ASSERT_TRUE(expect_iter.equals(row_sample_iterator));
}

void TestObRowSampleIterator::check_iters(ObQueryRowIterator &iter1, ObQueryRowIterator &iter2)
{
  int ret1 = OB_SUCCESS;
  int ret2 = OB_SUCCESS;
  ObStoreRow *row1 = nullptr;
  ObStoreRow *row2 = nullptr;
  while (OB_SUCCESS == ret1 && OB_SUCCESS == ret2) {
    ret1 = iter1.get_next_row(row1);
    ret2 = iter2.get_next_row(row2);
    if (ret1 == ret2 && OB_SUCCESS == ret1) {
      ASSERT_TRUE(nullptr != row1);
      ASSERT_TRUE(nullptr != row2);
      ASSERT_TRUE(ObMockIterator::equals(*row1, *row2));
    }
  }
  ASSERT_EQ(OB_ITER_END, ret1);
  ASSERT_EQ(ret1, ret2);
}

void print_hash_value(int64_t hash_input, double percent = 30.0)
{
  uint64_t hash = murmurhash(&hash_input, sizeof(int64_t), 100000);
  uint64_t cut_off = static_cast<uint64_t>(static_cast<double>(UINT64_MAX) * percent / 100.0);
  bool ret = hash <= cut_off;
  STORAGE_LOG(INFO, "hash value: ", K(hash_input), K(hash), K(cut_off), K(ret));
}

TEST_F(TestObRowSampleIterator, test_sample)
{
  SampleInfo sample_info;
  sample_info.method_ = SampleInfo::ROW_SAMPLE;
  sample_info.percent_ = 30.0;
  sample_info.seed_ = 100000;
  const char *input =
    "int var  flag  \n"
    "1   var1 EXIST \n"
    "2   var2 EXIST \n"
    "3   var3 EXIST \n"
    "4   var4 EXIST \n"
    "5   var5 EXIST \n";
  const char *output =
    "int var  flag  \n"
    "2   var2 EXIST \n"
    "4   var4 EXIST \n";
  check_result(input, output, sample_info);
  sample_info.percent_ = 99.99999;
  check_result(input, input, sample_info);
}

TEST_F(TestObRowSampleIterator, test_seed)
{
  SampleInfo sample_info;
  sample_info.method_ = SampleInfo::ROW_SAMPLE;
  sample_info.percent_ = 30.0;
  sample_info.seed_ = 0;
  ObRowSampleIterator iter1(sample_info);
  ObRowSampleIterator iter2(sample_info);
  ObMockQueryRowIterator miter1;
  ObMockQueryRowIterator miter2;
  const char *input =
    "int var  flag  \n"
    "1   var1 EXIST \n"
    "2   var2 EXIST \n"
    "3   var3 EXIST \n"
    "4   var4 EXIST \n"
    "5   var5 EXIST \n";
  ASSERT_EQ(OB_SUCCESS, miter1.from(input));
  ASSERT_EQ(OB_SUCCESS, miter2.from(input));
  ASSERT_EQ(OB_SUCCESS, iter1.open(miter1));
  ASSERT_EQ(OB_SUCCESS, iter2.open(miter2));
  check_iters(iter1, iter2);
}

}
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  printf("start running test\n");
  return RUN_ALL_TESTS();
}

