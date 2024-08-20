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
#include "lib/allocator/ob_malloc.h"
#include "lib/checksum/ob_crc64.h"
#include "lib/checksum/ob_parity_check.h"
#include <gtest/gtest.h>
#include <random>

namespace oceanbase
{
namespace unittest
{
using namespace common;

TEST(TestLogChecksum, test_log_checksum)
{
  int64_t buffer_size = 2 * 1024 * 1024l;
  char *buffer = NULL;
  buffer = (char *)ob_malloc(buffer_size, ObModIds::TEST);
  // fill buffer by random string
  std::mt19937 generator{std::random_device{}()};
  std::uniform_int_distribution<int> distribution{'a', 'z'};
  std::string rand_str(buffer_size, '\0');
  for(auto& dis: rand_str) {
    dis = distribution(generator);
  }
  memcpy(buffer, rand_str.c_str(), buffer_size);

  // test ob_crc64()
  for (int i = 0; i < 1000; ++i) {
    int64_t checksum01 = 0;
    int64_t checksum02 = 0;
    int64_t data_len = ObRandom::rand(10, buffer_size);
    EXPECT_TRUE(data_len >= 10);
    int64_t data_len_part_1 = ObRandom::rand(1, data_len - 1);;
    EXPECT_TRUE(data_len_part_1 > 0);
    int64_t data_len_part_2 = data_len - data_len_part_1;
    EXPECT_TRUE(data_len_part_1 > 0);
    checksum01 = static_cast<int64_t>(ob_crc64(buffer, data_len));
    checksum02 = static_cast<int64_t>(ob_crc64(buffer, data_len_part_1));
    checksum02 = static_cast<int64_t>(ob_crc64(checksum02, buffer + data_len_part_1, data_len_part_2));
    EXPECT_EQ(checksum01, checksum02);
    PALF_LOG(INFO, "test round ", K(i), K(data_len), K(data_len_part_1), K(data_len_part_2), K(checksum01), K(checksum02));
  }

  // test ob_crc32()
  // for (int i = 0; i < 1000; ++i) {
  //   int32_t checksum01 = 0;
  //   int32_t checksum02 = 0;
  //   int64_t data_len = ObRandom::rand(10, buffer_size);
  //   EXPECT_TRUE(data_len >= 10);
  //   int64_t data_len_part_1 = ObRandom::rand(1, data_len - 1);;
  //   EXPECT_TRUE(data_len_part_1 > 0);
  //   int64_t data_len_part_2 = data_len - data_len_part_1;
  //   EXPECT_TRUE(data_len_part_1 > 0);
  //   checksum01 = static_cast<int32_t>(ob_crc32(buffer, data_len));
  //   checksum02 = static_cast<int32_t>(ob_crc32(buffer, data_len_part_1));
  //   checksum02 = static_cast<int32_t>(ob_crc32(checksum02, buffer + data_len_part_1, data_len_part_2));
  //   EXPECT_EQ(checksum01, checksum02);
  //   PALF_LOG(INFO, "test round ", K(i), K(data_len), K(data_len_part_1), K(data_len_part_2), K(checksum01), K(checksum02));
  // }

  // test parity_check()
  uint64_t v1 = (1 << 20) + (1 << 10) + 1;
  uint64_t v2 = (1 << 11) + (1 << 5);
  EXPECT_TRUE(parity_check(v1));
  EXPECT_FALSE(parity_check(v2));
}
}
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_log_checksum.log", true);
  OB_LOGGER.set_log_level("INFO");
  PALF_LOG(INFO, "begin unittest::test_log_checksum");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
