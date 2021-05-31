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
#include "lib/utility/ob_print_utils.h"

using namespace oceanbase::common;

TEST(print_utility, hex_print)
{
  const int64_t data_size = 10;
  char data[10];
  for (int64_t i = 0; i < 10; ++i) {
    data[i] = (char)(random() % 255);
  }
  char buff_h[21];
  char buff_p[21];
  int64_t pos = 0;
  int ret = hex_print(data, data_size, buff_h, 20, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(buff_h[18], '\0');
  ASSERT_EQ(strlen(buff_h), 18);
  char* bf = buff_p;
  int64_t len = 0;
  for (int64_t i = 0; i < 10; ++i) {
    len = snprintf(bf, 20 - i * 2, "%02X", (unsigned char)data[i]);
    bf += len;
  }
  ASSERT_EQ(buff_p[19], '\0');
  pos = 0;
  ret = hex_print(data, data_size, buff_h, 21, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(buff_h[20], '\0');
  bf = buff_p;
  for (int64_t i = 0; i < 10; ++i) {
    len = snprintf(bf, 21 - i * 2, "%02X", (unsigned char)data[i]);
    bf += len;
  }
  ASSERT_EQ(strcasecmp(buff_h, buff_p), 0);

  pos = 0;
  ret = hex_print(NULL, data_size, buff_h, 21, pos);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  pos = 0;
  ret = hex_print(data, -1, buff_h, 21, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(buff_h[0], '\0');
}

int main(int argc, char** argv)
{
  system("rm -rf test_print_utility.log");

  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_print_utility.log", true);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
