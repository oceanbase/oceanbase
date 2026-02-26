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
#include "lib/net/ob_addr.h"

using namespace oceanbase::common;

TEST(OB_ADDR, TEST1)
{
  ObAddr addr;
  EXPECT_EQ(addr.get_ipv4(), 0U);
  EXPECT_FALSE(addr.is_valid());

  addr.set_ip_addr("0.0.0.1", 1024);
  EXPECT_EQ(addr.get_ipv4(), 1U);
  EXPECT_TRUE(addr.is_valid());

  addr.set_ip_addr("1.0.0.0", 1024);
  EXPECT_EQ(addr.get_ipv4(), 1U << 24);
  EXPECT_TRUE(addr.is_valid());

  EXPECT_EQ(addr.parse_from_cstring("1.0.0.0:1234"), OB_SUCCESS);
  EXPECT_TRUE(addr.is_valid());
  EXPECT_EQ(addr.get_ipv4(), 1U << 24);
  EXPECT_EQ(addr.get_port(), 1234);

  EXPECT_EQ(addr.parse_from_cstring("1.0.0.1234:1234"), OB_INVALID_ARGUMENT);

  addr.set_ip_addr("0.0.0.1", 1);
  EXPECT_EQ(addr.get_ipv4(), 1U);
  ObAddr addr2;
  addr2.set_ip_addr("1.0.0.0", 1);
  EXPECT_EQ(addr2.get_ipv4(), 1U << 24);
  EXPECT_LT(addr, addr2);

  addr.set_ip_addr("0.0.0.1", 2);
  addr2.set_ip_addr("1.0.0.0", 1);
  EXPECT_LT(addr, addr2);

  addr.set_ip_addr("1.0.0.1", 1);
  addr2.set_ip_addr("1.0.0.1", 2);
  EXPECT_LT(addr, addr2);
  char *buf_null = NULL;
  int ret = addr.ip_port_to_string(buf_null, 10);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  char buf[64];
  ret = addr.ip_port_to_string(buf, 0);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  ret = addr.ip_port_to_string(buf, 5);
  ASSERT_EQ(OB_SIZE_OVERFLOW, ret);
}

TEST(OB_ADDR, IPV6_FORMAT_TEST)
{
  ObAddr addr;
  addr.set_ip_addr("1::", 1024);
  EXPECT_TRUE(addr.is_valid());
  char buf[64];
  int ret = addr.ip_port_to_string(buf, sizeof(buf));
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, memcmp("[1::]:1024", buf, strlen(buf)));

  addr.set_ip_addr("::1", 1024);
  EXPECT_TRUE(addr.is_valid());
  ret = addr.ip_port_to_string(buf, sizeof(buf));
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, memcmp("[::1]:1024", buf, strlen(buf)));

  addr.set_ip_addr("fd00:1:100a:1c60::a", 2882);
  EXPECT_TRUE(addr.is_valid());
  ret = addr.ip_port_to_string(buf, sizeof(buf));
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, memcmp("[fd00:1:100a:1c60::a]:2882", buf, strlen(buf)));

  addr.set_ip_addr("fd00:1:100a:1c60::a", 2882);
  EXPECT_TRUE(addr.is_valid());
  int32_t ret_len;
  ret = addr.addr_to_buffer(buf, sizeof(buf), ret_len);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, memcmp("[fd00:1:100a:1c60::a]:2882", buf, strlen(buf)));

  addr.set_ip_addr("fd00:1:100a:1c60::a", 2882);
  EXPECT_TRUE(addr.is_valid());
  int64_t ret_pos = addr.to_string(buf, sizeof(buf));
  ASSERT_EQ(strlen(buf), ret_pos);
  ASSERT_EQ(0, memcmp("\"[fd00:1:100a:1c60::a]:2882\"", buf, strlen(buf)));

  addr.set_ip_addr("fd00:1:100a:1c60::a", 2882);
  EXPECT_TRUE(addr.is_valid());
  bool ret_bool = addr.ip_to_string(buf, sizeof(buf));
  ASSERT_EQ(true, ret_bool);
  ASSERT_EQ(0, memcmp("fd00:1:100a:1c60::a", buf, strlen(buf)));

  addr.set_ip_addr("2001:db8:0:0:1:0:0:1", 2882);
  EXPECT_TRUE(addr.is_valid());
  ret_bool = addr.ip_to_string(buf, sizeof(buf));
  ASSERT_EQ(true, ret_bool);
  ASSERT_EQ(0, memcmp("2001:db8::1:0:0:1", buf, strlen(buf)));

  addr.set_ip_addr("2001:0:0:1:0:0:0:1", 2882);
  EXPECT_TRUE(addr.is_valid());
  ret_bool = addr.ip_to_string(buf, sizeof(buf));
  ASSERT_EQ(true, ret_bool);
  ASSERT_EQ(0, memcmp("2001:0:0:1::1", buf, strlen(buf)));
}

TEST(OB_ADDR, TEST_UNIX_PATH)
{
  ObAddr addr;
  char buffer[128];

  EXPECT_FALSE(addr.set_unix_addr(NULL));
  EXPECT_FALSE(addr.is_valid());

  char path0[] = "";
  EXPECT_TRUE(addr.set_unix_addr(path0));
  EXPECT_FALSE(addr.is_valid());
  addr.ip_to_string(buffer, sizeof(buffer));
  ASSERT_EQ(strcmp("unix:", buffer), 0);

  char path1[] = "/path/to/file";
  EXPECT_TRUE(addr.set_unix_addr(path1));
  EXPECT_TRUE(addr.is_valid());
  ASSERT_EQ(strcmp(path1, addr.get_unix_path()), 0);
  addr.ip_to_string(buffer, sizeof(buffer));
  ASSERT_EQ(strcmp("unix:/path/to/file", buffer), 0);

  char path2[] = "1234567890123456"; // strlen(path2) = 16
  EXPECT_FALSE(addr.set_unix_addr(path2));
}

int main(int argc, char *argv[])
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
