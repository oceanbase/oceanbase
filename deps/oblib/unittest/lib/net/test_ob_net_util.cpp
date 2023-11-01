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
#include "lib/net/ob_net_util.h"

namespace oceanbase
{
namespace obsys
{

TEST(TestWhiteList, ObNetUtil)
{
  ObString host_name_0("100.104.127.0/26");
  EXPECT_EQ(true, ObNetUtil::is_ip_match("100.104.127.44", host_name_0));

  ObString host_name("192.168.0.0/16");
  EXPECT_EQ(true, ObNetUtil::is_ip_match("192.168.1.1", host_name));
  EXPECT_EQ(true, ObNetUtil::is_ip_match("192.168.1.0", host_name));
  EXPECT_EQ(true, ObNetUtil::is_ip_match("192.168.1.255", host_name));
  EXPECT_EQ(false, ObNetUtil::is_ip_match("192.168.1.-1", host_name));
  EXPECT_EQ(false, ObNetUtil::is_ip_match("192.168.-1.1", host_name));
  EXPECT_EQ(false, ObNetUtil::is_ip_match("192.168.256.1", host_name));
  EXPECT_EQ(false, ObNetUtil::is_ip_match("192.168.1.256", host_name));
  EXPECT_EQ(false, ObNetUtil::is_ip_match("192.169.1.1", host_name));
  EXPECT_EQ(false, ObNetUtil::is_ip_match("192.0.1.1", host_name));
  EXPECT_EQ(false, ObNetUtil::is_ip_match("192.1", host_name));
  EXPECT_EQ(false, ObNetUtil::is_ip_match("hello", host_name));

  ObString host_name_ipv6("fe80:90fa:2017:ff00::/56");
  EXPECT_EQ(true, ObNetUtil::is_ip_match("fe80:90fa:2017:ff03::0a:02", host_name_ipv6));
  EXPECT_EQ(true, ObNetUtil::is_ip_match("fe80:90fa:2017:ff00:00e8:0074:0a:02", host_name_ipv6));
  EXPECT_EQ(false, ObNetUtil::is_ip_match("fe80:90fa:2017:fe00:00e8:0074:0a:02", host_name_ipv6));
  EXPECT_EQ(false, ObNetUtil::is_ip_match(":90fa:2017:fe00:00e8:0074:0a:02", host_name_ipv6));
  EXPECT_EQ(false, ObNetUtil::is_ip_match("fe80:90fa:2017:ff:", host_name_ipv6));
  EXPECT_EQ(false, ObNetUtil::is_ip_match("::", host_name_ipv6));
  EXPECT_EQ(false, ObNetUtil::is_ip_match("::1", host_name_ipv6));

  ObString host_name2("0.0.0.0/0");
  EXPECT_EQ(true, ObNetUtil::is_ip_match("192.168.1.1", host_name2));
  EXPECT_EQ(true, ObNetUtil::is_ip_match("192.168.1.0", host_name2));
  EXPECT_EQ(true, ObNetUtil::is_ip_match("192.168.1.255", host_name2));
  EXPECT_EQ(false, ObNetUtil::is_ip_match("192.168.1.-1", host_name2));
  EXPECT_EQ(false, ObNetUtil::is_ip_match("192.168.-1.1", host_name2));
  EXPECT_EQ(false, ObNetUtil::is_ip_match("192.168.256.1", host_name2));
  EXPECT_EQ(false, ObNetUtil::is_ip_match("192.168.1.256", host_name2));
  EXPECT_EQ(true, ObNetUtil::is_ip_match("192.169.1.1", host_name2));
  EXPECT_EQ(true, ObNetUtil::is_ip_match("192.0.1.1", host_name2));
  EXPECT_EQ(false, ObNetUtil::is_ip_match("192.1", host_name2));
  EXPECT_EQ(false, ObNetUtil::is_ip_match("hello", host_name2));

  ObString host_name2_ipv6("::/0");
  EXPECT_EQ(true, ObNetUtil::is_ip_match("fe80:90fa:2017:ff03::0a:02", host_name2_ipv6));
  EXPECT_EQ(true, ObNetUtil::is_ip_match("fe80:90fa:2017:ff03::", host_name2_ipv6));
  EXPECT_EQ(true, ObNetUtil::is_ip_match("::", host_name2_ipv6));
  EXPECT_EQ(true, ObNetUtil::is_ip_match("::1", host_name2_ipv6));
  EXPECT_EQ(false, ObNetUtil::is_ip_match("192.168.1.1", host_name2_ipv6));
  EXPECT_EQ(false, ObNetUtil::is_ip_match(":2017:ff03::", host_name2_ipv6));

  ObString host_name3("192.168.1.1/32");
  EXPECT_EQ(true, ObNetUtil::is_ip_match("192.168.1.1", host_name3));
  EXPECT_EQ(false, ObNetUtil::is_ip_match("192.168.1.0", host_name3));
  EXPECT_EQ(false, ObNetUtil::is_ip_match("192.168.1.255", host_name3));
  EXPECT_EQ(false, ObNetUtil::is_ip_match("192.168.1.-1", host_name3));
  EXPECT_EQ(false, ObNetUtil::is_ip_match("192.168.-1.1", host_name3));
  EXPECT_EQ(false, ObNetUtil::is_ip_match("192.168.256.1", host_name3));
  EXPECT_EQ(false, ObNetUtil::is_ip_match("192.168.1.256", host_name3));
  EXPECT_EQ(false, ObNetUtil::is_ip_match("192.169.1.1", host_name3));
  EXPECT_EQ(false, ObNetUtil::is_ip_match("192.0.1.1", host_name3));
  EXPECT_EQ(false, ObNetUtil::is_ip_match("192.1", host_name3));
  EXPECT_EQ(false, ObNetUtil::is_ip_match("hello", host_name3));

  ObString host_name4("192.168.11.0/255.255.255.0");
  EXPECT_EQ(true, ObNetUtil::is_ip_match("192.168.11.1", host_name4));
  EXPECT_EQ(true, ObNetUtil::is_ip_match("192.168.11.0", host_name4));
  EXPECT_EQ(true, ObNetUtil::is_ip_match("192.168.11.255", host_name4));
  EXPECT_EQ(false, ObNetUtil::is_ip_match("192.168.11.-1", host_name4));
  EXPECT_EQ(false, ObNetUtil::is_ip_match("192.168.11.256", host_name4));
  EXPECT_EQ(false, ObNetUtil::is_ip_match("192.168.1.1", host_name4));
  EXPECT_EQ(false, ObNetUtil::is_ip_match("192.168.10.1", host_name4));
  EXPECT_EQ(false, ObNetUtil::is_ip_match("192.0.1.1", host_name4));
  EXPECT_EQ(false, ObNetUtil::is_ip_match("192.1", host_name4));
  EXPECT_EQ(false, ObNetUtil::is_ip_match("hello", host_name4));

  ObString host_name4_ipv6("fe80:90fa:2017:ff80::/ffff:ffff:ffff:ffff:0:0:0:0");
  EXPECT_EQ(true, ObNetUtil::is_ip_match("fe80:90fa:2017:ff80:00e8:ff74:0a:02", host_name4_ipv6));
  EXPECT_EQ(true, ObNetUtil::is_ip_match("fe80:90fa:2017:ff80::", host_name4_ipv6));
  EXPECT_EQ(true, ObNetUtil::is_ip_match("fe80:90fa:2017:ff80:e8::0a:02", host_name4_ipv6));
  EXPECT_EQ(false, ObNetUtil::is_ip_match("fe80:90fa:2017:ff00:00e8:0074:0a:02", host_name4_ipv6));
  EXPECT_EQ(false, ObNetUtil::is_ip_match("fe80:90fa:2017:ff00::", host_name4_ipv6));
  EXPECT_EQ(false, ObNetUtil::is_ip_match("::", host_name4_ipv6));
  EXPECT_EQ(false, ObNetUtil::is_ip_match("::1", host_name4_ipv6));

  ObString host_name5("0.0.0.0/0.0.0.0");
  EXPECT_EQ(true, ObNetUtil::is_ip_match("192.168.11.1", host_name5));
  EXPECT_EQ(true, ObNetUtil::is_ip_match("192.168.11.0", host_name5));
  EXPECT_EQ(true, ObNetUtil::is_ip_match("192.168.11.255", host_name5));
  EXPECT_EQ(false, ObNetUtil::is_ip_match("192.168.11.-1", host_name5));
  EXPECT_EQ(false, ObNetUtil::is_ip_match("192.168.11.256", host_name5));
  EXPECT_EQ(true, ObNetUtil::is_ip_match("192.168.1.1", host_name5));
  EXPECT_EQ(true, ObNetUtil::is_ip_match("192.168.10.1", host_name5));
  EXPECT_EQ(true, ObNetUtil::is_ip_match("192.0.1.1", host_name5));
  EXPECT_EQ(false, ObNetUtil::is_ip_match("192.1", host_name5));
  EXPECT_EQ(false, ObNetUtil::is_ip_match("hello", host_name5));

  ObString host_name5_ipv6("::/::");
  EXPECT_EQ(true, ObNetUtil::is_ip_match("fe80:90fa:2017:ff00:00e8:0074:0a:02", host_name5_ipv6));
  EXPECT_EQ(true, ObNetUtil::is_ip_match("::", host_name5_ipv6));
  EXPECT_EQ(true, ObNetUtil::is_ip_match("::1", host_name5_ipv6));
  EXPECT_EQ(true, ObNetUtil::is_ip_match("ff:ff:ff:ff:ff:ff:ff:ff", host_name5_ipv6));
  EXPECT_EQ(false, ObNetUtil::is_ip_match("90fa:2017:ff00:00e8:0074:0a:02", host_name5_ipv6));
  EXPECT_EQ(false, ObNetUtil::is_ip_match("fe80:90fa:2017:ff00:00e8:0074:0a:-1", host_name5_ipv6));
  EXPECT_EQ(false, ObNetUtil::is_ip_match("fe80:90fa:2017:ff00:00e8:0074:0a:g1", host_name5_ipv6));

  ObString host_name6("192.168.11.1/255.255.255.255");
  EXPECT_EQ(true, ObNetUtil::is_ip_match("192.168.11.1", host_name6));
  EXPECT_EQ(false, ObNetUtil::is_ip_match("192.168.11.0", host_name6));
  EXPECT_EQ(false, ObNetUtil::is_ip_match("192.168.11.255", host_name6));
  EXPECT_EQ(false, ObNetUtil::is_ip_match("192.168.11.-1", host_name6));
  EXPECT_EQ(false, ObNetUtil::is_ip_match("192.168.11.256", host_name6));
  EXPECT_EQ(false, ObNetUtil::is_ip_match("192.168.1.1", host_name6));
  EXPECT_EQ(false, ObNetUtil::is_ip_match("192.168.10.1", host_name6));
  EXPECT_EQ(false, ObNetUtil::is_ip_match("192.0.1.1", host_name6));
  EXPECT_EQ(false, ObNetUtil::is_ip_match("192.1", host_name6));
  EXPECT_EQ(false, ObNetUtil::is_ip_match("hello", host_name6));

  ObString host_name7("192.168.11.1/33");
  EXPECT_EQ(false, ObNetUtil::is_ip_match("192.168.11.1", host_name7));
  ObString host_name8("192.168.11.1/-1");
  EXPECT_EQ(false, ObNetUtil::is_ip_match("192.168.11.1", host_name8));
  ObString host_name9("192.168.11.1/");
  EXPECT_EQ(false, ObNetUtil::is_ip_match("192.168.11.1", host_name9));

  ObString host_name7_ipv6("fe80:90fa:2017:ff00::/129");
  EXPECT_EQ(false, ObNetUtil::is_ip_match("fe80:90fa:2017:ff03::0a:02", host_name7_ipv6));
  ObString host_name8_ipv6("fe80:90fa:2017:ff00::/-1");
  EXPECT_EQ(false, ObNetUtil::is_ip_match("fe80:90fa:2017:ff03::0a:02", host_name8_ipv6));
  ObString host_name9_ipv6("fe80:90fa:2017:ff00::/");
  EXPECT_EQ(false, ObNetUtil::is_ip_match("fe80:90fa:2017:ff03::0a:02", host_name9_ipv6));

  ObString host_name10("192.168.11.1");
  EXPECT_EQ(true, ObNetUtil::is_ip_match("192.168.11.1", host_name10));
  EXPECT_EQ(false, ObNetUtil::is_ip_match("192.168.11.0", host_name10));
  EXPECT_EQ(false, ObNetUtil::is_ip_match("192.168.11.255", host_name10));
  EXPECT_EQ(false, ObNetUtil::is_ip_match("192.168.11.-1", host_name10));
  EXPECT_EQ(false, ObNetUtil::is_ip_match("192.168.11.256", host_name10));
  EXPECT_EQ(false, ObNetUtil::is_ip_match("192.168.1.1", host_name10));
  EXPECT_EQ(false, ObNetUtil::is_ip_match("192.168.10.1", host_name10));
  EXPECT_EQ(false, ObNetUtil::is_ip_match("192.0.1.1", host_name10));
  EXPECT_EQ(false, ObNetUtil::is_ip_match("192.1", host_name10));
  EXPECT_EQ(false, ObNetUtil::is_ip_match("hello", host_name10));

  ObString host_name10_ipv6("2017::a:2");
  EXPECT_EQ(true, ObNetUtil::is_ip_match("2017:0:0:0:0:0:a:2", host_name10_ipv6));
  EXPECT_EQ(true, ObNetUtil::is_ip_match("2017::0a:02", host_name10_ipv6));
  EXPECT_EQ(false, ObNetUtil::is_ip_match("2017:2::0a:02", host_name10_ipv6));
  EXPECT_EQ(false, ObNetUtil::is_ip_match("::", host_name10_ipv6));
  EXPECT_EQ(false, ObNetUtil::is_ip_match("::1", host_name10_ipv6));

  ObString host_name11("192.168.11.%");
  EXPECT_EQ(true, ObNetUtil::is_wild_match("192.168.11.0", host_name11));
  EXPECT_EQ(true, ObNetUtil::is_wild_match("192.168.11.255", host_name11));
  EXPECT_EQ(true, ObNetUtil::is_wild_match("192.168.11.-1", host_name11));
  EXPECT_EQ(true, ObNetUtil::is_wild_match("192.168.11.256", host_name11));
  EXPECT_EQ(false, ObNetUtil::is_wild_match("192.168.1.1", host_name11));
  EXPECT_EQ(false, ObNetUtil::is_wild_match("192.168.10.1", host_name11));
  EXPECT_EQ(false, ObNetUtil::is_wild_match("192.0.1.1", host_name11));

  ObString host_name11_ipv6("fe80:fe20:ffff:%");
  EXPECT_EQ(true, ObNetUtil::is_wild_match("fe80:fe20:ffff:2014:0506:02:56:80", host_name11_ipv6));
  EXPECT_EQ(true, ObNetUtil::is_wild_match("fe80:fe20:ffff::", host_name11_ipv6));
  EXPECT_EQ(true, ObNetUtil::is_wild_match("fe80:fe20:ffff:2019::", host_name11_ipv6));
  EXPECT_EQ(false, ObNetUtil::is_wild_match("fe80:fe20:fffe::", host_name11_ipv6));
  EXPECT_EQ(false, ObNetUtil::is_wild_match("fe80:fe20:8fff::2084:92:23:78:ff30", host_name11_ipv6));

  ObString host_name12("192.168.11._");
  EXPECT_EQ(true, ObNetUtil::is_wild_match("192.168.11.0", host_name12));
  EXPECT_EQ(false, ObNetUtil::is_wild_match("192.168.11.255", host_name12));
  EXPECT_EQ(false, ObNetUtil::is_wild_match("192.168.11.-1", host_name12));
  EXPECT_EQ(false, ObNetUtil::is_wild_match("192.168.11.256", host_name12));
  EXPECT_EQ(false, ObNetUtil::is_wild_match("192.168.1.1", host_name12));
  EXPECT_EQ(false, ObNetUtil::is_wild_match("192.168.10.1", host_name12));
  EXPECT_EQ(false, ObNetUtil::is_wild_match("192.0.1.1", host_name12));

  int64_t value = 0;
  EXPECT_EQ(OB_SUCCESS, ObNetUtil::get_int_value("12", value));
  EXPECT_EQ(12, value);
  EXPECT_EQ(OB_SUCCESS, ObNetUtil::get_int_value("-12", value));
  EXPECT_EQ(-12, value);
  EXPECT_EQ(OB_SUCCESS, ObNetUtil::get_int_value("0", value));
  EXPECT_EQ(0, value);
  EXPECT_EQ(OB_SUCCESS, ObNetUtil::get_int_value("65536", value));
  EXPECT_EQ(65536, value);
  EXPECT_EQ(OB_SUCCESS, ObNetUtil::get_int_value("-65536", value));
  EXPECT_EQ(-65536, value);
  EXPECT_EQ(OB_INVALID_DATA, ObNetUtil::get_int_value("12sds", value));
  EXPECT_EQ(OB_INVALID_DATA, ObNetUtil::get_int_value("12 sddf", value));
  EXPECT_EQ(OB_INVALID_DATA, ObNetUtil::get_int_value("++12", value));
  EXPECT_EQ(OB_INVALID_DATA, ObNetUtil::get_int_value("--12", value));

  ObString ip_white_list1("");
  ASSERT_EQ(false, ObNetUtil::is_in_white_list("192.0.1.1", ip_white_list1));
  ASSERT_EQ(false, ObNetUtil::is_in_white_list("%", ip_white_list1));

  ObString ip_white_list2;
  ASSERT_EQ(false, ObNetUtil::is_in_white_list("192.0.1.1", ip_white_list2));

  ObString ip_white_list3("%");
  ASSERT_EQ(true, ObNetUtil::is_in_white_list("192.0.1.1", ip_white_list3));
  ASSERT_EQ(false, ObNetUtil::is_in_white_list("192.0.1.-1", ip_white_list3));
  ASSERT_EQ(false, ObNetUtil::is_in_white_list("hello", ip_white_list3));
  ASSERT_EQ(false, ObNetUtil::is_in_white_list("", ip_white_list3));

  ObString ip_white_list4("192.0.1.0/24");
  ASSERT_EQ(true, ObNetUtil::is_in_white_list("192.0.1.1", ip_white_list4));
  ASSERT_EQ(false, ObNetUtil::is_in_white_list("192.0.1.-1", ip_white_list4));
  ASSERT_EQ(false, ObNetUtil::is_in_white_list("hello", ip_white_list4));
  ASSERT_EQ(false, ObNetUtil::is_in_white_list("", ip_white_list4));

  ObString ip_white_list5("192.0.1.0/24, %");
  ASSERT_EQ(true, ObNetUtil::is_in_white_list("192.0.1.1", ip_white_list5));
  ASSERT_EQ(false, ObNetUtil::is_in_white_list("192.0.1.-1", ip_white_list5));
  ASSERT_EQ(false, ObNetUtil::is_in_white_list("hello", ip_white_list5));
  ASSERT_EQ(false, ObNetUtil::is_in_white_list("", ip_white_list5));

  ObString ip_white_list6("192.0.1.0/24,%");
  ASSERT_EQ(true, ObNetUtil::is_in_white_list("192.0.1.1", ip_white_list6));
  ASSERT_EQ(false, ObNetUtil::is_in_white_list("192.0.1.-1", ip_white_list6));
  ASSERT_EQ(false, ObNetUtil::is_in_white_list("hello", ip_white_list6));
  ASSERT_EQ(false, ObNetUtil::is_in_white_list("", ip_white_list6));

  ObString ip_white_list7(",192.0.1,192.0.1.0/24");
  ASSERT_EQ(true, ObNetUtil::is_in_white_list("192.0.1.1", ip_white_list7));
  ASSERT_EQ(false, ObNetUtil::is_in_white_list("192.0.1.-1", ip_white_list7));
  ASSERT_EQ(false, ObNetUtil::is_in_white_list("hello", ip_white_list7));
  ASSERT_EQ(false, ObNetUtil::is_in_white_list("", ip_white_list7));

  ObString ip_white_list8("10.125.224.0/255.255.252.0");
  ASSERT_EQ(true, ObNetUtil::is_in_white_list("10.125.224.15", ip_white_list8));
  ASSERT_EQ(true, ObNetUtil::is_in_white_list("10.125.224.5", ip_white_list8));
  ASSERT_EQ(false, ObNetUtil::is_in_white_list("192.0.1.-1", ip_white_list8));

  ObString ip_white_list9("10.125.224.0/22");
  ASSERT_EQ(true, ObNetUtil::is_in_white_list("10.125.224.15", ip_white_list9));
  ASSERT_EQ(true, ObNetUtil::is_in_white_list("10.125.224.5", ip_white_list9));
  ASSERT_EQ(false, ObNetUtil::is_in_white_list("192.0.1.-1", ip_white_list9));

  ObString ip_white_list10("255.255.224.0/22");
  ASSERT_EQ(true, ObNetUtil::is_in_white_list("255.255.224.15", ip_white_list10));
  ASSERT_EQ(true, ObNetUtil::is_in_white_list("255.255.224.5", ip_white_list10));
  ASSERT_EQ(false, ObNetUtil::is_in_white_list("255.255.255.15", ip_white_list10));

  ObString ip_white_list11("255.255.224.14,,255.255.224.15");
  ASSERT_EQ(false, ObNetUtil::is_in_white_list("255.255.224.13", ip_white_list11));
  ASSERT_EQ(true, ObNetUtil::is_in_white_list("255.255.224.14", ip_white_list11));
  ASSERT_EQ(true, ObNetUtil::is_in_white_list("255.255.224.15", ip_white_list11));

  ObString ip_white_list12("255.255.224.15,255.255.224.14,255.255.224.13,255.255.224.12,255.255.224.11");
  ASSERT_EQ(true, ObNetUtil::is_in_white_list("255.255.224.11", ip_white_list12));
  ASSERT_EQ(true, ObNetUtil::is_in_white_list("255.255.224.11", ip_white_list12));
  ASSERT_EQ(true, ObNetUtil::is_in_white_list("255.255.224.12", ip_white_list12));
  ASSERT_EQ(true, ObNetUtil::is_in_white_list("255.255.224.13", ip_white_list12));
  ASSERT_EQ(true, ObNetUtil::is_in_white_list("255.255.224.14", ip_white_list12));
  ASSERT_EQ(true, ObNetUtil::is_in_white_list("255.255.224.15", ip_white_list12));
  ASSERT_EQ(false, ObNetUtil::is_in_white_list("255.255.224.16", ip_white_list12));

  ObString ip_white_list13(" 255.255.224.15, 255.255.224.14, 255.255.224.13, 255.255.224.12");
  EXPECT_EQ(true, ObNetUtil::is_in_white_list("255.255.224.15", ip_white_list13));
  EXPECT_EQ(true, ObNetUtil::is_in_white_list("255.255.224.14", ip_white_list13));
  EXPECT_EQ(true, ObNetUtil::is_in_white_list("255.255.224.13", ip_white_list13));
  EXPECT_EQ(true, ObNetUtil::is_in_white_list("255.255.224.12", ip_white_list13));

  ObString ip_white_list14(" 255.255.224.15/32 , 255.255.224.14/32 , 255.255.224.13/32 , 255.255.224.12/32");
  EXPECT_EQ(true, ObNetUtil::is_in_white_list("255.255.224.15", ip_white_list14));
  EXPECT_EQ(true, ObNetUtil::is_in_white_list("255.255.224.14", ip_white_list14));
  EXPECT_EQ(true, ObNetUtil::is_in_white_list("255.255.224.13", ip_white_list14));
  EXPECT_EQ(true, ObNetUtil::is_in_white_list("255.255.224.12", ip_white_list14));

  ObString ip_white_list15("10.244.32.%,10.250.37.18,192.168.11._");
  EXPECT_EQ(true, ObNetUtil::is_in_white_list("10.250.37.18", ip_white_list15));
  EXPECT_EQ(true, ObNetUtil::is_in_white_list("192.168.11.1", ip_white_list15));
  EXPECT_EQ(false, ObNetUtil::is_in_white_list("192.168.12.1", ip_white_list15));
  EXPECT_EQ(true, ObNetUtil::is_in_white_list("10.244.32.222", ip_white_list15));
  EXPECT_EQ(false, ObNetUtil::is_in_white_list("10.250.49.24", ip_white_list15));
  EXPECT_EQ(false, ObNetUtil::is_in_white_list("10.250.49.251", ip_white_list15));

  ObString ip_white_list_mixed("10.244.32.%,10.250.37.18, 192.0.1.0/24, 192.168.11._, fe80:90fa:2017:ff00::/56, "
                               "fe80:90fa:2017:ee00:00e8:0074:0a:02");
  EXPECT_EQ(true, ObNetUtil::is_in_white_list("10.250.37.18", ip_white_list_mixed));
  EXPECT_EQ(true, ObNetUtil::is_in_white_list("10.244.32.222", ip_white_list_mixed));
  EXPECT_EQ(false, ObNetUtil::is_in_white_list("10.250.49.24", ip_white_list_mixed));
  EXPECT_EQ(false, ObNetUtil::is_in_white_list("10.250.49.251", ip_white_list_mixed));
  EXPECT_EQ(true, ObNetUtil::is_in_white_list("fe80:90fa:2017:ff00:00e8:0074:0a:02", ip_white_list_mixed));
  EXPECT_EQ(false, ObNetUtil::is_in_white_list("fe80:90ee:2017:ff00:00e8:0074:0a:02", ip_white_list_mixed));
  EXPECT_EQ(true, ObNetUtil::is_in_white_list("fe80:90fa:2017:ee00:00e8:0074:0a:02", ip_white_list_mixed));
  EXPECT_EQ(false, ObNetUtil::is_in_white_list("fe80:90fa:2017:ee00:00e8:0074:0a:03", ip_white_list_mixed));
  EXPECT_EQ(true, ObNetUtil::is_in_white_list("192.168.11.1", ip_white_list_mixed));
}
}  // namespace obsys
}  // end of namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
