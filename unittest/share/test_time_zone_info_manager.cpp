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

#define USING_LOG_PREFIX SHARE
#include "share/ob_time_zone_info_manager.h"
#include "lib/container/ob_array_wrap.h"
#include "sql/session/ob_sql_session_info.h"
#include "lib/timezone/ob_time_convert.h"
#include <gtest/gtest.h>
namespace oceanbase
{
namespace common
{
TEST(test_time_zone_info_manager, test_time_zone_key)
{
  ObTimeZoneKey tz_key1(ObString("Africa/Abidjan"));
  ObTimeZoneKey tz_key2(ObString("Africa/Abidjan"));
  ObTimeZoneKey tz_key3(ObString("AFRICA/ABIDJAN"));
  ObTimeZoneKey tz_key4(ObString("Africa/Abidjam"));
  EXPECT_EQ(tz_key1, tz_key2);
  EXPECT_EQ(tz_key1, tz_key3);
  EXPECT_EQ(false, tz_key3 == tz_key4);

  EXPECT_EQ(tz_key1.hash(0), tz_key2.hash(0));
  EXPECT_EQ(tz_key1.hash(0), tz_key3.hash(0));

}

void test_str_to_ob_time(const ObString &str, const ObString &expect_tz_name, const ObString &expect_tz_abbr)
{
  ObTime ob_time;
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::str_to_ob_time_with_date(str, ob_time));
  EXPECT_EQ(expect_tz_name, ObString(strlen(ob_time.tz_name_), ob_time.tz_name_));
  EXPECT_EQ(expect_tz_abbr, ObString(strlen(ob_time.tzd_abbr_), ob_time.tzd_abbr_));
  LOG_INFO("test result", K(str), K(ob_time));
}

TEST(test_time_zone_info_manager, str_to_digit_with_date)
{
  ObTime ob_time;
  //Test OB current parsing datetime str method
  test_str_to_ob_time("2000-01-01 00:00:00abc", "", "");
  test_str_to_ob_time("2000-01-01 00:00:00  ", "","");
  test_str_to_ob_time("2000-01-01 00:00:00 abc", "abc", "");
  test_str_to_ob_time("2000-01-01 00:00:00.11111 abc", "abc", "");
  test_str_to_ob_time("2000-01-01 00:00:00abc1111", "", "");
  test_str_to_ob_time("2000-01-01 00:00:00abc1111def222","","");

  //Test the parsing result of datetime string containing time zone and abbr
  //1. tz and abbr can only exist in the 6th or 7th element of delims.
  //2. And it can only exist in elements that start with space.
  test_str_to_ob_time("2000-01-01 00:00:00 Asia/Shanghai", "asia/shanghai", "");
  test_str_to_ob_time("2000-01-01 00:00:00 Asia/Shanghai CDT", "asia/shanghai", "CDT");
  test_str_to_ob_time("2000-01-01 00:00:00.11111 Asia/Shanghai CDT", "asia/shanghai", "CDT");
  test_str_to_ob_time("15-12-31 23:59:59.000000", "", "");
}


TEST(test_time_zone_info_manager, str_to_digit_without_date)
{
  int16_t scale = 0;
  ObTime ob_time1(DT_TYPE_TIME);
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::str_to_ob_time_without_date("10:11:12", ob_time1, &scale));
  int64_t value = ObTimeConverter::ob_time_to_time(ob_time1);
  LOG_INFO("str to ob_time", K(ob_time1), K(value));
  EXPECT_EQ(OB_SUCCESS, ObTimeConverter::time_overflow_trunc(value));
}

}
}

int main(int argc, char **argv)
{
  system("rm -rf test_time_zone_info_manager.log");
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_time_zone_info_manager.log", true);
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
