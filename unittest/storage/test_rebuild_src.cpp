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

#include "share/rebuild_tablet/ob_rebuild_tablet_location.h"
#include <gtest/gtest.h>

using namespace oceanbase;
using namespace common;
using namespace share;


TEST(ObRebuildTabletLocationUtil, set_location)
{
  int ret = OB_SUCCESS;
  ObRebuildTabletLocation location;
  const char *rebuild_src_server_ip = "server_addr:127.0.0.1:2800";
  ASSERT_EQ(OB_SUCCESS, location.resolve_location(rebuild_src_server_ip));
  LOG_INFO("location is", K(location));
  location.reset();

  const char *rebuild_src_region = "region:shanghai";
  ASSERT_EQ(OB_INVALID_ERROR, location.resolve_location(rebuild_src_region));
  LOG_INFO("location is", K(location));
  location.reset();

  const char *rebuild_src_server_id = "server_id:1";
  ASSERT_EQ(OB_INVALID_ERROR, location.resolve_location(rebuild_src_server_id));
  LOG_INFO("location is", K(location));
  location.reset();

  const char *rebuild_src_server_ip_prfix= "server_addr123:127.0.0.1:2800";
  ASSERT_EQ(OB_INVALID_ERROR, location.resolve_location(rebuild_src_server_id));
  LOG_INFO("location is", K(location));
  location.reset();
}

TEST(ObRebuildTabletLocationUtil, encode_and_decode)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObRebuildTabletLocation location;
  const char *rebuild_src_server_ip = "server_addr:127.0.0.1:2800";
  ASSERT_EQ(OB_SUCCESS, location.resolve_location(rebuild_src_server_ip));
  LOG_INFO("location is", K(location));

  const int64_t buf_len = 1024 * 1024;
  int64_t ser_pos = 0;
  char buf[buf_len] = "\0";
  ASSERT_EQ(OB_SUCCESS, location.serialize(buf, buf_len, ser_pos));

  COMMON_LOG(INFO, "serialize size", K(ser_pos));

  ObRebuildTabletLocation location1;
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, location1.deserialize(buf, ser_pos, pos));
  LOG_INFO("location is", K(location1));
}

TEST(ObRebuildTabletLocationUtil, encode_and_decode_with_space)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObRebuildTabletLocation location;
  const char *rebuild_src_server_ip = "  server_addr    :          127.0.0.1:2800      ";
  ASSERT_EQ(OB_SUCCESS, location.resolve_location(rebuild_src_server_ip));
  LOG_INFO("location is", K(location));

  const int64_t buf_len = 1024 * 1024;
  int64_t ser_pos = 0;
  char buf[buf_len] = "\0";
  ASSERT_EQ(OB_SUCCESS, location.serialize(buf, buf_len, ser_pos));

  COMMON_LOG(INFO, "serialize size", K(ser_pos));

  ObRebuildTabletLocation location1;
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, location1.deserialize(buf, ser_pos, pos));
  LOG_INFO("location is", K(location1));
}


int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  system("rm -rf test_rebuild_src.log");
  OB_LOGGER.set_file_name("test_rebuild_src.log");

  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
