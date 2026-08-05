/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include "share/rebuild_tablet/ob_rebuild_tablet_location.h"
#include "storage/high_availability/ob_ls_transfer_info.h"
#include "storage/high_availability/ob_storage_ha_struct.h"
#include "storage/high_availability/ob_tablet_ha_status.h"
#include <gtest/gtest.h>

using namespace oceanbase;
using namespace common;
using namespace share;
using namespace storage;

namespace
{
int encode_fixed_array_with_count(
    const int64_t count,
    char *buf,
    const int64_t buf_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  const int64_t version = 1;
  const int64_t payload_len = serialization::encoded_length_vi64(count);
  if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, version))) {
  } else if (OB_FAIL(serialization::encode_fixed_bytes_i64(buf, buf_len, pos, payload_len))) {
  } else if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, count))) {
  }
  return ret;
}

template <typename TabletIDArray>
void expect_invalid_count(const int64_t count)
{
  char buf[64] = {'\0'};
  int64_t serialize_pos = 0;
  ASSERT_EQ(OB_SUCCESS, encode_fixed_array_with_count(count, buf, sizeof(buf), serialize_pos));

  TabletIDArray tablet_id_array;
  int64_t deserialize_pos = 0;
  EXPECT_EQ(OB_DESERIALIZE_ERROR,
            tablet_id_array.deserialize(buf, serialize_pos, deserialize_pos));
  EXPECT_TRUE(tablet_id_array.empty());
}

template <typename TabletIDArray>
void expect_round_trip(const int64_t count)
{
  TabletIDArray src;
  for (int64_t i = 0; i < count; ++i) {
    ASSERT_EQ(OB_SUCCESS, src.push_back(ObTabletID(i + 1)));
  }

  char buf[64 * 1024] = {'\0'};
  int64_t serialize_pos = 0;
  ASSERT_EQ(OB_SUCCESS, src.serialize(buf, sizeof(buf), serialize_pos));

  TabletIDArray dst;
  int64_t deserialize_pos = 0;
  ASSERT_EQ(OB_SUCCESS, dst.deserialize(buf, serialize_pos, deserialize_pos));
  ASSERT_EQ(count, dst.count());
  for (int64_t i = 0; i < count; ++i) {
    EXPECT_EQ(src.at(i), dst.at(i));
  }
}

void expect_ha_status_round_trip(const ObTabletExpectedStatus::STATUS expected_status)
{
  ObTabletHAStatus src;
  ASSERT_EQ(OB_SUCCESS, src.set_restore_status(ObTabletRestoreStatus::FULL));
  ASSERT_EQ(OB_SUCCESS, src.set_data_status(ObTabletDataStatus::COMPLETE));
  ASSERT_EQ(OB_SUCCESS, src.set_expected_status(expected_status));
  ASSERT_TRUE(src.is_valid());

  char buf[64] = {'\0'};
  int64_t serialize_pos = 0;
  ASSERT_EQ(OB_SUCCESS, src.serialize(buf, sizeof(buf), serialize_pos));

  ObTabletHAStatus dst;
  int64_t deserialize_pos = 0;
  ASSERT_EQ(OB_SUCCESS, dst.deserialize(buf, serialize_pos, deserialize_pos));
  ASSERT_TRUE(dst.is_valid());
  ObTabletExpectedStatus::STATUS actual_status = ObTabletExpectedStatus::EXPECTED_STATUS_MAX;
  ASSERT_EQ(OB_SUCCESS, dst.get_expected_status(actual_status));
  EXPECT_EQ(expected_status, actual_status);
}
} // namespace


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

TEST(ObFixedTabletIDArray, reject_invalid_serialized_count)
{
  expect_invalid_count<ObTransferTabletIDArray>(-1);
  expect_invalid_count<ObTransferTabletIDArray>(
      schema::OB_MAX_TRANSFER_BINDING_TABLET_CNT + 1);
  expect_invalid_count<ObRebuildTabletIDArray>(-1);
  expect_invalid_count<ObRebuildTabletIDArray>(65);
}

TEST(ObFixedTabletIDArray, valid_boundary_round_trip)
{
  expect_round_trip<ObTransferTabletIDArray>(0);
  expect_round_trip<ObTransferTabletIDArray>(
      schema::OB_MAX_TRANSFER_BINDING_TABLET_CNT);
  expect_round_trip<ObRebuildTabletIDArray>(0);
  expect_round_trip<ObRebuildTabletIDArray>(64);
}

TEST(ObTabletHAStatus, require_and_preserve_expected_status)
{
  ObTabletHAStatus status;
  EXPECT_FALSE(status.is_valid());
  ASSERT_EQ(OB_SUCCESS, status.set_restore_status(ObTabletRestoreStatus::FULL));
  ASSERT_EQ(OB_SUCCESS, status.set_data_status(ObTabletDataStatus::COMPLETE));
  EXPECT_FALSE(status.is_valid());
  ASSERT_EQ(OB_SUCCESS, status.set_expected_status(ObTabletExpectedStatus::NORMAL));
  EXPECT_TRUE(status.is_valid());

  expect_ha_status_round_trip(ObTabletExpectedStatus::NORMAL);
  expect_ha_status_round_trip(ObTabletExpectedStatus::DELETED);
}


int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  system("rm -rf test_rebuild_src.log");
  OB_LOGGER.set_file_name("test_rebuild_src.log");

  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
