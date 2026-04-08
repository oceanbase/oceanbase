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

#define USING_LOG_PREFIX STORAGE
#include "gtest/gtest.h"

#include "lib/ob_errno.h"
#include "storage/shared_storage/ob_ss_format_util.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;

struct ObSSFormatBodyWithExtraField final
{
public:
  const char *const VERSION_ID_STR = "version_id=";
  const char *const CLUSTER_VERSION_STR = "cluster_version=";
  const char *const CREATE_TIMESTAMP_STR = "create_timestamp=";
  const char *const COMPAT_EXTRA_FIELD_STR = "compat_extra_field=";
public:
  ObSSFormatBodyWithExtraField()
    : version_(ObSSFormatBody::SS_FORMAT_VERSION),
      cluster_version_(OB_INVALID_ID),
      create_timestamp_(0),
      compat_extra_field_(0)
  {
  }

  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const
  {
    int ret = OB_SUCCESS;
    const int64_t ser_len = get_serialize_size();
    int64_t new_pos = pos;
    if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0 || (buf_len - new_pos) < ser_len)) {
      ret = OB_INVALID_ARGUMENT;
    } else if (OB_FAIL(databuff_printf(buf, buf_len, new_pos, "%s%ld\n", VERSION_ID_STR, version_))) {
    } else if (OB_FAIL(databuff_printf(buf, buf_len, new_pos, "%s%lu\n", CLUSTER_VERSION_STR, cluster_version_))) {
    } else if (OB_FAIL(databuff_printf(buf, buf_len, new_pos, "%s%ld\n", CREATE_TIMESTAMP_STR, create_timestamp_))) {
    } else if (OB_FAIL(databuff_printf(buf, buf_len, new_pos, "%s%ld\n", COMPAT_EXTRA_FIELD_STR, compat_extra_field_))) {
    } else {
      // Keep the same trailing '\0' convention as ObSSFormatBody::serialize().
      pos = new_pos + 1;
    }
    return ret;
  }

  int64_t get_serialize_size() const
  {
    int ret = OB_SUCCESS;
    int64_t len = 0;
    char tmp_buf[SS_FORMAT_META_BUFF_SIZE] = {0};
    int64_t tmp_pos = 0;
    if (OB_FAIL(databuff_printf(tmp_buf, sizeof(tmp_buf), tmp_pos, "%s%ld\n", VERSION_ID_STR, version_))) {
    } else if (OB_FAIL(databuff_printf(tmp_buf, sizeof(tmp_buf), tmp_pos, "%s%lu\n", CLUSTER_VERSION_STR, cluster_version_))) {
    } else if (OB_FAIL(databuff_printf(tmp_buf, sizeof(tmp_buf), tmp_pos, "%s%ld\n", CREATE_TIMESTAMP_STR, create_timestamp_))) {
    } else if (OB_FAIL(databuff_printf(tmp_buf, sizeof(tmp_buf), tmp_pos, "%s%ld\n", COMPAT_EXTRA_FIELD_STR, compat_extra_field_))) {
    } else {
      len = tmp_pos + 1;
    }
    return len;
  }

public:
  int64_t version_;
  uint64_t cluster_version_;
  int64_t create_timestamp_;
  int64_t compat_extra_field_;
};

TEST(TestSSCommonStruct, deserialize_format_body_from_new_struct)
{
  const int64_t buf_size = 1024;
  char buf[buf_size];
  MEMSET(buf, '\0', buf_size);

  ObSSFormatBodyWithExtraField new_body;
  new_body.version_ = ObSSFormatBody::SS_FORMAT_VERSION;
  new_body.cluster_version_ = 1718000337;
  new_body.create_timestamp_ = 1718000448;
  new_body.compat_extra_field_ = 2026031201;

  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, new_body.serialize(buf, buf_size, pos));
  ASSERT_EQ(new_body.get_serialize_size(), pos);

  ObSSFormatBody old_body;
  int64_t old_pos = 0;
  ASSERT_EQ(OB_SUCCESS, old_body.deserialize(buf, pos, old_pos));
  ASSERT_EQ(true, old_body.is_valid());
  ASSERT_EQ(new_body.version_, old_body.version_);
  ASSERT_EQ(new_body.cluster_version_, old_body.cluster_version_);
  ASSERT_EQ(new_body.create_timestamp_, old_body.create_timestamp_);
  ASSERT_EQ(pos, old_pos);
}

}  // namespace storage
}  // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_ss_common_struct.log*");
  OB_LOGGER.set_file_name("test_ss_common_struct.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
