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

#include <gtest/gtest.h>
#include "lib/utility/ob_print_utils.h"
#include "share/ob_lease_struct.h"

namespace oceanbase
{
namespace share
{
using namespace common;

template <typename T>
void try_reset(T &t)
{
  try_reset(t, BoolType<HAS_MEMBER(T, reset)>());
}

template <typename T>
void try_reset(T &t, const TrueType &)
{
  t.reset();
}

template <typename T>
void try_reset(T &, const FalseType &)
{
  return;
}

class TestLeaseStruct : public ::testing::Test
{
protected:
  template <typename T>
  T *alloc(void)
  {
    void *ptr = allocator_.alloc(sizeof(T));
    memset(ptr, 0, sizeof(T));
    return new(ptr) T();
  }

  template <typename T>
  void check_serialize(const T &t)
  {
    int64_t buf_size = t.get_serialize_size();
    char *buf = (char *)allocator_.alloc(buf_size);
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, t.serialize(buf, buf_size, pos));
    ASSERT_EQ(buf_size, pos);

    T *copy = alloc<T>();
    try_reset(*copy);
    pos = 0;
    ASSERT_EQ(OB_SUCCESS, copy->deserialize(buf, buf_size, pos));
    ASSERT_EQ(buf_size, pos);


    int cmp = memcmp(&t, copy, sizeof(T));
    if (0 != cmp) {
      LOG_WARN("copy by serialize and deserialize mismatch with original",
          "original", t, "copy", *copy,
          "original_hex", PHEX(&t, sizeof(T)), "copy_hex", PHEX(copy, sizeof(T)));
    }
    ASSERT_EQ(0, cmp);
  }

  ModuleArena allocator_;
};

TEST_F(TestLeaseStruct, ObServerResourceInfo)
{
  ObServerResourceInfo &res = *alloc<ObServerResourceInfo>();
  LOG_INFO("to_string", K(res));

  res.cpu_ = 1;
  res.mem_in_use_ = 2;
  res.mem_total_ = 3;
  res.disk_in_use_ = 4;
  res.disk_total_ = 5;

  LOG_INFO("to_string", K(res));

  check_serialize(res);
}

TEST_F(TestLeaseStruct, ObLeaseRequest)
{
  ObLeaseRequest &req = *alloc<ObLeaseRequest>();
  LOG_INFO("to_string", K(req));

  req.version_ = 1;
  req.zone_ = "test";
  req.server_.set_ip_addr("127.0.0.8", 80);
  req.inner_port_ = 8080;
  strcpy(req.build_version_, "test");

  LOG_INFO("to_string", K(req));
  check_serialize(req);
}

TEST_F(TestLeaseStruct, ObLeaseResponse)
{
  ObLeaseResponse &res = *alloc<ObLeaseResponse>();
  LOG_INFO("to_string", K(res));

  res.version_ = 1;
  res.lease_expire_time_ = ::oceanbase::common::ObTimeUtility::current_time();
  res.lease_info_version_ = ::oceanbase::common::ObTimeUtility::current_time() + 1024;
  res.frozen_version_ = 1024;
  res.schema_version_ = ::oceanbase::common::ObTimeUtility::current_time();

  LOG_INFO("to_string", K(res));
  check_serialize(res);
}

TEST_F(TestLeaseStruct, ObZoneLeaseInfo)
{
  ObZoneLeaseInfo &info = *alloc<ObZoneLeaseInfo>();
  LOG_INFO("to_string", K(info));

  info.zone_ = "test";
  info.privilege_version_ = ::oceanbase::common::ObTimeUtility::current_time();
  info.config_version_ = ::oceanbase::common::ObTimeUtility::current_time() + 1024;
  info.lease_info_version_ = ::oceanbase::common::ObTimeUtility::current_time() + 1234;
  info.broadcast_version_ = 3;
  info.last_merged_version_ = 2;
  info.suspend_merging_ = true;

  LOG_INFO("to_string", K(info));
}

} // end namespace share
} // end namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
