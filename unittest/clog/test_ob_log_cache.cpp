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

#include "clog/ob_log_cache.h"
#include "clog/ob_log_engine.h"

#include <gtest/gtest.h>
#include <gmock/gmock.h>

using namespace oceanbase::common;

namespace oceanbase {
using namespace clog;
namespace unittest {

class TestObLogCache : public ::testing::Test {};

TEST_F(TestObLogCache, smoke_test)
{
  ObLogCache log_cache;
  char write_buf[1024];
  char read_buf[1024];
  const int64_t priority = 1;
  const int64_t hot_cache_size = 1L << 28;

  common::ObAddr addr(ObAddr::VER::IPV4, "100.81.152.48", 2828);
  common::ObAddr fake_addr(ObAddr::VER::IPV4, "100.81.152.48", 2800);
  const int64_t seq = 1;
  ObKVGlobalCache::get_instance().init();
  EXPECT_EQ(OB_SUCCESS, log_cache.init(addr, "clog_cache", priority, hot_cache_size));
  ObHotCacheWarmUpHelper::test_warm_up(log_cache);
  EXPECT_EQ(OB_SUCCESS, log_cache.append_data(addr, write_buf, 1, 0, 1024));
  EXPECT_EQ(OB_SUCCESS, log_cache.hot_read(addr, 1, 512, 64, read_buf));
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, log_cache.hot_read(fake_addr, 1, 512, 64, read_buf));

  char line_buf[CLOG_CACHE_SIZE];
  ObKVCacheHandle handle;
  const char* ret_line_buf = NULL;
  EXPECT_EQ(OB_SUCCESS, log_cache.put_line(addr, seq, 1, 0, line_buf));
  EXPECT_EQ(OB_SUCCESS, log_cache.get_line(addr, seq, 1, 0, handle, ret_line_buf));
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, log_cache.get_line(fake_addr, seq, 1, 0, handle, ret_line_buf));
  ObKVGlobalCache::get_instance().destroy();
}

}  // end namespace unittest
}  // end namespace oceanbase

int main(int argc, char** argv)
{
  OB_LOGGER.set_file_name("test.log", true);
  OB_LOGGER.set_log_level("TRACE");
  CLOG_LOG(INFO, "begin unittest::test_ob_log_cache");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
