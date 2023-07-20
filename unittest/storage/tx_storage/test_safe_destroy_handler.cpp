/**
 * Copyright (c) 2023 OceanBase
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
#include <sys/stat.h>
#include <sys/types.h>

#define USING_LOG_PREFIX STORAGETEST

#define protected public
#define private public

#include "storage/tx_storage/ob_safe_destroy_handler.h"

namespace oceanbase
{
using namespace common;
namespace storage
{
class TestSafeDestroyHandler : public ::testing::Test
{
public:
  TestSafeDestroyHandler() = default;
  virtual ~TestSafeDestroyHandler() = default;
};


TEST_F(TestSafeDestroyHandler, destroy_before_init)
{
  int ret = OB_SUCCESS;
  ObSafeDestroyHandler handler;
  ret = handler.stop();
  ASSERT_EQ(OB_NOT_INIT, ret);
  handler.wait();
  handler.destroy();
}


} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f ./test_safe_destroy_handler.log*");

  OB_LOGGER.set_file_name("test_safe_destroy_handler.log", true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
