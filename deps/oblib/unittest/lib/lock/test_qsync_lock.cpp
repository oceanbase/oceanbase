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
#include <thread>
#include <vector>

#include "lib/lock/ob_qsync_lock.h"
#include "lib/time/ob_time_utility.h"

namespace oceanbase
{
namespace share
{
using namespace common;

TEST(TestQSyncLock, basic)
{
  lib::ObMemAttr mem_attr(OB_SERVER_TENANT_ID, "TestQSyncLock");
  ObQSyncLock lock;
  lock.init(mem_attr);
  for (int i=0;i<100;i++) {
    ObQSyncLockReadGuard read_guard(lock);
  }
  ObQSyncLockWriteGuard write_guard(lock);
}

TEST(TestQSyncLock, concurrent)
{
  lib::ObMemAttr mem_attr(OB_SERVER_TENANT_ID, "TestQSyncLock");
  ObQSyncLock lock;
  lock.init(mem_attr);
  std::vector<std::thread> ths;
  bool g_stop = false;
  for (int i=0;i<10;i++) {
    std::thread th([&](){
      while (!g_stop) {
        ObQSyncLockReadGuard read_guard(lock);
      }
    });
    ths.push_back(std::move(th));
  }
  ::sleep(1);
  {
    auto start = ObTimeUtility::current_time();
    ObQSyncLockWriteGuard write_guard(lock);
    auto end = ObTimeUtility::current_time();
    std::cout << end-start << std::endl;
    ASSERT_TRUE(end - start < 1000000);
  }
  g_stop = true;
  for (auto &it : ths) {
    it.join();
  }
}


} // end share
} // oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

