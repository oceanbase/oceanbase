/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include <thread>

#include "lib/lock/ob_qsync_lock.h"

namespace oceanbase
{
namespace share
{
using namespace common;

TEST(TestQSyncLock, basic)
{
  lib::ObMemAttr mem_attr(OB_SERVER_TENANT_ID, "TestQSyncLock");
  ObQSyncLock lock(common::ObLatchIds::OB_TEST_QSYNC_LOCK);
  lock.init(mem_attr);
  for (int i=0;i<100;i++) {
    ObQSyncLockReadGuard read_guard(lock);
  }
  ObQSyncLockWriteGuard write_guard(lock);
}

TEST(TestQSyncLock, concurrent)
{
  lib::ObMemAttr mem_attr(OB_SERVER_TENANT_ID, "TestQSyncLock");
  ObQSyncLock lock(common::ObLatchIds::OB_TEST_QSYNC_LOCK);
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

