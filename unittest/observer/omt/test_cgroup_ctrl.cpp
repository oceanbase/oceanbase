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
#include <unistd.h>
#include <pthread.h>
#include <sys/syscall.h>
#include "all_mock.h"
#include "observer/omt/ob_cgroup_ctrl.h"
#include "lib/atomic/ob_atomic.h"

using namespace oceanbase::common;
using namespace oceanbase::omt;
using namespace oceanbase::observer;

void *thread_func(void *args)
{
  ASSERT_EQ(OB_SUCCESS, cg_ctrl.add_self_to_cgroup(1001));
  sleep(3);
  ASSERT_EQ(OB_SUCCESS, cg_ctrl.remove_self_from_cgroup(1001));
  return nullptr;
}

// Create a soft link from the current directory to the cgroup directory in advance. The cgroup directory needs to be mounted with three modules cpu, cpuset, and cpuacct
TEST(TestCgroupCtrl, AddDelete)
{
  ObCgroupCtrl cg_ctrl;
  ASSERT_EQ(OB_SUCCESS, cg_ctrl.init());
  ASSERT_TRUE(cg_ctrl.is_valid());

  pthread_t ts[4];
  for (int i = 0; i < 4; i++) {
    pthread_create(&ts[i], nullptr, thread_func, NULL);
  }
  sleep(1);

  for (int i = 0; i < 4; i++) {
    pthread_join(ts[i], nullptr);
  }
  ASSERT_EQ(OB_SUCCESS, cg_ctrl.remove_tenant_cgroup(1001));
}

TEST(TestCgroupCtrl, SetGetValue)
{
  ObCgroupCtrl cg_ctrl;
  ASSERT_EQ(OB_SUCCESS, cg_ctrl.init());
  ASSERT_TRUE(cg_ctrl.is_valid());

  pthread_t ts[4];
  for (int i = 0; i < 4; i++) {
    pthread_create(&ts[i], nullptr, thread_func, NULL);
  }
  sleep(1);

  const int32_t cpu_shares = 2048;
  int32_t cpu_shares_v = 0;
  ASSERT_EQ(OB_SUCCESS, cg_ctrl.set_cpu_shares(cpu_shares, 1001));
  ASSERT_EQ(OB_SUCCESS, cg_ctrl.get_cpu_shares(cpu_shares_v, 1001));
  ASSERT_EQ(cpu_shares, cpu_shares_v);

  const int32_t cpu_cfs_quota = 80000;
  int32_t cpu_cfs_quota_v = 0;
  ASSERT_EQ(OB_SUCCESS, cg_ctrl.set_cpu_cfs_quota(cpu_cfs_quota, 1001));
  ASSERT_EQ(OB_SUCCESS, cg_ctrl.get_cpu_cfs_quota(cpu_cfs_quota_v, 1001));
  ASSERT_EQ(cpu_cfs_quota, cpu_cfs_quota_v);

  for (int i = 0; i < 4; i++) {
    pthread_join(ts[i], nullptr);
  }
}
int main(int argc, char *argv[])
{
  OB_LOGGER.set_file_name("test_cgroup_ctrl.log", true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

