/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/syscall.h>
#include "lib/resource/ob_affinity_ctrl.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log.h"

using namespace oceanbase::common;

#define USING_LOG_PREFIX SERVER

TEST(TestAffiCtrl, test0)
{
  int ret;
  int node_status;

  ret = AFFINITY_CTRL.init(true);
  ASSERT_EQ(ret, 0);

  ASSERT_EQ(AFFINITY_CTRL.get_tls_node(), OB_NUMA_SHARED_INDEX);

  ret = oceanbase::lib::ObAffinityCtrl::get_instance().thread_bind_to_node(0);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(oceanbase::lib::ObAffinityCtrl::get_tls_node(), 0);

  ret = oceanbase::lib::ObAffinityCtrl::get_instance().run_on_node(-1000);
  ASSERT_EQ(ret, OB_INVALID_ARGUMENT);
  ASSERT_EQ(oceanbase::lib::ObAffinityCtrl::get_tls_node(), 0);

  ret = oceanbase::lib::ObAffinityCtrl::get_instance().run_on_node(1000);
  ASSERT_EQ(ret, OB_INVALID_ARGUMENT);
  ASSERT_EQ(oceanbase::lib::ObAffinityCtrl::get_tls_node(), 0);
}


int main(int argc, char *argv[])
{
  OB_LOGGER.set_file_name("test_affinity_ctrl.log", true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
