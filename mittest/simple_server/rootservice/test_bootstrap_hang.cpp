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

#define USING_LOG_PREFIX RS

#include <unistd.h>
#include <sys/types.h>

#include "gtest/gtest.h"
#include "gmock/gmock.h"
#define private public
#define protected public

#include "env/ob_simple_cluster_test_base.h"
#include "share/config/ob_server_config.h"

#include "src/rootserver/ob_load_inner_table_schema_executor.h"
#define TEST_NAME "test_create_tenant_compare"

namespace oceanbase
{
namespace unittest
{
using namespace rootserver;
class ObBootstrapHangTest : public ObSimpleClusterTestBase
{
public:
  ObBootstrapHangTest() : ObSimpleClusterTestBase(TEST_NAME "_") {}
  virtual void SetUp() {}
};
bool passed = false;
void wait_load_schema_rpc_send()
{
  int64_t count = 0;
  int64_t start_ts = ObTimeUtility::current_time();
  while (ObTimeUtility::current_time() - start_ts < 300_s
      && (count = ObLoadInnerTableSchemaExecutor::get_need_hang_count()) != 0) {
    LOG_INFO("get_need_hang_count is not zero", K(count));
    ob_usleep(1_s);
  }
  if (count == 0) {
    passed = true;
  }
  ob_usleep(1_s);
  ObLoadInnerTableSchemaExecutor::load_schema_broadcast();
}
TEST_F(ObBootstrapHangTest, bootstrap_hang)
{
  int ret = OB_SUCCESS;
  ObLoadInnerTableSchemaExecutor::set_load_schema_hang_enabled(true);
  ObLoadInnerTableSchemaExecutor::load_schema_init();
  std::thread t(wait_load_schema_rpc_send);
  ObSimpleClusterTestBase::SetUp();
  t.join();
  ASSERT_TRUE(passed);
}
}
}
int main(int argc, char **argv)
{
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
