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
#include "lib/ob_define.h"
#include "lib/random/ob_random.h"
#include "lib/alloc/ob_malloc_allocator.h"
#include "share/allocator/ob_tenant_mutil_allocator_mgr.h"
#include "share/allocator/ob_tenant_mutil_allocator.h"
#include "lib/oblog/ob_log.h"
#include "storage/ob_file_system_router.h"

namespace oceanbase
{
using namespace common;

namespace unittest
{

static const int64_t TENANT_CNT = 10000;

TEST(TestTMAMgr, test_tma_mgr)
{
  ObTenantMutilAllocator *null_p = NULL;
  ObTenantMutilAllocator **cur = &null_p;
  PALF_LOG(INFO, "test_tma_mgr begin", "cur", (NULL == cur), "*cur", (NULL == (*cur)));

  PALF_LOG(INFO, "test_tma_mgr begin", "TMA size", sizeof(ObTenantMutilAllocator));
  ObMallocAllocator *malloc_allocator = ObMallocAllocator::get_instance();
  ASSERT_EQ(OB_SUCCESS, TMA_MGR_INSTANCE.init());
  int64_t tenant_id = 1;
  for (tenant_id = 1; tenant_id <= TENANT_CNT; ++tenant_id) {
    ObILogAllocator *tenant_allocator = NULL;
    EXPECT_EQ(OB_SUCCESS, TMA_MGR_INSTANCE.get_tenant_log_allocator(tenant_id, tenant_allocator));
  }
  PALF_LOG(INFO, "after create all TMA", "TMA size", sizeof(ObTenantMutilAllocator), "500 tenant hold", \
      malloc_allocator->get_tenant_hold(OB_SERVER_TENANT_ID));
  // delete TMA by increasing order
  for (tenant_id = 1; tenant_id <= TENANT_CNT; ++tenant_id) {
    EXPECT_EQ(OB_SUCCESS, TMA_MGR_INSTANCE.delete_tenant_log_allocator(tenant_id));
  }
  PALF_LOG(INFO, "after delete all TMA", "TMA size", sizeof(ObTenantMutilAllocator), "500 tenant hold", \
      malloc_allocator->get_tenant_hold(OB_SERVER_TENANT_ID));

  for (tenant_id = 1; tenant_id <= TENANT_CNT; ++tenant_id) {
    ObILogAllocator *tenant_allocator = NULL;
    EXPECT_EQ(OB_SUCCESS, TMA_MGR_INSTANCE.get_tenant_log_allocator(tenant_id, tenant_allocator));
  }
  PALF_LOG(INFO, "after create all TMA", "TMA size", sizeof(ObTenantMutilAllocator), "500 tenant hold", \
      malloc_allocator->get_tenant_hold(OB_SERVER_TENANT_ID));
  // delete TMA by decreasing order
  for (tenant_id = TENANT_CNT; tenant_id >= 1; --tenant_id) {
    EXPECT_EQ(OB_SUCCESS, TMA_MGR_INSTANCE.delete_tenant_log_allocator(tenant_id));
  }
  PALF_LOG(INFO, "after delete all TMA", "TMA size", sizeof(ObTenantMutilAllocator), "500 tenant hold", \
      malloc_allocator->get_tenant_hold(OB_SERVER_TENANT_ID));

  for (tenant_id = 1; tenant_id <= TENANT_CNT; ++tenant_id) {
    ObILogAllocator *tenant_allocator = NULL;
    EXPECT_EQ(OB_SUCCESS, TMA_MGR_INSTANCE.get_tenant_log_allocator(tenant_id, tenant_allocator));
  }
  PALF_LOG(INFO, "after create all TMA", "TMA size", sizeof(ObTenantMutilAllocator), "500 tenant hold", \
      malloc_allocator->get_tenant_hold(OB_SERVER_TENANT_ID));
  ObRandom random;
  // delete TMA by RANDOM order
  for (int64_t loop_cnt = 1; loop_cnt <= TENANT_CNT / 2; ++loop_cnt) {
    int64_t tmp_tenant_id = random.rand(1, TENANT_CNT);
    EXPECT_EQ(OB_SUCCESS, TMA_MGR_INSTANCE.delete_tenant_log_allocator(tmp_tenant_id));
    ObILogAllocator *tenant_allocator = NULL;
    EXPECT_EQ(OB_SUCCESS, TMA_MGR_INSTANCE.get_tenant_log_allocator(tmp_tenant_id, tenant_allocator));
  }
  PALF_LOG(INFO, "after delete all TMA", "TMA size", sizeof(ObTenantMutilAllocator), "500 tenant hold", \
      malloc_allocator->get_tenant_hold(OB_SERVER_TENANT_ID));
  PALF_LOG(INFO, "test_tma_mgr end");
}

} // END of unittest
} // end of oceanbase

int main(int argc, char **argv)
{
  system("rm -f ./test_tma_mgr.log*");
  OB_LOGGER.set_file_name("test_tma_mgr.log", true);
  OB_LOGGER.set_log_level("TRACE");
  PALF_LOG(INFO, "begin unittest::test_tma_mgr");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
