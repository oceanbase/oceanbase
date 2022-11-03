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

#include "gtest/gtest.h"

#include "share/ob_tenant_mgr.h"
#include "share/ob_srv_rpc_proxy.h"

using namespace oceanbase;
using namespace common;


TEST(ObTenantManager, Simple1)
{
  ObTenantManager &tm = ObTenantManager::get_instance();

  ObAddr self;
  self.set_ip_addr("127.0.0.1", 8088);
  rpc::frame::ObReqTransport req_transport(NULL, NULL);
  obrpc::ObSrvRpcProxy rpc_proxy;
  int ret = tm.init(self, rpc_proxy, &req_transport, &ObServerConfig::get_instance());
  EXPECT_EQ(OB_SUCCESS, ret);

  const uint64_t tenantA = 1;
  ret = tm.add_tenant(tenantA);
  EXPECT_EQ(OB_SUCCESS, ret);

  const int64_t ulmt = 10;
  const int64_t llmt = 1;
  const int64_t disk_usage = 100;
  int64_t ulmt_result, llmt_result, disk_result;
  ret = tm.set_tenant_mem_limit(tenantA, ulmt, llmt);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = tm.add_tenant_disk_used(tenantA, disk_usage, 1);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = tm.get_tenant_disk_used(tenantA, disk_result, 1);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(disk_usage, disk_result);
  ret = tm.subtract_tenant_disk_used(tenantA, disk_result, 1);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = tm.get_tenant_disk_used(tenantA, disk_result, 1);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(0, disk_result);
  ret = tm.get_tenant_mem_limit(tenantA, ulmt_result, llmt_result);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(ulmt, ulmt_result);
  EXPECT_EQ(llmt, llmt_result);

  tm.destroy();
}

int init_tenant_mgr()
{
  ObTenantManager &tm = ObTenantManager::get_instance();
  ObAddr self;
  self.set_ip_addr("127.0.0.1", 8086);
  rpc::frame::ObReqTransport req_transport(NULL, NULL);
  obrpc::ObSrvRpcProxy rpc_proxy;
  int ret = tm.init(self, rpc_proxy, &req_transport, &ObServerConfig::get_instance());
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = tm.add_tenant(OB_SYS_TENANT_ID);
  EXPECT_EQ(OB_SUCCESS, ret);
  const int64_t ulmt = 16LL << 30;
  const int64_t llmt = 8LL << 30;
  ret = tm.set_tenant_mem_limit(OB_SYS_TENANT_ID, ulmt, llmt);
  EXPECT_EQ(OB_SUCCESS, ret);
  return OB_SUCCESS;
}

class TestTenantStress: public share::ObThreadPool
{
public:
  TestTenantStress();
  virtual ~TestTenantStress();
  virtual void run1();
};

TestTenantStress::TestTenantStress()
  : alloc_()
{
  alloc_.init(OB_SYS_TENANT_ID, 0);
}

TestTenantStress::~TestTenantStress()
{
}

void TestTenantStress::run(obsys::CThread *thread, void *arg)
{

  UNUSED(arg);
  const int64_t loop_cnt = 1000;
  for (int64_t loop = 0; loop < loop_cnt ; ++loop) {
    void *ptr = malloc(65536);
    EXPECT_NE((void*)NULL, ptr);
  }
}

TEST(ObTenantManager, multithread)
{
  init_tenant_mgr();
  TestTenantStress stress;
  stress.set_thread_count(32);
  uint64_t begin_time = ::oceanbase::common::ObTimeUtility::current_time();
  stress.start();
  stress.wait();
  uint64_t end_time = ::oceanbase::common::ObTimeUtility::current_time();
  printf("time: %ld us\n", (end_time - begin_time));
  int64_t active_memstore_used = 0;
  int64_t total_memstore_used = 0;
  int64_t major_freeze_trigger = 0;
  int64_t memstore_limit = 0;
  int64_t freeze_cnt = 0;
  ObTenantManager::get_instance().get_tenant_memstore_cond(OB_SYS_TENANT_ID,
                                                           active_memstore_used,
                                                           total_memstore_used,
                                                           major_freeze_trigger,
                                                           memstore_limit,
                                                           freeze_cnt);
  //EXPECT_TRUE(active_memstore_used, 65560*32*1000LL);
}

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
