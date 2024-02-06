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
#define private public
#include "share/allocator/ob_memstore_allocator_mgr.h"
#include "share/ob_tenant_mgr.h"
#include "share/ob_srv_rpc_proxy.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::share;


TEST(ObMemstoreAllocatorMgr, funcs)
{
  const uint64_t PRESERVED_TENANT_COUNT = 10000;
  common::ObMemstoreAllocatorMgr &alloc_mgr = common::ObMemstoreAllocatorMgr::get_instance();
  int ret = alloc_mgr.init();
  EXPECT_EQ(OB_SUCCESS, ret);

  common::ObGMemstoreAllocator *allocator = NULL;

  ret = alloc_mgr.get_tenant_memstore_allocator(0, allocator);
  EXPECT_EQ(OB_INVALID_ARGUMENT, ret);
  EXPECT_EQ((void*)NULL, allocator);

  ret = alloc_mgr.get_tenant_memstore_allocator(OB_SYS_TENANT_ID, allocator);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_NE((void*)NULL, allocator);

  ret = alloc_mgr.get_tenant_memstore_allocator(OB_SYS_TENANT_ID, allocator);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_NE((void*)NULL, allocator);

  ret = alloc_mgr.get_tenant_memstore_allocator(OB_SYS_TENANT_ID + PRESERVED_TENANT_COUNT, allocator);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_NE((void*)NULL, allocator);

  ret = alloc_mgr.get_tenant_memstore_allocator(OB_SYS_TENANT_ID + PRESERVED_TENANT_COUNT, allocator);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_NE((void*)NULL, allocator);

}

int init_tenant_mgr()
{
  ObTenantManager &tm = ObTenantManager::get_instance();
  /*
  ObAddr self;
  self.set_ip_addr("127.0.0.1", 8086);
  rpc::frame::ObReqTransport req_transport(NULL, NULL);
  obrpc::ObSrvRpcProxy rpc_proxy;*/
  //int ret = tm.init(self, rpc_proxy, &req_transport, &ObServerConfig::get_instance());
  int ret = tm.init();
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = tm.add_tenant(OB_SYS_TENANT_ID);
  EXPECT_EQ(OB_SUCCESS, ret);
  const int64_t ulmt = 16LL << 30;
  const int64_t llmt = 8LL << 30;
  ret = tm.set_tenant_mem_limit(OB_SYS_TENANT_ID, ulmt, llmt);
  EXPECT_EQ(OB_SUCCESS, ret);
  common::ObMemstoreAllocatorMgr &mem_mgr = common::ObMemstoreAllocatorMgr::get_instance();
  ret = mem_mgr.init();
  EXPECT_EQ(OB_SUCCESS, ret);
  return ret;
}

int main(int argc, char** argv)
{
  //init_tenant_mgr();
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
