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

#define USING_LOG_PREFIX SHARE

#include "share/backup/ob_tenant_name_mgr.h"
#include <gtest/gtest.h>

using namespace oceanbase;
using namespace common;
using namespace share;

TEST(ObTenantNameSimpleMgr, basic)
{
  ObTenantNameSimpleMgr mgr;
  ObArenaAllocator allocator;
  char* buf = nullptr;
  int64_t buf_size = 0;
  int64_t pos = 0;
  int64_t schema_version = 10;
  uint64_t tenant_id = 0;

  ASSERT_EQ(OB_SUCCESS, mgr.init());
  // create 1001 t1
  ASSERT_EQ(OB_SUCCESS, mgr.add("t1", 1, 1001));
  // create 1002 t2
  ASSERT_EQ(OB_SUCCESS, mgr.add("t2", 2, 1002));
  // create 1003 t3
  ASSERT_EQ(OB_SUCCESS, mgr.add("t3", 3, 1003));
  ASSERT_EQ(OB_SUCCESS, mgr.complete(schema_version));

  pos = 0;
  buf_size = mgr.get_write_buf_size();
  buf = (char*)allocator.alloc(buf_size);
  ASSERT_TRUE(buf != nullptr);
  ASSERT_EQ(OB_SUCCESS, mgr.write_buf(buf, buf_size, pos));

  ObTenantNameSimpleMgr mgr2;
  ASSERT_EQ(OB_SUCCESS, mgr2.init());
  ASSERT_EQ(OB_SUCCESS, mgr2.read_buf(buf, buf_size));
  ASSERT_EQ(schema_version, mgr2.get_schema_version());
  ASSERT_EQ(OB_SUCCESS, mgr2.get_tenant_id("t1", 10, tenant_id));
  ASSERT_EQ(1001, tenant_id);
  ASSERT_EQ(OB_SUCCESS, mgr2.get_tenant_id("t2", 10, tenant_id));
  ASSERT_EQ(1002, tenant_id);
  ASSERT_EQ(OB_SUCCESS, mgr2.get_tenant_id("t3", 3, tenant_id));
  ASSERT_EQ(1003, tenant_id);
  ASSERT_EQ(OB_SUCCESS, mgr2.get_tenant_id("t3", 10, tenant_id));
  ASSERT_EQ(1003, tenant_id);
  ASSERT_EQ(OB_TENANT_NOT_EXIST, mgr2.get_tenant_id("t3", 2, tenant_id));

  // drop t1 and rename 1002 to t1
  schema_version = 20;
  ASSERT_EQ(OB_SUCCESS, mgr2.add("t1", 20, 1002));
  ASSERT_EQ(OB_NOT_INIT, mgr2.get_tenant_id("t1", 20, tenant_id));
  ASSERT_EQ(OB_SUCCESS, mgr2.complete(schema_version));
  ASSERT_EQ(OB_SUCCESS, mgr2.get_tenant_id("t1", 20, tenant_id));
  ASSERT_EQ(1002, tenant_id);
  ASSERT_EQ(OB_SUCCESS, mgr2.get_tenant_id("t1", 19, tenant_id));
  ASSERT_EQ(1001, tenant_id);
  pos = 0;
  buf_size = mgr2.get_write_buf_size();
  buf = (char*)allocator.alloc(buf_size);
  ASSERT_TRUE(buf != nullptr);
  ASSERT_EQ(OB_SUCCESS, mgr2.write_buf(buf, buf_size, pos));

  ObTenantNameSimpleMgr mgr3;
  ASSERT_EQ(OB_SUCCESS, mgr3.init());
  ASSERT_EQ(OB_SUCCESS, mgr3.read_buf(buf, buf_size));
  ASSERT_EQ(schema_version, mgr3.get_schema_version());
  ASSERT_EQ(OB_SUCCESS, mgr3.get_tenant_id("t1", 20, tenant_id));
  ASSERT_EQ(1002, tenant_id);
  ASSERT_EQ(OB_SUCCESS, mgr3.get_tenant_id("t1", 19, tenant_id));
  ASSERT_EQ(1001, tenant_id);
}

TEST(ObTenantNameSimpleMgr, add_same_tenant)
{
  ObTenantNameSimpleMgr mgr;
  ObArenaAllocator allocator;
  char* buf = nullptr;
  int64_t buf_size = 0;
  int64_t pos = 0;
  int64_t schema_version = 10;
  uint64_t tenant_id = 0;

  ASSERT_EQ(OB_SUCCESS, mgr.init());
  ASSERT_EQ(OB_SUCCESS, mgr.add("t1", 100, 1001));
  ASSERT_EQ(OB_ERR_SYS, mgr.add("t1", 100, 1002));
  ASSERT_EQ(OB_SUCCESS, mgr.add("t1", 3, 1003));
  ASSERT_EQ(OB_SUCCESS, mgr.add("t1", 30, 1004));
  ASSERT_EQ(OB_SUCCESS, mgr.add("t1", 300, 1001));
  ASSERT_EQ(OB_SUCCESS, mgr.complete(schema_version));
  ASSERT_EQ(OB_SUCCESS, mgr.get_tenant_id("t1", 200, tenant_id));
  ASSERT_EQ(1001, tenant_id);
}
int main(int argc, char** argv)
{
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
