/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX STORAGE
#include <gtest/gtest.h>

#define protected public
#define private public

#include "src/storage/high_availability/ob_cs_replica_migration.h"


namespace oceanbase
{
namespace storage
{
namespace unittest
{

class TestCSReplicaMigration: public ::testing::Test
{
public:
  TestCSReplicaMigration();
  virtual ~TestCSReplicaMigration() = default;
public:
  static int mock_tablet_id_array(
      int64_t start_tablet_id,
      int64_t count,
      ObIArray<ObLogicTabletID> &tablet_id_array);
};

int TestCSReplicaMigration::mock_tablet_id_array(
    int64_t start_tablet_id,
    int64_t count,
    ObIArray<ObLogicTabletID> &tablet_id_array)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    ObLogicTabletID logic_tablet_id;
    if (OB_FAIL(logic_tablet_id.init(ObTabletID(start_tablet_id + i), 0))) {
      LOG_WARN("failed to init logic tablet id", K(ret));
    } else if (OB_FAIL(tablet_id_array.push_back(logic_tablet_id))) {
      LOG_WARN("failed to push back", K(ret));
    }
  }
  return ret;
}

class A
{
public:
  A(int a) : a_(a) {
    LOG_INFO("print a", K_(a));
  }
  TO_STRING_KV(K_(a));
public:
  int a_;
};

class B : public A
{
public:
  B(int a, int b) : A(a), b_(b) {
    LOG_INFO("print b", K_(b));
  }
  INHERIT_TO_STRING_KV("A", A, K_(b));
public:
  int b_;
};

TEST(TestCSReplicaMigration, test_group_co_ctx_pretty_print)
{
  int ret = OB_SUCCESS;
  A a(1);
  B b(1, 2);
  LOG_INFO("print a", K(a));
  LOG_INFO("print b", K(b));

  ObHATabletGroupCOConvertCtx ctx;
  ObHATabletGroupCtx base_ctx(ObHATabletGroupCtx::TabletGroupCtxType::NORMAL_TYPE);
  ObSEArray<ObLogicTabletID, 8> tablet_id_array;
  ASSERT_EQ(OB_SUCCESS,TestCSReplicaMigration::mock_tablet_id_array(200001, 100, tablet_id_array));
  ASSERT_EQ(OB_SUCCESS, ctx.init(tablet_id_array));
  ASSERT_EQ(OB_SUCCESS, base_ctx.init(tablet_id_array));
  LOG_INFO("print ctx", K(ctx));
  LOG_INFO("print base_ctx", K(base_ctx));
}

TEST(TestCSReplicaMigration, test_ha_tablet_group_mgr)
{
  int ret = OB_SUCCESS;
  ObHATabletGroupMgr mgr;
  ObSEArray<ObLogicTabletID, 10> tablet_id_array[3];
  ASSERT_EQ(OB_SUCCESS, mgr.init());
  for (int64_t i = 0; OB_SUCC(ret) && i < 3; ++i) {
    ASSERT_EQ(OB_SUCCESS, TestCSReplicaMigration::mock_tablet_id_array(200001 + i * 10000, 100, tablet_id_array[i]));
    ASSERT_EQ(OB_SUCCESS, mgr.build_tablet_group_ctx(tablet_id_array[i], ObHATabletGroupCtx::TabletGroupCtxType::CS_REPLICA_TYPE));
  }
  LOG_INFO("print mgr", K(mgr));
}

} // end unittest
} // end storage
} // end oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
