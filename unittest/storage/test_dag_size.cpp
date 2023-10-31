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
#define protected public
#define private public
#include "storage/ob_partition_merge_task.h"
#include "storage/ob_build_index_task.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"

namespace oceanbase
{
using namespace blocksstable;
using namespace common;
using namespace storage;
using namespace share::schema;
using namespace ::testing;
using namespace share;

namespace unittest
{

class TestDagSize : public ::testing::Test
{
public:
  TestDagSize()
    : tenant_id_(500),
      tenant_base_(500)
  {}
  ~TestDagSize() {}
  void SetUp()
  {


    ObTenantDagScheduler *scheduler = OB_NEW(ObTenantDagScheduler, ObModIds::TEST);
    tenant_base_.set(scheduler);
    ObTenantEnv::set_tenant(&tenant_base_);
    ASSERT_EQ(OB_SUCCESS, tenant_base_.init());

    ASSERT_EQ(OB_SUCCESS, scheduler->init(500));
  }
  void TearDown()
  {
    tenant_base_.destroy();
    ObTenantEnv::set_tenant(nullptr);
  }

private:
  const uint64_t tenant_id_;
  ObTenantBase tenant_base_;
};

TEST_F(TestDagSize, test_size)
{
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler*);
  ASSERT_TRUE(nullptr != scheduler);

  ObBuildIndexDag *build_index_dag = NULL;
  ASSERT_EQ(OB_SUCCESS, scheduler->alloc_dag(build_index_dag));
  scheduler->free_dag(*build_index_dag);
  ObMigrateDag *migrate_dag = NULL;
  ASSERT_EQ(OB_SUCCESS, scheduler->alloc_dag(migrate_dag));
  scheduler->free_dag(*migrate_dag);
  ObSSTableMajorMergeDag *major_merge_dag = NULL;
  ASSERT_EQ(OB_SUCCESS, scheduler->alloc_dag(major_merge_dag));
  scheduler->free_dag(*major_merge_dag);
  ObSSTableMinorMergeDag *minor_merge_dag = NULL;
  ASSERT_EQ(OB_SUCCESS, scheduler->alloc_dag(minor_merge_dag));
  scheduler->free_dag(*minor_merge_dag);
  ObUniqueCheckingDag *unique_check_dag = NULL;
  ASSERT_EQ(OB_SUCCESS, scheduler->alloc_dag(unique_check_dag));
  scheduler->free_dag(*unique_check_dag);

  scheduler->destroy();
}

}
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
