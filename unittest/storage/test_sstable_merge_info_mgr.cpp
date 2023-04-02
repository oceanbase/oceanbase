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
#include "storage/compaction/ob_sstable_merge_info_mgr.h"
#include "observer/omt/ob_tenant_node_balancer.h"

namespace oceanbase
{
using namespace blocksstable;
using namespace common;
using namespace storage;
using namespace lib;
using namespace share;
using namespace omt;

namespace unittest
{

class TestSSTableMergeInfoMgr : public ::testing::Test
{
public:
  TestSSTableMergeInfoMgr()
    : tenant_id_(1001),
      merge_info_mgr_(nullptr),
      tenant_base_(1001)
  { }
  ~TestSSTableMergeInfoMgr() {}
  void SetUp()
  {
    ObMallocAllocator::get_instance()->create_and_add_tenant_allocator(1001);
    ObUnitInfoGetter::ObTenantConfig unit_config;
    unit_config.mode_ = lib::Worker::CompatMode::MYSQL;
    unit_config.tenant_id_ = 0;
    TenantUnits units;
    ASSERT_EQ(OB_SUCCESS, units.push_back(unit_config));

    merge_info_mgr_ = OB_NEW(ObTenantSSTableMergeInfoMgr, ObModIds::TEST);
    tenant_base_.set(merge_info_mgr_);

    ObTenantEnv::set_tenant(&tenant_base_);
    ASSERT_EQ(OB_SUCCESS, tenant_base_.init());
  }
  void TearDown()
  {
    merge_info_mgr_->~ObTenantSSTableMergeInfoMgr();
    merge_info_mgr_ = nullptr;
    tenant_base_.destroy();
    ObTenantEnv::set_tenant(nullptr);
    ObMallocAllocator::get_instance()->recycle_tenant_allocator(1001);
  }
private:
  const uint64_t tenant_id_;
  ObTenantSSTableMergeInfoMgr *merge_info_mgr_;
  ObTenantBase tenant_base_;
  DISALLOW_COPY_AND_ASSIGN(TestSSTableMergeInfoMgr);
};

TEST_F(TestSSTableMergeInfoMgr, normal)
{
  int ret = OB_SUCCESS;
  ObSSTableMergeInfo merge_info;
  ObTenantSSTableMergeInfoMgr *merge_info_mgr = MTL(ObTenantSSTableMergeInfoMgr*);
  ASSERT_TRUE(nullptr != merge_info_mgr);
  //not init
  ret = MTL(ObTenantSSTableMergeInfoMgr*)->add_sstable_merge_info(merge_info);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = MTL(ObTenantSSTableMergeInfoMgr*)->init(0);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = MTL(ObTenantSSTableMergeInfoMgr*)->init(1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = MTL(ObTenantSSTableMergeInfoMgr*)->init(1);
  ASSERT_NE(OB_SUCCESS, ret);

  ret = MTL(ObTenantSSTableMergeInfoMgr*)->add_sstable_merge_info(merge_info);
  ASSERT_NE(OB_SUCCESS, ret);

  const uint64_t tenant_id = 1001;
  merge_info.tenant_id_ = 1;
  merge_info.ls_id_ = 1;
  merge_info.tablet_id_ = 2;
  merge_info.compaction_scn_ = 100;
  ret = MTL(ObTenantSSTableMergeInfoMgr*)->add_sstable_merge_info(merge_info);
  ASSERT_EQ(OB_SUCCESS, ret);

  merge_info.tablet_id_ = 3;
  ret = MTL(ObTenantSSTableMergeInfoMgr*)->add_sstable_merge_info(merge_info);
  ASSERT_EQ(OB_SUCCESS, ret);

  //test iter
  ObSSTableMergeInfoIterator iter;
  ret = iter.open(tenant_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = iter.get_next_merge_info(merge_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = iter.get_next_merge_info(merge_info);
  ASSERT_EQ(OB_ITER_END, ret);
  ret = iter.get_next_merge_info(merge_info);
  ASSERT_EQ(OB_ITER_END, ret);
}

}  // end namespace unittest
}  // end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_sstable_merge_info_mgr.log*");
  OB_LOGGER.set_file_name("test_sstable_merge_info_mgr.log");
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
