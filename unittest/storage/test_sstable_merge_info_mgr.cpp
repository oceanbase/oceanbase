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
static const int64_t MERGE_INFO_PAGE_SIZE = 1LL << 13; // 8KB
class TestSSTableMergeInfoMgr : public ::testing::Test
{
public:
  TestSSTableMergeInfoMgr()
    : tenant_id_(1),
      merge_info_mgr_(nullptr),
      tenant_base_(tenant_id_)
  { }
  ~TestSSTableMergeInfoMgr() {}
  void SetUp()
  {
    ObMallocAllocator::get_instance()->create_and_add_tenant_allocator(tenant_id_);
    ObUnitInfoGetter::ObTenantConfig unit_config;
    unit_config.mode_ = lib::Worker::CompatMode::MYSQL;
    unit_config.tenant_id_ = 0;
    TenantUnits units;
    ASSERT_EQ(OB_SUCCESS, units.push_back(unit_config));

    merge_info_mgr_ = OB_NEW(ObTenantSSTableMergeInfoMgr, ObModIds::TEST);
    tenant_base_.set(merge_info_mgr_);

    ObTenantEnv::set_tenant(&tenant_base_);
    ASSERT_EQ(OB_SUCCESS, tenant_base_.init());

    ObMallocAllocator *ma = ObMallocAllocator::get_instance();
    ASSERT_EQ(OB_SUCCESS, ma->create_and_add_tenant_allocator(tenant_id_));
    ASSERT_EQ(OB_SUCCESS, ma->set_tenant_limit(tenant_id_, 1LL << 30));
  }
  void TearDown()
  {
    merge_info_mgr_->~ObTenantSSTableMergeInfoMgr();
    merge_info_mgr_ = nullptr;
    tenant_base_.destroy();
    ObTenantEnv::set_tenant(nullptr);
    ObMallocAllocator::get_instance()->recycle_tenant_allocator(tenant_id_);
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
  compaction::ObDiagnoseInfoParam<2, 0> info_param;
  info_param.type_.suspect_type_ = ObSuspectInfoType::SUSPECT_MEMTABLE_CANT_MINOR_MERGE;
  info_param.struct_type_ = compaction::ObInfoParamStructType::SUSPECT_INFO_PARAM;
  merge_info.info_param_ = &info_param;

  ObTenantSSTableMergeInfoMgr *merge_info_mgr = MTL(ObTenantSSTableMergeInfoMgr*);
  ASSERT_TRUE(nullptr != merge_info_mgr);
  //not init
  ret = MTL(ObTenantSSTableMergeInfoMgr*)->add_sstable_merge_info(merge_info);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = MTL(ObTenantSSTableMergeInfoMgr*)->init(MERGE_INFO_PAGE_SIZE);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = MTL(ObTenantSSTableMergeInfoMgr*)->add_sstable_merge_info(merge_info);
  ASSERT_NE(OB_SUCCESS, ret);

  merge_info.tenant_id_ = 1;
  merge_info.ls_id_ = 1;
  merge_info.tablet_id_ = 2;
  merge_info.compaction_scn_ = 100;
  ret = MTL(ObTenantSSTableMergeInfoMgr*)->add_sstable_merge_info(merge_info);
  ASSERT_EQ(OB_SUCCESS, ret);

  merge_info.tablet_id_ = 3;
  ret = MTL(ObTenantSSTableMergeInfoMgr*)->add_sstable_merge_info(merge_info);
  ASSERT_EQ(OB_SUCCESS, ret);

}

TEST_F(TestSSTableMergeInfoMgr, iterator)
{
  int ret = OB_SUCCESS;
  ObTenantSSTableMergeInfoMgr *merge_info_mgr = MTL(ObTenantSSTableMergeInfoMgr*);
  ASSERT_TRUE(nullptr != merge_info_mgr);

  ret = MTL(ObTenantSSTableMergeInfoMgr*)->init(MERGE_INFO_PAGE_SIZE);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObSSTableMergeInfo merge_info;
  compaction::ObDiagnoseInfoParam<2, 0> info_param;
  info_param.type_.suspect_type_ = ObSuspectInfoType::SUSPECT_MEMTABLE_CANT_MINOR_MERGE;
  info_param.struct_type_ = compaction::ObInfoParamStructType::SUSPECT_INFO_PARAM;
  merge_info.info_param_ = &info_param;

  merge_info.tenant_id_ = 1;
  merge_info.ls_id_ = 1;
  merge_info.tablet_id_ = 3;
  merge_info.compaction_scn_ = 100;
  merge_info.merge_type_ = ObMergeType::MINOR_MERGE;
  ret = MTL(ObTenantSSTableMergeInfoMgr*)->add_sstable_merge_info(merge_info);
  ASSERT_EQ(OB_SUCCESS, ret);

  merge_info.tablet_id_ = 4;
  ret = MTL(ObTenantSSTableMergeInfoMgr*)->add_sstable_merge_info(merge_info);
  ASSERT_EQ(OB_SUCCESS, ret);

  merge_info.tablet_id_ = 1;
  merge_info.merge_type_ = ObMergeType::MAJOR_MERGE;
  ret = MTL(ObTenantSSTableMergeInfoMgr*)->add_sstable_merge_info(merge_info);
  ASSERT_EQ(OB_SUCCESS, ret);

  merge_info.tablet_id_ = 2;
  ret = MTL(ObTenantSSTableMergeInfoMgr*)->add_sstable_merge_info(merge_info);
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_EQ(4, MTL(ObTenantSSTableMergeInfoMgr*)->size());

  compaction::ObIDiagnoseInfoMgr::Iterator major_iterator;
  compaction::ObIDiagnoseInfoMgr::Iterator minor_iterator;
  ASSERT_EQ(OB_SUCCESS, MTL(ObTenantSSTableMergeInfoMgr *)->open_iter(major_iterator, minor_iterator));

  ObSSTableMergeInfo read_info;
  char comment[common::OB_COMPACTION_EVENT_STR_LENGTH];
  int i = 1;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(ObTenantSSTableMergeInfoMgr::get_next_info(major_iterator, minor_iterator, read_info, comment, sizeof(comment)))) {
      ASSERT_EQ(OB_ITER_END, ret);
    } else {
      ASSERT_EQ(ObTabletID(i), read_info.tablet_id_);
      ++i;
    }
  }
  ASSERT_EQ(OB_ITER_END, ObTenantSSTableMergeInfoMgr::get_next_info(major_iterator, minor_iterator, read_info, comment, sizeof(comment)));

  compaction::ObIDiagnoseInfoMgr::Iterator major_iterator1;
  compaction::ObIDiagnoseInfoMgr::Iterator minor_iterator1;
  ASSERT_EQ(OB_SUCCESS, MTL(ObTenantSSTableMergeInfoMgr *)->open_iter(major_iterator1, minor_iterator1));
  i = 1;
  ASSERT_EQ(OB_SUCCESS, ObTenantSSTableMergeInfoMgr::get_next_info(major_iterator1, minor_iterator1, read_info, comment, sizeof(comment)));
  ASSERT_EQ(TRUE, read_info.tablet_id_ == ObTabletID(i++));
  ASSERT_EQ(TRUE, read_info.merge_type_ == MAJOR_MERGE);
  ASSERT_EQ(OB_SUCCESS, ObTenantSSTableMergeInfoMgr::get_next_info(major_iterator1, minor_iterator1, read_info, comment, sizeof(comment)));
  ASSERT_EQ(TRUE, read_info.tablet_id_ == ObTabletID(i++));
  ASSERT_EQ(TRUE, read_info.merge_type_ == MAJOR_MERGE);
  ASSERT_EQ(OB_SUCCESS, ObTenantSSTableMergeInfoMgr::get_next_info(major_iterator1, minor_iterator1, read_info, comment, sizeof(comment)));
  ASSERT_EQ(TRUE, read_info.tablet_id_ == ObTabletID(i++));
  ASSERT_EQ(TRUE, read_info.merge_type_ == MINOR_MERGE);

  compaction::ObIDiagnoseInfoMgr::Iterator major_iterator2;
  compaction::ObIDiagnoseInfoMgr::Iterator minor_iterator2;
  ASSERT_EQ(OB_SUCCESS, MTL(ObTenantSSTableMergeInfoMgr *)->open_iter(major_iterator2, minor_iterator2));
  i = 1;
  ASSERT_EQ(OB_SUCCESS, ObTenantSSTableMergeInfoMgr::get_next_info(major_iterator2, minor_iterator2, read_info, comment, sizeof(comment)));
  ASSERT_EQ(TRUE, read_info.tablet_id_ == ObTabletID(i++));
  ASSERT_EQ(TRUE, read_info.merge_type_ == MAJOR_MERGE);
  ASSERT_EQ(OB_SUCCESS, ObTenantSSTableMergeInfoMgr::get_next_info(major_iterator2, minor_iterator2, read_info, comment, sizeof(comment)));
  ASSERT_EQ(TRUE, read_info.tablet_id_ == ObTabletID(i++));
  ASSERT_EQ(TRUE, read_info.merge_type_ == MAJOR_MERGE);
}

TEST_F(TestSSTableMergeInfoMgr, resize)
{
  int ret = OB_SUCCESS;
  ObTenantSSTableMergeInfoMgr *merge_info_mgr = MTL(ObTenantSSTableMergeInfoMgr*);
  ASSERT_TRUE(nullptr != merge_info_mgr);

  ret = MTL(ObTenantSSTableMergeInfoMgr*)->init(MERGE_INFO_PAGE_SIZE);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObSSTableMergeInfo merge_info;
  compaction::ObDiagnoseInfoParam<2, 0> info_param;
  info_param.type_.suspect_type_ = ObSuspectInfoType::SUSPECT_MEMTABLE_CANT_MINOR_MERGE;
  info_param.struct_type_ = compaction::ObInfoParamStructType::SUSPECT_INFO_PARAM;
  merge_info.info_param_ = &info_param;

  merge_info.tenant_id_ = 1;
  merge_info.ls_id_ = 1;
  merge_info.compaction_scn_ = 100;
  merge_info.merge_type_ = ObMergeType::MINOR_MERGE;
  const int64_t max_cnt = 20;
  int i = 0;
  for (i = 0; i < max_cnt; ++i) {
    merge_info.tablet_id_ = 1+i;
    ASSERT_EQ(OB_SUCCESS, MTL(ObTenantSSTableMergeInfoMgr*)->add_sstable_merge_info(merge_info));
  }
  ASSERT_EQ(max_cnt, MTL(ObTenantSSTableMergeInfoMgr*)->size());

  merge_info.merge_type_ = ObMergeType::MAJOR_MERGE;
  for (i = 0; i < max_cnt; ++i) {
    merge_info.tablet_id_ = 1+i;
    ASSERT_EQ(OB_SUCCESS, MTL(ObTenantSSTableMergeInfoMgr*)->add_sstable_merge_info(merge_info));
  }
  ASSERT_EQ(2 * max_cnt, MTL(ObTenantSSTableMergeInfoMgr*)->size());

  compaction::ObIDiagnoseInfoMgr::Iterator major_iterator;
  compaction::ObIDiagnoseInfoMgr::Iterator minor_iterator;
  ASSERT_EQ(OB_SUCCESS, MTL(ObTenantSSTableMergeInfoMgr *)->open_iter(major_iterator, minor_iterator));

  ObSSTableMergeInfo read_info;
  char comment[common::OB_COMPACTION_EVENT_STR_LENGTH];
  ASSERT_EQ(OB_SUCCESS, ObTenantSSTableMergeInfoMgr::get_next_info(major_iterator, minor_iterator, read_info, comment, sizeof(comment)));
  ASSERT_EQ(TRUE, read_info.tablet_id_ == ObTabletID(1));
  ASSERT_EQ(TRUE, read_info.merge_type_ == ObMergeType::MAJOR_MERGE);

  // every info is 880 bytes, each page contains 8 info, 20 infos are in 3 pages (8 8 4)
  // after set_max, major pool has 1 page, minor pool has 3 page
  // major pool left 3 info (3 * 880 < 8192 * 0.4)
  // minor pool don't need to purge
  ret = MTL(ObTenantSSTableMergeInfoMgr*)->set_max(4 * MERGE_INFO_PAGE_SIZE);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(3 + 20, MTL(ObTenantSSTableMergeInfoMgr*)->size());

  i = 18;
  while (i <= 20 && OB_SUCC(ret)) {
    if (OB_FAIL(ObTenantSSTableMergeInfoMgr::get_next_info(major_iterator, minor_iterator, read_info, comment, sizeof(comment)))) {
      ASSERT_EQ(OB_ITER_END, ret);
    } else {
      ASSERT_EQ(TRUE, read_info.tablet_id_ == ObTabletID(i));
      ASSERT_EQ(TRUE, read_info.merge_type_ == ObMergeType::MAJOR_MERGE);
      ++i;
    }
  }
  i = 1;
  while (i <= 20 && OB_SUCC(ret)) {
    if (OB_FAIL(ObTenantSSTableMergeInfoMgr::get_next_info(major_iterator, minor_iterator, read_info, comment, sizeof(comment)))) {
      ASSERT_EQ(OB_ITER_END, ret);
    } else {
      ASSERT_EQ(TRUE, read_info.tablet_id_ == ObTabletID(i));
      ASSERT_EQ(TRUE, read_info.merge_type_ == ObMergeType::MINOR_MERGE);
      ++i;
    }
  }

  merge_info.merge_type_ = ObMergeType::MAJOR_MERGE;
  // before add , major pool has one page which contains 4 info (1 purged, 3 valid)
  for (i = 1; i <= 10; ++i) {
    merge_info.tablet_id_ = max_cnt+i;
    ASSERT_EQ(OB_SUCCESS, MTL(ObTenantSSTableMergeInfoMgr*)->add_sstable_merge_info(merge_info));
  }
  ASSERT_EQ(6 + 20, MTL(ObTenantSSTableMergeInfoMgr*)->size());

  major_iterator.reset();
  minor_iterator.reset();
  ret = MTL(ObTenantSSTableMergeInfoMgr *)->open_iter(major_iterator, minor_iterator);
  ASSERT_EQ(OB_SUCCESS, ret);

  // let the iter in the major merge info pool iter_end
  i = 25;
  while (i <= 30 && OB_SUCC(ret)) {
    if (OB_FAIL(ObTenantSSTableMergeInfoMgr::get_next_info(major_iterator, minor_iterator, read_info, comment, sizeof(comment)))) {
      ASSERT_EQ(OB_ITER_END, ret);
    } else {
      ASSERT_EQ(TRUE, read_info.tablet_id_ == ObTabletID(i));
      ASSERT_EQ(TRUE, read_info.merge_type_ == ObMergeType::MAJOR_MERGE);
      ++i;
    }
  }
  ASSERT_EQ(OB_SUCCESS, ObTenantSSTableMergeInfoMgr::get_next_info(major_iterator, minor_iterator, read_info, comment, sizeof(comment)));
  ASSERT_EQ(TRUE, read_info.tablet_id_ == ObTabletID(1));
  ASSERT_EQ(TRUE, read_info.merge_type_ == ObMergeType::MINOR_MERGE);

  compaction::ObIDiagnoseInfoMgr::Iterator major_iterator1;
  compaction::ObIDiagnoseInfoMgr::Iterator minor_iterator1;
  ASSERT_EQ(OB_SUCCESS, MTL(ObTenantSSTableMergeInfoMgr *)->open_iter(major_iterator1, minor_iterator1));
  i = 25;
  while (i <= 30 && OB_SUCC(ret)) {
    if (OB_FAIL(ObTenantSSTableMergeInfoMgr::get_next_info(major_iterator1, minor_iterator1, read_info, comment, sizeof(comment)))) {
      ASSERT_EQ(OB_ITER_END, ret);
    } else {
      ASSERT_EQ(TRUE, read_info.tablet_id_ == ObTabletID(i));
      ASSERT_EQ(TRUE, read_info.merge_type_ == ObMergeType::MAJOR_MERGE);
      ++i;
    }
  }

  // after set_max, major pool has 1 page, minor pool has 2 pages
  // major pool don't need to purge (5)
  // minor pool left 7 info (7 * 880 < 16384 * 0.4) (3 4)
  ret = MTL(ObTenantSSTableMergeInfoMgr*)->set_max(2 * MERGE_INFO_PAGE_SIZE);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(6 + 7, MTL(ObTenantSSTableMergeInfoMgr*)->size());

  merge_info.merge_type_ = ObMergeType::MAJOR_MERGE;
  merge_info.tablet_id_ = 31;
  ASSERT_EQ(OB_SUCCESS, MTL(ObTenantSSTableMergeInfoMgr*)->add_sstable_merge_info(merge_info));

  // the iterator will not get the new major merge info because it is in the iter_end
  // but it can continue to get the info in minor merge info pool
  ASSERT_EQ(OB_SUCCESS, ObTenantSSTableMergeInfoMgr::get_next_info(major_iterator, minor_iterator, read_info, comment, sizeof(comment)));
  ASSERT_EQ(TRUE, read_info.tablet_id_ == ObTabletID(14));
  ASSERT_EQ(TRUE, read_info.merge_type_ == ObMergeType::MINOR_MERGE);

  ASSERT_EQ(OB_SUCCESS, ObTenantSSTableMergeInfoMgr::get_next_info(major_iterator1, minor_iterator1, read_info, comment, sizeof(comment)));
  ASSERT_EQ(TRUE, read_info.tablet_id_ == ObTabletID(31));
  ASSERT_EQ(TRUE, read_info.merge_type_ == ObMergeType::MAJOR_MERGE);
}

}  // end namespace unittest
}  // end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_sstable_merge_info_mgr.log*");
  OB_LOGGER.set_file_name("test_sstable_merge_info_mgr.log");
  OB_LOGGER.set_log_level("DEBUG");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
