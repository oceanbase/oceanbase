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
#define private public
#define protected public

#include "ob_index_block_data_prepare.h"
#include "storage/ddl/ob_direct_insert_sstable_ctx_new.h"

namespace oceanbase
{
using namespace lib;
using namespace common;
using namespace share;
using namespace storage;
using namespace blocksstable;

class TestDirectLoad : public TestIndexBlockDataPrepare
{
public:
  TestDirectLoad() : TestIndexBlockDataPrepare("Test direct load") {}
  virtual ~TestDirectLoad() {}
  static void SetUpTestCase() { TestIndexBlockDataPrepare::SetUpTestCase(); }
  static void TearDownTestCase() { TestIndexBlockDataPrepare::TearDownTestCase(); }

  virtual void SetUp();
  virtual void TearDown();
};

void TestDirectLoad::SetUp()
{
  TestIndexBlockDataPrepare::SetUp();
  ObLSID ls_id(ls_id_);
  ObTabletID tablet_id(tablet_id_);
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle_));

  prepare_query_param(true);
}

void TestDirectLoad::TearDown()
{
  destroy_query_param();
  tablet_handle_.reset();
  TestIndexBlockDataPrepare::TearDown();
}

TEST_F(TestDirectLoad, init_ddl_table_store)
{
  ObTabletFullDirectLoadMgr tablet_dl_mgr;
  ObTabletDirectLoadInsertParam build_param;
  build_param.common_param_.ls_id_ = ls_id_;
  build_param.common_param_.tablet_id_ = tablet_id_;
  build_param.common_param_.direct_load_type_ = ObDirectLoadType::DIRECT_LOAD_DDL;
  build_param.common_param_.read_snapshot_ = SNAPSHOT_VERSION;
  build_param.runtime_only_param_.task_cnt_ = 1;
  build_param.runtime_only_param_.task_id_ = 1;
  build_param.runtime_only_param_.table_id_ = TEST_TABLE_ID;
  build_param.runtime_only_param_.schema_version_ = 1;
  SCN ddl_start_scn;
  ASSERT_EQ(OB_SUCCESS, ddl_start_scn.convert_from_ts(ObTimeUtility::current_time()));
  ASSERT_EQ(OB_SUCCESS, tablet_dl_mgr.update(nullptr, build_param));
  tablet_dl_mgr.start_scn_ = ddl_start_scn;
  tablet_dl_mgr.data_format_version_ = DATA_VERSION_4_0_0_0;
  ASSERT_EQ(OB_SUCCESS, tablet_dl_mgr.init_ddl_table_store(ddl_start_scn, SNAPSHOT_VERSION, ddl_start_scn));

}

TEST_F(TestDirectLoad, test_cg_aggregate)
{
  ObTabletFullDirectLoadMgr tablet_dl_mgr;
  ObTabletDirectLoadInsertParam build_param;
  build_param.common_param_.ls_id_ = ls_id_;
  build_param.common_param_.tablet_id_ = tablet_id_;
  build_param.common_param_.direct_load_type_ = ObDirectLoadType::DIRECT_LOAD_DDL;
  build_param.common_param_.read_snapshot_ = SNAPSHOT_VERSION;
  build_param.runtime_only_param_.task_cnt_ = 1;
  build_param.runtime_only_param_.task_id_ = 1;
  build_param.runtime_only_param_.table_id_ = TEST_TABLE_ID;
  build_param.runtime_only_param_.schema_version_ = 1;
  SCN ddl_start_scn;
  ASSERT_EQ(OB_SUCCESS, ddl_start_scn.convert_from_ts(ObTimeUtility::current_time()));
  ASSERT_EQ(OB_SUCCESS, tablet_dl_mgr.update(nullptr, build_param));
  tablet_dl_mgr.start_scn_ = ddl_start_scn;
  tablet_dl_mgr.data_format_version_ = DATA_VERSION_4_0_0_0;
  ASSERT_EQ(OB_SUCCESS, tablet_dl_mgr.init_ddl_table_store(ddl_start_scn, SNAPSHOT_VERSION, ddl_start_scn));

  common::ObArenaAllocator allocator;

  ObArray<ObDirectLoadSliceWriter *> sorted_slices;
  for (int64_t i = 0; i < 3; ++i) {
    ObDirectLoadSliceWriter *slice_writer = nullptr;
    slice_writer = OB_NEWx(ObDirectLoadSliceWriter, (&allocator));
    ASSERT_NE(nullptr, slice_writer);
    ASSERT_EQ(OB_SUCCESS, sorted_slices.push_back(slice_writer));
  }

  // case 1:one thread can handle the number of slices divided according to EACH_MACRO_MIN_ROW_CNT
  for (int64_t i = 0; i < sorted_slices.count(); ++i) {
    ASSERT_EQ(OB_SUCCESS, sorted_slices.at(i)->mock_chunk_store(ObTabletDirectLoadMgr::EACH_MACRO_MIN_ROW_CNT / 2 - 1));
  }
  ASSERT_EQ(OB_SUCCESS, tablet_dl_mgr.calc_cg_range(sorted_slices, 2));
  ASSERT_EQ(1, tablet_dl_mgr.get_sqc_build_ctx().sorted_slices_idx_.count());
  for (int64_t i = 0; i < tablet_dl_mgr.get_sqc_build_ctx().sorted_slices_idx_.count(); ++i) {
    const int64_t start_idx = tablet_dl_mgr.get_sqc_build_ctx().sorted_slices_idx_.at(i).start_idx_;
    const int64_t last_idx = tablet_dl_mgr.get_sqc_build_ctx().sorted_slices_idx_.at(i).last_idx_;
    STORAGE_LOG(INFO, "case1", K(start_idx), K(last_idx));
    ASSERT_EQ(start_idx, 0);
    ASSERT_EQ(last_idx, sorted_slices.count());
  }

  // case 2:all threads can handle the number of slices divided according to EACH_MACRO_MIN_ROW_CNT
  for (int64_t i = 0; i < sorted_slices.count(); ++i) {
    ASSERT_EQ(OB_SUCCESS, sorted_slices.at(i)->mock_chunk_store(ObTabletDirectLoadMgr::EACH_MACRO_MIN_ROW_CNT / 2 + 1));
  }
  ASSERT_EQ(OB_SUCCESS, tablet_dl_mgr.calc_cg_range(sorted_slices, 2));
  ASSERT_EQ(2, tablet_dl_mgr.get_sqc_build_ctx().sorted_slices_idx_.count());
  for (int64_t i = 0; i < tablet_dl_mgr.get_sqc_build_ctx().sorted_slices_idx_.count(); ++i) {
    const int64_t start_idx = tablet_dl_mgr.get_sqc_build_ctx().sorted_slices_idx_.at(i).start_idx_;
    const int64_t last_idx = tablet_dl_mgr.get_sqc_build_ctx().sorted_slices_idx_.at(i).last_idx_;
    STORAGE_LOG(INFO, "case2", K(start_idx), K(last_idx));
  }

  // case 3:all threads cannot handle the number of slices divided according to EACH_MACRO_MIN_ROW_CNT
  for (int64_t i = 0; i < sorted_slices.count(); ++i) {
    ASSERT_EQ(OB_SUCCESS, sorted_slices.at(i)->mock_chunk_store(ObTabletDirectLoadMgr::EACH_MACRO_MIN_ROW_CNT + 1));
  }
  ASSERT_EQ(OB_SUCCESS, tablet_dl_mgr.calc_cg_range(sorted_slices, 2));
  ASSERT_EQ(2, tablet_dl_mgr.get_sqc_build_ctx().sorted_slices_idx_.count());
  for (int64_t i = 0; i < tablet_dl_mgr.get_sqc_build_ctx().sorted_slices_idx_.count(); ++i) {
    const int64_t start_idx = tablet_dl_mgr.get_sqc_build_ctx().sorted_slices_idx_.at(i).start_idx_;
    const int64_t last_idx = tablet_dl_mgr.get_sqc_build_ctx().sorted_slices_idx_.at(i).last_idx_;
    STORAGE_LOG(INFO, "case3", K(start_idx), K(last_idx));
  }

  for (int64_t i = 0; i < 2; ++i) {
    ObDirectLoadSliceWriter *slice_writer = nullptr;
    ASSERT_EQ(OB_SUCCESS, sorted_slices.pop_back(slice_writer));
  }

  // case 4
  for (int64_t i = 0; i < sorted_slices.count(); ++i) {
    ASSERT_EQ(OB_SUCCESS, sorted_slices.at(i)->mock_chunk_store(ObTabletDirectLoadMgr::EACH_MACRO_MIN_ROW_CNT + 1));
  }
  ASSERT_EQ(OB_SUCCESS, tablet_dl_mgr.calc_cg_range(sorted_slices, 2));
  ASSERT_EQ(1, tablet_dl_mgr.get_sqc_build_ctx().sorted_slices_idx_.count());
  for (int64_t i = 0; i < tablet_dl_mgr.get_sqc_build_ctx().sorted_slices_idx_.count(); ++i) {
    const int64_t start_idx = tablet_dl_mgr.get_sqc_build_ctx().sorted_slices_idx_.at(i).start_idx_;
    const int64_t last_idx = tablet_dl_mgr.get_sqc_build_ctx().sorted_slices_idx_.at(i).last_idx_;
    STORAGE_LOG(INFO, "case4", K(start_idx), K(last_idx));
    ASSERT_EQ(start_idx, 0);
    ASSERT_EQ(last_idx, sorted_slices.count());
  }

  // case 5
  for (int64_t i = 0; i < sorted_slices.count(); ++i) {
    ASSERT_EQ(OB_SUCCESS, sorted_slices.at(i)->mock_chunk_store(ObTabletDirectLoadMgr::EACH_MACRO_MIN_ROW_CNT - 1));
  }
  ASSERT_EQ(OB_SUCCESS, tablet_dl_mgr.calc_cg_range(sorted_slices, 2));
  ASSERT_EQ(1, tablet_dl_mgr.get_sqc_build_ctx().sorted_slices_idx_.count());
  for (int64_t i = 0; i < tablet_dl_mgr.get_sqc_build_ctx().sorted_slices_idx_.count(); ++i) {
    const int64_t start_idx = tablet_dl_mgr.get_sqc_build_ctx().sorted_slices_idx_.at(i).start_idx_;
    const int64_t last_idx = tablet_dl_mgr.get_sqc_build_ctx().sorted_slices_idx_.at(i).last_idx_;
    STORAGE_LOG(INFO, "case5", K(start_idx), K(last_idx));
    ASSERT_EQ(start_idx, 0);
    ASSERT_EQ(last_idx, sorted_slices.count());
  }

}

} // namespace oceanbase


int main(int argc, char **argv)
{
  system("rm -f test_direct_load.log*");
  OB_LOGGER.set_file_name("test_direct_load.log", true, true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
