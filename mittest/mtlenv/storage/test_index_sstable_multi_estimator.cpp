// owner: peihan.dph
// owner group: sql3

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

#include "share/schema/ob_table_param.h"
#include "storage/access/ob_index_sstable_estimator.h"
#include "mtlenv/storage/blocksstable/ob_index_block_data_prepare.h"

namespace oceanbase
{
using namespace blocksstable;
using namespace common;
using namespace storage;
using namespace share::schema;

namespace unittest
{

class TestMultiVersionIndexSSTableEstimator : public TestIndexBlockDataPrepare
{
public:
  TestMultiVersionIndexSSTableEstimator();
  virtual ~TestMultiVersionIndexSSTableEstimator() {}
  virtual void SetUp();
  virtual void TearDown();
  static void SetUpTestCase();
  static void TearDownTestCase();

protected:
  void prepare_context();
  void generate_range(const int64_t start, const int64_t end, ObDatumRange &range);
  void get_part_est(ObSSTable &sstable, const ObDatumRange &range);

  // ObTableParam table_param_;
private:
  ObDatumRow start_row_;
  ObDatumRow end_row_;
};

TestMultiVersionIndexSSTableEstimator::TestMultiVersionIndexSSTableEstimator()
  : TestIndexBlockDataPrepare("Test multi version index sstable estimator", MINI_MERGE)
{
  is_ddl_merge_data_ = true;
  max_row_cnt_ = 150000;
  max_partial_row_cnt_ = 137312;
  partial_kv_start_idx_ = 29;
}

void TestMultiVersionIndexSSTableEstimator::SetUpTestCase()
{
  TestIndexBlockDataPrepare::SetUpTestCase();
}

void TestMultiVersionIndexSSTableEstimator::TearDownTestCase()
{
  TestIndexBlockDataPrepare::TearDownTestCase();
}

void TestMultiVersionIndexSSTableEstimator::SetUp()
{
  TestIndexBlockDataPrepare::SetUp();
  ObLSID ls_id(ls_id_);
  ObTabletID tablet_id(tablet_id_);
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));

  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle_));
  prepare_context();
  prepare_query_param(false);
}

void TestMultiVersionIndexSSTableEstimator::TearDown()
{
  destroy_query_param();
  tablet_handle_.get_obj()->ddl_kv_count_ = 0;
  tablet_handle_.get_obj()->ddl_kvs_ = nullptr;
  tablet_handle_.reset();
  TestIndexBlockDataPrepare::TearDown();
}

void TestMultiVersionIndexSSTableEstimator::prepare_context()
{
  start_row_.init(allocator_, TEST_COLUMN_CNT);
  end_row_.init(allocator_, TEST_COLUMN_CNT);
}

void TestMultiVersionIndexSSTableEstimator::generate_range(
    const int64_t start,
    const int64_t end,
    ObDatumRange &range)
{
  ObDatumRowkey tmp_rowkey;
  if (start < 0) {
    range.start_key_.set_min_rowkey();
  } else {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(start, start_row_));
    tmp_rowkey.assign(start_row_.storage_datums_, TEST_ROWKEY_COLUMN_CNT);
    ASSERT_EQ(OB_SUCCESS, tmp_rowkey.deep_copy(range.start_key_, allocator_));
    range.border_flag_.set_inclusive_start();
  }
  if (end < 0) {
    range.end_key_.set_max_rowkey();
  } else {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(end, end_row_));
    tmp_rowkey.assign(end_row_.storage_datums_, TEST_ROWKEY_COLUMN_CNT);
    ASSERT_EQ(OB_SUCCESS, tmp_rowkey.deep_copy(range.end_key_, allocator_));
    range.border_flag_.set_inclusive_end();
  }
}

void TestMultiVersionIndexSSTableEstimator::get_part_est(ObSSTable &sstable, const ObDatumRange &range)
{
  ObPartitionEst part_est;
  ObIndexSSTableEstimateContext esti_ctx(tablet_handle_, context_.query_flag_);
  ObIndexBlockScanEstimator estimator(esti_ctx);
  ASSERT_EQ(OB_SUCCESS, estimator.estimate_row_count(sstable, range, part_est));
  STORAGE_LOG(INFO, "part_est", K(part_est));
}

TEST_F(TestMultiVersionIndexSSTableEstimator, estimate_minor_sstable_whole_range)
{
  ObDatumRange range;
  range.set_whole_range();
  get_part_est(sstable_, range);
  get_part_est(ddl_kv_, range);
  get_part_est(partial_sstable_, range);

}

TEST_F(TestMultiVersionIndexSSTableEstimator, estimate_minor_sstable_range)
{
  ObDatumRange range;
  generate_range(100, -1, range);
  get_part_est(sstable_, range);
  get_part_est(ddl_kv_, range);
  get_part_est(partial_sstable_, range);
}

TEST_F(TestMultiVersionIndexSSTableEstimator, estimate_major_sstable_left_range)
{
  ObDatumRange range;
  generate_range(-1, 100, range);
  get_part_est(sstable_, range);
  get_part_est(ddl_kv_, range);
  get_part_est(partial_sstable_, range);
}

TEST_F(TestMultiVersionIndexSSTableEstimator, estimate_major_sstable_right_range)
{
  ObDatumRange range;
  generate_range(row_cnt_ - 100, -1, range);
  get_part_est(sstable_, range);
  get_part_est(ddl_kv_, range);
  get_part_est(partial_sstable_, range);
}

TEST_F(TestMultiVersionIndexSSTableEstimator, estimate_major_sstable_middle_range)
{
  ObDatumRange range;
  generate_range(100, row_cnt_ - 100, range);
  get_part_est(sstable_, range);
  get_part_est(ddl_kv_, range);
  get_part_est(partial_sstable_, range);
}

TEST_F(TestMultiVersionIndexSSTableEstimator, estimate_major_sstable_noexist_range)
{
  ObDatumRange range;
  generate_range(row_cnt_, row_cnt_, range);
  get_part_est(sstable_, range);
  get_part_est(ddl_kv_, range);
  get_part_est(partial_sstable_, range);
}



}  // end namespace unittest
}  // end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_index_sstable_multi_estimator.log*");
  OB_LOGGER.set_file_name("test_index_sstable_multi_estimator.log", true, true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
