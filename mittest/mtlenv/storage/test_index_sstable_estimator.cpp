// owner: yht146439
// owner group: storage

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

#define private public
#define protected public

#include "src/share/io/io_schedule/ob_io_mclock.h"
#include "mtlenv/storage/blocksstable/ob_index_block_data_prepare.h"

namespace oceanbase
{
using namespace blocksstable;
using namespace common;
using namespace storage;
using namespace share::schema;

namespace unittest
{

class TestIndexSSTableEstimator : public TestIndexBlockDataPrepare
{
public:
  TestIndexSSTableEstimator();
  virtual ~TestIndexSSTableEstimator() {}
  virtual void SetUp();
  virtual void TearDown();
  static void SetUpTestCase();
  static void TearDownTestCase();

protected:
  void prepare_context();
  void generate_range(const int64_t start, const int64_t end, ObDatumRange &range);
  void get_part_est(ObSSTable &sstable, const ObDatumRange &range, ObPartitionEst &part_est);
private:
  ObDatumRow start_row_;
  ObDatumRow end_row_;
  ObSSTable *ddl_sstable_ptr_array_[1];
};

TestIndexSSTableEstimator::TestIndexSSTableEstimator()
  : TestIndexBlockDataPrepare("Test index sstable estimator")
{
  is_ddl_merge_data_ = true;
  max_row_cnt_ = 150000;
  max_partial_row_cnt_ = 77873;
  partial_kv_start_idx_ = 3;
}
void TestIndexSSTableEstimator::SetUpTestCase()
{
  TestIndexBlockDataPrepare::SetUpTestCase();
}

void TestIndexSSTableEstimator::TearDownTestCase()
{
  TestIndexBlockDataPrepare::TearDownTestCase();
}

void TestIndexSSTableEstimator::SetUp()
{
  TestIndexBlockDataPrepare::SetUp();
  prepare_context();
  ObLSID ls_id(ls_id_);
  ObTabletID tablet_id(tablet_id_);
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));

  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle_));
  ddl_sstable_ptr_array_[0] = &partial_sstable_;
  tablet_handle_.get_obj()->table_store_addr_.get_ptr()->ddl_sstables_.reset();
  tablet_handle_.get_obj()->table_store_addr_.get_ptr()->ddl_sstables_.sstable_array_ = ddl_sstable_ptr_array_;
  tablet_handle_.get_obj()->table_store_addr_.get_ptr()->ddl_sstables_.cnt_ = 1;
  tablet_handle_.get_obj()->table_store_addr_.get_ptr()->ddl_sstables_.is_inited_ = true;
  prepare_query_param(false);
}

void TestIndexSSTableEstimator::TearDown()
{
  destroy_query_param();
  tablet_handle_.get_obj()->table_store_addr_.get_ptr()->ddl_sstables_.reset();
  tablet_handle_.get_obj()->ddl_kv_count_ = 0;
  tablet_handle_.get_obj()->ddl_kvs_ = nullptr;
  tablet_handle_.reset();
  TestIndexBlockDataPrepare::TearDown();
}

void TestIndexSSTableEstimator::prepare_context()
{
  start_row_.init(allocator_, TEST_COLUMN_CNT);
  end_row_.init(allocator_, TEST_COLUMN_CNT);
}

void TestIndexSSTableEstimator::generate_range(
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

void TestIndexSSTableEstimator::get_part_est(
    ObSSTable &sstable, const ObDatumRange &range, ObPartitionEst &part_est)
{
  ObIndexSSTableEstimateContext esti_ctx(tablet_handle_, context_.query_flag_);
  ObIndexBlockScanEstimator estimator(esti_ctx);
  ASSERT_EQ(OB_SUCCESS, estimator.estimate_row_count(sstable, range, part_est));
}

TEST_F(TestIndexSSTableEstimator, estimate_major_sstable_whole_range)
{
  ObDatumRange range;
  range.set_whole_range();

  ObPartitionEst part_est;
  ObPartitionEst ddl_kv_part_est;
  ObPartitionEst ddl_merge_part_est;

  get_part_est(sstable_, range, part_est);
  get_part_est(ddl_memtable_, range, ddl_kv_part_est);
  get_part_est(partial_sstable_, range, ddl_merge_part_est);

  STORAGE_LOG(INFO, "part_est", K(part_est), K(ddl_kv_part_est), K(ddl_merge_part_est));
  ASSERT_EQ(part_est, ddl_merge_part_est);
}

TEST_F(TestIndexSSTableEstimator, estimate_major_sstable_range)
{
  ObDatumRange range;
  generate_range(100, -1, range);
  ObPartitionEst part_est;
  ObPartitionEst ddl_kv_part_est;
  ObPartitionEst ddl_merge_part_est;

  get_part_est(sstable_, range, part_est);
  get_part_est(ddl_memtable_, range, ddl_kv_part_est);
  get_part_est(partial_sstable_, range, ddl_merge_part_est);

  STORAGE_LOG(INFO, "part_est", K(part_est), K(ddl_kv_part_est), K(ddl_merge_part_est));
  ASSERT_EQ(ddl_kv_part_est, ddl_merge_part_est);
  ASSERT_TRUE(part_est.logical_row_count_ > ddl_merge_part_est.logical_row_count_);
}

TEST_F(TestIndexSSTableEstimator, estimate_major_sstable_left_range)
{
  ObDatumRange range;
  generate_range(-1, 100, range);
  ObPartitionEst part_est;
  ObPartitionEst ddl_kv_part_est;
  ObPartitionEst ddl_merge_part_est;

  get_part_est(sstable_, range, part_est);
  get_part_est(ddl_memtable_, range, ddl_kv_part_est);
  get_part_est(partial_sstable_, range, ddl_merge_part_est);

  STORAGE_LOG(INFO, "part_est", K(part_est), K(ddl_kv_part_est), K(ddl_merge_part_est));
  ASSERT_EQ(part_est, ddl_merge_part_est);
}

TEST_F(TestIndexSSTableEstimator, estimate_major_sstable_right_range)
{
  ObDatumRange range;
  generate_range(row_cnt_ - 100, -1, range);
  ObPartitionEst part_est;
  ObPartitionEst ddl_kv_part_est;
  ObPartitionEst ddl_merge_part_est;

  get_part_est(sstable_, range, part_est);
  get_part_est(ddl_memtable_, range, ddl_kv_part_est);
  get_part_est(partial_sstable_, range, ddl_merge_part_est);

  STORAGE_LOG(INFO, "part_est", K(part_est), K(ddl_kv_part_est), K(ddl_merge_part_est));
  ASSERT_EQ(part_est, ddl_merge_part_est);
}

TEST_F(TestIndexSSTableEstimator, estimate_major_sstable_middle_range)
{
  ObDatumRange range;
  generate_range(100, row_cnt_ - 100, range);
  ObPartitionEst part_est;
  ObPartitionEst ddl_kv_part_est;
  ObPartitionEst ddl_merge_part_est;

  get_part_est(sstable_, range, part_est);
  get_part_est(ddl_memtable_, range, ddl_kv_part_est);
  get_part_est(partial_sstable_, range, ddl_merge_part_est);

  STORAGE_LOG(INFO, "part_est", K(part_est), K(ddl_kv_part_est), K(ddl_merge_part_est));
  ASSERT_EQ(ddl_kv_part_est, ddl_merge_part_est);
  ASSERT_TRUE(part_est.logical_row_count_ > ddl_merge_part_est.logical_row_count_);
}

TEST_F(TestIndexSSTableEstimator, estimate_major_sstable_noexist_range)
{
  ObDatumRange range;
  generate_range(row_cnt_, row_cnt_, range);
  ObPartitionEst part_est;
  ObPartitionEst ddl_kv_part_est;
  ObPartitionEst ddl_merge_part_est;

  get_part_est(sstable_, range, part_est);
  get_part_est(ddl_memtable_, range, ddl_kv_part_est);
  get_part_est(partial_sstable_, range, ddl_merge_part_est);

  STORAGE_LOG(INFO, "part_est", K(part_est), K(ddl_kv_part_est), K(ddl_merge_part_est));
  ASSERT_EQ(part_est, ddl_merge_part_est);
}

}  // end namespace unittest
}  // end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_index_sstable_estimator.log*");
  OB_LOGGER.set_file_name("test_index_sstable_estimator.log", true, true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
