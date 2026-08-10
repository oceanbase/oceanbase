/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "test_scan_basic.h"

namespace oceanbase {
namespace storage {

class TestDeleteInsertRefreshTableBugfix : public TestScanBasic, public ::testing::WithParamInterface<bool>
{
public:
  TestDeleteInsertRefreshTableBugfix();
  virtual ~TestDeleteInsertRefreshTableBugfix() {}

  void SetUp();
  void TearDown();
  static void SetUpTestCase();
  static void TearDownTestCase();
};

void TestDeleteInsertRefreshTableBugfix::SetUpTestCase()
{
  TestScanBasic::SetUpTestCase();
}

void TestDeleteInsertRefreshTableBugfix::TearDownTestCase()
{
  TestScanBasic::TearDownTestCase();
}

TestDeleteInsertRefreshTableBugfix::TestDeleteInsertRefreshTableBugfix()
    : TestScanBasic("test_delete_insert_refresh_table_bugfix") {}

void TestDeleteInsertRefreshTableBugfix::SetUp() {
  const bool use_cs_encoding = GetParam();
  row_store_type_ = use_cs_encoding ? CS_ENCODING_ROW_STORE : FLAT_ROW_STORE;
  TestScanBasic::SetUp();
}

void TestDeleteInsertRefreshTableBugfix::TearDown()
{
  TestScanBasic::TearDown();
}

TEST_P(TestDeleteInsertRefreshTableBugfix, test_issue_2026060500116565627)
{
  const int64_t max_batch_size = 4;
  int ret = OB_SUCCESS;
  ObTableStoreIterator table_store_iter;

  ObTableHandleV2 handle1;
  const char *micro_data1[1];
  micro_data1[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -50     DI_VERSION  1      1       INSERT   NORMAL     CLF\n";

  int schema_rowkey_cnt = 1;
  int64_t snapshot_version = 50;
  ObScnRange scn_range;
  scn_range.start_scn_.convert_for_tx(0);
  scn_range.end_scn_.convert_for_tx(50);
  prepare_table_schema(micro_data1, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data1, 1);
  ObTabletCreateSSTableParam param1;
  prepare_create_basic_sst_param(param1, scn_range, ObITable::MAJOR_SSTABLE);
  prepare_create_row_sst_param(param1);
  prepare_data_end_with_param(handle1, param1, 50);
  table_store_iter.add_table(handle1.get_table());
  STORAGE_LOG(INFO, "finish prepare major sstable", K(handle1.get_table()));

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   bigint   bigint      bigint  bigint  flag     flag_type  multi_version_row_flag\n"
      "2        -60      DI_VERSION  2       2       INSERT   NORMAL     CLF\n"
      "3        -60      0           3       3       DELETE   NORMAL     CF\n"
      "3        -55      DI_VERSION  3       3       INSERT   NORMAL     CL\n"
      "4        -60      DI_VERSION  4       4       INSERT   NORMAL     CLF\n"
      "5        -60      DI_VERSION  5       5       INSERT   NORMAL     CLF\n"
      "6        -60      DI_VERSION  6       6       INSERT   NORMAL     CLF\n"
      "7        -60      DI_VERSION  7       7       INSERT   NORMAL     CLF\n"
      "8        -60      DI_VERSION  8       8       INSERT   NORMAL     CLF\n"
      "9        -60      0           9       9       DELETE   NORMAL     CF\n"
      "9        -55      DI_VERSION  9       9       INSERT   NORMAL     CL\n"
      "10       -60      DI_VERSION  10      10      INSERT   NORMAL     CLF\n"
      "11       -60      DI_VERSION  11      11      INSERT   NORMAL     CLF\n"
      "12       -60      DI_VERSION  12      12      INSERT   NORMAL     CLF\n"
      "13       -60      DI_VERSION  13      13      INSERT   NORMAL     CLF\n"
      "14       -60      DI_VERSION  14      14      INSERT   NORMAL     CLF\n"
      "15       -60      0           15      15      DELETE   NORMAL     CF\n"
      "15       -55      DI_VERSION  15      15      INSERT   NORMAL     CL\n"
      "16       -60      DI_VERSION  16      16      INSERT   NORMAL     CLF\n";

  snapshot_version = 60;
  scn_range.start_scn_.convert_for_tx(50);
  scn_range.end_scn_.convert_for_tx(60);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  ObTabletCreateSSTableParam param2;
  prepare_create_basic_sst_param(param2, scn_range, ObITable::MINI_SSTABLE);
  prepare_data_end_with_param(handle2, param2, 60);
  table_store_iter.add_table(handle2.get_table());
  STORAGE_LOG(INFO, "finish prepare mini sstable", K(handle2.get_table()));

  // 修复前
  // mini扫2,3, 切major
  // major扫1, 返回OB_ITER_END, 切mini
  // mini扫4,5,6,7
  // mini扫8,9, 切major
  // refresh table, curr_rowkey=9, major返回OB_ITER_END, 切mini, 扫10,11,12,13
  // mini扫14,15, 切major
  // refresh table, curr_rowkey=9, major返回OB_ITER_END, 切mini, 扫10,11,12,13,
  // mini扫14,15, 切major
  // major返回OB_ITER_END, 切mini, 扫16, 返回OB_ITER_END

  // 修复后（mr268747将need_scan_di_base改成成员）
  // mini扫2,3, 切major
  // major扫1, 返回OB_ITER_END, 切mini
  // mini扫4,5,6,7
  // mini扫8,9,10,11,12
  // refresh table, curr_rowkey=12, mini扫13,14,15, 切major
  // major返回OB_ITER_END, 切mini，扫16, 返回OB_ITER_END
  // refresh table, curr_rowkey=16, 返回OB_ITER_END

  const char *result1 = "bigint   bigint bigint  flag      flag_type\n"
                        "2        2      2       INSERT    NORMAL\n"
                        "1        1      1       INSERT    NORMAL\n"
                        "4        4      4       INSERT    NORMAL\n"
                        "5        5      5       INSERT    NORMAL\n"
                        "6        6      6       INSERT    NORMAL\n"
                        "7        7      7       INSERT    NORMAL\n"
                        "8        8      8       INSERT    NORMAL\n"
                        "10       10     10      INSERT    NORMAL\n"
                        "11       11     11      INSERT    NORMAL\n"
                        "12       12     12      INSERT    NORMAL\n"
                        "13       13     13      INSERT    NORMAL\n"
                        "14       14     14      INSERT    NORMAL\n"
                        "16       16     16      INSERT    NORMAL\n";

  ObDatumRange range;
  range.set_whole_range();
  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 1;
  trans_version_range.multi_version_start_ = 50;
  trans_version_range.snapshot_version_ = INT64_MAX;
  prepare_scan_param(trans_version_range, table_store_iter, true/*is_delete_insert*/, max_batch_size);

  mock_tablet_table_store(table_store_iter);
  ObMultipleScanMerge scan_merge;
  OK(scan_merge.init(access_param_, context_, tablet_read_tables_));
  refresh_iter(scan_merge);
  OK(scan_merge.open(range));
  scan_merge.disable_padding();
  scan_merge.disable_fill_virtual_column();

  int64_t count = 0;
  int64_t total_count = 0;
  ret = OB_SUCCESS;
  ObMockIterator res_iter;
  res_iter.reset();
  ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));

  // mini扫2,3[delete]
  OK(scan_merge.get_next_rows(count, max_batch_size));
  if (count > 0) {
    ObMockScanMergeIterator merge_iter(count);
    OK(merge_iter.init(reinterpret_cast<ObVectorStore *>(scan_merge.block_row_store_),
                       query_allocator_,
                       *access_param_.iter_param_.get_read_info()));
    bool is_equal = res_iter.equals<ObMockScanMergeIterator, ObStoreRow>(merge_iter, false, false, false, true);
    ASSERT_TRUE(is_equal);
    total_count += count;
  }
  ASSERT_EQ(1, total_count);

  // major扫1
  OK(scan_merge.get_next_rows(count, max_batch_size));
  if (count > 0) {
    ObMockScanMergeIterator merge_iter(count);
    OK(merge_iter.init(reinterpret_cast<ObVectorStore *>(scan_merge.block_row_store_),
                       query_allocator_,
                       *access_param_.iter_param_.get_read_info()));
    bool is_equal = res_iter.equals<ObMockScanMergeIterator, ObStoreRow>(merge_iter, false, false, false, true);
    ASSERT_TRUE(is_equal);
    total_count += count;
  }
  ASSERT_EQ(2, total_count);

  // mini扫4,5,6,7
  OK(scan_merge.get_next_rows(count, max_batch_size));
  if (count > 0) {
    ObMockScanMergeIterator merge_iter(count);
    OK(merge_iter.init(reinterpret_cast<ObVectorStore *>(scan_merge.block_row_store_),
                       query_allocator_,
                       *access_param_.iter_param_.get_read_info()));
    bool is_equal = res_iter.equals<ObMockScanMergeIterator, ObStoreRow>(merge_iter, false, false, false, true);
    ASSERT_TRUE(is_equal);
    total_count += count;
  }
  ASSERT_EQ(6, total_count);

  // mini扫8,9[delete],10,11,12
  OK(scan_merge.get_next_rows(count, max_batch_size));
  if (count > 0) {
    ObMockScanMergeIterator merge_iter(count);
    OK(merge_iter.init(reinterpret_cast<ObVectorStore *>(scan_merge.block_row_store_),
                       query_allocator_,
                       *access_param_.iter_param_.get_read_info()));
    bool is_equal = res_iter.equals<ObMockScanMergeIterator, ObStoreRow>(merge_iter, false, false, false, true);
    ASSERT_TRUE(is_equal);
    total_count += count;
  }
  ASSERT_EQ(10, total_count);

  // refresh table
  OK(refresh_table(scan_merge, tablet_read_tables_.tablet_iter_.table_store_iter_));
  STORAGE_LOG(INFO, "refresh tablet iter", K(tablet_read_tables_.tablet_iter_.table_store_iter_));

  // mini扫13,14,15[delete]
  OK(scan_merge.get_next_rows(count, max_batch_size));
  if (count > 0) {
    ObMockScanMergeIterator merge_iter(count);
    OK(merge_iter.init(reinterpret_cast<ObVectorStore *>(scan_merge.block_row_store_),
                       query_allocator_,
                       *access_param_.iter_param_.get_read_info()));
    bool is_equal = res_iter.equals<ObMockScanMergeIterator, ObStoreRow>(merge_iter, false, false, false, true);
    ASSERT_TRUE(is_equal);
    total_count += count;
  }
  ASSERT_EQ(12, total_count);

  // mini扫16, 返回OB_ITER_END
  ASSERT_EQ(OB_ITER_END, scan_merge.get_next_rows(count, max_batch_size));
  if (count > 0) {
    ObMockScanMergeIterator merge_iter(count);
    OK(merge_iter.init(reinterpret_cast<ObVectorStore *>(scan_merge.block_row_store_),
                       query_allocator_,
                       *access_param_.iter_param_.get_read_info()));
    bool is_equal = res_iter.equals<ObMockScanMergeIterator, ObStoreRow>(merge_iter, false, false, false, true);
    ASSERT_TRUE(is_equal);
    total_count += count;
  }
  ASSERT_EQ(13, total_count);

  // refresh table
  OK(refresh_table(scan_merge, tablet_read_tables_.tablet_iter_.table_store_iter_));
  STORAGE_LOG(INFO, "refresh tablet iter", K(tablet_read_tables_.tablet_iter_.table_store_iter_));

  // 扫描剩余数据
  while (OB_SUCC(ret)) {
    ret = scan_merge.get_next_rows(count, max_batch_size);
    if (ret != OB_SUCCESS && ret != OB_ITER_END) {
      STORAGE_LOG(ERROR, "error return value", K(ret), K(count));
      ASSERT_EQ(1, 0);
    }
    if (count > 0) {
      ObMockScanMergeIterator merge_iter(count);
      ASSERT_EQ(OB_SUCCESS, merge_iter.init(reinterpret_cast<ObVectorStore *>(scan_merge.block_row_store_),
                                            query_allocator_,
                                            *access_param_.iter_param_.get_read_info()));
      bool is_equal = res_iter.equals<ObMockScanMergeIterator, ObStoreRow>(merge_iter, false, false, false, true);
      ASSERT_TRUE(is_equal);

      total_count += count;
      STORAGE_LOG(INFO, "get next rows", K(count), K(total_count));
    } else {
      break;
    }
  }
  ASSERT_EQ(13, total_count);

  handle1.reset();
  handle2.reset();
  scan_merge.reset();
}

INSTANTIATE_TEST_CASE_P(
  FlatAndCSEncoding,
  TestDeleteInsertRefreshTableBugfix,
  ::testing::Values(false, true));

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv) {
  system("rm -rf test_delete_insert_refresh_table_bugfix.log* "
         "test_delete_insert_refresh_table_bugfix.rs.log* "
         "test_delete_insert_refresh_table_bugfix.election.log*");
  OB_LOGGER.set_file_name(
      "test_delete_insert_refresh_table_bugfix.log", true, false,
      "test_delete_insert_refresh_table_bugfix.rs.log",
      "test_delete_insert_refresh_table_bugfix.election.log");
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
