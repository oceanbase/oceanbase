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

#include "test_scan_basic.h"

namespace oceanbase {
namespace storage {

class TestIncMajorScan : public TestScanBasic
{
public:
  TestIncMajorScan();
  virtual ~TestIncMajorScan() {}

  void SetUp();
  void TearDown();
  static void SetUpTestCase();
  static void TearDownTestCase();
};

void TestIncMajorScan::SetUpTestCase()
{
  TestScanBasic::SetUpTestCase();
}

void TestIncMajorScan::TearDownTestCase()
{
  TestScanBasic::TearDownTestCase();
}

TestIncMajorScan::TestIncMajorScan()
    : TestScanBasic("test_inc_major_scan") {}

void TestIncMajorScan::SetUp() { TestScanBasic::SetUp(); }

void TestIncMajorScan::TearDown()
{
  TestScanBasic::TearDown();
}

TEST_F(TestIncMajorScan, test_basic_multi_sst_scan)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator table_store_iter;

  ObTableHandleV2 handle1;
  const char *micro_data1[1];
  micro_data1[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -50      0             9       9    INSERT    NORMAL      CLF\n"
      "3        -50      0             9       9    INSERT    NORMAL      CLF\n"
      "5        -50      0             9       9    INSERT    NORMAL      CLF\n"
      "7        -50      0             9       9    INSERT    NORMAL      CLF\n"
      "9        -50      0             9       9    INSERT    NORMAL      CLF\n"
      "11       -50      0             9       9    INSERT    NORMAL      CLF\n"
      "13       -50      0             9       9    INSERT    NORMAL      CLF\n"
      "15       -50      0             9       9    INSERT    NORMAL      CLF\n"
      "17       -50      0             9       9    INSERT    NORMAL      CLF\n";

  int schema_rowkey_cnt = 1;
  int64_t snapshot_version = 50;
  ObScnRange scn_range;
  scn_range.start_scn_.convert_for_tx(0);
  scn_range.end_scn_.convert_for_tx(50);
  prepare_table_schema(micro_data1, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE);
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
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "2        -70      0           19     19    INSERT    NORMAL        CLF\n"
      "4        -70      0           19     19    INSERT    NORMAL        CLF\n"
      "6        -70      0           19     19    INSERT    NORMAL        CLF\n"
      "8        -70      0           19     19    INSERT    NORMAL        CLF\n"
      "10       -70      0           19     19    INSERT    NORMAL        CLF\n"
      "12       -70      0           19     19    INSERT    NORMAL        CLF\n"
      "14       -70      0           19     19    INSERT    NORMAL        CLF\n"
      "16       -70      0           19     19    INSERT    NORMAL        CLF\n";

  snapshot_version = 70;
  scn_range.start_scn_.convert_for_tx(0);
  scn_range.end_scn_.convert_for_tx(70);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  ObTabletCreateSSTableParam param2;
  prepare_create_basic_sst_param(param2, scn_range, ObITable::INC_MAJOR_SSTABLE);
  prepare_create_row_sst_param(param2);
  prepare_create_inc_sst_param(param2, {ObUncommitTxDesc(0, 80, ObUncommitTxDesc::KeyStatus::SQL_SEQ)});
  prepare_data_end_with_param(handle2, param2, 80);
  table_store_iter.add_table(handle2.get_table());
  STORAGE_LOG(INFO, "finish prepare inc major sstable 1", K(handle2.get_table()));

  ObTableHandleV2 handle3;
  const char *micro_data3[1];
  micro_data3[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "18       -90      0           99     99    INSERT    NORMAL        CLF\n"
      "19       -90      0           99     99    INSERT    NORMAL        CLF\n"
      "20       -90      0           99     99    INSERT    NORMAL        CLF\n"
      "21       -90      0           99     99    INSERT    NORMAL        CLF\n"
      "22       -90      0           99     99    INSERT    NORMAL        CLF\n";

  snapshot_version = 90;
  scn_range.start_scn_.convert_for_tx(0);
  scn_range.end_scn_.convert_for_tx(90);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data3, 1);
  ObTabletCreateSSTableParam param3;
  prepare_create_basic_sst_param(param3, scn_range, ObITable::INC_MAJOR_SSTABLE);
  prepare_create_row_sst_param(param3);
  prepare_create_inc_sst_param(param3, {ObUncommitTxDesc(0, 100, ObUncommitTxDesc::KeyStatus::SQL_SEQ)});
  prepare_data_end_with_param(handle3, param3, 100);
  table_store_iter.add_table(handle3.get_table());
  STORAGE_LOG(INFO, "finish prepare inc major sstable 2", K(handle3.get_table()));

  ObTableHandleV2 handle4;
  const char *micro_data4[1];
  micro_data4[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -60      0          100    100    UPDATE    NORMAL        CLF\n"
      "3        -60      0          100    100    UPDATE    NORMAL        CLF\n"
      "13       -60      0          100    100    UPDATE    NORMAL        CLF\n"
      "15       -60      0          100    100    UPDATE    NORMAL        CLF\n"
      "17       -60      0          100    100    UPDATE    NORMAL        CLF\n";

  snapshot_version = 60;
  scn_range.start_scn_.convert_for_tx(50);
  scn_range.end_scn_.convert_for_tx(60);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data4, 1);
  ObTabletCreateSSTableParam param4;
  prepare_create_basic_sst_param(param4, scn_range, ObITable::MINI_SSTABLE);
  prepare_data_end_with_param(handle4, param4, 60);
  table_store_iter.add_table(handle4.get_table());
  STORAGE_LOG(INFO, "finish prepare mini sstable", K(handle4.get_table()));

  ObTableHandleV2 handle5;
  const char *micro_data5[1];
  micro_data5[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "5        -80      0          100    100    UPDATE    NORMAL        CLF\n"
      "7        -80      0          100    100    UPDATE    NORMAL        CLF\n"
      "9        -80      0          100    100    UPDATE    NORMAL        CLF\n"
      "11       -80      0          100    100    UPDATE    NORMAL        CLF\n";

  snapshot_version = 80;
  scn_range.start_scn_.convert_for_tx(70);
  scn_range.end_scn_.convert_for_tx(80);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data5, 1);
  ObTabletCreateSSTableParam param5;
  prepare_create_basic_sst_param(param5, scn_range, ObITable::MINI_SSTABLE);
  prepare_data_end_with_param(handle5, param5, 80);
  table_store_iter.add_table(handle5.get_table());
  STORAGE_LOG(INFO, "finish prepare inc mini sstable 1", K(handle5.get_table()));

  ObTableHandleV2 handle6;
  const char *micro_data6[1];
  micro_data6[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "2        -100     0          100    100    UPDATE    NORMAL        CLF\n"
      "4        -100     0          100    100    UPDATE    NORMAL        CLF\n"
      "6        -100     0          100    100    UPDATE    NORMAL        CLF\n"
      "8        -100     0          100    100    UPDATE    NORMAL        CLF\n"
      "10       -100     0          100    100    UPDATE    NORMAL        CLF\n"
      "12       -100     0          100    100    UPDATE    NORMAL        CLF\n"
      "14       -100     0          100    100    UPDATE    NORMAL        CLF\n"
      "16       -100     0          100    100    UPDATE    NORMAL        CLF\n";

  snapshot_version = 100;
  scn_range.start_scn_.convert_for_tx(90);
  scn_range.end_scn_.convert_for_tx(100);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data6, 1);
  ObTabletCreateSSTableParam param6;
  prepare_create_basic_sst_param(param6, scn_range, ObITable::MINI_SSTABLE);
  prepare_data_end_with_param(handle6, param6, 100);
  table_store_iter.add_table(handle6.get_table());
  STORAGE_LOG(INFO, "finish prepare inc mini sstable 2", K(handle6.get_table()));

  const char *result1 = "bigint   bigint bigint  flag     flag_type\n"
                        "1        100     100    INSERT    NORMAL\n"
                        "2        100     100    INSERT    NORMAL\n"
                        "3        100     100    INSERT    NORMAL\n"
                        "4        100     100    INSERT    NORMAL\n"
                        "5        100     100    INSERT    NORMAL\n"
                        "6        100     100    INSERT    NORMAL\n"
                        "7        100     100    INSERT    NORMAL\n"
                        "8        100     100    INSERT    NORMAL\n"
                        "9        100     100    INSERT    NORMAL\n"
                        "10       100     100    INSERT    NORMAL\n"
                        "11       100     100    INSERT    NORMAL\n"
                        "12       100     100    INSERT    NORMAL\n"
                        "13       100     100    INSERT    NORMAL\n"
                        "14       100     100    INSERT    NORMAL\n"
                        "15       100     100    INSERT    NORMAL\n"
                        "16       100     100    INSERT    NORMAL\n"
                        "17       100     100    INSERT    NORMAL\n"
                        "18       99      99     INSERT    NORMAL\n"
                        "19       99      99     INSERT    NORMAL\n"
                        "20       99      99     INSERT    NORMAL\n"
                        "21       99      99     INSERT    NORMAL\n"
                        "22       99      99     INSERT    NORMAL\n";

  ObDatumRange range;
  range.set_whole_range();
  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 1;
  trans_version_range.multi_version_start_ = 50;
  trans_version_range.snapshot_version_ = INT64_MAX;
  prepare_scan_param(trans_version_range, table_store_iter, false/*is_delete_insert*/);

  mock_tablet_table_store(table_store_iter);
  ObMultipleScanMerge scan_merge;
  ASSERT_EQ(OB_SUCCESS, scan_merge.init(access_param_, context_, get_table_param_));
  refresh_iter(scan_merge);
  ASSERT_EQ(OB_SUCCESS, scan_merge.open(range));
  scan_merge.disable_padding();
  scan_merge.disable_fill_virtual_column();
  int64_t count = 0;
  int64_t total_count = 0;
  ret = OB_SUCCESS;
  ObMockIterator res_iter;
  res_iter.reset();
  ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
  while (OB_SUCC(ret)) {
    ret = scan_merge.get_next_rows(count, SQL_BATCH_SIZE);
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
  ASSERT_EQ(22, total_count);

  handle1.reset();
  handle2.reset();
  handle3.reset();
  handle4.reset();
  handle5.reset();
  handle6.reset();
  scan_merge.reset();
}

TEST_F(TestIncMajorScan, test_out_of_version_inc_major)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator table_store_iter;

  ObTableHandleV2 handle1;
  const char *micro_data1[1];
  micro_data1[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -50      0             9       9    INSERT    NORMAL      CLF\n"
      "3        -50      0             9       9    INSERT    NORMAL      CLF\n"
      "5        -50      0             9       9    INSERT    NORMAL      CLF\n"
      "7        -50      0             9       9    INSERT    NORMAL      CLF\n"
      "9        -50      0             9       9    INSERT    NORMAL      CLF\n";

  int schema_rowkey_cnt = 1;
  int64_t snapshot_version = 50;
  ObScnRange scn_range;
  scn_range.start_scn_.convert_for_tx(0);
  scn_range.end_scn_.convert_for_tx(50);
  prepare_table_schema(micro_data1, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data1, 1);
  ObTabletCreateSSTableParam param1;
  prepare_create_basic_sst_param(param1, scn_range, ObITable::MAJOR_SSTABLE);
  prepare_create_row_sst_param(param1);
  prepare_data_end_with_param(handle1, param1, 50);
  table_store_iter.add_table(handle1.get_table());
  STORAGE_LOG(INFO, "finish prepare major sstable", KPC(handle1.get_table()), K(handle1.get_table()->get_snapshot_version()));

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -40      0          19     19     INSERT    NORMAL        CLF\n"
      "3        -40      0          19     19     INSERT    NORMAL        CLF\n"
      "5        -40      0          19     19     INSERT    NORMAL        CLF\n"
      "7        -40      0          19     19     INSERT    NORMAL        CLF\n";

  snapshot_version = 40;
  scn_range.start_scn_.convert_for_tx(0);
  scn_range.end_scn_.convert_for_tx(40);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  ObTabletCreateSSTableParam param2;
  prepare_create_basic_sst_param(param2, scn_range, ObITable::INC_MAJOR_SSTABLE);
  prepare_create_row_sst_param(param2);
  prepare_create_inc_sst_param(param2, {ObUncommitTxDesc(0, 45, ObUncommitTxDesc::KeyStatus::SQL_SEQ)});
  prepare_data_end_with_param(handle2, param2, 45);
  table_store_iter.add_table(handle2.get_table());
  STORAGE_LOG(INFO, "finish prepare inc major sstable 1");

  ObTableHandleV2 handle3;
  const char *micro_data3[1];
  micro_data3[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "2        -70      0           19     19    INSERT    NORMAL        CLF\n"
      "4        -70      0           19     19    INSERT    NORMAL        CLF\n"
      "6        -70      0           19     19    INSERT    NORMAL        CLF\n"
      "8        -70      0           19     19    INSERT    NORMAL        CLF\n";

  snapshot_version = 70;
  scn_range.start_scn_.convert_for_tx(0);
  scn_range.end_scn_.convert_for_tx(70);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data3, 1);
  ObTabletCreateSSTableParam param3;
  prepare_create_basic_sst_param(param3, scn_range, ObITable::INC_MAJOR_SSTABLE);
  prepare_create_row_sst_param(param3);
  prepare_create_inc_sst_param(param3, {ObUncommitTxDesc(0, 80, ObUncommitTxDesc::KeyStatus::SQL_SEQ)});
  prepare_data_end_with_param(handle3, param3, 80);
  table_store_iter.add_table(handle3.get_table());
  STORAGE_LOG(INFO, "finish prepare inc major sstable 2");

  ObTableHandleV2 handle4;
  const char *micro_data4[1];
  micro_data4[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -60      0          99     99     UPDATE    NORMAL        CLF\n"
      "3        -60      0          99     99     UPDATE    NORMAL        CLF\n"
      "5        -60      0          99     99     UPDATE    NORMAL        CLF\n"
      "7        -60      0          99     99     UPDATE    NORMAL        CLF\n"
      "9        -60      0          99     99     UPDATE    NORMAL        CLF\n";

  snapshot_version = 60;
  scn_range.start_scn_.convert_for_tx(0);
  scn_range.end_scn_.convert_for_tx(60);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data4, 1);
  ObTabletCreateSSTableParam param4;
  prepare_create_basic_sst_param(param4, scn_range, ObITable::MINI_SSTABLE);
  prepare_data_end_with_param(handle4, param4, 60);
  table_store_iter.add_table(handle4.get_table());
  STORAGE_LOG(INFO, "finish prepare inc mini sstable");

  ObTableHandleV2 handle5;
  const char *micro_data5[1];
  micro_data5[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -80      0          100    100    UPDATE    NORMAL        CLF\n"
      "3        -80      0          100    100    UPDATE    NORMAL        CLF\n"
      "5        -80      0          100    100    UPDATE    NORMAL        CLF\n"
      "7        -80      0          100    100    UPDATE    NORMAL        CLF\n"
      "9        -80      0          100    100    UPDATE    NORMAL        CLF\n";

  snapshot_version = 80;
  scn_range.start_scn_.convert_for_tx(60);
  scn_range.end_scn_.convert_for_tx(80);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data5, 1);
  ObTabletCreateSSTableParam param5;
  prepare_create_basic_sst_param(param5, scn_range, ObITable::MINI_SSTABLE);
  prepare_data_end_with_param(handle5, param5, 80);
  table_store_iter.add_table(handle5.get_table());
  STORAGE_LOG(INFO, "finish prepare inc mini sstable");

  const char *result1 = "bigint   bigint bigint  flag     flag_type\n"
                        "1        99       99      INSERT    NORMAL\n"
                        "3        99       99      INSERT    NORMAL\n"
                        "5        99       99      INSERT    NORMAL\n"
                        "7        99       99      INSERT    NORMAL\n"
                        "9        99       99      INSERT    NORMAL\n";

  ObDatumRange range;
  range.set_whole_range();
  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 50;
  trans_version_range.multi_version_start_ = 50;
  trans_version_range.snapshot_version_ = 75;
  prepare_scan_param(trans_version_range, table_store_iter, false/*is_delete_insert*/);

  int64_t count = 0;
  int64_t total_count = 0;
  ret = OB_SUCCESS;
  ObMockIterator res_iter;
  res_iter.reset();
  OK(res_iter.from(result1));

  mock_tablet_table_store(table_store_iter);
  ObMultipleScanMerge scan_merge;
  OK(scan_merge.init(access_param_, context_, get_table_param_));
  refresh_iter(scan_merge);
  OK(scan_merge.open(range));
  scan_merge.disable_padding();
  scan_merge.disable_fill_virtual_column();
  while (OB_SUCC(ret)) {
    ret = scan_merge.get_next_rows(count, SQL_BATCH_SIZE);
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
  ASSERT_EQ(5, total_count);

  handle1.reset();
  handle2.reset();
  handle3.reset();
  handle4.reset();
  handle5.reset();
  scan_merge.reset();
}

TEST_F(TestIncMajorScan, test_out_of_version_minor)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator table_store_iter;

  ObTableHandleV2 handle1;
  const char *micro_data1[1];
  micro_data1[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -50      0             9       9    INSERT    NORMAL      CLF\n"
      "3        -50      0             9       9    INSERT    NORMAL      CLF\n"
      "5        -50      0             9       9    INSERT    NORMAL      CLF\n"
      "7        -50      0             9       9    INSERT    NORMAL      CLF\n"
      "9        -50      0             9       9    INSERT    NORMAL      CLF\n";

  int schema_rowkey_cnt = 1;
  int64_t snapshot_version = 50;
  ObScnRange scn_range;
  scn_range.start_scn_.convert_for_tx(0);
  scn_range.end_scn_.convert_for_tx(50);
  prepare_table_schema(micro_data1, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data1, 1);
  ObTabletCreateSSTableParam param1;
  prepare_create_basic_sst_param(param1, scn_range, ObITable::MAJOR_SSTABLE);
  prepare_create_row_sst_param(param1);
  prepare_data_end_with_param(handle1, param1, 50);
  table_store_iter.add_table(handle1.get_table());
  STORAGE_LOG(INFO, "finish prepare major sstable", KPC(handle1.get_table()), K(handle1.get_table()->get_snapshot_version()));

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -40      0          19     19     UPDATE    NORMAL        CLF\n"
      "3        -40      0          19     19     UPDATE    NORMAL        CLF\n"
      "5        -40      0          19     19     UPDATE    NORMAL        CLF\n"
      "7        -40      0          19     19     UPDATE    NORMAL        CLF\n"
      "9        -40      0          19     19     UPDATE    NORMAL        CLF\n";

  snapshot_version = 40;
  scn_range.start_scn_.convert_for_tx(0);
  scn_range.end_scn_.convert_for_tx(40);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  ObTabletCreateSSTableParam param2;
  prepare_create_basic_sst_param(param2, scn_range, ObITable::MINI_SSTABLE);
  prepare_data_end_with_param(handle2, param2, 40);
  table_store_iter.add_table(handle2.get_table());
  STORAGE_LOG(INFO, "finish prepare mini sstable");

  ObTableHandleV2 handle3;
  const char *micro_data3[1];
  micro_data3[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -60      0          99     99     UPDATE    NORMAL        CLF\n"
      "3        -60      0          99     99     UPDATE    NORMAL        CLF\n"
      "5        -60      0          99     99     UPDATE    NORMAL        CLF\n"
      "7        -60      0          99     99     UPDATE    NORMAL        CLF\n"
      "9        -60      0          99     99     UPDATE    NORMAL        CLF\n";

  snapshot_version = 60;
  scn_range.start_scn_.convert_for_tx(0);
  scn_range.end_scn_.convert_for_tx(60);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data3, 1);
  ObTabletCreateSSTableParam param3;
  prepare_create_basic_sst_param(param3, scn_range, ObITable::MINI_SSTABLE);
  prepare_data_end_with_param(handle3, param3, 60);
  table_store_iter.add_table(handle3.get_table());
  STORAGE_LOG(INFO, "finish prepare mini sstable");

  ObTableHandleV2 handle4;
  const char *micro_data4[1];
  micro_data4[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -80      0          100    100    UPDATE    NORMAL        CLF\n"
      "3        -80      0          100    100    UPDATE    NORMAL        CLF\n"
      "5        -80      0          100    100    UPDATE    NORMAL        CLF\n"
      "7        -80      0          100    100    UPDATE    NORMAL        CLF\n"
      "9        -80      0          100    100    UPDATE    NORMAL        CLF\n";

  snapshot_version = 80;
  scn_range.start_scn_.convert_for_tx(60);
  scn_range.end_scn_.convert_for_tx(80);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data4, 1);
  ObTabletCreateSSTableParam param4;
  prepare_create_basic_sst_param(param4, scn_range, ObITable::MINI_SSTABLE);
  prepare_data_end_with_param(handle4, param4, 80);
  table_store_iter.add_table(handle4.get_table());
  STORAGE_LOG(INFO, "finish prepare inc mini sstable");

  const char *result1 = "bigint   bigint bigint  flag     flag_type\n"
                        "1        99       99      INSERT    NORMAL\n"
                        "3        99       99      INSERT    NORMAL\n"
                        "5        99       99      INSERT    NORMAL\n"
                        "7        99       99      INSERT    NORMAL\n"
                        "9        99       99      INSERT    NORMAL\n";

  ObDatumRange range;
  range.set_whole_range();
  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 50;
  trans_version_range.multi_version_start_ = 50;
  trans_version_range.snapshot_version_ = 75;
  prepare_scan_param(trans_version_range, table_store_iter, false/*is_delete_insert*/);

  int64_t count = 0;
  int64_t total_count = 0;
  ret = OB_SUCCESS;
  ObMockIterator res_iter;
  res_iter.reset();
  OK(res_iter.from(result1));

  mock_tablet_table_store(table_store_iter);
  ObMultipleScanMerge scan_merge;
  OK(scan_merge.init(access_param_, context_, get_table_param_));
  refresh_iter(scan_merge);
  OK(scan_merge.open(range));
  scan_merge.disable_padding();
  scan_merge.disable_fill_virtual_column();
  while (OB_SUCC(ret)) {
    ret = scan_merge.get_next_rows(count, SQL_BATCH_SIZE);
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
  ASSERT_EQ(5, total_count);

  handle1.reset();
  handle2.reset();
  handle3.reset();
  handle4.reset();
  scan_merge.reset();
}

TEST_F(TestIncMajorScan, test_mixed_inc_major_scan)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator table_store_iter;

  ObTableHandleV2 handle1;
  const char *micro_data1[1];
  micro_data1[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -50      0             9       9    INSERT    NORMAL      CLF\n"
      "3        -50      0             9       9    INSERT    NORMAL      CLF\n"
      "5        -50      0             9       9    INSERT    NORMAL      CLF\n"
      "7        -50      0             9       9    INSERT    NORMAL      CLF\n"
      "9        -50      0             9       9    INSERT    NORMAL      CLF\n"
      "11       -50      0             9       9    INSERT    NORMAL      CLF\n"
      "13       -50      0             9       9    INSERT    NORMAL      CLF\n"
      "15       -50      0             9       9    INSERT    NORMAL      CLF\n"
      "17       -50      0             9       9    INSERT    NORMAL      CLF\n";

  int schema_rowkey_cnt = 1;
  int64_t snapshot_version = 50;
  ObScnRange scn_range;
  scn_range.start_scn_.convert_for_tx(0);
  scn_range.end_scn_.convert_for_tx(50);
  prepare_table_schema(micro_data1, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data1, 1);
  ObTabletCreateSSTableParam param1;
  prepare_create_basic_sst_param(param1, scn_range, ObITable::MAJOR_SSTABLE);
  prepare_create_column_sst_param(param1);
  prepare_data_end_with_param(handle1, param1, 50);
  ObTableHandleV2 co_handle1;
  convert_to_co_sstable(handle1, co_handle1);
  table_store_iter.add_table(co_handle1.get_table());
  STORAGE_LOG(INFO, "finish prepare major sstable", KPC(co_handle1.get_table()));

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "2        -70      0           19     19    INSERT    NORMAL        CLF\n"
      "4        -70      0           19     19    INSERT    NORMAL        CLF\n"
      "6        -70      0           19     19    INSERT    NORMAL        CLF\n"
      "8        -70      0           19     19    INSERT    NORMAL        CLF\n"
      "10       -70      0           19     19    INSERT    NORMAL        CLF\n"
      "12       -70      0           19     19    INSERT    NORMAL        CLF\n"
      "14       -70      0           19     19    INSERT    NORMAL        CLF\n"
      "16       -70      0           19     19    INSERT    NORMAL        CLF\n";

  snapshot_version = 70;
  scn_range.start_scn_.convert_for_tx(0);
  scn_range.end_scn_.convert_for_tx(70);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  ObTabletCreateSSTableParam param2;
  prepare_create_basic_sst_param(param2, scn_range, ObITable::INC_MAJOR_SSTABLE);
  prepare_create_column_sst_param(param2);
  prepare_create_inc_sst_param(param2, {ObUncommitTxDesc(0, 80, ObUncommitTxDesc::KeyStatus::SQL_SEQ)});
  prepare_data_end_with_param(handle2, param2, 80);
  table_store_iter.add_table(handle2.get_table());
  STORAGE_LOG(INFO, "finish prepare inc major sstable 1");

  ObTableHandleV2 handle3;
  const char *micro_data3[1];
  micro_data3[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "18       -90      0           99     99    INSERT    NORMAL        CLF\n"
      "19       -90      0           99     99    INSERT    NORMAL        CLF\n"
      "20       -90      0           99     99    INSERT    NORMAL        CLF\n"
      "21       -90      0           99     99    INSERT    NORMAL        CLF\n"
      "22       -90      0           99     99    INSERT    NORMAL        CLF\n";

  snapshot_version = 90;
  scn_range.start_scn_.convert_for_tx(0);
  scn_range.end_scn_.convert_for_tx(90);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data3, 1);
  ObTabletCreateSSTableParam param3;
  prepare_create_basic_sst_param(param3, scn_range, ObITable::INC_MAJOR_SSTABLE);
  prepare_create_row_sst_param(param3);
  prepare_create_inc_sst_param(param3, {ObUncommitTxDesc(0, 100, ObUncommitTxDesc::KeyStatus::SQL_SEQ)});
  prepare_data_end_with_param(handle3, param3, 100);
  table_store_iter.add_table(handle3.get_table());
  STORAGE_LOG(INFO, "finish prepare inc major sstable 2");

  ObTableHandleV2 handle4;
  const char *micro_data4[1];
  micro_data4[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -60      0          100    100    UPDATE    NORMAL        CLF\n"
      "3        -60      0          100    100    UPDATE    NORMAL        CLF\n"
      "13       -60      0          100    100    UPDATE    NORMAL        CLF\n"
      "15       -60      0          100    100    UPDATE    NORMAL        CLF\n"
      "17       -60      0          100    100    UPDATE    NORMAL        CLF\n";

  snapshot_version = 60;
  scn_range.start_scn_.convert_for_tx(50);
  scn_range.end_scn_.convert_for_tx(60);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data4, 1);
  ObTabletCreateSSTableParam param4;
  prepare_create_basic_sst_param(param4, scn_range, ObITable::MINI_SSTABLE);
  prepare_data_end_with_param(handle4, param4, 60);
  table_store_iter.add_table(handle4.get_table());
  STORAGE_LOG(INFO, "finish prepare mini sstable");

  ObTableHandleV2 handle5;
  const char *micro_data5[1];
  micro_data5[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "5        -80      0          100    100    UPDATE    NORMAL        CLF\n"
      "7        -80      0          100    100    UPDATE    NORMAL        CLF\n"
      "9        -80      0          100    100    UPDATE    NORMAL        CLF\n"
      "11       -80      0          100    100    UPDATE    NORMAL        CLF\n";

  snapshot_version = 80;
  scn_range.start_scn_.convert_for_tx(70);
  scn_range.end_scn_.convert_for_tx(80);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data5, 1);
  ObTabletCreateSSTableParam param5;
  prepare_create_basic_sst_param(param5, scn_range, ObITable::MINI_SSTABLE);
  prepare_data_end_with_param(handle5, param5, 80);
  table_store_iter.add_table(handle5.get_table());
  STORAGE_LOG(INFO, "finish prepare inc mini sstable 1");

  ObTableHandleV2 handle6;
  const char *micro_data6[1];
  micro_data6[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "2        -100     0          100    100    UPDATE    NORMAL        CLF\n"
      "4        -100     0          100    100    UPDATE    NORMAL        CLF\n"
      "6        -100     0          100    100    UPDATE    NORMAL        CLF\n"
      "8        -100     0          100    100    UPDATE    NORMAL        CLF\n"
      "10       -100     0          100    100    UPDATE    NORMAL        CLF\n"
      "12       -100     0          100    100    UPDATE    NORMAL        CLF\n"
      "14       -100     0          100    100    UPDATE    NORMAL        CLF\n"
      "16       -100     0          100    100    UPDATE    NORMAL        CLF\n";

  snapshot_version = 100;
  scn_range.start_scn_.convert_for_tx(90);
  scn_range.end_scn_.convert_for_tx(100);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data6, 1);
  ObTabletCreateSSTableParam param6;
  prepare_create_basic_sst_param(param6, scn_range, ObITable::MINI_SSTABLE);
  prepare_data_end_with_param(handle6, param6, 100);
  table_store_iter.add_table(handle6.get_table());
  STORAGE_LOG(INFO, "finish prepare inc mini sstable 2");

  const char *result1 = "bigint   bigint bigint  flag     flag_type\n"
                        "1        100     100    INSERT    NORMAL\n"
                        "2        100     100    INSERT    NORMAL\n"
                        "3        100     100    INSERT    NORMAL\n"
                        "4        100     100    INSERT    NORMAL\n"
                        "5        100     100    INSERT    NORMAL\n"
                        "6        100     100    INSERT    NORMAL\n"
                        "7        100     100    INSERT    NORMAL\n"
                        "8        100     100    INSERT    NORMAL\n"
                        "9        100     100    INSERT    NORMAL\n"
                        "10       100     100    INSERT    NORMAL\n"
                        "11       100     100    INSERT    NORMAL\n"
                        "12       100     100    INSERT    NORMAL\n"
                        "13       100     100    INSERT    NORMAL\n"
                        "14       100     100    INSERT    NORMAL\n"
                        "15       100     100    INSERT    NORMAL\n"
                        "16       100     100    INSERT    NORMAL\n"
                        "17       100     100    INSERT    NORMAL\n"
                        "18       99      99     INSERT    NORMAL\n"
                        "19       99      99     INSERT    NORMAL\n"
                        "20       99      99     INSERT    NORMAL\n"
                        "21       99      99     INSERT    NORMAL\n"
                        "22       99      99     INSERT    NORMAL\n";

  ObDatumRange range;
  range.set_whole_range();
  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 1;
  trans_version_range.multi_version_start_ = 50;
  trans_version_range.snapshot_version_ = INT64_MAX;
  prepare_scan_param(trans_version_range, table_store_iter, false/*is_delete_insert*/);

  mock_tablet_table_store(table_store_iter);
  ObMultipleScanMerge scan_merge;
  ASSERT_EQ(OB_SUCCESS, scan_merge.init(access_param_, context_, get_table_param_));
  refresh_iter(scan_merge);
  ASSERT_EQ(OB_SUCCESS, scan_merge.open(range));
  scan_merge.disable_padding();
  scan_merge.disable_fill_virtual_column();
  int64_t count = 0;
  int64_t total_count = 0;
  ret = OB_SUCCESS;
  ObMockIterator res_iter;
  res_iter.reset();
  ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
  while (OB_SUCC(ret)) {
    ret = scan_merge.get_next_rows(count, SQL_BATCH_SIZE);
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
  ASSERT_EQ(22, total_count);

  handle1.reset();
  handle2.reset();
  handle3.reset();
  handle4.reset();
  handle5.reset();
  handle6.reset();
  scan_merge.reset();
}

TEST_F(TestIncMajorScan, test_fresh_table_empty_range)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator table_store_iter;

  ObTableHandleV2 handle1;
  const char *micro_data1[1];
  micro_data1[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -50      0             9       9    INSERT    NORMAL      CLF\n"
      "3        -50      0             9       9    INSERT    NORMAL      CLF\n"
      "5        -50      0             9       9    INSERT    NORMAL      CLF\n"
      "7        -50      0             9       9    INSERT    NORMAL      CLF\n"
      "9        -50      0             9       9    INSERT    NORMAL      CLF\n";

  int schema_rowkey_cnt = 1;
  int64_t snapshot_version = 50;
  ObScnRange scn_range;
  scn_range.start_scn_.convert_for_tx(0);
  scn_range.end_scn_.convert_for_tx(50);
  prepare_table_schema(micro_data1, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data1, 1);
  ObTabletCreateSSTableParam param1;
  prepare_create_basic_sst_param(param1, scn_range, ObITable::MAJOR_SSTABLE);
  prepare_create_row_sst_param(param1);
  prepare_data_end_with_param(handle1, param1, 50);
  table_store_iter.add_table(handle1.get_table());
  STORAGE_LOG(INFO, "finish prepare major sstable", KPC(handle1.get_table()), K(handle1.get_table()->get_snapshot_version()));

  const char *result1 = "bigint   bigint bigint  flag     flag_type\n"
                        "1        9       9      INSERT    NORMAL\n"
                        "3        9       9      INSERT    NORMAL\n"
                        "5        9       9      INSERT    NORMAL\n"
                        "7        9       9      INSERT    NORMAL\n";

  ObDatumRange range;
  ObSEArray<ObDatumRange, 8> ranges;
  generate_range(1, 3, range);
  ranges.push_back(range);
  generate_range(4, 7, range);
  ranges.push_back(range);
  STORAGE_LOG(INFO, "generate ranges", K(ranges));
  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 1;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.snapshot_version_ = INT64_MAX;
  prepare_scan_param(trans_version_range, table_store_iter, false/*is_delete_insert*/);

  int64_t count = 0;
  int64_t total_count = 0;
  ret = OB_SUCCESS;
  ObMockIterator res_iter;
  res_iter.reset();
  OK(res_iter.from(result1));

  mock_tablet_table_store(table_store_iter);
  ObMultipleMultiScanMerge multi_scan_merge;
  OK(multi_scan_merge.init(access_param_, context_, get_table_param_));
  refresh_iter(multi_scan_merge);
  OK(multi_scan_merge.open(ranges));
  multi_scan_merge.disable_padding();
  multi_scan_merge.disable_fill_virtual_column();
  while (OB_SUCC(ret)) {
    ret = multi_scan_merge.get_next_rows(count, 4);
    if (ret != OB_SUCCESS && ret != OB_ITER_END) {
      STORAGE_LOG(ERROR, "error return value", K(ret), K(count));
      ASSERT_EQ(1, 0);
    }
    if (count > 0) {
      ObMockScanMergeIterator merge_iter(count);
      ASSERT_EQ(OB_SUCCESS, merge_iter.init(reinterpret_cast<ObVectorStore *>(multi_scan_merge.block_row_store_),
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
  OK(refresh_table(multi_scan_merge, get_table_param_.tablet_iter_.table_store_iter_));
  ASSERT_EQ(OB_ITER_END, multi_scan_merge.get_next_rows(count, 4));
  ASSERT_EQ(4, total_count);

  handle1.reset();
  multi_scan_merge.reset();
}

// TEST_F(TestIncMajorScan, test_multi_inc_refresh_table)
// {
//   int ret = OB_SUCCESS;
//   ObTableStoreIterator table_store_iter;

//   ObTableHandleV2 handle1;
//   const char *micro_data1[1];
//   micro_data1[0] =
//       "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
//       "1        -50      0             9       9    INSERT    NORMAL      CLF\n"
//       "3        -50      0             9       9    INSERT    NORMAL      CLF\n"
//       "5        -50      0             9       9    INSERT    NORMAL      CLF\n"
//       "7        -50      0             9       9    INSERT    NORMAL      CLF\n"
//       "9        -50      0             9       9    INSERT    NORMAL      CLF\n";

//   int schema_rowkey_cnt = 1;
//   int64_t snapshot_version = 50;
//   ObScnRange scn_range;
//   scn_range.start_scn_.convert_for_tx(0);
//   scn_range.end_scn_.convert_for_tx(50);
//   prepare_table_schema(micro_data1, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE);
//   reset_writer(snapshot_version);
//   prepare_one_macro(micro_data1, 1);
//   ObTabletCreateSSTableParam param1;
//   prepare_create_basic_sst_param(param1, scn_range, ObITable::MAJOR_SSTABLE);
//   prepare_create_row_sst_param(param1);
//   prepare_data_end_with_param(handle1, param1, ObITable::MAJOR_SSTABLE, 50);
//   table_store_iter.add_table(handle1.get_table());
//   STORAGE_LOG(INFO, "finish prepare major sstable", KPC(handle1.get_table()));

//   ObTableHandleV2 handle2;
//   const char *micro_data2[1];
//   micro_data2[0] =
//       "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
//       "2        -70      0           19     19    INSERT    NORMAL        CLF\n"
//       "4        -70      0           19     19    INSERT    NORMAL        CLF\n"
//       "6        -70      0           19     19    INSERT    NORMAL        CLF\n"
//       "8        -70      0           19     19    INSERT    NORMAL        CLF\n";

//   snapshot_version = 70;
//   scn_range.start_scn_.convert_for_tx(0);
//   scn_range.end_scn_.convert_for_tx(70);
//   reset_writer(snapshot_version);
//   prepare_one_macro(micro_data2, 1);
//   ObTabletCreateSSTableParam param2;
//   prepare_create_basic_sst_param(param2, scn_range, ObITable::INC_MAJOR_SSTABLE);
//   prepare_create_row_sst_param(param2);
//   // TODO: zhanghuidong.zhd, make adaptation after refactoring ObMetaUncommitTxInfo
//   prepare_create_inc_sst_param(param2, {ObUncommitTxDesc(0, 80, ObUncommitTxDesc::KeyStatus::SQL_SEQ)});
//   prepare_data_end_with_param(handle2, param2, ObITable::INC_MAJOR_SSTABLE, 80);
//   table_store_iter.add_table(handle2.get_table());
//   STORAGE_LOG(INFO, "finish prepare inc major sstable 1", KPC(handle2.get_table()));

//   ObTableHandleV2 handle3;
//   const char *micro_data3[1];
//   micro_data3[0] =
//       "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
//       "10       -90      0           99     99    INSERT    NORMAL        CLF\n"
//       "11       -90      0           99     99    INSERT    NORMAL        CLF\n"
//       "12       -90      0           99     99    INSERT    NORMAL        CLF\n";

//   snapshot_version = 90;
//   scn_range.start_scn_.convert_for_tx(0);
//   scn_range.end_scn_.convert_for_tx(90);
//   reset_writer(snapshot_version);
//   prepare_one_macro(micro_data3, 1);
//   ObTabletCreateSSTableParam param3;
//   prepare_create_basic_sst_param(param3, scn_range, ObITable::INC_MAJOR_SSTABLE);
//   prepare_create_row_sst_param(param3);
//   prepare_create_inc_sst_param(param3, {ObUncommitTxDesc(0, 100, ObUncommitTxDesc::KeyStatus::SQL_SEQ)});
//   prepare_data_end_with_param(handle3, param3, ObITable::INC_MAJOR_SSTABLE, 100);
//   table_store_iter.add_table(handle3.get_table());
//   STORAGE_LOG(INFO, "finish prepare inc major sstable 2", KPC(handle3.get_table()));

//   ObTableHandleV2 handle4;
//   const char *micro_data4[1];
//   micro_data4[0] =
//       "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
//       "2        -70      0           19     19    INSERT    NORMAL        CLF\n"
//       "4        -70      0           19     19    INSERT    NORMAL        CLF\n"
//       "6        -70      0           19     19    INSERT    NORMAL        CLF\n"
//       "8        -70      0           19     19    INSERT    NORMAL        CLF\n"
//       "10       -90      0           99     99    INSERT    NORMAL        CLF\n"
//       "11       -90      0           99     99    INSERT    NORMAL        CLF\n"
//       "12       -90      0           99     99    INSERT    NORMAL        CLF\n";
//   snapshot_version = 90;
//   scn_range.start_scn_.convert_for_tx(70);
//   scn_range.end_scn_.convert_for_tx(90);
//   reset_writer(snapshot_version);
//   prepare_one_macro(micro_data4, 1);
//   ObTabletCreateSSTableParam param4;
//   prepare_create_basic_sst_param(param4, scn_range, ObITable::INC_MAJOR_SSTABLE);
//   prepare_create_row_sst_param(param4);
//   prepare_create_inc_sst_param(param4, {ObUncommitTxDesc(70, 80, ObUncommitTxDesc::KeyStatus::SQL_SEQ),
//                                         ObUncommitTxDesc(90, 100, ObUncommitTxDesc::KeyStatus::SQL_SEQ)});
//   prepare_data_end_with_param(handle4, param4, ObITable::INC_MAJOR_SSTABLE, 100);
//   STORAGE_LOG(INFO, "finish prepare merged inc major sstable", KPC(handle4.get_table()));

//   const char *result1 = "bigint   bigint bigint  flag     flag_type\n"
//                         "1        9       9      INSERT    NORMAL\n"
//                         "2        19      19     INSERT    NORMAL\n"
//                         "3        9       9      INSERT    NORMAL\n"
//                         "4        19      19     INSERT    NORMAL\n"
//                         "5        9       9      INSERT    NORMAL\n"
//                         "6        19      19     INSERT    NORMAL\n"
//                         "7        9       9      INSERT    NORMAL\n"
//                         "8        19      19     INSERT    NORMAL\n"
//                         "9        9       9      INSERT    NORMAL\n"
//                         "10       99      99     INSERT    NORMAL\n"
//                         "11       99      99     INSERT    NORMAL\n"
//                         "12       99      99     INSERT    NORMAL\n";

//   ObDatumRange range;
//   range.set_whole_range();
//   ObVersionRange trans_version_range;
//   trans_version_range.base_version_ = 1;
//   trans_version_range.multi_version_start_ = 50;
//   trans_version_range.snapshot_version_ = INT64_MAX;
//   prepare_scan_param(trans_version_range, table_store_iter, false/*is_delete_insert*/);

//   ObMultipleScanMerge scan_merge;
//   ASSERT_EQ(OB_SUCCESS, scan_merge.init(access_param_, context_, get_table_param_));
//   ASSERT_EQ(OB_SUCCESS, scan_merge.open(range));
//   scan_merge.disable_padding();
//   scan_merge.disable_fill_virtual_column();
//   int64_t count = 0;
//   ret = OB_SUCCESS;
//   ObMockIterator res_iter;
//   res_iter.reset();
//   ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
//   OK(scan_merge.get_next_rows(count, 9));
//   ASSERT_EQ(9, count);
//   ObMockScanMergeIterator merge_iter1(count);
//   ASSERT_EQ(OB_SUCCESS, merge_iter1.init(reinterpret_cast<ObVectorStore *>(scan_merge.block_row_store_),
//                                             query_allocator_,
//                                             *access_param_.iter_param_.get_read_info()));
//   bool is_equal = res_iter.equals<ObMockScanMergeIterator, ObStoreRow>(merge_iter1, false, false, false, true);
//   ASSERT_TRUE(is_equal);
//   STORAGE_LOG(INFO, "get next rows", K(count));
//   ObLSID ls_id(ls_id_);
//   ObTabletID tablet_id(tablet_id_);
//   ObLSHandle ls_handle;
//   ObLSService *ls_svr = MTL(ObLSService*);
//   ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));

//   ObTabletHandle tablet_handle;
//   void *ptr = nullptr;
//   ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle));
//   ObTablet *tablet = tablet_handle.get_obj();
//   STORAGE_LOG(INFO, "check table store", K(ret), KPC(tablet->table_store_addr_.get_ptr()));
//   OK(inc_major_sst_merge({0, 1}, handle4));
//   OK(refresh_table(scan_merge, get_table_param_.tablet_iter_.table_store_iter_));
//   STORAGE_LOG(INFO, "inc major sst merge and refresh done", K(get_table_param_.tablet_iter_.table_store_iter_), K_(memstore_retired));
//   OK(scan_merge.get_next_rows(count, 3));
//   ASSERT_EQ(3, count);
//   ObMockScanMergeIterator merge_iter2(count);
//   ASSERT_EQ(OB_SUCCESS, merge_iter2.init(reinterpret_cast<ObVectorStore *>(scan_merge.block_row_store_),
//                                             query_allocator_,
//                                             *access_param_.iter_param_.get_read_info()));
//   is_equal = res_iter.equals<ObMockScanMergeIterator, ObStoreRow>(merge_iter2, false, false, false, true);
//   ASSERT_TRUE(is_equal);
//   STORAGE_LOG(INFO, "get next rows", K(count));
//   handle1.reset();
//   handle2.reset();
//   handle3.reset();
//   handle4.reset();
//   scan_merge.reset();
// }

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv) {
  system("rm -rf test_inc_major_scan.log*");
  OB_LOGGER.set_file_name("test_inc_major_scan.log");
  OB_LOGGER.set_log_level("INFO");
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
