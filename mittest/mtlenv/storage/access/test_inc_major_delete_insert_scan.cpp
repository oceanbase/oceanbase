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

class TestIncMajorDeleteInsertScan : public TestScanBasic
{
public:
  TestIncMajorDeleteInsertScan();
  virtual ~TestIncMajorDeleteInsertScan() {}

  void SetUp();
  void TearDown();
  static void SetUpTestCase();
  static void TearDownTestCase();
};

void TestIncMajorDeleteInsertScan::SetUpTestCase()
{
  TestScanBasic::SetUpTestCase();
}

void TestIncMajorDeleteInsertScan::TearDownTestCase()
{
  TestScanBasic::TearDownTestCase();
}

TestIncMajorDeleteInsertScan::TestIncMajorDeleteInsertScan()
    : TestScanBasic("test_inc_major_delete_insert_scan") {}

void TestIncMajorDeleteInsertScan::SetUp() { TestScanBasic::SetUp(); }

void TestIncMajorDeleteInsertScan::TearDown()
{
  TestScanBasic::TearDown();
}

TEST_F(TestIncMajorDeleteInsertScan, test_mixed_sst_scan)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator table_store_iter;

  ObTableHandleV2 handle1;
  const char *micro_data1[1];
  micro_data1[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "3        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "5        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "7        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "9        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "11       -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "13       -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "15       -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "17       -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n";

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
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "2        -70      DI_VERSION   19     19    INSERT    NORMAL        CLF\n"
      "4        -70      DI_VERSION   19     19    INSERT    NORMAL        CLF\n"
      "6        -70      DI_VERSION   19     19    INSERT    NORMAL        CLF\n"
      "8        -70      DI_VERSION   19     19    INSERT    NORMAL        CLF\n"
      "10       -70      DI_VERSION   19     19    INSERT    NORMAL        CLF\n"
      "12       -70      DI_VERSION   19     19    INSERT    NORMAL        CLF\n"
      "14       -70      DI_VERSION   19     19    INSERT    NORMAL        CLF\n"
      "16       -70      DI_VERSION   19     19    INSERT    NORMAL        CLF\n";

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
      "18       -90      DI_VERSION  99     99    INSERT    NORMAL        CLF\n"
      "19       -90      DI_VERSION  99     99    INSERT    NORMAL        CLF\n"
      "20       -90      DI_VERSION  99     99    INSERT    NORMAL        CLF\n"
      "21       -90      DI_VERSION  99     99    INSERT    NORMAL        CLF\n"
      "22       -90      DI_VERSION  99     99    INSERT    NORMAL        CLF\n";

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
      "1        -60      DI_VERSION  100    100    INSERT    NORMAL        CF\n"
      "1        -60      0            9      9     DELETE    NORMAL        CL\n"
      "3        -60      DI_VERSION  100    100    INSERT    NORMAL        CF\n"
      "3        -60      0            9      9     DELETE    NORMAL        CL\n"
      "13       -60      DI_VERSION  100    100    INSERT    NORMAL        CF\n"
      "13       -60      0            9      9     DELETE    NORMAL        CL\n"
      "15       -60      DI_VERSION  100    100    INSERT    NORMAL        CF\n"
      "15       -60      0            9      9     DELETE    NORMAL        CL\n"
      "17       -60      DI_VERSION  100    100    INSERT    NORMAL        CF\n"
      "17       -60      0            9      9     DELETE    NORMAL        CL\n";

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
      "5        -80      DI_VERSION  100    100    INSERT    NORMAL        CF\n"
      "5        -80      0            9      9     DELETE    NORMAL        CL\n"
      "7        -80      DI_VERSION  100    100    INSERT    NORMAL        CF\n"
      "7        -80      0            9      9     DELETE    NORMAL        CL\n"
      "9        -80      DI_VERSION  100    100    INSERT    NORMAL        CF\n"
      "9        -80      0            9      9     DELETE    NORMAL        CL\n"
      "11       -80      DI_VERSION  100    100    INSERT    NORMAL        CF\n"
      "11       -80      0            9      9     DELETE    NORMAL        CL\n";

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
      "2        -100     DI_VERSION   100    100    INSERT    NORMAL        CF\n"
      "2        -100     0            19     19     DELETE    NORMAL        CL\n"
      "4        -100     DI_VERSION   100    100    INSERT    NORMAL        CF\n"
      "4        -100     0            19     19     DELETE    NORMAL        CL\n"
      "6        -100     DI_VERSION   100    100    INSERT    NORMAL        CF\n"
      "6        -100     0            19     19     DELETE    NORMAL        CL\n"
      "8        -100     DI_VERSION   100    100    INSERT    NORMAL        CF\n"
      "8        -100     0            19     19     DELETE    NORMAL        CL\n"
      "10       -100     DI_VERSION   100    100    INSERT    NORMAL        CF\n"
      "10       -100     0            19     19     DELETE    NORMAL        CL\n"
      "12       -100     DI_VERSION   100    100    INSERT    NORMAL        CF\n"
      "12       -100     0            19     19     DELETE    NORMAL        CL\n"
      "14       -100     DI_VERSION   100    100    INSERT    NORMAL        CF\n"
      "14       -100     0            19     19     DELETE    NORMAL        CL\n"
      "16       -100     DI_VERSION   100    100    INSERT    NORMAL        CF\n"
      "16       -100     0            19     19     DELETE    NORMAL        CL\n";

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
  prepare_scan_param(trans_version_range, table_store_iter, true/*is_delete_insert*/);

  mock_tablet_table_store(table_store_iter);
  ObMultipleScanMerge scan_merge;
  OK(scan_merge.init(access_param_, context_, get_table_param_));
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

TEST_F(TestIncMajorDeleteInsertScan, test_out_of_version_inc_major)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator table_store_iter;

  ObTableHandleV2 handle1;
  const char *micro_data1[1];
  micro_data1[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "3        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "5        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "7        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "9        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n";

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
  STORAGE_LOG(INFO, "finish prepare major sstable", KPC(handle1.get_table()), K(handle1.get_table()->get_snapshot_version()));

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -40      DI_VERSION    19     19     INSERT    NORMAL        CLF\n"
      "3        -40      DI_VERSION    19     19     INSERT    NORMAL        CLF\n"
      "5        -40      DI_VERSION    19     19     INSERT    NORMAL        CLF\n"
      "7        -40      DI_VERSION    19     19     INSERT    NORMAL        CLF\n";

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
      "2        -70      DI_VERSION    19     19    INSERT    NORMAL        CLF\n"
      "4        -70      DI_VERSION    19     19    INSERT    NORMAL        CLF\n"
      "6        -70      DI_VERSION    19     19    INSERT    NORMAL        CLF\n"
      "8        -70      DI_VERSION    19     19    INSERT    NORMAL        CLF\n";

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
      "1        -60      DI_VERSION    99     99     INSERT    NORMAL        CF\n"
      "1        -60      0             9      9      DELETE    NORMAL        CL\n"
      "3        -60      DI_VERSION    99     99     INSERT    NORMAL        CF\n"
      "3        -60      0             9      9      DELETE    NORMAL        CL\n"
      "5        -60      DI_VERSION    99     99     INSERT    NORMAL        CF\n"
      "5        -60      0             9      9      DELETE    NORMAL        CL\n"
      "7        -60      DI_VERSION    99     99     INSERT    NORMAL        CF\n"
      "7        -60      0             9      9      DELETE    NORMAL        CL\n"
      "9        -60      DI_VERSION    99     99     INSERT    NORMAL        CF\n"
      "9        -60      0             9      9      DELETE    NORMAL        CL\n";

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
      "1        -80      DI_VERSION    100    100    INSERT    NORMAL        CF\n"
      "1        -80      0             99     99     DELETE    NORMAL        CL\n"
      "3        -80      DI_VERSION    100    100    INSERT    NORMAL        CF\n"
      "3        -80      0             99     99     DELETE    NORMAL        CL\n"
      "5        -80      DI_VERSION    100    100    INSERT    NORMAL        CF\n"
      "5        -80      0             99     99     DELETE    NORMAL        CL\n"
      "7        -80      DI_VERSION    100    100    INSERT    NORMAL        CF\n"
      "7        -80      0             99     99     DELETE    NORMAL        CL\n"
      "9        -80      DI_VERSION    100    100    INSERT    NORMAL        CF\n"
      "9        -80      0             99     99     DELETE    NORMAL        CL\n";

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
  prepare_scan_param(trans_version_range, table_store_iter, true/*is_delete_insert*/);

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

TEST_F(TestIncMajorDeleteInsertScan, test_multi_inc_major_scan)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator table_store_iter;

  ObTableHandleV2 handle1;
  const char *micro_data1[1];
  micro_data1[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "3        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "5        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "7        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "9        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n";

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
  ObTableHandleV2 co_handle1;
  convert_to_co_sstable(handle1, co_handle1);
  table_store_iter.add_table(co_handle1.get_table());
  STORAGE_LOG(INFO, "finish prepare major sstable", KPC(co_handle1.get_table()));

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "2        -60      DI_VERSION   19     19    INSERT    NORMAL        CLF\n"
      "4        -60      DI_VERSION   19     19    INSERT    NORMAL        CLF\n"
      "6        -60      DI_VERSION   19     19    INSERT    NORMAL        CLF\n"
      "8        -60      DI_VERSION   19     19    INSERT    NORMAL        CLF\n";

  snapshot_version = 60;
  scn_range.start_scn_.convert_for_tx(0);
  scn_range.end_scn_.convert_for_tx(60);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  ObTabletCreateSSTableParam param2;
  prepare_create_basic_sst_param(param2, scn_range, ObITable::INC_MAJOR_SSTABLE);
  prepare_create_row_sst_param(param2);
  prepare_create_inc_sst_param(param2, {ObUncommitTxDesc(0, 70, ObUncommitTxDesc::KeyStatus::SQL_SEQ)});
  prepare_data_end_with_param(handle2, param2, 70);
  table_store_iter.add_table(handle2.get_table());
  STORAGE_LOG(INFO, "finish prepare inc major sstable 1", KPC(handle2.get_table()));

  ObTableHandleV2 handle3;
  const char *micro_data3[1];
  micro_data3[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "18       -70      DI_VERSION   99     99    INSERT    NORMAL        CLF\n"
      "19       -70      DI_VERSION   99     99    INSERT    NORMAL        CLF\n"
      "20       -70      DI_VERSION   99     99    INSERT    NORMAL        CLF\n"
      "21       -70      DI_VERSION   99     99    INSERT    NORMAL        CLF\n"
      "22       -70      DI_VERSION   99     99    INSERT    NORMAL        CLF\n";

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
  STORAGE_LOG(INFO, "finish prepare inc major sstable 2", KPC(handle3.get_table()));

  ObTableHandleV2 handle4;
  const char *micro_data4[1];
  micro_data4[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "11       -80      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "13       -80      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "15       -80      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "17       -80      DI_VERSION    9       9    INSERT    NORMAL      CLF\n";

  snapshot_version = 80;
  scn_range.start_scn_.convert_for_tx(0);
  scn_range.end_scn_.convert_for_tx(80);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data4, 1);
  ObTabletCreateSSTableParam param4;
  prepare_create_basic_sst_param(param4, scn_range, ObITable::INC_MAJOR_SSTABLE);
  prepare_create_column_sst_param(param4);
  prepare_create_inc_sst_param(param4, {ObUncommitTxDesc(0, 90, ObUncommitTxDesc::KeyStatus::SQL_SEQ)});
  prepare_data_end_with_param(handle4, param4, 90);
  table_store_iter.add_table(handle4.get_table());
  STORAGE_LOG(INFO, "finish prepare inc major sstable 3", KPC(handle4.get_table()));

  ObTableHandleV2 handle5;
  const char *micro_data5[1];
  micro_data5[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "10       -90      DI_VERSION   19     19    INSERT    NORMAL        CLF\n"
      "12       -90      DI_VERSION   19     19    INSERT    NORMAL        CLF\n"
      "14       -90      DI_VERSION   19     19    INSERT    NORMAL        CLF\n"
      "16       -90      DI_VERSION   19     19    INSERT    NORMAL        CLF\n";

  snapshot_version = 90;
  scn_range.start_scn_.convert_for_tx(0);
  scn_range.end_scn_.convert_for_tx(90);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data5, 1);
  ObTabletCreateSSTableParam param5;
  prepare_create_basic_sst_param(param5, scn_range, ObITable::INC_MAJOR_SSTABLE);
  prepare_create_column_sst_param(param5);
  prepare_create_inc_sst_param(param5, {ObUncommitTxDesc(0, 100, ObUncommitTxDesc::KeyStatus::SQL_SEQ)});
  prepare_data_end_with_param(handle5, param5, 100);
  table_store_iter.add_table(handle5.get_table());
  STORAGE_LOG(INFO, "finish prepare inc major sstable 4", KPC(handle5.get_table()));

  ObTableHandleV2 handle6;
  const char *micro_data6[1];
  micro_data6[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "5        -100     DI_VERSION   200    200    INSERT    NORMAL        CF\n"
      "5        -100     0          9      9      DELETE    NORMAL        CL\n"
      "15       -100     DI_VERSION   200    200    INSERT    NORMAL        CF\n"
      "15       -100     0          9      9      DELETE    NORMAL        CL\n"
      "18       -100     DI_VERSION   200    200    INSERT    NORMAL        CF\n"
      "18       -100     0          99     99     DELETE    NORMAL        CL\n"
      "20       -100     DI_VERSION   200    200    INSERT    NORMAL        CF\n"
      "20       -100     0          99     99     DELETE    NORMAL        CL\n";

  snapshot_version = 100;
  scn_range.start_scn_.convert_for_tx(50);
  scn_range.end_scn_.convert_for_tx(100);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data6, 1);
  ObTabletCreateSSTableParam param6;
  prepare_create_basic_sst_param(param6, scn_range, ObITable::MINI_SSTABLE);
  prepare_data_end_with_param(handle6, param6, 100);
  table_store_iter.add_table(handle6.get_table());
  STORAGE_LOG(INFO, "finish prepare mini sstable", KPC(handle6.get_table()));

  const char *result1 = "bigint   bigint bigint  flag     flag_type\n"
                        "5        200     200    INSERT    NORMAL\n"
                        "1        9       9      INSERT    NORMAL\n"
                        "3        9       9      INSERT    NORMAL\n"
                        "2        19      19     INSERT    NORMAL\n"
                        "4        19      19     INSERT    NORMAL\n"
                        "15       200     200    INSERT    NORMAL\n"
                        "7        9       9      INSERT    NORMAL\n"
                        "9        9       9      INSERT    NORMAL\n"
                        "6        19      19     INSERT    NORMAL\n"
                        "8        19      19     INSERT    NORMAL\n"
                        "11       9       9      INSERT    NORMAL\n"
                        "13       9       9      INSERT    NORMAL\n"
                        "10       19      19     INSERT    NORMAL\n"
                        "12       19      19     INSERT    NORMAL\n"
                        "14       19      19     INSERT    NORMAL\n"
                        "18       200     200    INSERT    NORMAL\n"
                        "17       9       9      INSERT    NORMAL\n"
                        "16       19      19     INSERT    NORMAL\n"
                        "20       200     200    INSERT    NORMAL\n"
                        "19       99      99     INSERT    NORMAL\n"
                        "21       99      99     INSERT    NORMAL\n"
                        "22       99      99     INSERT    NORMAL\n";

  ObDatumRange range;
  range.set_whole_range();
  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 1;
  trans_version_range.multi_version_start_ = 50;
  trans_version_range.snapshot_version_ = INT64_MAX;
  prepare_scan_param(trans_version_range, table_store_iter, true/*is_delete_insert*/);
  STORAGE_LOG(INFO, "check table store iter", K(table_store_iter));

  mock_tablet_table_store(table_store_iter);
  ObMultipleScanMerge scan_merge;
  OK(scan_merge.init(access_param_, context_, get_table_param_));
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

TEST_F(TestIncMajorDeleteInsertScan, test_scan_with_refresh_di_base)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator table_store_iter;

  ObTableHandleV2 handle1;
  const char *micro_data1[1];
  micro_data1[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -50      DI_VERSION     9       9    INSERT    NORMAL      CLF\n"
      "3        -50      DI_VERSION     9       9    INSERT    NORMAL      CLF\n"
      "5        -50      DI_VERSION     9       9    INSERT    NORMAL      CLF\n"
      "7        -50      DI_VERSION     9       9    INSERT    NORMAL      CLF\n"
      "9        -50      DI_VERSION     9       9    INSERT    NORMAL      CLF\n";

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
  STORAGE_LOG(INFO, "finish prepare major sstable");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "2        -60      DI_VERSION   19     19    INSERT    NORMAL        CLF\n"
      "4        -60      DI_VERSION   19     19    INSERT    NORMAL        CLF\n"
      "6        -60      DI_VERSION   19     19    INSERT    NORMAL        CLF\n"
      "8        -60      DI_VERSION   19     19    INSERT    NORMAL        CLF\n";

  snapshot_version = 60;
  scn_range.start_scn_.convert_for_tx(0);
  scn_range.end_scn_.convert_for_tx(60);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  ObTabletCreateSSTableParam param2;
  prepare_create_basic_sst_param(param2, scn_range, ObITable::INC_MAJOR_SSTABLE);
  prepare_create_row_sst_param(param2);
  prepare_create_inc_sst_param(param2, {ObUncommitTxDesc(0, 70, ObUncommitTxDesc::KeyStatus::SQL_SEQ)});
  prepare_data_end_with_param(handle2, param2, 70);
  table_store_iter.add_table(handle2.get_table());
  STORAGE_LOG(INFO, "finish prepare inc major sstable 1");

  ObTableHandleV2 handle3;
  const char *micro_data3[1];
  micro_data3[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "5        -80      DI_VERSION    99     99    INSERT    NORMAL        CF\n"
      "5        -80      0              9      9    DELETE    NORMAL        CL\n"
      "6        -80      DI_VERSION    99     99    INSERT    NORMAL        CF\n"
      "6        -80      0             19     19    DELETE    NORMAL        CL\n";

  snapshot_version = 80;
  scn_range.start_scn_.convert_for_tx(0);
  scn_range.end_scn_.convert_for_tx(80);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data3, 1);
  ObTabletCreateSSTableParam param3;
  prepare_create_basic_sst_param(param3, scn_range, ObITable::MINI_SSTABLE);
  prepare_create_row_sst_param(param3);
  prepare_data_end_with_param(handle3, param3, 80);
  table_store_iter.add_table(handle3.get_table());
  STORAGE_LOG(INFO, "finish prepare mini sstable");

  const char *result1 = "bigint   bigint bigint  flag     flag_type\n"
                        "5        99      99     INSERT    NORMAL\n"
                        "1        9       9      INSERT    NORMAL\n"
                        "3        9       9      INSERT    NORMAL\n"
                        "2        19      19     INSERT    NORMAL\n"
                        "4        19      19     INSERT    NORMAL\n"
                        "6        99      99     INSERT    NORMAL\n"
                        "7        9       9      INSERT    NORMAL\n"
                        "9        9       9      INSERT    NORMAL\n"
                        "8        19      19     INSERT    NORMAL\n";

  ObDatumRange range;
  range.set_whole_range();
  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 1;
  trans_version_range.multi_version_start_ = 50;
  trans_version_range.snapshot_version_ = INT64_MAX;
  prepare_scan_param(trans_version_range, table_store_iter, true/*is_delete_insert*/);

  mock_tablet_table_store(table_store_iter);
  ObMultipleScanMerge scan_merge;
  OK(scan_merge.init(access_param_, context_, get_table_param_));
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

  // get one single row
  OK(scan_merge.get_next_rows(count, 1));
  if (count > 0) {
    ObMockScanMergeIterator merge_iter(count);
    ASSERT_EQ(OB_SUCCESS, merge_iter.init(reinterpret_cast<ObVectorStore *>(scan_merge.block_row_store_),
                                          query_allocator_,
                                          *access_param_.iter_param_.get_read_info()));
    bool is_equal = res_iter.equals<ObMockScanMergeIterator, ObStoreRow>(merge_iter, false, false, false, true);
    ASSERT_TRUE(is_equal);
    total_count += count;
  }
  ASSERT_EQ(1, total_count);

  // get two di base rows and refresh table
  OK(scan_merge.get_next_rows(count, 2));
  if (count > 0) {
    ObMockScanMergeIterator merge_iter(count);
    ASSERT_EQ(OB_SUCCESS, merge_iter.init(reinterpret_cast<ObVectorStore *>(scan_merge.block_row_store_),
                                          query_allocator_,
                                          *access_param_.iter_param_.get_read_info()));
    bool is_equal = res_iter.equals<ObMockScanMergeIterator, ObStoreRow>(merge_iter, false, false, false, true);
    ASSERT_TRUE(is_equal);
    total_count += count;
  }
  ASSERT_EQ(3, total_count);
  OK(refresh_table(scan_merge, get_table_param_.tablet_iter_.table_store_iter_));
  STORAGE_LOG(INFO, "refresh tablet iter", K(get_table_param_.tablet_iter_.table_store_iter_));

  // get residual rows
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
  ASSERT_EQ(9, total_count);

  handle1.reset();
  handle2.reset();
  handle3.reset();
  scan_merge.reset();
}

TEST_F(TestIncMajorDeleteInsertScan, test_scan_with_refresh_retry_number_changed)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator table_store_iter;

  ObTableHandleV2 handle1;
  const char *micro_data1[1];
  micro_data1[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -50      DI_VERSION     9       9    INSERT    NORMAL      CLF\n"
      "3        -50      DI_VERSION     9       9    INSERT    NORMAL      CLF\n"
      "5        -50      DI_VERSION     9       9    INSERT    NORMAL      CLF\n"
      "7        -50      DI_VERSION     9       9    INSERT    NORMAL      CLF\n"
      "9        -50      DI_VERSION     9       9    INSERT    NORMAL      CLF\n";

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
  STORAGE_LOG(INFO, "finish prepare major sstable");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "2        -60      DI_VERSION    19     19    INSERT    NORMAL        CLF\n"
      "4        -60      DI_VERSION    19     19    INSERT    NORMAL        CLF\n"
      "6        -60      DI_VERSION    19     19    INSERT    NORMAL        CLF\n"
      "8        -60      DI_VERSION    19     19    INSERT    NORMAL        CLF\n";

  snapshot_version = 60;
  scn_range.start_scn_.convert_for_tx(0);
  scn_range.end_scn_.convert_for_tx(60);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  ObTabletCreateSSTableParam param2;
  prepare_create_basic_sst_param(param2, scn_range, ObITable::INC_MAJOR_SSTABLE);
  prepare_create_row_sst_param(param2);
  prepare_create_inc_sst_param(param2, {ObUncommitTxDesc(0, 70, ObUncommitTxDesc::KeyStatus::SQL_SEQ)});
  prepare_data_end_with_param(handle2, param2, 70);
  table_store_iter.add_table(handle2.get_table());
  STORAGE_LOG(INFO, "finish prepare inc major sstable 1");

  ObTableHandleV2 handle3;
  const char *micro_data3[1];
  micro_data3[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "6        -80      DI_VERSION    99     99    INSERT    NORMAL        CF\n"
      "6        -80      0             99     99    DELETE    NORMAL        CL\n";

  snapshot_version = 80;
  scn_range.start_scn_.convert_for_tx(0);
  scn_range.end_scn_.convert_for_tx(80);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data3, 1);
  ObTabletCreateSSTableParam param3;
  prepare_create_basic_sst_param(param3, scn_range, ObITable::MINI_SSTABLE);
  prepare_create_row_sst_param(param3);
  prepare_data_end_with_param(handle3, param3, 80);
  table_store_iter.add_table(handle3.get_table());
  STORAGE_LOG(INFO, "finish prepare mini sstable");

  ObTableHandleV2 handle4;
  const char *micro_data4[1];
  micro_data4[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "10       -80      DI_VERSION    19     19    INSERT    NORMAL        CLF\n";

  snapshot_version = 80;
  scn_range.start_scn_.convert_for_tx(0);
  scn_range.end_scn_.convert_for_tx(80);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data4, 1);
  ObTabletCreateSSTableParam param4;
  prepare_create_basic_sst_param(param4, scn_range, ObITable::INC_MAJOR_SSTABLE);
  prepare_create_row_sst_param(param4);
  prepare_create_inc_sst_param(param4, {ObUncommitTxDesc(0, 90, ObUncommitTxDesc::KeyStatus::SQL_SEQ)});
  prepare_data_end_with_param(handle4, param4, 90);
  STORAGE_LOG(INFO, "finish prepare inc major sstable 4");

  const char *result1 = "bigint   bigint bigint  flag     flag_type\n"
                        "6        99      99     INSERT    NORMAL\n";

  ObDatumRange range;
  range.set_whole_range();
  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 1;
  trans_version_range.multi_version_start_ = 50;
  trans_version_range.snapshot_version_ = INT64_MAX;
  prepare_scan_param(trans_version_range, table_store_iter, true/*is_delete_insert*/);

  mock_tablet_table_store(table_store_iter);
  ObMultipleScanMerge scan_merge;
  OK(scan_merge.init(access_param_, context_, get_table_param_));
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
  // get one row and refresh table
  OK(scan_merge.get_next_rows(count, 1));
  if (count > 0) {
    ObMockScanMergeIterator merge_iter(count);
    ASSERT_EQ(OB_SUCCESS, merge_iter.init(reinterpret_cast<ObVectorStore *>(scan_merge.block_row_store_),
                                          query_allocator_,
                                          *access_param_.iter_param_.get_read_info()));
    bool is_equal = res_iter.equals<ObMockScanMergeIterator, ObStoreRow>(merge_iter, false, false, false, true);
    ASSERT_TRUE(is_equal);
    total_count += count;
  }
  table_store_iter.reset();
  table_store_iter.add_table(handle1.get_table());
  table_store_iter.add_table(handle2.get_table());
  table_store_iter.add_table(handle4.get_table());
  table_store_iter.add_table(handle3.get_table());
  mock_tablet_table_store(table_store_iter);
  OK(refresh_table(scan_merge, get_table_param_.tablet_iter_.table_store_iter_));
  ob_usleep(2 * 1000 * 1000); // sleep 2s
  ASSERT_EQ(OB_SNAPSHOT_DISCARDED, refresh_table(scan_merge, get_table_param_.tablet_iter_.table_store_iter_));
  STORAGE_LOG(INFO, "refresh tablet iter", K(get_table_param_.tablet_iter_.table_store_iter_));
  ASSERT_EQ(1, total_count);

  handle1.reset();
  handle2.reset();
  handle3.reset();
  scan_merge.reset();
}

TEST_F(TestIncMajorDeleteInsertScan, test_scan_with_refresh_retry_inc_major_changed)
{

  int ret = OB_SUCCESS;
  ObTableStoreIterator table_store_iter;

  ObTableHandleV2 handle1;
  const char *micro_data1[1];
  micro_data1[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -50      DI_VERSION     9       9    INSERT    NORMAL      CLF\n"
      "3        -50      DI_VERSION     9       9    INSERT    NORMAL      CLF\n"
      "5        -50      DI_VERSION     9       9    INSERT    NORMAL      CLF\n"
      "7        -50      DI_VERSION     9       9    INSERT    NORMAL      CLF\n"
      "9        -50      DI_VERSION     9       9    INSERT    NORMAL      CLF\n";

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
  STORAGE_LOG(INFO, "finish prepare major sstable");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "2        -60      DI_VERSION    19     19    INSERT    NORMAL        CLF\n"
      "4        -60      DI_VERSION    19     19    INSERT    NORMAL        CLF\n"
      "6        -60      DI_VERSION    19     19    INSERT    NORMAL        CLF\n"
      "8        -60      DI_VERSION    19     19    INSERT    NORMAL        CLF\n";

  snapshot_version = 60;
  scn_range.start_scn_.convert_for_tx(0);
  scn_range.end_scn_.convert_for_tx(60);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  ObTabletCreateSSTableParam param2;
  prepare_create_basic_sst_param(param2, scn_range, ObITable::INC_MAJOR_SSTABLE);
  prepare_create_row_sst_param(param2);
  prepare_create_inc_sst_param(param2, {ObUncommitTxDesc(0, 70, ObUncommitTxDesc::KeyStatus::SQL_SEQ)});
  prepare_data_end_with_param(handle2, param2, 70);
  table_store_iter.add_table(handle2.get_table());
  STORAGE_LOG(INFO, "finish prepare inc major sstable 1");

  ObTableHandleV2 handle3;
  const char *micro_data3[1];
  micro_data3[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "2        -60      DI_VERSION    19     19    INSERT    NORMAL        CLF\n"
      "4        -60      DI_VERSION    19     19    INSERT    NORMAL        CLF\n"
      "6        -60      DI_VERSION    19     19    INSERT    NORMAL        CLF\n"
      "8        -60      DI_VERSION    19     19    INSERT    NORMAL        CLF\n";

  snapshot_version = 70;
  scn_range.start_scn_.convert_for_tx(0);
  scn_range.end_scn_.convert_for_tx(70);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data3, 1);
  ObTabletCreateSSTableParam param3;
  prepare_create_basic_sst_param(param3, scn_range, ObITable::INC_MAJOR_SSTABLE);
  prepare_create_row_sst_param(param3);
  prepare_create_inc_sst_param(param3, {ObUncommitTxDesc(0, 70, ObUncommitTxDesc::KeyStatus::SQL_SEQ)});
  prepare_data_end_with_param(handle3, param3, 70);
  STORAGE_LOG(INFO, "finish prepare inc major sstable 2");

  const char *result1 = "bigint   bigint bigint  flag     flag_type\n"
                        "1        9       9      INSERT    NORMAL\n"
                        "5        99      99     INSERT    NORMAL\n"
                        "3        9       9      INSERT    NORMAL\n"
                        "6        99      99     INSERT    NORMAL\n"
                        "2        19      19     INSERT    NORMAL\n"
                        "4        19      19     INSERT    NORMAL\n"
                        "7        9       9      INSERT    NORMAL\n"
                        "9        9       9      INSERT    NORMAL\n"
                        "8        19      19     INSERT    NORMAL\n";

  ObDatumRange range;
  range.set_whole_range();
  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 1;
  trans_version_range.multi_version_start_ = 50;
  trans_version_range.snapshot_version_ = INT64_MAX;
  prepare_scan_param(trans_version_range, table_store_iter, true/*is_delete_insert*/);

  mock_tablet_table_store(table_store_iter);
  ObMultipleScanMerge scan_merge;
  OK(scan_merge.init(access_param_, context_, get_table_param_));
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
  // get one row and refresh table
  OK(scan_merge.get_next_rows(count, 1));
  if (count > 0) {
    ObMockScanMergeIterator merge_iter(count);
    ASSERT_EQ(OB_SUCCESS, merge_iter.init(reinterpret_cast<ObVectorStore *>(scan_merge.block_row_store_),
                                          query_allocator_,
                                          *access_param_.iter_param_.get_read_info()));
    bool is_equal = res_iter.equals<ObMockScanMergeIterator, ObStoreRow>(merge_iter, false, false, false, true);
    ASSERT_TRUE(is_equal);
    total_count += count;
  }
  table_store_iter.reset();
  table_store_iter.add_table(handle1.get_table());
  table_store_iter.add_table(handle3.get_table());
  mock_tablet_table_store(table_store_iter);
  OK(refresh_table(scan_merge, get_table_param_.tablet_iter_.table_store_iter_));
  ob_usleep(2 * 1000 * 1000); // sleep 2s
  ASSERT_EQ(OB_SNAPSHOT_DISCARDED, refresh_table(scan_merge, get_table_param_.tablet_iter_.table_store_iter_));
  STORAGE_LOG(INFO, "refresh tablet iter", K(get_table_param_.tablet_iter_.table_store_iter_));
  ASSERT_EQ(1, total_count);

  handle1.reset();
  handle2.reset();
  handle3.reset();
  scan_merge.reset();
}

TEST_F(TestIncMajorDeleteInsertScan, test_scan_with_refresh_single_row)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator table_store_iter;

  ObTableHandleV2 handle1;
  const char *micro_data1[1];
  micro_data1[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -50      DI_VERSION     9       9    INSERT    NORMAL      CLF\n"
      "3        -50      DI_VERSION     9       9    INSERT    NORMAL      CLF\n";

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
  prepare_create_column_sst_param(param1);
  prepare_data_end_with_param(handle1, param1, 50);
  table_store_iter.add_table(handle1.get_table());
  STORAGE_LOG(INFO, "finish prepare major sstable");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "2        -60      DI_VERSION   19     19    INSERT    NORMAL        CLF\n"
      "4        -60      DI_VERSION   19     19    INSERT    NORMAL        CLF\n"
      "6        -60      DI_VERSION   19     19    INSERT    NORMAL        CLF\n"
      "8        -60      DI_VERSION   19     19    INSERT    NORMAL        CLF\n";

  snapshot_version = 60;
  scn_range.start_scn_.convert_for_tx(0);
  scn_range.end_scn_.convert_for_tx(60);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  ObTabletCreateSSTableParam param2;
  prepare_create_basic_sst_param(param2, scn_range, ObITable::INC_MAJOR_SSTABLE);
  prepare_create_row_sst_param(param2);
  prepare_create_inc_sst_param(param2, {ObUncommitTxDesc(0, 70, ObUncommitTxDesc::KeyStatus::SQL_SEQ)});
  prepare_data_end_with_param(handle2, param2, 70);
  table_store_iter.add_table(handle2.get_table());
  STORAGE_LOG(INFO, "finish prepare inc major sstable 1");

  ObTableHandleV2 handle3;
  const char *micro_data3[1];
  micro_data3[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "9        -70      DI_VERSION    19      19    INSERT    NORMAL      CLF\n"
      "10       -70      DI_VERSION    19      19    INSERT    NORMAL      CLF\n";

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
      "11       -80      DI_VERSION   19     19    INSERT    NORMAL        CLF\n"
      "12       -80      DI_VERSION   19     19    INSERT    NORMAL        CLF\n"
      "13       -80      DI_VERSION   19     19    INSERT    NORMAL        CLF\n";

  snapshot_version = 80;
  scn_range.start_scn_.convert_for_tx(0);
  scn_range.end_scn_.convert_for_tx(80);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data4, 1);
  ObTabletCreateSSTableParam param4;
  prepare_create_basic_sst_param(param4, scn_range, ObITable::INC_MAJOR_SSTABLE);
  prepare_create_row_sst_param(param4);
  prepare_create_inc_sst_param(param4, {ObUncommitTxDesc(0, 90, ObUncommitTxDesc::KeyStatus::SQL_SEQ)});
  prepare_data_end_with_param(handle4, param4, 90);
  table_store_iter.add_table(handle4.get_table());
  STORAGE_LOG(INFO, "finish prepare inc major sstable 3");

  ObTableHandleV2 handle5;
  const char *micro_data5[1];
  micro_data5[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "5        -100      DI_VERSION     9       9    INSERT    NORMAL      CLF\n"
      "6        -100      DI_VERSION    99     99    INSERT    NORMAL        CF\n"
      "6        -100      0             19     19    DELETE    NORMAL        CL\n"
      "7        -70       DI_VERSION    99     99    INSERT    NORMAL        CLF\n"
      "12       -80       DI_VERSION    99     99    INSERT    NORMAL        CF\n"
      "12       -80       0             19     19    DELETE    NORMAL        CL\n";

  snapshot_version = 80;
  scn_range.start_scn_.convert_for_tx(0);
  scn_range.end_scn_.convert_for_tx(80);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data5, 1);
  ObTabletCreateSSTableParam param5;
  prepare_create_basic_sst_param(param5, scn_range, ObITable::MINI_SSTABLE);
  prepare_create_row_sst_param(param5);
  prepare_data_end_with_param(handle5, param5, 80);
  table_store_iter.add_table(handle5.get_table());
  STORAGE_LOG(INFO, "finish prepare mini sstable");

  const char *result1 = "bigint   bigint bigint  flag     flag_type\n"
                        "5        9       9      INSERT    NORMAL\n"
                        "6        99      99     INSERT    NORMAL\n"
                        "1        9       9      INSERT    NORMAL\n"
                        "3        9       9      INSERT    NORMAL\n"
                        "2        19      19     INSERT    NORMAL\n"
                        "4        19      19     INSERT    NORMAL\n"
                        "7        99      99     INSERT    NORMAL\n"
                        "12       99      99     INSERT    NORMAL\n"
                        "8        19      19     INSERT    NORMAL\n"
                        "9        19      19     INSERT    NORMAL\n"
                        "10       19      19     INSERT    NORMAL\n"
                        "11       19      19     INSERT    NORMAL\n"
                        "13       19      19     INSERT    NORMAL\n";

  ObDatumRange range;
  range.set_whole_range();
  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 1;
  trans_version_range.multi_version_start_ = 50;
  trans_version_range.snapshot_version_ = INT64_MAX;
  prepare_scan_param(trans_version_range, table_store_iter, true/*is_delete_insert*/);

  mock_tablet_table_store(table_store_iter);
  ObMultipleScanMerge scan_merge;
  OK(scan_merge.init(access_param_, context_, get_table_param_));
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

  // get one row and refresh table
  OK(scan_merge.get_next_rows(count, 1));
  if (count > 0) {
    ObMockScanMergeIterator merge_iter(count);
    ASSERT_EQ(OB_SUCCESS, merge_iter.init(reinterpret_cast<ObVectorStore *>(scan_merge.block_row_store_),
                                          query_allocator_,
                                          *access_param_.iter_param_.get_read_info()));
    bool is_equal = res_iter.equals<ObMockScanMergeIterator, ObStoreRow>(merge_iter, false, false, false, true);
    ASSERT_TRUE(is_equal);
    total_count += count;
  }
  ASSERT_EQ(1, total_count);
  OK(refresh_table(scan_merge, get_table_param_.tablet_iter_.table_store_iter_));
  STORAGE_LOG(INFO, "refresh tablet iter", K(get_table_param_.tablet_iter_.table_store_iter_));

  // get residual rows
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
  ASSERT_EQ(13, total_count);

  handle1.reset();
  handle2.reset();
  handle3.reset();
  handle4.reset();
  handle5.reset();
  scan_merge.reset();
}

TEST_F(TestIncMajorDeleteInsertScan, test_multi_range_scan)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator table_store_iter;

  ObTableHandleV2 handle1;
  const char *micro_data1[1];
  micro_data1[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -50      DI_VERSION     9       9    INSERT    NORMAL      CLF\n"
      "3        -50      DI_VERSION     9       9    INSERT    NORMAL      CLF\n"
      "5        -50      DI_VERSION     9       9    INSERT    NORMAL      CLF\n"
      "7        -50      DI_VERSION     9       9    INSERT    NORMAL      CLF\n"
      "9        -50      DI_VERSION     9       9    INSERT    NORMAL      CLF\n";

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
  STORAGE_LOG(INFO, "finish prepare major sstable");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "2        -60      DI_VERSION   19     19    INSERT    NORMAL        CLF\n"
      "4        -60      DI_VERSION   19     19    INSERT    NORMAL        CLF\n"
      "6        -60      DI_VERSION   19     19    INSERT    NORMAL        CLF\n"
      "8        -60      DI_VERSION   19     19    INSERT    NORMAL        CLF\n";

  snapshot_version = 60;
  scn_range.start_scn_.convert_for_tx(0);
  scn_range.end_scn_.convert_for_tx(60);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  ObTabletCreateSSTableParam param2;
  prepare_create_basic_sst_param(param2, scn_range, ObITable::INC_MAJOR_SSTABLE);
  prepare_create_row_sst_param(param2);
  prepare_create_inc_sst_param(param2, {ObUncommitTxDesc(0, 70, ObUncommitTxDesc::KeyStatus::SQL_SEQ)});
  prepare_data_end_with_param(handle2, param2, 70);
  table_store_iter.add_table(handle2.get_table());
  STORAGE_LOG(INFO, "finish prepare inc major sstable 1");

  ObTableHandleV2 handle3;
  const char *micro_data3[1];
  micro_data3[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "5        -80      DI_VERSION    99     99    INSERT    NORMAL        CF\n"
      "5        -80      0              9      9    DELETE    NORMAL        CL\n"
      "6        -80      DI_VERSION    99     99    INSERT    NORMAL        CF\n"
      "6        -80      0             19     19    DELETE    NORMAL        CL\n";

  snapshot_version = 80;
  scn_range.start_scn_.convert_for_tx(0);
  scn_range.end_scn_.convert_for_tx(80);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data3, 1);
  ObTabletCreateSSTableParam param3;
  prepare_create_basic_sst_param(param3, scn_range, ObITable::MINI_SSTABLE);
  prepare_create_row_sst_param(param3);
  prepare_data_end_with_param(handle3, param3, 80);
  table_store_iter.add_table(handle3.get_table());
  STORAGE_LOG(INFO, "finish prepare mini sstable");

  const char *result1 = "bigint   bigint bigint  flag     flag_type\n"
                        "5        99      99     INSERT    NORMAL\n"
                        "1        9       9      INSERT    NORMAL\n"
                        "3        9       9      INSERT    NORMAL\n"
                        "2        19      19     INSERT    NORMAL\n"
                        "4        19      19     INSERT    NORMAL\n"
                        "6        99      99     INSERT    NORMAL\n"
                        "7        9       9      INSERT    NORMAL\n"
                        "9        9       9      INSERT    NORMAL\n"
                        "8        19      19     INSERT    NORMAL\n";

  ObDatumRange range;
  ObSEArray<ObDatumRange, 8> ranges;
  generate_range(1, 3, range);
  ranges.push_back(range);
  generate_range(4, 6, range);
  ranges.push_back(range);
  generate_range(7, 9, range);
  ranges.push_back(range);
  STORAGE_LOG(INFO, "generate ranges", K(ranges));
  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 1;
  trans_version_range.multi_version_start_ = 50;
  trans_version_range.snapshot_version_ = INT64_MAX;
  prepare_scan_param(trans_version_range, table_store_iter, true/*is_delete_insert*/);

  mock_tablet_table_store(table_store_iter);
  ObMultipleMultiScanMerge multi_scan_merge;
  OK(multi_scan_merge.init(access_param_, context_, get_table_param_));
  refresh_iter(multi_scan_merge);
  OK(multi_scan_merge.open(ranges));
  multi_scan_merge.disable_padding();
  multi_scan_merge.disable_fill_virtual_column();

  int64_t count = 0;
  int64_t total_count = 0;
  ret = OB_SUCCESS;
  ObMockIterator res_iter;
  res_iter.reset();
  ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));

  // get one single row
  OK(multi_scan_merge.get_next_rows(count, 1));
  if (count > 0) {
    ObMockScanMergeIterator merge_iter(count);
    ASSERT_EQ(OB_SUCCESS, merge_iter.init(reinterpret_cast<ObVectorStore *>(multi_scan_merge.block_row_store_),
                                          query_allocator_,
                                          *access_param_.iter_param_.get_read_info()));
    bool is_equal = res_iter.equals<ObMockScanMergeIterator, ObStoreRow>(merge_iter, false, false, false, true);
    ASSERT_TRUE(is_equal);
    total_count += count;
    STORAGE_LOG(INFO, "get next rows", K(count), K(total_count));
  }
  ASSERT_EQ(1, total_count);

  // get two di base rows
  OK(multi_scan_merge.get_next_rows(count, 2));
  if (count > 0) {
    ObMockScanMergeIterator merge_iter(count);
    ASSERT_EQ(OB_SUCCESS, merge_iter.init(reinterpret_cast<ObVectorStore *>(multi_scan_merge.block_row_store_),
                                          query_allocator_,
                                          *access_param_.iter_param_.get_read_info()));
    bool is_equal = res_iter.equals<ObMockScanMergeIterator, ObStoreRow>(merge_iter, false, false, false, true);
    ASSERT_TRUE(is_equal);
    total_count += count;
    STORAGE_LOG(INFO, "get next rows", K(count), K(total_count));
  }
  ASSERT_EQ(3, total_count);

  // get one di base row and refresh table
  OK(multi_scan_merge.get_next_rows(count, 1));
  if (count > 0) {
    ObMockScanMergeIterator merge_iter(count);
    ASSERT_EQ(OB_SUCCESS, merge_iter.init(reinterpret_cast<ObVectorStore *>(multi_scan_merge.block_row_store_),
                                          query_allocator_,
                                          *access_param_.iter_param_.get_read_info()));
    bool is_equal = res_iter.equals<ObMockScanMergeIterator, ObStoreRow>(merge_iter, false, false, false, true);
    ASSERT_TRUE(is_equal);
    total_count += count;
    STORAGE_LOG(INFO, "get next rows", K(count), K(total_count));
  }
  ASSERT_EQ(4, total_count);
  OK(refresh_table(multi_scan_merge, get_table_param_.tablet_iter_.table_store_iter_));
  STORAGE_LOG(INFO, "refresh tablet iter", K(get_table_param_.tablet_iter_.table_store_iter_));

  // get residual rows
  while (OB_SUCC(ret)) {
    ret = multi_scan_merge.get_next_rows(count, SQL_BATCH_SIZE);
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
  ASSERT_EQ(9, total_count);

  handle1.reset();
  handle2.reset();
  handle3.reset();
  multi_scan_merge.reset();
}

TEST_F(TestIncMajorDeleteInsertScan, test_di_base_sstable_row_scanner_with_iter_pool)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator table_store_iter;

  ObTableHandleV2 handle1;
  const char *micro_data1[1];
  micro_data1[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -50      DI_VERSION     9       9    INSERT    NORMAL      CLF\n"
      "3        -50      DI_VERSION     9       9    INSERT    NORMAL      CLF\n"
      "5        -50      DI_VERSION     9       9    INSERT    NORMAL      CLF\n"
      "7        -50      DI_VERSION     9       9    INSERT    NORMAL      CLF\n"
      "9        -50      DI_VERSION     9       9    INSERT    NORMAL      CLF\n";

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
  STORAGE_LOG(INFO, "finish prepare major sstable");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "2        -60      DI_VERSION   19     19    INSERT    NORMAL        CLF\n"
      "4        -60      DI_VERSION   19     19    INSERT    NORMAL        CLF\n"
      "6        -60      DI_VERSION   19     19    INSERT    NORMAL        CLF\n"
      "8        -60      DI_VERSION   19     19    INSERT    NORMAL        CLF\n";

  snapshot_version = 60;
  scn_range.start_scn_.convert_for_tx(0);
  scn_range.end_scn_.convert_for_tx(60);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  ObTabletCreateSSTableParam param2;
  prepare_create_basic_sst_param(param2, scn_range, ObITable::INC_MAJOR_SSTABLE);
  prepare_create_row_sst_param(param2);
  prepare_create_inc_sst_param(param2, {ObUncommitTxDesc(0, 70, ObUncommitTxDesc::KeyStatus::SQL_SEQ)});
  prepare_data_end_with_param(handle2, param2, 70);
  table_store_iter.add_table(handle2.get_table());
  STORAGE_LOG(INFO, "finish prepare inc major sstable 1");

  ObTableHandleV2 handle3;
  const char *micro_data3[1];
  micro_data3[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "5        -80      DI_VERSION    99     99    INSERT    NORMAL        CF\n"
      "5        -80      0              9      9    DELETE    NORMAL        CL\n"
      "6        -80      DI_VERSION    99     99    INSERT    NORMAL        CF\n"
      "6        -80      0             19     19    DELETE    NORMAL        CL\n";

  snapshot_version = 80;
  scn_range.start_scn_.convert_for_tx(0);
  scn_range.end_scn_.convert_for_tx(80);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data3, 1);
  ObTabletCreateSSTableParam param3;
  prepare_create_basic_sst_param(param3, scn_range, ObITable::MINI_SSTABLE);
  prepare_create_row_sst_param(param3);
  prepare_data_end_with_param(handle3, param3, 80);
  table_store_iter.add_table(handle3.get_table());
  STORAGE_LOG(INFO, "finish prepare mini sstable");

  const char *result1 = "bigint   bigint bigint  flag     flag_type\n"
                        "5        99      99     INSERT    NORMAL\n"
                        "1        9       9      INSERT    NORMAL\n"
                        "3        9       9      INSERT    NORMAL\n"
                        "2        19      19     INSERT    NORMAL\n"
                        "4        19      19     INSERT    NORMAL\n"
                        "6        99      99     INSERT    NORMAL\n"
                        "7        9       9      INSERT    NORMAL\n"
                        "9        9       9      INSERT    NORMAL\n"
                        "8        19      19     INSERT    NORMAL\n";

  ObDatumRange range;
  range.set_whole_range();
  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 1;
  trans_version_range.multi_version_start_ = 50;
  trans_version_range.snapshot_version_ = INT64_MAX;
  prepare_scan_param(trans_version_range, table_store_iter, true/*is_delete_insert*/);
  access_param_.set_use_global_iter_pool();
  access_param_.iter_param_.set_use_stmt_iter_pool();

  mock_tablet_table_store(table_store_iter);
  ObMultipleScanMerge scan_merge;
  OK(scan_merge.init(access_param_, context_, get_table_param_));
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
  ASSERT_EQ(9, total_count);

  handle1.reset();
  handle2.reset();
  handle3.reset();
  scan_merge.reset();
}

TEST_F(TestIncMajorDeleteInsertScan, test_no_minor_sstable_scan)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator table_store_iter;

  ObTableHandleV2 handle1;
  const char *micro_data1[1];
  micro_data1[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "3        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "5        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "7        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "9        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "11       -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "13       -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "15       -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "17       -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n";

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
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "2        -70      DI_VERSION   19     19    INSERT    NORMAL        CLF\n"
      "4        -70      DI_VERSION   19     19    INSERT    NORMAL        CLF\n"
      "6        -70      DI_VERSION   19     19    INSERT    NORMAL        CLF\n"
      "8        -70      DI_VERSION   19     19    INSERT    NORMAL        CLF\n"
      "10       -70      DI_VERSION   19     19    INSERT    NORMAL        CLF\n"
      "12       -70      DI_VERSION   19     19    INSERT    NORMAL        CLF\n"
      "14       -70      DI_VERSION   19     19    INSERT    NORMAL        CLF\n"
      "16       -70      DI_VERSION   19     19    INSERT    NORMAL        CLF\n";

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
      "18       -90      DI_VERSION  99     99    INSERT    NORMAL        CLF\n"
      "19       -90      DI_VERSION  99     99    INSERT    NORMAL        CLF\n"
      "20       -90      DI_VERSION  99     99    INSERT    NORMAL        CLF\n"
      "21       -90      DI_VERSION  99     99    INSERT    NORMAL        CLF\n"
      "22       -90      DI_VERSION  99     99    INSERT    NORMAL        CLF\n";

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

  const char *result1 = "bigint   bigint bigint  flag     flag_type\n"
                        "1        9       9      INSERT    NORMAL\n"
                        "3        9       9      INSERT    NORMAL\n"
                        "5        9       9      INSERT    NORMAL\n"
                        "7        9       9      INSERT    NORMAL\n"
                        "9        9       9      INSERT    NORMAL\n"
                        "11       9       9      INSERT    NORMAL\n"
                        "13       9       9      INSERT    NORMAL\n"
                        "15       9       9      INSERT    NORMAL\n"
                        "17       9       9      INSERT    NORMAL\n"
                        "2        19      19     INSERT    NORMAL\n"
                        "4        19      19     INSERT    NORMAL\n"
                        "6        19      19     INSERT    NORMAL\n"
                        "8        19      19     INSERT    NORMAL\n"
                        "10       19      19     INSERT    NORMAL\n"
                        "12       19      19     INSERT    NORMAL\n"
                        "14       19      19     INSERT    NORMAL\n"
                        "16       19      19     INSERT    NORMAL\n"
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
  prepare_scan_param(trans_version_range, table_store_iter, true/*is_delete_insert*/);

  mock_tablet_table_store(table_store_iter);
  ObMultipleScanMerge scan_merge;
  OK(scan_merge.init(access_param_, context_, get_table_param_));
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
  OK(refresh_table(scan_merge, get_table_param_.tablet_iter_.table_store_iter_));
  STORAGE_LOG(INFO, "refresh tablet iter", K(get_table_param_.tablet_iter_.table_store_iter_));
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
  scan_merge.reset();
}

TEST_F(TestIncMajorDeleteInsertScan, test_no_minor_sstable_multi_scan)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator table_store_iter;

  ObTableHandleV2 handle1;
  const char *micro_data1[1];
  micro_data1[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "3        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "5        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "7        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "9        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "11       -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "13       -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "15       -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "17       -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n";

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
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "2        -70      DI_VERSION   19     19    INSERT    NORMAL        CLF\n"
      "4        -70      DI_VERSION   19     19    INSERT    NORMAL        CLF\n"
      "6        -70      DI_VERSION   19     19    INSERT    NORMAL        CLF\n"
      "8        -70      DI_VERSION   19     19    INSERT    NORMAL        CLF\n"
      "10       -70      DI_VERSION   19     19    INSERT    NORMAL        CLF\n"
      "12       -70      DI_VERSION   19     19    INSERT    NORMAL        CLF\n"
      "14       -70      DI_VERSION   19     19    INSERT    NORMAL        CLF\n"
      "16       -70      DI_VERSION   19     19    INSERT    NORMAL        CLF\n";

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
      "18       -90      DI_VERSION  99     99    INSERT    NORMAL        CLF\n"
      "19       -90      DI_VERSION  99     99    INSERT    NORMAL        CLF\n"
      "20       -90      DI_VERSION  99     99    INSERT    NORMAL        CLF\n"
      "21       -90      DI_VERSION  99     99    INSERT    NORMAL        CLF\n"
      "22       -90      DI_VERSION  99     99    INSERT    NORMAL        CLF\n";

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

  const char *result1 = "bigint   bigint bigint  flag     flag_type\n"
                        "1        9       9      INSERT    NORMAL\n"
                        "3        9       9      INSERT    NORMAL\n"
                        "5        9       9      INSERT    NORMAL\n"
                        "7        9       9      INSERT    NORMAL\n"
                        "9        9       9      INSERT    NORMAL\n"
                        "11       9       9      INSERT    NORMAL\n"
                        "13       9       9      INSERT    NORMAL\n"
                        "15       9       9      INSERT    NORMAL\n"
                        "17       9       9      INSERT    NORMAL\n"
                        "2        19      19     INSERT    NORMAL\n"
                        "4        19      19     INSERT    NORMAL\n"
                        "6        19      19     INSERT    NORMAL\n"
                        "8        19      19     INSERT    NORMAL\n"
                        "10       19      19     INSERT    NORMAL\n"
                        "12       19      19     INSERT    NORMAL\n"
                        "14       19      19     INSERT    NORMAL\n"
                        "16       19      19     INSERT    NORMAL\n"
                        "18       99      99     INSERT    NORMAL\n"
                        "19       99      99     INSERT    NORMAL\n"
                        "20       99      99     INSERT    NORMAL\n"
                        "21       99      99     INSERT    NORMAL\n"
                        "22       99      99     INSERT    NORMAL\n";

  ObDatumRange range;
  ObSEArray<ObDatumRange, 8> ranges;
  generate_range(1, 3, range);
  ranges.push_back(range);
  generate_range(4, 6, range);
  ranges.push_back(range);
  generate_range(7, 22, range);
  ranges.push_back(range);
  STORAGE_LOG(INFO, "generate ranges", K(ranges));
  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 1;
  trans_version_range.multi_version_start_ = 50;
  trans_version_range.snapshot_version_ = INT64_MAX;
  prepare_scan_param(trans_version_range, table_store_iter, true/*is_delete_insert*/);

  mock_tablet_table_store(table_store_iter);
  ObMultipleMultiScanMerge multi_scan_merge;
  OK(multi_scan_merge.init(access_param_, context_, get_table_param_));
  refresh_iter(multi_scan_merge);
  OK(multi_scan_merge.open(ranges));
  multi_scan_merge.disable_padding();
  multi_scan_merge.disable_fill_virtual_column();

  int64_t count = 0;
  int64_t total_count = 0;
  ret = OB_SUCCESS;
  ObMockIterator res_iter;
  res_iter.reset();
  ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
  OK(refresh_table(multi_scan_merge, get_table_param_.tablet_iter_.table_store_iter_));
  STORAGE_LOG(INFO, "refresh tablet iter", K(get_table_param_.tablet_iter_.table_store_iter_));
  while (OB_SUCC(ret)) {
    ret = multi_scan_merge.get_next_rows(count, SQL_BATCH_SIZE);
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
  ASSERT_EQ(22, total_count);

  handle1.reset();
  handle2.reset();
  handle3.reset();
  multi_scan_merge.reset();
}

TEST_F(TestIncMajorDeleteInsertScan, test_double_refresh_table)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator table_store_iter;

  ObTableHandleV2 handle1;
  const char *micro_data1[1];
  micro_data1[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -50      DI_VERSION     9       9    INSERT    NORMAL      CLF\n"
      "3        -50      DI_VERSION     9       9    INSERT    NORMAL      CLF\n"
      "5        -50      DI_VERSION     9       9    INSERT    NORMAL      CLF\n"
      "7        -50      DI_VERSION     9       9    INSERT    NORMAL      CLF\n"
      "9        -50      DI_VERSION     9       9    INSERT    NORMAL      CLF\n";

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
  STORAGE_LOG(INFO, "finish prepare major sstable");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "2        -60      DI_VERSION   19     19    INSERT    NORMAL        CLF\n"
      "4        -60      DI_VERSION   19     19    INSERT    NORMAL        CLF\n"
      "6        -60      DI_VERSION   19     19    INSERT    NORMAL        CLF\n"
      "8        -60      DI_VERSION   19     19    INSERT    NORMAL        CLF\n";

  snapshot_version = 60;
  scn_range.start_scn_.convert_for_tx(0);
  scn_range.end_scn_.convert_for_tx(60);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  ObTabletCreateSSTableParam param2;
  prepare_create_basic_sst_param(param2, scn_range, ObITable::INC_MAJOR_SSTABLE);
  prepare_create_row_sst_param(param2);
  prepare_create_inc_sst_param(param2, {ObUncommitTxDesc(0, 70, ObUncommitTxDesc::KeyStatus::SQL_SEQ)});
  prepare_data_end_with_param(handle2, param2, 70);
  table_store_iter.add_table(handle2.get_table());
  STORAGE_LOG(INFO, "finish prepare inc major sstable 1");

  ObTableHandleV2 handle3;
  const char *micro_data3[1];
  micro_data3[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "5        -80      DI_VERSION    99     99    INSERT    NORMAL        CF\n"
      "5        -80      0              9      9    DELETE    NORMAL        CL\n"
      "6        -80      DI_VERSION    99     99    INSERT    NORMAL        CF\n"
      "6        -80      0             19     19    DELETE    NORMAL        CL\n";

  snapshot_version = 80;
  scn_range.start_scn_.convert_for_tx(0);
  scn_range.end_scn_.convert_for_tx(80);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data3, 1);
  ObTabletCreateSSTableParam param3;
  prepare_create_basic_sst_param(param3, scn_range, ObITable::MINI_SSTABLE);
  prepare_create_row_sst_param(param3);
  prepare_data_end_with_param(handle3, param3, 80);
  table_store_iter.add_table(handle3.get_table());
  STORAGE_LOG(INFO, "finish prepare mini sstable");

  const char *result1 = "bigint   bigint bigint  flag     flag_type\n"
                        "5        99      99     INSERT    NORMAL\n"
                        "1        9       9      INSERT    NORMAL\n"
                        "3        9       9      INSERT    NORMAL\n"
                        "2        19      19     INSERT    NORMAL\n"
                        "4        19      19     INSERT    NORMAL\n"
                        "6        99      99     INSERT    NORMAL\n"
                        "7        9       9      INSERT    NORMAL\n"
                        "9        9       9      INSERT    NORMAL\n"
                        "8        19      19     INSERT    NORMAL\n";

  ObDatumRange range;
  range.set_whole_range();
  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 1;
  trans_version_range.multi_version_start_ = 50;
  trans_version_range.snapshot_version_ = INT64_MAX;
  prepare_scan_param(trans_version_range, table_store_iter, true/*is_delete_insert*/);

  mock_tablet_table_store(table_store_iter);
  ObMultipleScanMerge scan_merge;
  OK(scan_merge.init(access_param_, context_, get_table_param_));
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

  // get one single row
  OK(scan_merge.get_next_rows(count, 1));
  if (count > 0) {
    ObMockScanMergeIterator merge_iter(count);
    ASSERT_EQ(OB_SUCCESS, merge_iter.init(reinterpret_cast<ObVectorStore *>(scan_merge.block_row_store_),
                                          query_allocator_,
                                          *access_param_.iter_param_.get_read_info()));
    bool is_equal = res_iter.equals<ObMockScanMergeIterator, ObStoreRow>(merge_iter, false, false, false, true);
    ASSERT_TRUE(is_equal);
    total_count += count;
  }
  ASSERT_EQ(1, total_count);

  // get two di base rows and refresh table
  OK(scan_merge.get_next_rows(count, 2));
  if (count > 0) {
    ObMockScanMergeIterator merge_iter(count);
    ASSERT_EQ(OB_SUCCESS, merge_iter.init(reinterpret_cast<ObVectorStore *>(scan_merge.block_row_store_),
                                          query_allocator_,
                                          *access_param_.iter_param_.get_read_info()));
    bool is_equal = res_iter.equals<ObMockScanMergeIterator, ObStoreRow>(merge_iter, false, false, false, true);
    ASSERT_TRUE(is_equal);
    total_count += count;
  }
  ASSERT_EQ(3, total_count);
  OK(refresh_table(scan_merge, get_table_param_.tablet_iter_.table_store_iter_));
  STORAGE_LOG(INFO, "refresh tablet iter", K(get_table_param_.tablet_iter_.table_store_iter_));

  // get two di base rows
  OK(scan_merge.get_next_rows(count, 2));
  if (count > 0) {
    ObMockScanMergeIterator merge_iter(count);
    ASSERT_EQ(OB_SUCCESS, merge_iter.init(reinterpret_cast<ObVectorStore *>(scan_merge.block_row_store_),
                                          query_allocator_,
                                          *access_param_.iter_param_.get_read_info()));
    bool is_equal = res_iter.equals<ObMockScanMergeIterator, ObStoreRow>(merge_iter, false, false, false, true);
    ASSERT_TRUE(is_equal);
    total_count += count;
  }
  ASSERT_EQ(5, total_count);

  // get one single row
  OK(scan_merge.get_next_rows(count, 1));
  if (count > 0) {
    ObMockScanMergeIterator merge_iter(count);
    ASSERT_EQ(OB_SUCCESS, merge_iter.init(reinterpret_cast<ObVectorStore *>(scan_merge.block_row_store_),
                                          query_allocator_,
                                          *access_param_.iter_param_.get_read_info()));
    bool is_equal = res_iter.equals<ObMockScanMergeIterator, ObStoreRow>(merge_iter, false, false, false, true);
    ASSERT_TRUE(is_equal);
    total_count += count;
  }
  ASSERT_EQ(6, total_count);

  // get two di base rows and refresh table
  OK(scan_merge.get_next_rows(count, 2));
  if (count > 0) {
    ObMockScanMergeIterator merge_iter(count);
    ASSERT_EQ(OB_SUCCESS, merge_iter.init(reinterpret_cast<ObVectorStore *>(scan_merge.block_row_store_),
                                          query_allocator_,
                                          *access_param_.iter_param_.get_read_info()));
    bool is_equal = res_iter.equals<ObMockScanMergeIterator, ObStoreRow>(merge_iter, false, false, false, true);
    ASSERT_TRUE(is_equal);
    total_count += count;
  }
  ASSERT_EQ(8, total_count);
  OK(refresh_table(scan_merge, get_table_param_.tablet_iter_.table_store_iter_));
  STORAGE_LOG(INFO, "refresh tablet iter", K(get_table_param_.tablet_iter_.table_store_iter_));


  // get residual rows
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
  ASSERT_EQ(9, total_count);

  handle1.reset();
  handle2.reset();
  handle3.reset();
  scan_merge.reset();
}

TEST_F(TestIncMajorDeleteInsertScan, test_switch_table)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator table_store_iter;

  ObTableHandleV2 handle1;
  const char *micro_data1[1];
  micro_data1[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -50      DI_VERSION     9       9    INSERT    NORMAL      CLF\n"
      "3        -50      DI_VERSION     9       9    INSERT    NORMAL      CLF\n"
      "5        -50      DI_VERSION     9       9    INSERT    NORMAL      CLF\n"
      "7        -50      DI_VERSION     9       9    INSERT    NORMAL      CLF\n"
      "9        -50      DI_VERSION     9       9    INSERT    NORMAL      CLF\n";

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
  STORAGE_LOG(INFO, "finish prepare major sstable");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "2        -60      DI_VERSION   19     19    INSERT    NORMAL        CLF\n"
      "4        -60      DI_VERSION   19     19    INSERT    NORMAL        CLF\n"
      "6        -60      DI_VERSION   19     19    INSERT    NORMAL        CLF\n"
      "8        -60      DI_VERSION   19     19    INSERT    NORMAL        CLF\n";

  snapshot_version = 60;
  scn_range.start_scn_.convert_for_tx(0);
  scn_range.end_scn_.convert_for_tx(60);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  ObTabletCreateSSTableParam param2;
  prepare_create_basic_sst_param(param2, scn_range, ObITable::INC_MAJOR_SSTABLE);
  prepare_create_row_sst_param(param2);
  prepare_create_inc_sst_param(param2, {ObUncommitTxDesc(0, 70, ObUncommitTxDesc::KeyStatus::SQL_SEQ)});
  prepare_data_end_with_param(handle2, param2, 70);
  table_store_iter.add_table(handle2.get_table());
  STORAGE_LOG(INFO, "finish prepare inc major sstable 1");

  ObTableHandleV2 handle3;
  const char *micro_data3[1];
  micro_data3[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "5        -80      DI_VERSION    99     99    INSERT    NORMAL        CF\n"
      "5        -80      0              9      9    DELETE    NORMAL        CL\n"
      "6        -80      DI_VERSION    99     99    INSERT    NORMAL        CF\n"
      "6        -80      0             19     19    DELETE    NORMAL        CL\n";

  snapshot_version = 80;
  scn_range.start_scn_.convert_for_tx(0);
  scn_range.end_scn_.convert_for_tx(80);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data3, 1);
  ObTabletCreateSSTableParam param3;
  prepare_create_basic_sst_param(param3, scn_range, ObITable::MINI_SSTABLE);
  prepare_create_row_sst_param(param3);
  prepare_data_end_with_param(handle3, param3, 80);
  table_store_iter.add_table(handle3.get_table());
  STORAGE_LOG(INFO, "finish prepare mini sstable");

  const char *result1 = "bigint   bigint bigint  flag     flag_type\n"
                        "5        99      99     INSERT    NORMAL\n"
                        "1        9       9      INSERT    NORMAL\n"
                        "3        9       9      INSERT    NORMAL\n"
                        "2        19      19     INSERT    NORMAL\n"
                        "4        19      19     INSERT    NORMAL\n"
                        "6        99      99     INSERT    NORMAL\n"
                        "7        9       9      INSERT    NORMAL\n"
                        "9        9       9      INSERT    NORMAL\n"
                        "8        19      19     INSERT    NORMAL\n";

  ObDatumRange range;
  range.set_whole_range();
  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 1;
  trans_version_range.multi_version_start_ = 50;
  trans_version_range.snapshot_version_ = INT64_MAX;
  prepare_scan_param(trans_version_range, table_store_iter, true/*is_delete_insert*/);

  mock_tablet_table_store(table_store_iter);
  ObMultipleScanMerge scan_merge;
  OK(scan_merge.init(access_param_, context_, get_table_param_));
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

  // get one single row
  OK(scan_merge.get_next_rows(count, 1));
  ASSERT_EQ(1, count);

  // switch table
  scan_merge.reuse();
  scan_merge.tree_cmp_.reset();
  scan_merge.rows_merger_ = nullptr;
  scan_merge.reset_iter_array();
  scan_merge.inner_reset();
  scan_merge.tables_.reuse();
  scan_merge.unprojected_row_.row_flag_.reset();
  scan_merge.unprojected_row_.trans_info_ = nullptr;
  scan_merge.out_project_cols_.reuse();
  OK(scan_merge.switch_table(access_param_, context_, get_table_param_));
  OK(scan_merge.open(range));
  scan_merge.disable_padding();
  scan_merge.disable_fill_virtual_column();

  // get residual rows
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
  ASSERT_EQ(9, total_count);

  handle1.reset();
  handle2.reset();
  handle3.reset();
  scan_merge.reset();
}

TEST_F(TestIncMajorDeleteInsertScan, test_switch_param)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator table_store_iter;

  ObTableHandleV2 handle1;
  const char *micro_data1[1];
  micro_data1[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -50      DI_VERSION     9       9    INSERT    NORMAL      CLF\n"
      "3        -50      DI_VERSION     9       9    INSERT    NORMAL      CLF\n"
      "5        -50      DI_VERSION     9       9    INSERT    NORMAL      CLF\n"
      "7        -50      DI_VERSION     9       9    INSERT    NORMAL      CLF\n"
      "9        -50      DI_VERSION     9       9    INSERT    NORMAL      CLF\n";

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
  STORAGE_LOG(INFO, "finish prepare major sstable");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "2        -60      DI_VERSION   19     19    INSERT    NORMAL        CLF\n"
      "4        -60      DI_VERSION   19     19    INSERT    NORMAL        CLF\n"
      "6        -60      DI_VERSION   19     19    INSERT    NORMAL        CLF\n"
      "8        -60      DI_VERSION   19     19    INSERT    NORMAL        CLF\n";

  snapshot_version = 60;
  scn_range.start_scn_.convert_for_tx(0);
  scn_range.end_scn_.convert_for_tx(60);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  ObTabletCreateSSTableParam param2;
  prepare_create_basic_sst_param(param2, scn_range, ObITable::INC_MAJOR_SSTABLE);
  prepare_create_row_sst_param(param2);
  prepare_create_inc_sst_param(param2, {ObUncommitTxDesc(0, 70, ObUncommitTxDesc::KeyStatus::SQL_SEQ)});
  prepare_data_end_with_param(handle2, param2, 70);
  table_store_iter.add_table(handle2.get_table());
  STORAGE_LOG(INFO, "finish prepare inc major sstable 1");

  ObTableHandleV2 handle3;
  const char *micro_data3[1];
  micro_data3[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "5        -80      DI_VERSION    99     99    INSERT    NORMAL        CF\n"
      "5        -80      0              9      9    DELETE    NORMAL        CL\n"
      "6        -80      DI_VERSION    99     99    INSERT    NORMAL        CF\n"
      "6        -80      0             19     19    DELETE    NORMAL        CL\n";

  snapshot_version = 80;
  scn_range.start_scn_.convert_for_tx(0);
  scn_range.end_scn_.convert_for_tx(80);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data3, 1);
  ObTabletCreateSSTableParam param3;
  prepare_create_basic_sst_param(param3, scn_range, ObITable::MINI_SSTABLE);
  prepare_create_row_sst_param(param3);
  prepare_data_end_with_param(handle3, param3, 80);
  table_store_iter.add_table(handle3.get_table());
  STORAGE_LOG(INFO, "finish prepare mini sstable");

  const char *result1 = "bigint   bigint bigint  flag     flag_type\n"
                        "5        99      99     INSERT    NORMAL\n"
                        "1        9       9      INSERT    NORMAL\n"
                        "3        9       9      INSERT    NORMAL\n"
                        "2        19      19     INSERT    NORMAL\n"
                        "4        19      19     INSERT    NORMAL\n"
                        "6        99      99     INSERT    NORMAL\n"
                        "7        9       9      INSERT    NORMAL\n"
                        "9        9       9      INSERT    NORMAL\n"
                        "8        19      19     INSERT    NORMAL\n";

  ObDatumRange range;
  range.set_whole_range();
  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 1;
  trans_version_range.multi_version_start_ = 50;
  trans_version_range.snapshot_version_ = INT64_MAX;
  prepare_scan_param(trans_version_range, table_store_iter, true/*is_delete_insert*/);

  mock_tablet_table_store(table_store_iter);
  ObMultipleScanMerge scan_merge;
  OK(scan_merge.init(access_param_, context_, get_table_param_));
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

  // get one single row
  OK(scan_merge.get_next_rows(count, 1));
  ASSERT_EQ(1, count);

  // switch param
  scan_merge.reuse();
  OK(scan_merge.switch_param(access_param_, context_, get_table_param_));
  OK(scan_merge.open(range));

  // get residual rows
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
  ASSERT_EQ(9, total_count);

  handle1.reset();
  handle2.reset();
  handle3.reset();
  scan_merge.reset();
}

TEST_F(TestIncMajorDeleteInsertScan, test_delete_insert_continue_overwrite_major)
{
int ret = OB_SUCCESS;
  ObTableStoreIterator table_store_iter;

  ObTableHandleV2 handle1;
  const char *micro_data1[1];
  micro_data1[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "6        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "11       -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "16       -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "21       -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n";

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
  ObTableHandleV2 co_handle1;
  convert_to_co_sstable(handle1, co_handle1);
  table_store_iter.add_table(co_handle1.get_table());
  STORAGE_LOG(INFO, "finish prepare major sstable", KPC(co_handle1.get_table()));

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "2        -60      DI_VERSION    9      9    INSERT    NORMAL        CLF\n"
      "7        -60      DI_VERSION    9      9    INSERT    NORMAL        CLF\n"
      "12       -60      DI_VERSION    9      9    INSERT    NORMAL        CLF\n"
      "17       -60      DI_VERSION    9      9    INSERT    NORMAL        CLF\n"
      "22       -60      DI_VERSION    9      9    INSERT    NORMAL        CLF\n";

  snapshot_version = 60;
  scn_range.start_scn_.convert_for_tx(0);
  scn_range.end_scn_.convert_for_tx(60);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  ObTabletCreateSSTableParam param2;
  prepare_create_basic_sst_param(param2, scn_range, ObITable::INC_MAJOR_SSTABLE);
  prepare_create_row_sst_param(param2);
  prepare_create_inc_sst_param(param2, {ObUncommitTxDesc(0, 70, ObUncommitTxDesc::KeyStatus::SQL_SEQ)});
  prepare_data_end_with_param(handle2, param2, 70);
  table_store_iter.add_table(handle2.get_table());
  STORAGE_LOG(INFO, "finish prepare inc major sstable 1", KPC(handle2.get_table()));

  ObTableHandleV2 handle3;
  const char *micro_data3[1];
  micro_data3[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "3        -70      DI_VERSION    9      9    INSERT    NORMAL        CLF\n"
      "8        -70      DI_VERSION    9      9    INSERT    NORMAL        CLF\n"
      "13       -70      DI_VERSION    9      9    INSERT    NORMAL        CLF\n"
      "18       -70      DI_VERSION    9      9    INSERT    NORMAL        CLF\n"
      "23       -70      DI_VERSION    9      9    INSERT    NORMAL        CLF\n";

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
  STORAGE_LOG(INFO, "finish prepare inc major sstable 2", KPC(handle3.get_table()));

  ObTableHandleV2 handle4;
  const char *micro_data4[1];
  micro_data4[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "4        -80      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "9        -80      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "14       -80      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "19       -80      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "24       -80      DI_VERSION    9       9    INSERT    NORMAL      CLF\n";

  snapshot_version = 80;
  scn_range.start_scn_.convert_for_tx(0);
  scn_range.end_scn_.convert_for_tx(80);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data4, 1);
  ObTabletCreateSSTableParam param4;
  prepare_create_basic_sst_param(param4, scn_range, ObITable::INC_MAJOR_SSTABLE);
  prepare_create_column_sst_param(param4);
  prepare_create_inc_sst_param(param4, {ObUncommitTxDesc(0, 90, ObUncommitTxDesc::KeyStatus::SQL_SEQ)});
  prepare_data_end_with_param(handle4, param4, 90);
  table_store_iter.add_table(handle4.get_table());
  STORAGE_LOG(INFO, "finish prepare inc major sstable 3", KPC(handle4.get_table()));

  ObTableHandleV2 handle5;
  const char *micro_data5[1];
  micro_data5[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "5        -90      DI_VERSION    9      9    INSERT    NORMAL        CLF\n"
      "10       -90      DI_VERSION    9      9    INSERT    NORMAL        CLF\n"
      "15       -90      DI_VERSION    9      9    INSERT    NORMAL        CLF\n"
      "20       -90      DI_VERSION    9      9    INSERT    NORMAL        CLF\n"
      "25       -90      DI_VERSION    9      9    INSERT    NORMAL        CLF\n";

  snapshot_version = 90;
  scn_range.start_scn_.convert_for_tx(0);
  scn_range.end_scn_.convert_for_tx(90);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data5, 1);
  ObTabletCreateSSTableParam param5;
  prepare_create_basic_sst_param(param5, scn_range, ObITable::INC_MAJOR_SSTABLE);
  prepare_create_column_sst_param(param5);
  prepare_create_inc_sst_param(param5, {ObUncommitTxDesc(0, 100, ObUncommitTxDesc::KeyStatus::SQL_SEQ)});
  prepare_data_end_with_param(handle5, param5, 100);
  table_store_iter.add_table(handle5.get_table());
  STORAGE_LOG(INFO, "finish prepare inc major sstable 4", KPC(handle5.get_table()));

  ObTableHandleV2 handle6;
  const char *micro_data6[1];
  micro_data6[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -100     DI_VERSION   200    200    INSERT    NORMAL        CF\n"
      "1        -100     0          9      9      DELETE    NORMAL        CL\n"
      "2        -100     DI_VERSION   200    200    INSERT    NORMAL        CF\n"
      "2        -100     0          9      9      DELETE    NORMAL        CL\n"
      "3        -100     DI_VERSION   200    200    INSERT    NORMAL        CF\n"
      "3        -100     0          9      9      DELETE    NORMAL        CL\n"
      "4        -100     DI_VERSION   200    200    INSERT    NORMAL        CF\n"
      "4        -100     0          9      9      DELETE    NORMAL        CL\n"
      "5        -100     DI_VERSION   200    200    INSERT    NORMAL        CF\n"
      "5        -100     0          9      9      DELETE    NORMAL        CL\n"
      "6        -100     DI_VERSION   200    200    INSERT    NORMAL        CF\n"
      "6        -100     0          9      9      DELETE    NORMAL        CL\n"
      "7        -100     DI_VERSION   200    200    INSERT    NORMAL        CF\n"
      "7        -100     0          9      9      DELETE    NORMAL        CL\n"
      "8        -100     DI_VERSION   200    200    INSERT    NORMAL        CF\n"
      "8        -100     0          9      9      DELETE    NORMAL        CL\n"
      "9        -100     DI_VERSION   200    200    INSERT    NORMAL        CF\n"
      "9        -100     0          9      9      DELETE    NORMAL        CL\n"
      "10       -100     DI_VERSION   200    200    INSERT    NORMAL        CF\n"
      "10       -100     0          9      9      DELETE    NORMAL        CL\n"
      "11       -100     DI_VERSION   200    200    INSERT    NORMAL        CF\n"
      "11       -100     0          9      9      DELETE    NORMAL        CL\n"
      "12       -100     DI_VERSION   200    200    INSERT    NORMAL        CF\n"
      "12       -100     0          9      9      DELETE    NORMAL        CL\n"
      "13       -100     DI_VERSION   200    200    INSERT    NORMAL        CF\n"
      "13       -100     0          9      9      DELETE    NORMAL        CL\n"
      "14       -100     DI_VERSION   200    200    INSERT    NORMAL        CF\n"
      "14       -100     0          9      9      DELETE    NORMAL        CL\n"
      "15       -100     DI_VERSION   200    200    INSERT    NORMAL        CF\n"
      "15       -100     0          9      9      DELETE    NORMAL        CL\n"
      "16       -100     DI_VERSION   200    200    INSERT    NORMAL        CF\n"
      "16       -100     0          9      9      DELETE    NORMAL        CL\n"
      "17       -100     DI_VERSION   200    200    INSERT    NORMAL        CF\n"
      "17       -100     0          9      9      DELETE    NORMAL        CL\n"
      "18       -100     DI_VERSION   200    200    INSERT    NORMAL        CF\n"
      "18       -100     0          9      9      DELETE    NORMAL        CL\n"
      "19       -100     DI_VERSION   200    200    INSERT    NORMAL        CF\n"
      "19       -100     0          9      9      DELETE    NORMAL        CL\n"
      "20       -100     DI_VERSION   200    200    INSERT    NORMAL        CF\n"
      "20       -100     0          9      9      DELETE    NORMAL        CL\n"
      "21       -100     DI_VERSION   200    200    INSERT    NORMAL        CF\n"
      "21       -100     0          9      9      DELETE    NORMAL        CL\n"
      "22       -100     DI_VERSION   200    200    INSERT    NORMAL        CF\n"
      "22       -100     0          9      9      DELETE    NORMAL        CL\n"
      "23       -100     DI_VERSION   200    200    INSERT    NORMAL        CF\n"
      "23       -100     0          9      9      DELETE    NORMAL        CL\n"
      "24       -100     DI_VERSION   200    200    INSERT    NORMAL        CF\n"
      "24       -100     0          9      9      DELETE    NORMAL        CL\n"
      "25       -100     DI_VERSION   200    200    INSERT    NORMAL        CF\n"
      "25       -100     0          9      9      DELETE    NORMAL        CL\n";

  snapshot_version = 100;
  scn_range.start_scn_.convert_for_tx(50);
  scn_range.end_scn_.convert_for_tx(100);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data6, 1);
  ObTabletCreateSSTableParam param6;
  prepare_create_basic_sst_param(param6, scn_range, ObITable::MINI_SSTABLE);
  prepare_data_end_with_param(handle6, param6, 100);
  table_store_iter.add_table(handle6.get_table());
  STORAGE_LOG(INFO, "finish prepare mini sstable", KPC(handle6.get_table()));

  const char *result1 = "bigint   bigint bigint  flag     flag_type\n"
                        "1        200     200    INSERT    NORMAL\n"
                        "2        200     200    INSERT    NORMAL\n"
                        "3        200     200    INSERT    NORMAL\n"
                        "4        200     200    INSERT    NORMAL\n"
                        "5        200     200    INSERT    NORMAL\n"
                        "6        200     200    INSERT    NORMAL\n"
                        "7        200     200    INSERT    NORMAL\n"
                        "8        200     200    INSERT    NORMAL\n"
                        "9        200     200    INSERT    NORMAL\n"
                        "10       200     200    INSERT    NORMAL\n"
                        "11       200     200    INSERT    NORMAL\n"
                        "12       200     200    INSERT    NORMAL\n"
                        "13       200     200    INSERT    NORMAL\n"
                        "14       200     200    INSERT    NORMAL\n"
                        "15       200     200    INSERT    NORMAL\n"
                        "16       200     200    INSERT    NORMAL\n"
                        "17       200     200    INSERT    NORMAL\n"
                        "18       200     200    INSERT    NORMAL\n"
                        "19       200     200    INSERT    NORMAL\n"
                        "20       200     200    INSERT    NORMAL\n"
                        "21       200     200    INSERT    NORMAL\n"
                        "22       200     200    INSERT    NORMAL\n"
                        "23       200     200    INSERT    NORMAL\n"
                        "24       200     200    INSERT    NORMAL\n"
                        "25       200     200    INSERT    NORMAL\n";

  ObDatumRange range;
  range.set_whole_range();
  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 1;
  trans_version_range.multi_version_start_ = 50;
  trans_version_range.snapshot_version_ = INT64_MAX;
  prepare_scan_param(trans_version_range, table_store_iter, true/*is_delete_insert*/);
  STORAGE_LOG(INFO, "check table store iter", K(table_store_iter));

  mock_tablet_table_store(table_store_iter);
  ObMultipleScanMerge scan_merge;
  OK(scan_merge.init(access_param_, context_, get_table_param_));
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
  ASSERT_EQ(25, total_count);

  handle1.reset();
  handle2.reset();
  handle3.reset();
  handle4.reset();
  handle5.reset();
  handle6.reset();
  scan_merge.reset();
}

TEST_F(TestIncMajorDeleteInsertScan, test_apply_filter_empty)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator table_store_iter;

  ObTableHandleV2 handle1;
  const char *micro_data1[1];
  micro_data1[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -50      DI_VERSION     9       9    INSERT    NORMAL      CLF\n"
      "3        -50      DI_VERSION     9       9    INSERT    NORMAL      CLF\n"
      "5        -50      DI_VERSION     9       9    INSERT    NORMAL      CLF\n"
      "7        -50      DI_VERSION     9       9    INSERT    NORMAL      CLF\n"
      "9        -50      DI_VERSION     9       9    INSERT    NORMAL      CLF\n";

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
  STORAGE_LOG(INFO, "finish prepare major sstable");

  const char *result1 = "bigint   bigint bigint  flag     flag_type\n";

  ObDatumRange range;
  generate_range(6, 6, range);
  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 1;
  trans_version_range.multi_version_start_ = 50;
  trans_version_range.snapshot_version_ = INT64_MAX;
  prepare_scan_param(trans_version_range, table_store_iter, true/*is_delete_insert*/);

  mock_tablet_table_store(table_store_iter);
  ObMultipleScanMerge scan_merge;
  OK(scan_merge.init(access_param_, context_, get_table_param_));
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
  ASSERT_EQ(OB_ITER_END, scan_merge.get_next_rows(count, 1));

  handle1.reset();
  scan_merge.reset();
}

TEST_F(TestIncMajorDeleteInsertScan, test_multi_block)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator table_store_iter;

  ObTableHandleV2 handle1;
  const char *micro_data1[3];
  micro_data1[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "3        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "5        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "7        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n";
  micro_data1[1] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "9        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "11       -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "13       -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n";
  micro_data1[2] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "15       -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "17       -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n";

  int schema_rowkey_cnt = 1;
  int64_t snapshot_version = 50;
  ObScnRange scn_range;
  scn_range.start_scn_.convert_for_tx(0);
  scn_range.end_scn_.convert_for_tx(50);
  prepare_table_schema(micro_data1, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data1, 3);
  ObTabletCreateSSTableParam param1;
  prepare_create_basic_sst_param(param1, scn_range, ObITable::MAJOR_SSTABLE);
  prepare_create_row_sst_param(param1);
  prepare_data_end_with_param(handle1, param1, 50);
  table_store_iter.add_table(handle1.get_table());
  STORAGE_LOG(INFO, "finish prepare major sstable", K(handle1.get_table()));

  ObTableHandleV2 handle2;
  const char *micro_data2[4];
  micro_data2[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "2        -70      DI_VERSION    9       9    INSERT    NORMAL        CLF\n"
      "4        -70      DI_VERSION    9       9    INSERT    NORMAL        CLF\n"
      "6        -70      DI_VERSION    9       9    INSERT    NORMAL        CLF\n"
      "8        -70      DI_VERSION    9       9    INSERT    NORMAL        CLF\n";
  micro_data2[1] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "10       -70      DI_VERSION    9       9    INSERT    NORMAL        CLF\n"
      "12       -70      DI_VERSION    9       9    INSERT    NORMAL        CLF\n"
      "14       -70      DI_VERSION    9       9    INSERT    NORMAL        CLF\n";
  micro_data2[2] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "16       -70      DI_VERSION    9       9    INSERT    NORMAL        CLF\n"
      "18       -70      DI_VERSION    9       9    INSERT    NORMAL        CLF\n"
      "19       -70      DI_VERSION    9       9    INSERT    NORMAL        CLF\n"
      "20       -70      DI_VERSION    9       9    INSERT    NORMAL        CLF\n";
  micro_data2[3] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "21       -70      DI_VERSION    9       9    INSERT    NORMAL        CLF\n"
      "22       -70      DI_VERSION    9       9    INSERT    NORMAL        CLF\n";

  snapshot_version = 70;
  scn_range.start_scn_.convert_for_tx(0);
  scn_range.end_scn_.convert_for_tx(70);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 4);
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
      "1        -80      DI_VERSION  100    100    INSERT    NORMAL        CF\n"
      "1        -80      0            9      9     DELETE    NORMAL        CL\n"
      "4        -80      DI_VERSION  100    100    INSERT    NORMAL        CF\n"
      "4        -80      0            9      9     DELETE    NORMAL        CL\n"
      "6        -80      DI_VERSION  100    100    INSERT    NORMAL        CF\n"
      "6        -80      0            9      9     DELETE    NORMAL        CL\n"
      "11       -80      DI_VERSION  100    100    INSERT    NORMAL        CF\n"
      "11       -80      0            9      9     DELETE    NORMAL        CL\n"
      "16       -80      DI_VERSION  100    100    INSERT    NORMAL        CF\n"
      "16       -80      0            9      9     DELETE    NORMAL        CL\n"
      "21       -80      DI_VERSION  100    100    INSERT    NORMAL        CF\n"
      "21       -80      0            9      9     DELETE    NORMAL        CL\n";

  snapshot_version = 80;
  scn_range.start_scn_.convert_for_tx(70);
  scn_range.end_scn_.convert_for_tx(80);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data3, 1);
  ObTabletCreateSSTableParam param3;
  prepare_create_basic_sst_param(param3, scn_range, ObITable::MINI_SSTABLE);
  prepare_data_end_with_param(handle3, param3, 80);
  table_store_iter.add_table(handle3.get_table());
  STORAGE_LOG(INFO, "finish prepare mini sstable", K(handle3.get_table()));

  const char *result1 = "bigint   bigint bigint  flag     flag_type\n"
                        "1        100     100    INSERT    NORMAL\n"
                        "4        100     100    INSERT    NORMAL\n"
                        "3        9       9      INSERT    NORMAL\n"
                        "2        9       9      INSERT    NORMAL\n"
                        "6        100     100    INSERT    NORMAL\n"
                        "5        9       9      INSERT    NORMAL\n"
                        "11       100     100    INSERT    NORMAL\n"
                        "7        9       9      INSERT    NORMAL\n"
                        "9        9       9      INSERT    NORMAL\n"
                        "8        9       9      INSERT    NORMAL\n"
                        "10       9       9      INSERT    NORMAL\n"
                        "16       100     100    INSERT    NORMAL\n"
                        "13       9       9      INSERT    NORMAL\n"
                        "15       9       9      INSERT    NORMAL\n"
                        "12       9       9      INSERT    NORMAL\n"
                        "14       9       9      INSERT    NORMAL\n"
                        "21       100     100    INSERT    NORMAL\n"
                        "17       9       9      INSERT    NORMAL\n"
                        "18       9       9      INSERT    NORMAL\n"
                        "19       9       9      INSERT    NORMAL\n"
                        "20       9       9      INSERT    NORMAL\n"
                        "22       9       9      INSERT    NORMAL\n";

  ObDatumRange range;
  range.set_whole_range();
  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 1;
  trans_version_range.multi_version_start_ = 50;
  trans_version_range.snapshot_version_ = INT64_MAX;
  prepare_scan_param(trans_version_range, table_store_iter, true/*is_delete_insert*/);

  mock_tablet_table_store(table_store_iter);
  ObMultipleScanMerge scan_merge;
  OK(scan_merge.init(access_param_, context_, get_table_param_));
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
  while (OB_SUCC(ret)) {
    ret = scan_merge.get_next_rows(count, SQL_BATCH_SIZE);
    if (ret != OB_SUCCESS && ret != OB_ITER_END) {
      STORAGE_LOG(ERROR, "error return value", K(ret), K(count));
      ASSERT_EQ(1, 0);
    }
    if (count > 0) {
      ObMockScanMergeIterator merge_iter(count);
      EXPECT_EQ(OB_SUCCESS, merge_iter.init(reinterpret_cast<ObVectorStore *>(scan_merge.block_row_store_),
                                            query_allocator_,
                                            *access_param_.iter_param_.get_read_info()));
      bool is_equal = res_iter.equals<ObMockScanMergeIterator, ObStoreRow>(merge_iter, false, false, false, true);
      EXPECT_TRUE(is_equal);

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
  scan_merge.reset();
}

TEST_F(TestIncMajorDeleteInsertScan, test_refresh_table_intermediate)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator table_store_iter;

  ObTableHandleV2 handle1;
  const char *micro_data1[1];
  micro_data1[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "3        -60      DI_VERSION    9      9    INSERT    NORMAL        CLF\n"
      "4        -60      DI_VERSION    9      9    INSERT    NORMAL        CLF\n"
      "11       -60      DI_VERSION    9      9    INSERT    NORMAL        CLF\n"
      "14       -60      DI_VERSION    9      9    INSERT    NORMAL        CLF\n";

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
  STORAGE_LOG(INFO, "finish prepare major sstable");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "2        -50      DI_VERSION     9       9    INSERT    NORMAL      CLF\n"
      "5        -50      DI_VERSION     9       9    INSERT    NORMAL      CLF\n"
      "9        -50      DI_VERSION     9       9    INSERT    NORMAL      CLF\n"
      "13       -50      DI_VERSION     9       9    INSERT    NORMAL      CLF\n";


  snapshot_version = 60;
  scn_range.start_scn_.convert_for_tx(0);
  scn_range.end_scn_.convert_for_tx(60);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  ObTabletCreateSSTableParam param2;
  prepare_create_basic_sst_param(param2, scn_range, ObITable::INC_MAJOR_SSTABLE);
  prepare_create_row_sst_param(param2);
  prepare_create_inc_sst_param(param2, {ObUncommitTxDesc(0, 70, ObUncommitTxDesc::KeyStatus::SQL_SEQ)});
  prepare_data_end_with_param(handle2, param2, 70);
  table_store_iter.add_table(handle2.get_table());
  STORAGE_LOG(INFO, "finish prepare inc major sstable 1");

  ObTableHandleV2 handle3;
  const char *micro_data3[1];
  micro_data3[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -70      DI_VERSION    9      9    INSERT    NORMAL        CLF\n"
      "8        -70      DI_VERSION    9      9    INSERT    NORMAL        CLF\n"
      "12       -70      DI_VERSION    9      9    INSERT    NORMAL        CLF\n"
      "15       -70      DI_VERSION    9      9    INSERT    NORMAL        CLF\n";

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
      "1        -80      DI_VERSION    99     99    INSERT    NORMAL        CF\n"
      "1        -80      0              9      9    DELETE    NORMAL        CL\n"
      "5        -80      DI_VERSION    99     99    INSERT    NORMAL        CF\n"
      "5        -80      0              9      9    DELETE    NORMAL        CL\n";

  snapshot_version = 90;
  scn_range.start_scn_.convert_for_tx(0);
  scn_range.end_scn_.convert_for_tx(90);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data4, 1);
  ObTabletCreateSSTableParam param4;
  prepare_create_basic_sst_param(param4, scn_range, ObITable::MINI_SSTABLE);
  prepare_create_row_sst_param(param4);
  prepare_data_end_with_param(handle4, param4, 90);
  table_store_iter.add_table(handle4.get_table());
  STORAGE_LOG(INFO, "finish prepare mini sstable");

  const char *result1 = "bigint   bigint bigint  flag     flag_type\n"
                        "1        99      99     INSERT    NORMAL\n"
                        "5        99      99     INSERT    NORMAL\n"
                        "3        9       9      INSERT    NORMAL\n"
                        "4        9       9      INSERT    NORMAL\n"
                        "2        9       9      INSERT    NORMAL\n"
                        "11       9       9      INSERT    NORMAL\n"
                        "14       9       9      INSERT    NORMAL\n"
                        "9        9       9      INSERT    NORMAL\n"
                        "13       9       9      INSERT    NORMAL\n"
                        "8        9       9      INSERT    NORMAL\n"
                        "12       9       9      INSERT    NORMAL\n"
                        "15       9       9      INSERT    NORMAL\n";

  ObDatumRange range;
  range.set_whole_range();
  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 1;
  trans_version_range.multi_version_start_ = 50;
  trans_version_range.snapshot_version_ = INT64_MAX;
  prepare_scan_param(trans_version_range, table_store_iter, true/*is_delete_insert*/);

  mock_tablet_table_store(table_store_iter);
  ObMultipleScanMerge scan_merge;
  OK(scan_merge.init(access_param_, context_, get_table_param_));
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

  // get one single row
  OK(scan_merge.get_next_rows(count, 1));
  if (count > 0) {
    ObMockScanMergeIterator merge_iter(count);
    ASSERT_EQ(OB_SUCCESS, merge_iter.init(reinterpret_cast<ObVectorStore *>(scan_merge.block_row_store_),
                                          query_allocator_,
                                          *access_param_.iter_param_.get_read_info()));
    bool is_equal = res_iter.equals<ObMockScanMergeIterator, ObStoreRow>(merge_iter, false, false, false, true);
    ASSERT_TRUE(is_equal);
    total_count += count;
  }
  ASSERT_EQ(1, total_count);

  // get one single row
  OK(scan_merge.get_next_rows(count, 1));
  if (count > 0) {
    ObMockScanMergeIterator merge_iter(count);
    ASSERT_EQ(OB_SUCCESS, merge_iter.init(reinterpret_cast<ObVectorStore *>(scan_merge.block_row_store_),
                                          query_allocator_,
                                          *access_param_.iter_param_.get_read_info()));
    bool is_equal = res_iter.equals<ObMockScanMergeIterator, ObStoreRow>(merge_iter, false, false, false, true);
    ASSERT_TRUE(is_equal);
    total_count += count;
  }
  ASSERT_EQ(2, total_count);

  // get two di base rows
  OK(scan_merge.get_next_rows(count, 2));
  if (count > 0) {
    ObMockScanMergeIterator merge_iter(count);
    ASSERT_EQ(OB_SUCCESS, merge_iter.init(reinterpret_cast<ObVectorStore *>(scan_merge.block_row_store_),
                                          query_allocator_,
                                          *access_param_.iter_param_.get_read_info()));
    bool is_equal = res_iter.equals<ObMockScanMergeIterator, ObStoreRow>(merge_iter, false, false, false, true);
    ASSERT_TRUE(is_equal);
    total_count += count;
  }
  ASSERT_EQ(4, total_count);

  // get one di base row
  OK(scan_merge.get_next_rows(count, 1));
  if (count > 0) {
    ObMockScanMergeIterator merge_iter(count);
    ASSERT_EQ(OB_SUCCESS, merge_iter.init(reinterpret_cast<ObVectorStore *>(scan_merge.block_row_store_),
                                          query_allocator_,
                                          *access_param_.iter_param_.get_read_info()));
    bool is_equal = res_iter.equals<ObMockScanMergeIterator, ObStoreRow>(merge_iter, false, false, false, true);
    ASSERT_TRUE(is_equal);
    total_count += count;
  }
  ASSERT_EQ(5, total_count);
  OK(refresh_table(scan_merge, get_table_param_.tablet_iter_.table_store_iter_));
  STORAGE_LOG(INFO, "refresh tablet iter", K(get_table_param_.tablet_iter_.table_store_iter_));

  // get residual rows
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
  ASSERT_EQ(12, total_count);

  handle1.reset();
  handle2.reset();
  handle3.reset();
  handle4.reset();
  scan_merge.reset();
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv) {
  system("rm -rf test_inc_major_delete_insert_scan.log*");
  OB_LOGGER.set_file_name("test_inc_major_delete_insert_scan.log");
  OB_LOGGER.set_log_level("INFO");
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
