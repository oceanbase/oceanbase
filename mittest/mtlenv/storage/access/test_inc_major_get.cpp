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

class TestIncMajorGet : public TestScanBasic
{
public:
  TestIncMajorGet();
  virtual ~TestIncMajorGet() {}

  void SetUp();
  void TearDown();
  static void SetUpTestCase();
  static void TearDownTestCase();
};

void TestIncMajorGet::SetUpTestCase()
{
  TestScanBasic::SetUpTestCase();
}

void TestIncMajorGet::TearDownTestCase()
{
  TestScanBasic::TearDownTestCase();
}

TestIncMajorGet::TestIncMajorGet()
    : TestScanBasic("test_inc_major_delete_insert_scan") {}

void TestIncMajorGet::SetUp() { TestScanBasic::SetUp(); }

void TestIncMajorGet::TearDown()
{
  TestScanBasic::TearDown();
}

TEST_F(TestIncMajorGet, test_crossed_version_minor_get)
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
      "2        -40      0           NOP    NOP    DELETE    NORMAL        CLF\n"
      "3        -60      0           NOP    NOP    DELETE    NORMAL        CLF\n";

  snapshot_version = 40;
  scn_range.start_scn_.convert_for_tx(40);
  scn_range.end_scn_.convert_for_tx(60);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data3, 1);
  ObTabletCreateSSTableParam param3;
  prepare_create_basic_sst_param(param3, scn_range, ObITable::MINI_SSTABLE);
  prepare_data_end_with_param(handle3, param3, 60);
  table_store_iter.add_table(handle3.get_table());
  STORAGE_LOG(INFO, "finish prepare mini sstable", K(handle3.get_table()));

  ObDatumRowkey rowkey;
  generate_rowkey(2, rowkey);
  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 0;
  trans_version_range.multi_version_start_ = 0;
  trans_version_range.snapshot_version_ = INT64_MAX;
  prepare_get_param(trans_version_range, table_store_iter);

  mock_tablet_table_store(table_store_iter);
  ObSingleMerge single_merge;
  OK(single_merge.init(access_param_, context_, get_table_param_));
  refresh_iter(single_merge);
  OK(single_merge.open(rowkey));
  single_merge.disable_padding();
  single_merge.disable_fill_virtual_column();
  ObDatumRow *row = nullptr;
  OK(single_merge.get_next_row(row));
  ASSERT_EQ(row->count_, 5);
  ASSERT_EQ(row->storage_datums_[0].get_int(), 2);
  ASSERT_EQ(row->storage_datums_[1].get_int(), -70);
  ASSERT_EQ(row->storage_datums_[2].get_int(), -4611686018427387905);
  ASSERT_EQ(row->storage_datums_[3].get_int(), 19);
  ASSERT_EQ(row->storage_datums_[4].get_int(), 19);
  ASSERT_EQ(OB_ITER_END, single_merge.get_next_row(row));
  handle1.reset();
  handle2.reset();
  handle3.reset();
  single_merge.reset();
}

TEST_F(TestIncMajorGet, test_crossed_version_minor_get_not_filter)
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
      "2        -40      0           NOP    NOP    DELETE    NORMAL        CLF\n"
      "3        -60      0           NOP    NOP    DELETE    NORMAL        CLF\n";

  snapshot_version = 40;
  scn_range.start_scn_.convert_for_tx(40);
  scn_range.end_scn_.convert_for_tx(60);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data3, 1);
  ObTabletCreateSSTableParam param3;
  prepare_create_basic_sst_param(param3, scn_range, ObITable::MINI_SSTABLE);
  prepare_data_end_with_param(handle3, param3, 60);
  table_store_iter.add_table(handle3.get_table());
  STORAGE_LOG(INFO, "finish prepare mini sstable", K(handle3.get_table()));

  ObDatumRowkey rowkey;
  generate_rowkey(3, rowkey);
  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 0;
  trans_version_range.multi_version_start_ = 0;
  trans_version_range.snapshot_version_ = INT64_MAX;
  prepare_get_param(trans_version_range, table_store_iter);

  mock_tablet_table_store(table_store_iter);
  ObSingleMerge single_merge;
  OK(single_merge.init(access_param_, context_, get_table_param_));
  refresh_iter(single_merge);
  OK(single_merge.open(rowkey));
  single_merge.disable_padding();
  single_merge.disable_fill_virtual_column();
  ObDatumRow *row = nullptr;
  ASSERT_EQ(OB_ITER_END, single_merge.get_next_row(row));
  handle1.reset();
  handle2.reset();
  handle3.reset();
  single_merge.reset();
}

TEST_F(TestIncMajorGet, test_old_version_mini_single_merge)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator table_store_iter;

  ObTableHandleV2 handle1;
  const char *micro_data1[1];
  micro_data1[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -1761312565088476000      0     19       29    INSERT    NORMAL      CLF\n";

  int schema_rowkey_cnt = 1;
  int64_t snapshot_version = 1761312565088476000;
  ObScnRange scn_range;
  scn_range.start_scn_.convert_for_tx(0);
  scn_range.end_scn_.convert_for_tx(1761312565088476000);
  prepare_table_schema(micro_data1, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data1, 1);
  ObTabletCreateSSTableParam param1;
  prepare_create_basic_sst_param(param1, scn_range, ObITable::MAJOR_SSTABLE);
  prepare_create_row_sst_param(param1);
  prepare_data_end_with_param(handle1, param1, 1761312565088476000);
  table_store_iter.add_table(handle1.get_table());
  STORAGE_LOG(INFO, "finish prepare major sstable", K(handle1.get_table()));

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -1761312445524527000      0    19     19    INSERT    NORMAL        CLF\n";

  snapshot_version = 1761312445524527000;
  scn_range.start_scn_.convert_for_tx(1761312331398843001);
  scn_range.end_scn_.convert_for_tx(1761312445524527000);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  ObTabletCreateSSTableParam param2;
  prepare_create_basic_sst_param(param2, scn_range, ObITable::MINI_SSTABLE);
  prepare_data_end_with_param(handle2, param2, 1761312445524527000);
  table_store_iter.add_table(handle2.get_table());
  STORAGE_LOG(INFO, "finish prepare inc major sstable 1", K(handle2.get_table()));

  ObTableHandleV2 handle3;
  const char *micro_data3[1];
  micro_data3[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -1761312445524527001      0       NOP    29    UPDATE    NORMAL        CLF\n";

  snapshot_version = 1761312445524527003;
  scn_range.start_scn_.convert_for_tx(1761312445524527000);
  scn_range.end_scn_.convert_for_tx(1761312445524527003);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data3, 1);
  ObTabletCreateSSTableParam param3;
  prepare_create_basic_sst_param(param3, scn_range, ObITable::MINI_SSTABLE);
  prepare_data_end_with_param(handle3, param3, 1761312445524527003);
  table_store_iter.add_table(handle3.get_table());
  STORAGE_LOG(INFO, "finish prepare mini sstable", K(handle3.get_table()));

  ObDatumRowkey rowkey;
  generate_rowkey(1, rowkey);
  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 0;
  trans_version_range.multi_version_start_ = 0;
  trans_version_range.snapshot_version_ = 1761312445524527002;
  prepare_get_param(trans_version_range, table_store_iter);

  ObSingleMerge single_merge;
  OK(single_merge.init(access_param_, context_, get_table_param_));
  OK(single_merge.open(rowkey));
  single_merge.disable_padding();
  single_merge.disable_fill_virtual_column();
  ObDatumRow *row = nullptr;
  OK(single_merge.get_next_row(row));
  ASSERT_EQ(row->count_, 5);
  ASSERT_EQ(row->storage_datums_[0].get_int(), 1);
  ASSERT_EQ(row->storage_datums_[1].get_int(), -1761312445524527001);
  ASSERT_EQ(row->storage_datums_[2].get_int(), 0);
  ASSERT_EQ(row->storage_datums_[3].get_int(), 19);
  ASSERT_EQ(row->storage_datums_[4].get_int(), 29);
  ASSERT_EQ(OB_ITER_END, single_merge.get_next_row(row));
  handle1.reset();
  handle2.reset();
  handle3.reset();
  single_merge.reset();
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv) {
  system("rm -rf test_inc_major_get.log*");
  OB_LOGGER.set_file_name("test_inc_major_get.log");
  OB_LOGGER.set_log_level("INFO");
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}