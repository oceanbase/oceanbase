/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "test_scan_basic.h"
#include "storage/access/ob_aggregated_store.h"
#include "storage/access/ob_multiple_multi_scan_merge.h"

namespace oceanbase {
namespace storage {

class TestDeleteInsertFastRefreshTable : public TestScanBasic, public ::testing::WithParamInterface<bool>
{
public:
  TestDeleteInsertFastRefreshTable();
  virtual ~TestDeleteInsertFastRefreshTable() {}

  void SetUp();
  void TearDown();
  static void SetUpTestCase();
  static void TearDownTestCase();
};

void TestDeleteInsertFastRefreshTable::SetUpTestCase()
{
  TestScanBasic::SetUpTestCase();
}

void TestDeleteInsertFastRefreshTable::TearDownTestCase()
{
  TestScanBasic::TearDownTestCase();
}

TestDeleteInsertFastRefreshTable::TestDeleteInsertFastRefreshTable()
    : TestScanBasic("test_delete_insert_fast_refresh_table") {}

void TestDeleteInsertFastRefreshTable::SetUp() {
  const bool use_cs_encoding = GetParam();
  row_store_type_ = use_cs_encoding ? CS_ENCODING_ROW_STORE : FLAT_ROW_STORE;
  TestScanBasic::SetUp();
}

void TestDeleteInsertFastRefreshTable::TearDown()
{
  TestScanBasic::TearDown();
}

TEST_P(TestDeleteInsertFastRefreshTable, test_empty_major_single_scan)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator table_store_iter;

  ObTableHandleV2 handle1;
  const char *micro_data1[1];
  micro_data1[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n";

  int schema_rowkey_cnt = 1;
  int64_t snapshot_version = 30;
  ObScnRange scn_range;
  scn_range.start_scn_.convert_for_tx(1);
  scn_range.end_scn_.convert_for_tx(30);
  prepare_table_schema(
      micro_data1, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  ObTabletCreateSSTableParam param1;
  prepare_create_basic_sst_param(param1, scn_range, ObITable::MAJOR_SSTABLE);
  prepare_create_row_sst_param(param1);
  prepare_data_end_with_param(handle1, param1, 30);
  table_store_iter.add_table(handle1.get_table());
  STORAGE_LOG(INFO, "finish prepare empty major sstable", K(handle1.get_table()));

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -50      DI_VERSION     9       9    INSERT    NORMAL      CLF\n"
      "2        -50      DI_VERSION    19      19    INSERT    NORMAL      CLF\n"
      "3        -50      DI_VERSION    29      29    INSERT    NORMAL      CLF\n"
      "4        -50      0             39      39    DELETE    NORMAL      CF\n"
      "4        -40      DI_VERSION    39      39    INSERT    NORMAL      CL\n"
      "5        -50      DI_VERSION    49      49    INSERT    NORMAL      CLF\n"
      "6        -50      DI_VERSION    59      59    INSERT    NORMAL      CLF\n"
      "7        -50      DI_VERSION    69      69    INSERT    NORMAL      CLF\n"
      "8        -50      DI_VERSION    79      79    INSERT    NORMAL      CLF\n"
      "9        -50      DI_VERSION    89      89    INSERT    NORMAL      CLF\n";

  snapshot_version = 50;
  scn_range.start_scn_.convert_for_tx(1);
  scn_range.end_scn_.convert_for_tx(50);
  prepare_data_iter(micro_data2);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  ObTabletCreateSSTableParam param2;
  prepare_create_basic_sst_param(param2, scn_range, ObITable::MINI_SSTABLE);
  prepare_create_row_sst_param(param2);
  prepare_data_end_with_param(handle2, param2, 50);
  table_store_iter.add_table(handle2.get_table());
  STORAGE_LOG(INFO, "finish prepare mini sstable", K(handle2.get_table()));

  const char *result1 = "bigint   bigint bigint  flag     flag_type\n"
                        "2        19      19     INSERT    NORMAL\n"
                        "3        29      29     INSERT    NORMAL\n"
                        "5        49      49     INSERT    NORMAL\n"
                        "6        59      59     INSERT    NORMAL\n";
  const int64_t result1_count = 4;

  ObDatumRange range;
  generate_range(2, 6, range);
  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 1;
  trans_version_range.multi_version_start_ = 50;
  trans_version_range.snapshot_version_ = INT64_MAX;
  prepare_scan_param(trans_version_range, table_store_iter, true/*is_delete_insert*/);

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
  OK(res_iter.from(result1));

  OK(scan_merge.get_next_rows(count, 1));
  if (count > 0) {
    ObMockScanMergeIterator merge_iter(count);
    OK(merge_iter.init(reinterpret_cast<ObVectorStore *>(scan_merge.block_row_store_),
                       query_allocator_,
                       *access_param_.iter_param_.get_read_info()));
    bool is_equal = res_iter.equals<ObMockScanMergeIterator, ObStoreRow>(merge_iter, false, false, false, true);
    ASSERT_TRUE(is_equal);
    total_count += count;
  }
  OK(refresh_table(scan_merge, tablet_read_tables_.tablet_iter_.table_store_iter_));
  STORAGE_LOG(INFO, "refresh tablet iter", K(tablet_read_tables_.tablet_iter_.table_store_iter_));

  while (OB_SUCC(ret)) {
    ret = scan_merge.get_next_rows(count, SQL_BATCH_SIZE);
    if (ret != OB_SUCCESS && ret != OB_ITER_END) {
      STORAGE_LOG(ERROR, "error return value", K(ret), K(count));
      ASSERT_EQ(1, 0);
    }
    if (count > 0) {
      ObMockScanMergeIterator merge_iter(count);
      OK(merge_iter.init(reinterpret_cast<ObVectorStore *>(scan_merge.block_row_store_),
                         query_allocator_,
                         *access_param_.iter_param_.get_read_info()));
      bool is_equal = res_iter.equals<ObMockScanMergeIterator, ObStoreRow>(merge_iter, false, false, false, true);
      ASSERT_TRUE(is_equal);
      total_count += count;
    } else {
      break;
    }
  }
  ASSERT_EQ(result1_count, total_count);

  handle1.reset();
  handle2.reset();
  scan_merge.reset();
}

TEST_P(TestDeleteInsertFastRefreshTable, test_empty_major_multi_scan)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator table_store_iter;

  ObTableHandleV2 handle1;
  const char *micro_data1[1];
  micro_data1[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n";

  int schema_rowkey_cnt = 1;
  int64_t snapshot_version = 30;
  ObScnRange scn_range;
  scn_range.start_scn_.convert_for_tx(1);
  scn_range.end_scn_.convert_for_tx(30);
  prepare_table_schema(
      micro_data1, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  ObTabletCreateSSTableParam param1;
  prepare_create_basic_sst_param(param1, scn_range, ObITable::MAJOR_SSTABLE);
  prepare_create_row_sst_param(param1);
  prepare_data_end_with_param(handle1, param1, 30);
  table_store_iter.add_table(handle1.get_table());
  STORAGE_LOG(INFO, "finish prepare empty major sstable", K(handle1.get_table()));

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -50      DI_VERSION     9       9    INSERT    NORMAL      CLF\n"
      "2        -50      DI_VERSION    19      19    INSERT    NORMAL      CLF\n"
      "3        -50      DI_VERSION    29      29    INSERT    NORMAL      CLF\n"
      "4        -50      0             39      39    DELETE    NORMAL      CF\n"
      "4        -40      DI_VERSION    39      39    INSERT    NORMAL      CL\n"
      "5        -50      DI_VERSION    49      49    INSERT    NORMAL      CLF\n"
      "6        -50      DI_VERSION    59      59    INSERT    NORMAL      CLF\n"
      "7        -50      DI_VERSION    69      69    INSERT    NORMAL      CLF\n"
      "8        -50      DI_VERSION    79      79    INSERT    NORMAL      CLF\n"
      "9        -50      DI_VERSION    89      89    INSERT    NORMAL      CLF\n";

  snapshot_version = 50;
  scn_range.start_scn_.convert_for_tx(1);
  scn_range.end_scn_.convert_for_tx(50);
  prepare_data_iter(micro_data2);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  ObTabletCreateSSTableParam param2;
  prepare_create_basic_sst_param(param2, scn_range, ObITable::MINI_SSTABLE);
  prepare_create_row_sst_param(param2);
  prepare_data_end_with_param(handle2, param2, 50);
  table_store_iter.add_table(handle2.get_table());
  STORAGE_LOG(INFO, "finish prepare mini sstable", K(handle2.get_table()));

  const char *result1 = "bigint   bigint bigint  flag     flag_type\n"
                        "2        19      19     INSERT    NORMAL\n"
                        "3        29      29     INSERT    NORMAL\n"
                        "5        49      49     INSERT    NORMAL\n"
                        "7        69      69     INSERT    NORMAL\n"
                        "8        79      79     INSERT    NORMAL\n";
  const int64_t result1_count = 5;

  ObDatumRange range;
  ObSEArray<ObDatumRange, 8> ranges;
  generate_range(2, 5, range);
  ranges.push_back(range);
  generate_range(7, 8, range);
  ranges.push_back(range);

  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 1;
  trans_version_range.multi_version_start_ = 50;
  trans_version_range.snapshot_version_ = INT64_MAX;
  prepare_scan_param(trans_version_range, table_store_iter, true/*is_delete_insert*/);

  mock_tablet_table_store(table_store_iter);
  ObMultipleMultiScanMerge multi_scan_merge;
  OK(multi_scan_merge.init(access_param_, context_, tablet_read_tables_));
  refresh_iter(multi_scan_merge);
  OK(multi_scan_merge.open(ranges));
  multi_scan_merge.disable_padding();
  multi_scan_merge.disable_fill_virtual_column();

  int64_t count = 0;
  int64_t total_count = 0;
  ret = OB_SUCCESS;
  ObMockIterator res_iter;
  res_iter.reset();
  OK(res_iter.from(result1));

  OK(multi_scan_merge.get_next_rows(count, 1));
  if (count > 0) {
    ObMockScanMergeIterator merge_iter(count);
    OK(merge_iter.init(reinterpret_cast<ObVectorStore *>(multi_scan_merge.block_row_store_),
                       query_allocator_,
                       *access_param_.iter_param_.get_read_info()));
    bool is_equal = res_iter.equals<ObMockScanMergeIterator, ObStoreRow>(merge_iter, false, false, false, true);
    ASSERT_TRUE(is_equal);
    total_count += count;
  }
  OK(refresh_table(multi_scan_merge, tablet_read_tables_.tablet_iter_.table_store_iter_));
  STORAGE_LOG(INFO, "refresh tablet iter", K(tablet_read_tables_.tablet_iter_.table_store_iter_));

  while (OB_SUCC(ret)) {
    ret = multi_scan_merge.get_next_rows(count, SQL_BATCH_SIZE);
    if (ret != OB_SUCCESS && ret != OB_ITER_END) {
      STORAGE_LOG(ERROR, "error return value", K(ret), K(count));
      ASSERT_EQ(1, 0);
    }
    if (count > 0) {
      ObMockScanMergeIterator merge_iter(count);
      OK(merge_iter.init(reinterpret_cast<ObVectorStore *>(multi_scan_merge.block_row_store_),
                         query_allocator_,
                         *access_param_.iter_param_.get_read_info()));
      bool is_equal = res_iter.equals<ObMockScanMergeIterator, ObStoreRow>(merge_iter, false, false, false, true);
      ASSERT_TRUE(is_equal);
      total_count += count;
    } else {
      break;
    }
  }
  ASSERT_EQ(result1_count, total_count);

  handle1.reset();
  handle2.reset();
  multi_scan_merge.reset();
}

TEST_P(TestDeleteInsertFastRefreshTable, test_empty_major_pushdown_aggregate)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator table_store_iter;

  ObTableHandleV2 handle1;
  const char *micro_data1[1];
  micro_data1[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n";

  int schema_rowkey_cnt = 1;
  int64_t snapshot_version = 30;
  ObScnRange scn_range;
  scn_range.start_scn_.convert_for_tx(1);
  scn_range.end_scn_.convert_for_tx(30);
  prepare_table_schema(
      micro_data1, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  ObTabletCreateSSTableParam param1;
  prepare_create_basic_sst_param(param1, scn_range, ObITable::MAJOR_SSTABLE);
  prepare_create_row_sst_param(param1);
  prepare_data_end_with_param(handle1, param1, 30);
  table_store_iter.add_table(handle1.get_table());
  STORAGE_LOG(INFO, "finish prepare empty major sstable", K(handle1.get_table()));

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -50      DI_VERSION     9       9    INSERT    NORMAL      CLF\n"
      "2        -50      DI_VERSION    19      19    INSERT    NORMAL      CLF\n"
      "3        -50      DI_VERSION    29      29    INSERT    NORMAL      CLF\n"
      "4        -50      0             39      39    DELETE    NORMAL      CF\n"
      "4        -40      DI_VERSION    39      39    INSERT    NORMAL      CL\n"
      "5        -50      DI_VERSION    49      49    INSERT    NORMAL      CLF\n"
      "6        -50      DI_VERSION    59      59    INSERT    NORMAL      CLF\n"
      "7        -50      DI_VERSION    69      69    INSERT    NORMAL      CLF\n"
      "8        -50      DI_VERSION    79      79    INSERT    NORMAL      CLF\n"
      "9        -50      DI_VERSION    89      89    INSERT    NORMAL      CLF\n";

  snapshot_version = 50;
  scn_range.start_scn_.convert_for_tx(1);
  scn_range.end_scn_.convert_for_tx(50);
  prepare_data_iter(micro_data2);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  ObTabletCreateSSTableParam param2;
  prepare_create_basic_sst_param(param2, scn_range, ObITable::MINI_SSTABLE);
  prepare_create_row_sst_param(param2);
  prepare_data_end_with_param(handle2, param2, 50);
  table_store_iter.add_table(handle2.get_table());
  STORAGE_LOG(INFO, "finish prepare mini sstable", K(handle2.get_table()));

  const int64_t result1_value = 156;

  ObDatumRange range;
  generate_range(2, 6, range);
  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 1;
  trans_version_range.multi_version_start_ = 50;
  trans_version_range.snapshot_version_ = INT64_MAX;
  prepare_scan_param(trans_version_range, table_store_iter, true/*is_delete_insert*/);

  const int64_t agg_expr_cnt = 1;
  const int64_t sum_col_idx = 1;
  ObFixedArray<int32_t, ObIAllocator> agg_cols_project(query_allocator_);
  agg_cols_project.init(agg_expr_cnt);
  agg_cols_project.push_back(sum_col_idx);

  sql::ExprFixedArray agg_exprs(query_allocator_);
  agg_exprs.init(agg_expr_cnt);
  void *agg_expr_buf = query_allocator_.alloc(sizeof(sql::ObExpr));
  ASSERT_NE(nullptr, agg_expr_buf);
  sql::ObExpr *agg_expr = reinterpret_cast<sql::ObExpr *>(agg_expr_buf);
  agg_expr->reset();
  agg_expr->frame_idx_ = 0;
  agg_expr->datum_off_ = datum_buf_offset_;
  sql::ObDatum *agg_datums = new ((char *)datum_buf_ + datum_buf_offset_) sql::ObDatum[DATUM_ARRAY_CNT];
  datum_buf_offset_ += sizeof(sql::ObDatum) * DATUM_ARRAY_CNT;
  agg_expr->res_buf_off_ = datum_buf_offset_;
  agg_expr->res_buf_len_ = DATUM_RES_SIZE;
  char *agg_ptr = (char *)datum_buf_ + agg_expr->res_buf_off_;
  for (int64_t i = 0; i < DATUM_ARRAY_CNT; i++) {
    agg_datums[i].ptr_ = agg_ptr;
    agg_ptr += agg_expr->res_buf_len_;
  }
  datum_buf_offset_ += agg_expr->res_buf_len_ * DATUM_ARRAY_CNT;
  agg_expr->type_ = T_FUN_SUM;
  agg_expr->basic_funcs_ = ObDatumFuncs::get_basic_func(ObIntType, CS_TYPE_UTF8MB4_GENERAL_CI);
  agg_expr->datum_meta_.type_ = ObNumberType;
  agg_expr->datum_meta_.precision_ = MAX_PRECISION_DECIMAL_INT_128;
  agg_expr->obj_datum_map_ = OBJ_DATUM_8BYTE_DATA;
  agg_expr->batch_result_ = true;
  agg_expr->arg_cnt_ = 1;
  agg_expr->args_ = &output_exprs_.at(sum_col_idx);
  agg_exprs.push_back(agg_expr);

  access_param_.iter_param_.agg_cols_project_ = &agg_cols_project;
  access_param_.aggregate_exprs_ = &agg_exprs;
  access_param_.iter_param_.pd_storage_flag_.set_aggregate_pushdown(true);


  mock_tablet_table_store(table_store_iter);
  ObMultipleScanMerge scan_merge;
  OK(scan_merge.init(access_param_, context_, tablet_read_tables_));
  refresh_iter(scan_merge);
  OK(scan_merge.open(range));
  scan_merge.disable_padding();
  scan_merge.disable_fill_virtual_column();

  int64_t count = 0;
  ret = OB_SUCCESS;

  OK(refresh_table(scan_merge, tablet_read_tables_.tablet_iter_.table_store_iter_));
  STORAGE_LOG(INFO, "refresh tablet iter", K(tablet_read_tables_.tablet_iter_.table_store_iter_));

  OK(scan_merge.get_next_rows(count, 1));
  ObDatum &sum_result = agg_expr->locate_datum_for_write(eval_ctx_);
  sql::ObNumStackAllocator<1> tmp_alloc;
  common::number::ObNumber expect_nmb;
  OK(expect_nmb.from(result1_value, tmp_alloc));
  ASSERT_TRUE(expect_nmb == sum_result.get_number());

  ret = scan_merge.get_next_rows(count, 1);
  ASSERT_EQ(OB_ITER_END, ret);

  handle1.reset();
  handle2.reset();
  scan_merge.reset();
}

TEST_P(TestDeleteInsertFastRefreshTable, test_empty_major_merged_single_scan)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator table_store_iter;

  ObTableHandleV2 handle1;
  const char *micro_data1[1];
  micro_data1[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n";

  int schema_rowkey_cnt = 1;
  int64_t snapshot_version = 30;
  ObScnRange scn_range;
  scn_range.start_scn_.convert_for_tx(1);
  scn_range.end_scn_.convert_for_tx(30);
  prepare_table_schema(
      micro_data1, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  ObTabletCreateSSTableParam param1;
  prepare_create_basic_sst_param(param1, scn_range, ObITable::MAJOR_SSTABLE);
  prepare_create_row_sst_param(param1);
  prepare_data_end_with_param(handle1, param1, 30);
  table_store_iter.add_table(handle1.get_table());
  STORAGE_LOG(INFO, "finish prepare empty major sstable", K(handle1.get_table()));

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -50      DI_VERSION     9       9    INSERT    NORMAL      CLF\n"
      "2        -50      DI_VERSION    19      19    INSERT    NORMAL      CLF\n"
      "3        -50      DI_VERSION    29      29    INSERT    NORMAL      CLF\n"
      "4        -50      0             39      39    DELETE    NORMAL      CF\n"
      "4        -40      DI_VERSION    39      39    INSERT    NORMAL      CL\n"
      "5        -50      DI_VERSION    49      49    INSERT    NORMAL      CLF\n"
      "6        -50      DI_VERSION    59      59    INSERT    NORMAL      CLF\n"
      "7        -50      DI_VERSION    69      69    INSERT    NORMAL      CLF\n"
      "8        -50      DI_VERSION    79      79    INSERT    NORMAL      CLF\n"
      "9        -50      DI_VERSION    89      89    INSERT    NORMAL      CLF\n";

  snapshot_version = 50;
  scn_range.start_scn_.convert_for_tx(1);
  scn_range.end_scn_.convert_for_tx(50);
  prepare_data_iter(micro_data2);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  ObTabletCreateSSTableParam param2;
  prepare_create_basic_sst_param(param2, scn_range, ObITable::MINI_SSTABLE);
  prepare_create_row_sst_param(param2);
  prepare_data_end_with_param(handle2, param2, 50);
  table_store_iter.add_table(handle2.get_table());
  STORAGE_LOG(INFO, "finish prepare mini sstable 1", K(handle2.get_table()));

  ObTableHandleV2 handle4;
  const char *micro_data4[1];
  micro_data4[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "7        -80      DI_VERSION    99     99    INSERT    NORMAL        CF\n"
      "7        -80      0             69     69    DELETE    NORMAL        CL\n";

  snapshot_version = 80;
  scn_range.start_scn_.convert_for_tx(50);
  scn_range.end_scn_.convert_for_tx(80);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data4, 1);
  ObTabletCreateSSTableParam param4;
  prepare_create_basic_sst_param(param4, scn_range, ObITable::MINI_SSTABLE);
  prepare_create_row_sst_param(param4);
  prepare_data_end_with_param(handle4, param4, 80);
  table_store_iter.add_table(handle4.get_table());
  STORAGE_LOG(INFO, "finish prepare mini sstable 2", K(handle4.get_table()));

  ObTableHandleV2 handle3;
  const char *micro_data3[1];
  micro_data3[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -50      DI_VERSION     9       9    INSERT    NORMAL      CLF\n"
      "2        -50      DI_VERSION    19      19    INSERT    NORMAL      CLF\n"
      "3        -50      DI_VERSION    29      29    INSERT    NORMAL      CLF\n"
      "5        -50      DI_VERSION    49      49    INSERT    NORMAL      CLF\n"
      "6        -50      DI_VERSION    59      59    INSERT    NORMAL      CLF\n"
      "7        -50      DI_VERSION    69      69    INSERT    NORMAL      CLF\n"
      "8        -50      DI_VERSION    79      79    INSERT    NORMAL      CLF\n"
      "9        -50      DI_VERSION    89      89    INSERT    NORMAL      CLF\n";

  snapshot_version = 50;
  scn_range.start_scn_.convert_for_tx(1);
  scn_range.end_scn_.convert_for_tx(50);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data3, 1);
  ObTabletCreateSSTableParam param3;
  prepare_create_basic_sst_param(param3, scn_range, ObITable::MAJOR_SSTABLE);
  prepare_create_row_sst_param(param3);
  prepare_data_end_with_param(handle3, param3, 50);
  STORAGE_LOG(INFO, "finish prepare merged major sstable", K(handle3.get_table()));

  const char *result1 = "bigint   bigint bigint  flag     flag_type\n"
                        "2        19      19     INSERT    NORMAL\n"
                        "7        99      99     INSERT    NORMAL\n"
                        "3        29      29     INSERT    NORMAL\n"
                        "5        49      49     INSERT    NORMAL\n"
                        "6        59      59     INSERT    NORMAL\n"
                        "8        79      79     INSERT    NORMAL\n";
  const int64_t result1_count = 6;

  ObDatumRange range;
  generate_range(2, 8, range);
  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 1;
  trans_version_range.multi_version_start_ = 50;
  trans_version_range.snapshot_version_ = INT64_MAX;
  prepare_scan_param(trans_version_range, table_store_iter, true/*is_delete_insert*/);

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
  OK(res_iter.from(result1));

  OK(scan_merge.get_next_rows(count, 1));
  if (count > 0) {
    ObMockScanMergeIterator merge_iter(count);
    OK(merge_iter.init(reinterpret_cast<ObVectorStore *>(scan_merge.block_row_store_),
                       query_allocator_,
                       *access_param_.iter_param_.get_read_info()));
    bool is_equal = res_iter.equals<ObMockScanMergeIterator, ObStoreRow>(merge_iter, false, false, false, true);
    ASSERT_TRUE(is_equal);
    total_count += count;
  }

  // empty major skipped (no di-base) → fast refresh; mini1 compacted into merged major, mini2 remains
  table_store_iter.reset();
  table_store_iter.add_table(handle3.get_table());
  table_store_iter.add_table(handle4.get_table());
  mock_tablet_table_store(table_store_iter, false/*reuse_tablet*/);
  OK(refresh_table(scan_merge, tablet_read_tables_.tablet_iter_.table_store_iter_));
  STORAGE_LOG(INFO, "refresh tablet iter", K(tablet_read_tables_.tablet_iter_.table_store_iter_));

  while (OB_SUCC(ret)) {
    ret = scan_merge.get_next_rows(count, SQL_BATCH_SIZE);
    if (ret != OB_SUCCESS && ret != OB_ITER_END) {
      STORAGE_LOG(ERROR, "error return value", K(ret), K(count));
      ASSERT_EQ(1, 0);
    }
    if (count > 0) {
      ObMockScanMergeIterator merge_iter(count);
      OK(merge_iter.init(reinterpret_cast<ObVectorStore *>(scan_merge.block_row_store_),
                         query_allocator_,
                         *access_param_.iter_param_.get_read_info()));
      bool is_equal = res_iter.equals<ObMockScanMergeIterator, ObStoreRow>(merge_iter, false, false, false, true);
      ASSERT_TRUE(is_equal);
      total_count += count;
    } else {
      break;
    }
  }
  ASSERT_EQ(result1_count, total_count);

  handle1.reset();
  handle2.reset();
  handle3.reset();
  handle4.reset();
  scan_merge.reset();
}

TEST_P(TestDeleteInsertFastRefreshTable, test_empty_major_merged_multi_scan)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator table_store_iter;

  ObTableHandleV2 handle1;
  const char *micro_data1[1];
  micro_data1[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n";

  int schema_rowkey_cnt = 1;
  int64_t snapshot_version = 30;
  ObScnRange scn_range;
  scn_range.start_scn_.convert_for_tx(1);
  scn_range.end_scn_.convert_for_tx(30);
  prepare_table_schema(
      micro_data1, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  ObTabletCreateSSTableParam param1;
  prepare_create_basic_sst_param(param1, scn_range, ObITable::MAJOR_SSTABLE);
  prepare_create_row_sst_param(param1);
  prepare_data_end_with_param(handle1, param1, 30);
  table_store_iter.add_table(handle1.get_table());
  STORAGE_LOG(INFO, "finish prepare empty major sstable", K(handle1.get_table()));

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -50      DI_VERSION     9       9    INSERT    NORMAL      CLF\n"
      "2        -50      DI_VERSION    19      19    INSERT    NORMAL      CLF\n"
      "3        -50      DI_VERSION    29      29    INSERT    NORMAL      CLF\n"
      "4        -50      0             39      39    DELETE    NORMAL      CF\n"
      "4        -40      DI_VERSION    39      39    INSERT    NORMAL      CL\n"
      "5        -50      DI_VERSION    49      49    INSERT    NORMAL      CLF\n"
      "6        -50      DI_VERSION    59      59    INSERT    NORMAL      CLF\n"
      "7        -50      DI_VERSION    69      69    INSERT    NORMAL      CLF\n"
      "8        -50      DI_VERSION    79      79    INSERT    NORMAL      CLF\n"
      "9        -50      DI_VERSION    89      89    INSERT    NORMAL      CLF\n";

  snapshot_version = 50;
  scn_range.start_scn_.convert_for_tx(1);
  scn_range.end_scn_.convert_for_tx(50);
  prepare_data_iter(micro_data2);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  ObTabletCreateSSTableParam param2;
  prepare_create_basic_sst_param(param2, scn_range, ObITable::MINI_SSTABLE);
  prepare_create_row_sst_param(param2);
  prepare_data_end_with_param(handle2, param2, 50);
  table_store_iter.add_table(handle2.get_table());
  STORAGE_LOG(INFO, "finish prepare mini sstable 1", K(handle2.get_table()));

  ObTableHandleV2 handle4;
  const char *micro_data4[1];
  micro_data4[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "7        -80      DI_VERSION    99     99    INSERT    NORMAL        CF\n"
      "7        -80      0             69     69    DELETE    NORMAL        CL\n";

  snapshot_version = 80;
  scn_range.start_scn_.convert_for_tx(50);
  scn_range.end_scn_.convert_for_tx(80);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data4, 1);
  ObTabletCreateSSTableParam param4;
  prepare_create_basic_sst_param(param4, scn_range, ObITable::MINI_SSTABLE);
  prepare_create_row_sst_param(param4);
  prepare_data_end_with_param(handle4, param4, 80);
  table_store_iter.add_table(handle4.get_table());
  STORAGE_LOG(INFO, "finish prepare mini sstable 2", K(handle4.get_table()));

  ObTableHandleV2 handle3;
  const char *micro_data3[1];
  micro_data3[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -50      DI_VERSION     9       9    INSERT    NORMAL      CLF\n"
      "2        -50      DI_VERSION    19      19    INSERT    NORMAL      CLF\n"
      "3        -50      DI_VERSION    29      29    INSERT    NORMAL      CLF\n"
      "5        -50      DI_VERSION    49      49    INSERT    NORMAL      CLF\n"
      "6        -50      DI_VERSION    59      59    INSERT    NORMAL      CLF\n"
      "7        -50      DI_VERSION    69      69    INSERT    NORMAL      CLF\n"
      "8        -50      DI_VERSION    79      79    INSERT    NORMAL      CLF\n"
      "9        -50      DI_VERSION    89      89    INSERT    NORMAL      CLF\n";

  snapshot_version = 50;
  scn_range.start_scn_.convert_for_tx(1);
  scn_range.end_scn_.convert_for_tx(50);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data3, 1);
  ObTabletCreateSSTableParam param3;
  prepare_create_basic_sst_param(param3, scn_range, ObITable::MAJOR_SSTABLE);
  prepare_create_row_sst_param(param3);
  prepare_data_end_with_param(handle3, param3, 50);
  STORAGE_LOG(INFO, "finish prepare merged major sstable", K(handle3.get_table()));

  const char *result1 = "bigint   bigint bigint  flag     flag_type\n"
                        "2        19      19     INSERT    NORMAL\n"
                        "7        99      99     INSERT    NORMAL\n"
                        "3        29      29     INSERT    NORMAL\n"
                        "5        49      49     INSERT    NORMAL\n"
                        "6        59      59     INSERT    NORMAL\n"
                        "8        79      79     INSERT    NORMAL\n";
  const int64_t result1_count = 6;

  ObDatumRange range;
  ObSEArray<ObDatumRange, 8> ranges;
  generate_range(2, 5, range);
  ranges.push_back(range);
  generate_range(6, 8, range);
  ranges.push_back(range);

  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 1;
  trans_version_range.multi_version_start_ = 50;
  trans_version_range.snapshot_version_ = INT64_MAX;
  prepare_scan_param(trans_version_range, table_store_iter, true/*is_delete_insert*/);

  mock_tablet_table_store(table_store_iter);
  ObMultipleMultiScanMerge multi_scan_merge;
  OK(multi_scan_merge.init(access_param_, context_, tablet_read_tables_));
  refresh_iter(multi_scan_merge);
  OK(multi_scan_merge.open(ranges));
  multi_scan_merge.disable_padding();
  multi_scan_merge.disable_fill_virtual_column();

  int64_t count = 0;
  int64_t total_count = 0;
  ret = OB_SUCCESS;
  ObMockIterator res_iter;
  res_iter.reset();
  OK(res_iter.from(result1));

  OK(multi_scan_merge.get_next_rows(count, 1));
  if (count > 0) {
    ObMockScanMergeIterator merge_iter(count);
    OK(merge_iter.init(reinterpret_cast<ObVectorStore *>(multi_scan_merge.block_row_store_),
                       query_allocator_,
                       *access_param_.iter_param_.get_read_info()));
    bool is_equal = res_iter.equals<ObMockScanMergeIterator, ObStoreRow>(merge_iter, false, false, false, true);
    ASSERT_TRUE(is_equal);
    total_count += count;
  }

  // empty major skipped (no di-base) → fast refresh; mini1 compacted into merged major, mini2 remains
  table_store_iter.reset();
  table_store_iter.add_table(handle3.get_table());
  table_store_iter.add_table(handle4.get_table());
  mock_tablet_table_store(table_store_iter, false/*reuse_tablet*/);
  OK(refresh_table(multi_scan_merge, tablet_read_tables_.tablet_iter_.table_store_iter_));
  STORAGE_LOG(INFO, "refresh tablet iter", K(tablet_read_tables_.tablet_iter_.table_store_iter_));

  while (OB_SUCC(ret)) {
    ret = multi_scan_merge.get_next_rows(count, SQL_BATCH_SIZE);
    if (ret != OB_SUCCESS && ret != OB_ITER_END) {
      STORAGE_LOG(ERROR, "error return value", K(ret), K(count));
      ASSERT_EQ(1, 0);
    }
    if (count > 0) {
      ObMockScanMergeIterator merge_iter(count);
      OK(merge_iter.init(reinterpret_cast<ObVectorStore *>(multi_scan_merge.block_row_store_),
                         query_allocator_,
                         *access_param_.iter_param_.get_read_info()));
      bool is_equal = res_iter.equals<ObMockScanMergeIterator, ObStoreRow>(merge_iter, false, false, false, true);
      ASSERT_TRUE(is_equal);
      total_count += count;
    } else {
      break;
    }
  }
  ASSERT_EQ(result1_count, total_count);

  handle1.reset();
  handle2.reset();
  handle3.reset();
  handle4.reset();
  multi_scan_merge.reset();
}

TEST_P(TestDeleteInsertFastRefreshTable, test_empty_major_merged_pushdown_aggregate)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator table_store_iter;

  ObTableHandleV2 handle1;
  const char *micro_data1[1];
  micro_data1[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n";

  int schema_rowkey_cnt = 1;
  int64_t snapshot_version = 30;
  ObScnRange scn_range;
  scn_range.start_scn_.convert_for_tx(1);
  scn_range.end_scn_.convert_for_tx(30);
  prepare_table_schema(
      micro_data1, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  ObTabletCreateSSTableParam param1;
  prepare_create_basic_sst_param(param1, scn_range, ObITable::MAJOR_SSTABLE);
  prepare_create_row_sst_param(param1);
  prepare_data_end_with_param(handle1, param1, 30);
  table_store_iter.add_table(handle1.get_table());
  STORAGE_LOG(INFO, "finish prepare empty major sstable", K(handle1.get_table()));

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -50      DI_VERSION     9       9    INSERT    NORMAL      CLF\n"
      "2        -50      DI_VERSION    19      19    INSERT    NORMAL      CLF\n"
      "3        -50      DI_VERSION    29      29    INSERT    NORMAL      CLF\n"
      "4        -50      0             39      39    DELETE    NORMAL      CF\n"
      "4        -40      DI_VERSION    39      39    INSERT    NORMAL      CL\n"
      "5        -50      DI_VERSION    49      49    INSERT    NORMAL      CLF\n"
      "6        -50      DI_VERSION    59      59    INSERT    NORMAL      CLF\n"
      "7        -50      DI_VERSION    69      69    INSERT    NORMAL      CLF\n"
      "8        -50      DI_VERSION    79      79    INSERT    NORMAL      CLF\n"
      "9        -50      DI_VERSION    89      89    INSERT    NORMAL      CLF\n";

  snapshot_version = 50;
  scn_range.start_scn_.convert_for_tx(1);
  scn_range.end_scn_.convert_for_tx(50);
  prepare_data_iter(micro_data2);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  ObTabletCreateSSTableParam param2;
  prepare_create_basic_sst_param(param2, scn_range, ObITable::MINI_SSTABLE);
  prepare_create_row_sst_param(param2);
  prepare_data_end_with_param(handle2, param2, 50);
  table_store_iter.add_table(handle2.get_table());
  STORAGE_LOG(INFO, "finish prepare mini sstable 1", K(handle2.get_table()));

  ObTableHandleV2 handle4;
  const char *micro_data4[1];
  micro_data4[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "7        -80      DI_VERSION    99     99    INSERT    NORMAL        CF\n"
      "7        -80      0             69     69    DELETE    NORMAL        CL\n";

  snapshot_version = 80;
  scn_range.start_scn_.convert_for_tx(50);
  scn_range.end_scn_.convert_for_tx(80);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data4, 1);
  ObTabletCreateSSTableParam param4;
  prepare_create_basic_sst_param(param4, scn_range, ObITable::MINI_SSTABLE);
  prepare_create_row_sst_param(param4);
  prepare_data_end_with_param(handle4, param4, 80);
  table_store_iter.add_table(handle4.get_table());
  STORAGE_LOG(INFO, "finish prepare mini sstable 2", K(handle4.get_table()));

  ObTableHandleV2 handle3;
  const char *micro_data3[1];
  micro_data3[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -50      DI_VERSION     9       9    INSERT    NORMAL      CLF\n"
      "2        -50      DI_VERSION    19      19    INSERT    NORMAL      CLF\n"
      "3        -50      DI_VERSION    29      29    INSERT    NORMAL      CLF\n"
      "5        -50      DI_VERSION    49      49    INSERT    NORMAL      CLF\n"
      "6        -50      DI_VERSION    59      59    INSERT    NORMAL      CLF\n"
      "7        -50      DI_VERSION    69      69    INSERT    NORMAL      CLF\n"
      "8        -50      DI_VERSION    79      79    INSERT    NORMAL      CLF\n"
      "9        -50      DI_VERSION    89      89    INSERT    NORMAL      CLF\n";

  snapshot_version = 50;
  scn_range.start_scn_.convert_for_tx(1);
  scn_range.end_scn_.convert_for_tx(50);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data3, 1);
  ObTabletCreateSSTableParam param3;
  prepare_create_basic_sst_param(param3, scn_range, ObITable::MAJOR_SSTABLE);
  prepare_create_row_sst_param(param3);
  prepare_data_end_with_param(handle3, param3, 50);
  STORAGE_LOG(INFO, "finish prepare merged major sstable", K(handle3.get_table()));

  const int64_t result1_value = 334;

  ObDatumRange range;
  generate_range(2, 8, range);
  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 1;
  trans_version_range.multi_version_start_ = 50;
  trans_version_range.snapshot_version_ = INT64_MAX;
  prepare_scan_param(trans_version_range, table_store_iter, true/*is_delete_insert*/);

  const int64_t agg_expr_cnt = 1;
  const int64_t sum_col_idx = 1;
  ObFixedArray<int32_t, ObIAllocator> agg_cols_project(query_allocator_);
  agg_cols_project.init(agg_expr_cnt);
  agg_cols_project.push_back(sum_col_idx);

  sql::ExprFixedArray agg_exprs(query_allocator_);
  agg_exprs.init(agg_expr_cnt);
  void *agg_expr_buf = query_allocator_.alloc(sizeof(sql::ObExpr));
  ASSERT_NE(nullptr, agg_expr_buf);
  sql::ObExpr *agg_expr = reinterpret_cast<sql::ObExpr *>(agg_expr_buf);
  agg_expr->reset();
  agg_expr->frame_idx_ = 0;
  agg_expr->datum_off_ = datum_buf_offset_;
  sql::ObDatum *agg_datums = new ((char *)datum_buf_ + datum_buf_offset_) sql::ObDatum[DATUM_ARRAY_CNT];
  datum_buf_offset_ += sizeof(sql::ObDatum) * DATUM_ARRAY_CNT;
  agg_expr->res_buf_off_ = datum_buf_offset_;
  agg_expr->res_buf_len_ = DATUM_RES_SIZE;
  char *agg_ptr = (char *)datum_buf_ + agg_expr->res_buf_off_;
  for (int64_t i = 0; i < DATUM_ARRAY_CNT; i++) {
    agg_datums[i].ptr_ = agg_ptr;
    agg_ptr += agg_expr->res_buf_len_;
  }
  datum_buf_offset_ += agg_expr->res_buf_len_ * DATUM_ARRAY_CNT;
  agg_expr->type_ = T_FUN_SUM;
  agg_expr->basic_funcs_ = ObDatumFuncs::get_basic_func(ObIntType, CS_TYPE_UTF8MB4_GENERAL_CI);
  agg_expr->datum_meta_.type_ = ObNumberType;
  agg_expr->datum_meta_.precision_ = MAX_PRECISION_DECIMAL_INT_128;
  agg_expr->obj_datum_map_ = OBJ_DATUM_8BYTE_DATA;
  agg_expr->batch_result_ = true;
  agg_expr->arg_cnt_ = 1;
  agg_expr->args_ = &output_exprs_.at(sum_col_idx);
  agg_exprs.push_back(agg_expr);

  access_param_.iter_param_.agg_cols_project_ = &agg_cols_project;
  access_param_.aggregate_exprs_ = &agg_exprs;
  access_param_.iter_param_.pd_storage_flag_.set_aggregate_pushdown(true);

  mock_tablet_table_store(table_store_iter);
  ObMultipleScanMerge scan_merge;
  OK(scan_merge.init(access_param_, context_, tablet_read_tables_));
  refresh_iter(scan_merge);
  OK(scan_merge.open(range));
  scan_merge.disable_padding();
  scan_merge.disable_fill_virtual_column();

  int64_t count = 0;
  ret = OB_SUCCESS;

  // empty major skipped (no di-base) → fast refresh; mini1 compacted into merged major, mini2 remains
  table_store_iter.reset();
  table_store_iter.add_table(handle3.get_table());
  table_store_iter.add_table(handle4.get_table());
  mock_tablet_table_store(table_store_iter, false/*reuse_tablet*/);
  OK(refresh_table(scan_merge, tablet_read_tables_.tablet_iter_.table_store_iter_));
  STORAGE_LOG(INFO, "refresh tablet iter", K(tablet_read_tables_.tablet_iter_.table_store_iter_));

  OK(scan_merge.get_next_rows(count, 1));
  ObDatum &sum_result = agg_expr->locate_datum_for_write(eval_ctx_);
  sql::ObNumStackAllocator<1> tmp_alloc;
  common::number::ObNumber expect_nmb;
  OK(expect_nmb.from(result1_value, tmp_alloc));
  ASSERT_TRUE(expect_nmb == sum_result.get_number());

  ret = scan_merge.get_next_rows(count, 1);
  ASSERT_EQ(OB_ITER_END, ret);

  handle1.reset();
  handle2.reset();
  handle3.reset();
  handle4.reset();
  scan_merge.reset();
}

TEST_P(TestDeleteInsertFastRefreshTable, test_single_major_only_single_scan)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator table_store_iter;

  ObTableHandleV2 handle1;
  const char *micro_data1[1];
  micro_data1[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -50      DI_VERSION     9       9    INSERT    NORMAL      CLF\n"
      "2        -50      DI_VERSION    19      19    INSERT    NORMAL      CLF\n"
      "3        -50      DI_VERSION    29      29    INSERT    NORMAL      CLF\n"
      "5        -50      DI_VERSION    49      49    INSERT    NORMAL      CLF\n"
      "6        -50      DI_VERSION    59      59    INSERT    NORMAL      CLF\n"
      "7        -50      DI_VERSION    69      69    INSERT    NORMAL      CLF\n"
      "8        -50      DI_VERSION    79      79    INSERT    NORMAL      CLF\n"
      "9        -50      DI_VERSION    89      89    INSERT    NORMAL      CLF\n";

  int schema_rowkey_cnt = 1;
  int64_t snapshot_version = 50;
  ObScnRange scn_range;
  scn_range.start_scn_.convert_for_tx(1);
  scn_range.end_scn_.convert_for_tx(50);
  prepare_table_schema(
      micro_data1, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data1, 1);
  ObTabletCreateSSTableParam param1;
  prepare_create_basic_sst_param(param1, scn_range, ObITable::MAJOR_SSTABLE);
  prepare_create_row_sst_param(param1);
  prepare_data_end_with_param(handle1, param1, 50);
  table_store_iter.add_table(handle1.get_table());
  STORAGE_LOG(INFO, "finish prepare major sstable", K(handle1.get_table()));

  const char *result1 = "bigint   bigint bigint  flag     flag_type\n"
                        "2        19      19     INSERT    NORMAL\n"
                        "3        29      29     INSERT    NORMAL\n"
                        "5        49      49     INSERT    NORMAL\n"
                        "6        59      59     INSERT    NORMAL\n";
  const int64_t result1_count = 4;

  ObDatumRange range;
  generate_range(2, 6, range);
  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 1;
  trans_version_range.multi_version_start_ = 50;
  trans_version_range.snapshot_version_ = INT64_MAX;
  prepare_scan_param(trans_version_range, table_store_iter, true/*is_delete_insert*/);

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
  OK(res_iter.from(result1));

  OK(scan_merge.get_next_rows(count, 1));
  if (count > 0) {
    ObMockScanMergeIterator merge_iter(count);
    OK(merge_iter.init(reinterpret_cast<ObVectorStore *>(scan_merge.block_row_store_),
                       query_allocator_,
                       *access_param_.iter_param_.get_read_info()));
    bool is_equal = res_iter.equals<ObMockScanMergeIterator, ObStoreRow>(merge_iter, false, false, false, true);
    ASSERT_TRUE(is_equal);
    total_count += count;
  }
  OK(refresh_table(scan_merge, tablet_read_tables_.tablet_iter_.table_store_iter_));
  STORAGE_LOG(INFO, "refresh tablet iter", K(tablet_read_tables_.tablet_iter_.table_store_iter_));

  while (OB_SUCC(ret)) {
    ret = scan_merge.get_next_rows(count, SQL_BATCH_SIZE);
    if (ret != OB_SUCCESS && ret != OB_ITER_END) {
      STORAGE_LOG(ERROR, "error return value", K(ret), K(count));
      ASSERT_EQ(1, 0);
    }
    if (count > 0) {
      ObMockScanMergeIterator merge_iter(count);
      OK(merge_iter.init(reinterpret_cast<ObVectorStore *>(scan_merge.block_row_store_),
                         query_allocator_,
                         *access_param_.iter_param_.get_read_info()));
      bool is_equal = res_iter.equals<ObMockScanMergeIterator, ObStoreRow>(merge_iter, false, false, false, true);
      ASSERT_TRUE(is_equal);
      total_count += count;
    } else {
      break;
    }
  }
  ASSERT_EQ(result1_count, total_count);

  handle1.reset();
  scan_merge.reset();
}

TEST_P(TestDeleteInsertFastRefreshTable, test_single_major_only_multi_scan)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator table_store_iter;

  ObTableHandleV2 handle1;
  const char *micro_data1[1];
  micro_data1[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -50      DI_VERSION     9       9    INSERT    NORMAL      CLF\n"
      "2        -50      DI_VERSION    19      19    INSERT    NORMAL      CLF\n"
      "3        -50      DI_VERSION    29      29    INSERT    NORMAL      CLF\n"
      "5        -50      DI_VERSION    49      49    INSERT    NORMAL      CLF\n"
      "6        -50      DI_VERSION    59      59    INSERT    NORMAL      CLF\n"
      "7        -50      DI_VERSION    69      69    INSERT    NORMAL      CLF\n"
      "8        -50      DI_VERSION    79      79    INSERT    NORMAL      CLF\n"
      "9        -50      DI_VERSION    89      89    INSERT    NORMAL      CLF\n";

  int schema_rowkey_cnt = 1;
  int64_t snapshot_version = 50;
  ObScnRange scn_range;
  scn_range.start_scn_.convert_for_tx(1);
  scn_range.end_scn_.convert_for_tx(50);
  prepare_table_schema(
      micro_data1, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data1, 1);
  ObTabletCreateSSTableParam param1;
  prepare_create_basic_sst_param(param1, scn_range, ObITable::MAJOR_SSTABLE);
  prepare_create_row_sst_param(param1);
  prepare_data_end_with_param(handle1, param1, 50);
  table_store_iter.add_table(handle1.get_table());
  STORAGE_LOG(INFO, "finish prepare major sstable", K(handle1.get_table()));

  const char *result1 = "bigint   bigint bigint  flag     flag_type\n"
                        "2        19      19     INSERT    NORMAL\n"
                        "3        29      29     INSERT    NORMAL\n"
                        "5        49      49     INSERT    NORMAL\n"
                        "7        69      69     INSERT    NORMAL\n"
                        "8        79      79     INSERT    NORMAL\n";
  const int64_t result1_count = 5;

  ObDatumRange range;
  ObSEArray<ObDatumRange, 8> ranges;
  generate_range(2, 5, range);
  ranges.push_back(range);
  generate_range(7, 8, range);
  ranges.push_back(range);

  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 1;
  trans_version_range.multi_version_start_ = 50;
  trans_version_range.snapshot_version_ = INT64_MAX;
  prepare_scan_param(trans_version_range, table_store_iter, true/*is_delete_insert*/);

  mock_tablet_table_store(table_store_iter);
  ObMultipleMultiScanMerge multi_scan_merge;
  OK(multi_scan_merge.init(access_param_, context_, tablet_read_tables_));
  refresh_iter(multi_scan_merge);
  OK(multi_scan_merge.open(ranges));
  multi_scan_merge.disable_padding();
  multi_scan_merge.disable_fill_virtual_column();

  int64_t count = 0;
  int64_t total_count = 0;
  ret = OB_SUCCESS;
  ObMockIterator res_iter;
  res_iter.reset();
  OK(res_iter.from(result1));

  OK(multi_scan_merge.get_next_rows(count, 1));
  if (count > 0) {
    ObMockScanMergeIterator merge_iter(count);
    OK(merge_iter.init(reinterpret_cast<ObVectorStore *>(multi_scan_merge.block_row_store_),
                       query_allocator_,
                       *access_param_.iter_param_.get_read_info()));
    bool is_equal = res_iter.equals<ObMockScanMergeIterator, ObStoreRow>(merge_iter, false, false, false, true);
    ASSERT_TRUE(is_equal);
    total_count += count;
  }
  OK(refresh_table(multi_scan_merge, tablet_read_tables_.tablet_iter_.table_store_iter_));
  STORAGE_LOG(INFO, "refresh tablet iter", K(tablet_read_tables_.tablet_iter_.table_store_iter_));

  while (OB_SUCC(ret)) {
    ret = multi_scan_merge.get_next_rows(count, SQL_BATCH_SIZE);
    if (ret != OB_SUCCESS && ret != OB_ITER_END) {
      STORAGE_LOG(ERROR, "error return value", K(ret), K(count));
      ASSERT_EQ(1, 0);
    }
    if (count > 0) {
      ObMockScanMergeIterator merge_iter(count);
      OK(merge_iter.init(reinterpret_cast<ObVectorStore *>(multi_scan_merge.block_row_store_),
                         query_allocator_,
                         *access_param_.iter_param_.get_read_info()));
      bool is_equal = res_iter.equals<ObMockScanMergeIterator, ObStoreRow>(merge_iter, false, false, false, true);
      ASSERT_TRUE(is_equal);
      total_count += count;
    } else {
      break;
    }
  }
  ASSERT_EQ(result1_count, total_count);

  handle1.reset();
  multi_scan_merge.reset();
}

TEST_P(TestDeleteInsertFastRefreshTable, test_single_major_only_pushdown_aggregate)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator table_store_iter;

  ObTableHandleV2 handle1;
  const char *micro_data1[1];
  micro_data1[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -50      DI_VERSION     9       9    INSERT    NORMAL      CLF\n"
      "2        -50      DI_VERSION    19      19    INSERT    NORMAL      CLF\n"
      "3        -50      DI_VERSION    29      29    INSERT    NORMAL      CLF\n"
      "5        -50      DI_VERSION    49      49    INSERT    NORMAL      CLF\n"
      "6        -50      DI_VERSION    59      59    INSERT    NORMAL      CLF\n"
      "7        -50      DI_VERSION    69      69    INSERT    NORMAL      CLF\n"
      "8        -50      DI_VERSION    79      79    INSERT    NORMAL      CLF\n"
      "9        -50      DI_VERSION    89      89    INSERT    NORMAL      CLF\n";

  int schema_rowkey_cnt = 1;
  int64_t snapshot_version = 50;
  ObScnRange scn_range;
  scn_range.start_scn_.convert_for_tx(1);
  scn_range.end_scn_.convert_for_tx(50);
  prepare_table_schema(
      micro_data1, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data1, 1);
  ObTabletCreateSSTableParam param1;
  prepare_create_basic_sst_param(param1, scn_range, ObITable::MAJOR_SSTABLE);
  prepare_create_row_sst_param(param1);
  prepare_data_end_with_param(handle1, param1, 50);
  table_store_iter.add_table(handle1.get_table());
  STORAGE_LOG(INFO, "finish prepare major sstable", K(handle1.get_table()));

  const int64_t result1_value = 156;

  ObDatumRange range;
  generate_range(2, 6, range);
  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 1;
  trans_version_range.multi_version_start_ = 50;
  trans_version_range.snapshot_version_ = INT64_MAX;
  prepare_scan_param(trans_version_range, table_store_iter, true/*is_delete_insert*/);

  const int64_t agg_expr_cnt = 1;
  const int64_t sum_col_idx = 1;
  ObFixedArray<int32_t, ObIAllocator> agg_cols_project(query_allocator_);
  agg_cols_project.init(agg_expr_cnt);
  agg_cols_project.push_back(sum_col_idx);

  sql::ExprFixedArray agg_exprs(query_allocator_);
  agg_exprs.init(agg_expr_cnt);
  void *agg_expr_buf = query_allocator_.alloc(sizeof(sql::ObExpr));
  ASSERT_NE(nullptr, agg_expr_buf);
  sql::ObExpr *agg_expr = reinterpret_cast<sql::ObExpr *>(agg_expr_buf);
  agg_expr->reset();
  agg_expr->frame_idx_ = 0;
  agg_expr->datum_off_ = datum_buf_offset_;
  sql::ObDatum *agg_datums = new ((char *)datum_buf_ + datum_buf_offset_) sql::ObDatum[DATUM_ARRAY_CNT];
  datum_buf_offset_ += sizeof(sql::ObDatum) * DATUM_ARRAY_CNT;
  agg_expr->res_buf_off_ = datum_buf_offset_;
  agg_expr->res_buf_len_ = DATUM_RES_SIZE;
  char *agg_ptr = (char *)datum_buf_ + agg_expr->res_buf_off_;
  for (int64_t i = 0; i < DATUM_ARRAY_CNT; i++) {
    agg_datums[i].ptr_ = agg_ptr;
    agg_ptr += agg_expr->res_buf_len_;
  }
  datum_buf_offset_ += agg_expr->res_buf_len_ * DATUM_ARRAY_CNT;
  agg_expr->type_ = T_FUN_SUM;
  agg_expr->basic_funcs_ = ObDatumFuncs::get_basic_func(ObIntType, CS_TYPE_UTF8MB4_GENERAL_CI);
  agg_expr->datum_meta_.type_ = ObNumberType;
  agg_expr->datum_meta_.precision_ = MAX_PRECISION_DECIMAL_INT_128;
  agg_expr->obj_datum_map_ = OBJ_DATUM_8BYTE_DATA;
  agg_expr->batch_result_ = true;
  agg_expr->arg_cnt_ = 1;
  agg_expr->args_ = &output_exprs_.at(sum_col_idx);
  agg_exprs.push_back(agg_expr);

  access_param_.iter_param_.agg_cols_project_ = &agg_cols_project;
  access_param_.aggregate_exprs_ = &agg_exprs;
  access_param_.iter_param_.pd_storage_flag_.set_aggregate_pushdown(true);

  mock_tablet_table_store(table_store_iter);
  ObMultipleScanMerge scan_merge;
  OK(scan_merge.init(access_param_, context_, tablet_read_tables_));
  refresh_iter(scan_merge);
  OK(scan_merge.open(range));
  scan_merge.disable_padding();
  scan_merge.disable_fill_virtual_column();

  int64_t count = 0;
  ret = OB_SUCCESS;

  OK(refresh_table(scan_merge, tablet_read_tables_.tablet_iter_.table_store_iter_));
  STORAGE_LOG(INFO, "refresh tablet iter", K(tablet_read_tables_.tablet_iter_.table_store_iter_));

  OK(scan_merge.get_next_rows(count, 1));
  ObDatum &sum_result = agg_expr->locate_datum_for_write(eval_ctx_);
  sql::ObNumStackAllocator<1> tmp_alloc;
  common::number::ObNumber expect_nmb;
  OK(expect_nmb.from(result1_value, tmp_alloc));
  ASSERT_TRUE(expect_nmb == sum_result.get_number());

  ret = scan_merge.get_next_rows(count, 1);
  ASSERT_EQ(OB_ITER_END, ret);

  handle1.reset();
  scan_merge.reset();
}

TEST_P(TestDeleteInsertFastRefreshTable, test_single_major_not_merged_single_scan)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator table_store_iter;

  ObTableHandleV2 handle1;
  const char *micro_data1[1];
  micro_data1[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -50      DI_VERSION     9       9    INSERT    NORMAL      CLF\n"
      "2        -50      DI_VERSION    19      19    INSERT    NORMAL      CLF\n"
      "4        -50      DI_VERSION    39      39    INSERT    NORMAL      CLF\n"
      "5        -50      DI_VERSION    49      49    INSERT    NORMAL      CLF\n"
      "6        -50      DI_VERSION    59      59    INSERT    NORMAL      CLF\n"
      "7        -50      DI_VERSION    69      69    INSERT    NORMAL      CLF\n"
      "8        -50      DI_VERSION    79      79    INSERT    NORMAL      CLF\n"
      "9        -50      DI_VERSION    89      89    INSERT    NORMAL      CLF\n";

  int schema_rowkey_cnt = 1;
  int64_t snapshot_version = 50;
  ObScnRange scn_range;
  scn_range.start_scn_.convert_for_tx(1);
  scn_range.end_scn_.convert_for_tx(50);
  prepare_table_schema(
      micro_data1, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
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
      "3        -80      DI_VERSION    29     29    INSERT    NORMAL        CLF\n"
      "4        -80      DI_VERSION    99     99    INSERT    NORMAL        CF\n"
      "4        -80      0             39     39    DELETE    NORMAL        CL\n"
      "6        -80      DI_VERSION    99     99    INSERT    NORMAL        CF\n"
      "6        -80      0             59     59    DELETE    NORMAL        CL\n";

  snapshot_version = 80;
  scn_range.start_scn_.convert_for_tx(1);
  scn_range.end_scn_.convert_for_tx(80);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  ObTabletCreateSSTableParam param2;
  prepare_create_basic_sst_param(param2, scn_range, ObITable::MINI_SSTABLE);
  prepare_create_row_sst_param(param2);
  prepare_data_end_with_param(handle2, param2, 80);
  table_store_iter.add_table(handle2.get_table());
  STORAGE_LOG(INFO, "finish prepare mini sstable", K(handle2.get_table()));

  const char *result1 = "bigint   bigint bigint  flag     flag_type\n"
                        "3        29      29     INSERT    NORMAL\n"
                        "4        99      99     INSERT    NORMAL\n"
                        "2        19      19     INSERT    NORMAL\n"
                        "6        99      99     INSERT    NORMAL\n"
                        "5        49      49     INSERT    NORMAL\n";
  const int64_t result1_count = 5;

  ObDatumRange range;
  generate_range(2, 6, range);
  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 1;
  trans_version_range.multi_version_start_ = 50;
  trans_version_range.snapshot_version_ = INT64_MAX;
  prepare_scan_param(trans_version_range, table_store_iter, true/*is_delete_insert*/);

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
  OK(res_iter.from(result1));

  OK(scan_merge.get_next_rows(count, 1));
  if (count > 0) {
    ObMockScanMergeIterator merge_iter(count);
    OK(merge_iter.init(reinterpret_cast<ObVectorStore *>(scan_merge.block_row_store_),
                       query_allocator_,
                       *access_param_.iter_param_.get_read_info()));
    bool is_equal = res_iter.equals<ObMockScanMergeIterator, ObStoreRow>(merge_iter, false, false, false, true);
    ASSERT_TRUE(is_equal);
    total_count += count;
  }
  OK(refresh_table(scan_merge, tablet_read_tables_.tablet_iter_.table_store_iter_));
  STORAGE_LOG(INFO, "refresh tablet iter", K(tablet_read_tables_.tablet_iter_.table_store_iter_));

  while (OB_SUCC(ret)) {
    ret = scan_merge.get_next_rows(count, SQL_BATCH_SIZE);
    if (ret != OB_SUCCESS && ret != OB_ITER_END) {
      STORAGE_LOG(ERROR, "error return value", K(ret), K(count));
      ASSERT_EQ(1, 0);
    }
    if (count > 0) {
      ObMockScanMergeIterator merge_iter(count);
      OK(merge_iter.init(reinterpret_cast<ObVectorStore *>(scan_merge.block_row_store_),
                         query_allocator_,
                         *access_param_.iter_param_.get_read_info()));
      bool is_equal = res_iter.equals<ObMockScanMergeIterator, ObStoreRow>(merge_iter, false, false, false, true);
      ASSERT_TRUE(is_equal);
      total_count += count;
    } else {
      break;
    }
  }
  ASSERT_EQ(result1_count, total_count);

  handle1.reset();
  handle2.reset();
  scan_merge.reset();
}

TEST_P(TestDeleteInsertFastRefreshTable, test_single_major_not_merged_multi_scan)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator table_store_iter;

  ObTableHandleV2 handle1;
  const char *micro_data1[1];
  micro_data1[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -50      DI_VERSION     9       9    INSERT    NORMAL      CLF\n"
      "2        -50      DI_VERSION    19      19    INSERT    NORMAL      CLF\n"
      "4        -50      DI_VERSION    39      39    INSERT    NORMAL      CLF\n"
      "5        -50      DI_VERSION    49      49    INSERT    NORMAL      CLF\n"
      "6        -50      DI_VERSION    59      59    INSERT    NORMAL      CLF\n"
      "7        -50      DI_VERSION    69      69    INSERT    NORMAL      CLF\n"
      "8        -50      DI_VERSION    79      79    INSERT    NORMAL      CLF\n"
      "9        -50      DI_VERSION    89      89    INSERT    NORMAL      CLF\n";

  int schema_rowkey_cnt = 1;
  int64_t snapshot_version = 50;
  ObScnRange scn_range;
  scn_range.start_scn_.convert_for_tx(1);
  scn_range.end_scn_.convert_for_tx(50);
  prepare_table_schema(
      micro_data1, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
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
      "3        -80      DI_VERSION    29     29    INSERT    NORMAL        CLF\n"
      "4        -80      DI_VERSION    99     99    INSERT    NORMAL        CF\n"
      "4        -80      0             39     39    DELETE    NORMAL        CL\n"
      "6        -80      DI_VERSION    99     99    INSERT    NORMAL        CF\n"
      "6        -80      0             59     59    DELETE    NORMAL        CL\n";

  snapshot_version = 80;
  scn_range.start_scn_.convert_for_tx(1);
  scn_range.end_scn_.convert_for_tx(80);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  ObTabletCreateSSTableParam param2;
  prepare_create_basic_sst_param(param2, scn_range, ObITable::MINI_SSTABLE);
  prepare_create_row_sst_param(param2);
  prepare_data_end_with_param(handle2, param2, 80);
  table_store_iter.add_table(handle2.get_table());
  STORAGE_LOG(INFO, "finish prepare mini sstable", K(handle2.get_table()));

  const char *result1 = "bigint   bigint bigint  flag     flag_type\n"
                        "3        29      29     INSERT    NORMAL\n"
                        "4        99      99     INSERT    NORMAL\n"
                        "1        9       9      INSERT    NORMAL\n"
                        "2        19      19     INSERT    NORMAL\n"
                        "6        99      99     INSERT    NORMAL\n"
                        "7        69      69     INSERT    NORMAL\n";
  const int64_t result1_count = 6;

  ObDatumRange range;
  ObSEArray<ObDatumRange, 8> ranges;
  generate_range(1, 4, range);
  ranges.push_back(range);
  generate_range(6, 7, range);
  ranges.push_back(range);

  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 1;
  trans_version_range.multi_version_start_ = 50;
  trans_version_range.snapshot_version_ = INT64_MAX;
  prepare_scan_param(trans_version_range, table_store_iter, true/*is_delete_insert*/);

  mock_tablet_table_store(table_store_iter);
  ObMultipleMultiScanMerge multi_scan_merge;
  OK(multi_scan_merge.init(access_param_, context_, tablet_read_tables_));
  refresh_iter(multi_scan_merge);
  OK(multi_scan_merge.open(ranges));
  multi_scan_merge.disable_padding();
  multi_scan_merge.disable_fill_virtual_column();

  int64_t count = 0;
  int64_t total_count = 0;
  ret = OB_SUCCESS;
  ObMockIterator res_iter;
  res_iter.reset();
  OK(res_iter.from(result1));

  OK(multi_scan_merge.get_next_rows(count, 1));
  if (count > 0) {
    ObMockScanMergeIterator merge_iter(count);
    OK(merge_iter.init(reinterpret_cast<ObVectorStore *>(multi_scan_merge.block_row_store_),
                       query_allocator_,
                       *access_param_.iter_param_.get_read_info()));
    bool is_equal = res_iter.equals<ObMockScanMergeIterator, ObStoreRow>(merge_iter, false, false, false, true);
    ASSERT_TRUE(is_equal);
    total_count += count;
  }
  OK(refresh_table(multi_scan_merge, tablet_read_tables_.tablet_iter_.table_store_iter_));
  STORAGE_LOG(INFO, "refresh tablet iter", K(tablet_read_tables_.tablet_iter_.table_store_iter_));

  while (OB_SUCC(ret)) {
    ret = multi_scan_merge.get_next_rows(count, SQL_BATCH_SIZE);
    if (ret != OB_SUCCESS && ret != OB_ITER_END) {
      STORAGE_LOG(ERROR, "error return value", K(ret), K(count));
      ASSERT_EQ(1, 0);
    }
    if (count > 0) {
      ObMockScanMergeIterator merge_iter(count);
      OK(merge_iter.init(reinterpret_cast<ObVectorStore *>(multi_scan_merge.block_row_store_),
                         query_allocator_,
                         *access_param_.iter_param_.get_read_info()));
      bool is_equal = res_iter.equals<ObMockScanMergeIterator, ObStoreRow>(merge_iter, false, false, false, true);
      ASSERT_TRUE(is_equal);
      total_count += count;
    } else {
      break;
    }
  }
  ASSERT_EQ(result1_count, total_count);

  handle1.reset();
  handle2.reset();
  multi_scan_merge.reset();
}

TEST_P(TestDeleteInsertFastRefreshTable, test_single_major_not_merged_pushdown_aggregate)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator table_store_iter;

  ObTableHandleV2 handle1;
  const char *micro_data1[1];
  micro_data1[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -50      DI_VERSION     9       9    INSERT    NORMAL      CLF\n"
      "2        -50      DI_VERSION    19      19    INSERT    NORMAL      CLF\n"
      "4        -50      DI_VERSION    39      39    INSERT    NORMAL      CLF\n"
      "5        -50      DI_VERSION    49      49    INSERT    NORMAL      CLF\n"
      "6        -50      DI_VERSION    59      59    INSERT    NORMAL      CLF\n"
      "7        -50      DI_VERSION    69      69    INSERT    NORMAL      CLF\n"
      "8        -50      DI_VERSION    79      79    INSERT    NORMAL      CLF\n"
      "9        -50      DI_VERSION    89      89    INSERT    NORMAL      CLF\n";

  int schema_rowkey_cnt = 1;
  int64_t snapshot_version = 50;
  ObScnRange scn_range;
  scn_range.start_scn_.convert_for_tx(1);
  scn_range.end_scn_.convert_for_tx(50);
  prepare_table_schema(
      micro_data1, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
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
      "3        -80      DI_VERSION    29     29    INSERT    NORMAL        CLF\n"
      "4        -80      DI_VERSION    99     99    INSERT    NORMAL        CF\n"
      "4        -80      0             39     39    DELETE    NORMAL        CL\n"
      "6        -80      DI_VERSION    99     99    INSERT    NORMAL        CF\n"
      "6        -80      0             59     59    DELETE    NORMAL        CL\n";

  snapshot_version = 80;
  scn_range.start_scn_.convert_for_tx(1);
  scn_range.end_scn_.convert_for_tx(80);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  ObTabletCreateSSTableParam param2;
  prepare_create_basic_sst_param(param2, scn_range, ObITable::MINI_SSTABLE);
  prepare_create_row_sst_param(param2);
  prepare_data_end_with_param(handle2, param2, 80);
  table_store_iter.add_table(handle2.get_table());
  STORAGE_LOG(INFO, "finish prepare mini sstable", K(handle2.get_table()));

  const int64_t result1_value = 304;

  ObDatumRange range;
  generate_range(1, 6, range);
  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 1;
  trans_version_range.multi_version_start_ = 50;
  trans_version_range.snapshot_version_ = INT64_MAX;
  prepare_scan_param(trans_version_range, table_store_iter, true/*is_delete_insert*/);

  const int64_t agg_expr_cnt = 1;
  const int64_t sum_col_idx = 1;
  ObFixedArray<int32_t, ObIAllocator> agg_cols_project(query_allocator_);
  agg_cols_project.init(agg_expr_cnt);
  agg_cols_project.push_back(sum_col_idx);

  sql::ExprFixedArray agg_exprs(query_allocator_);
  agg_exprs.init(agg_expr_cnt);
  void *agg_expr_buf = query_allocator_.alloc(sizeof(sql::ObExpr));
  ASSERT_NE(nullptr, agg_expr_buf);
  sql::ObExpr *agg_expr = reinterpret_cast<sql::ObExpr *>(agg_expr_buf);
  agg_expr->reset();
  agg_expr->frame_idx_ = 0;
  agg_expr->datum_off_ = datum_buf_offset_;
  sql::ObDatum *agg_datums = new ((char *)datum_buf_ + datum_buf_offset_) sql::ObDatum[DATUM_ARRAY_CNT];
  datum_buf_offset_ += sizeof(sql::ObDatum) * DATUM_ARRAY_CNT;
  agg_expr->res_buf_off_ = datum_buf_offset_;
  agg_expr->res_buf_len_ = DATUM_RES_SIZE;
  char *agg_ptr = (char *)datum_buf_ + agg_expr->res_buf_off_;
  for (int64_t i = 0; i < DATUM_ARRAY_CNT; i++) {
    agg_datums[i].ptr_ = agg_ptr;
    agg_ptr += agg_expr->res_buf_len_;
  }
  datum_buf_offset_ += agg_expr->res_buf_len_ * DATUM_ARRAY_CNT;
  agg_expr->type_ = T_FUN_SUM;
  agg_expr->basic_funcs_ = ObDatumFuncs::get_basic_func(ObIntType, CS_TYPE_UTF8MB4_GENERAL_CI);
  agg_expr->datum_meta_.type_ = ObNumberType;
  agg_expr->datum_meta_.precision_ = MAX_PRECISION_DECIMAL_INT_128;
  agg_expr->obj_datum_map_ = OBJ_DATUM_8BYTE_DATA;
  agg_expr->batch_result_ = true;
  agg_expr->arg_cnt_ = 1;
  agg_expr->args_ = &output_exprs_.at(sum_col_idx);
  agg_exprs.push_back(agg_expr);

  access_param_.iter_param_.agg_cols_project_ = &agg_cols_project;
  access_param_.aggregate_exprs_ = &agg_exprs;
  access_param_.iter_param_.pd_storage_flag_.set_aggregate_pushdown(true);

  mock_tablet_table_store(table_store_iter);
  ObMultipleScanMerge scan_merge;
  OK(scan_merge.init(access_param_, context_, tablet_read_tables_));
  refresh_iter(scan_merge);
  OK(scan_merge.open(range));
  scan_merge.disable_padding();
  scan_merge.disable_fill_virtual_column();

  int64_t count = 0;
  ret = OB_SUCCESS;

  OK(refresh_table(scan_merge, tablet_read_tables_.tablet_iter_.table_store_iter_));
  STORAGE_LOG(INFO, "refresh tablet iter", K(tablet_read_tables_.tablet_iter_.table_store_iter_));

  OK(scan_merge.get_next_rows(count, 1));
  ObDatum &sum_result = agg_expr->locate_datum_for_write(eval_ctx_);
  sql::ObNumStackAllocator<1> tmp_alloc;
  common::number::ObNumber expect_nmb;
  OK(expect_nmb.from(result1_value, tmp_alloc));
  ASSERT_TRUE(expect_nmb == sum_result.get_number());

  ret = scan_merge.get_next_rows(count, 1);
  ASSERT_EQ(OB_ITER_END, ret);

  handle1.reset();
  handle2.reset();
  scan_merge.reset();
}

INSTANTIATE_TEST_CASE_P(
  FlatAndCSEncoding,
  TestDeleteInsertFastRefreshTable,
  ::testing::Values(false, true));

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv) {
  system("rm -rf test_delete_insert_fast_refresh_table.log* "
         "test_delete_insert_fast_refresh_table.rs.log* "
         "test_delete_insert_fast_refresh_table.election.log*");
  OB_LOGGER.set_file_name(
      "test_delete_insert_fast_refresh_table.log", true, false,
      "test_delete_insert_fast_refresh_table.rs.log",
      "test_delete_insert_fast_refresh_table.election.log");
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
