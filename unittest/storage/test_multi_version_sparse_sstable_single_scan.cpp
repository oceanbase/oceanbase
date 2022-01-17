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
#include "ob_multi_version_sstable_test.h"

namespace oceanbase {
using namespace common;
using namespace blocksstable;
using namespace storage;
using namespace share::schema;

namespace unittest {

class TestMultiVersionSparseSSTableSingleScan : public ObMultiVersionSSTableTest {
public:
  TestMultiVersionSparseSSTableSingleScan() : ObMultiVersionSSTableTest("testmultiversionsparsesingescan")
  {}
  virtual ~TestMultiVersionSparseSSTableSingleScan()
  {}

  virtual void SetUp()
  {
    ObMultiVersionSSTableTest::SetUp();
    ObPartitionKey pkey(combine_id(TENANT_ID, TABLE_ID), 0, 0);
    ASSERT_EQ(OB_SUCCESS, ObPartitionService::get_instance().get_pg_index().init());
    ASSERT_EQ(OB_SUCCESS, ObPartitionService::get_instance().get_pg_index().add_partition(pkey, pkey));
    ObPartitionKey& part = const_cast<ObPartitionKey&>(ctx_mgr_.get_partition());
    part = pkey;
    // trans_service_.part_trans_ctx_mgr_.mgr_cache_.set(pkey.hash(), &ctx_mgr_);
    trans_service_.part_trans_ctx_mgr_.partition_ctx_map_.insert_and_get(pkey, &ctx_mgr_);
    ObPartitionService::get_instance().txs_ = &trans_service_;
  }
  virtual void TearDown()
  {
    ObMultiVersionSSTableTest::TearDown();
    columns_.reset();
    param_.reset();
    context_.reset();
    allocator_.reuse();
    projector_.reuse();
    store_ctx_.reset();
    ObPartitionService::get_instance().get_pg_index().destroy();
  }

  void prepare_query_param(const ObVersionRange& trans_version_range, const bool is_whole_macro_scan,
      const bool is_minor_merge = false, const bool is_reverse_scan = false);
  void prepare_gap_query_param(const ObVersionRange& trans_version_range, const bool is_reverse_scan = false);

  ObArray<ObColDesc> columns_;
  ObTableIterParam param_;
  ObTableAccessContext context_;
  ObArenaAllocator allocator_;
  ObArray<int32_t> projector_;
  ObStoreCtx store_ctx_;
  transaction::ObTransService trans_service_;
  transaction::ObPartitionTransCtxMgr ctx_mgr_;
};

void TestMultiVersionSparseSSTableSingleScan::prepare_query_param(const ObVersionRange& trans_version_range,
    const bool is_whole_macro_scan, const bool is_minor_merge, const bool is_reverse_scan)
{
  columns_.reset();
  param_.reset();
  context_.reset();
  block_cache_ws_.reset();
  projector_.reset();

  ObQueryFlag query_flag;

  if (is_reverse_scan) {
    query_flag.scan_order_ = ObQueryFlag::Reverse;
  }
  if (is_whole_macro_scan) {
    query_flag.whole_macro_scan_ = is_whole_macro_scan;
    query_flag.multi_version_minor_merge_ = is_minor_merge;
  }

  ObColDesc col_desc;
  int multi_version_col_cnt = ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  const int schema_rowkey_cnt = rowkey_cnt_ - multi_version_col_cnt;  // schema rowkey count
  int trans_version_col =
      ObMultiVersionRowkeyHelpper::get_trans_version_col_store_index(schema_rowkey_cnt, multi_version_col_cnt);
  int sql_no_col =
      ObMultiVersionRowkeyHelpper::get_sql_sequence_col_store_index(schema_rowkey_cnt, multi_version_col_cnt);
  for (int i = 0; i < column_cnt_; i++) {
    if (trans_version_col != i && sql_no_col != i) {
      col_desc.col_id_ = i + OB_APP_MIN_COLUMN_ID;
      col_desc.col_type_ = data_iter_[0].get_column_type()[i];
      OK(columns_.push_back(col_desc));
    }
  }
  for (int i = 0; i < column_cnt_ - multi_version_col_cnt; i++) {
    OK(projector_.push_back(i));
  }

  param_.table_id_ = combine_id(TENANT_ID, TABLE_ID);
  param_.schema_version_ = SCHEMA_VERSION;
  param_.rowkey_cnt_ = schema_rowkey_cnt;  // schema rowkey count
  param_.out_cols_ = &columns_;
  if (!is_whole_macro_scan) {
    param_.projector_ = &projector_;
  }

  OK(block_cache_ws_.init(TENANT_ID));
  context_.query_flag_ = query_flag;
  context_.store_ctx_ = &store_ctx_;
  context_.allocator_ = &allocator_;
  context_.stmt_allocator_ = &allocator_;
  context_.block_cache_ws_ = &block_cache_ws_;
  context_.trans_version_range_ = trans_version_range;
  context_.read_out_type_ = SPARSE_ROW_STORE;
  context_.is_inited_ = true;
}

void TestMultiVersionSparseSSTableSingleScan::prepare_gap_query_param(
    const ObVersionRange& trans_version_range, const bool is_reverse_scan)
{
  int ret = OB_SUCCESS;
  columns_.reset();
  param_.reset();
  context_.reset();
  block_cache_ws_.reset();
  projector_.reset();

  ObQueryFlag query_flag;
  ObArray<ObColDesc> column_ids;
  int64_t rowkey_count = 0;

  if (is_reverse_scan) {
    query_flag.scan_order_ = ObQueryFlag::Reverse;
  }

  ret = table_schema_.get_column_ids(column_ids);
  ASSERT_EQ(OB_SUCCESS, ret);
  rowkey_count = table_schema_.get_rowkey_column_num();
  ObColDesc col_desc;
  int64_t column_id = OB_APP_MIN_COLUMN_ID;
  for (int i = 0; i < column_ids.count() + 2; i++) {
    const ObColumnSchemaV2* column_schema = NULL;
    if (rowkey_count + 1 != i && rowkey_count != i) {
      column_schema = table_schema_.get_column_schema(column_id);
      ASSERT_TRUE(NULL != column_schema);
      col_desc.col_id_ = column_schema->get_column_id();
      col_desc.col_type_ = column_schema->get_meta_type();
      OK(columns_.push_back(col_desc));
      ++column_id;
    }
  }
  for (int i = 0; i < column_ids.count(); i++) {
    OK(projector_.push_back(i));
  }

  param_.out_cols_ = &columns_;
  param_.projector_ = &projector_;
  param_.table_id_ = combine_id(TENANT_ID, TABLE_ID);
  param_.schema_version_ = SCHEMA_VERSION;
  param_.rowkey_cnt_ = rowkey_count;  // schema rowkey count

  OK(block_cache_ws_.init(TENANT_ID));
  context_.query_flag_ = query_flag;
  context_.store_ctx_ = &store_ctx_;
  context_.allocator_ = &allocator_;
  context_.stmt_allocator_ = &allocator_;
  context_.block_cache_ws_ = &block_cache_ws_;
  context_.trans_version_range_ = trans_version_range;
  context_.is_inited_ = true;
}

TEST_F(TestMultiVersionSparseSSTableSingleScan, add_sql_sequence_col_test)
{
  int column_num = 10;
  ObStoreRow row;
  oceanbase::common::ObObj objs[column_num];
  uint16_t col_ids[column_num];
  row.row_val_.cells_ = objs;
  row.column_ids_ = col_ids;
  row.capacity_ = column_num;
  row.is_sparse_row_ = true;
  for (int i = 0; i < column_num - 1; ++i) {
    row.row_val_.cells_[i].set_int(i);
    row.column_ids_[i] = i * 2;
  }
  row.row_val_.count_ = column_num - 1;
  int sql_sequence_col_idx = 5;
  STORAGE_LOG(INFO, "add sql sequence test", K(row));
  //
  for (int i = row.row_val_.count_; i > sql_sequence_col_idx; --i) {
    row.row_val_.cells_[i] = row.row_val_.cells_[i - 1];
    row.column_ids_[i] = row.column_ids_[i - 1];
  }
  row.row_val_.cells_[sql_sequence_col_idx].set_int(0);
  row.column_ids_[sql_sequence_col_idx] = OB_HIDDEN_SQL_SEQUENCE_COLUMN_ID;
  row.row_val_.count_++;  // add one

  STORAGE_LOG(INFO, "add sql sequence test", K(row));
}

TEST_F(TestMultiVersionSparseSSTableSingleScan, whole_scan_normal)
{  // whole scan is used in merge
  const int64_t rowkey_cnt = 4;
  const char* micro_data[3];
  micro_data[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "1        var1  -8      0      18       2       EXIST   CF\n"
                  "1        var1  -2      0      2        NOP     EXIST   L\n"
                  "2        var2  -7      0      4        3       EXIST   CLF\n"
                  "3        var3  -8      0      7        2       EXIST   CF\n"
                  "3        var3  -5      0      7        1       EXIST   N\n"  // 5
                  "3        var3  -3      0      6        NOP     EXIST   N\n"
                  "3        var3  -2      0      5        NOP     EXIST   L\n"
                  "3        var4  -3      0      3        4       DELETE  CLF\n"
                  "4        var4  -9      0      7        NOP     EXIST   CF\n"
                  "4        var4  -7      0      6        5       EXIST   C\n"  // 10
                  "4        var4  -3      0      7        NOP     EXIST   L\n";

  micro_data[1] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "5        var5  -9      0      10       11      EXIST   CF\n"
                  "5        var5  -6      0      6        NOP     EXIST   N\n"
                  "5        var5  -4      0      7        4       EXIST   N\n";

  micro_data[2] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "5        var5  -3      0      5        2       EXIST   N\n"  // 15
                  "5        var5  -2      0      5        1       EXIST   C\n"
                  "5        var5  -1      0      4        3       EXIST   N\n";

  const char* input_column = "int int int int int int\n"
                             "16  17  7  8  20  21\n"
                             "16  17  7  8  20  NOP\n"
                             "16  17  7  8  20  21\n"
                             "16  17  7  8  20  21\n"
                             "16  17  7  8  20  21\n"  // 5
                             "16  17  7  8  20  NOP\n"
                             "16  17  7  8  20  NOP\n"
                             "16  17  7  8  20  21\n"
                             "16  17  7  8  20  NOP\n"
                             "16  17  7  8  20  21\n"  // 10
                             "16  17  7  8  20  NOP\n"
                             "16  17  7  8  20  21\n"
                             "16  17  7  8  20  NOP\n"
                             "16  17  7  8  20  21\n"
                             "16  17  7  8  20  21\n"  // 15
                             "16  17  7  8  20  21\n"
                             "16  17  7  8  20  21\n";

  const int64_t col_cnt[] = {6, 5, 6, 6, 6, 5, 5, 6, 5, 6, 5, 6, 5, 6, 6, 6, 6};
  prepare_data(micro_data, 3, rowkey_cnt, 9, "none", SPARSE_ROW_STORE, 0, input_column, col_cnt);
  // prepare_data(micro_data, 3, rowkey_cnt, 9);

  ObMockIterator res_iter;
  ObStoreRowIterator* scanner = NULL;
  ObExtStoreRange range;

  const char* result1 = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "1        var1  -8       0      18       2       EXIST   CF\n"
                        "1        var1  -2       0      2        NOP     EXIST   CL\n"
                        "2        var2  -7       0      4        3       EXIST   CLF\n"
                        "3        var3  -8       0      7        2       EXIST   CF\n"
                        "3        var3  -5       0      7        1       EXIST   CL\n"  // 5
                        "3        var4  -3       0      3        4       DELETE  CLF\n"
                        "4        var4  -9       0      7        NOP     EXIST   CF\n"
                        "4        var4  -7       0      6        5       EXIST   CL\n"
                        "5        var5  -9       0      10       11      EXIST   CF\n"
                        "5        var5  -6       0      6        4       EXIST   C\n";  // 10

  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 0;
  trans_version_range.snapshot_version_ = INT64_MAX - 10;
  trans_version_range.multi_version_start_ = 7;
  // minor merge
  prepare_query_param(trans_version_range, true, true);

  res_iter.reset();
  range.get_range().set_whole_range();
  OK(range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));
  OK(sstable_.scan(param_, context_, range, scanner));
  uint16_t result_col_id[] = {
      16,
      17,
      7,
      8,
      20,
      21,
      16,
      17,
      7,
      8,
      20,
      16,
      17,
      7,
      8,
      20,
      21,
      16,
      17,
      7,
      8,
      20,
      21,
      16,
      17,
      7,
      8,
      20,
      21,  // 5
      16,
      17,
      7,
      8,
      20,
      21,
      16,
      17,
      7,
      8,
      20,
      16,
      17,
      7,
      8,
      20,
      21,
      16,
      17,
      7,
      8,
      20,
      21,
      16,
      17,
      7,
      8,
      20,
      21  // 10
  };
  int64_t result_col_cnt[] = {6, 5, 6, 6, 6, 6, 5, 6, 6, 6};
  OK(res_iter.from(result1, '\\', result_col_id, result_col_cnt));
  ASSERT_TRUE(res_iter.equals(*scanner, true));  // cmp multi version row flag
  scanner->~ObStoreRowIterator();

  const char* result2 = "bigint   var   bigint   bigint  flag    multi_version_row_flag\n"
                        "2        var2  4        3       EXIST   CL\n"
                        "3        var3  7        1       EXIST   C\n"
                        "3        var4  3        4       DELETE  L\n"
                        "4        var4  6        5       EXIST   CL\n"
                        "5        var5  6        4       EXIST   L\n";

  trans_version_range.snapshot_version_ = 7;
  trans_version_range.base_version_ = 2;
  // major merge
  prepare_query_param(trans_version_range, true, false);
  res_iter.reset();
  range.get_range().set_whole_range();
  OK(range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));
  OK(sstable_.scan(param_, context_, range, scanner));
  OK(res_iter.from(result2));
  ASSERT_TRUE(res_iter.equals(*scanner));
  scanner->~ObStoreRowIterator();

  const char* result3 = "bigint   var   bigint   bigint  flag    multi_version_row_flag\n"
                        "1        var1  2        NOP     EXIST   L\n"
                        "2        var2  4        3       EXIST   CL\n"
                        "3        var3  7        1       EXIST   C\n"
                        "3        var4  3        4       DELETE  L\n"
                        "4        var4  6        5       EXIST   CL\n"
                        "5        var5  6        4       EXIST   L\n";
  trans_version_range.snapshot_version_ = 7;
  trans_version_range.base_version_ = 0;
  // major merge
  prepare_query_param(trans_version_range, true, false);
  res_iter.reset();
  range.get_range().set_whole_range();
  OK(range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));
  OK(sstable_.scan(param_, context_, range, scanner));
  OK(res_iter.from(result3));
  ASSERT_TRUE(res_iter.equals(*scanner));
  scanner->~ObStoreRowIterator();

  const char* result4 = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "1        var1  -8      0      18       2       EXIST   CLF\n"
                        "2        var2  -7      0      4        3       EXIST   CLF\n"
                        "3        var3  -8      0      7        2       EXIST   CLF\n"
                        "3        var4  -3      0      3        4       DELETE  CLF\n"
                        "4        var4  -9      0      7        NOP     EXIST   CLF\n"
                        "5        var5  -9      0      10       11      EXIST   CF\n";
  uint16_t result_col_id2[] = {16,
      17,
      7,
      8,
      20,
      21,
      16,
      17,
      7,
      8,
      20,
      21,
      16,
      17,
      7,
      8,
      20,
      21,
      16,
      17,
      7,
      8,
      20,
      21,
      16,
      17,
      7,
      8,
      20,  // 5
      16,
      17,
      7,
      8,
      20,
      21};
  int64_t result_col_cnt2[] = {6, 6, 6, 6, 5, 6};
  trans_version_range.base_version_ = 0;
  trans_version_range.snapshot_version_ = INT64_MAX - 10;
  trans_version_range.multi_version_start_ = 20;
  // minor merge
  prepare_query_param(trans_version_range, true, true);

  res_iter.reset();
  range.get_range().set_whole_range();
  OK(range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));
  OK(sstable_.scan(param_, context_, range, scanner));
  OK(res_iter.from(result4, '\\', result_col_id2, result_col_cnt2));
  ASSERT_TRUE(res_iter.equals(*scanner, true));  // cmp multi version row flag
  scanner->~ObStoreRowIterator();
}

TEST_F(TestMultiVersionSparseSSTableSingleScan, micro_block_minor_merge_scan_test)
{  // whole scan is used in merge
  const int64_t rowkey_cnt = 4;
  const char* micro_data[3];
  micro_data[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "1        var1  -8       0      8        9       EXIST   CF\n"
                  "1        var1  -2       0      2        NOP     EXIST   L\n"
                  "2        var2  -7       0      4        3       EXIST   CLF\n"
                  "3        var3  -8       0      7        8       EXIST   CF\n"
                  "3        var3  -5       0      1        NOP     EXIST   N\n"  // 5
                  "3        var3  -3       0      6        NOP     EXIST   N\n"
                  "3        var3  -2       0      5        NOP     EXIST   L\n"
                  "3        var4  -3       0      3        4       DELETE  CLF\n"
                  "4        var4  -9       0      7        NOP     EXIST   CF\n"
                  "4        var4  -7       0      6        5       EXIST   C\n"  // 10
                  "4        var4  -3       0      7        NOP     EXIST   L\n";

  micro_data[1] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "5        var5  -9       0      10       11      EXIST   CF\n"
                  "5        var5  -6       0      6        NOP     EXIST   N\n"
                  "5        var5  -4       0      7        4       EXIST   N\n";

  micro_data[2] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "5        var5  -3       0      5        2       EXIST   N\n"  // 15
                  "5        var5  -2       0      5        1       EXIST   C\n"
                  "5        var5  -1       0      4        3       EXIST   N\n";

  const char* input_column = "int int int int int int\n"
                             "16  17  7   8  20  21\n"
                             "16  17  7   8  21  NOP\n"
                             "16  17  7   8  20  21\n"
                             "16  17  7   8  20  21\n"
                             "16  17  7   8  20  NOP\n"  // 5
                             "16  17  7   8  21  NOP\n"
                             "16  17  7   8  20  NOP\n"
                             "16  17  7   8  21  20\n"
                             "16  17  7   8  22  NOP\n"
                             "16  17  7   8  20  21\n"  // 10
                             "16  17  7   8  21  NOP\n"
                             "16  17  7   8  21  20\n"
                             "16  17  7   8  21  NOP\n"
                             "16  17  7   8  20  21\n"
                             "16  17  7   8  20  21\n"  // 15
                             "16  17  7   8  21  21\n"
                             "16  17  7   8  20  21\n";

  const int64_t col_cnt[] = {6, 5, 6, 6, 5, 5, 5, 6, 5, 6, 5, 6, 5, 6, 6, 6, 6};
  prepare_data(micro_data, 3, rowkey_cnt, 9, "none", SPARSE_ROW_STORE, 0, input_column, col_cnt);
  // prepare_data(micro_data, 3, rowkey_cnt, 9);

  ObMockIterator res_iter;
  ObStoreRowIterator* scanner = NULL;
  ObExtStoreRange range;

  const char* result1 = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "1        var1  -8       0       8        9       EXIST   CF\n"
                        "1        var1  -2       0       2        NOP     EXIST   CL\n"
                        "2        var2  -7       0       4        3       EXIST   CLF\n"
                        "3        var3  -8       0       7        8       EXIST   CF\n"
                        "3        var3  -5       0       1        6       EXIST   CL\n"  // 5
                        "3        var4  -3       0       3        4       DELETE  CLF\n"
                        "4        var4  -9       0       7        NOP     EXIST   CF\n"
                        "4        var4  -7       0       6        5       EXIST   CL\n"
                        "5        var5  -9       0       10       11      EXIST   CF\n"
                        "5        var5  -6       0       6        7       EXIST   C\n";  // 10

  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 0;
  trans_version_range.snapshot_version_ = INT64_MAX - 10;
  trans_version_range.multi_version_start_ = 7;
  // minor merge
  prepare_query_param(trans_version_range, true, true);

  res_iter.reset();
  range.get_range().set_whole_range();
  OK(range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));
  OK(sstable_.scan(param_, context_, range, scanner));
  uint16_t result_col_id[] = {16,
      17,
      7,
      8,
      20,
      21,
      16,
      17,
      7,
      8,
      21,
      16,
      17,
      7,
      8,
      20,
      21,
      16,
      17,
      7,
      8,
      20,
      21,
      16,
      17,
      7,
      8,
      20,
      21,  // 5
      16,
      17,
      7,
      8,
      21,
      20,
      16,
      17,
      7,
      8,
      22,
      16,
      17,
      7,
      8,
      20,
      21,
      16,
      17,
      7,
      8,
      21,
      20,
      16,
      17,
      7,
      8,
      21,
      20};
  int64_t result_col_cnt[] = {6, 5, 6, 6, 6, 6, 5, 6, 6, 6};
  OK(res_iter.from(result1, '\\', result_col_id, result_col_cnt));
  ASSERT_TRUE(res_iter.equals(*scanner, true));  // cmp multi version row flag
  scanner->~ObStoreRowIterator();
}

TEST_F(TestMultiVersionSparseSSTableSingleScan, bug_delete)
{
  const int64_t rowkey_cnt = 4;
  const char* micro_data[3];
  micro_data[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "1        var1  -8       0      2        NOP     EXIST   C\n"
                  "1        var1  -2       0      2        NOP     EXIST   L\n"
                  "2        var2  -7       0      4        3       EXIST   CL\n"
                  "3        var3  -8       0      7        2       EXIST   C\n"
                  "3        var3  -5       0      7        1       EXIST   N\n"  // 5
                  "3        var3  -3       0      6        NOP     EXIST   N\n"
                  "3        var3  -2       0      5        NOP     EXIST   L\n"
                  "3        var4  -3       0      3        4       DELETE  L\n"
                  "4        var4  -2       0      6        5       DELETE   C\n"
                  "4        var4  -1       0      7        NOP     EXIST   L\n";  // 10

  micro_data[1] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "5        var4  -9       0      10       11      DELETE  CL\n"
                  "5        var5  -6       0      6        NOP     DELETE   C\n"
                  "5        var5  -4       0      7        4       EXIST   N\n";

  micro_data[2] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "5        var5  -3       0      5        2       EXIST   N\n"
                  "5        var5  -2       0      5        1       EXIST   C\n"  // 15
                  "5        var5  -1       0      4        3       EXIST   N\n";
  const char* input_column = "int int int int int int\n"
                             "16  17  7   8  21  NOP\n"
                             "16  17  7   8  21  NOP\n"
                             "16  17  7   8  20  21\n"
                             "16  17  7   8  20  21\n"
                             "16  17  7   8  20  21\n"  // 5
                             "16  17  7   8  20  NOP\n"
                             "16  17  7   8  20  NOP\n"
                             "16  17  7   8  20  21\n"
                             "16  17  7   8  20  21\n"
                             "16  17  7   8  20  NOP\n"  // 10
                             "16  17  7   8  20  21\n"
                             "16  17  7   8  20  NOP\n"
                             "16  17  7   8  20  21\n"
                             "16  17  7   8  20  21\n"
                             "16  17  7   8  20  21\n"  // 15
                             "16  17  7   8  20  21\n";

  const int64_t col_cnt[] = {5, 5, 6, 6, 6, 5, 5, 6, 6, 5, 5, 6, 5, 6, 6, 6};
  prepare_data(micro_data, 3, rowkey_cnt, 9, "none", SPARSE_ROW_STORE, 0, input_column, col_cnt);

  ObMockIterator res_iter;
  ObStoreRowIterator* scanner = NULL;
  ObExtStoreRange range;

  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 3;
  trans_version_range.snapshot_version_ = 10;
  trans_version_range.multi_version_start_ = 7;

  // major merge
  const char* result1 = "bigint   var      bigint   bigint  flag    multi_version_row_flag\n"
                        "1        var1     NOP      2       EXIST   CF\n"
                        "2        var2     4        3       EXIST   CLF\n"
                        "3        var3     7        2       EXIST   CF\n"
                        "5        var4     10       11      DELETE  CLF\n"
                        "5        var5     6        NOP     DELETE  CLF\n";

  prepare_query_param(trans_version_range, true, false);

  res_iter.reset();
  range.get_range().set_whole_range();
  OK(range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));
  OK(sstable_.scan(param_, context_, range, scanner));
  OK(res_iter.from(result1));
  ASSERT_TRUE(res_iter.equals(*scanner));
  scanner->~ObStoreRowIterator();
}

TEST_F(TestMultiVersionSparseSSTableSingleScan, incremental_merge)
{
  const int64_t rowkey_cnt = 4;
  const char* micro_data[3];
  micro_data[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "1        var1  -8       0      2        8       EXIST   CF\n"
                  "1        var1  -2       0      2        NOP     EXIST   L\n"
                  "2        var2  -7       0      4        3       EXIST   CLF\n"
                  "3        var3  -8       0      7        2       EXIST   CF\n"
                  "3        var3  -5       0      1        NOP     EXIST   N\n"  // 5
                  "3        var3  -3       0      7        NOP     EXIST   L\n"
                  "4        var4  -9       0      17       NOP     EXIST   CF\n"
                  "4        var4  -7       0      6        5       EXIST   C\n"
                  "4        var4  -3       0      7        NOP     EXIST   L\n";

  micro_data[1] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "4        var5  -9       0      4        5       EXIST   CLF\n"  // 10
                  "5        var5  -6       0      6        NOP     EXIST   CF\n"
                  "5        var5  -4       0      7        18      EXIST   N\n";

  micro_data[2] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "5        var5  -3       0      5        2       EXIST   N\n"
                  "5        var5  -2       0      5        1       EXIST   C\n"
                  "5        var5  -1       0      4        3       EXIST   L\n";
  const char* input_column1 = "int int int int int int\n"
                              "16  17  7   8   20  21\n"
                              "16  17  7   8   21  NOP\n"
                              "16  17  7   8   20  21\n"
                              "16  17  7   8   21  23\n"
                              "16  17  7   8   21  NOP\n"  // 5
                              "16  17  7   8   21  NOP\n"
                              "16  17  7   8   20  NOP\n"
                              "16  17  7   8   21  20\n"
                              "16  17  7   8   23  NOP\n"
                              "16  17  7   8   20  21\n"  // 10
                              "16  17  7   8   21  NOP\n"
                              "16  17  7   8   21  20\n";
  const char* input_column2 = "int int int int int int\n"
                              "16  17  7   8   20  21\n"
                              "16  17  7   8   21  23\n"
                              "16  17  7   8   20  21\n";  // 15

  const int64_t col_cnt1[] = {6, 5, 6, 6, 5, 5, 5, 6, 5, 6, 5, 6};
  const int64_t col_cnt2[] = {6, 6, 6};
  prepare_data_start(micro_data, rowkey_cnt, 9, "none", SPARSE_ROW_STORE);
  const int64_t snapshot_version = 9;
  prepare_one_macro(micro_data, 2, snapshot_version, input_column1, col_cnt1);
  prepare_one_macro(&micro_data[2], 1, snapshot_version, input_column2, col_cnt2);
  prepare_data_end();

  const char* result1 = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "1        var1  -8       0       2        8       EXIST   CF\n"
                        "1        var1  -2       0       2        NOP     EXIST   L\n"
                        "2        var2  -7       0       4        3       EXIST   CLF\n"
                        "3        var3  -8       0       7        2       EXIST   CF\n"
                        "3        var3  -5       0       1        NOP     EXIST   N\n"  // 5
                        "3        var3  -3       0       7        NOP     EXIST   L\n"
                        "4        var4  -9       0       17       NOP     EXIST   CF\n"
                        "4        var4  -7       0       6        5       EXIST   C\n"
                        "4        var4  -3       0       7        NOP     EXIST   L\n"
                        "4        var5  -9       0       4        5       EXIST   CLF\n"  // 10
                        "5        var5  -6       0       6        NOP     EXIST   CF\n"
                        "5        var5  -4       0       7        18      EXIST   N\n";
  uint16_t result_col_id[] = {16,
      17,
      7,
      8,
      20,
      21,
      16,
      17,
      7,
      8,
      21,
      16,
      17,
      7,
      8,
      20,
      21,
      16,
      17,
      7,
      8,
      21,
      23,
      16,
      17,
      7,
      8,
      21,  // 5
      16,
      17,
      7,
      8,
      21,
      16,
      17,
      7,
      8,
      20,
      16,
      17,
      7,
      8,
      21,
      20,
      16,
      17,
      7,
      8,
      23,
      16,
      17,
      7,
      8,
      20,
      21,  // 10
      16,
      17,
      7,
      8,
      21,
      16,
      17,
      7,
      8,
      21,
      20};
  int64_t result_col_cnt[] = {6, 5, 6, 6, 5, 5, 5, 6, 5, 6, 5, 6

  };
  const char* rowkeys_str = "bigint   var   bigint  bigint flag\n"
                            "1        var1  -8      0      EXIST\n"
                            "5        var5  -4      0      EXIST\n";

  ObMockIterator res_iter;
  ObStoreRowIterator* scanner = NULL;
  ObExtStoreRange range;
  ObMockIterator rowkeys_iter;
  const ObStoreRow* start = NULL;
  const ObStoreRow* end = NULL;

  rowkey_cnt_ = 6;  // make rowkey = 4
  OK(rowkeys_iter.from(rowkeys_str));
  OK(rowkeys_iter.get_row(0, start));
  OK(rowkeys_iter.get_row(1, end));
  make_range(start, end, range);
  rowkey_cnt_ = 4;
  range.range_.get_start_key().set_min();

  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 0;
  trans_version_range.snapshot_version_ = INT64_MAX - 10;
  trans_version_range.multi_version_start_ = 0;
  // minor merge
  prepare_query_param(trans_version_range, true, true);

  res_iter.reset();
  OK(sstable_.scan(param_, context_, range, scanner));
  OK(res_iter.from(result1, '\\', result_col_id, result_col_cnt));
  ASSERT_TRUE(res_iter.equals(*scanner, true));  // cmp multi version row flag
  scanner->~ObStoreRowIterator();
}

TEST_F(TestMultiVersionSparseSSTableSingleScan, whole_scan_span_macro)
{
  const int64_t rowkey_cnt = 4;
  const char* micro_data[3];
  micro_data[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "1        var1  -8       0      19       2       EXIST   CF\n"
                  "1        var1  -2       0      2        NOP     EXIST   L\n"
                  "2        var2  -7       0      4        3       EXIST   CLF\n"
                  "3        var3  -8       0      7        2       EXIST   CF\n"
                  "3        var3  -5       0      18       1       EXIST   N\n"  // 5
                  "3        var3  -3       0      7        NOP     EXIST   L\n"
                  "4        var4  -9       0      17       NOP     EXIST   F\n"
                  "4        var4  -7       0      6        5       EXIST   C\n"
                  "4        var4  -3       0      27       NOP     EXIST   L\n";

  micro_data[1] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "5        var5  -6       0      6        NOP     EXIST   F\n"  // 10
                  "5        var5  -4       0      37       NOP     EXIST   N\n";

  micro_data[2] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "5        var5  -3       0      5        2       EXIST   N\n"
                  "5        var5  -2       0      5        1       EXIST   N\n"
                  "5        var5  -1       0      4        3       EXIST   L\n";
  const char* input_column1 = "int int int int int int\n"
                              "16  17  7   8   23  21\n"
                              "16  17  7   8   21  NOP\n"
                              "16  17  7   8   20  21\n"
                              "16  17  7   8   21  23\n"
                              "16  17  7   8   21  24\n"  // 5
                              "16  17  7   8   21  NOP\n"
                              "16  17  7   8   20  NOP\n"
                              "16  17  7   8   21  20\n"
                              "16  17  7   8   20  NOP\n"
                              "16  17  7   8   23  NOP\n"  // 10
                              "16  17  7   8   21  NOP\n";
  const char* input_column2 = "int int int int int int\n"
                              "16  17  7   8   23  21\n"
                              "16  17  7   8   21  23\n"
                              "16  17  7   8   20  21\n";  // 15

  const int64_t col_cnt1[] = {6, 5, 6, 6, 6, 5, 5, 6, 5, 5, 5};
  const int64_t col_cnt2[] = {6, 6, 6};
  const int64_t snapshot_version = 9;
  prepare_data_start(micro_data, rowkey_cnt, snapshot_version, "none", SPARSE_ROW_STORE);
  prepare_one_macro(micro_data, 2, snapshot_version, input_column1, col_cnt1);
  prepare_one_macro(&micro_data[2], 1, snapshot_version, input_column2, col_cnt2);
  prepare_data_end();

  ObMockIterator res_iter;
  ObStoreRowIterator* scanner = NULL;
  ObExtStoreRange range;

  const char* result1 = "bigint   var   bigint bigint  bigint   bigint  bigint  flag    multi_version_row_flag\n"
                        "1        var1  -8       0        19       2    NOP   EXIST   CLF\n"
                        "2        var2  -7       0        4        3    NOP   EXIST   CLF\n"
                        "3        var3  -8       0        7        2    NOP   EXIST   CLF\n"
                        "4        var4  -9       0        17       6    NOP   EXIST   CF\n"
                        "4        var4  -7       0        6        5    NOP   EXIST   CL\n"  // 5
                        "5        var5  -6       0        6        37   4     EXIST   CLF\n";

  uint16_t result_col_id[] = {16,
      17,
      7,
      8,
      23,
      21,
      16,
      17,
      7,
      8,
      20,
      21,
      16,
      17,
      7,
      8,
      21,
      23,
      16,
      17,
      7,
      8,
      20,
      21,
      16,
      17,
      7,
      8,
      21,
      20,  // 5
      16,
      17,
      7,
      8,
      23,
      21,
      20};
  int64_t result_col_cnt[] = {6, 6, 6, 6, 6, 7};
  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = INT64_MAX - 10;
  trans_version_range.multi_version_start_ = 8;
  trans_version_range.base_version_ = 4;
  // minor merge
  prepare_query_param(trans_version_range, true, true);

  res_iter.reset();
  range.get_range().set_whole_range();
  OK(range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));
  OK(sstable_.scan(param_, context_, range, scanner));
  OK(res_iter.from(result1, '\\', result_col_id, result_col_cnt));
  ASSERT_TRUE(res_iter.equals(*scanner, true));  // cmp multi version row flag
  scanner->~ObStoreRowIterator();

  const char* result2 = "bigint   var   bigint   bigint  flag    multi_version_row_flag\n"
                        "1        var1  NOP      2       EXIST   C\n"
                        "2        var2  4        3       EXIST   CL\n"
                        "3        var3  NOP      7       EXIST   CL\n"
                        "4        var4  5        6       EXIST   CL\n"
                        "5        var5  NOP      NOP     EXIST   L\n";

  trans_version_range.snapshot_version_ = 8;
  trans_version_range.base_version_ = 4;
  // major merge
  prepare_query_param(trans_version_range, true, false);

  res_iter.reset();
  range.get_range().set_whole_range();
  OK(range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));
  OK(sstable_.scan(param_, context_, range, scanner));
  OK(res_iter.from(result2));
  ASSERT_TRUE(res_iter.equals(*scanner));
  scanner->~ObStoreRowIterator();

  const char* result3 = "bigint   var   bigint   bigint  flag    multi_version_row_flag\n"
                        "1        var1  NOP      2       EXIST   C\n"
                        "2        var2  4        3       EXIST   CL\n"
                        "3        var3  NOP      7       EXIST   CL\n"
                        "4        var4  17       6       EXIST   CL\n"
                        "5        var5  4        37      EXIST   L\n";

  trans_version_range.snapshot_version_ = 9;
  trans_version_range.base_version_ = 0;
  // major merge
  prepare_query_param(trans_version_range, true, false);

  res_iter.reset();
  range.get_range().set_whole_range();
  OK(range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));
  OK(sstable_.scan(param_, context_, range, scanner));
  OK(res_iter.from(result3));
  ASSERT_TRUE(res_iter.equals(*scanner));
  scanner->~ObStoreRowIterator();

  const char* result4 = "bigint   var   bigint bigint  bigint   bigint  bigint flag    multi_version_row_flag\n"
                        "1        var1  -8       0       19       2       NOP  EXIST   CLF\n"
                        "2        var2  -7       0       4        3       NOP  EXIST   CLF\n"
                        "3        var3  -8       0       7        2       NOP  EXIST   CLF\n"
                        "4        var4  -9       0       17       6       NOP  EXIST   CLF\n"
                        "5        var5  -6       0       6        37       4   EXIST   CLF\n";
  uint16_t result_col_id4[] = {
      16,
      17,
      7,
      8,
      23,
      21,
      16,
      17,
      7,
      8,
      20,
      21,
      16,
      17,
      7,
      8,
      21,
      23,
      16,
      17,
      7,
      8,
      20,
      21,
      16,
      17,
      7,
      8,
      23,
      21,
      20,  // 5
  };
  int64_t result_col_cnt4[] = {6, 6, 6, 6, 7};
  trans_version_range.base_version_ = 0;
  trans_version_range.snapshot_version_ = INT64_MAX - 10;
  trans_version_range.multi_version_start_ = 20;
  // minor merge
  prepare_query_param(trans_version_range, true, true);

  res_iter.reset();
  range.get_range().set_whole_range();
  OK(range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));
  OK(sstable_.scan(param_, context_, range, scanner));
  OK(res_iter.from(result4, '\\', result_col_id4, result_col_cnt4));
  ASSERT_TRUE(res_iter.equals(*scanner, true));  // cmp multi version row flag
  scanner->~ObStoreRowIterator();
}

TEST_F(TestMultiVersionSparseSSTableSingleScan, whole_scan_span_macro2)
{
  const int64_t rowkey_cnt = 4;
  const char* micro_data[5];
  micro_data[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "1        var1  -8       0       13       2       EXIST   CF\n"
                  "1        var1  -2       0       2        NOP     EXIST   L\n";

  micro_data[1] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "2        var2  -7       0       4        3       EXIST   CLF\n"
                  "3        var3  -8       0       7        2       EXIST   CF\n";

  micro_data[2] = "bigint   var   bigint bigint   bigint   bigint  flag    multi_version_row_flag\n"
                  "3        var3  -5       0       1        NOP     EXIST   N\n"
                  "3        var3  -3       0       7        NOP     EXIST   L\n"
                  "4        var4  -9       0       7        NOP     EXIST   CF\n"
                  "4        var4  -7       0       6        5       EXIST   C\n";

  micro_data[3] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "4        var4  -3       0       7        NOP     EXIST   L\n"
                  "5        var5  -6       0       6        4       EXIST   CF\n"
                  "5        var5  -4       0       7        4       EXIST   N\n";

  micro_data[4] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "5        var5  -3       0       5        2       EXIST   N\n"
                  "5        var5  -2       0       5        1       EXIST   C\n"
                  "5        var5  -1       0       4        3       EXIST   L\n";
  const char* input_column1 = "int int int int int int\n"  // 0~1
                              "16  17  7   8   22  21\n"
                              "16  17  7   8   21  NOP\n"
                              "16  17  7   8   20  21\n"
                              "16  17  7   8   21  22\n";
  const char* input_column2 = "int int int int int int\n"  // 2~3
                              "16  17  7   8   21  NOP\n"
                              "16  17  7   8   20  NOP\n"
                              "16  17  7   8   21  NOP\n"
                              "16  17  7   8   21  20\n"
                              "16  17  7   8   20  NOP\n"  // 3
                              "16  17  7   8   22  21\n"
                              "16  17  7   8   21  20\n";
  const char* input_column3 = "int int int int int int\n"  // 4
                              "16  17  7   8   22  21\n"
                              "16  17  7   8   21  22\n"
                              "16  17  7   8   20  21\n";
  const int64_t col_cnt1[] = {6, 5, 6, 6};
  const int64_t col_cnt2[] = {5, 5, 5, 6, 5, 6, 6};
  const int64_t col_cnt3[] = {6, 6, 6};
  const int64_t snapshot_version = 9;
  prepare_data_start(micro_data, rowkey_cnt, snapshot_version, "none", SPARSE_ROW_STORE);
  prepare_one_macro(micro_data, 2, snapshot_version, input_column1, col_cnt1);      // micro_data 0~1
  prepare_one_macro(&micro_data[2], 2, snapshot_version, input_column2, col_cnt2);  // micro_data 2~3
  prepare_one_macro(&micro_data[4], 1, snapshot_version, input_column3, col_cnt3);  // micro_data 4
  prepare_data_end();

  ObMockIterator res_iter;
  ObStoreRowIterator* scanner = NULL;
  ObExtStoreRange range;

  const char* result1 = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "1        var1  -8      0      13       2       EXIST   CF\n"
                        "1        var1  -2      0      2        NOP     EXIST   CL\n"
                        "2        var2  -7      0      4        3       EXIST   CLF\n"
                        "3        var3  -8      0      7        2       EXIST   CF\n"
                        "3        var3  -5      0      1        7       EXIST   CL\n"  // 5
                        "4        var4  -9      0      7        NOP     EXIST   CF\n"
                        "4        var4  -7      0      6        5       EXIST   CL\n"
                        "5        var5  -6      0      6        4       EXIST   CLF\n";
  uint16_t result_col_id1[] = {
      16,
      17,
      7,
      8,
      22,
      21,
      16,
      17,
      7,
      8,
      21,
      16,
      17,
      7,
      8,
      20,
      21,
      16,
      17,
      7,
      8,
      21,
      22,
      16,
      17,
      7,
      8,
      21,
      20,  // 5
      16,
      17,
      7,
      8,
      21,
      16,
      17,
      7,
      8,
      21,
      20,
      16,
      17,
      7,
      8,
      22,
      21,
  };
  int64_t result_col_cnt1[] = {6, 5, 6, 6, 6, 5, 6, 6};
  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 0;
  trans_version_range.snapshot_version_ = INT64_MAX - 10;
  trans_version_range.multi_version_start_ = 7;
  // minor merge
  prepare_query_param(trans_version_range, true, true);

  res_iter.reset();
  range.get_range().set_whole_range();
  OK(range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));
  OK(sstable_.scan(param_, context_, range, scanner));
  OK(res_iter.from(result1, '\\', result_col_id1, result_col_cnt1));
  ASSERT_TRUE(res_iter.equals(*scanner, true));  // cmp multi version row flag
  scanner->~ObStoreRowIterator();

  const char* result2 = "bigint   var   bigint   bigint  flag    multi_version_row_flag\n"
                        "1        var1  NOP      2       EXIST   CL\n"
                        "2        var2  4        3       EXIST   CL\n"
                        "3        var3  7        1       EXIST   C\n"
                        "4        var4  5        6       EXIST   CL\n"
                        "5        var5  NOP      4       EXIST   L\n";

  trans_version_range.snapshot_version_ = 7;
  trans_version_range.base_version_ = 0;
  // major merge
  prepare_query_param(trans_version_range, true, false);

  res_iter.reset();
  range.get_range().set_whole_range();
  OK(range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));
  OK(sstable_.scan(param_, context_, range, scanner));
  OK(res_iter.from(result2));
  ASSERT_TRUE(res_iter.equals(*scanner));
  scanner->~ObStoreRowIterator();

  const char* result3 = "bigint   var   bigint   bigint  flag    multi_version_row_flag\n"
                        "1        var1  NOP      2       EXIST   CL\n"
                        "2        var2  4        3       EXIST   CL\n"
                        "3        var3  NOP      7       EXIST   CF\n"
                        "4        var4  NOP      7       EXIST   CL\n"
                        "5        var5  NOP      4       EXIST   L\n";

  trans_version_range.snapshot_version_ = 9;
  trans_version_range.base_version_ = 2;
  // major merge
  prepare_query_param(trans_version_range, true, false);

  res_iter.reset();
  range.get_range().set_whole_range();
  OK(range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));
  OK(sstable_.scan(param_, context_, range, scanner));
  OK(res_iter.from(result3));
  ASSERT_TRUE(res_iter.equals(*scanner));
  scanner->~ObStoreRowIterator();

  const char* result4 = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "1        var1  -8       0       13       2       EXIST   CLF\n"
                        "2        var2  -7       0       4        3       EXIST   CLF\n"
                        "3        var3  -8       0       7        2       EXIST   CLF\n"
                        "4        var4  -9       0       7        NOP     EXIST   CLF\n"
                        "5        var5  -6       0       6        4       EXIST   CLF\n";
  uint16_t result_col_id4[] = {
      16, 17, 7, 8, 22, 21, 16, 17, 7, 8, 20, 21, 16, 17, 7, 8, 21, 22, 16, 17, 7, 8, 21, 16, 17, 7, 8, 22, 21};
  int64_t result_col_cnt4[] = {6, 6, 6, 5, 6};
  trans_version_range.base_version_ = 0;
  trans_version_range.snapshot_version_ = INT64_MAX - 10;
  trans_version_range.multi_version_start_ = 20;
  // minor merge
  prepare_query_param(trans_version_range, true, true);

  res_iter.reset();
  range.get_range().set_whole_range();
  OK(range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));
  OK(sstable_.scan(param_, context_, range, scanner));
  OK(res_iter.from(result4, '\\', result_col_id4, result_col_cnt4));
  ASSERT_TRUE(res_iter.equals(*scanner, true));  // cmp multi version row flag
  scanner->~ObStoreRowIterator();
}

TEST_F(TestMultiVersionSparseSSTableSingleScan, test_get_gap_flag1)
{
  int ret = OB_SUCCESS;
  const ObStoreRow* row = NULL;
  uint8_t flag = 0;
  ObStoreRowIterator* scanner = NULL;
  ObExtStoreRange range;
  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 7;
  trans_version_range.multi_version_start_ = 0;
  trans_version_range.base_version_ = 0;
  range.get_range().set_whole_range();
  OK(range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));
  prepare_gap_schema();
  prepare_gap_sstable_data(ALL_DELETE);
  prepare_gap_query_param(trans_version_range, false);
  OK(gap_sstable_.scan(param_, context_, range, scanner));
  while (OB_SUCC(ret)) {
    if (OB_FAIL(scanner->get_next_row_ext(row, flag))) {
      if (OB_ITER_END == ret) {
        break;
      }
    } else {
      ASSERT_EQ(STORE_ITER_ROW_BIG_GAP_HINT | STORE_ITER_ROW_IN_GAP, flag);
    }
  }
  ASSERT_EQ(OB_ITER_END, ret);
  scanner->~ObStoreRowIterator();
}

TEST_F(TestMultiVersionSparseSSTableSingleScan, test_get_gap_flag)
{
  int ret = OB_SUCCESS;
  const ObStoreRow* row = NULL;
  uint8_t flag = 0;
  ObStoreRowIterator* scanner = NULL;
  ObExtStoreRange range;
  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 7;
  trans_version_range.multi_version_start_ = 0;
  trans_version_range.base_version_ = 0;
  range.get_range().set_whole_range();
  OK(range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));
  prepare_gap_schema();
  prepare_gap_sstable_data(ALL_DELETE);
  prepare_gap_query_param(trans_version_range, false);
  OK(gap_sstable_.scan(param_, context_, range, scanner));
  while (OB_SUCC(ret)) {
    if (OB_FAIL(scanner->get_next_row_ext(row, flag))) {
      if (OB_ITER_END == ret) {
        break;
      }
    } else {
      ASSERT_EQ(STORE_ITER_ROW_BIG_GAP_HINT | STORE_ITER_ROW_IN_GAP, flag);
    }
  }
  ASSERT_EQ(OB_ITER_END, ret);
  scanner->~ObStoreRowIterator();
}

TEST_F(TestMultiVersionSparseSSTableSingleScan, test_get_gap_flag_all_insert)
{
  int ret = OB_SUCCESS;
  const ObStoreRow* row = NULL;
  uint8_t flag = 0;
  ObStoreRowIterator* scanner = NULL;
  ObExtStoreRange range;
  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 7;
  trans_version_range.multi_version_start_ = 0;
  trans_version_range.base_version_ = 0;
  range.get_range().set_whole_range();
  OK(range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));
  prepare_gap_schema();
  prepare_gap_sstable_data(ALL_INSERT);
  prepare_gap_query_param(trans_version_range, false);
  OK(gap_sstable_.scan(param_, context_, range, scanner));
  while (OB_SUCC(ret)) {
    if (OB_FAIL(scanner->get_next_row_ext(row, flag))) {
      if (OB_ITER_END == ret) {
        break;
      }
    } else {
      ASSERT_EQ(0, flag);
    }
  }
  ASSERT_EQ(OB_ITER_END, ret);
  scanner->~ObStoreRowIterator();
}

TEST_F(TestMultiVersionSparseSSTableSingleScan, test_get_gap_end_all_delete)
{
  int ret = OB_SUCCESS;
  const ObStoreRow* row = NULL;
  uint8_t flag = 0;
  ObStoreRowIterator* scanner = NULL;
  ObExtStoreRange range;
  int64_t range_idx = -1;
  const common::ObStoreRowkey* gap_rowkey = NULL;
  int64_t gap_size = -1;
  ObRowGenerate row_generate;
  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 7;
  trans_version_range.multi_version_start_ = 0;
  trans_version_range.base_version_ = 0;
  prepare_gap_schema();
  prepare_gap_sstable_data(ALL_DELETE);
  ObObj check_cells[256];
  ObStoreRowkey check_rowkey;
  ObStoreRow check_row;
  uint16_t col_ids[256];
  check_row.column_ids_ = col_ids;
  check_row.row_val_.cells_ = check_cells;
  check_row.row_val_.count_ = table_schema_.get_column_count();
  ret = row_generate.init(table_schema_, &allocator_, true, true);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = row_generate.get_next_row(999, check_row);
  ASSERT_EQ(OB_SUCCESS, ret);
  check_rowkey.assign(check_cells, table_schema_.get_rowkey_column_num());
  range.get_range().set_whole_range();
  OK(range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));
  prepare_gap_query_param(trans_version_range, false);
  OK(gap_sstable_.scan(param_, context_, range, scanner));
  ASSERT_EQ(OB_SUCCESS, scanner->get_next_row_ext(row, flag));
  ASSERT_EQ(STORE_ITER_ROW_BIG_GAP_HINT | STORE_ITER_ROW_IN_GAP, flag);
  ASSERT_EQ(OB_SUCCESS, scanner->get_gap_end(range_idx, gap_rowkey, gap_size));
  STORAGE_LOG(INFO, "get gap end info", K(range_idx), K(*gap_rowkey), K(gap_size));
  ASSERT_EQ(0, range_idx);
  ASSERT_TRUE(NULL != gap_rowkey);
  ASSERT_EQ(check_rowkey, *gap_rowkey);
  ASSERT_TRUE(gap_size > 0);
  scanner->~ObStoreRowIterator();
}

TEST_F(TestMultiVersionSparseSSTableSingleScan, test_get_gap_end_all_delete_with_insert)
{
  int ret = OB_SUCCESS;
  const ObStoreRow* row = NULL;
  uint8_t flag = 0;
  ObStoreRowIterator* scanner = NULL;
  ObExtStoreRange range;
  int64_t range_idx = -1;
  const common::ObStoreRowkey* gap_rowkey = NULL;
  int64_t gap_size = -1;
  ObRowGenerate row_generate;
  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 7;
  trans_version_range.multi_version_start_ = 0;
  trans_version_range.base_version_ = 0;
  prepare_gap_schema();
  prepare_gap_sstable_data(ALL_DELETE);
  ObObj check_cells[256];
  ObStoreRowkey check_rowkey;
  ObStoreRow check_row;
  check_row.row_val_.cells_ = check_cells;
  check_row.row_val_.count_ = table_schema_.get_column_count();
  uint16_t col_ids[256];
  check_row.column_ids_ = col_ids;
  ret = row_generate.init(table_schema_, &allocator_, true, true);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = row_generate.get_next_row(999, check_row);
  ASSERT_EQ(OB_SUCCESS, ret);
  check_rowkey.assign(check_cells, table_schema_.get_rowkey_column_num());
  range.get_range().set_whole_range();
  OK(range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));
  prepare_gap_query_param(trans_version_range, false);
  OK(gap_sstable_.scan(param_, context_, range, scanner));
  ASSERT_EQ(OB_SUCCESS, scanner->get_next_row_ext(row, flag));
  ASSERT_EQ(STORE_ITER_ROW_BIG_GAP_HINT | STORE_ITER_ROW_IN_GAP, flag);
  ASSERT_EQ(OB_SUCCESS, scanner->get_gap_end(range_idx, gap_rowkey, gap_size));
  STORAGE_LOG(INFO, "get gap end info", K(range_idx), K(*gap_rowkey), K(gap_size));
  ASSERT_EQ(0, range_idx);
  ASSERT_TRUE(NULL != gap_rowkey);
  ASSERT_EQ(check_rowkey, *gap_rowkey);
  ASSERT_TRUE(gap_size > 0);
  scanner->~ObStoreRowIterator();
}

TEST_F(TestMultiVersionSparseSSTableSingleScan, bug16228318)
{
  const int64_t rowkey_cnt = 4;
  const char* micro_data[2];
  micro_data[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "1        var1  -5      0      2         NOP    EXIST   CL\n";
  micro_data[1] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "1        var2  -2      0       NOP        NOP     DELETE  CL\n";

  const char* input_column = "int int int int int int\n"  // 0~1
                             "16  17  7   8   21  NOP\n"
                             "16  17  7   8   NOP  NOP\n";
  const int64_t input_col_cnt[] = {5, 4};
  prepare_data_start(micro_data, rowkey_cnt, 9, "none", SPARSE_ROW_STORE);
  prepare_one_macro(micro_data, 2, 9, input_column, input_col_cnt);  // micro_data 0~1
  prepare_data_end();

  ObMockIterator res_iter;
  ObStoreRowIterator* scanner = NULL;
  ObExtStoreRange range;

  const char* result1 = "bigint   var   bigint   bigint  flag    multi_version_row_flag\n"
                        "1        var2  NOP      NOP     DELETE  N\n";

  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 0;
  trans_version_range.multi_version_start_ = 0;
  trans_version_range.snapshot_version_ = 7;
  prepare_query_param(trans_version_range, false, false, true);

  res_iter.reset();
  range.get_range().set_whole_range();
  OK(range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));
  OK(sstable_.scan(param_, context_, range, scanner));
  OK(res_iter.from(result1));
  const ObStoreRow* row_res = NULL;
  OK(res_iter.get_row(0, row_res));
  const ObStoreRow* row_scan = NULL;
  OK(scanner->get_next_row(row_scan));
  ASSERT_TRUE(ObMockIterator::equals(row_res->row_val_, row_scan->row_val_));
  scanner->~ObStoreRowIterator();
}

/*
TEST_F(TestMultiVersionSparseSSTableSingleScan, whole_scan_span_macro3)
{
  const int64_t rowkey_cnt = 4;
  const char *micro_data[5];
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1  -8       NOP      2       EXIST   CF\n"
      "1        var1  -2       2        NOP     EXIST   L\n";

  micro_data[1] =
      "bigint   var   bigint   bigint   bigint  flag    multi_version_row_flag\n"
      "2        var2  -7       4        3       EXIST   CLF\n"
      "3        var3  -8       7        2       EXIST   CF\n";

  micro_data[2] =
      "bigint   var   bigint   bigint   bigint  flag    multi_version_row_flag\n"
      "3        var3  -5       NOP      1       EXIST   N\n"
      "3        var3  -3       7        NOP     EXIST   L\n"
      "4        var4  -9       7        NOP     EXIST   CF\n"
      "4        var4  -7       6        5       EXIST   C\n";

  micro_data[3] =
      "bigint   var   bigint   bigint   bigint  flag    multi_version_row_flag\n"
      "4        var4  -3       7        NOP     EXIST   L\n"
      "5        var5  -6       6        4       EXIST   CF\n"
      "5        var5  -4       7        4       EXIST   N\n";

  micro_data[4] =
      "bigint   var   bigint   bigint   bigint  flag    multi_version_row_flag\n"
      "5        var5  -3       5        2       EXIST   N\n"
      "5        var5  -2       5        1       EXIST   C\n"
      "5        var5  -1       4        3       EXIST   L\n"
      "6        var6  -1       4        3       EXIST   CLF\n";

  prepare_data_start(micro_data, rowkey_cnt, 9);
  prepare_one_macro(micro_data, 1);
  prepare_one_macro(&micro_data[1], 1);
  prepare_one_macro(&micro_data[2], 1);
  prepare_one_macro(&micro_data[3], 1);
  prepare_one_macro(&micro_data[4], 1);
  prepare_data_end();

  ObMockIterator res_iter;
  ObStoreRowIterator *scanner = NULL;
  ObExtStoreRange range;

  const char *result1 =
      "bigint   var   bigint   bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1  -8       NOP      2       EXIST   CF\n"
      "1        var1  -2       2        NOP     EXIST   CL\n"
      "2        var2  -7       4        3       EXIST   CLF\n"
      "3        var3  -8       7        2       EXIST   CF\n"
      "3        var3  -5       7        1       EXIST   CL\n"
      "4        var4  -9       7        NOP     EXIST   CF\n"
      "4        var4  -7       6        5       EXIST   CL\n"
      "5        var5  -6       6        4       EXIST   CLF\n";

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = INT64_MAX - 10;
  trans_version_range.multi_version_start_ = 7;
  trans_version_range.base_version_ = 1;
  // minor mrege
  prepare_query_param(trans_version_range, true, true);

  res_iter.reset();
  range.get_range().set_whole_range();
  OK(range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));
  OK(sstable_.scan(param_, context_, range, scanner));
  OK(res_iter.from(result1));
  ASSERT_TRUE(res_iter.equals(*scanner, true));//cmp multi version row flag
  scanner->~ObStoreRowIterator();

  const char *result2 =
      "bigint   var   bigint   bigint  flag    multi_version_row_flag\n"
      "2        var2  4        3       EXIST   CL\n"
      "4        var4  6        5       EXIST   CL\n"
      "5        var5  6        4       EXIST   CL\n";

  trans_version_range.snapshot_version_ = 7;
  trans_version_range.base_version_ = 5;
  // major merge
  prepare_query_param(trans_version_range, true, false);

  res_iter.reset();
  range.get_range().set_whole_range();
  OK(range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));
  OK(sstable_.scan(param_, context_, range, scanner));
  OK(res_iter.from(result2));
  ASSERT_TRUE(res_iter.equals(*scanner));
  scanner->~ObStoreRowIterator();

  const char *result3 =
      "bigint   var   bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1  2        NOP     EXIST   CL\n"
      "3        var3  7        1       EXIST   CL\n"
      "4        var4  7        NOP     EXIST   CL\n"
      "5        var5  7        4       EXIST   L\n"
      "6        var6  4        3       EXIST   L\n";

  trans_version_range.snapshot_version_ = 5;
  trans_version_range.base_version_ = 0;
  trans_version_range.multi_version_start_ = 0;
  // major merge
  prepare_query_param(trans_version_range, true, false);

  res_iter.reset();
  range.get_range().set_whole_range();
  OK(range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));
  OK(sstable_.scan(param_, context_, range, scanner));
  OK(res_iter.from(result3));
  ASSERT_TRUE(res_iter.equals(*scanner));
  scanner->~ObStoreRowIterator();

  const char *result4 =
      "bigint   var   bigint   bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1  -8       NOP      2       EXIST   CLF\n"
      "2        var2  -7       4        3       EXIST   CLF\n"
      "3        var3  -8       7        2       EXIST   CLF\n"
      "4        var4  -9       7        NOP     EXIST   CLF\n"
      "5        var5  -6       6        4       EXIST   CLF\n"
      "6        var6  -1       4        3       EXIST   CLF\n";

  trans_version_range.base_version_ = 0;
  trans_version_range.snapshot_version_ = INT64_MAX - 10;
  trans_version_range.multi_version_start_ = 20;
  // minor merge
  prepare_query_param(trans_version_range, true, true);

  res_iter.reset();
  range.get_range().set_whole_range();
  OK(range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));
  OK(sstable_.scan(param_, context_, range, scanner));
  OK(res_iter.from(result4));
  ASSERT_TRUE(res_iter.equals(*scanner, true));//cmp multi version row flag
  scanner->~ObStoreRowIterator();
}

TEST_F(TestMultiVersionSparseSSTableSingleScan, single_scan_empty_sstable)
{
  const int64_t rowkey_cnt = 4;
  const char *micro_data[3];
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1  -8       NOP      2       EXIST   C\n"
      "1        var1  -2       2        NOP     EXIST   L\n"
      "2        var2  -10      4        3       EXIST   CL\n"
      "3        var3  -8       7        2       EXIST   C\n"
      "3        var3  -5       NOP      1       EXIST   N\n"
      "3        var3  -3       6        NOP     EXIST   L\n"
      "4        var4  -9       7        NOP     EXIST   C\n"
      "4        var4  -7       6        5       EXIST   C\n"
      "4        var4  -3       5        NOP     EXIST   L\n";

  micro_data[1] =
      "bigint   var   bigint   bigint   bigint  flag    multi_version_row_flag\n"
      "5        var5  -6       6        NOP     EXIST   N\n"
      "5        var5  -4       7        4       EXIST   N\n";

  micro_data[2] =
      "bigint   var   bigint   bigint   bigint  flag    multi_version_row_flag\n"
      "5        var5  -3       5        2       EXIST   N\n"
      "5        var5  -2       5        1       EXIST   C\n"
      "5        var5  -1       4        3       EXIST   L\n";

  prepare_empty_sstable(micro_data, rowkey_cnt);

  ObMockIterator res_iter;
  ObStoreRowIterator *scanner = NULL;
  ObExtStoreRange range;

  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 0;
  trans_version_range.multi_version_start_ = 0;
  trans_version_range.snapshot_version_ = 7;
  prepare_query_param(trans_version_range, false);

  res_iter.reset();
  range.get_range().set_whole_range();
  OK(range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));
  OK(sstable_.scan(param_, context_, range, scanner));
  const ObStoreRow *row = NULL;
  ASSERT_EQ(OB_ITER_END, scanner->get_next_row(row));
  scanner->~ObStoreRowIterator();

  prepare_query_param(trans_version_range, false, false, true);

  res_iter.reset();
  range.get_range().set_whole_range();
  OK(range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));
  OK(sstable_.scan(param_, context_, range, scanner));
  row = NULL;
  ASSERT_EQ(OB_ITER_END, scanner->get_next_row(row));
  scanner->~ObStoreRowIterator();
}

TEST_F(TestMultiVersionSparseSSTableSingleScan, single_scan_normal)
{
  const int64_t rowkey_cnt = 4;
  const char *micro_data[3];
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1  -8       NOP      2       EXIST   C\n"
      "1        var1  -2       2        NOP     EXIST   L\n"
      "2        var2  -10      4        3       EXIST   CL\n"
      "3        var3  -8       7        2       EXIST   C\n"
      "3        var3  -5       NOP      1       EXIST   N\n"
      "3        var3  -3       6        NOP     EXIST   L\n"
      "3        var4  -3       10       NOP     DELETE  L\n"
      "4        var4  -9       7        NOP     EXIST   C\n"
      "4        var4  -7       6        5       EXIST   C\n"
      "4        var4  -3       5        NOP     EXIST   L\n";

  micro_data[1] =
      "bigint   var   bigint   bigint   bigint  flag    multi_version_row_flag\n"
      "5        var5  -6       6        NOP     EXIST   N\n"
      "5        var5  -4       7        4       EXIST   N\n";

  micro_data[2] =
      "bigint   var   bigint   bigint   bigint  flag    multi_version_row_flag\n"
      "5        var5  -3       5        2       EXIST   N\n"
      "5        var5  -2       5        1       EXIST   C\n"
      "5        var5  -1       4        3       EXIST   L\n";

  prepare_data(micro_data, 3, rowkey_cnt, 10);

  ObMockIterator res_iter;
  ObStoreRowIterator *scanner = NULL;
  ObExtStoreRange range;

  const char *result1 =
      "bigint   var   bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1  2        NOP     EXIST   N\n"
      "3        var3  6        1       EXIST   N\n"
      "3        var4  10       NOP     DELETE  N\n"
      "4        var4  6        5       EXIST   N\n"
      "5        var5  6        4       EXIST   N\n";

  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 0;
  trans_version_range.multi_version_start_ = 0;
  trans_version_range.snapshot_version_ = 7;
  prepare_query_param(trans_version_range, false);

  res_iter.reset();
  range.get_range().set_whole_range();
  OK(range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));
  OK(sstable_.scan(param_, context_, range, scanner));
  OK(res_iter.from(result1));
  ASSERT_TRUE(res_iter.equals(*scanner));
  scanner->~ObStoreRowIterator();

  const char *reverse_result1 =
      "bigint   var   bigint   bigint  flag    multi_version_row_flag\n"
      "5        var5  6        4       EXIST   N\n"
      "4        var4  6        5       EXIST   N\n"
      "3        var4  10       NOP     DELETE  N\n"
      "3        var3  6        1       EXIST   N\n"
      "1        var1  2        NOP     EXIST   N\n";

  prepare_query_param(trans_version_range, false, false, true );//reverse_scan
  res_iter.reset();
  range.get_range().set_whole_range();
  OK(range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));
  OK(sstable_.scan(param_, context_, range, scanner));
  OK(res_iter.from(reverse_result1));
  ASSERT_TRUE(res_iter.equals(*scanner));
  scanner->~ObStoreRowIterator();

  // empty range
  const char *empty_range =
      "bigint   var   flag    multi_version_row_flag\n"
      "4        var5  EXIST   N\n"
      "4        var6  EXIST   N\n";
  ObMockIterator range_iter;
  ObStoreRow *row = NULL;
  const ObStoreRow *res_row = NULL;
  range.reset();

  prepare_query_param(trans_version_range, false);
  OK(range_iter.from(empty_range));
  OK(range_iter.get_row(0, row));
  range.get_range().get_start_key().assign(row->row_val_.cells_, 2);
  OK(range_iter.get_row(1, row));
  range.get_range().get_end_key().assign(row->row_val_.cells_, 2);
  range.get_range().get_border_flag().set_inclusive_start();
  range.get_range().get_border_flag().set_inclusive_end();
  OK(range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));
  OK(sstable_.scan(param_, context_, range, scanner));
  ASSERT_EQ(OB_ITER_END, scanner->get_next_row(res_row));
  scanner->~ObStoreRowIterator();

  prepare_query_param(trans_version_range, false, false, true);
  OK(sstable_.scan(param_, context_, range, scanner));
  ASSERT_EQ(OB_ITER_END, scanner->get_next_row(res_row));
  scanner->~ObStoreRowIterator();

  // hit two micro, the first micro has empty res, and the second micro has res
  const char *span_micro_range =
      "bigint   var   flag    multi_version_row_flag\n"
      "4        var5  EXIST   N\n"
      "5        var5  EXIST   N\n";

  const char *result2 =
      "bigint   var   bigint   bigint  flag    multi_version_row_flag\n"
      "5        var5  6        4       EXIST   N\n";
  ObMockIterator span_micro_range_iter;
  ObMockIterator span_micro_range_res_iter;
  range.reset();

  prepare_query_param(trans_version_range, false);
  OK(span_micro_range_iter.from(span_micro_range));
  OK(span_micro_range_iter.get_row(0, row));
  range.get_range().get_start_key().assign(row->row_val_.cells_, 2);
  OK(span_micro_range_iter.get_row(1, row));
  range.get_range().get_end_key().assign(row->row_val_.cells_, 2);
  range.get_range().get_border_flag().set_inclusive_start();
  range.get_range().get_border_flag().set_inclusive_end();
  OK(range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));
  OK(sstable_.scan(param_, context_, range, scanner));
  OK(span_micro_range_res_iter.from(result2));
  ASSERT_TRUE(span_micro_range_res_iter.equals(*scanner));
  scanner->~ObStoreRowIterator();
}

TEST_F(TestMultiVersionSparseSSTableSingleScan, single_scan_span_macro_simple)
{
  const int64_t rowkey_cnt = 4;
  const char *micro_data[3];
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint  flag    multi_version_row_flag\n"
      "2        var2  -10      2        2       EXIST   CL\n"
      "3        var3  -3       3        3       EXIST   CL\n"
      "4        var4  -9       4        4       EXIST   CL\n";

  micro_data[1] =
      "bigint   var   bigint   bigint   bigint  flag    multi_version_row_flag\n"
      "5        var5  -6       5        5       EXIST   CL\n"
      "6        var6  -4       6        6       EXIST   CL\n"
      "7        var7  -4       7        7       EXIST   CL\n"
      "8        var8  -4       8        8       EXIST   CL\n";


  prepare_data_start(micro_data, rowkey_cnt, 10);
  prepare_one_macro(micro_data, 1);
  prepare_one_macro(&micro_data[1], 1);
  prepare_data_end();

  ObMockIterator res_iter;
  ObStoreRowIterator *scanner = NULL;
  ObExtStoreRange range;

  const char *reverse_result1 =
      "bigint   var   bigint   bigint  flag    multi_version_row_flag\n"
      "8        var8  8        8       EXIST   N\n"
      "7        var7  7        7       EXIST   N\n"
      "6        var6  6        6       EXIST   N\n"
      "5        var5  5        5       EXIST   N\n"
      "4        var4  4        4       EXIST   N\n"
      "3        var3  3        3       EXIST   N\n"
      "2        var2  2        2       EXIST   N\n";

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 20;
  trans_version_range.multi_version_start_ = 0;
  trans_version_range.base_version_ = 0;
  prepare_query_param(trans_version_range, false, false, true);

  res_iter.reset();
  range.get_range().set_whole_range();
  OK(range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));
  OK(sstable_.scan(param_, context_, range, scanner));
  OK(res_iter.from(reverse_result1));
  ASSERT_TRUE(res_iter.equals(*scanner));
  scanner->~ObStoreRowIterator();
}

TEST_F(TestMultiVersionSparseSSTableSingleScan, single_scan_span_macro)
{
  const int64_t rowkey_cnt = 4;
  const char *micro_data[3];
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1  -8       NOP      2       EXIST   C\n"
      "1        var1  -2       2        NOP     EXIST   L\n"
      "2        var2  -10      4        3       EXIST   CL\n"
      "3        var3  -8       7        2       EXIST   C\n"
      "3        var3  -5       NOP      1       EXIST   N\n"
      "3        var3  -3       6        NOP     EXIST   L\n"
      "4        var4  -9       7        NOP     EXIST   C\n"
      "4        var4  -7       6        5       EXIST   C\n"
      "4        var4  -3       5        NOP     EXIST   L\n";

  micro_data[1] =
      "bigint   var   bigint   bigint   bigint  flag    multi_version_row_flag\n"
      "5        var5  -6       6        NOP     EXIST   N\n"
      "5        var5  -4       7        NOP     EXIST   N\n";

  micro_data[2] =
      "bigint   var   bigint   bigint   bigint  flag    multi_version_row_flag\n"
      "5        var5  -3       5        2       EXIST   N\n"
      "5        var5  -2       5        1       EXIST   C\n"
      "5        var5  -1       4        3       EXIST   L\n";

  prepare_data_start(micro_data, rowkey_cnt, 10);
  prepare_one_macro(micro_data, 1);
  prepare_one_macro(&micro_data[1], 1);
  prepare_one_macro(&micro_data[2], 1);
  prepare_data_end();

  ObMockIterator res_iter;
  ObStoreRowIterator *scanner = NULL;
  ObExtStoreRange range;

  const char *result1 =
      "bigint   var   bigint   bigint  flag    multi_version_row_flag\n"
      "3        var3  NOP      1       EXIST   N\n"
      "4        var4  6        5       EXIST   N\n"
      "5        var5  6        NOP     EXIST   N\n";

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 7;
  trans_version_range.multi_version_start_ = 0;
  trans_version_range.base_version_ = 4;
  prepare_query_param(trans_version_range, false);

  res_iter.reset();
  range.get_range().set_whole_range();
  OK(range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));
  OK(sstable_.scan(param_, context_, range, scanner));
  OK(res_iter.from(result1));
  ASSERT_TRUE(res_iter.equals(*scanner));
  scanner->~ObStoreRowIterator();

  const char *reverse_result1 =
      "bigint   var   bigint   bigint  flag    multi_version_row_flag\n"
      "5        var5  6        NOP     EXIST   N\n"
      "4        var4  6        5       EXIST   N\n"
      "3        var3  NOP      1       EXIST   N\n";

  prepare_query_param(trans_version_range, false, false, true );//reverse_scan
  res_iter.reset();
  range.get_range().set_whole_range();
  OK(range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));
  OK(sstable_.scan(param_, context_, range, scanner));
  OK(res_iter.from(reverse_result1));
  ASSERT_TRUE(res_iter.equals(*scanner));
  //int r = OB_SUCCESS;
  //const ObStoreRow *row = NULL;
  //while (r == OB_SUCCESS) {
    //r = scanner->get_next_row(row);
    //STORAGE_LOG(WARN, "test", K(*row));
  //}
  scanner->~ObStoreRowIterator();
}

TEST_F(TestMultiVersionSparseSSTableSingleScan, single_scan_span_macro1)
{
  const int64_t rowkey_cnt = 4;
  const char *micro_data[7];
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1  -8       NOP      2       EXIST   C\n";
  micro_data[1] =
      "bigint   var   bigint   bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1  -2       2        NOP     EXIST   L\n"
      "2        var2  -10      4        3       EXIST   CL\n"
      "3        var3  -8       7        2       EXIST   C\n";
  micro_data[2] =
      "bigint   var   bigint   bigint   bigint  flag    multi_version_row_flag\n"
      "3        var3  -5       NOP      1       EXIST   N\n";
  micro_data[3] =
      "bigint   var   bigint   bigint   bigint  flag    multi_version_row_flag\n"
      "3        var3  -3       6        NOP     EXIST   L\n"
      "4        var4  -9       7        NOP     EXIST   C\n";
  micro_data[4] =
      "bigint   var   bigint   bigint   bigint  flag    multi_version_row_flag\n"
      "4        var4  -7       6        5       EXIST   C\n"
      "4        var4  -3       5        NOP     EXIST   L\n";

  micro_data[5] =
      "bigint   var   bigint   bigint   bigint  flag    multi_version_row_flag\n"
      "5        var5  -6       6        NOP     EXIST   N\n"
      "5        var5  -4       7        4       EXIST   N\n";

  micro_data[6] =
      "bigint   var   bigint   bigint   bigint  flag    multi_version_row_flag\n"
      "5        var5  -3       5        2       EXIST   N\n"
      "5        var5  -2       5        1       EXIST   C\n"
      "5        var5  -1       4        3       EXIST   L\n";

  prepare_data_start(micro_data, rowkey_cnt, 10);
  for (int i = 0; i < 7; i++) {
    prepare_one_macro(&micro_data[i], 1);
  }
  prepare_data_end();

  ObMockIterator res_iter;
  ObStoreRowIterator *scanner = NULL;
  ObExtStoreRange range;

  const char *result1 =
      "bigint   var   bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1  2        NOP     EXIST   N\n"
      "3        var3  6        1       EXIST   N\n"
      "4        var4  6        5       EXIST   N\n"
      "5        var5  6        4       EXIST   N\n";

  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 0;
  trans_version_range.multi_version_start_ = 0;
  trans_version_range.snapshot_version_ = 7;
  prepare_query_param(trans_version_range, false);

  res_iter.reset();
  range.get_range().set_whole_range();
  OK(range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));
  OK(sstable_.scan(param_, context_, range, scanner));
  OK(res_iter.from(result1));
  ASSERT_TRUE(res_iter.equals(*scanner));
  scanner->~ObStoreRowIterator();

  const char *reverse_result1 =
      "bigint   var   bigint   bigint  flag    multi_version_row_flag\n"
      "5        var5  6        4       EXIST   N\n"
      "4        var4  6        5       EXIST   N\n"
      "3        var3  6        1       EXIST   N\n"
      "1        var1  2        NOP     EXIST   N\n";

  prepare_query_param(trans_version_range, false, false, true );//reverse_scan
  res_iter.reset();
  range.get_range().set_whole_range();
  OK(range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));
  OK(sstable_.scan(param_, context_, range, scanner));
  OK(res_iter.from(reverse_result1));
  ASSERT_TRUE(res_iter.equals(*scanner));
  scanner->~ObStoreRowIterator();

  ObModItem item;
  get_tenant_mod_memory(500, ObModIds::OB_COMMON_ARRAY, item);
  STORAGE_LOG(INFO, "memory usage",
      K(item.hold_),
      K(item.used_),
      K(item.count_),
      K(item.alloc_count_),
      K(item.free_count_));
}

TEST_F(TestMultiVersionSparseSSTableSingleScan, bug16197197)
{
  const int64_t rowkey_cnt = 4;
  const char *micro_data[2];
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1  -5       NOP      NOP       DELETE  C\n";
  micro_data[1] =
      "bigint   var   bigint   bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1  -2       2        NOP     EXIST   L\n";

  prepare_data(micro_data, 2, rowkey_cnt, 10);

  ObMockIterator res_iter;
  ObStoreRowIterator *scanner = NULL;
  ObExtStoreRange range;

  const char *result1 =
      "bigint   var   bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1  NOP      NOP     DELETE  N\n";

  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 0;
  trans_version_range.multi_version_start_ = 0;
  trans_version_range.snapshot_version_ = 7;
  prepare_query_param(trans_version_range, false, false, true);

  res_iter.reset();
  range.get_range().set_whole_range();
  OK(range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));
  OK(sstable_.scan(param_, context_, range, scanner));
  OK(res_iter.from(result1));
  const ObStoreRow *row_res = NULL;
  OK(res_iter.get_row(0, row_res));
  const ObStoreRow *row_scan = NULL;
  OK(scanner->get_next_row(row_scan));
  STORAGE_LOG(INFO, "row_res", K(row_res->row_val_));
  STORAGE_LOG(INFO, "row_scan", K(row_scan->row_val_));
  ASSERT_TRUE(ObMockIterator::equals(row_res->row_val_, row_scan->row_val_));
  scanner->~ObStoreRowIterator();
}

TEST_F(TestMultiVersionSparseSSTableSingleScan, bug16228318)
{
  const int64_t rowkey_cnt = 4;
  const char *micro_data[2];
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1  -5       NOP      2       EXIST   CL\n";
  micro_data[1] =
      "bigint   var   bigint   bigint   bigint  flag    multi_version_row_flag\n"
      "1        var2  -2       NOP        NOP     DELETE  CL\n";

  prepare_data(micro_data, 2, rowkey_cnt, 5);

  ObMockIterator res_iter;
  ObStoreRowIterator *scanner = NULL;
  ObExtStoreRange range;

  const char *result1 =
      "bigint   var   bigint   bigint  flag    multi_version_row_flag\n"
      "1        var2  NOP      NOP     DELETE  N\n";

  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 0;
  trans_version_range.multi_version_start_ = 0;
  trans_version_range.snapshot_version_ = 7;
  prepare_query_param(trans_version_range, false, false, true);

  res_iter.reset();
  range.get_range().set_whole_range();
  OK(range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));
  OK(sstable_.scan(param_, context_, range, scanner));
  OK(res_iter.from(result1));
  const ObStoreRow *row_res = NULL;
  OK(res_iter.get_row(0, row_res));
  const ObStoreRow *row_scan = NULL;
  OK(scanner->get_next_row(row_scan));
  ASSERT_TRUE(ObMockIterator::equals(row_res->row_val_, row_scan->row_val_));
  scanner->~ObStoreRowIterator();
}

TEST_F(TestMultiVersionSparseSSTableSingleScan, minor_whole_scan_reuse_uncomp_buf1)
{
  const int64_t rowkey_cnt = 2;
  const char *micro_data[2];

  micro_data[0] =
    "var   bigint  flag    multi_version_row_flag\n"
    "var1    -3    EXIST   CLF\n"
    "var2    -4    EXIST   CLF\n"
    "var3    -4    EXIST   CLF\n"
    "var4    -6    EXIST   CLF\n";

  micro_data[1] =
    "var   bigint  flag    multi_version_row_flag\n"
    "var5    -7    EXIST   CLF\n"
    "var6    -8    EXIST   CLF\n"
    "var7    -8    EXIST   CLF\n"
    "var8    -9    EXIST   CLF\n";

  prepare_data(micro_data, 2, rowkey_cnt, 9, "lz4_1.0");

  ObMockIterator res_iter;
  ObStoreRowIterator *scanner = NULL;
  ObExtStoreRange range;

  const char *result1 =
    "var   bigint  flag    multi_version_row_flag\n"
    "var1    -3    EXIST   CLF\n"
    "var2    -4    EXIST   CLF\n"
    "var3    -4    EXIST   CLF\n"
    "var4    -6    EXIST   CLF\n"
    "var5    -7    EXIST   CLF\n"
    "var6    -8    EXIST   CLF\n"
    "var7    -8    EXIST   CLF\n"
    "var8    -9    EXIST   CLF\n";

  // multi version start > max trans version in sstable
  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 1;
  trans_version_range.multi_version_start_ = 10;
  trans_version_range.snapshot_version_ = 15;
  // minor merge
  prepare_query_param(trans_version_range, true, true);

  const ObStoreRow *row = NULL;
  const char *start_key =
    "var   bigint  flag    multi_version_row_flag\n"
    "var0    -3    EXIST   CL\n";
  const char *end_key =
    "var   bigint  flag    multi_version_row_flag\n"
    "var8    -9    EXIST   CL\n";
  ObMockIterator start_key_iter, end_key_iter;
  OK(start_key_iter.from(start_key));
  OK(end_key_iter.from(end_key));
  OK(start_key_iter.get_next_row(row));
  range.get_range().get_start_key().assign(row->row_val_.cells_, 2);
  range.get_range().get_border_flag().unset_inclusive_start();
  OK(end_key_iter.get_next_row(row));
  range.get_range().get_end_key().assign(row->row_val_.cells_, 2);
  range.get_range().get_border_flag().set_inclusive_end();
  OK(range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));

  res_iter.reset();
  OK(sstable_.scan(param_, context_, range, scanner));
  OK(res_iter.from(result1));
  ASSERT_TRUE(res_iter.equals(*scanner, true));
  scanner->~ObStoreRowIterator();
}

TEST_F(TestMultiVersionSparseSSTableSingleScan, minor_whole_scan_reuse_uncomp_buf2)
{
  const int64_t rowkey_cnt = 2;
  const char *micro_data[2];

  // var4 span two micro block
  micro_data[0] =
    "var   bigint  flag    multi_version_row_flag\n"
    "var1    -3    EXIST   CLF\n"
    "var2    -4    EXIST   CLF\n"
    "var3    -4    EXIST   CLF\n"
    "var4    -7    EXIST   CF\n";

  micro_data[1] =
    "var   bigint  flag    multi_version_row_flag\n"
    "var4    -6    EXIST   L\n"
    "var6    -8    EXIST   CLF\n"
    "var7    -8    EXIST   CLF\n"
    "var8    -9    EXIST   CLF\n";

  prepare_data(micro_data, 2, rowkey_cnt, 9, "lz4_1.0");

  ObMockIterator res_iter;
  ObStoreRowIterator *scanner = NULL;
  ObExtStoreRange range;

  const char *result1 =
    "var   bigint  flag    multi_version_row_flag\n"
    "var1    -3    EXIST   CLF\n"
    "var2    -4    EXIST   CLF\n"
    "var3    -4    EXIST   CLF\n"
    "var4    -7    EXIST   CLF\n"
    "var6    -8    EXIST   CLF\n"
    "var7    -8    EXIST   CLF\n"
    "var8    -9    EXIST   CLF\n";

  // multi version start > max trans version in sstable
  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 1;
  trans_version_range.multi_version_start_ = 10;
  trans_version_range.snapshot_version_ = 15;
  // minor merge
  prepare_query_param(trans_version_range, true, true);

  const ObStoreRow *row = NULL;
  const char *start_key =
    "var   bigint  flag    multi_version_row_flag\n"
    "var0    -3    EXIST   CL\n";
  const char *end_key =
    "var   bigint  flag    multi_version_row_flag\n"
    "var8    -9    EXIST   CL\n";
  ObMockIterator start_key_iter, end_key_iter;
  OK(start_key_iter.from(start_key));
  OK(end_key_iter.from(end_key));
  OK(start_key_iter.get_next_row(row));
  range.get_range().get_start_key().assign(row->row_val_.cells_, 2);
  range.get_range().get_border_flag().unset_inclusive_start();
  OK(end_key_iter.get_next_row(row));
  range.get_range().get_end_key().assign(row->row_val_.cells_, 2);
  range.get_range().get_border_flag().set_inclusive_end();
  OK(range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));

  res_iter.reset();
  OK(sstable_.scan(param_, context_, range, scanner));
  OK(res_iter.from(result1));
  ASSERT_TRUE(res_iter.equals(*scanner, true));
  scanner->~ObStoreRowIterator();
}

TEST_F(TestMultiVersionSparseSSTableSingleScan, minor_whole_scan_reuse_uncomp_buf3)
{
  const int64_t rowkey_cnt = 2;
  const char *micro_data[2];

  // last row of micro 0 is var4 with delete flag
  // first row of micro 1 is var4 with insert flag
  micro_data[0] =
    "var   bigint  flag    multi_version_row_flag\n"
    "var1    -3    EXIST   CLF\n"
    "var2    -4    EXIST   CLF\n"
    "var3    -4    EXIST   CLF\n"
    "var4    -7    DELETE  CF\n";

  micro_data[1] =
    "var   bigint  flag    multi_version_row_flag\n"
    "var4    -6    EXIST   L\n"
    "var6    -8    EXIST   CLF\n"
    "var7    -8    EXIST   CLF\n"
    "var8    -9    EXIST   CLF\n";

  prepare_data(micro_data, 2, rowkey_cnt, 9, "lz4_1.0");

  ObMockIterator res_iter;
  ObStoreRowIterator *scanner = NULL;
  ObExtStoreRange range;

  const char *result1 =
    "var   bigint  flag    multi_version_row_flag\n"
    "var1    -3    EXIST   CLF\n"
    "var2    -4    EXIST   CLF\n"
    "var3    -4    EXIST   CLF\n"
    "var4    -7    DELETE  CLF\n"
    "var6    -8    EXIST   CLF\n"
    "var7    -8    EXIST   CLF\n"
    "var8    -9    EXIST   CLF\n";

  // multi version start > max trans version in sstable
  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 1;
  trans_version_range.multi_version_start_ = 10;
  trans_version_range.snapshot_version_ = 15;
  // minor merge
  prepare_query_param(trans_version_range, true, true);

  const ObStoreRow *row = NULL;
  const char *start_key =
    "var   bigint  flag    multi_version_row_flag\n"
    "var0    -3    EXIST   CL\n";
  const char *end_key =
    "var   bigint  flag    multi_version_row_flag\n"
    "var8    -9    EXIST   CL\n";
  ObMockIterator start_key_iter, end_key_iter;
  OK(start_key_iter.from(start_key));
  OK(end_key_iter.from(end_key));
  OK(start_key_iter.get_next_row(row));
  range.get_range().get_start_key().assign(row->row_val_.cells_, 2);
  range.get_range().get_border_flag().unset_inclusive_start();
  OK(end_key_iter.get_next_row(row));
  range.get_range().get_end_key().assign(row->row_val_.cells_, 2);
  range.get_range().get_border_flag().set_inclusive_end();
  OK(range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));

  res_iter.reset();
  OK(sstable_.scan(param_, context_, range, scanner));
  OK(res_iter.from(result1));
  ASSERT_TRUE(res_iter.equals(*scanner, true));
  scanner->~ObStoreRowIterator();
}

TEST_F(TestMultiVersionSparseSSTableSingleScan, major_whole_scan_reuse_uncomp_buf)
{
  const int64_t rowkey_cnt = 2;
  const char *micro_data[2];

  micro_data[0] =
    "var   bigint  bigint flag    multi_version_row_flag\n"
    "var1    -3      1    EXIST   CL\n"
    "var2    -4      1    EXIST   CL\n"
    "var3    -9      1    EXIST   C\n"
    "var3    -7      NOP  EXIST   N\n";

  micro_data[1] =
    "var   bigint  bigint flag    multi_version_row_flag\n"
    "var3    -6      1    EXIST   L\n"
    "var6    -8      1    EXIST   CL\n"
    "var7    -8      1    EXIST   CL\n"
    "var8    -9      1    EXIST   CL\n";

  prepare_data(micro_data, 2, rowkey_cnt, 9, "lz4_1.0");

  ObMockIterator res_iter;
  ObStoreRowIterator *scanner = NULL;
  ObExtStoreRange range;

  const char *result1 =
    "var   bigint  flag   multi_version_row_flag\n"
    "var1    1    EXIST   CL\n"
    "var2    1    EXIST   CL\n"
    "var3    1    EXIST   N\n"
    "var6    1    EXIST   CL\n"
    "var7    1    EXIST   CL\n";

  // snapshot_version < max trans version of var3
  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 1;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.snapshot_version_ = 8;
  prepare_query_param(trans_version_range, true);

  const ObStoreRow *row = NULL;
  const char *start_key =
    "var   flag    multi_version_row_flag\n"
    "var0  EXIST   CL\n";
  const char *end_key =
    "var   flag    multi_version_row_flag\n"
    "var8  EXIST   CL\n";
  ObMockIterator start_key_iter, end_key_iter;
  OK(start_key_iter.from(start_key));
  OK(end_key_iter.from(end_key));
  OK(start_key_iter.get_next_row(row));
  range.get_range().get_start_key().assign(row->row_val_.cells_, 1);
  range.get_range().get_border_flag().unset_inclusive_start();
  OK(end_key_iter.get_next_row(row));
  range.get_range().get_end_key().assign(row->row_val_.cells_, 1);
  range.get_range().get_border_flag().set_inclusive_end();
  OK(range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));

  res_iter.reset();
  OK(sstable_.scan(param_, context_, range, scanner));
  OK(res_iter.from(result1));
  ASSERT_TRUE(res_iter.equals(*scanner, true));
  scanner->~ObStoreRowIterator();
}

TEST_F(TestMultiVersionSparseSSTableSingleScan, bug_14915200)
{
  const int64_t rowkey_cnt = 4;
  const char *micro_data[2];
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1  -8       NOP      2       EXIST   C\n"
      "1        var1  -2       2        NOP     EXIST   L\n"
      "2        var2  -10      4        3       EXIST   CL\n"
      "3        var3  -8       7        2       EXIST   C\n"
      "3        var3  -5       NOP      1       EXIST   N\n"
      "3        var3  -3       6        NOP     EXIST   L\n"
      "3        var4  -3       10       NOP     DELETE  L\n"
      "4        var4  -9       7        NOP     EXIST   C\n"
      "4        var4  -7       6        5       EXIST   C\n"
      "4        var4  -3       5        NOP     EXIST   L\n"
      "5        var5  -79      NOP      5       EXIST   N\n"
      "5        var5  -78      NOP      5       EXIST   N\n"
      "5        var5  -77      NOP      5       EXIST   N\n"
      "5        var5  -76      NOP      5       EXIST   N\n"
      "5        var5  -75      NOP      5       EXIST   N\n"
      "5        var5  -74      NOP      5       EXIST   N\n"
      "5        var5  -73      NOP      5       EXIST   N\n"
      "5        var5  -72      NOP      5       EXIST   N\n"
      "5        var5  -71      NOP      5       EXIST   N\n"
      "5        var5  -70      NOP      5       EXIST   N\n"
      "5        var5  -69      NOP      5       EXIST   N\n"
      "5        var5  -68      NOP      5       EXIST   N\n"
      "5        var5  -67      NOP      5       EXIST   N\n"
      "5        var5  -66      NOP      5       EXIST   N\n"
      "5        var5  -65      NOP      5       EXIST   N\n"
      "5        var5  -64      NOP      5       EXIST   N\n"
      "5        var5  -63      NOP      5       EXIST   N\n"
      "5        var5  -62      NOP      5       EXIST   N\n"
      "5        var5  -61      NOP      5       EXIST   N\n"
      "5        var5  -59      NOP      5       EXIST   N\n"
      "5        var5  -58      NOP      5       EXIST   N\n"
      "5        var5  -57      NOP      5       EXIST   N\n"
      "5        var5  -56      NOP      5       EXIST   N\n";

  micro_data[1] =
      "bigint   var   bigint   bigint   bigint  flag    multi_version_row_flag\n"
      "5        var5  -6       6        NOP     EXIST   N\n"
      "5        var5  -4       7        4       EXIST   N\n"
      "5        var5  -3       5        2       EXIST   N\n"
      "5        var5  -2       5        1       EXIST   C\n"
      "5        var5  -1       4        3       EXIST   L\n";

  prepare_data(micro_data, 2, rowkey_cnt, 80);

  ObMockIterator res_iter;
  ObStoreRowIterator *scanner = NULL;
  ObExtStoreRange range;

  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 0;
  trans_version_range.multi_version_start_ = 0;
  trans_version_range.snapshot_version_ = 100;
  prepare_query_param(trans_version_range, false);

  const char *reverse_result1 =
      "bigint   var   bigint   bigint  flag    multi_version_row_flag\n"
      "5        var5  6        5       EXIST   N\n"
      "4        var4  7        NOP     EXIST   N\n"
      "3        var4  10       NOP     DELETE  N\n"
      "3        var3  7        2       EXIST   N\n"
      "2        var2  4        3       EXIST   N\n"
      "1        var1  NOP      2       EXIST   N\n";

  prepare_query_param(trans_version_range, false, false, true );//reverse_scan
  res_iter.reset();
  range.get_range().set_whole_range();
  OK(range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));
  OK(sstable_.scan(param_, context_, range, scanner));
  OK(res_iter.from(reverse_result1));
  ASSERT_TRUE(res_iter.equals(*scanner));
  scanner->~ObStoreRowIterator();
}

//TEST_F(TestMultiVersionSparseSSTableSingleScan, test_bloom_filter)
//{
  //const int64_t rowkey_cnt = 4;
  //const char *micro_data[2];
  //micro_data[0] =
      //"bigint   var   bigint   bigint   bigint  flag    multi_version_row_flag\n"
      //"3        var2  -18      NOP      8       EXIST   C\n"
      //"3        var2  -15      NOP      11      EXIST   N\n"
      //"3        var2  -13      NOP      9       EXIST   N\n";
  //micro_data[1] =
      //"bigint   var   bigint   bigint   bigint  flag    multi_version_row_flag\n"
      //"3        var6  -8       NOP      8       EXIST   N\n"
      //"3        var6  -5       6        10      EXIST   N\n"
      //"3        var6  -3       5        9       EXIST   L\n";

  //prepare_data_start(micro_data, rowkey_cnt, 18);
  //prepare_one_macro(micro_data, 1);
  //prepare_one_macro(&micro_data[1], 1);
  //prepare_data_end();

  //ObMockIterator rowkeys_iter;
  //ObStoreRowIterator *scanner = NULL;
  //ObExtStoreRange range;

  //const char *rowkeys_str =
      //"bigint   var   flag\n"
      //"2        var1  EXIST\n"
      //"2        var2  EXIST\n"
      //"3        var4  EXIST\n"
      //"3        var5  EXIST\n"
      //"2        var4  EXIST\n"
      //"2        var5  EXIST\n"
      //"3        var3  EXIST\n"
      //"3        var4  EXIST\n";

  //ObVersionRange trans_version_range;
  //trans_version_range.base_version_ = 0;
  //trans_version_range.multi_version_start_ = 0;
  //trans_version_range.snapshot_version_ = 16;
  //prepare_query_param(trans_version_range, false);
  //sstable_.key_.trans_version_range_.snapshot_version_ = 10;

  //OK(rowkeys_iter.from(rowkeys_str));
  //const ObStoreRow *start = NULL;
  //const ObStoreRow *end = NULL;
  //const ObStoreRow *row = NULL;
  //const ObMacroBlockMeta *meta = NULL;

  //range.reset();
  //OK(rowkeys_iter.get_row(0, start));
  //OK(rowkeys_iter.get_row(1, end));
  //make_range(start, end, range);

  //OK(sstable_.scan(param_, context_, range, scanner));
  //ASSERT_EQ(OB_ITER_END, scanner->get_next_row(row));
  //scanner->~ObStoreRowIterator();
  //get_macro_meta(1, 1, meta);
  //ASSERT_EQ(1, meta->empty_read_cnt_[0]);

  //range.reset();
  //OK(rowkeys_iter.get_row(2, start));
  //OK(rowkeys_iter.get_row(3, end));
  //make_range(start, end, range);

  //OK(sstable_.scan(param_, context_, range, scanner));
  //ASSERT_EQ(OB_ITER_END, scanner->get_next_row(row));
  //scanner->~ObStoreRowIterator();
  //get_macro_meta(2, 1, meta);
  //ASSERT_EQ(1, meta->empty_read_cnt_[0]);

  //range.reset();
  //OK(rowkeys_iter.get_row(4, start));
  //OK(rowkeys_iter.get_row(5, end));
  //make_range(start, end, range);

  //OK(sstable_.scan(param_, context_, range, scanner));
  //ASSERT_EQ(OB_ITER_END, scanner->get_next_row(row));
  //scanner->~ObStoreRowIterator();
  //get_macro_meta(1, 1, meta);
  //ASSERT_EQ(2, meta->empty_read_cnt_[0]);

  //range.reset();
  //OK(rowkeys_iter.get_row(6, start));
  //OK(rowkeys_iter.get_row(7, end));
  //make_range(start, end, range);

  //OK(sstable_.scan(param_, context_, range, scanner));
  //ASSERT_EQ(OB_ITER_END, scanner->get_next_row(row));
  //scanner->~ObStoreRowIterator();
  //get_macro_meta(2, 1, meta);
  //ASSERT_EQ(2, meta->empty_read_cnt_[0]);

  //prepare_query_param(trans_version_range, false, false, true);
  //range.reset();
  //OK(rowkeys_iter.get_row(0, start));
  //OK(rowkeys_iter.get_row(1, end));
  //make_range(start, end, range);

  //OK(sstable_.scan(param_, context_, range, scanner));
  //ASSERT_EQ(OB_ITER_END, scanner->get_next_row(row));
  //scanner->~ObStoreRowIterator();
  //get_macro_meta(1, 1, meta);
  //ASSERT_EQ(3, meta->empty_read_cnt_[0]);

  //range.reset();
  //OK(rowkeys_iter.get_row(2, start));
  //OK(rowkeys_iter.get_row(3, end));
  //make_range(start, end, range);

  //OK(sstable_.scan(param_, context_, range, scanner));
  //ASSERT_EQ(OB_ITER_END, scanner->get_next_row(row));
  //scanner->~ObStoreRowIterator();
  //get_macro_meta(2, 1, meta);
  //ASSERT_EQ(3, meta->empty_read_cnt_[0]);

  //range.reset();
  //OK(rowkeys_iter.get_row(4, start));
  //OK(rowkeys_iter.get_row(5, end));
  //make_range(start, end, range);

  //OK(sstable_.scan(param_, context_, range, scanner));
  //ASSERT_EQ(OB_ITER_END, scanner->get_next_row(row));
  //scanner->~ObStoreRowIterator();
  //get_macro_meta(1, 1, meta);
  //ASSERT_EQ(4, meta->empty_read_cnt_[0]);

  //range.reset();
  //OK(rowkeys_iter.get_row(6, start));
  //OK(rowkeys_iter.get_row(7, end));
  //make_range(start, end, range);

  //OK(sstable_.scan(param_, context_, range, scanner));
  //ASSERT_EQ(OB_ITER_END, scanner->get_next_row(row));
  //scanner->~ObStoreRowIterator();
  //get_macro_meta(2, 1, meta);
  //ASSERT_EQ(4, meta->empty_read_cnt_[0]);
//}

TEST_F(TestMultiVersionSparseSSTableSingleScan, minor_for_migration)
{
  const int64_t rowkey_cnt = 4;
  const char *micro_data[3];
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1  -8       NOP      2       EXIST   CF\n"
      "1        var1  -2       2        NOP     EXIST   L\n"
      "2        var2  -7       4        3       EXIST   CLF\n"
      "3        var3  -8       7        2       EXIST   CF\n"
      "3        var3  -5       NOP      1       EXIST   N\n"
      "3        var3  -3       7        NOP     EXIST   L\n"
      "4        var4  -9       7        NOP     EXIST   CF\n"
      "4        var4  -7       6        5       EXIST   C\n"
      "4        var4  -3       7        NOP     EXIST   L\n";

  micro_data[1] =
      "bigint   var   bigint   bigint   bigint  flag    multi_version_row_flag\n"
      "5        var5  -6       6        2       EXIST   CF\n"
      "5        var5  -4       7        2       EXIST   N\n";

  micro_data[2] =
      "bigint   var   bigint   bigint   bigint  flag    multi_version_row_flag\n"
      "5        var5  -3       5        2       EXIST   N\n"
      "5        var5  -2       5        1       EXIST   C\n"
      "5        var5  -1       4        3       EXIST   L\n"
      "6        var6  -10      10       10      EXIST   CF\n"
      "6        var6  -6       6        NOP     EXIST   N\n"
      "6        var6  -5       5        NOP     EXIST   N\n"
      "6        var6  -4       4        NOP     EXIST   L\n"
      "6        var7  -20      20       NOP     EXIST   CF\n"
      "6        var7  -3       3        NOP     EXIST   L\n"
      "7        var7  -10      10       10      EXIST   CF\n"
      "7        var7  -6       6        NOP     EXIST   N\n"
      "7        var7  -5       5        NOP     EXIST   N\n"
      "7        var7  -4       4        NOP     EXIST   L\n";

  prepare_data_start(micro_data, rowkey_cnt, 20);
  prepare_one_macro(micro_data, 2);
  prepare_one_macro(&micro_data[2], 1);
  prepare_data_end();

  ObMockIterator res_iter;
  ObStoreRowIterator *scanner = NULL;
  ObExtStoreRange range;

  const char *result1 =
      "bigint   var   bigint   bigint   bigint  flag    multi_version_row_flag\n"
      "2        var2  -7       4        3       EXIST   CLF\n"
      "3        var3  -5       NOP      1       EXIST   CLF\n"
      "4        var4  -7       6        5       EXIST   CLF\n"
      "5        var5  -6       6        2       EXIST   CLF\n"
      "6        var6  -6       6        NOP     EXIST   CF\n"
      "6        var6  -5       5        NOP     EXIST   L\n"
      "7        var7  -6       6        NOP     EXIST   CF\n"
      "7        var7  -5       5        NOP     EXIST   L\n";

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 7;
  trans_version_range.multi_version_start_ = 4;
  trans_version_range.base_version_ = 4;
  // minor merge
  prepare_query_param(trans_version_range, true, true);

  res_iter.reset();
  range.get_range().set_whole_range();
  OK(range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));
  OK(sstable_.scan(param_, context_, range, scanner));
  OK(res_iter.from(result1));
  ASSERT_TRUE(res_iter.equals(*scanner, true));//cmp multi version row flag
  scanner->~ObStoreRowIterator();

  //
  const char *result2 =
      "bigint   var   bigint   bigint   bigint  flag    multi_version_row_flag\n"
      "2        var2  -7       4        3       EXIST   CLF\n"
      "3        var3  -5       7        1       EXIST   CF\n"
      "3        var3  -3       7        NOP     EXIST   CL\n"
      "4        var4  -7       6        5       EXIST   CF\n"
      "4        var4  -3       7        NOP     EXIST   CL\n"
      "5        var5  -6       6        2       EXIST   CF\n"
      "5        var5  -4       7        2       EXIST   CL\n"
      "6        var6  -6       6        NOP     EXIST   CF\n"
      "6        var6  -5       5        NOP     EXIST   N\n"
      "6        var6  -4       4        NOP     EXIST   CL\n"
      "6        var7  -3       3        NOP     EXIST   CLF\n"
      "7        var7  -6       6        NOP     EXIST   CF\n"
      "7        var7  -5       5        NOP     EXIST   N\n"
      "7        var7  -4       4        NOP     EXIST   CL\n";
  trans_version_range.base_version_ = 2;
  prepare_query_param(trans_version_range, true, true);

  res_iter.reset();
  range.get_range().set_whole_range();
  OK(range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));
  OK(sstable_.scan(param_, context_, range, scanner));
  OK(res_iter.from(result2));
  ASSERT_TRUE(res_iter.equals(*scanner, true));//cmp multi version row flag
  scanner->~ObStoreRowIterator();
}

TEST_F(TestMultiVersionSparseSSTableSingleScan, test_minor_whole_scan_dml)
{
  const int64_t rowkey_cnt = 2;
  const char *micro_data[2];

  // last row of micro 0 is var4 with delete flag
  // first row of micro 1 is var4 with insert flag
  micro_data[0] =
    "var   bigint  flag    dml     first_dml    multi_version_row_flag\n"
    "var1    -3    EXIST   INSERT  INSERT       CLF\n"
    "var2    -4    EXIST   INSERT  DELETE       CF\n"
    "var2    -3    DELETE  DELETE  DELETE       CL\n"
    "var4    -7    DELETE  DELETE  INSERT       CF\n";

  micro_data[1] =
    "var   bigint  flag    dml     first_dml    multi_version_row_flag\n"
    "var4    -6    EXIST   INSERT  INSERT       L\n"
    "var6    -9    EXIST   INSERT  UPDATE       CF\n"
    "var6    -8    EXIST   UPDATE  UPDATE       CL\n"
    "var8    -9    EXIST   INSERT  INSERT       CLF\n";

  prepare_data(micro_data, 2, rowkey_cnt, 9, "lz4_1.0");

  ObMockIterator res_iter;
  ObStoreRowIterator *scanner = NULL;
  ObExtStoreRange range;

  const char *result1 =
    "var   bigint  flag    dml     first_dml    multi_version_row_flag\n"
    "var1    -3    EXIST   INSERT  INSERT       CLF\n"
    "var2    -4    EXIST   INSERT  DELETE       CLF\n"
    "var4    -7    DELETE  DELETE  INSERT       CLF\n"
    "var6    -9    EXIST   INSERT  UPDATE       CF\n"
    "var6    -8    EXIST   UPDATE  UPDATE       CL\n"
    "var8    -9    EXIST   INSERT  INSERT       CLF\n";

  // multi version start > max trans version in sstable
  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 1;
  trans_version_range.multi_version_start_ = 7;
  trans_version_range.snapshot_version_ = 15;
  // minor merge
  prepare_query_param(trans_version_range, true, true);

  const ObStoreRow *row = NULL;
  const char *start_key =
    "var   bigint  flag    multi_version_row_flag\n"
    "var0    -3    EXIST   CL\n";
  const char *end_key =
    "var   bigint  flag    multi_version_row_flag\n"
    "var8    -9    EXIST   CL\n";
  ObMockIterator start_key_iter, end_key_iter;
  OK(start_key_iter.from(start_key));
  OK(end_key_iter.from(end_key));
  OK(start_key_iter.get_next_row(row));
  range.get_range().get_start_key().assign(row->row_val_.cells_, 2);
  range.get_range().get_border_flag().unset_inclusive_start();
  OK(end_key_iter.get_next_row(row));
  range.get_range().get_end_key().assign(row->row_val_.cells_, 2);
  range.get_range().get_border_flag().set_inclusive_end();
  OK(range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));

  res_iter.reset();
  OK(sstable_.scan(param_, context_, range, scanner));
  OK(res_iter.from(result1));
  ASSERT_TRUE(res_iter.equals(*scanner, true));
  scanner->~ObStoreRowIterator();
}

TEST_F(TestMultiVersionSparseSSTableSingleScan, test_skip_single_range)
{
  const int64_t rowkey_cnt = 4;
  const char *micro_data[3];
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1  -8       NOP      2       EXIST   C\n"
      "1        var1  -2       2        NOP     EXIST   L\n"
      "2        var2  -10      4        3       EXIST   CL\n"
      "3        var3  -8       7        2       EXIST   C\n"
      "3        var3  -5       NOP      1       EXIST   N\n"
      "3        var3  -3       6        NOP     EXIST   L\n"
      "3        var4  -3       10       NOP     DELETE  L\n"
      "4        var4  -9       7        NOP     EXIST   C\n"
      "4        var4  -7       6        5       EXIST   C\n"
      "4        var4  -3       5        NOP     EXIST   L\n";

  micro_data[1] =
      "bigint   var   bigint   bigint   bigint  flag    multi_version_row_flag\n"
      "5        var5  -6       6        NOP     EXIST   N\n"
      "5        var5  -4       7        4       EXIST   N\n";

  micro_data[2] =
      "bigint   var   bigint   bigint   bigint  flag    multi_version_row_flag\n"
      "5        var5  -3       5        2       EXIST   N\n"
      "5        var5  -2       5        1       EXIST   C\n"
      "5        var5  -1       4        3       EXIST   L\n";

  prepare_data(micro_data, 3, rowkey_cnt, 10);

  ObMockIterator res_iter;
  ObStoreRowIterator *scanner = NULL;
  ObExtStoreRange range;

  const char *skip_result1 =
      "bigint   var   bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1  2        NOP     EXIST   N\n"
      "4        var4  6        5       EXIST   N\n"
      "5        var5  6        4       EXIST   N\n";
  const char *gap_key=
      "bigint   var   bigint   bigint  flag    multi_version_row_flag\n"
      "3        var5  10       NOP     DELETE  N\n";
  ObMockIterator gap_iter;
  ObArray<ObStoreRowkey *> gap_rowkeys;
  OK(gap_iter.from(gap_key));
  ASSERT_EQ(OB_SUCCESS, convert_iter_to_gapkeys(2, gap_iter, gap_rowkeys));

  ObArray<SkipInfo> skip_infos;
  ObArray<int64_t> range_idx;
  SkipInfo info;
  info.start_key_ = 1;
  info.gap_key_ = 3;
  ASSERT_EQ(OB_SUCCESS, skip_infos.push_back(info));


  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 0;
  trans_version_range.multi_version_start_ = 0;
  trans_version_range.snapshot_version_ = 7;
  prepare_query_param(trans_version_range, false);

  res_iter.reset();
  range.get_range().set_whole_range();
  OK(range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));
  OK(sstable_.scan(param_, context_, range, scanner));
  OK(res_iter.from(skip_result1));
  ASSERT_EQ(OB_SUCCESS, range_idx.push_back(0L));
  ASSERT_EQ(OB_SUCCESS, range_idx.push_back(0L));
  ASSERT_EQ(OB_SUCCESS, compare_skip_iter(res_iter, scanner, skip_infos, gap_rowkeys, range_idx));
  scanner->~ObStoreRowIterator();

  const char *reverse_skip_result1 =
      "bigint   var   bigint   bigint  flag    multi_version_row_flag\n"
      "5        var5  6        4       EXIST   N\n"
      "1        var1  2        NOP     EXIST   N\n";
  const char *reverse_gap_key=
      "bigint   var   bigint   bigint  flag    multi_version_row_flag\n"
      "3        var1  10       NOP     DELETE  N\n";

  prepare_query_param(trans_version_range, false, false, true );//reverse_scan
  res_iter.reset();
  range.get_range().set_whole_range();
  OK(range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));
  OK(gap_iter.from(reverse_gap_key));
  ASSERT_EQ(OB_SUCCESS, convert_iter_to_gapkeys(2, gap_iter, gap_rowkeys));
  OK(sstable_.scan(param_, context_, range, scanner));
  OK(res_iter.from(reverse_skip_result1));
  ASSERT_EQ(OB_SUCCESS, compare_skip_iter(res_iter, scanner, skip_infos, gap_rowkeys, range_idx));
  scanner->~ObStoreRowIterator();
}

TEST_F(TestMultiVersionSparseSSTableSingleScan, test_skip_multiple_ranges)
{
  const int64_t rowkey_cnt = 4;
  const char *micro_data[3];
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1  -8       NOP      2       EXIST   C\n"
      "1        var1  -2       2        NOP     EXIST   L\n"
      "2        var2  -10      4        3       EXIST   CL\n"
      "3        var3  -8       7        2       EXIST   C\n"
      "3        var3  -5       NOP      1       EXIST   N\n"
      "3        var3  -3       6        NOP     EXIST   L\n"
      "4        var4  -9       7        NOP     EXIST   C\n"
      "4        var4  -7       6        5       EXIST   C\n"
      "4        var4  -3       5        NOP     EXIST   L\n";

  micro_data[1] =
      "bigint   var   bigint   bigint   bigint  flag    multi_version_row_flag\n"
      "5        var5  -6       6        NOP     EXIST   N\n"
      "5        var5  -4       7        NOP     EXIST   N\n";

  micro_data[2] =
      "bigint   var   bigint   bigint   bigint  flag    multi_version_row_flag\n"
      "5        var5  -3       5        2       EXIST   N\n"
      "5        var5  -2       5        1       EXIST   C\n"
      "5        var5  -1       4        3       EXIST   L\n";

  prepare_data_start(micro_data, rowkey_cnt, 10);
  prepare_one_macro(micro_data, 1);
  prepare_one_macro(&micro_data[1], 1);
  prepare_one_macro(&micro_data[2], 1);
  prepare_data_end();

  ObMockIterator res_iter;
  ObStoreRowIterator *scanner = NULL;
  ObExtStoreRange range;

  const char *skip_result1 =
      "bigint   var   bigint   bigint  flag    multi_version_row_flag\n"
      "3        var3  NOP      1       EXIST   N\n"
      "4        var4  6        5       EXIST   N\n";
  const char *gap_key =
      "bigint   var   bigint   bigint  flag    multi_version_row_flag\n"
      "4        var3  6        5       EXIST   N\n"
      "5        var6  6        5       EXIST   N\n";
  ObMockIterator gap_iter;
  ObArray<ObStoreRowkey *> gap_rowkeys;
  OK(gap_iter.from(gap_key));
  ASSERT_EQ(OB_SUCCESS, convert_iter_to_gapkeys(2, gap_iter, gap_rowkeys));

  ObArray<SkipInfo> skip_infos;
  ObArray<int64_t> range_idx;
  SkipInfo info;
  info.start_key_ = 1;
  info.gap_key_ = 1;
  ASSERT_EQ(OB_SUCCESS, skip_infos.push_back(info));
  info.start_key_ = 2;
  info.gap_key_ = 2;
  ASSERT_EQ(OB_SUCCESS, skip_infos.push_back(info));

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 7;
  trans_version_range.multi_version_start_ = 0;
  trans_version_range.base_version_ = 4;
  prepare_query_param(trans_version_range, false);

  res_iter.reset();
  range.get_range().set_whole_range();
  OK(range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));
  OK(sstable_.scan(param_, context_, range, scanner));
  OK(res_iter.from(skip_result1));
  ASSERT_EQ(OB_SUCCESS, range_idx.push_back(0L));
  ASSERT_EQ(OB_SUCCESS, range_idx.push_back(0L));
  ASSERT_EQ(OB_SUCCESS, compare_skip_iter(res_iter, scanner, skip_infos, gap_rowkeys, range_idx));
  scanner->~ObStoreRowIterator();

  const char *reverse_skip_result1 =
      "bigint   var   bigint   bigint  flag    multi_version_row_flag\n"
      "5        var5  6        NOP     EXIST   N\n"
      "3        var3  NOP      1       EXIST   N\n";
  const char *reverse_gap_key =
      "bigint   var   bigint   bigint  flag    multi_version_row_flag\n"
      "4        var3  6        5       EXIST   N\n"
      "3        var1  6        5       EXIST   N\n";

  prepare_query_param(trans_version_range, false, false, true );//reverse_scan
  res_iter.reset();
  range.get_range().set_whole_range();
  OK(range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));
  OK(gap_iter.from(reverse_gap_key));
  ASSERT_EQ(OB_SUCCESS, convert_iter_to_gapkeys(2, gap_iter, gap_rowkeys));
  OK(sstable_.scan(param_, context_, range, scanner));
  OK(res_iter.from(reverse_skip_result1));
  ASSERT_EQ(OB_SUCCESS, compare_skip_iter(res_iter, scanner, skip_infos, gap_rowkeys, range_idx));
  scanner->~ObStoreRowIterator();
}

TEST_F(TestMultiVersionSparseSSTableSingleScan, test_get_gap_flag)
{
  int ret = OB_SUCCESS;
  const ObStoreRow *row = NULL;
  uint8_t flag = 0;
  ObStoreRowIterator *scanner = NULL;
  ObExtStoreRange range;
  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 7;
  trans_version_range.multi_version_start_ = 0;
  trans_version_range.base_version_ = 0;
  range.get_range().set_whole_range();
  OK(range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));
  prepare_gap_schema();
  prepare_gap_sstable_data(ALL_DELETE);
  prepare_gap_query_param(trans_version_range, false);
  OK(gap_sstable_.scan(param_, context_, range, scanner));
  while (OB_SUCC(ret)) {
    if (OB_FAIL(scanner->get_next_row_ext(row, flag))) {
      if (OB_ITER_END == ret) {
        break;
      }
    } else {
      ASSERT_EQ(STORE_ITER_ROW_BIG_GAP_HINT | STORE_ITER_ROW_IN_GAP, flag);
    }
  }
  ASSERT_EQ(OB_ITER_END, ret);
  scanner->~ObStoreRowIterator();
}

TEST_F(TestMultiVersionSparseSSTableSingleScan, test_get_gap_flag_all_insert)
{
  int ret = OB_SUCCESS;
  const ObStoreRow *row = NULL;
  uint8_t flag = 0;
  ObStoreRowIterator *scanner = NULL;
  ObExtStoreRange range;
  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 7;
  trans_version_range.multi_version_start_ = 0;
  trans_version_range.base_version_ = 0;
  range.get_range().set_whole_range();
  OK(range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));
  prepare_gap_schema();
  prepare_gap_sstable_data(ALL_INSERT);
  prepare_gap_query_param(trans_version_range, false);
  OK(gap_sstable_.scan(param_, context_, range, scanner));
  while (OB_SUCC(ret)) {
    if (OB_FAIL(scanner->get_next_row_ext(row, flag))) {
      if (OB_ITER_END == ret) {
        break;
      }
    } else {
      ASSERT_EQ(0, flag);
    }
  }
  ASSERT_EQ(OB_ITER_END, ret);
  scanner->~ObStoreRowIterator();
}

TEST_F(TestMultiVersionSparseSSTableSingleScan, test_get_gap_end_all_delete)
{
  int ret = OB_SUCCESS;
  const ObStoreRow *row = NULL;
  uint8_t flag = 0;
  ObStoreRowIterator *scanner = NULL;
  ObExtStoreRange range;
  int64_t range_idx = -1;
  const common::ObStoreRowkey *gap_rowkey = NULL;
  int64_t gap_size = -1;
  ObRowGenerate row_generate;
  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 7;
  trans_version_range.multi_version_start_ = 0;
  trans_version_range.base_version_ = 0;
  prepare_gap_schema();
  prepare_gap_sstable_data(ALL_DELETE);
  ObObj check_cells[256];
  ObStoreRowkey check_rowkey;
  ObStoreRow check_row;
  check_row.row_val_.cells_ = check_cells;
  check_row.row_val_.count_ = table_schema_.get_column_count();
  ret = row_generate.init(table_schema_, &allocator_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = row_generate.get_next_row(999, check_row);
  ASSERT_EQ(OB_SUCCESS, ret);
  check_rowkey.assign(check_cells, table_schema_.get_rowkey_column_num());
  range.get_range().set_whole_range();
  OK(range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));
  prepare_gap_query_param(trans_version_range, false);
  OK(gap_sstable_.scan(param_, context_, range, scanner));
  ASSERT_EQ(OB_SUCCESS, scanner->get_next_row_ext(row, flag));
  ASSERT_EQ(STORE_ITER_ROW_BIG_GAP_HINT | STORE_ITER_ROW_IN_GAP, flag);
  ASSERT_EQ(OB_SUCCESS, scanner->get_gap_end(range_idx, gap_rowkey, gap_size));
  STORAGE_LOG(INFO, "get gap end info", K(range_idx), K(*gap_rowkey), K(gap_size));
  ASSERT_EQ(0, range_idx);
  ASSERT_TRUE(NULL != gap_rowkey);
  ASSERT_EQ(check_rowkey, *gap_rowkey);
  ASSERT_TRUE(gap_size > 0);
  scanner->~ObStoreRowIterator();
}

TEST_F(TestMultiVersionSparseSSTableSingleScan, test_get_gap_end_all_delete_with_insert)
{
  int ret = OB_SUCCESS;
  const ObStoreRow *row = NULL;
  uint8_t flag = 0;
  ObStoreRowIterator *scanner = NULL;
  ObExtStoreRange range;
  int64_t range_idx = -1;
  const common::ObStoreRowkey *gap_rowkey = NULL;
  int64_t gap_size = -1;
  ObRowGenerate row_generate;
  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 7;
  trans_version_range.multi_version_start_ = 0;
  trans_version_range.base_version_ = 0;
  prepare_gap_schema();
  prepare_gap_sstable_data(ALL_DELETE);
  ObObj check_cells[256];
  ObStoreRowkey check_rowkey;
  ObStoreRow check_row;
  check_row.row_val_.cells_ = check_cells;
  check_row.row_val_.count_ = table_schema_.get_column_count();
  ret = row_generate.init(table_schema_, &allocator_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = row_generate.get_next_row(999, check_row);
  ASSERT_EQ(OB_SUCCESS, ret);
  check_rowkey.assign(check_cells, table_schema_.get_rowkey_column_num());
  range.get_range().set_whole_range();
  OK(range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));
  prepare_gap_query_param(trans_version_range, false);
  OK(gap_sstable_.scan(param_, context_, range, scanner));
  ASSERT_EQ(OB_SUCCESS, scanner->get_next_row_ext(row, flag));
  ASSERT_EQ(STORE_ITER_ROW_BIG_GAP_HINT | STORE_ITER_ROW_IN_GAP, flag);
  ASSERT_EQ(OB_SUCCESS, scanner->get_gap_end(range_idx, gap_rowkey, gap_size));
  STORAGE_LOG(INFO, "get gap end info", K(range_idx), K(*gap_rowkey), K(gap_size));
  ASSERT_EQ(0, range_idx);
  ASSERT_TRUE(NULL != gap_rowkey);
  ASSERT_EQ(check_rowkey, *gap_rowkey);
  ASSERT_TRUE(gap_size > 0);
  scanner->~ObStoreRowIterator();
}
*/
}  // namespace unittest
}  // namespace oceanbase

int main(int argc, char** argv)
{
  GCONF._enable_sparse_row = true;
  system("rm -rf test_multi_version_sparse_sstable_single_scan.log");
  OB_LOGGER.set_file_name("test_multi_version_sparse_sstable_single_scan.log");
  STORAGE_LOG(INFO, "begin unittest: test_multi_version_sparse_sstable_single_scan");
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
