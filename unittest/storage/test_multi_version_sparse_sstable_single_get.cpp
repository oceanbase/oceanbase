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

class TestMultiVersionSparseSSTableSingleGet : public ObMultiVersionSSTableTest {
public:
  TestMultiVersionSparseSSTableSingleGet() : ObMultiVersionSSTableTest("testmultiversionsparsesingeget")
  {}
  virtual ~TestMultiVersionSparseSSTableSingleGet()
  {}

  virtual void SetUp()
  {
    ObMultiVersionSSTableTest::SetUp();
    ObPartitionKey pkey(combine_id(TENANT_ID, TABLE_ID), 0, 0);
    ASSERT_EQ(OB_SUCCESS, ObPartitionService::get_instance().get_pg_index().init());
    ASSERT_EQ(OB_SUCCESS, ObPartitionService::get_instance().get_pg_index().add_partition(pkey, pkey));
    ObPartitionKey& part = const_cast<ObPartitionKey&>(ctx_mgr_.get_partition());
    part = pkey;
    trans_service_.part_trans_ctx_mgr_.mgr_cache_.set(pkey.hash(), &ctx_mgr_);
    ObPartitionService::get_instance().txs_ = &trans_service_;
  }
  virtual void TearDown()
  {
    columns_.reset();
    param_.reset();
    context_.reset();
    allocator_.reuse();
    ObMultiVersionSSTableTest::TearDown();
    projector_.reuse();
    store_ctx_.reset();
    ObPartitionService::get_instance().get_pg_index().destroy();
  }

  void prepare_query_param(const ObVersionRange& version_range, const bool is_reverse_scan = false);

  ObArray<ObColDesc> columns_;
  ObTableIterParam param_;
  ObTableAccessContext context_;
  ObArenaAllocator allocator_;
  ObArray<int32_t> projector_;
  ObStoreCtx store_ctx_;
  transaction::ObTransService trans_service_;
  transaction::ObPartitionTransCtxMgr ctx_mgr_;
};

void TestMultiVersionSparseSSTableSingleGet::prepare_query_param(
    const ObVersionRange& version_range, const bool is_reverse_scan)
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
  param_.projector_ = &projector_;
  param_.schema_version_ = SCHEMA_VERSION;
  param_.rowkey_cnt_ = schema_rowkey_cnt;  // schema rowkey count
  param_.out_cols_ = &columns_;

  OK(block_cache_ws_.init(TENANT_ID));
  context_.query_flag_ = query_flag;
  context_.store_ctx_ = &store_ctx_;
  context_.allocator_ = &allocator_;
  context_.stmt_allocator_ = &allocator_;
  context_.block_cache_ws_ = &block_cache_ws_;
  context_.trans_version_range_ = version_range;
  context_.is_inited_ = true;
}

TEST_F(TestMultiVersionSparseSSTableSingleGet, test_checksum)
{
  int64_t column_num = 10;
  ObStoreRow reader_row;
  oceanbase::common::ObObj objs[column_num];
  reader_row.row_val_.cells_ = objs;
  reader_row.row_val_.count_ = column_num;
  reader_row.capacity_ = column_num;
  reader_row.row_val_.cells_[0].set_int(8);
  reader_row.row_val_.cells_[1].set_char("c");
  int64_t new_checksum1 = 0;
  new_checksum1 = ObIMicroBlockWriter::cal_row_checksum(reader_row, new_checksum1);
  STORAGE_LOG(WARN, "test1", K(new_checksum1), K(reader_row));
  new_checksum1 = 0;
  reader_row.row_val_.cells_[0].set_tinyint(8);
  reader_row.row_val_.cells_[1].set_varchar("c");
  new_checksum1 = ObIMicroBlockWriter::cal_row_checksum(reader_row, new_checksum1);
  STORAGE_LOG(WARN, "test2", K(new_checksum1), K(reader_row));
}

TEST_F(TestMultiVersionSparseSSTableSingleGet, exist)
{
  const int64_t rowkey_cnt = 4;
  const char* micro_data[3];
  micro_data[0] = "bigint   var   bigint bigint  bigint   flag    multi_version_row_flag\n"
                  "1        var1  -8       0    NOP      EXIST   C\n"
                  "1        var1  -2       0    2        EXIST   L\n"
                  "2        var2  -7       0    4        DELETE  CL\n"
                  "3        var3  -28      0    7        EXIST   C\n"
                  "3        var3  -25      0    NOP      EXIST   N\n"
                  "3        var3  -23      0    7        EXIST   N\n";

  micro_data[1] = "bigint   var   bigint bigint  bigint  flag    multi_version_row_flag\n"
                  "3        var3  -18      0     8       EXIST   C\n"
                  "3        var3  -15      0     11      EXIST   N\n"
                  "3        var3  -13      0     9       EXIST   N\n";

  micro_data[2] = "bigint   var   bigint bigint  bigint  flag    multi_version_row_flag\n"
                  "3        var3  -8       0     8       EXIST   N\n"
                  "3        var3  -5       0     10      EXIST   N\n"
                  "3        var3  -3       0     9       EXIST   L\n";

  prepare_data_start(micro_data, rowkey_cnt, 28);
  prepare_one_macro(micro_data, 1);
  prepare_one_macro(&micro_data[1], 1);
  prepare_one_macro(&micro_data[2], 1);
  prepare_data_end();

  const char* rowkeys = "bigint   var   flag\n"
                        "1        var2  EXIST\n"
                        "1        var1  EXIST\n"
                        "2        var2  EXIST\n"
                        "3        var3  EXIST\n"
                        "4        var1  EXIST\n";
  ObMockIterator rowkey_iter;
  OK(rowkey_iter.from(rowkeys));

  ObStoreRow* row = NULL;
  ObStoreRowkey rowkey;
  ObStoreCtx store_ctx;
  bool is_exist = false;
  bool is_found = false;

  ObVersionRange version_range;
  version_range.snapshot_version_ = 8;
  version_range.base_version_ = 0;
  version_range.multi_version_start_ = 0;
  prepare_query_param(version_range);

  OK(rowkey_iter.get_row(0, row));
  ASSERT_TRUE(NULL != row);
  rowkey.assign(row->row_val_.cells_, rowkey_cnt - 2);
  is_exist = true;
  is_found = true;
  OK(sstable_.exist(store_ctx, combine_id(TENANT_ID, TABLE_ID), rowkey, *param_.out_cols_, is_exist, is_found));
  ASSERT_EQ(false, is_exist);
  ASSERT_EQ(false, is_found);

  OK(rowkey_iter.get_row(1, row));
  ASSERT_TRUE(NULL != row);
  rowkey.assign(row->row_val_.cells_, rowkey_cnt - 2);
  is_exist = false;
  is_found = false;
  OK(sstable_.exist(store_ctx, combine_id(TENANT_ID, TABLE_ID), rowkey, *param_.out_cols_, is_exist, is_found));
  ASSERT_EQ(true, is_exist);
  ASSERT_EQ(true, is_found);

  OK(rowkey_iter.get_row(2, row));
  ASSERT_TRUE(NULL != row);
  rowkey.assign(row->row_val_.cells_, rowkey_cnt - 2);
  is_exist = false;
  is_found = true;
  OK(sstable_.exist(store_ctx, combine_id(TENANT_ID, TABLE_ID), rowkey, *param_.out_cols_, is_exist, is_found));
  ASSERT_EQ(false, is_exist);
  ASSERT_EQ(true, is_found);

  OK(rowkey_iter.get_row(3, row));
  ASSERT_TRUE(NULL != row);
  rowkey.assign(row->row_val_.cells_, rowkey_cnt - 2);
  is_exist = false;
  is_found = false;
  OK(sstable_.exist(store_ctx, combine_id(TENANT_ID, TABLE_ID), rowkey, *param_.out_cols_, is_exist, is_found));
  ASSERT_EQ(true, is_exist);
  ASSERT_EQ(true, is_found);

  OK(rowkey_iter.get_row(4, row));
  ASSERT_TRUE(NULL != row);
  rowkey.assign(row->row_val_.cells_, rowkey_cnt - 2);
  is_exist = true;
  is_found = true;
  OK(sstable_.exist(store_ctx, combine_id(TENANT_ID, TABLE_ID), rowkey, *param_.out_cols_, is_exist, is_found));
  ASSERT_EQ(false, is_exist);
  ASSERT_EQ(false, is_found);

  // read newest snapshot_version
  version_range.snapshot_version_ = 30;
  prepare_query_param(version_range);

  OK(rowkey_iter.get_row(0, row));
  ASSERT_TRUE(NULL != row);
  rowkey.assign(row->row_val_.cells_, rowkey_cnt - 2);
  is_exist = true;
  is_found = true;
  OK(sstable_.exist(store_ctx, combine_id(TENANT_ID, TABLE_ID), rowkey, *param_.out_cols_, is_exist, is_found));
  ASSERT_EQ(false, is_exist);
  ASSERT_EQ(false, is_found);

  OK(rowkey_iter.get_row(1, row));
  ASSERT_TRUE(NULL != row);
  rowkey.assign(row->row_val_.cells_, rowkey_cnt - 2);
  is_exist = true;
  is_found = true;
  OK(sstable_.exist(store_ctx, combine_id(TENANT_ID, TABLE_ID), rowkey, *param_.out_cols_, is_exist, is_found));
  ASSERT_EQ(true, is_exist);
  ASSERT_EQ(true, is_found);

  OK(rowkey_iter.get_row(2, row));
  ASSERT_TRUE(NULL != row);
  rowkey.assign(row->row_val_.cells_, rowkey_cnt - 2);
  is_exist = true;
  is_found = true;
  OK(sstable_.exist(store_ctx, combine_id(TENANT_ID, TABLE_ID), rowkey, *param_.out_cols_, is_exist, is_found));
  ASSERT_EQ(false, is_exist);
  ASSERT_EQ(true, is_found);

  OK(rowkey_iter.get_row(3, row));
  ASSERT_TRUE(NULL != row);
  rowkey.assign(row->row_val_.cells_, rowkey_cnt - 2);
  is_exist = true;
  is_found = true;
  OK(sstable_.exist(store_ctx, combine_id(TENANT_ID, TABLE_ID), rowkey, *param_.out_cols_, is_exist, is_found));
  ASSERT_EQ(true, is_exist);
  ASSERT_EQ(true, is_found);

  OK(rowkey_iter.get_row(4, row));
  ASSERT_TRUE(NULL != row);
  rowkey.assign(row->row_val_.cells_, rowkey_cnt - 2);
  is_exist = true;
  is_found = true;
  OK(sstable_.exist(store_ctx, combine_id(TENANT_ID, TABLE_ID), rowkey, *param_.out_cols_, is_exist, is_found));
  ASSERT_EQ(false, is_exist);
  ASSERT_EQ(false, is_found);
}

TEST_F(TestMultiVersionSparseSSTableSingleGet, normal)
{
  GCONF._enable_sparse_row = true;
  const int64_t rowkey_cnt = 4;
  const char* micro_data = "bigint   var   bigint bigint  bigint   flag    multi_version_row_flag\n"
                           "1        var1  -8       0      NOP      EXIST   C\n"
                           "1        var1  -2       0      2        EXIST   L\n"
                           "2        var2  -7       0      4        DELETE  CL\n"
                           "3        var3  -8       0      7        EXIST   C\n"
                           "3        var3  -5       0      NOP      EXIST   N\n"
                           "3        var3  -3       0      7        EXIST   L\n";
  const char* input_column = "int int int int int\n"
                             "16  17  7  8   20 \n"
                             "16  17  7  8   20 \n"
                             "16  17  7  8   20 \n"
                             "16  17  7  8   20 \n"
                             "16  17  7  8   20 \n"
                             "16  17  7  8   20 \n";

  const int64_t col_cnt[] = {5, 5, 5, 5, 5, 5};
  prepare_data(&micro_data, 1, rowkey_cnt, 8, "none", SPARSE_ROW_STORE, 0, input_column, col_cnt);
  // prepare_data(&micro_data, 1, rowkey_cnt, 8);

  ObMockIterator res_iter;
  ObStoreRowIterator* getter = NULL;
  ObStoreRow* row = NULL;
  ObExtStoreRowkey ext_rowkey;

  const char* result1 = "bigint   var   bigint   flag    multi_version_row_flag is_get\n"
                        "1        var1  NOP      EXIST   N                      TRUE\n";

  ObVersionRange version_range;
  version_range.snapshot_version_ = 8;
  version_range.base_version_ = 0;
  version_range.multi_version_start_ = 0;
  prepare_query_param(version_range);

  OK(data_iter_[0].get_row(1, row));
  STORAGE_LOG(WARN, "test", K(*row));
  ext_rowkey.reset();
  res_iter.reset();
  OK(data_iter_[0].get_row(0, row));
  ASSERT_TRUE(NULL != row);
  ObSSTableTest::convert_rowkey(ObStoreRowkey(row->row_val_.cells_, rowkey_cnt - 2), ext_rowkey, allocator_);
  OK(sstable_.get(param_, context_, ext_rowkey, getter));
  OK(res_iter.from(result1));
  ASSERT_TRUE(res_iter.equals(*getter));
  getter->~ObStoreRowIterator();

  const char* result2 = "bigint   var   bigint   flag    multi_version_row_flag   is_get\n"
                        "3        var3  7        EXIST   N                        TRUE\n";
  version_range.snapshot_version_ = 5;
  prepare_query_param(version_range);

  ext_rowkey.reset();
  res_iter.reset();
  OK(data_iter_[0].get_row(3, row));
  ASSERT_TRUE(NULL != row);
  ObSSTableTest::convert_rowkey(ObStoreRowkey(row->row_val_.cells_, rowkey_cnt - 2), ext_rowkey, allocator_);
  OK(sstable_.get(param_, context_, ext_rowkey, getter));
  OK(res_iter.from(result2));
  ASSERT_TRUE(res_iter.equals(*getter));
  getter->~ObStoreRowIterator();
}

TEST_F(TestMultiVersionSparseSSTableSingleGet, span_micro_blocks)
{
  const int64_t rowkey_cnt = 4;
  const char* micro_data[2];
  micro_data[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "3        var3  -18      0      7        8       EXIST   C\n"
                  "3        var3  -15      0      NOP      11      EXIST   N\n"
                  "3        var3  -13      0      NOP      9       EXIST   N\n";
  micro_data[1] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "3        var3  -8       0      7        NOP     EXIST   N\n"
                  "3        var3  -5       0      NOP      10      EXIST   N\n"
                  "3        var3  -3       0      7        9       EXIST   L\n";
  // prepare_data(micro_data, 2, rowkey_cnt, 18);
  const char* input_column = "int int int int int int\n"
                             "16  17  7   8  20 21\n"
                             "16  17  7   8  20 21\n"
                             "16  17  7   8  20 21\n"
                             "16  17  7   8  20 21\n"
                             "16  17  7   8  20 21\n"
                             "16  17  7   8  20 21\n";

  const int64_t col_cnt[] = {6, 6, 6, 6, 6, 6};
  prepare_data(micro_data, 2, rowkey_cnt, 18, "none", SPARSE_ROW_STORE, 0, input_column, col_cnt);
  ObMockIterator res_iter;
  ObStoreRowIterator* getter = NULL;
  ObStoreRow* row = NULL;
  ObExtStoreRowkey ext_rowkey;

  const char* result1 = "bigint   var   bigint   bigint   flag    multi_version_row_flag\n"
                        "3        var3  7        11       EXIST   N\n";
  ObVersionRange version_range;
  version_range.snapshot_version_ = 16;
  version_range.base_version_ = 0;
  version_range.multi_version_start_ = 0;
  prepare_query_param(version_range);

  ext_rowkey.reset();
  res_iter.reset();
  OK(data_iter_[0].get_row(0, row));
  ASSERT_TRUE(NULL != row);
  ObSSTableTest::convert_rowkey(ObStoreRowkey(row->row_val_.cells_, rowkey_cnt - 2), ext_rowkey, allocator_);
  OK(sstable_.get(param_, context_, ext_rowkey, getter));
  OK(res_iter.from(result1));
  ASSERT_TRUE(res_iter.equals(*getter));
  getter->~ObStoreRowIterator();
}

TEST_F(TestMultiVersionSparseSSTableSingleGet, span_macro_blocks)
{
  const int64_t rowkey_cnt = 4;
  const char* micro_data[2];
  micro_data[0] = "bigint   var   bigint bigint  bigint   bigint    flag    multi_version_row_flag\n"
                  "3        var3  -18      0      7        8        EXIST   C\n"
                  "3        var3  -15      0      12       NOP      EXIST   N\n"
                  "3        var3  -13      0      9        NOP      EXIST   N\n";
  micro_data[1] = "bigint   var   bigint bigint  bigint   bigint   flag    multi_version_row_flag\n"
                  "3        var3  -8       0      7        NOP     EXIST   N\n"
                  "3        var3  -5       0      2        10      EXIST   N\n"
                  "3        var3  -3       0      7        1       EXIST   L\n";

  const char* input_column = "int int int int int int\n"
                             "16  17  7   8  20 21\n"
                             "16  17  7   8  20  NOP\n"
                             "16  17  7   8  21  NOP\n"
                             "16  17  7   8  20  NOP\n"
                             "16  17  7   8  20 21\n"
                             "16  17  7   8  20 21\n";

  const int64_t col_cnt[] = {6, 5, 5, 5, 6, 6};
  prepare_data(micro_data, 2, rowkey_cnt, 18, "none", SPARSE_ROW_STORE, 0, input_column, col_cnt);
  ObMockIterator res_iter;
  ObStoreRowIterator* getter = NULL;
  ObStoreRow* row = NULL;
  ObExtStoreRowkey ext_rowkey;

  const char* result1 = "bigint   var   bigint   bigint   flag    multi_version_row_flag\n"
                        "3        var3  12        9       EXIST   N\n";
  ObVersionRange version_range;
  version_range.snapshot_version_ = 16;
  version_range.base_version_ = 0;
  version_range.multi_version_start_ = 0;
  prepare_query_param(version_range);

  ext_rowkey.reset();
  res_iter.reset();
  OK(data_iter_[0].get_row(0, row));
  ASSERT_TRUE(NULL != row);
  ObSSTableTest::convert_rowkey(ObStoreRowkey(row->row_val_.cells_, rowkey_cnt - 2), ext_rowkey, allocator_);
  OK(sstable_.get(param_, context_, ext_rowkey, getter));
  OK(res_iter.from(result1));
  ASSERT_TRUE(res_iter.equals(*getter));
  getter->~ObStoreRowIterator();

  // read newest snapshot_version
  const char* result2 = "bigint   var   bigint   bigint   flag    multi_version_row_flag   is_get\n"
                        "3        var3  7        8        EXIST   C                         TRUE\n";
  version_range.snapshot_version_ = 20;
  prepare_query_param(version_range);

  ext_rowkey.reset();
  res_iter.reset();
  OK(data_iter_[0].get_row(0, row));
  ASSERT_TRUE(NULL != row);
  ObSSTableTest::convert_rowkey(ObStoreRowkey(row->row_val_.cells_, rowkey_cnt - 2), ext_rowkey, allocator_);
  OK(sstable_.get(param_, context_, ext_rowkey, getter));
  OK(res_iter.from(result2));
  ASSERT_TRUE(res_iter.equals(*getter));
  getter->~ObStoreRowIterator();

  // no need put row cache
  context_.query_flag_.use_row_cache_ = 1;

  ext_rowkey.reset();
  res_iter.reset();
  OK(data_iter_[0].get_row(0, row));
  ASSERT_TRUE(NULL != row);
  ObSSTableTest::convert_rowkey(ObStoreRowkey(row->row_val_.cells_, rowkey_cnt - 2), ext_rowkey, allocator_);
  OK(sstable_.get(param_, context_, ext_rowkey, getter));
  OK(res_iter.from(result2));
  ASSERT_TRUE(res_iter.equals(*getter));
  getter->~ObStoreRowIterator();

  const char* result3 = "bigint   var   bigint   bigint   flag    multi_version_row_flag\n"
                        "3        var3  7        9       EXIST   N\n";
  version_range.snapshot_version_ = 14;
  version_range.base_version_ = 0;
  version_range.multi_version_start_ = 0;
  prepare_query_param(version_range);

  ext_rowkey.reset();
  res_iter.reset();
  OK(data_iter_[0].get_row(0, row));
  ASSERT_TRUE(NULL != row);
  ObSSTableTest::convert_rowkey(ObStoreRowkey(row->row_val_.cells_, rowkey_cnt - 2), ext_rowkey, allocator_);
  OK(sstable_.get(param_, context_, ext_rowkey, getter));
  OK(res_iter.from(result3));
  ASSERT_TRUE(res_iter.equals(*getter));
  getter->~ObStoreRowIterator();

  const char* result4 = "bigint   var   bigint   bigint   flag    multi_version_row_flag\n"
                        "3        var3  7        10       EXIST   N\n";
  version_range.snapshot_version_ = 9;
  version_range.base_version_ = 4;
  version_range.multi_version_start_ = 0;
  prepare_query_param(version_range);

  ext_rowkey.reset();
  res_iter.reset();
  OK(data_iter_[0].get_row(0, row));
  ASSERT_TRUE(NULL != row);
  ObSSTableTest::convert_rowkey(ObStoreRowkey(row->row_val_.cells_, rowkey_cnt - 2), ext_rowkey, allocator_);
  OK(sstable_.get(param_, context_, ext_rowkey, getter));
  OK(res_iter.from(result4));
  ASSERT_TRUE(res_iter.equals(*getter));
  getter->~ObStoreRowIterator();
}

TEST_F(TestMultiVersionSparseSSTableSingleGet, skip_version)
{
  const int64_t rowkey_cnt = 4;
  const char* micro_data[2];
  micro_data[0] = "bigint   var   bigint bigint  bigint   bigint  bigint  flag    multi_version_row_flag\n"
                  "3        var3  -18      0     8        98      142      EXIST   C\n"
                  "3        var3  -15      0     11       NOP     NOP      EXIST   N\n"
                  "3        var3  -13      0     9        NOP     NOP      EXIST   N\n";
  micro_data[1] = "bigint   var   bigint bigint  bigint   bigint  bigint   flag    multi_version_row_flag\n"
                  "3        var3  -8       0     7        173     NOP     EXIST   N\n"
                  "3        var3  -5       0     6         10      11       EXIST   N\n"
                  "3        var3  -3       0     5         4        2        EXIST   L\n";

  const char* input_column = "int int int int int int int\n"
                             "16  17  7   8  20  21  22\n"
                             "16  17  7   8  21  NOP NOP\n"
                             "16  17  7   8  22  NOP NOP\n"
                             "16  17  7   8  20  22 NOP\n"
                             "16  17  7   8  21  22  20\n"
                             "16  17  7   8  22  21  20\n";

  const int64_t col_cnt[] = {7, 5, 5, 6, 7, 7};
  prepare_data(micro_data, 2, rowkey_cnt, 18, "none", SPARSE_ROW_STORE, 0, input_column, col_cnt);

  ObMockIterator res_iter;
  ObStoreRowIterator* getter = NULL;
  ObStoreRow* row = NULL;
  ObExtStoreRowkey ext_rowkey;

  const char* result1 = "bigint   var   bigint   bigint  bigint flag    multi_version_row_flag\n"
                        "3        var3    7         11      9   EXIST   N\n";
  ObVersionRange version_range;
  version_range.snapshot_version_ = 16;
  version_range.base_version_ = 7;
  version_range.multi_version_start_ = 0;
  prepare_query_param(version_range);

  ext_rowkey.reset();
  res_iter.reset();
  OK(data_iter_[0].get_row(0, row));
  ASSERT_TRUE(NULL != row);
  ObSSTableTest::convert_rowkey(ObStoreRowkey(row->row_val_.cells_, rowkey_cnt - 2), ext_rowkey, allocator_);
  OK(sstable_.get(param_, context_, ext_rowkey, getter));
  OK(res_iter.from(result1));
  ASSERT_TRUE(res_iter.equals(*getter));
  getter->~ObStoreRowIterator();

  const char* result2 = "bigint   var   bigint   bigint  bigint flag    multi_version_row_flag is_get\n"
                        "3        var3    8         98    142   EXIST   N                       TRUE\n";
  version_range.snapshot_version_ = 20;
  version_range.base_version_ = 14;
  version_range.multi_version_start_ = 0;
  prepare_query_param(version_range);

  ext_rowkey.reset();
  res_iter.reset();
  OK(data_iter_[0].get_row(0, row));
  ASSERT_TRUE(NULL != row);
  ObSSTableTest::convert_rowkey(ObStoreRowkey(row->row_val_.cells_, rowkey_cnt - 2), ext_rowkey, allocator_);
  OK(sstable_.get(param_, context_, ext_rowkey, getter));
  OK(res_iter.from(result2));
  ASSERT_TRUE(res_iter.equals(*getter));
  getter->~ObStoreRowIterator();

  const char* result3 = "bigint   var   bigint   bigint  bigint flag    multi_version_row_flag\n"
                        "3        var3    7         6      173   EXIST   N\n";
  version_range.snapshot_version_ = 9;
  version_range.base_version_ = 4;
  version_range.multi_version_start_ = 0;
  prepare_query_param(version_range);

  ext_rowkey.reset();
  res_iter.reset();
  OK(data_iter_[0].get_row(0, row));
  ASSERT_TRUE(NULL != row);
  ObSSTableTest::convert_rowkey(ObStoreRowkey(row->row_val_.cells_, rowkey_cnt - 2), ext_rowkey, allocator_);
  OK(sstable_.get(param_, context_, ext_rowkey, getter));
  OK(res_iter.from(result3));
  ASSERT_TRUE(res_iter.equals(*getter));
  getter->~ObStoreRowIterator();

  const char* result4 = "bigint   var   bigint   bigint  bigint flag    multi_version_row_flag\n"
                        "3        var3    NOP       11     9   EXIST   N\n";
  version_range.snapshot_version_ = 16;
  version_range.base_version_ = 12;
  version_range.multi_version_start_ = 0;
  prepare_query_param(version_range);

  ext_rowkey.reset();
  res_iter.reset();
  OK(data_iter_[0].get_row(0, row));
  ASSERT_TRUE(NULL != row);
  ObSSTableTest::convert_rowkey(ObStoreRowkey(row->row_val_.cells_, rowkey_cnt - 2), ext_rowkey, allocator_);
  OK(sstable_.get(param_, context_, ext_rowkey, getter));
  OK(res_iter.from(result4));
  ASSERT_TRUE(res_iter.equals(*getter));
  getter->~ObStoreRowIterator();

  const char* result5 = "bigint   var   bigint   bigint  bigint flag    multi_version_row_flag\n"
                        "3        var3    11       6       10   EXIST   N\n";
  version_range.snapshot_version_ = 5;
  version_range.base_version_ = 4;
  version_range.multi_version_start_ = 0;
  prepare_query_param(version_range);

  ext_rowkey.reset();
  res_iter.reset();
  OK(data_iter_[0].get_row(0, row));
  ASSERT_TRUE(NULL != row);
  ObSSTableTest::convert_rowkey(ObStoreRowkey(row->row_val_.cells_, rowkey_cnt - 2), ext_rowkey, allocator_);
  OK(sstable_.get(param_, context_, ext_rowkey, getter));
  OK(res_iter.from(result5));
  ASSERT_TRUE(res_iter.equals(*getter));
  getter->~ObStoreRowIterator();
}

TEST_F(TestMultiVersionSparseSSTableSingleGet, empty_get)
{
  const int64_t rowkey_cnt = 4;
  const char* micro_data[2];
  micro_data[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "3        var3  -18      0      8        NOP     EXIST   C\n"
                  "3        var3  -15      0      11       NOP     EXIST   N\n"
                  "3        var3  -13      0      9        NOP     EXIST   N\n";
  micro_data[1] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "3        var3  -8       0      8        NOP     EXIST   N\n"
                  "3        var3  -5       0      6        10      EXIST   N\n"
                  "3        var3  -3       0      5        9       EXIST   L\n";
  const char* input_column1 = "int int int int int\n"
                              "16  17  7  8  21\n"
                              "16  17  7  8  21\n"
                              "16  17  7  8  21\n";
  const char* input_column2 = "int int int int int int\n"
                              "16  17  7  8  20\n"
                              "16  17  7  8  20 21\n"
                              "16  17  7  8  20 21\n";

  const int64_t col_cnt1[] = {5, 5, 5};
  const int64_t col_cnt2[] = {5, 6, 6};
  const int64_t snapshot_version = 18;
  prepare_data_start(micro_data, rowkey_cnt, snapshot_version, "none", SPARSE_ROW_STORE);
  prepare_one_macro(micro_data, 1, snapshot_version, input_column1, col_cnt1);
  prepare_one_macro(&micro_data[1], 1, snapshot_version, input_column2, col_cnt2);
  prepare_data_end();

  // ObMockIterator res_iter;
  ObMockIterator rowkey_iter;
  ObStoreRowIterator* getter = NULL;
  const ObStoreRow* row = NULL;
  ObExtStoreRowkey ext_rowkey;

  // not exist rowkey
  const char* not_exist_rowkey1 = "bigint   var   flag    multi_version_row_flag\n"
                                  "3        var2  EXIST   N\n";
  ObVersionRange version_range;
  version_range.snapshot_version_ = 16;
  version_range.base_version_ = 8;
  version_range.multi_version_start_ = 0;
  prepare_query_param(version_range);

  ext_rowkey.reset();
  rowkey_iter.reset();
  OK(rowkey_iter.from(not_exist_rowkey1));
  OK(rowkey_iter.get_row(0, row));
  ASSERT_TRUE(NULL != row);
  ObSSTableTest::convert_rowkey(ObStoreRowkey(row->row_val_.cells_, rowkey_cnt - 2), ext_rowkey, allocator_);
  OK(sstable_.get(param_, context_, ext_rowkey, getter));
  OK(getter->get_next_row(row));
  ASSERT_TRUE(ObActionFlag::OP_ROW_DOES_NOT_EXIST == row->flag_);
  ASSERT_EQ(OB_ITER_END, getter->get_next_row(row));
  getter->~ObStoreRowIterator();

  // exist rowkey, but not exist mluti version row
  const char* not_exist_rowkey2 = "bigint   var   flag    multi_version_row_flag\n"
                                  "3        var3  EXIST   N\n";
  version_range.snapshot_version_ = 7;
  version_range.base_version_ = 6;
  prepare_query_param(version_range);
  ext_rowkey.reset();
  rowkey_iter.reset();
  OK(rowkey_iter.from(not_exist_rowkey2));
  OK(rowkey_iter.get_row(0, row));
  ASSERT_TRUE(NULL != row);
  ObSSTableTest::convert_rowkey(ObStoreRowkey(row->row_val_.cells_, rowkey_cnt - 2), ext_rowkey, allocator_);
  OK(sstable_.get(param_, context_, ext_rowkey, getter));
  OK(getter->get_next_row(row));
  ASSERT_TRUE(ObActionFlag::OP_ROW_DOES_NOT_EXIST == row->flag_);
  ASSERT_EQ(OB_ITER_END, getter->get_next_row(row));
  getter->~ObStoreRowIterator();

  // exist rowkey, but not exist mluti version row
  const char* not_exist_rowkey3 = "bigint   var   flag    multi_version_row_flag\n"
                                  "3        var3  EXIST   N\n";
  version_range.snapshot_version_ = 2;
  version_range.base_version_ = 1;
  prepare_query_param(version_range);
  ext_rowkey.reset();
  rowkey_iter.reset();
  OK(rowkey_iter.from(not_exist_rowkey3));
  OK(rowkey_iter.get_row(0, row));
  ASSERT_TRUE(NULL != row);
  ObSSTableTest::convert_rowkey(ObStoreRowkey(row->row_val_.cells_, rowkey_cnt - 2), ext_rowkey, allocator_);
  OK(sstable_.get(param_, context_, ext_rowkey, getter));
  OK(getter->get_next_row(row));
  ASSERT_TRUE(ObActionFlag::OP_ROW_DOES_NOT_EXIST == row->flag_);
  ASSERT_EQ(OB_ITER_END, getter->get_next_row(row));
  getter->~ObStoreRowIterator();

  const char* rowkey4 = "bigint   var   flag    multi_version_row_flag\n"
                        "3        var3  EXIST   N\n";
  const char* result4 = "bigint   var   bigint   bigint  flag    multi_version_row_flag\n"
                        "3        var3    NOP       11   EXIST   N\n";
  version_range.snapshot_version_ = 16;
  version_range.base_version_ = 12;
  version_range.multi_version_start_ = 0;
  prepare_query_param(version_range);

  ext_rowkey.reset();
  ObMockIterator res_iter;
  res_iter.reset();
  rowkey_iter.reset();
  OK(rowkey_iter.from(rowkey4));
  OK(rowkey_iter.get_row(0, row));
  ASSERT_TRUE(NULL != row);
  ObSSTableTest::convert_rowkey(ObStoreRowkey(row->row_val_.cells_, rowkey_cnt - 2), ext_rowkey, allocator_);
  OK(sstable_.get(param_, context_, ext_rowkey, getter));
  OK(res_iter.from(result4));
  ASSERT_TRUE(res_iter.equals(*getter));
  getter->~ObStoreRowIterator();
}

// TEST_F(TestMultiVersionSparseSSTableSingleGet, test_bloom_filter)
//{
// const int64_t rowkey_cnt = 4;
// const char *micro_data[2];
// micro_data[0] =
//"bigint   var   bigint   bigint   bigint  flag    multi_version_row_flag\n"
//"3        var3  -18      NOP      8       EXIST   C\n"
//"3        var3  -15      NOP      11      EXIST   N\n"
//"3        var3  -13      NOP      9       EXIST   N\n";
// micro_data[1] =
//"bigint   var   bigint   bigint   bigint  flag    multi_version_row_flag\n"
//"3        var3  -8       NOP      8       EXIST   N\n"
//"3        var3  -5       6        10      EXIST   N\n"
//"3        var3  -3       5        9       EXIST   L\n";
// prepare_data_start(micro_data, rowkey_cnt, 18);
// prepare_one_macro(micro_data, 1);
// prepare_one_macro(&micro_data[1], 1);
// prepare_data_end();

// ObMockIterator rowkey_iter;
// ObStoreRowIterator *getter = NULL;
// const ObStoreRow *row = NULL;
// ObExtStoreRowkey ext_rowkey;

//// not exist rowkey
// const char *not_exist_rowkey1 =
//"bigint   var   flag    multi_version_row_flag\n"
//"3        var2  EXIST   N\n";

// ObVersionRange version_range;
// version_range.snapshot_version_ = 16;
// version_range.base_version_ = 8;
// version_range.multi_version_start_ = 0;
// prepare_query_param(version_range);
// sstable_.key_.trans_version_range_.snapshot_version_ = 10;

// ext_rowkey.reset();
// rowkey_iter.reset();
// OK(rowkey_iter.from(not_exist_rowkey1));
// OK(rowkey_iter.get_row(0, row));
// ASSERT_TRUE(NULL != row);
// ObSSTableTest::convert_rowkey(ObStoreRowkey(row->row_val_.cells_, rowkey_cnt - 2), ext_rowkey, allocator_);
// OK(sstable_.get(param_, context_, ext_rowkey, getter));
// OK(getter->get_next_row(row));
// ASSERT_TRUE(ObActionFlag::OP_ROW_DOES_NOT_EXIST == row->flag_);
// ASSERT_EQ(OB_ITER_END, getter->get_next_row(row));
// getter->~ObStoreRowIterator();

// const ObMacroBlockMeta *meta = NULL;
// get_macro_meta(1, 1, meta);
// ASSERT_EQ(1, meta->empty_read_cnt_[1]);

// sstable_.key_.trans_version_range_.snapshot_version_ = 20;
// OK(sstable_.get(param_, context_, ext_rowkey, getter));
// OK(getter->get_next_row(row));
// ASSERT_TRUE(ObActionFlag::OP_ROW_DOES_NOT_EXIST == row->flag_);
// ASSERT_EQ(OB_ITER_END, getter->get_next_row(row));
// getter->~ObStoreRowIterator();

// get_macro_meta(1, 1, meta);
// ASSERT_EQ(1, meta->empty_read_cnt_[2]);
//}

}  // namespace unittest
}  // namespace oceanbase

int main(int argc, char** argv)
{
  GCONF._enable_sparse_row = true;
  system("rm -rf test_multi_version_sparse_sstable_single_get.log");
  OB_LOGGER.set_file_name("test_multi_version_sparse_sstable_single_get.log");
  STORAGE_LOG(INFO, "begin unittest: test_multi_version_sparse_sstable_single_get");
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
