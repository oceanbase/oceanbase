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

class TestMultiVersionSSTableSingleGet : public ObMultiVersionSSTableTest {
public:
  TestMultiVersionSSTableSingleGet() : ObMultiVersionSSTableTest("testmultiversionsingeget")
  {}
  virtual ~TestMultiVersionSSTableSingleGet()
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

void TestMultiVersionSSTableSingleGet::prepare_query_param(
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

TEST_F(TestMultiVersionSSTableSingleGet, exist)
{
  const int64_t rowkey_cnt = 4;
  const char* micro_data[3];
  micro_data[0] = "bigint   var   bigint bigint  bigint   flag    multi_version_row_flag\n"
                  "1        var1   -8      0     NOP       EXIST   C\n"
                  "1        var1   -2      0      2        EXIST   L\n"
                  "2        var2   -7      0      4        DELETE  CL\n"
                  "3        var3  -28      0      7        EXIST   C\n"
                  "3        var3  -25      0     NOP       EXIST   N\n"
                  "3        var3  -23      0      7        EXIST   N\n";

  micro_data[1] = "bigint   var   bigint bigint bigint  flag    multi_version_row_flag\n"
                  "3        var3  -18      0     8       EXIST   C\n"
                  "3        var3  -15      0     11      EXIST   N\n"
                  "3        var3  -13      0     9       EXIST   N\n";

  micro_data[2] = "bigint   var   bigint bigint  bigint  flag    multi_version_row_flag\n"
                  "3        var3  -8       0     8       EXIST   N\n"
                  "3        var3  -5       0     10      EXIST   N\n"
                  "3        var3  -3       0     9       EXIST   L\n";

  prepare_data_start(micro_data, rowkey_cnt, 28, "none", FLAT_ROW_STORE, 0);
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

TEST_F(TestMultiVersionSSTableSingleGet, normal)
{
  const int64_t rowkey_cnt = 4;
  const char* micro_data = "bigint   var   bigint bigint  bigint   flag    multi_version_row_flag\n"
                           "1        var1  -8     0       NOP      EXIST   C\n"
                           "1        var1  -2     0       2        EXIST   L\n"
                           "2        var2  -7     0       4        DELETE  CL\n"
                           "3        var3  -8     0       7        EXIST   C\n"
                           "3        var3  -5     0       NOP      EXIST   N\n"
                           "3        var3  -3     0       7        EXIST   L\n";
  prepare_data(&micro_data, 1, rowkey_cnt, 8);

  ObMockIterator res_iter;
  ObStoreRowIterator* getter = NULL;
  ObStoreRow* row = NULL;
  ObExtStoreRowkey ext_rowkey;

  const char* result1 = "bigint   var   bigint   flag    multi_version_row_flag\n"
                        "1        var1  NOP      EXIST   N\n";
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
  STORAGE_LOG(INFO, "rowkey", K(ext_rowkey), K(*row));
  const ObObj* obj_ptr = ext_rowkey.get_store_rowkey().get_rowkey().get_obj_ptr();
  STORAGE_LOG(INFO, "rowkey", K(obj_ptr[0].get_meta().get_type()), K(obj_ptr[1].get_meta().get_type()));
  OK(sstable_.get(param_, context_, ext_rowkey, getter));
  OK(res_iter.from(result1));
  ASSERT_TRUE(res_iter.equals(*getter));
  getter->~ObStoreRowIterator();

  const char* result2 = "bigint   var   bigint   flag    multi_version_row_flag\n"
                        "3        var3  7        EXIST   N\n";
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

TEST_F(TestMultiVersionSSTableSingleGet, span_micro_blocks)
{
  const int64_t rowkey_cnt = 4;
  const char* micro_data[2];
  micro_data[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "3        var3  -18      0       7        8       EXIST   C\n"
                  "3        var3  -15      0       NOP      11      EXIST   N\n"
                  "3        var3  -13      0       NOP      9       EXIST   N\n";
  micro_data[1] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "3        var3  -8       0       7        NOP     EXIST   N\n"
                  "3        var3  -5       0       NOP      10      EXIST   N\n"
                  "3        var3  -3       0       7        9       EXIST   L\n";
  prepare_data(micro_data, 2, rowkey_cnt, 18);

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

TEST_F(TestMultiVersionSSTableSingleGet, span_macro_blocks)
{
  const int64_t rowkey_cnt = 4;
  const char* micro_data[2];
  micro_data[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "3        var3  -18    0         7        8       EXIST   C\n"
                  "3        var3  -15    0         NOP      11      EXIST   N\n"
                  "3        var3  -13    0         NOP      9       EXIST   N\n";
  micro_data[1] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "3        var3  -8      0        7        NOP     EXIST   N\n"
                  "3        var3  -5      0        NOP      10      EXIST   N\n"
                  "3        var3  -3      0        7        9       EXIST   L\n";
  prepare_data_start(micro_data, rowkey_cnt, 18, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(micro_data, 1);
  prepare_one_macro(&micro_data[1], 1);
  prepare_data_end();

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

  // read newest snapshot_version
  const char* result2 = "bigint   var   bigint   bigint   flag    multi_version_row_flag\n"
                        "3        var3  7        8        EXIST   C\n";
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
}

TEST_F(TestMultiVersionSSTableSingleGet, skip_version)
{
  const int64_t rowkey_cnt = 4;
  const char* micro_data[2];
  micro_data[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "3        var3  -18     0        NOP      8       EXIST   C\n"
                  "3        var3  -15     0        NOP      11      EXIST   N\n"
                  "3        var3  -13     0        NOP      9       EXIST   N\n";
  micro_data[1] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "3        var3  -8      0        NOP      8       EXIST   N\n"
                  "3        var3  -5      0        6        10      EXIST   N\n"
                  "3        var3  -3      0        5        9       EXIST   L\n";
  prepare_data_start(micro_data, rowkey_cnt, 18, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(micro_data, 1);
  prepare_one_macro(&micro_data[1], 1);
  prepare_data_end();

  ObMockIterator res_iter;
  ObStoreRowIterator* getter = NULL;
  ObStoreRow* row = NULL;
  ObExtStoreRowkey ext_rowkey;

  const char* result1 = "bigint   var   bigint   bigint   flag    multi_version_row_flag\n"
                        "3        var3  NOP      11       EXIST   N\n";
  ObVersionRange version_range;
  version_range.snapshot_version_ = 16;
  version_range.base_version_ = 8;
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

TEST_F(TestMultiVersionSSTableSingleGet, empty_get)
{
  const int64_t rowkey_cnt = 4;
  const char* micro_data[2];
  micro_data[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "3        var3  -18     0        NOP      8       EXIST   C\n"
                  "3        var3  -15     0        NOP      11      EXIST   N\n"
                  "3        var3  -13     0        NOP      9       EXIST   N\n";
  micro_data[1] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "3        var3  -8      0        NOP      8       EXIST   N\n"
                  "3        var3  -5      0        6        10      EXIST   N\n"
                  "3        var3  -3      0        5        9       EXIST   L\n";
  prepare_data_start(micro_data, rowkey_cnt, 18, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(micro_data, 2);
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
}

// TEST_F(TestMultiVersionSSTableSingleGet, test_bloom_filter)
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
  system("rm -rf test_multi_version_sstable_single_get.log");
  OB_LOGGER.set_file_name("test_multi_version_sstable_single_get.log");
  STORAGE_LOG(INFO, "begin unittest: test_multi_version_sstable_single_get");
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
