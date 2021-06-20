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

#include "utils_rowkey_builder.h"
#include "utils_mock_row.h"
#include "share/schema/ob_table_schema.h"
#include "storage/blocksstable/ob_row_generate.h"
#include <gtest/gtest.h>
#include "share/ob_srv_rpc_proxy.h"
#define private public
#define protected public
#include "storage/memtable/ob_memtable_iterator.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/compaction/ob_partition_merge_util.h"
#include "storage/ob_ms_row_iterator.h"
#include "storage/memtable/mvcc/ob_mvcc_ctx.h"
#undef private
#undef protected
#include "storage/mockcontainer/mock_ob_iterator.h"

namespace oceanbase {
using namespace common;
using namespace share::schema;
using namespace storage;
using namespace blocksstable;
using namespace unittest;
using namespace rpc::frame;
using namespace compaction;
namespace memtable {
class TestMemtableMultiVersionRowIterator : public ::testing::Test {
public:
  static const int64_t TEST_ROWKEY_COLUMN_CNT = 2;
  static const int64_t TEST_COLUMN_CNT = ObExtendType;
  TestMemtableMultiVersionRowIterator();
  virtual ~TestMemtableMultiVersionRowIterator();
  virtual void SetUp();
  virtual void TearDown();
  void create_table_schema();
  void init();
  void init_table_param(const ObTableSchema& schema, const ObColDescIArray& cols);

private:
  bool is_inited_;

protected:
  ObTableIterParam table_param_;
  ObTableAccessContext table_access_context_;
  ObArenaAllocator allocator_;
  ObArray<ObColDesc, ObIAllocator&> column_ids_;
  ObTableSchema table_schema_;
  ObRowGenerate row_generate_;
  ObMemtableCtxFactory f_;
  ObStoreRange range_;
  // ObMemtableRowUncompactedScanIterator uncompacted_row_iter_;
  ObMTKVBuilder kv_builder_;
  ObArenaAllocator local_allocator_;
  ObQueryEngine query_engine_;
  // ObMvccEngine mvcc_engine_;
};

TestMemtableMultiVersionRowIterator::TestMemtableMultiVersionRowIterator()
    : table_param_(),
      table_access_context_(),
      allocator_(ObModIds::TEST),
      column_ids_(2048, allocator_),
      table_schema_(),
      row_generate_(),
      f_(),
      kv_builder_(),
      local_allocator_(),
      query_engine_(local_allocator_)
// mvcc_engine_()
{
  is_inited_ = true;
}

TestMemtableMultiVersionRowIterator::~TestMemtableMultiVersionRowIterator()
{}

void TestMemtableMultiVersionRowIterator::SetUp()
{
  ASSERT_TRUE(is_inited_);
  allocator_.reuse();
  create_table_schema();
  init();
}

void TestMemtableMultiVersionRowIterator::init()
{
  int ret = OB_SUCCESS;
  ret = query_engine_.init(extract_tenant_id(table_schema_.get_table_id()));
  ASSERT_EQ(OB_SUCCESS, ret);
  // ret = mvcc_engine_.init(&local_allocator_, &kv_builder_, &query_engine_);
  // ASSERT_EQ(OB_SUCCESS, ret);
}

void TestMemtableMultiVersionRowIterator::init_table_param(const ObTableSchema& schema, const ObColDescIArray& cols)
{
  table_param_.reset();
  table_param_.table_id_ = schema.get_table_id();
  table_param_.rowkey_cnt_ = schema.get_rowkey_column_num();
  table_param_.schema_version_ = schema.get_schema_version();
  table_param_.out_cols_ = &cols;
  table_param_.is_multi_version_minor_merge_ = true;
}

void TestMemtableMultiVersionRowIterator::create_table_schema()
{
  const int64_t table_id = 3000000000000001L;
  // ObTableSchema table_schema;
  ObColumnSchemaV2 column;
  // init table schema
  table_schema_.reset();
  ASSERT_EQ(OB_SUCCESS, table_schema_.set_table_name("test_lob_row_writer"));
  table_schema_.set_tenant_id(1);
  table_schema_.set_tablegroup_id(1);
  table_schema_.set_database_id(1);
  table_schema_.set_table_id(table_id);
  table_schema_.set_rowkey_column_num(TEST_ROWKEY_COLUMN_CNT);
  table_schema_.set_max_used_column_id(TEST_COLUMN_CNT);
  table_schema_.set_block_size(32 * 1024);  // 32K
  table_schema_.set_schema_version(1);
  // init column
  char name[OB_MAX_FILE_NAME_LENGTH];
  memset(name, 0, sizeof(name));
  for (int64_t i = 0; i < TEST_COLUMN_CNT; ++i) {
    ObObjType obj_type = static_cast<ObObjType>(i);
    column.reset();
    column.set_collation_type(CS_TYPE_BINARY);
    column.set_table_id(table_id);
    column.set_column_id(i);
    sprintf(name, "test%020ld", i);
    ASSERT_EQ(OB_SUCCESS, column.set_column_name(name));
    column.set_data_type(obj_type);
    if (obj_type == common::ObIntType) {
      column.set_rowkey_position(1);
    } else if (obj_type == common::ObNumberType) {
      column.set_rowkey_position(2);
    } else {
      column.set_rowkey_position(0);
    }
    column.set_collation_type(ObCollationType::CS_TYPE_UTF8MB4_GENERAL_CI);
    ASSERT_EQ(OB_SUCCESS, table_schema_.add_column(column));
  }
  // init ObRowGenerate
  ASSERT_EQ(OB_SUCCESS, row_generate_.init(table_schema_));
}

void TestMemtableMultiVersionRowIterator::TearDown()
{
  allocator_.clear();
}

TEST_F(TestMemtableMultiVersionRowIterator, append_memtable_iter)
{
  int64_t trans_version = 1;
  static const ObPartitionKey PKEY(table_schema_.get_table_id(), 1, 1);
  ObMemtable mt;
  ObMemtable replayed_mt;
  int ret = OB_SUCCESS;
  storage::ObITable::TableKey table_key;
  table_key.table_type_ = storage::ObITable::MEMTABLE;
  table_key.pkey_ = PKEY;
  table_key.table_id_ = PKEY.table_id_;
  table_key.version_ = 1;
  table_key.trans_version_range_.base_version_ = 0;
  table_key.trans_version_range_.multi_version_start_ = 0;
  table_key.trans_version_range_.snapshot_version_ = INT64_MAX - 2;

  ret = mt.init(table_key);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = replayed_mt.init(table_key);
  EXPECT_EQ(OB_SUCCESS, ret);

  storage::ObStoreCtx wctx;
  storage::ObStoreCtx rctx;

  ObArray<ObITable*> array;
  wctx.tables_ = &array;

  CD cd(100,
      ObVarcharType,
      CS_TYPE_UTF8MB4_GENERAL_CI,
      101,
      ObIntType,
      CS_TYPE_UTF8MB4_BIN,
      102,
      ObIntType,
      CS_TYPE_UTF8MB4_BIN,
      103,
      ObNumberType,
      CS_TYPE_UTF8MB4_BIN);
  const ObIArray<share::schema::ObColDesc>& columns = cd.get_columns();

  ObMtRowIterator mri;
  RK rk(V("hello", 5, CS_TYPE_UTF8MB4_GENERAL_CI), I(1025), I(1024), N("3.14"));
  ObStoreRow row;
  row.row_val_.cells_ = const_cast<ObObj*>(rk.get_rowkey().get_obj_ptr());
  row.row_val_.count_ = rk.get_rowkey().get_obj_cnt();
  row.set_dml(T_DML_INSERT);
  mri.add_row(row);
  wctx.mem_ctx_ = f_.alloc();
  wctx.mem_ctx_->trans_begin();
  wctx.mem_ctx_->sub_trans_begin(0, 1000000 + ::oceanbase::common::ObTimeUtility::current_time());
  ret = mt.set(wctx, table_schema_.get_table_id(), rk.get_rowkey().get_obj_cnt() - 2, columns, mri);
  EXPECT_EQ(OB_SUCCESS, ret);
  wctx.mem_ctx_->trans_end(true, trans_version);
  f_.free(wctx.mem_ctx_);

  ObQueryFlag query_flag(ObQueryFlag::Forward,
      true, /*is daily merge scan*/
      true, /*is read multiple macro block*/
      true, /*sys task scan, read one macro block in single io*/
      false /*is full row scan?*/,
      false,
      false);

  int64_t muti_version_start = 1;
  init_table_param(table_schema_, columns);

  wctx.mem_ctx_ = f_.alloc();
  wctx.mem_ctx_->trans_begin();
  wctx.mem_ctx_->sub_trans_begin(INT64_MAX - 2, INT64_MAX - 2);
  wctx.mem_ctx_->set_multi_version_start(muti_version_start);
  ASSERT_EQ(true, wctx.mem_ctx_->is_multi_version_range_valid());

  ObBlockCacheWorkingSet block_cache_ws;
  table_access_context_.query_flag_ = query_flag;
  table_access_context_.store_ctx_ = &wctx;
  table_access_context_.allocator_ = &allocator_;
  table_access_context_.stmt_allocator_ = &allocator_;
  table_access_context_.block_cache_ws_ = &block_cache_ws;
  table_access_context_.is_inited_ = true;  // just for test case

  range_.set_whole_range();
  range_.set_table_id(table_schema_.get_table_id());
  ObExtStoreRange ext_range(range_);
  ObStoreRowIterator* store_row_iter = NULL;

  ret = mt.scan(table_param_, table_access_context_, ext_range, store_row_iter);
  ASSERT_EQ(OB_SUCCESS, ret);

  const ObStoreRow* store_row = NULL;
  ret = store_row_iter->get_next_row(store_row);
  ASSERT_EQ(OB_SUCCESS, ret);
  STORAGE_LOG(WARN, "store_row is", K(*store_row), K(store_row->row_type_flag_));
  ret = store_row_iter->get_next_row(store_row);
  ASSERT_EQ(OB_ITER_END, ret);
  wctx.mem_ctx_->trans_end(true, trans_version);
  f_.free(wctx.mem_ctx_);
}
/*
// for sequence trans_version
void test_output_flag(
    const ObStoreRow *store_row,
    const int64_t trans_version_col_idx,
    const int64_t freeze_timestamp)
{
  if (-store_row->row_val_.cells_[trans_version_col_idx].get_int() > freeze_timestamp) {
    if (freeze_timestamp + 1 == -store_row->row_val_.cells_[trans_version_col_idx].get_int()) {
      ASSERT_TRUE(ObStoreRow::MOF_LAST_ROW_OF_COMPLEMENT_MINOR_SSTABLE == store_row->output_flag_);
    } else {
      ASSERT_TRUE(ObStoreRow::MOF_COMPLEMENT_MINOR_SSTABLE_ITER_START == store_row->output_flag_);
    }
  } else {
    ASSERT_TRUE(ObStoreRow::MOF_COMPLEMENT_MINOR_SSTABLE_ITER_END == store_row->output_flag_);
  }
}

TEST_F(TestMemtableMultiVersionRowIterator, test_new_minor_freeze_committed_list)
{
  int64_t trans_version = 0;
  static const ObPartitionKey PKEY(table_schema_.get_table_id(), 1, 1);
  ObMemtable mt;
  ObMemtable replayed_mt;
  int ret = OB_SUCCESS;
  storage::ObITable::TableKey table_key;
  table_key.table_type_ = storage::ObITable::MEMTABLE;
  table_key.pkey_ = PKEY;
  table_key.table_id_ = PKEY.table_id_;
  table_key.version_ = 1;
  table_key.trans_version_range_.base_version_ = 0;
  table_key.trans_version_range_.multi_version_start_ = 0;
  table_key.trans_version_range_.snapshot_version_ = INT64_MAX - 2;

  ret = mt.init(table_key);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = replayed_mt.init(table_key);
  EXPECT_EQ(OB_SUCCESS, ret);

  storage::ObStoreCtx wctx;
  storage::ObStoreCtx rctx;
  ObArray<ObITable *> array;
  wctx.tables_ = &array;

  CD cd(
    100, ObVarcharType, CS_TYPE_UTF8MB4_GENERAL_CI,
    101, ObVarcharType, CS_TYPE_UTF8MB4_GENERAL_CI,
    102, ObIntType, CS_TYPE_UTF8MB4_BIN,
    103, ObIntType, CS_TYPE_UTF8MB4_BIN
    );
  const ObIArray<share::schema::ObColDesc> &columns = cd.get_columns();

  ObMtRowIterator mri;
  RK rk(
    V("hello", 5, CS_TYPE_UTF8MB4_GENERAL_CI),
    V("GOGO", 4, CS_TYPE_UTF8MB4_GENERAL_CI),
    I(1024),
    I(987)
    );
  for (int i = 0; i < columns.count(); ++i) {
    STORAGE_LOG(INFO, "columns", K(i), K(columns.at(i)));
  }
  ObStoreRow row;
  row.row_val_.cells_ = const_cast<ObObj *>(rk.get_rowkey().get_obj_ptr());
  row.row_val_.count_ = rk.get_rowkey().get_obj_cnt();
  STORAGE_LOG(INFO, "row", K(row));
  row.set_dml(T_DML_INSERT);
  mri.add_row(row);
  wctx.mem_ctx_ = f_.alloc();
  wctx.mem_ctx_->trans_begin();
  wctx.mem_ctx_->sub_trans_begin(trans_version, 1000000 + ::oceanbase::common::ObTimeUtility::current_time());
  ret = mt.set(wctx, table_schema_.get_table_id(), rk.get_rowkey().get_obj_cnt() - 2, columns, mri);
  EXPECT_EQ(OB_SUCCESS, ret);
  ++trans_version;
  wctx.mem_ctx_->trans_end(true, trans_version);
  f_.free(wctx.mem_ctx_);

  for (int i = 0; i < 10; ++i) {
    row.set_dml(T_DML_UPDATE);
    mri.reset();

    row.row_val_.cells_[2].set_int(i * 11);
    row.row_val_.cells_[3].set_int(i * 111);
    mri.add_row(row);

    wctx.mem_ctx_ = f_.alloc();
    wctx.mem_ctx_->trans_begin();
    wctx.mem_ctx_->sub_trans_begin(trans_version, 1000000 + ::oceanbase::common::ObTimeUtility::current_time());
    ret = mt.set(wctx, table_schema_.get_table_id(), rk.get_rowkey().get_obj_cnt() - 2, columns, mri);
    EXPECT_EQ(OB_SUCCESS, ret);
    ++trans_version;
    wctx.mem_ctx_->trans_end(true, trans_version);
    f_.free(wctx.mem_ctx_);
  }
  // trans version is between [-11, -1]
  int freeze_timestamp_list[] = {5, 9, 7, 3, 0};
  for (int i = 0; i < 5; ++i) {
    // query
    ObQueryFlag query_flag(ObQueryFlag::Forward,
                               true, //is daily merge scan
                               true, //is read multiple macro block
                               true, //sys task scan, read one macro block in single io
                               false //is full row scan?,
                               false,
                               false);
    int64_t muti_version_start = 0;
    init_table_param(table_schema_, columns);
    wctx.mem_ctx_ = f_.alloc();
    wctx.mem_ctx_->trans_begin();
    wctx.mem_ctx_->sub_trans_begin(INT64_MAX - 2 , INT64_MAX - 2);
    wctx.mem_ctx_->set_multi_version_start(muti_version_start);
    wctx.mem_ctx_->set_base_version(0);
    mt.set_freeze_log_id(freeze_timestamp_list[i]);
    ASSERT_EQ(true, wctx.mem_ctx_->is_multi_version_range_valid());

    ObBlockCacheWorkingSet block_cache_ws;
    table_access_context_.query_flag_ = query_flag;
    table_access_context_.store_ctx_ = &wctx;
    table_access_context_.allocator_ = &allocator_;
    table_access_context_.stmt_allocator_ = &allocator_;
    table_access_context_.block_cache_ws_ = &block_cache_ws;
    table_access_context_.is_inited_ = true;//just for test case

    range_.set_whole_range();
    range_.set_table_id(table_schema_.get_table_id());
    ObExtStoreRange ext_range(range_);
    ObStoreRowIterator *store_row_iter = NULL;

    ret = mt.scan(table_param_, table_access_context_, ext_range, store_row_iter);
    ASSERT_EQ(OB_SUCCESS, ret);
    const ObStoreRow *store_row = NULL;
    int trans_version_col_idx = TEST_ROWKEY_COLUMN_CNT;
    while(OB_SUCCESS == store_row_iter->get_next_row(store_row)) {
      STORAGE_LOG(INFO, "store_row is", K(freeze_timestamp_list[i]), K(*store_row));
      test_output_flag(store_row, trans_version_col_idx, freeze_timestamp_list[i]);
    }
    wctx.mem_ctx_->trans_end(true, trans_version);
    f_.free(wctx.mem_ctx_);
  }
}

TEST_F(TestMemtableMultiVersionRowIterator, test_new_minor_freeze_committed_list_with_gap)
{
  int64_t trans_version = 0;
  static const ObPartitionKey PKEY(table_schema_.get_table_id(), 1, 1);
  ObMemtable mt;
  ObMemtable replayed_mt;
  int ret = OB_SUCCESS;
  storage::ObITable::TableKey table_key;
  table_key.table_type_ = storage::ObITable::MEMTABLE;
  table_key.pkey_ = PKEY;
  table_key.table_id_ = PKEY.table_id_;
  table_key.version_ = 1;
  table_key.trans_version_range_.base_version_ = 0;
  table_key.trans_version_range_.multi_version_start_ = 0;
  table_key.trans_version_range_.snapshot_version_ = INT64_MAX - 2;

  ret = mt.init(table_key);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = replayed_mt.init(table_key);
  EXPECT_EQ(OB_SUCCESS, ret);

  storage::ObStoreCtx wctx;
  storage::ObStoreCtx rctx;
  ObArray<ObITable *> array;
  wctx.tables_ = &array;

  CD cd(
    100, ObVarcharType, CS_TYPE_UTF8MB4_GENERAL_CI,
    101, ObVarcharType, CS_TYPE_UTF8MB4_GENERAL_CI,
    102, ObIntType, CS_TYPE_UTF8MB4_BIN,
    103, ObIntType, CS_TYPE_UTF8MB4_BIN
    );
  const ObIArray<share::schema::ObColDesc> &columns = cd.get_columns();

  ObMtRowIterator mri;
  RK rk(
    V("hello", 5, CS_TYPE_UTF8MB4_GENERAL_CI),
    V("GOGO", 4, CS_TYPE_UTF8MB4_GENERAL_CI),
    I(1024),
    I(987)
    );
  ObStoreRow row;
  row.row_val_.cells_ = const_cast<ObObj *>(rk.get_rowkey().get_obj_ptr());
  row.row_val_.count_ = rk.get_rowkey().get_obj_cnt();
  STORAGE_LOG(INFO, "row", K(row));
  row.set_dml(T_DML_INSERT);
  mri.add_row(row);
  wctx.mem_ctx_ = f_.alloc();
  wctx.mem_ctx_->trans_begin();
  wctx.mem_ctx_->sub_trans_begin(trans_version, 1000000 + ::oceanbase::common::ObTimeUtility::current_time());
  ret = mt.set(wctx, table_schema_.get_table_id(), rk.get_rowkey().get_obj_cnt() - 2, columns, mri);
  EXPECT_EQ(OB_SUCCESS, ret);
  ++trans_version;
  wctx.mem_ctx_->trans_end(true, trans_version);
  f_.free(wctx.mem_ctx_);

  int trans_version_cnt = 10;
  int trans_version_start_list[] = {
      2, 6, 9, 14, 85,
      102, 159, 190, 221, 281};
  int trans_version_end_list[] =   {
      5, 7, 10, 14, 87,
      157, 182, 201, 271, 290};

  for (int i = 0; i < 10; ++i) {
    row.set_dml(T_DML_UPDATE);
    mri.reset();

    row.row_val_.cells_[2].set_int(i * 11);
    row.row_val_.cells_[3].set_int(i * 111);
    mri.add_row(row);

    wctx.mem_ctx_ = f_.alloc();
    wctx.mem_ctx_->trans_begin();
    wctx.mem_ctx_->sub_trans_begin(trans_version_start_list[i], 1000000 +
::oceanbase::common::ObTimeUtility::current_time()); ret = mt.set(wctx, table_schema_.get_table_id(),
rk.get_rowkey().get_obj_cnt() - 2, columns, mri); EXPECT_EQ(OB_SUCCESS, ret); wctx.mem_ctx_->trans_end(true,
trans_version_end_list[i]); f_.free(wctx.mem_ctx_);
  }
  // trans version is between [-11, -1]
  int freeze_timestamp_list[] = {5, 180, 1, 289, 182};
  for (int i = 0; i < 5; ++i) {
    // query
    ObQueryFlag query_flag(ObQueryFlag::Forward,
                               true, //is daily merge scan
                               true, //is read multiple macro block
                               true, //sys task scan, read one macro block in single io
                               false //is full row scan?,
                               false,
                               false);
    int64_t muti_version_start = 0;
    init_table_param(table_schema_, columns);
    wctx.mem_ctx_ = f_.alloc();
    wctx.mem_ctx_->trans_begin();
    wctx.mem_ctx_->sub_trans_begin(INT64_MAX - 2 , INT64_MAX - 2);
    wctx.mem_ctx_->set_multi_version_start(muti_version_start);
    wctx.mem_ctx_->set_base_version(0);
    mt.set_freeze_log_id(freeze_timestamp_list[i]);
    ASSERT_EQ(true, wctx.mem_ctx_->is_multi_version_range_valid());

    ObBlockCacheWorkingSet block_cache_ws;
    table_access_context_.query_flag_ = query_flag;
    table_access_context_.store_ctx_ = &wctx;
    table_access_context_.allocator_ = &allocator_;
    table_access_context_.stmt_allocator_ = &allocator_;
    table_access_context_.block_cache_ws_ = &block_cache_ws;
    table_access_context_.is_inited_ = true;//just for test case

    range_.set_whole_range();
    range_.set_table_id(table_schema_.get_table_id());
    ObExtStoreRange ext_range(range_);
    ObStoreRowIterator *store_row_iter = NULL;

    ret = mt.scan(table_param_, table_access_context_, ext_range, store_row_iter);
    ASSERT_EQ(OB_SUCCESS, ret);
    const ObStoreRow *store_row = NULL;
    int trans_version_col_idx = TEST_ROWKEY_COLUMN_CNT;
    int trans_version_end_val_idx = trans_version_cnt - 1;
    while(OB_SUCCESS == store_row_iter->get_next_row(store_row)) {
      STORAGE_LOG(INFO, "store_row is", K(freeze_timestamp_list[i]), K(*store_row));
      if (-store_row->row_val_.cells_[trans_version_col_idx].get_int() > freeze_timestamp_list[i]) {
        if (0 == trans_version_end_val_idx
            || freeze_timestamp_list[i] >= trans_version_end_list[trans_version_end_val_idx - 1]) {
          ASSERT_TRUE(ObStoreRow::MOF_LAST_ROW_OF_COMPLEMENT_MINOR_SSTABLE == store_row->output_flag_);
        } else {
          ASSERT_TRUE(ObStoreRow::MOF_COMPLEMENT_MINOR_SSTABLE_ITER_START == store_row->output_flag_);
        }
      } else {
        ASSERT_TRUE(ObStoreRow::MOF_COMPLEMENT_MINOR_SSTABLE_ITER_END == store_row->output_flag_);
      }
      --trans_version_end_val_idx;
    }
    wctx.mem_ctx_->trans_end(true, trans_version);
    f_.free(wctx.mem_ctx_);
  }
}
*/
TEST_F(TestMemtableMultiVersionRowIterator, append_and_update_row_memtable_iter)
{
  int64_t trans_version = 1;
  static const ObPartitionKey PKEY(table_schema_.get_table_id(), 1, 1);
  ObMemtable mt;
  ObMemtable replayed_mt;
  int ret = OB_SUCCESS;
  storage::ObITable::TableKey table_key;
  table_key.table_type_ = storage::ObITable::MEMTABLE;
  table_key.pkey_ = PKEY;
  table_key.table_id_ = PKEY.table_id_;
  table_key.version_ = 1;
  table_key.trans_version_range_.base_version_ = 0;
  table_key.trans_version_range_.multi_version_start_ = 0;
  table_key.trans_version_range_.snapshot_version_ = INT64_MAX - 2;

  ret = mt.init(table_key);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = replayed_mt.init(table_key);
  EXPECT_EQ(OB_SUCCESS, ret);

  storage::ObStoreCtx wctx;
  storage::ObStoreCtx rctx;
  ObArray<ObITable*> array;
  wctx.tables_ = &array;

  CD cd(100,
      ObVarcharType,
      CS_TYPE_UTF8MB4_GENERAL_CI,
      101,
      ObVarcharType,
      CS_TYPE_UTF8MB4_GENERAL_CI,
      102,
      ObIntType,
      CS_TYPE_UTF8MB4_BIN,
      103,
      ObIntType,
      CS_TYPE_UTF8MB4_BIN);
  const ObIArray<share::schema::ObColDesc>& columns = cd.get_columns();

  ObMtRowIterator mri;
  RK rk(V("hello", 5, CS_TYPE_UTF8MB4_GENERAL_CI), V("GOGO", 4, CS_TYPE_UTF8MB4_GENERAL_CI), I(1024), I(987));
  for (int i = 0; i < columns.count(); ++i) {
    STORAGE_LOG(INFO, "columns", K(i), K(columns.at(i)));
  }
  ObStoreRow row;
  row.row_val_.cells_ = const_cast<ObObj*>(rk.get_rowkey().get_obj_ptr());
  row.row_val_.count_ = rk.get_rowkey().get_obj_cnt();
  STORAGE_LOG(INFO, "row", K(row));
  row.set_dml(T_DML_INSERT);
  mri.add_row(row);
  wctx.mem_ctx_ = f_.alloc();
  wctx.mem_ctx_->trans_begin();
  wctx.mem_ctx_->sub_trans_begin(0, 1000000 + ::oceanbase::common::ObTimeUtility::current_time());
  ret = mt.set(wctx, table_schema_.get_table_id(), rk.get_rowkey().get_obj_cnt() - 2, columns, mri);
  EXPECT_EQ(OB_SUCCESS, ret);
  wctx.mem_ctx_->trans_end(true, trans_version);
  f_.free(wctx.mem_ctx_);

  trans_version++;
  row.set_dml(T_DML_UPDATE);
  mri.reset();
  row.row_val_.cells_[2].set_int(999);
  row.row_val_.cells_[3].set_int(111);
  mri.add_row(row);
  wctx.mem_ctx_ = f_.alloc();
  wctx.mem_ctx_->trans_begin();
  wctx.mem_ctx_->sub_trans_begin(1, 1000000 + ::oceanbase::common::ObTimeUtility::current_time());
  ret = mt.set(wctx, table_schema_.get_table_id(), rk.get_rowkey().get_obj_cnt() - 2, columns, mri);
  EXPECT_EQ(OB_SUCCESS, ret);
  wctx.mem_ctx_->trans_end(true, trans_version);
  f_.free(wctx.mem_ctx_);

  trans_version++;
  row.set_dml(T_DML_UPDATE);
  row.row_val_.cells_[2].set_int(777);
  mri.reset();
  mri.add_row(row);
  wctx.mem_ctx_ = f_.alloc();
  wctx.mem_ctx_->trans_begin();
  wctx.mem_ctx_->sub_trans_begin(2, 1000000 + ::oceanbase::common::ObTimeUtility::current_time());
  ret = mt.set(wctx, table_schema_.get_table_id(), rk.get_rowkey().get_obj_cnt() - 2, columns, mri);
  EXPECT_EQ(OB_SUCCESS, ret);
  wctx.mem_ctx_->trans_end(true, trans_version);
  f_.free(wctx.mem_ctx_);

  ObQueryFlag query_flag(ObQueryFlag::Forward,
      true, /*is daily merge scan*/
      true, /*is read multiple macro block*/
      true, /*sys task scan, read one macro block in single io*/
      false /*is full row scan?*/,
      false,
      false);
  int64_t muti_version_start = 2;
  init_table_param(table_schema_, columns);

  wctx.mem_ctx_ = f_.alloc();
  wctx.mem_ctx_->trans_begin();
  wctx.mem_ctx_->sub_trans_begin(INT64_MAX - 2, INT64_MAX - 2);
  wctx.mem_ctx_->set_multi_version_start(muti_version_start);
  wctx.mem_ctx_->set_base_version(0);
  ASSERT_EQ(true, wctx.mem_ctx_->is_multi_version_range_valid());

  ObBlockCacheWorkingSet block_cache_ws;
  table_access_context_.query_flag_ = query_flag;
  table_access_context_.store_ctx_ = &wctx;
  table_access_context_.allocator_ = &allocator_;
  table_access_context_.stmt_allocator_ = &allocator_;
  table_access_context_.block_cache_ws_ = &block_cache_ws;
  table_access_context_.is_inited_ = true;  // just for test case

  range_.set_whole_range();
  range_.set_table_id(table_schema_.get_table_id());
  ObExtStoreRange ext_range(range_);
  ObStoreRowIterator* store_row_iter = NULL;

  ret = mt.scan(table_param_, table_access_context_, ext_range, store_row_iter);
  // ret = uncompacted_row_iter_.init(table_access_param_, table_access_context_, mvcc_engine_, range_);
  ASSERT_EQ(OB_SUCCESS, ret);
  const ObStoreRow* store_row = NULL;

  const char* result =
      " var     var   bigint bigint bigint   bigint   dml           first_dml       flag    multi_version_row_flag\n"
      "hello   GOGO     -3    0      777     111     T_DML_UPDATE   T_DML_INSERT    EXIST   CF\n"
      "hello   GOGO     -2    0      999     111     T_DML_UPDATE   T_DML_INSERT    EXIST   CL\n";
  uint16_t result_col_id[] = {100, 101, 7, 8, 102, 103, 100, 101, 7, 8, 102, 103};
  int64_t result_col_cnt[] = {6, 6};
  ObMockIterator res_iter;
  ret = res_iter.from(result, '\\', result_col_id, result_col_cnt);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(res_iter.equals(*store_row_iter, true));  // cmp multi version row flag
  wctx.mem_ctx_->trans_end(true, trans_version);
  f_.free(wctx.mem_ctx_);
}

TEST_F(TestMemtableMultiVersionRowIterator, append_and_update_row_memtable_iter_multi_version)
{
  int64_t trans_version = 1;
  static const ObPartitionKey PKEY(table_schema_.get_table_id(), 1, 1);
  ObMemtable mt;
  ObMemtable replayed_mt;
  int ret = OB_SUCCESS;
  storage::ObITable::TableKey table_key;
  table_key.table_type_ = storage::ObITable::MEMTABLE;
  table_key.pkey_ = PKEY;
  table_key.table_id_ = PKEY.table_id_;
  table_key.version_ = 1;
  table_key.trans_version_range_.base_version_ = 0;
  table_key.trans_version_range_.multi_version_start_ = 0;
  table_key.trans_version_range_.snapshot_version_ = INT64_MAX - 2;
  ret = mt.init(table_key);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = replayed_mt.init(table_key);
  EXPECT_EQ(OB_SUCCESS, ret);
  storage::ObStoreCtx wctx;
  storage::ObStoreCtx rctx;
  ObArray<ObITable*> array;
  wctx.tables_ = &array;
  CD cd(100,
      ObVarcharType,
      CS_TYPE_UTF8MB4_GENERAL_CI,
      101,
      ObVarcharType,
      CS_TYPE_UTF8MB4_GENERAL_CI,
      102,
      ObIntType,
      CS_TYPE_UTF8MB4_BIN,
      103,
      ObIntType,
      CS_TYPE_UTF8MB4_BIN);
  const ObIArray<share::schema::ObColDesc>& columns = cd.get_columns();
  ObMtRowIterator mri;
  RK rk(V("hello", 5, CS_TYPE_UTF8MB4_GENERAL_CI), V("GOGO", 4, CS_TYPE_UTF8MB4_GENERAL_CI), I(1024), I(987));
  for (int i = 0; i < columns.count(); ++i) {
    STORAGE_LOG(INFO, "columns", K(i), K(columns.at(i)));
  }
  ObStoreRow row;
  row.row_val_.cells_ = const_cast<ObObj*>(rk.get_rowkey().get_obj_ptr());
  row.row_val_.count_ = rk.get_rowkey().get_obj_cnt();
  STORAGE_LOG(INFO, "row", K(row));
  row.set_dml(T_DML_INSERT);
  mri.add_row(row);
  wctx.mem_ctx_ = f_.alloc();
  wctx.mem_ctx_->trans_begin();
  wctx.mem_ctx_->sub_trans_begin(0, 1000000 + ::oceanbase::common::ObTimeUtility::current_time());
  ret = mt.set(wctx, table_schema_.get_table_id(), rk.get_rowkey().get_obj_cnt() - 2, columns, mri);
  EXPECT_EQ(OB_SUCCESS, ret);
  wctx.mem_ctx_->trans_end(true, trans_version);
  f_.free(wctx.mem_ctx_);
  trans_version = 4;
  row.set_dml(T_DML_UPDATE);
  mri.reset();
  row.row_val_.cells_[2].set_int(999);
  row.row_val_.cells_[3].set_int(111);
  mri.add_row(row);
  wctx.mem_ctx_ = f_.alloc();
  wctx.mem_ctx_->trans_begin();
  wctx.mem_ctx_->sub_trans_begin(4, 1000000 + ::oceanbase::common::ObTimeUtility::current_time());
  ret = mt.set(wctx, table_schema_.get_table_id(), rk.get_rowkey().get_obj_cnt() - 2, columns, mri);
  EXPECT_EQ(OB_SUCCESS, ret);
  wctx.mem_ctx_->trans_end(true, trans_version);
  f_.free(wctx.mem_ctx_);
  trans_version++;
  row.set_dml(T_DML_UPDATE);
  row.row_val_.cells_[2].set_int(777);
  mri.reset();
  mri.add_row(row);
  wctx.mem_ctx_ = f_.alloc();
  wctx.mem_ctx_->trans_begin();
  wctx.mem_ctx_->sub_trans_begin(5, 1000000 + ::oceanbase::common::ObTimeUtility::current_time());
  ret = mt.set(wctx, table_schema_.get_table_id(), rk.get_rowkey().get_obj_cnt() - 2, columns, mri);
  EXPECT_EQ(OB_SUCCESS, ret);
  wctx.mem_ctx_->trans_end(true, trans_version);
  f_.free(wctx.mem_ctx_);

  ObQueryFlag query_flag(ObQueryFlag::Forward,
      true, /*is daily merge scan*/
      true, /*is read multiple macro block*/
      true, /*sys task scan, read one macro block in single io*/
      false /*is full row scan?*/,
      false,
      false);
  int64_t muti_version_start = 2;
  init_table_param(table_schema_, columns);

  wctx.mem_ctx_ = f_.alloc();
  wctx.mem_ctx_->trans_begin();
  wctx.mem_ctx_->sub_trans_begin(INT64_MAX - 2, INT64_MAX - 2);
  wctx.mem_ctx_->set_multi_version_start(muti_version_start);
  wctx.mem_ctx_->set_base_version(0);
  ASSERT_EQ(true, wctx.mem_ctx_->is_multi_version_range_valid());

  ObBlockCacheWorkingSet block_cache_ws;
  table_access_context_.query_flag_ = query_flag;
  table_access_context_.store_ctx_ = &wctx;
  table_access_context_.allocator_ = &allocator_;
  table_access_context_.stmt_allocator_ = &allocator_;
  table_access_context_.block_cache_ws_ = &block_cache_ws;
  table_access_context_.is_inited_ = true;  // just for test case

  range_.set_whole_range();
  range_.set_table_id(table_schema_.get_table_id());
  ObExtStoreRange ext_range(range_);
  ObStoreRowIterator* store_row_iter = NULL;
  ret = mt.scan(table_param_, table_access_context_, ext_range, store_row_iter);
  ASSERT_EQ(OB_SUCCESS, ret);
  const ObStoreRow* store_row = NULL;
  const char* result =
      " var     var   bigint bigint bigint   bigint   dml           first_dml       flag    multi_version_row_flag\n"
      "hello   GOGO     -5    0      777     111     T_DML_UPDATE   T_DML_INSERT    EXIST   CF\n"
      "hello   GOGO     -4    0      999     111     T_DML_UPDATE   T_DML_INSERT    EXIST   N\n"
      "hello   GOGO     -1    0      1024    987     T_DML_INSERT   T_DML_INSERT    EXIST   CL\n";
  uint16_t result_col_id[] = {100, 101, 7, 8, 102, 103, 100, 101, 7, 8, 102, 103, 100, 101, 7, 8, 102, 103};
  int64_t result_col_cnt[] = {6, 6, 6};
  ObMockIterator res_iter;
  ret = res_iter.from(result, '\\', result_col_id, result_col_cnt);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(res_iter.equals(*store_row_iter, true));  // cmp multi version row flag
  wctx.mem_ctx_->trans_end(true, trans_version);
  f_.free(wctx.mem_ctx_);
}

TEST_F(TestMemtableMultiVersionRowIterator, perf_test)
{
  int64_t trans_version = 1;
  static const ObPartitionKey PKEY(table_schema_.get_table_id(), 1, 1);
  ObMemtable mt;
  ObMemtable replayed_mt;
  int ret = OB_SUCCESS;
  storage::ObITable::TableKey table_key;
  table_key.table_type_ = storage::ObITable::MEMTABLE;
  table_key.pkey_ = PKEY;
  table_key.table_id_ = PKEY.table_id_;
  table_key.version_ = 1;
  table_key.trans_version_range_.base_version_ = 0;
  table_key.trans_version_range_.multi_version_start_ = 0;
  table_key.trans_version_range_.snapshot_version_ = INT64_MAX - 2;

  ret = mt.init(table_key);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = replayed_mt.init(table_key);
  EXPECT_EQ(OB_SUCCESS, ret);

  storage::ObStoreCtx wctx;
  storage::ObStoreCtx rctx;
  ObArray<ObITable*> array;
  wctx.tables_ = &array;

  CD cd(100,
      ObVarcharType,
      CS_TYPE_UTF8MB4_GENERAL_CI,
      101,
      ObVarcharType,
      CS_TYPE_UTF8MB4_GENERAL_CI,
      102,
      ObIntType,
      CS_TYPE_UTF8MB4_BIN,
      103,
      ObIntType,
      CS_TYPE_UTF8MB4_BIN);
  const ObIArray<share::schema::ObColDesc>& columns = cd.get_columns();

  ObMtRowIterator mri;
  RK rk(V("hello", 5, CS_TYPE_UTF8MB4_GENERAL_CI), V("GOGO", 4, CS_TYPE_UTF8MB4_GENERAL_CI), I(1024), I(987));

  ObStoreRow row;
  row.row_val_.cells_ = const_cast<ObObj*>(rk.get_rowkey().get_obj_ptr());
  row.row_val_.count_ = rk.get_rowkey().get_obj_cnt();
  row.set_dml(T_DML_INSERT);
  mri.add_row(row);
  wctx.mem_ctx_ = f_.alloc();
  wctx.mem_ctx_->trans_begin();
  wctx.mem_ctx_->sub_trans_begin(0, 1000000 + ::oceanbase::common::ObTimeUtility::current_time());
  ret = mt.set(wctx, table_schema_.get_table_id(), rk.get_rowkey().get_obj_cnt() - 2, columns, mri);
  EXPECT_EQ(OB_SUCCESS, ret);
  wctx.mem_ctx_->trans_end(true, trans_version);
  f_.free(wctx.mem_ctx_);
  STORAGE_LOG(INFO, "perf write start");
  for (int i = 1; i < 10 * 10000; ++i) {
    row.set_dml(T_DML_UPDATE);
    mri.reset();
    row.row_val_.cells_[2].set_int(i);
    mri.add_row(row);
    wctx.mem_ctx_ = f_.alloc();
    wctx.mem_ctx_->trans_begin();
    wctx.mem_ctx_->sub_trans_begin(trans_version, 1000000 + ::oceanbase::common::ObTimeUtility::current_time());
    ret = mt.set(wctx, table_schema_.get_table_id(), rk.get_rowkey().get_obj_cnt() - 2, columns, mri);
    EXPECT_EQ(OB_SUCCESS, ret);
    trans_version++;
    wctx.mem_ctx_->trans_end(true, trans_version);
    f_.free(wctx.mem_ctx_);
  }
  STORAGE_LOG(INFO, "perf write end");

  ObQueryFlag query_flag(ObQueryFlag::Forward,
      true, /*is daily merge scan*/
      true, /*is read multiple macro block*/
      true, /*sys task scan, read one macro block in single io*/
      false /*is full row scan?*/,
      false,
      false);
  int64_t muti_version_start = 2;
  init_table_param(table_schema_, columns);

  wctx.mem_ctx_ = f_.alloc();
  wctx.mem_ctx_->trans_begin();
  wctx.mem_ctx_->sub_trans_begin(INT64_MAX - 2, INT64_MAX - 2);
  wctx.mem_ctx_->set_multi_version_start(muti_version_start);
  wctx.mem_ctx_->set_base_version(0);
  ASSERT_EQ(true, wctx.mem_ctx_->is_multi_version_range_valid());

  ObBlockCacheWorkingSet block_cache_ws;
  table_access_context_.query_flag_ = query_flag;
  table_access_context_.store_ctx_ = &wctx;
  table_access_context_.allocator_ = &allocator_;
  table_access_context_.stmt_allocator_ = &allocator_;
  table_access_context_.block_cache_ws_ = &block_cache_ws;
  table_access_context_.is_inited_ = true;  // just for test case

  range_.set_whole_range();
  range_.set_table_id(table_schema_.get_table_id());
  ObExtStoreRange ext_range(range_);
  ObStoreRowIterator* store_row_iter = NULL;

  ret = mt.scan(table_param_, table_access_context_, ext_range, store_row_iter);
  ASSERT_EQ(OB_SUCCESS, ret);
  const ObStoreRow* store_row = NULL;
  STORAGE_LOG(INFO, "perf read start");
  ret = store_row_iter->get_next_row(store_row);
  ASSERT_EQ(true, store_row->row_type_flag_.is_compacted_multi_version_row());
  STORAGE_LOG(INFO, "perf read first row start");
  int i = 0;
  while (OB_SUCCESS == ret) {
    ret = store_row_iter->get_next_row(store_row);
    ++i;
  }
  STORAGE_LOG(INFO, "perf read end", K(i));
  wctx.mem_ctx_->trans_end(true, trans_version);
  f_.free(wctx.mem_ctx_);
}

TEST_F(TestMemtableMultiVersionRowIterator, append_and_delete_row_memtable_iter)
{
  int64_t trans_version = 1;
  static const ObPartitionKey PKEY(table_schema_.get_table_id(), 1, 1);
  ObMemtable mt;
  ObMemtable replayed_mt;
  int ret = OB_SUCCESS;
  storage::ObITable::TableKey table_key;
  table_key.table_type_ = storage::ObITable::MEMTABLE;
  table_key.pkey_ = PKEY;
  table_key.table_id_ = PKEY.table_id_;
  table_key.version_ = 1;
  table_key.trans_version_range_.base_version_ = 0;
  table_key.trans_version_range_.multi_version_start_ = 0;
  table_key.trans_version_range_.snapshot_version_ = INT64_MAX - 2;

  ret = mt.init(table_key);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = replayed_mt.init(table_key);
  EXPECT_EQ(OB_SUCCESS, ret);

  storage::ObStoreCtx wctx;
  storage::ObStoreCtx rctx;

  ObArray<ObITable*> array;
  wctx.tables_ = &array;

  CD cd(100,
      ObVarcharType,
      CS_TYPE_UTF8MB4_GENERAL_CI,
      101,
      ObVarcharType,
      CS_TYPE_UTF8MB4_GENERAL_CI,
      102,
      ObIntType,
      CS_TYPE_UTF8MB4_BIN,
      103,
      ObNumberType,
      CS_TYPE_UTF8MB4_BIN);
  const ObIArray<share::schema::ObColDesc>& columns = cd.get_columns();

  ObMtRowIterator mri;
  RK rk(V("hello", 5, CS_TYPE_UTF8MB4_GENERAL_CI), V("kk", 2, CS_TYPE_UTF8MB4_GENERAL_CI), I(1024), N("3.14"));
  ObStoreRow row;
  row.row_val_.cells_ = const_cast<ObObj*>(rk.get_rowkey().get_obj_ptr());
  row.row_val_.count_ = rk.get_rowkey().get_obj_cnt();
  row.set_dml(T_DML_INSERT);
  mri.add_row(row);
  wctx.mem_ctx_ = f_.alloc();
  wctx.mem_ctx_->trans_begin();
  wctx.mem_ctx_->sub_trans_begin(0, 1000000 + ::oceanbase::common::ObTimeUtility::current_time());
  ret = mt.set(wctx, table_schema_.get_table_id(), rk.get_rowkey().get_obj_cnt() - 2, columns, mri);
  EXPECT_EQ(OB_SUCCESS, ret);
  wctx.mem_ctx_->trans_end(true, trans_version);
  f_.free(wctx.mem_ctx_);

  trans_version++;
  row.set_dml(T_DML_UPDATE);
  mri.reset();
  mri.add_row(row);
  wctx.mem_ctx_ = f_.alloc();
  wctx.mem_ctx_->trans_begin();
  wctx.mem_ctx_->sub_trans_begin(1, 1000000 + ::oceanbase::common::ObTimeUtility::current_time());
  ret = mt.set(wctx, table_schema_.get_table_id(), rk.get_rowkey().get_obj_cnt() - 2, columns, mri);
  EXPECT_EQ(OB_SUCCESS, ret);
  wctx.mem_ctx_->trans_end(true, trans_version);
  f_.free(wctx.mem_ctx_);

  trans_version++;
  row.set_dml(T_DML_DELETE);
  mri.reset();
  mri.add_row(row);
  wctx.mem_ctx_ = f_.alloc();
  wctx.mem_ctx_->trans_begin();
  wctx.mem_ctx_->sub_trans_begin(2, 1000000 + ::oceanbase::common::ObTimeUtility::current_time());
  ret = mt.set(wctx, table_schema_.get_table_id(), rk.get_rowkey().get_obj_cnt() - 2, columns, mri);
  EXPECT_EQ(OB_SUCCESS, ret);
  wctx.mem_ctx_->trans_end(true, trans_version);
  f_.free(wctx.mem_ctx_);

  ObQueryFlag query_flag(ObQueryFlag::Forward,
      true, /*is daily merge scan*/
      true, /*is read multiple macro block*/
      true, /*sys task scan, read one macro block in single io*/
      false /*is full row scan?*/,
      false,
      false);
  int64_t muti_version_start = 1;
  init_table_param(table_schema_, columns);

  wctx.mem_ctx_ = f_.alloc();
  wctx.mem_ctx_->trans_begin();
  wctx.mem_ctx_->sub_trans_begin(INT64_MAX - 2, INT64_MAX - 2);
  wctx.mem_ctx_->set_multi_version_start(muti_version_start);
  wctx.mem_ctx_->set_base_version(0);
  ASSERT_EQ(true, wctx.mem_ctx_->is_multi_version_range_valid());

  ObBlockCacheWorkingSet block_cache_ws;
  /*  table_access_context_.init(
        query_flag,
        wctx,
        allocator_,
        range_
        );*/
  table_access_context_.query_flag_ = query_flag;
  table_access_context_.store_ctx_ = &wctx;
  table_access_context_.allocator_ = &allocator_;
  table_access_context_.stmt_allocator_ = &allocator_;
  table_access_context_.block_cache_ws_ = &block_cache_ws;
  table_access_context_.is_inited_ = true;
  range_.set_whole_range();
  range_.set_table_id(table_schema_.get_table_id());
  ObExtStoreRange ext_range(range_);
  ObStoreRowIterator* store_row_iter = NULL;

  ret = mt.scan(table_param_, table_access_context_, ext_range, store_row_iter);
  // ret = uncompacted_row_iter_.init(table_access_param_, table_access_context_, mvcc_engine_, range_);
  ASSERT_EQ(OB_SUCCESS, ret);
  const ObStoreRow* store_row = NULL;
  const char* result =
      " var    var   bigint bigint  bigint   number   dml           first_dml       flag    multi_version_row_flag\n"
      "hello   kk     -3       0    NOP     NOP     T_DML_DELETE   T_DML_INSERT    DELETE   CF\n"
      "hello   kk     -2       0    1024    3.14    T_DML_UPDATE   T_DML_INSERT    EXIST    N\n"
      "hello   kk     -1       0    1024    3.14    T_DML_INSERT   T_DML_INSERT    EXIST    CL\n";
  uint16_t result_col_id[] = {100, 101, 7, 8, 100, 101, 7, 8, 102, 103, 100, 101, 7, 8, 102, 103};
  int64_t result_col_cnt[] = {4, 6, 6};
  ObMockIterator res_iter;
  ret = res_iter.from(result, '\\', result_col_id, result_col_cnt);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(res_iter.equals(*store_row_iter, true));  // cmp multi version row flag

  wctx.mem_ctx_->trans_end(true, trans_version);
  f_.free(wctx.mem_ctx_);
}

TEST_F(TestMemtableMultiVersionRowIterator, append_muti_row_compacted_memtable_iter)
{
  int64_t trans_version = 1;
  static const ObPartitionKey PKEY(table_schema_.get_table_id(), 1, 1);
  ObMemtable mt;
  ObMemtable replayed_mt;
  int ret = OB_SUCCESS;
  storage::ObITable::TableKey table_key;
  table_key.table_type_ = storage::ObITable::MEMTABLE;
  table_key.pkey_ = PKEY;
  table_key.table_id_ = PKEY.table_id_;
  table_key.version_ = 1;
  table_key.trans_version_range_.base_version_ = 0;
  table_key.trans_version_range_.multi_version_start_ = 0;
  table_key.trans_version_range_.snapshot_version_ = INT64_MAX - 2;

  ret = mt.init(table_key);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = replayed_mt.init(table_key);
  EXPECT_EQ(OB_SUCCESS, ret);

  storage::ObStoreCtx wctx;
  storage::ObStoreCtx rctx;

  ObArray<ObITable*> array;
  wctx.tables_ = &array;

  CD cd(100,
      ObVarcharType,
      CS_TYPE_UTF8MB4_GENERAL_CI,
      101,
      ObVarcharType,
      CS_TYPE_UTF8MB4_GENERAL_CI,
      102,
      ObIntType,
      CS_TYPE_UTF8MB4_BIN,
      103,
      ObIntType,
      CS_TYPE_UTF8MB4_BIN,
      104,
      ObNumberType,
      CS_TYPE_UTF8MB4_BIN);
  const ObIArray<share::schema::ObColDesc>& columns = cd.get_columns();

  ObMtRowIterator mri;
  RK rk(V("hello", 5, CS_TYPE_UTF8MB4_GENERAL_CI), V("GG", 2, CS_TYPE_UTF8MB4_GENERAL_CI), I(1024), I(19), N("3.14"));
  ObStoreRow row;
  row.row_val_.cells_ = const_cast<ObObj*>(rk.get_rowkey().get_obj_ptr());
  row.row_val_.count_ = rk.get_rowkey().get_obj_cnt();
  row.set_dml(T_DML_INSERT);
  // row.column_ids_ = col_id;
  mri.add_row(row);
  wctx.mem_ctx_ = f_.alloc();
  wctx.mem_ctx_->trans_begin();
  wctx.mem_ctx_->sub_trans_begin(0, 1000000 + ::oceanbase::common::ObTimeUtility::current_time());
  ret = mt.set(wctx, table_schema_.get_table_id(), rk.get_rowkey().get_obj_cnt() - 3, columns, mri);
  EXPECT_EQ(OB_SUCCESS, ret);
  wctx.mem_ctx_->trans_end(true, trans_version);
  f_.free(wctx.mem_ctx_);

  trans_version++;
  row.set_dml(T_DML_UPDATE);
  row.row_val_.cells_[3].set_int(781);
  mri.reset();
  mri.add_row(row);
  wctx.mem_ctx_ = f_.alloc();
  wctx.mem_ctx_->trans_begin();
  wctx.mem_ctx_->sub_trans_begin(1, 1000000 + ::oceanbase::common::ObTimeUtility::current_time());
  ret = mt.set(wctx, table_schema_.get_table_id(), rk.get_rowkey().get_obj_cnt() - 3, columns, mri);
  EXPECT_EQ(OB_SUCCESS, ret);
  wctx.mem_ctx_->trans_end(true, trans_version);
  f_.free(wctx.mem_ctx_);

  trans_version++;
  row.set_dml(T_DML_UPDATE);
  row.row_val_.cells_[2].set_int(657);
  mri.reset();
  mri.add_row(row);
  wctx.mem_ctx_ = f_.alloc();
  wctx.mem_ctx_->trans_begin();
  wctx.mem_ctx_->sub_trans_begin(2, 1000000 + ::oceanbase::common::ObTimeUtility::current_time());
  ret = mt.set(wctx, table_schema_.get_table_id(), rk.get_rowkey().get_obj_cnt() - 3, columns, mri);
  EXPECT_EQ(OB_SUCCESS, ret);
  wctx.mem_ctx_->trans_end(true, trans_version);
  f_.free(wctx.mem_ctx_);

  ObString test_string("world");
  trans_version++;
  row.set_dml(T_DML_INSERT);
  row.row_val_.cells_[2].set_int(1025);
  row.row_val_.cells_[0].set_varchar(test_string);
  mri.reset();
  mri.add_row(row);
  wctx.mem_ctx_ = f_.alloc();
  wctx.mem_ctx_->trans_begin();
  wctx.mem_ctx_->sub_trans_begin(3, 1000000 + ::oceanbase::common::ObTimeUtility::current_time());
  ret = mt.set(wctx, table_schema_.get_table_id(), rk.get_rowkey().get_obj_cnt() - 3, columns, mri);
  EXPECT_EQ(OB_SUCCESS, ret);
  wctx.mem_ctx_->trans_end(true, trans_version);
  f_.free(wctx.mem_ctx_);

  ObString test_string2("hello");
  trans_version++;
  row.set_dml(T_DML_INSERT);
  row.row_val_.cells_[2].set_int(819);
  row.row_val_.cells_[0].set_varchar(test_string2);
  mri.reset();
  mri.add_row(row);
  wctx.mem_ctx_ = f_.alloc();
  wctx.mem_ctx_->trans_begin();
  wctx.mem_ctx_->sub_trans_begin(4, 1000000 + ::oceanbase::common::ObTimeUtility::current_time());
  ret = mt.set(wctx, table_schema_.get_table_id(), rk.get_rowkey().get_obj_cnt() - 3, columns, mri);
  EXPECT_EQ(OB_SUCCESS, ret);
  wctx.mem_ctx_->trans_end(true, trans_version);
  f_.free(wctx.mem_ctx_);

  ObQueryFlag query_flag(ObQueryFlag::Forward,
      true, /*is daily merge scan*/
      true, /*is read multiple macro block*/
      true, /*sys task scan, read one macro block in single io*/
      false /*is full row scan?*/,
      false,
      false);

  int64_t muti_version_start = 2;
  init_table_param(table_schema_, columns);

  wctx.mem_ctx_ = f_.alloc();
  wctx.mem_ctx_->trans_begin();
  wctx.mem_ctx_->sub_trans_begin(INT64_MAX - 2, INT64_MAX - 2);
  wctx.mem_ctx_->set_multi_version_start(muti_version_start);
  wctx.mem_ctx_->set_base_version(0);
  ASSERT_EQ(true, wctx.mem_ctx_->is_multi_version_range_valid());

  ObBlockCacheWorkingSet block_cache_ws;
  table_access_context_.query_flag_ = query_flag;
  table_access_context_.store_ctx_ = &wctx;
  table_access_context_.allocator_ = &allocator_;
  table_access_context_.stmt_allocator_ = &allocator_;
  table_access_context_.block_cache_ws_ = &block_cache_ws;
  table_access_context_.is_inited_ = true;  // just for test case

  range_.set_whole_range();
  range_.set_table_id(table_schema_.get_table_id());
  ObExtStoreRange ext_range(range_);
  ObStoreRowIterator* store_row_iter = NULL;

  ret = mt.scan(table_param_, table_access_context_, ext_range, store_row_iter);
  ASSERT_EQ(OB_SUCCESS, ret);
  const ObStoreRow* store_row = NULL;
  const char* result =
      " var     var   bigint bigint bigint   bigint   number dml           first_dml       flag    "
      "multi_version_row_flag\n"
      "hello    GG    -5      0     819      781      3.14  T_DML_INSERT   T_DML_INSERT    EXIST   CF\n"
      "hello    GG    -3      0     657      781      3.14  T_DML_UPDATE   T_DML_INSERT    EXIST   N\n"
      "hello    GG    -2      0     1024     781      3.14  T_DML_UPDATE   T_DML_INSERT    EXIST   CL\n"
      "world    GG    -4      0     1025     781      3.14  T_DML_INSERT   T_DML_INSERT    EXIST   CLF\n";
  uint16_t result_col_id[] = {
      100,
      101,
      7,
      8,
      102,
      103,
      104,
      100,
      101,
      7,
      8,
      102,
      103,
      104,
      100,
      101,
      7,
      8,
      102,
      103,
      104,
      100,
      101,
      7,
      8,
      102,
      103,
      104,
  };
  int64_t result_col_cnt[] = {7, 7, 7, 7};
  ObMockIterator res_iter;
  ret = res_iter.from(result, '\\', result_col_id, result_col_cnt);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(res_iter.equals(*store_row_iter, true));  // cmp multi version row flag

  wctx.mem_ctx_->trans_end(true, trans_version);
  f_.free(wctx.mem_ctx_);
}

TEST_F(TestMemtableMultiVersionRowIterator, append_muti_row_row_memtable_iter)
{
  int64_t trans_version = 1;
  static const ObPartitionKey PKEY(table_schema_.get_table_id(), 1, 1);
  ObMemtable mt;
  ObMemtable replayed_mt;
  int ret = OB_SUCCESS;
  storage::ObITable::TableKey table_key;
  table_key.table_type_ = storage::ObITable::MEMTABLE;
  table_key.pkey_ = PKEY;
  table_key.table_id_ = PKEY.table_id_;
  table_key.version_ = 1;
  table_key.trans_version_range_.base_version_ = 0;
  table_key.trans_version_range_.multi_version_start_ = 0;
  table_key.trans_version_range_.snapshot_version_ = INT64_MAX - 2;

  ret = mt.init(table_key);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = replayed_mt.init(table_key);
  EXPECT_EQ(OB_SUCCESS, ret);

  storage::ObStoreCtx wctx;
  storage::ObStoreCtx rctx;

  ObArray<ObITable*> array;
  wctx.tables_ = &array;

  CD cd(100,
      ObVarcharType,
      CS_TYPE_UTF8MB4_GENERAL_CI,
      101,
      ObVarcharType,
      CS_TYPE_UTF8MB4_GENERAL_CI,
      102,
      ObIntType,
      CS_TYPE_UTF8MB4_BIN,
      103,
      ObNumberType,
      CS_TYPE_UTF8MB4_BIN);
  const ObIArray<share::schema::ObColDesc>& columns = cd.get_columns();

  ObMtRowIterator mri;
  RK rk(V("hello", 5, CS_TYPE_UTF8MB4_GENERAL_CI), V("GG", 2, CS_TYPE_UTF8MB4_GENERAL_CI), I(1024), N("3.14"));
  ObStoreRow row;
  row.row_val_.cells_ = const_cast<ObObj*>(rk.get_rowkey().get_obj_ptr());
  row.row_val_.count_ = rk.get_rowkey().get_obj_cnt();
  row.set_dml(T_DML_INSERT);
  mri.add_row(row);
  wctx.mem_ctx_ = f_.alloc();
  wctx.mem_ctx_->trans_begin();
  wctx.mem_ctx_->sub_trans_begin(0, 1000000 + ::oceanbase::common::ObTimeUtility::current_time());
  ret = mt.set(wctx, table_schema_.get_table_id(), rk.get_rowkey().get_obj_cnt() - 2, columns, mri);
  EXPECT_EQ(OB_SUCCESS, ret);
  wctx.mem_ctx_->trans_end(true, trans_version);
  f_.free(wctx.mem_ctx_);

  trans_version++;
  row.set_dml(T_DML_UPDATE);
  mri.reset();
  mri.add_row(row);
  wctx.mem_ctx_ = f_.alloc();
  wctx.mem_ctx_->trans_begin();
  wctx.mem_ctx_->sub_trans_begin(1, 1000000 + ::oceanbase::common::ObTimeUtility::current_time());
  ret = mt.set(wctx, table_schema_.get_table_id(), rk.get_rowkey().get_obj_cnt() - 2, columns, mri);
  EXPECT_EQ(OB_SUCCESS, ret);
  wctx.mem_ctx_->trans_end(true, trans_version);
  f_.free(wctx.mem_ctx_);

  trans_version++;
  row.set_dml(T_DML_UPDATE);
  mri.reset();
  mri.add_row(row);
  wctx.mem_ctx_ = f_.alloc();
  wctx.mem_ctx_->trans_begin();
  wctx.mem_ctx_->sub_trans_begin(2, 1000000 + ::oceanbase::common::ObTimeUtility::current_time());
  ret = mt.set(wctx, table_schema_.get_table_id(), rk.get_rowkey().get_obj_cnt() - 2, columns, mri);
  EXPECT_EQ(OB_SUCCESS, ret);
  wctx.mem_ctx_->trans_end(true, trans_version);
  f_.free(wctx.mem_ctx_);

  ObString test_string("world");
  trans_version++;
  row.set_dml(T_DML_INSERT);
  row.row_val_.cells_[2].set_int(1025);
  row.row_val_.cells_[0].set_varchar(test_string);
  mri.reset();
  mri.add_row(row);
  wctx.mem_ctx_ = f_.alloc();
  wctx.mem_ctx_->trans_begin();
  wctx.mem_ctx_->sub_trans_begin(3, 1000000 + ::oceanbase::common::ObTimeUtility::current_time());
  ret = mt.set(wctx, table_schema_.get_table_id(), rk.get_rowkey().get_obj_cnt() - 2, columns, mri);
  EXPECT_EQ(OB_SUCCESS, ret);
  wctx.mem_ctx_->trans_end(true, trans_version);
  f_.free(wctx.mem_ctx_);

  ObQueryFlag query_flag(ObQueryFlag::Forward,
      true, /*is daily merge scan*/
      true, /*is read multiple macro block*/
      true, /*sys task scan, read one macro block in single io*/
      false /*is full row scan?*/,
      false,
      false);

  int64_t muti_version_start = 2;
  init_table_param(table_schema_, columns);

  wctx.mem_ctx_ = f_.alloc();
  wctx.mem_ctx_->trans_begin();
  wctx.mem_ctx_->sub_trans_begin(INT64_MAX - 2, INT64_MAX - 2);
  wctx.mem_ctx_->set_multi_version_start(muti_version_start);
  wctx.mem_ctx_->set_base_version(0);
  ASSERT_EQ(true, wctx.mem_ctx_->is_multi_version_range_valid());

  ObBlockCacheWorkingSet block_cache_ws;
  table_access_context_.query_flag_ = query_flag;
  table_access_context_.store_ctx_ = &wctx;
  table_access_context_.allocator_ = &allocator_;
  table_access_context_.stmt_allocator_ = &allocator_;
  table_access_context_.block_cache_ws_ = &block_cache_ws;
  table_access_context_.is_inited_ = true;
  range_.set_whole_range();
  range_.set_table_id(table_schema_.get_table_id());
  ObExtStoreRange ext_range(range_);
  ObStoreRowIterator* store_row_iter = NULL;

  ret = mt.scan(table_param_, table_access_context_, ext_range, store_row_iter);
  ASSERT_EQ(OB_SUCCESS, ret);
  const ObStoreRow* store_row = NULL;
  const char* result =
      " var     var   bigint bigint bigint  number   dml        first_dml       flag    multi_version_row_flag\n"
      "hello    GG    -3      0     1024    3.14  T_DML_UPDATE   T_DML_INSERT    EXIST   CF\n"
      "hello    GG    -2      0     1024    3.14  T_DML_UPDATE   T_DML_INSERT    EXIST   CL\n"
      "world    GG    -4      0     1025    3.14  T_DML_INSERT   T_DML_INSERT    EXIST   CLF\n";
  uint16_t result_col_id[] = {
      100,
      101,
      7,
      8,
      102,
      103,
      100,
      101,
      7,
      8,
      102,
      103,
      100,
      101,
      7,
      8,
      102,
      103,
  };
  int64_t result_col_cnt[] = {6, 6, 6, 6};
  ObMockIterator res_iter;
  ret = res_iter.from(result, '\\', result_col_id, result_col_cnt);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(res_iter.equals(*store_row_iter, true));  // cmp multi version row flag
  wctx.mem_ctx_->trans_end(true, trans_version);
  f_.free(wctx.mem_ctx_);
}

TEST_F(TestMemtableMultiVersionRowIterator, append_and_update_row_in_one_trans_id_memtable_iter)
{
  int64_t trans_version = 1;
  static const ObPartitionKey PKEY(table_schema_.get_table_id(), 1, 1);
  ObMemtable mt;
  ObMemtable replayed_mt;
  int ret = OB_SUCCESS;
  storage::ObITable::TableKey table_key;
  table_key.table_type_ = storage::ObITable::MEMTABLE;
  table_key.pkey_ = PKEY;
  table_key.table_id_ = PKEY.table_id_;
  table_key.version_ = 1;
  table_key.trans_version_range_.base_version_ = 0;
  table_key.trans_version_range_.multi_version_start_ = 0;
  table_key.trans_version_range_.snapshot_version_ = INT64_MAX - 2;

  ret = mt.init(table_key);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = replayed_mt.init(table_key);
  EXPECT_EQ(OB_SUCCESS, ret);

  storage::ObStoreCtx wctx;
  storage::ObStoreCtx rctx;

  ObArray<ObITable*> array;
  wctx.tables_ = &array;

  CD cd(100,
      ObVarcharType,
      CS_TYPE_UTF8MB4_GENERAL_CI,
      101,
      ObVarcharType,
      CS_TYPE_UTF8MB4_BIN,
      102,
      ObIntType,
      CS_TYPE_UTF8MB4_BIN,
      103,
      ObNumberType,
      CS_TYPE_UTF8MB4_BIN);
  const ObIArray<share::schema::ObColDesc>& columns = cd.get_columns();

  ObMtRowIterator mri;
  RK rk(V("hello", 5, CS_TYPE_UTF8MB4_GENERAL_CI), V(NULL, 0), I(1024), N("3.14"));
  ObStoreRow row;
  row.row_val_.cells_ = const_cast<ObObj*>(rk.get_rowkey().get_obj_ptr());
  row.row_val_.count_ = rk.get_rowkey().get_obj_cnt();
  row.set_dml(T_DML_INSERT);
  mri.add_row(row);
  wctx.mem_ctx_ = f_.alloc();
  wctx.mem_ctx_->trans_begin();
  wctx.mem_ctx_->sub_trans_begin(0, 1000000 + ::oceanbase::common::ObTimeUtility::current_time());
  ret = mt.set(wctx, table_schema_.get_table_id(), rk.get_rowkey().get_obj_cnt() - 2, columns, mri);
  EXPECT_EQ(OB_SUCCESS, ret);
  wctx.mem_ctx_->trans_end(true, trans_version);
  f_.free(wctx.mem_ctx_);

  // trans_version++;
  row.set_dml(T_DML_UPDATE);
  mri.reset();
  mri.add_row(row);
  wctx.mem_ctx_ = f_.alloc();
  wctx.mem_ctx_->trans_begin();
  wctx.mem_ctx_->sub_trans_begin(1, 1000000 + ::oceanbase::common::ObTimeUtility::current_time());
  ret = mt.set(wctx, table_schema_.get_table_id(), rk.get_rowkey().get_obj_cnt() - 2, columns, mri);
  EXPECT_EQ(OB_SUCCESS, ret);
  wctx.mem_ctx_->trans_end(true, trans_version);
  f_.free(wctx.mem_ctx_);

  // trans_version++;
  row.set_dml(T_DML_UPDATE);
  mri.reset();
  mri.add_row(row);
  wctx.mem_ctx_ = f_.alloc();
  wctx.mem_ctx_->trans_begin();
  wctx.mem_ctx_->sub_trans_begin(2, 1000000 + ::oceanbase::common::ObTimeUtility::current_time());
  ret = mt.set(wctx, table_schema_.get_table_id(), rk.get_rowkey().get_obj_cnt() - 2, columns, mri);
  EXPECT_EQ(OB_SUCCESS, ret);
  wctx.mem_ctx_->trans_end(true, trans_version);
  f_.free(wctx.mem_ctx_);

  ObQueryFlag query_flag(ObQueryFlag::Forward,
      true, /*is daily merge scan*/
      true, /*is read multiple macro block*/
      true, /*sys task scan, read one macro block in single io*/
      false /*is full row scan?*/,
      false,
      false);

  int64_t muti_version_start = 1;
  init_table_param(table_schema_, columns);

  wctx.mem_ctx_ = f_.alloc();
  wctx.mem_ctx_->trans_begin();
  wctx.mem_ctx_->sub_trans_begin(INT64_MAX - 2, INT64_MAX - 2);
  wctx.mem_ctx_->set_multi_version_start(muti_version_start);
  wctx.mem_ctx_->set_base_version(0);
  ASSERT_EQ(true, wctx.mem_ctx_->is_multi_version_range_valid());

  ObBlockCacheWorkingSet block_cache_ws;
  table_access_context_.query_flag_ = query_flag;
  table_access_context_.store_ctx_ = &wctx;
  table_access_context_.allocator_ = &allocator_;
  table_access_context_.stmt_allocator_ = &allocator_;
  table_access_context_.block_cache_ws_ = &block_cache_ws;
  table_access_context_.is_inited_ = true;  // just for test case

  range_.set_whole_range();
  range_.set_table_id(table_schema_.get_table_id());
  ObExtStoreRange ext_range(range_);
  ObStoreRowIterator* store_row_iter = NULL;

  ret = mt.scan(table_param_, table_access_context_, ext_range, store_row_iter);
  // ret = uncompacted_row_iter_.init(table_access_param_, table_access_context_, mvcc_engine_, range_);
  ASSERT_EQ(OB_SUCCESS, ret);
  const ObStoreRow* store_row = NULL;
  // ret = uncompacted_row_iter_.get_next_row(store_row);
  ret = store_row_iter->get_next_row(store_row);
  ASSERT_EQ(OB_SUCCESS, ret);
  STORAGE_LOG(WARN, "store_row is", K(*store_row), K(store_row->row_type_flag_));

  ret = store_row_iter->get_next_row(store_row);
  ASSERT_EQ(OB_ITER_END, ret);
  wctx.mem_ctx_->trans_end(true, trans_version);
  f_.free(wctx.mem_ctx_);
}

TEST_F(TestMemtableMultiVersionRowIterator, append_and_delete_and_append_row_memtable_iter)
{
  int64_t trans_version = 1;
  static const ObPartitionKey PKEY(table_schema_.get_table_id(), 1, 1);
  ObMemtable mt;
  ObMemtable replayed_mt;
  int ret = OB_SUCCESS;
  storage::ObITable::TableKey table_key;
  table_key.table_type_ = storage::ObITable::MEMTABLE;
  table_key.pkey_ = PKEY;
  table_key.table_id_ = PKEY.table_id_;
  table_key.version_ = 1;
  table_key.trans_version_range_.base_version_ = 0;
  table_key.trans_version_range_.multi_version_start_ = 0;
  table_key.trans_version_range_.snapshot_version_ = INT64_MAX - 2;

  ret = mt.init(table_key);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = replayed_mt.init(table_key);
  EXPECT_EQ(OB_SUCCESS, ret);

  storage::ObStoreCtx wctx;
  storage::ObStoreCtx rctx;

  ObArray<ObITable*> array;
  wctx.tables_ = &array;

  CD cd(100,
      ObVarcharType,
      CS_TYPE_UTF8MB4_GENERAL_CI,
      101,
      ObVarcharType,
      CS_TYPE_UTF8MB4_BIN,
      102,
      ObIntType,
      CS_TYPE_UTF8MB4_BIN,
      103,
      ObNumberType,
      CS_TYPE_UTF8MB4_BIN);
  const ObIArray<share::schema::ObColDesc>& columns = cd.get_columns();

  ObMtRowIterator mri;
  RK rk(V("hello", 5, CS_TYPE_UTF8MB4_GENERAL_CI), V(NULL, 0), I(1024), N("3.14"));
  ObStoreRow row;
  row.row_val_.cells_ = const_cast<ObObj*>(rk.get_rowkey().get_obj_ptr());
  row.row_val_.count_ = rk.get_rowkey().get_obj_cnt();
  row.set_dml(T_DML_INSERT);
  mri.add_row(row);
  wctx.mem_ctx_ = f_.alloc();
  wctx.mem_ctx_->trans_begin();
  wctx.mem_ctx_->sub_trans_begin(0, 1000000 + ::oceanbase::common::ObTimeUtility::current_time());
  ret = mt.set(wctx, table_schema_.get_table_id(), rk.get_rowkey().get_obj_cnt() - 2, columns, mri);
  EXPECT_EQ(OB_SUCCESS, ret);
  wctx.mem_ctx_->trans_end(true, trans_version);
  f_.free(wctx.mem_ctx_);

  trans_version++;
  row.set_dml(T_DML_UPDATE);
  mri.reset();
  mri.add_row(row);
  wctx.mem_ctx_ = f_.alloc();
  wctx.mem_ctx_->trans_begin();
  wctx.mem_ctx_->sub_trans_begin(1, 1000000 + ::oceanbase::common::ObTimeUtility::current_time());
  ret = mt.set(wctx, table_schema_.get_table_id(), rk.get_rowkey().get_obj_cnt() - 2, columns, mri);
  EXPECT_EQ(OB_SUCCESS, ret);
  wctx.mem_ctx_->trans_end(true, trans_version);
  f_.free(wctx.mem_ctx_);

  trans_version++;
  row.set_dml(T_DML_DELETE);
  mri.reset();
  mri.add_row(row);
  wctx.mem_ctx_ = f_.alloc();
  wctx.mem_ctx_->trans_begin();
  wctx.mem_ctx_->sub_trans_begin(2, 1000000 + ::oceanbase::common::ObTimeUtility::current_time());
  ret = mt.set(wctx, table_schema_.get_table_id(), rk.get_rowkey().get_obj_cnt() - 2, columns, mri);
  EXPECT_EQ(OB_SUCCESS, ret);
  wctx.mem_ctx_->trans_end(true, trans_version);
  f_.free(wctx.mem_ctx_);

  trans_version++;
  row.row_val_.cells_ = const_cast<ObObj*>(rk.get_rowkey().get_obj_ptr());
  row.row_val_.count_ = rk.get_rowkey().get_obj_cnt();
  row.set_dml(T_DML_INSERT);
  mri.add_row(row);
  wctx.mem_ctx_ = f_.alloc();
  wctx.mem_ctx_->trans_begin();
  wctx.mem_ctx_->sub_trans_begin(3, 1000000 + ::oceanbase::common::ObTimeUtility::current_time());
  ret = mt.set(wctx, table_schema_.get_table_id(), rk.get_rowkey().get_obj_cnt() - 2, columns, mri);
  EXPECT_EQ(OB_SUCCESS, ret);
  wctx.mem_ctx_->trans_end(true, trans_version);
  f_.free(wctx.mem_ctx_);

  trans_version++;
  row.set_dml(T_DML_UPDATE);
  mri.reset();
  mri.add_row(row);
  wctx.mem_ctx_ = f_.alloc();
  wctx.mem_ctx_->trans_begin();
  wctx.mem_ctx_->sub_trans_begin(4, 1000000 + ::oceanbase::common::ObTimeUtility::current_time());
  ret = mt.set(wctx, table_schema_.get_table_id(), rk.get_rowkey().get_obj_cnt() - 2, columns, mri);
  EXPECT_EQ(OB_SUCCESS, ret);
  wctx.mem_ctx_->trans_end(true, trans_version);
  f_.free(wctx.mem_ctx_);

  ObQueryFlag query_flag(ObQueryFlag::Forward,
      true, /*is daily merge scan*/
      true, /*is read multiple macro block*/
      true, /*sys task scan, read one macro block in single io*/
      false /*is full row scan?*/,
      false,
      false);
  int64_t muti_version_start = 1;
  init_table_param(table_schema_, columns);

  wctx.mem_ctx_ = f_.alloc();
  wctx.mem_ctx_->trans_begin();
  wctx.mem_ctx_->sub_trans_begin(INT64_MAX - 2, INT64_MAX - 2);
  wctx.mem_ctx_->set_multi_version_start(muti_version_start);
  wctx.mem_ctx_->set_base_version(0);
  ASSERT_EQ(true, wctx.mem_ctx_->is_multi_version_range_valid());

  ObBlockCacheWorkingSet block_cache_ws;
  table_access_context_.query_flag_ = query_flag;
  table_access_context_.store_ctx_ = &wctx;
  table_access_context_.allocator_ = &allocator_;
  table_access_context_.stmt_allocator_ = &allocator_;
  table_access_context_.block_cache_ws_ = &block_cache_ws;
  table_access_context_.is_inited_ = true;  // just for test case

  range_.set_whole_range();
  range_.set_table_id(table_schema_.get_table_id());
  ObExtStoreRange ext_range(range_);
  ObStoreRowIterator* store_row_iter = NULL;

  ret = mt.scan(table_param_, table_access_context_, ext_range, store_row_iter);
  // ret = uncompacted_row_iter_.init(table_access_param_, table_access_context_, mvcc_engine_, range_);
  ASSERT_EQ(OB_SUCCESS, ret);
  const ObStoreRow* store_row = NULL;
  ret = store_row_iter->get_next_row(store_row);
  ASSERT_EQ(OB_SUCCESS, ret);
  STORAGE_LOG(WARN, "store_row is", K(*store_row), K(store_row->row_type_flag_));

  ret = store_row_iter->get_next_row(store_row);
  ASSERT_EQ(OB_SUCCESS, ret);
  STORAGE_LOG(WARN, "store_row is", K(*store_row), K(store_row->row_type_flag_));

  ret = store_row_iter->get_next_row(store_row);
  ASSERT_EQ(OB_SUCCESS, ret);
  STORAGE_LOG(WARN, "store_row is", K(*store_row), K(store_row->row_type_flag_));

  ret = store_row_iter->get_next_row(store_row);
  ASSERT_EQ(OB_SUCCESS, ret);
  STORAGE_LOG(WARN, "store_row is", K(*store_row), K(store_row->row_type_flag_));

  ret = store_row_iter->get_next_row(store_row);
  ASSERT_EQ(OB_SUCCESS, ret);
  STORAGE_LOG(WARN, "store_row is", K(*store_row), K(store_row->row_type_flag_));

  ret = store_row_iter->get_next_row(store_row);
  ASSERT_EQ(OB_ITER_END, ret);

  wctx.mem_ctx_->trans_end(true, trans_version);
  f_.free(wctx.mem_ctx_);
}

TEST_F(TestMemtableMultiVersionRowIterator, mix_committed_and_uncommitted_memtable_iter)
{
  int64_t trans_version = 1;
  static const ObPartitionKey PKEY(table_schema_.get_table_id(), 1, 1);
  ObMemtable mt;
  ObMemtable replayed_mt;
  int ret = OB_SUCCESS;
  storage::ObITable::TableKey table_key;
  table_key.table_type_ = storage::ObITable::MEMTABLE;
  table_key.pkey_ = PKEY;
  table_key.table_id_ = PKEY.table_id_;
  table_key.version_ = 1;
  table_key.trans_version_range_.base_version_ = 0;
  table_key.trans_version_range_.multi_version_start_ = 0;
  table_key.trans_version_range_.snapshot_version_ = INT64_MAX - 2;

  ret = mt.init(table_key);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = replayed_mt.init(table_key);
  EXPECT_EQ(OB_SUCCESS, ret);

  storage::ObStoreCtx wctx;
  storage::ObStoreCtx rctx;

  ObArray<ObITable*> array;
  wctx.tables_ = &array;

  CD cd(100,
      ObVarcharType,
      CS_TYPE_UTF8MB4_GENERAL_CI,
      101,
      ObVarcharType,
      CS_TYPE_UTF8MB4_BIN,
      102,
      ObIntType,
      CS_TYPE_UTF8MB4_BIN,
      103,
      ObNumberType,
      CS_TYPE_UTF8MB4_BIN);
  const ObIArray<share::schema::ObColDesc>& columns = cd.get_columns();

  ObMtRowIterator mri;
  RK rk(V("hello", 5, CS_TYPE_UTF8MB4_GENERAL_CI), V(NULL, 0), I(1024), N("3.14"));
  ObStoreRow row;
  row.row_val_.cells_ = const_cast<ObObj*>(rk.get_rowkey().get_obj_ptr());
  row.row_val_.count_ = rk.get_rowkey().get_obj_cnt();
  row.set_dml(T_DML_INSERT);
  mri.add_row(row);
  wctx.mem_ctx_ = f_.alloc();
  wctx.mem_ctx_->trans_begin();
  wctx.mem_ctx_->sub_trans_begin(0, 1000000 + ::oceanbase::common::ObTimeUtility::current_time());
  ret = mt.set(wctx, table_schema_.get_table_id(), rk.get_rowkey().get_obj_cnt() - 2, columns, mri);
  EXPECT_EQ(OB_SUCCESS, ret);
  wctx.mem_ctx_->trans_end(true, trans_version);
  f_.free(wctx.mem_ctx_);

  trans_version++;
  row.set_dml(T_DML_UPDATE);
  row.row_val_.cells_[2].set_int(888);
  mri.reset();
  mri.add_row(row);
  wctx.mem_ctx_ = f_.alloc();
  wctx.mem_ctx_->trans_begin();
  wctx.mem_ctx_->sub_trans_begin(1, 1000000 + ::oceanbase::common::ObTimeUtility::current_time());
  ret = mt.set(wctx, table_schema_.get_table_id(), rk.get_rowkey().get_obj_cnt() - 2, columns, mri);
  EXPECT_EQ(OB_SUCCESS, ret);
  // wctx.mem_ctx_->trans_end(true, trans_version);
  // f_.free(wctx.mem_ctx_);

  // trans_version++;
  row.set_dml(T_DML_UPDATE);
  row.row_val_.cells_[2].set_int(666);
  mri.reset();
  mri.add_row(row);
  // wctx.mem_ctx_ = f_.alloc();
  // wctx.mem_ctx_->trans_begin();
  wctx.mem_ctx_->sub_trans_begin(2, 1000000 + ::oceanbase::common::ObTimeUtility::current_time());
  ret = mt.set(wctx, table_schema_.get_table_id(), rk.get_rowkey().get_obj_cnt() - 2, columns, mri);
  EXPECT_EQ(OB_SUCCESS, ret);
  // wctx.mem_ctx_->trans_end(true, trans_version);
  // f_.free(wctx.mem_ctx_);

  ObQueryFlag query_flag(ObQueryFlag::Forward,
      true, /*is daily merge scan*/
      true, /*is read multiple macro block*/
      true, /*sys task scan, read one macro block in single io*/
      false /*is full row scan?*/,
      false,
      false);
  int64_t muti_version_start = 1;
  init_table_param(table_schema_, columns);

  rctx.mem_ctx_ = f_.alloc();
  rctx.mem_ctx_->trans_begin();
  rctx.mem_ctx_->sub_trans_begin(INT64_MAX - 2, INT64_MAX - 2);
  rctx.mem_ctx_->set_multi_version_start(muti_version_start);
  rctx.mem_ctx_->set_base_version(0);
  ASSERT_EQ(true, rctx.mem_ctx_->is_multi_version_range_valid());

  ObBlockCacheWorkingSet block_cache_ws;
  table_access_context_.query_flag_ = query_flag;
  table_access_context_.store_ctx_ = &rctx;
  table_access_context_.allocator_ = &allocator_;
  table_access_context_.stmt_allocator_ = &allocator_;
  table_access_context_.block_cache_ws_ = &block_cache_ws;
  table_access_context_.is_inited_ = true;

  range_.set_whole_range();
  range_.set_table_id(table_schema_.get_table_id());
  ObExtStoreRange ext_range(range_);
  ObStoreRowIterator* store_row_iter = NULL;

  ret = mt.scan(table_param_, table_access_context_, ext_range, store_row_iter);
  ASSERT_EQ(OB_SUCCESS, ret);
  const ObStoreRow* store_row = NULL;
  ret = store_row_iter->get_next_row(store_row);
  STORAGE_LOG(WARN, "store row is", K(store_row));
  ASSERT_EQ(OB_SUCCESS, ret);
  rctx.mem_ctx_->trans_end(true, trans_version);
  f_.free(rctx.mem_ctx_);
}

TEST_F(TestMemtableMultiVersionRowIterator, append_and_update_rows_uncommitted_memtable_iter)
{
  int64_t trans_version = 1;
  static const ObPartitionKey PKEY(table_schema_.get_table_id(), 1, 1);
  ObMemtable mt;
  ObMemtable replayed_mt;
  int ret = OB_SUCCESS;
  storage::ObITable::TableKey table_key;
  table_key.table_type_ = storage::ObITable::MEMTABLE;
  table_key.pkey_ = PKEY;
  table_key.table_id_ = PKEY.table_id_;
  table_key.version_ = 1;
  table_key.trans_version_range_.base_version_ = 0;
  table_key.trans_version_range_.multi_version_start_ = 0;
  table_key.trans_version_range_.snapshot_version_ = INT64_MAX - 2;

  ret = mt.init(table_key);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = replayed_mt.init(table_key);
  EXPECT_EQ(OB_SUCCESS, ret);

  storage::ObStoreCtx wctx;
  storage::ObStoreCtx rctx;

  ObArray<ObITable*> array;
  wctx.tables_ = &array;

  CD cd(100,
      ObVarcharType,
      CS_TYPE_UTF8MB4_GENERAL_CI,
      101,
      ObVarcharType,
      CS_TYPE_UTF8MB4_BIN,
      102,
      ObIntType,
      CS_TYPE_UTF8MB4_BIN,
      103,
      ObNumberType,
      CS_TYPE_UTF8MB4_BIN);
  const ObIArray<share::schema::ObColDesc>& columns = cd.get_columns();

  ObMtRowIterator mri;
  RK rk(V("hello", 5, CS_TYPE_UTF8MB4_GENERAL_CI), V(NULL, 0), I(1024), N("3.14"));
  ObStoreRow row;
  row.row_val_.cells_ = const_cast<ObObj*>(rk.get_rowkey().get_obj_ptr());
  row.row_val_.count_ = rk.get_rowkey().get_obj_cnt();
  row.set_dml(T_DML_INSERT);
  mri.add_row(row);
  wctx.mem_ctx_ = f_.alloc();
  wctx.mem_ctx_->trans_begin();
  wctx.mem_ctx_->sub_trans_begin(0, 1000000 + ::oceanbase::common::ObTimeUtility::current_time());
  ret = mt.set(wctx, table_schema_.get_table_id(), rk.get_rowkey().get_obj_cnt() - 2, columns, mri);
  EXPECT_EQ(OB_SUCCESS, ret);
  // wctx.mem_ctx_->trans_end(true, trans_version);
  // f_.free(wctx.mem_ctx_);

  // trans_version++;
  row.set_dml(T_DML_UPDATE);
  mri.reset();
  mri.add_row(row);
  // wctx.mem_ctx_ = f_.alloc();
  // wctx.mem_ctx_->trans_begin();
  wctx.mem_ctx_->sub_trans_begin(1, 1000000 + ::oceanbase::common::ObTimeUtility::current_time());
  ret = mt.set(wctx, table_schema_.get_table_id(), rk.get_rowkey().get_obj_cnt() - 2, columns, mri);
  EXPECT_EQ(OB_SUCCESS, ret);
  // wctx.mem_ctx_->trans_end(true, trans_version);
  // f_.free(wctx.mem_ctx_);

  // trans_version++;
  row.set_dml(T_DML_UPDATE);
  mri.reset();
  mri.add_row(row);
  // wctx.mem_ctx_ = f_.alloc();
  // wctx.mem_ctx_->trans_begin();
  wctx.mem_ctx_->sub_trans_begin(2, 1000000 + ::oceanbase::common::ObTimeUtility::current_time());
  ret = mt.set(wctx, table_schema_.get_table_id(), rk.get_rowkey().get_obj_cnt() - 2, columns, mri);
  EXPECT_EQ(OB_SUCCESS, ret);
  // wctx.mem_ctx_->trans_end(true, trans_version);
  f_.free(wctx.mem_ctx_);

  ObQueryFlag query_flag(ObQueryFlag::Forward,
      true, /*is daily merge scan*/
      true, /*is read multiple macro block*/
      true, /*sys task scan, read one macro block in single io*/
      false /*is full row scan?*/,
      false,
      false);
  int64_t muti_version_start = 1;
  init_table_param(table_schema_, columns);

  wctx.mem_ctx_ = f_.alloc();
  wctx.mem_ctx_->trans_begin();
  wctx.mem_ctx_->sub_trans_begin(INT64_MAX - 2, INT64_MAX - 2);
  wctx.mem_ctx_->set_multi_version_start(muti_version_start);
  wctx.mem_ctx_->set_base_version(0);
  ASSERT_EQ(true, wctx.mem_ctx_->is_multi_version_range_valid());

  ObBlockCacheWorkingSet block_cache_ws;
  table_access_context_.query_flag_ = query_flag;
  table_access_context_.store_ctx_ = &wctx;
  table_access_context_.allocator_ = &allocator_;
  table_access_context_.stmt_allocator_ = &allocator_;
  table_access_context_.block_cache_ws_ = &block_cache_ws;
  table_access_context_.is_inited_ = true;  // just for test case

  range_.set_whole_range();
  range_.set_table_id(table_schema_.get_table_id());
  ObExtStoreRange ext_range(range_);
  ObStoreRowIterator* store_row_iter = NULL;

  ret = mt.scan(table_param_, table_access_context_, ext_range, store_row_iter);
  ASSERT_EQ(OB_SUCCESS, ret);
  const ObStoreRow* store_row = NULL;

  ret = store_row_iter->get_next_row(store_row);
  ASSERT_EQ(OB_ITER_END, ret);
  wctx.mem_ctx_->trans_end(true, trans_version);
  f_.free(wctx.mem_ctx_);
}
/*
TEST_F(TestMemtableMultiVersionRowIterator, append_compact_muti_row_memtable_iter)
{
  int64_t trans_version = 1;
  static const ObPartitionKey PKEY(table_schema_.get_table_id(), 1, 1);
  ObMemtable mt;
  ObMemtable replayed_mt;
  int ret = OB_SUCCESS;
  storage::ObITable::TableKey table_key;
  table_key.table_type_ = storage::ObITable::MEMTABLE;
  table_key.pkey_ = PKEY;
  table_key.table_id_ = PKEY.table_id_;
  table_key.version_ = 1;
  table_key.trans_version_range_.base_version_ = 0;
  table_key.trans_version_range_.multi_version_start_ = 0;
  table_key.trans_version_range_.snapshot_version_ = INT64_MAX - 2;

  ret = mt.init(table_key);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = replayed_mt.init(table_key);
  EXPECT_EQ(OB_SUCCESS, ret);

  storage::ObStoreCtx wctx;
  storage::ObStoreCtx rctx;

  ObArray<ObITable *> array;
  wctx.tables_ = &array;

  CD cd(
    100, ObVarcharType, CS_TYPE_UTF8MB4_GENERAL_CI,
    101, ObVarcharType, CS_TYPE_UTF8MB4_BIN,
    102, ObIntType, CS_TYPE_UTF8MB4_BIN,
    103, ObNumberType, CS_TYPE_UTF8MB4_BIN
    );
  const ObIArray<share::schema::ObColDesc> &columns = cd.get_columns();

  ObMtRowIterator mri;
  RK rk(
    V("hello", 5),
    V(NULL, 0),
    I(1024),
    N("3.14")
    );
  ObStoreRow row;
  row.row_val_.cells_ = const_cast<ObObj *>(rk.get_rowkey().get_obj_ptr());
  row.row_val_.count_ = rk.get_rowkey().get_obj_cnt();
  row.set_dml(T_DML_INSERT);

  int64_t start = 0;
  for (trans_version = 1; trans_version < 20; ++trans_version) {
    mri.add_row(row);
    wctx.mem_ctx_ = f_.alloc();
    wctx.mem_ctx_->trans_begin();
    wctx.mem_ctx_->sub_trans_begin(start, 1000000 + ::oceanbase::common::ObTimeUtility::current_time());
    ret = mt.set(wctx, table_schema_.get_table_id(), rk.get_rowkey().get_obj_cnt() - 2, columns, mri);
    EXPECT_EQ(OB_SUCCESS, ret);
    wctx.mem_ctx_->trans_end(true, trans_version);
    f_.free(wctx.mem_ctx_);
    row.set_dml(T_DML_UPDATE);
    ++start;
    mri.reset();
  }

  RK get_rk(V("hello", 5), V(NULL, 0));
  ObStoreRowkey gg = get_rk.get_rowkey();
  ObMemtableKey get_mk(table_schema_.get_table_id(), &gg);
  ObMemtableKey *return_key = NULL;
  ++start;
  wctx.mem_ctx_ = f_.alloc();
  wctx.mem_ctx_->trans_begin();
  wctx.mem_ctx_->sub_trans_begin(start, 1000000 + ::oceanbase::common::ObTimeUtility::current_time());
  ObMvccEngine &mvcc_engine = mt.get_mvcc_engine();
  ObMvccValueIterator value_iter;
  transaction::ObTransSnapInfo snapshot_info;
  ret = mvcc_engine.get(*wctx.mem_ctx_, snapshot_info, false, false, &get_mk, return_key, value_iter);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(NULL != return_key);
  wctx.mem_ctx_->trans_end(true, trans_version);
  f_.free(wctx.mem_ctx_);

  trans_version++;
  ++start;
  row.set_dml(T_DML_UPDATE);
  mri.reset();
  mri.add_row(row);
  wctx.mem_ctx_ = f_.alloc();
  wctx.mem_ctx_->trans_begin();
  wctx.mem_ctx_->sub_trans_begin(start, 1000000 + ::oceanbase::common::ObTimeUtility::current_time());
  ret = mt.set(wctx, table_schema_.get_table_id(), rk.get_rowkey().get_obj_cnt() - 2, columns, mri);
  EXPECT_EQ(OB_SUCCESS, ret);
  wctx.mem_ctx_->trans_end(true, trans_version);
  f_.free(wctx.mem_ctx_);

  ObString test_string("world");
  trans_version++;
  ++start;
  row.set_dml(T_DML_INSERT);
  row.row_val_.cells_[2].set_int(1025);
  row.row_val_.cells_[0].set_varchar(test_string);
  mri.reset();
  mri.add_row(row);
  wctx.mem_ctx_ = f_.alloc();
  wctx.mem_ctx_->trans_begin();
  wctx.mem_ctx_->sub_trans_begin(start, 1000000 + ::oceanbase::common::ObTimeUtility::current_time());
  ret = mt.set(wctx, table_schema_.get_table_id(), rk.get_rowkey().get_obj_cnt() - 2, columns, mri);
  EXPECT_EQ(OB_SUCCESS, ret);
  wctx.mem_ctx_->trans_end(true, trans_version);
  f_.free(wctx.mem_ctx_);

  ObQueryFlag query_flag(ObQueryFlag::Forward,
                             true, //is daily merge scan
                             true, //is read multiple macro block
                             true, //sys task scan, read one macro block in single io
                             false, //is full row scan?
                             false,
                             false);
  int64_t muti_version_start = 1;
  init_table_param(table_schema_, columns);

  wctx.mem_ctx_ = f_.alloc();
  wctx.mem_ctx_->trans_begin();
  wctx.mem_ctx_->sub_trans_begin(INT64_MAX - 2 , INT64_MAX - 2);
  wctx.mem_ctx_->set_multi_version_start(muti_version_start);
  wctx.mem_ctx_->set_base_version(0);
  ASSERT_EQ(true, wctx.mem_ctx_->is_multi_version_range_valid());

  ObBlockCacheWorkingSet block_cache_ws;
  table_access_context_.query_flag_ = query_flag;
  table_access_context_.store_ctx_ = &wctx;
  table_access_context_.allocator_ = &allocator_;
  table_access_context_.stmt_allocator_ = &allocator_;
  table_access_context_.block_cache_ws_ = &block_cache_ws;
  table_access_context_.is_inited_ = true;//just for test case

  range_.set_whole_range();
  range_.set_table_id(table_schema_.get_table_id());
  ObExtStoreRange ext_range(range_);
  ObStoreRowIterator *store_row_iter = NULL;

  ret = mt.scan(table_param_, table_access_context_, ext_range, store_row_iter);
  //ret = uncompacted_row_iter_.init(table_access_param_, table_access_context_, mvcc_engine_, range_);
  ASSERT_EQ(OB_SUCCESS, ret);
  const ObStoreRow *store_row = NULL;
  //ret = uncompacted_row_iter_.get_next_row(store_row);
  ret = store_row_iter->get_next_row(store_row);
  ASSERT_EQ(OB_SUCCESS, ret);
  STORAGE_LOG(WARN, "store_row is", K(*store_row), K(store_row->row_type_flag_));

  ret = store_row_iter->get_next_row(store_row);
  ASSERT_EQ(OB_SUCCESS, ret);
  STORAGE_LOG(WARN, "store_row is", K(*store_row), K(store_row->row_type_flag_));

  ret = store_row_iter->get_next_row(store_row);
  ASSERT_EQ(OB_SUCCESS, ret);
  STORAGE_LOG(WARN, "store_row is", K(*store_row), K(store_row->row_type_flag_));

  ret = store_row_iter->get_next_row(store_row);
  ASSERT_EQ(OB_SUCCESS, ret);
  STORAGE_LOG(WARN, "store_row is", K(*store_row), K(store_row->row_type_flag_));

  ret = store_row_iter->get_next_row(store_row);
  ASSERT_EQ(OB_SUCCESS, ret);
  STORAGE_LOG(WARN, "store_row is", K(*store_row), K(store_row->row_type_flag_));
  wctx.mem_ctx_->trans_end(true, trans_version);
  f_.free(wctx.mem_ctx_);
}

*/
TEST_F(TestMemtableMultiVersionRowIterator, base_version)
{
  int64_t trans_version = 1;
  static const ObPartitionKey PKEY(table_schema_.get_table_id(), 1, 1);
  ObMemtable mt;
  ObMemtable replayed_mt;
  int ret = OB_SUCCESS;
  storage::ObITable::TableKey table_key;
  table_key.table_type_ = storage::ObITable::MEMTABLE;
  table_key.pkey_ = PKEY;
  table_key.table_id_ = PKEY.table_id_;
  table_key.version_ = 1;
  table_key.trans_version_range_.base_version_ = 1;
  table_key.trans_version_range_.multi_version_start_ = 0;
  table_key.trans_version_range_.snapshot_version_ = INT64_MAX - 2;

  ret = mt.init(table_key);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = replayed_mt.init(table_key);
  EXPECT_EQ(OB_SUCCESS, ret);

  storage::ObStoreCtx wctx;
  storage::ObStoreCtx rctx;

  ObArray<ObITable*> array;
  wctx.tables_ = &array;

  CD cd(100,
      ObVarcharType,
      CS_TYPE_UTF8MB4_GENERAL_CI,
      101,
      ObVarcharType,
      CS_TYPE_UTF8MB4_BIN,
      102,
      ObIntType,
      CS_TYPE_UTF8MB4_BIN,
      103,
      ObNumberType,
      CS_TYPE_UTF8MB4_BIN);
  const ObIArray<share::schema::ObColDesc>& columns = cd.get_columns();

  ObMtRowIterator mri;
  RK rk(V("hello", 5, CS_TYPE_UTF8MB4_GENERAL_CI), V(NULL, 0), I(1024), N("3.14"));
  ObStoreRow row;
  row.row_val_.cells_ = const_cast<ObObj*>(rk.get_rowkey().get_obj_ptr());
  row.row_val_.count_ = rk.get_rowkey().get_obj_cnt();
  row.set_dml(T_DML_INSERT);
  mri.add_row(row);
  wctx.mem_ctx_ = f_.alloc();
  wctx.mem_ctx_->trans_begin();
  wctx.mem_ctx_->sub_trans_begin(0, 1000000 + ::oceanbase::common::ObTimeUtility::current_time());
  ret = mt.set(wctx, table_schema_.get_table_id(), rk.get_rowkey().get_obj_cnt() - 2, columns, mri);
  EXPECT_EQ(OB_SUCCESS, ret);
  wctx.mem_ctx_->trans_end(true, trans_version);
  f_.free(wctx.mem_ctx_);

  trans_version++;
  row.set_dml(T_DML_UPDATE);
  mri.reset();
  mri.add_row(row);
  wctx.mem_ctx_ = f_.alloc();
  wctx.mem_ctx_->trans_begin();
  wctx.mem_ctx_->sub_trans_begin(1, 1000000 + ::oceanbase::common::ObTimeUtility::current_time());
  ret = mt.set(wctx, table_schema_.get_table_id(), rk.get_rowkey().get_obj_cnt() - 2, columns, mri);
  EXPECT_EQ(OB_SUCCESS, ret);
  wctx.mem_ctx_->trans_end(true, trans_version);
  f_.free(wctx.mem_ctx_);

  trans_version++;
  row.set_dml(T_DML_UPDATE);
  mri.reset();
  mri.add_row(row);
  wctx.mem_ctx_ = f_.alloc();
  wctx.mem_ctx_->trans_begin();
  wctx.mem_ctx_->sub_trans_begin(2, 1000000 + ::oceanbase::common::ObTimeUtility::current_time());
  ret = mt.set(wctx, table_schema_.get_table_id(), rk.get_rowkey().get_obj_cnt() - 2, columns, mri);
  EXPECT_EQ(OB_SUCCESS, ret);
  wctx.mem_ctx_->trans_end(true, trans_version);
  f_.free(wctx.mem_ctx_);

  ObQueryFlag query_flag(ObQueryFlag::Forward,
      true, /*is daily merge scan*/
      true, /*is read multiple macro block*/
      true, /*sys task scan, read one macro block in single io*/
      false /*is full row scan?*/,
      false,
      false);

  int64_t muti_version_start = 0;
  init_table_param(table_schema_, columns);

  wctx.mem_ctx_ = f_.alloc();
  wctx.mem_ctx_->trans_begin();
  wctx.mem_ctx_->sub_trans_begin(INT64_MAX - 2, INT64_MAX - 2);
  wctx.mem_ctx_->set_multi_version_start(muti_version_start);
  wctx.mem_ctx_->set_base_version(table_key.trans_version_range_.base_version_);
  ASSERT_EQ(true, wctx.mem_ctx_->is_multi_version_range_valid());

  ObBlockCacheWorkingSet block_cache_ws;
  table_access_context_.query_flag_ = query_flag;
  table_access_context_.store_ctx_ = &wctx;
  table_access_context_.allocator_ = &allocator_;
  table_access_context_.stmt_allocator_ = &allocator_;
  table_access_context_.block_cache_ws_ = &block_cache_ws;
  table_access_context_.is_inited_ = true;

  range_.set_whole_range();
  range_.set_table_id(table_schema_.get_table_id());
  ObExtStoreRange ext_range(range_);
  ObStoreRowIterator* store_row_iter = NULL;

  ret = mt.scan(table_param_, table_access_context_, ext_range, store_row_iter);
  // ret = uncompacted_row_iter_.init(table_access_param_, table_access_context_, mvcc_engine_, range_);
  ASSERT_EQ(OB_SUCCESS, ret);
  const ObStoreRow* store_row = NULL;
  // ret = uncompacted_row_iter_.get_next_row(store_row);
  ret = store_row_iter->get_next_row(store_row);
  ASSERT_EQ(OB_SUCCESS, ret);
  STORAGE_LOG(WARN, "store_row is", K(*store_row), K(store_row->row_type_flag_));

  ret = store_row_iter->get_next_row(store_row);
  ASSERT_EQ(OB_SUCCESS, ret);
  STORAGE_LOG(WARN, "store_row is", K(*store_row), K(store_row->row_type_flag_));
  ret = store_row_iter->get_next_row(store_row);
  ASSERT_EQ(OB_ITER_END, ret);
  wctx.mem_ctx_->trans_end(true, trans_version);
  f_.free(wctx.mem_ctx_);
}
/*
TEST_F(TestMemtableMultiVersionRowIterator, test_vp_compact_iter)
{
  int64_t trans_version = 2;
  static const ObPartitionKey PKEY(table_schema_.get_table_id(), 1, 1);
  ObMemtable mt;
  int ret = OB_SUCCESS;
  storage::ObITable::TableKey table_key;
  table_key.table_type_ = storage::ObITable::MEMTABLE;
  table_key.pkey_ = PKEY;
  table_key.table_id_ = PKEY.table_id_;
  table_key.version_ = 1;
  table_key.trans_version_range_.base_version_ = 1;
  table_key.trans_version_range_.multi_version_start_ = 0;
  table_key.trans_version_range_.snapshot_version_ = INT64_MAX - 2;

  ret = mt.init(table_key);
  EXPECT_EQ(OB_SUCCESS, ret);

  storage::ObStoreCtx wctx;
  storage::ObStoreCtx rctx;

  ObArray<ObITable *> array;
  wctx.tables_ = &array;

  CD cd(
    100, ObVarcharType, CS_TYPE_UTF8MB4_GENERAL_CI,
    101, ObVarcharType, CS_TYPE_UTF8MB4_BIN,
    102, ObIntType, CS_TYPE_UTF8MB4_BIN,
    103, ObNumberType, CS_TYPE_UTF8MB4_BIN
    );
  const ObIArray<share::schema::ObColDesc> &columns = cd.get_columns();

  const char *new_rows =
    "var    var    bigint   number    flag     dml\n"
    "a      aa     1        1.0       EXIST    INSERT\n"
    "c      nop    nop      nop       DELETE   DELETE\n";

  ObMockIterator new_row_iter;
  ASSERT_EQ(OB_SUCCESS, new_row_iter.from(new_rows));
  wctx.mem_ctx_ = f_.alloc();
  wctx.mem_ctx_->trans_begin();
  wctx.mem_ctx_->sub_trans_begin(0, 1000000 + ::oceanbase::common::ObTimeUtility::current_time());
  ret = mt.set(wctx, table_schema_.get_table_id(), 1, columns, new_row_iter);
  EXPECT_EQ(OB_SUCCESS, ret);
  wctx.mem_ctx_->trans_end(true, trans_version);
  f_.free(wctx.mem_ctx_);

  CD cd0(
    100, ObVarcharType, CS_TYPE_UTF8MB4_GENERAL_CI,
    101, ObVarcharType, CS_TYPE_UTF8MB4_BIN,
    103, ObNumberType, CS_TYPE_UTF8MB4_BIN
    );
  const ObIArray<share::schema::ObColDesc> &columns0 = cd0.get_columns();

  const char *new_rows0 =
    "var    var    number    flag     dml\n"
    "b      bb     2.0       EXIST    UPDATE\n";

  new_row_iter.reset();
  ASSERT_EQ(OB_SUCCESS, new_row_iter.from(new_rows0));
  wctx.mem_ctx_ = f_.alloc();
  wctx.mem_ctx_->trans_begin();
  wctx.mem_ctx_->sub_trans_begin(trans_version, 1000000 + ::oceanbase::common::ObTimeUtility::current_time());
  ret = mt.set(wctx, table_schema_.get_table_id(), 1, columns0, new_row_iter);
  EXPECT_EQ(OB_SUCCESS, ret);
  wctx.mem_ctx_->trans_end(true, ++trans_version);
  f_.free(wctx.mem_ctx_);

  CD cd1(
    100, ObVarcharType, CS_TYPE_UTF8MB4_GENERAL_CI,
    101, ObVarcharType, CS_TYPE_UTF8MB4_BIN
    );
  const ObIArray<share::schema::ObColDesc> &columns1 = cd1.get_columns();

  const char *new_rows1 =
    "var    var    flag     dml\n"
    "b      bbb    EXIST    UPDATE\n"
    "e      eee    EXIST    INSERT\n"
    "f      fff    EXIST    UPDATE\n";

  new_row_iter.reset();
  ASSERT_EQ(OB_SUCCESS, new_row_iter.from(new_rows1));
  wctx.mem_ctx_ = f_.alloc();
  wctx.mem_ctx_->trans_begin();
  wctx.mem_ctx_->sub_trans_begin(trans_version, 1000000 + ::oceanbase::common::ObTimeUtility::current_time());
  ret = mt.set(wctx, table_schema_.get_table_id(), 1, columns1, new_row_iter);
  EXPECT_EQ(OB_SUCCESS, ret);
  wctx.mem_ctx_->trans_end(true, ++trans_version);
  f_.free(wctx.mem_ctx_);

  CD cd2(
    100, ObVarcharType, CS_TYPE_UTF8MB4_GENERAL_CI,
    102, ObIntType, CS_TYPE_UTF8MB4_BIN
    );
  const ObIArray<share::schema::ObColDesc> &columns2 = cd2.get_columns();

  const char *new_rows2 =
    "var    bigint   flag     dml\n"
    "b      2        EXIST    UPDATE\n"
    "d      100      EXIST    UPDATE\n";

  new_row_iter.reset();
  ASSERT_EQ(OB_SUCCESS, new_row_iter.from(new_rows2));
  wctx.mem_ctx_ = f_.alloc();
  wctx.mem_ctx_->trans_begin();
  wctx.mem_ctx_->sub_trans_begin(trans_version, 1000000 + ::oceanbase::common::ObTimeUtility::current_time());
  ret = mt.set(wctx, table_schema_.get_table_id(), 1, columns2, new_row_iter);
  EXPECT_EQ(OB_SUCCESS, ret);
  wctx.mem_ctx_->trans_end(true, ++trans_version);
  f_.free(wctx.mem_ctx_);

  ObQueryFlag query_flag(ObQueryFlag::Forward,
      true, //is daily merge scan
      true, //is read multiple macro block
      true, //sys task scan, read one macro block in single io
      false, //is full row scan?
      false,
      false);

  CD query_cd(
      100, ObVarcharType, CS_TYPE_UTF8MB4_GENERAL_CI,
      102, ObIntType, CS_TYPE_UTF8MB4_BIN,
      103, ObNumberType, CS_TYPE_UTF8MB4_BIN
      );
  const ObIArray<share::schema::ObColDesc> &query_columns = query_cd.get_columns();

  table_param_.reset();
  //table_access_param_.create_multi_version_row_param(column_ids, table_schema_);
  table_param_.table_id_ = table_schema_.get_table_id();
  table_param_.rowkey_cnt_ = 1;
  table_param_.schema_version_ = 100;
  table_param_.out_cols_->assign(query_columns);
  table_param_.is_multi_version_minor_merge_ = true;

  rctx.mem_ctx_ = f_.alloc();
  rctx.mem_ctx_->trans_begin();
  rctx.mem_ctx_->sub_trans_begin(100 , INT64_MAX - 2);
  rctx.mem_ctx_->set_multi_version_start(1);
  rctx.mem_ctx_->set_base_version(table_key.trans_version_range_.base_version_);
  ASSERT_EQ(true, rctx.mem_ctx_->is_multi_version_range_valid());

  ObBlockCacheWorkingSet block_cache_ws;
  table_access_context_.query_flag_ = query_flag;
  table_access_context_.store_ctx_ = &rctx;
  table_access_context_.allocator_ = &allocator_;
  table_access_context_.stmt_allocator_ = &allocator_;
  table_access_context_.block_cache_ws_ = &block_cache_ws;
  table_access_context_.is_inited_ = true;//just for test case

  range_.set_whole_range();
  range_.set_table_id(table_schema_.get_table_id());
  ObExtStoreRange ext_range(range_);
  ObStoreRowIterator *store_row_iter = NULL;

  ret = mt.scan(table_param_, table_access_context_, ext_range, store_row_iter);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObVPMemtableCompactIter vp_compact_iter;
  ASSERT_EQ(OB_SUCCESS, vp_compact_iter.init(2, store_row_iter));

  const char *result =
    "var    bigint    bigint   number    flag     first_dml   dml\n"
    "a      -2        1        1.0       EXIST    INSERT      INSERT\n"
    "b      -5        2        2.0       EXIST    UPDATE      UPDATE\n"
    "b      -3        nop      2.0       EXIST    UPDATE      UPDATE\n"
    "c      -2        nop      nop       DELETE   DELETE      DELETE\n"
    "d      -5        100      nop       EXIST    UPDATE      UPDATE\n"
    "e      -4        nop      nop       EXIST    INSERT      INSERT\n";

  ObMockIterator res_iter;
  ASSERT_EQ(OB_SUCCESS, res_iter.from(result));
  ASSERT_TRUE(res_iter.equals(vp_compact_iter));

  rctx.mem_ctx_->trans_end(true, trans_version);
  f_.free(rctx.mem_ctx_);
}
*/
int init_tenant_mgr()
{
  ObTenantManager& tm = ObTenantManager::get_instance();
  ObAddr self;
  self.set_ip_addr("127.0.0.1", 8086);
  ObReqTransport req_transport(NULL, NULL);
  obrpc::ObSrvRpcProxy rpc_proxy;
  obrpc::ObCommonRpcProxy rs_rpc_proxy;
  share::ObRsMgr rs_mgr;
  int ret = tm.init(self, rpc_proxy, rs_rpc_proxy, rs_mgr, &req_transport, &ObServerConfig::get_instance());
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = tm.add_tenant(OB_SYS_TENANT_ID);
  EXPECT_EQ(OB_SUCCESS, ret);
  const int64_t ulmt = 16LL << 30;
  const int64_t llmt = 8LL << 30;
  ret = tm.set_tenant_mem_limit(OB_SYS_TENANT_ID, ulmt, llmt);
  EXPECT_EQ(OB_SUCCESS, ret);
  return OB_SUCCESS;
}

}  // namespace memtable
}  // namespace oceanbase

int main(int argc, char** argv)
{
  GCONF._enable_sparse_row = true;
  system("rm -f test_memtable_multi_version_row_iterator.log");
  OB_LOGGER.set_file_name("test_memtable_multi_version_row_iterator.log");
  OB_LOGGER.set_log_level("DEBUG");
  oceanbase::memtable::init_tenant_mgr();
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
