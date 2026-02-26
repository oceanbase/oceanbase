/**
 * Copyright (c) 2022 OceanBase
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
#define USING_LOG_PREFIX STORAGE

#include "mtlenv/storage/blocksstable/ob_index_block_data_prepare.h"
#include "storage/access/ob_sstable_row_scanner.h"
#include "storage/access/ob_vector_store.h"

namespace oceanbase
{
using namespace storage;
using namespace common;
namespace blocksstable
{
#define DEFAULT_ROWKEY_CNT 4
#define DEFAULT_COLUMN_CNT 6
#define DEFAULT_GROUP_IDX 1234
#define DEFAULT_LOG_LEVEL "TRACE"
#define SQL_BATCH_SIZE 256
#define DATUM_ARRAY_CNT  1024
#define DATUM_RES_SIZE  10
#define DEFAULT_ROWKEY_PREFIX_CNT 2
#define DATUM_ARRAY_CNT  1024

class TestSSTableRowScannerBlockscan : public TestIndexBlockDataPrepare
{
public:
  TestSSTableRowScannerBlockscan();
  virtual ~TestSSTableRowScannerBlockscan();
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();
  void prepare_schema() override;
  void insert_data(ObMacroBlockWriter &data_writer) override;
  void prepare_output_expr(const ObIArray<int32_t> &projector);
  void prepare_access_param(const bool is_reverse_scan, ObArenaAllocator &alloc);
  void prepare_block_row_store(const ObTableAccessParam &param, ObArenaAllocator &alloc);
  void do_scan_and_check(std::vector<int> &begin, std::vector<int> &end, std::vector<int> &res);
  ObTableAccessParam access_param_;
  ObFixedArray<share::schema::ObColumnParam*, ObIAllocator> cols_param_;
  ObDatumRange scan_range_;
  ObDatumRow begin_row_;
  ObDatumRow end_row_;
  ObDatumRow res_row_;
  ObArenaAllocator case_alloc_;
  ObBlockRowStore *block_row_store_;
  sql::ObExecContext exec_ctx_;
  sql::ObEvalCtx eval_ctx_;
  sql::ObPushdownExprSpec expr_spec_;
  sql::ObPushdownOperator op_;
  ObFixedArray<int32_t, ObIAllocator> output_cols_project_;
  sql::ExprFixedArray output_exprs_;
  void *datum_buf_;
  int64_t datum_buf_offset_;
};

TestSSTableRowScannerBlockscan::TestSSTableRowScannerBlockscan()
  : TestIndexBlockDataPrepare("Test sstable row scanner blockscan",
                              compaction::ObMergeType::MAJOR_MERGE,
                              false,
                              OB_DEFAULT_MACRO_BLOCK_SIZE,
                              10000,
                              20, /*max_row_cnt*/
                              FLAT_ROW_STORE,
                              5, /* rows_per_mirco_block */
                              2  /* mirco_blocks_per_macro_block */),
    block_row_store_(nullptr),
    exec_ctx_(allocator_),
    eval_ctx_(exec_ctx_),
    expr_spec_(allocator_),
    op_(eval_ctx_, expr_spec_),
    datum_buf_(nullptr),
    datum_buf_offset_(0)
{
}

TestSSTableRowScannerBlockscan::~TestSSTableRowScannerBlockscan()
{
}

void TestSSTableRowScannerBlockscan::SetUpTestCase()
{
  TestIndexBlockDataPrepare::SetUpTestCase();
}

void TestSSTableRowScannerBlockscan::TearDownTestCase()
{
  TestIndexBlockDataPrepare::TearDownTestCase();
}

void TestSSTableRowScannerBlockscan::SetUp()
{
  TestIndexBlockDataPrepare::SetUp();
  prepare_output_expr(output_cols_project_);
}

void TestSSTableRowScannerBlockscan::TearDown()
{
  TestIndexBlockDataPrepare::TearDown();
  access_param_.reset();
  cols_param_.reset();
  scan_range_.reset();
  begin_row_.reset();
  end_row_.reset();
  res_row_.reset();
  case_alloc_.reset();
  output_exprs_.reset();
  datum_buf_ = nullptr;
  datum_buf_offset_ = 0;
  block_row_store_ = nullptr;
}

void TestSSTableRowScannerBlockscan::prepare_schema()
{
  // init table schema
  uint64_t table_id = TEST_TABLE_ID;
  table_schema_.reset();
  ASSERT_EQ(OB_SUCCESS, table_schema_.set_table_name("test_sstable_row_scanner_blockscan"));
  table_schema_.set_tenant_id(1);
  table_schema_.set_tablegroup_id(1);
  table_schema_.set_database_id(1);
  table_schema_.set_table_id(table_id);
  table_schema_.set_rowkey_column_num(DEFAULT_ROWKEY_CNT);
  table_schema_.set_max_used_column_id(ObHexStringType);
  table_schema_.set_block_size(2 * 1024);
  table_schema_.set_compress_func_name("none");
  table_schema_.set_row_store_type(row_store_type_);
  table_schema_.set_storage_format_version(OB_STORAGE_FORMAT_VERSION_V4);
  table_schema_.set_micro_index_clustered(false);
  index_schema_.reset();

  // init column schema
  ObObjType types[DEFAULT_COLUMN_CNT] =
    {ObIntType, ObIntType, ObIntType, ObIntType, ObIntType, ObIntType};
  char name[OB_MAX_FILE_NAME_LENGTH];
  memset(name, 0, sizeof(name));
  ObColumnSchemaV2 column;
  for (int64_t i = 0; i < DEFAULT_COLUMN_CNT; ++i) {
    ObObjType obj_type = types[i];
    column.reset();
    column.set_table_id(table_id);
    column.set_column_id(i + OB_APP_MIN_COLUMN_ID);
    sprintf(name, "test%020ld", i);
    ASSERT_EQ(OB_SUCCESS, column.set_column_name(name));
    column.set_data_type(obj_type);
    column.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    column.set_data_length(1);
    if (i < DEFAULT_ROWKEY_CNT) {
      column.set_rowkey_position(i + 1);
    } else {
      column.set_rowkey_position(0);
    }
    ASSERT_EQ(OB_SUCCESS, table_schema_.add_column(column));
  }

  schema_cols_.set_allocator(&allocator_);
  schema_cols_.init(table_schema_.get_column_count());
  ASSERT_EQ(OB_SUCCESS, table_schema_.get_column_ids(schema_cols_));

  cols_param_.set_allocator(&allocator_);
  cols_param_.init(schema_cols_.count());
  for (int64_t i = 0; i < schema_cols_.count(); ++i) {
    void *col_param_buf = allocator_.alloc(sizeof(ObColumnParam));
    ObColumnParam *col_param = new(col_param_buf) ObColumnParam(allocator_);
    col_param->set_meta_type(schema_cols_.at(i).col_type_);
    col_param->set_nullable_for_write(true);
    cols_param_.push_back(col_param);
  }

  output_cols_project_.set_allocator(&allocator_);
  output_cols_project_.init(DEFAULT_COLUMN_CNT);
  output_cols_project_.push_back(0);
  output_cols_project_.push_back(1);
  output_cols_project_.push_back(2);
  output_cols_project_.push_back(3);
  output_cols_project_.push_back(4);
  output_cols_project_.push_back(5);
}

void TestSSTableRowScannerBlockscan::prepare_output_expr(const ObIArray<int32_t> &projector)
{
  const int64_t output_expr_cnt = projector.count();
  output_exprs_.set_allocator(&allocator_);
  output_exprs_.init(output_expr_cnt);
  datum_buf_ = allocator_.alloc((sizeof(sql::ObDatum) + OBJ_DATUM_NUMBER_RES_SIZE) * DATUM_ARRAY_CNT * 2 * output_expr_cnt);
  ASSERT_NE(nullptr, datum_buf_);
  for (int64_t i = 0; i < output_expr_cnt; ++i) {
    void *expr_buf = allocator_.alloc(sizeof(sql::ObExpr));
    ASSERT_NE(nullptr, expr_buf);
    sql::ObExpr *expr = reinterpret_cast<sql::ObExpr *>(expr_buf);
    expr->reset();

    expr->frame_idx_ = 0;
    expr->datum_off_ = datum_buf_offset_;
    sql::ObDatum *datums = new ((char*)datum_buf_ + datum_buf_offset_) sql::ObDatum[DATUM_ARRAY_CNT];
    datum_buf_offset_ += sizeof(sql::ObDatum) * DATUM_ARRAY_CNT;
    expr->res_buf_off_ = datum_buf_offset_;
    expr->res_buf_len_ = DATUM_RES_SIZE;
    char *ptr = (char *)datum_buf_ + expr->res_buf_off_;
    for (int64_t i = 0; i < DATUM_ARRAY_CNT; i++) {
      datums[i].ptr_ = ptr;
      ptr += expr->res_buf_len_;
    }
    datum_buf_offset_ += expr->res_buf_len_ * DATUM_ARRAY_CNT;
    expr->type_ = T_REF_COLUMN;
    expr->datum_meta_.type_ =ObIntType;
    expr->obj_datum_map_ = OBJ_DATUM_8BYTE_DATA;
    output_exprs_.push_back(expr);
  }
}

void TestSSTableRowScannerBlockscan::prepare_access_param(const bool is_reverse_scan, ObArenaAllocator &alloc)
{
  access_param_.iter_param_.table_id_ = table_schema_.get_table_id();
  access_param_.iter_param_.tablet_id_ = table_schema_.get_table_id();
  access_param_.iter_param_.is_same_schema_column_ = true;
  access_param_.iter_param_.has_virtual_columns_ = false;
  access_param_.iter_param_.vectorized_enabled_ = true;
  access_param_.iter_param_.pd_storage_flag_.pd_blockscan_ = true;
  access_param_.iter_param_.pd_storage_flag_.pd_filter_ = true;
  access_param_.iter_param_.pd_storage_flag_.use_stmt_iter_pool_ = true;
  access_param_.iter_param_.pd_storage_flag_.use_column_store_ = false;
  access_param_.iter_param_.op_ = &op_;
  access_param_.output_exprs_ = &output_exprs_;
  access_param_.iter_param_.out_cols_project_ = &output_cols_project_;

  read_info_.reset();
  ASSERT_EQ(OB_SUCCESS, read_info_.init(alloc,
      DEFAULT_COLUMN_CNT,
      table_schema_.get_rowkey_column_num(),
      lib::is_oracle_mode(),
      schema_cols_,
      nullptr/*storage_cols_index*/,
      &cols_param_));
  access_param_.iter_param_.read_info_ = &read_info_;
  access_param_.iter_param_.has_lob_column_out_ = false;

  context_.reset();
  context_.query_flag_.set_not_use_row_cache();
  context_.query_flag_.set_use_block_cache();
  if (is_reverse_scan) {
    context_.query_flag_.scan_order_ = ObQueryFlag::ScanOrder::Reverse;
  } else {
    context_.query_flag_.scan_order_ = ObQueryFlag::ScanOrder::Forward;
  }
  context_.store_ctx_ = &store_ctx_;
  context_.ls_id_ = ls_id_;
  context_.tablet_id_ = tablet_id_;
  context_.allocator_ = &alloc;
  context_.stmt_allocator_ = &alloc;
  context_.limit_param_ = nullptr;
  ASSERT_EQ(OB_SUCCESS, context_.micro_block_handle_mgr_.init(false /* disable limit */,
            context_.tablet_id_, context_.table_store_stat_, context_.table_scan_stat_, context_.query_flag_));
  context_.is_inited_ = true;

  prepare_block_row_store(access_param_, alloc);
  context_.block_row_store_ = block_row_store_;
}

void TestSSTableRowScannerBlockscan::prepare_block_row_store(const ObTableAccessParam &param, ObArenaAllocator &alloc)
{
  void *buf = alloc.alloc(sizeof(ObVectorStore));
  ASSERT_EQ(false, nullptr == buf);
  eval_ctx_.batch_idx_ = 0;
  eval_ctx_.batch_size_ = SQL_BATCH_SIZE;
  expr_spec_.max_batch_size_ = SQL_BATCH_SIZE;
  eval_ctx_.frames_ = (char **)(&datum_buf_);
  block_row_store_ = new (buf) ObVectorStore(op_.get_batch_size(), eval_ctx_, context_, nullptr);
  ASSERT_EQ(OB_SUCCESS, block_row_store_->init(param));
}

int col_values[6][20] = {{/*block1*/  1,  1,  1,  1,  1, /*block2*/  2,  2,  2,  2,  2, /*block3*/  2,  7,  7,  7,  7, /*block4*/  7,  8,  9, 10, 11},
                         {/*block1*/  1,  1,  1,  1,  1, /*block2*/  2,  2,  3,  3,  3, /*block3*/  3,  8,  8,  9,  9, /*block4*/  9, 10, 11, 12, 13},
                         {/*block1*/  1,  2,  3,  4,  5, /*block2*/  6,  7,  8,  9, 10, /*block3*/ 11, 12, 13, 14, 15, /*block4*/ 16, 17, 18, 19, 20},
                         {/*block1*/ 20, 19, 18, 17, 16, /*block2*/ 15, 14, 13, 12, 11, /*block3*/ 10,  9,  8,  7,  6, /*block4*/  5,  4,  3,  2,  1},
                         {/*block1*/ 21, 22, 23, 24, 25, /*block2*/ 26, 27, 28, 29, 30, /*block3*/ 31, 32, 33, 34, 35, /*block4*/ 36, 37, 38, 39, 40},
                         {/*block1*/ 41, 42, 43, 44, 45, /*block2*/ 46, 47, 48, 49, 50, /*block3*/ 51, 52, 53, 54, 55, /*block4*/ 56, 57, 58, 59, 60}};

void TestSSTableRowScannerBlockscan::insert_data(ObMacroBlockWriter &data_writer)
{
  int ret = OB_SUCCESS;
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  row_cnt_ = 0;
  ObDatumRow row;
  ObDatumRow multi_row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, MAX_TEST_COLUMN_CNT));
  ASSERT_EQ(OB_SUCCESS, multi_row.init(allocator_, MAX_TEST_COLUMN_CNT));
  const ObDmlFlag dml = DF_INSERT;

  storage::ObColDescArray column_list;
  ASSERT_EQ(OB_SUCCESS, table_schema_.get_column_ids(column_list));
  int64_t rows_per_macro_block = rows_per_mirco_block_ * mirco_blocks_per_macro_block_;

  ObObj obj;
  while (true) {
    if (row_cnt_ >= max_row_cnt_) {
      break;
    }
    for (int64_t i = 0; i < DEFAULT_ROWKEY_CNT; ++i) {
      obj.set_int(col_values[i][row_cnt_]);
      ASSERT_EQ(OB_SUCCESS, row.storage_datums_[i].from_obj_enhance(obj));
    }
    for (int64_t i = DEFAULT_ROWKEY_CNT; i < DEFAULT_COLUMN_CNT; ++i) {
      obj.set_int(col_values[i][row_cnt_]);
      ASSERT_EQ(OB_SUCCESS, row.storage_datums_[i].from_obj_enhance(obj));
    }

    convert_to_multi_version_row(row, table_schema_, SNAPSHOT_VERSION, dml, multi_row);
    multi_row.mvcc_row_flag_.set_uncommitted_row(false);
    ASSERT_EQ(OB_SUCCESS, data_writer.append_row(multi_row));
    if (row_cnt_ == 0) {
      ObDatumRowkey &start_key = data_writer.last_key_;
      ASSERT_EQ(OB_SUCCESS, start_key.deep_copy(start_key_, allocator_));
    }
    if (row_cnt_ == max_row_cnt_ - 1) {
      ObDatumRowkey &end_key = data_writer.last_key_;
      ASSERT_EQ(OB_SUCCESS, end_key.deep_copy(end_key_, allocator_));
    }
    if ((row_cnt_ + 1) % rows_per_mirco_block_ == 0) {
      OK(data_writer.build_micro_block());
    }
    if ((row_cnt_ + 1) % rows_per_macro_block == 0) {
      OK(data_writer.try_switch_macro_block());
    }
    ++row_cnt_;
  }
  oceanbase::common::ObLogger::get_logger().set_log_level(DEFAULT_LOG_LEVEL);
}

void TestSSTableRowScannerBlockscan::do_scan_and_check(std::vector<int> &begin, std::vector<int> &end, std::vector<int> &res)
{
  int ret = OB_SUCCESS;
  scan_range_.reset();
  begin_row_.reset();
  end_row_.reset();
  res_row_.reset();
  ASSERT_EQ(OB_SUCCESS, begin_row_.init(case_alloc_, DEFAULT_ROWKEY_CNT));
  ASSERT_EQ(OB_SUCCESS, end_row_.init(case_alloc_, DEFAULT_ROWKEY_CNT));
  ASSERT_EQ(OB_SUCCESS, res_row_.init(case_alloc_, DEFAULT_ROWKEY_CNT));

  ObObj begin_obj, end_obj;
  for (int64_t i = 0; i < DEFAULT_ROWKEY_CNT; ++i) {
    begin_obj.set_int(begin[i]);
    end_obj.set_int(end[i]);
    ASSERT_EQ(OB_SUCCESS, begin_row_.storage_datums_[i].from_obj_enhance(begin_obj));
    ASSERT_EQ(OB_SUCCESS, end_row_.storage_datums_[i].from_obj_enhance(end_obj));
  }
  scan_range_.start_key_.assign(begin_row_.storage_datums_, DEFAULT_ROWKEY_CNT);
  scan_range_.end_key_.assign(end_row_.storage_datums_, DEFAULT_ROWKEY_CNT);
  scan_range_.set_group_idx(DEFAULT_GROUP_IDX);
  scan_range_.set_left_closed();
  scan_range_.set_right_closed();

  ObSSTableRowScanner<ObIndexTreeMultiPassPrefetcher<2,2>> scanner;
  ASSERT_EQ(OB_SUCCESS, scanner.init(access_param_.iter_param_, context_, &sstable_, &scan_range_));

  ObDatumRowkey border_rowkey;
  border_rowkey.reset();
  context_.query_flag_.is_reverse_scan() ? border_rowkey.set_min_rowkey() : border_rowkey.set_max_rowkey();
  scanner.prefetcher_.border_rowkey_ = border_rowkey;
  scanner.prefetcher_.can_blockscan_ = true;

  const ObStorageDatumUtils &datum_utils = read_info_.get_datum_utils();
  const ObStoreCmpFuncs &cmp_funcs = datum_utils.get_cmp_funcs();
  ASSERT_EQ(true, nullptr != block_row_store_);
  while (OB_SUCC(ret)) {
    static_cast<ObVectorStore *>(block_row_store_)->reuse_capacity(SQL_BATCH_SIZE);
    ret = scanner.get_next_rows();
    ASSERT_EQ(true, OB_SUCCESS == ret || OB_ITER_END == ret);
  }
}

TEST_F(TestSSTableRowScannerBlockscan, test_forward_scan)
{
  std::vector<std::vector<int>> start_rowkeys;
  std::vector<std::vector<int>> end_rowkeys;
  std::vector<std::vector<int>> res_prefixs;

  std::vector<int> start_rowkey;
  std::vector<int> end_rowkey;
  std::vector<int> res_prefix;

  start_rowkeys.push_back({0, 0, 0, 0});
  end_rowkeys.push_back({100, 100, 100, 100});
  res_prefixs.push_back({11, 13});

  for (int i = 0; i < 20; ++i) {
    for (int j = i; j < 20; ++j) {
      start_rowkey.clear();
      end_rowkey.clear();
      res_prefix.clear();
      for (int k = 0; k < DEFAULT_ROWKEY_CNT; ++k) {
        start_rowkey.push_back(col_values[k][i]);
        end_rowkey.push_back(col_values[k][j]);
        if (k < DEFAULT_ROWKEY_PREFIX_CNT) {
          res_prefix.push_back(col_values[k][j]);
        }
      }
      start_rowkeys.push_back(start_rowkey);
      end_rowkeys.push_back(end_rowkey);
      res_prefixs.push_back(res_prefix);
    }
  }

  for (int i = 0; i < start_rowkeys.size(); ++i) {
    case_alloc_.reuse();
    prepare_access_param(false, case_alloc_);
    do_scan_and_check(start_rowkeys[i], end_rowkeys[i], res_prefixs[i]);
  }
  printf("case count: %zu\n", start_rowkeys.size());
}

TEST_F(TestSSTableRowScannerBlockscan, test_reverse_scan)
{
  std::vector<std::vector<int>> start_rowkeys;
  std::vector<std::vector<int>> end_rowkeys;
  std::vector<std::vector<int>> res_prefixs;

  std::vector<int> start_rowkey;
  std::vector<int> end_rowkey;
  std::vector<int> res_prefix;

  start_rowkeys.push_back({7, 9, 15, 6});
  end_rowkeys.push_back({7, 9, 15, 6});
  res_prefixs.push_back({7, 9});

  start_rowkeys.push_back({0, 0, 0, 0});
  end_rowkeys.push_back({100, 100, 100, 100});
  res_prefixs.push_back({1, 1});

  for (int i = 0; i < 20; ++i) {
    for (int j = i; j < 20; ++j) {
      start_rowkey.clear();
      end_rowkey.clear();
      res_prefix.clear();
      for (int k = 0; k < DEFAULT_ROWKEY_CNT; ++k) {
        start_rowkey.push_back(col_values[k][i]);
        end_rowkey.push_back(col_values[k][j]);
        if (k < DEFAULT_ROWKEY_PREFIX_CNT) {
          res_prefix.push_back(col_values[k][i]);
        }
      }
      start_rowkeys.push_back(start_rowkey);
      end_rowkeys.push_back(end_rowkey);
      res_prefixs.push_back(res_prefix);
    }
  }

  for (int i = 0; i < start_rowkeys.size(); ++i) {
    case_alloc_.reuse();
    prepare_access_param(true, case_alloc_);
    do_scan_and_check(start_rowkeys[i], end_rowkeys[i], res_prefixs[i]);
  }
  printf("case count: %zu\n", start_rowkeys.size());
}

}
}

int main(int argc, char **argv)
{
  system("rm -f test_sstable_row_scanner_blockscan.log*");
  OB_LOGGER.set_file_name("test_sstable_row_scanner_blockscan.log", true, true, "test_sstable_row_scanner_blockscan_rs.log", "test_sstable_row_scanner_blockscan_elec.log");
  oceanbase::common::ObLogger::get_logger().set_log_level(DEFAULT_LOG_LEVEL);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
