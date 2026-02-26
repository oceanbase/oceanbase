
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

#define private public
#define protected public

#include "storage/column_store/ob_cg_scanner.h"
#include "mtlenv/storage/blocksstable/ob_index_block_data_prepare.h"

namespace oceanbase
{
using namespace storage;
using namespace common;
namespace blocksstable
{

class TestAllCGScanner : public TestIndexBlockDataPrepare {
  public:
  TestAllCGScanner();
  virtual ~TestAllCGScanner();
  static void SetUpTestCase();
  static void TearDownTestCase();

  virtual void SetUp();
  virtual void TearDown();
  void prepare_test(const bool is_reverse);
  void finish_test();
  void prepare_schema() override;
  void prepare_all_cg_query_param(const bool is_reverse_scan,
                              const ObVersionRange &scan_version);
  void destroy_all_cg_query_param();
  void check_data(int64_t start,
                  int64_t locate_count,
                  const bool is_reverse,
                  const ObCGBitmap *bitmap = nullptr);

  void test_large_micro_selected(const bool is_reverse);
  void test_border(const bool is_reverse);
  void test_random(const bool is_reverse);

  static constexpr int64_t TEST_COLUMN_COUNT = 2;
  static constexpr int64_t TEST_ROWKEY_COLUMN_COUNT = 1;
  static constexpr int64_t ARRAY_COUNT = 1024;

  public:
  ObArenaAllocator allocator_;
  ObDatumRow start_row_;
  ObDatumRow end_row_;
  ObTableAccessParam access_param_;
  ObVersionRange scan_version_;
  sql::ObExecContext exec_ctx_;
  sql::ObEvalCtx eval_ctx_;
  sql::ObPushdownExprSpec expr_spec_;
  sql::ExprFixedArray exprs_;
  sql::ObPushdownOperator op_;
  ObFixedArray<int32_t, ObIAllocator> out_cols_project_;
  ObSSTableWrapper wrapper_;
  ObCGRowScanner *cg_scanner_;
  ObBlockRowStore *block_row_store_;
  char **frames_;
};

TestAllCGScanner::TestAllCGScanner()
    : TestIndexBlockDataPrepare("Test cg sstable row scanner",
                                ObMergeType::MAJOR_MERGE,
                                false,
                                OB_DEFAULT_MACRO_BLOCK_SIZE,
                                10000,
                                65536,
                                ENCODING_ROW_STORE,
                                10,
                                10),
      scan_version_(),
      exec_ctx_(allocator_),
      eval_ctx_(exec_ctx_),
      expr_spec_(allocator_),
      exprs_(),
      op_(eval_ctx_, expr_spec_),
      out_cols_project_()
{
  is_all_cg_data_ = true;
}

TestAllCGScanner::~TestAllCGScanner()
{}

void TestAllCGScanner::SetUpTestCase()
{
  TestIndexBlockDataPrepare::SetUpTestCase();
}

void TestAllCGScanner::TearDownTestCase()
{
  TestIndexBlockDataPrepare::TearDownTestCase();
}

void TestAllCGScanner::prepare_test(const bool is_reverse)
{
  scan_version_.base_version_ = 1;
  scan_version_.multi_version_start_ = 1;
  scan_version_.snapshot_version_ = INT64_MAX;
  // prepare query param
  prepare_all_cg_query_param(is_reverse, scan_version_);
  eval_ctx_.batch_idx_ = 0;
  eval_ctx_.batch_size_ = 256;
  expr_spec_.max_batch_size_ = 256;
  exprs_.set_allocator(&allocator_);
  exprs_.init(TEST_COLUMN_COUNT);

  frames_ = (char **)allocator_.alloc(sizeof(char *) * TEST_COLUMN_COUNT);

  // column 0 bigint
  void *datum_buf = allocator_.alloc((sizeof(sql::ObDatum) + sizeof(int64_t)) *
                                     ARRAY_COUNT * 2);
  sql::ObDatum *datums = new (datum_buf) sql::ObDatum[ARRAY_COUNT];
  frames_[0] = (char *)(datum_buf);

  void *expr_buf = allocator_.alloc(sizeof(sql::ObExpr));
  ASSERT_NE(nullptr, expr_buf);
  sql::ObExpr *expr = reinterpret_cast<sql::ObExpr *>(expr_buf);
  expr->reset();
  expr->frame_idx_ = 0;
  expr->res_buf_off_ = sizeof(sql::ObDatum) * ARRAY_COUNT;
  expr->res_buf_len_ = sizeof(int64_t);
  char *ptr = (char *)datum_buf + expr->res_buf_off_;
  for (int64_t i = 0; i < ARRAY_COUNT; i++) {
    datums[i].ptr_ = ptr;
    ptr += expr->res_buf_len_;
  }
  exprs_.push_back(expr);

  // column 1 int
  datum_buf = allocator_.alloc((sizeof(sql::ObDatum) + sizeof(int32_t)) *
                               ARRAY_COUNT * 2);
  datums = new (datum_buf) sql::ObDatum[ARRAY_COUNT];
  frames_[1] = (char *)(datum_buf);

  expr_buf = allocator_.alloc(sizeof(sql::ObExpr));
  ASSERT_NE(nullptr, expr_buf);
  expr = reinterpret_cast<sql::ObExpr *>(expr_buf);
  expr->reset();
  expr->frame_idx_ = 1;
  expr->res_buf_off_ = sizeof(sql::ObDatum) * ARRAY_COUNT;
  expr->res_buf_len_ = sizeof(int32_t);
  ptr = (char *)datum_buf + expr->res_buf_off_;
  for (int64_t i = 0; i < ARRAY_COUNT; i++) {
    datums[i].ptr_ = ptr;
    ptr += expr->res_buf_len_;
  }
  exprs_.push_back(expr);

  eval_ctx_.frames_ = frames_;

  out_cols_project_.set_allocator(&allocator_);
  out_cols_project_.init(TEST_COLUMN_COUNT);
  for (int64_t i = 0; i < schema_cols_.count(); ++i) {
    out_cols_project_.push_back(i);
  }

  access_param_.iter_param_.out_cols_project_ = &out_cols_project_;
  access_param_.iter_param_.output_exprs_ = &exprs_;
  access_param_.iter_param_.op_ = &op_;
  void *buf = allocator_.alloc(sizeof(ObBlockRowStore));
  ASSERT_NE(nullptr, buf);
  block_row_store_ = new (buf) ObBlockRowStore(context_);
  ASSERT_EQ(OB_SUCCESS, block_row_store_->init(access_param_));
  context_.block_row_store_ = block_row_store_;

  buf = allocator_.alloc(sizeof(ObCGRowScanner));
  ASSERT_NE(nullptr, buf);
  cg_scanner_ = new (buf) ObCGRowScanner();
  wrapper_.sstable_ = &co_sstable_;
  ASSERT_EQ(OB_SUCCESS,
            cg_scanner_->init(access_param_.iter_param_, context_, wrapper_));
}

void TestAllCGScanner::finish_test()
{
  for (auto i = 0; i < TEST_COLUMN_COUNT; ++i) {
    allocator_.free(frames_[i]);
    exprs_[i]->reset();
    allocator_.free(exprs_[i]);
  }
  exprs_.reset();
  allocator_.free(frames_);
  frames_ = nullptr;
  out_cols_project_.reset();
  allocator_.free(block_row_store_);
  block_row_store_ = nullptr;
  cg_scanner_->reset();
  allocator_.free(cg_scanner_);
  cg_scanner_ = nullptr;
  destroy_all_cg_query_param();
}

void TestAllCGScanner::prepare_schema()
{
  ObColumnSchemaV2 column;
  //init table schema
  uint64_t table_id =  TEST_TABLE_ID;
  table_schema_.reset();
  ASSERT_EQ(OB_SUCCESS, table_schema_.set_table_name("test_index_block"));
  table_schema_.set_tenant_id(1);
  table_schema_.set_tablegroup_id(1);
  table_schema_.set_database_id(1);
  table_schema_.set_table_id(table_id);
  table_schema_.set_rowkey_column_num(TEST_ROWKEY_COLUMN_COUNT);
  table_schema_.set_max_used_column_id(TEST_COLUMN_COUNT);
  table_schema_.set_block_size(2 * 1024);
  table_schema_.set_compress_func_name("none");
  table_schema_.set_row_store_type(row_store_type_);
  table_schema_.set_storage_format_version(OB_STORAGE_FORMAT_VERSION_V4);
  table_schema_.set_micro_index_clustered(false);

  index_schema_.reset();
  ASSERT_EQ(OB_SUCCESS, index_schema_.set_table_name("test_index_block"));
  index_schema_.set_tenant_id(1);
  index_schema_.set_tablegroup_id(1);
  index_schema_.set_database_id(1);
  index_schema_.set_table_id(table_id);
  index_schema_.set_rowkey_column_num(TEST_ROWKEY_COLUMN_COUNT);
  index_schema_.set_max_used_column_id(TEST_COLUMN_COUNT);
  index_schema_.set_block_size(2 * 1024);
  index_schema_.set_compress_func_name("none");
  index_schema_.set_row_store_type(row_store_type_);
  index_schema_.set_storage_format_version(OB_STORAGE_FORMAT_VERSION_V4);
  index_schema_.set_micro_index_clustered(table_schema_.get_micro_index_clustered());

  // init column 0
  char name[OB_MAX_FILE_NAME_LENGTH];
  memset(name, 0, sizeof(name));
  ObObjType obj_type = ObObjType::ObIntType;
  column.reset();
  column.set_table_id(TEST_TABLE_ID);
  column.set_column_id(OB_APP_MIN_COLUMN_ID);
  sprintf(name, "test0");
  ASSERT_EQ(OB_SUCCESS, column.set_column_name(name));
  column.set_data_type(obj_type);
  column.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  column.set_data_length(1);
  column.set_rowkey_position(1);

  ASSERT_EQ(OB_SUCCESS, table_schema_.add_column(column));
  ASSERT_EQ(OB_SUCCESS, index_schema_.add_column(column));

  // init column 1
  obj_type = ObObjType::ObInt32Type;
  column.reset();
  column.set_table_id(TEST_TABLE_ID);
  column.set_column_id(OB_APP_MIN_COLUMN_ID + 1);
  memset(name, 0, sizeof(name));
  sprintf(name, "test1");
  ASSERT_EQ(OB_SUCCESS, column.set_column_name(name));
  column.set_data_type(obj_type);
  column.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  column.set_data_length(1);
  ASSERT_EQ(OB_SUCCESS, table_schema_.add_column(column));

  // init index block data column
  column.reset();
  column.set_table_id(TEST_TABLE_ID);
  column.set_column_id(1 + OB_APP_MIN_COLUMN_ID);
  ASSERT_EQ(OB_SUCCESS, column.set_column_name("Index block data"));
  column.set_data_type(ObVarcharType);
  column.set_collation_type(CS_TYPE_BINARY);
  column.set_data_length(1);
  column.set_rowkey_position(0);
  ASSERT_EQ(OB_SUCCESS, index_schema_.add_column(column));

  const uint64_t tenant_id = table_schema_.get_tenant_id();
  for (int64_t i = 0; i < schema_cols_.count(); i++) {
    if (ObHexStringType == schema_cols_.at(i).col_type_.type_) {
      schema_cols_.at(i).col_type_.set_collation_type(CS_TYPE_BINARY);
      schema_cols_.at(i).col_type_.set_collation_level(CS_LEVEL_NUMERIC);
    }
  }
}

void TestAllCGScanner::prepare_all_cg_query_param(
    const bool is_reverse_scan,
    const ObVersionRange &scan_version)
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

  read_info_.reset();
  schema_cols_.set_allocator(&allocator_);
  schema_cols_.init(table_schema_.get_column_count());
  ASSERT_EQ(OB_SUCCESS, table_schema_.get_column_ids(schema_cols_));
  ObArray<ObColumnParam*> cols_param;
  for (int i = 0; i < schema_cols_.count(); ++i) {
    void* buf = allocator_.alloc(sizeof(ObColumnParam));
    ASSERT_NE(nullptr, buf);
    ObColumnParam* col_param = new (buf) ObColumnParam(allocator_);
    ObObj default_value;
    default_value.meta_ = schema_cols_[i].col_type_;
    col_param->set_orig_default_value(default_value);
    OK(cols_param.push_back(col_param));
  }
  ASSERT_EQ(OB_SUCCESS,
            read_info_.init(allocator_,
                            table_schema_.get_column_count(),
                            table_schema_.get_rowkey_column_num(),
                            lib::is_oracle_mode(),
                            schema_cols_,
                            nullptr /*storage_cols_index*/,
                            &cols_param));
  access_param_.iter_param_.read_info_ = &read_info_;

  // jsut for test
  ObQueryFlag query_flag(
      ObQueryFlag::Forward,
      true, /*is daily merge scan*/
      true, /*is read multiple macro block*/
      true, /*sys task scan, read one macro block in single io*/
      false /*full row scan flag, obsoleted*/,
      false,  /*index back*/
      false); /*query_stat*/
  query_flag.set_not_use_row_cache();
  query_flag.set_use_block_cache();
  if (is_reverse_scan) {
    query_flag.scan_order_ = ObQueryFlag::ScanOrder::Reverse;
  } else {
    query_flag.scan_order_ = ObQueryFlag::ScanOrder::Forward;
  }
  ASSERT_EQ(OB_SUCCESS,
            context_.init(
                query_flag, store_ctx_, allocator_, allocator_, scan_version));
  context_.ls_id_ = ls_id_;
  context_.limit_param_ = nullptr;
  context_.is_inited_ = true;
}

void TestAllCGScanner::destroy_all_cg_query_param()
{
  access_param_.reset();
  schema_cols_.reset();
  context_.reset();
  read_info_.reset();
}

void TestAllCGScanner::SetUp()
{
  TestIndexBlockDataPrepare::SetUp();
  ASSERT_EQ(OB_SUCCESS, start_row_.init(allocator_, TEST_COLUMN_CNT));
  ASSERT_EQ(OB_SUCCESS, end_row_.init(allocator_, TEST_COLUMN_CNT));
  ObLSID ls_id(ls_id_);
  ObTabletID tablet_id(tablet_id_);
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService *);
  ASSERT_EQ(OB_SUCCESS,
            ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));

  ASSERT_EQ(OB_SUCCESS,
            ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle_));
}

void TestAllCGScanner::TearDown()
{
  tablet_handle_.reset();
  TestIndexBlockDataPrepare::TearDown();
}

void TestAllCGScanner::check_data(int64_t start,
                                  int64_t locate_count,
                                  const bool is_reverse,
                                  const ObCGBitmap *bitmap)
{
  int ret = OB_SUCCESS;
  uint64_t count = 0;
  int64_t data_count = MIN(row_cnt_ - start, locate_count);
  int64_t end = MIN(row_cnt_ - 1, start + locate_count - 1);
  int64_t sql_batch_size = 256;
  int64_t step = is_reverse ? -1 : 1;
  ObCSRowId current = is_reverse ? end : start;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(cg_scanner_->get_next_rows(count, sql_batch_size))) {
      if (OB_ITER_END != ret) {
        STORAGE_LOG(WARN, "Fail to get next rows", K(ret));
        ASSERT_EQ(OB_ITER_END, ret);
      }
    }
    auto col_0_datums = reinterpret_cast<ObDatum *>(frames_[0]);
    auto col_1_datums = reinterpret_cast<ObDatum *>(frames_[1]);
    for (int64_t i = 0; i < count;) {
      ASSERT_LE(current, end);
      ASSERT_GE(current, start);
      if (nullptr == bitmap || bitmap->test(current)) {
        int64_t val = col_0_datums[i].get_int();
        ASSERT_EQ(val * 4 + 17, col_1_datums[i].get_int32());
        ASSERT_EQ(current, val) << ",i: " << i << ", start: " << start
                                << ", locate_count: " << locate_count;
        ++i;
      }
      current += step;
    }
  }
}

void TestAllCGScanner::test_large_micro_selected(const bool is_reverse)
{
  prepare_test(is_reverse);

  int64_t start = 0;
  int64_t locate_count = row_cnt_;
  int64_t sql_batch_size = 256;
  int64_t total_cnt = row_cnt_ / 10;
  ObCGBitmap bitmap(allocator_);
  bitmap.init(row_cnt_, is_reverse);
  bitmap.reuse(0);
  for (int64_t i = 0; i < total_cnt; i++) {
    bitmap.set(i * 10);
  }
  ASSERT_EQ(OB_SUCCESS,
            cg_scanner_->locate(ObCSRange(start, locate_count), &bitmap));
  check_data(start, locate_count, is_reverse, &bitmap);

  STORAGE_LOG(INFO, "test large micro selected finished");

  finish_test();
}

void TestAllCGScanner::test_random(const bool is_reverse)
{
  prepare_test(is_reverse);

  int retry_cnt = 100;
  while (retry_cnt > 0) {
    int64_t start = ObRandom::rand(0, row_cnt_ - 1);
    int64_t locate_count = ObRandom::rand(1, row_cnt_);

    STORAGE_LOG(INFO, "start to locate random range", K(retry_cnt), K(start), K(locate_count), K(row_cnt_));
    ASSERT_EQ(OB_SUCCESS, cg_scanner_->locate(ObCSRange(start, locate_count)));
    check_data(start, locate_count, is_reverse);

    retry_cnt--;
  }
  STORAGE_LOG(INFO, "test random finished");

  finish_test();
}

void TestAllCGScanner::test_border(const bool is_reverse)
{
  prepare_test(is_reverse);

  int64_t start = 0;
  int64_t locate_count = 0;

  start = row_cnt_ - 99;
  locate_count = 100;
  ASSERT_EQ(OB_SUCCESS, cg_scanner_->locate(ObCSRange(start, locate_count)));
  check_data(start, locate_count, is_reverse);

  start = 0;
  locate_count = row_cnt_ + 1;
  ASSERT_EQ(OB_SUCCESS, cg_scanner_->locate(ObCSRange(start, locate_count)));
  check_data(start, locate_count, is_reverse);

  STORAGE_LOG(INFO, "test border finished");

  finish_test();
}

TEST_F(TestAllCGScanner, test_large_micro_selected)
{
  test_large_micro_selected(false);
}

TEST_F(TestAllCGScanner, test_large_micro_selected_reverse)
{
  test_large_micro_selected(true);
}

TEST_F(TestAllCGScanner, test_border)
{
  test_border(false);
}

TEST_F(TestAllCGScanner, test_random)
{
  test_random(false);
}

TEST_F(TestAllCGScanner, test_border_reverse)
{
  test_border(true);
}

TEST_F(TestAllCGScanner, test_random_reverse)
{
  test_random(true);
}

}
}

int main(int argc, char **argv)
{
  system("rm -f test_all_cg_row_scanner.log*");
  OB_LOGGER.set_file_name("test_all_cg_row_scanner.log", true, true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
