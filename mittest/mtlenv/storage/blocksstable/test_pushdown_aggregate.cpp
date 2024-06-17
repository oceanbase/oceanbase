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
#define USING_LOG_PREFIX STORAGE
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "sql/engine/aggregate/ob_aggregate_util.h"
#include "storage/access/ob_pushdown_aggregate.h"
#include "ob_index_block_data_prepare.h"

namespace oceanbase
{
using namespace storage;
using namespace common;
namespace blocksstable
{
class TestPushdownAggregate : public TestIndexBlockDataPrepare
{
public:
  TestPushdownAggregate();
  virtual ~TestPushdownAggregate();
  void reset();
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();
  virtual void prepare_schema();
  void prepare_access_param(const bool is_reverse_scan, ObIArray<share::schema::ObColumnParam*> &col_params);
  void prepare_output_expr(const ObIArray<int32_t> &projector);
  void prepare_agg_expr(const ObIArray<int32_t> &projector, const ObIArray<ObItemType> &iter_types);
public:
  static const int64_t ROWKEY_COLUMN_NUM = 1;
  static const int64_t COLUMN_NUM = 2;
  static const int64_t DATUM_ARRAY_CNT = 1024;
  static const int64_t DATUM_RES_SIZE = 10;
  static const int64_t SQL_BATCH_SIZE = 256;
  ObArenaAllocator allocator_;
  ObTableAccessParam access_param_;
  ObFixedArray<share::schema::ObColumnParam*, ObIAllocator> cols_param_;
  ObFixedArray<int32_t, ObIAllocator> output_cols_project_;
  sql::ExprFixedArray output_exprs_;
  ObFixedArray<int32_t, ObIAllocator> agg_cols_project_;
  ObFixedArray<int32_t, ObIAllocator> group_by_cols_project_;
  sql::ExprFixedArray agg_exprs_;
  ObFixedArray<ObItemType, ObIAllocator> agg_expr_type_;
  void *datum_buf_;
  int64_t datum_buf_offset_;
  sql::ObExecContext exec_ctx_;
  sql::ObEvalCtx eval_ctx_;
  sql::ObPushdownExprSpec expr_spec_;
  sql::ObPushdownOperator op_;
};

TestPushdownAggregate::TestPushdownAggregate()
  : TestIndexBlockDataPrepare("Test pushdown aggregate", compaction::ObMergeType::MAJOR_MERGE),
    exec_ctx_(allocator_),
    eval_ctx_(exec_ctx_),
    expr_spec_(allocator_),
    op_(eval_ctx_, expr_spec_)
{
  reset();
}

TestPushdownAggregate::~TestPushdownAggregate()
{
}

void TestPushdownAggregate::reset()
{
  access_param_.reset();
  cols_param_.reset();
  output_cols_project_.reset();
  output_exprs_.reset();
  agg_cols_project_.reset();
  group_by_cols_project_.reset();
  agg_exprs_.reset();
  agg_expr_type_.reset();
  datum_buf_ = nullptr;
  datum_buf_offset_ = 0;
  allocator_.reset();
}

void TestPushdownAggregate::SetUpTestCase()
{
  TestIndexBlockDataPrepare::SetUpTestCase();
}

void TestPushdownAggregate::TearDownTestCase()
{
  TestIndexBlockDataPrepare::TearDownTestCase();
}

void TestPushdownAggregate::SetUp()
{
  reset();
  TestIndexBlockDataPrepare::SetUp();
  ObLSID ls_id(ls_id_);
  ObTabletID tablet_id(tablet_id_);
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));

  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle_));
  sstable_.key_.table_type_ = ObITable::TableType::MAJOR_SSTABLE;
}

void TestPushdownAggregate::TearDown()
{
  reset();
  tablet_handle_.reset();
  TestIndexBlockDataPrepare::TearDown();
}

void TestPushdownAggregate::prepare_schema()
{
  ObColumnSchemaV2 column;
  const int64_t rowkey_column_num = ROWKEY_COLUMN_NUM;
  const int64_t column_num = COLUMN_NUM;
  // Init table schema
  uint64_t table_id = TEST_TABLE_ID;
  table_schema_.reset();
  ASSERT_EQ(OB_SUCCESS, table_schema_.set_table_name("test_cg_group_by_scanner"));
  table_schema_.set_tenant_id(1);
  table_schema_.set_tablegroup_id(1);
  table_schema_.set_database_id(1);
  table_schema_.set_table_id(table_id);
  table_schema_.set_rowkey_column_num(rowkey_column_num);
  table_schema_.set_max_used_column_id(column_num);
  table_schema_.set_block_size(2 * 1024);
  table_schema_.set_compress_func_name("none");
  table_schema_.set_row_store_type(row_store_type_);
  table_schema_.set_storage_format_version(OB_STORAGE_FORMAT_VERSION_V4);

  index_schema_.reset();

  // Init column
  char name[OB_MAX_FILE_NAME_LENGTH];
  memset(name, 0, sizeof(name));
  for(int64_t i = 0; i < column_num; ++i) {
    ObObjType obj_type = ObIntType;
    column.reset();
    column.set_table_id(TEST_TABLE_ID);
    column.set_column_id(i + OB_APP_MIN_COLUMN_ID);
    sprintf(name, "test%020ld", i);
    ASSERT_EQ(OB_SUCCESS, column.set_column_name(name));
    column.set_data_type(obj_type);
    column.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    column.set_data_length(1);
    if (0 == i) {
      column.set_rowkey_position(1);
    } else {
      column.set_rowkey_position(0);
    }

    ASSERT_EQ(OB_SUCCESS, table_schema_.add_column(column));
  }
}

void TestPushdownAggregate::prepare_access_param(const bool is_reverse_scan, ObIArray<share::schema::ObColumnParam*> &col_params)
{
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
  access_param_.iter_param_.table_id_ = table_schema_.get_table_id();
  access_param_.iter_param_.tablet_id_ = table_schema_.get_table_id();
  access_param_.iter_param_.is_same_schema_column_ = true;
  access_param_.iter_param_.has_virtual_columns_ = false;
  access_param_.iter_param_.vectorized_enabled_ = true;
  access_param_.iter_param_.pd_storage_flag_.pd_blockscan_ = true;
  access_param_.iter_param_.pd_storage_flag_.pd_filter_ = true;
  access_param_.iter_param_.pd_storage_flag_.pd_group_by_ = true;
  access_param_.iter_param_.pd_storage_flag_.use_stmt_iter_pool_ = true;
  access_param_.iter_param_.pd_storage_flag_.use_column_store_ = false;
  read_info_.reset();
  ASSERT_EQ(OB_SUCCESS, read_info_.init(allocator_,
      COLUMN_NUM,
      table_schema_.get_rowkey_column_num(),
      lib::is_oracle_mode(),
      schema_cols_,
      nullptr/*storage_cols_index*/,
      &col_params));
  access_param_.iter_param_.read_info_ = &read_info_;

  //just for test
  ObQueryFlag query_flag(ObQueryFlag::Forward,
                         true, /*is daily merge scan*/
                         true, /*is read multiple macro block*/
                         true, /*sys task scan, read one macro block in single io*/
                         false /*full row scan flag, obsoleted*/,
                         false,/*index back*/
                         false); /*query_stat*/
  query_flag.set_not_use_row_cache();
  query_flag.set_use_block_cache();
  if (is_reverse_scan) {
    query_flag.scan_order_ = ObQueryFlag::ScanOrder::Reverse;
  } else {
    query_flag.scan_order_ = ObQueryFlag::ScanOrder::Forward;
  }
  ObVersionRange scan_version;
  scan_version.base_version_ = 1;
  scan_version.multi_version_start_ = 1;
  scan_version.snapshot_version_ = INT64_MAX;
  ASSERT_EQ(OB_SUCCESS, context_.init(query_flag,
                                      store_ctx_,
                                      allocator_,
                                      allocator_,
                                      scan_version));
  context_.ls_id_ = ls_id_;
  context_.limit_param_ = nullptr;
  context_.is_inited_ = true;
}

void TestPushdownAggregate::prepare_output_expr(const ObIArray<int32_t> &projector)
{
  output_exprs_.set_allocator(&allocator_);
  output_exprs_.init(projector.count());
  for (int64_t i = 0; i < projector.count(); ++i) {
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

void TestPushdownAggregate::prepare_agg_expr(
    const ObIArray<int32_t> &projector,
    const ObIArray<ObItemType> &iter_types)
{
  agg_exprs_.set_allocator(&allocator_);
  agg_exprs_.init(projector.count());
  for (int64_t i = 0; i < projector.count(); ++i) {
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
    expr->type_ = iter_types.at(i);
    expr->basic_funcs_ = ObDatumFuncs::get_basic_func(ObIntType, CS_TYPE_UTF8MB4_GENERAL_CI);
    expr->obj_datum_map_ = OBJ_DATUM_8BYTE_DATA;
    expr->datum_meta_.type_ = ObNumberType;
    expr->datum_meta_.precision_ = MAX_PRECISION_DECIMAL_INT_128;
    agg_exprs_.push_back(expr);

    if (OB_COUNT_AGG_PD_COLUMN_ID != projector.at(i)) {
      for (int64_t j = 0; j < output_cols_project_.count(); ++j) {
        if (projector.at(i) == output_cols_project_.at(j)) {
          agg_exprs_.at(i)->arg_cnt_ = 1;
          agg_exprs_.at(i)->args_ = &output_exprs_.at(j);
        }
      }
    }
  }
}

TEST_F(TestPushdownAggregate, test_init_group_by_cell)
{
  const bool is_reverse_scan = false;
  prepare_access_param(is_reverse_scan, cols_param_);

  const int64_t output_expr_cnt = 2;
  const int64_t agg_expr_cnt = 3;
  eval_ctx_.batch_idx_ = 0;
  eval_ctx_.batch_size_ = SQL_BATCH_SIZE;
  expr_spec_.max_batch_size_ = SQL_BATCH_SIZE;
  datum_buf_ = allocator_.alloc((sizeof(sql::ObDatum) + OBJ_DATUM_NUMBER_RES_SIZE) * DATUM_ARRAY_CNT * 2 * (output_expr_cnt + agg_expr_cnt));
  ASSERT_NE(nullptr, datum_buf_);
  eval_ctx_.frames_ = (char **)(&datum_buf_);

  output_cols_project_.set_allocator(&allocator_);
  output_cols_project_.init(output_expr_cnt);
  output_cols_project_.push_back(0);
  output_cols_project_.push_back(1);

  agg_cols_project_.set_allocator(&allocator_);
  agg_cols_project_.init(agg_expr_cnt);
  agg_cols_project_.push_back(OB_COUNT_AGG_PD_COLUMN_ID);
  agg_cols_project_.push_back(1);
  agg_cols_project_.push_back(1);

  agg_expr_type_.set_allocator(&allocator_);
  agg_expr_type_.init(agg_expr_cnt);
  agg_expr_type_.push_back(T_FUN_COUNT);
  agg_expr_type_.push_back(T_FUN_COUNT);
  agg_expr_type_.push_back(T_FUN_MAX);

  prepare_output_expr(output_cols_project_);
  prepare_agg_expr(agg_cols_project_, agg_expr_type_);

  access_param_.iter_param_.out_cols_project_ = &output_cols_project_;
  access_param_.output_exprs_ = &output_exprs_;
  access_param_.iter_param_.agg_cols_project_ = &agg_cols_project_;
  access_param_.aggregate_exprs_ = &agg_exprs_;
  access_param_.iter_param_.op_ = &op_;

  group_by_cols_project_.set_allocator(&allocator_);
  group_by_cols_project_.init(1);
  group_by_cols_project_.push_back(0);
  access_param_.iter_param_.group_by_cols_project_ = &group_by_cols_project_;

  ObGroupByCell group_by_cell(eval_ctx_.batch_size_, allocator_);
  ASSERT_EQ(OB_SUCCESS, group_by_cell.init(access_param_, context_, eval_ctx_));

  ObIArray<ObAggCell*> &agg_cell = group_by_cell.get_agg_cells();
  ASSERT_EQ(4, group_by_cell.get_agg_cells().count());

  ASSERT_EQ(0, group_by_cell.get_group_by_col_offset());
  ASSERT_TRUE(group_by_cell.get_group_by_col_datums() == output_exprs_.at(0)->locate_batch_datums(eval_ctx_));

  ASSERT_EQ(PD_FIRST_ROW, agg_cell.at(0)->get_type());
  ASSERT_TRUE(agg_cell.at(0)->col_datums_ == output_exprs_.at(1)->locate_batch_datums(eval_ctx_));
  ASSERT_TRUE(agg_cell.at(0)->group_by_result_datum_buf_->get_basic_buf() != output_exprs_.at(1)->locate_batch_datums(eval_ctx_));

  ASSERT_EQ(PD_COUNT, agg_cell.at(1)->get_type());
  ASSERT_TRUE(agg_cell.at(1)->need_access_data());
  ASSERT_TRUE(agg_cell.at(1)->col_datums_ == output_exprs_.at(1)->locate_batch_datums(eval_ctx_));
  ASSERT_TRUE(agg_cell.at(1)->group_by_result_datum_buf_->get_basic_buf() == agg_exprs_.at(1)->locate_batch_datums(eval_ctx_));

  ASSERT_EQ(PD_MAX, agg_cell.at(2)->get_type());
  ASSERT_TRUE(agg_cell.at(2)->need_access_data());
  ASSERT_TRUE(agg_cell.at(2)->col_datums_ == output_exprs_.at(1)->locate_batch_datums(eval_ctx_));
  ASSERT_TRUE(agg_cell.at(2)->group_by_result_datum_buf_->get_basic_buf() == agg_exprs_.at(2)->locate_batch_datums(eval_ctx_));

  ASSERT_EQ(PD_COUNT, agg_cell.at(3)->get_type());
  ASSERT_FALSE(agg_cell.at(3)->need_access_data());
  ASSERT_TRUE(agg_cell.at(3)->col_datums_ == nullptr);
  ASSERT_TRUE(agg_cell.at(3)->group_by_result_datum_buf_->get_basic_buf() == agg_exprs_.at(0)->locate_batch_datums(eval_ctx_));
}

TEST_F(TestPushdownAggregate, test_decide_use_group_by1)
{
  const bool is_reverse_scan = false;
  prepare_access_param(is_reverse_scan, cols_param_);

  const int64_t output_expr_cnt = 2;
  const int64_t agg_expr_cnt = 3;
  eval_ctx_.batch_idx_ = 0;
  eval_ctx_.batch_size_ = SQL_BATCH_SIZE;
  expr_spec_.max_batch_size_ = SQL_BATCH_SIZE;
  datum_buf_ = allocator_.alloc((sizeof(sql::ObDatum) + OBJ_DATUM_NUMBER_RES_SIZE) * DATUM_ARRAY_CNT * 2 * (output_expr_cnt + agg_expr_cnt));
  ASSERT_NE(nullptr, datum_buf_);
  eval_ctx_.frames_ = (char **)(&datum_buf_);

  output_cols_project_.set_allocator(&allocator_);
  output_cols_project_.init(output_expr_cnt);
  output_cols_project_.push_back(0);
  output_cols_project_.push_back(1);

  agg_cols_project_.set_allocator(&allocator_);
  agg_cols_project_.init(agg_expr_cnt);
  agg_cols_project_.push_back(OB_COUNT_AGG_PD_COLUMN_ID);
  agg_cols_project_.push_back(1);
  agg_cols_project_.push_back(1);

  agg_expr_type_.set_allocator(&allocator_);
  agg_expr_type_.init(agg_expr_cnt);
  agg_expr_type_.push_back(T_FUN_COUNT);
  agg_expr_type_.push_back(T_FUN_COUNT);
  agg_expr_type_.push_back(T_FUN_MAX);

  prepare_output_expr(output_cols_project_);
  prepare_agg_expr(agg_cols_project_, agg_expr_type_);

  access_param_.iter_param_.out_cols_project_ = &output_cols_project_;
  access_param_.output_exprs_ = &output_exprs_;
  access_param_.iter_param_.agg_cols_project_ = &agg_cols_project_;
  access_param_.aggregate_exprs_ = &agg_exprs_;
  access_param_.iter_param_.op_ = &op_;

  group_by_cols_project_.set_allocator(&allocator_);
  group_by_cols_project_.init(1);
  group_by_cols_project_.push_back(0);
  access_param_.iter_param_.group_by_cols_project_ = &group_by_cols_project_;

  ObGroupByCell group_by_cell(eval_ctx_.batch_size_, allocator_);
  ASSERT_EQ(OB_SUCCESS, group_by_cell.init(access_param_, context_, eval_ctx_));

  int64_t row_count = 100;
  int64_t distinct_count = 10;
  ObCGBitmap *bitmap = nullptr;
  bool use_group_by = false;
  ASSERT_EQ(OB_SUCCESS, group_by_cell.decide_use_group_by(row_count, row_count, distinct_count, bitmap, use_group_by));
  ASSERT_TRUE(use_group_by);
  ASSERT_FALSE(group_by_cell.is_exceed_sql_batch());
  ASSERT_TRUE(nullptr == group_by_cell.group_by_col_datum_buf_->result_datum_buf_);

  distinct_count = 80;
  ASSERT_EQ(OB_SUCCESS, group_by_cell.decide_use_group_by(row_count, row_count, distinct_count, bitmap, use_group_by));
  ASSERT_FALSE(use_group_by);

  row_count = USE_GROUP_BY_MAX_DISTINCT_CNT * 10;
  distinct_count = USE_GROUP_BY_MAX_DISTINCT_CNT;
  ASSERT_EQ(OB_SUCCESS, group_by_cell.decide_use_group_by(row_count, row_count, distinct_count, bitmap, use_group_by));
  ASSERT_FALSE(use_group_by);

  distinct_count = row_count * ObGroupByCell::USE_GROUP_BY_DISTINCT_RATIO + 2;
  ASSERT_EQ(OB_SUCCESS, group_by_cell.decide_use_group_by(row_count, row_count, distinct_count, bitmap, use_group_by));
  ASSERT_FALSE(use_group_by);

  row_count = SQL_BATCH_SIZE * 10;
  distinct_count = SQL_BATCH_SIZE * 2;
  ASSERT_EQ(OB_SUCCESS, group_by_cell.decide_use_group_by(row_count, row_count, distinct_count, bitmap, use_group_by));
  ASSERT_TRUE(use_group_by);
  ASSERT_TRUE(group_by_cell.is_exceed_sql_batch());
  ASSERT_TRUE(nullptr != group_by_cell.group_by_col_datum_buf_->result_datum_buf_);

  ObIArray<ObAggCell*> &agg_cell = group_by_cell.get_agg_cells();
  ASSERT_EQ(4, group_by_cell.get_agg_cells().count());
  for (int64_t i = 0; i < agg_cell.count(); ++i) {
    ASSERT_TRUE(agg_cell.at(i)->group_by_result_datum_buf_->is_use_extra_data());
    ASSERT_TRUE(0 < agg_cell.at(i)->group_by_result_datum_buf_->extra_block_count_);
  }
}

TEST_F(TestPushdownAggregate, test_decide_use_group_by2)
{
  const bool is_reverse_scan = false;
  prepare_access_param(is_reverse_scan, cols_param_);

  const int64_t output_expr_cnt = 2;
  const int64_t agg_expr_cnt = 3;
  eval_ctx_.batch_idx_ = 0;
  eval_ctx_.batch_size_ = SQL_BATCH_SIZE;
  expr_spec_.max_batch_size_ = SQL_BATCH_SIZE;
  datum_buf_ = allocator_.alloc((sizeof(sql::ObDatum) + OBJ_DATUM_NUMBER_RES_SIZE) * DATUM_ARRAY_CNT * 2 * (output_expr_cnt + agg_expr_cnt));
  ASSERT_NE(nullptr, datum_buf_);
  eval_ctx_.frames_ = (char **)(&datum_buf_);

  output_cols_project_.set_allocator(&allocator_);
  output_cols_project_.init(output_expr_cnt);
  output_cols_project_.push_back(0);
  output_cols_project_.push_back(1);

  agg_cols_project_.set_allocator(&allocator_);
  agg_cols_project_.init(agg_expr_cnt);
  agg_cols_project_.push_back(OB_COUNT_AGG_PD_COLUMN_ID);
  agg_cols_project_.push_back(1);
  agg_cols_project_.push_back(1);

  agg_expr_type_.set_allocator(&allocator_);
  agg_expr_type_.init(agg_expr_cnt);
  agg_expr_type_.push_back(T_FUN_COUNT);
  agg_expr_type_.push_back(T_FUN_COUNT);
  agg_expr_type_.push_back(T_FUN_MAX);

  prepare_output_expr(output_cols_project_);
  prepare_agg_expr(agg_cols_project_, agg_expr_type_);

  access_param_.iter_param_.out_cols_project_ = &output_cols_project_;
  access_param_.output_exprs_ = &output_exprs_;
  access_param_.iter_param_.agg_cols_project_ = &agg_cols_project_;
  access_param_.aggregate_exprs_ = &agg_exprs_;
  access_param_.iter_param_.op_ = &op_;

  group_by_cols_project_.set_allocator(&allocator_);
  group_by_cols_project_.init(1);
  group_by_cols_project_.push_back(0);
  access_param_.iter_param_.group_by_cols_project_ = &group_by_cols_project_;

  ObGroupByCell group_by_cell(eval_ctx_.batch_size_, allocator_);
  ASSERT_EQ(OB_SUCCESS, group_by_cell.init(access_param_, context_, eval_ctx_));

  int64_t row_count = 1000;
  int64_t distinct_count = 10;

  int64_t true_count = row_count / ObGroupByCell::USE_GROUP_BY_FILTER_FACTOR - 2;
  ObCGBitmap bitmap(allocator_);
  bitmap.init(row_count);
  bitmap.reuse(0);
  for (int64_t i = 0; i < row_count; i++) {
    if (i < true_count) {
      bitmap.set(i);
    }
  }
  bool use_group_by = false;
  ASSERT_EQ(OB_SUCCESS, group_by_cell.decide_use_group_by(row_count, row_count, distinct_count, &bitmap, use_group_by));
  ASSERT_FALSE(use_group_by);
  ASSERT_TRUE(nullptr == group_by_cell.distinct_projector_buf_);
  ASSERT_TRUE(nullptr == group_by_cell.tmp_group_by_datum_buf_);

  bitmap.reuse(0);
  true_count = row_count / ObGroupByCell::USE_GROUP_BY_FILTER_FACTOR + 2;
  for (int64_t i = 0; i < row_count; i++) {
    if (i < true_count) {
      bitmap.set(i);
    }
  }
  ASSERT_EQ(OB_SUCCESS, group_by_cell.decide_use_group_by(row_count, row_count, distinct_count, &bitmap, use_group_by));
  ASSERT_TRUE(use_group_by);
  ASSERT_TRUE(nullptr != group_by_cell.distinct_projector_buf_);
  ASSERT_TRUE(nullptr != group_by_cell.tmp_group_by_datum_buf_);

  group_by_cell.set_row_capacity(eval_ctx_.batch_size_ - 1);
  ASSERT_EQ(OB_SUCCESS, group_by_cell.decide_use_group_by(row_count, row_count, distinct_count, &bitmap, use_group_by));
  ASSERT_FALSE(use_group_by);
  group_by_cell.set_row_capacity(eval_ctx_.batch_size_);
  ASSERT_EQ(OB_SUCCESS, group_by_cell.decide_use_group_by(row_count, row_count, distinct_count, &bitmap, use_group_by));
  ASSERT_TRUE(use_group_by);
}

TEST_F(TestPushdownAggregate, test_eval_batch)
{
  const bool is_reverse_scan = false;
  prepare_access_param(is_reverse_scan, cols_param_);

  const int64_t output_expr_cnt = 2;
  const int64_t agg_expr_cnt = 5;
  eval_ctx_.batch_idx_ = 0;
  eval_ctx_.batch_size_ = SQL_BATCH_SIZE;
  expr_spec_.max_batch_size_ = SQL_BATCH_SIZE;
  datum_buf_ = allocator_.alloc((sizeof(sql::ObDatum) + OBJ_DATUM_NUMBER_RES_SIZE) * DATUM_ARRAY_CNT * 2 * (output_expr_cnt + agg_expr_cnt));
  ASSERT_NE(nullptr, datum_buf_);
  eval_ctx_.frames_ = (char **)(&datum_buf_);

  output_cols_project_.set_allocator(&allocator_);
  output_cols_project_.init(output_expr_cnt);
  output_cols_project_.push_back(0);
  output_cols_project_.push_back(1);

  agg_cols_project_.set_allocator(&allocator_);
  agg_cols_project_.init(agg_expr_cnt);
  agg_cols_project_.push_back(OB_COUNT_AGG_PD_COLUMN_ID);
  agg_cols_project_.push_back(1);
  agg_cols_project_.push_back(1);
  agg_cols_project_.push_back(1);
  agg_cols_project_.push_back(1);
  agg_expr_type_.set_allocator(&allocator_);
  agg_expr_type_.init(agg_expr_cnt);
  agg_expr_type_.push_back(T_FUN_COUNT);
  agg_expr_type_.push_back(T_FUN_COUNT);
  agg_expr_type_.push_back(T_FUN_MIN);
  agg_expr_type_.push_back(T_FUN_MAX);
  agg_expr_type_.push_back(T_FUN_SUM);

  prepare_output_expr(output_cols_project_);
  prepare_agg_expr(agg_cols_project_, agg_expr_type_);

  access_param_.iter_param_.out_cols_project_ = &output_cols_project_;
  access_param_.output_exprs_ = &output_exprs_;
  access_param_.iter_param_.agg_cols_project_ = &agg_cols_project_;
  access_param_.aggregate_exprs_ = &agg_exprs_;
  access_param_.iter_param_.op_ = &op_;

  group_by_cols_project_.set_allocator(&allocator_);
  group_by_cols_project_.init(1);
  group_by_cols_project_.push_back(0);
  access_param_.iter_param_.group_by_cols_project_ = &group_by_cols_project_;

  ObGroupByCell group_by_cell(eval_ctx_.batch_size_, allocator_);
  ASSERT_EQ(OB_SUCCESS, group_by_cell.init(access_param_, context_, eval_ctx_));
  ASSERT_EQ(eval_ctx_.batch_size_, group_by_cell.get_batch_size());

  const int64_t distinct_cnt = 2;
  void *buf = allocator_.alloc(sizeof(ObDatum) * SQL_BATCH_SIZE);
  ASSERT_TRUE(nullptr != buf);
  ObDatum *col_datums = new (buf) ObDatum[SQL_BATCH_SIZE];
  buf = allocator_.alloc(OBJ_DATUM_NUMBER_RES_SIZE * SQL_BATCH_SIZE);
  ASSERT_TRUE(nullptr != buf);
  for(int64_t i = 0; i < SQL_BATCH_SIZE; ++i) {
    col_datums[i].pack_ = 0;
    col_datums[i].ptr_ = reinterpret_cast<char*>(buf) + i * OBJ_DATUM_NUMBER_RES_SIZE;
    col_datums[i].set_int(i);
  }
  ASSERT_EQ(OB_SUCCESS, group_by_cell.reserve_group_by_buf(distinct_cnt));
  group_by_cell.set_distinct_cnt(distinct_cnt);
  group_by_cell.set_ref_cnt(SQL_BATCH_SIZE);
  for (int64_t i = 0; i < group_by_cell.get_ref_cnt(); ++i) {
    group_by_cell.refs_buf_[i] = i % distinct_cnt;
  }
  ObIArray<ObAggCell*> &agg_cell = group_by_cell.get_agg_cells();
  ASSERT_EQ(6, group_by_cell.get_agg_cells().count());
  for (int64_t i = 0; i < agg_cell.count(); ++i) {
    ASSERT_EQ(OB_SUCCESS, group_by_cell.eval_batch(col_datums, SQL_BATCH_SIZE, i, false, false));
  }
  ObAggCell *cell = agg_cell.at(0);
  ASSERT_EQ(PD_FIRST_ROW, cell->get_type());
  ASSERT_EQ(0, cell->get_group_by_result_datum(0).get_int());
  ASSERT_EQ(1, cell->get_group_by_result_datum(1).get_int());

  cell = agg_cell.at(1);
  ASSERT_EQ(PD_COUNT, cell->get_type());
  ASSERT_EQ(128, cell->get_group_by_result_datum(0).get_int());
  ASSERT_EQ(128, cell->get_group_by_result_datum(1).get_int());

  cell = agg_cell.at(2);
  ASSERT_EQ(PD_MIN, cell->get_type());
  ASSERT_EQ(0, cell->get_group_by_result_datum(0).get_int());
  ASSERT_EQ(1, cell->get_group_by_result_datum(1).get_int());

  cell = agg_cell.at(3);
  ASSERT_EQ(PD_MAX, cell->get_type());
  ASSERT_EQ(254, cell->get_group_by_result_datum(0).get_int());
  ASSERT_EQ(255, cell->get_group_by_result_datum(1).get_int());

  cell = agg_cell.at(4);
  ASSERT_EQ(PD_SUM, cell->get_type());
  ObSumAggCell *sum_cell = static_cast<ObSumAggCell*>(cell);
  ASSERT_EQ(16256, sum_cell->num_int_buf_->at(0));
  ASSERT_EQ(16384, sum_cell->num_int_buf_->at(1));

  cell = agg_cell.at(5);
  ASSERT_EQ(PD_COUNT, cell->get_type());
  ASSERT_EQ(128, cell->get_group_by_result_datum(0).get_int());
  ASSERT_EQ(128, cell->get_group_by_result_datum(1).get_int());
}

TEST_F(TestPushdownAggregate, test_eval_batch_with_null)
{
  const bool is_reverse_scan = false;
  prepare_access_param(is_reverse_scan, cols_param_);

  const int64_t output_expr_cnt = 2;
  const int64_t agg_expr_cnt = 5;
  eval_ctx_.batch_idx_ = 0;
  eval_ctx_.batch_size_ = SQL_BATCH_SIZE;
  expr_spec_.max_batch_size_ = SQL_BATCH_SIZE;
  datum_buf_ = allocator_.alloc((sizeof(sql::ObDatum) + OBJ_DATUM_NUMBER_RES_SIZE) * DATUM_ARRAY_CNT * 2 * (output_expr_cnt + agg_expr_cnt));
  ASSERT_NE(nullptr, datum_buf_);
  eval_ctx_.frames_ = (char **)(&datum_buf_);

  output_cols_project_.set_allocator(&allocator_);
  output_cols_project_.init(output_expr_cnt);
  output_cols_project_.push_back(0);
  output_cols_project_.push_back(1);

  agg_cols_project_.set_allocator(&allocator_);
  agg_cols_project_.init(agg_expr_cnt);
  agg_cols_project_.push_back(OB_COUNT_AGG_PD_COLUMN_ID);
  agg_cols_project_.push_back(1);
  agg_cols_project_.push_back(1);
  agg_cols_project_.push_back(1);
  agg_cols_project_.push_back(1);
  agg_expr_type_.set_allocator(&allocator_);
  agg_expr_type_.init(agg_expr_cnt);
  agg_expr_type_.push_back(T_FUN_COUNT);
  agg_expr_type_.push_back(T_FUN_COUNT);
  agg_expr_type_.push_back(T_FUN_MIN);
  agg_expr_type_.push_back(T_FUN_MAX);
  agg_expr_type_.push_back(T_FUN_SUM);

  prepare_output_expr(output_cols_project_);
  prepare_agg_expr(agg_cols_project_, agg_expr_type_);

  access_param_.iter_param_.out_cols_project_ = &output_cols_project_;
  access_param_.output_exprs_ = &output_exprs_;
  access_param_.iter_param_.agg_cols_project_ = &agg_cols_project_;
  access_param_.aggregate_exprs_ = &agg_exprs_;
  access_param_.iter_param_.op_ = &op_;

  group_by_cols_project_.set_allocator(&allocator_);
  group_by_cols_project_.init(1);
  group_by_cols_project_.push_back(0);
  access_param_.iter_param_.group_by_cols_project_ = &group_by_cols_project_;

  ObGroupByCell group_by_cell(eval_ctx_.batch_size_, allocator_);
  ASSERT_EQ(OB_SUCCESS, group_by_cell.init(access_param_, context_, eval_ctx_));
  ASSERT_EQ(eval_ctx_.batch_size_, group_by_cell.get_batch_size());

  const int64_t distinct_cnt = 2;
  void *buf = allocator_.alloc(sizeof(ObDatum) * SQL_BATCH_SIZE);
  ASSERT_TRUE(nullptr != buf);
  ObDatum *col_datums = new (buf) ObDatum[SQL_BATCH_SIZE];
  buf = allocator_.alloc(OBJ_DATUM_NUMBER_RES_SIZE * SQL_BATCH_SIZE);
  ASSERT_TRUE(nullptr != buf);
  for(int64_t i = 0; i < SQL_BATCH_SIZE; ++i) {
    col_datums[i].pack_ = 0;
    if (i % 10 == 0) {
      col_datums[i].set_null();
    } else {
      col_datums[i].ptr_ = reinterpret_cast<char*>(buf) + i * OBJ_DATUM_NUMBER_RES_SIZE;
      col_datums[i].set_int(i);
    }
  }
  ASSERT_EQ(OB_SUCCESS, group_by_cell.reserve_group_by_buf(distinct_cnt));
  group_by_cell.set_distinct_cnt(distinct_cnt);
  group_by_cell.set_ref_cnt(SQL_BATCH_SIZE);
  for (int64_t i = 0; i < group_by_cell.get_ref_cnt(); ++i) {
    group_by_cell.refs_buf_[i] = i % distinct_cnt;
  }
  ObIArray<ObAggCell*> &agg_cell = group_by_cell.get_agg_cells();
  ASSERT_EQ(6, group_by_cell.get_agg_cells().count());
  for (int64_t i = 0; i < agg_cell.count(); ++i) {
    ASSERT_EQ(OB_SUCCESS, group_by_cell.eval_batch(col_datums, SQL_BATCH_SIZE, i, false, false));
  }
  ObAggCell *cell = agg_cell.at(0);
  ASSERT_EQ(PD_FIRST_ROW, cell->get_type());
  ASSERT_EQ(2, cell->get_group_by_result_datum(0).get_int());
  ASSERT_EQ(1, cell->get_group_by_result_datum(1).get_int());

  cell = agg_cell.at(1);
  ASSERT_EQ(PD_COUNT, cell->get_type());
  ASSERT_EQ(102, cell->get_group_by_result_datum(0).get_int());
  ASSERT_EQ(128, cell->get_group_by_result_datum(1).get_int());

  cell = agg_cell.at(2);
  ASSERT_EQ(PD_MIN, cell->get_type());
  ASSERT_EQ(2, cell->get_group_by_result_datum(0).get_int());
  ASSERT_EQ(1, cell->get_group_by_result_datum(1).get_int());

  cell = agg_cell.at(3);
  ASSERT_EQ(PD_MAX, cell->get_type());
  ASSERT_EQ(254, cell->get_group_by_result_datum(0).get_int());
  ASSERT_EQ(255, cell->get_group_by_result_datum(1).get_int());

  cell = agg_cell.at(4);
  ASSERT_EQ(PD_SUM, cell->get_type());
  ObSumAggCell *sum_cell = static_cast<ObSumAggCell*>(cell);
  ASSERT_EQ(13006, sum_cell->num_int_buf_->at(0));
  ASSERT_EQ(16384, sum_cell->num_int_buf_->at(1));

  cell = agg_cell.at(5);
  ASSERT_EQ(PD_COUNT, cell->get_type());
  ASSERT_EQ(128, cell->get_group_by_result_datum(0).get_int());
  ASSERT_EQ(128, cell->get_group_by_result_datum(1).get_int());
}

TEST_F(TestPushdownAggregate, test_copy_output_rows)
{
  const bool is_reverse_scan = false;
  prepare_access_param(is_reverse_scan, cols_param_);

  const int64_t output_expr_cnt = 2;
  const int64_t agg_expr_cnt = 5;
  eval_ctx_.batch_idx_ = 0;
  eval_ctx_.batch_size_ = SQL_BATCH_SIZE;
  expr_spec_.max_batch_size_ = SQL_BATCH_SIZE;
  datum_buf_ = allocator_.alloc((sizeof(sql::ObDatum) + OBJ_DATUM_NUMBER_RES_SIZE) * DATUM_ARRAY_CNT * 2 * (output_expr_cnt + agg_expr_cnt));
  ASSERT_NE(nullptr, datum_buf_);
  eval_ctx_.frames_ = (char **)(&datum_buf_);

  output_cols_project_.set_allocator(&allocator_);
  output_cols_project_.init(output_expr_cnt);
  output_cols_project_.push_back(0);
  output_cols_project_.push_back(1);

  agg_cols_project_.set_allocator(&allocator_);
  agg_cols_project_.init(agg_expr_cnt);
  agg_cols_project_.push_back(OB_COUNT_AGG_PD_COLUMN_ID);
  agg_cols_project_.push_back(1);
  agg_cols_project_.push_back(1);
  agg_cols_project_.push_back(1);
  agg_cols_project_.push_back(1);
  agg_expr_type_.set_allocator(&allocator_);
  agg_expr_type_.init(agg_expr_cnt);
  agg_expr_type_.push_back(T_FUN_COUNT);
  agg_expr_type_.push_back(T_FUN_COUNT);
  agg_expr_type_.push_back(T_FUN_MIN);
  agg_expr_type_.push_back(T_FUN_MAX);
  agg_expr_type_.push_back(T_FUN_SUM);

  prepare_output_expr(output_cols_project_);
  prepare_agg_expr(agg_cols_project_, agg_expr_type_);

  access_param_.iter_param_.out_cols_project_ = &output_cols_project_;
  access_param_.output_exprs_ = &output_exprs_;
  access_param_.iter_param_.agg_cols_project_ = &agg_cols_project_;
  access_param_.aggregate_exprs_ = &agg_exprs_;
  access_param_.iter_param_.op_ = &op_;

  group_by_cols_project_.set_allocator(&allocator_);
  group_by_cols_project_.init(1);
  group_by_cols_project_.push_back(0);
  access_param_.iter_param_.group_by_cols_project_ = &group_by_cols_project_;

  ObGroupByCell group_by_cell(eval_ctx_.batch_size_, allocator_);
  ASSERT_EQ(OB_SUCCESS, group_by_cell.init(access_param_, context_, eval_ctx_));
  ASSERT_EQ(eval_ctx_.batch_size_, group_by_cell.get_batch_size());

  ObDatum *col_datums = output_exprs_.at(1)->locate_batch_datums(eval_ctx_);
  void *buf = allocator_.alloc(OBJ_DATUM_NUMBER_RES_SIZE * SQL_BATCH_SIZE);
  ASSERT_TRUE(nullptr != buf);
  for(int64_t i = 0; i < SQL_BATCH_SIZE; ++i) {
    col_datums[i].pack_ = 0;
    if (i % 10 == 0) {
      col_datums[i].set_null();
    } else {
      col_datums[i].ptr_ = reinterpret_cast<char*>(buf) + i * OBJ_DATUM_NUMBER_RES_SIZE;
      col_datums[i].set_int(i);
    }
  }

  ObIArray<ObAggCell*> &agg_cell = group_by_cell.get_agg_cells();
  ASSERT_EQ(6, group_by_cell.get_agg_cells().count());
  ASSERT_EQ(OB_SUCCESS, group_by_cell.copy_output_rows(SQL_BATCH_SIZE));
  ASSERT_EQ(eval_ctx_.batch_size_, group_by_cell.get_distinct_cnt());

  for (int64_t agg_idx = 1; agg_idx < 6; ++agg_idx) {
    ObAggCell *cell = agg_cell.at(agg_idx);
    if (1 == agg_idx || 5 == agg_idx) {
      for (int64_t i = 0; i < SQL_BATCH_SIZE; ++i) {
        if (1 == agg_idx && i % 10 == 0) {
          ASSERT_EQ(0, cell->get_group_by_result_datum(i).get_int()) << "i=" << i << " agg_idx=" << agg_idx;
        } else {
          ASSERT_EQ(1, cell->get_group_by_result_datum(i).get_int()) << "i=" << i << " agg_idx=" << agg_idx;
        }
      }
    } else if (4 == agg_idx) {
      ObSumAggCell *sum_cell = static_cast<ObSumAggCell*>(cell);
      for (int64_t i = 0; i < SQL_BATCH_SIZE; ++i) {
        if (i % 10 == 0) {
          ASSERT_TRUE(cell->get_group_by_result_datum(i).is_null()) << "i=" << i << " agg_idx=" << agg_idx;
        } else {
          sql::ObNumStackAllocator<1> tmp_alloc;
          common::number::ObNumber nmb;
          ASSERT_EQ(OB_SUCCESS, nmb.from(col_datums[i].get_int(), tmp_alloc));
          ASSERT_TRUE(nmb == cell->get_group_by_result_datum(i).get_number()) << "i=" << i << " agg_idx=" << agg_idx;
        }
      }
    } else {
      for (int64_t i = 0; i < SQL_BATCH_SIZE; ++i) {
        if (i % 10 == 0) {
          ASSERT_TRUE(cell->get_group_by_result_datum(i).is_null()) << "i=" << i << " agg_idx=" << agg_idx;
        } else {
          ASSERT_EQ(col_datums[i].get_int(), cell->get_group_by_result_datum(i).get_int()) << "i=" << i << " agg_idx=" << agg_idx;
        }
      }
    }
  }
}

}
}

int main(int argc, char **argv)
{
  system("rm -f test_pushdown_aggregate.log*");
  OB_LOGGER.set_file_name("test_pushdown_aggregate.log", true, true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
