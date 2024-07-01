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
#include "storage/access/ob_vector_store.h"
#include "storage/column_store/ob_cg_group_by_scanner.h"
#include "ob_index_block_data_prepare.h"

namespace oceanbase
{
using namespace storage;
using namespace common;
namespace blocksstable
{

class TestCSCGGroupByScanner : public TestIndexBlockDataPrepare
{
public:
  TestCSCGGroupByScanner();
  virtual ~TestCSCGGroupByScanner();
  void reset();
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();
  virtual void insert_cg_data(ObMacroBlockWriter &data_writer);
  void prepare_access_param(const bool is_reverse_scan, ObIArray<ObColumnParam*> &col_params);
  void prepare_cg_access_param(const bool is_reverse_scan);
  void prepare_output_expr(const ObIArray<int32_t> &projector);
  void prepare_agg_expr(const ObIArray<int32_t> &projector, const ObIArray<ObItemType> &iter_types);
  void prepare_test_case(const bool is_reverse_scan);
public:
  static const int64_t ROWKEY_COLUMN_NUM = 1;
  static const int64_t COLUMN_NUM = 2;
  static const int64_t DATUM_ARRAY_CNT = 1024;
  static const int64_t DATUM_RES_SIZE = 10;
  static const int64_t SQL_BATCH_SIZE = 3;
  ObArenaAllocator allocator_;
  ObTableAccessParam access_param_;
  ObTableAccessParam cg_access_param_;
  ObFixedArray<ObColumnParam*, ObIAllocator> cols_param_;
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

TestCSCGGroupByScanner::TestCSCGGroupByScanner()
  : TestIndexBlockDataPrepare("Test cg group by scanner", ObMergeType::MAJOR_MERGE),
    exec_ctx_(allocator_),
    eval_ctx_(exec_ctx_),
    expr_spec_(allocator_),
    op_(eval_ctx_, expr_spec_)
{
  reset();
  row_store_type_ = CS_ENCODING_ROW_STORE;
  is_cg_data_ = true;
}

TestCSCGGroupByScanner::~TestCSCGGroupByScanner()
{
}

void TestCSCGGroupByScanner::reset()
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

void TestCSCGGroupByScanner::SetUpTestCase()
{
  TestIndexBlockDataPrepare::SetUpTestCase();
}

void TestCSCGGroupByScanner::TearDownTestCase()
{
  TestIndexBlockDataPrepare::TearDownTestCase();
}

void TestCSCGGroupByScanner::SetUp()
{
  reset();
  TestIndexBlockDataPrepare::SetUp();
  ObLSID ls_id(ls_id_);
  ObTabletID tablet_id(tablet_id_);
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));

  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle_));
  sstable_.key_.table_type_ = ObITable::TableType::NORMAL_COLUMN_GROUP_SSTABLE;
}

void TestCSCGGroupByScanner::TearDown()
{
  reset();
  tablet_handle_.reset();
  TestIndexBlockDataPrepare::TearDown();
}

void TestCSCGGroupByScanner::insert_cg_data(ObMacroBlockWriter &data_writer)
{
  row_cnt_ = 0;
  const int64_t test_row_cnt = 50000;
  // 500 rows per micro block;
  ObDatumRow cg_row;
  OK(cg_row.init(1));
  ObDmlFlag flags[] = {DF_INSERT, DF_UPDATE, DF_DELETE};

  while (true) {
    if (row_cnt_ >= test_row_cnt) {
      break;
    }
    cg_row.storage_datums_[0].set_int(100000 + row_cnt_ % 5);
    OK(data_writer.append_row(cg_row));
    if ((row_cnt_ + 1) % 500 == 0) {
      OK(data_writer.build_micro_block());
    }
    if ((row_cnt_ + 1) % 10000 == 0) {
      OK(data_writer.try_switch_macro_block());
    }
    ++row_cnt_;
  }
}

void TestCSCGGroupByScanner::prepare_access_param(const bool is_reverse_scan, ObIArray<share::schema::ObColumnParam*> &col_params)
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
  access_param_.iter_param_.pd_storage_flag_.use_column_store_ = true;
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

void TestCSCGGroupByScanner::prepare_cg_access_param(const bool is_reverse_scan)
{
  cg_access_param_.iter_param_.table_id_ = table_schema_.get_table_id();
  cg_access_param_.iter_param_.tablet_id_ = table_schema_.get_table_id();
  cg_access_param_.iter_param_.is_same_schema_column_ = true;
  cg_access_param_.iter_param_.has_virtual_columns_ = false;
  cg_access_param_.iter_param_.vectorized_enabled_ = true;
  cg_access_param_.iter_param_.pd_storage_flag_.pd_blockscan_ = true;
  cg_access_param_.iter_param_.pd_storage_flag_.pd_filter_ = true;
  cg_access_param_.iter_param_.pd_storage_flag_.use_stmt_iter_pool_ = true;
  cg_access_param_.iter_param_.pd_storage_flag_.use_column_store_ = true;
  cg_access_param_.iter_param_.read_info_ = &cg_read_info_;

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
}

void TestCSCGGroupByScanner::prepare_output_expr(const ObIArray<int32_t> &projector)
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

void TestCSCGGroupByScanner::prepare_agg_expr(
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

void TestCSCGGroupByScanner::prepare_test_case(const bool is_reverse_scan)
{
  prepare_access_param(is_reverse_scan, cols_param_);
  prepare_cg_access_param(is_reverse_scan);

  const int64_t output_expr_cnt = 1;
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

  agg_cols_project_.set_allocator(&allocator_);
  agg_cols_project_.init(agg_expr_cnt);
  agg_cols_project_.push_back(OB_COUNT_AGG_PD_COLUMN_ID);
  agg_cols_project_.push_back(0);
  agg_cols_project_.push_back(0);
  agg_cols_project_.push_back(0);
  agg_cols_project_.push_back(0);
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

  cg_access_param_.iter_param_.out_cols_project_ = &output_cols_project_;
  cg_access_param_.iter_param_.output_exprs_ = &output_exprs_;
  cg_access_param_.iter_param_.agg_cols_project_ = nullptr;
  cg_access_param_.aggregate_exprs_ = nullptr;
  cg_access_param_.iter_param_.op_ = &op_;
}

TEST_F(TestCSCGGroupByScanner, test_init)
{
  const bool is_reverse_scan = false;
  prepare_test_case(is_reverse_scan);

  ObGroupByCell group_by_cell(eval_ctx_.batch_size_, allocator_);
  ASSERT_EQ(OB_SUCCESS, group_by_cell.init(access_param_, context_, eval_ctx_));
  ASSERT_EQ(eval_ctx_.batch_size_, group_by_cell.get_batch_size());
  ObVectorStore vector_store(eval_ctx_.batch_size_, eval_ctx_, context_);
  vector_store.group_by_cell_ = &group_by_cell;
  context_.block_row_store_ = &vector_store;

  ObCGGroupByScanner group_by_scanner;
  ObSSTableWrapper wrapper;
  wrapper.sstable_ = &sstable_;
  ASSERT_EQ(OB_SUCCESS, group_by_scanner.init(cg_access_param_.iter_param_, context_, wrapper));
  ASSERT_EQ(OB_SUCCESS, group_by_scanner.init_group_by_info());
  ASSERT_EQ(1, group_by_scanner.group_by_agg_idxs_.count());
  ObIArray<int32_t> &agg_idxs = group_by_scanner.group_by_agg_idxs_.at(0);
  ASSERT_EQ(5, agg_idxs.count());
}

TEST_F(TestCSCGGroupByScanner, test_decide_group_size)
{
  const bool is_reverse_scan = false;
  prepare_test_case(is_reverse_scan);

  ObGroupByCell group_by_cell(eval_ctx_.batch_size_, allocator_);
  ASSERT_EQ(OB_SUCCESS, group_by_cell.init(access_param_, context_, eval_ctx_));
  ASSERT_EQ(eval_ctx_.batch_size_, group_by_cell.get_batch_size());
  ObVectorStore vector_store(eval_ctx_.batch_size_, eval_ctx_, context_);
  vector_store.group_by_cell_ = &group_by_cell;
  context_.block_row_store_ = &vector_store;

  ObCGGroupByScanner group_by_scanner;
  ObSSTableWrapper wrapper;
  wrapper.sstable_ = &sstable_;
  ASSERT_EQ(OB_SUCCESS, group_by_scanner.init(cg_access_param_.iter_param_, context_, wrapper));
  ASSERT_EQ(OB_SUCCESS, group_by_scanner.init_group_by_info());
  int64_t start = 0;
  int64_t locate_count = row_cnt_;
  ASSERT_EQ(OB_SUCCESS, group_by_scanner.locate_micro_index(ObCSRange(start, locate_count)));
  int64_t group_size = 0;
  ASSERT_EQ(OB_SUCCESS, group_by_scanner.decide_group_size(group_size));
  ASSERT_EQ(500, group_size);
}

TEST_F(TestCSCGGroupByScanner, test_decide_can_group_by)
{
  const bool is_reverse_scan = false;
  prepare_test_case(is_reverse_scan);

  ObGroupByCell group_by_cell(eval_ctx_.batch_size_, allocator_);
  ASSERT_EQ(OB_SUCCESS, group_by_cell.init(access_param_, context_, eval_ctx_));
  ASSERT_EQ(eval_ctx_.batch_size_, group_by_cell.get_batch_size());
  ObVectorStore vector_store(eval_ctx_.batch_size_, eval_ctx_, context_);
  vector_store.group_by_cell_ = &group_by_cell;
  context_.block_row_store_ = &vector_store;

  ObCGGroupByScanner group_by_scanner;
  ObSSTableWrapper wrapper;
  wrapper.sstable_ = &sstable_;
  ASSERT_EQ(OB_SUCCESS, group_by_scanner.init(cg_access_param_.iter_param_, context_, wrapper));
  ASSERT_EQ(OB_SUCCESS, group_by_scanner.init_group_by_info());

  int64_t start = 0;
  int64_t locate_count = row_cnt_;
  ASSERT_EQ(OB_SUCCESS, group_by_scanner.locate_micro_index(ObCSRange(start, locate_count)));
  int64_t group_size = 0;
  ASSERT_EQ(OB_SUCCESS, group_by_scanner.decide_group_size(group_size));
  ASSERT_EQ(500, group_size);

  bool can_group_by = false;
  ASSERT_EQ(OB_SUCCESS, group_by_scanner.locate(ObCSRange(start, group_size)));
  ASSERT_EQ(OB_SUCCESS, group_by_scanner.decide_can_group_by(0, can_group_by));
  ASSERT_TRUE(can_group_by);
}

TEST_F(TestCSCGGroupByScanner, test_read_distinct)
{
  const bool is_reverse_scan = false;
  prepare_test_case(is_reverse_scan);

  ObGroupByCell group_by_cell(eval_ctx_.batch_size_, allocator_);
  ASSERT_EQ(OB_SUCCESS, group_by_cell.init(access_param_, context_, eval_ctx_));
  ASSERT_EQ(eval_ctx_.batch_size_, group_by_cell.get_batch_size());
  ObVectorStore vector_store(eval_ctx_.batch_size_, eval_ctx_, context_);
  vector_store.group_by_cell_ = &group_by_cell;
  context_.block_row_store_ = &vector_store;

  ObCGGroupByScanner group_by_scanner;
  ObSSTableWrapper wrapper;
  wrapper.sstable_ = &sstable_;
  ASSERT_EQ(OB_SUCCESS, group_by_scanner.init(cg_access_param_.iter_param_, context_, wrapper));
  ASSERT_EQ(OB_SUCCESS, group_by_scanner.init_group_by_info());

  int64_t start = 0;
  int64_t locate_count = row_cnt_;
  ASSERT_EQ(OB_SUCCESS, group_by_scanner.locate_micro_index(ObCSRange(start, locate_count)));
  int64_t group_size = 0;
  ASSERT_EQ(OB_SUCCESS, group_by_scanner.decide_group_size(group_size));
  ASSERT_EQ(500, group_size);

  bool can_group_by = false;
  start = 0;
  locate_count = group_size;
  ASSERT_EQ(OB_SUCCESS, group_by_scanner.locate(ObCSRange(start, locate_count)));
  ASSERT_EQ(OB_SUCCESS, group_by_scanner.decide_can_group_by(0, can_group_by));
  ASSERT_TRUE(can_group_by);

  ASSERT_EQ(OB_SUCCESS, group_by_scanner.read_distinct(0));
  ASSERT_EQ(5, group_by_cell.get_distinct_cnt());
  ObDatum *group_by_datums = group_by_cell.get_group_by_col_datums();
  ASSERT_TRUE(nullptr != group_by_datums);
  for (int64_t i = 0; i < 5; ++i) {
    ASSERT_EQ(100000+i, group_by_datums[i].get_int());
  }
}

TEST_F(TestCSCGGroupByScanner, test_read_reference)
{
  const bool is_reverse_scan = false;
  prepare_test_case(is_reverse_scan);

  ObGroupByCell group_by_cell(eval_ctx_.batch_size_, allocator_);
  ASSERT_EQ(OB_SUCCESS, group_by_cell.init(access_param_, context_, eval_ctx_));
  ASSERT_EQ(eval_ctx_.batch_size_, group_by_cell.get_batch_size());
  ObVectorStore vector_store(eval_ctx_.batch_size_, eval_ctx_, context_);
  vector_store.group_by_cell_ = &group_by_cell;
  context_.block_row_store_ = &vector_store;

  ObCGGroupByScanner group_by_scanner;
  ObSSTableWrapper wrapper;
  wrapper.sstable_ = &sstable_;
  ASSERT_EQ(OB_SUCCESS, group_by_scanner.init(cg_access_param_.iter_param_, context_, wrapper));
  ASSERT_EQ(OB_SUCCESS, group_by_scanner.init_group_by_info());

  int64_t start = 0;
  int64_t locate_count = row_cnt_;
  ASSERT_EQ(OB_SUCCESS, group_by_scanner.locate_micro_index(ObCSRange(start, locate_count)));
  int64_t group_size = 0;
  ASSERT_EQ(OB_SUCCESS, group_by_scanner.decide_group_size(group_size));
  ASSERT_EQ(500, group_size);

  bool can_group_by = false;
  start = 0;
  locate_count = group_size;
  ASSERT_EQ(OB_SUCCESS, group_by_scanner.locate(ObCSRange(start, locate_count)));
  ASSERT_EQ(OB_SUCCESS, group_by_scanner.decide_can_group_by(0, can_group_by));
  ASSERT_TRUE(can_group_by);

  int total_ref_cnt = 0;
  while (total_ref_cnt < group_size) {
    ASSERT_EQ(OB_SUCCESS, group_by_scanner.read_reference(0));
    uint32_t *ref_buf = group_by_cell.get_refs_buf();
    ASSERT_TRUE(nullptr != ref_buf);
    for (int64_t j = 0; j < group_by_cell.get_ref_cnt(); ++j) {
      ASSERT_EQ(total_ref_cnt % 5, ref_buf[j]);
      total_ref_cnt++;
    }
  }
  ASSERT_EQ(total_ref_cnt, group_size);
  ASSERT_EQ(OB_ITER_END, group_by_scanner.read_reference(0));
}

TEST_F(TestCSCGGroupByScanner, test_calc_aggregate_group_by)
{
  const bool is_reverse_scan = false;
  prepare_test_case(is_reverse_scan);

  ObGroupByCell group_by_cell(eval_ctx_.batch_size_, allocator_);
  ASSERT_EQ(OB_SUCCESS, group_by_cell.init(access_param_, context_, eval_ctx_));
  ASSERT_EQ(eval_ctx_.batch_size_, group_by_cell.get_batch_size());
  ObVectorStore vector_store(eval_ctx_.batch_size_, eval_ctx_, context_);
  vector_store.group_by_cell_ = &group_by_cell;
  context_.block_row_store_ = &vector_store;

  ObCGGroupByScanner group_by_scanner;
  ObSSTableWrapper wrapper;
  wrapper.sstable_ = &sstable_;
  ASSERT_EQ(OB_SUCCESS, group_by_scanner.init(cg_access_param_.iter_param_, context_, wrapper));
  ASSERT_EQ(OB_SUCCESS, group_by_scanner.init_group_by_info());

  int64_t start = 0;
  int64_t locate_count = row_cnt_;
  ASSERT_EQ(OB_SUCCESS, group_by_scanner.locate_micro_index(ObCSRange(start, locate_count)));
  int64_t group_size = 0;
  ASSERT_EQ(OB_SUCCESS, group_by_scanner.decide_group_size(group_size));
  ASSERT_EQ(500, group_size);

  bool can_group_by = false;
  ASSERT_EQ(OB_SUCCESS, group_by_scanner.locate(ObCSRange(start, group_size)));
  ASSERT_EQ(OB_SUCCESS, group_by_scanner.decide_can_group_by(0, can_group_by));
  ASSERT_TRUE(can_group_by);
  ASSERT_EQ(OB_SUCCESS, group_by_scanner.read_distinct(0));

  int total_ref_cnt = 0;
  while (total_ref_cnt < group_size) {
    ASSERT_EQ(OB_SUCCESS, group_by_scanner.read_reference(0));
    ASSERT_EQ(OB_SUCCESS, group_by_scanner.calc_aggregate(true));
    total_ref_cnt += group_by_cell.get_ref_cnt();
  }
  ASSERT_EQ(total_ref_cnt, group_size);
  ASSERT_EQ(OB_ITER_END, group_by_scanner.read_reference(0));

  ObIArray<ObAggCell*> &agg_cell = group_by_cell.get_agg_cells();
  ASSERT_EQ(5, group_by_cell.get_agg_cells().count());
  ObAggCell *cell = agg_cell.at(0);
  ASSERT_EQ(PD_COUNT, cell->get_type());
  ASSERT_EQ(100, cell->get_group_by_result_datum(0).get_int());
  ASSERT_EQ(100, cell->get_group_by_result_datum(1).get_int());
  ASSERT_EQ(100, cell->get_group_by_result_datum(2).get_int());
  ASSERT_EQ(100, cell->get_group_by_result_datum(3).get_int());
  ASSERT_EQ(100, cell->get_group_by_result_datum(4).get_int());

  cell = agg_cell.at(1);
  ASSERT_EQ(PD_MIN, cell->get_type());
  ASSERT_EQ(100000, cell->get_group_by_result_datum(0).get_int());
  ASSERT_EQ(100001, cell->get_group_by_result_datum(1).get_int());
  ASSERT_EQ(100002, cell->get_group_by_result_datum(2).get_int());
  ASSERT_EQ(100003, cell->get_group_by_result_datum(3).get_int());
  ASSERT_EQ(100004, cell->get_group_by_result_datum(4).get_int());

  cell = agg_cell.at(2);
  ASSERT_EQ(PD_MAX, cell->get_type());
  ASSERT_EQ(100000, cell->get_group_by_result_datum(0).get_int());
  ASSERT_EQ(100001, cell->get_group_by_result_datum(1).get_int());
  ASSERT_EQ(100002, cell->get_group_by_result_datum(2).get_int());
  ASSERT_EQ(100003, cell->get_group_by_result_datum(3).get_int());
  ASSERT_EQ(100004, cell->get_group_by_result_datum(4).get_int());

  cell = agg_cell.at(3);
  ASSERT_EQ(PD_SUM, cell->get_type());
  ObSumAggCell *sum_cell = static_cast<ObSumAggCell*>(cell);
  ASSERT_EQ(10000000, sum_cell->num_int_buf_->at(0));
  ASSERT_EQ(10000100, sum_cell->num_int_buf_->at(1));
  ASSERT_EQ(10000200, sum_cell->num_int_buf_->at(2));
  ASSERT_EQ(10000300, sum_cell->num_int_buf_->at(3));
  ASSERT_EQ(10000400, sum_cell->num_int_buf_->at(4));

  cell = agg_cell.at(4);
  ASSERT_EQ(PD_COUNT, cell->get_type());
  ASSERT_EQ(100, cell->get_group_by_result_datum(0).get_int());
  ASSERT_EQ(100, cell->get_group_by_result_datum(1).get_int());
  ASSERT_EQ(100, cell->get_group_by_result_datum(2).get_int());
  ASSERT_EQ(100, cell->get_group_by_result_datum(3).get_int());
  ASSERT_EQ(100, cell->get_group_by_result_datum(4).get_int());
}

TEST_F(TestCSCGGroupByScanner, test_calc_aggregate_group_by_with_bitmap)
{
  const bool is_reverse_scan = false;
  prepare_test_case(is_reverse_scan);

  ObGroupByCell group_by_cell(eval_ctx_.batch_size_, allocator_);
  ASSERT_EQ(OB_SUCCESS, group_by_cell.init(access_param_, context_, eval_ctx_));
  ASSERT_EQ(eval_ctx_.batch_size_, group_by_cell.get_batch_size());
  ObVectorStore vector_store(eval_ctx_.batch_size_, eval_ctx_, context_);
  vector_store.group_by_cell_ = &group_by_cell;
  context_.block_row_store_ = &vector_store;

  ObCGGroupByScanner group_by_scanner;
  ObSSTableWrapper wrapper;
  wrapper.sstable_ = &sstable_;
  ASSERT_EQ(OB_SUCCESS, group_by_scanner.init(cg_access_param_.iter_param_, context_, wrapper));
  ASSERT_EQ(OB_SUCCESS, group_by_scanner.init_group_by_info());

  int64_t start = 0;
  int64_t locate_count = row_cnt_;
  ASSERT_EQ(OB_SUCCESS, group_by_scanner.locate_micro_index(ObCSRange(start, locate_count)));
  int64_t group_size = 0;
  ASSERT_EQ(OB_SUCCESS, group_by_scanner.decide_group_size(group_size));
  ASSERT_EQ(500, group_size);

  ObCGBitmap bitmap(allocator_);
  bitmap.init(group_size);
  bitmap.reuse(0);
  for (int64_t i = 0; i < group_size; i++) {
    if (i  % 5 == 0 || i % 5 == 2 || i % 5 == 4) {
      bitmap.set(i);
    }
  }

  bool can_group_by = false;
  ASSERT_EQ(OB_SUCCESS, group_by_scanner.locate(ObCSRange(start, group_size), &bitmap));
  ASSERT_EQ(OB_SUCCESS, group_by_scanner.decide_can_group_by(0, can_group_by));
  ASSERT_TRUE(can_group_by);
  ASSERT_EQ(OB_SUCCESS, group_by_scanner.read_distinct(0));
  ASSERT_TRUE(group_by_cell.need_extract_distinct());
  group_by_cell.set_distinct_cnt(0);

  int total_ref_cnt = 0;
  while (total_ref_cnt < 300) {
    ASSERT_EQ(OB_SUCCESS, group_by_scanner.read_reference(0));
    ASSERT_EQ(OB_SUCCESS, group_by_cell.extract_distinct());
    ASSERT_EQ(OB_SUCCESS, group_by_scanner.calc_aggregate(true));
    total_ref_cnt += group_by_cell.get_ref_cnt();
  }
  ASSERT_EQ(total_ref_cnt, 300);
  ASSERT_EQ(OB_ITER_END, group_by_scanner.read_reference(0));

  ASSERT_EQ(3, group_by_cell.get_distinct_cnt());
  ObIArray<ObAggCell*> &agg_cell = group_by_cell.get_agg_cells();
  ASSERT_EQ(5, group_by_cell.get_agg_cells().count());
  ObAggCell *cell = agg_cell.at(0);
  ASSERT_EQ(PD_COUNT, cell->get_type());
  ASSERT_EQ(100, cell->get_group_by_result_datum(0).get_int());
  ASSERT_EQ(100, cell->get_group_by_result_datum(1).get_int());
  ASSERT_EQ(100, cell->get_group_by_result_datum(2).get_int());

  cell = agg_cell.at(1);
  ASSERT_EQ(PD_MIN, cell->get_type());
  ASSERT_EQ(100000, cell->get_group_by_result_datum(0).get_int());
  ASSERT_EQ(100002, cell->get_group_by_result_datum(1).get_int());
  ASSERT_EQ(100004, cell->get_group_by_result_datum(2).get_int());

  cell = agg_cell.at(2);
  ASSERT_EQ(PD_MAX, cell->get_type());
  ASSERT_EQ(100000, cell->get_group_by_result_datum(0).get_int());
  ASSERT_EQ(100002, cell->get_group_by_result_datum(1).get_int());
  ASSERT_EQ(100004, cell->get_group_by_result_datum(2).get_int());

  cell = agg_cell.at(3);
  ASSERT_EQ(PD_SUM, cell->get_type());
  ObSumAggCell *sum_cell = static_cast<ObSumAggCell*>(cell);
  ASSERT_EQ(10000000, sum_cell->num_int_buf_->at(0));
  ASSERT_EQ(10000200, sum_cell->num_int_buf_->at(1));
  ASSERT_EQ(10000400, sum_cell->num_int_buf_->at(2));

  cell = agg_cell.at(4);
  ASSERT_EQ(PD_COUNT, cell->get_type());
  ASSERT_EQ(100, cell->get_group_by_result_datum(0).get_int());
  ASSERT_EQ(100, cell->get_group_by_result_datum(1).get_int());
  ASSERT_EQ(100, cell->get_group_by_result_datum(2).get_int());
}

}
}

int main(int argc, char **argv)
{
  system("rm -f test_cs_cg_group_by_scanner.log*");
  OB_LOGGER.set_file_name("test_cs_cg_group_by_scanner.log", true, true);
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
