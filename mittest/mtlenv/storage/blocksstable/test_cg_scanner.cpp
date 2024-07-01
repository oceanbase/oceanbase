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

#include "lib/random/ob_random.h"
#include "sql/engine/basic/ob_pushdown_filter.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/column_store/ob_cg_prefetcher.h"
#include "storage/column_store/ob_cg_scanner.h"
#include "ob_index_block_data_prepare.h"

namespace oceanbase
{
using namespace storage;
using namespace common;
namespace blocksstable
{
class TestCGScanner : public TestIndexBlockDataPrepare
{
public:
  TestCGScanner();
  virtual ~TestCGScanner();
  static void SetUpTestCase();
  static void TearDownTestCase();

  virtual void SetUp();
  virtual void TearDown();
  void prepare_cg_query_param(const bool is_reverse_scan, const ObVersionRange &scan_version, const uint32_t cg_idx);
  void destroy_cg_query_param();
  void check_data(ObCGRowScanner *cg_scanner, int64_t start, int64_t locate_count, sql::ObDatum *datums, const bool is_reverse);

  void test_border(const bool is_reverse);
  void test_random(const bool is_reverse);


public:
  ObArenaAllocator allocator_;
  ObDatumRow start_row_;
  ObDatumRow end_row_;
  ObTableAccessParam access_param_;
};

TestCGScanner::TestCGScanner()
  : TestIndexBlockDataPrepare("Test cg sstable row scanner", ObMergeType::MAJOR_MERGE)
{
  is_cg_data_ = true;
}

TestCGScanner::~TestCGScanner()
{
}

void TestCGScanner::SetUpTestCase()
{
  TestIndexBlockDataPrepare::SetUpTestCase();
}

void TestCGScanner::TearDownTestCase()
{
  TestIndexBlockDataPrepare::TearDownTestCase();
}

void TestCGScanner::prepare_cg_query_param(const bool is_reverse_scan, const ObVersionRange &scan_version, const uint32_t cg_idx)
{
  schema_cols_.set_allocator(&allocator_);
  schema_cols_.init(table_schema_.get_column_count());
  ASSERT_EQ(OB_SUCCESS, table_schema_.get_column_ids(schema_cols_));
  access_param_.iter_param_.table_id_ = table_schema_.get_table_id();
  access_param_.iter_param_.tablet_id_ = table_schema_.get_table_id();
  access_param_.iter_param_.is_same_schema_column_ = true;
  access_param_.iter_param_.has_virtual_columns_ = false;
  access_param_.iter_param_.vectorized_enabled_ = true;
  access_param_.iter_param_.pd_storage_flag_.pd_blockscan_ = true;
  access_param_.iter_param_.pd_storage_flag_.pd_filter_ = true;
  access_param_.iter_param_.pd_storage_flag_.use_stmt_iter_pool_ = true;
  access_param_.iter_param_.pd_storage_flag_.use_column_store_ = true;
  access_param_.iter_param_.read_info_ = &cg_read_info_;

  //jsut for test
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
  ASSERT_EQ(OB_SUCCESS, context_.init(query_flag,
                                      store_ctx_,
                                      allocator_,
                                      allocator_,
                                      scan_version));
  context_.ls_id_ = ls_id_;
  context_.limit_param_ = nullptr;
  context_.is_inited_ = true;
}

void TestCGScanner::destroy_cg_query_param()
{
  access_param_.reset();
  schema_cols_.reset();
  context_.reset();
  read_info_.reset();
}

void TestCGScanner::SetUp()
{
  TestIndexBlockDataPrepare::SetUp();
  ASSERT_EQ(OB_SUCCESS, start_row_.init(allocator_, TEST_COLUMN_CNT));
  ASSERT_EQ(OB_SUCCESS, end_row_.init(allocator_, TEST_COLUMN_CNT));
  ObLSID ls_id(ls_id_);
  ObTabletID tablet_id(tablet_id_);
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));

  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle_));
  sstable_.key_.table_type_ = ObITable::TableType::NORMAL_COLUMN_GROUP_SSTABLE;
}

void TestCGScanner::TearDown()
{
  tablet_handle_.reset();
  TestIndexBlockDataPrepare::TearDown();
}

void TestCGScanner::check_data(
    ObCGRowScanner *cg_scanner,
    int64_t start,
    int64_t locate_count,
    sql::ObDatum *datums,
    const bool is_reverse)
{
  int ret = OB_SUCCESS;
  uint64_t count = 0;
  int64_t get_count = 0;
  int64_t data_count = MIN(row_cnt_ - start, locate_count);
  int64_t end = MIN(row_cnt_ - 1, start + locate_count - 1);
  int64_t sql_batch_size = 256;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(cg_scanner->get_next_rows(count, sql_batch_size))) {
      if (OB_ITER_END != ret) {
        STORAGE_LOG(WARN, "Fail to get next rows", K(ret));
        ASSERT_EQ(OB_ITER_END, ret);
      } else {
        for (int64_t i = 0; i < count; i++) {
          //STORAGE_LOG(INFO, "get next row info", K(get_count), K(i), K(datums[i].get_int()));
          if (!is_reverse) {
            ASSERT_EQ(start + get_count, datums[i].get_int()) << i << " start " << start << " locate_count " << locate_count;
          } else {
            ASSERT_EQ(end - get_count, datums[i].get_int()) << i << " start " << start << " locate_count " << locate_count;
          }
          get_count++;
        }
      }
    } else {
      for (int64_t i = 0; i < count; i++) {
        //STORAGE_LOG(INFO, "get next row info", K(get_count), K(i), K(datums[i].get_int()));
        if (!is_reverse) {
          ASSERT_EQ(start + get_count, datums[i].get_int()) << i << " start " << start << " locate_count " << locate_count;
        } else {
          ASSERT_EQ(end - get_count, datums[i].get_int()) << i << " start " << start << " locate_count " << locate_count;
        }
        get_count++;
      }
    }
  }
}

void TestCGScanner::test_random(const bool is_reverse)
{
  ObVersionRange scan_version;
  scan_version.base_version_ = 1;
  scan_version.multi_version_start_ = 1;
  scan_version.snapshot_version_ = INT64_MAX;
  // prepare query param
  prepare_cg_query_param(is_reverse, scan_version, 0);
  sql::ObExecContext exec_ctx(allocator_);
  sql::ObEvalCtx eval_ctx(exec_ctx);
  sql::ObPushdownExprSpec expr_spec(allocator_);
  static int64_t array_cnt = 1024;
  eval_ctx.batch_idx_ = 0;
  eval_ctx.batch_size_ = 256;
  expr_spec.max_batch_size_ = 256;
  void *datum_buf = allocator_.alloc((sizeof(sql::ObDatum) + sizeof(int64_t)) * array_cnt * 2);
  sql::ObDatum *datums = new (datum_buf) sql::ObDatum[array_cnt];
  eval_ctx.frames_ = (char **)(&datum_buf);
  sql::ObPushdownOperator op(eval_ctx, expr_spec);
  ObFixedArray<int32_t, ObIAllocator> out_cols_project;
  out_cols_project.set_allocator(&allocator_);
  out_cols_project.init(1);
  out_cols_project.push_back(0);
  sql::ExprFixedArray exprs;
  exprs.set_allocator(&allocator_);
  void *expr_buf = allocator_.alloc(sizeof(sql::ObExpr));
  ASSERT_NE(nullptr, expr_buf);
  sql::ObExpr *expr = reinterpret_cast<sql::ObExpr *>(expr_buf);
  expr->reset();
  expr->res_buf_off_ = sizeof(sql::ObDatum) * array_cnt;
  expr->res_buf_len_ = sizeof(int64_t);
  char *ptr = (char *)datum_buf + expr->res_buf_off_;
  for (int64_t i = 0; i < array_cnt; i++) {
    datums[i].ptr_ = ptr;
    ptr += expr->res_buf_len_;
  }

  exprs.init(1);
  exprs.push_back(expr);
  access_param_.iter_param_.out_cols_project_ = &out_cols_project;
  access_param_.iter_param_.output_exprs_ = &exprs;
  access_param_.iter_param_.op_ = &op;
  void *buf = allocator_.alloc(sizeof(ObBlockRowStore));
  ASSERT_NE(nullptr, buf);
  ObBlockRowStore *block_row_store = new (buf) ObBlockRowStore(context_);
  ASSERT_EQ(OB_SUCCESS, block_row_store->init(access_param_));
  context_.block_row_store_ = block_row_store;

  buf = allocator_.alloc(sizeof(ObCGRowScanner));
  ASSERT_NE(nullptr, buf);
  ObCGRowScanner *cg_scanner = new (buf) ObCGRowScanner();
  ObSSTableWrapper wrapper;
  wrapper.sstable_ = &sstable_;
  ASSERT_EQ(OB_SUCCESS, cg_scanner->init(access_param_.iter_param_, context_, wrapper));

  int retry_cnt = 15;
  while (retry_cnt > 0) {
    int64_t start = ObRandom::rand(0, row_cnt_ - 1);
    int64_t locate_count = ObRandom::rand(1, row_cnt_);

    STORAGE_LOG(INFO, "start to locate random range", K(retry_cnt), K(start), K(locate_count), K(row_cnt_));
    ASSERT_EQ(OB_SUCCESS, cg_scanner->locate(ObCSRange(start, locate_count)));
    check_data(cg_scanner, start, locate_count, datums, is_reverse);

    retry_cnt--;
  }

  STORAGE_LOG(INFO, "test random finished");

  allocator_.free(datum_buf);
  datum_buf = nullptr;
  expr->reset();
  allocator_.free(expr);
  expr = nullptr;
  out_cols_project.reset();
  allocator_.free(block_row_store);
  block_row_store = nullptr;
  cg_scanner->reset();
  allocator_.free(cg_scanner);
  cg_scanner = nullptr;
  destroy_cg_query_param();
}

void TestCGScanner::test_border(const bool is_reverse)
{
  ObVersionRange scan_version;
  scan_version.base_version_ = 1;
  scan_version.multi_version_start_ = 1;
  scan_version.snapshot_version_ = INT64_MAX;
  // prepare query param
  prepare_cg_query_param(is_reverse, scan_version, 0);
  sql::ObExecContext exec_ctx(allocator_);
  sql::ObEvalCtx eval_ctx(exec_ctx);
  sql::ObPushdownExprSpec expr_spec(allocator_);
  static int64_t array_cnt = 1024;
  eval_ctx.batch_idx_ = 0;
  eval_ctx.batch_size_ = 256;
  expr_spec.max_batch_size_ = 256;
  void *datum_buf = allocator_.alloc((sizeof(sql::ObDatum) + sizeof(int64_t)) * array_cnt * 2);
  sql::ObDatum *datums = new (datum_buf) sql::ObDatum[array_cnt];
  eval_ctx.frames_ = (char **)(&datum_buf);
  sql::ObPushdownOperator op(eval_ctx, expr_spec);
  ObFixedArray<int32_t, ObIAllocator> out_cols_project;
  out_cols_project.set_allocator(&allocator_);
  out_cols_project.init(1);
  out_cols_project.push_back(0);
  sql::ExprFixedArray exprs;
  exprs.set_allocator(&allocator_);
  void *expr_buf = allocator_.alloc(sizeof(sql::ObExpr));
  ASSERT_NE(nullptr, expr_buf);
  sql::ObExpr *expr = reinterpret_cast<sql::ObExpr *>(expr_buf);
  expr->reset();
  expr->res_buf_off_ = sizeof(sql::ObDatum) * array_cnt;
  expr->res_buf_len_ = sizeof(int64_t);
  char *ptr = (char *)datum_buf + expr->res_buf_off_;
  for (int64_t i = 0; i < array_cnt; i++) {
    datums[i].ptr_ = ptr;
    ptr += expr->res_buf_len_;
  }

  exprs.init(1);
  exprs.push_back(expr);
  access_param_.iter_param_.out_cols_project_ = &out_cols_project;
  access_param_.iter_param_.output_exprs_ = &exprs;
  access_param_.iter_param_.op_ = &op;
  void *buf = allocator_.alloc(sizeof(ObBlockRowStore));
  ASSERT_NE(nullptr, buf);
  ObBlockRowStore *block_row_store = new (buf) ObBlockRowStore(context_);
  ASSERT_EQ(OB_SUCCESS, block_row_store->init(access_param_));
  context_.block_row_store_ = block_row_store;

  buf = allocator_.alloc(sizeof(ObCGRowScanner));
  ASSERT_NE(nullptr, buf);
  ObCGRowScanner *cg_scanner = new (buf) ObCGRowScanner();
  ObSSTableWrapper wrapper;
  wrapper.sstable_ = &sstable_;
  ASSERT_EQ(OB_SUCCESS, cg_scanner->init(access_param_.iter_param_, context_, wrapper));

  int64_t start = 0;
  int64_t locate_count = 0;
  ASSERT_EQ(OB_INVALID_ARGUMENT, cg_scanner->locate(ObCSRange(start, locate_count)));

  start = row_cnt_;
  locate_count = 1;
  ASSERT_EQ(OB_ITER_END, cg_scanner->locate(ObCSRange(start, locate_count)));

  start = row_cnt_ - 99;
  locate_count = 100;
  ASSERT_EQ(OB_SUCCESS, cg_scanner->locate(ObCSRange(start, locate_count)));
  check_data(cg_scanner, start, locate_count, datums, is_reverse);

  start = 0;
  locate_count = row_cnt_ + 1;
  ASSERT_EQ(OB_SUCCESS, cg_scanner->locate(ObCSRange(start, locate_count)));
  check_data(cg_scanner, start, locate_count, datums, is_reverse);

  STORAGE_LOG(INFO, "test border finished");

  allocator_.free(datum_buf);
  datum_buf = nullptr;
  expr->reset();
  allocator_.free(expr);
  expr = nullptr;
  out_cols_project.reset();
  allocator_.free(block_row_store);
  block_row_store = nullptr;
  cg_scanner->reset();
  allocator_.free(cg_scanner);
  cg_scanner = nullptr;
  destroy_cg_query_param();
}

TEST_F(TestCGScanner, test_large_micro_selected)
{
  ObVersionRange scan_version;
  scan_version.base_version_ = 1;
  scan_version.multi_version_start_ = 1;
  scan_version.snapshot_version_ = INT64_MAX;
  bool is_reverse_scan = false;
  // prepare query param
  prepare_cg_query_param(is_reverse_scan, scan_version, 0);
  sql::ObExecContext exec_ctx(allocator_);
  sql::ObEvalCtx eval_ctx(exec_ctx);
  sql::ObPushdownExprSpec expr_spec(allocator_);
  static int64_t array_cnt = 1024;
  eval_ctx.batch_idx_ = 0;
  eval_ctx.batch_size_ = 256;
  expr_spec.max_batch_size_ = 256;
  void *datum_buf = allocator_.alloc((sizeof(sql::ObDatum) + sizeof(int64_t)) * array_cnt * 2);
  sql::ObDatum *datums = new (datum_buf) sql::ObDatum[array_cnt];
  eval_ctx.frames_ = (char **)(&datum_buf);
  sql::ObPushdownOperator op(eval_ctx, expr_spec);
  ObFixedArray<int32_t, ObIAllocator> out_cols_project;
  out_cols_project.set_allocator(&allocator_);
  out_cols_project.init(1);
  out_cols_project.push_back(0);
  sql::ExprFixedArray exprs;
  exprs.set_allocator(&allocator_);
  void *expr_buf = allocator_.alloc(sizeof(sql::ObExpr));
  ASSERT_NE(nullptr, expr_buf);
  sql::ObExpr *expr = reinterpret_cast<sql::ObExpr *>(expr_buf);
  expr->reset();
  expr->res_buf_off_ = sizeof(sql::ObDatum) * array_cnt;
  expr->res_buf_len_ = sizeof(int64_t);
  char *ptr = (char *)datum_buf + expr->res_buf_off_;
  for (int64_t i = 0; i < array_cnt; i++) {
    datums[i].ptr_ = ptr;
    ptr += expr->res_buf_len_;
  }

  exprs.init(1);
  exprs.push_back(expr);
  access_param_.iter_param_.out_cols_project_ = &out_cols_project;
  access_param_.iter_param_.output_exprs_ = &exprs;
  access_param_.iter_param_.op_ = &op;
  void *buf = allocator_.alloc(sizeof(ObBlockRowStore));
  ASSERT_NE(nullptr, buf);
  ObBlockRowStore *block_row_store = new (buf) ObBlockRowStore(context_);
  ASSERT_EQ(OB_SUCCESS, block_row_store->init(access_param_));
  context_.block_row_store_ = block_row_store;

  buf = allocator_.alloc(sizeof(ObCGRowScanner));
  ASSERT_NE(nullptr, buf);
  ObCGRowScanner *cg_scanner = new (buf) ObCGRowScanner();
  ObSSTableWrapper wrapper;
  wrapper.sstable_ = &sstable_;
  ASSERT_EQ(OB_SUCCESS, cg_scanner->init(access_param_.iter_param_, context_, wrapper));

  int64_t start = 0;
  int64_t locate_count = row_cnt_;
  int64_t sql_batch_size = 256;
  int64_t total_cnt = row_cnt_ / 10;
  ObCGBitmap bitmap(allocator_);
  bitmap.init(row_cnt_);
  bitmap.reuse(0);
  for (int64_t i = 0; i < total_cnt; i++) {
    bitmap.set(i * 10);
  }
  ASSERT_EQ(OB_SUCCESS, cg_scanner->locate(ObCSRange(start, locate_count), &bitmap));
  int ret = OB_SUCCESS;
  uint64_t count = 0;
  int64_t get_count = 0;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(cg_scanner->get_next_rows(count, sql_batch_size))) {
      if (OB_ITER_END != ret) {
        STORAGE_LOG(WARN, "Fail to get next rows", K(ret));
        ASSERT_EQ(OB_ITER_END, ret);
      } else {
        for (uint64_t i = 0; i < count; i++) {
          // STORAGE_LOG(INFO, "get next row info", K(i), K(count), K(datums[i].get_int()));
          ASSERT_EQ(get_count * 10, datums[i].get_int());
          get_count++;
        }
      }
    } else {
      for (uint64_t i = 0; i < count; i++) {
        // STORAGE_LOG(INFO, "get next row info", K(i), K(count), K(datums[i].get_int()));
        ASSERT_EQ(get_count * 10, datums[i].get_int());
        get_count++;
      }
    }
  }
  ASSERT_EQ(get_count, total_cnt);

  STORAGE_LOG(INFO, "test large micro selected finished");

  allocator_.free(datum_buf);
  datum_buf = nullptr;
  expr->reset();
  allocator_.free(expr);
  expr = nullptr;
  out_cols_project.reset();
  allocator_.free(block_row_store);
  block_row_store = nullptr;
  cg_scanner->reset();
  allocator_.free(cg_scanner);
  cg_scanner = nullptr;
  destroy_cg_query_param();
}

TEST_F(TestCGScanner, test_border)
{
  test_border(false);
}

TEST_F(TestCGScanner, test_random)
{
  test_random(false);
}

TEST_F(TestCGScanner, test_border_reverse)
{
  test_border(true);
}

TEST_F(TestCGScanner, test_random_reverse)
{
  test_random(true);
}

TEST_F(TestCGScanner, test_filter)
{
  ObVersionRange scan_version;
  scan_version.base_version_ = 1;
  scan_version.multi_version_start_ = 1;
  scan_version.snapshot_version_ = INT64_MAX;
  // prepare query param
  bool is_reverse = true;
  prepare_cg_query_param(is_reverse, scan_version, 0);
  sql::ObExecContext exec_ctx(allocator_);
  sql::ObEvalCtx eval_ctx(exec_ctx);
  sql::ObPushdownExprSpec expr_spec(allocator_);

  static int64_t array_cnt = 1024;
  eval_ctx.batch_idx_ = 0;
  eval_ctx.batch_size_ = 256;
  expr_spec.max_batch_size_ = 256;
  void *datum_buf = allocator_.alloc((sizeof(sql::ObDatum) + sizeof(int64_t)) * array_cnt * 2);
  sql::ObDatum *datums = new (datum_buf) sql::ObDatum[array_cnt];
  eval_ctx.frames_ = (char **)(&datum_buf);
  sql::ObPushdownOperator op(eval_ctx, expr_spec);
  ObFixedArray<int32_t, ObIAllocator> out_cols_project;
  out_cols_project.set_allocator(&allocator_);
  out_cols_project.init(1);
  out_cols_project.push_back(0);
  sql::ExprFixedArray exprs;
  exprs.set_allocator(&allocator_);
  void *expr_buf = allocator_.alloc(sizeof(sql::ObExpr));
  ASSERT_NE(nullptr, expr_buf);
  sql::ObExpr *expr = reinterpret_cast<sql::ObExpr *>(expr_buf);
  expr->reset();
  expr->res_buf_off_ = sizeof(sql::ObDatum) * array_cnt;
  expr->res_buf_len_ = sizeof(int64_t);
  char *ptr = (char *)datum_buf + expr->res_buf_off_;
  for (int64_t i = 0; i < array_cnt; i++) {
    datums[i].ptr_ = ptr;
    ptr += expr->res_buf_len_;
  }

  sql::ObPushdownWhiteFilterNode white_filter(allocator_);
  white_filter.op_type_ = sql::WHITE_OP_EQ;
  sql::ObWhiteFilterExecutor filter(allocator_, white_filter, op);
  filter.cg_col_offsets_.init(1);
  filter.col_params_.init(1);
  const ObColumnParam *col_param = nullptr;
  filter.col_params_.push_back(col_param);
  filter.cg_col_offsets_.push_back(0);
  filter.n_cols_ = 1;
  ObObj filter_obj;
  filter_obj.set_int(10);
  int64_t arg_cnt = 2;
  void *expr_buf1 = allocator_.alloc(sizeof(sql::ObExpr));
  void *expr_buf2 = allocator_.alloc(sizeof(sql::ObExpr*) * arg_cnt);
  void *expr_buf3 = allocator_.alloc(sizeof(sql::ObExpr) * arg_cnt);
  filter.filter_.expr_ = reinterpret_cast<sql::ObExpr *>(expr_buf1);
  filter.filter_.expr_->arg_cnt_ = arg_cnt;
  filter.filter_.expr_->args_ = reinterpret_cast<sql::ObExpr **>(expr_buf2);
  filter.datum_params_.init(1);
  ObDatum arg_datum;
  void *datum_buf2 = allocator_.alloc((sizeof(int64_t)) * 32);
  filter.filter_.expr_->args_[0] = reinterpret_cast<sql::ObExpr *>(expr_buf3);
  filter.filter_.expr_->args_[0]->obj_meta_ = filter_obj.get_meta();
  filter.filter_.expr_->args_[0]->datum_meta_.type_ = filter_obj.get_meta().get_type();
  arg_datum.ptr_ = (char *)datum_buf2;
  arg_datum.from_obj(filter_obj);
  filter.datum_params_.push_back(arg_datum);
  filter.filter_.expr_->args_[1] = reinterpret_cast<sql::ObExpr *>(expr_buf3) + 1;
  filter.filter_.expr_->args_[1]->type_ = T_REF_COLUMN;
  filter.cmp_func_ = get_datum_cmp_func(filter.filter_.expr_->args_[0]->obj_meta_, filter.filter_.expr_->args_[0]->obj_meta_);

  exprs.init(1);
  exprs.push_back(expr);
  access_param_.iter_param_.out_cols_project_ = &out_cols_project;
  access_param_.iter_param_.output_exprs_ = &exprs;
  access_param_.iter_param_.op_ = &op;
  void *buf = allocator_.alloc(sizeof(ObBlockRowStore));
  ASSERT_NE(nullptr, buf);
  ObBlockRowStore *block_row_store = new (buf) ObBlockRowStore(context_);
  ASSERT_EQ(OB_SUCCESS, block_row_store->init(access_param_));
  context_.block_row_store_ = block_row_store;

  buf = allocator_.alloc(sizeof(ObCGRowScanner));
  ASSERT_NE(nullptr, buf);
  ObCGRowScanner *cg_scanner = new (buf) ObCGRowScanner();
  ObSSTableWrapper wrapper;
  wrapper.sstable_ = &sstable_;
  ASSERT_EQ(OB_SUCCESS, cg_scanner->init(access_param_.iter_param_, context_, wrapper));

  sql::PushdownFilterInfo pd_filter;
  pd_filter.filter_ = &filter;
  pd_filter.is_pd_filter_ = true;
  pd_filter.col_capacity_ = 1;
  pd_filter.batch_size_ = 256;
  pd_filter.allocator_ = &allocator_;
  void *buf3 = allocator_.alloc(sizeof(blocksstable::ObStorageDatum) * 1);
  pd_filter.datum_buf_ = new (buf3) blocksstable::ObStorageDatum[1]();
  buf3 = allocator_.alloc(sizeof(char *) * pd_filter.batch_size_);
  pd_filter.cell_data_ptrs_ = reinterpret_cast<const char **>(buf3);
  buf3 = allocator_.alloc(sizeof(int32_t) * pd_filter.batch_size_);
  pd_filter.row_ids_ = reinterpret_cast<int32_t *>(buf3);
  pd_filter.skip_bit_ = to_bit_vector(allocator_.alloc(ObBitVector::memory_size(256)));
  pd_filter.is_inited_ = true;

  int64_t start = 5;
  int64_t locate_count = 25;
  ObCGBitmap parent_bitmap(allocator_);
  ObCGBitmap bitmap(allocator_);
  bitmap.init(locate_count);
  bitmap.reuse(start, false);
  ASSERT_EQ(OB_SUCCESS, cg_scanner->locate(ObCSRange(start, locate_count)));
  ASSERT_EQ(OB_SUCCESS, cg_scanner->apply_filter(nullptr, pd_filter, locate_count, &parent_bitmap, bitmap));

  ASSERT_EQ(OB_SUCCESS, cg_scanner->locate(ObCSRange(start, locate_count), &bitmap));
  uint64_t count = 0;
  ASSERT_EQ(OB_ITER_END, cg_scanner->get_next_rows(count, 256));
  ASSERT_EQ(1, count);
  ASSERT_EQ(datums[0].get_int(), 10);

  STORAGE_LOG(INFO, "test filter finished");

  allocator_.free(datum_buf);
  datum_buf = nullptr;
  expr->reset();
  allocator_.free(expr);
  expr = nullptr;
  allocator_.free(expr_buf1);
  allocator_.free(expr_buf2);
  allocator_.free(expr_buf3);
  allocator_.free(datum_buf2);
  pd_filter.reset();
  out_cols_project.reset();
  allocator_.free(block_row_store);
  block_row_store = nullptr;
  cg_scanner->reset();
  allocator_.free(cg_scanner);
  cg_scanner = nullptr;
  destroy_cg_query_param();
}

}
}

int main(int argc, char **argv)
{
  system("rm -f test_cg_scanner.log*");
  OB_LOGGER.set_file_name("test_cg_scanner.log", true, true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
