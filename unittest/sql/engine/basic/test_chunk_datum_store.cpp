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

#define USING_LOG_PREFIX SQL

#include <gtest/gtest.h>

#define private public

#include "lib/alloc/ob_malloc_allocator.h"
#include "lib/allocator/ob_malloc.h"
#include "storage/blocksstable/ob_data_file_prepare.h"
#include "storage/blocksstable/ob_tmp_file.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"
#include "sql/engine/basic/ob_ra_row_store.h"
#include "common/row/ob_row_store.h"
#include "share/config/ob_server_config.h"
#include "sql/ob_sql_init.h"
#include "share/datum/ob_datum.h"
#include "sql/engine/expr/ob_expr.h"

namespace oceanbase {
namespace sql {
using namespace common;

class TestEnv : public ::testing::Environment {
public:
  virtual void SetUp() override
  {
    GCONF.enable_sql_operator_dump.set_value("True");
    int ret = OB_SUCCESS;
    lib::ObMallocAllocator* malloc_allocator = lib::ObMallocAllocator::get_instance();
    // ret = malloc_allocator->create_tenant_ctx_allocator(OB_SYS_TENANT_ID);
    // ASSERT_EQ(OB_SUCCESS, ret);
    ret = malloc_allocator->create_tenant_ctx_allocator(OB_SYS_TENANT_ID, common::ObCtxIds::WORK_AREA);
    ASSERT_EQ(OB_SUCCESS, ret);
    int s = (int)time(NULL);
    LOG_INFO("initial setup random seed", K(s));
    srandom(s);
  }

  virtual void TearDown() override
  {}
};

#define CALL(func, ...) \
  func(__VA_ARGS__);    \
  ASSERT_FALSE(HasFatalFailure());

class TestChunkDatumStore : public blocksstable::TestDataFilePrepare {
public:
  TestChunkDatumStore()
      : blocksstable::TestDataFilePrepare("TestDisk_chunk_datum_store", 2 << 20, 5000),
        eval_ctx_(exec_ctx_, eval_res_, eval_tmp_)
  {}

  void init_exprs()
  {
    int ret = OB_SUCCESS;
    int64_t pos = 0;
    eval_ctx_.frames_ = static_cast<char**>(alloc_.alloc(sizeof(void*) * 2));
    ASSERT_EQ(true, nullptr != eval_ctx_.frames_);
    eval_ctx_.frames_[0] = (char*)alloc_.alloc(4096);
    ASSERT_EQ(true, nullptr != eval_ctx_.frames_[0]);
    const int64_t datum_eval_info_size = sizeof(ObDatum) + sizeof(ObEvalInfo);
    for (int64_t i = 0; i < COLS; ++i) {
      ObExpr* expr = static_cast<ObExpr*>(alloc_.alloc(sizeof(ObExpr)));
      ASSERT_EQ(OB_SUCCESS, cells_.push_back(expr));
      expr->frame_idx_ = 0;
      expr->datum_off_ = pos;
      expr->eval_info_off_ = pos + sizeof(ObDatum);
      pos += datum_eval_info_size;
      ObDatum* expr_datum = &expr->locate_expr_datum(eval_ctx_);
      new (expr_datum) ObDatum;
      expr_datum->ptr_ = eval_ctx_.frames_[0] + pos;
      pos += sizeof(int64_t);
    }
    for (int64_t i = 0; i < COLS; ++i) {
      ObExpr* expr = static_cast<ObExpr*>(alloc_.alloc(sizeof(ObExpr)));
      ASSERT_EQ(OB_SUCCESS, ver_cells_.push_back(expr));
      expr->frame_idx_ = 0;
      expr->datum_off_ = pos;
      expr->eval_info_off_ = pos + sizeof(ObDatum);
      pos += datum_eval_info_size;
      ObDatum* expr_datum = &expr->locate_expr_datum(eval_ctx_);
      new (expr_datum) ObDatum;
      expr_datum->ptr_ = eval_ctx_.frames_[0] + pos;
      pos += sizeof(int64_t);
    }
  }
  virtual void SetUp() override
  {
    int ret = OB_SUCCESS;
    ASSERT_EQ(OB_SUCCESS, init_tenant_mgr());
    blocksstable::TestDataFilePrepare::SetUp();
    ret = blocksstable::ObTmpFileManager::get_instance().init();
    ASSERT_EQ(OB_SUCCESS, ret);

    cell_cnt_ = COLS;
    init_exprs();

    // mem limit 1M
    ret = rs_.init(1L << 20, tenant_id_, ctx_id_, label_);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(OB_SUCCESS, rs_.alloc_dir_id());

    memset(str_buf_, 'z', BUF_SIZE);
    LOG_INFO("setup finished");
  }

  int init_tenant_mgr();

  virtual void TearDown() override
  {
    it_.reset();
    rs_.reset();
    rs_.~ObChunkDatumStore();

    blocksstable::ObTmpFileManager::get_instance().destroy();
    ObTmpPageCache::get_instance().allocator_.is_inited_ = false;
    blocksstable::TestDataFilePrepare::TearDown();
    ObTenantManager::get_instance().destroy();
    LOG_WARN("TearDown finished", K_(rs));
  }

  void gen_row(int64_t row_id)
  {
    ObDatum* expr_datum_0 = &cells_.at(0)->locate_expr_datum(eval_ctx_);
    expr_datum_0->set_int(row_id);
    cells_.at(0)->get_eval_info(eval_ctx_).evaluated_ = true;
    int64_t max_size = 512;
    if (enable_big_row_ && row_id > 0 && random() % 100000 < 5) {
      max_size = 1 << 20;
    }
    ObDatum* expr_datum_1 = &cells_.at(1)->locate_expr_datum(eval_ctx_);
    expr_datum_1->set_null();
    cells_.at(1)->get_eval_info(eval_ctx_).evaluated_ = true;

    int64_t size = 10 + random() % max_size;
    ObDatum* expr_datum_2 = &cells_.at(2)->locate_expr_datum(eval_ctx_);
    expr_datum_2->set_string(str_buf_, (int)size);
    cells_.at(2)->get_eval_info(eval_ctx_).evaluated_ = true;
  }

  // varify next row
  // template <typename T>
  void verify_row(ObChunkDatumStore::Iterator& it, int64_t n, bool verify_all = false)
  {
    int ret = it.get_next_row(ver_cells_, eval_ctx_);
    ASSERT_EQ(OB_SUCCESS, ret);

    ObDatum* expr_datum_0 = &ver_cells_.at(0)->locate_expr_datum(eval_ctx_);
    ObDatum* expr_datum_1 = &ver_cells_.at(1)->locate_expr_datum(eval_ctx_);
    ObDatum* expr_datum_2 = &ver_cells_.at(2)->locate_expr_datum(eval_ctx_);
    int64_t v = expr_datum_0->get_int();
    if (verify_all) {
      expr_datum_1->is_null();
      if (0 != strncmp(str_buf_, expr_datum_2->ptr_, expr_datum_2->len_)) {
        LOG_WARN("verify failed", K(v), K(n));
      }
      ASSERT_EQ(0, strncmp(str_buf_, expr_datum_2->ptr_, expr_datum_2->len_));
    }
    if (n >= 0) {
      if (n != v) {
        LOG_WARN("verify failed", K(n), K(v));
      }
      ASSERT_EQ(n, v);
    }
  }

  void verify_next_row(int64_t id = -1, bool verify_all = false)
  {
    if (!it_.is_valid()) {
      ASSERT_EQ(rs_.begin(it_), 0);
    }
    return verify_row(it_, id, verify_all);
  }

  void verify_n_rows(ObChunkDatumStore& rs, ObChunkDatumStore::Iterator& it, int64_t n, bool verify_all = false,
      int64_t chunk_size = 0, int64_t start = 0)
  {
    if (!it.is_valid()) {
      ASSERT_EQ(rs.begin(it), 0);
      it.set_chunk_read_size(chunk_size);
    }
    for (int64_t i = start; i < n; i++) {
      CALL(verify_row, it, i, verify_all);
    }
  }

  void verify_n_rows(int64_t n, bool verify_all = false)
  {
    return verify_n_rows(rs_, it_, n, verify_all);
  }

  void append_rows(ObChunkDatumStore& rs, int64_t cnt)
  {
    int64_t ret = OB_SUCCESS;
    int64_t base = rs.get_row_cnt();
    for (int64_t i = 0; i < cnt; i++) {
      gen_row(base + i);
      ret = rs.add_row(cells_, &eval_ctx_);
      ASSERT_EQ(OB_SUCCESS, ret);
    }
    ASSERT_EQ(base + cnt, rs.get_row_cnt());
  }

  void append_rows(int64_t cnt)
  {
    return append_rows(rs_, cnt);
  }

  void test_time(int64_t block_size, int64_t rows)
  {
    ObArenaAllocator alloc(ObModIds::OB_MODULE_PAGE_ALLOCATOR, 2 << 20);
    ObChunkDatumStore rs(&alloc);
    int64_t v = 0;
    int64_t i;
    int64_t begin = ObTimeUtil::current_time();
    int ret = OB_SUCCESS;
    ret = rs.init(0, tenant_id_, ctx_id_, label_);
    ASSERT_EQ(OB_SUCCESS, ret);
    rs.set_block_size(block_size);
    begin = ObTimeUtil::current_time();
    for (i = 0; i < rows; i++) {
      gen_row(i);
      ret = rs.add_row(cells_, &eval_ctx_);
      ASSERT_EQ(OB_SUCCESS, ret);
    }
    LOG_INFO("rs write time:",
        K(block_size),
        K(rows),
        K(rs.get_block_cnt()),
        K(rs.get_block_list_cnt()),
        K(ObTimeUtil::current_time() - begin));
    begin = ObTimeUtil::current_time();
    ObChunkDatumStore::Iterator it;
    ASSERT_EQ(rs.begin(it), 0);
    i = 0;
    while (OB_SUCC(it.get_next_row(ver_cells_, eval_ctx_))) {
      ObDatum* expr_datum_0 = &ver_cells_.at(0)->locate_expr_datum(eval_ctx_);
      v = *expr_datum_0->int_;
      ASSERT_EQ(i, v);
      i++;
    }
    LOG_INFO("rc scan time:", K(block_size), K(rows), K(ObTimeUtil::current_time() - begin));
  }

  void with_or_without_chunk(bool is_with);

protected:
  const static int64_t COLS = 3;
  bool enable_big_row_ = false;
  int64_t cell_cnt_;
  ObSEArray<ObExpr*, COLS> cells_;
  ObSEArray<ObExpr*, COLS> ver_cells_;
  ObChunkDatumStore rs_;
  ObChunkDatumStore::Iterator it_;

  int64_t tenant_id_ = OB_SYS_TENANT_ID;
  int64_t ctx_id_ = ObCtxIds::WORK_AREA;
  const char* label_ = ObModIds::OB_SQL_ROW_STORE;

  const static int64_t BUF_SIZE = 2 << 20;
  char str_buf_[BUF_SIZE];
  ObArenaAllocator alloc_;
  ObExecContext exec_ctx_;
  ObArenaAllocator eval_res_;
  ObArenaAllocator eval_tmp_;
  ObEvalCtx eval_ctx_;
};

int TestChunkDatumStore::init_tenant_mgr()
{
  int ret = OB_SUCCESS;
  ObTenantManager& tm = ObTenantManager::get_instance();
  ObAddr self;
  oceanbase::rpc::frame::ObReqTransport req_transport(NULL, NULL);
  oceanbase::obrpc::ObSrvRpcProxy rpc_proxy;
  oceanbase::obrpc::ObCommonRpcProxy rs_rpc_proxy;
  oceanbase::share::ObRsMgr rs_mgr;
  int64_t tenant_id = OB_SYS_TENANT_ID;
  self.set_ip_addr("127.0.0.1", 8086);
  ret = tm.init(self, rpc_proxy, rs_rpc_proxy, rs_mgr, &req_transport, &ObServerConfig::get_instance());
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = tm.add_tenant(tenant_id);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = tm.set_tenant_mem_limit(tenant_id, 2L * 1024L * 1024L * 1024L, 4L * 1024L * 1024L * 1024L);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = tm.add_tenant(OB_SYS_TENANT_ID);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = tm.add_tenant(OB_SERVER_TENANT_ID);
  EXPECT_EQ(OB_SUCCESS, ret);
  const int64_t ulmt = 128LL << 30;
  const int64_t llmt = 128LL << 30;
  ret = tm.set_tenant_mem_limit(OB_SYS_TENANT_ID, ulmt, llmt);
  EXPECT_EQ(OB_SUCCESS, ret);
  oceanbase::lib::set_memory_limit(128LL << 32);
  return ret;
}

// Test start
TEST_F(TestChunkDatumStore, basic)
{
  int ret = OB_SUCCESS;
  LOG_WARN("starting basic test: append 3000 rows");
  CALL(append_rows, 3000);  // approximate 1MB, no need to dump
  CALL(verify_n_rows, rs_.get_row_cnt(), true);
  LOG_WARN("basic test: varified rows", K(rs_.get_row_cnt()));
  it_.reset();

  LOG_WARN("starting basic test: append 10000 rows");
  CALL(append_rows, 10000);  // need to dump
  rs_.finish_add_row();
  LOG_WARN("mem", K(rs_.get_mem_hold()), K(rs_.get_mem_used()));
  ASSERT_EQ(13000, rs_.get_row_cnt());
  LOG_WARN("starting basic test: verify rows");
  CALL(verify_n_rows, rs_.get_row_cnt() - 1, true);

  ret = it_.get_next_row(ver_cells_, eval_ctx_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = it_.get_next_row(ver_cells_, eval_ctx_);
  ASSERT_EQ(OB_ITER_END, ret);
  it_.reset();

  ret = it_.get_next_row(ver_cells_, eval_ctx_);
  ASSERT_EQ(OB_SUCCESS, ret);
  LOG_WARN("first row");
  it_.reset();

  rs_.reset();

  LOG_WARN("starting basic test: big row 20000 rows");
  enable_big_row_ = true;
  ret = rs_.init(1L << 20, tenant_id_, ctx_id_, label_);
  ASSERT_EQ(OB_SUCCESS, ret);
  CALL(append_rows, 20000);
  LOG_WARN("mem before finish add", K(rs_.get_mem_hold()), K(rs_.get_mem_used()));
  ASSERT_EQ(20000, rs_.get_row_cnt());
  rs_.finish_add_row();
  LOG_WARN("mem after finish add", K(rs_.get_mem_hold()), K(rs_.get_mem_used()));
  CALL(verify_n_rows, rs_.get_row_cnt(), true);
  LOG_WARN("big row finished");
  it_.reset();
  rs_.reset();
}

TEST_F(TestChunkDatumStore, multi_iter)
{
  int ret = OB_SUCCESS;
  int total = 100;
  int64_t i = 0;
  int64_t j = 0;
  ObChunkDatumStore rs;

  ret = rs.init(1 << 20, tenant_id_, ctx_id_, label_);
  ASSERT_EQ(OB_SUCCESS, ret);

  LOG_WARN("starting basic test: append 3000 rows");
  CALL(append_rows, rs, total);  // approximate 1MB, no need to dump
  rs.finish_add_row();
  LOG_WARN("Multi_iter", K_(rs.mem_hold), K_(rs.mem_used));

  ObChunkDatumStore::Iterator it1;
  ASSERT_EQ(OB_SUCCESS, rs.begin(it1));
  for (i = 0; i < 10; i++) {
    CALL(verify_row, it1, i, true);
    if (i % 1000 == 0) {
      LOG_WARN("verified rows:", K(i));
    }
  }

  ObChunkDatumStore::Iterator it2;
  ASSERT_EQ(OB_SUCCESS, rs.begin(it2));
  for (j = 0; j < 50; j++) {
    CALL(verify_row, it2, j, true);
  }

  for (; i < total; i++) {
    CALL(verify_row, it1, i, true);
  }

  for (; j < total; j++) {
    CALL(verify_row, it2, j, true);
  }

  it1.reset();
  it2.reset();
  LOG_INFO("Multi_iter", K_(rs.mem_hold), K_(rs.mem_used));
  rs.reset();
}

TEST_F(TestChunkDatumStore, basic2)
{
  int ret = OB_SUCCESS;
  ObChunkDatumStore rs;
  ObChunkDatumStore::Iterator it;
  // mem limit 5M
  ret = rs.init(5L << 20, tenant_id_, ctx_id_, label_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, rs.alloc_dir_id());
  CALL(append_rows, rs, 100000);
  ASSERT_GT(rs.get_mem_hold(), 0);
  ASSERT_GT(rs.get_file_size(), 0);
  LOG_WARN("mem and disk", K(rs.get_mem_hold()), K(rs.get_file_size()));
  rs.finish_add_row();
  LOG_WARN("mem and disk after finish add", K(rs.get_mem_hold()), K(rs.get_file_size()));

  CALL(verify_n_rows, rs, it, rs.get_row_cnt(), true);
  it.reset();
  rs.reset();
}

TEST_F(TestChunkDatumStore, chunk_iterator)
{
  int ret = OB_SUCCESS;
  ObChunkDatumStore rs;
  ObChunkDatumStore::ChunkIterator chunk_it;
  ObChunkDatumStore::RowIterator it;
  // mem limit 5M
  ret = rs.init(5L << 20, tenant_id_, ctx_id_, label_);
  ASSERT_EQ(OB_SUCCESS, rs.alloc_dir_id());
  ASSERT_EQ(OB_SUCCESS, ret);
  CALL(append_rows, rs, 100000);
  ASSERT_GT(rs.get_mem_hold(), 0);
  ASSERT_GT(rs.get_file_size(), 0);
  LOG_INFO("mem and disk", K(rs.get_mem_hold()), K(rs.get_file_size()));
  ASSERT_EQ(rs.get_row_cnt(), 100000);
  ASSERT_EQ(rs.get_row_cnt_in_memory() + rs.get_row_cnt_on_disk(), 100000);
  rs.finish_add_row();
  ASSERT_EQ(OB_SUCCESS, rs.begin(chunk_it, ObChunkDatumStore::BLOCK_SIZE * 4));
  const ObChunkDatumStore::StoredRow* v_sr = NULL;
  int64_t v;
  int64_t extend_v;
  int64_t row_cnt = 0;
  int i = 0;
  ret = chunk_it.load_next_chunk(it);
  ASSERT_EQ(OB_SUCCESS, ret);
  row_cnt += chunk_it.get_cur_chunk_row_cnt();
  LOG_WARN("got chunk of rows:", K(row_cnt));
  while (OB_SUCC(ret)) {
    ret = it.get_next_row(v_sr);
    if (ret == OB_ITER_END) {
      ret = chunk_it.load_next_chunk(it);
      if (ret == OB_ITER_END) {
        break;
      }
      ASSERT_EQ(OB_SUCCESS, ret);
      row_cnt += chunk_it.get_cur_chunk_row_cnt();
      LOG_WARN("got chunk of rows:", K(row_cnt));
      ret = it.get_next_row(v_sr);
    }
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = it.convert_to_row(v_sr, ver_cells_, eval_ctx_);
    ASSERT_EQ(OB_SUCCESS, ret);
    ObDatum* expr_datum_0 = &ver_cells_.at(0)->locate_expr_datum(eval_ctx_);
    ObDatum* expr_datum_1 = &ver_cells_.at(1)->locate_expr_datum(eval_ctx_);
    ObDatum* expr_datum_2 = &ver_cells_.at(2)->locate_expr_datum(eval_ctx_);
    v = expr_datum_0->get_int();
    expr_datum_1->is_null();
    if (0 != strncmp(str_buf_, expr_datum_2->ptr_, expr_datum_2->len_)) {
      LOG_WARN("verify failed", K(expr_datum_2->ptr_), K(expr_datum_2->len_));
    }
    ASSERT_EQ(0, strncmp(str_buf_, expr_datum_2->ptr_, expr_datum_2->len_));
    ASSERT_EQ(i, v);
    i++;
  }
  ASSERT_EQ(row_cnt, 100000);
  it.reset();
  chunk_it.reset();
  rs.reset();
}

TEST_F(TestChunkDatumStore, test_copy_row)
{
  int ret = OB_SUCCESS;
  int64_t rows = 1000;
  ObChunkDatumStore rs;
  ObChunkDatumStore::Iterator it;
  const ObChunkDatumStore::StoredRow* sr;
  LOG_WARN("starting mem_perf test: append rows", K(rows));
  int64_t begin = ObTimeUtil::current_time();
  ret = rs.init(0, tenant_id_, ctx_id_, label_);
  ASSERT_EQ(OB_SUCCESS, ret);
  CALL(append_rows, rs, rows);
  ASSERT_EQ(OB_SUCCESS, rs.begin(it));
  ASSERT_EQ(OB_SUCCESS, it.get_next_row(sr));
}

TEST_F(TestChunkDatumStore, mem_perf)
{
  int ret = OB_SUCCESS;
  int64_t rows = 2000000;
  ObChunkDatumStore rs;
  ObChunkDatumStore::Iterator it;
  LOG_WARN("starting mem_perf test: append rows", K(rows));
  int64_t begin = ObTimeUtil::current_time();
  ret = rs.init(0, tenant_id_, ctx_id_, label_);
  ASSERT_EQ(OB_SUCCESS, ret);
  CALL(append_rows, rs, rows);
  LOG_WARN("write time:", K(rows), K(ObTimeUtil::current_time() - begin));
  CALL(verify_n_rows, rs, it, 10000, true);
  LOG_WARN("mem", K(rs.get_mem_hold()), K(rs.get_mem_used()));
  it.reset();
  begin = ObTimeUtil::current_time();
  CALL(verify_n_rows, rs, it, rows, true);
  LOG_WARN("scan time:", K(rows), K(ObTimeUtil::current_time() - begin));
  it.reset();
  rs.reset();
}

TEST_F(TestChunkDatumStore, disk)
{
  int64_t begin = 0;
  int64_t write_time = 0;
  int64_t round = 500;
  int64_t rows = round * 10000;
  LOG_WARN("starting write disk test: append rows", K(rows));
  ObChunkDatumStore rs;
  ObChunkDatumStore::Iterator it;
  ASSERT_EQ(OB_SUCCESS, rs.init(0, tenant_id_, ctx_id_, label_));
  ASSERT_EQ(OB_SUCCESS, rs.alloc_dir_id());
  rs.set_mem_limit(100L << 20);
  for (int64_t i = 0; i < round; i++) {
    if (i == round / 2) {
      enable_big_row_ = true;
    }
    begin = common::ObTimeUtil::current_time();
    CALL(append_rows, rs, 10000);
    write_time += common::ObTimeUtil::current_time() - begin;
  }

  LOG_INFO("mem and disk", K(rows), K(rs.get_mem_hold()), K(rs.get_mem_used()), K(rs.get_file_size()));
  LOG_INFO("disk write:", K(rows), K(write_time));

  ASSERT_EQ(OB_SUCCESS, rs.finish_add_row());
  LOG_WARN("mem and disk after finish", K(rows), K(rs.get_mem_hold()), K(rs.get_mem_used()), K(rs.get_file_size()));

  it.reset();
  begin = ObTimeUtil::current_time();
  CALL(verify_n_rows, rs, it, rs.get_row_cnt(), true);
  LOG_WARN("disk scan time:", K(rows), K(ObTimeUtil::current_time() - begin));
  it.reset();
  rs.reset();
}

TEST_F(TestChunkDatumStore, disk_with_chunk)
{
  int64_t begin = 0;
  int64_t write_time = 0;
  int64_t round = 2;
  int64_t cnt = 10000;
  int64_t rows = round * cnt;
  LOG_WARN("starting write disk test: append rows", K(rows));
  ObChunkDatumStore rs;
  ObChunkDatumStore::Iterator it;
  ASSERT_EQ(OB_SUCCESS, rs.init(0, tenant_id_, ctx_id_, label_));
  ASSERT_EQ(OB_SUCCESS, rs.alloc_dir_id());
  rs.set_mem_limit(1L << 20);
  for (int64_t i = 0; i < round; i++) {
    begin = common::ObTimeUtil::current_time();
    CALL(append_rows, rs, cnt);
    write_time += common::ObTimeUtil::current_time() - begin;
  }

  LOG_INFO("mem and disk", K(rows), K(rs.get_mem_hold()), K(rs.get_mem_used()), K(rs.get_file_size()));
  LOG_INFO("disk write:", K(rows), K(write_time));

  ASSERT_EQ(OB_SUCCESS, rs.finish_add_row());
  LOG_INFO("mem and disk after finish",
      K(rows),
      K(rs.get_mem_hold()),
      K(rs.get_mem_used()),
      K(rs.get_file_size()),
      K(rs.max_blk_size_),
      K(rs.min_blk_size_),
      K(rs.n_block_in_file_));

  it.reset();
  begin = ObTimeUtil::current_time();
  CALL(verify_n_rows, rs, it, rs.get_row_cnt(), true);
  LOG_INFO("disk without chunk scan time:", K(rows), K(ObTimeUtil::current_time() - begin));
  it.reset();

  begin = ObTimeUtil::current_time();
  CALL(verify_n_rows, rs, it, rs.get_row_cnt(), true);
  LOG_INFO("disk without chunk scan time2:", K(rows), K(ObTimeUtil::current_time() - begin));
  it.reset();

  begin = ObTimeUtil::current_time();
  CALL(verify_n_rows, rs, it, rs.get_row_cnt(), true, 1L << 20);
  LOG_INFO("disk with chunk scan time:", K(rows), K(ObTimeUtil::current_time() - begin));
  it.reset();

  begin = ObTimeUtil::current_time();
  CALL(verify_n_rows, rs, it, rs.get_row_cnt(), true, 2L << 20);
  LOG_INFO("disk with chunk scan time:", K(rows), K(ObTimeUtil::current_time() - begin));
  it.reset();

  begin = ObTimeUtil::current_time();
  CALL(verify_n_rows, rs, it, rs.get_row_cnt(), true, 8L << 20);
  LOG_INFO("disk with chunk scan time:", K(rows), K(ObTimeUtil::current_time() - begin));
  it.reset();

  begin = ObTimeUtil::current_time();
  CALL(verify_n_rows, rs, it, rs.get_row_cnt(), true, 10L << 20);
  LOG_INFO("disk with chunk scan time:", K(rows), K(ObTimeUtil::current_time() - begin));
  it.reset();

  begin = ObTimeUtil::current_time();
  CALL(verify_n_rows, rs, it, rs.get_row_cnt(), true, 16L << 20);
  LOG_INFO("disk with chunk scan time:", K(rows), K(ObTimeUtil::current_time() - begin));
  it.reset();

  begin = ObTimeUtil::current_time();
  CALL(verify_n_rows, rs, it, rs.get_row_cnt(), true, 20L << 20);
  LOG_INFO("disk with chunk scan time:", K(rows), K(ObTimeUtil::current_time() - begin));
  it.reset();

  rs.reset();
}

TEST_F(TestChunkDatumStore, test_add_block)
{
  int ret = OB_SUCCESS;
  // send
  ObChunkDatumStore rs;
  ObChunkDatumStore::Block* block;
  ObArenaAllocator alloc(ObModIds::OB_MODULE_PAGE_ALLOCATOR, 2 << 20);

  gen_row(1);
  int64_t row_size = 0;
  ASSERT_EQ(OB_SUCCESS, ObChunkDatumStore::Block::row_store_size(cells_, eval_ctx_, row_size));

  ret = rs.init(0, tenant_id_, ctx_id_, label_);
  ASSERT_EQ(OB_SUCCESS, ret);

  int64_t min_size = rs.min_blk_size(row_size);
  void* mem = alloc.alloc(min_size);

  ret = rs.init_block_buffer(mem, min_size, block);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = rs.add_block(block, false);
  ASSERT_EQ(OB_SUCCESS, ret);

  CALL(append_rows, rs, 100);
  rs.finish_add_row();
  ret = block->unswizzling();
  ASSERT_EQ(OB_SUCCESS, ret);
  rs.remove_added_blocks();
  rs.reset();
  LOG_INFO("Molly size", K(block->get_buffer()->data_size()), K(block->get_buffer()->capacity()));

  ObArenaAllocator alloc2(ObModIds::OB_MODULE_PAGE_ALLOCATOR, 2 << 20);
  void* mem2 = alloc2.alloc(block->get_buffer()->data_size());
  memcpy(mem2, mem, block->get_buffer()->data_size());

  // recv
  ObChunkDatumStore rs2;
  ObChunkDatumStore::Block* block2 = reinterpret_cast<ObChunkDatumStore::Block*>(mem2);
  ret = rs2.init(0, tenant_id_, ctx_id_, label_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = rs2.add_block(block2, true);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObChunkDatumStore::Iterator it2;
  ASSERT_EQ(OB_SUCCESS, rs2.begin(it2));
  CALL(verify_n_rows, rs2, it2, 99, true);
  ASSERT_EQ(true, it2.has_next());
  CALL(verify_n_rows, rs2, it2, 100, true, 0, 99);
  ASSERT_EQ(false, it2.has_next());
  ASSERT_EQ(OB_ITER_END, it2.get_next_row(ver_cells_, eval_ctx_));
  rs2.remove_added_blocks();
  it2.reset();
  rs2.reset();
}

TEST_F(TestChunkDatumStore, row_with_extend_size)
{
  int64_t begin = 0;
  int64_t write_time = 0;
  int64_t round = 500;
  int64_t rows = round * 10000;
  LOG_INFO("starting write disk test: append rows", K(rows));
  ObChunkDatumStore rs;
  ObChunkDatumStore::Iterator it;
  ASSERT_EQ(OB_SUCCESS, rs.init(0, tenant_id_, ctx_id_, label_, true, 8));
  LOG_INFO("starting basic test: append 3000 rows");
  int64_t ret = OB_SUCCESS;
  int64_t base = rs.get_row_cnt();
  ObChunkDatumStore::StoredRow* sr = NULL;
  const ObChunkDatumStore::StoredRow* v_sr = NULL;
  for (int64_t i = 0; i < 100; i++) {
    gen_row(i);
    ret = rs.add_row(cells_, &eval_ctx_, &sr);
    ASSERT_EQ(OB_SUCCESS, ret);
    *static_cast<int64_t*>(sr->get_extra_payload()) = i;
  }
  ASSERT_EQ(OB_SUCCESS, rs.begin(it));
  sr = NULL;
  int64_t v;
  int64_t extend_v;
  for (int64_t i = 0; i < 100; i++) {
    int ret = it.get_next_row(v_sr);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = it.convert_to_row(v_sr, ver_cells_, eval_ctx_);
    ASSERT_EQ(OB_SUCCESS, ret);
    ObDatum* expr_datum_0 = &ver_cells_.at(0)->locate_expr_datum(eval_ctx_);
    ObDatum* expr_datum_1 = &ver_cells_.at(1)->locate_expr_datum(eval_ctx_);
    ObDatum* expr_datum_2 = &ver_cells_.at(2)->locate_expr_datum(eval_ctx_);
    v = expr_datum_0->get_int();
    expr_datum_1->is_null();
    if (0 != strncmp(str_buf_, expr_datum_2->ptr_, expr_datum_2->len_)) {
      LOG_WARN("verify failed");
    }
    ASSERT_EQ(0, strncmp(str_buf_, expr_datum_2->ptr_, expr_datum_2->len_));
    extend_v = *(static_cast<int64_t*>(v_sr->get_extra_payload()));
    if (i != v || i != extend_v) {
      LOG_WARN("verify failed", K(i), K(v), K(extend_v));
    }
    ASSERT_EQ(i, v);
  }
  it.reset();
  rs.reset();
}

TEST_F(TestChunkDatumStore, test_only_disk_data)
{
  int64_t round = 2;
  int64_t cnt = 10000;
  int64_t rows = round * cnt;
  LOG_WARN("starting write disk test: append rows", K(rows));
  ObChunkDatumStore rs;
  ASSERT_EQ(OB_SUCCESS, rs.alloc_dir_id());
  ObChunkDatumStore::Iterator it;
  ASSERT_EQ(OB_SUCCESS, rs.init(0, tenant_id_, ctx_id_, label_));
  rs.set_mem_limit(1L << 30);
  // disk data
  CALL(append_rows, rs, cnt);
  ASSERT_EQ(OB_SUCCESS, rs.dump(false, true));
  rs.finish_add_row();

  CALL(verify_n_rows, rs, it, rs.get_row_cnt(), true, 16L << 20);
  it.reset();
  CALL(verify_n_rows, rs, it, rs.get_row_cnt(), true, ObChunkDatumStore::BLOCK_SIZE);
  it.reset();
  CALL(verify_n_rows, rs, it, rs.get_row_cnt(), true, 2 * ObChunkDatumStore::BLOCK_SIZE);
  LOG_INFO("row store data count", K(rs.get_row_cnt_on_disk()), K(rs.get_row_cnt_in_memory()));

  it.reset();
  rs.reset();
}

TEST_F(TestChunkDatumStore, test_only_disk_data1)
{
  int64_t round = 2;
  int64_t cnt = 10000;
  int64_t rows = round * cnt;
  LOG_WARN("starting write disk test: append rows", K(rows));
  ObChunkDatumStore rs;
  ASSERT_EQ(OB_SUCCESS, rs.alloc_dir_id());
  ObChunkDatumStore::Iterator it;
  ASSERT_EQ(OB_SUCCESS, rs.init(0, tenant_id_, ctx_id_, label_));
  rs.set_mem_limit(1L << 30);
  // disk data
  CALL(append_rows, rs, cnt);
  ASSERT_EQ(OB_SUCCESS, rs.dump(false, true));
  rs.finish_add_row();

  CALL(verify_n_rows, rs, it, rs.get_row_cnt(), true, ObChunkDatumStore::BLOCK_SIZE);
  it.reset();
  ObChunkDatumStore::Iterator it2;
  CALL(verify_n_rows, rs, it2, rs.get_row_cnt(), true, 0);
  it2.reset();
  CALL(verify_n_rows, rs, it2, rs.get_row_cnt(), true, 0);
  it2.reset();
  CALL(verify_n_rows, rs, it2, rs.get_row_cnt(), true, 0);
  it2.reset();
  LOG_INFO("row store data count", K(rs.get_row_cnt_on_disk()), K(rs.get_row_cnt_in_memory()));

  it.reset();
  rs.reset();
}

}  // end namespace sql
}  // end namespace oceanbase

void ignore_sig(int sig)
{
  UNUSED(sig);
}

int main(int argc, char** argv)
{
  signal(49, ignore_sig);
  oceanbase::sql::init_sql_factories();
  oceanbase::common::ObLogger::get_logger().set_file_name("test_chunk_datum_store.log", true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  auto* env = new (oceanbase::sql::TestEnv);
  testing::AddGlobalTestEnvironment(env);
  int ret = RUN_ALL_TESTS();
  OB_LOGGER.disable();
  return ret;
}
