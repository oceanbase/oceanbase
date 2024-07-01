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

// #define USING_LOG_PREFIX SQL_ENGINE
#define USING_LOG_PREFIX COMMON
#include <iterator>
#include <gtest/gtest.h>
#include "sql/engine/test_op_engine.h"
#include "sql/engine/ob_test_config.h"
#include "sql/engine/basic/ob_temp_row_store.h"
#include <vector>
#include <string>
#include <gtest/gtest.h>
#include "lib/alloc/ob_malloc_allocator.h"
#include "lib/allocator/ob_malloc.h"
#include "storage/blocksstable/ob_data_file_prepare.h"
#include "storage/blocksstable/ob_tmp_file.h"
#include "sql/engine/basic/ob_temp_row_store.h"
#include "sql/engine/basic/ob_ra_row_store.h"
#include "common/row/ob_row_store.h"
#include "share/config/ob_server_config.h"
#include "sql/ob_sql_init.h"
#include "share/datum/ob_datum.h"
#include "sql/engine/expr/ob_expr.h"
#include "share/ob_simple_mem_limit_getter.h"
#include "share/vector/ob_fixed_length_vector.h"

using namespace ::oceanbase::sql;

namespace test
{
namespace sql
{
using namespace common;
static ObSimpleMemLimitGetter getter;

#define CALL(func, ...) func(__VA_ARGS__); ASSERT_FALSE(HasFatalFailure());

struct MyAllocator : public DefaultPageAllocator
{
  void *alloc(const int64_t sz, const ObMemAttr &attr) override
  {
    int64_t size = sz + 8;
    uint64_t *mem = (uint64_t *)DefaultPageAllocator::alloc(size, attr);
    if (NULL != mem) {
      *mem = sz;
      total_ += sz;
      return mem + 1;
    }
    return NULL;
  }

  void free(void *p)
  {
    if (NULL != p) {
      uint64_t *mem = ((uint64_t *)p - 1);
      total_ -= *mem;
      memset(p, 0xAA, *mem);
      DefaultPageAllocator::free(mem);
    }
  }

  int64_t total_ = 0;
};

class TestRATempRowStore : public TestOpEngine
{
public:
  TestRATempRowStore();
  virtual ~TestRATempRowStore();
  virtual void SetUp();
  virtual void TearDown();

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestRATempRowStore);

protected:
  // function members
  void init_exprs() {
    int ret = OB_SUCCESS;
    std::string test_file_path = ObTestOpConfig::get_instance().test_filename_prefix_ + ".test";
    std::ifstream if_tests(test_file_path);
    ASSERT_EQ(if_tests.is_open(), true);
    std::string line;
    bool first_line = true;
    while (std::getline(if_tests, line)) {
      // handle query
      if (line.size() <= 0) continue;
      if (line.at(0) == '#') continue;

      if (first_line) {
        ObOperator *op = NULL;
        ObExecutor exector;
        ASSERT_EQ(OB_SUCCESS, get_tested_op_from_string(line, true, op, exector));
        int round = 1;
        const int64_t max_row_cnt = 256;
        const ObBatchRows *child_brs = nullptr;
        while (!op->brs_.end_) {
          ASSERT_EQ(OB_SUCCESS, op->get_next_batch(max_row_cnt, child_brs));
          if (cells_.count_ == 0 && op->brs_.size_ > 0) {
            FOREACH_CNT_X(e, op->spec_.output_, OB_SUCC(ret))
            {
              ASSERT_EQ(OB_SUCCESS, cells_.push_back(*e));
            }
            eval_ctx_ = new ObEvalCtx(op->eval_ctx_);
          }
        }
        first_line = false;
      } else {
        ObOperator *op = NULL;
        ObExecutor exector;
        ASSERT_EQ(OB_SUCCESS, get_tested_op_from_string(line, true, op, exector, true));
        int round = 1;
        const int64_t max_row_cnt = 256;
        const ObBatchRows *child_brs = nullptr;
        while (!op->brs_.end_) {
          ASSERT_EQ(OB_SUCCESS, op->get_next_batch(max_row_cnt, child_brs));
          if (ver_cells_.count_ == 0 && op->brs_.size_ > 0) {
            FOREACH_CNT_X(e, op->spec_.output_, OB_SUCC(ret))
            {
              ASSERT_EQ(OB_SUCCESS, ver_cells_.push_back(*e));
            }
            ver_eval_ctx_ = new ObEvalCtx(op->eval_ctx_);
          }
        }
      }
      exec_ctx_.~ObExecContext();
      new (&exec_ctx_) ObExecContext(allocator_);
      exec_ctx_.set_sql_ctx(&sql_ctx_);
      exec_ctx_.set_my_session(&session_info_);
      exec_ctx_.create_physical_plan_ctx();

      vec_2_exec_ctx_.~ObExecContext();
      new (&vec_2_exec_ctx_) ObExecContext(allocator_);
      vec_2_exec_ctx_.set_sql_ctx(&sql_ctx_);
      vec_2_exec_ctx_.set_my_session(&session_info_);
      vec_2_exec_ctx_.create_physical_plan_ctx();
    }
    if_tests.close();
  }

  void gen_row(int64_t row_id, int64_t idx = 0)
  {
    ObIVector *vector_0 = cells_.at(0)->get_vector(*eval_ctx_);
    vector_0->set_int(idx, row_id);
    ObIVector *vector_1 = cells_.at(1)->get_vector(*eval_ctx_);
    vector_1->set_null(idx);

    int64_t size = 10 + random() % BUF_SIZE;
    ObIVector *vector_2 = cells_.at(2)->get_vector(*eval_ctx_);
    vector_2->set_string(idx, str_buf_, (int)size);
  }

  void verify_row(ObRATempRowStore::RAReader& it, int64_t n, bool verify_all = false)
  {
    int ret = it.get_row(n, stored_row_);
    ASSERT_EQ(OB_SUCCESS, ret);
    verify_row_data(n, verify_all);
  }

  void verify_row_data(int64_t n, bool verify_all = false, int64_t idx = 0, bool batch_verify = false)
  {
    ObIVector *vector_0 = ver_cells_.at(0)->get_vector(*ver_eval_ctx_);
    ObIVector *vector_1 = ver_cells_.at(1)->get_vector(*ver_eval_ctx_);
    ObIVector *vector_2 = ver_cells_.at(2)->get_vector(*ver_eval_ctx_);
    if (!batch_verify) {
      vector_0->from_row(row_meta_, stored_row_, idx, 0);
      vector_1->from_row(row_meta_, stored_row_, idx, 1);
      vector_2->from_row(row_meta_, stored_row_, idx, 2);
    }
    int64_t v = vector_0->get_int(idx);
    if (n >= 0) {
      ASSERT_EQ(n, v);
    }
    if (verify_all) {
      ASSERT_EQ(true, vector_1->is_null(idx));
      ASSERT_EQ(0, strncmp(str_buf_, vector_2->get_payload(idx), vector_2->get_length(idx)));
    }
  }

  void verify_n_rows(ObRATempRowStore &rs, ObRATempRowStore::RAReader &it, int64_t n,
                     bool verify_all = false, int64_t chunk_size = 0, int64_t start = 0)
  {
    for (int64_t i = start; i < n; i++) {
      verify_row(it, i, verify_all);
    }
  }

  void verify_n_rows(int64_t n, bool verify_all = false)
  {
    return verify_n_rows(rs_, reader_, n, verify_all);
  }

  void append_rows(ObRATempRowStore &rs, int64_t cnt)
  {
    int64_t ret = OB_SUCCESS;
    int64_t base = rs.get_row_cnt();
    ObCompactRow *sr = nullptr;
    for (int64_t i = 0; i < cnt; i++) {
      gen_row(base + i);
      ret = rs.add_row(cells_, *eval_ctx_, sr);
      ASSERT_EQ(OB_SUCCESS, ret);
    }
    ASSERT_EQ(base + cnt, rs.get_row_cnt());
  }

  void batch_append_rows(int64_t cnt)
  {
    int64_t ret = 0;
    int64_t base = rs_.get_row_cnt();
    skip_->reset(batch_size_);
    for (int64_t i = 0; i < cnt;) {
      int64_t bcnt = std::min(cnt - i, batch_size_);
      for (int64_t j = 0; j < bcnt; j++, i++) {
        gen_row(base + i, j);
      }
      int64_t stored_row_cnt = 0;
      ObCompactRow **srs = nullptr;
      ObBatchRows *br = new ObBatchRows();
      br->skip_ = skip_;
      br->all_rows_active_ = true;
      br->size_ = bcnt;
      ret = rs_.add_batch(cells_, *eval_ctx_, *br, stored_row_cnt, srs);
      ASSERT_EQ(OB_SUCCESS, ret);
      ASSERT_EQ(stored_row_cnt, bcnt);
    }
    ASSERT_EQ(base + cnt, rs_.get_row_cnt());
  }

  void batch_verify_all(int64_t row_cnt)
  {
    reader_.reset();
    int64_t read_cnt = 0;
    int ret = OB_SUCCESS;
    while (OB_SUCC(ret) && read_cnt < row_cnt) {
      int64_t cnt = 0;
      int64_t bcnt = std::min(batch_size_, row_cnt - read_cnt);
      ret = reader_.get_batch_rows(ver_cells_, *ver_eval_ctx_, read_cnt, read_cnt + bcnt, cnt, stored_rows_);
      if (OB_SUCC(ret)) {
        ASSERT_GT(cnt, 0);
        for (int64_t i = 0; i < cnt; i++) {
          CALL(verify_row_data, read_cnt + i, true, i, true);
        }
        read_cnt += cnt;
      }
    }
    if (row_cnt > rs_.get_row_cnt()) {
      ASSERT_EQ(read_cnt, rs_.get_row_cnt());
    } else {
      ASSERT_EQ(read_cnt, row_cnt);
    }
  }
  void append_rows(int64_t cnt)
  {
    return append_rows(rs_, cnt);
  }

  int init_tenant_mgr();

protected:
  // data members
  const static int64_t COLS = 3;
  int64_t cell_cnt_;
  MyAllocator rs_alloc_;
  ObRATempRowStore rs_;
  ObRATempRowStore::RAReader reader_;
  const ObCompactRow *stored_row_;
  const ObCompactRow **stored_rows_;
  RowMeta &row_meta_;
  ObBitVector *skip_;
  int64_t tenant_id_ = OB_SYS_TENANT_ID;
  const static int64_t BUF_SIZE = 2 << 7;
  char str_buf_[BUF_SIZE];
  int64_t batch_size_ = 256;
  ObSEArray<ObExpr *, COLS> cells_;
  ObSEArray<ObExpr *, COLS> ver_cells_;
  ObEvalCtx *eval_ctx_;
  ObEvalCtx *ver_eval_ctx_;
};

TestRATempRowStore::TestRATempRowStore() :
rs_(),
reader_(),
row_meta_(rs_.row_meta_),
eval_ctx_(NULL),
ver_eval_ctx_(NULL)
{
  std::string schema_filename = ObTestOpConfig::get_instance().test_filename_prefix_ + ".schema";
  strcpy(schema_file_path_, schema_filename.c_str());
}

TestRATempRowStore::~TestRATempRowStore()
{}

int TestRATempRowStore::init_tenant_mgr()
{
  int ret = OB_SUCCESS;
  ObAddr self;
  oceanbase::rpc::frame::ObReqTransport req_transport(NULL, NULL);
  oceanbase::obrpc::ObSrvRpcProxy rpc_proxy;
  oceanbase::obrpc::ObCommonRpcProxy rs_rpc_proxy;
  oceanbase::share::ObRsMgr rs_mgr;
  int64_t tenant_id = OB_SYS_TENANT_ID;
  self.set_ip_addr("127.0.0.1", 8086);
  ret = getter.add_tenant(tenant_id,
                          2L * 1024L * 1024L * 1024L, 4L * 1024L * 1024L * 1024L);
  EXPECT_EQ(OB_SUCCESS, ret);
  const int64_t ulmt = 128LL << 30;
  const int64_t llmt = 128LL << 30;
  ret = getter.add_tenant(OB_SERVER_TENANT_ID,
                          ulmt,
                          llmt);
  EXPECT_EQ(OB_SUCCESS, ret);
  oceanbase::lib::set_memory_limit(128LL << 32);
  return ret;
}


void TestRATempRowStore::SetUp()
{
  TestOpEngine::SetUp();
  int ret = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, init_tenant_mgr());
  ASSERT_EQ(OB_SUCCESS, ret);

  cell_cnt_ = COLS;

  skip_ = (ObBitVector *)vec_2_alloc_.alloc(ObBitVector::memory_size(batch_size_));

  init_exprs();
  //mem limit 1M
  rs_.set_allocator(rs_alloc_);
  ObMemAttr attr(tenant_id_, "TestTmpRStore", ObCtxIds::WORK_AREA);
  const int64_t extra_size = sizeof(uint64_t); // for hash value
  ret = rs_.init(cells_, 1L << 10, attr, 1L << 20, true, extra_size);
  row_meta_ = rs_.row_meta_;
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = reader_.init(&rs_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, rs_.alloc_dir_id());

  memset(str_buf_, 'a', BUF_SIZE);
  for (int64_t i = 0; i < BUF_SIZE; i++) {
    str_buf_[i] += i % 26;
  }
  LOG_INFO("setup finished");
}

void TestRATempRowStore::TearDown()
{
  destroy();
  reader_.reset();
  rs_.reset();
  rs_.~ObRATempRowStore();

  blocksstable::ObTmpFileManager::get_instance().destroy();
  LOG_INFO("TearDown finished", K_(rs));
}

TEST_F(TestRATempRowStore, basic)
{
  int ret = OB_SUCCESS;
  LOG_WARN("starting basic test: append 200 rows");
  CALL(append_rows, 300); // no need to dump
  CALL(verify_n_rows, rs_.get_row_cnt(), true);
  LOG_WARN("basic test: varified rows", K(rs_.get_row_cnt()));
  reader_.reset();
  rs_.reset();

  LOG_WARN("starting basic test: append 4000 rows");
  CALL(append_rows, 40000); //need to dump
  ASSERT_EQ(40000, rs_.get_row_cnt());
  LOG_WARN("starting basic test: verify rows");
  CALL(verify_n_rows, rs_.get_row_cnt() - 1, true);

  ret = reader_.get_row(rs_.get_row_cnt() - 1, stored_row_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = reader_.get_row(rs_.get_row_cnt(), stored_row_);
  ASSERT_EQ(OB_INDEX_OUT_OF_RANGE, ret);
  reader_.reset();

  ret = reader_.get_row(0, stored_row_);
  ASSERT_EQ(OB_SUCCESS, ret);
  LOG_WARN("first row");
  reader_.reset();
  rs_.reset();

  LOG_INFO("============== start to test batch ===============");
  CALL(batch_append_rows, 300); // no need to dump
  CALL(verify_n_rows, rs_.get_row_cnt(), true);
  reader_.reset();
  CALL(batch_verify_all, rs_.get_row_cnt());
  reader_.reset();
  rs_.reset();
  ASSERT_EQ(0, rs_alloc_.total_);

  LOG_INFO("============= start to test batch dump ===========");
  CALL(batch_append_rows, 30000); // need dump
  ASSERT_EQ(30000, rs_.get_row_cnt());
  CALL(batch_verify_all, 0);
  CALL(batch_verify_all, 100);
  CALL(batch_verify_all, 30000);
  CALL(batch_verify_all, 50000);
  reader_.reset();
  rs_.reset();
  ASSERT_EQ(0, rs_alloc_.total_);

  reader_.reset();
  rs_.reset();
  ASSERT_EQ(0, rs_alloc_.total_);
}

} // namespace test
} // namespace test

int main(int argc, char **argv)
{
  ObTestOpConfig::get_instance().test_filename_prefix_ = "test_ra_temp_row_store";
  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "-bg") == 0) {
      ObTestOpConfig::get_instance().test_filename_prefix_ += "_bg";
      ObTestOpConfig::get_instance().run_in_background_ = true;
    }
  }
  ObTestOpConfig::get_instance().init();

  system(("rm -f " + ObTestOpConfig::get_instance().test_filename_prefix_ + ".log").data());
  system(("rm -f " + ObTestOpConfig::get_instance().test_filename_prefix_ + ".log.*").data());
  oceanbase::common::ObClockGenerator::init();
  observer::ObReqTimeGuard req_timeinfo_guard;
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name((ObTestOpConfig::get_instance().test_filename_prefix_ + ".log").data(), true);
  init_sql_factories();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}