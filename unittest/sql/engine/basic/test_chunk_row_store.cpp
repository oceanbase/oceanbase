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
#include "lib/alloc/ob_malloc_allocator.h"
#include "lib/allocator/ob_malloc.h"
#include "storage/blocksstable/ob_data_file_prepare.h"
#include "storage/blocksstable/ob_tmp_file.h"
#include "sql/engine/basic/ob_chunk_row_store.h"
#include "sql/engine/basic/ob_ra_row_store.h"
#include "common/row/ob_row_store.h"
#include "share/config/ob_server_config.h"
#include "sql/ob_sql_init.h"
#include "share/ob_simple_mem_limit_getter.h"


namespace oceanbase
{
namespace sql
{
using namespace common;
static ObSimpleMemLimitGetter getter;

class TestEnv : public ::testing::Environment
{
public:
  virtual void SetUp() override
  {
    GCONF.enable_sql_operator_dump.set_value("True");
    int ret = OB_SUCCESS;
    lib::ObMallocAllocator *malloc_allocator = lib::ObMallocAllocator::get_instance();
    //ret = malloc_allocator->create_tenant_ctx_allocator(OB_SYS_TENANT_ID);
    //ASSERT_EQ(OB_SUCCESS, ret);
    ret = malloc_allocator->create_and_add_tenant_allocator(OB_SYS_TENANT_ID);
    ASSERT_EQ(OB_SUCCESS, ret);
    int s = (int)time(NULL);
    LOG_WARN("initial setup random seed", K(s));
    srandom(s);
  }

  virtual void TearDown() override
  {
  }
};

#define CALL(func, ...) func(__VA_ARGS__); ASSERT_FALSE(HasFatalFailure());

class TestChunkRowStore : public blocksstable::TestDataFilePrepare
{
public:
  TestChunkRowStore() : blocksstable::TestDataFilePrepare(&getter,
                                                          "TestDisk_chunk_row_store", 2<<20, 5000)
  {
  }

  virtual void SetUp() override
  {
    int ret = OB_SUCCESS;
    ASSERT_EQ(OB_SUCCESS, init_tenant_mgr());
    blocksstable::TestDataFilePrepare::SetUp();
    ret = blocksstable::ObTmpFileManager::get_instance().init();
    ASSERT_EQ(OB_SUCCESS, ret);
    if (!is_server_tenant(tenant_id_)) {
      static ObTenantBase tenant_ctx(tenant_id_);
      ObTenantEnv::set_tenant(&tenant_ctx);
      ObTenantIOManager *io_service = nullptr;
      EXPECT_EQ(OB_SUCCESS, ObTenantIOManager::mtl_new(io_service));
      EXPECT_EQ(OB_SUCCESS, ObTenantIOManager::mtl_init(io_service));
      EXPECT_EQ(OB_SUCCESS, io_service->start());
      tenant_ctx.set(io_service);
      ObTenantEnv::set_tenant(&tenant_ctx);
    }

    row_.count_ = COLS;
    row_.cells_ = cells_;
    cells_[1].set_null();

    row_verify_.count_ = COLS;
    row_verify_.cells_ = cells_;

    //mem limit 1M
    ret = rs_.init(1L << 20, tenant_id_, ctx_id_, label_);
    ASSERT_EQ(OB_SUCCESS, ret);

    ASSERT_EQ(OB_SUCCESS, rs_.alloc_dir_id());

    memset(str_buf_, 'z', BUF_SIZE);
    LOG_WARN("setup finished", K_(row));
  }

  int init_tenant_mgr();

  virtual void TearDown() override
  {
    it_.reset();
    rs_.reset();
    rs_.~ObChunkRowStore();

    blocksstable::ObTmpFileManager::get_instance().destroy();
    blocksstable::TestDataFilePrepare::TearDown();
    LOG_INFO("TearDown finished", K_(rs));
  }

  ObNewRow &gen_row(int64_t row_id)
  {
    cells_[0].set_int(row_id);
    int64_t max_size = 512;
    if (enable_big_row_ && row_id > 0 && random() % 100000 < 5) {
      max_size = 1 << 20;
    }
    int64_t size = 10 + random() % max_size;
    cells_[2].set_varchar(str_buf_, (int)size);
    return row_;
  }

  //varify next row
  //template <typename T>
  void verify_row(ObChunkRowStore::Iterator& it, int64_t n, bool verify_all = false)
  {
    int ret = it.get_next_row(row_verify_);
    ASSERT_EQ(OB_SUCCESS, ret);

    int64_t v;
    row_verify_.get_cell(0).get_int(v);
    if (verify_all) {
      row_verify_.get_cell(1).is_null();
      ObString s = row_verify_.get_cell(2).get_varchar();
      if (0 != strncmp(str_buf_, s.ptr(), s.length())) {
        LOG_WARN("verify failed", K(s.ptr()), K(s.length()), K(v), K(n));
      }
      ASSERT_EQ(0, strncmp(str_buf_, s.ptr(), s.length()));
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

  void verify_n_rows(ObChunkRowStore &rs, ObChunkRowStore::Iterator& it,
      int64_t n, bool verify_all = false, int64_t chunk_size = 0, int64_t start = 0)
  {
    if (!it.is_valid()) {
      ASSERT_EQ(rs.begin(it), 0);
      it.set_chunk_read_size(chunk_size);
    }
    for (int64_t i = start; i < n; i++) {
      CALL(verify_row, it, i, verify_all);
//      if (i % 1000 == 0) {
//        LOG_WARN("verified rows:", K(i));
//      }
    }
  }

  void verify_n_rows(int64_t n, bool verify_all = false)
  {
    return verify_n_rows(rs_, it_, n, verify_all);
  }

  void append_rows(ObChunkRowStore &rs, int64_t cnt)
  {
    int64_t ret = OB_SUCCESS;
    int64_t base = rs.get_row_cnt();
    for (int64_t i = 0; i < cnt; i++) {
      ObNewRow &row = gen_row(base + i);
      ret = rs.add_row(row);
      ASSERT_EQ(OB_SUCCESS, ret);
      //if (i % 1000 == 0) {
      //  LOG_WARN("appended rows:", K(i));
      //}
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
    ObChunkRowStore rs(&alloc);
    ASSERT_EQ(OB_SUCCESS, rs.alloc_dir_id());
    int64_t v = 0;
    int64_t i;
    int64_t begin = ObTimeUtil::current_time();
    int ret = OB_SUCCESS;
    ret = rs.init(0, tenant_id_, ctx_id_, label_);
    ASSERT_EQ(OB_SUCCESS, ret);
    rs.set_block_size(block_size);
    begin = ObTimeUtil::current_time();
    for (i = 0; i < rows; i++) {
      ObNewRow &row = gen_row(i);
      ret = rs.add_row(row);
      ASSERT_EQ(OB_SUCCESS, ret);
      //if (i % 1000 == 0) {
      //  LOG_WARN("appended rows:", K(i));
      //}
    }
    LOG_WARN("rs write time:", K(block_size), K(rows), K(rs.get_block_cnt()), K(rs.get_block_list_cnt()),
        K(ObTimeUtil::current_time() - begin));
    begin = ObTimeUtil::current_time();
    ObChunkRowStore::Iterator it;
    ASSERT_EQ(rs.begin(it), 0);
    i = 0;
    while (OB_SUCC(it.get_next_row(row_verify_))) {
      row_verify_.get_cell(0).get_int(v);
      ASSERT_EQ(i, v);
      i++;
    }
    LOG_WARN("rc scan time:", K(block_size), K(rows), K(ObTimeUtil::current_time() - begin));
  }

  void with_or_without_chunk(bool is_with);
protected:

  const static int64_t COLS = 3;
  bool enable_big_row_ = false;
  ObObj cells_[COLS];
  ObObj ver_cells_[COLS];
  ObNewRow row_;
  ObNewRow row_verify_;
  ObChunkRowStore rs_;
  ObChunkRowStore::Iterator it_;

  int64_t tenant_id_ = OB_SYS_TENANT_ID;
  int64_t ctx_id_ = ObCtxIds::WORK_AREA;
  const char *label_ = ObModIds::OB_SQL_ROW_STORE;

  const static int64_t BUF_SIZE = 2 << 20;
  char str_buf_[BUF_SIZE];
};

int TestChunkRowStore::init_tenant_mgr()
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

TEST_F(TestChunkRowStore, basic)
{
  int ret = OB_SUCCESS;

  LOG_INFO("starting basic test: append 3000 rows");
  CALL(append_rows, 3000); // approximate 1MB, no need to dump
  CALL(verify_n_rows, rs_.get_row_cnt(), true);
  LOG_WARN("basic test: varified rows", K(rs_.get_row_cnt()));
  it_.reset();

  LOG_INFO("starting basic test: append 10000 rows");
  CALL(append_rows, 10000); //need to dump
  rs_.finish_add_row();
  LOG_WARN("mem", K(rs_.get_mem_hold()), K(rs_.get_mem_used()));
  ASSERT_EQ(13000, rs_.get_row_cnt());
  LOG_INFO("starting basic test: verify rows");
  CALL(verify_n_rows, rs_.get_row_cnt() - 1, true);

  ret = it_.get_next_row(row_verify_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = it_.get_next_row(row_verify_);
  ASSERT_EQ(OB_ITER_END, ret);
  it_.reset();

  ret = it_.get_next_row(row_verify_);
  ASSERT_EQ(OB_SUCCESS, ret);
  LOG_WARN("first row", K_(row_verify));
  it_.reset();

  rs_.reset();

  LOG_INFO("starting basic test: big row 20000 rows");
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

TEST_F(TestChunkRowStore, multi_iter)
{
  int ret = OB_SUCCESS;
  int total = 100;
  int64_t i = 0;
  int64_t j = 0;
  ObChunkRowStore rs;
  ASSERT_EQ(OB_SUCCESS, rs.alloc_dir_id());

  ret = rs.init(1 << 20, tenant_id_, ctx_id_, label_);
  ASSERT_EQ(OB_SUCCESS, ret);

  LOG_INFO("starting basic test: append 3000 rows");
  CALL(append_rows, rs, total); // approximate 1MB, no need to dump
  rs.finish_add_row();
  LOG_WARN("Multi_iter", K_(rs.mem_hold), K_(rs.mem_used));

  ObChunkRowStore::Iterator it1;
  ASSERT_EQ(OB_SUCCESS, rs.begin(it1));
  for (i = 0; i < 10; i++) {
    CALL(verify_row, it1, i, true);
    if (i % 1000 == 0) {
      LOG_WARN("verified rows:", K(i));
    }
  }

  ObChunkRowStore::Iterator it2;
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
  LOG_WARN("Multi_iter", K_(rs.mem_hold), K_(rs.mem_used));
  rs.reset();
}


TEST_F(TestChunkRowStore, keep_projector0)
{
  ObChunkRowStore rs(NULL);
  ASSERT_EQ(OB_SUCCESS, rs.alloc_dir_id());
  ASSERT_EQ(OB_SUCCESS, rs.init(100 << 20, OB_SERVER_TENANT_ID, ObCtxIds::DEFAULT_CTX_ID,
                                common::ObModIds::OB_SQL_CHUNK_ROW_STORE, true));
  const int64_t OBJ_CNT = 3;
  ObObj objs[OBJ_CNT];
  ObNewRow r;
  r.cells_ = objs;
  r.count_ = OBJ_CNT;
  int64_t val = 0;
  for (int64_t i = 0; i < OBJ_CNT; i++) {
    objs[i].set_int(val);
    val++;
  }

  int32_t projector[] = {0, 2};
  r.projector_ = projector;
  r.projector_size_ = ARRAYSIZEOF(projector);

  ASSERT_EQ(OB_SUCCESS, rs.add_row(r));

  for (int64_t i = 0; i < OBJ_CNT; i++) {
    objs[i].set_int(val);
    val++;
  }
  ASSERT_EQ(OB_SUCCESS, rs.add_row(r));

  ObChunkRowStore::Iterator it1;
  ASSERT_EQ(OB_SUCCESS, rs.begin(it1));

  r.projector_size_ =0;
  r.projector_ = NULL;

  ASSERT_EQ(OB_SUCCESS, it1.get_next_row(r));
  // only fill cells, projector_ unchanged.
  ASSERT_EQ(NULL, r.projector_);
  ASSERT_EQ(0, r.get_cell(0).get_int());
  ASSERT_EQ(0, r.cells_[0].get_int());
  ASSERT_EQ(2, r.get_cell(1).get_int());
  ASSERT_EQ(2, r.cells_[1].get_int());

  ObNewRow *rr;
  ASSERT_EQ(OB_SUCCESS, it1.get_next_row(rr));

  ASSERT_TRUE(NULL == rr->projector_);

  ASSERT_EQ(OBJ_CNT, rr->get_cell(0).get_int());
  ASSERT_EQ(OBJ_CNT, rr->cells_[0].get_int());
  ASSERT_EQ(OBJ_CNT + 2, rr->get_cell(1).get_int());
  ASSERT_EQ(OBJ_CNT + 2, rr->cells_[1].get_int());

  it1.reset();
  rs.reset();
}


TEST_F(TestChunkRowStore, keep_projector2)
{
  ObChunkRowStore rs(NULL);
  ASSERT_EQ(OB_SUCCESS, rs.alloc_dir_id());
  ASSERT_EQ(OB_SUCCESS, rs.init(100 << 20, OB_SERVER_TENANT_ID, ObCtxIds::DEFAULT_CTX_ID,
                                common::ObModIds::OB_SQL_CHUNK_ROW_STORE, true,
                                ObChunkRowStore::STORE_MODE::FULL));
  const int64_t OBJ_CNT = 3;
  ObObj objs[OBJ_CNT];
  ObNewRow r;
  r.cells_ = objs;
  r.count_ = OBJ_CNT;
  int64_t val = 0;
  for (int64_t i = 0; i < OBJ_CNT; i++) {
    objs[i].set_int(val);
    val++;
  }

  int32_t projector[] = {0, 2};
  r.projector_ = projector;
  r.projector_size_ = ARRAYSIZEOF(projector);

  ASSERT_EQ(OB_SUCCESS, rs.add_row(r));

  for (int64_t i = 0; i < OBJ_CNT; i++) {
    objs[i].set_int(val);
    val++;
  }
  r.projector_size_--;
  ASSERT_NE(OB_SUCCESS, rs.add_row(r));
  r.projector_size_++;
  ASSERT_EQ(OB_SUCCESS, rs.add_row(r));

  ObChunkRowStore::Iterator it1;
  ASSERT_EQ(OB_SUCCESS, rs.begin(it1));
  ASSERT_EQ(OB_SUCCESS, it1.get_next_row(r));
  // only fill cells, projector_ unchanged.
  ASSERT_EQ(projector, r.projector_);
  ASSERT_EQ(0, r.get_cell(0).get_int());
  ASSERT_EQ(1, r.cells_[1].get_int());
  ASSERT_EQ(2, r.get_cell(1).get_int());

  ObNewRow *rr;
  ASSERT_EQ(OB_SUCCESS, it1.get_next_row(rr));

  ASSERT_TRUE(NULL != rr->projector_);
  ASSERT_NE(projector, rr->projector_);
  ASSERT_EQ(ARRAYSIZEOF(projector), rr->get_count());

  ASSERT_EQ(OBJ_CNT, rr->get_cell(0).get_int());
  ASSERT_EQ(OBJ_CNT + 1, rr->cells_[1].get_int());
  ASSERT_EQ(OBJ_CNT + 2, rr->get_cell(1).get_int());

  it1.reset();
  rs.reset();
}

TEST_F(TestChunkRowStore, keep_projector2_with_copy)
{
  ObChunkRowStore rs(NULL);
  ASSERT_EQ(OB_SUCCESS, rs.alloc_dir_id());
  ASSERT_EQ(OB_SUCCESS, rs.init(100 << 20, OB_SERVER_TENANT_ID, ObCtxIds::DEFAULT_CTX_ID,
                                common::ObModIds::OB_SQL_CHUNK_ROW_STORE, true,
                                ObChunkRowStore::STORE_MODE::FULL));
  ObChunkRowStore rs2(NULL);
  ASSERT_EQ(OB_SUCCESS, rs2.alloc_dir_id());
  ASSERT_EQ(OB_SUCCESS, rs2.init(100 << 20, OB_SERVER_TENANT_ID, ObCtxIds::DEFAULT_CTX_ID,
                                common::ObModIds::OB_SQL_CHUNK_ROW_STORE, true,
                                ObChunkRowStore::STORE_MODE::FULL));
  const int64_t OBJ_CNT = 3;
  ObObj objs[OBJ_CNT];
  ObNewRow r;
  r.cells_ = objs;
  r.count_ = OBJ_CNT;
  int64_t val = 0;
  for (int64_t i = 0; i < OBJ_CNT; i++) {
    objs[i].set_int(val);
    val++;
  }
  int32_t projector[] = {0, 2};
  r.projector_ = projector;
  r.projector_size_ = ARRAYSIZEOF(projector);
  ASSERT_EQ(OB_SUCCESS, rs.add_row(r));
  for (int64_t i = 0; i < OBJ_CNT; i++) {
    objs[i].set_int(val);
    val++;
  }
  ASSERT_EQ(OB_SUCCESS, rs.add_row(r));
  ObChunkRowStore::Iterator it1;
  ASSERT_EQ(OB_SUCCESS, rs.begin(it1));
  const ObChunkRowStore::StoredRow* sr;
  ASSERT_EQ(OB_SUCCESS, it1.get_next_row(sr));
  ASSERT_EQ(OB_SUCCESS, rs2.copy_row(sr, &rs));
  ASSERT_EQ(OB_SUCCESS, it1.get_next_row(sr));
  ASSERT_EQ(OB_SUCCESS, rs2.copy_row(sr, &rs));
  ObChunkRowStore::Iterator it2;
  ASSERT_EQ(OB_SUCCESS, rs2.begin(it2));
  ObNewRow *rr;
  ASSERT_EQ(OB_SUCCESS, it2.get_next_row(rr));
  // only fill cells, projector_ unchanged.
  ASSERT_EQ(0, rr->get_cell(0).get_int());
  ASSERT_EQ(1, rr->cells_[1].get_int());
  ASSERT_EQ(2, rr->get_cell(1).get_int());
  ASSERT_EQ(OB_SUCCESS, it2.get_next_row(rr));
  ASSERT_TRUE(NULL != rr->projector_);
  ASSERT_NE(projector, rr->projector_);
  ASSERT_EQ(ARRAYSIZEOF(projector), rr->get_count());
  ASSERT_EQ(OBJ_CNT, rr->get_cell(0).get_int());
  ASSERT_EQ(OBJ_CNT + 1, rr->cells_[1].get_int());
  ASSERT_EQ(OBJ_CNT + 2, rr->get_cell(1).get_int());
  it1.reset();
  rs.reset();
  it2.reset();
  rs2.reset();
}

TEST_F(TestChunkRowStore, basic2)
{
  int ret = OB_SUCCESS;
  ObChunkRowStore rs;
  ASSERT_EQ(OB_SUCCESS, rs.alloc_dir_id());
  ObChunkRowStore::Iterator it;
  //mem limit 5M
  ret = rs.init(5L << 20, tenant_id_, ctx_id_, label_);
  ASSERT_EQ(OB_SUCCESS, ret);
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


TEST_F(TestChunkRowStore, chunk_iterator)
{
  int ret = OB_SUCCESS;
  ObChunkRowStore rs;
  ASSERT_EQ(OB_SUCCESS, rs.alloc_dir_id());
  ObChunkRowStore::ChunkIterator chunk_it;
  ObChunkRowStore::RowIterator it;
  //mem limit 5M
  ret = rs.init(5L << 20, tenant_id_, ctx_id_, label_);
  ASSERT_EQ(OB_SUCCESS, ret);
  CALL(append_rows, rs, 100000);
  ASSERT_GT(rs.get_mem_hold(), 0);
  ASSERT_GT(rs.get_file_size(), 0);
  LOG_WARN("mem and disk", K(rs.get_mem_hold()), K(rs.get_file_size()));
  ASSERT_EQ(rs.get_row_cnt(), 100000);
  ASSERT_EQ(rs.get_row_cnt_in_memory() + rs.get_row_cnt_on_disk(), 100000);
  rs.finish_add_row();
  ASSERT_EQ(OB_SUCCESS, rs.begin(chunk_it, ObChunkRowStore::BLOCK_SIZE * 4));
  const ObChunkRowStore::StoredRow *v_sr = NULL;
  int64_t v;
  int64_t extend_v;
  int64_t row_cnt = 0;
  int i = 0;
  ret = chunk_it.load_next_chunk(it);
  ASSERT_EQ(OB_SUCCESS, ret);
  row_cnt += chunk_it.get_cur_chunk_row_cnt();
  LOG_WARN("got chunk of rows:", K(row_cnt));
  while(OB_SUCC(ret)) {
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
    ret = it.convert_to_row(row_verify_, v_sr);
    ASSERT_EQ(OB_SUCCESS, ret);
    row_verify_.get_cell(0).get_int(v);
    row_verify_.get_cell(1).is_null();
    ObString s = row_verify_.get_cell(2).get_varchar();
    if (0 != strncmp(str_buf_, s.ptr(), s.length())) {
      LOG_WARN("verify failed", K(s.ptr()), K(s.length()));
    }
    ASSERT_EQ(0, strncmp(str_buf_, s.ptr(), s.length()));
    ASSERT_EQ(i, v);
    i++;
  }
  ASSERT_EQ(row_cnt, 100000);
  it.reset();
  chunk_it.reset();
  rs.reset();
}

TEST_F(TestChunkRowStore, test_copy_row)
{
  int ret = OB_SUCCESS;
  int64_t rows = 1000;
  ObChunkRowStore rs;
  ASSERT_EQ(OB_SUCCESS, rs.alloc_dir_id());
  ObChunkRowStore::Iterator it;
  const ObChunkRowStore::StoredRow *sr;
  LOG_INFO("starting mem_perf test: append rows", K(rows));
  int64_t begin = ObTimeUtil::current_time();
  ret = rs.init(0, tenant_id_, ctx_id_, label_);
  ASSERT_EQ(OB_SUCCESS, ret);
  CALL(append_rows, rs, rows);
  ASSERT_EQ(OB_SUCCESS, rs.begin(it));
  ASSERT_EQ(OB_SUCCESS, it.get_next_row(sr));
}

TEST_F(TestChunkRowStore, mem_perf)
{
  int ret = OB_SUCCESS;
  int64_t rows = 2000000;
  ObChunkRowStore rs;
  ASSERT_EQ(OB_SUCCESS, rs.alloc_dir_id());
  ObChunkRowStore::Iterator it;
  LOG_INFO("starting mem_perf test: append rows", K(rows));
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

TEST_F(TestChunkRowStore, time_cmp)
{
  int ret = OB_SUCCESS;
  int64_t rows = 100;
  int64_t v = 0;
  int64_t i = 0;

  test_time(0, rows);
  test_time(0, rows);
  test_time(8L << 10, rows);
  test_time(256L << 10, rows);
  test_time(512L << 10, rows);
  test_time(1L << 20, rows);
  test_time(2L << 20, rows);
  test_time(4L << 20, rows);

  ObArenaAllocator allocra(ObModIds::OB_MODULE_PAGE_ALLOCATOR, 2 << 20);
  ObRARowStore ra(&allocra);
  ra.init(0, tenant_id_, ctx_id_, label_);
  int64_t begin = ObTimeUtil::current_time();
  for (int64_t i = 0; i < rows; i++) {
    ObNewRow &row = gen_row(i);
    ret = ra.add_row(row);
    ASSERT_EQ(OB_SUCCESS, ret);
    if (i % 1000 == 0) {
      LOG_WARN("appended rows:", K(i));
    }
  }
  LOG_WARN("ra write time:", K(rows), K(ra.get_mem_hold()), K(ra.get_file_size()),
      K(ObTimeUtil::current_time() - begin));
  begin = ObTimeUtil::current_time();
  i = 0;
  while (OB_SUCC(ra.get_row(i, row_verify_))) {
   row_verify_.get_cell(0).get_int(v);
   ASSERT_EQ(i, v);
   i++;
  }
  LOG_WARN("old rc scan time:", K(rows), K(ObTimeUtil::current_time() - begin));
  ra.reset();

  ObArenaAllocator allocrs(ObModIds::OB_MODULE_PAGE_ALLOCATOR, 2 << 20);
  ObRowStore rs_old(allocrs);
  rs_old.set_ctx_id(ctx_id_);
  rs_old.set_label(label_);
  rs_old.set_tenant_id(tenant_id_);

  begin = ObTimeUtil::current_time();
  for (int64_t i = 0; i < rows; i++) {
    ObNewRow &row = gen_row(i);
    ret = rs_old.add_row(row);
    ASSERT_EQ(OB_SUCCESS, ret);
    if (i % 1000 == 0) {
      LOG_WARN("appended rows:", K(i));
    }
  }
  LOG_WARN("old rs write time:", K(rows), K(rs_old.get_block_count()), K(rs_old.get_block_size()),
      K(rs_old.get_used_mem_size()), K(ObTimeUtil::current_time() - begin));

  begin = ObTimeUtil::current_time();
  ObRowStore::Iterator it_old = rs_old.begin();
  i = 0;
  while (OB_SUCC(it_old.get_next_row(row_verify_))) {
    row_verify_.get_cell(0).get_int(v);
    ASSERT_EQ(i, v);
    i++;
  }
  LOG_WARN("old rc scan time:", K(rows), K(ObTimeUtil::current_time() - begin));
  it_old.reset();
  rs_old.reset();
}

/*
TEST_F(TestOARowStore, disk_time_cmp)
{
  int ret = OB_SUCCESS;
  int64_t rows = 2000000;
  ObChunkRowStore rs;
  ASSERT_EQ(OB_SUCCESS, rs.alloc_dir_id());
  int64_t v = 0;
  int64_t i;
  int64_t begin = ObTimeUtil::current_time();
  ret = rs.init(1 << 20, tenant_id_, ctx_id_, label_);
  ASSERT_EQ(OB_SUCCESS, ret);
  begin = ObTimeUtil::current_time();
  for (int64_t i = 0; i < rows; i++) {
    ObNewRow &row = gen_row(i);
    ret = rs.add_row(row);
    ASSERT_EQ(OB_SUCCESS, ret);
    if (i % 1000 == 0) {
      LOG_WARN("appended rows:", K(i));
    }
  }
  LOG_WARN("rs write time:", K(rows), K(ObTimeUtil::current_time() - begin));
  begin = ObTimeUtil::current_time();
  ObChunkRowStore::Iterator it = rs.begin();
  i = 0;
  while (OB_SUCC(it.get_next_row(row_verify_))) {
    row_verify_.get_cell(0).get_int(v);
    ASSERT_EQ(i, v);
    i++;
  }
  LOG_WARN("rc scan time:", K(rows), K(ObTimeUtil::current_time() - begin));

  ObRowStore rs_old;
  rs_old.set_ctx_id(ctx_id_);
  rs_old.set_label(label_);
  rs_old.set_tenant_id(tenant_id_);

  begin = ObTimeUtil::current_time();
  for (int64_t i = 0; i < rows; i++) {
    ObNewRow &row = gen_row(i);
    ret = rs_old.add_row(row);
    ASSERT_EQ(OB_SUCCESS, ret);
    if (i % 1000 == 0) {
      LOG_WARN("appended rows:", K(i));
    }
  }
  LOG_WARN("old rs write time:", K(rows), K(ObTimeUtil::current_time() - begin));

  begin = ObTimeUtil::current_time();
  ObRowStore::Iterator it_old = rs_old.begin();
  i = 0;
  while (OB_SUCC(it_old.get_next_row(row_verify_))) {
    row_verify_.get_cell(0).get_int(v);
    ASSERT_EQ(i, v);
    i++;
  }
  LOG_WARN("old rc scan time:", K(rows), K(ObTimeUtil::current_time() - begin));
  it_old.reset();
  rs_old.reset();
}
*/

TEST_F(TestChunkRowStore, disk)
{
  int64_t begin = 0;
  int64_t write_time = 0;
  int64_t round = 500;
  int64_t rows = round * 10000;
  LOG_INFO("starting write disk test: append rows", K(rows));
  ObChunkRowStore rs;
  ASSERT_EQ(OB_SUCCESS, rs.alloc_dir_id());
  ObChunkRowStore::Iterator it;
  ASSERT_EQ(OB_SUCCESS, rs.init(0, tenant_id_, ctx_id_, label_));
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
  LOG_INFO("mem and disk after finish", K(rows), K(rs.get_mem_hold()), K(rs.get_mem_used()), K(rs.get_file_size()));

  it.reset();
  begin = ObTimeUtil::current_time();
  CALL(verify_n_rows, rs, it, rs.get_row_cnt(), true);
  LOG_INFO("disk scan time:", K(rows), K(ObTimeUtil::current_time() - begin));
  it.reset();
  rs.reset();
}


TEST_F(TestChunkRowStore, disk_with_chunk)
{
  int64_t begin = 0;
  int64_t write_time = 0;
  int64_t round = 2;
  int64_t cnt = 10000;
  int64_t rows = round * cnt;
  LOG_INFO("starting write disk test: append rows", K(rows));
  ObChunkRowStore rs;
  ASSERT_EQ(OB_SUCCESS, rs.alloc_dir_id());
  ObChunkRowStore::Iterator it;
  ASSERT_EQ(OB_SUCCESS, rs.init(0, tenant_id_, ctx_id_, label_));
  rs.set_mem_limit(1L << 20);
  for (int64_t i = 0; i < round; i++) {
    //if (i == round / 2) {
      //enable_big_row_ = true;
   // }
    begin = common::ObTimeUtil::current_time();
    CALL(append_rows, rs, cnt);
    write_time += common::ObTimeUtil::current_time() - begin;
  }

  LOG_INFO("mem and disk", K(rows), K(rs.get_mem_hold()), K(rs.get_mem_used()), K(rs.get_file_size()));
  LOG_INFO("disk write:", K(rows), K(write_time));

  ASSERT_EQ(OB_SUCCESS, rs.finish_add_row());
  LOG_INFO("mem and disk after finish", K(rows), K(rs.get_mem_hold()), K(rs.get_mem_used()), K(rs.get_file_size()), K(rs.max_blk_size_), K(rs.min_blk_size_), K(rs.n_block_in_file_));

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


// test chunk row store根据内存比例进行dump，去掉了这层逻辑，是否dump由上层驱动
//case from oarowstore
// TEST_F(TestChunkRowStore, start_dump_by_total_mem_used)
// {
//   LOG_INFO("starting dump mem test: append rows", K(500000));
//   int ret = OB_SUCCESS;
//   ObChunkRowStore rs;
//   ASSERT_EQ(OB_SUCCESS, rs.alloc_dir_id());
//   ObChunkRowStore::Iterator it;
//   ret = rs.init(0, tenant_id_, ctx_id_, mod_id_);
//   CALL(append_rows, rs, 500000);
//   int64_t avg_row_size = rs.get_mem_hold() / rs.get_row_cnt();
//   LOG_WARN("average row size", K(avg_row_size));

//   lib::ObMallocAllocator *malloc_allocator = lib::ObMallocAllocator::get_instance();
//   malloc_allocator->set_tenant_limit(OB_SYS_TENANT_ID, 1L << 30);
//   ASSERT_EQ(OB_SUCCESS, ret);
//   // 50MB for work area
//   ret = lib::set_wa_limit(OB_SYS_TENANT_ID, 5);
//   ASSERT_EQ(OB_SUCCESS, ret);

//   rs.reset();

//   ret = rs.init(0, tenant_id_, ctx_id_, mod_id_);
//   ASSERT_EQ(OB_SUCCESS, ret);


//   // case1: trigger dump by memory mod usage (60% of limit, 30MB)
//   // write 28MB, all in memory
//   CALL(append_rows, rs, (28L << 20) / avg_row_size);
//   ASSERT_EQ(rs.get_file_size(), 0);

//   // append 10MB, need dump
//   CALL(append_rows, rs, (10L << 20) / avg_row_size);
//   ASSERT_GT(rs.get_file_size(), 0);

//   rs.reset();
//   ret = rs.init(0, tenant_id_, ctx_id_, mod_id_);
//   ASSERT_EQ(OB_SUCCESS, ret);

//   // case2: trigger dump by memory ctx usage (80% of limit, 40MB)
//   ObMemAttr attr = default_memattr;
//   attr.tenant_id_ = tenant_id_;
//   attr.ctx_id_ = ctx_id_;
//   // memory ctx hold 20MB
//   void *mem = ob_malloc(20L << 20, attr);

//   // write 15MB, in memory
//   CALL(append_rows, rs, (15L << 20) / avg_row_size);
//   ASSERT_EQ(rs.get_file_size(), 0);

//   // append 10MB, need dump
//   CALL(append_rows, rs, (10L << 20) / avg_row_size);
//   ASSERT_GT(rs.get_file_size(), 0);

//   ob_free(mem);

//   // case3: write to disk disabled
//   // write 40MB, all in memmory
//   GCONF.enable_sql_operator_dump.set_value("False");
//   rs.reset();

//   ret = rs.init(0, tenant_id_, ctx_id_, mod_id_);
//   ASSERT_EQ(OB_SUCCESS, ret);

//   CALL(append_rows, rs, (40L << 20) / avg_row_size);
//   ASSERT_EQ(rs.get_file_size(), 0);

//   // append 20MB, exceed work area
//   int64_t rows = (100L << 20) / avg_row_size;
//   int64_t idx = 0;
//   int64_t base = rs.get_row_cnt();
//   for (; idx < rows; idx++) {
//     if (OB_SUCCESS != rs.add_row(gen_row(base + idx))) {
//       break;
//     }
//   }
//   ASSERT_LT(idx, rows);

//   lib::ObMallocAllocator::get_instance()->print_tenant_ctx_memory_usage(tenant_id_);
//   lib::ObMallocAllocator::get_instance()->print_tenant_memory_usage(tenant_id_);

//   ASSERT_EQ(rs.get_file_size(), 0);
//   rs.reset();

// }

TEST_F(TestChunkRowStore, test_add_block)
{
  int ret = OB_SUCCESS;
  //send
  ObChunkRowStore rs;
  ASSERT_EQ(OB_SUCCESS, rs.alloc_dir_id());
  ObChunkRowStore::Block *block;
  ObArenaAllocator alloc(ObModIds::OB_MODULE_PAGE_ALLOCATOR, 2 << 20);

  ObNewRow &row = gen_row(1);
  int64_t row_size = ObChunkRowStore::Block::row_store_size(row);

  ret = rs.init(0, tenant_id_, ctx_id_, label_);
  ASSERT_EQ(OB_SUCCESS, ret);

  int64_t min_size = rs.min_blk_size(row_size);
  void *mem = alloc.alloc(min_size);

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
  LOG_WARN("Molly size", K(block->get_buffer()->data_size()), K(block->get_buffer()->capacity()));

  ObArenaAllocator alloc2(ObModIds::OB_MODULE_PAGE_ALLOCATOR, 2 << 20);
  void *mem2 =  alloc2.alloc(block->get_buffer()->data_size());
  memcpy(mem2, mem, block->get_buffer()->data_size());

  //recv
  ObChunkRowStore rs2;
  ASSERT_EQ(OB_SUCCESS, rs2.alloc_dir_id());
  ObChunkRowStore::Block *block2 = reinterpret_cast<ObChunkRowStore::Block *>(mem2);
  ret = rs2.init(0, tenant_id_, ctx_id_, label_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = rs2.add_block(block2, true);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObChunkRowStore::Iterator it2;
  ASSERT_EQ(OB_SUCCESS, rs2.begin(it2));
  CALL(verify_n_rows, rs2, it2, 99, true);
  ASSERT_EQ(true, it2.has_next());
  CALL(verify_n_rows, rs2, it2, 100, true, 0, 99);
  ASSERT_FALSE(it2.has_next());
  ASSERT_EQ(OB_ITER_END, it2.get_next_row(row));
  rs2.remove_added_blocks();
  it2.reset();
  rs2.reset();
}

TEST_F(TestChunkRowStore, row_extend_row)
{
  int64_t begin = 0;
  int64_t write_time = 0;
  int64_t round = 500;
  int64_t rows = round * 10000;
  LOG_INFO("starting write disk test: append rows", K(rows));
  ObChunkRowStore rs;
  ASSERT_EQ(OB_SUCCESS, rs.alloc_dir_id());
  ObChunkRowStore::Iterator it;
  ASSERT_EQ(OB_SUCCESS,
            rs.init(0, tenant_id_, ctx_id_, label_, true, ObChunkRowStore::WITHOUT_PROJECTOR, 8));
  LOG_INFO("starting basic test: append 3000 rows");
  int64_t ret = OB_SUCCESS;
  int64_t base = rs.get_row_cnt();
  ObChunkRowStore::StoredRow* sr = NULL;
  const ObChunkRowStore::StoredRow* v_sr = NULL;
  for (int64_t i = 0; i < 100; i++) {
    ObNewRow &row = gen_row(i);
    ret = rs.add_row(row, &sr);
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
    ret = it.convert_to_row_with_obj(v_sr, row_verify_);
    ASSERT_EQ(OB_SUCCESS, ret);
    row_verify_.get_cell(0).get_int(v);
    row_verify_.get_cell(1).is_null();
    ObString s = row_verify_.get_cell(2).get_varchar();
    if (0 != strncmp(str_buf_, s.ptr(), s.length())) {
      LOG_WARN("verify failed", K(s.ptr()), K(s.length()));
    }
    ASSERT_EQ(0, strncmp(str_buf_, s.ptr(), s.length()));
    extend_v = *(static_cast<int64_t*>(v_sr->get_extra_payload()));
    if (i != v || i != extend_v) {
      LOG_WARN("verify failed", K(i), K(v), K(extend_v));
    }
    ASSERT_EQ(i, v);

  }
  it.reset();
  rs.reset();
}

TEST_F(TestChunkRowStore, test_both_disk_and_memory)
{
  int64_t round = 2;
  int64_t cnt = 100;
  int64_t rows = round * cnt;
  LOG_INFO("starting write disk test: append rows", K(rows));
  ObChunkRowStore rs;
  ASSERT_EQ(OB_SUCCESS, rs.alloc_dir_id());
  ObChunkRowStore::Iterator it;
  ASSERT_EQ(OB_SUCCESS, rs.init(0, tenant_id_, ctx_id_, label_));
  rs.set_mem_limit(1L << 30);
  // disk data
  CALL(append_rows, rs, cnt);
  ASSERT_EQ(OB_SUCCESS, rs.dump(false, true));
  // in memory
  CALL(append_rows, rs, cnt);
  rs.finish_add_row(false);

  CALL(verify_n_rows, rs, it, rs.get_row_cnt(), true, 16L << 20);
  ASSERT_EQ(rs.get_row_cnt_on_disk(), rs.get_row_cnt_in_memory());
  LOG_INFO("row store data count", K(rs.get_row_cnt_on_disk()), K(rs.get_row_cnt_in_memory()));

  it.reset();
  rs.reset();
}


TEST_F(TestChunkRowStore, test_only_disk_data)
{
  int64_t round = 2;
  int64_t cnt = 100;
  int64_t rows = round * cnt;
  LOG_INFO("starting write disk test: append rows", K(rows));
  ObChunkRowStore rs;
  ASSERT_EQ(OB_SUCCESS, rs.alloc_dir_id());
  ObChunkRowStore::Iterator it;
  ASSERT_EQ(OB_SUCCESS, rs.init(0, tenant_id_, ctx_id_, label_));
  rs.set_mem_limit(1L << 30);
  // disk data
  CALL(append_rows, rs, cnt);
  ASSERT_EQ(OB_SUCCESS, rs.dump(false, true));
  rs.finish_add_row();

  CALL(verify_n_rows, rs, it, rs.get_row_cnt(), true, 16L << 20);
  LOG_INFO("row store data count", K(rs.get_row_cnt_on_disk()), K(rs.get_row_cnt_in_memory()));

  it.reset();
  rs.reset();
}

} // end namespace sql
} // end namespace oceanbase


void ignore_sig(int sig)
{
  UNUSED(sig);
}

int main(int argc, char **argv)
{
  signal(49, ignore_sig);
  oceanbase::sql::init_sql_factories();
  oceanbase::common::ObLogger::get_logger().set_file_name("test_chunk_row_store.log", true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  auto *env = new (oceanbase::sql::TestEnv);
  testing::AddGlobalTestEnvironment(env);
  int ret = RUN_ALL_TESTS();
  OB_LOGGER.disable();
  return ret;
}
