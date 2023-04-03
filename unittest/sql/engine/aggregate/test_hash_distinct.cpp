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

#include "sql/ob_sql_init.h"
#include "sql/engine/table/ob_fake_table.h"
#include "sql/engine/aggregate/ob_hash_distinct.h"
#include "sql/engine/basic/ob_chunk_row_store.h"
#include "storage/blocksstable/ob_data_file_prepare.h"
#include "sql/session/ob_sql_session_info.h"
#include "share/config/ob_server_config.h"
#include "share/ob_define.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "share/ob_simple_mem_limit_getter.h"

namespace oceanbase
{
using namespace omt;
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
    uint64_t cluster_version = CLUSTER_VERSION_3000;
    common::ObClusterVersion::get_instance().update_cluster_version(cluster_version);
    int ret = OB_SUCCESS;
    lib::ObMallocAllocator *malloc_allocator = lib::ObMallocAllocator::get_instance();
    ret = malloc_allocator->create_and_add_tenant_allocator(OB_SYS_TENANT_ID);
    ASSERT_EQ(OB_SUCCESS, ret);
    int s = (int)time(NULL);
    SQL_ENG_LOG(WARN, "initial setup random seed", K(s));
    srandom(s);
  }

  virtual void TearDown() override
  {
  }
};

#define CALL(func, ...) func(__VA_ARGS__); ASSERT_FALSE(HasFatalFailure());


class TestHashDistinct : public ObHashDistinct
{
public:
      TestHashDistinct() :ObHashDistinct(alloc_) {}
            ~TestHashDistinct() {}
};

#define CALL(func, ...) func(__VA_ARGS__); ASSERT_FALSE(HasFatalFailure());

class TestHashDistinctTest:  public blocksstable::TestDataFilePrepare
{
public:
  TestHashDistinctTest() : blocksstable::TestDataFilePrepare(&getter,
                                                             "TestDisk_distinct", 2<<20, 5000)
  {
  }
  virtual ~TestHashDistinctTest();
  virtual void SetUp() override
  {
    int ret = OB_SUCCESS;

    ASSERT_EQ(OB_SUCCESS, init_tenant_mgr());
    blocksstable::TestDataFilePrepare::SetUp();
    ret = blocksstable::ObTmpFileManager::get_instance().init();
    ASSERT_EQ(OB_SUCCESS, ret);

    row_.count_ = COLS;
    row_.cells_ = cells_;
    cells_[1].set_null();

    row_verify_.count_ = COLS;
    row_verify_.cells_ = cells_;
    row_verify_.projector_size_ = 0;

    memset(str_buf_, 'z', BUF_SIZE);
    memset(hit_val_base, '1', BUF_SIZE);
    memset(hit_val, '0', BUF_SIZE);
    SQL_ENG_LOG(WARN, "setup finished", K_(row));
  }

  int init_tenant_mgr();

  virtual void TearDown() override
  {
    blocksstable::ObTmpFileManager::get_instance().destroy();
    blocksstable::TestDataFilePrepare::TearDown();
    common::ObLabelItem item;
    lib::get_tenant_label_memory(tenant_id_, ObNewModIds::TEST1, item);
    ASSERT_EQ(0, item.hold_);
    lib::get_tenant_label_memory(tenant_id_, ObNewModIds::TEST2, item);
    ASSERT_EQ(0, item.hold_);
    lib::get_tenant_label_memory(tenant_id_, ObNewModIds::TEST3, item);
    ASSERT_EQ(0, item.hold_);
    SQL_ENG_LOG(INFO,"TearDown finished");
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
   void verify_row(ObExecContext &ctx, TestHashDistinct &op, bool verify_all = false)
   {
     const ObNewRow *result_row = &row_verify_;
     int ret = op.get_next_row(ctx, result_row);
     ASSERT_EQ(OB_SUCCESS, ret);

     int64_t v;
     result_row->get_cell(0).get_int(v);
     if (verify_all) {
       result_row->get_cell(1).is_null();
       ObString s = result_row->get_cell(2).get_varchar();
       if (0 != strncmp(str_buf_, s.ptr(), s.length())) {
         SQL_ENG_LOG(WARN, "verify failed", K(s.ptr()), K(s.length()));
       }
       ASSERT_EQ(0, strncmp(str_buf_, s.ptr(), s.length()));
     }

     ASSERT_EQ(hit_val[v], '0');
     hit_val[v] = 1;
   }

   void verify_n_rows(ObExecContext &ctx, TestHashDistinct &op,
       int64_t n, bool verify_all = false, int64_t start = 0)
   {
     memset(hit_val, '0', BUF_SIZE);

     for (int64_t i = start; i < n; i++) {
       CALL(verify_row, ctx, op, verify_all);
     }

     ASSERT_EQ(0, memcpy(hit_val, hit_val_base, sizeof(char) * n));
   }

   void verify_all_rows(ObExecContext &ctx, TestHashDistinct &op,
          int64_t n, bool verify_all = false)
    {
      int64_t i = 0;
      int64_t v;
      memset(hit_val, '0', BUF_SIZE);
      while (true) {
        const ObNewRow *result_row = &row_verify_;
        int ret = op.get_next_row(ctx, result_row);
        if (OB_ITER_END == ret)
        {
          break;
        }
        ASSERT_EQ(OB_SUCCESS, ret);
        i++;
        result_row->get_cell(0).get_int(v);
        if (verify_all) {
          result_row->get_cell(1).is_null();
          ObString s = result_row->get_cell(2).get_varchar();
          if (0 != strncmp(str_buf_, s.ptr(), s.length())) {
            SQL_ENG_LOG(WARN, "verify failed", K(s.ptr()), K(s.length()), K(v), K(i));
          }
          ASSERT_EQ(0, strncmp(str_buf_, s.ptr(), s.length()));
        }
        if (hit_val[v] != '0') {
          SQL_ENG_LOG(WARN, "alread hitted", K(i), K(v));
        }
        ASSERT_EQ(hit_val[v], '0');
        hit_val[v] = '1';
      }
      if (n != i) {
        SQL_ENG_LOG_RET(WARN, OB_ERROR, "already hitted", K(i), K(v));
      }
      ASSERT_EQ(n, i);
      ASSERT_EQ(0, MEMCMP(hit_val, hit_val_base, sizeof(char) * n));
    }

   void append_rows(int64_t cnt)
   {
     int64_t ret = OB_SUCCESS;
     //int64_t base = fake_table_.get_rows();
     for (int64_t i = 0; i < cnt; i++) {
       ObNewRow &row = gen_row(i);
       fake_table_.add_row(row);
       ASSERT_EQ(OB_SUCCESS, ret);
       //if (i % 1000 == 0) {
       //  LOG_WARN("appended rows:", K(i));
       //}
     }
     //ASSERT_EQ(base + cnt, fake_table_.get_rows());
   }

  void init(ObExecContext &ctx, ObSQLSessionInfo &my_session, TestHashDistinct &hash_distinct, int64_t col_count)
  {
    ASSERT_EQ(OB_SUCCESS, my_session.test_init(0,0,0,NULL));
    ASSERT_EQ(OB_SUCCESS, ObPreProcessSysVars::init_sys_var());
    ASSERT_EQ(OB_SUCCESS, my_session.load_default_sys_variable(false, true));
    ASSERT_EQ(OB_SUCCESS, my_session.init_tenant("test", OB_SYS_TENANT_ID));
    ctx.set_my_session(&my_session);

    hash_distinct.reset();
    hash_distinct.reuse();
    fake_table_.reset();
    fake_table_.reuse();
    result_table_.reset();
    result_table_.reuse();
    physical_plan_.reset();
    fake_table_.set_column_count(col_count);
    result_table_.set_column_count(col_count);

    result_table_.set_projector(projector, col_count);
    fake_table_.set_projector(projector, col_count);
    hash_distinct.set_projector(projector, col_count);
    hash_distinct.set_column_count(col_count);

    fake_table_.set_id(0);
    result_table_.set_id(1);
    hash_distinct.set_id(2);

    fake_table_.set_phy_plan(&physical_plan_);
    result_table_.set_phy_plan(&physical_plan_);
    hash_distinct.set_phy_plan(&physical_plan_);

    hash_distinct.set_child(0, fake_table_);

    ASSERT_EQ(OB_SUCCESS, ctx.init_phy_op(3));
    ASSERT_EQ(OB_SUCCESS, ctx.create_physical_plan_ctx());
    ASSERT_FALSE(NULL == ctx.get_physical_plan_ctx());
    ctx.get_physical_plan_ctx()->set_phy_plan(&physical_plan_);
  }

  void open_operator(ObExecContext &ctx, TestHashDistinct &hash_distinct)
  {
    ASSERT_EQ(OB_SUCCESS, hash_distinct.open(ctx));
    ObHashDistinct::ObHashDistinctCtx *distinct_ctx = NULL;
    bool is_null = OB_ISNULL(distinct_ctx = GET_PHY_OPERATOR_CTX(ObHashDistinct::ObHashDistinctCtx, ctx, hash_distinct.get_id()));
    ASSERT_EQ(is_null, false);
    distinct_ctx->enable_sql_dumped_ = true;
    ASSERT_EQ(OB_SUCCESS, result_table_.open(ctx));
  }

  void close_operator(ObExecContext &ctx, TestHashDistinct &hash_distinct)
  {
    ASSERT_EQ(OB_SUCCESS, hash_distinct.close(ctx));
    ASSERT_EQ(OB_SUCCESS, result_table_.close(ctx));
  }

  ObFakeTable &get_fake_table() { return fake_table_; }
  ObFakeTable &get_result_table() { return result_table_; }
  ObPhysicalPlan &get_physical_plan() { return physical_plan_; }
protected:
  ObFakeTable fake_table_;
  ObFakeTable result_table_;
  ObPhysicalPlan physical_plan_;
  int32_t projector[3] = {0,1,2};

  const static int64_t COLS = 3;
  bool enable_big_row_ = false;
  ObObj cells_[COLS];
  ObObj ver_cells_[COLS];
  ObNewRow row_;
  ObNewRow row_verify_;
  const static int64_t BUF_SIZE = 2 << 20;
  char str_buf_[BUF_SIZE];
  char hit_val[BUF_SIZE];
  char hit_val_base[BUF_SIZE];

private:
  int64_t tenant_id_ = OB_SYS_TENANT_ID;
  int64_t ctx_id_ = ObCtxIds::WORK_AREA;
  // disallow copy
  TestHashDistinctTest(const TestHashDistinctTest &other);
  TestHashDistinctTest& operator=(const TestHashDistinctTest &other);
private:
  // data members
};

TestHashDistinctTest::~TestHashDistinctTest()
{
}

int TestHashDistinctTest::init_tenant_mgr()
{
  int ret = OB_SUCCESS;
  ObAddr self;
  oceanbase::rpc::frame::ObReqTransport req_transport(NULL, NULL);
  oceanbase::obrpc::ObSrvRpcProxy rpc_proxy;
  oceanbase::obrpc::ObCommonRpcProxy rs_rpc_proxy;
  oceanbase::share::ObRsMgr rs_mgr;
  int64_t tenant_id = OB_SYS_TENANT_ID;
  self.set_ip_addr("127.0.0.1", 8086);
  ret = ObTenantConfigMgr::get_instance().add_tenant_config(tenant_id);
  EXPECT_EQ(OB_SUCCESS, ret);
  GCONF.enable_sql_operator_dump.set_value("True");
  uint64_t cluster_version = CLUSTER_VERSION_3000;
  common::ObClusterVersion::get_instance().update_cluster_version(cluster_version);
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


TEST_F(TestHashDistinctTest, test_big_data00)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  ObFakeTable &fake_table = get_fake_table();
  ObFakeTable &result_table = get_result_table();
  TestHashDistinct hash_distinct;
  ObCollationType cs_type = CS_TYPE_UTF8MB4_GENERAL_CI;
  ObSQLSessionInfo my_session;
  init(ctx, my_session, hash_distinct, 3);

  enable_big_row_ = true;
  hash_distinct.set_mem_limit(64 * 1024 * 1024 * 10);
  //fake table: distinct column(c1)
  CALL(append_rows, 1000000);
  CALL(append_rows, 1000000);

  hash_distinct.init(1);
  hash_distinct.add_distinct_column(0, cs_type);
  //hash_distinct.add_distinct_column(1, cs_type);
  open_operator(ctx, hash_distinct);
  CALL(verify_all_rows, ctx, hash_distinct, 1000000, true);
  close_operator(ctx, hash_distinct);
}

TEST_F(TestHashDistinctTest, test_big_data01)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  ObFakeTable &fake_table = get_fake_table();
  ObFakeTable &result_table = get_result_table();
  TestHashDistinct hash_distinct;
  ObCollationType cs_type = CS_TYPE_UTF8MB4_GENERAL_CI;
  ObSQLSessionInfo my_session;
  init(ctx, my_session, hash_distinct, 3);

  enable_big_row_ = true;
  hash_distinct.set_mem_limit(64 * 1024 * 1024 * 10);
  //fake table: distinct column(c1)
  CALL(append_rows, 1000000);
  CALL(append_rows, 100000);
  CALL(append_rows, 1000000);
  CALL(append_rows, 1000000);
  CALL(append_rows, 1000000);

  hash_distinct.init(1);
  hash_distinct.add_distinct_column(0, cs_type);
  //hash_distinct.add_distinct_column(1, cs_type);
  open_operator(ctx, hash_distinct);
  CALL(verify_all_rows, ctx, hash_distinct, 1000000, true);
  close_operator(ctx, hash_distinct);
}



TEST_F(TestHashDistinctTest, test_utf8mb4_bin)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  ObFakeTable &fake_table = get_fake_table();
  ObFakeTable &result_table = get_result_table();
  TestHashDistinct hash_distinct;
  ObCollationType cs_type = CS_TYPE_UTF8MB4_BIN;
  ObSQLSessionInfo my_session;
  init(ctx, my_session, hash_distinct, 3);
  //fake table: distinct column(c1)
   ADD_ROW(fake_table, COL("r"), COL(1), COL(1));
  ADD_ROW(fake_table, COL("r"), COL(1), COL(2));
  ADD_ROW(fake_table, COL("s"), COL(2), COL(3));
  ADD_ROW(fake_table, COL("s"), COL(1), COL(3));
  ADD_ROW(fake_table, COL("t"), COL(2), COL(1));
  ADD_ROW(fake_table, COL("t"), COL(3), COL(2));
  ADD_ROW(fake_table, COL("ß"), COL(3), COL(4));

  ADD_ROW(result_table, COL("r"), COL(1), COL(1));
  ADD_ROW(result_table, COL("s"), COL(2), COL(3));
  ADD_ROW(result_table, COL("t"), COL(2), COL(1));
  ADD_ROW(result_table, COL("ß"), COL(3), COL(4));

  fake_table.set_rows(7);
  fake_table.set_width(20);

  ASSERT_EQ(OB_SUCCESS, hash_distinct.init(1));
  ASSERT_EQ(OB_SUCCESS, hash_distinct.add_distinct_column(0, cs_type));
  open_operator(ctx, hash_distinct);
  EXCEPT_RESULT_WITH_IDX(ctx, result_table, hash_distinct, 0, 0, cs_type);
  close_operator(ctx, hash_distinct);
}

TEST_F(TestHashDistinctTest, test_utf8mb4_general_ci)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  ObFakeTable &fake_table = get_fake_table();
  ObFakeTable &result_table = get_result_table();
  TestHashDistinct hash_distinct;
  ObCollationType cs_type = CS_TYPE_UTF8MB4_GENERAL_CI;
  ObSQLSessionInfo my_session;
  init(ctx, my_session, hash_distinct, 3);
  //fake table: distinct column(c1)
  ADD_ROW(fake_table, COL("r"), COL("r"), COL(1));
  ADD_ROW(fake_table, COL("r"), COL("r"), COL(2));
  ADD_ROW(fake_table, COL("s"), COL("s"), COL(3));
  ADD_ROW(fake_table, COL("ß"), COL("s"), COL(4));
  ADD_ROW(fake_table, COL("s"), COL("ß"), COL(3));
  ADD_ROW(fake_table, COL("t"), COL("t"), COL(1));
  ADD_ROW(fake_table, COL("t"), COL("t"), COL(2));

  ADD_ROW(result_table, COL("r"), COL("r"), COL(1));
  ADD_ROW(result_table, COL("s"), COL("s"), COL(3));
  ADD_ROW(result_table, COL("t"), COL("t"), COL(1));

  fake_table.set_rows(7);
  fake_table.set_width(20);

  hash_distinct.init(2);
  hash_distinct.add_distinct_column(0, cs_type);
  hash_distinct.add_distinct_column(1, cs_type);

  open_operator(ctx, hash_distinct);
  EXCEPT_RESULT_WITH_IDX(ctx, result_table, hash_distinct, 0, 1, cs_type);
  close_operator(ctx, hash_distinct);
}

TEST_F(TestHashDistinctTest, test_all_in_mem)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  ObFakeTable &fake_table = get_fake_table();
  ObFakeTable &result_table = get_result_table();
  TestHashDistinct hash_distinct;
  ObCollationType cs_type = CS_TYPE_UTF8MB4_GENERAL_CI;
  ObSQLSessionInfo my_session;
  init(ctx, my_session, hash_distinct, 3);

  hash_distinct.set_mem_limit(64 * 1024 * 1024);
  //fake table: distinct column(c1)
  CALL(append_rows, 1000);

  fake_table_.set_rows(1000);
  fake_table_.set_width(530);

  hash_distinct.init(2);
  hash_distinct.add_distinct_column(0, cs_type);
  hash_distinct.add_distinct_column(1, cs_type);

  open_operator(ctx, hash_distinct);
  CALL(verify_all_rows, ctx, hash_distinct, 1000, true);
  close_operator(ctx, hash_distinct);
}

TEST_F(TestHashDistinctTest, test_all_in_mem1)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  ObFakeTable &fake_table = get_fake_table();
  ObFakeTable &result_table = get_result_table();
  TestHashDistinct hash_distinct;
  ObCollationType cs_type = CS_TYPE_UTF8MB4_GENERAL_CI;
  ObSQLSessionInfo my_session;
  init(ctx, my_session, hash_distinct, 3);

  hash_distinct.set_mem_limit(64 * 1024 * 1024);
  //fake table: distinct column(c1)
  CALL(append_rows, 3000);

  fake_table_.set_rows(3000);
  fake_table_.set_width(530);

  hash_distinct.init(2);
  hash_distinct.add_distinct_column(0, cs_type);
  hash_distinct.add_distinct_column(1, cs_type);

  open_operator(ctx, hash_distinct);
  CALL(verify_all_rows, ctx, hash_distinct, 3000, true);
  close_operator(ctx, hash_distinct);
}

TEST_F(TestHashDistinctTest, test_all_in_mem2)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  ObFakeTable &fake_table = get_fake_table();
  ObFakeTable &result_table = get_result_table();
  TestHashDistinct hash_distinct;
  ObCollationType cs_type = CS_TYPE_UTF8MB4_GENERAL_CI;
  ObSQLSessionInfo my_session;
  init(ctx, my_session, hash_distinct, 3);

  hash_distinct.set_mem_limit(64 * 1024 * 1024 * 10);
  //fake table: distinct column(c1)
  CALL(append_rows, 3000);
  CALL(append_rows, 3000);
  CALL(append_rows, 3000);

  hash_distinct.init(2);
  hash_distinct.add_distinct_column(0, cs_type);
  hash_distinct.add_distinct_column(1, cs_type);
  open_operator(ctx, hash_distinct);
  //ASSERT_EQ(3000, hash_distinct.get_rows());
  CALL(verify_all_rows, ctx, hash_distinct, 3000, true);
  close_operator(ctx, hash_distinct);
}


TEST_F(TestHashDistinctTest, test_big_data)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  ObFakeTable &fake_table = get_fake_table();
  ObFakeTable &result_table = get_result_table();
  TestHashDistinct hash_distinct;
  ObCollationType cs_type = CS_TYPE_UTF8MB4_GENERAL_CI;
  ObSQLSessionInfo my_session;
  init(ctx, my_session, hash_distinct, 3);

  enable_big_row_ = true;
  hash_distinct.set_mem_limit(64 * 1024 * 1024 * 10);
  //fake table: distinct column(c1)
  CALL(append_rows, 20000);

  hash_distinct.init(2);
  hash_distinct.add_distinct_column(0, cs_type);
  hash_distinct.add_distinct_column(1, cs_type);
  open_operator(ctx, hash_distinct);
  CALL(verify_all_rows, ctx, hash_distinct, 20000, true);
  close_operator(ctx, hash_distinct);
}


TEST_F(TestHashDistinctTest, test_big_data2)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  TestHashDistinct hash_distinct;
  ObCollationType cs_type = CS_TYPE_UTF8MB4_GENERAL_CI;
  ObSQLSessionInfo my_session;
  init(ctx, my_session, hash_distinct, 3);

  enable_big_row_ = true;
  hash_distinct.set_mem_limit(64 * 1024 * 1024 * 10);
  //fake table: distinct column(c1)
  CALL(append_rows, 20000);
  CALL(append_rows, 20000);
  CALL(append_rows, 20000);

  hash_distinct.init(2);
  hash_distinct.add_distinct_column(0, cs_type);
  hash_distinct.add_distinct_column(1, cs_type);
  open_operator(ctx, hash_distinct);
  CALL(verify_all_rows, ctx, hash_distinct, 20000, true);
  close_operator(ctx, hash_distinct);
}


TEST_F(TestHashDistinctTest, test_big_data3)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  ObFakeTable &fake_table = get_fake_table();
  ObFakeTable &result_table = get_result_table();
  TestHashDistinct hash_distinct;
  ObCollationType cs_type = CS_TYPE_UTF8MB4_GENERAL_CI;
  ObSQLSessionInfo my_session;
  init(ctx, my_session, hash_distinct, 3);

  enable_big_row_ = true;
  hash_distinct.set_mem_limit(64 * 1024 * 1024 * 10);
  //fake table: distinct column(c1)
  CALL(append_rows, 50000);
  CALL(append_rows, 50000);
  CALL(append_rows, 50000);

  hash_distinct.init(2);
  hash_distinct.add_distinct_column(0, cs_type);
  hash_distinct.add_distinct_column(1, cs_type);
  open_operator(ctx, hash_distinct);
  CALL(verify_all_rows, ctx, hash_distinct, 50000, true);
  close_operator(ctx, hash_distinct);
}

TEST_F(TestHashDistinctTest, test_big_data4)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  ObFakeTable &fake_table = get_fake_table();
  ObFakeTable &result_table = get_result_table();
  TestHashDistinct hash_distinct;
  ObCollationType cs_type = CS_TYPE_UTF8MB4_GENERAL_CI;
  ObSQLSessionInfo my_session;
  init(ctx, my_session, hash_distinct, 3);

  enable_big_row_ = true;
  hash_distinct.set_mem_limit(64 * 1024 * 1024 * 10);
  //fake table: distinct column(c1)
  CALL(append_rows, 1000000);
  CALL(append_rows, 1000000);
  CALL(append_rows, 1000000);
  CALL(append_rows, 1000000);
  CALL(append_rows, 1000000);

  hash_distinct.init(2);
  hash_distinct.add_distinct_column(0, cs_type);
  hash_distinct.add_distinct_column(1, cs_type);
  open_operator(ctx, hash_distinct);
  CALL(verify_all_rows, ctx, hash_distinct, 1000000, true);
  close_operator(ctx, hash_distinct);
}


} // end namespace sql
} // end namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::sql::init_sql_factories();
  oceanbase::common::ObLogger::get_logger().set_log_level("WARN");
  testing::InitGoogleTest(&argc, argv);
  auto *env = new (oceanbase::sql::TestEnv);
  testing::AddGlobalTestEnvironment(env);
  int ret = RUN_ALL_TESTS();
  OB_LOGGER.disable();
  return ret;

}
