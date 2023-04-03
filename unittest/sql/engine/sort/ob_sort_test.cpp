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

#include "sql/engine/sort/ob_sort.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_physical_plan.h"
#include "lib/utility/ob_test_util.h"
#include "lib/utility/ob_tracepoint.h"
#include "lib/container/ob_se_array.h"
#include <gtest/gtest.h>
#include "ob_fake_table.h"
#include "sql/engine/ob_exec_context.h"
#include "lib/worker.h"
#include "observer/ob_signal_handle.h"
#include "storage/blocksstable/ob_data_file_prepare.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "share/ob_simple_mem_limit_getter.h"

#include <thread>
#include <vector>
#include <gtest/gtest.h>

using namespace oceanbase;
using namespace oceanbase::sql;
using namespace oceanbase::omt;
using namespace oceanbase::common;
using oceanbase::sql::test::ObFakeTable;

static ObSimpleMemLimitGetter getter;

#define TEST_SORT_DUMP_GET_HASH_AREA_SIZE() (get_sort_area_size())
#define TEST_SORT_DUMP_SET_HASH_AREA_SIZE(size) (set_sort_area_size(size))

class ObSortTest: public blocksstable::TestDataFilePrepare
{
public:
  ObSortTest();
  virtual ~ObSortTest();
private:
  // disallow copy
  ObSortTest(const ObSortTest &other);
  ObSortTest& operator=(const ObSortTest &other);
protected:
  virtual void SetUp() override
  {
    GCONF.enable_sql_operator_dump.set_value("True");
    ASSERT_EQ(OB_SUCCESS, init_tenant_mgr());
    blocksstable::TestDataFilePrepare::SetUp();
  }

  virtual void TearDown() override
  {
    blocksstable::TestDataFilePrepare::TearDown();
    destroy_tenant_mgr();
  }
  int init_tenant_mgr();
  int64_t get_sort_area_size()
  {
    int64_t sort_area_size = 0;
    int ret = OB_SUCCESS;
    ret = ObSqlWorkareaUtil::get_workarea_size(SORT_WORK_AREA, OB_SYS_TENANT_ID, sort_area_size);
    if (OB_FAIL(ret)) {
      SQL_ENG_LOG(WARN, "failed to get hash area size", K(ret), K(sort_area_size));
    }
    return sort_area_size;
  }

  void set_sort_area_size(int64_t size)
  {
    int ret = OB_SUCCESS;
    int64_t tenant_id = OB_SYS_TENANT_ID;
    ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
    if (tenant_config.is_valid()) {
      tenant_config->_sort_area_size = size;
    } else {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "unexpected status: config is invalid", K(tenant_id));
    }
    // ASSERT_EQ(OB_SUCCESS, ret);
  }
  void destroy_tenant_mgr()
  {
  }

  template<typename SortInit>
  void sort_test(int64_t row_count, int64_t mem_limit, SortInit sort_init, int64_t verify_row_cnt = -1, bool local_merge_sort = false);
  void sort_test(int64_t row_count, int64_t mem_limit,
                 int64_t sort_col1, ObCollationType cs_type1,
                 int64_t sort_col2, ObCollationType cs_type2);
  void local_merge_sort_test(int64_t row_count, int64_t mem_limit,
                 int64_t sort_col1, ObCollationType cs_type1,
                 int64_t sort_col2, ObCollationType cs_type2);
  void serialize_test();
  void sort_exception_test(int expect_ret);
private:
  static void copy_cell_varchar(ObObj &cell, char *buf, int64_t buf_size)
  {
    ObString str;
    ASSERT_EQ(OB_SUCCESS, cell.get_varchar(str));
    ASSERT_TRUE(str.length() < buf_size);
    memcpy(buf, str.ptr(), str.length());
    str.assign_ptr(buf, str.length());
    cell.set_varchar(str);
    return;
  }
  void cons_op_schema_objs(const ObIArray<ObSortColumn> &sort_columns,
                           ObIArray<ObOpSchemaObj> &op_schema_objs)
  {
    for (int64_t i =0; i < sort_columns.count(); i++) {
      ObOpSchemaObj op_schema_obj;
      if (0 == sort_columns.at(i).index_) {
        op_schema_obj.obj_type_ = common::ObVarcharType;
      } else {
        op_schema_obj.obj_type_ = common::ObIntType;
      }
      op_schema_objs.push_back(op_schema_obj);
    }
    return;
  }
};

class ObSortPlan
{
public:
  static ObSort &get_instance()
  {
    return *sort_;
  }
  template<typename SortInit>
  static int init(int64_t row_count, int64_t mem_limit, SortInit sort_init)
  {
    if (mem_limit <= 0) {
      mem_limit = 1 << 20;
    }
    int ret = OB_SUCCESS;
    sort_->set_id(0);
    input_table_->set_id(1);
    sort_->set_column_count(input_table_->get_column_count());
    sort_->set_mem_limit(mem_limit);
    int64_t tenant_id = OB_SYS_TENANT_ID;
    ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
    if (tenant_config.is_valid()) {
      tenant_config->_sort_area_size = mem_limit;
    } else {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "unexpected status: config is invalid", K(tenant_id));
    }
    cons_run_filename(*filename_);
    input_table_->set_row_count(row_count);
    input_table_->set_phy_plan(physical_plan_);
    sort_->set_phy_plan(physical_plan_);
    row_count_ = row_count;
    if (OB_FAIL(sort_->set_child(0, *input_table_))) {}
    else if (OB_FAIL(sort_init(*sort_))) {}
    return ret;
  }

  static void set_local_merge_sort()
  {
    sort_->set_local_merge_sort(true);
  }

  static int init(int64_t row_count, int64_t mem_limit,
                  int64_t sort_col1, ObCollationType cs_type1,
                  int64_t sort_col2, ObCollationType cs_type2)
  {
    return init(row_count, mem_limit,
        [&](ObSort &sort) {
          int ret = OB_SUCCESS;
          if (OB_FAIL(sort.init_sort_columns(2))) {
          } else if (OB_FAIL(sort.add_sort_column(sort_col1, cs_type1, false,
                                                  ObMaxType, default_asc_direction()))) {
          } else if (OB_FAIL(sort.add_sort_column(sort_col2, cs_type2, true,
                                                  ObMaxType, default_asc_direction()))) {
          }
          return ret;
        }
        );
  }
  static void reset()
  {
    sort_->reset();
    input_table_->reset();
    row_count_ = -1;
  }
  static void reuse()
  {
    sort_->reuse();
    input_table_->reuse();
    row_count_ = -1;
  }
private:
  ObSortPlan();
  static void cons_run_filename(ObString &filename)
  {
    char *filename_buf = (char *)"ob_sort_test.run";
    filename.assign_ptr(filename_buf, (int32_t)strlen(filename_buf));
    return;
  }
public:
private:
  static ObPhysicalPlan *physical_plan_;
  static ObFakeTable *input_table_;
  static ObSort *sort_;
  static int64_t row_count_;
  static ObString *filename_;
};

ObSortTest::ObSortTest()
  : blocksstable::TestDataFilePrepare(&getter, "TestDiskIR", 2<<20, 2000)
{
}

ObSortTest::~ObSortTest()
{
}

int ObSortTest::init_tenant_mgr()
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

#define BEGIN_THREAD_CODE_V2(num) {std::vector<std::thread *> _threads; for (int _i = 0; _i < (num); _i++) _threads.push_back(new std::thread([&]
#define END_THREAD_CODE_V2() )); for (auto t : _threads) t->join();}

template<typename SortInit>
void ObSortTest::sort_test(int64_t row_count, int64_t mem_limit, SortInit sort_init, int64_t verify_row_cnt, bool local_merge_sort)
{
  ASSERT_EQ(OB_SUCCESS, ObSortPlan::init(row_count, mem_limit, sort_init));
  if (local_merge_sort) {
    ObSortPlan::set_local_merge_sort();
  }

  BEGIN_THREAD_CODE_V2(2) {
    ObExecContext exec_ctx;
    ASSERT_EQ(OB_SUCCESS, exec_ctx.init_phy_op(2));
    ASSERT_EQ(OB_SUCCESS, exec_ctx.create_physical_plan_ctx());
    ObSQLSessionInfo my_session;
    my_session.test_init(0,0,0,NULL);
    my_session.init_tenant("sys", 1);
    exec_ctx.set_my_session(&my_session);
    THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + 600000000);

    // do sort.
    ObSort &sort = ObSortPlan::get_instance();
    ASSERT_EQ(OB_SUCCESS, sort.open(exec_ctx));

    auto &sort_columns = sort.get_sort_columns();

    ObSEArray<ObOpSchemaObj, 8> op_schema_objs;
    cons_op_schema_objs(sort_columns, op_schema_objs);
    sort.get_op_schema_objs_for_update().assign(op_schema_objs);

    ObObj pre[sort_columns.count()];
    char varchar_buf[1024];
    const ObNewRow *row = NULL;
    int64_t cnt = verify_row_cnt > 0 ? verify_row_cnt : row_count;
    for (int64_t i = 0; i < cnt; ++i) {
      ASSERT_EQ(OB_SUCCESS, sort.get_next_row(exec_ctx, row)) << i;
      // check order
      for (int64_t j = 0; i > 0 && j < sort_columns.count(); j++) {
        auto &col = sort_columns.at(j);
        int cmp = pre[j].compare(row->cells_[col.index_], col.cs_type_);
        if (cmp != 0) {
          ASSERT_TRUE(col.is_ascending() ? cmp < 0 : cmp > 0);
          break;
        }
      }
      // save previous row
      int64_t pos = 0;
      for (int64_t j = 0; j < sort_columns.count(); j++) {
        pre[j] = row->cells_[sort_columns.at(j).index_];
        auto &c = pre[j];
        if (c.is_string_type()) {
          auto len = std::min((uint64_t)c.val_len_, sizeof(varchar_buf) - pos);
          MEMCPY(varchar_buf + pos, c.v_.string_, len);
          c.v_.string_ = varchar_buf + pos;
          pos += len;
        }
      }
    } // end for

    ASSERT_EQ(OB_ITER_END, sort.get_next_row(exec_ctx, row));

    ASSERT_EQ(OB_SUCCESS, sort.close(exec_ctx));
    int64_t sort_row_count = 0;
    ASSERT_EQ(OB_SUCCESS, sort.get_sort_row_count(exec_ctx, sort_row_count));
    ASSERT_EQ(row_count, sort_row_count);

    ob_print_mod_memory_usage();
    // TODO: not supported now.
    // ObTenantManager::get_instance().print_tenant_usage();
  }
  END_THREAD_CODE_V2();
  ObSortPlan::reset();
}

void ObSortTest::sort_test(int64_t row_count, int64_t mem_limit,
                           int64_t sort_col1, ObCollationType cs_type1,
                           int64_t sort_col2, ObCollationType cs_type2)
{
  sort_test(row_count, mem_limit,
      [&](ObSort &sort) {
        int ret = OB_SUCCESS;
        if (OB_FAIL(sort.init_sort_columns(2))) {
        } else if (OB_FAIL(sort.add_sort_column(sort_col1, cs_type1, false,
                                                ObMaxType, default_asc_direction()))) {
        } else if (OB_FAIL(sort.add_sort_column(sort_col2, cs_type2, true,
                                                ObMaxType, default_asc_direction()))) {
        }
        return ret;
      }
      );
}

void ObSortTest::local_merge_sort_test(int64_t row_count, int64_t mem_limit,
                 int64_t sort_col1, ObCollationType cs_type1,
                 int64_t sort_col2, ObCollationType cs_type2)
{
  sort_test(row_count, mem_limit,
      [&](ObSort &sort) {
        int ret = OB_SUCCESS;
        if (OB_FAIL(sort.init_sort_columns(2))) {
        } else if (OB_FAIL(sort.add_sort_column(sort_col1, cs_type1, true,
                                                ObMaxType, default_asc_direction()))) {
        } else if (OB_FAIL(sort.add_sort_column(sort_col2, cs_type2, true,
                                                ObMaxType, default_asc_direction()))) {
        }
        return ret;
      }, -1, true);
}

void ObSortTest::serialize_test()
{
  ObSort &sort_1 = ObSortPlan::get_instance();
  ObArenaAllocator alloc;
  ObSort sort_2(alloc);
  const int64_t MAX_SERIALIZE_BUF_LEN = 1024;
  char buf[MAX_SERIALIZE_BUF_LEN] = {'\0'};
  ASSERT_EQ(OB_SUCCESS, ObSortPlan::init(1024, 1024, 0, CS_TYPE_INVALID, 1, CS_TYPE_INVALID));

  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, sort_1.serialize(buf, MAX_SERIALIZE_BUF_LEN, pos));
  ASSERT_EQ(pos, sort_1.get_serialize_size());
  int64_t data_len = pos;

  sort_2.set_phy_plan(const_cast<ObPhysicalPlan *>(sort_1.get_phy_plan()));
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, sort_2.deserialize(buf, data_len, pos));
  ASSERT_EQ(pos, data_len);
  const char *str_1 = to_cstring(sort_1);
  const char *str_2 = to_cstring(sort_2);
  ASSERT_EQ(0, strcmp(str_1, str_2)) << "sort_1: " << to_cstring(sort_1)
      << std::endl << "sort_2: " << to_cstring(sort_2);

  ObSortPlan::reuse();
}

void ObSortTest::sort_exception_test(int expect_ret)
{
  int ret = OB_SUCCESS;
  ObExecContext exec_ctx;
  const ObNewRow *row = NULL;
  ObSQLSessionInfo my_session;
  my_session.test_init(0,0,0,NULL);
  my_session.init_tenant("sys", 1);
  exec_ctx.set_my_session(&my_session);

  ASSERT_EQ(OB_SUCCESS, exec_ctx.init_phy_op(2));
  ASSERT_EQ(OB_SUCCESS, exec_ctx.create_physical_plan_ctx());
  ObSort &sort = ObSortPlan::get_instance();
  ObSortPlan::reset();

  int64_t row_count = 16 * 1024;
  if (OB_FAIL(ObSortPlan::init(row_count, 0, 0, CS_TYPE_UTF8MB4_BIN, 1, CS_TYPE_UTF8MB4_BIN))) {}
  else if (OB_FAIL(sort.open(exec_ctx))) {}
  else {
    ObSEArray<ObOpSchemaObj, 8> op_schema_objs;
    cons_op_schema_objs(sort.get_sort_columns(), op_schema_objs);
    sort.get_op_schema_objs_for_update().assign(op_schema_objs);
    while (OB_SUCC(ret)) {
      ret = sort.get_next_row(exec_ctx, row);
    }
    if (OB_ITER_END == ret) {
      int64_t sort_row_count = 0;
      if (OB_FAIL(sort.close(exec_ctx))) {}
      else if (OB_FAIL(sort.get_sort_row_count(exec_ctx, sort_row_count))) {}
      else {
        ASSERT_EQ(row_count, sort_row_count);
      }
    }
  }
  sort.close(exec_ctx);
  ObSortPlan::reuse();
  if (OB_FAIL(ret)) {
    ASSERT_EQ(expect_ret, ret);
  }
}

TEST_F(ObSortTest, varchar_int_item_in_mem_test)
{
  int64_t sort_col1 = 0;
  int64_t sort_col2 = 1;
  ObCollationType cs_type1 = CS_TYPE_UTF8MB4_BIN;
  ObCollationType cs_type2 = CS_TYPE_UTF8MB4_BIN;
  sort_test(16 * 1024, 256 * 1024 * 1024, sort_col1, cs_type1, sort_col2, cs_type2);
  sort_test(64 * 1024, 256 * 1024 * 1024, sort_col1, cs_type1, sort_col2, cs_type2);
  sort_test(256 * 1024, 256 * 1024 * 1024, sort_col1, cs_type1, sort_col2, cs_type2);
  cs_type2 = CS_TYPE_UTF8MB4_GENERAL_CI;
  sort_test(16 * 1024, 256 * 1024 * 1024, sort_col1, cs_type1, sort_col2, cs_type2);
  sort_test(64 * 1024, 256 * 1024 * 1024, sort_col1, cs_type1, sort_col2, cs_type2);
  sort_test(256 * 1024, 256 * 1024 * 1024, sort_col1, cs_type1, sort_col2, cs_type2);
}

TEST_F(ObSortTest, varchar_int_item_merge_sort_test)
{
  int64_t sort_col1 = 0;
  int64_t sort_col2 = 1;
  ObCollationType cs_type1 = CS_TYPE_UTF8MB4_BIN;
  ObCollationType cs_type2 = CS_TYPE_UTF8MB4_BIN;
  sort_test(128* 1024, 1024 * 1024, sort_col1, cs_type1, sort_col2, cs_type2);
  sort_test(256 * 1024, 4 * 1024 * 1024, sort_col1, cs_type1, sort_col2, cs_type2);
  // recursive merge needed.
  sort_test(256 * 1024, 512 * 1024, sort_col1, cs_type1, sort_col2, cs_type2);
  cs_type2 = CS_TYPE_UTF8MB4_GENERAL_CI;
  sort_test(128* 1024, 1024 * 1024, sort_col1, cs_type1, sort_col2, cs_type2);
  sort_test(256 * 1024, 4 * 1024 * 1024, sort_col1, cs_type1, sort_col2, cs_type2);
}

TEST_F(ObSortTest, int_int_item_in_mem_test)
{
  int64_t sort_col1 = 1;
  int64_t sort_col2 = 2;
  ObCollationType cs_type1 = CS_TYPE_UTF8MB4_BIN;
  ObCollationType cs_type2 = CS_TYPE_UTF8MB4_BIN;
  sort_test(16 * 1024, 256 * 1024 * 1024, sort_col1, cs_type1, sort_col2, cs_type2);
  sort_test(64 * 1024, 256 * 1024 * 1024, sort_col1, cs_type1, sort_col2, cs_type2);
  sort_test(256 * 1024, 256 * 1024 * 1024, sort_col1, cs_type1, sort_col2, cs_type2);
}

TEST_F(ObSortTest, int_int_item_merge_sort_test)
{
  int64_t sort_col1 = 1;
  int64_t sort_col2 = 2;
  ObCollationType cs_type1 = CS_TYPE_UTF8MB4_BIN;
  ObCollationType cs_type2 = CS_TYPE_UTF8MB4_BIN;
  // sort_test(64 * 1024, 1024 * 1024, sort_col1, cs_type1, sort_col2, cs_type2);
  sort_test(256 * 1024, 1024 * 1024, sort_col1, cs_type1, sort_col2, cs_type2);
}

TEST_F(ObSortTest, test_conf)
{
  TEST_SORT_DUMP_SET_HASH_AREA_SIZE(128* 1024 * 1024);
  int64_t sort_mem = 0;
  sort_mem = TEST_SORT_DUMP_GET_HASH_AREA_SIZE();
  ASSERT_EQ((128 * 1024 * 1024), sort_mem);

  TEST_SORT_DUMP_SET_HASH_AREA_SIZE(1 * 1024* 1024 * 1024);
  sort_mem = TEST_SORT_DUMP_GET_HASH_AREA_SIZE();
  ASSERT_EQ((1024 * 1024 * 1024), sort_mem);
  TEST_SORT_DUMP_SET_HASH_AREA_SIZE(128* 1024 * 1024);
}

TEST_F(ObSortTest, prefix_sort_test1)
{
  sort_test(1024 * 1024, 1024 * 1024, [](ObSort &sort) {
        int ret = OB_SUCCESS;
        if (OB_FAIL(sort.init_sort_columns(3))) {
        } else if (OB_FAIL(sort.add_sort_column(ObFakeTable::COL1_ROW_ID, CS_TYPE_BINARY, true, ObMaxType, default_asc_direction()))) {
        } else if (OB_FAIL(sort.add_sort_column(ObFakeTable::COL0_RAND_STR, CS_TYPE_UTF8MB4_BIN, true, ObMaxType, default_asc_direction()))) {
        } else if (OB_FAIL(sort.add_sort_column(ObFakeTable::COL1_ROW_ID, CS_TYPE_UTF8MB4_BIN, true, ObMaxType, default_asc_direction()))) {
        } else {
          sort.set_prefix_pos(1);
        }
        return ret;
      });
}

TEST_F(ObSortTest, prefix_sort_test3)
{
  sort_test(256 * 1024, 1024 * 1024, [](ObSort &sort) {
        int ret = OB_SUCCESS;
        if (OB_FAIL(sort.init_sort_columns(3))) {
        } else if (OB_FAIL(sort.add_sort_column(ObFakeTable::COL5_ROW_ID_DIV_3, CS_TYPE_BINARY, true, ObMaxType, default_asc_direction()))) {
        } else if (OB_FAIL(sort.add_sort_column(ObFakeTable::COL0_RAND_STR, CS_TYPE_UTF8MB4_BIN, true, ObMaxType, default_asc_direction()))) {
        } else if (OB_FAIL(sort.add_sort_column(ObFakeTable::COL1_ROW_ID, CS_TYPE_UTF8MB4_BIN, true, ObMaxType, default_asc_direction()))) {
        } else {
          sort.set_prefix_pos(1);
        }
        return ret;
      });
}

TEST_F(ObSortTest, prefix_merge_sort_test)
{
  sort_test(256 * 1024, 1024 * 1024, [](ObSort &sort) {
        int ret = OB_SUCCESS;
        if (OB_FAIL(sort.init_sort_columns(3))) {
        } else if (OB_FAIL(sort.add_sort_column(ObFakeTable::COL11_ROW_ID_MULTIPLY_3_DIV_COUNT,
                                                CS_TYPE_BINARY,
                                                true, ObMaxType, default_asc_direction()))) {
        } else if (OB_FAIL(sort.add_sort_column(ObFakeTable::COL0_RAND_STR,
                                                CS_TYPE_UTF8MB4_BIN,
                                                true, ObMaxType, default_asc_direction()))) {
        } else if (OB_FAIL(sort.add_sort_column(ObFakeTable::COL1_ROW_ID,
                                                CS_TYPE_UTF8MB4_BIN,
                                                true, ObMaxType, default_asc_direction()))) {
        } else {
          sort.set_prefix_pos(1);
        }
        return ret;
      });
}

class MockExpr : public ObSqlExpression
{
public:
  MockExpr(ObIAllocator &alloc, int64_t cnt) : ObSqlExpression(alloc)
  {
    set_item_count(1);
    start_gen_infix_exr();
    ObPostExprItem item;
    item.set_int(cnt);
    item.set_item_type(T_INT);
    add_expr_item(item);
  }

  int calc(common::ObExprCtx &, const common::ObNewRow &, common::ObObj &result) const
  {
    result.set_int(get_expr_items().at(0).get_obj().get_int());
    return OB_SUCCESS;
  }
};

TEST_F(ObSortTest, topn_sort_test)
{
  ObArenaAllocator alloc;
  MockExpr expr(alloc, 4);

  sort_test(20, 10 << 20, [&](ObSort &sort) {
        int ret = OB_SUCCESS;
        if (OB_FAIL(sort.init_sort_columns(2))) {
        } else if (OB_FAIL(sort.add_sort_column(ObFakeTable::COL0_RAND_STR,
                                                CS_TYPE_UTF8MB4_BIN,
                                                true, ObMaxType, default_asc_direction()))) {
        } else if (OB_FAIL(sort.add_sort_column(ObFakeTable::COL1_ROW_ID,
                                                CS_TYPE_UTF8MB4_BIN,
                                                true, ObMaxType, default_asc_direction()))) {
        } else {
          sort.set_topn_expr(&expr);
        }
        return ret;
      },
      4);
}

TEST_F(ObSortTest, topn_disk_sort_test)
{
  ObArenaAllocator alloc;
  MockExpr expr(alloc, 4);

  sort_test(20, 0, [&](ObSort &sort) {
        int ret = OB_SUCCESS;
        if (OB_FAIL(sort.init_sort_columns(2))) {
        } else if (OB_FAIL(sort.add_sort_column(ObFakeTable::COL0_RAND_STR,
                                                CS_TYPE_UTF8MB4_BIN,
                                                true, ObMaxType, default_asc_direction()))) {
        } else if (OB_FAIL(sort.add_sort_column(ObFakeTable::COL1_ROW_ID,
                                                CS_TYPE_UTF8MB4_BIN,
                                                true, ObMaxType, default_asc_direction()))) {
        } else {
          sort.set_topn_expr(&expr);
        }
        return ret;
      },
      4);
}

TEST_F(ObSortTest, local_merge_sort_test)
{
  int64_t sort_col1 = 1;
  int64_t sort_col2 = 2;
  ObCollationType cs_type1 = CS_TYPE_UTF8MB4_BIN;
  ObCollationType cs_type2 = CS_TYPE_UTF8MB4_BIN;
  local_merge_sort_test(0, 256 * 1024 * 1024, sort_col1, cs_type1, sort_col2, cs_type2);
  local_merge_sort_test(16, 256 * 1024 * 1024, sort_col1, cs_type1, sort_col2, cs_type2);
  local_merge_sort_test(256, 256 * 1024 * 1024, sort_col1, cs_type1, sort_col2, cs_type2);

  local_merge_sort_test(16, 0, sort_col1, cs_type1, sort_col2, cs_type2);
  local_merge_sort_test(256, 0, sort_col1, cs_type1, sort_col2, cs_type2);
  local_merge_sort_test(64 * 1024, 256 * 1024 * 1024, sort_col1, cs_type1, sort_col2, cs_type2);
  local_merge_sort_test(256 * 1024, 256 * 1024 * 1024, sort_col1, cs_type1, sort_col2, cs_type2);
}

TEST_F(ObSortTest, local_merge_sort_disk_test)
{
  ObArenaAllocator alloc;
  MockExpr expr(alloc, 4);

  sort_test(256 * 1024, 1 << 20, [&](ObSort &sort) {
        int ret = OB_SUCCESS;
        if (OB_FAIL(sort.init_sort_columns(2))) {
        } else if (OB_FAIL(sort.add_sort_column(ObFakeTable::COL0_RAND_STR,
                                                CS_TYPE_UTF8MB4_BIN,
                                                true, ObMaxType, default_asc_direction()))) {
        } else if (OB_FAIL(sort.add_sort_column(ObFakeTable::COL1_ROW_ID,
                                                CS_TYPE_UTF8MB4_BIN,
                                                true, ObMaxType, default_asc_direction()))) {
        } else {
          sort.set_topn_expr(&expr);
        }
        return ret;
      },
      4, true);
}


TEST_F(ObSortTest, local_merge_sort_topn_test)
{
  ObArenaAllocator alloc;
  MockExpr expr(alloc, 4);

  sort_test(1024, 10 << 20, [&](ObSort &sort) {
        int ret = OB_SUCCESS;
        if (OB_FAIL(sort.init_sort_columns(2))) {
        } else if (OB_FAIL(sort.add_sort_column(ObFakeTable::COL0_RAND_STR,
                                                CS_TYPE_UTF8MB4_BIN,
                                                true, ObMaxType, default_asc_direction()))) {
        } else if (OB_FAIL(sort.add_sort_column(ObFakeTable::COL1_ROW_ID,
                                                CS_TYPE_UTF8MB4_BIN,
                                                true, ObMaxType, default_asc_direction()))) {
        } else {
          sort.set_topn_expr(&expr);
        }
        return ret;
      },
      4, true);
}

TEST_F(ObSortTest, ser)
{
  serialize_test();
}

#define SORT_EXCEPTION_TEST(file, func, key, err, expect_ret) \
  do { \
    TP_SET_ERROR("engine/sort/" file, func, key, err); \
    sort_exception_test(expect_ret); \
    TP_SET_ERROR("engine/sort/" file, func, key, NULL); \
    ASSERT_FALSE(HasFatalFailure()); \
  } while (0)

TEST_F(ObSortTest, sort_exception)
{
  THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + 600000000);
  SORT_EXCEPTION_TEST("ob_sort.cpp", "add_sort_column", "t1", OB_ERROR, OB_ERROR);
  SORT_EXCEPTION_TEST("ob_sort.cpp", "open", "t1", OB_ERROR, OB_ERROR);
  SORT_EXCEPTION_TEST("ob_sort.cpp", "open", "t3", OB_ERROR, OB_ERROR);
  SORT_EXCEPTION_TEST("ob_sort.cpp", "open", "t5", 1, OB_ERR_UNEXPECTED);
  SORT_EXCEPTION_TEST("ob_sort.cpp", "open", "t7", OB_ERROR, OB_ERROR);
  SORT_EXCEPTION_TEST("ob_sort.cpp", "open", "t9", OB_ERROR, OB_ERROR);
  SORT_EXCEPTION_TEST("ob_sort.cpp", "close", "t1", 1, OB_ERR_UNEXPECTED);
  SORT_EXCEPTION_TEST("ob_sort.cpp", "close", "t3", OB_ERROR, OB_ERROR);
  SORT_EXCEPTION_TEST("ob_sort.cpp", "do_sort", "t1", OB_ERROR, OB_ERROR);
  SORT_EXCEPTION_TEST("ob_sort.cpp", "do_sort", "t3", OB_ERROR, OB_ERROR);
  SORT_EXCEPTION_TEST("ob_sort.cpp", "do_sort", "t5", OB_ERROR, OB_ERROR);
  SORT_EXCEPTION_TEST("ob_sort.cpp", "do_sort", "t7", OB_ERROR, OB_ERROR);
  SORT_EXCEPTION_TEST("ob_sort.cpp", "do_sort", "t9", OB_ERROR, OB_ERROR);
  SORT_EXCEPTION_TEST("ob_sort.cpp", "do_sort", "t11", OB_ERROR, OB_ERROR);
  SORT_EXCEPTION_TEST("ob_sort.cpp", "do_sort", "t13", OB_ERROR, OB_ERROR);
  SORT_EXCEPTION_TEST("ob_sort.cpp", "inner_get_next_row", "t1", 1, OB_ERR_UNEXPECTED);
  SORT_EXCEPTION_TEST("ob_sort.cpp", "inner_get_next_row", "t3", 1, OB_TIMEOUT);
  // see comments for tracepoint t5 in inner_get_next_row() of ob_sort.cpp.
  // SORT_EXCEPTION_TEST("ob_sort.cpp", "inner_get_next_row", "t5", OB_ERROR, OB_ERROR);
  SORT_EXCEPTION_TEST("ob_sort.cpp", "inner_get_next_row", "t7", 1, OB_ERR_UNEXPECTED);
  SORT_EXCEPTION_TEST("ob_sort.cpp", "inner_get_next_row", "t9", OB_ERROR, OB_ERROR);
  SORT_EXCEPTION_TEST("ob_sort.cpp", "get_sort_row_count", "t1", 1, OB_ERR_UNEXPECTED);
}

ObPhysicalPlan *ObSortPlan::physical_plan_ = nullptr;
ObSort *ObSortPlan::sort_ = nullptr;
ObFakeTable *ObSortPlan::input_table_ = nullptr;
int64_t ObSortPlan::row_count_ = -1;
ObString *ObSortPlan::filename_ = nullptr;

int main(int argc, char **argv)
{
  ObClockGenerator::init();

  system("rm -f test_sort.log*");
  OB_LOGGER.set_file_name("test_sort.log", true, true);
  OB_LOGGER.set_log_level("INFO");

  oceanbase::observer::ObSignalHandle signal_handle;
  oceanbase::observer::ObSignalHandle::change_signal_mask();
  signal_handle.start();

  void *buf = nullptr;
  ObArenaAllocator allocator;
  buf = allocator.alloc(sizeof(ObPhysicalPlan));
  ObSortPlan::physical_plan_ = new(buf)ObPhysicalPlan();
  buf = allocator.alloc(sizeof(ObSort));
  ObSortPlan::sort_ = new (buf)ObSort(ObSortPlan::physical_plan_->get_allocator());
  buf = allocator.alloc(sizeof(ObFakeTable));
  ObSortPlan::input_table_ = new(buf)ObFakeTable();
  ObSortPlan::row_count_ = -1;
  buf = allocator.alloc(sizeof(ObString));
  ObSortPlan::filename_ = new (buf)ObString();

  ::testing::InitGoogleTest(&argc,argv);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  return RUN_ALL_TESTS();
}
