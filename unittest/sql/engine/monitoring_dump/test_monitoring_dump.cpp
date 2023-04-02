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

#include <string>
#include <sstream>
#include <iostream>
#include <sys/time.h>
#include <fstream>
#include <iterator>
#include <gtest/gtest.h>
#include "../../test_sql_utils.h"
#include "common/object/ob_object.h"
#include "common/row/ob_row.h"
#include "sql/ob_sql_init.h"
#include "sql/engine/expr/ob_sql_expression.h"
#include "sql/engine/ob_phy_operator.h"
#include "sql/engine/ob_single_child_phy_operator.h"
#include "sql/engine/ob_no_children_phy_operator.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/aggregate/ob_merge_groupby.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/basic/ob_monitoring_dump.h"
#include "sql/engine/test_engine_util.h"


using namespace oceanbase;
using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace ::testing;
using namespace oceanbase::share;

int64_t row_size = 3;
static ObArenaAllocator alloc_;
class ObSingleChildOperatorFake : public ObSingleChildPhyOperator
{
  friend class ObPhyOperatorTest;
protected:
  class ObSingleOpCtx : public ObPhyOperator::ObPhyOperatorCtx
  {
  public:
    ObSingleOpCtx(ObExecContext &ctx)
      : ObPhyOperatorCtx(ctx)
  {
  }
    virtual void destroy() { return ObPhyOperatorCtx::destroy_base(); }
  };
public:
  ObSingleChildOperatorFake() : ObSingleChildPhyOperator(alloc_) {}
  ObPhyOperatorType get_type() const
  {
    return static_cast<ObPhyOperatorType>(0);
  }

  int inner_open(ObExecContext &ctx) const
  {
    int ret = OB_SUCCESS;
    if (OB_SUCCESS != (ret = init_op_ctx(ctx))) {
      _OB_LOG(WARN, "init operator context failed, ret=%d", ret);
    }/* else {
      ret = ObSingleChildPhyOperator::open(ctx);
    }*/
    return ret;
  }

  int inner_close(ObExecContext &ctx) const
  {
    UNUSED(ctx);
    int ret = OB_SUCCESS;
    //ret = ObSingleChildPhyOperator::close(ctx);
    return ret;
  }

  /*
   * int close(ObExecContext &ctx) const
  {
    int ret = OB_SUCCESS;
    ret = ObSingleChildPhyOperator::close(ctx);
    return ret;
  }
   */

protected:
  virtual int inner_get_next_row(ObExecContext &ctx, const ObNewRow *&row) const
  {
    int ret = OB_SUCCESS;
    const ObNewRow *input_row = NULL;
    ObPhyOperator *child_op = NULL;
    ObSingleOpCtx *phy_op_ctx = NULL;

    if (NULL == (child_op = get_child(0))) {
      ret = OB_ERR_UNEXPECTED;
      _OB_LOG(WARN, "get child operator failed");
    } else if (OB_SUCCESS != (ret = child_op->get_next_row(ctx, input_row))) {
      if (OB_ITER_END != ret) {
        _OB_LOG(WARN, "get_next_row failed, ret=%d", ret);
      }
    } else if (NULL == (phy_op_ctx = GET_PHY_OPERATOR_CTX(ObSingleOpCtx, ctx, get_id()))) {
      ret = OB_ERR_UNEXPECTED;
      _OB_LOG(WARN, "get physical operator context failed, ret=%d", ret);
    } else {
      _OB_LOG(DEBUG, "inner_get_next_row, row=%s", to_cstring(*input_row));
      ObNewRow &cur_row = phy_op_ctx->get_cur_row();
      OB_ASSERT(input_row->count_ <= cur_row.count_);
      for (int64_t i = 0; i < input_row->count_; ++i) {
        cur_row.cells_[i] = input_row->cells_[i];
      }
      row = &cur_row;
    }
    return ret;
  }

  virtual int init_op_ctx(ObExecContext &ctx) const
  {
    int ret = OB_SUCCESS;
    ObPhyOperatorCtx *op_ctx = NULL;

    if (OB_SUCCESS != (ret = CREATE_PHY_OPERATOR_CTX(ObSingleOpCtx, ctx, get_id(), get_type(), op_ctx))) {
      _OB_LOG(WARN, "create physical operator context failed, ret=%d", ret);
    } else if (OB_SUCCESS != (ret = op_ctx->create_cur_row(get_column_count(), projector_, projector_size_))) {
      _OB_LOG(WARN, "create current row failed, ret=%d", ret);
    }
    return ret;
  }
};
class ObTableScanFake : public ObNoChildrenPhyOperator
{
protected:
  class ObTableScanFakeCtx : public ObPhyOperatorCtx
  {
  public:
    ObTableScanFakeCtx(ObExecContext &ctx)
        : ObPhyOperatorCtx(ctx)
    {
    }
    virtual void destroy() { return ObPhyOperatorCtx::destroy_base(); }
  };

public:
  ObTableScanFake() : ObNoChildrenPhyOperator(alloc_),
      row_store_(NULL), store_size_(0), cur_index_(0)
  {
  }
  ObPhyOperatorType get_type() const
  {
    return static_cast<ObPhyOperatorType>(0);
  }

  int inner_open(ObExecContext &ctx) const
  {
    int ret = OB_SUCCESS;
    if (OB_SUCCESS != (ret = init_op_ctx(ctx))) {
      _OB_LOG(WARN, "init operator context failed, ret=%d", ret);
    }
    return ret;
  }

  int inner_close(ObExecContext &ctx) const
  {
    UNUSED(ctx);
    return OB_SUCCESS;
  }

  void load_data_row(ObExecContext &ctx, int64_t store_size)
  {
    int64_t cell_val = -1;
    void *ptr = NULL;

    store_size_ = store_size;
    ASSERT_GT(store_size_, 0);
    ptr = ctx.get_allocator().alloc(store_size * sizeof(ObNewRow));
    ASSERT_FALSE(NULL == ptr);
    row_store_ = static_cast<ObNewRow *>(ptr);
    for (int64_t i = 0; i < store_size; ++i) {
      void *cells = ctx.get_allocator().alloc(row_size * sizeof(ObObj));
      ASSERT_FALSE(NULL == cells);
      row_store_[i].cells_ = static_cast<ObObj*>(cells);
      row_store_[i].count_ = row_size;
      for (int64_t j = 0; j < row_size; ++j) {
        row_store_[i].cells_[j].set_int(++cell_val);
      }
    }
  }
protected:
  virtual int inner_get_next_row(ObExecContext &ctx, const ObNewRow *&row) const
  {
    UNUSED(ctx);
    int ret = OB_SUCCESS;
    if (cur_index_ < store_size_) {
      row = &row_store_[cur_index_++];
    } else {
      ret = OB_ITER_END;
    }
    return ret;
  }

  virtual int init_op_ctx(ObExecContext &ctx) const
  {
    int ret = OB_SUCCESS;
    ObPhyOperatorCtx *op_ctx = NULL;
    ret = CREATE_PHY_OPERATOR_CTX(ObTableScanFakeCtx, ctx, get_id(), get_type(), op_ctx);
    UNUSED(op_ctx);
    return ret;
  }
private:
  ObNewRow *row_store_;
  int64_t store_size_;
  mutable int64_t cur_index_;
};

class ObMonitoringDumpTest: public ::testing::Test
{
public:
  ObMonitoringDumpTest() {};
  virtual ~ObMonitoringDumpTest() = default;
  virtual void SetUp() {};
  virtual void TearDown(){};
  void is_equal_content(const char* tmp_file, const char* result_file);
private:
  // disallow copy
  ObMonitoringDumpTest(const ObMonitoringDumpTest &other);
  ObMonitoringDumpTest& operator=(const ObMonitoringDumpTest &other);
private:
  // data members
};

void ObMonitoringDumpTest::is_equal_content(const char* tmp_file, const char* result_file)
{
  std::ifstream if_test(tmp_file);
  if_test.is_open();
  EXPECT_EQ(true, if_test.is_open());
  std::istream_iterator<std::string> it_test(if_test);
  std::ifstream if_expected(result_file);
  if_expected.is_open();
  EXPECT_EQ(true, if_expected.is_open());
  std::istream_iterator<std::string> it_expected(if_expected);
  bool is_equal = std::equal(it_test, std::istream_iterator<std::string>(), it_expected);
  _OB_LOG(INFO, "result file is %s, expect file is %s, is_equal:%d", tmp_file, result_file, is_equal);
  if (is_equal) {
    std::remove(tmp_file);
  } else if (test::clp.record_test_result) {
    fprintf(stdout, "The result files mismatched, you can choose to\n");
    fprintf(stdout, "emacs -q %s %s\n", result_file, tmp_file);
    fprintf(stdout, "diff -u %s %s\n", result_file, tmp_file);
    fprintf(stdout, "mv %s %s\n", tmp_file, result_file);
    std::rename(tmp_file,result_file);
  } else {
    fprintf(stdout, "The result files mismatched, you can choose to\n");
    fprintf(stdout, "diff -u %s %s\n", tmp_file, result_file);
  }
  EXPECT_EQ(true, is_equal);
}

TEST_F(ObMonitoringDumpTest, test_get_next_row)
{
  const char* tmp_file = "./test_monitoring_dump.tmp";
  const char* result_file = "./test_monitoring_dump.result";
  std::ofstream of_result(tmp_file);
  const ObNewRow *row = NULL;
  ObExecContext ctx;
  ObTableScanFake table_scan;
  ObMonitoringDump root(ctx.get_allocator());
  root.set_flags(ObMonitorHint::OB_MONITOR_STAT | ObMonitorHint::OB_MONITOR_TRACING);
  ObPhysicalPlan physical_plan;
  int64_t op_size = 2;
  ObSQLSessionInfo origin_session;
  origin_session.test_init(0,0,0,NULL);

  ASSERT_EQ(OB_SUCCESS, ObPreProcessSysVars::init_sys_var());
  ASSERT_EQ(OB_SUCCESS, create_test_session(ctx));
  auto my_session = ctx.get_my_session();
  ASSERT_FALSE(NULL == my_session);
  ASSERT_EQ(OB_SUCCESS, ctx.init_phy_op(op_size));
  physical_plan.set_main_query(&root);
  ASSERT_EQ(OB_SUCCESS, my_session->set_cur_phy_plan(&physical_plan));
  ASSERT_EQ(OB_SUCCESS, ctx.create_physical_plan_ctx());
  ASSERT_EQ(OB_SUCCESS, my_session->load_default_sys_variable(false, true));

  ObString trace_id("muhangtest");
  ObString trace_id_name("tracefile_identifier");
  ObString trace_id_result;
  ASSERT_EQ(OB_SUCCESS, my_session->update_sys_variable(trace_id_name, trace_id));
  table_scan.load_data_row(ctx, 100);
  table_scan.set_id(0);
  table_scan.set_column_count(row_size);
  table_scan.set_phy_plan(&physical_plan);

  root.set_id(1);
  root.set_column_count(row_size);
  root.set_phy_plan(&physical_plan);
  ASSERT_EQ(OB_SUCCESS, root.set_child(0, table_scan));
  ASSERT_EQ(OB_SUCCESS, root.open(ctx));
  ASSERT_EQ(OB_SUCCESS, my_session->get_sys_variable(SYS_VAR_TRACEFILE_IDENTIFIER, trace_id_result));
  std::string trace_id_str(trace_id_result.ptr(), trace_id_result.length());
  for (int64_t i = 0; i < 100; ++i) {
    ASSERT_EQ(OB_SUCCESS, root.get_next_row(ctx, row));
    printf("row=%s\n", to_cstring(*row));
    of_result << trace_id_str << " " << row->cells_[0].get_int() << std::endl;
  }
  ASSERT_EQ(OB_SUCCESS, root.close(ctx));
  root.reset();
  root.reuse();
  table_scan.reset();
  table_scan.reuse();

  of_result.close();
  std::cout << "diff -u " << tmp_file << " " << result_file << std::endl;
  ObMonitoringDumpTest::is_equal_content(tmp_file, result_file);
}

int main(int argc, char **argv)
{
  init_sql_factories();
  ::testing::InitGoogleTest(&argc,argv);
  OB_LOGGER.set_log_level("INFO");
  return RUN_ALL_TESTS();
}
