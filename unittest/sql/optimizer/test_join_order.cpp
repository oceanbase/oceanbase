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

#define USING_LOG_PREFIX SQL_OPT
#include "sql/optimizer/ob_join_order.h"
#include "observer/ob_req_time_service.h"
#include "test_optimizer_utils.h"

namespace oceanbase
{
namespace sql
{
class Path;
}
}
namespace test
{
class TestJoinOrder: public TestOptimizerUtils
{
public:
  TestJoinOrder(){}
  ~TestJoinOrder(){}
};

TEST_F(TestJoinOrder, ob_join_order_select)
{
  const char* test_file = "./test_join_order_case.sql";
  const char* result_file = "./test_join_order_case.result";
  const char* tmp_file = "./test_join_order_case.tmp";
  run_test(test_file, result_file, tmp_file);
}

class TestPath : public oceanbase::sql::Path
{
  virtual void get_name_internal(char *buf, const int64_t buf_len, int64_t &pos) const
  { UNUSED(buf), UNUSED(buf_len), UNUSED(pos);}
  virtual int estimate_cost() override { return OB_SUCCESS; }
  virtual int re_estimate_cost(double need_row_count, double &cost) { UNUSED(need_row_count); cost = 0; return OB_SUCCESS; }
  virtual int contain_fake_cte(bool &contains) const { contains = false; return OB_SUCCESS; }

};

TEST_F(TestJoinOrder, ob_join_order_src)
{
  ObArenaAllocator allocator(ObModIds::OB_BUFFER);
  JoinInfo out_join(LEFT_OUTER_JOIN);
  char buf[256] = {0};
  AccessPath a_path(123, 123, 456, NULL, oceanbase::sql::NULLS_FIRST_ASC);
  a_path.op_cost_ = 100;
  a_path.cost_ = 100;
  int64_t pos = 0;
  int ret = a_path.get_name(buf, 256, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(a_path.get_cost(), 100);
  memset(buf, '\0', 256);
  a_path.get_name_internal(buf, 256, pos);
  JoinPath join_path;
  memset(buf, '\0', 256);
  join_path.get_name_internal(buf, 256, pos);
  SubQueryPath sp_path;
  memset(buf, '\0', 256);
  sp_path.get_name_internal(buf, 256, pos);
  ObGlobalHint global_hint;
  ObAddr addr;
  ObRawExprFactory expr_factory(allocator);
  ObOptimizerContext ctx(NULL, NULL, NULL, NULL, NULL,
                         allocator, NULL, NULL, addr, NULL,
                         global_hint, expr_factory, NULL, false, NULL);
  ObSelectLogPlan plan(ctx, NULL);
  ObJoinOrder join_order(NULL, &plan, sql::INVALID);

  TestPath test_path;
  LOG_INFO("test to string func", K(test_path), K(join_order));
  pos = join_order.get_name(buf, 256);
  //case: test reset()
  ret = join_order.interesting_paths_.push_back(&sp_path);
  ASSERT_EQ(OB_SUCCESS, ret);
  join_order.join_info_ = &out_join;
  ObSysFunRawExpr raw_expr;
  ret = join_order.restrict_info_set_.push_back(&raw_expr);
  ASSERT_EQ(OB_SUCCESS, ret);
}

}//end of namespace test
int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_join_order.log", true);
  observer::ObReqTimeGuard req_timeinfo_guard;
  init_sql_factories();
  ::testing::InitGoogleTest(&argc,argv);
  if(argc >= 2)
  {
    if (strcmp("DEBUG", argv[1]) == 0
        || strcmp("INFO", argv[1]) == 0
        || strcmp("WARN", argv[1]) == 0)
    OB_LOGGER.set_log_level(argv[1]);
  }
  test::parse_cmd_line_param(argc, argv, test::clp);
  return RUN_ALL_TESTS();
}
