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

#define ING_LOG_PREFIX RPC_TEST
#define USING_LOG_PREFIX RPC_TEST
#include <gtest/gtest.h>
#include "sql/engine/expr/ob_expr_operator.h"
#include "lib/number/ob_number_v2.h"
using namespace oceanbase::common;
using namespace oceanbase::obmysql;
using namespace oceanbase::sql;
class ObExprCompareTest
    : public ::testing::Test
{
public:
      ObExprCompareTest()
  {}
  virtual void SetUp()
  {
  }
  virtual void TearDown()
  {
  }

  void time_test(int zs, const char *str, int64_t count, int64_t &total_my) {
    ObObj obj1, obj2, result;
    ObCastCtx ctx;

    obj1.set_int(zs);
    obj1.set_collation_type(CS_TYPE_UTF8MB4_BIN);

    oceanbase::common::number::ObNumber nmb;
    static oceanbase::common::CharArena s_allocator;
    nmb.from(str, s_allocator);
    obj2.set_number(nmb);
    obj2.set_collation_type(CS_TYPE_UTF8MB4_BIN);

    const int64_t start_ts_my = ObTimeUtility::current_time();
    ObCompareCtx cmp_ctx(ObNumberType, CS_TYPE_UTF8MB4_BIN, false, 0);
    for (int i = 0; i < count; i++) {
      ObRelationalExprOperator::compare_cast(result, obj1, obj2, cmp_ctx, ctx, CO_GT);
    }
    const int64_t end_ts_my = ObTimeUtility::current_time();
    total_my += end_ts_my - start_ts_my;
    LOG_INFO("total ", K(total_my));
  }
protected:
};

TEST_F(ObExprCompareTest, calc)
{
  int64_t total_my = 0;
  time_test(10, "10.23", 1000000, total_my);
  time_test(0, "0", 1000000, total_my);
  time_test(0, "0.1234", 1000000, total_my);
  time_test(-1, "0", 1000000, total_my);
  time_test(1000000000, "10000000000.23", 1000000, total_my);
  LOG_INFO("all total 1000000", K(total_my));
}

int main(int argc, char *argv[])
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
