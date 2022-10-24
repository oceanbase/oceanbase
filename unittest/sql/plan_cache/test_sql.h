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

#ifndef OCEANBASE_TEST_SQL_H_
#define OCEANBASE_TEST_SQL_H_ 1

#undef protected
#undef private
#include <gtest/gtest.h>
#include "sql/optimizer/test_optimizer_utils.h"
#define protected public
#define private public
#include "lib/container/ob_iarray.h"
#include "lib/atomic/ob_atomic.h"

using namespace oceanbase;

namespace test
{
struct TestSqlCtx
{
  ObStmtFactory *stmt_factory_;
  ObRawExprFactory *expr_factory_;
  ObLogPlanFactory *log_plan_factory_;
  ObIAllocator *allocator_;
};

class TestSQL : public TestOptimizerUtils
{
public:
  TestSQL(const ObString &schema_file_name);
  virtual ~TestSQL();
  void TestBody(){}
  // function members
  int do_parse(ObIAllocator &allocator, const char* query_str, ParseResult &parse_result);
  int do_resolve(TestSqlCtx &test_sql_ctx,
                 ParseResult &parse_result, ObStmt *&stmt, ParamStore *params);
  int generate_logical_plan(TestSqlCtx &test_sql_ctx,
                            ParamStore *params,
                            ObStmt *stmt,
                            ObLogPlan *&logical_plan);
  int generate_physical_plan(ObLogPlan *logical_plan, ObPhysicalPlan *&physical_plan);

  int64_t get_case_id() { return case_id_; }
  int64_t next_case_id() { return ATOMIC_AAF((uint64_t*)&case_id_, 1); }
  ObAddr &get_addr() { return addr_; }
  ObSQLSessionInfo &get_session_info() { return session_info_; }
  uint64_t get_database_id() { return combine_id( sys_tenant_id_, next_user_database_id_);}
  void set_merged_version(int64_t version) { merged_version_ = version; }
  int64_t get_merged_version() { return merged_version_; }
protected:
  ObAddr addr_;  //local addr
  int64_t merged_version_;
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestSQL);
};
}//
#endif
