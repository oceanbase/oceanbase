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

#ifndef OCEANBASE_UNITTEST_PL_TEST_PL_UTILS_H_
#define OCEANBASE_UNITTEST_PL_TEST_PL_UTILS_H_

#include <fstream>
#include <dirent.h>
#include <getopt.h>
#include <gtest/gtest.h>
#define private public
#include "share/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/oblog/ob_log_module.h"
#include "../sql/test_sql_utils.h"
#include "pl/ob_pl.h"
#include "pl/ob_pl_stmt.h"

namespace test
{

class TestPLUtils : public TestSqlUtils
{
public:
  TestPLUtils() : TestSqlUtils(), case_id_(0) {}
  virtual ~TestPLUtils(){}
  virtual void init();
  virtual void destroy();

// function members
  void resolve_test(const char* test_file, const char* result_file, const char* tmp_file);
  void compile_test(const char* test_file, const char* result_file, const char* tmp_file);
  void resolve_pl(const char* pl_str, std::ofstream &of_result);
  void compile_pl(const char *query_str, std::ofstream &of_result);
  int do_resolve(const char* pl_str, sql::ObRawExprFactory &expr_factory, oceanbase::pl::ObPLFunctionAST &func);
  int do_compile(const char* pl_str, oceanbase::pl::ObPLFunction &func);

public:
  int64_t case_id_;

private:
  DISALLOW_COPY_AND_ASSIGN(TestPLUtils);
};

}

#endif /* OCEANBASE_UNITTEST_PL_TEST_PL_UTILS_H_ */
