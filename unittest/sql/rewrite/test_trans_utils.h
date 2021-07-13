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

#ifndef TEST_TRANS_UTILS_H_
#define TEST_TRANS_UTILS_H_

#include <fstream>
#include <getopt.h>
#include "sql/rewrite/ob_transformer_impl.h"
#include "sql/resolver/dml/ob_dml_stmt.h"

using namespace std;
using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace test {
class TestTransUtils {
  static const int trans_type_[];

public:
  struct CmdLineParam {
    CmdLineParam() : use_it_(false)
    {
      rules_ = SIMPLIFY | ANYALL | AGGR | ELIMINATE_OJ | VIEW_MERGE | WHERE_SQ_PULL_UP | SET_OP | QUERY_PUSH_DOWN;
    }
    uint64_t rules_;
    bool use_it_;
  };

public:
  TestTransUtils();
  virtual ~TestTransUtils()
  {}

public:
  static bool parse_cmd(int argc, char* argv[], CmdLineParam& param);

private:
  static void print_help();

private:
  DISALLOW_COPY_AND_ASSIGN(TestTransUtils);
};
}  // namespace test
#endif
