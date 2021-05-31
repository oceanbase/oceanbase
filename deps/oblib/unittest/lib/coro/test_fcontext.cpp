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
#include <iostream>
#include "lib/oblog/ob_log.h"
#include "lib/coro/context/fcontext.hpp"

using namespace std;
using namespace oceanbase::lib;
using namespace oceanbase::common;
using namespace boost::context::detail;

static void record(const char* str, const int i, vector<int>& seq)
{
  cout << str << endl;
  seq.push_back(i);
}

static void Func(transfer_t from)
{
  UNUSED(from);
  auto& seq = *reinterpret_cast<vector<int>*>(from.data);
  record("context begin", 2, seq);
  jump_fcontext(from.fctx, nullptr);
  record("context end", 4, seq);
  jump_fcontext(from.fctx, nullptr);
}

TEST(TestFcontext, Main)
{
  vector<int> seq;
  const vector<int> cmp_seq = {1, 2, 3, 4, 5};
  record("main context begin", 1, seq);
  static constexpr int stack_size = 1024 * 1024 * 2;
  char stack[stack_size];
  auto ctx = make_fcontext(stack + stack_size, stack_size, &Func);
  auto transfer = jump_fcontext(ctx, &seq);
  record("main context continue", 3, seq);
  jump_fcontext(transfer.fctx, &seq);
  record("main context end", 5, seq);
  ASSERT_TRUE(seq.size() == cmp_seq.size() && std::equal(seq.begin(), seq.end(), cmp_seq.begin()));
}

int main(int argc, char* argv[])
{
  OB_LOGGER.set_log_level(3);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
