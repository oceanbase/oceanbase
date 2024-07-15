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
#include <cstdint>
#include "lib/container/ob_se_array.h"

namespace oceanbase
{
using namespace common;

namespace unittest
{

class TestObSEArray : public ::testing::Test
{
public :
  virtual void SetUp() {}
  virtual void TearDown() {}
};

static int64_t g_count = 0;

class TestObj
{
public:
  TestObj() : t_(0) { g_count++; }
  explicit TestObj(const int v) : t_(v) { g_count++; }
  TestObj(const TestObj &obj) { t_ = obj.t_; g_count++; }
  ~TestObj() { g_count--; }
  int get_value() const { return t_; }
  TO_STRING_KV(K_(t));
private:
  int t_;
};

TEST(TestObSEArray, pop_back)
{
  static const int magic_num = 2651356;
  static const int64_t LOCAL_ARRAY_SIZE = 4;
  static const int64_t MAX_OBJ_COUNT = LOCAL_ARRAY_SIZE + 16;
  ObSEArray<TestObj, LOCAL_ARRAY_SIZE> a1;
  ObSEArray<int, LOCAL_ARRAY_SIZE> a2;

  for (int64_t i = 0; i < MAX_OBJ_COUNT; i++) {
    ASSERT_EQ(OB_SUCCESS, a1.push_back(TestObj(magic_num)));
    ASSERT_EQ(OB_SUCCESS, a2.push_back(magic_num));
  }

  ASSERT_EQ(MAX_OBJ_COUNT, g_count);

  for (int64_t i = 0; i < MAX_OBJ_COUNT / 2; i++) {
    a1.pop_back();
    a2.pop_back();
  }

  ASSERT_EQ(MAX_OBJ_COUNT - MAX_OBJ_COUNT / 2, g_count);

  {
    TestObj tmp_obj;
    ASSERT_EQ(OB_SUCCESS, a1.pop_back(tmp_obj));
    ASSERT_EQ(magic_num, tmp_obj.get_value());
  }

  int tmp = 0;
  ASSERT_EQ(OB_SUCCESS, a2.pop_back(tmp));
  ASSERT_EQ(magic_num, tmp);

  ASSERT_EQ(MAX_OBJ_COUNT - MAX_OBJ_COUNT / 2 - 1, g_count);

  a1.destroy();
  a2.destroy();

  ASSERT_EQ(0, g_count);
}

} // end of unittest
} // end of oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_ob_se_array.log", true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
