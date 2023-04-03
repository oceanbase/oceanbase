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

#include "lib/json/ob_yson.h"
#include "lib/utility/ob_print_utils.h"
#include <gtest/gtest.h>
#include "lib/utility/ob_test_util.h"
#include "lib/utility/utility.h"
using namespace oceanbase::common;
using namespace oceanbase::yson;

class TestYson: public ::testing::Test
{
public:
  TestYson();
  virtual ~TestYson();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestYson);
protected:
  // function members
protected:
  // data members
};

TestYson::TestYson()
{
}

TestYson::~TestYson()
{
}

void TestYson::SetUp()
{
}

void TestYson::TearDown()
{
}

struct ClassWithToYson
{
  TO_YSON_KV(0, val1_,
             1, val2_,
             2, val3_,
             3, val4_,
             4, val5_);
  TO_STRING_KV("0", val1_,
               "1", val2_,
               "2", val3_,
               "3", val4_,
               "4", val5_);
  OB_UNIS_VERSION(1);
public:
  int64_t val1_;
  int32_t val2_;
  bool val3_;
  uint32_t val4_;
  uint64_t val5_;
};
OB_SERIALIZE_MEMBER(ClassWithToYson, val1_, val2_, val3_, val4_, val5_);

TEST_F(TestYson, test_to_yson_kv)
{
  char buf[1024];
  int64_t pos = 0;
  ClassWithToYson a;
  a.val1_ = 100;
  a.val2_ = -9876;
  a.val3_ = false;
  a.val4_ = 100;
  a.val5_ = 9876;
  ASSERT_EQ(OB_SUCCESS, a.to_yson(buf, 40, pos));
  ASSERT_EQ(pos, 40);
  // to text
  char buf2[1024];
  int64_t pos2 = 0;
  ASSERT_EQ(OB_SUCCESS, oceanbase::yson::databuff_print_elements(buf2, 1024, pos2, buf, pos));
  ObString text;
  text.assign_ptr(buf2, static_cast<int32_t>(pos2));
  OB_LOG(INFO, "YSON to text", "text", text);
}

struct ClassB
{
  int32_t arr_[3];
  ClassWithToYson obj1_;
  ObString val1_;
  TO_YSON_KV(2, ObArrayWrap<int32_t>(arr_, 3),
             0, obj1_,
             11, val1_);
  TO_STRING_KV("2", ObArrayWrap<int32_t>(arr_, 3),
               "0", obj1_,
               "11", val1_);
  OB_UNIS_VERSION(1);
};
OB_SERIALIZE_MEMBER(ClassB, arr_, obj1_, val1_);

TEST_F(TestYson, test_object)
{
  char buf[1024];
  int64_t pos = 0;
  ClassB b;
  b.arr_[0] = 1;
  b.arr_[1] = 2;
  b.arr_[2] = 3;
  b.val1_ = ObString::make_string("hello YSON");
  b.obj1_.val1_ = 100;
  b.obj1_.val2_ = -9876;
  b.obj1_.val3_ = false;

  ASSERT_EQ(OB_SUCCESS, b.to_yson(buf, 92, pos));
  ASSERT_EQ(pos, 92);
  // to text
  OB_LOG(INFO, "YSON to text", "text", ObYsonToString(buf, pos));
}

TEST_F(TestYson, performance_vs_json)
{
  char buf[1024];
  int64_t pos = 0;
  ClassB b;
  b.arr_[0] = 1;
  b.arr_[1] = 2;
  b.arr_[2] = 3;
  b.val1_ = ObString::make_string("hello YSON");
  b.obj1_.val1_ = 100;
  b.obj1_.val2_ = -9876;
  b.obj1_.val3_ = false;
  int64_t begin_us = ObTimeUtility::current_time();
  static const int64_t ROUND = 10000;
  for (int64_t i = 0; i < ROUND; ++i) {
    pos = 0;
    b.to_yson(buf, 1024, pos);
    if (i == 0) {
      OB_LOG(INFO, "YSON size", K(pos));
    }
  }
  int64_t end_us = ObTimeUtility::current_time();
  int64_t elapsed_us = end_us - begin_us;
  OB_LOG(INFO, "YSON time", K(ROUND), K(elapsed_us), "avg", double(elapsed_us)/double(ROUND));

  // JSON
  begin_us = ObTimeUtility::current_time();
  for (int64_t i = 0; i < ROUND; ++i) {
    pos = b.to_string(buf, 1024);
    if (i == 0) {
      OB_LOG(INFO, "YSON size", K(pos));
    }
  }
  end_us = ObTimeUtility::current_time();
  elapsed_us = end_us - begin_us;
  OB_LOG(INFO, "JSON time", K(ROUND), K(elapsed_us), "avg", double(elapsed_us)/double(ROUND));
  // serialize
  begin_us = ObTimeUtility::current_time();
  for (int64_t i = 0; i < ROUND; ++i) {
    pos = 0;
    b.serialize(buf, 1024, pos);
    if (i == 0) {
      OB_LOG(INFO, "serialize size", K(pos));
    }
  }
  end_us = ObTimeUtility::current_time();
  elapsed_us = end_us - begin_us;
  OB_LOG(INFO, "serialize() time", K(ROUND), K(elapsed_us), "avg", double(elapsed_us)/double(ROUND));

}

struct X
{
  int64_t val1_;
  int32_t val2_;
  bool val3_;
  uint32_t val4_;
  uint64_t val5_;
  TO_STRING_AND_YSON(OB_ID(tag1), val1_,
                     OB_ID(tag2), val2_,
                     OB_ID(tag3), val3_,
                     OB_ID(tag4), val4_,
                     OB_ID(tag4), val5_);
};

TEST_F(TestYson, to_string_and_yson)
{
  X a;
  a.val1_ = 100;
  a.val2_ = -9876;
  a.val3_ = false;
  a.val4_ = 100;
  a.val5_ = 9876;
  char buf[1024];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, a.to_yson(buf, 40, pos));
  ASSERT_EQ(pos, 40);
  OB_LOG(INFO, "to_yson and to_string", "yson text", ObYsonToString(buf, pos), K(a));
}

TEST_F(TestYson, test_overflow)
{
  char buf[150];
  int64_t pos = 0;
  ClassB b;
  b.arr_[0] = 1;
  b.arr_[1] = 2;
  b.arr_[2] = 3;
  b.val1_ = ObString::make_string("hello YSON");
  b.obj1_.val1_ = 100;
  b.obj1_.val2_ = -9876;
  b.obj1_.val3_ = false;
  ASSERT_EQ(OB_SUCCESS, b.to_yson(buf, 150, pos));
  ASSERT_EQ(OB_BUF_NOT_ENOUGH, b.to_yson(buf, 150, pos));
  ASSERT_EQ(pos, 120);
  // to text
  OB_LOG(INFO, "YSON to text", "text", ObYsonToString(buf, pos));
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc,argv);
  OB_LOGGER.set_log_level("INFO");
  return RUN_ALL_TESTS();
}
