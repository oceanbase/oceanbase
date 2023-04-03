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

#include "lib/trace/ob_trace_event.h"
#include <gtest/gtest.h>
#include "lib/utility/ob_test_util.h"
#include "lib/oblog/ob_log.h"
#include "lib/utility/utility.h"
#include "lib/coro/testing.h"

using namespace oceanbase::common;
class TestTraceEvent: public ::testing::Test
{
public:
  TestTraceEvent();
  virtual ~TestTraceEvent();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestTraceEvent);
protected:
  // function members
protected:
  // data members
};

TestTraceEvent::TestTraceEvent()
{
}

TestTraceEvent::~TestTraceEvent()
{
}

void TestTraceEvent::SetUp()
{
}

void TestTraceEvent::TearDown()
{
}

struct A
{
  int64_t d1_;
  int64_t d2_;
  TO_YSON_KV(OB_ID(read_only), d1_,
             OB_ID(access_mode), d2_);
};

TEST_F(TestTraceEvent, basic_test)
{
  ObTraceEventRecorder m(false, ObLatchIds::TRACE_RECORDER_LOCK);
  A a;
  A b;
  a.d1_ = 0;
  a.d2_ = 0;
  b.d1_ = 100;
  b.d2_ = 100;
  REC_TRACE(m, start_trans);
  ASSERT_EQ(1, m.count());
  REC_TRACE_EXT(m, start_trans, OB_ID(arg1), a, OB_ID(arg2), b);
  ASSERT_EQ(2, m.count());
  OB_LOG(INFO, "test print trace", K(m));
}

TEST_F(TestTraceEvent, NG_trace)
{
  for (int i = 0; i < 5; ++i) {
    THE_TRACE->reset();
    A a;
    A b;
    a.d1_ = 0;
    a.d2_ = 0;
    b.d1_ = 100;
    b.d2_ = 100;
    NG_TRACE(affected_rows);
    NG_TRACE_EXT(id, OB_ID(arg1), a, OB_ID(arg2), b);
    OB_LOG(INFO, "test print trace", "", *THE_TRACE);
  } // end for
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
public:
  int64_t val1_;
  int32_t val2_;
  bool val3_;
  uint32_t val4_;
  uint64_t val5_;
};

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
};

TEST_F(TestTraceEvent, overflow)
{
  THE_TRACE->reset();
  ClassB b;
  b.arr_[0] = 1;
  b.arr_[1] = 2;
  b.arr_[2] = 3;
  b.val1_ = ObString::make_string("hello YSON");
  b.obj1_.val1_ = 100;
  b.obj1_.val2_ = -9876;
  b.obj1_.val3_ = false;
  for (int i = 0; i < 5000; ++i) {
    NG_TRACE_EXT(id, OB_ID(arg1), b, OB_ID(arg2), b);
  } // end for
  OB_LOG(INFO, "test print trace", "", *THE_TRACE);
}

static ObTraceEventRecorder MY_RECORDER(true, ObLatchIds::TRACE_RECORDER_LOCK);
void* concurrent_add_event(void *)
{
  ObString value = ObString::make_string("hello concurrent programming");
  for (int i = 0; i < 200; ++i) {
    for (int j = 0; j < 300; ++j) {
      REC_TRACE_EXT(MY_RECORDER, id1, OB_ID(arg1), value, OB_ID(arg2), value);
    }
    MY_RECORDER.reset();
  }
  return NULL;
}
TEST_F(TestTraceEvent, multi_thread)
{
  static const int N = 4;
  cotesting::FlexPool([]{
    concurrent_add_event(nullptr);
  }, N).start();
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc,argv);
  OB_LOGGER.set_log_level("INFO");
  return RUN_ALL_TESTS();
}
