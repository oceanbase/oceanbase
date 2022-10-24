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

#include "lib/profile/ob_perf_event.h"
#include <gtest/gtest.h>
#include "lib/utility/ob_test_util.h"

using namespace oceanbase::common;
class TestPerfEvent: public ::testing::Test
{
public:
  TestPerfEvent();
  virtual ~TestPerfEvent();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestPerfEvent);
protected:
  // function members
protected:
  // data members
};

TestPerfEvent::TestPerfEvent()
{
}

TestPerfEvent::~TestPerfEvent()
{
}

void TestPerfEvent::SetUp()
{
}

void TestPerfEvent::TearDown()
{
}

TEST_F(TestPerfEvent, basic_test)
{
  int ret = OB_SUCCESS;
  ObPerfConfig::sampling_period_ = 1;
  ENABLE_PERF_EVENT();
  for (int i = 0; i < 3; ++i) {
    PERF_RESET_RECORDER();
    PERF_SET_CAT_ID(100+i);
    OB_PERF_EVENT_BEGIN(process);
    OB_PERF_EVENT(key);
    OB_PERF_EVENT(value);
    OB_PERF_EVENT_END(process);
    OB_LOG(INFO, "write perf event", K(THE_PERF));
    PERF_GATHER_DATA();
  }
  DISABLE_PERF_EVENT();
  // read and print
  ObPerfReader reader;
  ASSERT_EQ(OB_SUCCESS, reader.open(ObPerfConfig::data_filename_));

  ObPerfEventRecorder rec;
  int64_t count = 0;
  while(OB_SUCCESS == ret && OB_SUCCESS == (ret = reader.read(rec))) {
    OB_LOG(INFO, "read perf event", K(rec));
    count++;
  }
  if (OB_SUCCESS != ret) {
    ASSERT_EQ(OB_ITER_END, ret);
  }
  ASSERT_EQ(3, count);
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
