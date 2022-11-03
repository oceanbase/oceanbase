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
#include "lib/stat/ob_diagnose_info.h"

using namespace oceanbase;
using namespace common;

#define OK(value) ASSERT_EQ(OB_SUCCESS, (value))

class ObDiagnoseInfoTest: public ::testing::Test
{
  public:
    ObDiagnoseInfoTest();
    virtual ~ObDiagnoseInfoTest();
    virtual void SetUp();
    virtual void TearDown();
  private:
    // disallow copy
    ObDiagnoseInfoTest(const ObDiagnoseInfoTest &other);
    ObDiagnoseInfoTest& operator=(const ObDiagnoseInfoTest &other);
  protected:
    // data members
};

ObDiagnoseInfoTest::ObDiagnoseInfoTest()
{
}

ObDiagnoseInfoTest::~ObDiagnoseInfoTest()
{
}

void ObDiagnoseInfoTest::SetUp()
{
}

void ObDiagnoseInfoTest::TearDown()
{
}

TEST_F(ObDiagnoseInfoTest, basic_test)
{
  // Not supported now

 // EVENT_INC(RPC_PACKET_IN);
 // ASSERT_EQ(1, EVENT_GET(RPC_PACKET_IN));
 // EVENT_SET(RPC_PACKET_IN, 2);
 // ASSERT_EQ(2, EVENT_GET(RPC_PACKET_IN));
 // EVENT_ADD(RPC_PACKET_IN, 3);
 // ASSERT_EQ(5, EVENT_GET(RPC_PACKET_IN));
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
