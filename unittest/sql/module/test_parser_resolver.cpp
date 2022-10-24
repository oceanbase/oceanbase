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

//using namespace oceanbase;
//using namespace common;

#define OK(value) ASSERT_EQ(OB_SUCCESS, (value))

class ObParserResolver: public ::testing::Test
{
  public:
    ObParserResolver();
    virtual ~ObParserResolver();
    virtual void SetUp();
    virtual void TearDown();
  private:
    // disallow copy
    ObParserResolver(const ObParserResolver &other);
    ObParserResolver& operator=(const ObParserResolver &other);
  protected:
    // data members
};

ObParserResolver::ObParserResolver()
{
}

ObParserResolver::~ObParserResolver()
{
}

void ObParserResolver::SetUp()
{
}

void ObParserResolver::TearDown()
{
}

TEST_F(ObParserResolver, basic_test)
{
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

