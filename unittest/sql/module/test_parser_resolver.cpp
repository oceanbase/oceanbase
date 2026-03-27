/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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

