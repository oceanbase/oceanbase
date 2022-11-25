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
#include "lib/allocator/ob_malloc.h"
#include "common/ob_role.h"
#include "lib/container/ob_se_array.h"
#include "lib/container/ob_array_serialization.h"
#include "lib/utility/ob_serialization_helper.h"

using namespace oceanbase;
using namespace common;

#define OK(value) ASSERT_EQ(OB_SUCCESS, (value))

class ObSerialzationHelper: public ::testing::Test
{
  public:
    ObSerialzationHelper();
    virtual ~ObSerialzationHelper();
    virtual void SetUp();
    virtual void TearDown();
  private:
    // disallow copy
    ObSerialzationHelper(const ObSerialzationHelper &other);
    ObSerialzationHelper& operator=(const ObSerialzationHelper &other);
  protected:
    // data members
};

ObSerialzationHelper::ObSerialzationHelper()
{
}

ObSerialzationHelper::~ObSerialzationHelper()
{
}

void ObSerialzationHelper::SetUp()
{
}

void ObSerialzationHelper::TearDown()
{
}

enum TestEnum1
{
  TestEnum1Value = 1024,
};

enum TestEnum2
{
  TestEnum2Value = 1L << 33,
};

TEST_F(ObSerialzationHelper, enum_type)
{
  char buf[1024];
  ObSArray<TestEnum1> array;

  ASSERT_TRUE(4 == sizeof(TestEnum1Value));

  ASSERT_EQ(OB_SUCCESS, array.push_back(TestEnum1Value));
  int64_t pos = 0;
  ASSERT_LT(0, array.get_serialize_size());
  ASSERT_EQ(OB_SUCCESS, array.serialize(buf, 1024, pos));
  pos = 0;
  array.deserialize(buf, 1024, pos);
  ASSERT_EQ(1, array.count());
  ASSERT_EQ(TestEnum1Value, array.at(0));

  ASSERT_TRUE(8 == sizeof(TestEnum2Value));
  ObSArray<TestEnum2> array2;
  pos = 0;
  // only 4 byte enum supported right now, the following code will fail to compile.
  // array2.get_serialize_size();
  // array2.serialize(buf, 1024, pos);
  // array2.deserialize(buf, 1024, pos);
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
