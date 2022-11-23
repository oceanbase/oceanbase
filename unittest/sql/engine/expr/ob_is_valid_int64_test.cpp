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
#include "lib/number/ob_number_v2.h"
#include "ob_expr_test_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

class ObGetInt64Test: public ::testing::Test
{
  public:
  ObGetInt64Test();
    virtual ~ObGetInt64Test();
    virtual void SetUp();
    virtual void TearDown();
  private:
    // disallow copy
    ObGetInt64Test(const ObGetInt64Test &other);
    ObGetInt64Test& operator=(const ObGetInt64Test &other);
  protected:
    // data members
};

ObGetInt64Test::ObGetInt64Test()
{
}

ObGetInt64Test::~ObGetInt64Test()
{
}

void ObGetInt64Test::SetUp()
{
}

void ObGetInt64Test::TearDown()
{
}
#define IS_VALID_UINT64(nmb, expected_ret, expected_out) \
{                                                     \
  uint64_t tmp = 0;                                      \
  bool ret = nmb.is_valid_uint64(tmp);                                    \
  EXPECT_TRUE(ret == expected_ret);                     \
  if (ret) \
  EXPECT_TRUE(tmp == expected_out##ULL);                        \
}while(0)

#define IS_VALID_INT64(nmb, expected_ret, expected_out) \
{                                                     \
  int64_t tmp = 0;                                      \
  bool ret = nmb.is_valid_int64(tmp);                                    \
  EXPECT_TRUE(ret == expected_ret);                     \
  if (ret)                                              \
  EXPECT_TRUE(tmp == expected_out##ULL);                        \
}while(0)

#define IS_INT_PARTS_VALID_INT64(nmb, expected_ret, expected_int,expected_out) \
{                                                     \
  int64_t inter = 0;      \
  int64_t decimal = 0;      \
  bool ret = nmb.is_int_parts_valid_int64(inter,decimal);                                    \
  EXPECT_TRUE(ret == expected_ret);                     \
  if (ret)  { \
  EXPECT_TRUE(inter == expected_int);                        \
  EXPECT_TRUE(decimal == expected_out); }                       \
}while(0)

TEST_F(ObGetInt64Test, is_valid_uint64_test)
{
  ObMalloc buf;
  number::ObNumber nmb1;
  nmb1.from("12345678912345678912345678999999933456789887666", buf);
  number::ObNumber nmb2;
  nmb2.from("0", buf);
  number::ObNumber nmb3;
  nmb3.from("111111111.2", buf);
  number::ObNumber nmb4;
  nmb4.from("111111111.123456789123456789", buf);
  number::ObNumber nmb5;
  nmb5.from("12345678", buf);
  number::ObNumber nmb6;
  nmb6.from("-123456789123456789123456789", buf);
  number::ObNumber nmb7;
  nmb7.from("-111111111.2", buf);
  number::ObNumber nmb8;
  nmb8.from("-111111111.123456789123456789", buf);
  number::ObNumber nmb9;
  nmb9.from("9999999999999.9", buf);
  number::ObNumber nmb10;
  nmb10.from("9.0", buf);
  number::ObNumber nmb11;
  nmb11.from("-9999999999999.9", buf);
  number::ObNumber nmb12;
  nmb12.from("18446744073709551615", buf);
  number::ObNumber nmb13;
  nmb13.from("18446744073709551616", buf);
  IS_VALID_UINT64(nmb1,false,0);
  IS_VALID_UINT64(nmb2,true,0);
  IS_VALID_UINT64(nmb3,false,0);
  IS_VALID_UINT64(nmb4,false,0);
  IS_VALID_UINT64(nmb5,true,12345678);
  IS_VALID_UINT64(nmb6,false,0);
  IS_VALID_UINT64(nmb7,false,0);
  IS_VALID_UINT64(nmb8,false,0);
  IS_VALID_UINT64(nmb9,false,0);
  IS_VALID_UINT64(nmb10,true,9);
  IS_VALID_UINT64(nmb11,false,0);
  IS_VALID_UINT64(nmb12,true,18446744073709551615);
  IS_VALID_UINT64(nmb13,false,0);
}

TEST_F(ObGetInt64Test, is_int_valid_int64_test)
{
  ObMalloc buf;
  number::ObNumber nmb1;
  nmb1.from("12345678912345678912345678999999933456789887666", buf);
  number::ObNumber nmb2;
  nmb2.from("0", buf);
  number::ObNumber nmb3;
  nmb3.from("111111111.2", buf);
  number::ObNumber nmb4;
  nmb4.from("111111111.123456789123456789", buf);
  number::ObNumber nmb5;
  nmb5.from("12345678", buf);
  number::ObNumber nmb6;
  nmb6.from("-123456789123456789123456789", buf);
  number::ObNumber nmb7;
  nmb7.from("-111111111.2", buf);
  number::ObNumber nmb8;
  nmb8.from("-111111111.123456789123456789", buf);
  number::ObNumber nmb9;
  nmb9.from("9999999999999.9", buf);
  number::ObNumber nmb10;
  nmb10.from("9.0", buf);
  number::ObNumber nmb11;
  nmb11.from("-9999999999999.9", buf);
  IS_INT_PARTS_VALID_INT64(nmb1,false,0,0);
  IS_INT_PARTS_VALID_INT64(nmb2,true,0,0);
  IS_INT_PARTS_VALID_INT64(nmb3,true,111111111,200000000);
  IS_INT_PARTS_VALID_INT64(nmb4,true,111111111,123456789);
  IS_INT_PARTS_VALID_INT64(nmb5,true,12345678,0);
  IS_INT_PARTS_VALID_INT64(nmb6,false,0,0);
  IS_INT_PARTS_VALID_INT64(nmb7,true,-111111111,200000000);
  IS_INT_PARTS_VALID_INT64(nmb8,true,-111111111,123456789);
  IS_INT_PARTS_VALID_INT64(nmb9,true,9999999999999,900000000);
  IS_INT_PARTS_VALID_INT64(nmb10,true,9,0);
  IS_INT_PARTS_VALID_INT64(nmb11,false,0,0);
}

TEST_F(ObGetInt64Test, is_valid_int64_test)
{
  ObMalloc buf;
  number::ObNumber nmb1;
  nmb1.from("12345678912345678912345678999999933456789887666", buf);
  number::ObNumber nmb2;
  nmb2.from("0", buf);
  number::ObNumber nmb3;
  nmb3.from("111111111.2", buf);
  number::ObNumber nmb4;
  nmb4.from("111111111.123456789123456789", buf);
  number::ObNumber nmb5;
  nmb5.from("12345678", buf);
  number::ObNumber nmb6;
  nmb6.from("-123456789123456789123456789", buf);
  number::ObNumber nmb7;
  nmb7.from("-111111111.2", buf);
  number::ObNumber nmb8;
  nmb8.from("-111111111.123456789123456789", buf);
  number::ObNumber nmb9;
  nmb9.from("9999999999999.9", buf);
  number::ObNumber nmb10;
  nmb10.from("9.0", buf);
  number::ObNumber nmb11;
  nmb11.from("-9999999999999.9", buf);
  number::ObNumber nmb12;
  nmb12.from("9223372036854775807", buf);
  number::ObNumber nmb13;
  nmb13.from("9223372036854775808", buf);
  IS_VALID_INT64(nmb1,false,0);
  IS_VALID_INT64(nmb2,true,0);
  IS_VALID_INT64(nmb3,false,0);
  IS_VALID_INT64(nmb4,false,0);
  IS_VALID_INT64(nmb5,true,12345678);
  IS_VALID_INT64(nmb6,false,0);
  IS_VALID_INT64(nmb7,false,0);
  IS_VALID_INT64(nmb8,false,0);
  IS_VALID_INT64(nmb9,false,0);
  IS_VALID_INT64(nmb10,true,9);
  IS_VALID_INT64(nmb11,false,0);
  IS_VALID_INT64(nmb12,true,9223372036854775807);
  IS_VALID_INT64(nmb13,false,0);
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
