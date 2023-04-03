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
#include "share/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/utility/ob_print_utils.h"
#include "share/object/ob_obj_cast.h"
#include "common/object/ob_object.h"
#include "sql/engine/expr/ob_expr_operator.h"
#include <iostream>
#include <stdio.h>
#include "rpc/obmysql/ob_mysql_util.h"
using namespace oceanbase::common;
using namespace oceanbase;
using namespace std;
int test_len(const ObString &str, int32_t expect_bit_len)
{
  int ret = OB_SUCCESS;
  int32_t bit_len = 0;
  EXPECT_EQ(OB_SUCCESS, get_bit_len(str, bit_len));
  EXPECT_EQ(expect_bit_len, bit_len);
  return ret;
}

TEST(tst_bit_type, get_bit_len)
{
  cout<<"*****************case 0**********************"<<endl;
  test_len(ObString(0, ""), 1);
  cout<<"*****************case 1**********************"<<endl;
  char test1[1] = {1};
  test_len(ObString(1, test1), 1);
  cout<<"*****************case 2**********************"<<endl;
  char test2[1] = {0x11};
  test_len(ObString(1, test2), 5);
  cout<<"*****************case 3**********************"<<endl;
  char test3[1] = {(char)0xF1};
  test_len(ObString(1, test3), 8);
  cout<<"*****************case 4**********************"<<endl;
  char test4[2] = {(char)0x1, (char)0xF1};
  test_len(ObString(2, test4), 9);
  cout<<"*****************case 5**********************"<<endl;
  char test5[2] = {(char)0xFF, (char)0xF1};
  test_len(ObString(2, test5), 16);
  cout<<"*****************case 6**********************"<<endl;
  char test6[8] = {(char)0xFF, (char)0xF1, (char)0xF1, (char)0xF1, (char)0xF1, (char)0xF1, (char)0xF1, (char)0xF1};
  test_len(ObString(8, test6), 64);
  cout<<"*****************case 7**********************"<<endl;
  char test7[8] = {(char)0x1F, (char)0xF1, (char)0xF1, (char)0xF1, (char)0xF1, (char)0xF1, (char)0xF1, (char)0xF1};
  test_len(ObString(8, test7), 61);
  cout<<"*****************case 8**********************"<<endl;
  char test8[8] = {(char)0x30, (char)0xF1, (char)0xF1, (char)0xF1, (char)0xF1, (char)0xF1, (char)0xF1, (char)0xF1};
  test_len(ObString(8, test8), 62);
  cout<<"*****************case 9**********************"<<endl;
  char test9[8] = {(char)0x40, (char)0xF1, (char)0xF1, (char)0xF1, (char)0xF1, (char)0xF1, (char)0xF1, (char)0xF1};
  test_len(ObString(8, test9), 63);
  cout<<"*****************case 10**********************"<<endl;
  char test10[8] = {(char)0x80, (char)0xF1, (char)0xF1, (char)0xF1, (char)0xF1, (char)0xF1, (char)0xF1, (char)0xF1};
  test_len(ObString(8, test10), 64);

}

TEST(tst_bit_type, get_bit_len2)
{
  int32_t bit_len = 0;
  EXPECT_EQ(OB_SUCCESS, get_bit_len(0,  bit_len));
  EXPECT_EQ(1, bit_len);

  EXPECT_EQ(OB_SUCCESS, get_bit_len(0x1,  bit_len));
  EXPECT_EQ(1, bit_len);

  EXPECT_EQ(OB_SUCCESS, get_bit_len(0x15,  bit_len));
  EXPECT_EQ(5, bit_len);

  EXPECT_EQ(OB_SUCCESS, get_bit_len(0x10,  bit_len));
  EXPECT_EQ(5, bit_len);

  EXPECT_EQ(OB_SUCCESS, get_bit_len(0x80,  bit_len));
  EXPECT_EQ(8, bit_len);

  EXPECT_EQ(OB_SUCCESS, get_bit_len(0x1000,  bit_len));
  EXPECT_EQ(13, bit_len);

  EXPECT_EQ(OB_SUCCESS, get_bit_len(0xFFFF,  bit_len));
  EXPECT_EQ(16, bit_len);

  EXPECT_EQ(OB_SUCCESS, get_bit_len(0xFFFFFFFFFFFFFFFF,  bit_len));
  EXPECT_EQ(64, bit_len);

  EXPECT_EQ(OB_SUCCESS, get_bit_len(0x8FFFFFFFFFFFFFFF,  bit_len));
  EXPECT_EQ(64, bit_len);

  EXPECT_EQ(OB_SUCCESS, get_bit_len(0x1FFFFFFFFFFFFFFF,  bit_len));
  EXPECT_EQ(61, bit_len);

  EXPECT_EQ(OB_SUCCESS, get_bit_len(0x1000000000000000,  bit_len));
  EXPECT_EQ(61, bit_len);

  EXPECT_EQ(OB_SUCCESS, get_bit_len(0x4000000000000000,  bit_len));
  EXPECT_EQ(63, bit_len);

  EXPECT_EQ(OB_SUCCESS, get_bit_len(0x4000000000000001,  bit_len));
  EXPECT_EQ(63, bit_len);

  EXPECT_EQ(OB_SUCCESS, get_bit_len(0x4000000100000000,  bit_len));
  EXPECT_EQ(63, bit_len);
}
#define TEST_CAST(src_func_type, src_val, dest_func_type, dest_type, dest_val) \
  do {                                                                  \
    const ObObj *res_obj = nullptr;					\
    ObObj dst_obj;                                                      \
    ObObj src_obj;                                                      \
    src_obj.set_##src_func_type(src_val);                               \
    dst_obj.set_##dest_func_type(dest_val);                             \
    EXPR_CAST_OBJ_V2(dest_type, src_obj, res_obj);                      \
    EXPECT_EQ(OB_SUCCESS, ret);                                         \
    EXPECT_EQ(dest_type, res_obj->get_type());                          \
    OB_LOG(INFO, "result", K(*res_obj), K(dst_obj));                              \
    if (res_obj->get_type() != ObNullType) {                           \
      EXPECT_TRUE(ObObjCmpFuncs::compare_oper_nullsafe(dst_obj, *res_obj, CS_TYPE_UTF8MB4_BIN, CO_EQ)); \
    }                                                                   \
  } while(0)


TEST(tst_bit_type, bit_to_int)
{
  int ret = OB_SUCCESS;
  ObCastCtx cast_ctx;

  cast_ctx.cast_mode_ = CM_WARN_ON_FAIL;
  TEST_CAST(bit, 0x1, int, ObIntType, 0x1);
  TEST_CAST(bit, 0x2, int, ObIntType, 0x2);
  TEST_CAST(bit, 0xFF, int, ObIntType, 0xFF);
  TEST_CAST(bit, 0xFFFF, int, ObIntType, 0xFFFF);
  TEST_CAST(bit, 0xFFFFFFFFFFFFFFFF, int, ObIntType, -1);

  TEST_CAST(bit, 0x1, tinyint, ObTinyIntType, 0x1);
  TEST_CAST(bit, 0x2, tinyint, ObTinyIntType, 0x2);
  TEST_CAST(bit, 0x80, tinyint, ObTinyIntType, 127);//same as mysql???
  TEST_CAST(bit, 0xFFFFFFFFFFFFFFFF, tinyint, ObTinyIntType, -1);
}

TEST(tst_bit_type, bit_to_uint)
{
  int ret = OB_SUCCESS;
  ObCastCtx cast_ctx;
  cast_ctx.cast_mode_ = CM_WARN_ON_FAIL;
  TEST_CAST(bit, 0x1, uint64, ObUInt64Type, 0x1);
  TEST_CAST(bit, 0x2, uint64, ObUInt64Type, 0x2);
  TEST_CAST(bit, 0xFFFF, uint64, ObUInt64Type, 0xFFFF);
  TEST_CAST(bit, 0xFFFFFFFFFFFFFFFF, uint64, ObUInt64Type, 0xFFFFFFFFFFFFFFFF);

  TEST_CAST(bit, 0x1, utinyint, ObUTinyIntType, 0x1);
  TEST_CAST(bit, 0x2, utinyint, ObUTinyIntType, 0x2);
  TEST_CAST(bit, 0x80, utinyint, ObUTinyIntType, 128);
  TEST_CAST(bit, 0xFFFFFFFFFFFFFFFF, utinyint, ObUTinyIntType, 255);
}

TEST(tst_bit_type, bit_to_double)
{
  int ret = OB_SUCCESS;
  ObCastCtx cast_ctx;
  cast_ctx.cast_mode_ = CM_WARN_ON_FAIL;
  TEST_CAST(bit, 0x1, double, ObDoubleType, 1);
  TEST_CAST(bit, 0xFF, double, ObDoubleType, 0xFF);
  TEST_CAST(bit, 0xFFFF, double, ObDoubleType, 0xFFFF);
  TEST_CAST(bit, 0x8FFFFFFFFFFFFFFF, double, ObDoubleType, 10376293541461622784.00000);//same as mysql???
  TEST_CAST(bit, 0xFFFFFFFFFFFFFFFF, double, ObDoubleType, 18446744073709551616.00000);//same as mysql???
}

TEST(tst_bit_type, bit_to_float)
{
  int ret = OB_SUCCESS;
  ObCastCtx cast_ctx;
  cast_ctx.cast_mode_ = CM_WARN_ON_FAIL;
  TEST_CAST(bit, 0x1, float, ObFloatType, 1);
  TEST_CAST(bit, 0xFF, float, ObFloatType, 0xFF);
  TEST_CAST(bit, 0xFFFF, float, ObFloatType, 0xFFFF);
  TEST_CAST(bit, 0x8FFFFFFFFFFFFFFF, float, ObFloatType, 10376293541461622784.00000);//same as mysql???
  TEST_CAST(bit, 0xFFFFFFFFFFFFFFFF, float, ObFloatType, 18446744073709551616.00000);//same as mysql???
}

TEST(tst_bit_type, bit_to_string)
{
  //  int ret = OB_SUCCESS;
  ObCastCtx cast_ctx;
  cast_ctx.cast_mode_ = CM_WARN_ON_FAIL;
  const int64_t LEN = 8;
  int64_t pos = 0;
  char buf[LEN];
  int64_t value = 0x010203040506ABCD;
  //  obmysql::ObMySQLUtil::store_int8(buf, LEN, value, pos);
  int32_t bit_len = 0;
  EXPECT_EQ(OB_SUCCESS, get_bit_len(value, bit_len));
  bit_to_char_array(value, bit_len, buf, LEN, pos);
  for(int i = 0; i < LEN; i++) {
    printf("%X ", buf[i]);
  }
  cout<<endl;
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc,argv);
  system("rm -rf test_expr.log");
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_expr.log", true);
  return RUN_ALL_TESTS();
}
