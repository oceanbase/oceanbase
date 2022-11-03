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
#include "sql/engine/expr/ob_expr_is_not.h"
#include "ob_expr_test_utils.h"
#include "lib/oblog/ob_log.h"
using namespace oceanbase::common;
using namespace oceanbase::sql;

class ObExprIsNotTest: public ::testing::Test
{
  public:
    ObExprIsNotTest();
    virtual ~ObExprIsNotTest();
    virtual void SetUp();
    virtual void TearDown();
  private:
    // disallow copy
    ObExprIsNotTest(const ObExprIsNotTest &other);
    ObExprIsNotTest& operator=(const ObExprIsNotTest &other);
  protected:
    // data members
};

ObExprIsNotTest::ObExprIsNotTest()
{
}

ObExprIsNotTest::~ObExprIsNotTest()
{
}

void ObExprIsNotTest::SetUp()
{
}

void ObExprIsNotTest::TearDown()
{
}

#define COMPARE_EXPECT_(cmp_op, ctx, func, type1, v1, type2, v2, res) \
     {                                                          \
       ObObj t1;                                            \
       ObObj t2;                                            \
       ObObj vres;                                          \
       ObObj t3;                                            \
       t3.set_bool(false);                                  \
       ObArenaAllocator alloc;\
       cmp_op op(alloc);                                               \
       t1.set_##type1(v1);                                      \
         t2.set_##type2(v2);                                    \
           int err = op.func(vres, t1, t2, t3, ctx);                     \
           if (res == MY_ERROR)                                 \
           {                                                    \
             ASSERT_NE(OB_SUCCESS, err);                        \
           }                                                    \
           else                                                 \
           {                                                    \
             ASSERT_EQ(OB_SUCCESS, err);                        \
             switch(res)                                        \
             {                                                  \
               case MY_TRUE:                                    \
                 ASSERT_TRUE(vres.is_true());                   \
                 break;                                         \
               case MY_FALSE:                                   \
                 ASSERT_TRUE(vres.is_false());                  \
                 break;                                         \
               default:                                         \
                 ASSERT_TRUE(vres.is_null());                   \
                 break;                                         \
             }                                                  \
           }\
} while(0)

#define COMPARE_EXPECT_NULL1(cmp_op, ctx, func, type1, type2, v2, res) \
     {                                                          \
       ObObj t1;                                            \
       ObObj t2;                                            \
       ObObj vres;                                          \
       ObObj t3;                                            \
       t3.set_bool(false);                                  \
       ObArenaAllocator alloc;\
       cmp_op op(alloc);                                               \
       t1.set_##type1();                                      \
         t2.set_##type2(v2);                                    \
           int err = op.func(vres, t1, t2, t3, ctx);                     \
           if (res == MY_ERROR)                                 \
           {                                                    \
             ASSERT_NE(OB_SUCCESS, err);                        \
           }                                                    \
           else                                                 \
           {                                                    \
             ASSERT_EQ(OB_SUCCESS, err);                        \
             switch(res)                                        \
             {                                                  \
               case MY_TRUE:                                    \
                 ASSERT_TRUE(vres.is_true());                   \
                 break;                                         \
               case MY_FALSE:                                   \
                 ASSERT_TRUE(vres.is_false());                  \
                 break;                                         \
               default:                                         \
                 ASSERT_TRUE(vres.is_null());                   \
                 break;                                         \
             }                                                  \
           }\
} while(0)

#define COMPARE_EXPECT_NULL2(cmp_op, ctx, func, type1, v1, type2, res) \
     {                                                          \
       ObObj t1;                                            \
       ObObj t2;                                            \
       ObObj vres;                                          \
       ObObj t3;                                            \
       t3.set_bool(false);                                  \
       ObArenaAllocator alloc;\
       cmp_op op(alloc);                                               \
       t1.set_##type1(v1);                                      \
         t2.set_##type2();                                    \
           int err = op.func(vres, t1, t2, t3, ctx);                     \
           if (res == MY_ERROR)                                 \
           {                                                    \
             ASSERT_NE(OB_SUCCESS, err);                        \
           }                                                    \
           else                                                 \
           {                                                    \
             ASSERT_EQ(OB_SUCCESS, err);                        \
             switch(res)                                        \
             {                                                  \
               case MY_TRUE:                                    \
                 ASSERT_TRUE(vres.is_true());                   \
                 break;                                         \
               case MY_FALSE:                                   \
                 ASSERT_TRUE(vres.is_false());                  \
                 break;                                         \
               default:                                         \
                 ASSERT_TRUE(vres.is_null());                   \
                 break;                                         \
             }                                                  \
           }\
} while(0)

#define COMPARE_EXPECT_NULL1_NULL2(cmp_op, ctx, func, type1, type2, res) \
     {                                                          \
       ObObj t1;                                            \
       ObObj t2;                                            \
       ObObj t3;                                            \
       t3.set_bool(false);                                  \
       ObObj vres;                                          \
       ObArenaAllocator alloc;\
       cmp_op op(alloc);                                               \
       t1.set_##type1();                                      \
         t2.set_##type2();                                    \
           int err = op.func(vres, t1, t2, t3, ctx);                     \
           if (res == MY_ERROR)                                 \
           {                                                    \
             ASSERT_NE(OB_SUCCESS, err);                        \
           }                                                    \
           else                                                 \
           {                                                    \
             ASSERT_EQ(OB_SUCCESS, err);                        \
             switch(res)                                        \
             {                                                  \
               case MY_TRUE:                                    \
                 ASSERT_TRUE(vres.is_true());                   \
                 break;                                         \
               case MY_FALSE:                                   \
                 ASSERT_TRUE(vres.is_false());                  \
                 break;                                         \
               default:                                         \
                 ASSERT_TRUE(vres.is_null());                   \
                 break;                                         \
             }                                                  \
           }\
} while(0)

#define T(t1, v1, t2, v2, res) COMPARE_EXPECT_(ObExprIsNot, ctx, calc_result3, t1, v1, t2, v2, res)
#define T_NULL1(t1, t2, v2, res) COMPARE_EXPECT_NULL1(ObExprIsNot, ctx, calc_result3, t1, t2, v2, res)
#define T_NULL2(t1, v1, t2, res) COMPARE_EXPECT_NULL2(ObExprIsNot, ctx, calc_result3, t1, v1, t2, res)
#define T_NULL1_NULL2(t1, t2, res) COMPARE_EXPECT_NULL1_NULL2(ObExprIsNot, ctx, calc_result3, t1, t2, res)
TEST_F(ObExprIsNotTest, basic_test)
{
  ObExprStringBuf buf;
  ObExprCtx ctx(NULL, NULL, NULL, &buf);
  T(bool, true, bool, true, MY_FALSE);
  T(bool, true, bool, false, MY_TRUE);
  T_NULL2(bool, true, null, MY_TRUE);
  T(bool, false, bool, true, MY_TRUE);
  T(bool, false, bool, false, MY_FALSE);
  T_NULL2(bool, true, null, MY_TRUE);
  T_NULL1(null, bool, true, MY_TRUE);
  T_NULL1(null, bool, false, MY_TRUE);
  T_NULL1_NULL2(null, null, MY_FALSE);

  T(int, 0, bool, true, MY_TRUE);
  T(int, 0, bool, false, MY_FALSE);
  T_NULL2(int, 0, null, MY_TRUE);
  T_NULL2(float, 0.0, null, MY_TRUE);
  T_NULL2(double, 0.0, null, MY_TRUE);
  //T(precise_datetime, 0, null, 0, MY_TRUE);
  //T(ctime, 0, null, 0, MY_TRUE);
  //T(mtime, 0, null, 0, MY_TRUE);

  T(varchar, "20080115", bool, true, MY_FALSE);
  T(varchar, "a", bool, true, MY_TRUE);
  T(varchar, "231#dsank2", bool, true, MY_FALSE);
  T(int, 1, bool, true, MY_FALSE);
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
