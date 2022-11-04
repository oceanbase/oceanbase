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

#ifndef _OB_EXPR_TEST_UTILS_H
#define _OB_EXPR_TEST_UTILS_H 1
#define USING_LOG_PREFIX SQL_ENG
#include <math.h>
#include "sql/engine/expr/ob_expr_equal.h"
#include "lib/number/ob_number_v2.h"
#include "lib/oblog/ob_log.h"
#include "lib/charset/ob_dtoa.h"
#include "share/object/ob_obj_cast.h"
enum MyBool
{
  MY_TRUE,
  MY_FALSE,
  MY_NULL,
  MY_ERROR
};
static oceanbase::common::ObArenaAllocator g_alloc_;

#define TEST_OPERATOR(expr_op) \
    class Test##expr_op : public expr_op \
{ \
public:\
       Test##expr_op() : expr_op(g_alloc_) {}\
  ~Test##expr_op() {}\
}

#define EV 0.00001
#define LOGIC_ERROR2(cmp_op, str_buf, func, type1, v1, type2, v2, res) \
  do {                                                                 \
    ObObj t1;                                             \
    ObObj t2;                                             \
    ObObj vres;                                           \
    cmp_op op(g_alloc_);                                            \
    t1.set_##type1(v1);                                   \
    t2.set_##type2(v2);                                   \
    ObExprCtx expr_ctx(NULL, NULL, NULL, str_buf);        \
    int err = op.func(vres, t1, t2, expr_ctx);            \
    ASSERT_EQ(res, err);                                  \
  } while(0)

#define COMPARE_EXPECT_INNER(cmp_op, collation, str_buf, func, type1, v1, type2, v2, res) \
  do {                                                          \
    ObObj t1;                                                \
    ObObj t2;                                                \
    ObObj vres;                                              \
    cmp_op op(g_alloc_);                                               \
    ObExprResType res_type;                                  \
    res_type.set_collation_level(CS_LEVEL_EXPLICIT);         \
    res_type.set_collation_type(collation);                  \
    op.set_result_type(res_type);                            \
    t1.set_##type1(v1);                                      \
    t1.set_collation_type(collation);              \
    t2.set_##type2(v2);                                      \
    t2.set_collation_type(collation);              \
    ObExprCtx expr_ctx(NULL, NULL, NULL, str_buf);           \
    res_type.set_calc_type(ObVarcharType);  \
    res_type.set_calc_collation_type(collation);   \
    op.set_result_type(res_type);  \
    int err = op.func(vres, t1, t2, expr_ctx);               \
    if (res == MY_ERROR) {                                   \
      ASSERT_NE(OB_SUCCESS, err);                            \
    } else {                                                 \
      ASSERT_EQ(OB_SUCCESS, err);                            \
      OB_LOG(INFO, "after calc", K(vres), K(t1), K(t2));     \
      switch(res) {                                          \
        case MY_TRUE:                                        \
          ASSERT_TRUE(vres.is_true());                       \
          break;                                             \
        case MY_FALSE:                                       \
          ASSERT_TRUE(vres.is_false());                      \
          break;                                             \
        case MY_NULL:                                        \
          ASSERT_TRUE(vres.is_null());                       \
        default:                                             \
          break;                                             \
      }                                                      \
    }                                                        \
  } while(0)

#define COMPARE_EXPECT(cmp_op, str_buf, func, type1, v1, type2, v2, res) \
  COMPARE_EXPECT_INNER(cmp_op, CS_TYPE_BINARY, str_buf, func, type1, v1, type2, v2, res)
#define COMPARE_EXPECT_BIN(cmp_op, str_buf, func, type1, v1, type2, v2, res) \
  COMPARE_EXPECT_INNER(cmp_op, CS_TYPE_UTF8MB4_BIN, str_buf, func, type1, v1, type2, v2, res)
#define COMPARE_EXPECT_GEN(cmp_op, str_buf, func, type1, v1, type2, v2, res) \
  COMPARE_EXPECT_INNER(cmp_op, CS_TYPE_UTF8MB4_GENERAL_CI, str_buf, func, type1, v1, type2, v2, res)

#define LOGIC_ERROR3(cmp_op, str_buf, func, type1, v1, type2, v2, type3, v3, res) \
  do {                                                                  \
   ObObj t1;                                                        \
   ObObj t2;                                                        \
   ObObj t3;                                                        \
   ObObj vres;                                                      \
   cmp_op op(g_alloc_);                                                           \
   t1.set_##type1(v1);                                                  \
   t2.set_##type2(v2);                                                  \
   t3.set_##type3(v3);                                                  \
   ObExprCtx expr_ctx(NULL, NULL, NULL, str_buf);                             \
   int err = op.func(vres, t1, t2, t3, expr_ctx);                       \
   ASSERT_EQ(res, err);                                                 \
   } while(0)

#define COMPARE3_EXPECT(cmp_op, str_buf, func, type1, v1, type2, v2, type3, v3, res) \
     do      {                                                    \
               ObObj t1;                                        \
               ObObj t2;                                        \
               ObObj t3;                                        \
               ObObj vres;                                      \
               cmp_op op(g_alloc_);                                       \
               t1.set_##type1(v1);                              \
               t2.set_##type2(v2);                              \
               t3.set_##type3(v3);                              \
               ObExprCtx expr_ctx(NULL, NULL, NULL, str_buf);         \
               int err = op.func(vres, t1, t2, t3, expr_ctx);   \
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

// bad design, need remove
#define STR_FUNC_EXPECT(str_op_object, func, type1, v1, type2, v2, res)    \
  do {                                                         \
    ObObj t1;                                           \
    ObObj t2;                                           \
    ObObj vres;                                         \
    t1.set_##type1(v1);                                 \
    t2.set_##type2(v2);                                   \
    int err = str_op_object.func(vres, t1, t2);         \
        ASSERT_EQ(OB_SUCCESS, err);                         \
        switch(res)                                         \
        {                                                   \
          case MY_TRUE:                                     \
            ASSERT_TRUE(vres.is_true());                    \
            break;                                          \
          case MY_FALSE:                                    \
            ASSERT_TRUE(vres.is_false());                   \
            break;                                          \
          default:                                          \
            ASSERT_TRUE(vres.is_null());                    \
            break;                                          \
        }                                                   \
  } while(0)

#define LOGIC_EXPECT2(cmp_op, str_buf, func, type1, v1, type2, v2, rtype, res) \
  do {                                                                   \
    ObObj t1;                                                     \
    ObObj t2;                                                     \
    ObObj vres;                                                   \
    bool bv;                                                          \
    cmp_op op(g_alloc_);                                                        \
    t1.set_##type1(v1);                                               \
      t2.set_##type2(v2);                                             \
      ObExprCtx expr_ctx(NULL, NULL, NULL, str_buf);            \
        int err = op.func(vres, t1, t2, expr_ctx);                              \
        if (OB_SUCCESS == err)\
        {\
       ASSERT_EQ(rtype, vres.get_type());                                    \
        if (ObBoolType == rtype)                                      \
        {                                                             \
          ASSERT_EQ(OB_SUCCESS, vres.get_bool(bv));                   \
          ASSERT_EQ(res, bv);                                         \
        }                                                             \
        }\
        else \
        {\
        ASSERT_EQ(res, err);                                   \
        }\
  }while(0)

// bad design, need remove
#define STR_FUNC_EXPECT(str_op_object, func, type1, v1, type2, v2, res) \
       do {                                                                \
        ObObj t1;                                                   \
        ObObj t2;                                                   \
        ObObj vres;                                                 \
        t1.set_##type1(v1);                                             \
        t2.set_##type2(v2);                                             \
        int err = str_op_object.func(vres, t1, t2);                     \
        ASSERT_EQ(OB_SUCCESS, err);                                     \
        switch(res)                                                     \
        {                                                               \
case MY_TRUE:                                                           \
ASSERT_TRUE(vres.is_true());                                            \
break;                                                                  \
case MY_FALSE:                                                          \
ASSERT_TRUE(vres.is_false());                                           \
break;                                                                  \
default:                                                                \
ASSERT_TRUE(vres.is_null());                                            \
break;                                                                  \
}                                                                       \
        } while(0)

#define LOGIC_EXPECT3(cmp_op, str_buf, func, type1, v1, type2, v2, type3, v3, rtype, res) \
           do {                                                           \
             ObObj t1;                                              \
             ObObj t2;                                              \
             ObObj t3;                                              \
             ObObj vres;                                            \
             bool bv;                                                   \
             cmp_op op(g_alloc_);                                                 \
             t1.set_##type1(v1);                                        \
             t2.set_##type2(v2);                                        \
             t3.set_##type3(v3);                                        \
             ObExprCtx expr_ctx(NULL, NULL, NULL, str_buf);            \
             int err = op.func(vres, t1, t2, t3, expr_ctx);                       \
             if (OB_SUCCESS == err)\
             {                                                          \
               ASSERT_EQ(rtype, vres.get_type());                       \
               if (ObBoolType == rtype)                                 \
               {                                                        \
                 ASSERT_EQ(OB_SUCCESS, vres.get_bool(bv));              \
                 ASSERT_EQ(res, bv);                                    \
               }                                                        \
             }                                                          \
             else                                                       \
             {                                                          \
               ASSERT_EQ(res, err);                                     \
             }                                                          \
            } while(0)
#define LOGIC_GENPARAM1(params, type1, v1) \
         params[0].set_##type1(v1);
#define LOGIC_GENPARAM2(params, type1, v1, type2, v2) \
         params[0].set_##type1(v1);                   \
         params[1].set_##type2(v2)

#define LOGIC_GENPARAM3(params, type1, v1, type2, v2, type3, v3)  \
         LOGIC_GENPARAM2(params, type1, v1, type2, v2);           \
         params[2].set_##type3(v3)

#define LOGIC_GENPARAM4(params, type1, v1, type2, v2, type3, v3, type4, v4) \
         LOGIC_GENPARAM3(params, type1, v1, type2, v2, type3, v3);      \
         params[3].set_##type4(v4)

#define LOGIC_GENPARAM5(params, type1, v1, type2, v2, type3, v3, type4, v4, type5, v5) \
         LOGIC_GENPARAM4(params, type1, v1, type2, v2, type3, v3, type4, v4); \
         params[4].set_##type5(v5)

#define LOGIC_GENPARAM6(params, type1, v1, type2, v2, type3, v3, type4, v4, type5, v5, type6, v6) \
                 LOGIC_GENPARAM5(params, type1, v1, type2, v2, type3, v3, type4, v4, type5, v5); \
                 params[5].set_##type6(v6)

#define LOGIC_GENPARAM7(params, type1, v1, type2, v2, type3, v3, type4, v4, type5, v5, type6, v6, type7, v7) \
  LOGIC_GENPARAM6(params, type1, v1, type2, v2, type3, v3, type4, v4, type5, v5, type6, v6); \
                 params[6].set_##type7(v7)

#define LOGIC_GENPARAM8(params, type1, v1, type2, v2, type3, v3, type4, v4, type5, v5, type6, v6, type7, v7, type8, v8) \
  LOGIC_GENPARAM7(params, type1, v1, type2, v2, type3, v3, type4, v4, type5, v5, type6, v6, type7, v7); \
                 params[7].set_##type8(v8)

#define LOGIC_GENPARAM9(params, type1, v1, type2, v2, type3, v3, type4, v4, type5, v5, type6, v6, type7, v7, type8, v8, type9, v9) \
  LOGIC_GENPARAM8(params, type1, v1, type2, v2, type3, v3, type4, v4, type5, v5, type6, v6, type7, v7, type8, v8); \
                 params[8].set_##type9(v9)

#define LOGIC_GENPARAM10(params, type1, v1, type2, v2, type3, v3, type4, v4, type5, v5, type6, v6, type7, v7, type8, v8, type9, v9, type10, v10) \
  LOGIC_GENPARAM9(params, type1, v1, type2, v2, type3, v3, type4, v4, type5, v5, type6, v6, type7, v7, type8, v8, type9, v9); \
                 params[9].set_##type10(v10)

#define LOGIC_GENPARAM11(params, type1, v1, type2, v2, type3, v3, type4, v4, type5, v5, type6, v6, type7, v7, type8, v8, type9, v9, type10, v10, type11, v11) \
  LOGIC_GENPARAM10(params, type1, v1, type2, v2, type3, v3, type4, v4, type5, v5, type6, v6, type7, v7, type8, v8, type9, v9, type10, v10); \
                 params[10].set_##type11(v11)

#define LOGIC_GENPARAM12(params, type1, v1, type2, v2, type3, v3, type4, v4, type5, v5, type6, v6, type7, v7, type8, v8, type9, v9, type10, v10, type11, v11, type12, v12) \
  LOGIC_GENPARAM11(params, type1, v1, type2, v2, type3, v3, type4, v4, type5, v5, type6, v6, type7, v7, type8, v8, type9, v9, type10, v10, type11, v11); \
                 params[11].set_##type12(v12)
#define LOGIC_GENPARAM13(params, type1, v1, type2, v2, type3, v3, type4, v4, type5, v5, type6, v6, type7, v7, type8, v8, type9, v9, type10, v10, type11, v11, type12, v12, type13, v13) \
  LOGIC_GENPARAM12(params, type1, v1, type2, v2, type3, v3, type4, v4, type5, v5, type6, v6, type7, v7, type8, v8, type9, v9, type10, v10, type11, v11, type12, v12); \
                 params[12].set_##type13(v13)

#define LOGIC_GENTYPE2(params, t1, t2)          \
                 params[0].set_type(t1);        \
                 params[1].set_type(t2)

#define LOGIC_GENTYPE3(params, t1, t2, t3)        \
                 LOGIC_GENTYPE2(params, t1, t2);  \
                 params[2].set_type(t3)

#define LOGIC_GENTYPE4(params, t1, t2, t3, t4)        \
                 LOGIC_GENTYPE3(params, t1, t2, t3);  \
                 params[3].set_type(t4)

#define LOGIC_GENTYPE5(params, t1, t2, t3, t4, t5)        \
                 LOGIC_GENTYPE4(params, t1, t2, t3, t4);  \
                 params[4].set_type(t5)

#define LOGIC_GENTYPE6(params, t1, t2, t3, t4, t5, t6)        \
                 LOGIC_GENTYPE5(params, t1, t2, t3, t4, t5);  \
                 params[5].set_type(t6)

#define LOGIC_EXPECTN(cmp_op, num, rtype, res, ...)     \
        do {                                              \
          ObObj vres;                               \
          cmp_op op(g_alloc_);                                    \
          bool bv;                                      \
          ObObj params[num];                        \
          LOGIC_GENPARAM##num(params, __VA_ARGS__);     \
          ObExprCtx expr_ctx(NULL, NULL, NULL, str_buf);            \
          int err = op.calc_resultN(vres, params, num, expr_ctx); \
          if (OB_SUCCESS == err)\
          {                                                             \
            ASSERT_EQ(rtype, vres.get_type());                          \
            if (ObBoolType == rtype)                                    \
            {                                                           \
              ASSERT_EQ(OB_SUCCESS, vres.get_bool(bv));                 \
              ASSERT_EQ(res, bv);                                       \
            }                                                           \
          }                                                             \
          else                                                          \
          {                                                             \
            ASSERT_EQ(res, err);                                        \
          }                                                             \
         } while(0)

#define LOGIC_EXPECT_TYPE2(cmp_op, func, type1, type2, res) \
                do {                                          \
                  ObExprResType t1;                         \
                  ObExprResType t2;                         \
                  ObExprResType vres;                       \
                  ObExprTypeCtx ctx;                           \
                  cmp_op op(g_alloc_);                                \
                  t1.set_type(type1);                       \
                  t2.set_type(type2);                       \
                  int err = op.func(vres, t1, t2, ctx);     \
                  if (OB_SUCCESS == err)                    \
                  {                                         \
                    ASSERT_EQ(res, vres.get_type());        \
                  }                                         \
                  else                                      \
                  {                                         \
                    ASSERT_EQ(res, err);                    \
                  }                                         \
                 }while(0)

#define LOGIC_EXPECT_TYPE3(cmp_op, func, type1, type2, type3, res)  \
                    do {                                              \
                       ObExprResType t1;                            \
                       ObExprResType t2;                            \
                       ObExprResType t3;                            \
                       ObExprResType vres;                          \
                       ObExprTypeCtx ctx;                           \
                       cmp_op op(g_alloc_);                                   \
                       t1.set_type(type1);                          \
                       t2.set_type(type2);                          \
                       t3.set_type(type3);                          \
                       int err = op.func(vres, t1, t2, t3, ctx);    \
                       if (OB_SUCCESS == err)                       \
                       {                                            \
                         ASSERT_EQ(res, vres.get_type());           \
                       }                                            \
                       else                                         \
                       {                                            \
                         ASSERT_EQ(res, err);                       \
                       }                                            \
                     }while(0)

#define LOGIC_EXPECT_TYPEN(cmp_op, num, res, ...)                       \
                    do {                                                   \
                     ObExprResType vres;                                \
                     ObExprTypeCtx ctx;                                 \
                     cmp_op op(g_alloc_);                                         \
                     ObExprResType params[num];                         \
                     LOGIC_GENTYPE##num(params, __VA_ARGS__);           \
                     int err = op.calc_result_typeN(vres, params, num, ctx); \
                      if (OB_SUCCESS == err)                       \
                       {                                            \
                         ASSERT_EQ(res, vres.get_type());           \
                       }                                            \
                       else                                         \
                       {                                            \
                         ASSERT_EQ(res, err);                       \
                       }                                            \
                     } while(0)

#define ARITH_EXPECT_TYPE_WITH_ROW(cmp_op, func, type1, type2, res) \
                       do {                                           \
                          ObExprResType t1;                         \
                          ObExprResType t2;                         \
                          ObExprResType vres;                       \
                          ObExprTypeCtx ctx;                        \
                          cmp_op op(g_alloc_);                                \
                          op.set_row_dimension(1);                  \
                          t1.set_type(type1);                       \
                          t2.set_type(type2);                       \
                          int err = op.func(vres, t1, t2, ctx);     \
                          ASSERT_EQ(res, err);                      \
                        }while(0)

#define ARITH_EXPECT_TYPE(cmp_op, func, type1, type2, res)  \
                       do {                                   \
                          ObExprResType t1;                 \
                          ObExprResType t2;                 \
                          ObExprResType vres;               \
                          ObExprTypeCtx ctx;                \
                          cmp_op op(g_alloc_);                        \
                          t1.set_type(type1);               \
                          t2.set_type(type2);               \
                          int err = op.func(vres, t1, t2, ctx);  \
                          ASSERT_EQ(OB_SUCCESS, err);       \
                          ASSERT_EQ(res, vres.get_type());  \
                        }while(0)

#define ARITH_EXPECT_TYPE_OBJ(cmp_op,str_buf, func, obj1, obj2, res)  \
                        do {                                   \
                          ObObj vres;               \
                          cmp_op op(g_alloc_);                        \
                          int err = op.func(vres, obj1, obj2,str_buf);  \
                          ASSERT_EQ(OB_SUCCESS, err);       \
                          ASSERT_EQ(res, vres.get_type());  \
                        }while(0)

#define ARITH_EXPECT_TYPE3(cmp_op, func, type1, type2, type3, res)  \
                       do {                                   \
                        ObExprResType t1;                 \
                        ObExprResType t2;                 \
                        ObExprResType t3;                 \
                        ObExprResType vres;               \
                        ObExprTypeCtx ctx;                \
                        cmp_op op(g_alloc_);                        \
                        t1.set_type(type1);               \
                        t2.set_type(type2);               \
                        t3.set_type(type3);               \
                        if (type1 == ObVarcharType) {     \
                          t1.set_collation_type(CS_TYPE_UTF8MB4_BIN); \
                          t1.set_collation_level(CS_LEVEL_EXPLICIT);                 \
                        } \
                        if (type2 == ObVarcharType) {     \
                          t2.set_collation_type(CS_TYPE_UTF8MB4_BIN); \
                          t2.set_collation_level(CS_LEVEL_EXPLICIT);                 \
                        } \
                        if (type3 == ObVarcharType) {     \
                          t3.set_collation_type(CS_TYPE_UTF8MB4_BIN); \
                          t3.set_collation_level(CS_LEVEL_EXPLICIT);                 \
                        } \
                        OB_LOG(INFO, "set type", K(t1),K(t2),K(t3)); \
                        int err = op.func(vres, t1, t2, t3, ctx);  \
                        ASSERT_EQ(OB_SUCCESS, err);       \
                        ASSERT_EQ(res, vres.get_type());  \
                        }while(0)

#define ARITH_EXPECT_TYPE_ERROR(cmp_op, func, type1, type2)           \
                        do {                                            \
                          ObExprResType t1;                           \
                          ObExprResType t2;                           \
                          ObExprResType vres;                         \
                          ObExprTypeCtx ctx;                          \
                          cmp_op op(g_alloc_);                                  \
                          t1.set_type(type1);                         \
                          t2.set_type(type2);                         \
                          int err = op.func(vres, t1, t2, ctx);       \
                          ASSERT_EQ(OB_ERR_INVALID_TYPE_FOR_OP, err); \
                         }while(0)

#define ARITH_EXPECT_TYPE_ERROR3(cmp_op, func, type1, type2, type3)   \
                         do {                                            \
                         ObExprResType t1;                           \
                         ObExprResType t2;                           \
                         ObExprResType t3;                           \
                         ObExprResType vres;                         \
                         ObExprTypeCtx ctx;                          \
                         cmp_op op(g_alloc_);                                  \
                         t1.set_type(type1);                         \
                         t2.set_type(type2);                         \
                         t3.set_type(type3);                         \
                         int err = op.func(vres, t1, t2, t3, ctx);    \
                         ASSERT_EQ(OB_INVALID_ARGUMENT, err); \
                         }while(0)

#define ARITH_EXPECT_TYPE_ERROR4(cmp_op, func, type1, type2, type3)   \
                         do {                                            \
                         ObExprResType t1;                           \
                         ObExprResType t2;                           \
                         ObExprResType t3;                           \
                         ObExprResType vres;                         \
                         ObExprTypeCtx ctx;                          \
                         cmp_op op(g_alloc_);                                  \
                         t1.set_type(type1);                         \
                         t2.set_type(type2);                         \
                         t3.set_type(type3);                         \
                         int err = op.func(vres, t1, t2, t3, ctx);   \
                         ASSERT_EQ(OB_ERR_UNEXPECTED, err); \
                         }while(0)

#define ARITH_ERROR(cmp_op, str_buf, func, type1, v1, type2, v2, res)      \
                           do {                                      \
                             ObObj t1;                        \
                             ObObj t2;                        \
                             ObObj vres;                      \
                             cmp_op op(g_alloc_);                           \
                             t1.set_##type1(v1);                  \
                               t2.set_##type2(v2);                \
                               ObExprCtx expr_ctx(NULL, NULL, NULL, str_buf);            \
                                 int err = op.func(vres, t1, t2, expr_ctx); \
                                 ASSERT_EQ(res, err);             \
                           }while(0)
#define ARITH_EXPECT_ERROR(cmp_op, str_buf, func, t1, v1, t2, v2)    \
do {                                                     \
  ObObj ob1;                                          \
  ObObj ob2;                                          \
  ObObj vres;                                         \
  cmp_op op(g_alloc_);                                          \
  ob1.set_##t1(v1);                                   \
  ob2.set_##t2(v2);                                   \
  int ret = op.func(vres, ob1, ob2, str_buf, -1);     \
  EXPECT_TRUE(OB_FAIL(ret));                     \
} while(0)

#define ARITH_EXPECT_OBJ(cmp_op, str_buf, func, type1, v1, type2, v2, rt, rv) \
do {                                                     \
  ObObj t1;                                           \
  ObObj t2;                                           \
  ObObj vres;                                         \
  ObObj resf;                                         \
  cmp_op op(g_alloc_);                                          \
  resf.set_##rt(rv);                                  \
  t1.set_##type1(v1);                                 \
  t1.set_collation_type(CS_TYPE_UTF8MB4_BIN);         \
  t2.set_##type2(v2);                                 \
  t2.set_collation_type(CS_TYPE_UTF8MB4_BIN);         \
  int ret = op.func(vres, t1, t2, str_buf, -1);       \
  OB_LOG(DEBUG, "after calc", K(t1), K(t2), K(vres), K(resf)); \
  EXPECT_TRUE(OB_SUCC(ret));                     \
  if (vres.get_type() == ObDoubleType || vres.get_type() == ObFloatType) { \
  } else { \
    EXPECT_TRUE(ObObjCmpFuncs::compare_oper_nullsafe(resf, vres, CS_TYPE_UTF8MB4_BIN, CO_EQ));\
  }\
} while(0)

#define ARITH_EXPECT(cmp_op, str_buf, func, type1, v1, type2, v2, res) \
  do {                                  \
    ObObj t1;                     \
    ObObj t2;                     \
    ObObj vres;                   \
    cmp_op op(g_alloc_);\
    t1.set_##type1(v1);               \
    t2.set_##type2(v2);               \
    int err = op.func(vres, t1, t2, str_buf, -1);  \
    if (OB_SUCCESS != err)            \
    {         \
      ASSERT_EQ(res, err);             \
    }        \
    else      \
    {         \
      switch(vres.get_type())          \
      {        \
        case ObIntType:                       \
        ASSERT_EQ(res, vres.get_int());       \
        break;                                \
        case ObUInt64Type:                   \
        ASSERT_EQ(res, vres.get_uint64());      \
        break;        \
        case ObFloatType:                     \
        ASSERT_TRUE(fabsf(res-vres.get_float()<EV));                  \
        break;        \
        case ObDoubleType:                    \
        ASSERT_TRUE(fabs(res - vres.get_double())<EV);                \
        break;        \
        case ObNumberType:                    \
        {             \
          number::ObNumber res_nmb;             \
          res_nmb.from(#res, *(str_buf));        \
          ASSERT_STREQ(res_nmb.format(), vres.get_number().format());   \
          break;        \
        }             \
        case ObMaxType:                       \
        ASSERT_EQ(res, vres.get_type());      \
        break;       \
        case ObNullType:                       \
        ASSERT_EQ(res, vres.get_type());      \
        default:      \
        ASSERT_TRUE(vres.is_null());          \
        break;        \
      }             \
    }        \
  } while(0)

#define ROW1_COMPARE_EXPECT(cmp_op, str_buf, func, type1, v1, type2, v2, res)  \
                                do {                                     \
                                 ObObj t1[1];                     \
                                 ObObj t2[1];                     \
                                 ObObj vres;                      \
                                 cmp_op op(g_alloc_);                           \
                                 t1[0].set_##type1(v1);               \
                                 t2[0].set_##type2(v2);               \
                                 ObExprCtx expr_ctx(NULL, NULL, NULL, str_buf);            \
                                 int err = op.func(vres, t1, t2, 1, expr_ctx);  \
                                 ASSERT_EQ(OB_SUCCESS, err);          \
                                 switch(res)                          \
                                 {                                    \
                                 case MY_TRUE:                        \
                                   ASSERT_TRUE(vres.is_true());       \
                                   break;                             \
                                 case MY_FALSE:                       \
                                   ASSERT_TRUE(vres.is_false());      \
                                   break;                             \
                                 default:                             \
                                   ASSERT_TRUE(vres.is_null());       \
                                   break;                             \
                                 }                                    \
                                } while(0)

#define ROW2_COMPARE_EXPECT(cmp_op, str_buf, func, type11, v11, type12, v12, type21, v21, type22, v22, res) \
                                    do {                                   \
                                     ObObj t1[2];                   \
                                     ObObj t2[2];                   \
                                     ObObj vres;                    \
                                     cmp_op op(g_alloc_);                         \
                                     t1[0].set_##type11(v11);           \
                                     t1[1].set_##type12(v12);           \
                                     t2[0].set_##type21(v21);           \
                                     t2[1].set_##type22(v22);           \
                                     ObExprCtx expr_ctx(NULL, NULL, NULL, str_buf);            \
                                     int err = op.func(vres, t1, t2, 2, expr_ctx); \
                                     ASSERT_EQ(OB_SUCCESS, err);        \
                                     switch(res)                        \
                                     {                                  \
case MY_TRUE:                                                           \
  ASSERT_TRUE(vres.is_true());                                          \
  break;                                                                \
                                     case MY_FALSE:                     \
                                       ASSERT_TRUE(vres.is_false());    \
                                       break;                           \
                                     default:                           \
                                       ASSERT_TRUE(vres.is_null());     \
                                       break;                           \
                                     }                                  \
                                    } while(0)

#define EXPECT_FAIL_RESULT0(str_op_object, str_buf, func, type1) \
                                    do {                                   \
                                      ObObj t1;                     \
                                      ObObj r;                      \
                                      ObObj ref;                    \
                                      t1.set_##type1();               \
                                      ObExprCtx expr_ctx(NULL, NULL, NULL, str_buf);            \
                                      int err = str_op_object.func(r, t1, expr_ctx); \
                                      ASSERT_TRUE(OB_SUCCESS != err); \
                                    } while(0)



#define EXPECT_FAIL_RESULT1(str_op_object, str_buf, func, type1, v1)                \
                                    do {                                               \
                                      ObObj t1;                                     \
                                      ObObj r;                                      \
                                      ObObj ref;                                    \
                                      t1.set_##type1(v1);                           \
                                      ObExprCtx expr_ctx(NULL, NULL, NULL, str_buf);\
                                      int err = str_op_object.func(r, t1, expr_ctx); \
                                      ASSERT_TRUE(OB_SUCCESS != err); \
                                    } while(0)

#define EXPECT_FAIL_RESULT1_CT(str_op_object, str_buf, func, type1, v1, ct1)                \
                                    do {                                               \
                                      ObObj t1;                                     \
                                      ObObj r;                                      \
                                      ObObj ref;                                    \
                                      t1.set_##type1(v1);                           \
                                      t1.set_collation_type(ct1);                   \
                                      ObExprCtx expr_ctx(NULL, NULL, NULL, str_buf);\
                                      int err = str_op_object.func(r, t1, expr_ctx); \
                                      ASSERT_TRUE(OB_SUCCESS != err); \
                                    } while(0)

#define EXPECT_RESULT0(str_op_object, str_buf, func, type1, ref_type, ref_value) \
                                        do {                               \
                                         ObObj t1;                  \
                                         ObObj r;                   \
                                         ObObj ref;                 \
                                         t1.set_##type1();            \
                                         t1.set_collation_type(CS_TYPE_UTF8MB4_BIN);         \
                                         ref.set_##ref_type(); \
                                         ref.set_collation_type(CS_TYPE_UTF8MB4_BIN);         \
                                         ObExprCtx expr_ctx(NULL, NULL, NULL, str_buf);\
                                         int err = str_op_object.func(r, t1, expr_ctx); \
                                         _OB_LOG(INFO, "ref=%s r=%s", to_cstring(ref), to_cstring(r)); \
                                         EXPECT_TRUE(OB_SUCCESS == err); \
                                         ASSERT_TRUE(ref.get_type() == r.get_type()); \
                                         if (ref.get_type() != ObNullType) \
                                         { \
                                           EXPECT_TRUE(ObObjCmpFuncs::compare_oper_nullsafe(ref, r, CS_TYPE_UTF8MB4_BIN, CO_EQ));\
                                         } \
                                        } while(0)



#define EXPECT_RESULT1(str_op_object, str_buf, func, type1, v1, ref_type, ref_value) \
                                        do {                               \
                                         ObObj t1;                  \
                                         ObObj r;                   \
                                         ObObj ref;                 \
                                         t1.set_##type1(v1);            \
                                         t1.set_collation_type(CS_TYPE_UTF8MB4_BIN);         \
                                         ref.set_##ref_type(ref_value); \
                                         ref.set_collation_type(CS_TYPE_UTF8MB4_BIN);         \
                                         ObExprCtx expr_ctx(NULL, NULL, NULL, str_buf);\
                                         int err = str_op_object.func(r, t1, expr_ctx); \
                                         _OB_LOG(INFO, "text=%s expect=%s result=%s", to_cstring(t1), to_cstring(ref), to_cstring(r)); \
                                         EXPECT_TRUE(OB_SUCCESS == err); \
                                         r.set_collation_type(CS_TYPE_UTF8MB4_BIN);         \
                                         ASSERT_TRUE(ref.get_type() == r.get_type()); \
                                         if (ref.get_type() != ObNullType) \
                                         { \
                                           EXPECT_TRUE(ObObjCmpFuncs::compare_oper_nullsafe(ref, r, CS_TYPE_UTF8MB4_BIN, CO_EQ));\
                                         } \
                                        } while(0)


#define EXPECT_RESULT1_CT(str_op_object, str_buf, func, type1, v1, ct1, ref_type, ref_value, ref_ct) \
                                        do {                               \
                                          ObObj t1;                 \
                                          ObObj t2;                 \
                                          ObObj r;                  \
                                          ObObj ref;                \
                                          t1.set_##type1(v1);           \
                                          t1.set_collation_type(ct1);\
                                          ref.set_##ref_type(ref_value); \
                                          ref.set_collation_type(ref_ct);\
                                          ObExprCtx expr_ctx(NULL, NULL, NULL, str_buf);            \
                                          int err = str_op_object.func(r, t1, expr_ctx); \
                                          _OB_LOG(INFO, "ref=%s r=%s", to_cstring(ref), to_cstring(r)); \
                                          EXPECT_TRUE(OB_SUCCESS == err); \
                                          EXPECT_TRUE(ref.get_type() == r.get_type()); \
                                          if (ref.get_type() != ObNullType) \
                                          { \
                                            EXPECT_TRUE(ObObjCmpFuncs::compare_oper_nullsafe(ref, r, ref_ct, CO_EQ));\
                                          } \
                                        } while(0)

#define EXPECT_FAIL_RESULT2(str_op_object, str_buf, func, type1, v1, type2, v2) \
                                        do {                               \
                                         ObObj t1;                  \
                                         ObObj t2;                  \
                                         ObObj r;                   \
                                         ObObj ref;                 \
                                         t1.set_##type1(v1);            \
                                         t2.set_##type2(v2);            \
                                         ObExprCtx expr_ctx(NULL, NULL, NULL, str_buf);            \
                                         int err = str_op_object.func(r, t1, t2, expr_ctx); \
                                         ASSERT_TRUE(OB_SUCCESS != err); \
                                         } while(0)

#define EXPECT_FAIL_RESULT2_CT(str_op_object, str_buf, func, type1, v1, ct1, type2, v2, ct2) \
                                        do {                               \
                                         ObObj t1;                  \
                                         ObObj t2;                  \
                                         ObObj r;                   \
                                         ObObj ref;                 \
                                         t1.set_##type1(v1);            \
                                         t1.set_collation_type(ct1);  \
                                         t2.set_##type2(v2);            \
                                         t2.set_collation_type(ct2);    \
                                         ObExprCtx expr_ctx(NULL, NULL, NULL, str_buf);            \
                                         int err = str_op_object.func(r, t1, t2, expr_ctx); \
                                         ASSERT_TRUE(OB_SUCCESS != err); \
                                         } while(0)

#define EXPECT_RESULT_WITH_NULL(str_op_object, str_buf, func, type1, type2, v2, ref_type, ref_value) \
                                        do {                               \
                                         UNUSED(ref_value);  \
                                          ObObj t1;                 \
                                          ObObj t2;                 \
                                          ObObj r;                  \
                                          ObObj ref;                \
                                          t1.set_##type1();           \
                                          t2.set_##type2(v2);         \
                                          ref.set_##ref_type(); \
                                          ObExprCtx expr_ctx(NULL, NULL, NULL, str_buf);\
                                          int err = str_op_object.func(r, t1, &t2, expr_ctx); \
                                          _OB_LOG(INFO, "respect=%s result=%s", to_cstring(ref), to_cstring(r)); \
                                          EXPECT_TRUE(OB_SUCCESS == err); \
                                          EXPECT_TRUE(ref.get_type() == r.get_type()); \
                                          EXPECT_TRUE(ref.get_type() == ObNullType); \
                                        } while(0)



#define EXPECT_RESULT2(str_op_object, str_buf, func, type1, v1, type2, v2,ref_type, ref_value) \
                                        do {                               \
                                          ObObj t1;                 \
                                          ObObj t2;                 \
                                          ObObj r;                  \
                                          ObObj ref;                \
                                          t1.set_##type1(v1);           \
                                          t2.set_##type2(v2);         \
                                          ref.set_##ref_type(ref_value); \
                                          ObExprResType res_type;                                  \
                                          res_type.set_collation_level(CS_LEVEL_EXPLICIT);         \
                                          res_type.set_collation_type(CS_TYPE_UTF8MB4_BIN);                  \
                                          res_type.set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);   \
                                          str_op_object.set_result_type(res_type);  \
                                          ObExprCtx expr_ctx(NULL, NULL, NULL, str_buf);            \
                                          int err = str_op_object.func(r, t1, t2, expr_ctx); \
                                          _OB_LOG(INFO, "ref=%s r=%s, t1=%s, t2=%s", to_cstring(ref), to_cstring(r), to_cstring(t1), to_cstring(t2)); \
                                          EXPECT_TRUE(OB_SUCCESS == err); \
                                          EXPECT_TRUE(ref.get_type() == r.get_type()); \
                                          if (ref.get_type() != ObNullType) \
                                          { \
                                            EXPECT_TRUE(ObObjCmpFuncs::compare_oper_nullsafe(ref, r, CS_TYPE_UTF8MB4_BIN, CO_EQ)); \
                                          } \
                                        } while(0)




#define EXPECT_RESULT2_CT(str_op_object, str_buf, func, type1, v1, ct1, type2, v2, ct2, ref_type, ref_value, ref_ct) \
                                        do {                               \
                                          ObObj t1;                 \
                                          ObObj t2;                 \
                                          ObObj r;                  \
                                          ObObj ref;                \
                                          t1.set_##type1(v1);           \
                                          t1.set_collation_type(ct1);\
                                          t2.set_##type2(v2);         \
                                          t2.set_collation_type(ct2);\
                                          ref.set_##ref_type(ref_value); \
                                          ref.set_collation_type(ref_ct);\
                                          ObExprCtx expr_ctx(NULL, NULL, NULL, str_buf);            \
                                          int err = str_op_object.func(r, t1, t2, expr_ctx); \
                                          _OB_LOG(INFO, "ref=%s r=%s", to_cstring(ref), to_cstring(r)); \
                                          EXPECT_TRUE(OB_SUCCESS == err); \
                                          EXPECT_TRUE(ref.get_type() == r.get_type()); \
                                          if (ref.get_type() != ObNullType) \
                                          { \
                                            EXPECT_TRUE(ObObjCmpFuncs::compare_oper_nullsafe(ref, r, ref_ct, CO_EQ));\
                                          } \
                                        } while(0)

#define EXPECT_FAIL_RESULT3(str_op_object, str_buf, func, type1, v1, type2, v2, type3, v3, ref_type, ref_value) \
                                             do {                          \
                                               ObObj t1;            \
                                               ObObj t2;            \
                                               ObObj t3;            \
                                               ObObj r;             \
                                               ObObj ref;           \
                                               t1.set_##type1(v1);      \
                                               t2.set_##type2(v2);    \
                                               t3.set_##type3(v3);  \
                                               ref.set_##ref_type(ref_value); \
                                               ObExprCtx expr_ctx(NULL, NULL, NULL, str_buf);            \
                                               int err = str_op_object.func(r, t1, t2, t3, expr_ctx); \
                                               ASSERT_TRUE(OB_SUCCESS != err); \
                                             } while(0)

#define EXPECT_RESULT3(str_op_object, str_buf, func, type1, v1, type2, v2, type3, v3, ref_type, ref_value) \
                                            do {                           \
                                             ObObj t1;              \
                                             ObObj t2;              \
                                             ObObj t3;              \
                                             ObObj r;               \
                                             ObObj ref;             \
                                             t1.set_collation_type(CS_TYPE_UTF8MB4_BIN);   \
                                             t1.set_##type1(v1);        \
                                             t2.set_##type2(v2);        \
                                             t3.set_##type3(v3);        \
                                             t2.set_collation_type(CS_TYPE_UTF8MB4_BIN);   \
                                             t3.set_collation_type(CS_TYPE_UTF8MB4_BIN);   \
                                             ref.set_##ref_type(ref_value); \
                                             ref.set_collation_type(CS_TYPE_UTF8MB4_BIN);   \
                                             r.set_collation_type(CS_TYPE_UTF8MB4_BIN);   \
                                             ObExprCtx expr_ctx(NULL, NULL, NULL, str_buf);            \
                                             ObExprResType res_type;                                  \
                                             res_type.set_collation_level(CS_LEVEL_EXPLICIT);         \
                                             res_type.set_collation_type(CS_TYPE_UTF8MB4_BIN);                  \
                                             res_type.set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);   \
                                             str_op_object.set_result_type(res_type);  \
                                             int err = str_op_object.func(r, t1, t2, t3, expr_ctx); \
                                                        _OB_LOG(INFO, "ref=%s r=%s", to_cstring(ref), to_cstring(r)); \
                                             EXPECT_TRUE(OB_SUCCESS == err); \
                                             EXPECT_TRUE(ref.get_type() == r.get_type()); \
                                             r.set_collation_type(CS_TYPE_UTF8MB4_BIN);   \
                                              if (ref.get_type() != ObNullType) \
                                             { \
                                               EXPECT_TRUE(ObObjCmpFuncs::compare_oper_nullsafe(r, ref, CS_TYPE_UTF8MB4_BIN, CO_EQ));\
                                             } \
                                            } while(0)
#define EXPECT_FAIL_RESULT4(str_op_object, str_buf, func, type1, v1, type2, v2, type3, v3, type4, v4, ref_type, ref_value) \
                                             do {                          \
                                               ObObj t1;            \
                                               ObObj t2;            \
                                               ObObj t3;            \
                                               ObObj t4;            \
                                               ObObj r;             \
                                               ObObj ref;           \
                                               t1.set_##type1(v1);      \
                                               t2.set_##type2(v2);    \
                                               t3.set_##type3(v3);  \
                                               t4.set_##type4(v4);  \
                                               ref.set_##ref_type(ref_value); \
                                               ObExprCtx expr_ctx(NULL, NULL, NULL, str_buf);            \
                                               int err = str_op_object.func(r, t1, t2, t3, t4, expr_ctx); \
                                               ASSERT_TRUE(OB_SUCCESS != err); \
                                             } while(0)

#define EXPECT_RESULT4(str_op_object, str_buf, func, type1, v1, type2, v2, type3, v3, type4, v4, ref_type, ref_value) \
                                            do {                           \
                                             ObObj t1;              \
                                             ObObj t2;              \
                                             ObObj t3;              \
                                             ObObj t4;              \
                                             ObObj r;               \
                                             ObObj ref;             \
                                             t1.set_collation_type(CS_TYPE_UTF8MB4_BIN);   \
                                             t1.set_##type1(v1);        \
                                             t2.set_##type2(v2);        \
                                             t3.set_##type3(v3);        \
                                             t4.set_##type4(v4);        \
                                             t2.set_collation_type(CS_TYPE_UTF8MB4_BIN);   \
                                             t3.set_collation_type(CS_TYPE_UTF8MB4_BIN);   \
                                             t4.set_collation_type(CS_TYPE_UTF8MB4_BIN);   \
                                             ref.set_##ref_type(ref_value); \
                                             ref.set_collation_type(CS_TYPE_UTF8MB4_BIN);   \
                                             r.set_collation_type(CS_TYPE_UTF8MB4_BIN);   \
                                             ObExprCtx expr_ctx(NULL, NULL, NULL, str_buf);            \
                                             ObExprResType res_type;                                  \
                                             res_type.set_collation_level(CS_LEVEL_EXPLICIT);         \
                                             res_type.set_collation_type(CS_TYPE_UTF8MB4_BIN);                  \
                                             res_type.set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);   \
                                             str_op_object.set_result_type(res_type);  \
                                             int err = str_op_object.func(r, t1, t2, t3, t4, expr_ctx); \
                                                        _OB_LOG(INFO, "ref=%s r=%s", to_cstring(ref), to_cstring(r)); \
                                             EXPECT_TRUE(OB_SUCCESS == err); \
                                             EXPECT_TRUE(ref.get_type() == r.get_type()); \
                                             r.set_collation_type(CS_TYPE_UTF8MB4_BIN);   \
                                              if (ref.get_type() != ObNullType) \
                                             { \
                                                EXPECT_TRUE(ObObjCmpFuncs::compare_oper_nullsafe(r, ref, CS_TYPE_UTF8MB4_BIN, CO_EQ));\
                                             } \
                                            } while(0)
#define EXPECT_RESULTN(obj, str_buf, func, num,ref_type, ref_value,...) \
                                            {                           \
                                             ObObj params[num];              \
                                             ObObj ref;             \
                                             ObObj res;\
                                             LOGIC_GENPARAM##num(params, __VA_ARGS__);   \
                                             ref.set_##ref_type(ref_value);\
                                             ObExprCtx expr_ctx(NULL, NULL, NULL, str_buf);            \
                                             int err = obj.func(res,params, num, expr_ctx); \
                                             _OB_LOG(INFO, "ref=%s res=%s ret=%d", to_cstring(ref), to_cstring(res),err); \
                                             _OB_LOG(INFO, "%ld, %ld", ref.get_data_length(), res.get_data_length()); \
                                             EXPECT_TRUE(OB_SUCCESS == err); \
                                             EXPECT_TRUE(ref.get_type() == res.get_type()); \
                                             if (ref.get_type() != ObNullType) \
                                             { \
                                               EXPECT_TRUE(ObObjCmpFuncs::compare_oper_nullsafe(res, ref, CS_TYPE_UTF8MB4_BIN, CO_EQ));\
                                             } \
                                            } while(0)

#define EXPECT_RESULTN_NULL(obj, str_buf, func, num,ref_type, ref_value,...) \
                                            do {                           \
                                             ObObj params[num + 1];              \
                                             ObObj ref;             \
                                             ObObj res;\
                                             LOGIC_GENPARAM##num(params, __VA_ARGS__);   \
                                             params[num + 1].set_null();\
                                             ref.set_##ref_type(ref_value);\
                                              ref.set_null();\
                                             ObExprCtx expr_ctx(NULL, NULL, NULL, str_buf);            \
                                             int err = obj.func(res,params, num+1, expr_ctx); \
                                             _OB_LOG(INFO, "ref=%s res=%s ret=%d", to_cstring(ref), to_cstring(res),err); \
                                             EXPECT_TRUE(OB_SUCCESS == err); \
                                             EXPECT_TRUE(ref.get_type() == res.get_type()); \
                                             if (ref.get_type() != ObNullType) \
                                             { \
                                               EXPECT_TRUE(ObObjCmpFuncs::compare_oper_nullsafe(res, ref, CS_TYPE_UTF8MB4_BIN, CO_EQ));\
                                             } \
                                            } while(0)


#define EXPECT_FAIL_RESULTN(obj, str_buf, func, num,...) \
                                            do {                           \
                                             ObObj params[num];              \
                                             ObObj res;               \
                                             ObObj ref;\
                                             LOGIC_GENPARAM##num(params, __VA_ARGS__);   \
                                             ObExprCtx expr_ctx(NULL, NULL, NULL, str_buf);            \
                                             int err = obj.func(res, params, num, expr_ctx);  \
                                             ASSERT_TRUE(OB_SUCCESS != err); \
                                             } while(0)

#define EXPECT_RESULTN_TO_INNER(obj, str_buf, func, is_null, is_to_str, num,ref_type, ref_value,...) \
                                            do {                           \
                                              ObObj params[num];              \
                                              ObObj ref;             \
                                              ObObj res;\
                                              LOGIC_GENPARAM##num(params, __VA_ARGS__);   \
                                              ObArray<ObString> str_values;\
                                              int ret = OB_SUCCESS;\
                                              for (int64_t i = 0; OB_SUCCESS == ret && i < num - 2; ++i) \
                                              { \
                                                ObString str;\
                                                ret = params[i].get_varchar(str); \
                                                EXPECT_TRUE(OB_SUCCESS == ret); \
                                                ret = str_values.push_back(str);\
                                                EXPECT_TRUE(OB_SUCCESS == ret); \
                                              }\
                                              ret = obj.shallow_copy_str_values(str_values); \
                                              EXPECT_TRUE(OB_SUCCESS == ret); \
                                              ObString refer_string(ref_value);\
                                              ObEnumSetInnerValue inner_value(params[num-1].get_uint64(), refer_string);\
                                              char local_buf[1024] = {0};\
                                              int64_t pos = 0;\
                                              if (is_null) {\
                                                params[num-1].set_null();\
                                              } else if (is_to_str) {\
                                                ref.set_##ref_type(refer_string);\
                                                ref.set_collation_type(CS_TYPE_UTF8MB4_BIN);\
                                              } else {\
                                                ret = inner_value.serialize(local_buf, 1024, pos);\
                                                EXPECT_TRUE(OB_SUCCESS == ret); \
                                                ref.set_##ref_type(local_buf, (ObString::obstr_size_t)pos);\
                                                ref.set_collation_type(CS_TYPE_UTF8MB4_BIN);\
                                              }\
                                              ObExprCtx expr_ctx(NULL, NULL, NULL, str_buf);            \
                                              ret = obj.func(res, params[num - 2], params[num - 1], expr_ctx); \
                                              EXPECT_TRUE(OB_SUCCESS == ret); \
                                              ObString ref_string = ref.get_varchar();\
                                              ObString res_string = res.get_varchar();\
                                              _OB_LOG(INFO, "%ld, %ld", ref.get_data_length(), res.get_data_length()); \
                                              _OB_LOG(INFO, "ref: %.*s result: %.*s", ref_string.length(), ref_string.ptr(), res_string.length(),  res_string.ptr()); \
                                              if (true == is_null) {\
                                                EXPECT_TRUE(ObNullType == res.get_type()); \
                                                _OB_LOG(INFO, "out_value is NULL" ); \
                                              } else if (is_to_str){ \
                                                EXPECT_TRUE(ref.get_type() == res.get_type()); \
                                                if (ref.get_type() != ObNullType) \
                                                { \
                                                  EXPECT_TRUE(ObObjCmpFuncs::compare_oper_nullsafe(res, ref, CS_TYPE_UTF8MB4_BIN, CO_EQ));\
                                                }\
                                              } else {\
                                                ObEnumSetInnerValue out_value;\
                                                pos = 0; \
                                                ret = out_value.deserialize(res_string.ptr(), res_string.length(), pos);\
                                                _OB_LOG(INFO, "out_value:%s", to_cstring(out_value)); \
                                                EXPECT_TRUE(OB_SUCCESS == ret); \
                                                EXPECT_TRUE(ref.get_type() == res.get_type()); \
                                                EXPECT_TRUE(ref.get_varchar() == res.get_varchar()); \
                                                if (ref.get_type() != ObNullType) \
                                                { \
                                                  EXPECT_TRUE(ObObjCmpFuncs::compare_oper_nullsafe(res, ref, CS_TYPE_UTF8MB4_BIN, CO_EQ));\
                                                } \
                                              }\
                                            } while(0)


#define EXPECT_RESULT2_UTF8MB4CI(str_op_object, str_buf, func, type1, v1, type2, v2,ref_type, ref_value) \
                                        do {                               \
                                          ObObj t1;                 \
                                          ObObj t2;                 \
                                          ObObj r;                  \
                                          ObObj ref;                \
                                          t1.set_##type1(v1);           \
                                          t1.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);\
                                          t2.set_##type2(v2);         \
                                          t2.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);\
                                          ref.set_##ref_type(ref_value); \
                                          ref.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);\
                                          ObExprCtx expr_ctx(NULL, NULL, NULL, str_buf);            \
                                          int err = str_op_object.func(r, t1, t2, expr_ctx); \
                                          _OB_LOG(INFO, "ref=%s r=%s", to_cstring(ref), to_cstring(r)); \
                                          EXPECT_TRUE(OB_SUCCESS == err); \
                                          EXPECT_TRUE(ref.get_type() == r.get_type()); \
                                          if (ref.get_type() != ObNullType) \
                                          { \
                                            EXPECT_TRUE(ObObjCmpFuncs::compare_oper_nullsafe(ref, r, CS_TYPE_UTF8MB4_GENERAL_CI, CO_EQ));\
                                          } \
                                        } while(0)


OB_INLINE int64_t trunc_double(const double &v, int64_t p, int64_t d)
{
  double shift = v;
  int64_t revert = shift > 0 ? 1 : -1;
  shift *= static_cast<double>(revert);
  shift *= pow(static_cast<double>(10), static_cast<double>(p - d - 1));
  return static_cast<int64_t>(shift) * revert;
}

double fabs(double x);

OB_INLINE int64_t find_effective_start(double v)
{
  double d = 0;
  if (fabs(v - 0) < 0.000000000000001) {
    d = 0;
  } else {
    d = floor(log(v) / log(10));
  }
  return static_cast<int64_t>(d);
}

#define DOUBLE_CMP_MAX_P	14

OB_INLINE int64_t double_cmp_given_precision(double v1, double v2, int p)
{
  int64_t d1 = 0;
  int64_t d2 = 0;
  int64_t d = 0;
  int64_t revert = 1;
  int64_t res = 0;
  double temp1 = 0;
  double temp2 = 0;
  int64_t e1 = 0;
  int64_t e2 = 0;
  if (v1 > 0 && v2 < 0)
    res = 1;
  else if (v1 < 0 && v2 > 0)
    res = -1;
  else {
    revert = v1 < 0 ? -1 : 1;
    temp1 = static_cast<double>(revert) * v1;
    temp2 = static_cast<double>(revert) * v2;
    d1 = find_effective_start(temp1);
    d2 = find_effective_start(temp2);
    d = d1 > d2 ? d1 : d2;
    e1 = trunc_double(temp1, p + 1, d); //assure LSBs of EFN are the same,next digit can be different
    e2 = trunc_double(temp2, p + 1, d);
    int64_t delta = 0;
    delta = e1 > e2 ? (e1 - e2) : (e2 - e1);
    if (delta < 10) {
      res = 0;
    } else if (e1 > e2) {
      res = 1;
    } else if (e1 < e2) {
      res = -1;
    }
    res = res * revert;
  }
  return res;
}

#define ARITH_EXPECT_OBJ_DOUBLE(cmp_op, str_buf, func, type1, v1, type2, v2, rv, p) \
do {                                                     \
  ObObj t1;                                           \
  ObObj t2;                                           \
  ObObj vres;                                         \
  cmp_op op(g_alloc_);                                          \
  t1.set_##type1(v1);                                 \
  t1.set_collation_type(CS_TYPE_UTF8MB4_BIN);         \
  t2.set_##type2(v2);                                 \
  t2.set_collation_type(CS_TYPE_UTF8MB4_BIN);         \
  int ret = op.func(vres, t1, t2, str_buf, -1);          \
  EXPECT_TRUE(OB_SUCC(ret));                     \
  ObObj res;                                          \
  EXPECT_TRUE(vres.is_double()==true);    				\
  EXPECT_TRUE(double_cmp_given_precision(vres.get_double(),rv,p) == 0);\
} while(0)

#define EXPECT_NULL1(obj, str_buf, func)        \
do {                                               \
  ObObj ob;                                     \
  ObObj res;                                    \
  ob.set_null();                                \
  ObExprCtx expr_ctx(NULL, NULL, NULL, str_buf);\
  int err = obj.func(res, ob, expr_ctx);        \
  EXPECT_TRUE(OB_SUCCESS == err);               \
  ASSERT_TRUE(res.get_type() == ObNullType);    \
} while(0)

#define EXPECT_BUF_NOT_INIT1(obj, str_buf, func, t1, v1) \
do {                                               \
  ObObj ob;                                     \
  ObObj res;                                    \
  ob.set_##t1(v1);                               \
  ObExprCtx expr_ctx(NULL, NULL, NULL, NULL);   \
  int err = obj.func(res, ob, expr_ctx);        \
  ASSERT_TRUE(OB_NOT_INIT == err);              \
} while(0)

#define EXPECT_ALLOCATE_MEMORY_FAILED1(obj, str_buf, func, t1, v1)    \
do {                                               \
  ObObj ob;                                     \
  ObObj res;                                    \
  ob.set_##t1(v1);                              \
  ObExprCtx expr_ctx(NULL, NULL, NULL, str_buf);\
  int err = obj.func(res, ob, expr_ctx);        \
  ASSERT_TRUE(OB_ALLOCATE_MEMORY_FAILED == err);\
} while(0)

inline oceanbase::common::number::ObNumber TO_NMB(const char *str)
{
  oceanbase::common::number::ObNumber nmb;
  static oceanbase::common::CharArena s_allocator;
  nmb.from(str, s_allocator);
  return nmb;
}

#define FLOAT_MAX 3.40e+38
#define DOUBLE_MAX 1.79e+308

#endif /* _OB_EXPR_TEST_UTILS_H */
