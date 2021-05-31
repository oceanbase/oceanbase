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

#ifndef _OB_MACRO_UTILS_H_
#define _OB_MACRO_UTILS_H_

#define SELECT100_(_0, \
    _1,                \
    _2,                \
    _3,                \
    _4,                \
    _5,                \
    _6,                \
    _7,                \
    _8,                \
    _9,                \
    _10,               \
    _11,               \
    _12,               \
    _13,               \
    _14,               \
    _15,               \
    _16,               \
    _17,               \
    _18,               \
    _19,               \
    _20,               \
    _21,               \
    _22,               \
    _23,               \
    _24,               \
    _25,               \
    _26,               \
    _27,               \
    _28,               \
    _29,               \
    _30,               \
    _31,               \
    _32,               \
    _33,               \
    _34,               \
    _35,               \
    _36,               \
    _37,               \
    _38,               \
    _39,               \
    _40,               \
    _41,               \
    _42,               \
    _43,               \
    _44,               \
    _45,               \
    _46,               \
    _47,               \
    _48,               \
    _49,               \
    _50,               \
    _51,               \
    _52,               \
    _53,               \
    _54,               \
    _55,               \
    _56,               \
    _57,               \
    _58,               \
    _59,               \
    _60,               \
    _61,               \
    _62,               \
    _63,               \
    _64,               \
    _65,               \
    _66,               \
    _67,               \
    _68,               \
    _69,               \
    _70,               \
    _71,               \
    _72,               \
    _73,               \
    _74,               \
    _75,               \
    _76,               \
    _77,               \
    _78,               \
    _79,               \
    _80,               \
    _81,               \
    _82,               \
    _83,               \
    _84,               \
    _85,               \
    _86,               \
    _87,               \
    _88,               \
    _89,               \
    _90,               \
    _91,               \
    _92,               \
    _93,               \
    _94,               \
    _95,               \
    _96,               \
    _97,               \
    _98,               \
    _99,               \
    _100,              \
    ...)               \
  _100

#define _SELECT100_(_0, \
    _1,                 \
    _2,                 \
    _3,                 \
    _4,                 \
    _5,                 \
    _6,                 \
    _7,                 \
    _8,                 \
    _9,                 \
    _10,                \
    _11,                \
    _12,                \
    _13,                \
    _14,                \
    _15,                \
    _16,                \
    _17,                \
    _18,                \
    _19,                \
    _20,                \
    _21,                \
    _22,                \
    _23,                \
    _24,                \
    _25,                \
    _26,                \
    _27,                \
    _28,                \
    _29,                \
    _30,                \
    _31,                \
    _32,                \
    _33,                \
    _34,                \
    _35,                \
    _36,                \
    _37,                \
    _38,                \
    _39,                \
    _40,                \
    _41,                \
    _42,                \
    _43,                \
    _44,                \
    _45,                \
    _46,                \
    _47,                \
    _48,                \
    _49,                \
    _50,                \
    _51,                \
    _52,                \
    _53,                \
    _54,                \
    _55,                \
    _56,                \
    _57,                \
    _58,                \
    _59,                \
    _60,                \
    _61,                \
    _62,                \
    _63,                \
    _64,                \
    _65,                \
    _66,                \
    _67,                \
    _68,                \
    _69,                \
    _70,                \
    _71,                \
    _72,                \
    _73,                \
    _74,                \
    _75,                \
    _76,                \
    _77,                \
    _78,                \
    _79,                \
    _80,                \
    _81,                \
    _82,                \
    _83,                \
    _84,                \
    _85,                \
    _86,                \
    _87,                \
    _88,                \
    _89,                \
    _90,                \
    _91,                \
    _92,                \
    _93,                \
    _94,                \
    _95,                \
    _96,                \
    _97,                \
    _98,                \
    _99,                \
    _100,               \
    ...)                \
  _100

// These two macros do same work that select the 100th argument from
// argument list.
#define _SELECT100(...) _SELECT100_(__VA_ARGS__)
#define SELECT100(...) SELECT100_(__VA_ARGS__)

// Expand to the number of arguments
#define ARGS_NUM(args...) \
  SELECT100(,             \
      ##args,             \
      99,                 \
      98,                 \
      97,                 \
      96,                 \
      95,                 \
      94,                 \
      93,                 \
      92,                 \
      91,                 \
      90,                 \
      89,                 \
      88,                 \
      87,                 \
      86,                 \
      85,                 \
      84,                 \
      83,                 \
      82,                 \
      81,                 \
      80,                 \
      79,                 \
      78,                 \
      77,                 \
      76,                 \
      75,                 \
      74,                 \
      73,                 \
      72,                 \
      71,                 \
      70,                 \
      69,                 \
      68,                 \
      67,                 \
      66,                 \
      65,                 \
      64,                 \
      63,                 \
      62,                 \
      61,                 \
      60,                 \
      59,                 \
      58,                 \
      57,                 \
      56,                 \
      55,                 \
      54,                 \
      53,                 \
      52,                 \
      51,                 \
      50,                 \
      49,                 \
      48,                 \
      47,                 \
      46,                 \
      45,                 \
      44,                 \
      43,                 \
      42,                 \
      41,                 \
      40,                 \
      39,                 \
      38,                 \
      37,                 \
      36,                 \
      35,                 \
      34,                 \
      33,                 \
      32,                 \
      31,                 \
      30,                 \
      29,                 \
      28,                 \
      27,                 \
      26,                 \
      25,                 \
      24,                 \
      23,                 \
      22,                 \
      21,                 \
      20,                 \
      19,                 \
      18,                 \
      17,                 \
      16,                 \
      15,                 \
      14,                 \
      13,                 \
      12,                 \
      11,                 \
      10,                 \
      9,                  \
      8,                  \
      7,                  \
      6,                  \
      5,                  \
      4,                  \
      3,                  \
      2,                  \
      1,                  \
      0)

// SELF expands to the argument itself, special processing for comma
// since GCC macro would treat comma in argument list as a separator
// between argument, Hence macro SELF accept two arguments and then
// expand to a single comma and the concrete name of these two
// arguments.
#define SELF_2(...) ,
#define SELF_1(x) x
#define SELECT3(_1, _2, _3, ...) _3
#define SELF(args...) SELECT3(args, SELF_2, SELF_1)(args)

// ignore all arguments
#define IGNORE(...)
#define IGNORE_(...)

#define CONCAT_(x, y) x##y
#define CONCAT(x, y) CONCAT_(x, y)
// same as CONCAT defined above, just avoid self referential macros
#define _CONCAT_(x, y) x##y
#define _CONCAT(x, y) _CONCAT_(x, y)

// make that many duplicates of X, X should be surrounded by parentheses.
//
// DUP1((=)) => =
// DUP2((,)) => ,,
// DUP5((a++;)) => a++;a++;a++;a++;a++;
//
#define DUP1(X) SELF X
#define DUP2(X) SELF X DUP1(X)
#define DUP3(X) SELF X DUP2(X)
#define DUP4(X) SELF X DUP3(X)
#define DUP5(X) SELF X DUP4(X)
#define DUP6(X) SELF X DUP5(X)
#define DUP7(X) SELF X DUP6(X)
#define DUP8(X) SELF X DUP7(X)
#define DUP9(X) SELF X DUP8(X)
#define DUP10(X) SELF X DUP9(X)
#define DUP11(X) SELF X DUP10(X)
#define DUP12(X) SELF X DUP11(X)
#define DUP13(X) SELF X DUP12(X)
#define DUP14(X) SELF X DUP13(X)
#define DUP15(X) SELF X DUP14(X)
#define DUP16(X) SELF X DUP15(X)
#define DUP17(X) SELF X DUP16(X)
#define DUP18(X) SELF X DUP17(X)
#define DUP19(X) SELF X DUP18(X)
#define DUP20(X) SELF X DUP19(X)
#define DUP21(X) SELF X DUP20(X)
#define DUP22(X) SELF X DUP21(X)
#define DUP23(X) SELF X DUP22(X)
#define DUP24(X) SELF X DUP23(X)
#define DUP25(X) SELF X DUP24(X)
#define DUP26(X) SELF X DUP25(X)
#define DUP27(X) SELF X DUP26(X)
#define DUP28(X) SELF X DUP27(X)
#define DUP29(X) SELF X DUP28(X)
#define DUP30(X) SELF X DUP29(X)
#define DUP31(X) SELF X DUP30(X)
#define DUP32(X) SELF X DUP31(X)
#define DUP33(X) SELF X DUP32(X)
#define DUP34(X) SELF X DUP33(X)
#define DUP35(X) SELF X DUP34(X)
#define DUP36(X) SELF X DUP35(X)
#define DUP37(X) SELF X DUP36(X)
#define DUP38(X) SELF X DUP37(X)
#define DUP39(X) SELF X DUP38(X)
#define DUP40(X) SELF X DUP39(X)
#define DUP41(X) SELF X DUP40(X)
#define DUP42(X) SELF X DUP41(X)
#define DUP43(X) SELF X DUP42(X)
#define DUP44(X) SELF X DUP43(X)
#define DUP45(X) SELF X DUP44(X)
#define DUP46(X) SELF X DUP45(X)
#define DUP47(X) SELF X DUP46(X)
#define DUP48(X) SELF X DUP47(X)
#define DUP49(X) SELF X DUP48(X)
#define DUP50(X) SELF X DUP49(X)
#define DUP51(X) SELF X DUP50(X)
#define DUP52(X) SELF X DUP51(X)
#define DUP53(X) SELF X DUP52(X)
#define DUP54(X) SELF X DUP53(X)
#define DUP55(X) SELF X DUP54(X)
#define DUP56(X) SELF X DUP55(X)
#define DUP57(X) SELF X DUP56(X)
#define DUP58(X) SELF X DUP57(X)
#define DUP59(X) SELF X DUP58(X)
#define DUP60(X) SELF X DUP59(X)
#define DUP61(X) SELF X DUP60(X)
#define DUP62(X) SELF X DUP61(X)
#define DUP63(X) SELF X DUP62(X)
#define DUP64(X) SELF X DUP63(X)
#define DUP65(X) SELF X DUP64(X)
#define DUP66(X) SELF X DUP65(X)
#define DUP67(X) SELF X DUP66(X)
#define DUP68(X) SELF X DUP67(X)
#define DUP69(X) SELF X DUP68(X)
#define DUP70(X) SELF X DUP69(X)
#define DUP71(X) SELF X DUP70(X)
#define DUP72(X) SELF X DUP71(X)
#define DUP73(X) SELF X DUP72(X)
#define DUP74(X) SELF X DUP73(X)
#define DUP75(X) SELF X DUP74(X)
#define DUP76(X) SELF X DUP75(X)
#define DUP77(X) SELF X DUP76(X)
#define DUP78(X) SELF X DUP77(X)
#define DUP79(X) SELF X DUP78(X)
#define DUP80(X) SELF X DUP79(X)
#define DUP81(X) SELF X DUP80(X)
#define DUP82(X) SELF X DUP81(X)
#define DUP83(X) SELF X DUP82(X)
#define DUP84(X) SELF X DUP83(X)
#define DUP85(X) SELF X DUP84(X)
#define DUP86(X) SELF X DUP85(X)
#define DUP87(X) SELF X DUP86(X)
#define DUP88(X) SELF X DUP87(X)
#define DUP89(X) SELF X DUP88(X)
#define DUP90(X) SELF X DUP89(X)
#define DUP91(X) SELF X DUP90(X)
#define DUP92(X) SELF X DUP91(X)
#define DUP93(X) SELF X DUP92(X)
#define DUP94(X) SELF X DUP93(X)
#define DUP95(X) SELF X DUP94(X)
#define DUP96(X) SELF X DUP95(X)
#define DUP97(X) SELF X DUP96(X)
#define DUP98(X) SELF X DUP97(X)
#define DUP99(X) SELF X DUP98(X)

#define DUP_(n, X) DUP##n(X)
#define DUP(n, X) DUP_(n, X)

// expand to the number of 100 minus n
#define COMP100(n)         \
  _SELECT100(DUP(n, (, )), \
      1,                   \
      2,                   \
      3,                   \
      4,                   \
      5,                   \
      6,                   \
      7,                   \
      8,                   \
      9,                   \
      10,                  \
      11,                  \
      12,                  \
      13,                  \
      14,                  \
      15,                  \
      16,                  \
      17,                  \
      18,                  \
      19,                  \
      20,                  \
      21,                  \
      22,                  \
      23,                  \
      24,                  \
      25,                  \
      26,                  \
      27,                  \
      28,                  \
      29,                  \
      30,                  \
      31,                  \
      32,                  \
      33,                  \
      34,                  \
      35,                  \
      36,                  \
      37,                  \
      38,                  \
      39,                  \
      40,                  \
      41,                  \
      42,                  \
      43,                  \
      44,                  \
      45,                  \
      46,                  \
      47,                  \
      48,                  \
      49,                  \
      50,                  \
      51,                  \
      52,                  \
      53,                  \
      54,                  \
      55,                  \
      56,                  \
      57,                  \
      58,                  \
      59,                  \
      60,                  \
      61,                  \
      62,                  \
      63,                  \
      64,                  \
      65,                  \
      66,                  \
      67,                  \
      68,                  \
      69,                  \
      70,                  \
      71,                  \
      72,                  \
      73,                  \
      74,                  \
      75,                  \
      76,                  \
      77,                  \
      78,                  \
      79,                  \
      80,                  \
      81,                  \
      82,                  \
      83,                  \
      84,                  \
      85,                  \
      86,                  \
      87,                  \
      88,                  \
      89,                  \
      90,                  \
      91,                  \
      92,                  \
      93,                  \
      94,                  \
      95,                  \
      96,                  \
      97,                  \
      98,                  \
      99)

#define LST_DO_0(...)
#define LST_DO_1(M, s, P, ...) P(M, 1, ##__VA_ARGS__)
#define LST_DO_2(M, s, P, ...) LST_DO_1(M, s, P, ##__VA_ARGS__) SELF s P(M, 2, ##__VA_ARGS__)
#define LST_DO_3(M, s, P, ...) LST_DO_2(M, s, P, ##__VA_ARGS__) SELF s P(M, 3, ##__VA_ARGS__)
#define LST_DO_4(M, s, P, ...) LST_DO_3(M, s, P, ##__VA_ARGS__) SELF s P(M, 4, ##__VA_ARGS__)
#define LST_DO_5(M, s, P, ...) LST_DO_4(M, s, P, ##__VA_ARGS__) SELF s P(M, 5, ##__VA_ARGS__)
#define LST_DO_6(M, s, P, ...) LST_DO_5(M, s, P, ##__VA_ARGS__) SELF s P(M, 6, ##__VA_ARGS__)
#define LST_DO_7(M, s, P, ...) LST_DO_6(M, s, P, ##__VA_ARGS__) SELF s P(M, 7, ##__VA_ARGS__)
#define LST_DO_8(M, s, P, ...) LST_DO_7(M, s, P, ##__VA_ARGS__) SELF s P(M, 8, ##__VA_ARGS__)
#define LST_DO_9(M, s, P, ...) LST_DO_8(M, s, P, ##__VA_ARGS__) SELF s P(M, 9, ##__VA_ARGS__)
#define LST_DO_10(M, s, P, ...) LST_DO_9(M, s, P, ##__VA_ARGS__) SELF s P(M, 10, ##__VA_ARGS__)
#define LST_DO_11(M, s, P, ...) LST_DO_10(M, s, P, ##__VA_ARGS__) SELF s P(M, 11, ##__VA_ARGS__)
#define LST_DO_12(M, s, P, ...) LST_DO_11(M, s, P, ##__VA_ARGS__) SELF s P(M, 12, ##__VA_ARGS__)
#define LST_DO_13(M, s, P, ...) LST_DO_12(M, s, P, ##__VA_ARGS__) SELF s P(M, 13, ##__VA_ARGS__)
#define LST_DO_14(M, s, P, ...) LST_DO_13(M, s, P, ##__VA_ARGS__) SELF s P(M, 14, ##__VA_ARGS__)
#define LST_DO_15(M, s, P, ...) LST_DO_14(M, s, P, ##__VA_ARGS__) SELF s P(M, 15, ##__VA_ARGS__)
#define LST_DO_16(M, s, P, ...) LST_DO_15(M, s, P, ##__VA_ARGS__) SELF s P(M, 16, ##__VA_ARGS__)
#define LST_DO_17(M, s, P, ...) LST_DO_16(M, s, P, ##__VA_ARGS__) SELF s P(M, 17, ##__VA_ARGS__)
#define LST_DO_18(M, s, P, ...) LST_DO_17(M, s, P, ##__VA_ARGS__) SELF s P(M, 18, ##__VA_ARGS__)
#define LST_DO_19(M, s, P, ...) LST_DO_18(M, s, P, ##__VA_ARGS__) SELF s P(M, 19, ##__VA_ARGS__)
#define LST_DO_20(M, s, P, ...) LST_DO_19(M, s, P, ##__VA_ARGS__) SELF s P(M, 20, ##__VA_ARGS__)
#define LST_DO_21(M, s, P, ...) LST_DO_20(M, s, P, ##__VA_ARGS__) SELF s P(M, 21, ##__VA_ARGS__)
#define LST_DO_22(M, s, P, ...) LST_DO_21(M, s, P, ##__VA_ARGS__) SELF s P(M, 22, ##__VA_ARGS__)
#define LST_DO_23(M, s, P, ...) LST_DO_22(M, s, P, ##__VA_ARGS__) SELF s P(M, 23, ##__VA_ARGS__)
#define LST_DO_24(M, s, P, ...) LST_DO_23(M, s, P, ##__VA_ARGS__) SELF s P(M, 24, ##__VA_ARGS__)
#define LST_DO_25(M, s, P, ...) LST_DO_24(M, s, P, ##__VA_ARGS__) SELF s P(M, 25, ##__VA_ARGS__)
#define LST_DO_26(M, s, P, ...) LST_DO_25(M, s, P, ##__VA_ARGS__) SELF s P(M, 26, ##__VA_ARGS__)
#define LST_DO_27(M, s, P, ...) LST_DO_26(M, s, P, ##__VA_ARGS__) SELF s P(M, 27, ##__VA_ARGS__)
#define LST_DO_28(M, s, P, ...) LST_DO_27(M, s, P, ##__VA_ARGS__) SELF s P(M, 28, ##__VA_ARGS__)
#define LST_DO_29(M, s, P, ...) LST_DO_28(M, s, P, ##__VA_ARGS__) SELF s P(M, 29, ##__VA_ARGS__)
#define LST_DO_30(M, s, P, ...) LST_DO_29(M, s, P, ##__VA_ARGS__) SELF s P(M, 30, ##__VA_ARGS__)
#define LST_DO_31(M, s, P, ...) LST_DO_30(M, s, P, ##__VA_ARGS__) SELF s P(M, 31, ##__VA_ARGS__)
#define LST_DO_32(M, s, P, ...) LST_DO_31(M, s, P, ##__VA_ARGS__) SELF s P(M, 32, ##__VA_ARGS__)
#define LST_DO_33(M, s, P, ...) LST_DO_32(M, s, P, ##__VA_ARGS__) SELF s P(M, 33, ##__VA_ARGS__)
#define LST_DO_34(M, s, P, ...) LST_DO_33(M, s, P, ##__VA_ARGS__) SELF s P(M, 34, ##__VA_ARGS__)
#define LST_DO_35(M, s, P, ...) LST_DO_34(M, s, P, ##__VA_ARGS__) SELF s P(M, 35, ##__VA_ARGS__)
#define LST_DO_36(M, s, P, ...) LST_DO_35(M, s, P, ##__VA_ARGS__) SELF s P(M, 36, ##__VA_ARGS__)
#define LST_DO_37(M, s, P, ...) LST_DO_36(M, s, P, ##__VA_ARGS__) SELF s P(M, 37, ##__VA_ARGS__)
#define LST_DO_38(M, s, P, ...) LST_DO_37(M, s, P, ##__VA_ARGS__) SELF s P(M, 38, ##__VA_ARGS__)
#define LST_DO_39(M, s, P, ...) LST_DO_38(M, s, P, ##__VA_ARGS__) SELF s P(M, 39, ##__VA_ARGS__)
#define LST_DO_40(M, s, P, ...) LST_DO_39(M, s, P, ##__VA_ARGS__) SELF s P(M, 40, ##__VA_ARGS__)
#define LST_DO_41(M, s, P, ...) LST_DO_40(M, s, P, ##__VA_ARGS__) SELF s P(M, 41, ##__VA_ARGS__)
#define LST_DO_42(M, s, P, ...) LST_DO_41(M, s, P, ##__VA_ARGS__) SELF s P(M, 42, ##__VA_ARGS__)
#define LST_DO_43(M, s, P, ...) LST_DO_42(M, s, P, ##__VA_ARGS__) SELF s P(M, 43, ##__VA_ARGS__)
#define LST_DO_44(M, s, P, ...) LST_DO_43(M, s, P, ##__VA_ARGS__) SELF s P(M, 44, ##__VA_ARGS__)
#define LST_DO_45(M, s, P, ...) LST_DO_44(M, s, P, ##__VA_ARGS__) SELF s P(M, 45, ##__VA_ARGS__)
#define LST_DO_46(M, s, P, ...) LST_DO_45(M, s, P, ##__VA_ARGS__) SELF s P(M, 46, ##__VA_ARGS__)
#define LST_DO_47(M, s, P, ...) LST_DO_46(M, s, P, ##__VA_ARGS__) SELF s P(M, 47, ##__VA_ARGS__)
#define LST_DO_48(M, s, P, ...) LST_DO_47(M, s, P, ##__VA_ARGS__) SELF s P(M, 48, ##__VA_ARGS__)
#define LST_DO_49(M, s, P, ...) LST_DO_48(M, s, P, ##__VA_ARGS__) SELF s P(M, 49, ##__VA_ARGS__)
#define LST_DO_50(M, s, P, ...) LST_DO_49(M, s, P, ##__VA_ARGS__) SELF s P(M, 50, ##__VA_ARGS__)
#define LST_DO_51(M, s, P, ...) LST_DO_50(M, s, P, ##__VA_ARGS__) SELF s P(M, 51, ##__VA_ARGS__)
#define LST_DO_52(M, s, P, ...) LST_DO_51(M, s, P, ##__VA_ARGS__) SELF s P(M, 52, ##__VA_ARGS__)
#define LST_DO_53(M, s, P, ...) LST_DO_52(M, s, P, ##__VA_ARGS__) SELF s P(M, 53, ##__VA_ARGS__)
#define LST_DO_54(M, s, P, ...) LST_DO_53(M, s, P, ##__VA_ARGS__) SELF s P(M, 54, ##__VA_ARGS__)
#define LST_DO_55(M, s, P, ...) LST_DO_54(M, s, P, ##__VA_ARGS__) SELF s P(M, 55, ##__VA_ARGS__)
#define LST_DO_56(M, s, P, ...) LST_DO_55(M, s, P, ##__VA_ARGS__) SELF s P(M, 56, ##__VA_ARGS__)
#define LST_DO_57(M, s, P, ...) LST_DO_56(M, s, P, ##__VA_ARGS__) SELF s P(M, 57, ##__VA_ARGS__)
#define LST_DO_58(M, s, P, ...) LST_DO_57(M, s, P, ##__VA_ARGS__) SELF s P(M, 58, ##__VA_ARGS__)
#define LST_DO_59(M, s, P, ...) LST_DO_58(M, s, P, ##__VA_ARGS__) SELF s P(M, 59, ##__VA_ARGS__)
#define LST_DO_60(M, s, P, ...) LST_DO_59(M, s, P, ##__VA_ARGS__) SELF s P(M, 60, ##__VA_ARGS__)
#define LST_DO_61(M, s, P, ...) LST_DO_60(M, s, P, ##__VA_ARGS__) SELF s P(M, 61, ##__VA_ARGS__)
#define LST_DO_62(M, s, P, ...) LST_DO_61(M, s, P, ##__VA_ARGS__) SELF s P(M, 62, ##__VA_ARGS__)
#define LST_DO_63(M, s, P, ...) LST_DO_62(M, s, P, ##__VA_ARGS__) SELF s P(M, 63, ##__VA_ARGS__)
#define LST_DO_64(M, s, P, ...) LST_DO_63(M, s, P, ##__VA_ARGS__) SELF s P(M, 64, ##__VA_ARGS__)
#define LST_DO_65(M, s, P, ...) LST_DO_64(M, s, P, ##__VA_ARGS__) SELF s P(M, 65, ##__VA_ARGS__)
#define LST_DO_66(M, s, P, ...) LST_DO_65(M, s, P, ##__VA_ARGS__) SELF s P(M, 66, ##__VA_ARGS__)
#define LST_DO_67(M, s, P, ...) LST_DO_66(M, s, P, ##__VA_ARGS__) SELF s P(M, 67, ##__VA_ARGS__)
#define LST_DO_68(M, s, P, ...) LST_DO_67(M, s, P, ##__VA_ARGS__) SELF s P(M, 68, ##__VA_ARGS__)
#define LST_DO_69(M, s, P, ...) LST_DO_68(M, s, P, ##__VA_ARGS__) SELF s P(M, 69, ##__VA_ARGS__)
#define LST_DO_70(M, s, P, ...) LST_DO_69(M, s, P, ##__VA_ARGS__) SELF s P(M, 70, ##__VA_ARGS__)
#define LST_DO_71(M, s, P, ...) LST_DO_70(M, s, P, ##__VA_ARGS__) SELF s P(M, 71, ##__VA_ARGS__)
#define LST_DO_72(M, s, P, ...) LST_DO_71(M, s, P, ##__VA_ARGS__) SELF s P(M, 72, ##__VA_ARGS__)
#define LST_DO_73(M, s, P, ...) LST_DO_72(M, s, P, ##__VA_ARGS__) SELF s P(M, 73, ##__VA_ARGS__)
#define LST_DO_74(M, s, P, ...) LST_DO_73(M, s, P, ##__VA_ARGS__) SELF s P(M, 74, ##__VA_ARGS__)
#define LST_DO_75(M, s, P, ...) LST_DO_74(M, s, P, ##__VA_ARGS__) SELF s P(M, 75, ##__VA_ARGS__)
#define LST_DO_76(M, s, P, ...) LST_DO_75(M, s, P, ##__VA_ARGS__) SELF s P(M, 76, ##__VA_ARGS__)
#define LST_DO_77(M, s, P, ...) LST_DO_76(M, s, P, ##__VA_ARGS__) SELF s P(M, 77, ##__VA_ARGS__)
#define LST_DO_78(M, s, P, ...) LST_DO_77(M, s, P, ##__VA_ARGS__) SELF s P(M, 78, ##__VA_ARGS__)
#define LST_DO_79(M, s, P, ...) LST_DO_78(M, s, P, ##__VA_ARGS__) SELF s P(M, 79, ##__VA_ARGS__)
#define LST_DO_80(M, s, P, ...) LST_DO_79(M, s, P, ##__VA_ARGS__) SELF s P(M, 80, ##__VA_ARGS__)
#define LST_DO_81(M, s, P, ...) LST_DO_80(M, s, P, ##__VA_ARGS__) SELF s P(M, 81, ##__VA_ARGS__)
#define LST_DO_82(M, s, P, ...) LST_DO_81(M, s, P, ##__VA_ARGS__) SELF s P(M, 82, ##__VA_ARGS__)
#define LST_DO_83(M, s, P, ...) LST_DO_82(M, s, P, ##__VA_ARGS__) SELF s P(M, 83, ##__VA_ARGS__)
#define LST_DO_84(M, s, P, ...) LST_DO_83(M, s, P, ##__VA_ARGS__) SELF s P(M, 84, ##__VA_ARGS__)
#define LST_DO_85(M, s, P, ...) LST_DO_84(M, s, P, ##__VA_ARGS__) SELF s P(M, 85, ##__VA_ARGS__)
#define LST_DO_86(M, s, P, ...) LST_DO_85(M, s, P, ##__VA_ARGS__) SELF s P(M, 86, ##__VA_ARGS__)
#define LST_DO_87(M, s, P, ...) LST_DO_86(M, s, P, ##__VA_ARGS__) SELF s P(M, 87, ##__VA_ARGS__)
#define LST_DO_88(M, s, P, ...) LST_DO_87(M, s, P, ##__VA_ARGS__) SELF s P(M, 88, ##__VA_ARGS__)
#define LST_DO_89(M, s, P, ...) LST_DO_88(M, s, P, ##__VA_ARGS__) SELF s P(M, 89, ##__VA_ARGS__)
#define LST_DO_90(M, s, P, ...) LST_DO_89(M, s, P, ##__VA_ARGS__) SELF s P(M, 90, ##__VA_ARGS__)
#define LST_DO_91(M, s, P, ...) LST_DO_90(M, s, P, ##__VA_ARGS__) SELF s P(M, 91, ##__VA_ARGS__)
#define LST_DO_92(M, s, P, ...) LST_DO_91(M, s, P, ##__VA_ARGS__) SELF s P(M, 92, ##__VA_ARGS__)
#define LST_DO_93(M, s, P, ...) LST_DO_92(M, s, P, ##__VA_ARGS__) SELF s P(M, 93, ##__VA_ARGS__)
#define LST_DO_94(M, s, P, ...) LST_DO_93(M, s, P, ##__VA_ARGS__) SELF s P(M, 94, ##__VA_ARGS__)
#define LST_DO_95(M, s, P, ...) LST_DO_94(M, s, P, ##__VA_ARGS__) SELF s P(M, 95, ##__VA_ARGS__)
#define LST_DO_96(M, s, P, ...) LST_DO_95(M, s, P, ##__VA_ARGS__) SELF s P(M, 96, ##__VA_ARGS__)
#define LST_DO_97(M, s, P, ...) LST_DO_96(M, s, P, ##__VA_ARGS__) SELF s P(M, 97, ##__VA_ARGS__)
#define LST_DO_98(M, s, P, ...) LST_DO_97(M, s, P, ##__VA_ARGS__) SELF s P(M, 98, ##__VA_ARGS__)
#define LST_DO_99(M, s, P, ...) LST_DO_98(M, s, P, ##__VA_ARGS__) SELF s P(M, 99, ##__VA_ARGS__)
#define LST_DO_100(M, s, P, ...) LST_DO_99(M, s, P, ##__VA_ARGS__) SELF s P(M, 100, ##__VA_ARGS__)

#define LST_DO__(N, M, s, P, ...) LST_DO_##N(M, s, P, ##__VA_ARGS__)
#define LST_DO_(...) LST_DO__(__VA_ARGS__)
#define LST_DO(M, s, ...) LST_DO_(ARGS_NUM(__VA_ARGS__), M, s, PROC_ONE, ##__VA_ARGS__)
#define LST_DO2(M, s, ...) LST_DO_(ARGS_NUM(__VA_ARGS__), M, s, PROC_ONE2, ##__VA_ARGS__)

// select nth argument
//
// SELECT(2, arg1, arg2, arg3) => arg2
#define SELECT(n, ...) SELECT100(DUP(COMP100(n), (, )), __VA_ARGS__)

#define PROC_ONE(M, ...) M(SELECT(__VA_ARGS__))
#define PROC_ONE2(M, IDX, ...) M(SELECT(IDX, __VA_ARGS__), IDX)

// map M to each of other arguments
//
// LST_DO_CODE(DECODE, arg1, arg2, arg3) => DECODE(arg1); DECODE(arg2); DEOCDE(arg3)
// LST_DO_CODE(CHECK, arg1, arg2) => CHECK(arg1); CHECK(arg2)
#define LST_DO_CODE(M, ...) LST_DO(M, (;), ##__VA_ARGS__)

#define ONE_TO_HUNDRED                                                                                                \
  1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31,  \
      32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, \
      60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, \
      88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100

#define MSTR(X) #X

// THE EXCLUSIVE EXAMPLE
//
// if (FAILEDx(some_function)) {
//   blabalbal...
// }
//
#define FAILEDx(X) OB_FAIL(ret)) \
  {} else if (OB_FAIL(X)

// select macro depend on parameter which have parent surround with.
//
//   #define MACRO_PAREN(x) some_code_if_has_paren
//   #define MACRO_NO_PAREN(x) some_code_if_no_paren
//
//   IF_PARENT(XXX, MACRO_PAREN, MACRO_NO_PAREN) ==> MACRO_NO_PAREN(XXX)
//   IF_PARENT((XXX), MACRO_PAREN, MACRO_NO_PAREN) ==> MACRO_PAREN(XXX)
//
#define OBMPAR IGNORE(
#define PAR(x) _SELF(
#define OBM_SELF(x) x

#define PAREN(x, M) CONCAT(OBM, PAR x) M x)

#define OBMNPNPAR SELF(IGNORE(
#define OBMNP_SELF(x) SELF
#define NPAR(x) _IGNORE(
#define OBMNP_IGNORE(...) IGNORE(

#define NO_PAREN(x, M) CONCAT(OBMNP, NPAR x)) M (x))

#define IF_PAREN(x, MP, MNP) PAREN(x, MP) NO_PAREN(x, MNP)

#ifndef MIN
#define MIN(x, y) ((x) < (y) ? (x) : (y))
#endif

#define MIN3(x, y, z) MIN(MIN(x, y), z)

#ifndef MAX
#define MAX(x, y) ((x) > (y) ? (x) : (y))
#endif

#define MAX3(x, y, z) MAX(MAX(x, y), z)
// Deferred expression and recursion
#define CAT(a, ...) PRIMITIVE_CAT(a, __VA_ARGS__)
#define PRIMITIVE_CAT(a, ...) a##__VA_ARGS__

#define EMPTY()
#define INNER_DEFER(id) id EMPTY()
#define OBSTRUCT(...) __VA_ARGS__ INNER_DEFER(EMPTY)()
#define EXPAND(...) __VA_ARGS__

#define EVAL(...) EVAL1(EVAL1(EVAL1(__VA_ARGS__)))
#define EVAL1(...) EVAL2(EVAL2(EVAL2(__VA_ARGS__)))
#define EVAL2(...) EVAL3(EVAL3(EVAL3(__VA_ARGS__)))
#define EVAL3(...) EVAL4(EVAL4(EVAL4(__VA_ARGS__)))
#define EVAL4(...) EVAL5(EVAL5(EVAL5(__VA_ARGS__)))
#define EVAL5(...) __VA_ARGS__

////////////////////////////////////////////////////////////////
/**
 * There are four kinds of FOREACH macros:
 * 1. FOREACH
 * 2. FOREACH_REF
 * 3. ARRAY_FOREACH
 * 4. DLIST_FOREACH @see ob_dlist.h
 *
 */
// container iterate macro, support container with begin(), end() interface.
// (e.g.: ObArray, ObSEArray, ObHashTable, stl::vector ...)
#define FOREACH_X(it, container, extra_condition) \
  for (__typeof__((container).begin()) it = (container).begin(); (extra_condition) && (it != (container).end()); ++it)
#define FOREACH(it, container) FOREACH_X(it, (container), true)

// container iterate macro, support container with count(), at() interface.
// (e.g.: ObIArray)
#define __INNER_I_NAME__(it) __i__##it
#define __INNER_I__(it) (*reinterpret_cast<int64_t*>(&__INNER_I_NAME__(it)))
#define FOREACH_CNT_X(it, c, extra_condition)                                                 \
  for (__typeof__((c).at(0))*it = ((extra_condition) && (c).count() > 0 ? &(c).at(0) : NULL), \
      *__INNER_I_NAME__(it) = NULL;                                                           \
       (extra_condition) && __INNER_I__(it) < (c).count();                                    \
       ++__INNER_I__(it), it = (__INNER_I__(it) < (c).count() ? &(c).at(__INNER_I__(it)) : NULL))
#define FOREACH_CNT(it, c) FOREACH_CNT_X(it, c, true)

// array iterate macro, in contrast to FOREACH_CNT, these macros can access index variable in the loop body
#define ARRAY_FOREACH_X(array, idx, cnt, extra_condition) \
  for (int64_t(idx) = 0, (cnt) = (array).count(); (extra_condition) && (idx) != (cnt); ++(idx))
#define ARRAY_FOREACH_N(array, idx, cnt) ARRAY_FOREACH_X(array, idx, cnt, OB_SUCC(ret))
#define ARRAY_FOREACH_NORET(array, idx) ARRAY_FOREACH_X(array, idx, _NuM__ArrAy_, true)
#define ARRAY_FOREACH(array, idx) ARRAY_FOREACH_N(array, idx, _NuM__ArrAy_)
////////////////////////////////////////////////////////////////
/**
 * Macros to help define enum and it string convert functions.
 * Usage:
 *   in .h file:
 *
 *      #define MY_ENUM_DEF(ACT) \
 *         ACT(INVALID_MY_ENUM_VALUE, = 0) \
 *         ACT(VALUE1,) \
 *         ACT(VALUE2, = 1024) \
 *         ACT(VALUE3,)
 *
 *     DECLARE_ENUM(ObMyEnum, my_enum, MY_ENUM_DEF)
 *
 * This delcare will will be expand to:
 *
 *   enum ObMyEnum {
 *     VALUE1,
 *     VALUE2 = 1024,
 *     VALUE3,
 *   };
 *   const char *get_my_enum_string(const ObMyEnum v);
 *   ObMyEnum get_my_enum_value(const char *str);
 *   ObMyEnum get_my_enum_value(const common::ObString &str);
 *
 * get_my_enum_string() return NULL if %v not exist.
 * get_my_enum_value() return 0 if %str not an valid enum string. So it's a good idea to
 * define an invalid enum value of value 0 for error detection.
 *
 * And you need also define the functions in .cpp file:
 *
 *   in .cpp file:
 *
 *     define(ObMyEnum, my_enum, MY_ENUM_DEF)
 *
 */
#define DEF_ENUM_VALUE(name, assign) name assign,
#define DEF_ENUM_CASE(name, ...) \
  case name:                     \
    return #name;
#define DEF_ENUM_STRCMP(name, ...)  \
  if (0 == str.case_compare(#name)) \
    return name;
#define DECLARE_ENUM(type, func_name, def, ...)                   \
  enum type { def(DEF_ENUM_VALUE) };                              \
  __VA_ARGS__ const char* get_##func_name##_string(const type v); \
  __VA_ARGS__ type get_##func_name##_value(const char* str);      \
  __VA_ARGS__ type get_##func_name##_value(const common::ObString& str);

#define DEFINE_ENUM_FUNC(type, func_name, def, ...)                                    \
  const char* __VA_ARGS__ get_##func_name##_string(const type v)                       \
  {                                                                                    \
    switch (v) {                                                                       \
      def(DEF_ENUM_CASE) default : LIB_LOG(WARN, "unknown" #type "value", "value", v); \
      return NULL;                                                                     \
    }                                                                                  \
  }                                                                                    \
  type __VA_ARGS__ get_##func_name##_value(const char* str)                            \
  {                                                                                    \
    return get_##func_name##_value(common::ObString::make_string(str));                \
  }                                                                                    \
  type __VA_ARGS__ get_##func_name##_value(const common::ObString& str)                \
  {                                                                                    \
    if (str.empty()) {                                                                 \
      LIB_LOG(WARN, "invalid argument, empty str");                                    \
      return static_cast<type>(0);                                                     \
    } else {                                                                           \
      def(DEF_ENUM_STRCMP)                                                             \
    }                                                                                  \
    LIB_LOG(WARN, "unknown " #type "string", K(str));                                  \
    return static_cast<type>(0);                                                       \
  }

#ifdef __cplusplus
#define EXTERN_C_BEGIN extern "C" {
#define EXTERN_C_END }
#else
#define EXTERN_C_BEGIN
#define EXTERN_C_END
#endif  // __cplusplus

////////////////////////////////////////////////////////////////
// OceanBase Idiom
#ifndef UNUSED
#define UNUSED(v) ((void)(v))
#endif

#define IGNORE_RETURN (void)

#define UNUSEDx(...) LST_DO_CODE(UNUSED, __VA_ARGS__)

#define DISALLOW_COPY_AND_ASSIGN(TypeName) \
  TypeName(const TypeName&);               \
  void operator=(const TypeName&)

#define DISABLE_COPY_ASSIGN(ClassType)  \
  ClassType(const ClassType&) = delete; \
  ClassType& operator=(const ClassType&) = delete

#define OB_INLINE inline __attribute__((always_inline))

#if __x86_64__
#define CACHE_ALIGN_SIZE 64
#elif __aarch64__
#define CACHE_ALIGN_SIZE 128
#endif

#define CACHE_ALIGNED __attribute__((aligned(CACHE_ALIGN_SIZE)))

// OB_LIKELY and OB_UNLIKELY may be used in C++ and C code, so using !!1 and !!0 instead of true and false
#define OB_LIKELY(x) __builtin_expect(!!(x), !!1)
#define OB_UNLIKELY(x) __builtin_expect(!!(x), !!0)

#define ARRAYSIZEOF(a) static_cast<int64_t>(sizeof(a) / sizeof(a[0]))
#define SIZEOF(x) static_cast<int64_t>(sizeof(x))

#define CONTAINER_OF(ptr, type, member) ({ (type*)((char*)ptr - __builtin_offsetof(type, member)); })

#ifdef NDEBUG
// release mode
#define CHAR_CARRAY_INIT(a) a[0] = '\0'
#else
// debug mode
#define CHAR_CARRAY_INIT(a) memset((a), 0, sizeof(a))
#endif

#define OB_SUCC(statement) (OB_LIKELY(::oceanbase::common::OB_SUCCESS == (ret = (statement))))
#define OB_FAIL(statement) (OB_UNLIKELY(::oceanbase::common::OB_SUCCESS != (ret = (statement))))
#define COVER_SUCC(errcode) (OB_SUCCESS == ret ? errcode : ret)
#define INIT_SUCC(ret) int ret = ::oceanbase::common::OB_SUCCESS

#define OB_ISNULL(statement) (OB_UNLIKELY(NULL == (statement)))
#define OB_NOT_NULL(statement) (OB_LIKELY(NULL != (statement)))
#define IS_NOT_INIT (OB_UNLIKELY(!is_inited_))
#define IS_INIT (OB_LIKELY(is_inited_))

// Default retry OB_TIMEOUT error code
#define RETRY_FUNC(stop_flag, var, func, args...) RETRY_FUNC_ON_ERROR(OB_TIMEOUT, stop_flag, var, func, ##args)

#define RETRY_FUNC_ON_ERROR(err_no, stop_flag, var, func, args...) \
  do {                                                             \
    if (OB_SUCC(ret)) {                                            \
      ret = (err_no);                                              \
      while ((err_no) == ret && !(stop_flag)) {                    \
        ret = ::oceanbase::common::OB_SUCCESS;                     \
        ret = (var).func(args);                                    \
      }                                                            \
      if ((stop_flag)) {                                           \
        ret = OB_IN_STOP_STATE;                                    \
      }                                                            \
    }                                                              \
  } while (0)

////////////////////////////////////////////////////////////////
// assert utilities
#define BACKTRACE(LEVEL, cond, _fmt_, args...)                                 \
  do {                                                                         \
    if (cond) {                                                                \
      _OB_LOG(LEVEL, _fmt_ " BACKTRACE:%s", ##args, oceanbase::common::lbt()); \
    }                                                                          \
  } while (false)

#ifdef NDEBUG
#define OB_ASSERT(x) (void)(x)
#else
#define OB_ASSERT(x)                             \
  do {                                           \
    bool v = (x);                                \
    if (OB_UNLIKELY(!(v))) {                     \
      _OB_LOG(ERROR, "assert fail, exp=%s", #x); \
      BACKTRACE(ERROR, 1, "assert fail");        \
      assert(v);                                 \
    }                                            \
  } while (false)
#endif

#define OB_ASSERT_MSG(x, msg...)                 \
  do {                                           \
    bool v = (x);                                \
    if (OB_UNLIKELY(!(v))) {                     \
      _OB_LOG(ERROR, "assert fail, exp=%s", #x); \
      BACKTRACE(ERROR, 1, ##msg);                \
      assert(v);                                 \
    }                                            \
  } while (false)

#define ob_release_assert(x)                     \
  do {                                           \
    bool v = (x);                                \
    if (OB_UNLIKELY(!(v))) {                     \
      _OB_LOG(ERROR, "assert fail, exp=%s", #x); \
      BACKTRACE(ERROR, 1, "assert fail");        \
      ob_abort();                                \
      exit(1);                                   \
    }                                            \
  } while (false)

//#define ob_assert(x) OB_ASSERT(x)
#define ob_assert(x) ob_release_assert(x)
////////////////////////////////////////////////////////////////
// interval
#define REACH_TIME_INTERVAL(i)                                                                               \
  ({                                                                                                         \
    bool bret = false;                                                                                       \
    static volatile int64_t last_time = 0;                                                                   \
    int64_t cur_time = ::oceanbase::common::ObTimeUtility::fast_current_time();                              \
    int64_t old_time = last_time;                                                                            \
    if (OB_UNLIKELY((i + last_time) < cur_time) && old_time == ATOMIC_CAS(&last_time, old_time, cur_time)) { \
      bret = true;                                                                                           \
    }                                                                                                        \
    bret;                                                                                                    \
  })

// reach count per secound
#define REACH_COUNT_PER_SEC(i)                                                        \
  ({                                                                                  \
    bool bool_ret = false;                                                            \
    types::uint128_t tmp;                                                             \
    types::uint128_t next;                                                            \
    static const uint64_t ONE_SECOND = 1 * 1000 * 1000;                               \
    static types::uint128_t last;                                                     \
    const int64_t cur_time = ::oceanbase::common::ObTimeUtility::fast_current_time(); \
    while (true) {                                                                    \
      LOAD128(tmp, &last);                                                            \
      if (tmp.lo + ONE_SECOND > (uint64_t)cur_time) {                                 \
        next.hi = tmp.hi + 1;                                                         \
        next.lo = tmp.lo;                                                             \
        if (next.hi > (uint64_t)i) {                                                  \
          bool_ret = true;                                                            \
        }                                                                             \
      } else {                                                                        \
        next.lo = (uint64_t)cur_time;                                                 \
        next.hi = 1;                                                                  \
      }                                                                               \
      if (CAS128(&last, tmp, next)) {                                                 \
        break;                                                                        \
      }                                                                               \
    }                                                                                 \
    bool_ret;                                                                         \
  })
// exclusive first time
#define REACH_TIME_INTERVAL_RANGE(i, j)                                                          \
  ({                                                                                             \
    bool bret = false;                                                                           \
    static volatile int64_t last_time = ::oceanbase::common::ObTimeUtility::fast_current_time(); \
    int64_t cur_time = ::oceanbase::common::ObTimeUtility::fast_current_time();                  \
    int64_t old_time = last_time;                                                                \
    if ((j + last_time) < cur_time) {                                                            \
      (void)ATOMIC_CAS(&last_time, old_time, cur_time);                                          \
    }                                                                                            \
    old_time = last_time;                                                                        \
    if ((i + last_time) < cur_time && old_time == ATOMIC_CAS(&last_time, old_time, cur_time)) {  \
      bret = true;                                                                               \
    }                                                                                            \
    bret;                                                                                        \
  })

#define TC_REACH_TIME_INTERVAL(i)                                               \
  ({                                                                            \
    bool bret = false;                                                          \
    static __thread int64_t last_time = 0;                                      \
    int64_t cur_time = ::oceanbase::common::ObTimeUtility::fast_current_time(); \
    if (OB_UNLIKELY((i + last_time) < cur_time)) {                              \
      last_time = cur_time;                                                     \
      bret = true;                                                              \
    }                                                                           \
    bret;                                                                       \
  })

#define REACH_COUNT_INTERVAL(i)             \
  ({                                        \
    bool bret = false;                      \
    static volatile int64_t count = 0;      \
    if (0 == (ATOMIC_AAF(&count, 1) % i)) { \
      bret = true;                          \
    }                                       \
    bret;                                   \
  })

#define TC_REACH_COUNT_INTERVAL(i)     \
  ({                                   \
    bool bret = false;                 \
    static __thread int64_t count = 0; \
    if (0 == (++count % i)) {          \
      bret = true;                     \
    }                                  \
    bret;                              \
  })

#define EXECUTE_COUNT_PER_SEC(i)                                                      \
  ({                                                                                  \
    bool bool_ret = false;                                                            \
    types::uint128_t tmp;                                                             \
    types::uint128_t next;                                                            \
    static const uint64_t ONE_SECOND = 1 * 1000 * 1000;                               \
    static types::uint128_t last;                                                     \
    const int64_t cur_time = ::oceanbase::common::ObTimeUtility::fast_current_time(); \
    while (true) {                                                                    \
      LOAD128(tmp, &last);                                                            \
      if (tmp.lo + ONE_SECOND < (uint64_t)cur_time) {                                 \
        next.lo = (uint64_t)cur_time;                                                 \
        next.hi = 1;                                                                  \
      } else {                                                                        \
        next.lo = tmp.lo;                                                             \
        next.hi = tmp.hi + 1;                                                         \
      }                                                                               \
      if (next.hi <= i) {                                                             \
        bool_ret = true;                                                              \
      }                                                                               \
      if (CAS128(&last, tmp, next)) {                                                 \
        break;                                                                        \
      }                                                                               \
    }                                                                                 \
    bool_ret;                                                                         \
  })

#endif /* _OB_MACRO_UTILS_H_ */
