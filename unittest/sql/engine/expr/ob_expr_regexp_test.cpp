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
#include "sql/engine/expr/ob_expr_regexp.h"
#include "sql/engine/expr/ob_expr_not_regexp.h"
#include "ob_expr_test_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

class ObExprRegexpTest : public ::testing::Test
{
public:
  ObExprRegexpTest();
  virtual ~ObExprRegexpTest();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  ObExprRegexpTest(const ObExprRegexpTest &other);
  ObExprRegexpTest& operator=(const ObExprRegexpTest &other);
private:
  // data members
};
ObExprRegexpTest::ObExprRegexpTest()
{
}

ObExprRegexpTest::~ObExprRegexpTest()
{
}

void ObExprRegexpTest::SetUp()
{
}

void ObExprRegexpTest::TearDown()
{
}

#define T(obj, t1, v1, t2, v2, ref_type, ref_value) \
  do {EXPECT_RESULT2(obj, &buf, calc_result2, t1, v1, t2, v2, ref_type, ref_value); obj.reset();} while(0)
#define F(obj, t1, v1, t2, v2, ref_type, ref_value) \
  do {EXPECT_FAIL_RESULT2(obj, &buf, calc_result2, t1, v1, t2, v2, ref_type, ref_value); obj.reset();} while(0)

TEST_F(ObExprRegexpTest, basic_test)
{
  ObExprRegexp regexp;
  ObExprStringBuf buf;

  ASSERT_EQ(2, regexp.get_param_num());

  T(regexp,null, 0, null, 0, null, 0);
  T(regexp,null, 0, min, 0, null, 0);
  T(regexp,null, 0, max, 0, null, 0);
  T(regexp,min, 0, null, 0, null, 0);
  T(regexp,max, 0, null, 0, null, 0);
  T(regexp,int, 1, null, 0, null, 0);
  T(regexp,null, 0, int , 0, null, 0);

  T(regexp,varchar, "", varchar, "a", bool, false);

  T(regexp,varchar, "hi", varchar, "hi", bool, true);
  T(regexp,varchar, "hi", varchar, ".*i", bool, true);
  T(regexp,varchar, "hi", varchar, "h.*", bool, true);
  T(regexp,varchar, "hello", varchar, ".*1.*", bool, false);
  T(regexp,varchar, "hi", varchar, "i", bool, true);
  T(regexp,varchar, "hi", varchar, "xx<F12>", bool, false);
  T(regexp,varchar, "hello", varchar, "elo", bool, false);
  T(regexp,varchar, "hello", varchar, "ell", bool, true);

  T(regexp,varchar, "a", varchar, "a", bool, true);
  T(regexp,varchar, "abc", varchar, "abc", bool, true);
  T(regexp,varchar, "abc", varchar, "abc|de", bool, true);
  T(regexp,varchar, "abc", varchar, "a|b|c", bool, true);
  T(regexp,varchar, "abc", varchar, "a(b)c", bool, true);
  T(regexp,varchar, "a(", varchar, "a\\(", bool, true);
  T(regexp,varchar, "EPAREN", varchar, "a\\(", bool, false);
  T(regexp,varchar, "EPAREN", varchar, "a\\(b", bool, false);
  T(regexp,varchar, "EPAREN", varchar, "a\\)", bool, false);
  T(regexp,varchar, "EPAREN", varchar, "\\)", bool, false);
  T(regexp,varchar, "ab", varchar, "a()b", bool, true);
  T(regexp,varchar, "abc", varchar, "^abc$", bool, true);
  T(regexp,varchar, "a^b", varchar, "a^b", bool, false);
  T(regexp,varchar, "a$b", varchar, "a$b", bool, false);
  T(regexp,varchar, "ab\nc", varchar, "b$", bool, false);
  T(regexp,varchar, "a\nb\nc", varchar, "^b$", bool, false);
  T(regexp,varchar, "abc", varchar, "^$", bool, false);
  T(regexp,varchar, "b", varchar, "a*(^b$)c*", bool, true);
  T(regexp,varchar, "", varchar, "''", bool, false);
  T(regexp,varchar, "abc", varchar, "a.c", bool, true);
  T(regexp,varchar, "abd", varchar, "a[bc]d", bool, true);
  T(regexp,varchar, "a*c", varchar, "a\\*c", bool, true);
  T(regexp,varchar, "a\\b", varchar, "a\\\\b", bool, true);
  T(regexp,varchar, "a\\*b", varchar, "a\\\\\\*b", bool, true);
  T(regexp,varchar, "a\\bc", varchar, "a\\\\bc", bool, true);
  T(regexp,varchar, "a[b", varchar, "a\\[b", bool, true);
  T(regexp,varchar, "a", varchar, "a$", bool, true);
  T(regexp,varchar, "a$", varchar, "a$", bool, false);
  T(regexp,varchar, "a", varchar, "a\\$", bool, false);
  T(regexp,varchar, "a$", varchar, "a\\$", bool, true);
  T(regexp,varchar, "a", varchar, "a\\\\$", bool, false);
  T(regexp,varchar, "a$", varchar, "a\\\\$", bool, false);
  T(regexp,varchar, "a\\$", varchar, "a\\\\$", bool, false);
  T(regexp,varchar, "a\\", varchar, "a\\\\$", bool, true);
  T(regexp,varchar, "abbcbd", varchar, "a\\(b*\\)c\\1d", bool, false);
  T(regexp,varchar, "abbcbbbd", varchar, "a\\(b*\\)c\\1d", bool, false);
  T(regexp,varchar, "abc", varchar, "^\\(.\\)\\1", bool, false);
  T(regexp,varchar, "abbcbd", varchar, "a\\(\\([bc]\\)\\2\\)*d", bool, false);
  T(regexp,varchar, "abc", varchar, "ab*c", bool, true);
  T(regexp,varchar, "abc", varchar, "ab+c", bool, true);
  T(regexp,varchar, "abc", varchar, "ab?c", bool, true);
  T(regexp,varchar, "ab", varchar, "a{1}b", bool, true);
  T(regexp,varchar, "ab", varchar, "a{1,}b", bool, true);
  T(regexp,varchar, "aab", varchar, "a{1,2}b", bool, true);
  T(regexp,varchar, "abcac", varchar, "ab{0,0}c", bool, true);
  T(regexp,varchar, "abcac", varchar, "ab{0,1}c", bool, true);
  T(regexp,varchar, "abbcac", varchar, "ab{0,3}c", bool, true);
  T(regexp,varchar, "acabc", varchar, "ab{1,1}c", bool, true);
  T(regexp,varchar, "acabc", varchar, "ab{1,3}c", bool, true);
  T(regexp,varchar, "abcabbc", varchar, "ab{2,2}c", bool, true);
  T(regexp,varchar, "abcabbc", varchar, "ab{2,4}c", bool, true);
  T(regexp,varchar, "a", varchar, "((a{1,10}){1,10}){1,10}", bool, true);
  T(regexp,varchar, "abc", varchar, "a[b]c", bool, true);
  T(regexp,varchar, "abc", varchar, "a[ab]c", bool, true);
  T(regexp,varchar, "adc", varchar, "a[^ab]c", bool, true);
  T(regexp,varchar, "a]c", varchar, "a[]b]c", bool, true);
  T(regexp,varchar, "a-c", varchar, "a[-b]c", bool, true);
  T(regexp,varchar, "adc", varchar, "a[^]b]c", bool, true);
  T(regexp,varchar, "adc", varchar, "a[^-b]c", bool, true);
  T(regexp,varchar, "a-c", varchar, "a[b-]c", bool, true);
  T(regexp,varchar, "a2c", varchar, "a[1-3]c", bool, true);
  T(regexp,varchar, "a-c", varchar, "a[[.-.]--]c", bool, true);
  T(regexp,varchar, "ax", varchar, "a[[.x.]]", bool, true);
  T(regexp,varchar, "abc", varchar, "a[[=b=]]c", bool, true);
  T(regexp,varchar, "abc", varchar, "a(((b)))c", bool, true);
  T(regexp,varchar, "abd", varchar, "a(b|(c))d", bool, true);
  T(regexp,varchar, "abbd", varchar, "a(b*|c)d", bool, true);
  T(regexp,varchar, "aaaaabaaaabaaaabaaaab", varchar, "a[ab]{20}", bool, true);
  T(regexp,varchar, "aaaaabaaaabaaaabaaaab", varchar, "a[ab][ab][ab][ab][ab][ab][ab][ab][ab][ab][ab][ab][ab][ab][ab][ab][ab][ab][ab][ab]", bool, true);
  T(regexp,varchar, "aaaaabaaaabaaaabaaaabweeknights", varchar, "a[ab][ab][ab][ab][ab][ab][ab][ab][ab][ab][ab][ab][ab][ab][ab][ab][ab][ab][ab][ab](wee|week)(knights|night)", bool, true);
  T(regexp,varchar, "a12345678901234567890123456789b", varchar, "12345678901234567890123456789", bool, true);
  T(regexp,varchar, "a123456789012345678901234567890b", varchar, "123456789012345678901234567890", bool, true);
  T(regexp,varchar, "a1234567890123456789012345678901b", varchar, "1234567890123456789012345678901", bool, true);
  T(regexp,varchar, "a12345678901234567890123456789012b", varchar, "12345678901234567890123456789012", bool, true);
  T(regexp,varchar, "a123456789012345678901234567890123b", varchar, "123456789012345678901234567890123", bool, true);
  T(regexp,varchar, "a1234567890123456789012345678901234567890123456789012345678901234567890b", varchar, "1234567890123456789012345678901234567890123456789012345678901234567890", bool, true);
  T(regexp,varchar, "xacegikmoq", varchar, "[ab][cd][ef][gh][ij][kl][mn]", bool, true);
  T(regexp,varchar, "xacegikmoq", varchar, "[ab][cd][ef][gh][ij][kl][mn][op]", bool, true);
  T(regexp,varchar, "xacegikmoqy", varchar, "[ab][cd][ef][gh][ij][kl][mn][op][qr]", bool, true);
  T(regexp,varchar, "xacegikmoqy", varchar, "[ab][cd][ef][gh][ij][kl][mn][op][q]", bool, true);
  T(regexp,varchar, "xabcy", varchar, "abc", bool, true);
  T(regexp,varchar, "acd", varchar, "a\\(b\\)?c\\1d", bool, false);
  T(regexp,varchar, "abc", varchar, "a[^b]c", bool, false);
  T(regexp,varchar, "adc", varchar, "a[^b]c", bool, true);
  T(regexp,varchar, "abc", varchar, "[a]b[c]", bool, true);
  T(regexp,varchar, "aba", varchar, "[a]b[a]", bool, true);
  T(regexp,varchar, "abc", varchar, "[abc]b[abc]", bool, true);
  T(regexp,varchar, "abd", varchar, "[abc]b[abd]", bool, true);
  T(regexp,varchar, "accd", varchar, "a(b?c)+d", bool, true);
  T(regexp,varchar, "weeknights", varchar, "(wee|week)(knights|night)", bool, true);
  T(regexp,varchar, "weeknights", varchar, "(we|wee|week|frob)(knights|night|day)", bool, true);
  T(regexp,varchar, "xyzaaabcaababdacd", varchar, "a[bc]d", bool, true);
  T(regexp,varchar, "aaabc", varchar, "a[ab]c", bool, true);
  T(regexp,varchar, "abc", varchar, "abc", bool, true);
  T(regexp,varchar, "/*x*/", varchar, "/\\*.*\\*/", bool, true);
  T(regexp,varchar, "/*x*/y/*z*/", varchar, "/\\*.*\\*/", bool, true);
  T(regexp,varchar, "/*x*/", varchar, "/\\*([^*]|\\*[^/])*\\*/", bool, true);
  T(regexp,varchar, "/*x*/y/*z*/", varchar, "/\\*([^*]|\\*[^/])*\\*/", bool, true);
  T(regexp,varchar, "/*x**/y/*z*/", varchar, "/\\*([^*]|\\*[^/])*\\*/", bool, true);
  T(regexp,varchar, "/*x*/", varchar, "/\\*([^*]|\\*+[^*/])*\\*+/", bool, true);
  T(regexp,varchar, "/*x*/y/*z*/", varchar, "/\\*([^*]|\\*+[^*/])*\\*+/", bool, true);
  T(regexp,varchar, "/*x**/y/*z*/", varchar, "/\\*([^*]|\\*+[^*/])*\\*+/", bool, true);
  T(regexp,varchar, "/*x****/y/*z*/", varchar, "/\\*([^*]|\\*+[^*/])*\\*+/", bool, true);
  T(regexp,varchar, "/*x**x*/y/*z*/", varchar, "/\\*([^*]|\\*+[^*/])*\\*+/", bool, true);
  T(regexp,varchar, "/*x***x/y/*z*/", varchar, "/\\*([^*]|\\*+[^*/])*\\*+/", bool, true);
  T(regexp,varchar, "abcd", varchar, "a(b)(c)d", bool, true);
  T(regexp,varchar, "abc", varchar, "a(((b)))c", bool, true);
  T(regexp,varchar, "abd", varchar, "a(b|(c))d", bool, true);
  T(regexp,varchar, "abbd", varchar, "a(b*|c|e)d", bool, true);
  T(regexp,varchar, "acd", varchar, "a(b*|c|e)d", bool, true);
  T(regexp,varchar, "ad", varchar, "a(b*|c|e)d", bool, true);
  T(regexp,varchar, "abc", varchar, "a(b?)c", bool, true);
  T(regexp,varchar, "ac", varchar, "a(b?)c", bool, true);
  T(regexp,varchar, "abc", varchar, "a(b+)c", bool, true);
  T(regexp,varchar, "abbbc", varchar, "a(b+)c", bool, true);
  T(regexp,varchar, "ac", varchar, "a(b*)c", bool, true);
  T(regexp,varchar, "abcdef", varchar, "(a|ab)(bc([de]+)f|cde)", bool, true);
  T(regexp,varchar, "abcdefghijk", varchar, "a(b)(c)(d)(e)(f)(g)(h)(i)(j)k", bool, true);
  T(regexp,varchar, "abcdefghijkl", varchar, "a(b)(c)(d)(e)(f)(g)(h)(i)(j)(k)l", bool, true);
  T(regexp,varchar, "abc", varchar, "a([bc]?)c", bool, true);
  T(regexp,varchar, "ac", varchar, "a([bc]?)c", bool, true);
  T(regexp,varchar, "abc", varchar, "a([bc]+)c", bool, true);
  T(regexp,varchar, "abcc", varchar, "a([bc]+)c", bool, true);
  T(regexp,varchar, "abcbc", varchar, "a([bc]+)bc", bool, true);
  T(regexp,varchar, "abb", varchar, "a(bb+|b)b", bool, true);
  T(regexp,varchar, "abb", varchar, "a(bbb+|bb+|b)b", bool, true);
  T(regexp,varchar, "abbb", varchar, "a(bbb+|bb+|b)b", bool, true);
  T(regexp,varchar, "abbb", varchar, "a(bbb+|bb+|b)bb", bool, true);
  T(regexp,varchar, "abcdef", varchar, "(.*).*", bool, true);
  T(regexp,varchar, "ad", varchar, "a(b|c)*d", bool, true);
  T(regexp,varchar, "abcd", varchar, "a(b|c)*d", bool, true);
  T(regexp,varchar, "abd", varchar, "a(b|c)+d", bool, true);
  T(regexp,varchar, "abcd", varchar, "a(b|c)+d", bool, true);
  T(regexp,varchar, "ad", varchar, "a(b|c?)+d", bool, true);
  T(regexp,varchar, "abcd", varchar, "a(b|c?)+d", bool, true);
  T(regexp,varchar, "ad", varchar, "a(b|c){0,0}d", bool, true);
  T(regexp,varchar, "ad", varchar, "a(b|c){0,1}d", bool, true);
  T(regexp,varchar, "abd", varchar, "a(b|c){0,1}d", bool, true);
  T(regexp,varchar, "ad", varchar, "a(b|c){0,2}d", bool, true);
  T(regexp,varchar, "abcd", varchar, "a(b|c){0,2}d", bool, true);
  T(regexp,varchar, "ad", varchar, "a(b|c){0,}d", bool, true);
  T(regexp,varchar, "abcd", varchar, "a(b|c){0,}d", bool, true);
  T(regexp,varchar, "abd", varchar, "a(b|c){1,1}d", bool, true);
  T(regexp,varchar, "acd", varchar, "a(b|c){1,1}d", bool, true);
  T(regexp,varchar, "abd", varchar, "a(b|c){1,2}d", bool, true);
  T(regexp,varchar, "abcd", varchar, "a(b|c){1,2}d", bool, true);
  T(regexp,varchar, "abd", varchar, "a(b|c){1,}d", bool, true);
  T(regexp,varchar, "abcd", varchar, "a(b|c){1,}d", bool, true);
  T(regexp,varchar, "acbd", varchar, "a(b|c){2,2}d", bool, true);
  T(regexp,varchar, "abcd", varchar, "a(b|c){2,2}d", bool, true);
  T(regexp,varchar, "abcd", varchar, "a(b|c){2,4}d", bool, true);
  T(regexp,varchar, "abcbd", varchar, "a(b|c){2,4}d", bool, true);
  T(regexp,varchar, "abcbcd", varchar, "a(b|c){2,4}d", bool, true);
  T(regexp,varchar, "abcd", varchar, "a(b|c){2,}d", bool, true);
  T(regexp,varchar, "abcbd", varchar, "a(b|c){2,}d", bool, true);
  T(regexp,varchar, "abd", varchar, "a(b+|((c)*))+d", bool, true);
  T(regexp,varchar, "abcd", varchar, "a(b+|((c)*))+d", bool, true);
  T(regexp,varchar, "abc", varchar, "abc", bool, true);
  T(regexp,varchar, "xabcy", varchar, "abc", bool, true);
  T(regexp,varchar, "xyz", varchar, "abc", bool, false);
  T(regexp,varchar, "a", varchar, "aZb", bool, false);
  T(regexp,varchar, "(aZb)", varchar, "aZb", bool, true);
  T(regexp,varchar, "(ab)", varchar, "aZ*b", bool, true);
  T(regexp,varchar, "(aZb)", varchar, "a.b", bool, true);
  T(regexp,varchar, "ba", varchar, "[[:<:]]a", bool, false);
  T(regexp,varchar, "ab", varchar, "a[[:>:]]", bool, false);
  T(regexp,varchar, "x_a_b", varchar, "[[:<:]]a_b[[:>:]]", bool, false);
  T(regexp,varchar, "A1", varchar, "(A[1])|(A[2])|(A[3])|(A[4])|(A[5])|(A[6])|(A[7])|(A[8])|(A[9])|(A[A])", bool, true);
  T(regexp,varchar, "abcdefghijklmnop", varchar, "abcdefghijklmnop", bool, true);
  T(regexp,varchar, "abcdefghijklmnopqrstuv", varchar, "abcdefghijklmnopqrstuv", bool, true);
  T(regexp,varchar, "CC11", varchar, "CC[13]1|a{21}[23][EO][123][Es][12]a{15}aa[34][EW]aaaaaaa[X]a", bool, true);
  T(regexp,varchar, "ab", varchar, "a?b", bool, true);

}

TEST_F(ObExprRegexpTest, fail_regexp_basic_test)
{
  ObExprRegexp regexp;
  ObExprStringBuf buf;

  F(regexp,max, 0, min, 0, null, 0);
  F(regexp,max, 0, max, 0, null, 0);
  F(regexp,int, 1, int, 2, null, 0);
  F(regexp,int, 1, int, 1, null, 0);

  // int vs special
  F(regexp,int, 1, min, 0, null, 0);
  F(regexp,int, 1, max, 0, null, 0);
  F(regexp,min, 0, int, 1, null, 0);
  F(regexp,max, 0, int, 1, null, 0);

  // int vs normal
  F(regexp,int, 1, varchar, "1", null, 0);
  F(regexp,int, 1, double, 1.0, null, 0);
  F(regexp,int, 1, varchar, "h1", null, 0);
  F(regexp,int, 0, varchar, "", null, 0);

  F(regexp,varchar, "1", bool, true, bool, false);
  F(regexp,int, 1, bool, true, bool, false);
  F(regexp,min, 0, min, 0, null, 0);
  F(regexp,min, 0, max, 0, null, 0);

  F(regexp,varchar, "", varchar, "(|a)b", bool, false);
  F(regexp,varchar, "", varchar, "(a|)b", bool, false);

  F(regexp,varchar, "EPAREN", varchar, "a(", bool, false);
  F(regexp,varchar, "EPAREN", varchar, "a(b", bool, false);
  F(regexp,varchar, "EMPTY", varchar, "|", bool, false);
  F(regexp,varchar, "BADRPT", varchar, "*", bool, false);
  F(regexp,varchar, "BADRPT", varchar, "+", bool, false);
  F(regexp,varchar, "BADRPT", varchar, "?", bool, false);
  F(regexp,varchar, "EMPTY", varchar, "a||b", bool, false);
  F(regexp,varchar, "EMPTY", varchar, "|ab", bool, false);
  F(regexp,varchar, "EMPTY", varchar, "ab|", bool, false);
  F(regexp,varchar, "EMPTY", varchar, "(|a)b", bool, false);
  F(regexp,varchar, "EMPTY", varchar, "(a|)b", bool, false);
  F(regexp,varchar, "BADRPT", varchar, "(*a)", bool, false);
  F(regexp,varchar, "BADRPT", varchar, "(+a)", bool, false);
  F(regexp,varchar, "BADRPT", varchar, "(?a)", bool, false);
  F(regexp,varchar, "BADRPT", varchar, "({1}a)", bool, false);
  F(regexp,varchar, "BADRPT", varchar, "(a|*b)", bool, false);
  F(regexp,varchar, "BADRPT", varchar, "(a|+b)", bool, false);
  F(regexp,varchar, "BADRPT", varchar, "(a|?b)", bool, false);
  F(regexp,varchar, "BADRPT", varchar, "(a|{1}b)", bool, false);
  F(regexp,varchar, "BADRPT", varchar, "^*", bool, false);
  F(regexp,varchar, "BADRPT", varchar, "^+", bool, false);
  F(regexp,varchar, "BADRPT", varchar, "^?", bool, false);
  F(regexp,varchar, "BADRPT", varchar, "^{1}", bool, false);
  F(regexp,varchar, "BADRPT", varchar, "{1", bool, false);
  F(regexp,varchar, "BADRPT", varchar, "{1}", bool, false);
  F(regexp,varchar, "EBRACE", varchar, "a{1", bool, false);
  F(regexp,varchar, "EBRACE", varchar, "a{1a", bool, false);
  F(regexp,varchar, "BADBR", varchar, "a{1a}", bool, false);
  F(regexp,varchar, "BADBR", varchar, "a{1,x}", bool, false);
  F(regexp,varchar, "EBRACE", varchar, "a{1,x", bool, false);
  F(regexp,varchar, "BADBR", varchar, "a{300}", bool, false);
  F(regexp,varchar, "BADBR", varchar, "a{1,0}", bool, false);
  F(regexp,varchar, "BADRPT", varchar, "a++", bool, false);
  F(regexp,varchar, "BADRPT", varchar, "a??", bool, false);
  F(regexp,varchar, "BADRPT", varchar, "a*+", bool, false);
  F(regexp,varchar, "BADRPT", varchar, "a*?", bool, false);
  F(regexp,varchar, "BADRPT", varchar, "a+*", bool, false);
  F(regexp,varchar, "BADRPT", varchar, "a+?", bool, false);
  F(regexp,varchar, "BADRPT", varchar, "a?*", bool, false);
  F(regexp,varchar, "BADRPT", varchar, "a?+", bool, false);
  F(regexp,varchar, "BADRPT", varchar, "a{1}{1}", bool, false);
  F(regexp,varchar, "BADRPT", varchar, "a*{1}", bool, false);
  F(regexp,varchar, "BADRPT", varchar, "a+{1}", bool, false);
  F(regexp,varchar, "BADRPT", varchar, "a?{1}", bool, false);
  F(regexp,varchar, "BADRPT", varchar, "a{1}*", bool, false);
  F(regexp,varchar, "BADRPT", varchar, "a{1}+", bool, false);
  F(regexp,varchar, "BADRPT", varchar, "a{1}?", bool, false);
}

#define TYPE(t1, t2, res) ARITH_EXPECT_TYPE(ObExprRegexp, calc_result_type2, t1, t2, res)

TEST_F(ObExprRegexpTest, interface_test)
{
  ObExprRegexp regexp;
  TYPE(ObExtendType, ObNullType, ObBoolType);
  TYPE(ObExtendType, ObExtendType, ObBoolType);
  TYPE(ObExtendType, ObNullType, ObBoolType);
  TYPE(ObNullType, ObNullType, ObBoolType);
  TYPE(ObNullType, ObIntType, ObBoolType);
  TYPE(ObIntType, ObIntType, ObBoolType);
  TYPE(ObIntType, ObVarcharType, ObBoolType);
  TYPE(ObVarcharType, ObVarcharType, ObBoolType);
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}



