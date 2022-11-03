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

#include <fnmatch.h>
#include <gtest/gtest.h>

#include "share/ob_define.h"

#include "logservice/libobcdc/src/ob_log_table_matcher.h"


using namespace oceanbase;
using namespace common;
using namespace libobcdc;

namespace oceanbase
{
namespace unittest
{

/*
 * TEST1.
 * Test fnmatch.
 * Used to study fnmatch().
 */
/*
 * Test Functions.
 * fnmatch Prototype:
 *   int fnmatch(const char *pattern, const char *string, int flags);
 */
void CASE_MATCH(const char *pattern, const char *string, int flags = 0)
{
  int err = fnmatch(pattern, string, flags);
  EXPECT_EQ(0, err);
  fprintf(stderr, ">>> %s: \t\"%s\" -> \"%s\"\n",
          (0 == err) ? "MATCH" : "NOMATCH",
          pattern, string);
}

void CASE_NOMATCH(const char *pattern, const char *string, int flags = 0)
{
  int err = fnmatch(pattern, string, flags);
  EXPECT_EQ(FNM_NOMATCH, err);
  fprintf(stderr, ">>> %s: \t\"%s\" -> \"%s\"\n",
          (0 == err) ? "MATCH" : "NOMATCH",
          pattern, string);
}
TEST(DISABLED_TableMatcher, Fnmatch1)
{
  CASE_MATCH("sky*", "SkyBlue", FNM_CASEFOLD);
  CASE_NOMATCH("sky*[!e]", "SkyBlue", FNM_CASEFOLD);
  CASE_MATCH("ab\\0c", "ab\\0c");
}

/*
 * TEST2.
 * Test TableMatcher.
 */
TEST(TableMatcher, BasicTest1)
{
  int err = OB_SUCCESS;
  ObLogTableMatcher matcher;
  const char *tb_whilte_list="TN1.DB-A*.table_1*|"
                            "TN2.DB-A*.TABLE_2*|"
                            "tn3.db-a*.table_*_tmp";
  const char *tb_black_list="|";
  const char *tg_whilte_list="*.*";
  const char *tg_black_list="|";

  err = matcher.init(tb_whilte_list, tb_black_list, tg_whilte_list, tg_black_list);
  EXPECT_EQ(OB_SUCCESS, err);

  int flag = FNM_CASEFOLD;

  // Test match.
  bool matched = false;
  err = matcher.table_match("tn1", "db-a-1", "table_1_1", matched, flag);
  EXPECT_TRUE(matched);

  err = matcher.table_match("tn1", "db-b-1", "table_1_1", matched, flag);
  EXPECT_FALSE(matched);

  err = matcher.table_match("tn3", "db-a-2", "table_1_tmp", matched, flag);
  EXPECT_TRUE(matched);

  matcher.destroy();
}

/*
 * TEST3.
 * Test TableMatcher static match.
 */
TEST(TableMatcher, BasicTest2)
{
  int err = OB_SUCCESS;
  const char *tb_whilte_list="*.*.*";
  const char *tb_black_list="|";
  const char *tg_whilte_list="*.*";
  const char *tg_black_list="|";

  ObLogTableMatcher matcher;

  err = matcher.init(tb_whilte_list, tb_black_list, tg_whilte_list, tg_black_list);
  EXPECT_EQ(OB_SUCCESS, err);

  int flag = FNM_CASEFOLD;

  // Case 1. Match.
  {
    const char *pattern1 = "tn1.db1*|tn2.db2*|tn3.db3*|tn4.db4*";
    ObArray<ObString> pattern2;
    err = pattern2.push_back(ObString("tn1.db1"));
    EXPECT_EQ(OB_SUCCESS, err);
    err = pattern2.push_back(ObString("tnx.dbx"));
    EXPECT_EQ(OB_SUCCESS, err);

    bool matched = false;
    err = matcher.match(pattern1, pattern2, matched, flag);
    EXPECT_EQ(OB_SUCCESS, err);
    EXPECT_TRUE(matched);
  }

  // Case 2. No match.
  {
    const char *pattern1 = "tn1.db1*|tn2.db2*|tn3.db3*|tn4.db4*";
    ObArray<ObString> pattern2;
    err = pattern2.push_back(ObString("tnx.dbx"));
    EXPECT_EQ(OB_SUCCESS, err);
    err = pattern2.push_back(ObString("tny.dby"));
    EXPECT_EQ(OB_SUCCESS, err);

    bool matched = false;
    err = matcher.match(pattern1, pattern2, matched, flag);
    EXPECT_EQ(OB_SUCCESS, err);
    EXPECT_FALSE(matched);
  }

  // Case 3. Empty pattern1.
  {
    const char *pattern1 = "";
    ObArray<ObString> pattern2;
    err = pattern2.push_back(ObString("tnx.dbx"));
    EXPECT_EQ(OB_SUCCESS, err);
    err = pattern2.push_back(ObString("tny.dby"));
    EXPECT_EQ(OB_SUCCESS, err);

    bool matched = false;
    err = matcher.match(pattern1, pattern2, matched, flag);
    EXPECT_EQ(OB_SUCCESS, err);
    EXPECT_FALSE(matched);
  }

  // Case 4. Invalid pattern1.
  {
    const char *pattern1 = "|";
    ObArray<ObString> pattern2;
    err = pattern2.push_back(ObString("tnx.dbx"));
    EXPECT_EQ(OB_SUCCESS, err);
    err = pattern2.push_back(ObString("tny.dby"));
    EXPECT_EQ(OB_SUCCESS, err);

    bool matched = false;
    err = matcher.match(pattern1, pattern2, matched, flag);
    EXPECT_EQ(OB_SUCCESS, err);
    EXPECT_FALSE(matched);
  }

  matcher.destroy();
}

// test tablegroup
TEST(TableMatcher, BasicTest3)
{
  int err = OB_SUCCESS;
  ObLogTableMatcher matcher;
  const char *tb_whilte_list="*.*.*";
  const char *tb_black_list="|";
  const char *tg_whilte_list="tt1.alitg*";
  const char *tg_black_list="|";

  err = matcher.init(tb_whilte_list, tb_black_list, tg_whilte_list, tg_black_list);
  EXPECT_EQ(OB_SUCCESS, err);

  int flag = FNM_CASEFOLD;

  // Test match.
  bool matched = false;
  err = matcher.tablegroup_match("tt1", "alitg1", matched, flag);
  EXPECT_TRUE(matched);

  err = matcher.tablegroup_match("tt1", "alitg2", matched, flag);
  EXPECT_TRUE(matched);

  err = matcher.tablegroup_match("tt1", "alipaytg", matched, flag);
  EXPECT_FALSE(matched);

  err = matcher.tablegroup_match("tt2", "alitg1", matched, flag);
  EXPECT_FALSE(matched);

  matcher.destroy();
}

TEST(TableMatcher, BasicTest4)
{
  int err = OB_SUCCESS;
  ObLogTableMatcher matcher;
  const char *tb_whilte_list="*.*.*";
  const char *tb_black_list="|";
  const char *tg_whilte_list="tt1.alitg*|tt1.anttg*";
  const char *tg_black_list="tt1.alitg*";

  err = matcher.init(tb_whilte_list, tb_black_list, tg_whilte_list, tg_black_list);
  EXPECT_EQ(OB_SUCCESS, err);

  int flag = FNM_CASEFOLD;
  // Whitelist matches, but blacklist does not

  // Test match.
  bool matched = false;
  err = matcher.tablegroup_match("tt1", "alitg1", matched, flag);
  EXPECT_FALSE(matched);

  err = matcher.tablegroup_match("tt1", "alitg2", matched, flag);
  EXPECT_FALSE(matched);

  err = matcher.tablegroup_match("tt1", "anttg1", matched, flag);
  EXPECT_TRUE(matched);

  err = matcher.tablegroup_match("tt1", "anttghello", matched, flag);
  EXPECT_TRUE(matched);

  err = matcher.tablegroup_match("tt2", "anttghello", matched, flag);
  EXPECT_FALSE(matched);

  matcher.destroy();
}

}
}

int main(int argc, char **argv)
{
  //ObLogger::get_logger().set_mod_log_levels("ALL.*:DEBUG, TLOG.*:DEBUG");
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_ob_log_table_match.log", true);
  logger.set_log_level(OB_LOG_LEVEL_INFO);
  testing::InitGoogleTest(&argc,argv);
  // testing::FLAGS_gtest_filter = "DO_NOT_RUN";
  return RUN_ALL_TESTS();
}
