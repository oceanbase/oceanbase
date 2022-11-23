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

#define USING_LOG_PREFIX SQL

#include "sql/parser/ob_parser.h"
#include <gtest/gtest.h>
#include "lib/utility/ob_test_util.h"
#include "../test_sql_utils.h"
#include "lib/allocator/page_arena.h"
#include "lib/json/ob_json_print_utils.h"  // for SJ
#include <fstream>
#include <iterator>
using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace test
{


class TestMultiParser: public TestSqlUtils, public ::testing::Test
{
public:
  TestMultiParser();
  virtual ~TestMultiParser();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestMultiParser);
protected:
  // data members
  ObArenaAllocator allocator_;
};

TestMultiParser::TestMultiParser() : allocator_(ObModIds::TEST)
{
}

TestMultiParser::~TestMultiParser()
{
}

void TestMultiParser::SetUp()
{
}

void TestMultiParser::TearDown()
{
}

TEST_F(TestMultiParser, basic_test)
{
  int ret = OB_SUCCESS;
  ObSQLMode mode = SMO_DEFAULT;
  /*
  const char *query_str = "alter system bootstrap ZONE 'zone1' SERVER '100.81.152.44:19518'";
  const char *query_str = "create database if not exists rongxuan default character set = 'utf8'  default collate = 'default_collate'";
  const char *query_str = "select * from d.t1 PARTITION(p1, p2);";
  const char *query_str = "update d.t1 PARTITION (p2) SET id = 2 WHERE name = 'Jill';";
  const char *query_str = "delete from d.t1 PARTITION(p0, p1);";
  */
  //const char *query_str = "select '12', '11', '11', '11'";
  //const char *query_str = "alter system bootstrap ZONE 'zone1' SERVER '100.81.152.44:19518';select 3;select '23';create table t1 (i int)  ;;;; ";
  const char *query_str = "";
  //const char *query_str = "select 1";
  ObString query = ObString::make_string(query_str);
  ObSEArray<ObString, 4> queries;
  ObParser parser(allocator_, mode);
  ObMPParseStat parse_stat;
  ret = parser.split_multiple_stmt(query, queries, parse_stat);
  LOG_INFO("YES. multi query", K(query), K(queries));
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, queries.count());

}


}


int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc,argv);
  OB_LOGGER.set_log_level("INFO");
  test::parse_cmd_line_param(argc, argv, test::clp);
  return RUN_ALL_TESTS();
}
