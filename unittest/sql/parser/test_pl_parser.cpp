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

#include "pl/parser/ob_pl_parser.h"
#include <gtest/gtest.h>
#include "lib/utility/ob_test_util.h"
#include "../test_sql_utils.h"
#include "lib/allocator/page_arena.h"
#include "lib/json/ob_json_print_utils.h"  // for SJ
//#include "sql/plan_cache/ob_sql_parameterization.cpp"
#include <fstream>
#include <iterator>
using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::pl;
namespace test
{
class TestPLParser: public ::testing::Test
{
public:
  TestPLParser();
  virtual ~TestPLParser();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestPLParser);
protected:
  // data members
  ObArenaAllocator allocator_;
};

TestPLParser::TestPLParser() : allocator_(ObModIds::TEST)
{
}

TestPLParser::~TestPLParser()
{
}

void TestPLParser::SetUp()
{
}

void TestPLParser::TearDown()
{
}

TEST_F(TestPLParser, basic_test)
{
  ObPLParser parser(allocator_, ObCharsets4Parser());
  ParseResult parse_result;
//  const char *query_str = "CREATE PROCEDURE sp (a varchar(10), b int(10)) BEGIN DECLARE c int default 1; DECLARE d, e varchar(11); IF 1=1 THEN select * from t1; ELSE IF 2=1 THEN select * from t1; ELSE select 1; END IF; END";
//  const char *query_str = "create procedure sp() begin declare i bigint; if(i=1) then select 1 from dual; end if; end";
//  const char *query_str = "do begin declare i bigint default 0; set i=i+1; if(i=1) then select 1 from dual; end if; end";
//  const char *query_str = "CREATE PROCEDURE handlerdemo () BEGIN DECLARE CONTINUE HANDLER FOR SQLSTATE '23000' SET @x2 = 1; SET @x = 1; INSERT INTO test.t VALUES (1); SET @x = 2; INSERT INTO test.t VALUES (1); SET @x = 3; END;";
//  const char *query_str = "CREATE PROCEDURE sp() insert into t1 values(1, 1);";
//  const char *query_str = "CREATE PROCEDURE sp (a varchar(10), b int(10)) BEGIN DECLARE TYPE RecType IS RECORD(rno int, rname int, rsal int); DECLARE TYPE Tabletype IS TABLE OF RecType; DECLARE curval CURSOR FOR select c1 from t1; OPEN curval; FETCH curval INTO a, b; FETCH curval BULK INTO tab1; FETCH curval BULK INTO tab2 LIMIT 3; CLOSE curval; IF 1=1 THEN select * from t1; ELSE IF 2=1 THEN select * from t1; ELSE select 1; END IF; END";
//  const char *query_str = "do begin declare i bigint default 0; set i=i+1; if(i=1) then select 1 from dual; end if; end";
  //  const char *query_str = "create procedure p(x int)  begin declare i int;  set i=i+1; if(1=1) then begin declare j int;  select 1 from a where a1=i into x; end; end if; while i>1 do set i=i-1; end while; call f(x); end";
  //  const char* query_str = "do begin DECLARE total_sale INT DEFAULT 0; set total_sale = ?; update t1 set c1 = c1 + total_sale; end";
  const char* query_str = "create procedure pro() begin DECLARE total_sale INT DEFAULT 0; set total_sale = 10; if total_sale > ? then update t1 set c1 = c1 + total_sale; end if; end";
  ObString query = ObString::make_string(query_str);
  int ret = OB_SUCCESS;
  _OB_LOG(INFO, "QUERY: %s", query_str);
  ret = parser.parse(query, query, parse_result);
  ASSERT_EQ(OB_SUCCESS, -ret);
  if (NULL != parse_result.result_tree_) {
    _OB_LOG(INFO, "%s", CSJ(ObParserResultPrintWrapper(*parse_result.result_tree_)));
  }
}
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc,argv);
  if(argc >= 2)
  {
    if (strcmp("DEBUG", argv[1]) == 0
        || strcmp("WARN", argv[1]) == 0)
    OB_LOGGER.set_log_level(argv[1]);
  } else {
    OB_LOGGER.set_log_level("INFO");
  }

  OB_LOGGER.set_file_name("test_pl_parser.log", true);
  return RUN_ALL_TESTS();
}
