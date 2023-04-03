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
#include "sql/engine/expr/ob_expr_replace.h"
#include "ob_expr_test_utils.h"
#include "lib/timezone/ob_time_convert.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

class ObExprReplaceTest : public ::testing::Test
{
public:
  ObExprReplaceTest();
  virtual ~ObExprReplaceTest();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  ObExprReplaceTest(const ObExprReplaceTest &other);
  ObExprReplaceTest& operator=(const ObExprReplaceTest &other);
private:
  // data members
};
ObExprReplaceTest::ObExprReplaceTest()
{
}

ObExprReplaceTest::~ObExprReplaceTest()
{
}

void ObExprReplaceTest::SetUp()
{
}

void ObExprReplaceTest::TearDown()
{
}

#define T(t1, v1, t2, v2, t3, v3, ref_type, ref_value) EXPECT_RESULT3     (obj, &buf, calc_result3, t1, v1, t2, v2, t3, v3, ref_type, ref_value)
#define F(t1, v1, t2, v2, t3, v3, ref_type, ref_value) EXPECT_FAIL_RESULT3(obj, &buf, calc_result3, t1, v1, t2, v2, t3, v3, ref_type, ref_value)

TEST_F(ObExprReplaceTest, type_test)
{
  DefaultPageAllocator buf;
  ObExprReplace obj(buf);
  ASSERT_EQ(3, obj.get_param_num());

  // null type
  T(null    ,  		, varchar , ""    	, varchar , ""  , null  ,  );
  T(varchar , ""   	, null    ,   		, varchar , ""  , null  ,  );
  T(varchar , ""   	, varchar , ""    	, null    ,  	, null  ,  );
  T(null    ,  		, null    ,   		, null    ,  	, null  ,  );

  // int type
  T(varchar , "www.b123aidu.com"  , int     , 123     , int     , 111	  , varchar , "www.b111aidu.com");
  T(varchar , "www.b123aidu.com"  , int     , 123     , varchar , "AAA"   , varchar , "www.bAAAaidu.com");
  T(varchar , "www.b123aidu.com"  , varchar , "123"   , int     , 111     , varchar , "www.b111aidu.com");

  T(int     , 123456              , int     , 123     , int     , 111     , varchar , "111456");
  T(int     , 123456              , int     , 123     , varchar , "AAA"   , varchar , "AAA456");
  T(int     , 123456              , varchar , "123"   , int     , 111     , varchar , "111456");
  T(int     , 123456              , varchar , "123"   , varchar , "AAA"   , varchar , "AAA456");

  // bool type
  T(bool, 1, bool, 1, int, 1, varchar , "1");
  T(bool, 1, bool, 0, int, 0, varchar , "1");
  T(bool, 0, bool, 1, int, 0, varchar , "0");
  T(bool, 0, bool, 0, int, 1, varchar , "1");
  T(bool, 0, int , 0, int, 1, varchar , "1");
  T(bool, 0, int , 1, int, 0, varchar , "0");
  T(bool, 1, int , 1, int, 1, varchar , "1");
  T(int, 12021, bool, 1, int, 3, varchar , "32023");
  T(int, 12021, bool, 0, int, 5, varchar , "12521");
  T(int, 12021, bool, 0, int, 2, varchar , "12221");


  // datetime type
  T(varchar, "www.baidu.com", varchar	, "www"		, datetime	,1434392608130640, varchar	, "2015-06-15 18:23:28.130640.baidu.com");
  T(varchar, "2015-06-15 18:23:28.130640.baidu.com"	, datetime	,1434392608130640, varchar	,"www"	,	varchar	, "www.baidu.com");
  T(datetime,1434392608130640,varchar 	, "2015-06-15"	, varchar	, "www"		 , varchar 	, "www 18:23:28.130640");

  // timestamp type
  T(varchar, "www.baidu.com", varchar	, "www"		, timestamp	,1434392608130640, varchar	, "2015-06-15 18:23:28.130640.baidu.com");
  T(varchar, "2015-06-15 18:23:28.130640.baidu.com"	, timestamp	,1434392608130640, varchar	,"www"	,	varchar	, "www.baidu.com");
  T(timestamp,1434392608130640,varchar 	, "2015-06-15"	, varchar	, "www"		 , varchar 	, "www 18:23:28.130640");

  // date type
  T(varchar, "www.baidu.com", varchar	, "www"		, date		,16601,			varchar	, "2015-06-15.baidu.com");
  T(varchar, "2015-06-15.baidu.com"	, date		, 16601		,varchar	,"www",	varchar	, "www.baidu.com");
  T(date   , 16601	    , varchar	, "2015"	, varchar	,"www",			varchar	, "www-06-15");

  // time type
  int64_t time1,time2;
  ObString str1="08:09:10";
  ObString str2="12:13:14";
  ObTimeConverter::str_to_time(str1, time1);
  ObTimeConverter::str_to_time(str2, time2);
  T(varchar , "www.baidu.com" 		, varchar , "www" , time     , time1 , varchar , "08:09:10.baidu.com");
  T(time    , time1					, varchar , "08"  , time     , time2 , varchar , "12:13:14:09:10");
  T(varchar , "www.baidu12:13:14.com" 	, time 	  , time2 , varchar  , "www"  , varchar , "www.baiduwww.com");

  // year type
}

TEST_F(ObExprReplaceTest, basic_test)
{
  DefaultPageAllocator buf;
  ObExprReplace obj(buf);
  ASSERT_EQ(3, obj.get_param_num());

  // empty varchar
  T(varchar   , ""    , varchar , ""    , varchar , ""    , varchar, ""   );
  T(varchar   , ""    , varchar , ""    , varchar , "www" , varchar, ""   );
  T(varchar   , ""    , varchar , "www" , varchar , ""    , varchar, ""   );
  T(varchar   , ""    , varchar , "www" , varchar , "www" , varchar, ""   );
  T(varchar   , "www" , varchar , ""    , varchar , ""    , varchar, "www");
  T(varchar   , "www" , varchar , ""    , varchar , "www" , varchar, "www");
  T(varchar   , "www" , varchar , "www" , varchar , ""    , varchar, "" );

  // case sensitive
  T(varchar     , "www.baidu.com"       , varchar       , "w"    , varchar       , "A"         , varchar, "AAA.baidu.com"   );
  T(varchar     , "www.baidu.com"       , varchar       , "W"    , varchar       , "A"         , varchar, "www.baidu.com"   );
  T(varchar     , "WWW.baidu.com"       , varchar       , "w" 	 , varchar       , "A"         , varchar, "WWW.baidu.com"   );
  T(varchar     , "WWW.baidu.com"       , varchar       , "W"    , varchar       , "A"         , varchar, "AAA.baidu.com"  );

  // other string type
  T(varchar     , "www.baidu.com"       , char       , "w"    , varchar       , "A"         , varchar, "AAA.baidu.com"   );
  T(varchar     , "www.baidu.com"       , char       , "W"    , varchar       , "A"         , varchar, "www.baidu.com"   );
  T(varchar     , "www.baidu.com"       , binary     , "w"    , varchar       , "A"         , varchar, "AAA.baidu.com"   );
  T(varchar     , "www.baidu.com"       , binary     , "W"    , varchar       , "A"         , varchar, "www.baidu.com"   );
  T(varchar     , "www.baidu.com"       , varbinary  , "w"    , varchar       , "A"         , varchar, "AAA.baidu.com"   );
  T(varchar     , "www.baidu.com"       , varbinary  , "W"    , varchar       , "A"         , varchar, "www.baidu.com"   );

  // normal
  T(varchar , "hhello"   		, varchar , ""    , varchar , ""	, varchar, "hhello"  );
  T(varchar , "www.baidu.com"       	, varchar , ""    , varchar , "www"     , varchar, "www.baidu.com"   );
  T(varchar , "hello"               	, varchar , "el"  , varchar , "el"    	, varchar, "hello"      );
  T(varchar , "hello"   		, varchar , "el"  , varchar , "E"   	, varchar, "hElo" );
  T(varchar , "hellohel"    		, varchar , "el"  , varchar , "E"   	, varchar, "hElohE" );
  T(varchar , "www.baidu.com" 		, varchar , "ww"  , varchar , "AAAA"  	, varchar, "AAAAw.baidu.com"  );
  T(varchar , "www.bau.com www.bau.com" , varchar , "ww"  , varchar , "AAAA"    , varchar, "AAAAw.bau.com AAAAw.bau.com");

}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  ::testing::InitGoogleTest(&argc,argv);

  return RUN_ALL_TESTS();
}

