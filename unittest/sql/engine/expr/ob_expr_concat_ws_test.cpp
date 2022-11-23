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
#include "sql/engine/expr/ob_expr_concat_ws.h"
#include "ob_expr_test_utils.h"
#include <sstream>
#include<string>
using namespace oceanbase::common;
using namespace oceanbase::sql;

class TestAllocator: public ObIAllocator
{
public:
	TestAllocator(): label_(ObModIds::TEST) {}
  virtual ~TestAllocator() {}
  void *alloc(const int64_t sz) { UNUSED(sz); return NULL; }
  void free(void *p) { UNUSED(p); }
  void freed(const int64_t sz) { UNUSED(sz); }
  void set_label(const char *label) {label_ = label;};
private:
  const char *label_;
};

class ObExprConcatWsTest : public ::testing::Test
{
public:
  ObExprConcatWsTest();
  virtual ~ObExprConcatWsTest();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  ObExprConcatWsTest(const ObExprConcatWsTest &other);
  ObExprConcatWsTest& operator=(const ObExprConcatWsTest &other);
private:
  // data members
};
ObExprConcatWsTest::ObExprConcatWsTest()
{
}

ObExprConcatWsTest::~ObExprConcatWsTest()
{
}

void ObExprConcatWsTest::SetUp()
{
}

void ObExprConcatWsTest::TearDown()
{
}
#define TN(num,rtype, res,...) EXPECT_RESULTN(concatws, &buf, calc_resultN, num,rtype, res,  __VA_ARGS__)

#define FN(num,...) EXPECT_FAIL_RESULTN(concatws, &buf, calc_resultN, num,  __VA_ARGS__)

#define EXCEPT_RESULTNN(obj, str_buf, func, num, params) \
{\
	ObObj res;\
  int err = obj.func(res, params, num, str_buf); \
  EXPECT_TRUE(OB_SUCCESS == err); \
} while(0)

#define EXCEPT_NULL_RESULT1(obj,str_buf,func,t1,v1,t2,v2,t3,v3)\
{\
	ObObj param[3];\
	ObObj res;\
	param[2].set_null();\
	param[1].set_null();\
	param[0].set_##t1(v1);\
	param[3].set_##t2(v2);\
	param[4].set_##t3(v3);\
	int ret = obj.func(res,param,5,str_buf);\
	_OB_LOG(INFO, "res=%s", to_cstring(res)); \
	ASSERT_EQ(OB_SUCCESS, ret);   \
}while(0)
#define EXCEPT_NULL_RESULT2(obj,str_buf,func,rt,rv)\
{\
	ObObj param[2];\
	ObObj res;\
	param[0].set_null();\
	param[1].set_null();\
	int ret = obj.func(res,param,2,str_buf);\
	_OB_LOG(INFO, "res=%s", to_cstring(res)); \
	ASSERT_EQ(OB_SUCCESS, ret);   \
}while(0)
#define EXCEPT_NULL_RESULT3(obj,str_buf,func,rt,rv)\
{\
	ObObj param[1];\
	ObObj res;\
	param[0].set_null();\
	int ret = obj.func(res,param,1,str_buf);\
	ASSERT_EQ(OB_INVALID_ARGUMENT, ret);   \
}while(0)
#define EXCEPT_NULL_RESULT4(obj,str_buf,func,t1,v1,t2,v2,t3,v3)\
{\
	ObObj param[3];\
	ObObj res;\
	param[0].set_##t1(v1);\
	param[1].set_##t2(v2);\
	param[2].set_##t3(v3);\
	int ret = obj.func(res,param,5,str_buf);\
	ASSERT_EQ(OB_NOT_INIT, ret);   \
}while(0)
#define EXCEPT_NULL_RESULT5(obj,str_buf,func,t1,v1,t2,v2,t3,v3)\
{\
	ObObj param[3];\
	ObObj res;\
	param[0].set_##t1(v1);\
	param[1].set_##t2(v2);\
	param[2].set_##t3(v3);\
	int ret = obj.func(res,param,5,str_buf);\
	ASSERT_EQ(OB_ALLOCATE_MEMORY_FAILED, ret);   \
}while(0)
#define EXCEPT_MAXLEN_RESULT(obj,str_buf,func,num, params)\
{\
	ObObj res;\
	int ret = obj.func(res,params,num,str_buf);\
	ASSERT_EQ(OB_SIZE_OVERFLOW, ret);   \
}while(0)

#define TNN(num,params) EXCEPT_RESULTNN(concatws, &buf, calc_resultN, num, params)
#define ENR1(t1,v1,t2,v2,t3,v3) EXCEPT_NULL_RESULT1(concatws, &buf, calc_resultN,t1,v1,t2,v2,t3,v3)
#define ENR2(rt,rv) EXCEPT_NULL_RESULT2(concatws, &buf, calc_resultN, rt,rv)
#define ENR3(rt,rv) EXCEPT_NULL_RESULT3(concatws, &buf, calc_resultN, rt,rv)
#define ENR4(buf,t1,v1,t2,v2,t3,v3) EXCEPT_NULL_RESULT4(concatws,buf, calc_resultN,t1,v1,t2,v2,t3,v3)
#define ENR5(buf,t1,v1,t2,v2,t3,v3) EXCEPT_NULL_RESULT5(concatws,buf, calc_resultN,t1,v1,t2,v2,t3,v3)
#define EMR(num,params) EXCEPT_MAXLEN_RESULT(concatws,&buf,calc_resultN,num, params)
TEST_F(ObExprConcatWsTest, basic_test)
{
	ObExprConcatWs concatws;
	DefaultPageAllocator buf;

  //  case 1. concat_ws(',','1');
  TN(2, varchar, "1", varchar, ",",varchar, "1");
  //  case 2. concat_ws(',','1','2');
  TN(3, varchar, "1,2", varchar,",",varchar, "1", varchar, "2");
  //  case 3. concat_ws(',','','');
  TN(3, varchar, ",", varchar,",",varchar, "", varchar, "");
  //  case 4. concat_ws('dffd','221','2','3');
  TN(4, varchar, "221dffd2dffd3", varchar,"dffd",varchar, "221", varchar, "2",varchar,"3");
  //  case 5. concat_ws(',','','2333','3');
  TN(4, varchar, ",2333,3", varchar,",",varchar, "", varchar, "2333",varchar,"3");
  //  case 6. concat_ws('','','','2','3');
  TN(5, varchar, "#23", varchar,"",varchar,"#",varchar, "", varchar, "2",varchar,"3");
  //  case 7. concat_ws('#','','','2','324343','');
  TN(6, varchar, "#,,2,324343,", varchar,",",varchar,"#",varchar, "", varchar, "2",varchar,"324343",varchar,"");
  //  case 8. concat_ws('sdsada',null,null,'3234sdda','4324sawsd');
  ENR1(varchar,"sdsada",varchar,"3234sdda",varchar,"4324sawsd");
  //  case 9. concat_ws('',null,null,'43534534','fdgsawsd');
  ENR1(varchar,"",varchar,"43534534",varchar,"fdgsawsd");
  //  case 10. concat_ws('',null,null,'','');
  ENR1(varchar,"",varchar,"",varchar,"");
  //  case 11. concat_ws('sds',null,null,'242%4','^7dfs');
  ENR1(varchar,"sds",varchar,"242%4",varchar,"^7dfs");
  //  case 12. concat_ws(null,null)
  ENR2(varchar,"");
  //	case 13. testing big size
  ObObj params[1000];
  for(int32_t idx = 0 ; idx < 1000 ; idx++) {
    params[idx].set_varchar("dsfdsfsfsdffsfdsfwererewrewrewddddddwererxdsseeefr343242sas**");
  }
  TNN(1000,params);
  //	case 14. testing the first parameter is null for big size
  params[0].set_null();
  TNN(1000,params);
}

TEST_F(ObExprConcatWsTest, special_test)
{
	TestAllocator tbuf;
	ObExprConcatWs concatws;
	DefaultPageAllocator buf;
	//  case 1. concat_ws('1');
	FN(1, varchar, "1");
	//  case 2. concat_ws(1,'1','2');
	FN(3,int,1,varchar, "1", varchar, "2");
	//  case 3. concat_ws('1saas','222',1);
	FN(3,varchar,"1saas", varchar, "222",int,1);
	//  case 4. concat_ws('abcdf','1',3.0,1);
	FN(4, varchar,"abcdf",varchar, "1", double, 3.0,int,1);
	//  case 5. concat_ws(',','1','2',1,2);
	FN(5,varchar,",",varchar, "1", varchar, "2",int,1,int, 2);
	//	case 6. concat_ws('ds','dfd343','433453') test when buf is NULL
	ENR4(NULL,varchar,"ds",varchar,"fdfds",varchar,"433453");
	//	case 7. concat_ws('ds','dfd343','433453') test when tbuf can not allocator memory
  ENR5(&tbuf,varchar,"ds",varchar,"fdfds",varchar,"433453");
  //  case 8. concat_ws(null)
  ENR3(varchar,"");
  // case 9. testing max length
  ObObj params[10000];
	for(int32_t idx = 0 ; idx < 10000 ; idx++) {
		params[idx].set_varchar("dsfdsfsfsdffsfdsfwererewrewrewddddddwererxdsseeefr343242sas**");
	}
	EMR(10000,params);
}
int main(int argc, char **argv)
{
	  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
	  ::testing::InitGoogleTest(&argc,argv);
	  return RUN_ALL_TESTS();
}
