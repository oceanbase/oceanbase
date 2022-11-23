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

#include  <gtest/gtest.h>
#include "lib/oblog/ob_log_print_kv.h"
#include "lib/ob_errno.h"
#include "common/ob_range.h"
using namespace std;
namespace oceanbase
{
namespace common
{
  enum ObLogTestLevel
  {
    first = 0,
    second = 1,
    third = 2,
  };
class ObLogTest
{
  public:
    ObLogTest():num_(10),name_("hello"),lever_(first) {}
    virtual ~ObLogTest() {};
    int64_t num_;
    char name_[100];
    ObLogTestLevel lever_;

};
TEST(ObLogPrintObj,basic_test )
{
  int ret = OB_SUCCESS;
  ObLogTest log_test;
  char buf[100];
  int64_t len = 100;
  int64_t pos = 0;
  ret = logdata_print_obj(buf, len, pos, &log_test);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(pos,14);
  pos =0 ;
  ret =logdata_print_obj(buf, len,pos,log_test.name_);
  ASSERT_EQ(7,pos);
  ASSERT_STREQ(buf, "\"hello\"");
  ObLogTest *tmp_test=NULL;
  ret =logdata_print_obj(buf, len,pos,tmp_test);
  ASSERT_EQ(11,pos);
  ASSERT_STREQ(buf, "\"hello\"NULL");
  ObVersion version;
  ret = logdata_print_obj(buf, len,  pos,version);
  ASSERT_EQ(18,pos);
  ASSERT_STREQ(buf, "\"hello\"NULL\"0-0-0\"");
  ObVersion *version1 = new ObVersion();
  ret = logdata_print_obj(buf, len,  pos,version1);
  ASSERT_EQ(25,pos);
  ASSERT_STREQ(buf, "\"hello\"NULL\"0-0-0\"\"0-0-0\"");
  ObVersion *version2 = NULL;
  ret = logdata_print_obj(buf, len,  pos,version2);
  ASSERT_EQ(29,pos);
  ASSERT_STREQ(buf, "\"hello\"NULL\"0-0-0\"\"0-0-0\"NULL");
  // TODO: use KVV, KVV(version2)
  // ret = logdata_print_obj(buf, len,  pos, *version2);
  // ASSERT_EQ(33,pos);
  // ASSERT_STREQ(buf, "\"hello\"NULL\"0-0-0\"\"0-0-0\"NULLNULL");
}
}
}


int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
