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
#include "share/config/ob_config.h"
#include "share/config/ob_common_config.h"
#include "share/ob_define.h"

using namespace oceanbase::common;
using namespace oceanbase;
/* using namespace oceanbase::common::hash; */

class TestServerConfig
    : public ::testing::Test, public ObInitConfigContainer
{
public:
  bool check() const
  {
    bool ret = true;
    ObConfigContainer::const_iterator it = container_.begin();
    for (; it != container_.end(); it++)
    {
      ret &= it->second->check();
    }
    return ret;
  }

  void check_each_true() const
  {
    ObConfigContainer::const_iterator it = container_.begin();
    for (; it != container_.end(); it++)
    {
      if (strncmp(it->first.str(), "f_", 2) == 0) {
        EXPECT_FALSE(it->second->check()) << it->first.str() << " not pass checker!";
      } else {
        EXPECT_TRUE(it->second->check()) << it->first.str() << " not pass checker!";
      }
    }
  }

  DEF_INT(num0, "-1", "[,0]", "info");
  DEF_INT(num1, "11", "(10,11]", "info");
  DEF_INT(num2, "10", "[10,11]", "info");
  DEF_INT(num3, "-1", "[-1,-1]", "info");
  DEF_INT(num4, "1", "(0,2)", "info");
  DEF_INT(num5, "1", "[1,2)", "info");
  DEF_INT(num6, "1", "(0,1]", "info");
  DEF_INT(num7, "1", "[1,1]", "info");
  DEF_INT(num8, "1", "info");

  DEF_INT(num9, "1", "[1,1]", "info");
  DEF_INT(num10, "1", "[1,1]", "info");

  DEF_TIME(time1, "1h", "[60m,3600]", "info");
  DEF_TIME(time2, "1s", "[1,2)", "info");
  DEF_TIME(time3, "1m", "(59s,60s]", "info");
  DEF_TIME(time4, "1h", "[60m,3600]", "info");

  DEF_TIME(time5, "1h", "[,3600]", "info");
  DEF_TIME(time6, "1h", "[60m,]", "info");

  DEF_CAP(cap1, "10M", "[10m,10m]", "info");
  DEF_CAP(cap2, "0", "[0,0]", "info");
  DEF_CAP(cap4, "1g", "[1024M,1g]", "info");
  DEF_CAP(cap5, "1M", "[1024k,1024k]", "info");
  DEF_CAP(cap6, "1M", "(1023k,1025k)", "info");

  DEF_CAP(cap7, "1M", "[1024k,]", "info");
  DEF_CAP(cap8, "1M", "[,1024k]", "info");

  DEF_INT(num_out, "15", "[0,20]", "info");

  DEF_BOOL(bool1, "True", "info");
  DEF_BOOL(bool2, "fALse", "info");

  DEF_STR(str1, "some string", "info");
  DEF_STR(str2, "another string", "info");

  DEF_IP(ip1, "1.2.3.4", "some ip description");
  DEF_IP(ip2, "1.2.3.4","some ip description");
  DEF_IP(ip3, "111.2.3.4", "some ip description");

  /*
   * The next variables are used for testing the added properties of config_item.
   */
  DEF_INT(num0_e, "-1", "[,0]", "info",
          CfgAttr(Section::OBSERVER, Scope::CLUSTER, Source::DFL, EditLevel::EDITABLE_DYNAMIC));
  DEF_INT(num1_e, "1", "info",
          CfgAttr(Section::OBSERVER, Scope::CLUSTER, Source::DFL, EditLevel::NOTEDITABLE));
  DEF_INT(num2_e, "1", "[1,1]", "info",
          CfgAttr(Section::OBSERVER, Scope::CLUSTER, Source::DFL, EditLevel::EDITABLE_DYNAMIC));

  DEF_INT(num3_e, "1", "[1,1]", "info",
          CfgAttr(Section::OBSERVER, Scope::CLUSTER, Source::DFL, EditLevel::EDITABLE_DYNAMIC));

  DEF_TIME(time1_e, "1h", "[60m,3600]", "info",
          CfgAttr(Section::OBSERVER, Scope::CLUSTER, Source::DFL, EditLevel::EDITABLE_DYNAMIC));
  DEF_TIME(time2_e, "1h", "[,3600]", "info",
          CfgAttr(Section::OBSERVER, Scope::CLUSTER, Source::DFL, EditLevel::EDITABLE_DYNAMIC));
  DEF_TIME(time3_e, "1h", "[60m,]", "info",
          CfgAttr(Section::OBSERVER, Scope::CLUSTER, Source::DFL, EditLevel::EDITABLE_DYNAMIC));

  DEF_CAP(cap1_e, "10M", "[10m,10m]", "info",
          CfgAttr(Section::OBSERVER, Scope::CLUSTER, Source::DFL, EditLevel::EDITABLE_DYNAMIC));
  DEF_CAP(cap2_e, "1M", "[1024k,]", "info",
          CfgAttr(Section::OBSERVER, Scope::CLUSTER, Source::DFL, EditLevel::EDITABLE_DYNAMIC));


  DEF_BOOL(bool1_e, "True", "info",
          CfgAttr(Section::OBSERVER, Scope::CLUSTER, Source::DFL, EditLevel::EDITABLE_DYNAMIC));
  DEF_BOOL(bool2_e, "fALse", "info",
          CfgAttr(Section::OBSERVER, Scope::CLUSTER, Source::DFL, EditLevel::EDITABLE_DYNAMIC));

  DEF_STR(str1_e, "some string", "info",
          CfgAttr(Section::OBSERVER, Scope::CLUSTER, Source::DFL, EditLevel::EDITABLE_DYNAMIC));
  DEF_STR(str2_e, "another string", "info",
          CfgAttr(Section::OBSERVER, Scope::CLUSTER, Source::DFL, EditLevel::EDITABLE_DYNAMIC));

  DEF_IP(ip1_e, "1.2.3.4", "some ip description",
          CfgAttr(Section::OBSERVER, Scope::CLUSTER, Source::DFL, EditLevel::EDITABLE_DYNAMIC));
  DEF_IP(ip2_e, "1.2.3.4","some ip description",
          CfgAttr(Section::OBSERVER, Scope::CLUSTER, Source::DFL, EditLevel::EDITABLE_DYNAMIC));

  DEF_INT_LIST(test_int_list, "1;2;3", "test list");
  DEF_INT_LIST(test_int_list_2, "1;2;3", "test list");
  DEF_STR_LIST(test_str_list, "str1;str2;str3", "test list");
  DEF_STR_LIST(test_str_list_2, "str1;str2;str3", "test list");
  DEF_STR(url, "test url", "test url");

  DEF_DBL(dbl1, "1.234", "some info");
  DEF_DBL(dbl2, "1.234", "[1.234,5.321]", "some info");
  DEF_DBL(dbl3, "1.234", "[1,]", "some info");
  DEF_DBL(dbl4, "1.234", "[,]", "some info");
  DEF_DBL(dbl5, "-1.234", "[,-1]", "some info");
  DEF_DBL(f_dbl6, "-1", "[,-1)", "some info");
};


TEST_F(TestServerConfig, ALL)
{
  check_each_true();


  num2.set_value("11");
  EXPECT_TRUE(num2.check());
  num2.set_value("12");
  EXPECT_FALSE(num2.check());

  EXPECT_FALSE(check());

  EXPECT_TRUE(num_out.check());
  num_out.set_value("-1");
  EXPECT_FALSE(num_out.check());
  EXPECT_EQ(11, num1);

  EXPECT_FALSE(num1.set_value("hello"));
  EXPECT_EQ(11, num1.get());
  EXPECT_FALSE(num1.set_value("1.01"));
  EXPECT_EQ(11, num1.get());

  /* test the default value of "section" "scope" "source" "editlevel" */
  EXPECT_STREQ("unknown", num0.section());
  EXPECT_STREQ("unknown", num0.scope());
  EXPECT_STREQ("unknown", num0.source());
  EXPECT_STREQ("unknown", num0.edit_level());

  num0_e.set_section("observer");
  EXPECT_STREQ("observer", num0_e.section());
  EXPECT_TRUE(num0_e.is_edit_dynamically_effective());
  EXPECT_EQ(-1, num0_e.get());
  EXPECT_EQ(-1, num0_e.get_value());
  num0_e.set_value("2");
  EXPECT_EQ(2, num0_e.get_value());
  
  num1_e.set_section("tenant");
  EXPECT_STREQ("tenant", num1_e.section());
  num1_e.set_scope("tenant");
  EXPECT_STREQ("tenant", num1_e.scope());
  EXPECT_TRUE(num1_e.is_not_editable());

  num2_e.set_edit_level("editable, not dynamically effective");
  EXPECT_TRUE(num2_e.reboot_effective());

  EXPECT_EQ(3600000000, time1_e.get());
  EXPECT_EQ(3600000000, time1_e.get_value());
  time1_e.set_value("2us");
  EXPECT_EQ(2, time1_e.get());
  EXPECT_EQ(2, time1_e.get_value());

  time2_e.set_section("RECOURCE");
  EXPECT_STREQ("RECOURCE", time2_e.section());

  cap1_e.set_section("RECOURCE");
  EXPECT_STREQ("RECOURCE", cap1_e.section());

  EXPECT_EQ(10485760, cap1_e.get());
  EXPECT_EQ(10485760, cap1_e.get_value());
  cap1_e.set_value("2b");
  EXPECT_EQ(2, cap1_e.get());
  EXPECT_EQ(2, cap1_e.get_value());

  cap2_e.set_section("RECOURCE");
  EXPECT_STREQ("RECOURCE", cap2_e.section());

  bool1_e.set_section("RECOURCE");
  EXPECT_STREQ("RECOURCE", bool1_e.section());
  EXPECT_EQ(true, bool1_e);
  bool1_e.set_value("false");
  EXPECT_EQ(false, bool1_e);
  EXPECT_FALSE(bool1_e);

  str1_e.set_section("RECOURCE");
  EXPECT_STREQ("RECOURCE", str1_e.section());
  EXPECT_STREQ("some string", str1_e);
  str1_e.set_value("another string");
  EXPECT_STREQ("another string", str1_e);


  ip1_e.set_section("RECOURCE");
  EXPECT_STREQ("RECOURCE", ip1_e.section());
  EXPECT_STREQ("1.2.3.4", ip1_e);
  ip1_e.set_value("4.3.2.1");
  EXPECT_STREQ("4.3.2.1", ip1_e);

  ip2_e.set_section("RECOURCE");
  EXPECT_STREQ("RECOURCE", ip2_e.section());

  EXPECT_TRUE(bool1);
  EXPECT_FALSE(bool2);

  bool2.set_value("t");
  EXPECT_TRUE(bool2);

  ip3.set_value("1234.12.3.4");
  EXPECT_FALSE(ip3.check());

  const int64_t &i = num0;
  EXPECT_EQ(-1, i);
  num0 = 2;
  EXPECT_EQ(2, i);

  char buf_tmp[3];
  char buf_tmp2[32];
  EXPECT_EQ(3, test_str_list_2.size());
  test_str_list_2.get(0, buf_tmp, 32);
  EXPECT_STREQ("str1", buf_tmp);
  EXPECT_EQ(3, test_str_list_2.size());
  test_str_list_2.get(1, buf_tmp, 32);
  EXPECT_STREQ("str2", buf_tmp);
  
  test_str_list_2.set_value("12;34");
  EXPECT_EQ(2, test_str_list_2.size());
  test_str_list_2.get(0, buf_tmp2, 32);
  EXPECT_STREQ("12", buf_tmp2);
  test_str_list_2.get(1, buf_tmp2, 32);
  EXPECT_STREQ("34", buf_tmp2);

  char buf[3];
  char buf2[32];

  test_str_list.get(0, buf2, 32);
  EXPECT_STREQ("str1", buf2);
  test_str_list.get(1, buf2, 32);
  EXPECT_STREQ("str2", buf2);
  test_str_list.get(2, buf2, 32);
  EXPECT_STREQ("str3", buf2);
  EXPECT_EQ(3, test_str_list.size());

  EXPECT_EQ(OB_INVALID_ARGUMENT, test_str_list.get(-1, buf2, 32));
  EXPECT_EQ(OB_INVALID_ARGUMENT, test_str_list.get(0, NULL, 0));
  EXPECT_EQ(OB_INVALID_ARGUMENT, test_str_list.get(ObConfigStrListItem::MAX_INDEX_SIZE, NULL, 32));
  EXPECT_EQ(OB_ARRAY_OUT_OF_RANGE, test_str_list.get(3, buf2, 32));

  test_str_list.tryget(0, buf2, 32);
  EXPECT_STREQ("str1", buf2);
  test_str_list.tryget(1, buf2, 32);
  EXPECT_STREQ("str2", buf2);
  test_str_list.tryget(2, buf2, 32);
  EXPECT_STREQ("str3", buf2);

  EXPECT_EQ(OB_INVALID_ARGUMENT, test_str_list.tryget(-1, buf2, 32));
  EXPECT_EQ(OB_INVALID_ARGUMENT, test_str_list.tryget(0, NULL, 0));
  EXPECT_EQ(OB_INVALID_ARGUMENT, test_str_list.tryget(ObConfigStrListItem::MAX_INDEX_SIZE, NULL, 32));
  EXPECT_EQ(OB_ARRAY_OUT_OF_RANGE, test_str_list.tryget(3, buf2, 32));


  EXPECT_EQ(true, test_int_list_2.valid());
  EXPECT_EQ(3, test_int_list_2.size());
  EXPECT_EQ(1, test_int_list_2[0]);
  EXPECT_EQ(2, test_int_list_2[1]);
  EXPECT_EQ(3, test_int_list_2[2]);
  EXPECT_EQ(true, test_int_list_2.valid());
  EXPECT_EQ(3, test_int_list_2.size());
  EXPECT_EQ(1, test_int_list_2[0]);
  EXPECT_EQ(2, test_int_list_2[1]);
  EXPECT_EQ(3, test_int_list_2[2]);
  test_int_list_2.set_value("4;5");
  EXPECT_EQ(true, test_int_list_2.valid());
  EXPECT_EQ(2, test_int_list_2.size());
  EXPECT_EQ(4, test_int_list_2[0]);
  EXPECT_EQ(5, test_int_list_2[1]);
  EXPECT_EQ(true, test_int_list_2.valid());
  EXPECT_EQ(2, test_int_list_2.size());
  EXPECT_EQ(4, test_int_list_2[0]);
  EXPECT_EQ(5, test_int_list_2[1]);

  test_int_list = "1;2;3";
  EXPECT_EQ(3, test_int_list.size());
  EXPECT_EQ(1, test_int_list[0]);
  EXPECT_EQ(2, test_int_list[1]);
  EXPECT_EQ(3, test_int_list[2]);

  test_int_list = "5;4;3;2;1";
  EXPECT_EQ(5, test_int_list.size());
  EXPECT_EQ(1, test_int_list[4]);
  EXPECT_EQ(2, test_int_list[3]);
  EXPECT_EQ(3, test_int_list[2]);
  EXPECT_EQ(4, test_int_list[1]);
  EXPECT_EQ(5, test_int_list[0]);

  test_str_list = "xxxx;pppp";
  EXPECT_TRUE(test_str_list.valid());
  EXPECT_EQ(2, test_str_list.size());
  test_str_list.get(0, buf2, 32);
  EXPECT_STREQ("xxxx", buf2);
  test_str_list.get(1, buf2, 32);
  EXPECT_STREQ("pppp", buf2);
  test_str_list.get(0, buf, 3);
  EXPECT_STREQ("xx", buf);
  test_str_list.get(1, buf2, 32);
  EXPECT_STREQ("pppp", buf2);
  test_str_list.get(0, buf, 3);
  EXPECT_STREQ("xx", buf);
  test_str_list.get(1, buf2, 32);
  EXPECT_STREQ("pppp", buf2);

  EXPECT_TRUE(test_str_list.set_value("0;1;2;3;4;5;6;7;8;9;0;1;2;3;4;5;6;7;8;9;0;1;2;3;4;5;6;7;8;9;0;1;2;3;4;5;6;7;8;9;0;1;2;3;4;5;6;7;8;9;0;1;2;3;4;5;6;7;8;9;0;1;2;3"));
  EXPECT_TRUE(test_str_list.valid());
  EXPECT_EQ(64, test_str_list.size());
  for (int64_t i = 0; i < 64; i = i + 10) {
    test_str_list.get(i, buf2, 2);
    EXPECT_STREQ("0", buf2);
  }

  test_int_list = "123x;22";
  EXPECT_FALSE(test_int_list.valid());

  test_int_list = "123;";
  EXPECT_TRUE(test_int_list.valid());
  EXPECT_EQ(1, test_int_list.size());

  test_int_list = "";
  EXPECT_TRUE(test_int_list.valid());
  EXPECT_EQ(0, test_int_list.size());

  test_str_list = "";
  EXPECT_TRUE(test_str_list.set_value(""));
  EXPECT_TRUE(test_str_list.valid());
  EXPECT_EQ(0, test_str_list.size());

  test_str_list = "123x;";
  EXPECT_TRUE(test_str_list.valid());
  EXPECT_EQ(2, test_str_list.size());

  test_str_list = "123x";
  EXPECT_TRUE(test_int_list.valid());

  EXPECT_STREQ("test url", url.str());

  EXPECT_EQ(1.234, dbl1);
  EXPECT_EQ(1.234, dbl2);
  EXPECT_EQ(1.234, dbl3);
  EXPECT_EQ(1.234, dbl4);
  EXPECT_EQ(-1.234, dbl5);

  EXPECT_TRUE(dbl1.check());
  EXPECT_TRUE(dbl2.check());
  EXPECT_TRUE(dbl3.check());
  EXPECT_TRUE(dbl4.check());
  EXPECT_TRUE(dbl5.check());
  EXPECT_FALSE(f_dbl6.check());

  EXPECT_FALSE(dbl1.set_value("hello"));
  EXPECT_EQ(1.234, dbl1.get());
  EXPECT_FALSE(dbl2.set_value("hello"));
  EXPECT_EQ(1.234, dbl2.get());

}

int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
