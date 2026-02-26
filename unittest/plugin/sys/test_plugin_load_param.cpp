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

#include <utility>
#include <gtest/gtest.h>

#define USING_LOG_PREFIX SHARE

#include "plugin/sys/ob_plugin_load_param.h"

using namespace std;
using namespace oceanbase::common;
using namespace oceanbase::plugin;

bool load_param_equal_to(const ObPluginLoadParam &param1, const ObPluginLoadParam &param2)
{
  return 0 == param1.library_name.compare(param2.library_name) &&
      param1.load_option.value() == param2.load_option.value();
}

bool load_param_array_equal_to(const ObIArray<ObPluginLoadParam> &params1, const ObIArray<ObPluginLoadParam> &params2)
{
  if (params1.count() != params2.count()) {
    return false;
  }

  for (int64_t i = 0; i < params1.count(); i++) {
    if (!load_param_equal_to(params1.at(i), params2.at(i))) {
      return false;
    }
  }
  return true;
}

struct Item
{
  const char *test_case = nullptr;
  bool expected_success = true;
  ObArray<ObPluginLoadParam> expected_result;
};

TEST(TestObPluginLoadOption, test_load_option)
{
  pair<const char *, ObPluginLoadOption::ObOptionEnum> test_cases[] = {
    {"on", ObPluginLoadOption::ON},
    {"On", ObPluginLoadOption::ON},
    {"ON", ObPluginLoadOption::ON},
    {"off", ObPluginLoadOption::OFF},
    {"Off", ObPluginLoadOption::OFF},
    {"OFF", ObPluginLoadOption::OFF},
    {"oFf", ObPluginLoadOption::OFF},
    {"hello", ObPluginLoadOption::INVALID},
    {"", ObPluginLoadOption::INVALID}
  };

  for (size_t i = 0; i < sizeof(test_cases) / sizeof(test_cases[0]); i++) {
    auto &test_case = test_cases[i];
    ObPluginLoadOption load_option = ObPluginLoadOption::from_string(test_case.first);
    LOG_INFO("compare load option", K(test_case.first), K(load_option), K(test_case.second));
    ASSERT_EQ(load_option.value(), test_case.second);
  }
}

TEST(TestObPluginLoadParam, test_load_param)
{
  ObArray<void *> test_cases;

  ObString libname1("libexample.so");
  ObString libname2("libexample2.so");

  ObArray<ObPluginLoadParam> test_result;
  Item item1;
  item1.test_case = "";
  item1.expected_result.reuse();
  test_cases.push_back(&item1);

  Item item2;
  item2.test_case = libname1.ptr();
  ObPluginLoadParam param;
  param.library_name = libname1;
  param.load_option = ObPluginLoadOption(ObPluginLoadOption::ON);
  item2.expected_result.push_back(param);
  test_cases.push_back(&item2);

  Item item3;
  item3.test_case = "libexample.so:on";
  param.library_name = libname1;
  param.load_option = ObPluginLoadOption(ObPluginLoadOption::ON);
  ASSERT_EQ(OB_SUCCESS, item3.expected_result.push_back(param));
  test_cases.push_back(&item3);

  Item item5;
  item5.test_case = "libexample.so:hello";
  param.library_name = libname1;
  param.load_option = ObPluginLoadOption(ObPluginLoadOption::INVALID);
  item5.expected_success = false;
  ASSERT_EQ(OB_SUCCESS, item5.expected_result.push_back(param));
  test_cases.push_back(&item5);

  Item item6;
  item6.test_case = "libexample.so,libexample2.so";
  param.library_name = libname1;
  param.load_option = ObPluginLoadOption(ObPluginLoadOption::ON);
  ASSERT_EQ(OB_SUCCESS, item6.expected_result.push_back(param));

  param.library_name = libname2;
  param.load_option = ObPluginLoadOption(ObPluginLoadOption::ON);
  ASSERT_EQ(OB_SUCCESS, item6.expected_result.push_back(param));

  test_cases.push_back(&item6);

  Item item7;
  item7.test_case = "libexample.so,libexample2.so:off ";
  param.library_name = libname1;
  param.load_option = ObPluginLoadOption(ObPluginLoadOption::ON);
  ASSERT_EQ(OB_SUCCESS, item7.expected_result.push_back(param));

  param.library_name = libname2;
  param.load_option = ObPluginLoadOption(ObPluginLoadOption::OFF);
  ASSERT_EQ(OB_SUCCESS, item7.expected_result.push_back(param));

  test_cases.push_back(&item7);

  Item item8;
  item8.test_case = "libexample.so: off, libexample2.so";
  param.library_name = libname1;
  param.load_option = ObPluginLoadOption(ObPluginLoadOption::OFF);
  ASSERT_EQ(OB_SUCCESS, item8.expected_result.push_back(param));

  param.library_name = libname2;
  param.load_option = ObPluginLoadOption(ObPluginLoadOption::ON);
  ASSERT_EQ(OB_SUCCESS, item8.expected_result.push_back(param));

  test_cases.push_back(&item8);

  Item item9;
  item9.test_case = "libexample.so:off,libexample2.so,";
  param.library_name = libname1;
  param.load_option = ObPluginLoadOption(ObPluginLoadOption::OFF);
  ASSERT_EQ(OB_SUCCESS, item9.expected_result.push_back(param));

  param.library_name = libname2;
  param.load_option = ObPluginLoadOption(ObPluginLoadOption::ON);
  ASSERT_EQ(OB_SUCCESS, item9.expected_result.push_back(param));

  test_cases.push_back(&item9);

  Item item10;
  item10.test_case = ":off,libexample2.so,";
  item10.expected_success = false;
  test_cases.push_back(&item10);

  Item item11;
  item11.test_case = " ";
  item11.expected_success = true;
  test_cases.push_back(&item11);

  ObArray<ObPluginLoadParam> load_params;
  for (int64_t i = 0; i < test_cases.count(); i++) {
    Item *item = static_cast<Item *>(test_cases.at(i));
    load_params.reuse();

    int ret = ObPluginLoadParamParser::parse(ObString(item->test_case), load_params);
    LOG_INFO("test load param",
             "case number", i,
             KCSTRING(item->test_case),
             K(item->expected_success),
             K(item->expected_result),
             K(load_params));
    if (item->expected_success) {
      ASSERT_EQ(ret, OB_SUCCESS);
      ASSERT_TRUE(load_param_array_equal_to(item->expected_result, load_params));
    } else {
      ASSERT_NE(ret, OB_SUCCESS);
    }
  }
}

int main(int argc, char **argv)
{
  ObLogger::get_logger().set_log_level("DEBUG");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
