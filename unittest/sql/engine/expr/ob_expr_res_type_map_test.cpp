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
#include "ob_expr_test_utils.h"
#include "sql/engine/expr/ob_expr_res_type_map.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

class TestArithRuleMap : public ::testing::Test, public ObArithResultTypeMap
{
  virtual void SetUp() override
  {
    ASSERT_EQ(OB_SUCCESS, init());
  }

  virtual void TearDown() override
  {
  }
  int define_rules()
  {
    using C = ObArithResultTypeChoice;
    int ret = OB_SUCCESS;
    ObArithRule rule;
    //type+type
    OZ (new_rules(ObNullType, ObNullType, ADD|SUB).result_as(ObNumberType).get_ret());
    //type+tc
    OZ (new_rules(ObNullType, ObOTimestampTC, MUL|DIV).result_as(ObVarcharType).get_ret());
    //tc+tc
    OZ (new_rules(ObStringTC, ObOTimestampTC, DIV).result_as(ObIntType).cast_param1_as(ObNumberType).get_ret());
    //type + func
    OZ (new_rules(ObDateType, ob_is_interval_tc, ADD|SUB|DIV).cast_param2_as(ObRawType).result_as(ObCharType).cast_param1_as(ObTimeType).get_ret()); 
    //func + func
    OZ (new_rules(ob_is_otimestampe_tc, ob_is_interval_tc, SUB|MUL).result_as(ObNCharType).cast_param1_as(ObNullType).get_ret());
    //ObArithResultTypeChoice
    OZ (new_rules(ob_is_interval_tc, ObStringTC, ADD).result_as(C::FIRST).cast_param1_as(ObNumberType).cast_param2_as(C::SECOND).get_ret()); 
    return ret; 
  }
};

TEST_F(TestArithRuleMap, get_rule)
{
  ObArithRule rule;
  ObArithRule default_rule;
  //type+type
  //OZ (new_rules(ObNullType, ObNullType, ADD|SUB).result_as(ObNumberType).get_ret());
  rule.result_type = ObNumberType;
  rule.param1_calc_type = ObMaxType;
  rule.param2_calc_type = ObMaxType;
  ASSERT_TRUE(rule == get_rule(ObNullType, ObNullType, ADD));
  ASSERT_TRUE(rule == get_rule(ObNullType, ObNullType, SUB));
  ASSERT_TRUE(default_rule == get_rule(ObNullType, ObNullType, MUL));
  ASSERT_TRUE(default_rule == get_rule(ObNullType, ObNullType, DIV));
  //type+tc
  //OZ (new_rules(ObNullType, ObOTimestampTC, MUL|DIV).result_as(ObVarcharType).get_ret());
  rule.result_type = ObVarcharType;
  rule.param1_calc_type = ObMaxType;
  rule.param2_calc_type = ObMaxType;
  ASSERT_TRUE(rule == get_rule(ObNullType, ObTimestampTZType, MUL));
  ASSERT_TRUE(rule == get_rule(ObNullType, ObTimestampTZType, DIV));
  ASSERT_TRUE(rule == get_rule(ObNullType, ObTimestampLTZType, MUL));
  ASSERT_TRUE(rule == get_rule(ObNullType, ObTimestampLTZType, DIV));
  ASSERT_TRUE(rule == get_rule(ObNullType, ObTimestampNanoType, MUL));
  ASSERT_TRUE(rule == get_rule(ObNullType, ObTimestampNanoType, DIV));
  ASSERT_TRUE(default_rule == get_rule(ObTimestampTZType, ObNullType, MUL));
  ASSERT_TRUE(default_rule == get_rule(ObTimestampLTZType, ObNullType, DIV));
  //tc+tc
  //OZ (new_rules(ObStringTC, ObOTimestampTC, DIV).result_as(ObIntType).cast_param1_as(ObNumberType).get_ret());
  rule.result_type = ObIntType;
  rule.param1_calc_type = ObNumberType;
  rule.param2_calc_type = ObMaxType;
  ASSERT_TRUE(rule == get_rule(ObVarcharType, ObTimestampTZType, DIV));
  ASSERT_TRUE(rule == get_rule(ObVarcharType, ObTimestampTZType, DIV));
  ASSERT_TRUE(rule == get_rule(ObVarcharType, ObTimestampTZType, DIV));
  ASSERT_TRUE(rule == get_rule(ObCharType, ObTimestampLTZType, DIV));
  ASSERT_TRUE(rule == get_rule(ObCharType, ObTimestampLTZType, DIV));
  ASSERT_TRUE(rule == get_rule(ObCharType, ObTimestampLTZType, DIV));
  ASSERT_TRUE(rule == get_rule(ObNCharType, ObTimestampTZType, DIV));
  ASSERT_TRUE(rule == get_rule(ObNCharType, ObTimestampNanoType, DIV));
  ASSERT_TRUE(rule == get_rule(ObNCharType, ObTimestampNanoType, DIV));
  //type + func
  //OZ (new_rules(ObDateType, ob_is_interval_tc, ADD|SUB|DIV).cast_param2_as(ObRawType).result_as(ObCharType).cast_param1_as(ObTimeType).get_ret()); 
  rule.result_type = ObCharType;
  rule.param1_calc_type = ObTimeType;
  rule.param2_calc_type = ObRawType;
  ASSERT_TRUE(rule == get_rule(ObDateType, ObIntervalYMType, ADD));
  ASSERT_TRUE(rule == get_rule(ObDateType, ObIntervalDSType, DIV));
  ASSERT_TRUE(rule == get_rule(ObDateType, ObIntervalYMType, SUB));
  //func + func
  //OZ (new_rules(ob_is_otimestampe_tc, ob_is_interval_tc, SUB|MUL).result_as(ObNCharType).cast_param1_as(ObNullType).get_ret());
  rule.result_type = ObNCharType;
  rule.param1_calc_type = ObNullType;
  rule.param2_calc_type = ObMaxType;
  ASSERT_TRUE(rule == get_rule(ObTimestampTZType, ObIntervalYMType, SUB));
  ASSERT_TRUE(rule == get_rule(ObTimestampTZType, ObIntervalYMType, MUL));
  ASSERT_TRUE(rule == get_rule(ObTimestampTZType, ObIntervalDSType, SUB));
  ASSERT_TRUE(rule == get_rule(ObTimestampTZType, ObIntervalDSType, MUL));
  ASSERT_TRUE(rule == get_rule(ObTimestampLTZType, ObIntervalYMType, SUB));
  ASSERT_TRUE(rule == get_rule(ObTimestampLTZType, ObIntervalYMType, MUL));
  ASSERT_TRUE(rule == get_rule(ObTimestampLTZType, ObIntervalDSType, SUB));
  ASSERT_TRUE(rule == get_rule(ObTimestampLTZType, ObIntervalDSType, MUL));
  ASSERT_TRUE(rule == get_rule(ObTimestampNanoType, ObIntervalYMType, SUB));
  ASSERT_TRUE(rule == get_rule(ObTimestampNanoType, ObIntervalYMType, MUL));
  ASSERT_TRUE(rule == get_rule(ObTimestampNanoType, ObIntervalDSType, SUB));
  ASSERT_TRUE(rule == get_rule(ObTimestampNanoType, ObIntervalDSType, MUL));
  //ObArithResultTypeChoice
  //OZ (new_rules(ob_is_interval_tc, ObStringTC, ADD).result_as(C::FIRST).cast_param1_as(ObNumberType).cast_param2_as(C::SECOND).get_ret()); 
  ASSERT_TRUE(ObIntervalYMType == get_rule(ObIntervalYMType, ObVarcharType, ADD).result_type);
  ASSERT_TRUE(ObNumberType == get_rule(ObIntervalYMType, ObVarcharType, ADD).param1_calc_type);
  ASSERT_TRUE(ObVarcharType == get_rule(ObIntervalYMType, ObVarcharType, ADD).param2_calc_type);
  ASSERT_TRUE(ObIntervalDSType == get_rule(ObIntervalDSType, ObVarcharType, ADD).result_type);
  ASSERT_TRUE(ObNumberType == get_rule(ObIntervalDSType, ObVarcharType, ADD).param1_calc_type);
  ASSERT_TRUE(ObVarcharType == get_rule(ObIntervalDSType, ObVarcharType, ADD).param2_calc_type);
  ASSERT_TRUE(ObIntervalYMType == get_rule(ObIntervalYMType, ObCharType, ADD).result_type);
  ASSERT_TRUE(ObNumberType == get_rule(ObIntervalYMType, ObCharType, ADD).param1_calc_type);
  ASSERT_TRUE(ObCharType == get_rule(ObIntervalYMType, ObCharType, ADD).param2_calc_type);
    
  //redefine
  ASSERT_EQ(OB_INIT_TWICE, new_rules(ObTimestampTZType, ObIntervalYMType, SUB).result_as(ObNullType).get_ret());
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc,argv);
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_arith_rule_map.log", true);
  return RUN_ALL_TESTS();
}
