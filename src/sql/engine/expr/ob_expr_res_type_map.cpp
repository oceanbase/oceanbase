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

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/expr/ob_expr_res_type_map.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"

namespace oceanbase {
using namespace common;
namespace sql {

int ObArithResultTypeMap::define_rules()
{
  using C = ObArithResultTypeChoice;
  int ret = OB_SUCCESS;

  //null
  OZ (new_rules(ObNullTC, ObNullTC, ADD|SUB|MUL|DIV|MOD|ROUND|SUM).result_as(ObNumberType).cast_param1_as(C::FIRST).cast_param2_as(C::SECOND).get_ret());
  OZ (new_rules(ObNullTC, ObIntTC, ADD|SUB|MUL|DIV|MOD).result_as(ObNumberType).cast_param1_as(C::FIRST).get_ret());
  OZ (new_rules(ObNullTC, ObNumberTC, ADD|SUB|MUL|DIV|MOD).result_as(ObNumberType).cast_param1_as(C::FIRST).get_ret());
  OZ (new_rules(ObNullTC, ObFloatType, ADD|SUB|MUL|DIV|MOD).result_as(ObFloatType).cast_param1_as(C::FIRST).get_ret());
  OZ (new_rules(ObNullTC, ObDoubleType, ADD|SUB|MUL|DIV|MOD).result_as(ObDoubleType).cast_param1_as(C::FIRST).get_ret());
  OZ (new_rules(ObNullTC, ObStringTC, ADD|SUB|MUL|DIV|MOD).result_as(ObNumberType).cast_param1_as(C::FIRST).get_ret());
  OZ (new_rules(ObNullTC, ObDateTimeType, ADD).result_as(ObDateTimeType).cast_param1_as(C::FIRST).get_ret());
  OZ (new_rules(ObNullTC, ObOTimestampTC, ADD).result_as(ObDateTimeType).cast_param1_as(C::FIRST).get_ret());
  OZ (new_rules(ObNullTC, ObIntervalTC, MUL|DIV).result_as(C::SECOND).cast_param1_as(C::FIRST).get_ret());

  //int
  OZ (new_rules(ObIntTC, ObNullTC, ADD|SUB|MUL|DIV|MOD|NANVL).result_as(ObNumberType).cast_param2_as(C::SECOND).get_ret());
  OZ (new_rules(ObIntTC, ObIntTC, ADD|SUB|MUL|DIV|MOD|ROUND|SUM|NANVL).result_as(ObNumberType).get_ret());
  OZ (new_rules(ObIntTC, ObNumberTC, ADD|SUB|MUL|DIV|MOD|ROUND|SUM|NANVL).result_as(ObNumberType).get_ret());
  OZ (new_rules(ObIntTC, ObFloatType, ADD|SUB|MUL|DIV|MOD|NANVL).result_as(ObFloatType).get_ret());
  OZ (new_rules(ObIntTC, ObDoubleType, ADD|SUB|MUL|DIV|MOD|NANVL).result_as(ObDoubleType).get_ret());
  OZ (new_rules(ObIntTC, ObStringTC, ADD|SUB|MUL|DIV|MOD|NANVL).result_as(ObNumberType).get_ret());
  OZ (new_rules(ObIntTC, ObDateTimeType, ADD).result_as(ObDateTimeType).cast_param1_as(ObNumberType).get_ret());
  OZ (new_rules(ObIntTC, ObOTimestampTC, ADD).result_as(ObDateTimeType).cast_param1_as(ObNumberType).get_ret());
  OZ (new_rules(ObIntTC, ObIntervalTC, MUL).result_as(C::SECOND).cast_param1_as(ObNumberType).get_ret());

  //number
  OZ (new_rules(ObNumberTC, ObNullTC, ADD|SUB|MUL|DIV|MOD|NANVL).result_as(ObNumberType).cast_param2_as(C::SECOND).get_ret());
  OZ (new_rules(ObNumberTC, ObIntTC, ADD|SUB|MUL|DIV|MOD|ROUND|NANVL).result_as(ObNumberType).get_ret());
  OZ (new_rules(ObNumberTC, ObNumberTC, ADD|SUB|MUL|DIV|MOD|ROUND|NANVL).result_as(ObNumberType).get_ret());
  OZ (new_rules(ObNumberTC, ObFloatType, ADD|SUB|MUL|DIV|MOD|NANVL).result_as(ObFloatType).get_ret());
  OZ (new_rules(ObNumberTC, ObDoubleType, ADD|SUB|MUL|DIV|MOD|NANVL).result_as(ObDoubleType).get_ret());
  OZ (new_rules(ObNumberTC, ObStringTC, ADD|SUB|MUL|DIV|MOD|NANVL).result_as(ObNumberType).get_ret());
  OZ (new_rules(ObNumberTC, ObDateTimeType, ADD).result_as(ObDateTimeType).cast_param1_as(ObNumberType).get_ret());
  OZ (new_rules(ObNumberTC, ObOTimestampTC, ADD).result_as(ObDateTimeType).cast_param1_as(ObNumberType).get_ret());
  OZ (new_rules(ObNumberTC, ObIntervalTC, MUL).result_as(C::SECOND).cast_param1_as(ObNumberType).get_ret());
  OZ (new_rules(ObNumberType, ObIntTC, SUM).result_as(ObNumberType).get_ret());
  OZ (new_rules(ObNumberType, ObNumberType, SUM).result_as(ObNumberType).get_ret());
  OZ (new_rules(ObNumberFloatType, ObNumberFloatType, SUM).result_as(ObNumberFloatType).get_ret());

  //float
  OZ (new_rules(ObFloatType, ObNullTC, ADD|SUB|MUL|DIV|MOD|NANVL).result_as(ObFloatType).cast_param2_as(C::SECOND).get_ret());
  OZ (new_rules(ObFloatType, ObIntTC, ADD|SUB|MUL|DIV|MOD|NANVL).result_as(ObFloatType).get_ret());
  OZ (new_rules(ObFloatType, ObNumberTC, ADD|SUB|MUL|DIV|MOD|NANVL).result_as(ObFloatType).get_ret());
  OZ (new_rules(ObFloatType, ObFloatType, ADD|SUB|MUL|DIV|MOD|ROUND|SUM|NANVL).result_as(ObFloatType).get_ret());
  OZ (new_rules(ObFloatType, ObDoubleType, ADD|SUB|MUL|DIV|MOD|NANVL).result_as(ObDoubleType).get_ret());
  OZ (new_rules(ObFloatType, ObStringTC, ADD|SUB|MUL|DIV|MOD|NANVL).result_as(ObFloatType).get_ret());
  OZ (new_rules(ObFloatType, ObDateTimeType, ADD).result_as(ObDateTimeType).cast_param1_as(ObNumberType).get_ret());
  OZ (new_rules(ObFloatType, ObOTimestampTC, ADD).result_as(ObDateTimeType).cast_param1_as(ObNumberType).get_ret());
  OZ (new_rules(ObFloatType, ObIntervalTC, MUL).result_as(C::SECOND).cast_param1_as(ObNumberType).get_ret());

  //double
  OZ (new_rules(ObDoubleType, ObNullTC, ADD|SUB|MUL|DIV|MOD|NANVL).result_as(ObDoubleType).cast_param2_as(C::SECOND).get_ret());
  OZ (new_rules(ObDoubleType, ObIntTC, ADD|SUB|MUL|DIV|MOD|NANVL).result_as(ObDoubleType).get_ret());
  OZ (new_rules(ObDoubleType, ObNumberTC, ADD|SUB|MUL|DIV|MOD|NANVL).result_as(ObDoubleType).get_ret());
  OZ (new_rules(ObDoubleType, ObFloatType, ADD|SUB|MUL|DIV|MOD|NANVL).result_as(ObDoubleType).get_ret());
  OZ (new_rules(ObDoubleType, ObDoubleType, ADD|SUB|MUL|DIV|MOD|ROUND|SUM|NANVL).result_as(ObDoubleType).get_ret());
  OZ (new_rules(ObDoubleType, ObStringTC, ADD|SUB|MUL|DIV|MOD|NANVL).result_as(ObDoubleType).get_ret());
  OZ (new_rules(ObDoubleType, ObDateTimeType, ADD).result_as(ObDateTimeType).cast_param1_as(ObNumberType).get_ret());
  OZ (new_rules(ObDoubleType, ObOTimestampTC, ADD).result_as(ObDateTimeType).cast_param1_as(ObNumberType).get_ret());
  OZ (new_rules(ObDoubleType, ObIntervalTC, MUL).result_as(C::SECOND).cast_param1_as(ObNumberType).get_ret());

  //string
  OZ (new_rules(ObStringTC, ObNullTC, ADD|SUB|MUL|DIV|MOD|NANVL).result_as(ObNumberType).cast_param2_as(C::SECOND).get_ret());
  OZ (new_rules(ObStringTC, ObIntTC, ADD|SUB|MUL|DIV|MOD|NANVL).result_as(ObNumberType).get_ret());
  OZ (new_rules(ObStringTC, ObNumberTC, ADD|SUB|MUL|DIV|MOD|NANVL).result_as(ObNumberType).get_ret());
  OZ (new_rules(ObStringTC, ObFloatType, ADD|SUB|MUL|DIV|MOD|NANVL).result_as(ObFloatType).get_ret());
  OZ (new_rules(ObStringTC, ObDoubleType, ADD|SUB|MUL|DIV|MOD|NANVL).result_as(ObDoubleType).get_ret());
  OZ (new_rules(ObStringTC, ObStringTC, ADD|SUB|MUL|DIV|MOD|ROUND|SUM|NANVL).result_as(ObNumberType).get_ret());
  OZ (new_rules(ObStringTC, ObDateTimeType, ADD).result_as(ObDateTimeType).cast_param1_as(ObNumberType).get_ret());
  OZ (new_rules(ObStringTC, ObOTimestampTC, ADD).result_as(ObDateTimeType).cast_param1_as(ObNumberType).get_ret());
  OZ (new_rules(ObStringTC, ObIntervalTC, MUL).result_as(C::SECOND).cast_param1_as(ObNumberType).get_ret());

  //DATE op others
  OZ (new_rules(ObDateTimeType, ObNullTC, ADD|SUB).result_as(ObDateTimeType).cast_param2_as(C::SECOND).get_ret());
  OZ (new_rules(ObDateTimeType, ObIntTC, ADD|SUB).result_as(ObDateTimeType).cast_param2_as(ObNumberType).get_ret());
  OZ (new_rules(ObDateTimeType, ObNumberTC, ADD|SUB).result_as(ObDateTimeType).cast_param2_as(ObNumberType).get_ret());
  OZ (new_rules(ObDateTimeType, ObFloatType, ADD|SUB).result_as(ObDateTimeType).cast_param2_as(ObNumberType).get_ret());
  OZ (new_rules(ObDateTimeType, ObDoubleType, ADD|SUB).result_as(ObDateTimeType).cast_param2_as(ObNumberType).get_ret());
  OZ (new_rules(ObDateTimeType, ObStringTC, ADD|SUB).result_as(ObDateTimeType).cast_param2_as(ObNumberType).get_ret());
  OZ (new_rules(ObDateTimeType, ObDateTimeType, SUB).result_as(ObNumberType).cast_param1_as(ObDateTimeType).cast_param2_as(ObDateTimeType).get_ret());
  OZ (new_rules(ObDateTimeType, ObOTimestampTC, SUB).result_as(ObIntervalDSType).cast_param1_as(ObDateTimeType).cast_param2_as(C::SECOND).get_ret());
  OZ (new_rules(ObDateTimeType, ObDateTimeType, ROUND).result_as(ObDateTimeType).get_ret());
  OZ (new_rules(ObDateTimeType, ObIntervalTC, ADD|SUB).result_as(ObDateTimeType).cast_param2_as(C::SECOND).get_ret());


  //TIMESTAMP op others
  OZ (new_rules(ObOTimestampTC, ObNullTC, ADD|SUB).result_as(ObDateTimeType).cast_param2_as(C::SECOND).get_ret());
  OZ (new_rules(ObOTimestampTC, ObIntTC, ADD|SUB).result_as(ObDateTimeType).cast_param2_as(ObNumberType).get_ret());
  OZ (new_rules(ObOTimestampTC, ObNumberTC, ADD|SUB).result_as(ObDateTimeType).cast_param2_as(ObNumberType).get_ret());
  OZ (new_rules(ObOTimestampTC, ObFloatType, ADD|SUB).result_as(ObDateTimeType).cast_param2_as(ObNumberType).get_ret());
  OZ (new_rules(ObOTimestampTC, ObDoubleType, ADD|SUB).result_as(ObDateTimeType).cast_param2_as(ObNumberType).get_ret());
  OZ (new_rules(ObOTimestampTC, ObStringTC, ADD|SUB).result_as(ObDateTimeType).cast_param2_as(ObNumberType).get_ret());
  OZ (new_rules(ObOTimestampTC, ObDateTimeType, SUB).result_as(ObIntervalDSType).cast_param1_as(C::FIRST).cast_param2_as(C::FIRST).get_ret());
  OZ (new_rules(ObOTimestampTC, ObOTimestampTC, SUB).result_as(ObIntervalDSType).cast_param1_as(C::FIRST).cast_param2_as(C::SECOND).get_ret());
  OZ (new_rules(ObOTimestampTC, ObOTimestampTC, ROUND).result_as(ObDateTimeType).get_ret());
  OZ (new_rules(ObOTimestampTC, ObIntervalTC, ADD|SUB).result_as(C::FIRST).cast_param2_as(C::SECOND).get_ret());


  //INTERVAL op others
  OZ (new_rules(ObIntervalTC, ObNullTC, MUL|DIV).result_as(C::FIRST).cast_param2_as(C::SECOND).get_ret());
  OZ (new_rules(ObIntervalTC, ObIntTC, MUL|DIV).result_as(C::FIRST).cast_param2_as(C::SECOND).get_ret());
  OZ (new_rules(ObIntervalTC, ObNumberTC, MUL|DIV).result_as(C::FIRST).cast_param2_as(ObNumberType).get_ret());
  OZ (new_rules(ObIntervalTC, ObFloatType, MUL|DIV).result_as(C::FIRST).cast_param2_as(ObNumberType).get_ret());
  OZ (new_rules(ObIntervalTC, ObDoubleType, MUL|DIV).result_as(C::FIRST).cast_param2_as(ObNumberType).get_ret());
  OZ (new_rules(ObIntervalTC, ObStringTC, MUL|DIV).result_as(C::FIRST).cast_param2_as(ObNumberType).get_ret());
  OZ (new_rules(ObIntervalTC, ObDateTimeType, ADD).result_as(ObDateTimeType).cast_param1_as(C::FIRST).get_ret());
  OZ (new_rules(ObIntervalTC, ObOTimestampTC, ADD).result_as(C::SECOND).cast_param1_as(C::FIRST).get_ret());
  OZ (new_rules(ObIntervalYMType, ObIntervalYMType, ADD|SUB).result_as(ObIntervalYMType).get_ret());
  OZ (new_rules(ObIntervalDSType, ObIntervalDSType, ADD|SUB).result_as(ObIntervalDSType).get_ret());

  return ret;
}

ObArithResultTypeMap ARITH_RESULT_TYPE_ORACLE;

}
}

