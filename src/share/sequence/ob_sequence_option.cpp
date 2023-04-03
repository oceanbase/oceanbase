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

#define USING_LOG_PREFIX SHARE
#include "share/sequence/ob_sequence_option.h"
#include "share/ob_define.h"

using namespace oceanbase::common;
using namespace oceanbase::common::number;
using namespace oceanbase::share;

OB_SERIALIZE_MEMBER(ObSequenceValue,
                    val_);

OB_SERIALIZE_MEMBER(ObSequenceOption,
                    increment_by_,
                    start_with_,
                    maxvalue_,
                    minvalue_,
                    cache_,
                    cycle_,
                    order_);


ObSequenceMaxMinInitializer::ObSequenceMaxMinInitializer()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(MAX_VALUE.set("9999999999999999999999999999"))) {
    LOG_ERROR("fail set max value", K(ret));
  } else if (OB_FAIL(MIN_VALUE.set("-999999999999999999999999999"))) {
    LOG_ERROR("fail set min value", K(ret));
  } else if (OB_FAIL(NO_MAX_VALUE.set("10000000000000000000000000000"))) {
    LOG_ERROR("fail set max value", K(ret));
  } else if (OB_FAIL(NO_MIN_VALUE.set("-1000000000000000000000000000"))) {
    LOG_ERROR("fail set min value", K(ret));
  } else if (OB_FAIL(MYSQL_MAX_VALUE.set("9223372036854775807"))) {
    LOG_ERROR("fail set max value", K(ret));
  } else if (OB_FAIL(MYSQL_MIN_VALUE.set("-9223372036854775808"))) {
    LOG_ERROR("fail set min value", K(ret));
  } else if (OB_FAIL(MYSQL_NO_MAX_VALUE.set("9223372036854775808"))) {
    LOG_ERROR("fail set max value", K(ret));
  } else if (OB_FAIL(MYSQL_NO_MIN_VALUE.set("-9223372036854775809"))) {
    LOG_ERROR("fail set min value", K(ret));
  }
}

// for Oracle Mode
ObSequenceValue ObSequenceMaxMinInitializer::NO_MAX_VALUE;
ObSequenceValue ObSequenceMaxMinInitializer::NO_MIN_VALUE;
ObSequenceValue ObSequenceMaxMinInitializer::MAX_VALUE;
ObSequenceValue ObSequenceMaxMinInitializer::MIN_VALUE;

// for MySQL Mode (only support values range in MIN_INT64 ~ MAX_INT64)
ObSequenceValue ObSequenceMaxMinInitializer::MYSQL_NO_MAX_VALUE;
ObSequenceValue ObSequenceMaxMinInitializer::MYSQL_NO_MIN_VALUE;
ObSequenceValue ObSequenceMaxMinInitializer::MYSQL_MAX_VALUE;
ObSequenceValue ObSequenceMaxMinInitializer::MYSQL_MIN_VALUE;


// You only need to initialize one copy globally, which is used to assign values to MIN_VALUE and MAX_VALUE
static ObSequenceMaxMinInitializer max_min_intializer_;

ObSequenceValue::ObSequenceValue()
{
}

ObSequenceValue::ObSequenceValue(int64_t int_val)
{
  int ret = OB_SUCCESS;
  ObDataBuffer allocator(static_cast<char *>(buf_), ObNumber::MAX_BYTE_LEN);
  if (OB_SUCCESS != (ret = val_.from(int_val, allocator))) {
    LOG_ERROR("set a int value to number should never fail with current allocator",
              K(int_val), K(ret));
  }
}

int ObSequenceValue::assign(const ObSequenceValue &other)
{
  ObDataBuffer allocator(static_cast<char *>(buf_), ObNumber::MAX_BYTE_LEN);
  return val_.from(other.val_, allocator);
}

int ObSequenceValue::set(const common::number::ObNumber &val)
{
  ObDataBuffer allocator(static_cast<char *>(buf_), ObNumber::MAX_BYTE_LEN);
  return val_.from(val, allocator);
}

int ObSequenceValue::set(int64_t val)
{
  int ret = OB_SUCCESS;
  ObDataBuffer allocator(static_cast<char *>(buf_), ObNumber::MAX_BYTE_LEN);
  if (OB_SUCCESS != (ret = val_.from(val, allocator))) {
    LOG_ERROR("set a int value to number should never fail with current allocator",
              K(val), K(ret));
  }
  return ret;
}

// for init a very big int (28 of 9)
int ObSequenceValue::set(const char *val)
{
  ObDataBuffer allocator(static_cast<char *>(buf_), ObNumber::MAX_BYTE_LEN);
  return val_.from(val, allocator);
}



int ObSequenceOption::assign(const share::ObSequenceOption &from)
{
  int ret = common::OB_SUCCESS;
  OZ(increment_by_.assign(from.increment_by_));
  OZ(start_with_.assign(from.start_with_));
  OZ(maxvalue_.assign(from.maxvalue_));
  OZ(minvalue_.assign(from.minvalue_));
  OZ(cache_.assign(from.cache_));
  cycle_ = from.cycle_;
  order_ = from.order_;
  return ret;
}

