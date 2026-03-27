/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_I_COLLECT_VALUE_H
#define OCEANBASE_SQL_OB_I_COLLECT_VALUE_H
namespace oceanbase
{
namespace sql
{
enum StatType
{
  OB_INVALID_STAT_TYPE = 0,
  PLAN_MONITOR_INFO
};

class ObIValue
{
public:
  explicit ObIValue(StatType type) { value_type_ = type; }
  virtual ~ObIValue() {}
  StatType get_type()const { return  value_type_; }
protected:
  StatType value_type_;
};
} //namespace sql
} //namespace oceanbase
#endif


