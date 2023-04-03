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


