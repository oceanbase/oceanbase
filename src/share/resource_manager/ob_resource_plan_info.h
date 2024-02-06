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

#ifndef OB_SHARE_RESOURCE_MANAGER_OB_PLAN_INFO_H_
#define OB_SHARE_RESOURCE_MANAGER_OB_PLAN_INFO_H_

#include "lib/utility/ob_macro_utils.h"
#include "common/data_buffer.h"
#include "lib/string/ob_string.h"
#include "share/ob_define.h"

namespace oceanbase
{
namespace common
{
class ObString;
}
namespace share
{
enum ObFunctionType
{
  PRIO_COMPACTION_HIGH = 0,
  PRIO_HA_HIGH = 1,
  PRIO_COMPACTION_MID = 2,
  PRIO_HA_MID = 3,
  PRIO_COMPACTION_LOW = 4,
  PRIO_HA_LOW = 5,
  PRIO_DDL = 6,
  PRIO_DDL_HIGH = 7,
  MAX_FUNCTION_NUM
};

ObString get_io_function_name(ObFunctionType function_type);

// 为了便于作为 hash value，所以把 ObString 包一下
class ObResMgrVarcharValue
{
public:
  ObResMgrVarcharValue()
  {
    MEMSET(value_buf_, 0, sizeof(value_buf_));
  }
  ObResMgrVarcharValue(const ObResMgrVarcharValue &other)
  {
    (void)assign(other);
  }
  ObResMgrVarcharValue(const ObString &other)
  {
    (void)set_value(other);
  }
  int set_value(const common::ObString &value)
  {
    common::ObDataBuffer allocator(value_buf_, common::OB_MAX_RESOURCE_PLAN_NAME_LENGTH);
    return common::ob_write_string(allocator, value, value_);
  }
  // 自动隐式类型转换成 ObString
  operator const common::ObString& () const
  {
    return value_;
  }
  const common::ObString &get_value() const
  {
    return value_;
  }
  int assign(const ObResMgrVarcharValue &other)
  {
    return set_value(other.value_);
  }

  void reset()
  {
    value_.reset();
  }
  uint64_t hash() const
  {
    return value_.hash();
  }
  int compare(const ObResMgrVarcharValue &r) const
  {
    return value_.compare(r.value_);
  }
  bool operator== (const ObResMgrVarcharValue &other) const { return 0 == compare(other); }
  bool operator!=(const ObResMgrVarcharValue &other) const { return !operator==(other); }
  bool operator<(const ObResMgrVarcharValue &other) const { return -1 == compare(other); }
  TO_STRING_KV(K_(value));
private:
  common::ObString value_;
  char value_buf_[common::OB_MAX_RESOURCE_PLAN_NAME_LENGTH];
};

class ObGroupName : public ObResMgrVarcharValue
{
public:
  ObGroupName() {}
  int assign(const ObGroupName &other)
  {
    return ObResMgrVarcharValue::assign(other);
  }
};

class ObTenantFunctionKey {
public:
  ObTenantFunctionKey() : tenant_id_(0), func_name_()
  {}
  ObTenantFunctionKey(const uint64_t tenant_id, const ObResMgrVarcharValue &func_name) :
    tenant_id_(tenant_id), func_name_(func_name)
  {}
  ObTenantFunctionKey(const uint64_t tenant_id, const common::ObString &func_name) :
    tenant_id_(tenant_id), func_name_(func_name)
  {}
  int assign(const ObTenantFunctionKey &other)
  {
    tenant_id_ = other.tenant_id_;
    return func_name_.assign(other.func_name_);
  }
  uint64_t hash() const
  {
    return common::murmurhash(&tenant_id_, sizeof(tenant_id_), func_name_.hash());
  }
  int hash(uint64_t &hash_val) const
  {
    hash_val = hash();
    return OB_SUCCESS;
  }
  int compare(const ObTenantFunctionKey& r) const
  {
    int cmp = 0;
    if (tenant_id_ < r.tenant_id_) {
      cmp = -1;
    } else if (tenant_id_ == r.tenant_id_) {
      cmp = func_name_.compare(r.func_name_);
    } else {
      cmp = 1;
    }
    return cmp;
  }
  bool operator== (const ObTenantFunctionKey &other) const { return 0 == compare(other); }
  bool operator!=(const ObTenantFunctionKey &other) const { return !operator==(other); }
  bool operator<(const ObTenantFunctionKey &other) const { return -1 == compare(other); }
  TO_STRING_KV(K_(tenant_id), K_(func_name));

public:
  uint64_t tenant_id_;
  ObResMgrVarcharValue func_name_;
};

class ObPlanDirective
{
public:
  ObPlanDirective() :
      tenant_id_(common::OB_INVALID_ID),
      mgmt_p1_(100),
      utilization_limit_(100),
      min_iops_(0),
      max_iops_(100),
      weight_iops_(0),
      group_id_(),
      group_name_(),
      level_(1)
  {}
  ~ObPlanDirective() = default;
public:
  bool is_valid() const
  {
    bool bret =  min_iops_ >= 0 && min_iops_ <= 100 && max_iops_ >= 0 &&
                 max_iops_ <= 100 && weight_iops_ >= 0 && weight_iops_ <= 100 &&
                 min_iops_ <= max_iops_;
    return bret;
  }
  int set_tenant_id(const uint64_t tenant_id)
  {
    tenant_id_ = tenant_id;
    return common::OB_SUCCESS;
  }
  int set_mgmt_p1(const int64_t mgmt_p1)
  {
    mgmt_p1_ = mgmt_p1;
    return common::OB_SUCCESS;
  }
  int set_utilization_limit(const int64_t limit)
  {
    utilization_limit_ = limit;
    return common::OB_SUCCESS;
  }
  int set_min_iops(const int64_t min_iops)
  {
    min_iops_ = min_iops;
    return common::OB_SUCCESS;
  }
  int set_max_iops(const int64_t max_iops)
  {
    max_iops_ = max_iops;
    return common::OB_SUCCESS;
  }
  int set_weight_iops(const int64_t weight_iops)
  {
    weight_iops_ = weight_iops;
    return common::OB_SUCCESS;
  }
  void set_group_id(const uint64_t group_id)
  {
    group_id_ = group_id;
  }
  int set_group_or_subplan(const common::ObString &name)
  {
    return group_name_.set_value(name);
  }
  int assign(const ObPlanDirective &other);
  TO_STRING_KV(K_(tenant_id),
               "group_name", group_name_.get_value(),
               K_(mgmt_p1),
               K_(utilization_limit),
               K_(min_iops),
               K_(max_iops),
               K_(weight_iops),
               K_(group_id),
               K_(level));
public:
  uint64_t tenant_id_;
  int64_t mgmt_p1_;
  int64_t utilization_limit_;
  uint64_t min_iops_;
  uint64_t max_iops_;
  uint64_t weight_iops_;
  uint64_t group_id_;
  share::ObGroupName group_name_;
  int level_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObPlanDirective);
};

class ObResourceMappingRule
{
public:
  ObResourceMappingRule() :
      tenant_id_(common::OB_INVALID_ID),
      group_id_(0), //default: other
      attr_(),
      value_(),
      group_()
  {}
  ~ObResourceMappingRule() = default;
public:
  int set_tenant_id(uint64_t tenant_id)
  {
    tenant_id_ = tenant_id;
    return common::OB_SUCCESS;
  }
  int set_group_id(uint64_t group_id)
  {
    group_id_ = group_id;
    return common::OB_SUCCESS;
  }
  int set_group(const common::ObString &group)
  {
    return group_.set_value(group);
  }
  int set_attr(const common::ObString &attr)
  {
    return attr_.set_value(attr);
  }
  int set_value(const common::ObString &value)
  {
    return value_.set_value(value);
  }
  int assign(const ObResourceMappingRule &other);
  TO_STRING_KV(K_(tenant_id),
               K_(group_id),
               K_(attr),
               K_(value),
               K_(group));
public:
  uint64_t tenant_id_;
  uint64_t group_id_;
  share::ObResMgrVarcharValue attr_;
  share::ObResMgrVarcharValue value_;
  share::ObGroupName group_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObResourceMappingRule);
};

class ObResourceIdNameMappingRule
{
public:
  ObResourceIdNameMappingRule() :
      tenant_id_(common::OB_INVALID_ID),
      group_id_(),
      group_name_()
  {}
  ~ObResourceIdNameMappingRule() = default;
public:
  void set_tenant_id(uint64_t tenant_id)
  {
    tenant_id_ = tenant_id;
  }
  void set_group_id(uint64_t group_id)
  {
    group_id_ = group_id;
  }
  int set_group_name(const common::ObString &name)
  {
    return group_name_.set_value(name);
  }
  int assign(const ObResourceIdNameMappingRule &other);
  TO_STRING_KV(K_(tenant_id),
               K_(group_id),
               "group_name", group_name_.get_value());
public:
  uint64_t tenant_id_;
  uint64_t group_id_;
  ObGroupName group_name_;
private:
  char group_name_buf_[common::OB_MAX_RESOURCE_PLAN_NAME_LENGTH];
  DISALLOW_COPY_AND_ASSIGN(ObResourceIdNameMappingRule);
};


class ObResourceUserMappingRule
{
public:
  ObResourceUserMappingRule() :
      tenant_id_(common::OB_INVALID_ID),
      user_id_(),
      group_id_(),
      group_name_()
  {}
  ~ObResourceUserMappingRule() = default;
public:
  void set_tenant_id(uint64_t tenant_id)
  {
    tenant_id_ = tenant_id;
  }
  void set_user_id(uint64_t user_id)
  {
    user_id_ = user_id;
  }
  void set_group_id(uint64_t group_id)
  {
    group_id_ = group_id;
  }
  int set_group_name(const common::ObString &name)
  {
    return group_name_.set_value(name);
  }
  int assign(const ObResourceUserMappingRule &other);
  TO_STRING_KV(K_(tenant_id),
               K_(user_id),
               K_(group_id),
               "group_name", group_name_.get_value());
public:
  uint64_t tenant_id_;
  uint64_t user_id_;
  uint64_t group_id_;
  share::ObGroupName group_name_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObResourceUserMappingRule);
};

class ObTenantGroupKey {
public:
  ObTenantGroupKey() : tenant_id_(0), group_name_()
  {}
  ObTenantGroupKey(const uint64_t tenant_id, const ObGroupName &group_name) :
    tenant_id_(tenant_id), group_name_(group_name)
  {}
  int assign(const ObTenantGroupKey &other)
  {
    tenant_id_ = other.tenant_id_;
    return group_name_.assign(other.group_name_);
  }
  uint64_t hash() const
  {
    return common::murmurhash(&tenant_id_, sizeof(tenant_id_), group_name_.hash());
  }
  int hash(uint64_t &hash_val) const
  {
    hash_val = hash();
    return OB_SUCCESS;
  }
  int compare(const ObTenantGroupKey& r) const
  {
    int cmp = 0;
    if (tenant_id_ < r.tenant_id_) {
      cmp = -1;
    } else if (tenant_id_ == r.tenant_id_) {
      cmp = group_name_.compare(r.group_name_);
    } else {
      cmp = 1;
    }
    return cmp;
  }
  bool operator== (const ObTenantGroupKey &other) const { return 0 == compare(other); }
  bool operator!=(const ObTenantGroupKey &other) const { return !operator==(other); }
  bool operator<(const ObTenantGroupKey &other) const { return -1 == compare(other); }
  TO_STRING_KV(K_(tenant_id), K_(group_name));

public:
  uint64_t tenant_id_;
  share::ObGroupName group_name_;
};

class ObResourceColumnMappingRule
{
public:
  ObResourceColumnMappingRule() :
      tenant_id_(common::OB_INVALID_ID),
      database_id_(common::OB_INVALID_ID),
      table_name_(),
      column_name_(),
      literal_value_(),
      user_name_(),
      group_id_(),
      case_mode_(common::OB_NAME_CASE_INVALID)
  {}
  ~ObResourceColumnMappingRule() = default;
public:
  void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
  void set_database_id(uint64_t database_id) { database_id_ = database_id; }
  void set_table_name(common::ObString table_name) {table_name_ = table_name; }
  void set_column_name(common::ObString column_name) {column_name_ = column_name; }
  void set_literal_value(common::ObString literal_value) {literal_value_ = literal_value; }
  void set_user_name(common::ObString user_name) { user_name_ = user_name; }
  void set_group_id(uint64_t group_id) { group_id_ = group_id; }
  void set_case_mode(common:: ObNameCaseMode case_mode) { case_mode_ = case_mode; }
  int write_string_values(uint64_t tenant_id,
                          common::ObString table_name, common::ObString column_name,
                          common::ObString literal_value, common::ObString user_name,
                          ObIAllocator &allocator);
  void reset_table_column_name(ObIAllocator &allocator);
  void reset_user_name_literal(ObIAllocator &allocator);
  void reset(ObIAllocator &allocator)
  {
    reset_table_column_name(allocator);
    reset_user_name_literal(allocator);
  }
  int assign(const ObResourceColumnMappingRule &other);
  TO_STRING_KV(K_(tenant_id), K_(database_id), K_(table_name), K_(column_name),
              K_(literal_value), K_(user_name), K_(group_id), K_(case_mode));
public:
  uint64_t tenant_id_;
  uint64_t database_id_;
  common::ObString table_name_;
  common::ObString column_name_;
  common::ObString literal_value_;
  common::ObString user_name_;
  uint64_t group_id_;
  common:: ObNameCaseMode case_mode_;

private:
  //char group_name_buf_[common::OB_MAX_RESOURCE_PLAN_NAME_LENGTH];
  DISALLOW_COPY_AND_ASSIGN(ObResourceColumnMappingRule);
};

}
}
#endif /* OB_SHARE_RESOURCE_MANAGER_OB_PLAN_INFO_H_ */
//// end of header file
