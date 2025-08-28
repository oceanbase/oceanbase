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

 #define USING_LOG_PREFIX SHARE_SCHEMA
 #include "share/schema/ob_sensitive_rule_schema_struct.h"
 namespace oceanbase
{
namespace share
{
namespace schema
{
ObSensitiveRuleSchema::ObSensitiveRuleSchema() : ObSchema()
{
  reset(); 
}

ObSensitiveRuleSchema::ObSensitiveRuleSchema(const ObSensitiveRuleSchema &src_schema) : ObSchema()
{
  reset();
  *this = src_schema;
}

ObSensitiveRuleSchema::ObSensitiveRuleSchema(ObIAllocator *allocator) : ObSchema(allocator)
{
  reset();
}

ObSensitiveRuleSchema::~ObSensitiveRuleSchema()
{
}

ObSensitiveRuleSchema &ObSensitiveRuleSchema::operator=(const ObSensitiveRuleSchema &src_schema)
{
  if (this != &src_schema) {
    reset();
    int ret = OB_SUCCESS;
    set_enabled(src_schema.enabled_);
    set_tenant_id(src_schema.tenant_id_);
    set_sensitive_rule_id(src_schema.sensitive_rule_id_);
    set_protection_policy(src_schema.protection_policy_);
    set_schema_version(src_schema.schema_version_);
    if (OB_FAIL(set_sensitive_rule_name(src_schema.sensitive_rule_name_))) {
      LOG_WARN("set_sensitive_rule_name failed", K(ret));
    } else if (OB_FAIL(set_method(src_schema.method_))) {
      LOG_WARN("set_method failed", K(ret));
    } else if (OB_FAIL(set_sensitive_field_items(src_schema.sensitive_field_items_))) {
    } else { /* no more to do */ }

    if (OB_FAIL(ret)) {
      error_ret_ = ret;
    }
  }
  return *this;
}

int ObSensitiveRuleSchema::assign(const ObSensitiveRuleSchema &src_schema)
{
  int ret = OB_SUCCESS;
  *this = src_schema;
  ret = get_err_ret();
  return ret;
}

void ObSensitiveRuleSchema::reset()
{
  enabled_ = false;
  tenant_id_ = OB_INVALID_ID;
  sensitive_rule_id_ = OB_INVALID_ID;
  protection_policy_ = 0;
  schema_version_ = OB_INVALID_VERSION;
  sensitive_rule_name_.reset();
  method_.reset();
  sensitive_field_items_.reset();
}

int64_t ObSensitiveRuleSchema::get_convert_size() const
{
  int64_t convert_size = sizeof(*this);
  convert_size += sensitive_rule_name_.length() + 1;
  convert_size += method_.length() + 1;
  convert_size += sensitive_field_items_.count() * sizeof(ObSensitiveFieldItem);
  return convert_size;
}

bool ObSensitiveRuleSchema::is_valid() const
{
  bool ret = true;
  if (!ObSchema::is_valid()
      || !is_valid_tenant_id(tenant_id_)
      || !is_valid_id(sensitive_rule_id_)
      || schema_version_ < 0
      || sensitive_rule_name_.empty()) {
    ret = false;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObSensitiveFieldItem, table_id_, column_id_);

DEF_TO_STRING(ObSensitiveFieldItem)
{
  int64_t pos = 0;
  J_KV(K_(table_id),
       K_(column_id));
  return pos;
}

OB_SERIALIZE_MEMBER(ObSensitiveRuleSchema,
                    enabled_,
                    tenant_id_,
                    sensitive_rule_id_,
                    protection_policy_,
                    schema_version_,
                    sensitive_rule_name_,
                    method_,
                    sensitive_field_items_);

ObSensitiveColumnSchema::ObSensitiveColumnSchema() : ObSchema()
{
  reset();
}

ObSensitiveColumnSchema::ObSensitiveColumnSchema(const ObSensitiveColumnSchema &other) : ObSchema()
{
  reset();
  *this = other;
}

ObSensitiveColumnSchema::ObSensitiveColumnSchema(ObIAllocator *allocator) : ObSchema(allocator)
{
  reset();
}

ObSensitiveColumnSchema::~ObSensitiveColumnSchema()
{
}

ObSensitiveColumnSchema &ObSensitiveColumnSchema::operator=(const ObSensitiveColumnSchema &other)
{
  if (this != &other) {
    reset();
    tenant_id_ = other.tenant_id_;
    sensitive_rule_id_ = other.sensitive_rule_id_;
    table_id_ = other.table_id_;
    column_id_ = other.column_id_;
    schema_version_ = other.schema_version_;
  }
  return *this;
}

int ObSensitiveColumnSchema::assign(const ObSensitiveColumnSchema &other)
{
  int ret = OB_SUCCESS;
  *this = other;
  ret = get_err_ret();
  return ret;
}

void ObSensitiveColumnSchema::reset()
{
  tenant_id_ = OB_INVALID_ID;
  sensitive_rule_id_ = OB_INVALID_ID;
  table_id_ = OB_INVALID_ID;
  column_id_ = OB_INVALID_ID;
  schema_version_ = OB_INVALID_ID;
}

int64_t ObSensitiveColumnSchema::get_convert_size() const
{
  int64_t convert_size = sizeof(*this);
  return convert_size;
}

bool ObSensitiveColumnSchema::is_valid() const
{
  return ObSchema::is_valid()
      && is_valid_tenant_id(tenant_id_)
      && is_valid_id(sensitive_rule_id_)
      && is_valid_id(table_id_)
      && is_valid_id(column_id_)
      && schema_version_ >= 0;
}

OB_SERIALIZE_MEMBER(ObSensitiveColumnSchema,
                    tenant_id_,
                    sensitive_rule_id_,
                    table_id_,
                    column_id_,
                    schema_version_);

ObSensitiveRulePriv &ObSensitiveRulePriv::operator=(const ObSensitiveRulePriv &other)
{
  if (this != &other) {
    reset();
    int ret = OB_SUCCESS;
    error_ret_ = other.error_ret_;
    if (OB_FAIL(ObPriv::assign(other))) {
      LOG_WARN("assign failed", K(ret));
    } else if (OB_FAIL(deep_copy_str(other.sensitive_rule_, sensitive_rule_))) {
      LOG_WARN("failed to deep copy catalog", K(ret));
    }
    if (OB_FAIL(ret)) {
      error_ret_ = ret;
    }
  }
  return *this;
}

void ObSensitiveRulePriv::reset()
{
  sensitive_rule_.reset();
  ObSchema::reset();
  ObPriv::reset();
}

int64_t ObSensitiveRulePriv::get_convert_size() const
{
  int64_t convert_size = sizeof(ObSensitiveRulePriv) - sizeof(ObPriv);
  convert_size += ObPriv::get_convert_size();
  convert_size += sensitive_rule_.length() + 1;
  return convert_size;
}

OB_SERIALIZE_MEMBER((ObSensitiveRulePriv, ObPriv), 
                    sensitive_rule_);

}
}
}