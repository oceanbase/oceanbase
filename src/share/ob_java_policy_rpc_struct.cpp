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

#include "ob_java_policy_rpc_struct.h"
#include "share/schema/ob_schema_utils.h"
#include "share/schema/ob_java_policy_mgr.h"

namespace oceanbase
{

using namespace common;
using namespace share::schema;

namespace obrpc
{

OB_SERIALIZE_MEMBER((ObCreateJavaPolicyArg, ObDDLArg), tenant_id_, kind_, grantee_, type_schema_, type_name_, name_, action_, status_);

bool ObCreateJavaPolicyArg::is_valid() const
{
  return is_valid_tenant_id(tenant_id_)
           && ObSimpleJavaPolicySchema::is_valid_java_policy_kind(kind_)
           && OB_INVALID_ID != grantee_
           && OB_INVALID_ID != type_schema_
           && ObSimpleJavaPolicySchema::is_valid_java_policy_status(status_)
           && !type_name_.empty();
}

int ObCreateJavaPolicyArg::assign(const ObCreateJavaPolicyArg &other)
{
  int ret = OB_SUCCESS;

  if (this == &other) {
    // do nothing
  } else if (!other.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(other));
  } else if (OB_FAIL(ObDDLArg::assign(other))) {
    LOG_WARN("failed to assign base class", K(ret), K(*this), K(other));
  } else {
    tenant_id_ = other.tenant_id_;
    kind_ = other.kind_;
    grantee_ = other.grantee_;
    type_schema_ = other.type_schema_;
    type_name_ = other.type_name_;
    name_ = other.name_;
    action_ = other.action_;
    status_ = other.status_;
  }

  return ret;
}

OB_SERIALIZE_MEMBER((ObDropJavaPolicyArg, ObDDLArg), tenant_id_, key_);

bool ObDropJavaPolicyArg::is_valid() const
{
  return is_valid_tenant_id(tenant_id_) && (common::OB_INVALID_ID != key_);
}

int ObDropJavaPolicyArg::assign(const ObDropJavaPolicyArg &other)
{
  int ret = OB_SUCCESS;

  if (this == &other) {
    // do nothing
  } else if (!other.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(other));
  } else if (OB_FAIL(ObDDLArg::assign(other))) {
    LOG_WARN("failed to assign base class", K(ret), K(*this), K(other));
  } else {
    tenant_id_ = other.tenant_id_;
    key_ = other.key_;
  }

  return ret;
}

OB_SERIALIZE_MEMBER((ObModifyJavaPolicyArg, ObDDLArg), tenant_id_, status_, key_);

bool ObModifyJavaPolicyArg::is_valid() const
{
  return is_valid_tenant_id(tenant_id_) && ObSimpleJavaPolicySchema::is_valid_java_policy_status(status_) && common::OB_INVALID_ID != key_;
}

int ObModifyJavaPolicyArg::assign(const ObModifyJavaPolicyArg &other)
{
  int ret = OB_SUCCESS;

  if (this == &other) {
    // do nothing
  } else if (!other.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(other));
  } else if (OB_FAIL(ObDDLArg::assign(other))) {
    LOG_WARN("failed to assign base class", K(ret), K(*this), K(other));
  } else {
    tenant_id_ = other.tenant_id_;
    status_ = other.status_;
    key_ = other.key_;
  }

  return ret;
}

OB_SERIALIZE_MEMBER(ObCreateJavaPolicyRes, key_, schema_version_);
OB_SERIALIZE_MEMBER(ObDropJavaPolicyRes, key_, schema_version_);
OB_SERIALIZE_MEMBER(ObModifyJavaPolicyRes, key_, schema_version_);

} // namespace obrpc
} // namespace oceanbase
