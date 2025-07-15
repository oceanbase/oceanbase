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

#include "ob_external_resource_rpc_struct.h"

namespace oceanbase
{

using namespace common;
using namespace sql;
using namespace share::schema;

namespace obrpc
{

OB_SERIALIZE_MEMBER((ObCreateExternalResourceArg, ObDDLArg), tenant_id_, database_id_, name_, type_, content_, comment_);

bool ObCreateExternalResourceArg::is_valid() const
{
  return is_valid_tenant_id(tenant_id_)
           && OB_INVALID_ID != database_id_
           && !name_.empty()
           && ObSimpleExternalResourceSchema::INVALID_TYPE != type_;
}

int ObCreateExternalResourceArg::assign(const ObCreateExternalResourceArg &other)
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
    database_id_ = other.database_id_;
    name_ = other.name_;
    type_ = other.type_;
    content_ = other.content_;
    comment_ = other.comment_;
  }

  return ret;
}

OB_SERIALIZE_MEMBER((ObDropExternalResourceArg, ObDDLArg), tenant_id_, database_id_, name_, type_);

bool ObDropExternalResourceArg::is_valid() const
{
  return is_valid_tenant_id(tenant_id_)
           && OB_INVALID_ID != database_id_
           && !name_.empty();
}

int ObDropExternalResourceArg::assign(const ObDropExternalResourceArg &other)
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
    database_id_ = other.database_id_;
    name_ = other.name_;
    type_ = other.type_;
  }

  return ret;
}

OB_SERIALIZE_MEMBER(ObCreateExternalResourceRes, resource_id_, schema_version_);
OB_SERIALIZE_MEMBER(ObDropExternalResourceRes, resource_id_, schema_version_, type_);

} // namespace obrpc
} // namespace oceanbase
