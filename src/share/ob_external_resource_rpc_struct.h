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

#ifndef OCEANBASE_SRC_SHARE_OB_EXTERNAL_RESOURCE_RPC_STRUCT_H_
#define OCEANBASE_SRC_SHARE_OB_EXTERNAL_RESOURCE_RPC_STRUCT_H_

#include "share/ob_ddl_args.h"
#include "share/schema/ob_external_resource_mgr.h"

namespace oceanbase
{

namespace obrpc
{

struct ObCreateExternalResourceArg final : public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  ObCreateExternalResourceArg()
    : tenant_id_(OB_INVALID_TENANT_ID),
      database_id_(OB_INVALID_ID),
      type_(share::schema::ObSimpleExternalResourceSchema::INVALID_TYPE)
  {
    name_.reset();
    content_.reset();
    comment_.reset();
  }

  ObCreateExternalResourceArg(const uint64_t &tenant_id,
                              const uint64_t &database_id,
                              const ObString &external_resource_name,
                              const share::schema::ObSimpleExternalResourceSchema::ResourceType &external_resource_type,
                              const ObString &content,
                              const ObString &comment)
    : tenant_id_(tenant_id),
      database_id_(database_id),
      name_(external_resource_name),
      type_(external_resource_type),
      content_(content),
      comment_(comment)
  {
    exec_tenant_id_ = tenant_id;
  }

  int assign(const ObCreateExternalResourceArg &arg);
  bool is_valid() const;

  TO_STRING_KV(K_(tenant_id), K_(database_id), K_(name), K_(type), K_(content), K_(comment));

public:
  uint64_t tenant_id_;
  uint64_t database_id_;
  ObString name_;
  share::schema::ObSimpleExternalResourceSchema::ResourceType type_;
  ObString content_;
  ObString comment_;
};

struct ObDropExternalResourceArg final : public ObDDLArg
{
  OB_UNIS_VERSION(1);

public:
  ObDropExternalResourceArg()
    : tenant_id_(OB_INVALID_TENANT_ID),
      database_id_(OB_INVALID_ID),
      type_(share::schema::ObSimpleExternalResourceSchema::INVALID_TYPE)
  {
    name_.reset();
  }

  ObDropExternalResourceArg(const uint64_t &tenant_id,
                            const uint64_t &database_id,
                            const ObString &external_resource_name,
                            const share::schema::ObSimpleExternalResourceSchema::ResourceType type)
    : tenant_id_(tenant_id),
      database_id_(database_id),
      name_(external_resource_name),
      type_(type)
  {
    exec_tenant_id_ = tenant_id;
  }

  int assign(const ObDropExternalResourceArg &arg);
  bool is_valid() const;

  TO_STRING_KV(K_(tenant_id), K_(database_id), K_(name), K_(type));

public:
  uint64_t tenant_id_;
  uint64_t database_id_;
  ObString name_;
  share::schema::ObSimpleExternalResourceSchema::ResourceType type_;
};

struct ObCreateExternalResourceRes final
{
  OB_UNIS_VERSION(1);

public:
  ObCreateExternalResourceRes()
    : resource_id_(OB_INVALID_ID),
      schema_version_(OB_INVALID_VERSION)
  {  }

  inline int assign(const ObCreateExternalResourceRes &other)
  {
    int ret = OB_SUCCESS;

    resource_id_ = other.resource_id_;
    schema_version_ = other.schema_version_;

    return ret;
  }

  TO_STRING_KV(K_(resource_id), K_(schema_version));

public:
  uint64_t resource_id_;
  int64_t schema_version_;
};

struct ObDropExternalResourceRes final
{
  OB_UNIS_VERSION(1);

public:
  ObDropExternalResourceRes()
    : resource_id_(OB_INVALID_ID),
      schema_version_(OB_INVALID_VERSION),
      type_(share::schema::ObSimpleExternalResourceSchema::INVALID_TYPE)
  {  }

  inline int assign(const ObDropExternalResourceRes &other)
  {
    int ret = OB_SUCCESS;

    resource_id_ = other.resource_id_;
    schema_version_ = other.schema_version_;
    type_ = other.type_;

    return ret;
  }

  TO_STRING_KV(K_(resource_id), K_(schema_version), K_(type));

public:
  uint64_t resource_id_;
  int64_t schema_version_;
  share::schema::ObSimpleExternalResourceSchema::ResourceType type_;
};

} // namespace obrpc
} // namespace oceanbase

#endif // OCEANBASE_SRC_SHARE_OB_EXTERNAL_RESOURCE_RPC_STRUCT_H_