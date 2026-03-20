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

struct ObOraUploadJarArg final : public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  ObOraUploadJarArg()
    : tenant_id_(OB_INVALID_TENANT_ID),
      database_id_(OB_INVALID_ID),
      is_force_(false)
  {
    jar_name_.reset();
    jar_binary_.reset();
    class_names_.reset();
  }

  ObOraUploadJarArg(const uint64_t &tenant_id,
                    const uint64_t &database_id,
                    const ObString &jar_name,
                    const ObString &jar_binary,
                    const bool is_force)
    : tenant_id_(tenant_id),
      database_id_(database_id),
      jar_name_(jar_name),
      jar_binary_(jar_binary),
      is_force_(is_force)
  {
    exec_tenant_id_ = tenant_id;
  }

  inline int assign_class_names(const ObIArray<ObString> &class_names)
  {
    return class_names_.assign(class_names);
  }

  int assign(const ObOraUploadJarArg &arg);
  bool is_valid() const;

  TO_STRING_KV(K_(tenant_id), K_(database_id), K_(jar_name), K_(jar_binary), K_(class_names), K_(is_force));

public:
  uint64_t tenant_id_;
  uint64_t database_id_;
  ObString jar_name_;
  ObString jar_binary_;
  ObSEArray<ObString, 8> class_names_;
  bool is_force_;
};

struct ObOraUploadJarRes final
{
  OB_UNIS_VERSION(1);

public:
  ObOraUploadJarRes()
    : jar_id_(OB_INVALID_ID),
      schema_version_(OB_INVALID_VERSION),
      del_jar_id_(OB_INVALID_ID)
  {
    class_ids_.reset();
    del_class_ids_.reset();
  }

  inline int assign(const ObOraUploadJarRes &other)
  {
    int ret = OB_SUCCESS;

    if (OB_FAIL(class_ids_.assign(other.class_ids_))) {
      SHARE_LOG(WARN, "failed to assign class ids", K(ret), K(other));
    } else if (OB_FAIL(del_class_ids_.assign(other.del_class_ids_))) {
      SHARE_LOG(WARN, "failed to assign del class ids", K(ret), K(other));
    } else {
      jar_id_ = other.jar_id_;
      schema_version_ = other.schema_version_;
    }

    return ret;
  }

  TO_STRING_KV(K_(jar_id), K_(schema_version), K_(class_ids), K_(del_class_ids));

public:
  uint64_t jar_id_;
  int64_t schema_version_;
  ObSEArray<uint64_t, 8> class_ids_;
  uint64_t del_jar_id_;
  ObSEArray<uint64_t, 8> del_class_ids_;
};

struct ObOraDropJarArg final : public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  ObOraDropJarArg()
    : tenant_id_(OB_INVALID_TENANT_ID),
      jar_id_(OB_INVALID_ID)
  {
    class_names_.reset();
  }

  ObOraDropJarArg(const uint64_t &tenant_id,
                  const uint64_t &jar_id)
    : tenant_id_(tenant_id),
      jar_id_(jar_id)
  {
    exec_tenant_id_ = tenant_id;
  }

  inline int assign_class_names(const ObIArray<ObString> &class_names)
  {
    return class_names_.assign(class_names);
  }

  int assign(const ObOraDropJarArg &arg);
  bool is_valid() const;

  TO_STRING_KV(K_(tenant_id), K_(jar_id), K_(class_names));

public:
  uint64_t tenant_id_;

  // jar resource can't be replaced, so a resource id can be used to identify a jar resource,
  // database id or schema version is not required for dropping a jar resource.
  uint64_t jar_id_;
  ObSEArray<ObString, 8> class_names_;
};

struct ObOraDropJarRes final
{
  OB_UNIS_VERSION(1);

public:
  ObOraDropJarRes()
    : jar_id_(OB_INVALID_ID),
      schema_version_(OB_INVALID_VERSION)
  {
    del_class_ids_.reset();
  }

  inline int assign(const ObOraDropJarRes &other)
  {
    int ret = OB_SUCCESS;

    if (OB_FAIL(del_class_ids_.assign(other.del_class_ids_))) {
      SHARE_LOG(WARN, "failed to assign del class ids", K(ret), K(other));
    } else {
      jar_id_ = other.jar_id_;
      schema_version_ = other.schema_version_;
    }

    return ret;
  }

  TO_STRING_KV(K_(jar_id), K_(schema_version), K_(del_class_ids));

public:
  uint64_t jar_id_;
  int64_t schema_version_;
  ObSEArray<uint64_t, 8> del_class_ids_;
};

} // namespace obrpc
} // namespace oceanbase

#endif // OCEANBASE_SRC_SHARE_OB_EXTERNAL_RESOURCE_RPC_STRUCT_H_