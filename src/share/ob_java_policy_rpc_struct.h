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

#ifndef OCEANBASE_SRC_SHARE_OB_JAVA_POLICY_RPC_STRUCT_H_
#define OCEANBASE_SRC_SHARE_OB_JAVA_POLICY_RPC_STRUCT_H_

#include "share/ob_ddl_args.h"
#include "share/schema/ob_java_policy_mgr.h"

namespace oceanbase
{

namespace obrpc
{

struct ObCreateJavaPolicyArg final : public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  using JavaPolicyKind = share::schema::ObSimpleJavaPolicySchema::JavaPolicyKind;
  using JavaPolicyStatus = share::schema::ObSimpleJavaPolicySchema::JavaPolicyStatus;

  ObCreateJavaPolicyArg()
    : tenant_id_(common::OB_INVALID_TENANT_ID),
      kind_(JavaPolicyKind::INVALID),
      grantee_(common::OB_INVALID_ID),
      type_schema_(common::OB_INVALID_ID),
      status_(JavaPolicyStatus::INVALID)
  {
    type_name_.reset();
    name_.reset();
    action_.reset();
  }

  ObCreateJavaPolicyArg(const uint64_t &tenant_id,
                        const JavaPolicyKind kind,
                        const uint64_t grantee,
                        const uint64_t type_schema,
                        const common::ObString &type_name,
                        const common::ObString &name,
                        const common::ObString &action,
                        const JavaPolicyStatus status)
    : tenant_id_(tenant_id),
      kind_(kind),
      grantee_(grantee),
      type_schema_(type_schema),
      type_name_(type_name),
      name_(name),
      action_(action),
      status_(status)
  {
    exec_tenant_id_ = tenant_id;
  }

  int assign(const ObCreateJavaPolicyArg &arg);
  bool is_valid() const;

  TO_STRING_KV(K_(tenant_id), K_(kind), K_(grantee), K_(type_schema), K_(type_name), K_(name), K_(action), K_(status));

public:
  uint64_t tenant_id_;
  JavaPolicyKind kind_;
  uint64_t grantee_;
  uint64_t type_schema_;
  common::ObString type_name_;
  common::ObString name_;
  common::ObString action_;
  JavaPolicyStatus status_;
};

struct ObDropJavaPolicyArg final : public ObDDLArg
{
  OB_UNIS_VERSION(1);

public:
  ObDropJavaPolicyArg()
    : tenant_id_(common::OB_INVALID_TENANT_ID),
      key_(common::OB_INVALID_ID)
  {  }

  ObDropJavaPolicyArg(const uint64_t &tenant_id,
                      const uint64_t key)
    : tenant_id_(tenant_id),
      key_(key)
  {
    exec_tenant_id_ = tenant_id;
  }

  int assign(const ObDropJavaPolicyArg &arg);
  bool is_valid() const;

  TO_STRING_KV(K_(tenant_id), K_(key));

public:
  uint64_t tenant_id_;
  uint64_t key_;
};

struct ObModifyJavaPolicyArg final : public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  using JavaPolicyStatus = share::schema::ObSimpleJavaPolicySchema::JavaPolicyStatus;

  ObModifyJavaPolicyArg()
    : tenant_id_(common::OB_INVALID_TENANT_ID),
      status_(JavaPolicyStatus::INVALID),
      key_(common::OB_INVALID_ID)
  {
  }

  ObModifyJavaPolicyArg(const uint64_t &tenant_id,
                        const JavaPolicyStatus status,
                        const uint64_t key)
    : tenant_id_(tenant_id),
      status_(status),
      key_(key)
  {
    exec_tenant_id_ = tenant_id;
  }

  int assign(const ObModifyJavaPolicyArg &arg);
  bool is_valid() const;

  TO_STRING_KV(K_(tenant_id), K_(status), K_(key));

public:
  uint64_t tenant_id_;
  JavaPolicyStatus status_;
  uint64_t key_;
};

struct ObCreateJavaPolicyRes
{
  OB_UNIS_VERSION(1);

public:
  ObCreateJavaPolicyRes() : key_(common::OB_INVALID_ID), schema_version_(common::OB_INVALID_VERSION) {}

  TO_STRING_KV(K_(key), K_(schema_version));

  uint64_t key_;
  int64_t schema_version_;
};

struct ObDropJavaPolicyRes
{
  OB_UNIS_VERSION(1);

public:
  ObDropJavaPolicyRes() : key_(common::OB_INVALID_ID), schema_version_(common::OB_INVALID_VERSION) {}

  TO_STRING_KV(K_(key), K_(schema_version));

  uint64_t key_;
  int64_t schema_version_;
};

struct ObModifyJavaPolicyRes
{
  OB_UNIS_VERSION(1);

public:
  ObModifyJavaPolicyRes() : key_(common::OB_INVALID_ID), schema_version_(common::OB_INVALID_VERSION) {}

  TO_STRING_KV(K_(key), K_(schema_version));

  uint64_t key_;
  int64_t schema_version_;
};

} // namespace obrpc
} // namespace oceanbase

#endif // OCEANBASE_SRC_SHARE_OB_JAVA_POLICY_RPC_STRUCT_H_
