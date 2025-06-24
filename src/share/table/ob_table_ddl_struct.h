/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#ifndef OCEANBASE_SHARE_TABLE_OB_DDL_STRUCT_H_
#define OCEANBASE_SHARE_TABLE_OB_DDL_STRUCT_H_
#include "lib/utility/ob_unify_serialize.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/container/ob_array_serialization.h"
#include "share/ob_rpc_struct.h"

namespace oceanbase
{

namespace table
{

class ObHTableDDLParam
{
  OB_UNIS_VERSION_PV();
public:
  ObHTableDDLParam() {}
  virtual ~ObHTableDDLParam() = default;
public:
  virtual bool is_valid() const = 0;
  virtual bool is_allow_when_upgrade() const = 0;
  virtual int assign(const ObHTableDDLParam &other) = 0;
  DECLARE_PURE_VIRTUAL_TO_STRING;
};

class ObCreateHTableDDLParam : public ObHTableDDLParam
{
  OB_UNIS_VERSION_V(1);
public:
  ObCreateHTableDDLParam();
  virtual ~ObCreateHTableDDLParam() = default;
public:
  virtual bool is_valid() const override;
  virtual bool is_allow_when_upgrade() const override;
  virtual int assign(const ObHTableDDLParam &other) override;
  TO_STRING_KV(K_(table_group_arg), K_(cf_arg_list));
public:
  obrpc::ObCreateTablegroupArg table_group_arg_;
  common::ObSArray<obrpc::ObCreateTableArg> cf_arg_list_;
};

struct ObSetKvAttributeParam : public ObHTableDDLParam
{
  OB_UNIS_VERSION_V(1);
public:
  ObSetKvAttributeParam() :
    ObHTableDDLParam(),
    session_id_(common::OB_INVALID_ID),
    database_name_(),
    table_group_name_(),
    is_disable_(false)
  {}
  virtual ~ObSetKvAttributeParam() = default;
  virtual bool is_valid() const override
  {
    return !database_name_.empty() && !table_group_name_.empty();
  }
  virtual bool is_allow_when_upgrade() const { return false; }
  virtual int assign(const ObHTableDDLParam &other) override;
  VIRTUAL_TO_STRING_KV(K_(session_id),
                       K_(database_name),
                       K_(table_group_name),
                       K_(is_disable));
public:
  uint64_t session_id_;
  ObString database_name_;
  ObString table_group_name_;
  bool is_disable_;
};

class ObDropHTableDDLParam : public ObHTableDDLParam
{
  OB_UNIS_VERSION_V(1);
public:
  ObDropHTableDDLParam();
  virtual ~ObDropHTableDDLParam() = default;
public:
  virtual bool is_valid() const override;
  virtual bool is_allow_when_upgrade() const override;
  virtual int assign(const ObHTableDDLParam &other) override;
  TO_STRING_KV(K_(table_group_arg));
public:
  obrpc::ObDropTablegroupArg table_group_arg_;
};

}  // end namespace table
}  // end namespace oceanbase

#endif /* OCEANBASE_SHARE_TABLE_OB_DDL_STRUCT_H_ */
