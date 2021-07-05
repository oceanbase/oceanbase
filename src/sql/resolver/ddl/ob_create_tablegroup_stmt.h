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

#ifndef OCEANBASE_SQL_OB_CREATE_TABLEGROUP_STMT_
#define OCEANBASE_SQL_OB_CREATE_TABLEGROUP_STMT_

#include "share/ob_rpc_struct.h"
#include "sql/resolver/ddl/ob_tablegroup_stmt.h"

namespace oceanbase {
namespace sql {
class ObCreateTablegroupStmt : public ObTablegroupStmt {
public:
  ObCreateTablegroupStmt();
  explicit ObCreateTablegroupStmt(common::ObIAllocator* name_pool);
  virtual ~ObCreateTablegroupStmt();

  virtual void set_tenant_id(const uint64_t tenant_id) override;

  void set_if_not_exists(bool if_not_exists);
  int set_tablegroup_name(const common::ObString& tablegroup_name);
  const common::ObString& get_tablegroup_name() const;
  int set_create_mode(const obrpc::ObCreateTableMode create_mode);
  virtual int set_primary_zone(const common::ObString& zone) override;
  virtual int set_locality(const common::ObString& locality) override;
  obrpc::ObCreateTablegroupArg& get_create_tablegroup_arg();
  virtual obrpc::ObDDLArg& get_ddl_arg()
  {
    return create_tablegroup_arg_;
  }
  virtual int set_tablegroup_id(uint64_t tablegroup_id) override;
  virtual int set_binding(const bool binding) override;
  virtual int set_max_used_part_id(int64_t max_used_part_id) override;
  uint64_t get_tablegroup_id() const
  {
    return create_tablegroup_arg_.tablegroup_schema_.get_tablegroup_id();
  }

  int64_t get_max_used_part_id() const
  {
    return create_tablegroup_arg_.tablegroup_schema_.get_part_option().get_max_used_part_id();
  }
  TO_STRING_KV(K_(create_tablegroup_arg));

private:
  obrpc::ObCreateTablegroupArg create_tablegroup_arg_;
  DISALLOW_COPY_AND_ASSIGN(ObCreateTablegroupStmt);
};

inline int ObCreateTablegroupStmt::set_create_mode(const obrpc::ObCreateTableMode create_mode)
{
  int ret = common::OB_SUCCESS;
  if (create_mode <= obrpc::OB_CREATE_TABLE_MODE_INVALID || create_mode >= obrpc::OB_CREATE_TABLE_MODE_MAX) {
    ret = common::OB_INVALID_ARGUMENT;
  } else {
    create_tablegroup_arg_.create_mode_ = create_mode;
  }
  return ret;
}

inline const common::ObString& ObCreateTablegroupStmt::get_tablegroup_name() const
{
  return create_tablegroup_arg_.tablegroup_schema_.get_tablegroup_name();
}

inline int ObCreateTablegroupStmt::set_tablegroup_name(const common::ObString& tablegroup_name)
{
  return create_tablegroup_arg_.tablegroup_schema_.set_tablegroup_name(tablegroup_name);
}

inline int ObCreateTablegroupStmt::set_tablegroup_id(uint64_t tablegroup_id)
{
  int ret = OB_SUCCESS;
  create_tablegroup_arg_.tablegroup_schema_.set_tablegroup_id(tablegroup_id);
  return ret;
}

inline int ObCreateTablegroupStmt::set_max_used_part_id(int64_t max_used_part_id)
{
  int ret = OB_SUCCESS;
  create_tablegroup_arg_.tablegroup_schema_.get_part_option().set_max_used_part_id(max_used_part_id);
  return ret;
}

inline int ObCreateTablegroupStmt::set_binding(const bool binding)
{
  int ret = OB_SUCCESS;
  create_tablegroup_arg_.tablegroup_schema_.set_binding(binding);
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
#endif  // OCEANBASE_SQL_OB_CREATE_TABLEGROUP_STMT_
