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

#ifndef OCEANBASE_SQL_OB_ALTER_TABLEGROUP_STMT_
#define OCEANBASE_SQL_OB_ALTER_TABLEGROUP_STMT_

#include "share/schema/ob_schema_service.h"
#include "share/ob_rpc_struct.h"
#include "sql/resolver/ddl/ob_tablegroup_stmt.h"

namespace oceanbase
{
namespace sql
{

class ObAlterTablegroupStmt : public ObTablegroupStmt
{
public:
  explicit ObAlterTablegroupStmt(common::ObIAllocator *name_pool);
  ObAlterTablegroupStmt();
  virtual ~ObAlterTablegroupStmt();

  virtual void set_tenant_id(const uint64_t tenant_id) override;

  const common::ObString &get_tablegroup_name();
  int add_table_item(const obrpc::ObTableItem &table_item);
  void set_tablegroup_name(const common::ObString &tablegroup_name);
  obrpc::ObAlterTablegroupArg &get_alter_tablegroup_arg();
  virtual obrpc::ObDDLArg &get_ddl_arg() { return alter_tablegroup_arg_; }
  virtual int set_primary_zone(const common::ObString &zone) override;
  virtual int set_locality(const common::ObString &locality) override;
  virtual int set_tablegroup_sharding(const common::ObString &sharding) override;
  inline void set_alter_option_set(const common::ObBitSet<> &alter_option_set);
  bool is_alter_partition() const { return alter_tablegroup_arg_.is_alter_partitions(); }
  virtual int set_tablegroup_id(uint64_t tablegroup_id) override;

  TO_STRING_KV(K_(alter_tablegroup_arg));
private:
  obrpc::ObAlterTablegroupArg alter_tablegroup_arg_;
  DISALLOW_COPY_AND_ASSIGN(ObAlterTablegroupStmt);
};

inline int ObAlterTablegroupStmt::set_tablegroup_id(uint64_t tablegroup_id)
{
  int ret = OB_NOT_SUPPORTED;
  UNUSED(tablegroup_id);
  LOG_USER_ERROR(OB_NOT_SUPPORTED, "alter tablegroup with tablegroup_id");
  return ret;
}

inline obrpc::ObAlterTablegroupArg &ObAlterTablegroupStmt::get_alter_tablegroup_arg()
{
  return alter_tablegroup_arg_;
}

inline void ObAlterTablegroupStmt::set_tenant_id(const uint64_t tenant_id) {
  alter_tablegroup_arg_.tenant_id_ = tenant_id;
}

inline const common::ObString &ObAlterTablegroupStmt::get_tablegroup_name()
{
  return alter_tablegroup_arg_.tablegroup_name_;
}

inline void ObAlterTablegroupStmt::set_tablegroup_name(const common::ObString &tablegroup_name)
{
  alter_tablegroup_arg_.tablegroup_name_ = tablegroup_name;
}

inline void ObAlterTablegroupStmt::set_alter_option_set(const common::ObBitSet<> &alter_option_set)
{
  //copy
  alter_tablegroup_arg_.alter_option_bitset_ = alter_option_set;
}
} // namespace sql
} // namespace oceanbase
#endif //OCEANBASE_SQL_OB_ALTER_TABLEGROUP_STMT_
