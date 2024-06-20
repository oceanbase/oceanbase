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

#ifndef OB_DROP_MLOG_STMT_H_
#define OB_DROP_MLOG_STMT_H_

#include "sql/resolver/ddl/ob_ddl_stmt.h"
#include "lib/allocator/ob_allocator.h"
#include "src/share/ob_rpc_struct.h"

namespace oceanbase
{
namespace sql
{

class ObDropMLogStmt : public ObDDLStmt
{
public:
  explicit ObDropMLogStmt(common::ObIAllocator *name_pool);
  ObDropMLogStmt();
  virtual ~ObDropMLogStmt();

  obrpc::ObDropIndexArg &get_drop_index_arg() { return drop_index_arg_; }
  inline void set_database_name(const common::ObString &database_name)
  {
    drop_index_arg_.database_name_ = database_name;
  }
  inline const common::ObString &get_database_name() const { return drop_index_arg_.database_name_; }
  inline void set_table_name(const common::ObString &table_name)
  {
    drop_index_arg_.table_name_ = table_name;
  }
  inline const common::ObString &get_table_name() const { return drop_index_arg_.table_name_; }
  inline void set_mlog_name(const common::ObString &mlog_name)
  {
    drop_index_arg_.index_name_ = mlog_name;
  }
  inline const common::ObString &get_mlog_name() const { return drop_index_arg_.index_name_; }
  void set_tenant_id(const uint64_t tenant_id)
  {
    drop_index_arg_.tenant_id_ = tenant_id;
  }
  inline uint64_t get_tenant_id() const { return drop_index_arg_.tenant_id_; }
  void set_exec_tenant_id(const uint64_t exec_tenant_id)
  {
    drop_index_arg_.exec_tenant_id_ = exec_tenant_id;
  }
  inline uint64_t get_exec_tenant_id() const { return drop_index_arg_.exec_tenant_id_; }
  inline const obrpc::ObIndexArg::IndexActionType &get_index_action_type() const
  {
    return drop_index_arg_.index_action_type_;
  }
  inline void set_index_action_type(obrpc::ObIndexArg::IndexActionType action_type)
  {
    drop_index_arg_.index_action_type_ = action_type;
  }
  virtual obrpc::ObDDLArg &get_ddl_arg() { return drop_index_arg_; }

private:
  obrpc::ObDropIndexArg drop_index_arg_;
};
} // sql
} // oceanbase
#endif  // OB_DROP_MLOG_STMT_H_