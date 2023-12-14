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

#ifndef OB_CREATE_MLOG_STMT_H_
#define OB_CREATE_MLOG_STMT_H_

#include "sql/resolver/ddl/ob_ddl_stmt.h"
#include "lib/allocator/ob_allocator.h"

namespace oceanbase
{
namespace sql
{

class ObCreateMLogStmt : public ObDDLStmt
{
public:
  explicit ObCreateMLogStmt(common::ObIAllocator *name_pool);
  ObCreateMLogStmt();
  virtual ~ObCreateMLogStmt();

  obrpc::ObCreateMLogArg &get_create_mlog_arg() { return create_mlog_arg_; }
  inline void set_database_name(const common::ObString &database_name)
  {
    create_mlog_arg_.database_name_ = database_name;
  }
  inline const common::ObString &get_database_name() const { return create_mlog_arg_.database_name_; }
  inline void set_table_name(const common::ObString &table_name)
  {
    create_mlog_arg_.table_name_ = table_name;
  }
  inline const common::ObString &get_table_name() const { return create_mlog_arg_.table_name_; }
  inline void set_mlog_name(const common::ObString &mlog_name)
  {
    create_mlog_arg_.mlog_name_ = mlog_name;
  }
  inline const common::ObString &get_mlog_name() const { return create_mlog_arg_.mlog_name_; }
  void set_tenant_id(const uint64_t tenant_id)
  {
    create_mlog_arg_.tenant_id_ = tenant_id;
  }
  inline uint64_t get_tenant_id() const { return create_mlog_arg_.tenant_id_; }
  void set_exec_tenant_id(const uint64_t exec_tenant_id)
  {
    create_mlog_arg_.exec_tenant_id_ = exec_tenant_id;
  }
  inline uint64_t get_exec_tenant_id() const { return create_mlog_arg_.exec_tenant_id_; }
  inline void set_data_table_id(const uint64_t table_id)
  {
    create_mlog_arg_.base_table_id_ = table_id;
  }
  inline uint64_t get_data_table_id() const { return create_mlog_arg_.base_table_id_; }
  inline void set_mlog_table_id(const uint64_t table_id)
  {
    create_mlog_arg_.mlog_table_id_ = table_id;
  }
  inline uint64_t get_mlog_table_id() const { return create_mlog_arg_.mlog_table_id_; }
  inline void set_with_primary_key(bool with_primary_key)
  {
    create_mlog_arg_.with_primary_key_ = with_primary_key;
  }
  inline bool get_with_primary_key() const { return create_mlog_arg_.with_primary_key_; }
  inline void set_with_rowid(bool with_rowid)
  {
    create_mlog_arg_.with_rowid_ = with_rowid;
  }
  inline bool get_with_rowid() const { return create_mlog_arg_.with_rowid_; }
  inline void set_with_sequence(bool with_sequence)
  {
    create_mlog_arg_.with_sequence_ = with_sequence;
  }
  inline bool get_with_sequence() const { return create_mlog_arg_.with_sequence_; }
  inline void set_include_new_values(bool include_new_values)
  {
    create_mlog_arg_.include_new_values_ = include_new_values;
  }
  inline bool get_include_new_values() const { return create_mlog_arg_.include_new_values_; }
  inline void set_name_generated_type(const ObNameGeneratedType type)
  {
    create_mlog_arg_.mlog_schema_.set_name_generated_type(type);
  }
  inline void set_purge_mode(ObMLogPurgeMode purge_mode)
  {
    create_mlog_arg_.purge_options_.purge_mode_ = purge_mode;
  }

  virtual obrpc::ObDDLArg &get_ddl_arg() { return create_mlog_arg_; }

private:
  obrpc::ObCreateMLogArg create_mlog_arg_;
};
} // sql
} // oceanbase
#endif  // OB_CREATE_MLOG_STMT_H_