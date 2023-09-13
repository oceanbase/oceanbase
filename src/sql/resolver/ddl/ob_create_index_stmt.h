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

#ifndef OCEANBASE_SQL_RESOLVER_DDL_CREATE_INDEX_STMT_
#define OCEANBASE_SQL_RESOLVER_DDL_CREATE_INDEX_STMT_

#include "lib/container/ob_se_array.h"
#include "lib/string/ob_string.h"
#include "lib/allocator/ob_allocator.h"
#include "common/object/ob_object.h"
#include "share/schema/ob_table_schema.h"
#include "sql/parser/parse_node.h"
#include "sql/resolver/ddl/ob_partitioned_stmt.h"
namespace oceanbase
{
namespace sql
{
class ObCreateIndexStmt : public ObPartitionedStmt
{
  const static int OB_DEFAULT_ARRAY_SIZE = 16;
public:
  explicit ObCreateIndexStmt(common::ObIAllocator *name_pool);
  ObCreateIndexStmt();
  virtual ~ObCreateIndexStmt();

  obrpc::ObCreateIndexArg &get_create_index_arg();
  int add_storing_column(const common::ObString &column_name);
  int add_hidden_storing_column(const common::ObString &column_name);
  int add_sort_column(const obrpc::ObColumnSortItem &sort_column);
  void set_compress_method(const common::ObString &compress_method);
  void set_comment(const common::ObString &comment);
  void set_index_name(const common::ObString &index_name);
  uint64_t get_tenant_id() const { return create_index_arg_.tenant_id_; };
  void set_tenant_id(const uint64_t tenant_id);
  void set_index_dop(int64_t index_dop);
  int64_t get_index_dop();
  inline void set_database_name(const common::ObString &db_name);
  inline const common::ObString &get_database_name() const;
  inline const common::ObString &get_table_name() const;
  common::ObString &get_table_name();
  inline const common::ObString &get_index_name() const;
  common::ObString &get_index_name();
  inline void set_index_using_type(const share::schema::ObIndexUsingType index_using_type) {
    create_index_arg_.index_using_type_ = index_using_type;
  }
  inline void set_index_status(share::schema::ObIndexStatus status) {
     create_index_arg_.index_option_.index_status_ = status;
  }
  inline void set_if_not_exists(const bool if_not_exist) {
    create_index_arg_.if_not_exist_ = if_not_exist;
  }
  inline bool get_if_not_exists() const {
    return create_index_arg_.if_not_exist_;
  }
  inline share::schema::ObIndexUsingType get_index_using_type() const { return create_index_arg_.index_using_type_; }
  void set_table_name(const common::ObString &table_name);
  void set_name_generated_type(const ObNameGeneratedType type);

  inline void set_data_table_id(const uint64_t table_id);
  inline void set_data_index_id(const uint64_t table_id);
  inline uint64_t get_data_table_id() const;
  inline uint64_t get_data_index_id() const;
  inline void set_table_id(const uint64_t table_id);
  inline uint64_t get_table_id() const;

  virtual bool cause_implicit_commit() const { return true; }
  int invalidate_backup_index_id(); // used by restore index
  virtual obrpc::ObDDLArg &get_ddl_arg() { return create_index_arg_; }
  inline void set_tablespace_id(const uint64_t id) { create_index_arg_.index_schema_.set_tablespace_id(id); }
  inline int set_encryption_str(const common::ObString &str) 
  { return create_index_arg_.index_schema_.set_encryption_str(str); }
  TO_STRING_KV(K_(stmt_type),K_(create_index_arg));
private:
  obrpc::ObCreateIndexArg create_index_arg_;
  uint64_t table_id_;
  DISALLOW_COPY_AND_ASSIGN(ObCreateIndexStmt);
};

inline void ObCreateIndexStmt::set_data_table_id(const uint64_t table_id)
{
  create_index_arg_.data_table_id_ = table_id;
}

inline void ObCreateIndexStmt::set_data_index_id(const uint64_t table_id)
{
  create_index_arg_.index_table_id_ = table_id;
}

inline uint64_t ObCreateIndexStmt::get_data_table_id() const
{
  return create_index_arg_.data_table_id_;
}

inline uint64_t ObCreateIndexStmt::get_data_index_id() const
{
  return create_index_arg_.index_table_id_;
}

inline void ObCreateIndexStmt::set_table_id(const uint64_t table_id)
{
  table_id_ = table_id;
}

inline uint64_t ObCreateIndexStmt::get_table_id() const
{
  return table_id_;
}

inline void ObCreateIndexStmt::set_database_name(const common::ObString &db_name)
{
  create_index_arg_.database_name_ = db_name;
}

inline const common::ObString &ObCreateIndexStmt::get_database_name() const
{
  return create_index_arg_.database_name_;
}

inline void ObCreateIndexStmt::set_table_name(const common::ObString &table_name)
{
  create_index_arg_.table_name_ = table_name;
}

inline void ObCreateIndexStmt::set_name_generated_type(const ObNameGeneratedType type)
{
  create_index_arg_.index_schema_.set_name_generated_type(type);
}

inline void ObCreateIndexStmt::set_tenant_id(const uint64_t tenant_id)
{
  create_index_arg_.tenant_id_ = tenant_id;
}

inline const common::ObString &ObCreateIndexStmt::get_table_name() const
{
  return create_index_arg_.table_name_;
}

inline common::ObString &ObCreateIndexStmt::get_table_name()
{
  return create_index_arg_.table_name_;
}

inline const common::ObString &ObCreateIndexStmt::get_index_name() const
{
  return create_index_arg_.index_name_;
}

inline common::ObString &ObCreateIndexStmt::get_index_name()
{
  return create_index_arg_.index_name_;
}

inline void ObCreateIndexStmt::set_index_dop(int64_t index_dop) 
{
  create_index_arg_.index_schema_.set_dop(index_dop);
}

inline int64_t ObCreateIndexStmt::get_index_dop()
{
  return create_index_arg_.index_schema_.get_dop();
}
}//namespace sql
}//namespace oceanbase

#endif //OCEANBASE_SQL_RESOLVER_DDL_CREATE_INDEX_STMT_
