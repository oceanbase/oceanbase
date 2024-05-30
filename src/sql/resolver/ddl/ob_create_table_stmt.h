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

#ifndef OCEANBASE_SQL_RESOLVER_DDL_CREATE_TABLE_STMT_H_
#define OCEANBASE_SQL_RESOLVER_DDL_CREATE_TABLE_STMT_H_
#include "share/ob_define.h"
#include "sql/resolver/ddl/ob_table_stmt.h"
#include "sql/resolver/ob_stmt_resolver.h"
#include "sql/resolver/ddl/ob_create_index_stmt.h"

namespace oceanbase
{
namespace sql
{

class ObCreateTableStmt : public ObTableStmt
{
public:
  explicit ObCreateTableStmt(common::ObIAllocator *name_pool);
  ObCreateTableStmt();
  virtual ~ObCreateTableStmt();

  int get_first_stmt(ObString &first_stmt) override;
  void set_if_not_exists(const bool if_not_exists);
  uint64_t get_table_id() const;
  const common::ObString &get_table_name() const;
  int64_t get_database_id() const { return create_table_arg_.schema_.get_database_id(); };
  void set_database_id(const uint64_t database_id);
  void set_database_name(const common::ObString &database_name);
  void set_locality(const common::ObString &locality);
  void set_primary_zone(const common::ObString &zone);
  const common::ObString &get_database_name() const;
  common::ObString &get_non_const_db_name() { return create_table_arg_.db_name_; }
  bool is_view_stmt() const { return is_view_stmt_; }
  void set_is_view_stmt(const bool is_view_stmt) { is_view_stmt_ = is_view_stmt; }
  bool is_view_table() const;
  int64_t get_block_size() const;
  int64_t get_progressive_merge_num() const;
  bool use_bloom_filter() const;
  int add_column_schema(const share::schema::ObColumnSchemaV2 &column);
  int64_t get_column_count() const;
  inline void set_index_using_type(share::schema::ObIndexUsingType);
  share::schema::ObIndexUsingType get_index_using_type();
  const share::schema::ObColumnSchemaV2 *get_column_schema(const common::ObString &column_name) const;
  obrpc::ObCreateTableArg &get_create_table_arg();
  virtual bool cause_implicit_commit() const { return false == create_table_arg_.schema_.is_mysql_tmp_table(); }
  void set_allocator(common::ObIAllocator &alloc) { view_need_privs_.set_allocator(&alloc); }
  share::schema::ObStmtNeedPrivs::NeedPrivs &get_view_need_privs() { return view_need_privs_; }
  const share::schema::ObStmtNeedPrivs::NeedPrivs &get_view_need_privs() const
  { return view_need_privs_; }
  int add_view_need_priv(share::schema::ObNeedPriv &need_priv)
  { return view_need_privs_.push_back(need_priv); }
  int invalidate_backup_table_id(); // used by restore table
  common::ObSArray<obrpc::ObCreateIndexArg> &get_index_arg_list() { return index_arg_list_; }
  common::ObSArray<obrpc::ObCreateForeignKeyArg> &get_foreign_key_arg_list()
  { return create_table_arg_.foreign_key_arg_list_; }
  const common::ObSArray<obrpc::ObCreateForeignKeyArg> &get_read_only_foreign_key_arg_list() const
  { return create_table_arg_.foreign_key_arg_list_; }
  common::ObSArray<obrpc::ObCreateVertialPartitionArg> &get_vertical_partition_arg_list()
  { return create_table_arg_.vertical_partition_arg_list_; }
  virtual obrpc::ObDDLArg &get_ddl_arg() { return create_table_arg_; }
  void set_sub_select(ObSelectStmt *select_stmt);
  void set_view_define(ObSelectStmt *select_stmt);
  const ObSelectStmt *get_sub_select() const;
  const ObSelectStmt *get_view_define() const;
  ObSelectStmt *get_sub_select() { return sub_select_stmt_; }
  ObSelectStmt *get_view_define() { return view_define_; }
  void set_sequence_ddl_arg(const obrpc::ObSequenceDDLArg sequence_ddl_arg);
  const obrpc::ObSequenceDDLArg &get_sequence_ddl_arg() const;
  void set_masked_sql(const common::ObString &masked_sql) { masked_sql_ = masked_sql; }
  common::ObString get_masked_sql() const { return masked_sql_; }
  void set_insert_mode(uint64_t mode) { insert_mode_ = mode; }
  uint64_t get_insert_mode() const { return insert_mode_; }

  ObTableType get_table_type() const { return create_table_arg_.schema_.get_table_type(); }
  INHERIT_TO_STRING_KV("ObTableStmt", ObTableStmt, K_(stmt_type), K_(create_table_arg), K_(index_arg_list));
private:
  int set_table_id(ObStmtResolver &ctx, const uint64_t table_id);
private:
  obrpc::ObCreateTableArg create_table_arg_;
  bool is_view_stmt_;
  share::schema::ObStmtNeedPrivs::NeedPrivs view_need_privs_;
  common::ObSArray<obrpc::ObCreateIndexArg> index_arg_list_;
  common::ObString masked_sql_;
  //common::ObSEArray<ObRawExpr *, OB_DEFAULT_ARRAY_SIZE, common::ModulePageAllocator, true> partition_fun_expr_; // for range fun expr
  //common::ObSEArray<ObRawExpr *, OB_DEFAULT_ARRAY_SIZE, common::ModulePageAllocator, true> range_values_exprs_; //range partition expr
  // for future use: create table xxx as select ......
  //ObSelectStmt                *select_clause;inline int64_t ObCreateTableStmt::get_block_size() const
  //{
  //  return create_table_arg_.schema_.get_block_size();
  //  }
  //
  // create table xxx as already_exist_table, pay attention to whether data are need
protected:
  ObSelectStmt *sub_select_stmt_; //create table  ... as select...
  ObSelectStmt *view_define_;
  /**
   * @brief insert_mode_
   *example: create table t1 [ignore/replace] select * from t2
   *1.  create table ctas_t1(xxx);
   *2.  construct inner sql
   *  a. insert_mode_ = 0 -> normal insert -> insert into ctas_t1 select * from t2
   *  b. insert_mode_ = 1 -> insert ignore -> insert ignore into ctas_t1 select * from t2
   *  c. insert_mode_ = 2 -> replace into  -> replace into ctas_t1 select * from t2
   3. rename ctas_t1 as t1
   */
  uint64_t insert_mode_;
};

inline obrpc::ObCreateTableArg &ObCreateTableStmt::get_create_table_arg()
{
  return create_table_arg_;
}

inline void ObCreateTableStmt::set_database_id(const uint64_t database_id)
{
  create_table_arg_.schema_.set_database_id(database_id);
}

inline void ObCreateTableStmt::set_database_name(const common::ObString &database_name)
{
  create_table_arg_.db_name_ = database_name;
}

inline void ObCreateTableStmt::set_locality(const common::ObString &locality)
{
  // not support
}

inline void ObCreateTableStmt::set_primary_zone(const common::ObString &zone)
{
  // not support
}

inline const common::ObString &ObCreateTableStmt::get_database_name() const
{
  return create_table_arg_.db_name_;
}

inline uint64_t ObCreateTableStmt::get_table_id() const
{
  return create_table_arg_.schema_.get_table_id();
}

inline const common::ObString &ObCreateTableStmt::get_table_name() const
{
  return create_table_arg_.schema_.get_table_name_str();
}

inline bool ObCreateTableStmt::is_view_table() const
{
  return create_table_arg_.schema_.is_view_table();
}

inline void ObCreateTableStmt::set_if_not_exists(bool if_not_exists)
{
  create_table_arg_.if_not_exist_ = if_not_exists;
}

inline int64_t ObCreateTableStmt::get_progressive_merge_num() const
{
  return create_table_arg_.schema_.get_progressive_merge_num();
}

inline int64_t ObCreateTableStmt::get_block_size() const
{
  return create_table_arg_.schema_.get_block_size();
}

inline int64_t ObCreateTableStmt::get_column_count() const
{
  return create_table_arg_.schema_.get_column_count();
}

inline bool ObCreateTableStmt::use_bloom_filter() const
{
  return create_table_arg_.schema_.is_use_bloomfilter();
}

inline void ObCreateTableStmt::set_index_using_type(
    const share::schema::ObIndexUsingType index_using_type)
{
  return create_table_arg_.schema_.set_index_using_type(index_using_type);
}

inline void ObCreateTableStmt::set_sequence_ddl_arg(
    const obrpc::ObSequenceDDLArg sequence_ddl_arg)
{
  create_table_arg_.sequence_ddl_arg_ = sequence_ddl_arg; 
}

inline const obrpc::ObSequenceDDLArg &ObCreateTableStmt::get_sequence_ddl_arg() const
{
  return create_table_arg_.sequence_ddl_arg_; 
}

inline void ObCreateTableStmt::set_sub_select(ObSelectStmt *select_stmt)
{
  sub_select_stmt_ = select_stmt;
}

inline void ObCreateTableStmt::set_view_define(ObSelectStmt *select_stmt)
{
  view_define_ = select_stmt;
}

inline const ObSelectStmt *ObCreateTableStmt::get_sub_select() const
{
  return sub_select_stmt_;
}

inline const ObSelectStmt *ObCreateTableStmt::get_view_define() const
{
  return view_define_;
}

}
}

#endif //OCEANBASE_SQL_RESOLVER_DDL_CREATE_TABLE_STMT_H_
