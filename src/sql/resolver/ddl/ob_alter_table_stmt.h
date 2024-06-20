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

#ifndef OCEANBASE_SQL_OB_ALTER_TABLE_STMT_
#define OCEANBASE_SQL_OB_ALTER_TABLE_STMT_


#include "sql/resolver/ddl/ob_table_stmt.h"
#include "sql/resolver/ddl/ob_ddl_stmt.h"
#include "sql/resolver/ob_stmt_resolver.h"
#include "common/sql_mode/ob_sql_mode.h"
#include "lib/hash/ob_placement_hashset.h"

namespace oceanbase
{
namespace sql
{

class ObAlterTableStmt : public ObTableStmt
{
public:
  explicit ObAlterTableStmt(common::ObIAllocator *name_pool);
  ObAlterTableStmt();
  virtual ~ObAlterTableStmt();
  int add_column(const share::schema::AlterColumnSchema &column_schema);
  int add_column_group(const ObColumnGroupSchema &column_group);
  int add_index_arg(obrpc::ObIndexArg *index_arg);
  int check_drop_fk_arg_exist(obrpc::ObDropForeignKeyArg *drop_fk_arg, bool &has_same_fk_arg);
  int check_drop_cst_exist(const ObConstraint &constraint, bool &has_same_cst);
  obrpc::ObAlterTableArg& get_alter_table_arg(){ return alter_table_arg_; }
  //check duplicate alter of same column in one clause
  bool is_column_modified(const common::ObString &column_name);
  bool is_comment_table() const { return is_comment_table_; }
  void set_is_comment_table(const bool flag) { is_comment_table_ = flag; }

  uint64_t get_alter_table_action_count() { return alter_table_action_count_; }
  void inc_alter_table_action_count() { ++alter_table_action_count_; }

  int64_t get_alter_external_table_type() { return alter_external_table_type_; }
  void set_alter_external_table_type(int64_t type) { alter_external_table_type_ = type; }

  bool is_alter_system() const { return is_alter_system_; }
  void set_is_alter_system(const bool flag) { is_alter_system_ = flag; }

  bool is_alter_triggers() const { return is_alter_triggers_; }
  void set_is_alter_triggers(const bool flag) { is_alter_triggers_ = flag; }

  int set_database_name(const common::ObString &db_name);
  int set_origin_database_name(const common::ObString &origin_db_name);
  int set_table_name(const common::ObString &table_name);
  int set_origin_table_name(const common::ObString &origin_table_name);
  void set_table_id(const uint64_t table_id);
  inline void set_alter_table_option(){alter_table_arg_.is_alter_options_ = true;}
  inline void set_convert_to_character(){alter_table_arg_.is_convert_to_character_ = true;}
  inline void set_alter_table_column(){alter_table_arg_.is_alter_columns_ = true;}
  inline void set_alter_table_index(){alter_table_arg_.is_alter_indexs_ = true;}
  inline void set_alter_table_partition(){alter_table_arg_.is_alter_partitions_ = true;}
  /* to do: interval */
  inline void set_interval_expr(ObRawExpr *expr) {interval_expr_ = expr;}
  inline void set_transition_expr(ObRawExpr *expr) {transition_expr_ = expr;}

  inline ObRawExpr *get_transition_expr() { return transition_expr_; }
  inline ObRawExpr *get_interval_expr() { return interval_expr_; }

  const common::ObString &get_org_database_name() const
  { return alter_table_arg_.alter_table_schema_.get_origin_database_name(); }
  const common::ObString &get_org_table_name() const
  { return alter_table_arg_.alter_table_schema_.get_origin_table_name(); }
  const common::ObString &get_database_name() const
  { return alter_table_arg_.alter_table_schema_.get_database_name(); }
  const common::ObString &get_table_name() const
  { return alter_table_arg_.alter_table_schema_.get_table_name_str(); }
  inline int set_tz_info_wrap(const common::ObTimeZoneInfoWrap &tz_info_wrap);
  inline int set_nls_formats(const common::ObString &nls_date_format,
                             const common::ObString &nls_timestamp_format,
                             const common::ObString &nls_timestamp_tz_format);
  int fill_session_vars(const ObBasicSessionInfo &session);
  inline const common::ObTimeZoneInfoWrap &get_tz_info_wrap()
  { return alter_table_arg_.tz_info_wrap_; }
  void set_tenant_id(const uint64_t tenant_id)
  { alter_table_arg_.alter_table_schema_.set_tenant_id(tenant_id);}
  void set_sql_mode(ObSQLMode sql_mode)
  { alter_table_arg_.alter_table_schema_.set_sql_mode(sql_mode);}
  uint64_t get_tenant_id()
  {
    return alter_table_arg_.alter_table_schema_.get_tenant_id();
  }
  bool has_rename_action() const
  { return alter_table_arg_.has_rename_action(); }
  virtual obrpc::ObDDLArg &get_ddl_arg() { return alter_table_arg_; }
  common::ObSArray<obrpc::ObCreateIndexArg*> &get_index_arg_list() { return index_arg_list_; }
  void set_fts_arg_allocator(common::ObIAllocator *fts_arg_allocator) { fts_arg_allocator_ = fts_arg_allocator; }
  common::ObSArray<obrpc::ObCreateForeignKeyArg> &get_foreign_key_arg_list()
    { return alter_table_arg_.foreign_key_arg_list_; }
  const common::ObSArray<obrpc::ObCreateForeignKeyArg> &get_read_only_foreign_key_arg_list() const
    { return alter_table_arg_.foreign_key_arg_list_; }
  void set_sequence_ddl_arg(const obrpc::ObSequenceDDLArg sequence_ddl_arg);
  const obrpc::ObSequenceDDLArg &get_sequence_ddl_arg() const;
  INHERIT_TO_STRING_KV("ObTableStmt", ObTableStmt, K_(stmt_type), K_(alter_table_arg), K_(index_arg_list));
  const common::ObSArray<obrpc::ObIndexArg*> &get_alter_index_arg_list() const 
    { return alter_table_arg_.index_arg_list_; }

  obrpc::ObAlterTriggerArg &get_tg_arg() { return tg_arg_; }
  const ObTableSchema &get_alter_table_schema() const { return alter_table_arg_.alter_table_schema_; }
  ObTableSchema &get_alter_table_schema() { return alter_table_arg_.alter_table_schema_; }
  obrpc::ObExchangePartitionArg &get_exchange_partition_arg() { return exchange_partition_arg_;}
  int set_exchange_partition_arg(const obrpc::ObExchangePartitionArg &exchange_partition_arg);
private:
  obrpc::ObAlterTableArg alter_table_arg_;
  bool is_comment_table_;
  bool is_alter_system_;
  common::ObSArray<obrpc::ObCreateIndexArg*> index_arg_list_;
  common::ObIAllocator *fts_arg_allocator_;
  bool is_alter_triggers_;
  obrpc::ObAlterTriggerArg tg_arg_;
  ObRawExpr *interval_expr_;
  ObRawExpr *transition_expr_;
  uint64_t alter_table_action_count_;
  int64_t alter_external_table_type_;
  obrpc::ObExchangePartitionArg exchange_partition_arg_;
};

inline int ObAlterTableStmt::set_tz_info_wrap(const common::ObTimeZoneInfoWrap &tz_info_wrap)
{
  int ret = alter_table_arg_.tz_info_wrap_.deep_copy(tz_info_wrap);
  if (OB_SUCC(ret)) {
    //compat for old server
    alter_table_arg_.tz_info_ = tz_info_wrap.get_tz_info_offset();
  }
  return ret;
}

inline int ObAlterTableStmt::set_nls_formats(const common::ObString &nls_date_format,
                                             const common::ObString &nls_timestamp_format,
                                             const common::ObString &nls_timestamp_tz_format)
{
  return alter_table_arg_.set_nls_formats(nls_date_format, nls_timestamp_format,
                                          nls_timestamp_tz_format);
}

inline void ObAlterTableStmt::set_sequence_ddl_arg(
    const obrpc::ObSequenceDDLArg sequence_ddl_arg)
{
  alter_table_arg_.sequence_ddl_arg_ = sequence_ddl_arg; 
}

inline const obrpc::ObSequenceDDLArg &ObAlterTableStmt::get_sequence_ddl_arg() const
{
  return alter_table_arg_.sequence_ddl_arg_; 
}

} // namespace sql
} // namespace oceanbase


#endif //OCEANBASE_SQL_OB_ALTER_TABLE_STMT_
