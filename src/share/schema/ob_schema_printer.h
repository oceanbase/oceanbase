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

#ifndef OCEANBASE_SHARE_SCHEMA_OB_SCHEMA_PRINTER_H_
#define OCEANBASE_SHARE_SCHEMA_OB_SCHEMA_PRINTER_H_
#include <stdint.h>
#include "share/ob_define.h"
#include "lib/container/ob_iarray.h"
#include "share/schema/ob_schema_struct.h"
#include "pl/parser/ob_pl_parser.h"
#include "share/schema/ob_table_schema.h"

namespace oceanbase
{
namespace sql
{
  class ObExecEnv;
}
namespace common
{
class ObTimeZoneInfo;
class ObObj;
class ObString;
class ObMySQLProxy;
class ObIAllocator;
class ObRowkeyInfo;
namespace sqlclient
{
class ObMySQLResult;
}
}

namespace share
{
namespace schema
{
class ObTenantSchema;
class ObDatabaseSchema;
class ObTablegroupSchema;
class ObTableSchema;
class ObRoutineInfo;
class ObUDTBase;
class ObRoutineParam;
class ObTriggerInfo;
class ObColumnSchemaV2;
class ObSchemaGetterGuard;
class ObUDTObjectType;
class ObConstraint;

class ObSchemaPrinter
{
public:
  explicit ObSchemaPrinter(ObSchemaGetterGuard &schema_guard,
                           bool strict_compat = false,
                           bool sql_quote_show_create = true,
                           bool ansi_quotes = false);
  virtual ~ObSchemaPrinter() { }
private:
  ObSchemaPrinter();
  DISALLOW_COPY_AND_ASSIGN(ObSchemaPrinter);
public:
  int print_table_definition(const uint64_t tenant_id,
                             const uint64_t table_id,
                             char* buf,
                             const int64_t& buf_len,
                             int64_t& pos,
                             const common::ObTimeZoneInfo *tz_info,
                             const common::ObLengthSemantics default_length_semantics,
                             bool agent_mode,
                             ObSQLMode sql_mode = SMO_DEFAULT,
                             ObCharsetType charset_type = ObCharsetType::CHARSET_UTF8MB4) const;
  int print_table_index_stroing(
      const share::schema::ObTableSchema *index_schema,
      const share::schema::ObTableSchema *table_schema,
      char *buf, const int64_t buf_len, int64_t &pos) const;
  int print_table_definition_fulltext_indexs(
      const bool is_oracle_mode,
      const common::ObIArray<common::ObString> &fulltext_indexs,
      const uint64_t virtual_column_id,
      char *buf, int64_t buf_len, int64_t &pos) const;
  int print_table_definition_table_options(
      const share::schema::ObTableSchema &table_schema,
      const common::ObIArray<common::ObString> &full_text_columns,
      const uint64_t virtual_column_id,
      char* buf, const int64_t buf_len, int64_t& pos,
      bool is_for_table_status,
      common::ObMySQLProxy *sql_proxy,
      bool is_agent_mode) const;
  int print_func_index_columns_definition(
      const common::ObString &column_name,
      uint64_t column_id,
      char *buf,
      int64_t buf_len,
      int64_t &pos,
      bool is_agent_mode) const;
  int print_index_definition_columns(
      const share::schema::ObTableSchema &data_schema,
      const share::schema::ObTableSchema &index_schema,
      common::ObIArray<common::ObString> &full_text_columns,
      uint64_t &virtual_column_id,
      char* buf,
      const int64_t buf_len,
      int64_t& pos,
      bool is_agent_mode) const;
  int print_full_text_columns_definition(
      const share::schema::ObTableSchema &data_schema,
      const common::ObString &generated_column_name,
      common::ObIArray<common::ObString> &full_text_columns,
      char* buf, const int64_t buf_len, int64_t& pos, bool &is_first,
      bool is_agent_mode) const;
  int print_index_table_definition(
      const uint64_t tenant_id,
      const uint64_t index_table_id,
      char* buf, const int64_t buf_len, int64_t& pos,
      const common::ObTimeZoneInfo *tz_info,
      bool is_agent_mode) const;

  int print_view_definiton(const uint64_t tenant_id,
                           const uint64_t table_id,
                           char *buf,
                           const int64_t &buf_len,
                           int64_t &pos,
                           const ObTimeZoneInfo *tz_info,
                           bool agent_mode,
                           ObSQLMode sql_mode) const;

  int print_database_definiton(const uint64_t tenant_id,
                               const uint64_t database_id,
                               bool if_not_exists,
                               char *buf,
                               const int64_t &buf_len,
                               int64_t &pos) const;

  int print_table_definition_columns(const ObTableSchema &table_schema,
                                     char* buf,
                                     const int64_t& buf_len,
                                     int64_t& pos,
                                     const common::ObTimeZoneInfo *tz_info,
                                     const common::ObLengthSemantics default_length_semantics,
                                     bool is_agent_mode = false,
                                     ObSQLMode sql_mode = SMO_DEFAULT,
                                     ObCharsetType charset_type = ObCharsetType::CHARSET_UTF8MB4) const;
  int print_generated_column_definition(const ObColumnSchemaV2 &gen_col,
                                        char *buf,
                                        int64_t buf_len,
                                        const ObTableSchema &table_schema,
                                        int64_t &pos) const;
  int print_identity_column_definition(const ObColumnSchemaV2 &iden_col,
                                       char *buf,
                                       int64_t buf_len,
                                       const ObTableSchema &table_schema,
                                       int64_t &pos) const;
  int print_table_definition_indexes(const ObTableSchema &table_schema,
                                     char* buf,
                                     const int64_t& buf_len,
                                     int64_t& pos,
                                     bool is_unique_index,
                                     ObSQLMode sql_mode = SMO_DEFAULT,
                                     const ObTimeZoneInfo *tz_info = NULL) const;
  int print_single_index_definition(const ObTableSchema *index_schema, const ObTableSchema &table_schema,
      ObIAllocator &arena_allocator, char* buf, const int64_t& buf_len, int64_t& pos,
      const bool is_unique_index, const bool is_oracle_mode, const bool is_alter_table_add, ObSQLMode sql_mode,
      const ObTimeZoneInfo *tz_info) const;
  int print_constraint_stat(const bool rely_flag,
                            const bool enable_flag,
                            const bool validate_flag,
                            char* buf,
                            const int64_t buf_len,
                            int64_t& pos) const;
  int print_table_definition_constraints(const ObTableSchema &table_schema,
                                         char* buf,
                                         const int64_t& buf_len,
                                         int64_t& pos) const;
  int print_index_column(const ObTableSchema &table_schema,
                         const ObColumnSchemaV2 &column,
                         bool is_last,
                         char *buf,
                         int64_t buf_len,
                         int64_t &pos) const;
  int print_fulltext_index_column(const ObTableSchema &table_schema,
                                  const ObColumnSchemaV2 &column,
                                  bool is_last,
                                  char *buf,
                                  int64_t buf_len,
                                  int64_t &pos) const;
  int print_multivalue_index_column(const ObTableSchema &table_schema,
                                  const ObColumnSchemaV2 &column,
                                  bool is_last,
                                  char *buf,
                                  int64_t buf_len,
                                  int64_t &pos) const;
  int print_spatial_index_column(const ObTableSchema &table_schema,
                                 const ObColumnSchemaV2 &column,
                                 char *buf,
                                 int64_t buf_len,
                                 int64_t &pos) const;
  int print_prefix_index_column(const ObColumnSchemaV2 &column,
                                bool is_last,
                                char *buf,
                                int64_t buf_len,
                                int64_t &pos) const;
  int print_ordinary_index_column_expr(const ObColumnSchemaV2 &column,
                              bool is_last,
                              char *buf,
                              int64_t buf_len,
                              int64_t &pos) const;
  int print_table_definition_fulltext_indexs(
      const bool is_oracle_mode,
      const common::ObIArray<common::ObString> &fulltext_indexs,
      char *buf, int64_t buf_len, int64_t &pos) const;
  int print_table_definition_rowkeys(const ObTableSchema &table_schema,
                                     char* buf,
                                     const int64_t& buf_len,
                                     int64_t& pos) const;
  int print_rowkey_info(const common::ObRowkeyInfo& rowkey_info,
                        const uint64_t tenant_id,
                        const uint64_t table_id,
                        char* buf,
                        const int64_t& buf_len,
                        int64_t& pos) const;
  int print_table_definition_foreign_keys(const ObTableSchema &table_schema,
                                          char* buf,
                                          const int64_t& buf_len,
                                          int64_t& pos) const;
  template<typename T>
  int print_referenced_table_info(
      char* buf, const int64_t &buf_len, int64_t &pos, ObArenaAllocator &allocator,
      const bool is_oracle_mode, const ObForeignKeyInfo *foreign_key_info,
      const T *&schema) const;
  template<typename T>
  int print_column_list(const T &table_schema,
                        const common::ObIArray<uint64_t> &column_ids,
                        char* buf,
                        const int64_t& buf_len,
                        int64_t& pos) const;
  int print_table_definition_store_format(const ObTableSchema &table_schema,
                                          char* buf,
                                          const int64_t& buf_len,
                                          int64_t& pos) const;
  int print_table_definition_table_options(const ObTableSchema &table_schema,
                                           char* buf,
                                           const int64_t& buf_len,
                                           int64_t& pos,
                                           bool is_for_table_status,
                                           bool agent_mode = false,
                                           ObSQLMode sql_mode = SMO_DEFAULT) const;
  int print_table_definition_comment_oracle(const ObTableSchema &table_schema,
                                                  char* buf,
                                                  const int64_t& buf_len,
                                                  int64_t& pos) const;
  int print_interval_if_ness(const ObTableSchema &table_schema,
                             char* buf,
                             const int64_t& buf_len,
                             int64_t& pos,
                             const ObTimeZoneInfo *tz_info) const;
  int print_table_definition_partition_options(const ObTableSchema &table_schema,
                                               char* buf,
                                               const int64_t& buf_len,
                                               int64_t& pos,
                                               bool agent_mode,
                                               const common::ObTimeZoneInfo *tz_info) const;
  int print_list_partition_elements(const ObPartitionSchema *&schema,
                                    char* buf,
                                    const int64_t& buf_len,
                                    int64_t& pos,
                                    bool print_sub_part_element,
                                    bool agent_mode = false,
                                    bool tablegroup_def = false,
                                    const common::ObTimeZoneInfo *tz_info = NULL,
                                    bool is_external_table = false) const;
  int print_range_partition_elements(const ObPartitionSchema *&schema,
                               char* buf,
                               const int64_t& buf_len,
                               int64_t& pos,
                               bool print_sub_part_element,
                               bool agent_mode,
                               bool tablegroup_def,
                               const common::ObTimeZoneInfo *tz_info) const;
  int print_hash_sub_partition_elements(ObSubPartition **sub_part_array,
                                        const int64_t sub_part_num,
                                        char* buf,
                                        const int64_t& buf_len,
                                        int64_t& pos) const;
  int print_hash_sub_partition_elements_for_tablegroup(const ObPartitionSchema *&schema,
                                                       char* buf,
                                                       const int64_t& buf_len,
                                                       int64_t& pos) const;
  int print_range_sub_partition_elements(const bool is_oracle_mode,
                                         ObSubPartition **sub_part_array,
                                         const int64_t sub_part_num,
                                         char *buf,
                                         const int64_t &buf_len,
                                         int64_t &pos,
                                         const common::ObTimeZoneInfo *tz_info) const;
  int print_list_sub_partition_elements(
      const bool is_oracle_mode,
      ObSubPartition **sub_part_array,
      const int64_t sub_part_num,
      char *buf,
      const int64_t &buf_len,
      int64_t &pos,
      const common::ObTimeZoneInfo *tz_info) const;

  int print_template_sub_partition_elements(const ObPartitionSchema *&schema,
                                            char *buf,
                                            const int64_t &buf_len,
                                            int64_t &pos,
                                            const common::ObTimeZoneInfo *tz_info,
                                            bool is_tablegroup) const;
  int print_individual_sub_partition_elements(const ObPartitionSchema *&schema,
                                              const ObPartition *partition,
                                              char *buf,
                                              const int64_t &buf_len,
                                              int64_t &pos,
                                              const common::ObTimeZoneInfo *tz_info) const;
  int print_tablegroup_definition(const uint64_t tenant_id,
                                  const uint64_t tablegroup_id,
                                  char* buf,
                                  const int64_t& buf_len,
                                  int64_t& pos,
                                  bool agent_mode,
                                  const common::ObTimeZoneInfo *tz_info);
  int print_tablegroup_definition_tablegroup_options(
      const share::schema::ObTablegroupSchema &tablegroup_schema,
      char* buf,
      const int64_t& buf_len,
      int64_t& pos,
      bool agent_mode = false) const;
  int print_tenant_definition(uint64_t tenant_id,
                              common::ObMySQLProxy *sql_proxy,
                              char* buf,
                              const int64_t& buf_len,
                              int64_t& pos,
                              bool is_agent_mode = false) const;
  int print_routine_definition_v1(const ObRoutineInfo *routine_info,
                               const ObStmtNodeTree *param_list,
                               const ObStmtNodeTree *return_type,
                               const common::ObString &body,
                               const common::ObString &clause,
                               char* buf,
                               const int64_t& buf_len,
                               int64_t &pos,
                               const common::ObTimeZoneInfo *tz_info) const;
  int print_routine_definition(const uint64_t tenant_id,
                               const uint64_t routine_id,
                               const sql::ObExecEnv &exec_env,
                               char* buf,
                               const int64_t& buf_len,
                               int64_t& pos,
                               const common::ObTimeZoneInfo *tz_info) const;
  int print_routine_definition_v2_mysql(const ObRoutineInfo &routine_info,
                                const ObStmtNodeTree *parse_tree,
                                const sql::ObExecEnv &exec_env,
                                char* buf,
                                const int64_t& buf_len,
                                int64_t& pos,
                                const common::ObTimeZoneInfo *tz_info) const;
  int print_routine_definition_v2_oracle(const ObRoutineInfo &routine_info,
                                const ObStmtNodeTree *parse_tree,
                                char* buf,
                                const int64_t& buf_len,
                                int64_t& pos,
                                const common::ObTimeZoneInfo *tz_info) const;
  int print_routine_param_type(const ObRoutineParam *param,
                               const ObStmtNodeTree *param_type,
                               char *buf,
                               const int64_t &buf_len,
                               int64_t &pos,
                               const common::ObTimeZoneInfo *tz_info) const;
  int print_routine_definition_param_v1(const ObRoutineInfo &routine_info,
                                     const ObStmtNodeTree *param_list,
                                     char* buf,
                                     const int64_t& buf_len,
                                     int64_t& pos,
                                     const common::ObTimeZoneInfo *tz_info) const;
  int print_foreign_key_definition(const uint64_t tenant_id,
                                   const share::schema::ObForeignKeyInfo &foreign_key_info,
                                   char *buf, int64_t buf_len, int64_t &pos) const;
  int print_udt_definition(const uint64_t tenant_id,
                           const uint64_t udt_id,
                           char* buf,
                           const int64_t& buf_len,
                           int64_t &pos) const;
  int print_udt_body_definition(const uint64_t tenant_id,
                                const uint64_t udt_id,
                                char* buf,
                                const int64_t& buf_len,
                                int64_t& pos,
                                bool &body_exist) const;
  int print_object_definition(const share::schema::ObUDTObjectType *object,
                                char *buf,
                                const int64_t &buf_len,
                                int64_t &pos) const;
  int print_element_type(const uint64_t tenant_id,
                         const uint64_t element_type_id,
                         const ObUDTBase *element_type_info,
                         char* buf,
                         const int64_t& buf_len,
                         int64_t &pos) const;
  int print_trigger_definition(const share::schema::ObTriggerInfo &trigger_info,
                               char *buf, int64_t buf_len, int64_t &pos,
                               bool get_ddl = false) const;
  int print_simple_trigger_definition(const ObTriggerInfo &trigger_info,
                                      char *buf, int64_t buf_len, int64_t &pos,
                                      bool get_ddl = false) const;
  int print_compound_instead_trigger_definition(const ObTriggerInfo &trigger_info,
                                                char *buf, int64_t buf_len, int64_t &pos,
                                                bool get_ddl) const;
  int print_trigger_status(const ObTriggerInfo &trigger_info, char *buf, int64_t buf_len, int64_t &pos) const;
  int print_trigger_base_object(const ObTriggerInfo &trigger_info,
                                char *buf, int64_t buf_len, int64_t &pos) const;
  int print_trigger_referencing(const ObTriggerInfo &trigger_info,
                                char *buf, int64_t buf_len, int64_t &pos) const;
  int print_tablespace_definition(const uint64_t tenant_id,
                                  const uint64_t tablespace_id,
                                  char* buf, int64_t buf_len, int64_t &pos) const;
  int print_tablespace_definition_for_table(const uint64_t tenant_id,
                                            const uint64_t tablespace_id,
                                            char* buf, int64_t buf_len, int64_t &pos) const;
  int print_table_definition_on_commit_options(const ObTableSchema &table_schema,
                                               char* buf,
                                               const int64_t& buf_len,
                                               int64_t& pos) const;
  int deep_copy_obj(common::ObIAllocator &allocator, const common::ObObj &src, common::ObObj &dest) const;
  int print_sequence_definition(const ObSequenceSchema &sequence_schema,
                                char *buf,
                                const int64_t &buf_len,
                                int64_t &pos,
                                bool is_from_create_sequence) const;
  // print pk/check/not null constraint definition
  int print_constraint_definition(const ObDatabaseSchema &db_schema,
                                  const ObTableSchema &table_schema,
                                  uint64_t constraint_id,
                                  char *buf,
                                  const int64_t &buf_len,
                                  int64_t &pos);
  // print unique constraint definition for dbms_metadata.get_ddl in oracle mode
  int print_unique_cst_definition(const ObDatabaseSchema &db_schema,
                                  const ObTableSchema &data_table_schema,
                                  const ObTableSchema &unique_index_schema,
                                  char *buf,
                                  const int64_t &buf_len,
                                  int64_t &pos);
  int print_user_definition(uint64_t tenant_id,
                            const ObUserInfo &user_info,
                            char *buf,
                            const int64_t &buf_len,
                            int64_t &pos,
                            bool is_role);
  int print_synonym_definition(const ObSynonymInfo &synonym_info,
                                char *buf,
                                const int64_t &buf_len,
                                int64_t &pos);
  int add_create_tenant_variables(
      const uint64_t tenant_id, common::ObMySQLProxy *const sql_proxy,
      char *buf, const int64_t buf_len, int64_t &pos) const;
  
  int print_hash_partition_elements(const ObPartitionSchema *&schema,
                                    char* buf,
                                    const int64_t& buf_len,
                                    int64_t& pos,
                                    bool print_sub_part_element,
                                    bool agent_mode,
                                    const common::ObTimeZoneInfo *tz_info) const;
  int print_external_table_file_info(const ObTableSchema &table_schema,
                                     ObIAllocator& allocator,
                                     char* buf,
                                     const int64_t& buf_len,
                                     int64_t& pos) const;
  int print_table_definition_column_group(const ObTableSchema &table_schema,
                                          char* buf,
                                          const int64_t& buf_len,
                                          int64_t& pos) const;
  int print_identifier(char* buf,
                       const int64_t& buf_len,
                       int64_t& pos,
                       const ObString &ident,
                       bool is_oracle_mode) const;

  int print_view_define_str(char* buf,
                            const int64_t &buf_len,
                            int64_t& pos,
                            bool is_oracle_mode,
                            const ObString &sql) const;

  int print_column_lob_params(const ObColumnSchemaV2 &column_schema,
                             char* buf,
                             const int64_t& buf_len,
                             int64_t& pos) const;
  int print_table_definition_lob_params(const ObTableSchema &table_schema,
                                        char* buf,
                                        const int64_t& buf_len,
                                        int64_t& pos) const;

private:
  static bool is_subpartition_valid_in_mysql(const ObTableSchema &table_schema)
  {
    const ObPartitionOption &part_opt = table_schema.get_part_option();
    const ObPartitionOption &sub_part_opt = table_schema.get_sub_part_option();
    ObPartitionFuncType type = part_opt.get_part_func_type();
    ObPartitionFuncType sub_type = sub_part_opt.get_part_func_type();
    return is_hash_like_part(sub_type) && !is_hash_like_part(type);
  }

  ObSchemaGetterGuard &schema_guard_;
  bool strict_compat_;
  bool sql_quote_show_create_;
  bool ansi_quotes_;
};


}//end of namespace schema
}//end of namespace share
}//end of namespace oceanbase

#endif
