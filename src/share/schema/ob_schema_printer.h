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

namespace oceanbase {
namespace common {
class ObTimeZoneInfo;
class ObObj;
class ObString;
class ObMySQLProxy;
class ObIAllocator;
namespace sqlclient {
class ObMySQLResult;
}
}  // namespace common

namespace share {
namespace schema {
class ObTenantSchema;
class ObDatabaseSchema;
class ObTablegroupSchema;
class ObTableSchema;
class ObColumnSchemaV2;
class ObSchemaGetterGuard;

class ObSchemaPrinter {
public:
  explicit ObSchemaPrinter(ObSchemaGetterGuard& schema_guard);
  virtual ~ObSchemaPrinter()
  {}

private:
  ObSchemaPrinter();
  DISALLOW_COPY_AND_ASSIGN(ObSchemaPrinter);

public:
  int print_table_definition(uint64_t table_id, char* buf, const int64_t& buf_len, int64_t& pos,
      const common::ObTimeZoneInfo* tz_info, const common::ObLengthSemantics default_length_semantics,
      bool agent_mode) const;
  int print_table_index_stroing(const share::schema::ObTableSchema* index_schema,
      const share::schema::ObTableSchema* table_schema, char* buf, const int64_t buf_len, int64_t& pos) const;
  int print_table_definition_fulltext_indexs(const common::ObIArray<common::ObString>& fulltext_indexs,
      const uint64_t virtual_column_id, char* buf, int64_t buf_len, int64_t& pos) const;
  int print_table_definition_table_options(const share::schema::ObTableSchema& table_schema,
      const common::ObIArray<common::ObString>& full_text_columns, const uint64_t virtual_column_id, char* buf,
      const int64_t buf_len, int64_t& pos, bool is_for_table_status, common::ObMySQLProxy* sql_proxy,
      bool is_agent_mode) const;
  int print_func_index_columns_definition(const common::ObString& column_name, uint64_t column_id, char* buf,
      int64_t buf_len, int64_t& pos, bool is_agent_mode) const;
  int print_index_definition_columns(const share::schema::ObTableSchema& data_schema,
      const share::schema::ObTableSchema& index_schema, common::ObIArray<common::ObString>& full_text_columns,
      uint64_t& virtual_column_id, char* buf, const int64_t buf_len, int64_t& pos, bool is_agent_mode) const;
  int print_full_text_columns_definition(const share::schema::ObTableSchema& data_schema,
      const common::ObString& generated_column_name, common::ObIArray<common::ObString>& full_text_columns, char* buf,
      const int64_t buf_len, int64_t& pos, bool& is_first, bool is_agent_mode) const;
  int print_index_table_definition(uint64_t index_table_id, char* buf, const int64_t buf_len, int64_t& pos,
      const common::ObTimeZoneInfo* tz_info, bool is_agent_mode) const;

  int print_view_definiton(uint64_t table_id, char* buf, const int64_t& buf_len, int64_t& pos) const;

  int print_database_definiton(
      uint64_t database_id, bool if_not_exists, char* buf, const int64_t& buf_len, int64_t& pos) const;

  int print_table_definition_columns(const ObTableSchema& table_schema, char* buf, const int64_t& buf_len, int64_t& pos,
      const common::ObTimeZoneInfo* tz_info, const common::ObLengthSemantics default_length_semantics,
      bool is_agent_mode = false) const;
  int print_generated_column_definition(const ObColumnSchemaV2& gen_col, char* buf, int64_t buf_len,
      const ObTableSchema& table_schema, int64_t& pos) const;
  int print_table_definition_indexes(
      const ObTableSchema& table_schema, char* buf, const int64_t& buf_len, int64_t& pos, bool is_unique_index) const;
  int print_table_definition_constraints(
      const ObTableSchema& table_schema, char* buf, const int64_t& buf_len, int64_t& pos) const;
  int print_index_column(const ObTableSchema& table_schema, const ObColumnSchemaV2& column,
      common::ObIArray<common::ObString>& ctxcat_cols, bool is_last, char* buf, int64_t buf_len, int64_t& pos) const;
  int print_fulltext_index_column(const ObTableSchema& table_schema, const ObColumnSchemaV2& column,
      common::ObIArray<common::ObString>& ctxcat_cols, bool is_last, char* buf, int64_t buf_len, int64_t& pos) const;
  int print_prefix_index_column(
      const ObColumnSchemaV2& column, bool is_last, char* buf, int64_t buf_len, int64_t& pos) const;
  int print_ordinary_index_column_expr(
      const ObColumnSchemaV2& column, bool is_last, char* buf, int64_t buf_len, int64_t& pos) const;
  int print_table_definition_fulltext_indexs(
      const common::ObIArray<common::ObString>& fulltext_indexs, char* buf, int64_t buf_len, int64_t& pos) const;
  int print_table_definition_rowkeys(
      const ObTableSchema& table_schema, char* buf, const int64_t& buf_len, int64_t& pos) const;
  int print_rowkey_info(const common::ObRowkeyInfo& rowkey_info, uint64_t table_id, char* buf, const int64_t& buf_len,
      int64_t& pos) const;
  int print_table_definition_foreign_keys(
      const ObTableSchema& table_schema, char* buf, const int64_t& buf_len, int64_t& pos) const;
  int print_column_list(const ObTableSchema& table_schema, const common::ObIArray<uint64_t>& column_ids, char* buf,
      const int64_t& buf_len, int64_t& pos) const;
  int print_table_definition_store_format(
      const ObTableSchema& table_schema, char* buf, const int64_t& buf_len, int64_t& pos) const;
  int print_table_definition_table_options(const ObTableSchema& table_schema, char* buf, const int64_t& buf_len,
      int64_t& pos, bool is_for_table_status, bool agent_mode = false) const;
  int print_table_definition_comment_oracle(
      const ObTableSchema& table_schema, char* buf, const int64_t& buf_len, int64_t& pos) const;
  int print_table_definition_partition_options(const ObTableSchema& table_schema, char* buf, const int64_t& buf_len,
      int64_t& pos, bool agent_mode, const common::ObTimeZoneInfo* tz_info) const;
  int print_list_partition_elements(const ObPartitionSchema*& schema, char* buf, const int64_t& buf_len, int64_t& pos,
      bool agent_mode = false, bool tablegroup_def = false, const common::ObTimeZoneInfo* tz_info = NULL) const;
  int print_range_partition_elements(const ObPartitionSchema*& schema, char* buf, const int64_t& buf_len, int64_t& pos,
      bool agent_mode, bool tablegroup_def, const common::ObTimeZoneInfo* tz_info) const;
  int print_hash_sub_partition_elements(ObSubPartition** sub_part_array, const int64_t sub_part_num, char* buf,
      const int64_t& buf_len, int64_t& pos) const;
  int print_hash_sub_partition_elements_for_tablegroup(
      const ObPartitionSchema*& schema, char* buf, const int64_t& buf_len, int64_t& pos) const;
  int print_range_sub_partition_elements(ObSubPartition** sub_part_array, const int64_t sub_part_num, char* buf,
      const int64_t& buf_len, int64_t& pos, const common::ObTimeZoneInfo* tz_info) const;
  int print_list_sub_partition_elements(ObSubPartition** sub_part_array, const int64_t sub_part_num, char* buf,
      const int64_t& buf_len, int64_t& pos, const common::ObTimeZoneInfo* tz_info) const;

  int print_template_sub_partition_elements(const ObPartitionSchema*& schema, char* buf, const int64_t& buf_len,
      int64_t& pos, const common::ObTimeZoneInfo* tz_info, bool is_tablegroup) const;
  int print_individual_sub_partition_elements(const ObPartitionSchema*& schema, const ObPartition* partition, char* buf,
      const int64_t& buf_len, int64_t& pos, const common::ObTimeZoneInfo* tz_info) const;
  int print_tablegroup_definition(uint64_t tablegroup_id, char* buf, const int64_t& buf_len, int64_t& pos,
      bool agent_mode, const common::ObTimeZoneInfo* tz_info);
  int print_tablegroup_definition_tablegroup_options(const share::schema::ObTablegroupSchema& tablegroup_schema,
      char* buf, const int64_t& buf_len, int64_t& pos, bool agent_mode = false) const;
  int print_tablegroup_definition_partition_options(const share::schema::ObTablegroupSchema& tablegroup_schema,
      char* buf, const int64_t& buf_len, int64_t& pos, bool agent_mode, const common::ObTimeZoneInfo* tz_info) const;
  int print_tenant_definition(uint64_t tenant_id, common::ObMySQLProxy* sql_proxy, char* buf, const int64_t& buf_len,
      int64_t& pos, bool is_agent_mode = false) const;
  int print_foreign_key_definition(
      const share::schema::ObForeignKeyInfo& foreign_key_info, char* buf, int64_t buf_len, int64_t& pos) const;
  int print_table_definition_on_commit_options(
      const ObTableSchema& table_schema, char* buf, const int64_t& buf_len, int64_t& pos) const;
  int deep_copy_obj(common::ObIAllocator& allocator, const common::ObObj& src, common::ObObj& dest) const;
  static int get_sequence_value(common::ObMySQLProxy& sql_proxy, const uint64_t tenant_id, const uint64_t table_id,
      const uint64_t column_id, uint64_t& seq_value);
  int print_sequence_definition(
      const ObSequenceSchema& sequence_schema, char* buf, const int64_t& buf_len, int64_t& pos);
  int print_constraint_definition(const ObDatabaseSchema& db_schema, const ObTableSchema& table_schema,
      uint64_t constraint_id, char* buf, const int64_t& buf_len, int64_t& pos);
  int print_user_definition(
      uint64_t tenant_id, const ObUserInfo& user_info, char* buf, const int64_t& buf_len, int64_t& pos);
  int add_create_tenant_variables(const uint64_t tenant_id, common::ObMySQLProxy* const sql_proxy, char* buf,
      const int64_t buf_len, int64_t& pos) const;

  int print_hash_partition_elements(const ObPartitionSchema*& schema, char* buf, const int64_t& buf_len, int64_t& pos,
      bool agent_mode, const common::ObTimeZoneInfo* tz_info) const;

private:
  ObSchemaGetterGuard& schema_guard_;
};

}  // end of namespace schema
}  // end of namespace share
}  // end of namespace oceanbase

#endif
