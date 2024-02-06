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

#ifndef OCEANBASE_SQL_RESOLVER_SCHEMA_CHECKER2_
#define OCEANBASE_SQL_RESOLVER_SCHEMA_CHECKER2_

#include <stdint.h>
#include "lib/string/ob_string.h"
#include "lib/container/ob_iarray.h"
#include "lib/container/ob_array_wrap.h"
#include "share/schema/ob_column_schema.h"
#include "sql/resolver/ob_stmt_type.h"
//#include "sql/resolver/dml/ob_select_stmt.h"
#include "share/schema/ob_dblink_mgr.h"

#define PRIV_CHECK_FLAG_NORMAL 0
#define PRIV_CHECK_FLAG_DISABLE 1
#define PRIV_CHECK_FLAG_IN_PL 2

namespace oceanbase {
using namespace common;
namespace share {
namespace schema {
class ObTenantSchema;
class ObUserInfo;
class ObDatabaseSchema;
class ObTablegroupSchema;
class ObTableSchema;
class ObSimpleTableSchemaV2;
class ObColumnSchemaV2;
class ObSessionPrivInfo;
class ObStmtNeedPrivs;
class ObStmtOraNeedPrivs;
class ObSchemaGetterGuard;
class ObUDF;
}  // namespace schema
}  // namespace share
namespace sql {
// wrapper of schema manager which is used by SQL module

#define LBCA_OP_FLAG 1

class ObSqlSchemaGuard;
class ObSchemaChecker {
public:
  ObSchemaChecker();
  virtual ~ObSchemaChecker();
  int init(share::schema::ObSchemaGetterGuard& schema_mgr, uint64_t session_id = common::OB_INVALID_ID);
  int init(ObSqlSchemaGuard& schema_guard, uint64_t session_id = common::OB_INVALID_ID);
  ObSqlSchemaGuard* get_sql_schema_guard()
  {
    return sql_schema_mgr_;
  }
  share::schema::ObSchemaGetterGuard* get_schema_guard()
  {
    return schema_mgr_;
  }

  int check_priv(const share::schema::ObSessionPrivInfo& session_priv,
      const share::schema::ObStmtNeedPrivs& stmt_need_privs) const;

  int check_ora_priv(const uint64_t tenant_id, const uint64_t uid,
      const share::schema::ObStmtOraNeedPrivs& stmt_need_privs, const ObIArray<uint64_t>& role_id_array) const;

  int check_priv_or(
      const share::schema::ObSessionPrivInfo& session_priv, const share::schema::ObStmtNeedPrivs& stmt_need_privs);

  int check_db_access(share::schema::ObSessionPrivInfo& s_priv, const common::ObString& database_name) const;

  int check_table_show(const share::schema::ObSessionPrivInfo& s_priv, const common::ObString& db,
      const common::ObString& table, bool& allow_show) const;
  int check_routine_show(const share::schema::ObSessionPrivInfo& s_priv, const common::ObString& db,
      const common::ObString& routine, bool& allow_show) const;
  int check_column_exists(const uint64_t table_id, const common::ObString& column_name, bool& is_exist);
  int check_column_exists(uint64_t table_id, uint64_t column_id, bool& is_exist);
  int check_table_or_index_exists(
      const uint64_t tenant_id, const uint64_t database_id, const common::ObString& table_name, bool& is_exist);
  int check_table_exists(const uint64_t tenant_id, const uint64_t database_id, const common::ObString& table_name,
      const bool is_index, bool& is_exist);
  // int check_table_exists(uint64_t table_id, bool &is_exist) const;
  int check_table_exists(const uint64_t tenant_id, const common::ObString& database_name,
      const common::ObString& table_name, const bool is_index_table, bool& is_exist);

  int get_table_id(const uint64_t tenant_id, const uint64_t database_id, const common::ObString& table_name,
      const bool is_index_table, uint64_t& table_id);
  // int get_database_name(const uint64_t tenant_id,
  //                      const uint64_t database_id,
  //                      common::ObString &database_name) const;
  int get_database_id(const uint64_t tenant_id, const common::ObString& database_name, uint64_t& database_id);
  // int get_local_table_id(const uint64_t tenant_id,
  //                       const uint64_t database_id,
  //                       const common::ObString &table_name,
  //                       uint64_t &table_id) const;
  int get_user_id(const uint64_t tenant_id, const common::ObString& user_name, const common::ObString& host_name,
      uint64_t& user_id);
  int get_user_info(const uint64_t tenant_id, const common::ObString& user_name, const common::ObString& host_name,
      const share::schema::ObUserInfo*& user_info);
  int get_user_info(const uint64_t user_id, const share::schema::ObUserInfo*& user_info);
  int check_table_exists_with_synonym(const uint64_t tenant_id, const common::ObString& tbl_db_name,
      const common::ObString& tbl_name, bool is_index_table, bool& has_synonym, bool& table_exist);
  int get_table_schema_with_synonym(const uint64_t tenant_id, const common::ObString& tbl_db_name,
      const common::ObString& tbl_name, bool is_index_table, bool& has_synonym, common::ObString& new_db_name,
      common::ObString& new_tbl_name, const share::schema::ObTableSchema*& tbl_schema);
  int get_table_schema(const uint64_t tenant_id, const common::ObString& database_name,
      const common::ObString& table_name, const bool is_index_table, const share::schema::ObTableSchema*& table_schema);
  int get_table_schema(const uint64_t tenant_id, const uint64_t database_id, const common::ObString& table_name,
      const bool is_index_table, const bool cte_table_fisrt, const share::schema::ObTableSchema*& table_schema);
  int get_table_schema(const uint64_t table_id, const share::schema::ObTableSchema*& table_schema) const;
  int get_link_table_schema(const uint64_t dblink_id, const common::ObString& database_name,
      const common::ObString& table_name, const share::schema::ObTableSchema*& table_schema);

  int get_simple_table_schema(const uint64_t tenant_id, const uint64_t& db_id, const ObString& table_name,
      const bool is_index_table, const share::schema::ObSimpleTableSchemaV2*& simple_table_schema);
  // int column_can_be_droped(const uint64_t table_id, const uint64_t column_id, bool &can_be_drop) const;
  int get_column_schema(const uint64_t table_id, const common::ObString& column_name,
      const share::schema::ObColumnSchemaV2*& column_schema, const bool get_hidden = false);
  int get_column_schema(const uint64_t table_id, const uint64_t column_id,
      const share::schema::ObColumnSchemaV2*& column_schema, const bool get_hidden = false);
  // int check_is_rowkey_column(const uint64_t tenant_id,
  //                      const uint64_t database_id,
  //                      const common::ObString &table_name,
  //                      const common::ObString &column_name,
  //                      const bool is_index_table,
  //                      bool &is_rowkey_column) const;
  // int check_is_index_table(uint64_t table_id, bool &is_index_table) const;
  int get_can_read_index_array(uint64_t table_id, uint64_t* index_tid_array, int64_t& size, bool with_mv) const;
  int get_can_write_index_array(
      uint64_t table_id, uint64_t* index_tid_array, int64_t& size, bool only_global = false) const;
  int column_is_key(uint64_t table_id, uint64_t column_id, bool& is_key);
  // tenant
  int get_tenant_id(const common::ObString& tenant_name, uint64_t& teannt_id);
  int get_tenant_info(const uint64_t& tenant_id, const share::schema::ObTenantSchema*& tenant_schema);
  int get_sys_variable_schema(
      const uint64_t& tenant_id, const share::schema::ObSysVariableSchema*& sys_variable_schema);
  int get_database_schema(
      const uint64_t tenant_id, const uint64_t database_id, const share::schema::ObDatabaseSchema*& database_schema);
  int get_fulltext_column(uint64_t table_id, const share::schema::ColumnReferenceSet& column_set,
      const share::schema::ObColumnSchemaV2*& column_schema) const;
  // check if there is an index on this column
  int check_column_has_index(uint64_t table_id, uint64_t column_id, bool& has_index);
  int check_if_partition_key(uint64_t table_id, uint64_t column_id, bool& is_part_key) const;
  // int get_collation_info_from_database(const uint64_t tenant_id,
  //                                     const uint64_t database_id,
  //                                     common::ObCharsetType &char_type,
  //                                     common::ObCollationType &coll_type);
  // int get_collation_info_from_tenant(const uint64_t tenant_id,
  //                                   common::ObCharsetType &char_type,
  //                                   common::ObCollationType &coll_type);

  int get_syn_info(const uint64_t tenant_id, const ObString& database_name, const ObString& sym_name,
      ObString& obj_dbname, ObString& obj_name, bool& exists);

  int check_has_all_server_readonly_replica(uint64_t table_id, bool& has);
  int check_is_all_server_readonly_replica(uint64_t table_id, bool& is);
  int get_synonym_info_version(uint64_t synonym_id, int64_t& synonym_version);
  int get_synonym_schema(uint64_t tenant_id, const uint64_t database_id, const common::ObString& synonym_name,
      uint64_t& object_database_id, uint64_t& synonym_id, common::ObString& object_table_name, bool& exist);
  int get_obj_info_recursively_with_synonym(const uint64_t tenant_id, const uint64_t syn_db_id,
      const common::ObString& syn_name, uint64_t& obj_db_id, common::ObString& obj_name,
      common::ObIArray<uint64_t>& syn_id_arr, bool is_first_called = true);
  int get_udf_info(
      uint64_t tenant_id, const common::ObString& udf_name, const share::schema::ObUDF*& udf_info, bool& exist);
  int check_sequence_exist_with_name(const uint64_t tenant_id, const uint64_t database_id,
      const common::ObString& sequence_name, bool& exists, uint64_t& sequence_id);
  int get_sequence_id(const uint64_t tenant_id, const common::ObString& database_name,
      const common::ObString& sequence_name, uint64_t& sequence_id);
  int add_fake_cte_schema(share::schema::ObTableSchema* tbl_schema);
  int find_fake_cte_schema(common::ObString tblname, bool& exist);
  int get_schema_version(uint64_t table_id, share::schema::ObSchemaType schema_type, int64_t& schema_version);
  share::schema::ObSchemaGetterGuard* get_schema_mgr()
  {
    return schema_mgr_;
  }
  int get_tablegroup_schema(const int64_t tenant_id, const common::ObString& tablegroup_name,
      const share::schema::ObTablegroupSchema*& tablegroup_schema);
  int get_idx_schema_by_origin_idx_name(const uint64_t tenant_id, const uint64_t database_id,
      const common::ObString& index_name, const share::schema::ObTableSchema*& table_schema);
  int get_profile_id(const uint64_t tenant_id, const common::ObString& profile_name, uint64_t& profile_id);
  int get_object_type(const uint64_t tenant_id, const common::ObString& database_name,
      const common::ObString& table_name, share::schema::ObObjectType& object_type, uint64_t& object_id,
      common::ObString& object_db_name, bool is_directory, bool explicit_db, const common::ObString& prev_table_name);
  int get_object_type_with_view_info(common::ObIAllocator* allocator, void* param, const uint64_t tenant_id,
      const common::ObString& database_name, const common::ObString& table_name,
      share::schema::ObObjectType& object_type, uint64_t& object_id, void*& view_query, bool is_directory,
      common::ObString& object_db_name, bool explicit_db, const common::ObString& prev_table_name);
  int check_access_to_obj(const uint64_t tenant_id, const uint64_t user_id, const uint64_t obj_id,
      const sql::stmt::StmtType stmt_type, const ObIArray<uint64_t>& role_id_array, bool& accessible);
  int check_ora_ddl_priv(const uint64_t tenant_id, const uint64_t user_id, const common::ObString& database_name,
      const sql::stmt::StmtType stmt_type, const ObIArray<uint64_t>& role_id_array);
  int check_ora_ddl_priv(const uint64_t tenant_id, const uint64_t user_id, const common::ObString& database_name,
      const bool is_replace, const sql::stmt::StmtType stmt_type, const sql::stmt::StmtType stmt_type2,
      const ObIArray<uint64_t>& role_id_array);
  int check_ora_ddl_priv(const uint64_t tenant_id, const uint64_t user_id, const common::ObString& database_name,
      const uint64_t obj_id, const uint64_t obj_type, const sql::stmt::StmtType stmt_type,
      const ObIArray<uint64_t>& role_id_array);
  int check_ora_grant_obj_priv(const uint64_t tenant_id, const uint64_t user_id, const common::ObString& database_name,
      const uint64_t obj_id, const uint64_t obj_type, const share::ObRawObjPrivArray& table_priv_array,
      const ObSEArray<uint64_t, 4>& ins_col_ids, const ObSEArray<uint64_t, 4>& upd_col_ids,
      const ObSEArray<uint64_t, 4>& ref_col_ids, uint64_t& grantor_id_out, const ObIArray<uint64_t>& role_id_array);
  int check_ora_grant_sys_priv(const uint64_t tenant_id, const uint64_t user_id,
      const share::ObRawPrivArray& sys_priv_array, const ObIArray<uint64_t>& role_id_array);
  int check_ora_grant_role_priv(const uint64_t tenant_id, const uint64_t user_id,
      const ObSEArray<uint64_t, 4>& role_granted_id_array, const ObIArray<uint64_t>& role_id_array);
  int set_lbca_op();
  bool is_lbca_op();

  static bool is_ora_priv_check();

  // dblink.
  int get_dblink_id(uint64_t tenant_id, const common::ObString& dblink_name, uint64_t& dblink_id);
  int get_dblink_user(const uint64_t tenant_id, const common::ObString& dblink_name, common::ObString& dblink_user,
      common::ObIAllocator& allocator);

private:
  int get_table_schema_inner(uint64_t table_id, const share::schema::ObTableSchema*& table_schema) const;
  int get_column_schema_inner(uint64_t table_id, const common::ObString& column_name,
      const share::schema::ObColumnSchemaV2*& column_schema) const;
  int get_column_schema_inner(
      uint64_t table_id, const uint64_t column_id, const share::schema::ObColumnSchemaV2*& column_schema) const;

private:
  bool is_inited_;
  share::schema::ObSchemaGetterGuard* schema_mgr_;
  ObSqlSchemaGuard* sql_schema_mgr_;
  // cte tmp schema, used for recursive cte service, the life cycle is only valid for this query
  common::ObArray<share::schema::ObTableSchema*, common::ModulePageAllocator, true> tmp_cte_schemas_;
  // record additional information of the checker, such as the operation of the security officer, etc
  int flag_;
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObSchemaChecker);
};
}  // end namespace sql
}  // end namespace oceanbase

#endif /* OCEANBASE_SQL_RESOLVER_SCHEMA_CHECKER2_ */
