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
#include "share/schema/ob_column_schema.h"
#include "share/schema/ob_routine_info.h"
#include "share/schema/ob_package_info.h"
#include "sql/resolver/ob_stmt_type.h"
//#include "sql/resolver/dml/ob_select_stmt.h"
#include "share/schema/ob_dblink_mgr.h"
#include "share/schema/ob_udt_info.h"
#include "share/schema/ob_trigger_info.h"

#define PRIV_CHECK_FLAG_NORMAL     0
#define PRIV_CHECK_FLAG_DISABLE    1
#define PRIV_CHECK_FLAG_IN_PL      2

namespace oceanbase
{
namespace common
{
class ObString;
}
namespace sql {
class ObSynonymChecker;
}
namespace share
{
namespace schema
{
class ObTenantSchema;
class ObUserInfo;
class ObDatabaseSchema;
class ObTablegroupSchema;
class ObTableSchema;
class ObSimpleTableSchemaV2;
class ObColumnSchemaV2;
struct ObSessionPrivInfo;
struct ObStmtNeedPrivs;
struct ObStmtOraNeedPrivs;
class ObSchemaGetterGuard;
class ObUDF;
class ObUDTTypeInfo;
class ObKeystoreSchema;
}
}
namespace sql
{
// wrapper of schema manager which is used by SQL module

#define LBCA_OP_FLAG  1

class ObSqlSchemaGuard;
class ObSchemaChecker
{
public:
  ObSchemaChecker();
  virtual ~ObSchemaChecker();
  int init(share::schema::ObSchemaGetterGuard &schema_mgr, uint64_t session_id = common::OB_INVALID_ID);
  int init(ObSqlSchemaGuard &schema_guard, uint64_t session_id = common::OB_INVALID_ID);
  ObSqlSchemaGuard *get_sql_schema_guard() { return sql_schema_mgr_; }
  share::schema::ObSchemaGetterGuard *get_schema_guard() { return schema_mgr_; }
  // need satifing each priv in stmt_need_privs
  int check_priv(const share::schema::ObSessionPrivInfo &session_priv,
                 const share::schema::ObStmtNeedPrivs &stmt_need_privs) const;

  int check_ora_priv(const uint64_t tenant_id,
                     const uint64_t uid,
                     const share::schema::ObStmtOraNeedPrivs &stmt_need_privs,
                     const ObIArray<uint64_t> &role_id_array) const;
  // need satifing one of stmt_need_privs
  int check_priv_or(const share::schema::ObSessionPrivInfo &session_priv,
                    const share::schema::ObStmtNeedPrivs &stmt_need_privs);

  int check_db_access(share::schema::ObSessionPrivInfo &s_priv,
                      const common::ObString &database_name) const;

  int check_table_show(const share::schema::ObSessionPrivInfo &s_priv,
                       const common:: ObString &db,
                       const common::ObString &table,
                       bool &allow_show) const;
  int check_routine_show(const share::schema::ObSessionPrivInfo &s_priv,
                         const common::ObString &db,
                         const common::ObString &routine,
                         bool &allow_show) const;
  int check_trigger_show(const share::schema::ObSessionPrivInfo &s_priv,
                         const common::ObString &db,
                         const common::ObString &trigger,
                         bool &allow_show) const;
  int check_column_exists(const uint64_t tenant_id, const uint64_t table_id,
                          const common::ObString &column_name,
                          bool &is_exist,
                          bool is_link = false);
  int check_column_exists(const uint64_t tenant_id, uint64_t table_id, uint64_t column_id, bool &is_exist, bool is_link = false);
  int check_table_or_index_exists(const uint64_t tenant_id,
                                  const uint64_t database_id,
                                  const common::ObString &table_name,
                                  const bool with_hidden_flag,
                                  const bool is_built_in_index,
                                  bool &is_exist);
  int check_table_exists(const uint64_t tenant_id,
                         const uint64_t database_id,
                         const common::ObString &table_name,
                         const bool is_index,
                         const bool with_hidden_flag,
                         bool &is_exist,
                         const bool is_built_in_index = false);
  //int check_table_exists(uint64_t table_id, bool &is_exist) const;
  int check_table_exists(const uint64_t tenant_id,
                        const common::ObString &database_name,
                        const common::ObString &table_name,
                        const bool is_index_table,
                        const bool with_hidden_flag,
                        bool &is_exist,
                        const bool is_built_in_index = false);

  // mock_fk_parent_table begin
  int get_mock_fk_parent_table_with_name(
      const uint64_t tenant_id,
      const uint64_t database_id,
      const common::ObString &name,
      const share::schema::ObMockFKParentTableSchema *&schema);
  // mock_fk_parent_table end

  int get_table_id(const uint64_t tenant_id,
                   const uint64_t database_id,
                   const common::ObString &table_name,
                   const bool is_index_table,
                   uint64_t &table_id);
  //int get_database_name(const uint64_t tenant_id,
  //                      const uint64_t database_id,
  //                      common::ObString &database_name) const;
  int get_database_id(const uint64_t tenant_id,
                      const common::ObString &database_name,
                      uint64_t &database_id) const;
  //int get_local_table_id(const uint64_t tenant_id,
  //                       const uint64_t database_id,
  //                       const common::ObString &table_name,
  //                       uint64_t &table_id) const;
  int get_user_id(const uint64_t tenant_id,
                  const common::ObString &user_name,
                  const common::ObString &host_name,
                  uint64_t &user_id);
  int get_user_info(const uint64_t tenant_id,
                  const common::ObString &user_name,
                  const common::ObString &host_name,
                  const share::schema::ObUserInfo *&user_info);
  int get_user_info(const uint64_t tenant_id,
                    const uint64_t user_id,
                    const share::schema::ObUserInfo *&user_info);
  // TODO: public syn
  // 先使用tbl_name来检查tbl是否存在，不存在再将tbl_name看作synonym name
  // 检查同义词代表的表是否存在
  int check_table_exists_with_synonym(const uint64_t tenant_id,
                                      const common::ObString &tbl_db_name,
                                      const common::ObString &tbl_name,
                                      bool is_index_table,
                                      bool &has_synonym,
                                      bool &table_exist);
  // 先尝试获取tbl_name的schema，不存在则将tbl_name看作synonym name,获取同义词
  // 代表的表的schema
  int get_table_schema_with_synonym(const uint64_t tenant_id,
                                    const common::ObString &tbl_db_name,
                                    const common::ObString &tbl_name,
                                    bool is_index_table,
                                    bool &has_synonym,
                                    common::ObString &new_db_name,
                                    common::ObString &new_tbl_name,
                                    const share::schema::ObTableSchema *&tbl_schema);
  int get_table_schema(const uint64_t tenant_id,
                       const common::ObString &database_name,
                       const common::ObString &table_name,
                       const bool is_index_table,
                       const share::schema::ObTableSchema *&table_schema);
  int get_table_schema(const uint64_t tenant_id,
                       const uint64_t database_id,
                       const common::ObString &table_name,
                       const bool is_index_table,
                       const bool cte_table_fisrt,
                       const bool with_hidden_flag,
                       const share::schema::ObTableSchema *&table_schema,
                       const bool is_built_in_index = false);
  int get_table_schema(const uint64_t tenant_id, const uint64_t table_id, const share::schema::ObTableSchema *&table_schema, bool is_link = false) const;
  int get_link_table_schema(const uint64_t dblink_id,
                            const common::ObString &database_name,
                            const common::ObString &table_name,
                            const share::schema::ObTableSchema *&table_schema,
                            sql::ObSQLSessionInfo *session_info,
                            const common::ObString &dblink_name,
                            bool is_reverse_link);
  int set_link_table_schema(uint64_t dblink_id,
                            const common::ObString &database_name,
                            share::schema::ObTableSchema *table_schema);
  int get_simple_table_schema(const uint64_t tenant_id,
                              const uint64_t &db_id,
                              const ObString &table_name,
                              const bool is_index_table,
                              const share::schema::ObSimpleTableSchemaV2 *&simple_table_schema);
  //int column_can_be_droped(const uint64_t table_id, const uint64_t column_id, bool &can_be_drop) const;
  int get_column_schema(const uint64_t tenant_id, const uint64_t table_id,
                        const common::ObString &column_name,
                        const share::schema::ObColumnSchemaV2 *&column_schema,
                        const bool get_hidden = false,
                        bool is_link = false);
  int get_column_schema(const uint64_t tenant_id,
                        const uint64_t table_id,
                        const uint64_t column_id,
                        const share::schema::ObColumnSchemaV2 *&column_schema,
                        const bool get_hidden = false,
                        bool is_link = false);
  //int check_is_rowkey_column(const uint64_t tenant_id,
  //                      const uint64_t database_id,
  //                      const common::ObString &table_name,
  //                      const common::ObString &column_name,
  //                      const bool is_index_table,
  //                      bool &is_rowkey_column) const;
  //int check_is_index_table(uint64_t table_id, bool &is_index_table) const;
  int get_can_read_index_array(const uint64_t tenant_id, uint64_t table_id, uint64_t *index_tid_array, int64_t &size, bool with_mv) const;
  int get_can_write_index_array(const uint64_t tenant_id, uint64_t table_id, uint64_t *index_tid_array, int64_t &size, bool only_global = false, bool with_mlog = false) const;
  // tenant
  int get_tenant_id(const common::ObString &tenant_name, uint64_t &teannt_id);
  int get_tenant_info(const uint64_t &tenant_id, const share::schema::ObTenantSchema *&tenant_schema);
  int get_sys_variable_schema(const uint64_t &tenant_id,
                              const share::schema::ObSysVariableSchema *&sys_variable_schema);
  int get_database_schema(const uint64_t tenant_id,
                          const uint64_t database_id,
                          const share::schema::ObDatabaseSchema *&database_schema);
  int get_database_schema(const uint64_t tenant_id,
                          const ObString &database_name,
                          const share::schema::ObDatabaseSchema *&database_schema);
  int get_fulltext_column(const uint64_t tenant_id, uint64_t table_id,
                          const share::schema::ColumnReferenceSet &column_set,
                          const share::schema::ObColumnSchemaV2 *&column_schema) const;
  //check if there is an index on this column
  int check_column_has_index(const uint64_t tenant_id, uint64_t table_id, uint64_t column_id, bool &has_index, bool is_link = false);
  int check_if_partition_key(const uint64_t tenant_id, uint64_t table_id, uint64_t column_id, bool &is_part_key, bool is_link = false) const;
  //int get_collation_info_from_database(const uint64_t tenant_id,
  //                                     const uint64_t database_id,
  //                                     common::ObCharsetType &char_type,
  //                                     common::ObCollationType &coll_type);
  //int get_collation_info_from_tenant(const uint64_t tenant_id,
  //                                   common::ObCharsetType &char_type,
  //                                   common::ObCollationType &coll_type);
  int get_udt_info(const uint64_t tenant_id,
                   const uint64_t udt_id,
                   const share::schema::ObUDTTypeInfo *&udt_inf);
  int get_udt_info(const uint64_t tenant_id,
                   const common::ObString &database_name,
                   const common::ObString &type_name,
                   const share::schema::ObUDTTypeInfo *&udt_info);
  int get_udt_info(const uint64_t tenant_id,
                   const common::ObString &database_name,
                   const common::ObString &type_name,
                   const share::schema::ObUDTTypeCode &type_code,
                   const share::schema::ObUDTTypeInfo *&udt_info);
  int get_udt_id(uint64_t tenant_id, uint64_t database_id, uint64_t package_id,
                 const ObString &udt_name, uint64_t &udt_id);

  int check_udt_exist(uint64_t tenant_id, uint64_t database_id,
                      uint64_t package_id, share::schema::ObUDTTypeCode type_code,
                      const ObString &udt_name, bool &exist);

  int get_udt_routine_infos(const uint64_t tenant_id,
                       const uint64_t udt_id,
                       const common::ObString &database_name,
                       const common::ObString &routine_name,
                       const share::schema::ObRoutineType routine_type,
                       common::ObIArray<const share::schema::ObIRoutineInfo *> &routine_infos);

  int get_udt_routine_infos(const uint64_t tenant_id,
                       const uint64_t database_id,
                       const uint64_t udt_id,
                       const common::ObString &routine_name,
                       const share::schema::ObRoutineType routine_type,
                       common::ObIArray<const share::schema::ObIRoutineInfo *> &routine_infos);
  
  int get_routine_infos_in_udt(const uint64_t tenant_id,
                               const uint64_t udt_id,
                       common::ObIArray<const share::schema::ObRoutineInfo *> &routine_infos);

  int get_routine_info(const uint64_t tenant_id,
                       const uint64_t routine_id,
                       const share::schema::ObRoutineInfo *&routine_info);
  int get_standalone_procedure_info(const uint64_t tenant_id,
                                   const uint64_t db_id,
                                   const ObString &routine_name,
                                   const share::schema::ObRoutineInfo *&routine_info);
  int get_standalone_procedure_info(const uint64_t tenant_id,
                                    const common::ObString &database_name,
                                    const common::ObString &routine_name,
                                    const share::schema::ObRoutineInfo *&routine_info);
  int get_standalone_function_info(const uint64_t tenant_id,
                                   const uint64_t db_id,
                                   const ObString &routine_name,
                                   const share::schema::ObRoutineInfo *&routine_info);
  int get_standalone_function_info(const uint64_t tenant_id,
                                   const common::ObString &database_name,
                                   const common::ObString &routine_name,
                                   const share::schema::ObRoutineInfo *&routine_info);
  int get_package_routine_infos(const uint64_t tenant_id,
                        const uint64_t package_id,
                        const uint64_t db_id,
                        const common::ObString &routine_name,
                        const share::schema::ObRoutineType routine_type,
                        common::ObIArray<const share::schema::ObIRoutineInfo *> &routine_infos);
  int get_package_routine_infos(const uint64_t tenant_id,
                       const uint64_t package_id,
                       const common::ObString &database_name,
                       const common::ObString &routine_name,
                       const share::schema::ObRoutineType routine_type,
                       common::ObIArray<const share::schema::ObIRoutineInfo *> &routine_infos);
  int get_package_info(const uint64_t tenant_id,
                       const common::ObString &database_name,
                       const common::ObString &package_name,
                       const share::schema::ObPackageType type,
                       const int64_t compatible_mode,
                       const share::schema::ObPackageInfo *&package_info);
  int get_trigger_info(const uint64_t tenant_id,
                       const common::ObString &database_name,
                       const common::ObString &tg_name,
                       const share::schema::ObTriggerInfo *&tg_info);
  int get_syn_info(const uint64_t tenant_id,
                   const ObString &database_name,
                   const ObString &sym_name,
                   ObString &obj_dbname,
                   ObString &obj_name,
                   uint64_t &synonym_id,
                   uint64_t &database_id,
                   bool &exists);
  int get_package_id(const uint64_t tenant_id,
                     const uint64_t database_id,
                     const common::ObString &package_name,
                     const int64_t compatible_mode,
                     uint64_t &package_id);
  int get_package_id(const uint64_t tenant_id,
                     const common::ObString &database_name,
                     const common::ObString &package_name,
                     const int64_t compatible_mode,
                     uint64_t &package_id);
  int get_udt_id(const uint64_t tenant_id,
                 const ObString &database_name,
                 const ObString &udt_name,
                 uint64_t &udt_id);
  int get_sys_udt_id(const ObString &udt_name,
                     uint64_t &udt_id);
  int get_routine_id(const uint64_t tenant_id,
                     const ObString &database_name,
                     const ObString &routine_name,
                     uint64_t &routine_id,
                     bool &is_proc);
  int get_package_body_id(const uint64_t tenant_id,
                       const common::ObString &database_name,
                       const common::ObString &package_name,
                       const int64_t compatible_mode,
                       uint64_t &package_body_id);
  int check_has_all_server_readonly_replica(const uint64_t tenant_id, uint64_t table_id, bool &has);
  int check_is_all_server_readonly_replica(const uint64_t tenant_id, uint64_t table_id, bool &is);
  int get_synonym_schema(uint64_t tenant_id,
                         const uint64_t database_id,
                         const common::ObString &synonym_name,
                         uint64_t &object_database_id,
                         uint64_t &synonym_id,
                         common::ObString &object_table_name,
                         bool &exist,
                         bool search_public_schema = true,
                         bool *is_public = NULL) const;
  int get_obj_info_recursively_with_synonym(const uint64_t tenant_id,
                                            const uint64_t syn_db_id,
                                            const common::ObString &syn_name,
                                            uint64_t &obj_db_id,
                                            common::ObString &obj_name,
                                            common::ObIArray<uint64_t> &syn_id_arr,
                                            bool is_first_called = true);
  int get_udf_info(uint64_t tenant_id,
                   const common::ObString &udf_name,
                   const share::schema::ObUDF *&udf_info,
                   bool &exist);
  int check_sequence_exist_with_name(const uint64_t tenant_id,
                                     const uint64_t database_id,
                                     const common::ObString &sequence_name,
                                     bool &exists,
                                     uint64_t &sequence_id) const;
  int get_sequence_id(const uint64_t tenant_id,
                      const common::ObString &database_name,
                      const common::ObString &sequence_name,
                      uint64_t &sequence_id) const;
  int check_keystore_exist(const uint64_t tenant_id,
                           bool &exist);
  int get_keystore_schema(const uint64_t tenant_id,
                          const share::schema::ObKeystoreSchema *&keystore_schema);
  int get_label_se_policy_name_by_column_name(const uint64_t tenant_id,
                                              const common::ObString &column_name,
                                              ObString &policy_name);
  int add_fake_cte_schema(share::schema::ObTableSchema* tbl_schema);
  int find_fake_cte_schema(common::ObString tblname, ObNameCaseMode mode, bool& exist);
  int adjust_fake_cte_column_type(uint64_t table_id,
                                  uint64_t column_id,
                                  const common::ColumnType &type,
                                  const common::ObAccuracy &accuracy);
  int get_schema_version(const uint64_t tenant_id, uint64_t table_id, share::schema::ObSchemaType schema_type, int64_t &schema_version);
  share::schema::ObSchemaGetterGuard *get_schema_mgr() { return schema_mgr_; }
  int get_tablegroup_schema(const int64_t tenant_id, const common::ObString &tablegroup_name,
                            const share::schema::ObTablegroupSchema *&tablegroup_schema);
  int get_idx_schema_by_origin_idx_name(const uint64_t tenant_id,
                                        const uint64_t database_id,
                                        const common::ObString &index_name,
                                        const share::schema::ObTableSchema *&table_schema);
  int get_tablespace_schema(const int64_t tenant_id, const common::ObString &tablespace_name,
                            const share::schema::ObTablespaceSchema *&tablespace_schema);
  int get_tablespace_id(const int64_t tenant_id, const common::ObString &tablespace_name,
                        int64_t &tablespace_id);
  int get_profile_id(const uint64_t tenant_id,
                     const common::ObString &profile_name,
                     uint64_t &profile_id);
  int check_exist_same_name_object_with_synonym(const uint64_t tenant_id,
                                                uint64_t database_id,
                                                const common::ObString &object_name,
                                                bool &exist,
                                                bool &is_private_syn);
  int get_object_type(const uint64_t tenant_id,
                      const common::ObString &database_name,
                      const common::ObString &table_name,
                      share::schema::ObObjectType &object_type,
                      uint64_t &object_id,
                      common::ObString &object_db_name,
                      bool is_directory,
                      bool explicit_db,
                      const common::ObString &prev_table_name,
                      ObSynonymChecker &synonym_checker);
  int get_object_type_with_view_info(common::ObIAllocator* allocator,
                                     void* param,
                                     const uint64_t tenant_id,
                                     const common::ObString &database_name,
                                     const common::ObString &table_name,
                                     share::schema::ObObjectType &object_type,
                                     uint64_t &object_id,
                                     void* & view_query,
                                     bool is_directory,
                                     common::ObString &object_db_name,
                                     bool explicit_db,
                                     const common::ObString &prev_table_name,
                                     ObSynonymChecker &synonym_checker);
  int check_access_to_obj(const uint64_t tenant_id,
                          const uint64_t user_id,
                          const uint64_t obj_id,
                          const common::ObString &database_name,
                          const sql::stmt::StmtType stmt_type,
                          const ObIArray<uint64_t> &role_id_array,
                          bool &accessible,
                          bool is_sys_view = false);
  int check_ora_ddl_priv(const uint64_t tenant_id,
                         const uint64_t user_id,
                         const common::ObString &database_name,
                         const sql::stmt::StmtType stmt_type,
                         const ObIArray<uint64_t> &role_id_array);
  int check_ora_ddl_priv(const uint64_t tenant_id,
                         const uint64_t user_id,
                         const common::ObString &database_name,
                         const bool is_replace,
                         const sql::stmt::StmtType stmt_type,
                         const sql::stmt::StmtType stmt_type2,
                         const ObIArray<uint64_t> &role_id_array);
  int check_ora_ddl_priv(const uint64_t tenant_id,
                         const uint64_t user_id,
                         const common::ObString &database_name,
                         const uint64_t obj_id,
                         const uint64_t obj_type,
                         const sql::stmt::StmtType stmt_type,
                         const ObIArray<uint64_t> &role_id_array);
  int check_ora_ddl_ref_priv(const uint64_t tenant_id,
                             const common::ObString &user_name,
                             const common::ObString &database_name,
                             const common::ObString &table_name,
                             const common::ObIArray<common::ObString>  &column_name_array,
                             const uint64_t obj_type,
                             const sql::stmt::StmtType stmt_type,
                             const ObIArray<uint64_t> &role_id_array);
  int check_ora_grant_obj_priv(const uint64_t tenant_id,
                               const uint64_t user_id,
                               const common::ObString &database_name,
                               const uint64_t obj_id,
                               const uint64_t obj_type,
                               const share::ObRawObjPrivArray &table_priv_array,
                               const ObIArray<uint64_t> &ins_col_ids,
                               const ObIArray<uint64_t> &upd_col_ids,
                               const ObIArray<uint64_t> &ref_col_ids,
                               uint64_t &grantor_id_out,
                               const ObIArray<uint64_t> &role_id_array);
  int check_ora_grant_sys_priv(const uint64_t tenant_id,
                               const uint64_t user_id,
                               const share::ObRawPrivArray &sys_priv_array,
                               const ObIArray<uint64_t> &role_id_array);
  int check_ora_grant_role_priv(const uint64_t tenant_id,
                                const uint64_t user_id,
                                const ObSEArray<uint64_t, 4> &role_granted_id_array,
                                const ObIArray<uint64_t> &role_id_array);
  int check_mysql_grant_role_priv(const ObSqlCtx &sql_ctx,
                                  const common::ObIArray<uint64_t> &granting_role_ids);
  int check_mysql_revoke_role_priv(const ObSqlCtx &sql_ctx,
                                   const common::ObIArray<uint64_t> &granting_role_ids);
  int check_set_default_role_priv(const ObSqlCtx &sql_ctx);
  int set_lbca_op();
  bool is_lbca_op();

  static bool is_ora_priv_check(); 
  static bool enable_mysql_pl_priv_check(int64_t tenant_id, share::schema::ObSchemaGetterGuard &schema_guard);

  // dblink.
  int get_dblink_id(uint64_t tenant_id, const common::ObString &dblink_name, uint64_t &dblink_id);
  int get_dblink_user(const uint64_t tenant_id,
                      const common::ObString &dblink_name,
                      common::ObString &dblink_user,
                      common::ObIAllocator &allocator);
  int get_dblink_schema(uint64_t tenant_id, const common::ObString &dblink_name, const share::schema::ObDbLinkSchema *&dblink_schema);

  // directory
  int get_directory_id(const uint64_t tenant_id,
                       const common::ObString &directory_name,
                       uint64_t &directory_id);
int flatten_udt_attributes(const uint64_t tenant_id,
                           const uint64_t udt_id,
                           ObIAllocator &allocator,
                           ObString &qualified_name,
                           int64_t &schema_version,
                           ObIArray<ObString> &udt_qualified_names);
  int get_udt_attribute_id(const uint64_t udt_id, const ObString &attr_name, uint64_t &attr_id, uint64_t &attr_pos);


  int remove_tmp_cte_schemas(const ObString& cte_table_name);
private:

int construct_udt_qualified_name(const share::schema::ObUDTTypeInfo &udt_info, ObIAllocator &allocator,
                                 const uint64_t tenant_id,
                                 ObString &qualified_name,
                                 ObIArray<ObString> &udt_qualified_names);
  int get_link_table_schema_inner(uint64_t table_id,
                             const share::schema::ObTableSchema *&table_schema) const;
  int get_table_schema_inner(const uint64_t tenant_id, uint64_t table_id,
                             const share::schema::ObTableSchema *&table_schema) const;
  int get_column_schema_inner(const uint64_t tenant_id, uint64_t table_id,
                              const common::ObString &column_name,
                              const share::schema::ObColumnSchemaV2 *&column_schema,
                              bool is_link = false) const;
  int get_column_schema_inner(const uint64_t tenant_id, uint64_t table_id, const uint64_t column_id,
                              const share::schema::ObColumnSchemaV2 *&column_schema,
                              bool is_link = false) const;
private:
  bool is_inited_;
  share::schema::ObSchemaGetterGuard *schema_mgr_;
  ObSqlSchemaGuard *sql_schema_mgr_;
  // cte tmp schema，用于递归的cte服务，生命周期仅在本次查询有效
  common::ObArray<share::schema::ObTableSchema*,
                  common::ModulePageAllocator, true> tmp_cte_schemas_;
  // 记录checker的额外信息，例如安全员的操作等
  int flag_;
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObSchemaChecker);
};
} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_SQL_RESOLVER_SCHEMA_CHECKER2_ */
