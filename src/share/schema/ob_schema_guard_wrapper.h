/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_OCEANBASE_SCHEMA_OB_SCHEMA_GUARD_WRAPPER_H_
#define OB_OCEANBASE_SCHEMA_OB_SCHEMA_GUARD_WRAPPER_H_

#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_latest_schema_guard.h"
namespace oceanbase
{
namespace rootserver
{
class ObDDLService;
class ObDDLSQLTransaction;
} // namespace rootserver

namespace common
{
class ObMySQLProxy;
}
namespace share
{
namespace schema
{

// Wrapper that unifies ObSchemaGetterGuard (local/serial) and ObLatestSchemaGuard (parallel).
// The caller-owned guard must outlive this wrapper (wrapper stores a raw pointer, not a copy).
class ObSchemaGuardWrapper
{
public:
  ObSchemaGuardWrapper(const uint64_t tenant_id,
                       share::schema::ObMultiVersionSchemaService *schema_service);
  ~ObSchemaGuardWrapper();
  int init(ObSchemaGetterGuard &guard);
  int init(ObLatestSchemaGuard &guard);
  int get_local_schema_version(int64_t &schema_version) const;
  int get_foreign_key_id(const uint64_t database_id,
                         const ObString &foreign_key_name,
                         uint64_t &foreign_key_id);
  int get_foreign_key_id(const uint64_t tenant_id,
                         const uint64_t database_id,
                         const ObString &foreign_key_name,
                         uint64_t &foreign_key_id);
  int get_constraint_id(const uint64_t database_id,
                        const ObString &constraint_name,
                        uint64_t &constraint_id);
  int get_constraint_id(const uint64_t tenant_id,
                        const uint64_t database_id,
                        const ObString &constraint_name,
                        uint64_t &constraint_id);
  int get_udt_info(const uint64_t udt_id,
                   const ObUDTTypeInfo *&udt_info);
  int get_mock_fk_parent_table_id(const uint64_t database_id,
                                  const ObString &table_name,
                                  uint64_t &mock_fk_parent_table_id);
  int get_mock_fk_parent_table_schema(const uint64_t mock_fk_parent_table_id,
                                      const ObMockFKParentTableSchema *&mock_fk_parent_table_schema);
  int check_oracle_object_exist(const uint64_t database_id,
                                const uint64_t session_id,
                                const ObString &object_name,
                                const ObSchemaType &schema_type,
                                const ObRoutineType &routine_type,
                                const bool is_or_replace);
  int get_tablespace_schema(const uint64_t tablespace_id,
                            const ObTablespaceSchema *&tablespace_schema);

  int get_table_schema(const uint64_t table_id,
                       const ObTableSchema *&table_schema);
  int get_table_schema(const uint64_t tenant_id,
                       const uint64_t table_id,
                       const ObTableSchema *&table_schema);
  int get_table_schema_in_database_by_name(const uint64_t database_id,
                                           const common::ObString &table_name,
                                           const bool is_index,
                                           const ObTableSchema *&table_schema,
                                           const uint64_t session_id = 0,
                                           const bool is_hidden = false,
                                           const bool is_built_in = false);
  int get_database_id(const common::ObString &database_name,
                      uint64_t &database_id);
  int get_database_schema(const uint64_t database_id,
                          const ObDatabaseSchema *&database_schema);
  int get_database_schema(const uint64_t tenant_id,
                          const uint64_t database_id,
                          const ObDatabaseSchema *&database_schema);
  int get_synonym_id(const uint64_t database_id,
                     const ObString &synonym_name,
                     uint64_t &synonym_id);
  int get_table_id(const uint64_t database_id,
                   const uint64_t session_id,
                   const ObString &table_name,
                   uint64_t &table_id,
                   ObTableType &table_type,
                   int64_t &schema_version);
  int get_tenant_schema(const uint64_t tenant_id,
                        const ObTenantSchema *&tenant_schema);
  int get_tablegroup_id(const common::ObString &tablegroup_name,
                        uint64_t &tablegroup_id);
  int get_tablegroup_schema(const uint64_t tablegroup_id,
                            const ObTablegroupSchema *&tablegroup_schema);
#ifndef GET_OBJ_SCHEMA_VERSIONS
#define GET_OBJ_SCHEMA_VERSIONS(OBJECT_NAME) \
  int get_##OBJECT_NAME##_schema_versions(const common::ObIArray<uint64_t> &obj_ids, \
                                          common::ObIArray<ObSchemaIdVersion> &versions);

  GET_OBJ_SCHEMA_VERSIONS(table);
  GET_OBJ_SCHEMA_VERSIONS(mock_fk_parent_table);
  GET_OBJ_SCHEMA_VERSIONS(routine);
  GET_OBJ_SCHEMA_VERSIONS(synonym);
  GET_OBJ_SCHEMA_VERSIONS(package);
  GET_OBJ_SCHEMA_VERSIONS(type);
  GET_OBJ_SCHEMA_VERSIONS(sequence);
#undef GET_OBJ_SCHEMA_VERSIONS
#endif

int get_obj_privs(const uint64_t obj_id,
                  const ObObjectType obj_type,
                  common::ObIArray<ObObjPriv> &obj_privs);
int get_trigger_info(const uint64_t trigger_id,
                     const ObTriggerInfo *&trigger_info);
int get_trigger_info(const uint64_t tenant_id,
                     const uint64_t trigger_id,
                     const ObTriggerInfo *&trigger_info);
#ifndef GET_RLS_SCHEMA
#define GET_RLS_SCHEMA(SCHEMA, SCHEMA_TYPE) \
  int get_##SCHEMA##s(const uint64_t rls_id, \
                      const SCHEMA_TYPE *&rls_schema);
  GET_RLS_SCHEMA(rls_policy, ObRlsPolicySchema);
  GET_RLS_SCHEMA(rls_group, ObRlsGroupSchema);
  GET_RLS_SCHEMA(rls_context, ObRlsContextSchema);
#undef GET_RLS_SCHEMA
#endif

  int get_default_audit_schemas(common::ObIArray<ObSAuditSchema> &audit_schemas);
  int get_audit_schemas_in_owner(const oceanbase::share::schema::ObSAuditType audit_type,
                                 const uint64_t object_id,
                                 common::ObIArray<ObSAuditSchema> &audit_schemas);
  int get_coded_index_name_info_mysql(common::ObIAllocator &allocator,
                                      const uint64_t database_id,
                                      const uint64_t data_table_id,
                                      const ObString &index_name,
                                      const bool is_built_in,
                                      ObIndexSchemaInfo &index_info);
  int get_sequence_schema(const uint64_t sequence_id,
                          const ObSequenceSchema *&sequence_schema);
  int get_sequence_schema(const uint64_t tenant_id,
                          const uint64_t sequence_id,
                          const ObSequenceSchema *&sequence_schema);
  int check_sequence_exist_with_name(const uint64_t tenant_id,
                                     const uint64_t database_id,
                                     const common::ObString &sequence_name,
                                     bool &exist,
                                     uint64_t &sequence_id,
                                     bool &is_system_generated);

  int get_table_id_and_table_name_in_tablegroup(
      const uint64_t tablegroup_id,
      common::ObIArray<ObString> &table_names,
      common::ObIArray<uint64_t> &table_ids);
  int get_table_schemas_in_tablegroup(
      const uint64_t tablegroup_id,
      common::ObIArray<const ObTableSchema *> &table_schemas);
  int get_primary_table_schema_in_tablegroup(
      const uint64_t tenant_id,
      const uint64_t tablegroup_id,
      const ObTableSchema *&primary_table_schema);
  int check_database_exists_in_tablegroup(
      const uint64_t tablegroup_id,
      bool &exists);
  int get_sensitive_rule_schemas_by_table(
      const ObTableSchema &table_schema,
      ObIArray<const ObSensitiveRuleSchema *> &schemas);
  int get_sys_variable_schema(const ObSysVariableSchema *&sys_var_schema);
  ObSchemaGetterGuard *get_local_schema_guard() { return local_schema_guard_; }
  // Name-based table schema lookup. Uses get_table_id + get_table_schema(id)
  // to dispatch correctly in both local and non-local modes.
  int get_table_schema(const uint64_t database_id,
                       const common::ObString &table_name,
                       const bool is_index,
                       const ObTableSchema *&table_schema,
                       const bool with_hidden_flag = false,
                       const bool is_built_in_index = false);
  int get_table_schema(const uint64_t tenant_id,
                       const uint64_t database_id,
                       const common::ObString &table_name,
                       const bool is_index,
                       const ObTableSchema *&table_schema,
                       const bool with_hidden_flag = false,
                       const bool is_built_in_index = false);
  int get_rls_policy_schema_by_id(const uint64_t rls_policy_id,
                                  const ObRlsPolicySchema *&rls_policy_schema);
  int get_rls_policy_schema_by_id(const uint64_t tenant_id,
                                  const uint64_t rls_policy_id,
                                  const ObRlsPolicySchema *&rls_policy_schema);
  int get_sensitive_rule_schema_by_name(const common::ObString &name,
                                        const ObSensitiveRuleSchema *&schema);
  int get_sensitive_rule_schema_by_name(const uint64_t tenant_id,
                                        const common::ObString &name,
                                        const ObSensitiveRuleSchema *&schema);
  int get_obj_priv_with_obj_id(const uint64_t obj_id,
                               const uint64_t obj_type,
                               common::ObIArray<const ObObjPriv *> &obj_privs,
                               bool reset_flag);
  int get_obj_priv_with_obj_id(const uint64_t tenant_id,
                               const uint64_t obj_id,
                               const uint64_t obj_type,
                               common::ObIArray<const ObObjPriv *> &obj_privs,
                               bool reset_flag);
  int get_audit_schema_in_owner(const ObSAuditType audit_type,
                                const uint64_t owner_id,
                                common::ObIArray<const ObSAuditSchema *> &audit_schemas);
  int get_audit_schema_in_owner(const uint64_t tenant_id,
                                const ObSAuditType audit_type,
                                const uint64_t owner_id,
                                common::ObIArray<const ObSAuditSchema *> &audit_schemas);
  uint64_t get_session_id() const { return OB_ISNULL(local_schema_guard_) ? OB_INVALID_ID : local_schema_guard_->get_session_id(); }
  void set_session_id(const uint64_t session_id) { if (OB_NOT_NULL(local_schema_guard_)) { local_schema_guard_->set_session_id(session_id); } }
  ObLatestSchemaGuard* get_latest_schema_guard() { return latest_schema_guard_; }
  // NOTE: get_local_schema_guard() was intentionally removed. In parallel DDL
  // mode (is_local_guard_ == false), local_schema_guard_ is never initialized,
  // so exposing it as a raw reference would hand callers an empty guard. All
  // schema access in parallel mode must go through the typed wrapper methods
  // (which dispatch to latest_schema_guard_) or the serial path must be used.
private:
  int check_inner_stat_() const;
private:
  const uint64_t tenant_id_;
  ObMultiVersionSchemaService *schema_service_;
  ObSchemaGetterGuard *local_schema_guard_;
  ObLatestSchemaGuard *latest_schema_guard_;
  bool is_inited_;
};

} //end of namespace schema
} //end of namespace share
} //end of namespace oceanbase
#endif //OB_OCEANBASE_SCHEMA_OB_SCHEMA_GUARD_WRAPPER_H_
