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

#ifndef OB_OCEANBASE_SCHEMA_OB_SCHEMA_GETTER_GUARD_H_
#define OB_OCEANBASE_SCHEMA_OB_SCHEMA_GETTER_GUARD_H_
#include <stdint.h>
#include "share/ob_define.h"
#include "lib/container/ob_se_array.h"
#include "lib/allocator/ob_mod_define.h"
#include "lib/allocator/page_arena.h"
#include "share/schema/ob_schema_mgr_cache.h"
#include "share/schema/ob_package_info.h"
#include "share/schema/ob_routine_info.h"
#include "share/schema/ob_udt_info.h"
#include "share/schema/ob_outline_mgr.h"
#include "share/schema/ob_udt_mgr.h"

namespace oceanbase
{
namespace lib
{
class Worker;
}
namespace common
{
class ObString;
class ObKVCacheHandle;
template <class T>
	class ObIArray;
}
namespace sql {
class ObSQLSessionInfo;
}
namespace share
{
namespace schema
{
class IdVersion;
class ObColumnSchemaV2;
class ObDBPriv;
class ObDatabaseSchema;
class ObMultiVersionSchemaService;
class ObPrivMgr;
class ObSimpleDatabaseSchema;
class ObSimplePackageSchema;
class ObSimpleRoutineSchema;
class ObSimpleSynonymSchema;
class ObSimpleSysVariableSchema;
class ObSimpleTablegroupSchema;
class ObSimpleTenantSchema;
class ObSimpleTriggerSchema;
class ObSimpleMockFKParentTableSchema;
class ObMockFKParentTableSchema;
class ObTablegroupSchema;
class ObTablePriv;
class ObTableSchema;
class ObTenantSchema;
class ObTriggerInfo;
class ObUDTTypeInfo;
class ObUDF;
class ObUserInfo;
class SchemaName;
struct ObNeedPriv;
struct ObSessionPrivInfo;
struct ObStmtNeedPrivs;
struct ObUserLoginInfo;


class ObSchemaMgrInfo
{
public:
  ObSchemaMgrInfo()
			: tenant_id_(common::OB_INVALID_TENANT_ID),
        snapshot_version_(common::OB_INVALID_VERSION),
        schema_mgr_(NULL),
        mgr_handle_(),
        schema_status_()
	{}
  ObSchemaMgrInfo(
        const uint64_t tenant_id,
				const int64_t snapshot_version,
				const ObSchemaMgr *&schema_mgr,
				const ObSchemaMgrHandle &mgr_handle,
				const ObRefreshSchemaStatus &schema_status)
			: tenant_id_(tenant_id),
			snapshot_version_(snapshot_version),
			schema_mgr_(schema_mgr),
			mgr_handle_(mgr_handle)
	{ schema_status_ = schema_status; }
	ObSchemaMgrInfo &operator=(const ObSchemaMgrInfo &other);
	explicit ObSchemaMgrInfo(const ObSchemaMgrInfo &other);
	virtual ~ObSchemaMgrInfo();
	uint64_t get_tenant_id() const { return tenant_id_; }
	int64_t get_snapshot_version() const { return snapshot_version_; }
	void set_schema_mgr(const ObSchemaMgr* schema_mgr) { schema_mgr_ = schema_mgr; }
	const ObSchemaMgr *get_schema_mgr() const { return schema_mgr_; }
	ObRefreshSchemaStatus get_schema_status() const { return schema_status_; }
	ObSchemaMgrHandle& get_schema_mgr_handle() { return mgr_handle_; }
	void reset();
	TO_STRING_KV(K_(tenant_id), K_(snapshot_version), KP_(schema_mgr), K_(schema_status));
private:
  uint64_t tenant_id_;
	int64_t snapshot_version_;
	const ObSchemaMgr *schema_mgr_;
	ObSchemaMgrHandle mgr_handle_;
	ObRefreshSchemaStatus schema_status_;
};

class ObSchemaGetterGuard
{
friend class ObMultiVersionSchemaService;
friend class MockSchemaService;
const static int DEFAULT_RESERVE_SIZE = 2;
typedef common::ObSEArray<SchemaObj, DEFAULT_RESERVE_SIZE> SchemaObjs;
typedef common::ObSEArray<ObSchemaMgrInfo, DEFAULT_RESERVE_SIZE> SchemaMgrInfos;

public:

	enum CheckTableType
	{
		ALL_NON_HIDDEN_TYPES = 0,
		TEMP_TABLE_TYPE = 1,
		NON_TEMP_WITH_NON_HIDDEN_TABLE_TYPE = 2,
    USER_HIDDEN_TABLE_TYPE = 3,
	};

	enum SchemaGuardType
	{
		INVALID_SCHEMA_GUARD_TYPE = 0,
		SCHEMA_GUARD = 1,
		TENANT_SCHEMA_GUARD = 2,
		TABLE_SCHEMA_GUARD = 3
	};

	ObSchemaGetterGuard();
  explicit ObSchemaGetterGuard(const ObSchemaMgrItem::Mod mod);
	virtual ~ObSchemaGetterGuard();
  int reset();
	void dump();
	OB_INLINE bool is_inited() const { return is_inited_; }

	int get_schema_version(const uint64_t tenant_id, int64_t &schema_version) const;

	/*
   * with_mv: if index_tid_array contains ematerialized view.
   * with_global_index: if index_tid_array contains global index.
   * with_domain_index: if index_tid_array contains domain index.
   */
	int get_can_read_index_array(
      const uint64_t tenant_id,
      const uint64_t table_id,
      uint64_t *index_tid_array,
      int64_t &size,
      bool with_mv,
      bool with_global_index = true,
      bool with_domain_index = true,
      bool with_spatial_index = true);
  int get_table_mlog_schema(const uint64_t tenant_id,
                            const uint64_t data_table_id,
                            const ObTableSchema *&mlog_schema);
  int check_has_local_unique_index(
      const uint64_t tenant_id,
      const uint64_t table_id,
      bool &has_local_unique_index);
  int get_all_unique_index(const uint64_t tenant_id,
                           const uint64_t table_id,
                           ObIArray<uint64_t> &unique_index_ids);
	bool is_tenant_schema_valid(const int64_t tenant_id) const;
	/*
   interface for simple schema
   */
	int get_simple_table_schema(
      const uint64_t tenant_id,
      const uint64_t table_id,
			const ObSimpleTableSchemaV2 *&table_schema);
	int get_simple_table_schema(
      const uint64_t tenant_id,
			const uint64_t database_id,
			const ObString &table_name,
			const bool is_index,
			const ObSimpleTableSchemaV2 *&simple_table_schema,
      const bool with_hidden_flag = false,
      const bool is_built_in_index = false);
	int get_table_schemas_in_tenant(const uint64_t tenant_id,
			common::ObIArray<const ObSimpleTableSchemaV2 *> &table_schemas);
  int get_database_schemas_in_tenant(const uint64_t tenant_id,
      common::ObIArray<const ObSimpleDatabaseSchema *> &database_schemas);
	int get_tablegroup_schemas_in_tenant(const uint64_t tenant_id,
			common::ObIArray<const ObTablegroupSchema*> &tablegroup_schemas);

	int get_user_schemas_in_tenant(const uint64_t tenant_id,
			common::ObIArray<const ObUserInfo *> &user_schemas);
	int get_database_schemas_in_tenant(const uint64_t tenant_id,
			common::ObIArray<const  ObDatabaseSchema *> &database_schemas);
	int get_tablegroup_schemas_in_tenant(const uint64_t tenant_id,
			common::ObIArray<const ObSimpleTablegroupSchema*> &tablegroup_schemas);
	int get_table_schemas_in_tenant(const uint64_t tenant_id,
			common::ObIArray<const ObTableSchema *> &table_schemas);
  int get_view_schemas_in_tenant(const uint64_t tenant_id,
      common::ObIArray<const ObTableSchema *> &table_schemas);
	int get_outline_infos_in_tenant(const uint64_t tenant_id,
			common::ObIArray<const ObOutlineInfo *> &outline_infos);
	int get_package_infos_in_tenant(const uint64_t tenant_id,
			common::ObIArray<const ObPackageInfo *> &package_infos);
	int get_routine_infos_in_tenant(const uint64_t tenant_id,
			common::ObIArray<const ObRoutineInfo *> &routine_infos);
	int get_trigger_infos_in_tenant(const uint64_t tenant_id,
			common::ObIArray<const ObTriggerInfo *> &triger_infos);
	int get_udt_infos_in_tenant(const uint64_t tenant_id,
			common::ObIArray<const ObUDTTypeInfo *> &udt_info);

  int get_audit_schemas_in_tenant(const uint64_t tenant_id,
                                  common::ObIArray<const ObSAuditSchema *> &security_audits);
  int get_audit_schema_in_tenant(const uint64_t tenant_id,
                                 const ObSAuditType audit_type,
                                 const uint64_t owner_id,
                                 const ObSAuditOperationType operation_type,
                                 const ObSAuditSchema *&audit_schema) const;
  int get_audit_schema_in_owner(const uint64_t tenant_id,
                                const ObSAuditType audit_type,
                                const uint64_t object_id,
                                common::ObIArray<const ObSAuditSchema *> &audit_schemas) const;
  int check_allow_audit(const uint64_t tenant_id,
                        ObSAuditType &audit_type,
                        const uint64_t owner_id,
                        ObSAuditOperationType &operation_type,
                        const int return_code,
                        uint64_t &audit_id,
                        bool &is_allow_audit);
  int check_allow_audit_by_default(const uint64_t tenant_id,
                                   ObSAuditType &audit_type,
                                   ObSAuditOperationType &operation_type,
                                   const int return_code,
                                   uint64_t &audit_id,
                                   bool &is_allow_audit);

  int get_synonym_infos_in_tenant(const uint64_t tenant_id,
                              common::ObIArray<const ObSynonymInfo *> &synonym_info);
  int get_table_schemas_in_database(const uint64_t tenant_id,
                                    const uint64_t database_id,
                                    common::ObIArray<const ObTableSchema *> &table_schemas);
  int get_table_schemas_in_database(const uint64_t tenant_id,
                                    const uint64_t database_id,
                                    common::ObIArray<const ObSimpleTableSchemaV2 *> &table_schemas);
  int get_table_schemas_in_tablegroup(const uint64_t tenant_id,
                                      const uint64_t tablegroup_id,
                                      common::ObIArray<const ObTableSchema *> &table_schemas);
  int get_table_schemas_in_tablegroup(const uint64_t tenant_id,
                                      const uint64_t tablegroup_id,
                                      common::ObIArray<const ObSimpleTableSchemaV2 *> &table_schemas);
  int get_primary_table_schema_in_tablegroup(const uint64_t tenant_id,
                                             const uint64_t tablegroup_id,
                                             const ObSimpleTableSchemaV2 *&primary_table_schema);

  int get_table_schemas_in_tablespace(const uint64_t tenant_id,
                                      const uint64_t tablespace_id,
                                      common::ObIArray<const ObTableSchema *> &table_schemas);

  int get_simple_tenant_schemas(common::ObIArray<const ObSimpleTenantSchema *> &tenant_schemas) const;

  int get_tenant_ids(common::ObIArray<uint64_t> &tenant_ids) const;
  int get_available_tenant_ids(common::ObIArray<uint64_t> &tenant_ids) const;
  int get_tablegroup_ids_in_tenant(const uint64_t tenant_id,
                                   common::ObIArray<uint64_t> &tablegroup_id_array);
  int get_routine_ids_in_tenant(const uint64_t tenant_id,
                                common::ObIArray<uint64_t> &routine_id_array,
                                bool is_agent_mode = false);
  int get_package_ids_in_tenant(const uint64_t tenant_id,
                                common::ObIArray<uint64_t> &package_id_array);
  int get_udt_ids_in_tenant(const uint64_t tenant_id,
                                common::ObIArray<uint64_t> &udt_id_array);
  int get_table_ids_in_tenant(const uint64_t tenant_id,
                              common::ObIArray<uint64_t> &table_ids);
  int get_table_ids_in_database(const uint64_t tenant_id,
                                const uint64_t dataspace_id,
                                common::ObIArray<uint64_t> &table_id_array);
  int get_table_ids_in_tablegroup(const uint64_t tenant_id,
                                  const uint64_t tablegroup_id,
                                  common::ObIArray<uint64_t> &table_id_array);
  int get_trigger_ids_in_database(const uint64_t tenant_id,
                                  const uint64_t database_id,
                                  common::ObIArray<uint64_t> &trigger_ids);
  int get_routine_ids_in_database(const uint64_t tenant_id,
                                  const uint64_t database_id,
                                  common::ObIArray<uint64_t> &routine_ids);
  int get_udt_ids_in_database(const uint64_t tenant_id,
                              const uint64_t database_id,
                              common::ObIArray<uint64_t> &udt_ids);
  int get_routine_info_in_udt(const uint64_t tenant_id,
                              const uint64_t udt_id,
                              const uint64_t subprogram_id,
                              const ObRoutineInfo *&routine_info);
  int get_routine_infos_in_udt(const uint64_t tenant_id,
                               const uint64_t udt_id,
                               common::ObIArray<const ObRoutineInfo *> &routine_infos);
  int get_routine_info_in_package(const uint64_t tenant_id,
                                  const uint64_t package_id,
                                  const uint64_t subprogram_id,
                                  const ObRoutineInfo *&routine_info);
  int get_routine_infos_in_package(const uint64_t tenant_id,
                                   const uint64_t package_id,
                                   common::ObIArray<const ObRoutineInfo *> &routine_infos);
  int get_sequence_schemas_in_database(const uint64_t tenant_id,
                                       const uint64_t database_id,
                                       common::ObIArray<const ObSequenceSchema*> &sequence_schemas);
  int get_label_se_policy_infos_in_tenant(const uint64_t tenant_id,
                                          common::ObIArray<const ObLabelSePolicySchema *> &label_se_policy_infos);
  int get_label_se_component_infos_in_tenant(const uint64_t tenant_id,
                                             common::ObIArray<const ObLabelSeComponentSchema *> &label_se_component_infos);
  int get_label_se_label_infos_in_tenant(const uint64_t tenant_id,
                                         common::ObIArray<const ObLabelSeLabelSchema *> &label_se_label_infos);
  int get_label_se_user_level_infos_in_tenant(const uint64_t tenant_id,
                                              common::ObIArray<const ObLabelSeUserLevelSchema *> &label_se_user_level_infos);
  int get_profile_infos_in_tenant(const uint64_t tenant_id,
                                  common::ObIArray<const ObProfileSchema *> &profile_infos);

  // generate tablet-table map by specified tenant_id
  // @notice:
  // - schema_guard should be tenant schema guard, which is not lazy and is formal.
  //   (Get from ObMultiVersionSchemaService::get_tenant_schema_guard() without specified schema_version)
  // @param[in]:
  // - tenant_id: tenant id
  // @param[out]:
  // - tablet_map: pairs of tablet-table. map will be created by this function.
  int generate_tablet_table_map(
      const uint64_t tenant_id,
      common::hash::ObHashMap<common::ObTabletID, uint64_t> &tablet_map);
  /*
     get_id
  */
  int get_tenant_id(const common::ObString &tenant_name,
                    uint64_t &tenant_id);
  int get_user_id(uint64_t tenant_id,
                  const common::ObString &user_name,
                  const common::ObString &host_name,
                  uint64_t &user_id,
                  const bool is_role = false);
  int get_database_id(uint64_t tenant_id,
                      const common::ObString &database_name,
                      uint64_t &database_id);
  int get_tablegroup_id(uint64_t tenant_id,
                        const common::ObString &tablegroup_name,
                        uint64_t &tablegroup_id);
  int get_table_id(uint64_t tenant_id,
                   uint64_t database_id,
                   const common::ObString &table_name,
                   const bool is_index,
                   const CheckTableType check_type,  // if temporary table is visable
                   uint64_t &table_id,
                   const bool is_built_in_index = false);
  int get_table_id(uint64_t tenant_id,
                   const common::ObString &database_name,
                   const common::ObString &table_name,
                   const bool is_index,
                   const CheckTableType check_type,  // if temporary table is visable
                   uint64_t &table_id,
                   const bool is_built_in_index = false);
  int get_foreign_key_id(const uint64_t tenant_id,
                         const uint64_t database_id,
                         const common::ObString &foreign_key_name,
                         uint64_t &foreign_key_id);
  int get_foreign_key_info(const uint64_t tenant_id,
                          const uint64_t database_id,
                          const common::ObString &foreign_key_name,
                          ObSimpleForeignKeyInfo &foreign_key_info);
  int get_constraint_id(const uint64_t tenant_id,
                        const uint64_t database_id,
                        const common::ObString &constraint_name,
                        uint64_t &constraint_id);
  int get_constraint_info(const uint64_t tenant_id,
                          const uint64_t database_id,
                          const common::ObString &constraint_name,
                          ObSimpleConstraintInfo &constraint_info) const;
  int get_tenant_name_case_mode(const uint64_t tenant_id, common::ObNameCaseMode &mode);
  int get_tenant_compat_mode(const uint64_t tenant_id, lib::Worker::CompatMode &compat_mode);
  int get_tenant_read_only(const uint64_t tenant_id, bool &read_only);
  /*
     get_schema
  */
  // basic interface
  int get_tenant_info(uint64_t tenant_id,
                      const ObTenantSchema *&tenant_info);
  int get_tenant_info(uint64_t tenant_id,
                      const ObSimpleTenantSchema *&tenant_info);
  int get_database_schema(const uint64_t tenant_id,
                          const uint64_t database_id,
                          const ObDatabaseSchema *&database_schema);
  int get_database_schema(const uint64_t tenant_id,
                          const uint64_t database_id,
                          const ObSimpleDatabaseSchema *&database_schema);
  int get_database_schema(const uint64_t tenant_id,
                          const common::ObString &database_name,
                          const ObDatabaseSchema *&database_schema);
  int get_tablegroup_schema(const uint64_t tenant_id,
                            const uint64_t tablegroup_id,
                            const ObTablegroupSchema *&tablegourp_schema);
  int get_tablegroup_schema(const uint64_t tenant_id,
                            const uint64_t tablegroup_id,
                            const ObSimpleTablegroupSchema *&tablegroup_schema);
  int get_table_schema(const uint64_t tenant_id,
                       const uint64_t table_id,
                       const ObTableSchema *&table_schema);
  int get_table_schema(const uint64_t tenant_id,
                       const uint64_t database_id,
                       const common::ObString &table_name,
                       const bool is_index,
                       const ObTableSchema *&table_schema,
                       const bool with_hidden_flag = false,
                       const bool is_built_in_index = false);
  int get_table_schema(const uint64_t tenant_id,
                       const common::ObString &database_name,
                       const common::ObString &table_name,
                       const bool is_index,
                       const ObTableSchema *&table_schema,
                       const bool with_hidden_flag = false,
                       const bool is_built_in_index = false);
  int get_index_schemas_with_data_table_id(const uint64_t tenant_id,
                                           const uint64_t data_table_id,
                                           ObIArray<const ObSimpleTableSchemaV2 *> &aux_schemas);
  int get_sys_variable_schema(const common::ObString &tenant_name,
                              const ObSysVariableSchema *&sys_variable_schema);
  int get_sys_variable_schema(const uint64_t tenant_id,
                              const ObSysVariableSchema *&sys_variable_schema);
  int get_sys_variable_schema(const uint64_t tenant_id,
                              const ObSimpleSysVariableSchema *&sys_variable_schema);
  int get_tenant_system_variable(uint64_t tenant_id,
                                 const common::ObString &var_name,
                                 const ObSysVarSchema *&var_schema);
  int get_tenant_system_variable(uint64_t tenant_id,
                                 ObSysVarClassType var_id,
                                 const ObSysVarSchema *&var_schema);
  int get_tenant_info(const common::ObString &tenant_name,
                      const ObTenantSchema *&tenant_schema);
  int get_user_info(const uint64_t tenant_id,
                    const uint64_t user_id,
                    const ObUserInfo *&user_info);
  int get_user_info(const uint64_t tenant_id,
                    const common::ObString &user_name,
                    const common::ObString &host_name,
                    const ObUserInfo *&user_info);
  int get_user_info(const uint64_t tenant_id,
                    const common::ObString &user_name,
                    common::ObIArray<const ObUserInfo *> &users_info);
  int get_column_schema(const uint64_t tenant_id,
                        const uint64_t table_id,
                        const uint64_t column_id,
                        const ObColumnSchemaV2 *&column_schema);
  int get_column_schema(const uint64_t tenant_id,
                        const uint64_t table_id,
                        const common::ObString &column_name,
                        const ObColumnSchemaV2 *&column_schema);

  // for resolver
  int get_can_write_index_array(const uint64_t tenant_id,
                                const uint64_t table_id,
                                uint64_t *index_tid_array,
                                int64_t &size,
                                bool only_global = false,
                                bool with_mlog = false);

  // for readonly
  int verify_read_only(const uint64_t tenant_id, const ObStmtNeedPrivs &stmt_need_privs);
  int check_user_access(const ObUserLoginInfo &login_info,
                        ObSessionPrivInfo &s_priv,
                        SSL *ssl_st,
                        const ObUserInfo *&sel_user_info);
  int check_db_access(ObSessionPrivInfo &s_priv,
                      const common::ObString& database_name);
  int check_db_show(const ObSessionPrivInfo &session_priv,
                    const common::ObString &db,
                    bool &allow_show);
  int check_table_show(const ObSessionPrivInfo &session_priv,
                       const common::ObString &db,
                       const common::ObString &table,
                       bool &allow_show);

  int check_ora_priv(const uint64_t tenant_id,
                     const uint64_t uid,
                     const ObStmtOraNeedPrivs &stmt_need_privs,
                     const common::ObIArray<uint64_t> &role_id_array);
  int check_priv(const ObSessionPrivInfo &session_priv,
                 const ObStmtNeedPrivs &stmt_need_privs);
  int check_priv_or(const ObSessionPrivInfo &session_priv,
                    const ObStmtNeedPrivs &stmt_need_privs);
  int check_db_access(const ObSessionPrivInfo &session_priv,
                      const common::ObString &db,
                      ObPrivSet &db_priv_set,
                      bool print_warn = true);
  int check_single_table_priv(const ObSessionPrivInfo &session_priv,
                              const ObNeedPriv &table_need_priv);
  int check_single_table_priv_or(const ObSessionPrivInfo &session_priv,
                                 const ObNeedPriv &table_need_priv);

  int check_priv_any_column_priv(const ObSessionPrivInfo &session_priv,
                                 const common::ObString &db_name,
                                 const common::ObString &table_name,
                                 bool &pass);

  int collect_all_priv_for_column(const ObSessionPrivInfo &session_priv,
                                  const common::ObString &db_name,
                                  const common::ObString &table_name,
                                  const common::ObString &column_name,
                                  ObPrivSet &column_priv_set);

  int get_session_priv_info(const uint64_t tenant_id,
                            const uint64_t user_id,
                            const ObString &database_name,
                            ObSessionPrivInfo &session_priv);
  int get_user_infos_with_tenant_id(const uint64_t tenant_id,
                                    common::ObIArray<const ObUserInfo *> &user_infos);
  int get_db_priv_with_tenant_id(const uint64_t tenant_id,
                                 common::ObIArray<const ObDBPriv *> &db_privs);
  int get_column_priv_in_table(const uint64_t tenant_id,
                              const uint64_t user_id,
                              const ObString &db,
                              const ObString &table,
                              ObIArray<const ObColumnPriv *> &column_privs);

  int get_column_priv_in_table(const ObTablePrivSortKey &table_priv_key,
                              ObIArray<const ObColumnPriv *> &column_privs);
  int get_column_priv(const ObColumnPrivSortKey &column_priv_key,
                          const ObColumnPriv *&column_priv);

  int get_column_priv_id(const uint64_t tenant_id,
                        const uint64_t user_id,
                        const ObString &db,
                        const ObString &table,
                        const ObString &column,
                        uint64_t &priv_id);
  int get_column_priv_with_user_id(const uint64_t tenant_id,
                                    const uint64_t user_id,
                                    common::ObIArray<const ObColumnPriv*> &column_privs);
  int get_column_priv_set(const ObColumnPrivSortKey &column_priv_key, ObPrivSet &priv_set);
  int get_db_priv_with_user_id(const uint64_t tenant_id,
                               const uint64_t user_id,
                               common::ObIArray<const ObDBPriv*> &db_privs);

  int get_routine_priv_with_user_id(const uint64_t tenant_id,
                                    const uint64_t user_id,
                                    common::ObIArray<const ObRoutinePriv*> &routine_privs);
  int get_table_priv_with_tenant_id(const uint64_t tenant_id,
                                    common::ObIArray<const ObTablePriv *> &table_privs);
  int get_table_priv_with_user_id(const uint64_t tenant_id,
                                  const uint64_t user_id,
                                  common::ObIArray<const ObTablePriv *> &table_privs);
  int get_obj_priv_with_grantee_id(const uint64_t tenant_id,
                                   const uint64_t grnatee_id,
                                   common::ObIArray<const ObObjPriv *> &obj_privs);
  int get_obj_priv_with_grantor_id(const uint64_t tenant_id,
                                   const uint64_t grantor_id,
                                   common::ObIArray<const ObObjPriv *> &obj_privs,
                                   bool reset_flag);
  int get_obj_priv_with_obj_id(const uint64_t tenant_id,
                               const uint64_t obj_id,
                               const uint64_t obj_type,
                               common::ObIArray<const ObObjPriv *> &obj_privs,
                               bool reset_flag);
  int get_obj_privs_in_ur_and_obj(const uint64_t tenant_id,
                                  const ObObjPrivSortKey &obj_key,
                                  ObPackedObjPriv &obj_privs);
  int get_obj_privs_in_grantor_ur_obj_id(const uint64_t tenant_id,
                                         const ObObjPrivSortKey &obj_key,
                                         common::ObIArray<const ObObjPriv *> &obj_privs);
  int get_obj_privs_in_grantor_obj_id(const uint64_t tenant_id,
                                         const ObObjPrivSortKey &obj_key,
                                         common::ObIArray<const ObObjPriv *> &obj_privs);
  int get_db_priv_set(const uint64_t tenant_id,
                      const uint64_t user_id,
                      const common::ObString &db,
                      ObPrivSet &priv_set);
  // for compatible
  int get_db_priv_set(const ObOriginalDBKey &db_priv_key, ObPrivSet &priv_set, bool is_pattern = false);
  int get_table_priv_set(const ObTablePrivSortKey &table_priv_key, ObPrivSet &priv_set);
  int get_routine_priv_set(const ObRoutinePrivSortKey &routine_priv_key, ObPrivSet &priv_set);
  int get_obj_privs(
      const ObObjPrivSortKey &obj_priv_key,
      ObPackedObjPriv &obj_privs);
  //TODO@xiyu: ObDDLOperator::drop_tablegroup
  int check_database_exists_in_tablegroup(
      const uint64_t tenant_id,
      const uint64_t tablegroup_id,
      bool &not_empty);

  // xiyu: just return pointer to save my life.
  const ObUserInfo *get_user_info(const uint64_t tenant_id, const uint64_t user_id);
  const ObTablegroupSchema *get_tablegroup_schema(const uint64_t tenant_id, const uint64_t tablegroup_id);
  const ObColumnSchemaV2 *get_column_schema(const uint64_t tenant_id,
                                            const uint64_t table_id,
                                            const uint64_t column_id);
  const ObTenantSchema *get_tenant_info(const common::ObString &tenant_name);

  // nijia.nj: check exist, for root_service/ddl_service/ddl_operator
  //
  int check_database_exist(const uint64_t tenant_id,
                           const common::ObString &database_name,
                           bool &is_exist,
                           uint64_t *database_id = NULL);
  int check_database_in_recyclebin(const uint64_t tenant_id,
                                   const uint64_t database_id,
                                   bool &in_recyclebin);
  int check_database_exist(const uint64_t tenant_id,
                           const uint64_t database_id,
                           bool &is_exist);
  int check_tablegroup_exist(const uint64_t tenant_id,
                             const common::ObString &tablegroup_name,
                             bool &is_exist,
                             uint64_t *tablegroup_id = NULL);
  int check_tablegroup_exist(const uint64_t tenant_id,
                             const uint64_t tablegroup_id,
                             bool &is_exist);
  int check_oracle_object_exist(const uint64_t tenant_id, const uint64_t db_id,
      const common::ObString &object_name, const ObSchemaType &schema_type,
      const ObRoutineType &routine_type, const bool is_or_replace,
      common::ObIArray<ObSchemaType> &conflict_schema_types);
  int check_table_exist(const uint64_t tenant_id,
                        const uint64_t database_id,
                        const common::ObString &table_name,
                        const bool is_index,
                        const CheckTableType check_type, // if temporary table is visable
                        bool &is_exist,
                        uint64_t *table_id = NULL);
  int check_table_exist(const uint64_t tenant_id,
                        const uint64_t table_id,
                        bool &is_exist);
  int check_tenant_exist(const uint64_t tenant_id,
                         bool &is_exist);
  int check_outline_exist_with_name(const uint64_t tenant_id,
                                    const uint64_t database_id,
                                    const common::ObString &outline_name,
                                    uint64_t &outline_id,
                                    bool &exist);
  int check_outline_exist_with_sql(const uint64_t tenant_id,
                                   const uint64_t database_id,
                                   const common::ObString &paramlized_sql,
                                   bool &exist);
  int check_outline_exist_with_sql_id(const uint64_t tenant_id,
                                   const uint64_t database_id,
                                   const common::ObString &sql_id,
                                   bool &exist) ;
  int get_outline_info_with_name(const uint64_t tenant_id,
                                 const uint64_t database_id,
                                 const common::ObString &name,
                                 const ObOutlineInfo *&outline_info);
  int get_outline_info_with_name(const uint64_t tenant_id,
                                 const common::ObString &db_name,
                                 const common::ObString &outline_name,
                                 const ObOutlineInfo *&outline_info);
  int get_outline_info_with_signature(const uint64_t tenant_id,
                                      const uint64_t database_id,
                                      const common::ObString &signature,
                                      const ObOutlineInfo *&outline_info);
  //package
  int check_package_exist(uint64_t tenant_id, uint64_t database_id,
                          const common::ObString &package_name,
                          ObPackageType package_type, int64_t compatible_mode, bool &exist) ;
  int get_package_id(uint64_t tenant_id, uint64_t database_id, const common::ObString &package_name,
                     ObPackageType package_type, int64_t compatible_mode, uint64_t &package_id) ;
  int get_package_info(const uint64_t tenant_id,
                       const uint64_t database_id,
                       const common::ObString &package_name,
                       ObPackageType package_type,
                       int64_t compatible_mode,
                       const ObPackageInfo *&package_info) ;
  int get_package_info(const uint64_t tenant_id,
                       const uint64_t package_id,
                       const ObPackageInfo *&package_info);
  int get_simple_package_info(const uint64_t tenant_id,
                              const uint64_t package_id,
                              const ObSimplePackageSchema *&package_info);
  int get_package_routine_infos(uint64_t tenant_id, uint64_t database_id, uint64_t package_id,
                                const common::ObString &routine_name, ObRoutineType routine_type,
                                common::ObIArray<const ObIRoutineInfo *> &routine_infos,
   share::schema::ObRoutineType inside_routine_type = share::schema::ObRoutineType::ROUTINE_PACKAGE_TYPE);

  int get_trigger_info(const uint64_t tenant_id,
                       const uint64_t trigger_id,
                       const ObTriggerInfo *&trigger_info);
  int get_trigger_info(const uint64_t tenant_id,
                       const uint64_t database_id,
                       const common::ObString &trigger_name,
                       const ObTriggerInfo *&trigger_info);
  int get_package_info_from_trigger(const uint64_t tenant_id,
                                    const uint64_t package_id,
                                    const ObPackageInfo *&package_spec_info,
                                    const ObPackageInfo *&package_body_info);
  int get_package_id_from_trigger(uint64_t tenant_id,
                                  uint64_t database_id,
                                  const common::ObString &pacakge_name,
                                  ObPackageType package_type,
                                  uint64_t &package_id);
  //procedure
  inline int check_standalone_procedure_exist(uint64_t tenant_id, uint64_t database_id,
                                              const common::ObString &procedure_name, bool &exist) const
  {
    return check_routine_exist(tenant_id, database_id, common::OB_INVALID_ID, procedure_name,
                               0, ROUTINE_PROCEDURE_TYPE, exist);
  }
  inline int get_standalone_procedure_id(uint64_t tenant_id, uint64_t database_id, const common::ObString &procedure_name,
                                         uint64_t &procedure_id)
  {
    return get_routine_id(tenant_id, database_id, common::OB_INVALID_ID, procedure_name,
                          0, ROUTINE_PROCEDURE_TYPE, procedure_id);
  }
  inline int get_standalone_procedure_info(uint64_t tenant_id, uint64_t database_id, const common::ObString &procedure_name,
                                           const ObRoutineInfo *&procedure_info)
  {
    return get_routine_info(tenant_id, database_id, common::OB_INVALID_ID, procedure_name,
                            0, ROUTINE_PROCEDURE_TYPE, procedure_info);
  }
  //function
  inline int check_standalone_function_exist(uint64_t tenant_id, uint64_t database_id,
                                              const common::ObString &function_name, bool &exist) const
  {
    return check_routine_exist(tenant_id, database_id, common::OB_INVALID_ID, function_name,
                               0, ROUTINE_FUNCTION_TYPE, exist);
  }
  inline int get_standalone_function_id(uint64_t tenant_id, uint64_t database_id, const common::ObString &function_name,
                                         uint64_t &function_id)
  {
    return get_routine_id(tenant_id, database_id, common::OB_INVALID_ID, function_name,
                          0, ROUTINE_FUNCTION_TYPE, function_id);
  }
  inline int get_standalone_function_info(uint64_t tenant_id, uint64_t database_id, const common::ObString &function_name,
                                          const ObRoutineInfo *&function_info)
  {
    return get_routine_info(tenant_id, database_id, common::OB_INVALID_ID, function_name,
                            0, ROUTINE_FUNCTION_TYPE, function_info);
  }
  //routine
  int get_routine_info(const uint64_t tenant_id,
                       uint64_t routine_id,
                       const ObRoutineInfo *&routine_info);

  // udt
  int get_udt_info(const uint64_t tenant_id, const uint64_t udt_id, const ObUDTTypeInfo*& udt_info);
  int get_udt_routine_infos(uint64_t tenant_id, uint64_t database_id, uint64_t udt_id,
                            const common::ObString &routine_name, ObRoutineType routine_type,
                            common::ObIArray<const ObIRoutineInfo *> &routine_infos,
  share::schema::ObRoutineType inside_routine_type = share::schema::ObRoutineType::ROUTINE_UDT_TYPE);

  int get_synonym_info(const uint64_t tenant_id,
                       const uint64_t database_id,
                       const common::ObString &name,
                       const ObSynonymInfo *&synonym_info);
  int get_synonym_info(const uint64_t tenant_id,
                       const uint64_t database_id,
                       const common::ObString &name,
                       const ObSimpleSynonymSchema *&synonym_info);
  int get_simple_synonym_info(const uint64_t tenant_id,
                              const uint64_t synonym_id,
                              const ObSimpleSynonymSchema *&synonym_info);
  int check_synonym_exist_with_name(const uint64_t tenant_id,
                                    const uint64_t database_id,
                                    const common::ObString &synonym_name,
                                    bool &exist,
                                    uint64_t &synonym_id);
  int get_object_with_synonym(const uint64_t tenant_id,
                              const uint64_t syn_database_id,
                              const common::ObString &syn_name,
                              uint64_t &obj_database_id,
                              uint64_t &synonym_id,
                              common::ObString &obj_table_name,
                              bool &do_exist,
                              bool search_public_schema = true,
                              bool *is_public = NULL) const;
  int get_outline_info_with_sql_id(const uint64_t tenant_id,
                                      const uint64_t database_id,
                                      const common::ObString &sql_id,
                                      const ObOutlineInfo *&outline_info) ;
  //about user define function
  int check_udf_exist_with_name(const uint64_t tenant_id,
                                const common::ObString &name,
                                bool &exist,
                                uint64_t &udf_id);
  int get_udf_info(const uint64_t tenant_id,
                   const common::ObString &name,
                   const ObUDF *&udf_info,
                   bool &exist);

  // udt
  int check_udt_exist(uint64_t tenant_id, uint64_t database_id,
                      uint64_t package_id, share::schema::ObUDTTypeCode type_code,
                      const common::ObString &udt_name, bool &exist);
  int get_udt_info(const uint64_t tenant_id, const uint64_t database_id, const uint64_t package_id,
                   const common::ObString &udt_name, const ObUDTTypeInfo *&udt_info);
  int get_udt_info(const uint64_t tenant_id, const uint64_t database_id, const uint64_t package_id,
                   const common::ObString &udt_name, const share::schema::ObUDTTypeCode &type_code,
                   const ObUDTTypeInfo *&udt_info);
  int get_udt_id(uint64_t tenant_id, uint64_t database_id, uint64_t package_id,
                 const common::ObString &udt_name, uint64_t &udt_id);
  int get_udt_schemas_in_tenant(const uint64_t tenant_id,
                                common::ObIArray<const ObUDTTypeInfo *> &udt_schemas);

  int check_sequence_exist_with_name(const uint64_t tenant_id,
                                     const uint64_t database_id,
                                     const common::ObString &sequence_name,
                                     bool &exist,
                                     uint64_t &sequence_id,
                                     bool &is_system_generated) const;
  int check_context_exist_with_name(const uint64_t tenant_id,
                                     const common::ObString &context_name,
                                     const ObContextSchema *&context_schema,
                                     bool &exist);
  int check_context_exist_by_id(const uint64_t tenant_id,
                                const uint64_t context_id,
                                const ObContextSchema *&context_schema,
                                bool &exist);
  int get_sequence_schema(const uint64_t tenant_id,
                          const uint64_t sequence_id,
                          const ObSequenceSchema *&schema);
  int get_sequence_schema_with_name(const uint64_t tenant_id,
                                    const uint64_t database_id,
                                    const common::ObString &sequence_name,
                                    const ObSequenceSchema *&sequence_schema);
  int get_context_schema_with_name(const uint64_t tenant_id,
                                   const common::ObString &context_name,
                                   const ObContextSchema *&context_schema);

  // mock_fk_parent_table begin
  int get_mock_fk_parent_table_ids_in_database(const uint64_t tenant_id,
                                               const uint64_t database_id,
                                               ObIArray<uint64_t> &mock_fk_parent_table_ids);
  int get_simple_mock_fk_parent_table_schema(const uint64_t tenant_id,
                                             const uint64_t database_id,
                                             const common::ObString &name,
                                             const ObSimpleMockFKParentTableSchema *&schema);
  int get_simple_mock_fk_parent_table_schema(const uint64_t tenant_id,
                                              const uint64_t mock_fk_parent_table_id,
                                              const ObSimpleMockFKParentTableSchema *&schema);
  int get_mock_fk_parent_table_schema_with_name(const uint64_t tenant_id,
                                                const uint64_t database_id,
                                                const common::ObString &name,
                                                const ObMockFKParentTableSchema *&schema);
  int get_mock_fk_parent_table_schema_with_id(const uint64_t tenant_id,
                                              const uint64_t mock_fk_parent_table_id,
                                              const ObMockFKParentTableSchema *&schema);
  // mock_fk_parent_table end

  //label security
  int get_label_se_policy_schema_by_id(const uint64_t tenant_id,
                                       const uint64_t label_se_policy_id,
                                       const ObLabelSePolicySchema *&schema);
  int get_label_se_policy_schema_by_name(const uint64_t tenant_id,
                                         const ObString &policy_name,
                                         const ObLabelSePolicySchema *&schema);
  int get_label_se_policy_schema_by_column_name(const uint64_t tenant_id,
                                                const ObString &column_name,
                                                const ObLabelSePolicySchema *&schema);
  int get_label_se_component_schema_by_id(const uint64_t tenant_id,
                                          const uint64_t label_se_comp_id,
                                          const ObLabelSeComponentSchema *&schema);
  int get_label_se_component_schema_by_short_name(const uint64_t tenant_id,
                                                  const uint64_t label_se_policy_id,
                                                  const int64_t comp_type,
                                                  const common::ObString &short_name,
                                                  const ObLabelSeComponentSchema *&schema);
  int get_label_se_component_schema_by_long_name(const uint64_t tenant_id,
                                                 const uint64_t label_se_policy_id,
                                                 const int64_t comp_type,
                                                 const common::ObString &long_name,
                                                 const ObLabelSeComponentSchema *&schema);
  int get_label_se_component_schema_by_comp_num(const uint64_t tenant_id,
                                                const uint64_t label_se_policy_id,
                                                const int64_t comp_type,
                                                const int64_t comp_num,
                                                const ObLabelSeComponentSchema *&schema);

  int get_label_se_label_by_label_tag(const uint64_t tenant_id,
                                      const int64_t label_tag,
                                      const ObLabelSeLabelSchema *&schema);

  int get_label_se_label_schema_by_id(const uint64_t tenant_id,
                                      const uint64_t label_se_label_id,
                                      const ObLabelSeLabelSchema *&schema);
  int get_label_se_label_schema_by_name(const uint64_t tenant_id,
                                        const ObString &short_name,
                                        const ObLabelSeLabelSchema *&schema);
  int get_label_se_user_level_by_id(const uint64_t tenant_id,
                                    const uint64_t user_id,
                                    const uint64_t policy_id,
                                    const ObLabelSeUserLevelSchema *&schema);
  // tablespace interface
  int get_tablespace_schema_with_name(const uint64_t tenant_id,
                                       const common::ObString &tablespace_name,
                                       const ObTablespaceSchema *&schema);
  int get_tablespace_schema(const uint64_t tenant_id,
                            const uint64_t tablespace_id,
                            const ObTablespaceSchema *&schema);
  int get_tablespace_schemas_in_tenant(const uint64_t tenant_id,
                                      common::ObIArray<const ObTablespaceSchema *> &tablespace_schemas);
  int get_tablespace_ids_in_tenant(const uint64_t tenant_id,
                                   common::ObIArray<uint64_t> &tablespace_ids_array);
  //end user define function

  ///begin oracle profile define function
  int get_profile_schema_by_name(const uint64_t tenant_id,
                                 const common::ObString &name,
                                 const ObProfileSchema *&schema);

  int get_profile_schema_by_id(const uint64_t tenant_id,
                               const uint64_t profile_id,
                               const ObProfileSchema *&schema);

  int get_user_profile_failed_login_limits(const uint64_t tenant_id,
                                           const uint64_t user_id,
                                           int64_t &failed_login_limit_num,
                                           int64_t &failed_login_limit_time);
  int get_user_password_expire_times(const uint64_t tenant_id,
                                     const uint64_t user_id,
                                     int64_t &password_last_change,
                                     int64_t &password_life_time,
                                     int64_t &password_grace_time);
  int get_user_profile_function_name(const uint64_t tenant_id,
                                     const uint64_t profile_id,
                                     common::ObString &function_name);
  ///end oracle profile define function

  // dblink function begin
  int check_dblink_exist(const uint64_t tenant_id,
                         const common::ObString &dblink_name,
                         bool &exist) const;
  int get_dblink_id(const uint64_t tenant_id,
                    const common::ObString &dblink_name,
                    uint64_t &dblink_id) const;
  int get_dblink_user(const uint64_t tenant_id,
                      const common::ObString &dblink_name,
                      common::ObString &dblink_user,
                      common::ObIAllocator &allocator);
  int get_dblink_schema(const uint64_t tenant_id,
                        const common::ObString &dblink_name,
                        const share::schema::ObDbLinkSchema *&dblink_schema) const;
  int get_dblink_schema(const uint64_t tenant_id,
                        const uint64_t dblink_id,
                        const share::schema::ObDbLinkSchema *&dblink_schema);
  int get_link_table_schema(const uint64_t tenant_id,
                            const uint64_t dblink_id,
                            const common::ObString &database_name,
                            const common::ObString &table_name,
                            common::ObIAllocator &allocator,
                            ObTableSchema *&table_schema,
                            sql::ObSQLSessionInfo *session_info,
                            const ObString &dblink_name,
                            bool is_reverse_link,
                            uint64_t *current_scn);
  // dblink function end

  // directory function begin
  int get_directory_schema_by_name(const uint64_t tenant_id,
                                   const common::ObString &name,
                                   const ObDirectorySchema *&schema) const;
  int get_directory_schema_by_id(const uint64_t tenant_id,
                                 const uint64_t directory_id,
                                 const ObDirectorySchema *&schema);
  int get_directory_schemas_in_tenant(const uint64_t tenant_id,
                                      common::ObIArray<const ObDirectorySchema *> &directory_schemas);
  // directory function end

  // rls function begin
  int get_rls_policy_schema_by_name(const uint64_t tenant_id,
                                    const uint64_t table_id,
                                    const uint64_t rls_group_id,
                                    const common::ObString &name,
                                    const ObRlsPolicySchema *&schema);
  int get_rls_policy_schema_by_id(const uint64_t tenant_id,
                                  const uint64_t rls_policy_id,
                                  const ObRlsPolicySchema *&schema);
  int get_rls_policy_schemas_in_group(const uint64_t tenant_id,
                                      const uint64_t table_id,
                                      const uint64_t rls_group_id,
                                      common::ObIArray<const ObRlsPolicySchema *> &schemas);
  int get_rls_policy_schemas_in_table(const uint64_t tenant_id,
                                      const uint64_t table_id,
                                      common::ObIArray<const ObRlsPolicySchema *> &schemas);
  int get_rls_group_schema_by_name(const uint64_t tenant_id,
                                   const uint64_t table_id,
                                   const common::ObString &name,
                                   const ObRlsGroupSchema *&schema);
  int get_rls_group_schema_by_id(const uint64_t tenant_id,
                                 const uint64_t rls_group_id,
                                 const ObRlsGroupSchema *&schema);
  int get_rls_group_schemas_in_table(const uint64_t tenant_id,
                                     const uint64_t table_id,
                                     common::ObIArray<const ObRlsGroupSchema *> &schemas);
  int get_rls_context_schema_by_name(const uint64_t tenant_id,
                                     const uint64_t table_id,
                                     const common::ObString &name,
                                     const common::ObString &attribute,
                                     const ObRlsContextSchema *&schema);
  int get_rls_context_schema_by_id(const uint64_t tenant_id,
                                   const uint64_t rls_context_id,
                                   const ObRlsContextSchema *&schema);
  int get_rls_context_schemas_in_table(const uint64_t tenant_id,
                                       const uint64_t table_id,
                                       common::ObIArray<const ObRlsContextSchema *> &schemas);
  // rls function end

  int check_user_exist(const uint64_t tenant_id,
                       const common::ObString &user_name,
                       const common::ObString &host_name,
                       bool &is_exist,
                       uint64_t *user_id = NULL);
  int check_user_exist(const uint64_t tenant_id,
                       const uint64_t user_id,
                       bool &is_exist);

  template <typename SchemaType>
  int check_flashback_object_exist(const SchemaType &object_schema,
                                   const common::ObString &object_name,
                                   bool &object_exist);

  int get_schema_count(const uint64_t tenant_id, int64_t &schema_count);
  int get_schema_size(const uint64_t tenant_id, int64_t &schema_count);
  /*
   * get schema object's schema_version.
   * OB_INVALID_VERSION will be returned if schema object doesn't exist.
   * For TENANT_SCHEMA, tenant_id should be OB_SYS_TENANT_ID.
   * For SYS_VARIABLE_SCHEMA, schema_id should be equal with tenant_id.
   */
  int get_schema_version(const ObSchemaType schema_type,
                         const uint64_t tenant_id,
                         const uint64_t schema_id,
                         int64_t &schema_version);
  int get_idx_schema_by_origin_idx_name(uint64_t tenant_id,
                                        uint64_t database_id,
                                        const common::ObString &index_name,
                                        const ObTableSchema *&table_schema);

  int get_tenant_unavailable_index(const uint64_t tenant_id, common::ObIArray<uint64_t> &table_ids);
  int check_unavailable_index_exist(const uint64_t tenant_id, bool &exist);
  int check_restore_error_index_exist(const uint64_t tenant_id, bool &exist);

  inline uint64_t get_session_id() const { return session_id_; }
  inline void set_session_id(const uint64_t id)  { session_id_ = id; }

  bool is_tenant_schema_guard() const { return common::OB_INVALID_TENANT_ID != tenant_id_; }
  uint64_t get_tenant_id() const { return tenant_id_; }

  SchemaGuardType get_schema_guard_type() const { return schema_guard_type_; }

  bool is_standby_cluster() { return is_standby_cluster_; }
  bool restore_tenant_exist() { return restore_tenant_exist_; }
  bool use_schema_status() { return is_standby_cluster() || restore_tenant_exist(); }

  int check_formal_guard() const;
  int is_lazy_mode(const uint64_t tenant_id, bool &is_lazy) const;

  int check_tenant_is_restore(const uint64_t tenant_id, bool &is_restore);
  int get_tenant_status(const uint64_t tenant_id, ObTenantStatus &status);
  int check_if_tenant_has_been_dropped(const uint64_t tenant_id, bool &is_dropped);
  int get_dropped_tenant_ids(common::ObIArray<uint64_t> &dropped_tenant_ids) const;
  int check_is_creating_standby_tenant(const uint64_t tenant_id, bool &is_creating_standby);

  int check_keystore_exist(const uint64_t tenant_id, bool &exist);
  int get_keystore_schema(const uint64_t tenant_id, const ObKeystoreSchema *&schema);

  int get_sys_priv_with_tenant_id(const uint64_t tenant_id,
                                  common::ObIArray<const ObSysPriv *> &sys_privs);
  int get_sys_priv_with_grantee_id(const uint64_t tenant_id,
                                   const uint64_t grantee_id,
                                   ObSysPriv *&sys_priv);
  int check_global_index_exist(const uint64_t tenant_id, const uint64_t table_id, bool &exist);

  int column_is_key(
      const uint64_t tenant_id,
      const uint64_t table_id,
      const uint64_t column_id,
      bool &is_key);

  int deep_copy_index_name_map(common::ObIAllocator &allocator,
                               ObIndexNameMap &index_name_cache);
  #define GET_SIMPLE_SCHEMAS_IN_DATABASE_FUNC_DECLARE(SCHEMA, SCHEMA_TYPE)     \
    int get_simple_##SCHEMA##_schemas_in_database(const uint64_t tenant_id,    \
                                                  const uint64_t database_id,  \
                                                  common::ObIArray<const SCHEMA_TYPE *> &schema_array);
  GET_SIMPLE_SCHEMAS_IN_DATABASE_FUNC_DECLARE(udt, ObSimpleUDTSchema);
  GET_SIMPLE_SCHEMAS_IN_DATABASE_FUNC_DECLARE(outline, ObSimpleOutlineSchema);
  GET_SIMPLE_SCHEMAS_IN_DATABASE_FUNC_DECLARE(synonym, ObSimpleSynonymSchema);
  GET_SIMPLE_SCHEMAS_IN_DATABASE_FUNC_DECLARE(package, ObSimplePackageSchema);
  GET_SIMPLE_SCHEMAS_IN_DATABASE_FUNC_DECLARE(routine, ObSimpleRoutineSchema);
  GET_SIMPLE_SCHEMAS_IN_DATABASE_FUNC_DECLARE(mock_fk_parent_table, ObSimpleMockFKParentTableSchema);

  int check_routine_priv(const ObSessionPrivInfo &session_priv,
                         const ObNeedPriv &routine_need_priv);

  int check_routine_definer_existed(uint64_t tenant_id, const ObString &user_name, bool &existed);

private:
  int check_ssl_access(const ObUserInfo &user_info,
                       SSL *ssl_st);
  int check_ssl_invited_cn(const uint64_t tenant_id, SSL *ssl_st);

  int check_db_priv(const ObSessionPrivInfo &session_priv,
                    const common::ObString &db,
                    const ObPrivSet need_priv_set,
                    ObPrivSet &user_db_priv_set);
  int check_db_priv(const ObSessionPrivInfo &session_priv,
                    const common::ObString &db,
                    const ObPrivSet need_priv_set);
  int check_user_priv(const ObSessionPrivInfo &session_priv,
                      const ObPrivSet priv_set);
  int verify_db_read_only(const uint64_t tenant_id,
                          const ObNeedPriv &need_priv);
  int verify_table_read_only(const uint64_t tenant_id,
                             const ObNeedPriv &need_priv);
  int check_single_obj_priv(const uint64_t tenant_id,
                            const uint64_t uid,
                            const ObOraNeedPriv &need_priv,
                            const common::ObIArray<uint64_t> &role_id_array);

  // for privilege
  int add_role_id_recursively(const uint64_t tenant_id,
                              const uint64_t role_id,
                              ObSessionPrivInfo &s_priv);
  int get_simple_trigger_schema(const uint64_t tenant_id,
                                const uint64_t trigger_id,
                                const ObSimpleTriggerSchema *&simple_trigger);
  int get_simple_trigger_schema(const uint64_t tenant_id,
                                const uint64_t database_id,
                                const common::ObString &trigger_name,
                                const ObSimpleTriggerSchema *&simple_trigger);
  int get_package_info_from_trigger(const uint64_t tenant_id,
                                    const uint64_t package_id,
                                    const ObPackageInfo *&package_info);
  //routine
  int check_routine_exist(uint64_t tenant_id, uint64_t database_id,
                          uint64_t package_id, const common::ObString &routine_name,
                          uint64_t overload, ObRoutineType routine_type, bool &exist) const;
  int get_routine_info(const uint64_t tenant_id,
                       const uint64_t database_id,
                       const uint64_t package_id,
                       const common::ObString &routine_name, uint64_t overload,
                       ObRoutineType routine_type, const ObRoutineInfo *&routine_info);
  int get_routine_id(uint64_t tenant_id, uint64_t database_id, uint64_t package_id,
                     const common::ObString &routine_name, uint64_t overload,
                     ObRoutineType routine_type, uint64_t &routine_id);

  int get_outline_schemas_in_tenant(const uint64_t tenant_id,
                                    common::ObIArray<const ObOutlineInfo *> &outline_schemas);
  int get_routine_schemas_in_tenant(const uint64_t tenant_id,
                                    common::ObIArray<const ObRoutineInfo *> &routine_schemas);
  int get_package_schemas_in_tenant(const uint64_t tenant_id,
                                    common::ObIArray<const ObPackageInfo *> &package_schemas);
  int get_trigger_schemas_in_tenant(const uint64_t tenant_id,
                                    common::ObIArray<const ObTriggerInfo*> &trigger_infos);
  int get_synonym_schemas_in_tenant(const uint64_t tenant_id,
                                    common::ObIArray<const ObSynonymInfo *> &synonym_schemas);

  // TODO: add this to all member functions
  bool check_inner_stat() const;

  // For TENANT_SCHEMA, tenant_id should be OB_SYS_TENANT_ID;
  // For SYS_VARIABLE_SCHEMA, tenant_id should be equal with schema_id;
  // specified_version should be invalid for lazy mode.
  template<typename T>
  int get_schema(const ObSchemaType schema_type,
                 const uint64_t tenant_id,
                 const uint64_t schema_id,
                 const T *&schema,
                 int64_t specified_version = common::OB_INVALID_VERSION);
  template<typename T>
  int get_from_local_cache(const ObSchemaType schema_type,
                           const uint64_t tenant_id,
                           const uint64_t schema_id,
                           const T *&schema);
  template<typename T>
  int put_to_local_cache(
      const ObSchemaType schema_type,
      const uint64_t tenant_id,
      const uint64_t schema_id,
      const T *&schema,
      common::ObKVCacheHandle &handle);

  int init(const bool is_standby_cluster);
  int fast_reset() {
    return is_inited_? reset(): common::OB_SUCCESS;
  }
  int check_tenant_schema_guard(const uint64_t tenant_id) const;
  int get_schema_mgr(const uint64_t tenant_id, const ObSchemaMgr *&schema_mgr) const;
  int get_schema_mgr_info(const uint64_t tenant_id, const ObSchemaMgrInfo *&schema_mgr_info) const;
  int check_lazy_guard(const uint64_t tenant_id, const ObSchemaMgr *&mgr) const;
  int get_schema_status(const uint64_t tenant_id, ObRefreshSchemaStatus &schema_status);

  int check_ora_conn_access(const uint64_t tenant_id,
      const uint64_t user_id,
      bool print_warn,
      const ObIArray<uint64_t> &role_id_array);
  bool ignore_tenant_not_exist_error(const uint64_t tenant_id);

  int check_priv_db_or_(const ObSessionPrivInfo &session_priv,
                        const ObNeedPriv &need_priv,
                        const ObPrivMgr &priv_mgr,
                        const uint64_t tenant_id,
                        const uint64_t user_id,
                        bool& pass);
  int check_priv_table_or_(const ObSessionPrivInfo &session_priv,
                           const ObNeedPriv &need_priv,
                           const ObPrivMgr &priv_mgr,
                           const uint64_t tenant_id,
                           const uint64_t user_id,
                           bool& pass);
  int get_table_schemas_in_tenant_(const uint64_t tenant_id,
                                   const bool only_view_schema,
                                   common::ObIArray<const ObTableSchema *> &table_schemas);
  int check_single_table_priv_for_update_(const ObSessionPrivInfo &session_priv,
                                          const ObNeedPriv &table_need_priv,
                                          const ObPrivMgr &priv_mgr);
  int check_activate_all_role_var(uint64_t tenant_id, bool &activate_all_role);
private:
  common::ObArenaAllocator local_allocator_;
  ObMultiVersionSchemaService *schema_service_;
  uint64_t session_id_; // 0: default value (session_id_ is useless)
                        // OB_INVALID_ID: inner session
                        // other: session id from SQL
                        // it's use to control if table is visable in some sessions

  static const int MAX_ID_SCHEMAS = 32;
  const static int64_t FULL_SCHEMA_MEM_THREHOLD = 100 * 1024 * 1024L;//100M
  // tenant_id_ is valid means it's tenant schema guard
  uint64_t tenant_id_;
  SchemaMgrInfos schema_mgr_infos_;
  // for new lazy logic
  SchemaObjs schema_objs_;

  ObSchemaMgrItem::Mod mod_;
  SchemaGuardType schema_guard_type_;
  bool is_standby_cluster_;
  bool restore_tenant_exist_;
  bool is_inited_;
  int64_t pin_cache_size_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObSchemaGetterGuard);
};
} //end of namespace schema
} //end of namespace share
} //end of namespace oceanbase
#endif //OB_OCEANBASE_SCHEMA_OB_SCHEMA_GETTER_GUARD_H_
