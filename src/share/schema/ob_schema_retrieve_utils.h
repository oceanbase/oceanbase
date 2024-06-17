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

#ifndef OCEANBASE_SHARE_SCHEMA_OB_SCHEMA_RETRIEVE_UTILS_H_
#define OCEANBASE_SHARE_SCHEMA_OB_SCHEMA_RETRIEVE_UTILS_H_

#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "share/schema/ob_schema_service.h"
#include "share/schema/ob_schema_utils.h"
#include "share/ob_primary_zone_util.h"
#include "share/system_variable/ob_system_variable_alias.h"
#include "share/ob_replica_info.h"
#include "share/ob_time_zone_info_manager.h"
#include "observer/ob_server_struct.h"
#include "observer/omt/ob_tenant_timezone_mgr.h"
#include "rootserver/ob_locality_util.h"

namespace oceanbase
{
namespace common
{
namespace sqlclient
{
class ObMySQLResult;
}
}

namespace share
{
namespace schema
{

template<typename TABLE_SCHEMA, typename SCHEMA>
class ObSchemaRetrieveHelperBase
{
public:
  static int64_t get_schema_id(const SCHEMA &schema) { UNUSED(schema); return -1; }
  template<typename T>
  static int fill_current(
      const uint64_t tenant_id,
      const bool check_deleted,
      T &result, SCHEMA &p,
      bool &is_deleted) { UNUSED(tenant_id); UNUSED(check_deleted); UNUSED(result); UNUSED(p); UNUSED(is_deleted);return common::OB_SUCCESS; };
  int add_schema(TABLE_SCHEMA &table_schema, SCHEMA &schema)
  { UNUSED(table_schema);UNUSED(schema);return common::OB_SUCCESS; }
};

template<typename TABLE_SCHEMA>
class ObSchemaRetrieveHelperBase<TABLE_SCHEMA, ObColumnSchemaV2>
{
public:
  static int64_t get_schema_id(const ObColumnSchemaV2 &s);
  template<typename T>
  static int fill_current(
      const uint64_t tenant_id,
      const bool check_deleted,
      T &result, ObColumnSchemaV2 &p,
      bool &is_deleted);
  static int add_schema(TABLE_SCHEMA &table_schema, ObColumnSchemaV2 &schema);
};

template<typename TABLE_SCHEMA>
class ObSchemaRetrieveHelperBase<TABLE_SCHEMA, ObConstraint>
{
public:
  static int64_t get_schema_id(const ObConstraint &s);
  template<typename T>
  static int fill_current(
      const uint64_t tenant_id,
      const bool check_deleted,
      T &result, ObConstraint &p,
      bool &is_deleted);
  static int add_schema(TABLE_SCHEMA &table_schema, ObConstraint &schema);
};

template<typename TABLE_SCHEMA>
class ObSchemaRetrieveHelperBase<TABLE_SCHEMA, ObPartition>
{
public:
  static int64_t get_schema_id(const ObPartition &s);
  template<typename T>
  static int fill_current(
      const uint64_t tenant_id,
      const bool check_deleted,
      T &result, ObPartition &p,
      bool &is_deleted);
  static int add_schema(TABLE_SCHEMA &table_schema, ObPartition &schema);
};

template<typename TABLE_SCHEMA, typename SCHEMA>
class ObSchemaRetrieveHelper
{
private:
  enum Mode {
    SINGLE_TABLE,
    MULTIPLE_TABLE
  };
public:
  ObSchemaRetrieveHelper(TABLE_SCHEMA &table)
    : mode_(SINGLE_TABLE),
      index_(0),
      table_(&table),
      tmp_table_(NULL),
      table_array_(NULL),
      current_allocator_("ScheRetri"),
      another_allocator_("ScheRetri"),
      current_schema_(&current_allocator_),
      another_schema_(&another_allocator_)
    {}
  ObSchemaRetrieveHelper(common::ObArray<TABLE_SCHEMA *> &tables)
    : mode_(Mode::MULTIPLE_TABLE),
      index_(0),
      table_(NULL),
      tmp_table_(NULL),
      table_array_(&tables),
      current_allocator_("ScheRetri"),
      another_allocator_("ScheRetri"),
      current_schema_(&current_allocator_),
      another_schema_(&another_allocator_)
    {}

  ~ObSchemaRetrieveHelper() {}
  SCHEMA &get_and_reset_current()
  {
    if (0 == index_) {
      current_schema_.reset();
      current_allocator_.reuse();
    } else {
      another_schema_.reset();
      another_allocator_.reuse();
    }
    return 0 == index_ ? current_schema_ : another_schema_;
  }
  int64_t get_curr_schema_id();
  void rotate() { index_ = 1 - index_; }
  int get_table(const uint64_t table_id, TABLE_SCHEMA *&table);
  int add(SCHEMA &s);
  template<typename T>
  static int fill_current(
      const uint64_t tenant_id,
      const bool check_deleted,
      T &result, SCHEMA &p,
      bool &is_deleted);
private:
  const Mode mode_;
  int index_;
  //for single table
  TABLE_SCHEMA *table_;
  //for multi table
  TABLE_SCHEMA *tmp_table_;
  ObArray<TABLE_SCHEMA *> *table_array_;
  ObArenaAllocator current_allocator_;
  ObArenaAllocator another_allocator_;
  SCHEMA current_schema_;
  SCHEMA another_schema_;
};


template<typename TABLE_SCHEMA>
class ObSubPartSchemaRetrieveHelper
{
private:
  enum Mode {
    SINGLE_TABLE,
    MULTIPLE_TABLE
  };
public:
  ObSubPartSchemaRetrieveHelper(TABLE_SCHEMA &table,
                                const bool is_subpart_template = true)
    : mode_(SINGLE_TABLE),
      index_(0),
      table_(&table),
      tmp_table_(NULL),
      table_array_(NULL),
      partition_(NULL),
      is_subpart_template_(is_subpart_template),
      current_allocator_("SubScheRetri"),
      another_allocator_("SubScheRetri"),
      current_schema_(&current_allocator_),
      another_schema_(&another_allocator_)
    {}
  ObSubPartSchemaRetrieveHelper(common::ObArray<TABLE_SCHEMA *> &tables,
                                const bool is_subpart_template = true)
    : mode_(Mode::MULTIPLE_TABLE),
      index_(0),
      table_(NULL),
      tmp_table_(NULL),
      table_array_(&tables),
      partition_(NULL),
      is_subpart_template_(is_subpart_template),
      current_allocator_("SubScheRetri"),
      another_allocator_("SubScheRetri"),
      current_schema_(&current_allocator_),
      another_schema_(&another_allocator_)
    {}

  ~ObSubPartSchemaRetrieveHelper() {}
  ObSubPartition &get_and_reset_current()
  {
    if (0 == index_) {
      current_schema_.reset();
      current_allocator_.reuse();
    } else {
      another_schema_.reset();
      another_allocator_.reuse();
    }
    return 0 == index_ ? current_schema_ : another_schema_;
  }
  int64_t get_curr_schema_id();
  void rotate() { index_ = 1 - index_; }
  int get_table(const uint64_t table_id, TABLE_SCHEMA *&table);
  int add(ObSubPartition &s);
  template<typename T>
  int fill_current(
      const uint64_t tenant_id,
      const bool check_deleted,
      T &result,
      ObSubPartition &p,
      bool &is_deleted);
private:
  const Mode mode_;
  int index_;
  //for single table
  TABLE_SCHEMA *table_;
  //for multi table
  TABLE_SCHEMA *tmp_table_;
  ObArray<TABLE_SCHEMA *> *table_array_;
  // for is_subpart_template = false
  ObPartition *partition_;
  bool is_subpart_template_;
  ObArenaAllocator current_allocator_;
  ObArenaAllocator another_allocator_;
  ObSubPartition current_schema_;
  ObSubPartition another_schema_;
};

struct VersionHisVal;
class ObSchemaRetrieveUtils
{
public:

/******************************************************************
 *
 * for full schemas
 *
 ******************************************************************/

  template<typename T, typename S>
  static int retrieve_tenant_schema(common::ObISQLClient &client,
                                    T &result,
                                    common::ObIArray<S> &tenant_schema_array);
  template<typename T, typename SCHEMA>
  static int retrieve_system_variable(const uint64_t tenant_id, T &result, SCHEMA &tenant_schema);

  template<typename T>
  static int retrieve_system_variable_obj(
             const uint64_t tenant_id,
             T &result,
             ObIAllocator &allocator,
             ObObj &out_var_obj);

  //for batch table
  template<typename T, typename TABLE_SCHEMA>
  static int retrieve_table_schema(const uint64_t tenant_id,
                                   const bool check_deleted,
                                   T &result,
                                   common::ObIAllocator &allocator,
                                   common::ObIArray<TABLE_SCHEMA *> &table_schema_array);

  template<typename TABLE_SCHEMA, typename SCHEMA,  typename T>
  static int retrieve_schema(const uint64_t tenant_id,
                             const bool check_deleted,
                             T &result,
                             ObArray<TABLE_SCHEMA *> &table_schema_array);

  template<typename TABLE_SCHEMA, typename T>
  static int retrieve_subpart_schema(
             const uint64_t tenant_id,
             const bool check_deleted,
             const bool is_subpart_template,
             T &result,
             ObArray<TABLE_SCHEMA *> &table_schema_array);

  template<typename T>
  static int retrieve_column_schema(const uint64_t tenant_id,
                                    const bool check_deleted,
                                    T &result,
                                    common::ObArray<ObTableSchema *> &table_schema_array);
  template<typename T>
  static int retrieve_column_group_schema(const uint64_t tenant_id,
                                          const bool check_deleted,
                                          T &result,
                                          common::ObArray<ObTableSchema *> &table_schema_array);

  template<typename T>
  static int retrieve_constraint(const uint64_t tenant_id,
                                 const bool check_deleted,
                                 T &result,
                                 common::ObArray<ObTableSchema *> &table_schema_array);

  template<typename T>
  static int retrieve_constraint_column_info(const uint64_t tenant_id,
                                             T &result, ObConstraint *&cst);

  template<typename T>
  static int retrieve_aux_tables(const uint64_t tenant_id,
                                 T &result,
                                 ObIArray<ObAuxTableMetaInfo> &aux_tables);

  template<typename TABLE_SCHEMA, typename T>
  static int retrieve_part_info(const uint64_t tenant_id,
                                const bool check_deleted,
                                T &result,
                                common::ObArray<TABLE_SCHEMA *> &table_schema_array);

  template<typename TABLE_SCHEMA, typename T>
  static int retrieve_def_subpart_info(const uint64_t tenant_id,
                                       const bool check_deleted,
                                       T &result,
                                       common::ObArray<TABLE_SCHEMA *> &table_schema_array);

  template<typename TABLE_SCHEMA, typename T>
  static int retrieve_subpart_info(const uint64_t tenant_id,
                                   const bool check_deleted,
                                   T &result,
                                   common::ObArray<TABLE_SCHEMA *> &table_schema_array);

  //for single table
  template<typename T>
  static int retrieve_table_schema(const uint64_t tenant_id,
                                   const bool check_deleted,
                                   T &result,
                                   common::ObIAllocator &allocator,
                                   ObTableSchema *&table_schema);
  template<typename T>
  static int retrieve_column_schema(const uint64_t tenant_id,
                                    const bool check_deleted,
                                    T &result,
                                    ObTableSchema *&table_schema);

  template<typename T>
  static int retrieve_constraint(const uint64_t tenant_id,
                                 const bool check_deleted,
                                 T &result,
                                 ObTableSchema *&table_schema);

  template<typename T>
  static int retrieve_column_group_schema(const uint64_t tenant_id,
                                          const bool check_deleted,
                                          T &result,
                                          ObTableSchema *&table_schema);

  template<typename T>
  static int retrieve_column_group_mapping(const uint64_t tenant_id,
                                           const bool check_deleted,
                                           T &result,
                                           ObTableSchema *&table_schema);

  //for ObColumnSchema ObPartition
  template<typename TABLE_SCHEMA, typename SCHEMA,  typename T>
  static int retrieve_schema(const uint64_t tenant_id,
                             const bool check_deleted,
                             T &result,
                             TABLE_SCHEMA *&table_schema);

  template<typename TABLE_SCHEMA, typename T>
  static int retrieve_subpart_schema(
             const uint64_t tenant_id,
             const bool check_deleted,
             const bool is_subpart_template,
             T &result,
             TABLE_SCHEMA *&table_schema);

  template<typename TABLE_SCHEMA, typename T>
  static int retrieve_part_info(const uint64_t tenant_id,
                                const bool check_deleted, T &result,
                                TABLE_SCHEMA *&table_schema);

  template<typename TABLE_SCHEMA, typename T>
  static int retrieve_def_subpart_info(const uint64_t tenant_id,
                                       const bool check_deleted,
                                       T &result,
                                       TABLE_SCHEMA *&table_schema);

  template<typename TABLE_SCHEMA, typename T>
  static int retrieve_subpart_info(const uint64_t tenant_id,
                                   const bool check_deleted,
                                   T &result,
                                   TABLE_SCHEMA *&table_schema);

  template<typename TABLE_SCHEMA, typename T>
  static int retrieve_foreign_key_info(const uint64_t tenant_id, T &result, TABLE_SCHEMA &table_schema);

  template<typename T>
  static int retrieve_foreign_key_column_info(const uint64_t tenant_id,
                                              T &result, ObForeignKeyInfo &foreign_key_info);

  template<typename T, typename S>
  static int retrieve_simple_encrypt_info(const uint64_t tenant_id,
                                          T &result,
                                          ObArray<S *> &table_schema_array);
  #define RETRIEVE_SCHEMA_FUNC_DECLARE(SCHEMA)                                \
    template<typename T, typename S>                                          \
    static int retrieve_##SCHEMA##_schema(const uint64_t tenant_id, \
                                          T &result,                          \
                                          common::ObIArray<S> &schema_array);
  RETRIEVE_SCHEMA_FUNC_DECLARE(user);
  RETRIEVE_SCHEMA_FUNC_DECLARE(database);
  RETRIEVE_SCHEMA_FUNC_DECLARE(tablegroup);
  RETRIEVE_SCHEMA_FUNC_DECLARE(outline);
  RETRIEVE_SCHEMA_FUNC_DECLARE(db_priv);
  RETRIEVE_SCHEMA_FUNC_DECLARE(table_priv);
  RETRIEVE_SCHEMA_FUNC_DECLARE(routine_priv);

  RETRIEVE_SCHEMA_FUNC_DECLARE(column_priv);
  RETRIEVE_SCHEMA_FUNC_DECLARE(package);
  RETRIEVE_SCHEMA_FUNC_DECLARE(routine);
  RETRIEVE_SCHEMA_FUNC_DECLARE(trigger);
  template<typename T>
  static int retrieve_trigger_list(const uint64_t tenant_id, T &result, common::ObIArray<uint64_t> &trigger_list);
  template<typename T>
  static int retrieve_routine_param_schema(const uint64_t tenant_id, T &result, common::ObIArray<ObRoutineInfo> &routine_infos);
  RETRIEVE_SCHEMA_FUNC_DECLARE(synonym);
  RETRIEVE_SCHEMA_FUNC_DECLARE(udf);
  // udt
  RETRIEVE_SCHEMA_FUNC_DECLARE(udt);
  template<typename T>
  static int retrieve_udt_attr_schema(const uint64_t tenant_id, T &result, common::ObIArray<ObUDTTypeInfo> &udt_infos);
  template<typename T>
  static int retrieve_udt_coll_schema(const uint64_t tenant_id, T &result, common::ObIArray<ObUDTTypeInfo> &udt_infos);
  RETRIEVE_SCHEMA_FUNC_DECLARE(udt_object);
  RETRIEVE_SCHEMA_FUNC_DECLARE(sequence);
  RETRIEVE_SCHEMA_FUNC_DECLARE(keystore);
  RETRIEVE_SCHEMA_FUNC_DECLARE(label_se_policy);
  RETRIEVE_SCHEMA_FUNC_DECLARE(label_se_component);
  RETRIEVE_SCHEMA_FUNC_DECLARE(label_se_label);
  RETRIEVE_SCHEMA_FUNC_DECLARE(label_se_user_level);
  RETRIEVE_SCHEMA_FUNC_DECLARE(tablespace);
  RETRIEVE_SCHEMA_FUNC_DECLARE(profile);
  RETRIEVE_SCHEMA_FUNC_DECLARE(audit);
  RETRIEVE_SCHEMA_FUNC_DECLARE(sys_priv);
  RETRIEVE_SCHEMA_FUNC_DECLARE(obj_priv);

  RETRIEVE_SCHEMA_FUNC_DECLARE(dblink);
  RETRIEVE_SCHEMA_FUNC_DECLARE(directory);
  RETRIEVE_SCHEMA_FUNC_DECLARE(context);
  RETRIEVE_SCHEMA_FUNC_DECLARE(mock_fk_parent_table);
  RETRIEVE_SCHEMA_FUNC_DECLARE(rls_policy);
  RETRIEVE_SCHEMA_FUNC_DECLARE(rls_group);
  RETRIEVE_SCHEMA_FUNC_DECLARE(rls_context);
  //RETRIEVE_SCHEMA_FUNC_DECLARE(proxy);
  //RETRIEVE_SCHEMA_FUNC_DECLARE(proxy_role);
  template<typename T>
  static int retrieve_object_list(const uint64_t tenant_id, T &result, common::ObIArray<uint64_t> &trigger_list);
  template<typename T>
  static int retrieve_mock_fk_parent_table_schema_column(
      const uint64_t tenant_id, T &result,
      ObMockFKParentTableSchema &mock_fk_parent_table);

  template<typename AT, typename TST>
  static int retrieve_link_table_schema(const uint64_t tenant_id,
                                        AT &result,
                                        common::ObIAllocator &allocator,
                                        TST *&table_schema);
  template<typename T>
  static int retrieve_link_column_schema(const uint64_t tenant_id,
                                         T &result,
                                         const common::sqlclient::DblinkDriverProto driver_type,
                                         ObTableSchema &table_schema);

  template<typename T>
  static int retrieve_tablegroup_schema(const uint64_t tenant_id,
                                        T &result,
                                        ObIAllocator &allocator,
                                        ObTablegroupSchema *&tablegroup_schema);

  template<typename T>
  static int retrieve_recycle_object(const uint64_t tenant_id, T &result, ObIArray<ObRecycleObject> &recycle_objs);
  template<typename T>
  static int retrieve_schema_version(T &result, VersionHisVal &version_his_val);

/**
 * start of fill schema
 */
  #define FILL_SCHEMA_FUNC_DECLARE(SCHEMA, SCHEMA_TYPE)       \
    template<typename T>                                      \
    static int fill_##SCHEMA##_schema(const uint64_t tenant_id,\
                                      T &result,              \
                                      SCHEMA_TYPE &schema,    \
                                      bool &is_deleted);
  //for simple schema
  template<typename T>
  static int fill_tenant_schema(T &result,
                                ObSimpleTenantSchema &schema,
                                bool &is_deleted);
  FILL_SCHEMA_FUNC_DECLARE(user, ObSimpleUserSchema);
  FILL_SCHEMA_FUNC_DECLARE(database, ObSimpleDatabaseSchema);
  FILL_SCHEMA_FUNC_DECLARE(tablegroup, ObSimpleTablegroupSchema);
  FILL_SCHEMA_FUNC_DECLARE(outline, ObSimpleOutlineSchema);
  FILL_SCHEMA_FUNC_DECLARE(routine, ObSimpleRoutineSchema);
  FILL_SCHEMA_FUNC_DECLARE(synonym, ObSimpleSynonymSchema);
  FILL_SCHEMA_FUNC_DECLARE(package, ObSimplePackageSchema);
  FILL_SCHEMA_FUNC_DECLARE(trigger, ObSimpleTriggerSchema);
  FILL_SCHEMA_FUNC_DECLARE(udf, ObSimpleUDFSchema);
  FILL_SCHEMA_FUNC_DECLARE(udt, ObSimpleUDTSchema);
  FILL_SCHEMA_FUNC_DECLARE(sequence, ObSequenceSchema);
  FILL_SCHEMA_FUNC_DECLARE(keystore, ObKeystoreSchema);
  FILL_SCHEMA_FUNC_DECLARE(label_se_policy, ObLabelSePolicySchema);
  FILL_SCHEMA_FUNC_DECLARE(label_se_component, ObLabelSeComponentSchema);
  FILL_SCHEMA_FUNC_DECLARE(label_se_label, ObLabelSeLabelSchema);
  FILL_SCHEMA_FUNC_DECLARE(label_se_user_level, ObLabelSeUserLevelSchema);
  FILL_SCHEMA_FUNC_DECLARE(tablespace, ObTablespaceSchema);
  FILL_SCHEMA_FUNC_DECLARE(profile, ObProfileSchema);
  FILL_SCHEMA_FUNC_DECLARE(audit, ObSAuditSchema);
  FILL_SCHEMA_FUNC_DECLARE(dblink, ObDbLinkSchema);
  FILL_SCHEMA_FUNC_DECLARE(directory, ObDirectorySchema);
  FILL_SCHEMA_FUNC_DECLARE(context, ObContextSchema);
  FILL_SCHEMA_FUNC_DECLARE(mock_fk_parent_table, ObSimpleMockFKParentTableSchema);
  FILL_SCHEMA_FUNC_DECLARE(rls_policy, ObRlsPolicySchema);
  FILL_SCHEMA_FUNC_DECLARE(rls_group, ObRlsGroupSchema);
  FILL_SCHEMA_FUNC_DECLARE(rls_context, ObRlsContextSchema);
  FILL_SCHEMA_FUNC_DECLARE(rls_column, ObRlsSecColumnSchema);

  //for full schema
  template<typename T>
  static int fill_tenant_schema(T &result,
                                ObTenantSchema &schema,
                                bool &is_deleted);
  FILL_SCHEMA_FUNC_DECLARE(user, ObUserInfo);
  FILL_SCHEMA_FUNC_DECLARE(database, ObDatabaseSchema);
  FILL_SCHEMA_FUNC_DECLARE(tablegroup, ObTablegroupSchema);
  FILL_SCHEMA_FUNC_DECLARE(outline, ObOutlineInfo);
  FILL_SCHEMA_FUNC_DECLARE(db_priv, ObDBPriv);
  FILL_SCHEMA_FUNC_DECLARE(table_priv, ObTablePriv);
  FILL_SCHEMA_FUNC_DECLARE(routine_priv, ObRoutinePriv);

  FILL_SCHEMA_FUNC_DECLARE(column_priv, ObColumnPriv);
  FILL_SCHEMA_FUNC_DECLARE(package, ObPackageInfo);
  FILL_SCHEMA_FUNC_DECLARE(routine, ObRoutineInfo);
  FILL_SCHEMA_FUNC_DECLARE(routine_param, ObRoutineParam);
  FILL_SCHEMA_FUNC_DECLARE(trigger, ObTriggerInfo);
  FILL_SCHEMA_FUNC_DECLARE(synonym, ObSynonymInfo);
  FILL_SCHEMA_FUNC_DECLARE(sysvar, ObSysVarSchema);
  FILL_SCHEMA_FUNC_DECLARE(udf, ObUDF);
  // udt
  FILL_SCHEMA_FUNC_DECLARE(udt, ObUDTTypeInfo);
  FILL_SCHEMA_FUNC_DECLARE(udt_attr, ObUDTTypeAttr);
  FILL_SCHEMA_FUNC_DECLARE(udt_coll, ObUDTCollectionType);
  FILL_SCHEMA_FUNC_DECLARE(udt_object, ObUDTObjectType);
  // link table
  FILL_SCHEMA_FUNC_DECLARE(link_table, ObTableSchema);
  FILL_SCHEMA_FUNC_DECLARE(link_column, ObColumnSchemaV2);
  template<typename T>
  static int fill_mock_fk_parent_table_column_info(
      const uint64_t tenant_id, T &result, uint64_t &parent_column_id, ObString &parent_column_name,
      bool &is_deleted);

  template<typename T>
  static int fill_sys_priv_schema(
                                 const uint64_t tenant_id,
                                 T &result,
                                 ObSysPriv &schema,
                                 bool &is_deleted,
                                 ObRawPriv &raw_p_id,
                                 uint64_t &option);

  template<typename T>
  static int fill_obj_priv_schema(const uint64_t tenant_id,
                                  T &result,
                                  ObObjPriv &obj_priv,
                                  bool &is_deleted,
                                  ObRawObjPriv &raw_p_id,
                                  uint64_t &option);

  template<typename T>
  static int fill_trigger_id(const uint64_t tenant_id, T &result,
                             uint64_t &trigger_id, bool &is_deleted);
  template<typename T>
  static int fill_table_schema(const uint64_t tenant_id, const bool check_deleted, T &result,
                               ObSimpleTableSchemaV2 &table_schema, bool &is_deleted);
  template<typename T>
  static int fill_table_schema(const uint64_t tenant_id, const bool check_deleted, T &result,
                               ObTableSchema &table_schema, bool &is_deleted);
  template<typename T>
  static int fill_column_schema(const uint64_t tenant_id, const bool check_deleted, T &result,
                                ObColumnSchemaV2 &column, bool &is_deleted);

  template<typename T>
  static int fill_constraint(const uint64_t tenant_id, const bool check_deleted, T &result,
                             ObConstraint &constraint, bool &is_deleted);

  template<typename T>
  static int fill_column_group_info(const bool check_deleted, T &result,
                                    ObColumnGroupSchema &column_group,
                                    uint64_t &table_id, bool &is_deleted);

  template<typename T>
  static int fill_constraint_column_info(
      const uint64_t tenant_id, T &result, uint64_t &column_id, bool &is_deleted);

  template<typename T>
  static int fill_base_part_info(const uint64_t tenant_id,
                                 const bool check_deleted,
                                 const bool is_subpart_def,
                                 const bool is_subpart_template,
                                 T &result,
                                 ObBasePartition &partition, bool &is_deleted);

  template<typename T>
  static int fill_part_info(const uint64_t tenant_id, const bool check_deleted, T &result,
                            ObPartition &partition, bool &is_deleted);

  template<typename T>
  static int fill_def_subpart_info(const uint64_t tenant_id,
                                   const bool check_deleted,
                                   T &result,
                                   ObSubPartition &partition,
                                   bool &is_deleted);

  template<typename T>
  static int fill_subpart_info(const uint64_t tenant_id,
                               const bool check_deleted,
                               T &result,
                               ObSubPartition &partition,
                               bool &is_deleted);

  template<typename T>
  static int fill_foreign_key_info(const uint64_t tenant_id, uint64_t table_id, T &result, ObForeignKeyInfo &foreign_key_info, bool &is_deleted);

  template<typename T>
  static int fill_foreign_key_column_info(const uint64_t tenant_id, T &result, int64_t &child_column_id, int64_t &parent_column_id, bool &is_deleted);

  template<typename T>
  static int retrieve_simple_foreign_key_info(const uint64_t tenant_id, T &result, ObArray<ObSimpleTableSchemaV2 *> &table_schema_array);

  template<typename T>
  static int get_foreign_key_id_and_name(const uint64_t tenant_id,
                                         T &result,
                                         bool &is_deleted,
                                         uint64_t &fk_id,
                                         ObString &fk_name,
                                         uint64_t &table_id);
  template<typename T>
  static int retrieve_simple_constraint_info(const uint64_t tenant_id, T &result, ObArray<ObSimpleTableSchemaV2 *> &table_schema_array);

  template<typename T>
  static int get_constraint_id_and_name(const uint64_t tenant_id,
                                        T &result,
                                        bool &is_deleted,
                                        uint64_t &cst_id,
                                        ObString &cst_name,
                                        uint64_t &table_id,
                                        ObConstraintType &cst_type);
  template<typename T>
  static int retrieve_role_grantee_map_schema(const uint64_t tenant_id,
      T &result,
      const bool is_fetch_role,
      ObArray<ObUserInfo> &user_array);
  template<typename T>
  static int retrieve_proxy_info_schema(const uint64_t tenant_id,
      T &result,
      const bool is_fetch_proxy,
      ObArray<ObUserInfo> &user_array);

  template<typename T>
  static int retrieve_proxy_role_info_schema(const uint64_t tenant_id,
    T &result,
    const bool is_fetch_proxy,
    ObArray<ObUserInfo> &user_array);
  static inline int find_user_info(const uint64_t user_id,
      ObArray<ObUserInfo> &user_array,
      ObUserInfo *&user_info);
  template<typename T>
  static bool compare_user_id(const T &user_info, const uint64_t user_id);
  template<typename T>
  static int retrieve_rls_column_schema(const uint64_t tenant_id,
                                        T &result,
                                        ObArray<ObRlsPolicySchema *> &rls_policy_array);
  template<typename T>
  static int find_rls_policy_schema(const uint64_t rls_policy_id,
                                    ObArray<T *> rls_policy_schema_array,
                                    T *&rls_policy_schema);
  template<typename T>
  static bool compare_rls_policy_id(const T *rls_policy_schema, const uint64_t rls_policy_id);
  template<typename T>
  static int fill_object_id(const uint64_t tenant_id, T &result,
                            uint64_t &object_id, bool &is_deleted);

  // template<typename T>
  // static bool compare_proxy_id(const T *proxy_schema, const uint64_t proxy_id);
//===========================================================================

  template<typename T, typename SCHEMA>
  static int fill_replica_options(T &result, SCHEMA &schema);
  template<typename T>
  static int fill_recycle_object(const uint64_t tenant_id, T &result, ObRecycleObject &recycle_obj);
  template<typename T>
  static int fill_schema_operation(const uint64_t tenant_id,
                                   T &result, ObSchemaService::SchemaOperationSetWithAlloc &schema_operations,
                                   ObSchemaOperation &schema_op);
  template<typename SCHEMA>
  static int fill_schema_zone_region_replica_num_array(SCHEMA &schema);
  template<typename SCHEMA>
  static int fill_table_zone_region_replica_num_array(SCHEMA &table_schema);
  template<typename T>
  static T *find_table_schema(const uint64_t table_id,
                              common::ObArray<T *> &table_schema_array);
  template<typename T>
  static int fill_temp_table_schema(const uint64_t tenant_id, T &result, ObTableSchema &table_schema);

  template<typename T>
  static int retrieve_drop_tenant_infos(T &result, ObIArray<ObDropTenantInfo> &drop_tenant_infos);

  template<typename TABLE_SCHEMA>
  static int cascaded_generated_column(TABLE_SCHEMA &table_schema);

  template<typename T>
  static int retrieve_table_latest_schema_versions(
      T &result,
      ObIArray<ObTableLatestSchemaVersion> &table_schema_versions);

private:
  template<typename T>
  static bool compare_table_id(const T *table_schema, const uint64_t table_id);
  static inline int retrieve_generated_column(const ObTableSchema &table_schema, ObColumnSchemaV2 &column);

  template<typename S>
  static int push_prev_array_if_has(
      ObIArray<S> &sys_priv_array, 
      S &prev_priv,
      ObPackedPrivArray &packed_grant_privs);

  template<typename S>
  static int push_prev_obj_privs_if_has(
      ObIArray<S> &obj_priv_array, 
      S &obj_priv,
      ObPackedObjPriv &packed_obj_privs);

  template<typename T, typename S>
  static int retrieve_sys_priv_schema_inner(const uint64_t tenant_id,
                                            T &result,
                                            ObIArray<S> &sys_priv_array);
  template<typename T, typename S>
  static int retrieve_obj_priv_schema_inner(
      const uint64_t tenant_id,
      T &result,
      ObIArray<S> &obj_priv_array);
  static int fill_sys_table_lob_tid(ObTableSchema &table);

  ObSchemaRetrieveUtils() {}
  ~ObSchemaRetrieveUtils() {}
};

class ObRoutineParamSetter
{
public:
  ObRoutineParamSetter(common::ObIArray<ObRoutineInfo> &routine_infos)
    : routine_infos_(routine_infos),
      cur_idx_(0) { }

  int add_routine_param(const ObRoutineParam &routine_param)
  {
    int ret = common::OB_SUCCESS;
    bool is_break = false;
    while (OB_SUCC(ret) && !is_break) {
      if (cur_idx_ >= routine_infos_.count() || cur_idx_ < 0) {
        ret = common::OB_ERR_UNEXPECTED;
        SHARE_SCHEMA_LOG(WARN, "current index is out of range", K_(cur_idx), K(routine_infos_.count()));
      } else if (routine_param.get_routine_id() == routine_infos_.at(cur_idx_).get_routine_id()) {
        if (OB_FAIL(routine_infos_.at(cur_idx_).add_routine_param(routine_param))) {
          SHARE_SCHEMA_LOG(WARN, "add routine param failed", K(ret), K(routine_param), K_(cur_idx));
        } else {
          is_break = true;
        }
      } else {
        ++cur_idx_;
      }
    }
    return ret;
  }
private:
  common::ObIArray<ObRoutineInfo> &routine_infos_;
  int64_t cur_idx_;
};

class ObUDTAttrSetter
{
public:
  ObUDTAttrSetter(common::ObIArray<ObUDTTypeInfo> &udt_infos)
    : udt_infos_(udt_infos),
      cur_idx_(0) { }

  int add_type_coll(const ObUDTCollectionType &udt_coll)
  {
    int ret = common::OB_SUCCESS;
    bool is_break = false;
    while (OB_SUCC(ret) && !is_break) {
      if (cur_idx_ >= udt_infos_.count() || cur_idx_ < 0) {
        ret = common::OB_ERR_UNEXPECTED;
        SHARE_SCHEMA_LOG(WARN, "current index is out of range", K_(cur_idx), K(udt_infos_.count()));
      } else if (udt_coll.get_coll_type_id() == udt_infos_.at(cur_idx_).get_type_id()) {
        if (OB_FAIL(udt_infos_.at(cur_idx_).set_coll_info(udt_coll))) {
          SHARE_SCHEMA_LOG(WARN, "add udt coll failed", K(ret), K(udt_coll), K_(cur_idx));
        } else {
          is_break = true;
        }
      } else {
        ++cur_idx_;
      }
    }
    return ret;
  }

  int add_type_attr(const ObUDTTypeAttr &udt_attr)
  {
    int ret = common::OB_SUCCESS;
    bool is_break = false;
    while (OB_SUCC(ret) && !is_break) {
      if (cur_idx_ >= udt_infos_.count() || cur_idx_ < 0) {
        ret = common::OB_ERR_UNEXPECTED;
        SHARE_SCHEMA_LOG(WARN, "current index is out of range", K_(cur_idx), K(udt_infos_.count()));
      } else if (udt_attr.get_type_id() == udt_infos_.at(cur_idx_).get_type_id()) {
        if (OB_FAIL(udt_infos_.at(cur_idx_).add_type_attr(udt_attr))) {
          SHARE_SCHEMA_LOG(WARN, "add udt attr failed", K(ret), K(udt_attr), K_(cur_idx));
        } else {
          is_break = true;
        }
      } else {
        ++cur_idx_;
      }
    }
    return ret;
  }

  int add_type_object(const ObUDTObjectType &udt_object)
  {
    int ret = common::OB_SUCCESS;
    bool is_break = false;
    while (OB_SUCC(ret) && !is_break) {
      if (cur_idx_ >= udt_infos_.count() || cur_idx_ < 0) {
        ret = common::OB_ERR_UNEXPECTED;
        SHARE_SCHEMA_LOG(WARN, "current index is out of range", K_(cur_idx), K(udt_infos_.count()));
      } else if (udt_object.get_object_type_id() == udt_infos_.at(cur_idx_).get_type_id()) {
        if (!udt_infos_.at(cur_idx_).is_object_type_info_exist(udt_object)
            && OB_FAIL(udt_infos_.at(cur_idx_).add_object_type_info(udt_object))) {
          SHARE_SCHEMA_LOG(WARN, "add udt object type failed", K(ret), K(udt_object), K_(cur_idx));
        } else {
          is_break = true;
        }
      } else {
        ++cur_idx_;
      }
    }
    return ret;
  }
private:
  common::ObIArray<ObUDTTypeInfo> &udt_infos_;
  int64_t cur_idx_;
};

} //end of namespace schema
} //end of namespace share
} //end of namespace oceanbase

#include "ob_schema_retrieve_utils.ipp"
#endif
