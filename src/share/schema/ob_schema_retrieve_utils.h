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
#include "share/ob_primary_zone_util.h"
#include "share/system_variable/ob_system_variable_alias.h"
#include "share/ob_replica_info.h"
#include "share/ob_time_zone_info_manager.h"
#include "observer/ob_server_struct.h"
#include "observer/omt/ob_tenant_timezone_mgr.h"

namespace oceanbase {
namespace common {
namespace sqlclient {
class ObMySQLResult;
}
}  // namespace common

namespace share {
namespace schema {

template <typename TABLE_SCHEMA, typename SCHEMA>
class ObSchemaRetrieveHelperBase {
public:
  static int64_t get_schema_id(const SCHEMA& schema)
  {
    UNUSED(schema);
    return -1;
  }
  template <typename T>
  static int fill_current(const uint64_t tenant_id, const bool check_deleted, T& result, SCHEMA& p, bool& is_deleted)
  {
    UNUSED(tenant_id);
    UNUSED(check_deleted);
    UNUSED(result);
    UNUSED(p);
    UNUSED(is_deleted);
    return common::OB_SUCCESS;
  };
  int add_schema(TABLE_SCHEMA& table_schema, SCHEMA& schema)
  {
    UNUSED(table_schema);
    UNUSED(schema);
    return common::OB_SUCCESS;
  }
};

template <typename TABLE_SCHEMA>
class ObSchemaRetrieveHelperBase<TABLE_SCHEMA, ObColumnSchemaV2> {
public:
  static int64_t get_schema_id(const ObColumnSchemaV2& s);
  template <typename T>
  static int fill_current(
      const uint64_t tenant_id, const bool check_deleted, T& result, ObColumnSchemaV2& p, bool& is_deleted);
  static int add_schema(TABLE_SCHEMA& table_schema, ObColumnSchemaV2& schema);
};

template <typename TABLE_SCHEMA>
class ObSchemaRetrieveHelperBase<TABLE_SCHEMA, ObConstraint> {
public:
  static int64_t get_schema_id(const ObConstraint& s);
  template <typename T>
  static int fill_current(
      const uint64_t tenant_id, const bool check_deleted, T& result, ObConstraint& p, bool& is_deleted);
  static int add_schema(TABLE_SCHEMA& table_schema, ObConstraint& schema);
};

template <typename TABLE_SCHEMA>
class ObSchemaRetrieveHelperBase<TABLE_SCHEMA, ObPartition> {
public:
  static int64_t get_schema_id(const ObPartition& s);
  template <typename T>
  static int fill_current(
      const uint64_t tenant_id, const bool check_deleted, T& result, ObPartition& p, bool& is_deleted);
  static int add_schema(TABLE_SCHEMA& table_schema, ObPartition& schema);
};

template <typename TABLE_SCHEMA>
class ObSchemaRetrieveHelperBase<TABLE_SCHEMA, ObSubPartition> {
public:
  static int64_t get_schema_id(const ObSubPartition& s);
  template <typename T>
  static int fill_current(const uint64_t tenant_id, const bool check_deleted, const bool is_subpart_template, T& result,
      ObSubPartition& p, bool& is_deleted);
  static int add_schema(TABLE_SCHEMA& table_schema, ObSubPartition& schema);
};

template <typename TABLE_SCHEMA, typename SCHEMA>
class ObSchemaRetrieveHelper {
private:
  enum Mode { SINGLE_TABLE, MULTIPLE_TABLE };

public:
  ObSchemaRetrieveHelper(TABLE_SCHEMA& table)
      : mode_(SINGLE_TABLE), index_(0), is_subpart_def_(false), table_(&table), tmp_table_(NULL), table_array_(NULL)
  {}
  ObSchemaRetrieveHelper(common::ObArray<TABLE_SCHEMA*>& tables)
      : mode_(Mode::MULTIPLE_TABLE),
        index_(0),
        is_subpart_def_(false),
        table_(NULL),
        tmp_table_(NULL),
        table_array_(&tables)
  {}

  ~ObSchemaRetrieveHelper()
  {}
  SCHEMA& get_current()
  {
    return schemas_[index_];
  }
  int64_t get_curr_schema_id();
  void rotate()
  {
    index_ = 1 - index_;
  }
  int get_table(const uint64_t table_id, TABLE_SCHEMA*& table);
  int add(SCHEMA& s);
  template <typename T>
  static int fill_current(const uint64_t tenant_id, const bool check_deleted, T& result, SCHEMA& p, bool& is_deleted);

private:
  const Mode mode_;
  int index_;
  bool is_subpart_def_;
  // for single table
  TABLE_SCHEMA* table_;
  // for multi table
  TABLE_SCHEMA* tmp_table_;
  ObArray<TABLE_SCHEMA*>* table_array_;
  SCHEMA schemas_[2];
};

template <typename TABLE_SCHEMA>
class ObSubPartSchemaRetrieveHelper {
private:
  enum Mode { SINGLE_TABLE, MULTIPLE_TABLE };

public:
  ObSubPartSchemaRetrieveHelper(
      TABLE_SCHEMA& table, const bool is_subpart_def = false, const bool is_subpart_template = true)
      : mode_(SINGLE_TABLE),
        index_(0),
        is_subpart_def_(is_subpart_def),
        table_(&table),
        tmp_table_(NULL),
        table_array_(NULL),
        partition_(NULL),
        is_subpart_template_(is_subpart_template)
  {}
  ObSubPartSchemaRetrieveHelper(
      common::ObArray<TABLE_SCHEMA*>& tables, const bool is_subpart_def = false, const bool is_subpart_template = true)
      : mode_(Mode::MULTIPLE_TABLE),
        index_(0),
        is_subpart_def_(is_subpart_def),
        table_(NULL),
        tmp_table_(NULL),
        table_array_(&tables),
        partition_(NULL),
        is_subpart_template_(is_subpart_template)
  {}

  ~ObSubPartSchemaRetrieveHelper()
  {}
  ObSubPartition& get_current()
  {
    return schemas_[index_];
  }
  int64_t get_curr_schema_id();
  void rotate()
  {
    index_ = 1 - index_;
  }
  int get_table(const uint64_t table_id, TABLE_SCHEMA*& table);
  int add(ObSubPartition& s);
  template <typename T>
  static int fill_current(const uint64_t tenant_id, const bool check_deleted, const bool is_subpart_template, T& result,
      ObSubPartition& p, bool& is_deleted);

private:
  const Mode mode_;
  int index_;
  bool is_subpart_def_;
  // for single table
  TABLE_SCHEMA* table_;
  // for multi table
  TABLE_SCHEMA* tmp_table_;
  ObArray<TABLE_SCHEMA*>* table_array_;
  ObSubPartition schemas_[2];
  // for is_subpart_template = false
  ObPartition* partition_;
  bool is_subpart_template_;
};

class VersionHisVal;
class ObSchemaRetrieveUtils {
public:
  /******************************************************************
   *
   * for full schemas
   *
   ******************************************************************/

  template <typename T, typename S>
  static int retrieve_tenant_schema(common::ObISQLClient& client, T& result, common::ObIArray<S>& tenant_schema_array);
  template <typename T, typename SCHEMA>
  static int retrieve_system_variable(const uint64_t tenant_id, T& result, SCHEMA& tenant_schema);

  template <typename T>
  static int retrieve_system_variable_obj(
      const uint64_t tenant_id, T& result, ObIAllocator& allocator, ObObj& out_var_obj);

  // for batch table
  template <typename T>
  static int retrieve_table_schema(const uint64_t tenant_id, const bool check_deleted, T& result,
      common::ObIAllocator& allocator, common::ObIArray<ObTableSchema*>& table_schema_array);

  template <typename TABLE_SCHEMA, typename SCHEMA, typename T>
  static int retrieve_schema(
      const uint64_t tenant_id, const bool check_deleted, T& result, ObArray<TABLE_SCHEMA*>& table_schema_array);

  template <typename TABLE_SCHEMA, typename T>
  static int retrieve_subpart_schema(const uint64_t tenant_id, const bool check_deleted, const bool is_subpart_template,
      T& result, ObArray<TABLE_SCHEMA*>& table_schema_array);

  template <typename T>
  static int retrieve_column_schema(const uint64_t tenant_id, const bool check_deleted, T& result,
      common::ObArray<ObTableSchema*>& table_schema_array);

  template <typename T>
  static int retrieve_constraint(const uint64_t tenant_id, const bool check_deleted, T& result,
      common::ObArray<ObTableSchema*>& table_schema_array);

  template <typename T>
  static int retrieve_constraint_column_info(const uint64_t tenant_id, T& result, ObConstraint*& cst);

  template <typename T>
  static int retrieve_aux_tables(const uint64_t tenant_id, T& result, ObIArray<ObAuxTableMetaInfo>& aux_tables);

  template <typename TABLE_SCHEMA, typename T>
  static int retrieve_part_info(const uint64_t tenant_id, const bool check_deleted, T& result,
      common::ObArray<TABLE_SCHEMA*>& table_schema_array);

  template <typename TABLE_SCHEMA, typename T>
  static int retrieve_def_subpart_info(const uint64_t tenant_id, const bool check_deleted, T& result,
      common::ObArray<TABLE_SCHEMA*>& table_schema_array);

  template <typename TABLE_SCHEMA, typename T>
  static int retrieve_subpart_info(const uint64_t tenant_id, const bool check_deleted, T& result,
      common::ObArray<TABLE_SCHEMA*>& table_schema_array);

  // for single table
  template <typename T>
  static int retrieve_table_schema(const uint64_t tenant_id, const bool check_deleted, T& result,
      common::ObIAllocator& allocator, ObTableSchema*& table_schema);
  template <typename T>
  static int retrieve_column_schema(
      const uint64_t tenant_id, const bool check_deleted, T& result, ObTableSchema*& table_schema);

  template <typename T>
  static int retrieve_constraint(
      const uint64_t tenant_id, const bool check_deleted, T& result, ObTableSchema*& table_schema);

  // for ObColumnSchema ObPartition
  template <typename TABLE_SCHEMA, typename SCHEMA, typename T>
  static int retrieve_schema(
      const uint64_t tenant_id, const bool check_deleted, T& result, TABLE_SCHEMA*& table_schema);

  template <typename TABLE_SCHEMA, typename T>
  static int retrieve_subpart_schema(const uint64_t tenant_id, const bool check_deleted, const bool is_subpart_template,
      T& result, TABLE_SCHEMA*& table_schema);

  template <typename TABLE_SCHEMA, typename T>
  static int retrieve_part_info(
      const uint64_t tenant_id, const bool check_deleted, T& result, TABLE_SCHEMA*& table_schema);

  template <typename TABLE_SCHEMA, typename T>
  static int retrieve_def_subpart_info(
      const uint64_t tenant_id, const bool check_deleted, T& result, TABLE_SCHEMA*& table_schema);

  template <typename TABLE_SCHEMA, typename T>
  static int retrieve_subpart_info(
      const uint64_t tenant_id, const bool check_deleted, T& result, TABLE_SCHEMA*& table_schema);

  template <typename T>
  static int retrieve_foreign_key_info(const uint64_t tenant_id, T& result, ObTableSchema& table_schema);

  template <typename T>
  static int retrieve_foreign_key_column_info(const uint64_t tenant_id, T& result, ObForeignKeyInfo& foreign_key_info);
#define RETRIEVE_SCHEMA_FUNC_DECLARE(SCHEMA) \
  template <typename T, typename S>          \
  static int retrieve_##SCHEMA##_schema(const uint64_t tenant_id, T& result, common::ObIArray<S>& schema_array);
  RETRIEVE_SCHEMA_FUNC_DECLARE(user);
  RETRIEVE_SCHEMA_FUNC_DECLARE(database);
  RETRIEVE_SCHEMA_FUNC_DECLARE(tablegroup);
  RETRIEVE_SCHEMA_FUNC_DECLARE(table);
  RETRIEVE_SCHEMA_FUNC_DECLARE(outline);
  RETRIEVE_SCHEMA_FUNC_DECLARE(db_priv);
  RETRIEVE_SCHEMA_FUNC_DECLARE(table_priv);
  RETRIEVE_SCHEMA_FUNC_DECLARE(synonym);
  RETRIEVE_SCHEMA_FUNC_DECLARE(udf);
  RETRIEVE_SCHEMA_FUNC_DECLARE(sequence);
  RETRIEVE_SCHEMA_FUNC_DECLARE(profile);
  RETRIEVE_SCHEMA_FUNC_DECLARE(sys_priv);
  RETRIEVE_SCHEMA_FUNC_DECLARE(obj_priv);

  RETRIEVE_SCHEMA_FUNC_DECLARE(dblink);
  template <typename AT, typename TST>
  static int retrieve_link_table_schema(
      const uint64_t tenant_id, AT& result, common::ObIAllocator& allocator, TST*& table_schema);
  template <typename T>
  static int retrieve_link_column_schema(const uint64_t tenant_id, T& result, ObTableSchema& table_schema);

  template <typename T>
  static int retrieve_tablegroup_schema(
      const uint64_t tenant_id, T& result, ObIAllocator& allocator, ObTablegroupSchema*& tablegroup_schema);

  template <typename T>
  static int retrieve_recycle_object(const uint64_t tenant_id, T& result, ObIArray<ObRecycleObject>& recycle_objs);
  template <typename T>
  static int retrieve_schema_version(T& result, VersionHisVal& version_his_val);

  /**
   * start of fill schema
   */
#define FILL_SCHEMA_FUNC_DECLARE(SCHEMA, SCHEMA_TYPE) \
  template <typename T>                               \
  static int fill_##SCHEMA##_schema(const uint64_t tenant_id, T& result, SCHEMA_TYPE& schema, bool& is_deleted);
  // for simple schema
  template <typename T>
  static int fill_tenant_schema(T& result, ObSimpleTenantSchema& schema, bool& is_deleted);
  FILL_SCHEMA_FUNC_DECLARE(user, ObSimpleUserSchema);
  FILL_SCHEMA_FUNC_DECLARE(database, ObSimpleDatabaseSchema);
  FILL_SCHEMA_FUNC_DECLARE(tablegroup, ObSimpleTablegroupSchema);
  FILL_SCHEMA_FUNC_DECLARE(table, ObSimpleTableSchemaV2);
  FILL_SCHEMA_FUNC_DECLARE(outline, ObSimpleOutlineSchema);
  FILL_SCHEMA_FUNC_DECLARE(synonym, ObSimpleSynonymSchema);
  FILL_SCHEMA_FUNC_DECLARE(udf, ObSimpleUDFSchema);
  FILL_SCHEMA_FUNC_DECLARE(sequence, ObSequenceSchema);
  FILL_SCHEMA_FUNC_DECLARE(profile, ObProfileSchema);
  FILL_SCHEMA_FUNC_DECLARE(dblink, ObDbLinkSchema);
  // for full schema
  template <typename T>
  static int fill_tenant_schema(T& result, ObTenantSchema& schema, bool& is_deleted);
  FILL_SCHEMA_FUNC_DECLARE(user, ObUserInfo);
  FILL_SCHEMA_FUNC_DECLARE(database, ObDatabaseSchema);
  FILL_SCHEMA_FUNC_DECLARE(tablegroup, ObTablegroupSchema);
  FILL_SCHEMA_FUNC_DECLARE(outline, ObOutlineInfo);
  FILL_SCHEMA_FUNC_DECLARE(db_priv, ObDBPriv);
  FILL_SCHEMA_FUNC_DECLARE(table_priv, ObTablePriv);
  FILL_SCHEMA_FUNC_DECLARE(synonym, ObSynonymInfo);
  FILL_SCHEMA_FUNC_DECLARE(sysvar, ObSysVarSchema);
  FILL_SCHEMA_FUNC_DECLARE(udf, ObUDF);
  // link table
  FILL_SCHEMA_FUNC_DECLARE(link_table, ObTableSchema);
  FILL_SCHEMA_FUNC_DECLARE(link_column, ObColumnSchemaV2);

  template <typename T>
  static int fill_sys_priv_schema(
      const uint64_t tenant_id, T& result, ObSysPriv& schema, bool& is_deleted, ObRawPriv& raw_p_id, uint64_t& option);

  template <typename T>
  static int fill_obj_priv_schema(const uint64_t tenant_id, T& result, ObObjPriv& obj_priv, bool& is_deleted,
      ObRawObjPriv& raw_p_id, uint64_t& option);

  template <typename T>
  static int fill_table_schema(
      const uint64_t tenant_id, const bool check_deleted, T& result, ObTableSchema& table_schema, bool& is_deleted);
  template <typename T>
  static int fill_column_schema(
      const uint64_t tenant_id, const bool check_deleted, T& result, ObColumnSchemaV2& column, bool& is_deleted);

  template <typename T>
  static int fill_constraint(
      const uint64_t tenant_id, const bool check_deleted, T& result, ObConstraint& constraint, bool& is_deleted);

  template <typename T>
  static int fill_constraint_column_info(const uint64_t tenant_id, T& result, uint64_t& column_id, bool& is_deleted);

  template <typename T>
  static int fill_base_part_info(const uint64_t tenant_id, const bool check_deleted, const bool is_subpart_def,
      const bool is_subpart_template, T& result, ObBasePartition& partition, bool& is_deleted);

  template <typename T>
  static int fill_part_info(
      const uint64_t tenant_id, const bool check_deleted, T& result, ObPartition& partition, bool& is_deleted);

  template <typename T>
  static int fill_def_subpart_info(
      const uint64_t tenant_id, const bool check_deleted, T& result, ObSubPartition& partition, bool& is_deleted);

  template <typename T>
  static int fill_subpart_info(
      const uint64_t tenant_id, const bool check_deleted, T& result, ObSubPartition& partition, bool& is_deleted);

  template <typename T>
  static int fill_foreign_key_info(
      const uint64_t tenant_id, uint64_t table_id, T& result, ObForeignKeyInfo& foreign_key_info, bool& is_deleted);

  template <typename T>
  static int fill_foreign_key_column_info(
      const uint64_t tenant_id, T& result, int64_t& child_column_id, int64_t& parent_column_id, bool& is_deleted);

  template <typename T>
  static int retrieve_simple_foreign_key_info(
      const uint64_t tenant_id, T& result, ObArray<ObSimpleTableSchemaV2*>& table_schema_array);

  template <typename T>
  static int get_foreign_key_id_and_name(
      const uint64_t tenant_id, T& result, bool& is_deleted, uint64_t& fk_id, ObString& fk_name, uint64_t& table_id);
  template <typename T>
  static int retrieve_simple_constraint_info(
      const uint64_t tenant_id, T& result, ObArray<ObSimpleTableSchemaV2*>& table_schema_array);

  template <typename T>
  static int get_constraint_id_and_name(const uint64_t tenant_id, T& result, bool& is_deleted, uint64_t& cst_id,
      ObString& cst_name, uint64_t& table_id, ObConstraintType& cst_type);
  template <typename T>
  static int retrieve_role_grantee_map_schema(
      const uint64_t tenant_id, T& result, const bool is_fetch_role, ObArray<ObUserInfo>& user_array);
  static int find_user_info(const uint64_t user_id, ObArray<ObUserInfo>& user_array, ObUserInfo*& user_info);
  template <typename T>
  static bool compare_user_id(const T& user_info, const uint64_t user_id);

  //===========================================================================

  template <typename T, typename SCHEMA>
  static int fill_replica_options(T& result, SCHEMA& schema);
  template <typename T>
  static int fill_recycle_object(const uint64_t tenant_id, T& result, ObRecycleObject& recycle_obj);
  template <typename T>
  static int fill_schema_operation(const uint64_t tenant_id, T& result,
      ObSchemaService::SchemaOperationSetWithAlloc& schema_operations, ObSchemaOperation& schema_op);
  template <typename SCHEMA>
  static int fill_schema_zone_region_replica_num_array(SCHEMA& schema);
  static int fill_tenant_zone_region_replica_num_array(share::schema::ObTenantSchema& tenant_schema);
  template <typename SCHEMA>
  static int fill_table_zone_region_replica_num_array(SCHEMA& table_schema);
  template <typename T>
  static T* find_table_schema(const uint64_t table_id, common::ObArray<T*>& table_schema_array);
  template <typename T>
  static int fill_temp_table_schema(const uint64_t tenant_id, T& result, ObTableSchema& table_schema);

  template <typename T>
  static int retrieve_drop_tenant_infos(T& result, ObIArray<ObDropTenantInfo>& drop_tenant_infos);

private:
  template <typename T>
  static bool compare_table_id(const T* table_schema, const uint64_t table_id);
  static int retrieve_generated_column(const ObTableSchema& table_schema, ObColumnSchemaV2& column);

  template <typename S>
  static int push_prev_array_if_has(ObIArray<S>& sys_priv_array, S& prev_priv, ObPackedPrivArray& packed_grant_privs);

  template <typename S>
  static int push_prev_obj_privs_if_has(ObIArray<S>& obj_priv_array, S& obj_priv, ObPackedObjPriv& packed_obj_privs);

  template <typename T, typename S>
  static int retrieve_sys_priv_schema_inner(const uint64_t tenant_id, T& result, ObIArray<S>& sys_priv_array);
  template <typename T, typename S>
  static int retrieve_obj_priv_schema_inner(const uint64_t tenant_id, T& result, ObIArray<S>& obj_priv_array);

  ObSchemaRetrieveUtils()
  {}
  ~ObSchemaRetrieveUtils()
  {}
};

}  // end of namespace schema
}  // end of namespace share
}  // end of namespace oceanbase

#include "ob_schema_retrieve_utils.ipp"
#endif
