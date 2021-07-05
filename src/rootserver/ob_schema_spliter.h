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

#ifndef OCEANBASE_SCHEMA_SPLIT_SPLITER_H_
#define OCEANBASE_SCHEMA_SPLIT_SPLITER_H_

#include "lib/thread/ob_async_task_queue.h"
#include "rootserver/ob_schema_split_executor.h"
#include "share/ob_common_rpc_proxy.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_table_schema.h"
#include "observer/ob_inner_sql_result.h"

namespace oceanbase {
namespace rootserver {
class ObSchemaSplitExecutor;
class ObTableSchemaSpliter {
public:
  ObTableSchemaSpliter(ObSchemaSplitExecutor& executor, share::schema::ObMultiVersionSchemaService& schema_service,
      common::ObMySQLProxy* sql_proxy, share::schema::ObTableSchema& table_schema, bool iter_sys)
      : executor_(executor),
        schema_service_(schema_service),
        sql_proxy_(sql_proxy),
        table_schema_(table_schema),
        base_sql_(),
        extra_condition_(),
        allocator_(),
        mapping_(),
        is_inited_(false),
        iter_sys_(iter_sys)
  {}
  virtual ~ObTableSchemaSpliter(){};
  virtual int init();
  virtual int set_column_name_with_tenant_id(const char* column_name);
  virtual int process();
  virtual int process(const uint64_t tenant_id);

protected:
  typedef int (*convert_func_t)(const common::ObObj& src, common::ObObj& dst, common::ObIAllocator&);

  struct MapItem {
    MapItem() : base_col_name_(), convert_func_(NULL)
    {}
    TO_STRING_KV(K(base_col_name_), KP(convert_func_));

    common::ObString base_col_name_;
    convert_func_t convert_func_;
  };

  class ObSplitTableIterator {
  public:
    ObSplitTableIterator(ObSchemaSplitExecutor& executor, common::ObMySQLProxy* sql_proxy)
        : executor_(executor),
          sql_proxy_(sql_proxy),
          base_sql_(),
          exec_tenant_id_(OB_INVALID_TENANT_ID),
          start_idx_(0),
          cur_idx_(0),
          alloc_(),
          rows_(MAX_FETCH_ROW_COUNT, ModulePageAllocator(alloc_)),
          is_inited_(false)
    {}
    virtual ~ObSplitTableIterator()
    {
      destroy();
    }
    int init(common::ObSqlString& base_sql, const uint64_t exec_tenant_id);
    int get_next_row(const common::ObNewRow*& row);

  private:
    void destroy();
    int get_next_batch();
    int check_stop();

  private:
    static const int64_t MAX_FETCH_ROW_COUNT = 5000;
    ObSchemaSplitExecutor& executor_;
    common::ObMySQLProxy* sql_proxy_;
    common::ObSqlString base_sql_;
    uint64_t exec_tenant_id_;  // tenant to execute sql
    int64_t start_idx_;
    int64_t cur_idx_;
    ObArenaAllocator alloc_;
    ObSEArray<const ObNewRow*, MAX_FETCH_ROW_COUNT> rows_;
    bool is_inited_;
  };

protected:
  virtual int build_column_mapping();
  virtual int construct_base_sql();
  virtual int add_extra_condition();
  virtual int construct_sql(const uint64_t tenant_id, common::ObSqlString& sql);
  virtual int quick_check(const uint64_t tenant_id, bool& passed);
  virtual int migrate(const uint64_t tenant_id);
  virtual int check(const uint64_t tenant_id);
  bool column_name_equal(const common::ObString& column_name, const char* target);
  int check_stop();
  virtual int init_sys_iterator(const uint64_t tenant_id, ObSplitTableIterator& iter);
  virtual int init_tenant_iterator(const uint64_t tenant_id, ObSplitTableIterator& iter);

private:
  virtual int add_convert_func(const share::schema::ObColumnSchemaV2& column, MapItem& item);
  virtual int batch_replace_rows(const uint64_t tenant_id, common::ObIArray<common::ObNewRow*>& replace_rows);
  virtual int compare_obj(const common::ObObj& obj1, const common::ObObj& obj2);

protected:
  static const int64_t BATCH_REPLACE_ROW_COUNT = 10;
  static const int64_t DEFAULT_COLUMN_NUM = 10;
  ObSchemaSplitExecutor& executor_;
  share::schema::ObMultiVersionSchemaService& schema_service_;
  common::ObMySQLProxy* sql_proxy_;
  share::schema::ObTableSchema& table_schema_;
  common::ObSqlString base_sql_;
  common::ObSqlString extra_condition_;
  common::ObArenaAllocator allocator_;
  common::ObArray<MapItem, common::ObWrapperAllocator> mapping_;
  bool is_inited_;
  bool iter_sys_;
};

#define DEF_SIMPLE_SCHEMA_SPLITER(TID, SchemaSpliter)                                                          \
  class SchemaSpliter : public ObTableSchemaSpliter {                                                          \
  public:                                                                                                      \
    SchemaSpliter(ObSchemaSplitExecutor& executor, share::schema::ObMultiVersionSchemaService& schema_service, \
        common::ObMySQLProxy* sql_proxy, share::schema::ObTableSchema& table_schema)                           \
        : ObTableSchemaSpliter(executor, schema_service, sql_proxy, table_schema, false)                       \
    {}                                                                                                         \
    virtual ~SchemaSpliter()                                                                                   \
    {}                                                                                                         \
  };
// migrate table by tenant_id & filter tenant_id
DEF_SIMPLE_SCHEMA_SPLITER(OB_ALL_CONSTRAINT_TID, ObAllConstraintSchemaSpliter);
DEF_SIMPLE_SCHEMA_SPLITER(OB_ALL_CONSTRAINT_HISTORY_TID, ObAllConstraintHistorySchemaSpliter);
DEF_SIMPLE_SCHEMA_SPLITER(OB_ALL_COLL_TYPE_TID, ObAllCollTypeSchemaSpliter);
DEF_SIMPLE_SCHEMA_SPLITER(OB_ALL_COLL_TYPE_HISTORY_TID, ObAllCollTypeHistorySchemaSpliter);
DEF_SIMPLE_SCHEMA_SPLITER(OB_ALL_DATABASE_TID, ObAllDatabaseSchemaSpliter);
DEF_SIMPLE_SCHEMA_SPLITER(OB_ALL_DATABASE_HISTORY_TID, ObAllDatabaseHistorySchemaSpliter);
DEF_SIMPLE_SCHEMA_SPLITER(OB_ALL_DATABASE_PRIVILEGE_TID, ObAllDatabasePrivilegeSchemaSpliter);
DEF_SIMPLE_SCHEMA_SPLITER(OB_ALL_DATABASE_PRIVILEGE_HISTORY_TID, ObAllDatabasePrivilegeHistorySchemaSpliter);
DEF_SIMPLE_SCHEMA_SPLITER(OB_ALL_DEF_SUB_PART_TID, ObAllDefSubPartSchemaSpliter);
DEF_SIMPLE_SCHEMA_SPLITER(OB_ALL_DEF_SUB_PART_HISTORY_TID, ObAllDefSubPartHistorySchemaSpliter);
DEF_SIMPLE_SCHEMA_SPLITER(OB_ALL_FOREIGN_KEY_TID, ObAllForeignKeySchemaSpliter);
DEF_SIMPLE_SCHEMA_SPLITER(OB_ALL_FOREIGN_KEY_COLUMN_TID, ObAllForeignKeyColumnSchemaSpliter);
DEF_SIMPLE_SCHEMA_SPLITER(OB_ALL_FOREIGN_KEY_COLUMN_HISTORY_TID, ObAllForeignKeyColumnHistorySchemaSpliter);
DEF_SIMPLE_SCHEMA_SPLITER(OB_ALL_FOREIGN_KEY_HISTORY_TID, ObAllForeignKeyHistorySchemaSpliter);
DEF_SIMPLE_SCHEMA_SPLITER(OB_ALL_FUNC_TID, ObAllFuncSchemaSpliter);
DEF_SIMPLE_SCHEMA_SPLITER(OB_ALL_FUNC_HISTORY_TID, ObAllFuncHistorySchemaSpliter);
DEF_SIMPLE_SCHEMA_SPLITER(OB_ALL_ORI_SCHEMA_VERSION_TID, ObAllOriSchemaVersionSchemaSpliter);
DEF_SIMPLE_SCHEMA_SPLITER(OB_ALL_OUTLINE_TID, ObAllOutlineSchemaSpliter);
DEF_SIMPLE_SCHEMA_SPLITER(OB_ALL_OUTLINE_HISTORY_TID, ObAllOutlineHistorySchemaSpliter);
DEF_SIMPLE_SCHEMA_SPLITER(OB_ALL_PACKAGE_TID, ObAllPackageSchemaSpliter);
DEF_SIMPLE_SCHEMA_SPLITER(OB_ALL_PACKAGE_HISTORY_TID, ObAllPackageHistorySchemaSpliter);
DEF_SIMPLE_SCHEMA_SPLITER(OB_ALL_PART_TID, ObAllPartSchemaSpliter);
DEF_SIMPLE_SCHEMA_SPLITER(OB_ALL_PART_HISTORY_TID, ObAllPartHistorySchemaSpliter);
DEF_SIMPLE_SCHEMA_SPLITER(OB_ALL_PART_INFO_TID, ObAllPartInfoSchemaSpliter);
DEF_SIMPLE_SCHEMA_SPLITER(OB_ALL_PART_INFO_HISTORY_TID, ObAllPartInfoHistorySchemaSpliter);
DEF_SIMPLE_SCHEMA_SPLITER(OB_ALL_RECYCLEBIN_TID, ObAllRecyclebinSchemaSpliter);
DEF_SIMPLE_SCHEMA_SPLITER(OB_ALL_ROUTINE_TID, ObAllRoutineSchemaSpliter);
DEF_SIMPLE_SCHEMA_SPLITER(OB_ALL_ROUTINE_HISTORY_TID, ObAllRoutineHistorySchemaSpliter);
DEF_SIMPLE_SCHEMA_SPLITER(OB_ALL_ROUTINE_PARAM_TID, ObAllRoutineParamSchemaSpliter);
DEF_SIMPLE_SCHEMA_SPLITER(OB_ALL_ROUTINE_PARAM_HISTORY_TID, ObAllRoutineParamHistorySchemaSpliter);
DEF_SIMPLE_SCHEMA_SPLITER(OB_ALL_SEQUENCE_OBJECT_TID, ObAllSequenceObjectSchemaSpliter);
DEF_SIMPLE_SCHEMA_SPLITER(OB_ALL_SEQUENCE_OBJECT_HISTORY_TID, ObAllSequenceObjectHistorySchemaSpliter);
DEF_SIMPLE_SCHEMA_SPLITER(OB_ALL_SUB_PART_TID, ObAllSubPartSchemaSpliter);
DEF_SIMPLE_SCHEMA_SPLITER(OB_ALL_SUB_PART_HISTORY_TID, ObAllSubPartHistorySchemaSpliter);
DEF_SIMPLE_SCHEMA_SPLITER(OB_ALL_SYNONYM_TID, ObAllSynonymSchemaSpliter);
DEF_SIMPLE_SCHEMA_SPLITER(OB_ALL_SYNONYM_HISTORY_TID, ObAllSynonymHistorySchemaSpliter);
DEF_SIMPLE_SCHEMA_SPLITER(OB_ALL_SYS_STAT_TID, ObAllSysStatSchemaSpliter);
DEF_SIMPLE_SCHEMA_SPLITER(OB_ALL_SYS_VARIABLE_TID, ObAllSysVariableSchemaSpliter);
DEF_SIMPLE_SCHEMA_SPLITER(OB_ALL_SYS_VARIABLE_HISTORY_TID, ObAllSysVariableHistorySchemaSpliter);
DEF_SIMPLE_SCHEMA_SPLITER(OB_ALL_TABLE_PRIVILEGE_TID, ObAllTablePrivilegeSchemaSpliter);
DEF_SIMPLE_SCHEMA_SPLITER(OB_ALL_TABLE_PRIVILEGE_HISTORY_TID, ObAllTablePrivilegeHistorySchemaSpliter);
DEF_SIMPLE_SCHEMA_SPLITER(OB_ALL_TABLEGROUP_TID, ObAllTablegroupSchemaSpliter);
DEF_SIMPLE_SCHEMA_SPLITER(OB_ALL_TABLEGROUP_HISTORY_TID, ObAllTablegroupHistorySchemaSpliter);
DEF_SIMPLE_SCHEMA_SPLITER(OB_ALL_TEMP_TABLE_TID, ObAllTempTableSchemaSpliter);
DEF_SIMPLE_SCHEMA_SPLITER(OB_ALL_TENANT_PLAN_BASELINE_TID, ObAllTenantPlanBaselineSchemaSpliter);
DEF_SIMPLE_SCHEMA_SPLITER(OB_ALL_TENANT_PLAN_BASELINE_HISTORY_TID, ObAllTenantPlanBaselineHistorySchemaSpliter);
DEF_SIMPLE_SCHEMA_SPLITER(OB_ALL_TYPE_TID, ObAllTypeSchemaSpliter);
DEF_SIMPLE_SCHEMA_SPLITER(OB_ALL_TYPE_HISTORY_TID, ObAllTypeHistorySchemaSpliter);
DEF_SIMPLE_SCHEMA_SPLITER(OB_ALL_TYPE_ATTR_TID, ObAllTypeAttrSchemaSpliter);
DEF_SIMPLE_SCHEMA_SPLITER(OB_ALL_TYPE_ATTR_HISTORY_TID, ObAllTypeAttrHistorySchemaSpliter);
DEF_SIMPLE_SCHEMA_SPLITER(OB_ALL_USER_TID, ObAllUserSchemaSpliter);
DEF_SIMPLE_SCHEMA_SPLITER(OB_ALL_USER_HISTORY_TID, ObAllUserHistorySchemaSpliter);
DEF_SIMPLE_SCHEMA_SPLITER(OB_ALL_TENANT_ROLE_GRANTEE_MAP_TID, ObAllTenantRoleGranteeMapSchemaSpliter);
DEF_SIMPLE_SCHEMA_SPLITER(OB_ALL_TENANT_ROLE_GRANTEE_MAP_HISTORY_TID, ObAllTenantRoleGranteeMapHistorySchemaSpliter);
DEF_SIMPLE_SCHEMA_SPLITER(OB_ALL_DBLINK_TID, ObAllDblinkSchemaSpliter);
DEF_SIMPLE_SCHEMA_SPLITER(OB_ALL_DBLINK_HISTORY_TID, ObAllDblinkHistorySchemaSpliter);
// table should not filter tenant_id
DEF_SIMPLE_SCHEMA_SPLITER(OB_ALL_COLUMN_STATISTIC_TID, ObAllColumnStatisticSchemaSpliter);
DEF_SIMPLE_SCHEMA_SPLITER(OB_ALL_DDL_ID_TID, ObAllDdlIdSchemaSpliter);
// table should be empty
DEF_SIMPLE_SCHEMA_SPLITER(OB_ALL_COLUMN_STAT_TID, ObAllColumnStatSchemaSpliter);
DEF_SIMPLE_SCHEMA_SPLITER(OB_ALL_HISTOGRAM_STAT_TID, ObAllHistogramStatSchemaSpliter);
DEF_SIMPLE_SCHEMA_SPLITER(OB_ALL_TABLE_STAT_TID, ObAllTableStatSchemaSpliter);
DEF_SIMPLE_SCHEMA_SPLITER(OB_ALL_TENANT_USER_FAILED_LOGIN_STAT_TID, ObAllTenantUserFailedLoginStatSchemaSpliter);
DEF_SIMPLE_SCHEMA_SPLITER(OB_ALL_TENANT_PROFILE_TID, ObAllTenantProfileSchemaSpliter);
DEF_SIMPLE_SCHEMA_SPLITER(OB_ALL_TENANT_PROFILE_HISTORY_TID, ObAllTenantProfileHistorySchemaSpliter);

#define DEF_SCHEMA_SPLITER_FILTER_INNER_TABLE(TID, SchemaSpliter)                                              \
  class SchemaSpliter : public ObTableSchemaSpliter {                                                          \
  public:                                                                                                      \
    SchemaSpliter(ObSchemaSplitExecutor& executor, share::schema::ObMultiVersionSchemaService& schema_service, \
        common::ObMySQLProxy* sql_proxy, share::schema::ObTableSchema& table_schema)                           \
        : ObTableSchemaSpliter(executor, schema_service, sql_proxy, table_schema, false)                       \
    {}                                                                                                         \
    virtual ~SchemaSpliter()                                                                                   \
    {}                                                                                                         \
                                                                                                               \
  private:                                                                                                     \
    virtual int add_extra_condition() override;                                                                \
  };
// migrate table by tenant_id & filter inner_table info
DEF_SCHEMA_SPLITER_FILTER_INNER_TABLE(OB_ALL_COLUMN_TID, ObAllColumnSchemaSpliter);
DEF_SCHEMA_SPLITER_FILTER_INNER_TABLE(OB_ALL_COLUMN_HISTORY_TID, ObAllColumnHistorySchemaSpliter);
DEF_SCHEMA_SPLITER_FILTER_INNER_TABLE(OB_ALL_TABLE_TID, ObAllTableSchemaSpliter);
DEF_SCHEMA_SPLITER_FILTER_INNER_TABLE(OB_ALL_TABLE_HISTORY_TID, ObAllTableHistorySchemaSpliter);
// special migrate, filter inner_table & split sys variable
DEF_SCHEMA_SPLITER_FILTER_INNER_TABLE(OB_ALL_DDL_OPERATION_TID, ObAllDdlOperationSchemaSpliter);

#define DEF_SCHEMA_SPLITER_FILTER_TENANT_ID(TID, SchemaSpliter)                                                \
  class SchemaSpliter : public ObTableSchemaSpliter {                                                          \
  public:                                                                                                      \
    SchemaSpliter(ObSchemaSplitExecutor& executor, share::schema::ObMultiVersionSchemaService& schema_service, \
        common::ObMySQLProxy* sql_proxy, share::schema::ObTableSchema& table_schema)                           \
        : ObTableSchemaSpliter(executor, schema_service, sql_proxy, table_schema, false)                       \
    {}                                                                                                         \
    virtual ~SchemaSpliter()                                                                                   \
    {}                                                                                                         \
                                                                                                               \
  private:                                                                                                     \
    virtual int quick_check(const uint64_t tenant_id, bool& passed) override;                                  \
    virtual int migrate(const uint64_t tenant_id) override;                                                    \
    virtual int check(const uint64_t tenant_id) override;                                                      \
  };
// special migrate, filter tenant_id
DEF_SCHEMA_SPLITER_FILTER_TENANT_ID(OB_ALL_SEQUENCE_V2_TID, ObAllSequenceV2SchemaSpliter);
DEF_SCHEMA_SPLITER_FILTER_TENANT_ID(OB_ALL_TENANT_GC_PARTITION_INFO_TID, ObAllTenantGcPartitionInfoSchemaSpliter);

// split system variable schema from tenant schema
class ObSysVarDDLOperationSchemaSpliter : public ObTableSchemaSpliter {
public:
  ObSysVarDDLOperationSchemaSpliter(ObSchemaSplitExecutor& executor,
      share::schema::ObMultiVersionSchemaService& schema_service, common::ObMySQLProxy* sql_proxy,
      share::schema::ObTableSchema& table_schema)
      : ObTableSchemaSpliter(executor, schema_service, sql_proxy, table_schema, false)
  {}
  virtual ~ObSysVarDDLOperationSchemaSpliter()
  {}

private:
  virtual int build_column_mapping() override;
  virtual int add_extra_condition() override;
};

class ObAllClogHistoryInfoV2SchemaSpliter : public ObTableSchemaSpliter {
public:
  ObAllClogHistoryInfoV2SchemaSpliter(ObSchemaSplitExecutor& executor,
      share::schema::ObMultiVersionSchemaService& schema_service, common::ObMySQLProxy* sql_proxy,
      share::schema::ObTableSchema& table_schema)
      : ObTableSchemaSpliter(executor, schema_service, sql_proxy, table_schema, false)
  {}
  virtual ~ObAllClogHistoryInfoV2SchemaSpliter()
  {}

private:
  virtual int quick_check(const uint64_t tenant_id, bool& passed) override;
  virtual int construct_sql(const uint64_t tenant_id, ObSqlString& sql) override;
};
class ObAllTenantPartitionMetaTableSchemaSpliter : public ObTableSchemaSpliter {
public:
  ObAllTenantPartitionMetaTableSchemaSpliter(ObSchemaSplitExecutor& executor,
      share::schema::ObMultiVersionSchemaService& schema_service, common::ObMySQLProxy* sql_proxy,
      share::schema::ObTableSchema& table_schema)
      : ObTableSchemaSpliter(executor, schema_service, sql_proxy, table_schema, false)
  {}
  virtual ~ObAllTenantPartitionMetaTableSchemaSpliter()
  {}

private:
  virtual int quick_check(const uint64_t tenant_id, bool& passed) override;
  virtual int construct_sql(const uint64_t tenant_id, ObSqlString& sql) override;
  virtual int process();
};

class ObAllTableV2SchemaSpliter : public ObTableSchemaSpliter {
public:
  ObAllTableV2SchemaSpliter(ObSchemaSplitExecutor& executor, share::schema::ObMultiVersionSchemaService& schema_service,
      common::ObMySQLProxy* sql_proxy, share::schema::ObTableSchema& table_schema)
      : ObTableSchemaSpliter(executor, schema_service, sql_proxy, table_schema, true)
  {}
  virtual ~ObAllTableV2SchemaSpliter()
  {}

private:
  virtual int add_convert_func(const share::schema::ObColumnSchemaV2& column_schema, MapItem& item) override;
  virtual int construct_base_sql() override;
  virtual int construct_sql(const uint64_t tenant_id, const char* tname, common::ObSqlString& sql);
  virtual int init_sys_iterator(const uint64_t tenant_id, ObSplitTableIterator& iter) override;
  virtual int init_tenant_iterator(const uint64_t tenant_id, ObSplitTableIterator& iter) override;
  virtual int quick_check(const uint64_t tenant_id, bool& passed) override;
};

class ObAllTableV2HistorySchemaSpliter : public ObTableSchemaSpliter {
public:
  ObAllTableV2HistorySchemaSpliter(ObSchemaSplitExecutor& executor,
      share::schema::ObMultiVersionSchemaService& schema_service, common::ObMySQLProxy* sql_proxy,
      share::schema::ObTableSchema& table_schema)
      : ObTableSchemaSpliter(executor, schema_service, sql_proxy, table_schema, true)
  {}
  virtual ~ObAllTableV2HistorySchemaSpliter()
  {}

private:
  virtual int add_convert_func(const share::schema::ObColumnSchemaV2& column_schema, MapItem& item) override;
  virtual int construct_base_sql() override;
  virtual int construct_sql(const uint64_t tenant_id, const char* tname, common::ObSqlString& sql);
  virtual int init_sys_iterator(const uint64_t tenant_id, ObSplitTableIterator& iter) override;
  virtual int init_tenant_iterator(const uint64_t tenant_id, ObSplitTableIterator& iter) override;
  virtual int quick_check(const uint64_t tenant_id, bool& passed) override;
};
}  // namespace rootserver
}  // namespace oceanbase
#endif  // OCEANBASE_SCHEMA_SPLITER_H
