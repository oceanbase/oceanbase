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

#define USING_LOG_PREFIX RS
#include "rootserver/parallel_ddl/ob_table_helper.h"
#include "rootserver/parallel_ddl/ob_index_name_checker.h"
#include "rootserver/ob_lob_meta_builder.h"
#include "rootserver/ob_lob_piece_builder.h"
#include "rootserver/ob_table_creator.h"
#include "rootserver/ob_balance_group_ls_stat_operator.h"
#include "rootserver/freeze/ob_major_freeze_helper.h"
#include "share/ob_rpc_struct.h"
#include "share/ob_debug_sync_point.h"
#include "share/sequence/ob_sequence_option_builder.h" // ObSequenceOptionBuilder
#include "share/schema/ob_table_sql_service.h"
#include "share/schema/ob_security_audit_sql_service.h"
#include "share/schema/ob_sequence_sql_service.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "sql/resolver/ob_resolver_utils.h"

using namespace oceanbase::lib;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::rootserver;



ObTableHelper::ObTableHelper(
  share::schema::ObMultiVersionSchemaService *schema_service,
  const uint64_t tenant_id,
  const char* parallel_ddl_type,
  bool enable_ddl_parallel)
  : ObDDLHelper(schema_service, tenant_id, parallel_ddl_type, enable_ddl_parallel)
{}
// create audit schema for table/sequence
int ObTableHelper::generate_audit_schema_(const common::ObArray<ObTableSchema> &new_tables,
                                          const common::ObArray<ObSequenceSchema *> &sequences,
                                          common::ObArray<ObSAuditSchema *> &new_audits)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(new_tables.count() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected table cnt", KR(ret), K(new_tables.count()));
  } else {
    // check if data table's columns are matched with existed mock fk parent table
    const ObTableSchema &data_table = new_tables.at(0);
    ObArray<ObSAuditSchema> audits;
    if (OB_FAIL(schema_guard_wrapper_.get_default_audit_schemas(audits))) {
      LOG_WARN("fail to get audits", KR(ret));
    } else if (!audits.empty()) {
      ObSAuditSchema *new_audit_schema = NULL;
      for (int64_t i = 0; OB_SUCC(ret) && i < audits.count(); i++) {
        const ObSAuditSchema &audit = audits.at(i);
        if (audit.is_access_operation_for_sequence()) {
          for (int64_t j = 0; OB_SUCC(ret) && j < sequences.count(); j++) {
            const ObSequenceSchema *new_sequence = sequences.at(j);
            if (OB_ISNULL(new_sequence)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("new sequence is null", KR(ret));
            } else if (OB_FAIL(ObSchemaUtils::alloc_schema(allocator_, new_audit_schema))) {
              LOG_WARN("fail to alloc schema", KR(ret));
            } else if (OB_FAIL(new_audit_schema->assign(audit))) {
              LOG_WARN("fail to assign audit", KR(ret));
            } else {
              new_audit_schema->set_audit_type(AUDIT_SEQUENCE);
              new_audit_schema->set_owner_id(new_sequence->get_sequence_id());
              if (OB_FAIL(new_audits.push_back(new_audit_schema))) {
                LOG_WARN("fail to push back audit", KR(ret), KPC(new_audit_schema));
              }
            }
          } // end for
        }
        if (OB_FAIL(ret)) {
        } else if (audit.is_access_operation_for_table()) {
          if (OB_FAIL(ObSchemaUtils::alloc_schema(allocator_, new_audit_schema))) {
            LOG_WARN("fail to alloc schema", KR(ret));
          } else if (OB_FAIL(new_audit_schema->assign(audit))) {
            LOG_WARN("fail to assign audit", KR(ret));
          } else {
            new_audit_schema->set_audit_type(AUDIT_TABLE);
            new_audit_schema->set_owner_id(data_table.get_table_id());
            if (OB_FAIL(new_audits.push_back(new_audit_schema))) {
              LOG_WARN("fail to push back audit", KR(ret), KPC(new_audit_schema));
            }
          }
        }
      } // end for

      if (OB_SUCC(ret) && !new_audits.empty()) {
        ObIDGenerator id_generator;
        const uint64_t object_cnt = new_audits.count();
        uint64_t object_id = OB_INVALID_ID;
        if (OB_FAIL(gen_object_ids_(object_cnt, id_generator))) {
          LOG_WARN("fail to gen object ids", KR(ret), K_(tenant_id), K(object_cnt));
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < new_audits.count(); i++) {
          ObSAuditSchema *audit = new_audits.at(i);
          if (OB_ISNULL(audit)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("audit is null", KR(ret));
          } else if (OB_FAIL(id_generator.next(object_id))) {
            LOG_WARN("fail to get next object_id", KR(ret));
          } else {
            audit->set_audit_id(object_id);
          }
        } // end for
      }
    }
  }
  return ret;
}

int ObTableHelper::try_replace_mock_fk_parent_table_(
                   const uint64_t replace_mock_fk_parent_table_id,
                   const common::ObArray<ObTableSchema> &new_tables,
                   ObMockFKParentTableSchema *&new_mock_fk_parent_table)
{
  int ret = OB_SUCCESS;
  new_mock_fk_parent_table = NULL;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_INVALID_ID == replace_mock_fk_parent_table_id) {
    // do nothing
  } else if (OB_UNLIKELY(new_tables.count() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected table cnt", KR(ret), K(new_tables.count()));
  } else {
    // check if data table's columns are matched with existed mock fk parent table
    const ObTableSchema &data_table = new_tables.at(0);

    ObArray<const share::schema::ObTableSchema*> index_schemas;
    for (int64_t i = 1; OB_SUCC(ret) && i < new_tables.count(); ++i) {
      if (new_tables.at(i).is_unique_index()
          && OB_FAIL(index_schemas.push_back(&new_tables.at(i)))) {
        LOG_WARN("failed to push back to index_schemas", KR(ret));
      }
    } // end for

    const ObMockFKParentTableSchema *mock_fk_parent_table = NULL;
    if (FAILEDx(schema_guard_wrapper_.get_mock_fk_parent_table_schema(
        replace_mock_fk_parent_table_id, mock_fk_parent_table))) {
      LOG_WARN("fail to get mock fk parent table schema",
               KR(ret), K_(tenant_id), K(replace_mock_fk_parent_table_id));
    } else if (OB_ISNULL(mock_fk_parent_table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mock fk parent table not exist",
               KR(ret), K_(tenant_id), K(replace_mock_fk_parent_table_id));
    } else if (OB_FAIL(check_fk_columns_type_for_replacing_mock_fk_parent_table_(
               data_table, *mock_fk_parent_table))) {
      LOG_WARN("fail to check if data table is matched with mock_fk_parent_table", KR(ret));
    } else if (OB_FAIL(ObSchemaUtils::alloc_schema(
               allocator_, *mock_fk_parent_table, new_mock_fk_parent_table))) {
      LOG_WARN("fail to alloc schema", KR(ret), KPC(mock_fk_parent_table));
    } else {
      (void) new_mock_fk_parent_table->set_operation_type(
             ObMockFKParentTableOperationType::MOCK_FK_PARENT_TABLE_OP_REPLACED_BY_REAL_PREANT_TABLE);
      const ObIArray<ObForeignKeyInfo> &ori_mock_fk_infos_array = mock_fk_parent_table->get_foreign_key_infos();
      // modify the parent column id of fk，make it fit with real parent table
      // mock_column_id -> column_name -> real_column_id
      for (int64_t i = 0; OB_SUCC(ret) && i < ori_mock_fk_infos_array.count(); ++i) {
        const ObForeignKeyInfo &ori_foreign_key_info = mock_fk_parent_table->get_foreign_key_infos().at(i);
        ObForeignKeyInfo &new_foreign_key_info = new_mock_fk_parent_table->get_foreign_key_infos().at(i);
        new_foreign_key_info.parent_column_ids_.reuse();
        new_foreign_key_info.fk_ref_type_ = FK_REF_TYPE_INVALID;
        new_foreign_key_info.is_parent_table_mock_ = false;
        new_foreign_key_info.parent_table_id_ = data_table.get_table_id();
        // replace parent table columns
        for (int64_t j = 0;  OB_SUCC(ret) && j < ori_foreign_key_info.parent_column_ids_.count(); ++j) {
          bool is_column_exist = false;
          uint64_t mock_parent_table_column_id = ori_foreign_key_info.parent_column_ids_.at(j);
          ObString column_name;
          const ObColumnSchemaV2 *col_schema = NULL;
          (void) mock_fk_parent_table->get_column_name_by_column_id(
                 mock_parent_table_column_id, column_name, is_column_exist);
          if (!is_column_exist) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("column is not exist", KR(ret), K(mock_parent_table_column_id), KPC(mock_fk_parent_table));
          } else if (OB_ISNULL(col_schema = data_table.get_column_schema(column_name))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get column schema failed", KR(ret), K(column_name));
          } else if (OB_FAIL(new_foreign_key_info.parent_column_ids_.push_back(col_schema->get_column_id()))) {
            LOG_WARN("push_back to parent_column_ids failed", KR(ret), K(col_schema->get_column_id()));
          }
        } // end for
        // check and mofidy ref cst type and ref cst id of fk
        const ObRowkeyInfo &rowkey_info = data_table.get_rowkey_info();
        common::ObArray<uint64_t> pk_column_ids;
        for (int64_t j = 0; OB_SUCC(ret) && j < rowkey_info.get_size(); ++j) {
          uint64_t column_id = 0;
          const ObColumnSchemaV2 *col_schema = NULL;
          if (OB_FAIL(rowkey_info.get_column_id(j, column_id))) {
            LOG_WARN("fail to get rowkey info", KR(ret), K(j), K(rowkey_info));
          } else if (OB_ISNULL(col_schema = data_table.get_column_schema(column_id))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get index column schema failed", K(ret));
          } else if (col_schema->is_hidden() || col_schema->is_shadow_column()) {
            // do nothing
          } else if (OB_FAIL(pk_column_ids.push_back(col_schema->get_column_id()))) {
            LOG_WARN("push back column_id failed", KR(ret), K(col_schema->get_column_id()));
          }
        } // end for
        bool is_match = false;
        if (FAILEDx(sql::ObResolverUtils::check_match_columns(
            pk_column_ids, new_foreign_key_info.parent_column_ids_, is_match))) {
          LOG_WARN("check_match_columns failed", KR(ret));
        } else if (is_match) {
          new_foreign_key_info.fk_ref_type_ = FK_REF_TYPE_PRIMARY_KEY;
        } else { // pk is not match, check if uk match
          if (OB_FAIL(ddl_service_->get_uk_cst_id_for_replacing_mock_fk_parent_table(
              index_schemas, new_foreign_key_info))) {
            LOG_WARN("fail to get_uk_cst_id_for_replacing_mock_fk_parent_table", KR(ret));
          } else if (FK_REF_TYPE_INVALID == new_foreign_key_info.fk_ref_type_) {
            ret = OB_ERR_CANNOT_ADD_FOREIGN;
            LOG_WARN("fk_ref_type is invalid", KR(ret), KPC(mock_fk_parent_table));
          }
        }
      }
    }
  }
  return ret;
}

int ObTableHelper::check_fk_columns_type_for_replacing_mock_fk_parent_table_(
    const ObTableSchema &parent_table_schema,
    const ObMockFKParentTableSchema &mock_parent_table_schema)
{
  int ret = OB_SUCCESS;
  bool is_oracle_mode = false;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(parent_table_schema.check_if_oracle_compat_mode(is_oracle_mode))) {
    LOG_WARN("check if oracle compat mode failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < mock_parent_table_schema.get_foreign_key_infos().count(); ++i) {
    const ObTableSchema *child_table_schema = NULL;
    const ObForeignKeyInfo &fk_info = mock_parent_table_schema.get_foreign_key_infos().at(i);
    if (OB_FAIL(schema_guard_wrapper_.get_table_schema(fk_info.child_table_id_, child_table_schema))) {
      LOG_WARN("table is not exist", KR(ret), K(fk_info));
    } else if (OB_ISNULL(child_table_schema)) {
      ret = OB_ERR_PARALLEL_DDL_CONFLICT;
      LOG_WARN("child table schema is null, need retry", KR(ret), K(fk_info));
    } else {
      // prepare params for check_foreign_key_columns_type
      ObArray<ObString> child_columns;
      ObArray<ObString> parent_columns;
      bool is_column_exist = false;
      for (int64_t j = 0; OB_SUCC(ret) && j < fk_info.child_column_ids_.count(); ++j) {
        ObString child_column_name;
        const ObColumnSchemaV2 *child_col = child_table_schema->get_column_schema(fk_info.child_column_ids_.at(j));
        if (OB_ISNULL(child_col)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column is not exist", KR(ret), K(fk_info));
        } else if (OB_FAIL(child_columns.push_back(child_col->get_column_name_str()))) {
          LOG_WARN("fail to push_back to child_columns", KR(ret), KPC(child_col));
        }
      } // end for
      for (int64_t j = 0; OB_SUCC(ret) && j < fk_info.parent_column_ids_.count(); ++j) {
        ObString parent_column_name;
        (void) mock_parent_table_schema.get_column_name_by_column_id(
               fk_info.parent_column_ids_.at(j), parent_column_name, is_column_exist);
        if (!is_column_exist) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column is not exist", KR(ret), K(fk_info));
        } else if (OB_FAIL(parent_columns.push_back(parent_column_name))) {
          LOG_WARN("fail to push_back to real_parent_table_schema_columns", KR(ret), K(fk_info));
        }
      } // end for
      if (FAILEDx(sql::ObResolverUtils::check_foreign_key_columns_type(
          !is_oracle_mode/*is_mysql_compat_mode*/,
          *child_table_schema,
          parent_table_schema,
          child_columns,
          parent_columns,
          NULL))) {
        ret = OB_ERR_CANNOT_ADD_FOREIGN;
        LOG_WARN("Failed to check_foreign_key_columns_type", KR(ret));
      }
    }
  } // end for
  return ret;
}

int ObTableHelper::create_audits_(common::ObArray<ObSAuditSchema *> new_audits)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service_impl = NULL;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_ISNULL(schema_service_impl = schema_service_->get_schema_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service impl is null", KR(ret));
  } else {
    common::ObSqlString public_sql_string;
    int64_t new_schema_version = OB_INVALID_VERSION;
    for (int64_t i = 0; OB_SUCC(ret) && i < new_audits.count(); i++) {
      ObSAuditSchema *new_audit = new_audits.at(i);
      if (OB_ISNULL(new_audit)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("audit is null", KR(ret));
      } else if (OB_FAIL(schema_service_->gen_new_schema_version(tenant_id_, new_schema_version))) {
        LOG_WARN("fail to gen new schema_version", KR(ret), K_(tenant_id));
      } else if (FALSE_IT(new_audit->set_schema_version(new_schema_version))) {
      } else if (OB_FAIL(schema_service_impl->get_audit_sql_service().handle_audit_metainfo(
                 *new_audit,
                 AUDIT_MT_ADD,
                 false,
                 new_schema_version,
                 NULL,
                 trans_,
                 public_sql_string))) {
        LOG_WARN("fail to add audit", KR(ret), K_(tenant_id), K(new_audit));
      }
    } // end for
  }
  return ret;
}

int ObTableHelper::create_tables_(common::ObArray<ObTableSchema> &new_tables,
                                  const ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service_impl = NULL;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_ISNULL(schema_service_impl = schema_service_->get_schema_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service impl is null", KR(ret));
  } else {
    int64_t new_schema_version = OB_INVALID_VERSION;
    for (int64_t i = 0; OB_SUCC(ret) && i < new_tables.count(); i++) {
      ObTableSchema &new_table = new_tables.at(i);
      const ObString *ddl_stmt = (0 == i) ? ddl_stmt_str : NULL;
      const bool need_sync_schema_version = (new_tables.count() - 1 == i);
      if (OB_FAIL(schema_service_->gen_new_schema_version(tenant_id_, new_schema_version))) {
          LOG_WARN("fail to gen new schema_version", KR(ret), K_(tenant_id));
      } else if (FALSE_IT(new_table.set_schema_version(new_schema_version))) {
      } else if (OB_FAIL(schema_service_impl->get_table_sql_service().create_table(
                 new_table,
                 trans_,
                 ddl_stmt,
                 need_sync_schema_version,
                 false/*is_truncate_table*/))) {
        LOG_WARN("fail to create table", KR(ret), K(new_table));
      } else if (OB_FAIL(schema_service_impl->get_table_sql_service().insert_temp_table_info(
                 trans_, new_table))) {
        LOG_WARN("insert_temp_table_info failed", KR(ret), K(new_table));
      }
    } // end for
  }
  return ret;
}

int ObTableHelper::deal_with_mock_fk_parent_tables_(
      const uint64_t replace_mock_fk_parent_table_id,
      const common::ObArray<ObTableSchema> &new_tables,
      common::ObArray<ObMockFKParentTableSchema *> &new_mock_fk_parent_tables)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service_impl = NULL;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_ISNULL(schema_service_impl = schema_service_->get_schema_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service impl is null", KR(ret));
  } else if (OB_UNLIKELY(new_tables.count() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected table cnt", KR(ret), K(new_tables.count()));
  } else {
    const ObTableSchema &data_table = new_tables.at(0);
    const uint64_t data_table_id = data_table.get_table_id();
    int64_t new_schema_version = OB_INVALID_VERSION;
    for (int64_t i = 0; OB_SUCC(ret) && i < new_mock_fk_parent_tables.count(); i++) {
      ObMockFKParentTableSchema *new_mock_fk_parent_table = new_mock_fk_parent_tables.at(i);
      if (OB_ISNULL(new_mock_fk_parent_table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("mock fk parent table is null", KR(ret));
      } else if (OB_FAIL(schema_service_->gen_new_schema_version(tenant_id_, new_schema_version))) {
          LOG_WARN("fail to gen new schema_version", KR(ret), K_(tenant_id));
      } else {
        new_mock_fk_parent_table->set_schema_version(new_schema_version);
        ObMockFKParentTableOperationType operation_type = new_mock_fk_parent_table->get_operation_type();
        if (MOCK_FK_PARENT_TABLE_OP_CREATE_TABLE_BY_ADD_FK_IN_CHILD_TBALE == operation_type) {
          // 1. create table: mock fk parent table doesn't exist.
          if (OB_FAIL(schema_service_impl->get_table_sql_service().add_mock_fk_parent_table(
              &trans_, *new_mock_fk_parent_table, false /*need_update_foreign_key*/))) {
            LOG_WARN("fail to add mock fk parent table", KR(ret), KPC(new_mock_fk_parent_table));
          }
        } else if (MOCK_FK_PARENT_TABLE_OP_UPDATE_SCHEMA_VERSION == operation_type
                   || MOCK_FK_PARENT_TABLE_OP_ADD_COLUMN == operation_type) {
          // 2. alter table: mock fk parent table has new child table.
          if (OB_FAIL(schema_service_impl->get_table_sql_service().alter_mock_fk_parent_table(
                      &trans_, *new_mock_fk_parent_table))) {
            LOG_WARN("fail to alter mock fk parent table", KR(ret), KPC(new_mock_fk_parent_table));
          }
        } else if (MOCK_FK_PARENT_TABLE_OP_REPLACED_BY_REAL_PREANT_TABLE == operation_type) {
          // 3. replace table: replace existed mock fk parent table with data table
          const ObMockFKParentTableSchema *ori_mock_fk_parent_table = NULL;
          if (OB_UNLIKELY(OB_INVALID_ID == replace_mock_fk_parent_table_id)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid replace mock fk parent table id", KR(ret), K(replace_mock_fk_parent_table_id));
          } else if (OB_FAIL(schema_guard_wrapper_.get_mock_fk_parent_table_schema(
            replace_mock_fk_parent_table_id, ori_mock_fk_parent_table))) {
            LOG_WARN("fail to get mock fk parent table schema",
                     KR(ret), K_(tenant_id), K(replace_mock_fk_parent_table_id));
          } else if (OB_ISNULL(ori_mock_fk_parent_table)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("mock fk parent table not exist, unexpected",
                     KR(ret), K_(tenant_id), K(replace_mock_fk_parent_table_id));
          } else {
            // 3.1. drop mock fk parent table.
            // 3.2. update foreign keys from mock fk parent table.
            if (OB_FAIL(schema_service_impl->get_table_sql_service().replace_mock_fk_parent_table(
                        &trans_, *new_mock_fk_parent_table, ori_mock_fk_parent_table))) {
              LOG_WARN("fail to replace mock fk parent table", KR(ret), KPC(new_mock_fk_parent_table));
            }

            // 3.3. update child new_tables' schema version.
            for (int64_t j = 0; OB_SUCC(ret) && j < new_mock_fk_parent_table->get_foreign_key_infos().count(); j++) {
              const ObForeignKeyInfo &foreign_key = new_mock_fk_parent_table->get_foreign_key_infos().at(j);
              const uint64_t child_table_id = foreign_key.child_table_id_;
              const ObTableSchema *child_table = NULL;
              if (OB_FAIL(schema_guard_wrapper_.get_table_schema(child_table_id, child_table))) {
                LOG_WARN("fail to get table schema", KR(ret), K_(tenant_id), K(child_table_id));
              } else if (OB_ISNULL(child_table)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("child table is not exist", KR(ret), K_(tenant_id), K(child_table_id));
              } else if (OB_FAIL(schema_service_impl->get_table_sql_service().update_data_table_schema_version(
                          trans_, tenant_id_, child_table_id, child_table->get_in_offline_ddl_white_list()))) {
                LOG_WARN("fail to update child table's schema version", KR(ret), K_(tenant_id), K(child_table_id));
              }
            } // end for

            // 3.4. update data table's schema version at last.
            if (FAILEDx(schema_service_impl->get_table_sql_service().update_data_table_schema_version(
                        trans_, tenant_id_, data_table_id, false/*in_offline_ddl_white_list*/))) {
              LOG_WARN("fail to update data table's schema version", KR(ret), K_(tenant_id), K(data_table_id));
            }
          }
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("mock fk parent table operation type is not supported", KR(ret), K(operation_type));
        }
      }
    } // end for
  }
  return ret;
}

int ObTableHelper::create_tablets_(const common::ObArray<ObTableSchema> &new_tables)
{
  int ret = OB_SUCCESS;
  SCN frozen_scn;
  ObSchemaGetterGuard schema_guard;
  ObSchemaService *schema_service_impl = NULL;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_ISNULL(schema_service_impl = schema_service_->get_schema_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service impl is null", KR(ret));
  } else if (OB_FAIL(ObMajorFreezeHelper::get_frozen_scn(tenant_id_, frozen_scn))) {
    LOG_WARN("failed to get frozen status for create tablet", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", KR(ret), K_(tenant_id));
  } else if (OB_UNLIKELY(new_tables.count() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected table cnt", KR(ret), K(new_tables.count()));
  } else {
    const ObTableSchema &data_table = new_tables.at(0);
    ObTableCreator table_creator(
                   tenant_id_,
                   frozen_scn,
                   trans_);
    // TODO:(yanmu.ztl)
    // schema_guard is used to get primary table in tablegroup or data table for local index.
    // - primary table may be incorrect when ddl execute concurrently.
    ObNewTableTabletAllocator new_table_tablet_allocator(
                              tenant_id_,
                              schema_guard,
                              sql_proxy_,
                              true /*use parallel ddl*/);
    const ObTablegroupSchema *data_tablegroup_schema = NULL; // keep NULL if no tablegroup
    int64_t last_schema_version = OB_INVALID_VERSION;
    ObSchemaVersionGenerator *tsi_generator = GET_TSI(TSISchemaVersionGenerator);
    if (OB_FAIL(table_creator.init(true/*need_tablet_cnt_check*/))) {
      LOG_WARN("fail to init table creator", KR(ret));
    } else if (OB_FAIL(new_table_tablet_allocator.init())) {
      LOG_WARN("fail to init new table tablet allocator", KR(ret));
    } else if (OB_ISNULL(tsi_generator)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tsi schema version generator is null", KR(ret));
    } else if (OB_FAIL(tsi_generator->get_current_version(last_schema_version))) {
      LOG_WARN("fail to get end version", KR(ret), K_(tenant_id));
    } else if (OB_UNLIKELY(last_schema_version <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("last schema version is invalid", KR(ret), K_(tenant_id), K(last_schema_version));
    } else if (OB_INVALID_ID != data_table.get_tablegroup_id()) {
      if (OB_FAIL(schema_guard_wrapper_.get_tablegroup_schema(
          data_table.get_tablegroup_id(),
          data_tablegroup_schema))) {
        LOG_WARN("get tablegroup_schema failed", KR(ret), K(data_table));
      } else if (OB_ISNULL(data_tablegroup_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("data_tablegroup_schema is null", KR(ret), K(data_table));
      }
    }
    if (OB_FAIL(ret)) {
    } else {
      ObArray<const ObTableSchema*> schemas;
      common::ObArray<share::ObLSID> ls_id_array;
      for (int64_t i = 0; OB_SUCC(ret) && i < new_tables.count(); i++) {
        const ObTableSchema &new_table = new_tables.at(i);
        const uint64_t table_id = new_table.get_table_id();
        if (!new_table.has_tablet()) {
          // eg. external table ...
        } else if (!new_table.is_global_index_table()) {
          if (OB_FAIL(schemas.push_back(&new_table))) {
            LOG_WARN("fail to push back new table", KR(ret));
          }
        } else {
          if (OB_FAIL(new_table_tablet_allocator.prepare(trans_, new_table, data_tablegroup_schema))) {
            LOG_WARN("fail to prepare ls for global index", KR(ret), K(new_table));
          } else if (OB_FAIL(new_table_tablet_allocator.get_ls_id_array(ls_id_array))) {
            LOG_WARN("fail to get ls id array", KR(ret));
          } else if (OB_FAIL(table_creator.add_create_tablets_of_table_arg(
                     new_table, ls_id_array))) {
            LOG_WARN("create table partitions failed", KR(ret), K(new_table));
          }
        }
        //TODO:(yanmu.ztl) can be optimized into one sql
        if (FAILEDx(schema_service_impl->get_table_sql_service().insert_ori_schema_version(
                    trans_, tenant_id_, table_id, last_schema_version))) {
          LOG_WARN("fail to get insert ori schema version",
                   KR(ret), K_(tenant_id), K(table_id), K(last_schema_version));
        }
      } // end for

      if (OB_FAIL(ret)) {
      } else if (schemas.count() > 0) {
        const ObTableSchema &data_table = new_tables.at(0);
        if (OB_FAIL(new_table_tablet_allocator.prepare(trans_, data_table, data_tablegroup_schema))) {
          LOG_WARN("fail to prepare ls for data table", KR(ret));
        } else if (OB_FAIL(new_table_tablet_allocator.get_ls_id_array(ls_id_array))) {
          LOG_WARN("fail to get ls id array", KR(ret));
        } else if (OB_FAIL(table_creator.add_create_tablets_of_tables_arg(
                   schemas, ls_id_array))) {
          LOG_WARN("create table partitions failed", KR(ret), K(data_table));
        } else if (OB_FAIL(table_creator.execute())) {
          LOG_WARN("execute create partition failed", KR(ret));
        }
      }
    }
    // finish() will do nothing here, can be removed
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = new_table_tablet_allocator.finish(OB_SUCCESS == ret))) {
      LOG_WARN("fail to finish new table tablet allocator", KR(tmp_ret));
    }
  }
  RS_TRACE(create_tablets);
  return ret;
}


int ObTableHelper::inner_calc_schema_version_cnt_(const common::ObArray<ObTableSchema> &new_tables,
                                                  const common::ObArray<ObSequenceSchema *> &new_sequences,
                                                  const common::ObArray<ObSAuditSchema *> &new_audits,
                                                  const common::ObArray<ObMockFKParentTableSchema *> &new_mock_fk_parent_tables)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(new_tables.count() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected table cnt", KR(ret), K(new_tables.count()));
  } else {
    const ObTableSchema &data_table = new_tables.at(0);
    // 0. data table
    schema_version_cnt_ = 1; // init

    // 1. sequence
    schema_version_cnt_ += new_sequences.count();

    // 2. audit
    schema_version_cnt_ += new_audits.count();

    // 3. create index/lob table
    if (OB_SUCC(ret) && new_tables.count() > 1) {
      schema_version_cnt_ += (new_tables.count() - 1);
      // update data table schema version
      schema_version_cnt_++;
    }

    // 4. foreign key (without mock fk parent table)
    if (OB_SUCC(ret)) {
    // this logic is duplicated because of add_foreign_key() will also update data table's schema_version.
    // schema_version_cnt_ += data_table.get_depend_table_ids();
      const ObIArray<ObForeignKeyInfo> &foreign_key_infos = data_table.get_foreign_key_infos();
      for (int64_t i = 0; OB_SUCC(ret) && i < foreign_key_infos.count(); i++) {
        const ObForeignKeyInfo &foreign_key_info = foreign_key_infos.at(i);
        if (foreign_key_info.is_modify_fk_state_) {
          continue;
        } else if (!foreign_key_info.is_parent_table_mock_) {
          schema_version_cnt_++;
          // TODO(yanmu.ztl): can be optimized in the following cases:
          // - self reference
          // - foreign keys has same parent table.
        }
      } // end for
    }

    // 5. mock fk parent table
    if (OB_SUCC(ret)) {
      // schema version for new mock fk parent tables
      schema_version_cnt_ += new_mock_fk_parent_tables.count();
      for (int64_t i = 0; OB_SUCC(ret) && i < new_mock_fk_parent_tables.count(); i++) {
        const ObMockFKParentTableSchema *new_mock_fk_parent_table = new_mock_fk_parent_tables.at(i);
        if (OB_ISNULL(new_mock_fk_parent_table)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("mock fk parent table is null", KR(ret));
        } else if (MOCK_FK_PARENT_TABLE_OP_CREATE_TABLE_BY_ADD_FK_IN_CHILD_TBALE
                   == new_mock_fk_parent_table->get_operation_type()) {
          // skip
        } else if (MOCK_FK_PARENT_TABLE_OP_UPDATE_SCHEMA_VERSION
                   == new_mock_fk_parent_table->get_operation_type()
                   || MOCK_FK_PARENT_TABLE_OP_ADD_COLUMN
                   == new_mock_fk_parent_table->get_operation_type()) {
          // update new mock fk parent table(is useless here, just to be compatible with other logic)
          schema_version_cnt_++;
        } else if (MOCK_FK_PARENT_TABLE_OP_REPLACED_BY_REAL_PREANT_TABLE
                   == new_mock_fk_parent_table->get_operation_type()) {
          // update foreign keys' schema version.
          schema_version_cnt_++;
          // update child tables' schema version.
          // TODO(yanmu.ztl): can be optimized when child table is duplicated.
          schema_version_cnt_ += (new_mock_fk_parent_table->get_foreign_key_infos().count());
          // update data table's schema version at last.
          schema_version_cnt_++;
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("not supported operation type", KR(ret), KPC(new_mock_fk_parent_table));
        }
      } // end for
    }

    // 6. for 1503 boundary ddl operation
    schema_version_cnt_++;
  }
  return ret;
}

int ObTableHelper::create_schemas_(common::ObArray<ObTableSchema> &new_tables,
                                   const ObString *ddl_stmt_str,
                                   const common::ObArray<ObSequenceSchema *> &new_sequences,
                                   const common::ObArray<ObSAuditSchema *> &new_audits,
                                   const uint64_t replace_mock_fk_parent_table_id,
                                   common::ObArray<ObMockFKParentTableSchema *> &new_mock_fk_parent_tables)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(create_tables_(new_tables, ddl_stmt_str))) {
    LOG_WARN("fail to create tables", KR(ret));
  } else if (OB_FAIL(create_sequences_(new_sequences))) {
    LOG_WARN("fail to create sequences", KR(ret));
  } else if (OB_FAIL(create_audits_(new_audits))) {
    LOG_WARN("fail to create tables", KR(ret));
  } else if (OB_FAIL(deal_with_mock_fk_parent_tables_(replace_mock_fk_parent_table_id,
                                                      new_tables,
                                                      new_mock_fk_parent_tables))) {
    LOG_WARN("fail to deal with mock fk parent tables", KR(ret));
  }
  return ret;
}

int ObTableHelper::create_sequences_(const common::ObArray<ObSequenceSchema *> &new_sequences)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service_impl = NULL;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (new_sequences.count() <= 0) {
    // skip
  } else if (OB_ISNULL(schema_service_impl = schema_service_->get_schema_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service impl is null", KR(ret));
  } else {
    ObString *ddl_stmt_str = NULL;
    uint64_t *old_sequence_id = NULL; // means don't sync sequence value
    int64_t new_schema_version = OB_INVALID_VERSION;
    for (int64_t i = 0; OB_SUCC(ret) && i < new_sequences.count(); i++) {
      ObSequenceSchema *new_sequence = new_sequences.at(i);
      if (OB_ISNULL(new_sequence)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("new sequence is null", KR(ret));
      } else if (OB_FAIL(schema_service_->gen_new_schema_version(tenant_id_, new_schema_version))) {
        LOG_WARN("fail to gen new schema_version", KR(ret), K_(tenant_id));
      } else if (FALSE_IT(new_sequence->set_schema_version(new_schema_version))) {
      } else if (OB_FAIL(schema_service_impl->get_sequence_sql_service().insert_sequence(
                 *new_sequence, &trans_, ddl_stmt_str, old_sequence_id))) {
        LOG_WARN("fail to create sequence", KR(ret), KPC(new_sequence));
      }
    } // end for
  }
  return ret;
}

void ObTableHelper::adjust_create_if_not_exist_(int &ret, bool if_not_exist, bool &do_nothing)
{
  if (OB_ERR_TABLE_EXIST == ret) {
    //create table if not exist xx like (...)
    if (if_not_exist) {
      do_nothing = true;
      ret = OB_SUCCESS;
    }
  }
}

int ObTableHelper::inner_create_table_(common::ObArray<ObTableSchema> &new_tables,
                                       const ObString *ddl_stmt_str,
                                       const common::ObArray<ObSequenceSchema *> &new_sequences,
                                       const common::ObArray<ObSAuditSchema *> &new_audits,
                                       const uint64_t replace_mock_fk_parent_table_id,
                                       common::ObArray<ObMockFKParentTableSchema *> &new_mock_fk_parent_tables)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(create_schemas_(new_tables, ddl_stmt_str, new_sequences, new_audits,
                                     replace_mock_fk_parent_table_id, new_mock_fk_parent_tables))) {
    LOG_WARN("fail create schemas", KR(ret));
  } else if (OB_FAIL(create_tablets_(new_tables))) {
    LOG_WARN("fail create schemas", KR(ret));
  }
  return ret;
}

int ObTableHelper::inner_generate_schemas_(common::ObArray<ObTableSchema> &new_tables,
                                           const common::ObArray<ObSequenceSchema *> &new_sequences,
                                           common::ObArray<ObSAuditSchema *> &new_audits)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(generate_table_schema_())) {
    LOG_WARN("fail to generate table schema", KR(ret));
  } else if (OB_FAIL(generate_aux_table_schemas_())) {
    LOG_WARN("fail to generate aux table schemas", KR(ret));
  } else if (OB_FAIL(gen_partition_object_and_tablet_ids_(new_tables))) {
    LOG_WARN("fail to gen partition object/tablet ids", KR(ret));
  } else if (OB_FAIL(generate_foreign_keys_())) {
    LOG_WARN("fail to generate foreign keys", KR(ret));
  } else if (OB_FAIL(generate_sequence_object_())) {
    LOG_WARN("fail to generate sequence object", KR(ret));
  } else if (OB_FAIL(generate_audit_schema_(new_tables, new_sequences, new_audits))) {
    LOG_WARN("fail to generate audit schema", KR(ret));
  }
  return ret;
}