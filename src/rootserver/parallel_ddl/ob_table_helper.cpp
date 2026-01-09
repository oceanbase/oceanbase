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
#include "rootserver/ob_index_builder.h"
#include "rootserver/parallel_ddl/ob_index_name_checker.h"
#include "rootserver/ob_lob_meta_builder.h"
#include "rootserver/ob_lob_piece_builder.h"
#include "rootserver/ob_table_creator.h"
#include "rootserver/ob_balance_group_ls_stat_operator.h"
#include "rootserver/freeze/ob_major_freeze_helper.h"
#include "share/ob_rpc_struct.h"
#include "share/ob_debug_sync_point.h"
#include "share/sequence/ob_sequence_option_builder.h" // ObSequenceOptionBuilder
#include "share/ob_fts_index_builder_util.h"
#include "rootserver/ob_location_ddl_service.h"
#include "share/ob_dynamic_partition_manager.h"
#include "share/schema/ob_table_sql_service.h"
#include "share/schema/ob_security_audit_sql_service.h"
#include "share/schema/ob_sequence_sql_service.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/ob_index_builder_util.h"
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
  ObDDLSQLTransaction *external_trans,
  bool enable_ddl_parallel)
  : ObDDLHelper(schema_service, tenant_id, parallel_ddl_type, external_trans, enable_ddl_parallel),
    new_tables_(),
    new_sequences_(),
    new_audits_(),
    new_mock_fk_parent_tables_()
{}
// create audit schema for table/sequence

int ObTableHelper::generate_audit_schema_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(new_tables_.count() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected table cnt", KR(ret), K(new_tables_.count()));
  } else {
    // check if data table's columns are matched with existed mock fk parent table
    const ObTableSchema &data_table = new_tables_.at(0);
    ObArray<ObSAuditSchema> audits;
    if (OB_FAIL(schema_guard_wrapper_.get_default_audit_schemas(audits))) {
      LOG_WARN("fail to get audits", KR(ret));
    } else if (!audits.empty()) {
      ObSAuditSchema *new_audit_schema = NULL;
      for (int64_t i = 0; OB_SUCC(ret) && i < audits.count(); i++) {
        const ObSAuditSchema &audit = audits.at(i);
        if (audit.is_access_operation_for_sequence()) {
          for (int64_t j = 0; OB_SUCC(ret) && j < new_sequences_.count(); j++) {
            const ObSequenceSchema *new_sequence = new_sequences_.at(j);
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
              if (OB_FAIL(new_audits_.push_back(new_audit_schema))) {
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
            if (OB_FAIL(new_audits_.push_back(new_audit_schema))) {
              LOG_WARN("fail to push back audit", KR(ret), KPC(new_audit_schema));
            }
          }
        }
      } // end for

      if (OB_SUCC(ret) && !new_audits_.empty()) {
        ObIDGenerator id_generator;
        const uint64_t object_cnt = new_audits_.count();
        uint64_t object_id = OB_INVALID_ID;
        if (OB_FAIL(gen_object_ids_(object_cnt, id_generator))) {
          LOG_WARN("fail to gen object ids", KR(ret), K_(tenant_id), K(object_cnt));
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < new_audits_.count(); i++) {
          ObSAuditSchema *audit = new_audits_.at(i);
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
                   ObMockFKParentTableSchema *&new_mock_fk_parent_table)
{
  int ret = OB_SUCCESS;
  new_mock_fk_parent_table = NULL;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_INVALID_ID == replace_mock_fk_parent_table_id) {
    // do nothing
  } else if (OB_UNLIKELY(new_tables_.count() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected table cnt", KR(ret), K(new_tables_.count()));
  } else {
    // check if data table's columns are matched with existed mock fk parent table
    const ObTableSchema &data_table = new_tables_.at(0);

    ObArray<const share::schema::ObTableSchema*> index_schemas;
    for (int64_t i = 1; OB_SUCC(ret) && i < new_tables_.count(); ++i) {
      if (new_tables_.at(i).is_unique_index()
          && OB_FAIL(index_schemas.push_back(&new_tables_.at(i)))) {
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
            LOG_WARN("get index column schema failed", KR(ret));
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
    LOG_WARN("check if oracle compat mode failed", KR(ret));
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

int ObTableHelper::create_audits_()
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
    for (int64_t i = 0; OB_SUCC(ret) && i < new_audits_.count(); i++) {
      ObSAuditSchema *new_audit = new_audits_.at(i);
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
                 get_trans_(),
                 public_sql_string))) {
        LOG_WARN("fail to add audit", KR(ret), K_(tenant_id), K(new_audit));
      }
    } // end for
  }
  return ret;
}

int ObTableHelper::create_tables_(const ObString *ddl_stmt_str)
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
    for (int64_t i = 0; OB_SUCC(ret) && i < new_tables_.count(); i++) {
      ObTableSchema &new_table = new_tables_.at(i);
      if (OB_FAIL(ObFtsIndexBuilderUtil::try_load_and_lock_dictionary_tables(new_table, get_trans_()))) {
        LOG_WARN("fail to try load and lock dictionary tables", KR(ret), K(tenant_id_));
      } else if (OB_FAIL(schema_service_->gen_new_schema_version(tenant_id_, new_schema_version))) {
          LOG_WARN("fail to gen new schema_version", KR(ret), K_(tenant_id));
      } else if (FALSE_IT(new_table.set_schema_version(new_schema_version))) {
      } else if ((new_table.is_vec_delta_buffer_type() || new_table.is_hybrid_vec_index_log_type()) &&
                 OB_FAIL(ObVectorIndexUtil::add_dbms_vector_jobs(get_trans_(), new_table.get_tenant_id(),
                                                                 new_table.get_table_id(),
                                                                 new_table.get_exec_env()))) {
        LOG_WARN("failed to add dbms_vector jobs", KR(ret), K(new_table.get_tenant_id()), K(new_table));
      }
    } // end for
    if (FAILEDx(schema_service_impl->get_table_sql_service().batch_create_table(new_tables_,
                get_trans_(),
                ddl_stmt_str,
                true/*sync_schema_version_for_last_table*/))) {
      LOG_WARN("failed to batch create table", KR(ret), K_(new_tables), KPC(ddl_stmt_str));
    } else if (OB_FAIL(schema_service_impl->get_table_sql_service().batch_insert_temp_table_info(
                 get_trans_(), new_tables_))) {
      LOG_WARN("failed to batch insert temp table info", KR(ret), K_(new_tables));
    }
  }
  return ret;
}

int ObTableHelper::deal_with_mock_fk_parent_tables_(const uint64_t replace_mock_fk_parent_table_id)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service_impl = NULL;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_ISNULL(schema_service_impl = schema_service_->get_schema_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service impl is null", KR(ret));
  } else if (OB_UNLIKELY(new_tables_.count() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected table cnt", KR(ret), K(new_tables_.count()));
  } else {
    const ObTableSchema &data_table = new_tables_.at(0);
    const uint64_t data_table_id = data_table.get_table_id();
    int64_t new_schema_version = OB_INVALID_VERSION;
    for (int64_t i = 0; OB_SUCC(ret) && i < new_mock_fk_parent_tables_.count(); i++) {
      ObMockFKParentTableSchema *new_mock_fk_parent_table = new_mock_fk_parent_tables_.at(i);
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
              &get_trans_(), *new_mock_fk_parent_table, false /*need_update_foreign_key*/))) {
            LOG_WARN("fail to add mock fk parent table", KR(ret), KPC(new_mock_fk_parent_table));
          }
        } else if (MOCK_FK_PARENT_TABLE_OP_UPDATE_SCHEMA_VERSION == operation_type
                   || MOCK_FK_PARENT_TABLE_OP_ADD_COLUMN == operation_type) {
          // 2. alter table: mock fk parent table has new child table.
          if (OB_FAIL(schema_service_impl->get_table_sql_service().alter_mock_fk_parent_table(
                      &get_trans_(), *new_mock_fk_parent_table))) {
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
                        &get_trans_(), *new_mock_fk_parent_table, ori_mock_fk_parent_table))) {
              LOG_WARN("fail to replace mock fk parent table", KR(ret), KPC(new_mock_fk_parent_table));
            }

            // 3.3. update child new_tables_' schema version.
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
                                 get_trans_(), tenant_id_, child_table_id, child_table->get_in_offline_ddl_white_list()))) {
                LOG_WARN("fail to update child table's schema version", KR(ret), K_(tenant_id), K(child_table_id));
              }
            } // end for

            // 3.4. update data table's schema version at last.
            if (FAILEDx(schema_service_impl->get_table_sql_service().update_data_table_schema_version(
                        get_trans_(), tenant_id_, data_table_id, false/*in_offline_ddl_white_list*/))) {
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

int ObTableHelper::create_tablets_()
{
  int ret = OB_SUCCESS;
  SCN frozen_scn;
  ObSchemaGetterGuard schema_guard;
  ObSchemaService *schema_service_impl = NULL;
  uint64_t tenant_data_version = 0;

  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_ISNULL(schema_service_impl = schema_service_->get_schema_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service impl is null", KR(ret));
  } else if (OB_FAIL(ObMajorFreezeHelper::get_frozen_scn(tenant_id_, frozen_scn, &get_trans_()))) {
    LOG_WARN("failed to get frozen status for create tablet", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", KR(ret), K_(tenant_id));
  } else if (OB_UNLIKELY(new_tables_.count() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected table cnt", KR(ret), K(new_tables_.count()));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id_, tenant_data_version))) {
    LOG_WARN("get min data version failed", KR(ret), K_(tenant_id));
  } else {
    const ObTableSchema &data_table = new_tables_.at(0);
    ObTableCreator table_creator(
                   tenant_id_,
                   frozen_scn,
                   get_trans_());

    // use the external_trans as sql_proxy if not null,
    // to ensure that changes in the current DDL transaction can be queried
    common::ObISQLClient *sql_proxy = get_external_trans_();
    if (sql_proxy == NULL) {
      sql_proxy = sql_proxy_;
    }
    // TODO:(yanmu.ztl)
    // schema_guard is used to get primary table in tablegroup or data table for local index.
    // - primary table may be incorrect when ddl execute concurrently.
    ObNewTableTabletAllocator new_table_tablet_allocator(
                              tenant_id_,
                              schema_guard,
                              sql_proxy,
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
      ObArray<bool> need_create_empty_majors;
      ObArray<uint64_t> table_ids;
      for (int64_t i = 0; OB_SUCC(ret) && i < new_tables_.count(); i++) {
        const ObTableSchema &new_table = new_tables_.at(i);
        const uint64_t table_id = new_table.get_table_id();
        if (!new_table.has_tablet()) {
          // eg. external table ...
        } else if (!new_table.is_global_index_table()) {
          if (OB_FAIL(schemas.push_back(&new_table))) {
            LOG_WARN("fail to push back new table", KR(ret));
          } else if (OB_FAIL(need_create_empty_majors.push_back(true))) {
            LOG_WARN("fail to push back need create empty major", KR(ret));
          }
        } else {
          if (OB_FAIL(new_table_tablet_allocator.prepare(get_trans_(), new_table, data_tablegroup_schema))) {
            LOG_WARN("fail to prepare ls for global index", KR(ret), K(new_table));
          } else if (OB_FAIL(new_table_tablet_allocator.get_ls_id_array(ls_id_array))) {
            LOG_WARN("fail to get ls id array", KR(ret));
          } else if (OB_FAIL(table_creator.add_create_tablets_of_table_arg(
                     new_table, ls_id_array, tenant_data_version, true/*need create major sstable*/))) {
            LOG_WARN("create table partitions failed", KR(ret), K(new_table));
          }
        }
        if (FAILEDx(table_ids.push_back(table_id))) {
          LOG_WARN("failed to push_back table_id", KR(ret), K(table_id));
        }
      } // end for

      if (FAILEDx(schema_service_impl->get_table_sql_service().batch_insert_ori_schema_version(
                                       get_trans_(), tenant_id_, table_ids, last_schema_version))) {
        LOG_WARN("failed to batch insert ori schema version", KR(ret), K(tenant_id_), K(table_ids),
                                                              K(last_schema_version));
      } else if (schemas.count() > 0) {
        if (OB_FAIL(new_table_tablet_allocator.prepare(get_trans_(),
                                                       data_table,
                                                       data_tablegroup_schema,
                                                       false,  /* is_add_partition */
                                                       get_external_trans_() == NULL ? NULL : schema_guard_wrapper_.get_latest_schema_guard()))) {
          LOG_WARN("fail to prepare ls for data table", KR(ret));
        } else if (OB_FAIL(new_table_tablet_allocator.get_ls_id_array(ls_id_array))) {
          LOG_WARN("fail to get ls id array", KR(ret));
        } else if (OB_FAIL(table_creator.add_create_tablets_of_tables_arg(
                   schemas, ls_id_array, tenant_data_version, need_create_empty_majors /*need create major sstable*/))) {
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


int ObTableHelper::calc_schema_version_cnt_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(new_tables_.count() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected table cnt", KR(ret), K(new_tables_.count()));
  } else {
    const ObTableSchema &data_table = new_tables_.at(0);
    // 0. data table
    schema_version_cnt_ = 1; // init

    // 1. sequence
    schema_version_cnt_ += new_sequences_.count();

    // 2. audit
    schema_version_cnt_ += new_audits_.count();

    // 3. create index/lob table
    if (new_tables_.count() > 1) {
      schema_version_cnt_ += (new_tables_.count() - 1);
      // update data table schema version
      schema_version_cnt_++;
    }

    // 4. foreign key (without mock fk parent table)

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

    // 5. mock fk parent table
    // schema version for new mock fk parent tables
    schema_version_cnt_ += new_mock_fk_parent_tables_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < new_mock_fk_parent_tables_.count(); i++) {
      const ObMockFKParentTableSchema *new_mock_fk_parent_table = new_mock_fk_parent_tables_.at(i);
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

    // 6. for 1503 boundary ddl operation
    schema_version_cnt_++;
  }
  return ret;
}

int ObTableHelper::create_schemas_(const ObString *ddl_stmt_str,
                                   const uint64_t replace_mock_fk_parent_table_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(create_tables_(ddl_stmt_str))) {
    LOG_WARN("fail to create tables", KR(ret));
  } else if (OB_FAIL(create_sequences_())) {
    LOG_WARN("fail to create sequences", KR(ret));
  } else if (OB_FAIL(create_audits_())) {
    LOG_WARN("fail to create tables", KR(ret));
  } else if (OB_FAIL(deal_with_mock_fk_parent_tables_(replace_mock_fk_parent_table_id))) {
    LOG_WARN("fail to deal with mock fk parent tables", KR(ret));
  }
  RS_TRACE(operate_schemas);
  return ret;
}

int ObTableHelper::create_sequences_()
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service_impl = NULL;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (new_sequences_.count() <= 0) {
    // skip
  } else if (OB_ISNULL(schema_service_impl = schema_service_->get_schema_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service impl is null", KR(ret));
  } else {
    ObString *ddl_stmt_str = NULL;
    uint64_t *old_sequence_id = NULL; // means don't sync sequence value
    int64_t new_schema_version = OB_INVALID_VERSION;
    for (int64_t i = 0; OB_SUCC(ret) && i < new_sequences_.count(); i++) {
      ObSequenceSchema *new_sequence = new_sequences_.at(i);
      if (OB_ISNULL(new_sequence)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("new sequence is null", KR(ret));
      } else if (OB_FAIL(schema_service_->gen_new_schema_version(tenant_id_, new_schema_version))) {
        LOG_WARN("fail to gen new schema_version", KR(ret), K_(tenant_id));
      } else if (FALSE_IT(new_sequence->set_schema_version(new_schema_version))) {
      } else if (OB_FAIL(schema_service_impl->get_sequence_sql_service().insert_sequence(
                 *new_sequence, &get_trans_(), ddl_stmt_str, old_sequence_id))) {
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

int ObTableHelper::inner_create_table_(const ObString *ddl_stmt_str,
                                       const uint64_t replace_mock_fk_parent_table_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(create_schemas_(ddl_stmt_str,
                                     replace_mock_fk_parent_table_id))) {
    LOG_WARN("fail create schemas", KR(ret));
  } else if (OB_FAIL(create_tablets_())) {
    LOG_WARN("fail create schemas", KR(ret));
  }
  return ret;
}

int ObTableHelper::generate_schemas_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(generate_table_schema_())) {
    LOG_WARN("fail to generate table schema", KR(ret));
  } else if (OB_FAIL(generate_aux_table_schemas_())) {
    LOG_WARN("fail to generate aux table schemas", KR(ret));
  } else if (OB_FAIL(gen_partition_object_and_tablet_ids_(new_tables_))) {
    LOG_WARN("fail to gen partition object/tablet ids", KR(ret));
  } else if (OB_FAIL(generate_foreign_keys_())) {
    LOG_WARN("fail to generate foreign keys", KR(ret));
  } else if (OB_FAIL(generate_sequence_object_())) {
    LOG_WARN("fail to generate sequence object", KR(ret));
  } else if (OB_FAIL(generate_audit_schema_())) {
    LOG_WARN("fail to generate audit schema", KR(ret));
  }
  return ret;
}

int ObTableHelper::inner_generate_table_schema_(const ObCreateTableArg &arg, ObTableSchema &new_table)
{
  int ret = OB_SUCCESS;
  const uint64_t mock_table_id = OB_MIN_USER_OBJECT_ID + 1;
  uint64_t compat_version = 0;
  bool is_oracle_mode = false;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id_, compat_version))) {
    LOG_WARN("fail to get data version", KR(ret), K_(tenant_id));
  } else if (not_compat_for_queuing_mode(compat_version) && arg.schema_.is_new_queuing_table_mode()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN(QUEUING_MODE_NOT_COMPAT_WARN_STR, KR(ret), K_(tenant_id), K(compat_version), K(arg));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, QUEUING_MODE_NOT_COMPAT_USER_ERROR_STR);
  } else if (compat_version < DATA_VERSION_4_3_5_1 && arg.schema_.get_enable_macro_block_bloom_filter()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("fail to generate schema, not support enable_macro_block_bloom_filter for this version",
             KR(ret), K(tenant_id_), K(compat_version), K(arg));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "this version not support enable_macro_block_bloom_filter");
  } else if (compat_version < DATA_VERSION_4_3_5_2 &&
            !is_storage_cache_policy_default(arg.schema_.get_storage_cache_policy())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("fail to generate schema, not support storage_cache_policy for this version",
             KR(ret), K(tenant_id_), K(compat_version), K(arg));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "this version not support storage_cache_policy");
  } else if (compat_version < DATA_VERSION_4_3_5_2 && arg.schema_.is_delete_insert_merge_engine()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("fail to generate schema, not support delete insert merge engine for this version", KR(ret), K_(tenant_id), K(compat_version), K(arg));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "this version not support delete insert merge engine");
  } else if (!ObMicroBlockFormatVersionHelper::check_version_valid(arg.schema_.get_micro_block_format_version(), compat_version)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("fail to generate schema, not support this micro block format version for this version", KR(ret), K_(tenant_id), K(compat_version), K(arg));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant data version is less than needed, this micro block format version is");
  } else if (OB_UNLIKELY(OB_INVALID_ID != arg.schema_.get_table_id())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("create table with table_id in 4.x is not supported",
             KR(ret), K_(tenant_id), "table_id", arg.schema_.get_table_id());
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "create table with id is");
  } else if (compat_version < DATA_VERSION_4_3_5_2 && arg.schema_.get_semistruct_encoding_flags() != 0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("fail to generate schema, not support semistruct encoding for this version",
             KR(ret), K(tenant_id_), K(compat_version), K(arg));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "this version not support semistruct encoding");
  } else if (compat_version < DATA_VERSION_4_4_1_0 && !arg.schema_.get_semistruct_properties().empty()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("fail to generate schema, not support semistruct properties for this version",
             KR(ret), K_(tenant_id), K(compat_version), K(arg));
  } else if ((compat_version < MOCK_DATA_VERSION_4_3_5_4
              || (compat_version < MOCK_DATA_VERSION_4_4_2_0
                  && compat_version >= DATA_VERSION_4_4_0_0)
              || (compat_version < DATA_VERSION_4_5_1_0
                  && compat_version >= DATA_VERSION_4_5_0_0))
             && arg.schema_.is_mysql_tmp_table()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "MySQL compatible temporary table");
    LOG_WARN("not support to create mysql tmp table", KR(ret), K_(tenant_id), K(compat_version), K(arg.schema_));
  } else if (arg.schema_.is_duplicate_table()) { // check compatibility for duplicate table
    bool is_compatible = false;
    if (OB_FAIL(ObShareUtil::check_compat_version_for_readonly_replica(tenant_id_, is_compatible))) {
      LOG_WARN("fail to check compat version for duplicate log stream", KR(ret), K_(tenant_id));
    } else if (!is_compatible) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("duplicate table is not supported below 4.2", KR(ret), K_(tenant_id));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "create duplicate table below 4.2");
    } else if (!is_user_tenant(tenant_id_)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not user tenant, create duplicate table not supported", KR(ret), K_(tenant_id));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "not user tenant, create duplicate table");
    }
  }

  if (FALSE_IT(new_table.set_table_id(mock_table_id))) {
  } else if (FAILEDx(ddl_service_->try_format_partition_schema(new_table))) {
    LOG_WARN("fail to format partition schema", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(new_table.check_if_oracle_compat_mode(is_oracle_mode))) {
    LOG_WARN("failed to get compat mode", KR(ret), K_(tenant_id));
  }

  const uint64_t tablespace_id = new_table.get_tablespace_id();
  if (OB_SUCC(ret) && OB_INVALID_ID != tablespace_id) {
    const ObTablespaceSchema *tablespace = NULL;
    if (OB_FAIL(schema_guard_wrapper_.get_tablespace_schema(
        tablespace_id, tablespace))) {
      LOG_WARN("fail to get tablespace schema", KR(ret), K_(tenant_id), K(tablespace_id));
    } else if (OB_ISNULL(tablespace)) {
      ret = OB_ERR_PARALLEL_DDL_CONFLICT;
      LOG_WARN("tablespace not exist, need retry", KR(ret), K_(tenant_id), K(tablespace_id));
    } else if (OB_FAIL(new_table.set_encrypt_key(tablespace->get_encrypt_key()))) {
      LOG_WARN("fail to set encrypt key", KR(ret), K_(tenant_id), KPC(tablespace));
    } else {
      new_table.set_master_key_id(tablespace->get_master_key_id());
    }
  }

  const uint64_t tablegroup_id = new_table.get_tablegroup_id();
  if (OB_SUCC(ret) && OB_INVALID_ID != tablegroup_id) {
    //TODO:(yanmu.ztl) local schema maybe too old for concurrent create table
    // to check partition options with the primary table in tablegroup.
    ObSchemaGetterGuard guard;
    const ObTablegroupSchema *tablegroup = NULL;
    if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id_, guard))) {
      LOG_WARN("fail to get tenant schema guard", KR(ret), K_(tenant_id));
    } else if (OB_NOT_NULL(external_trans_) && OB_FAIL(schema_guard_wrapper_.get_tablegroup_schema(
          tablegroup_id, tablegroup))) {
      LOG_WARN("get tablegroup_schema failed", KR(ret), K(tablegroup_id));
    } else if (OB_NOT_NULL(external_trans_) && OB_ISNULL(tablegroup)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablegroup is null", KR(ret), K(tablegroup_id));
    } else if (OB_FAIL(ddl_service_->try_check_and_set_table_schema_in_tablegroup(guard,
        new_table, tablegroup))) {
      LOG_WARN("fail to check table in tablegorup", KR(ret), K(new_table));
    }
  }

  if (FAILEDx(check_table_udt_exist_(new_table))) {
    LOG_WARN("fail to check table udt exist", KR(ret));
  }

  // check if constraint name duplicated
  const uint64_t database_id = new_table.get_database_id();
  bool cst_exist = false;
  const ObIArray<ObConstraint> &constraints = arg.constraint_list_;
  const int64_t cst_cnt = constraints.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < cst_cnt; i++) {
    const ObConstraint &cst = constraints.at(i);
    const ObString &cst_name = cst.get_constraint_name_str();
    if (OB_UNLIKELY(cst.get_constraint_name_str().empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cst name is empty", KR(ret), K_(tenant_id), K(database_id), K(cst_name));
    } else if (OB_FAIL(check_constraint_name_exist_(new_table, cst_name, false /*is_foreign_key*/, cst_exist))) {
      LOG_WARN("fail to check constraint name exist", KR(ret), K_(tenant_id), K(database_id), K(cst_name));
    } else if (cst_exist) {
      ret = OB_ERR_CONSTRAINT_NAME_DUPLICATE;
      if (!is_oracle_mode) {
        LOG_USER_ERROR(OB_ERR_CONSTRAINT_NAME_DUPLICATE, cst_name.length(), cst_name.ptr());
      }
      LOG_WARN("cst name is duplicate", KR(ret), K_(tenant_id), K(database_id), K(cst_name));
    }
  } // end for

  // fetch object_ids (data table + constraints)
  ObIDGenerator id_generator;
  const uint64_t object_cnt = cst_cnt + 1;
  uint64_t object_id = OB_INVALID_ID;
  if (FAILEDx(gen_object_ids_(object_cnt, id_generator))) {
    LOG_WARN("fail to gen object ids", KR(ret), K_(tenant_id), K(object_cnt));
  } else if (OB_FAIL(id_generator.next(object_id))) {
    LOG_WARN("fail to get next object_id", KR(ret));
  } else {
    (void) new_table.set_table_id(object_id);
  }

  // generate constraints
  for (int64_t i = 0; OB_SUCC(ret) && i < cst_cnt; i++) {
    ObConstraint &cst = const_cast<ObConstraint &>(constraints.at(i));
    cst.set_tenant_id(tenant_id_);
    cst.set_table_id(new_table.get_table_id());
    if (OB_FAIL(id_generator.next(object_id))) {
      LOG_WARN("fail to get next object_id", KR(ret));
    } else if (FALSE_IT(cst.set_constraint_id(object_id))) {
    } else if (OB_FAIL(new_table.add_constraint(cst))) {
      LOG_WARN("fail to add constraint", KR(ret), K(cst));
    }
  } // end for

  // fill table schema for interval part
  if (OB_SUCC(ret)
      && new_table.has_partition()
      && new_table.is_interval_part()) {
    int64_t part_num = new_table.get_part_option().get_part_num();
    ObPartition **part_array = new_table.get_part_array();
    const ObRowkey *transition_point = NULL;
    if (OB_FAIL(new_table.check_support_interval_part())) {
      LOG_WARN("fail to check support interval part", KR(ret), K(new_table));
    } else if (OB_ISNULL(part_array)
               || OB_UNLIKELY(0 == part_num)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("range part array is null or part_num is 0", KR(ret));
    } else if (OB_ISNULL(transition_point = &part_array[part_num - 1]->get_high_bound_val())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("transition_point is null", KR(ret), KPC(transition_point));
    } else if (OB_FAIL(ObPartitionUtils::check_interval_partition_table(
                       tenant_id_, *transition_point, new_table.get_interval_range()))) {
      LOG_WARN("fail to check_interval_partition_table", KR(ret), K(new_table));
    } else if (OB_FAIL(new_table.set_transition_point(*transition_point))) {
      LOG_WARN("fail to set transition point", KR(ret), K(new_table));
    }
  }

  // check auto_partition validity
  if (FAILEDx(new_table.check_validity_for_auto_partition())) {
    LOG_WARN("fail to check auto partition setting", KR(ret), K(new_table), K(arg));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(new_table.set_storage_cache_policy(arg.schema_.get_storage_cache_policy()))) {
      LOG_WARN("fail to set storage_cache_policy", K(ret), K(arg.schema_.get_storage_cache_policy()));
    }
  }

  if (OB_SUCC(ret) && !new_table.get_dynamic_partition_policy().empty()) {
    bool is_supported = false;
    if (compat_version < DATA_VERSION_4_3_5_2) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("dynamic partition less than 4.3.5.2 not support", KR(ret), K(compat_version));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "dynamic partition less than 4.3.5.2");
    } else if (OB_FAIL(ObDynamicPartitionManager::check_is_supported(new_table))) {
      LOG_WARN("fail to check dynamic partition is supported", KR(ret), K(new_table));
    } else if (OB_FAIL(ObDynamicPartitionManager::check_is_valid(new_table))) {
      LOG_WARN("fail to check dynamic partition is valid", KR(ret), K(new_table));
    }
  }

  if (OB_SUCC(ret) && new_table.is_external_table()) {
    if (OB_FAIL(ObLocationDDLService::check_location_constraint(new_table))) {
      LOG_WARN("fail to check location ", KR(ret), K(new_table));
    }
  }


  return ret;
}

int ObTableHelper::inner_generate_aux_table_schema_(const ObCreateTableArg &arg)
{
  int ret = OB_SUCCESS;
  HEAP_VAR(ObTableSchema, index_schema) {
  ObTableSchema *data_table = nullptr;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(new_tables_.count() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table cnt not match", KR(ret), "table_cnt", new_tables_.count());
  } else {
    ObTableSchema *data_table = &(new_tables_.at(0));
    if (OB_ISNULL(data_table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("data table is nullptr", KR(ret));
    }
    if (OB_SUCC(ret)) {
      data_table = &(new_tables_.at(0));
      // 0. fetch object_ids
      ObIDGenerator id_generator;
      int64_t object_cnt = arg.index_arg_list_.size();
      bool has_lob_table = false;
      uint64_t object_id = OB_INVALID_ID;
      if (!data_table->is_external_table()) {
        has_lob_table = data_table->has_lob_column(true/*ignore_unused_column*/);
        if (has_lob_table) {
          object_cnt += 2;
        }
      }
      if (FAILEDx(gen_object_ids_(object_cnt, id_generator))) {
        LOG_WARN("fail to gen object ids", KR(ret), K_(tenant_id), K(object_cnt));
      }

      // 1. build index table
      ObIndexBuilder index_builder(*ddl_service_);
      for (int64_t i = 0; OB_SUCC(ret) && i < arg.index_arg_list_.size(); ++i) {
        index_schema.reset();
        obrpc::ObCreateIndexArg &index_arg = const_cast<obrpc::ObCreateIndexArg&>(arg.index_arg_list_.at(i));
        if (!index_arg.index_schema_.is_partitioned_table()
            && !data_table->is_partitioned_table()
            && !data_table->is_auto_partitioned_table()) {
          if (INDEX_TYPE_NORMAL_GLOBAL == index_arg.index_type_) {
            index_arg.index_type_ = INDEX_TYPE_NORMAL_GLOBAL_LOCAL_STORAGE;
          } else if (INDEX_TYPE_UNIQUE_GLOBAL == index_arg.index_type_) {
            index_arg.index_type_ = INDEX_TYPE_UNIQUE_GLOBAL_LOCAL_STORAGE;
          } else if (INDEX_TYPE_SPATIAL_GLOBAL == index_arg.index_type_) {
            index_arg.index_type_ = INDEX_TYPE_SPATIAL_GLOBAL_LOCAL_STORAGE;
          } else if (is_global_fts_index(index_arg.index_type_)) {
            if (index_arg.index_type_ == INDEX_TYPE_DOC_ID_ROWKEY_GLOBAL) {
              index_arg.index_type_ = INDEX_TYPE_DOC_ID_ROWKEY_GLOBAL_LOCAL_STORAGE;
            } else if (index_arg.index_type_ == INDEX_TYPE_FTS_INDEX_GLOBAL) {
              index_arg.index_type_ = INDEX_TYPE_FTS_INDEX_GLOBAL_LOCAL_STORAGE;
            } else if (index_arg.index_type_ == INDEX_TYPE_FTS_DOC_WORD_GLOBAL) {
              index_arg.index_type_ = INDEX_TYPE_FTS_DOC_WORD_GLOBAL_LOCAL_STORAGE;
            }
          }
        }
        // the global index has generated column schema during resolve, RS no need to generate index schema,
        // just assign column schema
        if (INDEX_TYPE_NORMAL_GLOBAL == index_arg.index_type_
            || INDEX_TYPE_UNIQUE_GLOBAL == index_arg.index_type_
            || INDEX_TYPE_SPATIAL_GLOBAL == index_arg.index_type_) {
          if (OB_FAIL(index_schema.assign(index_arg.index_schema_))) {
            LOG_WARN("fail to assign schema", KR(ret), K(index_arg));
          }
        }
        // TODO(yanmu.ztl): should check if index name is duplicated in oracle mode.
        const bool global_index_without_column_info = false;
        ObSEArray<ObColumnSchemaV2 *, 1> gen_columns;
        ObIAllocator *allocator = index_arg.index_schema_.get_allocator();
        bool index_exist = false;
        if (OB_FAIL(ret)) {
        } else if (OB_ISNULL(allocator)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid allocator", KR(ret));
        } else if (OB_FAIL(ObIndexBuilderUtil::adjust_expr_index_args(
                   index_arg, *data_table, *allocator, gen_columns))) {
            LOG_WARN("fail to adjust expr index args", KR(ret), K(index_arg), KPC(data_table));
        } else if (OB_FAIL(index_builder.generate_schema(index_arg,
                                                         *data_table,
                                                         global_index_without_column_info,
                                                         false, /*generate_id*/
                                                         index_schema))) {
          LOG_WARN("generate_schema for index failed", KR(ret), K(index_arg), KPC(data_table));
        } else if (index_schema.is_hybrid_vec_index_log_type()) {
          if (!has_lob_table) {
            object_cnt += 2;
            if (FAILEDx(gen_object_ids_(object_cnt, id_generator))) {
              LOG_WARN("fail to gen object ids", KR(ret), K_(tenant_id), K(object_cnt));
            }
            has_lob_table = true;
          } else {
            // do nothing, don't need to force generate the lob table
          }
        }

        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(id_generator.next(object_id))) {
          LOG_WARN("fail to get next object_id", KR(ret));
        } else if (FALSE_IT(index_schema.set_table_id(object_id))) {
        } else if (OB_FAIL(index_schema.generate_origin_index_name())) {
          // For the later operation ObIndexNameChecker::add_index_name()
          LOG_WARN("fail to generate origin index name", KR(ret), K(index_schema));
        } else if (OB_FAIL(ddl_service_->get_index_name_checker().check_index_name_exist(
                   index_schema.get_tenant_id(),
                   index_schema.get_database_id(),
                   index_schema.get_table_name_str(),
                   index_exist))) {
        } else if (index_exist) {
          // actually, only index name in oracle tenant will be checked.
          ret = OB_ERR_KEY_NAME_DUPLICATE;
          LOG_WARN("duplicate index name", KR(ret), K_(tenant_id),
                   "database_id", index_schema.get_database_id(),
                   "index_name", index_schema.get_origin_index_name_str());
          LOG_USER_ERROR(OB_ERR_KEY_NAME_DUPLICATE,
                         index_schema.get_origin_index_name_str().length(),
                         index_schema.get_origin_index_name_str().ptr());
        } else if (OB_FAIL(new_tables_.push_back(index_schema))) {
          LOG_WARN("push_back failed", KR(ret));
        } else {
          data_table = &(new_tables_.at(0)); // memory of data table may change after add table to new_tables_
        }
      } // end for

      // 2. build lob table
      if (OB_SUCC(ret) && has_lob_table) {
        HEAP_VARS_2((ObTableSchema, lob_meta_schema), (ObTableSchema, lob_piece_schema)) {
        ObLobMetaBuilder lob_meta_builder(*ddl_service_);
        ObLobPieceBuilder lob_piece_builder(*ddl_service_);
        bool need_object_id = false;
        if (OB_FAIL(id_generator.next(object_id))) {
          LOG_WARN("fail to get next object_id", KR(ret));
        } else if (OB_FAIL(lob_meta_builder.generate_aux_lob_meta_schema(
          schema_service_->get_schema_service(), *data_table, object_id, lob_meta_schema, need_object_id))) {
          LOG_WARN("generate lob meta table failed", KR(ret), KPC(data_table));
        } else if (OB_FAIL(id_generator.next(object_id))) {
          LOG_WARN("fail to get next object_id", KR(ret));
        } else if (OB_FAIL(lob_piece_builder.generate_aux_lob_piece_schema(
          schema_service_->get_schema_service(), *data_table, object_id, lob_piece_schema, need_object_id))) {
          LOG_WARN("generate_schema for lob data table failed", KR(ret), KPC(data_table));
        } else if (OB_FAIL(new_tables_.push_back(lob_meta_schema))) {
          LOG_WARN("push_back lob meta table failed", KR(ret));
        } else if (OB_FAIL(new_tables_.push_back(lob_piece_schema))) {
          LOG_WARN("push_back lob piece table failed", KR(ret));
        } else {
          data_table = &(new_tables_.at(0)); // memory of data table may change after add table to new_tables_
          data_table->set_aux_lob_meta_tid(lob_meta_schema.get_table_id());
          data_table->set_aux_lob_piece_tid(lob_piece_schema.get_table_id());
        }

        } // end HEAP_VARS_2
      }
    }
  }

  } // end HEAP_VAR
  return ret;
}