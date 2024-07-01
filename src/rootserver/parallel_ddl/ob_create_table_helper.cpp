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
#include "rootserver/parallel_ddl/ob_create_table_helper.h"
#include "rootserver/parallel_ddl/ob_index_name_checker.h"
#include "rootserver/ob_index_builder.h"
#include "rootserver/ob_lob_meta_builder.h"
#include "rootserver/ob_lob_piece_builder.h"
#include "rootserver/ob_table_creator.h"
#include "rootserver/ob_balance_group_ls_stat_operator.h"
#include "rootserver/freeze/ob_major_freeze_helper.h"
#include "share/ob_rpc_struct.h"
#include "share/ob_index_builder_util.h"
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

uint64_t ObCreateTableHelper::MockFKParentTableNameWrapper::hash() const
{
  uint64_t hash_ret = 0;
  common::ObCollationType cs_type = ObSchema::get_cs_type_with_cmp_mode(OB_ORIGIN_AND_INSENSITIVE);
  hash_ret = common::ObCharset::hash(cs_type, parent_database_, hash_ret);
  hash_ret = common::ObCharset::hash(cs_type, parent_table_, hash_ret);
  return hash_ret;
}

int ObCreateTableHelper::MockFKParentTableNameWrapper::hash(uint64_t &hash_val) const
{
  hash_val = hash();
  return OB_SUCCESS;
}

bool ObCreateTableHelper::MockFKParentTableNameWrapper::operator==(const MockFKParentTableNameWrapper &rv) const
{
  return 0 == parent_database_.case_compare(rv.parent_database_)
         && 0 == parent_table_.case_compare(rv.parent_table_);
}

ObCreateTableHelper::ObCreateTableHelper(
    share::schema::ObMultiVersionSchemaService *schema_service,
    const uint64_t tenant_id,
    const obrpc::ObCreateTableArg &arg,
    obrpc::ObCreateTableRes &res)
  : ObDDLHelper(schema_service, tenant_id),
    arg_(arg),
    res_(res),
    replace_mock_fk_parent_table_id_(common::OB_INVALID_ID),
    new_tables_(),
    new_mock_fk_parent_tables_(),
    new_mock_fk_parent_table_map_(),
    new_audits_(),
    new_sequences_(),
    has_index_(false)
{}

ObCreateTableHelper::~ObCreateTableHelper()
{
}

int ObCreateTableHelper::init_()
{
  int ret = OB_SUCCESS;
  const int64_t BUCKET_NUM = 100;
  if (OB_FAIL(new_mock_fk_parent_table_map_.create(BUCKET_NUM, "MockFkPMap", "MockFkPMap"))) {
    LOG_WARN("fail to init mock fk parent table map", KR(ret));
  }
  return ret;
}

int ObCreateTableHelper::execute()
{
  RS_TRACE(create_table_begin);
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(init_())) {
    LOG_WARN("fail to init struct", KR(ret));
  } else if (OB_FAIL(start_ddl_trans_())) {
    LOG_WARN("fail to start ddl trans", KR(ret));
  } else if (OB_FAIL(lock_objects_())) {
    LOG_WARN("fail to lock objects", KR(ret));
  } else if (OB_FAIL(generate_schemas_())) {
    LOG_WARN("fail to generate schemas", KR(ret));
  } else if (OB_FAIL(calc_schema_version_cnt_())) {
    LOG_WARN("fail to calc schema version cnt", KR(ret));
  } else if (OB_FAIL(gen_task_id_and_schema_versions_())) {
    LOG_WARN("fail to gen task id and schema versions", KR(ret));
  } else if (OB_FAIL(create_schemas_())) {
    LOG_WARN("fail create schemas", KR(ret));
  } else if (OB_FAIL(create_tablets_())) {
    LOG_WARN("fail create schemas", KR(ret));
  } else if (OB_FAIL(serialize_inc_schema_dict_())) {
    LOG_WARN("fail to serialize inc schema dict", KR(ret));
  } else if (OB_FAIL(wait_ddl_trans_())) {
    LOG_WARN("fail to wait ddl trans", KR(ret));
  } else if (OB_FAIL(add_index_name_to_cache_())) {
    LOG_WARN("fail to add index name to cache", KR(ret));
  }

  const bool commit = OB_SUCC(ret);
  if (OB_FAIL(end_ddl_trans_(ret))) { // won't overwrite ret
    LOG_WARN("fail to end ddl trans", KR(ret));
    if (commit && has_index_) {
      // Because index name is added to cache before trans commit,
      // it will remain garbage in cache when trans commit failed and false alarm will occur.
      //
      // To solve this problem:
      // 1. check_index_name_exist() will double check by inner_sql and erase garbage if index name conflicts.
      // 2. (Fully unnecessary) clean up index name cache when trans commit failed.
      int tmp_ret = OB_SUCCESS;
      if (OB_ISNULL(ddl_service_)) {
        tmp_ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ddl_service_ is null", KR(tmp_ret));
      } else if (OB_TMP_FAIL(ddl_service_->get_index_name_checker().reset_cache(tenant_id_))) {
        LOG_ERROR("fail to reset cache", K(ret), KR(tmp_ret), K_(tenant_id));
      }
    }
  }

  if (OB_SUCC(ret)) {
    auto *tsi_generator = GET_TSI(TSISchemaVersionGenerator);
    int64_t last_schema_version = OB_INVALID_VERSION;
    int64_t end_schema_version = OB_INVALID_VERSION;
    if (OB_UNLIKELY(new_tables_.count() <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table cnt is invalid", KR(ret));
    } else if (OB_ISNULL(tsi_generator)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tsi schema version generator is null", KR(ret));
    } else if (OB_FAIL(tsi_generator->get_current_version(last_schema_version))) {
      LOG_WARN("fail to get end version", KR(ret), K_(tenant_id), K_(arg));
    } else if (OB_FAIL(tsi_generator->get_end_version(end_schema_version))) {
      LOG_WARN("fail to get end version", KR(ret), K_(tenant_id), K_(arg));
    } else if (OB_UNLIKELY(last_schema_version != end_schema_version)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("too much schema versions may be allocated", KR(ret), KPC(tsi_generator));
    } else {
      res_.table_id_ = new_tables_.at(0).get_table_id();
      res_.schema_version_ = last_schema_version;
    }
  }

  if (OB_ERR_TABLE_EXIST == ret) {
    const ObTableSchema &table = arg_.schema_;
    //create table xx if not exist (...)
    if (arg_.if_not_exist_) {
      res_.do_nothing_ = true;
      ret = OB_SUCCESS;
      LOG_INFO("table is exist, no need to create again",
               "tenant_id", table.get_tenant_id(),
               "database_id", table.get_database_id(),
               "table_name", table.get_table_name());
    } else {
      LOG_WARN("table is exist, cannot create it twice", KR(ret),
               "tenant_id", table.get_tenant_id(),
               "database_id", table.get_database_id(),
               "table_name", table.get_table_name());
      LOG_USER_ERROR(OB_ERR_TABLE_EXIST,
                     table.get_table_name_str().length(),
                     table.get_table_name_str().ptr());
    }
  }

  RS_TRACE(create_table_end);
  FORCE_PRINT_TRACE(THE_RS_TRACE, "[parallel create table]");
  return ret;
}

int ObCreateTableHelper::lock_objects_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  }
  // 1. lock database name first
  if (FAILEDx(lock_database_by_obj_name_())) {
    LOG_WARN("fail to lock databases by obj name", KR(ret), K_(tenant_id));
  }
  // 2. lock objects by name
  if (FAILEDx(lock_objects_by_name_())) {
    LOG_WARN("fail to lock objects by name", KR(ret), K_(tenant_id));
  }
  DEBUG_SYNC(AFTER_PARALLEL_DDL_LOCK_OBJ_BY_NAME);
  // 3. prefetch schemas
  if (FAILEDx(prefetch_schemas_())) {
    LOG_WARN("fail to prefech schemas", KR(ret), K_(tenant_id));
  }
  // 4. lock objects by id
  if (FAILEDx(lock_objects_by_id_())) {
    LOG_WARN("fail to lock objects by name", KR(ret), K_(tenant_id));
  }
  // 5. lock objects by id after related objects are locked.
  if (FAILEDx(post_lock_objects_by_id_())) {
    LOG_WARN("fail to lock objects by id in post", KR(ret));
  }
  // 6. check ddl conflict
  if (FAILEDx(check_ddl_conflict_())) {
    LOG_WARN("fail to check ddl confict", KR(ret));
  }
  RS_TRACE(lock_objects);
  return ret;
}

// Lock ddl related database by name
// 1. database        (S)
// - to create table
// 2. parent database (S)
// - in foreign key
int ObCreateTableHelper::lock_database_by_obj_name_()
{
  int ret = OB_SUCCESS;
  const int64_t start_ts =  ObTimeUtility::current_time();
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else {
    const ObString &database_name = arg_.db_name_;
    if (OB_FAIL(add_lock_object_by_database_name_(database_name, transaction::tablelock::SHARE))) {
      LOG_WARN("fail to lock database by name", KR(ret), K_(tenant_id), K(database_name));
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < arg_.foreign_key_arg_list_.count(); i++) {
      const obrpc::ObCreateForeignKeyArg &foreign_key_arg = arg_.foreign_key_arg_list_.at(i);
      const ObString &parent_database_name = foreign_key_arg.parent_database_;
       if (OB_FAIL(add_lock_object_by_database_name_(parent_database_name, transaction::tablelock::SHARE))) {
         LOG_WARN("fail to lock database by name", KR(ret), K_(tenant_id), K(parent_database_name));
       }
    } // end for

    if (FAILEDx(lock_databases_by_name_())) {
      LOG_WARN("fail to lock databases by name", KR(ret), K_(tenant_id));
    }
  }
  const int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
  LOG_INFO("lock databases", KR(ret), K_(tenant_id), K(cost_ts));
  return ret;
}

// Lock related objects' name for create table (`X` for EXCLUSIVE, `S` for SHARE):
// 1. table               (X)
// 2. index               (X)
// - in oracle mode
// 3. constraint          (X)
// 4. foreign key         (X)
// 5. tablegroup          (S)
// 6. sequence            (X)
// - Operation to lock sequence name will be delayed to generate_schemas_() stage.
// 7. parent table        (X)
// 8. mock fk parent table(X)
int ObCreateTableHelper::lock_objects_by_name_()
{
  int ret = OB_SUCCESS;
  bool is_oracle_mode = false;
  const int64_t start_ts =  ObTimeUtility::current_time();
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_tenant_id(
             tenant_id_, is_oracle_mode))) {
    LOG_WARN("fail to check is oracle mode", KR(ret));
  } else {
    const ObString &database_name = arg_.db_name_;
    const ObTableSchema &table = arg_.schema_;

    // 1. table
    const ObString &table_name = table.get_table_name();
    if (OB_FAIL(add_lock_object_by_name_(database_name, table_name,
        share::schema::TABLE_SCHEMA, transaction::tablelock::EXCLUSIVE))) {
      LOG_WARN("fail to lock object by table name", KR(ret), K_(tenant_id), K(database_name), K(table_name));
    }

    // 2. index (oracle) :
    // 1) oracle mode: index name is unique in user(database)
    // 2) mysql mode: index name is unique in table
    if (OB_SUCC(ret) && is_oracle_mode) {
      for (int64_t i = 0; OB_SUCC(ret) && i < arg_.index_arg_list_.size(); i++) {
        const ObString &index_name = arg_.index_arg_list_.at(i).index_name_; // original index name
        if (OB_FAIL(add_lock_object_by_name_(database_name, index_name,
            share::schema::TABLE_SCHEMA, transaction::tablelock::EXCLUSIVE))) {
          LOG_WARN("fail to lock object by index name", KR(ret), K_(tenant_id), K(database_name), K(index_name));
        }
      } // end for
    }

    // 3. constraint
    for (int64_t i = 0; OB_SUCC(ret) && i < arg_.constraint_list_.count(); i++) {
      const ObString &cst_name = arg_.constraint_list_.at(i).get_constraint_name_str();
      if (OB_FAIL(add_lock_object_by_name_(database_name, cst_name,
          share::schema::CONSTRAINT_SCHEMA, transaction::tablelock::EXCLUSIVE))) {
        LOG_WARN("fail to lock object by constraint name", KR(ret), K_(tenant_id), K(database_name), K(cst_name));
      }
    } // end for

    // 4. foreign key
    for (int64_t i = 0; OB_SUCC(ret) && i < arg_.foreign_key_arg_list_.count(); i++) {
      const ObString &fk_name = arg_.foreign_key_arg_list_.at(i).foreign_key_name_;
      if (OB_FAIL(add_lock_object_by_name_(database_name, fk_name,
          share::schema::FOREIGN_KEY_SCHEMA, transaction::tablelock::EXCLUSIVE))) {
        LOG_WARN("fail to lock object by foreign key name", KR(ret), K_(tenant_id), K(database_name), K(fk_name));
      }
    } // end for

    // 5. tablegroup
    const ObString &tablegroup_name = arg_.schema_.get_tablegroup_name();
    if (OB_SUCC(ret) && !tablegroup_name.empty()) {
      ObString mock_database_name(OB_SYS_DATABASE_NAME);  // consider that tablegroup may across databases.
      if (OB_FAIL(add_lock_object_by_name_(mock_database_name, tablegroup_name,
          share::schema::TABLEGROUP_SCHEMA, transaction::tablelock::SHARE))) {
        LOG_WARN("fail to lock object by tablegroup name", KR(ret), K_(tenant_id), K(database_name), K(tablegroup_name));
      }
    }

    // 6. sequence
    // - will be delayed to generate_schemas_() stage.

    // 7. parent table/mock fk parent table
    // - here we don't distinguish between table and mocked table.
    for (int64_t i = 0; OB_SUCC(ret) && i < arg_.foreign_key_arg_list_.count(); i++) {
      const obrpc::ObCreateForeignKeyArg &foreign_key_arg = arg_.foreign_key_arg_list_.at(i);
      const ObString &parent_database_name = foreign_key_arg.parent_database_;
      const ObString &parent_table_name = foreign_key_arg.parent_table_;
      if (OB_FAIL(add_lock_object_by_name_(parent_database_name, parent_table_name,
          share::schema::TABLE_SCHEMA, transaction::tablelock::EXCLUSIVE))) {
        LOG_WARN("fail to lock object by parent table", KR(ret),
                 K_(tenant_id), K(parent_database_name), K(parent_table_name));
      }
    } // end for

    if (FAILEDx(lock_existed_objects_by_name_())) {
      LOG_WARN("fail to lock objects by name", KR(ret), K_(tenant_id));
    }
  }
  const int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
  LOG_INFO("lock objects by name", KR(ret), K_(tenant_id), K(cost_ts));
  return ret;
}

// lock related objects' id for create table (`X` for EXCLUSIVE, `S` for SHARE):
// 1. tablegroup          (S)
// 2. audit               (S)
// - add share lock for OB_AUDIT_MOCK_USER_ID
// 3. tablespace          (S)
// 4. parent table        (X)
// 5. mock fk parent table(X)
// 6. udt                 (S)

// Specially, when table name is duplicated with existed mock fk parent table name,
// we may replace mock fk parent table with new table and modified related foreign key/child table.
// So after lock table/mock fk parent table by name, we should lock all foreign keys/child tables
// from mock fk parent table by id.
//
// 6. mock fk parent table for replacement (X)
int ObCreateTableHelper::lock_objects_by_id_()
{
  int ret = OB_SUCCESS;
  const int64_t start_ts =  ObTimeUtility::current_time();
  const ObTableSchema &table = arg_.schema_;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  }
  // 1. tablegroup
  const uint64_t tablegroup_id = table.get_tablegroup_id();
  if (OB_SUCC(ret) && OB_INVALID_ID != tablegroup_id) {
    if (OB_FAIL(add_lock_object_by_id_(tablegroup_id, // filled in set_tablegroup_id_()
        share::schema::TABLEGROUP_SCHEMA, transaction::tablelock::SHARE))) {
      LOG_WARN("fail to lock tablegroup_id", KR(ret), K_(tenant_id), K(tablegroup_id));
    }
  }
  // 2. audit (lock by user_id)
  if (OB_SUCC(ret)) {
    if (OB_FAIL(add_lock_object_by_id_(OB_AUDIT_MOCK_USER_ID,
        share::schema::USER_SCHEMA, transaction::tablelock::SHARE))) {
      LOG_WARN("fail to lock user_id", KR(ret), K_(tenant_id), "user_id", OB_AUDIT_MOCK_USER_ID);
    }
  }
  // 3. tablespace
  const uint64_t tablespace_id = arg_.schema_.get_tablespace_id();
  if (OB_SUCC(ret) && OB_INVALID_ID != tablespace_id) {
    if (OB_FAIL(add_lock_object_by_id_(tablespace_id,
        share::schema::TABLESPACE_SCHEMA, transaction::tablelock::SHARE))) {
      LOG_WARN("fail to lock tablespace_id", KR(ret), K_(tenant_id), K(tablespace_id));
    }
  }
  // 4. parent table/mock fk parent table
  for (int64_t i = 0; OB_SUCC(ret) && i < arg_.foreign_key_arg_list_.count(); i++) {
    const obrpc::ObCreateForeignKeyArg &foreign_key_arg = arg_.foreign_key_arg_list_.at(i);
    if (OB_INVALID_ID != foreign_key_arg.parent_table_id_) { // filled in check_and_set_parent_table_id_()
      if (OB_FAIL(add_lock_object_by_id_(foreign_key_arg.parent_table_id_,
          share::schema::TABLE_SCHEMA, transaction::tablelock::EXCLUSIVE))) {
        LOG_WARN("fail to lock parent table id",
                 KR(ret), K_(tenant_id), "parent_table_id", foreign_key_arg.parent_table_id_);
      }
    }
  } // end for
  // 5. lock mock fk parent table for replacement
  if (OB_SUCC(ret)) {
    const ObString &table_name = table.get_table_name();
    const uint64_t database_id = table.get_database_id();
    //TODO(yanmu.ztl): this interface has poor performance.
    if (OB_FAIL(latest_schema_guard_.get_mock_fk_parent_table_id(
        database_id, table_name, replace_mock_fk_parent_table_id_))) {
      LOG_WARN("fail to ge mock fk parent table id",
               KR(ret), K_(tenant_id), K(database_id), K(table_name));
    } else if (OB_INVALID_ID != replace_mock_fk_parent_table_id_) {
      // has existed mock fk parent table
      if (OB_FAIL(add_lock_object_by_id_(replace_mock_fk_parent_table_id_,
          share::schema::TABLE_SCHEMA, transaction::tablelock::EXCLUSIVE))) {
        LOG_WARN("fail to lock mock fk parent table id",
                 KR(ret), K_(tenant_id), K_(replace_mock_fk_parent_table_id));
      }
    }
  }
  // 6. udt
  if (OB_SUCC(ret)) {
    ObTableSchema::const_column_iterator begin = table.column_begin();
    ObTableSchema::const_column_iterator end = table.column_end();
    ObSchemaGetterGuard guard;
    if (OB_FAIL(schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, guard))) {
      LOG_WARN("fail to get schema guard", KR(ret));
    }
    for (; OB_SUCC(ret) && begin != end; begin++) {
      ObColumnSchemaV2 *col = (*begin);
      if (OB_ISNULL(col)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get column schema failed", KR(ret));
      } else if (col->get_meta_type().is_user_defined_sql_type()) {
        const uint64_t udt_id = col->get_sub_data_type();
        if (is_inner_object_id(udt_id) && !is_sys_tenant(tenant_id_)) {
          // can't add object lock across tenant, assumed that sys inner udt won't be changed.
          const ObUDTTypeInfo *udt_info = NULL;
          if (OB_FAIL(guard.get_udt_info(OB_SYS_TENANT_ID, udt_id, udt_info))) {
            LOG_WARN("fail to get udt info", KR(ret), K(udt_id));
          } else if (OB_ISNULL(udt_info)) {
            ret = OB_ERR_PARALLEL_DDL_CONFLICT;
            LOG_WARN("inner udt not found", KR(ret), K(udt_id));
          }
        } else if (OB_FAIL(add_lock_object_by_id_(udt_id,
                   share::schema::UDT_SCHEMA, transaction::tablelock::SHARE))) {
          LOG_WARN("fail to lock udt id", KR(ret), K_(tenant_id), K(udt_id));
        }
      }
    } // end for
  }

  if (FAILEDx(lock_existed_objects_by_id_())) {
    LOG_WARN("fail to lock objects by id", KR(ret));
  }
  const int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
  LOG_INFO("lock objects by id", KR(ret), K_(tenant_id), K(cost_ts));
  return ret;
}

// tablegroup/mock fk parent table for replacement are locked by id in lock_objects_by_id_() first.
// 1. foreign key         (X)
// - from mock fk parent table
// 2. child table         (X)
// - from mock fk parent table
// 3. primary table       (S)
// - in tablegroup
//
// TODO:(yanmu.ztl)
// small timeout should be used here to avoid deadlock problem
// since we have already locked some objects by id.
int ObCreateTableHelper::post_lock_objects_by_id_()
{
  int ret = OB_SUCCESS;
  const int64_t start_ts =  ObTimeUtility::current_time();
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  }
  // lock foreign key/child table if need replace existed mock fk parent table.
  if (OB_SUCC(ret) && OB_INVALID_ID != replace_mock_fk_parent_table_id_) {
    const ObMockFKParentTableSchema *mock_fk_parent_table = NULL;
    if (OB_FAIL(latest_schema_guard_.get_mock_fk_parent_table_schema(
        replace_mock_fk_parent_table_id_, mock_fk_parent_table))) {
      LOG_WARN("fail to get mock fk parent table schema",
               KR(ret), K_(tenant_id), K_(replace_mock_fk_parent_table_id));
    } else if (OB_ISNULL(mock_fk_parent_table)) {
      ret = OB_ERR_PARALLEL_DDL_CONFLICT;
      LOG_WARN("mock fk parent table not exist, ddl need retry",
               KR(ret), K_(tenant_id), K_(replace_mock_fk_parent_table_id));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < mock_fk_parent_table->get_foreign_key_infos().count(); i++) {
        const ObForeignKeyInfo &foreign_key = mock_fk_parent_table->get_foreign_key_infos().at(i);
        const uint64_t foreign_key_id = foreign_key.foreign_key_id_;
        const uint64_t child_table_id = foreign_key.child_table_id_;
        if (OB_FAIL(add_lock_object_by_id_(child_table_id,
            share::schema::TABLE_SCHEMA, transaction::tablelock::EXCLUSIVE))) {
          LOG_WARN("fail to lock child table",
                   KR(ret), K_(tenant_id), K(child_table_id));
        } else if (OB_FAIL(add_lock_object_by_id_(foreign_key_id,
            share::schema::FOREIGN_KEY_SCHEMA, transaction::tablelock::EXCLUSIVE))) {
          LOG_WARN("fail to lock foreign key",
                   KR(ret), K_(tenant_id), K(foreign_key_id));
        }
      } // end for
    }
  }

  // TODO:(yanmu.ztl) lock primary table in tablegroup
  if (OB_SUCC(ret) && OB_INVALID_ID != arg_.schema_.get_tablegroup_id()) {
  }

  if (FAILEDx(lock_existed_objects_by_id_())) {
    LOG_WARN("fail to lock objects by id", KR(ret));
  }
  const int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
  LOG_INFO("lock objects by id in post", KR(ret), K_(tenant_id), K(cost_ts));
  return ret;
}

int ObCreateTableHelper::check_ddl_conflict_()
{
  int ret = OB_SUCCESS;
  const int64_t start_ts =  ObTimeUtility::current_time();
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (!arg_.is_need_check_based_schema_objects()) {
    // skip
  } else {
    ObArray<uint64_t> parent_table_ids;
    ObArray<uint64_t> mock_fk_parent_table_ids;
    // check schema object infos are all existed.
    for (int64_t i = 0; OB_SUCC(ret) && (i < arg_.based_schema_object_infos_.count()); ++i) {
      const ObBasedSchemaObjectInfo &info = arg_.based_schema_object_infos_.at(i);
      if (MOCK_FK_PARENT_TABLE_SCHEMA == info.schema_type_
          || TABLE_SCHEMA == info.schema_type_) {
        bool find = false;
        for (int64_t j = 0; OB_SUCC(ret) && !find && j < arg_.foreign_key_arg_list_.count(); j++) {
          const obrpc::ObCreateForeignKeyArg &foreign_key_arg = arg_.foreign_key_arg_list_.at(j);
          if (MOCK_FK_PARENT_TABLE_SCHEMA == info.schema_type_
              && foreign_key_arg.is_parent_table_mock_
              && info.schema_id_ == foreign_key_arg.parent_table_id_) {
            find = true;
          } else if (TABLE_SCHEMA == info.schema_type_
                     && !foreign_key_arg.is_parent_table_mock_
                     && info.schema_id_ == foreign_key_arg.parent_table_id_) {
            find = true;
          }
        } // end for
        if (OB_SUCC(ret) && !find) {
          ret = OB_ERR_PARALLEL_DDL_CONFLICT;
          LOG_WARN("parent table may change, ddl need retry",
                   KR(ret), K_(tenant_id), K(info));
        }
        if (OB_FAIL(ret)) {
        } else if (MOCK_FK_PARENT_TABLE_SCHEMA == info.schema_type_) {
          if (!has_exist_in_array(mock_fk_parent_table_ids, info.schema_id_)
              && OB_FAIL(mock_fk_parent_table_ids.push_back(info.schema_id_))) {
            LOG_WARN("fail to push back mock fk parent table id", KR(ret), K(info));
          }
        } else if (TABLE_SCHEMA == info.schema_type_) {
          if (!has_exist_in_array(parent_table_ids, info.schema_id_)
              && OB_FAIL(parent_table_ids.push_back(info.schema_id_))) {
            LOG_WARN("fail to push back parent table id", KR(ret), K(info));
          }
        }
      }
    } // end for

    ObArray<ObSchemaIdVersion> parent_table_versions;
    if (OB_SUCC(ret) && parent_table_ids.count() > 0) {
      if (OB_FAIL(parent_table_versions.reserve(parent_table_ids.count()))) {
        LOG_WARN("fail to reserve array", KR(ret));
      } else if (OB_FAIL(latest_schema_guard_.get_table_schema_versions(
                 parent_table_ids, parent_table_versions))) {
        LOG_WARN("fail to get table schema versions", KR(ret));
      } else if (parent_table_ids.count() != parent_table_versions.count()) {
        ret = OB_ERR_PARALLEL_DDL_CONFLICT;
        LOG_WARN("parent table may be deleted, ddl need retry",
                 KR(ret), K_(tenant_id), "base_objs_cnt", parent_table_ids.count(),
                 "fetch_cnt", parent_table_versions.count());
      }
    }

    ObArray<ObSchemaIdVersion> mock_fk_parent_table_versions;
    if (OB_SUCC(ret) && mock_fk_parent_table_ids.count() > 0) {
      if (OB_FAIL(mock_fk_parent_table_versions.reserve(mock_fk_parent_table_ids.count()))) {
        LOG_WARN("fail to reserve array", KR(ret));
      } else if (OB_FAIL(latest_schema_guard_.get_mock_fk_parent_table_schema_versions(
                 mock_fk_parent_table_ids, mock_fk_parent_table_versions))) {
        LOG_WARN("fail to get table schema versions", KR(ret));
      } else if (mock_fk_parent_table_ids.count() != mock_fk_parent_table_versions.count()) {
        ret = OB_ERR_PARALLEL_DDL_CONFLICT;
        LOG_WARN("mock fk parent table may be deleted, ddl need retry",
                 KR(ret), K_(tenant_id), "base_objs_cnt", mock_fk_parent_table_ids.count(),
                 "fetch_cnt", mock_fk_parent_table_versions.count());
      }
    }

    if (OB_SUCC(ret)) {
      for (int64_t i = 0; OB_SUCC(ret) && (i < arg_.based_schema_object_infos_.count()); ++i) {
        const ObBasedSchemaObjectInfo &info = arg_.based_schema_object_infos_.at(i);
        if (MOCK_FK_PARENT_TABLE_SCHEMA == info.schema_type_
            || TABLE_SCHEMA == info.schema_type_) {
          bool find = false;
          for (int64_t j = 0; OB_SUCC(ret) && !find && j < parent_table_versions.count(); j++) {
            const ObSchemaIdVersion &version = parent_table_versions.at(j);
            if (version.get_schema_id() == info.schema_id_) {
              find = true;
              if (version.get_schema_version() != info.schema_version_) {
                ret = OB_ERR_PARALLEL_DDL_CONFLICT;
                LOG_WARN("parent table may be changed, ddl need retry",
                         KR(ret), K_(tenant_id), K(info), K(version));
              }
            }
          } // end for
          for (int64_t j = 0; OB_SUCC(ret) && !find && j < mock_fk_parent_table_versions.count(); j++) {
            const ObSchemaIdVersion &version = mock_fk_parent_table_versions.at(j);
            if (version.get_schema_id() == info.schema_id_) {
              find = true;
              if (version.get_schema_version() != info.schema_version_) {
                ret = OB_ERR_PARALLEL_DDL_CONFLICT;
                LOG_WARN("mock fk parent table may be changed, ddl need retry",
                         KR(ret), K_(tenant_id), K(info), K(version));
              }
            }
          } // end for
          if (OB_SUCC(ret) && !find) {
            ret = OB_ERR_PARALLEL_DDL_CONFLICT;
            LOG_WARN("parent table may be deleted, ddl need retry",
                     KR(ret), K_(tenant_id), K(info));
          }
        }
      } // end for
    }

    // for replace mock fk parent table:
    // 1. check child tables still exist.
    // 2. cache child tables' schema before gen_task_id_and_schema_versions_() to increase throughput.
    if (OB_SUCC(ret) && OB_INVALID_ID != replace_mock_fk_parent_table_id_) {
      const ObMockFKParentTableSchema *mock_fk_parent_table = NULL;
      if (OB_FAIL(latest_schema_guard_.get_mock_fk_parent_table_schema(
          replace_mock_fk_parent_table_id_, mock_fk_parent_table))) {
        LOG_WARN("fail to get mock fk parent table schema",
                 KR(ret), K_(tenant_id), K_(replace_mock_fk_parent_table_id));
      } else if (OB_ISNULL(mock_fk_parent_table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("mock fk parent table not exist after lock obj",
                 KR(ret), K_(tenant_id), K_(replace_mock_fk_parent_table_id));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < mock_fk_parent_table->get_foreign_key_infos().count(); i++) {
        const ObForeignKeyInfo &foreign_key = mock_fk_parent_table->get_foreign_key_infos().at(i);
        const uint64_t child_table_id = foreign_key.child_table_id_;
        const ObTableSchema *child_table = NULL;
        if (OB_FAIL(latest_schema_guard_.get_table_schema(child_table_id, child_table))) {
          LOG_WARN("fail to get table schema", KR(ret), K_(tenant_id), K(child_table_id));
        } else if (OB_ISNULL(child_table)) {
          ret = OB_ERR_PARALLEL_DDL_CONFLICT;
          LOG_WARN("child table is not exist", KR(ret), K_(tenant_id), K(child_table_id));
        }
      } // end for
    }

    // check udt exist & not changed
    for (int64_t i = 0; OB_SUCC(ret) && (i < arg_.based_schema_object_infos_.count()); ++i) {
      const ObBasedSchemaObjectInfo &info = arg_.based_schema_object_infos_.at(i);
      if (UDT_SCHEMA == info.schema_type_) {
        const uint64_t udt_id = info.schema_id_;
        const ObUDTTypeInfo *udt_info = NULL;
        if (is_inner_object_id(udt_id) && !is_sys_tenant(tenant_id_)) {
          // can't add object lock across tenant, assumed that sys inner udt won't be changed.
        } else if (OB_FAIL(latest_schema_guard_.get_udt_info(udt_id, udt_info))) {
          LOG_WARN("fail to get udt info", KR(ret), K_(tenant_id), K(udt_id), K(info));
        } else if (OB_ISNULL(udt_info)) {
          ret = OB_ERR_PARALLEL_DDL_CONFLICT;
          LOG_WARN("udt doesn't exist", KR(ret), K_(tenant_id), K(udt_id));
        } else if (udt_info->get_schema_version() != info.schema_version_) {
          ret = OB_ERR_PARALLEL_DDL_CONFLICT;
          LOG_WARN("udt changed", KR(ret), K(info), KPC(udt_info));
        }
      }
    } // end for
  }
  const int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
  LOG_INFO("check ddl confict", KR(ret), K_(tenant_id), K(cost_ts));
  return ret;
}

int ObCreateTableHelper::prefetch_schemas_()
{
  int ret = OB_SUCCESS;
  const int64_t start_ts =  ObTimeUtility::current_time();
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(check_and_set_database_id_())) {
    LOG_WARN("fail to check and set database id", KR(ret));
  } else if (OB_FAIL(check_table_name_())) {
    LOG_WARN("fail to check table name", KR(ret));
  } else if (OB_FAIL(set_tablegroup_id_())) {
    LOG_WARN("fail to set tablegroup id", KR(ret));
  } else if (OB_FAIL(check_and_set_parent_table_id_())) {
    LOG_WARN("fail to check and set parent table id", KR(ret));
  }
  const int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
  LOG_INFO("prefetch schemas", KR(ret), K_(tenant_id), K(cost_ts));
  return ret;
}

int ObCreateTableHelper::check_and_set_database_id_()
{
  int ret = OB_SUCCESS;
  const ObString &database_name = arg_.db_name_;
  uint64_t database_id = OB_INVALID_ID;
  const ObDatabaseSchema *database_schema = NULL;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(latest_schema_guard_.get_database_id(database_name, database_id))) {
    LOG_WARN("fail to get database id", KR(ret), K_(tenant_id), K(database_name));
  } else if (OB_UNLIKELY(OB_INVALID_ID == database_id)) {
    ret = OB_ERR_BAD_DATABASE;
    LOG_WARN("database not exist",  KR(ret), K_(tenant_id), K(database_name));
    LOG_USER_ERROR(OB_ERR_BAD_DATABASE, database_name.length(), database_name.ptr());
  } else if (OB_FAIL(latest_schema_guard_.get_database_schema(database_id, database_schema))) {
    LOG_WARN("fail to get database schema", KR(ret), K_(tenant_id), K(database_id), K(database_name));
  } else if (OB_ISNULL(database_schema)) {
    ret = OB_ERR_BAD_DATABASE;
    LOG_WARN("database not exist", KR(ret), K_(tenant_id), K(database_id), K(database_name));
    LOG_USER_ERROR(OB_ERR_BAD_DATABASE, database_name.length(), database_name.ptr());
  } else if (!arg_.is_inner_ && database_schema->is_in_recyclebin()) {
    ret = OB_ERR_OPERATION_ON_RECYCLE_OBJECT;
    LOG_WARN("Can't not create table in database which is in recyclebin",
             KR(ret), K_(tenant_id), K(database_id), K(database_name));
  } else {
    (void) const_cast<ObTableSchema&>(arg_.schema_).set_database_id(database_id);
  }
  return ret;
}

int ObCreateTableHelper::check_table_name_()
{
  int ret = OB_SUCCESS;
  bool is_oracle_mode = false;
  const ObTableSchema &table = arg_.schema_;
  const uint64_t database_id = table.get_database_id();
  const ObString &table_name = table.get_table_name();
  // session_id is > 0 when table is mysql tmp table or creating ctas table.
  const uint64_t session_id = table.get_session_id();
  bool if_not_exist = arg_.if_not_exist_;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_tenant_id(tenant_id_, is_oracle_mode))) {
    LOG_WARN("fail to check is oracle mode", KR(ret));
  } else if (is_oracle_mode) {
    if (OB_FAIL(latest_schema_guard_.check_oracle_object_exist(
        database_id, session_id, table_name, TABLE_SCHEMA,
        INVALID_ROUTINE_TYPE, if_not_exist))) {
      LOG_WARN("fail to check oracle object exist", KR(ret),
               K(database_id), K(session_id), K(table_name), K(if_not_exist));
    }
  } else {
    uint64_t synonym_id = OB_INVALID_ID;
    uint64_t table_id = OB_INVALID_ID;
    ObTableType table_type = MAX_TABLE_TYPE;
    int64_t schema_version = OB_INVALID_VERSION;
    if (OB_FAIL(latest_schema_guard_.get_synonym_id(database_id, table_name, synonym_id))) {
      LOG_WARN("fail to get synonymn_id", KR(ret), K_(tenant_id), K(database_id), K(table_name));
    } else if (OB_UNLIKELY(OB_INVALID_ID != synonym_id)) {
      ret = OB_ERR_EXIST_OBJECT;
      LOG_WARN("Name is already used by an existing object",
               KR(ret), K_(tenant_id), K(database_id), K(table_name), K(synonym_id));
    } else if (OB_FAIL(latest_schema_guard_.get_table_id(
               database_id, session_id, table_name, table_id, table_type, schema_version))) {
      LOG_WARN("fail to get table_id", KR(ret), K_(tenant_id), K(database_id), K(session_id), K(table_name));
    } else if (OB_UNLIKELY(OB_INVALID_ID != table_id)) {
      if (table.is_mysql_tmp_table()
          && !(is_inner_table(table_id) || is_mysql_tmp_table(table_type))) {
        // mysql tmp table name is only duplicated in the same session.
      } else {
        // Raise error here to skip the following steps,
        // ret will be overwrite if if_not_exist_ is true before rpc returns.
        ret = OB_ERR_TABLE_EXIST;
        res_.table_id_ = table_id;
        res_.schema_version_ = schema_version;
        LOG_WARN("table exist", KR(ret), K_(tenant_id), K(database_id),
                 K(session_id), K(table_name), K(table_id), K(schema_version),
                 K(arg_.if_not_exist_));
      }
    }
  }
  return ret;
}

int ObCreateTableHelper::set_tablegroup_id_()
{
  int ret = OB_SUCCESS;
  const ObTableSchema &table = arg_.schema_;
  const ObString &tablegroup_name = table.get_tablegroup_name();
  uint64_t tablegroup_id = OB_INVALID_ID;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (tablegroup_name.empty()) {
    if (table.has_partition()) {
      // try use default tablegroup id
      const uint64_t database_id = table.get_database_id();
      const ObDatabaseSchema *database_schema = NULL;
      if (OB_FAIL(latest_schema_guard_.get_database_schema(database_id, database_schema))) {
        LOG_WARN("fail to get database schema", KR(ret), K_(tenant_id), K(database_id));
      } else if (OB_ISNULL(database_schema)) {
        ret = OB_ERR_BAD_DATABASE;
        LOG_WARN("database not exist", KR(ret), K_(tenant_id), K(database_id));
      } else {
        tablegroup_id = database_schema->get_default_tablegroup_id();
      }

      if (OB_SUCC(ret) && OB_INVALID_ID == tablegroup_id) {
        const ObTenantSchema *tenant_schema = NULL;
        if (OB_FAIL(latest_schema_guard_.get_tenant_schema(tenant_id_, tenant_schema))) {
          LOG_WARN("fail to get tenant schema", KR(ret), K_(tenant_id));
        } else if (OB_ISNULL(tenant_schema)) {
          ret = OB_TENANT_NOT_EXIST;
          LOG_WARN("tenant not exist", KR(ret), K_(tenant_id));
        } else {
          tablegroup_id = tenant_schema->get_default_tablegroup_id();
        }
      }
    }
  } else {
    if (!table.has_partition()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("table which has no partitions with tablegroup", KR(ret), K(table));
    } else if (OB_FAIL(latest_schema_guard_.get_tablegroup_id(
               tablegroup_name, tablegroup_id))) {
      LOG_WARN("fail to get tablegroup id", KR(ret), K_(tenant_id), K(tablegroup_name));
    } else if (OB_UNLIKELY(OB_INVALID_ID == tablegroup_id)) {
      ret = OB_TABLEGROUP_NOT_EXIST;
      LOG_WARN("tabelgroup not exist ", KR(ret), K_(tenant_id), K(tablegroup_name));
    } else {}
  }

  if (OB_SUCC(ret) && OB_INVALID_ID != tablegroup_id) {
    // TODO:(yanmu.ztl) after 4.2, we can use ObSimpleTableSchema instead of ObTablegroupSchema
    const ObTablegroupSchema *tablegroup_schema = NULL;
    if (OB_FAIL(latest_schema_guard_.get_tablegroup_schema(tablegroup_id, tablegroup_schema))) {
      LOG_WARN("fail to get tablegroup schema", KR(ret), K_(tenant_id), K(tablegroup_id));
    } else if (OB_ISNULL(tablegroup_schema)) {
      ret = OB_TABLEGROUP_NOT_EXIST;
      LOG_WARN("tabelgroup not exist ", KR(ret), K_(tenant_id), K(tablegroup_id));
    } else if (OB_UNLIKELY(ObDuplicateScope::DUPLICATE_SCOPE_NONE != table.get_duplicate_scope()
               && OB_INVALID_ID != tablegroup_id)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("duplicated table in tablegroup is not supported", K(ret),
               "table_id", table.get_table_id(),
               "tablegroup_id", table.get_tablegroup_id());
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "duplicated table in tablegroup");
    }
  }

  if (OB_SUCC(ret)) {
    (void) const_cast<ObTableSchema&>(table).set_tablegroup_id(tablegroup_id);
  }
  return ret;
}

// parent table id won't change after parent table name is locked.
int ObCreateTableHelper::check_and_set_parent_table_id_()
{
  int ret = OB_SUCCESS;
  const ObTableSchema &table = arg_.schema_;
  const ObString &database_name = arg_.db_name_;
  const ObString &table_name = table.get_table_name();
  const uint64_t session_id = table.get_session_id();
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < arg_.foreign_key_arg_list_.count(); i++) {
      const obrpc::ObCreateForeignKeyArg &foreign_key_arg = arg_.foreign_key_arg_list_.at(i);
      const ObString &parent_database_name = foreign_key_arg.parent_database_;
      const ObString &parent_table_name = foreign_key_arg.parent_table_;
      if (0 == parent_database_name.case_compare(database_name)
          && 0 == parent_table_name.case_compare(table_name)) {
        // self reference
      } else {
        uint64_t parent_database_id = OB_INVALID_ID;
        const ObDatabaseSchema *parent_database = NULL;
        if (0 == parent_database_name.case_compare(database_name)) {
          parent_database_id = table.get_database_id();
        } else if (OB_FAIL(latest_schema_guard_.get_database_id(parent_database_name, parent_database_id))) {
          LOG_WARN("fail to get database id", KR(ret), K_(tenant_id), K(parent_database_name));
        } else if (OB_UNLIKELY(OB_INVALID_ID == parent_database_id)) {
          ret = OB_ERR_BAD_DATABASE;
          LOG_WARN("parent database not exist", KR(ret), K_(tenant_id), K(parent_database_name));
          LOG_USER_ERROR(OB_ERR_BAD_DATABASE, parent_database_name.length(), parent_database_name.ptr());
        } else if (OB_FAIL(latest_schema_guard_.get_database_schema(parent_database_id, parent_database))) {
          LOG_WARN("fail to get database schema", KR(ret), K_(tenant_id), K(parent_database_id));
        } else if (OB_ISNULL(parent_database)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("database schema is null", KR(ret), K_(tenant_id), K(parent_database_id));
        } else if (parent_database->is_in_recyclebin()) {
          ret = OB_ERR_OPERATION_ON_RECYCLE_OBJECT;
          LOG_WARN("Can't not do ddl on db in recyclebin", KR(ret), K_(tenant_id), K(parent_database_id));
        }

        uint64_t parent_table_id = OB_INVALID_ID;
        ObTableType parent_table_type = ObTableType::MAX_TABLE_TYPE;
        int64_t parent_table_schema_version = OB_INVALID_VERSION; // not used
        if (FAILEDx(latest_schema_guard_.get_table_id(
            parent_database_id, session_id, parent_table_name,
            parent_table_id, parent_table_type, parent_table_schema_version))) {
          LOG_WARN("fail to get parent table id", KR(ret), K_(tenant_id),
                   K(session_id), K(parent_database_id), K(parent_table_name));
        } else if (OB_UNLIKELY(OB_INVALID_ID == parent_table_id)) {
          if (!foreign_key_arg.is_parent_table_mock_) {
            ret = OB_TABLE_NOT_EXIST;
            LOG_WARN("parent table not exist", KR(ret), K_(tenant_id),
                     K(session_id), K(parent_database_id), K(parent_table_name));
            LOG_USER_ERROR(OB_TABLE_NOT_EXIST,
                           to_cstring(parent_database_name),
                           to_cstring(parent_table_name));
          } else {
            //TODO(yanmu.ztl): this interface has poor performance.
            if (OB_FAIL(latest_schema_guard_.get_mock_fk_parent_table_id(
                parent_database_id, parent_table_name, parent_table_id))) {
              LOG_WARN("fail to get mock fk parent table id", KR(ret),
                       K_(tenant_id), K(parent_database_id), K(parent_table_name));
            } else if (OB_UNLIKELY(OB_INVALID_ID == parent_table_id)) {
              LOG_INFO("mock fk parent table not exist", KR(ret), K_(tenant_id),
                       K(parent_database_id), K(parent_table_name));
            }
          }
        } else {
          if (foreign_key_arg.is_parent_table_mock_) {
            ret = OB_ERR_PARALLEL_DDL_CONFLICT;
            LOG_WARN("parenet table already exist, should retry",
                     KR(ret), K_(tenant_id), K(parent_table_id), K(foreign_key_arg));
          }
        }
        // parent_table_id will be OB_INVALID_ID in the following cases:
        // 1. foreign key is self reference.
        // 2. mock fk parent table doesn't exist.
        if (OB_SUCC(ret)) {
          const_cast<obrpc::ObCreateForeignKeyArg&>(foreign_key_arg).parent_database_id_ = parent_database_id;
          const_cast<obrpc::ObCreateForeignKeyArg&>(foreign_key_arg).parent_table_id_ = parent_table_id;
        }
      }
    } // end for
  }
  return ret;
}

int ObCreateTableHelper::generate_schemas_()
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
  RS_TRACE(generate_schemas);
  return ret;
}

int ObCreateTableHelper::generate_table_schema_()
{
  int ret = OB_SUCCESS;
  HEAP_VAR(ObTableSchema, new_table) {

  // to make try_format_partition_schema() passed
  const uint64_t mock_table_id = OB_MIN_USER_OBJECT_ID + 1;
  uint64_t compat_version = 0;
  bool is_oracle_mode = false;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id_, compat_version))) {
    LOG_WARN("fail to get data version", K(ret), K_(tenant_id));
  } else if (not_compat_for_queuing_mode(compat_version) && arg_.schema_.is_new_queuing_table_mode()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN(QUEUING_MODE_NOT_COMPAT_WARN_STR, K(ret), K_(tenant_id), K(compat_version), K(arg_));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, QUEUING_MODE_NOT_COMPAT_USER_ERROR_STR);
  } else if (OB_UNLIKELY(OB_INVALID_ID != arg_.schema_.get_table_id())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("create table with table_id in 4.x is not supported",
             KR(ret), K_(tenant_id), "table_id", arg_.schema_.get_table_id());
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "create table with id is");
  } else if (OB_FAIL(new_table.assign(arg_.schema_))) {
    LOG_WARN("fail to assign table schema", KR(ret), K_(tenant_id));
  } else if (FALSE_IT(new_table.set_table_id(mock_table_id))) {
  } else if (OB_FAIL(ddl_service_->try_format_partition_schema(new_table))) {
    LOG_WARN("fail to format partition schema", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(new_table.check_if_oracle_compat_mode(is_oracle_mode))) {
    LOG_WARN("failed to get compat mode", KR(ret), K_(tenant_id));
  }

  const uint64_t tablespace_id = new_table.get_tablespace_id();
  if (OB_SUCC(ret) && OB_INVALID_ID != tablespace_id) {
    const ObTablespaceSchema *tablespace = NULL;
    if (OB_FAIL(latest_schema_guard_.get_tablespace_schema(
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
    if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id_, guard))) {
      LOG_WARN("fail to get tenant schema guard", KR(ret), K_(tenant_id));
    } else if (OB_FAIL(ddl_service_->try_check_and_set_table_schema_in_tablegroup(guard, new_table))) {
      LOG_WARN("fail to check table in tablegorup", KR(ret), K(new_table));
    }
  }

  if (OB_SUCC(ret)) {
    ObTableSchema::const_column_iterator begin = new_table.column_begin();
    ObTableSchema::const_column_iterator end = new_table.column_end();
    ObSchemaGetterGuard guard;
    if (OB_FAIL(schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, guard))) {
      LOG_WARN("fail to get schema guard", KR(ret));
    }
    for (; OB_SUCC(ret) && begin != end; begin++) {
      ObColumnSchemaV2 *col = (*begin);
      if (OB_ISNULL(col)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get column schema failed", KR(ret));
      } else if (col->get_meta_type().is_user_defined_sql_type()) {
        const uint64_t udt_id = col->get_sub_data_type();
        const ObUDTTypeInfo *udt_info = NULL;
        if (is_inner_object_id(udt_id) && !is_sys_tenant(tenant_id_)) {
          // can't add object lock across tenant, assumed that sys inner udt won't be changed.
          if (OB_FAIL(guard.get_udt_info(OB_SYS_TENANT_ID, udt_id, udt_info))) {
            LOG_WARN("fail to get udt info", KR(ret), K(udt_id));
          } else if (OB_ISNULL(udt_info)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("inner udt not found", KR(ret), K(udt_id));
          }
        } else if (OB_FAIL(latest_schema_guard_.get_udt_info(udt_id, udt_info))) {
          LOG_WARN("fail to get udt info", KR(ret), K_(tenant_id), K(udt_id));
        } else if (OB_ISNULL(udt_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("udt doesn't exist", KR(ret), K_(tenant_id), K(udt_id));
        }
      }
    } // end for
  }

  // check if constraint name duplicated
  const uint64_t database_id = new_table.get_database_id();
  bool cst_exist = false;
  const ObIArray<ObConstraint> &constraints = arg_.constraint_list_;
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
    if (OB_UNLIKELY(PARTITION_LEVEL_TWO == new_table.get_part_level()
        && !new_table.has_sub_part_template_def())) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("interval part of composited-partitioned table not support", KR(ret), K(new_table));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "interval part of composited-partitioned table without template");
    } else if (OB_UNLIKELY(1 != new_table.get_partition_key_column_num())) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("more than one partition key not support", KR(ret), K(new_table));
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "more than one partition key");
    } else if (OB_ISNULL(part_array)
               || OB_UNLIKELY(0 == part_num)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("range part array is null or part_num is 0", KR(ret), K(new_table));
    } else if (OB_ISNULL(transition_point = &part_array[part_num - 1]->get_high_bound_val())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("transition_point is null", KR(ret), KP(transition_point));
    } else if (OB_FAIL(ObPartitionUtils::check_interval_partition_table(
                       *transition_point, new_table.get_interval_range()))) {
      LOG_WARN("fail to check_interval_partition_table", KR(ret), K(new_table));
    } else if (OB_FAIL(new_table.set_transition_point(*transition_point))) {
      LOG_WARN("fail to set transition point", KR(ret), K(new_table));
    }
  }

  if (FAILEDx(new_tables_.push_back(new_table))) {
    LOG_WARN("fail to push back table", KR(ret));
  }

  } // end HEAP_VAR
  return ret;
}

int ObCreateTableHelper::generate_aux_table_schemas_()
{
  int ret = OB_SUCCESS;
  HEAP_VAR(ObTableSchema, index_schema) {

  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(new_tables_.count() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table cnt not match", KR(ret), "table_cnt", new_tables_.count());
  } else {
    ObTableSchema *data_table = &(new_tables_.at(0));

    // 0. fetch object_ids
    ObIDGenerator id_generator;
    int64_t object_cnt = arg_.index_arg_list_.size();
    bool has_lob_table = false;
    uint64_t object_id = OB_INVALID_ID;
    if (!data_table->is_external_table()) {
      for (int64_t i = 0; OB_SUCC(ret) && !has_lob_table && i < data_table->get_column_count(); i++) {
        const ObColumnSchemaV2 *column = data_table->get_column_schema_by_idx(i);
        if (OB_ISNULL(column)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column is null", KR(ret), K(i), KPC(data_table));
        } else if (is_lob_storage(column->get_data_type())) {
          has_lob_table = true;
          object_cnt += 2;
        }
      } // end for
    }
    if (FAILEDx(gen_object_ids_(object_cnt, id_generator))) {
      LOG_WARN("fail to gen object ids", KR(ret), K_(tenant_id), K(object_cnt));
    }

    // 1. build index table
    ObIndexBuilder index_builder(*ddl_service_);
    for (int64_t i = 0; OB_SUCC(ret) && i < arg_.index_arg_list_.size(); ++i) {
      index_schema.reset();
      obrpc::ObCreateIndexArg &index_arg = const_cast<obrpc::ObCreateIndexArg&>(arg_.index_arg_list_.at(i));
      if (!index_arg.index_schema_.is_partitioned_table()
          && !data_table->is_partitioned_table()) {
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
        LOG_WARN("invalid allocator", K(ret));
      } else if (OB_FAIL(ObIndexBuilderUtil::adjust_expr_index_args(
                 index_arg, *data_table, *allocator, gen_columns))) {
          LOG_WARN("fail to adjust expr index args", KR(ret), K(index_arg), KPC(data_table));
      } else if (OB_FAIL(index_builder.generate_schema(index_arg,
                                                       *data_table,
                                                       global_index_without_column_info,
                                                       false, /*generate_id*/
                                                       index_schema))) {
        LOG_WARN("generate_schema for index failed", KR(ret), K(index_arg), KPC(data_table));
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

  } // end HEAP_VAR
  return ret;
}

int ObCreateTableHelper::generate_foreign_keys_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(new_tables_.count() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid table cnt", KR(ret), "table_cnt", new_tables_.count());
  } else {
    bool is_oracle_mode = false;
    ObTableSchema &data_table = new_tables_.at(0);
    const uint64_t session_id = data_table.get_session_id();
    ObIDGenerator id_generator; // for foreign key
    const uint64_t object_cnt = arg_.foreign_key_arg_list_.count();
    if (OB_FAIL(gen_object_ids_(object_cnt, id_generator))) {
      LOG_WARN("fail to gen object ids", KR(ret), K_(tenant_id), K(object_cnt));
    } else if (OB_FAIL(data_table.check_if_oracle_compat_mode(is_oracle_mode))) {
      LOG_WARN("fail to check oracle mode", KR(ret));
    }
    // genernate foreign keys
    for (int64_t i = 0; OB_SUCC(ret) && i < arg_.foreign_key_arg_list_.count(); i++) {
      const obrpc::ObCreateForeignKeyArg &foreign_key_arg = arg_.foreign_key_arg_list_.at(i);
      const ObString &foreign_key_name = foreign_key_arg.foreign_key_name_;
      bool fk_exist = false;
      ObForeignKeyInfo foreign_key_info;
      // check if foreign key name is duplicated
      if (OB_UNLIKELY(foreign_key_name.empty())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fk name is empty", KR(ret), K(foreign_key_arg));
      } else if (OB_FAIL(check_constraint_name_exist_(data_table, foreign_key_name, true /*is_foreign_key*/, fk_exist))) {
        LOG_WARN("fail to check foreign key name exist", KR(ret), K_(tenant_id), K(foreign_key_name));
      } else if (fk_exist) {
        if (is_oracle_mode) {
          ret = OB_ERR_CONSTRAINT_NAME_DUPLICATE;
          LOG_WARN("fk name is duplicate", KR(ret), K(foreign_key_name));
        } else { // mysql mode
          ret = OB_ERR_DUP_KEY;
          LOG_USER_ERROR(OB_ERR_DUP_KEY,
                         data_table.get_table_name_str().length(),
                         data_table.get_table_name_str().ptr());
        }
      } else {
        const ObTableSchema *parent_table = NULL;
        ObMockFKParentTableSchema *new_mock_fk_parent_table = NULL;
        const ObString &parent_database_name = foreign_key_arg.parent_database_;
        const ObString &parent_table_name = foreign_key_arg.parent_table_;
        const bool self_reference = (0 == parent_table_name.case_compare(data_table.get_table_name_str())
                                     && 0 == parent_database_name.case_compare(arg_.db_name_));
        // 1. fill ref_cst_type_/ref_cst_id_
        if (self_reference) {
          // TODO: is it necessory to determine whether it is case sensitive by check sys variable
          // check whether it belongs to self reference, if so, the parent schema is child schema.
          parent_table = &data_table;
          if (CONSTRAINT_TYPE_PRIMARY_KEY == foreign_key_arg.ref_cst_type_) {
            if (is_oracle_mode) {
              ObTableSchema::const_constraint_iterator iter = parent_table->constraint_begin();
              for ( ; iter != parent_table->constraint_end(); ++iter) {
                if (CONSTRAINT_TYPE_PRIMARY_KEY == (*iter)->get_constraint_type()) {
                  foreign_key_info.ref_cst_type_ = CONSTRAINT_TYPE_PRIMARY_KEY;
                  foreign_key_info.ref_cst_id_ = (*iter)->get_constraint_id();
                  break;
                }
              } // end for
            } else {
              foreign_key_info.ref_cst_type_ = CONSTRAINT_TYPE_PRIMARY_KEY;
              foreign_key_info.ref_cst_id_ = common::OB_INVALID_ID;
            }
          } else if (CONSTRAINT_TYPE_UNIQUE_KEY == foreign_key_arg.ref_cst_type_) {
            if (OB_FAIL(ddl_service_->get_uk_cst_id_for_self_ref(new_tables_, foreign_key_arg, foreign_key_info))) {
              LOG_WARN("failed to get uk cst id for self ref", KR(ret), K(foreign_key_arg));
            }
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid foreign key ref cst type", KR(ret), K(foreign_key_arg));
          }
        } else {
          foreign_key_info.ref_cst_type_ = foreign_key_arg.ref_cst_type_;
          foreign_key_info.ref_cst_id_ = foreign_key_arg.ref_cst_id_;
          if (foreign_key_arg.is_parent_table_mock_) {
            // skip
          } else {
            const uint64_t parent_table_id = foreign_key_arg.parent_table_id_;
            if (OB_FAIL(latest_schema_guard_.get_table_schema(parent_table_id, parent_table))) {
              LOG_WARN("fail to get table schema", KR(ret), K_(tenant_id), K(parent_table_id));
            } else if (OB_ISNULL(parent_table)) {
              ret = OB_TABLE_NOT_EXIST;
              LOG_WARN("parent table is not exist", KR(ret), K(foreign_key_arg));
            }
          }
        }

        // 2. fill parent columns
        if (OB_FAIL(ret)) {
        } else if (foreign_key_arg.is_parent_table_mock_) {
          if (OB_FAIL(get_mock_fk_parent_table_info_(foreign_key_arg, foreign_key_info, new_mock_fk_parent_table))) {
            LOG_WARN("fail to get mock fk parent table info", KR(ret), K(foreign_key_arg));
          }
        } else {
          if (false == parent_table->is_tmp_table()
              && 0 != parent_table->get_session_id()
              && OB_INVALID_ID != arg_.schema_.get_session_id()) {
            ret = OB_TABLE_NOT_EXIST;
            LOG_USER_ERROR(OB_TABLE_NOT_EXIST, to_cstring(parent_database_name), to_cstring(parent_table_name));
          } else if (!arg_.is_inner_ && parent_table->is_in_recyclebin()) {
            ret = OB_ERR_OPERATION_ON_RECYCLE_OBJECT;
            LOG_WARN("parent table is in recyclebin", KR(ret), K(foreign_key_arg));
          }
          for (int64_t j = 0; OB_SUCC(ret) && j < foreign_key_arg.parent_columns_.count(); j++) {
            const ObString &column_name = foreign_key_arg.parent_columns_.at(j);
            const ObColumnSchemaV2 *column_schema = parent_table ->get_column_schema(column_name);
            if (OB_ISNULL(column_schema)) {
              ret = OB_ERR_COLUMN_NOT_FOUND;
              LOG_WARN("parent column is not exist", KR(ret), K(column_name));
            } else if (OB_FAIL(foreign_key_info.parent_column_ids_.push_back(column_schema->get_column_id()))) {
              LOG_WARN("failed to push parent column id", KR(ret), K(column_name));
            }
          } // end for
        }

        // 3. fill child columns
        if (OB_SUCC(ret)) {
          foreign_key_info.child_table_id_ = data_table.get_table_id();
          foreign_key_info.parent_table_id_ = foreign_key_arg.is_parent_table_mock_ ?
                                              new_mock_fk_parent_table->get_mock_fk_parent_table_id() :
                                              parent_table->get_table_id();
          for (int64_t j = 0; OB_SUCC(ret) && j < foreign_key_arg.child_columns_.count(); j++) {
            const ObString &column_name = foreign_key_arg.child_columns_.at(j);
            const ObColumnSchemaV2 *column_schema = data_table.get_column_schema(column_name);
            if (OB_ISNULL(column_schema)) {
              ret = OB_ERR_COLUMN_NOT_FOUND;
              LOG_WARN("child column is not exist", KR(ret), K(column_name));
            } else if (OB_FAIL(foreign_key_info.child_column_ids_.push_back(column_schema->get_column_id()))) {
              LOG_WARN("failed to push child column id", KR(ret), K(column_name));
            }
          } // end for
        }

        // 4. get reference option and foreign key name.
        if (OB_SUCC(ret)) {
          foreign_key_info.update_action_ = foreign_key_arg.update_action_;
          foreign_key_info.delete_action_ = foreign_key_arg.delete_action_;
          foreign_key_info.foreign_key_name_ = foreign_key_arg.foreign_key_name_;
          foreign_key_info.enable_flag_ = foreign_key_arg.enable_flag_;
          foreign_key_info.validate_flag_ = foreign_key_arg.validate_flag_;
          foreign_key_info.rely_flag_ = foreign_key_arg.rely_flag_;
          foreign_key_info.is_parent_table_mock_ = foreign_key_arg.is_parent_table_mock_;
          foreign_key_info.name_generated_type_  = foreign_key_arg.name_generated_type_;
        }

        // 5. add foreign key info
        if (OB_SUCC(ret)) {
          uint64_t object_id = OB_INVALID_ID;
          if (OB_FAIL(id_generator.next(object_id))) {
            LOG_WARN("fail to get next object_id", KR(ret));
          } else if (FALSE_IT(foreign_key_info.foreign_key_id_ = object_id)) {
          } else if (OB_FAIL(data_table.add_foreign_key_info(foreign_key_info))) {
            LOG_WARN("fail to add foreign key info", KR(ret), K(foreign_key_info));
          } else if (foreign_key_arg.is_parent_table_mock_) {
            if (OB_FAIL(new_mock_fk_parent_table->add_foreign_key_info(foreign_key_info))) {
              LOG_WARN("fail to add foreign key info", KR(ret), K(foreign_key_info));
            }
          } else {
            // this logic is duplicated because of add_foreign_key() will also update data table's schema_version.
            //if (parent_table->get_table_id() != data_table.get_table_id()) {
            //  // no need to update sync_versin_for_cascade_table while the refrence table is itself
            //  if (OB_FAIL(data_table.add_depend_table_id(parent_table->get_table_id()))) {
            //    LOG_WARN("failed to add depend table id", KR(ret), K(foreign_key_info));
            //  }
            //}
          }
        }
      }
    } // end for

    FOREACH_X(iter, new_mock_fk_parent_table_map_, OB_SUCC(ret)) {
      ObMockFKParentTableSchema *&new_mock_fk_parent_table = iter->second;
      if (OB_ISNULL(new_mock_fk_parent_table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("mock fk parent table is null", KR(ret));
      } else if (OB_FAIL(new_mock_fk_parent_tables_.push_back(new_mock_fk_parent_table))) {
        LOG_WARN("fail to push back mock fk parent table", KR(ret), KPC(new_mock_fk_parent_table));
      }
    } // end foreach

    // check & replace existed mock fk parent table with data table
    if (OB_SUCC(ret)) {
      ObMockFKParentTableSchema *new_mock_fk_parent_table = NULL;
      if (OB_FAIL(try_replace_mock_fk_parent_table_(new_mock_fk_parent_table))) {
        LOG_WARN("replace mock fk parent table failed", KR(ret));
      } else if (OB_NOT_NULL(new_mock_fk_parent_table)
                 && OB_FAIL(new_mock_fk_parent_tables_.push_back(new_mock_fk_parent_table))) {
        LOG_WARN("fail to push back mock fk parent table", KR(ret), K(new_mock_fk_parent_table));
      }
    }
  }
  return ret;
}

int ObCreateTableHelper::get_mock_fk_parent_table_info_(
    const obrpc::ObCreateForeignKeyArg &foreign_key_arg,
    ObForeignKeyInfo &foreign_key_info,
    ObMockFKParentTableSchema *&new_mock_fk_parent_table_schema)
{
  int ret = OB_SUCCESS;
  new_mock_fk_parent_table_schema = NULL;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(!foreign_key_arg.is_parent_table_mock_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("foreign_key_arg shoud be parent_table_mock", KR(ret), K(foreign_key_arg));
  } else {
    const ObMockFKParentTableSchema *ori_mock_fk_parent_table_schema = NULL;
    if (OB_INVALID_ID != foreign_key_arg.parent_table_id_) {
      // mock fk parent table already exist
      if (OB_FAIL(latest_schema_guard_.get_mock_fk_parent_table_schema(
          foreign_key_arg.parent_table_id_, ori_mock_fk_parent_table_schema))) {
        LOG_WARN("fail to get mock fk parent table", KR(ret), K_(tenant_id), K(foreign_key_arg));
      } else if (OB_ISNULL(ori_mock_fk_parent_table_schema)) {
        ret = OB_ERR_PARALLEL_DDL_CONFLICT;
        LOG_WARN("mock fk parent table may be dropped, ddl need retry", KR(ret), K_(tenant_id), K(foreign_key_arg));
      }
    }

    if (OB_SUCC(ret)) {
      MockFKParentTableNameWrapper name_wrapper(foreign_key_arg.parent_database_,
                                                foreign_key_arg.parent_table_);
      int hash_ret = new_mock_fk_parent_table_map_.get_refactored(name_wrapper, new_mock_fk_parent_table_schema);
      if (OB_SUCCESS == hash_ret) {
        // skip
      } else if (OB_HASH_NOT_EXIST != hash_ret) {
        ret = hash_ret;
        LOG_WARN("fail to get new mock fk parent table from map", KR(ret), K_(tenant_id), K(name_wrapper));
      } else {
        // 1. try init new mock fk parent table schema
        if (OB_FAIL(ObSchemaUtils::alloc_schema(allocator_, new_mock_fk_parent_table_schema))) {
          LOG_WARN("fail to new ObMockFKParentTableSchema", KR(ret), K_(tenant_id));
        } else if (OB_INVALID_ID != foreign_key_arg.parent_table_id_) {
          // mock fk parent table already exist
          if (OB_ISNULL(ori_mock_fk_parent_table_schema)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("ori mock fk parent table not exist", KR(ret), K(foreign_key_arg.parent_table_id_));
          } else if (OB_FAIL(new_mock_fk_parent_table_schema->assign(*ori_mock_fk_parent_table_schema))) {
            LOG_WARN("fail to assign mock fk parent table schema", KR(ret), KPC(ori_mock_fk_parent_table_schema));
          } else {
            (void) new_mock_fk_parent_table_schema->reset_column_array();
            (void) new_mock_fk_parent_table_schema->set_operation_type(
                   ObMockFKParentTableOperationType::MOCK_FK_PARENT_TABLE_OP_UPDATE_SCHEMA_VERSION);
          }
        } else {
          // mock fk parent table not exist
          ObIDGenerator id_generator;
          const uint64_t object_cnt = 1;
          uint64_t object_id = OB_INVALID_ID;
          if (OB_FAIL(gen_object_ids_(object_cnt, id_generator))) {
            LOG_WARN("fail to gen object ids", KR(ret), K_(tenant_id), K(object_cnt));
          } else if (OB_FAIL(id_generator.next(object_id))) {
            LOG_WARN("fail to get next object_id", KR(ret));
          } else if (OB_FAIL(new_mock_fk_parent_table_schema->set_mock_fk_parent_table_name(foreign_key_arg.parent_table_))) {
            LOG_WARN("fail to set mock fk parent table name", KR(ret), K(foreign_key_arg));
          } else {
            (void) new_mock_fk_parent_table_schema->set_tenant_id(tenant_id_);
            (void) new_mock_fk_parent_table_schema->set_database_id(foreign_key_arg.parent_database_id_);
            (void) new_mock_fk_parent_table_schema->set_mock_fk_parent_table_id(object_id);
            (void) new_mock_fk_parent_table_schema->set_operation_type(
                   ObMockFKParentTableOperationType::MOCK_FK_PARENT_TABLE_OP_CREATE_TABLE_BY_ADD_FK_IN_CHILD_TBALE);
          }
        }

        if (FAILEDx(new_mock_fk_parent_table_map_.set_refactored(name_wrapper, new_mock_fk_parent_table_schema))) {
          LOG_WARN("fail to set mock fk parent table to map", KR(ret), KPC(new_mock_fk_parent_table_schema));
        }
      }
    }

    // 2. try add new column to mock fk parent table
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(new_mock_fk_parent_table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mock fk parent table should not be null", KR(ret), K(foreign_key_arg));
    } else {
      bool is_alter_table = ObMockFKParentTableOperationType::MOCK_FK_PARENT_TABLE_OP_CREATE_TABLE_BY_ADD_FK_IN_CHILD_TBALE
                            != new_mock_fk_parent_table_schema->get_operation_type();

      uint64_t max_used_column_id = 0;
      if (new_mock_fk_parent_table_schema->get_column_array().count() > 0) {
        // 1. new mock fk parent table : update schema_version/add column
        // 2. existed mock fk parent table: update schema_version/add column
        max_used_column_id = new_mock_fk_parent_table_schema->get_column_array()
                             .at(new_mock_fk_parent_table_schema->get_column_array().count() - 1).first;
      } else if (!is_alter_table) {
        // 1. new mock fk parent table: create
        max_used_column_id = 0;
      } else {
        // 1. update schema_version of existed mock fk parent table
        // 2. add new column to mock fk parent table at the first time.
        if (OB_ISNULL(ori_mock_fk_parent_table_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("mock fk parent table doesn't exist", KR(ret), K_(tenant_id), K(foreign_key_arg));
        } else if (OB_UNLIKELY(ori_mock_fk_parent_table_schema->get_column_array().count() <= 0)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column cnt unexpected", KR(ret), KPC(ori_mock_fk_parent_table_schema));
        } else {
          max_used_column_id = ori_mock_fk_parent_table_schema->get_column_array()
                               .at(ori_mock_fk_parent_table_schema->get_column_array().count() - 1).first;
        }
      }

      for (int64_t i = 0; OB_SUCC(ret) && i < foreign_key_arg.parent_columns_.count(); ++i) {
        uint64_t column_id = OB_INVALID_ID;
        bool is_column_exist = false;
        const ObString &column_name = foreign_key_arg.parent_columns_.at(i);
        (void) new_mock_fk_parent_table_schema->get_column_id_by_column_name(column_name, column_id, is_column_exist);

        if (!is_column_exist && is_alter_table) {
          if (OB_ISNULL(ori_mock_fk_parent_table_schema)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("mock fk parent table doesn't exist", KR(ret), K_(tenant_id), K(foreign_key_arg));
          } else {
            (void) ori_mock_fk_parent_table_schema->get_column_id_by_column_name(column_name, column_id, is_column_exist);
          }
        }

        if (OB_FAIL(ret)) {
        } else if (!is_column_exist) {
          if (OB_FAIL(new_mock_fk_parent_table_schema->add_column_info_to_column_array(
                      std::make_pair(++max_used_column_id, column_name)))) {
            LOG_WARN("fail to add_column_info_to_column_array for mock_fk_parent_table_schema",
                     KR(ret), K(max_used_column_id), K(column_name));
          } else if (OB_FAIL(foreign_key_info.parent_column_ids_.push_back(max_used_column_id))) {
            LOG_WARN("failed to push parent column id", KR(ret), K(max_used_column_id));
          }
        } else {
          if (OB_FAIL(foreign_key_info.parent_column_ids_.push_back(column_id))) {
            LOG_WARN("failed to push parent column id", KR(ret), K(column_id));
          }
        }
      } // end for
    }

    if (OB_SUCC(ret)
        && ObMockFKParentTableOperationType::MOCK_FK_PARENT_TABLE_OP_UPDATE_SCHEMA_VERSION
           == new_mock_fk_parent_table_schema->get_operation_type()
        && new_mock_fk_parent_table_schema->get_column_array().count() > 0) {
      (void) new_mock_fk_parent_table_schema->set_operation_type(
             ObMockFKParentTableOperationType::MOCK_FK_PARENT_TABLE_OP_ADD_COLUMN);
    }
  }
  return ret;
}

int ObCreateTableHelper::try_replace_mock_fk_parent_table_(
    ObMockFKParentTableSchema *&new_mock_fk_parent_table)
{
  int ret = OB_SUCCESS;
  new_mock_fk_parent_table = NULL;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_INVALID_ID == replace_mock_fk_parent_table_id_) {
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
    if (FAILEDx(latest_schema_guard_.get_mock_fk_parent_table_schema(
        replace_mock_fk_parent_table_id_, mock_fk_parent_table))) {
      LOG_WARN("fail to get mock fk parent table schema",
               KR(ret), K_(tenant_id), K_(replace_mock_fk_parent_table_id));
    } else if (OB_ISNULL(mock_fk_parent_table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mock fk parent table not exist",
               KR(ret), K_(tenant_id), K_(replace_mock_fk_parent_table_id));
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
        new_foreign_key_info.ref_cst_type_ = CONSTRAINT_TYPE_INVALID;
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
          new_foreign_key_info.ref_cst_type_ = CONSTRAINT_TYPE_PRIMARY_KEY;
        } else { // pk is not match, check if uk match
          if (OB_FAIL(ddl_service_->get_uk_cst_id_for_replacing_mock_fk_parent_table(
              index_schemas, new_foreign_key_info))) {
            LOG_WARN("fail to get_uk_cst_id_for_replacing_mock_fk_parent_table", KR(ret));
          } else if (CONSTRAINT_TYPE_INVALID == new_foreign_key_info.ref_cst_type_) {
            ret = OB_ERR_CANNOT_ADD_FOREIGN;
            LOG_WARN("ref_cst_type is invalid", KR(ret), KPC(mock_fk_parent_table));
          }
        }
      }
    }
  }
  return ret;
}

int ObCreateTableHelper::check_fk_columns_type_for_replacing_mock_fk_parent_table_(
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
    if (OB_FAIL(latest_schema_guard_.get_table_schema(fk_info.child_table_id_, child_table_schema))) {
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

int ObCreateTableHelper::generate_sequence_object_()
{
  int ret = OB_SUCCESS;
  bool is_oracle_mode = false;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_tenant_id(tenant_id_, is_oracle_mode))) {
    LOG_WARN("fail to check is oracle mode", KR(ret));
  } else if (!is_oracle_mode) {
    // skip
  } else if (OB_UNLIKELY(new_tables_.count() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected table cnt", KR(ret), K(new_tables_.count()));
  } else if (!(new_tables_.at(0).is_user_table()
               || new_tables_.at(0).is_oracle_tmp_table())) {
    // skip
  } else {
    // 1. lock object name
    // - Sequence object name for table is encoded with table_id which is determinated in generate_schemas_() stage,
    // - so lock_objects_() stage will be delayed. And sequence_name won't be conficted for most cases since it's encoded.
    const obrpc::ObSequenceDDLArg &sequence_ddl_arg = arg_.sequence_ddl_arg_;
    const ObTableSchema &data_table = new_tables_.at(0);
    const uint64_t tenant_id = data_table.get_tenant_id();
    const uint64_t database_id = data_table.get_database_id();
    const uint64_t session_id = arg_.schema_.get_session_id();
    const ObString &database_name = arg_.db_name_;
    ObSequenceSchema *new_sequence = NULL;
    char sequence_name[OB_MAX_SEQUENCE_NAME_LENGTH + 1] = { 0 };
    const bool is_or_replace = false;
    for (ObTableSchema::const_column_iterator iter = data_table.column_begin();
         OB_SUCC(ret) && iter != data_table.column_end(); ++iter) {
      ObColumnSchemaV2 *column_schema = *iter;
      if (OB_ISNULL(column_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column is null", KR(ret));
      } else if (!column_schema->is_identity_column()) {
        continue;
      } else if (OB_FAIL(ObSchemaUtils::alloc_schema(allocator_, new_sequence))) {
        LOG_WARN("fail to alloc schema", KR(ret));
      } else if (OB_FAIL(new_sequence->assign(sequence_ddl_arg.seq_schema_))) {
        LOG_WARN("fail to assign sequence schema", KR(ret), K(sequence_ddl_arg));
      } else {
        ObSequenceOptionBuilder opt_builder;
        int32_t len = snprintf(sequence_name, sizeof(sequence_name),
                               "%s%lu%c%lu", IDENTITY_COLUMN_SEQUENCE_OBJECT_NAME_PREFIX,
                                data_table.get_table_id(), '_', column_schema->get_column_id());
        new_sequence->set_tenant_id(tenant_id);
        new_sequence->set_database_id(database_id);
        if (OB_UNLIKELY(len < 0)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("create sequence name fail", KR(ret), KPC(column_schema));
        } else if (OB_FAIL(new_sequence->set_sequence_name(sequence_name))) {
          LOG_WARN("fail to set sequence name", KR(ret), K(sequence_name));
        } else if (OB_FAIL(opt_builder.build_create_sequence_option(
          sequence_ddl_arg.option_bitset_, new_sequence->get_sequence_option()))) {
          LOG_WARN("fail to build sequence_option", KR(ret), K(sequence_ddl_arg));
        } else if (OB_FAIL(add_lock_object_by_name_(
                   database_name, new_sequence->get_sequence_name(),
                   share::schema::SEQUENCE_SCHEMA, transaction::tablelock::EXCLUSIVE))) {
          LOG_WARN("fail to lock object by sequence name prefix", KR(ret), K_(tenant_id),
                   K(database_name), KPC(new_sequence));
        } else if (OB_FAIL(latest_schema_guard_.check_oracle_object_exist(
                   new_sequence->get_database_id(), session_id,
                   new_sequence->get_sequence_name(), SEQUENCE_SCHEMA,
                   INVALID_ROUTINE_TYPE, is_or_replace))) {
          LOG_WARN("fail to check oracle object exist", KR(ret), KPC(new_sequence));
        } else if (OB_FAIL(new_sequences_.push_back(new_sequence))) {
          LOG_WARN("fail to push back new sequence ptr", KR(ret));
        }
      }
    } // end for

    if (FAILEDx(lock_existed_objects_by_name_())) {
      LOG_WARN("fail to lock objects by name", KR(ret), K_(tenant_id));
    }

    // 2. generate object ids
    ObIDGenerator id_generator;
    const uint64_t object_cnt = new_sequences_.count();
    if (FAILEDx(gen_object_ids_(object_cnt, id_generator))) {
      LOG_WARN("fail to gen object ids", KR(ret), K_(tenant_id), K(object_cnt));
    }

    // 3. generate schema
    int64_t idx = 0;
    uint64_t sequence_id = OB_INVALID_ID;
    char sequence_string[OB_MAX_SEQUENCE_NAME_LENGTH + 1] = { 0 };
    for (ObTableSchema::const_column_iterator iter = data_table.column_begin();
         OB_SUCC(ret) && iter != data_table.column_end(); ++iter) {
      ObColumnSchemaV2 *column_schema = *iter;
      if (OB_ISNULL(column_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column is null", KR(ret));
      } else if (!column_schema->is_identity_column()) {
        continue;
      } else if (OB_UNLIKELY(idx >= new_sequences_.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid idx", KR(ret), K(idx), K(new_sequences_.count()));
      } else if (OB_ISNULL(new_sequence = new_sequences_.at(idx++))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sequence is null", KR(ret));
      } else if (OB_FAIL(id_generator.next(sequence_id))) {
        LOG_WARN("fail to get next object_id", KR(ret));
      } else {
        // 3.1. generate sequence id
        new_sequence->set_sequence_id(sequence_id);
        // 3.2. modify column schema
        column_schema->set_sequence_id(sequence_id);
        int32_t len = snprintf(sequence_string, sizeof(sequence_string), "%lu", sequence_id);
        if (OB_UNLIKELY(len < 0)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("create sequence string fail", KR(ret), K(sequence_id));
        } else {
          ObObjParam cur_default_value;  // for desc table
          ObObjParam orig_default_value; // for store pure_sequence_id
          cur_default_value.set_varchar("SEQUENCE.NEXTVAL");
          cur_default_value.set_collation_type(ObCharset::get_system_collation());
          cur_default_value.set_collation_level(CS_LEVEL_IMPLICIT);
          cur_default_value.set_param_meta();
          orig_default_value.set_varchar(sequence_string);
          orig_default_value.set_collation_type(ObCharset::get_system_collation());
          orig_default_value.set_collation_level(CS_LEVEL_IMPLICIT);
          orig_default_value.set_param_meta();
          if (OB_FAIL(column_schema->set_cur_default_value(cur_default_value))) {
            LOG_WARN("set current default value fail", KR(ret));
          } else if (OB_FAIL(column_schema->set_orig_default_value(orig_default_value))) {
            LOG_WARN("set origin default value fail", KR(ret), K(column_schema));
          }
        }
      }
    } // end for
  }
  return ret;
}

// create audit schema for table/sequence
int ObCreateTableHelper::generate_audit_schema_()
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
    if (OB_FAIL(latest_schema_guard_.get_default_audit_schemas(audits))) {
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

int ObCreateTableHelper::calc_schema_version_cnt_()
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
    if (OB_SUCC(ret) && new_tables_.count() > 1) {
      schema_version_cnt_ += (new_tables_.count() - 1);
      // update data table schema version
      schema_version_cnt_++;
    }

    // 4. foreign key (without mock fk parent table)
    if (OB_SUCC(ret)) {
    // this logic is duplicated because of add_foreign_key() will also update data table's schema_version.
    //schema_version_cnt_ += data_table.get_depend_table_ids();
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
    }

    // 6. for 1503 boundary ddl operation
    schema_version_cnt_++;
  }
  return ret;
}

int ObCreateTableHelper::create_schemas_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(create_sequence_())) {
    LOG_WARN("fail to create sequences", KR(ret));
  } else if (OB_FAIL(create_tables_())) {
    LOG_WARN("fail to create tables", KR(ret));
  } else if (OB_FAIL(create_audits_())) {
    LOG_WARN("fail to create tables", KR(ret));
  } else if (OB_FAIL(deal_with_mock_fk_parent_tables_())) {
    LOG_WARN("fail to deal with mock fk parent tables", KR(ret));
  }
  RS_TRACE(create_schemas);
  return ret;
}

int ObCreateTableHelper::create_sequence_()
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
                 *new_sequence, &trans_, ddl_stmt_str, old_sequence_id))) {
        LOG_WARN("fail to create sequence", KR(ret), KPC(new_sequence));
      }
    } // end for
  }
  return ret;
}

int ObCreateTableHelper::create_audits_()
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
                 trans_,
                 public_sql_string))) {
        LOG_WARN("fail to add audit", KR(ret), K_(tenant_id), K(new_audit));
      }
    } // end for
  }
  return ret;
}

int ObCreateTableHelper::create_tables_()
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
      const ObString *ddl_stmt_str = (0 == i) ? &arg_.ddl_stmt_str_ : NULL;
      const bool need_sync_schema_version = (new_tables_.count() - 1 == i);
      if (OB_FAIL(schema_service_->gen_new_schema_version(tenant_id_, new_schema_version))) {
          LOG_WARN("fail to gen new schema_version", KR(ret), K_(tenant_id));
      } else if (FALSE_IT(new_table.set_schema_version(new_schema_version))) {
      } else if (OB_FAIL(schema_service_impl->get_table_sql_service().create_table(
                 new_table,
                 trans_,
                 ddl_stmt_str,
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

int ObCreateTableHelper::deal_with_mock_fk_parent_tables_()
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
          if (OB_UNLIKELY(OB_INVALID_ID == replace_mock_fk_parent_table_id_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid replace mock fk parent table id", KR(ret), K_(replace_mock_fk_parent_table_id));
          } else if (OB_FAIL(latest_schema_guard_.get_mock_fk_parent_table_schema(
            replace_mock_fk_parent_table_id_, ori_mock_fk_parent_table))) {
            LOG_WARN("fail to get mock fk parent table schema",
                     KR(ret), K_(tenant_id), K_(replace_mock_fk_parent_table_id));
          } else if (OB_ISNULL(ori_mock_fk_parent_table)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("mock fk parent table not exist, unexpected",
                     KR(ret), K_(tenant_id), K_(replace_mock_fk_parent_table_id));
          } else {
            // 3.1. drop mock fk parent table.
            // 3.2. update foreign keys from mock fk parent table.
            if (OB_FAIL(schema_service_impl->get_table_sql_service().replace_mock_fk_parent_table(
                        &trans_, *new_mock_fk_parent_table, ori_mock_fk_parent_table))) {
              LOG_WARN("fail to replace mock fk parent table", KR(ret), KPC(new_mock_fk_parent_table));
            }

            // 3.3. update child tables' schema version.
            for (int64_t j = 0; OB_SUCC(ret) && j < new_mock_fk_parent_table->get_foreign_key_infos().count(); j++) {
              const ObForeignKeyInfo &foreign_key = new_mock_fk_parent_table->get_foreign_key_infos().at(j);
              const uint64_t child_table_id = foreign_key.child_table_id_;
              const ObTableSchema *child_table = NULL;
              if (OB_FAIL(latest_schema_guard_.get_table_schema(child_table_id, child_table))) {
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

int ObCreateTableHelper::create_tablets_()
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
  } else if (OB_FAIL(ObMajorFreezeHelper::get_frozen_scn(tenant_id_, frozen_scn))) {
    LOG_WARN("failed to get frozen status for create tablet", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", KR(ret), K_(tenant_id));
  } else if (OB_UNLIKELY(new_tables_.count() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected table cnt", KR(ret), K(new_tables_.count()));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id_, tenant_data_version))) {
    LOG_WARN("get min data version failed", K(ret), K_(tenant_id));
  } else {
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
    int64_t last_schema_version = OB_INVALID_VERSION;
    auto *tsi_generator = GET_TSI(TSISchemaVersionGenerator);
    if (OB_FAIL(table_creator.init(true/*need_tablet_cnt_check*/))) {
      LOG_WARN("fail to init table creator", KR(ret));
    } else if (OB_FAIL(new_table_tablet_allocator.init())) {
      LOG_WARN("fail to init new table tablet allocator", KR(ret));
    } else if (OB_ISNULL(tsi_generator)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tsi schema version generator is null", KR(ret));
    } else if (OB_FAIL(tsi_generator->get_current_version(last_schema_version))) {
      LOG_WARN("fail to get end version", KR(ret), K_(tenant_id), K_(arg));
    } else if (OB_UNLIKELY(last_schema_version <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("last schema version is invalid", KR(ret), K_(tenant_id), K(last_schema_version));
    } else {
      ObArray<const ObTableSchema*> schemas;
      common::ObArray<share::ObLSID> ls_id_array;
      ObArray<bool> need_create_empty_majors;
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
          if (OB_FAIL(new_table_tablet_allocator.prepare(trans_, new_table))) {
            LOG_WARN("fail to prepare ls for global index", KR(ret), K(new_table));
          } else if (OB_FAIL(new_table_tablet_allocator.get_ls_id_array(ls_id_array))) {
            LOG_WARN("fail to get ls id array", KR(ret));
          } else if (OB_FAIL(table_creator.add_create_tablets_of_table_arg(
                     new_table, ls_id_array, tenant_data_version, true/*need create major sstable*/))) {
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
        const ObTableSchema &data_table = new_tables_.at(0);
        if (OB_FAIL(new_table_tablet_allocator.prepare(trans_, data_table))) {
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

int ObCreateTableHelper::add_index_name_to_cache_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else {
    ObIndexNameChecker &checker = ddl_service_->get_index_name_checker();
    for (int64_t i = 0; OB_SUCC(ret) && i < new_tables_.count(); i++) {
      const ObTableSchema &table = new_tables_.at(i);
      if (table.is_index_table()) {
        has_index_ = true;
        if (OB_FAIL(checker.add_index_name(table))) {
          LOG_WARN("fail to add index name", KR(ret), K(table));
        }
      }
    } // end for
  }
  return ret;
}
