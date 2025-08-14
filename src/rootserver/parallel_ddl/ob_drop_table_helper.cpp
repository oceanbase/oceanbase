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
#include "rootserver/parallel_ddl/ob_drop_table_helper.h"

#include "rootserver/ob_snapshot_info_manager.h"
#include "rootserver/ob_tablet_drop.h"
#include "share/external_table/ob_external_table_file_mgr.h"
#include "share/ob_autoincrement_service.h"
#include "share/ob_rpc_struct.h"
#include "share/schema/ob_table_sql_service.h"
#include "share/schema/ob_schema_service_sql_impl.h"
#include "storage/tablelock/ob_lock_inner_connection_util.h"

using namespace oceanbase::rootserver;
using namespace oceanbase::obrpc;
using namespace oceanbase::share::schema;
using namespace oceanbase::transaction::tablelock;
using namespace oceanbase::common;

ObDropTableHelper::ObDropTableHelper(
    share::schema::ObMultiVersionSchemaService *schema_service,
    const uint64_t tenant_id, const obrpc::ObDropTableArg &arg,
    obrpc::ObDropTableRes &res)
    : ObDDLHelper(schema_service, tenant_id, "[parallel drop table]"),
      arg_(arg),
      res_(res),
      table_items_(arg.tables_),
      existing_table_items_(),
      database_ids_(),
      table_schemas_(),
      drop_table_ids_(),
      mock_fk_parent_table_schemas_(),
      dep_objs_(),
      ddl_stmt_str_(),
      err_table_list_(),
      tablet_autoinc_cleaner_(tenant_id) {}

ObDropTableHelper::~ObDropTableHelper() {}

int ObDropTableHelper::init_()
{
  int ret = OB_SUCCESS;
  ObTableType table_type = arg_.table_type_;
  if (!ObSchemaUtils::is_support_parallel_drop(table_type)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupport table type for parallel drop table", KR(ret), K(table_type));
    LOG_USER_WARN(OB_NOT_SUPPORTED, "For this table type, parallel drop table");
  } else if (OB_UNLIKELY(arg_.tables_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table list of drop table arg is empty", KR(ret), K_(arg));
  }
  return ret;
}

int ObDropTableHelper::lock_objects_()
{
  int ret = OB_SUCCESS;
  LOG_INFO("start to lock objects", KR(ret));
  DEBUG_SYNC(BEFORE_PARALLEL_DDL_LOCK);
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(lock_databases_by_name_())) {
    LOG_WARN("fail to lock databases by name", KR(ret));
  } else if (OB_FAIL(lock_tables_by_name_())) {
    LOG_WARN("fail to lock tables by name", KR(ret));
  } else if (OB_FAIL(lock_databases_by_id_())) {
    LOG_WARN("fail to lock databases by id", KR(ret));
  } else if (OB_FAIL(prefetch_table_schemas_())) {
    LOG_WARN("fail to prefetch table schemas", KR(ret));
  } else if (OB_FAIL(lock_objects_by_id_())) {
    LOG_WARN("fail to lock objects by id", KR(ret));
  } else if (OB_FAIL(lock_tables_())) {
    LOG_WARN("fail to lock tables", KR(ret));
  } else if (OB_FAIL(check_legitimacy_())) {
    LOG_WARN("fail to check legitimacy", KR(ret));
  }

  RS_TRACE(lock_objects);
  DEBUG_SYNC(AFTER_PARALLEL_DDL_LOCK);
  return ret;
}

int ObDropTableHelper::lock_tables_()
{
  int ret = OB_SUCCESS;
  observer::ObInnerSQLConnection *conn = NULL;
  const int64_t timeout = 0;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_ISNULL(conn = dynamic_cast<observer::ObInnerSQLConnection *>(trans_.get_connection()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("conn is NULL", KR(ret));
  } else {
    ObArray<uint64_t> sorted_table_ids;
    for (int64_t i = 0; OB_SUCC(ret) && i < table_schemas_.count(); i++) {
      const ObTableSchema *table_schema = table_schemas_.at(i);
      if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table schema is null", KR(ret));
      } else if (!table_schema->has_tablet()) {
        // skip
      } else if (OB_FAIL(sorted_table_ids.push_back(table_schema->get_table_id()))) {
        LOG_WARN("fail to push back table id", KR(ret), K(table_schema->get_table_id()));
      }
    }

    // sort by table id, avoid dead lock
    lib::ob_sort(sorted_table_ids.begin(), sorted_table_ids.end());
    for (int64_t i = 0; OB_SUCC(ret) && i < sorted_table_ids.count(); i++) {
      const uint64_t table_id = sorted_table_ids.at(i);
      LOG_INFO("lock table", KR(ret), K(table_id), K_(tenant_id));
      if (OB_FAIL(ObInnerConnectionLockUtil::lock_table(tenant_id_,
                                                        table_id,
                                                        EXCLUSIVE,
                                                        timeout,
                                                        conn))) {
        LOG_WARN("lock dest table failed", KR(ret), K_(tenant_id), K(table_id));
      }
    }
  }
  return ret;
}

int ObDropTableHelper::check_legitimacy_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < table_schemas_.count(); i++) {
      const ObTableSchema *table_schema = table_schemas_.at(i);
      bool has_conflict_ddl = false;
      if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table schema is null", KR(ret));
      } else if (!ObSchemaUtils::is_support_parallel_drop(table_schema->get_table_type())) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("unsupport table type for parallel drop table", KR(ret), K(table_schema->get_table_type()));
        LOG_USER_WARN(OB_NOT_SUPPORTED, "For this table type, parallel drop table");
      } else if (!arg_.force_drop_ && table_schema->is_in_recyclebin()) {
        ret = OB_ERR_OPERATION_ON_RECYCLE_OBJECT;
        LOG_WARN("can not drop table in recyclebin, use purge instead", KR(ret), K(table_schema));
      } else if (!table_schema->check_can_do_ddl()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("offline ddl is being executed, other ddl operations are not allowed", KR(ret), KPC(table_schema));
        LOG_USER_WARN(OB_NOT_SUPPORTED, "Offline ddl is being executed, other ddl operations");
      } else if (arg_.table_type_ == USER_TABLE && OB_FAIL(ObDDLTaskRecordOperator::check_has_conflict_ddl(
                 sql_proxy_,
                 arg_.tenant_id_,
                 table_schema->get_table_id(),
                 arg_.task_id_,
                 ObDDLType::DDL_DROP_TABLE,
                 has_conflict_ddl))) {
        LOG_WARN("failed to check ddl conflict", KR(ret));
      } else if (has_conflict_ddl) {
        ret = OB_EAGAIN;
        LOG_WARN("failed to drop table that has conflict ddl", KR(ret), K(table_schema->get_table_id()));
      } else if (table_schema->has_mlog_table()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("drop table with materialized view log is not supported", KR(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "drop table with materialized view log is");
      } else if (table_schema->table_referenced_by_fast_lsm_mv()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("drop table required by materialized view is not supported", KR(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "drop table required by materialized view is");
      }
    }
  }
  return ret;
}

int ObDropTableHelper::calc_schema_version_cnt_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(table_schemas_.count() != mock_fk_parent_table_schemas_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schemas count and mock fk parent table schemas count mismatch", KR(ret), K(table_schemas_.count()), K(mock_fk_parent_table_schemas_.count()));
  } else {
    schema_version_cnt_ = 0;

    for (int64_t i = 0; OB_SUCC(ret) && i < table_schemas_.count(); i++) {
      const ObTableSchema *table_schema = table_schemas_.at(i);
      if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table schema is null", KR(ret));
      } else {
        bool to_recyclebin = is_to_recyclebin_(*table_schema);
        // index, aux vp, lob meta, lob piece
        ObArray<const ObTableSchema *> aux_table_schemas;
        if (OB_FAIL(collect_aux_table_schemas_(*table_schema, aux_table_schemas))) {
          LOG_WARN("fail to collect aux table schemas", KR(ret));
        }
        for (int64_t j = 0; OB_SUCC(ret) && j < aux_table_schemas.count(); j++) {
          const ObTableSchema *aux_table_schema = aux_table_schemas.at(j);
          if (OB_ISNULL(aux_table_schema)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("aux table schema is null", KR(ret));
          } else if (to_recyclebin && aux_table_schema->is_in_recyclebin()) {
            LOG_INFO("aux table is already in recyclebin");
          } else if (OB_FAIL(calc_schema_version_cnt_for_table_(*aux_table_schema, to_recyclebin))) {
            LOG_WARN("calc schema version cnt drop table failed", KR(ret));
          }
        }

        // trigger
        const ObIArray<uint64_t> &trigger_ids = table_schema->get_trigger_list();
        // trigger schema
        schema_version_cnt_ += trigger_ids.count();
        if (!to_recyclebin) {
          // update data table schema version
          schema_version_cnt_ += trigger_ids.count();
        }

        // data table
        if (FAILEDx(calc_schema_version_cnt_for_table_(*table_schema, to_recyclebin))) {
          LOG_WARN("calc schema version cnt drop table failed", KR(ret));
        }

        // mock fk parent tables
        for (int64_t j = 0; OB_SUCC(ret) && j < mock_fk_parent_table_schemas_.at(i).count(); j++) {
          const ObMockFKParentTableSchema &mock_fk_parent_table_schema = mock_fk_parent_table_schemas_.at(i).at(j);
          ObMockFKParentTableOperationType operation_type = mock_fk_parent_table_schema.get_operation_type();
          if (MOCK_FK_PARENT_TABLE_OP_CREATE_TABLE_BY_DROP_PARENT_TABLE == operation_type) {
            // create mock fk parent table:
            // - dropped table is parent table
            // --------------------------------------------------------------
            // create table t1 (c1 int primary key, c2 int);
            // create table t2 (c1 int primary key, c2 int, constraint fk1 foreign key (c2) references t1(c1));
            // drop table t1;
            // --------------------------------------------------------------

            // new mock fk parent table schema
            schema_version_cnt_++;
            // sync version for cascade table
            schema_version_cnt_ += mock_fk_parent_table_schema.get_foreign_key_infos().count();
          } else if (MOCK_FK_PARENT_TABLE_OP_DROP_TABLE == operation_type) {
            // drop mock table: dropped table is child table, and all columns of its mock parent table are unferenced, so drop entire mock parent table
            // --------------------------------------------------------------
            // create table t1 (c1 int primary key, c2 int, constraint fk1 foreign key (c2) references t2(c1));
            // drop table t1;
            // --------------------------------------------------------------

            // new mock fk parent table schema
            schema_version_cnt_++;
          } else if (MOCK_FK_PARENT_TABLE_OP_DROP_COLUMN == operation_type || MOCK_FK_PARENT_TABLE_OP_UPDATE_SCHEMA_VERSION == operation_type) {
            // alter mock fk parent table:
            // - dropped table is child table, and some columns of its mock parent table are unferenced, so drop these columns
            // --------------------------------------------------------------
            // create table t1 (c1 int primary key, c2 int, constraint fk1 foreign key (c2) references t3(c1));
            // create table t2 (c1 int primary key, c2 int, constraint fk2 foreign key (c2) references t3(c2));
            // drop table t1;
            // --------------------------------------------------------------
            //
            // - dropped table is child table, and all columns of its mock parent table are still referenced, just update mock parent table's schema version
            // --------------------------------------------------------------
            // create table t1 (c1 int primary key, c2 int, constraint fk1 foreign key (c2) references t3(c1));
            // create table t2 (c1 int primary key, c2 int, constraint fk2 foreign key (c2) references t3(c1));
            // drop table t1;
            // --------------------------------------------------------------

            // new mock fk parent table schema
            schema_version_cnt_++;
            // update mock fk parent table schema version
            schema_version_cnt_++;
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("operation type is invalid", KR(ret), K(operation_type));
          }
        }
      }
    }

    // dep objs
    if (FAILEDx(calc_schema_version_cnt_for_dep_objs_())) {
      LOG_WARN("fail to calc schema version cnt for dep objs", KR(ret));
    }

    // 1503 boundary
    schema_version_cnt_++;
  }

  LOG_INFO("finish calc schema version cnt", KR(ret), K_(schema_version_cnt));
  return ret;
}

int ObDropTableHelper::generate_schemas_()
{
  int ret = OB_SUCCESS;
  lib::Worker::CompatMode compat_mode = lib::Worker::CompatMode::INVALID;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(ObCompatModeGetter::get_tenant_mode(tenant_id_, compat_mode))) {
    LOG_WARN("fail to get tenant compat mode", KR(ret), K_(tenant_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < table_schemas_.count(); i++) {
      ObArray<ObMockFKParentTableSchema> mock_fk_parent_table_schemas;

      const ObTableSchema *table_schema = table_schemas_.at(i);
      if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table schema is null", KR(ret));
      } else {
        // 1. gen mock fk parent tables when dropped table has mock parent table
        if (table_schema->get_foreign_key_real_count() > 0) {
          const ObIArray<ObForeignKeyInfo> &fk_infos = table_schema->get_foreign_key_infos();
          ObArray<const ObForeignKeyInfo*> parent_table_mock_fk_infos;
          for (int64_t j = 0; OB_SUCC(ret) && j < fk_infos.count(); j++) {
            const ObForeignKeyInfo &fk_info = fk_infos.at(j);
            if (fk_info.is_parent_table_mock_) {
              if (drop_table_ids_.size() > 1) {
                // TODO: delete this restriction,
                ret = OB_NOT_SUPPORTED;
                LOG_WARN("drop multiple tables with mock fk parent table in one sql is not supported", KR(ret));
              } else if (OB_FAIL(parent_table_mock_fk_infos.push_back(&fk_info))) {
                LOG_WARN("fail to push back to parent_table_mock_fk_infos", KR(ret), K(fk_info));
              }
            }
          }

          if (OB_SUCC(ret) && !parent_table_mock_fk_infos.empty()) {
            if (OB_FAIL(gen_mock_fk_parent_tables_for_drop_fks_(parent_table_mock_fk_infos, mock_fk_parent_table_schemas))) {
              LOG_WARN("fail to gen mock fk parent tables for drop fks", KR(ret));
            }
          }
        }

        // 2. gen mock fk parent table schema when dropped table is parent table
        // if dropped table is parent table, and it is not totally self referenced:
        //   if mysql mode && foreign_key_checks == off, gen mock fk parent table
        //   if oracle mode && cascade_constraints, just delete fk later
        //   otherwise return OB_ERR_TABLE_IS_REFERENCED
        if (OB_SUCC(ret) && table_schema->is_parent_table()) {
          int64_t violated_fk_index = -1;
          const ObIArray<ObForeignKeyInfo> &fk_infos = table_schema->get_foreign_key_infos();
          for (int64_t j = 0; OB_SUCC(ret) && j < fk_infos.count(); j++) {
            const ObForeignKeyInfo &fk_info = fk_infos.at(j);
            if (OB_HASH_EXIST == drop_table_ids_.exist_refactored(fk_info.child_table_id_)) {
              // this child table is dropped in this statement
            } else {
              // there is a child table and it is not dropped in this statement
              violated_fk_index = j;
              ret = OB_ERR_TABLE_IS_REFERENCED;
              LOG_WARN("dropped table is referenced by foreign key", KR(ret));
            }
          }

          // oracle cascade constraints use if_exist_ flag
          bool is_cascade_constraints = arg_.if_exist_;

          if (OB_ERR_TABLE_IS_REFERENCED == ret) {
            if (OB_UNLIKELY(violated_fk_index < 0 || violated_fk_index >= fk_infos.count())) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("idx is invalid", KR(ret), K(violated_fk_index), K(fk_infos.count()));
            } else {
              const ObForeignKeyInfo &violated_fk_info = fk_infos.at(violated_fk_index);
              if (lib::Worker::CompatMode::MYSQL == compat_mode && !arg_.foreign_key_checks_) {
                // gen mock fk parent table, overwrite ret
                if (OB_FAIL(gen_mock_fk_parent_table_for_drop_table_(fk_infos,
                                                                    violated_fk_info,
                                                                    *table_schema,
                                                                    mock_fk_parent_table_schemas))) {
                  LOG_WARN("fail to gen mock fk parent table schema", KR(ret));
                }
              } else if (lib::Worker::CompatMode::ORACLE == compat_mode && is_cascade_constraints) {
                // delete fk later, overwrite ret
                ret = OB_SUCCESS;
              } else {
                // return OB_ERR_TABLE_IS_REFERENCED
                const ObTableSchema *child_table_schema = NULL;
                const uint64_t child_table_id = violated_fk_info.child_table_id_;
                if (OB_FAIL(latest_schema_guard_.get_table_schema(child_table_id, child_table_schema))) {
                  LOG_WARN("fail to get child table schema", KR(ret), K(child_table_id));
                } else if (OB_ISNULL(child_table_schema)) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("child table schema is null", KR(ret));
                } else {
                  ret = OB_ERR_TABLE_IS_REFERENCED;
                  // in oracle mode we do not log user error
                  if (lib::Worker::CompatMode::MYSQL == compat_mode) {
                    LOG_USER_ERROR(OB_ERR_TABLE_IS_REFERENCED,
                                   table_schema->get_table_name_str().length(), table_schema->get_table_name_str().ptr(),
                                   violated_fk_info.foreign_key_name_.length(), violated_fk_info.foreign_key_name_.ptr(),
                                   child_table_schema->get_table_name_str().length(), child_table_schema->get_table_name_str().ptr());
                  }
                }
              }
            }
          }
        }

        if (FAILEDx(mock_fk_parent_table_schemas_.push_back(mock_fk_parent_table_schemas))) {
          LOG_WARN("fail to push back obj", KR(ret), K(mock_fk_parent_table_schemas));
        }
      }
    }
  }
  return ret;
}

int ObDropTableHelper::operate_schemas_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(table_schemas_.count() != existing_table_items_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schemas count and existing table items count mismatch", KR(ret), K(table_schemas_.count()), K(existing_table_items_.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < table_schemas_.count(); i++) {
      const ObTableSchema *table_schema = table_schemas_.at(i);
      if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table schema is null", KR(ret));
      } else if (OB_FAIL(add_table_to_tablet_autoinc_cleaner_(*table_schema))) {
        LOG_WARN("fail to add table to tablet autoinc cleaner", KR(ret), KPC(table_schema));
      } else if (OB_FAIL(construct_drop_table_sql_(*table_schema, existing_table_items_.at(i)))) {
        LOG_WARN("fail to construct drop table sql", KR(ret));
      } else if (!table_schema->is_aux_table()) {
        // drop index, aux vp, lob meta, lob piece
        HEAP_VAR(ObTableSchema, new_aux_table_schema) {
        ObArray<const ObTableSchema *> aux_table_schemas;
        if (OB_FAIL(collect_aux_table_schemas_(*table_schema, aux_table_schemas))) {
          LOG_WARN("fail to collect aux table schemas", KR(ret));
        }
        for (int64_t j = 0; OB_SUCC(ret) && j < aux_table_schemas.count(); j++) {
          const ObTableSchema *aux_table_schema = aux_table_schemas.at(j);
          if (OB_ISNULL(aux_table_schema)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("aux table schema is null", KR(ret));
          } else if (OB_FAIL(new_aux_table_schema.assign(*aux_table_schema))) {
            LOG_WARN("fail to assign table schema", KR(ret));
          } else {
            new_aux_table_schema.set_in_offline_ddl_white_list(table_schema->get_in_offline_ddl_white_list());
            if (is_to_recyclebin_(*table_schema)) {
              if (new_aux_table_schema.is_in_recyclebin()) {
                LOG_INFO("aux table is already in recyclebin");
              } else if (OB_FAIL(drop_table_to_recyclebin_(new_aux_table_schema, NULL/*ddl_stmt_str*/))) {
                LOG_WARN("fail to drop aux table to recyclebin", KR(ret));
              }
            } else if (OB_FAIL(drop_table_(new_aux_table_schema, NULL/*ddl_stmt_str*/))) {
              LOG_WARN("fail to drop aux table", KR(ret));
            }
          }
        } // end for
        } // end HEAP_VAR

        // drop trigger
        if (FAILEDx(drop_triggers_(*table_schema))) {
          LOG_WARN("fail to drop trigger", KR(ret));
        }
      }

      if (FAILEDx(modify_dep_obj_status_(i))) {
        LOG_WARN("fail to modify dep obj status", KR(ret));
      }

      // drop table
      if (OB_SUCC(ret)) {
        ObString ddl_stmt_str = ddl_stmt_str_.string();
        if (is_to_recyclebin_(*table_schema)) {
          if (OB_FAIL(drop_table_to_recyclebin_(*table_schema, &ddl_stmt_str))) {
            LOG_WARN("fail to drop table to recyclebin", KR(ret));
          }
        } else {
          if (OB_FAIL(drop_table_(*table_schema, &ddl_stmt_str))) {
            LOG_WARN("fail to drop table", KR(ret));
          }
        }
      }

      if (FAILEDx(deal_with_mock_fk_parent_tables_(i))) {
        LOG_WARN("fail to deal with mock fk parent tables", KR(ret));
      }
    }
  }

  RS_TRACE(drop_schemas);
  return ret;
}

int ObDropTableHelper::operation_before_commit_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(ddl_service_->get_index_name_checker().reset_cache(tenant_id_))) {
    LOG_ERROR("fail to reset cache", KR(ret), K_(tenant_id));
  }
  return ret;
}

int ObDropTableHelper::clean_on_fail_commit_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(ddl_service_->get_index_name_checker().reset_cache(tenant_id_))) {
    LOG_ERROR("fail to reset cache", KR(ret), K_(tenant_id));
  }
  return ret;
}

int ObDropTableHelper::construct_and_adjust_result_(int &return_ret)
{
  int ret = return_ret;
  ObSchemaVersionGenerator *tsi_generator = GET_TSI(TSISchemaVersionGenerator);
  if (FAILEDx(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_ISNULL(tsi_generator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tsi generator is null", KR(ret));
  } else {
    tsi_generator->get_current_version(res_.schema_version_);
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(tablet_autoinc_cleaner_.commit())) {
      LOG_WARN("fail to commit tablet audoinc cleaner", K(tmp_ret));
    }
  }

  if (OB_ERR_BAD_TABLE == ret) {
    if (arg_.if_exist_) {
      ret = OB_SUCCESS;
      // skip try_check_parallel_ddl_schema_in_sync
      res_.do_nothing_ = true;
    } else {
      LOG_USER_ERROR(OB_ERR_BAD_TABLE, static_cast<int>(err_table_list_.length()) - 1, err_table_list_.ptr());
    }
  }
  return ret;
}

int ObDropTableHelper::lock_databases_by_name_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < table_items_.count(); i++) {
      const ObString &database_name = table_items_.at(i).database_name_;
      if (OB_FAIL(add_lock_object_by_database_name_(database_name, SHARE))) {
        LOG_WARN("fail to lock database by name", KR(ret), K_(tenant_id), K(database_name));
      }
    }
    if (FAILEDx(ObDDLHelper::lock_databases_by_name_())) {
      LOG_WARN("fail to lock databses by name", KR(ret), K_(tenant_id));
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < table_items_.count(); i++) {
      const ObString &database_name = table_items_.at(i).database_name_;
      uint64_t database_id = OB_INVALID_ID;
      if (OB_FAIL(check_database_legitimacy_(database_name, database_id))) {
        LOG_WARN("fail to check database legitimacy", KR(ret), K(database_name));
      }
      if (OB_SUCC(ret) || OB_ERR_BAD_DATABASE == ret) {
        // OB_INVALID_ID == database_id indicates bad database
        if (OB_FAIL(database_ids_.push_back(database_id))) { // overwrite ret
          LOG_WARN("fail to push back database id", KR(ret));
        }
      }
    }
  }

  return ret;
}

int ObDropTableHelper::lock_tables_by_name_()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < table_items_.count(); i++) {
      const ObString &database_name = table_items_.at(i).database_name_;
      const ObString &table_name = table_items_.at(i).table_name_;
      if (OB_FAIL(add_lock_object_by_name_(database_name, table_name, TABLE_SCHEMA, EXCLUSIVE))) {
        LOG_WARN("fail to lock object by table name", KR(ret), K_(tenant_id), K(database_name), K(table_name));
      }
    }

    if (FAILEDx(lock_existed_objects_by_name_())) {
      LOG_WARN("fail to lock objects by name", KR(ret), K_(tenant_id));
    }
  }

  return ret;
}

int ObDropTableHelper::lock_databases_by_id_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < database_ids_.count(); i++) {
      const uint64_t database_id = database_ids_.at(i);
      if (OB_UNLIKELY(OB_INVALID_ID == database_id)) {
        // invalid database_id indicates bad database, which should log table not exist error,
        // so here we ignore it
      } else if (OB_FAIL(add_lock_object_by_id_(database_id, DATABASE_SCHEMA, SHARE))) {
        LOG_WARN("fail to lock database id", KR(ret), K(database_id));
      }
    }

    if (FAILEDx(lock_existed_objects_by_id_())) {
      LOG_WARN("fail to lock objects by id", KR(ret));
    }
  }

  return ret;
}

int ObDropTableHelper::prefetch_table_schemas_()
{
  int ret = OB_SUCCESS;
  const uint64_t session_id = arg_.session_id_;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(table_items_.count() != database_ids_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table items count is not equal to database ids count", KR(ret), K(table_items_.count()), K(database_ids_.count()));
  } else if (OB_FAIL(drop_table_ids_.create(table_items_.count()))) {
    LOG_WARN("fail to create ObHashSet", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < table_items_.count(); i++) {
      const ObString &database_name = table_items_.at(i).database_name_;
      const ObString &table_name = table_items_.at(i).table_name_;
      const uint64_t database_id = database_ids_.at(i);
      uint64_t table_id = OB_INVALID_ID;
      ObTableType table_type = ObTableType::MAX_TABLE_TYPE;
      int64_t schema_version = OB_INVALID_VERSION;
      const ObTableSchema *table_schema = NULL;

      if (OB_UNLIKELY(OB_INVALID_ID == database_id)) {
        // invalid database_id indicates bad database
        LOG_WARN("database id is invalid", KR(ret));
        if (OB_FAIL(log_table_not_exist_msg_(table_items_.at(i)))) {
          LOG_WARN("fail to log table not exsit msg", KR(ret));
        }
      } else if (OB_FAIL(latest_schema_guard_.get_table_id(database_id, session_id, table_name, table_id, table_type, schema_version))) {
        LOG_WARN("fail to get table id", KR(ret), K_(tenant_id), K(database_id), K(session_id), K(table_name));
      } else if (OB_UNLIKELY(OB_INVALID_ID == table_id)) {
        // skip
        LOG_WARN("table does not exist", KR(ret), K(table_name));
        if (OB_FAIL(log_table_not_exist_msg_(table_items_.at(i)))) {
          LOG_WARN("fail to log table not exsit msg", KR(ret));
        }
      } else if (ObTableType::SYSTEM_TABLE == table_type
                 || ObTableType::VIRTUAL_TABLE == table_type) {
        ret = OB_OP_NOT_ALLOW;
        LOG_WARN("this type of table is not allowed for parallel drop table", KR(ret), K(table_type));
        LOG_USER_ERROR(OB_OP_NOT_ALLOW, "parallel drop sys table is");
      } else if (!ObSchemaUtils::is_support_parallel_drop(table_type)) {
        // skip
        LOG_WARN("this type of table should be invisable for drop table", KR(ret), KPC(table_schema));
        if (OB_FAIL(log_table_not_exist_msg_(table_items_.at(i)))) {
          LOG_WARN("fail to log table not exsit msg", KR(ret));
        }
      } else if (OB_FAIL(latest_schema_guard_.get_table_schema(table_id, table_schema))) {
        LOG_WARN("fail to get table schema", KR(ret), K(table_id));
      } else if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table schema is null", KR(ret), K(table_id));
      } else if (table_schema->mv_container_table()) {
        // skip
        LOG_WARN("this type of table should be invisable for drop table", KR(ret), KPC(table_schema));
        if (OB_FAIL(log_table_not_exist_msg_(table_items_.at(i)))) {
          LOG_WARN("fail to log table not exsit msg", KR(ret));
        }
      } else if (OB_FAIL(drop_table_ids_.set_refactored(table_id))) {
        LOG_WARN("fail to set table id", KR(ret), K(table_id));
      } else if (OB_FAIL(table_schemas_.push_back(table_schema))) {
        LOG_WARN("fail to push back table schema", KR(ret));
      } else if (OB_FAIL(existing_table_items_.push_back(table_items_.at(i)))) {
        LOG_WARN("fail to push back existing table items", KR(ret));
      }
    }

    if (OB_SUCC(ret) && (!err_table_list_.empty() || table_schemas_.empty())) {
      // NEITHER of these two scenarios should commit transaction:
      // - !err_table_list_.empty() indicates some tables are non-existing, and without if_exists flag
      // - table_schemas_.empty() indicates all tables are non-existing
      ret = OB_ERR_BAD_TABLE;
      LOG_WARN("table not exist", KR(ret), K_(err_table_list), K_(table_schemas));
    }
  }

  return ret;
}

int ObDropTableHelper::lock_objects_by_id_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else {
    ObArray<ObArray<std::pair<uint64_t, share::schema::ObObjectType>>> dep_objs_before_lock_array;
    for (int64_t i = 0; OB_SUCC(ret) && i < table_schemas_.count(); i++) {
      const ObTableSchema *table_schema = table_schemas_.at(i);
      ObArray<std::pair<uint64_t, share::schema::ObObjectType>> dep_objs_before_lock;
      if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table schema is null", KR(ret));
      } else {
        const uint64_t table_id = table_schema->get_table_id();
        // table
        if (OB_FAIL(add_lock_object_by_id_(table_id, TABLE_SCHEMA, EXCLUSIVE))) {
          LOG_WARN("fail to lock table id", KR(ret), K(table_id));
        }

        // fk parent/child tables, include mock fk parent table
        if (FAILEDx(lock_fk_tables_by_id_(*table_schema))) {
          LOG_WARN("fail to lock fk tables by id", KR(ret), KPC(table_schema));
        }

        // aux tables
        if (FAILEDx(lock_aux_tables_by_id_(*table_schema))) {
          LOG_WARN("fail to lock fk tables by id", KR(ret), KPC(table_schema));
        }

        // triggers
        if (FAILEDx(lock_triggers_by_id_(*table_schema))) {
          LOG_WARN("fail to lock triggers by id", KR(ret), KPC(table_schema));
        }

        // sequences
        if (FAILEDx(lock_sequences_by_id_(*table_schema))) {
          LOG_WARN("fail to lock sequences by id", KR(ret), KPC(table_schema));
        }

        // rls
        if (FAILEDx(lock_rls_by_id_(*table_schema))) {
          LOG_WARN("fail to lock rls by id", KR(ret), KPC(table_schema));
        }

        // audit
        if (FAILEDx(lock_audits_by_id_(*table_schema))) {
          LOG_WARN("fail to lock audits by id", KR(ret), KPC(table_schema));
        }

        // dep views
        if (FAILEDx(ObDependencyInfo::collect_all_dep_objs(tenant_id_, table_id, *sql_proxy_, dep_objs_before_lock))) {
          LOG_WARN("fail to collect all dep objs", KR(ret), K_(tenant_id), K(table_id));
        } else {
          for (int64_t j = 0; OB_SUCC(ret) && j < dep_objs_before_lock.count(); j++) {
            if (ObObjectType::VIEW == dep_objs_before_lock.at(j).second && OB_FAIL(add_lock_object_by_id_(dep_objs_before_lock.at(j).first, VIEW_SCHEMA, EXCLUSIVE))) {
              LOG_WARN("fail to add lock object", KR(ret));
            }
          }
        }

        if (FAILEDx(dep_objs_before_lock_array.push_back(dep_objs_before_lock))) {
          LOG_WARN("fail to push back obj", KR(ret), K(dep_objs_before_lock));
        }
      }
    }

    if (FAILEDx(lock_existed_objects_by_id_())) {
      LOG_WARN("fail to lock existed objects by id", KR(ret));
    } else if (OB_UNLIKELY(table_schemas_.count() != dep_objs_before_lock_array.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table schemas count is not equal to dep objs before lock array count", KR(ret), K(table_schemas_.count()), K(dep_objs_before_lock_array.count()));
    } else {
      // check if table schemas are consistent
      const uint64_t session_id = arg_.session_id_;
      for (int64_t i = 0; OB_SUCC(ret) && i < table_schemas_.count(); i++) {
        const ObTableSchema *table_schema = table_schemas_.at(i);
        uint64_t table_id_after_lock = OB_INVALID_ID;
        ObTableType table_type_after_lock = ObTableType::MAX_TABLE_TYPE;
        int64_t schema_version_after_lock = OB_INVALID_VERSION;
        if (OB_ISNULL(table_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("table schema is null", KR(ret));
        } else if (OB_FAIL(latest_schema_guard_.get_table_id(table_schema->get_database_id(),
                                                             session_id,
                                                             table_schema->get_table_name(),
                                                             table_id_after_lock,
                                                             table_type_after_lock,
                                                             schema_version_after_lock))) {
          LOG_WARN("fail to get table id", KR(ret), K(table_schema->get_database_id()), K(session_id), K(table_schema->get_table_name()));
        } else if (OB_UNLIKELY(table_id_after_lock != table_schema->get_table_id()
                               || table_type_after_lock != table_schema->get_table_type()
                               || schema_version_after_lock != table_schema->get_schema_version())) {
          ret = OB_ERR_PARALLEL_DDL_CONFLICT;
          LOG_WARN("table schema has changed after lock", KR(ret));
        }
      }

      // check if dep objs are consistent
      for (int64_t i = 0; OB_SUCC(ret) && i < table_schemas_.count(); i++) {
        ObArray<std::pair<uint64_t, share::schema::ObObjectType>> dep_objs_after_lock;
        const ObTableSchema *table_schema = table_schemas_.at(i);
        uint64_t table_id = OB_INVALID_ID;
        if (OB_ISNULL(table_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("table schema is null", KR(ret));
        } else if (FALSE_IT(table_id = table_schema->get_table_id())) {
        } else if (OB_FAIL(ObDependencyInfo::collect_all_dep_objs(tenant_id_, table_id, *sql_proxy_, dep_objs_after_lock))) {
          LOG_WARN("fail to collect dep obj", KR(ret), K_(tenant_id), K(table_id));
        } else if (OB_FAIL(check_dep_objs_consistent(dep_objs_before_lock_array.at(i), dep_objs_after_lock))) {
          LOG_WARN("dep objs count not consistent", KR(ret));
        } else if (OB_FAIL(dep_objs_.push_back(dep_objs_after_lock))) {
          LOG_WARN("fail to push back obj", KR(ret));
        }
      } // end for
    }
  }

  return ret;
}

int ObDropTableHelper::gen_mock_fk_parent_tables_for_drop_fks_(
    const ObIArray<const ObForeignKeyInfo*> &fk_infos,
    ObArray<ObMockFKParentTableSchema> &new_mock_fk_parent_table_schemas)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else {
    ObArray<const ObMockFKParentTableSchema *> old_mock_fk_parent_table_schema_ptrs;

    // collect mock fk parent table schemas
    for (int64_t i = 0; OB_SUCC(ret) && i < fk_infos.count(); i++) {
      const ObForeignKeyInfo *fk_info = fk_infos.at(i);
      if (OB_ISNULL(fk_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fk info is null", KR(ret));
      } else if (!fk_info->is_parent_table_mock_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("parent table should be mock", KR(ret), KPC(fk_info));
      } else {
        bool already_exist = false;
        for (int64_t j = 0; OB_SUCC(ret) && !already_exist && j < new_mock_fk_parent_table_schemas.count(); j++) {
          if (new_mock_fk_parent_table_schemas.at(j).get_mock_fk_parent_table_id() == fk_info->parent_table_id_) {
            already_exist = true;
            if (OB_FAIL(new_mock_fk_parent_table_schemas.at(j).add_foreign_key_info(*fk_info))) { // add fk infos to be deleted into new_mock_fk_parent_table_schema
              LOG_WARN("fail to add fk info", KR(ret), KPC(fk_info));
            }
          }
        }
        if (OB_SUCC(ret) && !already_exist) {
          // new_mock_fk_parent_table_schema only contains fk infos to be deleted
          ObMockFKParentTableSchema new_mock_fk_parent_table_schema;
          // old_mock_fk_parent_table_schema_ptr contains all fk infos
          const ObMockFKParentTableSchema *old_mock_fk_parent_table_schema_ptr = NULL;
          if (OB_FAIL(latest_schema_guard_.get_mock_fk_parent_table_schema(fk_info->parent_table_id_, old_mock_fk_parent_table_schema_ptr))) {
            LOG_WARN("fail to get old mock fk parent table schema", KR(ret));
          } else if (OB_ISNULL(old_mock_fk_parent_table_schema_ptr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("old mock fk parent table schema ptr is null", KR(ret));
          } else if (OB_FAIL(new_mock_fk_parent_table_schema.assign(*old_mock_fk_parent_table_schema_ptr))) {
            LOG_WARN("fail to assign mock fk parent table", KR(ret));
          } else if (FALSE_IT(new_mock_fk_parent_table_schema.reset_column_array())) {
          } else if (FALSE_IT(new_mock_fk_parent_table_schema.reset_foreign_key_infos())) {
          } else if (OB_FAIL(new_mock_fk_parent_table_schema.add_foreign_key_info(*fk_info))) { // add fk infos to be deleted into new_mock_fk_parent_table_schema
            LOG_WARN("fail to add foreign key info", KR(ret), KPC(fk_info));
          } else if (OB_FAIL(new_mock_fk_parent_table_schemas.push_back(new_mock_fk_parent_table_schema))) {
            LOG_WARN("fail to push back new mock fk parent table schema", KR(ret), K(new_mock_fk_parent_table_schema));
          } else if (OB_FAIL(old_mock_fk_parent_table_schema_ptrs.push_back(old_mock_fk_parent_table_schema_ptr))) {
            LOG_WARN("fail to push back old mock fk parent table schema ptr", KR(ret), KPC(old_mock_fk_parent_table_schema_ptr));
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(new_mock_fk_parent_table_schemas.count() != old_mock_fk_parent_table_schema_ptrs.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("counts of new mock fk parent table schemas and old mock fk parent table schema ptrs are not equal", KR(ret), K(new_mock_fk_parent_table_schemas.count()), K(old_mock_fk_parent_table_schema_ptrs.count()));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < new_mock_fk_parent_table_schemas.count(); i++) {
          // 1. collect fk infos to be remained

          // now new_mock_fk_parent_table_schema only contains fk infos to be deleted
          ObMockFKParentTableSchema &new_mock_fk_parent_table_schema = new_mock_fk_parent_table_schemas.at(i);
          // old_mock_fk_parent_table_ptr containts all fk infos
          const ObMockFKParentTableSchema *old_mock_fk_parent_table_ptr = old_mock_fk_parent_table_schema_ptrs.at(i);

          ObArray<ObForeignKeyInfo> fk_infos_to_be_deleted;
          if (OB_FAIL(fk_infos_to_be_deleted.assign(new_mock_fk_parent_table_schema.get_foreign_key_infos()))) {
            LOG_WARN("fail to assign fk infos to be delete", KR(ret));
          } else if (OB_ISNULL(old_mock_fk_parent_table_ptr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("old mock fk parent table ptr is null", KR(ret));
          } else {
            // 1.1 clear all fk infos first
            new_mock_fk_parent_table_schema.reset_foreign_key_infos();
          }
          for (int64_t j = 0; OB_SUCC(ret) && j < old_mock_fk_parent_table_ptr->get_foreign_key_infos().count(); j++) {
            // 1.2 pick up fk info which is not in fk_infos_to_be_deleted list
            bool is_to_be_remained = true;
            for (int64_t k = 0; is_to_be_remained && k < fk_infos_to_be_deleted.count(); k++) {
              if (old_mock_fk_parent_table_ptr->get_foreign_key_infos().at(j).foreign_key_id_ == fk_infos_to_be_deleted.at(k).foreign_key_id_) {
                is_to_be_remained = false;
              }
            }
            // 1.3 add fk info back if it is to be remained
            if (is_to_be_remained && OB_FAIL(new_mock_fk_parent_table_schema.add_foreign_key_info(old_mock_fk_parent_table_ptr->get_foreign_key_infos().at(j)))) {
              LOG_WARN("fail to add foreign key info", KR(ret));
            }
          }
          // 1.4 now new_mock_fk_parent_table_schema only contains fk infos to be remained


          // 2. reconstruct columns to be remained, and set operation type by counting remained referenced columns
          if (FAILEDx(new_mock_fk_parent_table_schema.reconstruct_column_array_by_foreign_key_infos(old_mock_fk_parent_table_ptr))) {
            LOG_WARN("fail to reconstruct column array by foreign key infos", KR(ret));
          } else if (new_mock_fk_parent_table_schema.get_column_array().count() == old_mock_fk_parent_table_ptr->get_column_array().count()) {
            // 2.1 all columns are still referenced, just update schema version
            new_mock_fk_parent_table_schema.set_operation_type(MOCK_FK_PARENT_TABLE_OP_UPDATE_SCHEMA_VERSION);
          } else if (0 == new_mock_fk_parent_table_schema.get_column_array().count()) {
            // 2.2 all columns are unreferenced, drop this entire mock table
            new_mock_fk_parent_table_schema.set_operation_type(MOCK_FK_PARENT_TABLE_OP_DROP_TABLE);
            if (OB_FAIL(new_mock_fk_parent_table_schema.set_column_array(old_mock_fk_parent_table_ptr->get_column_array()))) {
              LOG_WARN("fail to set column array", KR(ret), K(new_mock_fk_parent_table_schema), KPC(old_mock_fk_parent_table_ptr));
            }
            // now new_mock_fk_parent_table_schema contains columns to be deleted
          } else {
            // 2.3 some columns are unreferenced, drop these columns
            new_mock_fk_parent_table_schema.set_operation_type(MOCK_FK_PARENT_TABLE_OP_DROP_COLUMN);
            ObMockFKParentTableColumnArray columns_to_be_remained;
            if (OB_FAIL(columns_to_be_remained.assign(new_mock_fk_parent_table_schema.get_column_array()))) {
              LOG_WARN("fail to assign columns to be remained", KR(ret));
            } else {
              new_mock_fk_parent_table_schema.reset_column_array();
            }
            // collect columns to be deleted
            for (int64_t j = 0; OB_SUCC(ret) && j < old_mock_fk_parent_table_ptr->get_column_array().count(); j++) {
              bool is_col_to_be_deleted = true;
              for (int64_t k = 0; is_col_to_be_deleted && k < columns_to_be_remained.count(); k++) {
                if (old_mock_fk_parent_table_ptr->get_column_array().at(j).first == columns_to_be_remained.at(k).first) {
                  is_col_to_be_deleted = false;
                }
              }
              if (is_col_to_be_deleted && OB_FAIL(new_mock_fk_parent_table_schema.add_column_info_to_column_array(old_mock_fk_parent_table_ptr->get_column_array().at(j)))) {
                LOG_WARN("fail to add column info to column array", KR(ret));
              }
            }
            // now new_mock_fk_parent_table_schema contains columns to be deleted
          }
        }
      }
    }
  }
  return ret;
}

int ObDropTableHelper::gen_mock_fk_parent_table_for_drop_table_(
    const ObIArray<ObForeignKeyInfo> &fk_infos,
    const ObForeignKeyInfo &violated_fk_info,
    const ObTableSchema &table_schema,
    ObArray<ObMockFKParentTableSchema> &mock_fk_parent_table_schemas)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *child_table_schema = NULL;
  const uint64_t child_table_id = violated_fk_info.child_table_id_;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(latest_schema_guard_.get_table_schema(child_table_id, child_table_schema))) {
    LOG_WARN("fail to get table schema", KR(ret), K(child_table_id));
  } else if (OB_ISNULL(child_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child table schema is null", KR(ret));
  } else if (table_items_.count() > 1) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "drop fk parent table and more tables in one sql");
    LOG_WARN("drop fk parent table and more tables in one sql", KR(ret), KPC(child_table_schema), K(table_schema));
  } else if (child_table_schema->get_database_id() != table_schema.get_database_id()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "drop fk parent table which has child tables in different database");
    LOG_WARN("drop fk parent table which has child tables in different database not supported", KR(ret), KPC(child_table_schema), K(table_schema));
  } else {
    ObIDGenerator id_generator;
    const uint64_t object_cnt = 1;
    uint64_t object_id = OB_INVALID_ID;
    ObMockFKParentTableSchema mock_fk_parent_table_schema;
    if (OB_FAIL(gen_object_ids_(object_cnt, id_generator))) {
      LOG_WARN("fail to gen object ids", KR(ret), K(object_cnt));
    } else if (OB_FAIL(id_generator.next(object_id))) {
      LOG_WARN("fauk to get next object id", KR(ret));
    } else if (OB_FAIL(mock_fk_parent_table_schema.set_mock_fk_parent_table_name(table_schema.get_table_name_str()))) {
      LOG_WARN("fail to set mock fk parent table name", KR(ret));
    } else {
      mock_fk_parent_table_schema.set_tenant_id(table_schema.get_tenant_id());
      mock_fk_parent_table_schema.set_database_id(table_schema.get_database_id());
      mock_fk_parent_table_schema.set_mock_fk_parent_table_id(object_id);
      mock_fk_parent_table_schema.set_operation_type(ObMockFKParentTableOperationType::MOCK_FK_PARENT_TABLE_OP_CREATE_TABLE_BY_DROP_PARENT_TABLE);
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < fk_infos.count(); i++) {
      ObForeignKeyInfo tmp_fk_info = fk_infos.at(i);
      if (table_schema.get_table_id() == tmp_fk_info.child_table_id_) {
        // for this fk relation, dropped table is the child table, pass
      } else if (tmp_fk_info.parent_table_id_ == tmp_fk_info.child_table_id_) {
        // self referenced fk, pass
      } else {
        // The difference of fk_info between orig_parent_table and mock_fk_parent_table is only parent_table_id.
        // parent_column_ids are all the same.
        const uint64_t invalid_cst_id = 0;
        tmp_fk_info.set_parent_table_id(mock_fk_parent_table_schema.get_mock_fk_parent_table_id());
        tmp_fk_info.set_is_parent_table_mock(true);
        tmp_fk_info.set_fk_ref_type(FK_REF_TYPE_INVALID);
        tmp_fk_info.set_ref_cst_id(invalid_cst_id);
        if (OB_FAIL(mock_fk_parent_table_schema.add_foreign_key_info(tmp_fk_info))) {
          LOG_WARN("fail to add foreign key info for mock fk parent table schema", KR(ret), K(tmp_fk_info));
        } else {
          // add column info of fk_infos to mock_fk_parent_table_schema info
          for (int64_t j = 0; OB_SUCC(ret) && j < fk_infos.at(i).parent_column_ids_.count(); j++) {
            const ObColumnSchemaV2 *parent_column_schema = table_schema.get_column_schema(fk_infos.at(i).parent_column_ids_.at(j));
            if (OB_ISNULL(parent_column_schema)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("parent column schema is null", KR(ret));
            } else {
              bool is_exist = false;
              // check if column info is already in mock_fk_parent_table_schema
              for (int64_t k = 0; !is_exist && k < mock_fk_parent_table_schema.get_column_array().count(); k++) {
                if (parent_column_schema->get_column_id() == mock_fk_parent_table_schema.get_column_array().at(k).first
                    && 0 == parent_column_schema->get_column_name_str().compare(mock_fk_parent_table_schema.get_column_array().at(k).second)) {
                  is_exist = true;
                }
              }
              if (!is_exist && OB_FAIL(mock_fk_parent_table_schema.add_column_info_to_column_array(std::make_pair(parent_column_schema->get_column_id(), parent_column_schema->get_column_name_str())))) {
                LOG_WARN("fail to add column info to column array for mock fk parent table schema", KR(ret), KPC(parent_column_schema));
              }
            }
          }
        }
      }
    }

    if (FAILEDx(mock_fk_parent_table_schemas.push_back(mock_fk_parent_table_schema))) {
      LOG_WARN("fail to push back mock fk parent table schema", KR(ret), K(mock_fk_parent_table_schema));
    }
  }

  return ret;
}

int ObDropTableHelper::collect_aux_table_schemas_(
  const ObTableSchema &table_schema,
  ObIArray<const ObTableSchema *> &aux_table_schemas)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else {
    // index
    ObArray<ObAuxTableMetaInfo> simple_index_infos;
    if (OB_FAIL(table_schema.get_simple_index_infos(simple_index_infos))) {
      LOG_WARN("get simple index infos failed", KR(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); i++) {
      const ObTableSchema *aux_table_schema = NULL;
      const uint64_t table_id = simple_index_infos.at(i).table_id_;
      if (OB_FAIL(latest_schema_guard_.get_table_schema(table_id, aux_table_schema))) {
        LOG_WARN("get table schema failed", KR(ret), K(table_id));
      } else if (OB_ISNULL(aux_table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table schema is null", KR(ret));
      } else if (OB_FAIL(aux_table_schemas.push_back(aux_table_schema))) {
        LOG_WARN("fail to push back aux table schema", KR(ret));
      }
    }

    // aux vp & lob meta & lob piece
    ObArray<uint64_t> aux_table_ids;
    const uint64_t aux_lob_piece_table_id = table_schema.get_aux_lob_piece_tid();
    const uint64_t aux_lob_meta_table_id = table_schema.get_aux_lob_meta_tid();
    if (FAILEDx(table_schema.get_aux_vp_tid_array(aux_table_ids))) {
      LOG_WARN("get_aux_vp_tid_array failed", KR(ret));
    } else if (OB_INVALID_ID != aux_lob_piece_table_id && OB_FAIL(aux_table_ids.push_back(aux_lob_piece_table_id))) {
      LOG_WARN("push back aux_lob_piece_table_id failed", KR(ret));
    } else if (OB_INVALID_ID != aux_lob_meta_table_id && OB_FAIL(aux_table_ids.push_back(aux_lob_meta_table_id))) {
      LOG_WARN("push back aux_lob_meta_table_id failed", KR(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < aux_table_ids.count(); i++) {
        const ObTableSchema *aux_table_schema = NULL;
        const uint64_t table_id = aux_table_ids.at(i);
        if (OB_FAIL(latest_schema_guard_.get_table_schema(table_id, aux_table_schema))) {
          LOG_WARN("get table schema failed", KR(ret), K(table_id));
        } else if (OB_ISNULL(aux_table_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("table schema is null", KR(ret));
        } else if (OB_FAIL(aux_table_schemas.push_back(aux_table_schema))) {
          LOG_WARN("fail to push back aux table schema", KR(ret));
        }
      }
    }
  }

  return ret;
}

int ObDropTableHelper::calc_schema_version_cnt_for_table_(
  const ObTableSchema &table_schema,
  bool to_recyclebin)
{
  int ret = OB_SUCCESS;
  const uint64_t table_id = table_schema.get_table_id();
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else {
    // table
    schema_version_cnt_++;

    // for aux table, promote data table schema version
    if (table_schema.is_aux_table()) {
      schema_version_cnt_++;
    }

    // for fks, promote parent/child table schema version
    const ObIArray<ObForeignKeyInfo> &fk_infos = table_schema.get_foreign_key_infos();
    for (int64_t i = 0; OB_SUCC(ret) && i < fk_infos.count(); i++) {
      const ObForeignKeyInfo &fk_info = fk_infos.at(i);
      const uint64_t update_table_id = table_schema.get_table_id() == fk_info.parent_table_id_
                                      ? fk_info.child_table_id_
                                      : fk_info.parent_table_id_;
      if (!to_recyclebin && OB_HASH_EXIST == drop_table_ids_.exist_refactored(update_table_id)) {
        // if drop to recyclebin, always promote
        // if not drop to recyclebin, and parent/child table is also dropped, skip
      } else {
        // update data table schema version
        schema_version_cnt_++;
        if (fk_info.is_parent_table_mock_) {
          // update mock fk parent table schema version
          schema_version_cnt_++;
        }
      }
    }

    if (!to_recyclebin) {
      // obj priv
      ObArray<ObObjPriv> obj_privs;
      if (OB_FAIL(latest_schema_guard_.get_obj_privs(table_id, ObObjectType::TABLE, obj_privs))) {
        LOG_WARN("fail to get obj privs", KR(ret), K(table_id));
      }
      schema_version_cnt_ += obj_privs.count();

      // sequence
      if (FAILEDx(calc_schema_version_cnt_for_sequence_(table_schema))) {
        LOG_WARN("fail to calc schema version cnt for sequence", KR(ret));
      }

      // rls
      schema_version_cnt_ += table_schema.get_rls_policy_ids().count();
      schema_version_cnt_ += table_schema.get_rls_group_ids().count();
      schema_version_cnt_ += table_schema.get_rls_context_ids().count();

      // sync version for cascade table
      schema_version_cnt_ += table_schema.get_base_table_ids().count();
      schema_version_cnt_ += table_schema.get_depend_table_ids().count();

      // sync version for cascade mock fk parent table
      schema_version_cnt_ += table_schema.get_depend_mock_fk_parent_table_ids().count();

      // audit
      if (OB_SUCC(ret) && (table_schema.is_user_table() || table_schema.is_external_table())) {
        ObArray<ObSAuditSchema> audits;
        if (OB_FAIL(latest_schema_guard_.get_audit_schemas_in_owner(AUDIT_TABLE, table_id, audits))) {
          LOG_WARN("fail to get audit schemas in owner", KR(ret), K(table_id));
        }
        schema_version_cnt_ += audits.count();
      }

      // tablet
      if (table_schema.has_tablet()) {
        schema_version_cnt_++;
      }
    }
  }

  return ret;
}

int ObDropTableHelper::calc_schema_version_cnt_for_dep_objs_() {
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < dep_objs_.count(); i++) {
      for (int64_t j = 0; OB_SUCC(ret) && j < dep_objs_.at(i).count(); j++) {
        if (OB_INVALID_ID == dep_objs_.at(i).at(j).first) {
          // skipped by ddl
          continue;
        }
        if (OB_SUCC(ret) && ObObjectType::VIEW == dep_objs_.at(i).at(j).second) {
          const ObTableSchema *view_schema = NULL;
          const uint64_t view_id = dep_objs_.at(i).at(j).first;
          if (OB_FAIL(latest_schema_guard_.get_table_schema(view_id, view_schema))) {
            LOG_WARN("fail to get view schema", KR(ret), K(view_id));
          } else if (OB_ISNULL(view_schema)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("view schema is null", KR(ret));
          } else if (ObObjectStatus::INVALID != view_schema->get_object_status()) {
            schema_version_cnt_++;
          }
        }
      }
    }
  }

  return ret;
}

int ObDropTableHelper::calc_schema_version_cnt_for_sequence_(
    const ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else {
    if (table_schema.is_user_table() || table_schema.is_oracle_tmp_table()) {
      for (ObTableSchema::const_column_iterator iter = table_schema.column_begin();
           OB_SUCC(ret) && iter != table_schema.column_end(); ++iter) {
        ObColumnSchemaV2 &column_schema = (**iter);
        if (column_schema.is_identity_column()) {
          const uint64_t sequence_id = column_schema.get_sequence_id();
          if (OB_UNLIKELY(OB_INVALID_ID == sequence_id)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("sequence id is invalid", KR(ret));
          } else {
            // sequence
            schema_version_cnt_++;

            // obj priv
            ObArray<ObObjPriv> obj_privs;
            if (OB_FAIL(latest_schema_guard_.get_obj_privs(sequence_id, ObObjectType::SEQUENCE, obj_privs))) {
              LOG_WARN("fail to get obj privs", KR(ret), K(sequence_id));
            }
            schema_version_cnt_ += obj_privs.count();

            // audit
            ObArray<ObSAuditSchema> audits;
            if (FAILEDx(latest_schema_guard_.get_audit_schemas_in_owner(AUDIT_SEQUENCE, sequence_id, audits))) {
              LOG_WARN("fail to get audit schemas in owner", KR(ret), K(sequence_id));
            }
            schema_version_cnt_ += audits.count();
          }
        }
      }
    }
  }

  return ret;
}


int ObDropTableHelper::lock_fk_tables_by_id_(const ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else {
    const uint64_t table_id = table_schema.get_table_id();
    const ObIArray<ObForeignKeyInfo> &fk_infos = table_schema.get_foreign_key_infos();
    for (int64_t i = 0; OB_SUCC(ret) && i < fk_infos.count(); i++) {
      const ObForeignKeyInfo& fk_info = fk_infos.at(i);
      const ObSchemaType schema_type = fk_info.is_parent_table_mock_ ? MOCK_FK_PARENT_TABLE_SCHEMA : TABLE_SCHEMA;
      if (fk_info.parent_table_id_ != table_id && OB_FAIL(add_lock_object_by_id_(fk_info.parent_table_id_, schema_type, EXCLUSIVE))) {
        LOG_WARN("fail to add lock object by id", KR(ret), K(fk_info.parent_table_id_), K(schema_type));
      }
      if (OB_SUCC(ret) && fk_info.child_table_id_ != table_id && OB_FAIL(add_lock_object_by_id_(fk_info.child_table_id_, TABLE_SCHEMA, EXCLUSIVE))) {
        LOG_WARN("fail to add lock object by id", KR(ret), K(fk_info.child_table_id_));
      }
    }
  }

  return ret;
}

int ObDropTableHelper::lock_aux_tables_by_id_(const ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else {
    // index
    const ObIArray<ObAuxTableMetaInfo> &index_infos = table_schema.get_simple_index_infos();
    for (int64_t i = 0; OB_SUCC(ret) && i < index_infos.count(); i++) {
      const uint64_t index_id = index_infos.at(i).table_id_;
      if (OB_FAIL(add_lock_object_by_id_(index_id, TABLE_SCHEMA, EXCLUSIVE))) {
        LOG_WARN("fail to lock index id", KR(ret), K(index_id));
      }
    }

    // aux (vertical partition, lob piece, lob meta)
    ObArray<uint64_t> aux_table_ids;
    const uint64_t aux_lob_piece_table_id = table_schema.get_aux_lob_piece_tid();
    const uint64_t aux_lob_meta_table_id = table_schema.get_aux_lob_meta_tid();
    if (FAILEDx(table_schema.get_aux_vp_tid_array(aux_table_ids))) {
      LOG_WARN("get_aux_vp_tid_array failed", KR(ret));
    } else if (OB_INVALID_ID != aux_lob_piece_table_id && OB_FAIL(aux_table_ids.push_back(aux_lob_piece_table_id))) {
      LOG_WARN("push back aux_lob_piece_table_id failed", KR(ret));
    } else if (OB_INVALID_ID != aux_lob_meta_table_id && OB_FAIL(aux_table_ids.push_back(aux_lob_meta_table_id))) {
      LOG_WARN("push back aux_lob_meta_table_id failed", KR(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < aux_table_ids.count(); i++) {
        const uint64_t aux_table_id = aux_table_ids.at(i);
        if (OB_FAIL(add_lock_object_by_id_(aux_table_id, TABLE_SCHEMA, EXCLUSIVE))) {
          LOG_WARN("fail to lock aux table id", KR(ret), K(aux_table_id));
        }
      }
    }
  }

  return ret;
}

int ObDropTableHelper::lock_triggers_by_id_(const ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else {
    const ObIArray<uint64_t> &trigger_ids = table_schema.get_trigger_list();
    for (int64_t i = 0; OB_SUCC(ret) && i < trigger_ids.count(); i++) {
      const uint64_t trigger_id = trigger_ids.at(i);
      if (OB_FAIL(add_lock_object_by_id_(trigger_id, TRIGGER_SCHEMA, EXCLUSIVE))) {
        LOG_WARN("fail to lock trigger id", KR(ret), K(trigger_id));
      }
    }
  }

  return ret;
}

int ObDropTableHelper::lock_sequences_by_id_(const ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (table_schema.is_user_table() || table_schema.is_oracle_tmp_table()) {
    for (ObTableSchema::const_column_iterator iter = table_schema.column_begin();
        OB_SUCC(ret) && iter != table_schema.column_end(); ++iter) {
      ObColumnSchemaV2 &column_schema = (**iter);
      if (column_schema.is_identity_column()) {
        const uint64_t sequence_id = column_schema.get_sequence_id();
        if (OB_FAIL(add_lock_object_by_id_(sequence_id, SEQUENCE_SCHEMA, EXCLUSIVE))) {
          LOG_WARN("fail to lock sequence id", KR(ret), K(sequence_id));
        } else {
          ObArray<ObSAuditSchema> audits;
          if (OB_FAIL(latest_schema_guard_.get_audit_schemas_in_owner(AUDIT_SEQUENCE, sequence_id, audits))) {
            LOG_WARN("fail to get audit schemas in owner", KR(ret), K(sequence_id));
          }
          for (int64_t i = 0; OB_SUCC(ret) && i < audits.count(); i++) {
            const uint64_t audit_id = audits.at(i).get_audit_key().audit_id_;
            if (OB_FAIL(add_lock_object_by_id_(audit_id, AUDIT_SCHEMA, EXCLUSIVE))) {
              LOG_WARN("fail to lock audit by id", KR(ret), K(audit_id));
            }
          }
        }
      }
    }
  }

  return ret;
}

int ObDropTableHelper::lock_rls_by_id_(const ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else {
    const ObIArray<uint64_t> &rls_policy_ids = table_schema.get_rls_policy_ids();
    for (int64_t i = 0; OB_SUCC(ret) && i < rls_policy_ids.count(); i++) {
      const uint64_t rls_policy_id = rls_policy_ids.at(i);
      if (OB_FAIL(add_lock_object_by_id_(rls_policy_id, RLS_POLICY_SCHEMA, EXCLUSIVE))) {
        LOG_WARN("fail to lock rls policy id", KR(ret), K(rls_policy_id));
      }
    }

    const ObIArray<uint64_t> &rls_group_ids = table_schema.get_rls_group_ids();
    for (int64_t i = 0; OB_SUCC(ret) && i < rls_group_ids.count(); i++) {
      const uint64_t rls_group_id = rls_group_ids.at(i);
      if (OB_FAIL(add_lock_object_by_id_(rls_group_id, RLS_GROUP_SCHEMA, EXCLUSIVE))) {
        LOG_WARN("fail to lock rls group id", KR(ret), K(rls_group_id));
      }
    }

    const ObIArray<uint64_t> &rls_context_ids = table_schema.get_rls_context_ids();
    for (int64_t i = 0; OB_SUCC(ret) && i < rls_context_ids.count(); i++) {
      const uint64_t rls_context_id = rls_context_ids.at(i);
      if (OB_FAIL(add_lock_object_by_id_(rls_context_id, RLS_CONTEXT_SCHEMA, EXCLUSIVE))) {
        LOG_WARN("fail to lock rls context id", KR(ret), K(rls_context_id));
      }
    }
  }

  return ret;
}

int ObDropTableHelper::lock_audits_by_id_(const ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if ((table_schema.is_user_table() || table_schema.is_external_table())) {
    const uint64_t table_id = table_schema.get_table_id();
    ObArray<ObSAuditSchema> audits;
    if (OB_FAIL(latest_schema_guard_.get_audit_schemas_in_owner(AUDIT_TABLE, table_id, audits))) {
      LOG_WARN("fail to get audit schemas in owner", KR(ret), K(table_id));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < audits.count(); i++) {
      const uint64_t audit_id = audits.at(i).get_audit_key().audit_id_;
      if (OB_FAIL(add_lock_object_by_id_(audit_id, AUDIT_SCHEMA, EXCLUSIVE))) {
        LOG_WARN("fail to lock audit by id", KR(ret), K(audit_id));
      }
    }
  }

  return ret;
}

int ObDropTableHelper::add_table_to_tablet_autoinc_cleaner_(const ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(!table_schema.has_tablet())) {
    // do nothing
  } else if (OB_FAIL(tablet_autoinc_cleaner_.add_single_table(table_schema))) {
    LOG_WARN("fail to add single table", KR(ret), K(table_schema));
  } else {
    const uint64_t lob_meta_tid = table_schema.get_aux_lob_meta_tid();
    if (OB_INVALID_ID != lob_meta_tid) {
      const ObTableSchema *lob_meta_table_schema = nullptr;
      if (OB_FAIL(latest_schema_guard_.get_table_schema(lob_meta_tid, lob_meta_table_schema))) {
        LOG_WARN("failed to get aux table schema", KR(ret), K(lob_meta_tid));
      } else if (OB_ISNULL(lob_meta_table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid table schema", KR(ret), K(lob_meta_tid));
      } else if (OB_FAIL(tablet_autoinc_cleaner_.add_single_table(*lob_meta_table_schema))) {
        LOG_WARN("failed to add single table", KR(ret));
      }
    }
  }

  return ret;
}

int ObDropTableHelper::construct_drop_table_sql_(const ObTableSchema &table_schema, const ObTableItem &table_item)
{
  int ret = OB_SUCCESS;

  ddl_stmt_str_.reset();
  lib::Worker::CompatMode compat_mode = lib::Worker::CompatMode::INVALID;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(ObCompatModeGetter::get_tenant_mode(tenant_id_, compat_mode))) {
    LOG_WARN("fail to get tenant mode", KR(ret), K_(tenant_id));
  } else {
    bool is_offline_ddl_hidden_data_table = ObTableStateFlag::TABLE_STATE_HIDDEN_OFFLINE_DDL == table_schema.get_table_state_flag();
    bool use_drop_table_stmt_in_arg = (USER_INDEX == arg_.table_type_) || is_offline_ddl_hidden_data_table;
    bool is_oracle_mode = lib::Worker::CompatMode::ORACLE == compat_mode;
    bool is_cascade_constraints = is_oracle_mode && arg_.if_exist_;
    const ObTableType table_type = table_schema.get_table_type();
    if (use_drop_table_stmt_in_arg) {
      ddl_stmt_str_.append(arg_.ddl_stmt_str_);
    } else {
      if (OB_FAIL(ddl_service_->construct_drop_sql(table_item, table_type, is_oracle_mode, is_cascade_constraints, ddl_stmt_str_))) {
        LOG_WARN("fail to construct drop sql", KR(ret));
      }
    }
  }
  return ret;
}

int ObDropTableHelper::drop_table_(const ObTableSchema &table_schema, const ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service_impl = NULL;
  int64_t new_schema_version = OB_INVALID_VERSION;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_ISNULL(schema_service_impl = schema_service_->get_schema_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service impl is null", KR(ret));
  } else {
    ObDDLOperator ddl_operator(*schema_service_, *sql_proxy_);
    if (FAILEDx(drop_obj_privs_(table_schema.get_table_id(), ObObjectType::TABLE))) {
      LOG_WARN("fail to drop obj privs", KR(ret));
    } else if (OB_FAIL(schema_service_->gen_new_schema_version(tenant_id_, new_schema_version))) {
      LOG_WARN("fail to gen new schema version", KR(ret));
    } else if (OB_FAIL(ddl_operator.cleanup_autoinc_cache(table_schema))) {
      LOG_WARN("fail to cleanup autoinc cache", KR(ret), K_(tenant_id));
    } else if (OB_FAIL(drop_sequences_(table_schema))) {
      LOG_WARN("fail to drop sequences", KR(ret), K_(tenant_id), K(table_schema));
    } else if (OB_FAIL(drop_rls_object_(table_schema))) {
      LOG_WARN("fail to drop rls object", KR(ret));
    } else if (OB_FAIL(schema_service_impl->get_table_sql_service().drop_table(
                       table_schema,
                       new_schema_version,
                       trans_,
                       ddl_stmt_str,
                       false/*is_truncate_table*/,
                       false/*is_drop_db*/,
                       false/*is_force_drop_lonely_lob_aux_table*/,
                       NULL/*schema_guard*/,
                       &drop_table_ids_))) {
      LOG_WARN("fail to drop table", KR(ret), K_(tenant_id), K(table_schema));
    } else if (OB_FAIL(ddl_operator.sync_version_for_cascade_table(tenant_id_, table_schema.get_base_table_ids(), trans_)
               || OB_FAIL(ddl_operator.sync_version_for_cascade_table(tenant_id_, table_schema.get_depend_table_ids(), trans_)))) {
      LOG_WARN("fail to sync version for cascade tables", KR(ret), K_(tenant_id));
    } else if (OB_FAIL(sync_version_for_cascade_mock_fk_parent_table_(table_schema.get_depend_mock_fk_parent_table_ids()))) {
      LOG_WARN("fail to sync version for cascade mock fk parent table", KR(ret), K_(tenant_id));
    }

    // delete audit
    if (OB_SUCC(ret) && (table_schema.is_user_table() || table_schema.is_external_table())) {
      ObArray<ObSAuditSchema> audit_schemas;
      if (OB_FAIL(latest_schema_guard_.get_audit_schemas_in_owner(AUDIT_TABLE, table_schema.get_table_id(), audit_schemas))) {
        LOG_WARN("fail to get audit schemas in owner", KR(ret), K(table_schema.get_table_id()));
      } else {
        ObSqlString public_sql_string;
        for (int64_t i = 0; OB_SUCC(ret) && i < audit_schemas.count(); i++) {
          const ObSAuditSchema &audit_schema = audit_schemas.at(i);
          if (OB_FAIL(schema_service_->gen_new_schema_version(tenant_id_, new_schema_version))) {
            LOG_WARN("fail to gen new schema version", KR(ret), K_(tenant_id));
          } else if (OB_FAIL(schema_service_impl->get_audit_sql_service().handle_audit_metainfo(
                             audit_schema,
                             AUDIT_MT_DEL,
                             false/*need_update*/,
                             new_schema_version,
                             NULL/*ddl_stmt_str*/,
                             trans_,
                             public_sql_string))) {
            LOG_WARN("fail to handle audit metainfo", KR(ret), K(audit_schema));
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (table_schema.is_aux_table() && !is_inner_table(table_schema.get_table_id())) {
        ObSnapshotInfoManager snapshot_mgr;
        ObArray<ObTabletID> tablet_ids;
        SCN invalid_scn;
        if (OB_FAIL(snapshot_mgr.init(GCTX.self_addr()))) {
          LOG_WARN("fail to init snapshot mgr", KR(ret));
        } else if (OB_FAIL(table_schema.get_tablet_ids(tablet_ids))) {
          LOG_WARN("fail to get tablet ids", KR(ret));
          // when a index or lob is dropped, it should release all snapshots acquired, otherwise
          // if a building index is dropped in another session, the index build task cannot release snapshots
          // because the task needs schema to know tablet ids.
        } else if (OB_FAIL(snapshot_mgr.batch_release_snapshot_in_trans(
                           trans_,
                           SNAPSHOT_FOR_DDL,
                           tenant_id_,
                           -1/*schema_version*/,
                           invalid_scn/*snapshot_scn*/,
                           tablet_ids))) {
          LOG_WARN("fail to release ddl snapshot acquired by this table", KR(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (table_schema.is_external_table()) {
        if (OB_FAIL(ObExternalTableFileManager::get_instance().clear_inner_table_files(tenant_id_, table_schema.get_table_id(), trans_))) {
          LOG_WARN("fail to clear inner table files", KR(ret), K_(tenant_id), K(table_schema.get_table_id()));
        }
      } else if (table_schema.has_tablet() && OB_FAIL(ddl_operator.drop_tablet_of_table(table_schema, trans_))) {
        LOG_WARN("fail to drop tablet", KR(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (table_schema.is_vec_delta_buffer_type() && OB_FAIL(ObVectorIndexUtil::remove_dbms_vector_jobs(trans_, tenant_id_, table_schema.get_table_id()))) {
        LOG_WARN("failed to remove dbms vector jobs", KR(ret), K_(tenant_id), K(table_schema.get_table_id()));
      }
    }
  }

  return ret;
}

int ObDropTableHelper::drop_table_to_recyclebin_(const ObTableSchema &table_schema, const ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service_impl = NULL;
  const ObDatabaseSchema *recyclebin_database_schema = NULL;
  int64_t new_schema_version = OB_INVALID_VERSION;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_ISNULL(schema_service_impl = schema_service_->get_schema_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service impl is null", KR(ret));
  } else if (OB_FAIL(latest_schema_guard_.get_database_schema(OB_RECYCLEBIN_SCHEMA_ID, recyclebin_database_schema))) {
    LOG_WARN("fail to get recyclebin database schema", KR(ret));
  } else if (OB_ISNULL(recyclebin_database_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("__recyclebin db not exist", KR(ret));
  } else if (OB_FAIL(schema_service_->gen_new_schema_version(tenant_id_, new_schema_version))) {
    LOG_WARN("fail to gen new schema version", KR(ret));
  } else {
    ObDDLOperator ddl_operator(*schema_service_, *sql_proxy_);
    ObTableSchema new_table_schema;
    if (OB_FAIL(ddl_operator.cleanup_autoinc_cache(table_schema))) {
      LOG_WARN("fail to cleanup autoinc cache", KR(ret));
    } else if (OB_FAIL(new_table_schema.assign(table_schema))) {
      LOG_WARN("fail to assign schema", KR(ret));
    } else {
      ObSqlString new_table_name;
      new_table_schema.set_database_id(OB_RECYCLEBIN_SCHEMA_ID);
      new_table_schema.set_tablegroup_id(OB_INVALID_ID);
      new_table_schema.set_schema_version(new_schema_version);
      if (OB_FAIL(ddl_operator.construct_new_name_for_recyclebin(new_table_schema, new_table_name))) {
        LOG_WARN("fail to construct new name for table", KR(ret));
      } else if (OB_FAIL(new_table_schema.set_table_name(new_table_name.string()))) {
        LOG_WARN("fail to set new table name", KR(ret), K(new_table_name));
      } else {
        ObRecycleObject recycle_obj;
        recycle_obj.set_object_name(new_table_name.string());
        recycle_obj.set_original_name(table_schema.get_table_name_str());
        recycle_obj.set_tenant_id(table_schema.get_tenant_id());
        recycle_obj.set_database_id(table_schema.get_database_id());
        recycle_obj.set_table_id(table_schema.get_table_id());
        recycle_obj.set_tablegroup_id(table_schema.get_tablegroup_id());
        if (OB_FAIL(recycle_obj.set_type_by_table_schema(table_schema))) {
          LOG_WARN("fail to set type by table schema", KR(ret));
        } else if (OB_FAIL(schema_service_impl->insert_recyclebin_object(recycle_obj, trans_))) {
          LOG_WARN("fail to insert recyclebin object", KR(ret), K(recycle_obj));
        } else if (OB_FAIL(schema_service_impl->get_table_sql_service().update_table_options(
                           trans_,
                           table_schema,
                           new_table_schema,
                           OB_DDL_DROP_TABLE_TO_RECYCLEBIN,
                           ddl_stmt_str))) {
          LOG_WARN("fail to update table options", KR(ret));
        }
      }
    }
  }

  return ret;
}

int ObDropTableHelper::drop_triggers_(const ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else {
    const ObIArray<uint64_t> &trigger_ids = table_schema.get_trigger_list();
    for (int64_t i = 0; OB_SUCC(ret) && i < trigger_ids.count(); i++) {
      const uint64_t trigger_id = trigger_ids.at(i);
      const ObTriggerInfo *trigger_info = NULL;
      if (OB_FAIL(latest_schema_guard_.get_trigger_info(trigger_id, trigger_info))) {
        LOG_WARN("fail to get trigger info", KR(ret), K(trigger_id));
      } else if (OB_ISNULL(trigger_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("trigger info is null", KR(ret));
      } else if (trigger_info->is_in_recyclebin()) {
        ret= OB_ERR_UNEXPECTED;
        LOG_WARN("trigger is in recyclebin", KR(ret), K(trigger_id));
      } else {
        if (is_to_recyclebin_(table_schema)) {
          if (OB_FAIL(drop_trigger_to_recyclebin_(*trigger_info))) {
            LOG_WARN("fail to drop trigger", KR(ret), KPC(trigger_info));
          }
        } else {
          if (OB_FAIL(drop_trigger_(*trigger_info, table_schema))) {
            LOG_WARN("fail to drop trigger to recyclebin", KR(ret), KPC(trigger_info));
          }
        }
      }
    }
  }
  return ret;
}

int ObDropTableHelper::drop_trigger_(const ObTriggerInfo &trigger_info, const ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  int64_t new_schema_version = OB_INVALID_VERSION;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else {
    ObPLDDLOperator pl_ddl_operator(*schema_service_, *sql_proxy_);
    if (OB_FAIL(pl_ddl_operator.drop_trigger(trigger_info,
                                             trans_,
                                             NULL/*ddl_stmt_str*/,
                                             true/*is_update_table_schema_version*/,
                                             table_schema.get_in_offline_ddl_white_list()))) {
      LOG_WARN("fail to drop trigger", KR(ret), K(trigger_info));
    }
  }
  return ret;
}

int ObDropTableHelper::drop_trigger_to_recyclebin_(const ObTriggerInfo &trigger_info)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service_impl = NULL;
  const ObDatabaseSchema *recyclebin_database_schema = NULL;
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObTriggerInfo new_trigger_info;
  ObSqlString new_trigger_name;
  const ObTableSchema *base_table_schema = NULL;
  ObRecycleObject recyclebin_object;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_ISNULL(schema_service_impl = schema_service_->get_schema_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service impl is null", KR(ret));
  } else if (OB_FAIL(latest_schema_guard_.get_database_schema(OB_RECYCLEBIN_SCHEMA_ID, recyclebin_database_schema))) {
    LOG_WARN("fail to get recyclebin schema", KR(ret));
  } else if (OB_ISNULL(recyclebin_database_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("recyclebin schema is null", KR(ret));
  } else if (OB_FAIL(schema_service_->gen_new_schema_version(tenant_id_, new_schema_version))) {
    LOG_WARN("fail to gen new schema version", KR(ret));
  } else if (OB_FAIL(new_trigger_info.assign(trigger_info))) {
    LOG_WARN("fail to assign trigger info", KR(ret), K(trigger_info));
  } else if (FALSE_IT(new_trigger_info.set_schema_version(new_schema_version))) {
  } else if (OB_FAIL(latest_schema_guard_.get_table_schema(trigger_info.get_base_object_id(), base_table_schema))) {
    LOG_WARN("fail to get table schema", KR(ret), K(trigger_info.get_base_object_id()));
  } else if (OB_ISNULL(base_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("base table schema is null", KR(ret));
  } else {
    ObDDLOperator ddl_operator(*schema_service_, *sql_proxy_);
    if (OB_FAIL(ddl_operator.construct_new_name_for_recyclebin(new_trigger_info, new_trigger_name))) {
      LOG_WARN("fail to construct new name for recyclebin", KR(ret));
    }

    new_trigger_info.set_database_id(OB_RECYCLEBIN_SCHEMA_ID);
    new_trigger_info.set_trigger_name(new_trigger_name.string());

    recyclebin_object.set_tenant_id(tenant_id_);
    recyclebin_object.set_database_id(base_table_schema->get_database_id());
    recyclebin_object.set_table_id(trigger_info.get_trigger_id());
    recyclebin_object.set_tablegroup_id(OB_INVALID_ID);
    recyclebin_object.set_object_name(new_trigger_name.string());
    recyclebin_object.set_original_name(trigger_info.get_trigger_name());
    recyclebin_object.set_type(ObRecycleObject::TRIGGER);

    if (FAILEDx(schema_service_impl->insert_recyclebin_object(recyclebin_object, trans_))) {
      LOG_WARN("fail to insert recyclebin object", KR(ret), K(recyclebin_object));
    } else if (OB_FAIL(schema_service_impl->get_trigger_sql_service().drop_trigger(new_trigger_info, true/*drop_to_recyclebin*/, new_schema_version, trans_))) {
      LOG_WARN("fail to drop trigger", KR(ret), K(new_trigger_info), K(new_schema_version));
    }
  }
  return ret;
}

int ObDropTableHelper::drop_obj_privs_(const uint64_t obj_id, const ObObjectType obj_type)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service_impl = NULL;
  ObArray<ObObjPriv> obj_privs;

  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_ISNULL(schema_service_impl = schema_service_->get_schema_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service impl is null", KR(ret));
  } else if (OB_FAIL(latest_schema_guard_.get_obj_privs(obj_id, obj_type, obj_privs))) {
    LOG_WARN("fail to get obj privs", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < obj_privs.count(); i++) {
      const ObObjPriv &obj_priv = obj_privs.at(i);
      int64_t new_schema_version = OB_INVALID_VERSION;
      if (OB_FAIL(schema_service_->gen_new_schema_version(tenant_id_, new_schema_version))) {
        LOG_WARN("fail to gen new schema version", KR(ret));
      } else if (schema_service_impl->get_priv_sql_service().delete_obj_priv(obj_priv, new_schema_version, trans_)) {
        LOG_WARN("fail to delete obj priv", KR(ret), K(obj_priv));
      }
      // serial drop table:
      // In order to prevent being deleted, but there is no time to refresh the schema.
      // for example, obj priv has deleted, but obj schema unrefresh
      if (ret == OB_SEARCH_NOT_FOUND) {
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ObDropTableHelper::drop_sequences_(const ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (table_schema.is_user_table() || table_schema.is_oracle_tmp_table()) {
    for (ObTableSchema::const_column_iterator iter = table_schema.column_begin(); OB_SUCC(ret) && iter != table_schema.column_end(); ++iter) {
      ObColumnSchemaV2 &column_schema = (**iter);
      if (OB_FAIL(drop_sequence_(column_schema))) {
        LOG_WARN("fail to drop sequence", KR(ret), K(column_schema));
      }
    }
  }
  return ret;
}

int ObDropTableHelper::drop_rls_object_(const ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  ObString empty_str;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else {
    ObDDLOperator ddl_operator(*schema_service_, *sql_proxy_);
    for (int64_t i = 0; OB_SUCC(ret) && i < table_schema.get_rls_policy_ids().count(); i++) {
      const ObRlsPolicySchema *policy_schema = NULL;
      uint64_t policy_id = table_schema.get_rls_policy_ids().at(i);
      if (OB_FAIL(latest_schema_guard_.get_rls_policys(policy_id, policy_schema))) {
        LOG_WARN("fail to get rls policy schema", KR(ret), K(policy_id));
      } else if (OB_ISNULL(policy_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("rls policy schema is null", KR(ret));
      } else if (OB_FAIL(ddl_operator.drop_rls_policy(
                         *policy_schema,
                         trans_,
                         empty_str,
                         false/*is_update_table_schema*/,
                         NULL/*table_schema*/))) {
        LOG_WARN("fail to drop rls policy", KR(ret), KPC(policy_schema));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < table_schema.get_rls_group_ids().count(); i++) {
      const ObRlsGroupSchema *group_schema = NULL;
      uint64_t group_id = table_schema.get_rls_group_ids().at(i);
      if (OB_FAIL(latest_schema_guard_.get_rls_groups(group_id, group_schema))) {
        LOG_WARN("fail to get rls group schema", KR(ret), K(group_id));
      } else if (OB_ISNULL(group_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("rls group schema is null", KR(ret));
      } else if (OB_FAIL(ddl_operator.drop_rls_group(
                         *group_schema,
                         trans_,
                         empty_str,
                         false/*is_update_table_schema*/,
                         NULL/*table_schema*/))) {
        LOG_WARN("fail to drop rls group", KR(ret), KPC(group_schema));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < table_schema.get_rls_context_ids().count(); i++) {
      const ObRlsContextSchema *context_schema = NULL;
      uint64_t context_id = table_schema.get_rls_context_ids().at(i);
      if (OB_FAIL(latest_schema_guard_.get_rls_contexts(context_id, context_schema))) {
        LOG_WARN("fail to get rls context schema", KR(ret), K(context_id));
      } else if (OB_ISNULL(context_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("rls context schema is null", KR(ret));
      } else if (OB_FAIL(ddl_operator.drop_rls_context(
                         *context_schema,
                         trans_,
                         empty_str,
                         false/*is_update_table_schema*/,
                         NULL/*table_schema*/))) {
        LOG_WARN("fail to drop rls context", KR(ret), KPC(context_schema));
      }
    }
  }
  return ret;
}

int ObDropTableHelper::drop_sequence_(const ObColumnSchemaV2 &column_schema)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service_impl = NULL;
  int64_t new_schema_version = OB_INVALID_VERSION;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_ISNULL(schema_service_impl = schema_service_->get_schema_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service impl is null", KR(ret));
  } else if (column_schema.is_identity_column()) {
    const ObSequenceSchema *sequence_schema = NULL;
    if (OB_FAIL(latest_schema_guard_.get_sequence_schema(column_schema.get_sequence_id(), sequence_schema))) {
      LOG_WARN("get sequence schema fail", KR(ret), K(column_schema));
      if (ret == OB_ERR_UNEXPECTED) {
        // sequence has been deleted externally.
        // Oracle does not allow sequences internally created to be deleted externally.
        // In the future, it will be solved by adding columns to the internal table,
        // and then the error code conversion can be removed.
        ret = OB_SUCCESS;
      }
    } else if (OB_ISNULL(sequence_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sequence not exist", KR(ret), K(column_schema));
    } else if (OB_FAIL(drop_obj_privs_(sequence_schema->get_sequence_id(), ObObjectType::SEQUENCE))) {
      LOG_WARN("fail to drop obj privs", KR(ret));
    } else if (OB_FAIL(schema_service_->gen_new_schema_version(tenant_id_, new_schema_version))) {
      LOG_WARN("fail to gen new schema version", KR(ret), K_(tenant_id));
    } else if (OB_FAIL(schema_service_impl->get_sequence_sql_service().drop_sequence(*sequence_schema, new_schema_version, &trans_))) {
      LOG_WARN("fail to drop sequence", KR(ret), KPC(sequence_schema));
    }

    // delete sequence audit
    if (OB_SUCC(ret)) {
      ObArray<ObSAuditSchema> audit_schemas;
      if (OB_FAIL(latest_schema_guard_.get_audit_schemas_in_owner(AUDIT_SEQUENCE, sequence_schema->get_sequence_id(), audit_schemas))) {
        LOG_WARN("fail to get audit schemas in owner", KR(ret), K(sequence_schema->get_sequence_id()));
      } else {
        ObSqlString public_sql_string;
        for (int64_t i = 0; OB_SUCC(ret) && i < audit_schemas.count(); i++) {
          const ObSAuditSchema &audit_schema = audit_schemas.at(i);
          if (OB_FAIL(schema_service_->gen_new_schema_version(tenant_id_, new_schema_version))){
            LOG_WARN("fail to gen new schema version", KR(ret), K_(tenant_id));
          } else if (OB_FAIL(schema_service_impl->get_audit_sql_service().handle_audit_metainfo(
                             audit_schema,
                             AUDIT_MT_DEL,
                             false/*need_update*/,
                             new_schema_version,
                             NULL/*ddl_stmt_str*/,
                             trans_,
                             public_sql_string))) {
            LOG_WARN("fail to handle audit metainfo", KR(ret), K(audit_schema));
          }
        }
      }
    }
  }

  return ret;
}

int ObDropTableHelper::modify_dep_obj_status_(const int64_t idx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(idx < 0 || idx >= dep_objs_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("idx is invalid", KR(ret), K(idx), K(dep_objs_.count()));
  } else {
    for (int64_t j = 0; OB_SUCC(ret) && j < dep_objs_.at(idx).count(); j++) {
      const uint64_t obj_id = dep_objs_.at(idx).at(j).first;
      const ObObjectType obj_type = dep_objs_.at(idx).at(j).second;
      if (OB_INVALID_ID == obj_id) {
        // skipped by ddl
        continue;
      }
      if (OB_SUCC(ret) && ObObjectType::VIEW == obj_type) {
        const ObTableSchema* view_schema = nullptr;
        int64_t new_schema_version = OB_INVALID_VERSION;

        if (OB_FAIL(latest_schema_guard_.get_table_schema(obj_id, view_schema))) {
          LOG_WARN("fail to get table schema", KR(ret), K_(tenant_id), K(obj_id));
        } else if (OB_ISNULL(view_schema)) {
          ret = OB_ERR_PARALLEL_DDL_CONFLICT;
        } else if (ObObjectStatus::INVALID == view_schema->get_object_status()) {
          // do nothing
        } else if (OB_FAIL(schema_service_->gen_new_schema_version(tenant_id_, new_schema_version))) {
          LOG_WARN("fail to gen new schema version", KR(ret));
        } else {
          ObObjectStatus new_status = ObObjectStatus::INVALID;
          const bool update_object_status_ignore_version = false;
          ObDDLOperator ddl_operator(*schema_service_, *sql_proxy_);
          HEAP_VAR(ObTableSchema, new_dep_view) {
          if (OB_FAIL(new_dep_view.assign(*view_schema))) {
            LOG_WARN("fail to assign new dep view", KR(ret));
          } else if (OB_FAIL(ddl_operator.update_table_status(new_dep_view, new_schema_version, new_status,
                              update_object_status_ignore_version, trans_))) {
            LOG_WARN("failed to update table status", KR(ret));
          }
          } // end heap var
        }
      }
    }
  }

  return ret;
}

int ObDropTableHelper::deal_with_mock_fk_parent_tables_(const int64_t idx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(idx < 0 || idx >= mock_fk_parent_table_schemas_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("idx is invalid", KR(ret), K(idx), K(mock_fk_parent_table_schemas_.count()));
  } else {
    for (int64_t j = 0; OB_SUCC(ret) && j < mock_fk_parent_table_schemas_.at(idx).count(); j++) {
      if (OB_FAIL(deal_with_mock_fk_parent_table_(mock_fk_parent_table_schemas_.at(idx).at(j)))) {
        LOG_WARN("fail to deal with mock fk parent table", KR(ret));
      }
    }
  }
  return ret;
}

int ObDropTableHelper::deal_with_mock_fk_parent_table_(ObMockFKParentTableSchema &mock_fk_parent_table_schema)
{
  int ret = OB_SUCCESS;
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObMockFKParentTableOperationType operation_type = mock_fk_parent_table_schema.get_operation_type();
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(schema_service_->gen_new_schema_version(tenant_id_, new_schema_version))) {
    LOG_WARN("fail to gen new schema version", KR(ret));
  } else if (FALSE_IT(mock_fk_parent_table_schema.set_schema_version(new_schema_version))) {
  } else if (MOCK_FK_PARENT_TABLE_OP_CREATE_TABLE_BY_DROP_PARENT_TABLE == operation_type) {
    // create mock fk parent table:
    // - dropped table is parent table
    // --------------------------------------------------------------
    // create table t1 (c1 int primary key, c2 int);
    // create table t2 (c1 int primary key, c2 int, constraint fk1 foreign key (c2) references t1(c1));
    // drop table t1;
    // --------------------------------------------------------------
    if (OB_FAIL(create_mock_fk_parent_table_(mock_fk_parent_table_schema))) {
      LOG_WARN("fail to create mock fk parent table", KR(ret));
    }
  } else if (MOCK_FK_PARENT_TABLE_OP_DROP_TABLE == operation_type) {
    // drop mock fk parent table:
    // - dropped table is child table, and all columns of its mock parent table are unferenced, so drop entire mock parent table
    // --------------------------------------------------------------
    // create table t1 (c1 int primary key, c2 int, constraint fk1 foreign key (c2) references t2(c1));
    // drop table t1;
    // --------------------------------------------------------------
    if (OB_FAIL(drop_mock_fk_parent_table_(mock_fk_parent_table_schema))) {
      LOG_WARN("fail to drop mock fk parent table", KR(ret));
    }
  } else if (MOCK_FK_PARENT_TABLE_OP_DROP_COLUMN == operation_type || MOCK_FK_PARENT_TABLE_OP_UPDATE_SCHEMA_VERSION == operation_type) {
    // alter mock fk parent table:
    // - dropped table is child table, and some columns of its mock parent table are unferenced, so drop these columns
    // --------------------------------------------------------------
    // create table t1 (c1 int primary key, c2 int, constraint fk1 foreign key (c2) references t3(c1));
    // create table t2 (c1 int primary key, c2 int, constraint fk2 foreign key (c2) references t3(c2));
    // drop table t1;
    // --------------------------------------------------------------
    //
    // - dropped table is child table, and all columns of its mock parent table are still referenced, just update mock parent table's schema version
    // --------------------------------------------------------------
    // create table t1 (c1 int primary key, c2 int, constraint fk1 foreign key (c2) references t3(c1));
    // create table t2 (c1 int primary key, c2 int, constraint fk2 foreign key (c2) references t3(c1));
    // drop table t1;
    // --------------------------------------------------------------
    if (OB_FAIL(alter_mock_fk_parent_table_(mock_fk_parent_table_schema))) {
      LOG_WARN("fail to alter mock fk parent table", KR(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("operation type is invalid", KR(ret), K(operation_type));
  }

  return ret;
}

int ObDropTableHelper::create_mock_fk_parent_table_(const ObMockFKParentTableSchema &mock_fk_parent_table_schema)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service_impl = NULL;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_ISNULL(schema_service_impl = schema_service_->get_schema_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service impl is null", KR(ret));
  } else if (OB_FAIL(schema_service_impl->get_table_sql_service().add_mock_fk_parent_table(&trans_, mock_fk_parent_table_schema, true/*need_update_foreign_key*/))) {
    LOG_WARN("fail to add mock fk parent table", KR(ret));
  } else {
    ObDDLOperator ddl_operator(*schema_service_, *sql_proxy_);
    ObArray<uint64_t> child_table_ids;
    const common::ObIArray<ObForeignKeyInfo> &fk_infos = mock_fk_parent_table_schema.get_foreign_key_infos();
    for (int64_t i = 0; OB_SUCC(ret) && i < fk_infos.count(); i++) {
      if (OB_FAIL(child_table_ids.push_back(fk_infos.at(i).child_table_id_))) {
        LOG_WARN("fail to push back child table id", KR(ret));
      }
    }

    if (FAILEDx(ddl_operator.sync_version_for_cascade_table(tenant_id_, child_table_ids, trans_))) {
      LOG_WARN("fail to sync version for cascade table", KR(ret), K(child_table_ids));
    }
  }

  return ret;
}

int ObDropTableHelper::drop_mock_fk_parent_table_(const ObMockFKParentTableSchema &mock_fk_parent_table_schema)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service_impl = NULL;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_ISNULL(schema_service_impl = schema_service_->get_schema_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service impl is null", KR(ret));
  } else if (OB_FAIL(schema_service_impl->get_table_sql_service().drop_mock_fk_parent_table(
              &trans_, mock_fk_parent_table_schema))) {
    LOG_WARN("fail to drop mock fk parent table failed", KR(ret), K(mock_fk_parent_table_schema));
  }
  return ret;
}

int ObDropTableHelper::alter_mock_fk_parent_table_(ObMockFKParentTableSchema &mock_fk_parent_table_schema)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service_impl = NULL;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_ISNULL(schema_service_impl = schema_service_->get_schema_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service impl is null", KR(ret));
  } else if (OB_FAIL(schema_service_impl->get_table_sql_service().alter_mock_fk_parent_table(
              &trans_, mock_fk_parent_table_schema))) {
    LOG_WARN("fail to alter mock fk parent table failed", KR(ret), K(mock_fk_parent_table_schema));
  }
  return ret;
}

int ObDropTableHelper::sync_version_for_cascade_mock_fk_parent_table_(const ObIArray<uint64_t> &mock_fk_parent_table_ids)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service_impl = NULL;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_ISNULL(schema_service_impl = schema_service_->get_schema_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service impl is null", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < mock_fk_parent_table_ids.count(); i++) {
      const uint64_t mock_fk_parent_table_id = mock_fk_parent_table_ids.at(i);
      const ObMockFKParentTableSchema *mock_fk_parent_table_schema = NULL;
      ObMockFKParentTableSchema tmp_mock_fk_parent_table_schema;
      if (OB_FAIL(latest_schema_guard_.get_mock_fk_parent_table_schema(mock_fk_parent_table_id, mock_fk_parent_table_schema))) {
        LOG_WARN("fail to get mock fk parent table schema", KR(ret), K(mock_fk_parent_table_id));
      } else if (OB_ISNULL(mock_fk_parent_table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("mock fk parent table schema is null", KR(ret));
      } else if (OB_FAIL(tmp_mock_fk_parent_table_schema.assign(*mock_fk_parent_table_schema))) {
        LOG_WARN("fail to assign mock fk parent table schema", KR(ret), KPC(mock_fk_parent_table_schema));
      } else if (OB_FAIL(schema_service_impl->get_table_sql_service().update_mock_fk_parent_table_schema_version(&trans_, tmp_mock_fk_parent_table_schema))) {
        LOG_WARN("fail to update mock fk parent table schema version", KR(ret), K(tmp_mock_fk_parent_table_schema));
      }
    }
  }
  return ret;
}

bool ObDropTableHelper::is_to_recyclebin_(const ObTableSchema &table_schema)
{
  return arg_.to_recyclebin_
         && !table_schema.is_materialized_view()
         && !table_schema.is_tmp_table()
         && !table_schema.is_external_table()
         && !table_schema.is_aux_table()
         && !is_inner_table(table_schema.get_table_id());
}

int ObDropTableHelper::log_table_not_exist_msg_(const obrpc::ObTableItem &table_item)
{
  int ret = OB_SUCCESS;
  if (arg_.if_exist_) {
    ObSqlString warning_str;
    if (OB_FAIL(warning_str.append_fmt("%.*s.%.*s",
                                       table_item.database_name_.length(),
                                       table_item.database_name_.ptr(),
                                       table_item.table_name_.length(),
                                       table_item.table_name_.ptr()))) {
      LOG_WARN("append warning str failed", KR(ret), K(table_item));
    } else {
      LOG_USER_NOTE(OB_ERR_BAD_TABLE, static_cast<int>(warning_str.length()), warning_str.ptr());
      LOG_WARN("table not exist", KR(ret), K(table_item));
    }
  } else {
    if (OB_FAIL(err_table_list_.append_fmt("%.*s.%.*s,",
                                           table_item.database_name_.length(),
                                           table_item.database_name_.ptr(),
                                           table_item.table_name_.length(),
                                           table_item.table_name_.ptr()))) {
      LOG_WARN("failed to append err table", KR(ret));
    }
  }

  return ret;
}
