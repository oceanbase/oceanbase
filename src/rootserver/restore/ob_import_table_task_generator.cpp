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

#include "ob_recover_table_initiator.h"
#include "lib/hash/ob_hashset.h"
#include "lib/charset/ob_charset.h"
#include "share/ob_rpc_struct.h"
#include "share/backup/ob_backup_data_table_operator.h"
#include "rootserver/ob_rs_event_history_table_operator.h"
#include "ob_restore_util.h"
#include "share/restore/ob_recover_table_persist_helper.h"
#include "sql/parser/parse_node.h"
#include "rootserver/ddl_task/ob_ddl_task.h"
#include "share/restore/ob_import_util.h"

using namespace oceanbase;
using namespace share::schema;
using namespace common;
using namespace obrpc;
using namespace rootserver;
using namespace share;



int ObImportTableTaskGenerator::init(
    share::schema::ObMultiVersionSchemaService &schema_service, common::ObMySQLProxy &sql_proxy)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObImportTableTaskGenerator init twice", K(ret));
  } else {
    schema_service_ = &schema_service;
    sql_proxy_ = &sql_proxy;
    is_inited_ = true;
  }
  return ret;
}

int ObImportTableTaskGenerator::gen_import_task(
    share::ObImportTableJob &import_job,
    common::ObIArray<share::ObImportTableTask> &import_tasks)
{
  int ret = OB_SUCCESS;
  import_tasks.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObImportTableTaskGenerator not init", K(ret));
  } else if (!import_job.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid import job", K(ret), K(import_job));
  } else if (OB_FAIL(gen_db_import_tasks_(import_job, import_tasks))) {
    LOG_WARN("failed to gen db import tasks", K(ret), K(import_job));
  } else if (OB_FAIL(gen_table_import_tasks_(import_job, import_tasks))) {
    LOG_WARN("failed to gen table import tasks", K(ret), K(import_job));
  } else {
    LOG_INFO("finish gen import task", K(ret), K(import_job), K(import_tasks));
  }
  return ret;
}

int ObImportTableTaskGenerator::gen_db_import_tasks_(
    share::ObImportTableJob &import_job,
    common::ObIArray<share::ObImportTableTask> &import_tasks)
{
  int ret = OB_SUCCESS;
  MTL_SWITCH(OB_SYS_TENANT_ID) {
    if (import_job.get_import_arg().get_import_table_arg().is_import_all()) {
      ObSchemaGetterGuard guard;
      uint64_t src_tenant_id = import_job.get_src_tenant_id();
      common::ObArray<const ObSimpleDatabaseSchema*> database_schemas;
      if (OB_FAIL(ObImportTableUtil::get_tenant_schema_guard(*schema_service_, src_tenant_id, guard))) {
        LOG_WARN("failed to get tenant schema guard", K(ret), K(src_tenant_id));
      } else if (OB_FAIL(guard.get_database_schemas_in_tenant(src_tenant_id, database_schemas))) {
        LOG_WARN("failed to get database id", K(ret), K(src_tenant_id));
      }
      ARRAY_FOREACH(database_schemas, i) {
        const ObSimpleDatabaseSchema* database_schema = database_schemas.at(i);
        const share::ObImportDatabaseItem db_item(database_schema->get_name_case_mode(),
                                                          database_schema->get_database_name_str().ptr(),
                                                          database_schema->get_database_name_str().length());
        if (OB_FAIL(gen_one_db_import_tasks_(import_job, db_item, import_tasks))) {
          LOG_WARN("failed to generate one db import tasks", K(ret), K(db_item));
        }
      }
    } else {
      const common::ObSArray<share::ObImportDatabaseItem> &database_array = import_job.get_import_arg().get_import_database_array().get_items();
      ARRAY_FOREACH(database_array, i) {
        const share::ObImportDatabaseItem &db_item = database_array.at(i);
        if (OB_FAIL(gen_one_db_import_tasks_(import_job, db_item, import_tasks))) {
          LOG_WARN("failed to generate one db import tasks", K(ret), K(db_item));
        }
      }
    }
  }
  return ret;
}

int ObImportTableTaskGenerator::gen_one_db_import_tasks_(
    share::ObImportTableJob &import_job,
    const share::ObImportDatabaseItem &db_item,
    common::ObIArray<share::ObImportTableTask> &import_tasks)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObSchemaGetterGuard guard;
  uint64_t database_id = 0;
  uint64_t src_tenant_id = import_job.get_src_tenant_id();
  common::ObArray<const ObTableSchema *> table_schemas;

  if (OB_FAIL(ObImportTableUtil::get_tenant_schema_guard(*schema_service_, src_tenant_id, guard))) {
    LOG_WARN("failed to get tenant schema guard", K(ret), K(src_tenant_id));
  } else if (OB_FAIL(guard.get_database_id(src_tenant_id, db_item.name_, database_id))) {
    LOG_WARN("failed to get database id", K(ret), K(src_tenant_id), K(db_item));
  } else if (OB_INVALID_ID == database_id) {
    ret = OB_ERR_BAD_DATABASE;
    LOG_WARN("database not exist", K(ret));
    ObImportResult::Comment comment;
    if (OB_TMP_FAIL(databuff_printf(comment.ptr(), comment.capacity(), "import database not exist, %.*s",
        db_item.name_.length(), db_item.name_.ptr()))) {
      LOG_WARN("failed to databuff printf", K(ret));
    } else {
      import_job.get_result().set_result(false, comment);
    }
  } else if (OB_FAIL(guard.get_table_schemas_in_database(src_tenant_id, database_id, table_schemas))) {
    LOG_WARN("failed to get table schemas", K(ret), K(src_tenant_id), K(database_id));
  }
  ARRAY_FOREACH(table_schemas, i) {
    share::ObImportTableTask import_task;
    const ObTableSchema *table_schema = table_schemas.at(i);
    ObImportTableItem table_item;
    table_item.mode_ = db_item.mode_;
    table_item.database_name_ = db_item.name_;
    table_item.table_name_ = table_schema->get_table_name_str();
    if (!table_schema->is_user_table()) {
    } else if (OB_FAIL(fill_import_task_from_import_db_(import_job, guard, db_item, table_item, *table_schema, import_task))) {
      LOG_WARN("failed to fill import task", K(ret), K(import_job), K(db_item), K(table_item));
    } else if (OB_FAIL(import_tasks.push_back(import_task))) {
      LOG_WARN("failed to push back import task", K(ret), K(import_task));
    } else {
      LOG_INFO("[RECOVER_TABLE]succeed gen one import task", K(import_task));
    }
  }
  return ret;
}

int ObImportTableTaskGenerator::gen_table_import_tasks_(
    share::ObImportTableJob &import_job,
    common::ObIArray<share::ObImportTableTask> &import_tasks)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard guard;
  const ObTableSchema * table_schema = nullptr;
  const share::ObImportTableArg &import_arg = import_job.get_import_arg().get_import_table_arg();
  const common::ObSArray<share::ObImportTableItem> &table_array = import_arg.get_import_table_array().get_items();
  ARRAY_FOREACH(table_array, i) {
    const share::ObImportTableItem &table_item = table_array.at(i);
    share::ObImportTableTask import_task;
    if (OB_FAIL(gen_table_import_task_(import_job, table_item, import_task))) {
      LOG_WARN("failed to gen import task", K(ret), K(table_item));
    } else if (OB_FAIL(import_tasks.push_back(import_task))) {
      LOG_WARN("failed to push back import task", K(ret), K(import_task));
    } else {
      LOG_INFO("[RECOVER_TABLE]succeed gen one import task", K(import_task));
    }
  }

  return ret;
}

int ObImportTableTaskGenerator::gen_table_import_task_(
    share::ObImportTableJob &import_job,
    const share::ObImportTableItem &table_item,
    share::ObImportTableTask &import_task)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard guard;
  const uint64_t src_tenant_id = import_job.get_src_tenant_id();
  const ObTableSchema *table_schema = NULL;
  MTL_SWITCH(OB_SYS_TENANT_ID) {
    if (OB_FAIL(ObImportTableUtil::get_tenant_schema_guard(*schema_service_, src_tenant_id, guard))) {
      LOG_WARN("failed to get tenant schema guard", K(ret));
    } else if (OB_FAIL(guard.get_table_schema(src_tenant_id, table_item.database_name_, table_item.table_name_, false/*no index*/, table_schema))) {
      LOG_WARN("failed to get table schema", K(ret), K(src_tenant_id), K(table_item));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("table not exist", K(ret), K(src_tenant_id), K(table_item));
      int tmp_ret = OB_SUCCESS;
      ObImportResult::Comment comment;
      if (OB_TMP_FAIL(databuff_printf(comment.ptr(), comment.capacity(), "import table not exist, %.*s.%.*s",
          table_item.database_name_.length(), table_item.database_name_.ptr(),
          table_item.table_name_.length(), table_item.table_name_.ptr()))) {
        LOG_WARN("failed to databuff printf", K(ret));
      } else {
        import_job.get_result().set_result(false, comment);
      }
    } else if (OB_FAIL(fill_import_task_from_import_table_(import_job, guard, *table_schema, table_item, import_task))) {
      LOG_WARN("failed to fill import task", K(ret), K(import_job), K(table_item));
    }
  }
  return ret;
}

int ObImportTableTaskGenerator::fill_import_task_from_import_db_(
    share::ObImportTableJob &import_job,
    share::schema::ObSchemaGetterGuard &guard,
    const share::ObImportDatabaseItem &db_item,
    const share::ObImportTableItem &table_item,
    const share::schema::ObTableSchema &table_schema,
    share::ObImportTableTask &import_task)
{
  int ret = OB_SUCCESS;
  share::ObImportDatabaseItem remap_db_item;
  share::ObImportTableItem remap_table_item;
  const share::ObImportRemapArg &remap_arg = import_job.get_import_arg().get_remap_table_arg();

  if (OB_FAIL(remap_arg.get_remap_database(db_item, remap_db_item))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      // no remap, set target database name the same as source.
      remap_db_item = db_item;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get remap database", K(ret), K(remap_arg), K(db_item));
    }
  }


  if (OB_SUCC(ret)) {
    remap_table_item.mode_ = remap_db_item.mode_;
    remap_table_item.database_name_ = remap_db_item.name_;
    remap_table_item.table_name_ = table_item.table_name_;
  }

  if (FAILEDx(fill_import_task_(import_job, guard, table_schema, table_item, remap_table_item, import_task))) {
    LOG_WARN("failed to fill import task", K(ret), K(import_job), K(table_schema), K(table_item), K(remap_table_item));
  }

  return ret;
}

int ObImportTableTaskGenerator::fill_import_task_from_import_table_(
    share::ObImportTableJob &import_job,
    share::schema::ObSchemaGetterGuard &guard,
    const share::schema::ObTableSchema &table_schema,
    const share::ObImportTableItem &table_item,
    share::ObImportTableTask &import_task)
{
  int ret = OB_SUCCESS;
  share::ObImportTableItem remap_table_item;
  const share::ObImportRemapArg &remap_arg = import_job.get_import_arg().get_remap_table_arg();

  if (OB_FAIL(remap_arg.get_remap_table(table_item, remap_table_item))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      // no remap, set target name the same as source.
      remap_table_item = table_item;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get remap table", K(ret), K(remap_arg), K(table_item));
    }
  }


  if (FAILEDx(fill_import_task_(import_job, guard, table_schema, table_item, remap_table_item, import_task))) {
    LOG_WARN("failed to fill import task", K(ret), K(import_job), K(table_schema), K(table_item), K(remap_table_item));
  }

  return ret;
}

int ObImportTableTaskGenerator::fill_import_task_(
    share::ObImportTableJob &import_job,
    share::schema::ObSchemaGetterGuard &guard,
    const share::schema::ObTableSchema &table_schema,
    const share::ObImportTableItem &table_item,
    const share::ObImportTableItem &remap_table_item,
    share::ObImportTableTask &import_task)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_src_table_schema_(import_job, table_schema, table_item))) {
    LOG_WARN("failed to check src table schema", K(ret));
  } else if (OB_FAIL(import_task.set_src_database(table_item.database_name_))) {
    LOG_WARN("failed to set src database name", K(ret));
  } else if (OB_FAIL(import_task.set_src_table(table_item.table_name_))) {
    LOG_WARN("failed to set src table name", K(ret));
  } else if (OB_FAIL(import_task.set_target_database(remap_table_item.database_name_))) {
    LOG_WARN("failed to set target database name", K(ret));
  } else if (OB_FAIL(import_task.set_target_table(remap_table_item.table_name_))) {
    LOG_WARN("failed to set target table name", K(ret));
  } else if (OB_FAIL(fill_common_para_(import_job, table_schema, import_task))) {
    LOG_WARN("failed to fill task common para", K(ret));
  } else if (OB_FAIL(fill_tablegroup_(import_job, guard, table_schema, table_item, import_task))) {
    LOG_WARN("failed to set tablegroup", K(ret));
  } else if (OB_FAIL(fill_tablespace_(import_job, guard, table_schema, table_item, import_task))) {
    LOG_WARN("failed to set tablespace", K(ret));
  } else if (OB_FAIL(check_target_schema_(import_job, import_task))) {
    LOG_WARN("failed to check target schema", K(ret), K(import_task));
  }

  return ret;
}

int ObImportTableTaskGenerator::check_src_table_schema_(
    share::ObImportTableJob &import_job,
    const share::schema::ObTableSchema &table_schema,
    const share::ObImportTableItem &table_item)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (!table_schema.is_user_table()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("import not user table is not allowed", K(ret), K(table_item));
    ObImportResult::Comment comment;
    if (OB_TMP_FAIL(databuff_printf(comment.ptr(), comment.capacity(), "import table %.*s.%.*s is not user table",
        table_item.database_name_.length(), table_item.database_name_.ptr(),
        table_item.table_name_.length(), table_item.table_name_.ptr()))) {
      LOG_WARN("failed to databuff printf", K(ret));
    } else {
      import_job.get_result().set_result(false/*failed*/, comment);
    }
  } else if (table_schema.is_in_recyclebin()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("import table in recyclebin is not allowed", K(ret), K(table_item));
    ObImportResult::Comment comment;
    if (OB_TMP_FAIL(databuff_printf(comment.ptr(), comment.capacity(), "import table %.*s.%.*s is in recyclebin",
        table_item.database_name_.length(), table_item.database_name_.ptr(),
        table_item.table_name_.length(), table_item.table_name_.ptr()))) {
      LOG_WARN("failed to databuff printf", K(ret));
    } else {
      import_job.get_result().set_result(false/*failed*/, comment);
    }
  } else if (table_schema.is_user_hidden_table()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("import hidden user table is not allowed", K(ret), K(table_item));
    ObImportResult::Comment comment;
    if (OB_TMP_FAIL(databuff_printf(comment.ptr(), comment.capacity(), "import table %.*s.%.*s is hidden table",
        table_item.database_name_.length(), table_item.database_name_.ptr(),
        table_item.table_name_.length(), table_item.table_name_.ptr()))) {
      LOG_WARN("failed to databuff printf", K(ret));
    } else {
      import_job.get_result().set_result(false/*failed*/, comment);
    }
  }
  return ret;
}

int ObImportTableTaskGenerator::check_target_schema_(
    share::ObImportTableJob &import_job,
    const share::ObImportTableTask &task)
{
  int ret = OB_SUCCESS;
  const uint64_t target_tenant_id = task.get_tenant_id();
  bool is_exist = true;
  int tmp_ret = OB_SUCCESS;
  if (OB_FAIL(ObImportTableUtil::check_database_schema_exist(*schema_service_,
                                                             target_tenant_id,
                                                             task.get_target_database(),
                                                             is_exist))) {
    LOG_WARN("failed to check target database schema exist", K(ret), K(task));
  } else if (!is_exist) {
    ret = OB_ERR_BAD_DATABASE;;
    LOG_INFO("database not exist", K(ret), K(task.get_target_database()));
    ObImportResult::Comment comment;
    if (OB_TMP_FAIL(databuff_printf(comment.ptr(), comment.capacity(), "import database %.*s not exist",
        task.get_target_database().length(), task.get_target_database().ptr()))) {
      LOG_WARN("failed to databuff printf", K(ret));
    } else {
      import_job.get_result().set_result(false, comment);
    }
  } else if (OB_FAIL(ObImportTableUtil::check_table_schema_exist(*schema_service_,
                                                                 target_tenant_id,
                                                                 task.get_target_database(),
                                                                 task.get_target_table(),
                                                                 is_exist))) {
    LOG_WARN("failed to check target table schema exist", K(ret), K(task));
  } else if (is_exist) {
    ret = OB_ERR_TABLE_EXIST;
    LOG_INFO("target table exist", K(ret), K(task.get_target_table()));
    ObImportResult::Comment comment;
    if (OB_TMP_FAIL(databuff_printf(comment.ptr(), comment.capacity(), "import table %.*s.%.*s exist",
        task.get_target_database().length(), task.get_target_database().ptr(),
        task.get_target_table().length(), task.get_target_table().ptr()))) {
      LOG_WARN("failed to databuff printf", K(ret));
    } else {
      import_job.get_result().set_result(false, comment);
    }
  }
  is_exist = false;
  if (task.get_target_tablegroup().empty()) {
  } else if (FAILEDx(ObImportTableUtil::check_tablegroup_exist(*schema_service_,
                                                               target_tenant_id,
                                                               task.get_target_tablegroup(),
                                                               is_exist))) {
    LOG_WARN("failed to check tablegroup exist", K(ret));
  } else if (!is_exist) {
    ret = OB_TABLEGROUP_NOT_EXIST;
    LOG_WARN("tablegroup not exist", K(ret), K(task.get_target_tablegroup()));
    ObImportResult::Comment comment;
    if (OB_TMP_FAIL(databuff_printf(comment.ptr(), comment.capacity(), "remap tablegroup %.*s not exist",
        task.get_target_tablegroup().length(), task.get_target_tablegroup().ptr()))) {
      LOG_WARN("failed to databuff printf", K(ret));
    } else {
      import_job.get_result().set_result(false, comment);
    }
  }
  is_exist = false;
  if (task.get_target_tablespace().empty()) {
  } else if (FAILEDx(ObImportTableUtil::check_tablespace_exist(*schema_service_,
                                                               target_tenant_id,
                                                               task.get_target_tablespace(),
                                                               is_exist))) {
    LOG_WARN("failed to check tablespace exist", K(ret));
  } else if (!is_exist) {
    ret = OB_TABLESPACE_NOT_EXIST;
    LOG_WARN("tablespace not exist", K(ret), K(task.get_target_tablespace()));
    ObImportResult::Comment comment;
    if (OB_TMP_FAIL(databuff_printf(comment.ptr(), comment.capacity(), "remap tablespace %.*s not exist",
        task.get_target_tablespace().length(), task.get_target_tablespace().ptr()))) {
      LOG_WARN("failed to databuff printf", K(ret));
    } else {
      import_job.get_result().set_result(false, comment);
    }
  }
  return ret;
}

int ObImportTableTaskGenerator::fill_common_para_(
    const share::ObImportTableJob &import_job,
    const share::schema::ObTableSchema &table_schema,
    share::ObImportTableTask &task)
{
  int ret = OB_SUCCESS;
  int64_t task_id = 0;
  if (OB_FAIL(ObDDLTask::fetch_new_task_id(*sql_proxy_, import_job.get_tenant_id(), task_id))) {
    LOG_WARN("fail to fecth new ddl task id", K(ret));
  } else {
    task.set_tenant_id(import_job.get_tenant_id());
    task.set_task_id(task_id);
    task.set_job_id(import_job.get_job_id());
    task.set_src_tenant_id(import_job.get_src_tenant_id());
    task.set_table_column(table_schema.get_column_count());
    task.set_status(ObImportTableTaskStatus(ObImportTableTaskStatus::INIT));
    task.set_start_ts(ObTimeUtility::current_time());
    task.set_total_bytes(table_schema.get_tablet_size());
    //task.set_total_rows()
    task.set_total_index_count(table_schema.get_simple_index_infos().count());
    task.set_total_constraint_count(table_schema.get_constraint_count());
    task.set_total_ref_constraint_count(table_schema.get_foreign_key_infos().count());
    task.set_total_trigger_count(table_schema.get_trigger_list().count());
  }

  return ret;
}

int ObImportTableTaskGenerator::fill_tablespace_(
    const share::ObImportTableJob &import_job,
    share::schema::ObSchemaGetterGuard &guard,
    const share::schema::ObTableSchema &table_schema,
    const share::ObImportTableItem &table_item,
    share::ObImportTableTask &task)
{
  int ret = OB_SUCCESS;
  const uint64_t src_tenant_id = import_job.get_src_tenant_id();
  uint64_t tablespace_id = table_schema.get_tablespace_id();
  const schema::ObTablespaceSchema *schema = nullptr;
  if (OB_INVALID_ID == tablespace_id) {
    LOG_DEBUG("[RECOVER_TABLE]no tablespace", K(table_schema));
  } else if (OB_FAIL(guard.get_tablespace_schema(src_tenant_id, tablespace_id, schema))) {
    LOG_WARN("failed to get tablesapce schema", K(ret), K(src_tenant_id), K(tablespace_id));
  } else if (OB_ISNULL(schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablespace schema must not be null", K(ret), K(src_tenant_id), K(tablespace_id));
  } else if (OB_FAIL(task.set_src_tablespace(schema->get_tablespace_name_str()))) {
    LOG_WARN("failed to set src table space", K(ret));
  } else {
    share::ObImportTablespaceItem src_ts;
    share::ObImportTablespaceItem target_ts;
    const ObString &ts_name = schema->get_tablespace_name();
    src_ts.mode_ = table_item.mode_;
    src_ts.name_ = ts_name;

    const share::ObImportRemapArg &remap_arg = import_job.get_import_arg().get_remap_table_arg();
    if (OB_FAIL(remap_arg.get_remap_tablespace(src_ts, target_ts))) {
      if (OB_ENTRY_NOT_EXIST == ret && OB_FAIL(task.set_target_tablespace(ts_name))) {
        // no remap, set the same as source tablespace
        LOG_WARN("failed to set target tablespace", K(ret), K(ts_name));
      }
    } else if (OB_FAIL(task.set_target_tablespace(target_ts.name_))) {
      LOG_WARN("failed to set target tablespace", K(ret), K(target_ts));
    }
  }

  return ret;
}

int ObImportTableTaskGenerator::fill_tablegroup_(
    const share::ObImportTableJob &import_job,
    share::schema::ObSchemaGetterGuard &guard,
    const share::schema::ObTableSchema &table_schema,
    const share::ObImportTableItem &table_item,
    share::ObImportTableTask &task)
{
  int ret = OB_SUCCESS;
  const uint64_t src_tenant_id = import_job.get_src_tenant_id();
  uint64_t tablegroup_id = table_schema.get_tablegroup_id();
  const schema::ObSimpleTablegroupSchema *tablegroup_schema = nullptr;
  if (OB_INVALID_ID == tablegroup_id) {
    LOG_DEBUG("[RECOVER_TABLE]no tablegroup", K(table_schema));
  } else if (OB_FAIL(guard.get_tablegroup_schema(src_tenant_id, tablegroup_id, tablegroup_schema))) {
    LOG_WARN("failed to get tablegroup scheam", K(ret));
  } else if (OB_ISNULL(tablegroup_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablegroup schema must not be null", K(ret), K(src_tenant_id), K(tablegroup_id));
  } else if (OB_FAIL(task.set_src_tablegroup(tablegroup_schema->get_tablegroup_name()))) {
    LOG_WARN("failed to set tablegroup name", K(ret));
  } else {
    share::ObImportTablegroupItem src_tg;
    share::ObImportTablegroupItem target_tg;
    const ObString &tg_name = tablegroup_schema->get_tablegroup_name();
    src_tg.mode_ = table_item.mode_;
    src_tg.name_ = tg_name;

    const share::ObImportRemapArg &remap_arg = import_job.get_import_arg().get_remap_table_arg();
    if (OB_FAIL(remap_arg.get_remap_tablegroup(src_tg, target_tg))) {
      if (OB_ENTRY_NOT_EXIST == ret && OB_FAIL(task.set_target_tablegroup(tg_name))) {
        // no remap, set the same as source tablegroup
        LOG_WARN("failed to set target tablegroup", K(ret), K(tg_name));
      }
    } else if (OB_FAIL(task.set_target_tablegroup(target_tg.name_))) {
      LOG_WARN("failed to set target tablegroup", K(ret), K(target_tg));
    }
  }

  return ret;
}