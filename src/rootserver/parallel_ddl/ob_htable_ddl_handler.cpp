/**
 * Copyright (c) 2025 OceanBase
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
#include "rootserver/parallel_ddl/ob_htable_ddl_handler.h"
#include "rootserver/parallel_ddl/ob_create_table_helper.h"
#include "rootserver/parallel_ddl/ob_drop_table_helper.h"
#include "rootserver/parallel_ddl/ob_create_tablegroup_helper.h"
#include "rootserver/parallel_ddl/ob_drop_tablegroup_helper.h"

using namespace oceanbase::lib;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::rootserver;
using namespace oceanbase::obrpc;
using namespace oceanbase::transaction::tablelock;
using namespace oceanbase::table;

int ObHTableDDLHandler::gen_task_id_and_schema_versions_(const uint64_t schema_version_cnt,
                                                         int64_t &task_id)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = arg_.exec_tenant_id_;
  ObDDLTransController &controller = schema_service_.get_ddl_trans_controller();
  if (OB_FAIL(ObDDLHelperUtils::gen_task_id_and_schema_versions(&controller, tenant_id,
        schema_version_cnt, task_id))) {
    LOG_WARN("fail to gen task_id and schema_version", KR(ret), K(tenant_id));
  }
  return ret;
}
int ObHTableDDLHandler::wait_and_end_ddl_trans_(const int return_ret,
                                                const int64_t task_id,
                                                ObDDLSQLTransaction &trans,
                                                bool &need_clean_failed)
{
  int ret = return_ret;
  const uint64_t tenant_id = arg_.exec_tenant_id_;
  ObDDLTransController &controller = schema_service_.get_ddl_trans_controller();
  if (OB_FAIL(ObDDLHelperUtils::wait_and_end_ddl_trans(ret, &schema_service_, &controller, tenant_id,
        task_id, trans, need_clean_failed))) {
    LOG_WARN("fail to wait and end ddl trans", KR(ret));
  }
  return ret;
}

int ObHTableDDLHandler::construct_result_()
{
  int ret = OB_SUCCESS;
  ObSchemaVersionGenerator *tsi_generator = GET_TSI(TSISchemaVersionGenerator);
  int64_t curr_schema_version = OB_INVALID_VERSION;

  if (OB_ISNULL(tsi_generator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tsi schema version generator is null", KR(ret));
  } else if (OB_FAIL(tsi_generator->get_current_version(curr_schema_version))) {
    LOG_WARN("fail to get current version", KR(ret));
  } else {
    res_.schema_version_ = curr_schema_version;
  }

  return ret;
}

int ObCreateHTableHandler::init()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    param_ = static_cast<ObCreateHTableDDLParam*>(arg_.ddl_param_);
    if (OB_ISNULL(param_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("param_ is null", KR(ret));
    } else if (param_->cf_arg_list_.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cf_arg_list_ is empty", KR(ret));
    } else {
      tablegroup_name_ = param_->table_group_arg_.tablegroup_schema_.get_tablegroup_name();
      database_name_ = param_->cf_arg_list_.at(0).db_name_;
      for (int64_t i = 0; OB_SUCC(ret) && i < param_->cf_arg_list_.count(); ++i) {
        if (OB_FAIL(table_names_.push_back(param_->cf_arg_list_.at(i).schema_.get_table_name_str()))) {
          LOG_WARN("fail to push back table name", KR(ret));
        }
      }
      if (OB_SUCC(ret)) {
        // data table + 1503 + tablegroup
        schema_version_cnt_ = param_->cf_arg_list_.count() + 1 + 1;
        is_inited_ = true;
      }
    }
  }

  return ret;
}

int ObCreateHTableHandler::gen_create_helpers_(ObCreateTablegroupHelper *&create_tablegroup_helper,
                                               ObCreateTableGroupRes *&create_tablegroup_result,
                                               ObIArray<ObCreateTableHelper*> &create_table_helpers,
                                               ObIArray<ObCreateTableRes*> &create_table_results,
                                               ObDDLSQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  int64_t count = 0;
  const uint64_t tenant_id = arg_.exec_tenant_id_;

  if (OB_ISNULL(param_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("arguments are null", KR(ret));
  } else if (FALSE_IT(count = param_->cf_arg_list_.count())) {
  } else if (!create_table_helpers.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("create_table_helpers is not empty", KR(ret));
  } else if (!create_table_results.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("create_table_results is not empty", KR(ret), K(create_table_results));
  } else if (OB_FAIL(alloc_and_construct_(allocator_, count, create_table_results))) {
    LOG_WARN("fail to alloc and construct create_table_results", KR(ret), K(count));
  } else if (OB_ISNULL(create_tablegroup_result = OB_NEWx(ObCreateTableGroupRes, &allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc ObCreateTableGroupRes", KR(ret), K(sizeof(ObCreateTableGroupRes)));
  } else if (OB_ISNULL(create_tablegroup_helper = OB_NEWx(ObCreateTablegroupHelper, &allocator_,
      &schema_service_, tenant_id, param_->table_group_arg_, *create_tablegroup_result, &trans))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc ObCreateTableGroupRes", KR(ret), K(sizeof(ObCreateTableGroupRes)));
  } else if (OB_FAIL(create_tablegroup_helper->init(ddl_service_))) {
    LOG_WARN("fail to init create tablegroup helper", KR(ret));
  } else if (OB_FAIL(create_table_helpers.prepare_allocate(count))) {
    LOG_WARN("fail to prepare allocate create_table_helpers", KR(ret), K(count));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(sizeof(ObCreateTableHelper) * count)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc ObCreateTableHelper", KR(ret), K(sizeof(ObCreateTableHelper)), K(count));
  } else {
    for (int64_t i = 0; i < count && OB_SUCC(ret); i++) {
      ObCreateTableArg &create_table_arg = param_->cf_arg_list_.at(i);
      ObCreateTableRes &create_table_res = *create_table_results.at(i);
      create_table_helpers.at(i) = new (buf + sizeof(ObCreateTableHelper) * i)
          ObCreateTableHelper(&schema_service_, tenant_id, create_table_arg, create_table_res, &trans);
      if (OB_FAIL(create_table_helpers.at(i)->init(ddl_service_))) {
        LOG_WARN("fail to init create table helper", KR(ret));
      }
    }
  }

  return ret;
}

void ObCreateHTableHandler::clear_helpers_(ObCreateTablegroupHelper *create_tablegroup_helper,
                                           ObCreateTableGroupRes *create_tablegroup_result,
                                           ObIArray<ObCreateTableHelper*> &create_table_helpers,
                                           ObIArray<ObCreateTableRes*> &create_table_results)
{
  // tablegroup
  OB_DELETEx(ObCreateTablegroupHelper, &allocator_, create_tablegroup_helper);
  OB_DELETEx(ObCreateTableGroupRes, &allocator_, create_tablegroup_result);

  // tables
  for (int i = 0; i < create_table_helpers.count(); i++) {
    ObCreateTableHelper *helper = create_table_helpers.at(i);
    OB_DELETEx(ObCreateTableHelper, &allocator_, helper);
  }
  for (int i = 0; i < create_table_results.count(); i++) {
    ObCreateTableRes *res = create_table_results.at(i);
    OB_DELETEx(ObCreateTableRes, &allocator_, res);
  }
}

// 1. start trans
// 2. lock tablegroup and table by name
// 3. alloc schema versions
// 4. create table group
// 5. create tables
// 6. end trans
int ObCreateHTableHandler::handle()
{
  int ret = OB_SUCCESS;
  ObDDLSQLTransaction trans(&schema_service_, false/*need_end_signal*/, false/*enable_query_stash*/, true/*enable_ddl_parallel*/);
  ObMySQLProxy *sql_proxy = &(ddl_service_.get_sql_proxy());
  const uint64_t tenant_id = arg_.exec_tenant_id_;
  bool with_snapshot = false;
  int64_t fake_schema_version = 1000;
  ObCreateTablegroupHelper *create_tablegroup_helper = nullptr;
  ObCreateTableGroupRes *create_tablegroup_result = nullptr;
  ObSEArray<ObCreateTableHelper*, 4> create_table_helpers;
  ObSEArray<ObCreateTableRes*, 4> create_table_results;
  create_table_helpers.set_attr(ObMemAttr(OB_SERVER_TENANT_ID, "TmpTbHelps"));
  create_table_results.set_attr(ObMemAttr(OB_SERVER_TENANT_ID, "TmpTbRes"));
  ObHTableLockHelper lock_helper(&schema_service_, tenant_id, &trans);

  if (OB_ISNULL(sql_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else if (OB_FAIL(trans.start(sql_proxy, tenant_id, fake_schema_version, with_snapshot))) {
    LOG_WARN("fail to start trans", KR(ret), K(tenant_id), K(fake_schema_version), K(with_snapshot));
  } else if (OB_FAIL(lock_helper.init(ddl_service_))) { // lock tablegroup and tables in advance
    LOG_WARN("fail to init lock helper", KR(ret), K(tenant_id));
  } else if (OB_FAIL(lock_helper.lock_objects(tablegroup_name_, database_name_, table_names_, false /* need_lock_id*/))) {
    LOG_WARN("fail to lock objects", KR(ret), K(tenant_id), K_(tablegroup_name), K_(database_name), K_(table_names));
  } else if (OB_FAIL(gen_task_id_and_schema_versions_(schema_version_cnt_, task_id_))) {
    LOG_WARN("fail to gen task id and schema versions", KR(ret), K_(schema_version_cnt));
  } else if (OB_FAIL(gen_create_helpers_(create_tablegroup_helper, create_tablegroup_result,
      create_table_helpers, create_table_results, trans))) {
    LOG_WARN("fail to gen helpers", KR(ret));
  } else if (OB_ISNULL(create_tablegroup_helper)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("create_tablegroup_helper is null", KR(ret));
  } else if (OB_ISNULL(create_tablegroup_result)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("create_tablegroup_result is null", KR(ret));
  } else {
    // create tablegroup
    if (OB_FAIL(create_tablegroup_helper->execute())) {
      LOG_WARN("fail to execute create tablegroup", KR(ret), K(tenant_id));
    }
    #ifdef ERRSIM
    if (OB_SUCC(ret)) {
      ret = OB_E(common::EventTable::EN_CREATE_HTABLE_TG_FINISH_ERR) OB_SUCCESS;
      if (OB_FAIL(ret)) {
        LOG_WARN("[ERRSIM] fail to create tablegroup", KR(ret));
      }
    }
    #endif

    // create tables
    for (int i = 0; i < create_table_helpers.count() && OB_SUCC(ret); i++) {
      ObCreateTableHelper *helper = create_table_helpers.at(i);
      if (OB_ISNULL(helper)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ObCreateTableHelper is null", KR(ret), K(i));
      } else if (OB_FAIL(helper->execute())) {
        LOG_WARN("fail to execute create table", KR(ret), K(i));
      }
      #ifdef ERRSIM
      if (OB_SUCC(ret)) {
        ret = OB_E(common::EventTable::EN_CREATE_HTABLE_CF_FINISH_ERR) OB_SUCCESS;
        if (OB_FAIL(ret)) {
          LOG_WARN("[ERRSIM] fail to create table", KR(ret));
        }
      }
      #endif
    }
  }
  bool need_clean_failed = false;
  if (OB_FAIL(wait_and_end_ddl_trans_(ret, task_id_, trans, need_clean_failed))) {
    LOG_WARN("fail to wait and end ddl trans", KR(ret));
    if (need_clean_failed) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(clean_on_fail_commit_(create_tablegroup_helper, create_table_helpers))) {
        LOG_WARN("fail to clean on fail commit", KR(tmp_ret));
      }
    }
  }

  if (FAILEDx(construct_result_())) {
    LOG_WARN("fail to construct result", KR(ret));
  }

  clear_helpers_(create_tablegroup_helper, create_tablegroup_result, create_table_helpers, create_table_results);

  return ret;
}

int ObDropHTableHandler::init_drop_table_args_(ObIArray<ObDropTableArg*> &drop_table_args)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = arg_.exec_tenant_id_;

  if (table_names_.empty() || table_ids_.empty() || drop_table_args.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("arg is empty", KR(ret), K_(table_names), K_(table_ids), K(drop_table_args));
  } else if (table_ids_.count() != table_names_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table_ids not equal to table_names count", KR(ret), K(table_ids_.count()),
        K(table_names_.count()));
  } else if (table_ids_.count() != drop_table_args.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table_ids not equal to drop_table_args count", KR(ret), K(table_ids_.count()),
        K(drop_table_args.count()));
  } else {
    ObLatestSchemaGuard latest_schema_guard(&schema_service_, arg_.exec_tenant_id_);
    for (int i = 0; i < drop_table_args.count() && OB_SUCC(ret); i++) {
      ObDropTableArg *arg = drop_table_args.at(i);
      const ObTableSchema *schema = nullptr;
      if (OB_FAIL(latest_schema_guard.get_table_schema(table_ids_.at(i), schema))) {
        LOG_WARN("fail to get table schema", KR(ret), K(table_ids_.at(i)));
      } else if (OB_ISNULL(schema) || OB_ISNULL(arg)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table schema or arg is null", KR(ret), K(i), KP(arg));
      } else {
        arg->if_exist_ = true;
        arg->to_recyclebin_ = false;
        arg->table_type_ = ObTableType::USER_TABLE;
        arg->tenant_id_ = tenant_id;
        arg->exec_tenant_id_ = tenant_id;
        arg->task_id_ = task_id_;
        ObTableItem item;
        item.table_id_ = schema->get_table_id();
        item.mode_ = schema->get_name_case_mode();
        item.is_hidden_ = schema->is_user_hidden_table();
        item.table_name_ = table_names_.at(i);
        item.database_name_ = database_name_;
        if (OB_FAIL(arg->tables_.push_back(item))) {
          LOG_WARN("fail to push back table item", KR(ret), K(i));
        }
      }
    }
  }

  return ret;
}

int ObDropHTableHandler::gen_drop_helpers_(ObDropTablegroupHelper *&drop_tablegroup_helper,
                                           ObParallelDDLRes *&drop_tablegroup_result,
                                           ObIArray<ObDropTableHelper*> &drop_table_helpers,
                                           ObIArray<ObDropTableArg*> &drop_table_args,
                                           ObIArray<ObDropTableRes*> &drop_table_results,
                                           ObDDLSQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  int64_t count = table_names_.count();
  const uint64_t tenant_id = arg_.exec_tenant_id_;

  if (OB_ISNULL(param_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("arguments are null", KR(ret));
  } else if (0 == count) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("drop_schemas_ is empty", KR(ret));
  } else if (!drop_table_helpers.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("drop_table_helpers is not empty", KR(ret));
  } else if (!drop_table_args.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("drop_table_args is not empty", KR(ret), K(drop_table_args));
  } else if (!drop_table_results.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("drop_table_results is not empty", KR(ret), K(drop_table_results));
  } else if (OB_FAIL(alloc_and_construct_(allocator_, count, drop_table_args))) {
    LOG_WARN("fail to alloc and construct drop_table_args", KR(ret), K(count));
  } else if (OB_FAIL(alloc_and_construct_(allocator_, count, drop_table_results))) {
    LOG_WARN("fail to alloc and construct drop_table_results", KR(ret), K(count));
  } else if (OB_ISNULL(drop_tablegroup_result = OB_NEWx(ObParallelDDLRes, &allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc ObParallelDDLRes", KR(ret), K(sizeof(ObParallelDDLRes)));
  } else if (OB_ISNULL(drop_tablegroup_helper = OB_NEWx(ObDropTablegroupHelper, &allocator_,
      &schema_service_, tenant_id, param_->table_group_arg_, *drop_tablegroup_result, &trans))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc ObParallelDDLRes", KR(ret), K(sizeof(ObParallelDDLRes)));
  } else if (OB_FAIL(drop_tablegroup_helper->init(ddl_service_))) {
    LOG_WARN("fail to init drop tablegroup helper", KR(ret));
  } else if (OB_FAIL(drop_table_helpers.prepare_allocate(count))) {
    LOG_WARN("fail to prepare allocate drop_table_helpers", KR(ret), K(count));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(sizeof(ObDropTableHelper) * count)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc ObDropTableHelper", KR(ret), K(sizeof(ObDropTableHelper)), K(count));
  } else if (OB_FAIL(init_drop_table_args_(drop_table_args))) {
    LOG_WARN("fail to init drop table args", KR(ret));
  } else {
    for (int64_t i = 0; i < count && OB_SUCC(ret); i++) {
      ObDropTableArg &drop_table_arg = *drop_table_args.at(i);
      ObDropTableRes &drop_table_res = *drop_table_results.at(i);
      drop_table_helpers.at(i) = new (buf + sizeof(ObDropTableHelper) * i)
          ObDropTableHelper(&schema_service_, tenant_id, drop_table_arg, drop_table_res, &trans);
      if (OB_FAIL(drop_table_helpers.at(i)->init(ddl_service_))) {
        LOG_WARN("fail to init create table helper", KR(ret));
      }
    }
  }

  return ret;
}

void ObDropHTableHandler::clear_helpers_(ObDropTablegroupHelper *drop_tablegroup_helper,
                                         ObParallelDDLRes *drop_tablegroup_result,
                                         ObIArray<ObDropTableHelper*> &drop_table_helpers,
                                         ObIArray<ObDropTableArg*> &drop_table_args,
                                         ObIArray<ObDropTableRes*> &drop_table_results)
{
  // tablegroup
  OB_DELETEx(ObDropTablegroupHelper, &allocator_, drop_tablegroup_helper);
  OB_DELETEx(ObParallelDDLRes, &allocator_, drop_tablegroup_result);

  // tables
  for (int i = 0; i < drop_table_helpers.count(); i++) {
    ObDropTableHelper *helper = drop_table_helpers.at(i);
    OB_DELETEx(ObDropTableHelper, &allocator_, helper);
  }
  for (int i = 0; i < drop_table_args.count(); i++) {
    ObDropTableArg *arg = drop_table_args.at(i);
    OB_DELETEx(ObDropTableArg, &allocator_, arg);
  }
  for (int i = 0; i < drop_table_results.count(); i++) {
    ObDropTableRes *res = drop_table_results.at(i);
    OB_DELETEx(ObDropTableRes, &allocator_, res);
  }
}

int ObDropHTableHandler::init()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    param_ = static_cast<ObDropHTableDDLParam*>(arg_.ddl_param_);
    if (OB_ISNULL(param_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("param_ is null", KR(ret));
    } else {
      ObLatestSchemaGuard latest_schema_guard(&schema_service_, arg_.exec_tenant_id_);
      const uint64_t tenant_id = arg_.exec_tenant_id_;
      const ObString tablegroup_name = param_->table_group_arg_.tablegroup_name_;
      uint64_t tablegroup_id = OB_INVALID_ID;
      const ObDatabaseSchema *database_schema = nullptr;
      ObSEArray<const ObTableSchema*, 4> drop_schemas;
      if (OB_FAIL(latest_schema_guard.get_tablegroup_id(tablegroup_name, tablegroup_id))) {
        LOG_WARN("failed to get table group id", KR(ret), K(tenant_id), K(tablegroup_name));
      } else if (tablegroup_id == OB_INVALID_ID) {
        ret = OB_KV_HBASE_TABLE_NOT_FOUND;
        LOG_WARN("the table group for hbase table not found", KR(ret), K(tablegroup_name));
        LOG_USER_ERROR(OB_KV_HBASE_TABLE_NOT_FOUND, tablegroup_name.length(), tablegroup_name.ptr());
      } else if (OB_FAIL(latest_schema_guard.get_table_schemas_in_tablegroup(tablegroup_id, drop_schemas))) {
        LOG_WARN("failed to get table schemas in tablegroup", KR(ret), K(tenant_id), K(tablegroup_id));
      } else if (drop_schemas.empty() || OB_ISNULL(drop_schemas.at(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("empty schema or null schema", KR(ret), K(drop_schemas.empty()));
      } else if (OB_FAIL(latest_schema_guard.get_database_schema(drop_schemas.at(0)->get_database_id(),
          database_schema))) {
        LOG_WARN("failed to get database schema", KR(ret), K(tenant_id), KPC_(param));
      } else if (OB_FAIL(ob_write_string(allocator_, database_schema->get_database_name(), database_name_))) {
        LOG_WARN("failed to copy database name", KR(ret), K(database_schema->get_database_name()));
      } else if (OB_FAIL(ob_write_string(allocator_, tablegroup_name, tablegroup_name_))) {
        LOG_WARN("failed to copy tablegroup name", KR(ret), K(tablegroup_name_));
      } else {
        // data tables
        for (int i = 0; i < drop_schemas.count() && OB_SUCC(ret); i++) {
          const ObTableSchema *schema = drop_schemas.at(i);
          ObString table_name;
          if (OB_ISNULL(schema)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("schema is null", KR(ret), K(i));
          } else if (OB_FAIL(ObTTLUtil::check_htable_ddl_supported(*schema, true/*by_admin*/, get_ddl_type(), tablegroup_name))) {
            LOG_WARN("failed to check htable ddl supoprted", KR(ret));
          } else if (OB_FAIL(ob_write_string(allocator_, schema->get_table_name_str(), table_name))) {
            LOG_WARN("failed to copy table name", KR(ret), KPC(schema));
          } else if (OB_FAIL(table_names_.push_back(table_name))) {
            LOG_WARN("failed to push back table name", KR(ret), KPC(schema));
          } else if (OB_FAIL(table_ids_.push_back(schema->get_table_id()))) {
            LOG_WARN("failed to push back table id", KR(ret), KPC(schema));
          } else {
            schema_version_cnt_++;
            if (schema->has_tablet()) {
              schema_version_cnt_++;
            }
          }
        }
        // 1503 + tablegroup
        schema_version_cnt_ += 2;
      }
    }
    is_inited_ = true;
  }

  return ret;
}

// 1. start trans
// 2. lock tablegroup and table by name
// 3. alloc schema versions
// 4. drop tables
// 5. drop tablegroup
// 6. end trans
int ObDropHTableHandler::handle()
{
  int ret = OB_SUCCESS;
  ObDDLSQLTransaction trans(&schema_service_, false/*need_end_signal*/, false/*enable_query_stash*/, true/*enable_ddl_parallel*/);
  ObMySQLProxy *sql_proxy = &(ddl_service_.get_sql_proxy());
  const uint64_t tenant_id = arg_.exec_tenant_id_;
  bool with_snapshot = false;
  int64_t fake_schema_version = 1000;
  ObDropTablegroupHelper *drop_tablegroup_helper = nullptr;
  ObParallelDDLRes *drop_tablegroup_result = nullptr;
  ObSEArray<ObDropTableHelper*, 4> drop_table_helpers;
  ObSEArray<ObDropTableArg*, 4> drop_table_args;
  ObSEArray<ObDropTableRes*, 4> drop_table_results;
  drop_table_helpers.set_attr(ObMemAttr(OB_SERVER_TENANT_ID, "DrpTmpTbHelps"));
  drop_table_args.set_attr(ObMemAttr(OB_SERVER_TENANT_ID, "DrpTmpTbArg"));
  drop_table_results.set_attr(ObMemAttr(OB_SERVER_TENANT_ID, "DrpTmpTbRes"));
  ObHTableLockHelper lock_helper(&schema_service_, tenant_id, &trans);

  if (OB_ISNULL(sql_proxy) || OB_ISNULL(param_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy or param_ is null", KR(ret), KP_(param), KP(sql_proxy));
  } else if (OB_FAIL(trans.start(sql_proxy, tenant_id, fake_schema_version, with_snapshot))) {
    LOG_WARN("fail to start trans", KR(ret), K(tenant_id), K(fake_schema_version), K(with_snapshot));
  } else if (OB_FAIL(lock_helper.init(ddl_service_))) { // lock tablegroup and tables in advance
    LOG_WARN("fail to init lock helper", KR(ret), K(tenant_id));
  } else if (OB_FAIL(lock_helper.lock_objects(tablegroup_name_, database_name_, table_names_, true /* need_lock_id */))) {
    LOG_WARN("fail to lock objects", KR(ret), K(tenant_id), K_(tablegroup_name), K_(database_name), K_(table_names));
  } else if (OB_FAIL(gen_task_id_and_schema_versions_(schema_version_cnt_, task_id_))) {
    LOG_WARN("fail to gen task id and schema versions", KR(ret), K_(schema_version_cnt));
  } else if (OB_FAIL(gen_drop_helpers_(drop_tablegroup_helper, drop_tablegroup_result,
      drop_table_helpers, drop_table_args,drop_table_results, trans))) {
    LOG_WARN("fail to gen helpers", KR(ret));
  } else if (OB_ISNULL(drop_tablegroup_helper)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("drop_tablegroup_helper is null", KR(ret));
  } else if (OB_ISNULL(drop_tablegroup_result)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("drop_tablegroup_result is null", KR(ret));
  } else {
    // drop tables
    for (int i = 0; i < drop_table_helpers.count() && OB_SUCC(ret); i++) {
      ObDropTableHelper *helper = drop_table_helpers.at(i);
      #ifdef ERRSIM
      ret = OB_E(common::EventTable::EN_DELETE_HTABLE_SKIP_CF_ERR) OB_SUCCESS;
      if (OB_FAIL(ret)) {
        LOG_WARN("[ERRSIM] skip one cf table", KR(ret));
        ret = OB_SUCCESS;
        continue;
      }
      #endif
      if (OB_ISNULL(helper)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ObDropTableHelper is null", KR(ret), K(i));
      } else if (OB_FAIL(helper->execute())) {
        LOG_WARN("fail to execute drop table", KR(ret), K(i));
      }
      #ifdef ERRSIM
      if (OB_SUCC(ret)) {
        ret = OB_E(common::EventTable::EN_DELETE_HTABLE_CF_FINISH_ERR) OB_SUCCESS;
        if (OB_FAIL(ret)) {
          LOG_WARN("[ERRSIM] fail to drop table", KR(ret));
        }
      }
      #endif
    }

    // drop tablegroup
    if (FAILEDx(drop_tablegroup_helper->execute())) {
      LOG_WARN("fail to execute drop tablegroup", KR(ret), K(tenant_id));
    }
  }

  bool need_clean_failed = false;
  if (OB_FAIL(wait_and_end_ddl_trans_(ret, task_id_, trans, need_clean_failed))) {
    LOG_WARN("fail to wait and end ddl trans", KR(ret));
    if (need_clean_failed) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(clean_on_fail_commit_(drop_tablegroup_helper, drop_table_helpers))) {
        LOG_WARN("fail to clean on fail commit", KR(tmp_ret));
      }
    }
  }

  if (FAILEDx(construct_result_())) {
    LOG_WARN("fail to construct result", KR(ret));
  }

  clear_helpers_(drop_tablegroup_helper, drop_tablegroup_result, drop_table_helpers,
      drop_table_args, drop_table_results);

  return ret;
}

int ObHTableControlHandler::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(kv_attribute_helper_.init(ddl_service_))) {
    LOG_WARN("fail to init kv_attribute helper", KR(ret));
  }
  return ret;
}

int ObHTableControlHandler::handle()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(kv_attribute_helper_.execute())) {
    LOG_WARN("fail to execute set kv_attribute", KR(ret));
  }
  return ret;
}

int ObHTableDDLHandlerGuard::get_handler(ObDDLService &ddl_service,
                                         ObMultiVersionSchemaService &schema_service,
                                         const ObHTableDDLArg &arg,
                                         ObHTableDDLRes &res,
                                         ObHTableDDLHandler *&handler)
{
  int ret = OB_SUCCESS;
  handler = nullptr;
  switch (arg.ddl_type_) {
    case ObHTableDDLType::CREATE_TABLE: {
      if (OB_ISNULL(handler_ = OB_NEWx(ObCreateHTableHandler, &allocator_, ddl_service, schema_service, arg, res))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc ObCreateHTableHandler", KR(ret));
      }
      break;
    }
    case ObHTableDDLType::DROP_TABLE: {
      if (OB_ISNULL(handler_ = OB_NEWx(ObDropHTableHandler, &allocator_, ddl_service, schema_service, arg, res))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc ObDropHTableHandler", KR(ret));
      }
      break;
    }
    case ObHTableDDLType::DISABLE_TABLE:
    case ObHTableDDLType::ENABLE_TABLE: {
      if (OB_ISNULL(handler_ = OB_NEWx(ObHTableControlHandler, &allocator_, ddl_service, schema_service, arg, res))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc ObHTableControlHandler", KR(ret));
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid htable ddl type", KR(ret), K(arg.ddl_type_));
    }
  }

  if (OB_SUCC(ret)) {
    handler = handler_;
  }

  return ret;
}

ObHTableLockHelper::ObHTableLockHelper(share::schema::ObMultiVersionSchemaService *schema_service,
                                       const uint64_t tenant_id,
                                       ObDDLSQLTransaction *external_trans)
  : ObDDLHelper(schema_service, tenant_id, "[htable lock helper]", external_trans),
    tablegroup_id_(OB_INVALID_ID),
    database_id_(OB_INVALID_ID),
    table_ids_()
{}

int ObHTableLockHelper::lock_objects(const ObString &tablegroup_name,
                                     const ObString &database_name,
                                     const ObIArray<ObString> &table_names,
                                     bool need_lock_id)
{
  int ret = OB_SUCCESS;
  DEBUG_SYNC(BEFORE_PARALLEL_DDL_LOCK);
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(lock_database_by_name_(database_name))) {
    LOG_WARN("fail to lock database by name", KR(ret));
  } else if (OB_FAIL(lock_tablegroup_and_tables_by_name_(tablegroup_name, database_name, table_names))) {
    LOG_WARN("fail to lock tables by name", KR(ret), K(table_names));
  } else if (need_lock_id) {
    if (OB_FAIL(check_htable_exist(tablegroup_name, database_name, table_names))) {
      LOG_WARN("fail to check htable exist", KR(ret), K(tablegroup_name), K(database_name), K(table_names));
    } else if (OB_FAIL(lock_htable_objects_by_id_())) {
      LOG_WARN("fail to lock htable objects by id", KR(ret));
    }
  } else if (!need_lock_id && OB_FAIL(check_htable_not_exist(tablegroup_name, database_name, table_names))) {
    LOG_WARN("fail to check htable not exist", KR(ret), K(tablegroup_name), K(database_name), K(table_names));
  }
  DEBUG_SYNC(AFTER_PARALLEL_DDL_LOCK);
  RS_TRACE(lock_objects);

  return ret;
}

// check htable if is exist or not,
// if is exist, return OB_SUCCESS. Otherwise, return related error code
int ObHTableLockHelper::check_htable_exist(const ObString &tablegroup_name,
                                           const ObString &database_name,
                                           const ObIArray<ObString> &table_names)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(latest_schema_guard_.get_tablegroup_id(tablegroup_name, tablegroup_id_))) {
    LOG_WARN("fail to get table group id", KR(ret), K(tablegroup_name));
  } else if (OB_INVALID_ID == tablegroup_id_) {
    ret = OB_KV_HBASE_TABLE_NOT_FOUND;
    LOG_WARN("the table group for hbase table not found", KR(ret), K(tablegroup_name));
    LOG_USER_ERROR(OB_KV_HBASE_TABLE_NOT_FOUND, tablegroup_name.length(), tablegroup_name.ptr());
  } else if (OB_FAIL(latest_schema_guard_.get_database_id(database_name, database_id_))) {
    LOG_WARN("fail to get database id", KR(ret), K(database_name));
  } else if (OB_INVALID_ID == database_id_) {
    ret = OB_ERR_BAD_DATABASE;
    LOG_WARN("unknown database name", KR(ret), K(database_name));
  } else {
    for (int64_t i = 0; i < table_names.count() && OB_SUCC(ret); i++) {
      const ObString &table_name = table_names.at(i);
      uint64_t table_id = OB_INVALID_ID;
      ObTableType table_type = ObTableType::MAX_TABLE_TYPE;
      int64_t schema_version = OB_INVALID_VERSION;
      if (OB_FAIL(latest_schema_guard_.get_table_id(database_id_, 0 /* sess_id */, table_name,
            table_id, table_type, schema_version))) {
        LOG_WARN("fail to get table id", KR(ret), K(database_id_), K(table_name));
      } else if (OB_INVALID_ID == table_id) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("table is not exist", KR(ret), K(table_name));
      } else if (OB_FAIL(table_ids_.push_back(table_id))) {
        LOG_WARN("fail to push back table_id", KR(ret), K(table_id));
      }
    }
  }
  return ret;
}

// check htable if is exist or not,
// if is not exist, return OB_SUCCESS. Otherwise, return related error code
int ObHTableLockHelper::check_htable_not_exist(const ObString &tablegroup_name,
                                               const ObString &database_name,
                                               const ObIArray<ObString> &table_names)
{
  int ret = OB_SUCCESS;
  uint64_t tablegroup_id = OB_INVALID_ID;
  uint64_t database_id = OB_INVALID_ID;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(latest_schema_guard_.get_tablegroup_id(tablegroup_name, tablegroup_id))) {
    LOG_WARN("fail to get table group id", KR(ret), K(tablegroup_name));
  } else if (OB_INVALID_ID != tablegroup_id) {
    ret = OB_KV_HBASE_TABLE_EXISTS;
    LOG_WARN("the table group for hbase table has already existed", KR(ret), K(tablegroup_name));
    LOG_USER_ERROR(OB_KV_HBASE_TABLE_EXISTS, tablegroup_name.length(), tablegroup_name.ptr());
  } else if (OB_FAIL(latest_schema_guard_.get_database_id(database_name, database_id))) {
    LOG_WARN("fail to get database id", KR(ret), K(database_name));
  } else if (OB_INVALID_ID == database_id) {
    ret = OB_ERR_BAD_DATABASE;
    LOG_WARN("unknown database name", KR(ret), K(database_name));
  } else {
    for (int64_t i = 0; i < table_names.count() && OB_SUCC(ret); i++) {
      const ObString &table_name = table_names.at(i);
      uint64_t table_id = OB_INVALID_ID;
      uint64_t parent_table_id = OB_INVALID_ID;
      ObTableType table_type = ObTableType::MAX_TABLE_TYPE;
      int64_t schema_version = OB_INVALID_VERSION;
      if (OB_FAIL(latest_schema_guard_.get_table_id(database_id, 0 /* sess_id */, table_name,
            table_id, table_type, schema_version))) {
        LOG_WARN("fail to get table id", KR(ret), K(database_id), K(table_name));
      } else if (OB_INVALID_ID != table_id) {
        ret = OB_ERR_TABLE_EXIST;
        LOG_WARN("table has already existed", KR(ret), K(table_name));
      } else if (OB_FAIL(latest_schema_guard_.get_mock_fk_parent_table_id(database_id, table_name, parent_table_id ))) {
        LOG_WARN("fail to get mock fk parent table id", KR(ret), K(database_id), K(table_name));
      } else if (parent_table_id != OB_INVALID_ID) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("create htable with mock fk parent table exists not supported", KR(ret), K(database_name), K(table_name));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "create htable with mock fk parent table exists");
      }
    }
  }
  return ret;
}

int ObHTableLockHelper::lock_database_by_name_(const ObString &database_name)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(add_lock_object_by_database_name_(database_name, transaction::tablelock::SHARE))) {
    LOG_WARN("fail to add lock object by database name", KR(ret), K(database_name));
  } else if (OB_FAIL(lock_databases_by_name_())) {
    LOG_WARN("fail to lock databases by name", KR(ret), K_(tenant_id));
  }
  return ret;
}

int ObHTableLockHelper::lock_tablegroup_and_tables_by_name_(const ObString &tablegroup_name,
                                                            const ObString &database_name,
                                                            const ObIArray<ObString> &table_names)
{
  int ret = OB_SUCCESS;
  DEBUG_SYNC(BEFORE_PARALLEL_DDL_LOCK);
  if (OB_FAIL(add_lock_object_by_tablegroup_name_(tablegroup_name, transaction::tablelock::EXCLUSIVE))) { // lock tablegroup name
    LOG_WARN("fail to add tablegroup lock by obj name", KR(ret), K_(tenant_id));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < table_names.count(); i++) {
    const ObString &table_name = table_names.at(i);
    if (OB_FAIL(add_lock_object_by_name_(database_name, table_name,
          share::schema::TABLE_SCHEMA, transaction::tablelock::EXCLUSIVE))) {
      LOG_WARN("fail to lock object by table name", KR(ret), K(database_name), K(table_name));
    }
  }

  if (FAILEDx(lock_existed_objects_by_name_())) {
    LOG_WARN("fail to lock existed obj by name", KR(ret));
  }
  RS_TRACE(lock_objects);
  return ret;
}

int ObHTableLockHelper::lock_htable_objects_by_id_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(add_lock_object_by_id_(database_id_,
             share::schema::DATABASE_SCHEMA, transaction::tablelock::SHARE))) {
    LOG_WARN("fail to add lock database id", KR(ret), K_(database_id));
  } else if (OB_FAIL(add_lock_object_by_id_(tablegroup_id_,
      share::schema::TABLEGROUP_SCHEMA, transaction::tablelock::EXCLUSIVE))) {
    LOG_WARN("fail to add lock tablegroup id", KR(ret), K_(tablegroup_id));
  }
  for (int64_t i = 0; i < table_ids_.count() && OB_SUCC(ret); i++) {
    uint64_t table_id = table_ids_.at(i);
    if (OB_FAIL(add_lock_object_by_id_(table_id, share::schema::TABLE_SCHEMA, transaction::tablelock::EXCLUSIVE))) {
      LOG_WARN("fail to add lock table id", KR(ret), K_(tenant_id));
    }
  }

  if (FAILEDx(lock_existed_objects_by_id_())) {
    LOG_WARN("fail to lock objects by id", KR(ret));
  } else {  // double check for table_ids_ is consistent
    // we may need to check database_id is consistent in future, for the scene:
    // table is recover from recycle bin and its database is chanage, but this operation is not supported in parallel ddl
    // and we can ignore this check for now
    ObArray<uint64_t> ori_table_ids;
    ObArray<uint64_t> latest_table_ids;
    ObArray<ObString> latest_table_names;  // not used
    if (OB_FAIL(ori_table_ids.assign(table_ids_))) {
      LOG_WARN("fail to assign origin table ids", KR(ret));
    } else if (OB_FAIL(latest_schema_guard_.get_table_id_and_table_name_in_tablegroup(tablegroup_id_,
        latest_table_names, latest_table_ids))) {
      LOG_WARN("failed to get table schemas in table group", KR(ret), K_(tablegroup_id));
    } else if (ori_table_ids.count() != latest_table_ids.count()) {
      ret = OB_ERR_PARALLEL_DDL_CONFLICT;
      LOG_WARN("table_id count not consistent", KR(ret), K(ori_table_ids.count()), K(latest_table_ids.count()));
    } else {
      lib::ob_sort(ori_table_ids.begin(), ori_table_ids.end());
      lib::ob_sort(latest_table_ids.begin(), latest_table_ids.end());
      for (int64_t i = 0; i < ori_table_ids.count() && OB_SUCC(ret); i++) {
        if (ori_table_ids.at(i) != latest_table_ids.at(i)) {
          ret = OB_ERR_PARALLEL_DDL_CONFLICT;
          LOG_WARN("table_id in double check is not consistent", KR(ret),
                   K(ori_table_ids.at(i)), K(latest_table_ids.at(i)));
        }
      }
    }
  }
  return ret;
}
