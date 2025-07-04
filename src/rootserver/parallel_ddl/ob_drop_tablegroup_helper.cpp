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
#include "rootserver/parallel_ddl/ob_drop_tablegroup_helper.h"
#include "share/schema/ob_tablegroup_sql_service.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::rootserver;

ObDropTablegroupHelper::ObDropTablegroupHelper(
  share::schema::ObMultiVersionSchemaService *schema_service,
  const uint64_t tenant_id,
  const obrpc::ObDropTablegroupArg &arg,
  obrpc::ObParallelDDLRes &res,
  ObDDLSQLTransaction *external_trans)
  : ObDDLHelper(schema_service, tenant_id, "[paralle drop tablegroup]", external_trans),
  arg_(arg),
  res_(res),
  tablegroup_schema_(nullptr),
  tablegroup_id_(OB_INVALID_ID)
{
}

ObDropTablegroupHelper::~ObDropTablegroupHelper()
{
}

int ObDropTablegroupHelper::init_()
{
  int ret = OB_SUCCESS;
  // TODO: ObDropTablegroupHelper is only used by HBase DDL currently, and if_exist_ can only be false
  if (arg_.if_exist_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  }
  return ret;
}

int ObDropTablegroupHelper::check_inner_stat_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDDLHelper::check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (!arg_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("arg is invalid", KR(ret), K(arg_));
  }
  return ret;
}

int ObDropTablegroupHelper::lock_objects_()
{
  int ret = OB_SUCCESS;
  const ObString &tablegroup_name = arg_.tablegroup_name_;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(add_lock_object_by_tablegroup_name_(tablegroup_name, transaction::tablelock::EXCLUSIVE))) { // lock tablegroup name
    LOG_WARN("fail to lock tablegroup by obj name", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(lock_existed_objects_by_name_())) {
    LOG_WARN("fail to lock objects by name", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(lock_tablegroup_by_obj_id_())) { // check tablegroup name
    LOG_WARN("fail to lock tablegroup by obj id", KR(ret), K_(tenant_id));
  }

  return ret;
}

int ObDropTablegroupHelper::lock_tablegroup_by_obj_id_()
{
  int ret = OB_SUCCESS;
  const ObString &tablegroup_name = arg_.tablegroup_name_;
  if (OB_FAIL(latest_schema_guard_.get_tablegroup_id(tablegroup_name, tablegroup_id_))) {
    LOG_WARN("fail to get database schema", KR(ret), K_(tenant_id), K(tablegroup_name));
  } else if (tablegroup_id_ == OB_INVALID_ID) {
    ret = OB_TABLEGROUP_NOT_EXIST;
    LOG_INFO("tablegroup not exists", K(ret), K(tablegroup_name));
  } else if (OB_FAIL(add_lock_object_by_id_(tablegroup_id_, share::schema::TABLEGROUP_SCHEMA,
                     transaction::tablelock::EXCLUSIVE))) {
    LOG_WARN("failed to add lock object by tablegroup id", K(ret), K_(tablegroup_id));
  } else if (OB_FAIL(lock_existed_objects_by_id_())) {
    LOG_WARN("fail to lock objects by id", KR(ret));
  }

  return ret;
}


int ObDropTablegroupHelper::generate_schemas_()
{
  int ret = OB_SUCCESS;
  ObArray<const ObTableSchema *> tables;
  tables.set_attr(ObMemAttr(tenant_id_, "DropTGTblSches"));

  // When tablegroup is dropped, there must not be table in tablegroup
  if (tablegroup_id_ == OB_INVALID_ID) {
    ret = OB_TABLEGROUP_NOT_EXIST;
  } else if (OB_FAIL(latest_schema_guard_.get_table_schemas_in_tablegroup(tablegroup_id_, tables))) {
    LOG_WARN("failed to get table schemas in tablegroup", K(ret), K_(tenant_id), K_(tablegroup_id));
  } else if (tables.count() > 0) {
    ret = OB_TABLEGROUP_NOT_EMPTY;
    LOG_WARN("tables in tablegroup when drop tablegroup", K(ret), K_(tablegroup_id), K(tables.count()));
  }

  // check databases' default_tablegroup_id
  if (OB_SUCC(ret)) {
    bool exists = false;
    uint64_t database_id = OB_INVALID_ID;
    if (OB_FAIL(latest_schema_guard_.check_database_exists_in_tablegroup(
                tablegroup_id_, exists))) {
      LOG_WARN("failed to check whether database exists in table group",
                K(ret), K_(tablegroup_id));
    } else if (exists) {
      ret = OB_TABLEGROUP_NOT_EMPTY;
      LOG_WARN("tablegroup is database's default_tablegroup_id when drop tablegroup", K(ret), K_(tablegroup_id));
    }
  }

  // check tenants' default_tablegroup_id
  if (OB_SUCC(ret)) {
    const ObTenantSchema *tenant_schema = NULL;
    if (OB_FAIL(latest_schema_guard_.get_tenant_schema(tenant_id_, tenant_schema))) {
      LOG_WARN("failed to get tenant schema", K(ret), K_(tenant_id));
    } else if (OB_ISNULL(tenant_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant schema is null", K(ret), K_(tenant_id));
    } else if (tablegroup_id_ == tenant_schema->get_default_tablegroup_id()) {
      ret = OB_TABLEGROUP_NOT_EMPTY;
      LOG_WARN("tablegroup is database's default_tablegroup_id when drop tablegroup", K(ret), K_(tablegroup_id), K_(tenant_id));
    }

  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(latest_schema_guard_.get_tablegroup_schema(tablegroup_id_, tablegroup_schema_))) {
    LOG_WARN("failed to get tablegroup schema by tenant id", K(ret), K_(tablegroup_id));
    } else if (tablegroup_schema_ == NULL) {
      ret = OB_TABLEGROUP_NOT_EXIST;
      LOG_INFO("tablegroup not exists", K(ret), K_(tablegroup_id));
    }
  }

  return ret;
}

int ObDropTablegroupHelper::calc_schema_version_cnt_()
{
  int ret = OB_SUCCESS;
  schema_version_cnt_ = 0;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else {
    // 1. tablegroup
    schema_version_cnt_++;
    // 2. 1503
    schema_version_cnt_++;
  }
  return ret;
}

int ObDropTablegroupHelper::operate_schemas_()
{
  int ret = OB_SUCCESS;
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service_impl = nullptr;
  const ObString *ddl_stmt_str = &arg_.ddl_stmt_str_;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_ISNULL(tablegroup_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null tablegroup schema", K(ret));
  } else if (OB_ISNULL(schema_service_impl = schema_service_->get_schema_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service impl is null", KR(ret));
  } else if (OB_FAIL(schema_service_->gen_new_schema_version(tenant_id_, new_schema_version))) {
    LOG_WARN("fail to gen new schema version", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(schema_service_impl->get_tablegroup_sql_service().delete_tablegroup(
                                                                  *tablegroup_schema_,
                                                                  new_schema_version,
                                                                  get_trans_(),
                                                                  ddl_stmt_str))) {
    LOG_WARN("failed to insert tablegroup", KR(ret), K(new_schema_version), K(ddl_stmt_str), K_(tablegroup_schema));
  }
  return ret;
}

int ObDropTablegroupHelper::construct_and_adjust_result_(int &return_ret)
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
  }
  if (OB_TABLEGROUP_NOT_EXIST == ret) {
    if (arg_.if_exist_) {
      ret = OB_SUCCESS;
      LOG_USER_NOTE(OB_TABLEGROUP_NOT_EXIST);
      LOG_INFO("tablegroup not exist, no need to delete it", KR(ret), K_(tenant_id),
               "tablegroup_name", arg_.tablegroup_name_);
    } else {
      LOG_WARN("tablegroup not exist, can't delete it", KR(ret), K_(tenant_id),
               "tablegroup_name", arg_.tablegroup_name_);
      LOG_USER_ERROR(OB_TABLEGROUP_NOT_EXIST);
    }
  } else if (OB_TABLEGROUP_NOT_EMPTY == ret) {
    LOG_WARN("tablegroup not empty, can't delete it", KR(ret), K_(tenant_id),
              "tablegroup_name", arg_.tablegroup_name_);
    LOG_USER_ERROR(OB_TABLEGROUP_NOT_EMPTY);
  }
  return ret;
}

int ObDropTablegroupHelper::operation_before_commit_()
{
  int ret = OB_SUCCESS;
  // do nothing
  return ret;
}

int ObDropTablegroupHelper::clean_on_fail_commit_()
{
  int ret = OB_SUCCESS;
  // do nothing
  return ret;
}
