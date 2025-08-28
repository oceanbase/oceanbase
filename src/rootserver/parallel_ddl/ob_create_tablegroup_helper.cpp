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
#include "rootserver/parallel_ddl/ob_create_tablegroup_helper.h"
#include "share/schema/ob_tablegroup_sql_service.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::rootserver;

ObCreateTablegroupHelper::ObCreateTablegroupHelper(
  share::schema::ObMultiVersionSchemaService *schema_service,
  const uint64_t tenant_id,
  const obrpc::ObCreateTablegroupArg &arg,
  obrpc::ObCreateTableGroupRes &res,
  ObDDLSQLTransaction *external_trans)
  : ObDDLHelper(schema_service, tenant_id, "[paralle create tablegroup]", external_trans),
  arg_(arg),
  res_(res),
  tablegroup_schema_(nullptr)
{}

ObCreateTablegroupHelper::~ObCreateTablegroupHelper()
{
}

int ObCreateTablegroupHelper::check_inner_stat_()
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

int ObCreateTablegroupHelper::init_()
{
  int ret = OB_SUCCESS;
  uint64_t compat_version = 0;
  // TODO: ObCreateTablegroupHelper is only used by HBase DDL currently, and if_not_exist_ can only be false
  if (arg_.if_not_exist_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id_, compat_version))) {
    LOG_WARN("fail to get data version", KR(ret), K_(tenant_id));
  } else if (compat_version < DATA_VERSION_4_3_5_3) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("parallel create tablegroup below 4.3.5.3 is not supported", KR(ret), K(compat_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "parallel create tablegroup below 4.3.5.3");
  }
  return ret;
}

int ObCreateTablegroupHelper::lock_objects_()
{
  int ret = OB_SUCCESS;
  const ObString &tablegroup_name = arg_.tablegroup_schema_.get_tablegroup_name();
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(add_lock_object_by_tablegroup_name_(tablegroup_name, transaction::tablelock::EXCLUSIVE))) { // lock tablegroup name
    LOG_WARN("fail to add tablegroup lock by obj name", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(lock_existed_objects_by_name_())) {
    LOG_WARN("fail to lock objects by name", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(check_tablegroup_name_())) { // check tablegroup name
    LOG_WARN("fail to check tablegroup legitimacy", KR(ret), K_(tenant_id));
  }
  return ret;
}

int ObCreateTablegroupHelper::check_tablegroup_name_()
{
  int ret = OB_SUCCESS;
  const ObString &tablegroup_name = arg_.tablegroup_schema_.get_tablegroup_name();
  uint64_t tablegroup_id = OB_INVALID_ID;
  if (OB_FAIL(latest_schema_guard_.get_tablegroup_id(tablegroup_name, tablegroup_id))) {
    LOG_WARN("fail to get tablegroup id", KR(ret), K(tablegroup_name));
  } else if (tablegroup_id != OB_INVALID_ID) {
    // Raise error here to skip the following steps,
    // ret will be overwrite if if_not_exist_ is true before rpc returns.
    ret = OB_TABLEGROUP_EXIST;
    LOG_INFO("create tablegroup while tablegroup exists", KR(ret), K(tablegroup_name), K(tablegroup_id));
  }
  return ret;
}

int ObCreateTablegroupHelper::generate_schemas_()
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service_impl = nullptr;
  uint64_t new_tablegroup_id = OB_INVALID_ID;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(ObSchemaUtils::alloc_schema(allocator_, arg_.tablegroup_schema_, tablegroup_schema_))) {
    LOG_WARN("fail to alloc schema", KR(ret));
  } else if (OB_ISNULL(schema_service_impl = schema_service_->get_schema_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service impl is null", KR(ret));
  } else if (OB_FAIL(schema_service_impl->fetch_new_tablegroup_id(tenant_id_, new_tablegroup_id))) {
    LOG_WARN("fail to gen new tablegroup id", KR(ret), K_(tenant_id));
  } else {
    tablegroup_schema_->set_tablegroup_id(new_tablegroup_id);
  }
  return ret;
}

int ObCreateTablegroupHelper::calc_schema_version_cnt_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else {
    // 1. tablegroup
    schema_version_cnt_ = 1;
    // 2. 1503
    schema_version_cnt_++;
  }
  return ret;
}

int ObCreateTablegroupHelper::operate_schemas_()
{
  int ret = OB_SUCCESS;
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service_impl = nullptr;
  const ObString *ddl_stmt_str = &arg_.ddl_stmt_str_;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_ISNULL(tablegroup_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null tablegroup schema", KR(ret));
  } else if (OB_ISNULL(schema_service_impl = schema_service_->get_schema_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service impl is null", KR(ret));
  } else if (OB_FAIL(schema_service_->gen_new_schema_version(tenant_id_, new_schema_version))) {
    LOG_WARN("fail to gen new schema version", KR(ret), K_(tenant_id));
  } else {
    tablegroup_schema_->set_schema_version(new_schema_version);
    if (OB_FAIL(schema_service_impl->get_tablegroup_sql_service().insert_tablegroup(
                                                                  *tablegroup_schema_,
                                                                  get_trans_(),
                                                                  ddl_stmt_str))) {
      LOG_WARN("failed to insert tablegroup", KR(ret), K(ddl_stmt_str), K_(tablegroup_schema));
    }
  }
  return ret;
}

int ObCreateTablegroupHelper::construct_and_adjust_result_(int &return_ret)
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

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(tablegroup_schema_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablegroup schema is NULL", KR(ret));
    } else {
      res_.tablegroup_id_ = tablegroup_schema_->get_tablegroup_id();
    }
  } else if (OB_TABLEGROUP_EXIST == ret) {
    const ObTablegroupSchema &tablegroup = arg_.tablegroup_schema_;
    //create tablegroup xx if not exist (...)
    if (arg_.if_not_exist_) {
      ret = OB_SUCCESS;
      res_.tablegroup_id_ = tablegroup.get_tablegroup_id();
      LOG_USER_NOTE(OB_TABLEGROUP_EXIST);
      LOG_INFO("tablegroup already exists, not need to create", "tenant_id",
        tablegroup.get_tenant_id(), "tablegroup_name",
        tablegroup.get_tablegroup_name_str());
    } else {
      LOG_USER_ERROR(OB_TABLEGROUP_EXIST);
      LOG_WARN("tablegroup already exists", KR(ret), "tenant_id", tablegroup.get_tenant_id(),
        "tablegroup_name", tablegroup.get_tablegroup_name_str());
    }
  }
  
  return ret;
}

int ObCreateTablegroupHelper::operation_before_commit_()
{
  int ret = OB_SUCCESS;
  // do nothing
  return ret;
}

int ObCreateTablegroupHelper::clean_on_fail_commit_()
{
  int ret = OB_SUCCESS;
  // do nothing
  return ret;
}
