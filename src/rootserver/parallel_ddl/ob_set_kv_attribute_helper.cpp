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
#include "rootserver/parallel_ddl/ob_set_kv_attribute_helper.h"
#include "share/ob_rpc_struct.h"
#include "share/schema/ob_table_sql_service.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "rootserver/ob_snapshot_info_manager.h"
#include "storage/ddl/ob_ddl_lock.h"
#include "share/ob_debug_sync.h"
#include "share/table/ob_ttl_util.h"

using namespace oceanbase::lib;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::rootserver;

const char* ObSetKvAttributeHelper::ALTER_KV_ATTRIBUTE_FORMAT_STR = "ALTER TABLE %.*s KV_ATTRIBUTES=\'%.*s\'";

ObSetKvAttributeHelper::ObSetKvAttributeHelper(
  share::schema::ObMultiVersionSchemaService *schema_service,
  const obrpc::ObHTableDDLArg &arg,
  obrpc::ObParallelDDLRes &res)
  : ObDDLHelper(schema_service, arg.exec_tenant_id_, "[paralle set kv_attribute]"),
  arg_(arg),
  res_(res),
  database_id_(OB_INVALID_ID),
  tablegroup_id_(OB_INVALID_ID)
{
  table_names_.set_attr(ObMemAttr(arg.exec_tenant_id_, "SetKvAttTblNam"));
  table_ids_.set_attr(ObMemAttr(arg.exec_tenant_id_, "SetKvAttTblId"));
  origin_table_schemas_.set_attr(ObMemAttr(arg.exec_tenant_id_, "SetKvAttOriSch"));
  new_table_schemas_.set_attr(ObMemAttr(arg.exec_tenant_id_, "SetKvAttNewSch"));
  ddl_stmt_strs_.set_attr(ObMemAttr(arg.exec_tenant_id_, "SetKvAttDDLStr"));
}

ObSetKvAttributeHelper::~ObSetKvAttributeHelper()
{
  table_names_.reset();
  table_ids_.reset();
  origin_table_schemas_.reset();
  new_table_schemas_.reset();
  ddl_stmt_strs_.reset();
}

int ObSetKvAttributeHelper::check_inner_stat_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDDLHelper::check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (!arg_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("arg is invalid", KR(ret), K(arg_));
  } else if (OB_ISNULL(arg_.ddl_param_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ddl_param is nullptr", KR(ret), K(arg_.ddl_param_));
  }
  return ret;
}

int ObSetKvAttributeHelper::init_()
{
  int ret = OB_SUCCESS;
  return ret;
}

int ObSetKvAttributeHelper::lock_objects_()
{
  int ret = OB_SUCCESS;
  DEBUG_SYNC(BEFORE_PARALLEL_DDL_LOCK);
  const ObDatabaseSchema *database_schema = nullptr;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(lock_databases_by_obj_name_())) { // lock database name
    LOG_WARN("fail to lock databases by obj name", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(check_database_legitimacy_())) { // check database legitimacy
    LOG_WARN("fail to check database legitimacy", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(lock_tablegroup_by_name_())) { // lock tablegroup name and id
    LOG_WARN("fail to lock tablegroup by name", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(lock_objects_by_name_())) { // lock object name
    LOG_WARN("fail to lock objects by name", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(lock_objects_by_id_())) { // lock objects by id
    LOG_WARN("fail to lock objects by id" , KR(ret), K_(tenant_id));
  }
  DEBUG_SYNC(AFTER_PARALLEL_DDL_LOCK);
  RS_TRACE(lock_objects);
  if (FAILEDx(lock_for_common_ddl_())) { // online ddl lock & table lock
    LOG_WARN("fail to lock for common ddl", KR(ret));
  }
  for (int64_t i = 0; i < origin_table_schemas_.count() && OB_SUCC(ret); i++) {
    const ObTableSchema *orig_table_schema = origin_table_schemas_.at(i);
    if (OB_ISNULL(orig_table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("orig_table_schema is null", KR(ret));
    } else if (OB_UNLIKELY(database_id_ != orig_table_schema->get_database_id())) {
      ret = OB_ERR_PARALLEL_DDL_CONFLICT;
      LOG_WARN("database_id_ is not equal to table schema's databse_id",
              KR(ret), K_(database_id), K(orig_table_schema->get_database_id()));
    } else if (OB_FAIL(schema_guard_wrapper_.get_database_schema(database_id_, database_schema))) {
      LOG_WARN("fail to get database schema", KR(ret), K_(tenant_id), K_(database_id));
    } else if (OB_ISNULL(database_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("databse_schema is null", KR(ret));
    } else if (OB_UNLIKELY(database_schema->get_database_name_str() != get_params_().database_name_)) {
      ret = OB_ERR_PARALLEL_DDL_CONFLICT;
      LOG_WARN("database_schema's database name not equal to arg",
              KR(ret), K(database_schema->get_database_name_str()), K_(get_params_().database_name));
    }
  }

  return ret;
}

// lock ddl related tablegroup by name with X lock
int ObSetKvAttributeHelper::lock_tablegroup_by_name_()
{
  int ret = OB_SUCCESS;
  const ObString &tablegroup_name = get_params_().table_group_name_;
  if (OB_FAIL(add_lock_object_by_tablegroup_name_(tablegroup_name, transaction::tablelock::EXCLUSIVE))) { // lock tablegroup name
    LOG_WARN("fail to add tablegroup lock by obj name", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(lock_existed_objects_by_name_())) {
    LOG_WARN("fail to lock objects by name", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(check_tablegroup_name_())) { // check tablegroup name
    LOG_WARN("fail to check tablegroup legitimacy", KR(ret), K_(tenant_id));
  }
  return ret;
}

int ObSetKvAttributeHelper::check_tablegroup_name_()
{
  int ret = OB_SUCCESS;
  const ObString &tablegroup_name = get_params_().table_group_name_;
  if (OB_FAIL(schema_guard_wrapper_.get_tablegroup_id(tablegroup_name, tablegroup_id_))) {
    LOG_WARN("fail to get tablegroup id", KR(ret), K(tablegroup_name));
  } else if (tablegroup_id_ == OB_INVALID_ID) {
    ret = OB_TABLEGROUP_NOT_EXIST;
    LOG_INFO("create tablegroup while tablegroup exists", KR(ret), K(tablegroup_name), K_(tablegroup_id));
  }
  return ret;
}

// Lock ddl related database by name
// 1. database (S)
// - to alter table
int ObSetKvAttributeHelper::lock_databases_by_obj_name_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else {
    const ObString &database_name = get_params_().database_name_;
    if (OB_FAIL(add_lock_object_by_database_name_(database_name, transaction::tablelock::SHARE))) {
      LOG_WARN("fail to add lock database by name", KR(ret), K_(tenant_id), K(database_name));
    } else if (OB_FAIL(lock_databases_by_name_())) {
      LOG_WARN("fail to lock databases by name", KR(ret), K_(tenant_id));
    }
  }
  return ret;
}

// check database not in recyclebin
int ObSetKvAttributeHelper::check_database_legitimacy_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else {
    const ObString &database_name = get_params_().database_name_;
    if (OB_FAIL(ObDDLHelper::check_database_legitimacy_(database_name, database_id_))) {
      LOG_WARN("fail to check database legitimacy", KR(ret), K(database_name));
    }
  }
  return ret;
}

// lock table's name object lock (x)
int ObSetKvAttributeHelper::lock_objects_by_name_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else {
    const ObString &database_name = get_params_().database_name_;
    if (database_id_ == OB_INVALID_ID) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("database id is invalid", KR(ret));
    } else if (OB_FAIL(schema_guard_wrapper_.get_table_id_and_table_name_in_tablegroup(tablegroup_id_,
        table_names_, table_ids_))) {
      LOG_WARN("failed to get table schemas in table group", KR(ret), K_(tablegroup_id));
    } else if (table_names_.count() != table_ids_.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table id and name is not match", KR(ret), K_(tablegroup_id),
        K_(get_params_().table_group_name), K_(table_names), K_(table_ids));
    } else if (table_names_.count() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("no tables in tablegroup", KR(ret), K_(tablegroup_id),  K_(get_params_().table_group_name));
    } else {
      for (int64_t i = 0; i < table_names_.count() && OB_SUCC(ret); i++) {
        const ObString &table_name = table_names_.at(i);
        if (OB_FAIL(add_lock_object_by_name_(database_name, table_name,
            share::schema::TABLE_SCHEMA, transaction::tablelock::EXCLUSIVE))) {
          LOG_WARN("fail to lock object by table name", KR(ret), K_(tenant_id), K(database_name), K(table_name));
        }
      } // end for
      if (FAILEDx(lock_existed_objects_by_name_())) {
        LOG_WARN("fail to lock existed objects by names", KR(ret));
      }
    }
  }
  return ret;
}

// lock table' id for alter kv_attributes (x)
int ObSetKvAttributeHelper::lock_objects_by_id_()
{
  int ret = OB_SUCCESS;
  ObTableType table_type;
  int64_t schema_version = OB_INVALID_VERSION;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == database_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("database is not exist", KR(ret), K_(tenant_id), K_(get_params_().database_name));
  } else if (OB_FAIL(add_lock_object_by_id_(database_id_,
    share::schema::DATABASE_SCHEMA, transaction::tablelock::SHARE))) {
    LOG_WARN("fail to lock database id", KR(ret), K_(database_id));
  } else if (OB_FAIL(add_lock_object_by_id_(tablegroup_id_, share::schema::TABLEGROUP_SCHEMA,
                     transaction::tablelock::EXCLUSIVE))) {
    LOG_WARN("failed to add lock object by tablegroup id", KR(ret), K_(tablegroup_id));
  }

  for (int64_t i = 0; i < table_ids_.count() && OB_SUCC(ret); i++) {
    uint64_t table_id = table_ids_.at(i);
    if (OB_FAIL(add_lock_object_by_id_(table_id, share::schema::TABLE_SCHEMA, transaction::tablelock::EXCLUSIVE))) {
      LOG_WARN("fail to lock table id", KR(ret), K_(tenant_id));
    }
  }

  if (FAILEDx(lock_existed_objects_by_id_())) {
    LOG_WARN("fail to lock objects by id", KR(ret));
  } else {  // double check for table_ids_ is consistent
    // we may need to check database_id is consistent in future, for the scene:
    // table is recover from recycle bin and its database is chanage, but this operation is not
    // supported in parallel ddl and we can ignore this check for now
    ObArray<uint64_t> ori_table_ids;
    ObArray<uint64_t> latest_table_ids;
    ObArray<ObString> latest_table_names;  // not used
    if (OB_FAIL(ori_table_ids.assign(table_ids_))) {
      LOG_WARN("fail to assign origin table ids", KR(ret));
    } else if (OB_FAIL(schema_guard_wrapper_.get_table_id_and_table_name_in_tablegroup(tablegroup_id_,
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

// here get the table schemas before lock it. it is rely on mutex between parallel ddl and serial ddl
int ObSetKvAttributeHelper::lock_for_common_ddl_()
{
  int ret = OB_SUCCESS;
  uint64_t tenant_data_version = 0;
  if (OB_UNLIKELY(OB_INVALID_ID == database_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("database is not exist", KR(ret), K_(tenant_id));
  }
  for (int64_t i = 0; i < table_ids_.count() && OB_SUCC(ret); ++i) {
    const uint64_t table_id = table_ids_.at(i);
    const ObTableSchema *orig_table_schema = nullptr;
    if (OB_UNLIKELY(OB_INVALID_ID == table_id)) {
      ret = OB_ERR_OBJECT_NOT_EXIST;
      LOG_WARN("table not exist", KR(ret), K_(database_id));
    } else if (OB_FAIL(schema_guard_wrapper_.get_table_schema(table_id, orig_table_schema))) {
      LOG_WARN("fail to get orig table schema", KR(ret), K(table_id));
    } else if (OB_ISNULL(orig_table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("orig_table_schema_ is nullptr", KR(ret), K(table_id));
    } else if (orig_table_schema->get_database_id() != database_id_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("orig_table_schema_ database id is not match", KR(ret), K(database_id_), KPC(orig_table_schema));
    } else if (OB_UNLIKELY(!orig_table_schema->check_can_do_ddl())) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("offline ddl is being executed, other ddl operations are not allowed", KR(ret), KP(orig_table_schema));
    } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id_, tenant_data_version))) {
      LOG_WARN("get min data version failed", KR(ret), K_(tenant_id));
    } else if (OB_FAIL(ObDDLLock::lock_for_common_ddl_in_trans(*orig_table_schema, false/*require_strict_binary_format*/,get_trans_()))) {
      LOG_WARN("fail to lock for common ddl", KR(ret));
    } else if (OB_FAIL(origin_table_schemas_.push_back(orig_table_schema))) {
      LOG_WARN("fail to push back orig_table_schema", KR(ret));
    }
  }
  RS_TRACE(lock_common_ddl);
  return ret;
}

int ObSetKvAttributeHelper::check_table_legitimacy_()
{
  int ret = OB_SUCCESS;
  bool is_oracle_mode = false;
  int64_t schema_version = OB_INVALID_VERSION;
  bool is_exist = false;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else {
    for (int64_t i = 0; i < origin_table_schemas_.count() && OB_SUCC(ret); i++) {
      const ObTableSchema *orig_table_schema = origin_table_schemas_.at(i);
      if (OB_ISNULL(orig_table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("orig_table_schema_ is nullptr", KR(ret));
      } else if (OB_UNLIKELY(!orig_table_schema->is_user_table())) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("table type which is not user table is", KR(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "alter materialized view is");
      } else if (OB_UNLIKELY(orig_table_schema->is_in_recyclebin())) {
        ret = OB_ERR_OPERATION_ON_RECYCLE_OBJECT;
        LOG_WARN("can not comment table in recyclebin", KR(ret), K(orig_table_schema->is_in_recyclebin()));
      } else if (OB_UNLIKELY(orig_table_schema->is_in_splitting())) {
        ret = OB_OP_NOT_ALLOW;
        LOG_WARN("table is physical or logical split can not split", KR(ret), KPC(orig_table_schema));
      } else if (OB_FAIL(orig_table_schema->check_if_oracle_compat_mode(is_oracle_mode))) {
        LOG_WARN("fail to check oracle compat mode", KR(ret));
      } else if (OB_UNLIKELY(is_oracle_mode)) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not support parallel set kv_attributes on oracle mode", KR(ret));
      } else if (OB_FAIL(ddl_service_->get_snapshot_mgr().check_restore_point(
        ddl_service_->get_sql_proxy(), tenant_id_, orig_table_schema->get_table_id(), is_exist))) {
        LOG_WARN("failed to check restore point", KR(ret), K_(tenant_id));
      } else if (OB_UNLIKELY(is_exist)) {
        ret = OB_OP_NOT_ALLOW;
        LOG_WARN("restore point exist, cannot alter ", KR(ret), K_(tenant_id), K(orig_table_schema->get_table_id()));
      } else if (OB_FAIL(ObTTLUtil::check_htable_ddl_supported(*orig_table_schema, true/*by_admin*/))) {
        LOG_WARN("failed to check htable ddl supoprted", KR(ret));
      }
    }
  }
  RS_TRACE(check_schemas);
  return ret;
}

int ObSetKvAttributeHelper::generate_schemas_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(check_table_legitimacy_())) {
    LOG_WARN("fail to check table legitimacy", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < origin_table_schemas_.count(); ++i) {
      const ObTableSchema *orig_table_schema = origin_table_schemas_.at(i);
      ObTableSchema *new_table_schema = nullptr;
      ObKVAttr kv_attr;
      ObString new_kv_attr_str;
      ObString ddl_stmt_str;
      bool is_disable = get_params_().is_disable_;
      if (OB_ISNULL(orig_table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("orig table schema is nullptr", KR(ret));
      } else if (OB_FAIL(ObSchemaUtils::alloc_schema(allocator_, *orig_table_schema, new_table_schema))) {
        LOG_WARN("fail to alloc schema", KR(ret));
      } else if (OB_FAIL(ObTTLUtil::parse_kv_attributes(orig_table_schema->get_kv_attributes(), kv_attr))) {
        LOG_WARN("fail to parse kv attributes", KR(ret));
      } else if (OB_FAIL(check_and_modify_kv_attr_(kv_attr, is_disable))) {
        LOG_WARN("fail to check and modify kv attr", KR(ret), K(is_disable));
      } else if (OB_FAIL(ObTTLUtil::format_kv_attributes_to_json_str(allocator_, kv_attr, new_kv_attr_str))) {
        LOG_WARN("fail to format kv attr", KR(ret), K(kv_attr));
      } else if (OB_FAIL(construct_ddl_stmt_(orig_table_schema->get_table_name(), new_kv_attr_str, ddl_stmt_str))) {
        LOG_WARN("fail to construct ddl stmt", KR(ret));
      } else if (OB_FAIL(new_table_schema->set_kv_attributes(new_kv_attr_str))) {
        LOG_WARN("fail to set table kv attr", KR(ret));
      } else if (OB_FAIL(new_table_schemas_.push_back(new_table_schema))) {
        LOG_WARN("fail to push back new table schema", KR(ret));
      } else if (OB_FAIL(ddl_stmt_strs_.push_back(ddl_stmt_str))) {
        LOG_WARN("fail to push back ddl stmt str", KR(ret), K(ddl_stmt_str));
      }
    }
  }
  RS_TRACE(generate_schemas);
  return ret;
}

int ObSetKvAttributeHelper::check_and_modify_kv_attr_(ObKVAttr &kv_attr, bool is_table_disable)
{
  int ret = OB_SUCCESS;
  const ObString &tablegroup_name = get_params_().table_group_name_;
  if (kv_attr.is_disable_ && is_table_disable) {
    ret = OB_KV_TABLE_NOT_ENABLED;
    LOG_WARN("table has already disable, can't disable again", KR(ret), K(kv_attr));
    LOG_USER_ERROR(OB_KV_TABLE_NOT_ENABLED, tablegroup_name.length(), tablegroup_name.ptr());
  } else if (!kv_attr.is_disable_ && !is_table_disable) {
    ret = OB_KV_TABLE_NOT_DISABLED;
    LOG_WARN("table has already enable, can't enable again", KR(ret), K(kv_attr));
    LOG_USER_ERROR(OB_KV_TABLE_NOT_DISABLED, tablegroup_name.length(), tablegroup_name.ptr());
  } else {
    kv_attr.is_disable_ = is_table_disable;
  }
  return ret;
}

int ObSetKvAttributeHelper::construct_ddl_stmt_(const ObString &table_name,
                                               const ObString &kv_attr_str,
                                               ObString &ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  int64_t table_name_len = table_name.length();
  int64_t kv_attr_str_len = kv_attr_str.length();
  const int64_t fix_part_len = 29; // "ALTER TABLE "(12) + " KV_ATTRIBUTES="(15) + "\'\'"(2)
  int64_t total_len = table_name_len + kv_attr_str_len + fix_part_len + 1; // '\0'
  char *buf_ptr = nullptr;
  int64_t pos = 0;
  if (OB_ISNULL(buf_ptr = static_cast<char *>(allocator_.alloc(total_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc buffer", KR(ret), K(total_len));
  } else if (OB_FAIL(databuff_printf(buf_ptr, total_len, pos, ALTER_KV_ATTRIBUTE_FORMAT_STR,
      table_name_len, table_name.ptr(), kv_attr_str_len, kv_attr_str.ptr()))) {
    LOG_WARN("fail to format ddl stmt str", KR(ret), K(table_name), K(kv_attr_str));
  } else {
    ddl_stmt_str.assign(buf_ptr, pos);
  }
  return ret;
}

int ObSetKvAttributeHelper::calc_schema_version_cnt_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else {
    // 1. all modify tables
    schema_version_cnt_ = new_table_schemas_.count();
    // 1503
    schema_version_cnt_++;
  }
  return ret;
}

int ObSetKvAttributeHelper::operate_schemas_()
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::current_time();
  ObSchemaService *schema_service_impl = nullptr;
  int64_t new_schema_version = OB_INVALID_VERSION;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (new_table_schemas_.count() != ddl_stmt_strs_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("counts of new_table_schema and ddl_stmt_strs is not equal", KR(ret),
          K(new_table_schemas_.count()), K(ddl_stmt_strs_.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < new_table_schemas_.count() ; i++) {
    ObTableSchema *new_table_schema = new_table_schemas_.at(i);
    ObString ddl_stmt_str = ddl_stmt_strs_.at(i);
    if (OB_ISNULL(new_table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("new table schema is null", KR(ret), KP(new_table_schema));
    } else if (OB_ISNULL(schema_service_impl = schema_service_->get_schema_service())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema_service impl is null", KR(ret));
    } else if (OB_FAIL(schema_service_->gen_new_schema_version(tenant_id_, new_schema_version))) {
      LOG_WARN("fail to gen new schema version", KR(ret), K_(tenant_id));
    } else {
      new_table_schema->set_schema_version(new_schema_version);
      if (FAILEDx(schema_service_impl->get_table_sql_service().only_update_table_options(
                                                              get_trans_(),
                                                              *new_table_schema,
                                                              OB_DDL_ALTER_TABLE,
                                                              &ddl_stmt_str))) {
        LOG_WARN("fail to alter table option", KR(ret));
      }
    }
    #ifdef ERRSIM
    if (OB_SUCC(ret) && get_params_().is_disable_) {
      ret = OB_E(common::EventTable::EN_DISABLE_HTABLE_CF_FINISH_ERR) OB_SUCCESS;
      if (OB_FAIL(ret)) {
        LOG_WARN("[ERRSIM] fail to disable table", KR(ret));
      }
    }
    #endif
  }
  RS_TRACE(alter_schemas);
  return ret;
}

int ObSetKvAttributeHelper::construct_and_adjust_result_(int &return_ret)
{
  int ret = return_ret;
  if (FAILEDx(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else {
    ObSchemaVersionGenerator *tsi_generator = GET_TSI(TSISchemaVersionGenerator);
    if (OB_ISNULL(tsi_generator)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tsi schema version generator is null", KR(ret));
    } else {
      tsi_generator->get_current_version(res_.schema_version_);
    }
  }
  return ret;
}

int ObSetKvAttributeHelper::operation_before_commit_()
{
  int ret = OB_SUCCESS;
  // do nothing
  return ret;
}

int ObSetKvAttributeHelper::clean_on_fail_commit_()
{
  int ret = OB_SUCCESS;
  // do nothing
  return ret;
}
