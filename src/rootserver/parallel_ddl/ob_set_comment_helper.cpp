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
#include "rootserver/parallel_ddl/ob_set_comment_helper.h"
#include "share/ob_rpc_struct.h"
#include "share/schema/ob_table_sql_service.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "rootserver/ob_snapshot_info_manager.h"
#include "storage/ddl/ob_ddl_lock.h"
#include "share/ob_debug_sync.h"

using namespace oceanbase::lib;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::rootserver;


ObSetCommentHelper::ObSetCommentHelper(
  share::schema::ObMultiVersionSchemaService *schema_service,
  const uint64_t tenant_id,
  const obrpc::ObSetCommentArg &arg,
  obrpc::ObParallelDDLRes &res)
  : ObDDLHelper(schema_service, tenant_id),
  arg_(arg),
  res_(res),
  database_id_(OB_INVALID_ID),
  table_id_(OB_INVALID_ID),
  orig_table_schema_(nullptr),
  new_table_schema_(nullptr),
  new_column_schemas_()
{}

ObSetCommentHelper::~ObSetCommentHelper()
{
}

int ObSetCommentHelper::check_inner_stat_()
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

int ObSetCommentHelper::execute()
{
  int ret = OB_SUCCESS;
  RS_TRACE(parallel_ddl_begin);
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(start_ddl_trans_())) {
    LOG_WARN("fail to start ddl trans", KR(ret));
  } else if (OB_FAIL(lock_objects_())) {
    LOG_WARN("fail to lock objects", KR(ret));
  } else if (OB_FAIL(check_table_legitimacy_())) {
    LOG_WARN("fail to check comment", KR(ret));
  } else if (OB_FAIL(generate_schemas_())) {
    LOG_WARN("fail to generate schemas", KR(ret));
  } else if (OB_FAIL(calc_schema_version_cnt_())) {
    LOG_WARN("fail to calc schema version cnt", KR(ret));
  } else if (OB_FAIL(gen_task_id_and_schema_versions_())) {
    LOG_WARN("fail to gen task id and schema versions", KR(ret));
  } else if (OB_FAIL(alter_schema_())) {
    LOG_WARN("fail to create schemas", KR(ret));
  } else if (OB_FAIL(serialize_inc_schema_dict_())) {
    LOG_WARN("fail to serialize inc schema dict", KR(ret));
  } else if (OB_FAIL(wait_ddl_trans_())) {
    LOG_WARN("fail to wait ddl trans", KR(ret));
  }
  if (OB_FAIL(end_ddl_trans_(ret))) { //won't overwrite ret
    LOG_WARN("fail to end ddl trans", KR(ret));
  } else {
    ObSchemaVersionGenerator *tsi_generator = GET_TSI(TSISchemaVersionGenerator);
    int64_t last_schema_version = OB_INVALID_VERSION;
    int64_t end_schema_version = OB_INVALID_VERSION;
    if (OB_ISNULL(tsi_generator)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tsi generator is null", KR(ret));
    } else if (OB_FAIL(tsi_generator->get_current_version(last_schema_version))) {
      LOG_WARN("fail to get current version", KR(ret), K_(tenant_id), K_(arg));
    } else if (OB_FAIL(tsi_generator->get_end_version(end_schema_version))) {
      LOG_WARN("fail to get end version", KR(ret), K_(tenant_id), K_(arg));
    } else if (OB_UNLIKELY(last_schema_version != end_schema_version)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("too much schema versions may be allocated", KR(ret), KPC(tsi_generator));
    } else {
      res_.schema_version_ = last_schema_version;
    }
  }
  RS_TRACE(parallel_ddl_end);
  FORCE_PRINT_TRACE(THE_RS_TRACE, "[parallel set comment]");
  return ret;
}

int ObSetCommentHelper::lock_objects_()
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
  } else if (OB_FAIL(lock_objects_by_name_())) { // lock object name
    LOG_WARN("fail to lock objects by name", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(lock_objects_by_id_())) { // lock objects by id
    LOG_WARN("fail to lock objects by id" , KR(ret), K_(tenant_id));
  }
  DEBUG_SYNC(AFTER_PARALLEL_DDL_LOCK);
  RS_TRACE(lock_objects);
  if (FAILEDx(lock_for_common_ddl_())) { // online ddl lock & table lock
    LOG_WARN("fail to lock for common ddl", KR(ret));
  } else if (OB_UNLIKELY(database_id_ != orig_table_schema_->get_database_id())) {
    ret = OB_ERR_PARALLEL_DDL_CONFLICT;
    LOG_WARN("database_id_ is not equal to table schema's databse_id",
             KR(ret), K_(database_id), K(orig_table_schema_->get_database_id()));
  } else if (OB_FAIL(latest_schema_guard_.get_database_schema(database_id_, database_schema))) {
    LOG_WARN("fail to get database schema", KR(ret), K_(tenant_id), K_(database_id));
  } else if (OB_ISNULL(database_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("databse_schema is null", KR(ret));
  } else if (OB_UNLIKELY(database_schema->get_database_name_str() != arg_.database_name_)) {
    ret = OB_ERR_PARALLEL_DDL_CONFLICT;
    LOG_WARN("database_schema's database name not equal to arg",
             KR(ret), K(database_schema->get_database_name_str()), K_(arg_.database_name));
  }
  return ret;
}

// Lock ddl related database by name
// 1. database (S)
// - to alter table
int ObSetCommentHelper::lock_databases_by_obj_name_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else {
    const ObString &database_name = arg_.database_name_;
    if (OB_FAIL(add_lock_object_by_database_name_(database_name, transaction::tablelock::SHARE))) {
      LOG_WARN("fail to add lock database by name", KR(ret), K_(tenant_id), K(database_name));
    } else if (OB_FAIL(lock_databases_by_name_())) {
      LOG_WARN("fail to lock databases by name", KR(ret), K_(tenant_id));
    }
  }
  return ret;
}
// check database not in recyclebin
int ObSetCommentHelper::check_database_legitimacy_()
{
  int ret = OB_SUCCESS;
  const ObString &database_name = arg_.database_name_;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(ObDDLHelper::check_database_legitimacy_(database_name, database_id_))) {
    LOG_WARN("fail to check database legitimacy", KR(ret), K(database_name));
  }
  return ret;
}
// lock table's name object lock (x)
int ObSetCommentHelper::lock_objects_by_name_()
{
  int ret = OB_SUCCESS;
  const ObString &database_name = arg_.database_name_;
  const ObString &table_name = arg_.table_name_;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(add_lock_object_by_name_(database_name, table_name,
      share::schema::TABLE_SCHEMA, transaction::tablelock::EXCLUSIVE))) {
    LOG_WARN("fail to lock object by table name", KR(ret), K_(tenant_id), K(database_name), K(table_name));
  } else if (OB_FAIL(lock_existed_objects_by_name_())) {
    LOG_WARN("fail to lock objects by name", KR(ret), K_(tenant_id));
  }
  return ret;
}

// lock table' id for comment (x)
int ObSetCommentHelper::lock_objects_by_id_()
{
  int ret = OB_SUCCESS;
  ObTableType table_type;
  int64_t schema_version = OB_INVALID_VERSION;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == database_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("database is not exist", KR(ret), K_(tenant_id), K_(arg_.database_name));
  } else if (OB_FAIL(add_lock_object_by_id_(database_id_,
    share::schema::DATABASE_SCHEMA, transaction::tablelock::SHARE))) {
    LOG_WARN("fail to lock database id", KR(ret), K_(database_id));
  } else if (OB_FAIL(latest_schema_guard_.get_table_id(database_id_, arg_.session_id_, arg_.table_name_, table_id_, table_type, schema_version))) {
    LOG_WARN("fail to get table id", KR(ret), K_(database_id), K_(arg_.session_id), K_(arg_.table_name));
  } else if (OB_UNLIKELY(OB_INVALID_ID == table_id_)) {
    ret = OB_ERR_OBJECT_NOT_EXIST;
    LOG_WARN("table not exist", KR(ret), K_(database_id), K_(arg_.session_id), K_(arg_.table_name));
  } else if (OB_FAIL(add_lock_object_by_id_(table_id_,
    share::schema::TABLE_SCHEMA, transaction::tablelock::EXCLUSIVE))) {
    LOG_WARN("fail to lock table id", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(lock_existed_objects_by_id_())) {
    LOG_WARN("fail to lock objects by id", KR(ret));
  }
  return ret;
}

int ObSetCommentHelper::check_table_legitimacy_()
{
  int ret = OB_SUCCESS;
  bool is_oracle_mode = false;
  int64_t schema_version = OB_INVALID_VERSION;
  bool is_exist = false;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_ISNULL(orig_table_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("orig_table_schema_ is nullptr", KR(ret), K_(table_id));
  } else if (OB_UNLIKELY(orig_table_schema_->is_materialized_view())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("alter materialized view is not supported", KR(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "alter materialized view is");
  } else if (OB_UNLIKELY(orig_table_schema_->is_ctas_tmp_table())) {
    ret = OB_ERR_WRONG_OBJECT;
    LOG_USER_ERROR(OB_ERR_WRONG_OBJECT,
    to_cstring(arg_.database_name_), to_cstring(arg_.table_name_), "BASE TABLE");
  } else if (OB_UNLIKELY((!orig_table_schema_->is_user_table()
                          && !orig_table_schema_->is_tmp_table()
                          && !orig_table_schema_->is_view_table()
                          && !orig_table_schema_->is_external_table()))) {
    ret = OB_ERR_WRONG_OBJECT;
    LOG_USER_ERROR(OB_ERR_WRONG_OBJECT,
    to_cstring(arg_.database_name_), to_cstring(arg_.table_name_), "BASE TABLE");
  } else if (OB_UNLIKELY(orig_table_schema_->is_sys_view() && !GCONF.enable_sys_table_ddl)) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("comment on sys view is not allowed", KR(ret), K(orig_table_schema_->get_table_id()));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "alter system view");
  } else if (OB_UNLIKELY(orig_table_schema_->is_in_recyclebin())) {
    ret = OB_ERR_OPERATION_ON_RECYCLE_OBJECT;
    LOG_WARN("can not comment table in recyclebin", KR(ret), K(orig_table_schema_->is_in_recyclebin()));
  } else if (OB_UNLIKELY(orig_table_schema_->is_in_splitting())) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("table is physical or logical split can not split", KR(ret), KPC(orig_table_schema_));
  } else if (OB_FAIL(orig_table_schema_->check_if_oracle_compat_mode(is_oracle_mode))) {
    LOG_WARN("fail to check oracle compat mode", KR(ret));
  } else if (OB_UNLIKELY(!is_oracle_mode)) {
    // if need to support parallel comment on mysql mode,
    // should check column name not duplicate to prevent modify the same column in one sql
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support parallel comment on mysql mode right know", KR(ret));
  } else if (OB_FAIL(ddl_service_->get_snapshot_mgr().check_restore_point(
    ddl_service_->get_sql_proxy(), tenant_id_, orig_table_schema_->get_table_id(), is_exist))) {
    LOG_WARN("failed to check restore point", KR(ret), K_(tenant_id));
  } else if (OB_UNLIKELY(is_exist)) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("restore point exist, cannot alter ", KR(ret), K_(tenant_id), K(orig_table_schema_->get_table_id()));
  }
  RS_TRACE(check_schemas);
  return ret;
}

// here get the table schema before lock it. it is rely on mutex between parallel ddl and serial ddl
int ObSetCommentHelper::lock_for_common_ddl_()
{
  int ret = OB_SUCCESS;
  int64_t schema_version = OB_INVALID_VERSION;
  uint64_t tenant_data_version = 0;
  if (OB_UNLIKELY(OB_INVALID_ID == database_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("database is not exist", KR(ret), K_(tenant_id), K_(arg_.database_name));
  } else if (OB_UNLIKELY(OB_INVALID_ID == table_id_)) {
    ret = OB_ERR_OBJECT_NOT_EXIST;
    LOG_WARN("table not exist", KR(ret), K_(database_id), K_(arg_.session_id), K_(arg_.table_name));
  } else if (OB_FAIL(latest_schema_guard_.get_table_schema(table_id_, orig_table_schema_))) {
    LOG_WARN("fail to get orig table schema", KR(ret), K_(table_id));
  } else if (OB_ISNULL(orig_table_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("orig_table_schema_ is nullptr", KR(ret), K_(table_id));
  } else if (OB_UNLIKELY(!orig_table_schema_->check_can_do_ddl())) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("offline ddl is being executed, other ddl operations are not allowed", KR(ret), KP(orig_table_schema_));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id_, tenant_data_version))) {
    LOG_WARN("get min data version failed", KR(ret), K_(tenant_id));
  } else {
    if (OB_FAIL(ObDDLLock::lock_for_common_ddl_in_trans(*orig_table_schema_, false/*require_strict_binary_format*/,trans_))) {
      LOG_WARN("fail to lock for common ddl", KR(ret));
    }
  }
  RS_TRACE(lock_common_ddl);
  return ret;
}

int ObSetCommentHelper::generate_schemas_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_ISNULL(orig_table_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("orig table schema is nullptr", KR(ret));
  } else if (OB_FAIL(ObSchemaUtils::alloc_schema(allocator_, *orig_table_schema_, new_table_schema_))) {
    LOG_WARN("fail to alloc schema", KR(ret));
  } else {
    if (obrpc::ObSetCommentArg::COMMENT_TABLE == arg_.op_type_) {
      if (OB_FAIL(new_table_schema_->set_comment(arg_.table_comment_))) {
        LOG_WARN("fail to set table comment", KR(ret));
      }
    } else if (obrpc::ObSetCommentArg::COMMENT_COLUMN == arg_.op_type_) {
      for (uint64_t column_idx = 0; OB_SUCC(ret) && column_idx < arg_.column_name_list_.size(); column_idx++) {
        const ObString &orig_column_name = arg_.column_name_list_.at(column_idx);
        ObColumnSchemaV2 *new_column_schema = nullptr;
        new_column_schema = new_table_schema_->get_column_schema(orig_column_name);
        if (OB_ISNULL(new_column_schema)) {
          ret = OB_ERR_BAD_FIELD_ERROR;
          LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR, orig_column_name.length(), orig_column_name.ptr(),
                      orig_table_schema_->get_table_name_str().length(),
                      orig_table_schema_->get_table_name_str().ptr());
          LOG_WARN("failed to find old column schema", KR(ret), K(orig_column_name));
        } else if (FALSE_IT(new_column_schema->set_comment(arg_.column_comment_list_.at(column_idx)))){
        } else if (OB_FAIL(new_column_schemas_.push_back(new_column_schema))) {
          LOG_WARN("fail to assign column schema", KR(ret));
        }
      }
    }
  }
  RS_TRACE(generate_schemas);
  return ret;
}

int ObSetCommentHelper::calc_schema_version_cnt_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else {
    // 0. data table
    // for now just one comment at a once
    schema_version_cnt_ = 1;
    // 1503
    schema_version_cnt_++;
  }
  return ret;
}

int ObSetCommentHelper::alter_schema_()
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::current_time();
  ObSchemaService *schema_service_impl = nullptr;
  int64_t new_schema_version = OB_INVALID_VERSION;
  const ObString *ddl_stmt_str = &arg_.ddl_stmt_str_;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_ISNULL(schema_service_impl = schema_service_->get_schema_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service impl is null", KR(ret));
  } else if (OB_FAIL(schema_service_->gen_new_schema_version(tenant_id_, new_schema_version))) {
    LOG_WARN("fail to gen new schema version", KR(ret), K_(tenant_id));
  } else {
    const bool need_del_stat = false;
    for (uint64_t column_idx = 0; OB_SUCC(ret) && column_idx < arg_.column_name_list_.size(); column_idx++) {
      new_column_schemas_.at(column_idx)->set_schema_version(new_schema_version);
      if (OB_FAIL(schema_service_impl->get_table_sql_service().update_single_column(
                trans_, *orig_table_schema_, *new_table_schema_,
                *new_column_schemas_.at(column_idx), false /* record ddl operation*/, need_del_stat))) {
        LOG_WARN("fail to update single column", KR(ret));
      }
    }
    new_table_schema_->set_schema_version(new_schema_version);
    if (FAILEDx(schema_service_impl->get_table_sql_service().only_update_table_options(
                                                             trans_,
                                                             *new_table_schema_,
                                                             OB_DDL_ALTER_TABLE,
                                                             ddl_stmt_str))) {
      LOG_WARN("fail to alter table option", KR(ret));
    }
  }
  RS_TRACE(alter_schemas);
  return ret;
}
