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
#include "rootserver/ob_root_service.h"
#include "rootserver/parallel_ddl/ob_create_index_helper.h"
#include "rootserver/ob_table_creator.h"
#include "rootserver/freeze/ob_major_freeze_helper.h"
#include "rootserver/ob_balance_group_ls_stat_operator.h"
#include "rootserver/ddl_task/ob_ddl_scheduler.h"
#include "share/ob_index_builder_util.h"
#include "share/ob_rpc_struct.h"
#include "share/ob_debug_sync_point.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_table_sql_service.h"
#include "sql/resolver/ob_resolver_utils.h"
using namespace oceanbase::lib;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::rootserver;

ObCreateIndexHelper::ObCreateIndexHelper(
    share::schema::ObMultiVersionSchemaService *schema_service,
    const uint64_t tenant_id,
    rootserver::ObDDLService &ddl_service,
    const obrpc::ObCreateIndexArg &arg,
    obrpc::ObAlterTableRes &res)
  : ObDDLHelper(schema_service, tenant_id),
    arg_(arg),
    new_arg_(nullptr),
    res_(res),
    orig_data_table_schema_(nullptr),
    new_data_table_schema_(nullptr),
    index_schemas_(),
    gen_columns_(),
    index_builder_(ddl_service),
    task_record_()
{
}

ObCreateIndexHelper::~ObCreateIndexHelper()
{
  if (OB_NOT_NULL(new_arg_)) {
    new_arg_->~ObCreateIndexArg();
    new_arg_ = nullptr;
  }
}

int ObCreateIndexHelper::execute()
{
  RS_TRACE(parallel_ddl_begin);
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(start_ddl_trans_())) {
    LOG_WARN("fail to start ddl trans", KR(ret));
  } else if (OB_FAIL(lock_objects_())) {
    LOG_WARN("fail to lock objects", KR(ret));
  } else if (OB_FAIL(check_table_legitimacy_())) {
    LOG_WARN("fail to check create index", KR(ret));
  } else if (OB_FAIL(generate_index_schema_())) {
    LOG_WARN("fail to generate index schema");
  } else if (OB_FAIL(calc_schema_version_cnt_())) {
    LOG_WARN("fail to calc schema version cnt", KR(ret));
  } else if (OB_FAIL(gen_task_id_and_schema_versions_())) {
    LOG_WARN("fail to gen task id and schema versions", KR(ret));
  } else if (OB_FAIL(create_index_())) {
    LOG_WARN("fail to create index", KR(ret));
  } else if (OB_FAIL(serialize_inc_schema_dict_())) {
    LOG_WARN("fail to serialize inc schema dict", KR(ret));
  } else if (OB_FAIL(wait_ddl_trans_())) {
    LOG_WARN("fail to wait ddl trans", KR(ret));
  } else if (OB_FAIL(add_index_name_to_cache_())) {
    LOG_WARN("fail to add index name to cache", KR(ret));
  }
  const bool commit = OB_SUCC(ret);
  if (OB_FAIL(end_ddl_trans_(ret))) {
    LOG_WARN("fail to end ddl trans", KR(ret));
    if (commit) {
      int tmp_ret = OB_SUCCESS;
      if (OB_ISNULL(ddl_service_)) {
        tmp_ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ddl_service_ is null", KR(tmp_ret));
      } else if (OB_TMP_FAIL(ddl_service_->get_index_name_checker().reset_cache(tenant_id_))) {
        LOG_WARN("fail to reset cache", K(ret), KR(tmp_ret), K_(tenant_id));
      }
    }
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
    } else if (OB_UNLIKELY(index_schemas_.count() != 1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("index schemas count unexpected", KR(ret), K(index_schemas_.count()));
    } else {
      res_.index_table_id_ = index_schemas_.at(0).get_table_id();
      res_.schema_version_ = last_schema_version;
      res_.task_id_ = task_record_.task_id_;
      if (OB_FAIL(GCTX.root_service_->get_ddl_task_scheduler().schedule_ddl_task(task_record_))) {
        LOG_WARN("fail to schedule ddl task", KR(ret), K_(task_record));
      }
    }
  }
  if (OB_ERR_KEY_NAME_DUPLICATE == ret) {
    if (true == arg_.if_not_exist_) {
      ret = OB_SUCCESS;
      LOG_USER_WARN(OB_ERR_KEY_NAME_DUPLICATE, arg_.index_name_.length(), arg_.index_name_.ptr());
    } else {
      LOG_USER_ERROR(OB_ERR_KEY_NAME_DUPLICATE, arg_.index_name_.length(), arg_.index_name_.ptr());
    }
  }
  RS_TRACE(parallel_ddl_end);
  FORCE_PRINT_TRACE(THE_RS_TRACE, "[parallel create index]");
  return ret;
}

// the online ddl lock and table lock will be locked when submit ddl task
int ObCreateIndexHelper::lock_objects_()
{
  int ret = OB_SUCCESS;
  const ObDatabaseSchema *database_schema = NULL;
  DEBUG_SYNC(BEFORE_PARALLEL_DDL_LOCK);
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(lock_database_by_obj_name_())) {
    LOG_WARN("fail to lock databases by obj name", KR(ret));
  } else if (OB_FAIL(check_database_legitimacy_())) {
    LOG_WARN("fail to prefech schemas", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(lock_objects_by_name_())) {
    LOG_WARN("fail to prefech schemas", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(lock_objects_by_id_())) {
    LOG_WARN("fail to lock objects by id", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(check_parallel_ddl_conflict_(arg_.based_schema_object_infos_))) {
    LOG_WARN("fail to check parallel ddl conflict", KR(ret));
  } else if (OB_ISNULL(orig_data_table_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("orig_data_table_schema", KR(ret));
  } else if (OB_UNLIKELY(database_id_ != orig_data_table_schema_->get_database_id())) {
    ret = OB_ERR_PARALLEL_DDL_CONFLICT;
    LOG_WARN("database_id_ is not equal to table schema's databse_id",
             KR(ret), K_(database_id), K(orig_data_table_schema_->get_database_id()));
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
  DEBUG_SYNC(AFTER_PARALLEL_DDL_LOCK);
  RS_TRACE(lock_objects);
  return ret;
}

int ObCreateIndexHelper::lock_database_by_obj_name_()
{
  int ret = OB_SUCCESS;
  const ObString &database_name = arg_.database_name_;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(add_lock_object_by_database_name_(database_name, transaction::tablelock::SHARE))) {
    LOG_WARN("fail to lock database by name", KR(ret), K_(tenant_id), K(database_name));
  } else if (OB_FAIL(lock_databases_by_name_())) {
    LOG_WARN("fail to lock databases by name", KR(ret), K_(tenant_id));
  }
  return ret;
}

//lock table name and index name (x)
int ObCreateIndexHelper::lock_objects_by_name_()
{
  int ret = OB_SUCCESS;
  bool is_oracle_mode = false;
  const ObString &database_name = arg_.database_name_;
  const ObString &table_name = arg_.table_name_;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_tenant_id(
             tenant_id_, is_oracle_mode))) {
    LOG_WARN("fail to check is oracle mode", KR(ret));
  } else if (OB_FAIL(add_lock_object_by_name_(database_name, table_name,
    share::schema::TABLE_SCHEMA, transaction::tablelock::EXCLUSIVE))) {
    LOG_WARN("fail to add lock object by name", KR(ret), K_(tenant_id), K(database_name), K(table_name));
  } else if (is_oracle_mode) {
    const ObString &index_name = arg_.index_name_;
    if (OB_FAIL(add_lock_object_by_name_(database_name, index_name,
        share::schema::TABLE_SCHEMA, transaction::tablelock::EXCLUSIVE))) {
      LOG_WARN("fail to lock object by index name", KR(ret), K_(tenant_id), K(database_name), K(index_name));
    }
  }
  if (FAILEDx(lock_existed_objects_by_name_())) {
    LOG_WARN("fail to lock objects by name", KR(ret), K_(tenant_id));
  }
  return ret;
}

int ObCreateIndexHelper::check_database_legitimacy_()
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
// lock data table id and foreign table id
// the foreign table need to aware the related table have created an index
// thus, need to push foreign table schema_version
int ObCreateIndexHelper::lock_objects_by_id_()
{
  int ret = OB_SUCCESS;
  uint64_t table_id = OB_INVALID_ID;
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
  } else if (OB_FAIL(latest_schema_guard_.get_table_id(database_id_, arg_.session_id_, arg_.table_name_, table_id, table_type, schema_version))) {
    LOG_WARN("fail to get table id", KR(ret), K_(database_id), K_(arg_.session_id), K_(arg_.table_name));
  } else if (OB_UNLIKELY(OB_INVALID_ID == table_id)) {
    ret = OB_ERR_OBJECT_NOT_EXIST;
    LOG_WARN("table not exist", KR(ret), K_(database_id), K_(arg_.session_id), K_(arg_.table_name));
  } else if (OB_FAIL(add_lock_object_by_id_(table_id,
    share::schema::TABLE_SCHEMA, transaction::tablelock::EXCLUSIVE))) {
    LOG_WARN("fail to lock table id", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(latest_schema_guard_.get_table_schema(table_id, orig_data_table_schema_))) {
    LOG_WARN("fail to get orig table schema", KR(ret), K(table_id));
  } else if (OB_ISNULL(orig_data_table_schema_)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_USER_ERROR(OB_TABLE_NOT_EXIST, to_cstring(arg_.database_name_), to_cstring(arg_.table_name_));
    LOG_WARN("table not exist", KR(ret), K_(arg));
  } else if (OB_FAIL(add_lock_table_udt_id_(*orig_data_table_schema_))) {
    LOG_WARN("fail to add lock table udt id", KR(ret));
  } else {
    const ObIArray<ObForeignKeyInfo> &foreign_key_infos = orig_data_table_schema_->get_foreign_key_infos();
    for (int64_t i = 0; OB_SUCC(ret) && i < foreign_key_infos.count(); i++) {
      const ObForeignKeyInfo &foreign_key_info = foreign_key_infos.at(i);
      const uint64_t related_table_id = table_id == foreign_key_info.parent_table_id_
          ? foreign_key_info.child_table_id_
          : foreign_key_info.parent_table_id_;
      if (related_table_id == table_id) {
      } else if (OB_FAIL(add_lock_object_by_id_(related_table_id,
        share::schema::TABLE_SCHEMA, transaction::tablelock::EXCLUSIVE))) {
        LOG_WARN("fail to lock table id", KR(ret), K_(tenant_id));
      }
    }
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; OB_SUCC(ret) && (i < arg_.based_schema_object_infos_.count()); ++i) {
        const ObBasedSchemaObjectInfo &info = arg_.based_schema_object_infos_.at(i);
        if (info.schema_id_ == table_id) {
          // already have x lock
        } else if (is_inner_pl_udt_id(info.schema_id_) || is_inner_pl_object_id(info.schema_id_)) {
          // do nothing
        } else if (OB_FAIL(add_lock_object_by_id_(info.schema_id_,
                                                  info.schema_type_,
                                                  transaction::tablelock::SHARE))) {
          LOG_WARN("fail to lock based object schema id", KR(ret), K_(tenant_id));
        }
      }
    }
  }
  if (FAILEDx(lock_existed_objects_by_id_())) {
    LOG_WARN("fail to lock objects by id", KR(ret));
  }

  return ret;
}

int ObCreateIndexHelper::check_table_legitimacy_()
{
  int ret = OB_SUCCESS;
  uint64_t table_id = OB_INVALID_ID;
  bool in_tenant_space = true;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_ISNULL(orig_data_table_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data table schea is null", KR(ret), KP(orig_data_table_schema_));
  } else if (FALSE_IT(table_id = orig_data_table_schema_->get_table_id())) {
  } else if (OB_FAIL(ObSysTableChecker::is_tenant_space_table_id(table_id, in_tenant_space))) {
    LOG_WARN("fail to check table in tenant space", KR(ret), K(table_id));
  } else if (OB_UNLIKELY(is_inner_table(table_id))) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("create index on inner table not support", KR(ret), K(table_id));
  } else if (OB_UNLIKELY(!arg_.is_inner_ && orig_data_table_schema_->is_in_recyclebin())) {
    ret = OB_ERR_OPERATION_ON_RECYCLE_OBJECT;
    LOG_WARN("can not add index on table in recyclebin", KR(ret), K_(arg));
  // There used to be check_restore_point_allow() here, but it is currently useless.
  // Meanwhile, the frequent addition and deletion of __all_acquired_snapshot during the index creation
  // will cause it's buffer table to bloat,
  // resulting in performance decline during long periods of concentrated index building.
  } else if (!orig_data_table_schema_->check_can_do_ddl()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "execute ddl while table is executing offline ddl");
    LOG_WARN("offline ddl is being executed, other ddl operations are not allowed", KR(ret), KPC(orig_data_table_schema_));
  } else if (OB_UNLIKELY(orig_data_table_schema_->get_index_tid_count() >= OB_MAX_AUX_TABLE_PER_MAIN_TABLE
                         || orig_data_table_schema_->get_index_count() >= OB_MAX_INDEX_PER_TABLE)) {
    ret = OB_ERR_TOO_MANY_KEYS;
    LOG_USER_ERROR(OB_ERR_TOO_MANY_KEYS, OB_MAX_INDEX_PER_TABLE);
    LOG_WARN("too many index for table", KR(ret), K(OB_MAX_INDEX_PER_TABLE), K(orig_data_table_schema_->get_index_count()));
  } else if (OB_FAIL(check_table_udt_exist_(*orig_data_table_schema_))) {
    LOG_WARN("fail to check table udt exist", KR(ret));
  } else if (OB_FAIL(check_fk_related_table_ddl_(*orig_data_table_schema_, ObDDLType::DDL_CREATE_INDEX))) {
    LOG_WARN("check whether the forign key related table is executing ddl failed", KR(ret));
  }
  RS_TRACE(check_schemas);
  return ret;
}

int ObCreateIndexHelper::is_local_generate_schema_(bool &is_local_generate)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(orig_data_table_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("orig data table schema is nullptr", KR(ret));
  } else if (INDEX_TYPE_NORMAL_LOCAL == arg_.index_type_
      || INDEX_TYPE_UNIQUE_LOCAL == arg_.index_type_
      || INDEX_TYPE_DOMAIN_CTXCAT_DEPRECATED == arg_.index_type_
      || INDEX_TYPE_SPATIAL_LOCAL == arg_.index_type_
      || is_fts_index(arg_.index_type_)
      || is_multivalue_index(arg_.index_type_)) {
    is_local_generate = true;
  } else if (INDEX_TYPE_NORMAL_GLOBAL == arg_.index_type_
             || INDEX_TYPE_UNIQUE_GLOBAL == arg_.index_type_
             || INDEX_TYPE_SPATIAL_GLOBAL == arg_.index_type_) {
    if (!orig_data_table_schema_->is_partitioned_table() && !arg_.index_schema_.is_partitioned_table() && !orig_data_table_schema_->is_auto_partitioned_table()) {
      is_local_generate = true;
    } else {
      is_local_generate = false;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected index type", KR(ret), K_(arg_.index_type));
  }
  return ret;
}

int ObCreateIndexHelper::generate_index_schema_()
{
  int ret = OB_SUCCESS;
  bool is_local_generate = false;
  bool global_index_without_column_info = false;
  ObTableSchema *index_schema = nullptr;
  void *new_arg_ptr = nullptr;
  // not used when is global generate
  HEAP_VAR(ObTableSchema, tmp_index_schema){
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_ISNULL(new_arg_ptr = allocator_.alloc(sizeof(obrpc::ObCreateIndexArg)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail alloc memory", KR(ret), KP(new_arg_ptr));
  } else if (FALSE_IT(new_arg_ = new (new_arg_ptr)obrpc::ObCreateIndexArg)) {
  } else if (OB_ISNULL(new_arg_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("new_arg_ is null", KR(ret));
  } else if (OB_FAIL(new_arg_->assign(arg_))) {
    LOG_WARN("fail to assign arg", KR(ret));
  } else if (OB_UNLIKELY(!new_arg_->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("new arg invalid", KR(ret), KP_(new_arg));
  } else if (OB_FAIL(is_local_generate_schema_(is_local_generate))) {
    LOG_WARN("fail to check is local generate schema", KR(ret));
  } else if (OB_ISNULL(orig_data_table_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data table schema is nullptr", KR(ret));
  } else if (OB_FAIL(ObSchemaUtils::alloc_schema(allocator_, *orig_data_table_schema_, new_data_table_schema_))) {
    LOG_WARN("fail to allocate new table schema", KR(ret), K_(tenant_id));
  } else if (OB_ISNULL(new_data_table_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("new_data_table_schema_ is null", KR(ret));
  } else if (is_local_generate) {
    global_index_without_column_info = true;
    index_schema = &tmp_index_schema;
    if (INDEX_TYPE_NORMAL_GLOBAL == new_arg_->index_type_) {
      new_arg_->index_type_ = INDEX_TYPE_NORMAL_GLOBAL_LOCAL_STORAGE;
      new_arg_->index_schema_.set_index_type(INDEX_TYPE_NORMAL_GLOBAL_LOCAL_STORAGE);
    } else if (INDEX_TYPE_UNIQUE_GLOBAL == new_arg_->index_type_) {
      new_arg_->index_type_ = INDEX_TYPE_UNIQUE_GLOBAL_LOCAL_STORAGE;
      new_arg_->index_schema_.set_index_type(INDEX_TYPE_UNIQUE_GLOBAL_LOCAL_STORAGE);
    } else if (INDEX_TYPE_SPATIAL_GLOBAL == new_arg_->index_type_) {
      new_arg_->index_type_ = INDEX_TYPE_SPATIAL_GLOBAL_LOCAL_STORAGE;
      new_arg_->index_schema_.set_index_type(INDEX_TYPE_SPATIAL_GLOBAL_LOCAL_STORAGE);
    }
  } else {
    global_index_without_column_info = false;
    index_schema = &new_arg_->index_schema_;
  }

  if (FAILEDx(ObIndexBuilderUtil::adjust_expr_index_args(
      *new_arg_, *new_data_table_schema_, allocator_, gen_columns_))) {
    LOG_WARN("fail to adjust expr index args", KR(ret));
  } else if (OB_ISNULL(index_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index schema is null", KR(ret));
  } else if (OB_FAIL(index_builder_.generate_schema(*new_arg_, *new_data_table_schema_, global_index_without_column_info,
              false/*generate_id*/, *index_schema))) {
    LOG_WARN("fail to generate schema", KR(ret));
  } else if (gen_columns_.empty() || is_local_generate) {
    if (OB_FAIL(new_data_table_schema_->check_create_index_on_hidden_primary_key(*index_schema))) {
      LOG_WARN("fail to check create index on hidden primary key", KR(ret), KPC(index_schema));
    }
  }
  bool is_oracle_mode = false;
  if (FAILEDx(ObCompatModeGetter::check_is_oracle_mode_with_tenant_id(
              tenant_id_, is_oracle_mode))) {
    LOG_WARN("fail to check is oracle mode", KR(ret));
  } else if (OB_FAIL(index_schema->generate_origin_index_name())) {
    LOG_WARN("fail to generate origin index name", KR(ret), KPC(index_schema));
  } else if (!is_oracle_mode) {
    ObIndexSchemaInfo index_info;
    if (OB_FAIL(latest_schema_guard_.get_coded_index_name_info_mysql(
                allocator_,
                database_id_,
                orig_data_table_schema_->get_table_id(),
                index_schema->get_table_name(),
                false /*is built in index*/,
                index_info))) {
      LOG_WARN("fail to get index info", KR(ret), K(database_id_), K(index_schema->get_table_name()));
    } else if (index_info.is_valid()) {
      if (arg_.if_not_exist_) {
          // executor rely on invalid index id when arg_.if_not_exists_
          res_.schema_version_ = index_info.get_schema_version();
      }
      ret = OB_ERR_KEY_NAME_DUPLICATE;
      LOG_WARN("duplicate index name", KR(ret), K_(tenant_id),
              "database_id", orig_data_table_schema_->get_database_id(),
              "data_table_id", orig_data_table_schema_->get_table_id(),
              "index_name", arg_.index_name_);
    }
  } else {
    bool name_exist = false;
    if (OB_FAIL(ddl_service_->get_index_name_checker().check_index_name_exist(tenant_id_,
                                                                              index_schema->get_database_id(),
                                                                              index_schema->get_table_name_str(),
                                                                              name_exist))) {
    LOG_WARN("fail to check index name exist", KR(ret), K_(tenant_id), K(index_schema->get_table_name_str()));
    } else if (name_exist) {
      ret = OB_ERR_KEY_NAME_DUPLICATE;
      LOG_WARN("duplicate index name", KR(ret), K_(tenant_id),
              "database_id", orig_data_table_schema_->get_database_id(),
              "data_table_id", orig_data_table_schema_->get_table_id(),
              "index_name", arg_.index_name_);
    }
  }
  uint64_t object_id = OB_INVALID_ID;
  ObIDGenerator id_generator;
  if (FAILEDx(index_schemas_.push_back(*index_schema))) {
    LOG_WARN("fail to push back index schema", KR(ret));
  } else if (OB_FAIL(gen_partition_object_and_tablet_ids_(index_schemas_))) {
    LOG_WARN("fail to gen partition object and tablet ids", KR(ret));
  } else if (OB_FAIL(gen_object_ids_(1/*object_cnt*/, id_generator))) {
    LOG_WARN("fail to gen object ids", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(id_generator.next(object_id))) {
    LOG_WARN("fail to get next object id", KR(ret));
  } else {
    index_schemas_.at(0).set_table_id(object_id);
  }
  } // end heapvar
  RS_TRACE(generate_schemas);
  return ret;
}


int ObCreateIndexHelper::calc_schema_version_cnt_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_ISNULL(orig_data_table_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("orig_data_table_schema is null", KR(ret));
  } else {
    schema_version_cnt_ = 0;
    if (!gen_columns_.empty()) {
      // gen columns & alter table option
      schema_version_cnt_++;
      uint64_t table_id = orig_data_table_schema_->get_table_id();
      const ObIArray<ObForeignKeyInfo> &foreign_key_infos = orig_data_table_schema_->get_foreign_key_infos();
      for (int64_t i = 0; OB_SUCC(ret) && i < foreign_key_infos.count(); i++) {
        const ObForeignKeyInfo &foreign_key_info = foreign_key_infos.at(i);
        const uint64_t related_table_id = table_id == foreign_key_info.parent_table_id_
            ? foreign_key_info.child_table_id_
            : foreign_key_info.parent_table_id_;
        if (related_table_id == table_id) {
          schema_version_cnt_++;
        } else if (foreign_key_info.is_parent_table_mock_) {
          schema_version_cnt_++; // for now, when parent table is mock
          schema_version_cnt_++; // update table option will push schema version twice
        } else {
          schema_version_cnt_++;
        }
      }
    }
    // index table
    schema_version_cnt_++;
    // data table
    schema_version_cnt_++;
    // 1503
    schema_version_cnt_++;
  }
  return ret;
}

int ObCreateIndexHelper::create_index_()
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service_impl = nullptr;
  int64_t new_schema_version = OB_INVALID_VERSION;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check iner stat", KR(ret));
  } else if (OB_ISNULL(schema_service_impl = schema_service_->get_schema_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service impl is null", KR(ret));
  } else if (OB_UNLIKELY(index_schemas_.count() != 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index schemas count not expected", KR(ret), K(index_schemas_.count()));
  } else if (OB_ISNULL(new_data_table_schema_) || OB_ISNULL(orig_data_table_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("new/orig table schema is nullptr", KR(ret), KP(new_data_table_schema_), KP(orig_data_table_schema_));
  } else {
    ObDDLOperator ddl_operator(*schema_service_, *sql_proxy_);
    ObTableSchema &index_schema = index_schemas_.at(0);
    if (!gen_columns_.empty()) {
      if (OB_FAIL(schema_service_->gen_new_schema_version(tenant_id_, new_schema_version))) {
        LOG_WARN("fail to gen new schema_version", KR(ret), K_(tenant_id));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < gen_columns_.count(); ++i) {
        ObColumnSchemaV2 *new_column_schema = gen_columns_.at(i);
        if (OB_ISNULL(new_column_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("new column schema is null", KR(ret));
        } else if (FALSE_IT(new_column_schema->set_schema_version(new_schema_version))) {
        } else if (OB_FAIL(schema_service_impl->get_table_sql_service().insert_single_column(
                   trans_, *new_data_table_schema_, *new_column_schema, false/*record_ddl_option*/))) {
          LOG_WARN("insert single column failed", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (FALSE_IT(new_data_table_schema_->set_schema_version(new_schema_version))) {
      } else if (OB_FAIL(schema_service_impl->get_table_sql_service().update_table_options(
                trans_, *orig_data_table_schema_, *new_data_table_schema_, OB_DDL_ALTER_TABLE))) {
        LOG_WARN("fail to alter table option", KR(ret));
      }
    }
    uint64_t tenant_data_version = OB_INVALID_VERSION;
    if (FAILEDx(create_table_())) {
      LOG_WARN("fail to create table", KR(ret));
    } else if (index_schema.has_tablet() && OB_FAIL(create_tablets_())) {
      LOG_WARN("fail to create tablets", KR(ret));
    } else if (OB_ISNULL(new_arg_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("new arg is null", KR(ret));
    } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id_, tenant_data_version))) {
      LOG_WARN("fail to get data version", K(ret), K_(tenant_id));
    } else if (tenant_data_version < DATA_VERSION_4_3_5_1
        && (share::schema::is_fts_index_aux(new_arg_->index_type_) || share::schema::is_fts_doc_word_aux(new_arg_->index_type_))
        && !new_arg_->index_option_.parser_properties_.empty()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("parser properties isn't supported before version 4.3.5.1", K(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "parser properties before version 4.3.5.1 is");
    } else if (OB_FAIL(index_builder_.submit_build_index_task(trans_,
                                                              *new_arg_,
                                                              new_data_table_schema_,
                                                              nullptr/*inc_data_tablet_ids*/,
                                                              nullptr/*del_data_tablet_ids*/,
                                                              &index_schema,
                                                              arg_.parallelism_,
                                                              arg_.consumer_group_id_,
                                                              tenant_data_version,
                                                              allocator_,
                                                              task_record_))) {
      LOG_WARN("fail to submit build local index task", KR(ret));
    }
  }
  RS_TRACE(submit_task);
  return ret;
}

int ObCreateIndexHelper::create_table_()
{
  int ret = OB_SUCCESS;
  int64_t new_schema_version = OB_INVALID_VERSION;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(index_schemas_.count() != 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index schemas count not expected", KR(ret));
  } else {
    ObDDLOperator ddl_operator(*schema_service_, *sql_proxy_);
    ObSchemaService *schema_service_impl = schema_service_->get_schema_service();
    int64_t last_schema_version = OB_INVALID_VERSION;
    ObSchemaVersionGenerator *tsi_generator = GET_TSI(TSISchemaVersionGenerator);
    ObTableSchema &index_schema = index_schemas_.at(0);
    if (OB_ISNULL(tsi_generator)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tsi generator is null", KR(ret));
    } else if (OB_ISNULL(schema_service_impl)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema service must not by null", KR(ret));
    } else if (OB_FAIL(schema_service_->gen_new_schema_version(tenant_id_, new_schema_version))) {
      LOG_WARN("fail to gen new schema version", KR(ret), K_(tenant_id));
    } else if (FALSE_IT(index_schema.set_schema_version(new_schema_version))) {
    } else if (OB_FAIL(schema_service_impl->get_table_sql_service().create_table(
              index_schema, trans_,
              nullptr/*ddl_stmt_str*/, true/*need_sync_schema_version*/, false/*is_truncate_table*/))) {
      LOG_WARN("fail to create table", KR(ret));
    } else if (OB_FAIL(schema_service_impl->get_table_sql_service().insert_temp_table_info(trans_, index_schema))) {
      LOG_WARN("insert temp table info", KR(ret), K(index_schema));
    } else if (OB_FAIL(tsi_generator->get_current_version(last_schema_version))) {
      LOG_WARN("fail to get end version", KR(ret), K_(tenant_id), K_(arg));
    } else if (OB_UNLIKELY(last_schema_version <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("last schema version is invalid", KR(ret), K_(tenant_id), K(last_schema_version));
    } else if (OB_FAIL(ddl_operator.insert_ori_schema_version(trans_, tenant_id_, index_schema.get_table_id(), last_schema_version))) {
      LOG_WARN("fail to insert ori schema version", KR(ret), K_(tenant_id), K(new_schema_version));
    }
  }
  RS_TRACE(create_schemas);
  return ret;
}

int ObCreateIndexHelper::create_tablets_()
{
  int ret = OB_SUCCESS;
  SCN frozen_scn;
  ObSchemaGetterGuard schema_guard;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check inner stat failed", KR(ret));
  } else if (OB_UNLIKELY(index_schemas_.count() != 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index schemas count not expected", KR(ret), K(index_schemas_.count()));
  } else if (OB_ISNULL(orig_data_table_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("orig_data_table_schema is null", KR(ret));
  } else if(OB_FAIL(ObMajorFreezeHelper::get_frozen_scn(tenant_id_, frozen_scn))) {
    LOG_WARN("fail to get frozen status for create tablet", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", KR(ret), K_(tenant_id));
  } else {
    ObTableSchema &index_schema = index_schemas_.at(0);
    ObTableCreator table_creator(tenant_id_, frozen_scn, trans_);
    ObNewTableTabletAllocator new_table_tablet_allocator(
                              tenant_id_,
                              schema_guard,
                              sql_proxy_,
                              true /*use parallel ddl*/,
                              orig_data_table_schema_);
    common::ObArray<share::ObLSID> ls_id_array;
    const ObTablegroupSchema *data_tablegroup_schema = NULL; // keep NULL if no tablegroup
    ObSEArray<bool, 1> need_create_empty_majors;
    uint64_t tenant_data_version = 0;
    if (OB_FAIL(table_creator.init(true/*need_tablet_cnt_check*/))) {
      LOG_WARN("fail to init table craetor", KR(ret));
    } else if (OB_FAIL(new_table_tablet_allocator.init())) {
      LOG_WARN("fail to init new table tablet allocator", KR(ret));
    } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id_, tenant_data_version))) {
      LOG_WARN("fail to get data version", K(ret), K_(tenant_id));
    } else if (OB_INVALID_ID != orig_data_table_schema_->get_tablegroup_id()) {
      if (OB_FAIL(latest_schema_guard_.get_tablegroup_schema(
          orig_data_table_schema_->get_tablegroup_id(),
          data_tablegroup_schema))) {
        LOG_WARN("get tablegroup_schema failed", KR(ret), KPC(orig_data_table_schema_));
      } else if (OB_ISNULL(data_tablegroup_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("data_tablegroup_schema is null", KR(ret), KPC(orig_data_table_schema_));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (index_schema.is_index_local_storage()) {
      ObSEArray<const share::schema::ObTableSchema*, 1> schemas;
      if (OB_FAIL(schemas.push_back(&index_schema))
          || OB_FAIL(need_create_empty_majors.push_back(false))) {
        LOG_WARN("fail to push back index schema", KR(ret), K(index_schema));
      } else if (OB_FAIL(new_table_tablet_allocator.prepare(trans_, index_schema, data_tablegroup_schema))) {
        LOG_WARN("fail to prepare ls for index schema tablets", KR(ret));
      } else if (OB_FAIL(new_table_tablet_allocator.get_ls_id_array(ls_id_array))) {
        LOG_WARN("fail to get ls id array", KR(ret));
      } else if (OB_FAIL(table_creator.add_create_tablets_of_local_aux_tables_arg(
                                       schemas,
                                       orig_data_table_schema_,
                                       ls_id_array,
                                       tenant_data_version,
                                       need_create_empty_majors))) {
        LOG_WARN("create table tablet failed", KR(ret), K(index_schema), K(ls_id_array));
      }
    } else {
      if (OB_FAIL(new_table_tablet_allocator.prepare(trans_, index_schema, data_tablegroup_schema))) {
        LOG_WARN("fail to prepare ls for index schema tablets", KR(ret));
      } else if (OB_FAIL(new_table_tablet_allocator.get_ls_id_array(ls_id_array))) {
        LOG_WARN("fail to get ls id array", KR(ret));
      } else if (OB_FAIL(table_creator.add_create_tablets_of_table_arg(index_schema, ls_id_array,
                                       tenant_data_version, false/*need create major sstable*/))) {
        LOG_WARN("create table tablet failed", KR(ret), K(index_schema));
      }
    }
    if (FAILEDx(table_creator.execute())) {
      LOG_WARN("execute create partition failed", KR(ret));
    }

    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = new_table_tablet_allocator.finish(OB_SUCCESS == ret))) {
      LOG_WARN("fail to finish new table tablet allocator", KR(tmp_ret));
    }
  }
  RS_TRACE(create_tablets);
  return ret;
}

int ObCreateIndexHelper::add_index_name_to_cache_()
{
  int ret = OB_SUCCESS;
  ObIndexNameChecker &checker = ddl_service_->get_index_name_checker();
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(index_schemas_.count() != 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index schemas count not expected", KR(ret), K(index_schemas_.count()));
  } else {
    ObTableSchema &index_schema = index_schemas_.at(0);
    if (OB_FAIL(checker.add_index_name(index_schema))) {
      LOG_WARN("fail to add index name", KR(ret), K(index_schema));
    }
  }
  return ret;
}

// self ref table no need to check
// for mock fk table, should check it is exist
// for fk table, should check not doing long ddl
int ObCreateIndexHelper::check_fk_related_table_ddl_(const share::schema::ObTableSchema &data_table_schema,
                                                     const share::ObDDLType &ddl_type)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id_ || share::ObDDLType::DDL_INVALID == ddl_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K_(tenant_id), K(ddl_type));
  } else {
    const ObIArray<ObForeignKeyInfo> &foreign_key_infos = data_table_schema.get_foreign_key_infos();
    const ObCheckExistedDDLMode check_mode = is_double_table_long_running_ddl(ddl_type) ?
          ObCheckExistedDDLMode::ALL_LONG_RUNNING_DDL : ObCheckExistedDDLMode::DOUBLE_TABLE_RUNNING_DDL;
    for (int64_t i = 0; OB_SUCC(ret) && i < foreign_key_infos.count(); ++i) {
      const ObForeignKeyInfo &foreign_key_info = foreign_key_infos.at(i);
      const uint64_t related_table_id = data_table_schema.get_table_id() == foreign_key_info.parent_table_id_?
                                        foreign_key_info.child_table_id_ : foreign_key_info.parent_table_id_;
      if (data_table_schema.get_table_id() == related_table_id) {
        // self no need to check
      } else if (foreign_key_info.is_parent_table_mock_) {
        const ObMockFKParentTableSchema *related_schema = nullptr;
        if (OB_FAIL(latest_schema_guard_.get_mock_fk_parent_table_schema(related_table_id, related_schema))) {
          LOG_WARN("fail to get related mock fk parent table schema", KR(ret), K_(tenant_id), K(related_table_id));
        } else if (OB_ISNULL(related_schema)) {
          ret = OB_ERR_PARALLEL_DDL_CONFLICT;
          LOG_WARN("mock fk parent table schema is null ptr, may be dropped", KR(ret), K_(tenant_id), K(related_table_id), K(foreign_key_info));
        }
      } else {
        const ObTableSchema *related_schema = nullptr;
        bool has_long_running_ddl = false;
        if (OB_FAIL(latest_schema_guard_.get_table_schema(related_table_id, related_schema))) {
          LOG_WARN("fail to get related table schema", KR(ret), K_(tenant_id), K(related_table_id));
        } else if (OB_ISNULL(related_schema)) {
          ret = OB_ERR_PARALLEL_DDL_CONFLICT;
          LOG_WARN("related schema is null ptr, may be dropped", KR(ret), K_(tenant_id), K(related_table_id), K(foreign_key_info));
        } else if (!related_schema->check_can_do_ddl()) {
          ret = OB_OP_NOT_ALLOW;
          LOG_USER_ERROR(OB_OP_NOT_ALLOW, "execute ddl while foreign key related table is executing long running ddl");
        } else if (OB_FAIL(ObDDLTaskRecordOperator::check_has_long_running_ddl(sql_proxy_,
                                                                              tenant_id_,
                                                                              related_table_id,
                                                                              check_mode,
                                                                              has_long_running_ddl))) {
          LOG_WARN("check has long running ddl failed", KR(ret), K_(tenant_id), K(related_table_id));
        } else if (has_long_running_ddl) {
          ret = OB_OP_NOT_ALLOW;
          LOG_WARN("foreign key related table is executing offline ddl", KR(ret), K(check_mode), K_(tenant_id), K(related_table_id));
          LOG_USER_ERROR(OB_OP_NOT_ALLOW, "execute ddl while foreign key related table is executing long running ddl");
        }
      }
    }
  }
  return ret;
}
