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
#include "ob_index_builder.h"

#include "share/ob_define.h"
#include "lib/allocator/page_arena.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/container/ob_array_iterator.h"
#include "share/ob_srv_rpc_proxy.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/ob_ddl_common.h"
#include "share/ob_debug_sync.h"
#include "share/ob_common_rpc_proxy.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_part_mgr_util.h"
#include "share/ob_common_rpc_proxy.h"
#include "share/config/ob_server_config.h"
#include "share/ob_index_builder_util.h"
#include "observer/ob_server_struct.h"
#include "sql/resolver/ddl/ob_ddl_resolver.h"
#include "ob_zone_manager.h"
#include "ob_ddl_service.h"
#include "ob_root_service.h"
#include "ob_snapshot_info_manager.h"
#include "share/ob_thread_mgr.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "storage/ddl/ob_ddl_lock.h"
#include "storage/tx/ob_ts_mgr.h"
#include "storage/tx/ob_i_ts_source.h"
#include "storage/tx/ob_ts_mgr.h"
#include "storage/tx/ob_i_ts_source.h"
#include <map>
#include "rootserver/ddl_task/ob_ddl_scheduler.h"
#include "rootserver/ddl_task/ob_ddl_task.h"
#include "share/scn.h"

namespace oceanbase
{
using namespace common;
using namespace common::sqlclient;
using namespace obrpc;
using namespace share;
using namespace share::schema;
using namespace sql;
using namespace palf;
namespace rootserver
{

ObIndexBuilder::ObIndexBuilder(ObDDLService &ddl_service)
  : ddl_service_(ddl_service)
{
}

ObIndexBuilder::~ObIndexBuilder()
{
}

int ObIndexBuilder::create_index(
    const ObCreateIndexArg &arg,
    obrpc::ObAlterTableRes &res)
{
  int ret = OB_SUCCESS;
  LOG_INFO("start create index", K(arg));
  if (!ddl_service_.is_inited()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("ddl_service not init", "ddl_service inited", ddl_service_.is_inited(), K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(do_create_index(arg, res))) {
    LOG_WARN("generate_schema failed", K(arg), K(ret));
  }
  if (OB_ERR_TABLE_EXIST == ret) {
    if (true == arg.if_not_exist_) {
      ret = OB_SUCCESS;
      LOG_USER_WARN(OB_ERR_KEY_NAME_DUPLICATE, arg.index_name_.length(), arg.index_name_.ptr());
    } else {
      ret = OB_ERR_KEY_NAME_DUPLICATE;
      LOG_USER_ERROR(OB_ERR_KEY_NAME_DUPLICATE, arg.index_name_.length(), arg.index_name_.ptr());
    }
  }
  LOG_INFO("finish create index", K(arg), K(ret));
  return ret;
}

int ObIndexBuilder::drop_index(const ObDropIndexArg &arg, obrpc::ObDropIndexRes &res)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = arg.tenant_id_;

  const bool is_index = false;
  ObArenaAllocator allocator(ObModIds::OB_SCHEMA);
  const ObTableSchema *table_schema = NULL;
  ObSchemaGetterGuard schema_guard;
  bool is_db_in_recyclebin = false;
  schema_guard.set_session_id(arg.session_id_);
  if (!ddl_service_.is_inited()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("ddl_service not init", "ddl_service inited", ddl_service_.is_inited(), K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(ddl_service_.get_tenant_schema_guard_with_version_in_inner_table(tenant_id, schema_guard))) {
    LOG_WARN("get_schema_guard failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(
                         tenant_id, arg.database_name_, arg.table_name_,
                         is_index, table_schema, arg.is_hidden_))) {
    LOG_WARN("failed to get data table schema", K(arg), K(ret));
  } else if (NULL == table_schema) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_USER_ERROR(OB_TABLE_NOT_EXIST, to_cstring(arg.database_name_), to_cstring(arg.table_name_));
    LOG_WARN("table not found", K(arg), K(ret));
  } else if (arg.is_in_recyclebin_) {
    // internal delete index
  } else if (table_schema->is_in_recyclebin()) {
    ret = OB_ERR_OPERATION_ON_RECYCLE_OBJECT;
    LOG_WARN("can not drop index of table in recyclebin.", K(ret), K(arg));
  } else if (OB_FAIL(schema_guard.check_database_in_recyclebin(tenant_id,
            table_schema->get_database_id(), is_db_in_recyclebin))) {
    LOG_WARN("check database in recyclebin failed", K(ret), K(tenant_id));
  } else if (is_db_in_recyclebin) {
    ret = OB_ERR_OPERATION_ON_RECYCLE_OBJECT;
    LOG_WARN("Can not drop index of db in recyclebin", K(ret), K(arg));
  } else if (OB_FAIL(ddl_service_.check_fk_related_table_ddl(*table_schema, ObDDLType::DDL_DROP_INDEX))) {
    LOG_WARN("check whether foreign key related table executes ddl failed", K(ret));
  }
  if (OB_SUCC(ret)) {
    const uint64_t data_table_id = table_schema->get_table_id();
    const ObTableSchema *index_table_schema = NULL;
    if (OB_INVALID_ID != arg.index_table_id_) {
      LOG_DEBUG("drop index with index_table_id", K(arg.index_table_id_));
      if (OB_FAIL(schema_guard.get_table_schema(tenant_id, arg.index_table_id_, index_table_schema))) {
        LOG_WARN("fail to get index table schema", K(ret), K(tenant_id), K(arg.index_table_id_));
      }
    } else {
      ObString index_table_name;
      if (OB_FAIL(ObTableSchema::build_index_table_name(
        allocator, data_table_id, arg.index_name_, index_table_name))) {
      LOG_WARN("build_index_table_name failed", K(arg), K(data_table_id), K(ret));
      } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id,
              table_schema->get_database_id(),
              index_table_name,
              true,
              index_table_schema))) {
        LOG_WARN("fail to get table schema", K(ret), K(tenant_id), K(index_table_schema));
      }
    }
    bool have_index = false;
    const common::ObIArray<ObForeignKeyInfo> &foreign_key_infos = table_schema->get_foreign_key_infos();
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(index_table_schema)) {
      ret = OB_ERR_CANT_DROP_FIELD_OR_KEY;
      LOG_WARN("index table schema should not be null", K(arg.index_name_), K(ret));
      LOG_USER_ERROR(OB_ERR_CANT_DROP_FIELD_OR_KEY, arg.index_name_.length(), arg.index_name_.ptr());
    } else if (OB_FAIL(ddl_service_.check_index_on_foreign_key(index_table_schema,
                                                               foreign_key_infos,
                                                               have_index))) {
      LOG_WARN("fail to check index on foreign key", K(ret), K(foreign_key_infos), KPC(index_table_schema));
    } else if (have_index) {
      ret = OB_ERR_ATLER_TABLE_ILLEGAL_FK;
      LOG_WARN("cannot delete index with foreign key dependency", K(ret));
    } else if (!arg.is_inner_ && index_table_schema->is_unavailable_index()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not support to drop a building index", K(ret), K(arg.is_inner_), KPC(index_table_schema));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "dropping a building index is");
    } else if (arg.is_add_to_scheduler_) {
      ObDDLOperator ddl_operator(ddl_service_.get_schema_service(), ddl_service_.get_sql_proxy());
      ObDDLSQLTransaction trans(&ddl_service_.get_schema_service());
      int64_t refreshed_schema_version = 0;
      ObArenaAllocator allocator(lib::ObLabel("DdlTaskTmp"));
      ObDDLTaskRecord task_record;
      bool has_index_task = false;
      SMART_VAR(ObTableSchema, new_index_schema) {
        if (OB_FAIL(schema_guard.get_schema_version(tenant_id, refreshed_schema_version))) {
          LOG_WARN("failed to get tenant schema version", KR(ret), K(tenant_id));
        } else if (OB_FAIL(trans.start(&ddl_service_.get_sql_proxy(), tenant_id, refreshed_schema_version))) {
          LOG_WARN("start transaction failed", KR(ret), K(tenant_id), K(refreshed_schema_version));
        } else if (!arg.is_inner_ && !index_table_schema->can_read_index() && OB_FAIL(ObDDLTaskRecordOperator::check_has_index_task(
              trans, tenant_id, data_table_id, index_table_schema->get_table_id(), has_index_task))) {
          LOG_WARN("failed to check ddl conflict", K(ret));
        } else if (has_index_task) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("not support to drop a building or dropping index", K(ret), K(arg.is_inner_), KPC(index_table_schema));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "dropping a building or dropping index is");
        } else if (OB_FAIL(ddl_service_.rename_dropping_index_name(
                                                      table_schema->get_table_id(),
                                                      table_schema->get_database_id(),
                                                      arg,
                                                      schema_guard,
                                                      ddl_operator,
                                                      trans,
                                                      new_index_schema))) {
          LOG_WARN("renmae index name failed", K(ret));
        } else if (OB_FAIL(submit_drop_index_task(trans, *table_schema, new_index_schema, new_index_schema.get_schema_version(), arg, allocator, task_record))) {
          LOG_WARN("submit drop index task failed", K(ret));
        } else {
          res.tenant_id_ = new_index_schema.get_tenant_id();
          res.index_table_id_ = new_index_schema.get_table_id();
          res.schema_version_ = new_index_schema.get_schema_version();
          res.task_id_ = task_record.task_id_;
        }
      }
      if (trans.is_started()) {
        int temp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (temp_ret = trans.end(OB_SUCC(ret)))) {
          LOG_WARN_RET(temp_ret, "trans end failed", "is_commit", OB_SUCCESS == ret, K(temp_ret));
          ret = (OB_SUCC(ret)) ? temp_ret : ret;
        }
      }
      if (OB_SUCC(ret)) {
        int tmp_ret = OB_SUCCESS;
        if (OB_FAIL(ddl_service_.publish_schema(tenant_id))) {
          LOG_WARN("fail to publish schema", K(ret), K(tenant_id));
        } else if (OB_TMP_FAIL(GCTX.root_service_->get_ddl_task_scheduler().schedule_ddl_task(task_record))) {
          LOG_WARN("fail to schedule ddl task", K(tmp_ret), K(task_record));
        }
      }
    } else {
      //construct an arg for drop table
      ObTableItem table_item;
      table_item.database_name_ = arg.database_name_;
      table_item.table_name_ = index_table_schema->get_table_name();
      table_item.is_hidden_ = index_table_schema->is_user_hidden_table();
      obrpc::ObDDLRes ddl_res;
      obrpc::ObDropTableArg drop_table_arg;
      drop_table_arg.tenant_id_ = tenant_id;
      drop_table_arg.if_exist_ = false;
      drop_table_arg.table_type_ = USER_INDEX;
      drop_table_arg.ddl_stmt_str_ = arg.ddl_stmt_str_;
      drop_table_arg.force_drop_ = arg.is_in_recyclebin_;
      drop_table_arg.task_id_ = arg.task_id_;
      if (OB_FAIL(drop_table_arg.tables_.push_back(table_item))) {
        LOG_WARN("failed to add table item!", K(table_item), K(ret));
      } else if (OB_FAIL(ddl_service_.drop_table(drop_table_arg, ddl_res))) {
        if (OB_TABLE_NOT_EXIST == ret) {
          ret = OB_ERR_CANT_DROP_FIELD_OR_KEY;
          LOG_USER_ERROR(OB_ERR_CANT_DROP_FIELD_OR_KEY, arg.index_name_.length(), arg.index_name_.ptr());
          LOG_WARN("index not exist, can't drop it", K(arg), K(ret));
        } else {
          LOG_WARN("drop_table failed", K(arg), K(drop_table_arg), K(ret));
        }
      }
    }
  }

  LOG_INFO("finish drop index", K(arg), K(ret));
  return ret;
}

int ObIndexBuilder::do_create_global_index(
    share::schema::ObSchemaGetterGuard &schema_guard,
    const obrpc::ObCreateIndexArg &arg,
    const share::schema::ObTableSchema &table_schema,
    obrpc::ObAlterTableRes &res)
{
  int ret = OB_SUCCESS;
  ObArray<ObColumnSchemaV2 *> gen_columns;
  const bool global_index_without_column_info = false;
  ObDDLTaskRecord task_record;
  ObArenaAllocator allocator(lib::ObLabel("DdlTaskTmp"));
  HEAP_VARS_2((ObTableSchema, new_table_schema),
              (obrpc::ObCreateIndexArg, new_arg)) {
    ObTableSchema &index_schema = new_arg.index_schema_;
    ObDDLSQLTransaction trans(&ddl_service_.get_schema_service());
    int64_t refreshed_schema_version = 0;
    const uint64_t tenant_id = table_schema.get_tenant_id();
    if (OB_FAIL(schema_guard.get_schema_version(tenant_id, refreshed_schema_version))) {
      LOG_WARN("failed to get tenant schema version", KR(ret), K(tenant_id));
    } else if (OB_FAIL(trans.start(&ddl_service_.get_sql_proxy(), tenant_id, refreshed_schema_version))) {
      LOG_WARN("start transaction failed", KR(ret), K(tenant_id), K(refreshed_schema_version));
    } else if (OB_FAIL(new_table_schema.assign(table_schema))) {
      LOG_WARN("fail to assign schema", K(ret));
    } else if (!new_table_schema.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to copy table schema", K(ret));
    } else if (OB_FAIL(new_arg.assign(arg))) {
      LOG_WARN("fail to assign arg", K(ret));
    } else if (!new_arg.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to copy create index arg", K(ret));
    } else if (OB_FAIL(ObIndexBuilderUtil::adjust_expr_index_args(
            new_arg, new_table_schema, allocator, gen_columns))) {
      LOG_WARN("fail to adjust expr index args", K(ret));
    } else if (OB_FAIL(generate_schema(
        new_arg, new_table_schema, global_index_without_column_info,
        true/*generate_id*/, index_schema))) {
      LOG_WARN("fail to generate schema", K(ret), K(new_arg));
    } else {
      if (gen_columns.empty()) {
        if (OB_FAIL(ddl_service_.create_global_index(
                trans, new_arg, new_table_schema, index_schema))) {
          LOG_WARN("fail to create global index", K(ret));
        }
      } else {
        if (OB_FAIL(ddl_service_.create_global_inner_expr_index(
              trans, table_schema, new_table_schema, gen_columns, index_schema))) {
          LOG_WARN("fail to create global inner expr index", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(submit_build_index_task(trans,
                                                 new_arg,
                                                 &new_table_schema,
                                                 nullptr/*inc_data_tablet_ids*/,
                                                 nullptr/*del_data_tablet_ids*/,
                                                 &index_schema,
                                                 arg.parallelism_,
                                                 allocator,
                                                 task_record,
                                                 arg.consumer_group_id_))) {
        LOG_WARN("fail to submit build global index task", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      res.index_table_id_ = index_schema.get_table_id();
      res.schema_version_ = index_schema.get_schema_version();
      res.task_id_ = task_record.task_id_;
    }
    if (trans.is_started()) {
      int temp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (temp_ret = trans.end(OB_SUCC(ret)))) {
        LOG_WARN("trans end failed", "is_commit", OB_SUCCESS == ret, K(temp_ret));
        ret = (OB_SUCC(ret)) ? temp_ret : ret;
      }
    }
    if (OB_SUCC(ret)) {
      int tmp_ret = OB_SUCCESS;
      if (OB_FAIL(ddl_service_.publish_schema(tenant_id))) {
        LOG_WARN("fail to publish schema", K(ret), K(tenant_id));
      } else if (OB_TMP_FAIL(GCTX.root_service_->get_ddl_task_scheduler().schedule_ddl_task(task_record))) {
        LOG_WARN("fail to schedule ddl task", K(tmp_ret), K(task_record));
      }
    }
  }
  return ret;
}

int ObIndexBuilder::submit_build_index_task(
    ObMySQLTransaction &trans,
    const obrpc::ObCreateIndexArg &create_index_arg,
    const ObTableSchema *data_schema,
    const ObIArray<ObTabletID> *inc_data_tablet_ids,
    const ObIArray<ObTabletID> *del_data_tablet_ids,
    const ObTableSchema *index_schema,
    const int64_t parallelism,
    common::ObIAllocator &allocator,
    ObDDLTaskRecord &task_record,
    const int64_t group_id)
{
  int ret = OB_SUCCESS;
  ObCreateDDLTaskParam param(index_schema->get_tenant_id(),
                             ObDDLType::DDL_CREATE_INDEX,
                             data_schema,
                             index_schema,
                             0/*object_id*/,
                             index_schema->get_schema_version(),
                             parallelism,
                             group_id,
                             &allocator,
                             &create_index_arg);
  if (OB_ISNULL(data_schema) || OB_ISNULL(index_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("schema is invalid", K(ret), K(data_schema), K(index_schema));
  } else if (OB_FAIL(GCTX.root_service_->get_ddl_task_scheduler().create_ddl_task(param, trans, task_record))) {
    LOG_WARN("submit create index ddl task failed", K(ret));
  } else if (OB_FAIL(ObDDLLock::lock_for_add_drop_index(
      *data_schema, inc_data_tablet_ids, del_data_tablet_ids, *index_schema, ObTableLockOwnerID(task_record.task_id_), trans))) {
    LOG_WARN("failed to lock online ddl lock", K(ret));
  }
  return ret;
}

int ObIndexBuilder::submit_drop_index_task(ObMySQLTransaction &trans,
                                           const ObTableSchema &data_schema,
                                           const ObTableSchema &index_schema,
                                           const int64_t schema_version,
                                           const obrpc::ObDropIndexArg &arg,
                                           common::ObIAllocator &allocator,
                                           ObDDLTaskRecord &task_record)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!index_schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret));
  } else {
    int64_t refreshed_schema_version = 0;
    const uint64_t tenant_id = index_schema.get_tenant_id();
    ObCreateDDLTaskParam param(tenant_id,
                               ObDDLType::DDL_DROP_INDEX,
                               &index_schema,
                               nullptr,
                               0/*object_id*/,
                               schema_version,
                               0/*parallelism*/,
                               arg.consumer_group_id_,
                               &allocator,
                               &arg);
    if (OB_FAIL(GCTX.root_service_->get_ddl_task_scheduler().create_ddl_task(param, trans, task_record))) {
      LOG_WARN("submit create index ddl task failed", K(ret));
    } else if (OB_FAIL(ObDDLLock::lock_for_add_drop_index(
        data_schema, nullptr/*inc_data_tablet_ids*/, nullptr/*del_data_tablet_ids*/, index_schema, ObTableLockOwnerID(task_record.task_id_), trans))) {
      LOG_WARN("failed to lock online ddl lock", K(ret));
    }
  }
  return ret;
}

int ObIndexBuilder::do_create_local_index(
    share::schema::ObSchemaGetterGuard &schema_guard,
    const obrpc::ObCreateIndexArg &create_index_arg,
    const share::schema::ObTableSchema &table_schema,
    obrpc::ObAlterTableRes &res)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObColumnSchemaV2 *, 1> gen_columns;
  ObDDLTaskRecord task_record;
  ObArenaAllocator allocator(lib::ObLabel("DdlTaskTmp"));
  HEAP_VARS_3((ObTableSchema, index_schema),
              (ObTableSchema, new_table_schema),
              (obrpc::ObCreateIndexArg, my_arg)) {
    ObDDLSQLTransaction trans(&ddl_service_.get_schema_service());
    int64_t refreshed_schema_version = 0;
    const uint64_t tenant_id = table_schema.get_tenant_id();
    uint64_t tenant_data_version = 0;
    if (OB_FAIL(schema_guard.get_schema_version(tenant_id, refreshed_schema_version))) {
      LOG_WARN("failed to get tenant schema version", KR(ret), K(tenant_id));
    } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, tenant_data_version))) {
    LOG_WARN("get tenant data version failed", K(ret));
    } else if (OB_FAIL(trans.start(&ddl_service_.get_sql_proxy(), tenant_id, refreshed_schema_version))) {
      LOG_WARN("start transaction failed", KR(ret), K(tenant_id), K(refreshed_schema_version));
    } else if (OB_FAIL(new_table_schema.assign(table_schema))) {
      LOG_WARN("fail to assign schema", K(ret));
    } else if (!new_table_schema.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to copy table schema", K(ret));
    } else if (OB_FAIL(my_arg.assign(create_index_arg))) {
      LOG_WARN("fail to assign arg", K(ret));
    } else if (!my_arg.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to copy create index arg", K(ret));
    } else {
      const bool global_index_without_column_info = true;
      // build a global index with local storage if both the data table and index table are non-partitioned
      if (INDEX_TYPE_NORMAL_GLOBAL == my_arg.index_type_) {
        my_arg.index_type_ = INDEX_TYPE_NORMAL_GLOBAL_LOCAL_STORAGE;
        my_arg.index_schema_.set_index_type(INDEX_TYPE_NORMAL_GLOBAL_LOCAL_STORAGE);
      } else if (INDEX_TYPE_UNIQUE_GLOBAL == my_arg.index_type_) {
        my_arg.index_type_ = INDEX_TYPE_UNIQUE_GLOBAL_LOCAL_STORAGE;
        my_arg.index_schema_.set_index_type(INDEX_TYPE_UNIQUE_GLOBAL_LOCAL_STORAGE);
      } else if (INDEX_TYPE_SPATIAL_GLOBAL == my_arg.index_type_) {
        if (tenant_data_version < DATA_VERSION_4_1_0_0) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("tenant data version is less than 4.1, spatial index is not supported", K(ret), K(tenant_data_version));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant data version is less than 4.1, spatial index");
        } else {
          my_arg.index_type_ = INDEX_TYPE_SPATIAL_GLOBAL_LOCAL_STORAGE;
          my_arg.index_schema_.set_index_type(INDEX_TYPE_SPATIAL_GLOBAL_LOCAL_STORAGE);
        }
      }
      if (OB_FAIL(ObIndexBuilderUtil::adjust_expr_index_args(
              my_arg, new_table_schema, allocator, gen_columns))) {
        LOG_WARN("fail to adjust expr index args", K(ret));
      } else if (OB_FAIL(generate_schema(
              my_arg, new_table_schema, global_index_without_column_info,
              true/*generate_id*/, index_schema))) {
        LOG_WARN("fail to generate schema", K(ret), K(my_arg));
      } else if (OB_FAIL(new_table_schema.check_create_index_on_hidden_primary_key(index_schema))) {
        LOG_WARN("failed to check create index on table", K(ret), K(index_schema));
      } else if (gen_columns.empty()) {
        if (OB_FAIL(ddl_service_.create_index_table(my_arg, index_schema, trans))) {
          LOG_WARN("fail to create index", K(ret), K(index_schema));
        }
      } else {
        if (OB_FAIL(ddl_service_.create_inner_expr_index(trans,
                                                         table_schema,
                                                         new_table_schema,
                                                         gen_columns,
                                                         index_schema))) {
          LOG_WARN("fail to create inner expr index", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(submit_build_index_task(trans,
                                                 create_index_arg,
                                                 &new_table_schema,
                                                 nullptr/*inc_data_tablet_ids*/,
                                                 nullptr/*del_data_tablet_ids*/,
                                                 &index_schema,
                                                 create_index_arg.parallelism_,
                                                 allocator,
                                                 task_record,
                                                 create_index_arg.consumer_group_id_))) {
        LOG_WARN("failt to submit build local index task", K(ret));
      } else {
        res.index_table_id_ = index_schema.get_table_id();
        res.schema_version_ = index_schema.get_schema_version();
        res.task_id_ = task_record.task_id_;
      }
    }
    if (trans.is_started()) {
      int temp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (temp_ret = trans.end(OB_SUCC(ret)))) {
        LOG_WARN("trans end failed", "is_commit", OB_SUCCESS == ret, K(temp_ret));
        ret = (OB_SUCC(ret)) ? temp_ret : ret;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ddl_service_.publish_schema(tenant_id))) {
        LOG_WARN("fail to publish schema", K(ret), K(tenant_id));
      } else if (OB_FAIL(GCTX.root_service_->get_ddl_task_scheduler().schedule_ddl_task(task_record))) {
        LOG_WARN("fail to schedule ddl task", K(ret), K(task_record));
      }
    }
  }
  return ret;
}

// not generate table_id for index, caller will do that
// if we pass the data_schema argument, the create_index_arg can not set database_name
// and table_name, which will used for getting data table schema in generate_schema
int ObIndexBuilder::do_create_index(
    const ObCreateIndexArg &arg,
    obrpc::ObAlterTableRes &res)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const bool is_index = false;
  const ObTableSchema *table_schema = NULL;
  uint64_t table_id = OB_INVALID_ID;
  bool in_tenant_space = true;
  schema_guard.set_session_id(arg.session_id_);
  const uint64_t tenant_id = arg.tenant_id_;
  if (!ddl_service_.is_inited()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("ddl_service not init", "ddl_service inited", ddl_service_.is_inited(), K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(arg), K(ret));
  } else if (OB_FAIL(ddl_service_.get_tenant_schema_guard_with_version_in_inner_table(tenant_id, schema_guard))) {
    LOG_WARN("get_schema_guard failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(
             tenant_id, arg.database_name_, arg.table_name_, is_index, table_schema))) {
    LOG_WARN("get_table_schema failed", K(arg), K(ret));
  } else if (NULL == table_schema) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_USER_ERROR(OB_TABLE_NOT_EXIST, to_cstring(arg.database_name_), to_cstring(arg.table_name_));
    LOG_WARN("table not exist", K(arg), K(ret));
  } else if (FALSE_IT(table_id = table_schema->get_table_id())) {
  } else if (OB_FAIL(ObSysTableChecker::is_tenant_space_table_id(table_id, in_tenant_space))) {
    LOG_WARN("fail to check table in tenant space", K(ret), K(table_id));
  } else if (is_inner_table(table_id)) {
    // FIXME: create index for inner table is not supported yet.
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("create index on inner table not supported", K(ret), K(table_id));
  } else if (!arg.is_inner_ && table_schema->is_in_recyclebin()) {
    ret = OB_ERR_OPERATION_ON_RECYCLE_OBJECT;
    LOG_WARN("can not add index on table in recyclebin", K(ret), K(arg));
  } else if (table_schema->is_in_splitting()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("can not create index during splitting", K(ret), K(arg));
  } else if (OB_FAIL(ddl_service_.check_restore_point_allow(tenant_id, *table_schema))) {
    LOG_WARN("failed to check restore point allow.", K(ret), K(tenant_id), K(table_id));
  } else if (table_schema->get_index_tid_count() >= OB_MAX_INDEX_PER_TABLE) {
    ret = OB_ERR_TOO_MANY_KEYS;
    LOG_USER_ERROR(OB_ERR_TOO_MANY_KEYS, OB_MAX_INDEX_PER_TABLE);
    int64_t index_count = table_schema->get_index_tid_count();
    LOG_WARN("too many index for table", K(OB_MAX_INDEX_PER_TABLE), K(index_count), K(ret));
  } else if (OB_FAIL(ddl_service_.check_fk_related_table_ddl(*table_schema, ObDDLType::DDL_CREATE_INDEX))) {
    LOG_WARN("check whether the foreign key related table is executing ddl failed", K(ret));
  } else if (INDEX_TYPE_NORMAL_LOCAL == arg.index_type_
             || INDEX_TYPE_UNIQUE_LOCAL == arg.index_type_
             || INDEX_TYPE_DOMAIN_CTXCAT == arg.index_type_
             || INDEX_TYPE_SPATIAL_LOCAL == arg.index_type_) {
    if (OB_FAIL(do_create_local_index(schema_guard, arg, *table_schema, res))) {
      LOG_WARN("fail to do create local index", K(ret), K(arg));
    }
  } else if (INDEX_TYPE_NORMAL_GLOBAL == arg.index_type_
             || INDEX_TYPE_UNIQUE_GLOBAL == arg.index_type_
             || INDEX_TYPE_SPATIAL_GLOBAL == arg.index_type_) {
    if (!table_schema->is_partitioned_table() && !arg.index_schema_.is_partitioned_table()) {
      // create a global index with local storage when both the data table and index table are non-partitioned
      if (OB_FAIL(do_create_local_index(schema_guard, arg, *table_schema, res))) {
        LOG_WARN("fail to do create local index", K(ret));
      }
    } else {
      if (OB_FAIL(do_create_global_index(schema_guard, arg, *table_schema, res))) {
        LOG_WARN("fail to do create global index", K(ret));
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index type unexpected", K(ret), "index_type", arg.index_type_);
  }
  return ret;
}

/* after global index is introducted, the arguments of this interface become complicated
 * if this interface is modified, please update this comments
 * modified by wenduo, 2018/5/15
 *   ObIndexBuilder::generate_schema is invoked in thoe following three circumstances:
 *   1. invoked when Create index to build global index: this interface helps to specifiy partition columns,
 *      the column information of index schema is generated during the resolve stage, no need to generate
 *      column information of the index schema any more in this interface,
 *      the argument global_index_without_column_info is false for this situation
 *   2. invoked when Create table with index: this interface helps to specify partition columns,
 *      the column information of index schema is generated during the resolve stage, need to to generate
 *      column information of the index schema any more in this interface,
 *      the argument global_index_without_column_info is false for this situation
 *   3. invoked when Alter table with index, in this situation only non-partition global index can be build,
 *      column information is not filled in the index schema and the global_index_without_column_info is true.
 *   when generate_schema is invoked to build a local index or a global index with local storage,
 *   global_index_without_column_info is false.
 */
int ObIndexBuilder::generate_schema(
    const ObCreateIndexArg &arg,
    ObTableSchema &data_schema,
    const bool global_index_without_column_info,
    const bool generate_id,
    ObTableSchema &schema)
{
  int ret = OB_SUCCESS;
  bool is_oracle_mode = false;
  // some items in arg may be invalid, don't check arg here(when create table with index, alter
  // table add index)
  if (OB_UNLIKELY(!ddl_service_.is_inited())) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("ddl_service not init", "ddl_service inited", ddl_service_.is_inited(), K(ret));
  } else if (OB_UNLIKELY(!data_schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument",  K(ret), K(data_schema));
  } else if (OB_FAIL(data_schema.check_if_oracle_compat_mode(is_oracle_mode))) {
    LOG_WARN("check_if_oracle_compat_mode failed", K(ret));
  }

  if (OB_SUCC(ret)) {
    if (arg.index_columns_.count() <= 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("index columns can't be empty", "index columns", arg.index_columns_, K(ret));
    } else {}

    //do some check
    if (OB_SUCC(ret)) {
      if (!GCONF.enable_sys_table_ddl) {
        if (!data_schema.is_user_table() && !data_schema.is_tmp_table()) {
          ret = OB_ERR_WRONG_OBJECT;
          LOG_USER_ERROR(OB_ERR_WRONG_OBJECT, to_cstring(arg.database_name_),
                         to_cstring(arg.table_name_), "BASE_TABLE");
          ObTableType table_type = data_schema.get_table_type();
          LOG_WARN("Not support to create index on non-normal table", K(table_type), K(arg), K(ret));
        } else if (OB_INVALID_ID != arg.index_table_id_ || OB_INVALID_ID != arg.data_table_id_) {
          char err_msg[number::ObNumber::MAX_PRINTABLE_SIZE];
          MEMSET(err_msg, 0, sizeof(err_msg));
          // create index specifying index_id can only be used when the configuration is on
          ret = OB_OP_NOT_ALLOW;
          (void)snprintf(err_msg, sizeof(err_msg),
                   "%s", "create index with index_table_id/data_table_id");
          LOG_USER_ERROR(OB_OP_NOT_ALLOW, err_msg);
        }
      }

      if (OB_SUCC(ret)
          && (INDEX_TYPE_NORMAL_LOCAL == arg.index_type_
              || INDEX_TYPE_UNIQUE_LOCAL == arg.index_type_
              || INDEX_TYPE_DOMAIN_CTXCAT == arg.index_type_)) {
        if (OB_FAIL(sql::ObResolverUtils::check_unique_index_cover_partition_column(
                data_schema, arg))) {
          RS_LOG(WARN, "fail to check unique key cover partition column", K(ret));
        }
      }
      int64_t index_data_length = 0;
      bool is_ctxcat_added = false;
      bool is_mysql_func_index = false;
      for (int64_t i = 0; OB_SUCC(ret) && i < arg.index_columns_.count(); ++i) {
        const ObColumnSchemaV2 *data_column = NULL;
        const ObColumnSortItem &sort_item = arg.index_columns_.at(i);
        if (NULL == (data_column = data_schema.get_column_schema(sort_item.column_name_))) {
          ret = OB_ERR_KEY_COLUMN_DOES_NOT_EXITS;
          LOG_USER_ERROR(OB_ERR_KEY_COLUMN_DOES_NOT_EXITS, sort_item.column_name_.length(), sort_item.column_name_.ptr());
          LOG_WARN("get_column_schema failed", "tenant_id", data_schema.get_tenant_id(),
                   "database_id", data_schema.get_database_id(),
                   "table_name", data_schema.get_table_name(),
                   "column name", sort_item.column_name_, K(ret));
        } else if (OB_INVALID_ID != sort_item.get_column_id()
                   && data_column->get_column_id() != sort_item.get_column_id()) {
          ret = OB_ERR_INVALID_COLUMN_ID;
          LOG_USER_ERROR(OB_ERR_INVALID_COLUMN_ID, sort_item.column_name_.length(), sort_item.column_name_.ptr());
          LOG_WARN("Column ID specified by create index mismatch with data table Column ID",
                   "data_table_column_id", data_column->get_column_id(),
                   "user_specidifed_column_id", sort_item.get_column_id(),
                   K(ret));
        } else if (sort_item.prefix_len_ > 0) {
          if ((index_data_length += sort_item.prefix_len_) > OB_MAX_USER_ROW_KEY_LENGTH) {
            ret = OB_ERR_TOO_LONG_KEY_LENGTH;
            LOG_USER_ERROR(OB_ERR_TOO_LONG_KEY_LENGTH, OB_MAX_USER_ROW_KEY_LENGTH);
            LOG_WARN("index table rowkey length over max_user_row_key_length",
                K(index_data_length), LITERAL_K(OB_MAX_USER_ROW_KEY_LENGTH), K(ret));
          }
        } else if (FALSE_IT(is_mysql_func_index |= !is_oracle_mode && data_column->is_func_idx_column())) {
        } else if (!is_oracle_mode && data_column->is_func_idx_column() && ob_is_text_tc(data_column->get_data_type())) {
          ret = OB_ERR_FUNCTIONAL_INDEX_ON_LOB;
          LOG_WARN("Cannot create a functional index on an expression that returns a BLOB or TEXT.", K(ret));
        } else if (ob_is_text_tc(data_column->get_data_type()) && !data_column->is_fulltext_column()) {
          ret = OB_ERR_WRONG_KEY_COLUMN;
          LOG_USER_ERROR(OB_ERR_WRONG_KEY_COLUMN, sort_item.column_name_.length(), sort_item.column_name_.ptr());
          LOG_WARN("index created direct on large text column should only be fulltext", K(arg.index_type_), K(ret));
        } else if (ObTimestampTZType == data_column->get_data_type()
                   && arg.is_unique_primary_index()) {
          ret = OB_ERR_WRONG_KEY_COLUMN;
          LOG_USER_ERROR(OB_ERR_WRONG_KEY_COLUMN, sort_item.column_name_.length(), sort_item.column_name_.ptr());
          LOG_WARN("TIMESTAMP WITH TIME ZONE column can't be primary/unique key", K(arg.index_type_), K(ret));
        } else if (data_column->get_meta_type().is_blob()) {
          ret = OB_ERR_WRONG_KEY_COLUMN;
          LOG_USER_ERROR(OB_ERR_WRONG_KEY_COLUMN, sort_item.column_name_.length(), sort_item.column_name_.ptr());
          LOG_WARN("fulltext index created on blob column is not supported", K(arg.index_type_), K(ret));
        } else if (data_column->get_meta_type().is_ext() || data_column->get_meta_type().is_user_defined_sql_type()) {
          ret = OB_ERR_WRONG_KEY_COLUMN;
          LOG_USER_ERROR(OB_ERR_WRONG_KEY_COLUMN, sort_item.column_name_.length(), sort_item.column_name_.ptr());
          LOG_WARN("index created on udt column is not supported", K(arg.index_type_), K(ret));
        } else if (ob_is_json_tc(data_column->get_data_type())) {
          if (!is_oracle_mode && data_column->is_func_idx_column()) {
            ret = OB_ERR_FUNCTIONAL_INDEX_ON_JSON_OR_GEOMETRY_FUNCTION;
            LOG_WARN("Cannot create a functional index on an expression that returns a JSON or GEOMETRY.",K(ret));
          } else {
            ret = OB_ERR_JSON_USED_AS_KEY;
            LOG_USER_ERROR(OB_ERR_JSON_USED_AS_KEY, sort_item.column_name_.length(), sort_item.column_name_.ptr());
            LOG_WARN("JSON column cannot be used in key specification.", K(arg.index_type_), K(ret));
          }
        } else if (data_column->is_string_type()) {
          int64_t length = 0;
          if (data_column->is_fulltext_column()) {
            if (!is_ctxcat_added) {
              length = OB_MAX_OBJECT_NAME_LENGTH;
              is_ctxcat_added = true;
            }
          } else if (OB_FAIL(data_column->get_byte_length(length, is_oracle_mode, false))) {
            LOG_WARN("fail to get byte length of column", K(ret));
          } else if (length < 0) {
            ret = OB_ERR_WRONG_KEY_COLUMN;
            LOG_USER_ERROR(OB_ERR_WRONG_KEY_COLUMN, sort_item.column_name_.length(), sort_item.column_name_.ptr());
            LOG_WARN("byte_length of string type column is less than zero", K(length), K(ret));
          } else { /*do nothing*/ }

          if (OB_SUCC(ret)) {
            index_data_length += length;
            if (index_data_length > OB_MAX_USER_ROW_KEY_LENGTH) {
              ret = OB_ERR_TOO_LONG_KEY_LENGTH;
              LOG_USER_ERROR(OB_ERR_TOO_LONG_KEY_LENGTH, (OB_MAX_USER_ROW_KEY_LENGTH));
              LOG_WARN("index table rowkey length over max_user_row_key_length",
                       K(index_data_length), LITERAL_K(OB_MAX_USER_ROW_KEY_LENGTH), K(ret));
            }
          }
        }
      }
      if (OB_SUCC(ret) && is_mysql_func_index) {
        uint64_t tenant_data_version = 0;
        if (OB_FAIL(GET_MIN_DATA_VERSION(data_schema.get_tenant_id(), tenant_data_version))) {
          LOG_WARN("get tenant data version failed", K(ret));
        } else if (tenant_data_version < DATA_VERSION_4_2_0_0){
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("tenant version is less than 4.2, functional index is not supported in mysql mode", K(ret), K(tenant_data_version));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "version is less than 4.2, functional index in mysql mode not supported");
        }
      }
    }

    if (OB_SUCC(ret)) {
      // column information of the global index is filled during the resolve stage
      const bool is_index_local_storage = (INDEX_TYPE_NORMAL_LOCAL == arg.index_type_
                                           || INDEX_TYPE_UNIQUE_LOCAL == arg.index_type_
                                           || INDEX_TYPE_NORMAL_GLOBAL_LOCAL_STORAGE == arg.index_type_
                                           || INDEX_TYPE_UNIQUE_GLOBAL_LOCAL_STORAGE == arg.index_type_
                                           || INDEX_TYPE_DOMAIN_CTXCAT == arg.index_type_
                                           || INDEX_TYPE_SPATIAL_LOCAL == arg.index_type_
                                           || INDEX_TYPE_SPATIAL_GLOBAL_LOCAL_STORAGE == arg.index_type_);
      const bool need_generate_index_schema_column = (is_index_local_storage || global_index_without_column_info);
      schema.set_table_mode(data_schema.get_table_mode_flag());
      schema.set_table_state_flag(data_schema.get_table_state_flag());
      schema.set_duplicate_scope(data_schema.get_duplicate_scope());
      if (OB_FAIL(set_basic_infos(arg, data_schema, schema))) {
        LOG_WARN("set_basic_infos failed", K(arg), K(data_schema), K(ret));
      } else if (need_generate_index_schema_column
                 && OB_FAIL(set_index_table_columns(arg, data_schema, schema))) {
        LOG_WARN("set_index_table_columns failed", K(arg), K(data_schema), K(ret));
      } else if (OB_FAIL(set_index_table_options(arg, data_schema, schema))) {
        LOG_WARN("set_index_table_options failed", K(arg), K(data_schema), K(ret));
      } else {
        schema.set_name_generated_type(arg.index_schema_.get_name_generated_type());
        LOG_INFO("finish generate index schema", K(schema));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (data_schema.get_part_level() > 0
               && is_index_local_storage(arg.index_type_)
               && OB_FAIL(schema.assign_partition_schema(data_schema))) {
      LOG_WARN("fail to assign partition schema", K(schema), K(ret));
    } else if (OB_FAIL(ddl_service_.try_format_partition_schema(schema))) {
      LOG_WARN("fail to format partition schema", KR(ret), K(schema));
    } else if (generate_id) {
      if (OB_FAIL(ddl_service_.generate_object_id_for_partition_schema(schema))) {
        LOG_WARN("fail to generate object_id for partition schema", KR(ret), K(schema));
      } else if (OB_FAIL(ddl_service_.generate_tablet_id(schema))) {
        LOG_WARN("fail to fetch new table id", KR(ret), K(schema));
      }
    }
  }
  return ret;
}

int ObIndexBuilder::set_basic_infos(const ObCreateIndexArg &arg,
                                    const ObTableSchema &data_schema,
                                    ObTableSchema &schema)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObDatabaseSchema *database = NULL;
  const uint64_t tenant_id = data_schema.get_tenant_id();
  if (!ddl_service_.is_inited()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("ddl_service not init", "ddl_service inited", ddl_service_.is_inited(), K(ret));
  } else if (OB_FAIL(ddl_service_.get_schema_service().get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get schema_guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_database_schema(tenant_id, data_schema.get_database_id(), database))) {
    LOG_WARN("fail to get database_schema", K(ret), K(tenant_id), "database_id", data_schema.get_database_id());
  } else if (OB_ISNULL(database)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("database_schema is null", K(ret), "database_id", data_schema.get_database_id());
  } else if (!data_schema.is_valid()) {
    // some items in arg may be invalid, don't check arg
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(data_schema), K(ret));
  } else {
    ObString index_table_name = arg.index_name_;
    ObArenaAllocator allocator(ObModIds::OB_SCHEMA);
    const uint64_t table_schema_id = data_schema.get_table_id();
    const ObTablespaceSchema *tablespace_schema = NULL;
    if (table_schema_id != ((OB_INVALID_ID == arg.data_table_id_) ? table_schema_id : arg.data_table_id_)) {
      // need to check if the data table ids are the same when data table id is specified
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Invalid data table id", K(table_schema_id), K(arg.data_table_id_), K(ret));
    } else if (OB_FAIL(ObTableSchema::build_index_table_name(
               allocator, table_schema_id, arg.index_name_, index_table_name))) {
      LOG_WARN("build_index_table_name failed", K(table_schema_id), K(arg), K(ret));
    } else if (OB_FAIL(schema.set_table_name(index_table_name))) {
      LOG_WARN("set_table_name failed", K(index_table_name), K(arg), K(ret));
    } else {
      schema.set_name_generated_type(arg.index_schema_.get_name_generated_type());
      schema.set_table_id(arg.index_table_id_);
      schema.set_table_type(USER_INDEX);
      schema.set_index_type(arg.index_type_);
      schema.set_index_status(arg.index_option_.index_status_);
      schema.set_data_table_id(table_schema_id);

      // priority same with data table schema
      schema.set_tenant_id(tenant_id);
      schema.set_database_id(data_schema.get_database_id());
      schema.set_tablegroup_id(OB_INVALID_ID);
      schema.set_load_type(data_schema.get_load_type());
      schema.set_def_type(data_schema.get_def_type());
      if (INDEX_TYPE_NORMAL_LOCAL == arg.index_type_
          || INDEX_TYPE_UNIQUE_LOCAL == arg.index_type_
          || INDEX_TYPE_SPATIAL_LOCAL == arg.index_type_) {
        schema.set_part_level(data_schema.get_part_level());
      } else {} // partition level is filled during resolve stage for global index
      schema.set_charset_type(data_schema.get_charset_type());
      schema.set_collation_type(data_schema.get_collation_type());
      schema.set_row_store_type(data_schema.get_row_store_type());
      schema.set_store_format(data_schema.get_store_format());
      schema.set_storage_format_version(data_schema.get_storage_format_version());
      schema.set_tablespace_id(arg.index_schema_.get_tablespace_id());
      if (OB_FAIL(schema.set_encryption_str(arg.index_schema_.get_encryption_str()))) {
        LOG_WARN("fail to set set_encryption_str", K(ret), K(arg));
      }

      if (data_schema.get_max_used_column_id() > schema.get_max_used_column_id()) {
        schema.set_max_used_column_id(data_schema.get_max_used_column_id());
      }
      //index table will not contain auto increment column
      schema.set_autoinc_column_id(0);
      schema.set_progressive_merge_num(data_schema.get_progressive_merge_num());
      schema.set_progressive_merge_round(data_schema.get_progressive_merge_round());
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(schema.set_compress_func_name(data_schema.get_compress_func_name()))) {
        LOG_WARN("set_compress_func_name failed", K(data_schema));
      } else if (OB_INVALID_ID != schema.get_tablespace_id()) {
        if (OB_FAIL(schema_guard.get_tablespace_schema(
            tenant_id, schema.get_tablespace_id(), tablespace_schema))) {
          LOG_WARN("fail to get tablespace schema", K(schema), K(ret));
        } else if (OB_UNLIKELY(NULL == tablespace_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("tablespace schema is null", K(ret), K(schema));
        } else if (OB_FAIL(schema.set_encrypt_key(tablespace_schema->get_encrypt_key()))) {
          LOG_WARN("fail to set encrypt key", K(ret), K(schema));
        } else {
          schema.set_master_key_id(tablespace_schema->get_master_key_id());
        }
      }
    }
  }
  return ret;
}

int ObIndexBuilder::set_index_table_columns(const ObCreateIndexArg &arg,
                                            const ObTableSchema &data_schema,
                                            ObTableSchema &schema)
{
  int ret = OB_SUCCESS;
  if (!ddl_service_.is_inited()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("ddl_service not init", "ddl_service inited", ddl_service_.is_inited(), K(ret));
  } else if (OB_FAIL(ObIndexBuilderUtil::set_index_table_columns(arg, data_schema, schema))) {
    LOG_WARN("fail to set index table columns", K(ret));
  } else {} // no more to do
  return ret;
}

int ObIndexBuilder::set_index_table_options(const obrpc::ObCreateIndexArg &arg,
                                            const share::schema::ObTableSchema &data_schema,
                                            share::schema::ObTableSchema &schema)
{
  int ret = OB_SUCCESS;
  if (!ddl_service_.is_inited()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("ddl_service not init", "ddl_service inited", ddl_service_.is_inited(), K(ret));
  } else if (!data_schema.is_valid()) {
    // some items in arg may be invalid, don't check arg
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(data_schema), K(ret));
  } else {
    schema.set_block_size(arg.index_option_.block_size_);
    schema.set_tablet_size(data_schema.get_tablet_size());
    schema.set_pctfree(data_schema.get_pctfree());
    schema.set_index_attributes_set(arg.index_option_.index_attributes_set_);
    schema.set_is_use_bloomfilter(arg.index_option_.use_bloom_filter_);
    //schema.set_progressive_merge_num(arg.index_option_.progressive_merge_num_);
    schema.set_index_using_type(arg.index_using_type_);
    schema.set_row_store_type(data_schema.get_row_store_type());
    schema.set_store_format(data_schema.get_store_format());
    // set dop for index table
    schema.set_dop(arg.index_schema_.get_dop());
    if (OB_FAIL(schema.set_compress_func_name(data_schema.get_compress_func_name()))) {
      LOG_WARN("set_compress_func_name failed", K(ret),
               "compress method", data_schema.get_compress_func_name());
    } else if (OB_FAIL(schema.set_comment(arg.index_option_.comment_))) {
      LOG_WARN("set_comment failed", "comment", arg.index_option_.comment_, K(ret));
    } else if (OB_FAIL(schema.set_parser_name(arg.index_option_.parser_name_))) {
      LOG_WARN("set parser name failed", K(ret), "parser_name", arg.index_option_.parser_name_);
    }
  }
  return ret;
}

bool ObIndexBuilder::is_final_index_status(const ObIndexStatus index_status) const
{
  return (INDEX_STATUS_AVAILABLE == index_status
          || INDEX_STATUS_UNIQUE_INELIGIBLE == index_status
          || is_error_index_status(index_status));
}

}//end namespace rootserver
}//end namespace oceanbase
