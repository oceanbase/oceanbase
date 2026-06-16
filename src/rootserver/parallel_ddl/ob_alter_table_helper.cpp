/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX RS
#include "rootserver/parallel_ddl/ob_alter_table_helper.h"
#include "rootserver/parallel_ddl/ob_alter_table_add_column_action.h"
#include "rootserver/parallel_ddl/ob_alter_table_drop_column_action.h"
#include "rootserver/parallel_ddl/ob_alter_table_partition_action.h"
#include "rootserver/parallel_ddl/ob_add_partition_action.h"
#include "rootserver/parallel_ddl/ob_add_subpartition_action.h"
#include "rootserver/parallel_ddl/ob_drop_partition_action.h"
#include "rootserver/parallel_ddl/ob_drop_subpartition_action.h"
#include "rootserver/parallel_ddl/ob_truncate_partition_action.h"
#include "rootserver/parallel_ddl/ob_truncate_subpartition_action.h"
#include "rootserver/ddl_task/ob_sys_ddl_util.h"
#include "share/ob_rpc_struct.h"
#include "share/schema/ob_schema_service.h"
#include "share/schema/ob_schema_utils.h"
#include "rootserver/ob_ddl_service.h"
#include "share/ob_share_util.h"

using namespace oceanbase::rootserver;
using namespace oceanbase::obrpc;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::common;

ObAlterTableHelper::ObAlterTableHelper(
    share::schema::ObMultiVersionSchemaService *schema_service,
    const uint64_t tenant_id,
    const obrpc::ObAlterTableArg &arg,
    obrpc::ObAlterTableRes &res)
    : ObDDLHelper(schema_service, tenant_id, "[parallel alter table]", nullptr, true/*enable_ddl_parallel*/),
      arg_(arg),
      res_(res),
      actions_(),
      action_types_(),
      orig_table_schema_(nullptr),
      new_table_schema_(),
      ddl_task_records_()
{
}

ObAlterTableHelper::~ObAlterTableHelper()
{
  for (int64_t i = 0; i < actions_.count(); ++i) {
    if (OB_NOT_NULL(actions_.at(i))) {
      actions_.at(i)->~ObIAlterTableAction();
      actions_.at(i) = nullptr;
    }
  }
}

// ObIAlterTableAction forwarding accessors
int ObIAlterTableAction::check_inner_stat_() const { return helper_.check_inner_stat(); }
const ObAlterTableArg &ObIAlterTableAction::get_arg() const { return helper_.get_arg(); }
ObAlterTableRes &ObIAlterTableAction::get_res() { return helper_.get_res(); }
const ObTableSchema *ObIAlterTableAction::get_orig_table_schema() const { return helper_.get_orig_table_schema(); }
ObTableSchema &ObIAlterTableAction::get_new_table_schema() { return helper_.get_new_table_schema(); }
ObIArray<ObDDLTaskRecord> &ObIAlterTableAction::get_ddl_task_records() { return helper_.get_ddl_task_records(); }
void ObIAlterTableAction::add_schema_version_cnt(const int64_t cnt) { helper_.add_schema_version_cnt(cnt); }
ObMultiVersionSchemaService *ObIAlterTableAction::get_schema_service() { return helper_.get_schema_service(); }
ObMySQLProxy *ObIAlterTableAction::get_sql_proxy() { return helper_.get_sql_proxy(); }
ObDDLService *ObIAlterTableAction::get_ddl_service() { return helper_.get_ddl_service(); }
ObDDLSQLTransaction &ObIAlterTableAction::get_trans() { return helper_.get_trans(); }
ObSchemaGuardWrapper &ObIAlterTableAction::get_schema_guard_wrapper() { return helper_.get_schema_guard_wrapper(); }
uint64_t ObIAlterTableAction::get_tenant_id() const { return helper_.get_tenant_id(); }
ObArenaAllocator &ObIAlterTableAction::get_allocator() { return helper_.get_allocator(); }
int ObIAlterTableAction::add_ddl_object_lock_by_id(
    const uint64_t lock_obj_id,
    const ObSchemaType schema_type,
    const transaction::tablelock::ObTableLockMode lock_mode)
{ return helper_.add_ddl_object_lock_by_id(lock_obj_id, schema_type, lock_mode); }

int ObAlterTableHelper::init_()
{
  int ret = OB_SUCCESS;
  bool can_parallel_alter = false;
  const char *reason = nullptr;
  if (OB_FAIL(check_alter_table_arg_for_parallel(arg_, can_parallel_alter, reason))) {
    LOG_WARN("fail to check alter table arg for parallel", KR(ret));
  } else if (!can_parallel_alter) {
    ret = OB_NOT_SUPPORTED_FOR_PARALLEL_DDL;
    LOG_WARN("alter table arg not supported for parallel DDL", KR(ret), "reason", reason);
  } else if (OB_FAIL(resolve_action_types_())) {
    LOG_WARN("fail to resolve action types", KR(ret));
  } else if (OB_FAIL(check_supported_action_types_())) {
    LOG_WARN("fail to check supported action types", KR(ret));
  } else if (OB_FAIL(build_actions_())) {
    LOG_WARN("fail to build actions", KR(ret));
  }
  return ret;
}

int ObAlterTableHelper::resolve_column_action_type_(
    common::ObArray<ObAlterTableActionType> &action_types)
{
  int ret = OB_SUCCESS;
  // action_types_ order tracks the user-input column order so [ADD b, DROP a]
  // yields [ADD, DROP] and [DROP a, ADD b] yields [DROP, ADD]. First occurrence
  // of each kind decides the action's position; subsequent columns of the same
  // kind are folded into the same action.
  bool has_add = false;
  bool has_drop = false;
  const AlterTableSchema &alter_table_schema = arg_.alter_table_schema_;
  ObTableSchema::const_column_iterator it = alter_table_schema.column_begin();
  ObTableSchema::const_column_iterator it_end = alter_table_schema.column_end();
  for (; OB_SUCC(ret) && it != it_end; ++it) {
    if (OB_ISNULL(*it)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column schema is null", KR(ret));
    } else {
      const AlterColumnSchema *alter_col = static_cast<const AlterColumnSchema *>(*it);
      if (OB_DDL_ADD_COLUMN == alter_col->alter_type_) {
        if (!has_add) {
          has_add = true;
          if (OB_FAIL(action_types.push_back(ADD_COLUMN_ACTION))) {
            LOG_WARN("fail to push back action type", KR(ret));
          }
        }
      } else if (OB_DDL_DROP_COLUMN == alter_col->alter_type_) {
        if (!has_drop) {
          has_drop = true;
          if (OB_FAIL(action_types.push_back(DROP_COLUMN_ACTION))) {
            LOG_WARN("fail to push back action type", KR(ret));
          }
        }
      } else {
        ret = OB_NOT_SUPPORTED_FOR_PARALLEL_DDL;
        LOG_WARN("unexpected column alter type for parallel DDL, fall back to serial",
                 KR(ret), "alter_type", alter_col->alter_type_);
      }
    }
  }
  return ret;
}

int ObAlterTableHelper::resolve_partition_action_type_(
    common::ObArray<ObAlterTableActionType> &action_types)
{
  int ret = OB_SUCCESS;
  if (ObAlterTableArg::NO_OPERATION != arg_.alter_part_type_) {
    if (OB_FAIL(action_types.push_back(PARTITION_ACTION))) {
      LOG_WARN("fail to push back action type", KR(ret));
    }
  }
  return ret;
}

int ObAlterTableHelper::resolve_action_types_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(resolve_column_action_type_(action_types_))) {
    LOG_WARN("fail to resolve column action type", KR(ret));
  } else if (OB_FAIL(resolve_partition_action_type_(action_types_))) {
    LOG_WARN("fail to resolve partition action type", KR(ret));
  } else if (action_types_.empty()) {
    ret = OB_NOT_SUPPORTED_FOR_PARALLEL_DDL;
    LOG_WARN("no valid alter table action resolved", KR(ret));
  }
  return ret;
}

// Whitelist of action types currently enabled for parallel DDL.
static constexpr ObAlterTableActionType kParallelAllowedActionTypes[] = {
    ADD_COLUMN_ACTION,
    DROP_COLUMN_ACTION,
    PARTITION_ACTION,
};

int ObAlterTableHelper::check_supported_action_types_()
{
  int ret = OB_SUCCESS;
  // 1. Per-action whitelist.
  for (int64_t i = 0; OB_SUCC(ret) && i < action_types_.count(); ++i) {
    bool hit = false;
    for (int64_t j = 0;
         j < ARRAYSIZEOF(kParallelAllowedActionTypes) && !hit; ++j) {
      if (kParallelAllowedActionTypes[j] == action_types_.at(i)) {
        hit = true;
      }
    }
    if (!hit) {
      ret = OB_NOT_SUPPORTED_FOR_PARALLEL_DDL;
      LOG_WARN("action type not enabled for parallel alter table",
               KR(ret), "action_type", action_types_.at(i), K_(action_types));
    }
  }
  // 2. Single-action invariant: parallel alter table only supports one action at a time.
  if (OB_SUCC(ret) && 1 != action_types_.count()) {
    ret = OB_NOT_SUPPORTED_FOR_PARALLEL_DDL;
    LOG_WARN("only single action supported for parallel alter table",
             KR(ret), K_(action_types));
  }
  return ret;
}

int ObAlterTableHelper::build_actions_()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < action_types_.count(); ++i) {
    void *buf = nullptr;
    ObIAlterTableAction *action = nullptr;
    switch (action_types_.at(i)) {
      case ADD_COLUMN_ACTION: {
        if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObAlterTableAddColumnAction)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc memory", KR(ret));
        } else {
          action = new (buf) ObAlterTableAddColumnAction(*this);
        }
        break;
      }
      case DROP_COLUMN_ACTION: {
        if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObAlterTableDropColumnAction)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc memory", KR(ret));
        } else {
          action = new (buf) ObAlterTableDropColumnAction(*this);
        }
        break;
      }
      case PARTITION_ACTION: {
        if (ObAlterTableArg::ADD_PARTITION == arg_.alter_part_type_) {
          if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObAddPartitionAction)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to alloc memory", KR(ret));
          } else {
            action = new (buf) ObAddPartitionAction(*this);
          }
        } else if (ObAlterTableArg::ADD_SUB_PARTITION == arg_.alter_part_type_) {
          if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObAddSubpartitionAction)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to alloc memory", KR(ret));
          } else {
            action = new (buf) ObAddSubpartitionAction(*this);
          }
        } else if (ObAlterTableArg::DROP_PARTITION == arg_.alter_part_type_) {
          if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObDropPartitionAction)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to alloc memory", KR(ret));
          } else {
            action = new (buf) ObDropPartitionAction(*this);
          }
        } else if (ObAlterTableArg::DROP_SUB_PARTITION == arg_.alter_part_type_) {
          if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObDropSubpartitionAction)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to alloc memory", KR(ret));
          } else {
            action = new (buf) ObDropSubpartitionAction(*this);
          }
        } else if (ObAlterTableArg::TRUNCATE_PARTITION == arg_.alter_part_type_) {
          if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObTruncatePartitionAction)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to alloc memory", KR(ret));
          } else {
            action = new (buf) ObTruncatePartitionAction(*this);
          }
        } else if (ObAlterTableArg::TRUNCATE_SUB_PARTITION == arg_.alter_part_type_) {
          if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObTruncateSubpartitionAction)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to alloc memory", KR(ret));
          } else {
            action = new (buf) ObTruncateSubpartitionAction(*this);
          }
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("unsupported partition alter type for parallel ddl", KR(ret),
                   "alter_part_type", arg_.alter_part_type_);
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected action type", KR(ret), K(action_types_.at(i)));
        break;
      }
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(action)) {
      if (OB_FAIL(action->init())) {
        LOG_WARN("fail to init action", KR(ret), K(action_types_.at(i)));
        action->~ObIAlterTableAction();
        action = nullptr;
      } else if (OB_FAIL(actions_.push_back(action))) {
        LOG_WARN("fail to push back action", KR(ret));
        action->~ObIAlterTableAction();
        action = nullptr;
      }
    }
  }
  return ret;
}

int ObAlterTableHelper::check_table_legitimacy_()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(orig_table_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("orig table schema is null", KR(ret));
  } else if (orig_table_schema_->is_view_table()) {
    ret = OB_NOT_SUPPORTED_FOR_PARALLEL_DDL;
    LOG_WARN("alter view is not supported for parallel DDL, fall back to serial", KR(ret),
             K(orig_table_schema_->get_table_id()));
  } else if (orig_table_schema_->is_materialized_view()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("alter materialized view is not supported", KR(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "alter materialized view is");
  } else if (orig_table_schema_->is_ctas_tmp_table()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("try to alter invisible table schema", KR(ret),
             K(orig_table_schema_->get_session_id()), K_(arg));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "try to alter invisible table");
  } else if (OB_FAIL(ddl_service_->check_alter_table_legitimacy_common(
                     *orig_table_schema_, arg_,
                     false /*is_db_in_recyclebin*/,
                     false /*is_offline_ddl*/))) {
    // Database recyclebin state is filtered out before this point by the
    // name-lock + schema_guard path, so we pass false here.
    LOG_WARN("fail to check alter table legitimacy", KR(ret));
  }
  return ret;
}

int ObAlterTableHelper::check_alter_table_column_common_gates_()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(orig_table_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("orig table schema is null", KR(ret));
  } else if (OB_ISNULL(ddl_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ddl service is null", KR(ret));
  } else if (!arg_.is_alter_columns_) {
  } else if (OB_FAIL(ddl_service_->check_alter_table_column_common_gates(
                         tenant_id_, arg_.alter_table_schema_, *orig_table_schema_))) {
    LOG_WARN("fail to check alter table column common gates", KR(ret),
             K_(tenant_id), K_(arg));
  }
  return ret;
}

int ObAlterTableHelper::prepare_alter_table_arg_()
{
  int ret = OB_SUCCESS;
  uint64_t tenant_data_version = 0;
  ObDDLType ddl_type = ObDDLType::DDL_INVALID;
  if (OB_UNLIKELY(action_types_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("action type is empty", KR(ret));
  } else if (OB_ISNULL(orig_table_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("orig table schema is null", KR(ret));
  } else if (OB_ISNULL(ddl_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ddl service is null", KR(ret));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id_, tenant_data_version))) {
    LOG_WARN("get min data version failed", KR(ret), K_(tenant_id));
  } else {
    const bool has_add = has_exist_in_array(action_types_, ADD_COLUMN_ACTION);
    const bool has_drop = has_exist_in_array(action_types_, DROP_COLUMN_ACTION);
    // prepare_alter_table_arg_progressive_merge_round only treats pure drop
    // column instant specially. Do not depend on per-action result ddl type
    // here; that belongs to result construction after actions finish.
    if (has_drop && !has_add && 1 == action_types_.count()) {
      ddl_type = ObDDLType::DDL_DROP_COLUMN_INSTANT;
    } else if (has_add) {
      ddl_type = ObDDLType::DDL_ADD_COLUMN_ONLINE;
    }
  }
  if (OB_SUCC(ret)) {
    ObAlterTableArg &alter_table_arg = const_cast<ObAlterTableArg &>(arg_);
    if (OB_FAIL(ddl_service_->prepare_alter_table_arg_progressive_merge_round(
            tenant_data_version,
            ddl_type,
            *orig_table_schema_,
            alter_table_arg,
            &new_table_schema_))) {
      LOG_WARN("fail to prepare alter table arg progressive merge round",
               KR(ret), K(tenant_data_version));
    }
  }
  return ret;
}

int ObAlterTableHelper::lock_table_and_online_ddl_()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < actions_.count(); ++i) {
    if (OB_ISNULL(actions_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("action is null", KR(ret), K(i));
    } else if (OB_FAIL(actions_.at(i)->lock_table_and_online_ddl())) {
      LOG_WARN("fail to lock table and online ddl", KR(ret), K(i));
    }
  }
  return ret;
}

int ObAlterTableHelper::fetch_orig_table_schema_()
{
  int ret = OB_SUCCESS;
  const AlterTableSchema &alter_table_schema = arg_.alter_table_schema_;
  const ObString &origin_database_name = alter_table_schema.get_origin_database_name();
  const ObString &origin_table_name = alter_table_schema.get_origin_table_name();
  uint64_t database_id = OB_INVALID_ID;
  uint64_t table_id = OB_INVALID_ID;
  ObTableType table_type = MAX_TABLE_TYPE;
  int64_t schema_version = OB_INVALID_VERSION;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(schema_guard_wrapper_.get_database_id(
                     origin_database_name, database_id))) {
    LOG_WARN("fail to get database id", KR(ret), K(origin_database_name));
  } else if (OB_INVALID_ID == database_id) {
    ret = OB_ERR_BAD_DATABASE;
    LOG_WARN("database not exist", KR(ret), K(origin_database_name));
  } else if (OB_FAIL(schema_guard_wrapper_.get_table_id(
                     database_id, arg_.session_id_, origin_table_name,
                     table_id, table_type, schema_version))) {
    LOG_WARN("fail to get table id", KR(ret), K(database_id), K(origin_table_name));
  } else if (OB_INVALID_ID == table_id) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table not exist", KR(ret), K(origin_database_name), K(origin_table_name));
  } else if (OB_FAIL(schema_guard_wrapper_.get_table_schema(table_id, orig_table_schema_))) {
    LOG_WARN("fail to get table schema", KR(ret), K(table_id));
  } else if (OB_ISNULL(orig_table_schema_)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table not exist", KR(ret), K(table_id));
  }
  return ret;
}

int ObAlterTableHelper::lock_objects_()
{
  int ret = OB_SUCCESS;
  const AlterTableSchema &alter_table_schema = arg_.alter_table_schema_;
  const ObString &origin_database_name = alter_table_schema.get_origin_database_name();
  const ObString &origin_table_name = alter_table_schema.get_origin_table_name();
  LOG_INFO("start to lock objects for parallel alter table");
  DEBUG_SYNC(BEFORE_PARALLEL_DDL_LOCK);
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  }
  // 1. Name lock: SHARE on database, EXCLUSIVE on table name.
  //    Temporary tables lock both session_id and session_id=0.
  if (FAILEDx(add_lock_object_by_database_name_(origin_database_name, SHARE))) {
    LOG_WARN("fail to add lock by database name", KR(ret), K(origin_database_name));
  } else if (OB_FAIL(lock_databases_by_name_())) {
    LOG_WARN("fail to lock databases by name", KR(ret));
  } else if (0 != arg_.session_id_) {
    if (OB_FAIL(add_lock_table_by_name_with_session_id_zero_(
                origin_database_name, origin_table_name, EXCLUSIVE, arg_.session_id_))) {
      LOG_WARN("fail to add lock by table name with session id 0",
               KR(ret), K(origin_database_name), K(origin_table_name),
               "session_id", arg_.session_id_);
    }
  } else if (OB_FAIL(add_lock_object_by_name_(origin_database_name,
                                              origin_table_name,
                                              TABLE_SCHEMA,
                                              EXCLUSIVE))) {
    LOG_WARN("fail to add lock by table name",
             KR(ret), K(origin_database_name), K(origin_table_name));
  }
  if (FAILEDx(lock_existed_objects_by_name_())) {
    LOG_WARN("fail to lock existed objects by name", KR(ret));
  }
  // 2. Fetch orig table schema after name lock is taken.
  if (FAILEDx(fetch_orig_table_schema_())) {
    LOG_WARN("fail to fetch orig table schema", KR(ret));
  } else {
    schema_guard_wrapper_.set_session_id(arg_.session_id_);
  }
  // 3 + 4. Object lock layer: collect ids from all actions, then flush sorted.
  //        This is the only layer that enforces global id ordering to avoid
  //        cross-DDL deadlocks.
  for (int64_t i = 0; OB_SUCC(ret) && i < actions_.count(); ++i) {
    if (OB_ISNULL(actions_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("action is null", KR(ret), K(i));
    } else if (OB_FAIL(actions_.at(i)->register_ddl_object_locks())) {
      LOG_WARN("fail to register ddl object locks", KR(ret), K(i));
    }
  }
  if (FAILEDx(lock_existed_objects_by_id_())) {
    LOG_WARN("fail to lock existed objects by id", KR(ret));
  }
  DEBUG_SYNC(AFTER_PARALLEL_ALTER_TABLE_LOCK_OBJ);
  return ret;
}

int ObAlterTableHelper::check_after_lock_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_parallel_ddl_conflict_(arg_.based_schema_object_infos_))) {
    LOG_WARN("fail to check parallel ddl conflict", KR(ret));
  } else if (!action_based_schema_object_infos_.empty()
      && OB_FAIL(check_parallel_ddl_conflict_(action_based_schema_object_infos_))) {
    LOG_WARN("fail to check action-pushed schema conflict", KR(ret));
  } else if (OB_FAIL(check_table_legitimacy_())) {
    LOG_WARN("fail to check table legitimacy", KR(ret));
  // 6. Deep copy orig_table_schema_ -> new_table_schema_ before check_legitimacy()
  //    so actions can read a real schema via get_new_table_schema() (e.g. check_can_drop_column).
  } else if (OB_FAIL(new_table_schema_.assign(*orig_table_schema_))) {
    LOG_WARN("fail to deep copy table schema", KR(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < actions_.count(); ++i) {
    if (OB_ISNULL(actions_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("action is null", KR(ret), K(i));
    } else if (OB_FAIL(actions_.at(i)->check_legitimacy())) {
      LOG_WARN("fail to check action legitimacy", KR(ret), K(i));
    }
  }
  if (FAILEDx(check_alter_table_column_common_gates_())) {
    LOG_WARN("fail to check alter table column common gates", KR(ret));
  } else if (FAILEDx(prepare_alter_table_arg_())) {
    LOG_WARN("fail to prepare alter table arg", KR(ret));
  } else if (FAILEDx(lock_table_and_online_ddl_())) {
    // Storage-layer DDL lock (lock_for_common_ddl_in_trans) is acquired here
    // rather than in lock_objects_() because it depends on
    // arg_.progressive_merge_round_, which is set by prepare_alter_table_arg_()
    // above.  The two lock layers serve different purposes: lock_objects_() holds
    // the parallel-DDL framework locks (name/id) that serialise concurrent
    // framework-level DDLs, while this storage lock guards DDL/DML concurrency.
    LOG_WARN("fail to lock table and online ddl", KR(ret));
  }
  RS_TRACE(lock_objects);
  DEBUG_SYNC(AFTER_PARALLEL_DDL_LOCK);
  return ret;
}

int ObAlterTableHelper::generate_schemas_()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < actions_.count(); ++i) {
    if (OB_ISNULL(actions_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("action is null", KR(ret), K(i));
    } else if (OB_FAIL(actions_.at(i)->generate_schemas())) {
      LOG_WARN("fail to generate schemas", KR(ret), K(i), KPC(actions_.at(i)));
    }
  }
  return ret;
}

int ObAlterTableHelper::calc_schema_version_cnt_()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < actions_.count(); ++i) {
    if (OB_ISNULL(actions_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("action is null", KR(ret), K(i));
    } else if (OB_FAIL(actions_.at(i)->calc_schema_version_cnt())) {
      LOG_WARN("fail to calc schema version cnt", KR(ret), K(i), KPC(actions_.at(i)));
    }
  }
  if (OB_SUCC(ret)) {
    // 1503 boundary ddl operation
    schema_version_cnt_++;
  }
  return ret;
}

int ObAlterTableHelper::operate_schemas_()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < actions_.count(); ++i) {
    if (OB_ISNULL(actions_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("action is null", KR(ret), K(i));
    } else if (OB_FAIL(actions_.at(i)->operate_schemas())) {
      LOG_WARN("fail to operate schemas", KR(ret), K(i), KPC(actions_.at(i)));
    }
  }
  RS_TRACE(operate_schemas);
  return ret;
}

int ObAlterTableHelper::operation_before_commit_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < actions_.count(); ++i) {
    if (OB_ISNULL(actions_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("action is null", KR(ret), K(i));
    } else if (OB_FAIL(actions_.at(i)->before_commit())) {
      LOG_WARN("fail to do before_commit in action", KR(ret), K(i), KPC(actions_.at(i)));
    }
  }
  return ret;
}

int ObAlterTableHelper::clean_on_fail_commit_()
{
  int ret = OB_SUCCESS;
  ddl_task_records_.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < actions_.count(); ++i) {
    if (OB_ISNULL(actions_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("action is null", KR(ret), K(i));
    } else if (OB_FAIL(actions_.at(i)->clean_on_fail())) {
      LOG_WARN("fail to clean on fail in action", KR(ret), K(i), KPC(actions_.at(i)));
    }
  }
  return ret;
}

int ObAlterTableHelper::construct_and_adjust_result_(int &return_ret)
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
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < actions_.count(); ++i) {
        if (OB_ISNULL(actions_.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("action is null", KR(ret), K(i));
        } else if (OB_FAIL(actions_.at(i)->adjust_result())) {
          LOG_WARN("fail to adjust result in action", KR(ret), K(i), KPC(actions_.at(i)));
        }
      }
      // Set task_id from drop_lob async task if present so executor can wait for it.
      for (int64_t i = 0; i < ddl_task_records_.count(); ++i) {
        if (DDL_DROP_LOB == ddl_task_records_.at(i).ddl_type_) {
          res_.task_id_ = ddl_task_records_.at(i).task_id_;
          break;
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < ddl_task_records_.count(); ++i) {
        ObDDLTaskRecord &task_record = ddl_task_records_.at(i);
        if (OB_FAIL(ObSysDDLSchedulerUtil::schedule_ddl_task(task_record))) {
          LOG_WARN("fail to schedule ddl task", KR(ret), K(task_record));
        }
      }
    }
  }
  return ret;
}
