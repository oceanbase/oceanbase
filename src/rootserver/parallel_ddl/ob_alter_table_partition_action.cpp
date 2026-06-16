/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX RS
#include "rootserver/parallel_ddl/ob_alter_table_partition_action.h"
#include "rootserver/ob_ddl_service.h"
#include "rootserver/ob_ddl_operator.h"
#include "share/ob_rpc_struct.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_schema_struct.h"

using namespace oceanbase::rootserver;
using namespace oceanbase::common;
using namespace oceanbase::obrpc;
using namespace oceanbase::share::schema;
using namespace oceanbase::transaction::tablelock;

int ObAlterTablePartitionAction::register_ddl_object_locks()
{
  int ret = OB_SUCCESS;
  const ObTableSchema *orig_table_schema = nullptr;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_ISNULL(orig_table_schema = get_orig_table_schema())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("orig table schema is null", KR(ret));
  } else {
    // 1. EXCLUSIVE lock on data table ID
    const uint64_t table_id = orig_table_schema->get_table_id();
    if (OB_FAIL(add_ddl_object_lock_by_id(table_id, TABLE_SCHEMA, EXCLUSIVE))) {
      LOG_WARN("fail to add lock on data table", KR(ret), K(table_id));
    }
    // 2. EXCLUSIVE lock on index IDs (including both local and global indexes)
    const ObIArray<ObAuxTableMetaInfo> &index_infos = orig_table_schema->get_simple_index_infos();
    for (int64_t i = 0; OB_SUCC(ret) && i < index_infos.count(); i++) {
      if (OB_FAIL(add_ddl_object_lock_by_id(index_infos.at(i).table_id_, TABLE_SCHEMA, EXCLUSIVE))) {
        LOG_WARN("fail to add lock on index", KR(ret), K(index_infos.at(i).table_id_));
      }
    }
    // 3. EXCLUSIVE lock on LOB aux tables
    const uint64_t lob_meta_tid = orig_table_schema->get_aux_lob_meta_tid();
    const uint64_t lob_piece_tid = orig_table_schema->get_aux_lob_piece_tid();
    if (OB_SUCC(ret) && OB_INVALID_ID != lob_meta_tid) {
      if (OB_FAIL(add_ddl_object_lock_by_id(lob_meta_tid, TABLE_SCHEMA, EXCLUSIVE))) {
        LOG_WARN("fail to add lock on lob meta", KR(ret), K(lob_meta_tid));
      }
    }
    if (OB_SUCC(ret) && OB_INVALID_ID != lob_piece_tid) {
      if (OB_FAIL(add_ddl_object_lock_by_id(lob_piece_tid, TABLE_SCHEMA, EXCLUSIVE))) {
        LOG_WARN("fail to add lock on lob piece", KR(ret), K(lob_piece_tid));
      }
    }
    // 4. EXCLUSIVE lock on mlog table
    const uint64_t mlog_tid = orig_table_schema->get_mlog_tid();
    if (OB_SUCC(ret) && OB_INVALID_ID != mlog_tid) {
      if (OB_FAIL(add_ddl_object_lock_by_id(mlog_tid, TABLE_SCHEMA, EXCLUSIVE))) {
        LOG_WARN("fail to add lock on mlog", KR(ret), K(mlog_tid));
      }
    }
    // 5. EXCLUSIVE lock on search def child indexes
    for (int64_t i = 0; OB_SUCC(ret) && i < index_infos.count(); i++) {
      const ObTableSchema *index_schema = nullptr;
      if (OB_FAIL(get_schema_guard_wrapper().get_table_schema(
          index_infos.at(i).table_id_, index_schema))) {
        LOG_WARN("fail to get index schema", KR(ret), K(index_infos.at(i).table_id_));
      } else if (OB_ISNULL(index_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("index schema is null", KR(ret), K(index_infos.at(i).table_id_));
      } else if (OB_FAIL(helper_.add_based_schema_object_info(ObBasedSchemaObjectInfo(
          index_schema->get_table_id(), TABLE_SCHEMA, index_schema->get_schema_version())))) {
        LOG_WARN("fail to record index schema version", KR(ret), K(index_infos.at(i).table_id_));
      } else if (index_schema->is_search_def_index()) {
        ObSEArray<ObAuxTableMetaInfo, 16> child_infos;
        if (OB_FAIL(index_schema->get_simple_index_infos(child_infos))) {
          LOG_WARN("fail to get child index infos", KR(ret), K(index_infos.at(i).table_id_));
        }
        for (int64_t j = 0; OB_SUCC(ret) && j < child_infos.count(); j++) {
          if (OB_FAIL(add_ddl_object_lock_by_id(child_infos.at(j).table_id_, TABLE_SCHEMA, EXCLUSIVE))) {
            LOG_WARN("fail to add lock on search def child", KR(ret), K(child_infos.at(j).table_id_));
          }
        }
      }
    }
    // 6. EXCLUSIVE lock on foreign key related tables
    const ObIArray<ObForeignKeyInfo> &foreign_key_infos = orig_table_schema->get_foreign_key_infos();
    for (int64_t i = 0; OB_SUCC(ret) && i < foreign_key_infos.count(); i++) {
      const ObForeignKeyInfo &foreign_key_info = foreign_key_infos.at(i);
      const uint64_t related_table_id = table_id == foreign_key_info.parent_table_id_
          ? foreign_key_info.child_table_id_
          : foreign_key_info.parent_table_id_;
      if (related_table_id == table_id) {
      } else if (OB_FAIL(add_ddl_object_lock_by_id(related_table_id, TABLE_SCHEMA, EXCLUSIVE))) {
        LOG_WARN("fail to add lock on fk related table", KR(ret), K(related_table_id));
      }
    }
  }
  return ret;
}

int ObAlterTablePartitionAction::check_legitimacy()
{
  int ret = OB_SUCCESS;
  const ObTableSchema *orig_table_schema = nullptr;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_ISNULL(orig_table_schema = get_orig_table_schema())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("orig table schema is null", KR(ret));
  } else if (OB_FAIL(ObDDLService::fill_part_name_if_needed(
      *orig_table_schema,
      const_cast<AlterTableSchema &>(get_arg().alter_table_schema_)))) {
    LOG_WARN("fail to fill part name", KR(ret));
  } else if (OB_FAIL(get_ddl_service()->check_restore_point_allow(
      get_tenant_id(), *orig_table_schema))) {
    LOG_WARN("check restore point allow failed", KR(ret),
             K(get_tenant_id()), KPC(orig_table_schema));
  } else if (OB_FAIL(get_ddl_service()->check_alter_partitions(
      *orig_table_schema, const_cast<obrpc::ObAlterTableArg &>(get_arg()),
      get_schema_guard_wrapper()))) {
    LOG_WARN("check alter partitions failed", KR(ret), KPC(orig_table_schema));
  }
  return ret;
}

int ObAlterTablePartitionAction::generate_schemas()
{
  int ret = OB_SUCCESS;
  const ObTableSchema *orig_table_schema = nullptr;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_ISNULL(orig_table_schema = get_orig_table_schema())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("orig table schema is null", KR(ret));
  } else if (OB_FAIL(new_table_schema_.assign(*orig_table_schema))) {
    LOG_WARN("fail to assign new table schema", KR(ret));
  } else if (OB_FAIL(generate_tables_array_(
      *orig_table_schema, new_table_schema_,
      const_cast<AlterTableSchema &>(get_arg().alter_table_schema_)))) {
    LOG_WARN("fail to generate tables array", KR(ret));
  }
  return ret;
}

int ObAlterTablePartitionAction::operate_schemas()
{
  int ret = OB_SUCCESS;
  const ObTableSchema *orig_table_schema = nullptr;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_ISNULL(orig_table_schema = get_orig_table_schema())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("orig table schema is null", KR(ret));
  } else {
    ObDDLOperator ddl_operator(*get_schema_service(), *get_sql_proxy());
    ObDDLSQLTransaction &trans = get_trans();
    const uint64_t tenant_id = get_tenant_id();
    uint64_t tenant_data_version = 0;
    const ObSchemaOperationType operation_type =
        ObDDLService::get_alter_table_schema_operation_type(get_arg());
    if (OB_FAIL(ddl_operator.alter_table_options(
        get_schema_guard_wrapper(),
        new_table_schema_,
        *orig_table_schema,
        false /* need_update_aux_table */,
        trans))) {
      LOG_WARN("fail to alter table options", KR(ret));
    } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, tenant_data_version))) {
      LOG_WARN("fail to get min data version", KR(ret), K(tenant_id));
    } else if (OB_FAIL(get_ddl_service()->update_global_index(
        const_cast<ObAlterTableArg &>(get_arg()),
        tenant_id,
        *orig_table_schema,
        ddl_operator,
        trans,
        tenant_data_version,
        get_res(),
        get_ddl_task_records(),
        get_schema_guard_wrapper(),
        global_index_op_types_))) {
      LOG_WARN("fail to update global index", KR(ret));
    } else if (OB_FAIL(get_ddl_service()->alter_tables_partitions(
        get_arg(),
        orig_table_schemas_,
        new_table_schemas_,
        inc_table_schemas_,
        del_table_schemas_,
        upd_table_schemas_,
        ddl_operator,
        get_schema_guard_wrapper(),
        trans))) {
      LOG_WARN("fail to alter tables partitions", KR(ret));
    } else if (OB_FAIL(get_ddl_service()->update_tables_attribute(
        new_table_schemas_, ddl_operator, trans, operation_type, get_arg().ddl_stmt_str_))) {
      LOG_WARN("fail to update tables attribute", KR(ret));
    }
  }
  return ret;
}

int ObAlterTablePartitionAction::pre_decide_global_index_op_types_()
{
  int ret = OB_SUCCESS;
  const ObTableSchema *orig_table_schema = nullptr;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_ISNULL(orig_table_schema = get_orig_table_schema())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("orig table schema is null", KR(ret));
  } else if (OB_FAIL(get_ddl_service()->pre_decide_global_index_op_types(
      get_arg(), get_tenant_id(), *orig_table_schema,
      get_schema_guard_wrapper(), global_index_op_types_))) {
    LOG_WARN("fail to pre decide global index op types", KR(ret));
  }
  return ret;
}

int ObAlterTablePartitionAction::calc_global_index_schema_version_cnt_(int64_t &cnt) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < global_index_op_types_.count(); ++i) {
    const ObDDLService::UpdateGlobalIndexOpType op_type = global_index_op_types_.at(i);
    switch (op_type) {
      case ObDDLService::WRITE_TRUNCATE_INFO:
        cnt += 1; // update_table_attribute for index
        break;
      case ObDDLService::MARK_INDEX_UNUSABLE:
        cnt += 2; // update_index_status(1v) + update_data_table_schema_version(1v)
        break;
      case ObDDLService::USE_OLD_UPDATE_FUNC:
        if (get_arg().is_update_global_indexes_) {
          // drop_table: drop_table_for_not_dropped_schema(1v) + update_data_table_schema_version(1v)
          //           + drop_tablet_of_table(1v)
          // create_table: gen_new_schema_version(1v) + update_data_table_schema_version(1v) = 5v
          cnt += 5;
        } else {
          cnt += 2; // update_index_status(1v) + update_data_table_schema_version(1v)
        }
        break;
      case ObDDLService::DROP_AND_CREATE_INDEX:
        // update_index_status(1v + 1v update_data_table_schema_version)
        // + create_table(1v + 1v update_data_table_schema_version)
        // + rename_dropping_index_name in before_commit(1v + 1v update_data_table_schema_version) = 6v
        cnt += 6;
        break;
      case ObDDLService::MAX_OP_TYPE:
        // non-global or non-applicable index, no schema version needed
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected op type", KR(ret), K(op_type), K(i));
        break;
    }
  }
  return ret;
}

int ObAlterTablePartitionAction::generate_tables_array_(
    const ObTableSchema &orig_table_schema,
    ObTableSchema &new_table_schema,
    AlterTableSchema &inc_table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(get_ddl_service()->generate_tables_array(
      get_arg(),
      orig_table_schemas_,
      new_table_schemas_,
      inc_table_schemas_,
      del_table_schemas_,
      upd_table_schemas_,
      orig_table_schema,
      new_table_schema,
      inc_table_schema,
      get_schema_guard_wrapper(),
      get_allocator()))) {
    LOG_WARN("fail to generate tables array", KR(ret));
  }
  return ret;
}
