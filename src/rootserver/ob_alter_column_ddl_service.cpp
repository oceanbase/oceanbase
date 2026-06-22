/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX RS

#include "rootserver/ob_ddl_service.h"
#include "rootserver/ob_sensitive_rule_ddl_service.h"
#include "share/schema/ob_schema_utils.h"
#include "share/ob_ddl_common.h"
#include "sql/resolver/ddl/ob_ddl_resolver.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;
using namespace obrpc;

namespace rootserver
{

int ObDDLService::check_can_drop_column_instant_(const ObTableSchema &orig_table_schema,
                                                 const AlterTableSchema &alter_table_schema,
                                                 const uint64_t tenant_data_version,
                                                 const bool is_oracle_mode,
                                                 obrpc::ObAlterTableArg &alter_table_arg)
{
  int ret = OB_SUCCESS;
  bool drop_column_instant = false;
  bool has_other_ddl_except_drop = false;
  AlterColumnSchema *alter_column_schema = NULL;
  const uint64_t tenant_id = orig_table_schema.get_tenant_id();
  ObTableSchema::const_column_iterator it_begin = alter_table_schema.column_begin();
  ObTableSchema::const_column_iterator it_end = alter_table_schema.column_end();
  for (; OB_SUCC(ret) && !(drop_column_instant && has_other_ddl_except_drop) && it_begin != it_end; it_begin++) {
    if (OB_ISNULL(alter_column_schema = static_cast<AlterColumnSchema *>(*it_begin))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("alter_column_schema is NULL", KR(ret), K(alter_table_schema));
    } else {
      const ObSchemaOperationType op_type = alter_column_schema->alter_type_;
      switch (op_type) {
        case OB_DDL_DROP_COLUMN: {
          const ObString &orig_column_name = alter_column_schema->get_origin_column_name();
          const ObColumnSchemaV2 *orig_column_schema = orig_table_schema.get_column_schema(orig_column_name);
          if (OB_ISNULL(orig_column_schema)) {
            ret = OB_ERR_CANT_DROP_FIELD_OR_KEY;
            LOG_USER_ERROR(OB_ERR_CANT_DROP_FIELD_OR_KEY, orig_column_name.length(), orig_column_name.ptr());
            LOG_WARN("fail to find old column schema", KR(ret), K(orig_column_name), KPC(orig_column_schema));
          } else {
            ObDDLType per_col_type = ObDDLType::DDL_INVALID;
            int64_t algo_hint = static_cast<int64_t>(alter_table_arg.alter_algorithm_);
            if (OB_FAIL(share::ObAlterColumnDDLHelper::compute_drop_column_ddl_type(*orig_column_schema, tenant_id,
                                                                                    is_oracle_mode, tenant_data_version,
                                                                                    per_col_type, algo_hint))) {
              LOG_WARN("fail to compute drop column ddl type", KR(ret), K(orig_column_name));
            } else {
              alter_table_arg.alter_algorithm_ = static_cast<obrpc::ObAlterTableArg::AlterAlgorithm>(algo_hint);
              if (ObDDLType::DDL_DROP_COLUMN_INSTANT == per_col_type) {
                drop_column_instant = true;
              }
            }
          }
          break;
        }
        case OB_DDL_ADD_COLUMN:
        case OB_DDL_MODIFY_COLUMN:
        case OB_DDL_CHANGE_COLUMN:
        case OB_DDL_ALTER_COLUMN: {
          has_other_ddl_except_drop = true;
          break;
        }
        default: {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("unhandled operator type!", KR(ret), K(op_type));
          break;
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    // drop column instant compound other ddl should do offline ddl
    if (drop_column_instant && has_other_ddl_except_drop) {
      alter_table_arg.alter_algorithm_ = obrpc::ObAlterTableArg::AlterAlgorithm::INPLACE;
      LOG_INFO("drop column instant with other ddl should do offline ddl");
    }
  }
  return ret;
}

int ObDDLService::check_mysql_drop_column_with_index_(obrpc::ObAlterTableArg &alter_table_arg,
                                                      share::schema::ObSchemaGetterGuard &schema_guard,
                                                      const share::schema::ObTableSchema &orig_table_schema,
                                                      const uint64_t tenant_data_version,
                                                      const bool is_oracle_mode,
                                                      ObDDLType &ddl_type)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> drop_cols_id_arr;
  // TODO(xingrui.cwh) after miyuan support mutli or other type index do offline ddl,
  // delete check_has_domain_index and check_has_vec_domain_index constraint
  bool has_vec_index = false;
  bool has_fts_or_multivalue_index = false;
  const uint64_t tenant_id = orig_table_schema.get_tenant_id();
  const uint64_t table_id = orig_table_schema.get_table_id();
  ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check inner stat failed", KR(ret));
  } else if (ObDDLType::DDL_DROP_COLUMN != ddl_type) {
    // ignore
  } else if (is_oracle_mode
              || (!is_oracle_mode && tenant_data_version < DATA_VERSION_4_3_5_2)) {
    // ignore
  } else if (OB_FAIL(orig_table_schema.get_simple_index_infos(simple_index_infos))) {
    LOG_WARN("fail to get simple index infos", KR(ret));
  } else if (OB_FAIL(get_all_dropped_column_ids(alter_table_arg, orig_table_schema, drop_cols_id_arr))) {
    LOG_WARN("fail to prefetch all drop columns id", KR(ret), K(alter_table_arg));
  } else {
    bool has_this_col = false;
    int64_t index_count = simple_index_infos.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < index_count && !has_this_col; i++) {
      const ObTableSchema *index_schema = nullptr;
      if (OB_FAIL(schema_guard.get_table_schema(tenant_id, simple_index_infos.at(i).table_id_, index_schema))) {
        LOG_WARN("get index schema failed", KR(ret), K(tenant_id), "table id", simple_index_infos.at(i).table_id_);
      } else if (OB_ISNULL(index_schema)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("index not exist", KR(ret), K(tenant_id), "table id", simple_index_infos.at(i).table_id_);
      } else if (OB_UNLIKELY(index_schema->is_in_recyclebin())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error", KR(ret), K(tenant_id), "index_tid", simple_index_infos.at(i).table_id_);
      } else {
        FOREACH_CNT_X(each_col_id, drop_cols_id_arr, OB_SUCC(ret) && !has_this_col) {
          if (OB_FAIL(index_schema->has_column(*each_col_id, has_this_col))) {
            LOG_WARN("check has column failed", KR(ret), K(*each_col_id), KPC(index_schema));
          } else if (has_this_col) {
            ddl_type = DDL_TABLE_REDEFINITION;
            LOG_TRACE("drop column with index, it should do offline ddl", K(ddl_type));
          }
        }
      }
    }
  }
  return ret;
}

int ObDDLService::drop_constraint_caused_by_drop_column(
    const obrpc::ObAlterTableArg &alter_table_arg,
    ObSchemaGuardWrapper &schema_guard,
    const ObTableSchema &orig_table_schema,
    ObTableSchema &new_table_schema,
    ObDDLOperator &ddl_operator,
    common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const AlterTableSchema &alter_table_schema = alter_table_arg.alter_table_schema_;
  if (OB_FAIL(check_can_alter_table_constraints(obrpc::ObAlterTableArg::DROP_CONSTRAINT,
                                                schema_guard, orig_table_schema, alter_table_schema))) {
    LOG_WARN("fail to check can alter constraints", KR(ret), K(alter_table_schema));
  } else if (OB_FAIL(ddl_operator.drop_table_constraints(orig_table_schema, alter_table_schema,
                                                          new_table_schema, trans))) {
    LOG_WARN("failed to drop table constraints", KR(ret), K(alter_table_schema));
  } else if (OB_FAIL(delete_constraint_update_new_table(alter_table_schema, new_table_schema))) {
    LOG_WARN("fail to delete constraints from new table", KR(ret), K(alter_table_schema));
  }
  return ret;
}

int ObDDLService::drop_rls_policy_caused_by_drop_column_online(
    ObSchemaGuardWrapper &schema_guard,
    const share::schema::ObTableSchema &origin_table_schema,
    const common::ObIArray<uint64_t> &drop_cols_id_arr,
    share::schema::ObTableSchema &new_table_schema,
    ObDDLOperator &ddl_operator,
    common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObString empty_ddl_stmt;
  bool rls_object_changed = false;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check inner stat failed", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < origin_table_schema.get_rls_policy_ids().count(); i++) {
      ObRlsPolicySchema new_rls_policy;
      const ObRlsPolicySchema *policy_schema = nullptr;
      const uint64_t policy_id = origin_table_schema.get_rls_policy_ids().at(i);
      if (OB_FAIL(schema_guard.get_rls_policy_schema_by_id(origin_table_schema.get_tenant_id(),
                                                           policy_id,
                                                           policy_schema))) {
        LOG_WARN("get rls policy schema failed", KR(ret), K(policy_id));
      } else if (OB_ISNULL(policy_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null policy", KR(ret), K(policy_id));
      } else if (OB_FAIL(new_rls_policy.rebuild_with_table_schema(*policy_schema, new_table_schema))) {
        LOG_WARN("rebuild with table schema failed", KR(ret), K(policy_id));
      } else if (new_rls_policy.get_sec_column_count() == policy_schema->get_sec_column_count()) {
        // do nothing, the policy does not releated to the dropped column.
      } else if (OB_FALSE_IT(rls_object_changed = true)) {
      } else if (OB_FAIL(ddl_operator.drop_rls_policy(*policy_schema, trans, empty_ddl_stmt,
          false/*is_update_table_schema*/, nullptr/*table_schema*/))) {
        LOG_WARN("drop origin policy failed", KR(ret));
      } else if (policy_schema->is_column_level_policy() && !new_rls_policy.is_column_level_policy()) {
        // column level policy will be dropped after drop column
      } else if (OB_FAIL(ddl_operator.create_rls_policy(new_rls_policy, trans, empty_ddl_stmt,
          false/*is_update_table_schema*/, nullptr/*table_schema*/))) {
        LOG_WARN("create new policy failed", KR(ret));
      }
    }
    if (OB_SUCC(ret) && rls_object_changed) {
      if (OB_FAIL(update_new_table_rls_flag(schema_guard, drop_cols_id_arr, new_table_schema))) {
        LOG_WARN("fail to update new table flags", KR(ret));
      } else if (OB_FAIL(ddl_operator.update_table_attribute(new_table_schema, trans, OB_DDL_ALTER_TABLE))) {
        LOG_WARN("fail to update table schema", KR(ret));
      }
    }
  }
  return ret;
}

int ObDDLService::drop_index_caused_by_drop_column_online(
    ObSchemaGuardWrapper &schema_guard,
    const share::schema::ObTableSchema &origin_table_schema,
    const common::ObIArray<uint64_t> &drop_cols_id_arr,
    common::ObIAllocator &allocator,
    ObDDLOperator &ddl_operator,
    common::ObMySQLTransaction &trans,
    common::ObIArray<ObDDLTaskRecord> &ddl_task_records)
{
  int ret = OB_SUCCESS;
  bool is_oracle_mode = false;
  ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
  const uint64_t tenant_id = origin_table_schema.get_tenant_id();
  const ObDatabaseSchema *database_schema = nullptr;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check inner stat failed", KR(ret));
  } else if (OB_UNLIKELY(drop_cols_id_arr.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(drop_cols_id_arr));
  } else if (OB_FAIL(origin_table_schema.check_if_oracle_compat_mode(is_oracle_mode))) {
    LOG_WARN("check compat mode failed", KR(ret));
  } else if (origin_table_schema.is_tmp_table() && !origin_table_schema.is_mysql_tmp_table()) {
    ret = OB_OP_NOT_ALLOW;
    char err_msg[OB_MAX_ERROR_MSG_LEN] = {0};
    (void)snprintf(err_msg, sizeof(err_msg), "drop column on oracle temporary table is");
    LOG_WARN("drop column on oracle temporary table is disallowed", KR(ret));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, err_msg);
  } else if (OB_FAIL(schema_guard.get_database_schema(tenant_id, origin_table_schema.get_database_id(), database_schema))) {
    LOG_WARN("get database schema failed", KR(ret), K(tenant_id), "db_id", origin_table_schema.get_database_id());
  } else if (OB_ISNULL(database_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", KR(ret), K(tenant_id), "db_id", origin_table_schema.get_database_id());
  } else if (OB_UNLIKELY(database_schema->is_in_recyclebin())) {
    ret = OB_ERR_OPERATION_ON_RECYCLE_OBJECT;
    LOG_WARN("can not drop table column in recyclebin", KR(ret), K(tenant_id), "db_id", origin_table_schema.get_database_id());
  } else if (OB_FAIL(origin_table_schema.get_simple_index_infos(simple_index_infos))) {
    LOG_WARN("fail to get simple index infos", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); i++) {
      const ObTableSchema *index_schema = nullptr;
      if (OB_FAIL(schema_guard.get_table_schema(tenant_id, simple_index_infos.at(i).table_id_, index_schema))) {
        LOG_WARN("get index schema failed", KR(ret), K(tenant_id), "table id", simple_index_infos.at(i).table_id_);
      } else if (OB_ISNULL(index_schema)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("index not exist", KR(ret), K(tenant_id), "table id", simple_index_infos.at(i).table_id_);
      } else if (OB_UNLIKELY(index_schema->is_in_recyclebin())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error", KR(ret), K(tenant_id), "index_tid", simple_index_infos.at(i).table_id_);
      } else {
        ObString index_name;
        bool has_this_col = false;
        FOREACH_CNT_X(each_col_id, drop_cols_id_arr, OB_SUCC(ret) && !has_this_col) {
          if (OB_FAIL(index_schema->has_column(*each_col_id, has_this_col))) {
            LOG_WARN("check has column failed", KR(ret), K(*each_col_id), KPC(index_schema));
          } else if (!has_this_col) {
          } else if (has_this_col && !is_oracle_mode) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("drop column with index should do offline ddl in mysql mode", KR(ret));
          } else if (OB_FAIL(index_schema->get_index_name(index_name))) {
            LOG_WARN("get index name without prefix failed", KR(ret), KPC(index_schema));
          } else if (index_schema->is_oracle_tmp_table_v2_index_table()) {
            //construct an arg for drop table
            ObTableItem table_item;
            table_item.database_name_ = database_schema->get_database_name_str();
            table_item.table_name_ = index_schema->get_table_name();
            table_item.is_hidden_ = index_schema->is_user_hidden_table();
            table_item.table_id_ = index_schema->get_table_id();
            obrpc::ObDDLRes ddl_res;
            obrpc::ObDropTableArg drop_table_arg;
            drop_table_arg.tenant_id_ = tenant_id;
            drop_table_arg.if_exist_ = false;
            drop_table_arg.table_type_ = index_schema->get_table_type();
            drop_table_arg.force_drop_ = index_schema->is_in_recyclebin();
            if (OB_FAIL(drop_table_arg.tables_.push_back(table_item))) {
              LOG_WARN("failed to add table item!", K(table_item), KR(ret));
            } else if (OB_FAIL(drop_table(drop_table_arg, ddl_res))) {
              if (OB_TABLE_NOT_EXIST == ret) {
                ret = OB_ERR_CANT_DROP_FIELD_OR_KEY;
                LOG_USER_ERROR(OB_ERR_CANT_DROP_FIELD_OR_KEY, index_name.length(), index_name.ptr());
                LOG_WARN("index not exist, can't drop it", K(drop_table_arg), KR(ret));
              } else {
                LOG_WARN("drop_table failed", K(drop_table_arg), KR(ret));
              }
            }
          } else {
            ObDDLTaskRecord drop_index_task_record;
            obrpc::ObDropIndexArg drop_index_arg;
            drop_index_arg.tenant_id_         = tenant_id;
            drop_index_arg.exec_tenant_id_    = tenant_id;
            drop_index_arg.index_table_id_    = index_schema->get_table_id();
            drop_index_arg.session_id_        = schema_guard.get_session_id();
            drop_index_arg.index_name_        = index_name;
            drop_index_arg.table_name_        = origin_table_schema.get_table_name();
            drop_index_arg.database_name_     = database_schema->get_database_name_str();
            drop_index_arg.index_action_type_ = obrpc::ObIndexArg::DROP_INDEX;
            // drop_index_arg.ddl_stmt_str_      = // empty.
            drop_index_arg.is_add_to_scheduler_ = false;
            drop_index_arg.is_hidden_         = origin_table_schema.is_user_hidden_table();
            drop_index_arg.is_in_recyclebin_  = index_schema->is_in_recyclebin();
            drop_index_arg.is_inner_          = true;
            ObArray<obrpc::ObDDLRes> unused_ddl_res_array;
            if (OB_FAIL(drop_index_to_scheduler_(trans, schema_guard, allocator, origin_table_schema,
                                                 nullptr/*inc_tablet_ids*/, nullptr/*del_tablet_ids*/,
                                                 &drop_index_arg, ddl_operator,
                                                 unused_ddl_res_array, ddl_task_records))) {
              LOG_WARN("fail to drop index to scheduler", KR(ret), K(drop_index_arg));
            }
          }
        }
      }
    }
  }

  return ret;
}

int ObDDLService::drop_lob_caused_by_drop_column_online_if_need(
    const obrpc::ObAlterTableArg &alter_table_arg,
    const share::schema::ObTableSchema &origin_table_schema,
    const share::schema::ObTableSchema &new_table_schema,
    common::ObIAllocator &allocator,
    common::ObMySQLTransaction &trans,
    common::ObIArray<ObDDLTaskRecord> &ddl_task_records,
    obrpc::ObAlterTableRes &res)
{
  int ret = OB_SUCCESS;
  bool is_oracle_mode = false;
  uint64_t tenant_data_version = 0;
  // To check whether there is any lob column before and after this drop column operation,
  // If so, a drop lob tablet task will be created to remove lob meta/piece tablet.
  const bool has_lob_column_before = origin_table_schema.has_lob_column(true/*ignore_unused_column*/);
  const bool has_lob_column_after = new_table_schema.has_lob_column(true/*ignore_unused_column*/);
  const uint64_t tenant_id = origin_table_schema.get_tenant_id();
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check inner stat failed", KR(ret));
  } else if (has_lob_column_before && !has_lob_column_after) {
    ObDDLTaskRecord task_record;
    if (OB_FAIL(submit_drop_lob_task_(trans, new_table_schema, alter_table_arg, allocator, task_record))) {
      LOG_WARN("submit drop lob task failed", KR(ret));
    } else if (OB_FAIL(ddl_task_records.push_back(task_record))) {
      LOG_WARN("push back failed", KR(ret));
    } else {
      // for wait ddl finish.
      res.task_id_ = task_record.task_id_;
    }
  }
  return ret;
}

int ObDDLService::check_can_drop_column(
    const ObString &orig_column_name,
    const ObColumnSchemaV2 *orig_column_schema,
    const ObTableSchema &orig_table_schema,
    const ObTableSchema &new_table_schema,
    const int64_t new_tbl_visible_cols_cnt_after_alter,
    const int64_t last_drop_column_id,
    ObSchemaGuardWrapper &schema_guard)
{
  int ret = OB_SUCCESS;
  int64_t column_count = new_tbl_visible_cols_cnt_after_alter;
  bool is_last_drop_column = false;
  if (OB_ISNULL(orig_column_schema) || OB_ISNULL(new_table_schema.get_column_schema(orig_column_name))) {
    ret = OB_ERR_CANT_DROP_FIELD_OR_KEY;
    LOG_USER_ERROR(OB_ERR_CANT_DROP_FIELD_OR_KEY, orig_column_name.length(), orig_column_name.ptr());
    LOG_WARN("fail to find old column schema!", KR(ret), K(orig_column_name), KP(orig_column_schema),
        K(new_table_schema));
  } else if (FALSE_IT(is_last_drop_column = (last_drop_column_id == orig_column_schema->get_column_id()) ? true : false)) {
  } else if (orig_table_schema.is_oracle_tmp_table() || orig_table_schema.is_oracle_tmp_table_v2()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "drop column on oracle temporary table is");
    LOG_WARN("oracle temporary table column is not allowed to be dropped", KR(ret));
  } else if (orig_column_schema->has_generated_column_deps()) {
    bool has_func_idx_col_deps = false;
    bool is_oracle_mode = false;
    if (OB_FAIL(orig_table_schema.check_if_oracle_compat_mode(is_oracle_mode))) {
      LOG_WARN("fail to check if oracle compat mode", KR(ret));
    } else if (!is_oracle_mode && OB_FAIL(orig_table_schema.check_functional_index_columns_depend(*orig_column_schema, schema_guard, has_func_idx_col_deps))) {
      LOG_WARN("fail to check if column has functional index dependency.", KR(ret));
    } else if (!has_func_idx_col_deps) {
      ret = OB_ERR_DEPENDENT_BY_GENERATED_COLUMN;
      LOG_USER_ERROR(OB_ERR_DEPENDENT_BY_GENERATED_COLUMN, orig_column_name.length(), orig_column_name.ptr());
      LOG_WARN("Dropping column has generated column deps", KR(ret), K(orig_column_name));
    } else {
      ret = OB_ERR_DEPENDENT_BY_FUNCTIONAL_INDEX;
      LOG_USER_ERROR(OB_ERR_DEPENDENT_BY_FUNCTIONAL_INDEX, orig_column_name.length(), orig_column_name.ptr());
      LOG_WARN("Dropping column has functional index column deps", KR(ret), K(orig_column_name));
    }
  } else if (OB_FAIL(check_is_drop_partition_key(orig_table_schema, *orig_column_schema, schema_guard))) {
    LOG_WARN("check drop partition column failed", KR(ret));
  } else if (is_last_drop_column && column_count < ObTableSchema::MIN_COLUMN_COUNT) {
    ret = OB_CANT_REMOVE_ALL_FIELDS;
    LOG_USER_ERROR(OB_CANT_REMOVE_ALL_FIELDS);
    LOG_WARN("Can not delete all columns in table", KR(ret), K(column_count));
  } else if (orig_column_schema->is_rowkey_column()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "drop rowkey column is");
    LOG_WARN("rowkey column is not allowed to be dropped", KR(ret), K(orig_column_schema->get_column_name_str()));
  }
  return ret;
}

int ObDDLService::check_is_drop_partition_key(
    const share::schema::ObTableSchema &orig_table_schema,
    const ObColumnSchemaV2 &to_drop_column,
    ObSchemaGuardWrapper &schema_guard)
{
  int ret = OB_SUCCESS;
  bool is_tbl_partition_key = false;
  ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
  const uint64_t tenant_id = orig_table_schema.get_tenant_id();
  const uint64_t to_drop_column_id = to_drop_column.get_column_id();
  const ObPartitionOption &part_option = orig_table_schema.get_part_option();
  const ObString &part_func_str = part_option.get_part_func_expr_str();
  if (OB_FAIL(orig_table_schema.get_simple_index_infos(simple_index_infos))) {
    LOG_WARN("get index infos failed", KR(ret));
  } else if (!part_func_str.empty()/*nonempty means user-defined part key*/ && OB_FAIL(orig_table_schema.is_tbl_partition_key(to_drop_column_id, is_tbl_partition_key,
                                                           false /* ignore_presetting_key */))) {
    LOG_WARN("fail to check tbl partition key", KR(ret), K(to_drop_column), K(orig_table_schema));
  } else {
    // to check whether the column is the partition key of the global index.
    for (int64_t idx = 0; OB_SUCC(ret) && !is_tbl_partition_key && idx < simple_index_infos.count(); idx++) {
      const ObTableSchema *index_schema = nullptr;
      const uint64_t index_tid = simple_index_infos.at(idx).table_id_;
      if (OB_FAIL(schema_guard.get_table_schema(tenant_id, index_tid, index_schema))) {
        LOG_WARN("get index schema failed", KR(ret), K(tenant_id), K(index_tid));
      } else if (OB_ISNULL(index_schema)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("index not exist", KR(ret), K(tenant_id), K(index_tid));
      } else if (index_schema->is_global_index_table()) {
        const ObPartitionOption &part_option = index_schema->get_part_option();
        const ObString &part_func_str = part_option.get_part_func_expr_str();
        if (!part_func_str.empty()/*nonempty means user-defined part key*/
            && OB_FAIL(index_schema->is_tbl_partition_key(to_drop_column_id, is_tbl_partition_key, false /* ignore_presetting_key */))) {
          LOG_WARN("check column in part key failed", KR(ret), K(index_tid));
        }
      }
    }
  }
  if (OB_SUCC(ret) && is_tbl_partition_key) {
    ret = OB_ERR_DEPENDENT_BY_PARTITION_FUNC;
    LOG_USER_ERROR(OB_ERR_DEPENDENT_BY_PARTITION_FUNC,
                  to_drop_column.get_column_name_str().length(),
                  to_drop_column.get_column_name_str().ptr());
    LOG_WARN("drop column has table part key deps", KR(ret), "column_name", to_drop_column.get_column_name_str());
  }
  return ret;
}

// to check whether the dropped column is related to constraint, and check can drop the column.
int ObDDLService::check_drop_column_with_drop_constraint(
    const obrpc::ObAlterTableArg &alter_table_arg,
    ObSchemaGuardWrapper &schema_guard,
    const ObTableSchema &orig_table_schema,
    const common::ObIArray<uint64_t> &drop_cols_id_arr)
{
  int ret = OB_SUCCESS;
  bool is_oracle_mode = false;
  const AlterTableSchema &alter_table_schema = alter_table_arg.alter_table_schema_;
  const ObAlterTableArg::AlterConstraintType type = alter_table_arg.alter_constraint_type_;
  if (OB_FAIL(orig_table_schema.check_if_oracle_compat_mode(is_oracle_mode))) {
    LOG_WARN("fail to check if oracle compat mode", KR(ret));
  } else if (OB_FAIL(check_can_alter_table_constraints(type, schema_guard, orig_table_schema, alter_table_schema))) {
    LOG_WARN("fail to check can alter constraints", KR(ret), K(type), K(alter_table_schema));
  } else {
    FOREACH_CNT_X(dropped_col, drop_cols_id_arr, OB_SUCC(ret)) {
      for (ObTableSchema::const_constraint_iterator iter = orig_table_schema.constraint_begin(); OB_SUCC(ret) &&
        iter != orig_table_schema.constraint_end(); iter++) {
        if (CONSTRAINT_TYPE_CHECK != (*iter)->get_constraint_type()) {
        } else if (0 == (*iter)->get_column_cnt()) {
        } else {
          const ObString &cst_name = (*iter)->get_constraint_name_str();
          for (ObConstraint::const_cst_col_iterator cst_col_iter = (*iter)->cst_col_begin();
            OB_SUCC(ret) && (cst_col_iter != (*iter)->cst_col_end()); ++cst_col_iter) {
            if (*cst_col_iter == *dropped_col) {
              // the dropped column is related to check constraint.
              const ObString &dropped_column_name = orig_table_schema.get_column_schema(*dropped_col)->get_column_name_str();
              ObConstraint* const* res = std::find_if(alter_table_schema.constraint_begin(), alter_table_schema.constraint_end(), [&cst_name](const ObConstraint* cst)
                             { return 0 == cst_name.case_compare(cst->get_constraint_name_str()); });
              if (alter_table_schema.constraint_end() == res) {
                LOG_WARN("the column is related to check constraint, can not be dropped", KR(ret), K(cst_name), K(dropped_column_name));
                if (is_oracle_mode) {
                  ret = OB_ERR_MODIFY_OR_DROP_MULTI_COLUMN_CONSTRAINT;
                  LOG_USER_ERROR(OB_ERR_DROP_COL_REFERENCED_MULTI_COLS_CONSTRAINT,
                                 dropped_column_name.length(), dropped_column_name.ptr(),
                                 cst_name.length(), cst_name.ptr());
                } else {
                  ret = OB_ERR_DROP_COL_REFERENCED_MULTI_COLS_CONSTRAINT;
                  LOG_USER_ERROR(OB_ERR_DROP_COL_REFERENCED_MULTI_COLS_CONSTRAINT,
                                 cst_name.length(), cst_name.ptr(), dropped_column_name.length(),
                                 dropped_column_name.ptr());
                }
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

// To be compatible with Mysql 5.6 and 8.0, follwing behavior on child table are allowed on OB 4.0:
// 1. drop foreign key non-related columns and drop any foreign key in single stmt;
// 2. drop the foreign key and its' some/all related columns in single stmt.
// 3. drop foreign key in the same time when drop fk-related column under oracle mode, but report error under mysql mode,
// ensured by the ObAlterTableArg.
// Notice that, drop fk related column on parent table has been processed in phase ddl resolver.
// Here, only need to report OB_ERR_ALTER_COLUMN_FK if drop foreign key related columns without drop the fk.
int ObDDLService::check_drop_column_with_drop_foreign_key(
    const obrpc::ObAlterTableArg &alter_table_arg,
    const ObTableSchema &orig_table_schema,
    const common::ObIArray<uint64_t> &drop_cols_id_arr)
{
  int ret = OB_SUCCESS;
  FOREACH_CNT_X(dropped_col, drop_cols_id_arr, OB_SUCC(ret)) {
    // 1. to iter all foreign keys related to the dropped column.
    const ObIArray<ObForeignKeyInfo> &foreign_key_infos = orig_table_schema.get_foreign_key_infos();
    for (int64_t i = 0; OB_SUCC(ret) && i < foreign_key_infos.count(); i++) {
      const ObForeignKeyInfo &fk_info = foreign_key_infos.at(i);
      const ObString &curr_fk_name = fk_info.foreign_key_name_;
      if (fk_info.child_table_id_ == orig_table_schema.get_table_id()) {
        bool is_drop_child_col = false;
        FOREACH_CNT_X(col_id, fk_info.child_column_ids_, OB_SUCC(ret) && !is_drop_child_col) {
          if (*dropped_col == *col_id) {
            is_drop_child_col = true;
          }
        }
        if (is_drop_child_col) {
          // 2. to check whether to drop the related foreign key.
          bool is_drop_curr_fk = false;
          const ObSArray<ObIndexArg *> &index_arg_list = alter_table_arg.index_arg_list_;
          for (int64_t i = 0; OB_SUCC(ret) && !is_drop_curr_fk && i < index_arg_list.size(); ++i) {
            ObIndexArg *index_arg = const_cast<ObIndexArg *>(index_arg_list.at(i));
            if (OB_ISNULL(index_arg)) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("invalid argument, index arg should not be null", KR(ret), K(index_arg_list));
            } else if (ObIndexArg::DROP_FOREIGN_KEY != index_arg->index_action_type_) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("invalid argument", KR(ret), K(index_arg));
            } else {
              const ObDropForeignKeyArg *drop_foreign_key_arg = static_cast<ObDropForeignKeyArg *>(index_arg);
              if (0 == curr_fk_name.case_compare(drop_foreign_key_arg->foreign_key_name_)) {
                is_drop_curr_fk = true;
              }
            }
          }
          // 3. drop child column of fk, but the fk is not dropped, should report error.
          if (OB_FAIL(ret)) {
          } else if (!is_drop_curr_fk) {
            const ObString &column_name = orig_table_schema.get_column_schema(*dropped_col)->get_column_name_str();
            ret = OB_ERR_ALTER_COLUMN_FK;
            LOG_USER_ERROR(OB_ERR_ALTER_COLUMN_FK, column_name.length(), column_name.ptr());
            LOG_WARN("the column is related to foreign key, and can not be dropped", KR(ret), K(column_name), K(curr_fk_name));
          }
        }
      }
    }
  }
  return ret;
}

int ObDDLService::delete_column_from_schema_in_trans(
    const AlterTableSchema &alter_table_schema,
    ObSchemaGuardWrapper &schema_guard,
    const ObTableSchema &orig_table_schema,
    ObTableSchema &new_table_schema,
    ObDDLOperator &ddl_operator,
    common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObTableSchema::const_column_iterator it = nullptr;
  AlterColumnSchema *alter_column_schema = nullptr;
  for (it = alter_table_schema.column_begin(); OB_SUCC(ret) && it != alter_table_schema.column_end(); it++) {
    if (OB_ISNULL(alter_column_schema = static_cast<AlterColumnSchema *>(*it))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("alter_column_schema is NULL", KR(ret), K(alter_table_schema));
    } else {
      const uint64_t tenant_id = orig_table_schema.get_tenant_id();
      const ObString &orig_column_name = alter_column_schema->get_origin_column_name();
      const ObColumnSchemaV2 *orig_column_schema = orig_table_schema.get_column_schema(orig_column_name);
      if (OB_ISNULL(orig_column_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ObColumnSchemaV2 should not be null", KR(ret), K(*it));
      } else if (OB_FAIL(ddl_operator.drop_sequence_in_drop_column(
                         *orig_column_schema,
                         trans,
                         schema_guard))) {
        LOG_WARN("alter table drop identity column fail", KR(ret));
      } else if (OB_FAIL(ddl_operator.update_prev_id_for_delete_column(orig_table_schema,
                         new_table_schema, *orig_column_schema, trans))) {
        LOG_WARN("failed to update column previous id for delele column", KR(ret));
      } else if (OB_FAIL(schema_service_->gen_new_schema_version(tenant_id, new_schema_version))) {
        LOG_WARN("fail to gen new schema version", KR(ret), K(tenant_id));
      } else if (OB_FAIL(ddl_operator.delete_single_column(trans, new_schema_version, new_table_schema, orig_column_name))) {
        LOG_WARN("fail to delete column", KR(ret), K(alter_column_schema));
      } else {
        LOG_INFO("delete column from schema", K(orig_column_name));
      }
    }
  }
  return ret;
}

int ObDDLService::drop_column_update_new_table(
    const ObString &column_name,
    ObTableSchema &new_table_schema)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = new_table_schema.get_tenant_id();
  ObColumnSchemaV2 *new_origin_col = new_table_schema.get_column_schema(column_name);
  ObSchemaService *schema_service = schema_service_->get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    LOG_WARN("schema_service must not null", KR(ret));
  } else if (OB_ISNULL(new_origin_col)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get column from new table schema", KR(ret));
  } else {
    ObColumnSchemaV2 *next_col = new_table_schema.get_column_schema_by_prev_next_id(new_origin_col->get_next_column_id());
    if (OB_ISNULL(next_col)) {
      // do nothing since local_column is tail column
    } else {
      next_col->set_prev_column_id(new_origin_col->get_prev_column_id());
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(new_table_schema.delete_column(new_origin_col->get_column_name_str()))) {
      LOG_WARN("fail to delete column", KR(ret), K(new_origin_col->get_column_name_str()));
    } else if (OB_FAIL(drop_udt_hidden_columns(*new_origin_col, new_table_schema))) {
      LOG_WARN("fail to delete udt hidden column", KR(ret), K(new_origin_col->get_column_name_str()));
    }
  }
  return ret;
}

int ObDDLService::drop_column_online(
    ObSchemaGuardWrapper &schema_guard,
    const ObTableSchema &origin_table_schema,
    const ObString &origin_column_name,
    const int64_t new_tbl_visible_cols_cnt_after_alter,
    const int64_t last_drop_column_id,
    ObDDLOperator &ddl_operator,
    common::ObMySQLTransaction &trans,
    ObTableSchema &new_table_schema,
    common::hash::ObHashSet<ObColumnNameHashWrapper> &update_column_name_set)
{
  int ret = OB_SUCCESS;
  bool is_oracle_mode = false;
  ObColumnSchemaV2 *new_column_schema = nullptr;
  const ObColumnSchemaV2 *orig_column_schema = nullptr;
  ObColumnNameHashWrapper orig_column_key(origin_column_name);
  const uint64_t tenant_id = origin_table_schema.get_tenant_id();
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check inner stat failed", KR(ret));
  } else if (OB_UNLIKELY(!origin_table_schema.is_valid() || origin_column_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(origin_column_name), K(origin_table_schema));
  } else if (OB_ISNULL(orig_column_schema = origin_table_schema.get_column_schema(origin_column_name))
    || OB_ISNULL(new_column_schema = new_table_schema.get_column_schema(origin_column_name))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid null column schema", KR(ret), KP(orig_column_schema), KP(new_column_schema),
      K(origin_column_name), K(origin_table_schema), K(new_table_schema));
  } else if (OB_FAIL(check_can_drop_column(origin_column_name, orig_column_schema, origin_table_schema,
                                           new_table_schema, new_tbl_visible_cols_cnt_after_alter, last_drop_column_id, schema_guard))) {
    LOG_WARN("check drop column failed", KR(ret));
  } else if (OB_HASH_EXIST == update_column_name_set.exist_refactored(orig_column_key)) {
    ret = OB_ERR_BAD_FIELD_ERROR;
    LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR, origin_column_name.length(), origin_column_name.ptr(),
                    origin_table_schema.get_table_name_str().length(),
                    origin_table_schema.get_table_name_str().ptr());
    LOG_WARN("column has beed modified, can't drop", KR(ret));
  } else if (OB_FAIL(ddl_operator.drop_sequence_in_drop_column(*orig_column_schema, trans,
                                                                schema_guard))) {
    RS_LOG(WARN, "alter table drop identity column fail", KR(ret));
  } else if (OB_FAIL(ddl_operator.update_prev_id_for_delete_column(
                origin_table_schema, new_table_schema, *orig_column_schema, trans))) {
    LOG_WARN("failed to update column previous id for delete column", KR(ret));
  } else if (OB_FAIL(origin_table_schema.check_if_oracle_compat_mode(is_oracle_mode))) {
    LOG_WARN("fail to check is oracle mode", KR(ret), K(origin_table_schema));
  } else {
    char new_col_name[OB_MAX_COLUMN_NAME_BUF_LENGTH] = {0};
    ObObj default_value;
    if (new_column_schema->is_virtual_generated_column()
        || new_column_schema->is_stored_generated_column()
        || new_column_schema->is_default_expr_v2_column()) {
      // generated column expression, generated alway as (null), generated column retrieve needed.
      default_value.set_varchar("NULL");
      default_value.set_collation_type(ObCharset::get_system_collation());
    } else {
      default_value.set_null();
    }
    uint64_t col_id = orig_column_schema->get_column_id();
    struct timeval t;
    gettimeofday(&t, nullptr);
    struct tm tm;
    ::localtime_r(&t.tv_sec, &tm);
    new_column_schema->set_unused();
    new_column_schema->set_prev_column_id(new_column_schema->get_column_id());
    new_column_schema->set_next_column_id(new_column_schema->get_column_id());
    new_column_schema->erase_identity_column_flags(); // to avoid drop sequences again.
    new_column_schema->drop_not_null_cst(); // change column to null.
    new_column_schema->set_nullable(true);
    if (OB_FAIL(databuff_printf(new_col_name, OB_MAX_COLUMN_NAME_BUF_LENGTH,
      "SYS_C%05lu_%02d%02d%02d%02d:%02d:%02d$", orig_column_schema->get_column_id(),
      (tm.tm_year + 1900) % 100, (tm.tm_mon + 1), tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec))) {
      LOG_WARN("databuff printf failed", KR(ret));
    } else if (OB_UNLIKELY(nullptr != new_table_schema.get_column_schema(new_col_name))) {
      ret = OB_ERR_COLUMN_DUPLICATE;
      LOG_WARN("duplicate column name", KR(ret), K(new_col_name));
    } else if (OB_FAIL(new_column_schema->set_column_name(new_col_name))) {
      LOG_WARN("set column name failed", KR(ret));
    } else if (OB_FAIL(new_column_schema->set_cur_default_value(default_value, new_column_schema->is_default_expr_v2_column()))) {
      LOG_WARN("failed to set current default value", KR(ret));
    } else if (OB_FAIL(ddl_operator.update_column_and_column_group(trans, origin_table_schema, new_table_schema,
                                                                   *new_column_schema, true/*need_del_stats*/))) {
      LOG_WARN("failed to update column and column group", KR(ret), KPC(new_column_schema));
    } else if (OB_FAIL(alter_table_update_index_and_view_column(new_table_schema, *new_column_schema, schema_guard, ddl_operator, trans))) {
      LOG_WARN("update column in aux table failed", KR(ret));
    }
  }
  return ret;
}

int ObDDLService::drop_main_table_column_online(
    ObSchemaGuardWrapper &schema_guard,
    const ObTableSchema &origin_table_schema,
    const ObString &origin_column_name,
    const bool is_oracle_mode,
    const int64_t new_tbl_visible_cols_cnt_after_alter,
    const int64_t last_drop_column_id,
    ObDDLOperator &ddl_operator,
    common::ObMySQLTransaction &trans,
    ObTableSchema &new_table_schema,
    common::hash::ObHashSet<ObColumnNameHashWrapper> &update_column_name_set)
{
  int ret = OB_SUCCESS;
  const ObColumnSchemaV2 *orig_column_schema = nullptr;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check inner stat failed", KR(ret));
  } else if (OB_ISNULL(orig_column_schema = origin_table_schema.get_column_schema(origin_column_name))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid null column schema", KR(ret), K(origin_column_name));
  } else if (is_oracle_mode && orig_column_schema->is_xmltype()) {
    // Oracle XMLType: drop hidden columns in same column group first.
    ObSEArray<ObColumnSchemaV2 *, 1> hidden_cols;
    if (OB_FAIL(new_table_schema.get_column_schema_in_same_col_group(
        orig_column_schema->get_column_id(), orig_column_schema->get_udt_set_id(), hidden_cols))) {
      LOG_WARN("failed to get column schema in same col group", KR(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < hidden_cols.count(); i++) {
        if (OB_FAIL(drop_column_online(schema_guard, origin_table_schema,
                                       hidden_cols.at(i)->get_column_name_str(),
                                       new_tbl_visible_cols_cnt_after_alter,
                                       last_drop_column_id,
                                       ddl_operator, trans, new_table_schema,
                                       update_column_name_set))) {
          LOG_WARN("online drop udt hidden column failed", KR(ret));
        }
      }
    }
  }
  if (FAILEDx(drop_column_online(schema_guard, origin_table_schema, origin_column_name,
                                 new_tbl_visible_cols_cnt_after_alter, last_drop_column_id,
                                 ddl_operator, trans, new_table_schema,
                                 update_column_name_set))) {
    LOG_WARN("online drop column failed", KR(ret), K(origin_column_name));
  }
  return ret;
}

int ObDDLService::drop_column_aux_objects_online(
    ObSchemaGuardWrapper &schema_guard,
    const obrpc::ObAlterTableArg &alter_table_arg,
    const ObTableSchema &orig_table_schema,
    const common::ObIArray<uint64_t> &drop_cols_id_arr,
    common::ObIAllocator &allocator,
    ObTableSchema &new_table_schema,
    ObDDLOperator &ddl_operator,
    common::ObMySQLTransaction &trans,
    common::ObIArray<ObDDLTaskRecord> &ddl_task_records,
    obrpc::ObAlterTableRes &res)
{
  int ret = OB_SUCCESS;
  ObSensitiveRuleDDLService sensitive_rule_ddl_service(this);
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check inner stat failed", KR(ret));
  } else if (OB_FAIL(drop_rls_policy_caused_by_drop_column_online(schema_guard, orig_table_schema,
                     drop_cols_id_arr, new_table_schema, ddl_operator, trans))) {
    LOG_WARN("drop rls policy failed", KR(ret));
  } else if (OB_FAIL(drop_lob_caused_by_drop_column_online_if_need(alter_table_arg, orig_table_schema,
                     new_table_schema, allocator, trans, ddl_task_records, res))) {
    LOG_WARN("drop lob caused by drop column online failed", KR(ret));
  } else if (OB_FAIL(drop_index_caused_by_drop_column_online(schema_guard, orig_table_schema,
                     drop_cols_id_arr, allocator, ddl_operator, trans, ddl_task_records))) {
    LOG_WARN("drop index caused by drop column failed", KR(ret), K(drop_cols_id_arr));
  } else if (OB_FAIL(sensitive_rule_ddl_service.drop_sensitive_column_caused_by_drop_column_online(
                     schema_guard, orig_table_schema, drop_cols_id_arr, trans))) {
    LOG_WARN("drop sensitive column caused by drop column failed", KR(ret));
  }
  return ret;
}

int ObDDLService::check_can_add_column_use_instant_(const bool is_oracle_mode,
                                                    const uint64_t tenant_data_version,
                                                    bool &can_add_column_instant)
{
  return share::ObAlterColumnDDLHelper::check_can_add_column_use_instant(is_oracle_mode, tenant_data_version, can_add_column_instant);
}

int ObDDLService::check_is_add_column_online_(const AlterTableSchema &alter_table_schema,
                                              const ObTableSchema &table_schema,
                                              const AlterColumnSchema &alter_column_schema,
                                              const obrpc::ObAlterTableArg::AlterAlgorithm &algorithm,
                                              const bool is_oracle_mode,
                                              const uint64_t tenant_data_version,
                                              ObDDLType &tmp_ddl_type)
{
  return share::ObAlterColumnDDLHelper::compute_add_column_ddl_type(alter_table_schema,
                                                table_schema,
                                                alter_column_schema,
                                                static_cast<int64_t>(algorithm),
                                                is_oracle_mode,
                                                tenant_data_version,
                                                tmp_ddl_type);
}

int ObDDLService::check_can_add_column_instant_(const ObTableSchema &orig_table_schema,
                                                const AlterTableSchema &alter_table_schema,
                                                const obrpc::ObAlterTableArg::AlterAlgorithm &algorithm,
                                                const uint64_t tenant_data_version,
                                                const bool is_oracle_mode,
                                                ObSchemaGetterGuard &schema_guard,
                                                ObDDLType &ddl_type)
{
  int ret = OB_SUCCESS;
  bool add_column_instant = false;
  ddl_type = ObDDLType::DDL_INVALID;
  bool has_other_ddl_except_add = false;
  AlterColumnSchema *alter_column_schema = NULL;
  ObTableSchema::const_column_iterator it_begin = alter_table_schema.column_begin();
  ObTableSchema::const_column_iterator it_end = alter_table_schema.column_end();
  // 1.add column instant only under mysql
  // 2.add column instant except modify column, with other ddl should be offline
  for (; OB_SUCC(ret) && !is_oracle_mode && !(add_column_instant && has_other_ddl_except_add) && it_begin != it_end; it_begin++) {
    if (OB_ISNULL(alter_column_schema = static_cast<AlterColumnSchema *>(*it_begin))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("alter_column_schema is NULL", KR(ret), K(alter_table_schema));
    } else {
      const ObSchemaOperationType op_type = alter_column_schema->alter_type_;
      switch (op_type) {
        case OB_DDL_ADD_COLUMN: {
          ObDDLType tmp_ddl_type = ObDDLType::DDL_INVALID;
          if (!add_column_instant && OB_FAIL(check_is_add_column_online_(alter_table_schema, orig_table_schema, *alter_column_schema, algorithm,
                                                                         is_oracle_mode, tenant_data_version, tmp_ddl_type))) {
            LOG_WARN("fail to check is add column online", KR(ret));
          } else if (ObDDLType::DDL_ADD_COLUMN_INSTANT == tmp_ddl_type) {
            add_column_instant = true;
          }
          break;
        }
        case OB_DDL_MODIFY_COLUMN: {
          // add column online compound with modify column online should be online
          break;
        }
        case OB_DDL_DROP_COLUMN:
        case OB_DDL_CHANGE_COLUMN:
        case OB_DDL_ALTER_COLUMN: {
          has_other_ddl_except_add = true;
          break;
        }
        default: {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("unhandled operator type!", KR(ret), K(op_type));
          break;
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (add_column_instant && has_other_ddl_except_add) {
      ddl_type = ObDDLType::DDL_TABLE_REDEFINITION;
      LOG_INFO("add column instant with other ddl except modify column, set ddl type to DDL_TABLE_REDEFINITION", K(ddl_type));
    }
  }
  return ret;
}

// update relevant inner table if all of schema_guard, ddl_operator and trans are not null
int ObDDLService::add_new_column_to_table_schema(
    const ObTableSchema &origin_table_schema,
    const AlterTableSchema &alter_table_schema,
    const common::ObTimeZoneInfoWrap &tz_info_wrap,
    const common::ObString &nls_formats,
    sql::ObLocalSessionVar &local_session_var,
    obrpc::ObSequenceDDLArg &sequence_ddl_arg,
    common::ObIAllocator &allocator,
    ObTableSchema &new_table_schema,
    AlterColumnSchema &alter_column_schema,
    ObIArray<ObString> &gen_col_expr_arr,
    ObSchemaGetterGuard &schema_guard,
    uint64_t &curr_udt_set_id,
    ObDDLOperator *ddl_operator,
    common::ObMySQLTransaction *trans,
    ObSchemaGetterGuard *checker_schema_guard,
    ObSchemaGuardWrapper *ext_schema_guard_wrapper,
    const bool record_wasted_version)
{
  int ret = OB_SUCCESS;
  const ObSQLMode sql_mode = alter_table_schema.get_sql_mode();
  const bool update_inner_table = nullptr != ddl_operator && nullptr != trans;
  ObSchemaGetterGuard &actual_checker = (nullptr != checker_schema_guard)
      ? *checker_schema_guard : schema_guard;
  ObSchemaGuardWrapper local_schema_guard_wrapper(origin_table_schema.get_tenant_id(), schema_service_);
  ObSchemaGuardWrapper *schema_guard_wrapper_ptr = ext_schema_guard_wrapper;
  bool is_oracle_mode = false;
  bool is_contain_part_key = false;
  ObCharsetCompatType charset_compat_type = CHARSET_COMPAT_MYSQL57;
  if (nullptr == schema_guard_wrapper_ptr) {
    if (OB_FAIL(local_schema_guard_wrapper.init(schema_guard))) {
      LOG_WARN("fail to init schema guard wrapper", KR(ret));
    } else {
      schema_guard_wrapper_ptr = &local_schema_guard_wrapper;
    }
  }
  LOG_DEBUG("check before alter table column", K(origin_table_schema), K(alter_table_schema), K(new_table_schema));
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(origin_table_schema.check_if_oracle_compat_mode(is_oracle_mode))) {
    LOG_WARN("failed to get oracle mode", K(ret));
  } else if (OB_ISNULL(tz_info_wrap.get_time_zone_info())
             || OB_ISNULL(tz_info_wrap.get_time_zone_info()->get_tz_info_map())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tz_info_wrap", K(tz_info_wrap), K(ret));
  }
  // fill column collation
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_charset_compat_type(new_table_schema.get_tenant_id(), schema_guard, charset_compat_type))) {
    LOG_WARN("fail to get compat type", K(ret));
  } else if (OB_FAIL(fill_column_collation(sql_mode,
                                           is_oracle_mode,
                                           new_table_schema,
                                           allocator,
                                           alter_column_schema,
                                           charset_compat_type))) {
    LOG_WARN("failed to fill column collation", K(ret));
  } else {
    int64_t max_used_column_id = new_table_schema.get_max_used_column_id();
    const uint64_t tenant_id = new_table_schema.get_tenant_id();
    if (OB_FAIL(ret)) {
    } else if (new_table_schema.is_mlog_table()) {
      const ObTableSchema *base_table_schema = nullptr;
      const ObColumnSchemaV2 *base_column_schema = nullptr;
      const ObColumnSchemaV2 *old_mlog_column_schema = nullptr;
      const uint64_t base_table_id = new_table_schema.get_data_table_id();
      if (OB_FAIL(schema_guard.get_table_schema(tenant_id, base_table_id, base_table_schema))) {
        LOG_WARN("failed to get base table schema", KR(ret), K(tenant_id), K(base_table_id));
      } else if (OB_ISNULL(base_table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("base table schema is null", K(ret), K(tenant_id), K(base_table_id));
      } else if (OB_ISNULL(base_column_schema = base_table_schema->get_column_schema(alter_column_schema.get_column_name_str()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("base column schema is null", K(ret), K(alter_column_schema));
      } else if (NULL != (old_mlog_column_schema = new_table_schema.get_column_schema(base_column_schema->get_column_id()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("old mlog column schema already existed",
                  K(ret), K(alter_column_schema), KPC(old_mlog_column_schema));
      } else {
        alter_column_schema.set_column_id(base_column_schema->get_column_id());
      }
    }
    if (OB_FAIL(ret)) {
    } else if (is_inner_table(new_table_schema.get_table_id())
        && (OB_INVALID_ID == alter_column_schema.get_column_id()
        || alter_column_schema.get_column_id() != max_used_column_id + 1)) {
      // 225 is barrier version, after this adding column in system table need specify column_id
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("inner table should add column at last and specify column_id",
               K(ret), K(alter_column_schema), K(max_used_column_id));
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "inner table add column without column_id");
    } else {
      if (alter_column_schema.is_udt_hidden_column()) {
        // udt hidden column
        char col_name[OB_MAX_COLUMN_NAME_LENGTH] = {0};
        alter_column_schema.set_udt_set_id(curr_udt_set_id);
        if (OB_FAIL(databuff_printf(col_name, OB_MAX_COLUMN_NAME_LENGTH, "SYS_NC%05lu$", max_used_column_id + 1))) {
          SQL_RESV_LOG(WARN, "failed to print column name", K(ret), K(max_used_column_id));
        } else if (OB_FAIL(alter_column_schema.set_column_name(col_name))) {
          SQL_RESV_LOG(WARN, "failed to set column name", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_INVALID_ID == alter_column_schema.get_column_id()) {
          alter_column_schema.set_column_id(++max_used_column_id);
        } else {
          max_used_column_id = MAX(max_used_column_id, alter_column_schema.get_column_id());
        }
        alter_column_schema.set_rowkey_position(0);
        alter_column_schema.set_index_position(0);
        alter_column_schema.set_not_part_key();
        alter_column_schema.set_table_id(new_table_schema.get_table_id());
        alter_column_schema.set_tenant_id(new_table_schema.get_tenant_id());
        if (new_table_schema.is_primary_vp_table()) {
          // The last column add in the primary VP
          alter_column_schema.add_column_flag(PRIMARY_VP_COLUMN_FLAG);
        }
        if (alter_column_schema.is_xmltype()) {
          alter_column_schema.set_udt_set_id(alter_column_schema.get_column_id());
          curr_udt_set_id = alter_column_schema.get_udt_set_id();
        }
        new_table_schema.set_max_used_column_id(max_used_column_id);
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (new_table_schema.is_mlog_table()) {
    // columns with constraints/sequences in aux tables need not create related schema objects
  } else if (OB_FAIL(refill_columns_id_for_check_constraint(origin_table_schema,
                                                            alter_table_schema,
                                                            alter_column_schema,
                                                            is_oracle_mode,
                                                            allocator))) {
    LOG_WARN("fail to refill columns id for check constraint", K(ret));
  } else if (is_oracle_mode
    && OB_FAIL(refill_columns_id_for_not_null_constraint(alter_table_schema,
                                                         alter_column_schema))) {
    LOG_WARN("fail to refill column id to constraints", K(ret));
  } else if (update_inner_table) {
    if (OB_FAIL(ddl_operator->create_sequence_in_add_column(new_table_schema,
            alter_column_schema, *trans, *schema_guard_wrapper_ptr, sequence_ddl_arg))) {
      LOG_WARN("alter table add identity column fail", K(alter_column_schema), K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    ObSchemaChecker schema_checker;
    if (OB_FAIL(schema_checker.init(actual_checker))) {
      LOG_WARN("failed to init schema guard", K(ret));
    } else if (alter_column_schema.is_udt_related_column(is_oracle_mode)) {
      // udt column/oracle gis not need to do the flowing else ifs:
      // 1. default values is check and calculated in resolver, only check dependency version on RS
      // 2. udt column and it's hidden columns cannot be primary key
      LOG_INFO("alter table add udt related column", K(alter_column_schema));
    } else if (OB_FAIL(ObDDLResolver::check_default_value(
                alter_column_schema.get_cur_default_value(),
                tz_info_wrap, &nls_formats, &local_session_var, allocator,
                new_table_schema,
                alter_column_schema,
                gen_col_expr_arr,
                alter_table_schema.get_sql_mode(),
                false, /* allow_sequence */
                &schema_checker))) {
      LOG_WARN("fail to check default value", K(alter_column_schema), K(ret));
    } else if (OB_FAIL(resolve_orig_default_value(alter_column_schema,
                                                  tz_info_wrap,
                                                  &nls_formats,
                                                  allocator))) {
      LOG_WARN("fail to resolve default value", K(ret));
    } else if (alter_column_schema.is_primary_key_) {
      if (new_table_schema.get_rowkey_column_num() > 0) {
        if (new_table_schema.is_table_without_pk()) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("not support to add primary key!", K(ret));
        } else {
          ret = OB_ERR_MULTIPLE_PRI_KEY;
          LOG_WARN("multiple primary key defined", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    const ObColumnSchemaV2 *mem_col = NULL;
    if (OB_FAIL(resolve_timestamp_column(&alter_column_schema,
                                         new_table_schema,
                                         alter_column_schema,
                                         tz_info_wrap,
                                         &nls_formats,
                                         allocator))) {
      LOG_WARN("fail to resolve timestamp column", K(ret));
    } else if (OB_FAIL(deal_default_value_padding(alter_column_schema, allocator))) {
      LOG_WARN("fail to deal default value padding", K(alter_column_schema), K(ret));
    } else if (OB_FAIL(new_table_schema.check_primary_key_cover_partition_column(*schema_guard_wrapper_ptr))) {
      LOG_WARN("fail to check primary key cover partition column", K(ret));
    } else if (OB_FAIL(update_prev_id_and_add_column_(origin_table_schema,
               new_table_schema, alter_column_schema, ddl_operator, trans,
               record_wasted_version))) {
      LOG_WARN("failed to update prev id", KR(ret));
    } else if (OB_ISNULL(mem_col = new_table_schema.get_column_schema(
                                    alter_column_schema.get_column_id()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mem_col is NULL", K(ret));
    } else {
      alter_column_schema.set_prev_column_id(mem_col->get_prev_column_id());
      if (update_inner_table) {
        if (OB_FAIL(ddl_operator->insert_single_column(*trans,
                new_table_schema, alter_column_schema))) {
          LOG_WARN("failed to add column", K(ret), K(alter_column_schema));
        }
      }
    }
  }
  return ret;
}

int ObDDLService::add_column_to_column_group(
    const share::schema::ObTableSchema &origin_table_schema,
    const share::schema::AlterTableSchema &alter_table_schema,
    share::schema::ObTableSchema &new_table_schema,
    ObDDLOperator &ddl_operator,
    common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  uint64_t cur_column_group_id = origin_table_schema.get_next_single_column_group_id();
  ObArray<uint64_t> column_ids;
  ObTableSchema::const_column_iterator it_begin = alter_table_schema.column_begin();
  ObTableSchema::const_column_iterator it_end = alter_table_schema.column_end();
  AlterColumnSchema *alter_column_schema = nullptr;

  bool build_old_version_cg = false;
  if (!origin_table_schema.is_valid() ) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(origin_table_schema), K(alter_table_schema));
  } else if (!new_table_schema.is_column_store_supported()) {
    /* skip*/
  } else {
    if (OB_FAIL(ObSchemaUtils::check_build_old_version_column_group(origin_table_schema, build_old_version_cg))) {
      LOG_WARN("fail to get next single column group id", K(ret), K(origin_table_schema));
    } else if (build_old_version_cg) {
      cur_column_group_id = origin_table_schema.get_max_used_column_group_id() + 1;
    }
    for(; OB_SUCC(ret) && it_begin != it_end; it_begin++) {
      if (OB_ISNULL(alter_column_schema = static_cast<AlterColumnSchema *>(*it_begin))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("*it_begin is NULL", K(ret));
      } else if (alter_column_schema->alter_type_ == OB_DDL_ADD_COLUMN) {
        const ObColumnSchemaV2 *column_schema = nullptr;
        if (OB_ISNULL(column_schema = new_table_schema.get_column_schema(alter_column_schema->get_column_name_str()))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null column schema", K(ret), KPC(alter_column_schema), K(new_table_schema));
        } else if (column_schema->is_virtual_generated_column()) {
          // skip virtual column
        } else if (OB_FAIL(column_ids.push_back(column_schema->get_column_id()))) {
          LOG_WARN("fali to push back column id", K(ret));
        }
      }
    }

    if (OB_FAIL(ret)) {
      /* skip do nothing*/
    } else if (column_ids.count() == 0){
      /* do not add column, skip */
    } else {
      bool is_all_cg_exist = false;
      bool is_each_cg_exist = false;
      if (OB_FAIL(new_table_schema.is_column_group_exist(OB_ALL_COLUMN_GROUP_NAME, is_all_cg_exist))) {
        LOG_WARN("fail to check whether all cg exist", K(ret), K(new_table_schema));
      } else if (OB_FAIL(new_table_schema.is_column_group_exist(OB_EACH_COLUMN_GROUP_NAME, is_each_cg_exist))) {
        LOG_WARN("fail to check whether each cg exist", K(ret), K(new_table_schema));
      }

      /* update info about each column group*/
      if (OB_FAIL(ret)) {
      } else if (is_each_cg_exist) {
        HEAP_VAR(ObTableSchema, tmp_table) {
        if (OB_FAIL(tmp_table.assign(new_table_schema))) {
          LOG_WARN("fail to assign", K(ret), K(new_table_schema), K(tmp_table));
        }
        tmp_table.reset_column_group_info();
        for (int64_t i = 0; OB_SUCC(ret) && i < column_ids.count(); i++) {
          ObColumnGroupSchema cg_schema;
          if (OB_FAIL(ObSchemaUtils::build_single_column_group(new_table_schema,
                                                   new_table_schema.get_column_schema(column_ids.at(i)),
                                                   new_table_schema.get_tenant_id(),
                                                   cur_column_group_id++,
                                                   cg_schema))) {
            LOG_WARN("fail to build single column group", K(ret), K(new_table_schema), K(column_ids.at(i)));
          } else if (OB_FAIL(new_table_schema.add_column_group(cg_schema))) {
            LOG_WARN("fail to add new column group schema to table", K(ret), K(cg_schema));
          } else if (OB_FAIL(tmp_table.add_column_group(cg_schema))) {
            LOG_WARN("fail to add new column group schema to tmp_cg", K(ret), K(tmp_table), K(cg_schema));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (tmp_table.get_column_group_count() == 0){
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column_group array should not be empty", K(ret), K(tmp_table));
        } else if (OB_FAIL(ddl_operator.insert_column_groups(trans, tmp_table))) {
          LOG_WARN("fail to insert new table_schema to each column gorup", K(ret), K(tmp_table));
        }
        }
      }
      /* update info about all column group*/
      if (OB_FAIL(ret)) {
      } else if (is_all_cg_exist) {
        ObColumnGroupSchema* all_cg = nullptr;
        if (OB_FAIL(new_table_schema.get_column_group_by_name(OB_ALL_COLUMN_GROUP_NAME, all_cg))) {
          LOG_WARN("fail to get all column group", K(ret), K(new_table_schema));
        } else if (OB_ISNULL(all_cg)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column group should not be null", K(ret));
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < column_ids.count(); i++) {
          if (OB_FAIL(all_cg->add_column_id(column_ids.at(i)))) {
            LOG_WARN("fail to add column id", K(ret), K(new_table_schema), K(column_ids.at(i)));
          }
        }
        if (OB_FAIL(ret)){
        } else if (column_ids.count() == 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column_ids should not be empty", K(ret), K(column_ids));
        } else if (OB_FAIL(ddl_operator.insert_column_ids_into_column_group(trans, new_table_schema, column_ids, *all_cg))) {
          LOG_WARN("fail to insert column ids into inner table", K(ret), K(new_table_schema),K(column_ids));
        }
      }

      /* update info about default column group*/
      if (OB_FAIL(ret)) {
      } else if (!is_all_cg_exist && !is_each_cg_exist) {
        ObColumnGroupSchema *default_cg = nullptr;
        if (OB_FAIL(new_table_schema.get_column_group_by_name(OB_DEFAULT_COLUMN_GROUP_NAME, default_cg))) {
          LOG_WARN("fail get default column group", K(ret), K(new_table_schema));
        } else if (OB_ISNULL(default_cg)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column group should not be null", K(ret), K(new_table_schema));
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < column_ids.count(); i++) {
          if (OB_FAIL(default_cg->add_column_id(column_ids.at(i)))) {
            LOG_WARN("fail to add column id", K(ret), K(new_table_schema), K(column_ids.at(i)));
          }
        }

        if (OB_FAIL(ret)){
        } else if (column_ids.count() == 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column_ids should not be empty", K(ret), K(column_ids));
        } else if (OB_FAIL(ddl_operator.insert_column_ids_into_column_group(trans, new_table_schema, column_ids, *default_cg))) {
          LOG_WARN("fail to insert column ids into inner table", K(ret), K(new_table_schema));
        }
      }
    }
  }
  return ret;
}

int ObDDLService::update_prev_id_for_add_column(const ObTableSchema &origin_table_schema,
    ObTableSchema &new_table_schema,
    AlterColumnSchema &alter_column_schema,
    ObDDLOperator *ddl_operator,
    common::ObMySQLTransaction *trans,
    const bool record_wasted_version)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = origin_table_schema.get_tenant_id();
  const bool is_first = alter_column_schema.is_first_;
  const bool is_after = (!alter_column_schema.get_prev_column_name().empty());
  const bool is_before = (!alter_column_schema.get_next_column_name().empty());
  const bool is_last = !(is_first || is_after || is_before);
  const bool update_inner_table = nullptr != ddl_operator && nullptr != trans;
  const bool need_del_stats = false;
  if (is_last) {
    if (update_inner_table && record_wasted_version) {
      auto *tsi_generator = GET_TSI(share::schema::TSISchemaVersionGenerator);
      if (OB_NOT_NULL(tsi_generator)) {
        tsi_generator->inc_wasted_cnt();
      }
    }
  } else {
    ObString pos_column_name;
    const uint64_t alter_column_id = alter_column_schema.get_column_id();
    if (new_table_schema.is_mlog_table()) {
      ObColumnIterByPrevNextID iter(new_table_schema);
      const ObColumnSchemaV2 *column = NULL;
      const ObColumnSchemaV2 *last_column = NULL;
      while (OB_SUCC(ret) && OB_SUCC(iter.next(column))) {
        if (OB_ISNULL(column)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("The column is null", K(ret));
        } else {
          last_column = column;
        }
      }
      if (OB_UNLIKELY(ret != OB_ITER_END)) {
        LOG_WARN("Failed to iterate all table columns. iter quit. ", K(ret));
      } else if (OB_ISNULL(last_column)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Failed to get last column", K(ret));
      } else {
        ret = OB_SUCCESS;
        alter_column_schema.set_prev_column_id(last_column->get_column_id());
      }
    } else if (is_first) {
      // this first means the first of no hidden/shdow column.
      ObColumnIterByPrevNextID iter(new_table_schema);
      const ObColumnSchemaV2 *head_col = NULL;
      const ObColumnSchemaV2 *col = NULL;
      bool is_first = false;
      while (OB_SUCC(ret) && OB_SUCC(iter.next(col))) {
        if (OB_ISNULL(col)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("The column is null", K(ret));
        } else if (col->is_shadow_column() || col->is_hidden()) {
          // do nothing
        } else if (!is_first) {
          head_col = col;
          is_first = true;
        }
      }
      if (ret != OB_ITER_END) {
        LOG_WARN("Failed to iterate all table columns. iter quit. ", K(ret));
      } else {
        ret = OB_SUCCESS;
        if (OB_ISNULL(head_col)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Failed to get first column", K(ret));
        } else {
          alter_column_schema.set_next_column_name(head_col->get_column_name());
        }
      }
    }
    if (OB_SUCC(ret)) {
      pos_column_name = (is_after ? alter_column_schema.get_prev_column_name()
          : alter_column_schema.get_next_column_name());
      ObColumnSchemaV2 *pos_column_schema = new_table_schema.get_column_schema(pos_column_name);
      ObColumnSchemaV2 *update_column_schema = NULL;
      if (OB_ISNULL(pos_column_schema)) {
        ret = OB_ERR_BAD_FIELD_ERROR;
        LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR, pos_column_name.length(), pos_column_name.ptr(),
                      new_table_schema.get_table_name_str().length(),
                      new_table_schema.get_table_name_str().ptr());
        LOG_WARN("pos column is NULL", K(pos_column_name));
      } else {
        if (is_after) {
          // add column after
          alter_column_schema.set_prev_column_id(pos_column_schema->get_column_id());
          update_column_schema = new_table_schema.get_column_schema_by_prev_next_id(pos_column_schema->get_next_column_id());
          if (OB_NOT_NULL(update_column_schema)) {
            update_column_schema->set_prev_column_id(alter_column_id);
          }
        } else {
          // add column before / first
          alter_column_schema.set_prev_column_id(pos_column_schema->get_prev_column_id());
          update_column_schema = pos_column_schema;
          update_column_schema->set_prev_column_id(alter_column_id);
        }
        if (OB_SUCC(ret)) {
          if (OB_ISNULL(update_column_schema)) {
            // alter column is the last column
            if (update_inner_table && record_wasted_version) {
              auto *tsi_generator = GET_TSI(share::schema::TSISchemaVersionGenerator);
              if (OB_NOT_NULL(tsi_generator)) {
                tsi_generator->inc_wasted_cnt();
              }
            }
          } else if (update_inner_table) {
            if (OB_FAIL(ddl_operator->update_single_column(
                        *trans,
                        origin_table_schema,
                        new_table_schema,
                        *update_column_schema,
                        need_del_stats))) {
              LOG_WARN("Failed to update single column", K(ret), K(update_column_schema->get_column_name_str()));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObDDLService::update_prev_id_and_add_column_(
    const ObTableSchema &origin_table_schema,
    ObTableSchema &new_table_schema,
    AlterColumnSchema &alter_column_schema,
    ObDDLOperator *ddl_operator,
    common::ObMySQLTransaction *trans,
    const bool record_wasted_version)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(update_prev_id_for_add_column(origin_table_schema, new_table_schema,
              alter_column_schema, ddl_operator, trans, record_wasted_version))) {
    LOG_WARN("fail to update prev id", KR(ret));
  } else if (OB_FAIL(new_table_schema.add_column(alter_column_schema))) {
    if (OB_ERR_COLUMN_DUPLICATE == ret) {
      const ObString &column_name = alter_column_schema.get_column_name_str();
      LOG_USER_ERROR(OB_ERR_COLUMN_DUPLICATE, column_name.length(), column_name.ptr());
      LOG_WARN("duplicate column name", K(column_name), KR(ret));
    }
    LOG_WARN("failed to add new column", KR(ret));
  }
  return ret;
}

} // end namespace rootserver
} // end namespace oceanbase
