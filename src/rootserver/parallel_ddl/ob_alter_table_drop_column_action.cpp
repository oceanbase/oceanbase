/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX RS
#include "rootserver/parallel_ddl/ob_alter_table_drop_column_action.h"
#include "rootserver/ob_ddl_service.h"
#include "rootserver/ob_ddl_operator.h"
#include "share/ob_rpc_struct.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_sensitive_rule_schema_struct.h"
#include "storage/ddl/ob_ddl_lock.h"
#include "sql/resolver/mv/ob_mv_dep_utils.h"

using namespace oceanbase::rootserver;
using namespace oceanbase::obrpc;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace oceanbase::transaction::tablelock;

int ObAlterTableDropColumnAction::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(update_column_name_set_.create(OB_MAX_COLUMN_NUMBER))) {
    LOG_WARN("fail to create hash set", KR(ret));
  }
  return ret;
}

int ObAlterTableDropColumnAction::register_search_def_child_locks_(
    const ObTableSchema &index_schema)
{
  int ret = OB_SUCCESS;
  // Only one level of child index under a search-def parent is enumerated here;
  // a deeper subgraph (if ever exposed) needs separate evaluation.
  const ObIArray<ObAuxTableMetaInfo> &child_infos = index_schema.get_simple_index_infos();
  for (int64_t i = 0; OB_SUCC(ret) && i < child_infos.count(); ++i) {
    const uint64_t child_id = child_infos.at(i).table_id_;
    if (OB_FAIL(add_ddl_object_lock_by_id(child_id, TABLE_SCHEMA, EXCLUSIVE))) {
      LOG_WARN("fail to register search def child object lock", KR(ret), K(child_id));
    }
  }
  return ret;
}

int ObAlterTableDropColumnAction::register_identity_sequence_locks_(
    const ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  if (table_schema.is_user_table()
      || table_schema.is_oracle_tmp_table()
      || table_schema.is_oracle_tmp_table_v2()) {
    // Only lock identity sequences for columns being dropped, not all identity
    // columns on the table. drop_cols_id_arr_ is not yet populated at this point,
    // so resolve dropped column names from the alter arg.
    const AlterTableSchema &alter_table_schema = get_arg().alter_table_schema_;
    const AlterColumnSchema *alter_col = nullptr;
    ObTableSchema::const_column_iterator it = alter_table_schema.column_begin();
    ObTableSchema::const_column_iterator it_end = alter_table_schema.column_end();
    for (; OB_SUCC(ret) && it != it_end; ++it) {
      if (OB_ISNULL(*it)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column schema is null", KR(ret));
      } else if (OB_ISNULL(alter_col = static_cast<const AlterColumnSchema *>(*it))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("alter column is null", KR(ret));
      } else if (OB_DDL_DROP_COLUMN != alter_col->alter_type_) {
        ret = OB_NOT_SUPPORTED_FOR_PARALLEL_DDL;
        LOG_WARN("unexpected column alter type for drop column parallel DDL, fall back to serial",
                 KR(ret), "alter_type", alter_col->alter_type_);
      } else {
        const ObString &col_name = alter_col->get_origin_column_name();
        const ObColumnSchemaV2 *orig_col = table_schema.get_column_schema(col_name);
        if (OB_ISNULL(orig_col)) {
          // Column not found is fine here; RS will report the proper error later.
          continue;
        } else if (!orig_col->is_identity_column()) {
          continue;
        }
        const uint64_t sequence_id = orig_col->get_sequence_id();
        if (OB_FAIL(add_ddl_object_lock_by_id(sequence_id, SEQUENCE_SCHEMA, EXCLUSIVE))) {
          LOG_WARN("fail to register sequence object lock", KR(ret), K(sequence_id));
        } else {
          ObArray<const ObSAuditSchema *> audits;
          if (OB_FAIL(get_schema_guard_wrapper().get_audit_schema_in_owner(
                      AUDIT_SEQUENCE, sequence_id, audits))) {
            LOG_WARN("fail to get audit schemas for sequence", KR(ret), K(sequence_id));
          }
          for (int64_t i = 0; OB_SUCC(ret) && i < audits.count(); ++i) {
            if (OB_ISNULL(audits.at(i))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("audit schema ptr is null", KR(ret), K(i), K(sequence_id));
            } else {
              const uint64_t audit_id = audits.at(i)->get_audit_key().audit_id_;
              if (OB_FAIL(add_ddl_object_lock_by_id(audit_id, AUDIT_SCHEMA, EXCLUSIVE))) {
                LOG_WARN("fail to register audit object lock", KR(ret), K(audit_id));
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObAlterTableDropColumnAction::register_table_audit_locks_(
    const ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  if (table_schema.is_user_table() || table_schema.is_external_table()) {
    const uint64_t table_id = table_schema.get_table_id();
    // Pointer variant: entries owned by the underlying schema guard.
    ObArray<const ObSAuditSchema *> audits;
    if (OB_FAIL(get_schema_guard_wrapper().get_audit_schema_in_owner(
                AUDIT_TABLE, table_id, audits))) {
      LOG_WARN("fail to get audit schemas for table", KR(ret), K(table_id));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < audits.count(); ++i) {
      if (OB_ISNULL(audits.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("audit schema ptr is null", KR(ret), K(i), K(table_id));
      } else {
        const uint64_t audit_id = audits.at(i)->get_audit_key().audit_id_;
        if (OB_FAIL(add_ddl_object_lock_by_id(audit_id, AUDIT_SCHEMA, EXCLUSIVE))) {
          LOG_WARN("fail to register audit object lock", KR(ret), K(audit_id));
        }
      }
    }
  }
  return ret;
}

int ObAlterTableDropColumnAction::register_sensitive_rule_locks_(
    const ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  ObArray<const ObSensitiveRuleSchema *> sensitive_rules;
  if (OB_FAIL(get_schema_guard_wrapper().get_sensitive_rule_schemas_by_table(
              table_schema, sensitive_rules))) {
    LOG_WARN("fail to get sensitive rule schemas by table", KR(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < sensitive_rules.count(); ++i) {
    const ObSensitiveRuleSchema *rule = sensitive_rules.at(i);
    if (OB_ISNULL(rule)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sensitive rule is null", KR(ret));
    } else if (OB_FAIL(add_ddl_object_lock_by_id(
                       rule->get_sensitive_rule_id(), SENSITIVE_RULE_SCHEMA, EXCLUSIVE))) {
      LOG_WARN("fail to register sensitive rule object lock", KR(ret),
               "rule_id", rule->get_sensitive_rule_id());
    }
  }
  return ret;
}

// Registered together with aux objects so that a single sort at flush time
// handles global deadlock-avoidance ordering.
int ObAlterTableDropColumnAction::register_main_table_lock_(const ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(add_ddl_object_lock_by_id(table_schema.get_table_id(), TABLE_SCHEMA, EXCLUSIVE))) {
    LOG_WARN("fail to register main table object lock", KR(ret),
             "table_id", table_schema.get_table_id());
  }
  return ret;
}

// Index (including search-def parent) + one level of search-def children.
int ObAlterTableDropColumnAction::register_index_locks_(const ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObAuxTableMetaInfo> &infos = table_schema.get_simple_index_infos();
  for (int64_t i = 0; OB_SUCC(ret) && i < infos.count(); ++i) {
    const uint64_t idx_id = infos.at(i).table_id_;
    const ObTableSchema *idx_schema = nullptr;
    if (OB_FAIL(add_ddl_object_lock_by_id(idx_id, TABLE_SCHEMA, EXCLUSIVE))) {
      LOG_WARN("fail to register index object lock", KR(ret), K(idx_id));
    } else if (OB_FAIL(get_schema_guard_wrapper().get_table_schema(idx_id, idx_schema))) {
      LOG_WARN("fail to get index schema", KR(ret), K(idx_id));
    } else if (OB_NOT_NULL(idx_schema) && idx_schema->is_search_def_index()
               && OB_FAIL(register_search_def_child_locks_(*idx_schema))) {
      LOG_WARN("fail to register search def child object locks", KR(ret), K(idx_id));
    }
  }
  return ret;
}

int ObAlterTableDropColumnAction::register_lob_aux_locks_(const ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID != table_schema.get_aux_lob_meta_tid()
      && OB_FAIL(add_ddl_object_lock_by_id(
                 table_schema.get_aux_lob_meta_tid(), TABLE_SCHEMA, EXCLUSIVE))) {
    LOG_WARN("fail to register lob meta object lock", KR(ret));
  } else if (OB_INVALID_ID != table_schema.get_aux_lob_piece_tid()
      && OB_FAIL(add_ddl_object_lock_by_id(
                 table_schema.get_aux_lob_piece_tid(), TABLE_SCHEMA, EXCLUSIVE))) {
    LOG_WARN("fail to register lob piece object lock", KR(ret));
  }
  return ret;
}

// No MLOG lock: has_mlog_table() drop column is rejected by check_can_drop_column_
// as MLOG/MVIEW dependency, falling back to serial.
int ObAlterTableDropColumnAction::register_trigger_locks_(const ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  const ObIArray<uint64_t> &trigger_ids = table_schema.get_trigger_list();
  for (int64_t i = 0; OB_SUCC(ret) && i < trigger_ids.count(); ++i) {
    if (OB_FAIL(add_ddl_object_lock_by_id(trigger_ids.at(i), TRIGGER_SCHEMA, EXCLUSIVE))) {
      LOG_WARN("fail to register trigger object lock", KR(ret),
               "trigger_id", trigger_ids.at(i));
    }
  }
  return ret;
}

int ObAlterTableDropColumnAction::register_rls_locks_(const ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  const struct { const ObIArray<uint64_t> *ids; ObSchemaType type; const char *label; } rls_groups[] = {
    { &table_schema.get_rls_policy_ids(),  RLS_POLICY_SCHEMA,  "rls policy"  },
    { &table_schema.get_rls_group_ids(),   RLS_GROUP_SCHEMA,   "rls group"   },
    { &table_schema.get_rls_context_ids(), RLS_CONTEXT_SCHEMA, "rls context" },
  };
  for (int64_t g = 0; OB_SUCC(ret) && g < ARRAYSIZEOF(rls_groups); ++g) {
    const ObIArray<uint64_t> &ids = *rls_groups[g].ids;
    for (int64_t i = 0; OB_SUCC(ret) && i < ids.count(); ++i) {
      if (OB_FAIL(add_ddl_object_lock_by_id(ids.at(i), rls_groups[g].type, EXCLUSIVE))) {
        LOG_WARN("fail to register rls object lock", KR(ret),
                 "label", rls_groups[g].label, "id", ids.at(i));
      }
    }
  }
  return ret;
}

// Collect before lock, register VIEW_SCHEMA locks. dep_objs_before_lock_ is
// reused later by check_dep_objs_consistency_().
int ObAlterTableDropColumnAction::register_dep_view_locks_(const ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(ObDependencyInfo::collect_all_dep_objs(
                     get_tenant_id(), table_schema.get_table_id(),
                     *get_sql_proxy(), dep_objs_before_lock_))) {
    LOG_WARN("fail to collect dep objs before lock", KR(ret),
             "table_id", table_schema.get_table_id());
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < dep_objs_before_lock_.count(); ++i) {
      if (ObObjectType::VIEW == dep_objs_before_lock_.at(i).second) {
        if (OB_FAIL(add_ddl_object_lock_by_id(
                    dep_objs_before_lock_.at(i).first, VIEW_SCHEMA, EXCLUSIVE))) {
          LOG_WARN("fail to register view object lock", KR(ret),
                   "view_id", dep_objs_before_lock_.at(i).first);
        }
      }
    }
  }
  return ret;
}

int ObAlterTableDropColumnAction::register_ddl_object_locks()
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = nullptr;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_ISNULL(table_schema = get_orig_table_schema())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("orig table schema is null", KR(ret));
  } else if (OB_FAIL(register_main_table_lock_(*table_schema))) {
    LOG_WARN("fail to register main table lock", KR(ret));
  } else if (OB_FAIL(register_index_locks_(*table_schema))) {
    LOG_WARN("fail to register index locks", KR(ret));
  } else if (OB_FAIL(register_lob_aux_locks_(*table_schema))) {
    LOG_WARN("fail to register lob aux locks", KR(ret));
  } else if (OB_FAIL(register_trigger_locks_(*table_schema))) {
    LOG_WARN("fail to register trigger locks", KR(ret));
  } else if (OB_FAIL(register_identity_sequence_locks_(*table_schema))) {
    LOG_WARN("fail to register identity sequence locks", KR(ret));
  } else if (OB_FAIL(register_rls_locks_(*table_schema))) {
    LOG_WARN("fail to register rls locks", KR(ret));
  } else if (OB_FAIL(register_table_audit_locks_(*table_schema))) {
    LOG_WARN("fail to register table audit locks", KR(ret));
  } else if (OB_FAIL(register_sensitive_rule_locks_(*table_schema))) {
    LOG_WARN("fail to register sensitive rule locks", KR(ret));
  } else if (OB_FAIL(register_dep_view_locks_(*table_schema))) {
    LOG_WARN("fail to register dep view locks", KR(ret));
  }
  return ret;
}

int ObAlterTableDropColumnAction::lock_table_and_online_ddl()
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = nullptr;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_ISNULL(table_schema = get_orig_table_schema())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("orig table schema is null", KR(ret));
  } else if (OB_FAIL(storage::ObDDLLock::lock_for_online_drop_column_in_trans(*table_schema, get_trans()))) {
    // lock_for_online_drop_column_in_trans already maps retry ret codes to OB_EAGAIN internally.
    if (OB_EAGAIN == ret) {
      ret = OB_ERR_PARALLEL_DDL_CONFLICT;
    }
    LOG_WARN("fail to lock for online drop column in trans", KR(ret),
             "table_id", table_schema->get_table_id());
  }
  return ret;
}

int ObAlterTableDropColumnAction::check_legitimacy()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(check_can_drop_column_())) {
    LOG_WARN("fail to check can drop column", KR(ret));
  } else if (OB_FAIL(check_dep_objs_consistency_())) {
    LOG_WARN("fail to check dep objs consistency", KR(ret));
  } else if (OB_FAIL(collect_drop_column_related_infos_())) {
    LOG_WARN("fail to collect drop column related infos", KR(ret));
  } else if (OB_FAIL(check_can_drop_columns_())) {
    LOG_WARN("fail to check can drop columns", KR(ret));
  } else if (OB_FAIL(check_drop_column_with_index_in_mysql_mode_())) {
    LOG_WARN("fail to check drop column with index in mysql mode", KR(ret));
  } else if (OB_FAIL(check_drop_column_constraints_and_fk_())) {
    LOG_WARN("fail to check drop column constraints and fk", KR(ret));
  }
  return ret;
}

// Preliminary checks that can be unified with ObDDLService once the view
// dependency framework is complete. Until then, keep aligned with the serial
// path's check_can_drop_column_instant_ and drop_column_online entry checks.
int ObAlterTableDropColumnAction::check_can_drop_column_()
{
  int ret = OB_SUCCESS;
  const ObTableSchema *orig_table_schema = get_orig_table_schema();
  if (OB_ISNULL(orig_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("orig table schema is null", KR(ret));
  } else {
    // Check unused column count limit (must be checked under lock to prevent concurrent overflow)
    // Keep consistent with serial path: only check existing unused column count.
    ObArray<uint64_t> unused_column_ids;
    if (OB_FAIL(orig_table_schema->get_unused_column_ids(unused_column_ids))) {
      LOG_WARN("get unused column ids failed", KR(ret), KPC(orig_table_schema));
    } else if (OB_UNLIKELY(unused_column_ids.count() >= OB_MAX_UNUSED_COLUMNS_COUNT)) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("The number of obsolete columns reaches the limit", KR(ret),
               K(unused_column_ids.count()));
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "The number of obsolete columns reaches the limit. "
          "Use \"alter table table_name force\" to defragment first, otherwise dropping column is");
    } else if (orig_table_schema->is_tmp_table() && !orig_table_schema->is_mysql_tmp_table()) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("oracle temporary table column is not allowed to be dropped", KR(ret));
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "drop column on oracle temporary table is");
    }
    // MLOG / MVIEW handling, aligned with master resolver
    // (ObAlterMviewUtils::check_column_option_for_mlog_master) and serial path
    // (ObMviewAlterService::update_mview_in_modify_column).
    //
    //   has_mlog_table()            -> OB_NOT_SUPPORTED (resolver also rejects)
    //   is_materialized_view()      -> OB_NOT_SUPPORTED (MV itself does not go
    //                                  through normal alter column)
    //   table_referenced_by_mv()    -> fall back to serial
    //
    // A plain base table referenced only by full-refresh MVs (no MLOG, no
    // incremental MV, no nested MV) can be supported in the future once the
    // parallel path gains an equivalent of ObMviewAlterService.
    if (OB_SUCC(ret) && orig_table_schema->has_mlog_table()) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED,
                     "modify column to table with materialized view log is");
      LOG_WARN("modify column to table with materialized view log is not supported",
               KR(ret), "table_id", orig_table_schema->get_table_id());
    } else if (OB_SUCC(ret) && orig_table_schema->is_materialized_view()) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "drop column on materialized view is");
      LOG_WARN("drop column on materialized view is not supported",
               KR(ret), "table_id", orig_table_schema->get_table_id());
    } else if (OB_SUCC(ret) && orig_table_schema->table_referenced_by_mv()) {
      ret = OB_NOT_SUPPORTED_FOR_PARALLEL_DDL;
      LOG_INFO("table referenced by materialized view, parallel drop column "
               "falls back to serial (serial will rebuild MV container schema "
               "or reject nested MV)", KR(ret),
               "table_id", orig_table_schema->get_table_id());
    }
  }
  return ret;
}

// Per-column checks kept aligned with the serial path by reusing ObDDLService methods
// (ObAlterTableDropColumnAction is already a friend of ObDDLService):
//   - check_alter_unused_column: reject drop on an already-unused column. Serial path
//     calls the same method inside ObDDLService::alter_table_column.
//   - check_can_drop_column (v2): column existence in new schema, Oracle temp table,
//     generated-column / functional-index dependency, partition key (incl. global
//     index), last-column removal, rowkey column. Same method the serial path uses
//     inside ObDDLService::drop_column_online.
// Depends on collect_drop_column_related_infos_() having populated
// new_tbl_visible_cols_cnt_after_alter_ and last_drop_column_id_.
// Depends on check_after_lock_() deep-copying orig_table_schema_ -> new_table_schema_
// before calling check_legitimacy(), so get_new_table_schema() returns a real value.
int ObAlterTableDropColumnAction::check_can_drop_columns_()
{
  int ret = OB_SUCCESS;
  const ObTableSchema *orig_table_schema = get_orig_table_schema();
  const AlterTableSchema &alter_table_schema = get_arg().alter_table_schema_;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_ISNULL(orig_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("orig table schema is null", KR(ret), KP(orig_table_schema));
  } else {
    const AlterColumnSchema *alter_col = nullptr;
    ObTableSchema::const_column_iterator it = alter_table_schema.column_begin();
    ObTableSchema::const_column_iterator it_end = alter_table_schema.column_end();
    for (; OB_SUCC(ret) && it != it_end; ++it) {
      if (OB_ISNULL(*it)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column is null", KR(ret));
      } else if (OB_ISNULL(alter_col = static_cast<const AlterColumnSchema *>(*it))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("alter column is null", KR(ret));
      } else if (OB_DDL_DROP_COLUMN != alter_col->alter_type_) {
        ret = OB_NOT_SUPPORTED_FOR_PARALLEL_DDL;
        LOG_WARN("unexpected column alter type for drop column parallel DDL, fall back to serial",
                 KR(ret), "alter_type", alter_col->alter_type_);
      } else {
        const ObString &origin_column_name = alter_col->get_origin_column_name();
        const ObColumnSchemaV2 *orig_col = orig_table_schema->get_column_schema(origin_column_name);
        if (OB_ISNULL(orig_col)) {
          ret = OB_ERR_CANT_DROP_FIELD_OR_KEY;
          LOG_USER_ERROR(OB_ERR_CANT_DROP_FIELD_OR_KEY,
                         origin_column_name.length(), origin_column_name.ptr());
          LOG_WARN("column not found", KR(ret), K(origin_column_name));
        } else if (OB_FAIL(get_ddl_service()->check_alter_unused_column(OB_DDL_DROP_COLUMN, orig_col))) {
          LOG_WARN("cannot drop unused column", KR(ret), K(origin_column_name));
        } else if (OB_FAIL(get_ddl_service()->check_can_drop_column(
                           origin_column_name,
                           orig_col,
                           *orig_table_schema,
                           get_new_table_schema(),
                           new_tbl_visible_cols_cnt_after_alter_,
                           last_drop_column_id_,
                           get_schema_guard_wrapper()))) {
          LOG_WARN("check_can_drop_column failed", KR(ret), K(origin_column_name));
        }
      }
    }
  }
  return ret;
}

int ObAlterTableDropColumnAction::check_dep_objs_consistency_()
{
  int ret = OB_SUCCESS;
  const ObTableSchema *orig_table_schema = get_orig_table_schema();
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_ISNULL(orig_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("orig table schema is null", KR(ret), KP(orig_table_schema));
  } else if (OB_FAIL(ObDependencyInfo::collect_all_dep_objs(get_tenant_id(),
                                                            orig_table_schema->get_table_id(),
                                                            *get_sql_proxy(),
                                                            dep_objs_))) {
    LOG_WARN("fail to collect dep objs after lock", KR(ret),
             "table_id", orig_table_schema->get_table_id(),
             "before_lock_cnt", dep_objs_before_lock_.count());
  } else if (OB_FAIL(ObDDLHelper::check_dep_objs_consistent(
                     dep_objs_before_lock_, dep_objs_))) {
    LOG_WARN("dep objs changed between lock phases", KR(ret),
             "table_id", orig_table_schema->get_table_id(),
             "before_lock_cnt", dep_objs_before_lock_.count(),
             "after_lock_cnt", dep_objs_.count());
  }
  return ret;
}

int ObAlterTableDropColumnAction::collect_drop_column_related_infos_()
{
  int ret = OB_SUCCESS;
  const ObTableSchema *orig_table_schema = get_orig_table_schema();
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_ISNULL(orig_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("orig table schema is null", KR(ret), KP(orig_table_schema));
  } else if (OB_FAIL(get_ddl_service()->get_all_dropped_column_ids(
                     get_arg(), *orig_table_schema, drop_cols_id_arr_,
                     &new_tbl_visible_cols_cnt_after_alter_, &last_drop_column_id_))) {
    LOG_WARN("fail to get all dropped column ids", KR(ret));
  } else if (OB_FAIL(collect_affected_index_infos_())) {
    LOG_WARN("fail to collect affected index infos", KR(ret));
  }
  // dep_objs_ is filled by check_dep_objs_consistency_ above, which runs under
  // lock. Do not re-query here.
  return ret;
}

int ObAlterTableDropColumnAction::calc_sequence_schema_version_cnt_()
{
  int ret = OB_SUCCESS;
  const ObTableSchema *orig_table_schema = get_orig_table_schema();
  if (OB_ISNULL(orig_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("orig table schema is null", KR(ret));
  }
  // Count identity columns for schema version calculation.
  // drop_sequence consumes: 1 (sequence itself) + obj_priv_count + audit_count
  // See ObSequenceDDLProxy::drop_sequence() in ob_sequence_ddl_proxy.cpp
  for (int64_t i = 0; OB_SUCC(ret) && i < drop_cols_id_arr_.count(); ++i) {
    const ObColumnSchemaV2 *col = orig_table_schema->get_column_schema(drop_cols_id_arr_.at(i));
    if (OB_NOT_NULL(col) && col->is_identity_column()) {
      drop_sequence_schema_version_cnt_ += 1; // sequence drop itself
      // Count obj_privs on the sequence
      const ObSequenceSchema *seq_schema = nullptr;
      if (OB_FAIL(get_schema_guard_wrapper().get_sequence_schema(
                  col->get_sequence_id(), seq_schema))) {
        LOG_WARN("fail to get sequence schema", KR(ret), "seq_id", col->get_sequence_id());
      } else if (OB_ISNULL(seq_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sequence schema is null for identity column", KR(ret),
                 "seq_id", col->get_sequence_id(), "col_id", drop_cols_id_arr_.at(i));
      } else {
        ObSEArray<const ObObjPriv *, 4> obj_privs;
        if (OB_FAIL(get_schema_guard_wrapper().get_obj_priv_with_obj_id(
                    seq_schema->get_sequence_id(),
                    static_cast<uint64_t>(ObObjectType::SEQUENCE),
                    obj_privs, true/*reset_flag*/))) {
          LOG_WARN("fail to get obj privs for sequence", KR(ret));
        } else {
          drop_sequence_schema_version_cnt_ += obj_privs.count();
        }
        // Count audit records on the sequence
        if (OB_SUCC(ret)) {
          ObSEArray<const ObSAuditSchema *, 4> audits;
          if (OB_FAIL(get_schema_guard_wrapper().get_audit_schema_in_owner(
                      AUDIT_SEQUENCE, seq_schema->get_sequence_id(), audits))) {
            LOG_WARN("fail to get audit schemas for sequence", KR(ret));
          } else {
            drop_sequence_schema_version_cnt_ += audits.count();
          }
        }
      }
    }
  }
  return ret;
}

int ObAlterTableDropColumnAction::collect_affected_index_infos_()
{
  int ret = OB_SUCCESS;
  const ObTableSchema *orig_table_schema = get_orig_table_schema();
  if (OB_ISNULL(orig_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("orig table schema is null", KR(ret));
  } else {
    ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
    if (OB_FAIL(orig_table_schema->get_simple_index_infos(simple_index_infos))) {
      LOG_WARN("fail to get simple index infos", KR(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); ++i) {
      const ObTableSchema *index_schema = nullptr;
      const uint64_t index_id = simple_index_infos.at(i).table_id_;
      if (OB_FAIL(get_schema_guard_wrapper().get_table_schema(index_id, index_schema))) {
        LOG_WARN("fail to get index schema", KR(ret), K(index_id));
      } else if (OB_ISNULL(index_schema)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("index not exist", KR(ret), K(index_id));
      } else if (OB_UNLIKELY(index_schema->is_in_recyclebin())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("index in recyclebin", KR(ret), K(index_id));
      } else {
        bool is_affected = false;
        for (int64_t j = 0; OB_SUCC(ret) && j < drop_cols_id_arr_.count(); ++j) {
          bool has_this_col = false;
          if (OB_FAIL(index_schema->has_column(drop_cols_id_arr_.at(j), has_this_col))) {
            LOG_WARN("fail to check has_column", KR(ret));
          } else if (has_this_col) {
            is_affected = true;
            // Per (dropped_col, aux_table) hit:
            //   update_column_and_column_group(1 ver) + sync_aux_schema_version_for_history(1 ver)
            aux_column_update_schema_version_cnt_ += 2;
            // global unique index has shadow column that also needs update (1 ver)
            if (index_schema->is_global_index_table()) {
              const ObColumnSchemaV2 *shadow_col = index_schema->get_column_schema(
                    drop_cols_id_arr_.at(j) + common::OB_MIN_SHADOW_COLUMN_ID);
              if (OB_NOT_NULL(shadow_col)) {
                aux_column_update_schema_version_cnt_ += 1;
              }
            }
          }
        }
        if (OB_SUCC(ret) && is_affected) {
          if (OB_FAIL(affected_index_ids_.push_back(index_id))) {
            LOG_WARN("fail to push back affected index id", KR(ret), K(index_id));
          }
        }
      }
    }
    LOG_INFO("collect affected index infos",
             "affected_index_count", affected_index_ids_.count(),
             K_(aux_column_update_schema_version_cnt));
  }
  return ret;
}

int ObAlterTableDropColumnAction::check_drop_column_with_index_in_mysql_mode_()
{
  int ret = OB_SUCCESS;
  const ObTableSchema *orig_table_schema = get_orig_table_schema();
  if (OB_ISNULL(orig_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("orig table schema is null", KR(ret));
  } else {
    bool is_oracle_mode = false;
    if (OB_FAIL(orig_table_schema->check_if_oracle_compat_mode(is_oracle_mode))) {
      LOG_WARN("fail to check oracle mode", KR(ret));
    } else if (!is_oracle_mode && affected_index_ids_.count() > 0) {
      // The executor pre-check should already route this case to serial DDL.
      // If schema changes between pre-check and RS execution, return the fallback
      // code so the caller can retry in the serial path instead of surfacing an
      // internal error to the user.
      ret = OB_NOT_SUPPORTED_FOR_PARALLEL_DDL;
      LOG_INFO("drop column with index in mysql mode falls back to serial path",
               KR(ret), K_(affected_index_ids));
    }
  }
  return ret;
}

int ObAlterTableDropColumnAction::calc_index_schema_version_cnt_()
{
  // aux_column_update_schema_version_cnt_ is already calculated in collect_affected_index_infos_()
  // Each affected index needs 2 versions for drop_index_to_scheduler:
  //   rename_dropping_index_name generates 1 version for the index table rename,
  //   and update_table_options internally calls update_data_table_schema_version
  //   for index tables, which generates 1 more version for the data table.
  drop_index_schema_version_cnt_ = affected_index_ids_.count() * 2;
  LOG_INFO("calc index schema version cnt",
           K_(drop_index_schema_version_cnt),
           K_(aux_column_update_schema_version_cnt),
           "affected_index_count", affected_index_ids_.count());
  return OB_SUCCESS;
}

int ObAlterTableDropColumnAction::calc_sensitive_rule_schema_version_cnt_()
{
  int ret = OB_SUCCESS;
  const ObTableSchema *orig_table_schema = get_orig_table_schema();
  if (OB_ISNULL(orig_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("orig table schema is null", KR(ret));
  } else {
    ObSEArray<const ObSensitiveRuleSchema *, 4> sensitive_rules;
    if (OB_FAIL(get_schema_guard_wrapper().get_sensitive_rule_schemas_by_table(
                *orig_table_schema, sensitive_rules))) {
      LOG_WARN("fail to get sensitive rule schemas by table", KR(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < sensitive_rules.count(); ++i) {
      const ObSensitiveRuleSchema *rule = sensitive_rules.at(i);
      if (OB_ISNULL(rule)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sensitive rule is null", KR(ret));
      } else {
        // handle_sensitive_rule_function_inner always consumes 1 version
        sensitive_rule_schema_version_cnt_ += 1;
        // update_table_schema consumes 1 version per unique table with matching items
        bool has_matching = false;
        for (int64_t j = 0; !has_matching && j < drop_cols_id_arr_.count(); ++j) {
          ObSensitiveFieldItem item(orig_table_schema->get_table_id(), drop_cols_id_arr_.at(j));
          if (has_exist_in_array(rule->get_sensitive_field_items(), item)) {
            has_matching = true;
          }
        }
        if (has_matching) {
          sensitive_rule_schema_version_cnt_ += 1;
        }
      }
    }
    LOG_INFO("calc sensitive rule schema version cnt",
             K_(sensitive_rule_schema_version_cnt), "rule_count", sensitive_rules.count());
  }
  return ret;
}

int ObAlterTableDropColumnAction::calc_rls_schema_version_cnt_()
{
  int ret = OB_SUCCESS;
  const ObTableSchema *orig_table_schema = get_orig_table_schema();
  if (OB_ISNULL(orig_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("orig table schema is null", KR(ret));
  } else if (orig_table_schema->get_rls_policy_ids().empty()) {
    // no RLS policies, nothing to count
  } else {
    bool rls_object_changed = false;
    const ObIArray<uint64_t> &rls_policy_ids = orig_table_schema->get_rls_policy_ids();
    for (int64_t i = 0; OB_SUCC(ret) && i < rls_policy_ids.count(); ++i) {
      const ObRlsPolicySchema *policy_schema = nullptr;
      if (OB_FAIL(get_schema_guard_wrapper().get_rls_policy_schema_by_id(
              rls_policy_ids.at(i), policy_schema))) {
        LOG_WARN("fail to get rls policy schema", KR(ret), "policy_id", rls_policy_ids.at(i));
      } else if (OB_ISNULL(policy_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("rls policy schema is null", KR(ret), "policy_id", rls_policy_ids.at(i));
      } else {
        // Count how many sec columns in this policy will be dropped
        int64_t affected_sec_count = 0;
        for (int64_t j = 0; OB_SUCC(ret) && j < policy_schema->get_sec_column_count(); ++j) {
          const ObRlsSecColumnSchema *sec_col = policy_schema->get_sec_column_by_idx(j);
          if (OB_ISNULL(sec_col)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("sec column is null", KR(ret), K(j));
          } else {
            for (int64_t k = 0; k < drop_cols_id_arr_.count(); ++k) {
              if (sec_col->get_column_id() == drop_cols_id_arr_.at(k)) {
                ++affected_sec_count;
                break;
              }
            }
          }
        }
        if (OB_SUCC(ret) && affected_sec_count > 0) {
          rls_object_changed = true;
          rls_schema_version_cnt_ += 1; // drop_rls_policy
          const int64_t new_sec_count = policy_schema->get_sec_column_count() - affected_sec_count;
          // If original was column-level and new has 0 sec columns, skip create
          if (!(policy_schema->is_column_level_policy() && 0 == new_sec_count)) {
            rls_schema_version_cnt_ += 1; // create_rls_policy
          }
        }
      }
    }
    if (OB_SUCC(ret) && rls_object_changed) {
      rls_schema_version_cnt_ += 1; // update_table_attribute for RLS flag update
    }
    LOG_INFO("calc rls schema version cnt", K_(rls_schema_version_cnt),
             K(rls_object_changed), "policy_count", rls_policy_ids.count(),
             "drop_col_count", drop_cols_id_arr_.count());
  }
  return ret;
}

int ObAlterTableDropColumnAction::check_fk_related_table_ddl_(
    const ObTableSchema &data_table_schema,
    const ObDDLType &ddl_type)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == get_tenant_id()
             || ObDDLType::DDL_INVALID == ddl_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(get_tenant_id()), K(ddl_type));
  } else {
    // Adapted from ObCreateIndexHelper::check_fk_related_table_ddl_
    // Uses schema_guard_wrapper_ instead of creating local guard
    const ObIArray<ObForeignKeyInfo> &foreign_key_infos = data_table_schema.get_foreign_key_infos();
    const ObCheckExistedDDLMode check_mode = is_double_table_long_running_ddl(ddl_type) ?
          ObCheckExistedDDLMode::ALL_LONG_RUNNING_DDL : ObCheckExistedDDLMode::DOUBLE_TABLE_RUNNING_DDL;
    for (int64_t i = 0; OB_SUCC(ret) && i < foreign_key_infos.count(); ++i) {
      const ObForeignKeyInfo &foreign_key_info = foreign_key_infos.at(i);
      const uint64_t related_table_id = data_table_schema.get_table_id() == foreign_key_info.parent_table_id_
                                        ? foreign_key_info.child_table_id_ : foreign_key_info.parent_table_id_;
      if (data_table_schema.get_table_id() == related_table_id) {
        // self-ref, skip
      } else if (foreign_key_info.is_parent_table_mock_) {
        const ObMockFKParentTableSchema *related_schema = nullptr;
        if (OB_FAIL(get_schema_guard_wrapper().get_mock_fk_parent_table_schema(
                    related_table_id, related_schema))) {
          LOG_WARN("fail to get mock fk parent table schema", KR(ret), K(related_table_id));
        } else if (OB_ISNULL(related_schema)) {
          ret = OB_ERR_PARALLEL_DDL_CONFLICT;
          LOG_WARN("mock fk parent table may be dropped", KR(ret), K(related_table_id));
        }
      } else {
        const ObTableSchema *related_schema = nullptr;
        bool has_long_running_ddl = false;
        if (OB_FAIL(get_schema_guard_wrapper().get_table_schema(
                    related_table_id, related_schema))) {
          LOG_WARN("fail to get related table schema", KR(ret), K(related_table_id));
        } else if (OB_ISNULL(related_schema)) {
          ret = OB_ERR_PARALLEL_DDL_CONFLICT;
          LOG_WARN("related table may be dropped", KR(ret), K(related_table_id));
        } else if (!related_schema->check_can_do_ddl()) {
          ret = OB_OP_NOT_ALLOW;
          LOG_USER_ERROR(OB_OP_NOT_ALLOW,
                         "execute ddl while foreign key related table is executing long running ddl");
        } else if (OB_FAIL(ObDDLTaskRecordOperator::check_has_long_running_ddl(
                get_sql_proxy(), get_tenant_id(), related_table_id,
                check_mode, has_long_running_ddl))) {
          LOG_WARN("check has long running ddl failed", KR(ret), K(related_table_id));
        } else if (has_long_running_ddl) {
          ret = OB_OP_NOT_ALLOW;
          LOG_WARN("foreign key related table is executing offline ddl", KR(ret));
          LOG_USER_ERROR(OB_OP_NOT_ALLOW,
                         "execute ddl while foreign key related table is executing long running ddl");
        }
      }
    }
  }
  return ret;
}

// NOTE on mock FK parent table: when DROP COLUMN is combined with
// DROP FOREIGN KEY, the executor's check_alter_table_arg_for_parallel()
// rejects via has_no_non_column_operation() (index_arg_list_ is non-empty),
// so the DDL falls back to the serial path where alter_table_index() handles
// mock_fk_parent_table_schema creation/deletion. No mock FK handling is
// needed in the parallel path.
int ObAlterTableDropColumnAction::check_drop_column_constraints_and_fk_()
{
  int ret = OB_SUCCESS;
  const ObTableSchema *orig_table_schema = get_orig_table_schema();
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_ISNULL(orig_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("orig table schema is null", KR(ret));
  // 1. FK related table DDL conflict check
  } else if (OB_FAIL(check_fk_related_table_ddl_(*orig_table_schema, ObDDLType::DDL_DROP_COLUMN_INSTANT))) {
    LOG_WARN("fail to check fk related table ddl", KR(ret));
  // 2. FK child column check (drop FK when its child column is dropped)
  } else if (OB_FAIL(get_ddl_service()->check_drop_column_with_drop_foreign_key(
                     get_arg(), *orig_table_schema, drop_cols_id_arr_))) {
    LOG_WARN("fail to check drop column with drop foreign key", KR(ret));
  // 3. Check constraint validation (check_drop_column_with_drop_constraint)
  } else if (OB_FAIL(get_ddl_service()->check_drop_column_with_drop_constraint(
                     get_arg(), get_schema_guard_wrapper(), *orig_table_schema, drop_cols_id_arr_))) {
    LOG_WARN("fail to check drop column with drop constraint", KR(ret));
  }
  return ret;
}

int ObAlterTableDropColumnAction::calc_constraint_schema_version_cnt_()
{
  int ret = OB_SUCCESS;
  const AlterTableSchema &alter_table_schema = get_arg().alter_table_schema_;
  for (ObTableSchema::const_constraint_iterator iter = alter_table_schema.constraint_begin();
       OB_SUCC(ret) && iter != alter_table_schema.constraint_end(); ++iter) {
    if (OB_ISNULL(*iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("constraint is null", KR(ret));
    } else if (OB_NOT_NULL(get_new_table_schema().get_constraint((*iter)->get_constraint_id()))) {
      drop_constraint_schema_version_cnt_++;
    }
  }
  return ret;
}

// All pre-checks and collection have moved to check() (under lock); the actual
// new_table_schema_ is materialized by operate_schemas(). Nothing left to do
// here, but the base class declares generate_schemas() as pure virtual, so we
// keep an empty override.
int ObAlterTableDropColumnAction::generate_schemas()
{
  return OB_SUCCESS;
}

int ObAlterTableDropColumnAction::calc_dep_obj_schema_version_cnt_()
{
  int ret = OB_SUCCESS;
  // Views are locked with EXCLUSIVE in register_ddl_object_locks() and verified
  // by check_dep_objs_consistency_(), so they cannot be dropped between calc and
  // operate phases. The null/INVALID checks below are purely defensive.
  for (int64_t i = 0; OB_SUCC(ret) && i < dep_objs_.count(); ++i) {
    if (OB_INVALID_ID == dep_objs_.at(i).first) {
      continue;
    }
    if (ObObjectType::VIEW == dep_objs_.at(i).second) {
      const ObTableSchema *view_schema = nullptr;
      if (OB_FAIL(get_schema_guard_wrapper().get_table_schema(dep_objs_.at(i).first, view_schema))) {
        LOG_WARN("fail to get view schema", KR(ret), "view_id", dep_objs_.at(i).first);
      } else if (OB_ISNULL(view_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("view schema is null under EXCLUSIVE lock, should not happen",
                 KR(ret), "view_id", dep_objs_.at(i).first);
      } else if (ObObjectStatus::INVALID != view_schema->get_object_status()) {
        dep_obj_schema_version_cnt_++;
      }
    }
  }
  return ret;
}

int ObAlterTableDropColumnAction::calc_schema_version_cnt()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(calc_sequence_schema_version_cnt_())) {
    LOG_WARN("fail to calc sequence schema version cnt", KR(ret));
  } else if (OB_FAIL(calc_constraint_schema_version_cnt_())) {
    LOG_WARN("fail to calc constraint schema version cnt", KR(ret));
  } else if (OB_FAIL(calc_index_schema_version_cnt_())) {
    LOG_WARN("fail to calc index schema version cnt", KR(ret));
  } else if (OB_FAIL(calc_rls_schema_version_cnt_())) {
    LOG_WARN("fail to calc rls schema version cnt", KR(ret));
  } else if (OB_FAIL(calc_sensitive_rule_schema_version_cnt_())) {
    LOG_WARN("fail to calc sensitive rule schema version cnt", KR(ret));
  } else if (OB_FAIL(calc_dep_obj_schema_version_cnt_())) {
    LOG_WARN("fail to calc dep obj schema version cnt", KR(ret));
  } else {
    // NOTE: drop_cols_id_arr_ already includes XMLType hidden columns
    // (populated by get_all_dropped_udt_hidden_column_ids in collect_drop_column_related_infos_),
    // so schema versions for hidden column's prev_id update and column_and_column_group update
    // are already accounted for in total_drop_column_count * 2.
    const int64_t total_drop_column_count = drop_cols_id_arr_.count();
    // Per column: update_prev_id(1) + update_column_and_column_group on data table(1) = 2
    // Per (col, aux_table) hit: update_column_and_column_group(1) + sync_aux_schema_version_for_history(1) = 2
    //   + 1 for global index shadow column update_single_column if applicable
    // Per affected index: drop_index_to_scheduler -> rename_dropping_index_name(1)
    //   + update_data_table_schema_version(1) triggered by update_table_options for index tables
    // Per constraint: drop_table_constraints -> delete_single_constraint(1) = 1
    // Per sensitive rule on table: handle_sensitive_rule_function_inner(1) [+ update_table_schema(1) if matching]
    // Per identity column: drop_sequence consumes 1 + obj_priv_count + audit_count versions
    // RLS: drop/create per affected policy + update_table_attribute if any changed
    // MLog/MView are rejected upfront by check_can_drop_column_, so no version cost here.
    const int64_t table_update_cnt = 1;
    const int64_t schema_version_cnt = total_drop_column_count * 2
                                       + aux_column_update_schema_version_cnt_
                                       + drop_index_schema_version_cnt_
                                       + drop_constraint_schema_version_cnt_
                                       + sensitive_rule_schema_version_cnt_
                                       + drop_sequence_schema_version_cnt_
                                       + rls_schema_version_cnt_
                                       + dep_obj_schema_version_cnt_
                                       + table_update_cnt;
    add_schema_version_cnt(schema_version_cnt);
    LOG_INFO("calc drop column schema version cnt",
             K(total_drop_column_count), K(schema_version_cnt),
             K_(aux_column_update_schema_version_cnt),
             K_(drop_index_schema_version_cnt),
             K_(drop_constraint_schema_version_cnt),
             K_(sensitive_rule_schema_version_cnt),
             K_(drop_sequence_schema_version_cnt),
             K_(rls_schema_version_cnt),
             K_(dep_obj_schema_version_cnt),
             K(table_update_cnt));
  }
  return ret;
}

int ObAlterTableDropColumnAction::operate_schemas()
{
  int ret = OB_SUCCESS;
  bool is_oracle_mode = false;
  const ObTableSchema *orig_table_schema = nullptr;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_ISNULL(orig_table_schema = get_orig_table_schema())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("orig table schema is null", KR(ret));
  } else if (OB_FAIL(orig_table_schema->check_if_oracle_compat_mode(is_oracle_mode))) {
    LOG_WARN("fail to check oracle mode", KR(ret));
  }
  if (OB_SUCC(ret)) {
    ObTableSchema &new_table_schema = get_new_table_schema();
    const AlterTableSchema &alter_table_schema = get_arg().alter_table_schema_;
    // Set compat mode for correct column name comparison (case sensitivity in Oracle mode)
    lib::CompatModeGuard compat_mode_guard(is_oracle_mode
                         ? lib::Worker::CompatMode::ORACLE : lib::Worker::CompatMode::MYSQL);

    // Step 1-2: Drop main-table columns one by one (must follow arg order for prev_id chain).
    // drop_main_table_column_online handles Oracle XMLType hidden columns internally.
    ObDDLOperator ddl_operator(get_ddl_service()->get_schema_service(), get_ddl_service()->get_sql_proxy());
    if (OB_SUCC(ret)) {
      const AlterColumnSchema *alter_col = nullptr;
      ObTableSchema::const_column_iterator it = alter_table_schema.column_begin();
      ObTableSchema::const_column_iterator it_end = alter_table_schema.column_end();
      for (; OB_SUCC(ret) && it != it_end; ++it) {
        if (OB_ISNULL(*it)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column is null", KR(ret));
        } else if (OB_ISNULL(alter_col = static_cast<const AlterColumnSchema *>(*it))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("alter column is null", KR(ret));
        } else if (OB_DDL_DROP_COLUMN != alter_col->alter_type_) {
          ret = OB_NOT_SUPPORTED_FOR_PARALLEL_DDL;
          LOG_WARN("unexpected column alter type for drop column parallel DDL, fall back to serial",
                   KR(ret), "alter_type", alter_col->alter_type_);
        } else {
          const ObString &origin_column_name = alter_col->get_origin_column_name();
          if (OB_FAIL(get_ddl_service()->drop_main_table_column_online(
                                         get_schema_guard_wrapper(),
                                         *orig_table_schema,
                                         origin_column_name,
                                         is_oracle_mode,
                                         new_tbl_visible_cols_cnt_after_alter_,
                                         last_drop_column_id_,
                                         ddl_operator,
                                         get_trans(),
                                         new_table_schema,
                                         update_column_name_set_))) {
            LOG_WARN("fail to drop main table column online", KR(ret), K(origin_column_name));
          }
        }
      }
    }
    // Step 3: Drop constraints caused by drop column (persist to __all_constraint + update in-memory)
    if (FAILEDx(get_ddl_service()->drop_constraint_caused_by_drop_column(
                                   get_arg(), get_schema_guard_wrapper(),
                                   *orig_table_schema, new_table_schema,
                                   ddl_operator, get_trans()))) {
      LOG_WARN("fail to drop constraint caused by drop column", KR(ret));
    // Step 4-7: Drop column-related aux objects (rls/lob/index/sensitive).
    } else if (OB_FAIL(get_ddl_service()->drop_column_aux_objects_online(
                                          get_schema_guard_wrapper(),
                                          get_arg(),
                                          *orig_table_schema,
                                          drop_cols_id_arr_,
                                          get_allocator(),
                                          new_table_schema,
                                          ddl_operator,
                                          get_trans(),
                                          get_ddl_task_records(),
                                          get_res()))) {
      LOG_WARN("fail to drop column aux objects online", KR(ret));
    // Step 8: Invalidate dependent objects (views that reference this table)
    } else if (OB_FAIL(get_ddl_service()->modify_dep_obj_status_for_alter_table(
                           get_arg(), *orig_table_schema, ddl_operator, get_trans()))) {
      LOG_WARN("fail to modify dep obj status", KR(ret));
    } else if (OB_FAIL(ddl_operator.update_table_attribute(
                   new_table_schema, get_trans(), OB_DDL_ALTER_TABLE, &get_arg().ddl_stmt_str_))) {
      LOG_WARN("fail to update table attribute", KR(ret));
    }
  }
  RS_TRACE(operate_schemas);
  return ret;
}

int ObAlterTableDropColumnAction::adjust_result()
{
  get_res().ddl_type_ = DDL_DROP_COLUMN_INSTANT;
  return OB_SUCCESS;
}
