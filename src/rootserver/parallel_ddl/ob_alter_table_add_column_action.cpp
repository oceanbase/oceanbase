/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX RS
#include "rootserver/parallel_ddl/ob_alter_table_add_column_action.h"
#include "rootserver/ob_ddl_service.h"
#include "rootserver/ob_ddl_operator.h"
#include "share/ob_rpc_struct.h"
#include "share/schema/ob_schema_utils.h"
#include "share/ob_ddl_common.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_schema_struct.h"
#include "storage/ddl/ob_ddl_lock.h"

using namespace oceanbase::rootserver;
using namespace oceanbase::obrpc;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::common;
using namespace oceanbase::transaction::tablelock;

ERRSIM_POINT_DEF(ERRSIM_ADD_COLUMN_STALE_SCHEMA_GUARD);
int ObAlterTableAddColumnAction::init_local_schema_guard_for_default_value_()
{
  int ret = OB_SUCCESS;
  const ObTableSchema *orig_table_schema = get_orig_table_schema();
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_ISNULL(orig_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("orig table schema is null", KR(ret));
  } else if (local_schema_guard_.is_inited()) {
    // Already initialized by a previous generate_schemas retry.
  } else if (OB_ISNULL(get_schema_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", KR(ret));
  } else {
    // A historical snapshot exercises both missing and outdated local schemas.
    const int64_t schema_version = OB_UNLIKELY(ERRSIM_ADD_COLUMN_STALE_SCHEMA_GUARD)
        ? orig_table_schema->get_schema_version() - 1 : OB_INVALID_VERSION;
    if (OB_FAIL(get_schema_service()->get_tenant_schema_guard(
                  get_tenant_id(), local_schema_guard_, schema_version))) {
      LOG_WARN("fail to get local tenant schema guard",
               KR(ret), "tenant_id", get_tenant_id());
    } else {
      local_schema_guard_.set_session_id(get_arg().session_id_);
    }
  }
  if (OB_SUCC(ret)) {
    const ObTableSchema *local_table_schema = nullptr;
    if (OB_FAIL(local_schema_guard_.get_table_schema(get_tenant_id(), orig_table_schema->get_table_id(), local_table_schema))) {
      LOG_WARN("fail to get local table schema", KR(ret),
               "tenant_id", get_tenant_id(), "table_id", orig_table_schema->get_table_id());
    } else if (OB_ISNULL(local_table_schema)) {
      ret = OB_ERR_PARALLEL_DDL_CONFLICT;
      LOG_WARN("local table schema not ready, retry add column", KR(ret),
               "tenant_id", get_tenant_id(), "table_id", orig_table_schema->get_table_id());
    } else if (local_table_schema->get_schema_version() != orig_table_schema->get_schema_version()) {
      ret = OB_ERR_PARALLEL_DDL_CONFLICT;
      LOG_WARN("schema snapshot mismatch between latest guard and local guard, retry add column",
               KR(ret),
               "tenant_id", get_tenant_id(),
               "table_id", orig_table_schema->get_table_id(),
               "latest_schema_version", orig_table_schema->get_schema_version(),
               "local_schema_version", local_table_schema->get_schema_version());
    }
  }
  if (OB_SCHEMA_EAGAIN == ret) {
    // DDL retries use the parallel-conflict code for a schema that is not ready.
    ret = OB_ERR_PARALLEL_DDL_CONFLICT;
  }
  return ret;
}

int ObAlterTableAddColumnAction::init()
{
  return check_inner_stat_();
}

int ObAlterTableAddColumnAction::register_main_table_lock_(const ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(add_ddl_object_lock_by_id(table_schema.get_table_id(), TABLE_SCHEMA, EXCLUSIVE))) {
    LOG_WARN("fail to register main table object lock", KR(ret),
             "table_id", table_schema.get_table_id());
  }
  return ret;
}

// Existing auxiliary tables must be locked before synchronizing their round.
// Newly created LOB tables inherit the updated main-table round at creation.
int ObAlterTableAddColumnAction::register_aux_table_locks_(const ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  bool need_progressive_merge = false;
  aux_table_ids_for_round_.reset();
  if (OB_FAIL(get_ddl_service()->check_need_add_progressive_round(
                  get_arg().data_version_, table_schema,
                  get_arg().alter_table_schema_, need_progressive_merge))) {
    LOG_WARN("fail to check progressive merge round", KR(ret));
  } else if (need_progressive_merge) {
    const ObIArray<ObAuxTableMetaInfo> &index_infos = table_schema.get_simple_index_infos();
    for (int64_t i = 0; OB_SUCC(ret) && i < index_infos.count(); ++i) {
      const uint64_t table_id = index_infos.at(i).table_id_;
      if (OB_FAIL(add_ddl_object_lock_by_id(table_id, TABLE_SCHEMA, EXCLUSIVE))) {
        LOG_WARN("fail to register index object lock", KR(ret), K(table_id));
      } else if (OB_FAIL(aux_table_ids_for_round_.push_back(table_id))) {
        LOG_WARN("fail to record index table id", KR(ret), K(table_id));
      } else if (is_search_def_index(index_infos.at(i).index_type_)) {
        const ObTableSchema *index_schema = nullptr;
        if (OB_FAIL(get_schema_guard_wrapper().get_table_schema(table_id, index_schema))) {
          LOG_WARN("fail to get search index schema", KR(ret), K(table_id));
        } else if (OB_ISNULL(index_schema)) {
          ret = OB_ERR_PARALLEL_DDL_CONFLICT;
          LOG_WARN("search index changed before locking", KR(ret), K(table_id));
        // Validate the parent version after locking so its child list cannot change unnoticed.
        } else if (OB_FAIL(helper_.add_based_schema_object_info(ObBasedSchemaObjectInfo(
                             table_id, TABLE_SCHEMA, index_schema->get_schema_version())))) {
          LOG_WARN("fail to record search index schema version", KR(ret), K(table_id));
        } else {
          const ObIArray<ObAuxTableMetaInfo> &children = index_schema->get_simple_index_infos();
          for (int64_t j = 0; OB_SUCC(ret) && j < children.count(); ++j) {
            const uint64_t child_id = children.at(j).table_id_;
            if (OB_FAIL(add_ddl_object_lock_by_id(child_id, TABLE_SCHEMA, EXCLUSIVE))) {
              LOG_WARN("fail to register search index child lock", KR(ret), K(child_id));
            } else if (OB_FAIL(aux_table_ids_for_round_.push_back(child_id))) {
              LOG_WARN("fail to record search index child id", KR(ret), K(child_id));
            }
          }
        }
      }
    }
    if (OB_SUCC(ret) && OB_INVALID_ID != table_schema.get_aux_lob_meta_tid()
        && OB_FAIL(aux_table_ids_for_round_.push_back(table_schema.get_aux_lob_meta_tid()))) {
      LOG_WARN("fail to record lob meta table id", KR(ret));
    } else if (OB_SUCC(ret) && OB_INVALID_ID != table_schema.get_aux_lob_piece_tid()
        && OB_FAIL(aux_table_ids_for_round_.push_back(table_schema.get_aux_lob_piece_tid()))) {
      LOG_WARN("fail to record lob piece table id", KR(ret));
    }
  }
  if (OB_SUCC(ret) && OB_INVALID_ID != table_schema.get_aux_lob_meta_tid()
      && OB_FAIL(add_ddl_object_lock_by_id(
                 table_schema.get_aux_lob_meta_tid(), TABLE_SCHEMA, EXCLUSIVE))) {
    LOG_WARN("fail to register lob meta object lock", KR(ret));
  } else if (OB_SUCC(ret) && OB_INVALID_ID != table_schema.get_aux_lob_piece_tid()
      && OB_FAIL(add_ddl_object_lock_by_id(
                 table_schema.get_aux_lob_piece_tid(), TABLE_SCHEMA, EXCLUSIVE))) {
    LOG_WARN("fail to register lob piece object lock", KR(ret));
  }
  return ret;
}

int ObAlterTableAddColumnAction::register_dep_view_locks_(
    const ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  dep_objs_before_lock_.reset();
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

// Register main/auxiliary object locks together for global id ordering.
// Dependent views are locked because the
// serial alter_table_in_trans path invalidates dependent object status for
// column operations.
int ObAlterTableAddColumnAction::register_ddl_object_locks()
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
  } else if (OB_FAIL(register_aux_table_locks_(*table_schema))) {
    LOG_WARN("fail to register auxiliary table locks", KR(ret));
  } else if (OB_FAIL(register_dep_view_locks_(*table_schema))) {
    LOG_WARN("fail to register dep view locks", KR(ret));
  }
  return ret;
}

int ObAlterTableAddColumnAction::lock_table_and_online_ddl()
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = nullptr;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_ISNULL(table_schema = get_orig_table_schema())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("orig table schema is null", KR(ret));
  } else {
    // Use the same data version as the shared add-column admission check.
    const bool require_strict_binary_format =
        share::ObDDLUtil::use_idempotent_mode(get_arg().data_version_)
        && get_arg().need_progressive_merge();
    if (OB_FAIL(storage::ObDDLLock::lock_for_common_ddl_in_trans(
                                            *table_schema,
                                            require_strict_binary_format,
                                            get_trans()))) {
      LOG_WARN("fail to lock for common ddl in trans", KR(ret),
               "table_id", table_schema->get_table_id(),
               K(require_strict_binary_format));
      // lock_for_common_ddl_in_trans does NOT map retry ret codes internally.
      // Use the base-class helper so add / drop behaviour is symmetric.
      map_table_lock_retry_to_parallel_ddl_conflict_(ret);
    }
  }
  return ret;
}

int ObAlterTableAddColumnAction::collect_add_column_infos_()
{
  int ret = OB_SUCCESS;
  const ObTableSchema *orig_table_schema = get_orig_table_schema();
  if (OB_ISNULL(orig_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("orig table schema is null", KR(ret));
  } else if (OB_FAIL(orig_table_schema->check_if_oracle_compat_mode(is_oracle_mode_))) {
    LOG_WARN("fail to check oracle mode", KR(ret));
  } else {
    has_add_lob_column_ = false;
    const AlterTableSchema &alter_table_schema = get_arg().alter_table_schema_;
    ObSEArray<ObString, 4> add_names;
    ObTableSchema::const_column_iterator it = alter_table_schema.column_begin();
    ObTableSchema::const_column_iterator it_end = alter_table_schema.column_end();
    for (; OB_SUCC(ret) && it != it_end; ++it) {
      if (OB_ISNULL(*it)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column is null", KR(ret));
      } else {
        const AlterColumnSchema *alter_col = static_cast<const AlterColumnSchema *>(*it);
        if (OB_DDL_ADD_COLUMN != alter_col->alter_type_) {
          ret = OB_NOT_SUPPORTED_FOR_PARALLEL_DDL;
          LOG_WARN("unexpected column alter type for add column parallel DDL, fall back to serial",
                   KR(ret), "alter_type", alter_col->alter_type_);
        } else {
          // is_lob_storage covers TEXT/BLOB, JSON, GEOMETRY, Collection, and
          // RoaringBitmap — all types that require aux LOB tables.  Set the
          // flag so operate_schemas can skip create_aux_lob_table_if_need when
          // no such column is being added.
          if (is_lob_storage(alter_col->get_data_type())) {
            has_add_lob_column_ = true;
          }
          ObColumnNameHashWrapper key(alter_col->get_column_name_str());
          for (int64_t i = 0; OB_SUCC(ret) && i < add_names.count(); ++i) {
            if (ObColumnNameHashWrapper(add_names.at(i)) == key) {
              ret = OB_ERR_COLUMN_DUPLICATE;
              LOG_WARN("duplicate add column name in same statement", KR(ret),
                       "column", alter_col->get_column_name_str());
            }
          }
          if (FAILEDx(add_names.push_back(alter_col->get_column_name_str()))) {
            LOG_WARN("fail to push back add column name", KR(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObAlterTableAddColumnAction::check_dep_objs_consistency_()
{
  int ret = OB_SUCCESS;
  const ObTableSchema *orig_table_schema = get_orig_table_schema();
  dep_objs_.reset();
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

int ObAlterTableAddColumnAction::check_legitimacy()
{
  int ret = OB_SUCCESS;
  bool can_parallel_alter = false;
  const char *reason = nullptr;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(collect_add_column_infos_())) {
    LOG_WARN("fail to collect add column infos", KR(ret));
  } else if (OB_FAIL(check_dep_objs_consistency_())) {
    LOG_WARN("fail to check dep objs consistency", KR(ret));
  } else {
    const ObTableSchema *orig_table_schema = get_orig_table_schema();
    if (OB_ISNULL(orig_table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("orig table schema is null", KR(ret));
    } else if (OB_FAIL(share::schema::check_add_column_can_parallel(
                           get_arg(), *orig_table_schema,
                           can_parallel_alter, reason, &ddl_type_))) {
      LOG_WARN("fail to re-check add column parallel gate", KR(ret));
    } else if (!can_parallel_alter) {
      ret = OB_NOT_SUPPORTED_FOR_PARALLEL_DDL;
      LOG_WARN("add column is not supported in parallel at action layer", KR(ret),
               "reason", nullptr != reason ? reason : "unknown");
    }
  }
  return ret;
}

int ObAlterTableAddColumnAction::generate_schemas()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else {
    const ObTableSchema *orig_table_schema = get_orig_table_schema();
    if (OB_ISNULL(orig_table_schema) || OB_ISNULL(get_ddl_service())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("orig table schema or ddl service is null", KR(ret));
    } else if (OB_FAIL(init_local_schema_guard_for_default_value_())) {
      LOG_WARN("fail to init local schema guard for add-column default value", KR(ret));
    } else {
      curr_udt_set_id_ = 0;
      orig_table_has_lob_column_ = false;
      gen_col_expr_arr_.reset();
      if (OB_FAIL(get_ddl_service()->init_gen_col_exprs_by_prev_next_(
                  *orig_table_schema, gen_col_expr_arr_, &orig_table_has_lob_column_))) {
        LOG_WARN("fail to init existing generated column exprs", KR(ret));
      }
      const AlterTableSchema &alter_table_schema = get_arg().alter_table_schema_;
      ObTableSchema::const_column_iterator it = alter_table_schema.column_begin();
      ObTableSchema::const_column_iterator it_end = alter_table_schema.column_end();
      for (; OB_SUCC(ret) && it != it_end; ++it) {
        if (OB_ISNULL(*it)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column is null", KR(ret));
        } else {
          AlterColumnSchema *alter_col = static_cast<AlterColumnSchema *>(*it);
          if (OB_DDL_ADD_COLUMN != alter_col->alter_type_) {
            ret = OB_NOT_SUPPORTED_FOR_PARALLEL_DDL;
            LOG_WARN("unexpected column alter type for add column parallel DDL, fall back to serial",
                     KR(ret), "alter_type", alter_col->alter_type_);
          }
        }
      }
    }
  }
  return ret;
}

int ObAlterTableAddColumnAction::calc_main_column_schema_version_cnt_(int64_t &cnt) const
{
  int ret = OB_SUCCESS;
  cnt = 0;
  // Each ADD column consumes at most 2 schema versions:
  // 1 for insert_single_column + 1 for potential update_single_column (neighbor).
  const AlterTableSchema &alter_table_schema = get_arg().alter_table_schema_;
  ObTableSchema::const_column_iterator it = alter_table_schema.column_begin();
  ObTableSchema::const_column_iterator it_end = alter_table_schema.column_end();
  for (; OB_SUCC(ret) && it != it_end; ++it) {
    if (OB_ISNULL(*it)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column is null", KR(ret));
    } else {
      const AlterColumnSchema *alter_col = static_cast<const AlterColumnSchema *>(*it);
      if (OB_DDL_ADD_COLUMN != alter_col->alter_type_) {
        ret = OB_NOT_SUPPORTED_FOR_PARALLEL_DDL;
        LOG_WARN("unexpected column alter type for add column parallel DDL, fall back to serial",
                 KR(ret), "alter_type", alter_col->alter_type_);
      } else {
        cnt += 2;
      }
    }
  }
  return ret;
}

int ObAlterTableAddColumnAction::calc_column_group_added_column_cnt_(int64_t &cnt) const
{
  int ret = OB_SUCCESS;
  cnt = 0;
  const AlterTableSchema &alter_table_schema = get_arg().alter_table_schema_;
  ObTableSchema::const_column_iterator it = alter_table_schema.column_begin();
  ObTableSchema::const_column_iterator it_end = alter_table_schema.column_end();
  for (; OB_SUCC(ret) && it != it_end; ++it) {
    if (OB_ISNULL(*it)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column is null", KR(ret));
    } else {
      const AlterColumnSchema *alter_col = static_cast<const AlterColumnSchema *>(*it);
      if (OB_DDL_ADD_COLUMN == alter_col->alter_type_
          && !alter_col->is_virtual_generated_column()) {
        ++cnt;
      }
    }
  }
  return ret;
}

int ObAlterTableAddColumnAction::calc_column_group_schema_version_cnt_(int64_t &cnt) const
{
  int ret = OB_SUCCESS;
  cnt = 0;
  ObTableSchema &new_table_schema = const_cast<ObAlterTableAddColumnAction *>(this)
                                        ->get_new_table_schema();
  int64_t cg_added_column_cnt = 0;
  if (!new_table_schema.is_column_store_supported()) {
    // STANDARD / no-cg: 0
  } else if (OB_FAIL(calc_column_group_added_column_cnt_(cg_added_column_cnt))) {
    LOG_WARN("fail to calc column group added column cnt", KR(ret));
  } else if (0 == cg_added_column_cnt) {
    // Only virtual generated columns are added; column groups are untouched.
  } else {
    bool is_all_cg_exist = false;
    bool is_each_cg_exist = false;
    if (OB_FAIL(new_table_schema.is_column_group_exist(OB_ALL_COLUMN_GROUP_NAME, is_all_cg_exist))) {
      LOG_WARN("fail to check all cg exist", KR(ret));
    } else if (OB_FAIL(new_table_schema.is_column_group_exist(OB_EACH_COLUMN_GROUP_NAME, is_each_cg_exist))) {
      LOG_WARN("fail to check each cg exist", KR(ret));
    } else if (is_each_cg_exist) {
      // EACH: add_column_to_column_group calls insert_column_groups ONCE with a
      // tmp_table holding all new per-column groups (batch insert, one schema version),
      // plus one insert_column_ids_into_column_group call when all_cg also exists.
      cnt = 1;
      if (is_all_cg_exist) {
        cnt += 1;
      }
    } else if (is_all_cg_exist) {
      // ALL: single insert_column_ids_into_column_group call.
      cnt = 1;
    } else {
      // DEFAULT: single insert_column_ids_into_column_group call.
      cnt = 1;
    }
  }
  return ret;
}

int ObAlterTableAddColumnAction::calc_lob_aux_schema_version_cnt_(int64_t &cnt) const
{
  int ret = OB_SUCCESS;
  cnt = 0;
  const ObTableSchema *orig_table_schema = const_cast<ObAlterTableAddColumnAction *>(this)
                                               ->get_orig_table_schema();
  if (OB_ISNULL(orig_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("orig table schema is null", KR(ret));
  } else if (has_add_lob_column_ && !orig_table_has_lob_column_) {
    // Keep the counter aligned with create_aux_lob_table_if_need():
    // build_aux_lob_table_schema_if_need() may create either/both aux tables
    // depending on existing aux ids, not only the "both absent" case.
    if (OB_INVALID_ID == orig_table_schema->get_aux_lob_meta_tid()) {
      ++cnt; // create lob meta table
    }
    if (OB_INVALID_ID == orig_table_schema->get_aux_lob_piece_tid()) {
      ++cnt; // create lob piece table
    }
  }
  return ret;
}

int ObAlterTableAddColumnAction::calc_dep_obj_schema_version_cnt_()
{
  int ret = OB_SUCCESS;
  dep_obj_schema_version_cnt_ = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < dep_objs_.count(); ++i) {
    if (OB_INVALID_ID == dep_objs_.at(i).first) {
      continue;
    } else if (ObObjectType::VIEW == dep_objs_.at(i).second) {
      const ObTableSchema *view_schema = nullptr;
      if (OB_FAIL(get_schema_guard_wrapper().get_table_schema(dep_objs_.at(i).first, view_schema))) {
        LOG_WARN("fail to get view schema", KR(ret), "view_id", dep_objs_.at(i).first);
      } else if (OB_ISNULL(view_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("view schema is null under EXCLUSIVE lock, should not happen",
                 KR(ret), "view_id", dep_objs_.at(i).first);
      } else if (ObObjectStatus::INVALID != view_schema->get_object_status()) {
        ++dep_obj_schema_version_cnt_;
      }
    }
  }
  return ret;
}

int ObAlterTableAddColumnAction::calc_table_attribute_schema_version_cnt_(int64_t &cnt) const
{
  cnt = 2; // table options and the final operation carrying the user DDL
  return common::OB_SUCCESS;
}

int ObAlterTableAddColumnAction::calc_schema_version_cnt()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else {
    int64_t main_col = 0;
    int64_t col_group = 0;
    int64_t lob_aux = 0;
    int64_t tbl_attr = 0;
    if (OB_FAIL(calc_main_column_schema_version_cnt_(main_col))) {
      LOG_WARN("fail to calc main column schema version cnt", KR(ret));
    } else if (OB_FAIL(calc_column_group_schema_version_cnt_(col_group))) {
      LOG_WARN("fail to calc column group schema version cnt", KR(ret));
    } else if (OB_FAIL(calc_lob_aux_schema_version_cnt_(lob_aux))) {
      LOG_WARN("fail to calc lob aux schema version cnt", KR(ret));
    } else if (OB_FAIL(calc_dep_obj_schema_version_cnt_())) {
      LOG_WARN("fail to calc dep obj schema version cnt", KR(ret));
    } else if (OB_FAIL(calc_table_attribute_schema_version_cnt_(tbl_attr))) {
      LOG_WARN("fail to calc table attribute schema version cnt", KR(ret));
    } else {
      // tbl_attr covers both main-table updates in persist_added_columns_.
      // helper's calc_schema_version_cnt_ adds +1 for write_1503_ddl_operation.
      const int64_t schema_version_cnt =
          main_col + col_group + lob_aux + dep_obj_schema_version_cnt_ + tbl_attr
          + aux_table_ids_for_round_.count();
      add_schema_version_cnt(schema_version_cnt);
      LOG_INFO("calc add column schema version cnt",
               K(main_col), K(col_group), K(lob_aux),
               "aux_round_cnt", aux_table_ids_for_round_.count(),
               K_(dep_obj_schema_version_cnt), K(tbl_attr), K(schema_version_cnt));
    }
  }
  return ret;
}

int ObAlterTableAddColumnAction::persist_added_columns_(ObDDLOperator &ddl_operator)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *orig_table_schema = get_orig_table_schema();
  if (OB_ISNULL(orig_table_schema) || OB_ISNULL(get_ddl_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("orig table schema or ddl service is null", KR(ret));
  } else {
    ObTableSchema &new_table_schema = get_new_table_schema();
    AlterTableSchema &alter_table_schema = const_cast<AlterTableSchema &>(get_arg().alter_table_schema_);
    obrpc::ObSequenceDDLArg &sequence_ddl_arg = const_cast<obrpc::ObSequenceDDLArg &>(get_arg().sequence_ddl_arg_);
    common::ObIAllocator &arg_allocator = const_cast<ObAlterTableArg &>(get_arg()).allocator_;
    bool is_add_lob = false;
    ObTableSchema::const_column_iterator it = alter_table_schema.column_begin();
    ObTableSchema::const_column_iterator it_end = alter_table_schema.column_end();
    for (; OB_SUCC(ret) && it != it_end; ++it) {
      AlterColumnSchema *alter_col = static_cast<AlterColumnSchema *>(*it);
      if (OB_ISNULL(alter_col)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("alter column is null", KR(ret));
      } else if (OB_DDL_ADD_COLUMN != alter_col->alter_type_) {
        ret = OB_NOT_SUPPORTED_FOR_PARALLEL_DDL;
        LOG_WARN("unexpected column alter type for add column parallel DDL, fall back to serial",
                 KR(ret), "alter_type", alter_col->alter_type_);
      } else if (OB_FAIL(get_ddl_service()->add_new_column_to_table_schema(
                                            *orig_table_schema,
                                            alter_table_schema,
                                            get_arg().tz_info_wrap_,
                                            get_arg().nls_formats_[0],
                                            const_cast<sql::ObLocalSessionVar &>(get_arg().local_session_var_),
                                            sequence_ddl_arg,
                                            arg_allocator,
                                            new_table_schema,
                                            *alter_col,
                                            gen_col_expr_arr_,
                                            local_schema_guard_,
                                            curr_udt_set_id_,
                                            &ddl_operator,
                                            &get_trans(),
                                            &local_schema_guard_,
                                            &get_schema_guard_wrapper(),
                                            true /*record_wasted_version*/))) {
        LOG_WARN("fail to add new column to table schema", KR(ret),
                 "column", alter_col->get_column_name_str());
      }
    }
    if (FAILEDx(get_ddl_service()->add_column_to_column_group(
                                   *orig_table_schema, alter_table_schema, new_table_schema,
                                   ddl_operator, get_trans()))) {
      LOG_WARN("fail to add column to column group", KR(ret));
    } else if (OB_FAIL(new_table_schema.check_skip_index_valid())) {
      LOG_WARN("fail to check new table schema skip index valid", KR(ret));
    } else if (OB_FAIL(new_table_schema.check_row_length(is_oracle_mode_))) {
      LOG_WARN("fail to check new table schema row length", KR(ret));
    } else if (has_add_lob_column_ && !orig_table_has_lob_column_
               && OB_FAIL(get_ddl_service()->create_aux_lob_table_if_need(
                                             new_table_schema,
                                             get_schema_guard_wrapper(),
                                             ddl_operator,
                                             get_trans(),
                                             false /*need_sync_schema_version*/,
                                             is_add_lob))) {
      LOG_WARN("fail to create aux lob table if need", KR(ret));
    // Preserve the serial ALTER table-options operation before updating auxiliary tables.
    } else if (OB_FAIL(ddl_operator.update_table_attribute(new_table_schema,
                                                           get_trans(),
                                                           OB_DDL_ALTER_TABLE))) {
      LOG_WARN("fail to persist add column table options", KR(ret));
    } else if (OB_FAIL(sync_aux_table_progressive_merge_round_(ddl_operator))) {
      LOG_WARN("fail to sync auxiliary table progressive merge round", KR(ret));
    } else if (OB_FAIL(ddl_operator.update_table_attribute(new_table_schema,
                                                           get_trans(),
                                                           OB_DDL_ALTER_TABLE,
                                                           &get_arg().ddl_stmt_str_))) {
      LOG_WARN("fail to update table attribute", KR(ret));
    }
  }
  return ret;
}

int ObAlterTableAddColumnAction::sync_aux_table_progressive_merge_round_(ObDDLOperator &ddl_operator)
{
  int ret = OB_SUCCESS;
  const int64_t new_round = get_new_table_schema().get_progressive_merge_round();
  HEAP_VAR(ObTableSchema, new_aux_table_schema) {
    for (int64_t i = 0; OB_SUCC(ret) && i < aux_table_ids_for_round_.count(); ++i) {
      const uint64_t table_id = aux_table_ids_for_round_.at(i);
      const ObTableSchema *aux_table_schema = nullptr;
      if (OB_FAIL(get_schema_guard_wrapper().get_table_schema(table_id, aux_table_schema))) {
        LOG_WARN("fail to get locked auxiliary table schema", KR(ret), K(table_id));
      } else if (OB_ISNULL(aux_table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("locked auxiliary table schema is null", KR(ret), K(table_id));
      } else {
        new_aux_table_schema.reset();
        if (OB_FAIL(new_aux_table_schema.assign(*aux_table_schema))) {
          LOG_WARN("fail to copy auxiliary table schema", KR(ret), K(table_id));
        } else {
          new_aux_table_schema.set_progressive_merge_round(
              std::max(new_round, aux_table_schema->get_progressive_merge_round()));
          if (OB_FAIL(ddl_operator.update_table_attribute(
                        new_aux_table_schema, get_trans(), OB_DDL_ALTER_TABLE))) {
            LOG_WARN("fail to update auxiliary table round", KR(ret), K(table_id), K(new_round));
          }
        }
      }
    }
  } // end HEAP_VAR
  return ret;
}

int ObAlterTableAddColumnAction::modify_dep_obj_status_(ObDDLOperator &ddl_operator)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *orig_table_schema = get_orig_table_schema();
  if (OB_ISNULL(orig_table_schema) || OB_ISNULL(get_ddl_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("orig table schema or ddl service is null", KR(ret),
             KP(orig_table_schema), KP(get_ddl_service()));
  } else if (OB_FAIL(get_ddl_service()->modify_dep_obj_status_for_alter_table(
                     get_arg(), *orig_table_schema, ddl_operator, get_trans()))) {
    LOG_WARN("fail to modify dep obj status", KR(ret));
  }
  return ret;
}

int ObAlterTableAddColumnAction::adjust_result()
{
  int ret = OB_SUCCESS;
  if (DDL_INVALID == ddl_type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid ddl type for add column action", KR(ret));
  } else {
    get_res().ddl_type_ = ddl_type_;
  }
  return ret;
}

int ObAlterTableAddColumnAction::operate_schemas()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_ISNULL(get_ddl_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ddl service is null", KR(ret));
  } else {
    // Same CompatModeGuard pattern as drop_column_action.cpp:1040 — keeps
    // Oracle case-sensitive name comparison consistent with the serial path.
    lib::CompatModeGuard compat_mode_guard(is_oracle_mode_
                            ? lib::Worker::CompatMode::ORACLE
                            : lib::Worker::CompatMode::MYSQL);
    ObDDLOperator ddl_operator(get_ddl_service()->get_schema_service(),
                                get_ddl_service()->get_sql_proxy());
    if (OB_FAIL(modify_dep_obj_status_(ddl_operator))) {
      LOG_WARN("fail to modify dep obj status", KR(ret));
    } else if (OB_FAIL(persist_added_columns_(ddl_operator))) {
      LOG_WARN("fail to persist added columns", KR(ret));
    }
  }
  return ret;
}
