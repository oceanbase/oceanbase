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
  } else if (OB_ISNULL(get_ddl_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ddl service is null", KR(ret));
  } else if (OB_FAIL(get_ddl_service()->get_tenant_schema_guard_with_version_in_inner_table(
                 get_tenant_id(), local_schema_guard_))) {
    LOG_WARN("fail to get tenant schema guard with version in inner table",
             KR(ret), "tenant_id", get_tenant_id());
  } else {
    local_schema_guard_.set_session_id(get_arg().session_id_);
  }
  if (OB_SUCC(ret)) {
    const ObTableSchema *local_table_schema = nullptr;
    if (OB_FAIL(local_schema_guard_.get_table_schema(
                   get_tenant_id(), orig_table_schema->get_table_id(), local_table_schema))) {
      LOG_WARN("fail to get local table schema", KR(ret),
               "tenant_id", get_tenant_id(), "table_id", orig_table_schema->get_table_id());
    } else if (OB_ISNULL(local_table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("local table schema not exist", KR(ret),
               "tenant_id", get_tenant_id(), "table_id", orig_table_schema->get_table_id());
    } else if (local_table_schema->get_schema_version() != orig_table_schema->get_schema_version()) {
      ret = OB_NOT_SUPPORTED_FOR_PARALLEL_DDL;
      LOG_WARN("schema snapshot mismatch between latest guard and local guard, fallback to serial ddl",
               KR(ret),
               "tenant_id", get_tenant_id(),
               "table_id", orig_table_schema->get_table_id(),
               "latest_schema_version", orig_table_schema->get_schema_version(),
               "local_schema_version", local_table_schema->get_schema_version());
    }
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

// Only register LOB aux locks when the original table already has aux tables.
// Aux tables created for the first new LOB column are allocated and persisted
// inside operate_schemas_, so they need no prior lock registration.
int ObAlterTableAddColumnAction::register_lob_aux_locks_(const ObTableSchema &table_schema)
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

// Object lock registration. Add column only touches the main table and the
// LOB aux pair (when pre-existing). Dependent views are locked because the
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
  } else if (OB_FAIL(register_lob_aux_locks_(*table_schema))) {
    LOG_WARN("fail to register lob aux locks", KR(ret));
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
    // Use arg.data_version_ (set by executor pre-RPC) to match the version used
    // throughout other checks (e.g. check_add_column_types_), keeping the two
    // callers consistent without an extra GET_MIN_DATA_VERSION round-trip.
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

int ObAlterTableAddColumnAction::check_add_column_types_()
{
  int ret = OB_SUCCESS;
  add_col_ddl_types_.reset();
  const ObTableSchema *orig_table_schema = get_orig_table_schema();
  const AlterTableSchema &alter_table_schema = get_arg().alter_table_schema_;
  const uint64_t tenant_data_version = get_arg().data_version_;
  if (OB_ISNULL(orig_table_schema) || OB_ISNULL(get_ddl_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("orig table schema or ddl service is null", KR(ret),
             KP(orig_table_schema), KP(get_ddl_service()));
  } else {
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
          ObDDLType tmp_ddl_type = ObDDLType::DDL_INVALID;
          if (OB_FAIL(share::ObAlterColumnDDLHelper::compute_add_column_ddl_type(
                                         alter_table_schema, *orig_table_schema, *alter_col,
                                         static_cast<int64_t>(get_arg().alter_algorithm_), is_oracle_mode_,
                                         tenant_data_version, tmp_ddl_type))) {
            // Oracle mode + INSTANT algorithm causes check_can_add_column_use_instant_ to
            // return OB_NOT_SUPPORTED; remap so the parallel path falls back to serial DDL
            // instead of surfacing an error to the user.
            if (OB_NOT_SUPPORTED == ret) {
              ret = OB_NOT_SUPPORTED_FOR_PARALLEL_DDL;
            }
            LOG_WARN("fail to check is add column online, fall back to serial", KR(ret));
          } else if (ObDDLType::DDL_ADD_COLUMN_ONLINE != tmp_ddl_type
                     && ObDDLType::DDL_ADD_COLUMN_INSTANT != tmp_ddl_type) {
            // DDL_TABLE_REDEFINITION / DDL_ADD_COLUMN_OFFLINE not supported in parallel
            ret = OB_NOT_SUPPORTED_FOR_PARALLEL_DDL;
            LOG_WARN("add column type not supported in parallel", KR(ret),
                     "ddl_type", tmp_ddl_type, "column", alter_col->get_column_name_str());
          } else if (OB_FAIL(add_col_ddl_types_.push_back(tmp_ddl_type))) {
            LOG_WARN("fail to push back ddl type", KR(ret));
          }
        }
      }
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
    ObArray<ObString> add_names;
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
          if (OB_SUCC(ret) && OB_FAIL(add_names.push_back(alter_col->get_column_name_str()))) {
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
  } else if (OB_FAIL(check_add_column_types_())) {
    LOG_WARN("fail to check add column types", KR(ret));
  } else {
    const ObTableSchema *orig_table_schema = get_orig_table_schema();
    if (OB_ISNULL(orig_table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("orig table schema is null", KR(ret));
    // Inner / system table rejected from parallel path. Mirrors the schema
    // utils gate; defensive check in case any path bypasses the executor pre-check.
    } else if (is_inner_table(orig_table_schema->get_table_id())) {
      ret = OB_NOT_SUPPORTED_FOR_PARALLEL_DDL;
      LOG_WARN("inner table add column not supported in parallel", KR(ret),
               "table_id", orig_table_schema->get_table_id());
    } else if (OB_FAIL(share::schema::check_add_column_can_parallel(
                           get_arg(), *orig_table_schema,
                           can_parallel_alter, reason))) {
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
  cnt = 1;
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
      // tbl_attr for update_table_attribute called in persist_added_columns_.
      // helper's calc_schema_version_cnt_ adds +1 for write_1503_ddl_operation.
      const int64_t schema_version_cnt =
          main_col + col_group + lob_aux + dep_obj_schema_version_cnt_ + tbl_attr;
      add_schema_version_cnt(schema_version_cnt);
      LOG_INFO("calc add column schema version cnt",
               K(main_col), K(col_group), K(lob_aux),
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
    } else if (OB_FAIL(ddl_operator.update_table_attribute(new_table_schema,
                                                           get_trans(),
                                                           OB_DDL_ALTER_TABLE,
                                                           &get_arg().ddl_stmt_str_))) {
      LOG_WARN("fail to update table attribute", KR(ret));
    }
  }
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

ObDDLType ObAlterTableAddColumnAction::get_result_ddl_type_() const
{
  ObDDLType result = ObDDLType::DDL_INVALID;
  for (int64_t i = 0; i < add_col_ddl_types_.count(); ++i) {
    const ObDDLType t = add_col_ddl_types_.at(i);
    if (ObDDLType::DDL_ADD_COLUMN_INSTANT == t) {
      result = ObDDLType::DDL_ADD_COLUMN_INSTANT;
      break;
    } else if (ObDDLType::DDL_ADD_COLUMN_ONLINE == t
               && ObDDLType::DDL_INVALID == result) {
      result = ObDDLType::DDL_ADD_COLUMN_ONLINE;
    }
  }
  return result;
}

int ObAlterTableAddColumnAction::adjust_result()
{
  int ret = OB_SUCCESS;
  const ObDDLType ddl_type = get_result_ddl_type_();
  if (DDL_INVALID == ddl_type) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid ddl type for add column action", KR(ret));
  } else {
    get_res().ddl_type_ = ddl_type;
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
