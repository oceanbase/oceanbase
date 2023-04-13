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

#define USING_LOG_PREFIX SHARE

#include "ob_ddl_common.h"
#include "common/ob_smart_call.h"
#include "share/ob_autoincrement_service.h"
#include "share/ob_ddl_checksum.h"
#include "share/ob_get_compat_mode.h"
#include "share/ob_rpc_struct.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_part_mgr_util.h"
#include "share/location_cache/ob_location_service.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/table/ob_table_scan_op.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "storage/tx_storage/ob_ls_map.h"
#include "rootserver/ob_root_service.h"
#include "rootserver/ddl_task/ob_ddl_task.h"

using namespace oceanbase::share;
using namespace oceanbase::common;
using namespace oceanbase::share::schema;
using namespace oceanbase::obrpc;
using namespace oceanbase::sql;

int ObColumnNameMap::init(const ObTableSchema &orig_table_schema,
                          const AlterTableSchema &alter_table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObCompatModeGetter::get_table_compat_mode(orig_table_schema.get_tenant_id(), orig_table_schema.get_table_id(), compat_mode_))) {
    LOG_WARN("failed to get table compat mode", K(ret));
  } else if (OB_FAIL(col_name_map_.create(32, "ColNameMap"))) {
    LOG_WARN("failed to create column name map", K(ret));
  } else {
    lib::CompatModeGuard guard(compat_mode_);
    for (ObTableSchema::const_column_iterator it = orig_table_schema.column_begin();
        OB_SUCC(ret) && it != orig_table_schema.column_end(); it++) {
      ObColumnSchemaV2 *column = *it;
      if (OB_ISNULL(column)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid column", K(ret));
      } else if (OB_FAIL(set(column->get_column_name_str(), column->get_column_name_str()))) {
        LOG_WARN("failed to set colum name map", K(ret));
      }
    }
    for (ObTableSchema::const_column_iterator it = alter_table_schema.column_begin();
        OB_SUCC(ret) && it < alter_table_schema.column_end(); it++) {
      const AlterColumnSchema *alter_column_schema = nullptr;
      if (OB_ISNULL(alter_column_schema = static_cast<AlterColumnSchema *>(*it))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("*it_begin is NULL", K(ret));
      } else {
        const ObString &orig_column_name = alter_column_schema->get_origin_column_name();
        const ObString &new_column_name = alter_column_schema->get_column_name_str();
        const ObColumnNameHashWrapper orig_column_key(orig_column_name);
        const ObSchemaOperationType op_type = alter_column_schema->alter_type_;
        switch (op_type) {
        case OB_DDL_DROP_COLUMN: {
          // can only drop original table columns
          if (OB_FAIL(col_name_map_.erase_refactored(orig_column_key))) {
            LOG_WARN("failed to erase from col name map", K(ret));
          }
          break;
        }
        case OB_DDL_ADD_COLUMN: {
          break;
        }
        case OB_DDL_CHANGE_COLUMN:
        case OB_DDL_MODIFY_COLUMN: {
          const ObColumnSchemaV2 *orig_column = nullptr;
          if (OB_ISNULL(orig_column = orig_table_schema.get_column_schema(orig_column_name))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("column not in orig table", K(ret));
          } else if (orig_column->get_column_name_str() != new_column_name) {
            const ObString &column_name = orig_column->get_column_name_str();
            if (OB_FAIL(col_name_map_.erase_refactored(ObColumnNameHashWrapper(column_name)))) {
              LOG_WARN("failed to erase col name map", K(ret));
            } else if (OB_FAIL(set(column_name, new_column_name))) {
              LOG_WARN("failed to set col name map", K(ret));
            }
          }
          break;
        }
        default: {
          LOG_DEBUG("ignore unexpected operator", K(ret), KPC(alter_column_schema));
          break;
        }
        }
      }
    }
  }
  return ret;
}

int ObColumnNameMap::assign(const ObColumnNameMap &other)
{
  int ret = OB_SUCCESS;
  if (!other.col_name_map_.created()) {
    ret = OB_NOT_INIT;
    LOG_WARN("assign from uninitialized name map", K(ret));
  } else if (!col_name_map_.created()) {
    if (OB_FAIL(col_name_map_.create(32, "ColNameMap"))) {
      LOG_WARN("failed to create col name map", K(ret));
    }
  } else if (OB_FAIL(col_name_map_.reuse())) {
    LOG_WARN("failed to clear map", K(ret));
  }
  if (OB_SUCC(ret)) {
    allocator_.reuse();
    compat_mode_ = other.compat_mode_;
    for (common::hash::ObHashMap<ObColumnNameHashWrapper, ObString>::const_iterator it = other.col_name_map_.begin();
        OB_SUCC(ret) && it != other.col_name_map_.end(); it++) {
      if (OB_FAIL(set(it->first.column_name_, it->second))) {
        LOG_WARN("failed to copy col name map entry", K(ret));
      }
    }
  }
  return ret;
}

int ObColumnNameMap::set(const ObString &orig_column_name, const ObString &new_column_name)
{
  int ret = OB_SUCCESS;
  ObString orig_name;
  ObString new_name;
  lib::CompatModeGuard guard(compat_mode_);
  if (OB_FAIL(deep_copy_ob_string(allocator_, orig_column_name, orig_name))) {
    LOG_WARN("failed to copy string", K(ret));
  } else if (OB_FAIL(deep_copy_ob_string(allocator_, new_column_name, new_name))) {
    LOG_WARN("failed to copy string", K(ret));
  } else if (OB_FAIL(col_name_map_.set_refactored(ObColumnNameHashWrapper(orig_name), new_name))) {
    LOG_WARN("failed to set col name map", K(ret));
  }
  return ret;
}

int ObColumnNameMap::get(const ObString &orig_column_name, ObString &new_column_name) const
{
  int ret = OB_SUCCESS;
  lib::CompatModeGuard guard(compat_mode_);
  ret = col_name_map_.get_refactored(ObColumnNameHashWrapper(orig_column_name), new_column_name);
  if (OB_HASH_NOT_EXIST == ret) {
    ret = OB_ENTRY_NOT_EXIST;
  }
  return ret;
}

int ObColumnNameMap::get_orig_column_name(const ObString &new_column_name, ObString &orig_column_name) const
{
  int ret = OB_SUCCESS;
  lib::CompatModeGuard guard(compat_mode_);
  if (OB_UNLIKELY(!col_name_map_.created())) {
    ret = OB_NOT_INIT;
    LOG_WARN("invalid column name map", K(ret));
  } else {
    const ObColumnNameHashWrapper new_column_key = ObColumnNameHashWrapper(new_column_name);
    bool found = false;
    for (common::hash::ObHashMap<ObColumnNameHashWrapper, ObString>::const_iterator it = col_name_map_.begin();
        OB_SUCC(ret) && !found && it != col_name_map_.end(); it++) {
      if (ObColumnNameHashWrapper(it->second) == new_column_key) {
        orig_column_name = it->first.column_name_;
        found = true;
      }
    }
    if (OB_SUCC(ret) && !found) {
      ret = OB_ENTRY_NOT_EXIST;
    }
  }
  return ret;
}

int ObColumnNameMap::get_changed_names(ObIArray<std::pair<ObString, ObString>> &changed_names) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!col_name_map_.created())) {
    ret = OB_NOT_INIT;
    LOG_WARN("invalid column name map", K(ret));
  }
  for (common::hash::ObHashMap<ObColumnNameHashWrapper, ObString>::const_iterator it = col_name_map_.begin();
      OB_SUCC(ret) && it != col_name_map_.end(); it++) {
    if (it->first.column_name_ != it->second) {
      if (OB_FAIL(changed_names.push_back(std::make_pair(it->first.column_name_, it->second)))) {
        LOG_WARN("failed to push back changed name", K(ret));
      }
    }
  }
  return ret;
}

int64_t ObColumnNameMap::to_string(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(compat_mode));
  J_COMMA();
  J_NAME("col_name_map");
  J_COLON();
  J_OBJ_START();
  common::hash::ObHashMap<ObColumnNameHashWrapper, ObString>::const_iterator it = col_name_map_.begin();
  for (; it != col_name_map_.end(); it++) {
    BUF_PRINTO(it->first.column_name_);
    J_COLON();
    BUF_PRINTO(it->second);
    J_COMMA();
  }
  J_OBJ_END();
  J_OBJ_END();
  return pos;
}

/******************           ObDDLUtil         *************/

int ObDDLUtil::get_tablets(
    const uint64_t tenant_id,
    const int64_t table_id,
    common::ObIArray<common::ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  share::schema::ObSchemaGetterGuard schema_guard;
  const share::schema::ObTableSchema *table_schema = nullptr;
  if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(table_id));
  } else if (OB_FAIL(share::schema::ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(
      tenant_id, schema_guard))) {
    LOG_WARN("get tenant schema guard failed", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id, table_schema))) {
    LOG_WARN("get table schema failed", K(ret), K(tenant_id), K(table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("get table schema failed", K(ret), K(table_id));
  } else if (OB_FAIL(table_schema->get_tablet_ids(tablet_ids))) {
    LOG_WARN("get tablets failed", K(ret), K(*table_schema));
  }
  return ret;
}

int ObDDLUtil::refresh_alter_table_arg(
    const uint64_t tenant_id,
    const int64_t orig_table_id,
    obrpc::ObAlterTableArg &alter_table_arg)
{
  int ret = OB_SUCCESS;
  share::schema::ObSchemaGetterGuard schema_guard;
  const share::schema::ObTableSchema *table_schema = nullptr;
  const ObDatabaseSchema *db_schema = nullptr;
  if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == orig_table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(orig_table_id));
  } else if (OB_FAIL(share::schema::ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(
      tenant_id, schema_guard))) {
    LOG_WARN("get tenant schema guard failed", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, orig_table_id, table_schema))) {
    LOG_WARN("get table schema failed", K(ret), K(tenant_id), K(orig_table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table dropped", K(ret), K(tenant_id), K(orig_table_id));
  } else if (OB_FAIL(alter_table_arg.alter_table_schema_.set_origin_table_name(table_schema->get_table_name_str()))) {
    LOG_WARN("failed to set orig table name", K(ret));
  } else if (OB_FAIL(schema_guard.get_database_schema(tenant_id, table_schema->get_database_id(), db_schema))) {
    LOG_WARN("fail to get database schema", K(ret), K(tenant_id));
  } else if (OB_ISNULL(db_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("database dropped", K(ret), K(tenant_id), K(table_schema->get_database_id()));
  } else if (OB_FAIL(alter_table_arg.alter_table_schema_.set_origin_database_name(db_schema->get_database_name_str()))) {
    LOG_WARN("failed to set orig database name", K(ret));
  }

  // refresh constraint
  for (ObTableSchema::const_constraint_iterator it = alter_table_arg.alter_table_schema_.constraint_begin();
       OB_SUCC(ret) && it != alter_table_arg.alter_table_schema_.constraint_end(); it++) {
    ObConstraint *cst = (*it);
    const ObConstraint *cur_cst = nullptr;
    if (OB_ISNULL(cst)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid cst", K(ret));
    } else if (OB_ISNULL(cur_cst = table_schema->get_constraint(cst->get_constraint_id()))) {
      LOG_INFO("current constraint not exists, maybe dropped", K(ret), KPC(cst), K(table_schema));
    } else if (OB_FAIL(cst->set_constraint_name(cur_cst->get_constraint_name_str()))) {
      LOG_WARN("failed to set new constraint name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(alter_table_arg.based_schema_object_infos_.push_back(ObBasedSchemaObjectInfo(
        table_schema->get_table_id(),
        TABLE_SCHEMA,
        table_schema->get_schema_version())))) {
      LOG_WARN("failed to push back base schema object info", K(ret));
    } else if (OB_FAIL(alter_table_arg.based_schema_object_infos_.push_back(ObBasedSchemaObjectInfo(
        db_schema->get_database_id(),
        DATABASE_SCHEMA,
        db_schema->get_schema_version())))) {
      LOG_WARN("failed to push back base schema object info", K(ret));
    }
  }
  return ret;
}

int ObDDLUtil::generate_column_name_str(
    const common::ObIArray<ObColumnNameInfo> &column_names,
    const bool is_oracle_mode,
    const bool with_origin_name,
    const bool with_alias_name,
    const bool use_heap_table_ddl_plan,
    ObSqlString &sql_string)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(column_names.count() == 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(column_names.count()));
  } else {
    bool with_comma = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < column_names.count(); ++i) {
      if (use_heap_table_ddl_plan && column_names.at(i).column_name_ == OB_HIDDEN_PK_INCREMENT_COLUMN_NAME) {
      } else if (OB_FAIL(generate_column_name_str(column_names.at(i), is_oracle_mode, with_origin_name, with_alias_name, with_comma, sql_string))) {
        LOG_WARN("generate column name string failed", K(ret));
      } else {
        with_comma = true;
      }
    }
  }
  return ret;
}

int ObDDLUtil::generate_order_by_str(
    const ObIArray<int64_t> &select_column_ids,
    const ObIArray<int64_t> &order_column_ids,
    ObSqlString &sql_string)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(select_column_ids.count() <= 0
        || order_column_ids.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(select_column_ids), K(order_column_ids));
  } else if (OB_FAIL(sql_string.append("order by "))) {
    LOG_WARN("append failed", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < order_column_ids.count(); ++i) {
      for (int64_t j = 0; OB_SUCC(ret) && j < select_column_ids.count(); ++j) {
        const bool append_comma = 0 != i;
        if (select_column_ids.at(j) == order_column_ids.at(i)) {
          if (OB_FAIL(sql_string.append_fmt("%s %ld", append_comma ? ",": "", j + 1))) {
            LOG_WARN("append fmt failed", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObDDLUtil::generate_column_name_str(
    const ObColumnNameInfo &column_name_info,
    const bool is_oracle_mode,
    const bool with_origin_name,
    const bool with_alias_name,
    const bool with_comma,
    ObSqlString &sql_string)
{
  int ret = OB_SUCCESS;
  const char *split_char = is_oracle_mode ? "\"" : "`";
  // append comma
  if (with_comma) {
    if (OB_FAIL(sql_string.append_fmt(", "))) {
      LOG_WARN("append fmt failed", K(ret));
    }
  }
  // append original column name
  if (OB_SUCC(ret) && with_origin_name) {
    if (OB_FAIL(sql_string.append_fmt("%s%.*s%s", split_char, column_name_info.column_name_.length(), column_name_info.column_name_.ptr(), split_char))) {
      LOG_WARN("append origin column name failed", K(ret));
    }
  }
  // append AS
  if (OB_SUCC(ret) && with_origin_name && with_alias_name) {
    if (OB_FAIL(sql_string.append_fmt(" AS "))) {
      LOG_WARN("append as failed", K(ret));
    }
  }
  // append alias column name
  if (OB_SUCC(ret) && with_alias_name) {
    if (OB_FAIL(sql_string.append_fmt("%s%s%.*s%s", split_char, column_name_info.is_shadow_column_ ? "__SHADOW_" : "",
        column_name_info.column_name_.length(), column_name_info.column_name_.ptr(), split_char))) {
      LOG_WARN("append alias name failed", K(ret));
    }
  }
  return ret;
}

int ObDDLUtil::generate_ddl_schema_hint_str(
    const ObString &table_name,
    const int64_t schema_version,
    const bool is_oracle_mode,
    ObSqlString &sql_string)
{
  int ret = OB_SUCCESS;
  if (is_oracle_mode) {
    if (OB_FAIL(sql_string.append_fmt("ob_ddl_schema_version(\"%.*s\", %ld)",
        static_cast<int>(table_name.length()), table_name.ptr(), schema_version))) {
      LOG_WARN("append origin column name failed", K(ret));
    }
  } else {
    if (OB_FAIL(sql_string.append_fmt("ob_ddl_schema_version(`%.*s`, %ld)",
        static_cast<int>(table_name.length()), table_name.ptr(), schema_version))) {
      LOG_WARN("append origin column name failed", K(ret));
    }
  }
  return ret;
}

int ObDDLUtil::generate_build_replica_sql(
    const uint64_t tenant_id,
    const int64_t data_table_id,
    const int64_t dest_table_id,
    const int64_t schema_version,
    const int64_t snapshot_version,
    const int64_t execution_id,
    const int64_t task_id,
    const int64_t parallelism,
    const bool use_heap_table_ddl_plan,
    const bool use_schema_version_hint_for_src_table,
    const ObColumnNameMap *col_name_map,
    ObSqlString &sql_string)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *source_table_schema = nullptr;
  const ObTableSchema *dest_table_schema = nullptr;
  bool oracle_mode = false;
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || OB_INVALID_ID == data_table_id || OB_INVALID_ID == dest_table_id
      || schema_version <= 0 || snapshot_version <= 0 || execution_id <= 0 || task_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(data_table_id), K(dest_table_id), K(schema_version),
                                  K(snapshot_version), K(execution_id), K(task_id));
  } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(
      tenant_id, schema_guard, schema_version))) {
    LOG_WARN("fail to get tenant schema guard", K(ret), K(data_table_id));
  } else if (OB_FAIL(schema_guard.check_formal_guard())) {
    LOG_WARN("fail to check formal guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, data_table_id, source_table_schema))) {
    LOG_WARN("fail to get table schema", K(ret), K(tenant_id), K(data_table_id));
  } else if (OB_ISNULL(source_table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("fail to get table schema", K(ret), K(data_table_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, dest_table_id, dest_table_schema))) {
    LOG_WARN("fail to get table schema", K(ret), K(tenant_id), K(dest_table_id));
  } else if (OB_ISNULL(dest_table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("fail to get table schema", K(ret), K(dest_table_id));
  } else if (OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_table_id(tenant_id, data_table_id, oracle_mode))) {
    LOG_WARN("check if oracle mode failed", K(ret), K(data_table_id));
  } else {
    ObArray<ObColDesc> column_ids;
    ObArray<ObColumnNameInfo> column_names;
    ObArray<ObColumnNameInfo> insert_column_names;
    ObArray<ObColumnNameInfo> rowkey_column_names;
    ObArray<int64_t> select_column_ids;
    ObArray<int64_t> order_column_ids;
    bool is_shadow_column = false;
    int64_t real_parallelism = std::max(1L, parallelism);
    real_parallelism = std::min(ObMacroDataSeq::MAX_PARALLEL_IDX + 1, real_parallelism);
    // get dest table column names
    if (OB_FAIL(dest_table_schema->get_column_ids(column_ids))) {
      LOG_WARN("fail to get column ids", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < column_ids.count(); ++i) {
        const ObColumnSchemaV2 *column_schema = nullptr;
        ObString orig_column_name;
        is_shadow_column = column_ids.at(i).col_id_ >= OB_MIN_SHADOW_COLUMN_ID;
        const int64_t col_id = is_shadow_column ? column_ids.at(i).col_id_ - OB_MIN_SHADOW_COLUMN_ID : column_ids.at(i).col_id_;
        if (OB_ISNULL(column_schema = dest_table_schema->get_column_schema(col_id))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("error unexpected, column schema must not be nullptr", K(ret));
        } else if (is_shadow_column) {
          // do nothing
        } else if (column_schema->is_generated_column()) {
          // cannot insert to generated columns.
        } else if (nullptr == col_name_map && OB_FALSE_IT(orig_column_name.assign_ptr(column_schema->get_column_name_str().ptr(), column_schema->get_column_name_str().length()))) {
        } else if (nullptr != col_name_map && OB_FAIL(col_name_map->get_orig_column_name(column_schema->get_column_name_str(), orig_column_name))) {
          if (OB_ENTRY_NOT_EXIST == ret) {
            // newly added column cannot be selected from source table.
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("failed to get orig column name", K(ret));
          }
        } else if (OB_FAIL(column_names.push_back(ObColumnNameInfo(orig_column_name, is_shadow_column)))) {
          LOG_WARN("fail to push back column name", K(ret));
        } else if (OB_FAIL(select_column_ids.push_back(col_id))) {
          LOG_WARN("push back select column id failed", K(ret), K(col_id));
        } else if (!is_shadow_column) {
          if (OB_FAIL(insert_column_names.push_back(ObColumnNameInfo(column_schema->get_column_name_str(), is_shadow_column)))) {
            LOG_WARN("push back insert column name failed", K(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret) && source_table_schema->is_heap_table() && dest_table_schema->is_index_local_storage()) {
      ObArray<ObColDesc> src_column_ids;
      ObSEArray<uint64_t, 5> extra_column_ids;
      if (OB_FAIL(source_table_schema->get_column_ids(src_column_ids))) {
        LOG_WARN("fail to get column ids", K(ret));
      } else {
        // Add part keys and their cascaded columns first
        for (int64_t i = 0; OB_SUCC(ret) && i < src_column_ids.count(); ++i) {
          const ObColumnSchemaV2 *column_schema = nullptr;
          const int64_t col_id = src_column_ids.at(i).col_id_;
          if (OB_ISNULL(column_schema = source_table_schema->get_column_schema(col_id))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("error unexpected, column schema must not be nullptr", K(ret));
          } else if (!column_schema->is_tbl_part_key_column()) {
            // do nothing
          } else if (OB_FAIL(extra_column_ids.push_back(col_id))) {
            LOG_WARN("failed to push column id", K(ret), K(col_id));
          } else if (column_schema->is_generated_column()) {
            ObSEArray<uint64_t, 5> cascaded_columns;
            if (OB_FAIL(column_schema->get_cascaded_column_ids(cascaded_columns))) {
              LOG_WARN("failed to get cascaded_column_ids", K(ret));
            } else {
              for (int64_t i = 0; OB_SUCC(ret) && i < cascaded_columns.count(); ++i) {
                uint64_t cascade_col_id = cascaded_columns.at(i);
                if (is_contain(extra_column_ids, cascade_col_id)) {
                } else if (OB_FAIL(extra_column_ids.push_back(cascade_col_id))) {
                  LOG_WARN("failed to push cascade column id", K(ret), K(cascade_col_id));
                }
              }
            }
          }
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < extra_column_ids.count(); ++i) {
          const ObColumnSchemaV2 *column_schema = nullptr;
          ObString orig_column_name;
          const int64_t col_id = extra_column_ids.at(i);
          if (OB_ISNULL(column_schema = source_table_schema->get_column_schema(col_id))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("error unexpected, column schema must not be nullptr", K(ret));
          } else if (is_contain(select_column_ids, col_id)) {
            // do nothing
          } else if (nullptr == col_name_map && OB_FALSE_IT(orig_column_name.assign_ptr(column_schema->get_column_name_str().ptr(), column_schema->get_column_name_str().length()))) {
          } else if (nullptr != col_name_map && OB_FAIL(col_name_map->get_orig_column_name(column_schema->get_column_name_str(), orig_column_name))) {
            if (OB_ENTRY_NOT_EXIST == ret) {
              // newly added column cannot be selected from source table.
              ret = OB_SUCCESS;
            } else {
              LOG_WARN("failed to get orig column name", K(ret));
            }
          } else if (OB_FAIL(column_names.push_back(ObColumnNameInfo(orig_column_name, false)))) {
            LOG_WARN("fail to push back column name", K(ret));
          } else if (OB_FAIL(insert_column_names.push_back(ObColumnNameInfo(column_schema->get_column_name_str(), false)))) {
            LOG_WARN("push back insert column name failed", K(ret));
          }
        }
      }
    }

    // get dest table rowkey columns
    if (OB_SUCC(ret)) {
      const ObRowkeyInfo &rowkey_info = dest_table_schema->get_rowkey_info();
      const ObRowkeyColumn *rowkey_column = nullptr;
      const ObColumnSchemaV2 *column_schema = nullptr;
      int64_t col_id = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_info.get_size(); ++i) {
        if (OB_ISNULL(rowkey_column = rowkey_info.get_column(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("error unexpected, rowkey column must not be nullptr", K(ret));
        } else if (FALSE_IT(is_shadow_column = rowkey_column->column_id_ >= OB_MIN_SHADOW_COLUMN_ID)) {
        } else if (FALSE_IT(col_id = is_shadow_column ? rowkey_column->column_id_ - OB_MIN_SHADOW_COLUMN_ID : rowkey_column->column_id_)) {
        } else if (OB_ISNULL(column_schema = dest_table_schema->get_column_schema(col_id))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("error unexpected, column schema must not be nullptr", K(ret), K(col_id));
        } else if (column_schema->is_generated_column()) {
          // generated columns cannot be row key.
        } else if (OB_FAIL(rowkey_column_names.push_back(ObColumnNameInfo(column_schema->get_column_name_str(), is_shadow_column)))) {
          LOG_WARN("fail to push back rowkey column name", K(ret));
        } else if (OB_FAIL(order_column_ids.push_back(col_id))) {
          LOG_WARN("push back order column id failed", K(ret), K(col_id));
        }
      }
    }

    // generate build replica sql
    if (OB_SUCC(ret)) {
      ObSqlString query_column_sql_string;
      ObSqlString insert_column_sql_string;
      ObSqlString rowkey_column_sql_string;
      ObSqlString src_table_schema_version_hint_sql_string;
      const ObString &dest_table_name = dest_table_schema->get_table_name_str();
      const uint64_t dest_database_id = dest_table_schema->get_database_id();
      ObString dest_database_name;
      const ObString &source_table_name = source_table_schema->get_table_name_str();
      const uint64_t source_database_id = source_table_schema->get_database_id();
      ObString source_database_name;

      if (OB_SUCC(ret)) {
        const ObDatabaseSchema *db_schema = nullptr;
        if (OB_FAIL(schema_guard.get_database_schema(tenant_id, dest_database_id, db_schema))) {
          LOG_WARN("fail to get database schema", K(ret), K(tenant_id), K(dest_database_id),
                   K(dest_table_id), K(data_table_id), K(source_database_id));
        } else if (OB_ISNULL(db_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("error unexpected, database schema must not be nullptr", K(ret));
        } else {
          dest_database_name = db_schema->get_database_name_str();
        }

        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(schema_guard.get_database_schema(tenant_id, source_database_id, db_schema))) {
          LOG_WARN("fail to get database schema", K(ret), K(tenant_id));
        } else if (OB_ISNULL(db_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("error unexpected, database schema must not be nullptr", K(ret));
        } else {
          source_database_name = db_schema->get_database_name_str();
        }

        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(generate_column_name_str(column_names, oracle_mode, true/*with origin name*/, true/*with alias name*/, use_heap_table_ddl_plan, query_column_sql_string))) {
          LOG_WARN("fail to generate column name str", K(ret));
        } else if (OB_FAIL(generate_column_name_str(insert_column_names, oracle_mode, true/*with origin name*/, false/*with alias name*/, use_heap_table_ddl_plan, insert_column_sql_string))) {
          LOG_WARN("generate column name str failed", K(ret));
        } else if (!use_heap_table_ddl_plan && OB_FAIL(generate_order_by_str(select_column_ids, order_column_ids, rowkey_column_sql_string))) {
          LOG_WARN("generate order by string failed", K(ret));
        }
      }

      if (OB_SUCC(ret) && use_schema_version_hint_for_src_table) {
        if (OB_FAIL(generate_ddl_schema_hint_str(source_table_name, schema_version, oracle_mode, src_table_schema_version_hint_sql_string))) {
          LOG_WARN("failed to generated ddl schema hint", K(ret));
        }
      }

      if (OB_FAIL(ret)) {
      } else if (oracle_mode) {
        if (OB_FAIL(sql_string.assign_fmt("INSERT /*+ monitor enable_parallel_dml parallel(%ld) opt_param('ddl_execution_id', %ld) opt_param('ddl_task_id', %ld) use_px */INTO \"%.*s\".\"%.*s\"(%.*s) SELECT /*+ index(\"%.*s\" primary) %.*s */ %.*s from \"%.*s\".\"%.*s\" as of scn %ld %.*s",
            real_parallelism, execution_id, task_id,
            static_cast<int>(dest_database_name.length()), dest_database_name.ptr(), static_cast<int>(dest_table_name.length()), dest_table_name.ptr(),
            static_cast<int>(insert_column_sql_string.length()), insert_column_sql_string.ptr(),
            static_cast<int>(source_table_name.length()), source_table_name.ptr(),
            static_cast<int>(src_table_schema_version_hint_sql_string.length()), src_table_schema_version_hint_sql_string.ptr(),
            static_cast<int>(query_column_sql_string.length()), query_column_sql_string.ptr(),
            static_cast<int>(source_database_name.length()), source_database_name.ptr(), static_cast<int>(source_table_name.length()), source_table_name.ptr(),
            snapshot_version, static_cast<int>(rowkey_column_sql_string.length()), rowkey_column_sql_string.ptr()))) {
          LOG_WARN("fail to assign sql string", K(ret));
        }
      } else {
        if (OB_FAIL(sql_string.assign_fmt("INSERT /*+ monitor enable_parallel_dml parallel(%ld) opt_param('ddl_execution_id', %ld) opt_param('ddl_task_id', %ld) use_px */INTO `%.*s`.`%.*s`(%.*s) SELECT /*+ index(`%.*s` primary) %.*s */ %.*s from `%.*s`.`%.*s` as of snapshot %ld %.*s",
            real_parallelism, execution_id, task_id,
            static_cast<int>(dest_database_name.length()), dest_database_name.ptr(), static_cast<int>(dest_table_name.length()), dest_table_name.ptr(),
            static_cast<int>(insert_column_sql_string.length()), insert_column_sql_string.ptr(),
            static_cast<int>(source_table_name.length()), source_table_name.ptr(),
            static_cast<int>(src_table_schema_version_hint_sql_string.length()), src_table_schema_version_hint_sql_string.ptr(),
            static_cast<int>(query_column_sql_string.length()), query_column_sql_string.ptr(),
            static_cast<int>(source_database_name.length()), source_database_name.ptr(), static_cast<int>(source_table_name.length()), source_table_name.ptr(),
            snapshot_version, static_cast<int>(rowkey_column_sql_string.length()), rowkey_column_sql_string.ptr()))) {
          LOG_WARN("fail to assign sql string", K(ret));
        }
      }
    }
    LOG_INFO("execute sql", K(sql_string));
  }
  return ret;
}

int ObDDLUtil::get_tablet_leader_addr(
    share::ObLocationService *location_service,
    const uint64_t tenant_id,
    const ObTabletID &tablet_id,
    const int64_t timeout,
    ObLSID &ls_id,
    ObAddr &leader_addr)
{
  int ret = OB_SUCCESS;
  const bool force_renew = true;
  bool is_cache_hit = false;
  leader_addr.reset();
  const int64_t expire_renew_time = force_renew ? INT64_MAX : 0;
  ObTimeoutCtx timeout_ctx;
  if (nullptr == location_service || OB_INVALID_ID == tenant_id || !tablet_id.is_valid() || timeout <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(location_service), K(tenant_id), K(tablet_id), K(timeout));
  } else if (OB_FAIL(timeout_ctx.set_trx_timeout_us(timeout))) {
    LOG_WARN("set trx timeout failed", K(ret));
  } else if (OB_FAIL(timeout_ctx.set_timeout(timeout))) {
    LOG_WARN("set timeout failed", K(ret));
  } else if (OB_FAIL(location_service->get(tenant_id, tablet_id, expire_renew_time, is_cache_hit, ls_id))) {
    LOG_WARN("fail to get log stream id", K(ret), K(tablet_id));
  } else if (OB_FAIL(location_service->get_leader(GCONF.cluster_id,
          tenant_id,
          ls_id,
          force_renew,
          leader_addr))) {
    LOG_WARN("get leader failed", K(ret), K(ls_id));
  }
  return ret;
}

// Used in offline ddl to delete all checksum record in __all_ddl_checksum
// DELETE FROM __all_ddl_checksum WHERE 
int ObDDLUtil::clear_ddl_checksum(ObPhysicalPlan *phy_plan)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(phy_plan)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    const int64_t execution_id = phy_plan->get_ddl_execution_id();
    const ObOpSpec *root_op_spec = phy_plan->get_root_op_spec();
    uint64_t table_scan_table_id = OB_INVALID_ID;
    if (OB_FAIL(find_table_scan_table_id(root_op_spec, table_scan_table_id))) {
      LOG_WARN("failed to get table scan table id", K(ret));
    } else if (OB_FAIL(ObDDLChecksumOperator::delete_checksum(MTL_ID(),
                                                              execution_id,
                                                              table_scan_table_id,
                                                              phy_plan->get_ddl_table_id(),
                                                              phy_plan->get_ddl_task_id(),
                                                              *GCTX.sql_proxy_))) {
      LOG_WARN("failed to delete checksum", K(ret));
    }
  }
  return ret;
}

int ObDDLUtil::find_table_scan_table_id(const ObOpSpec *spec, uint64_t &table_id) {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(spec) || OB_INVALID_ID != table_id) {
    /*do nothing*/
  } else if (spec->is_table_scan()) {
    table_id = static_cast<const ObTableScanSpec *>(spec)->get_ref_table_id();
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < spec->get_child_cnt(); ++i) {
      if (OB_FAIL(SMART_CALL(find_table_scan_table_id(spec->get_child(i), table_id)))) {
        LOG_WARN("fail to find sample scan", K(ret));
      }
    }
  }
  return ret;
}

int ObDDLUtil::ddl_get_tablet(
    ObLSHandle &ls_handle,
    const ObTabletID &tablet_id,
    storage::ObTabletHandle &tablet_handle,
    const int64_t get_timeout_ts)
{
  int ret = OB_SUCCESS;
  ObLS *ls = nullptr;
  const int64_t DDL_GET_TABLET_RETRY_TIMEOUT = 30 * 1000 * 1000; // 30s
  const int64_t timeout_ts = ObTimeUtility::current_time() + DDL_GET_TABLET_RETRY_TIMEOUT;
  if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ls should not be null", K(ret));
  } else if (OB_FAIL(ls->get_tablet_svr()->get_tablet_with_timeout(tablet_id,
                                                                   tablet_handle,
                                                                   timeout_ts,
                                                                   get_timeout_ts))) {
    LOG_WARN("fail to get tablet handle", K(ret), K(tablet_id));
    if (OB_ALLOCATE_MEMORY_FAILED == ret) {
      ret = OB_TIMEOUT;
    }
  }
  return ret;
}

int ObDDLUtil::check_table_exist(
    const uint64_t tenant_id,
    const uint64_t table_id,
    ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = nullptr;
  const ObDatabaseSchema *database_schema = nullptr;
  uint64_t database_id = OB_INVALID_ID;
  if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id, table_schema))) {
    LOG_WARN("failed to get table schema", K(ret));
  } else if (OB_ISNULL(table_schema) || table_schema->is_in_recyclebin()) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table not exist", K(ret), K(tenant_id), K(table_id), K(table_schema));
  } else if (OB_FALSE_IT(database_id = table_schema->get_database_id())) {
  } else if (OB_FAIL(schema_guard.get_database_schema(tenant_id, database_id, database_schema))) {
    LOG_WARN("failed to get database schema", K(ret), K(tenant_id), K(table_id), K(database_id));
  } else if (OB_ISNULL(database_schema) || database_schema->is_in_recyclebin()) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("database not exist", K(ret), K(tenant_id), K(table_id), K(database_id), K(database_schema));
  }
  return ret;
}

bool ObDDLUtil::need_remote_write(const int ret_code)
{
  return OB_NOT_MASTER == ret_code
    || OB_NOT_RUNNING == ret_code
    || OB_LS_LOCATION_LEADER_NOT_EXIST == ret_code
    || OB_EAGAIN == ret_code;
}

int64_t ObDDLUtil::get_ddl_rpc_timeout()
{
  return max(GCONF.rpc_timeout, 9 * 1000 * 1000L);
}

int ObDDLUtil::get_ddl_cluster_version(
    const uint64_t tenant_id,
    const uint64_t task_id,
    int64_t &ddl_cluster_version)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || task_id <= 0
      || nullptr == GCTX.sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(tenant_id), K(task_id), KP(GCTX.sql_proxy_));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ObSqlString query_string;
      sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(query_string.assign_fmt(" SELECT ddl_type, UNHEX(message) as message_unhex FROM %s WHERE task_id = %lu",
          OB_ALL_DDL_TASK_STATUS_TNAME, task_id))) {
        LOG_WARN("assign sql string failed", K(ret));
      } else if (OB_FAIL(GCTX.sql_proxy_->read(res, tenant_id, query_string.ptr()))) {
        LOG_WARN("read record failed", K(ret), K(query_string));
      } else if (OB_UNLIKELY(nullptr == (result = res.get_result()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get sql result", K(ret), KP(result));
      } else if (OB_FAIL(result->next())) {
        LOG_WARN("get next row failed", K(ret));
      } else {
        int64_t pos = 0;
        ObDDLType ddl_type = ObDDLType::DDL_INVALID;
        ObString task_message;
        EXTRACT_INT_FIELD_MYSQL(*result, "ddl_type", ddl_type, ObDDLType);
        EXTRACT_VARCHAR_FIELD_MYSQL(*result, "message_unhex", task_message);
        if (ObDDLType::DDL_CREATE_INDEX == ddl_type) {
          SMART_VAR(rootserver::ObIndexBuildTask, task) {
            if (OB_FAIL(task.deserlize_params_from_message(task_message.ptr(), task_message.length(), pos))) {
              LOG_WARN("deserialize from msg failed", K(ret));
            } else {
              ddl_cluster_version = task.get_cluster_version();
            }
          }
        } else {
          SMART_VAR(rootserver::ObTableRedefinitionTask, task) {
            if (OB_FAIL(task.deserlize_params_from_message(task_message.ptr(), task_message.length(), pos))) {
              LOG_WARN("deserialize from msg failed", K(ret));
            } else {
              ddl_cluster_version = task.get_cluster_version();
            }
          }
        }
      }
    }
  }
  return ret;
}