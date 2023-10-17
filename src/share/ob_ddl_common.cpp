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

int ObDDLUtil::get_tablet_count(const uint64_t tenant_id,
                              const int64_t table_id,
                              int64_t &tablet_count)
{
  int ret = OB_SUCCESS;
  tablet_count = 0;
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
  } else {
    tablet_count = table_schema->get_all_part_num();
  }
  return ret;
}

int ObDDLUtil::refresh_alter_table_arg(
    const uint64_t tenant_id,
    const int64_t orig_table_id,
    const uint64_t foreign_key_id,
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
      ret = OB_ERR_CONTRAINT_NOT_FOUND;
      LOG_WARN("current constraint not exists, maybe dropped", K(ret), KPC(cst), K(table_schema));
    } else if (OB_FAIL(cst->set_constraint_name(cur_cst->get_constraint_name_str()))) {
      LOG_WARN("failed to set new constraint name", K(ret));
    } else {
      cst->set_name_generated_type(cur_cst->get_name_generated_type());
    }
  }

  // refresh fk arg list
  if (OB_FAIL(ret)) {
  } else if (OB_INVALID_ID == foreign_key_id) {
    if (OB_UNLIKELY(0 != alter_table_arg.foreign_key_arg_list_.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("must specify foreign key id to refresh fk arg list", K(ret), K(alter_table_arg.foreign_key_arg_list_));
    }
  } else {
    if (1 != alter_table_arg.foreign_key_arg_list_.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("only support refresh one fk arg", K(ret));
    } else {
      const ObIArray<ObForeignKeyInfo> &fk_infos = table_schema->get_foreign_key_infos();
      const ObForeignKeyInfo *found_fk_info = nullptr;
      for (int64_t i = 0; nullptr == found_fk_info && i < fk_infos.count(); i++) {
        const ObForeignKeyInfo &fk_info = fk_infos.at(i);
        if (fk_info.foreign_key_id_ == foreign_key_id) {
          found_fk_info = &fk_info;
        }
      }
      if (OB_ISNULL(found_fk_info)) {
        ret = OB_ERR_CONTRAINT_NOT_FOUND;
        LOG_WARN("fk info not found, maybe dropped", K(ret), K(orig_table_id), K(foreign_key_id), K(fk_infos));
      } else if (OB_FAIL(ob_write_string(alter_table_arg.allocator_, found_fk_info->foreign_key_name_, alter_table_arg.foreign_key_arg_list_.at(0).foreign_key_name_, true/*c_style*/))) {
        LOG_WARN("failed to deep copy str", K(ret));
      }
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
    if (column_name_info.is_enum_set_need_cast_) {
      // Enum and set in Recover restore table ddl operation will be cast to unsigned, and then append into macro block.
      if (OB_FAIL(sql_string.append_fmt("cast(%s%.*s%s as unsigned)", split_char, column_name_info.column_name_.length(), column_name_info.column_name_.ptr(), split_char))) {
        LOG_WARN("append origin column name failed", K(ret));
      }
    } else if (OB_FAIL(sql_string.append_fmt("%s%.*s%s", split_char, column_name_info.column_name_.length(), column_name_info.column_name_.ptr(), split_char))) {
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

int ObDDLUtil::generate_spatial_index_column_names(const ObTableSchema &dest_table_schema,
                                                   const ObTableSchema &source_table_schema,
                                                   ObArray<ObColumnNameInfo> &insert_column_names,
                                                   ObArray<ObColumnNameInfo> &column_names,
                                                   ObArray<int64_t> &select_column_ids)
{
  int ret = OB_SUCCESS;
  if (dest_table_schema.is_spatial_index()) {
    uint64_t geo_col_id = OB_INVALID_ID;
    ObArray<ObColDesc> column_ids;
    const ObColumnSchemaV2 *column_schema = nullptr;
    // get dest table column names
    if (OB_FAIL(dest_table_schema.get_column_ids(column_ids))) {
      LOG_WARN("fail to get column ids", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < column_ids.count(); ++i) {
        const int64_t col_id =  column_ids.at(i).col_id_;
        if (OB_ISNULL(column_schema = dest_table_schema.get_column_schema(col_id))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("error unexpected, column schema must not be nullptr", K(ret));
        } else if (OB_FAIL(insert_column_names.push_back(ObColumnNameInfo(column_schema->get_column_name_str(), false)))) {
          LOG_WARN("push back insert column name failed", K(ret));
        } else if (OB_FAIL(column_names.push_back(ObColumnNameInfo(column_schema->get_column_name_str(), false)))) {
          LOG_WARN("push back rowkey column name failed", K(ret));
        } else if (OB_FAIL(select_column_ids.push_back(col_id))) {
          LOG_WARN("push back select column id failed", K(ret), K(col_id));
        } else if (OB_NOT_NULL(column_schema = source_table_schema.get_column_schema(col_id))
                   && !column_schema->is_rowkey_column()
                   && geo_col_id == OB_INVALID_ID) {
          geo_col_id = column_schema->get_geo_col_id();
        }
      }
      if (OB_SUCC(ret)) {
        if (geo_col_id == OB_INVALID_ID) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("error unexpected, get geo column failed", K(ret));
        } else if (OB_ISNULL(column_schema = source_table_schema.get_column_schema(geo_col_id))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("error unexpected, column schema must not be nullptr", K(ret));
        } else if (OB_FAIL(column_names.push_back(ObColumnNameInfo(column_schema->get_column_name_str(), false)))) {
          LOG_WARN("push back geo column name failed", K(ret));
        } else if (OB_FAIL(select_column_ids.push_back(geo_col_id))) {
          LOG_WARN("push back select column id failed", K(ret), K(geo_col_id));
        }
      }
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
      || schema_version <= 0 || snapshot_version <= 0 || execution_id < 0 || task_id <= 0)) {
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
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, dest_table_id, dest_table_schema))) {
    LOG_WARN("fail to get table schema", K(ret), K(tenant_id), K(dest_table_id));
  } else if (OB_ISNULL(source_table_schema) || OB_ISNULL(dest_table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("fail to get table schema", K(ret), KP(source_table_schema), KP(dest_table_schema),
      K(tenant_id), K(data_table_id), K(dest_table_id));
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
    if (dest_table_schema->is_spatial_index()) {
      if (OB_FAIL(ObDDLUtil::generate_spatial_index_column_names(*dest_table_schema, *source_table_schema, insert_column_names,
                                                                 column_names, select_column_ids))) {
        LOG_WARN("generate spatial index column names failed", K(ret));
      }
    } else if (OB_FAIL(dest_table_schema->get_column_ids(column_ids))) {
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
        } else if (column_schema->is_generated_column() && !dest_table_schema->is_spatial_index()) {
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



      if (OB_SUCC(ret)) {
        ObArenaAllocator allocator("ObDDLTmp");
        ObString new_dest_database_name;
        ObString new_dest_table_name;
        ObString new_source_table_name;
        ObString new_source_database_name;

        if (OB_FAIL(sql::ObSQLUtils::generate_new_name_with_escape_character(
              allocator,
              dest_database_name,
              new_dest_database_name,
              oracle_mode))) {
          LOG_WARN("fail to generate new name with escape character",
                    K(ret), K(dest_database_name));
        } else if (OB_FAIL(sql::ObSQLUtils::generate_new_name_with_escape_character(
              allocator,
              dest_table_name,
              new_dest_table_name,
              oracle_mode))) {
          LOG_WARN("fail to generate new name with escape character",
                    K(ret), K(dest_table_name));
        } else if (OB_FAIL(sql::ObSQLUtils::generate_new_name_with_escape_character(
              allocator,
              source_database_name,
              new_source_database_name,
              oracle_mode))) {
          LOG_WARN("fail to generate new name with escape character",
                    K(ret), K(source_database_name));
        } else if (OB_FAIL(sql::ObSQLUtils::generate_new_name_with_escape_character(
              allocator,
              source_table_name,
              new_source_table_name,
              oracle_mode))) {
          LOG_WARN("fail to generate new name with escape character",
                    K(ret), K(source_table_name));
        } else if (use_schema_version_hint_for_src_table) {
          if (OB_FAIL(generate_ddl_schema_hint_str(new_source_table_name, schema_version, oracle_mode, src_table_schema_version_hint_sql_string))) {
            LOG_WARN("failed to generated ddl schema hint", K(ret));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (oracle_mode) {
          if (OB_FAIL(sql_string.assign_fmt("INSERT /*+ monitor enable_parallel_dml parallel(%ld) opt_param('ddl_execution_id', %ld) opt_param('ddl_task_id', %ld) opt_param('enable_newsort', 'false') use_px */INTO \"%.*s\".\"%.*s\"(%.*s) SELECT /*+ index(\"%.*s\" primary) %.*s */ %.*s from \"%.*s\".\"%.*s\" as of scn %ld %.*s",
              real_parallelism, execution_id, task_id,
              static_cast<int>(new_dest_database_name.length()), new_dest_database_name.ptr(), static_cast<int>(new_dest_table_name.length()), new_dest_table_name.ptr(),
              static_cast<int>(insert_column_sql_string.length()), insert_column_sql_string.ptr(),
              static_cast<int>(new_source_table_name.length()), new_source_table_name.ptr(),
              static_cast<int>(src_table_schema_version_hint_sql_string.length()), src_table_schema_version_hint_sql_string.ptr(),
              static_cast<int>(query_column_sql_string.length()), query_column_sql_string.ptr(),
              static_cast<int>(new_source_database_name.length()), new_source_database_name.ptr(), static_cast<int>(new_source_table_name.length()), new_source_table_name.ptr(),
              snapshot_version, static_cast<int>(rowkey_column_sql_string.length()), rowkey_column_sql_string.ptr()))) {
            LOG_WARN("fail to assign sql string", K(ret));
          }
        } else {
          if (OB_FAIL(sql_string.assign_fmt("INSERT /*+ monitor enable_parallel_dml parallel(%ld) opt_param('ddl_execution_id', %ld) opt_param('ddl_task_id', %ld) opt_param('enable_newsort', 'false') use_px */INTO `%.*s`.`%.*s`(%.*s) SELECT /*+ index(`%.*s` primary) %.*s */ %.*s from `%.*s`.`%.*s` as of snapshot %ld %.*s",
              real_parallelism, execution_id, task_id,
              static_cast<int>(new_dest_database_name.length()), new_dest_database_name.ptr(), static_cast<int>(new_dest_table_name.length()), new_dest_table_name.ptr(),
              static_cast<int>(insert_column_sql_string.length()), insert_column_sql_string.ptr(),
              static_cast<int>(new_source_table_name.length()), new_source_table_name.ptr(),
              static_cast<int>(src_table_schema_version_hint_sql_string.length()), src_table_schema_version_hint_sql_string.ptr(),
              static_cast<int>(query_column_sql_string.length()), query_column_sql_string.ptr(),
              static_cast<int>(new_source_database_name.length()), new_source_database_name.ptr(), static_cast<int>(new_source_table_name.length()), new_source_table_name.ptr(),
              snapshot_version, static_cast<int>(rowkey_column_sql_string.length()), rowkey_column_sql_string.ptr()))) {
            LOG_WARN("fail to assign sql string", K(ret));
          }
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
    for (uint32_t i = 0; OB_SUCC(ret) && i < spec->get_child_cnt(); ++i) {
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
    storage::ObMDSGetTabletMode mode)
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
                                                                   mode))) {
    LOG_WARN("fail to get tablet handle", K(ret), K(tablet_id));
    if (OB_ALLOCATE_MEMORY_FAILED == ret) {
      ret = OB_TIMEOUT;
    }
  }
  return ret;
}

bool ObDDLUtil::need_remote_write(const int ret_code)
{
  return ObTenantRole::PRIMARY_TENANT == MTL_GET_TENANT_ROLE_CACHE()
    && (OB_NOT_MASTER == ret_code
        || OB_NOT_RUNNING == ret_code
        || OB_LS_LOCATION_LEADER_NOT_EXIST == ret_code
        || OB_EAGAIN == ret_code);
}

int ObDDLUtil::get_tablet_paxos_member_list(
  const uint64_t tenant_id,
  const common::ObTabletID &tablet_id,
  common::ObIArray<common::ObAddr> &paxos_server_list,
  int64_t &paxos_member_count)
{
  int ret = OB_SUCCESS;
  ObLSLocation location;
  paxos_member_count = 0;
  if (OB_INVALID_TENANT_ID == tenant_id || !tablet_id.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get tablet replica location, invalid id", K(ret), K(tenant_id), K(tablet_id));
  } else if (OB_FAIL(get_tablet_replica_location(tenant_id, tablet_id, location))) {
    LOG_WARN("fail to get tablet replica location", K(tenant_id), K(tablet_id), K(ret));
  } else {
    const ObIArray<ObLSReplicaLocation> &ls_locations = location.get_replica_locations();
    for (int64_t i = 0; i < ls_locations.count() && OB_SUCC(ret); ++i) {
      common::ObReplicaType replica_type  = ls_locations.at(i).get_replica_type();
      if (REPLICA_TYPE_FULL != replica_type && REPLICA_TYPE_LOGONLY != replica_type) {
        continue;
      }
      paxos_member_count++;
      if (REPLICA_TYPE_FULL == replica_type) {  // paxos replica
        const ObAddr &server = ls_locations.at(i).get_server();
        if (!has_exist_in_array(paxos_server_list, server) && OB_FAIL(paxos_server_list.push_back(server))) {
          LOG_WARN("fail to push back addr", K(ret), K(server));
        }
      }
    }
  }
  return ret;
}

int ObDDLUtil::get_tablet_replica_location(
    const uint64_t tenant_id,
    const common::ObTabletID &tablet_id,
    ObLSLocation &location)
{
  int ret = OB_SUCCESS;
  const int64_t cluster_id = GCONF.cluster_id;
  share::ObLSID ls_id;
  int64_t expire_renew_time = INT64_MAX;
  bool is_cache_hit = false;
  if (OB_UNLIKELY(nullptr == GCTX.location_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("location service ptr is null", K(ret));
  } else if (tenant_id == OB_INVALID_TENANT_ID || !tablet_id.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get tablet replica location, invalid id", K(ret), K(tenant_id), K(tablet_id));
  } else if (OB_FAIL(GCTX.location_service_->get(tenant_id,
                                                 tablet_id,
                                                 INT64_MAX,
                                                 is_cache_hit,
                                                 ls_id))) {
    LOG_WARN("fail to get ls id according to tablet_id", K(ret), K(tenant_id), K(tablet_id));
  } else if (OB_FAIL(GCTX.location_service_->get(cluster_id,
                                                 tenant_id,
                                                 ls_id,
                                                 expire_renew_time,
                                                 is_cache_hit,
                                                 location))) {
    LOG_WARN("fail to get ls location", K(ret), K(cluster_id), K(tenant_id), K(ls_id), K(tablet_id));
  }
  return ret;
}

int ObDDLUtil::get_sys_ls_leader_addr(
    const uint64_t cluster_id,
    const uint64_t tenant_id,
    common::ObAddr &leader_addr)
{
  int ret = OB_SUCCESS;
  bool force_renew = false;
  share::ObLocationService *location_service = nullptr;
  share::ObLSID ls_id(share::ObLSID::SYS_LS_ID);

  if (OB_ISNULL(location_service = GCTX.location_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to check and wait old completement, null pointer. ", K(ret));
  } else if (OB_FAIL(location_service->get_leader(cluster_id,
                                                  tenant_id,
                                                  ls_id,
                                                  force_renew,
                                                  leader_addr))) {
    LOG_WARN("failed to get ls_leader", K(ret));
  } else if (OB_UNLIKELY(!leader_addr.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("leader addr is invalid", K(ret), K(tenant_id), K(leader_addr), K(cluster_id));
  } else {
    LOG_INFO("succ to get ls leader addr", K(cluster_id), K(tenant_id), K(leader_addr));
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

int ObDDLUtil::get_ddl_rpc_timeout(const int64_t tablet_count, int64_t &ddl_rpc_timeout_us)
{
  int ret = OB_SUCCESS;
  const int64_t rpc_timeout_upper = 20L * 60L * 1000L * 1000L; // upper 20 minutes
  const int64_t cost_per_tablet = 20L * 60L * 100L; // 10000 tablets use 20 minutes, so 1 tablet use 20 * 60 * 100 us
  ddl_rpc_timeout_us = tablet_count * cost_per_tablet;
  ddl_rpc_timeout_us = max(ddl_rpc_timeout_us, GCONF._ob_ddl_timeout);
  ddl_rpc_timeout_us = min(ddl_rpc_timeout_us, rpc_timeout_upper);
  ddl_rpc_timeout_us = max(ddl_rpc_timeout_us, GCONF.rpc_timeout);
  return ret;
}

int ObDDLUtil::get_ddl_rpc_timeout(const int64_t tenant_id, const int64_t table_id, int64_t &ddl_rpc_timeout_us)
{
  int ret = OB_SUCCESS;
  int64_t tablet_count = 0;
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || OB_INVALID_ID == table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(table_id));
  } else if (OB_FAIL(get_tablet_count(tenant_id, table_id, tablet_count))) {
    ret = OB_SUCCESS; // force succ
    tablet_count = 0;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_ddl_rpc_timeout(tablet_count, ddl_rpc_timeout_us))) {
    LOG_WARN("get ddl rpc timeout failed", K(ret));
  }
  return ret;
}

void ObDDLUtil::get_ddl_rpc_timeout_for_database(const int64_t tenant_id, const int64_t database_id, int64_t &ddl_rpc_timeout_us)
{
  int ret = OB_SUCCESS;
  const int64_t cost_per_tablet = 100 * 1000L; // 100ms
  share::schema::ObSchemaGetterGuard schema_guard;
  ObArray<uint64_t> table_ids;
  if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == database_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(database_id));
  } else if (OB_FAIL(share::schema::ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(
      tenant_id, schema_guard))) {
    LOG_WARN("get tenant schema guard failed", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_table_ids_in_database(tenant_id,
                                                            database_id,
                                                            table_ids))) {
    LOG_WARN("failed to get table ids in database", K(ret));
  }
  for (int64_t i = 0; i < table_ids.count(); i++) {
    int64_t tablet_count = 0;
    if (OB_SUCCESS != get_tablet_count(tenant_id, table_ids[i], tablet_count)) {
      tablet_count = 0;
    }
    ddl_rpc_timeout_us += tablet_count * cost_per_tablet;
  }
  ddl_rpc_timeout_us = max(ddl_rpc_timeout_us, get_default_ddl_rpc_timeout());
  ddl_rpc_timeout_us = max(ddl_rpc_timeout_us, GCONF._ob_ddl_timeout);
  return;
}

int ObDDLUtil::get_ddl_tx_timeout(const int64_t tablet_count, int64_t &ddl_tx_timeout_us)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_ddl_rpc_timeout(tablet_count, ddl_tx_timeout_us))) {
    LOG_WARN("get ddl rpc timeout faild", K(ret));
  }
  return ret;
}
int ObDDLUtil::get_ddl_tx_timeout(const int64_t tenant_id, const int64_t table_id, int64_t &ddl_tx_timeout_us)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_ddl_rpc_timeout(tenant_id, table_id, ddl_tx_timeout_us))) {
    LOG_WARN("get ddl rpc timeout faild", K(ret));
  }
  return ret;
}

int64_t ObDDLUtil::get_default_ddl_rpc_timeout()
{
  return min(20L * 60L * 1000L * 1000L, max(GCONF.rpc_timeout, 9 * 1000 * 1000L));
}

int64_t ObDDLUtil::get_default_ddl_tx_timeout()
{
  return get_default_ddl_rpc_timeout();
}


int ObDDLUtil::get_data_format_version(
    const uint64_t tenant_id,
    const uint64_t task_id,
    int64_t &data_format_version)
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
            if (OB_FAIL(task.deserlize_params_from_message(tenant_id, task_message.ptr(), task_message.length(), pos))) {
              LOG_WARN("deserialize from msg failed", K(ret));
            } else {
              data_format_version = task.get_data_format_version();
            }
          }
        } else {
          SMART_VAR(rootserver::ObTableRedefinitionTask, task) {
            if (OB_FAIL(task.deserlize_params_from_message(tenant_id, task_message.ptr(), task_message.length(), pos))) {
              LOG_WARN("deserialize from msg failed", K(ret));
            } else {
              data_format_version = task.get_data_format_version();
            }
          }
        }
      }
    }
  }
  return ret;
}

static inline void try_replace_user_tenant_id(const uint64_t user_tenant_id, uint64_t &check_tenant_id)
{
  check_tenant_id = !is_user_tenant(check_tenant_id) ? check_tenant_id : user_tenant_id;
}

int ObDDLUtil::replace_user_tenant_id(
    const ObDDLType &ddl_type,
    const uint64_t tenant_id,
    obrpc::ObAlterTableArg &alter_table_arg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_invalid_ddl_type(ddl_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(ddl_type));
  } else if (!is_user_tenant(tenant_id)) {
    LOG_TRACE("not user tenant, no need to replace", K(tenant_id));
  } else {
    const bool need_replace_schema_info = DDL_TABLE_RESTORE != ddl_type;
    try_replace_user_tenant_id(tenant_id, alter_table_arg.exec_tenant_id_);
    for (int64_t i = 0; OB_SUCC(ret) && i < alter_table_arg.index_arg_list_.count(); ++i) {
      obrpc::ObIndexArg *index_arg = alter_table_arg.index_arg_list_.at(i);
      try_replace_user_tenant_id(tenant_id, index_arg->exec_tenant_id_);
      try_replace_user_tenant_id(tenant_id, index_arg->tenant_id_);
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < alter_table_arg.foreign_key_arg_list_.count(); ++i) {
      obrpc::ObCreateForeignKeyArg &fk_arg = alter_table_arg.foreign_key_arg_list_.at(i);
      try_replace_user_tenant_id(tenant_id, fk_arg.exec_tenant_id_);
      try_replace_user_tenant_id(tenant_id, fk_arg.tenant_id_);
    }
    if (need_replace_schema_info && is_user_tenant(alter_table_arg.alter_table_schema_.get_tenant_id())) {
      alter_table_arg.alter_table_schema_.set_tenant_id(tenant_id);
    }
    try_replace_user_tenant_id(tenant_id, alter_table_arg.sequence_ddl_arg_.exec_tenant_id_);
    if (is_user_tenant(alter_table_arg.sequence_ddl_arg_.seq_schema_.get_tenant_id())) {
      alter_table_arg.sequence_ddl_arg_.seq_schema_.set_tenant_id(tenant_id);
    }
  }
  return ret;
}

int ObDDLUtil::replace_user_tenant_id(const uint64_t tenant_id, obrpc::ObCreateIndexArg &create_index_arg)
{
  int ret = OB_SUCCESS;
  if (!is_user_tenant(tenant_id)) {
    LOG_TRACE("not user tenant, no need to replace", K(tenant_id));
  } else {
    try_replace_user_tenant_id(tenant_id, create_index_arg.exec_tenant_id_);
    try_replace_user_tenant_id(tenant_id, create_index_arg.tenant_id_);
    if (is_user_tenant(create_index_arg.index_schema_.get_tenant_id())) {
      create_index_arg.index_schema_.set_tenant_id(tenant_id);
    }
  }
  return ret;
}

#define REPLACE_DDL_ARG_FUNC(ArgType) \
int ObDDLUtil::replace_user_tenant_id(const uint64_t tenant_id, ArgType &ddl_arg) \
{ \
  int ret = OB_SUCCESS; \
  if (!is_user_tenant(tenant_id)) { \
    LOG_TRACE("not user tenant, no need to replace", K(tenant_id)); \
  } else { \
    try_replace_user_tenant_id(tenant_id, ddl_arg.exec_tenant_id_); \
    try_replace_user_tenant_id(tenant_id, ddl_arg.tenant_id_); \
  } \
  return ret; \
}

REPLACE_DDL_ARG_FUNC(obrpc::ObDropDatabaseArg)
REPLACE_DDL_ARG_FUNC(obrpc::ObDropTableArg)
REPLACE_DDL_ARG_FUNC(obrpc::ObDropIndexArg)
REPLACE_DDL_ARG_FUNC(obrpc::ObTruncateTableArg)

#undef REPLACE_DDL_ARG_FUNC

int ObDDLUtil::reshape_ddl_column_obj(
    common::ObDatum &datum,
    const ObObjMeta &obj_meta)
{
  int ret = OB_SUCCESS;
  if (datum.is_null()) {
    // do not need to reshape
  } else if (obj_meta.is_lob_storage()) {
    ObLobLocatorV2 lob(datum.get_string(), obj_meta.has_lob_header());
    ObString disk_loc;
    if (!lob.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid lob locator", K(ret));
    } else if (!lob.is_lob_disk_locator() && !lob.is_persist_lob()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid lob locator, should be persist lob", K(ret), K(lob));
    } else if (OB_FAIL(lob.get_disk_locator(disk_loc))) {
      LOG_WARN("get disk locator failed", K(ret), K(lob));
    }
    if (OB_SUCC(ret)) {
      datum.set_string(disk_loc);
    }
  } else if (OB_UNLIKELY(!obj_meta.is_fixed_len_char_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no need to reshape non-char", K(ret));
  } else {
    const char *ptr = datum.ptr_;
    int32_t len = datum.len_;
    int32_t trunc_len_byte = static_cast<int32_t>(ObCharset::strlen_byte_no_sp(
        obj_meta.get_collation_type(), ptr, len));
    datum.set_string(ObString(trunc_len_byte, ptr));
  }
  return ret;
}

int ObDDLUtil::get_tenant_schema_guard(
    const uint64_t src_tenant_id,
    const uint64_t dst_tenant_id,
    share::schema::ObSchemaGetterGuard &hold_buf_src_tenant_schema_guard,
    share::schema::ObSchemaGetterGuard &hold_buf_dst_tenant_schema_guard,
    share::schema::ObSchemaGetterGuard *&src_tenant_schema_guard,
    share::schema::ObSchemaGetterGuard *&dst_tenant_schema_guard)
{
  int ret = OB_SUCCESS;
  src_tenant_schema_guard = nullptr;
  dst_tenant_schema_guard = nullptr;
  rootserver::ObRootService *root_service = GCTX.root_service_;
  if (OB_UNLIKELY(common::OB_INVALID_ID == src_tenant_id || common::OB_INVALID_ID == dst_tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(src_tenant_id), K(dst_tenant_id));
  } else if (OB_ISNULL(root_service)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, root service must not be nullptr", K(ret));
  } else {
    share::schema::ObMultiVersionSchemaService &schema_service = root_service->get_schema_service();
    if (OB_FAIL(schema_service.get_tenant_schema_guard(dst_tenant_id, hold_buf_dst_tenant_schema_guard))) {
      LOG_WARN("get tanant schema guard failed", K(ret), K(dst_tenant_id));
    } else if (src_tenant_id != dst_tenant_id) {
      if (OB_FAIL(schema_service.get_tenant_schema_guard(src_tenant_id, hold_buf_src_tenant_schema_guard))) {
        LOG_WARN("get tanant schema guard failed", K(ret), K(src_tenant_id));
      } else {
        src_tenant_schema_guard = &hold_buf_src_tenant_schema_guard;
        dst_tenant_schema_guard = &hold_buf_dst_tenant_schema_guard;
      }
    } else {
      src_tenant_schema_guard = &hold_buf_dst_tenant_schema_guard;
      dst_tenant_schema_guard = &hold_buf_dst_tenant_schema_guard;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(nullptr == src_tenant_schema_guard || nullptr == dst_tenant_schema_guard)) {
      ret = OB_TENANT_NOT_EXIST;
      LOG_WARN("tenant not exist", K(ret), K(src_tenant_id), K(dst_tenant_id), KP(src_tenant_schema_guard), KP(dst_tenant_schema_guard));
    }
  }
  return ret;
}

/******************           ObCheckTabletDataComplementOp         *************/

int ObCheckTabletDataComplementOp::check_task_inner_sql_session_status(
    const common::ObAddr &inner_sql_exec_addr,
    const common::ObCurTraceId::TraceId &trace_id,
    const uint64_t tenant_id,
    const int64_t task_id,
    const int64_t scn,
    bool &is_old_task_session_exist)
{
  int ret = OB_SUCCESS;
  is_old_task_session_exist = false;
  char ip_str[common::OB_IP_STR_BUFF];
  rootserver::ObRootService *root_service = nullptr;

  if (OB_ISNULL(root_service = GCTX.root_service_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("fail to get sql proxy, root service is null.!");
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || trace_id.is_invalid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(trace_id), K(inner_sql_exec_addr));
  } else {
    ret = OB_SUCCESS;
    common::ObMySQLProxy &proxy = root_service->get_sql_proxy();
    ObSqlString sql_string;
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      sqlclient::ObMySQLResult *result = NULL;
      char trace_id_str[64] = { 0 };
      char charater = '%';
      if (OB_UNLIKELY(0 > trace_id.to_string(trace_id_str, sizeof(trace_id_str)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get trace id string failed", K(ret), K(trace_id));
      } else if (!inner_sql_exec_addr.is_valid()) {
        if (OB_FAIL(sql_string.assign_fmt(" SELECT id as session_id FROM %s WHERE trace_id = \"%s\" "
              " and tenant = (select tenant_name from __all_tenant where tenant_id = %lu) "
              " and info like \"%cINSERT%c('ddl_task_id', %ld)%cINTO%cSELECT%c%ld%c\" ",
            OB_ALL_VIRTUAL_SESSION_INFO_TNAME,
            trace_id_str,
            tenant_id,
            charater,
            charater,
            task_id,
            charater,
            charater,
            charater,
            scn,
            charater ))) {
          LOG_WARN("assign sql string failed", K(ret));
        }
      } else {
        if (!inner_sql_exec_addr.ip_to_string(ip_str, sizeof(ip_str))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("ip to string failed", K(ret), K(inner_sql_exec_addr));
        } else if (OB_FAIL(sql_string.assign_fmt(" SELECT id as session_id FROM %s WHERE trace_id = \"%s\" "
              " and tenant = (select tenant_name from __all_tenant where tenant_id = %lu) "
              " and svr_ip = \"%s\" and svr_port = %d and info like \"%cINSERT%c('ddl_task_id', %ld)%cINTO%cSELECT%c%ld%c\" ",
            OB_ALL_VIRTUAL_SESSION_INFO_TNAME,
            trace_id_str,
            tenant_id,
            ip_str,
            inner_sql_exec_addr.get_port(),
            charater,
            charater,
            task_id,
            charater,
            charater,
            charater,
            scn,
            charater ))) {
          LOG_WARN("assign sql string failed", K(ret));
        }
      }
      if (REACH_TIME_INTERVAL(10L * 1000L * 1000L)) { // every 10s
        LOG_INFO("check task inner sql string", K(sql_string));
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(proxy.read(res, OB_SYS_TENANT_ID, sql_string.ptr(), &inner_sql_exec_addr))) {
        LOG_WARN("query ddl task record failed", K(ret), K(sql_string));
      } else if (OB_ISNULL((result = res.get_result()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get sql result", K(ret), KP(result));
      } else {
        uint64_t session_id = 0;
        while (OB_SUCC(ret)) {
          if (OB_FAIL(result->next())) {
            if (OB_ITER_END == ret) {
              ret = OB_SUCCESS;
              break;
            } else {
              LOG_WARN("fail to get next row", K(ret));
            }
          } else {
            is_old_task_session_exist =  true;
            EXTRACT_UINT_FIELD_MYSQL(*result, "session_id", session_id, uint64_t);
          }
        }
      }
    }
  }
  return ret;
}

int ObCheckTabletDataComplementOp::update_replica_merge_status(
    const ObTabletID &tablet_id,
    const bool merge_status,
    hash::ObHashMap<ObTabletID, int32_t> &tablets_commited_map)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("update replica merge status fail.", K(ret));
  } else {
    int32_t commited_count = 0;
    if (OB_SUCC(tablets_commited_map.get_refactored(tablet_id, commited_count))) {
      // overwrite
      if (merge_status) {
        commited_count++;
        if (OB_FAIL(tablets_commited_map.set_refactored(tablet_id, commited_count, true /* overwrite */))) {
          LOG_WARN("fail to insert map status", K(ret));
        }
      }
    } else if (OB_HASH_NOT_EXIST == ret) {  // new insert
      ret = OB_SUCCESS;
      if (merge_status) {
        commited_count = 1;
        if (OB_FAIL(tablets_commited_map.set_refactored(tablet_id, commited_count, true /* overwrite */))) {
          LOG_WARN("fail to insert map status", K(ret));
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to update replica merge status", K(ret));
    }
    LOG_INFO("success to update replica merge status.", K(tablet_id), K(merge_status));
  }
  return ret;
}


// only get un-merge tablet replica ip addr
int ObCheckTabletDataComplementOp::construct_tablet_ip_map(
    const uint64_t tenant_id,
    const ObTabletID &tablet_id,
    hash::ObHashMap<ObAddr, ObArray<ObTabletID>> &ip_tablets_map)
{
  int ret = OB_SUCCESS;
  common::ObArray<ObAddr> paxos_server_list;
  common::ObArray<ObAddr> unfinished_replica_addrs;
  common::ObArray<ObTabletID> tablet_array;
  int64_t paxos_member_count; // unused, but need

  if (!tablet_id.is_valid() || OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tablet_id), K(tenant_id));
  } else if (OB_FAIL(ObDDLUtil::get_tablet_paxos_member_list(tenant_id, tablet_id, paxos_server_list, paxos_member_count))) {
    LOG_WARN("fail to get tablet replica location!", K(ret), K(tablet_id));
  } else {
    // classify un-merge tablet addr and tablet ids
    for (int64_t i = 0; OB_SUCC(ret) && i < paxos_server_list.count(); ++i) {
      tablet_array.reset();
      const ObAddr & addr = paxos_server_list.at(i);
      if (OB_FAIL(ip_tablets_map.get_refactored(addr, tablet_array))) {
        if (OB_HASH_NOT_EXIST == ret) { // first time
          ret = OB_SUCCESS;
          if (OB_FAIL(tablet_array.push_back(tablet_id))) {
            LOG_WARN("fail to push back to array", K(ret), K(tablet_id));
          } else if (OB_FAIL(ip_tablets_map.set_refactored(addr, tablet_array, true/* overwrite */))) {
            LOG_WARN("set ip tablet map fail.", K(ret), K(tablet_id), K(addr));
          }
        } else {
          LOG_WARN("get ip tablet from map fail.", K(ret), K(tablet_id), K(addr));
        }
      } else if (OB_FAIL(tablet_array.push_back(tablet_id))) {
        LOG_WARN("fail to push back to array", K(ret), K(tablet_id));
      } else if (OB_FAIL(ip_tablets_map.set_refactored(addr, tablet_array, true/* overwrite */))) {
        LOG_WARN("set ip tablet map fail.", K(ret), K(tablet_id), K(addr));
      }
    }
  }

  return ret;
}

int ObCheckTabletDataComplementOp::construct_ls_tablet_map(
  const uint64_t tenant_id,
  const common::ObTabletID &tablet_id,
  hash::ObHashMap<ObLSID, ObArray<ObTabletID>> &ls_tablets_map)
{
  int ret = OB_SUCCESS;
  bool is_cache_hit = false;
  share::ObLSID ls_id;
  common::ObArray<ObTabletID> tablet_array;

  if (!tablet_id.is_valid() || OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tablet_id), K(tenant_id));
  } else if (OB_FAIL(GCTX.location_service_->get(tenant_id,
                                                 tablet_id,
                                                 INT64_MAX,
                                                 is_cache_hit, /*is_cache_hit*/
                                                 ls_id))) {
    LOG_WARN("fail to get ls id according to tablet_id", K(ret), K(tenant_id), K(tablet_id));
  } else if (OB_FAIL(ls_tablets_map.get_refactored(ls_id, tablet_array))) {
    if (OB_HASH_NOT_EXIST == ret) { // first time
      ret = OB_SUCCESS;
      if (OB_FAIL(tablet_array.push_back(tablet_id))) {
        LOG_WARN("fail to push back to array", K(ret), K(tablet_id));
      } else if (OB_FAIL(ls_tablets_map.set_refactored(ls_id, tablet_array, false))) {
        LOG_WARN("ls_tablets_map set fail", K(ret), K(tablet_id), K(ls_id));
      }
    } else {
      LOG_WARN("ls_tablets_map get fail", K(ret), K(tablet_id), K(ls_id));
    }
  } else if (OB_FAIL(tablet_array.push_back(tablet_id))) {
    LOG_WARN("fail to push back to array", K(ret), K(tablet_id));
  } else if (OB_FAIL(ls_tablets_map.set_refactored(ls_id, tablet_array, true /* overwrite */))) {
    LOG_WARN("ls_tablets_map set fail", K(ret), K(tablet_id), K(ls_id));
  }

  return ret;
}

int ObCheckTabletDataComplementOp::calculate_build_finish(
  const uint64_t tenant_id,
  const common::ObIArray<common::ObTabletID> &tablet_ids,
  hash::ObHashMap<ObTabletID, int32_t> &tablets_commited_map,
  int64_t &build_succ_count)
{
  int ret = OB_SUCCESS;
  common::ObArray<common::ObAddr> paxos_server_list;  // unused
  int64_t paxos_member_count = 0;

  build_succ_count = 0;

  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to check tablets commit status", K(ret), K(tenant_id));
  } else if (tablets_commited_map.size() <= 0) {
    // do nothing
  } else {
    int commited_count = 0;
    for (int64_t tablet_idx = 0; OB_SUCC(ret) && tablet_idx < tablet_ids.count(); ++tablet_idx) {
      common::ObTabletID tablet_id = tablet_ids.at(tablet_idx);
      if (OB_FAIL(ObDDLUtil::get_tablet_paxos_member_list(tenant_id,
                                                tablet_id,
                                                paxos_server_list,
                                                paxos_member_count))) {
        LOG_WARN("fail to get tablet paxos member list.",
          K(ret), K(tenant_id), K(tablet_id), K(paxos_server_list), K(paxos_member_count));
      } else if (paxos_member_count == 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to check task tablet, unexpected!",
          K(ret), K(paxos_member_count), K(tablet_id), K(tenant_id));
      } else if (OB_FAIL(tablets_commited_map.get_refactored(tablet_id, commited_count))){
        LOG_WARN("fail to get tablet commited map, unexpected!", K(ret), K(tablet_id));
      } else if (commited_count < ((paxos_member_count >> 1) + 1)) {  // not finished majority
        // do nothing
      } else {
        build_succ_count++;
      }

    }
    LOG_INFO("succ check and commit count", K(build_succ_count));
  }
  return ret;
}


int ObCheckTabletDataComplementOp::do_check_tablets_merge_status(
  const uint64_t tenant_id,
  const int64_t snapshot_version,
  const ObIArray<ObTabletID> &tablet_ids,
  const ObLSID &ls_id,
  hash::ObHashMap<ObAddr, ObArray<ObTabletID>> &ip_tablets_map,
  hash::ObHashMap<ObTabletID, int32_t> &tablets_commited_map,
  int64_t &tablet_build_succ_count)
{
  int ret = OB_SUCCESS;
  obrpc::ObSrvRpcProxy *rpc_proxy = GCTX.srv_rpc_proxy_;
  ip_tablets_map.reuse();
  tablets_commited_map.reuse();

  tablet_build_succ_count = 0;

  if (OB_UNLIKELY(tablet_ids.count() < 0 || OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_TIMESTAMP == snapshot_version) ||
        OB_ISNULL(rpc_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tablet_ids.count()), K(tenant_id), K(snapshot_version), K(rpc_proxy));
  } else {
    rootserver::ObCheckTabletMergeStatusProxy proxy(*rpc_proxy,
        &obrpc::ObSrvRpcProxy::check_ddl_tablet_merge_status);
    obrpc::ObDDLCheckTabletMergeStatusArg arg;
    const int64_t rpc_timeout = max(GCONF.rpc_timeout, 1000L * 1000L * 9L);

    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids.count(); ++i) {
      const ObTabletID &tablet_id = tablet_ids.at(i);
      if (OB_FAIL(construct_tablet_ip_map(tenant_id, tablet_id, ip_tablets_map))) {
        LOG_WARN("fail to get tablet ip addr", K(ret), K(tablet_id));
      }
    }
    // handle every addr tablet
    for (hash::ObHashMap<ObAddr, ObArray<ObTabletID>>::const_iterator ip_iter = ip_tablets_map.begin();
      OB_SUCC(ret) && ip_iter != ip_tablets_map.end(); ++ip_iter) {
      const ObAddr & dest_ip = ip_iter->first;
      const ObArray<ObTabletID> &tablet_array = ip_iter->second;
      if (OB_FAIL(arg.tablet_ids_.assign(tablet_array))) {
        LOG_WARN("fail to get tablet ip addr", K(ret), K(tablet_array));
      } else {
        arg.tenant_id_ = tenant_id;
        arg.ls_id_ = ls_id;
        arg.snapshot_version_ = snapshot_version;
        if (OB_FAIL(proxy.call(dest_ip, rpc_timeout, tenant_id, arg))) {
          LOG_WARN("send rpc failed", K(ret), K(arg), K(dest_ip), K(tenant_id));
        }
      }
    }
    // handle batch result
    int tmp_ret = OB_SUCCESS;
    common::ObArray<int> return_ret_array;
    if (OB_SUCCESS != (tmp_ret = proxy.wait_all(return_ret_array))) {
      LOG_WARN("rpc proxy wait failed", K(tmp_ret));
      ret = OB_SUCCESS == ret ? tmp_ret : ret;
    } else if (OB_SUCC(ret)) {
      if (return_ret_array.count() != ip_tablets_map.size()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("rpc proxy rsp size not equal to send size", K(ret), K(return_ret_array.count()), K(ip_tablets_map.size()));
      } else {
        const ObIArray<const obrpc::ObDDLCheckTabletMergeStatusResult *> &result_array = proxy.get_results();
        // 1. handle every ip addr result
        for (int64_t i = 0; OB_SUCC(ret) && i < result_array.count(); i++) {
          int return_ret = return_ret_array.at(i);  // check return ret code
          if (OB_SUCCESS == return_ret) {
            const obrpc::ObDDLCheckTabletMergeStatusResult *cur_result = nullptr;  // ip tablets status result
            common::ObSArray<bool> tablet_rsp_array;
            common::ObArray<ObTabletID> tablet_req_array;
            const common::ObAddr &tablet_addr = proxy.get_dests().at(i); // get rpc dest addr

            if (OB_ISNULL(cur_result = result_array.at(i))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("merge status result is null.", K(ret), K(cur_result));
            } else if (FALSE_IT(tablet_rsp_array = cur_result->merge_status_)) {
            } else if (OB_FAIL(ip_tablets_map.get_refactored(tablet_addr, tablet_req_array))) {
              LOG_WARN("get from ip tablet map fail.", K(ret), K(tablet_addr));
            } else if (tablet_req_array.count() != tablet_rsp_array.count()) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("tablet req count is not equal to tablet rsp count", K(ret), K(tablet_req_array), K(tablet_rsp_array));
            } else {
              // 2. handle every tablet status
              for (int64_t idx = 0; OB_SUCC(ret) && idx < tablet_rsp_array.count(); ++idx) {
                const common::ObTabletID &tablet_id = tablet_req_array.at(idx); // tablet id
                const bool tablet_status = tablet_rsp_array.at(idx);
                if (OB_FAIL(update_replica_merge_status(tablet_id, tablet_status, tablets_commited_map))) { // update tablet merge status from get
                  LOG_WARN("fail to update replica merge status", K(ret), K(tablet_id), K(tablet_addr));
                } else {
                  LOG_INFO("succ to update replica merge status", K(tablet_addr), K(tablet_id), K(tablet_status));
                }
              }
            }
          } else {
            LOG_WARN("rpc proxy return fail.", K(return_ret));
          }
        }
        // 3. check any commit tablet
        if (OB_SUCC(ret)) {
          int64_t build_succ_count = 0;
          if (OB_FAIL(calculate_build_finish(tenant_id, tablet_ids, tablets_commited_map, build_succ_count))) {
            LOG_WARN("check and commit tbalets commit log fail.", K(ret), K(tablet_ids), K(build_succ_count));
          } else {
            DEBUG_SYNC(DDL_CHECK_TABLET_MERGE_STATUS);
            tablet_build_succ_count += build_succ_count;
          }
        }
      }
    }
  }
  return ret;
}

int ObCheckTabletDataComplementOp::check_tablet_merge_status(
  const uint64_t tenant_id,
  const ObIArray<common::ObTabletID> &tablet_ids,
  const int64_t snapshot_version,
  bool &is_all_tablets_commited)
{
  int ret = OB_SUCCESS;
  is_all_tablets_commited = false;

  hash::ObHashMap<ObAddr, ObArray<ObTabletID>> ip_tablets_map; // use for classify tablet replica addr
  hash::ObHashMap<ObLSID, ObArray<ObTabletID>> ls_tablets_map; // use for classify tablet ls
  hash::ObHashMap<ObTabletID, int32_t> tablets_commited_map;

  const static int64_t max_map_hash_bucket = tablet_ids.count();

  if (OB_UNLIKELY( tablet_ids.count() <= 0 || OB_INVALID_ID == tenant_id || OB_INVALID_TIMESTAMP == snapshot_version)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tablet_ids.count()), K(tenant_id), K(snapshot_version));
  } else if (OB_FAIL(ip_tablets_map.create(max_map_hash_bucket, "DdlTablet"))) {
    LOG_WARN("fail to create ip_tablets_map", K(ret));
  } else if (OB_FAIL(ls_tablets_map.create(max_map_hash_bucket, "DdlTablet"))) {
    LOG_WARN("fail to create ls_tablets_map", K(ret));
  } else if (OB_FAIL(tablets_commited_map.create(max_map_hash_bucket, "DdlTablet"))){
    LOG_WARN("fail to create tablets_commited_map", K(ret));
  } else {
    const static int64_t batch_size = 100;  // batch tablet number
    int64_t total_build_succ_count = 0;
    int64_t one_batch_build_succ_count = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids.count(); ++i) {
      const ObTabletID &tablet_id = tablet_ids.at(i);
      if (OB_FAIL(construct_ls_tablet_map(tenant_id, tablet_id, ls_tablets_map))) {
        LOG_WARN("construct_tablet_ls_map fail", K(ret), K(tenant_id), K(tablet_id));
      } else {
        if ((i != 0 && i % batch_size == 0) /* reach batch size */ || i == tablet_ids.count() - 1 /* reach end */) {
          for (hash::ObHashMap<ObLSID, ObArray<ObTabletID>>::const_iterator ls_iter = ls_tablets_map.begin();
            ls_iter != ls_tablets_map.end() && OB_SUCC(ret); ++ls_iter) {
            const ObLSID &ls_id = ls_iter->first;
            const ObArray<ObTabletID> &tablet_array = ls_iter->second;
            if (OB_FAIL(do_check_tablets_merge_status(tenant_id,
                                                      snapshot_version,
                                                      tablet_array,
                                                      ls_id,
                                                      ip_tablets_map,
                                                      tablets_commited_map,
                                                      one_batch_build_succ_count))) {
              LOG_WARN("do check tablets merge status fail", K(ret));
            } else {
              total_build_succ_count += one_batch_build_succ_count;
            }
          }
          ls_tablets_map.reuse(); // reuse map
        }
      }
    }
    int64_t total_tablets_count = tablet_ids.count();
    if (total_build_succ_count == total_tablets_count) {
      is_all_tablets_commited = true;
      LOG_INFO("all tablet finished create sstables", K(ret), K(total_tablets_count), K(total_build_succ_count));
    } else {
      LOG_WARN("not all tablets finished create sstables", K(ret), K(total_tablets_count), K(total_build_succ_count));
    }
  }

  ip_tablets_map.destroy();
  ls_tablets_map.destroy();
  tablets_commited_map.destroy();

  return ret;
}

int ObCheckTabletDataComplementOp::check_tablet_checksum_update_status(
  const uint64_t tenant_id,
  const uint64_t index_table_id,
  const uint64_t ddl_task_id,
  const int64_t execution_id,
  ObIArray<ObTabletID> &tablet_ids,
  bool &is_checksums_all_report)
{
  int ret = OB_SUCCESS;
  is_checksums_all_report = false;
  common::hash::ObHashMap<uint64_t, bool> tablet_checksum_status_map;
  int64_t tablet_count = tablet_ids.count();

  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || OB_INVALID_ID == index_table_id ||
      execution_id < 0 || tablet_count <= 0 || ddl_task_id == OB_INVALID_ID)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to check and wait complement task",
      K(ret), K(tenant_id), K(index_table_id), K(tablet_ids), K(execution_id), K(ddl_task_id));
  } else if (OB_FAIL(tablet_checksum_status_map.create(tablet_count, ObModIds::OB_SSTABLE_CREATE_INDEX))) {
    LOG_WARN("fail to create column checksum map", K(ret));
  } else if (OB_FAIL(ObDDLChecksumOperator::get_tablet_checksum_record(
      tenant_id,
      execution_id,
      index_table_id,
      ddl_task_id,
      tablet_ids,
      GCTX.root_service_->get_sql_proxy(),
      tablet_checksum_status_map))) {
    LOG_WARN("fail to get tablet checksum status",
      K(ret), K(tenant_id), K(execution_id), K(index_table_id), K(ddl_task_id));
  } else {
    int64_t report_checksum_cnt = 0;
    int64_t tablet_idx = 0;
    for (tablet_idx = 0; OB_SUCC(ret) && tablet_idx < tablet_count; ++tablet_idx) {
      const ObTabletID &tablet_id = tablet_ids.at(tablet_idx);
      uint64_t tablet_id_id = tablet_id.id();
      bool status = false;
      if (OB_FAIL(tablet_checksum_status_map.get_refactored(tablet_id_id, status))) {
        LOG_WARN("fail to get tablet checksum record from map", K(ret), K(tablet_id_id));
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          break;
        }
      } else if (!status) {
        break;
      } else {
        report_checksum_cnt++;
      }
    }
    if (OB_SUCC(ret)) {
      if (report_checksum_cnt == tablet_count) {
        is_checksums_all_report = true;
      } else {
        is_checksums_all_report = false;
        LOG_INFO("not all tablet has update checksum",
          K(ret), K(tablet_idx), K(tablet_count), K(is_checksums_all_report));
      }
    }
  }
  if (tablet_checksum_status_map.created()) {
    tablet_checksum_status_map.destroy();
  }
  return ret;
}

/*
 * 1. get a batch of tablets and construct a tmp ls_tablet_map
 * 2. get tablet_ip_map and send async batch rpc and get results
 * 3. push every tablet result to tablet_result_array
 * 4. check result and find finished tablets
 */
int ObCheckTabletDataComplementOp::check_all_tablet_sstable_status(
    const uint64_t tenant_id,
    const uint64_t index_table_id,
    const int64_t snapshot_version,
    const int64_t execution_id,
    const uint64_t ddl_task_id,
    bool &is_all_sstable_build_finished)
{
  int ret = OB_SUCCESS;
  ObArray<ObTabletID> dest_tablet_ids;
  bool is_checksums_all_report = false;
  is_all_sstable_build_finished = false;

  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || OB_INVALID_ID == index_table_id || OB_INVALID_TIMESTAMP == snapshot_version ||
      ddl_task_id == OB_INVALID_ID || execution_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to check and wait complement task", K(ret), K(tenant_id), K(index_table_id), K(snapshot_version), K(execution_id), K(ddl_task_id));
  } else if (OB_FAIL(ObDDLUtil::get_tablets(tenant_id, index_table_id, dest_tablet_ids))) {
    LOG_WARN("fail to get tablets", K(ret), K(tenant_id), K(index_table_id));
  } else if (OB_FAIL(check_tablet_merge_status(tenant_id, dest_tablet_ids, snapshot_version, is_all_sstable_build_finished))){
    LOG_WARN("fail to check tablet merge status.", K(ret), K(tenant_id), K(dest_tablet_ids), K(snapshot_version));
  } else {
    if (is_all_sstable_build_finished) {
      if (OB_FAIL(check_tablet_checksum_update_status(tenant_id, index_table_id, ddl_task_id, execution_id, dest_tablet_ids, is_checksums_all_report))) {
        LOG_WARN("fail to check tablet checksum update status.", K(ret), K(tenant_id), K(dest_tablet_ids), K(execution_id));
      }
      is_all_sstable_build_finished &= is_checksums_all_report;
    }
  }
  return ret;
}

int ObCheckTabletDataComplementOp::check_finish_report_checksum(
  const uint64_t tenant_id,
  const uint64_t index_table_id,
  const int64_t execution_id,
  const uint64_t ddl_task_id)
{
  int ret = OB_SUCCESS;
  bool is_checksums_all_report = false;
  ObArray<ObTabletID> dest_tablet_ids;

  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || OB_INVALID_ID == index_table_id ||
      ddl_task_id == OB_INVALID_ID || execution_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to check report checksum finished", K(ret), K(tenant_id), K(index_table_id), K(execution_id), K(ddl_task_id));
  } else if (OB_FAIL(ObDDLUtil::get_tablets(tenant_id, index_table_id, dest_tablet_ids))) {
    LOG_WARN("fail to get tablets", K(ret), K(tenant_id), K(index_table_id));
  } else if (OB_FAIL(check_tablet_checksum_update_status(tenant_id, index_table_id, ddl_task_id, execution_id, dest_tablet_ids, is_checksums_all_report))) {
    LOG_WARN("fail to check tablet checksum update status, maybe EAGAIN", K(ret), K(tenant_id), K(dest_tablet_ids), K(execution_id));
  } else if (!is_checksums_all_report) {
    ret = OB_EAGAIN;
    LOG_WARN("tablets checksum not all report!", K(is_checksums_all_report), K(ret));
  }
  return ret;
}

/*
 * This func is used to check duplicate data completement inner sql
 * if has running inner sql, we should wait until finished. But
 * if not has running inner sql, we should found if all tablet sstable
 * has builded already. If not all builded and no inner sql running, or
 * error case happen, we still execute new inner sql outside.
 */
int ObCheckTabletDataComplementOp::check_and_wait_old_complement_task(
    const uint64_t tenant_id,
    const uint64_t table_id,
    const int64_t ddl_task_id,
    const int64_t execution_id,
    const common::ObAddr &inner_sql_exec_addr,
    const common::ObCurTraceId::TraceId &trace_id,
    const int64_t schema_version,
    const int64_t scn,
    bool &need_exec_new_inner_sql)
{
  int ret = OB_SUCCESS;
  need_exec_new_inner_sql = true; // default need execute new inner sql
  bool is_old_task_session_exist = true;
  bool is_dst_checksums_all_report = false;

  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || OB_INVALID_ID == table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to check and wait complement task", K(ret), K(tenant_id), K(table_id));
  } else {
    if (OB_FAIL(check_task_inner_sql_session_status(inner_sql_exec_addr, trace_id, tenant_id, ddl_task_id, scn, is_old_task_session_exist))) {
      LOG_WARN("fail check task inner sql session status", K(ret), K(trace_id), K(inner_sql_exec_addr));
    } else if (is_old_task_session_exist) {
      ret = OB_EAGAIN;
    } else {
      LOG_INFO("old inner sql session is not exist.", K(ret));
    }

    // After old session exits, the rule of retry is specified as follows
    //
    // A. for dst table merge checksums of this execution,
    // - if complete, goto B (need_exec_new_inner_sql = false)
    // - else if all tablets has been merged, this means some checksum report failed, retry
    // - else old session must fail/crash, retry
    //
    // B. do checksum validation against src table scan checksums of this execution,
    // - if src checksums are complete, this is exactly a validation
    // - else old session must fail/crash "unexpectedly" (because complete dst checksum in A
    //   guarantees at least one preivous execution has successfully finished table scan),
    //   the validation may returns error due to lack of src checksum records

    ObArray<ObTabletID> dest_tablet_ids;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObDDLUtil::get_tablets(tenant_id, table_id, dest_tablet_ids))) {
      LOG_WARN("fail to get tablets", K(ret), K(tenant_id), K(table_id));
    } else if (OB_FAIL(check_tablet_checksum_update_status(tenant_id, table_id, ddl_task_id, execution_id, dest_tablet_ids, is_dst_checksums_all_report))) {
      LOG_WARN("fail to check tablet checksum update status.", K(ret), K(tenant_id), K(dest_tablet_ids), K(execution_id));
    } else if (is_dst_checksums_all_report) {
      need_exec_new_inner_sql = false;
      LOG_INFO("no need execute because all tablet sstable has build finished", K(need_exec_new_inner_sql));
    }
  }
  if (OB_EAGAIN != ret) {
    LOG_INFO("end to check and wait complement task", K(ret),
      K(table_id), K(is_old_task_session_exist), K(is_dst_checksums_all_report), K(need_exec_new_inner_sql));
  }
  return ret;
}
