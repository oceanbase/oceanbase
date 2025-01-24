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
#include "share/ob_ddl_checksum.h"
#include "share/ob_ddl_sim_point.h"
#include "sql/engine/table/ob_table_scan_op.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "rootserver/ob_root_service.h"
#include "storage/tx_storage/ob_ls_service.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "close_modules/shared_storage/meta_store/ob_shared_storage_obj_meta.h"
#include "storage/meta_store/ob_tenant_storage_meta_service.h"
#include "close_modules/shared_storage/share/compaction/ob_shared_storage_compaction_util.h"
#endif

using namespace oceanbase::share;
using namespace oceanbase::common;
using namespace oceanbase::share::schema;
using namespace oceanbase::obrpc;
using namespace oceanbase::sql;

const char *oceanbase::share::get_ddl_type(ObDDLType ddl_type)
{
  const char *ret_name = "UNKNOWN_DDL_TYPE";
  switch (ddl_type) {
    case ObDDLType::DDL_INVALID:
      ret_name = "DDL_INVALID";
      break;
    case ObDDLType::DDL_CHECK_CONSTRAINT:
      ret_name = "DDL_CHECK_CONSTRAINT";
      break;
    case ObDDLType::DDL_FOREIGN_KEY_CONSTRAINT:
      ret_name = "DDL_FOREIGN_KEY_CONSTRAINT";
      break;
    case ObDDLType::DDL_ADD_NOT_NULL_COLUMN:
      ret_name = "DDL_ADD_NOT_NULL_COLUMN";
      break;
    case ObDDLType::DDL_MODIFY_AUTO_INCREMENT:
      ret_name = "DDL_MODIFY_AUTO_INCREMENT";
      break;
    case ObDDLType::DDL_CREATE_INDEX:
      ret_name = "DDL_CREATE_INDEX";
      break;
    case ObDDLType::DDL_DROP_INDEX:
      ret_name = "DDL_DROP_INDEX";
      break;
    case ObDDLType::DDL_CREATE_FTS_INDEX:
      ret_name = "DDL_CREATE_FTS_INDEX";
      break;
    case ObDDLType::DDL_CREATE_MLOG:
      ret_name = "DDL_CREATE_MLOG";
      break;
    case ObDDLType::DDL_DROP_MLOG:
      ret_name = "DDL_DROP_MLOG";
      break;
    case ObDDLType::DDL_CREATE_PARTITIONED_LOCAL_INDEX:
      ret_name = "DDL_CREATE_PARTITIONED_LOCAL_INDEX";
      break;
    case ObDDLType::DDL_DROP_LOB:
      ret_name = "DDL_DROP_LOB";
      break;
    case ObDDLType::DDL_DROP_FTS_INDEX:
      ret_name = "DDL_DROP_FTS_INDEX";
      break;
    case ObDDLType::DDL_DROP_MULVALUE_INDEX:
      ret_name = "DDL_DROP_MULVALUE_INDEX";
      break;
    case ObDDLType::DDL_DROP_VEC_INDEX:
      ret_name = "DDL_DROP_VEC_INDEX";
      break;
    case ObDDLType::DDL_CREATE_VEC_INDEX:
      ret_name = "DDL_CREATE_VEC_INDEX";
      break;
    case ObDDLType::DDL_CREATE_MULTIVALUE_INDEX:
      ret_name = "DDL_CREATE_MULTIVALUE_INDEX";
      break;
    case ObDDLType::DDL_REBUILD_INDEX:
      ret_name = "DDL_REBUILD_INDEX";
      break;
    case ObDDLType::DDL_AUTO_SPLIT_BY_RANGE:
      ret_name = "DDL_AUTO_SPLIT_BY_RANGE";
      break;
    case ObDDLType::DDL_AUTO_SPLIT_NON_RANGE:
      ret_name = "DDL_AUTO_SPLIT_NON_RANGE";
      break;
    case ObDDLType::DDL_MANUAL_SPLIT_BY_RANGE:
      ret_name = "DDL_MANUAL_SPLIT_BY_RANGE";
      break;
    case ObDDLType::DDL_MANUAL_SPLIT_NON_RANGE:
      ret_name = "DDL_MANUAL_SPLIT_NON_RANGE";
      break;
    case ObDDLType::DDL_DROP_SCHEMA_AVOID_CONCURRENT_TRANS:
      ret_name = "DDL_DROP_SCHEMA_AVOID_CONCURRENT_TRANS";
      break;
    case ObDDLType::DDL_DROP_DATABASE:
      ret_name = "DDL_DROP_DATABASE";
      break;
    case ObDDLType::DDL_DROP_TABLE:
      ret_name = "DDL_DROP_TABLE";
    case ObDDLType::DDL_TRUNCATE_TABLE:
      ret_name = "DDL_TRUNCATE_TABLE";
      break;
    case ObDDLType::DDL_DROP_PARTITION:
      ret_name = "DDL_DROP_PARTITION";
      break;
    case ObDDLType::DDL_DROP_SUB_PARTITION:
      ret_name = "DDL_DROP_SUB_PARTITION";
      break;
    case ObDDLType::DDL_TRUNCATE_PARTITION:
      ret_name = "DDL_TRUNCATE_PARTITION";
      break;
    case ObDDLType::DDL_TRUNCATE_SUB_PARTITION:
      ret_name = "DDL_TRUNCATE_SUB_PARTITION";
      break;
    case ObDDLType::DDL_RENAME_PARTITION:
      ret_name = "DDL_RENAME_PARTITION";
      break;
    case ObDDLType::DDL_RENAME_SUB_PARTITION:
      ret_name = "DDL_RENAME_SUB_PARTITION";
      break;
    case ObDDLType::DDL_DOUBLE_TABLE_OFFLINE:
      ret_name = "DDL_DOUBLE_TABLE_OFFLINE";
      break;
    case ObDDLType::DDL_MODIFY_COLUMN:
      ret_name = "DDL_MODIFY_COLUMN";
      break;
    case ObDDLType::DDL_ADD_PRIMARY_KEY:
      ret_name = "DDL_ADD_PRIMARY_KEY";
      break;
    case ObDDLType::DDL_DROP_PRIMARY_KEY:
      ret_name = "DDL_DROP_PRIMARY_KEY";
      break;
    case ObDDLType::DDL_ALTER_PRIMARY_KEY:
      ret_name = "DDL_ALTER_PRIMARY_KEY";
      break;
    case ObDDLType::DDL_ALTER_PARTITION_BY:
      ret_name = "DDL_ALTER_PARTITION_BY";
      break;
    case ObDDLType::DDL_DROP_COLUMN:
      ret_name = "DDL_DROP_COLUMN";
      break;
    case ObDDLType::DDL_CONVERT_TO_CHARACTER:
      ret_name = "DDL_CONVERT_TO_CHARACTER";
      break;
    case ObDDLType::DDL_ADD_COLUMN_OFFLINE:
      ret_name = "DDL_ADD_COLUMN_OFFLINE";
      break;
    case ObDDLType::DDL_COLUMN_REDEFINITION:
      ret_name = "DDL_COLUMN_REDEFINITION";
      break;
    case ObDDLType::DDL_TABLE_REDEFINITION:
      ret_name = "DDL_TABLE_REDEFINITION";
      break;
    case ObDDLType::DDL_DIRECT_LOAD:
      ret_name = "DDL_DIRECT_LOAD";
      break;
    case ObDDLType::DDL_DIRECT_LOAD_INSERT:
      ret_name = "DDL_DIRECT_LOAD_INSERT";
      break;
    case ObDDLType::DDL_TABLE_RESTORE:
      ret_name = "DDL_TABLE_RESTORE";
      break;
    case ObDDLType::DDL_MVIEW_COMPLETE_REFRESH:
      ret_name = "DDL_MVIEW_COMPLETE_REFRESH";
      break;
    case ObDDLType::DDL_CREATE_MVIEW:
      ret_name = "DDL_CREATE_MVIEW";
      break;
    case ObDDLType::DDL_ALTER_COLUMN_GROUP:
      ret_name = "DDL_ALTER_COLUMN_GROUP";
      break;
    case ObDDLType::DDL_MODIFY_AUTO_INCREMENT_WITH_REDEFINITION:
      ret_name = "DDL_MODIFY_AUTO_INCREMENT_WITH_REDEFINITION";
      break;
    case ObDDLType::DDL_PARTITION_SPLIT_RECOVERY_TABLE_REDEFINITION:
      ret_name = "DDL_PARTITION_SPLIT_RECOVERY_TABLE_REDEFINITION";
      break;
    case ObDDLType::DDL_NORMAL_TYPE:
      ret_name = "DDL_NORMAL_TYPE";
      break;
    case ObDDLType::DDL_ADD_COLUMN_ONLINE:
      ret_name = "DDL_ADD_COLUMN_ONLINE";
      break;
    case ObDDLType::DDL_CHANGE_COLUMN_NAME:
      ret_name = "DDL_CHANGE_COLUMN_NAME";
      break;
    case ObDDLType::DDL_DROP_COLUMN_INSTANT:
      ret_name = "DDL_DROP_COLUMN_INSTANT";
      break;
    case ObDDLType::DDL_ALTER_PARTITION_AUTO_SPLIT_ATTRIBUTE:
      ret_name = "DDL_ALTER_PARTITION_AUTO_SPLIT_ATTRIBUTE";
      break;
    case ObDDLType::DDL_ADD_COLUMN_INSTANT:
      ret_name = "DDL_ADD_COLUMN_INSTANT";
      break;
    case ObDDLType::DDL_COMPOUND_INSTANT:
      ret_name = "DDL_COMPOUND_INSTANT";
      break;
    case ObDDLType::DDL_ALTER_COLUMN_GROUP_DELAYED:
      ret_name = "DDL_ALTER_COLUMN_GROUP_DELAYED";
      break;
    default:
      break;
  }
  return ret_name;
}

int ObColumnNameMap::init(const ObTableSchema &orig_table_schema,
                          const ObTableSchema &new_table_schema,
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
      } else if (column->is_unused()) {
        // unused column, extra column compared to the hidden table.
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
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(init_xml_hidden_column_name_map(orig_table_schema, new_table_schema))) {
    LOG_WARN("failed to init xml hidden column name map", K(ret));
  }
  return ret;
}

int ObColumnNameMap::init_xml_hidden_column_name_map(const ObTableSchema &orig_table_schema,
                                                     const ObTableSchema &new_table_schema)
{
  int ret = OB_SUCCESS;
  lib::CompatModeGuard guard(compat_mode_);
  for (ObTableSchema::const_column_iterator it = orig_table_schema.column_begin();
      OB_SUCC(ret) && it != orig_table_schema.column_end(); it++) {
    ObColumnSchemaV2 *column = *it;
    if (OB_ISNULL(column)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid column", K(ret));
    } else if (column->is_xmltype()) {
      ObString new_column_name;
      const ObColumnSchemaV2 *new_column = nullptr;
      if (OB_FAIL(get(column->get_column_name_str(), new_column_name))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to get new xml column name", K(ret));
        }
      } else if (OB_ISNULL(new_column = new_table_schema.get_column_schema(new_column_name))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to find new column", K(ret), K(new_column_name), K(new_table_schema));
      } else if (new_column->is_xmltype()) {
        const uint64_t orig_column_id = column->get_column_id();
        const uint64_t orig_udt_set_id = column->get_udt_set_id();
        const uint64_t new_column_id = new_column->get_column_id();
        const uint64_t new_udt_set_id = new_column->get_udt_set_id();
        ObColumnSchemaV2 *orig_xml_hidden_column = nullptr;
        ObColumnSchemaV2 *new_xml_hidden_column = nullptr;
        if (OB_UNLIKELY(orig_udt_set_id <= 0 || new_udt_set_id <= 0)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid udt set id for xml column", K(ret), KPC(column), KPC(new_column));
        } else if (OB_ISNULL(orig_xml_hidden_column = orig_table_schema.get_xml_hidden_column_schema(orig_column_id, orig_udt_set_id))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("orig xml hidden column not found", K(ret), K(orig_column_id), K(orig_udt_set_id));
        } else if (OB_ISNULL(new_xml_hidden_column = new_table_schema.get_xml_hidden_column_schema(new_column_id, new_udt_set_id))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("new xml hidden column not found", K(ret), K(new_column_id), K(new_udt_set_id));
        } else {
          const ObString &orig_xml_hidden_column_name = orig_xml_hidden_column->get_column_name_str();
          const ObString &new_xml_hidden_column_name = new_xml_hidden_column->get_column_name_str();
          if (orig_xml_hidden_column_name != new_xml_hidden_column_name) {
            if (OB_FAIL(col_name_map_.erase_refactored(ObColumnNameHashWrapper(orig_xml_hidden_column_name)))) {
              LOG_WARN("failed to erase col name map", K(ret));
            } else if (OB_FAIL(set(orig_xml_hidden_column_name, new_xml_hidden_column_name))) {
              LOG_WARN("failed to set column name map", K(ret));
            }
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
    LOG_WARN("get table schema failed", K(ret), K(tenant_id), K(table_id));
  } else if (OB_FAIL(table_schema->get_tablet_ids(tablet_ids))) {
    LOG_WARN("get tablets failed", K(ret), KPC(table_schema));
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

int ObDDLUtil::get_all_indexes_tablets_count(
    ObSchemaGetterGuard &schema_guard,
    const uint64_t tenant_id,
    const uint64_t data_table_id,
    int64_t &all_tablet_count)
{
  int ret = OB_SUCCESS;
  all_tablet_count = 0;
  const ObTableSchema *data_table_schema = nullptr;
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || OB_INVALID_ID == data_table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, data_table_id, data_table_schema))) {
    LOG_WARN("get table schema failed", K(ret), K(tenant_id), K(data_table_id));
  } else if (OB_ISNULL(data_table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table not exist", K(ret), K(tenant_id), K(data_table_id));
  } else {
    const common::ObIArray<ObAuxTableMetaInfo> &index_infos = data_table_schema->get_simple_index_infos();
    for (int64_t i = 0; OB_SUCC(ret) && i < index_infos.count(); i++) {
      const uint64_t index_tid = index_infos.at(i).table_id_;
      const ObTableSchema *index_schema = nullptr;
      if (OB_FAIL(schema_guard.get_table_schema(tenant_id, index_tid, index_schema))) {
        LOG_WARN("get table schema failed", K(ret), K(tenant_id), K(index_tid));
      } else if (OB_ISNULL(index_schema)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("get table schema failed", K(ret), K(tenant_id), K(index_tid));
      } else {
        all_tablet_count += index_schema->get_all_part_num();
      }
    }
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
    bool append_comma = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < order_column_ids.count(); ++i) {
      for (int64_t j = 0; OB_SUCC(ret) && j < select_column_ids.count(); ++j) {
        if (select_column_ids.at(j) == order_column_ids.at(i)) {
          if (OB_FAIL(sql_string.append_fmt("%s %ld", append_comma ? ",": "", j + 1))) {
            LOG_WARN("append fmt failed", K(ret));
          } else if (!append_comma) {
            append_comma = true;
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

int ObDDLUtil::generate_mview_ddl_schema_hint_str(
    const uint64_t tenant_id,
    const uint64_t mview_table_id,
    share::schema::ObSchemaGetterGuard &schema_guard,
    const ObIArray<ObBasedSchemaObjectInfo> &based_schema_object_infos,
    const bool is_oracle_mode,
    ObSqlString &sql_string)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("ObDDLTmp");
  ObString database_name;
  ObString table_name;
  for (int64_t i = 0; OB_SUCC(ret) && i < based_schema_object_infos.count(); ++i) {
    const ObBasedSchemaObjectInfo &based_info = based_schema_object_infos.at(i);
    const ObTableSchema *table_schema = nullptr;
    const ObDatabaseSchema *database_schema = nullptr;
    database_name.reset();
    table_name.reset();
    allocator.reuse();
    if (OB_FAIL(schema_guard.get_table_schema(tenant_id, based_info.schema_id_, table_schema))) {
      LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(based_info));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("table not exist", KR(ret), K(tenant_id), K(based_info));
    } else if (OB_FAIL(schema_guard.get_database_schema(tenant_id, table_schema->get_database_id(),
                                                        database_schema))) {
      LOG_WARN("fail to get database schema", KR(ret), K(tenant_id),
               K(table_schema->get_database_id()));
    } else if (OB_ISNULL(database_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, database schema must not be nullptr", KR(ret));
    } else if (OB_FAIL(sql::ObSQLUtils::generate_new_name_with_escape_character(
                 allocator, database_schema->get_database_name_str(), database_name,
                 is_oracle_mode))) {
      LOG_WARN("fail to generate new name with escape character", KR(ret),
               K(database_schema->get_database_name_str()), K(is_oracle_mode));
    } else if (OB_FAIL(sql::ObSQLUtils::generate_new_name_with_escape_character(
                 allocator, table_schema->get_table_name_str(), table_name, is_oracle_mode))) {
      LOG_WARN("fail to generate new name with escape character", KR(ret),
               K(table_schema->get_table_name_str()), K(is_oracle_mode));
    } else if (is_oracle_mode) {
      if (OB_FAIL(sql_string.append_fmt("ob_ddl_schema_version(\"%.*s\".\"%.*s\", %ld) ",
                                        static_cast<int>(database_name.length()), database_name.ptr(),
                                        static_cast<int>(table_name.length()), table_name.ptr(),
                                        based_info.schema_version_))) {
        LOG_WARN("append sql string failed", KR(ret));
      }
    } else {
      if (OB_FAIL(sql_string.append_fmt("ob_ddl_schema_version(`%.*s`.`%.*s`, %ld) ",
                                        static_cast<int>(database_name.length()), database_name.ptr(),
                                        static_cast<int>(table_name.length()), table_name.ptr(),
                                        based_info.schema_version_))) {
        LOG_WARN("append sql string failed", KR(ret));
      }
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
        } else if (is_contain(select_column_ids, col_id)) {
          // do nothing
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


int ObDDLUtil::append_multivalue_extra_column(const ObTableSchema &dest_table_schema,
                                              const share::schema::ObTableSchema &source_table_schema,
                                              ObArray<ObColumnNameInfo> &column_names,
                                              ObArray<int64_t> &select_column_ids)
{
  int ret = OB_SUCCESS;
  if (dest_table_schema.is_multivalue_index_aux()) {
    ObArray<ObColDesc> column_ids;
    const ObColumnSchemaV2 *column_schema = nullptr;
    const ObColumnSchemaV2 *array_column = nullptr;
    // get dest table column names
    if (OB_FAIL(dest_table_schema.get_column_ids(column_ids))) {
      LOG_WARN("fail to get column ids", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < column_ids.count(); ++i) {
        const int64_t col_id =  column_ids.at(i).col_id_;
        if (OB_ISNULL(column_schema = source_table_schema.get_column_schema(col_id))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("error unexpected, column schema must not be nullptr", K(ret));
        } else if (column_schema->is_multivalue_generated_column()) {
          array_column = source_table_schema.get_column_schema(col_id + 1);
          break;
        }
      } // end for

      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(array_column)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, array column schema must not be nullptr", K(ret));
      } else {
        if (OB_FAIL(column_names.push_back(ObColumnNameInfo(array_column->get_column_name_str(), false)))) {
          LOG_WARN("push back rowkey column name failed", K(ret));
        } else if (OB_FAIL(select_column_ids.push_back(array_column->get_column_id()))) {
          LOG_WARN("push back select column id failed", K(ret));
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
    const ObString &partition_names,
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
  } else if (OB_FAIL(DDL_SIM(tenant_id, task_id, GENERATE_BUILD_REPLICA_SQL))) {
    LOG_WARN("ddl sim failure", K(ret), K(tenant_id), K(task_id));
  } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(
      tenant_id, schema_guard))) {
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
    const int64_t real_parallelism = ObDDLUtil::get_real_parallelism(parallelism, false/*is mv refresh*/);
    // get dest table column names
    if (OB_FAIL(dest_table_schema->get_column_ids(column_ids))) {
      LOG_WARN("fail to get column ids", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < column_ids.count(); ++i) {
        const ObColumnSchemaV2 *column_schema = nullptr;
        ObString orig_column_name;
        is_shadow_column = common::is_shadow_column(column_ids.at(i).col_id_);
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
    if (OB_SUCC(ret) && dest_table_schema->need_partition_key_for_build_local_index(*source_table_schema)) {
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

    if (OB_SUCC(ret) && dest_table_schema->is_multivalue_index_aux()
        && OB_FAIL(ObDDLUtil::append_multivalue_extra_column(*dest_table_schema, *source_table_schema, column_names, select_column_ids))) {
      LOG_WARN("fail append extra column", K(ret));
    }

    if (OB_SUCC(ret) && dest_table_schema->is_spatial_index()) {
      if (OB_FAIL(ObDDLUtil::generate_spatial_index_column_names(*dest_table_schema, *source_table_schema, insert_column_names,
                                                                 column_names, select_column_ids))) {
        LOG_WARN("generate spatial index column names failed", K(ret));
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
        } else if (FALSE_IT(is_shadow_column = common::is_shadow_column(rowkey_column->column_id_))) {
        } else if (FALSE_IT(col_id = is_shadow_column ? rowkey_column->column_id_ - OB_MIN_SHADOW_COLUMN_ID : rowkey_column->column_id_)) {
        } else if (OB_ISNULL(column_schema = dest_table_schema->get_column_schema(col_id))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("error unexpected, column schema must not be nullptr", K(ret), K(col_id));
        } else if (column_schema->is_generated_column() &&
          !dest_table_schema->is_spatial_index() &&
          !dest_table_schema->is_multivalue_index_aux()) {
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
        const char *io_read_hint = GCTX.is_shared_storage_mode() ? " opt_param('io_read_batch_size', '2M') opt_param('io_read_redundant_limit_percentage', 0) " : " ";
        if (dest_table_schema->is_vec_vid_rowkey_type()) {
          src_table_schema_version_hint_sql_string.reset();
        }
        if (OB_FAIL(ret)) {
        } else if (oracle_mode) {
          if (OB_FAIL(sql_string.assign_fmt("INSERT /*+ monitor enable_parallel_dml parallel(%ld) opt_param('ddl_execution_id', %ld) opt_param('ddl_task_id', %ld) opt_param('enable_newsort', 'false') %.*s use_px */INTO \"%.*s\".\"%.*s\" %.*s(%.*s) SELECT /*+ index(\"%.*s\" primary) %.*s */ %.*s from \"%.*s\".\"%.*s\" %.*s as of scn %ld %.*s",
              real_parallelism, execution_id, task_id,
              static_cast<int>(strlen(io_read_hint)), io_read_hint,
              static_cast<int>(new_dest_database_name.length()), new_dest_database_name.ptr(), static_cast<int>(new_dest_table_name.length()), new_dest_table_name.ptr(),
              static_cast<int>(partition_names.length()), partition_names.ptr(),
              static_cast<int>(insert_column_sql_string.length()), insert_column_sql_string.ptr(),
              static_cast<int>(new_source_table_name.length()), new_source_table_name.ptr(),
              static_cast<int>(src_table_schema_version_hint_sql_string.length()), src_table_schema_version_hint_sql_string.ptr(),
              static_cast<int>(query_column_sql_string.length()), query_column_sql_string.ptr(),
              static_cast<int>(new_source_database_name.length()), new_source_database_name.ptr(), static_cast<int>(new_source_table_name.length()), new_source_table_name.ptr(),
              static_cast<int>(partition_names.length()), partition_names.ptr(),
              snapshot_version, static_cast<int>(rowkey_column_sql_string.length()), rowkey_column_sql_string.ptr()))) {
            LOG_WARN("fail to assign sql string", K(ret));
          }
        } else {
          if (OB_FAIL(sql_string.assign_fmt("INSERT /*+ monitor enable_parallel_dml parallel(%ld) opt_param('ddl_execution_id', %ld) opt_param('ddl_task_id', %ld) opt_param('enable_newsort', 'false') %.*s use_px */INTO `%.*s`.`%.*s` %.*s(%.*s) SELECT /*+ index(`%.*s` primary) %.*s */ %.*s from `%.*s`.`%.*s` %.*s as of snapshot %ld %.*s",
              real_parallelism, execution_id, task_id,
              static_cast<int>(strlen(io_read_hint)), io_read_hint,
              static_cast<int>(new_dest_database_name.length()), new_dest_database_name.ptr(), static_cast<int>(new_dest_table_name.length()), new_dest_table_name.ptr(),
              static_cast<int>(partition_names.length()), partition_names.ptr(),
              static_cast<int>(insert_column_sql_string.length()), insert_column_sql_string.ptr(),
              static_cast<int>(new_source_table_name.length()), new_source_table_name.ptr(),
              static_cast<int>(src_table_schema_version_hint_sql_string.length()), src_table_schema_version_hint_sql_string.ptr(),
              static_cast<int>(query_column_sql_string.length()), query_column_sql_string.ptr(),
              static_cast<int>(new_source_database_name.length()), new_source_database_name.ptr(), static_cast<int>(new_source_table_name.length()), new_source_table_name.ptr(),
              static_cast<int>(partition_names.length()), partition_names.ptr(),
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

int ObDDLUtil::generate_build_mview_replica_sql(
    const uint64_t tenant_id,
    const int64_t mview_table_id,
    const int64_t container_table_id,
    ObSchemaGetterGuard &schema_guard,
    const int64_t snapshot_version,
    const int64_t execution_id,
    const int64_t task_id,
    const int64_t parallelism,
    const bool use_schema_version_hint_for_src_table,
    const ObIArray<ObBasedSchemaObjectInfo> &based_schema_object_infos,
    ObSqlString &sql_string)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || OB_INVALID_ID == mview_table_id ||
                  OB_INVALID_ID == container_table_id || snapshot_version <= 0 ||
                  execution_id < 0 || task_id <= 0 || based_schema_object_infos.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(tenant_id), K(mview_table_id), K(container_table_id),
             K(snapshot_version), K(execution_id), K(task_id), K(based_schema_object_infos));
  } else {
    const ObTableSchema *mview_table_schema = nullptr;
    const ObTableSchema *container_table_schema = nullptr;
    const ObDatabaseSchema *database_schema = nullptr;
    bool is_oracle_mode = false;
    if (OB_FAIL(schema_guard.get_table_schema(tenant_id, mview_table_id, mview_table_schema))) {
      LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(mview_table_id));
    } else if (OB_ISNULL(mview_table_schema)) {
      ret = OB_ERR_MVIEW_NOT_EXIST;
      LOG_WARN("fail to get mview table schema", KR(ret), K(mview_table_id));
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, container_table_id,
                                                     container_table_schema))) {
      LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(container_table_id));
    } else if (OB_ISNULL(container_table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("fail to get table schema", KR(ret), K(container_table_id));
    } else if (OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_table_id(
                 tenant_id, mview_table_id, is_oracle_mode))) {
      LOG_WARN("check if oracle mode failed", KR(ret), K(mview_table_id));
    } else if (OB_FAIL(schema_guard.get_database_schema(
                 tenant_id, mview_table_schema->get_database_id(), database_schema))) {
      LOG_WARN("fail to get database schema", KR(ret), K(tenant_id),
               K(mview_table_schema->get_database_id()));
    } else if (OB_ISNULL(database_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, database schema must not be nullptr", KR(ret));
    } else {
      ObArenaAllocator allocator("ObDDLTmp");
      ObString database_name;
      ObString container_table_name;
      ObSqlString src_table_schema_version_hint;
      ObSqlString rowkey_column_sql_string;
      if (OB_FAIL(sql::ObSQLUtils::generate_new_name_with_escape_character(
            allocator, database_schema->get_database_name_str(), database_name, is_oracle_mode))) {
        LOG_WARN("fail to generate new name with escape character", KR(ret),
                 K(database_schema->get_database_name_str()), K(is_oracle_mode));
      } else if (OB_FAIL(sql::ObSQLUtils::generate_new_name_with_escape_character(
                   allocator, container_table_schema->get_table_name_str(), container_table_name,
                   is_oracle_mode))) {
        LOG_WARN("fail to generate new name with escape character", KR(ret),
                 K(container_table_schema->get_table_name_str()), K(is_oracle_mode));
      } else if (use_schema_version_hint_for_src_table) {
        int64_t based_schema_version = OB_INVALID_VERSION;
        for (int64_t i = 0; OB_SUCC(ret) && i < based_schema_object_infos.count(); ++i) {
          const ObBasedSchemaObjectInfo &based_info = based_schema_object_infos.at(i);
          const ObTableSchema *based_table_schema = nullptr;
          if (OB_FAIL(schema_guard.get_table_schema(tenant_id, based_info.schema_id_,
                                                    based_table_schema))) {
            LOG_WARN("fail to get table schema", KR(ret), K(based_info));
          } else if (OB_ISNULL(based_table_schema)) {
            ret = OB_OLD_SCHEMA_VERSION;
            LOG_WARN("based table is not exist", KR(ret), K(based_info));
          } else if (OB_UNLIKELY(based_table_schema->get_schema_version() !=
                                 based_info.schema_version_)) {
            ret = OB_OLD_SCHEMA_VERSION;
            LOG_WARN("based table schema version is changed", KR(ret), K(based_info),
                     KPC(based_table_schema));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(generate_mview_ddl_schema_hint_str(
                     tenant_id, mview_table_id, schema_guard, based_schema_object_infos,
                     is_oracle_mode, src_table_schema_version_hint))) {
          LOG_WARN("failed to generated mview ddl schema hint", KR(ret));
        }
      }
      if (OB_SUCC(ret)) {
        const int64_t real_parallelism = ObDDLUtil::get_real_parallelism(parallelism, true/*is mv refresh*/);
        const ObString &select_sql_string = mview_table_schema->get_view_schema().get_view_definition_str();
        if (is_oracle_mode) {
          if (OB_FAIL(sql_string.assign_fmt("INSERT /*+ append monitor enable_parallel_dml parallel(%ld) opt_param('ddl_execution_id', %ld) opt_param('ddl_task_id', %ld) use_px */ INTO \"%.*s\".\"%.*s\""
                                            " SELECT /*+ %.*s */ * from (%.*s) as of scn %ld %.*s;",
              real_parallelism, execution_id, task_id,
              static_cast<int>(database_name.length()), database_name.ptr(),
              static_cast<int>(container_table_name.length()), container_table_name.ptr(),
              static_cast<int>(src_table_schema_version_hint.length()), src_table_schema_version_hint.ptr(),
              static_cast<int>(select_sql_string.length()), select_sql_string.ptr(),
              snapshot_version,
              static_cast<int>(rowkey_column_sql_string.length()), rowkey_column_sql_string.ptr()))) {
            LOG_WARN("fail to assign sql string", KR(ret));
          }
        } else {
          if (OB_FAIL(sql_string.assign_fmt("INSERT /*+ append monitor enable_parallel_dml parallel(%ld) opt_param('ddl_execution_id', %ld) opt_param('ddl_task_id', %ld) use_px */ INTO `%.*s`.`%.*s`"
                                            " SELECT /*+ %.*s */ * from (%.*s) as of snapshot %ld %.*s;",
              real_parallelism, execution_id, task_id,
              static_cast<int>(database_name.length()), database_name.ptr(),
              static_cast<int>(container_table_name.length()), container_table_name.ptr(),
              static_cast<int>(src_table_schema_version_hint.length()), src_table_schema_version_hint.ptr(),
              static_cast<int>(select_sql_string.length()), select_sql_string.ptr(),
              snapshot_version,
              static_cast<int>(rowkey_column_sql_string.length()), rowkey_column_sql_string.ptr()))) {
            LOG_WARN("fail to assign sql string", KR(ret));
          }
        }
      }
    }
    LOG_INFO("execute sql", K(sql_string));
  }
  return ret;
}

int ObDDLUtil::generate_order_by_str_for_mview(const ObTableSchema &container_table_schema,
                                               ObSqlString &rowkey_column_sql_string)
{
  int ret = OB_SUCCESS;
  rowkey_column_sql_string.reset();
  const ObRowkeyInfo &rowkey_info = container_table_schema.get_rowkey_info();
  if (container_table_schema.is_heap_table()) {
    /* do nothing */
  } else if (OB_UNLIKELY(rowkey_info.get_size() < 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpcted rowkey size", K(ret), K(rowkey_info.get_size()));
  } else if (OB_FAIL(rowkey_column_sql_string.append(" order by "))) {
    LOG_WARN("append failed", KR(ret));
  } else {
    uint64_t column_id = OB_INVALID_ID;
    int64_t sel_pos = 0;
    for (int col_idx = 0; OB_SUCC(ret) && col_idx < rowkey_info.get_size(); ++col_idx) {
      if (OB_FAIL(rowkey_info.get_column_id(col_idx, column_id))) {
        LOG_WARN("Failed to get column id", K(ret));
      } else if (OB_UNLIKELY(1 > (sel_pos = column_id - OB_APP_MIN_COLUMN_ID + 1))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpcted select position", K(ret), K(sel_pos), K(column_id));
      } else if (OB_FAIL(rowkey_column_sql_string.append_fmt("%s %ld", 0 != col_idx ? ",": "", sel_pos))) {
        LOG_WARN("append fmt failed", K(ret));
      }
    }
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

int ObDDLUtil::obtain_snapshot(
    const share::ObDDLTaskStatus next_task_status,
    const uint64_t table_id,
    const uint64_t target_table_id,
    int64_t &snapshot_version,
    rootserver::ObDDLTask* task,
    const common::ObIArray<common::ObTabletID> *extra_mv_tablet_ids)
{
  int ret = OB_SUCCESS;
  rootserver::ObDDLWaitTransEndCtx* wait_trans_ctx = nullptr;
  rootserver::ObRootService *root_service = GCTX.root_service_;
  if (OB_ISNULL(root_service)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, root service must not be nullptr", K(ret));
  } else if (OB_UNLIKELY(nullptr == task || snapshot_version != 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(task), K(snapshot_version));
  } else if (OB_ISNULL(wait_trans_ctx = task->get_wait_trans_ctx())) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("wait trans ctx is null", K(ret));
  } else if (!task->is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("args have not been inited", K(ret), K(wait_trans_ctx->is_inited()), K(task->is_inited()), K(task->get_task_type()));
  } else {
    ObDDLTaskStatus new_status = ObDDLTaskStatus::OBTAIN_SNAPSHOT;
    uint64_t tenant_id = task->get_src_tenant_id();
    int64_t new_fetched_snapshot = 0;
    int64_t persisted_snapshot = 0;
    if (!wait_trans_ctx->is_inited()) {
      if (OB_FAIL(wait_trans_ctx->init(tenant_id, task->get_task_id(), task->get_object_id(), rootserver::ObDDLWaitTransEndCtx::WAIT_SCHEMA_TRANS, task->get_src_schema_version()))) {
        LOG_WARN("fail to init wait trans ctx", K(ret));
      }
    } else {
      // to get snapshot version.
      bool is_trans_end = false;
      const bool need_wait_trans_end = false;
      if (OB_FAIL(wait_trans_ctx->try_wait(is_trans_end, new_fetched_snapshot, need_wait_trans_end))) {
        LOG_WARN("just to get snapshot rather than wait trans end", K(ret));
      }
      DEBUG_SYNC(DDL_REDEFINITION_HOLD_SNAPSHOT);
      // try hold snapshot
      if (OB_FAIL(ret)) {
      } else if (new_fetched_snapshot <= 0) {
        // the snapshot version obtained here must be valid.
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("snapshot version is invalid", K(ret), K(new_fetched_snapshot), KPC(wait_trans_ctx));
      } else {
        ObMySQLTransaction trans;
        if (OB_FAIL(trans.start(&root_service->get_sql_proxy(), tenant_id))) {
          LOG_WARN("fail to start trans", K(ret), K(tenant_id));
        } else if (OB_FAIL(rootserver::ObDDLTaskRecordOperator::update_snapshot_version_if_not_exist(trans,
                                                                    tenant_id,
                                                                    task->get_task_id(),
                                                                    new_fetched_snapshot,
                                                                    persisted_snapshot))) {
          LOG_WARN("update snapshot version failed", K(ret), K(task->get_task_id()), K(tenant_id), K(new_fetched_snapshot), K(persisted_snapshot));
        } else if (persisted_snapshot > 0) {
          // found a persisted snapshot, do not hold it again.
          FLOG_INFO("found a persisted snapshot in inner table", "task_id", task->get_task_id(), K(persisted_snapshot), K(new_fetched_snapshot));
        } else if (OB_FAIL(hold_snapshot(trans, task, table_id, target_table_id, root_service, new_fetched_snapshot, extra_mv_tablet_ids))) {
          if (OB_SNAPSHOT_DISCARDED == ret) {
            wait_trans_ctx->reset();
          } else {
            LOG_WARN("hold snapshot version failed", K(ret));
          }
        }
        if (trans.is_started()) {
          const bool need_commit = (ret == OB_SUCCESS);
          const int tmp_ret = trans.end(need_commit);
          if (OB_SUCCESS != tmp_ret) {
            LOG_WARN("fail to end trans", K(ret), K(tmp_ret), K(need_commit));
          } else if (need_commit) {
            // update when commit succ.
            snapshot_version = persisted_snapshot > 0 ? persisted_snapshot : new_fetched_snapshot;
          }
          ret = OB_SUCC(ret) ? tmp_ret : ret;
        }
      }

      if (OB_FAIL(ret)) {
        if (OB_SNAPSHOT_DISCARDED == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to obtain snapshot version", K(ret));
        }
      } else {
        new_status = next_task_status;
      }
    }
    if (new_status == next_task_status || OB_FAIL(ret)) {
      if (OB_FAIL(task->switch_status(new_status, true, ret))) {
        LOG_WARN("fail to switch task status", K(ret));
      }
    }
    task->add_event_info("obtain snapshot finish");
    LOG_INFO("obtain snapshot", K(ret), K(task->get_snapshot_version()), K(table_id), K(target_table_id), K(task->get_src_schema_version()), "ddl_event_info", ObDDLEventInfo(),
        K(persisted_snapshot), K(new_fetched_snapshot));
  }
  return ret;
}

int ObDDLUtil::hold_snapshot(
    common::ObMySQLTransaction &trans,
    rootserver::ObDDLTask* task,
    const uint64_t table_id,
    const uint64_t target_table_id,
    rootserver::ObRootService *root_service,
    const int64_t snapshot_version,
    const common::ObIArray<common::ObTabletID> *extra_mv_tablet_ids)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(task) || OB_ISNULL(root_service)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("invalid argument", K(ret), KP(task), KP(root_service));
  } else if (!task->is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("args have not been inited", K(ret), K(task->get_task_type()));
  } else {
    ObSEArray<ObTabletID, 1> tablet_ids;
    SCN snapshot_scn;
    ObSchemaGetterGuard schema_guard;
    const ObTableSchema *data_table_schema = nullptr;
    const ObTableSchema *dest_table_schema = nullptr;
    uint64_t tenant_id = task->get_src_tenant_id();
    int64_t schema_version = task->get_src_schema_version();
    ObMultiVersionSchemaService &schema_service = ObMultiVersionSchemaService::get_instance();
    if (OB_UNLIKELY(snapshot_version < 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments", K(ret), K(snapshot_version));
    } else if (OB_FAIL(DDL_SIM(tenant_id, task->get_task_id(), DDL_TASK_HOLD_SNAPSHOT_FAILED))) {
      LOG_WARN("ddl sim failure", K(ret), K(tenant_id), K(task->get_task_id()));
    } else if (OB_FAIL(snapshot_scn.convert_for_tx(snapshot_version))) {
      LOG_WARN("failed to convert", K(snapshot_version), K(ret));
    } else if (OB_FAIL(schema_service.get_tenant_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("get tenant schema guard failed", K(ret));
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id, data_table_schema))) {
      LOG_WARN("get table schema failed", K(ret), K(table_id));
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, target_table_id, dest_table_schema))) {
      LOG_WARN("get table schema failed", K(ret), K(target_table_id));
    } else if (OB_ISNULL(data_table_schema) || OB_ISNULL(dest_table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("table not exist", K(ret), K(table_id), K(target_table_id), KP(data_table_schema), KP(dest_table_schema));
    } else if (OB_FAIL(ObDDLUtil::get_tablets(tenant_id, table_id, tablet_ids))) {
      LOG_WARN("failed to get data table snapshot", K(ret), K(table_id));
    } else if (OB_FAIL(ObDDLUtil::get_tablets(tenant_id, target_table_id, tablet_ids))) {
      LOG_WARN("failed to get dest table snapshot", K(ret), K(target_table_id));
    } else if (data_table_schema->get_aux_lob_meta_tid() != OB_INVALID_ID &&
              OB_FAIL(ObDDLUtil::get_tablets(tenant_id, data_table_schema->get_aux_lob_meta_tid(), tablet_ids))) {
      LOG_WARN("failed to get data lob meta table snapshot", K(ret));
    } else if (data_table_schema->get_aux_lob_piece_tid() != OB_INVALID_ID &&
              OB_FAIL(ObDDLUtil::get_tablets(tenant_id, data_table_schema->get_aux_lob_piece_tid(), tablet_ids))) {
      LOG_WARN("failed to get data lob piece table snapshot", K(ret));
    } else if (dest_table_schema->get_aux_lob_meta_tid() != OB_INVALID_ID &&
              OB_FAIL(ObDDLUtil::get_tablets(tenant_id, dest_table_schema->get_aux_lob_meta_tid(), tablet_ids))) {
      LOG_WARN("failed to get dest lob meta table snapshot", K(ret));
    } else if (dest_table_schema->get_aux_lob_piece_tid() != OB_INVALID_ID &&
              OB_FAIL(ObDDLUtil::get_tablets(tenant_id, dest_table_schema->get_aux_lob_piece_tid(), tablet_ids))) {
      LOG_WARN("failed to get dest lob piece table snapshot", K(ret));
    } else {
      rootserver::ObDDLService &ddl_service = root_service->get_ddl_service();
      if (OB_FAIL(ddl_service.get_snapshot_mgr().batch_acquire_snapshot(
          trans, SNAPSHOT_FOR_DDL, tenant_id, schema_version, snapshot_scn, nullptr, tablet_ids))) {
        LOG_WARN("batch acquire snapshot failed", K(ret), K(tablet_ids));
      } else if (OB_NOT_NULL(extra_mv_tablet_ids) &&
                 !extra_mv_tablet_ids->empty() &&
                 OB_FAIL(ddl_service.get_snapshot_mgr().batch_acquire_snapshot(
                     trans, SNAPSHOT_FOR_MAJOR_REFRESH_MV, tenant_id, schema_version, snapshot_scn,
                     nullptr, *extra_mv_tablet_ids))) {
        LOG_WARN("batch acquire mv snapshot failed", K(ret), K(extra_mv_tablet_ids));
      }
    }
    task->add_event_info("hold snapshot finish");
    LOG_INFO("hold snapshot finished", K(ret), K(task->get_snapshot_version()), K(table_id), K(target_table_id), K(schema_version), "ddl_event_info", ObDDLEventInfo());
  }
  return ret;
}

int ObDDLUtil::release_snapshot(
    rootserver::ObDDLTask* task,
    const uint64_t table_id,
    const uint64_t target_table_id,
    const int64_t snapshot_version)
{
  int ret = OB_SUCCESS;
  rootserver::ObRootService *root_service = GCTX.root_service_;
  ObSEArray<ObTabletID, 1> tablet_ids;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *data_table_schema = nullptr;
  const ObTableSchema *dest_table_schema = nullptr;
  ObMultiVersionSchemaService &schema_service = ObMultiVersionSchemaService::get_instance();
  if (OB_ISNULL(root_service)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, root service must not be nullptr", K(ret));
  } else if (OB_ISNULL(task)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("invalid argument", K(ret));
  } else if (!task->is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("args have not been inited", K(ret), K(task->get_task_type()));
  } else {
    uint64_t tenant_id = task->get_src_tenant_id();
    int64_t schema_version = task->get_src_schema_version();
    if (OB_FAIL(DDL_SIM(tenant_id, task->get_task_id(), DDL_TASK_RELEASE_SNAPSHOT_FAILED))) {
      LOG_WARN("ddl sim failure", K(ret), K(tenant_id), K(task->get_task_id()));
    } else if (OB_FAIL(schema_service.get_tenant_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("get tenant schema guard failed", K(ret));
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id, data_table_schema))) {
      LOG_WARN("get table schema failed", K(ret), K(table_id));
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, target_table_id, dest_table_schema))) {
      LOG_WARN("get table schema failed", K(ret), K(target_table_id));
    }

    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(data_table_schema)) {
      LOG_INFO("table not exist", K(ret), K(table_id), K(target_table_id), KP(data_table_schema));
    } else if (OB_FAIL(ObDDLUtil::get_tablets(tenant_id, table_id, tablet_ids))) {
      LOG_WARN("failed to get data table snapshot", K(ret));
    } else if (data_table_schema->get_aux_lob_meta_tid() != OB_INVALID_ID &&
              OB_FAIL(ObDDLUtil::get_tablets(tenant_id, data_table_schema->get_aux_lob_meta_tid(), tablet_ids))) {
      LOG_WARN("failed to get data lob meta table snapshot", K(ret));
    } else if (data_table_schema->get_aux_lob_piece_tid() != OB_INVALID_ID &&
              OB_FAIL(ObDDLUtil::get_tablets(tenant_id, data_table_schema->get_aux_lob_piece_tid(), tablet_ids))) {
      LOG_WARN("failed to get data lob piece table snapshot", K(ret));
    }

    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(dest_table_schema)) {
      LOG_INFO("table not exist", K(ret), K(table_id), K(target_table_id), KP(dest_table_schema));
    } else if (OB_FAIL(ObDDLUtil::get_tablets(tenant_id, target_table_id, tablet_ids))) {
      LOG_WARN("failed to get dest table snapshot", K(ret));
    } else if (dest_table_schema->get_aux_lob_meta_tid() != OB_INVALID_ID &&
              OB_FAIL(ObDDLUtil::get_tablets(tenant_id, dest_table_schema->get_aux_lob_meta_tid(), tablet_ids))) {
      LOG_WARN("failed to get dest lob meta table snapshot", K(ret));
    } else if (dest_table_schema->get_aux_lob_piece_tid() != OB_INVALID_ID &&
              OB_FAIL(ObDDLUtil::get_tablets(tenant_id, dest_table_schema->get_aux_lob_piece_tid(), tablet_ids))) {
      LOG_WARN("failed to get dest lob piece table snapshot", K(ret));
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(task->batch_release_snapshot(snapshot_version, tablet_ids))) {
      LOG_WARN("failed to release snapshot", K(ret));
    }
    task->add_event_info("release snapshot finish");
    LOG_INFO("release snapshot finished", K(ret), K(snapshot_version), K(table_id), K(target_table_id), K(schema_version), "ddl_event_info", ObDDLEventInfo());
  }
  return ret;
}

int ObDDLUtil::hold_snapshot(
    common::ObMySQLTransaction &trans,
    const ObTableSchema &data_table_schema,
    const ObTableSchema &index_table_schema,
    const int64_t snapshot)
{
  int ret = OB_SUCCESS;
  SCN snapshot_scn;
  const uint64_t tenant_id = data_table_schema.get_tenant_id();
  const int64_t data_table_id = data_table_schema.get_table_id();
  const int64_t index_table_id = index_table_schema.get_table_id();
  const int64_t schema_version = index_table_schema.get_schema_version();
  if (snapshot <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("snapshot version not valid", K(ret), K(snapshot));
  } else if (OB_FAIL(snapshot_scn.convert_for_tx(snapshot))) {
    LOG_WARN("failed to convert", K(snapshot), K(ret));
  } else {
    rootserver::ObDDLService &ddl_service = GCTX.root_service_->get_ddl_service();
    ObSEArray<ObTabletID, 2> tablet_ids;
    bool need_acquire_lob = false;
    if (OB_FAIL(data_table_schema.get_tablet_ids(tablet_ids))) {
      LOG_WARN("failed to get data table snapshot", K(ret));
    } else if (OB_FAIL(index_table_schema.get_tablet_ids(tablet_ids))) {
      LOG_WARN("failed to get data table snapshot", K(ret));
    } else if (OB_FAIL(check_need_acquire_lob_snapshot(&data_table_schema, &index_table_schema, need_acquire_lob))) {
      LOG_WARN("failed to check if need to acquire lob snapshot", K(ret));
    } else if (need_acquire_lob && data_table_schema.get_aux_lob_meta_tid() != OB_INVALID_ID &&
               OB_FAIL(ObDDLUtil::get_tablets(tenant_id, data_table_schema.get_aux_lob_meta_tid(), tablet_ids))) {
      LOG_WARN("failed to get data lob meta table snapshot", K(ret));
    } else if (need_acquire_lob && data_table_schema.get_aux_lob_piece_tid() != OB_INVALID_ID &&
               OB_FAIL(ObDDLUtil::get_tablets(tenant_id, data_table_schema.get_aux_lob_piece_tid(), tablet_ids))) {
      LOG_WARN("failed to get data lob piece table snapshot", K(ret));
    } else if (OB_FAIL(ddl_service.get_snapshot_mgr().batch_acquire_snapshot(
            trans, SNAPSHOT_FOR_DDL, tenant_id, schema_version, snapshot_scn, nullptr, tablet_ids))) {
      LOG_WARN("batch acquire snapshot failed", K(ret), K(tablet_ids));
    }
  }
  LOG_INFO("hold snapshot finished", K(ret), K(snapshot), K(data_table_id), K(index_table_id), K(schema_version));
  return ret;
}

int ObDDLUtil::check_need_acquire_lob_snapshot(
    const ObTableSchema *data_table_schema,
    const ObTableSchema *index_table_schema,
    bool &need_acquire)
{
  int ret = OB_SUCCESS;
  need_acquire = false;
  if (OB_ISNULL(data_table_schema) || OB_ISNULL(index_table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("there are invalid arguments", K(ret), KP(data_table_schema), KP(index_table_schema));
  } else {
    ObTableSchema::const_column_iterator iter = index_table_schema->column_begin();
    ObTableSchema::const_column_iterator iter_end = index_table_schema->column_end();
    for (; OB_SUCC(ret) && !need_acquire && iter != iter_end; iter++) {
      const ObColumnSchemaV2 *index_col = *iter;
      if (OB_ISNULL(index_col)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column schema is null", K(ret));
      } else {
        const ObColumnSchemaV2 *col = data_table_schema->get_column_schema(index_col->get_column_id());
        if (OB_ISNULL(col)) {
        } else if (col->is_generated_column()) {
          ObSEArray<uint64_t, 8> ref_columns;
          if (OB_FAIL(col->get_cascaded_column_ids(ref_columns))) {
            STORAGE_LOG(WARN, "Failed to get cascaded column ids", K(ret));
          } else {
            for (int64_t i = 0; OB_SUCC(ret) && !need_acquire && i < ref_columns.count(); i++) {
              const ObColumnSchemaV2 *data_table_col = data_table_schema->get_column_schema(ref_columns.at(i));
              if (OB_ISNULL(data_table_col)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("column schema is null", K(ret));
              } else if (is_lob_storage(data_table_col->get_data_type())) {
                need_acquire = true;
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObDDLUtil::obtain_snapshot(
    common::ObMySQLTransaction &trans,
    const ObTableSchema &data_table_schema,
    const ObTableSchema &index_table_schema,
    int64_t &new_fetched_snapshot)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = data_table_schema.get_tenant_id();
  int64_t data_table_id = data_table_schema.get_table_id();
  new_fetched_snapshot = 0;
  if (OB_FAIL(calc_snapshot_with_gts(new_fetched_snapshot, tenant_id))) {
    LOG_WARN("fail to calc snapshot with gts", K(ret), K(new_fetched_snapshot));
  } else if (new_fetched_snapshot <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the snapshot is not valid", K(ret), K(new_fetched_snapshot));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObDDLUtil::hold_snapshot(trans, data_table_schema, index_table_schema, new_fetched_snapshot))) {
    if (OB_SNAPSHOT_DISCARDED == ret) {
      LOG_INFO("snapshot discarded, need retry waiting trans", K(ret), K(new_fetched_snapshot));
    } else {
      LOG_WARN("hold snapshot failed", K(ret), K(new_fetched_snapshot));
    }
  }
  return ret;
}

int ObDDLUtil::calc_snapshot_with_gts(
    int64_t &snapshot,
    const uint64_t tenant_id,
    const int64_t ddl_task_id,
    const int64_t trans_end_snapshot,
    const int64_t index_snapshot_version_diff)
{
  int ret = OB_SUCCESS;
  snapshot = 0;
  SCN curr_ts;
  bool is_external_consistent = false;
  rootserver::ObRootService *root_service = nullptr;
  const int64_t timeout_us = ObDDLUtil::get_default_ddl_rpc_timeout();
  ObFreezeInfoProxy freeze_info_proxy(tenant_id);
  ObFreezeInfo frozen_status;
  if (OB_UNLIKELY(tenant_id == common::OB_INVALID_ID || ddl_task_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(tenant_id), K(ddl_task_id));
  } else if (OB_ISNULL(root_service = GCTX.root_service_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("root service is null", K(ret), KP(root_service));
  } else {
    {
      MAKE_TENANT_SWITCH_SCOPE_GUARD(tenant_guard);
      // ignore return, MTL is only used in get_ts_sync, which will handle switch failure.
      // for performance, everywhere calls get_ts_sync should ensure using correct tenant ctx
      tenant_guard.switch_to(tenant_id);
      if (OB_FAIL(OB_TS_MGR.get_ts_sync(tenant_id,
                                        timeout_us,
                                        curr_ts,
                                        is_external_consistent))) {
        LOG_WARN("fail to get gts sync", K(ret), K(tenant_id), K(timeout_us), K(curr_ts), K(is_external_consistent));
      }
    }
    if (OB_SUCC(ret)) {
      snapshot = max(trans_end_snapshot, curr_ts.get_val_for_tx() - index_snapshot_version_diff);
      if (OB_FAIL(freeze_info_proxy.get_freeze_info(
          root_service->get_sql_proxy(), SCN::min_scn(), frozen_status))) {
        LOG_WARN("get freeze info failed", K(ret));
      } else if (OB_FAIL(DDL_SIM(tenant_id, ddl_task_id, GET_FREEZE_INFO_FAILED))) {
        LOG_WARN("ddl sim failure: get freeze info failed", K(ret), K(tenant_id), K(ddl_task_id));
      } else {
        const int64_t frozen_scn_val = frozen_status.frozen_scn_.get_val_for_tx();
        snapshot = max(snapshot, frozen_scn_val);
      }
    }
  }
  return ret;
}

int ObDDLUtil::check_and_cancel_single_replica_dag(
    rootserver::ObDDLTask* task,
    const uint64_t table_id,
    const uint64_t target_table_id,
    common::hash::ObHashMap<common::ObTabletID, common::ObTabletID>& check_dag_exit_tablets_map,
    int64_t &check_dag_exit_retry_cnt,
    bool is_complement_data_dag,
    bool &all_dag_exit)
{
  int ret = OB_SUCCESS;
  all_dag_exit = false;
  const bool force_renew = true;
  bool is_cache_hit = false;
  const int64_t expire_renew_time = force_renew ? INT64_MAX : 0;
  share::ObLocationService *location_service = GCTX.location_service_;
  rootserver::ObRootService *root_service = GCTX.root_service_;
  if (OB_ISNULL(task)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_UNLIKELY(!task->is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(location_service) || OB_ISNULL(root_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), KP(location_service), KP(root_service));
  } else if (OB_UNLIKELY(!check_dag_exit_tablets_map.created())) {
    const int64_t CHECK_DAG_EXIT_BUCKET_NUM = 64;
    common::ObArray<common::ObTabletID> src_tablet_ids;
    common::ObArray<common::ObTabletID> dst_tablet_ids;
    uint64_t tenant_id = task->get_src_tenant_id();
    uint64_t dst_tenant_id = task->get_tenant_id();
    if (OB_FAIL(ObDDLUtil::get_tablets(tenant_id, table_id, src_tablet_ids))) {
      LOG_WARN("fail to get tablets", K(ret), K(tenant_id), K(table_id));
    } else if (OB_FAIL(ObDDLUtil::get_tablets(dst_tenant_id, target_table_id, dst_tablet_ids))) {
      LOG_WARN("fail to get tablets", K(ret), K(dst_tenant_id), K(target_table_id));
    } else if (OB_FAIL(check_dag_exit_tablets_map.create(CHECK_DAG_EXIT_BUCKET_NUM, lib::ObLabel("DDLChkDagMap")))) {
      LOG_WARN("create hashset set failed", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < src_tablet_ids.count(); i++) {
        if (OB_FAIL(check_dag_exit_tablets_map.set_refactored(src_tablet_ids.at(i), dst_tablet_ids.at(i)))) {
          LOG_WARN("set refactored failed", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    int saved_ret = OB_SUCCESS;
    ObAddr unused_leader_addr;
    const int64_t timeout_us = ObDDLUtil::get_default_ddl_rpc_timeout();
    common::hash::ObHashMap<common::ObTabletID, common::ObTabletID> ::const_iterator iter =
      check_dag_exit_tablets_map.begin();
    ObArray<common::ObTabletID> dag_not_exist_tablets;
    uint64_t tenant_id = task->get_src_tenant_id();
    uint64_t dst_tenant_id = task->get_tenant_id();
    for (; OB_SUCC(ret) && iter != check_dag_exit_tablets_map.end(); iter++) {
      ObLSID src_ls_id;
      ObLSID dst_ls_id;
      const common::ObTabletID &src_tablet_id = iter->first;
      const common::ObTabletID &dst_tablet_id = iter->second;
      int64_t paxos_member_count = 0;
      common::ObArray<ObAddr> paxos_server_list;
      if (OB_FAIL(ObDDLUtil::get_tablet_leader_addr(location_service, tenant_id, src_tablet_id, timeout_us, src_ls_id, unused_leader_addr))) {
        LOG_WARN("get src tablet leader addr failed", K(ret));
      } else if (OB_FAIL(ObDDLUtil::get_tablet_leader_addr(location_service, dst_tenant_id, dst_tablet_id, timeout_us, dst_ls_id, unused_leader_addr))) {
        LOG_WARN("get dst tablet leader addr failed", K(ret));
      } else if (OB_FAIL(ObDDLUtil::get_tablet_paxos_member_list(dst_tenant_id, dst_tablet_id, paxos_server_list, paxos_member_count))) {
        LOG_WARN("get tablet paxos member list failed", K(ret));
      } else {
        bool is_tablet_dag_exist = false;
        obrpc::ObDDLBuildSingleReplicaRequestArg arg;
        arg.ls_id_ = src_ls_id;
        arg.dest_ls_id_ = dst_ls_id;
        arg.tenant_id_ = tenant_id;
        arg.dest_tenant_id_ = dst_tenant_id;
        arg.source_tablet_id_ = src_tablet_id;
        arg.dest_tablet_id_ = dst_tablet_id;
        arg.source_table_id_ = table_id;
        arg.dest_schema_id_ = target_table_id;
        arg.schema_version_ = task->get_src_schema_version();
        arg.dest_schema_version_ = task->get_schema_version();
        arg.snapshot_version_ = 1; // to ensure arg valid only.
        arg.ddl_type_ = task->get_task_type();
        arg.task_id_ = task->get_task_id();
        arg.parallelism_ = 1; // to ensure arg valid only.
        arg.execution_id_ = 1; // to ensure arg valid only.
        arg.data_format_version_ = 1; // to ensure arg valid only.
        arg.tablet_task_id_ = 1; // to ensure arg valid only.
        arg.consumer_group_id_ = 0; // to ensure arg valid only.
        for (int64_t j = 0; OB_SUCC(ret) && j < paxos_server_list.count(); j++) {
          int tmp_ret = OB_SUCCESS;
          obrpc::Bool is_replica_dag_exist(true);
          if (is_complement_data_dag && OB_TMP_FAIL(root_service->get_rpc_proxy().to(paxos_server_list.at(j))
            .by(dst_tenant_id).timeout(timeout_us).check_and_cancel_ddl_complement_dag(arg, is_replica_dag_exist))) {
            // consider as dag does exist in this server.
            saved_ret = OB_SUCC(saved_ret) ? tmp_ret : saved_ret;
            is_tablet_dag_exist = true;
            LOG_WARN("check and cancel ddl complement dag failed", K(ret), K(tmp_ret), K(arg));
          } else if (!is_complement_data_dag && OB_TMP_FAIL(root_service->get_rpc_proxy().to(paxos_server_list.at(j))
            .by(dst_tenant_id).timeout(timeout_us).check_and_cancel_delete_lob_meta_row_dag(arg, is_replica_dag_exist))) {
            // consider as dag does exist in this server.
            saved_ret = OB_SUCC(saved_ret) ? tmp_ret : saved_ret;
            is_tablet_dag_exist = true;
            LOG_WARN("check and cancel ddl complement dag failed", K(ret), K(tmp_ret), K(arg));
          } else if (is_replica_dag_exist) {
            is_tablet_dag_exist = true;
            if (REACH_COUNT_INTERVAL(1000L)) {
              LOG_INFO("wait dag exist", "addr", paxos_server_list.at(j), K(arg));
            }
          }
        }
        if (OB_SUCC(ret) && !is_tablet_dag_exist) {
          if (OB_FAIL(dag_not_exist_tablets.push_back(src_tablet_id))) {
            LOG_WARN("push back failed", K(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      for (int64_t j = 0; OB_SUCC(ret) && j < dag_not_exist_tablets.count(); j++) {
        if (OB_FAIL(check_dag_exit_tablets_map.erase_refactored(dag_not_exist_tablets.at(j)))) {
          LOG_WARN("erase failed", K(ret));
        }
      }
      ret = OB_SUCC(ret) ? saved_ret : ret;
    }
  }
  if (OB_SUCC(ret)) {
    all_dag_exit = check_dag_exit_tablets_map.empty() ? true : false;
    task->set_delay_schedule_time(3000L * 1000L); // 3s, to avoid sending too many rpcs to the same replica frequently if retry.
  } else if (OB_TABLE_NOT_EXIST == ret
      || OB_TENANT_HAS_BEEN_DROPPED == ret
      || OB_TENANT_NOT_EXIST == ret
      || (++check_dag_exit_retry_cnt >= 10 /*MAX RETRY COUNT IF FAILED*/)) {
    ret = OB_SUCCESS;
    all_dag_exit = true;
  }
  return ret;
}

int ObDDLUtil::ddl_get_tablet(
    const ObLSHandle &ls_handle,
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


int ObDDLUtil::get_tablet_leader(
  const uint64_t tenant_id,
  const common::ObTabletID &tablet_id,
  common::ObAddr &leader_addr)
{
  int ret = OB_SUCCESS;
  ObLSLocation location;
  share::ObLSID unused_ls_id;
  if (OB_INVALID_TENANT_ID == tenant_id || !tablet_id.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get tablet leader, invalid id", K(ret), K(tenant_id), K(tablet_id));
  } else if (OB_FAIL(get_tablet_replica_location(tenant_id, tablet_id, unused_ls_id, location))) {
    LOG_WARN("fail to get tablet replica location", K(ret), K(tenant_id), K(tablet_id));
  } else if (OB_FAIL(location.get_leader(leader_addr))) {
    LOG_WARN("fail to get tablet leader addr", K(ret), K(tenant_id), K(tablet_id));
  } else if (!leader_addr.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get tablet leader, addr is invalid", K(ret), K(tenant_id), K(tablet_id));
  }
  return ret;
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
  share::ObLSID unused_ls_id;
  if (OB_INVALID_TENANT_ID == tenant_id || !tablet_id.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get tablet replica location, invalid id", K(ret), K(tenant_id), K(tablet_id));
  } else if (OB_FAIL(get_tablet_replica_location(tenant_id, tablet_id, unused_ls_id, location))) {
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
int ObDDLUtil::get_tablet_physical_row_cnt(
  const share::ObLSID &ls_id,
  const ObTabletID &tablet_id,
  const bool calc_sstable,
  const bool calc_memtable,
  int64_t &physical_row_count /*OUT*/)
{
  int ret = OB_SUCCESS;

  // get total rows of the table; physical
  // src_tablet_id -> tablet -> sstables -> sstable_metas -> row_count
  //                         -> memtables -> physical_row_cnt
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  ObTableStoreIterator table_store_iter;

  physical_row_count = 0;

  if (!ls_id.is_valid() || !tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_FAIL(MTL(ObLSService*)->get_ls(ls_id, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("get ls failed", K(ret), K(ls_id));
  } else if (OB_FAIL(ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle, ObTabletCommon::DEFAULT_GET_TABLET_DURATION_10_S, ObMDSGetTabletMode::READ_ALL_COMMITED))) {
    LOG_WARN("fail to get tablet", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpecter error", K(ret), K(tablet_handle));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet is nullptr", K(ret), K(tablet_handle));
  } else if (OB_FAIL(tablet->get_all_tables(table_store_iter))) {
    LOG_WARN("get all tables failed", K(ret));
  } else if (!table_store_iter.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table_store_iter is invalid", K(ret), K(table_store_iter), KPC(tablet));
  } else {
    table_store_iter.resume();
    while (OB_SUCC(ret)) {
      ObITable *table = nullptr;
      ObSSTable *sstable = nullptr;
      memtable::ObMemtable* memtable = nullptr;
      ObSSTableMetaHandle sstable_meta_hdl;
      if (OB_FAIL(table_store_iter.get_next(table))) {
        if (OB_UNLIKELY(OB_ITER_END == ret)) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("get next table failed", K(ret));
        }
      } else if (OB_UNLIKELY(OB_ISNULL(table))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected err", K(ret), KPC(table));
      } else if (calc_sstable && table->is_sstable()) {
        if (OB_FALSE_IT(sstable = static_cast<ObSSTable*>(table))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("sstable static_cast failed", K(ret), KPC(table));
        } else if (OB_ISNULL(sstable) || !sstable->is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("the sstable is null or invalid", K(ret));
        } else if (OB_FAIL(sstable->get_meta(sstable_meta_hdl))) {
          LOG_WARN("get sstable meta failed", K(ret), KPC(sstable));
        } else {
          physical_row_count += sstable_meta_hdl.get_sstable_meta().get_row_count();
        }
      } else if (calc_memtable && table->is_memtable()) {
        if (OB_FALSE_IT(memtable = static_cast<memtable::ObMemtable*>(table))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("memtable static_cast failed", K(ret), KPC(table));
        } else if (OB_ISNULL(memtable)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get memtable meta failed", K(ret), KPC(memtable));
        } else {
          physical_row_count += memtable->get_physical_row_cnt();
        }
      }
    } // end while
  }
  if (OB_FAIL(ret)) {
    physical_row_count = 0;
  }
  return ret;
}

int ObDDLUtil::get_tablet_replica_location(
    const uint64_t tenant_id,
    const common::ObTabletID &tablet_id,
    ObLSID &ls_id,
    ObLSLocation &location)
{
  int ret = OB_SUCCESS;
  const int64_t cluster_id = GCONF.cluster_id;
  int64_t expire_renew_time = INT64_MAX;
  bool is_cache_hit = false;
  ls_id.reset();
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

// filter offline replica and arbitration one.
int ObDDLUtil::get_split_replicas_addrs(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    ObIArray<ObAddr> &member_addrs_array,
    ObIArray<ObAddr> &learner_addrs_array)
{
  int ret = OB_SUCCESS;
  member_addrs_array.reset();
  learner_addrs_array.reset();
  ObLSInfo ls_info;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || !ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(tenant_id), K(ls_id));
  } else if (OB_ISNULL(GCTX.lst_operator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lst_operator is null", K(ret));
  } else if (OB_FAIL(GCTX.lst_operator_->get(GCONF.cluster_id, tenant_id, ls_id, share::ObLSTable::COMPOSITE_MODE/*for sys tenant only*/, ls_info))) {
    LOG_WARN("fail to get ls info", K(ret), K(tenant_id), K(ls_id));
  } else {
    int64_t leader_replica_index = OB_INVALID_INDEX;
    ObArray<ObAddr> filter_replica_addrs; // for split, we should ignore offline replica and arbitration replica.
    const ObLSInfo::ReplicaArray &all_replicas = ls_info.get_replicas();
    for (int64_t idx = 0; OB_SUCC(ret) && idx < all_replicas.count(); idx++) {
      const ObLSReplica &tmp_replica = all_replicas.at(idx);
      if (REPLICA_TYPE_ARBITRATION == tmp_replica.get_replica_type()
        || REPLICA_STATUS_OFFLINE == tmp_replica.get_replica_status()) {
        if (OB_FAIL(filter_replica_addrs.push_back(tmp_replica.get_server()))) {
          LOG_WARN("push back failed", K(ret));
        } else {
          LOG_TRACE("filter offline replica and arbitration replica for split", K(ret), K(tmp_replica));
        }
      } else if (ObRole::LEADER == tmp_replica.get_role()) {
        leader_replica_index = idx;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_INVALID_INDEX == leader_replica_index) {
        ret = OB_EAGAIN;
        LOG_WARN("No leader found, try again", K(ret), K(tenant_id), K(ls_id), K(all_replicas));
      } else {
        const ObLSReplica &leader_replica = all_replicas.at(leader_replica_index);
        const ObLSReplica::MemberList &member_list = leader_replica.get_member_list();
        for (int64_t idx = 0; OB_SUCC(ret) && idx < member_list.count(); ++idx) {
          const common::ObAddr &addr = member_list.at(idx).get_server();
          if (common::is_contain(filter_replica_addrs, addr)) {
            // filter.
            LOG_TRACE("ignore replica", K(ret), K(addr));
          } else if (OB_FAIL(member_addrs_array.push_back(addr))) {
            LOG_WARN("failed to push addr", K(ret));
          }
        }
        const common::GlobalLearnerList &learner_list = leader_replica.get_learner_list();
        for (int64_t idx = 0; OB_SUCC(ret) && idx < learner_list.get_member_number(); ++idx) {
          common::ObAddr addr;
          if (OB_FAIL(learner_list.get_server_by_index(idx, addr))) {
            LOG_WARN("failed to push addr", KR(ret), K(idx));
          } else if (OB_FAIL(learner_addrs_array.push_back(addr))) {
            LOG_WARN("failed to push addr", KR(ret), K(addr));
          }
        }
      }
    }
  }
  if (OB_SUCC(ret) && member_addrs_array.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected empty member list", K(ret), K(tenant_id), K(ls_id));
  }
  return ret;
}

int ObDDLUtil::get_split_replicas_addrs(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    ObIArray<ObAddr> &replica_addr_array)
{
  int ret = OB_SUCCESS;
  replica_addr_array.reset();
  ObArray<ObAddr> member_addrs_array;
  ObArray<ObAddr> learners_addr_array;
  if (OB_FAIL(get_split_replicas_addrs(tenant_id, ls_id, member_addrs_array, learners_addr_array))) {
    LOG_WARN("get addrs failed", K(ret), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(append(replica_addr_array/*dst*/, member_addrs_array/*src*/))) {
    LOG_WARN("append failed", K(ret));
  } else if (OB_FAIL(append(replica_addr_array/*dst*/, learners_addr_array/*src*/))) {
    LOG_WARN("append failed", K(ret));
  }
  return ret;
}

int ObDDLUtil::construct_ls_tablet_id_map(
  const uint64_t &tenant_id,
  const share::ObLSID &ls_id,
  const common::ObTabletID &tablet_id,
  hash::ObHashMap<ObLSID, ObArray<ObTabletID>> &ls_tablet_id_map)
{
  int ret = OB_SUCCESS;
  bool is_cache_hit = false;
  ObArray<ObTabletID> tablet_id_array;
  if (!ls_id.is_valid() || !tablet_id.is_valid() || OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id), K(tablet_id), K(tenant_id));
  } else if (OB_FAIL(ls_tablet_id_map.get_refactored(ls_id, tablet_id_array))) {
    if (OB_HASH_NOT_EXIST == ret) { // first time
      ret = OB_SUCCESS;
      if (OB_FAIL(tablet_id_array.push_back(tablet_id))) {
        LOG_WARN("fail to push back to array", K(ret), K(tablet_id), K(tablet_id_array));
      } else if (OB_FAIL(ls_tablet_id_map.set_refactored(ls_id, tablet_id_array, true /* overwrite */))) {
        LOG_WARN("ls tablets map set fail", K(ret), K(ls_id), K(tablet_id_array));
      }
    } else {
      LOG_WARN("ls tablets map get fail", K(ret), K(ls_id), K(tablet_id_array));
    }
  } else if (OB_FAIL(tablet_id_array.push_back(tablet_id))) {
    LOG_WARN("fail to push back to array", K(ret), K(tablet_id_array), K(tablet_id));
  } else if (OB_FAIL(ls_tablet_id_map.set_refactored(ls_id, tablet_id_array, true /* overwrite */))) {
    LOG_WARN("ls tablets map set fail", K(ret), K(ls_id), K(tablet_id_array));
  }
  return ret;
}

int ObDDLUtil::get_index_table_batch_partition_names(
    const uint64_t &tenant_id,
    const int64_t &data_table_id,
    const int64_t &index_table_id,
    const ObIArray<ObTabletID> &tablets,
    common::ObIAllocator &allocator,
    ObIArray<ObString> &partition_names)
{
  int ret = OB_SUCCESS;
  if ((OB_UNLIKELY(OB_INVALID_ID == tenant_id || OB_INVALID_ID == data_table_id || OB_INVALID_ID == index_table_id || tablets.count() < 1))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("the parameters is invalid", K(ret), K(tenant_id), K(data_table_id), K(index_table_id), K(tablets.count()));
  } else {
    ObSchemaGetterGuard schema_guard;
    const ObTableSchema *data_table_schema = nullptr;
    const ObTableSchema *index_schema = nullptr;
    if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("fail to get schema guard", K(ret), K(tenant_id));
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, data_table_id, data_table_schema))) {
      LOG_WARN("get table schema failed", K(ret), K(tenant_id), K(data_table_id));
    } else if (OB_ISNULL(data_table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("error unexpected, data table schema is null", K(ret), K(data_table_id));
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, index_table_id, index_schema))) {
      LOG_WARN("get table schema failed", K(ret), K(tenant_id), K(index_table_id));
    } else if (OB_ISNULL(index_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("error unexpected, index table schema is null", K(ret), K(index_table_id));
    } else {
      const ObPartitionOption &data_part_option = data_table_schema->get_part_option();
      const ObPartitionOption &index_part_option = index_schema->get_part_option();
      if (OB_UNLIKELY(data_part_option.get_part_num() < 1)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("data table part num less than 1", K(ret), K(data_part_option));
      } else if (OB_UNLIKELY(index_part_option.get_part_num() < 1)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("index table part num less than 1", K(ret), K(index_part_option));
      } else if (OB_UNLIKELY(data_part_option.get_part_num() != index_part_option.get_part_num())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, data table partition num not equal to index table partition num", K(ret), K(data_part_option.get_part_num()), K(index_part_option.get_part_num()));
      } else {
        ObPartition **data_partitions = data_table_schema->get_part_array();
        const ObPartitionLevel part_level = data_table_schema->get_part_level();
        if (OB_ISNULL(data_partitions)) {
          ret = OB_PARTITION_NOT_EXIST;
          LOG_WARN("data table part array is null", K(ret));
        } else {
          int64_t part_index = -1;
          int64_t subpart_index = -1;
          for (int64_t i = 0; i < tablets.count() && OB_SUCC(ret); i++) {
            if (OB_FAIL(index_schema->get_part_idx_by_tablet(tablets.at(i), part_index, subpart_index))) {
              LOG_WARN("failed to get part idx by tablet", K(ret), K(tablets.at(i)), K(part_index), K(subpart_index));
            } else {
              ObString tmp_name;
              if (PARTITION_LEVEL_ONE == part_level) {
                if OB_FAIL(deep_copy_ob_string(allocator,
                                               data_partitions[part_index]->get_part_name(),
                                               tmp_name)) {
                  LOG_WARN("fail to deep copy partition names", K(ret), K(data_partitions[part_index]->get_part_name()), K(tmp_name));
                } else if (OB_FAIL(partition_names.push_back(tmp_name))) {
                  LOG_WARN("fail to push back", K(ret), K(data_partitions[part_index]->get_part_name()), K(tmp_name), K(partition_names));
                }
              } else if (PARTITION_LEVEL_TWO == part_level) {
                ObSubPartition **data_subpart_array = data_partitions[part_index]->get_subpart_array();
                if (OB_ISNULL(data_subpart_array)) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("part array is null", K(ret), K(part_index));
                } else if OB_FAIL(deep_copy_ob_string(allocator,
                                                      data_subpart_array[subpart_index]->get_part_name(),
                                                      tmp_name)) {
                  LOG_WARN("fail to deep copy partition names", K(ret), K(data_subpart_array[subpart_index]->get_part_name()), K(tmp_name));
                } else if (OB_FAIL(partition_names.push_back(tmp_name))) {
                  LOG_WARN("fail to push back", K(ret), K(data_subpart_array[subpart_index]->get_part_name()), K(tmp_name), K(partition_names));
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

int ObDDLUtil::get_tablet_data_size(
    const uint64_t &tenant_id,
    const common::ObTabletID &tablet_id,
    const share::ObLSID &ls_id,
    int64_t &data_size)
{
  int ret = OB_SUCCESS;
  const int64_t obj_pos = 0;
  ObObj result_obj;
  const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
  data_size = 0;
  if (!tablet_id.is_valid() || !ls_id.is_valid() || OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tablet_id), K(tenant_id));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ObSqlString query_string;
      sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(query_string.assign_fmt("SELECT max(data_size) as data_size FROM %s WHERE tenant_id = %lu AND tablet_id = %lu AND ls_id = %lu",
          OB_ALL_TABLET_META_TABLE_TNAME, tenant_id, tablet_id.id(), ls_id.id()))) {
        LOG_WARN("assign sql string failed", K(ret), K(OB_ALL_TABLET_META_TABLE_TNAME), K(tenant_id), K(tablet_id), K(ls_id));
      } else if (OB_FAIL(GCTX.sql_proxy_->read(res, meta_tenant_id, query_string.ptr()))) {
        LOG_WARN("read record failed", K(ret), K(tenant_id), K(meta_tenant_id), K(query_string));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get sql result", K(ret), K(tenant_id), K(meta_tenant_id), K(query_string));
      } else if (OB_FAIL(result->next())) {
        LOG_WARN("get next result failed", K(ret), K(tenant_id), K(meta_tenant_id), K(query_string));
      } else if (OB_FAIL(result->get_obj(obj_pos, result_obj))) {
        LOG_WARN("failed to get object", K(ret));
      } else if (result_obj.is_null()) {
        data_size = 0;
        LOG_WARN("data size is null", K(ret));
        ret = OB_SUCCESS;
      } else if (OB_UNLIKELY(!result_obj.is_integer_type())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected obj type", K(ret), K(result_obj.get_type()));
      } else {
        data_size = result_obj.get_int();
      }
    }
  }
  return ret;
}

int ObDDLUtil::get_tablet_data_row_cnt(
    const uint64_t &tenant_id,
    const common::ObTabletID &tablet_id,
    const share::ObLSID &ls_id,
    int64_t &data_row_cnt)
{
  int ret = OB_SUCCESS;
  const int64_t obj_pos = 0;
  ObObj result_obj;
  const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
  data_row_cnt = 0;
  if (!tablet_id.is_valid() || !ls_id.is_valid() || OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tablet_id), K(tenant_id));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ObSqlString query_string;
      sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(query_string.assign_fmt("SELECT max(row_count) as row_count FROM %s WHERE tenant_id = %lu AND tablet_id = %lu AND ls_id = %lu",
          OB_ALL_TABLET_REPLICA_CHECKSUM_TNAME, tenant_id, tablet_id.id(), ls_id.id()))) {
        LOG_WARN("assign sql string failed", K(ret), K(OB_ALL_TABLET_REPLICA_CHECKSUM_TNAME), K(tenant_id), K(tablet_id), K(ls_id));
      } else if (OB_FAIL(GCTX.sql_proxy_->read(res, meta_tenant_id, query_string.ptr()))) {
        LOG_WARN("read record failed", K(ret), K(tenant_id), K(meta_tenant_id), K(query_string));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get sql result", K(ret), K(tenant_id), K(meta_tenant_id), K(query_string));
      } else if (OB_FAIL(result->next())) {
        LOG_WARN("get next result failed", K(ret), K(tenant_id), K(meta_tenant_id), K(query_string));
      } else if (OB_FAIL(result->get_obj(obj_pos, result_obj))) {
        LOG_WARN("failed to get object", K(ret));
      } else if (result_obj.is_null()) {
        data_row_cnt = 0;
        LOG_WARN("data size is null", K(ret));
        ret = OB_SUCCESS;
      } else if (OB_UNLIKELY(!result_obj.is_integer_type())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected obj type", K(ret), K(result_obj.get_type()));
      } else {
        data_row_cnt = result_obj.get_int();
      }
    }
  }
  return ret;
}

int ObDDLUtil::get_ls_host_left_disk_space(
    const uint64_t &tenant_id,
    const share::ObLSID &ls_id,
    const common::ObAddr &leader_addr,
    uint64_t &left_space_size)
{
  int ret = OB_SUCCESS;
  left_space_size = 0;
  if (OB_INVALID_TENANT_ID == tenant_id || !ls_id.is_valid() || !leader_addr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(ls_id), K(leader_addr));
  } else {
    char svr_ip[MAX_IP_ADDR_LENGTH] = "\0";
    if (!leader_addr.ip_to_string(svr_ip, sizeof(svr_ip))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("format ip str failed", K(ret), K(leader_addr));
    } else {
      SMART_VAR(ObMySQLProxy::MySQLResult, res) {
        ObSqlString query_string;
        sqlclient::ObMySQLResult *result = NULL;
        if (OB_FAIL(query_string.assign_fmt("SELECT free_size FROM %s WHERE svr_ip = \"%s\" AND svr_port = '%d'",
            OB_ALL_VIRTUAL_DISK_STAT_TNAME, svr_ip, leader_addr.get_port()))) {
          LOG_WARN("assign sql string failed", K(ret), K(OB_ALL_VIRTUAL_DISK_STAT_TNAME), K(svr_ip), K(leader_addr.get_port()));
        } else if (OB_FAIL(GCTX.sql_proxy_->read(res, OB_SYS_TENANT_ID, query_string.ptr()))) {
          LOG_WARN("read record failed", K(ret), K(tenant_id), K(ls_id), K(leader_addr), K(OB_SYS_TENANT_ID), K(query_string));
        } else if (OB_ISNULL(result = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to get sql result", K(ret), K(tenant_id), K(ls_id), K(leader_addr), K(OB_SYS_TENANT_ID), K(query_string));
        } else if (OB_FAIL(result->next())) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("fail to get next", K(ret), K(tenant_id), K(ls_id), K(leader_addr), K(OB_SYS_TENANT_ID), K(query_string));
          }
        } else {
          EXTRACT_INT_FIELD_MYSQL(*result, "free_size", left_space_size, uint64_t);
        }
      }
    }
  }
  return ret;
}

int ObDDLUtil::generate_partition_names(const common::ObIArray<ObString> &partition_names_array, const bool is_oracle_mode, common::ObIAllocator &allocator, ObString &partition_names)
{
  int ret = OB_SUCCESS;
  const char quote = is_oracle_mode ? '"' : '`';
  ObArenaAllocator tmp_allocator("ObDDLTmp");
  partition_names.reset();
  ObSqlString sql_partition_names;
  if (OB_UNLIKELY(partition_names_array.count() < 1)) {
    LOG_WARN("array num is less than 1", K(ret), K(partition_names_array));
  } else {
    int64_t partition_nums = partition_names_array.count();
    if (OB_FAIL(sql_partition_names.append("PARTITION("))) {
      LOG_WARN("append partition names failed", K(ret), K(partition_names_array));
    } else {
      for (int64_t i = 0; i < partition_nums && OB_SUCC(ret); i++) {
        ObString part_name;
        tmp_allocator.reuse();
        if (OB_FAIL(sql::ObSQLUtils::generate_new_name_with_escape_character(tmp_allocator, partition_names_array.at(i), part_name, is_oracle_mode))) {
          LOG_WARN("failed to generate new name", K(ret), K(partition_names_array.at(i)));
        } else if (i == partition_nums - 1) {
          if (OB_FAIL(sql_partition_names.append_fmt("%c%.*s%c)", quote, static_cast<int>(part_name.length()), part_name.ptr(), quote))) {
            LOG_WARN("append partition names failed", K(ret), K(partition_nums), K(partition_names_array), K(i), K(sql_partition_names), K(part_name));
          }
        } else {
          if (OB_FAIL(sql_partition_names.append_fmt("%c%.*s%c,", quote, static_cast<int>(part_name.length()), part_name.ptr(), quote))) {
            LOG_WARN("append partition names failed", K(ret), K(partition_nums), K(partition_names_array), K(i), K(sql_partition_names), K(part_name));
          }
        }
      }
    }
    ObString tmp_name = sql_partition_names.string();
    if (OB_SUCC(ret)) {
      if OB_FAIL(deep_copy_ob_string(allocator,
                                    tmp_name,
                                    partition_names)) {
        LOG_WARN("fail to deep copy partition names", K(ret), K(tmp_name), K(partition_names), K(partition_names_array));
      }
    }
  }
  return ret;
}

int ObDDLUtil::check_target_partition_is_running(const ObString &running_sql_info, const ObString &partition_name, common::ObIAllocator &allocator, bool &is_running_status)
{
  int ret = OB_SUCCESS;
  ObSqlString sql_partition_name;
  ObString tmp_name;
  is_running_status = false;
  if (OB_UNLIKELY(running_sql_info.empty() || partition_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(running_sql_info), K(partition_name));
  } else if (OB_FAIL(sql_partition_name.append_fmt("%.*s,", static_cast<int>(partition_name.length()), partition_name.ptr()))) {
    LOG_WARN("append partition names failed", K(ret), K(partition_name), K(sql_partition_name));
  } else {
    tmp_name = sql_partition_name.string();
    if (0 != ObCharset::instr(ObCollationType::CS_TYPE_UTF8MB4_BIN, running_sql_info.ptr(), running_sql_info.length(), tmp_name.ptr(), tmp_name.length())) {
      is_running_status = true;
    }
    if (is_running_status == false) {
      sql_partition_name.reuse();
      tmp_name.reset();
      if (OB_FAIL(sql_partition_name.append_fmt("%.*s)", static_cast<int>(partition_name.length()), partition_name.ptr()))) {
        LOG_WARN("append partition names failed", K(ret), K(partition_name), K(sql_partition_name));
      } else {
        tmp_name = sql_partition_name.string();
        if (0 != ObCharset::instr(ObCollationType::CS_TYPE_UTF8MB4_BIN, running_sql_info.ptr(), running_sql_info.length(), tmp_name.ptr(), tmp_name.length())) {
          is_running_status = true;
        }
      }
    }
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
  ddl_rpc_timeout_us = min(ddl_rpc_timeout_us, rpc_timeout_upper);
  ddl_rpc_timeout_us = max(ddl_rpc_timeout_us, GCONF._ob_ddl_timeout);
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


/*
* return the map between tablet id & slice cnt;
* note that pair <0, 0> may exist when result is not partition table
*/

int ObDDLUtil::get_task_tablet_slice_count(const int64_t tenant_id,  const int64_t ddl_task_id, bool &is_partitioned_table, common::hash::ObHashMap<int64_t, int64_t> &tablet_slice_cnt_map)
{
  int ret = OB_SUCCESS;

  bool use_idem_mode = false;
  rootserver::ObDDLSliceInfo ddl_slice_info;
  ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
  ObArenaAllocator arena(ObMemAttr(tenant_id, "get_slice_info"));
  bool is_use_idem_mode = false;
  is_partitioned_table = true;
  if (OB_ISNULL(sql_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", K(ret));
  } else if (OB_FAIL(rootserver::ObDDLTaskRecordOperator::get_schedule_info_for_update(
                    *sql_proxy, tenant_id, ddl_task_id, arena, ddl_slice_info, use_idem_mode))) {
    LOG_WARN("fail to get schedule info", K(ret), K(tenant_id), K(ddl_task_id));
  } else {
    for (int64_t i = 0; i < ddl_slice_info.part_ranges_.count() && OB_SUCC(ret); i++) {
      int64_t tablet_slice_cnt = 0;
      const ObPxTabletRange &cur_part_range = ddl_slice_info.part_ranges_.at(i);
      const int64_t cur_tablet_id = cur_part_range.tablet_id_;
      if (0 == cur_tablet_id && 1 == ddl_slice_info.part_ranges_.count()) {
        is_partitioned_table = false;
      }

      if (OB_FAIL(tablet_slice_cnt_map.get_refactored(cur_tablet_id, tablet_slice_cnt))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          if (OB_FAIL(tablet_slice_cnt_map.set_refactored(cur_tablet_id, 0))) {
            LOG_WARN("failed to set refactor", K(ret));
          }
        } else {
          LOG_WARN("failed to get  slice cnt", K(ret));
        }
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(tablet_slice_cnt_map.set_refactored(cur_tablet_id, tablet_slice_cnt + cur_part_range.range_cut_.count(), 1 /* over write*/))) {
        LOG_WARN("failed to set slice cnt", K(ret), K(tablet_slice_cnt), K( cur_part_range.range_cut_.count()));
      }
    }
  }
  return ret;
}

int ObDDLUtil::get_data_information(
    const uint64_t tenant_id,
    const uint64_t task_id,
    uint64_t &data_format_version,
    int64_t &snapshot_version,
    share::ObDDLTaskStatus &task_status)
{
  uint64_t target_object_id = 0;
  int64_t schema_version = 0;
  bool is_no_logging = false;
  return get_data_information(
      tenant_id,
      task_id,
      data_format_version,
      snapshot_version,
      task_status,
      target_object_id,
      schema_version,
      is_no_logging);
}

int ObDDLUtil::get_data_information(
    const uint64_t tenant_id,
    const uint64_t task_id,
    uint64_t &data_format_version,
    int64_t &snapshot_version,
    share::ObDDLTaskStatus &task_status,
    uint64_t &target_object_id,
    int64_t &schema_version,
    bool &is_no_logging)
{
  int ret = OB_SUCCESS;
  data_format_version = 0;
  snapshot_version = 0;
  task_status = share::ObDDLTaskStatus::PREPARE;
  target_object_id = 0;
  data_format_version = 0;
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || task_id <= 0
      || nullptr == GCTX.sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(tenant_id), K(task_id), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(DDL_SIM(tenant_id, task_id, GET_DATA_FORMAT_VERISON_FAILED))) {
    LOG_WARN("ddl sim failure", K(ret), K(tenant_id), K(task_id));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ObSqlString query_string;
      sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(query_string.assign_fmt(" SELECT snapshot_version, ddl_type, UNHEX(message) as message_unhex, status, schema_version, target_object_id FROM %s WHERE task_id = %lu",
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
        int cur_task_status = 0;
        ObDDLType ddl_type = ObDDLType::DDL_INVALID;
        ObString task_message;
        EXTRACT_UINT_FIELD_MYSQL(*result, "snapshot_version", snapshot_version, uint64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "ddl_type", ddl_type, ObDDLType);
        EXTRACT_VARCHAR_FIELD_MYSQL(*result, "message_unhex", task_message);
        EXTRACT_INT_FIELD_MYSQL(*result, "status", cur_task_status, int);
        EXTRACT_INT_FIELD_MYSQL(*result, "target_object_id", target_object_id, int64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "schema_version", schema_version, int64_t);

        task_status = static_cast<share::ObDDLTaskStatus>(cur_task_status);
        if (OB_SUCC(ret)) {
          SMART_VAR(rootserver::ObDDLTask, task) {
            if (OB_FAIL(task.deserialize_params_from_message(tenant_id, task_message.ptr(), task_message.length(), pos))) {
              LOG_WARN("deserialize from msg failed", K(ret));
            } else {
              data_format_version = task.get_data_format_version();
              is_no_logging = task.get_is_no_logging();
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

int ObDDLUtil::replace_user_tenant_id(const uint64_t tenant_id,
                                      obrpc::ObPartitionSplitArg &split_arg)
{
  int ret = OB_SUCCESS;
  if (!is_user_tenant(tenant_id)) {
    LOG_TRACE("not user tenant, no need to replace", K(tenant_id));
  } else {
    try_replace_user_tenant_id(tenant_id, split_arg.exec_tenant_id_);
  }
  return ret;
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
REPLACE_DDL_ARG_FUNC(obrpc::ObRebuildIndexArg)
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

int ObDDLUtil::check_tenant_status_normal(
    ObISQLClient *proxy,
    const uint64_t check_tenant_id)
{
  int ret = OB_SUCCESS;
  bool is_tenant_dropped = false;
  bool is_standby_tenant = false;
  const ObSimpleTenantSchema *tenant_schema = nullptr;
  ObSchemaGetterGuard schema_guard;
  if (OB_ISNULL(proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("proxy is null", K(ret));
  } else if (OB_FAIL(GSCHEMASERVICE.get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("get sys tenant schema guard failed", K(ret));
  } else if (OB_FAIL(schema_guard.check_if_tenant_has_been_dropped(check_tenant_id, is_tenant_dropped))) {
    LOG_WARN("check tenant dropped failed", K(ret), K(check_tenant_id));
  } else if (is_tenant_dropped) {
    ret = OB_TENANT_HAS_BEEN_DROPPED;
    LOG_INFO("tenant has been dropped", K(ret), K(check_tenant_id));
  } else if (OB_FAIL(schema_guard.get_tenant_info(check_tenant_id, tenant_schema))) {
    LOG_WARN("get tenant schema failed", K(ret), K(check_tenant_id));
  } else if (OB_ISNULL(tenant_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K(ret), K(check_tenant_id));
  } else if (tenant_schema->is_dropping() || tenant_schema->is_in_recyclebin()) {
    ret = OB_TENANT_HAS_BEEN_DROPPED;
    LOG_INFO("tenant in dropping or in recyclebin", K(ret), K(check_tenant_id));
  } else if (OB_FAIL(ObAllTenantInfoProxy::is_standby_tenant(proxy, check_tenant_id, is_standby_tenant))) {
    LOG_WARN("check is standby tenant failed", K(ret), K(check_tenant_id));
  } else if (is_standby_tenant) {
    ret = OB_STANDBY_READ_ONLY;
    LOG_INFO("tenant is standby", K(ret), K(check_tenant_id));
  }
  return ret;
}

int ObDDLUtil::check_schema_version_refreshed(
    const uint64_t tenant_id,
    const int64_t target_schema_version)
{
  int ret = OB_SUCCESS;
  int64_t refreshed_schema_version = 0;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || target_schema_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(tenant_id), K(target_schema_version));
  } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_refreshed_schema_version(
      tenant_id, refreshed_schema_version))) {
    LOG_WARN("get refreshed schema version failed", K(ret), K(tenant_id), K(refreshed_schema_version));
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SCHEMA_EAGAIN;
    }
  } else if (!ObSchemaService::is_formal_version(refreshed_schema_version) || refreshed_schema_version < target_schema_version) {
    ret = OB_SCHEMA_EAGAIN;
    if (REACH_TIME_INTERVAL(1000L * 1000L)) {
      LOG_INFO("tenant schema not refreshed to the target version", K(ret), K(tenant_id), K(target_schema_version), K(refreshed_schema_version));
    }
  }
  return ret;
}

bool ObDDLUtil::reach_time_interval(const int64_t i, volatile int64_t &last_time)
{
  bool bret = false;
  const int64_t old_time = last_time;
  const int64_t cur_time = common::ObTimeUtility::fast_current_time();
  if (OB_UNLIKELY((i + last_time) < cur_time)
      && old_time == ATOMIC_CAS(&last_time, old_time, cur_time))
  {
    bret = true;
  }
  return bret;
}

int ObDDLUtil::get_temp_store_compress_type(const share::schema::ObTableSchema *table_schema,
                                            const int64_t parallel,
                                            ObCompressorType &compr_type)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_schema)) {
    ret  = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(table_schema));
  } else {
    ObCompressorType schema_compr_type = table_schema->get_compressor_type();
    if (NONE_COMPRESSOR == schema_compr_type && table_schema->get_row_store_type() != FLAT_ROW_STORE) { // encoding without compress
      schema_compr_type = ZSTD_COMPRESSOR;
    }
    ret = get_temp_store_compress_type(schema_compr_type, parallel, compr_type);
  }
  return ret;
}

int ObDDLUtil::get_temp_store_compress_type(const ObCompressorType schema_compr_type,
                                            const int64_t parallel,
                                            ObCompressorType &compr_type)
{
  int ret = OB_SUCCESS;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  compr_type = NONE_COMPRESSOR;
  if (OB_UNLIKELY(!tenant_config.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail get tenant_config", K(ret), K(MTL_ID()));
  } else {
    if (0 == tenant_config->_ob_ddl_temp_file_compress_func.get_value_string().case_compare("NONE")) {
      compr_type = NONE_COMPRESSOR;
    } else if (0 == tenant_config->_ob_ddl_temp_file_compress_func.get_value_string().case_compare("ZSTD")) {
      compr_type = ZSTD_COMPRESSOR;
    } else if (0 == tenant_config->_ob_ddl_temp_file_compress_func.get_value_string().case_compare("LZ4")) {
      compr_type = LZ4_COMPRESSOR;
    } else if (0 == tenant_config->_ob_ddl_temp_file_compress_func.get_value_string().case_compare("AUTO")) {
      UNUSED(parallel);
      if (schema_compr_type > INVALID_COMPRESSOR && schema_compr_type < MAX_COMPRESSOR) {
        compr_type = schema_compr_type;
      } else {
        compr_type = NONE_COMPRESSOR;
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the temp store format config is unexpected", K(ret), K(tenant_config->_ob_ddl_temp_file_compress_func.get_value_string()));
    }
  }
  LOG_INFO("get compressor type", K(ret), K(compr_type), K(schema_compr_type));
  return ret;
}

int ObDDLUtil::check_table_compaction_checksum_error(
    const uint64_t tenant_id,
    const uint64_t table_id)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(table_id));
  } else if (OB_FAIL(check_table_column_checksum_error(tenant_id, table_id))) {
    LOG_WARN("check_table_column_checksum_error fail", KR(ret), K(tenant_id), K(table_id));
  } else if (OB_FAIL(check_tablet_checksum_error(tenant_id, table_id))) {
    LOG_WARN("check_tablet_checksum_error fail", KR(ret), K(tenant_id), K(table_id));
  }
  return ret;
}

int ObDDLUtil::check_table_column_checksum_error(
      const uint64_t tenant_id,
      const int64_t table_id)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(table_id));
  } else {
    ObSqlString query_string;
    sqlclient::ObMySQLResult *result = nullptr;
    ObTimeoutCtx timeout_ctx;
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      if OB_FAIL(ret) {
        LOG_WARN("fail to create object ObMySQLProxy::MySQLResult", KR(ret), K(tenant_id), K(table_id));
      } else if (OB_FAIL(query_string.append_fmt("SELECT data_table_id FROM %s WHERE tenant_id = %lu AND data_table_id = %lu LIMIT 1",
          OB_ALL_COLUMN_CHECKSUM_ERROR_INFO_TNAME, tenant_id, table_id))) {
        LOG_WARN("assign sql string failed", KR(ret), K(query_string));
      } else if (OB_ISNULL(GCTX.sql_proxy_)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid arg", K(ret), K(tenant_id), KP(GCTX.sql_proxy_));
      } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(timeout_ctx, GCONF.internal_sql_execute_timeout))) {
        LOG_WARN("failed to set timeout ctx", K(ret), K(timeout_ctx));
      } else if (OB_FAIL(GCTX.sql_proxy_->read(res, OB_SYS_TENANT_ID, query_string.ptr()))) {
        LOG_WARN("read record failed", K(ret), K(query_string));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get sql result", K(ret), KP(result));
      } else if (OB_FAIL(result->next()) && ret != OB_ITER_END ) {
        LOG_WARN("fail to get sql result", K(ret), KP(result));
      } else if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {
        ret = OB_NOT_SUPPORTED; // we expect the sql to return an empty result
        LOG_WARN("table index checksum error", K(ret), K(tenant_id), K(table_id));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "Redefinition on compaction checksum error table is");
      }
    }
  }
  return ret;
}

int ObDDLUtil::check_tablet_checksum_error(
      const uint64_t tenant_id,
      const int64_t table_id)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(table_id));
  } else {
    ObArray<ObTabletID> tablet_ids;
    if (OB_FAIL(ObDDLUtil::get_tablets(tenant_id, table_id, tablet_ids))) {
      LOG_WARN("fail to get tablets", K(ret), K(tenant_id), K(tablet_ids));
    } else {
      int64_t start_idx = 0;
      int64_t end_idx = min(ObDDLUtil::MAX_BATCH_COUNT, tablet_ids.count());
      while (OB_SUCC(ret) && start_idx < tablet_ids.count()) {
        if (OB_FAIL(batch_check_tablet_checksum(tenant_id, start_idx, end_idx, tablet_ids))) {
          LOG_WARN("fail to batch get teablet_ids", K(ret), K(tenant_id), K(table_id));
        } else {
          start_idx = end_idx;
          end_idx = min(start_idx + ObDDLUtil::MAX_BATCH_COUNT, tablet_ids.count());
        }
      }
    }
  }
  return ret;
}

int ObDDLUtil::check_table_empty_in_oracle_mode(
    const uint64_t tenant_id,
    const uint64_t table_id,
    ObSchemaGetterGuard &schema_guard,
    bool &is_table_empty)
{
  int ret = OB_SUCCESS;
  is_table_empty = false;
  const ObSimpleTableSchemaV2 *table_schema = nullptr;
  const ObSimpleDatabaseSchema *database_schema = nullptr;
  bool is_oracle_mode = false;
  if (OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(table_id));
  } else if (OB_FAIL(schema_guard.get_simple_table_schema(tenant_id, table_id, table_schema))) {
    LOG_WARN("get table schema failed", K(ret), K(tenant_id), K(table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table schema not exist", K(ret), K(table_id));
  } else if (OB_FAIL(table_schema->check_if_oracle_compat_mode(is_oracle_mode))) {
    LOG_WARN("fail to check is oracle mode", K(ret), K(table_schema));
  } else if (!is_oracle_mode) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("check table empty in mysql mode support later", K(ret), K(is_oracle_mode));
  } else if (OB_FAIL(schema_guard.get_database_schema(tenant_id, table_schema->get_database_id(), database_schema))) {
    LOG_WARN("get database schema failed", K(ret), K(tenant_id));
  } else if (OB_ISNULL(database_schema)) {
    ret = OB_ERR_SYS;
    LOG_WARN("get database schema failed", K(ret), K(table_id));
  } else {
    const ObString &check_expr_str = "1 != 1";
    const ObString &database_name = database_schema->get_database_name_str();
    const ObString &table_name = table_schema->get_table_name_str();
    ObSqlString sql_string;
    ObSessionParam session_param;
    session_param.sql_mode_ = nullptr;
    session_param.tz_info_wrap_ = nullptr;
    session_param.ddl_info_.set_is_ddl(true);
    session_param.ddl_info_.set_source_table_hidden(table_schema->is_user_hidden_table());
    session_param.ddl_info_.set_dest_table_hidden(false);
    ObTimeoutCtx timeout_ctx;
    ObCommonSqlProxy *sql_proxy = nullptr;
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = nullptr;
      ObSqlString ddl_schema_hint_str;
      if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(timeout_ctx, GCONF.internal_sql_execute_timeout))) {
        LOG_WARN("failed to set default timeout ctx", K(ret), K(timeout_ctx));
      } else if (OB_FAIL(ObDDLUtil::generate_ddl_schema_hint_str(table_name, table_schema->get_schema_version(), true, ddl_schema_hint_str))) {
        LOG_WARN("failed to generate ddl schema hint str", K(ret));
      } else if (OB_FAIL(sql_string.assign_fmt(
        "SELECT /*+ %.*s */ 1 FROM \"%.*s\".\"%.*s\" WHERE NOT (%.*s) AND ROWNUM = 1",
          static_cast<int>(ddl_schema_hint_str.length()), ddl_schema_hint_str.ptr(),
          static_cast<int>(database_name.length()), database_name.ptr(),
          static_cast<int>(table_name.length()), table_name.ptr(),
          static_cast<int>(check_expr_str.length()), check_expr_str.ptr()))) {
        LOG_WARN("fail to assign format", K(ret));
      }
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(sql_proxy = GCTX.ddl_oracle_sql_proxy_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sql proxy is null", K(ret));
      } else if (OB_FAIL(GCTX.ddl_oracle_sql_proxy_->read(res, table_schema->get_tenant_id(), sql_string.ptr(), &session_param))) {
        LOG_WARN("execute sql failed", K(ret), K(sql_string.ptr()));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("execute sql failed", K(ret), K(table_schema->get_tenant_id()), K(sql_string));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          is_table_empty = true;
        } else {
          LOG_WARN("iterate next result fail", K(ret), K(sql_string));
        }
      }
    }
  }
  return ret;
}

int ObDDLUtil::batch_check_tablet_checksum(
    const uint64_t tenant_id,
    const int64_t start_idx,
    const int64_t end_idx,
    const ObIArray<ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == tenant_id || start_idx < 0 || end_idx > tablet_ids.count()
      || start_idx >= end_idx || tablet_ids.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(start_idx), K(end_idx));
  } else {
    ObSqlString query_string;
    sqlclient::ObMySQLResult *result = nullptr;
    ObTimeoutCtx timeout_ctx;
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      if (OB_FAIL(ret)) {
        LOG_WARN("fail to create object ObMySQLProxy::MySQLResult", KR(ret), K(tenant_id));
      } else if (OB_FAIL(query_string.append(" SELECT tenant_id, tablet_id FROM "))) {
        LOG_WARN("assign sql string failed", K(ret), K(query_string));
      } else if (OB_FAIL(query_string.append_fmt("( SELECT tenant_id,tablet_id,row_count,data_checksum,b_column_checksums,compaction_scn FROM %s "
        " WHERE tenant_id = %lu AND tablet_id IN (", OB_ALL_TABLET_REPLICA_CHECKSUM_TNAME, tenant_id))) {
        LOG_WARN("assign sql string failed", K(ret), K(query_string));
      } else {
        for (int64_t idx = start_idx; OB_SUCC(ret) && (idx < end_idx); ++idx) {
          if (OB_FAIL(query_string.append_fmt(
            "%lu%s",
            tablet_ids.at(idx).id(),
            ((idx == end_idx - 1) ? ")) as J" : ",")))) {
            LOG_WARN("assign sql string failed", K(ret), K(tenant_id), K(tablet_ids.at(idx).id()));
          }
        } // end of for
        if (OB_SUCC(ret)) {
          if (OB_FAIL(query_string.append(" GROUP BY J.tablet_id, J.compaction_scn"
              " HAVING MIN(J.data_checksum) != MAX(J.data_checksum)"
              " OR MIN(J.row_count) != MAX(J.row_count)"
              " OR MIN(J.b_column_checksums) != MAX(J.b_column_checksums) LIMIT 1"))) {
            LOG_WARN("assign sql string failed", K(ret), K(tenant_id));
          } else if (OB_ISNULL(GCTX.sql_proxy_)) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid arg", K(ret), K(tenant_id), KP(GCTX.sql_proxy_));
          } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(timeout_ctx, GCONF.internal_sql_execute_timeout))) {
            LOG_WARN("failed to set timeout ctx", K(ret), K(timeout_ctx));
          } else if (OB_FAIL(GCTX.sql_proxy_->read(res, OB_SYS_TENANT_ID, query_string.ptr()))) {
            LOG_WARN("read record failed", K(ret), K(query_string));
          } else if ((OB_ISNULL(result = res.get_result()))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("fail to get sql result", K(ret), KP(result));
          } else if (OB_FAIL(result->next()) && ret != OB_ITER_END) {
            LOG_WARN("fail to get sql result", K(ret), KP(result));
          } else if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
          } else {
            ret = OB_NOT_SUPPORTED; // we expect the sql to return an empty result
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "Redefinition on compaction checksum error table is");
            LOG_WARN("tablet replicas checksum error", K(ret), K(tenant_id));
          }
        }
      }
    }
  }
  return ret;
}

bool ObDDLUtil::use_idempotent_mode(const int64_t data_format_version)
{
  return (GCTX.is_shared_storage_mode() && data_format_version >= DATA_VERSION_4_3_3_0);
}

int ObDDLUtil::init_macro_block_seq(const int64_t parallel_idx, blocksstable::ObMacroDataSeq &start_seq)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(parallel_idx < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(parallel_idx));
#ifdef OB_BUILD_SHARED_STORAGE
  } else if (GCTX.is_shared_storage_mode()) {
    start_seq.data_seq_ = parallel_idx * compaction::MACRO_STEP_SIZE;
#endif
  } else if (OB_FAIL(start_seq.set_parallel_degree(parallel_idx))) {
    LOG_WARN("set parallel index failed", K(ret), K(parallel_idx));
  }
  return ret;
}

bool ObDDLUtil::is_mview_not_retryable(const int64_t data_format_version, const share::ObDDLType task_type)
{
  return (task_type == DDL_MVIEW_COMPLETE_REFRESH && data_format_version >= DATA_VERSION_4_3_1_0);
}

int ObDDLUtil::set_tablet_autoinc_seq(const ObLSID &ls_id, const ObTabletID &tablet_id, const int64_t seq_value)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ls_id.is_valid() || !tablet_id.is_valid() || seq_value < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id), K(tablet_id), K(seq_value));
  } else {
    ObMigrateTabletAutoincSeqParam tablet_autoinc_param;
    obrpc::ObBatchSetTabletAutoincSeqArg arg;
    obrpc::ObBatchSetTabletAutoincSeqRes res;
    arg.tenant_id_ = MTL_ID();
    arg.ls_id_ = ls_id;
    tablet_autoinc_param.src_tablet_id_ = tablet_id;
    tablet_autoinc_param.dest_tablet_id_ = tablet_id;
    tablet_autoinc_param.autoinc_seq_ = seq_value;
    if (OB_FAIL(arg.autoinc_params_.push_back(tablet_autoinc_param))) {
      LOG_WARN("push back tablet autoinc param failed", K(ret), K(tablet_autoinc_param));
    } else if (OB_FAIL(GCTX.srv_rpc_proxy_->set_tablet_autoinc_seq(arg, res))) {
      LOG_WARN("set tablet auto inc seq failed", K(ret));
    } else if (1 != res.autoinc_params_.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected sync tablet autoinc result", K(ret), K(res));
    } else if (OB_FAIL(res.autoinc_params_.at(0).ret_code_)) {
      LOG_WARN("sync tablet autoinc failed", K(ret), K(res.autoinc_params_.at(0)));
    }
  }
  return ret;
}

int ObDDLUtil::is_major_exist(const ObLSID &ls_id, const common::ObTabletID &tablet_id, bool &is_major_exist)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  ObLSService* ls_svr = MTL(ObLSService*);
  is_major_exist = false;
  if (!ls_id.is_valid() || !tablet_id.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_ISNULL(ls_svr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service should not be null", K(ret));
  } else if (OB_FAIL(ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("failed to get ls", K(ret), K(ls_id));
  } else if (OB_FAIL(ddl_get_tablet(ls_handle, tablet_id, tablet_handle))) {
    LOG_WARN("failed to get tablet id", K(ret), K(ls_id), K(tablet_id));
  } else {
    is_major_exist = tablet_handle.get_obj()->get_major_table_count() > 0
                  || tablet_handle.get_obj()->get_tablet_meta().table_store_flag_.with_major_sstable();
  }
  return ret;
}
#ifdef OB_BUILD_SHARED_STORAGE
int ObDDLUtil::upload_block_for_ss(const char *buf, const int64_t len, const blocksstable::MacroBlockId &macro_block_id)
{
  int ret = OB_SUCCESS;
  if (nullptr == buf || 0 == len || !macro_block_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argumen", K(ret), KP(buf), K(len), K(macro_block_id));
  } else {
    ObStorageObjectHandle object_handle;
    ObStorageObjectWriteInfo object_info;
    object_info.buffer_ = buf;
    object_info.offset_ = 0;
    object_info.size_ = len;
    object_info.mtl_tenant_id_ = MTL_ID();
    object_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_WRITE);
    object_info.io_desc_.set_unsealed();
    object_info.io_desc_.set_sys_module_id(ObIOModule::SHARED_BLOCK_RW_IO);
    object_info.ls_epoch_id_ = 0;

    if (OB_FAIL(OB_STORAGE_OBJECT_MGR.async_write_object(macro_block_id, object_info, object_handle))) {
      LOG_WARN("failed to write info", K(ret), K(macro_block_id), K(object_info), K(object_handle));
    } else if (OB_FAIL(object_handle.wait())) {
      LOG_WARN("failed to wai object handle finish", K(ret));
    }
  }
  return ret;
}

/*
 used for adding gc info when ddl update tablet
 ddl may retry and generate same major which need to skip
*/
int ObDDLUtil::update_tablet_gc_info(const ObTabletID &tablet_id, const int64_t pre_snapshot_version, const int64_t new_snapshot_version)
{
  int ret = OB_SUCCESS;
  ObGCTabletMetaInfoList tablet_meta_version_list;
  ObTenantStorageMetaService *meta_service = MTL(ObTenantStorageMetaService*);
  bool is_exist = false;

  if (!tablet_id.is_valid() || OB_INVALID_TIMESTAMP == new_snapshot_version) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tablet_id), K(new_snapshot_version));
  } else if (OB_ISNULL(meta_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta service should not be null", K(ret));
  } else if (pre_snapshot_version == new_snapshot_version) {
    /* skip */
  } else if (OB_FAIL(ObTenantStorageMetaService::ss_is_meta_list_exist(tablet_id, is_exist))) {
    LOG_WARN("fail to check existence", K(ret), K(tablet_id));
  } else if (is_exist) {
    /* skip */
  } else {
    ObGCTabletMetaInfo meta_info;
    ObGCTabletMetaInfoList tablet_meta_version_list;
    if (OB_FAIL(meta_info.scn_.convert_for_tx(new_snapshot_version))) {
      LOG_WARN("fail to convert for tx", K(ret), K(new_snapshot_version));
    } else if (OB_FAIL(tablet_meta_version_list.tablet_version_arr_.push_back(meta_info))) {
      LOG_WARN("failed to push back gc info", K(ret));
    } else if (OB_FAIL(meta_service->write_gc_tablet_scn_arr(tablet_id, ObStorageObjectType::SHARED_MAJOR_META_LIST, tablet_meta_version_list))) {
      LOG_WARN("failed to write gc info arr", K(ret), K(tablet_id));
    }
  }
  return ret;
}

#endif

int ObDDLUtil::get_global_index_table_ids(const schema::ObTableSchema &table_schema, ObIArray<uint64_t> &global_index_table_ids, ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
  global_index_table_ids.reset();
  if (OB_UNLIKELY(!table_schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(table_schema), K(table_schema.is_valid()));
  } else if (OB_FAIL(table_schema.get_simple_index_infos(simple_index_infos))) {
    LOG_WARN("get simple index infos failed", K(ret));
  } else {
    const uint64_t tenant_id = table_schema.get_tenant_id();
    for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); i++) {
      const ObTableSchema *aux_table_schema = NULL;
      if (OB_FAIL(schema_guard.get_table_schema(tenant_id, simple_index_infos.at(i).table_id_, aux_table_schema))) {
        LOG_WARN("get table schema failed", K(ret), K(tenant_id), K(simple_index_infos.at(i).table_id_));
      } else if (OB_ISNULL(aux_table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table schema should not be null", K(ret));
      } else if (aux_table_schema->is_global_index_table()) {
        if (OB_FAIL(global_index_table_ids.push_back(aux_table_schema->get_table_id()))) {
          LOG_WARN("failed to push back", K(ret), K(aux_table_schema->get_table_id()));
        }
      }
    }
  }
  return ret;
}

int64_t ObDDLUtil::get_real_parallelism(const int64_t parallelism, const bool is_mv_refresh)
{
  int64_t real_parallelism = 0L;
  if (is_mv_refresh) {
    real_parallelism = std::max(2L, parallelism);
  } else {
    real_parallelism = std::min(oceanbase::ObMacroDataSeq::MAX_PARALLEL_IDX + 1, std::max(1L, parallelism));
  }
  return real_parallelism;
}
int ObDDLUtil::get_no_logging_param(const int64_t tenant_id, bool &is_no_logging)
{
  int ret = OB_SUCCESS;
  is_no_logging = false;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  if (!tenant_config.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant config is invalid", K(ret), K(tenant_id));
  } else {
    is_no_logging = tenant_config->_no_logging;
  }
  return ret;
}

int ObSqlMonitorStats::init(const uint64_t tenant_id, const int64_t task_id, const ObDDLType ddl_type)
{
  int ret = OB_SUCCESS;
  if (tenant_id == OB_INVALID_ID || task_id <= 0 || ddl_type == DDL_INVALID) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id_), K(task_id_), K(ddl_type));
  } else {
    tenant_id_ = tenant_id;
    task_id_ = task_id;
    ddl_type_ = ddl_type;
    is_inited_ = true;
  }
  return ret;
}

int ObSqlMonitorStats::clean_invalid_data(const int64_t execution_id)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (execution_id > execution_id_ && ddl_type_ != ObDDLType::DDL_CREATE_PARTITIONED_LOCAL_INDEX) {
    reuse();
  }
  execution_id_ = OB_MAX(execution_id, execution_id_);

  return ret;
}

int ObSqlMonitorStatsCollector::init(ObMySQLProxy *sql_proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString select_sql_monitor_sql;
  ObSqlString cond_sql;
  int64_t task_id_tmp = 0;
  uint64_t tenant_id_tmp = 0;
  sql_proxy_ = sql_proxy;
  if (scan_task_id_.count() == 0 || scan_tenant_id_.count() == 0 || OB_ISNULL(sql_proxy_) || !sql_proxy_->is_inited()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty scan task id", K(ret), K(scan_task_id_.count()), K(scan_tenant_id_.count()), KP(sql_proxy_));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < scan_task_id_.count() && i < scan_tenant_id_.count(); ++i) {
    task_id_tmp = scan_task_id_.at(i);
    tenant_id_tmp = scan_tenant_id_.at(i);
    if (task_id_tmp > 0 && tenant_id_tmp != OB_INVALID_ID) {
      if (i == 0) {
        if (OB_FAIL(cond_sql.assign_fmt("(TENANT_ID=%lu AND OTHERSTAT_5_VALUE='%ld') " ,tenant_id_tmp, task_id_tmp))) {
          LOG_WARN("failed to assign sql", K(ret));
        }
      } else if (OB_FAIL(cond_sql.append_fmt("OR (TENANT_ID=%lu AND OTHERSTAT_5_VALUE='%ld') " ,tenant_id_tmp, task_id_tmp))) {
          LOG_WARN("failed to assign sql", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(select_sql_monitor_sql.assign_fmt(
        "SELECT TENANT_ID, TRACE_ID, THREAD_ID, OUTPUT_ROWS, FIRST_CHANGE_TIME, LAST_CHANGE_TIME, LAST_REFRESH_TIME, PLAN_OPERATION, OTHERSTAT_5_VALUE AS TASK_ID,  "
        "OTHERSTAT_1_VALUE, OTHERSTAT_2_VALUE, OTHERSTAT_6_VALUE, OTHERSTAT_7_ID, OTHERSTAT_7_VALUE, OTHERSTAT_8_VALUE, OTHERSTAT_9_VALUE, OTHERSTAT_10_VALUE FROM %s "
        "WHERE PLAN_OPERATION in ('PHY_STAT_COLLECTOR', 'PHY_SORT', 'PHY_VEC_SORT', 'PHY_PX_MULTI_PART_SSTABLE_INSERT') AND OTHERSTAT_5_ID = '%d' AND (%s) ORDER BY OTHERSTAT_5_VALUE DESC, TENANT_ID DESC, THREAD_ID ASC",
        OB_ALL_VIRTUAL_SQL_PLAN_MONITOR_TNAME, sql::ObSqlMonitorStatIds::DDL_TASK_ID, cond_sql.ptr()))) {
      LOG_WARN("failed to assign sql", K(ret), K(select_sql_monitor_sql));
    } else {
      sqlclient::ObMySQLResult *scan_result = nullptr;
      char op_type_str[OB_MAX_OPERATOR_NAME_LENGTH] = "";
      SMART_VAR(ObMySQLProxy::MySQLResult, scan_res) {
        if (!select_sql_monitor_sql.is_valid() || OB_ISNULL(sql_proxy_)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("assign get invalid argument", K(ret), K(select_sql_monitor_sql), KP(sql_proxy_));
        } else if (OB_FAIL(sql_proxy_->read(scan_res, common::OB_SYS_TENANT_ID, select_sql_monitor_sql.ptr()))) {
          LOG_WARN("fail to execute sql", K(ret), K(select_sql_monitor_sql));
        } else if (OB_ISNULL(scan_result = scan_res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("error unexpected, query result must not be NULL", K(ret));
        } else {
          while (OB_SUCC(ret)) {
            if (OB_FAIL(scan_result->next())) {
              if (OB_ITER_END == ret) {
                ret = OB_SUCCESS;
                break;
              } else {
                LOG_WARN("failed to get next row", K(ret));
              }
            } else {
              int op_type_len = 0;
              EXTRACT_STRBUF_FIELD_MYSQL(*scan_result, "PLAN_OPERATION", op_type_str, OB_MAX_OPERATOR_NAME_LENGTH, op_type_len);
              if (OB_FAIL(ret)) {
                LOG_WARN("failed to extract str buf field", K(ret), K(op_type_str));
              } else if (strcmp(op_type_str, "PHY_STAT_COLLECTOR") == 0) { // scan monitor node
                if (OB_FAIL(get_scan_monitor_stats_batch(scan_result))) {
                  LOG_WARN("fail to execute sql", K(ret));
                }
              } else if (strcmp(op_type_str, "PHY_SORT") == 0 || strcmp(op_type_str, "PHY_VEC_SORT") == 0) { // sort monitor node
                if (OB_FAIL(get_sort_monitor_stats_batch(scan_result))) {
                  LOG_WARN("fail to execute sql", K(ret));
                }
              } else if (strcmp(op_type_str, "PHY_PX_MULTI_PART_SSTABLE_INSERT") == 0) { // insert monitor node
                if (OB_FAIL(get_insert_monitor_stats_batch(scan_result))) {
                  LOG_WARN("fail to execute sql", K(ret));
                }
              } else {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unexpected op type", K(ret), K(op_type_str));
              }
            }
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    scan_index_id_ = 0;
    sort_index_id_ = 0;
    insert_index_id_ = 0;
    is_inited_ = true;
  }
  return ret;
}

int ObSqlMonitorStatsCollector::get_scan_monitor_stats_batch(sqlclient::ObMySQLResult *scan_result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(scan_result)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("scan result is null", K(ret));
  } else {
    char trace_id_str[OB_MAX_TRACE_ID_BUFFER_SIZE] = "";
    common::ObCurTraceId::TraceId inner_sql_trace_id;
    ScanMonitorNodeInfo scan_node_info;
    EXTRACT_INT_FIELD_MYSQL(*scan_result, "TASK_ID", scan_node_info.task_id_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*scan_result, "TENANT_ID", scan_node_info.tenant_id_, uint64_t);
    EXTRACT_TIMESTAMP_FIELD_MYSQL_SKIP_RET(*scan_result, "FIRST_CHANGE_TIME", scan_node_info.first_change_time_);
    EXTRACT_TIMESTAMP_FIELD_MYSQL_SKIP_RET(*scan_result, "LAST_CHANGE_TIME", scan_node_info.last_change_time_);
    EXTRACT_TIMESTAMP_FIELD_MYSQL_SKIP_RET(*scan_result, "LAST_REFRESH_TIME", scan_node_info.last_refresh_time_);
    EXTRACT_INT_FIELD_MYSQL(*scan_result, "OUTPUT_ROWS", scan_node_info.output_rows_, int64_t);
    int trace_id_len = 0;
    EXTRACT_STRBUF_FIELD_MYSQL(*scan_result, "TRACE_ID", trace_id_str, OB_MAX_TRACE_ID_BUFFER_SIZE, trace_id_len);
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to extract field from mysql", K(ret));
    } else if (OB_FAIL(inner_sql_trace_id.parse_from_buf(trace_id_str))) {
      LOG_WARN("failed to parse trace id from buf", KR(ret), K(trace_id_str));
    } else if (FALSE_IT(scan_node_info.execution_id_ = inner_sql_trace_id.get_execution_id())) {
    } else if (OB_FAIL(scan_res_.push_back(scan_node_info))) {
      LOG_WARN("failed to push back sort monitor node info", K(ret));
    }
  }
  return ret;
}

int ObSqlMonitorStatsCollector::get_sort_monitor_stats_batch(sqlclient::ObMySQLResult *scan_result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(scan_result)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("scan result is null", K(ret));
  } else {
    char trace_id_str[OB_MAX_TRACE_ID_BUFFER_SIZE] = "";
    common::ObCurTraceId::TraceId inner_sql_trace_id;
    SortMonitorNodeInfo sort_node_info;
    EXTRACT_INT_FIELD_MYSQL(*scan_result, "TASK_ID", sort_node_info.task_id_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*scan_result, "TENANT_ID", sort_node_info.tenant_id_, uint64_t);
    EXTRACT_INT_FIELD_MYSQL(*scan_result, "THREAD_ID", sort_node_info.thread_id_, int64_t);
    EXTRACT_TIMESTAMP_FIELD_MYSQL_SKIP_RET(*scan_result, "FIRST_CHANGE_TIME", sort_node_info.first_change_time_);
    EXTRACT_TIMESTAMP_FIELD_MYSQL_SKIP_RET(*scan_result, "LAST_CHANGE_TIME", sort_node_info.last_change_time_);
    EXTRACT_INT_FIELD_MYSQL(*scan_result, "OUTPUT_ROWS", sort_node_info.output_rows_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*scan_result, "OTHERSTAT_1_VALUE", sort_node_info.row_sorted_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*scan_result, "OTHERSTAT_6_VALUE", sort_node_info.dump_size_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*scan_result, "OTHERSTAT_7_VALUE", sort_node_info.row_count_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*scan_result, "OTHERSTAT_7_ID", sort_node_info.row_count_id_, int16_t);
    EXTRACT_INT_FIELD_MYSQL(*scan_result, "OTHERSTAT_8_VALUE", sort_node_info.sort_expected_round_count_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*scan_result, "OTHERSTAT_9_VALUE", sort_node_info.merge_sort_start_time_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*scan_result, "OTHERSTAT_10_VALUE", sort_node_info.compress_type_, int64_t);
    int trace_id_len = 0;
    EXTRACT_STRBUF_FIELD_MYSQL(*scan_result, "TRACE_ID", trace_id_str, OB_MAX_TRACE_ID_BUFFER_SIZE, trace_id_len);
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to extract field from mysql", K(ret));
    } else if (OB_FAIL(inner_sql_trace_id.parse_from_buf(trace_id_str))) {
      LOG_WARN("failed to parse trace id from buf", KR(ret), K(trace_id_str));
    } else if (FALSE_IT(sort_node_info.execution_id_ = inner_sql_trace_id.get_execution_id())) {
    } else if (OB_FAIL(sort_res_.push_back(sort_node_info))) {
      LOG_WARN("failed to push back sort monitor node info", K(ret));
    }
  }
  return ret;
}

int ObSqlMonitorStatsCollector::get_insert_monitor_stats_batch(sqlclient::ObMySQLResult *scan_result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(scan_result)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("scan result is null", K(ret));
  } else {
    char trace_id_str[OB_MAX_TRACE_ID_BUFFER_SIZE] = "";
    common::ObCurTraceId::TraceId inner_sql_trace_id;
    InsertMonitorNodeInfo insert_node_info;
    EXTRACT_INT_FIELD_MYSQL(*scan_result, "TASK_ID", insert_node_info.task_id_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*scan_result, "TENANT_ID", insert_node_info.tenant_id_, uint64_t);
    EXTRACT_INT_FIELD_MYSQL(*scan_result, "THREAD_ID", insert_node_info.thread_id_, int64_t);
    EXTRACT_TIMESTAMP_FIELD_MYSQL_SKIP_RET(*scan_result, "LAST_REFRESH_TIME", insert_node_info.last_refresh_time_);
    EXTRACT_INT_FIELD_MYSQL(*scan_result, "OTHERSTAT_1_VALUE", insert_node_info.cg_row_inserted_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*scan_result, "OTHERSTAT_2_VALUE", insert_node_info.sstable_row_inserted_, int64_t);
    int trace_id_len = 0;
    EXTRACT_STRBUF_FIELD_MYSQL(*scan_result, "TRACE_ID", trace_id_str, OB_MAX_TRACE_ID_BUFFER_SIZE, trace_id_len);
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to extract field from mysql", K(ret));
    } else if (OB_FAIL(inner_sql_trace_id.parse_from_buf(trace_id_str))) {
      LOG_WARN("failed to parse trace id from buf", KR(ret), K(trace_id_str));
    } else if (FALSE_IT(insert_node_info.execution_id_ = inner_sql_trace_id.get_execution_id())) {
    } else if (OB_FAIL(insert_res_.push_back(insert_node_info))) {
      LOG_WARN("failed to push back sort monitor node info", K(ret));
    }
  }
  return ret;
}

int ObSqlMonitorStatsCollector::get_next_sql_plan_monitor_stat(ObSqlMonitorStats &sql_monitor_stats)
{
  int ret = OB_SUCCESS;
  tenant_id_ = sql_monitor_stats.tenant_id_;
  task_id_ = sql_monitor_stats.task_id_;
  ddl_type_ = sql_monitor_stats.ddl_type_;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(tenant_id_ == OB_INVALID_ID || task_id_ <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id_), K(task_id_));
  } else if (OB_FAIL(get_next_scanned_stats(sql_monitor_stats))) {
    LOG_WARN("get next scanned stats failed", K(ret));
  } else if (!sql_monitor_stats.is_empty_ && OB_FAIL(get_next_sorted_stats(sql_monitor_stats))) {
    LOG_WARN("get next sorted stats failed", K(ret));
  } else if (!sql_monitor_stats.is_empty_ && OB_FAIL(get_next_inserted_stats(sql_monitor_stats))) {
    LOG_WARN("get next inserted stats failed", K(ret));
  }
  return ret;
}

int ObSqlMonitorStatsCollector::get_next_scanned_stats(ObSqlMonitorStats &sql_monitor_stats)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  }
  for (; OB_SUCC(ret) && scan_index_id_ < scan_res_.count(); scan_index_id_++) {
    const ScanMonitorNodeInfo &scan_monitor_node = scan_res_.at(scan_index_id_);
    const uint64_t tenant_id = scan_monitor_node.tenant_id_;
    const int64_t task_id = scan_monitor_node.task_id_;
    const int64_t execution_id = scan_monitor_node.execution_id_;
    if (next_ddl_monitor_node(tenant_id, task_id)) {
      break;
    } else if (previous_ddl_monitor_node(tenant_id, task_id)) {
    } else if (outdated_monitor_node(execution_id)) {
    } else if (OB_FAIL(sql_monitor_stats.clean_invalid_data(execution_id))) {
      LOG_WARN("failed to clean invalid data", K(ret), K(execution_id));
    } else if (scan_monitor_node.output_rows_ == 0) {
    } else if (OB_FAIL(sql_monitor_stats.scan_node_.push_back(scan_monitor_node))) {
      LOG_WARN("failed to push back scan node", K(ret));
    } else {
      execution_id_ = sql_monitor_stats.execution_id_;
      sql_monitor_stats.is_empty_ = false;
    }
  }
  return ret;
}

int ObSqlMonitorStatsCollector::get_next_sorted_stats(ObSqlMonitorStats &sql_monitor_stats)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  }
  for (; OB_SUCC(ret) && sort_index_id_ < sort_res_.count(); sort_index_id_++) {
    const SortMonitorNodeInfo &sort_monitor_node = sort_res_.at(sort_index_id_);
    const uint64_t tenant_id = sort_monitor_node.tenant_id_;
    const int64_t task_id = sort_monitor_node.task_id_;
    const int64_t execution_id = sort_monitor_node.execution_id_;
    if (next_ddl_monitor_node(tenant_id, task_id)) {
      break;
    } else if (previous_ddl_monitor_node(tenant_id, task_id)) {
    } else if (outdated_monitor_node(execution_id)) {
    } else if (OB_FAIL(sql_monitor_stats.sort_node_.push_back(sort_monitor_node))) {
      LOG_WARN("failed to push back sort node", K(ret));
    }
  }
  return ret;
}

int ObSqlMonitorStatsCollector::get_next_inserted_stats(ObSqlMonitorStats &sql_monitor_stats)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  }
  for (; OB_SUCC(ret) && insert_index_id_ < insert_res_.count(); insert_index_id_++) {
    const InsertMonitorNodeInfo &insert_monitor_node = insert_res_.at(insert_index_id_);
    const uint64_t tenant_id = insert_monitor_node.tenant_id_;
    const int64_t task_id = insert_monitor_node.task_id_;
    const int64_t execution_id = insert_monitor_node.execution_id_;
    if (next_ddl_monitor_node(tenant_id, task_id)) {
      break;
    } else if (previous_ddl_monitor_node(tenant_id, task_id)) {
    } else if (outdated_monitor_node(execution_id)) {
    } else if (OB_FAIL(sql_monitor_stats.insert_node_.push_back(insert_monitor_node))) {
      LOG_WARN("failed to push back insert node", K(ret));
    }
  }
  return ret;
}

int ObDDLDiagnoseInfo::init(const uint64_t tenant_id, const int64_t task_id, const ObDDLType ddl_type, const int64_t execution_id)
{
  int ret = OB_SUCCESS;
  if (tenant_id == OB_INVALID_ID || task_id <= 0 || ddl_type == DDL_INVALID) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id_), K(task_id_), K(ddl_type));
  } else {
    tenant_id_ = tenant_id;
    task_id_ = task_id;
    ddl_type_ = ddl_type;
    finish_ddl_ = execution_id < -1 ? true : false;
    is_inited_ = true;
  }
  return ret;
}

int ObDDLDiagnoseInfo::diagnose(const ObSqlMonitorStats &sql_monitor_stats)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(calculate_sql_plan_monitor_node_info(sql_monitor_stats))) {
    LOG_INFO("failed to calculate sql plan monitor node info", K(ret));
  } else if (is_skip_case()) {
    ret = OB_EMPTY_RESULT;
  } else if (ddl_type_ == ObDDLType::DDL_CREATE_PARTITIONED_LOCAL_INDEX && execution_id_ > 1) {
    if (OB_FAIL(local_index_diagnose())) {
      LOG_WARN("failed to diagnose local index", K(ret));
    }
  } else if (finish_ddl_) {
    if (OB_FAIL(finish_ddl_diagnose())) {
      LOG_WARN("failed to diagnose finish ddl", K(ret));
    }
  } else if (is_empty_) { // before scan
  } else if (OB_FAIL(running_ddl_diagnose())) {
    LOG_WARN("failed to diagnose running ddl", K(ret));
  }
  return ret;
}

int ObDDLDiagnoseInfo::process_sql_monitor_and_generate_longops_message(const ObSqlMonitorStats &sql_monitor_stats, const int64_t target_cg_cnt, ObDDLTaskStatInfo &stat_info, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(calculate_sql_plan_monitor_node_info(sql_monitor_stats))) {
    LOG_INFO("failed to calculate sql plan monitor node info", K(ret));
  } else if (OB_FAIL(diagnose_stats_analysis())) {
    LOG_WARN("failed to diagnose stats analysis ", K(ret));
  } else if (OB_FAIL(generate_session_longops_message(target_cg_cnt, stat_info, pos))) {
    LOG_WARN("failed to generate session longops message", K(ret), K(target_cg_cnt), K(stat_info), K(pos));
  }
  return ret;
}

int ObDDLDiagnoseInfo::calculate_sql_plan_monitor_node_info(const ObSqlMonitorStats &sql_monitor_stats)
{
  int ret = OB_SUCCESS;
  if (FALSE_IT(execution_id_ = sql_monitor_stats.execution_id_)) {
  } else if (sql_monitor_stats.is_empty_) {
  } else if (OB_FAIL(calculate_scan_monitor_node_info(sql_monitor_stats))) {
    LOG_WARN("failed to calculate scan monitor node info", K(ret));
  } else if (OB_FAIL(calculate_sort_and_insert_info(sql_monitor_stats))) {
    LOG_WARN("failed to calculate sort and insert info", K(ret));
  } else {
    is_empty_ = false;
  }
  return ret;
}

int ObDDLDiagnoseInfo::calculate_scan_monitor_node_info(const ObSqlMonitorStats &sql_monitor_stats)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < sql_monitor_stats.scan_node_.count(); ++i) {
    const ScanMonitorNodeInfo &scan_monitor_node = sql_monitor_stats.scan_node_.at(i);
    row_scanned_ += scan_monitor_node.output_rows_;
    max_row_scan_ = OB_MAX(max_row_scan_, scan_monitor_node.output_rows_);
    min_row_scan_ = OB_MIN(min_row_scan_, scan_monitor_node.output_rows_);
    scan_start_time_ = OB_MAX(scan_start_time_, scan_monitor_node.first_change_time_);
    scan_end_time_ = OB_MAX(scan_end_time_, scan_monitor_node.last_change_time_);
    if (scan_monitor_node.last_refresh_time_ == 0) {
      scan_thread_num_++;
    }
  }
  return ret;
}

int ObDDLDiagnoseInfo::calculate_sort_and_insert_info(const ObSqlMonitorStats &sql_monitor_stats)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < sql_monitor_stats.sort_node_.count(); ++i) {
    const SortMonitorNodeInfo &sort_monitor_node = sql_monitor_stats.sort_node_.at(i);
    parallelism_++;
    int64_t row_sorted_tmp = sort_monitor_node.row_sorted_;
    if (row_sorted_tmp == 0) {
      continue;
    }
    real_parallelism_++;
    int64_t row_count_tmp = 0;
    if (sort_monitor_node.row_count_id_ != sql::ObSqlMonitorStatIds::ROW_COUNT) {
      row_count_tmp = sort_monitor_node.output_rows_;
    } else {
      row_count_tmp = sort_monitor_node.row_count_;
    }
    if (row_count_tmp == 0) {
      continue;
    } else if (OB_FAIL(calculate_inmem_sort_info(row_sorted_tmp, row_count_tmp, sort_monitor_node.first_change_time_, sort_monitor_node.thread_id_))) { // inmem sort
      LOG_WARN("failed to calculate inmem sort info", K(ret));
    } else if (OB_FAIL(calculate_merge_sort_info(row_count_tmp, row_sorted_tmp, sort_monitor_node))) {
      LOG_WARN("failed to calculate merge sort info", K(ret));
    } else if (OB_FAIL(calculate_insert_info(row_count_tmp, sort_monitor_node, sql_monitor_stats))) {
      LOG_WARN("failed to calculate insert info", K(ret));
    }
  }
  return ret;
}

int ObDDLDiagnoseInfo::calculate_inmem_sort_info(
    const int64_t row_sorted,
    const int64_t row_count,
    const int64_t first_change_time,
    const int64_t thread_id)
{
  int ret = OB_SUCCESS;
  if (0 == row_sorted || 0 == row_count) {
  } else if (row_sorted <= row_count) {
    row_sorted_ += row_sorted;
    if (0 == first_change_time) {
      double inmem_sort_progress_tmp = static_cast<double>(row_sorted) / row_count;
      if (inmem_sort_progress_tmp > 0) {
        int64_t spend_time = ObTimeUtility::fast_current_time() - scan_end_time_;
        double inmem_sort_remain_time = spend_time / inmem_sort_progress_tmp - spend_time;
        inmem_sort_thread_num_++;
        inmem_sort_remain_time_ = OB_MAX(inmem_sort_remain_time_, inmem_sort_remain_time);
        if (inmem_sort_progress_tmp <= inmem_sort_progress_) {
          inmem_sort_spend_time_ = spend_time;
          inmem_sort_slowest_thread_id_ = thread_id;
          min_inmem_sort_row_ = row_sorted;
          inmem_sort_progress_ = inmem_sort_progress_tmp;
        }
      }
    }
  }
  return ret;
}

int ObDDLDiagnoseInfo::calculate_merge_sort_info(
    const int64_t row_count,
    const int64_t row_sorted,
    const SortMonitorNodeInfo &sort_monitor_node)
{
  int ret = OB_SUCCESS;
  dump_size_ += sort_monitor_node.dump_size_;
  compress_type_ = sort_monitor_node.compress_type_;
  if (row_sorted > row_count && row_count > 0) {
    int64_t real_merge_count = row_sorted - row_count;
    row_sorted_ += row_count;
    row_merge_sorted_ += real_merge_count;
    int64_t expected_round_tmp = sort_monitor_node.sort_expected_round_count_;
    if (expected_round_tmp > 0 && sort_monitor_node.first_change_time_ == 0) { // first_change_time_ > 0 means sort phase has finished
      double merge_sort_progress_tmp = static_cast<double>(real_merge_count) / (row_count * expected_round_tmp);
      int64_t spend_time = ObTimeUtility::fast_current_time() - sort_monitor_node.merge_sort_start_time_;
      if (merge_sort_progress_tmp > 0) {
        double merge_sort_remain_time = spend_time / merge_sort_progress_tmp - spend_time;
        merge_sort_thread_num_++;
        merge_sort_remain_time_ = OB_MAX(merge_sort_remain_time_, merge_sort_remain_time);
        if (merge_sort_progress_tmp <= merge_sort_progress_) {
          merge_sort_spend_time_ = spend_time;
          merge_sort_slowest_thread_id_ = sort_monitor_node.thread_id_;
          min_merge_sort_row_ = real_merge_count;
          merge_sort_progress_ = merge_sort_progress_tmp;
        }
      }
    }
  }
  return ret;
}

int ObDDLDiagnoseInfo::calculate_insert_info(
    const int64_t row_count,
    const SortMonitorNodeInfo &sort_info,
    const ObSqlMonitorStats &sql_monitor_stats)
{
  int ret = OB_SUCCESS;
  int64_t thread_id = sort_info.thread_id_;
  int64_t change_time = sort_info.first_change_time_;
  if (row_count > row_max_) {
    row_max_ = row_count;
    row_max_thread_ = thread_id;
  }

  if (row_min_ == 0 || row_count < row_min_) {
    row_min_ = row_count;
    row_min_thread_ = thread_id;
  }
  if (change_time > 0) {
    sort_end_time_ = OB_MAX(sort_end_time_, change_time);
    while (OB_SUCC(ret) && thread_index_ < sql_monitor_stats.insert_node_.count()) {
      const InsertMonitorNodeInfo &insert_monitor_node = sql_monitor_stats.insert_node_.at(thread_index_);
      uint64_t thread_id_tmp = insert_monitor_node.thread_id_;
      if (thread_id_tmp < thread_id || (thread_id_tmp == thread_id && insert_monitor_node.execution_id_ < sort_info.execution_id_)) {
      } else if (thread_id_tmp > thread_id || insert_monitor_node.execution_id_ > sort_info.execution_id_ ) {
        break;
      } else {
        int64_t row_inserted_file_tmp = insert_monitor_node.sstable_row_inserted_;
        row_inserted_file_ += row_inserted_file_tmp;
        row_inserted_cg_ += insert_monitor_node.cg_row_inserted_;

        int64_t finish_time_tmp = insert_monitor_node.last_refresh_time_;
        if (finish_time_tmp > insert_end_time_) {
          insert_end_time_ = finish_time_tmp;
          slowest_thread_id_ = thread_id_tmp;
        }
        if (0 == row_inserted_file_tmp || 0 == row_count) {
        } else if (row_inserted_file_tmp < row_count) {
          double insert_progress_tmp = static_cast<double>(row_inserted_file_tmp) / row_count;
          int64_t spend_time = ObTimeUtility::fast_current_time() - change_time;
          if (insert_progress_tmp > 0) {
            double remain_time = spend_time / insert_progress_tmp - spend_time;
            insert_thread_num_++;
            insert_remain_time_ = OB_MAX(insert_remain_time_, remain_time);
            if (insert_progress_tmp <= insert_progress_) {
              insert_spend_time_ = spend_time;
              insert_slowest_thread_id_ = thread_id_tmp;
              min_insert_row_ = row_inserted_file_tmp;
              insert_progress_ = insert_progress_tmp;
            }
          }
        }
      }
     thread_index_++;
    }
  }
  return ret;
}

int ObDDLDiagnoseInfo::local_index_diagnose()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(diagnose_message_, common::OB_DIAGNOSE_INFO_LENGTH, pos_,
                              "build local index batch num: %ld, "
                              "THREAD_INFO: { parallel_num : %ld, row_max: %ld, row_max_thread_id: %ld, row_min: %ld, row_min_thread_id: %ld }",
                              execution_id_, parallelism_, row_max_, row_max_thread_, row_min_, row_min_thread_))) {
    LOG_WARN("failed to print message", K(ret), K(diagnose_message_), K(pos_));
  } else if (is_thread_without_data()
             && OB_FAIL(databuff_printf(diagnose_message_, common::OB_DIAGNOSE_INFO_LENGTH, pos_,
                                        ", DIAGNOSE_CASE:{ The number of threads with data is less than the dop. real_parallelism: %ld }",
                                        real_parallelism_))) {
    LOG_WARN("failed to print diagnose message", K(diagnose_message_), K(pos_), K(ret));
  }
  return ret;
}

int ObDDLDiagnoseInfo::finish_ddl_diagnose()
{
  int ret = OB_SUCCESS;
  double scan_time = OB_MAX(0.0, static_cast<double>(scan_end_time_ - scan_start_time_) / (1000 * 1000));
  double sort_time = OB_MAX(0.0, static_cast<double>(sort_end_time_ - scan_end_time_) / (1000 * 1000));
  double insert_time = OB_MAX(0.0, static_cast<double>(insert_end_time_ - sort_end_time_) / (1000 * 1000));
  if (execution_id_ > 1 && OB_FAIL(databuff_printf(diagnose_message_, common::OB_DIAGNOSE_INFO_LENGTH, pos_, "try count: %ld, ", execution_id_))) {
    LOG_WARN("failed to print ddl try count message", K(ret));
  } else if (OB_FAIL(databuff_printf(diagnose_message_, common::OB_DIAGNOSE_INFO_LENGTH, pos_,
                                     "THREAD_INFO: { parallel_num : %ld, row_max: %ld, row_max_thread_id: %ld, row_min: %ld, row_min_thread_id: %ld slowest_thread_id: %ld }, "
                                     "TIME_INFO: { scan_time: %.3fs, sort_time: %.3fs, insert_time: %.3fs }",
                                     parallelism_, row_max_, row_max_thread_, row_min_, row_min_thread_, slowest_thread_id_,
                                     scan_time, sort_time, insert_time))) {
    LOG_WARN("failed to print message", K(ret));
  } else if (OB_FAIL(check_diagnose_case())) {
    LOG_WARN("failed to check diagnose case", K(ret));
  }
  return ret;
}

int ObDDLDiagnoseInfo::running_ddl_diagnose()
{
  int ret = OB_SUCCESS;
  if (execution_id_ > 1 && OB_FAIL(databuff_printf(diagnose_message_, common::OB_DIAGNOSE_INFO_LENGTH, pos_, "try count: %ld, ", execution_id_))) {
    LOG_WARN("failed to print ddl try count message", K(ret));
  } else if (real_parallelism_ == 0) {
    if (OB_FAIL(databuff_printf(diagnose_message_, common::OB_DIAGNOSE_INFO_LENGTH, pos_, "Scanning"))) {
      LOG_WARN("failed to print message", K(ret));
    }
  } else if (OB_FAIL(databuff_printf(diagnose_message_, common::OB_DIAGNOSE_INFO_LENGTH, pos_,
                                     "THREAD_INFO: { parallel_num : %ld, row_max: %ld, row_max_thread_id: %ld, row_min: %ld, row_min_thread_id: %ld }",
                                     parallelism_, row_max_, row_max_thread_, row_min_, row_min_thread_))) {
    LOG_WARN("failed to print thread info message", K(ret));
  } else if (OB_FAIL(check_diagnose_case())) {
    LOG_WARN("failed to check diagnose case", K(ret));
  }
  return ret;
}

int ObDDLDiagnoseInfo::check_diagnose_case()
{
  int ret = OB_SUCCESS;
  if (is_data_skew() || is_thread_without_data()) {
    if (OB_FAIL(databuff_printf(diagnose_message_, common::OB_DIAGNOSE_INFO_LENGTH, pos_, ", DIAGNOSE_CASE: {"))) {
      LOG_WARN("failed to print diagnose message", K(ret));
    } else if (OB_SUCC(ret)
              && is_data_skew()
              && OB_FAIL(databuff_printf(diagnose_message_, common::OB_DIAGNOSE_INFO_LENGTH, pos_,
                                        " The data skew is significant, with a low sampling rate or uneven sampling."))) {
      LOG_WARN("failed to print diagnose message", K(ret));
    } else if (OB_SUCC(ret)
              && is_thread_without_data()
              && OB_FAIL(databuff_printf(diagnose_message_, common::OB_DIAGNOSE_INFO_LENGTH, pos_,
                                          " The number of threads with data is less than the dop. real_parallelism: %ld.",
                                          real_parallelism_))) {
      LOG_WARN("failed to print diagnose message", K(ret));
    }  else if (OB_SUCC(ret)
              && OB_FAIL(databuff_printf(diagnose_message_, common::OB_DIAGNOSE_INFO_LENGTH, pos_,
                                          " }"))) {
      LOG_WARN("failed to print diagnose message", K(ret));
    }
  }
  return ret;
}

int ObDDLDiagnoseInfo::diagnose_stats_analysis()
{
  int ret = OB_SUCCESS;
  if (row_scanned_ == 0) {
    state_ = RedefinitionState::BEFORESCAN;
  } else if (scan_thread_num_ > 0 || row_sorted_ == 0) {
    parallelism_ = scan_thread_num_;
    state_ = RedefinitionState::SCAN;
    scan_spend_time_ = ObTimeUtility::fast_current_time() - scan_start_time_;
  } else {
    parallelism_ = inmem_sort_thread_num_ + merge_sort_thread_num_ + insert_thread_num_;
    if (inmem_sort_thread_num_ > 0) {
      state_ = RedefinitionState::INMEM_SORT;
    } else if (merge_sort_thread_num_ > 0) {
      state_ = RedefinitionState::MERGE_SORT;
    } else if (insert_thread_num_ > 0){
      state_ = RedefinitionState::INSERT;
    } else {
      state_ = RedefinitionState::DDL_DIAGNOSE_V1;
    }
  }
  return ret;
}

ObDDLTaskStatInfo::ObDDLTaskStatInfo()
  : start_time_(0), finish_time_(0), time_remaining_(0), percentage_(0), op_name_(), target_(), message_()
{
}

int ObDDLTaskStatInfo::init(const char *&ddl_type_str, const uint64_t table_id)
{
  int ret = OB_SUCCESS;
  MEMSET(op_name_, 0, common::MAX_LONG_OPS_NAME_LENGTH);
  MEMSET(target_, 0, common::MAX_LONG_OPS_TARGET_LENGTH);
  if (OB_FAIL(databuff_printf(op_name_, common::MAX_LONG_OPS_NAME_LENGTH, "%s", ddl_type_str))) {
    LOG_WARN("failed to print ddl type str", K(ret));
  } else if (OB_FAIL(databuff_printf(target_, common::MAX_LONG_OPS_TARGET_LENGTH, "%lu", table_id))) {
    LOG_WARN("failed to print ddl table name", K(ret), K(table_id));
  } else {
    start_time_ = ObTimeUtility::current_time();
  }
  return ret;
}

int ObDDLDiagnoseInfo::generate_session_longops_message(const int64_t target_cg_cnt, ObDDLTaskStatInfo &stat_info, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (ddl_type_ == share::ObDDLType::DDL_CREATE_PARTITIONED_LOCAL_INDEX
      && execution_id_ > 1
      && OB_FAIL(databuff_printf(stat_info.message_,
                                 MAX_LONG_OPS_MESSAGE_LENGTH,
                                 pos,
                                 "build local index batch num: %ld, ",
                                 execution_id_))) {
    LOG_WARN("failed to print", K(ret));
  } else if (state_ == RedefinitionState::DDL_DIAGNOSE_V1 || target_cg_cnt > 1) {
    if (OB_FAIL(generate_session_longops_message_v1(target_cg_cnt, stat_info, pos))) {
      LOG_WARN("failed to print", K(ret));
    }
  } else {
    switch (state_) {
      case RedefinitionState::BEFORESCAN: {
        if (OB_FAIL(databuff_printf(stat_info.message_,
                                    MAX_LONG_OPS_MESSAGE_LENGTH,
                                    pos,
                                    "STATUS: REPLICA BUILD, BEFORE-SCAN"))) {
          LOG_WARN("failed to print", K(ret));
        }
        break;
      }

      case RedefinitionState::SCAN: {
        if (OB_FAIL(databuff_printf(stat_info.message_,
                                    MAX_LONG_OPS_MESSAGE_LENGTH,
                                    pos,
                                    "STATUS: REPLICA BUILD, SCANNING, PARALLELISM: %ld, "
                                    "ROW_COUNT_INFO:{ ROW_SCANNED: %ld, ROW_SORTED: %ld, ROW_INSERTED: %ld }, "
                                    "SCAN_INFO:{ SCAN_TIME_ELAPSED: %.3fs, MAX_THREAD_ROW_SCANNED: %ld, MIN_THREAD_ROW_SCANNED: %ld }",
                                    parallelism_,
                                    row_scanned_, row_sorted_ + row_merge_sorted_, row_inserted_file_,
                                    scan_spend_time_ / (1000 * 1000), max_row_scan_, min_row_scan_))) {
          LOG_WARN("failed to print", K(ret));
        }
        break;
      }

      case RedefinitionState::INMEM_SORT: {
        if (OB_FAIL(databuff_printf(stat_info.message_,
                                    MAX_LONG_OPS_MESSAGE_LENGTH,
                                    pos,
                                    "STATUS: REPLICA BUILD, SORT_PHASE1, PARALLELISM: %ld, SORT_PHASE1_THREAD_NUM: %ld, "
                                    "ROW_COUNT_INFO:{ ROW_SCANNED: %ld, ROW_SORTED: %ld, ROW_INSERTED: %ld }, "
                                    "SORT_PHASE1_PROGRESS_INFO:{ SORT_PHASE1_TIME_ELAPSED: %.3fs, SORT_PHASE1_PROGRESS: %.2f%%, SORT_PHASE1_TIME_REMAINING: %.3fs }, "
                                    "SLOWEST_THREAD_INFO:{ THREAD_ID: %ld, SORTED_ROW_COUNT: %ld }",
                                    parallelism_, inmem_sort_thread_num_,
                                    row_scanned_, row_sorted_ + row_merge_sorted_, row_inserted_file_,
                                    inmem_sort_spend_time_ / (1000 * 1000), inmem_sort_progress_ * 100, inmem_sort_remain_time_ / (1000 * 1000),
                                    inmem_sort_slowest_thread_id_, min_inmem_sort_row_))) {
          LOG_WARN("failed to print", K(ret));
        }
        break;
      }
      case RedefinitionState::MERGE_SORT: {
        if (OB_FAIL(databuff_printf(stat_info.message_,
                                    MAX_LONG_OPS_MESSAGE_LENGTH,
                                    pos,
                                    "STATUS: REPLICA BUILD, SORT_PHASE2, PARALLELISM: %ld, SORT_PHASE2_THREAD_NUM: %ld, "
                                    "ROW_COUNT_INFO:{ ROW_SCANNED: %ld, ROW_SORTED: %ld, ROW_INSERTED: %ld }, "
                                    "SORT_PHASE2_PROGRESS_INFO:{ SORT_PHASE2_TIME_ELAPSED: %.3fs, SORT_PHASE2_PROGRESS: %.2f%%, SORT_PHASE2_TIME_REMAINING: %.3fs }, "
                                    "SLOWEST_THREAD_INFO:{ THREAD_ID: %ld, SORTRD_ROW_COUNT: %ld }, "
                                    "TEMP_FILE_INFO:{ DUMP_SIZE: %ld, COMPRESS_TYPE: %s }",
                                    parallelism_, merge_sort_thread_num_,
                                    row_scanned_, row_sorted_ + row_merge_sorted_, row_inserted_file_,
                                    merge_sort_spend_time_ / (1000 * 1000), merge_sort_progress_ * 100, merge_sort_remain_time_/ (1000 * 1000),
                                    merge_sort_slowest_thread_id_, min_merge_sort_row_,
                                    dump_size_, all_compressor_name[compress_type_]))) {
          LOG_WARN("failed to print", K(ret));
        }
        break;
      }
      case RedefinitionState::INSERT: {
        if (OB_FAIL(databuff_printf(stat_info.message_,
                                    MAX_LONG_OPS_MESSAGE_LENGTH,
                                    pos,
                                    "STATUS: REPLICA BUILD, INSERT, PARALLELISM: %ld, INSERT_THREAD: %ld, "
                                    "ROW_COUNT_INFO:{ ROW_SCANNED: %ld, ROW_SORTED: %ld, ROW_INSERTED: %ld }, "
                                    "INSERT_PROGRESS_INFO:{ INSERT_TIME_ELAPSED: %.3fs, INSERT_PROGRESS: %.2f%%, INSERT_TIME_REMAINING: %.3fs }, "
                                    "SLOWEST_THREAD_INFO:{ THREAD_ID: %ld, INSERTED_ROW_COUNT: %ld }",
                                    parallelism_, insert_thread_num_,
                                    row_scanned_, row_sorted_ + row_merge_sorted_, row_inserted_file_,
                                    insert_spend_time_ / (1000 * 1000), insert_progress_ * 100, insert_remain_time_ / (1000 * 1000),
                                    insert_slowest_thread_id_, min_insert_row_))) {
          LOG_WARN("failed to print", K(ret));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("not expected status", K(ret), K(state_), K(*this));
        break;
      }
    }
  }
  return ret;
}

int ObDDLDiagnoseInfo::generate_session_longops_message_v1(const int64_t target_cg_cnt, ObDDLTaskStatInfo &stat_info, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (target_cg_cnt > 1) {
    if (OB_FAIL(databuff_printf(stat_info.message_,
                                MAX_LONG_OPS_MESSAGE_LENGTH,
                                pos,
                                "STATUS: REPLICA BUILD, PARALLELISM: %ld, ROW_SCANNED: %ld, ROW_SORTED: %ld, ROW_INSERTED_INTO_TMP_FILE: %ld, ROW_INSERTED: %ld out of %ld column group rows",
                                ObDDLUtil::get_real_parallelism(parallelism_, false/*is mv refresh*/),
                                row_scanned_,
                                row_sorted_ + row_merge_sorted_,
                                row_inserted_file_,
                                row_inserted_cg_,
                                row_scanned_ * target_cg_cnt))) {
      LOG_WARN("failed to print", K(ret));
    }
  } else {
    if (OB_FAIL(databuff_printf(stat_info.message_,
                                MAX_LONG_OPS_MESSAGE_LENGTH,
                                pos,
                                "STATUS: REPLICA BUILD, PARALLELISM: %ld, ROW_SCANNED: %ld, ROW_SORTED: %ld, ROW_INSERTED: %ld",
                                ObDDLUtil::get_real_parallelism(parallelism_, false/*is mv refresh*/),
                                row_scanned_,
                                row_sorted_ + row_merge_sorted_,
                                row_inserted_file_))) {
    LOG_WARN("failed to print", K(ret));
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
    if (OB_TMP_FAIL(proxy.wait_all(return_ret_array))) {
      LOG_WARN("rpc proxy wait failed", K(tmp_ret));
      ret = OB_SUCCESS == ret ? tmp_ret : ret;
    } else if (OB_FAIL(ret)) {
    } else if (OB_FAIL(proxy.check_return_cnt(return_ret_array.count()))) {
      LOG_WARN("return cnt not match", KR(ret), "return_cnt", return_ret_array.count());
    } else {
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
  const ObIArray<ObTabletID> &tablet_ids,
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
  } else if (OB_FAIL(DDL_SIM(tenant_id, ddl_task_id, CHECK_TABLET_CHECKSUM_STATUS_FAILED))) {
    LOG_WARN("ddl sim failure", K(ret), K(tenant_id), K(ddl_task_id));
  } else if (OB_FAIL(tablet_checksum_status_map.create(tablet_count, ObModIds::OB_SSTABLE_CREATE_INDEX))) {
    LOG_WARN("fail to create column checksum map", K(ret));
  } else if (OB_ISNULL(GCTX.root_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rootservice is null", K(ret));
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
#ifdef ERRSIM
  if (GCONF.errsim_ddl_major_delay_time.get() > 0) {
    return OB_SUCCESS;
  }
#endif
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || OB_INVALID_ID == index_table_id ||
      ddl_task_id == OB_INVALID_ID || execution_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to check report checksum finished", K(ret), K(tenant_id), K(index_table_id), K(execution_id), K(ddl_task_id));
  } else if (OB_FAIL(ObDDLUtil::get_tablets(tenant_id, index_table_id, dest_tablet_ids))) {
    LOG_WARN("fail to get tablets", K(ret), K(tenant_id), K(index_table_id));
  } else if (OB_FALSE_IT(lib::ob_sort(dest_tablet_ids.begin(), dest_tablet_ids.end()))) { // sort in ASC order.
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
  } else if (OB_FAIL(DDL_SIM(tenant_id, ddl_task_id, CHECK_OLD_COMPLEMENT_TASK_FAILED))) {
    LOG_WARN("ddl sim failure: check old complement task failed", K(ret), K(tenant_id), K(ddl_task_id));
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

int ObCODDLUtil::get_base_cg_idx(const storage::ObStorageSchema *storage_schema, int64_t &base_cg_idx)
{
  int ret = OB_SUCCESS;
  base_cg_idx = -1;
  if (OB_UNLIKELY(nullptr == storage_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(storage_schema));
  } else {
    bool found_base_cg_idx = false;
    const ObIArray<ObStorageColumnGroupSchema> &cg_schemas = storage_schema->get_column_groups();
    for (int64_t i = 0; OB_SUCC(ret) && !found_base_cg_idx && i < cg_schemas.count(); ++i) {
      const ObStorageColumnGroupSchema &cur_cg_schmea = cg_schemas.at(i);
      if (cur_cg_schmea.is_all_column_group() || cur_cg_schmea.is_rowkey_column_group()) {
        base_cg_idx = i;
        found_base_cg_idx = true;
      }
    }
    if (OB_SUCC(ret) && !found_base_cg_idx) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("base columng group schema not found", K(ret));
    }
  }
  LOG_DEBUG("get base cg idx", K(ret), K(base_cg_idx));
  return ret;
}

int ObCODDLUtil::get_column_checksums(
    const storage::ObCOSSTableV2 *co_sstable,
    const storage::ObStorageSchema *storage_schema,
    ObIArray<int64_t> &column_checksums)
{
  int ret = OB_SUCCESS;
  column_checksums.reset();
  int64_t column_count = 0;
  if (OB_UNLIKELY(nullptr == co_sstable || nullptr == storage_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(co_sstable), KP(storage_schema));
  } else if (OB_FAIL(storage_schema->get_stored_column_count_in_sstable(column_count))) {
    LOG_WARN("fail to get_stored_column_count_in_sstable", K(ret), KPC(storage_schema));
  } else {
    const common::ObIArray<ObStorageColumnGroupSchema> &column_groups = storage_schema->get_column_groups();
    ObArray<bool/*checksum_ready*/> checksum_ready_array;
    if (OB_FAIL(checksum_ready_array.reserve(column_count))) {
      LOG_WARN("reserve checksum ready array failed", K(ret), K(column_count));
    } else if (OB_FAIL(column_checksums.reserve(column_count))) {
      LOG_WARN("reserve checksum array failed", K(ret), K(column_count));
    }
    for (int64_t i = 0; i < column_count && OB_SUCC(ret); i ++) {
      if (OB_FAIL(checksum_ready_array.push_back(false))) {
        LOG_WARN("push back ready flag failed", K(ret), K(i));
      } else if (OB_FAIL(column_checksums.push_back(0))) {
        LOG_WARN("fail to push back column checksum", K(ret), K(i));
      }
    }
    ObSSTableWrapper cg_sstable_wrapper;
    ObSSTable *cg_sstable = nullptr;
    for (int64_t i = 0; !co_sstable->is_cgs_empty_co_table() && i < column_groups.count() && OB_SUCC(ret); i++) {
      const ObStorageColumnGroupSchema &column_group = column_groups.at(i);
      ObSSTableMetaHandle cg_table_meta_hdl;
      if (column_group.is_all_column_group()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected column_group", K(ret), K(i));
      } else if (OB_FAIL(co_sstable->fetch_cg_sstable(i, cg_sstable_wrapper))) {
        LOG_WARN("fail to get cg sstable", K(ret), K(i));
      } else if (OB_FAIL(cg_sstable_wrapper.get_loaded_column_store_sstable(cg_sstable))) {
        LOG_WARN("get sstable failed", K(ret));
      } else if (OB_UNLIKELY(cg_sstable == nullptr || !cg_sstable->is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpec cg sstable", K(ret), KPC(cg_sstable));
      } else if (OB_FAIL(cg_sstable->get_meta(cg_table_meta_hdl))) {
        LOG_WARN("fail to get meta", K(ret), KPC(cg_sstable));
      } else if (OB_UNLIKELY(cg_table_meta_hdl.get_sstable_meta().get_col_checksum_cnt() != column_group.get_column_count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected col_checksum_cnt", K(ret),
            K(cg_table_meta_hdl.get_sstable_meta().get_col_checksum_cnt()), K(column_group.get_column_count()));
      } else {
        for (int64_t j = 0; j < column_group.get_column_count() && OB_SUCC(ret); j++) {
          const uint16_t column_idx = column_group.get_column_idx(j);
          if (column_idx < 0 || column_idx >= column_checksums.count()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid column index", K(ret), K(i), K(j), K(column_idx), K(column_checksums.count()));
          } else {
            int64_t &column_checksum = column_checksums.at(column_idx);
            bool &is_checksum_ready = checksum_ready_array.at(column_idx);
            if (!is_checksum_ready) {
              column_checksum = cg_table_meta_hdl.get_sstable_meta().get_col_checksum()[j];
              is_checksum_ready = true;
            } else if (OB_UNLIKELY(column_checksum != cg_table_meta_hdl.get_sstable_meta().get_col_checksum()[j])) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected col_checksum_cnt", K(ret), K(column_checksum), K(cg_table_meta_hdl.get_sstable_meta().get_col_checksum()[j]));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObCODDLUtil::is_rowkey_based_co_sstable(
    const storage::ObCOSSTableV2 *co_sstable,
    const storage::ObStorageSchema *storage_schema,
    bool &is_rowkey_based)
{
  int ret = OB_SUCCESS;
  is_rowkey_based = false;
  if (OB_UNLIKELY(nullptr == co_sstable || nullptr == storage_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(co_sstable), KP(storage_schema));
  } else {
    const int64_t base_cg_idx = co_sstable->get_key().get_column_group_id();
    if (base_cg_idx < 0 || base_cg_idx >= storage_schema->get_column_groups().count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid base column group index", K(ret), K(base_cg_idx));
    } else {
      is_rowkey_based = storage_schema->get_column_groups().at(base_cg_idx).is_rowkey_column_group();
    }
  }
  return ret;
}



int ObCODDLUtil::need_column_group_store(const storage::ObStorageSchema &table_schema, bool &need_column_group)
{
  int ret = OB_SUCCESS;
  need_column_group = table_schema.get_column_group_count() > 1;
  return ret;
}

int ObCODDLUtil::need_column_group_store(const schema::ObTableSchema &table_schema, bool &need_column_group)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(table_schema.get_is_column_store(need_column_group))) {
      SHARE_LOG(WARN, "fail to check whether table is column store", K(ret));
  }
  return ret;
}

// for partition split.
int ObSplitUtil::deserializ_parallel_datum_rowkey(
      common::ObIAllocator &rowkey_allocator,
      const char *buf, const int64_t data_len, int64_t &pos,
      ObIArray<blocksstable::ObDatumRowkey> &parallel_datum_rowkey_list)
{
  int ret = OB_SUCCESS;
  parallel_datum_rowkey_list.reset();
  if (pos == data_len) {
    LOG_INFO("no parallel info", K(pos), K(data_len), KP(buf));
  } else if (OB_UNLIKELY(nullptr == buf || pos > data_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), KP(buf), K(pos), K(data_len));
  } else {
    int64_t rowkey_arr_cnt = 0;
    LST_DO_CODE(OB_UNIS_DECODE, rowkey_arr_cnt);
    if (FAILEDx(parallel_datum_rowkey_list.prepare_allocate(rowkey_arr_cnt))) {
      LOG_WARN("reserve failed", K(ret), K(rowkey_arr_cnt));
    } else {
      ObStorageDatum tmp_storage_datum[OB_INNER_MAX_ROWKEY_COLUMN_NUMBER];
      ObDatumRowkey tmp_datum_rowkey;
      tmp_datum_rowkey.assign(tmp_storage_datum, OB_INNER_MAX_ROWKEY_COLUMN_NUMBER);
      for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_arr_cnt; i++) {
        if (OB_FAIL(tmp_datum_rowkey.deserialize(buf, data_len, pos))) {
          LOG_WARN("failed to decode concurrent cnt", K(ret), K(i), K(rowkey_arr_cnt), K(data_len), K(pos));
        } else if (OB_FAIL(tmp_datum_rowkey.deep_copy(parallel_datum_rowkey_list.at(i), rowkey_allocator))) {
          LOG_WARN("failed to deep copy end key", K(ret), K(i), K(tmp_datum_rowkey));
        }
      }
    }
  }
  return ret;
}
OB_SERIALIZE_MEMBER(ObSplitTabletInfo, split_info_, split_src_tablet_id_);

//record trace_id
ObDDLEventInfo::ObDDLEventInfo()
  : addr_(GCTX.self_addr()),
    sub_id_(0),
    event_ts_(ObTimeUtility::fast_current_time())
{
  init_sub_trace_id(sub_id_);
}

//modify trace_id
ObDDLEventInfo::ObDDLEventInfo(const int32_t sub_id)
  : addr_(GCTX.self_addr()),
    sub_id_(sub_id),
    event_ts_(ObTimeUtility::fast_current_time())
{
  init_sub_trace_id(sub_id_);
}

void ObDDLEventInfo::init_sub_trace_id(const int32_t sub_id)
{
  parent_trace_id_ = *ObCurTraceId::get_trace_id();
  if (sub_id == 0) {
    // ignore
  } else {
    ObCurTraceId::set_sub_id(sub_id);
  }
  trace_id_ = *ObCurTraceId::get_trace_id();
}

void ObDDLEventInfo::copy_event(const ObDDLEventInfo &other)
{
  addr_ = other.addr_;
  sub_id_ = other.sub_id_;
  parent_trace_id_ = other.parent_trace_id_;
  trace_id_ = other.trace_id_;
  event_ts_ = other.event_ts_;
}

void ObDDLEventInfo::set_inner_sql_id(const int64_t execution_id)
{
  parent_trace_id_ = *ObCurTraceId::get_trace_id();
  ObCurTraceId::set_inner_sql_id(execution_id);
  trace_id_ = *ObCurTraceId::get_trace_id();
}