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

#include "ob_dump_inner_table_schema.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/schema/ob_schema_utils.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/schema/ob_table_sql_service.h"

#define USING_LOG_PREFIX RS

namespace oceanbase
{
namespace share
{
using namespace schema;

///////////////////////////////////////////////////////////////////////////////////////////
// the following code should be the same with python code in generate_inner_table_schema.py
int ObDumpInnerTableSchemaUtils::upper(ObSqlString &str)
{
  int ret = OB_SUCCESS;
  ObSqlString tmp;
  ObString raw = str.string();
  for (int64_t i = 0; OB_SUCC(ret) && i < raw.length(); i++) {
    if (raw[i] >= 'a' && raw[i] <= 'z') {
      OZ (tmp.append_fmt("%c", raw[i] - 'a' + 'A'));
    } else {
      OZ (tmp.append_fmt("%c", raw[i]));
    }
  }
  OZ (str.assign(tmp));
  return ret;
}

int ObDumpInnerTableSchemaUtils::replace(ObSqlString &str, const char a, const char b) 
{
  int ret = OB_SUCCESS;
  ObSqlString tmp;
  ObString raw = str.string();
  for (int64_t i = 0; OB_SUCC(ret) && i < raw.length(); i++) {
    if (raw[i] == a) {
      OZ (tmp.append_fmt("%c", b));
    } else {
      OZ (tmp.append_fmt("%c", raw[i]));
    }
  }
  OZ (str.assign(tmp));
  return ret;
}

int ObDumpInnerTableSchemaUtils::lstrip(ObSqlString &str, const char c)
{
  int ret = OB_SUCCESS;
  int remove = true;
  ObSqlString tmp;
  ObString raw = str.string();
  for (int64_t i = 0; OB_SUCC(ret) && i < raw.length(); i++) {
    if (raw[i] == c && remove) {
    } else {
      remove = false;
      OZ (tmp.append_fmt("%c", raw[i]));
    }
  }
  OZ (str.assign(tmp));
  return ret;
}

int ObDumpInnerTableSchemaUtils::rstrip(ObSqlString &str, const char c)
{
  int ret = OB_SUCCESS;
  int remove = true;
  ObSqlString tmp;
  ObString raw = str.string();
  for (int64_t i = 0; OB_SUCC(ret) && i < raw.length() && remove; i++) {
    if (raw[raw.length() - 1 - i] == c) {
    } else {
      OZ (tmp.assign(str.ptr(), raw.length() - i));
      remove = false;
    }
  }
  OZ (str.assign(tmp));
  return ret;
}

int ObDumpInnerTableSchemaUtils::strip(ObSqlString &str, const char c)
{
  int ret = OB_SUCCESS;
  OZ (lstrip(str, c));
  OZ (rstrip(str, c));
  return ret;
}

int ObDumpInnerTableSchemaUtils::table_name2tid(const ObString &table_name, ObSqlString &tid)
{
  int ret = OB_SUCCESS;
  ObSqlString tmp;
  OZ (tmp.assign(table_name));
  OZ (replace(tmp, '$', '_'));
  OZ (upper(tmp));
  OZ (strip(tmp, '_'));
  OZ (tid.append("OB_"));
  OZ (tid.append(tmp.string()));
  OZ (tid.append("_TID"));
  return ret;
}

int ObDumpInnerTableSchemaUtils::table_name2tname(const ObString &table_name, ObSqlString &tname)
{
  int ret = OB_SUCCESS;
  ObSqlString tmp;
  OZ (tmp.assign(table_name));
  OZ (replace(tmp, '$', '_'));
  OZ (upper(tmp));
  OZ (strip(tmp, '_'));
  OZ (tname.append("OB_"));
  OZ (tname.append(tmp.string()));
  OZ (tname.append("_TNAME"));
  return ret;
}

int ObDumpInnerTableSchemaUtils::table_name2schema_version(const ObTableSchema &table, ObSqlString &schema_version)
{
  int ret = OB_SUCCESS;
  ObSqlString tmp;
  OZ (tmp.assign(table.get_table_name()));
  OZ (replace(tmp, '$', '_'));
  OZ (upper(tmp));
  OZ (strip(tmp, '_'));
  OZ (schema_version.append("OB_"));
  OZ (schema_version.append(tmp.string()));
  if ((is_ora_virtual_table(table.get_table_id()) || is_ora_sys_view_table(table.get_table_id()))) {
    OZ (schema_version.append("_ORACLE"));
  }
  OZ (schema_version.append("_SCHEMA_VERSION"));
  return ret;
}
///////////////////////////////////////////////////////////////////////////////////////////

int ObInnerTableSchemaDumper::get_schema_pointers_(
    ObIArray<schema::ObTableSchema *> &schema_ptrs)
{
  int ret = OB_SUCCESS;
  schema_ptrs.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < schemas_.count(); i++) {
    if (OB_FAIL(schema_ptrs.push_back(&schemas_.at(i)))) {
      LOG_WARN("failed to push_back", KR(ret), K(i));
    }
  }
  return ret;
}

int ObInnerTableSchemaDumper::init()
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> table_ids;
  if (OB_FAIL(ObSchemaUtils::construct_inner_table_schemas(OB_INVALID_TENANT_ID, table_ids,
          true/*include_index_and_lob_aux_schemas*/, allocator_, schemas_, true/*ignore_tenant_id*/))) {
    LOG_WARN("failed to construct inner table schemas", KR(ret));
  } else if (OB_FAIL(ObSchemaUtils::generate_hard_code_schema_version(schemas_))) {
    LOG_WARN("failed to generate hard code schema version", KR(ret));
  }
  return ret;
}

int ObInnerTableSchemaDumper::get_hard_code_schema(ObIArray<schema::ObTableSchema *> &schema)
{
  return get_schema_pointers_(schema);
}

int ObInnerTableSchemaDumper::get_inner_table_schema_info(ObIArray<ObLoadInnerTableSchemaInfo> &infos)
{
  int ret = OB_SUCCESS;
  ObArray<schema::ObTableSchema *> schema_ptrs;
  ObLoadInnerTableSchemaInfo info;
  infos.reset();
  int (ObInnerTableSchemaDumper::*func[])(const ObIArray<schema::ObTableSchema *> &, ObLoadInnerTableSchemaInfo &) = {
    &ObInnerTableSchemaDumper::get_all_core_table_info_,
    &ObInnerTableSchemaDumper::get_all_table_info_,
    &ObInnerTableSchemaDumper::get_all_table_history_info_,
    &ObInnerTableSchemaDumper::get_all_column_info_,
    &ObInnerTableSchemaDumper::get_all_column_history_info_,
    &ObInnerTableSchemaDumper::get_all_ddl_operation_info_,
  };
  if (OB_FAIL(get_schema_pointers_(schema_ptrs))) {
    LOG_WARN("failed to get schema pointers", KR(ret));
  } else {
    for (int64_t i = 0; i < ARRAYSIZEOF(func) && OB_SUCC(ret); i++) {
      if (OB_ISNULL(func[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pointer is null", KR(ret));
      } else if (OB_FAIL((this->*(func[i]))(schema_ptrs, info))) {
        LOG_WARN("failed to get info", KR(ret));
      } else if (OB_FAIL(infos.push_back(info))) {
        LOG_WARN("failed to push_back", KR(ret), K(info));
      }
    }
  }
  return ret;
}

int ObInnerTableSchemaDumper::get_table_info_(const ObIArray<schema::ObTableSchema *> &schema_ptrs,
    const ObString &table_name, const uint64_t table_id, const bool is_history, ObLoadInnerTableSchemaInfo &info)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  ObNotCoreTableLoadInfoConstructor constructor(table_name, table_id, allocator_);
  for (int64_t i = 0; i < schema_ptrs.count() && OB_SUCC(ret); i++) {
    ObTableSchema *table = nullptr;
    dml.reset();
    if (OB_ISNULL(table = schema_ptrs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pointer is null", KR(ret), KP(table), K(i));
    } else if (!is_history && is_core_table(table->get_table_id())) {
    } else if (OB_FAIL(ObTableSqlService::gen_table_dml_without_check(OB_INVALID_TENANT_ID, *table,
            false, DATA_CURRENT_VERSION, dml, true/*is_test*/))) {
      LOG_WARN("failed to gen_table_dml", KR(ret));
    } else if (is_history && OB_FAIL(dml.add_column("is_deleted", "0"))) {
      LOG_WARN("failed to add is_deleted", KR(ret));
    } else if (OB_FAIL(constructor.add_lines(table->get_table_id(), dml))) {
      LOG_WARN("failed to add lines", KR(ret), K(table));
    }
  }
  if (FAILEDx(constructor.get_load_info(info))) {
    LOG_WARN("failed to get load info", KR(ret), K(info));
  }
  return ret;
}

int ObInnerTableSchemaDumper::get_all_table_info_(const ObIArray<schema::ObTableSchema *> &schema_ptrs,
    ObLoadInnerTableSchemaInfo &info)
{
  return get_table_info_(schema_ptrs, OB_ALL_TABLE_TNAME, OB_ALL_TABLE_TID, false/*is_history*/, info);
}

int ObInnerTableSchemaDumper::get_all_table_history_info_(const ObIArray<schema::ObTableSchema *> &schema_ptrs,
    ObLoadInnerTableSchemaInfo &info)
{
  return get_table_info_(schema_ptrs, OB_ALL_TABLE_HISTORY_TNAME, OB_ALL_TABLE_HISTORY_TID, true/*is_history*/, info);
}

int ObInnerTableSchemaDumper::get_column_info_(const ObIArray<schema::ObTableSchema *> &schema_ptrs,
    const ObString &table_name, const uint64_t table_id, const bool is_history, ObLoadInnerTableSchemaInfo &info)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  ObNotCoreTableLoadInfoConstructor constructor(table_name, table_id, allocator_);
  for (int64_t i = 0; i < schema_ptrs.count() && OB_SUCC(ret); i++) {
    ObTableSchema *table = nullptr;
    if (OB_ISNULL(table = schema_ptrs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pointer is null", KR(ret), KP(table), K(i));
    } else if (!is_history && is_core_table(table->get_table_id())) {
    } else {
      for (ObTableSchema::const_column_iterator iter = table->column_begin();
          OB_SUCC(ret) && iter != table->column_end(); ++iter) {
        dml.reset();
        if (OB_ISNULL(iter) || OB_ISNULL(*iter)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("pointer is null", KR(ret), KP(iter));
        } else if (OB_FAIL(ObTableSqlService::gen_column_dml_without_check(OB_INVALID_TENANT_ID, **iter,
                DATA_CURRENT_VERSION, lib::Worker::CompatMode::MYSQL, dml, true/*is_test*/))) {
          LOG_WARN("failed to gen_table_dml", KR(ret));
        } else if (is_history && OB_FAIL(dml.add_column("is_deleted", "0"))) {
          LOG_WARN("failed to add is_deleted", KR(ret));
        } else if (OB_FAIL(constructor.add_lines(table->get_table_id(), dml))) {
          LOG_WARN("failed to add lines", KR(ret), KPC(table));
        }
      }
    }
  }
  if (FAILEDx(constructor.get_load_info(info))) {
    LOG_WARN("failed to get load info", KR(ret), K(info));
  }
  return ret;
}

int ObInnerTableSchemaDumper::get_all_column_info_(const ObIArray<schema::ObTableSchema *> &schema_ptrs,
    ObLoadInnerTableSchemaInfo &info)
{
  return get_column_info_(schema_ptrs, OB_ALL_COLUMN_TNAME, OB_ALL_COLUMN_TID, false/*is_history*/, info);
}

int ObInnerTableSchemaDumper::get_all_column_history_info_(const ObIArray<schema::ObTableSchema *> &schema_ptrs,
    ObLoadInnerTableSchemaInfo &info)
{
  return get_column_info_(schema_ptrs, OB_ALL_COLUMN_HISTORY_TNAME, OB_ALL_COLUMN_HISTORY_TID, true/*is_history*/, info);
}

int ObInnerTableSchemaDumper::get_all_ddl_operation_info_(const ObIArray<schema::ObTableSchema *> &schema_ptrs,
    ObLoadInnerTableSchemaInfo &info)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  ObNotCoreTableLoadInfoConstructor constructor(OB_ALL_DDL_OPERATION_TNAME, OB_ALL_DDL_OPERATION_TID, allocator_);
  for (int64_t i = 0; i < schema_ptrs.count() && OB_SUCC(ret); i++) {
    ObTableSchema *table = nullptr;
    dml.reset();
    if (OB_ISNULL(table = schema_ptrs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pointer is null", KR(ret), KP(table), K(i));
    } else {
      ObSchemaOperationType op_type;
      if (table->is_index_table()) {
        op_type = table->is_global_index_table() ? OB_DDL_CREATE_GLOBAL_INDEX : OB_DDL_CREATE_INDEX;
      } else if (table->is_view_table()) {
        op_type = OB_DDL_CREATE_VIEW;
      } else {
        op_type = OB_DDL_CREATE_TABLE;
      }
      // modify here if change __all_ddl_operation schema
      const int64_t line_begin = __LINE__;
      OZ (dml.add_gmt_create());
      OZ (dml.add_gmt_modified());
      // OZ (dml.add_column("schema_version", table->get_schema_version()));
      OZ (dml.add_column("tenant_id", OB_INVALID_TENANT_ID));
      OZ (dml.add_column("user_id", 0));
      OZ (dml.add_column("database_id", table->get_database_id()));
      OZ (dml.add_column("database_name", ""));
      OZ (dml.add_column("tablegroup_id", table->get_tablegroup_id()));
      OZ (dml.add_column("table_id", table->get_table_id()));
      OZ (dml.add_column("table_name", ""));
      OZ (dml.add_column("operation_type", op_type));
      OZ (dml.add_column("ddl_stmt_str", ""));
      OZ (dml.add_function_call("exec_tenant_id", "effective_tenant_id()"));
      const int64_t line_end = __LINE__;
      if (OB_SUCC(ret) && table->get_table_id() == OB_ALL_DDL_OPERATION_TID && 
          table->get_column_count() != line_end - line_begin - 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("__all_ddl_operation table schema is changed", KR(ret),
            "count", line_end - line_begin - 1, K(table->get_column_count()));
      }
    }
    if (FAILEDx(constructor.add_lines(table->get_table_id(), dml))) {
      LOG_WARN("failed to add lines", KR(ret), K(table));
    }
  }
  if (FAILEDx(constructor.get_load_info(info))) {
    LOG_WARN("failed to get load info", KR(ret), K(info));
  }
  return ret;
}

int ObInnerTableSchemaDumper::get_all_core_table_info_(const ObIArray<schema::ObTableSchema *> &schema_ptrs,
    ObLoadInnerTableSchemaInfo &info)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  ObCoreTableLoadInfoConstructor table_constructor(OB_ALL_TABLE_TNAME, allocator_);
  ObCoreTableLoadInfoConstructor column_constructor(OB_ALL_COLUMN_TNAME, allocator_);
  ObMergeLoadInfoConstructor constructor(OB_ALL_CORE_TABLE_TNAME, OB_ALL_CORE_TABLE_TID, allocator_);
  for (int64_t i = 0; i < schema_ptrs.count() && OB_SUCC(ret); i++) {
    ObTableSchema *table = nullptr;
    dml.reset();
    if (OB_ISNULL(table = schema_ptrs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pointer is null", KR(ret), KP(table), K(i));
    } else if (!is_core_table(table->get_table_id())) {
    } else if (OB_FAIL(ObTableSqlService::gen_table_dml_without_check(OB_INVALID_TENANT_ID, *table,
            false, DATA_CURRENT_VERSION, dml, true/*is_test*/))) {
      LOG_WARN("failed to gen_table_dml", KR(ret));
    } else if (OB_FAIL(dml.add_column("schema_version", table->get_schema_version()))) {
      LOG_WARN("failed to add schema_version", KR(ret));
    } else if (OB_FAIL(table_constructor.add_lines(table->get_table_id(), dml))) {
      LOG_WARN("failed to add table", KR(ret), KPC(table));
    } else {
      for (ObTableSchema::const_column_iterator iter = table->column_begin();
          OB_SUCC(ret) && iter != table->column_end(); ++iter) {
        dml.reset();
        if (OB_ISNULL(iter) || OB_ISNULL(*iter)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("pointer is null", KR(ret), KP(iter));
        } else if (OB_FAIL(ObTableSqlService::gen_column_dml_without_check(OB_INVALID_TENANT_ID, **iter,
                DATA_CURRENT_VERSION, lib::Worker::CompatMode::MYSQL, dml, true/*is_test*/))) {
          LOG_WARN("failed to gen_table_dml", KR(ret));
        } else if (OB_FAIL(dml.add_column("schema_version", table->get_schema_version()))) {
          LOG_WARN("failed to add schema_version", KR(ret));
        } else if (OB_FAIL(column_constructor.add_lines(table->get_table_id(), dml))) {
          LOG_WARN("failed to add column", KR(ret), KPC(table));
        }
      }
    }
  }
  if (FAILEDx(constructor.add_constructor(table_constructor))) {
    LOG_WARN("faled to add constructor", KR(ret));
  } else if (OB_FAIL(constructor.add_constructor(column_constructor))) {
    LOG_WARN("faled to add constructor", KR(ret));
  } else if (OB_FAIL(constructor.get_load_info(info))) {
    LOG_WARN("failed to get load info", KR(ret));
  }
  return ret;
}

int ObLoadInnerTableSchemaInfoConstructor::add_line(const ObString &line,
    const uint64_t table_id, const ObString header)
{
  int ret = OB_SUCCESS;
  ObString tmp_line;
  uint64_t checksum = 0;
  if (header_.empty()) {
    if (OB_FAIL(ob_write_string(allocator_, header, header_, true/*c_style*/))) {
      LOG_WARN("failed to write header", KR(ret), K(header));
    }
  } else {
    if (header != header_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("header is not equal", KR(ret), K(header), K(header_));
    }
  }
  if (FAILEDx(ob_write_string(allocator_, line, tmp_line, true/*c_style*/))) {
    LOG_WARN("failed to write string", KR(ret), K(line));
  } else if (OB_FAIL(rows_.push_back(tmp_line))) {
    LOG_WARN("failed to push_back line", KR(ret), K(tmp_line));
  } else if (OB_FAIL(table_ids_.push_back(table_id))) {
    LOG_WARN("failed to push_back line", KR(ret), K(table_id));
  } else if (FALSE_IT(checksum = ObLoadInnerTableSchemaInfo::get_checksum(tmp_line.ptr(), table_id))) {
  } else if (OB_FAIL(checksums_.push_back(checksum))) {
    LOG_WARN("failed to push_back checksum", KR(ret), K(tmp_line), K(table_id));
  }
  return ret;
}

bool ObLoadInnerTableSchemaInfoConstructor::is_valid() const
{
  bool valid = true;
  if (!is_system_table(table_id_)) {
    valid = false;
  } else if (rows_.count() != table_ids_.count()) {
    valid = false;
  } else if (checksums_.count() != rows_.count()) {
    valid = false;
  }
  return valid;
}

int ObLoadInnerTableSchemaInfoConstructor::get_load_info(ObLoadInnerTableSchemaInfo &info)
{
  int ret = OB_SUCCESS;
  uint64_t *table_id_buf = nullptr;
  const char **row_buf = nullptr;
  uint64_t *checksum_buf = nullptr;
  ObString table_name;
  ObString header;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("constructor is invalid", KR(ret), K(*this));
  } else if (OB_FAIL(ob_write_string(allocator_, table_name_, table_name, true/*c_style*/))) {
    LOG_WARN("failed to write string", KR(ret), K_(table_name));
  } else if (OB_FAIL(ob_write_string(allocator_, header_, header, true/*c_style*/))) {
    LOG_WARN("failed to write string", KR(ret), K_(header));
  } else if (OB_ISNULL(table_id_buf = static_cast<uint64_t *>(allocator_.alloc(sizeof(table_id_buf[0]) * table_ids_.count())))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc table_ids_ memory", KR(ret), K(table_ids_.count()));
  } else if (OB_ISNULL(row_buf = static_cast<const char **>(allocator_.alloc(sizeof(row_buf[0]) * rows_.count())))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc rows_ memory", KR(ret), K(rows_.count()));
  } else if (OB_ISNULL(checksum_buf = static_cast<uint64_t *>(allocator_.alloc(sizeof(checksum_buf[0]) * rows_.count())))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc rows_ memory", KR(ret), K(rows_.count()));
  } else {
    for (int64_t i = 0; i < table_ids_.count(); i++) {
      table_id_buf[i] = table_ids_[i];
    }
    for (int64_t i = 0; i < rows_.count(); i++) {
      row_buf[i] = rows_[i].ptr();
    }
    for (int64_t i = 0; i < checksums_.count(); i++) {
      checksum_buf[i] = checksums_[i];
    }
    info = ObLoadInnerTableSchemaInfo(
        table_id_,
        table_name.ptr(),
        header.ptr(),
        table_id_buf,
        row_buf,
        checksum_buf,
        rows_.count()
    );
  }
  return ret;
}

int ObNotCoreTableLoadInfoConstructor::add_lines(const uint64_t table_id, ObDMLSqlSplicer &splicer)
{
  int ret = OB_SUCCESS;
  ObSqlString header;
  ObSqlString line;
  if (OB_FAIL(splicer.splice_column_names(header))) {
    LOG_WARN("failed to get header", KR(ret));
  } else if (OB_FAIL(splicer.splice_values(line))) {
    LOG_WARN("failed to get line", KR(ret));
  } else if (OB_FAIL(add_line(line.string(), table_id, header.string()))) {
    LOG_WARN("failed to add line", KR(ret), K(line), K(table_id), K(header));
  }
  return ret;
}

int ObCoreTableLoadInfoConstructor::add_lines(const uint64_t table_id, ObDMLSqlSplicer &splicer)
{
  int ret = OB_SUCCESS;
  ObArray<ObCoreTableProxy::UpdateCell> cells;
  const char *header = "table_name,row_id,column_name,column_value";
  if (OB_FAIL(splicer.splice_core_cells(store_cell_, cells))) {
    LOG_WARN("failed to store core cells", KR(ret));
  } else {
    row_id_++;
    ObSqlString line;
    for (int64_t i = 0; i < cells.count() && OB_SUCC(ret); i++) {
      ObCoreTableProxy::Cell cell = cells.at(i).cell_;
      line.reset();
      if (OB_ISNULL(cell.value_.ptr())) {
        if (OB_FAIL(line.assign_fmt("'%.*s', %ld, '%.*s', NULL", core_table_name_.length(),
                core_table_name_.ptr(), row_id_, cell.name_.length(), cell.name_.ptr()))) {
          LOG_WARN("failed to assign line", KR(ret), K(cell));
        }
      } else if (cell.is_hex_value_) {
        if (OB_FAIL(line.append_fmt("'%.*s', %ld, '%.*s', %.*s", core_table_name_.length(),
                core_table_name_.ptr(), row_id_, cell.name_.length(), cell.name_.ptr(),
                cell.value_.length(), cell.value_.ptr()))) {
          LOG_WARN("failed to assign line", KR(ret), K(cell));
        }
      } else {
        if (OB_FAIL(line.append_fmt("'%.*s', %ld, '%.*s', '%.*s'", core_table_name_.length(),
                core_table_name_.ptr(), row_id_, cell.name_.length(), cell.name_.ptr(),
                cell.value_.length(), cell.value_.ptr()))) {
          LOG_WARN("failed to assign line", KR(ret), K(cell));
        }
      }
      if (FAILEDx(add_line(line.string(), table_id, header))) {
        LOG_WARN("failed to add line", KR(ret), K(line), K(table_id), K(header));
      }
    }
  }
  return ret;
}

int ObCoreTableLoadInfoConstructor::DumpCoreTableStoreCell::store_string(
    const common::ObString &src, common::ObString &dest)
{
  return ob_write_string(allocator_, src, dest, true/*c_style*/);
}

int ObCoreTableLoadInfoConstructor::DumpCoreTableStoreCell::store_cell(
    const ObCoreTableCell &src, ObCoreTableCell &dest)
{
  int ret = OB_SUCCESS;
  if (!src.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(src));
  } else if (OB_FAIL(store_string(src.name_, dest.name_))) {
    LOG_WARN("store cell name failed", K(ret), K(src));
  } else if (OB_FAIL(store_string(src.value_, dest.value_))) {
    LOG_WARN("store cell value failed", K(ret), K(src));
  } else {
    dest.is_hex_value_ = src.is_hex_value_;
  }
  return ret;
}

int ObMergeLoadInfoConstructor::add_lines(const uint64_t table_id, ObDMLSqlSplicer &splicer)
{
  return OB_NOT_SUPPORTED;
}

int ObMergeLoadInfoConstructor::add_constructor(ObLoadInnerTableSchemaInfoConstructor &constructor)
{
  int ret = OB_SUCCESS;
  if (constructor.get_table_id() != table_id_ || constructor.get_table_name() != table_name_ 
      || &constructor.get_allocator() != &allocator_ || (!header_.empty() && constructor.get_header() != header_) 
      || !is_valid() || !constructor.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("two construct is not same, cannot merge", KR(ret), KPC(this), K(constructor));
  } else if (OB_FAIL(rows_.push_back(constructor.get_rows()))) {
    LOG_WARN("failed to push_back rows", KR(ret));
  } else if (OB_FAIL(table_ids_.push_back(constructor.get_table_ids()))) {
    LOG_WARN("failed to push_back rows", KR(ret));
  } else if (OB_FAIL(checksums_.push_back(constructor.get_checksums()))) {
    LOG_WARN("failed to push_back checksums", KR(ret));
  } else {
    header_ = constructor.get_header();
  }
  return ret;
}

int ObInnerTableSchemaPrinter::get_file_header_(ObSqlString &code)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(code.append_fmt(R"__(/**
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

#define USING_LOG_PREFIX SHARE_SCHEMA
#include "ob_load_inner_table_schema.h"
#include "share/schema/ob_schema_struct.h"

namespace oceanbase
{
namespace share
{
)__"))) {
  LOG_WARN("failed to write file header", KR(ret));
  }
  return ret;
}

int ObInnerTableSchemaPrinter::get_file_tail_(ObSqlString &code)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(code.append_fmt(R"__(} // end namespace share
} // end namespace oceanbase
)__"))) {
  LOG_WARN("failed to write file tail", KR(ret));
  }
  return ret;
}

int ObInnerTableSchemaPrinter::get_schema_version_enum_code_(ObSqlString &code)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(code.append_fmt(R"__(enum class ObHardCodeInnerTableSchemaVersion : int64_t { //FARM COMPAT WHITELIST
  HARD_CODE_SCHEMA_VERSION_BEGIN = %ld,
)__", HARD_CODE_SCHEMA_VERSION_BEGIN))) {
    LOG_WARN("failed to append schema version header", KR(ret));
  }
  if (OB_SUCC(ret) && !empty_file_) {
    lib::ob_sort(schema_ptrs_.get_data(), schema_ptrs_.get_data() + schema_ptrs_.count(), TableSchemaCmpBySchemaVersion());
    for (int64_t i = 0; i < schema_ptrs_.count() && OB_SUCC(ret); i++) {
      ObTableSchema *table = nullptr;
      ObSqlString schema_version_name;
      if (OB_ISNULL(table = schema_ptrs_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pointer is null", KR(ret), K(i), KP(table));
      } else if (OB_FAIL(ObDumpInnerTableSchemaUtils::table_name2schema_version(*table, schema_version_name))) {
        LOG_WARN("failed to get schema version name", KR(ret), K(*table));
      } else if (OB_FAIL(code.append_fmt("  %s,\n", schema_version_name.ptr()))) {
        LOG_WARN("failed to append schema version", KR(ret), K(schema_version_name));
      }
    }
  }
  if (FAILEDx(code.append_fmt("  MAX_HARD_CODE_SCHEMA_VERSION,\n};\n"))) {
    LOG_WARN("failed to append schema version tail", KR(ret));
  } else if (OB_FAIL(code.append_fmt(R"__(int64_t get_hard_code_schema_count()
{
  return static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::MAX_HARD_CODE_SCHEMA_VERSION) - 
    static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::HARD_CODE_SCHEMA_VERSION_BEGIN) - 1;
}
)__"))) {
    LOG_WARN("failed to append get_hard_code_schema_count code", KR(ret));
  }
  return ret;
}

int ObInnerTableSchemaPrinter::get_load_schema_info_code_(ObSqlString &code)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(code.append(R"__(
uint64_t ObLoadInnerTableSchemaInfo::get_checksum(const char *row, const uint64_t table_id)
{
  return ob_crc64(row, strlen(row)) ^ table_id;
}
int ObLoadInnerTableSchemaInfo::get_checksum(const int64_t idx, uint64_t &checksum) const
{
  int ret = OB_SUCCESS;
  if (idx < 0 || idx >= row_count_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("index out of range", KR(ret), K(idx), K(row_count_));
  } else {
    checksum = row_checksum_[idx];
  }
  return ret;
}
int ObLoadInnerTableSchemaInfo::check_row_valid_(const int64_t idx) const
{
  int ret = OB_SUCCESS;
  uint64_t checksum = 0;
  if (idx < 0 || idx >= row_count_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("idx is out of range", KR(ret), K(idx), K(row_count_));
  } else if (FALSE_IT(checksum = get_checksum(inner_table_rows_[idx], inner_table_table_ids_[idx]))) {
  } else if (checksum != row_checksum_[idx]) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("checksum is not equal, may be observer binary is broken", KR(ret), K(idx), K(checksum),
        K(row_checksum_[idx]), K(inner_table_table_ids_[idx]), K(inner_table_rows_[idx]));
  }
  return ret;
}
int ObLoadInnerTableSchemaInfo::get_row(const int64_t idx, const char *&row, uint64_t &table_id) const
{
  int ret = OB_SUCCESS;
  if (idx < 0 || idx >= row_count_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("idx is out of range", KR(ret), K(idx), K(row_count_));
  } else if (OB_FAIL(check_row_valid_(idx))) {
    LOG_WARN("failed to check row valid", KR(ret), K(idx));
  } else {
    row = inner_table_rows_[idx];
    table_id = inner_table_table_ids_[idx];
  }
  return ret;
}
)__"))) {
    LOG_WARN("failed to append load schema info code", KR(ret));
  }
  return ret;
}

int ObInnerTableSchemaPrinter::get_schema_version_mapping_code_(ObSqlString &code)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(code.append_fmt(R"__(int get_hard_code_schema_version_mapping(const uint64_t table_id, int64_t &schema_version)
{
  int ret = OB_SUCCESS;)__"))) {
    LOG_WARN("failed to append schema version mapping header", KR(ret));
  }
  if (OB_SUCC(ret) && !empty_file_) {
    if (OB_FAIL(code.append_fmt(R"__(
  switch (table_id) {
#define INNER_TABLE_HARD_CODE_SCHEMA_MAPPING_SWITCH
#include "ob_inner_table_schema_misc.ipp"
#undef INNER_TABLE_HARD_CODE_SCHEMA_MAPPING_SWITCH
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unknown table_id", KR(ret), K(table_id));
    }
  }
  schema_version *= schema::ObSchemaVersionGenerator::SCHEMA_VERSION_INC_STEP;
)__"))) {
    LOG_WARN("failed to append schema version mapping body", KR(ret));
  }
  }
  if (FAILEDx(code.append(R"__(  return ret;
}
)__"))) {
    LOG_WARN("failed to append schema version mapping tail", KR(ret));
  }
  return ret;
}

int ObInnerTableSchemaPrinter::get_core_schema_version_code_(ObSqlString &code)
{
  int ret = OB_SUCCESS;
  int64_t core_schema_version = 0;
  if (!empty_file_) {
    for (int64_t i = 0; i < schema_ptrs_.count(); i++) {
      if (OB_ISNULL(schema_ptrs_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pointer is null", KR(ret), KP(schema_ptrs_[i]));
      } else if (is_core_table(schema_ptrs_[i]->get_table_id()) && schema_ptrs_[i]->get_schema_version() > core_schema_version) {
        core_schema_version = schema_ptrs_[i]->get_schema_version();
      }
    }
  }
  if (FAILEDx(code.append_fmt("const int64_t INNER_TABLE_CORE_SCHEMA_VERSION = %ld;\n", core_schema_version))) {
    LOG_WARN("failed to append core schema version", KR(ret), K(core_schema_version));
  }
  return ret;
}

#define ROWS_FMT "INNER_TABLE_%.*s_ROWS"
#define TABLE_IDS_FMT "INNER_TABLE_%.*s_TABLE_IDS"
#define CHECKSUMS_FMT "INNER_TABLE_%.*s_CHECKSUMS"
#define TABLE_HEADER_FMT "INNER_TABLE_%.*s_HEADER"

int ObInnerTableSchemaPrinter::get_rows_code_(const ObLoadInnerTableSchemaInfo &info,
    const ObString &table_name_upper, ObSqlString &code)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(code.append_fmt("const char * " ROWS_FMT "[] = {\n",
          table_name_upper.length(), table_name_upper.ptr()))) {
    LOG_WARN("failed to get sql header", KR(ret), K(table_name_upper));
  } else if (!empty_file_) {
    const char *row = nullptr;
    uint64_t table_id = 0;
    for (int64_t i = 0; i < info.get_row_count() && OB_SUCC(ret); i++) {
      if (OB_FAIL(info.get_row(i, row, table_id))) {
        LOG_WARN("failed to get row", KR(ret), K(i));
      } else if (OB_FAIL(code.append_fmt("  R\"__(%s)__\",\n", row))) {
        LOG_WARN("failed to append row", KR(ret), K(i));
      }
    }
  }
  if (FAILEDx(code.append("};\n"))) {
    LOG_WARN("failed to append load info tail", KR(ret), K(info));
  }
  return ret;
}

int ObInnerTableSchemaPrinter::get_table_ids_code_(const ObLoadInnerTableSchemaInfo &info,
    const ObString &table_name_upper, ObSqlString &code)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(code.append_fmt("const uint64_t " TABLE_IDS_FMT "[] = {\n",
          table_name_upper.length(), table_name_upper.ptr()))) {
    LOG_WARN("failed to get sql header", KR(ret), K(table_name_upper));
  } else if (!empty_file_) {
    const char *row = nullptr;
    uint64_t table_id = 0;
    for (int64_t i = 0; i < info.get_row_count() && OB_SUCC(ret); i++) {
      if (OB_FAIL(info.get_row(i, row, table_id))) {
        LOG_WARN("failed to get row", KR(ret), K(i));
      } else if (OB_FAIL(code.append_fmt("  %ld,\n", table_id))) {
        LOG_WARN("failed to append row", KR(ret), K(i));
      }
    }
  }
  if (FAILEDx(code.append("};\n"))) {
    LOG_WARN("failed to append load info tail", KR(ret), K(info));
  }
  return ret;
}

int ObInnerTableSchemaPrinter::get_checksum_code_(const ObLoadInnerTableSchemaInfo &info,
    const ObString &table_name_upper, ObSqlString &code)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(code.append_fmt("const uint64_t " CHECKSUMS_FMT "[] = {\n",
          table_name_upper.length(), table_name_upper.ptr()))) {
    LOG_WARN("failed to get sql header", KR(ret), K(table_name_upper));
  } else if (!empty_file_) {
    for (int64_t i = 0; i < info.get_row_count() && OB_SUCC(ret); i++) {
      uint64_t checksum = 0;
      if (OB_FAIL(info.get_checksum(i, checksum))) {
        LOG_WARN("failed to get checksum", KR(ret), K(i));
      } else if (OB_FAIL(code.append_fmt("  0x%lxull,\n", checksum))) {
        LOG_WARN("failed to append row", KR(ret), K(i), K(checksum));
      }
    }
  }
  if (FAILEDx(code.append("};\n"))) {
    LOG_WARN("failed to append load info tail", KR(ret), K(info));
  }
  return ret;
}

int ObInnerTableSchemaPrinter::get_table_header_code_(const ObLoadInnerTableSchemaInfo &info,
    const ObString &table_name_upper, ObSqlString &code)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(code.append_fmt("const char * " TABLE_HEADER_FMT " = \"%s\";\n", table_name_upper.length(),
          table_name_upper.ptr(), info.get_inner_table_column_names()))) {
    LOG_WARN("failed to get table header code", KR(ret), K(info), K(table_name_upper));
  }
  return ret;
}

int ObInnerTableSchemaPrinter::get_load_info_code_(const ObLoadInnerTableSchemaInfo &info,
    const ObString &table_name_upper, ObSqlString &code)
{
  int ret = OB_SUCCESS;
  ObSqlString tid;
  ObSqlString tname;
  if (OB_FAIL(ObDumpInnerTableSchemaUtils::table_name2tname(info.get_inner_table_name(), tname))) {
    LOG_WARN("failed to get tname", KR(ret), K(info));
  } else if (OB_FAIL(ObDumpInnerTableSchemaUtils::table_name2tid(info.get_inner_table_name(), tid))) {
    LOG_WARN("failed to get tname", KR(ret), K(info));
  } else if (OB_FAIL(code.append_fmt("const ObLoadInnerTableSchemaInfo %.*s_LOAD_INFO(\n",
          table_name_upper.length(), table_name_upper.ptr()))) {
    LOG_WARN("failed to append load info header", KR(ret), K(table_name_upper));
  } else if (OB_FAIL(code.append_fmt("  %s,\n", tid.ptr()))) {
    LOG_WARN("failed to append tid", KR(ret), K(tid));
  } else if (OB_FAIL(code.append_fmt("  %s,\n", tname.ptr()))) {
    LOG_WARN("failed to append tname", KR(ret), K(tname));
  } else if (OB_FAIL(code.append_fmt("  " TABLE_HEADER_FMT ",\n", table_name_upper.length(), table_name_upper.ptr()))) {
    LOG_WARN("failed to append table header", KR(ret), K(table_name_upper));
  } else if (OB_FAIL(code.append_fmt("  " TABLE_IDS_FMT ",\n", table_name_upper.length(), table_name_upper.ptr()))) {
    LOG_WARN("failed to append table ids", KR(ret), K(table_name_upper));
  } else if (OB_FAIL(code.append_fmt("  " ROWS_FMT ",\n", table_name_upper.length(), table_name_upper.ptr()))) {
    LOG_WARN("failed to append rows", KR(ret), K(table_name_upper));
  } else if (OB_FAIL(code.append_fmt("  " CHECKSUMS_FMT ",\n", table_name_upper.length(), table_name_upper.ptr()))) {
    LOG_WARN("failed to append rows", KR(ret), K(table_name_upper));
  } else if (OB_FAIL(code.append_fmt("  ARRAYSIZEOF(" ROWS_FMT ")\n", table_name_upper.length(), table_name_upper.ptr()))) {
    LOG_WARN("failed to append row count", KR(ret), K(table_name_upper));
  } else if (OB_FAIL(code.append_fmt(");\n"))) {
    LOG_WARN("failed to append load info tail", KR(ret));
  }
  return ret;
}

int ObInnerTableSchemaPrinter::get_load_info_code_(const ObLoadInnerTableSchemaInfo &info, ObSqlString &code)
{
  int ret = OB_SUCCESS;
  ObSqlString table_name_upper;
  if (OB_FAIL(table_name_upper.assign(info.get_inner_table_name()))) {
    LOG_WARN("failed to assign table name", KR(ret), K(info));
  } else if (OB_FAIL(ObDumpInnerTableSchemaUtils::lstrip(table_name_upper, '_'))) {
    LOG_WARN("failed to lstrip", KR(ret), K(table_name_upper));
  } else if (OB_FAIL(ObDumpInnerTableSchemaUtils::upper(table_name_upper))) {
    LOG_WARN("failed to upper", KR(ret), K(table_name_upper));
  } else if (OB_FAIL(get_rows_code_(info, table_name_upper.string(), code))) {
    LOG_WARN("failed to get rows", KR(ret), K(info), K(table_name_upper));
  } else if (OB_FAIL(get_table_ids_code_(info, table_name_upper.string(), code))) {
    LOG_WARN("failed to get table_ids", KR(ret), K(info), K(table_name_upper));
  } else if (OB_FAIL(get_checksum_code_(info, table_name_upper.string(), code))) {
    LOG_WARN("failed to get checksums", KR(ret), K(info), K(table_name_upper));
  } else if (OB_FAIL(get_table_header_code_(info, table_name_upper.string(), code))) {
    LOG_WARN("failed to get table header", KR(ret), K(info), K(table_name_upper));
  } else if (OB_FAIL(code.append_fmt("static_assert(ARRAYSIZEOF(" ROWS_FMT ") == ARRAYSIZEOF(" TABLE_IDS_FMT "),"
          " \"%s header and table_ids length is not equal\");\n", table_name_upper.string().length(),
          table_name_upper.ptr(), table_name_upper.string().length(), table_name_upper.ptr(),
          table_name_upper.ptr()))) {
    LOG_WARN("failed to append static_assert", KR(ret), K(table_name_upper));
  } else if (OB_FAIL(code.append_fmt("static_assert(ARRAYSIZEOF(" ROWS_FMT ") == ARRAYSIZEOF(" CHECKSUMS_FMT "),"
          " \"%s header and table_ids length is not equal\");\n", table_name_upper.string().length(),
          table_name_upper.ptr(), table_name_upper.string().length(), table_name_upper.ptr(),
          table_name_upper.ptr()))) {
    LOG_WARN("failed to append static_assert", KR(ret), K(table_name_upper));
  } else if (OB_FAIL(get_load_info_code_(info, table_name_upper.string(), code))) {
    LOG_WARN("failed to get load info code", KR(ret), K(info), K(table_name_upper));
  }
  return ret;
}

int ObInnerTableSchemaPrinter::get_all_code(const ObIArray<ObLoadInnerTableSchemaInfo> &infos, ObSqlString &code)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_file_header_(code))) {
    LOG_WARN("failed to get file header", KR(ret));
  } else if (OB_FAIL(get_schema_version_enum_code_(code))) {
    LOG_WARN("failed to get schema version enum code", KR(ret));
  } else if (OB_FAIL(get_load_schema_info_code_(code))) {
    LOG_WARN("failed to get load schema info code", KR(ret));
  } else if (OB_FAIL(get_schema_version_mapping_code_(code))) {
    LOG_WARN("failed to get schema version mapping code", KR(ret));
  } else if (OB_FAIL(get_core_schema_version_code_(code))) {
    LOG_WARN("failed to get core schema version code", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < infos.count(); i++) {
      if (OB_FAIL(get_load_info_code_(infos.at(i), code))) {
        LOG_WARN("failed to get load info code", KR(ret), K(infos.at(i)));
      }
    }
  }
  if (FAILEDx(get_file_tail_(code))) {
    LOG_WARN("failed to get file tail", KR(ret));
  }
  return ret;
}

int ObInnerTableSchemaPrinter::init(const bool empty_file, const ObIArray<schema::ObTableSchema *> &schema_ptrs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(schema_ptrs_.assign(schema_ptrs))) {
    LOG_WARN("failed to assign schema_ptrs_", KR(ret));
  } else {
    empty_file_ = empty_file;
  }
  return ret;
}

#undef ROWS_FMT
#undef TABLE_IDS_FMT
#undef TABLE_HEADER_FMT

}
}
