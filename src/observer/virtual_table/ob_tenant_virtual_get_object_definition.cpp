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

#define USING_LOG_PREFIX SERVER
#include "observer/virtual_table/ob_tenant_virtual_get_object_definition.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_schema_printer.h"
#include "sql/session/ob_sql_session_info.h"

#include "share/schema/ob_table_schema.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
namespace oceanbase
{
namespace observer
{

ObGetObjectDefinition::ObGetObjectDefinition()
    : ObVirtualTableScannerIterator()
{
}

ObGetObjectDefinition::~ObGetObjectDefinition()
{
}

const char *ObGetObjectDefinition::ObjectTypeName[T_GET_DDL_MAX] = 
{
  "TABLE",
  "PROCEDURE",
  "PACKAGE",
  "CONSTRAINT",
  "REF_CONSTRAINT",
  "TABLESPACE",
  "SEQUENCE",
  "TRIGGER",
  "USER",
  "SYNONYM",
  "TYPE",
  "TYPE_SPEC",
  "TYPE_BODY"
};

void ObGetObjectDefinition::reset()
{
  ObVirtualTableScannerIterator::reset();
}

int ObGetObjectDefinition::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(schema_guard_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "schema guard is NULL", K(ret), K(schema_guard_));
  } else if (!start_to_read_) {
    GetDDLObjectType object_type = T_GET_DDL_MAX;
    ObString object_name;
    ObString ob_schema;
    ObString version;
    ObString model;
    ObString transform;
    ObString ddl_str;
    if (OB_UNLIKELY(NULL == session_ || NULL == allocator_
                    || NULL == cur_row_.cells_)) {
      ret = OB_NOT_INIT;
      SERVER_LOG(WARN, "data member isn't init", K(ret), K(session_),
                  K(allocator_), K(cur_row_.cells_));
    } else if (OB_UNLIKELY(cur_row_.count_ < output_column_ids_.count())) {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "cells count is less than output column count",
                    K(ret), K(cur_row_.count_), K(output_column_ids_.count()));
    } else if (OB_FAIL(get_object_type_and_name(object_type, object_name, ob_schema,
                                                version, model, transform))) {
      SERVER_LOG(WARN, "fail to get object type and name", K(ret));
    } else if (OB_FAIL(get_ddl_creation_str(ddl_str, object_type, object_name, ob_schema))) {
      SERVER_LOG(WARN, "fail to get ddl creation string", K(ret));
    } else if (OB_FAIL(fill_row_cells(ddl_str, object_type, object_name,
                                      ob_schema, version, model, transform))) {
      SERVER_LOG(WARN, "fail to fill row cells", K(ret));
    } else if (OB_FAIL(scanner_.add_row(cur_row_))) {
      SERVER_LOG(WARN, "fail to add row", K(ret), K(cur_row_));
    } else {
      scanner_it_ = scanner_.begin();
      start_to_read_ = true;
    }
  }
  if (OB_SUCCESS == ret && start_to_read_) {
    if (OB_FAIL(scanner_it_.get_next_row(cur_row_))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        SERVER_LOG(WARN, "fail to get next row", K(ret));
      }
    } else {
      row = &cur_row_;
    }
  }
  return ret;
}

int ObGetObjectDefinition::get_object_type_and_name(GetDDLObjectType &object_type,
                                                  ObString &object_name,
                                                  ObString &ob_schema,
                                                  ObString &version,
                                                  ObString &model,
                                                  ObString &transform)
{
  int ret = OB_SUCCESS;
  const int64_t pk_count = 6;
  if (key_ranges_.count() > 0) {
    const ObRowkey &start_key = key_ranges_.at(0).start_key_;
    const ObRowkey &end_key = key_ranges_.at(0).end_key_;
    const ObObj *start_key_obj_ptr = start_key.get_obj_ptr();
    const ObObj *end_key_obj_ptr = end_key.get_obj_ptr();
    if (start_key.get_obj_cnt() != end_key.get_obj_cnt()) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "start key count and end key count not equal", K(ret));
    } else if (pk_count == start_key.get_obj_cnt()) {
      if (OB_UNLIKELY(NULL == start_key_obj_ptr || NULL == end_key_obj_ptr)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "key obj ptr is NULL", K(ret), K(start_key_obj_ptr), K(end_key_obj_ptr));
      } else if (start_key_obj_ptr[0] == end_key_obj_ptr[0]
                && ObIntType == start_key_obj_ptr[0].get_type()
                && start_key_obj_ptr[1] == end_key_obj_ptr[1]
                && ObVarcharType == start_key_obj_ptr[1].get_type()
                && start_key_obj_ptr[2] == end_key_obj_ptr[2]
                && start_key_obj_ptr[3] == end_key_obj_ptr[3]
                && ObVarcharType == start_key_obj_ptr[3].get_type()
                && start_key_obj_ptr[4] == end_key_obj_ptr[4]
                && ObVarcharType == start_key_obj_ptr[4].get_type()
                && start_key_obj_ptr[5] == end_key_obj_ptr[5]
                && ObVarcharType == start_key_obj_ptr[5].get_type()) {
        object_type = static_cast<GetDDLObjectType>(start_key_obj_ptr[0].get_int());
        object_name = start_key_obj_ptr[1].get_string();
        if (ObVarcharType == start_key_obj_ptr[2].get_type()) {
          ob_schema = start_key_obj_ptr[2].get_string();
        }
        version = start_key_obj_ptr[3].get_string();
        model = start_key_obj_ptr[4].get_string();
        transform = start_key_obj_ptr[5].get_string();
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_USER_ERROR(OB_ERR_UNEXPECTED, "this table is used for get ddl, can't be selected");
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_USER_ERROR(OB_ERR_UNEXPECTED, "this table is used for get ddl, can't be selected");
    }
  }
  return ret;
}

int ObGetObjectDefinition::get_ddl_creation_str(ObString &ddl_str,
                                          GetDDLObjectType object_type,
                                          const ObString &object_name,
                                          const ObString &db_name)
{
  int ret = OB_SUCCESS;
  switch(object_type) {
    case T_GET_DDL_TABLE:
      // table, procedure, package仍使用原来的实现方式，未使用tenant_virtual_object_definition虚拟表
      // 在下一个barrier版本的下一个版本改为使用tenant_virtual_object_definition虚拟表，并删去旧的虚拟表
      //ret = get_table_definition(ddl_str, object_id);
    case T_GET_DDL_PROCEDURE:
    case T_GET_DDL_PACKAGE:
      ret = OB_NOT_SUPPORTED;
      break;
    case T_GET_DDL_CONSTRAINT:
      ret = get_constraint_definition(ddl_str,object_name, db_name, object_type);
      break;
    case T_GET_DDL_REF_CONSTRAINT:
      ret = get_foreign_key_definition(ddl_str,object_name, db_name, object_type);
      break;
    case T_GET_DDL_TABLESPACE:
      ret = get_tablespace_definition(ddl_str, object_name, db_name, object_type);
      break;
    case T_GET_DDL_SEQUENCE:
      ret = get_sequence_definition(ddl_str, object_name, db_name, object_type);
      break;
    case T_GET_DDL_TRIGGER:
      ret = get_trigger_definition(ddl_str, object_name, db_name, object_type);
      break;
    case T_GET_DDL_USER:
      ret = get_user_definition(ddl_str, object_name, db_name, object_type, false);
      break;
    case T_GET_DDL_SYNONYM:
      ret = get_synonym_definition(ddl_str, object_name, db_name, object_type);
      break;
    case T_GET_DDL_TYPE:
    case T_GET_DDL_TYPE_BODY:
    case T_GET_DDL_TYPE_SPEC:
      ret = get_udt_definition(ddl_str, object_name, db_name, object_type);
      break;
    case T_GET_DDL_ROLE:
      ret = get_user_definition(ddl_str, object_name, db_name, object_type, true);
      break;
    default:
      ret = OB_NOT_SUPPORTED;
      SERVER_LOG(WARN, "get ddl object type not supported", K(ret), K(object_type));
  }
  return ret;
}

int ObGetObjectDefinition::print_error_log(GetDDLObjectType object_type,
                                          const common::ObString &db_name,
                                          const common::ObString &object_name)
{
  int ret = OB_ERR_OBJECT_NOT_FOUND;
  const char *type = ObjectTypeName[object_type];
  ObString type_str(type);
  LOG_USER_ERROR(OB_ERR_OBJECT_NOT_FOUND, object_name.length(), object_name.ptr(),
        type_str.length(), type_str.ptr(), db_name.length(), db_name.ptr());
  return ret;
}

/*
int ObGetObjectDefinition::get_table_definition(ObString &ddl_str, const uint64_t table_id)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = NULL;
  if (OB_ISNULL(schema_guard_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "schema guard is NULL", K(ret), K(schema_guard_));
  } else if (OB_FAIL(schema_guard_->get_table_schema(table_id, table_schema))) {
    SERVER_LOG(WARN, "fail to get table schema", K(ret), K(table_id));
  } else if (OB_UNLIKELY(NULL == table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    SERVER_LOG(WARN, "fail to get table schema", K(ret), K(table_id));
  } else if (OB_SYS_TENANT_ID != table_schema->get_tenant_id()
            && table_schema->is_vir_table()
            && is_restrict_access_virtual_table(table_schema->get_table_id())) {
    ret = OB_TABLE_NOT_EXIST;
    SERVER_LOG(WARN, "fail to get table schema", K(ret), K(table_id));
  } else {
    char *table_def_buf = NULL;
    int64_t table_def_buf_size = OB_MAX_VARCHAR_LENGTH;
    if (OB_UNLIKELY(NULL == (table_def_buf = static_cast<char *>
                            (allocator_->alloc(table_def_buf_size))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SERVER_LOG(ERROR, "fail to alloc table_def_buf", K(ret), K(table_def_buf_size));
    } else {
      ObSchemaPrinter schema_printer(*schema_guard_);
      int64_t pos = 0;
      if (table_schema->is_view_table()) {
        if (OB_FAIL(schema_printer.print_view_definiton(table_id,
                                                        table_def_buf,
                                                        OB_MAX_VARCHAR_LENGTH,
                                                        pos))) {
          SERVER_LOG(WARN, "Generate view definition failed");
        }
      } else if (table_schema->is_index_table()) {
        if (OB_FAIL(schema_printer.print_index_table_definition(table_id,
                                                        table_def_buf,
                                                        OB_MAX_VARCHAR_LENGTH,
                                                        pos,
                                                        TZ_INFO(session_),
                                                        false))) {
          SERVER_LOG(WARN, "Generate index definition failed");
        }
      } else {
        const ObLengthSemantics default_length_semantics = 
          session_->get_local_nls_length_semantics();
        // get auto_increment from auto_increment service, not from table option
        if (OB_FAIL(schema_printer.print_table_definition(table_id,
                                                          table_def_buf,
                                                          OB_MAX_VARCHAR_LENGTH,
                                                          pos,
                                                          TZ_INFO(session_),
                                                          default_length_semantics,
                                                          false))) {
          SERVER_LOG(WARN, "Generate table definition failed");
        }
      }
      if (OB_SUCC(ret)) {
        ddl_str.assign(table_def_buf, pos);
      }
    }
  }
  return ret;
}
*/

int ObGetObjectDefinition::get_constraint_definition(ObString &ddl_str,
                                                    const ObString &constraint_name,
                                                    const ObString &db_name,
                                                    GetDDLObjectType object_type)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = table_schema_->get_tenant_id();
  const ObDatabaseSchema *database_schema = NULL;
  const ObTableSchema *table_schema = NULL;
  uint64_t constraint_id = OB_INVALID_ID;
  uint64_t database_id = OB_INVALID_ID;
  ObSimpleConstraintInfo constraint_info;
  const ObTableSchema *unique_index_table_schema = NULL;
  bool is_unique_cst = false;
  if (OB_ISNULL(schema_guard_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "schema guard is NULL", K(ret), K(schema_guard_));
  } else if (OB_FAIL(schema_guard_->get_database_schema(tenant_id, db_name, database_schema))) {
    LOG_WARN("get database schema failed", K(ret));
  } else if (OB_ISNULL(database_schema)) {
    ret = print_error_log(object_type, db_name, constraint_name);
    LOG_WARN("database not found", K(db_name));
  } else if (FALSE_IT(database_id = database_schema->get_database_id())) {
  } else if (OB_FAIL(schema_guard_->get_constraint_info(tenant_id,
                                                        database_id,
                                                        constraint_name,
                                                        constraint_info))) {
    LOG_WARN("get constraint info failed", K(ret), K(tenant_id),
              K(database_id), K(constraint_name));
  } else if (OB_INVALID_ID == (constraint_id = constraint_info.constraint_id_)) {
    // The unique constraint is mocked by a unique index.
    // If other types of constraint is not exist, we will try to find if the uk exists.
    // bool is_unique_constraint_exist = false;
    if (OB_FAIL(schema_guard_->get_idx_schema_by_origin_idx_name(
                tenant_id, database_id, constraint_name, unique_index_table_schema))) {
      LOG_WARN("fail to get idx_schema by origin_idx_name",
               K(ret), K(tenant_id), K(database_id), K(constraint_name));
    } else if (OB_NOT_NULL(unique_index_table_schema)
               && unique_index_table_schema->is_unique_index()
               && !unique_index_table_schema->is_partitioned_table()) {
      // In oracle mode, the unique constraint can only be mocked the non-partitioned unique index.
      is_unique_cst = true;
    } else {
      ret = print_error_log(object_type, db_name, constraint_name);
      LOG_WARN("constraint not found", K(db_name), K(object_type), K(constraint_name));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (is_unique_cst && OB_FAIL(schema_guard_->get_table_schema(
             tenant_id, unique_index_table_schema->get_data_table_id(), table_schema))) {
    LOG_WARN("get table schema failed", K(ret), K(unique_index_table_schema->get_data_table_id()));
  } else if (!is_unique_cst && OB_FAIL(schema_guard_->get_table_schema(
             tenant_id, constraint_info.table_id_, table_schema))) {
    LOG_WARN("get table schema failed", K(ret), K(constraint_info.table_id_));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(ret), K(db_name));
  } else {
    char *cons_def_buf = NULL;
    int64_t cons_def_buf_size = OB_MAX_VARCHAR_LENGTH;
    if (OB_UNLIKELY(NULL == (cons_def_buf = static_cast<char *>
                                            (allocator_->alloc(cons_def_buf_size))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SERVER_LOG(ERROR, "fail to alloc table_def_buf", K(ret), K(cons_def_buf_size));
    } else {
      ObSchemaPrinter schema_printer(*schema_guard_);
      int64_t pos = 0;
      if (is_unique_cst && OB_FAIL(schema_printer.print_unique_cst_definition(*database_schema,
          *table_schema, *unique_index_table_schema, cons_def_buf, cons_def_buf_size, pos))) {
        SERVER_LOG(WARN, "Generate unique constraint definition failed");
      } else if (!is_unique_cst && OB_FAIL(schema_printer.print_constraint_definition(*database_schema,
                                                              *table_schema,
                                                              constraint_id,
                                                              cons_def_buf,
                                                              cons_def_buf_size,
                                                              pos))) {
        SERVER_LOG(WARN, "Generate constraint definition failed");
      } else {
        ddl_str.assign(cons_def_buf, pos);
      }
    }
  }
  return ret;
}

int ObGetObjectDefinition::get_foreign_key_definition(ObString &ddl_str,
                                                    const ObString &foreign_key_name,
                                                    const ObString &db_name,
                                                    GetDDLObjectType object_type)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = table_schema_->get_tenant_id();
  const ObDatabaseSchema *database_schema = NULL;
  const ObTableSchema *table_schema = NULL;
  int64_t foreign_key_id = OB_INVALID_ID;
  uint64_t database_id = OB_INVALID_ID;
  ObSimpleForeignKeyInfo foreign_key_info;
  if (OB_ISNULL(schema_guard_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "schema guard is NULL", K(ret), K(schema_guard_));
  } else if (OB_FAIL(schema_guard_->get_database_schema(tenant_id, db_name, database_schema))) {
    LOG_WARN("get database schema failed", K(ret));
  } else if (OB_ISNULL(database_schema)) {
    ret = print_error_log(object_type, db_name, foreign_key_name);
    LOG_WARN("database not found", K(db_name));
  } else if (FALSE_IT(database_id = database_schema->get_database_id())) {
  } else if (OB_FAIL(schema_guard_->get_foreign_key_info(tenant_id,
                                                        database_id,
                                                        foreign_key_name,
                                                        foreign_key_info))) {
    LOG_WARN("get foreign key info failed", K(ret), K(tenant_id),
              K(database_id), K(foreign_key_name));
  } else if (OB_INVALID_ID == (foreign_key_id = foreign_key_info.foreign_key_id_)) {
    ret = print_error_log(object_type, db_name, foreign_key_name);
    LOG_WARN("foreign key not found", K(ret), K(foreign_key_name));
  } else if (schema_guard_->get_table_schema(tenant_id, foreign_key_info.table_id_, table_schema)) {
    LOG_WARN("get table schema failed", K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(ret), K(db_name));
  } else {
    char *forkey_def_buf = NULL;
    int64_t forkey_def_buf_size = OB_MAX_VARCHAR_LENGTH;
    const ObForeignKeyInfo *forkey_info = NULL;
    const ObIArray<ObForeignKeyInfo> &foreign_key_infos = table_schema->get_foreign_key_infos();
    for (int64_t i = 0; i < foreign_key_infos.count(); i++) {
      const ObForeignKeyInfo &tmp_foreign_key_info = foreign_key_infos.at(i);
      if (foreign_key_id == tmp_foreign_key_info.foreign_key_id_) {
        forkey_info = &tmp_foreign_key_info;
        break;
      }
    }
    if (OB_ISNULL(forkey_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("foreign key info not found", K(table_schema->get_table_name_str()),
                    K(foreign_key_id));
    } else if (OB_UNLIKELY(NULL == (forkey_def_buf = static_cast<char *>
                                                  (allocator_->alloc(forkey_def_buf_size))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("fail to alloc table_def_buf", K(ret), K(forkey_def_buf_size));
    } else {
      ObSchemaPrinter schema_printer(*schema_guard_);
      int64_t pos = 0;
      if (OB_FAIL(schema_printer.print_foreign_key_definition(tenant_id,
                                                              *forkey_info,
                                                              forkey_def_buf,
                                                              forkey_def_buf_size,
                                                              pos))) {
        LOG_WARN("print foreign key definition failed");
      } else {
        ddl_str.assign(forkey_def_buf, pos);
      }
    }
  }
  return ret;
}

int ObGetObjectDefinition::get_tablespace_definition(ObString &ddl_str,
                                                    const ObString &tablespace_name,
                                                    const ObString &db_name,
                                                    GetDDLObjectType object_type)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = table_schema_->get_tenant_id();
  const ObTablespaceSchema *tablespace_schema = NULL;
  if (OB_ISNULL(schema_guard_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "schema guard is NULL", K(ret), K(schema_guard_));
  } else if (OB_FAIL(schema_guard_->get_tablespace_schema_with_name(tenant_id, tablespace_name,
                                                             tablespace_schema))) {
    LOG_WARN("get tablespace schema with name failed", K(ret), K(tenant_id), K(tablespace_name));
  } else if (OB_ISNULL(tablespace_schema)) {
    ret = print_error_log(object_type, db_name, tablespace_name);
    LOG_WARN("tablespace not found", K(ret));
  } else {
    char *tbs_def_buf = NULL;
    int64_t tbs_def_buf_size = OB_MAX_VARCHAR_LENGTH;
    if (OB_UNLIKELY(NULL == (tbs_def_buf = static_cast<char *>
                                                  (allocator_->alloc(tbs_def_buf_size))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SERVER_LOG(ERROR, "fail to alloc tablespace_def_buf", K(ret), K(tbs_def_buf_size));
    } else {
      ObSchemaPrinter schema_printer(*schema_guard_);
      int64_t pos = 0;
      if (OB_FAIL(schema_printer.print_tablespace_definition(tenant_id,
                  tablespace_schema->get_tablespace_id(),
                  tbs_def_buf,
                  tbs_def_buf_size,
                  pos))) {
        SERVER_LOG(WARN, "Generate tablespace definition failed", KR(ret), K(tenant_id));
      } else {
        ddl_str.assign(tbs_def_buf, pos);
      }
    }
  }
  return ret;
}

int ObGetObjectDefinition::get_sequence_definition(ObString &ddl_str,
                                                  const ObString &sequence_name,
                                                  const ObString &db_name,
                                                  GetDDLObjectType object_type)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = table_schema_->get_tenant_id();
  uint64_t database_id = OB_INVALID_ID;
  const ObSequenceSchema *sequence_schema = NULL;
  const ObDatabaseSchema *database_schema = NULL;
  if (OB_ISNULL(schema_guard_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "schema guard is NULL", K(ret), K(schema_guard_));
  } else if (OB_FAIL(schema_guard_->get_database_schema(tenant_id, db_name, database_schema))) {
    LOG_WARN("get database schema failed", K(ret));
  } else if (OB_ISNULL(database_schema)) {
    ret = print_error_log(object_type, db_name, sequence_name);
    LOG_WARN("database not found", K(db_name));
  } else if (FALSE_IT(database_id = database_schema->get_database_id())) {
  } else if (OB_FAIL(schema_guard_->get_sequence_schema_with_name(tenant_id, database_id,
                                                    sequence_name, sequence_schema))) {
    LOG_WARN("get sequence schema failed", K(ret), K(tenant_id), K(sequence_name));
  } else if (OB_ISNULL(sequence_schema)) {
    ret = print_error_log(object_type, db_name, sequence_name);
    LOG_WARN("sequence not found", K(ret));
  } else {
    char *sequence_def_buf = NULL;
    int64_t sequence_def_buf_size = OB_MAX_VARCHAR_LENGTH;
    if (OB_UNLIKELY(NULL == (sequence_def_buf = static_cast<char *>
                                                  (allocator_->alloc(sequence_def_buf_size))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SERVER_LOG(ERROR, "fail to alloc sequence_def_buf", K(ret), K(sequence_def_buf_size));
    } else {
      ObSchemaPrinter schema_printer(*schema_guard_);
      int64_t pos = 0;
      if (OB_FAIL(schema_printer.print_sequence_definition(*sequence_schema,
                                                          sequence_def_buf,
                                                          sequence_def_buf_size,
                                                          pos,
                                                          true))) {
        SERVER_LOG(WARN, "Generate sequence definition failed");
      } else {
        ddl_str.assign(sequence_def_buf, pos);
      }
    }
  }
  return ret;
}

int ObGetObjectDefinition::get_trigger_definition(ObString &ddl_str,
                                                  const ObString &trigger_name,
                                                  const ObString &db_name,
                                                  GetDDLObjectType object_type)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = table_schema_->get_tenant_id();
  uint64_t database_id = OB_INVALID_ID;
  const ObTriggerInfo *trigger_info = NULL;
  const ObDatabaseSchema *database_schema = NULL;
  if (OB_ISNULL(schema_guard_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "schema guard is NULL", K(ret), K(schema_guard_));
  } else if (OB_FAIL(schema_guard_->get_database_schema(tenant_id, db_name, database_schema))) {
    LOG_WARN("get database schema failed", K(ret));
  } else if (OB_ISNULL(database_schema)) {
    ret = print_error_log(object_type, db_name, trigger_name);
    LOG_WARN("database not found", K(db_name));
  } else if (FALSE_IT(database_id = database_schema->get_database_id())) {
  } else if (OB_FAIL(schema_guard_->get_trigger_info(tenant_id, database_id, trigger_name,
                                                     trigger_info))) {
    LOG_WARN("get trigger info failed", K(ret), K(tenant_id), K(trigger_name));
  } else if (OB_ISNULL(trigger_info)) {
    ret = print_error_log(object_type, db_name, trigger_name);
    LOG_WARN("trigger not found", K(ret));
  } else {
    char *trigger_def_buf = NULL;
    int64_t trigger_def_buf_size = OB_MAX_VARCHAR_LENGTH;
    if (OB_UNLIKELY(NULL == (trigger_def_buf = static_cast<char *>
                                                  (allocator_->alloc(trigger_def_buf_size))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SERVER_LOG(ERROR, "fail to alloc trigger_def_buf", K(ret), K(trigger_def_buf_size));
    } else {
      ObSchemaPrinter schema_printer(*schema_guard_);
      int64_t pos = 0;
      if (OB_FAIL(schema_printer.print_trigger_definition(*trigger_info,
                                                          trigger_def_buf,
                                                          trigger_def_buf_size,
                                                          pos, true))) {
        SERVER_LOG(WARN, "Generate trigger definition failed");
      } else {
        ddl_str.assign(trigger_def_buf, pos);
      }
    }
  }
  return ret;
}

int ObGetObjectDefinition::get_user_definition(ObString &ddl_str,
                                              const ObString &user_name,
                                              const ObString &db_name,
                                              GetDDLObjectType object_type,
                                              bool is_role)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = table_schema_->get_tenant_id();
  const ObUserInfo *user_info = NULL;
  ObArray<const ObUserInfo *> users_info;
  if (OB_ISNULL(schema_guard_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "schema guard is NULL", K(ret), K(schema_guard_));
  } else if (OB_FAIL(schema_guard_->get_user_info(tenant_id, user_name, users_info))) {
    LOG_WARN("get user info with name failed", K(ret), K(tenant_id), K(user_name));
  } else if (users_info.empty()) {
    ret = print_error_log(object_type, db_name, user_name);
    LOG_WARN("user not found", K(ret));
  } else if (users_info.count() > 1) {
    //用户名不能作为用户的唯一标识，username+hostname才是。
    //但是由于oracle模式创建用户时会同时创建同名数据库，所以不会允许创建username相同的用户，即使它们的hostname不同。
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("users with same name is not allowed in oracle mode", K(ret));
  } else if (OB_ISNULL(user_info = users_info.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("user info is null", K(ret));
  } else if (user_info->is_role() != is_role) {
    ret = print_error_log(object_type, db_name, user_name);
    LOG_WARN("user not found", K(ret));
  } else {
    char *user_def_buf = NULL;
    int64_t user_def_buf_size = OB_MAX_VARCHAR_LENGTH;
    if (OB_UNLIKELY(NULL == (user_def_buf = static_cast<char *>
                                                  (allocator_->alloc(user_def_buf_size))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SERVER_LOG(ERROR, "fail to alloc user_def_buf", K(ret), K(user_def_buf_size));
    } else {
      ObSchemaPrinter schema_printer(*schema_guard_);
      int64_t pos = 0;
      if (OB_FAIL(schema_printer.print_user_definition(tenant_id,
                                                      *user_info,
                                                      user_def_buf,
                                                      user_def_buf_size,
                                                      pos,
                                                      is_role))) {
        SERVER_LOG(WARN, "Generate user definition failed");
      } else {
        ddl_str.assign(user_def_buf, pos);
      }
    }
  }
  return ret;
}

int ObGetObjectDefinition::get_synonym_definition(ObString &ddl_str, const ObString &synonym_name,
                                                  const ObString &db_name,
                                                  GetDDLObjectType object_type)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = table_schema_->get_tenant_id();
  uint64_t database_id = OB_INVALID_ID;
  const ObSynonymInfo *synonym_info = NULL;
  const ObDatabaseSchema *database_schema = NULL;
  if (OB_ISNULL(schema_guard_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "schema guard is NULL", K(ret), K(schema_guard_));
  } else if (OB_FAIL(schema_guard_->get_database_schema(tenant_id, db_name, database_schema))) {
    LOG_WARN("get database schema failed", K(ret));
  } else if (OB_ISNULL(database_schema)) {
    ret = print_error_log(object_type, db_name, synonym_name);
    LOG_WARN("database not found", K(db_name));
  } else if (FALSE_IT(database_id = database_schema->get_database_id())) {
  } else if (OB_FAIL(schema_guard_->get_synonym_info(tenant_id, database_id, synonym_name,
                                                      synonym_info))) {
    LOG_WARN("get synonym info failed", K(ret), K(tenant_id), K(synonym_name));
  } else if (OB_ISNULL(synonym_info)) {
    ret = print_error_log(object_type, db_name, synonym_name);
    LOG_WARN("synonym not found", K(ret));
  } else {
    char *synonym_def_buf = NULL;
    int64_t synonym_def_buf_size = OB_MAX_VARCHAR_LENGTH;
    if (OB_UNLIKELY(NULL == (synonym_def_buf = static_cast<char *>
                                                  (allocator_->alloc(synonym_def_buf_size))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SERVER_LOG(ERROR, "fail to alloc synonym_def_buf", K(ret), K(synonym_def_buf_size));
    } else {
      ObSchemaPrinter schema_printer(*schema_guard_);
      int64_t pos = 0;
      if (OB_FAIL(schema_printer.print_synonym_definition(*synonym_info,
                                                          synonym_def_buf,
                                                          synonym_def_buf_size,
                                                          pos))) {
        SERVER_LOG(WARN, "Generate synonym definition failed");
      } else {
        ddl_str.assign(synonym_def_buf, pos);
      }
    }
  }
  return ret;
}

int ObGetObjectDefinition::get_udt_definition(ObString &ddl_str, const ObString &udt_name,
                                              const ObString &db_name,
                                              GetDDLObjectType object_type)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = table_schema_->get_tenant_id();
  uint64_t database_id = OB_INVALID_ID;
  const ObUDTTypeInfo *udt_info = NULL;
  const ObDatabaseSchema *database_schema = NULL;
  bool body_exist = false;
  if (OB_ISNULL(schema_guard_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "schema guard is NULL", K(ret), K(schema_guard_));
  } else if (OB_FAIL(schema_guard_->get_database_schema(tenant_id, db_name, database_schema))) {
    LOG_WARN("get database schema failed", K(ret));
  } else if (OB_ISNULL(database_schema)) {
    ret = print_error_log(object_type, db_name, udt_name);
    LOG_WARN("database not found", K(db_name));
  } else if (FALSE_IT(database_id = database_schema->get_database_id())) {
  } else if (OB_FAIL(schema_guard_->get_udt_info(tenant_id, database_id, OB_INVALID_ID, udt_name,
                                                udt_info))) {
    LOG_WARN("get udt info failed", K(ret), K(tenant_id), K(udt_name));
  } else if (OB_ISNULL(udt_info)) {
    ret = print_error_log(object_type, db_name, udt_name);
    LOG_WARN("type not found", K(ret));
  } else {
    char *udt_def_buf = NULL;
    int64_t udt_def_buf_size = OB_MAX_VARCHAR_LENGTH;
    if (OB_UNLIKELY(NULL == (udt_def_buf = static_cast<char *>
                                                  (allocator_->alloc(udt_def_buf_size))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SERVER_LOG(ERROR, "fail to alloc udt_def_buf", K(ret), K(udt_def_buf_size));
    } else {
      ObSchemaPrinter schema_printer(*schema_guard_);
      int64_t pos = 0;
      if (T_GET_DDL_TYPE == object_type || T_GET_DDL_TYPE_SPEC == object_type) {
        if (OB_FAIL(schema_printer.print_udt_definition(udt_info->get_tenant_id(),
                                                        udt_info->get_type_id(),
                                                        udt_def_buf,
                                                        udt_def_buf_size,
                                                        pos))) {
          SERVER_LOG(WARN, "Generate udt definition failed", KR(ret), KPC(udt_info));
        }
      }
      if (OB_SUCC(ret) && (T_GET_DDL_TYPE == object_type || T_GET_DDL_TYPE_BODY == object_type)) {
        if (OB_FAIL(schema_printer.print_udt_body_definition(udt_info->get_tenant_id(),
                                                             udt_info->get_type_id(),
                                                             udt_def_buf,
                                                             udt_def_buf_size,
                                                             pos, body_exist))) {
          SERVER_LOG(WARN, "Generate udt body definition failed", KR(ret), KPC(udt_info));
        } else if (OB_UNLIKELY(! body_exist && T_GET_DDL_TYPE_BODY == object_type)) {
          ret = print_error_log(object_type, db_name, udt_name);
          LOG_WARN("type not found", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        ddl_str.assign(udt_def_buf, pos);
      }
    }
  }
  return ret;
}

int ObGetObjectDefinition::fill_row_cells(ObString &ddl_str,
                                          int64_t object_type,
                                          ObString &object_name,
                                          ObString &ob_schema,
                                          ObString &version,
                                          ObString &model,
                                          ObString &transform)
{
  int ret = OB_SUCCESS;
  uint64_t cell_idx = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < output_column_ids_.count(); ++i) {
    uint64_t col_id = output_column_ids_.at(i);
    switch(col_id) {
      case OB_APP_MIN_COLUMN_ID:
        // object_type
        cur_row_.cells_[cell_idx].set_int(object_type);
        break;
      case OB_APP_MIN_COLUMN_ID + 1:
        // object_name
        cur_row_.cells_[cell_idx].set_varchar(object_name);
        cur_row_.cells_[cell_idx].set_collation_type(
            ObCharset::get_default_collation(ObCharset::get_default_charset()));
        break;
      case OB_APP_MIN_COLUMN_ID + 2:
        // schema
        cur_row_.cells_[cell_idx].set_varchar(ob_schema);
        cur_row_.cells_[cell_idx].set_collation_type(
            ObCharset::get_default_collation(ObCharset::get_default_charset()));
        break;
      case OB_APP_MIN_COLUMN_ID + 3:
        // version
        cur_row_.cells_[cell_idx].set_varchar(version);
        cur_row_.cells_[cell_idx].set_collation_type(
            ObCharset::get_default_collation(ObCharset::get_default_charset()));
        break;
      case OB_APP_MIN_COLUMN_ID + 4:
        // model
        cur_row_.cells_[cell_idx].set_varchar(model);
        cur_row_.cells_[cell_idx].set_collation_type(
            ObCharset::get_default_collation(ObCharset::get_default_charset()));
        break;
      case OB_APP_MIN_COLUMN_ID + 5: {
        // transform
        cur_row_.cells_[cell_idx].set_varchar(transform);
        cur_row_.cells_[cell_idx].set_collation_type(
            ObCharset::get_default_collation(ObCharset::get_default_charset()));
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 6: {
        // object definition
        cur_row_.cells_[cell_idx].set_lob_value(ObLongTextType, ddl_str.ptr(),
                                                static_cast<int32_t>(ddl_str.length()));
        cur_row_.cells_[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                                     ObCharset::get_default_charset()));
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "invalid column id", K(ret), K(cell_idx),
                    K(i), K(output_column_ids_), K(col_id));
        break;
      }
    }
    if (OB_SUCC(ret)) {
      cell_idx++;
    }
  }
  return ret;
}

}/* ns observer*/
}/* ns oceanbase */
