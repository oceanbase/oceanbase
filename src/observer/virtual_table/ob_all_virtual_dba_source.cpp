/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "observer/virtual_table/ob_all_virtual_dba_source.h"

#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_routine_info.h"
#include "share/schema/ob_schema_printer.h"
#include "common/sql_mode/ob_sql_mode_utils.h"
#include "sql/session/ob_sql_session_info.h"
#include "observer/ob_server_struct.h"
#include "sql/engine/expr/ob_expr_user_can_access_obj.h"

using namespace oceanbase::common;
using namespace oceanbase::share::schema;
namespace oceanbase
{
namespace observer
{

ObAllVirtualDbaSource::ObAllVirtualDbaSource()
    : ObVirtualTableScannerIterator(),
      tenant_id_(OB_INVALID_ID),
      iter_state_()
{
}

ObAllVirtualDbaSource::~ObAllVirtualDbaSource()
{
}

void ObAllVirtualDbaSource::reset()
{
  tenant_id_ = OB_INVALID_ID;
  iter_state_.reset();
  package_array_.reset();
  routine_array_.reset();
  trigger_array_.reset();
  udt_array_.reset();
  ObVirtualTableScannerIterator::reset();
}

int ObAllVirtualDbaSource::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_) || OB_ISNULL(schema_guard_) || OB_ISNULL(session_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "argument is NULL", K(allocator_), K(schema_guard_), K(session_), K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "tenant_id is invalid", K(ret));
  } else {
    if (!start_to_read_) {
      if (OB_FAIL(get_package_arrays())) {
        SERVER_LOG(WARN, "fail to get package arrays", K(ret));
      } else if (OB_FAIL(get_routine_arrays())) {
        SERVER_LOG(WARN, "fail to get routine arrays", K(ret));
      } else if (OB_FAIL(get_trigger_arrays())) {
        SERVER_LOG(WARN, "fail to get trigger arrays", K(ret));
      } else if (OB_FAIL(get_udt_arrays())) {
        SERVER_LOG(WARN, "fail to get udt arrays", K(ret));
      } else if (OB_FAIL(get_system_package_arrays())) {
        SERVER_LOG(WARN, "fail to get system package arrays", K(ret));
      } else if (OB_FAIL(get_system_routine_arrays())) {
        SERVER_LOG(WARN, "fail to get system routine arrays", K(ret));
      } else if (OB_FAIL(get_system_trigger_arrays())) {
        SERVER_LOG(WARN, "fail to get system trigger arrays", K(ret));
      } else if (OB_FAIL(get_system_udt_arrays())) {
        SERVER_LOG(WARN, "fail to get system udt arrays", K(ret));
      } else {
        start_to_read_ = true;
      }
    }

    if (OB_SUCC(ret) && start_to_read_) {
      ret = get_next_source_line(row);
    }
  }
  return ret;
}

int ObAllVirtualDbaSource::get_next_source_line(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  bool found_line = false;

  while (OB_SUCC(ret) && !found_line) {
    if (iter_state_.lines_.count() == 0) {
      ObString source_text;
      switch (iter_state_.current_type_) {
        case SOURCE_PACKAGE:
          if (iter_state_.object_idx_ < package_array_.count()) {
            const ObPackageInfo *package_info = package_array_.at(iter_state_.object_idx_);
            if (OB_NOT_NULL(package_info)) {
              source_text = package_info->get_source();
            }
          }
          break;
        case SOURCE_ROUTINE:
          if (iter_state_.object_idx_ < routine_array_.count()) {
            const ObRoutineInfo *routine_info = routine_array_.at(iter_state_.object_idx_);
            if (OB_NOT_NULL(routine_info)) {
              source_text = routine_info->get_routine_body();
            }
          }
          break;
        case SOURCE_TRIGGER:
          if (iter_state_.object_idx_ < trigger_array_.count()) {
            const ObTriggerInfo *trigger_info = trigger_array_.at(iter_state_.object_idx_);
            if (OB_NOT_NULL(trigger_info)) {
              source_text = trigger_info->get_trigger_body();
            }
          }
          break;
        case SOURCE_TYPE:
          if (iter_state_.object_idx_ < udt_array_.count()) {
            const ObUDTTypeInfo *udt_info = udt_array_.at(iter_state_.object_idx_);
            if (OB_NOT_NULL(udt_info)) {
              const common::ObIArray<share::schema::ObUDTObjectType*> &object_type_infos = udt_info->get_object_type_infos();
              if (iter_state_.udt_object_type_idx_ < object_type_infos.count()) {
                const share::schema::ObUDTObjectType *object_info = object_type_infos.at(iter_state_.udt_object_type_idx_);
                if (OB_NOT_NULL(object_info)) {
                  source_text = object_info->get_source();
                }
              }
            }
          }
          break;
        default:
          if (move_to_next_object_type()) {
            ret = OB_ITER_END;
            break;
          }
          continue;
      }

      if (!source_text.empty()) {
        if (OB_FAIL(split_text_into_lines(source_text, iter_state_.lines_))) {
          SERVER_LOG(WARN, "fail to split text into lines", K(ret));
        } else {
          iter_state_.line_idx_ = 0;
        }
      }

      if (OB_SUCC(ret) && iter_state_.lines_.count() == 0) {
        bool need_next_type = move_to_next_object_type();

        if (need_next_type) {
          if (move_to_next_object_type()) {
            ret = OB_ITER_END;
            break;
          }
        }
        continue;
      }
    }

    if (iter_state_.line_idx_ < iter_state_.lines_.count()) {
      const ObString &line_text = iter_state_.lines_.at(iter_state_.line_idx_);
      int64_t line_num = iter_state_.line_idx_ + 1;

      switch (iter_state_.current_type_) {
        case SOURCE_PACKAGE:
          if (iter_state_.object_idx_ < package_array_.count()) {
            ret = fill_row_from_package(package_array_.at(iter_state_.object_idx_), line_text, line_num);
            found_line = true;
          }
          break;
        case SOURCE_ROUTINE:
          if (iter_state_.object_idx_ < routine_array_.count()) {
            ret = fill_row_from_routine(routine_array_.at(iter_state_.object_idx_), line_text, line_num);
            found_line = true;
          }
          break;
        case SOURCE_TRIGGER:
          if (iter_state_.object_idx_ < trigger_array_.count()) {
            ret = fill_row_from_trigger(trigger_array_.at(iter_state_.object_idx_), line_text, line_num);
            found_line = true;
          }
          break;
        case SOURCE_TYPE:
          if (iter_state_.object_idx_ < udt_array_.count()) {
            const ObUDTTypeInfo *udt_info = udt_array_.at(iter_state_.object_idx_);
            if (OB_NOT_NULL(udt_info)) {
              const common::ObIArray<share::schema::ObUDTObjectType*> &object_type_infos = udt_info->get_object_type_infos();
              if (iter_state_.udt_object_type_idx_ < object_type_infos.count()) {
                const share::schema::ObUDTObjectType *object_info = object_type_infos.at(iter_state_.udt_object_type_idx_);
                ret = fill_row_from_udt(udt_info, object_info, line_text, line_num);
                found_line = true;
              }
            }
          }
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "invalid source object type", K(iter_state_.current_type_), K(ret));
      }

      if (OB_SUCC(ret)) {
        iter_state_.line_idx_++;
        if (found_line) {
          row = &cur_row_;
        }
      }
    } else {
      bool need_next_type = move_to_next_object();

      if (need_next_type) {
        if (move_to_next_object_type()) {
          ret = OB_ITER_END;
          break;
        }
      }
    }
  }

  return ret;
}

int ObAllVirtualDbaSource::split_text_into_lines(const ObString &text, ObArray<ObString> &lines)
{
  int ret = OB_SUCCESS;
  lines.reset();

  if (text.empty()) {
    return ret;
  }

  const char *start = text.ptr();
  const char *end = text.ptr() + text.length();
  const char *line_start = start;

  for (const char *p = start; p < end; ++p) {
    if (*p == '\n' || *p == '\r') {
      if (p > line_start) {
        if (OB_FAIL(push_line_segments(line_start, p - line_start, lines))) {
          break;
        }
      }

      if (p + 1 < end && ((*p == '\r' && *(p + 1) == '\n') || (*p == '\n' && *(p + 1) == '\r'))) {
        p++;
      }
      line_start = p + 1;
    }
  }

  if (OB_SUCC(ret) && line_start < end) {
    if (OB_FAIL(push_line_segments(line_start, end - line_start, lines))) {
      SERVER_LOG(WARN, "fail to push back last line", K(ret));
    }
  }

  return ret;
}

int ObAllVirtualDbaSource::push_line_segments(const char *seg_start,
                         int64_t seg_len,
                         common::ObArray<common::ObString> &lines)
{
  int ret = OB_SUCCESS;
  const int64_t text_col_limit = 4000;
  const bool need_chunk = (lib::is_oracle_mode()) && (seg_len > text_col_limit);
  if (!need_chunk) {
    ObString line;
    line.assign_ptr(seg_start, static_cast<int32_t>(seg_len));
    if (OB_FAIL(lines.push_back(line))) {
      SERVER_LOG(WARN, "fail to push back line", K(ret));
    }
  } else {
    while (OB_SUCC(ret) && seg_len > 0) {
      const int64_t chunk_len = seg_len > text_col_limit
                                  ? text_col_limit
                                  : seg_len;
      ObString line;
      line.assign_ptr(seg_start, static_cast<int32_t>(chunk_len));
      if (OB_FAIL(lines.push_back(line))) {
        SERVER_LOG(WARN, "fail to push back line", K(ret));
        break;
      }
      seg_start += chunk_len;
      seg_len -= chunk_len;
    }
  }
  return ret;
}

int ObAllVirtualDbaSource::fill_row_from_package(const ObPackageInfo *package_info,
                                                const ObString &line_text,
                                                int64_t line_num)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(package_info)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "package info is null", K(ret));
  } else {
    ObObj *cells = cur_row_.cells_;

    const ObDatabaseSchema *db_schema = NULL;
    bool is_sys_object = (package_info->get_tenant_id() == OB_SYS_TENANT_ID);
    if (!is_sys_object) {
      if (OB_FAIL(schema_guard_->get_database_schema(tenant_id_, package_info->get_database_id(), db_schema))) {
        SERVER_LOG(WARN, "fail to get database schema", K(ret), K(tenant_id_), K(package_info->get_database_id()));
      } else if (OB_ISNULL(db_schema)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "database schema is null", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      for (int64_t col_idx = 0; OB_SUCC(ret) && col_idx < output_column_ids_.count(); ++col_idx) {
        const uint64_t col_id = output_column_ids_.at(col_idx);
        switch (col_id) {
          case OWNER:
            if (is_sys_object) {
              cells[col_idx].set_varchar("SYS");
            } else {
              cells[col_idx].set_varchar(db_schema->get_database_name());
            }
            cells[col_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          case NAME:
            cells[col_idx].set_varchar(package_info->get_package_name());
            cells[col_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          case TYPE:
            cells[col_idx].set_varchar(package_info->get_type() == PACKAGE_TYPE ? "PACKAGE" : "PACKAGE BODY");
            cells[col_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          case LINE:
            {
              number::ObNumber line_number;
              if (OB_FAIL(line_number.from(line_num, *allocator_))) {
                SERVER_LOG(WARN, "failed to convert line number", K(ret), K(line_num));
              } else {
                cells[col_idx].set_number(line_number);
              }
            }
            break;
          case TEXT:
            cells[col_idx].set_varchar(line_text);
            cells[col_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          case ORIGIN_CON_ID:
            {
              number::ObNumber tenant_id_number;
              if (OB_FAIL(tenant_id_number.from(package_info->get_tenant_id(), *allocator_))) {
                SERVER_LOG(WARN, "failed to convert tenant id", K(ret), K(package_info->get_tenant_id()));
              } else {
                cells[col_idx].set_number(tenant_id_number);
              }
            }
            break;
          case OBJECT_ID:
            {
              number::ObNumber object_id_number;
              if (OB_FAIL(object_id_number.from(package_info->get_package_id(), *allocator_))) {
                SERVER_LOG(WARN, "failed to convert object id", K(ret), K(package_info->get_package_id()));
              } else {
                cells[col_idx].set_number(object_id_number);
              }
            }
            break;
          case DATABASE_ID:
            {
              number::ObNumber database_id_number;
              if (OB_FAIL(database_id_number.from(package_info->get_database_id(), *allocator_))) {
                SERVER_LOG(WARN, "failed to convert database id", K(ret), K(package_info->get_database_id()));
              } else {
                cells[col_idx].set_number(database_id_number);
              }
            }
            break;
          default:
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "invalid column id", K(col_id), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObAllVirtualDbaSource::fill_row_from_routine(const ObRoutineInfo *routine_info,
                                                const ObString &line_text,
                                                int64_t line_num)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(routine_info)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "routine info is null", K(ret));
  } else {
    ObObj *cells = cur_row_.cells_;

    const ObDatabaseSchema *db_schema = NULL;
    bool is_sys_object = (routine_info->get_tenant_id() == OB_SYS_TENANT_ID);
    if (!is_sys_object) {
      if (OB_FAIL(schema_guard_->get_database_schema(tenant_id_, routine_info->get_database_id(), db_schema))) {
        SERVER_LOG(WARN, "fail to get database schema", K(ret), K(tenant_id_), K(routine_info->get_database_id()));
      } else if (OB_ISNULL(db_schema)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "database schema is null", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      for (int64_t col_idx = 0; OB_SUCC(ret) && col_idx < output_column_ids_.count(); ++col_idx) {
        const uint64_t col_id = output_column_ids_.at(col_idx);
        switch (col_id) {
          case OWNER:
            if (is_sys_object) {
              cells[col_idx].set_varchar("SYS");
            } else {
              cells[col_idx].set_varchar(db_schema->get_database_name());
            }
            cells[col_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          case NAME:
            cells[col_idx].set_varchar(routine_info->get_routine_name());
            cells[col_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          case TYPE:
            cells[col_idx].set_varchar(routine_info->get_routine_type() == ROUTINE_PROCEDURE_TYPE ? "PROCEDURE" : "FUNCTION");
            cells[col_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          case LINE:
            {
              number::ObNumber line_number;
              if (OB_FAIL(line_number.from(line_num, *allocator_))) {
                SERVER_LOG(WARN, "failed to convert line number", K(ret), K(line_num));
              } else {
                cells[col_idx].set_number(line_number);
              }
            }
            break;
          case TEXT:
            cells[col_idx].set_varchar(line_text);
            cells[col_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          case ORIGIN_CON_ID:
            {
              number::ObNumber tenant_id_number;
              if (OB_FAIL(tenant_id_number.from(routine_info->get_tenant_id(), *allocator_))) {
                SERVER_LOG(WARN, "failed to convert tenant id", K(ret), K(routine_info->get_tenant_id()));
              } else {
                cells[col_idx].set_number(tenant_id_number);
              }
            }
            break;
          case OBJECT_ID:
            {
              number::ObNumber object_id_number;
              if (OB_FAIL(object_id_number.from(routine_info->get_routine_id(), *allocator_))) {
                SERVER_LOG(WARN, "failed to convert object id", K(ret), K(routine_info->get_routine_id()));
              } else {
                cells[col_idx].set_number(object_id_number);
              }
            }
            break;
          case DATABASE_ID:
            {
              number::ObNumber database_id_number;
              if (OB_FAIL(database_id_number.from(routine_info->get_database_id(), *allocator_))) {
                SERVER_LOG(WARN, "failed to convert database id", K(ret), K(routine_info->get_database_id()));
              } else {
                cells[col_idx].set_number(database_id_number);
              }
            }
            break;
          default:
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "invalid column id", K(col_id), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObAllVirtualDbaSource::fill_row_from_trigger(const ObTriggerInfo *trigger_info,
                                                const ObString &line_text,
                                                int64_t line_num)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(trigger_info)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "trigger info is null", K(ret));
  } else {
    ObObj *cells = cur_row_.cells_;

    const ObDatabaseSchema *db_schema = NULL;
    bool is_sys_object = (trigger_info->get_tenant_id() == OB_SYS_TENANT_ID);
    if (!is_sys_object) {
      if (OB_FAIL(schema_guard_->get_database_schema(tenant_id_, trigger_info->get_database_id(), db_schema))) {
        SERVER_LOG(WARN, "fail to get database schema", K(ret), K(tenant_id_), K(trigger_info->get_database_id()));
      } else if (OB_ISNULL(db_schema)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "database schema is null", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      for (int64_t col_idx = 0; OB_SUCC(ret) && col_idx < output_column_ids_.count(); ++col_idx) {
        const uint64_t col_id = output_column_ids_.at(col_idx);
        switch (col_id) {
          case OWNER:
            if (is_sys_object) {
              cells[col_idx].set_varchar("SYS");
            } else {
              cells[col_idx].set_varchar(db_schema->get_database_name());
            }
            cells[col_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          case NAME:
            cells[col_idx].set_varchar(trigger_info->get_trigger_name());
            cells[col_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          case TYPE:
            cells[col_idx].set_varchar("TRIGGER");
            cells[col_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          case LINE:
            {
              number::ObNumber line_number;
              if (OB_FAIL(line_number.from(line_num, *allocator_))) {
                SERVER_LOG(WARN, "failed to convert line number", K(ret), K(line_num));
              } else {
                cells[col_idx].set_number(line_number);
              }
            }
            break;
          case TEXT:
            cells[col_idx].set_varchar(line_text);
            cells[col_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          case ORIGIN_CON_ID:
            {
              number::ObNumber tenant_id_number;
              if (OB_FAIL(tenant_id_number.from(trigger_info->get_tenant_id(), *allocator_))) {
                SERVER_LOG(WARN, "failed to convert tenant id", K(ret), K(trigger_info->get_tenant_id()));
              } else {
                cells[col_idx].set_number(tenant_id_number);
              }
            }
            break;
          case OBJECT_ID:
            {
              number::ObNumber object_id_number;
              if (OB_FAIL(object_id_number.from(trigger_info->get_trigger_id(), *allocator_))) {
                SERVER_LOG(WARN, "failed to convert object id", K(ret), K(trigger_info->get_trigger_id()));
              } else {
                cells[col_idx].set_number(object_id_number);
              }
            }
            break;
          case DATABASE_ID:
            {
              number::ObNumber database_id_number;
              if (OB_FAIL(database_id_number.from(trigger_info->get_database_id(), *allocator_))) {
                SERVER_LOG(WARN, "failed to convert database id", K(ret), K(trigger_info->get_database_id()));
              } else {
                cells[col_idx].set_number(database_id_number);
              }
            }
            break;
          default:
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "invalid column id", K(col_id), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObAllVirtualDbaSource::fill_row_from_udt(const ObUDTTypeInfo *udt_info,
                                            const share::schema::ObUDTObjectType *object_type_info,
                                            const ObString &line_text,
                                            int64_t line_num)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(udt_info)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "udt info is null", K(ret));
  } else if (OB_ISNULL(object_type_info)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "object type info is null", K(ret));
  } else {
    ObObj *cells = cur_row_.cells_;

    const ObDatabaseSchema *db_schema = NULL;
    bool is_sys_object = (udt_info->get_tenant_id() == OB_SYS_TENANT_ID);
    if (!is_sys_object) {
      if (OB_FAIL(schema_guard_->get_database_schema(tenant_id_, udt_info->get_database_id(), db_schema))) {
        SERVER_LOG(WARN, "fail to get database schema", K(ret), K(tenant_id_), K(udt_info->get_database_id()));
      } else if (OB_ISNULL(db_schema)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "database schema is null", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      for (int64_t col_idx = 0; OB_SUCC(ret) && col_idx < output_column_ids_.count(); ++col_idx) {
        const uint64_t col_id = output_column_ids_.at(col_idx);
        switch (col_id) {
          case OWNER:
            if (is_sys_object) {
              cells[col_idx].set_varchar("SYS");
            } else {
              cells[col_idx].set_varchar(db_schema->get_database_name());
            }
            cells[col_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          case NAME:
            cells[col_idx].set_varchar(udt_info->get_type_name());
            cells[col_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          case TYPE:
            if (object_type_info->is_object_spec()) {
              cells[col_idx].set_varchar("TYPE");
            } else if (object_type_info->is_object_body()) {
              cells[col_idx].set_varchar("TYPE BODY");
            } else {
              cells[col_idx].set_varchar("TYPE");
            }
          cells[col_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        case LINE:
          {
            number::ObNumber line_number;
            if (OB_FAIL(line_number.from(line_num, *allocator_))) {
              SERVER_LOG(WARN, "failed to convert line number", K(ret), K(line_num));
            } else {
              cells[col_idx].set_number(line_number);
            }
          }
          break;
          case TEXT:
            cells[col_idx].set_varchar(line_text);
            cells[col_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          case ORIGIN_CON_ID:
            {
              number::ObNumber tenant_id_number;
              if (OB_FAIL(tenant_id_number.from(udt_info->get_tenant_id(), *allocator_))) {
                SERVER_LOG(WARN, "failed to convert tenant id", K(ret), K(udt_info->get_tenant_id()));
              } else {
                cells[col_idx].set_number(tenant_id_number);
              }
            }
            break;
          case OBJECT_ID:
            {
              number::ObNumber object_id_number;
              if (OB_FAIL(object_id_number.from(udt_info->get_type_id(), *allocator_))) {
                SERVER_LOG(WARN, "failed to convert object id", K(ret), K(udt_info->get_type_id()));
              } else {
                cells[col_idx].set_number(object_id_number);
              }
            }
            break;
          case DATABASE_ID:
            {
              number::ObNumber database_id_number;
              if (OB_FAIL(database_id_number.from(object_type_info->get_database_id(), *allocator_))) {
                SERVER_LOG(WARN, "failed to convert database id", K(ret), K(object_type_info->get_database_id()));
              } else {
                cells[col_idx].set_number(database_id_number);
              }
            }
            break;
          default:
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "invalid column id", K(col_id), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObAllVirtualDbaSource::get_package_arrays()
{
  int ret = OB_SUCCESS;
  package_array_.reset();

  ObArray<const ObPackageInfo *> package_infos;
  if (OB_FAIL(schema_guard_->get_package_infos_in_tenant(tenant_id_, package_infos))) {
    SERVER_LOG(WARN, "fail to get package infos", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < package_infos.count(); ++i) {
      const ObPackageInfo *package_info = package_infos.at(i);
      if (OB_NOT_NULL(package_info) &&
          package_info->get_database_id() != OB_SYS_DATABASE_ID) {
        if (OB_FAIL(package_array_.push_back(package_info))) {
          SERVER_LOG(WARN, "fail to push back package info", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObAllVirtualDbaSource::get_routine_arrays()
{
  int ret = OB_SUCCESS;
  routine_array_.reset();

  ObArray<const ObRoutineInfo *> routine_infos;
  if (OB_FAIL(schema_guard_->get_routine_infos_in_tenant(tenant_id_, routine_infos))) {
    SERVER_LOG(WARN, "fail to get routine infos", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < routine_infos.count(); ++i) {
      const ObRoutineInfo *routine_info = routine_infos.at(i);
      if (OB_NOT_NULL(routine_info) &&
          routine_info->get_database_id() != OB_SYS_DATABASE_ID &&
          routine_info->get_routine_type() != ROUTINE_PACKAGE_TYPE &&
          routine_info->get_routine_type() != ROUTINE_UDT_TYPE) {
        if (OB_FAIL(routine_array_.push_back(routine_info))) {
          SERVER_LOG(WARN, "fail to push back routine info", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObAllVirtualDbaSource::get_trigger_arrays()
{
  int ret = OB_SUCCESS;
  trigger_array_.reset();

  ObArray<const ObTriggerInfo *> trigger_infos;
  if (OB_FAIL(schema_guard_->get_trigger_infos_in_tenant(tenant_id_, trigger_infos))) {
    SERVER_LOG(WARN, "fail to get trigger infos", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < trigger_infos.count(); ++i) {
      const ObTriggerInfo *trigger_info = trigger_infos.at(i);
      if (OB_NOT_NULL(trigger_info) &&
          trigger_info->get_database_id() != OB_SYS_DATABASE_ID) {
        if (OB_FAIL(trigger_array_.push_back(trigger_info))) {
          SERVER_LOG(WARN, "fail to push back trigger info", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObAllVirtualDbaSource::get_udt_arrays()
{
  int ret = OB_SUCCESS;
  udt_array_.reset();

  ObArray<const ObUDTTypeInfo *> udt_infos;
  if (OB_FAIL(schema_guard_->get_udt_infos_in_tenant(tenant_id_, udt_infos))) {
    SERVER_LOG(WARN, "fail to get udt infos", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < udt_infos.count(); ++i) {
      const ObUDTTypeInfo *udt_info = udt_infos.at(i);
      if (OB_NOT_NULL(udt_info) &&
          udt_info->get_database_id() != OB_SYS_DATABASE_ID) {

        const common::ObIArray<share::schema::ObUDTObjectType*> &object_type_infos = udt_info->get_object_type_infos();
        bool has_any_object = false;
        for (int64_t j = 0; j < object_type_infos.count() && !has_any_object; ++j) {
          const share::schema::ObUDTObjectType *object_info = object_type_infos.at(j);
          if (OB_NOT_NULL(object_info)) {
            has_any_object = true;
          }
        }

        if (has_any_object) {
          if (OB_FAIL(udt_array_.push_back(udt_info))) {
            SERVER_LOG(WARN, "fail to push back udt info", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObAllVirtualDbaSource::get_system_package_arrays()
{
  int ret = OB_SUCCESS;
  ObArray<const ObPackageInfo*> sys_packages;
  if (OB_FAIL(schema_guard_->get_package_infos_in_tenant(OB_SYS_TENANT_ID, sys_packages))) {
    SERVER_LOG(WARN, "fail to get package infos in sys tenant", K(ret), K(OB_SYS_TENANT_ID));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < sys_packages.count(); ++i) {
      const ObPackageInfo *package_info = sys_packages.at(i);
      if (OB_NOT_NULL(package_info)) {
        if (OB_FAIL(package_array_.push_back(package_info))) {
          SERVER_LOG(WARN, "fail to push system package info", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObAllVirtualDbaSource::get_system_routine_arrays()
{
  int ret = OB_SUCCESS;
  ObArray<const ObRoutineInfo*> sys_routines;
  if (OB_FAIL(schema_guard_->get_routine_infos_in_tenant(OB_SYS_TENANT_ID, sys_routines))) {
    SERVER_LOG(WARN, "fail to get routine infos in sys tenant", K(ret), K(OB_SYS_TENANT_ID));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < sys_routines.count(); ++i) {
      const ObRoutineInfo *routine_info = sys_routines.at(i);
      if (OB_NOT_NULL(routine_info) &&
          routine_info->get_routine_type() != ROUTINE_PACKAGE_TYPE &&
          routine_info->get_routine_type() != ROUTINE_UDT_TYPE) {
        if (OB_FAIL(routine_array_.push_back(routine_info))) {
          SERVER_LOG(WARN, "fail to push system routine info", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObAllVirtualDbaSource::get_system_trigger_arrays()
{
  int ret = OB_SUCCESS;
  ObArray<const ObTriggerInfo*> sys_triggers;
  if (OB_FAIL(schema_guard_->get_trigger_infos_in_tenant(OB_SYS_TENANT_ID, sys_triggers))) {
    SERVER_LOG(WARN, "fail to get trigger infos in sys tenant", K(ret), K(OB_SYS_TENANT_ID));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < sys_triggers.count(); ++i) {
      const ObTriggerInfo *trigger_info = sys_triggers.at(i);
      if (OB_NOT_NULL(trigger_info)) {
        if (OB_FAIL(trigger_array_.push_back(trigger_info))) {
          SERVER_LOG(WARN, "fail to push system trigger info", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObAllVirtualDbaSource::get_system_udt_arrays()
{
  int ret = OB_SUCCESS;
  ObArray<const ObUDTTypeInfo*> sys_udts;
  if (OB_FAIL(schema_guard_->get_udt_infos_in_tenant(OB_SYS_TENANT_ID, sys_udts))) {
    SERVER_LOG(WARN, "fail to get udt infos in sys tenant", K(ret), K(OB_SYS_TENANT_ID));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < sys_udts.count(); ++i) {
      const ObUDTTypeInfo *udt_info = sys_udts.at(i);
      if (OB_NOT_NULL(udt_info)) {
        const common::ObIArray<share::schema::ObUDTObjectType*> &object_type_infos = udt_info->get_object_type_infos();
        bool has_any_object = false;
        for (int64_t j = 0; j < object_type_infos.count() && !has_any_object; ++j) {
          const share::schema::ObUDTObjectType *object_info = object_type_infos.at(j);
          if (OB_NOT_NULL(object_info)) {
            has_any_object = true;
          }
        }

        if (has_any_object) {
          if (OB_FAIL(udt_array_.push_back(udt_info))) {
            SERVER_LOG(WARN, "fail to push system udt info", K(ret));
          }
        }
      }
    }
  }
  return ret;
}



bool ObAllVirtualDbaSource::move_to_next_object()
{
  if (iter_state_.current_type_ == SOURCE_TYPE) {
    const ObUDTTypeInfo *udt_info = nullptr;
    if (iter_state_.object_idx_ < udt_array_.count()) {
      udt_info = udt_array_.at(iter_state_.object_idx_);
    }

    if (OB_NOT_NULL(udt_info)) {
      const common::ObIArray<share::schema::ObUDTObjectType*> &object_type_infos = udt_info->get_object_type_infos();
      iter_state_.udt_object_type_idx_++;

      if (iter_state_.udt_object_type_idx_ >= object_type_infos.count()) {
        iter_state_.object_idx_++;
        iter_state_.udt_object_type_idx_ = 0;
      }
    } else {
      iter_state_.object_idx_++;
      iter_state_.udt_object_type_idx_ = 0;
    }
  } else {
    iter_state_.object_idx_++;
  }

  iter_state_.line_idx_ = 0;
  iter_state_.lines_.reset();

  return is_current_object_array_exhausted();
}

bool ObAllVirtualDbaSource::move_to_next_object_type()
{
  iter_state_.current_type_ = static_cast<SourceObjectType>(iter_state_.current_type_ + 1);
  iter_state_.object_idx_ = 0;
  iter_state_.udt_object_type_idx_ = 0;
  iter_state_.line_idx_ = 0;
  iter_state_.lines_.reset();

  return (iter_state_.current_type_ >= MAX_SOURCE_TYPE);
}

bool ObAllVirtualDbaSource::is_current_object_array_exhausted()
{
  switch (iter_state_.current_type_) {
    case SOURCE_PACKAGE:
      return (iter_state_.object_idx_ >= package_array_.count());
    case SOURCE_ROUTINE:
      return (iter_state_.object_idx_ >= routine_array_.count());
    case SOURCE_TRIGGER:
      return (iter_state_.object_idx_ >= trigger_array_.count());
    case SOURCE_TYPE:
      return (iter_state_.object_idx_ >= udt_array_.count());
    default:
      return true;
  }
}

}
}
