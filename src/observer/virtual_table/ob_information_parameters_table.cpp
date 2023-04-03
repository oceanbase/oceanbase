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

#include "observer/virtual_table/ob_information_parameters_table.h"

#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_schema_printer.h"
#include "common/sql_mode/ob_sql_mode_utils.h"
#include "sql/session/ob_sql_session_info.h"

using namespace oceanbase::common;
using namespace oceanbase::share::schema;
namespace oceanbase
{
namespace observer
{

ObInformationParametersTable::ObInformationParametersTable()
    : ObVirtualTableScannerIterator(),
      tenant_id_(OB_INVALID_ID)
{
}

ObInformationParametersTable::~ObInformationParametersTable()
{
}

void ObInformationParametersTable::reset()
{
  tenant_id_ = OB_INVALID_ID;
  ObVirtualTableScannerIterator::reset();
}

int ObInformationParametersTable::fill_row_cells(const ObRoutineInfo *routine_info, const ObRoutineParam* param_info, ObObj*& cells)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(routine_info) || OB_ISNULL(param_info) || OB_ISNULL(cells) || OB_ISNULL(session_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "null parameter", K(routine_info), K(param_info), K(cells), K(session_), K(ret));
  } else {
    const common::ObDataType &param_type = param_info->get_param_type();
    const ObLengthSemantics default_length_semantics = session_->get_local_nls_length_semantics();

    for (int64_t col_idx = 0; OB_SUCC(ret) && col_idx < output_column_ids_.count(); ++col_idx) {
      const uint64_t col_id = output_column_ids_.at(col_idx);
      switch (col_id) {
        case (SPECIFIC_SCHEMA): {
          const ObDatabaseSchema *db_schema = NULL;
          if (OB_FAIL(schema_guard_->get_database_schema(tenant_id_, routine_info->get_database_id(), db_schema))) {
            SERVER_LOG(WARN, "Failed to get database schema", K_(tenant_id), K(routine_info->get_database_id()), K(ret));
          } else if (OB_ISNULL(db_schema)) {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "Database schema should not be NULL", K(ret));
          } else {
            cells[col_idx].set_varchar(db_schema->get_database_name());
            cells[col_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        }
        case (PARAMETER_MODE): {
          if (param_info->is_ret_param()) {
            cells[col_idx].set_varchar("NULL");
            cells[col_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          } else {
            int64_t flags = param_info->get_mode();
            switch (flags) {
              case ObRoutineParamInOut::SP_PARAM_IN: {
                cells[col_idx].set_varchar("IN");
                cells[col_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
                break;
              }
              case ObRoutineParamInOut::SP_PARAM_OUT: {
                cells[col_idx].set_varchar("OUT");
                cells[col_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
                break;
              }
              case ObRoutineParamInOut::SP_PARAM_INOUT: {
                cells[col_idx].set_varchar("INOUT");
                cells[col_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
                break;
              }
              default: {
                ret = OB_ERR_UNEXPECTED;
                SERVER_LOG(WARN, "unrecongnize routine parameter flags", K(ret), K(flags));
              }
            }
          }
          break;
        }
        case (DATA_TYPE): {
          char *data_type_str = static_cast<char *>(allocator_->alloc(OB_MAX_SYS_PARAM_NAME_LENGTH));
          if (OB_ISNULL(data_type_str)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            SERVER_LOG(WARN, "memory is not enough", K(ret));
          } else if (OB_FAIL(ob_sql_type_str(data_type_str,
                                           OB_MAX_SYS_PARAM_NAME_LENGTH,
                                           param_type.get_obj_type(),
                                           param_type.get_collation_type()))) {
            SERVER_LOG(WARN, "fail to get data type str", K(ret), K(param_type.get_obj_type()));
          } else {
            ObString type_val(OB_MAX_SYS_PARAM_NAME_LENGTH, static_cast<int32_t>(strlen(data_type_str)), data_type_str);
            cells[col_idx].set_varchar(type_val);
            cells[col_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        }
        case (CHARACTER_MAXIMUM_LENGTH): {
          if(common::ObStringTC == param_type.get_type_class()
             || common::ObTextTC == param_type.get_type_class()) {
            cells[col_idx].set_uint64(static_cast<uint64_t>(param_type.get_length()));
          } else {
            cells[col_idx].set_null();
          }
          break;
        }
        case (CHARACTER_OCTET_LENGTH): {
          if((common::ObStringTC == param_type.get_type_class() || common::ObTextTC == param_type.get_type_class())
             && param_type.get_charset_type() != CHARSET_ANY) {
            ObCollationType coll = param_type.get_collation_type();
            int64_t mbmaxlen = 0;
            if (OB_FAIL(ObCharset::get_mbmaxlen_by_coll(coll, mbmaxlen))) {
              SERVER_LOG(WARN, "failed to get mbmaxlen", K(ret), K(coll));
            } else {
              cells[col_idx].set_uint64(static_cast<uint64_t>(
                        mbmaxlen * param_type.get_length()));
            }
          } else {
            cells[col_idx].set_null();
          }
          break;
        }
        case (NUMERIC_PRECISION): {
          if(common::ObNumberTC == param_type.get_type_class()
             || common::ObIntTC == param_type.get_type_class()) {
            cells[col_idx].set_uint64(static_cast<uint64_t>(param_type.get_precision()));
          } else {
            cells[col_idx].set_null();
          }
          break;
        }
        case (NUMERIC_SCALE): {
          if(common::ObNumberTC == param_type.get_type_class()) {
            cells[col_idx].set_uint64(static_cast<uint64_t>(param_type.get_scale()));
          } else if(common::ObIntTC == param_type.get_type_class()
                    || common::ObFloatTC == param_type.get_type_class()) {
            cells[col_idx].set_uint64(0);
          } else {
            cells[col_idx].set_null();
          }
          break;
        }
        case (DATETIME_PRECISION): {
          if(common::ObDateTimeTC == param_type.get_type_class()
             || ObTimeTC == param_type.get_type_class()) {
            cells[col_idx].set_uint64(static_cast<uint64_t>(param_type.get_scale()));
          } else {
            cells[col_idx].set_null();
          }
          break;
        }
        case (DTD_IDENTIFIER): {
          int16_t precision_or_length_semantics = param_type.get_precision();
          if (lib::is_oracle_mode() && param_type.get_meta_type().is_varchar_or_char() && precision_or_length_semantics == default_length_semantics) {
            precision_or_length_semantics = LS_DEFAULT;
          }
          int64_t pos = 0;
          char *column_type_str = static_cast<char *>(allocator_->alloc(OB_MAX_SYS_PARAM_NAME_LENGTH));
          if (OB_ISNULL(column_type_str)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            SERVER_LOG(WARN, "memory is not enough", K(ret));
          } else if (OB_FAIL(ob_sql_type_str(column_type_str,
                                           OB_MAX_SYS_PARAM_NAME_LENGTH, pos,
                                           param_type.get_obj_type(),
                                           param_type.get_length(),
                                           precision_or_length_semantics,
                                           param_type.get_scale(),
                                           param_type.get_collation_type()))) {
            SERVER_LOG(WARN,"fail to get column type str",K(ret), K(param_type.get_obj_type()));
          } else {
            ObString type_val(OB_MAX_SYS_PARAM_NAME_LENGTH, static_cast<int32_t>(strlen(column_type_str)),column_type_str);
            cells[col_idx].set_varchar(type_val);
            cells[col_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        }

#define COLUMN_SET_WITH_TYPE(COL_NAME, TYPE, VALUE) \
  case (COL_NAME): {    \
    cells[col_idx].set_##TYPE(VALUE); \
    cells[col_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset())); \
    break;\
  }
        COLUMN_SET_WITH_TYPE(SPECIFIC_CATALOG, varchar, "def");
        COLUMN_SET_WITH_TYPE(SPECIFIC_NAME, varchar, routine_info->get_routine_name());
        COLUMN_SET_WITH_TYPE(ORDINAL_POSITION, int, param_info->get_param_position());
        COLUMN_SET_WITH_TYPE(PARAMETER_NAME, varchar, param_info->get_param_name());
        COLUMN_SET_WITH_TYPE(CHARACTER_SET_NAME, varchar, common::ObCharset::charset_name(param_type.get_charset_type()));
        COLUMN_SET_WITH_TYPE(COLLATION_NAME, varchar, common::ObCharset::collation_name(param_type.get_collation_type()));
        COLUMN_SET_WITH_TYPE(ROUTINE_TYPE, varchar, (ROUTINE_PROCEDURE_TYPE == routine_info->get_routine_type()) ? "PROCEDURE" : "FUNCTION");

#undef COLUMN_SET_WITH_TYPE

        default: {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "Column id unexpected", K(col_id), K(ret));
        }
      } // end of case   
    } // end of for
  } // end of else
  return ret;
}

int ObInformationParametersTable::inner_get_next_row(common::ObNewRow *&row)
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
      ObObj *cells = NULL;
      if (OB_ISNULL(cells = cur_row_.cells_)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(ERROR, "cur row cell is NULL", K(ret));
      } else {
        ObArray<const ObRoutineInfo *> routine_array;
        if (OB_FAIL(schema_guard_->get_routine_infos_in_tenant(tenant_id_, routine_array))) {
          SERVER_LOG(WARN, "Get routine info with tenant id error", K(ret));
        } else {
          const ObRoutineInfo *routine_info = NULL;
          const ObRoutineParam *param_info = NULL;
          for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < routine_array.count(); ++row_idx) {
            if (OB_ISNULL(routine_info = routine_array.at(row_idx))) {
              ret = OB_ERR_UNEXPECTED;
              SERVER_LOG(WARN, "User info should not be NULL", K(ret));
            } else {
              const ObIArray<ObRoutineParam*> &params = routine_info->get_routine_params();
              for (int64_t param_idx = 0; OB_SUCC(ret) && param_idx < params.count(); ++param_idx) {
                if (OB_ISNULL(param_info = params.at(param_idx))) {
                  ret = OB_ERR_UNEXPECTED;
                  SERVER_LOG(WARN, "Parameter info should not be NULL", K(ret));
                } else if (OB_FAIL(fill_row_cells(routine_info, param_info, cur_row_.cells_))) {
                  SERVER_LOG(WARN, "fail to fill current row", K(ret));
                } else if (OB_FAIL(scanner_.add_row(cur_row_))) {
                  SERVER_LOG(WARN, "fail to add row", K(ret), K(cur_row_));
                }
              } // end of for parameters count
            } //end of else
          } //end of for routine array count
        } // end of else
      } // end of else
      if (OB_SUCC(ret)) {
        scanner_it_ = scanner_.begin();
        start_to_read_ = true;
      }
    }
    if (OB_SUCC(ret)) {
      if (start_to_read_) {
        if (OB_SUCCESS != (ret = scanner_it_.get_next_row(cur_row_))) {
          if (OB_ITER_END != ret) {
            SERVER_LOG(WARN, "fail to get next row", K(ret));
          }
        } else {
          row = &cur_row_;
        }
      } else {
        //do nothing
      }
    }
  }
  return ret;
}

}
}


