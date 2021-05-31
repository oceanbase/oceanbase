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

#include "observer/virtual_table/ob_mysql_proc_table.h"

#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_schema_printer.h"
#include "common/sql_mode/ob_sql_mode_utils.h"
#include "sql/session/ob_sql_session_info.h"

using namespace oceanbase::common;
using namespace oceanbase::share::schema;
namespace oceanbase {
namespace observer {

ObMySQLProcTable::ObMySQLProcTable() : ObVirtualTableScannerIterator(), tenant_id_(OB_INVALID_ID)
{}

ObMySQLProcTable::~ObMySQLProcTable()
{}

void ObMySQLProcTable::reset()
{
  tenant_id_ = OB_INVALID_ID;
  ObVirtualTableScannerIterator::reset();
}

int ObMySQLProcTable::inner_get_next_row(common::ObNewRow*& row)
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
      ObObj* cells = NULL;
      if (OB_ISNULL(cells = cur_row_.cells_)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(ERROR, "cur row cell is NULL", K(ret));
      } else {
        //         ObArray<const ObRoutineInfo *> routine_array;
        //         if (OB_FAIL(schema_guard_->get_routine_infos_in_tenant(tenant_id_, routine_array))) {
        //           SERVER_LOG(WARN, "Get user info with tenant id error", K(ret));
        //         } else {
        //           const ObRoutineInfo *routine_info = NULL;
        //           sql::ObExecEnv exec_env;
        //           for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < routine_array.count(); ++row_idx) {
        //             exec_env.reset();
        //             if (OB_ISNULL(routine_info = routine_array.at(row_idx))) {
        //               ret = OB_ERR_UNEXPECTED;
        //               SERVER_LOG(WARN, "User info should not be NULL", K(ret));
        //             } else if (ROUTINE_PACKAGE_TYPE == routine_info->get_routine_type()) {
        //               // mysql compatible view, ignore oracle system package routine
        //               continue;
        //             } else if (OB_FAIL(exec_env.init(routine_info->get_exec_env()))) {
        //               SERVER_LOG(ERROR, "fail to load exec env", K(ret));
        //             } else {
        //               const ObUserInfo *user_info = NULL;
        //               ObString user_name;
        //               if (OB_FAIL(schema_guard_->get_user_info(routine_info->get_owner_id(), user_info))) {
        //                 SERVER_LOG(WARN, "Failed to get database schema", K(routine_info->get_owner_id()), K(ret));
        //               } else if (OB_ISNULL(user_info)) {
        //                 ret = OB_ERR_UNEXPECTED;
        //                 SERVER_LOG(WARN, "Database schema should not be NULL", K(ret));
        //               } else {
        //                 const int64_t USERNAME_AUX_LEN = 6;// "''@''" + '\0'
        //                 int64_t pos = 0;
        //                 int64_t buf_size = user_info->get_user_name_str().length() +
        //                 user_info->get_host_name_str().length() + USERNAME_AUX_LEN;// "''@''" char
        //                 username_buf[buf_size]; memset(username_buf, 0, sizeof(username_buf)); if
        //                 (OB_FAIL(databuff_printf(username_buf, sizeof(username_buf),
        //                     pos, "'%s'@'%s'", user_info->get_user_name(), user_info->get_host_name()))) {
        //                   SERVER_LOG(WARN, "databuff_printf failed", K(ret), K(buf_size), K(pos),
        //                       "user_name", user_info->get_user_name());
        //                 } else {
        //                   user_name.assign_ptr(username_buf, static_cast<int32_t>(buf_size - 1));
        //                 }

        //                 common::ObArenaAllocator local_allocator;
        //                 for (int64_t col_idx = 0; OB_SUCC(ret) && col_idx < output_column_ids_.count(); ++col_idx) {
        //                   const uint64_t col_id = output_column_ids_.at(col_idx);
        //                   switch (col_id) {
        //                     case (DB): {
        //                       const ObDatabaseSchema *db_schema = NULL;
        //                       if (OB_FAIL(schema_guard_->get_database_schema(routine_info->get_database_id(),
        //                       db_schema))) {
        //                         SERVER_LOG(WARN, "Failed to get database schema", K(routine_info->get_database_id()),
        //                         K(ret));
        //                       } else if (OB_ISNULL(db_schema)) {
        //                         ret = OB_ERR_UNEXPECTED;
        //                         SERVER_LOG(WARN, "Database schema should not be NULL", K(ret));
        //                       } else {
        //                         cells[col_idx].set_varchar(db_schema->get_database_name());
        //                         cells[col_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        //                       }
        //                       break;
        //                     }
        //                     case (DEFINER): {
        //                       cells[col_idx].set_varchar(user_name);
        //                       cells[col_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        //                       break;
        //                     }
        //                     case (PARAM_LIST): {
        //                       char *param_list_buf = NULL;
        //                       int64_t param_list_buf_size = OB_MAX_VARCHAR_LENGTH;
        //                       if (OB_UNLIKELY(NULL == (param_list_buf
        //                           = static_cast<char *>(local_allocator.alloc(param_list_buf_size))))) {
        //                         ret = OB_ALLOCATE_MEMORY_FAILED;
        //                         SERVER_LOG(ERROR, "fail to alloc param_list_buf", K(ret));
        //                       } else {
        //                         ObSchemaPrinter schema_printer(*schema_guard_);
        //                         int64_t pos = 0;
        //                         if (OB_FAIL(schema_printer.print_routine_definition_param(*routine_info,
        //                                                                                   NULL,
        //                                                                                   param_list_buf,
        //                                                                                   OB_MAX_VARCHAR_LENGTH,
        //                                                                                   pos,
        //                                                                                   TZ_INFO(session_)))) {
        //                           SERVER_LOG(WARN, "Generate table definition failed");
        //                         } else {
        //                           ObString value_str(static_cast<int32_t>(pos), static_cast<int32_t>(pos),
        //                           param_list_buf); cells[col_idx].set_varchar(value_str);
        //                           cells[col_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        //                         }
        //                       }
        //                       break;
        //                     }
        //                     case (RETURNS): {
        //                       char *returns_buf = NULL;
        //                       int64_t returns_buf_size = OB_MAX_VARCHAR_LENGTH;
        //                       int64_t pos = 0;
        //                       if (OB_UNLIKELY(NULL == (returns_buf = static_cast<char
        //                       *>(local_allocator.alloc(returns_buf_size))))) {
        //                         ret = OB_ALLOCATE_MEMORY_FAILED;
        //                         SERVER_LOG(ERROR, "fail to alloc returns_buf", K(ret));
        //                       } else {
        //                         if (routine_info->is_function()) {
        //                           if (OB_FAIL(ob_sql_type_str(returns_buf,
        //                                                       returns_buf_size,
        //                                                       pos,
        //                                                       routine_info->get_ret_type()->get_obj_type(),
        //                                                       routine_info->get_ret_type()->get_length(),
        //                                                       routine_info->get_ret_type()->get_precision(),
        //                                                       routine_info->get_ret_type()->get_scale(),
        //                                                       routine_info->get_ret_type()->get_collation_type()))) {
        //                             SHARE_SCHEMA_LOG(WARN, "fail to get data type str",
        //                             KPC(routine_info->get_ret_type()));
        //                           }
        //                         } else {
        //                           ObDataType ret_type;
        //                           if (OB_FAIL(ob_sql_type_str(returns_buf,
        //                                                       returns_buf_size,
        //                                                       pos,
        //                                                       ret_type.get_obj_type(),
        //                                                       ret_type.get_length(),
        //                                                       ret_type.get_precision(),
        //                                                       ret_type.get_scale(),
        //                                                       ret_type.get_collation_type()))) {
        //                             SHARE_SCHEMA_LOG(WARN, "fail to get data type str", K(ret_type));
        //                           }
        //                         }
        //                         if (OB_SUCC(ret)) {
        //                           ObString value_str(static_cast<int32_t>(pos), static_cast<int32_t>(pos),
        //                           returns_buf); cells[col_idx].set_varchar(value_str);
        //                           cells[col_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        //                         }
        //                       }
        //                       break;
        //                     }
        //                     case (SQL_MODE): {
        //                       ObObj int_value;
        //                       int_value.set_int(exec_env.get_sql_mode());
        //                       if (OB_FAIL(ob_sql_mode_to_str(int_value, cells[col_idx], allocator_))) {
        //                         SERVER_LOG(ERROR, "fail to convert sqlmode to string", K(int_value), K(ret));
        //                       } else {
        //                         cells[col_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        //                       }
        //                       break;
        //                     }
        //                     case (CHARACTER_SET_CLIENT): {
        //                       cells[col_idx].set_varchar(ObCharset::charset_name(exec_env.get_charset_client()));
        //                       cells[col_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        //                       break;
        //                     }
        //                     case (COLLATION_CONNECTION): {
        //                       cells[col_idx].set_varchar(ObCharset::collation_name(exec_env.get_collation_connection()));
        //                       cells[col_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        //                       break;
        //                     }
        //                     case (DB_COLLATION): {
        //                       cells[col_idx].set_varchar(ObCharset::collation_name(exec_env.get_collation_database()));
        //                       cells[col_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        //                       break;
        //                     }

        // #define COLUMN_SET_WITH_TYPE(COL_NAME, TYPE, VALUE) \
//   case (COL_NAME): {    \
//     cells[col_idx].set_##TYPE(VALUE); \
//     cells[col_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset())); \
//     break;\
//   }

        //                     COLUMN_SET_WITH_TYPE(NAME, varchar, routine_info->get_routine_name())
        //                     COLUMN_SET_WITH_TYPE(TYPE, varchar, ROUTINE_PROCEDURE_TYPE ==
        //                     routine_info->get_routine_type() ? "PROCEDURE" : "FUNCTION")
        //                     COLUMN_SET_WITH_TYPE(SPECIFIC_NAME, varchar, routine_info->get_routine_name())
        //                     COLUMN_SET_WITH_TYPE(LANGUAGE, varchar, "SQL")
        //                     COLUMN_SET_WITH_TYPE(SQL_DATA_ACCESS, varchar, "CONTAINS_SQL")
        //                     COLUMN_SET_WITH_TYPE(IS_DETERMINISTIC, varchar, "NO")
        //                     COLUMN_SET_WITH_TYPE(SECURITY_TYPE, varchar, "DEFINER")
        //                     COLUMN_SET_WITH_TYPE(BODY, varchar, routine_info->get_routine_body())
        //                     COLUMN_SET_WITH_TYPE(CREATED, timestamp, OB_INVALID_TIMESTAMP)
        //                     COLUMN_SET_WITH_TYPE(MODIFIED, timestamp, OB_INVALID_TIMESTAMP)
        //                     COLUMN_SET_WITH_TYPE(COMMENT, varchar, routine_info->get_comment())
        //                     COLUMN_SET_WITH_TYPE(BODY_UTF8, varchar, routine_info->get_routine_body())

        // #undef COLUMN_SET_WITH_TYPE

        //                     default: {
        //                       ret = OB_ERR_UNEXPECTED;
        //                       SERVER_LOG(WARN, "Column id unexpected", K(col_id), K(ret));
        //                     }
        //                   } //end of case
        //                 } //end of for col_count
        //                 if (OB_SUCC(ret)) {
        //                   if (OB_FAIL(scanner_.add_row(cur_row_))) {
        //                     SERVER_LOG(WARN, "fail to add row", K(ret), K(cur_row_));
        //                   }
        //                 }
        //               } //end of else
        //             } //end of else
        //           } //end of for user array count
        //         }
      }
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
        // do nothing
      }
    }
  }
  return ret;
}

}  // namespace observer
}  // namespace oceanbase
