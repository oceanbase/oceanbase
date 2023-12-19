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
#include "observer/ob_server_struct.h"

using namespace oceanbase::common;
using namespace oceanbase::share::schema;
namespace oceanbase
{
namespace observer
{

ObMySQLProcTable::ObMySQLProcTable()
    : ObVirtualTableScannerIterator(),
      tenant_id_(OB_INVALID_ID)
{
}

ObMySQLProcTable::~ObMySQLProcTable()
{
}

void ObMySQLProcTable::reset()
{
  tenant_id_ = OB_INVALID_ID;
  ObVirtualTableScannerIterator::reset();
}

int ObMySQLProcTable::inner_get_next_row(common::ObNewRow *&row)
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
      }  else {
        ObArray<const ObRoutineInfo *> routine_array;
        if (OB_FAIL(schema_guard_->get_routine_infos_in_tenant(tenant_id_, routine_array))) {
          SERVER_LOG(WARN, "Get user info with tenant id error", K(ret));
        } else {
          const ObRoutineInfo *routine_info = NULL;
          sql::ObExecEnv exec_env;
          for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < routine_array.count(); ++row_idx) {
            exec_env.reset();
            if (OB_ISNULL(routine_info = routine_array.at(row_idx))) {
              ret = OB_ERR_UNEXPECTED;
              SERVER_LOG(WARN, "User info should not be NULL", K(ret));
            } else if (ROUTINE_PACKAGE_TYPE == routine_info->get_routine_type()
                       || ROUTINE_UDT_TYPE == routine_info->get_routine_type()) {
              // mysql compatible view, ignore oracle system package/udt routine
              continue;
            } else if (OB_FAIL(exec_env.init(routine_info->get_exec_env()))) {
              SERVER_LOG(ERROR, "fail to load exec env", K(ret));
            } else {
              ObString user_name;
              const int64_t USERNAME_AUX_LEN = 6;// "''@''" + '\0'
              int64_t pos = 0;
              ObString priv_user = routine_info->get_priv_user();
              ObString routine_user_name = priv_user.split_on('@');
              ObString routine_host_name = priv_user;
              int64_t buf_size = routine_user_name.length()
                                  + routine_host_name.length()
                                  + USERNAME_AUX_LEN;// "''@''"
              char *username_buf = static_cast<char *>(allocator_->alloc(buf_size));
              if (OB_ISNULL(username_buf)) {
                ret = OB_ERR_UNEXPECTED;
                SERVER_LOG(WARN, "alloc buf failed", K(ret));
              } else if (OB_FAIL(databuff_printf(username_buf, buf_size,
                                                  pos, "'%.*s'@'%.*s'",
                                                  routine_user_name.length(), routine_user_name.ptr(),
                                                  routine_host_name.length(), routine_host_name.ptr()))) {
                SERVER_LOG(WARN, "databuff_printf failed", K(ret), K(buf_size), K(pos),
                          "user_name", routine_info->get_priv_user());
              } else {
                user_name.assign_ptr(username_buf, static_cast<int32_t>(buf_size - 1));
              }

              common::ObArenaAllocator local_allocator;
              ParseNode *create_node = nullptr;

              if (OB_FAIL(ret)) {
                // do nothing
              } else if (routine_info->get_routine_body().prefix_match_ci("procedure")
                         || routine_info->get_routine_body().prefix_match_ci("function")) {
                if (OB_FAIL(extract_create_node_from_routine_info(
                              local_allocator, *routine_info, exec_env, create_node))) {
                  SERVER_LOG(WARN, "failed to extract create node from routine info",
                             K(ret), K(*routine_info), K(exec_env), K(create_node));
                }
              }

              for (int64_t col_idx = 0; OB_SUCC(ret) && col_idx < output_column_ids_.count(); ++col_idx) {
                const uint64_t col_id = output_column_ids_.at(col_idx);
                switch (col_id) {
                  case (DB): {
                    const ObDatabaseSchema *db_schema = NULL;
                    if (OB_FAIL(schema_guard_->get_database_schema(tenant_id_,
                        routine_info->get_database_id(), db_schema))) {
                      SERVER_LOG(WARN, "Failed to get database schema", K_(tenant_id),
                                  K(routine_info->get_database_id()), K(ret));
                    } else if (OB_ISNULL(db_schema)) {
                      ret = OB_ERR_UNEXPECTED;
                      SERVER_LOG(WARN, "Database schema should not be NULL", K(ret));
                    } else {
                      cells[col_idx].set_varchar(db_schema->get_database_name());
                      cells[col_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
                    }
                    break;
                  }
                  case (DEFINER): {
                    cells[col_idx].set_varchar(user_name);
                    cells[col_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
                    break;
                  }
                  case (PARAM_LIST): {
                    const ObColumnSchemaV2 *tmp_column_schema = NULL;
                    bool type_is_lob = true;
                    if (OB_ISNULL(table_schema_) ||
                          OB_ISNULL(
                              tmp_column_schema =
                                  table_schema_->get_column_schema(col_id))) {
                      ret = OB_ERR_UNEXPECTED;
                      SERVER_LOG(WARN, "table or column schema is null",
                                 KR(ret),
                                 KP(table_schema_),
                                 KP(tmp_column_schema));
                    } else {
                      type_is_lob = tmp_column_schema->get_meta_type().is_lob();
                    }

                    if (nullptr != create_node) {
                      if (T_SP_CREATE != create_node->type_ && T_SF_CREATE != create_node->type_ && OB_ISNULL(create_node->children_[2])) {
                        ret = OB_ERR_UNEXPECTED;
                        SERVER_LOG(WARN, "unexpected parse node type of routine body", K(create_node->type_));
                      } else {
                        ParseNode *param_node = create_node->children_[2];
                        ObString value_str;
                        if (param_node != nullptr) {
                          if (OB_FAIL(ob_write_string(
                                          *allocator_,
                                          ObString(min(OB_MAX_VARCHAR_LENGTH, param_node->str_len_),
                                          param_node->str_value_),
                                          value_str))) {
                            SERVER_LOG(WARN, "failed to ob_write_string",
                                       K(ret),
                                       K(param_node->str_len_),
                                       K(param_node->str_value_),
                                       K(value_str));
                          }
                        }
                        if (OB_FAIL(ret)) {
                          // do nothing
                        } else if (type_is_lob) {
                          cells[col_idx].set_lob_value(ObLongTextType,
                                                       value_str.ptr(),
                                                       value_str.length());
                          ObCollationType cs_type = tmp_column_schema->get_collation_type() == CS_TYPE_BINARY
                                                     ? CS_TYPE_BINARY  // when this column is longblob
                                                     : ObCharset::get_default_collation(ObCharset::get_default_charset()); // when this column is longtext
                          cells[col_idx].set_collation_type(cs_type);
                        } else {
                          cells[col_idx].set_varchar(value_str);
                          cells[col_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
                        }
                      }
                    } else {
                      char *param_list_buf = NULL;
                      int64_t param_list_buf_size = OB_MAX_VARCHAR_LENGTH;
                      if (OB_UNLIKELY(NULL == (param_list_buf
                          = static_cast<char *>(allocator_->alloc(param_list_buf_size))))) {
                        ret = OB_ALLOCATE_MEMORY_FAILED;
                        SERVER_LOG(WARN, "fail to alloc param_list_buf", K(ret));
                      } else {
                        ObSchemaPrinter schema_printer(*schema_guard_);
                        int64_t pos = 0;
                        if (OB_FAIL(schema_printer.print_routine_definition_param_v1(*routine_info,
                                                                                  NULL,
                                                                                  param_list_buf,
                                                                                  OB_MAX_VARCHAR_LENGTH,
                                                                                  pos,
                                                                                  TZ_INFO(session_)))) {
                          SERVER_LOG(WARN, "Generate table definition failed");
                        } else {
                          ObString value_str(static_cast<int32_t>(pos), static_cast<int32_t>(pos), param_list_buf);

                          if (type_is_lob) {
                            cells[col_idx].set_lob_value(ObLongTextType,
                                                         value_str.ptr(),
                                                         value_str.length());
                            ObCollationType cs_type = tmp_column_schema->get_collation_type() == CS_TYPE_BINARY
                                                     ? CS_TYPE_BINARY  // when this column is longblob
                                                     : ObCharset::get_default_collation(ObCharset::get_default_charset()); // when this column is longtext
                            cells[col_idx].set_collation_type(cs_type);
                          } else {
                            cells[col_idx].set_varchar(value_str);
                            cells[col_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
                          }
                        }
                      }
                    }
                    break;
                  }
                  case (RETURNS): {
                    char *returns_buf = NULL;
                    int64_t returns_buf_size = OB_MAX_VARCHAR_LENGTH;
                    int64_t pos = 0;
                    const ObColumnSchemaV2 *tmp_column_schema = NULL;
                    bool type_is_lob = true;
                    if (OB_ISNULL(table_schema_) ||
                          OB_ISNULL(
                              tmp_column_schema =
                                  table_schema_->get_column_schema(col_id))) {
                      ret = OB_ERR_UNEXPECTED;
                      SERVER_LOG(WARN, "table or column schema is null",
                                 KR(ret),
                                 KP(table_schema_),
                                 KP(tmp_column_schema));
                    } else {
                      type_is_lob = tmp_column_schema->get_meta_type().is_lob();
                    }

                    if (OB_UNLIKELY(NULL == (returns_buf = static_cast<char *>(allocator_->alloc(returns_buf_size))))) {
                      ret = OB_ALLOCATE_MEMORY_FAILED;
                      SERVER_LOG(WARN, "fail to alloc returns_buf", K(ret));
                    } else {
                      if (routine_info->is_function()) {
                        if (OB_FAIL(ob_sql_type_str(returns_buf,
                                                    returns_buf_size,
                                                    pos,
                                                    routine_info->get_ret_type()->get_obj_type(),
                                                    routine_info->get_ret_type()->get_length(),
                                                    routine_info->get_ret_type()->get_precision(),
                                                    routine_info->get_ret_type()->get_scale(),
                                                    routine_info->get_ret_type()->get_collation_type()))) {
                          SHARE_SCHEMA_LOG(WARN, "fail to get data type str", KPC(routine_info->get_ret_type()));
                        }
                      } else {
                        // proc no returns, fill empty.
                      }
                      if (OB_SUCC(ret)) {
                        ObString value_str(static_cast<int32_t>(pos), static_cast<int32_t>(pos), returns_buf);

                        if (type_is_lob) {
                          cells[col_idx].set_lob_value(ObLongTextType,
                                                        value_str.ptr(),
                                                        value_str.length());
                          ObCollationType cs_type = tmp_column_schema->get_collation_type() == CS_TYPE_BINARY
                                                    ? CS_TYPE_BINARY  // when this column is longblob
                                                    : ObCharset::get_default_collation(ObCharset::get_default_charset()); // when this column is longtext
                          cells[col_idx].set_collation_type(cs_type);
                        } else {
                          cells[col_idx].set_varchar(value_str);
                          cells[col_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
                        }
                      }
                    }
                    break;
                  }
                  case (SQL_MODE): {
                    ObObj int_value;
                    int_value.set_int(exec_env.get_sql_mode());
                    if (OB_FAIL(ob_sql_mode_to_str(int_value, cells[col_idx], allocator_))) {
                      SERVER_LOG(ERROR, "fail to convert sqlmode to string", K(int_value), K(ret));
                    } else {
                      cells[col_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
                    }
                    break;
                  }
                  case (CHARACTER_SET_CLIENT): {
                    cells[col_idx].set_varchar(ObCharset::charset_name(exec_env.get_charset_client()));
                    cells[col_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
                    break;
                  }
                  case (COLLATION_CONNECTION): {
                    cells[col_idx].set_varchar(ObCharset::collation_name(exec_env.get_collation_connection()));
                    cells[col_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
                    break;
                  }
                  case (DB_COLLATION): {
                    cells[col_idx].set_varchar(ObCharset::collation_name(exec_env.get_collation_database()));
                    cells[col_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
                    break;
                  }
                  case (SQL_DATA_ACCESS): {
                    if (routine_info->is_no_sql()) {
                      cells[col_idx].set_varchar("NO_SQL");
                    } else if (routine_info->is_reads_sql_data()) {
                      cells[col_idx].set_varchar("READS_SQL_DATA");
                    } else if (routine_info->is_modifies_sql_data()) {
                      cells[col_idx].set_varchar("MODIFIES_SQL_DATA");
                    } else {
                      cells[col_idx].set_varchar("CONTAINS_SQL");
                    }
                    cells[col_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
                    break;
                  }
                  case (BODY):
                  case (BODY_UTF8): {
                    if (nullptr != create_node) {
                      ParseNode *body_node = nullptr;
                      if (T_SP_CREATE != create_node->type_ && T_SF_CREATE != create_node->type_) {
                        ret = OB_ERR_UNEXPECTED;
                        SERVER_LOG(WARN, "unexpected parse node type of routine body", K(create_node->type_));
                      } else if (FALSE_IT(body_node = create_node->type_ == T_SP_CREATE ? create_node->children_[4] : create_node->children_[5])) {
                        // do nothing
                      } else if (OB_ISNULL(body_node) || OB_ISNULL(body_node->raw_text_)) {
                        ret = OB_ERR_UNEXPECTED;
                        SERVER_LOG(WARN, "unexpected empty routine body", K(routine_info->get_routine_body()));
                      } else {
                        ObString value_str;
                        if (OB_FAIL(ob_write_string(*allocator_,
                                                    ObString(min(OB_MAX_VARCHAR_LENGTH, body_node->text_len_), body_node->raw_text_),
                                                    value_str))) {
                          SERVER_LOG(WARN, "failed to ob_write_string", K(ret), K(ObString(body_node->text_len_, body_node->raw_text_)));
                        } else {
                          cells[col_idx].set_varchar(value_str);
                          cells[col_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
                        }
                      }
                    } else {
                      const ObString &body = routine_info->get_routine_body();
                      cells[col_idx].set_varchar(body);
                      cells[col_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
                    }
                    break;
                  }
                  case (CREATED):
                  case (MODIFIED): {
                    int64_t routine_time = OB_INVALID_TIMESTAMP;
                    get_info_from_all_routine(col_id, routine_info, routine_time);
                    cells[col_idx].set_timestamp(routine_time);
                    cells[col_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
                    break;
                  }

#define COLUMN_SET_WITH_TYPE(COL_NAME, TYPE, VALUE) \
case (COL_NAME): {    \
  cells[col_idx].set_##TYPE(VALUE); \
  cells[col_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset())); \
  break;\
}

                  COLUMN_SET_WITH_TYPE(NAME, varchar, routine_info->get_routine_name())
                  COLUMN_SET_WITH_TYPE(TYPE, varchar, ROUTINE_PROCEDURE_TYPE == routine_info->get_routine_type() ? "PROCEDURE" : "FUNCTION")
                  COLUMN_SET_WITH_TYPE(SPECIFIC_NAME, varchar, routine_info->get_routine_name())
                  COLUMN_SET_WITH_TYPE(LANGUAGE, varchar, "SQL")
                  COLUMN_SET_WITH_TYPE(IS_DETERMINISTIC, varchar, routine_info->is_deterministic() ? "YES" : "NO")
                  COLUMN_SET_WITH_TYPE(SECURITY_TYPE, varchar, routine_info->is_invoker_right() ? "INVOKER" : "DEFINER")
                  COLUMN_SET_WITH_TYPE(COMMENT, varchar, routine_info->get_comment())

#undef COLUMN_SET_WITH_TYPE

                  default: {
                    ret = OB_ERR_UNEXPECTED;
                    SERVER_LOG(WARN, "Column id unexpected", K(col_id), K(ret));
                  }
                } //end of case
              } //end of for col_count
              if (OB_SUCC(ret)) {
                if (OB_FAIL(scanner_.add_row(cur_row_))) {
                  SERVER_LOG(WARN, "fail to add row", K(ret), K(cur_row_));
                }
              }
            } //end of else
          } //end of for user array count
        }
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
        //do nothing
      }
    }
  }
  return ret;
}

int ObMySQLProcTable::extract_create_node_from_routine_info(ObIAllocator &alloc, const ObRoutineInfo &routine_info, const sql::ObExecEnv &exec_env, ParseNode *&create_node) {
  int ret = OB_SUCCESS;

  ParseResult parse_result;
  ObString routine_stmt;
  pl::ObPLParser parser(alloc, sql::ObCharsets4Parser(), exec_env.get_sql_mode());
  const ObString &routine_body = routine_info.get_routine_body();
  const char prefix[] = "CREATE\n";
  int64_t prefix_len = STRLEN(prefix);
  int64_t buf_sz = prefix_len + routine_body.length();
  char *stmt_buf = static_cast<char *>(alloc.alloc(buf_sz));
  if (OB_ISNULL(stmt_buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SERVER_LOG(WARN, "failed to allocate memory for routine body buffer",
               K(buf_sz));
  } else {
    MEMCPY(stmt_buf, prefix, prefix_len);
    MEMCPY(stmt_buf + prefix_len, routine_body.ptr(), routine_body.length());
    routine_stmt.assign_ptr(stmt_buf, buf_sz);
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(parser.parse(routine_stmt, routine_stmt, parse_result, true))) {
    SERVER_LOG(WARN, "failed to parse mysql routine body",
               K(ret), K(routine_info), K(routine_body));
  }

  if OB_SUCC(ret) {
    if (OB_NOT_NULL(parse_result.result_tree_) &&
        T_STMT_LIST == parse_result.result_tree_->type_ &&
        1 == parse_result.result_tree_->num_child_) {
      create_node = parse_result.result_tree_->children_[0];
    } else {
      create_node = nullptr;
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "unexpected parse node of mysql routine body", K(routine_info), K(routine_body), K(parse_result.result_tree_));
    }
  }

  return ret;
}

int ObMySQLProcTable::get_info_from_all_routine(const uint64_t col_id,
                                                const ObRoutineInfo *routine_info,
                                                int64_t &routine_time)
{
  int ret = OB_SUCCESS;
  common::ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
  if (OB_NOT_NULL(routine_info)) {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = NULL;
      const uint64_t exec_tenant_id = tenant_id_;
      ObString col_name = col_id == CREATED ? "GMT_CREATE" : "GMT_MODIFIED";
      const char *sql_str = "select %.*s from oceanbase.__all_routine where "
                            " database_id = %ld and package_id = %ld "
                            " and routine_id = %lu and subprogram_id = %ld";
      ObSqlString sql;
      if (OB_FAIL(sql.append_fmt(sql_str, col_name.length(), col_name.ptr(),
                                routine_info->get_database_id() & 0xFFFFFFFF,
                                routine_info->get_package_id(),
                                routine_info->get_routine_id() & 0xFFFFFFFF,
                                routine_info->get_subprogram_id()))) {
        SERVER_LOG(WARN, "fail to append sql", K(sql_str), K(routine_info->get_database_id()),
          K(routine_info->get_routine_id()), K(ret));
      } else if (OB_ISNULL(sql_proxy)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "data member is not init", K(ret));
      } else if (OB_FAIL(sql_proxy->read(res, exec_tenant_id, sql.ptr()))) {
        SERVER_LOG(WARN, "fail to read result", K(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "result set from read is NULL", K(ret));
      } else if (OB_SUCC(result->next())) {
        const int64_t col_idx = 0;
        const common::ObTimeZoneInfo *time_info = NULL;
        ret = result->get_timestamp(col_idx, time_info, routine_time);
      }

      if (OB_LIKELY(OB_ITER_END == ret)) {
        ret = OB_SUCCESS;
        SERVER_LOG(INFO, "get null info from all_routine", K(col_name));
      } else {
        SERVER_LOG(WARN, "fail to fill table statstistics", K(ret));
      }
    }
  }
  return ret;
}

}
}


