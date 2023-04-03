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

#include "observer/virtual_table/ob_information_triggers_table.h"
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
ObInfoSchemaTriggersTable::ObInfoSchemaTriggersTable()
    : ObVirtualTableScannerIterator(),
      tenant_id_(OB_INVALID_ID)
{

}

ObInfoSchemaTriggersTable::~ObInfoSchemaTriggersTable()
{

}

void ObInfoSchemaTriggersTable::reset()
{
  tenant_id_ = OB_INVALID_ID;
  ObVirtualTableScannerIterator::reset();
}

int ObInfoSchemaTriggersTable::inner_get_next_row(ObNewRow *&row)
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
        ObArray<const ObTriggerInfo *> tg_array;
        if (OB_FAIL(schema_guard_->get_trigger_infos_in_tenant(tenant_id_, tg_array))) {
          SERVER_LOG(WARN, "Get trigger info with tenant id error", K(ret));
        } else {
          const ObTriggerInfo *tg_info = NULL;
          sql::ObExecEnv exec_env;
          for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < tg_array.count(); ++row_idx) {
            exec_env.reset();
            if (OB_ISNULL(tg_info = tg_array.at(row_idx))) {
              ret = OB_ERR_UNEXPECTED;
              SERVER_LOG(WARN, "Trigger info should not be null", K(ret));
            } else if (tg_info->is_in_recyclebin()) {
              //在回收站中的trigger不需要展示
            } else if (OB_FAIL(exec_env.init(tg_info->get_package_exec_env()))) {
              SERVER_LOG(ERROR, "fail to load exec env", K(ret));
            } else {
              const ObUserInfo *user_info = NULL;
              ObString user_name;
              if (OB_FAIL(schema_guard_->get_user_info(tenant_id_, tg_info->get_owner_id(), user_info))) {
                SERVER_LOG(WARN, "Failed to get database schema", K_(tenant_id),
                           K(tg_info->get_owner_id()), K(ret));
              } else {
                if (OB_NOT_NULL(user_info)) {
                  // 这里兼容mysql,如果user存在,则给user_name赋值,如果user已经被删除,则user_name = ""
                  const int64_t USERNAME_AUX_LEN = 6;// "''@''" + '\0'
                  int64_t pos = 0;
                  int64_t buf_size = user_info->get_user_name_str().length()
                                     + user_info->get_host_name_str().length()
                                     + USERNAME_AUX_LEN;// "''@''"
                  char *username_buf = static_cast<char *>(allocator_->alloc(buf_size));
                  if (OB_ISNULL(username_buf)) {
                    ret = OB_ERR_UNEXPECTED;
                    SERVER_LOG(WARN, "alloc buf failed", K(ret));
                  } else if (OB_FAIL(databuff_printf(username_buf, buf_size,
                                                     pos, "'%s'@'%s'",
                                                     user_info->get_user_name(),
                                                     user_info->get_host_name()))) {
                    SERVER_LOG(WARN, "Databuff_printf failed", K(ret), K(buf_size), K(pos),
                               "user_name", user_info->get_user_name());
                  } else {
                    user_name.assign_ptr(username_buf, static_cast<int32_t>(buf_size - 1));
                  }
                }

                common::ObArenaAllocator local_allocator;
                for (int64_t col_idx = 0; OB_SUCC(ret) && col_idx < output_column_ids_.count();
                     ++col_idx) {
                  const uint64_t col_id = output_column_ids_.at(col_idx);
                  const ObTableSchema *table = NULL;
                  if (OB_FAIL(schema_guard_->get_table_schema(tenant_id_,
                                                              tg_info->get_base_object_id(),
                                                              table))) {
                    SERVER_LOG(WARN, "Failed to get table schema", K(tenant_id_),
                               K(tg_info->get_base_object_id()), K(ret));
                  } else if (OB_ISNULL(table)) {
                    ret = OB_ERR_UNEXPECTED;
                    SERVER_LOG(WARN, "Table schema should not be NULL", K(ret));
                  }
                  if (OB_SUCC(ret)) {
                    switch (col_id) {
                      case TRIGGER_SCHEMA: {
                        const ObDatabaseSchema *db = NULL;
                        if (OB_FAIL(schema_guard_->get_database_schema(tenant_id_,
                            tg_info->get_database_id(), db))) {
                          SERVER_LOG(WARN, "Failed to get database schema",
                                     K_(tenant_id), K(tg_info->get_database_id()), K(ret));
                        } else if (OB_ISNULL(db)) {
                          ret = OB_ERR_UNEXPECTED;
                          SERVER_LOG(WARN, "Database schema should not be NULL", K(ret));
                        } else {
                          cells[col_idx].set_varchar(db->get_database_name());
                          cells[col_idx].set_collation_type(ObCharset::get_default_collation(
                                                            ObCharset::get_default_charset()));
                        }
                        break;
                      }
                      case EVENT_MANIPULATION: {
                        const uint64_t tg_event = tg_info->get_trigger_events();
                        cells[col_idx].set_collation_type(ObCharset::get_default_collation(
                                                            ObCharset::get_default_charset()));
                        if (ObDmlEventType::DE_INSERTING == tg_event) {
                           cells[col_idx].set_varchar("INSERT");
                        } else if (ObDmlEventType::DE_UPDATING == tg_event) {
                          cells[col_idx].set_varchar("UPDATE");
                        } else if (ObDmlEventType::DE_DELETING == tg_event) {
                          cells[col_idx].set_varchar("DELETE");
                        } else {
                          ret = OB_ERR_UNEXPECTED;
                          SERVER_LOG(WARN, "Invalid trigger event", K(tg_event), K(ret));
                        }
                        break;
                      }
                      case EVENT_OBJECT_SCHEMA: {
                        const ObDatabaseSchema *table_db = NULL;
                        if (OB_FAIL(schema_guard_->get_database_schema(tenant_id_,
                            table->get_database_id(), table_db))) {
                          SERVER_LOG(WARN, "Failed to get database schema",
                                     K_(tenant_id), K(table->get_database_id()), K(ret));
                        } else if (OB_ISNULL(table_db)) {
                          ret = OB_ERR_UNEXPECTED;
                          SERVER_LOG(WARN, "Database schema should not be NULL", K(ret));
                        } else {
                          cells[col_idx].set_varchar(table_db->get_database_name());
                          cells[col_idx].set_collation_type(ObCharset::get_default_collation(
                                                            ObCharset::get_default_charset()));
                        }
                        break;
                      }
                      case ACTION_STATEMENT: {
                        const ObString tg_body = tg_info->get_trigger_body();
                        cells[col_idx].set_lob_value(ObLongTextType, tg_body.ptr(),
                                                     static_cast<int32_t>(tg_body.length()));
                        cells[col_idx].set_collation_type(ObCharset::get_default_collation(
                                                            ObCharset::get_default_charset()));
                        break;
                      }
                      case ACTION_TIMING: {
                        const uint64_t timing_points = tg_info->get_timing_points();
                        if (4 == timing_points) {
                          cells[col_idx].set_varchar("BEFORE");
                        } else if (8 == timing_points) {
                          cells[col_idx].set_varchar("AFTER");
                        } else {
                          SERVER_LOG(WARN, "Invalid timing points in mysql mode", K(timing_points),
                                     K(ret));
                        }
                        cells[col_idx].set_collation_type(ObCharset::get_default_collation(
                                                            ObCharset::get_default_charset()));
                        break;
                      }
                      case SQL_MODE: {
                        ObObj int_value;
                        int_value.set_int(exec_env.get_sql_mode());
                        if (OB_FAIL(ob_sql_mode_to_str(int_value, cells[col_idx], allocator_))) {
                          SERVER_LOG(ERROR, "fail to convert sqlmode to string", K(int_value),
                                     K(ret));
                        } else {
                          cells[col_idx].set_collation_type(ObCharset::get_default_collation(
                                                              ObCharset::get_default_charset()));
                        }
                        break;
                      }
                      case CREATED:
                      case ACTION_CONDITION:
                      case ACTION_REFERENCE_OLD_TABLE:
                      case ACTION_REFERENCE_NEW_TABLE: {
                        cells[col_idx].set_null();
                        break;
                      }
#define COLUMN_SET_WITH_TYPE(COL_NAME, TYPE, VALUE) \
  case (COL_NAME): {    \
    cells[col_idx].set_##TYPE(VALUE); \
    cells[col_idx].set_collation_type(  \
                      ObCharset::get_default_collation(ObCharset::get_default_charset())); \
    break;\
  }
                      COLUMN_SET_WITH_TYPE(TRIGGER_CATALOG, varchar, "def")
                      COLUMN_SET_WITH_TYPE(TRIGGER_NAME, varchar, tg_info->get_trigger_name())
                      COLUMN_SET_WITH_TYPE(EVENT_OBJECT_CATALOG, varchar, "def")
                      COLUMN_SET_WITH_TYPE(EVENT_OBJECT_TABLE, varchar, table->get_table_name())
                      COLUMN_SET_WITH_TYPE(ACTION_ORDER, int, tg_info->get_action_order())
                      COLUMN_SET_WITH_TYPE(ACTION_ORIENTATION, varchar, "ROW")
                      COLUMN_SET_WITH_TYPE(ACTION_REFERENCE_OLD_ROW, varchar, "OLD")
                      COLUMN_SET_WITH_TYPE(ACTION_REFERENCE_NEW_ROW, varchar, "NEW")
                      COLUMN_SET_WITH_TYPE(DEFINER, varchar, user_name)
                      COLUMN_SET_WITH_TYPE(CHARACTER_SET_CLIENT, varchar,
                                           ObCharset::charset_name(exec_env.get_charset_client()))
                      COLUMN_SET_WITH_TYPE(COLLATION_CONNECTION, varchar,
                                      ObCharset::charset_name(exec_env.get_collation_connection()))
                      COLUMN_SET_WITH_TYPE(DATABASE_COLLATION, varchar,
                                      ObCharset::charset_name(exec_env.get_collation_database()))
#undef COLUMN_SET_WITH_TYPE
                      default: {
                        ret = OB_ERR_UNEXPECTED;
                        SERVER_LOG(WARN, "Column id unexpected", K(col_id), K(ret));
                      }
                    } // end of case
                  } // end of if
                } // end of for
                if (OB_SUCC(ret)) {
                  if (OB_FAIL(scanner_.add_row(cur_row_))) {
                    SERVER_LOG(WARN, "fail to add row", K(ret), K(cur_row_));
                  }
                }
              } // end of else
            } // end of else
          } // end of for tg_array count
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
        // do nothing
      }
    }
  }
  return ret;
}

} // observer
} // oceanbase
