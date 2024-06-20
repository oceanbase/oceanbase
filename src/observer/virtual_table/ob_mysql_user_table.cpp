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

#include "observer/virtual_table/ob_mysql_user_table.h"

#include "share/schema/ob_priv_type.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "sql/session/ob_sql_session_info.h"

using namespace oceanbase::common;
using namespace oceanbase::share::schema;
namespace oceanbase
{
namespace observer
{

ObMySQLUserTable::ObMySQLUserTable()
    : ObVirtualTableScannerIterator(),
      tenant_id_(OB_INVALID_ID)
{
}

ObMySQLUserTable::~ObMySQLUserTable()
{
}

void ObMySQLUserTable::reset()
{
  tenant_id_ = OB_INVALID_ID;
  ObVirtualTableScannerIterator::reset();
}

int ObMySQLUserTable::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "allocator is NULL", K(ret));
  } else if (OB_ISNULL(schema_guard_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "schema guard is NULL", K(ret));
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
        ObArray<const ObUserInfo *> user_array;
        if (OB_FAIL(schema_guard_->get_user_infos_with_tenant_id(tenant_id_, user_array))) {
          SERVER_LOG(WARN, "Get user info with tenant id error", K(ret));
        } else {
          const ObUserInfo *user_info = NULL;
          for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < user_array.count(); ++row_idx) {
            if (OB_ISNULL(user_info = user_array.at(row_idx))) {
              ret = OB_ERR_UNEXPECTED;
              SERVER_LOG(WARN, "User info should not be NULL", K(ret));
            } else {
              const ObString &ssl_type_str = (ObSSLType::SSL_TYPE_NOT_SPECIFIED == user_info->get_ssl_type()
                  ? get_ssl_type_string(ObSSLType::SSL_TYPE_NONE)
                  : user_info->get_ssl_type_str());
              for (int64_t col_idx = 0; OB_SUCC(ret) && col_idx < output_column_ids_.count(); ++col_idx) {
                const uint64_t col_id = output_column_ids_.at(col_idx);
                switch (col_id) {
                  case (HOST): {
                    cells[col_idx].set_varchar(user_info->get_host_name_str());
                    cells[col_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
                    break;
                  }
                  case (USER_NAME): {
                    cells[col_idx].set_varchar(user_info->get_user_name_str());
                    cells[col_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
                    break;
                  }
                  case (PASSWD): {
                    cells[col_idx].set_varchar(user_info->get_passwd_str());
                    cells[col_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
                    break;
                  }
                  case (MAX_CONNECTIONS): {
                    cells[col_idx].set_int(static_cast<int64_t>(user_info->get_max_connections()));
                    break;
                  }
                  case (MAX_USER_CONNECTIONS): {
                    cells[col_idx].set_int(static_cast<int64_t>(user_info->get_max_user_connections()));
                    break;
                  }
#define EXIST_PRIV_CASE(PRIV_NAME) \
  case (PRIV_NAME##_PRIV): {    \
    cells[col_idx].set_varchar((user_info->get_priv_set() & OB_PRIV_##PRIV_NAME) ? "Y" : "N"); \
    cells[col_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));\
    break;\
  }
#define NO_EXIST_PRIV_CASE(PRIV_NAME) \
  case (PRIV_NAME##_PRIV): {    \
    cells[col_idx].set_varchar("N"); \
    cells[col_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));\
    break;\
  }
#define COLUMN_SET_WITH_TYPE(COL_NAME, TYPE, VALUE) \
  case (COL_NAME): {    \
    cells[col_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));\
    cells[col_idx].set_##TYPE(VALUE); \
    break;\
  }

                  EXIST_PRIV_CASE(SELECT);
                  EXIST_PRIV_CASE(INSERT);
                  EXIST_PRIV_CASE(UPDATE);
                  EXIST_PRIV_CASE(DELETE);
                  EXIST_PRIV_CASE(CREATE);
                  EXIST_PRIV_CASE(DROP);
                  EXIST_PRIV_CASE(RELOAD);
                  EXIST_PRIV_CASE(SHUTDOWN);
                  EXIST_PRIV_CASE(PROCESS);
                  EXIST_PRIV_CASE(FILE);
                  EXIST_PRIV_CASE(GRANT);
                  NO_EXIST_PRIV_CASE(REFERENCES);
                  EXIST_PRIV_CASE(INDEX);
                  EXIST_PRIV_CASE(ALTER);
                  EXIST_PRIV_CASE(SHOW_DB);
                  EXIST_PRIV_CASE(SUPER);
                  NO_EXIST_PRIV_CASE(CREATE_TMP_TABLE);
                  NO_EXIST_PRIV_CASE(LOCK_TABLES);
                  EXIST_PRIV_CASE(EXECUTE);
                  EXIST_PRIV_CASE(REPL_SLAVE);
                  EXIST_PRIV_CASE(REPL_CLIENT);
                  EXIST_PRIV_CASE(DROP_DATABASE_LINK);
                  EXIST_PRIV_CASE(CREATE_DATABASE_LINK);
                  EXIST_PRIV_CASE(CREATE_VIEW);
                  EXIST_PRIV_CASE(SHOW_VIEW);
                  EXIST_PRIV_CASE(CREATE_ROUTINE);
                  EXIST_PRIV_CASE(ALTER_ROUTINE);
                  EXIST_PRIV_CASE(CREATE_USER);
                  NO_EXIST_PRIV_CASE(EVENT);
                  NO_EXIST_PRIV_CASE(TRIGGER);
                  EXIST_PRIV_CASE(CREATE_TABLESPACE);
                  NO_EXIST_PRIV_CASE(CREATE_ROLE);
                  NO_EXIST_PRIV_CASE(DROP_ROLE);
                  COLUMN_SET_WITH_TYPE(SSL_TYPE, varchar, ssl_type_str);
                  COLUMN_SET_WITH_TYPE(SSL_CIPHER, varchar, user_info->get_ssl_cipher());
                  COLUMN_SET_WITH_TYPE(X509_ISSUER, varchar, user_info->get_x509_issuer());
                  COLUMN_SET_WITH_TYPE(X509_SUBJECT, varchar, user_info->get_x509_subject());
                  COLUMN_SET_WITH_TYPE(MAX_QUESTIONS, int, 0);
                  COLUMN_SET_WITH_TYPE(MAX_UPDATES, int, 0);
                  COLUMN_SET_WITH_TYPE(PLUGIN, varchar, "ob_native_password");
                  COLUMN_SET_WITH_TYPE(AUTHENTICATION_STRING, varchar, "");
                  COLUMN_SET_WITH_TYPE(PASSWORD_EXPIRED, varchar, "");
                  COLUMN_SET_WITH_TYPE(ACCOUNT_LOCKED, varchar, user_info->get_is_locked() ? "Y" : "N");

#undef EXIST_PRIV_CASE
#undef NO_EXIST_PRIV_CASE
#undef COLUMN_SET_WITH_TYPE
                  default: {
                    ret = OB_ERR_UNEXPECTED;
                    SERVER_LOG(WARN, "Column id unexpected", K(col_id), K(ret));
                  }
                } //end of case
              } //end of for col_count
            } //end of else
            if (OB_SUCC(ret)) {
              if (OB_FAIL(scanner_.add_row(cur_row_))) {
                SERVER_LOG(WARN, "fail to add row", K(ret), K(cur_row_));
              }
            }
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

}
}
