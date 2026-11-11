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
#include "lib/charset/ob_charset.h"
#include "lib/string/ob_sql_string.h"
#include "share/ob_cluster_version.h"

using namespace oceanbase::common;
using namespace oceanbase::share::schema;
namespace oceanbase
{
namespace observer
{

static int append_json_escaped_string(ObSqlString &sql, const ObString &str)
{
  int ret = OB_SUCCESS;
  const char *ptr = str.ptr();
  int64_t len = str.length();
  for (int64_t i = 0; OB_SUCC(ret) && i < len; ++i) {
    unsigned char c = static_cast<unsigned char>(ptr[i]);

    if (c == '"') {
      ret = sql.append("\\\"");
    } else if (c == '\\') {
      ret = sql.append("\\\\");
    } else if (c == '\b') {
      ret = sql.append("\\b");
    } else if (c == '\f') {
      ret = sql.append("\\f");
    } else if (c == '\n') {
      ret = sql.append("\\n");
    } else if (c == '\r') {
      ret = sql.append("\\r");
    } else if (c == '\t') {
      ret = sql.append("\\t");
    } else if (c >= 32 && c <= 126) {
      ret = sql.append_fmt("%c", c);
    } else {
      char hex_buf[8] = {0};
      snprintf(hex_buf, sizeof(hex_buf), "\\u%04X", c);
      ret = sql.append(hex_buf);
    }
  }
  return ret;
}

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

int ObMySQLUserTable::fill_password_last_changed_cell(const int64_t password_last_changed, common::ObObj &cell) const
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_TIMESTAMP == password_last_changed) {
    cell.set_null();
  } else if (OB_UNLIKELY(0 > password_last_changed)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "password last changed timestamp is invalid", K(ret), K(password_last_changed));
  } else {
    cell.set_timestamp(password_last_changed);
  }
  return ret;
}

int ObMySQLUserTable::fill_password_lifetime_cell(const int64_t password_lifetime_us, common::ObObj &cell) const
{
  int ret = OB_SUCCESS;
  if (static_cast<int64_t>(PASSWORD_EXPIRE_DEFAULT) == password_lifetime_us) {
    cell.set_null();
  } else if (static_cast<int64_t>(PASSWORD_EXPIRE_NEVER) == password_lifetime_us) {
    // mysql.user uses 0 to represent PASSWORD EXPIRE NEVER.
    cell.set_int(0);
  } else if (OB_UNLIKELY(0 > password_lifetime_us || 0 != password_lifetime_us % USECS_PER_DAY)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "password lifetime is invalid", K(ret), K(password_lifetime_us));
  } else {
    cell.set_int(password_lifetime_us / USECS_PER_DAY);
  }
  return ret;
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
      uint64_t compat_version = 0;
      if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id_, compat_version))) {
        SERVER_LOG(WARN, "fail to get tenant data version", K(ret), K_(tenant_id));
      } else if (OB_ISNULL(cells = cur_row_.cells_)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(ERROR, "cur row cell is NULL", K(ret));
      }  else {
        const bool is_password_expire_supported = DATA_VERSION_4_4_2_4 <= compat_version;
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
              const char *password_expired_str =
                  is_password_expire_supported ? user_info->get_password_expired_str() : "";
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
                    ObString upper_passwd_str;
                    if (OB_FAIL(ObCharset::toupper(ObCharset::get_default_collation(ObCharset::get_default_charset()), user_info->get_passwd_str(), upper_passwd_str, *allocator_))) {
                      SERVER_LOG(WARN, "failed to upper password", K(ret));
                    } else {
                      cells[col_idx].set_varchar(upper_passwd_str);
                    }
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
                  EXIST_PRIV_CASE(REFERENCES);
                  EXIST_PRIV_CASE(INDEX);
                  EXIST_PRIV_CASE(ALTER);
                  EXIST_PRIV_CASE(SHOW_DB);
                  EXIST_PRIV_CASE(SUPER);
                  EXIST_PRIV_CASE(CREATE_TMP_TABLE);
                  EXIST_PRIV_CASE(LOCK_TABLE);
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
                  EXIST_PRIV_CASE(EVENT);
                  EXIST_PRIV_CASE(TRIGGER);
                  EXIST_PRIV_CASE(CREATE_TABLESPACE);
                  EXIST_PRIV_CASE(CREATE_ROLE);
                  EXIST_PRIV_CASE(DROP_ROLE);
                  case (USER_ATTRIBUTES): {
                    ObSqlString json_builder;
                    ObString json_str;
                    if (user_info->get_old_password_start_time() == OB_INVALID_TIMESTAMP) {
                      cells[col_idx].set_null();
                    } else if (OB_FAIL(json_builder.assign("{\"additional_password\":\""))) {
                      SERVER_LOG(WARN, "failed to assign json head", K(ret));
                    } else if (OB_FAIL(append_json_escaped_string(json_builder, user_info->get_old_password_str()))) {
                      SERVER_LOG(WARN, "failed to append escaped password", K(ret));
                    } else if (OB_FAIL(json_builder.append("\"}"))) {
                      SERVER_LOG(WARN, "failed to append json tail", K(ret));
                    } else if (OB_FAIL(ob_write_string(*allocator_, json_builder.string(), json_str))) {
                      SERVER_LOG(WARN, "failed to write json string", K(ret));
                    } else {
                      cells[col_idx].set_string(ObLongTextType, json_str);
                      cells[col_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
                    }
                    break;
                  }
                  COLUMN_SET_WITH_TYPE(SSL_TYPE, varchar, ssl_type_str);
                  COLUMN_SET_WITH_TYPE(SSL_CIPHER, varchar, user_info->get_ssl_cipher());
                  COLUMN_SET_WITH_TYPE(X509_ISSUER, varchar, user_info->get_x509_issuer());
                  COLUMN_SET_WITH_TYPE(X509_SUBJECT, varchar, user_info->get_x509_subject());
                  COLUMN_SET_WITH_TYPE(MAX_QUESTIONS, int, 0);
                  COLUMN_SET_WITH_TYPE(MAX_UPDATES, int, 0);
                  COLUMN_SET_WITH_TYPE(PLUGIN, varchar, user_info->get_plugin());
                  COLUMN_SET_WITH_TYPE(AUTHENTICATION_STRING, varchar, user_info->get_passwd_str());
                  COLUMN_SET_WITH_TYPE(PASSWORD_EXPIRED, varchar, password_expired_str);
                  COLUMN_SET_WITH_TYPE(ACCOUNT_LOCKED, varchar, user_info->get_is_locked() ? "Y" : "N");
                  case (PASSWORD_LAST_CHANGED): {
                    const int64_t password_last_changed = user_info->get_password_last_changed();
                    if (OB_FAIL(fill_password_last_changed_cell(password_last_changed, cells[col_idx]))) {
                      SERVER_LOG(WARN, "failed to fill password last changed cell", K(ret), K(password_last_changed));
                    }
                    break;
                  }
                  case (PASSWORD_LIFETIME): {
                    const int64_t password_lifetime_us = user_info->get_password_lifetime();
                    if (OB_FAIL(fill_password_lifetime_cell(password_lifetime_us, cells[col_idx]))) {
                      SERVER_LOG(WARN, "failed to fill password lifetime cell", K(ret), K(password_lifetime_us));
                    }
                    break;
                  }

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
