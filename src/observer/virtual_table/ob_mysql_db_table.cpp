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

#include "observer/virtual_table/ob_mysql_db_table.h"

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

ObMySQLDBTable::ObMySQLDBTable()
    : ObVirtualTableScannerIterator(),
      tenant_id_(OB_INVALID_ID)
{
}

ObMySQLDBTable::~ObMySQLDBTable()
{
}

void ObMySQLDBTable::reset()
{
  tenant_id_ = OB_INVALID_ID;
  ObVirtualTableScannerIterator::reset();
}

int ObMySQLDBTable::inner_get_next_row(common::ObNewRow *&row)
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
      } else {
        ObArray<const ObDBPriv *> db_array;
        if (OB_FAIL(schema_guard_->get_db_priv_with_tenant_id(tenant_id_, db_array))) {
          SERVER_LOG(WARN, "Get user info with tenant id error", K(ret));
        } else {
          ObString user_name;
          const ObUserInfo *user_info = NULL;
          for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < db_array.count(); ++row_idx) {
            const ObDBPriv *&db_priv = db_array.at(row_idx);
            if ((ObString(ObString(OB_ORA_SYS_SCHEMA_NAME)) == db_priv->get_database_name_str())
                || (ObString(ObString(OB_ORA_LBACSYS_NAME)) == db_priv->get_database_name_str())
                || (ObString(ObString(OB_ORA_AUDITOR_NAME)) == db_priv->get_database_name_str())) {
              // oracle db不需要展示出来
              continue;
            }
            if (OB_FAIL(get_user_info(tenant_id_, db_priv->get_user_id(), user_info))) {
              SERVER_LOG(WARN, "Failed to get user_info", K(ret), K_(tenant_id));
            } else {
              for (int64_t col_idx = 0; OB_SUCC(ret) && col_idx < output_column_ids_.count(); ++col_idx) {
                const uint64_t col_id = output_column_ids_.at(col_idx);
                switch (col_id) {
                  case (HOST): {
                    cells[col_idx].set_varchar(user_info->get_host_name_str());
                    cells[col_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
                    break;
                  }
                  case (DB): {
                    cells[col_idx].set_varchar(db_priv->get_database_name_str());
                    cells[col_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
                    break;
                  }
                  case (USER): {
                    cells[col_idx].set_varchar(user_info->get_user_name_str());
                    cells[col_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
                    break;
                  }
#define EXIST_PRIV_CASE(PRIV_NAME) \
  case (PRIV_NAME##_PRIV): {    \
    cells[col_idx].set_varchar((db_priv->get_priv_set() & OB_PRIV_##PRIV_NAME) ? "Y" : "N"); \
    cells[col_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));\
    break;\
  }
#define NO_EXIST_PRIV_CASE(PRIV_NAME) \
  case (PRIV_NAME##_PRIV): {    \
    cells[col_idx].set_varchar("N"); \
    cells[col_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));\
    break;\
  }

                  EXIST_PRIV_CASE(SELECT);
                  EXIST_PRIV_CASE(INSERT);
                  EXIST_PRIV_CASE(UPDATE);
                  EXIST_PRIV_CASE(DELETE);
                  EXIST_PRIV_CASE(CREATE);
                  EXIST_PRIV_CASE(DROP);
                  EXIST_PRIV_CASE(GRANT);
                  NO_EXIST_PRIV_CASE(REFERENCES);
                  EXIST_PRIV_CASE(INDEX);
                  EXIST_PRIV_CASE(ALTER);
                  NO_EXIST_PRIV_CASE(CREATE_TMP_TABLE);
                  NO_EXIST_PRIV_CASE(LOCK_TABLES);
                  EXIST_PRIV_CASE(CREATE_VIEW);
                  EXIST_PRIV_CASE(SHOW_VIEW);
                  NO_EXIST_PRIV_CASE(CREATE_ROUTINE);
                  NO_EXIST_PRIV_CASE(ALTER_ROUTINE);
                  NO_EXIST_PRIV_CASE(EXECUTE);
                  NO_EXIST_PRIV_CASE(EVENT);
                  NO_EXIST_PRIV_CASE(TRIGGER);

#undef EXIST_PRIV_CASE
#undef NO_EXIST_PRIV_CASE
                  default: {
                    ret = OB_ERR_UNEXPECTED;
                    SERVER_LOG(WARN, "Column id unexpected", K(col_id), K(ret));
                  }
                } //end of case
              } //end of for col_count

            }
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
        if (OB_FAIL(scanner_it_.get_next_row(cur_row_))) {
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

int ObMySQLDBTable::get_user_info(const uint64_t tenant_id,
                                  const uint64_t user_id,
                                  const share::schema::ObUserInfo *&user_info)
{
  int ret = OB_SUCCESS;
  user_info = NULL;
  if (OB_ISNULL(user_info = schema_guard_->get_user_info(tenant_id, user_id))) {
    ret = OB_USER_NOT_EXIST;
    SERVER_LOG(WARN, "Failed to get user_info", K(tenant_id), K(user_id), K(ret));
  }
  return ret;
}

}
}
