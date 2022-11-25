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
#include "observer/virtual_table/ob_information_schema_privileges_table.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "sql/session/ob_sql_session_info.h"
using namespace oceanbase::common;
using namespace oceanbase::share::schema;
namespace oceanbase
{
namespace observer
{
const char *ObInfoSchemaSchemaPrivilegesTable::priv_type_strs[OB_PRIV_MAX_SHIFT + 1];
ObInfoSchemaSchemaPrivilegesTable::StaticInit ob_schema_priv_static_init;

ObInfoSchemaSchemaPrivilegesTable::StaticInit::StaticInit()
{
  for (int64_t i = 0; i <= OB_PRIV_MAX_SHIFT; ++i) {
    ObInfoSchemaSchemaPrivilegesTable::priv_type_strs[i] = "SHOULD NOT SHOW";
  }
  ObInfoSchemaSchemaPrivilegesTable::priv_type_strs[OB_PRIV_ALTER_SHIFT] = "ALTER";
  ObInfoSchemaSchemaPrivilegesTable::priv_type_strs[OB_PRIV_CREATE_SHIFT] = "CREATE";
  ObInfoSchemaSchemaPrivilegesTable::priv_type_strs[OB_PRIV_DELETE_SHIFT] = "DELETE";
  ObInfoSchemaSchemaPrivilegesTable::priv_type_strs[OB_PRIV_DROP_SHIFT] = "DROP";
  ObInfoSchemaSchemaPrivilegesTable::priv_type_strs[OB_PRIV_INSERT_SHIFT] = "INSERT";
  ObInfoSchemaSchemaPrivilegesTable::priv_type_strs[OB_PRIV_UPDATE_SHIFT] = "UPDATE";
  ObInfoSchemaSchemaPrivilegesTable::priv_type_strs[OB_PRIV_SELECT_SHIFT] = "SELECT";
  ObInfoSchemaSchemaPrivilegesTable::priv_type_strs[OB_PRIV_INDEX_SHIFT] = "INDEX";
  ObInfoSchemaSchemaPrivilegesTable::priv_type_strs[OB_PRIV_CREATE_VIEW_SHIFT] = "CREATE VIEW";
  ObInfoSchemaSchemaPrivilegesTable::priv_type_strs[OB_PRIV_SHOW_VIEW_SHIFT] = "SHOW VIEW";
}

ObInfoSchemaSchemaPrivilegesTable::ObInfoSchemaSchemaPrivilegesTable()
    : ObVirtualTableScannerIterator(),
      tenant_id_(OB_INVALID_ID),
      user_id_(OB_INVALID_ID)
{
}

ObInfoSchemaSchemaPrivilegesTable::~ObInfoSchemaSchemaPrivilegesTable()
{
}

void ObInfoSchemaSchemaPrivilegesTable::reset()
{
  tenant_id_ = OB_INVALID_ID;
  user_id_ = OB_INVALID_ID;
  session_ = NULL;
  ObVirtualTableScannerIterator::reset();
}

int ObInfoSchemaSchemaPrivilegesTable::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  const int64_t col_count = output_column_ids_.count();
  if (OB_UNLIKELY(col_count > cur_row_.count_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "column count too big", K(col_count), K(cur_row_.count_), K(ret));
  } else if (OB_UNLIKELY(OB_ISNULL(allocator_) || OB_ISNULL(schema_guard_)
      || OB_INVALID_ID == tenant_id_ || OB_INVALID_ID == user_id_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "Invalid argument", K(allocator_), K(schema_guard_),
        K(tenant_id_), K(user_id_), K(ret));
  } else {
    if (!start_to_read_) {
      ObArray<const ObDBPriv *> db_priv_array;
      if (OB_FAIL(get_db_privs(tenant_id_, user_id_, db_priv_array))) {
        SERVER_LOG(WARN, "Failed to get table privs", K(ret));
      } else {
        for (int64_t dp_id = 0; OB_SUCC(ret) && dp_id < db_priv_array.count(); ++dp_id) {
          const ObDBPriv *db_priv = db_priv_array.at(dp_id);
          if (OB_ISNULL(db_priv)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("db_priv is null", K(ret), K(dp_id));
          } else if (ObString(OB_RECYCLEBIN_SCHEMA_NAME) == db_priv->get_database_name_str()
                     || ObString(OB_PUBLIC_SCHEMA_NAME) == db_priv->get_database_name_str()
                     || ObString(OB_ORA_SYS_SCHEMA_NAME) == db_priv->get_database_name_str()
                     || ObString(OB_ORA_LBACSYS_NAME) == db_priv->get_database_name_str()
                     || ObString(OB_ORA_AUDITOR_NAME) == db_priv->get_database_name_str()) {
            continue;
          } else if (OB_FAIL(fill_row_with_db_priv(db_priv))) {
            SERVER_LOG(WARN, "failed to fill row", K(ret));
          }// get db priv success
        }// traverse db priv
        if (OB_SUCC(ret)) {
          scanner_it_ = scanner_.begin();
          start_to_read_ = true;
        }
      }// get db priv success
    } else {
      //start_to_read_, do nothing
    }
    if (OB_SUCC(ret)) {
      if (start_to_read_) {
        if (OB_FAIL(scanner_it_.get_next_row(cur_row_))) {
          if (OB_ITER_END != ret) {
            SERVER_LOG(WARN, "fail to get next row", K(ret));
          }
        }else {
          row = &cur_row_;
        }
      }
    }
  }//param check success 1
  return ret;
}


int ObInfoSchemaSchemaPrivilegesTable::get_db_privs(const uint64_t tenant_id,
                                                    const uint64_t user_id,
                                                    ObArray<const ObDBPriv *> &db_privs)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(session_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "session_ is null", K(ret));
  } else {
    ObPrivSet user_db_priv_set = session_->get_user_priv_set();
    //ObOriginalDBKey db_key(tenant_id, user_id, ObString::make_string("mysql"));
    ObPrivSet db_priv_set = OB_PRIV_SET_EMPTY;
    if (OB_FAIL(schema_guard_->get_db_priv_set(tenant_id, user_id, ObString::make_string("mysql"), db_priv_set))) {
      SERVER_LOG(WARN, "get db priv set failed", K(ret));
    } else {
      user_db_priv_set |= db_priv_set;
      if (OB_PRIV_HAS_ANY(user_db_priv_set, OB_PRIV_SELECT)) {
        if (OB_FAIL(schema_guard_->get_db_priv_with_tenant_id(tenant_id_,
                                                              db_privs))) {
          SERVER_LOG(WARN, "Get db priv with tenant id error", K(ret));
        }
      } else {
        if (OB_FAIL(schema_guard_->get_db_priv_with_user_id(tenant_id_,
                                                            user_id_,
                                                            db_privs))) {
          SERVER_LOG(WARN, "Get db priv with user id error", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObInfoSchemaSchemaPrivilegesTable::get_user_name_from_db_priv(const ObDBPriv *db_priv,
                                                                  ObString &user_name,
                                                                  ObString &host_name)
{
  int ret = OB_SUCCESS;
  //UNUSED(priv_mgr);
  const ObUserInfo *user_info = NULL;
  if (OB_ISNULL(db_priv)) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "db_priv is null", K(ret));
  } else if (OB_ISNULL(user_info = schema_guard_->get_user_info(
      db_priv->get_tenant_user_id().tenant_id_,
      db_priv->get_tenant_user_id().user_id_))) {
    ret = OB_USER_NOT_EXIST;
    SERVER_LOG(WARN, "user not exist", K(ret));
  } else {
    user_name = user_info->get_user_name_str();
    host_name = user_info->get_host_name_str();
  }
  return ret;
}

int ObInfoSchemaSchemaPrivilegesTable::fill_row_with_db_priv(
    const share::schema::ObDBPriv *db_priv)
{
  int ret = OB_SUCCESS;
  ObObj *cells = NULL;
  if (OB_ISNULL(cells = cur_row_.cells_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "cur row cell is NULL", K(ret));
  } else if (OB_ISNULL(session_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "session_ is NULL", K(ret));
  } else if (OB_ISNULL(db_priv)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "db_priv is NULL", K(ret));
  } else {
    ObString user_name;
    ObString host_name;
    ObString account_name;
    if (OB_FAIL(get_user_name_from_db_priv(db_priv, user_name, host_name))) {
      SERVER_LOG(WARN, "Failed to get user name", K(ret));
    } else {
      int64_t pos = 0;
      int64_t buf_size = user_name.length() + host_name.length() + USERNAME_AUX_LEN;// "''@''"
      char account_name_buf[buf_size];
      memset(account_name_buf, 0, sizeof(account_name_buf));
      if (OB_FAIL(databuff_printf(account_name_buf, sizeof(account_name_buf),
          pos, "'%.*s'@'%.*s'", user_name.length(), user_name.ptr(), host_name.length(), host_name.ptr()))) {
        SERVER_LOG(WARN, "databuff_printf failed", K(ret), K(buf_size), K(pos), K(user_name), K(host_name));
      } else {
        account_name.assign_ptr(account_name_buf, static_cast<int32_t>(sizeof(account_name_buf) - 1));
        bool with_grant_option = OB_PRIV_HAS_ANY(db_priv->get_priv_set(), OB_PRIV_GRANT);
        for (int64_t shift = 1; OB_SUCC(ret) && shift <= OB_PRIV_MAX_SHIFT; ++shift) {
          //skip GRANT and privs not allowed to show
          if (shift != OB_PRIV_GRANT_SHIFT
              && OB_PRIV_HAS_ANY(db_priv->get_priv_set(), OB_PRIV_GET_TYPE(shift))
              && OB_PRIV_HAS_ANY(OB_PRIV_DB_ACC, OB_PRIV_GET_TYPE(shift))) {
            for (int64_t oc_id = 0; OB_SUCC(ret) && oc_id < output_column_ids_.count(); ++oc_id) {
              int64_t column_id = output_column_ids_.at(oc_id);
              switch (column_id) {
              case GRANTEE: {
                  cells[oc_id].set_varchar(account_name);
                  cells[oc_id].set_collation_type(ObCharset::get_system_collation());
                  break;
                }
              case TABLE_CATALOG: {
                  cells[oc_id].set_varchar("def");
                  cells[oc_id].set_collation_type(ObCharset::get_system_collation());
                  break;
                }
              case TABLE_SCHEMA: {
                  cells[oc_id].set_varchar(db_priv->get_database_name_str());
                  cells[oc_id].set_collation_type(ObCharset::get_system_collation());
                  break;
                }
              case PRIVILEGE_TYPE: {
                  cells[oc_id].set_varchar(priv_type_strs[shift]);
                  cells[oc_id].set_collation_type(ObCharset::get_system_collation());
                  break;
                }
              case IS_GRANTABLE: {
                  cells[oc_id].set_varchar(with_grant_option ? "YES" : "NO");
                  cells[oc_id].set_collation_type(ObCharset::get_system_collation());
                  break;
                }
              default: {
                  SERVER_LOG(WARN, "Unsupported column in SCHEMA_PRIVILEGES", K(column_id));
                  break;
                }
              }
            } // traverse column
            if (OB_SUCC(ret)) {
              if (OB_FAIL(scanner_.add_row(cur_row_))) {
                SERVER_LOG(WARN, "fail to add row", K(ret), K(cur_row_));
              }
            }
          } else {
            //privs not allowed to show, do nothing
          }
        }// traverse priv type
      }// databuf printf success
    }// get user name success
  }
  return ret;
}

}/* ns observer*/
}/* ns oceanbase */
