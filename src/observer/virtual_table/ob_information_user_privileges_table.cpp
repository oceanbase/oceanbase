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
#include "observer/virtual_table/ob_information_user_privileges_table.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "sql/session/ob_sql_session_info.h"
using namespace oceanbase::common;
using namespace oceanbase::share::schema;
namespace oceanbase
{
namespace observer
{
const char *ObInfoSchemaUserPrivilegesTable::priv_type_strs[OB_PRIV_MAX_SHIFT + 1];
ObInfoSchemaUserPrivilegesTable::StaticInit ob_user_priv_information_schema_static_init;

ObInfoSchemaUserPrivilegesTable::StaticInit::StaticInit()
{
  for (int64_t i = 0; i <= OB_PRIV_MAX_SHIFT; ++i) {
    ObInfoSchemaUserPrivilegesTable::priv_type_strs[i] = "SHOULD NOT SHOW";
  }
  //USAGE is not an actual priv type, this is just a hack to show "USAGE" for empty priv set!
  ObInfoSchemaUserPrivilegesTable::priv_type_strs[0] = "USAGE";
  ObInfoSchemaUserPrivilegesTable::priv_type_strs[OB_PRIV_ALTER_SHIFT] = "ALTER";
  ObInfoSchemaUserPrivilegesTable::priv_type_strs[OB_PRIV_CREATE_SHIFT] = "CREATE";
  ObInfoSchemaUserPrivilegesTable::priv_type_strs[OB_PRIV_CREATE_USER_SHIFT] = "CREATE USER";
  ObInfoSchemaUserPrivilegesTable::priv_type_strs[OB_PRIV_DELETE_SHIFT] = "DELETE";
  ObInfoSchemaUserPrivilegesTable::priv_type_strs[OB_PRIV_DROP_SHIFT] = "DROP";
  ObInfoSchemaUserPrivilegesTable::priv_type_strs[OB_PRIV_INSERT_SHIFT] = "INSERT";
  ObInfoSchemaUserPrivilegesTable::priv_type_strs[OB_PRIV_UPDATE_SHIFT] = "UPDATE";
  ObInfoSchemaUserPrivilegesTable::priv_type_strs[OB_PRIV_SELECT_SHIFT] = "SELECT";
  ObInfoSchemaUserPrivilegesTable::priv_type_strs[OB_PRIV_INDEX_SHIFT] = "INDEX";
  ObInfoSchemaUserPrivilegesTable::priv_type_strs[OB_PRIV_CREATE_VIEW_SHIFT] = "CREATE VIEW";
  ObInfoSchemaUserPrivilegesTable::priv_type_strs[OB_PRIV_SHOW_VIEW_SHIFT] = "SHOW VIEW";
  ObInfoSchemaUserPrivilegesTable::priv_type_strs[OB_PRIV_SHOW_DB_SHIFT] = "SHOW DB";
  ObInfoSchemaUserPrivilegesTable::priv_type_strs[OB_PRIV_SUPER_SHIFT] = "SUPER";
  ObInfoSchemaUserPrivilegesTable::priv_type_strs[OB_PRIV_PROCESS_SHIFT] = "PROCESS";
  ObInfoSchemaUserPrivilegesTable::priv_type_strs[OB_PRIV_CREATE_SYNONYM_SHIFT] = "CREATE SYNONYM";
  ObInfoSchemaUserPrivilegesTable::priv_type_strs[OB_PRIV_FILE_SHIFT] = "FILE";
  ObInfoSchemaUserPrivilegesTable::priv_type_strs[OB_PRIV_ALTER_TENANT_SHIFT] = "ALTER TENANT";
  ObInfoSchemaUserPrivilegesTable::priv_type_strs[OB_PRIV_ALTER_SYSTEM_SHIFT] = "ALTER SYSTEM";
  ObInfoSchemaUserPrivilegesTable::priv_type_strs[OB_PRIV_CREATE_RESOURCE_POOL_SHIFT] = 
                                                   "CREATE RESOURCE POOL";
  ObInfoSchemaUserPrivilegesTable::priv_type_strs[OB_PRIV_CREATE_RESOURCE_UNIT_SHIFT] = 
                                                   "CREATE RESOURCE UNIT";
  ObInfoSchemaUserPrivilegesTable::priv_type_strs[OB_PRIV_REPL_SLAVE_SHIFT] = 
                                                   "REPLICATION SLAVE";
  ObInfoSchemaUserPrivilegesTable::priv_type_strs[OB_PRIV_REPL_CLIENT_SHIFT] = 
                                                   "REPLICATION CLIENT";
  ObInfoSchemaUserPrivilegesTable::priv_type_strs[OB_PRIV_DROP_DATABASE_LINK_SHIFT] =
                                                   "DROP DATABASE LINK";
  ObInfoSchemaUserPrivilegesTable::priv_type_strs[OB_PRIV_CREATE_DATABASE_LINK_SHIFT] =
                                                   "CREATE DATABASE LINK";
}

ObInfoSchemaUserPrivilegesTable::ObInfoSchemaUserPrivilegesTable()
    : ObVirtualTableScannerIterator(),
      tenant_id_(OB_INVALID_ID),
      user_id_(OB_INVALID_ID)
{
}

ObInfoSchemaUserPrivilegesTable::~ObInfoSchemaUserPrivilegesTable()
{
}

void ObInfoSchemaUserPrivilegesTable::reset()
{
  tenant_id_ = OB_INVALID_ID;
  user_id_ = OB_INVALID_ID;
  session_ = NULL;
  ObVirtualTableScannerIterator::reset();
}

int ObInfoSchemaUserPrivilegesTable::inner_get_next_row(common::ObNewRow *&row)
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
      ObArray<const ObUserInfo *> user_info_array;
      if (OB_FAIL(get_user_infos(tenant_id_, user_id_, user_info_array))) {
        SERVER_LOG(WARN, "Failed to get user infos");
      } else {
        for (int64_t user_id = 0; OB_SUCC(ret) && user_id < user_info_array.count(); ++user_id) {
          const ObUserInfo *user_info = user_info_array.at(user_id);
          if (OB_ISNULL(user_info)) {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "Failed to get user info");
          } else if (OB_FAIL(fill_row_with_user_info(*user_info))) {
            SERVER_LOG(WARN, "Failed to fill row");
          }// get user info success
        }// traverse userinfo
      }// get user info array success
      if (OB_SUCC(ret)) {
        scanner_it_ = scanner_.begin();
        start_to_read_ = true;
      }
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

int ObInfoSchemaUserPrivilegesTable::get_user_infos(const uint64_t tenant_id,
                                                    const uint64_t user_id,
                                                    ObArray<const ObUserInfo *> &user_infos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "session_ is null", K(ret));
  } else {
    ObPrivSet user_db_priv_set = session_->get_user_priv_set();
    ObPrivSet db_priv_set = OB_PRIV_SET_EMPTY;
    ObOriginalDBKey db_priv_key(tenant_id, user_id, ObString::make_string("mysql"));
    if (OB_FAIL(schema_guard_->get_db_priv_set(db_priv_key, db_priv_set))) {
      LOG_WARN("get db priv set failed", K(ret));
    } else {
      user_db_priv_set |= db_priv_set;
      if (OB_PRIV_HAS_ANY(user_db_priv_set, OB_PRIV_SELECT)) {
        if (OB_FAIL(schema_guard_->get_user_infos_with_tenant_id(tenant_id_, user_infos))) {
          SERVER_LOG(WARN, "Get user infos with tenant id error", K(ret));
        }
      } else {
        const share::schema::ObUserInfo *user_info = NULL;
        if (OB_ISNULL(user_info = schema_guard_->get_user_info(tenant_id_, user_id))) {
          SERVER_LOG(WARN, "Get user infos with tenant user id error", K(ret), K_(tenant_id));
        } else if (OB_FAIL(user_infos.push_back(user_info))) {
          SERVER_LOG(WARN, "Failed to add user info", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObInfoSchemaUserPrivilegesTable::fill_row_with_user_info(
    const share::schema::ObUserInfo &user_info)
{
  int ret = OB_SUCCESS;
  ObObj *cells = NULL;
  if (OB_ISNULL(cells = cur_row_.cells_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(ERROR, "cur row cell is NULL", K(ret));
  } else if (OB_ISNULL(session_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(ERROR, "session_ is NULL", K(ret));
  } else {
    int64_t pos = 0;
    int64_t buf_size = user_info.get_user_name_str().length() + user_info.get_host_name_str().length() + USERNAME_AUX_LEN;// "''@''"
    char username_buf[buf_size];
    memset(username_buf, 0, sizeof(username_buf));
    if (OB_FAIL(databuff_printf(username_buf, sizeof(username_buf),
        pos, "'%s'@'%s'", user_info.get_user_name(), user_info.get_host_name()))) {
      SERVER_LOG(WARN, "databuff_printf failed", K(ret), K(buf_size), K(pos),
          "user_name", user_info.get_user_name());
    } else {
      ObString user_name;
      user_name.assign_ptr(username_buf, static_cast<int32_t>(buf_size - 1));
      ObPrivSet user_priv_set = user_info.get_priv_set();
      bool with_grant_option = OB_PRIV_HAS_ANY(user_priv_set, OB_PRIV_GRANT);
      //Special case for "USAGE" (empty user privset)
      //USAGE is not an actual priv type, this is a hack to show "USAGE" for empty priv
      if (!OB_PRIV_HAS_ANY(user_priv_set, OB_PRIV_ALL)) { // no priv except grant
        user_priv_set |= 1LL; // show "USAGE"
        with_grant_option = false;
      }
      for (int64_t shift = 0; OB_SUCC(ret) && shift <= OB_PRIV_MAX_SHIFT; ++shift) {
        //skip GRANT, BOOTSTRAP and privs not allowed to show, but allow "USAGE"
        if ((!OB_PRIV_HAS_ANY(user_priv_set, OB_PRIV_ALL) && 0 == shift) //allow USAGE
            || (shift != OB_PRIV_GRANT_SHIFT
                && shift != OB_PRIV_BOOTSTRAP_SHIFT
                && OB_PRIV_HAS_ANY(user_priv_set, OB_PRIV_GET_TYPE(shift))
                && OB_PRIV_HAS_ANY(OB_PRIV_ALL, OB_PRIV_GET_TYPE(shift)))) {
          for (int64_t oc_id = 0; OB_SUCC(ret) && oc_id < output_column_ids_.count(); ++oc_id) {
            int64_t column_id = output_column_ids_.at(oc_id);
            switch (column_id) {
            case GRANTEE: {
              cells[oc_id].set_varchar(user_name);
              cells[oc_id].set_collation_type(ObCharset::get_system_collation());
              break;
            }
            case TABLE_CATALOG: {
              cells[oc_id].set_varchar("def");
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
              SERVER_LOG(WARN, "Unsupported column in USER_PRIVILEGES", K(column_id));
              break;
            }
            }// switch
          }// traverse column
          if (OB_SUCC(ret)) {
            if (OB_FAIL(scanner_.add_row(cur_row_))) {
              SERVER_LOG(WARN, "fail to add row", K(ret), K(cur_row_));
            }
          }
        } else {
          //privs not allowed to show, do nothing
        }
      }// traverse priv
    }// build user name success
  }
  return ret;
}

}/* ns observer*/
}/* ns oceanbase */
