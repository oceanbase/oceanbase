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

#define USING_LOG_PREFIX SQL_RESV
#include "sql/resolver/ob_schema_checker.h"
#include "lib/string/ob_string.h"
#include "lib/thread_local/ob_tsi_factory.h"
#include "lib/oblog/ob_log.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_column_schema.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_schema_mgr.h"
#include "share/schema/ob_udf.h"
#include "share/schema/ob_synonym_mgr.h"
#include "sql/privilege_check/ob_ora_priv_check.h"
#include "sql/ob_sql_context.h"
#include "sql/resolver/ob_resolver_define.h"
#include "observer/virtual_table/ob_table_columns.h"
#include "common/ob_smart_call.h"
#include "share/schema/ob_sys_variable_mgr.h" // ObSimpleSysVariableSchema
#include "sql/resolver/ob_stmt_resolver.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::share::schema;
using oceanbase::share::schema::ObColumnSchemaV2;
using oceanbase::share::schema::ObTableSchema;
using oceanbase::share::schema::ObTenantSchema;
using oceanbase::share::schema::ObDatabaseSchema;

namespace oceanbase
{
namespace sql
{
ObSchemaChecker::ObSchemaChecker()
  :
  is_inited_(false), schema_mgr_(NULL), sql_schema_mgr_(NULL), flag_(0)
{
}

ObSchemaChecker::~ObSchemaChecker()
{
  schema_mgr_ = NULL;
  sql_schema_mgr_ = NULL;
}

int ObSchemaChecker::init(ObSchemaGetterGuard &schema_mgr, uint64_t session_id)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("inited twice", K(ret));
  } else {
    schema_mgr_ = &schema_mgr;
    is_inited_ = true;
    flag_ = 0;
    schema_mgr.set_session_id(session_id);
    if (OB_INVALID_ID != session_id) {
      LOG_DEBUG("ObSchemaChecker init with valid session id", K(session_id));
    }
  }
  return ret;
}

int ObSchemaChecker::init(ObSqlSchemaGuard &sql_schema_mgr, uint64_t session_id)
{
  int ret = OB_SUCCESS;
  OV (OB_NOT_NULL(sql_schema_mgr.get_schema_guard()));
  OZ (init(*sql_schema_mgr.get_schema_guard(), session_id));
  OX (sql_schema_mgr_ = &sql_schema_mgr);
  return ret;
}

int ObSchemaChecker::set_lbca_op()
{
  int ret = OB_SUCCESS;
  flag_ = LBCA_OP_FLAG;
  return ret;
}

bool ObSchemaChecker::is_lbca_op()
{
  return flag_ == LBCA_OP_FLAG;
}

int ObSchemaChecker::check_ora_priv(
    const uint64_t tenant_id,
    const uint64_t uid,
    const share::schema::ObStmtOraNeedPrivs &stmt_need_privs,
    const ObIArray<uint64_t> &role_id_array) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema checker is not inited", K(is_inited_), K(ret));
  } else if (OB_FAIL(schema_mgr_->check_ora_priv(tenant_id, uid,
                                                 stmt_need_privs, role_id_array))) {
    LOG_WARN("failed to check_priv", K(tenant_id), K(uid), K(stmt_need_privs), K(ret));
  } else {}
  return ret;
}

int ObSchemaChecker::check_priv(const share::schema::ObSessionPrivInfo &session_priv,
                                const share::schema::ObStmtNeedPrivs &stmt_need_privs) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema checker is not inited", K(is_inited_), K(ret));
  } else if (OB_UNLIKELY(!session_priv.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("session_priv is invalid", K(session_priv), K(ret));
  } else if (OB_FAIL(schema_mgr_->check_priv(session_priv, stmt_need_privs))) {
    LOG_WARN("failed to check_priv", K(session_priv), K(stmt_need_privs), K(ret));
  } else {}
  return ret;
}


int ObSchemaChecker::check_priv_or(const share::schema::ObSessionPrivInfo &session_priv,
                                   const share::schema::ObStmtNeedPrivs &stmt_need_privs)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema checker is not inited", K(is_inited_), K(ret));
  } else if (OB_UNLIKELY(!session_priv.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("session_priv is invalid", K(session_priv), K(ret));
  } else if (OB_FAIL(schema_mgr_->check_priv_or(session_priv, stmt_need_privs))) {
    LOG_WARN("failed to check_priv_or", K(session_priv), K(stmt_need_privs), K(ret));
  } else {}
  return ret;
}

int ObSchemaChecker::check_db_access(share::schema::ObSessionPrivInfo &s_priv,
                                     const ObString& database_name) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema checker is not inited", K(is_inited_), K(ret));
  } else if (OB_UNLIKELY(!s_priv.is_valid() || database_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(s_priv), K(database_name), K(ret));
  } else if (OB_FAIL(schema_mgr_->check_db_access(s_priv, database_name))) {
    LOG_WARN("failed to check_db_access", K(s_priv), K(database_name), K(ret));
  } else {}
  return ret;
}

int ObSchemaChecker::check_table_show(const share::schema::ObSessionPrivInfo &s_priv,
                                      const ObString &db,
                                      const ObString &table,
                                      bool &allow_show) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema checker is not inited", K(is_inited_), K(ret));
  } else if (OB_UNLIKELY(!s_priv.is_valid() || db.empty() || table.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(s_priv), K(db), K(table), K(ret));
  } else if (OB_FAIL(schema_mgr_->check_table_show(s_priv, db, table, allow_show))) {
    LOG_WARN("failed to check_table_show", K(s_priv), K(db), K(table), K(ret));
  } else {}
  return ret;
}

int ObSchemaChecker::check_column_exists(const uint64_t tenant_id,
                                         const uint64_t table_id,
                                         const ObString &column_name,
                                         bool &is_exist,
                                         bool is_link /* = false */)
{
  int ret = OB_SUCCESS;

  is_exist = false;
  const ObColumnSchemaV2 *column_schema = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema checker is not inited", K(is_inited_), K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == table_id || column_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(table_id), K(column_name), K(ret));
  } else {
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(get_column_schema_inner(tenant_id, table_id, column_name, column_schema, is_link))) {
      LOG_WARN("get column schema failed", K(tenant_id), K(table_id), K(column_name), K(ret));
    }
    if (NULL == column_schema) {
      for (int64_t i = 0; OB_SUCC(ret) && i < tmp_cte_schemas_.count(); ++i) {
        if (tmp_cte_schemas_.at(i)->get_table_id() == table_id) {
          column_schema = tmp_cte_schemas_.at(i)->get_column_schema(column_name);
          break;
        }
      }
    }
    if (NULL == column_schema) {
      is_exist = false;
    } else {
      is_exist = true;
    }
  }

  return ret;
}

int ObSchemaChecker::check_column_exists(const uint64_t tenant_id, uint64_t table_id, uint64_t column_id, bool &is_exist, bool is_link /* = false */)
{
  int ret = OB_SUCCESS;

  is_exist = false;
  const ObColumnSchemaV2 *column_schema = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema checker is not inited", K(is_inited_), K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == table_id || OB_INVALID_ID == column_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(table_id), K(column_id), K(ret));
  } else {
    if (OB_FAIL(get_column_schema_inner(tenant_id, table_id, column_id, column_schema, is_link))) {
      LOG_WARN("get column schema failed", K(tenant_id), K(table_id), K(column_id), K(ret));
    }
    if (NULL == column_schema) {
      for (int64_t i = 0; OB_SUCC(ret) && i < tmp_cte_schemas_.count(); ++i) {
        if (tmp_cte_schemas_.at(i)->get_table_id() == table_id) {
          column_schema = tmp_cte_schemas_.at(i)->get_column_schema(column_id);
          break;
        }
      }
    }
    if (NULL == column_schema) {
      is_exist = false;
    } else {
      is_exist = true;
    }
  }

  return ret;
}

int ObSchemaChecker::check_routine_show(const share::schema::ObSessionPrivInfo &s_priv,
                                        const ObString &db,
                                        const ObString &routine,
                                        bool &allow_show) const
{
  int ret = OB_SUCCESS;
  allow_show = true;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema checker is not inited", K(is_inited_), K(ret));
  } else if (OB_UNLIKELY(!s_priv.is_valid() || db.empty() || routine.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(s_priv), K(db), K(routine), K(ret));
//  } else if (OB_FAIL(schema_mgr_->check_routine_show(s_priv, db, routine, allow_show))) { //TODO: ryan.ly
//    LOG_WARN("failed to check_table_show", K(s_priv), K(db), K(routine), K(ret));
  } else {}
  return ret;
}

int ObSchemaChecker::check_trigger_show(const share::schema::ObSessionPrivInfo &s_priv,
                                        const ObString &db,
                                        const ObString &trigger,
                                        bool &allow_show) const
{
  int ret = OB_SUCCESS;
  allow_show = true;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema checker is not inited", K(is_inited_), K(ret));
  } else if (OB_UNLIKELY(!s_priv.is_valid() || db.empty() || trigger.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(s_priv), K(db), K(trigger), K(ret));
  } else {}
  return ret;
}

int ObSchemaChecker::check_table_or_index_exists(
    const uint64_t tenant_id,
    const uint64_t database_id,
    const ObString &table_name,
    const bool is_hidden,
    bool &is_exist)
{
  int ret = OB_SUCCESS;
  bool is_index_table = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema checker is not inited", K(is_inited_), K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || OB_INVALID_ID == database_id
                        || table_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(tenant_id), K(database_id), K(table_name), K(ret));
  } else if (OB_FAIL(check_table_exists(tenant_id, database_id, table_name,
                                        is_index_table, is_hidden, is_exist))) {
    LOG_WARN("check table exist failed", K(tenant_id), K(database_id), K(table_name), K(ret));
  } else if(!is_exist) {
    is_index_table = true;
    if (OB_FAIL(check_table_exists(tenant_id, database_id, table_name,
                                   is_index_table, is_hidden, is_exist))) {
      LOG_WARN("check index exist failed", K(tenant_id), K(database_id), K(table_name), K(ret));
    }
  }
  return ret;
}

int ObSchemaChecker::check_table_exists(const uint64_t tenant_id,
                                        const uint64_t database_id,
                                        const ObString &table_name,
                                        const bool is_index_table,
                                        const bool is_hidden,
                                        bool &is_exist)
{
  int ret = OB_SUCCESS;

  is_exist = false;
  uint64_t table_id= OB_INVALID_ID;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema checker is not inited", K(is_inited_), K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || OB_INVALID_ID == database_id
                        || table_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(tenant_id), K(database_id), K(table_name), K(ret));
  } else {
    if (OB_FAIL(schema_mgr_->get_table_id(tenant_id, database_id, table_name,
                                          is_index_table, is_hidden ? ObSchemaGetterGuard::USER_HIDDEN_TABLE_TYPE : ObSchemaGetterGuard::ALL_NON_HIDDEN_TYPES, table_id))) {

      LOG_WARN("get table id failed", K(ret), K(tenant_id), K(database_id),
               K(table_name), K(is_index_table));
    } else {
      is_exist = (OB_INVALID_ID != table_id);
      if (is_exist == false) {
        bool exist = false;
        ObNameCaseMode mode = OB_NAME_CASE_INVALID;
        if (is_sys_tenant(tenant_id) || is_oceanbase_sys_database_id(database_id)) {
          // The system tenant cannot obtain the name_case_mode of the other tenants, and the system tenant shall prevail.
          mode = OB_ORIGIN_AND_INSENSITIVE;
        } else if (OB_FAIL(schema_mgr_->get_tenant_name_case_mode(tenant_id, mode))) {
          LOG_WARN("fail to get name case mode");
        } else if (OB_NAME_CASE_INVALID == mode) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid case mode", K(ret), K(mode));
        }
        if (OB_FAIL(ret)) {
          //do nothing
        } else if (OB_FAIL(find_fake_cte_schema(table_name, mode, exist))) {
          LOG_WARN("can not find the table", K(ret), K(tenant_id), K(database_id), K(table_name), K(is_index_table));
        } else {
          is_exist = exist;
        }
      }
    }
  }
  return ret;
}

int ObSchemaChecker::check_table_exists(const uint64_t tenant_id,
                                        const ObString &database_name,
                                        const ObString &table_name,
                                        const bool is_index_table,
                                        const bool is_hidden,
                                        bool &is_exist)
{
  int ret = OB_SUCCESS;

  is_exist = false;
  uint64_t table_id= OB_INVALID_ID;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema checker is not inited", K(is_inited_), K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || database_name.empty() || table_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(tenant_id), K(database_name), K(table_name), K(ret));
  } else {
    if (OB_FAIL(schema_mgr_->get_table_id(tenant_id, database_name, table_name,
                                          is_index_table, is_hidden ? ObSchemaGetterGuard::USER_HIDDEN_TABLE_TYPE : ObSchemaGetterGuard::ALL_NON_HIDDEN_TYPES, table_id))) {
      LOG_WARN("fail to check table exist", K(tenant_id), K(database_name), K(table_name),
               K(is_index_table), K(ret));
    } else {
      is_exist = (OB_INVALID_ID != table_id);
    }
  }
  return ret;
}

// mock_fk_parent_table begin
int ObSchemaChecker::get_mock_fk_parent_table_with_name(
    const uint64_t tenant_id,
    const uint64_t database_id,
    const common::ObString &name,
    const ObMockFKParentTableSchema *&schema)
{
  int ret= OB_SUCCESS;
  if (OB_FAIL(schema_mgr_->get_mock_fk_parent_table_schema_with_name(tenant_id, database_id, name, schema))) {
    LOG_WARN("failed to get mock fk parent table schema", K(ret));
  }
  return ret;
}
// mock_fk_parent_table end

int ObSchemaChecker::get_database_id(const uint64_t tenant_id,
                                     const ObString &database_name,
                                     uint64_t &database_id) const
{
  int ret = OB_SUCCESS;
  database_id = OB_INVALID_ID;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema checker is not inited", K(is_inited_), K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || database_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(tenant_id), K(database_name), K(ret));
  } else {
    if (OB_FAIL(schema_mgr_->get_database_id(tenant_id, database_name, database_id))) {
      LOG_WARN("fail to get database id", K(tenant_id), K(database_name), K(database_id), K(ret));
    } else if (OB_INVALID_ID == database_id) {
      ret = OB_ERR_BAD_DATABASE;
      LOG_WARN("database is not exist", K(tenant_id), K(database_name), K(ret));
    }
  }
  return ret;
}

int ObSchemaChecker::get_column_schema(
    const uint64_t tenant_id,
    const uint64_t table_id,
    const ObString &column_name,
    const ObColumnSchemaV2 *&column_schema,
    bool get_hidden,
    bool is_link /* = false */)
{
  int ret = OB_SUCCESS;
  column_schema = NULL;

  const ObColumnSchemaV2 *column = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema checker is not inited", K(is_inited_), K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == table_id || column_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(table_id), K(column_name), K(ret));
  } else {
    if (OB_FAIL(get_column_schema_inner(tenant_id, table_id, column_name, column, is_link))) {
      LOG_WARN("get column schema failed", K(tenant_id), K(table_id), K(column_name), K(ret));
    } else if (NULL == column) {
      for (int64_t i = 0; i < tmp_cte_schemas_.count(); i++) {
        if (tmp_cte_schemas_.at(i)->get_table_id() == table_id) {
          column = tmp_cte_schemas_.at(i)->get_column_schema(column_name);
          break;
        }
      }
      if (NULL == column) {
        ret = OB_ERR_BAD_FIELD_ERROR;
        LOG_WARN("column is not exist", K(table_id), K(column_name), K(ret));
      } else {
        column_schema = column;
        LOG_DEBUG("find a cte fake column", K(column_name));
      }
    } else if (!get_hidden && column->is_hidden()) {
      ret = OB_ERR_BAD_FIELD_ERROR;
      LOG_INFO("do not get hidden column", K(table_id), K(column_name), K(ret));
    } else {
      column_schema = column;
    }
  }

  return ret;
}

int ObSchemaChecker::get_column_schema(
    const uint64_t tenant_id,
    const uint64_t table_id,
    const uint64_t column_id,
    const ObColumnSchemaV2 *&column_schema,
    const bool get_hidden,
    bool is_link /* = false*/)
{
  int ret = OB_SUCCESS;
  column_schema = NULL;

  const ObColumnSchemaV2 *column = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema checker is not inited", K(is_inited_), K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == table_id || OB_INVALID_ID == column_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(table_id), K(column_id), K(ret));
  } else if (OB_FAIL(get_column_schema_inner(tenant_id, table_id, column_id, column, is_link))) {
    LOG_WARN("get column schema failed", K(tenant_id), K(table_id), K(column_id), K(ret));
  } else if (NULL == column) {
    ret = OB_ERR_BAD_FIELD_ERROR;
    LOG_WARN("column is not exist", K(table_id), K(column_id), K(ret));
  } else if (!get_hidden && column->is_hidden()) {
    ret = OB_ERR_BAD_FIELD_ERROR;
    LOG_INFO("do not get hidden column", K(table_id), K(column_id), K(ret));
  } else {
    column_schema = column;
  }

  return ret;
}

int ObSchemaChecker::get_user_id(const uint64_t tenant_id,
                                 const ObString &user_name,
                                 const ObString &host_name,
                                 uint64_t &user_id)
{
  int ret = OB_SUCCESS;
  user_id = OB_INVALID_ID;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema checker is not inited", K(is_inited_), K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || user_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(tenant_id), K(user_name), K(ret));
  } else if (OB_FAIL(schema_mgr_->get_user_id(tenant_id, user_name, host_name, user_id))) {
    LOG_WARN("get user id failed", K(tenant_id), K(user_name), K(host_name), K(ret));
  } else if (OB_INVALID_ID == user_id) {
    ret = OB_USER_NOT_EXIST;
    LOG_WARN("user is not exist", K(tenant_id), K(user_name), K(host_name), K(ret));
  }
  return ret;
}

int ObSchemaChecker::get_user_info(
    const uint64_t tenant_id,
    const uint64_t user_id,
    const ObUserInfo *&user_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema checker is not inited", K(is_inited_), K(ret));
  } else if (OB_INVALID_ID == user_id) {
    ret = OB_USER_NOT_EXIST;
    LOG_WARN("user is not exist", K(user_id), K(ret));
  } else if (OB_FAIL(schema_mgr_->get_user_info(tenant_id, user_id, user_info))) {
    LOG_WARN("user is not exist", K(tenant_id), K(user_id), K(ret));
  } else if (NULL == user_info) {
    ret = OB_USER_NOT_EXIST;
    LOG_WARN("user is not exist", K(user_id), K(ret));
  }

  return ret;
}

int ObSchemaChecker::get_user_info(const uint64_t tenant_id,
                                 const ObString &user_name,
                                 const ObString &host_name,
                                 const ObUserInfo *&user_info)
{
  int ret = OB_SUCCESS;
  uint64_t user_id = OB_INVALID_ID;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema checker is not inited", K(is_inited_), K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || user_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(tenant_id), K(user_name), K(ret));
  } else if (OB_FAIL(schema_mgr_->get_user_id(tenant_id, user_name, host_name, user_id))) {
    LOG_WARN("get user id failed", K(tenant_id), K(user_name), K(host_name), K(ret));
  } else if (OB_FAIL(get_user_info(tenant_id, user_id, user_info))) {
    LOG_WARN("user is not exist", K(tenant_id), K(user_id), K(user_name), K(host_name), K(ret));
  } else if (NULL == user_info) {
    ret = OB_USER_NOT_EXIST;
    LOG_WARN("user is not exist", K(tenant_id), K(user_name), K(host_name), K(ret));
  }

  return ret;
}

int ObSchemaChecker::check_table_exists_with_synonym(const uint64_t tenant_id,
                                                     const ObString &tbl_db_name,
                                                     const ObString &tbl_name,
                                                     bool is_index_table,
                                                     bool &has_synonym,
                                                     bool &table_exist)
{
  int ret = OB_SUCCESS;
  uint64_t tbl_db_id = OB_INVALID_ID;
  uint64_t obj_db_id = OB_INVALID_ID;
  ObSEArray<uint64_t, 8> syn_id_arr;
  ObString obj_name;
  table_exist = false;
  has_synonym = false;
  if (OB_FAIL(check_table_exists(tenant_id, tbl_db_name, tbl_name, is_index_table, false/*is_hidden*/, table_exist))) {
    LOG_WARN("check_table_exists failed", K(tenant_id), K(tbl_db_name), K(tbl_name), K(is_index_table));
  } else if (false == table_exist) {
    // try again with synonym
    if (OB_FAIL(get_database_id(tenant_id, tbl_db_name, tbl_db_id))) {
      LOG_WARN("get database id failed", K(ret), K(tenant_id), K(tbl_db_name), K(tbl_name));
    } else if (OB_FAIL(get_obj_info_recursively_with_synonym(tenant_id, tbl_db_id, tbl_name,
                                                             obj_db_id, obj_name,
                                                             syn_id_arr))) {
      if (OB_SYNONYM_NOT_EXIST == ret) {
        table_exist = false;
        ret = OB_SUCCESS;
        LOG_DEBUG("obj is not a synonym", K(tbl_name), K(ret));
      } else {
        LOG_WARN("get real obj represented by synonym failed", K(ret), K(tenant_id), K(tbl_db_name),
                                                               K(tbl_db_id), K(tbl_name));
      }
    } else {
      has_synonym = true;
      if (OB_FAIL(check_table_exists(tenant_id, obj_db_id, obj_name, is_index_table, false/*is_hidden*/, table_exist))) {
        LOG_WARN("get_table_schema failed", K(ret), K(tenant_id), K(tbl_db_name), K(tbl_db_id), K(tbl_name));
      } else if (false == table_exist) {
        // TODO: 错误码与Oracle不兼容
        // 如果底层对象不是table, 不能在其上建立索引，则应该返回obj xxx is not allowed here的错误信息
        // 但是目前没法区分是table不存在，还是该对象不是table。所以错误码与Oracle不一致
        ret = OB_ERR_SYNONYM_TRANSLATION_INVALID;
        LOG_WARN("object is synonym, but real object not exist", K(ret), K(tenant_id), K(tbl_db_name), K(tbl_name), K(obj_name));
        LOG_USER_ERROR(OB_ERR_SYNONYM_TRANSLATION_INVALID, to_cstring(tbl_name));
      }
    }
  } else {
    // table found. do nothing
  }

  return ret;
}

int ObSchemaChecker::get_table_schema_with_synonym(const uint64_t tenant_id,
                                                   const ObString &tbl_db_name,
                                                   const ObString &tbl_name,
                                                   bool is_index_table,
                                                   bool &has_synonym,
                                                   ObString &new_db_name,
                                                   ObString &new_tbl_name,
                                                   const share::schema::ObTableSchema *&tbl_schema)
{
  int ret = OB_SUCCESS;
  uint64_t tbl_db_id = OB_INVALID_ID;
  uint64_t obj_db_id = OB_INVALID_ID;
  ObString obj_name;
  new_db_name.reset();
  new_tbl_name.reset();
  has_synonym = false;
  tbl_schema = NULL;
  ObSEArray<uint64_t, 8> syn_id_arr;

  if (OB_FAIL(get_table_schema(tenant_id, tbl_db_name, tbl_name, is_index_table, tbl_schema))) {
    if (OB_TABLE_NOT_EXIST == ret) {
      // try with synonym
      if (OB_FAIL(get_database_id(tenant_id, tbl_db_name, tbl_db_id))) {
        LOG_WARN("get database id failed", K(ret), K(tenant_id), K(tbl_db_name));
      } else if (OB_FAIL(get_obj_info_recursively_with_synonym(tenant_id, tbl_db_id, tbl_name,
              obj_db_id, obj_name, syn_id_arr))) {
        if (OB_SYNONYM_NOT_EXIST == ret) {
          ret = OB_TABLE_NOT_EXIST;
          LOG_DEBUG("obj is not a synonym", K(tbl_name), K(ret));
        } else {
          LOG_WARN("get real obj represented by synonym failed", K(ret), K(tenant_id), K(tbl_db_name),
                                                                 K(tbl_db_id), K(tbl_name));
        }
      } else {
        const ObDatabaseSchema  *db_schema = NULL;
        has_synonym = true;
        if (OB_FAIL(schema_mgr_->get_table_schema(tenant_id, obj_db_id, obj_name, is_index_table, tbl_schema))) {
          LOG_WARN("get_table_schema failed", K(ret), K(tenant_id), K(obj_db_id), K(tbl_db_id), K(tbl_name));
        } else if (OB_ISNULL(tbl_schema)) {
          ret = OB_ERR_SYNONYM_TRANSLATION_INVALID;
          LOG_WARN("object is synonym, but real object not exist", K(ret), K(tenant_id), K(tbl_db_name), K(tbl_name), K(obj_name));
          LOG_USER_ERROR(OB_ERR_SYNONYM_TRANSLATION_INVALID, to_cstring(tbl_name));
        } else if (OB_FAIL(get_database_schema(tenant_id, obj_db_id, db_schema))) {
          LOG_WARN("get database schema failed", K(ret), K(tenant_id), K(obj_db_id));
        } else if (OB_ISNULL(db_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get database schema failed", K(ret), K(tenant_id), K(obj_db_id));
        } else {
          new_db_name = db_schema->get_database_name();
          new_tbl_name = obj_name;
        }
      }
    } else {
      LOG_WARN("get_table_schema failed", K(ret), K(tenant_id), K(tbl_db_name), K(tbl_db_id), K(tbl_name));
    }
  } else {
    has_synonym = false;
  }

  return ret;
}

int ObSchemaChecker::get_simple_table_schema(
    const uint64_t tenant_id,
    const uint64_t &db_id,
    const ObString &table_name,
    const bool is_index_table,
    const ObSimpleTableSchemaV2 *&simple_table_schema)
{
  int ret = OB_SUCCESS;
  simple_table_schema = NULL;

  const ObSimpleTableSchemaV2 *table = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema checker is not inited", K(is_inited_), K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || db_id == OB_INVALID_ID
                         || table_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(tenant_id), K(db_id), K(table_name), K(ret));
  } else if (OB_FAIL(schema_mgr_->get_simple_table_schema(tenant_id, db_id, table_name,
                                            is_index_table, table))) {
    LOG_WARN("get table schema failed", K(tenant_id), K(db_id), K(table_name), K(ret));
  } else if (NULL == table) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table is not exist", K(tenant_id), K(db_id), K(table_name),
        K(ret));
  } else {
    simple_table_schema = table;
  }
  return ret;
}

int ObSchemaChecker::get_table_schema(const uint64_t tenant_id, const ObString &database_name,
                                      const ObString &table_name, const bool is_index_table,
                                      const ObTableSchema *&table_schema)
{
  int ret = OB_SUCCESS;
  table_schema = NULL;

  const ObTableSchema *table = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema checker is not inited", K(is_inited_), K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || database_name.empty() || table_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(tenant_id), K(database_name), K(table_name), K(ret));
  } else if (OB_FAIL(schema_mgr_->get_table_schema(tenant_id, database_name, table_name,
                                            is_index_table, table))) {
    LOG_WARN("get table schema failed", K(tenant_id), K(database_name), K(table_name), K(ret));
  } else if (NULL == table) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table is not exist", K(tenant_id), K(database_name), K(table_name),
        K(ret));
  } else if (false == table->is_tmp_table()
             && 0 != table->get_session_id()
             && OB_INVALID_ID != schema_mgr_->get_session_id()) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_USER_ERROR(OB_TABLE_NOT_EXIST, to_cstring(database_name), to_cstring(table_name));
  } else {
    table_schema = table;
  }
  return ret;
}

// 注意：这个函数只能在 sql 层使用
int ObSchemaChecker::get_table_schema(const uint64_t tenant_id,
                                      const uint64_t database_id,
                                      const ObString &table_name,
                                      const bool is_index_table,
                                      const bool cte_table_fisrt,
                                      const bool is_hidden,
                                      const ObTableSchema *&table_schema)
{
  int ret = OB_SUCCESS;
  table_schema = NULL;

  const ObTableSchema *table = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema checker is not inited", K(is_inited_), K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || OB_INVALID_ID == database_id
             || table_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(tenant_id), K(database_id), K(table_name), K(ret));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(schema_mgr_->get_table_schema(tenant_id, database_id, table_name,
        is_index_table, table, is_hidden))) {
    LOG_WARN("get table schema failed", K(tenant_id), K(database_id), K(table_name),
             K(is_index_table), K(ret));
  } else {
    // 也有可能是临时cte递归表schema与已有表重名，
    // 这个时候必须由cte递归表schema优先(same with oracle)
    // 在fake schema中找到了，则覆盖之前找到的基本表
    if (cte_table_fisrt) {
      ObNameCaseMode mode = OB_NAME_CASE_INVALID;
      if (is_sys_tenant(tenant_id) || is_oceanbase_sys_database_id(database_id)) {
        // The system tenant cannot obtain the name_case_mode of the other tenants, and the system tenant shall prevail.
        mode = OB_ORIGIN_AND_INSENSITIVE;
      } else if (OB_FAIL(schema_mgr_->get_tenant_name_case_mode(tenant_id, mode))) {
        LOG_WARN("fail to get name case mode");
      } else if (OB_NAME_CASE_INVALID == mode) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid case mode", K(ret), K(mode));
      }
      if (OB_SUCC(ret)) {
        for (int64_t i = 0; i < tmp_cte_schemas_.count(); i++) {
          if (ObCharset::case_mode_equal(mode, tmp_cte_schemas_.at(i)->get_table_name_str(), table_name)) {
            table = tmp_cte_schemas_.at(i);
            break;
          }
        }
      }
    }
    if (NULL == table) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("table is not exist", K(tenant_id), K(database_id), K(table_name), K(ret));
     } else if (false == table->is_tmp_table()
                && 0 != table->get_session_id()
                && OB_INVALID_ID != schema_mgr_->get_session_id()
                && table->get_session_id() != schema_mgr_->get_session_id()) {
      const ObDatabaseSchema  *db_schema = NULL;
      if (OB_FAIL(schema_mgr_->get_database_schema(tenant_id, database_id, db_schema))) {
        LOG_WARN("get database schema failed", K(tenant_id), K(database_id), K(ret));
      } else if (NULL == db_schema) {
        ret = OB_ERR_BAD_DATABASE;
        LOG_WARN("fail to get database schema", K(tenant_id), K(database_id), K(ret));
      } else {
        ret = OB_TABLE_NOT_EXIST;
        LOG_USER_ERROR(OB_TABLE_NOT_EXIST, db_schema->get_database_name(), to_cstring(table_name));
      }
    } else {
      table_schema = table;
    }
  }
  return ret;
}

// 注意：这个函数只能在 sql 层使用
// tmp_cte_schemas_ is only maintained in resolver's SchemaChecker
// Transformer's SchemaChecker doesn't have tmp_cte_schemas.
int ObSchemaChecker::get_table_schema(const uint64_t tenant_id, const uint64_t table_id,
                                      const ObTableSchema *&table_schema,
                                      bool is_link /* = false */) const
{
  int ret = OB_SUCCESS;
  table_schema = NULL;
  const ObTableSchema *table = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema checker is not inited", K(is_inited_), K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(table_id), K(ret), K(lbt()));
  } else if (!is_link && OB_FAIL(get_table_schema_inner(tenant_id, table_id, table))) {
    LOG_WARN("get table schema failed", K(tenant_id), K(table_id), K(ret));
  } else if (is_link && OB_FAIL(get_link_table_schema_inner(table_id, table))) {
    LOG_WARN("get link table schema failed", K(table_id), K(ret));
  } else if (NULL == table) {
    // 也有可能是临时cte递归表schema
    for (int64_t i = 0; i < tmp_cte_schemas_.count(); i++) {
      if (tmp_cte_schemas_.at(i)->get_table_id() == table_id) {
        table = tmp_cte_schemas_.at(i);
        break;
      }
    }
    if (NULL == table) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("table is not exist", K(table_id));
    } else {
      table_schema = table;
    }
  } else {
    table_schema = table;
  }
  return ret;
}

int ObSchemaChecker::get_link_table_schema(const uint64_t dblink_id,
                                           const ObString &database_name,
                                           const ObString &table_name,
                                           const ObTableSchema *&table_schema,
                                           sql::ObSQLSessionInfo *session_info,
                                           const common::ObString &dblink_name,
                                           bool is_reverse_link)
{
  int ret = OB_SUCCESS;
  OV (OB_NOT_NULL(sql_schema_mgr_));
  OZ (sql_schema_mgr_->get_table_schema(dblink_id, database_name, table_name, table_schema, session_info, dblink_name, is_reverse_link),
      dblink_id, database_name, table_name, is_reverse_link);
  return ret;
}

int ObSchemaChecker::set_link_table_schema(uint64_t dblink_id,
                          const common::ObString &database_name,
                          share::schema::ObTableSchema *table_schema)
{
  int ret = OB_SUCCESS;
  OV (OB_NOT_NULL(sql_schema_mgr_));
  OZ (sql_schema_mgr_->set_link_table_schema(dblink_id, database_name, table_schema));
  return ret;
}

int ObSchemaChecker::check_if_partition_key(const uint64_t tenant_id, uint64_t table_id, uint64_t column_id, bool &is_part_key, bool is_link /* = false*/) const
{
  int ret = OB_SUCCESS;
  is_part_key = false;
  const ObTableSchema *tbl_schema = NULL;
  if (!is_link) {
    if (OB_FAIL(get_table_schema(tenant_id, table_id, tbl_schema))) {
      LOG_WARN("get table schema failed", K(ret), K(table_id));
    } else if (tbl_schema->is_partitioned_table()) {
      if (OB_FAIL(tbl_schema->get_partition_key_info().is_rowkey_column(column_id, is_part_key))) {
        LOG_WARN("check if the column_id is partition key failed", K(ret), K(table_id), K(column_id));
      } else if (!is_part_key && PARTITION_LEVEL_TWO == tbl_schema->get_part_level()) {
        if (OB_FAIL(tbl_schema->get_subpartition_key_info().is_rowkey_column(column_id, is_part_key))) {
          LOG_WARN("check if the column_id is subpartition key failed", K(ret), K(table_id), K(column_id));
        }
      }
    }
  }
  return ret;
}

int ObSchemaChecker::get_can_read_index_array(
    const uint64_t tenant_id,
    uint64_t table_id,
    uint64_t *index_tid_array,
    int64_t &size,
    bool with_mv) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema checker is not inited", K(is_inited_), K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == table_id || size <= 0) || OB_ISNULL(index_tid_array)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(table_id), K(size), K(index_tid_array), K(ret));
  } else if (OB_NOT_NULL(sql_schema_mgr_)) {
    if (OB_FAIL(sql_schema_mgr_->get_can_read_index_array(
                table_id, index_tid_array, size, with_mv,
                true /* with_global_index*/, true /* with_domin_index*/, false /* with_spatial_index*/))) {
      LOG_WARN("failed to get_can_read_index_array", K(table_id), K(ret));
    }
  } else {
    if (OB_FAIL(schema_mgr_->get_can_read_index_array(
        tenant_id, table_id, index_tid_array, size, with_mv))) {
      LOG_WARN("failed to get_can_read_index_array", K(tenant_id), K(table_id), K(ret));
    }
  }
  return ret;
}

int ObSchemaChecker::get_can_write_index_array(const uint64_t tenant_id,
                                               uint64_t table_id,
                                               uint64_t *index_tid_array,
                                               int64_t &size,
                                               bool only_global) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema checker is not inited", K(is_inited_), K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == table_id || size <= 0) || OB_ISNULL(index_tid_array)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(table_id), K(size), K(index_tid_array), K(ret));
  } else if (OB_FAIL(schema_mgr_->get_can_write_index_array(tenant_id, table_id, index_tid_array, size, only_global))) {
    LOG_WARN("failed to get_can_write_index_array", K(tenant_id), K(table_id), K(ret));
  } else {}
  return ret;
}

int ObSchemaChecker::get_tenant_id(const ObString &tenant_name, uint64_t &tenant_id)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema checker is not inited", K(is_inited_), K(ret));
  } else if (OB_UNLIKELY(tenant_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_name", K(tenant_name), K(ret));
  } else if (OB_FAIL(schema_mgr_->get_tenant_id(tenant_name, tenant_id))) {
    LOG_WARN("get tenant id failed", K(tenant_name), K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_TENANT_NOT_EXIST;
    LOG_WARN("tenant is not exist", K(tenant_name), K(ret));
  }
  return ret;
}

int ObSchemaChecker::get_tenant_info(const uint64_t &tenant_id,
                                      const ObTenantSchema *&tenant_schema)
{
  int ret = OB_SUCCESS;
  tenant_schema = NULL;

  const ObTenantSchema *tenant = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema checker is not inited", K(is_inited_), K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(tenant_id), K(ret));
  } else if (OB_FAIL(schema_mgr_->get_tenant_info(tenant_id, tenant))) {
    LOG_WARN("get tenant schema failed", K(ret), K(tenant_id));
  } else if (NULL == tenant) {
    ret = OB_TENANT_NOT_EXIST;
    LOG_WARN("tenant is not exist", K(tenant_id), K(ret));
  } else {
    tenant_schema = tenant;
  }
  return ret;
}

int ObSchemaChecker::get_sys_variable_schema(const uint64_t &tenant_id,
                                             const ObSysVariableSchema *&sys_variable_schema)
{
  int ret = OB_SUCCESS;
  sys_variable_schema = NULL;

  const ObSysVariableSchema *sys_variable = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema checker is not inited", K(is_inited_), K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(tenant_id), K(ret));
  } else if (OB_FAIL(schema_mgr_->get_sys_variable_schema(tenant_id, sys_variable))) {
    LOG_WARN("get tenant schema failed", K(ret), K(tenant_id));
  } else if (NULL == sys_variable) {
    ret = OB_TENANT_NOT_EXIST;
    LOG_WARN("tenant is not exist", K(tenant_id), K(ret));
  } else {
    sys_variable_schema = sys_variable;
  }
  return ret;
}

int ObSchemaChecker::get_database_schema(const uint64_t tenant_id,
                                          const uint64_t database_id,
                                          const ObDatabaseSchema *&database_schema)
{
  int ret = OB_SUCCESS;
  database_schema = NULL;
  const ObDatabaseSchema *database = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema checker is not inited", K(is_inited_), K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || OB_INVALID_ID == database_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(tenant_id), K(database_id), K(ret));
  } else if (OB_FAIL(schema_mgr_->get_database_schema(tenant_id, database_id, database))) {
    LOG_WARN("get database schema failed", K(tenant_id), K(database_id), K(ret));
  } else if (NULL == database) {
    ret = OB_ERR_BAD_DATABASE;
    LOG_WARN("fail to get database schema", K(tenant_id), K(database_id), K(ret));
  } else {
    database_schema = database;
  }
  return ret;
}

int ObSchemaChecker::get_database_schema(
    const uint64_t tenant_id,
    const ObString &database_name,
    const ObDatabaseSchema *&database_schema)
{
  int ret = OB_SUCCESS;
  database_schema = NULL;
  const ObDatabaseSchema *database = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema checker is not inited", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(database_name));
  } else if (OB_FAIL(schema_mgr_->get_database_schema(tenant_id, database_name, database))) {
    LOG_WARN("get database schema failed", K(ret), K(tenant_id), K(database_name));
  } else if (NULL == database) {
    ret = OB_ERR_BAD_DATABASE;
    LOG_WARN("fail to get database schema", K(ret), K(tenant_id), K(database_name));
  } else {
    database_schema = database;
  }
  return ret;
}

int ObSchemaChecker::get_fulltext_column(const uint64_t tenant_id, uint64_t table_id,
                                         const ColumnReferenceSet &column_set,
                                         const ObColumnSchemaV2 *&column_schema) const
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table = NULL;
  if (OB_FAIL(get_table_schema(tenant_id, table_id, table))) {
    LOG_WARN("get table schema failed", K(ret), K(table_id));
  } else if (NULL == table) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table is not exist", K(table_id));
  } else {
    column_schema = table->get_fulltext_column(column_set);
  }
  return ret;
}

int ObSchemaChecker::check_column_has_index(const uint64_t tenant_id, uint64_t table_id, uint64_t column_id, bool &has_index, bool is_link /* = false */)
{
  int ret = OB_SUCCESS;
  const ObColumnSchemaV2 *col_schema = NULL;
  uint64_t index_tid_array[OB_MAX_INDEX_PER_TABLE];
  int64_t index_cnt = OB_MAX_INDEX_PER_TABLE;

  has_index = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema checker is not inited", K(is_inited_), K(ret));
  } else if (OB_FAIL(get_can_read_index_array(tenant_id, table_id, index_tid_array, index_cnt, true))) {
    LOG_WARN("get table schema failed", K(tenant_id), K(table_id));
  }
  for (int64_t i = 0; OB_SUCC(ret) && !has_index && i < index_cnt; ++i) {
    if (OB_FAIL(get_column_schema_inner(tenant_id, index_tid_array[i], column_id, col_schema, is_link))) {
      LOG_WARN("get column schema failed", K(ret), K(tenant_id), K(index_tid_array[i]), K(column_id));
    } else if (col_schema != NULL && col_schema->is_index_column()) {
      has_index = true;
    }
  }
  return ret;
}

int ObSchemaChecker::get_udt_info(
    const uint64_t tenant_id,
    const uint64_t udt_id,
    const share::schema::ObUDTTypeInfo *&udt_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema checker is not inited", K_(is_inited));
  } else if (OB_FAIL(schema_mgr_->get_udt_info(tenant_id, udt_id, udt_info))) {
    LOG_WARN("get udt info failed", K(ret), K(tenant_id));
  }
  return ret;
}

int ObSchemaChecker::get_udt_info(const uint64_t tenant_id,
                                  const common::ObString &database_name,
                                  const common::ObString &type_name,
                                  const share::schema::ObUDTTypeInfo *&udt_info)
{
  int ret = OB_SUCCESS;
  uint64_t database_id = OB_INVALID_ID;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema checker is not inited", K_(is_inited));
  } else if (OB_FAIL(get_database_id(tenant_id, database_name, database_id))) {
    LOG_WARN("get_database_id failed", K(ret));
  } else if (OB_FAIL(schema_mgr_->get_udt_info(tenant_id,
                                               database_id,
                                               OB_INVALID_ID,
                                               type_name,
                                               udt_info))) {
    LOG_WARN("get udt info failed", K(ret));
  }
  return ret;
}

int ObSchemaChecker::get_udt_info(const uint64_t tenant_id,
                                  const common::ObString &database_name,
                                  const common::ObString &type_name,
                                  const share::schema::ObUDTTypeCode &type_code,
                                  const share::schema::ObUDTTypeInfo *&udt_info)
{
  int ret = OB_SUCCESS;
  uint64_t database_id = OB_INVALID_ID;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema checker is not inited", K_(is_inited));
  } else if (OB_FAIL(get_database_id(tenant_id, database_name, database_id))) {
    LOG_WARN("get_database_id failed", K(ret));
  } else if (OB_FAIL(schema_mgr_->get_udt_info(tenant_id,
                                               database_id,
                                               OB_INVALID_ID,
                                               type_name,
                                               type_code,
                                               udt_info))) {
    LOG_WARN("get udt info failed", K(ret));
  }
  return ret;
}

int ObSchemaChecker::get_udt_id(uint64_t tenant_id,
                                uint64_t database_id,
                                uint64_t package_id,
                                const ObString &udt_name,
                                uint64_t &udt_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema checker is not inited", K_(is_inited));
  } else if (OB_FAIL(schema_mgr_->get_udt_id(tenant_id,
                                               database_id,
                                               package_id,
                                               udt_name,
                                               udt_id))) {
    LOG_WARN("get udt info failed", K(ret));
  }
  return ret;
}

int ObSchemaChecker::get_sys_udt_id(const ObString &udt_name,
                                    uint64_t &udt_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema checker is not inited", K_(is_inited));
  } else if (lib::is_oracle_mode() && udt_name.case_compare("xmltype") == 0) {
    if (OB_FAIL(schema_mgr_->get_udt_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID, -1, udt_name, udt_id))) {
      LOG_WARN("get udt info failed", K(ret));
    }
  }
  return ret;
}

int ObSchemaChecker::check_udt_exist(uint64_t tenant_id, uint64_t database_id,
                                     uint64_t package_id, ObUDTTypeCode type_code,
                                     const ObString &udt_name, bool &exist)
{
  return schema_mgr_->check_udt_exist(tenant_id, database_id,
                                      package_id, type_code, udt_name, exist);
}

int ObSchemaChecker::get_routine_info(
    const uint64_t tenant_id,
    const uint64_t routine_id,
    const share::schema::ObRoutineInfo *&routine_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema checker is not inited", K_(is_inited));
  } else if (OB_FAIL(schema_mgr_->get_routine_info(tenant_id, routine_id, routine_info))) {
    LOG_WARN("get routine info failed", K(ret), K(tenant_id));
  }
  return ret;
}

int ObSchemaChecker::get_standalone_procedure_info(const uint64_t tenant_id,
                                                const uint64_t db_id,
                                                const ObString &routine_name,
                                                const share::schema::ObRoutineInfo *&routine_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema checker is not inited", K_(is_inited));
  } else if (OB_FAIL(schema_mgr_->get_standalone_procedure_info(
                                    tenant_id, db_id, routine_name, routine_info))) {
    LOG_WARN("get routine info failed", K(ret), K(tenant_id), K(routine_name));
  }
  return ret;
}

int ObSchemaChecker::get_standalone_procedure_info(const uint64_t tenant_id,
                                                   const ObString &database_name,
                                                   const ObString &routine_name,
                                                   const share::schema::ObRoutineInfo *&routine_info)
{
  int ret = OB_SUCCESS;
  uint64_t db_id = OB_INVALID_ID;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema checker is not inited", K_(is_inited));
  } else if (OB_FAIL(get_database_id(tenant_id, database_name, db_id))) {
    LOG_WARN("get database id failed", K(ret), K(tenant_id), K(database_name), K(routine_name));
  } else if (OB_FAIL(schema_mgr_->get_standalone_procedure_info(tenant_id, db_id, routine_name, routine_info))) {
    LOG_WARN("get routine info failed", K(ret), K(tenant_id), K(database_name), K(routine_name));
  }
  return ret;
}

int ObSchemaChecker::get_standalone_function_info(const uint64_t tenant_id,
                                                  const uint64_t db_id,
                                                  const ObString &routine_name,
                                                  const share::schema::ObRoutineInfo *&routine_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema checker is not inited", K_(is_inited));
  } else if (OB_FAIL(schema_mgr_->get_standalone_function_info(
                                    tenant_id, db_id, routine_name, routine_info))) {
    LOG_WARN("get routine info failed", K(ret), K(tenant_id), K(db_id), K(routine_name));
  }
  return ret;
}

int ObSchemaChecker::get_standalone_function_info(const uint64_t tenant_id,
                                                  const ObString &database_name,
                                                  const ObString &routine_name,
                                                  const share::schema::ObRoutineInfo *&routine_info)
{
  int ret = OB_SUCCESS;
  uint64_t db_id = OB_INVALID_ID;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema checker is not inited", K_(is_inited));
  } else if (OB_FAIL(get_database_id(tenant_id, database_name, db_id))) {
    LOG_WARN("get database id failed", K(ret), K(tenant_id), K(database_name), K(routine_name));
  } else if (OB_FAIL(schema_mgr_->get_standalone_function_info(tenant_id, db_id, routine_name, routine_info))) {
    LOG_WARN("get routine info failed", K(ret), K(tenant_id), K(database_name), K(routine_name));
  }
  return ret;
}

int ObSchemaChecker::get_package_routine_infos(
  const uint64_t tenant_id, const uint64_t package_id, const uint64_t db_id,
  const common::ObString &routine_name,
  const ObRoutineType routine_type,
  common::ObIArray<const share::schema::ObIRoutineInfo *> &routine_infos)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema checker is not inited", K_(is_inited));
  } else if (OB_FAIL(schema_mgr_->get_package_routine_infos(tenant_id, db_id, package_id,
        routine_name, routine_type, routine_infos))) {
    LOG_WARN("get routine infos failed",
        K(ret), K(tenant_id), K(package_id), K(db_id), K(routine_name), K(routine_type));
  }
  return ret;
}

int ObSchemaChecker::get_package_routine_infos(
    const uint64_t tenant_id, const uint64_t package_id,
    const common::ObString &database_name,
    const common::ObString &routine_name,
    const ObRoutineType routine_type,
    common::ObIArray<const share::schema::ObIRoutineInfo *> &routine_infos)
{
  int ret = OB_SUCCESS;
  uint64_t db_id = OB_INVALID_ID;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema checker is not inited", K_(is_inited));
  } else if (OB_FAIL(get_database_id(tenant_id, database_name, db_id))) {
    LOG_WARN("get database id failed", K(ret));
  } else if (OB_FAIL(schema_mgr_->get_package_routine_infos(tenant_id, db_id, package_id,
        routine_name, routine_type, routine_infos))) {
    LOG_WARN("get routine infos failed",
        K(ret), K(tenant_id), K(package_id), K(database_name), K(routine_name), K(routine_type));
  }
  return ret;
}

int ObSchemaChecker::get_udt_routine_infos(
    const uint64_t tenant_id, const uint64_t udt_id,
    const common::ObString &database_name,
    const common::ObString &routine_name,
    const ObRoutineType routine_type,
    common::ObIArray<const share::schema::ObIRoutineInfo *> &routine_infos)
{
  int ret = OB_SUCCESS;
  uint64_t db_id = OB_INVALID_ID;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema checker is not inited", K_(is_inited));
  } else if (OB_FAIL(get_database_id(tenant_id, database_name, db_id))) {
    LOG_WARN("get database id failed", K(ret));
  } else if (OB_FAIL(schema_mgr_->get_udt_routine_infos(tenant_id, db_id, udt_id,
        routine_name, routine_type, routine_infos))) {
    LOG_WARN("get routine infos failed", K(ret));
  }
  return ret;
}

int ObSchemaChecker::get_udt_routine_infos(const uint64_t tenant_id,
                       const uint64_t database_id,
                       const uint64_t udt_id,
                       const common::ObString &routine_name,
                       const share::schema::ObRoutineType routine_type,
                       common::ObIArray<const share::schema::ObIRoutineInfo *> &routine_infos)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema checker is not inited", K_(is_inited));
  } else if (OB_FAIL(schema_mgr_->get_udt_routine_infos(tenant_id, database_id, udt_id,
              routine_name, routine_type, routine_infos))) {
    LOG_WARN("get udt routine infos failed", K(ret));
  } else {
    // do nothing
  }

  return ret;
}

int ObSchemaChecker::get_routine_infos_in_udt(const uint64_t tenant_id,
                                              const uint64_t udt_id,
                                              ObIArray<const ObRoutineInfo *> &routine_infos)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema checker is not inited", K_(is_inited));
  } else if (OB_FAIL(schema_mgr_->get_routine_infos_in_udt(tenant_id, udt_id, routine_infos))) {
    LOG_WARN("get udt routine infos failed", K(ret));
  } else {
    // do nothing
  }
  return ret;
}

int ObSchemaChecker::get_package_info(const uint64_t tenant_id,
                                      const ObString &database_name,
                                      const ObString &package_name,
                                      const share::schema::ObPackageType type,
                                      const int64_t compatible_mode,
                                      const ObPackageInfo *&package_info)
{
  int ret = OB_SUCCESS;
  uint64_t db_id = OB_INVALID_ID;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema checker is not inited", K_(is_inited));
  } else if (OB_FAIL(get_database_id(tenant_id, database_name, db_id))) {
    LOG_WARN("get database id failed", K(ret));
  } else if (OB_FAIL(schema_mgr_->get_package_info(tenant_id, db_id, package_name,
                                              type, compatible_mode, package_info))) {
    LOG_WARN("get package id failed", K(ret));
  } else if (OB_ISNULL(package_info)) {
    ret = OB_ERR_PACKAGE_DOSE_NOT_EXIST;
    LOG_WARN("package is not exist", K(tenant_id), K(database_name), K(package_name), K(ret));
  }
  return ret;
}

int ObSchemaChecker::get_trigger_info(const uint64_t tenant_id,
                                      const common::ObString &database_name,
                                      const common::ObString &tg_name,
                                      const share::schema::ObTriggerInfo *&tg_info)
{
  int ret = OB_SUCCESS;
  uint64_t db_id = OB_INVALID_ID;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema checker is not inited", K_(is_inited));
  } else if (OB_FAIL(get_database_id(tenant_id, database_name, db_id))) {
    LOG_WARN("get database id failed", K(ret), K(tenant_id), K(database_name), K(tg_name));
  } else if (schema_mgr_->get_trigger_info(tenant_id, db_id, tg_name, tg_info)) {
    LOG_WARN("get trigger info failed", K(ret), K(tenant_id), K(database_name), K(tg_name));
  }
  return ret;
}

int ObSchemaChecker::get_udt_id(const uint64_t tenant_id,
                                const ObString &database_name,
                                const ObString &udt_name,
                                uint64_t &udt_id)
{
  int ret = OB_SUCCESS;
  const ObUDTTypeInfo *udt_info = NULL;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema checker is not inited", K_(is_inited));
  } else if (OB_FAIL(get_udt_info(tenant_id, database_name, udt_name, udt_info))) {
    LOG_WARN("get udt id failed", K(ret));
  } else if (udt_info == NULL) {
    ret = OB_ERR_SP_DOES_NOT_EXIST;
    LOG_WARN("udt is not exist", K(tenant_id), K(database_name), K(udt_name), K(ret));
  } else {
    udt_id = udt_info->get_type_id();
  }
  return ret;
}

int ObSchemaChecker::get_syn_info(const uint64_t tenant_id,
                                  const ObString &database_name,
                                  const ObString &sym_name,
                                  ObString &obj_dbname,
                                  ObString &obj_name,
                                  uint64_t &synonym_id,
                                  uint64_t &database_id,
                                  bool &exists)
{
  int ret = OB_SUCCESS;
  database_id = OB_INVALID_ID;
  const ObSimpleSynonymSchema *synonym_schema = NULL;

  exists = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema checker is not inited", K_(is_inited));
  } else if (OB_FAIL(get_database_id(tenant_id, database_name, database_id))) {
    LOG_WARN("get database id failed", K(ret));
  } else if (OB_FAIL(schema_mgr_->get_synonym_info(
                      tenant_id, database_id, sym_name, synonym_schema))) {
    LOG_WARN("get_synonym_schema_with_name failed", K(ret));
  } else if (synonym_schema == NULL) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("sym is not exist", K(tenant_id), K(database_name), K(sym_name), K(ret));
  } else {
    const ObSimpleDatabaseSchema *db_schema = NULL;
    OZ (schema_mgr_->get_database_schema(synonym_schema->get_tenant_id(),
        synonym_schema->get_object_database_id(), db_schema));
    if (OB_SUCC(ret) && db_schema != NULL) {
      exists = true;
      OX (obj_dbname = db_schema->get_database_name_str());
      OX (obj_name = synonym_schema->get_object_name_str());
      OX (synonym_id = synonym_schema->get_synonym_id());
    }
  }
  return ret;
}

int ObSchemaChecker::get_package_id(const uint64_t tenant_id,
                                    const uint64_t database_id,
                                    const ObString &package_name,
                                    const int64_t compatible_mode,
                                    uint64_t &package_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema checker is not inited", K_(is_inited));
  } else if (OB_FAIL(schema_mgr_->get_package_id(
                      tenant_id, database_id, package_name, PACKAGE_TYPE, compatible_mode, package_id))) {
    LOG_WARN("get package id failed", K(ret));
  } else if (OB_INVALID_ID == package_id) {
    ret = OB_ERR_PACKAGE_DOSE_NOT_EXIST;
    LOG_WARN("package is not exist", K(tenant_id), K(database_id), K(package_name), K(ret));
  }
  return ret;
}

int ObSchemaChecker::get_package_id(const uint64_t tenant_id,
                                    const ObString &database_name,
                                    const ObString &package_name,
                                    const int64_t compatible_mode,
                                    uint64_t &package_id)
{
  int ret = OB_SUCCESS;
  uint64_t db_id = OB_INVALID_ID;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema checker is not inited", K_(is_inited));
  } else if (OB_FAIL(get_database_id(tenant_id, database_name, db_id))) {
    LOG_WARN("get database id failed", K(ret));
  } else if (OB_FAIL(schema_mgr_->get_package_id(tenant_id, db_id, package_name, PACKAGE_TYPE, compatible_mode, package_id))) {
    LOG_WARN("get package id failed", K(ret));
  } else if (OB_INVALID_ID == package_id) {
    ret = OB_ERR_PACKAGE_DOSE_NOT_EXIST;
    LOG_WARN("package is not exist", K(tenant_id), K(database_name), K(package_name), K(ret));
  }
  return ret;
}

int ObSchemaChecker::get_routine_id(const uint64_t tenant_id,
                                    const ObString &database_name,
                                    const ObString &routine_name,
                                    uint64_t &routine_id,
                                    bool &is_proc)
{
  int ret = OB_SUCCESS;
  const share::schema::ObRoutineInfo *routine_info = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema checker is not inited", K_(is_inited));
  } else if (OB_FAIL(get_standalone_procedure_info(tenant_id,
                                                   database_name,
                                                   routine_name,
                                                   routine_info))) {
    LOG_WARN("get standalone procedure failed", K(ret));
  } else if (routine_info == NULL) {
    if (OB_FAIL(get_standalone_function_info(tenant_id,
                                             database_name,
                                             routine_name,
                                             routine_info))) {
      LOG_WARN("get standalone function failed", K(ret));
    } else if (routine_info == NULL) {
      ret = OB_ERR_SP_DOES_NOT_EXIST;
      LOG_WARN("routine is not exist", K(tenant_id), K(database_name), K(routine_name), K(ret));
    } else {
      routine_id = routine_info->get_routine_id();
      is_proc = false;
    }
  } else {
    is_proc = true;
    routine_id = routine_info->get_routine_id();
  }
  return ret;
}


int ObSchemaChecker::get_package_body_id(const uint64_t tenant_id,
                                         const ObString &database_name,
                                         const ObString &package_name,
                                         const int64_t compatible_mode,
                                         uint64_t &package_body_id) {
  int ret = OB_SUCCESS;
    uint64_t db_id = OB_INVALID_ID;
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      LOG_WARN("schema checker is not inited", K_(is_inited));
    } else if (OB_FAIL(get_database_id(tenant_id, database_name, db_id))) {
      LOG_WARN("get database id failed", K(ret));
    } else if (OB_FAIL(schema_mgr_->get_package_id(tenant_id, db_id, package_name, PACKAGE_BODY_TYPE,
                                                   compatible_mode, package_body_id))) {
      LOG_WARN("get package body id failed", K(ret));
    } else if (OB_INVALID_ID == package_body_id) {
      ret = OB_ERR_PACKAGE_DOSE_NOT_EXIST;
      LOG_WARN("package body is not exist", K(tenant_id), K(database_name), K(package_name), K(ret));
    }
    return ret;
}

int ObSchemaChecker::check_keystore_exist(const uint64_t tenant_id,
                                          bool &exist)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema checker is not inited", K_(is_inited));
  } else {
    const ObKeystoreSchema *schema = NULL;
    if (OB_FAIL(schema_mgr_->get_keystore_schema(tenant_id, schema))) {
      LOG_WARN("get keystore schema failed", K(ret));
    } else if (NULL != schema) {
      exist = true;
    }
  }
  return ret;
}

int ObSchemaChecker::get_keystore_schema(const uint64_t tenant_id,
                                         const ObKeystoreSchema *&keystore_schema)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema checker is not inited", K_(is_inited));
  } else {
    const ObKeystoreSchema *schema = NULL;
    if (OB_FAIL(schema_mgr_->get_keystore_schema(tenant_id, schema))) {
      LOG_WARN("get keystore schema failed", K(ret));
    } else if (NULL != schema) {
      keystore_schema = schema;
    }
  }
  return ret;
}

int ObSchemaChecker::check_has_all_server_readonly_replica(
    const uint64_t tenant_id,
    uint64_t table_id,
    bool &has)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema checker is not inited", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(OB_INVALID_ID == table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table id", K(ret), K(table_id));
  } else if (OB_FAIL(get_table_schema_inner(tenant_id, table_id, table_schema))) {
    LOG_WARN("fail to get table schema", K(ret), K(table_id));
  } else if (OB_UNLIKELY(NULL == table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema null", K(ret), KP(table_schema), K(table_id));
  } else if (OB_FAIL(table_schema->check_has_all_server_readonly_replica(*schema_mgr_, has))) {
    LOG_WARN("fail to check has all server readonly replica", K(ret), K(table_id));
  } else {} // no more to do
  return ret;
}

int ObSchemaChecker::check_is_all_server_readonly_replica(
    const uint64_t tenant_id,
    uint64_t table_id,
    bool &is)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema checker is not inited", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(OB_INVALID_ID == table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table id", K(ret), K(table_id));
  } else if (OB_FAIL(get_table_schema_inner(tenant_id, table_id, table_schema))) {
    LOG_WARN("fail to get table schema", K(ret), K(table_id));
  } else if (OB_UNLIKELY(NULL == table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema null", K(ret), KP(table_schema), K(table_id));
  } else if (OB_FAIL(table_schema->check_is_all_server_readonly_replica(*schema_mgr_, is))) {
    LOG_WARN("fail to check is all server readonly replica", K(ret), K(table_id));
  } else {} // no more to do
  return ret;
}

int ObSchemaChecker::get_synonym_schema(uint64_t tenant_id,
                                        const uint64_t database_id,
                                        const ObString &synonym_name,
                                        uint64_t &object_database_id,
                                        uint64_t &synonym_id,
                                        ObString &object_table_name,
                                        bool &exist,
                                        bool search_public_schema,
                                        bool *is_public) const
{
  return schema_mgr_->get_object_with_synonym(tenant_id,database_id,synonym_name,
                                              object_database_id,synonym_id, object_table_name,
                                              exist, search_public_schema, is_public);
}

int ObSchemaChecker::get_obj_info_recursively_with_synonym(const uint64_t tenant_id,
                                                           const uint64_t syn_db_id,
                                                           const common::ObString &syn_name,
                                                           uint64_t &obj_db_id,
                                                           common::ObString &obj_name,
                                                           ObIArray<uint64_t> &syn_id_arr,
                                                           bool is_first_called)
{
  int ret = OB_SUCCESS;
  uint64_t syn_id = OB_INVALID_ID;
  bool syn_exist = false;
  if (OB_FAIL(get_synonym_schema(tenant_id, syn_db_id, syn_name, obj_db_id, syn_id, obj_name, syn_exist))) {
    LOG_WARN("get_obj_info_recursively_with_synonym failed", K(ret), K(tenant_id), K(syn_db_id), K(syn_name));
  }

  if (OB_SUCC(ret)) {
    if (!syn_exist) {
      if (is_first_called) {
        // 返回OB_SYNONYM_NOT_EXIST含义是：要么输入对象不存在，要么是输入对象不是同义词
        ret = OB_SYNONYM_NOT_EXIST;
        LOG_WARN("synonym not exist", K(ret), K(tenant_id), K(syn_db_id), K(syn_name));
      } else {
        // 同义词是存在的，但是最底层的对象不是synonym，所以导致syn_exist为false
        // do nothing
      }
    } else {
      uint64_t new_syn_db_id = obj_db_id;
      ObString new_syn_obj_name = obj_name;
      if (OB_UNLIKELY(has_exist_in_array(syn_id_arr, syn_id))) {
        ret = OB_ERR_LOOP_OF_SYNONYM;
        LOG_WARN("duplicated synonym id", K(ret), K(syn_id), K(syn_id_arr));
      } else if (OB_FAIL(syn_id_arr.push_back(syn_id))) {
        LOG_WARN("push back syn_id failed", K(ret), K(syn_id), K(syn_id_arr));
      } else if (OB_FAIL(SMART_CALL(get_obj_info_recursively_with_synonym(tenant_id, new_syn_db_id,
                                    new_syn_obj_name, obj_db_id, obj_name, syn_id_arr, false)))) {
        LOG_WARN("get_obj_info_recursively_with_synonym failed", K(ret), K(tenant_id),
                  K(new_syn_db_id), K(new_syn_obj_name));
      }
    }
  }
  return ret;
}

int ObSchemaChecker::add_fake_cte_schema(share::schema::ObTableSchema* tbl_schema)
{
  int ret = OB_SUCCESS;
  bool dup_schame = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < tmp_cte_schemas_.count(); i++) {
    if (tbl_schema->get_table_name() == tmp_cte_schemas_.at(i)->get_table_name()) {
      dup_schame = true;
    }
  }
  if (!dup_schame) {
    if (OB_FAIL(tmp_cte_schemas_.push_back(tbl_schema))) {
      LOG_WARN("push back cte schema failed");
    }
  }
  return ret;
}

int ObSchemaChecker::find_fake_cte_schema(common::ObString tblname, ObNameCaseMode mode, bool& exist)
{
  int ret = OB_SUCCESS;
  exist = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < tmp_cte_schemas_.count(); ++i) {
    if (ObCharset::case_mode_equal(mode, tmp_cte_schemas_.at(i)->get_table_name_str(), tblname)) {
      exist = true;
      break;
    }
  }
  return ret;
}

int ObSchemaChecker::get_schema_version(const uint64_t tenant_id, uint64_t table_id, share::schema::ObSchemaType schema_type, int64_t &schema_version)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(schema_mgr_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema mgr is null");
  } else {
    ret = schema_mgr_->get_schema_version(schema_type, tenant_id, table_id, schema_version);
  }
  return ret;
}

int ObSchemaChecker::get_tablegroup_schema(const int64_t tenant_id, const ObString &tablegroup_name, const ObTablegroupSchema *&tablegroup_schema)
{
  int ret = OB_SUCCESS;
  uint64_t tablegroup_id = OB_INVALID_ID;
  if (OB_INVALID_ID == tenant_id || tablegroup_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(tablegroup_name));
  } else if (OB_ISNULL(schema_mgr_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("invalid argument", K(ret), K(schema_mgr_));
  } else if (OB_FAIL(schema_mgr_->get_tablegroup_id(tenant_id, tablegroup_name, tablegroup_id))) {
    LOG_WARN("fail to get tablegroup id", K(ret), K(tenant_id), K(tablegroup_name));
  } else if (OB_INVALID_ID == tablegroup_id) {
    ret = OB_TABLEGROUP_NOT_EXIST;
    LOG_WARN("table group not exist", K(ret), K(tablegroup_id));
  } else if (OB_FAIL(schema_mgr_->get_tablegroup_schema(tenant_id, tablegroup_id, tablegroup_schema))) {
    LOG_WARN("fail to get tablegroup schema", K(ret), K(tenant_id), K(tablegroup_id));
  } else if (OB_ISNULL(tablegroup_schema)) {
    ret = OB_TABLEGROUP_NOT_EXIST;
    LOG_WARN("get invalid tablegroup schema", K(ret), K(tenant_id), K(tablegroup_name));
  }
  return ret;
}

int ObSchemaChecker::get_tablespace_schema(const int64_t tenant_id, const ObString &tablespace_name, const ObTablespaceSchema *&tablespace_schema)
{
  int ret = OB_SUCCESS;
  const ObTablespaceSchema *ts_schema = NULL;
  if (OB_INVALID_ID == tenant_id || tablespace_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(tablespace_name));
  } else if (OB_ISNULL(schema_mgr_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("invalid argument", K(ret), K(schema_mgr_));
  } else if (OB_FAIL(schema_mgr_->get_tablespace_schema_with_name(tenant_id, tablespace_name, ts_schema))) {
    LOG_WARN("fail to get tablespace schema", K(ret), K(tablespace_name));
  } else if (OB_ISNULL(ts_schema)) {
    ret = OB_TABLESPACE_NOT_EXIST;
    LOG_WARN("get invalid tablespace schema", K(ret), K(tenant_id), K(tablespace_name));
  } else {
    tablespace_schema = ts_schema;
  }
  return ret;
}

int ObSchemaChecker::get_tablespace_id(const int64_t tenant_id, const ObString &tablespace_name, int64_t &tablespace_id)
{
  int ret = OB_SUCCESS;
  const ObTablespaceSchema *tablespace_schema = NULL;
  if (OB_INVALID_ID == tenant_id || tablespace_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(tablespace_name));
  } else if (OB_ISNULL(schema_mgr_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("invalid argument", K(ret), K(schema_mgr_));
  } else if (OB_FAIL(schema_mgr_->get_tablespace_schema_with_name(tenant_id, tablespace_name, tablespace_schema))) {
    LOG_WARN("fail to get tablespace schema", K(ret), K(tablespace_name));
  } else if (OB_ISNULL(tablespace_schema)) {
    ret = OB_TABLESPACE_NOT_EXIST;
    LOG_WARN("get invalid tablespace schema", K(ret), K(tenant_id), K(tablespace_name));
  } else {
    tablespace_id = tablespace_schema->get_tablespace_id();
  }
  return ret;
}

int ObSchemaChecker::get_udf_info(uint64_t tenant_id,
                                  const common::ObString &udf_name,
                                  const share::schema::ObUDF *&udf_info,
                                  bool &exist)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == tenant_id || udf_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(udf_name));
  } else if (OB_ISNULL(schema_mgr_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("invalid argument", K(ret), K(schema_mgr_));
  } else if (OB_FAIL(schema_mgr_->get_udf_info(tenant_id, udf_name, udf_info, exist))) {
    LOG_WARN("failed to get udf schema", K(ret));
  }
  return ret;
}

int ObSchemaChecker::check_sequence_exist_with_name(const uint64_t tenant_id,
                                                    const uint64_t database_id,
                                                    const common::ObString &sequence_name,
                                                    bool &exists,
                                                    uint64_t &sequence_id) const
{
  int ret = OB_SUCCESS;
  bool is_system_generated = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema checker is not inited", K_(is_inited));
  } else if (OB_FAIL(schema_mgr_->check_sequence_exist_with_name(tenant_id,
                                                                 database_id,
                                                                 sequence_name,
                                                                 exists,
                                                                 sequence_id,
                                                                 is_system_generated))) {
    LOG_WARN("get sequence id failed",
             K(tenant_id),
             K(database_id),
             K(database_id),
             K(sequence_name),
             K(ret));
  }
  return ret;
}

int ObSchemaChecker::get_sequence_id(const uint64_t tenant_id,
                                     const common::ObString &database_name,
                                     const common::ObString &sequence_name,
                                     uint64_t &sequence_id) const
{
  int ret = OB_SUCCESS;
  bool is_exist = false;
  bool is_system_generated = false;
  uint64_t database_id = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema checker is not inited", K_(is_inited));
  } else if (OB_FAIL(get_database_id(tenant_id, database_name, database_id))) {
    LOG_WARN("fail to get_database_id", K(ret));
  } else if (OB_FAIL(schema_mgr_->check_sequence_exist_with_name(tenant_id,
                                                                 database_id,
                                                                 sequence_name,
                                                                 is_exist,
                                                                 sequence_id,
                                                                 is_system_generated))) {
    LOG_WARN("get sequence id failed", K(tenant_id), K(database_id), K(database_id),
                                       K(sequence_name), K(ret));
  } else if (!is_exist) {
    ret = OB_ERR_SEQ_NOT_EXIST;
  }
  return ret;
}

int ObSchemaChecker::get_label_se_policy_name_by_column_name(const uint64_t tenant_id,
                                                             const ObString &column_name,
                                                             ObString &policy_name)
{
  int ret = OB_SUCCESS;
  const ObLabelSePolicySchema *policy_schema = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema checker is not inited", K_(is_inited));
  } else if (OB_FAIL(schema_mgr_->get_label_se_policy_schema_by_column_name(tenant_id,
                                                                            column_name,
                                                                            policy_schema))) {
    LOG_WARN("policy not exsit", K(ret), K(column_name));
  } else if (OB_ISNULL(policy_schema)) {
    ret = OB_OBJECT_NAME_EXIST;
    LOG_WARN("policy not exist", K(ret));
  } else {
    policy_name = policy_schema->get_policy_name_str();
  }
  return ret;
}

// only use in oracle mode
// 如果函数执行结束时 table_schema 为空，则说明当前 db 下没有 index_name 对应的 index
int ObSchemaChecker::get_idx_schema_by_origin_idx_name(const uint64_t tenant_id,
                                                       const uint64_t database_id,
                                                       const ObString &index_name,
                                                       const ObTableSchema *&table_schema)
{
  int ret = OB_SUCCESS;
  table_schema = NULL;

  const ObTableSchema *table = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema checker is not inited", K(is_inited_), K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || OB_INVALID_ID == database_id
             || index_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(tenant_id), K(database_id), K(index_name), K(ret));
  } else if (OB_FAIL(schema_mgr_->get_idx_schema_by_origin_idx_name(tenant_id, database_id, index_name, table))) {
    LOG_WARN("get table schema failed", K(tenant_id), K(database_id), K(index_name), K(ret));
  } else {
    if (NULL == table) {
      LOG_WARN("index table schema is null", K(index_name), K(ret));
     } else if (false == table->is_tmp_table() && 0 != table->get_session_id() && OB_INVALID_ID != schema_mgr_->get_session_id()) {
      // 这种场景是查询建表时，数据还没有 insert 完成，表对外不可见
      // table->get_session_id() 为 0 时只会是临时表，或者查询建表数据未插入完成两种情况
      const ObDatabaseSchema  *db_schema = NULL;
      if (OB_FAIL(schema_mgr_->get_database_schema(tenant_id, database_id, db_schema))) {
        LOG_WARN("get database schema failed", K(tenant_id), K(database_id), K(ret));
      } else if (NULL == db_schema) {
        ret = OB_ERR_BAD_DATABASE;
        LOG_WARN("fail to get database schema", K(tenant_id), K(database_id), K(ret));
      } else {
        ret = OB_TABLE_NOT_EXIST;
        LOG_USER_ERROR(OB_TABLE_NOT_EXIST, db_schema->get_database_name(), to_cstring(index_name));
      }
    } else {
      table_schema = table;
    }
  }
  return ret;
}

int ObSchemaChecker::get_profile_id(const uint64_t tenant_id,
                                    const common::ObString &profile_name,
                                    uint64_t &profile_id)
{
  int ret = OB_SUCCESS;
  const ObProfileSchema *schema = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema checker is not inited", K_(is_inited));
  } else if (OB_FAIL(schema_mgr_->get_profile_schema_by_name(tenant_id, profile_name, schema))) {
    LOG_WARN("get profile id failed", K(ret));
  } else if (OB_ISNULL(schema)) {
    ret = OB_ERR_PROFILE_STRING_DOES_NOT_EXIST;
    LOG_USER_ERROR(OB_ERR_PROFILE_STRING_DOES_NOT_EXIST, profile_name.length(), profile_name.ptr());
  } else {
    profile_id = schema->get_profile_id();
  }
  return ret;
}

int ObSchemaChecker::get_dblink_id(uint64_t tenant_id, const ObString &dblink_name, uint64_t &dblink_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(schema_mgr_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("invalid argument", K(ret), K(schema_mgr_));
  } else if (OB_FAIL(schema_mgr_->get_dblink_id(tenant_id, dblink_name, dblink_id))) {
    LOG_WARN("failed to get dblink id", K(ret), K(tenant_id), K(dblink_name));
  }
  return ret;
}

int ObSchemaChecker::get_dblink_user(const uint64_t tenant_id,
                      const common::ObString &dblink_name,
                      common::ObString &dblink_user,
                      common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(schema_mgr_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("invalid argument", K(ret), K(schema_mgr_));
  } else if (OB_FAIL(schema_mgr_->get_dblink_user(tenant_id, dblink_name,
                                                  dblink_user, allocator))) {
    LOG_WARN("failed to get dblink id", K(ret), K(tenant_id), K(dblink_name));
  }
  return ret;
}

int ObSchemaChecker::get_dblink_schema(uint64_t tenant_id, const common::ObString &dblink_name, const share::schema::ObDbLinkSchema *&dblink_schema)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(schema_mgr_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("invalid argument", K(ret), K(schema_mgr_));
  } else if (OB_FAIL(schema_mgr_->get_dblink_schema(tenant_id, dblink_name, dblink_schema))) {
    LOG_WARN("failed to get dblink schema", K(ret), K(tenant_id), K(dblink_name));
  }
  return ret;
}

int ObSchemaChecker::get_directory_id(const uint64_t tenant_id,
                                      const common::ObString &directory_name,
                                      uint64_t &directory_id)
{
  int ret = OB_SUCCESS;
  const ObDirectorySchema *schema = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema checker is not inited", K_(is_inited));
  } else if (OB_FAIL(schema_mgr_->get_directory_schema_by_name(tenant_id, directory_name, schema))) {
    LOG_WARN("get directory schema failed", K(ret));
  } else if (OB_ISNULL(schema)) {
    /* comp oracle err code
    SQL> GRANT READ ON DD TO U2;
    GRANT READ ON DD TO U2
                  *
    ERROR at line 1:
    ORA-00942: table or view does not exist
    */
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("directory is not exists", K(directory_name));
  } else {
    directory_id = schema->get_directory_id();
  }
  return ret;
}

int ObSchemaChecker::get_table_schema_inner(const uint64_t tenant_id, uint64_t table_id,
                                            const ObTableSchema *&table_schema) const
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(sql_schema_mgr_)) {
    OZ (sql_schema_mgr_->get_table_schema(table_id, table_schema), table_id);
  } else {
    OV (OB_NOT_NULL(schema_mgr_));
    OZ (schema_mgr_->get_table_schema(tenant_id, table_id, table_schema), table_id);
  }
  return ret;
}

int ObSchemaChecker::get_link_table_schema_inner(
    uint64_t table_id,
    const ObTableSchema *&table_schema) const
{
  int ret = OB_SUCCESS;
  if (NULL == sql_schema_mgr_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_schema_mgr_ is NULL", K(ret));
  } else {
    OZ (sql_schema_mgr_->get_link_table_schema(table_id, table_schema), table_id);
  }
  return ret;
}

int ObSchemaChecker::get_column_schema_inner(const uint64_t tenant_id, uint64_t table_id,
                                             const ObString &column_name,
                                             const ObColumnSchemaV2 *&column_schema,
                                             bool is_link /* = false */) const
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(sql_schema_mgr_)) {
    OZ (sql_schema_mgr_->get_column_schema(table_id, column_name, column_schema, is_link),
        table_id, column_name);
  } else {
    OV (OB_NOT_NULL(schema_mgr_));
    OZ (schema_mgr_->get_column_schema(tenant_id, table_id, column_name, column_schema),
        table_id, column_name);
  }
  return ret;
}

int ObSchemaChecker::get_column_schema_inner(const uint64_t tenant_id, uint64_t table_id, const uint64_t column_id,
                                             const ObColumnSchemaV2 *&column_schema,
                                             bool is_link /* = false */) const
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(sql_schema_mgr_)) {
    OZ (sql_schema_mgr_->get_column_schema(table_id, column_id, column_schema, is_link),
        table_id, column_id, is_link);
  } else {
    OV (OB_NOT_NULL(schema_mgr_));
    OZ (schema_mgr_->get_column_schema(tenant_id, table_id, column_id, column_schema),
        table_id, column_id);
  }
  return ret;
}

int ObSchemaChecker::get_object_type_with_view_info(ObIAllocator* allocator,
    void* param_org,
    const uint64_t tenant_id,
    const common::ObString &database_name,
    const common::ObString &table_name,
    share::schema::ObObjectType &object_type,
    uint64_t &object_id,
    void* & view_query,
    bool is_directory,
    common::ObString &object_db_name,
    bool explicit_db,
    const common::ObString &prev_table_name,
    ObSynonymChecker &synonym_checker) /* ObSelectStmt* */
{
  int ret = OB_SUCCESS;
  object_type = ObObjectType::INVALID;
  object_id = OB_INVALID_ID;
  const share::schema::ObTableSchema *table_schema = NULL;
  uint64_t database_id = OB_INVALID_ID;

  CK (allocator != NULL);
  CK (param_org != NULL);
  object_db_name = database_name;
  ObResolverParams &param = *static_cast<ObResolverParams*>(param_org);
  if (OB_FAIL(get_database_id(tenant_id, database_name, database_id))) {
    /* 兼容oracle错误吗 */
    if (lib::is_oracle_mode() && ret == OB_ERR_BAD_DATABASE) {
      ret = OB_TABLE_NOT_EXIST;
    }
  } else if (false == is_directory) {
    ret = get_table_schema(tenant_id, database_name, table_name, false, table_schema);
    if (OB_TABLE_NOT_EXIST == ret) {
      if (lib::is_oracle_mode()) {
        ret = get_idx_schema_by_origin_idx_name(tenant_id, database_id, table_name, table_schema);
        if (OB_SUCC(ret) && table_schema == NULL) {
          ret = OB_TABLE_NOT_EXIST;
        }
      }
    }
    if (OB_SUCC(ret) && table_schema != NULL) {
      if (table_schema->is_index_table()) {
        object_type = ObObjectType::INDEX;
      } else {
        object_type = ObObjectType::TABLE;
      }
      object_id = table_schema->get_table_id();
      bool throw_error = true;
      if (table_schema->is_view_table() && !table_schema->is_materialized_view()) {
        ObSelectStmt *select_stmt = NULL;
        //ObStmtFactory stmt_factory(*allocator);
        //ObRawExprFactory expr_factory(*allocator);
        if (OB_FAIL(observer::ObTableColumns::resolve_view_definition(allocator,
            param.session_info_, schema_mgr_,
            *table_schema, select_stmt,
            *param.expr_factory_,
            *param.stmt_factory_,
            throw_error))) {
          LOG_WARN("failed to resolve view definition", K(table_name), K(ret));
        } else if (OB_UNLIKELY(NULL == select_stmt)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("select_stmt is NULL", K(ret));
        } else {
          view_query = select_stmt;
        }
      }
    }
    //check sequence
    if (OB_TABLE_NOT_EXIST == ret) {
      uint64_t sequence_id = 0;
      if (OB_FAIL(get_sequence_id(tenant_id, database_name, table_name, sequence_id))) {
        if (OB_ERR_SEQ_NOT_EXIST == ret) {
        ret = OB_TABLE_NOT_EXIST;
        }
      } else {
        object_type = ObObjectType::SEQUENCE;
        object_id = sequence_id;
      }
    }
    //check procedure/function
    if (OB_TABLE_NOT_EXIST == ret) {
      uint64_t routine_id = 0;
      bool is_proc = false;
      if (OB_FAIL(get_routine_id(tenant_id, database_name, table_name, routine_id, is_proc))) {
        if (OB_ERR_SP_DOES_NOT_EXIST == ret) {
        ret = OB_TABLE_NOT_EXIST;
        }
      } else {
        if (true == is_proc) {
          object_type = ObObjectType::PROCEDURE;
        } else {
          object_type = ObObjectType::FUNCTION;
        }
        object_id = routine_id;
      }
    }

    //check package
    if (OB_TABLE_NOT_EXIST == ret) {
      uint64_t package_id = 0;
      int64_t compatible_mode = lib::is_oracle_mode() ? COMPATIBLE_ORACLE_MODE
                                                      : COMPATIBLE_MYSQL_MODE;
      if (OB_FAIL(get_package_id(tenant_id, database_name, table_name,
                                 compatible_mode, package_id))) {
        if (OB_ERR_PACKAGE_DOSE_NOT_EXIST == ret) {
          if (OB_FAIL(get_package_id(OB_SYS_TENANT_ID,
                                     OB_SYS_DATABASE_ID,
                                     table_name,
                                     compatible_mode,
                                     package_id))) {
            if (OB_ERR_PACKAGE_DOSE_NOT_EXIST == ret) {
              ret = OB_TABLE_NOT_EXIST;
            }
          } else {
            object_type = ObObjectType::SYS_PACKAGE;
            object_id = package_id;
          }
        }
      } else {
        object_type = ObObjectType::PACKAGE;
        object_id = package_id;
      }
    }

    //check type
    if (OB_TABLE_NOT_EXIST == ret) {
      uint64_t udt_id = 0;
      if (OB_FAIL(get_udt_id(tenant_id, database_name, table_name, udt_id))) {
        if (OB_ERR_SP_DOES_NOT_EXIST == ret) {
          ret = OB_TABLE_NOT_EXIST;
        }
      } else {
        object_type = ObObjectType::TYPE;
        object_id = udt_id;
      }
    }

    //check synonym
    if (OB_TABLE_NOT_EXIST == ret) {
      ObString obj_db_name;
      ObString obj_name;
      uint64_t synonym_id = OB_INVALID_ID;
      uint64_t database_id = OB_INVALID_ID;
      bool exists = false;
      ret = OB_SUCCESS;
      OZ (get_syn_info(tenant_id, database_name, table_name, obj_db_name, obj_name, synonym_id, database_id, exists));
      if (OB_SUCC(ret) && exists) {
        object_db_name = obj_db_name;
        synonym_checker.set_synonym(true);
        OZ (synonym_checker.add_synonym_id(synonym_id, database_id));
        OZ (SMART_CALL(get_object_type_with_view_info(allocator,
                                                      param_org,
                                                      tenant_id,
                                                      obj_db_name,
                                                      obj_name,
                                                      object_type,
                                                      object_id,
                                                      view_query,
                                                      is_directory,
                                                      object_db_name,
                                                      false,
                                                      ObString(""),
                                                      synonym_checker)));
      }
    }
    //check public synonym
    if (OB_TABLE_NOT_EXIST == ret) {
      ObString obj_db_name;
      ObString obj_name;
      bool exists = false;
      ret = OB_SUCCESS;
      /* 如果用户指定了dbname，则不能check公共同义词 */
      if (explicit_db) {
        ret = OB_TABLE_NOT_EXIST;
      } else {
        uint64_t synonym_id = OB_INVALID_ID;
        uint64_t database_id = OB_INVALID_ID;
        OZ (get_syn_info(tenant_id, OB_PUBLIC_SCHEMA_NAME, table_name, obj_db_name, obj_name, synonym_id, database_id, exists));
        if (OB_SUCC(ret) && exists) {
          /* 如果上次同义词和这次的一样，说明一直没找到对应的数据库对象 */
          if (prev_table_name == obj_name) {
            ret = OB_TABLE_NOT_EXIST;
          } else {
            object_db_name = obj_db_name;
            synonym_checker.set_synonym(true);
            OZ (synonym_checker.add_synonym_id(synonym_id, database_id));
            OZ (SMART_CALL(get_object_type_with_view_info(allocator,
                                                          param_org,
                                                          tenant_id,
                                                          obj_db_name,
                                                          obj_name,
                                                          object_type,
                                                          object_id,
                                                          view_query,
                                                          is_directory,
                                                          object_db_name,
                                                          false,
                                                          table_name,
                                                          synonym_checker))); /* false，用于公共同义词上建立公共同义词的情况 */
          }
        }
      }
    }
  } else {
    // if directory, support it's a directory, to do after create directory
    uint64_t dir_id = 0;
    OZ (get_directory_id(tenant_id, table_name, dir_id));
    OX (object_type = ObObjectType::DIRECTORY);
    OX (object_id = dir_id);
  }

  if (OB_FAIL(ret)) {
    LOG_WARN("get_object_type_with_view_info failed",
             K(tenant_id), K(database_name), K(table_name), K(ret));
  }
  return ret;
}

int ObSchemaChecker::check_exist_same_name_object_with_synonym(const uint64_t tenant_id,
                                     uint64_t database_id,
                                     const common::ObString &object_name,
                                     bool &exist)
{
  int ret = OB_SUCCESS;
  exist = false;
  common::ObString database_name;
  const ObDatabaseSchema  *db_schema = NULL;
  const share::schema::ObTableSchema *table_schema = NULL;
  if (OB_FAIL(get_database_schema(tenant_id, database_id, db_schema))) {
    LOG_WARN("fail to get db schema", K(ret));
  } else if (OB_NOT_NULL(db_schema)) {
    database_name = db_schema->get_database_name();
    ret = get_table_schema(tenant_id, database_name, object_name, false, table_schema);
    if (OB_TABLE_NOT_EXIST == ret) {
      if (lib::is_oracle_mode()) {
        ret = get_idx_schema_by_origin_idx_name(tenant_id, database_id, object_name, table_schema);
        if (OB_SUCC(ret)) {
          if (table_schema == NULL) {
            ret = OB_TABLE_NOT_EXIST;
          }
        }
      }
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(table_schema)) {
      exist = true;
    }

    //check sequence
    if (OB_TABLE_NOT_EXIST == ret) {
      uint64_t sequence_id = 0;
      if (OB_FAIL(get_sequence_id(tenant_id, database_name, object_name, sequence_id))) {
        if (OB_ERR_SEQ_NOT_EXIST == ret) {
          ret = OB_TABLE_NOT_EXIST;
        }
      } else {
        exist = true;
      }
    }
    //check procedure/function
    if (OB_TABLE_NOT_EXIST == ret) {
      uint64_t routine_id = 0;
      bool is_proc = false;
      if (OB_FAIL(get_routine_id(tenant_id, database_name, object_name, routine_id, is_proc))) {
        if (OB_ERR_SP_DOES_NOT_EXIST == ret) {
          ret = OB_TABLE_NOT_EXIST;
        }
      } else {
        exist = true;
      }
    }
    //check package
    if (OB_TABLE_NOT_EXIST == ret) {
      uint64_t package_id = 0;
      int64_t compatible_mode = lib::is_oracle_mode() ? COMPATIBLE_ORACLE_MODE
                                                      : COMPATIBLE_MYSQL_MODE;
      if (OB_FAIL(get_package_id(tenant_id, database_name, object_name,
                                  compatible_mode, package_id))) {
        if (OB_ERR_PACKAGE_DOSE_NOT_EXIST == ret) {
          if (OB_FAIL(get_package_id(OB_SYS_TENANT_ID,
                                      OB_SYS_DATABASE_ID,
                                      object_name,
                                      compatible_mode,
                                      package_id))) {
            if (OB_ERR_PACKAGE_DOSE_NOT_EXIST == ret) {
              ret = OB_TABLE_NOT_EXIST;
            }
          } else {
            exist = true;
          }
        }
      } else {
        exist = true;
      }
    }

    //check type
    if (OB_TABLE_NOT_EXIST == ret) {
      uint64_t udt_id = 0;
      if (OB_FAIL(get_udt_id(tenant_id, database_name, object_name, udt_id))) {
        if (OB_ERR_SP_DOES_NOT_EXIST == ret) {
        ret = OB_TABLE_NOT_EXIST;
        }
      } else {
        exist = true;
      }
    }
  }
  if (OB_TABLE_NOT_EXIST == ret) {
    ret = OB_SUCCESS;
  }

  return ret;
}

int ObSchemaChecker::get_object_type(const uint64_t tenant_id,
                                     const common::ObString &database_name,
                                     const common::ObString &table_name,
                                     share::schema::ObObjectType &object_type,
                                     uint64_t &object_id,
                                     common::ObString &object_db_name,
                                     bool is_directory,
                                     bool explicit_db,
                                     const common::ObString &prev_table_name,
                                     ObSynonymChecker &synonym_checker)
{
  int ret = OB_SUCCESS;
  object_type = ObObjectType::INVALID;
  object_id = OB_INVALID_ID;
  const share::schema::ObTableSchema *table_schema = NULL;
  uint64_t database_id = OB_INVALID_ID;

  object_db_name = database_name;
  if (OB_FAIL(get_database_id(tenant_id, database_name, database_id))) {
    /* 兼容oracle错误吗 */
    if (lib::is_oracle_mode() && ret == OB_ERR_BAD_DATABASE) {
      ret = OB_TABLE_NOT_EXIST;
    }
  } else if (false == is_directory) {
    ret = get_table_schema(tenant_id, database_name, table_name, false, table_schema);
    if (OB_TABLE_NOT_EXIST == ret) {
      if (lib::is_oracle_mode()) {
        ret = get_idx_schema_by_origin_idx_name(tenant_id, database_id, table_name, table_schema);
        if (OB_SUCC(ret)) {
          if (table_schema == NULL) {
            ret = OB_TABLE_NOT_EXIST;
          }
        }
      }
    }
    if (OB_SUCC(ret) && table_schema != NULL) {
      if (table_schema->is_index_table()) {
        object_type = ObObjectType::INDEX;
      } else {
        object_type = ObObjectType::TABLE;
      }
      object_id = table_schema->get_table_id();
    }
    //check sequence
    if (OB_TABLE_NOT_EXIST == ret) {
      uint64_t sequence_id = 0;
      if (OB_FAIL(get_sequence_id(tenant_id, database_name, table_name, sequence_id))) {
        if (OB_ERR_SEQ_NOT_EXIST == ret) {
        ret = OB_TABLE_NOT_EXIST;
        }
      } else {
        object_type = ObObjectType::SEQUENCE;
        object_id = sequence_id;
      }
    }
    //check procedure/function
    if (OB_TABLE_NOT_EXIST == ret) {
      uint64_t routine_id = 0;
      bool is_proc = false;
      if (OB_FAIL(get_routine_id(tenant_id, database_name, table_name, routine_id, is_proc))) {
        if (OB_ERR_SP_DOES_NOT_EXIST == ret) {
        ret = OB_TABLE_NOT_EXIST;
        }
      } else {
        if (is_proc) {
          object_type = ObObjectType::PROCEDURE;
        } else {
          object_type = ObObjectType::FUNCTION;
        }
        object_id = routine_id;
      }
    }
    //check package
    if (OB_TABLE_NOT_EXIST == ret) {
      uint64_t package_id = 0;
      int64_t compatible_mode = lib::is_oracle_mode() ? COMPATIBLE_ORACLE_MODE
                                                      : COMPATIBLE_MYSQL_MODE;
      if (OB_FAIL(get_package_id(tenant_id, database_name, table_name,
                                 compatible_mode, package_id))) {
        if (OB_ERR_PACKAGE_DOSE_NOT_EXIST == ret) {
          if (OB_FAIL(get_package_id(OB_SYS_TENANT_ID,
                                     OB_SYS_DATABASE_ID,
                                     table_name,
                                     compatible_mode,
                                     package_id))) {
            if (OB_ERR_PACKAGE_DOSE_NOT_EXIST == ret) {
              ret = OB_TABLE_NOT_EXIST;
            }
          } else {
            object_type = ObObjectType::SYS_PACKAGE;
            object_id = package_id;
          }
        }
      } else {
        object_type = ObObjectType::PACKAGE;
        object_id = package_id;
      }
    }

    //check type
    if (OB_TABLE_NOT_EXIST == ret) {
      uint64_t udt_id = 0;
      if (OB_FAIL(get_udt_id(tenant_id, database_name, table_name, udt_id))) {
        if (OB_ERR_SP_DOES_NOT_EXIST == ret) {
        ret = OB_TABLE_NOT_EXIST;
        }
      } else {
        object_type = ObObjectType::TYPE;
        object_id = udt_id;
      }
    }
    //check synonym
    if (OB_TABLE_NOT_EXIST == ret) {
      ObString obj_db_name;
      ObString obj_name;
      uint64_t synonym_id = OB_INVALID_ID;
      uint64_t database_id = OB_INVALID_ID;
      bool exists = false;
      ret = OB_SUCCESS;
      OZ (get_syn_info(tenant_id, database_name, table_name, obj_db_name, obj_name, synonym_id, database_id, exists));
      if (OB_SUCC(ret) && exists) {
        object_db_name = obj_db_name;
        synonym_checker.set_synonym(true);
        OZ (synonym_checker.add_synonym_id(synonym_id, database_id));
        OZ (SMART_CALL(get_object_type(tenant_id,
                                       obj_db_name,
                                       obj_name,
                                       object_type,
                                       object_id,
                                       object_db_name,
                                       is_directory,
                                       false,
                                       ObString(""),
                                       synonym_checker)));
      }
    }
    //check public synonym
    if (OB_TABLE_NOT_EXIST == ret) {
      ObString obj_db_name;
      ObString obj_name;
      uint64_t synonym_id = OB_INVALID_ID;
      uint64_t database_id = OB_INVALID_ID;
      bool exists = false;
      ret = OB_SUCCESS;
      if (explicit_db) {
        ret = OB_TABLE_NOT_EXIST;
      } else {
        OZ (get_syn_info(tenant_id, OB_PUBLIC_SCHEMA_NAME, table_name, obj_db_name, obj_name, synonym_id, database_id, exists));
        if (OB_SUCC(ret) && exists) {
          if (prev_table_name == obj_name) {
            ret = OB_TABLE_NOT_EXIST;
          } else {
            synonym_checker.set_synonym(true);
            OZ (synonym_checker.add_synonym_id(synonym_id, database_id));
            object_db_name = obj_db_name;
            OZ (SMART_CALL(get_object_type(tenant_id,
                                          obj_db_name,
                                          obj_name,
                                          object_type,
                                          object_id,
                                          object_db_name,
                                          is_directory,
                                          false,
                                          table_name,
                                          synonym_checker)));
          }
        }
      }
    }

  } else {
    // if directory, support it's a directory, to do after create directory
    uint64_t dir_id = 0;
    OZ (get_directory_id(tenant_id, table_name, dir_id));
    OX (object_type = ObObjectType::DIRECTORY);
    OX (object_id = dir_id);
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("get_object_type failed", K(tenant_id), K(database_name), K(table_name), K(ret));
  }
  return ret;
}

/* 检查grant role priv 语句的权限（系统权限部分）*/
int ObSchemaChecker::check_ora_grant_role_priv(
    const uint64_t tenant_id,
    const uint64_t user_id,
    const ObSEArray<uint64_t, 4> &role_granted_id_array,
    const ObIArray<uint64_t> &role_id_array)
{
  int ret = OB_SUCCESS;
  if (is_ora_priv_check()) {
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      LOG_WARN("schema checker is not inited", K(is_inited_), K(ret));
    } else if (schema_mgr_ == NULL) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      OZ (ObOraSysChecker::check_ora_grant_role_priv(*schema_mgr_,
                                                     tenant_id,
                                                     user_id,
                                                     role_granted_id_array,
                                                     role_id_array));
    }
  }
  return ret;
}

/* 检查grant sys priv 语句的权限（系统权限部分）*/
int ObSchemaChecker::check_ora_grant_sys_priv(
    const uint64_t tenant_id,
    const uint64_t user_id,
    const share::ObRawPrivArray &sys_priv_array,
    const ObIArray<uint64_t> &role_id_array)
{
  int ret = OB_SUCCESS;
  if (is_ora_priv_check()) {
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      LOG_WARN("schema checker is not inited", K(is_inited_), K(ret));
    } else if (schema_mgr_ == NULL) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      OZ (ObOraSysChecker::check_ora_grant_sys_priv(*schema_mgr_,
                                                    tenant_id,
                                                    user_id,
                                                    sys_priv_array,
                                                    role_id_array));
    }
  }
  return ret;
}

/* 检查grant obj 语句的权限（对象部分） */
int ObSchemaChecker::check_ora_grant_obj_priv(
    const uint64_t tenant_id,
    const uint64_t user_id,
    const common::ObString &db_name,
    const uint64_t obj_id,
    const uint64_t obj_type,
    const share::ObRawObjPrivArray &table_priv_array,
    const ObSEArray<uint64_t, 4> &ins_col_ids,
    const ObSEArray<uint64_t, 4> &upd_col_ids,
    const ObSEArray<uint64_t, 4> &ref_col_ids,
    uint64_t &grantor_id_out,
    const ObIArray<uint64_t> &role_id_array)
{
  int ret = OB_SUCCESS;
  if (is_ora_priv_check()) {
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      LOG_WARN("schema checker is not inited", K(is_inited_), K(ret));
    } else if (schema_mgr_ == NULL) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      OZ (ObOraSysChecker::check_ora_grant_obj_priv(*schema_mgr_,
                                                    tenant_id,
                                                    user_id,
                                                    db_name,
                                                    obj_id,
                                                    obj_type,
                                                    table_priv_array,
                                                    ins_col_ids,
                                                    upd_col_ids,
                                                    ref_col_ids,
                                                    grantor_id_out,
                                                    role_id_array));
    }
  }
  return ret;
}

/* oracle模式，检查create table语句的权限。
1. 检查用户是否有create any table权限
2. 否则，当db_name=user_name时，检查用户是否有create table权限
*/
int ObSchemaChecker::check_ora_ddl_priv(
    const uint64_t tenant_id,
    const uint64_t user_id,
    const common::ObString &database_name,
    const sql::stmt::StmtType stmt_type,
    const ObIArray<uint64_t> &role_id_array)
{
  int ret = OB_SUCCESS;
  if (is_ora_priv_check()) {
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      LOG_WARN("schema checker is not inited", K(is_inited_), K(ret));
    } else if (schema_mgr_ == NULL) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      if (is_ora_lbacsys_user(user_id) && is_lbca_op()) {
        // need not check
      } else {
        OZ (ObOraSysChecker::check_ora_ddl_priv(*schema_mgr_,
                                                tenant_id,
                                                user_id,
                                                database_name,
                                                stmt_type,
                                                role_id_array),
          K(tenant_id), K(user_id), K(database_name), K(stmt_type), K(role_id_array));
      }
    }
  }
  return ret;
}

/**检查用户user_id是否能access到obj_id，会检查系统权限和对象权限*/
int ObSchemaChecker::check_access_to_obj(
    const uint64_t tenant_id,
    const uint64_t user_id,
    const uint64_t obj_id,
    const sql::stmt::StmtType stmt_type,
    const ObIArray<uint64_t> &role_id_array,
    bool &accessible,
    bool is_sys_view)
{
  int ret = OB_SUCCESS;
  accessible = false;
  if (is_ora_priv_check()) {
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      LOG_WARN("schema checker is not inited", K(is_inited_), K(ret));
    } else if (OB_ISNULL(schema_mgr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema_mgr_ is NULL", K(ret), K(tenant_id),
          K(user_id), K(obj_id));
    } else {
      if (is_ora_lbacsys_user(user_id) && is_lbca_op()) {
        accessible = true;
      } else {
        switch (stmt_type) {
          case stmt::T_CREATE_VIEW: {
            OZ (ObOraSysChecker::check_access_to_obj(*schema_mgr_,
                                                    tenant_id,
                                                    user_id,
                                                    PRIV_ID_CREATE_ANY_VIEW,
                                                    static_cast<uint64_t>
                                                    (share::schema::ObObjectType::TABLE),
                                                    obj_id,
                                                    role_id_array,
                                                    accessible),
              K(tenant_id), K(user_id), K(stmt_type), K(role_id_array));
            break;
          }
          case stmt::T_SHOW_COLUMNS: {
            share::ObRawPriv p1 = is_sys_view ?
                                  PRIV_ID_SELECT_ANY_DICTIONARY: PRIV_ID_SELECT_ANY_TABLE;
            OZ (ObOraSysChecker::check_access_to_obj(*schema_mgr_,
                                                    tenant_id,
                                                    user_id,
                                                    p1,
                                                    static_cast<uint64_t>
                                                    (share::schema::ObObjectType::TABLE),
                                                    obj_id,
                                                    role_id_array,
                                                    accessible),
              K(tenant_id), K(user_id), K(stmt_type), K(role_id_array));
            break;
          }
          default: {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexcepted stmt_type", K(ret), K(stmt_type));
            break;
          }
        }
      }
    }
  }
  return ret;
}

/* 对于一些ddl，系统权限可以，对象权限也可以。
例如：alter table
     create index
     flashback table from recyclebin */
int ObSchemaChecker::check_ora_ddl_priv(
    const uint64_t tenant_id,
    const uint64_t user_id,
    const common::ObString &database_name,
    const uint64_t obj_id,
    const uint64_t obj_type,
    const sql::stmt::StmtType stmt_type,
    const ObIArray<uint64_t> &role_id_array)
{
  int ret = OB_SUCCESS;
  if (is_ora_priv_check()) {
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      LOG_WARN("schema checker is not inited", K(is_inited_), K(ret));
    } else if (schema_mgr_ == NULL) {
      ret = OB_ERR_UNEXPECTED;
    } else if (is_ora_lbacsys_user(user_id) && is_lbca_op()) {
      // need not check
    } else {
      OZ (ObOraSysChecker::check_ora_ddl_priv(*schema_mgr_,
                                              tenant_id,
                                              user_id,
                                              database_name,
                                              obj_id,
                                              obj_type,
                                              stmt_type,
                                              role_id_array),
        K(tenant_id), K(user_id), K(database_name), K(obj_id), K(obj_type), K(stmt_type));
    }
  }
  return ret;
}

// 检查列权限， Oracle模式下，仅支持 insert， update 和 references 的列权限。
// 前两者属于DML，权限检查在 check_privilege_new中统一进行。
// references属于DDL，在这里单独为references创建一个权限检查函数
int ObSchemaChecker::check_ora_ddl_ref_priv(
    const uint64_t tenant_id,
    const ObString &user_name,
    const ObString &database_name,
    const ObString &table_name,
    const ObIArray<ObString>  &column_name_array,
    const uint64_t obj_type,
    const sql::stmt::StmtType stmt_type,
    const ObIArray<uint64_t> &role_id_array)
{
  int ret = OB_SUCCESS;
  // 对于 references 权限来说，要检查 schema 拥有者的权限，而不是当前用户的权限
  uint64_t user_id = OB_INVALID_ID;
  if (is_ora_priv_check()) {
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      LOG_WARN("schema checker is not inited", K(is_inited_), K(ret));
    } else if (OB_ISNULL(schema_mgr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema_mgr is nulll", K(ret));
    } else if (OB_FAIL(get_user_id(tenant_id,
                              user_name,
                              ObString(OB_DEFAULT_HOST_NAME),
                              user_id))) {
      LOG_WARN("get_user_id failed", K(ret), K(user_name));
    } else if (is_ora_lbacsys_user(user_id) && is_lbca_op()) {
      // need not check
    } else {
      uint64_t table_id = OB_INVALID_ID;
      ObArray<uint64_t> col_id_array;
      const ObTableSchema *table_schema = NULL;
      const ObColumnSchemaV2 *col_schema = NULL;
      OZ (get_table_schema(tenant_id, database_name, table_name, false, table_schema),
          K(database_name), K(table_name));
      CK (OB_NOT_NULL(table_schema));
      OX (table_id = table_schema->get_table_id());
      for (int i = 0; OB_SUCC(ret) && i < column_name_array.count(); ++i) {
        const ObString &col_name = column_name_array.at(i);
        OX (col_schema = table_schema->get_column_schema(col_name));
        CK (OB_NOT_NULL(col_schema));
        OZ (col_id_array.push_back(col_schema->get_column_id()));
      }
      OZ (ObOraSysChecker::check_ora_ddl_ref_priv(*schema_mgr_,
                                              tenant_id,
                                              user_id,
                                              database_name,
                                              table_id,
                                              col_id_array,
                                              obj_type,
                                              stmt_type,
                                              role_id_array),
        K(tenant_id), K(user_id), K(database_name),
        K(table_id), K(col_id_array), K(obj_type), K(stmt_type));
    }
  }
  return ret;
}

bool ObSchemaChecker::is_ora_priv_check()
{
  if (lib::is_oracle_mode() && GCONF._enable_oracle_priv_check)
    return true;
  else
    return false;
}

}//end of namespace sql
}//end of namespace oceanbase
