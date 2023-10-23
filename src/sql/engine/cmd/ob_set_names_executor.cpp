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

#include "sql/engine/cmd/ob_set_names_executor.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#include "lib/string/ob_sql_string.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "observer/ob_server_struct.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

int ObSetNamesExecutor::execute(ObExecContext &ctx, ObSetNamesStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = NULL;
  if (NULL == (session = ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(ERROR, "session is NULL", K(ret), K(ctx));
  } else {
    ObString charset;
    if (stmt.is_default_charset()) {
      // 和mysql兼容，取global的character_set_client值
      if (OB_FAIL(get_global_sys_var_character_set_client(ctx, charset))) {
        SQL_ENG_LOG(WARN, "fail to get global character_set_client", K(ret));
      }
    } else {
      charset = stmt.get_charset();
    }
    if (OB_SUCC(ret)) {
      ObString collation = stmt.get_collation();
      ObCollationType collation_type = CS_TYPE_INVALID;
      ObCharsetType cs_type = ObCharset::charset_type(charset);
      if (CHARSET_INVALID == cs_type) {
        ret = OB_ERR_UNKNOWN_CHARSET;
        LOG_USER_ERROR(OB_ERR_UNKNOWN_CHARSET, charset.length(), charset.ptr());
      } else {
        charset = ObString::make_string(ObCharset::charset_name(cs_type));
        if (stmt.is_default_collation() || !collation.empty()) {
          collation_type = ObCharset::collation_type(collation);
          if (CS_TYPE_INVALID == collation_type) {
            ret = OB_ERR_UNKNOWN_COLLATION;
            LOG_USER_ERROR(OB_ERR_UNKNOWN_COLLATION, collation.length(), collation.ptr());
          } else if (!ObCharset::is_valid_collation(cs_type, collation_type)) {
            ret = OB_ERR_COLLATION_MISMATCH;
            LOG_USER_ERROR(OB_ERR_COLLATION_MISMATCH, collation.length(), collation.ptr(),
                           charset.length(), charset.ptr());
          } else {
            collation = ObString::make_string(ObCharset::collation_name(collation_type));
          }
        } else {
          // the use default collation of this charset
          collation_type = ObCharset::get_default_collation(cs_type);
          collation = ObString::make_string(ObCharset::collation_name(collation_type));
        }
      }
      if (OB_SUCC(ret)) {
        if (!ObCharset::is_valid_connection_collation(collation_type)) {
          ret = OB_NOT_SUPPORTED;
          SQL_ENG_LOG(WARN, "collation type not supported",
                      "charset type", ObCharset::charset_name(cs_type),
                      "collation type", ObCharset::collation_name(collation_type));
        }
      }
      if (OB_SUCC(ret)) {
        if (stmt.is_set_names()) {
          // SET NAMES
          ObCollationType cs_coll_type = ObCharset::get_default_collation(ObCharset::charset_type(charset));
          ObCollationType coll_type = ObCharset::collation_type(collation);
          if (CS_TYPE_INVALID == cs_coll_type || CS_TYPE_INVALID == coll_type) {
            ret = OB_ERR_UNEXPECTED;
            SQL_ENG_LOG(ERROR, "cs coll type or coll type is invalid", K(ret), K(cs_coll_type), K(coll_type));
          } else if (OB_FAIL(sql::ObSQLUtils::is_charset_data_version_valid(ObCharset::charset_type(charset),
                                                                            session->get_effective_tenant_id()))) {
            SQL_EXE_LOG(WARN, "failed to check charset data version valid", K(ret));
          } else if (OB_FAIL(session->update_sys_variable(SYS_VAR_CHARACTER_SET_CLIENT,
                                                          static_cast<int64_t>(cs_coll_type)))) {
            SQL_ENG_LOG(WARN, "failed to update sys var", K(ret));
          } else if (OB_FAIL(session->update_sys_variable(SYS_VAR_CHARACTER_SET_RESULTS,
                                                          static_cast<int64_t>(cs_coll_type)))) {
            SQL_ENG_LOG(WARN, "failed to update sys var", K(ret));
          } else if (OB_FAIL(session->update_sys_variable(SYS_VAR_CHARACTER_SET_CONNECTION,
                                                          static_cast<int64_t>(cs_coll_type)))) {
            SQL_ENG_LOG(WARN, "failed to update sys var", K(ret));
          } else if (OB_FAIL(session->update_sys_variable(SYS_VAR_COLLATION_CONNECTION,
                                                          static_cast<int64_t>(coll_type)))) {
            SQL_ENG_LOG(WARN, "failed to update sys var", K(ret));
          }
        } else {
          // SET CHARACTER SET
          ObObj database_charset;
          ObObj database_collation;
          ObCollationType cs_coll_type = ObCharset::get_default_collation(ObCharset::charset_type(charset));
          if (OB_FAIL(sql::ObSQLUtils::is_charset_data_version_valid(ObCharset::charset_type(charset),
                                                                     session->get_effective_tenant_id()))) {
            SQL_EXE_LOG(WARN, "failed to check charset data version valid", K(ret));
          } else if (OB_FAIL(session->get_sys_variable(SYS_VAR_CHARACTER_SET_DATABASE,
                                                       database_charset))) {
          } else if (OB_FAIL(session->get_sys_variable(SYS_VAR_COLLATION_DATABASE,
                                                       database_collation))) {
          } else {
            ObCollationType collation_connection = static_cast<ObCollationType>(database_collation.get_int());
            ObCharsetType charset_connection = ObCharset::charset_type_by_coll(collation_connection);
            if (!ObCharset::is_valid_connection_collation(collation_connection)) {
              ret = OB_NOT_SUPPORTED;
              SQL_ENG_LOG(WARN, "connection collation type not supported",
                  "charset type", ObCharset::charset_name(charset_connection),
                  "collation type", ObCharset::collation_name(collation_connection));
            }
          }
          if (OB_SUCC(ret)) {
            if (OB_FAIL(session->update_sys_variable(SYS_VAR_CHARACTER_SET_CLIENT,
                                                            static_cast<int64_t>(cs_coll_type)))) {
              SQL_EXE_LOG(WARN, "failed to update sys var", K(ret));
            } else if (OB_FAIL(session->update_sys_variable(SYS_VAR_CHARACTER_SET_RESULTS,
                                                            static_cast<int64_t>(cs_coll_type)))) {
              SQL_EXE_LOG(WARN, "failed to update sys var", K(ret));
            } else if (OB_FAIL(session->update_sys_variable(SYS_VAR_CHARACTER_SET_CONNECTION,
                                                            database_charset))) {
              SQL_EXE_LOG(WARN, "failed to update sys var", K(ret));
            } else if (OB_FAIL(session->update_sys_variable(SYS_VAR_COLLATION_CONNECTION,
                                                            database_collation))) {
              SQL_EXE_LOG(WARN, "failed to update sys var", K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}


int ObSetNamesExecutor::get_global_sys_var_character_set_client(
    ObExecContext &ctx,
    ObString &character_set_client) const
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = NULL;
  ObIAllocator &allocator = ctx.get_allocator();
  ObSchemaGetterGuard schema_guard;
  const ObSysVarSchema *var_schema = NULL;
  ObObj value;
  if (NULL == (session = ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(ERROR, "session is NULL", K(ret), K(ctx));
  } else if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(ERROR, "schema service is null");
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
              session->get_effective_tenant_id(),
              schema_guard))) {
    SQL_ENG_LOG(WARN, "get schema guard failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_system_variable(session->get_effective_tenant_id(),
      SYS_VAR_CHARACTER_SET_CLIENT, var_schema))) {
    SQL_ENG_LOG(WARN, "get tenant system variable failed", K(ret));
  } else if (OB_ISNULL(var_schema)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "var_schema is null");
  } else if (OB_FAIL(var_schema->get_value(&allocator,
                                           ObBasicSessionInfo::create_dtc_params(session),
                                           value))) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "get value from var_schema failed", K(ret), K(*var_schema));
  }  else if (ObIntType != value.get_type()) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(ERROR, "sys var character_set_client's type must be ObIntType", K(ret), K(value));
  } else {
    ObCollationType coll_type = static_cast<ObCollationType>(value.get_int());
    if (!ObCharset::is_valid_collation(coll_type)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "invalid collation type", K(ret), K(coll_type));
    } else {
      const char *cs_name_ptr = ObCharset::charset_name(coll_type);
      character_set_client = ObString(cs_name_ptr);
    }
  }
  return ret;
}

