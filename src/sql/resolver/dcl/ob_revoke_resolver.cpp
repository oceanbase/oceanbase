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
#include "sql/resolver/dcl/ob_revoke_resolver.h"

#include "share/schema/ob_schema_struct.h"
#include "objit/common/ob_item_type.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/resolver/dcl/ob_grant_resolver.h"
#include "sql/resolver/dcl/ob_revoke_stmt.h"
#include "sql/engine/ob_exec_context.h"


using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::share::schema;

ObRevokeResolver::ObRevokeResolver(ObResolverParams &params)
    : ObDCLResolver(params)
{
}

ObRevokeResolver::~ObRevokeResolver()
{
}

/* 解析revoke role from ur */
int ObRevokeResolver::resolve_revoke_role_inner(
    const ParseNode *revoke_role,
    ObRevokeStmt *revoke_stmt)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = OB_INVALID_ID;
  ObSEArray<uint64_t, 4> role_id_array;
  ObArray<ObString> role_user_name;
  ObArray<ObString> role_host_name;
  
  CK (revoke_role != NULL && revoke_stmt != NULL);
  CK ((2 == revoke_role->num_child_ || 4 == revoke_role->num_child_) &&
      NULL != revoke_role->children_[0] &&
      NULL != revoke_role->children_[1]);
  bool ignore_unknown_role = false;
  bool ignore_unknown_user = false;
  bool ignore_error = false;
  tenant_id = revoke_stmt->get_tenant_id();
  if (lib::is_mysql_mode() && 4 == revoke_role->num_child_) {
    ignore_unknown_role = NULL != revoke_role->children_[2];
    ignore_unknown_user = NULL != revoke_role->children_[3];
  }
  if (lib::is_mysql_mode()) {
    OZ (ObSQLUtils::compatibility_check_for_mysql_role_and_column_priv(tenant_id));
  }
  // 1. resolve role list
  ParseNode *role_list = revoke_role->children_[0];
  for (int i = 0; OB_SUCC(ret) && i < role_list->num_child_; ++i) {
    const ObUserInfo *role_info = NULL;
    uint64_t role_id = OB_INVALID_ID;
    ParseNode *role = role_list->children_[i];
    if (NULL == role) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("role node is null", K(ret));
    } else {
      ObString role_name;
      ObString host_name(OB_DEFAULT_HOST_NAME);
      if (lib::is_oracle_mode()) {
        role_name.assign_ptr(const_cast<char *>(role->str_value_),
            static_cast<int32_t>(role->str_len_));
      } else {
        OZ (resolve_user_host(role, role_name, host_name));
      }

      OZ (params_.schema_checker_->get_user_info(tenant_id,
                                                 role_name,
                                                 host_name,
                                                 role_info), role_name, host_name);
      if (OB_USER_NOT_EXIST == ret || OB_ISNULL(role_info) || (lib::is_oracle_mode() && !role_info->is_role())) {
        if (lib::is_oracle_mode()) {
          ret = OB_ROLE_NOT_EXIST;
          LOG_USER_ERROR(OB_ROLE_NOT_EXIST, role_name.length(), role_name.ptr());
        } else {
          ret = OB_ERR_UNKNOWN_AUTHID;
          ignore_error = ignore_unknown_role;
          LOG_USER(ignore_unknown_role ? ObLogger::USER_WARN : ObLogger::USER_ERROR,
                   OB_ERR_UNKNOWN_AUTHID,
                   role_name.length(), role_name.ptr(),
                   host_name.length(), host_name.ptr());
        }
      } else {
        role_id = role_info->get_user_id();
        if (OB_FAIL(revoke_stmt->add_role(role_id))) {
          if (OB_PRIV_DUP == ret && lib::is_mysql_mode()) {
            //ignored
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("failed to add role", K(ret));
          }
        } else {
          OZ (role_id_array.push_back(role_id));
          OZ (role_user_name.push_back(role_name));
          OZ (role_host_name.push_back(host_name));
        }
      }
    }
  }
  // 2. check privilege
  if (OB_SUCC(ret)) {
    if (ObSchemaChecker::is_ora_priv_check()) {
      OZ (params_.schema_checker_->check_ora_grant_role_priv(
                                    revoke_stmt->get_tenant_id(),
                                    params_.session_info_->get_priv_user_id(),
                                    role_id_array,
                                    params_.session_info_->get_enable_role_array()),
          revoke_stmt->get_tenant_id(), revoke_stmt->get_roles(), role_id_array);
    } else {
      ObSqlCtx *sql_ctx = NULL;
      if (OB_ISNULL(params_.session_info_->get_cur_exec_ctx())
          || OB_ISNULL(sql_ctx = params_.session_info_->get_cur_exec_ctx()->get_sql_ctx())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected ctx", K(ret), KP(params_.session_info_->get_cur_exec_ctx()));
      }
      OZ (params_.schema_checker_->check_mysql_grant_role_priv(*sql_ctx, role_id_array));
    }
  }

  // 3. resolve grantee
  uint64_t user_id = OB_INVALID_ID;
  const ObUserInfo *user_info = NULL;
  ObSArray<ObString> user_name_array;
  ObSArray<ObString> host_name_array;
  OZ (ObGrantResolver::resolve_grantee_clause(revoke_role->children_[1], 
                                              params_.session_info_,
                                              user_name_array, 
                                              host_name_array));
  CK (user_name_array.count() == host_name_array.count());
  for (int i = 0; OB_SUCC(ret) && i < user_name_array.count(); ++i) {
    const ObString &user_name = user_name_array.at(i);
    const ObString &host_name = host_name_array.at(i);
    if (OB_FAIL(params_.schema_checker_->get_user_id(tenant_id, user_name, host_name, user_id))) {
      if (OB_USER_NOT_EXIST == ret) {
        if (lib::is_oracle_mode()) {
          ret = OB_ERR_USER_OR_ROLE_DOES_NOT_EXIST;
          LOG_USER_ERROR(OB_ERR_USER_OR_ROLE_DOES_NOT_EXIST, user_name.length(), user_name.ptr());
        } else {
          ret = OB_ERR_UNKNOWN_AUTHID;
          ignore_error = ignore_unknown_user;
          LOG_USER(ignore_unknown_user ? ObLogger::USER_WARN : ObLogger::USER_ERROR,
                   OB_ERR_UNKNOWN_AUTHID,
                   user_name.length(), user_name.ptr(),
                   host_name.length(), host_name.ptr());
        }
      }
      LOG_WARN("fail to get user id", K(ret), K(user_name), K(host_name));
    } else if (OB_FAIL(check_dcl_on_inner_user(revoke_role->type_,
                                               params_.session_info_->get_priv_user_id(),
                                               user_id))) {
      LOG_WARN("failed to check dcl on inner-user or unsupport to modify reserved user", K(ret),
               K(session_info_->get_priv_user_id()), K(user_name));
    }
    OZ (revoke_stmt->add_grantee(user_name));
    OZ (params_.schema_checker_->get_user_info(tenant_id, user_id, user_info), user_id);
    OZ (revoke_stmt->add_user(user_id));
    //4. check user has roles
    if (OB_SUCC(ret)) {
      CK (OB_NOT_NULL(user_info));
      for (int i = 0; OB_SUCC(ret) && i < role_id_array.count(); i++) {
        if (!has_exist_in_array(user_info->get_role_id_array(), role_id_array.at(i))) {
          if (lib::is_oracle_mode()) {
            ret = OB_ERR_ROLE_NOT_GRANTED_TO;
            LOG_USER_ERROR(OB_ERR_ROLE_NOT_GRANTED_TO,
                           static_cast<int32_t>(role_list->children_[i]->str_len_),
                           role_list->children_[i]->str_value_,
                           0, "",
                           user_info->get_user_name_str().length(),
                           user_info->get_user_name_str().ptr(),
                           0, "");
          } else {
            //ignored
          }
        }
      }
    }
  }
  // 5. set grant level
  if (OB_SUCC(ret)) {
    // roles_: 必须大于等于0
    if ((revoke_stmt->get_roles()).count() < 1) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("role argument is invalid", K(ret));
    } else {
      // role当作user处理
      revoke_stmt->set_grant_level(OB_PRIV_USER_LEVEL);
    }
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(revoke_stmt) && ignore_error) {
    revoke_stmt->set_has_warning();
    ret = OB_SUCCESS;
  }

  return ret;
}

/*
revoke XXX from u/r;

1. 先check XXX 是否存在
2. 再check 是否有相应的privilege
3. 最后check u/r的存在
*/
int ObRevokeResolver::resolve_revoke_sysprivs_inner(
    const ParseNode *revoke_role,
    ObRevokeStmt *revoke_stmt)
{
  int ret = OB_SUCCESS;
  share::ObRawPrivArray sys_priv_array;
  ObString user_name;
  ObString host_name(OB_DEFAULT_HOST_NAME);
  uint64_t user_id;
  const ObUserInfo *user_info = NULL;

  CK (OB_NOT_NULL(params_.schema_checker_) &&  OB_NOT_NULL(params_.session_info_));
  CK (revoke_role != NULL && revoke_stmt != NULL);
  // 1. resolve priv list 
  OZ (ObGrantResolver::resolve_sys_privs(revoke_role->children_[0], sys_priv_array));
  OZ (revoke_stmt->set_priv_array(sys_priv_array));
  
  // 2. check privilege
  if (OB_SUCC(ret) && ObSchemaChecker::is_ora_priv_check()) {
    OZ (params_.schema_checker_->check_ora_grant_sys_priv(
                                  revoke_stmt->get_tenant_id(),
                                  params_.session_info_->get_priv_user_id(),
                                  sys_priv_array,
                                  params_.session_info_->get_enable_role_array()),
      revoke_stmt->get_tenant_id(), params_.session_info_->get_priv_user_id(), sys_priv_array);
  }
  // 3. resolve grantee
  ObSArray<ObString> user_name_array;
  ObSArray<ObString> host_name_array;
  OZ (ObGrantResolver::resolve_grantee_clause(revoke_role->children_[1], 
                                              params_.session_info_,
                                              user_name_array, 
                                              host_name_array));
  CK (user_name_array.count() == host_name_array.count());
  for (int i = 0; OB_SUCC(ret) && i < user_name_array.count(); ++i) {
    const ObString &user_name = user_name_array.at(i);
    const ObString &host_name = host_name_array.at(i);
    OZ (revoke_stmt->add_grantee(user_name));
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(params_.schema_checker_->get_user_id(revoke_stmt->get_tenant_id(), user_name,
                                                            host_name, user_id))) {
      if (OB_USER_NOT_EXIST == ret) {
        ret = OB_ERR_USER_OR_ROLE_DOES_NOT_EXIST;
        LOG_USER_ERROR(OB_ERR_USER_OR_ROLE_DOES_NOT_EXIST, user_name.length(), user_name.ptr());
      }
      LOG_WARN("fail to get user id", K(ret), K(user_name), K(host_name));
    }  else if (OB_FAIL(check_dcl_on_inner_user(revoke_role->type_,
                                               params_.session_info_->get_priv_user_id(),
                                               user_id))) {
      LOG_WARN("failed to check dcl on inner-user or unsupport to modify reserved user", K(ret),
               K(session_info_->get_priv_user_id()), K(user_name));
    }
    OZ (params_.schema_checker_->get_user_info(
        revoke_stmt->get_tenant_id(), user_id, user_info), user_id);
    OZ (revoke_stmt->add_user(user_id));
  }

  // 4. set grant level
  OX (revoke_stmt->set_grant_level(OB_PRIV_SYS_ORACLE_LEVEL));
  return ret;
}

int ObRevokeResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  CHECK_COMPATIBILITY_MODE(session_info_);
  if (lib::is_oracle_mode()) {
    ret = resolve_ora(parse_tree);
  } else {
    ret = resolve_mysql(parse_tree);
  }
  return ret;
}

int ObRevokeResolver::resolve_mysql(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  static const int REVOKE_NUM_CHILD = 6;
  static const int REVOKE_ALL_NUM_CHILD = 3;
  static const int REVOKE_ROLE_NUM_CHILD = 1;
  ParseNode *node = const_cast<ParseNode*>(&parse_tree);
  ObRevokeStmt *revoke_stmt = NULL;
  if (OB_ISNULL(params_.schema_checker_) || OB_ISNULL(params_.session_info_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema_checker or session info not inited",
        K(ret), "schema checker", params_.schema_checker_, "session info", params_.session_info_);
  } else if (node != NULL 
      && ((T_REVOKE == node->type_ && REVOKE_NUM_CHILD == node->num_child_)
        || (T_REVOKE_ALL == node->type_ && REVOKE_ALL_NUM_CHILD == node->num_child_)
        || (T_SYSTEM_REVOKE == node->type_ && REVOKE_ROLE_NUM_CHILD == node->num_child_))) {
    if (OB_ISNULL(revoke_stmt = create_stmt<ObRevokeStmt>())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("Failed to create ObCreateUserStmt", K(ret));
    } else {
      revoke_stmt->set_stmt_type(stmt::T_REVOKE);
      stmt_ = revoke_stmt;
      uint64_t tenant_id = params_.session_info_->get_effective_tenant_id();
      revoke_stmt->set_tenant_id(tenant_id);
      ParseNode *users_node =  NULL;
      ParseNode *privs_node = NULL;
      ObPrivLevel grant_level = OB_PRIV_INVALID_LEVEL;
      if (T_SYSTEM_REVOKE == node->type_ && REVOKE_ROLE_NUM_CHILD == node->num_child_) {
        // resolve oracle revoke
        // 0: role_list; 1: grantee
        ParseNode *revoke_role = node->children_[0];
        if (NULL == revoke_role) {
          ret = OB_INVALID_ARGUMENT;
          LOG_ERROR("revoke ParseNode error", K(ret));
        } else if (T_REVOKE_ROLE == revoke_role->type_) {
          revoke_stmt->set_stmt_type(stmt::T_REVOKE_ROLE);
          OZ (resolve_revoke_role_inner(revoke_role, revoke_stmt));
        } else if (T_REVOKE_SYSAUTH == revoke_role->type_) {
          revoke_stmt->set_stmt_type(stmt::T_SYSTEM_REVOKE);
          OZ (resolve_revoke_sysprivs_inner(revoke_role, revoke_stmt));
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_ERROR("Revoke ParseNode error", K(ret));
        }
      } else {
        bool ignore_priv_not_exist = false;
        bool ignore_user_not_exist = false;
        bool ignore_error = false;
        // resolve mysql revoke
        if (T_REVOKE == node->type_ && REVOKE_NUM_CHILD == node->num_child_) {
          privs_node = node->children_[0];
          ParseNode *priv_object_node = node->children_[1];
          ParseNode *priv_level_node = node->children_[2];
          users_node = node->children_[3];
          ignore_priv_not_exist = NULL != node->children_[4];
          ignore_user_not_exist = NULL != node->children_[5];
          //resolve priv_level
          if (OB_ISNULL(priv_level_node) || OB_ISNULL(allocator_)) {
            ret = OB_ERR_PARSE_SQL;
            LOG_WARN("Priv level node should not be NULL", K(ret));
          } else {
            ObString db = ObString::make_string("");
            ObString table = ObString::make_string("");
            if (OB_FAIL(ObGrantResolver::resolve_priv_level(
                        params_.schema_checker_->get_schema_guard(),
                        session_info_,
                        priv_level_node,
                        params_.session_info_->get_database_name(), 
                        db, 
                        table, 
                        grant_level,
                        *allocator_))) {
              LOG_WARN("Resolve priv_level node error", K(ret));
            }

            if (OB_FAIL(ret)) {
            } else if (OB_FAIL(check_and_convert_name(db, table))) {
              LOG_WARN("Check and convert name error", K(db), K(table), K(ret));
            } else {
              revoke_stmt->set_grant_level(grant_level);
              if (OB_FAIL(revoke_stmt->set_database_name(db))) {
                LOG_WARN("Failed to set database_name to revoke_stmt", K(ret));
              } else if (OB_FAIL(revoke_stmt->set_table_name(table))) {
                LOG_WARN("Failed to set table_name to revoke_stmt", K(ret));
              } else {
                share::schema::ObObjectType object_type = share::schema::ObObjectType::INVALID;
                uint64_t object_id = OB_INVALID_ID;
                ObString object_db_name;
                if (db.empty() || table.empty()) {
                  object_type = share::schema::ObObjectType::MAX_TYPE;
                } else {
                  ObSynonymChecker synonym_checker;
                  (void)params_.schema_checker_->get_object_type(tenant_id, db, table,
                                                                object_type, object_id,
                                                                object_db_name, false,
                                                                false, ObString(""),
                                                                synonym_checker);
                }
                if (OB_FAIL(ret)) {
                } else {
                  revoke_stmt->set_object_type(object_type);
                  revoke_stmt->set_object_id(object_id);
                }
              }
            }
          }

          if (OB_FAIL(ret)) {
          } else if (priv_object_node != NULL) {
            uint64_t compat_version = 0;
            if (grant_level != OB_PRIV_TABLE_LEVEL) {
              ret = OB_ILLEGAL_GRANT_FOR_TABLE;
              LOG_WARN("illegal grant", K(ret));
            } else if (priv_object_node->value_ == 1) {
              grant_level = OB_PRIV_TABLE_LEVEL;
            } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
              LOG_WARN("fail to get data version", K(tenant_id));
            } else if (!sql::ObSQLUtils::is_data_version_ge_422_or_431(compat_version)) {
              ret = OB_NOT_SUPPORTED;
              LOG_WARN("grammar is not support when MIN_DATA_VERSION is below DATA_VERSION_4_3_1_0 or 4_2_2_0", K(ret));
            } else if (priv_object_node->value_ == 2) {
              grant_level = OB_PRIV_ROUTINE_LEVEL;
              revoke_stmt->set_object_type(ObObjectType::PROCEDURE);
            } else if (priv_object_node->value_ == 3) {
              grant_level = OB_PRIV_ROUTINE_LEVEL;
              revoke_stmt->set_object_type(ObObjectType::FUNCTION);
            }
            revoke_stmt->set_grant_level(grant_level);
          }

        } else if (T_REVOKE_ALL == node->type_ && REVOKE_ALL_NUM_CHILD == node->num_child_) {
          ignore_priv_not_exist = NULL != node->children_[1];
          ignore_user_not_exist = NULL != node->children_[2];
          users_node = node->children_[0];
          revoke_stmt->set_revoke_all(true);
          revoke_stmt->set_grant_level(OB_PRIV_USER_LEVEL);
          if (OB_SUCC(ret)) {
            ObSessionPrivInfo session_priv;
            ObArenaAllocator alloc;
            ObStmtNeedPrivs stmt_need_privs(alloc);
            ObNeedPriv need_priv("mysql", "", OB_PRIV_DB_LEVEL, OB_PRIV_UPDATE, false);
            OZ (stmt_need_privs.need_privs_.init(1));
            OZ (stmt_need_privs.need_privs_.push_back(need_priv));
            //check CREATE USER or UPDATE privilege on mysql
            params_.session_info_->get_session_priv_info(session_priv);
            if (OB_SUCC(ret) && OB_FAIL(schema_checker_->check_priv(session_priv, stmt_need_privs))) {
              stmt_need_privs.need_privs_.at(0) =
                  ObNeedPriv("", "", OB_PRIV_USER_LEVEL, OB_PRIV_CREATE_USER, false);
              if (OB_FAIL(schema_checker_->check_priv(session_priv, stmt_need_privs))) {
                LOG_WARN("no priv", K(ret));
              }
            }
          }
        }
        //resolve privileges
        if (OB_SUCC(ret) && (NULL != privs_node)) {
          ObPrivSet priv_set = 0;
          uint64_t compat_version = 0;
          const uint64_t tenant_id = params_.session_info_->get_effective_tenant_id();
          if (OB_ISNULL(allocator_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected error", K(ret));
          } else if (OB_FAIL(ObGrantResolver::resolve_priv_set(tenant_id, privs_node, grant_level, priv_set, revoke_stmt,
                                                        params_.schema_checker_, params_.session_info_,
                                                        *allocator_))) {
            LOG_WARN("Resolve priv set error", K(ret));
          } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
            LOG_WARN("fail to get data version", K(tenant_id));
          } else if (!sql::ObSQLUtils::is_data_version_ge_422_or_431(compat_version)
                     && ((priv_set & OB_PRIV_EXECUTE) != 0 ||
                         (priv_set & OB_PRIV_ALTER_ROUTINE) != 0 ||
                         (priv_set & OB_PRIV_CREATE_ROUTINE) != 0)) {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("grammar is not support when MIN_DATA_VERSION is below DATA_VERSION_4_3_1_0 or 4_2_2_0", K(ret));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "revoke execute/alter routine/create routine privilege");
          } else if (!sql::ObSQLUtils::is_data_version_ge_423_or_432(compat_version)
                     && ((priv_set & OB_PRIV_CREATE_TABLESPACE) != 0 ||
                         (priv_set & OB_PRIV_SHUTDOWN) != 0 ||
                         (priv_set & OB_PRIV_RELOAD) != 0)) {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("grammar is not support when MIN_DATA_VERSION is below DATA_VERSION_4_2_3_0 or 4_3_2_0", K(ret));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "revoke create tablespace/shutdown/reload privilege");
          }
          if (OB_FAIL(ret)) {
          } else {
            revoke_stmt->set_priv_set(priv_set);
          }
        }

        //resolve users_node
        if (OB_SUCC(ret)) {
          if (OB_ISNULL(users_node)) {
            ret = OB_ERR_PARSE_SQL;
            LOG_WARN("Users node should not be NULL", K(ret));
          } else {
            for (int i = 0; OB_SUCC(ret) && i < users_node->num_child_; ++i) {
              ParseNode *user_hostname_node = users_node->children_[i];
              if (OB_ISNULL(user_hostname_node)) {
                ret = OB_ERR_PARSE_SQL;
                LOG_WARN("the child of users should not be NULL", K(ret), K(i));
              } else if (2 != user_hostname_node->num_child_) {
                ret = OB_INVALID_ARGUMENT;
                LOG_WARN("sql_parser parse user error", K(ret));
              } else if (OB_ISNULL(user_hostname_node->children_[0])) {
                // 0: user, 1: hostname
                ret = OB_ERR_PARSE_SQL;
                LOG_WARN("The child of user node should not be NULL", K(ret), K(i));
              } else {
                uint64_t user_id = OB_INVALID_ID;
                //0: user name; 1: host name
                ObString user_name;
                ObString host_name;
                if (user_hostname_node->children_[0]->type_ == T_FUN_SYS_CURRENT_USER) {
                  user_name = params_.session_info_->get_user_name();
                } else {
                  user_name = ObString(static_cast<int32_t>(user_hostname_node->children_[0]->str_len_),
                                   user_hostname_node->children_[0]->str_value_);
                }
                if (NULL == user_hostname_node->children_[1]) {
                  if (user_hostname_node->children_[0]->type_ == T_FUN_SYS_CURRENT_USER) {
                    host_name = params_.session_info_->get_host_name();
                  } else {
                    host_name.assign_ptr(OB_DEFAULT_HOST_NAME,
                                       static_cast<int32_t>(STRLEN(OB_DEFAULT_HOST_NAME)));
                  }
                } else {
                  host_name.assign_ptr(user_hostname_node->children_[1]->str_value_,
                            static_cast<int32_t>(user_hostname_node->children_[1]->str_len_));
                }
                if (user_name.length() > OB_MAX_USER_NAME_LENGTH) {
                  ret = OB_WRONG_USER_NAME_LENGTH;
                  LOG_USER_ERROR(OB_WRONG_USER_NAME_LENGTH, user_name.length(), user_name.ptr());
                } else if (OB_FAIL(revoke_stmt->add_grantee(user_name))) {
                  SQL_RESV_LOG(WARN, "fail to add grantee", K(ret), K(user_name), K(host_name));
                } else if (OB_FAIL(
                    params_.schema_checker_->get_user_id(tenant_id, user_name, 
                                                         host_name, user_id))) {
                  if (OB_USER_NOT_EXIST == ret) {
                     ignore_error = ignore_user_not_exist;
                     LOG_USER(ignore_user_not_exist ? ObLogger::USER_WARN : ObLogger::USER_ERROR,
                              OB_ERR_UNKNOWN_AUTHID,
                              user_name.length(), user_name.ptr(),
                              host_name.length(), host_name.ptr());
                  }
                  SQL_RESV_LOG(WARN, "fail to get user id", K(ret), K(user_name), K(host_name));
                } else if (is_root_user(user_id) && OB_PRIV_USER_LEVEL == grant_level) {
                  ret = OB_NOT_SUPPORTED;
                  LOG_WARN("Revoke privilege from root at global level is not supported", K(ret));
                  LOG_USER_ERROR(OB_NOT_SUPPORTED, "Revoke privilege from root at global level");
                } else if (OB_FAIL(check_dcl_on_inner_user(node->type_,
                                                          params_.session_info_->get_priv_user_id(),
                                                          user_id))) {
                  LOG_WARN("failed to check dcl on inner-user or unsupport to modify reserved user",
                           K(ret), K(session_info_->get_priv_user_id()), K(user_name));
                } else if (OB_FAIL(revoke_stmt->add_user(user_id))) {
                  LOG_WARN("Add user to grant_stmt error", K(ret), K(user_id));
                } else {
                  //do nothing
                }
              }
            } //end for
          }
        }

        if (OB_FAIL(ret) && OB_NOT_NULL(revoke_stmt) && ignore_error) {
          revoke_stmt->set_has_warning();
          ret = OB_SUCCESS;
        }
      }
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("Revoke ParseNode error", K(ret));
  }
  return ret;
}

// 兼容2_2_1_release的备份功能，需要支持 grant create user, create view, create synonym on *.* to user;
// 因此当priv_type为这三种情况时，将其从系统权限转换为对象权限再做处理
int ObRevokeResolver::trans_ora_sys_priv_to_obj(ParseNode *priv_type_node)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(priv_type_node)
      && T_ORACLE_SYS_PRIV_TYPE == priv_type_node->type_) {
    if (PRIV_ID_CREATE_USER == priv_type_node->value_) {
      priv_type_node->type_ = T_PRIV_TYPE;
      priv_type_node->value_ = OB_PRIV_CREATE_USER;
    } else if (PRIV_ID_CREATE_VIEW == priv_type_node->value_) {
      priv_type_node->type_ = T_PRIV_TYPE;
      priv_type_node->value_ = OB_PRIV_CREATE_VIEW;
    } else if (PRIV_ID_CREATE_SYN == priv_type_node->value_) {
      priv_type_node->type_ = T_PRIV_TYPE;
      priv_type_node->value_ = OB_PRIV_CREATE_SYNONYM;
    }
  }
  return ret;
}

int ObRevokeResolver::resolve_priv_set_ora(
    const ParseNode *privs_node,
    ObPrivLevel grant_level,
    ObPrivSet &priv_set,
    share::ObRawObjPrivArray &obj_priv_array,
    bool &revoke_all_ora)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(privs_node)) { // privs_node is role_sys_obj_all_col_priv_list
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument, priv_node_list should not be NULL", K(privs_node), K(ret));
  } else if (OB_PRIV_INVALID_LEVEL == grant_level) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument, grant_level should not be invalid", K(grant_level), K(ret));
  } else {
    obj_priv_array.reset();
    revoke_all_ora = false;
    for (int i = 0; i < privs_node->num_child_ && OB_SUCCESS == ret; ++i) {
      ParseNode *role_sys_obj_all_col_priv_node = NULL;
      ParseNode *priv_node = NULL;
      if (OB_NOT_NULL(role_sys_obj_all_col_priv_node = privs_node->children_[i]) 
          && T_ORA_PRIV_TYPE == privs_node->children_[i]->type_
          && OB_NOT_NULL(priv_node = role_sys_obj_all_col_priv_node->children_[0])) {
        OZ (trans_ora_sys_priv_to_obj(priv_node)); // For compatibility with 2_2_1 on backup
        const ObPrivType priv_type = priv_node->value_;
        if ((OB_PRIV_INSERT == priv_type
             || OB_PRIV_UPDATE == priv_type
             || OB_PRIV_REFERENCES == priv_type) 
            && OB_NOT_NULL(role_sys_obj_all_col_priv_node->children_[1])) {
          ret = OB_ERR_REVOKE_BY_COLUMN;
          LOG_WARN("cannot revoke column privilege, must revoke from whole table", K(ret));
        } else if (OB_PRIV_USER_LEVEL == grant_level) {
          priv_set |= priv_type;
        } else if (OB_PRIV_DB_LEVEL == grant_level) {
          if (OB_PRIV_ALL == priv_type) {
            priv_set |= OB_PRIV_DB_ACC;
          } else if (priv_type & (~(OB_PRIV_DB_ACC | OB_PRIV_GRANT))) {
            ret = OB_ERR_PRIV_USAGE;
            LOG_WARN("Grant/Revoke privilege than can not be used",
                      "priv_type", ObPrintPrivSet(priv_type), K(ret));
          } else {
            priv_set |= priv_type;
          }
        } else if (OB_PRIV_TABLE_LEVEL == grant_level) {
          if (OB_PRIV_ALL == priv_type) {
            revoke_all_ora = true;
            priv_set |= OB_PRIV_TABLE_ACC;
          } else if (priv_type & (~(OB_PRIV_TABLE_ACC | OB_PRIV_GRANT)) && 
                     ObGrantResolver::is_ora_obj_priv_type(priv_type) == false) {
            ret = OB_ILLEGAL_GRANT_FOR_TABLE;
            LOG_WARN("Grant/Revoke privilege than can not be used",
                      "priv_type", ObPrintPrivSet(priv_type), K(ret));
          } else {
            bool can_map;
            share::ObRawObjPriv ora_obj_priv;
            if (ObGrantResolver::is_ora_obj_priv_type(priv_type) == false) {
              priv_set |= priv_type;
            }
            OZ (ObGrantResolver::map_mysql_priv_type_to_ora_type(priv_type,
                                                                 ora_obj_priv, 
                                                                 can_map));
            if (OB_SUCC(ret) && can_map) {
              bool exists;
              OZ (ObGrantResolver::obj_priv_exists(obj_priv_array, ora_obj_priv, exists));
              if (OB_SUCC(ret) && exists) {
                ret = OB_PRIV_DUP;
                LOG_WARN("duplicate privilege list", K(ora_obj_priv), K(ret));
              }
              OZ (obj_priv_array.push_back(ora_obj_priv));
            }
          }
        } else {
          //do nothing
        }
      } else {
        ret = OB_ERR_PARSE_SQL;
        LOG_WARN("sql_parser parse privileges error", K(ret));
      }
    }
  }
  return ret;
}

int ObRevokeResolver::resolve_ora(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ParseNode *node = const_cast<ParseNode*>(&parse_tree);
  ObRevokeStmt *revoke_stmt = NULL;
  if (OB_ISNULL(params_.schema_checker_) || OB_ISNULL(params_.session_info_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema_checker or session info not inited",
        K(ret), "schema checker", params_.schema_checker_, "session info", params_.session_info_);
  } else if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node is NULL", K(ret));
  } else if (T_REVOKE != node->type_ && T_SYSTEM_REVOKE != node->type_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid node type", K(ret), K(node->type_));
  } else if ((T_REVOKE == node->type_ && 3 != node->num_child_)
             || (T_SYSTEM_REVOKE == node->type_ && 2 != node->num_child_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid num_child_", K(ret), K(node->type_), K(node->num_child_));
  } else if (OB_ISNULL(revoke_stmt = create_stmt<ObRevokeStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("Failed to create ObRevokeStmt", K(ret));
  } else {
    stmt_ = revoke_stmt;
    uint64_t tenant_id = params_.session_info_->get_effective_tenant_id();
    revoke_stmt->set_tenant_id(tenant_id);
    if (T_SYSTEM_REVOKE == node->type_) {
      revoke_stmt->set_stmt_type(stmt::T_SYSTEM_REVOKE);
      if (OB_FAIL(resolve_revoke_role_and_sysprivs_inner(node, revoke_stmt))) {
        LOG_WARN("fail to resolve_revoke_role_and_sysprivs_inner", K(ret));
      }
    } else if (T_REVOKE == node->type_) {
      revoke_stmt->set_stmt_type(stmt::T_REVOKE);
      if (OB_FAIL(resolve_revoke_obj_priv_inner(node, revoke_stmt))) {
        LOG_WARN("fail to resolve_revoke_obj_priv_inner", K(ret));
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid node type", K(ret), K(node->type_));
    }
  }
  return ret;
}

int ObRevokeResolver::resolve_revoke_role_and_sysprivs_inner(const ParseNode *node,
                                                             ObRevokeStmt *revoke_stmt)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = OB_INVALID_ID;
  ObSEArray<uint64_t, 4> role_id_array;
  share::ObRawPrivArray sys_priv_array;
  ObArray<ObString> role_name_array;
  CK (OB_NOT_NULL(node) && OB_NOT_NULL(revoke_stmt));
  CK (OB_NOT_NULL(params_.schema_checker_) && OB_NOT_NULL(params_.session_info_));
  CK (2 == node->num_child_
      && OB_NOT_NULL(node->children_[0])
      && OB_NOT_NULL(node->children_[1]));
  // 1. Resolve role and sys_privs
  if (OB_SUCC(ret)) {
    tenant_id = revoke_stmt->get_tenant_id();
    ParseNode *role_sys_obj_all_col_priv_list = node->children_[0];
    if (OB_FAIL(ObGrantResolver::resolve_role_sys_obj_all_col_priv_list(
                                      role_sys_obj_all_col_priv_list,
                                      role_name_array,
                                      sys_priv_array))) {
      LOG_WARN("fail to resolve_role_sys_obj_all_col_priv_list", K(ret));
    } else {
      for (int i = 0; OB_SUCC(ret) && i < role_name_array.count(); ++i) {
        const ObUserInfo *role_info = NULL;
        const ObString &role_name = role_name_array.at(i);
        // check roles exists
        OZ (schema_checker_->get_user_info(tenant_id, 
                                            role_name, 
                                            // role has fixed host_name '%'
                                            ObString::make_string(OB_DEFAULT_HOST_NAME), 
                                            role_info), tenant_id, role_name);
        if (OB_USER_NOT_EXIST == ret || OB_ISNULL(role_info) || !role_info->is_role()) {
          ret = OB_ROLE_NOT_EXIST;
          LOG_USER_ERROR(OB_ROLE_NOT_EXIST, role_name.length(), role_name.ptr());
        }
        OZ (revoke_stmt->add_role_ora(role_info->get_user_id()));
        OZ (role_id_array.push_back(role_info->get_user_id()));
      }
      OZ (revoke_stmt->set_priv_array(sys_priv_array));
    }
    CK (OB_LIKELY(role_id_array.count() == role_name_array.count()));
  }
  // 2. check role privileges
  if (OB_SUCC(ret) && ObSchemaChecker::is_ora_priv_check()) {
    OZ (params_.schema_checker_->check_ora_grant_role_priv(
                                  revoke_stmt->get_tenant_id(),
                                  params_.session_info_->get_priv_user_id(),
                                  role_id_array,
                                  params_.session_info_->get_enable_role_array()),
        revoke_stmt->get_tenant_id(), revoke_stmt->get_roles(), role_id_array);
  }
  // 3. check sys privileges
  if (OB_SUCC(ret) && ObSchemaChecker::is_ora_priv_check()) {
    OZ (params_.schema_checker_->check_ora_grant_sys_priv(
                                  revoke_stmt->get_tenant_id(),
                                  params_.session_info_->get_priv_user_id(),
                                  sys_priv_array,
                                  params_.session_info_->get_enable_role_array()),
      revoke_stmt->get_tenant_id(), params_.session_info_->get_priv_user_id(), sys_priv_array);
  }
  // 4. resolve grantee
  uint64_t user_id = OB_INVALID_ID;
  const ObUserInfo *user_info = NULL;
  ObSArray<ObString> user_name_array;
  ObSArray<ObString> host_name_array;
  OZ (ObGrantResolver::resolve_grantee_clause(node->children_[1],
                                              params_.session_info_,
                                              user_name_array, 
                                              host_name_array));
  CK (user_name_array.count() == host_name_array.count());
  for (int i = 0; OB_SUCC(ret) && i < user_name_array.count(); ++i) {
    const ObString &user_name = user_name_array.at(i);
    const ObString &host_name = host_name_array.at(i);
    if (OB_FAIL(params_.schema_checker_->get_user_id(tenant_id, user_name, host_name, user_id))) {
      if (OB_USER_NOT_EXIST == ret) {
        ret = OB_ERR_USER_OR_ROLE_DOES_NOT_EXIST;
        LOG_USER_ERROR(OB_ERR_USER_OR_ROLE_DOES_NOT_EXIST, user_name.length(), user_name.ptr());
      }
      LOG_WARN("fail to get user id", K(ret), K(user_name), K(host_name));
    } else if (OB_FAIL(check_dcl_on_inner_user(node->type_,
                                               params_.session_info_->get_priv_user_id(),
                                               user_id))) {
      LOG_WARN("failed to check dcl on inner-user or unsupport to modify reserved user", K(ret),
               K(session_info_->get_priv_user_id()), K(user_id));
    }
    OZ (revoke_stmt->add_grantee(user_name));
    OZ (params_.schema_checker_->get_user_info(tenant_id, user_id, user_info), user_id);
    OZ (revoke_stmt->add_user(user_id));
    // check user has roles
    if (OB_SUCC(ret)) {
      CK (OB_NOT_NULL(user_info));
      if (false == role_id_array.empty()) {
        for (int i = 0; OB_SUCC(ret) && i < role_id_array.count(); i++) {
          if (!has_exist_in_array(user_info->get_role_id_array(), role_id_array.at(i))) {
            ret = OB_ERR_ROLE_NOT_GRANTED_TO;
            LOG_USER_ERROR(OB_ERR_ROLE_NOT_GRANTED_TO, 
                           role_name_array.at(i).length(),
                           role_name_array.at(i).ptr(),
                           0, "",
                           user_info->get_user_name_str().length(),
                           user_info->get_user_name_str().ptr(),
                           0, "");
          }
        }
      }
    }
  }
  // 5. set grant level
  OX (revoke_stmt->set_grant_level(OB_PRIV_SYS_ORACLE_LEVEL));
  return ret;
}

int ObRevokeResolver::resolve_revoke_obj_priv_inner(const ParseNode *node,
                                                    ObRevokeStmt *revoke_stmt)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = OB_INVALID_ID;
  ObPrivLevel grant_level = OB_PRIV_INVALID_LEVEL;
  bool is_directory = false;
  bool explicit_db = false;
  CK (OB_NOT_NULL(node) && OB_NOT_NULL(revoke_stmt));
  CK (OB_NOT_NULL(params_.schema_checker_) && OB_NOT_NULL(params_.session_info_));
  CK (3 == node->num_child_
      && OB_NOT_NULL(node->children_[0])
      && OB_NOT_NULL(node->children_[1])
      && OB_NOT_NULL(node->children_[2]));
  if (OB_SUCC(ret)) {
    ParseNode *privs_node = node->children_[0];
    ParseNode *priv_level_node = node->children_[1];
    ParseNode *users_node = node->children_[2];
    tenant_id = revoke_stmt->get_tenant_id();
    // 1. Resolve priv_level
    if (OB_SUCC(ret)) {
      ObString db = ObString::make_string("");
      ObString table = ObString::make_string("");
      if (OB_FAIL(ObGrantResolver::resolve_obj_ora(priv_level_node,
                            params_.session_info_->get_database_name(),
                            db, 
                            table, 
                            grant_level,
                            is_directory,
                            explicit_db))) {
        LOG_WARN("Resolve priv_level node error", K(ret));
      }  else if (OB_FAIL(check_and_convert_name(db, table))) {
        LOG_WARN("Check and convert name error", K(db), K(table), K(ret));
      } else {
        revoke_stmt->set_grant_level(grant_level);
        if (OB_FAIL(revoke_stmt->set_database_name(db))) {
          LOG_WARN("Failed to set database_name to revoke_stmt", K(ret));
        } else if (OB_FAIL(revoke_stmt->set_table_name(table))) {
          LOG_WARN("Failed to set table_name to revoke_stmt", K(ret));
        } else {
          share::schema::ObObjectType object_type = share::schema::ObObjectType::INVALID;
          uint64_t object_id = OB_INVALID_ID;
          if (db.empty() || table.empty()) {
            object_type = share::schema::ObObjectType::MAX_TYPE;
          } else {
            ObString obj_db_name;
            ObSynonymChecker synonym_checker;
            OZ (params_.schema_checker_->get_object_type(
                tenant_id, db, table,
                object_type, object_id, obj_db_name, is_directory, 
                explicit_db, ObString(""), synonym_checker));
            OZ (revoke_stmt->set_database_name(obj_db_name));
          }
          revoke_stmt->set_object_type(object_type);
          revoke_stmt->set_object_id(object_id);
        }  //do nothing
      }
    }
    // 2. resolve privileges
    if (OB_SUCC(ret)) {
      ObPrivSet priv_set = 0;
      bool revoke_all_ora = false;
      share::ObRawObjPrivArray obj_priv_array;
      if (OB_FAIL(resolve_priv_set_ora(privs_node, grant_level, 
          priv_set, obj_priv_array, revoke_all_ora))) {
        LOG_WARN("Resolve priv set error", K(ret));
      }
      OX (revoke_stmt->set_revoke_all_ora(revoke_all_ora));
      OX (revoke_stmt->set_priv_set(priv_set));
      OZ (revoke_stmt->set_obj_priv_array(obj_priv_array));
    }
    // 3. check revoke object privs for oracle 
    if (OB_SUCC(ret) 
        && ObSchemaChecker::is_ora_priv_check()
        && revoke_stmt->get_object_id() != OB_INVALID_ID) {
      uint64_t grantor_id_out = OB_INVALID_ID;
      ObSEArray<uint64_t, 4> col_id_array;
      OZ (params_.schema_checker_->check_ora_grant_obj_priv(
          tenant_id,
          params_.session_info_->get_priv_user_id(),
          revoke_stmt->get_database_name(),
          revoke_stmt->get_object_id(),
          static_cast<uint64_t>(revoke_stmt->get_object_type()),
          revoke_stmt->get_obj_priv_array(),
          col_id_array,
          col_id_array,
          col_id_array,
          grantor_id_out,
          params_.session_info_->get_enable_role_array()));
      OX (revoke_stmt->set_grantor_id(grantor_id_out));  
    }
    // 4. resolve users_node
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(users_node)) {
        ret = OB_ERR_PARSE_SQL;
        LOG_WARN("Users node should not be NULL", K(ret));
      } else {
        ObSEArray<uint64_t, 4> user_id_array;
        for (int i = 0; OB_SUCC(ret) && i < users_node->num_child_; ++i) {
          ParseNode *user_hostname_node = users_node->children_[i];
          uint64_t user_id = OB_INVALID_ID;
          //0: user name; 1: host name
          ObString user_name;
          ObString host_name;
          
          if (OB_ISNULL(user_hostname_node)) {
            ret = OB_ERR_PARSE_SQL;
            LOG_WARN("the child of users should not be NULL", K(ret), K(i));
          } else if (0 == user_hostname_node->num_child_) {
            user_name.assign_ptr(const_cast<char *>(user_hostname_node->str_value_),
                                  static_cast<int32_t>(user_hostname_node->str_len_));
            host_name.assign_ptr(OB_DEFAULT_HOST_NAME, 
                                  static_cast<int32_t>(STRLEN(OB_DEFAULT_HOST_NAME)));
          } else if (2 != user_hostname_node->num_child_) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("sql_parser parse user error", K(ret));
          } else if (OB_ISNULL(user_hostname_node->children_[0])) {
            // 0: user, 1: hostname
            ret = OB_ERR_PARSE_SQL;
            LOG_WARN("The child of user node should not be NULL", K(ret), K(i));
          } else {
            user_name.assign_ptr(user_hostname_node->children_[0]->str_value_,
                                  static_cast<int32_t>(user_hostname_node->children_[0]->str_len_));
            if (NULL == user_hostname_node->children_[1]) {
              host_name.assign_ptr(OB_DEFAULT_HOST_NAME, 
                                    static_cast<int32_t>(STRLEN(OB_DEFAULT_HOST_NAME)));
            } else {
              host_name.assign_ptr(user_hostname_node->children_[1]->str_value_,
                                    static_cast<int32_t>(user_hostname_node->children_[1]->str_len_));
            }
            if (user_name.length() > OB_MAX_USER_NAME_LENGTH) {
              ret = OB_WRONG_USER_NAME_LENGTH;
              LOG_USER_ERROR(OB_WRONG_USER_NAME_LENGTH, user_name.length(), user_name.ptr());
            }
          }
          if (OB_SUCC(ret)) {
            if (OB_FAIL(revoke_stmt->add_grantee(user_name))) {
              SQL_RESV_LOG(WARN, "fail to add grantee", K(ret), K(user_name), K(host_name));
            } else if (OB_FAIL(params_.schema_checker_->get_user_id(tenant_id, user_name, 
                                                              host_name, user_id))) {
              if (ret == OB_USER_NOT_EXIST) {
                ret = OB_ERR_USER_OR_ROLE_DOES_NOT_EXIST;
                LOG_USER_ERROR(OB_ERR_USER_OR_ROLE_DOES_NOT_EXIST, 
                                user_name.length(), user_name.ptr());
              }
              SQL_RESV_LOG(WARN, "fail to get user id", K(ret), K(user_name), K(host_name));
            } else if (is_root_user(user_id) && OB_PRIV_USER_LEVEL == grant_level) {
              ret = OB_NOT_SUPPORTED;
              LOG_WARN("Revoke privilege from root at global level is not supported", K(ret));
              LOG_USER_ERROR(OB_NOT_SUPPORTED, "Revoke privilege from root at global level");
            } else if (OB_FAIL(check_dcl_on_inner_user(node->type_,
                                                       params_.session_info_->get_priv_user_id(),
                                                       user_id))) {
              LOG_WARN("failed to check dcl on inner-user or unsupport to modify reserved user",
                       K(ret), K(params_.session_info_->get_priv_user_id()), K(user_id));
            } else if (OB_FAIL(revoke_stmt->add_user(user_id))) {
              LOG_WARN("Add user to grant_stmt error", K(ret), K(user_id));
            } else {
              //check user name dup
              if (has_exist_in_array(user_id_array, user_id)) {
                ret = OB_ERR_DUPLICATE_USERNAME_IN_LIST;
              } else {
                OZ (user_id_array.push_back(user_id));
              }
            }
          }
        } //end for
        /* check revoke objauth from himself */
        if (OB_SUCC(ret)) {
          if (has_exist_in_array(user_id_array, params_.session_info_->get_priv_user_id())) {
            ret = OB_ERR_YOU_MAY_NOT_REVOKE_PRIVILEGES_FROM_YOURSELF;
          }
        }
      }
    }
  }
  return ret;
}
