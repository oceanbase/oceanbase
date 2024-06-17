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

#include "sql/resolver/dcl/ob_alter_user_profile_resolver.h"

#include "share/schema/ob_schema_struct.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/resolver/dcl/ob_grant_resolver.h"
#include "share/ob_rpc_struct.h"
#include "lib/encrypt/ob_encrypted_helper.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/optimizer/ob_optimizer_util.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;
using oceanbase::share::schema::ObUserInfo;

ObAlterUserProfileResolver::ObAlterUserProfileResolver(ObResolverParams &params)
    : ObDCLResolver(params)
{
}

ObAlterUserProfileResolver::~ObAlterUserProfileResolver()
{
}

int ObAlterUserProfileResolver::resolve_set_role(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObAlterUserProfileStmt *stmt = NULL;

  if (OB_ISNULL(params_.session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(params_.schema_checker_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not init", K(ret));
  } else if (lib::is_mysql_mode() && OB_FAIL(ObSQLUtils::compatibility_check_for_mysql_role_and_column_priv(MTL_ID()))) {
    LOG_WARN("set role not supported while upgrading", K(ret));
  } else if (T_SET_ROLE != parse_tree.type_
             || 1 != parse_tree.num_child_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("wrong root", K(ret), K(parse_tree.type_), K(parse_tree.num_child_));
  } else if (OB_ISNULL(stmt = create_stmt<ObAlterUserProfileStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to create ObAlterUserProfileStmt", K(ret));
  } else {
    ObString user_name;
    ObString host_name(OB_DEFAULT_HOST_NAME);
    uint64_t session_user_id = lib::is_mysql_mode() ? params_.session_info_->get_priv_user_id()
                                                    : params_.session_info_->get_user_id();
    const ObUserInfo *user_info = NULL;
    if (OB_FAIL(params_.schema_checker_->get_user_info(
         params_.session_info_->get_effective_tenant_id(),
         session_user_id, user_info))) {
      LOG_WARN("get user info failed", K(ret));
    } else if (NULL == user_info) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("current user info is null", K(ret));
    } else {
      
      obrpc::ObAlterUserProfileArg &arg = stmt->get_ddl_arg();
      arg.tenant_id_ = params_.session_info_->get_effective_tenant_id();
      stmt->set_set_role_flag(ObAlterUserProfileStmt::SET_ROLE);

      ObSEArray<uint64_t, 8> role_id_array = user_info->get_role_id_array();
      if (params_.session_info_->get_proxy_user_id() != OB_INVALID_ID) {
        for (int64_t i = 0; OB_SUCC(ret) && i < user_info->get_proxied_user_info_cnt(); i++) {
          const ObProxyInfo *proxy_info = user_info->get_proxied_user_info_by_idx(i);
          if (OB_ISNULL(proxy_info)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected error", K(ret));
          } else if (proxy_info->user_id_ == params_.session_info_->get_proxy_user_id()) {
            ObArray<uint64_t> res_role_id_array;
            ObArray<uint64_t> res_role_id_option_array;
            if (OB_FAIL(ObSQLUtils::get_proxy_can_activate_role(user_info->get_role_id_array(),
                                                                user_info->get_role_id_option_array(),
                                                                *proxy_info,
                                                                res_role_id_array,
                                                                res_role_id_option_array))) {
              LOG_WARN("get proxy can activate role failed", K(ret));
            } else {
              OZ (role_id_array.assign(res_role_id_array));
            }
          }
        }
      }
      /* 1. resolve default role */
      OZ (resolve_default_role_clause(parse_tree.children_[0], arg, role_id_array, false));

    }
  }
  return ret;
}

int ObAlterUserProfileResolver::resolve_role_list(
  const ParseNode *role_list,
  obrpc::ObAlterUserProfileArg &arg,
  const ObIArray<uint64_t> &role_id_array,
  bool for_default_role_stmt)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(role_list));
  CK (OB_NOT_NULL(params_.session_info_));
  CK (OB_NOT_NULL(params_.schema_checker_));
  hash::ObHashMap<uint64_t, ObString> roleid_pwd_map;
  OZ (roleid_pwd_map.create(32, "HashRRLPwdMa"));
  if (OB_SUCC(ret)) {
    for (int i = 0; OB_SUCC(ret) && i < role_list->num_child_; ++i) {
      uint64_t role_id = OB_INVALID_ID;
      ParseNode *role = role_list->children_[i];
      ParseNode *pwd_node = NULL;
      if (NULL == role) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("role opt identified by node is null", K(ret));
      } else if (lib::is_mysql_mode()) {
      } else if (T_IDENT == role->type_) {
      } else if (T_SET_ROLE_PASSWORD != role->type_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("role type is error", K(ret), K(role->type_));
      } else if (2 == role->value_ && NULL == role->children_[1]) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("passwd_node is NULL", K(ret));
      } else if (NULL == role->children_[0]) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("role node is null", K(ret));
      } else {
        if (2 == role->value_) {
          pwd_node = role->children_[1];
        }
        role = role->children_[0];
      }
      if (OB_SUCC(ret)) {
        ObString role_name;
        ObString host_name(OB_DEFAULT_HOST_NAME);
        const ObUserInfo *role_info = NULL;
        if (lib::is_oracle_mode()) {
          role_name.assign_ptr(const_cast<char *>(role->str_value_),
              static_cast<int32_t>(role->str_len_));
        } else {
          OZ (resolve_user_host(role, role_name, host_name));
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(params_.schema_checker_->get_user_info(arg.tenant_id_, role_name,
                                                                  host_name, role_info))) {
          if (lib::is_oracle_mode()) {
            if (OB_USER_NOT_EXIST == ret || OB_ISNULL(role_info) || !role_info->is_role()) {
              ret = OB_ROLE_NOT_EXIST;
              LOG_USER_ERROR(OB_ROLE_NOT_EXIST,
                              role_name.length(), role_name.ptr());
              LOG_WARN("role not exists", K(ret), K(role_name));
            }
          } else {
            if (OB_USER_NOT_EXIST == ret) {
              if (obrpc::OB_DEFAULT_ROLE_ALL_EXCEPT == arg.default_role_flag_) {
                ret = OB_SUCCESS; //ignore EXCEPTED ROLE not exist
              } else {
                ret = OB_ERR_UNKNOWN_AUTHID;
                LOG_USER_ERROR(OB_ERR_UNKNOWN_AUTHID, role_name.length(), role_name.ptr(),
                                                      host_name.length(), host_name.ptr());
              }
            }
          }
          LOG_WARN("fail to get user id", K(ret), K(role_name), K(host_name));
        } else if (role_info == NULL) {
          if (for_default_role_stmt) {
            ret = OB_ROLE_NOT_EXIST;
            LOG_USER_ERROR(OB_ROLE_NOT_EXIST, 
                            role_name.length(), role_name.ptr());
            LOG_WARN("role not exists", K(ret), K(role_name));
          } else {
            ret = OB_ERR_ROLE_NOT_GRANTED_OR_DOES_NOT_EXIST;
            LOG_USER_ERROR(OB_ERR_ROLE_NOT_GRANTED_OR_DOES_NOT_EXIST, 
                            role_name.length(), role_name.ptr());
            LOG_WARN("role not granted or does not exists", K(ret), K(role_name));
          }
        } else {
          bool skip = false;
          role_id = role_info->get_user_id();
          if (lib::is_oracle_mode() && is_ora_public_role(role_id)) {
            ret = OB_ERR_MISSING_OR_INVALID_ROLE_NAME;
          } else if (has_exist_in_array(arg.role_id_array_, role_id)) {
            /* if role duplicate in role list, then raise error */
            if (lib::is_mysql_mode()) {
              skip = true;
            } else {
              ret = OB_PRIV_DUP;
            }
          } else if (lib::is_oracle_mode()) {
            if (!has_exist_in_array(role_id_array, role_id)) {
              /* if role not granted to user, then raise error */
              if (for_default_role_stmt) {
                  ret = OB_ERR_DEFAULT_ROLE_NOT_GRANTED_TO_USER;
                  LOG_USER_ERROR(OB_ERR_DEFAULT_ROLE_NOT_GRANTED_TO_USER,
                                role_name.length(), role_name.ptr());
                LOG_WARN("role not granted to user", K(ret), K(role_name));
              } else {
                ret = OB_ERR_ROLE_NOT_GRANTED_OR_DOES_NOT_EXIST;
                LOG_USER_ERROR(OB_ERR_ROLE_NOT_GRANTED_OR_DOES_NOT_EXIST,
                              role_name.length(), role_name.ptr());
                LOG_WARN("role not granted or does not exists", K(ret), K(role_name));
              }
            }
          } else {
            ObString cur_user_name;
            ObString cur_host_name;
            if (!for_default_role_stmt) {
              if (!has_exist_in_array(role_id_array, role_id)) {
                ret = OB_ERR_ROLE_NOT_GRANTED_TO;
                cur_user_name = params_.session_info_->get_user_name();
                cur_host_name = params_.session_info_->get_host_name();
              }
            } else {
              for (int j = 0; OB_SUCC(ret) && j < arg.user_ids_.count(); j++) {
                const ObUserInfo *cur_user_info = NULL;
                OZ (params_.schema_checker_->get_user_info(arg.tenant_id_, arg.user_ids_.at(j), cur_user_info));
                CK (OB_NOT_NULL(cur_user_info));
                if (OB_SUCC(ret) && !has_exist_in_array(cur_user_info->get_role_id_array(), role_id)) {
                  ret = OB_ERR_ROLE_NOT_GRANTED_TO;
                  cur_user_name = cur_user_info->get_user_name_str();
                  cur_host_name = cur_user_info->get_host_name_str();
                }
              }
            }
            if (OB_ERR_ROLE_NOT_GRANTED_TO == ret) {
              if (obrpc::OB_DEFAULT_ROLE_ALL_EXCEPT == arg.default_role_flag_) {
                skip = true;
                ret = OB_SUCCESS; //ignore EXCEPTED ROLE not granted to user
              } else {
                LOG_USER_ERROR(OB_ERR_ROLE_NOT_GRANTED_TO,
                              role_name.length(), role_name.ptr(),
                              host_name.length(), host_name.ptr(),
                              cur_user_name.length(), cur_user_name.ptr(),
                              cur_host_name.length(), cur_host_name.ptr());
              }
            }
          }
          if (OB_SUCC(ret) && !skip) {
            OZ (arg.role_id_array_.push_back(role_id));
            ObString pwd = ObString::make_string("");
            if (NULL != pwd_node) {
              pwd.assign_ptr(pwd_node->str_value_, static_cast<int32_t>(pwd_node->str_len_));
            }
            OZ (roleid_pwd_map.set_refactored(role_id, pwd));
          }
        }
      }
    }
  }
  // Check role password for SET ROLE
  if (OB_SUCC(ret) && lib::is_oracle_mode()) {
    if (for_default_role_stmt) {
      // do nothing when ALTER USER DEFAULT ROLE
    } else {
      switch (arg.default_role_flag_) {
        case obrpc::OB_DEFAULT_ROLE_ALL: {
          if (OB_FAIL(check_role_password(arg.tenant_id_,
                                          role_id_array,
                                          roleid_pwd_map,
                                          true,
                                          false))) {
            LOG_WARN("failed to pass check_role_password", K(ret), K(arg));
          }
          break;
        }
        case obrpc::OB_DEFAULT_ROLE_ALL_EXCEPT: {
          if (OB_FAIL(check_role_password(arg.tenant_id_,
                                          role_id_array,
                                          roleid_pwd_map,
                                          true,
                                          true))) {
            LOG_WARN("failed to pass check_role_password", K(ret), K(arg));
          }
          break;
        }
        case obrpc::OB_DEFAULT_ROLE_LIST: {
          if (OB_FAIL(check_role_password(arg.tenant_id_,
                                          role_id_array,
                                          roleid_pwd_map,
                                          false,
                                          false))) {
            LOG_WARN("failed to pass check_role_password", K(ret), K(arg));
          }
          break;
        }
        case obrpc::OB_DEFAULT_ROLE_DEFAULT:
        case obrpc::OB_DEFAULT_ROLE_NONE: {
          // No need to check for NONE
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("default_role_flag invalid", K(ret), K(arg.default_role_flag_));
          break;
        }
      }
    }
  }
  return ret;
}

int ObAlterUserProfileResolver::resolve_default_role_clause(
    const ParseNode *parse_tree,
    obrpc::ObAlterUserProfileArg &arg,
    const ObIArray<uint64_t> &role_id_array,
    bool for_default_role_stmt)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(parse_tree));
  if (T_DEFAULT_ROLE != parse_tree->type_ || (1 != parse_tree->num_child_
                                           && 2 != parse_tree->num_child_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("wrong root", K(ret), K(parse_tree->type_), K(parse_tree->num_child_));
  } else {
    if (1 == parse_tree->num_child_) {
      CK (OB_NOT_NULL(parse_tree->children_[0]));
      if (OB_SUCC(ret)) {
        switch (parse_tree->children_[0]->value_) {
          case 1: {
            arg.default_role_flag_ = obrpc::OB_DEFAULT_ROLE_ALL;
            // Sinc resolve_role_list will not be called in case OB_DEFAULT_ROLE_ALL,
            // so we manually do this check here. Only for SET ROLE
            if (false == for_default_role_stmt) {
              if (OB_FAIL(check_role_password_all(arg.tenant_id_, role_id_array))) {
                LOG_WARN("Password mismatch for set role all", K(ret), K(arg));
              }
            }
            break;
          }
          case 3: {
            arg.default_role_flag_ = obrpc::OB_DEFAULT_ROLE_NONE;
            break;
          }
          case 4: {
            arg.default_role_flag_ = obrpc::OB_DEFAULT_ROLE_DEFAULT;
            break;
          }
          default: {
            ret = OB_ERR_UNDEFINED;
            LOG_WARN("invalid type", K(ret), K(parse_tree->children_[0]->value_));
          }
        }
      }
    } else {
      CK (2 == parse_tree->num_child_);
      if (OB_SUCC(ret)) {
        if (0 == parse_tree->children_[0]->value_) {
          OX (arg.default_role_flag_ = obrpc::OB_DEFAULT_ROLE_LIST);
        } else {
          CK (2 == parse_tree->children_[0]->value_);
          OX (arg.default_role_flag_ = obrpc::OB_DEFAULT_ROLE_ALL_EXCEPT);
        }
        OZ (resolve_role_list(parse_tree->children_[1], arg, role_id_array, for_default_role_stmt));
      }
    }
  }
  
  return ret;
}

int ObAlterUserProfileResolver::resolve_default_role(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObAlterUserProfileStmt *stmt = NULL;
  uint64_t tenant_id = OB_INVALID_ID;

  if (OB_ISNULL(params_.session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(params_.schema_checker_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not init", K(ret));
  } else if (lib::is_mysql_mode() && OB_FAIL(ObSQLUtils::compatibility_check_for_mysql_role_and_column_priv(MTL_ID()))) {
    LOG_WARN("set default role not supported while upgrading", K(ret));
  } else if (T_ALTER_USER_DEFAULT_ROLE != parse_tree.type_
             || 2 != parse_tree.num_child_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("wrong root", K(ret), K(parse_tree.type_), K(parse_tree.num_child_));
  } else if (OB_ISNULL(stmt = create_stmt<ObAlterUserProfileStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to create ObAlterUserProfileStmt", K(ret));
  } else {
    ObString user_name;
    ObString host_name;
    const ObUserInfo *user_info = NULL;
    obrpc::ObAlterUserProfileArg &arg = stmt->get_ddl_arg();
    stmt->set_set_role_flag(ObAlterUserProfileStmt::SET_DEFAULT_ROLE);

    /* 1. resolve user */
    tenant_id = params_.session_info_->get_effective_tenant_id();
    arg.tenant_id_ = tenant_id;
    if (T_USER_WITH_HOST_NAME == parse_tree.children_[0]->type_) {
      ParseNode *user_with_host_name = parse_tree.children_[0];
      // Get user_name and host_name
      if (OB_ISNULL(user_with_host_name)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("user_with_host_name is NULL");
      } else {
        ParseNode *user_name_node = user_with_host_name->children_[0];
        ParseNode *host_name_node = user_with_host_name->children_[1];
        if (OB_ISNULL(user_name_node)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("user_name is NULL", K(ret), K(user_name));
        } else {
          user_name = ObString(user_name_node->str_len_, user_name_node->str_value_);
        }
        if (NULL != host_name_node) {
          host_name = ObString(host_name_node->str_len_, host_name_node->str_value_);
        } else {
          host_name = ObString(OB_DEFAULT_HOST_NAME);
        }
      }
      OZ (params_.schema_checker_->get_user_info(tenant_id, user_name, host_name, user_info),
            tenant_id, user_name, host_name);
      if (ret == OB_USER_NOT_EXIST) {
        if (lib::is_oracle_mode()) {
          LOG_USER_ERROR(OB_USER_NOT_EXIST, user_name.length(), user_name.ptr());
        } else {
          ret = OB_ERR_UNKNOWN_AUTHID;
          LOG_USER_ERROR(OB_ERR_UNKNOWN_AUTHID, user_name.length(), user_name.ptr(), host_name.length(), host_name.ptr());
        }
      }
      if (OB_SUCC(ret)) {
        if (user_info == NULL) {
          ret = OB_USER_NOT_EXIST;
          LOG_USER_ERROR(OB_USER_NOT_EXIST, user_name.length(), user_name.ptr());
        } else if (lib::is_mysql_mode()) {
          OZ (arg.user_ids_.push_back(user_info->get_user_id()));
        } else if (OB_FAIL(check_dcl_on_inner_user(parse_tree.type_,
                                                    session_info_->get_priv_user_id(),
                                                    user_info->get_user_id()))) {
          LOG_WARN("failed to check dcl on inner-user or unsupport to modify reserved user", K(ret),
                  K(session_info_->get_user_name()), K(user_name));
        } else {
          arg.user_id_ = user_info->get_user_id();
        }
      }

    } else {
      ParseNode *user_list_node = parse_tree.children_[0];
      for (int i = 0; OB_SUCC(ret) && i < user_list_node->num_child_; i++) {
        OZ (ObDCLResolver::resolve_user_list_node(user_list_node->children_[i], user_list_node, user_name, host_name));
        OZ (params_.schema_checker_->get_user_info(tenant_id, user_name, host_name, user_info),
              tenant_id, user_name, host_name);
        if (OB_USER_NOT_EXIST == ret || OB_ISNULL(user_info)) {
          ret = OB_ERR_UNKNOWN_AUTHID;
          LOG_USER_ERROR(OB_ERR_UNKNOWN_AUTHID, user_name.length(), user_name.ptr(), host_name.length(), host_name.ptr());
        }
        OZ (arg.user_ids_.push_back(user_info->get_user_id()));
      }
    }

    /* 2. resolve default role */
    OZ (resolve_default_role_clause(parse_tree.children_[1], arg, 
                                    user_info->get_role_id_array(), true));

    if (OB_SUCC(ret) && lib::is_mysql_mode()) {
      ObSqlCtx *sql_ctx = NULL;
      if (OB_ISNULL(params_.session_info_->get_cur_exec_ctx())
          || OB_ISNULL(sql_ctx = params_.session_info_->get_cur_exec_ctx()->get_sql_ctx())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected ctx", K(ret), KP(params_.session_info_->get_cur_exec_ctx()));
      }
      for (int i = 0; OB_SUCC(ret) && i < arg.user_ids_.count(); i++) {
        if (arg.user_ids_.at(i) != params_.session_info_->get_priv_user_id()) {
          OZ (schema_checker_->check_set_default_role_priv(*sql_ctx));
        }
      }
    }

  }
  return ret;
}

int ObAlterUserProfileResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ParseNode *user_with_host_name = NULL;
  ParseNode *user_name = NULL;
  ParseNode *host_name = NULL;
  ParseNode *user_profile = NULL;
  ParseNode *profile_name = NULL;
  ObAlterUserProfileStmt *lock_alter_user_profile_stmt = NULL;

  if (OB_ISNULL(params_.session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not init", K(ret));
  } else if (T_SET_ROLE == parse_tree.type_) {
    OZ (resolve_set_role(parse_tree));
  } else if (T_ALTER_USER_DEFAULT_ROLE == parse_tree.type_) {
    OZ (resolve_default_role(parse_tree));
  } else if (T_ALTER_USER_PROFILE != parse_tree.type_
             || 2 != parse_tree.num_child_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("wrong root", K(ret), K(parse_tree.type_), K(parse_tree.num_child_));
  } else if (OB_ISNULL(user_with_host_name = parse_tree.children_[0])
             || OB_ISNULL(user_profile = parse_tree.children_[1])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child is null", K(parse_tree.children_[0]), K(parse_tree.children_[1]), K(ret));
  } else if (T_USER_WITH_HOST_NAME != user_with_host_name->type_
             || user_with_host_name->num_child_ != 2) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("1st child type error",
             "type", user_with_host_name->type_,
             "child_num", user_with_host_name->num_child_,
             K(ret));
  } else if (T_USER_PROFILE != user_profile->type_
             || user_profile->num_child_ != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("2nd child type error",
             "type", user_profile->type_,
             "child_num", user_profile->num_child_,
             K(ret));
  } else if (OB_ISNULL(user_name = user_with_host_name->children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child is null", K(ret));
  } else if (OB_ISNULL(profile_name = user_profile->children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child is null", K(ret));
  } else if (OB_ISNULL(lock_alter_user_profile_stmt = create_stmt<ObAlterUserProfileStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to create ObAlterUserProfileStmt", K(ret));
  } else {
    host_name = user_with_host_name->children_[1];
    obrpc::ObAlterUserProfileArg &arg = lock_alter_user_profile_stmt->get_ddl_arg();
    arg.default_role_flag_ = OB_INVALID_ID;
    arg.user_name_ = ObString(user_name->str_len_, user_name->str_value_);
    if (NULL != host_name) {
      arg.host_name_ = ObString(host_name->str_len_, host_name->str_value_);
    } else {
      arg.host_name_ = ObString(OB_DEFAULT_HOST_NAME);
    }
    if (profile_name->type_ != T_DEFAULT) {
      arg.profile_name_ = ObString(profile_name->str_len_, profile_name->str_value_);
    }
    arg.tenant_id_ = params_.session_info_->get_effective_tenant_id();
    if (OB_FAIL(check_dcl_on_inner_user(parse_tree.type_,
                                        session_info_->get_priv_user_id(),
                                        arg.user_name_,
                                        arg.host_name_))) {
      LOG_WARN("failed to check dcl on inner-user or unsupport to modify reserved user", K(ret),
               K(params_.session_info_->get_user_name()), K(arg.user_name_));
    }
  }
  if (OB_SUCC(ret) && T_SET_ROLE != parse_tree.type_
      && ObSchemaChecker::is_ora_priv_check()) {
    OZ (schema_checker_->check_ora_ddl_priv(
          session_info_->get_effective_tenant_id(),
          session_info_->get_priv_user_id(),
          ObString(""),
          stmt::T_ALTER_USER_PROFILE,
          session_info_->get_enable_role_array()),
          session_info_->get_effective_tenant_id(), session_info_->get_user_id());
  }

  return ret;
}

/**
 * check_role_password_all 
 * A wrapper function to check password when default_role_flag_ == OB_DEFAULT_ROLE_ALL
 * @param  {const ObIArray<uint64_t> &} role_id_array : rold ids being checked
 * @return {int}                                      : OB_SUCCESS, 
 *                                                      OB_ERR_MISSING_OR_INVALID_PASSWORD_FOR_ROLE,
 *                                                      Other error.
 */
int ObAlterUserProfileResolver::check_role_password_all(
    const uint64_t tenant_id,
    const ObIArray<uint64_t> &role_id_array)
{
  int ret = OB_SUCCESS;
  hash::ObHashMap<uint64_t, ObString> roleid_pwd_map;
  if (OB_FAIL(roleid_pwd_map.create(2, "HasTmpCKPwdMa"))) {
    LOG_WARN("failed to create ObHaspMap", K(ret));
  } else if (OB_FAIL(check_role_password(tenant_id, role_id_array, 
                                         roleid_pwd_map, 
                                         true, 
                                         false))) {
    LOG_WARN("failed to check password for SET ROLE ALL", K(ret), K(tenant_id));
  }
  return ret;
}

/**
 * check_role_password 
 * Check whether there is any password mismatch for SET ROLE
 * @param  {const ObIArray<uint64_t> &} role_id_array : role ids being checked
 * @param  {const hash::ObHashMap<uint64_t, ObString>} roleid_pwd_map : role_id and its passwd read
 *         from user input
 * @param  {bool} has_all                             : Whether ALL is in user's input
 * @param  {bool} has_except                          : Whether except is in user's input
 * @return {int}                                      : OB_SUCCESS, 
 *                                                      OB_ERR_MISSING_OR_INVALID_PASSWORD_FOR_ROLE,
 *                                                      Other error.
 */
int ObAlterUserProfileResolver::check_role_password(
    const uint64_t tenant_id,
    const ObIArray<uint64_t> &role_id_array,
    const hash::ObHashMap<uint64_t, ObString> &roleid_pwd_map,
    bool has_all,
    bool has_except)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> checked_roleid_array;
  // Find out which role shall be checked
  if (has_all) {
    if (has_except) {
      ObString pwd;
      for (int i = 0; OB_SUCC(ret) && i < role_id_array.count(); ++i) {
        uint64_t role_id = role_id_array.at(i);
        if (OB_FAIL(roleid_pwd_map.get_refactored(role_id, pwd))) {
          if (OB_HASH_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
            OZ (checked_roleid_array.push_back(role_id));
          } else {
            LOG_WARN("failed to read from roleid_pwd_map", K(ret), K(role_id));
          }
        } else {
          // role_id is in except list, do not append it to checked_roleid_array
        }
      }
    } else { // false == has_except
      // check all of the role ids
      OZ (checked_roleid_array.assign(role_id_array));
    }
  } else { //false == has_all
    for (hash::ObHashMap<uint64_t, ObString>::const_iterator iter = roleid_pwd_map.begin();
         OB_SUCC(ret) && iter != roleid_pwd_map.end(); ++iter) {
      OZ (checked_roleid_array.push_back(iter->first));
    }
  }
  // Check every role for its password
  if (OB_SUCC(ret)) {
    const ObUserInfo *role_info = NULL;
    for (int i = 0; OB_SUCC(ret) && i < checked_roleid_array.count(); ++i) {
      role_info = NULL;
      uint64_t role_id = checked_roleid_array.at(i);
      if (OB_FAIL(params_.schema_checker_->get_user_info(tenant_id, role_id, role_info))) {
        LOG_WARN("get role info failed", K(ret), K(tenant_id));
      } else if (NULL == role_info) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Current role info is NULL", K(ret));
      } else {
        const ObString &saved_pwd = role_info->get_passwd_str();
        ObString pwd;
        // Read password from map
        if (OB_FAIL(roleid_pwd_map.get_refactored(role_id, pwd))) {
          if (OB_HASH_NOT_EXIST == ret) {
            pwd = ObString::make_string("");
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("failed to read from roleid_pwd_map", K(ret), K(role_id));
          }
        }
        // Check if password match 
        if (OB_SUCC(ret)) {
          if (OB_FAIL(check_passwd(pwd, saved_pwd))) {
            LOG_WARN("password mismatch", K(ret));
            LOG_USER_ERROR(OB_ERR_MISSING_OR_INVALID_PASSWORD_FOR_ROLE, 
                           role_info->get_user_name_str().length(),
                           role_info->get_user_name_str().ptr());
          } else {
            // Password match, do nothing
          }
        }
      }
    }
  }
  return ret;
}

/**
 * check_passwd 
 * Function to check whether password mismatch
 * @param  {const ObString &} pwd       : unencrypted password from user input
 * @param  {const ObString &} saved_pwd : encrypted password stored in __all_user
 * @return {int}                        : OB_SUCCESS, 
 *                                        OB_ERR_MISSING_OR_INVALID_PASSWORD_FOR_ROLE,
 *                                        Other error.
 */
int ObAlterUserProfileResolver::check_passwd(const ObString &pwd, const ObString &saved_pwd) 
{
  int ret = OB_SUCCESS;
  if (saved_pwd.empty()) {
    // if saved_pwd is empty, this function return OB_SUCCESS
  } else {
    if (pwd.empty()) {
      ret = OB_ERR_MISSING_OR_INVALID_PASSWORD_FOR_ROLE;
      LOG_WARN("Password empty", K(ret));
    } else {
      ObString pwd_enc;
      char enc_buf[ENC_BUF_LEN] = {0};
      pwd_enc.assign_ptr(enc_buf, ENC_STRING_BUF_LEN);
      if (OB_FAIL(ObEncryptedHelper::encrypt_passwd_to_stage2(pwd, pwd_enc))) {
        LOG_WARN("Encrypt password failed", K(ret));
      } else if (!ObCharset::case_sensitive_equal(pwd_enc, saved_pwd)) {
        ret = OB_ERR_MISSING_OR_INVALID_PASSWORD_FOR_ROLE;
        LOG_WARN("Password for role mismatch", K(ret));
      }
    }
  }
  return ret;
}
