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
#include "sql/resolver/ddl/ob_create_profile_resolver.h"
#include "sql/resolver/ddl/ob_create_profile_stmt.h"
#include "sql/resolver/ddl/ob_ddl_resolver.h"
#include "share/ob_rpc_struct.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_schema_service.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;

namespace sql
{

ObUserProfileResolver::ObUserProfileResolver(ObResolverParams &params)
    : ObDDLResolver(params)
{
}

ObUserProfileResolver::~ObUserProfileResolver()
{
}

int ObUserProfileResolver::fill_arg(int64_t type, ObObj &value, obrpc::ObProfileDDLArg &arg)
{
  int ret = OB_SUCCESS;
  int64_t schema_value = 0;
  double d_value = 0;
  float f_value = 0;
  number::ObNumber num_value;
  number::ObNumber timestamp_value;
  number::ObNumber usec_per_day;

  if (OB_ISNULL(params_.allocator_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid allocator", K(ret));
  } else {
    switch (type) {
    case ObProfileSchema::FAILED_LOGIN_ATTEMPTS:
      if (OB_FAIL(value.get_number(num_value))) {
        LOG_WARN("fail to get numer", K(ret));
      } else if (!num_value.is_valid_int64(schema_value)) {
        ret = OB_INVALID_NUMERIC;
        LOG_WARN("fail to get int64_t", K(ret));
      }
      break;
    case ObProfileSchema::PASSWORD_LOCK_TIME:
    case ObProfileSchema::PASSWORD_LIFE_TIME:
    case ObProfileSchema::PASSWORD_GRACE_TIME:
      switch (value.get_type()) {
      case ObNumberType: {
        if (OB_FAIL(value.get_number(num_value))) {
          LOG_WARN("fail to get numer", K(ret));
        } else if (OB_FAIL(usec_per_day.from(USECS_PER_DAY, *params_.allocator_))) {
          LOG_WARN("fail to from interger", K(ret));
        } else if (OB_FAIL(num_value.mul(usec_per_day, timestamp_value, *params_.allocator_))) {
          LOG_WARN("fail to do mul", K(ret));
        } else if (OB_FAIL(timestamp_value.round(0))) {
          LOG_WARN("fail to do round", K(ret));
        } else if (!timestamp_value.is_valid_int64(schema_value)) {
          ret = OB_INVALID_NUMERIC;
          LOG_WARN("fail to get int64_t", K(ret));
        }
        break;
      }
      case ObDoubleType: {
        if (OB_FAIL(value.get_double(d_value))) {
          LOG_WARN("fail to get double", K(ret));
        } else {
          schema_value = static_cast<int64_t>(d_value * USECS_PER_DAY);
        }
        break;
      }
      case ObFloatType: {
        if (OB_FAIL(value.get_float(f_value))) {
          LOG_WARN("fail to get float", K(ret));
        } else {
          schema_value = static_cast<int64_t>(f_value * USECS_PER_DAY);
        }
        break;
      }
      default:
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not support value type", K(value.get_type()));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "specified value type");
      }
      break;
    default:
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not support profile type", K(type));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "specified profile type");
    }
    if (OB_SUCC(ret) && schema_value <= 0) {
      ret = OB_ERR_INVALID_RESOURCE_LIMIT;
      LOG_USER_ERROR(OB_ERR_INVALID_RESOURCE_LIMIT, ObProfileSchema::PARAM_VALUE_NAMES[type]);
    }
  }

  if (OB_SUCC(ret) &&
      OB_FAIL(arg.schema_.set_value(type, schema_value))) {
    LOG_WARN("fail to set schema value", K(ret));
  }
  return ret;
}

int ObUserProfileResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  stmt::StmtType stmt_type = stmt::T_NONE;
  CHECK_COMPATIBILITY_MODE(session_info_);
  ObUserProfileStmt *create_profile_stmt = NULL;
  ObCompatType compat_type = COMPAT_MYSQL57;
  if (2 != parse_tree.num_child_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expect 2 child, create profile type",
             "actual_num", parse_tree.num_child_,
             K(ret));
  } else if (OB_ISNULL(params_.session_info_)
             || OB_ISNULL(params_.allocator_)
             || OB_ISNULL(schema_checker_->get_schema_guard())) {
    ret = OB_NOT_INIT;
    LOG_WARN("params not init", K(ret), KP(params_.allocator_), KP(params_.session_info_));
  } else if (OB_FAIL(params_.session_info_->get_compatibility_control(compat_type))) {
    LOG_WARN("failed to get compat type", K(ret));
  } else if (!lib::is_oracle_mode()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "create profile except Oracle mode");
  } else if (OB_ISNULL(create_profile_stmt = create_stmt<ObUserProfileStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("Failed to create ObUserProfileStmt", K(ret));
  } else {
    //target
    obrpc::ObProfileDDLArg &arg = create_profile_stmt->get_ddl_arg();
    //tenant_id
    arg.schema_.set_tenant_id(params_.session_info_->get_effective_tenant_id());

    switch(parse_tree.type_) {
    case T_CREATE_PROFILE:
      arg.ddl_type_ = OB_DDL_CREATE_PROFILE;
      stmt_type = stmt::T_CREATE_PROFILE;
      break;
    case T_ALTER_PROFILE:
      arg.ddl_type_ = OB_DDL_ALTER_PROFILE;
      stmt_type = stmt::T_ALTER_PROFILE;
      break;
    case T_DROP_PROFILE:
      arg.ddl_type_ = OB_DDL_DROP_PROFILE;
      stmt_type = stmt::T_DROP_PROFILE;
      break;
    default:
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unsupported ddl type", K(ret), K(parse_tree.type_));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "specified ddl type");
    }
    OX (create_profile_stmt->set_stmt_type(stmt_type));

    //profile name
    if (OB_SUCC(ret)) {
      ParseNode *profile = NULL;
      if (OB_ISNULL(profile = const_cast<ParseNode*>(parse_tree.children_[0]))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("profile name node is null", K(ret));
      } else {
        arg.schema_.set_profile_name(ObString(profile->str_len_, profile->str_value_));
      }
      if (OB_SUCC(ret) && ObSchemaChecker::is_ora_priv_check()) {
        CK (params_.schema_checker_ != NULL);
        OZ (params_.schema_checker_->check_ora_ddl_priv(
              session_info_->get_effective_tenant_id(),
              session_info_->get_priv_user_id(),
              ObString(""),
              stmt_type,
              session_info_->get_enable_role_array()),
              session_info_->get_effective_tenant_id(), session_info_->get_user_id());
      }
    }

    //params
    if (OB_SUCC(ret) && T_DROP_PROFILE != parse_tree.type_) {
      ParseNode *param_list = NULL;
      if (OB_ISNULL(param_list = const_cast<ParseNode*>(parse_tree.children_[1]))
          || param_list->type_ != T_PROFILE_PARAM_LIST) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("profile name node is null", K(ret), K(param_list));
      } else {
        if (T_CREATE_PROFILE == parse_tree.type_) {
          if (OB_FAIL(arg.schema_.set_default_values_v2())) {
            LOG_WARN("fail to get default profile", K(ret));
          }
        } else {
          if (OB_FAIL(arg.schema_.set_invalid_values())) {
            LOG_WARN("fail to set invalid values", K(ret));
          }
        }
      }

      for (int64_t i = 0; OB_SUCC(ret) && i < param_list->num_child_; ++i) {
        ParseNode *param_pair = NULL;
        ParseNode *param_type = NULL;
        ParseNode *param_value = NULL;

        if (OB_ISNULL(param_pair = param_list->children_[i])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("param is null", K(ret));
        } else if (param_pair->type_ != T_PROFILE_PAIR
                   || param_pair->num_child_ != 2) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected node type", K(ret), K(param_pair->type_), K(param_pair->num_child_));
        } else if (OB_ISNULL(param_type = param_pair->children_[0])
                   || param_type->type_ != T_INT) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected node", K(ret), KP(param_type));
        } else if (OB_ISNULL(param_value = param_pair->children_[1])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected node", KP(ret));
        } else {
          if (ObProfileSchema::PASSWORD_VERIFY_FUNCTION == param_type->value_) {
            if (OB_FAIL(resolver_password_verify_function(param_value, arg))) {
              LOG_WARN("fail to resolver verify function name", K(ret));
            }
          } else {
            ObObjParam numeric_value;
            ObString literal_prefix;
            ObDataTypeCastParams dtc_params = params_.session_info_->get_dtc_params();
            const ObLengthSemantics length_semantics
                = params_.session_info_->get_local_nls_length_semantics();
            switch(param_value->type_) {
            case T_INT:
            case T_NUMBER:
            case T_DOUBLE:
            case T_FLOAT:
              if (OB_FAIL(ObResolverUtils::resolve_const(param_value,
                                                         create_profile_stmt->get_stmt_type(),
                                                         *params_.allocator_,
                                                         dtc_params.connection_collation_,
                                                         dtc_params.nls_collation_nation_,
                                                         NULL,
                                                         numeric_value,
                                                         false,
                                                         literal_prefix,
                                                         length_semantics,
                                                         dtc_params.nls_collation_,
                                                         NULL,
                                                         params_.session_info_->get_sql_mode(),
                                                         false, // FIXME: enable decimal int
                                                         compat_type))) {
                LOG_WARN("fail to resolve const", K(ret));
              } else if (OB_FAIL(fill_arg(param_type->value_, numeric_value, arg))) {
                LOG_WARN("fail to fill arg", K(ret), K(param_type->value_));
              }
              break;
            case T_PROFILE_UNLIMITED:
              if (OB_FAIL(arg.schema_.set_value(param_type->value_, INT64_MAX))) {
                LOG_WARN("fail to set schema value", K(ret));
              }
              break;
            case T_PROFILE_DEFAULT:
              if (0 == arg.schema_.get_profile_name_str().case_compare("DEFAULT")) {
                ret = OB_ERR_INVALID_RESOURCE_LIMIT;
                LOG_WARN("invalid default profile can not have a default value", K(ret));
              } else if (OB_FAIL(arg.schema_.set_value(param_type->value_,
                                                       ObProfileSchema::DEFAULT_VALUE))) {
                LOG_WARN("fail to set schema default value", K(param_type->value_), K(ret));
              }
              break;
            default:
              ret = OB_ERR_INVALID_RESOURCE_LIMIT;
              LOG_WARN("unknown value type", K(param_value->type_));
            }
          }
        }
      }
    }

    LOG_DEBUG("profile ddl arg", K(ret), K(arg));
  }

  return ret;
}

int ObUserProfileResolver::resolver_password_verify_function(const ParseNode *node,
                                                             obrpc::ObProfileDDLArg &arg)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node is null", K(ret));
  } else if (node->type_ == T_PROFILE_DEFAULT) {
    arg.schema_.set_password_verify_function("DEFAULT");
  } else if (1 != node->num_child_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expect 1 child",
             "actual_num", node->num_child_,
             K(ret));
  } else if (OB_ISNULL(node->children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node is null", K(ret));
  } else {
    LOG_DEBUG("check node type", K(node->type_));
    ObString password_verify_function(node->children_[0]->str_len_, node->children_[0]->str_value_);
    ObString package_name;
    bool exists = false;
    if (0 == password_verify_function.case_compare("NULL") ||
        0 == password_verify_function.length()) {
      /*do nothing*/
    } else if (OB_FAIL(ObResolverUtils::check_routine_exists(*schema_checker_, *params_.session_info_,
        params_.session_info_->get_database_name(),
        package_name, password_verify_function,
        share::schema::ObRoutineType::ROUTINE_FUNCTION_TYPE, exists))) {
      LOG_WARN("failed to get routine info", K(package_name), K(password_verify_function), K(ret));
    } else if (!exists) {
      ret = OB_OBJECT_NAME_NOT_EXIST;
      LOG_USER_ERROR(OB_OBJECT_NAME_NOT_EXIST, "function");
      LOG_WARN("the function is not exist", K(ret));
    } else {
      arg.schema_.set_password_verify_function(password_verify_function);
    }
  }
  return ret;
}

} //namespace sql
} //namespace oceanbase
