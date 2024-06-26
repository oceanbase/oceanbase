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
#include "ob_create_routine_resolver.h"
#include "ob_create_routine_stmt.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "pl/parser/parse_stmt_item_type.h"
#include "pl/ob_pl_router.h"
#include "pl/ob_pl_package.h"
#include "pl/ob_pl_resolver.h"
#include "share/schema/ob_trigger_info.h"
#ifdef OB_BUILD_ORACLE_PL
#include "pl/ob_pl_udt_object_manager.h"
#endif

namespace oceanbase
{
using namespace common;
using namespace share::schema;
using namespace pl;
namespace sql
{
int ObCreateRoutineResolver::check_dup_routine_param(const ObIArray<ObRoutineParam*> &params, const ObString &param_name)
{
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < params.count(); ++i) {
    ObRoutineParam* param = params.at(i);
    CK(OB_NOT_NULL(param));
    if (OB_SUCC(ret) && 0 == param_name.case_compare(param->get_param_name())) {
      ret = OB_ERR_SP_DUP_PARAM;
      LOG_USER_ERROR(OB_ERR_SP_DUP_PARAM, param_name.length(), param_name.ptr());
      LOG_WARN("Duplicate parameter", K(param_name), K(ret));
      break;
    }
  }
  return ret;
}

int ObCreateRoutineResolver::create_routine_arg(obrpc::ObCreateRoutineArg *&crt_routine_arg)
{
  int ret = OB_SUCCESS;
  ObCreateRoutineStmt *crt_routine_stmt = NULL;
  crt_routine_arg = NULL;
  if (OB_ISNULL(crt_routine_stmt = create_stmt<ObCreateRoutineStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory for create routine stmt failed", K(ret));
  } else {
    crt_routine_arg = &(crt_routine_stmt->get_routine_arg());
  }
  return ret;
}

int ObCreateRoutineResolver::set_routine_info(const ObRoutineType &type,
                                              ObRoutineInfo &routine_info,
                                              bool is_udt_udf)
{
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(session_info_));
  if (OB_SUCC(ret)) {
    if (is_udt_udf) {
      routine_info.set_is_udt_udf();
    }
    routine_info.set_routine_type(type);
    routine_info.set_tenant_id(session_info_->get_effective_tenant_id());
    routine_info.set_owner_id(session_info_->get_user_id());
    routine_info.set_overload(ROUTINE_STANDALONE_OVERLOAD);
    routine_info.set_subprogram_id(ROUTINE_STANDALONE_SUBPROGRAM_ID);
    routine_info.set_tg_timing_event(static_cast<TgTimingEvent>(params_.tg_timing_event_));
    char buf[OB_MAX_PROC_ENV_LENGTH];
    int64_t pos = 0;
    if (OB_FAIL(ObExecEnv::gen_exec_env(*session_info_, buf, OB_MAX_PROC_ENV_LENGTH, pos))) {
      LOG_WARN("failed to generate exec env", K(ret));
    } else {
      routine_info.set_exec_env(ObString(pos, buf));
    }
  }
  return ret;
}

int ObCreateRoutineResolver::analyze_router_sql(obrpc::ObCreateRoutineArg *crt_routine_arg)
{
  int ret = OB_SUCCESS;
  ObRoutineInfo &routine_info = crt_routine_arg->routine_info_;
  CK(OB_NOT_NULL(schema_checker_), OB_NOT_NULL(params_.sql_proxy_), OB_NOT_NULL(session_info_));
  if (OB_SUCC(ret)) {
    pl::ObPLRouter router(routine_info, *session_info_, *schema_checker_->get_schema_guard(), *params_.sql_proxy_);
    ObString route_sql;
    if (OB_FAIL(router.analyze(route_sql, crt_routine_arg->dependency_infos_, routine_info))) {
      LOG_WARN("failed to analyze route sql", K(route_sql), K(ret));
    } else if (OB_FAIL(ObSQLUtils::convert_sql_text_to_schema_for_storing(
                         *allocator_, session_info_->get_dtc_params(), route_sql))) {
      LOG_WARN("fail to convert charset", K(ret));
    } else if (OB_FAIL(routine_info.set_route_sql(route_sql))) {
      LOG_WARN("set routine body failed", K(route_sql), K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObCreateRoutineResolver::resolve_sp_definer(const ParseNode *parse_node,
                                                ObRoutineInfo &routine_info)
{
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(schema_checker_));
  CK(OB_NOT_NULL(schema_checker_->get_schema_guard()));
  CK(OB_NOT_NULL(session_info_));
  CK(OB_NOT_NULL(allocator_));
  ObString user_name, host_name;
  ObString cur_user_name, cur_host_name;
  cur_user_name = session_info_->get_user_name();
  cur_host_name = session_info_->get_host_name();
  if (OB_NOT_NULL(parse_node)) {
    CK(T_USER_WITH_HOST_NAME == parse_node->type_);
    if (OB_SUCC(ret)) {
      const ParseNode *user_node = parse_node->children_[0];
      const ParseNode *host_node = parse_node->children_[1];

      if (OB_ISNULL(user_node)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("user must be specified", K(ret));
      } else {
        user_name.assign_ptr(user_node->str_value_, static_cast<int32_t>(user_node->str_len_));
        // 得区分current_user和“current_user”, 前者需要获取当前用户和host，后者是作为用户名存在
        if (0 == user_name.case_compare("current_user") && T_IDENT == user_node->type_) {
          user_name = cur_user_name;
          host_name = cur_host_name;
        } else if (user_name != cur_user_name && !session_info_->has_user_super_privilege()) {
          ret = OB_ERR_NO_PRIVILEGE;
          LOG_WARN("no privilege", K(ret));
        } else if (OB_ISNULL(host_node)) {
          // 需要检查当前用户是否有超级权限或者set user id的权限，如果权限ok，那么host为%
          if (session_info_->has_user_super_privilege()) {
            host_name.assign_ptr("%", 1);
          } else if (user_name == cur_user_name) {
            host_name = cur_host_name;
          } else {
            ret = OB_ERR_NO_PRIVILEGE;
            LOG_WARN("no privilege", K(ret));
          }
        } else {
          host_name.assign_ptr(host_node->str_value_, static_cast<int32_t>(host_node->str_len_));
          // 显式指定host为%，需要当前用户有超级权限或者set user id的权限
          if (user_name == cur_user_name && host_name == cur_host_name) {
            // do nothing
          } else if (0 == host_name.case_compare("%") && !session_info_->has_user_super_privilege()) {
            ret = OB_ERR_NO_PRIVILEGE;
            LOG_WARN("no privilege", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          // 检查user@host是否在mysql.user表中
          const ObUserInfo* user_info = nullptr;
          if (OB_FAIL(schema_checker_->get_schema_guard()->get_user_info(session_info_->get_effective_tenant_id(),
                                                                         user_name,
                                                                         host_name,
                                                                         user_info))) {
            LOG_WARN("fail to get_user_info", K(ret));
          } else if (OB_ISNULL(user_info)) {
            LOG_USER_WARN(OB_ERR_USER_NOT_EXIST);
            ObPL::insert_error_msg(OB_ERR_USER_NOT_EXIST);
            ret = OB_SUCCESS;
          }
        }
      }
    }
  } else if (lib::is_mysql_mode()) {
    // 不指定definer时，默认为当前用户和host
    user_name = cur_user_name;
    host_name = cur_host_name;
  }
  if (OB_SUCC(ret) && lib::is_mysql_mode()) {
    //user@host作为一个整体存储到priv_user字段
    char tmp_buf[common::OB_MAX_USER_NAME_LENGTH + common::OB_MAX_HOST_NAME_LENGTH + 2] = {};
    snprintf(tmp_buf, sizeof(tmp_buf), "%.*s@%.*s", user_name.length(), user_name.ptr(),
                                                    host_name.length(), host_name.ptr());

    ObString priv_user(tmp_buf);
    if (OB_FAIL(ObSQLUtils::convert_sql_text_to_schema_for_storing(
              *allocator_, session_info_->get_dtc_params(), priv_user))) {
      LOG_WARN("fail to convert charset", K(ret));
    } else if (OB_FAIL(routine_info.set_priv_user(priv_user))) {
      LOG_WARN("failed to set priv user", K(ret));
    }
  }
  return ret;
}

int ObCreateRoutineResolver::resolve_sp_name(const ParseNode *parse_node,
                                             obrpc::ObCreateRoutineArg *crt_routine_arg)
{
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(parse_node), OB_NOT_NULL(session_info_), OB_NOT_NULL(crt_routine_arg));
  if (OB_SUCC(ret)) {
    ObRoutineInfo &proc_info = crt_routine_arg->routine_info_;
    ObString db_name, sp_name;
    if (OB_FAIL(ObResolverUtils::resolve_sp_name(*session_info_, *parse_node, db_name, sp_name))) {
      LOG_WARN("failed to resolve sp name", K(ret));
    } else if (OB_FAIL(proc_info.set_routine_name(sp_name))) {
      LOG_WARN("failed to set routine name", K(sp_name), K(ret));
    } else {
      crt_routine_arg->db_name_ = db_name;
    }
  }
  return ret;
}

int ObCreateRoutineResolver::resolve_sp_body(const ParseNode *parse_node,
                                             ObRoutineInfo &routine_info)
{
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(parse_node));
  if (OB_SUCC(ret)) {
    ObString routine_body;
    routine_body.assign_ptr(parse_node->str_value_, static_cast<int32_t>(parse_node->str_len_));
    if (OB_FAIL(ObSQLUtils::convert_sql_text_to_schema_for_storing(
                  *allocator_, session_info_->get_dtc_params(), routine_body))) {
      LOG_WARN("fail to convert charset", K(ret));
    } else if (OB_FAIL(routine_info.set_routine_body(routine_body))) {
      LOG_WARN("failed to set routine body", K(ret));
    }
  }
  return ret;
}

int ObCreateRoutineResolver::collect_ref_obj_info(int64_t ref_obj_id, int64_t ref_timestamp,
                                                  ObDependencyTableType dependent_type)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(stmt_));
  if (OB_SUCC(ret)) {
    obrpc::ObCreateRoutineArg &crt_routine_arg =
        static_cast<ObCreateRoutineStmt *>(stmt_)->get_routine_arg();
    ObObjectType dep_obj_type = crt_routine_arg.routine_info_.get_object_type();
    OV (ObObjectType::INVALID != dep_obj_type);
    OZ (ObDependencyInfo::collect_dep_info(crt_routine_arg.dependency_infos_, dep_obj_type,
                                           ref_obj_id, ref_timestamp, dependent_type));
  }
  return ret;
}

int ObCreateRoutineResolver::set_routine_param(const ObIArray<ObObjAccessIdx> &access_idxs,
                                               ObRoutineParam &routine_param)
{
  int ret = OB_SUCCESS;
  CK (ObObjAccessIdx::is_table_column(access_idxs)
      || ObObjAccessIdx::is_package_variable(access_idxs)
      || ObObjAccessIdx::is_table(access_idxs)
      || ObObjAccessIdx::is_pkg_type(access_idxs)
      || ObObjAccessIdx::is_udt_type(access_idxs));
  CK (OB_NOT_NULL(params_.session_info_));
  if (ObObjAccessIdx::is_table_column(access_idxs)) {
    const ObTableSchema *table = nullptr;
    CK (2 == access_idxs.count() || 3 == access_idxs.count()); // table.col or db.table.col
    OX (routine_param.set_param_type(ObExtendType));
    OX (routine_param.set_table_col_type());
    OX (routine_param.set_type_name(access_idxs.at(access_idxs.count() - 1).var_name_)); // table column name
    OX (routine_param.set_type_subname(access_idxs.at(access_idxs.count() - 2).var_name_));
    if (OB_FAIL(ret)) {
    } else if (3 == access_idxs.count()) {
      routine_param.set_type_owner(access_idxs.at(0).var_index_);
      CK (OB_NOT_NULL(params_.schema_checker_));
      OZ (params_.schema_checker_->get_table_schema(
              params_.session_info_->get_effective_tenant_id(),
              access_idxs.at(1).var_index_, table));
      CK (OB_NOT_NULL(table));
    } else {
      CK (OB_NOT_NULL(params_.schema_checker_));
      OZ (params_.schema_checker_->get_table_schema(
              params_.session_info_->get_effective_tenant_id(),
              access_idxs.at(0).var_index_, table));
      CK (OB_NOT_NULL(table));
      if (OB_SUCC(ret) && ObCharset::case_compat_mode_equal(table->get_table_name_str(), routine_param.get_type_subname())) {
        routine_param.set_type_owner(table->get_database_id());
      }
    }
    OZ (collect_ref_obj_info(table->get_table_id(), table->get_schema_version(),
                             ObDependencyTableType::DEPENDENCY_TABLE));
  } else if (ObObjAccessIdx::is_package_variable(access_idxs)) {
    CK (2 == access_idxs.count() || 3 == access_idxs.count()); // pkg.var or db.pkg.var
    OX (routine_param.set_param_type(ObExtendType));
    OX (routine_param.set_pkg_var_type());
    OX (routine_param.set_type_name(access_idxs.at(access_idxs.count() - 1).var_name_));
    OX (routine_param.set_type_subname(access_idxs.at(access_idxs.count() - 2).var_name_));
    if (OB_FAIL(ret)) {
    } else if (3 == access_idxs.count()) {
      if (OB_SYS_TENANT_ID == get_tenant_id_by_object_id(access_idxs.at(1).var_index_)) {
        OX (routine_param.set_type_owner(OB_SYS_DATABASE_ID));
      } else {
        OX (routine_param.set_type_owner(access_idxs.at(0).var_index_));
      }
    } else if (OB_SYS_TENANT_ID == get_tenant_id_by_object_id(access_idxs.at(0).var_index_)) { // 系统包中的var
      OX (routine_param.set_type_owner(OB_SYS_DATABASE_ID));
    }
    if (OB_SUCC(ret)) {
      const int64_t package_id = access_idxs.at(access_idxs.count() - 2).var_index_;
      const ObPackageInfo* package_info = nullptr;
      ObSchemaGetterGuard* schema_guard = nullptr;
      CK (OB_NOT_NULL(params_.schema_checker_));
      OX (schema_guard = schema_checker_->get_schema_guard());
      CK (OB_NOT_NULL(schema_guard));
      OZ (schema_guard->get_package_info(get_tenant_id_by_object_id(package_id),
                                         package_id, package_info),
          package_id);
      CK (OB_NOT_NULL(package_info));
      OZ (collect_ref_obj_info(package_id, package_info->get_schema_version(),
                               ObDependencyTableType::DEPENDENCY_PACKAGE));
    }
  } else if (ObObjAccessIdx::is_table(access_idxs)) {
    const ObTableSchema *table = nullptr;
    CK (1 == access_idxs.count() || 2 == access_idxs.count());
    OX (routine_param.set_param_type(ObExtendType));
    OX (routine_param.set_table_row_type());
    OX (routine_param.set_type_name(access_idxs.at(access_idxs.count() - 1).var_name_)); // table name
    if (OB_FAIL(ret)) {
    } else if (2 == access_idxs.count()) {
      routine_param.set_type_owner(access_idxs.at(0).var_index_);
      CK (OB_NOT_NULL(params_.schema_checker_));
      OZ (params_.schema_checker_->get_table_schema(
              params_.session_info_->get_effective_tenant_id(),
              access_idxs.at(1).var_index_, table));
      CK (OB_NOT_NULL(table));
    } else {
      CK (OB_NOT_NULL(params_.schema_checker_));
      OZ (params_.schema_checker_->get_table_schema(
              params_.session_info_->get_effective_tenant_id(),
              access_idxs.at(0).var_index_, table));
      CK (OB_NOT_NULL(table));
      if (OB_SUCC(ret) && ObCharset::case_compat_mode_equal(table->get_table_name_str(), routine_param.get_type_name())) {
        routine_param.set_type_owner(table->get_database_id());
      }
    }
    OZ (collect_ref_obj_info(table->get_table_id(), table->get_schema_version(),
                             ObDependencyTableType::DEPENDENCY_TABLE));
  } else if (ObObjAccessIdx::is_pkg_type(access_idxs)) {
    CK (access_idxs.count() >= 1 && access_idxs.count() <= 3);
    if (OB_FAIL(ret)) {
#ifdef OB_BUILD_ORACLE_PL
    } else if (1 == access_idxs.count()) { // standard package type
      ObPLPackageGuard package_guard(params_.session_info_->get_effective_tenant_id());
      const ObUserDefinedType *user_type = NULL;
      const ObUserDefinedSubType *sub_type = NULL;
      CK (OB_NOT_NULL(params_.schema_checker_));
      OZ (ObResolverUtils::get_user_type(params_.allocator_,
                                         params_.session_info_,
                                         params_.sql_proxy_,
                                         params_.schema_checker_->get_schema_guard(),
                                         package_guard,
                                         access_idxs.at(0).var_index_,
                                         user_type));
      CK (OB_NOT_NULL(sub_type = static_cast<const ObUserDefinedSubType *>(user_type)));
      if (OB_FAIL(ret)) {
      } else if (sub_type->get_base_type()->is_obj_type()) {
        ObDataType data_type = *(sub_type->get_base_type()->get_data_type());
        if (ObNumberTC == data_type.get_type_class() &&
            38 == data_type.get_precision() &&
            0 == data_type.get_scale()) {
          data_type.set_precision(-1);
          data_type.set_scale(-85);
        }
        routine_param.set_param_type(data_type);
      } else {
        OX (routine_param.set_param_type(ObExtendType));
        OX (routine_param.set_pkg_type());
        OX (routine_param.set_type_name(access_idxs.at(access_idxs.count()-1).var_name_));
        OX (routine_param.set_type_subname("STANDARD"));
        OX (routine_param.set_type_owner(OB_SYS_DATABASE_ID));
      }
#endif
    } else {
      OX (routine_param.set_param_type(ObExtendType));
      OX (routine_param.set_pkg_type());
      OX (routine_param.set_type_name(access_idxs.at(access_idxs.count()-1).var_name_));
      if (2 == access_idxs.count()) { // pkg.type
        OX (routine_param.set_type_subname(access_idxs.at(0).var_name_));
        if (OB_SYS_TENANT_ID == get_tenant_id_by_object_id(access_idxs.at(0).var_index_)) { // 系统包中的type
          OX (routine_param.set_type_owner(OB_SYS_DATABASE_ID));
        }
      } else if (3 == access_idxs.count()) { // db.pkg.type
        OX (routine_param.set_type_subname(access_idxs.at(1).var_name_));
        if (OB_SYS_TENANT_ID == get_tenant_id_by_object_id(access_idxs.at(1).var_index_)) {
          OX (routine_param.set_type_owner(OB_SYS_DATABASE_ID));
        } else {
          OX (routine_param.set_type_owner(access_idxs.at(0).var_index_));
        }
      }
      if (OB_SUCC(ret)) {
        const int64_t package_id = access_idxs.at(access_idxs.count() - 2).var_index_;
        const ObPackageInfo* package_info = nullptr;
        ObSchemaGetterGuard* schema_guard = nullptr;
        CK (OB_NOT_NULL(params_.schema_checker_));
        OX (schema_guard = schema_checker_->get_schema_guard());
        CK (OB_NOT_NULL(schema_guard));
        OZ (schema_guard->get_package_info(get_tenant_id_by_object_id(package_id),
                                           package_id, package_info), package_id);
        CK (OB_NOT_NULL(package_info));
        OZ (collect_ref_obj_info(package_id, package_info->get_schema_version(),
                                 ObDependencyTableType::DEPENDENCY_PACKAGE));
      }
    }
  } else if (ObObjAccessIdx::is_udt_type(access_idxs)) {
    CK (access_idxs.count() >= 1 && access_idxs.count() <= 2);
    OX (routine_param.set_param_type(ObExtendType));
    OX (routine_param.set_udt_type());
    OX (routine_param.set_type_name(access_idxs.at(access_idxs.count()-1).var_name_));
    if (OB_FAIL(ret)) {
    } else if (2 == access_idxs.count()) {
      if (OB_SYS_TENANT_ID == get_tenant_id_by_object_id(access_idxs.at(1).var_index_)) {
        // system type, set owner is oceanbase
        routine_param.set_type_owner(OB_SYS_DATABASE_ID);
      } else {
        routine_param.set_type_owner(access_idxs.at(0).var_index_);
      }
    } else if (OB_SYS_TENANT_ID == get_tenant_id_by_object_id(access_idxs.at(0).var_index_)) {
      // system type, set owner is oceanbase
      routine_param.set_type_owner(OB_SYS_DATABASE_ID);
    }
    if (OB_SUCC(ret)) {
      const int64_t udt_id = access_idxs.at(access_idxs.count() - 1).var_index_;
      const ObUDTTypeInfo* udt_info = nullptr;
      CK (OB_NOT_NULL(params_.schema_checker_));
      OZ (params_.schema_checker_->get_udt_info(get_tenant_id_by_object_id(udt_id),
                                                udt_id, udt_info), udt_id);
      CK (OB_NOT_NULL(udt_info));
      OZ (collect_ref_obj_info(udt_id, udt_info->get_schema_version(),
                               ObDependencyTableType::DEPENDENCY_TYPE));
    }
  }
  return ret;
}

int ObCreateRoutineResolver::resolve_param_type(const ParseNode *type_node,
                                                const ObString &param_name,
                                                ObSQLSessionInfo &session_info,
                                                ObRoutineParam &routine_param)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(type_node));
  CK (OB_NOT_NULL(allocator_));
  if (OB_SUCC(ret)) {
    if (T_SP_ROWTYPE == type_node->type_
        || T_SP_TYPE == type_node->type_) { // %Type %RowType
      ObArray<ObObjAccessIdx> access_idxs;
      ObArray<ObObjAccessIdent> obj_access_idents;
      CK (OB_LIKELY(2 == type_node->num_child_),
          OB_NOT_NULL(type_node->children_[0]),
          OB_ISNULL(type_node->children_[1]),
          OB_LIKELY(T_SP_OBJ_ACCESS_REF == type_node->children_[0]->type_),
          OB_NOT_NULL(schema_checker_),
          OB_NOT_NULL(schema_checker_->get_schema_guard()),
          OB_NOT_NULL(params_.expr_factory_),
          OB_NOT_NULL(params_.sql_proxy_));
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ObPLResolver::resolve_obj_access_node(*(type_node->children_[0]),
                                                               session_info,
                                                               *(params_.expr_factory_),
                                                               *(schema_checker_->get_schema_guard()),
                                                               *(params_.sql_proxy_),
                                                               obj_access_idents,
                                                               access_idxs,
                                                               params_.package_guard_))) {
        // maybe dependent object not exist yet!
        LOG_WARN("failed to transform from iparam", K(ret));
        if (ObPLResolver::is_object_not_exist_error(ret)) {
          ret = OB_SUCCESS;
          ObArray<ObObjAccessIdent> obj_access_idents;
          ObPLExternTypeInfo extern_type_info;
          CK (OB_NOT_NULL(params_.expr_factory_));
          OZ (ObPLResolver::resolve_obj_access_idents(*(type_node->children_[0]),
                                                      *(params_.expr_factory_),
                                                      obj_access_idents,
                                                      session_info));
          OZ (ObPLResolver::resolve_extern_type_info(T_SP_ROWTYPE == type_node->type_,
                                                     *(schema_checker_->get_schema_guard()),
                                                     session_info,
                                                     obj_access_idents,
                                                     &extern_type_info));
          OX (routine_param.set_param_type(ObExtendType));
          OX (routine_param.set_type_owner(extern_type_info.type_owner_));
          OX (routine_param.set_type_name(extern_type_info.type_name_));
          OX (routine_param.set_type_subname(extern_type_info.type_subname_));
          OX (routine_param.set_extern_type_flag(
                              static_cast<ObParamExternType>(extern_type_info.flag_)));
          if (OB_SUCC(ret)) {
            LOG_USER_ERROR(OB_ERR_SP_UNDECLARED_TYPE,
                           extern_type_info.type_name_.length(), extern_type_info.type_name_.ptr());
          }
        }
      } else {
        uint64_t owner_id = OB_INVALID_ID;
        CK (!session_info.get_database_name().empty());
        OZ (schema_checker_->get_database_id(session_info.get_effective_tenant_id(),
                                             session_info.get_database_name(),
                                             owner_id));
        OX (routine_param.set_type_owner(owner_id));
        CK (OB_LIKELY(access_idxs.count() > 0));
        if (OB_SUCC(ret)) {
          if (T_SP_TYPE == type_node->type_) {
            if (ObObjAccessIdx::is_table_column(access_idxs)
                || ObObjAccessIdx::is_package_variable(access_idxs)) {
              OZ (set_routine_param(access_idxs, routine_param));
            } else {
              ret = OB_ERR_TYPE_DECL_ILLEGAL;
              LOG_WARN("PLS-00206: %TYPE must be applied to a variable, column, field or attribute",
                      K(ret), K(access_idxs));
            }
          } else if (ObObjAccessIdx::is_table(access_idxs) ||
                     ObObjAccessIdx::is_package_cursor_variable(access_idxs)) {
            OZ (set_routine_param(access_idxs, routine_param));
          } else {
            ret = OB_ERR_WRONG_ROWTYPE;
            LOG_WARN("PLS-00310: with %ROWTYPE attribute, ident must name a table, cursor or cursor-variable",
                     K(ret), K(access_idxs));
          }
        }
      }
    } else if (T_SP_OBJ_ACCESS_REF == type_node->type_) { // User Define Type
      ObArray<ObObjAccessIdx> access_idxs;
      ObArray<ObObjAccessIdent> obj_access_idents;
      uint64_t current_db_id = OB_INVALID_ID;
      CK (OB_NOT_NULL(schema_checker_),
          OB_NOT_NULL(schema_checker_->get_schema_guard()));
      CK (OB_NOT_NULL(params_.expr_factory_));
      OZ (schema_checker_->get_database_id(session_info.get_effective_tenant_id(),
                                           session_info.get_database_name(),
                                           current_db_id));
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ObPLResolver::resolve_obj_access_node(
                                                      *(type_node),
                                                      session_info,
                                                      *(params_.expr_factory_),
                                                      *(schema_checker_->get_schema_guard()),
                                                      *(params_.sql_proxy_),
                                                      obj_access_idents,
                                                      access_idxs,
                                                      params_.package_guard_))) {
        // maybe dependent object not exist yet!
        LOG_WARN("failed to transform from iparam", K(ret));
        if (ObPLResolver::is_object_not_exist_error(ret)) {
          ret = OB_SUCCESS;
          ObArray<ObObjAccessIdent> obj_access_idents;
          ObPLExternTypeInfo extern_type_info;
          CK (OB_NOT_NULL(params_.expr_factory_));
          OZ (ObPLResolver::resolve_obj_access_idents(*(type_node),
                                                      *(params_.expr_factory_),
                                                      obj_access_idents,
                                                      session_info));
          if (OB_FAIL(ret)) {
          } else if (1 == obj_access_idents.count()
                     && 0 == obj_access_idents.at(0).access_name_.case_compare("SYS_REFCURSOR")) {
            routine_param.set_type_name(obj_access_idents.at(0).access_name_);
            routine_param.set_sys_refcursor_type();
          } else if (1 == obj_access_idents.count()) { //udt
            routine_param.set_type_name(obj_access_idents.at(0).access_name_);
            routine_param.set_type_owner(current_db_id);
            routine_param.set_udt_type();
          } else if (3 == obj_access_idents.count()) { //db.pkg.type
            uint64_t owner_id = OB_INVALID_ID;
            OZ (schema_checker_->get_database_id(session_info.get_effective_tenant_id(),
                                                 obj_access_idents.at(0).access_name_,
                                                 owner_id));
            OX (routine_param.set_type_name(obj_access_idents.at(2).access_name_));
            OX (routine_param.set_type_owner(owner_id));
            OX (routine_param.set_type_subname(obj_access_idents.at(1).access_name_));
            OX (routine_param.set_pkg_type());
          } else if (2 == obj_access_idents.count()) {//db.type or pkg.type
            bool exist = false;
            uint64_t owner_id = OB_INVALID_ID;
            OZ (schema_checker_->get_schema_guard()->check_database_exist(
              session_info.get_effective_tenant_id(),
              obj_access_idents.at(0).access_name_,
              exist,
              &owner_id));
            if (OB_FAIL(ret)) {
            } else if (exist) {
              routine_param.set_type_name(obj_access_idents.at(1).access_name_);
              routine_param.set_type_owner(owner_id);
              routine_param.set_udt_type();
            } else {
              routine_param.set_type_name(obj_access_idents.at(1).access_name_);
              routine_param.set_type_owner(current_db_id);
              routine_param.set_type_subname(obj_access_idents.at(0).access_name_);
              routine_param.set_pkg_type();
            }
          }
          OX (routine_param.set_param_type(ObExtendType));
        }
      } else {
        CK (ObObjAccessIdx::is_pkg_type(access_idxs) || ObObjAccessIdx::is_udt_type(access_idxs));
        OZ (set_routine_param(access_idxs, routine_param));
        if (OB_SUCC(ret)
            && routine_param.is_extern_type()
            && OB_INVALID_ID == routine_param.get_type_owner()) {
          OX (routine_param.set_type_owner(current_db_id));
        }
      }
    } else { // Basic Type
      ObPLDataType data_type;
      data_type.reset();
      OZ (ObPLResolver::resolve_sp_scalar_type(*allocator_,
                                               type_node,
                                               param_name,
                                               *session_info_,
                                               data_type,
                                               param_name.empty() ? false : true));
      if (OB_SUCC(ret) && data_type.is_pl_integer_type()) {
        routine_param.set_pl_integer_type(data_type.get_pl_integer_type());
      }
      if (OB_SUCC(ret) && OB_NOT_NULL(data_type.get_data_type())
          && data_type.get_data_type()->get_charset_type() == CHARSET_ANY) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, 
                       "character set ANY_CS not supported in standalone function/procedure");
        LOG_WARN("character set ANY_CS not supported in standalone function/procedure", K(ret));
      }
      CK (OB_NOT_NULL(data_type.get_data_type()));
      OZ (routine_param.set_extended_type_info(data_type.get_type_info()));
      OX (routine_param.set_param_type(*(data_type.get_data_type())));
    }
  }
  return ret;
}

int ObCreateRoutineResolver::analyze_expr_type(ObRawExpr *&expr,
                                               ObRoutineInfo &routine_info)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(expr));
  if (OB_FAIL(ret)) {
  } else if (T_OP_GET_PACKAGE_VAR == expr->get_expr_type()) {
    OX (routine_info.set_rps());
  } else if (T_FUN_UDF == expr->get_expr_type()) {
    OX (routine_info.set_external_state());
  } else {
    for (int64_t i = 0;
         OB_SUCC(ret) &&
         (!routine_info.is_rps() || !routine_info.is_external_state()) &&
         i < expr->get_param_count();
         ++i) {
      OZ (analyze_expr_type(expr->get_param_expr(i), routine_info));
    }
  }
  return ret;
}

int ObCreateRoutineResolver::resolve_param_list(const ParseNode *param_list, ObRoutineInfo &routine_info)
{
  int ret = OB_SUCCESS;
  ObString param_name;
  ObRoutineParam routine_param;
  ObPLDataType data_type;
  CK(OB_NOT_NULL(session_info_));
  if (OB_SUCC(ret) && param_list != NULL) {
    CK(OB_LIKELY(T_SP_PARAM_LIST == param_list->type_));
    CK(OB_NOT_NULL(param_list->children_));
    if (param_list->num_child_ > OB_MAX_PROC_PARAM_COUNT) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("too many formal parameters, max number of formal parameters"
               "in an explicit cursor, function, or procedure is 65536!",
               K(ret), K(OB_MAX_PROC_PARAM_COUNT));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "number of formal parameters large than 65536");
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < param_list->num_child_; ++i) {
      const ParseNode *param_node = param_list->children_[i];
      const ParseNode *name_node = NULL;
      const ParseNode *type_node = NULL;

      CK(OB_NOT_NULL(param_node));
      CK(OB_LIKELY(T_SP_PARAM == param_node->type_));
      CK(OB_NOT_NULL(param_node->children_));
      CK(OB_NOT_NULL(name_node = param_node->children_[0]));
      CK(OB_NOT_NULL(type_node = param_node->children_[1]));
      routine_param.reset();
      data_type.reset();
      if (OB_SUCC(ret)) {
        routine_param.set_tenant_id(session_info_->get_effective_tenant_id());
        routine_param.set_sequence(routine_info.is_procedure()?i+1:i+2);
        routine_param.set_subprogram_id(routine_info.get_subprogram_id());
        routine_param.set_param_position(i+1);
        routine_param.set_param_level(0); //todo user defined type guangang.gg
        param_name.assign_ptr(name_node->str_value_, static_cast<int32_t>(name_node->str_len_));
        if (OB_FAIL(check_dup_routine_param(routine_info.get_routine_params(), param_name))) {
          LOG_WARN("fail to check dup routine param", K(param_name), K(ret));
        } else if (OB_FAIL(routine_param.set_param_name(param_name))) {
          LOG_WARN("set param name failed", K(ret), K(param_name));
        } else if (OB_FAIL(resolve_param_type(type_node,
                                              param_name,
                                              *session_info_,
                                              routine_param))) {
          LOG_WARN("failed to resolve param type", K(ret), K(param_name));
        }
      }
      if (OB_SUCC(ret)) {
        switch (param_node->int32_values_[0]) {
        case MODE_IN:
          routine_param.set_in_sp_param_flag();
          break;
        case MODE_OUT:
          routine_param.set_out_sp_param_flag();
          routine_info.set_has_out_param();
          break;
        case MODE_INOUT:
          routine_param.set_inout_sp_param_flag();
          routine_info.set_has_out_param();
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("sp inout flag is invalid", K(param_node->value_));
          break;
        }
      }
      if (OB_SUCC(ret) && 1 == param_node->int32_values_[1]) {
        routine_param.set_nocopy_param();
      }
#ifdef OB_BUILD_ORACLE_PL
      if (OB_SUCC(ret) && 0 == param_name.case_compare("SELF")) {
        routine_param.set_is_self_param();
      }
#endif
      // 设置default value expr str
      if (OB_SUCC(ret)
          && 3 == param_node->num_child_ // oracle mode has default node
          && OB_NOT_NULL(param_node->children_[2])) {
        if (lib::is_mysql_mode()) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("stored procedure's paramlist not supported default value in mysql mode", K(ret), K(lbt()));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "stored procedure's paramlist use default value in mysql mode");
        } else {
          const ParseNode *default_node = param_node->children_[2];
          if (OB_UNLIKELY(default_node->type_ != T_SP_DECL_DEFAULT)
              || OB_ISNULL(default_node->children_[0])) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("wrong default value node", K(ret));
          } else if (!routine_param.is_in_sp_param()) {
            ret = OB_ERR_OUT_PARAM_HAS_DEFAULT;
            LOG_WARN("PLS-00230: out or in out parameter can not has default value", K(ret));
          } else {
            ObString default_value(static_cast<int32_t>(default_node->str_len_),
                                  default_node->str_value_);
            ObRawExpr *default_expr = NULL;
            ObPLDataType pl_type;
            ObObjType src_type = ObMaxType;
            uint64_t src_type_id = OB_INVALID_ID;
            ObRoutineMatchInfo::MatchInfo match_info;
            OZ (pl::ObPLDataType::transform_from_iparam(&(routine_param),
                                                        *(schema_checker_->get_schema_guard()),
                                                        *(session_info_),
                                                        *(allocator_),
                                                        *(params_.sql_proxy_),
                                                        pl_type));
            // 默认值在CreateRoutine阶段不需要计算,执行时计算,但是这里要resolve下,避免用户使用非法变量
            OZ (pl::ObPLResolver::resolve_raw_expr(*(default_node->children_[0]),
                                                  params_,
                                                  default_expr,
                                                  false /*for_writer*/));
            CK (OB_NOT_NULL(default_expr));
            OZ (ObResolverUtils::get_type_and_type_id(default_expr, src_type, src_type_id));
            OZ (ObResolverUtils::check_type_match(
              params_, match_info, default_expr, src_type, src_type_id, pl_type));
            OZ (ObSQLUtils::convert_sql_text_to_schema_for_storing(
              *allocator_, session_info_->get_dtc_params(), default_value));
            OZ (routine_param.set_default_value(default_value));
            OX (match_info.need_cast_ ? routine_param.set_default_cast() : void(NULL));
            OZ (analyze_expr_type(default_expr, routine_info));
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(routine_info.add_routine_param(routine_param))) {
          LOG_WARN("add proc param failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObCreateRoutineResolver::resolve_clause_list(
  const ParseNode *clause_list, share::schema::ObRoutineInfo &func_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(clause_list)) { // do nothing ...
  } else {
    ObProcType proc_type =
      ObRoutineType::ROUTINE_FUNCTION_TYPE == func_info.get_routine_type() ?
        ObProcType::STANDALONE_FUNCTION
          : ObRoutineType::ROUTINE_PROCEDURE_TYPE == func_info.get_routine_type() ?
            ObProcType::STANDALONE_PROCEDURE
              : ObProcType::INVALID_PROC_TYPE;
    ObPLDataType ret_type;
    if (func_info.is_function()) {
      const ObRoutineParam *routine_param = NULL;
      CK (OB_NOT_NULL(func_info.get_ret_info()));
      OX (routine_param = static_cast<const ObRoutineParam*>(func_info.get_ret_info()));
      CK (OB_NOT_NULL(routine_param));
      OZ (pl::ObPLDataType::transform_from_iparam(routine_param,
                                                  *schema_checker_->get_schema_guard(),
                                                  *session_info_,
                                                  *allocator_,
                                                  *params_.sql_proxy_,
                                                  ret_type));
    }
    CK (proc_type != ObProcType::INVALID_PROC_TYPE);
    OZ (ObPLResolver::resolve_sf_clause(clause_list, &func_info, proc_type, ret_type));
  }
  return ret;
}

int ObCreateRoutineResolver::resolve_ret_type(const ParseNode *ret_type_node,
                                              share::schema::ObRoutineInfo &func_info)
{
  int ret = OB_SUCCESS;
  ObRoutineParam ret_type_param;
  CK (OB_NOT_NULL(session_info_));
  CK (OB_NOT_NULL(ret_type_node));
  CHECK_COMPATIBILITY_MODE(session_info_);
  OX (ret_type_param.set_tenant_id(session_info_->get_effective_tenant_id()));
  OX (ret_type_param.set_sequence(ROUTINE_RET_TYPE_SEQUENCE));
  OX (ret_type_param.set_param_position(ROUTINE_RET_TYPE_POSITION));
  OX (ret_type_param.set_subprogram_id(func_info.get_subprogram_id()));
  OX (ret_type_param.set_param_level(0));
  OZ (resolve_param_type(ret_type_node, ObString(), *session_info_, ret_type_param));
  OZ (func_info.add_routine_param(ret_type_param));
  return ret;
}

int ObCreateRoutineResolver::resolve_replace(const ParseNode *parse_node, obrpc::ObCreateRoutineArg *crt_routine_arg)
{
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(crt_routine_arg), OB_NOT_NULL(parse_node));
  if (OB_SUCC(ret)) {
    crt_routine_arg->is_or_replace_ = parse_node->int32_values_[0];
  }
  return ret;
}

int ObCreateRoutineResolver::resolve_editionable(
  const ParseNode *parse_node, obrpc::ObCreateRoutineArg *crt_routine_arg)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(crt_routine_arg), OB_NOT_NULL(parse_node));
  if (OB_SUCC(ret) && 1 == parse_node->int32_values_[1]) {
    crt_routine_arg->routine_info_.set_noneditionable();
  }
  return ret;
}

int ObCreateRoutineResolver::resolve_aggregate_body(
  const ParseNode *parse_node, ObRoutineInfo &routine_info)
{
  int ret = OB_SUCCESS;
  ObString db_name, type_name, real_db_name;
  const share::schema::ObUDTTypeInfo *udt_info = NULL;

  CK (OB_NOT_NULL(parse_node), OB_NOT_NULL(session_info_));
  CK (OB_NOT_NULL(schema_checker_));
  CK (T_SF_AGGREGATE_BODY == parse_node->type_);
  CK (1 == parse_node->num_child_);
  CK (OB_NOT_NULL(parse_node->children_[0]));

  OZ (ObResolverUtils::resolve_sp_name(
    *session_info_, *(parse_node->children_[0]), db_name, type_name, false));

  if (OB_SUCC(ret)) {
    if (db_name.empty()) {
      if (session_info_->get_database_name().empty() || OB_INVALID_ID == session_info_->get_database_id()) {
        ret = OB_ERR_NO_DB_SELECTED;
        LOG_USER_ERROR(OB_ERR_NO_DB_SELECTED);
        LOG_WARN("No Database Selected", K(ret));
      } else {
        real_db_name = session_info_->get_database_name();
      }
    } else {
      real_db_name = db_name;
    }
  }

  OZ (schema_checker_->get_udt_info(
    session_info_->get_effective_tenant_id(), real_db_name, type_name, udt_info));
  if (OB_ISNULL(udt_info) // try sys udt
      && (0 == real_db_name.case_compare(OB_ORA_SYS_SCHEMA_NAME)
          || 0 == real_db_name.case_compare(OB_SYS_DATABASE_NAME))) {
    OZ (schema_checker_->get_udt_info(
      OB_SYS_TENANT_ID, OB_SYS_DATABASE_NAME, type_name, udt_info));
  }

  if (OB_ISNULL(udt_info) && ret != OB_ERR_NO_DB_SELECTED) { // try synonym
    uint64_t tenant_id = session_info_->get_effective_tenant_id();
    uint64_t database_id = session_info_->get_database_id();
    ObSynonymChecker synonym_checker;
    uint64_t object_database_id = OB_INVALID_ID;
    ObString object_name;
    bool exist = false;
    if (!db_name.empty() // try database name synonym
        && (OB_FAIL(schema_checker_->get_database_id(tenant_id, db_name, database_id))
            || OB_INVALID_ID == database_id)) {
      database_id = session_info_->get_database_id();
      OZ (ObResolverUtils::resolve_synonym_object_recursively(*schema_checker_,
                                                              synonym_checker,
                                                              tenant_id,
                                                              database_id,
                                                              db_name,
                                                              object_database_id,
                                                              object_name,
                                                              exist,
                                                              true));
      if (OB_SUCC(ret) && exist) {
        OZ (schema_checker_->get_udt_info(tenant_id, object_name, type_name, udt_info));
      }
    } else { // try type name synonym
      OZ (ObResolverUtils::resolve_synonym_object_recursively(*schema_checker_,
                                                              synonym_checker,
                                                              tenant_id,
                                                              database_id,
                                                              type_name,
                                                              object_database_id,
                                                              object_name,
                                                              exist,
                                                              true));
      if (OB_FAIL(ret) || !exist) {
        // do nothing ...
      } else {
        if (object_database_id != session_info_->get_database_id()) {
          const share::schema::ObDatabaseSchema *database_schema = NULL;
          OZ (schema_checker_->get_database_schema(tenant_id, object_database_id, database_schema));
          CK (OB_NOT_NULL(database_schema));
          OX (real_db_name = database_schema->get_database_name_str());
        }
        OZ (schema_checker_->get_udt_info(tenant_id, real_db_name, object_name, udt_info));
      }
    }
  }


  if ((OB_SUCC(ret) && OB_ISNULL(udt_info)) || OB_SYNONYM_NOT_EXIST == ret) {
    ret = OB_ERR_SP_UNDECLARED_VAR;
    LOG_WARN("PLS-00201: identifier type_name must be declared",
             K(ret), K(type_name), K(db_name));
    LOG_USER_ERROR(OB_ERR_SP_UNDECLARED_VAR, type_name.length(), type_name.ptr());
  }

  OX (routine_info.set_type_id(udt_info->get_type_id()));
  return ret;
}

int ObCreateRoutineResolver::resolve_impl(ObRoutineType routine_type,
                                     const ParseNode *sp_definer_node,
                                     const ParseNode *name_node,
                                     const ParseNode *body_node,
                                     const ParseNode *ret_node,
                                     const ParseNode *param_node,
                                     const ParseNode *clause_list,
                                     obrpc::ObCreateRoutineArg *crt_routine_arg,
                                     bool is_udt_udf)
{
  int ret = OB_SUCCESS;
  bool need_reset_default_database = false;
  uint64_t old_database_id = OB_INVALID_ID;
  ObSqlString old_database_name;
  CK(OB_NOT_NULL(session_info_), OB_NOT_NULL(allocator_), OB_NOT_NULL(schema_checker_));
  CHECK_COMPATIBILITY_MODE(session_info_);
  CK (INVALID_ROUTINE_TYPE != routine_type);

  OZ(resolve_sp_definer(sp_definer_node, crt_routine_arg->routine_info_));
  OZ (resolve_sp_name(name_node, crt_routine_arg));
  OZ (set_routine_info(routine_type, crt_routine_arg->routine_info_, is_udt_udf));

  if (OB_SUCC(ret) && ObSchemaChecker::is_ora_priv_check()) {
    OZ (schema_checker_->check_ora_ddl_priv(session_info_->get_effective_tenant_id(),
                                            session_info_->get_priv_user_id(),
                                            crt_routine_arg->db_name_,
                                            stmt::T_CREATE_ROUTINE,
                                            session_info_->get_enable_role_array()));
  }
  if (OB_SUCC(ret)) {
    uint64_t database_id = OB_INVALID_ID;
    const share::schema::ObDatabaseSchema *database_schema = NULL;
    OZ (schema_checker_->get_schema_guard()->get_database_id(session_info_->get_effective_tenant_id(),
                                                             crt_routine_arg->db_name_,
                                                             database_id));
    OZ (schema_checker_->get_schema_guard()->get_database_schema(session_info_->get_effective_tenant_id(),
                                                                 database_id, database_schema));
    if (OB_FAIL(ret) || OB_ISNULL(database_schema)) {
      ret = OB_ERR_BAD_DATABASE;
      LOG_WARN("fail to get database schema", K(ret));
      LOG_USER_ERROR(OB_ERR_BAD_DATABASE, crt_routine_arg->db_name_.length(), crt_routine_arg->db_name_.ptr());
    }
    if (OB_FAIL(ret)) {
    }else if (database_schema->get_database_name_str() != session_info_->get_database_name()) {
      OZ (old_database_name.append(session_info_->get_database_name()));
      OX (old_database_id = session_info_->get_database_id());
      OZ (session_info_->set_default_database(database_schema->get_database_name_str()));
      OX (session_info_->set_database_id(database_id));
      OX (crt_routine_arg->routine_info_.set_database_id(database_id));
      OX (need_reset_default_database = true);
    } else {
      OX (crt_routine_arg->routine_info_.set_database_id(database_id));
    }
  }
  OZ (resolve_sp_body(body_node, crt_routine_arg->routine_info_));
  if (OB_SUCC(ret) && ROUTINE_FUNCTION_TYPE == routine_type) {
    OZ (resolve_ret_type(ret_node, crt_routine_arg->routine_info_));
  }
  OZ (resolve_param_list(param_node, crt_routine_arg->routine_info_));
  OZ (resolve_clause_list(clause_list, crt_routine_arg->routine_info_));
  CK (OB_NOT_NULL(body_node));
  if (OB_FAIL(ret)) {
  } else if (T_SF_AGGREGATE_BODY == body_node->type_) {
    if (crt_routine_arg->routine_info_.get_param_count() != 1) {
      ret = OB_ERR_INCORRECT_ARGUMENTS;
      LOG_WARN("PLS-00652: aggregate functions should have exactly one argument",
               K(ret), K(crt_routine_arg->routine_info_.get_param_count()));
    }
    OZ (resolve_aggregate_body(body_node, crt_routine_arg->routine_info_));
    OX (crt_routine_arg->routine_info_.set_is_aggregate());
  } else {
    OZ (analyze_router_sql(crt_routine_arg));
  }
  if (need_reset_default_database) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = session_info_->set_default_database(old_database_name.string()))) {
      ret = OB_SUCCESS == ret ? tmp_ret : ret; // 不覆盖错误码
      LOG_ERROR("failed to reset default database", K(ret), K(tmp_ret), K(old_database_name));
    } else {
      session_info_->set_database_id(old_database_id);
    }
  }
  return ret;
}

int ObCreateRoutineResolver::resolve(const ParseNode &parse_tree,
                                     const ParseNode *sp_definer_node,
                                     const ParseNode *name_node,
                                     const ParseNode *body_node,
                                     const ParseNode *ret_node,
                                     const ParseNode *param_node,
                                     const ParseNode *clause_list, 
                                     obrpc::ObCreateRoutineArg *crt_routine_arg)
{
  int ret = OB_SUCCESS;

  CK (OB_NOT_NULL(crt_routine_arg));
  CK (OB_NOT_NULL(session_info_));
  CK (OB_NOT_NULL(allocator_));
  CK (OB_NOT_NULL(schema_checker_));

  CHECK_COMPATIBILITY_MODE(session_info_);
  if (OB_SUCC(ret)) {
    ObRoutineType type = T_SF_CREATE == parse_tree.type_ ? ROUTINE_FUNCTION_TYPE : 
                 T_SP_CREATE == parse_tree.type_ ? ROUTINE_PROCEDURE_TYPE : INVALID_ROUTINE_TYPE;
    if (lib::is_oracle_mode() && OB_FAIL(resolve_replace(&parse_tree, crt_routine_arg))) {
      LOG_WARN("failed to resolve replace", K(ret));
    } else if (lib::is_mysql_mode()
               && session_info_->is_inner()
               && FALSE_IT(crt_routine_arg->is_or_replace_ = true)) {
      // MySQL模式下该字段被复用未是否是InnerSQL发送的请求。用于恢复MySQL下被放入回收站的Routine。
      // Oracle模式下不做处理, 因为Oracle模式下PL对象不进入回收站。
    } else if (lib::is_oracle_mode() && OB_FAIL(resolve_editionable(&parse_tree,
                                                                    crt_routine_arg))) {
      LOG_WARN("failed to resolve editionable", K(ret));
    } else if (OB_FAIL(resolve_impl(type,
                                    sp_definer_node,
                                    name_node,
                                    body_node,
                                    ret_node,
                                    param_node,
                                    clause_list,
                                    crt_routine_arg))) {
      LOG_WARN("failed to resolve routine info", K(ret));
    } else {
      if (parse_tree.value_ != 0 && is_mysql_mode()) {
        OX (crt_routine_arg->with_if_not_exist_ = parse_tree.value_);
      }
    }
  }
  return ret;
}

int ObCreateRoutineResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  obrpc::ObCreateRoutineArg *crt_routine_arg = NULL;
  OZ (create_routine_arg(crt_routine_arg));
  if (OB_SUCC(ret) && OB_ISNULL(crt_routine_arg)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory for create routine stmt failed", K(ret));
  }
  OZ (resolve_impl(parse_tree, crt_routine_arg));
  if (OB_SUCC(ret)) {
    ObErrorInfo &error_info = crt_routine_arg->error_info_;
    ObRoutineInfo &routine_info = crt_routine_arg->routine_info_;
    error_info.collect_error_info(&routine_info);
  }
  return ret;
}

int ObCreateProcedureResolver::resolve_impl(
  const ParseNode &parse_tree, obrpc::ObCreateRoutineArg *crt_routine_arg)
{
  int ret = OB_SUCCESS;

  CK (OB_NOT_NULL(crt_routine_arg));
  CK(OB_NOT_NULL(session_info_),
     OB_LIKELY(T_SP_CREATE == parse_tree.type_), OB_NOT_NULL(parse_tree.children_),
     OB_LIKELY((5 == parse_tree.num_child_ && !lib::is_oracle_mode())
            || (1 == parse_tree.num_child_
                && lib::is_oracle_mode()
                && NULL != parse_tree.children_[0]
                && T_SP_SOURCE == parse_tree.children_[0]->type_
                && 4 == parse_tree.children_[0]->num_child_)));

  if (OB_SUCC(ret)) {
    bool is_oracle_compatible = lib::is_oracle_mode();
    const ParseNode &source_tree = is_oracle_compatible ? *parse_tree.children_[0] : parse_tree;
    const ParseNode *sp_definer_node = is_oracle_compatible ? nullptr : source_tree.children_[0];
    const ParseNode *sp_name_node = source_tree.children_[is_oracle_compatible ? 0 : 1];
    const ParseNode *body_node = source_tree.children_[is_oracle_compatible ? 3 : 4];
    const ParseNode *param_node = source_tree.children_[is_oracle_compatible ? 1 : 2];
    const ParseNode *clause_node = source_tree.children_[is_oracle_compatible ? 2 : 3];
    if (OB_FAIL(ObCreateRoutineResolver::resolve(parse_tree, sp_definer_node, sp_name_node,
                                                 body_node, NULL, param_node, clause_node,
                                                 crt_routine_arg))) {
      LOG_WARN("failed to resolve sp body", K(ret));
    }
  }
  return ret;
}


int ObCreateFunctionResolver::resolve_impl(
  const ParseNode &parse_tree, obrpc::ObCreateRoutineArg *crt_routine_arg)
{
  int ret = OB_SUCCESS;

  CK (OB_NOT_NULL(crt_routine_arg));
  CK (OB_NOT_NULL(session_info_));
  CK (OB_NOT_NULL(allocator_));
  CK (OB_NOT_NULL(schema_checker_));
  CK (OB_LIKELY(T_SF_CREATE == parse_tree.type_));
  CK (OB_NOT_NULL(parse_tree.children_));
  CK (OB_LIKELY((6 == parse_tree.num_child_ && !lib::is_oracle_mode())
                 || (1 == parse_tree.num_child_
                     && lib::is_oracle_mode()
                     && NULL != parse_tree.children_[0])));

  if (OB_SUCC(ret)
      && lib::is_oracle_mode()) {
    CK (T_SF_SOURCE == parse_tree.children_[0]->type_
      || T_SF_AGGREGATE_SOURCE == parse_tree.children_[0]->type_);
    CK (6 == parse_tree.children_[0]->num_child_);
  }

  if (OB_SUCC(ret)) {
    bool is_oracle_compatible = lib::is_oracle_mode();
    const ParseNode &source_tree = is_oracle_compatible ? *parse_tree.children_[0] : parse_tree;
    const ParseNode *sp_definer_node = is_oracle_compatible ? nullptr : source_tree.children_[0];
    ParseNode *sf_name_node = source_tree.children_[is_oracle_compatible ? 0 : 1];
    ParseNode *body_node = source_tree.children_[5];
    ParseNode *param_node = source_tree.children_[is_oracle_compatible ? 1 : 2];
    ParseNode *ret_node = source_tree.children_[is_oracle_compatible ? 2 : 3];
    ParseNode *sf_clause_list = source_tree.children_[is_oracle_compatible ? 3 : 4];
    ParseNode *pipelined_clase = is_oracle_compatible ? source_tree.children_[4] : NULL;
  
    if (OB_FAIL(ObCreateRoutineResolver::resolve(parse_tree,
                                                 sp_definer_node,
                                                 sf_name_node,
                                                 body_node,
                                                 ret_node,
                                                 param_node,
                                                 sf_clause_list,
                                                 crt_routine_arg))) {
      LOG_WARN("failed to resolve sp body", K(ret));
    }
  }
  return ret;
}

}
}


