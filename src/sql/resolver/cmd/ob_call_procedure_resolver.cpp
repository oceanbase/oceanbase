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

#include "ob_call_procedure_resolver.h"
#include "ob_call_procedure_stmt.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/resolver/dml/ob_select_resolver.h"
#include "pl/ob_pl_stmt.h"
#include "pl/ob_pl_package.h"
#include "observer/ob_req_time_service.h"
#include "pl/ob_pl_compile.h"
#include "pl/pl_cache/ob_pl_cache_mgr.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;
namespace sql
{
int ObCallProcedureResolver::check_param_expr_legal(ObRawExpr *param)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(param)) {
    if (T_REF_QUERY == param->get_expr_type()) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "subqueries or stored function calls here");
    } else if (T_FUN_SYS_PL_SEQ_NEXT_VALUE == param->get_expr_type()) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "ORA-06576 : not a valid function or procedure name");
    } /* else if (T_OP_GET_PACKAGE_VAR == param->get_expr_type()) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "PLS-221: not procedure or not defined!");
    } */
    for (int64_t i = 0; OB_SUCC(ret) && i < param->get_param_count(); ++i) {
      OZ (check_param_expr_legal(param->get_param_expr(i)));
    }
  }
  return ret;
}
int ObCallProcedureResolver::resolve_cparams(const ParseNode *params_node,
                                             const ObRoutineInfo *routine_info,
                                             ObCallProcedureInfo *call_proc_info,
                                             ObIArray<ObRawExpr*> &params)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(routine_info));
  CK (OB_NOT_NULL(call_proc_info));

  // Step 1: 初始化参数列表
  for (int64_t i = 0; OB_SUCC(ret) && i < routine_info->get_param_count(); ++i) {
    OZ (params.push_back(NULL));
  }
  // Step 2: 从ParamsNode中解析参数
  if (OB_SUCC(ret) && OB_NOT_NULL(params_node)) {
    bool has_assign_param = false;
    if (T_SP_CPARAM_LIST != params_node->type_) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid params list node", K(ret), K(params_node->type_));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < params_node->num_child_; ++i) {
      if (OB_ISNULL(params_node->children_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("param node is NULL", K(i), K(ret));
      } else if (T_SP_CPARAM == params_node->children_[i]->type_) {
        has_assign_param = true;
        if (OB_FAIL(resolve_cparam_with_assign(params_node->children_[i], routine_info, params))) {
          LOG_WARN("failed to resolve cparam with assign", K(ret));
        }
      } else if (has_assign_param) {
        ret = OB_ERR_SP_WRONG_ARG_NUM;
        LOG_WARN("can not set param without assign after param with assign", K(ret));
      } else if (OB_FAIL(resolve_cparam_without_assign(params_node->children_[i], i, params))) {
        LOG_WARN("failed to resolve cparam without assign", K(ret), K(i));
      }
    }
  }
  // Step 3: 处理空缺参数, 如果有默认值填充默认值, 否则报错
  for (int64_t i = 0; OB_SUCC(ret) && i < params.count(); ++i) {
    ObConstRawExpr *default_expr = NULL;
    if (OB_ISNULL(params.at(i))) { // 空缺参数
      params_.is_default_param_ = true;
      ObRoutineParam *routine_param = routine_info->get_routine_params().at(i);
      CK (OB_NOT_NULL(routine_param));
      if (OB_SUCC(ret) && routine_param->get_default_value().empty()) {
        ret = OB_ERR_SP_WRONG_ARG_NUM;
        LOG_WARN("routine param dese not has default value", K(ret));
      }
      CK (OB_NOT_NULL(params_.expr_factory_));
      OZ (ObRawExprUtils::build_const_int_expr(
        *(params_.expr_factory_), ObIntType, 0, default_expr));
      CK (OB_NOT_NULL(default_expr));
      OZ (default_expr->add_flag(IS_PL_MOCK_DEFAULT_EXPR));
      OX (params.at(i) = default_expr);
    }
  }

  if (OB_SUCC(ret)) { // 判断所有参数没有复杂表达式参数
    bool v = true;
    for (int64_t i = 0; v && OB_SUCC(ret) && i < params.count(); i ++) {
      if (OB_ISNULL(params.at(i))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(ret));
      } else if (params.at(i)->is_const_raw_expr()) {
        const ObConstRawExpr *const_expr = static_cast<const ObConstRawExpr *>(params.at(i));
        if (T_QUESTIONMARK != const_expr->get_expr_type()) {
          v = false;
        }
      } else {
        v = false;
      }
    } // for end
    call_proc_info->set_can_direct_use_param(v);
  }
  return ret;
}

int ObCallProcedureResolver::resolve_cparam_without_assign(const ParseNode *param_node,
                                                           const int64_t position,
                                                           ObIArray<ObRawExpr*> &params)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(param_node));
  ObRawExpr *param = NULL;
  if (OB_FAIL(ret)) {
  } else if (position < 0 || position >= params.count()) {
    ret = OB_ERR_SP_WRONG_ARG_NUM;
    LOG_WARN("wrong argument number", K(ret), K(position), K(params.count()));
  } else if (OB_NOT_NULL(params.at(position))) {
    ret = OB_ERR_SP_DUP_PARAM;
    LOG_WARN("dup params", K(ret), K(position));
  } else if (OB_FAIL(pl::ObPLResolver::resolve_raw_expr(*param_node, params_, param))) {
    LOG_WARN("failed to resolve const expr", K(ret));
  } else if (OB_ISNULL(param)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param expr is null", K(ret), K(param));
  } else if (OB_FAIL(check_param_expr_legal(param))) {
    LOG_WARN("failed to check param expr legal", K(ret), KPC(param));
  } else if (T_OP_ROW == param->get_expr_type() && 1 != param->get_param_count()) {
    ret = OB_ERR_INVALID_COLUMN_NUM;
    LOG_USER_ERROR(OB_ERR_INVALID_COLUMN_NUM, static_cast<int64_t>(1));
    LOG_WARN("op_row input param count is not 1", K(param->get_param_count()), K(ret));
  } else {
    params.at(position) = param;
  }
  return ret;
}

int ObCallProcedureResolver::resolve_cparam_with_assign(const ParseNode *param_node,
                                                        const ObRoutineInfo* routine_info,
                                                        ObIArray<ObRawExpr*> &params)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(param_node));
  CK (OB_NOT_NULL(routine_info));
  CK (OB_LIKELY(2 == param_node->num_child_));
  CK (OB_NOT_NULL(param_node->children_[0]));
  CK (OB_NOT_NULL(param_node->children_[1]));
  if (OB_SUCC(ret)) {
    const ParseNode *name_node = NULL;
    if (T_OBJ_ACCESS_REF == param_node->children_[0]->type_) {
      CK (OB_LIKELY(2 == param_node->children_[0]->num_child_));
      CK (OB_NOT_NULL(param_node->children_[0]->children_[0]));
      CK (OB_ISNULL(param_node->children_[0]->children_[1]));
      CK (OB_LIKELY(T_IDENT == param_node->children_[0]->children_[0]->type_));
      name_node = param_node->children_[0]->children_[0];
    } else if (T_IDENT == param_node->children_[0]->type_) {
      name_node = param_node->children_[0];
    } else if (T_COLUMN_REF == param_node->children_[0]->type_ &&
               3 == param_node->children_[0]->num_child_ &&
               NULL == param_node->children_[0]->children_[0] &&
               NULL ==  param_node->children_[0]->children_[1] &&
               T_IDENT == param_node->children_[0]->children_[2]->type_) {
      name_node = param_node->children_[0]->children_[2];
    } else {
      ret = OB_ERR_CALL_WRONG_ARG;
      LOG_WARN("PLS-00306: wrong number or types of arguments in call", K(ret));
    }

    if (OB_SUCC(ret)) {
      ObString name = ObString(static_cast<int32_t>(name_node->str_len_), name_node->str_value_);
      int64_t position = -1;
      if (OB_FAIL(routine_info->find_param_by_name(name, position))) {
        LOG_WARN("failed to find param name in proc info", K(ret), K(name), K(*routine_info));
      } else if (OB_UNLIKELY(-1 == position)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid postition value", K(ret), K(position));
      } else if (OB_FAIL(resolve_cparam_without_assign(param_node->children_[1], position, params))) {
        LOG_WARN("failed to resolve cparam without assign", K(ret));
      }
    }
  }
  return ret;
}

int ObCallProcedureResolver::resolve_param_exprs(const ParseNode *params_node,
                                                 ObIArray<ObRawExpr*> &expr_params)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(params_node));
  CK (T_SP_CPARAM_LIST == params_node->type_);
  CK (OB_NOT_NULL(params_.session_info_));
  for (int64_t i = 0; OB_SUCC(ret) && i < params_node->num_child_; ++i) {
    ObRawExpr* raw_expr = NULL;
    OZ (pl::ObPLResolver::resolve_raw_expr(*params_node->children_[i], params_, raw_expr));
    CK (OB_NOT_NULL(raw_expr));
    OZ (check_param_expr_legal(raw_expr));
    OZ (expr_params.push_back(raw_expr));
  }
  return ret;
}

int ObCallProcedureResolver::generate_pl_cache_ctx(pl::ObPLCacheCtx &pc_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(schema_checker_) || OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("argument is NULL", K(schema_checker_), K(session_info_), K(ret));
  } else if (OB_ISNULL(schema_checker_->get_schema_mgr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("argument is NULL", K(ret));
  } else {
    pc_ctx.session_info_ = session_info_;
    pc_ctx.schema_guard_ = schema_checker_->get_schema_mgr();
    pc_ctx.cache_params_ = const_cast<ParamStore *>(params_.param_list_);
    pc_ctx.raw_sql_ = params_.cur_sql_;
    pc_ctx.key_.namespace_ = ObLibCacheNameSpace::NS_CALLSTMT;
    pc_ctx.key_.db_id_ = session_info_->get_database_id();
    pc_ctx.key_.sessid_ = 0;
    pc_ctx.key_.key_id_ = OB_INVALID_ID;
    pc_ctx.key_.name_ = params_.cur_sql_;
  }
  return ret;
}

int ObCallProcedureResolver::add_call_proc_info(ObCallProcedureInfo *call_info)
{
  int ret = OB_SUCCESS;
  ObPlanCache *plan_cache = NULL;
  pl::ObPLCacheCtx pc_ctx;
  if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("argument is NULL", K(ret));
  } else if (OB_ISNULL(plan_cache = session_info_->get_plan_cache())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("argument is NULL", K(ret));
  } else if (OB_FAIL(generate_pl_cache_ctx(pc_ctx))) {
    LOG_WARN("generate pl cache ctx failed", K(ret));
  } else if (OB_FAIL(pl::ObPLCacheMgr::add_pl_cache(plan_cache, call_info, pc_ctx))) {
    if (OB_SQL_PC_PLAN_DUPLICATE == ret) {
      ret = OB_SUCCESS;
      LOG_DEBUG("this plan has been added by others, need not add again", KPC(call_info));
    } else if (OB_REACH_MEMORY_LIMIT == ret || OB_SQL_PC_PLAN_SIZE_LIMIT == ret) {
      if (REACH_TIME_INTERVAL(1000000)) { //1s, 当内存达到上限时, 该日志打印会比较频繁, 所以以1s为间隔打印
        LOG_DEBUG("can't add plan to plan cache",
                K(ret), K(call_info->get_mem_size()), K(pc_ctx.key_),
                K(plan_cache->get_mem_used()));
      }
      ret = OB_SUCCESS;
    } else if (is_not_supported_err(ret)) {
      ret = OB_SUCCESS;
      LOG_DEBUG("plan cache don't support add this kind of plan now",  KPC(call_info));
    } else {
      if (OB_REACH_MAX_CONCURRENT_NUM != ret) { //如果是达到限流上限, 则将错误码抛出去
        ret = OB_SUCCESS; //add plan出错, 覆盖错误码, 确保因plan cache失败不影响正常执行路径
        LOG_WARN("Failed to add plan to ObPlanCache", K(ret));
      }
    }
  }
  return ret;
}

int ObCallProcedureResolver::find_call_proc_info(ObCallProcedureStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObPlanCache *plan_cache = NULL;
  ObCallProcedureInfo *call_proc_info = NULL;
  pl::ObPLCacheCtx pc_ctx;
  if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("argument is NULL", K(ret));
  } else if (OB_ISNULL(plan_cache = session_info_->get_plan_cache())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("argument is NULL", K(ret));
  } else if (OB_FAIL(generate_pl_cache_ctx(pc_ctx))) {
    LOG_WARN("generate pl cache ctx failed", K(ret));
  } else if (OB_FAIL(pl::ObPLCacheMgr::get_pl_cache(plan_cache, stmt.get_cacheobj_guard(), pc_ctx))) {
      LOG_INFO("get pl function by sql failed, will ignore this error",
              K(ret), K(pc_ctx.key_));
      ret = OB_ERR_UNEXPECTED != ret ? OB_SUCCESS : ret;
  } else {
    call_proc_info = static_cast<ObCallProcedureInfo*>(stmt.get_cacheobj_guard().get_cache_obj());
    CK (OB_NOT_NULL(call_proc_info));
    OX (stmt.set_call_proc_info(call_proc_info));
  }
  return ret;
}

int ObCallProcedureResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObCallProcedureStmt *stmt = NULL;
  ParseNode *name_node = parse_tree.children_[0];
  ParseNode *dblink_node = lib::is_oracle_mode() ? parse_tree.children_[1] : NULL;
  ParseNode *params_node = lib::is_oracle_mode() ? parse_tree.children_[2] : parse_tree.children_[1];
  ObString db_name;
  ObString package_name;
  ObString sp_name;
  ObString dblink_name;
  ObCallProcedureInfo *call_proc_info = NULL;
  const ObRoutineInfo *proc_info = NULL;
  if (OB_ISNULL(schema_checker_) || OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("argument is NULL", K(schema_checker_), K(session_info_), K(ret));
  } else if (OB_UNLIKELY(T_SP_CALL_STMT != parse_tree.type_ || OB_ISNULL(name_node))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the children of parse tree is NULL", K(parse_tree.type_), K(name_node), K(ret));
  } else if (OB_ISNULL(stmt = create_stmt<ObCallProcedureStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("create call stmt failed", K(ret));
  } else if (FALSE_IT(stmt_ = stmt)) {
  } else if (FALSE_IT(stmt->get_cacheobj_guard().init(CALLSTMT_HANDLE))) {
  } else if (params_.is_execute_call_stmt_ && 0 != params_.cur_sql_.length() &&
             OB_FAIL(find_call_proc_info(*stmt))) {
    LOG_WARN("fail to find call stmt", K(ret));
  } else if (NULL != stmt->get_call_proc_info()) {
    // find call procedure info in pl cache.
  } else {
    OZ (ObCacheObjectFactory::alloc(stmt->get_cacheobj_guard(),
                                  ObLibCacheNameSpace::NS_CALLSTMT,
                                  session_info_->get_effective_tenant_id()));
    OX (call_proc_info = static_cast<ObCallProcedureInfo*>(stmt->get_cacheobj_guard().get_cache_obj()));
    CK (OB_NOT_NULL(call_proc_info));

    // 解析过程名称
    if (OB_SUCC(ret)) {
      if (T_SP_ACCESS_NAME != name_node->type_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Invalid procedure name node", K(name_node->type_), K(ret));
      } else if (OB_ISNULL(dblink_node)) {
        if (OB_FAIL(ObResolverUtils::resolve_sp_access_name(*schema_checker_,
                                                            session_info_->get_effective_tenant_id(),
                                                            session_info_->get_database_name(),
                                                            *name_node,
                                                            db_name, package_name, sp_name,
                                                            dblink_name))) {
          LOG_WARN("resolve sp name failed", K(ret));
        } else if (db_name.empty() && session_info_->get_database_name().empty()) {
          ret = OB_ERR_NO_DB_SELECTED;
          LOG_WARN("no database selected", K(ret), K(db_name));
        } else {
          if (!db_name.empty()) {
            OZ (call_proc_info->set_db_name(db_name));
          } else {
            OZ (call_proc_info->set_db_name(session_info_->get_database_name()));
          }

        }
      } else {
        CK (OB_NOT_NULL(parse_tree.children_[1]->children_[0]));
        OZ (resolve_dblink_routine_name(*parse_tree.children_[0],
                                        *parse_tree.children_[1]->children_[0],
                                        dblink_name,
                                        db_name,
                                        package_name,
                                        sp_name));
      }
    }
    ObSEArray<ObRawExpr*, 16> expr_params;
    pl::ObPLPackageGuard package_guard(params_.session_info_->get_effective_tenant_id());
    // 获取routine schem info
    if (OB_SUCC(ret)) {
      if (OB_NOT_NULL(params_node)
          && OB_FAIL(resolve_param_exprs(params_node, expr_params))) {
        LOG_WARN("failed to resolve param exprs", K(ret));
      } else if (OB_FAIL(ObResolverUtils::get_routine(package_guard,
                                                      params_,
                                                      (*session_info_).get_effective_tenant_id(),
                                                      (*session_info_).get_database_name(),
                                                      db_name,
                                                      package_name,
                                                      sp_name,
                                                      ROUTINE_PROCEDURE_TYPE,
                                                      expr_params,
                                                      proc_info,
                                                      dblink_name,
                                                      &(call_proc_info->get_allocator())))) {
        LOG_WARN("failed to get routine info", K(ret), K(db_name), K(package_name), K(sp_name));
      } else if (OB_ISNULL(proc_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("proc info is null", K(ret), K(db_name), K(package_name), K(sp_name), K(proc_info));
      } else if (proc_info->has_accessible_by_clause()) {
        ret = OB_ERR_MISMATCH_SUBPROGRAM;
        LOG_WARN("PLS-00263: mismatch between string on a subprogram specification and body",
                K(ret), KPC(proc_info));
      }
      if (OB_SUCC(ret) && proc_info->is_udt_routine() && !proc_info->is_udt_static_routine()) {
        ret = OB_ERR_CALL_WRONG_ARG;
        LOG_USER_ERROR(OB_ERR_CALL_WRONG_ARG, proc_info->get_routine_name().length(),
                                              proc_info->get_routine_name().ptr());
      }
      if (OB_SUCC(ret) && proc_info->is_udt_routine()) {
        call_proc_info->set_is_udt_routine(true);
      }
      if (OB_SUCC(ret)) {
        ObSchemaObjVersion obj_version;
        obj_version.object_id_ = proc_info->get_routine_id();
        obj_version.object_type_ = proc_info->is_procedure() ? DEPENDENCY_PROCEDURE : DEPENDENCY_FUNCTION;
        obj_version.version_ = proc_info->get_schema_version();
        int64_t tenant_id = session_info_->get_effective_tenant_id();
        int64_t tenant_schema_version = OB_INVALID_VERSION;
        int64_t sys_schema_version = OB_INVALID_VERSION;
        CK (OB_NOT_NULL(schema_checker_->get_schema_mgr()));
        OZ (schema_checker_->get_schema_mgr()->get_schema_version(tenant_id, tenant_schema_version));
        OZ (schema_checker_->get_schema_mgr()->get_schema_version(OB_SYS_TENANT_ID, sys_schema_version));
        OX (call_proc_info->set_tenant_schema_version(tenant_schema_version));
        OX (call_proc_info->set_sys_schema_version(sys_schema_version));
        OZ (call_proc_info->init_dependency_table_store(1));
        OZ (call_proc_info->get_dependency_table().push_back(obj_version));
      }
    }
    ObSEArray<ObRawExpr*, 16> params;
    OZ (resolve_cparams(params_node, proc_info, call_proc_info, params));

    if (OB_SUCC(ret)) {
      if (OB_INVALID_ID == proc_info->get_package_id()) {
        //standalone procedure
        call_proc_info->set_package_id(proc_info->get_package_id());
        call_proc_info->set_routine_id(proc_info->get_routine_id());
      } else {
        //package procedure
        call_proc_info->set_package_id(proc_info->get_package_id());
        call_proc_info->set_routine_id(proc_info->get_subprogram_id());
      }

      for (int64_t i = 0; OB_SUCC(ret) && i < proc_info->get_param_count(); ++i) {
        const ObRoutineParam *param_info = proc_info->get_routine_params().at(i);
        const ObRawExpr *param_expr = params.at(i);
        pl::ObPLDataType pl_type;
        CK (OB_NOT_NULL(param_info));
        CK (OB_NOT_NULL(param_expr));

        if (OB_SUCC(ret)) {
          CK (OB_NOT_NULL(schema_checker_->get_schema_mgr()));
          CK (OB_NOT_NULL(params_.sql_proxy_));
          CK (OB_NOT_NULL(session_info_));
          OZ (pl::ObPLDataType::transform_from_iparam(param_info,
                                                      *(schema_checker_->get_schema_mgr()),
                                                      *(session_info_),
                                                      *(params_.allocator_),
                                                      *(params_.sql_proxy_),
                                                      pl_type,
                                                      NULL,
                                                      &package_guard.dblink_guard_));
        }
        if (OB_SUCC(ret)) {
          if (param_info->is_out_sp_param() || param_info->is_inout_sp_param()) {
            const ObRawExpr* param = params.at(i);
            if (lib::is_mysql_mode()
                && param->get_expr_type() != T_OP_GET_USER_VAR
                && param->get_expr_type() != T_OP_GET_SYS_VAR) {
              ret = OB_ER_SP_NOT_VAR_ARG;
              LOG_USER_ERROR(OB_ER_SP_NOT_VAR_ARG, static_cast<int32_t>(i), static_cast<int32_t>(sp_name.length()), sp_name.ptr());
              LOG_WARN("OUT or INOUT argument for routine is not a variable", K(param->get_expr_type()), K(ret));
            } else if (lib::is_oracle_mode() &&
                        param->get_expr_type() != T_OP_GET_USER_VAR &&
                        param->get_expr_type() != T_QUESTIONMARK &&
                        param->get_expr_type() != T_OP_GET_PACKAGE_VAR &&
                        !param->is_obj_access_expr()) {
              ret = OB_ERR_CALL_WRONG_ARG;
              LOG_WARN("PLS-00306: wrong number or types of arguments in call stmt", K(ret));
            } else if (param->is_obj_access_expr() && !(static_cast<const ObObjAccessRawExpr *>(param))->for_write()) {
              ret = OB_ERR_CALL_WRONG_ARG;
              LOG_WARN("PLS-00306: wrong number or types of arguments in call stmt", K(ret));
            } else if (param_info->is_sys_refcursor_type()
                      || (param_info->is_pkg_type() && pl_type.is_cursor_type())) {
              OZ (call_proc_info->add_out_param(i,
                                      param_info->get_mode(),
                                      param_info->get_param_name(),
                                      pl_type,
                                      ObString("SYS_REFCURSOR"),
                                      ObString("")));
            } else if (param_info->is_complex_type()) { // UDT
              if (param_info->get_type_owner() == session_info_->get_database_id()) {
                CK (!session_info_->get_database_name().empty());
                OZ (call_proc_info->add_out_param(i,
                                        param_info->get_mode(),
                                        param_info->get_param_name(),
                                        pl_type,
                                        param_info->get_type_name(),
                                        session_info_->get_database_name()));
              } else {
                const ObDatabaseSchema *db_schema = NULL;
                CK (OB_NOT_NULL(schema_checker_));
                CK (OB_NOT_NULL(schema_checker_->get_schema_mgr()));
                OZ (schema_checker_->get_schema_mgr()->get_database_schema(param_info->get_tenant_id(),
                    param_info->get_type_owner(), db_schema), param_info->get_type_owner());
                if (OB_SUCC(ret) && OB_ISNULL(db_schema)) {
                  ret = OB_ERR_BAD_DATABASE;
                  LOG_WARN("failed to get type owner", K(param_info->get_type_owner()));
                }
                OZ (call_proc_info->add_out_param(i,
                                        param_info->get_mode(),
                                        param_info->get_param_name(),
                                        pl_type,
                                        param_info->get_type_name(),
                                        OB_SYS_TENANT_ID == db_schema->get_tenant_id()
                                          ? ObString("SYS") : db_schema->get_database_name_str()), i);
              }
            } else if (pl_type.is_user_type()) {
              // 通过Call语句执行PL且参数是复杂类型的情况, 仅在PS模式支持, 通过客户端无法构造复杂数据类型;
              // PS模式仅支持UDT作为出参, 这里将其他模式的复杂类型出参禁掉;
              ret = OB_NOT_SUPPORTED;
              LOG_WARN("not supported other type as out parameter except udt", K(ret), K(pl_type.is_user_type()));
              LOG_USER_ERROR(OB_NOT_SUPPORTED, "other complex type as out parameter except user define type");
            } else {
              OZ (call_proc_info->add_out_param(i,
                                      param_info->get_mode(),
                                      param_info->get_param_name(),
                                      pl_type,
                                      param_info->get_type_name(),
                                      ObString("")));
            }
          }
        }
      }
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(proc_info) && (OB_INVALID_ID != proc_info->get_dblink_id())) {
      stmt->set_dblink_routine_info(proc_info);
    }
    // Step 4: cg raw expr
    OX (call_proc_info->set_param_cnt(params.count()));
    OZ (call_proc_info->prepare_expression(params));
    OZ (call_proc_info->final_expression(params, session_info_, schema_checker_->get_schema_mgr()));
    OX (stmt->set_call_proc_info(call_proc_info));
    if (params_.is_execute_call_stmt_
        && 0 != params_.cur_sql_.length()
        && NULL == stmt->get_dblink_routine_info()) {
      if (NULL != params_.param_list_) {
        OZ (call_proc_info->set_params_info(*params_.param_list_));
      }
      OZ (add_call_proc_info(call_proc_info));
    }
    CK (1 == call_proc_info->get_dependency_table().count());
    OZ (stmt->add_global_dependency_table(call_proc_info->get_dependency_table().at(0)));
  }

  return ret;
}

int ObCallProcedureResolver::resolve_dblink_routine_name(const ParseNode &access_node,
                                                         const ParseNode &dblink_node,
                                                         ObString &dblink_name,
                                                         ObString &db_name,
                                                         ObString &pkg_name,
                                                         ObString &sp_name)
{
  int ret = OB_SUCCESS;
  const ParseNode *db_node = access_node.children_[0];
  const ParseNode *pkg_node = access_node.children_[1];
  const ParseNode *sp_node = access_node.children_[2];
  CK (OB_LIKELY(T_SP_ACCESS_NAME == access_node.type_));
  CK (OB_LIKELY(T_USER_VARIABLE_IDENTIFIER == dblink_node.type_));
  if (OB_SUCC(ret)) {
    OX (dblink_name.assign_ptr(dblink_node.str_value_, static_cast<int32_t>(dblink_node.str_len_)));
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(db_node)) {
    if (OB_LIKELY(T_IDENT == db_node->type_)) {
      db_name.assign_ptr(db_node->str_value_, static_cast<int32_t>(db_node->str_len_));
    }
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(pkg_node)) {
    if (OB_LIKELY(T_IDENT == pkg_node->type_)) {
      pkg_name.assign_ptr(pkg_node->str_value_, static_cast<int32_t>(pkg_node->str_len_));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(sp_node) && OB_UNLIKELY(T_IDENT != sp_node->type_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sp_node is invalid", K(ret), K(sp_node));
    } else {
      sp_name.assign_ptr(sp_node->str_value_, static_cast<int32_t>(sp_node->str_len_));
    }
  }
  return ret;
}

}
}
