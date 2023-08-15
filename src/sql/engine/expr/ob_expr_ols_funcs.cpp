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

#define USING_LOG_PREFIX  SQL_ENG

#include "ob_expr_ols_funcs.h"

#include "sql/engine/expr/ob_expr_res_type.h"
#include "sql/executor/ob_task_executor_ctx.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/cmd/ob_table_executor.h"
#include "sql/parser/ob_parser.h"
#include "sql/resolver/ddl/ob_alter_table_resolver.h"
#include "sql/plan_cache/ob_sql_parameterization.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_schema_struct.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/ob_common_rpc_proxy.h"
#include "share/ob_label_security.h"
#include "observer/ob_server_struct.h"
#include "sql/ob_spi.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

namespace oceanbase
{
namespace sql
{

constexpr ObValueChecker<int64_t>                 FLAG_CHECKER                  (0, 1, OB_ERR_ARGUMENT_OUT_OF_RANGE);
constexpr ObValueChecker<int64_t>                 COMP_NUM_CHECKER              (0, 9999, OB_ERR_ARGUMENT_OUT_OF_RANGE);
constexpr ObValueChecker<int64_t>                 LABEL_TAG_CHECKER             (0, 99999999, OB_ERR_ARGUMENT_OUT_OF_RANGE);

constexpr ObValueChecker<ObString::obstr_size_t>  POLICY_NAME_CHECKER           (1, OB_MAX_ORACLE_VARCHAR_LENGTH, OB_INVALID_ARGUMENT);
constexpr ObValueChecker<ObString::obstr_size_t>  COLUMN_NAME_CHECKER           (1, OB_MAX_COLUMN_NAME_LENGTH, OB_INVALID_ARGUMENT);
constexpr ObValueChecker<ObString::obstr_size_t>  COMP_SHORT_NAME_CHECKER       (1, MAX_ORACLE_SA_COMPONENTS_SHORT_NAME_LENGTH, OB_INVALID_ARGUMENT);
constexpr ObValueChecker<ObString::obstr_size_t>  COMP_LONG_NAME_CHECKER        (1, MAX_ORACLE_SA_COMPONENTS_LONG_NAME_LENGTH, OB_INVALID_ARGUMENT);
constexpr ObValueChecker<ObString::obstr_size_t>  LABEL_TEXT_CHECKER            (1, OB_MAX_ORACLE_VARCHAR_LENGTH, OB_INVALID_ARGUMENT);
constexpr ObValueChecker<ObString::obstr_size_t>  ENFORCEMENT_OPTIONS_CHECKER   (0, OB_MAX_COLUMN_NAME_LENGTH, OB_INVALID_ARGUMENT);
constexpr ObValueChecker<ObString::obstr_size_t>  TABLE_NAME_CHECKER            (1, OB_MAX_TABLE_NAME_LENGTH, OB_WRONG_TABLE_NAME);
constexpr ObValueChecker<ObString::obstr_size_t>  LABEL_FUNCTION_CHECKER        (0, OB_MAX_ORACLE_VARCHAR_LENGTH, OB_WRONG_TABLE_NAME);
constexpr ObValueChecker<ObString::obstr_size_t>  PREDICATE_CHECKER             (0, OB_MAX_ORACLE_VARCHAR_LENGTH, OB_WRONG_TABLE_NAME);
constexpr ObValueChecker<ObString::obstr_size_t>  USER_NAME_CHECKER             (0, OB_MAX_USER_NAME_LENGTH, OB_WRONG_USER_NAME_LENGTH);

constexpr ObPointerChecker<ObLabelSePolicySchema>      POLICY_SCHEMA_CHECKER         (OB_ERR_POLICY_STRING_NOT_FOUND);
constexpr ObPointerChecker<ObLabelSeComponentSchema>   COMPONENT_SCHEMA_CHECKER      (OB_ERR_UNDEFINED_LEVEL_STRING_FOR_POLICY_STRING);
constexpr ObPointerChecker<ObLabelSeLabelSchema>       LABEL_SCHEMA_CHECKER          (OB_ERR_INVALID_LABEL_STRING);
//constexpr ObPointerChecker<ObLabelSeUserLevelSchema>   USER_LEVEL_SCHEMA_CHECKER     (OB_OBJECT_NAME_NOT_EXIST);

static const int64_t INVALID_OLS_VALUE = -1;

int ObExprOLSBase::check_func_access_role(ObSQLSessionInfo &session) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!session.is_lbacsys_user())) {
    ret = OB_ERR_NO_PRIVILEGE;
    LOG_WARN("no privilege using label security", K(ret), K(session.get_user_name()));
  }
  return ret;
}

bool ObExprOLSBase::need_retry_ddl(ObExecContext &ctx, int &ret) const
{
  bool need_retry = false;
  if (THIS_WORKER.is_timeout()) {
    if (OB_SUCC(ret)) {
      ret = OB_TIMEOUT;
      LOG_WARN("timeout at retry ddl", K(ret));
    }
  } else if (OB_EAGAIN == ret
             || OB_SNAPSHOT_DISCARDED == ret
             || OB_ERR_PARALLEL_DDL_CONFLICT == ret) {
    ret = OB_SUCCESS;
    if (OB_FAIL(ObSPIService::force_refresh_schema(
                  ctx.get_my_session()->get_effective_tenant_id()))) {
      LOG_WARN("fail to get schema guard", K(ret));
    } else {
      need_retry = true;
    }
  }
  return need_retry;
}

int ObExprOLSBase::init_phy_plan_timeout(ObExecContext &exec_ctx, ObSQLSessionInfo &session)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = NULL;
  int64_t plan_timeout = 0;

  if (OB_ISNULL(plan_ctx = exec_ctx.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr phy plan ctx NULL", K(ret));
  } else if (OB_FAIL(session.get_query_timeout(plan_timeout))) {
    LOG_WARN("fail to get query timeout", K(ret));
  } else {
    int64_t start_time = session.get_query_start_time();
    plan_ctx->set_timeout_timestamp(start_time + plan_timeout);
    //THIS_WORKER.set_timeout_ts(plan_ctx->get_timeout_timestamp());
  }
  return ret;
}

int ObExprOLSBase::send_policy_ddl_rpc(ObExprCtx &expr_ctx, const obrpc::ObLabelSePolicyDDLArg &ddl_arg) const
{
  int ret = OB_SUCCESS;

  ObExecContext *exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;

  if (OB_ISNULL(exec_ctx = expr_ctx.exec_ctx_)
      || OB_ISNULL(GCTX.schema_service_)
      || OB_ISNULL(exec_ctx->get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get executor context failed", K(ret), K(exec_ctx), K(GCTX.schema_service_));
  } else if (OB_ISNULL(common_rpc_proxy = exec_ctx->get_task_exec_ctx().get_common_rpc())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get common rpc proxy failed", K(ret));
  } else if (OB_FAIL(common_rpc_proxy->handle_label_se_policy_ddl(ddl_arg))) {
    LOG_WARN("rpc proxy failed", K(ddl_arg),  K(ret));
  } else if (OB_FAIL(ObSQLUtils::update_session_last_schema_version(*GCTX.schema_service_,
                                                                    *exec_ctx->get_my_session()))) {
    LOG_WARN("fail to update session schema version", K(ret));
  }

  LOG_DEBUG("send rpc for policy ddl", K(ddl_arg));

  return ret;
}

int ObExprOLSBase::send_component_ddl_rpc(ObExprCtx &expr_ctx, const obrpc::ObLabelSeComponentDDLArg &ddl_arg) const
{
  int ret = OB_SUCCESS;

  ObExecContext *exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;

  if (OB_ISNULL(exec_ctx = expr_ctx.exec_ctx_)
      || OB_ISNULL(GCTX.schema_service_)
      || OB_ISNULL(exec_ctx->get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get executor context failed", K(ret), K(exec_ctx), K(GCTX.schema_service_));
  } else if (OB_ISNULL(common_rpc_proxy = exec_ctx->get_task_exec_ctx().get_common_rpc())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get common rpc proxy failed", K(ret));
  } else if (OB_FAIL(common_rpc_proxy->handle_label_se_component_ddl(ddl_arg))) {
    LOG_WARN("rpc proxy failed", K(ddl_arg),  K(ret));
  } else if (OB_FAIL(ObSQLUtils::update_session_last_schema_version(*GCTX.schema_service_,
                                                                    *exec_ctx->get_my_session()))) {
    LOG_WARN("fail to update session schema version", K(ret));
  }

  LOG_DEBUG("send rpc for component ddl", K(ddl_arg));

  return ret;
}

int ObExprOLSBase::send_label_ddl_rpc(ObExprCtx &expr_ctx, const obrpc::ObLabelSeLabelDDLArg &ddl_arg) const
{
  int ret = OB_SUCCESS;

  ObExecContext *exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;

  if (OB_ISNULL(exec_ctx = expr_ctx.exec_ctx_)
      || OB_ISNULL(GCTX.schema_service_)
      || OB_ISNULL(exec_ctx->get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get executor context failed", K(ret), K(exec_ctx), K(GCTX.schema_service_));
  } else if (OB_ISNULL(common_rpc_proxy = exec_ctx->get_task_exec_ctx().get_common_rpc())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get common rpc proxy failed", K(ret));
  } else if (OB_FAIL(common_rpc_proxy->handle_label_se_label_ddl(ddl_arg))) {
    LOG_WARN("rpc proxy failed", K(ddl_arg),  K(ret));
  } else if (OB_FAIL(ObSQLUtils::update_session_last_schema_version(*GCTX.schema_service_,
                                                                    *exec_ctx->get_my_session()))) {
    LOG_WARN("fail to update session schema version", K(ret));
  }

  LOG_DEBUG("send rpc for label ddl", K(ddl_arg));

  return ret;
}

int ObExprOLSBase::send_user_level_ddl_rpc(ObExprCtx &expr_ctx, const obrpc::ObLabelSeUserLevelDDLArg &ddl_arg) const
{
  int ret = OB_SUCCESS;

  ObExecContext *exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;

  if (OB_ISNULL(exec_ctx = expr_ctx.exec_ctx_)
      || OB_ISNULL(GCTX.schema_service_)
      || OB_ISNULL(exec_ctx->get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get executor context failed", K(ret), K(exec_ctx), K(GCTX.schema_service_));
  } else if (OB_ISNULL(common_rpc_proxy = exec_ctx->get_task_exec_ctx().get_common_rpc())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get common rpc proxy failed", K(ret));
  } else if (OB_FAIL(common_rpc_proxy->handle_label_se_user_level_ddl(ddl_arg))) {
    LOG_WARN("rpc proxy failed", K(ddl_arg),  K(ret));
  } else if (OB_FAIL(ObSQLUtils::update_session_last_schema_version(*GCTX.schema_service_,
                                                                    *exec_ctx->get_my_session()))) {
    LOG_WARN("fail to update session schema version", K(ret));
  }

  LOG_DEBUG("send rpc for user label ddl", K(ddl_arg));

  return ret;
}

int ObExprOLSBase::append_str_to_sqlstring(ObSqlString &target, const ObString &param)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(target.append_fmt("%s'%.*s'",
                                (target.empty() ? "" : ", "),
                                param.length(),
                                param.ptr()))) {
    LOG_WARN("sql append value failed", K(ret), K(param));
  }
  return ret;
}

int ObExprOLSBase::append_int_to_sqlstring(ObSqlString &target, const int64_t param) const
{
  int ret = OB_SUCCESS;

  if (OB_SUCC(ret)) {
    if (OB_FAIL(target.append_fmt("%s%ld",
                                  (target.empty() ? "" : ", "),
                                  param))) {
      LOG_WARN("sql append value failed", K(ret), K(param));
    }
  }
  return ret;
}

void ObExprOLSBase::set_ols_func_common_result_type(ObExprResType &type) const
{
  type.set_int();
  type.set_precision(DEFAULT_PRECISION_FOR_BOOL);
  type.set_scale(DEFAULT_SCALE_FOR_INTEGER);
}

void ObExprOLSBase::set_ols_func_common_result(ObObj &result) const
{
  result.set_int(0);
}

int ObExprOLSBase::gen_stmt_string(ObSqlString &ddl_stmt_str, const ObString &function_name, const ObString &args) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ddl_stmt_str.append_fmt("select %.*s(%.*s) from dual",
                                      function_name.length(), function_name.ptr(),
                                      args.length(), args.ptr()))) {
    LOG_WARN("fail to append sql string", K(ret));
  }
  return ret;
}

int ObExprOLSBase::get_interger_from_obj_and_check(const ObObj &param,
                                                   int64_t &comp_num,
                                                   const ObValueChecker<int64_t> &checker,
                                                   ObSqlString &param_str,
                                                   bool accept_null) const
{
  int ret = OB_SUCCESS;

  if (param.is_null_oracle() && accept_null) {
    //do nothing
  } else if (OB_FAIL(param.get_int(comp_num))) {
    LOG_WARN("fail to get value from obj", K(ret));
  } else if (OB_FAIL(checker.validate(comp_num))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("number validation failed", K(ret), K(comp_num), K(checker));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(append_int_to_sqlstring(param_str, comp_num))) {
      LOG_WARN("fail to append to param_str", K(ret), K(param_str.length()));
    }
  }
  return ret;
}

int ObExprOLSBase::get_string_from_obj_and_check(ObIAllocator *allocator,
                                                 const ObObj &param,
                                                 ObString &name,
                                                 const ObValueChecker<ObString::obstr_size_t> &checker,
                                                 ObSqlString &param_str,
                                                 bool accept_null)
{
  int ret = OB_SUCCESS;
  ObString input_str;

  if (OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (param.is_null_oracle() && accept_null) {
    //input_str.reset
  } else if (OB_FAIL(param.get_varchar(input_str))) {
    LOG_WARN("fail to get value from obj", K(ret));
  } else if (OB_FAIL(ob_simple_low_to_up(*allocator, input_str.trim(), name))) {
    LOG_WARN("change to upper string failed", K(ret));
  } else if (OB_FAIL(checker.validate_str_length(name))) {
    LOG_WARN("string length validation failed", K(ret), K(name), K(checker));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(append_str_to_sqlstring(param_str, name))) {
      LOG_WARN("fail to append to param_str", K(ret), K(param_str.length()));
    }
  }

  return ret;
}

int ObExprOLSUtil::adjust_column_flag(ObAlterTableStmt &alter_table_stmt, bool is_add)
{
  int ret = OB_SUCCESS;
  AlterTableSchema &alter_table_schema = alter_table_stmt.get_alter_table_arg().alter_table_schema_;
  ObColumnSchemaV2 *alter_column_schema = NULL;
  if (OB_UNLIKELY(alter_table_schema.get_column_count() != 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("alter more than one column schema", K(ret));
  } else if (OB_ISNULL(alter_column_schema = alter_table_schema.get_column_schema_by_idx(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("alter column schema is NULL", K(ret));
  } else {
    if (is_add) {
      alter_column_schema->add_column_flag(LABEL_SE_COLUMN_FLAG);
    } else {
      alter_column_schema->del_column_flag(LABEL_SE_COLUMN_FLAG);
    }
  }
  return ret;
}

int ObExprOLSUtil::restore_invisible_column_flag(
    ObSchemaGetterGuard &schema_guard,
    const uint64_t tenant_id,
    ObAlterTableStmt &alter_table_stmt)
{
  int ret = OB_SUCCESS;
  AlterTableSchema &alter_table_schema = alter_table_stmt.get_alter_table_arg().alter_table_schema_;
  ObColumnSchemaV2 *alter_column_schema = NULL;
  const ObColumnSchemaV2 *old_column_schema = NULL;
  if (OB_UNLIKELY(alter_table_schema.get_column_count() != 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("alter more than one column schema", K(ret));
  } else if (OB_ISNULL(alter_column_schema = alter_table_schema.get_column_schema_by_idx(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("alter column schema is NULL", K(ret));
  } else if (OB_ISNULL(old_column_schema = schema_guard.get_column_schema(
                       tenant_id,
                       alter_column_schema->get_table_id(),
                       alter_column_schema->get_column_id()))) {
    ret = OB_ERR_COLUMN_NOT_FOUND;
    LOG_WARN("column schema is NULL", K(ret), K(tenant_id));
  } else if (alter_column_schema->get_schema_version() != old_column_schema->get_schema_version()) {
    ret = OB_NEED_RETRY;
    LOG_WARN("schema version change", K(ret));
  } else {
    if (old_column_schema->has_column_flag(INVISIBLE_COLUMN_FLAG)) {
      alter_column_schema->add_column_flag(INVISIBLE_COLUMN_FLAG);
    } else {
      alter_column_schema->del_column_flag(INVISIBLE_COLUMN_FLAG);
    }
  }
  return ret;
}

int ObExprOLSUtil::generate_alter_table_args(ObExprCtx &expr_ctx,
                                             ObSchemaGetterGuard &schema_guard,
                                             ObSQLSessionInfo &session,
                                             const ObString &ddl_stmt_str,
                                             ObAlterTableStmt *&alter_table_stmt) {
  int ret = OB_SUCCESS;
  ObSqlCtx *context = NULL;

  if (OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("calc buf is NULL", K(ret));
  } else {
    ObParser parser(*expr_ctx.calc_buf_, session.get_sql_mode());

    ObResolverParams resolver_ctx;
    ObSchemaChecker schema_checker;


    if (OB_FAIL(schema_checker.init(schema_guard))) {
      LOG_WARN("fail to init schema checker", K(ret));
    } else if (OB_ISNULL(expr_ctx.exec_ctx_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("exec context is null", K(ret));
    } else if (OB_ISNULL(context = expr_ctx.exec_ctx_->get_sql_ctx())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("context is null", K(ret));
    } else {
      resolver_ctx.allocator_  = expr_ctx.calc_buf_;
      resolver_ctx.schema_checker_ = &schema_checker;
      //resolver_ctx.secondary_namespace_ = NULL;
      resolver_ctx.session_info_ = &session;
      resolver_ctx.expr_factory_ = expr_ctx.exec_ctx_->get_expr_factory();
      resolver_ctx.stmt_factory_ = expr_ctx.exec_ctx_->get_stmt_factory();
      //resolver_ctx.cur_sql_ = add_column_ddl_sql.string();
      //resolver_ctx.is_restore_ = context->is_restore_;
      //resolver_ctx.is_ddl_from_primary_ = context->is_ddl_from_primary_;
      //resolver_ctx.select_item_param_infos_ = NULL;
      if (OB_ISNULL(expr_ctx.exec_ctx_->get_stmt_factory())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("stmt factory is NULL", K(ret));
      } else if (FALSE_IT(resolver_ctx.query_ctx_ =
                          expr_ctx.exec_ctx_->get_stmt_factory()->get_query_ctx())) {
      } else if (OB_ISNULL(expr_ctx.exec_ctx_->get_physical_plan_ctx())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("phy plan ctx is NULL", K(ret));
      } else {
        resolver_ctx.param_list_ = &expr_ctx.exec_ctx_->get_physical_plan_ctx()->get_param_store();
      }
      //resolver_ctx.is_prepare_protocol_ = context->is_prepare_protocol_;
      //resolver_ctx.is_prepare_stage_ = context->is_prepare_stage_;
      //resolver_ctx.statement_id_ = context->statement_id_;
      //resolver_ctx.sql_proxy_ = context->sql_proxy_;
    }
    OZ (schema_checker.set_lbca_op());
    if (OB_SUCC(ret)) {
      ParseResult parse_result;
      HEAP_VAR(ObAlterTableResolver, resolver, resolver_ctx) {
        //SqlInfo not_param_info;
        //bool is_transform_outline = false;
        //ObMaxConcurrentParam::FixParamStore fixed_param_store;
        ParseNode *tree = NULL;

        if (OB_FAIL(parser.parse(ddl_stmt_str, parse_result))) {
          LOG_WARN("fail to parse stmt", K(ret));
        } else if (OB_ISNULL(tree = parse_result.result_tree_->children_[0])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("result tree is null", K(ret));
        } else if (OB_FAIL(resolver.resolve(*tree))) {
          LOG_WARN("fail to resove parse tree", K(ret));
        } else {
         alter_table_stmt = resolver.get_alter_table_stmt();
         if (OB_NOT_NULL(alter_table_stmt)) {
           obrpc::ObDDLArg &ddl_arg = alter_table_stmt->get_ddl_arg();
           ddl_arg.exec_tenant_id_ = session.get_effective_tenant_id();
         }
        }
      }
    }
  }
  return ret;
}


int ObExprOLSUtil::exec_switch_policy_column(const ObString &schema_name,
                                             const ObString &table_name,
                                             const ObString &column_name,
                                             ObExprCtx &expr_ctx,
                                             ObExecContext &exec_ctx,
                                             ObSQLSessionInfo &session,
                                             ObSchemaGetterGuard &schema_guard,
                                             bool is_switch_on)
{
  int ret = OB_SUCCESS;
  ObSqlString modify_column_ddl_sql;

  if (OB_SUCC(ret)) {
    if (OB_FAIL(modify_column_ddl_sql.assign_fmt("alter table \"%.*s\".\"%.*s\" modify \"%.*s\" invisible",
                                                 schema_name.length(), schema_name.ptr(),
                                                 table_name.length(), table_name.ptr(),
                                                 column_name.length(), column_name.ptr()))) {
      LOG_WARN("fail to assign sql string", K(ret));
    }
  }

  ObAlterTableStmt *alter_table_stmt = NULL;

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObExprOLSUtil::generate_alter_table_args(expr_ctx,
                                          schema_guard,
                                          session,
                                          modify_column_ddl_sql.string(),
                                          alter_table_stmt))) {
      LOG_WARN("fail to generate alter table args", K(ret), K(modify_column_ddl_sql));
    } else if (OB_ISNULL(alter_table_stmt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("stmt is NULL", K(ret));
    } else if (OB_FAIL(ObExprOLSUtil::adjust_column_flag(*alter_table_stmt, is_switch_on))) {
      LOG_WARN("fail to adjust column flag", K(ret));
    } else if (OB_FAIL(ObExprOLSUtil::restore_invisible_column_flag(
               schema_guard, session.get_effective_tenant_id(), *alter_table_stmt))) {
      LOG_WARN("fail to restore invisible column flag", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    ObAlterTableExecutor alter_table_executor;
    if (OB_FAIL(alter_table_executor.execute(exec_ctx, *alter_table_stmt))) {
      LOG_WARN("fail to senc rpc", K(ret));
    }
  }
  return ret;
}

int ObExprOLSUtil::exec_drop_policy_column(const ObString &schema_name,
                                           const ObString &table_name,
                                           const ObString &column_name,
                                           ObExecContext &exec_ctx,
                                           ObSQLSessionInfo &session)
{
  int ret = OB_SUCCESS;
  ObSqlString drop_column_ddl_sql;

  if (OB_SUCC(ret)) {
    if (OB_FAIL(drop_column_ddl_sql.assign_fmt("alter table \"%.*s\".\"%.*s\" drop column \"%.*s\"",
                                               schema_name.length(), schema_name.ptr(),
                                               table_name.length(), table_name.ptr(),
                                               column_name.length(), column_name.ptr()))) {
      LOG_WARN("fail to assign sql string", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    int64_t affected_rows = 0;
    ObMySQLProxy *sql_proxy = NULL;
    if (OB_ISNULL(sql_proxy = exec_ctx.get_sql_proxy())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sql proxy is NULL", K(ret));
    } else if (OB_FAIL(exec_ctx.get_sql_proxy()->write(session.get_effective_tenant_id(),
                                                       drop_column_ddl_sql.ptr(),
                                                       affected_rows,
                                                       session.get_compatibility_mode()))) {
      LOG_WARN("exec inner sql failed", K(ret), K(drop_column_ddl_sql));
    }
  }
  return ret;
}

int ObExprOLSUtil::exec_add_policy_column(const ObString &ddl_stmt_str,
                                          const ObString &schema_name,
                                          const ObString &table_name,
                                          const ObString &column_name,
                                          ObExprCtx &expr_ctx,
                                          ObExecContext &exec_ctx,
                                          ObSQLSessionInfo &session,
                                          ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  ObSqlString add_column_ddl_sql;

  if (OB_SUCC(ret)) {
    if (OB_FAIL(add_column_ddl_sql.assign_fmt("alter table \"%.*s\".\"%.*s\" add \"%.*s\" number(10) default NULL",
                                              schema_name.length(), schema_name.ptr(),
                                              table_name.length(), table_name.ptr(),
                                              column_name.length(), column_name.ptr()))) {
      LOG_WARN("fail to assign sql string", K(ret));
    }
  }

  ObAlterTableStmt *alter_table_stmt = NULL;

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObExprOLSUtil::generate_alter_table_args(expr_ctx,
                                          schema_guard,
                                          session,
                                          add_column_ddl_sql.string(),
                                          alter_table_stmt))) {
      LOG_WARN("fail to generate alter table args", K(ret), K(add_column_ddl_sql));
    } else if (OB_ISNULL(alter_table_stmt) ||
               OB_ISNULL(exec_ctx.get_stmt_factory()) ||
               OB_ISNULL(exec_ctx.get_stmt_factory()->get_query_ctx())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null pointer", K(ret));
    } else if (OB_FAIL(ObExprOLSUtil::adjust_column_flag(*alter_table_stmt, true))) {
      LOG_WARN("fail to adjust column flag", K(ret));
    } else {
      exec_ctx.get_stmt_factory()->get_query_ctx()->set_sql_stmt_coll_type(session.get_local_collation_connection());
      exec_ctx.get_stmt_factory()->get_query_ctx()->set_sql_stmt(ddl_stmt_str);
    }
  }

  if (OB_SUCC(ret)) {
    ObAlterTableExecutor alter_table_executor;
    if (OB_FAIL(alter_table_executor.execute(exec_ctx, *alter_table_stmt))) {
      LOG_WARN("fail to senc rpc");
    }
  }
  return ret;
}

int ObExprOLSUtil::label_tag_compare(int64_t tenant_id,
                                     ObSchemaGetterGuard &schema_guard,
                                     const int64_t label_tag1,
                                     const int64_t label_tag2,
                                     int64_t &cmp_result)
{
  int ret = OB_SUCCESS;

  int64_t label_tag[2] = {label_tag1, label_tag2};
  uint64_t policy_id[2];

  //将label tag转换为label字符串
  ObString label_text[2];

  for (int64_t i = 0; OB_SUCC(ret) && i < 2; ++i) {
    const ObLabelSeLabelSchema *ols_label_schema = NULL;

    if (OB_FAIL(schema_guard.get_label_se_label_by_label_tag(tenant_id,
                                                             label_tag[i],
                                                             ols_label_schema))) {
      LOG_WARN("get label schema failed", K(ret));
    } else if (OB_FAIL(LABEL_SCHEMA_CHECKER.validate(ols_label_schema))) {
      LOG_WARN("label schema not exist", K(ret));
    } else {
      label_text[i] = ols_label_schema->get_label();
      policy_id[i] = ols_label_schema->get_label_se_policy_id();
    }
  }

  //额外检查，防止意外错误
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(policy_id[0] != policy_id[1])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("policy id not the same", K(policy_id[0]), K(policy_id[1]));
    }
  }

  //把字符串形式的label拆分成component字符串，再把component字符串转换成component值
  ObLabelSeDecomposedLabel label_comps[2];
  ObLabelSeLabelCompNums label_nums[2];

  for (int64_t i = 0; OB_SUCC(ret) && i < 2; ++i) {
    if (OB_FAIL(ObLabelSeResolver::resolve_label_text(label_text[i], label_comps[i]))) {
      LOG_WARN("fail to resolve label", K(ret));
    } else if (OB_FAIL(ObLabelSeUtil::convert_label_comps_name_to_num(tenant_id,
                                                                      policy_id[i],
                                                                      schema_guard,
                                                                      label_comps[i],
                                                                      label_nums[i]))) {
      LOG_WARN("fail to validate component", K(ret), K(label_comps));
    }
  }

  cmp_result = (label_nums[0] <= label_nums[1]) ? 0 : -1;
  return ret;
}

int ObExprOLSBase::get_schema_guard(ObExecContext *exec_ctx, ObSchemaGetterGuard *&schema_guard)
{
  int ret = OB_SUCCESS;
  schema_guard = NULL;
  if (OB_ISNULL(exec_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("task executor context is NULL", K(ret), KP(exec_ctx));
  } else if (OB_ISNULL(schema_guard = exec_ctx->get_virtual_table_ctx().schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema guard is NULL", K(ret));
  }
  return ret;
}

ObExprOLSPolicyCreate::ObExprOLSPolicyCreate(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_LABEL_SE_POLICY_CREATE, N_OLS_POLICY_CREATE, 3, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION,
                         false, INTERNAL_IN_ORACLE_MODE)
{
}

ObExprOLSPolicyCreate::~ObExprOLSPolicyCreate()
{
}


int ObExprOLSPolicyCreate::calc_result_type3(ObExprResType &type,
                                             ObExprResType &type1,
                                             ObExprResType &type2,
                                             ObExprResType &type3,
                                             ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);

  type1.set_calc_type(ObVarcharType);
  type2.set_calc_type(ObVarcharType);
  type3.set_calc_type(ObVarcharType);

  set_ols_func_common_result_type(type);
  return ret;
}

ObExprOLSPolicyAlter::ObExprOLSPolicyAlter(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_LABEL_SE_POLICY_ALTER, N_OLS_POLICY_ALTER, 2, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION,
                         false, INTERNAL_IN_ORACLE_MODE)
{
}

ObExprOLSPolicyAlter::~ObExprOLSPolicyAlter()
{
}

int ObExprOLSPolicyAlter::calc_result_type2(ObExprResType &type,
                                            ObExprResType &type1,
                                            ObExprResType &type2,
                                            ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);

  type1.set_calc_type(ObVarcharType);
  type2.set_calc_type(ObVarcharType);

  set_ols_func_common_result_type(type);
  return ret;
}

ObExprOLSPolicyDisable::ObExprOLSPolicyDisable(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_LABEL_SE_POLICY_DISABLE, N_OLS_POLICY_DISABLE, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION,
                         false, INTERNAL_IN_ORACLE_MODE)
{
}

ObExprOLSPolicyDisable::~ObExprOLSPolicyDisable()
{
}


/**
 * @brief DISABLE_POLICY function
 * @param type        exec_ret
 * @param type1       policy_name   varchar
 * @param type_ctx
 * @return
 */
int ObExprOLSPolicyDisable::calc_result_type1(ObExprResType &type,
                                             ObExprResType &type1,
                                             ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);

  type1.set_calc_type(ObVarcharType);

  set_ols_func_common_result_type(type);
  return ret;
}

ObExprOLSPolicyEnable::ObExprOLSPolicyEnable(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_LABEL_SE_POLICY_ENABLE, N_OLS_POLICY_ENABLE, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION,
                         false, INTERNAL_IN_ORACLE_MODE)
{
}

ObExprOLSPolicyEnable::~ObExprOLSPolicyEnable()
{
}


/**
 * @brief ENABLE_POLICY function
 * @param type        exec_ret
 * @param type1       policy_name       varchar
 * @param type_ctx
 * @return
 */
int ObExprOLSPolicyEnable::calc_result_type1(ObExprResType &type,
                                             ObExprResType &type1,
                                             ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);

  type1.set_calc_type(ObVarcharType);

  set_ols_func_common_result_type(type);
  return ret;
}

ObExprOLSPolicyDrop::ObExprOLSPolicyDrop(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_LABEL_SE_POLICY_DROP, N_OLS_POLICY_DROP, 2, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION,
                         false, INTERNAL_IN_ORACLE_MODE)
{
}

ObExprOLSPolicyDrop::~ObExprOLSPolicyDrop()
{
}


/**
 * @brief CREATE_POLICY function
 * @param type        exec_ret
 * @param type1       policy_name       varchar
 * @param type2       drop_column       int
 * @param type_ctx
 * @return
 */
int ObExprOLSPolicyDrop::calc_result_type2(ObExprResType &type,
                                             ObExprResType &type1,
                                             ObExprResType &type2,
                                             ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);

  type1.set_calc_type(ObVarcharType);
  type2.set_calc_type(ObIntType);

  set_ols_func_common_result_type(type);
  return ret;
}

ObExprOLSLevelCreate::ObExprOLSLevelCreate(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_LABEL_SE_LEVEL_CREATE, N_OLS_LEVEL_CREATE, 4, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION,
                         false, INTERNAL_IN_ORACLE_MODE)
{
}

ObExprOLSLevelCreate::~ObExprOLSLevelCreate()
{
}

int ObExprOLSLevelCreate::calc_result_typeN(ObExprResType &type,
                                            ObExprResType *types_array,
                                            int64_t param_num,
                                            ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  UNUSED(param_num);
  if (OB_ISNULL(types_array)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret));
  } else {
    types_array[0].set_calc_type(ObVarcharType);
    types_array[1].set_calc_type(ObIntType);
    types_array[2].set_calc_type(ObVarcharType);
    types_array[3].set_calc_type(ObVarcharType);
  }

  set_ols_func_common_result_type(type);
  return ret;
}

ObExprOLSLevelAlter::ObExprOLSLevelAlter(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_LABEL_SE_LEVEL_ALTER, N_OLS_LEVEL_ALTER, 4, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION,
                         false, INTERNAL_IN_ORACLE_MODE)
{
}

ObExprOLSLevelAlter::~ObExprOLSLevelAlter()
{
}

int ObExprOLSLevelAlter::calc_result_typeN(ObExprResType &type,
                                           ObExprResType *types_array,
                                           int64_t param_num,
                                           ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  UNUSED(param_num);
  if (OB_ISNULL(types_array)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret));
  } else {
    types_array[0].set_calc_type(ObVarcharType);
    types_array[1].set_calc_type(ObIntType);
    types_array[2].set_calc_type(ObVarcharType);
    types_array[3].set_calc_type(ObVarcharType);
  }

  set_ols_func_common_result_type(type);
  return ret;
}

ObExprOLSLevelDrop::ObExprOLSLevelDrop(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_LABEL_SE_LEVEL_DROP, N_OLS_LEVEL_DROP, 2, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION,
                         false, INTERNAL_IN_ORACLE_MODE)
{
}

ObExprOLSLevelDrop::~ObExprOLSLevelDrop()
{
}

int ObExprOLSLevelDrop::calc_result_type2(ObExprResType &type,
                                          ObExprResType &type1,
                                          ObExprResType &type2,
                                          ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);

  type1.set_calc_type(ObVarcharType);
  type2.set_calc_type(ObIntType);

  set_ols_func_common_result_type(type);
  return ret;
}

ObExprOLSLabelCreate::ObExprOLSLabelCreate(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_LABEL_SE_LABEL_CREATE, N_OLS_LABEL_CREATE, 4, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION,
                         false, INTERNAL_IN_ORACLE_MODE)
{
}

ObExprOLSLabelCreate::~ObExprOLSLabelCreate()
{
}

int ObExprOLSLabelCreate::calc_result_typeN(ObExprResType &type,
                                            ObExprResType *types_array,
                                            int64_t param_num,
                                            ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  UNUSED(param_num);
  if (OB_ISNULL(types_array)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret));
  } else {
    types_array[0].set_calc_type(ObVarcharType);
    types_array[1].set_calc_type(ObIntType);
    types_array[2].set_calc_type(ObVarcharType);
    types_array[3].set_calc_type(ObIntType);
  }

  set_ols_func_common_result_type(type);
  return ret;
}

ObExprOLSLabelAlter::ObExprOLSLabelAlter(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_LABEL_SE_LABEL_ALTER, N_OLS_LABEL_ALTER, 4, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION,
                         false, INTERNAL_IN_ORACLE_MODE)
{
}

ObExprOLSLabelAlter::~ObExprOLSLabelAlter()
{
}

int ObExprOLSLabelAlter::calc_result_typeN(ObExprResType &type,
                                           ObExprResType *types_array,
                                           int64_t param_num,
                                           ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  UNUSED(param_num);
  if (OB_ISNULL(types_array)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret));
  } else {
    types_array[0].set_calc_type(ObVarcharType);
    types_array[1].set_calc_type(ObIntType);
    types_array[2].set_calc_type(ObVarcharType);
    types_array[3].set_calc_type(ObIntType);
  }

  set_ols_func_common_result_type(type);
  return ret;
}

ObExprOLSLabelDrop::ObExprOLSLabelDrop(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_LABEL_SE_LABEL_DROP, N_OLS_LABEL_DROP, 2, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION,
                         false, INTERNAL_IN_ORACLE_MODE)
{
}

ObExprOLSLabelDrop::~ObExprOLSLabelDrop()
{
}

int ObExprOLSLabelDrop::calc_result_type2(ObExprResType &type,
                                          ObExprResType &type1,
                                          ObExprResType &type2,
                                          ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);

  type1.set_calc_type(ObVarcharType);
  type2.set_calc_type(ObIntType);

  set_ols_func_common_result_type(type);
  return ret;
}

ObExprOLSTablePolicyApply::ObExprOLSTablePolicyApply(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_LABEL_SE_TABLE_POLICY_APPLY, N_OLS_TABLE_POLICY_APPLY, 6,VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION,
                         false, INTERNAL_IN_ORACLE_MODE)
{
}

ObExprOLSTablePolicyApply::~ObExprOLSTablePolicyApply()
{
}

int ObExprOLSTablePolicyApply::calc_result_typeN(ObExprResType &type,
                                                 ObExprResType *types_array,
                                                 int64_t param_num,
                                                 ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);

  for (int64_t i = 0; i < param_num; ++i) {
    types_array[i].set_calc_type(ObVarcharType);
  }

  set_ols_func_common_result_type(type);
  return ret;
}

ObExprOLSTablePolicyRemove::ObExprOLSTablePolicyRemove(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_LABEL_SE_TABLE_POLICY_REMOVE, N_OLS_TABLE_POLICY_REMOVE, 4, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION,
                         false, INTERNAL_IN_ORACLE_MODE)
{
}

ObExprOLSTablePolicyRemove::~ObExprOLSTablePolicyRemove()
{
}

int ObExprOLSTablePolicyRemove::calc_result_typeN(ObExprResType &type,
                                                  ObExprResType *types_array,
                                                  int64_t param_num,
                                                  ObExprTypeCtx &type_ctx) const
{

  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  UNUSED(param_num);

  types_array[0].set_calc_type(ObVarcharType);
  types_array[1].set_calc_type(ObVarcharType);
  types_array[2].set_calc_type(ObVarcharType);
  types_array[3].set_calc_type(ObIntType);

  set_ols_func_common_result_type(type);
  return ret;
}

ObExprOLSTablePolicyDisable::ObExprOLSTablePolicyDisable(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_LABEL_SE_TABLE_POLICY_DISABLE, N_OLS_TABLE_POLICY_DISABLE, 3, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION,
                         false, INTERNAL_IN_ORACLE_MODE)
{
}

ObExprOLSTablePolicyDisable::~ObExprOLSTablePolicyDisable()
{
}

int ObExprOLSTablePolicyDisable::calc_result_type3(ObExprResType &type,
                                                   ObExprResType &type1,
                                                   ObExprResType &type2,
                                                   ObExprResType &type3,
                                                   ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);

  type1.set_calc_type(ObVarcharType);
  type2.set_calc_type(ObVarcharType);
  type3.set_calc_type(ObVarcharType);

  set_ols_func_common_result_type(type);
  return ret;
}

ObExprOLSTablePolicyEnable::ObExprOLSTablePolicyEnable(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_LABEL_SE_TABLE_POLICY_ENABLE, N_OLS_TABLE_POLICY_ENABLE, 3, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION,
                         false, INTERNAL_IN_ORACLE_MODE)
{
}

ObExprOLSTablePolicyEnable::~ObExprOLSTablePolicyEnable()
{
}

int ObExprOLSTablePolicyEnable::calc_result_type3(ObExprResType &type,
                                                  ObExprResType &type1,
                                                  ObExprResType &type2,
                                                  ObExprResType &type3,
                                                  ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);

  type1.set_calc_type(ObVarcharType);
  type2.set_calc_type(ObVarcharType);
  type3.set_calc_type(ObVarcharType);

  set_ols_func_common_result_type(type);
  return ret;
}

ObExprOLSUserSetLevels::ObExprOLSUserSetLevels(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_LABEL_SE_USER_SET_LEVELS, N_OLS_USER_SET_LEVELS, 6, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION,
                         false, INTERNAL_IN_ORACLE_MODE)
{
}

ObExprOLSUserSetLevels::~ObExprOLSUserSetLevels()
{
}

int ObExprOLSUserSetLevels::calc_result_typeN(ObExprResType &type,
                                             ObExprResType *types_array,
                                             int64_t param_num,
                                             ObExprTypeCtx &type_ctx) const
{

  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  UNUSED(param_num);

  for (int64_t i = 0; i < param_num; ++i) {
    types_array[i].set_calc_type(ObVarcharType);
  }

  set_ols_func_common_result_type(type);
  return ret;
}

ObExprOLSSessionSetLabel::ObExprOLSSessionSetLabel(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_LABEL_SE_SESSION_SET_LABEL, N_OLS_SESSION_SET_LABEL, 2, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION,
                         false, INTERNAL_IN_ORACLE_MODE)
{
}

ObExprOLSSessionSetLabel::~ObExprOLSSessionSetLabel()
{
}

int ObExprOLSSessionSetLabel::calc_result_type2(ObExprResType &type,
                                                ObExprResType &type1,
                                                ObExprResType &type2,
                                                ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);

  type1.set_calc_type(ObVarcharType);
  type2.set_calc_type(ObVarcharType);

  set_ols_func_common_result_type(type);
  return ret;
}

ObExprOLSSessionSetRowLabel::ObExprOLSSessionSetRowLabel(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_LABEL_SE_SESSION_SET_ROW_LABEL, N_OLS_SESSION_SET_ROW_LABEL, 2, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION,
                         false, INTERNAL_IN_ORACLE_MODE)
{
}

ObExprOLSSessionSetRowLabel::~ObExprOLSSessionSetRowLabel()
{
}

int ObExprOLSSessionSetRowLabel::calc_result_type2(ObExprResType &type,
                                                   ObExprResType &type1,
                                                   ObExprResType &type2,
                                                   ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);

  type1.set_calc_type(ObVarcharType);
  type2.set_calc_type(ObVarcharType);

  set_ols_func_common_result_type(type);
  return ret;
}

ObExprOLSSessionRestoreDefaultLabels::ObExprOLSSessionRestoreDefaultLabels(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_LABEL_SE_SESSION_RESTORE_DEFAULT_LABEL, N_OLS_SESSION_RESTORE_DEFAULT_LABEL, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION,
                         false, INTERNAL_IN_ORACLE_MODE)
{
}

ObExprOLSSessionRestoreDefaultLabels::~ObExprOLSSessionRestoreDefaultLabels()
{
}

int ObExprOLSSessionRestoreDefaultLabels::calc_result_type1(ObExprResType &type,
                                                            ObExprResType &type1,
                                                            ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);

  type1.set_calc_type(ObVarcharType);
  set_ols_func_common_result_type(type);
  return ret;
}

ObExprOLSSessionLabel::ObExprOLSSessionLabel(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_LABEL_SE_SESSION_LABEL, N_OLS_SESSION_LABEL, 1, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprOLSSessionLabel::~ObExprOLSSessionLabel()
{
}

int ObExprOLSSessionLabel::calc_result_type1(ObExprResType &type, ObExprResType &type1, ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);

  type1.set_calc_type(ObVarcharType);
  set_ols_func_common_result_type(type);
  return ret;
}


int ObExprOLSSessionLabel::cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  CK (1 == rt_expr.arg_cnt_);
  rt_expr.eval_func_ = ObExprOLSSessionLabel::eval_label;
  return ret;
}
int ObExprOLSSessionLabel::eval_label(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
#ifndef OB_BUILD_LABEL_SECURITY
  int ret = OB_NOT_IMPLEMENT;
#else
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = NULL;
  ObSchemaGetterGuard *schema_guard = NULL;
  uint64_t tenant_id = OB_INVALID_ID;
  ObDatum *param = nullptr;

  if (OB_ISNULL(session =ctx.exec_ctx_.get_my_session())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(session), K(ret));
  } else if (OB_FAIL(init_phy_plan_timeout(ctx.exec_ctx_, *session))) {
    LOG_WARN("fail to init phy plan timeout", K(ret));
  } else if (OB_FAIL(get_schema_guard(&ctx.exec_ctx_, schema_guard))) {
    LOG_WARN("schema guard is NULL", K(ret));
  } else if (OB_FAIL(expr.args_[0]->eval(ctx, param))) {
    LOG_WARN("failed to eval params", K(ret));
  } else {
    tenant_id = session->get_effective_tenant_id();
  }

  ObString policy_name;

  if (OB_SUCC(ret)) {
    policy_name = param->get_string();
    if (OB_FAIL(POLICY_NAME_CHECKER.validate_str_length(policy_name))) {
      LOG_WARN("check policy name failed", K(ret), K(policy_name));
    }
  }

  uint64_t policy_id = OB_INVALID_ID;

  if (OB_SUCC(ret)) {
    const ObLabelSePolicySchema *ols_policy_schema = NULL;

    if (OB_FAIL(schema_guard->get_label_se_policy_schema_by_name(tenant_id,
                                                                 policy_name,
                                                                 ols_policy_schema))) {
      LOG_WARN("fail to get ols policy schema", K(ret));
    } else if (OB_FAIL(POLICY_SCHEMA_CHECKER.validate(ols_policy_schema))) {
      LOG_WARN("policy is not exist", K(ret));
    } else {
      policy_id = ols_policy_schema->get_label_se_policy_id();
    }
  }

  ObLabelSeSessionLabel session_label;
  if (OB_SUCC(ret)) {
    if (OB_SUCCESS == session->get_session_label(policy_id, session_label)) {
      //do nothing
    } else {
      if (OB_FAIL(ObLabelSeUtil::load_default_session_label(tenant_id,
                                                            policy_id, session->get_user_id(),
                                                            *schema_guard, session_label))) {
        LOG_WARN("fail to load default session label", K(ret));
      } else if (OB_FAIL(session->replace_new_session_label(policy_id, session_label))) {
        LOG_WARN("fail to replace new session label", K(ret));
      }
      LOG_DEBUG("load default session label", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    ObLabelSeLabelTag &read_tag = session_label.get_read_label_tag();
    if (read_tag.is_unauth()) {
      res.set_null();
    } else {
      res.set_int(read_tag.get_value());
    }
  }
#endif

  return ret;
}

ObExprOLSSessionRowLabel::ObExprOLSSessionRowLabel(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_LABEL_SE_SESSION_ROW_LABEL, N_OLS_SESSION_ROW_LABEL, 1, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprOLSSessionRowLabel::~ObExprOLSSessionRowLabel()
{
}

int ObExprOLSSessionRowLabel::calc_result_type1(ObExprResType &type, ObExprResType &type1, ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  type1.set_calc_type(ObVarcharType);
  set_ols_func_common_result_type(type);
  return ret;
}


int ObExprOLSSessionRowLabel::cg_expr(ObExprCGCtx &op_cg_ctx,
                                      const ObRawExpr &raw_expr,
                                      ObExpr &rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  CK (1 == rt_expr.arg_cnt_);
  rt_expr.eval_func_ = ObExprOLSSessionRowLabel::eval_row_label;
  return ret;
}

int ObExprOLSSessionRowLabel::eval_row_label(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
#ifndef OB_BUILD_LABEL_SECURITY
  int ret = OB_NOT_IMPLEMENT;
#else
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = NULL;
  ObSchemaGetterGuard *schema_guard = NULL;
  uint64_t tenant_id = OB_INVALID_ID;
  ObDatum *param = nullptr;

  if (OB_ISNULL(session = ctx.exec_ctx_.get_my_session())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(session), K(ret));
  } else if (OB_FAIL(init_phy_plan_timeout(ctx.exec_ctx_, *session))) {
    LOG_WARN("fail to init phy plan timeout", K(ret));
  } else if (OB_FAIL(get_schema_guard(&ctx.exec_ctx_, schema_guard))) {
    LOG_WARN("schema guard is NULL", K(ret));
  } else if (OB_FAIL(expr.args_[0]->eval(ctx, param))) {
    LOG_WARN("failed to eval params", K(ret));
  } else {
    tenant_id = session->get_effective_tenant_id();
  }

  ObString policy_name;

  if (OB_SUCC(ret)) {
    policy_name = param->get_string();
    if (OB_FAIL(POLICY_NAME_CHECKER.validate_str_length(policy_name))) {
      LOG_WARN("check policy name failed", K(ret), K(policy_name));
    }
  }

  uint64_t policy_id = OB_INVALID_ID;

  if (OB_SUCC(ret)) {
    const ObLabelSePolicySchema *ols_policy_schema = NULL;

    if (OB_FAIL(schema_guard->get_label_se_policy_schema_by_name(tenant_id,
                                                                 policy_name,
                                                                 ols_policy_schema))) {
      LOG_WARN("fail to get ols policy schema", K(ret));
    } else if (OB_FAIL(POLICY_SCHEMA_CHECKER.validate(ols_policy_schema))) {
      LOG_WARN("policy is not exist", K(ret));
    } else {
      policy_id = ols_policy_schema->get_label_se_policy_id();
    }
  }

  ObLabelSeSessionLabel session_label;
  if (OB_SUCC(ret)) {
    if (OB_SUCCESS == session->get_session_label(policy_id, session_label)) {
      //do nothing
    } else {
      if (OB_FAIL(ObLabelSeUtil::load_default_session_label(tenant_id,
                                                            policy_id, session->get_user_id(),
                                                            *schema_guard, session_label))) {
        LOG_WARN("fail to load default session label", K(ret));
      } else if (OB_FAIL(session->replace_new_session_label(policy_id, session_label))) {
        LOG_WARN("fail to replace new session label", K(ret));
      }
      LOG_DEBUG("load default session label", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    ObLabelSeLabelTag &write_tag = session_label.get_write_label_tag();
    if (write_tag.is_unauth()) {
      res.set_null();
    } else {
      res.set_int(write_tag.get_value());
    }
  }
#endif

  return ret;
}

ObExprOLSLabelCmpLE::ObExprOLSLabelCmpLE(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_LABEL_SE_LABEL_VALUE_CMP_LE, N_OLS_LABEL_VALUE_CMP_LE, 2, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION,
                         false, INTERNAL_IN_ORACLE_MODE)
{
}

ObExprOLSLabelCmpLE::~ObExprOLSLabelCmpLE()
{
}

int ObExprOLSLabelCmpLE::calc_result_type2(ObExprResType &type,
                                           ObExprResType &type1,
                                           ObExprResType &type2,
                                           ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);

  type1.set_calc_type(ObIntType);
  type2.set_calc_type(ObIntType);

  set_ols_func_common_result_type(type);
  return ret;
}

int ObExprOLSLabelCmpLE::cg_expr(ObExprCGCtx &op_cg_ctx,
                                  const ObRawExpr &raw_expr,
                                  ObExpr &rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  CK (2 == rt_expr.arg_cnt_);
  if (OB_SUCC(ret)) {
    rt_expr.eval_func_ = eval_cmple;
  }
  return ret;
}

int ObExprOLSLabelCmpLE::eval_cmple(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
#ifndef OB_BUILD_LABEL_SECURITY
  int ret = OB_NOT_IMPLEMENT;
#else
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = NULL;
  ObSchemaGetterGuard *schema_guard = NULL;
  int64_t label_tag[2] = {-1, -1};
  // uint64_t policy_id[2] = {OB_INVALID_ID, OB_INVALID_ID};
  enum {COLUMN_LABEL = 0, SESSION_LABEL};
  ObDatum *param1 = nullptr;
  ObDatum *param2 = nullptr;
  if (OB_ISNULL(session = ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get session", K(ret));
  } else if (OB_FAIL(expr.eval_param_value(ctx, param1, param2))) {
    LOG_WARN("failed to eval parmas", K(ret));
  } else if (OB_FAIL(get_schema_guard(&ctx.exec_ctx_, schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret));
  } else if (param1->is_null()) {
    //column label is null, noone can read it, return fail(-1)
    res.set_int(-1);
  } else if (param2->is_null()) {
    //session label is null, column label is not null, can't access return fail(-1)
    res.set_int(-1);
  } else {
    //从参数中获取label tag
    if (OB_SUCC(ret)) {
      label_tag[COLUMN_LABEL] = param1->get_int();
      label_tag[SESSION_LABEL] = param2->get_int();
      if (OB_FAIL(LABEL_TAG_CHECKER.validate(label_tag[COLUMN_LABEL]))
                 || OB_FAIL(LABEL_TAG_CHECKER.validate(label_tag[SESSION_LABEL]))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unknown label tag", K(ret),
                 K(label_tag[COLUMN_LABEL]), K(label_tag[SESSION_LABEL]));
      }
    }

    //比较component值，返回函数结果
    if (OB_SUCC(ret)) {
      int64_t cmp_result = -1;
      if (OB_FAIL(ObExprOLSUtil::label_tag_compare(session->get_effective_tenant_id(),
                                                   *schema_guard,
                                                   label_tag[COLUMN_LABEL],
                                                   label_tag[SESSION_LABEL],
                                                   cmp_result))) {
        LOG_WARN("fail to compare label tag", K(ret));
      } else {
        res.set_int(cmp_result);
      }
    }
  }
#endif
  return ret;
}


ObExprOLSLabelCheck::ObExprOLSLabelCheck(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_LABEL_SE_LABEL_VALUE_CHECK, N_OLS_LABEL_VALUE_CHECK, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION,
                         false, INTERNAL_IN_ORACLE_MODE)
{
}

ObExprOLSLabelCheck::~ObExprOLSLabelCheck()
{
}

int ObExprOLSLabelCheck::calc_result_type1(ObExprResType &type,
                                           ObExprResType &type1,
                                           ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);

  type1.set_calc_type(ObIntType);

  set_ols_func_common_result_type(type);
  return ret;
}

int ObExprOLSLabelCheck::cg_expr(ObExprCGCtx &op_cg_ctx,
                                 const ObRawExpr &raw_expr,
                                 ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  CK (1 == rt_expr.arg_cnt_);
  if (OB_SUCC(ret)) {
    rt_expr.eval_func_ = eval_label_check;
  }
  return ret;
}

int ObExprOLSLabelCheck::eval_label_check(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
#ifndef OB_BUILD_LABEL_SECURITY
  int ret = OB_NOT_IMPLEMENT;
#else
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = NULL;
  ObSchemaGetterGuard *schema_guard = NULL;
  const ObLabelSeLabelSchema *ols_label_schema = NULL;

  ObLabelSeLabelTag label_tag;

  ObDatum *param = nullptr;
  if (OB_ISNULL(session = ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get session failed", K(ret));
  } else if (OB_FAIL(get_schema_guard(&ctx.exec_ctx_, schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret));
  } else if (OB_FAIL(expr.eval_param_value(ctx, param))) {
    LOG_WARN("failed to eval params", K(ret));
  }

  if (OB_SUCC(ret)) {
    if (param->is_null()) {
      label_tag.set_unauth_value();
    } else if (ObIntType == expr.args_[0]->datum_meta_.type_) {
      int64_t label_tag_value = param->get_int();
      uint64_t tenant_id = session->get_effective_tenant_id();

      if (OB_FAIL(LABEL_TAG_CHECKER.validate(label_tag_value))) {
        LOG_WARN("invalid label tag value", K(ret));
      } else if (OB_FAIL(schema_guard->get_label_se_label_by_label_tag(tenant_id,
                                                                       label_tag_value,
                                                                       ols_label_schema))) {
        LOG_WARN("get label schema failed", K(ret));
      } else if (OB_FAIL(LABEL_SCHEMA_CHECKER.validate(ols_label_schema))) {
        LOG_WARN("label schema not exist", K(ret));
      } else if (ols_label_schema->get_flag() != 1) {
        ret = OB_ERR_POLICY_WITH_CHECK_OPTION_VIOLATION;
        LOG_WARN("data label is false for write dml", K(ret));
      } else {
        ObString label_text = ols_label_schema->get_label_str();
        uint64_t policy_id = ols_label_schema->get_label_se_policy_id();
        //TODO [label] get label_nums from ols_label_schema->get_label_num_str()
        ObLabelSeDecomposedLabel label_comps;
        ObLabelSeLabelCompNums label_nums;
        if (OB_FAIL(ObLabelSeResolver::resolve_label_text(label_text, label_comps))) {
          LOG_WARN("fail to resolve label", K(ret));
        } else if (OB_FAIL(ObLabelSeUtil::convert_label_comps_name_to_num(tenant_id, policy_id,
                                                                          *schema_guard,
                                                                          label_comps, label_nums))) {
          LOG_WARN("fail to validate component", K(ret), K(label_comps));
        } else if (OB_FAIL(ObLabelSeUtil::validate_user_auth(tenant_id, policy_id,
                                                             session->get_user_id(),
                                                             *schema_guard, label_nums, true))) {
          LOG_WARN("validate user auth failed", K(ret));
        } else {
          label_tag.set_value(label_tag_value);
        }
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid obj type", K(expr.args_[0]->datum_meta_.type_));
    }
  }

  if (OB_SUCC(ret)) {
    if (label_tag.is_unauth()) {
      res.set_null();
    } else {
      res.set_int(label_tag.get_value());
    }
  }
#endif
  return ret;
}

ObExprOLSLabelToChar::ObExprOLSLabelToChar(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_LABEL_SE_LABEL_VALUE_TO_CHAR,
                         N_OLS_LABEL_VALUE_TO_CHAR, 1, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprOLSLabelToChar::~ObExprOLSLabelToChar()
{
}

int ObExprOLSLabelToChar::calc_result_type1(ObExprResType &type,
                                            ObExprResType &type1,
                                            ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;

  type1.set_calc_type(ObIntType);

  type.set_varchar();
  type.set_collation_type(get_default_collation_type(type.get_type(), *type_ctx.get_session()));

  return ret;
}

int ObExprOLSLabelToChar::cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  CK (1 == rt_expr.arg_cnt_);
  if (OB_SUCC(ret)) {
    rt_expr.eval_func_ = eval_label_to_char;
  }
  return ret;
}

int ObExprOLSLabelToChar::eval_label_to_char(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
#ifndef OB_BUILD_LABEL_SECURITY
  int ret = OB_NOT_IMPLEMENT;
#else
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = NULL;
  ObSchemaGetterGuard *schema_guard = NULL;
  const ObLabelSeLabelSchema *ols_label_schema = NULL;
  ObDatum *param = nullptr;

  if (OB_ISNULL(session = ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get session", K(ret));
  } else if (OB_FAIL(get_schema_guard(&ctx.exec_ctx_, schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret));
  } else if (OB_FAIL(expr.eval_param_value(ctx, param))) {
    LOG_WARN("failed to eval param value", K(ret));
  }

  if (OB_SUCC(ret)) {
    if (param->is_null()) {
      res.set_null();
    } else if (ObIntType == expr.args_[0]->datum_meta_.type_) {
      int64_t label_tag_value = param->get_int();

      if (OB_FAIL(LABEL_TAG_CHECKER.validate(label_tag_value))) {
        LOG_WARN("invalid label tag value", K(ret));
      } else if (OB_FAIL(schema_guard->get_label_se_label_by_label_tag(
                           session->get_effective_tenant_id(),
                           label_tag_value,
                           ols_label_schema))) {
        LOG_WARN("get label schema failed", K(ret));
      } else if (OB_FAIL(LABEL_SCHEMA_CHECKER.validate(ols_label_schema))) {
        LOG_WARN("label schema not exist", K(ret));
      } else {
        ObString label_text = ols_label_schema->get_label_str();
        res.set_string(label_text);
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid obj type", K(expr.args_[0]->datum_meta_.type_));
    }
  }
#endif
  return ret;
}


ObExprOLSCharToLabel::ObExprOLSCharToLabel(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_LABEL_SE_CHAR_TO_LABEL_VALUE,
                         N_OLS_CHAR_TO_LABEL_VALUE, 2, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprOLSCharToLabel::~ObExprOLSCharToLabel()
{
}

int ObExprOLSCharToLabel::calc_result_type2(ObExprResType &type,
                                            ObExprResType &type1,
                                            ObExprResType &type2,
                                            ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);

  type1.set_calc_type(ObVarcharType);
  type2.set_calc_type(ObVarcharType);

  set_ols_func_common_result_type(type);
  return ret;
}

int ObExprOLSCharToLabel::cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  CK (2 == rt_expr.arg_cnt_);
  if (OB_SUCC(ret)) {
    rt_expr.eval_func_ = eval_char_to_label;
  }
  return ret;
}
int ObExprOLSCharToLabel::eval_char_to_label(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
#ifndef OB_BUILD_LABEL_SECURITY
  int ret = OB_NOT_IMPLEMENT;
#else
  int ret = OB_SUCCESS;
  ObString policy_name;
  ObString label_text;

  ObSQLSessionInfo *session = NULL;
  ObSchemaGetterGuard *schema_guard = NULL;
  ObSqlString stmt_param_str;

  uint64_t tenant_id = OB_INVALID_ID;
  ObObj obj1;
  ObObj obj2;
  ObDatum *param1 = nullptr;
  ObDatum *param2 = nullptr;

  if (OB_ISNULL(session = ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get session failed", K(ret));
  } else if (OB_FAIL(init_phy_plan_timeout(ctx.exec_ctx_, *session))) {
    LOG_WARN("fail to init phy plan timeout", K(ret));
  } else if (OB_FAIL(get_schema_guard(&ctx.exec_ctx_, schema_guard))) {
    LOG_WARN("schema guard is NULL", K(ret));
  } else if (OB_FAIL(expr.eval_param_value(ctx, param1, param2))) {
    LOG_WARN("failed to eval params", K(ret));
  } else if (param1->to_obj(obj1, expr.args_[0]->obj_meta_)) {
    LOG_WARN("failed to convert to obj", K(ret));
  } else if (param2->to_obj(obj2, expr.args_[1]->obj_meta_)) {
    LOG_WARN("failed to convert to obj", K(ret));
  } else {
    tenant_id = session->get_effective_tenant_id();
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(get_string_from_obj_and_check(&ctx.exec_ctx_.get_allocator(), obj1,
                                              policy_name, POLICY_NAME_CHECKER,
                                              stmt_param_str))) {
      LOG_WARN("fail to check input value", K(ret));
    } else if (OB_FAIL(get_string_from_obj_and_check(&ctx.exec_ctx_.get_allocator(), obj2,
                                                     label_text, LABEL_TEXT_CHECKER,
                                                     stmt_param_str))) {
      LOG_WARN("fail to check input value", K(ret));
    }
  }

  uint64_t policy_id = OB_INVALID_ID;

  if (OB_SUCC(ret)) {
    const ObLabelSePolicySchema *ols_policy_schema = NULL;

    if (OB_FAIL(schema_guard->get_label_se_policy_schema_by_name(tenant_id,
                                                                 policy_name,
                                                                 ols_policy_schema))) {
      LOG_WARN("fail to get ols policy schema", K(ret));
    } else if (OB_FAIL(POLICY_SCHEMA_CHECKER.validate(ols_policy_schema))) {
      LOG_WARN("policy is not exist", K(ret));
    } else {
      policy_id = ols_policy_schema->get_label_se_policy_id();
    }
  }

  //resolve session label
  ObLabelSeDecomposedLabel label_comps;
  ObLabelSeLabelCompNums label_nums;
  ObString normalized_label_text;

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObLabelSeResolver::resolve_label_text(label_text, label_comps))) {
      LOG_WARN("fail to resolve label", K(ret));
    } else if (OB_FAIL(ObLabelSeUtil::convert_label_comps_name_to_num(tenant_id, policy_id,
                                                                      *schema_guard,
                                                                      label_comps, label_nums))) {
      LOG_WARN("fail to validate component", K(ret), K(label_comps));
    } else if (OB_FAIL(ObLabelSeResolver::construct_label_text(label_comps,
                                                               &ctx.exec_ctx_.get_allocator(),
                                                               normalized_label_text))) {
      LOG_WARN("fail to construct label text", K(ret));
    }
  }

  int64_t new_label_tag;

  if (OB_SUCC(ret)) {
    const ObLabelSeLabelSchema *ols_label_schema = NULL;

    if (OB_FAIL(schema_guard->get_label_se_label_schema_by_name(tenant_id,
                                                                normalized_label_text,
                                                                ols_label_schema))) {
      LOG_WARN("get label schema failed", K(ret));
    } else if (OB_FAIL(LABEL_SCHEMA_CHECKER.validate(ols_label_schema))) {
      //TODO [label] create one
      LOG_WARN("label schema not exist", K(ret), K(normalized_label_text));
    } else {
      new_label_tag = ols_label_schema->get_label_tag();
    }
  }

  if (OB_SUCC(ret)) {
    res.set_int(new_label_tag);
  }
#endif

  return ret;
}


int ObExprOLSBase::set_session_schema_update_flag(ObExprCtx &expr_ctx) const
{
  int ret = OB_SUCCESS;
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObExecContext *exec_ctx = NULL;

  if (OB_ISNULL(exec_ctx = expr_ctx.exec_ctx_)
      || OB_ISNULL(expr_ctx.my_session_)
      || OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get executor context failed", K(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_received_broadcast_version(expr_ctx.my_session_->get_effective_tenant_id(), new_schema_version))) {
    LOG_WARN("fail to get received_schema_version", K(ret));
  } else if (OB_FAIL(expr_ctx.my_session_->get_ob_last_schema_version(new_schema_version))) {
    LOG_WARN("failed to get_sys_variable", K(new_schema_version), K(ret));
  }

  return ret;
}

} //namespace sql
} //namespace oceanbase
