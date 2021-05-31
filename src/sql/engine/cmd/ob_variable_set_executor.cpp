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

#define USING_LOG_PREFIX SQL_ENG

#include "lib/string/ob_sql_string.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "common/sql_mode/ob_sql_mode_utils.h"
#include "observer/ob_server_struct.h"
#include "observer/ob_sql_client_decorator.h"
#include "share/ob_i_sql_expression.h"
#include "share/ob_schema_status_proxy.h"
#include "share/ob_common_rpc_proxy.h"
#include "share/object/ob_obj_cast.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/schema/ob_schema_utils.h"
#include "sql/engine/cmd/ob_variable_set_executor.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/code_generator/ob_expr_generator_impl.h"
#include "sql/code_generator/ob_column_index_provider.h"
#include "sql/ob_sql_trans_control.h"
#include "sql/ob_end_trans_callback.h"
#include "lib/timezone/ob_oracle_format_models.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
namespace oceanbase {
namespace sql {

#define DEFINE_CAST_CTX()                                                                              \
  ObCollationType cast_coll_type = CS_TYPE_INVALID;                                                    \
  if (NULL != ctx.get_my_session()) {                                                                  \
    if (common::OB_SUCCESS != ctx.get_my_session()->get_collation_connection(cast_coll_type)) {        \
      LOG_WARN("fail to get collation_connection");                                                    \
      cast_coll_type = ObCharset::get_default_collation(ObCharset::get_default_charset());             \
    } else {                                                                                           \
    }                                                                                                  \
  } else {                                                                                             \
    LOG_WARN("session is null");                                                                       \
    cast_coll_type = ObCharset::get_system_collation();                                                \
  }                                                                                                    \
  const ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params(ctx.get_my_session()); \
  ObCastCtx cast_ctx(                                                                                  \
      &calc_buf, &dtc_params, get_cur_time(ctx.get_physical_plan_ctx()), CM_NONE, cast_coll_type, (NULL));

ObVariableSetExecutor::ObVariableSetExecutor()
{}

ObVariableSetExecutor::~ObVariableSetExecutor()
{}

int ObVariableSetExecutor::execute(ObExecContext& ctx, ObVariableSetStmt& stmt)
{
  int ret = OB_SUCCESS;
  int ret_ac = OB_SUCCESS;
  ObSQLSessionInfo* session = NULL;
  ObMySQLProxy* sql_proxy = NULL;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  if (OB_ISNULL(session = ctx.get_my_session()) || OB_ISNULL(sql_proxy = ctx.get_sql_proxy()) ||
      OB_ISNULL(plan_ctx = ctx.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session or sql proxy or physical plan ctx is NULL", K(ret), K(session), K(sql_proxy), K(plan_ctx));
  } else {
    ObPhysicalPlan phy_plan;
    ObPhysicalPlanCtx phy_plan_ctx(ctx.get_allocator());
    ObExprCtx expr_ctx;
    phy_plan_ctx.set_phy_plan(&phy_plan);
    phy_plan_ctx.set_last_insert_id_session(session->get_local_last_insert_id());
    const int64_t cur_time =
        plan_ctx->has_cur_time() ? plan_ctx->get_cur_time().get_timestamp() : ObTimeUtility::current_time();
    phy_plan_ctx.set_cur_time(cur_time, *session);

    expr_ctx.phy_plan_ctx_ = &phy_plan_ctx;
    expr_ctx.my_session_ = session;
    expr_ctx.exec_ctx_ = &ctx;
    expr_ctx.calc_buf_ = &ctx.get_allocator();
    if (OB_ISNULL(expr_ctx.exec_ctx_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("expr_ctx.exec_ctx_ is NULL", K(ret));
    } else {
      expr_ctx.exec_ctx_->set_sql_proxy(sql_proxy);
    }
    ObVariableSetStmt::VariableSetNode tmp_node;  // just for init node
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_variables_size(); ++i) {
      ObVariableSetStmt::VariableSetNode& node = tmp_node;
      if (OB_FAIL(stmt.get_variable_node(i, node))) {
        LOG_WARN("fail to get variable node", K(i), K(ret));
      } else {
        ObNewRow tmp_row;
        ObObj value_obj;
        ObBasicSysVar* sys_var = NULL;
        RowDesc row_desc;
        ObExprGeneratorImpl expr_gen(0, 0, NULL, row_desc);
        ObSqlExpression sql_expr(ctx.get_allocator(), 0);
        if (true == node.is_set_default_) {
          if (false == node.is_system_variable_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("when reach here, node.is_system_variable_ must be true", K(ret));
          } else {
          }
        } else {
          if (OB_ISNULL(node.value_expr_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("node.value_expr_ is NULL", K(ret));
          } else {
            if ((!node.is_system_variable_) || (!is_strict_mode(session->get_sql_mode()))) {
              expr_ctx.cast_mode_ = CM_WARN_ON_FAIL;
            } else {
            }
            if (OB_FAIL(expr_gen.generate(*node.value_expr_, sql_expr))) {
              LOG_WARN("fail to fill sql expression", K(ret));
            } else if (FALSE_IT(phy_plan.set_regexp_op_count(expr_gen.get_cur_regexp_op_count()))) {
            } else if (FALSE_IT(phy_plan.set_like_op_count(expr_gen.get_cur_like_op_count()))) {
            } else if (OB_FAIL(sql_expr.calc(expr_ctx, tmp_row, value_obj))) {
              LOG_WARN("fail to calc value", K(ret), K(*node.value_expr_));
            } else {
            }
          }
        }
        if (OB_FAIL(ret)) {
        } else if (false == node.is_system_variable_) {
          if (OB_FAIL(set_user_variable(value_obj, node.variable_name_, expr_ctx))) {
            LOG_WARN("set user variable failed", K(ret));
          }
        } else {
          ObSetVar set_var(node.variable_name_,
              node.set_scope_,
              node.is_set_default_,
              stmt.get_actual_tenant_id(),
              *expr_ctx.calc_buf_,
              *sql_proxy);
          ObObj out_obj;
          const bool is_set_stmt = true;
          if (OB_FAIL(session->get_sys_variable_by_name(node.variable_name_, sys_var))) {
            if (OB_ERR_SYS_VARIABLE_UNKNOWN == ret) {
              // session system variable not found, maybe latest data from proxy,
              // so search in __all_sys_variable first.
              const uint64_t tenant_id = session->get_effective_tenant_id();
              const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
              ObSQLClientRetryWeak sql_client_retry_weak(sql_proxy, exec_tenant_id, OB_ALL_SYS_VARIABLE_TID);
              ObObj tmp_val;
              ObSqlString sql;
              SMART_VAR(ObMySQLProxy::MySQLResult, res)
              {
                sqlclient::ObMySQLResult* result = NULL;
                if (OB_FAIL(sql.assign_fmt("select 1 from %s where tenant_id=%lu and name='%.*s';",
                        OB_ALL_SYS_VARIABLE_TNAME,
                        ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                        node.variable_name_.length(),
                        node.variable_name_.ptr()))) {
                  LOG_WARN("assign sql string failed", K(ret));
                } else if (OB_FAIL(sql_client_retry_weak.read(res, exec_tenant_id, sql.ptr()))) {
                  LOG_WARN("execute sql failed", K(sql), K(ret));
                } else if (OB_ISNULL(result = res.get_result())) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("fail to get sql result", K(ret));
                } else if (OB_FAIL(result->next())) {
                  if (OB_ITER_END == ret) {
                    // not found in inner table, means it is not a system variable
                    ret = OB_ERR_SYS_VARIABLE_UNKNOWN;
                    LOG_USER_ERROR(
                        OB_ERR_SYS_VARIABLE_UNKNOWN, node.variable_name_.length(), node.variable_name_.ptr());
                  } else {
                    LOG_WARN("get result failed", K(ret));
                  }
                } else {
                  // found in __all_sys_variable, means it's caused by version compatibility.
                  // return OB_SYS_VARS_MAYBE_DIFF_VERSION, set OB_SUCCESS later.
                  ret = OB_SYS_VARS_MAYBE_DIFF_VERSION;
                  LOG_INFO("try to set sys var from new version, ignore it", K(ret), K(node.variable_name_));
                }
              }
            } else {
              LOG_WARN("fail to get system variable", K(ret), K(node.variable_name_));
            }
          } else if (OB_ISNULL(sys_var)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("sys_var is NULL", K(ret), K(node.variable_name_));
          } else if (!share::is_mysql_mode() && sys_var->is_mysql_only()) {
            // ignore set mysql only variables in oracle mode
          } else if (!share::is_oracle_mode() && sys_var->is_oracle_only()) {
            // ignore set oracle only variables in mysql mode
          } else {
            if (OB_FAIL(check_and_convert_sys_var(ctx, set_var, *sys_var, value_obj, out_obj, is_set_stmt))) {
              LOG_WARN("fail to check", K(ret), K(node), K(*sys_var), K(value_obj));
            } else if (FALSE_IT(value_obj = out_obj)) {
            } else if (OB_FAIL(cast_value(ctx,
                           node,
                           stmt.get_actual_tenant_id(),
                           *expr_ctx.calc_buf_,
                           *sys_var,
                           value_obj,
                           out_obj))) {
              LOG_WARN("fail to cast value", K(ret), K(node), K(*sys_var), K(value_obj));
            } else if (FALSE_IT(value_obj = out_obj)) {
            } else if (node.variable_name_ == OB_SV_AUTO_INCREMENT_INCREMENT ||
                       node.variable_name_ == OB_SV_AUTO_INCREMENT_OFFSET) {
              if (OB_FAIL(process_auto_increment_hook(session->get_sql_mode(), node.variable_name_, value_obj))) {
                LOG_WARN("fail to process auto increment hook", K(ret));
              } else {
              }
            } else if (node.variable_name_ == OB_SV_LAST_INSERT_ID) {
              if (OB_FAIL(
                      process_last_insert_id_hook(plan_ctx, session->get_sql_mode(), node.variable_name_, value_obj))) {
                LOG_WARN("fail to process auto increment hook", K(ret));
              } else {
              }
            } else if (ObSetVar::SET_SCOPE_GLOBAL == node.set_scope_ &&
                       node.variable_name_ == OB_SV_STMT_PARALLEL_DEGREE) {
              const static int64_t PARALLEL_DEGREE_VALID_VALUE = 1;
              int64_t parallel_degree_set_val = -1;
              if (OB_FAIL(value_obj.get_int(parallel_degree_set_val))) {
                LOG_WARN("fail to get int64_t from value_obj", K(ret), K(value_obj));
              } else if (PARALLEL_DEGREE_VALID_VALUE != parallel_degree_set_val) {
                ret = OB_ERR_WRONG_VALUE_FOR_VAR;
                char buf[32];
                (void)databuff_printf(buf, 32, "%ld", parallel_degree_set_val);
                ObString value(buf);
                LOG_USER_ERROR(OB_ERR_WRONG_VALUE_FOR_VAR,
                    node.variable_name_.length(),
                    node.variable_name_.ptr(),
                    value.length(),
                    value.ptr());
              }
            } else {
            }

            if (OB_FAIL(ret)) {
            } else if (ObSetVar::SET_SCOPE_SESSION == node.set_scope_) {
              // handle autocommit case, must call before update_sys_variable,
              // because update_sys_variable will change value of ac.
              if (node.variable_name_ == OB_SV_AUTOCOMMIT) {
                if (OB_UNLIKELY(OB_SUCCESS != (ret_ac = process_session_autocommit_hook(ctx, value_obj)))) {
                  LOG_WARN("fail to process session autocommit", K(ret), K(ret_ac));
                  if (OB_ERR_WRONG_VALUE_FOR_VAR == ret_ac) {
                    ret = ret_ac;
                  }
                } else {
                }
              } else {
              }
            }

            if (OB_FAIL(ret)) {
            } else if (set_var.var_name_ == OB_SV_READ_ONLY) {
              if (session->get_in_transaction()) {
                ret = OB_ERR_LOCK_OR_ACTIVE_TRANSACTION;

                LOG_WARN("Can't execute the given command because "
                         "you have active locked tables or an active transaction",
                    K(ret));
              } else {
              }
            } else {
            }

            if (OB_FAIL(ret)) {
            } else if (set_var.var_name_ == OB_SV_COMPATIBILITY_MODE) {
              if (!(OB_SYS_TENANT_ID == session->get_effective_tenant_id()) || !GCONF.in_upgrade_mode()) {
                ret = OB_OP_NOT_ALLOW;
                LOG_WARN("Compatibility mode can be changed only under upgrade mode and system tenant",
                    K(ret),
                    K(session->get_effective_tenant_id()));
                LOG_USER_ERROR(
                    OB_OP_NOT_ALLOW, "Compatibility mode be changed not under upgrade mode and system tenant");
              } else if (ObSetVar::SET_SCOPE_SESSION != set_var.set_scope_) {
                ret = OB_OP_NOT_ALLOW;
                LOG_WARN("Compatibility mode can be changed only under session scope",
                    K(ret),
                    K(session->get_effective_tenant_id()));
                LOG_USER_ERROR(OB_OP_NOT_ALLOW, "Compatibility mode be changed not in session scope");
              }
            } else {
            }

            if (OB_SUCC(ret) && set_var.set_scope_ == ObSetVar::SET_SCOPE_GLOBAL) {
              if (OB_FAIL(update_global_variables(ctx, stmt, set_var, value_obj))) {
                LOG_WARN("failed to update global variables", K(ret));
              } else {
              }
            }
            if (OB_SUCC(ret) && set_var.set_scope_ == ObSetVar::SET_SCOPE_SESSION) {
              if (OB_FAIL(sys_var->session_update(ctx, set_var, value_obj))) {
                LOG_WARN("fail to update", K(ret), K(*sys_var), K(set_var), K(value_obj));
              }
            }
            if (OB_SUCC(ret)) {
              if (OB_FAIL(sys_var->update(ctx, set_var, value_obj))) {
                LOG_WARN("update sys var state failed", K(ret), K(set_var));
              }
            }
          }
        }
      }
      if (OB_SYS_VARS_MAYBE_DIFF_VERSION == ret) {
        ret = OB_SUCCESS;
      }
    }
  }
  if (OB_SUCCESS != ret_ac) {
    ret = ret_ac;
  }
  return ret;
}

int ObVariableSetExecutor::set_user_variable(const ObObj& val, const ObString& variable_name, const ObExprCtx& expr_ctx)
{
  int ret = OB_SUCCESS;
  // user defined tmp variable
  ObSQLSessionInfo* session = expr_ctx.my_session_;
  ObSessionVariable sess_var;
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (OB_FAIL(switch_to_session_variable(expr_ctx, val, sess_var))) {
    LOG_WARN("fail to stiwch to session variablee", K(ret), K(val));
  } else if (OB_FAIL(session->replace_user_variable(variable_name, sess_var))) {
    LOG_WARN("set variable to session plan failed", K(ret), K(variable_name));
  } else {
  }
  return ret;
}

int ObVariableSetExecutor::update_global_variables(
    ObExecContext& ctx, ObDDLStmt& stmt, const ObSetVar& set_var, const ObObj& val)
{
  int ret = OB_SUCCESS;
  obrpc::ObRpcOpts rpc_opt;
  ObSQLSessionInfo* session = NULL;
  ObTaskExecutorCtx* task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy* common_rpc_proxy = NULL;
  obrpc::ObModifySysVarArg& arg = static_cast<obrpc::ObModifySysVarArg&>(stmt.get_ddl_arg());
  ObString extra_var_name;
  ObString extra_var_value;
  ObString extra_val;
  ObString val_str;
  ObCollationType extra_coll_type = CS_TYPE_INVALID;
  char extra_var_value_buf[32] = {'\0'};
  int64_t pos = 0;
  bool should_update_extra_var = false;
  if (OB_ISNULL(session = ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else {
    arg.tenant_id_ = set_var.actual_tenant_id_;
    arg.exec_tenant_id_ = set_var.actual_tenant_id_;
    ObString first_stmt;
    if (OB_FAIL(stmt.get_first_stmt(first_stmt))) {
      LOG_WARN("fail to get first stmt", K(ret));
    } else {
      arg.ddl_stmt_str_ = first_stmt;
    }
    if (OB_FAIL(ret)) {
    } else if (set_var.var_name_ == OB_SV_COLLATION_SERVER || set_var.var_name_ == OB_SV_COLLATION_DATABASE ||
               set_var.var_name_ == OB_SV_COLLATION_CONNECTION) {
      ObString coll_str;
      int64_t coll_int64 = OB_INVALID_INDEX;
      if (OB_FAIL(val.get_int(coll_int64))) {
        LOG_WARN("get int from val failed", K(ret));
      } else if (OB_UNLIKELY(!ObCharset::is_valid_collation(coll_int64))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid collation", K(ret), K(coll_int64), K(val));
      } else if (FALSE_IT(coll_str = ObString::make_string(
                              ObCharset::collation_name(static_cast<ObCollationType>(coll_int64))))) {
        // do nothing
      } else if (OB_FAIL(ObBasicSysVar::get_charset_var_and_val_by_collation(
                     set_var.var_name_, coll_str, extra_var_name, extra_val, extra_coll_type))) {
        LOG_ERROR(
            "fail to get charset variable and value by collation", K(ret), K(set_var.var_name_), K(val), K(coll_str));
      } else if (OB_FAIL(databuff_printf(extra_var_value_buf,
                     sizeof(extra_var_value_buf),
                     pos,
                     "%d",
                     static_cast<int32_t>(extra_coll_type)))) {
        LOG_WARN("databuff printf failed", K(extra_coll_type), K(ret));
      } else {
        extra_var_value.assign(extra_var_value_buf, pos);
        should_update_extra_var = true;
      }
    } else if (set_var.var_name_ == OB_SV_CHARACTER_SET_SERVER || set_var.var_name_ == OB_SV_CHARACTER_SET_DATABASE ||
               set_var.var_name_ == OB_SV_CHARACTER_SET_CONNECTION) {
      ObString cs_str;
      int64_t coll_int64 = OB_INVALID_INDEX;
      if (OB_FAIL(val.get_int(coll_int64))) {
        LOG_WARN("get int from value failed", K(ret));
      } else if (OB_UNLIKELY(!ObCharset::is_valid_collation(coll_int64))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid collation", K(ret), K(coll_int64));
      } else if (FALSE_IT(cs_str = ObString::make_string(ObCharset::charset_name(
                              ObCharset::charset_type_by_coll(static_cast<ObCollationType>(coll_int64)))))) {
        // do nothing
      } else if (OB_FAIL(ObBasicSysVar::get_collation_var_and_val_by_charset(
                     set_var.var_name_, cs_str, extra_var_name, extra_val, extra_coll_type))) {
        LOG_ERROR(
            "fail to get collation variable and value by charset", K(ret), K(set_var.var_name_), K(val), K(cs_str));
      } else if (OB_FAIL(databuff_printf(extra_var_value_buf,
                     sizeof(extra_var_value_buf),
                     pos,
                     "%d",
                     static_cast<int32_t>(extra_coll_type)))) {
        LOG_WARN("databuff printf failed", K(extra_coll_type), K(ret));
      } else {
        extra_var_value.assign(extra_var_value_buf, pos);
        should_update_extra_var = true;
      }
    } else if (set_var.var_name_ == OB_SV_NLS_DATE_FORMAT || set_var.var_name_ == OB_SV_NLS_TIMESTAMP_FORMAT ||
               set_var.var_name_ == OB_SV_NLS_TIMESTAMP_TZ_FORMAT) {
      ObString format;
      if (OB_UNLIKELY(val.is_null_oracle())) {
        ret = OB_INVALID_DATE_FORMAT;
        LOG_WARN("date format not recognized", K(ret), K(set_var.var_name_), K(val));
      } else if (OB_FAIL(val.get_varchar(format))) {
        LOG_WARN("fail get varchar", K(val), K(ret));
      } else {
        int64_t nls_enum = ObNLSFormatEnum::NLS_DATE;
        ObDTMode mode = DT_TYPE_DATETIME;
        if (set_var.var_name_ == OB_SV_NLS_TIMESTAMP_FORMAT) {
          mode |= DT_TYPE_ORACLE;
          nls_enum = ObNLSFormatEnum::NLS_TIMESTAMP;
        } else if (set_var.var_name_ == OB_SV_NLS_TIMESTAMP_TZ_FORMAT) {
          mode |= DT_TYPE_ORACLE;
          mode |= DT_TYPE_TIMEZONE;
          nls_enum = ObNLSFormatEnum::NLS_TIMESTAMP_TZ;
        }
        ObSEArray<ObDFMElem, ObDFMUtil::COMMON_ELEMENT_NUMBER> dfm_elems;
        ObBitSet<ObDFMFlag::MAX_FLAG_NUMBER> elem_flags;
        // 1. parse and check semantic of format string
        if (OB_FAIL(ObDFMUtil::parse_datetime_format_string(format, dfm_elems))) {
          LOG_WARN("fail to parse oracle datetime format string", K(ret), K(format));
        } else if (OB_FAIL(ObDFMUtil::check_semantic(dfm_elems, elem_flags, mode))) {
          LOG_WARN("check semantic of format string failed", K(ret), K(format));
        }
      }
    } else if (set_var.var_name_ == OB_SV_LOG_LEVEL) {
      ObString log_level;
      if (OB_FAIL(val.get_varchar(log_level))) {
        LOG_WARN("fail get varchar", K(val), K(ret));
      } else if (0 == log_level.case_compare("disabled")) {
        // allowed for variables
      } else if (OB_FAIL(OB_LOGGER.parse_check(log_level.ptr(), log_level.length()))) {
        LOG_WARN("Log level parse check error", K(log_level), K(ret));
      }
    } else if (set_var.var_name_ == OB_SV_TRANSACTION_ISOLATION) {
      extra_var_name = ObString::make_string(OB_SV_TX_ISOLATION);
      should_update_extra_var = true;
      if (OB_FAIL(val.get_varchar(extra_var_value))) {
        LOG_WARN("fail get varchar", K(val), K(ret));
      }
    } else if (set_var.var_name_ == OB_SV_TX_ISOLATION) {
      extra_var_name = ObString::make_string(OB_SV_TRANSACTION_ISOLATION);
      should_update_extra_var = true;
      if (OB_FAIL(val.get_varchar(extra_var_value))) {
        LOG_WARN("fail get varchar", K(val), K(ret));
      }
    } else if (set_var.var_name_ == OB_SV_TX_READ_ONLY) {
      int64_t extra_var_values = -1;
      extra_var_name = ObString::make_string(OB_SV_TRANSACTION_READ_ONLY);
      if (OB_FAIL(val.get_int(extra_var_values))) {
        LOG_WARN("fail get int", K(val), K(ret));
      } else if (OB_FAIL(databuff_printf(extra_var_value_buf,
                     sizeof(extra_var_value_buf),
                     pos,
                     "%d",
                     static_cast<int32_t>(extra_var_values)))) {
        LOG_WARN("databuff printf failed", K(extra_var_values), K(ret));
      } else {
        extra_var_value.assign(extra_var_value_buf, pos);
        should_update_extra_var = true;
      }
    } else if (set_var.var_name_ == OB_SV_TRANSACTION_READ_ONLY) {
      extra_var_name = ObString::make_string(OB_SV_TX_READ_ONLY);
      int64_t extra_var_values = -1;
      if (OB_FAIL(val.get_int(extra_var_values))) {
        LOG_WARN("fail get int", K(val), K(ret));
      } else if (OB_FAIL(databuff_printf(extra_var_value_buf,
                     sizeof(extra_var_value_buf),
                     pos,
                     "%d",
                     static_cast<int32_t>(extra_var_values)))) {
        LOG_WARN("databuff printf failed", K(extra_var_values), K(ret));
      } else {
        extra_var_value.assign(extra_var_value_buf, pos);
        should_update_extra_var = true;
      }
    }

    if (OB_SUCC(ret) && should_update_extra_var) {
      ObSysVarSchema sysvar_schema;
      if (OB_FAIL(sysvar_schema.set_name(extra_var_name))) {
        LOG_WARN("set sysvar schema name failed", K(ret));
      } else if (OB_FAIL(sysvar_schema.set_value(extra_var_value))) {
        LOG_WARN("set sysvar schema value failed", K(ret));
      } else {
        sysvar_schema.set_tenant_id(arg.tenant_id_);
        if (OB_FAIL(arg.sys_var_list_.push_back(sysvar_schema))) {
          LOG_WARN("store sys var to array failed", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObExprCtx expr_ctx;
    expr_ctx.exec_ctx_ = &ctx;
    expr_ctx.calc_buf_ = &set_var.calc_buf_;
    expr_ctx.my_session_ = ctx.get_my_session();
    EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
    EXPR_GET_VARCHAR_V2(val, val_str);
    ObSysVarSchema sysvar_schema;
    if (OB_UNLIKELY(val_str.length() > OB_MAX_SYS_VAR_VAL_LENGTH)) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("set sysvar value is overflow",
          "max length",
          OB_MAX_SYS_VAR_VAL_LENGTH,
          "value length",
          val_str.length(),
          "name",
          set_var.var_name_,
          "value",
          val_str);
    } else if (OB_FAIL(sysvar_schema.set_name(set_var.var_name_))) {
      LOG_WARN("set sysvar schema name failed", K(ret));
    } else if (OB_FAIL(sysvar_schema.set_value(val_str))) {
      LOG_WARN("set sysvar schema value failed", K(ret));
    } else {
      sysvar_schema.set_tenant_id(arg.tenant_id_);
      if (OB_FAIL(arg.sys_var_list_.push_back(sysvar_schema))) {
        LOG_WARN("store sys var to array failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx)) ||
        OB_ISNULL(common_rpc_proxy = task_exec_ctx->get_common_rpc())) {
      ret = OB_NOT_INIT;
      LOG_WARN("task exec ctx or common rpc proxy is NULL", K(ret), K(task_exec_ctx), K(common_rpc_proxy));
    } else if (OB_FAIL(common_rpc_proxy->modify_system_variable(arg))) {
      LOG_WARN("rpc proxy alter system variable failed", K(ret));
    } else {
    }
  }
  return ret;
}

int ObVariableSetExecutor::check_and_convert_sys_var(ObExecContext& ctx, const ObSetVar& set_var,
    ObBasicSysVar& sys_var, const ObObj& in_val, ObObj& out_val, bool is_set_stmt)
{
  int ret = OB_SUCCESS;
  // OB_ASSERT(true == var_node.is_system_variable_);

  // can't set collation_connection to utf16
  if (OB_SUCC(ret)) {
    if ((0 == set_var.var_name_.case_compare(OB_SV_CHARACTER_SET_CLIENT) ||
            0 == set_var.var_name_.case_compare(OB_SV_CHARACTER_SET_CONNECTION) ||
            0 == set_var.var_name_.case_compare(OB_SV_CHARACTER_SET_RESULTS) ||
            0 == set_var.var_name_.case_compare(OB_SV_COLLATION_CONNECTION)) &&
        (in_val.get_string().prefix_match_ci("utf16"))) {
      ret = OB_ERR_WRONG_VALUE_FOR_VAR;
      LOG_USER_ERROR(OB_ERR_WRONG_VALUE_FOR_VAR,
          set_var.var_name_.length(),
          set_var.var_name_.ptr(),
          in_val.get_string().length(),
          in_val.get_string().ptr());
    }
  }

  // check readonly
  if (is_set_stmt && sys_var.is_readonly()) {
    if (sys_var.is_with_upgrade() && GCONF.in_upgrade_mode()) {
      // do nothing ...
    } else {
      ret = OB_ERR_INCORRECT_GLOBAL_LOCAL_VAR;
      LOG_USER_ERROR(OB_ERR_INCORRECT_GLOBAL_LOCAL_VAR,
          set_var.var_name_.length(),
          set_var.var_name_.ptr(),
          (int)strlen("read only"),
          "read only");
    }
  }

  // check scope
  if (OB_FAIL(ret)) {
  } else if (ObSetVar::SET_SCOPE_GLOBAL == set_var.set_scope_ && !sys_var.is_global_scope()) {
    ret = OB_ERR_LOCAL_VARIABLE;
    LOG_USER_ERROR(OB_ERR_LOCAL_VARIABLE, set_var.var_name_.length(), set_var.var_name_.ptr());
  } else if (ObSetVar::SET_SCOPE_SESSION == set_var.set_scope_ && !sys_var.is_session_scope()) {
    ret = OB_ERR_GLOBAL_VARIABLE;
    LOG_USER_ERROR(OB_ERR_GLOBAL_VARIABLE, set_var.var_name_.length(), set_var.var_name_.ptr());
  }

  // check update type and value
  if (OB_FAIL(ret)) {
  } else if (OB_SUCCESS != (ret = sys_var.check_update_type(set_var, in_val))) {
    if (OB_ERR_WRONG_TYPE_FOR_VAR == ret) {
      LOG_USER_ERROR(OB_ERR_WRONG_TYPE_FOR_VAR, set_var.var_name_.length(), set_var.var_name_.ptr());
    } else {
      LOG_WARN("fail to check update type", K(ret));
    }
  } else if (OB_FAIL(sys_var.check_and_convert(ctx, set_var, in_val, out_val))) {
    if (OB_ERR_WRONG_TYPE_FOR_VAR == ret) {
      LOG_USER_ERROR(OB_ERR_WRONG_TYPE_FOR_VAR, set_var.var_name_.length(), set_var.var_name_.ptr());
    } else {
      LOG_WARN("fail to check value", K(ret));
    }
  }

  // do not support modify now
  if (OB_FAIL(ret)) {
  } else if (share::is_oracle_mode() && set_var.var_name_.prefix_match("nls_")) {
    static const common::ObString DEFAULT_VALUE_LANGUAGE("AMERICAN");
    static const common::ObString DEFAULT_VALUE_TERRITORY("AMERICA");
    static const common::ObString DEFAULT_VALUE_SORT("BINARY");
    static const common::ObString DEFAULT_VALUE_COMP("BINARY");
    static const common::ObString DEFAULT_VALUE_NCHAR_CHARACTERSET("AL16UTF16");
    static const common::ObString DEFAULT_VALUE_DATE_LANGUAGE("AMERICAN");
    static const common::ObString DEFAULT_VALUE_NCHAR_CONV_EXCP("FALSE");
    static const common::ObString DEFAULT_VALUE_CALENDAR("GREGORIAN");
    static const common::ObString DEFAULT_VALUE_NUMERIC_CHARACTERS(".,");
    static const common::ObString DEFAULT_VALUE_CURRENCY("$");
    static const common::ObString DEFAULT_VALUE_ISO_CURRENCY("AMERICA");
    static const common::ObString DEFAULT_VALUE_DUAL_CURRENCY("$");

    const ObString new_value = out_val.get_string();
    if ((set_var.var_name_ == OB_SV_NLS_LANGUAGE && new_value.case_compare(DEFAULT_VALUE_LANGUAGE) != 0) ||
        (set_var.var_name_ == OB_SV_NLS_TERRITORY && new_value.case_compare(DEFAULT_VALUE_TERRITORY) != 0) ||
        (set_var.var_name_ == OB_SV_NLS_SORT && new_value.case_compare(DEFAULT_VALUE_SORT) != 0) ||
        (set_var.var_name_ == OB_SV_NLS_COMP && new_value.case_compare(DEFAULT_VALUE_COMP) != 0) ||
        (set_var.var_name_ == OB_SV_NLS_CHARACTERSET) ||
        (set_var.var_name_ == OB_SV_NLS_NCHAR_CHARACTERSET &&
            new_value.case_compare(DEFAULT_VALUE_NCHAR_CHARACTERSET) != 0) ||
        (set_var.var_name_ == OB_SV_NLS_DATE_LANGUAGE && new_value.case_compare(DEFAULT_VALUE_DATE_LANGUAGE) != 0) ||
        (set_var.var_name_ == OB_SV_NLS_NCHAR_CONV_EXCP &&
            new_value.case_compare(DEFAULT_VALUE_NCHAR_CONV_EXCP) != 0) ||
        (set_var.var_name_ == OB_SV_NLS_CALENDAR && new_value.case_compare(DEFAULT_VALUE_CALENDAR) != 0) ||
        (set_var.var_name_ == OB_SV_NLS_NUMERIC_CHARACTERS &&
            new_value.case_compare(DEFAULT_VALUE_NUMERIC_CHARACTERS) != 0) ||
        (set_var.var_name_ == OB_SV_NLS_CURRENCY && new_value.case_compare(DEFAULT_VALUE_CURRENCY) != 0) ||
        (set_var.var_name_ == OB_SV_NLS_ISO_CURRENCY && new_value.case_compare(DEFAULT_VALUE_ISO_CURRENCY) != 0) ||
        (set_var.var_name_ == OB_SV_NLS_DUAL_CURRENCY && new_value.case_compare(DEFAULT_VALUE_DUAL_CURRENCY) != 0)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not support modify this variables now", K(set_var), K(new_value), K(ret));
    }
  }
  return ret;
}

int ObVariableSetExecutor::cast_value(ObExecContext& ctx, const ObVariableSetStmt::VariableSetNode& var_node,
    uint64_t actual_tenant_id, ObIAllocator& calc_buf, const ObBasicSysVar& sys_var, const ObObj& in_val,
    ObObj& out_val)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service_ is null");
  } else if (OB_ISNULL(ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("my session is null");
  } else if (var_node.is_set_default_) {
    uint64_t tenant_id = actual_tenant_id;
    if (ObSetVar::SET_SCOPE_SESSION == var_node.set_scope_) {
      ObSchemaGetterGuard schema_guard;
      const ObSysVarSchema* var_schema = NULL;
      const ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params(ctx.get_my_session());
      if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
        LOG_WARN("get schema guard failed", K(ret));
      } else if (OB_FAIL(schema_guard.get_tenant_system_variable(tenant_id, var_node.variable_name_, var_schema))) {
        LOG_WARN("get tenant system variable failed", K(ret), K(tenant_id), K(var_node.variable_name_));
      } else if (OB_FAIL(var_schema->get_value(&calc_buf, dtc_params, out_val))) {
        LOG_WARN("get value from sysvar schema failed", K(ret));
      }
    } else if (ObSetVar::SET_SCOPE_GLOBAL == var_node.set_scope_) {
      const ObObj& def_val = sys_var.get_global_default_value();
      DEFINE_CAST_CTX();
      if (OB_FAIL(ObObjCaster::to_type(sys_var.get_data_type(), cast_ctx, def_val, out_val))) {
        LOG_ERROR("failed to cast object", K(ret), K(var_node.variable_name_), K(def_val), K(sys_var.get_data_type()));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid set scope", K(ret), K(var_node.set_scope_));
    }
  } else if (ObNullType == in_val.get_type()) {
    out_val = in_val;
  } else {
    DEFINE_CAST_CTX();
    if (OB_FAIL(ObObjCaster::to_type(sys_var.get_data_type(), cast_ctx, in_val, out_val))) {
      LOG_WARN("failed to cast object", K(ret), K(var_node.variable_name_), K(in_val), K(sys_var.get_data_type()));
    } else {
    }
  }
  return ret;
}

int ObVariableSetExecutor::process_session_autocommit_hook(ObExecContext& exec_ctx, const ObObj& val)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo* my_session = GET_MY_SESSION(exec_ctx);
  if (OB_ISNULL(my_session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else {
    int64_t autocommit = 0;
    bool in_trans = my_session->get_in_transaction();
    bool ac = true;

    if (OB_FAIL(my_session->get_autocommit(ac))) {
      LOG_WARN("fail to get autocommit", K(ret));
    } else if (OB_FAIL(val.get_int(autocommit))) {
      LOG_WARN("fail get commit val", K(val), K(ret));
    } else if (0 != autocommit && 1 != autocommit) {
      const char* autocommit_str = to_cstring(autocommit);
      ret = OB_ERR_WRONG_VALUE_FOR_VAR;
      LOG_USER_ERROR(OB_ERR_WRONG_VALUE_FOR_VAR,
          (int)strlen(OB_SV_AUTOCOMMIT),
          OB_SV_AUTOCOMMIT,
          (int)strlen(autocommit_str),
          autocommit_str);
    } else {
      if (false == ac && true == in_trans && 1 == autocommit) {
        ObEndTransSyncCallback callback;
        const bool is_rollback = false;
        if (OB_FAIL(callback.init(&(my_session->get_trans_desc()), my_session))) {
          LOG_WARN("fail init callback", K(ret));
        } else {
          int wait_ret = OB_SUCCESS;
          if (OB_FAIL(ObSqlTransControl::implicit_end_trans(
                  exec_ctx, is_rollback, callback))) {  // implicit commit, no rollback
            LOG_WARN("fail end implicit trans", K(is_rollback), K(ret));
          }
          if (OB_UNLIKELY(OB_SUCCESS != (wait_ret = callback.wait()))) {
            LOG_WARN("sync end trans callback return an error!",
                K(ret),
                K(wait_ret),
                K(is_rollback),
                K(my_session->get_trans_desc()));
          }
          ret = OB_SUCCESS != ret ? ret : wait_ret;
        }
      } else {
      }
    }
  }
  return ret;
}

int ObVariableSetExecutor::process_auto_increment_hook(const ObSQLMode sql_mode, const ObString var_name, ObObj& val)
{
  int ret = OB_SUCCESS;
  uint64_t auto_increment = 0;
  if (OB_FAIL(val.get_uint64(auto_increment))) {
    LOG_WARN("fail get auto_increment value", K(ret), K(val));
  } else {
    if (SMO_STRICT_ALL_TABLES & sql_mode) {
      if (auto_increment <= 0 || auto_increment > UINT16_MAX) {
        char auto_increment_str[OB_CAST_TO_VARCHAR_MAX_LENGTH];
        int length = snprintf(auto_increment_str, OB_CAST_TO_VARCHAR_MAX_LENGTH, "%lu", auto_increment);
        if (length < 0 || length >= OB_CAST_TO_VARCHAR_MAX_LENGTH) {
          length = OB_CAST_TO_VARCHAR_MAX_LENGTH - 1;
          auto_increment_str[length] = '\0';
        }
        ret = OB_ERR_WRONG_VALUE_FOR_VAR;
        LOG_USER_ERROR(OB_ERR_WRONG_VALUE_FOR_VAR, var_name.length(), var_name.ptr(), length, auto_increment_str);
      }
    } else {
      if (auto_increment <= 0) {
        auto_increment = 1;
        // should generate warning message
        // SQL_ENG_LOG(WARN, "Truncated incorrect value");
      } else if (auto_increment > UINT16_MAX) {
        auto_increment = UINT16_MAX;
        // should generate warning message
        // SQL_ENG_LOG(WARN, "Truncated incorrect value");
      }
      val.set_uint64(auto_increment);
    }
  }
  return ret;
}

int ObVariableSetExecutor::process_last_insert_id_hook(
    ObPhysicalPlanCtx* plan_ctx, const ObSQLMode sql_mode, const ObString var_name, ObObj& val)
{
  int ret = OB_SUCCESS;
  int64_t value = 0;
  uint64_t unsigned_value = 0;
  if (OB_ISNULL(plan_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("plan ctx is NULL", K(ret));
  } else if (OB_FAIL(val.get_int(value))) {
    if (OB_FAIL(val.get_uint64(unsigned_value))) {
      ret = OB_ERR_WRONG_TYPE_FOR_VAR;
      LOG_WARN("failed to get value", K(val), K(ret));
    }
  } else {
    if (SMO_STRICT_ALL_TABLES & sql_mode) {
      if (value < 0) {
        char auto_increment_str[OB_CAST_TO_VARCHAR_MAX_LENGTH];
        int32_t length = snprintf(auto_increment_str, OB_CAST_TO_VARCHAR_MAX_LENGTH, "%ld", value);
        if (length < 0 || length >= OB_CAST_TO_VARCHAR_MAX_LENGTH) {
          length = OB_CAST_TO_VARCHAR_MAX_LENGTH - 1;
          auto_increment_str[length] = '\0';
        }
        ret = OB_ERR_WRONG_VALUE_FOR_VAR;
        LOG_USER_ERROR(OB_ERR_WRONG_VALUE_FOR_VAR, var_name.length(), var_name.ptr(), length, auto_increment_str);
      }
    } else {
      if (value < 0) {
        value = 0;
        // should generate warning message
        // SQL_ENG_LOG(WARN, "Truncated incorrect value");
      }
      val.set_int(value);
    }
  }
  if (OB_SUCC(ret)) {
    if (0 != unsigned_value) {
      plan_ctx->set_last_insert_id_session(unsigned_value);
    } else {
      plan_ctx->set_last_insert_id_session(static_cast<uint64_t>(value));
    }
  }
  return ret;
}

int ObVariableSetExecutor::switch_to_session_variable(
    const ObExprCtx& expr_ctx, const ObObj& value, ObSessionVariable& sess_var)
{
  int ret = OB_SUCCESS;
  if (ob_is_temporal_type(value.get_type())) {  // switch the meta type and value type
    EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
    ObObj obj_tmp;
    const ObObj* res_obj_ptr = NULL;
    if (OB_FAIL(ObObjCaster::to_type(ObVarcharType, cast_ctx, value, obj_tmp, res_obj_ptr))) {
      LOG_WARN("failed to cast object to ObVarcharType ", K(ret), K(value));
    } else if (OB_ISNULL(res_obj_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("res_obj_ptr is NULL", K(ret));
    } else {
      sess_var.value_.set_varchar(res_obj_ptr->get_varchar());
      sess_var.meta_.set_collation_level(CS_LEVEL_IMPLICIT);
      sess_var.meta_.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
      sess_var.meta_.set_varchar();
    }
  } else if (ObNullType == value.get_type()) {  // switch the meta type only
    sess_var.value_.set_null();
    sess_var.meta_.set_collation_level(CS_LEVEL_IMPLICIT);
    sess_var.meta_.set_collation_type(CS_TYPE_BINARY);
  } else {  // won't switch
    sess_var.value_ = value;
    sess_var.meta_.set_type(value.get_type());
    sess_var.meta_.set_scale(value.get_scale());
    sess_var.meta_.set_collation_level(CS_LEVEL_IMPLICIT);
    sess_var.meta_.set_collation_type(value.get_collation_type());
  }
  return ret;
}

#undef DEFINE_CAST_CTX

}  // namespace sql
}  // namespace oceanbase
