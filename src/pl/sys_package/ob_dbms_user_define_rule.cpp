/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX PL

#include "pl/ob_pl_package_manager.h"
#include "share/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/oblog/ob_log_module.h"
#include "sql/executor/ob_task_executor_ctx.h"
#include "sql/parser/ob_parser.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/udr/ob_udr_analyzer.h"
#include "sql/udr/ob_udr_mgr.h"
#include "observer/ob_server_struct.h"
#include "pl/sys_package/ob_dbms_user_define_rule.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"

namespace oceanbase
{
using namespace obrpc;
namespace pl
{

#define DEF_UDR_FUNC_IMPL(func_name, Processor)                     \
  int ObDBMSUserDefineRule::func_name(sql::ObExecContext &ctx,      \
                                      sql::ParamStore &params,      \
                                      common::ObObj &result)        \
  {                                                                 \
    int ret = OB_SUCCESS;                                           \
    Processor processor(ctx, params, result);                       \
    if (OB_FAIL(processor.init())) {                                \
      LOG_WARN("processor init failed", K(ret));                    \
    } else if (OB_FAIL(processor.process())) {                      \
      LOG_WARN("failed to process", K(ret));                        \
    }                                                               \
    return ret;                                                     \
  }

int ObUDRProcessor::init()
{
  int ret = OB_SUCCESS;
  is_inited_ = true;
  return ret;
}

int ObUDRProcessor::pre_execution_check()
{
  int ret = OB_SUCCESS;
  if (arg_.rule_name_.empty()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "empty rule name");
    LOG_WARN("empty rule name not supported", K(ret));
  } else if (arg_.rule_name_.length() > OB_MAX_ORIGINAL_NANE_LENGTH) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "rule name length exceeding 256 is");
    LOG_WARN("rule name length exceeding 256 is not supported", K(ret));
  }
  return ret;
}

int ObUDRProcessor::sync_rule_from_inner_table()
{
  int ret = OB_SUCCESS;
  ObSyncRewriteRuleArg sync_udr_arg;
  sql::ObBasicSessionInfo *session = NULL;
  sql::ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc = NULL;
  if (OB_ISNULL(session = ctx_.get_my_session()) ||
      OB_ISNULL(task_exec_ctx = ctx_.get_task_executor_ctx()) ||
      OB_ISNULL(common_rpc = task_exec_ctx->get_common_rpc())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(task_exec_ctx), K(common_rpc));
  } else {
    sync_udr_arg.tenant_id_ = session->get_effective_tenant_id();
    if (OB_FAIL(common_rpc->admin_sync_rewrite_rules(sync_udr_arg))) {
      LOG_WARN("sync rewrite rules rpc failed ", K(ret), "rpc_arg", sync_udr_arg);
    }
  }
  return ret;
}

int ObUDRProcessor::process()
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  sql::ObSQLSessionInfo *session = ctx_.get_my_session();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("processor should be inited first", K(ret));
  } else if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(session->get_effective_tenant_id(), data_version))) {
    LOG_WARN("failed to get min data version", K(ret));
  } else if (data_version < DATA_VERSION_4_1_0_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "user-defined_rules with minimal data version < 4100");
    LOG_WARN("user-defined_rules with minimal data version < 4100", K(ret));
  } else if (OB_FAIL(parse_request_param())) {
    LOG_WARN("failed to parse request param", K(ret));
  } else if (OB_FAIL(generate_exec_arg())) {
    LOG_WARN("failed to generate exec arg", K(ret));
  } else if (OB_FAIL(pre_execution_check())) {
    LOG_WARN("failed to pre execution check", K(ret));
  } else if (OB_FAIL(execute())) {
    LOG_WARN("failed to execute rpc", K(ret));
  } else if (OB_FAIL(sync_rule_from_inner_table())) {
    LOG_WARN("failed to sync rule from inner table", K(ret));
  }
  return ret;
}

/**
 * PROCEDURE CREATE_RULE (
 *  rule_name          VARCHAR(256),
 *  database_name      VARCHAR(128),
 *  pattern            LONGTEXT,
 *  replacement        LONGTEXT,
 *  enabled            IN VARCHAR  := 'YES'
 *);
 */
int ObCreateRuleProcessor::parse_request_param()
{
  int ret = OB_SUCCESS;
  ObString enabled;
  sql::ObSQLSessionInfo *session = ctx_.get_my_session();
  LOG_DEBUG("parse request param", K(params_));
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if ((lib::is_oracle_mode() && 4 != params_.count()) ||
            (!lib::is_oracle_mode() && 5 != params_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid params", K(params_.count()));
  } else if (lib::is_oracle_mode()) {
    OV (params_.at(0).is_varchar(), OB_INVALID_ARGUMENT);
    OZ (params_.at(0).get_string(arg_.rule_name_));
    OV (params_.at(1).is_lob(), OB_INVALID_ARGUMENT);
    OZ (sql::ObTextStringHelper::read_real_string_data(&ctx_.get_allocator(), params_.at(1), arg_.pattern_));
    OV (params_.at(2).is_lob(), OB_INVALID_ARGUMENT);
    OZ (sql::ObTextStringHelper::read_real_string_data(&ctx_.get_allocator(), params_.at(2), arg_.replacement_));
    OV (params_.at(3).is_varchar(), OB_INVALID_ARGUMENT);
    OZ (params_.at(3).get_string(enabled));
    OX (arg_.db_name_ = session->get_database_name());
  } else {
    OV (params_.at(0).is_varchar(), OB_INVALID_ARGUMENT);
    OZ (params_.at(0).get_string(arg_.rule_name_));
    OV (params_.at(1).is_varchar(), OB_INVALID_ARGUMENT);
    OZ (params_.at(1).get_string(arg_.db_name_));
    OV (params_.at(2).is_text(), OB_INVALID_ARGUMENT);
    OZ (sql::ObTextStringHelper::read_real_string_data(&ctx_.get_allocator(), params_.at(2), arg_.pattern_));
    OV (params_.at(3).is_text(), OB_INVALID_ARGUMENT);
    OZ (sql::ObTextStringHelper::read_real_string_data(&ctx_.get_allocator(), params_.at(3), arg_.replacement_));
    OV (params_.at(4).is_varchar(), OB_INVALID_ARGUMENT);
    OZ (params_.at(4).get_string(enabled));
  }
  if (OB_SUCC(ret)) {
    arg_.tenant_id_ = session->get_effective_tenant_id();
    arg_.coll_type_ = session->get_local_collation_connection();
    if (0 == enabled.case_compare("yes")) {
      arg_.rule_status_ = ObUDRInfo::ENABLE_STATUS;
    } else if (0 == enabled.case_compare("no")) {
      arg_.rule_status_ = ObUDRInfo::DISABLE_STATUS;
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid fixed param", K(enabled), K(ret));
    }
  }
  return ret;
}

int ObCreateRuleProcessor::generate_exec_arg()
{
#define ISSPACE(c) ((c) == ' ' || (c) == '\n' || (c) == '\r' || (c) == '\t' || (c) == '\f' || (c) == '\v')
  int ret = OB_SUCCESS;
  sql::ObSQLSessionInfo *session = ctx_.get_my_session();
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else {
    ObUDRAnalyzer analyzer(ctx_.get_allocator(),
                           session->get_sql_mode(),
                           session->get_charsets4parser());
    if (OB_FAIL(analyzer.parse_and_check(arg_.pattern_, arg_.replacement_))) {
      LOG_WARN("failed to parse and check", K(ret));
    } else if (OB_FAIL(analyzer.parse_pattern_to_gen_param_infos_str(
                       arg_.pattern_,
                       arg_.normalized_pattern_,
                       arg_.fixed_param_infos_str_,
                       arg_.dynamic_param_infos_str_,
                       arg_.question_mark_ctx_str_))) {
      LOG_WARN("failed to parse to gen param infos", K(ret), K(arg_.pattern_));
    } else {
      const ObString stmt(arg_.normalized_pattern_.length(), arg_.normalized_pattern_.ptr());
      int64_t len = stmt.length();
      while (len > 0 && ISSPACE(stmt[len - 1])) {
        --len;
      }
      if (';' == stmt[len - 1]) {
        --len;
      }
      arg_.normalized_pattern_.assign_ptr(stmt.ptr(), len);
      arg_.pattern_digest_ = arg_.normalized_pattern_.hash();
    }
  }
  if (OB_SUCC(ret)) {
    LOG_DEBUG("succ generate exec arg", K(arg_));
  }
  return ret;
}

int ObCreateRuleProcessor::execute()
{
  int ret = OB_SUCCESS;
  sql::ObUDRMgr *rule_mgr = MTL(sql::ObUDRMgr*);
  if (OB_FAIL(rule_mgr->insert_rule(arg_))) {
    LOG_WARN("failed to insert rewrite rule", K(ret), K(arg_));
  }
  return ret;
}

/**
 * PROCEDURE REMOVE_RULE (
 *  rule_name          VARCHAR(256)
 *);
 */
int ObRemoveRuleProcessor::parse_request_param()
{
  int ret = OB_SUCCESS;
  bool is_enabled = false;
  LOG_DEBUG("parse request param", K(params_));
  sql::ObSQLSessionInfo *session = ctx_.get_my_session();
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (1 != params_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid params", K(params_.count()));
  } else {
    OV (params_.at(0).is_varchar(), OB_INVALID_ARGUMENT);
    OZ (params_.at(0).get_string(arg_.rule_name_));
  }
  if (OB_SUCC(ret)) {
    arg_.tenant_id_ = session->get_effective_tenant_id();
  }
  return ret;
}

int ObRemoveRuleProcessor::generate_exec_arg()
{
  int ret = OB_SUCCESS;
  arg_.rule_status_ = ObUDRInfo::DELETE_STATUS;
  LOG_DEBUG("succ generate exec arg", K(arg_));
  return ret;
}

int ObRemoveRuleProcessor::execute()
{
  int ret = OB_SUCCESS;
  sql::ObUDRMgr *rule_mgr = MTL(sql::ObUDRMgr*);
  if (OB_FAIL(rule_mgr->remove_rule(arg_))) {
    LOG_WARN("failed to remove rewrite rule", K(ret), K(arg_));
  }
  return ret;
}

/**
 * PROCEDURE ENABLE_RULE (
 *  rule_name          VARCHAR(256)
 *);
 */
int ObEnableRuleProcessor::parse_request_param()
{
  int ret = OB_SUCCESS;
  bool is_enabled = false;
  LOG_DEBUG("parse request param", K(params_));
  sql::ObSQLSessionInfo *session = ctx_.get_my_session();
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (1 != params_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid params", K(params_.count()));
  } else {
    OV (params_.at(0).is_varchar(), OB_INVALID_ARGUMENT);
    OZ (params_.at(0).get_string(arg_.rule_name_));
  }
  if (OB_SUCC(ret)) {
    arg_.tenant_id_ = session->get_effective_tenant_id();
  }
  return ret;
}

int ObEnableRuleProcessor::generate_exec_arg()
{
  int ret = OB_SUCCESS;
  arg_.rule_status_ = ObUDRInfo::ENABLE_STATUS;
  LOG_DEBUG("succ generate exec arg", K(arg_));
  return ret;
}

int ObEnableRuleProcessor::execute()
{
  int ret = OB_SUCCESS;
  sql::ObUDRMgr *rule_mgr = MTL(sql::ObUDRMgr*);
  if (OB_FAIL(rule_mgr->alter_rule_status(arg_))) {
    LOG_WARN("failed to alter rewrite rule status", K(ret), K(arg_));
  }
  return ret;
}

/**
 * PROCEDURE DISABLED_RULE (
 *  rule_name          VARCHAR(256)
 *);
 */
int ObDisableRuleProcessor::parse_request_param()
{
  int ret = OB_SUCCESS;
  bool is_enabled = false;
  LOG_DEBUG("parse request param", K(params_));
  sql::ObSQLSessionInfo *session = ctx_.get_my_session();
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (1 != params_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid params", K(params_.count()));
  } else {
    OV (params_.at(0).is_varchar(), OB_INVALID_ARGUMENT);
    OZ (params_.at(0).get_string(arg_.rule_name_));
  }
  if (OB_SUCC(ret)) {
    arg_.tenant_id_ = session->get_effective_tenant_id();
  }
  return ret;
}

int ObDisableRuleProcessor::generate_exec_arg()
{
  int ret = OB_SUCCESS;
  arg_.rule_status_ = ObUDRInfo::DISABLE_STATUS;
  LOG_DEBUG("succ generate exec arg", K(arg_));
  return ret;
}

int ObDisableRuleProcessor::execute()
{
  int ret = OB_SUCCESS;
  sql::ObUDRMgr *rule_mgr = MTL(sql::ObUDRMgr*);
  if (OB_FAIL(rule_mgr->alter_rule_status(arg_))) {
    LOG_WARN("failed to alter rewrite rule status", K(ret), K(arg_));
  }
  return ret;
}

DEF_UDR_FUNC_IMPL(create_rule, ObCreateRuleProcessor);
DEF_UDR_FUNC_IMPL(remove_rule, ObRemoveRuleProcessor);
DEF_UDR_FUNC_IMPL(enable_rule, ObEnableRuleProcessor);
DEF_UDR_FUNC_IMPL(disable_rule, ObDisableRuleProcessor);

} // end of pl
} // end of oceanbase
