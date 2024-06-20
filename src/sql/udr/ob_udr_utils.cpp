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

#define USING_LOG_PREFIX SQL_QRR
#include "sql/ob_sql.h"
#include "share/config/ob_server_config.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/ob_sql_utils.h"
#include "sql/udr/ob_udr_analyzer.h"
#include "sql/udr/ob_udr_mgr.h"
#include "sql/udr/ob_udr_utils.h"

namespace oceanbase
{
namespace sql
{

int ObUDRUtils::match_udr_item(const ObString &pattern,
                               const ObSQLSessionInfo &session_info,
                               ObExecContext &ectx,
                               ObIAllocator &allocator,
                               ObUDRItemMgr::UDRItemRefGuard &guard,
                               PatternConstConsList *cst_cons_list)
{
  int ret = OB_SUCCESS;
  ObUDRContext rule_ctx;
  sql::ObUDRMgr *rule_mgr = MTL(sql::ObUDRMgr*);
  ObUDRAnalyzer analyzer(allocator,
                        session_info.get_sql_mode(),
                        session_info.get_charsets4parser());
  if (OB_FAIL(analyzer.parse_sql_to_gen_match_param_infos(pattern,
                                                          rule_ctx.normalized_pattern_,
                                                          rule_ctx.raw_param_list_))) {
    LOG_WARN("failed to parse to gen param infos", K(ret), K(pattern));
  } else {
    rule_ctx.pattern_digest_ = rule_ctx.normalized_pattern_.hash();
    rule_ctx.tenant_id_ = session_info.get_effective_tenant_id();
    rule_ctx.db_name_ = session_info.get_database_name();
    rule_ctx.coll_type_ = session_info.get_local_collation_connection();
    if (OB_FAIL(rule_mgr->get_udr_item(rule_ctx, guard, cst_cons_list))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to match rewrite rule item", K(ret));
      }
    } else if (guard.is_valid()) {
      LOG_TRACE("succ to match rewrite rule item", KPC(guard.get_ref_obj()));
    }
  }
  if (OB_FAIL(ret) && !ObSQLUtils::check_need_disconnect_parser_err(ret)) {
    ectx.set_need_disconnect(false);
  }
  return ret;
}

int ObUDRUtils::cons_udr_const_cons_list(const PatternConstConsList &cst_const_list,
                                         ObPlanCacheCtx &pc_ctx)
{
  int ret = OB_SUCCESS;
  OX (pc_ctx.tpl_sql_const_cons_.set_capacity(cst_const_list.count()));
  for (int64_t i = 0; OB_SUCC(ret) && i < cst_const_list.count(); ++i) {
    const FixedParamValueArray &fixed_param_list =  cst_const_list.at(i);
    NotParamInfoList not_param_info_list;
    for (int64_t j = 0; OB_SUCC(ret) && j < fixed_param_list.count(); ++j) {
      const FixedParamValue &fixed_param = fixed_param_list.at(j);
      NotParamInfo not_param_info;
      not_param_info.idx_ = fixed_param.idx_;
      not_param_info.raw_text_ = fixed_param.raw_text_;
      OZ (not_param_info_list.push_back(not_param_info));
    }
    OZ (pc_ctx.tpl_sql_const_cons_.push_back(not_param_info_list));
  }
  return ret;
}

int ObUDRUtils::refill_udr_exec_ctx(const ObUDRItemMgr::UDRItemRefGuard &item_guard,
                                    ObSqlCtx &context,
                                    ObResultSet &result,
                                    ObPlanCacheCtx &pc_ctx)
{
  int ret = OB_SUCCESS;
  if (item_guard.is_valid()) {
    ObIAllocator &allocator = result.get_mem_pool();
    const ObUDRItem *rule_item = item_guard.get_ref_obj();
    const ObString &replacement = rule_item->get_replacement();
    ParamStore param_store( (ObWrapperAllocator(allocator)) );
    const DynamicParamInfoArray& dynamic_param_list = rule_item->get_dynamic_param_info_array();
    OX (context.cur_sql_ = replacement);
    OX (pc_ctx.is_rewrite_sql_ = true);
    OX (pc_ctx.rule_name_ = rule_item->get_rule_name());
    OX (pc_ctx.def_name_ctx_ = const_cast<QuestionMarkDefNameCtx *>(rule_item->get_question_mark_def_name_ctx()));
    OZ (cons_udr_param_store(dynamic_param_list, pc_ctx, param_store));
    OZ (ObSql::construct_parameterized_params(param_store, pc_ctx));
    OX (pc_ctx.normal_parse_const_cnt_ = param_store.count());
    if (pc_ctx.mode_ != PC_PS_MODE) {
      OX (context.is_prepare_protocol_ = true);
      OX (const_cast<ObString &>(pc_ctx.raw_sql_) = replacement);
      OX (pc_ctx.mode_ = PC_PS_MODE);
      OX (pc_ctx.set_is_parameterized_execute());
    }
  }
  return ret;
}

int ObUDRUtils::add_param_to_param_store(const ObObjParam &param,
                                         ParamStore &param_store)
{
  int ret = OB_SUCCESS;
  if (lib::is_oracle_mode()
      && ( (param.is_varchar() && 0 == param.get_varchar().length())
            || (param.is_char() && 0 == param.get_char().length())
            || (param.is_nstring() && 0 == param.get_string_len()) )) {
    const_cast<ObObjParam &>(param).set_null();
    const_cast<ObObjParam &>(param).set_param_meta();
  }
  if (OB_FAIL(param_store.push_back(param))) {
    LOG_WARN("pushback param failed", K(ret));
  }
  return ret;
}

int ObUDRUtils::cons_udr_param_store(const DynamicParamInfoArray& dynamic_param_list,
                                     ObPlanCacheCtx &pc_ctx,
                                     ParamStore &param_store)
{
  int ret = OB_SUCCESS;
  ObIAllocator &allocator = pc_ctx.allocator_;
  ObPhysicalPlanCtx *phy_ctx = pc_ctx.exec_ctx_.get_physical_plan_ctx();
  if (dynamic_param_list.empty()) {
    // do nothing
  } else if (OB_ISNULL(phy_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(phy_ctx));
  } else {
    if (OB_ISNULL(pc_ctx.sql_ctx_.session_info_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(pc_ctx.sql_ctx_.session_info_));
    } else if (PC_PS_MODE == pc_ctx.mode_) {
      ObSQLMode sql_mode = pc_ctx.sql_ctx_.session_info_->get_sql_mode();
      ObCharsets4Parser charsets4parser = pc_ctx.sql_ctx_.session_info_->get_charsets4parser();
      FPContext fp_ctx(charsets4parser);
      fp_ctx.enable_batched_multi_stmt_ = pc_ctx.sql_ctx_.handle_batched_multi_stmt();
      fp_ctx.sql_mode_ = sql_mode;
      if (OB_FAIL(ObSqlParameterization::fast_parser(allocator,
                                                    fp_ctx,
                                                    pc_ctx.raw_sql_,
                                                    pc_ctx.fp_result_))) {
        LOG_WARN("failed to fast parser", K(ret), K(sql_mode), K(pc_ctx.raw_sql_));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(clac_dynamic_param_store(dynamic_param_list,
                                                pc_ctx,
                                                param_store))) {
      LOG_WARN("failed to calc dynamic param store", K(ret));
    }
  }
  return ret;
}

int ObUDRUtils::clac_dynamic_param_store(const DynamicParamInfoArray& dynamic_param_list,
                                         ObPlanCacheCtx &pc_ctx,
                                         ParamStore &param_store)
{
  int ret = OB_SUCCESS;
  ObString literal_prefix;
  ObObjParam value;
  const bool is_paramlize = false;
  int64_t server_collation = CS_TYPE_INVALID;
  uint64_t tenant_data_version = 0;

  ObIAllocator &allocator = pc_ctx.allocator_;
  ObSQLSessionInfo *session = pc_ctx.exec_ctx_.get_my_session();
  ObIArray<ObPCParam *> &raw_params = pc_ctx.fp_result_.raw_params_;
  ObPhysicalPlanCtx *phy_ctx = pc_ctx.exec_ctx_.get_physical_plan_ctx();
  bool enable_decimal_int = false;
  ObCompatType compat_type = COMPAT_MYSQL57;
  if (OB_ISNULL(session) || OB_ISNULL(phy_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(session), KP(phy_ctx));
  } else if (OB_FAIL(session->get_compatibility_control(compat_type))) {
    LOG_WARN("failed to get compat type", K(ret));
  } else if (OB_FAIL(param_store.reserve(dynamic_param_list.count()))) {
    LOG_WARN("failed to reserve array", K(ret), K(dynamic_param_list.count()));
  } else if (lib::is_oracle_mode() && OB_FAIL(
    session->get_sys_variable(share::SYS_VAR_COLLATION_SERVER, server_collation))) {
    LOG_WARN("get sys variable failed", K(ret));
  } else if (OB_FAIL(ObSQLUtils::check_enable_decimalint(session, enable_decimal_int))) {
    LOG_WARN("fail to check enable decimal int", K(ret));
  } else {
    ObCollationType coll_conn = static_cast<ObCollationType>(session->get_local_collation_connection());
    for (int i = 0; OB_SUCC(ret) && i < dynamic_param_list.count(); ++i) {
      value.reset();
      const DynamicParamInfo &dynamic_param_info = dynamic_param_list.at(i);
      ParseNode *raw_param = NULL;
      ObPCParam *pc_param = NULL;
      if (dynamic_param_info.raw_param_idx_ >= raw_params.count()) {
        ret = OB_ERR_PARSER_SYNTAX;
        LOG_WARN("invalid idx", K(dynamic_param_info.raw_param_idx_), K(raw_params.count()));
      } else if (OB_ISNULL(pc_param = raw_params.at(dynamic_param_info.raw_param_idx_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("raw param is null", K(ret));
      } else if (OB_ISNULL(raw_param = pc_param->node_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("node is null", K(ret));
      } else if (T_QUESTIONMARK == raw_param->type_) {
        if (pc_ctx.mode_ != PC_PS_MODE || raw_param->value_ >= pc_ctx.fp_result_.parameterized_params_.count()) {
          ret = OB_ERR_PARSER_SYNTAX;
          LOG_WARN("invalid argument", K(ret), K(raw_param->value_), K(dynamic_param_info.raw_param_idx_),
          K(pc_ctx.mode_), K(pc_ctx.fp_result_.parameterized_params_.count()));
        } else if (OB_FAIL(param_store.push_back(*pc_ctx.fp_result_.parameterized_params_.at(raw_param->value_)))) {
          LOG_WARN("pushback param failed", K(ret));
        }
      } else if (OB_FAIL(ObResolverUtils::resolve_const(raw_param,
                                                        stmt::T_NONE,
                                                        allocator,
                                                        coll_conn,
                                                        session->get_nls_collation_nation(),
                                                        session->get_timezone_info(),
                                                        value,
                                                        is_paramlize,
                                                        literal_prefix,
                                                        session->get_actual_nls_length_semantics(),
                                                        static_cast<ObCollationType>(server_collation),
                                                        NULL, session->get_sql_mode(),
                                                        enable_decimal_int,
                                                        compat_type))) {
        LOG_WARN("fail to resolve const", K(ret));
      } else if (OB_FAIL(add_param_to_param_store(value, param_store))) {
        LOG_WARN("failed to add param to param store", K(ret), K(value), K(param_store));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(phy_ctx->get_param_store_for_update().assign(param_store))) {
        LOG_WARN("failed to assign param store", K(ret));
      } else {
        pc_ctx.fp_result_.cache_params_ = &(phy_ctx->get_param_store_for_update());
      }
    }
  }
  return ret;
}

int ObUDRUtils::match_udr_and_refill_ctx(const ObString &pattern,
                                         ObSqlCtx &sql_ctx,
                                         ObResultSet &result,
                                         ObPlanCacheCtx &pc_ctx,
                                         bool &is_match_udr,
                                         ObUDRItemMgr::UDRItemRefGuard &item_guard)
{
  int ret = OB_SUCCESS;
  is_match_udr = false;
  bool enable_udr = sql_ctx.get_enable_user_defined_rewrite();
  ObSQLSessionInfo &session = result.get_session();
  ObExecContext &ectx = result.get_exec_context();
  if (enable_udr && !(pc_ctx.is_inner_sql() || PC_PL_MODE == pc_ctx.mode_)) {
    ObIAllocator &allocator = result.get_mem_pool();
    PatternConstConsList cst_cons_list;
    if (OB_FAIL(match_udr_item(pattern, session, ectx, allocator, item_guard, &cst_cons_list))) {
      LOG_WARN("failed to match user defined rewrite rule", K(ret));
    } else if (!cst_cons_list.empty()
      && OB_FAIL(cons_udr_const_cons_list(cst_cons_list, pc_ctx))) {
      LOG_WARN("failed to cons tpl sql const cons list", K(ret));
    } else if (!item_guard.is_valid()) {
      is_match_udr = false;
      LOG_TRACE("no matching user-defined rules", K(ret));
    } else if (OB_FAIL(refill_udr_exec_ctx(item_guard,
                                          sql_ctx,
                                          result,
                                          pc_ctx))) {
      LOG_WARN("failed to refill rewrite sql exec ctx", K(ret));
    } else {
      is_match_udr = true;
      LOG_TRACE("succ to match user-defined rule", K(ret));
    }
  }
  return ret;
}

} // namespace sql end
} // namespace oceanbase end
