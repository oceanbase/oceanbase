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

#define USING_LOG_PREFIX SQL
#include "sql/ob_sql.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_se_array.h"
#include "lib/encrypt/ob_encrypted_helper.h"
#include "lib/json/ob_json.h"
#include "lib/profile/ob_profile_log.h"
#include "lib/profile/ob_trace_id.h"
#include "lib/thread_local/ob_tsi_factory.h"
#include "lib/string/ob_sql_string.h"
#include "lib/json/ob_json_print_utils.h"
#include "lib/profile/ob_perf_event.h"
#include "lib/rc/context.h"
#include "share/ob_truncated_string.h"
#include "share/partition_table/ob_partition_location.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/ob_autoincrement_service.h"
#include "share/ob_rs_mgr.h"
#include "share/config/ob_server_config.h"
#include "common/sql_mode/ob_sql_mode_utils.h"
#include "share/vector/ob_fixed_length_vector.h"
#include "share/vector/ob_continuous_vector.h"
#include "share/vector/ob_uniform_vector.h"
#include "share/vector/ob_discrete_vector.h"
#include "sql/ob_sql_context.h"
#include "sql/ob_result_set.h"
#include "sql/optimizer/ob_log_plan_factory.h"
#include "sql/plan_cache/ob_plan_cache.h"
#include "sql/plan_cache/ob_ps_cache.h"
#include "sql/plan_cache/ob_pcv_set.h"
#include "sql/engine/table/ob_virtual_table_ctx.h"
#include "sql/ob_sql_init.h"
#include "sql/ob_sql_utils.h"
#include "sql/monitor/ob_security_audit_utils.h"
#include "sql/optimizer/ob_log_plan.h"
#include "sql/optimizer/ob_optimizer.h"
#include "sql/optimizer/ob_optimizer_context.h"
#include "sql/parser/ob_parser.h"
#include "sql/parser/parse_malloc.h"
#include "sql/parser/parse_node.h"
#include "sql/parser/parse_define.h"
#include "sql/resolver/cmd/ob_help_stmt.h"
#include "sql/resolver/ob_cmd.h"
#include "sql/resolver/ob_resolver.h"
#include "sql/resolver/ob_schema_checker.h"
#include "sql/resolver/cmd/ob_variable_set_stmt.h"
#include "sql/resolver/cmd/ob_call_procedure_stmt.h"
#include "sql/resolver/cmd/ob_anonymous_block_stmt.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/privilege_check/ob_privilege_check.h"
#include "share/system_variable/ob_system_variable_alias.h"
#include "sql/rewrite/ob_transformer_impl.h"
#include "sql/rewrite/ob_transform_project_pruning.h"
#include "sql/rewrite/ob_transform_pre_process.h"
#include "sql/plan_cache/ob_cache_object_factory.h"
#include "sql/monitor/ob_phy_plan_monitor_info.h"
#include "sql/plan_cache/ob_ps_sql_utils.h"
#include "lib/utility/ob_tracepoint.h"
#include "observer/ob_server_struct.h"
#include "observer/omt/ob_th_worker.h"
#include "sql/resolver/dml/ob_del_upd_stmt.h"
#include "sql/resolver/dml/ob_update_stmt.h"
#include "sql/printer/ob_raw_expr_printer.h"
#include "sql/engine/px/ob_px_admission.h"
#include "sql/code_generator/ob_code_generator.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "sql/executor/ob_executor_rpc_impl.h"
#include "sql/executor/ob_remote_executor_processor.h"
#include "sql/udr/ob_udr_utils.h"
#include "sql/udr/ob_udr_mgr.h"
#include "sql/udr/ob_udr_analyzer.h"
#include "common/ob_smart_call.h"
#include "sql/ob_optimizer_trace_impl.h"
#include "share/resource_manager/ob_resource_manager.h"
#include "share/resource_manager/ob_cgroup_ctrl.h"
#ifdef OB_BUILD_SPM
#include "sql/spm/ob_spm_controller.h"
#include "sql/spm/ob_spm_define.h"
#endif
#ifdef OB_BUILD_ORACLE_PL
#include "pl/sys_package/ob_json_pl_utils.h"
#endif
#include "sql/ob_optimizer_trace_impl.h"
#include "sql/monitor/ob_sql_plan.h"
#include "sql/optimizer/ob_explain_log_plan.h"
#include "sql/dblink/ob_dblink_utils.h"
#include "sql/plan_cache/ob_values_table_compression.h"
#include "pl/ob_pl_stmt.h"
#include "pl/ob_pl_resolver.h"

namespace oceanbase
{
using namespace common;
using namespace rpc::frame;
using namespace obrpc;
using namespace share;
using namespace share::schema;

namespace sql
{

const int64_t ObSql::max_error_length = 80;
const int64_t ObSql::SQL_MEM_SIZE_LIMIT = 1024 * 1024 * 64;

int ObSql::init(common::ObOptStatManager *opt_stat_mgr,
                ObReqTransport *transport,
                common::ObITabletScan *vt_partition_service,
                common::ObAddr &addr,
                share::ObRsMgr &rs_mgr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(opt_stat_mgr)
      || OB_ISNULL(transport)
      || OB_ISNULL(vt_partition_service)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args",
             K(ret),
             KP(opt_stat_mgr),
             KP(transport),
             KP(vt_partition_service));
  } else {
    if (OB_FAIL(queue_.init(1, 512))) {
      LOG_WARN("queue init failed", K(ret));
    } else {
      opt_stat_mgr_ = opt_stat_mgr;
      transport_ = transport;
      vt_partition_service_ = vt_partition_service;
      self_addr_ = addr;
      rs_mgr_ = &rs_mgr;
      inited_ = true;
      queue_.start();
    }
  }
  return ret;
}

void ObSql::destroy() {
  if (inited_) {
    queue_.destroy();
    inited_ = false;
  }
}

void ObSql::stat()
{
  sql::print_sql_stat();
}
#define STMT_SUPPORT_BY_TXN_FREE_ROUTE(stmt_type, allow_ps)             \
  (ObStmt::is_dml_stmt(stmt_type)                                       \
   || (stmt_type == stmt::StmtType::T_VARIABLE_SET)                     \
   || (stmt_type == stmt::StmtType::T_USE_DATABASE)                     \
   || (allow_ps && stmt_type == stmt::StmtType::T_PREPARE)              \
   || (allow_ps && stmt_type == stmt::StmtType::T_EXECUTE)              \
   || (allow_ps && stmt_type == stmt::StmtType::T_DEALLOCATE))

#define CHECK_STMT_SUPPORTED_BY_TXN_FREE_ROUTE(result, allow_ps)        \
 if (OB_SUCC(ret)) {                                                    \
   stmt::StmtType stmt_type = result.get_stmt_type();                   \
   ObSQLSessionInfo &session = result.get_session();                    \
   if (!session.is_inner() && session.is_txn_free_route_temp()) {       \
     if (!STMT_SUPPORT_BY_TXN_FREE_ROUTE(stmt_type, allow_ps)) {        \
       ret = OB_TRANS_FREE_ROUTE_NOT_SUPPORTED;                         \
       LOG_WARN("only DML stmt or SET command is supported to be executed on txn temporary node", \
                KR(ret), K(stmt_type), K(session.get_txn_free_route_ctx()), K(session)); \
     }                                                                  \
     ObPhysicalPlan* phy_plan = result.get_physical_plan();             \
     if (OB_SUCCESS == ret && NULL != phy_plan) {                       \
       if (phy_plan->has_link_table()) {                                \
         ret = OB_TRANS_FREE_ROUTE_NOT_SUPPORTED;                       \
         LOG_WARN("stmt with dblink can not be executed on txn temporary node", \
                  KR(ret), K(stmt_type), K(session.get_txn_free_route_ctx()), K(session)); \
       }                                                                \
     }                                                                  \
   }                                                                    \
 }


int ObSql::stmt_prepare(const common::ObString &stmt,
                        ObSqlCtx &context,
                        ObResultSet &result,
                        bool is_inner_sql/*true*/)
{
  int ret = OB_SUCCESS;
  LinkExecCtxGuard link_guard(result.get_session(), result.get_exec_context());
  if (OB_FAIL(sanity_check(context))) {
    LOG_WARN("Failed to do sanity check", K(ret));
  } else if (OB_FAIL(handle_ps_prepare(stmt, context, result, is_inner_sql))) {
    LOG_WARN("failed to handle ps query", K(stmt), K(ret));
  }
  CHECK_STMT_SUPPORTED_BY_TXN_FREE_ROUTE(result, false);
  if (OB_FAIL(ret) && OB_SUCCESS == result.get_errcode()) {
    result.set_errcode(ret);
  }
  if (OB_FAIL(ret)) {
    rollback_implicit_trans_when_fail(result, ret);
  }
  return ret;
}

int ObSql::stmt_query(const common::ObString &stmt, ObSqlCtx &context, ObResultSet &result)
{
  int ret = OB_SUCCESS;
  LinkExecCtxGuard link_guard(result.get_session(), result.get_exec_context());
  FLTSpanGuard(sql_compile);
  ObTruncatedString trunc_stmt(stmt);
#ifndef NDEBUG
  LOG_INFO("Begin to handle text statement", K(trunc_stmt),
           "sess_id", result.get_session().get_sessid(),
           "proxy_sess_id", result.get_session().get_proxy_sessid(),
           "tenant_id", result.get_session().get_effective_tenant_id(),
           "execution_id", result.get_session().get_current_execution_id());
#endif
  NG_TRACE(parse_begin);
  //1 check inited
  if (OB_FAIL(sanity_check(context))) {
    LOG_WARN("Failed to do sanity check", K(ret));
  } else if (OB_FAIL(handle_text_query(stmt, context, result))) {
    if (OB_EAGAIN != ret && OB_ERR_PROXY_REROUTE != ret) {
      LOG_WARN("fail to handle text query",
               "stmt", context.is_sensitive_ ? ObString(OB_MASKED_STR) : stmt, K(ret));
    }
  }
  CHECK_STMT_SUPPORTED_BY_TXN_FREE_ROUTE(result, true);
  if (OB_SUCC(ret)) {
    result.get_session().set_exec_min_cluster_version();
  }
  //LOG_DEBUG("result errno", N_ERR_CODE, result.get_errcode(), K(ret));
  if (OB_SUCCESS != ret
      && OB_SUCCESS == result.get_errcode()) {
    result.set_errcode(ret);
  }
  if (OB_ISNULL(result.get_physical_plan())) {
  } else {
    FLT_SET_TAG(plan_hash, result.get_physical_plan()->get_plan_hash_value());
  }
  FLT_SET_TAG(database_id, result.get_session().get_database_id(),
                sql_id, context.sql_id_);
  NG_TRACE_EXT(stmt_query_end, OB_ID(stmt),
               context.is_sensitive_ ? ObString(OB_MASKED_STR) : trunc_stmt.string(),
               OB_ID(stmt_len), stmt.length());

  if (OB_FAIL(ret)) {
    rollback_implicit_trans_when_fail(result, ret);
  }
  return ret;
}

int ObSql::stmt_execute(const ObPsStmtId stmt_id,
                        const stmt::StmtType stmt_type,
                        const ParamStore &params,
                        ObSqlCtx &context,
                        ObResultSet &result,
                        bool is_inner_sql)
{
  int ret = OB_SUCCESS;
  LinkExecCtxGuard link_guard(result.get_session(), result.get_exec_context());
  if (OB_FAIL(sanity_check(context))) {
    LOG_WARN("failed to do sanity check", K(ret));
  } else if (OB_FAIL(init_result_set(context, result))) {
    LOG_WARN("failed to init result set", K(ret));
  } else if (
#ifdef ERRSIM
      // inject error for pr-ex protocol only
      // inject after `init_result_set` because retry test would check session ptr in the exec ctx,
      // which is initialized by `init_result_set`.
      OB_FAIL(EVENT_CALL(common::EventTable::COM_STMT_PREXECUTE_EXECUTE_ERROR, context.is_pre_execute_)) ||
#endif
      OB_FAIL(handle_ps_execute(stmt_id, stmt_type, params, context, result, is_inner_sql))) {
    if (OB_ERR_PROXY_REROUTE != ret) {
      LOG_WARN("failed to handle ps execute", K(stmt_id), K(ret));
    }
  }
  CHECK_STMT_SUPPORTED_BY_TXN_FREE_ROUTE(result, false);
  if (OB_SUCC(ret)) {
    result.get_session().set_exec_min_cluster_version();
  }
  if (OB_FAIL(ret) && OB_SUCCESS == result.get_errcode()) {
    result.set_errcode(ret);
  }
  FLT_SET_TAG(sql_id, context.sql_id_);
  if (OB_FAIL(ret)) {
    rollback_implicit_trans_when_fail(result, ret);
  }
  return ret;
}

int ObSql::stmt_list_field(const common::ObString &table_name,
                           const common::ObString &wild_str,
                           ObSqlCtx &context,
                           ObResultSet &result)
{
  UNUSED(table_name);
  UNUSED(wild_str);
  UNUSED(context);
  UNUSED(result);
  return OB_NOT_IMPLEMENT;
}

int ObSql::fill_result_set(ObResultSet &result_set,
                           ObSqlCtx *context,
                           const PlanCacheMode mode,
                           ObStmt &basic_stmt)
{
  int ret = OB_SUCCESS;

  ObStmt *stmt = &basic_stmt;
  ObExecContext &ectx = result_set.get_exec_context();
  ObPhysicalPlanCtx *pctx = ectx.get_physical_plan_ctx();
  if (OB_UNLIKELY(NULL == context) || OB_UNLIKELY(NULL == context->session_info_) || OB_UNLIKELY(NULL == pctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(context), K(pctx),
             "session", (context != NULL) ? context->session_info_ : NULL);
  } else {
    result_set.set_affected_rows(0);
    result_set.set_warning_count(0);
    result_set.set_message("");
    ObString type_name = ObString::make_string("varchar");
    number::ObNumber number;
    number.set_zero();
    ObDelUpdStmt *del_upd_stmt = NULL;
    ObField field;
    common::ObIAllocator &alloc = result_set.get_mem_pool();
    ObCharsetType result_charset = CHARSET_INVALID;
    ObCollationType collation_type = CS_TYPE_INVALID;
    context->session_info_->get_character_set_results(result_charset);
    collation_type =
        ObCharset::get_default_collation_by_mode(result_charset, lib::is_oracle_mode());
    switch (stmt->get_stmt_type()) {
    case stmt::T_SELECT: {
      if (OB_FAIL(fill_select_result_set(result_set, context, mode, collation_type, type_name,
                                         basic_stmt, field))) {
        LOG_WARN("fill select result set failed", K(ret));
      }
      break;
    }
    case stmt::T_INSERT:
    case stmt::T_REPLACE:
    case stmt::T_UPDATE:
    case stmt::T_DELETE: {
      del_upd_stmt = static_cast<ObDelUpdStmt *>(stmt);
      if (!del_upd_stmt->is_returning()) {
        break;
      }
      const common::ObIArray<ObRawExpr*> *returning_exprs = &(del_upd_stmt->get_returning_exprs());
      const common::ObIArray<ObString> &returning_strs = del_upd_stmt->get_returning_strs();
      int64_t size = returning_exprs->count();
      field.charsetnr_ = CS_TYPE_UTF8MB4_GENERAL_CI;
      if (OB_FAIL(result_set.reserve_field_columns(size))) {
        LOG_WARN("reserve field columns failed", K(ret), K(size));
      }

      for (int64_t i = 0; OB_SUCC(ret) && i < size; i++) {
        ObRawExpr *expr = returning_exprs->at(i);
        if (OB_UNLIKELY(OB_ISNULL(expr))) {
          ret = OB_ERR_ILLEGAL_ID;
          LOG_WARN("fail to get expr", K(ret), K(i), K(size));
        }
        if (OB_SUCC(ret)) {
          if (ob_is_string_or_lob_type(expr->get_data_type())
              && CS_TYPE_BINARY != expr->get_collation_type()
              && ObCharset::is_valid_collation(collation_type)) {
            field.charsetnr_ = static_cast<uint16_t>(collation_type);
          } else {
            field.charsetnr_ = static_cast<uint16_t>(expr->get_collation_type());
          }
        }
        if (OB_SUCC(ret)) {
          expr->deduce_type(context->session_info_);
          field.type_.set_type(expr->get_data_type());
          field.accuracy_ = expr->get_accuracy();
          field.flags_ = static_cast<uint16_t>(expr->get_result_flag());
          // Setup Collation and Collation levl
          if (ob_is_string_or_lob_type(static_cast<ObObjType>(expr->get_data_type()))
              || ob_is_raw(static_cast<ObObjType>(expr->get_data_type()))
              || ob_is_enum_or_set_type(static_cast<ObObjType>(expr->get_data_type()))) {
            field.type_.set_collation_type(expr->get_collation_type());
            field.type_.set_collation_level(expr->get_collation_level());
          }
          if (ObVarcharType == field.type_.get_type()) {
            field.type_.set_varchar(type_name);
          } else if (ObNumberType == field.type_.get_type()) {
            field.type_.set_number(number);
          }

          if (expr->get_result_type().is_geometry()) {
            uint16_t subschema_id = ObInvalidSqlType;
            ObSqlUDTMeta udt_meta;
            field.type_.meta_.set_ext();
            field.accuracy_.set_accuracy(T_OBJ_SDO_GEOMETRY);
            if (OB_FAIL(result_set.get_exec_context().get_subschema_id_by_udt_id(T_OBJ_SDO_GEOMETRY, subschema_id))) {
              LOG_WARN("unsupported udt id", K(ret), K(subschema_id));
            } else if (OB_FAIL(result_set.get_exec_context().get_sqludt_meta_by_subschema_id(subschema_id, udt_meta))) {
              LOG_WARN("failed to get udt meta", K(ret), K(subschema_id));
            } else if(ObObjUDTUtil::ob_is_supported_sql_udt(udt_meta.udt_id_)) {
              field.type_.set_subschema_id(subschema_id);
              if (OB_FAIL(ob_write_string(alloc, ObString(udt_meta.udt_name_len_, udt_meta.udt_name_), field.type_name_))) {
                LOG_WARN("fail to alloc string", K(i), K(field), K(ret));
              }
            } else {
              ret = OB_NOT_SUPPORTED;
              LOG_WARN("udt type not supported", K(ret), K(subschema_id));
            }
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(ob_write_string(alloc, returning_strs.at(i), field.cname_))) {
            LOG_WARN("fail to alloc", K(ret), K(returning_strs.at(i)));
          }
        }
        if (OB_SUCC(ret)) {
          field.is_paramed_select_item_ = false;
          field.paramed_ctx_ = NULL;
          if (OB_FAIL(result_set.add_field_column(field))) {
            LOG_WARN("fail to add field column to result_set", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          field.cname_.assign(NULL, 0);
          field.org_cname_.assign(NULL, 0);
          field.dname_.assign(NULL, 0);
          field.tname_.assign(NULL, 0);
          field.org_tname_.assign(NULL, 0);
          field.type_.reset();
          field.type_.set_type(ObExtendType);
        }
      }
      break;
    }
    case stmt::T_EXPLAIN: {
      ObString tname = ObString::make_string("explain_table");
      ObString cname = ObString::make_string("Query Plan");
      field.tname_ = tname;
      field.org_tname_ = tname;
      field.cname_ = cname;
      field.org_cname_ = cname;
      field.type_.set_type(ObVarcharType);
      field.charsetnr_ = CS_TYPE_UTF8MB4_GENERAL_CI;
      field.type_.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
      field.type_.set_collation_level(CS_LEVEL_IMPLICIT);
      field.type_.set_varchar(type_name);
      if (OB_FAIL(result_set.reserve_field_columns(1))) {
        LOG_WARN("reserve field columns failed", K(ret));
      } else if (OB_FAIL(result_set.add_field_column(field))) {
        LOG_WARN("fail to add field column to result_set", K(ret));
      }
      break;
    }
    case stmt::T_HELP: {
      ObHelpStmt *help_stmt = static_cast<ObHelpStmt *>(stmt);
      if (OB_UNLIKELY(NULL == help_stmt)) {
        ret = OB_ERR_PARSE_SQL;
        LOG_WARN("logical plan of help statement error", K(ret));
      } else {
        ObString tname = ObString::make_string("help_table");
        field.tname_ = tname;
        field.org_tname_ = tname;
        field.charsetnr_ = CS_TYPE_UTF8MB4_GENERAL_CI;
        int64_t col_count = help_stmt->get_col_count();
        if (OB_FAIL(result_set.reserve_field_columns(col_count))) {
          LOG_WARN("reserve field columns failed", K(ret), K(col_count));
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
          field.type_.set_type(ObVarcharType);
          field.type_.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
          field.type_.set_collation_level(CS_LEVEL_IMPLICIT);
          field.type_.set_varchar(type_name);
          ObString col_name;
          if (OB_FAIL(help_stmt->get_col_name(i, col_name))) {
            LOG_WARN("fail to get column name", K(ret), K(i));
          } else if (OB_FAIL(ob_write_string(alloc, col_name, field.cname_))) {
            LOG_WARN("fail to alloc string", K(ret), "name", col_name);
          } else if (OB_FAIL(ob_write_string(alloc, col_name, field.org_cname_))) {
            LOG_WARN("fail to alloc string", K(ret), "name", col_name);
          } else if (OB_FAIL(result_set.add_field_column(field))) {
            LOG_WARN("fail to add field column to result_set", K(ret));
          } else {
            field.cname_.assign(NULL, 0);
            field.org_cname_.assign(NULL, 0);
          }
        }
      }
      break;
    }
    case stmt::T_CALL_PROCEDURE: {
      ObCallProcedureStmt &call_stmt = static_cast<ObCallProcedureStmt&>(basic_stmt);
      ObString tname = ObString::make_string("procedure");
      if (NULL == call_stmt.get_call_proc_info()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("call proc info is null", K(ret));
      } else {
        int64_t size = call_stmt.get_call_proc_info()->get_output_count();
        field.charsetnr_ = CS_TYPE_UTF8MB4_GENERAL_CI;

        if (0 == size) {
          break;
        }

        if (OB_SUCC(ret) && OB_FAIL(result_set.reserve_field_columns(size))) {
          LOG_WARN("reserve field columns failed", K(ret), K(size));
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
          ObCollationType charsetnr;
          ObDataType *type = call_stmt.get_call_proc_info()->get_out_type().at(i).get_data_type();
          ObObjType out_obj_type = call_stmt.get_call_proc_info()->get_out_type().at(i).get_obj_type();
          if (ObUnknownType == out_obj_type
            || ObExtendType == out_obj_type) {
            // do nothing ...
          } else if (OB_NOT_NULL(type)) {
            if (ob_is_string_or_lob_type(out_obj_type)
                  && CS_TYPE_ANY == type->get_collation_type()) {
              charsetnr = ObCharset::is_valid_collation(collation_type)
                            ? collation_type
                            : CS_TYPE_UTF8MB4_BIN;
            } else {
              OZ (ObCharset::get_default_collation(type->get_collation_type(), charsetnr));
            }
            OX (field.charsetnr_ = static_cast<uint16_t>(charsetnr));
          }
          if (OB_SUCC(ret)) {
            field.type_.set_type(out_obj_type);
            if (OB_NOT_NULL(type)) {
              field.accuracy_ = type->get_accuracy();
              // Setup Collation and Collation levl
              if (ob_is_string_or_lob_type(out_obj_type)
                  || ob_is_raw(out_obj_type)
                  || ob_is_enum_or_set_type(out_obj_type)) {
                field.type_.set_collation_type(type->get_collation_type());
                field.type_.set_collation_level(type->get_collation_level());
              }
            }
            if (ObExtendType == out_obj_type) {
              field.length_ = field.accuracy_.get_length();
            } else if (ObCharType == out_obj_type
                      || ObVarcharType == out_obj_type
                      || ob_is_nstring_type(out_obj_type)) {
              if (-1 == field.accuracy_.get_length()) {
                field.length_ = ObCharType == out_obj_type ?
                  OB_MAX_ORACLE_CHAR_LENGTH_BYTE : OB_MAX_ORACLE_VARCHAR_LENGTH;
              } else {
                field.length_ = field.accuracy_.get_length();
              }
            } else if (ob_is_enum_or_set_type(out_obj_type)) {
              CK (OB_NOT_NULL(type));
              OZ (common::ObField::get_field_mb_length(field.type_.get_type(),
                                                      field.accuracy_,
                                                      type->get_collation_type(),
                                                      field.length_));
              OX (field.type_.set_type(ObVarcharType));
            } else {
              OZ (common::ObField::get_field_mb_length(field.type_.get_type(),
                                                      field.accuracy_,
                                                      common::CS_TYPE_INVALID,
                                                      field.length_));
            }
            // Setup Scale
            if (OB_SUCC(ret)) {
              if (ObVarcharType == field.type_.get_type()) {
                field.type_.set_varchar(type_name);
              } else if (ObNumberType == field.type_.get_type()) {
                field.type_.set_number(number);
              }
            }
          }
          if (OB_SUCC(ret)) {
            if (OB_FAIL(ob_write_string(alloc, tname, field.tname_))) {
              LOG_WARN("fail to alloc string", K(call_stmt.get_call_proc_info()->get_out_name().at(i)), K(ret));
            } else if (OB_FAIL(ob_write_string(alloc, tname, field.org_tname_))) {
              LOG_WARN("fail to alloc string", K(call_stmt.get_call_proc_info()->get_out_name().at(i)), K(ret));
            } else if (OB_FAIL(ob_write_string(alloc, call_stmt.get_call_proc_info()->get_out_name().at(i), field.cname_))) {
              LOG_WARN("fail to alloc string", K(call_stmt.get_call_proc_info()->get_out_name().at(i)), K(ret));
            } else if (OB_FAIL(ob_write_string(alloc, call_stmt.get_call_proc_info()->get_out_name().at(i), field.org_cname_))) {
              LOG_WARN("fail to alloc string", K(call_stmt.get_call_proc_info()->get_out_name().at(i)), K(ret));
            } else if (OB_FAIL(ob_write_string(alloc, call_stmt.get_call_proc_info()->get_out_type_name().at(i), field.type_name_))) {
              LOG_WARN("fail to alloc string", K(call_stmt.get_call_proc_info()->get_out_type_name().at(i)), K(ret));
            } else if (OB_FAIL(ob_write_string(alloc, call_stmt.get_call_proc_info()->get_out_type_owner().at(i), field.type_owner_))) {
              LOG_WARN("fail to alloc string", K(call_stmt.get_call_proc_info()->get_out_type_owner().at(i)), K(ret));
            } else { /*do nothing*/ }
          }

          if (OB_SUCC(ret)) {
            if (OB_FAIL(result_set.add_field_column(field))) {
              LOG_WARN("fail to add field column to result_set.", K(ret));
            }
          }

          if (OB_SUCC(ret)) {
            field.cname_.assign(NULL, 0);
            field.org_cname_.assign(NULL, 0);
            field.dname_.assign(NULL, 0);
            field.tname_.assign(NULL, 0);
            field.org_tname_.assign(NULL, 0);
            field.type_.reset();
            field.type_.set_type(ObExtendType);
            field.type_name_.assign(NULL, 0);
            field.type_owner_.assign(NULL, 0);
          }
        }
        break;
      }
    }
    default:
      break;
    }

    const int64_t question_marks_count = pctx->get_is_ps_rewrite_sql() ?
          pctx->get_orig_question_mark_cnt() : stmt->get_query_ctx()->get_prepare_param_count();
    // param column is only needed in ps mode
    if (OB_SUCC(ret) && question_marks_count > 0
        && (PC_PS_MODE == mode || PC_PL_MODE == mode)) {
      if (OB_FAIL(result_set.reserve_param_columns(question_marks_count))) {
        LOG_WARN("reserve param columns failed", K(ret), K(question_marks_count));
      }
      ObAnonymousBlockStmt *anonymous_stmt = NULL;
      ObCallProcedureStmt *call_stmt = NULL;
      if (stmt::T_ANONYMOUS_BLOCK == stmt->get_stmt_type()) {
        CK (OB_NOT_NULL(anonymous_stmt = static_cast<ObAnonymousBlockStmt *>(stmt)));
        OZ (result_set.reserve_field_columns(anonymous_stmt->get_out_idx().num_members()));
      } else if (stmt::T_CALL_PROCEDURE == stmt->get_stmt_type()) {
        CK (OB_NOT_NULL(call_stmt = static_cast<ObCallProcedureStmt *>(stmt)));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < question_marks_count; ++i) {
        ObField param_field;
        param_field.type_.set_type(ObIntType); // @bug
        param_field.cname_ = ObString::make_string("?");
        if (OB_NOT_NULL(anonymous_stmt) && anonymous_stmt->get_out_idx().has_member(i)) {
          ObField column_field;
          column_field.type_.set_type(ObNullType);
          OX (param_field.inout_mode_ = ObRoutineParamInOut::SP_PARAM_INOUT);
          OZ (result_set.add_field_column(column_field),
              K(i), K(question_marks_count), K(column_field));
        } else if (OB_NOT_NULL(call_stmt) && call_stmt->get_call_proc_info()->is_out_param(i)) {
          OX (param_field.inout_mode_ = ObRoutineParamInOut::SP_PARAM_INOUT);
        }
        OZ (result_set.add_param_column(param_field),
            K(param_field), K(i), K(question_marks_count));
      }
    }
    // for resolve returning_params and only work for ps_mode
    // SQL: insert/update/delete ...returning expr1 ... into ?...
    if (OB_SUCC(ret) && ObStmt::is_dml_write_stmt(stmt->get_stmt_type())
        && (PC_PS_MODE == mode || PC_PL_MODE == mode)) {
      del_upd_stmt = static_cast<ObDelUpdStmt *>(stmt);
      if (del_upd_stmt->is_returning()) {
        int64_t returning_param_num = del_upd_stmt->get_returning_exprs().count();
        if (OB_FAIL(result_set.reserve_returning_param_column(returning_param_num))) {
          LOG_WARN("reserve returning param columns failed", K(ret), K(question_marks_count));
        }
        for (int i = 0; OB_SUCC(ret) && i < returning_param_num; ++i) {
          ObField param_field;
          // type is mock, client not depend it
          param_field.type_.set_type(ObIntType);
          param_field.cname_ = ObString::make_string("?");
          param_field.inout_mode_ = ObRoutineParamInOut::SP_PARAM_OUT;
          OZ (result_set.add_returning_param_column(param_field), param_field, i, returning_param_num);
        }
      }
    }
  }
  return ret;
}

int ObSql::get_composite_type_field_name(ObSchemaGetterGuard &schema_guard,
                                         int64_t type_id,
                                         ObSqlString &composite_field_name)
{
  int ret = OB_SUCCESS;
  const pl::ObUserDefinedType *user_type = NULL;
  const ObUDTTypeInfo *udt_info = NULL;
  ObArenaAllocator allocator;
  const uint64_t tenant_id = pl::get_tenant_id_by_object_id(type_id);
  if (OB_FAIL(schema_guard.get_udt_info(tenant_id, type_id, udt_info))) {
    OB_LOG(WARN, "get user type fail.", K(type_id), K(ret));
  } else if (NULL == udt_info) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "udt info is null.", K(type_id), K(ret));
  } else if (OB_FAIL(udt_info->transform_to_pl_type(allocator, user_type))) {
    OB_LOG(WARN, "faild to transform to pl type", K(ret));
  } else if (NULL == user_type) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "user type is null.", K(type_id), K(ret));
  } else {
    if (user_type->is_record_type()) {
      const pl::ObRecordType *rec_type = static_cast<const pl::ObRecordType *>(user_type);
      OZ (composite_field_name.append("("));
      for (int64_t i = 0; OB_SUCC(ret) && i < rec_type->get_member_count(); ++i) {
        pl::ObRecordMember *member = const_cast<pl::ObRecordMember *>(rec_type->get_record_member(i));
        CK (OB_NOT_NULL(member));
        OZ (composite_field_name.append(member->member_name_));
        if (OB_FAIL(ret)) {
          // do nothing
        } else if (member->member_type_.is_record_type() || member->member_type_.is_collection_type()) {
          OZ (get_composite_type_field_name(schema_guard,
                                            member->member_type_.get_user_type_id(),
                                            composite_field_name));
        }
        if (i < rec_type->get_member_count() - 1) {
          OZ (composite_field_name.append(", "));
        }
      }
      OZ (composite_field_name.append(")"));
    } else if (user_type->is_collection_type()) {
      const pl::ObCollectionType *coll_type = static_cast<const pl::ObCollectionType*>(user_type);
      if (coll_type->get_element_type().is_record_type()
           || coll_type->get_element_type().is_collection_type()) {
        OZ (get_composite_type_field_name(schema_guard,
                                          coll_type->get_element_type().get_user_type_id(),
                                          composite_field_name));
      }
    }
  }
  return ret;
}

int ObSql::fill_select_result_set(ObResultSet &result_set, ObSqlCtx *context, const PlanCacheMode mode,
                                  ObCollationType collation_type, const ObString &type_name,
                                  ObStmt &basic_stmt, ObField &field)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *select_stmt = static_cast<ObSelectStmt *>(&basic_stmt);
  if (select_stmt->has_select_into()) { //  for select into, no rows return
    // do nothing.
  } else {
    int64_t size = select_stmt->get_select_item_size();
    common::ObIAllocator &alloc = result_set.get_mem_pool();
    number::ObNumber number;
    number.set_zero();
    if (OB_FAIL(result_set.reserve_field_columns(size))) {
      LOG_WARN("reserve field columns failed", K(ret), K(size));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
      const SelectItem &select_item = select_stmt->get_select_item(i);
      LOG_DEBUG("select item info", K(select_item));
      ObRawExpr *expr = select_item.expr_;
      bool is_ext_field = false;
      ObSqlString composite_field_name;
      if (OB_UNLIKELY(NULL == expr)) {
        ret = OB_ERR_ILLEGAL_ID;
        LOG_WARN("fail to get expr", K(ret), K(i), K(size));
      } else {
        if (ob_is_string_or_lob_type(expr->get_data_type())
            && CS_TYPE_BINARY != expr->get_collation_type()
            && ObCharset::is_valid_collation(collation_type)) {
          field.charsetnr_ = static_cast<uint16_t>(collation_type);
        } else {
          field.charsetnr_ = static_cast<uint16_t>(expr->get_collation_type());
        }
      }

      if (OB_SUCC(ret) && expr->get_result_type().is_ext()) {
#ifdef OB_BUILD_ORACLE_PL
        // error code compiltable with oracle
        if (pl::ObPlJsonUtil::is_pl_jsontype(expr->get_result_type().get_udt_id())) {
          ret = OB_ERR_PL_JSONTYPE_USAGE;
        }
#endif
        if (OB_FAIL(ret)) {
          // do nothing
        } else if ((expr->is_query_ref_expr() && static_cast<ObQueryRefRawExpr*>(expr)->is_cursor())
            || (expr->is_udf_expr() && static_cast<ObUDFRawExpr*>(expr)->get_is_return_sys_cursor())) {
          if (OB_FAIL(ob_write_string(alloc, "SYS_REFCURSOR", field.type_name_))) {
            LOG_WARN("fail to alloc string", K(i), K(field), K(ret));
          }
        } else if (lib::is_oracle_mode() && expr->is_column_ref_expr() &&
                   static_cast<ObColumnRefRawExpr *>(expr)->is_xml_column()) {
          // xmltype is supported, do nothing
        } else if (NULL == context->secondary_namespace_ // pl resolve
                    && NULL == context->session_info_->get_pl_context()) { // pl execute
          is_ext_field = true;
          field.type_.set_collation_type(CS_TYPE_BINARY);
          field.type_.set_collation_level(CS_LEVEL_IMPLICIT);
          field.length_ = OB_MAX_LONGTEXT_LENGTH;
          if (CS_TYPE_BINARY != expr->get_collation_type()
            && ObCharset::is_valid_collation(collation_type)) {
            field.charsetnr_ = static_cast<uint16_t>(collation_type);
          } else {
            field.charsetnr_ = static_cast<uint16_t>(expr->get_collation_type());
          }
          if (OB_FAIL(get_composite_type_field_name(*context->schema_guard_,
                                                    expr->get_result_type().get_udt_id(),
                                                    composite_field_name))) {
            LOG_WARN("get record member name fail.", K(ret), K(composite_field_name));
          }
        }
      }

      if (OB_SUCC(ret)) {
        // Setup field Type and Accuracy
        field.type_.set_type(expr->get_data_type());
        field.accuracy_ = expr->get_accuracy();
        field.flags_ = static_cast<uint16_t>(expr->get_result_flag());
        // Setup Collation and Collation level
        if (ob_is_string_or_lob_type(static_cast<ObObjType>(expr->get_data_type()))
            || ob_is_raw(static_cast<ObObjType>(expr->get_data_type()))
            || ob_is_enum_or_set_type(static_cast<ObObjType>(expr->get_data_type()))) {
          field.type_.set_collation_type(expr->get_collation_type());
          field.type_.set_collation_level(expr->get_collation_level());
        }
        // Setup Scale
        if (ObVarcharType == field.type_.get_type()) {
          field.type_.set_varchar(type_name);
        } else if (ObNumberType == field.type_.get_type()) {
          field.type_.set_number(number);
        }
        if (expr->get_result_type().is_user_defined_sql_type() ||
            expr->get_result_type().is_collection_sql_type() ||
            ((PC_PS_MODE == mode || PC_PL_MODE == mode) && expr->get_result_type().is_geometry() && lib::is_oracle_mode())) {//oracle gis ps protocol
          uint16_t subschema_id = expr->get_result_type().get_subschema_id();
          uint16_t tmp_subschema_id = ObInvalidSqlType;
          uint64_t udt_id = expr->get_result_type().get_udt_id();
          ObSqlUDTMeta udt_meta;
          if (subschema_id == ObXMLSqlType) {
            udt_id = T_OBJ_XML;
          }
          if (expr->get_result_type().is_geometry()) {
            udt_id = T_OBJ_SDO_GEOMETRY;
            field.type_.meta_.set_ext();
            field.accuracy_.set_accuracy(T_OBJ_SDO_GEOMETRY);
          }
          if (OB_FAIL(result_set.get_exec_context().get_subschema_id_by_udt_id(udt_id, tmp_subschema_id))) {
            LOG_WARN("unsupported udt id", K(ret), K(subschema_id));
          } else if (OB_FAIL(result_set.get_exec_context().get_sqludt_meta_by_subschema_id(tmp_subschema_id, udt_meta))) {
            LOG_WARN("failed to get udt meta", K(ret), K(tmp_subschema_id));
          } else if(ObObjUDTUtil::ob_is_supported_sql_udt(udt_meta.udt_id_)) {
            // common udt constructors or functions set udt id , but xml exprs not
            if (udt_meta.udt_id_ == T_OBJ_XML) {
              field.accuracy_.set_accuracy(T_OBJ_XML);
            } else if (udt_id != udt_meta.udt_id_) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("udt id mismarch", K(ret), K(udt_id), K(udt_meta.udt_id_));
            }
            field.type_.set_subschema_id(tmp_subschema_id);
            field.charsetnr_ = CS_TYPE_BINARY;
            field.length_ = OB_MAX_LONGTEXT_LENGTH;
          } else {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("udt type not supported", K(ret), K(tmp_subschema_id));
          }
          if (OB_SUCC(ret)) {
            if (OB_FAIL(ob_write_string(alloc, ObString(udt_meta.udt_name_len_, udt_meta.udt_name_), field.type_name_))) {
              LOG_WARN("fail to alloc string", K(i), K(field), K(ret));
            }
          }
        } else if (expr->get_result_type().is_ext()
                   && OB_INVALID_ID != expr->get_result_type().get_udt_id()
                   && (PL_VARRAY_TYPE == expr->get_result_type().get_extend_type()
                       || PL_NESTED_TABLE_TYPE == expr->get_result_type().get_extend_type()
                       || PL_ASSOCIATIVE_ARRAY_TYPE == expr->get_result_type().get_extend_type()
                       || PL_RECORD_TYPE == expr->get_result_type().get_extend_type())) {
          if (PC_PS_MODE == mode || PC_PL_MODE == mode) {
            const ObUDTTypeInfo *udt_info = NULL;
            const ObSimpleDatabaseSchema *db_schema = NULL;
            uint64_t udt_id = expr->get_result_type().get_udt_id();
            const uint64_t tenant_id = get_tenant_id_by_object_id(udt_id);
            if (OB_FAIL(context->schema_guard_->get_udt_info(tenant_id, udt_id, udt_info))) {
              LOG_WARN("fail to get udt info. ", K(tenant_id), K(udt_id), K(ret));
            } else if (NULL == udt_info) {
              LOG_WARN("udt is invalid. ", K(tenant_id), K(udt_id), K(udt_info), K(ret));
            } else if (OB_FAIL(context->schema_guard_->get_database_schema(
                           tenant_id, udt_info->get_database_id(), db_schema))) {
              LOG_WARN("get database info fail. ", K(tenant_id), K(udt_info->get_database_id()), K(ret));
            } else if (NULL == db_schema) {
              LOG_WARN("database is invalid. ", K(tenant_id), K(udt_id), K(udt_info->get_database_id()), K(ret));
            } else if (OB_FAIL(ob_write_string(alloc, udt_info->get_type_name(), field.type_name_))) {
              LOG_WARN("fail to alloc string", K(udt_info->get_type_name()), K(ret));
            } else if (OB_FAIL(ob_write_string(alloc, db_schema->get_database_name_str(), field.type_owner_))) {
              LOG_WARN("fail to alloc string", K(db_schema->get_database_name_str()), K(ret));
            }
          } else {
            // Text protocol convert extend type field to varchar
            field.type_.set_varchar(type_name);
          }
        } else if (!expr->get_result_type().is_ext() && OB_FAIL(expr->get_length_for_meta_in_bytes(field.length_))) {
          LOG_WARN("get length failed", K(ret), KPC(expr));
        }
      }

      // SELECT ITEM的alias name和expr name规则举例：
      // SELECT field1+field2 AS f1, field1+3, "thanks", field2 AS f2, field3, "hello" as f4, field1+4 as f5 FROM t1
      // "is_alias":true,  "alias_name":"f1", "expr_name":"f1"
      // "is_alias":false, "alias_name":"", "expr_name":"field1+3"
      // "is_alias":false, "alias_name":"", "expr_name":", "thanks"",
      // "is_alias":true,  "alias_name":"f2", "expr_name":"f2",  "column_name":"field2",
      // "is_alias":true,  "alias_name":"field3", "expr_name":"field3",  "column_name":"field3",
      // "is_alias":true,  "alias_name":"f4", "expr_name":"f4"
      // "is_alias":true,  "alias_name":"f5", "expr_name":"f5"
      //
      ObCollationType field_names_collation =
            ObCharset::is_valid_collation(collation_type) ? collation_type : CS_TYPE_UTF8MB4_BIN;
      if (OB_SUCC(ret)) {
        ObSqlString field_name;
        if (composite_field_name.length() > 0) {
          // need record member name
          if (OB_FAIL(field_name.append(select_item.alias_name_))) {
            LOG_WARN("append field name fail.", K(ret), K(select_item.alias_name_), K(composite_field_name));
          } else if (OB_FAIL(field_name.append(composite_field_name.string()))) {
            LOG_WARN("get field name fail.", K(ret), K(select_item.alias_name_), K(composite_field_name));
          }
        }
        if (OB_FAIL(ret)) {
          // do nothing
        } else if (OB_FAIL(ObSQLUtils::copy_and_convert_string_charset(alloc,
                                field_name.length() > 0 ? field_name.string() : select_item.alias_name_,
                                field.cname_, CS_TYPE_UTF8MB4_BIN, field_names_collation))) {
          LOG_WARN("fail to alloc string", K(select_item.alias_name_), K(ret));
        } else {
          field.is_hidden_rowid_ = select_item.is_hidden_rowid_;
          LOG_TRACE("is_hidden_rowid", K(select_item));
        }
      }
      if (OB_SUCC(ret)) {
        bool is_contain_column_ref = false;
        if (expr->is_column_ref_expr()
            || T_FUN_SYS_CALC_UROWID == expr->get_expr_type()
            || T_FUN_SET_TO_STR == expr->get_expr_type()
            || T_FUN_ENUM_TO_STR == expr->get_expr_type()) {
          const TableItem *table_item = NULL;
          ObString column_name(OB_HIDDEN_LOGICAL_ROWID_COLUMN_NAME);
          if ((T_FUN_SET_TO_STR != expr->get_expr_type() &&
              T_FUN_ENUM_TO_STR != expr->get_expr_type()) &&
              (T_FUN_SYS_CALC_UROWID == expr->get_expr_type() ||
               (lib::is_oracle_mode() &&
                ObCharset::case_insensitive_equal(OB_HIDDEN_LOGICAL_ROWID_COLUMN_NAME,
                                                  static_cast<ObColumnRefRawExpr *>(expr)->get_column_name())))) {
            //Although the current implement of rowid does not use mock a column schema, it should
            //be as normal column when displayed externally.
            if (T_FUN_SYS_CALC_UROWID == expr->get_expr_type()) {
              bool got_it = false;
              for (int64_t i = 0; !got_it && OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
                ObRawExpr *param_expr = NULL;
                if (OB_ISNULL(param_expr = expr->get_param_expr(i))) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("get unexpected null", K(ret), K(param_expr), KPC(expr));
                } else if (param_expr->is_column_ref_expr()) {
                  uint64_t table_id = static_cast<ObColumnRefRawExpr *>(param_expr)->get_table_id();
                  table_item = select_stmt->get_table_item_by_id(table_id);
                  got_it = true;
                  is_contain_column_ref = true;
                } else {/*do nothing*/}
              }
            } else {//mock empty column expr for generate table
              is_contain_column_ref = true;
              ObColumnRefRawExpr *column_expr = static_cast<ObColumnRefRawExpr *>(expr);
              table_item = select_stmt->get_table_item_by_id(column_expr->get_table_id());
            }
          } else {
            ObColumnRefRawExpr *column_expr = NULL;
            if (OB_FAIL(ObRawExprUtils::get_col_ref_expr_recursively(expr, column_expr))) {
              LOG_WARN("failed to get col ref expr recursively", K(ret));
            } else if (OB_NOT_NULL(column_expr)) {
              is_contain_column_ref = true;
              uint64_t table_id = column_expr->get_table_id();
              uint64_t column_id = column_expr->get_column_id();
              ColumnItem *column_item = select_stmt->get_column_item_by_id(table_id, column_id);
              if (OB_ISNULL(column_item)) {
                ret = OB_ERR_ILLEGAL_ID;
                LOG_WARN("fail to get column item by id.", K(ret), K(table_id), K(column_id));
              } else {
                column_name = column_item->column_name_;
                table_item = select_stmt->get_table_item_by_id(table_id);
                if (column_expr->is_unique_key_column()) {
                  field.flags_ |= UNIQUE_KEY_FLAG;
                }
                if (column_expr->is_mul_key_column()) {
                  field.flags_ |= MULTIPLE_KEY_FLAG;
                }
              }
            }
          }
          if (OB_SUCC(ret) && is_contain_column_ref) {
            if (OB_ISNULL(table_item)) {
              ret = OB_ERR_ILLEGAL_ID;
              LOG_WARN("fail to get table item by id.", K(ret));
            } else if (OB_FAIL(ob_write_string(alloc,
                                                column_name,
                                                field.org_cname_))) {
              LOG_WARN("fail to alloc", K(ret), K(column_name));
            } else if (OB_FAIL(ObSQLUtils::copy_and_convert_string_charset(
                                    alloc, table_item->database_name_, field.dname_,
                                    CS_TYPE_UTF8MB4_BIN, field_names_collation))) {
              LOG_WARN("fail to alloc string", K(ret), K(table_item->database_name_));
            } else if (table_item->alias_name_.length() > 0) {
              if (OB_FAIL(ObSQLUtils::copy_and_convert_string_charset(
                                    alloc, table_item->alias_name_, field.tname_,
                                    CS_TYPE_UTF8MB4_BIN, field_names_collation))) {
                LOG_WARN("fail to alloc string", K(ret), K(table_item->alias_name_));
              }
            } else {
              if (OB_FAIL(ObSQLUtils::copy_and_convert_string_charset(
                                    alloc, table_item->table_name_, field.tname_,
                                    CS_TYPE_UTF8MB4_BIN, field_names_collation))) {
                LOG_WARN("fail to alloc string", K(ret), K(table_item->table_name_));
              }
            }
            if (OB_FAIL(ret)) {
            } else if (OB_FAIL(ob_write_string(alloc, table_item->table_name_, field.org_tname_))) {
              LOG_WARN("fail to alloc string", K(ret), K(table_item->table_name_));
            }
          }
        }
      }
      if (OB_SUCC(ret) && !(PC_PS_MODE == mode || PC_PL_MODE == mode)) {
        void *buf = NULL;
        if (OB_ISNULL(buf = alloc.alloc(sizeof(ObParamedSelectItemCtx)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(ret));
        } else {
          ObSqlString paramed_field_name;
          field.paramed_ctx_ = new(buf) ObParamedSelectItemCtx();
          if (composite_field_name.length() > 0) {
            // need record member name
            if (OB_FAIL(paramed_field_name.append(select_item.paramed_alias_name_))) {
              LOG_WARN("append paramed field fail.", K(ret), K(select_item.paramed_alias_name_), K(composite_field_name));
            } else if (OB_FAIL(paramed_field_name.append(composite_field_name.string()))) {
              LOG_WARN("get field name fail.", K(ret), K(select_item.paramed_alias_name_), K(composite_field_name));
            }
          }
          if (OB_FAIL(ret)) {
            // do nothing
          } else if (OB_FAIL(ob_write_string(alloc,
                                             paramed_field_name.length() > 0
                                               ? paramed_field_name.string()
                                               : select_item.paramed_alias_name_,
                                             field.paramed_ctx_->paramed_cname_))) {
            LOG_WARN("failed to copy paramed cname", K(ret));
          } else if (OB_FAIL(field.paramed_ctx_->param_str_offsets_.assign(
                                                  select_item.questions_pos_))) {
            LOG_WARN("failed to copy param_str_offsets_", K(ret));
          } else if (OB_FAIL(field.paramed_ctx_->param_idxs_.assign(select_item.params_idx_))) {
            LOG_WARN("failed to copy param idxs", K(ret));
          } else {
            field.paramed_ctx_->neg_param_idxs_ = select_item.neg_param_idx_;
            field.paramed_ctx_->esc_str_flag_ = select_item.esc_str_flag_;
            field.paramed_ctx_->need_check_dup_name_ = select_item.need_check_dup_name_;
            // 如果投影列是一个column
            field.paramed_ctx_->is_column_field_ = (T_REF_COLUMN == expr->get_expr_type());
            field.is_paramed_select_item_ = true;
          }
        }
      }
      LOG_TRACE("column field info", K(field), K(select_item));
      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_FAIL(result_set.add_field_column(field))) {
        LOG_WARN("failed to add field column", K(ret));
      } else {
        field.cname_.assign(NULL, 0);
        field.org_cname_.assign(NULL, 0);
        field.dname_.assign(NULL, 0);
        field.tname_.assign(NULL, 0);
        field.org_tname_.assign(NULL, 0);
        field.type_.reset();
        field.type_.set_type(ObExtendType);
        field.paramed_ctx_ = NULL;
      }
    }
  }
  return ret;
}

int ObSql::fill_result_set(const ObPsStmtId stmt_id, const ObPsStmtInfo &stmt_info, ObResultSet &result)
{
  int ret = OB_SUCCESS;
  result.set_statement_id(stmt_id);
  result.set_stmt_type(stmt_info.get_stmt_type());
  result.set_literal_stmt_type(stmt_info.get_literal_stmt_type());
  const ObPsSqlMeta &sql_meta = stmt_info.get_ps_sql_meta();
  result.set_p_param_fileds(const_cast<common::ParamsFieldIArray *>(&sql_meta.get_param_fields()));
  result.set_p_column_fileds(const_cast<common::ParamsFieldIArray *>(&sql_meta.get_column_fields()));
  //ObPsSqlMeta::const_column_iterator column_iter = sql_meta.column_begin();
  //result.reserve_field_columns(sql_meta.get_column_size());
  //for (; OB_SUCC(ret) && column_iter != sql_meta.column_end(); ++column_iter) {
    //if (OB_ISNULL(column_iter) || OB_ISNULL(*column_iter)) {
      //ret = OB_ERR_UNEXPECTED;
      //LOG_WARN("column iter is null", K(ret), K(column_iter));
    //} else if (OB_FAIL(result.add_field_column(**column_iter))) {
      //LOG_WARN("add column field failed", K(ret));
    //}
  //}

  //ObPsSqlMeta::const_param_iterator param_iter = sql_meta.param_begin();
  //for (; OB_SUCC(ret) && param_iter != sql_meta.param_end(); ++param_iter) {
    //if (OB_ISNULL(param_iter) || OB_ISNULL(param_iter)) {
      //ret = OB_ERR_UNEXPECTED;
      //LOG_WARN("param iter is null", K(ret), K(param_iter));
    //} else if (OB_FAIL(result.add_param_column(**param_iter))) {
      //LOG_WARN("add param field faield", K(ret));
    //}
  //}
  return ret;
}

int ObSql::do_add_ps_cache(const PsCacheInfoCtx &info_ctx,
                           ObSchemaGetterGuard &schema_guard,
                           ObResultSet &result)
{
  int ret = OB_SUCCESS;
  bool is_contain_tmp_tbl = false;
  ObSQLSessionInfo &session = result.get_session();
  ObPsCache *ps_cache = session.get_ps_cache();
  uint64_t db_id = OB_INVALID_ID;
  (void)session.get_database_id(db_id);
  if (OB_ISNULL(ps_cache)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ps plan cache should not be null", K(ret));
  } else if (lib::is_mysql_mode() &&
      OB_FAIL(check_contain_temporary_table(schema_guard, result, is_contain_tmp_tbl))) {
    LOG_WARN("failed to check contain temporary table", K(ret));
  } else {
    ObPsStmtItem *ps_stmt_item = NULL;
    ObPsStmtInfo *ref_stmt_info = NULL;
    bool duplicate_prepare = false;
    ObPsSqlKey ps_key;
    ps_key.db_id_ = db_id;
    ps_key.ps_sql_ = info_ctx.normalized_sql_;
    ps_key.is_client_return_hidden_rowid_ = session.is_client_return_rowid();
    // add stmt item
    if (OB_FAIL(ps_cache->get_or_add_stmt_item(ps_key,
                                               is_contain_tmp_tbl,
                                               ps_stmt_item))) {
      LOG_WARN("get or create stmt item faield", K(ret), K(db_id), K(info_ctx.normalized_sql_));
    } else if (OB_FAIL(ps_cache->get_or_add_stmt_info(info_ctx,
                                                      result,
                                                      schema_guard,
                                                      ps_stmt_item,
                                                      ref_stmt_info))) {
      LOG_WARN("get or create stmt info failed", K(ret), K(ps_stmt_item), K(db_id), K(info_ctx));
    } else if (OB_ISNULL(ps_stmt_item) || OB_ISNULL(ref_stmt_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("stmt_item or stmt_info is NULL", K(ret), KP(ps_stmt_item), KP(ref_stmt_info));
    } else {
      ref_stmt_info->set_literal_stmt_type(result.get_literal_stmt_type());
    }
    if (NULL != ref_stmt_info) {
      ref_stmt_info->set_is_sensitive_sql(info_ctx.is_sensitive_sql_);
    }
    //add session info
    if (OB_SUCC(ret)) {
      ObPsStmtId inner_stmt_id = ps_stmt_item->get_ps_stmt_id();
      ObPsStmtId client_stmt_id = OB_INVALID_ID;
      if (OB_FAIL(session.prepare_ps_stmt(inner_stmt_id,
                                          ref_stmt_info,
                                          client_stmt_id,
                                          duplicate_prepare,
                                          info_ctx.is_inner_sql_))) {
        LOG_WARN("prepare_ps_stmt failed", K(ret), K(inner_stmt_id), K(client_stmt_id));
      } else {
        result.set_statement_id(client_stmt_id);
        result.set_stmt_type(info_ctx.stmt_type_);
        LOG_TRACE("add ps session info", K(ret), K(*ref_stmt_info), K(client_stmt_id),
                                        K(*ps_stmt_item), K(session.get_sessid()));
      }
    }
    if (OB_FAIL(ret) || duplicate_prepare) { //dec ref count
      if (NULL != ps_stmt_item) {
        if (NULL != ref_stmt_info) {
          ObPsStmtId inner_stmt_id = ps_stmt_item->get_ps_stmt_id();
          ps_cache->deref_stmt_info(inner_stmt_id); //需要决定是否摘除
        }
        ps_stmt_item->dec_ref_count();
      }
    }
  }
  return ret;
}

int ObSql::check_contain_temporary_table(share::schema::ObSchemaGetterGuard &schema_guard,
                                         ObResultSet &result,
                                         bool &is_contain_tmp_tbl)
{
  int ret = OB_SUCCESS;
  is_contain_tmp_tbl = false;
  const ObTableSchema *table_schema = nullptr;
  for (int64_t i = 0; OB_SUCC(ret) && !is_contain_tmp_tbl && i < result.get_ref_objects().count(); i++) {
    table_schema = nullptr;
    ObSchemaObjVersion &obj_version = result.get_ref_objects().at(i);
    if (DEPENDENCY_TABLE != obj_version.object_type_) {
      // do nothing
    } else if (OB_FAIL(schema_guard.get_table_schema(MTL_ID(),
                                              obj_version.object_id_,
                                              table_schema))) {
      LOG_WARN("failed to get table schema", K(ret), K(obj_version), K(table_schema));
    } else if (nullptr == table_schema) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get an unexpected null schema", K(ret), K(table_schema));
    } else if (table_schema->is_tmp_table()) {
      is_contain_tmp_tbl = true;
      break;
    }
  }
  return ret;
}

int ObSql::do_real_prepare(const ObString &sql,
                           ObSqlCtx &context,
                           ObResultSet &result,
                           bool is_inner_sql)
{
  int ret = OB_SUCCESS;
  bool enable_udr = false;
  ParseResult parse_result;
  ObStmt *basic_stmt = NULL;
  stmt::StmtType stmt_type = stmt::T_NONE;
  int64_t param_cnt = 0;
  PsCacheInfoCtx info_ctx;
  ObUDRItemMgr::UDRItemRefGuard item_guard;
  ObIAllocator &allocator = result.get_mem_pool();
  ObSQLSessionInfo &session = result.get_session();
  ObExecContext &ectx = result.get_exec_context();
  ObParser parser(allocator, session.get_sql_mode(), session.get_charsets4parser());
  ParseMode parse_mode = context.is_dbms_sql_ ? DBMS_SQL_MODE :
                         (context.is_dynamic_sql_  || !is_inner_sql) ? DYNAMIC_SQL_MODE :
                         session.is_for_trigger_package() ? TRIGGER_MODE : STD_MODE;

  // normal ps sql also a dynamic sql, we adjust is_dynamic_sql_ for normal ps sql parser.
  context.is_dynamic_sql_ = !context.is_dynamic_sql_ ? !is_inner_sql : context.is_dynamic_sql_;

  bool is_from_pl = (NULL != context.secondary_namespace_ || result.is_simple_ps_protocol());
  ObPsPrepareStatusGuard ps_status_guard(session);
  ObPlanCacheCtx pc_ctx(sql, PC_PS_MODE, allocator, context, ectx,
                        session.get_effective_tenant_id());
  ParamStore param_store( (ObWrapperAllocator(&allocator)) );
  pc_ctx.set_is_inner_sql(is_inner_sql);

  CHECK_COMPATIBILITY_MODE(context.session_info_);
  enable_udr = context.get_enable_user_defined_rewrite();
  if (OB_ISNULL(context.session_info_) || OB_ISNULL(context.schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is NULL", K(ret));
  } else if (OB_FAIL(parser.parse(sql,
                                  parse_result,
                                  parse_mode))) {
    LOG_WARN("generate syntax tree failed",
             "sql", parse_result.contain_sensitive_data_ ? ObString(OB_MASKED_STR) : sql, K(ret));
  } else if (is_mysql_mode()
             && ObSQLUtils::is_mysql_ps_not_support_stmt(parse_result)) {
    ret = OB_ER_UNSUPPORTED_PS;
    LOG_WARN("This command is not supported in the prepared statement protocol yet", K(ret));
  }  else if (parse_result.question_mark_ctx_.count_ > common::OB_MAX_PS_PARAM_COUNT) {
    ret = OB_ERR_PS_TOO_MANY_PARAM;
    LOG_WARN("There are too many parameters in the prepared statement", K(ret));
    LOG_USER_ERROR(OB_ERR_PS_TOO_MANY_PARAM);
  } else {
    ps_status_guard.is_varparams_sql_prepare(is_from_pl, parse_result.question_mark_ctx_.count_ > 0 ? true : false);
  }
  context.is_sensitive_ |= parse_result.contain_sensitive_data_;

  OZ (ObResolverUtils::resolve_stmt_type(parse_result, stmt_type));

  if (OB_FAIL(ret)) {
  } else if (result.is_simple_ps_protocol() // simple_ps_protocol only do parse
             // for anonymous block, only parser in prepare of preexecute
             || (stmt::T_ANONYMOUS_BLOCK == stmt_type
                 && context.is_prepare_protocol_
                 && context.is_prepare_stage_
                 && context.is_pre_execute_)) {
    param_cnt = parse_result.question_mark_ctx_.count_;
    info_ctx.normalized_sql_ = sql;
    if (stmt::T_ANONYMOUS_BLOCK == stmt_type
                 && context.is_prepare_protocol_
                 && context.is_prepare_stage_
                 && context.is_pre_execute_) {
      OZ (result.reserve_param_columns(param_cnt));
      for (int64_t i = 0; OB_SUCC(ret) && i < param_cnt; ++i) {
        ObField param_field;
        param_field.type_.set_type(ObIntType);
        param_field.cname_ = ObString::make_string("?");
        OZ (result.add_param_column(param_field), K(param_field), K(i), K(param_cnt));
      }
    }
  } else {
    if (context.is_dynamic_sql_ && !context.is_dbms_sql_) {
      parse_result.input_sql_ = parse_result.no_param_sql_;
      parse_result.input_sql_len_ = parse_result.no_param_sql_len_;
    }
    if (OB_FAIL(generate_stmt(parse_result, NULL, context, allocator, result, basic_stmt))) {
      LOG_WARN("generate stmt failed", K(ret));
    } else if (!is_from_pl
              && !is_inner_sql
              && !(ObStmt::is_dml_write_stmt(stmt_type) && // returning into from oci not supported
                   static_cast<ObDelUpdStmt*>(basic_stmt)->get_returning_into_exprs().count() > 0)
              && enable_udr
              && OB_FAIL(ObUDRUtils::match_udr_item(sql, session, ectx, allocator, item_guard))) {
      if (!ObSQLUtils::check_need_disconnect_parser_err(ret)) {
        ectx.set_need_disconnect(false);
      }
      LOG_WARN("failed to match rewrite rule", K(ret));
    } else if (ObStmt::is_dml_stmt(stmt_type)
              && NULL == item_guard.get_ref_obj()
              && !ObStmt::is_show_stmt(stmt_type)
              && !is_inner_sql
              && !is_from_pl
              && !(ObStmt::is_dml_write_stmt(stmt_type) && // returning into from oci not need parameterization
                   static_cast<ObDelUpdStmt*>(basic_stmt)->get_returning_into_exprs().count() > 0)) {
      bool is_transform_outline = false;
      if (OB_FAIL(ObSqlParameterization::parameterize_syntax_tree(allocator,
                                                                  is_transform_outline,
                                                                  pc_ctx,
                                                                  parse_result.result_tree_,
                                                                  param_store,
                                                                  session.get_charsets4parser()))) {
        LOG_INFO("parameterize syntax tree failed", K(ret));
        pc_ctx.ps_need_parameterized_ = false;
        ret = OB_SUCCESS;
      }
      if (OB_SUCC(ret)) {
        if (!pc_ctx.ps_need_parameterized_) {
          pc_ctx.fixed_param_idx_.reset();
          pc_ctx.fp_result_.raw_params_.reset();
        } else {
          info_ctx.no_param_sql_ = pc_ctx.sql_ctx_.spm_ctx_.bl_key_.constructed_sql_;
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(basic_stmt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("generate stmt success, but stmt is NULL", K(ret));
    } else if (OB_ISNULL(basic_stmt->get_query_ctx())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("query ctx is null", K(ret));
    } else if (stmt::T_CALL_PROCEDURE == basic_stmt->get_stmt_type()
               && FALSE_IT(result.set_cmd(dynamic_cast<ObICmd*>(basic_stmt)))) {
    } else if (OB_FAIL(fill_result_set(result, &context, PC_PS_MODE, *basic_stmt))) {
      LOG_WARN("Failed to fill result set", K(ret));
    } else if (OB_ISNULL(result.get_param_fields())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(result.get_param_fields()), K(ret));
    } else {
      param_cnt = result.get_param_fields()->count();
      stmt_type = basic_stmt->get_stmt_type();
      //如果是内部sql, 比如pl内部sql, 需要使用格式化后的文本串,
      //因为需要将pl中sql的变量替换为标准的ps文本进行硬解析
      //而外部请求的ps文本不能格式化, 因为在解析ps execute包时需要进行checksum校验,
      //需要确保prepare的文本与客户端发过来的一致
      if (is_inner_sql) {
        // pl
        info_ctx.normalized_sql_ = basic_stmt->get_query_ctx()->get_sql_stmt();
      } else if (result.is_returning() && ObStmt::is_dml_write_stmt(stmt_type)) {
        info_ctx.normalized_sql_ = sql;
        info_ctx.no_param_sql_ = basic_stmt->get_query_ctx()->get_sql_stmt();
      } else {
        info_ctx.normalized_sql_ = sql;
      }
    }
    if (OB_SUCC(ret)) {
      if (basic_stmt->is_insert_stmt() || basic_stmt->is_update_stmt() || basic_stmt->is_delete_stmt()) {
        ObDelUpdStmt *dml_stmt = static_cast<ObDelUpdStmt*>(basic_stmt);
        if (dml_stmt->get_returning_into_exprs().count() != 0 && dml_stmt->is_returning()) {
          info_ctx.num_of_returning_into_ = dml_stmt->get_returning_into_exprs().count();
        }
      }
    }

    LOG_INFO("generate new stmt", K(ret), K(param_cnt), K(stmt_type), K(info_ctx.no_param_sql_),
             K(info_ctx.normalized_sql_), K(info_ctx.num_of_returning_into_),
             "sql", context.is_sensitive_ ? ObString(OB_MASKED_STR) : sql);
  }
  if (OB_SUCC(ret)) {
    info_ctx.param_cnt_ = param_cnt;
    info_ctx.stmt_type_ = stmt_type;
    info_ctx.is_inner_sql_ = is_inner_sql;
    info_ctx.is_sensitive_sql_ = context.is_sensitive_;
    info_ctx.raw_params_ = &pc_ctx.fp_result_.raw_params_;
    info_ctx.fixed_param_idx_ = &pc_ctx.fixed_param_idx_;
    info_ctx.raw_sql_.assign_ptr(sql.ptr(), sql.length());
    if (OB_FAIL(do_add_ps_cache(info_ctx, *context.schema_guard_, result))) {
      LOG_WARN("add to ps plan cache failed",
               K(ret), K(info_ctx.normalized_sql_), K(param_cnt));
    }
  }
  //if the error code is ob_timeout, we add more error info msg for dml query.
  if (OB_TIMEOUT == ret &&
      session.is_user_session() &&
      parse_result.result_tree_ != NULL &&
      parse_result.result_tree_->children_ != NULL &&
      parse_result.result_tree_->num_child_ >= 1 &&
      (parse_result.result_tree_->children_[0]->type_ == T_EXPLAIN ||
       IS_DML_STMT(parse_result.result_tree_->children_[0]->type_) ||
       IS_SHOW_STMT(parse_result.result_tree_->children_[0]->type_))) {
    LOG_USER_ERROR(OB_TIMEOUT, THIS_WORKER.get_timeout_ts() - session.get_query_start_time());
  }
  LOG_INFO("add ps cache", K(info_ctx.normalized_sql_), K(param_cnt), K(ret));
  return ret;
}


ObSql::TimeoutGuard::TimeoutGuard(ObSQLSessionInfo &session)
  : session_(session)
{
  int ret = OB_SUCCESS;
  worker_timeout_ = THIS_WORKER.get_timeout_ts();
}

ObSql::TimeoutGuard::~TimeoutGuard()
{
  int ret = OB_SUCCESS;
  if (THIS_WORKER.get_timeout_ts() != worker_timeout_) {
    THIS_WORKER.set_timeout_ts(worker_timeout_);
  }
}

int ObSql::set_timeout_for_pl(ObSQLSessionInfo &session_info, int64_t &abs_timeout_us)
{
  int ret = OB_SUCCESS;
  int64_t query_timeout;
  if (THIS_WORKER.is_timeout()) {
    ret = OB_TIMEOUT;
    LOG_WARN("already timeout", K(ret), K(abs_timeout_us), K(THIS_WORKER.get_timeout_ts()));
  } else if (OB_FAIL(session_info.get_query_timeout(query_timeout))) {
    // do nothing
  } else {
    OX (abs_timeout_us = session_info.get_query_start_time() > 0
                         ? session_info.get_query_start_time() + query_timeout
                           : ObTimeUtility::current_time() + query_timeout);
    if (THIS_WORKER.get_timeout_ts() > abs_timeout_us) {
      OX (THIS_WORKER.set_timeout_ts(abs_timeout_us));
    }
  }
  return ret;
}

/*
 * sql: pl 中的 sql 语句
 * PLPrepareCtx: prepare 用到的相关信息
 * PLPrepareResult: prepare 后的输出结果
 */

int ObSql::handle_pl_prepare(const ObString &sql,
                             ObSPIService::PLPrepareCtx &pl_prepare_ctx,
                             ObSPIService::PLPrepareResult &pl_prepare_result)
{
  int ret = OB_SUCCESS;
  ObString cur_query;
  ObString trimed_stmt = const_cast<ObString &>(sql).trim();
  ObSqlCtx &context = pl_prepare_result.sql_ctx_;
  int64_t param_cnt = 0;
  ObString normalized_sql;
  ParseResult parse_result;
  ObStmt *basic_stmt = NULL;
  int64_t cur_timeout_us = 0;
  stmt::StmtType stmt_type = stmt::T_NONE;
  ObSchemaGetterGuard &schema_guard = pl_prepare_result.schema_guard_;
  ObSQLSessionInfo &sess = pl_prepare_ctx.sess_info_;
  TimeoutGuard timeout_guard(sess);
  int64_t old_query_start_time = sess.get_query_start_time();
  sess.set_query_start_time(ObTimeUtility::current_time());
  CK (OB_NOT_NULL(pl_prepare_result.get_allocator()));
  CK (OB_NOT_NULL(pl_prepare_result.result_set_));
  if (OB_SUCC(ret)) {
    ObIAllocator &allocator = *pl_prepare_result.get_allocator();
    ObParser parser(allocator, sess.get_sql_mode(), sess.get_charsets4parser());
    ParseMode parse_mode = pl_prepare_ctx.is_dbms_sql_ ? DBMS_SQL_MODE :
                          pl_prepare_ctx.is_dynamic_sql_ ? DYNAMIC_SQL_MODE :
                          sess.is_for_trigger_package() ? TRIGGER_MODE : STD_MODE;

    context.is_dynamic_sql_ = pl_prepare_ctx.is_dynamic_sql_;
    context.is_dbms_sql_ = pl_prepare_ctx.is_dbms_sql_;
    context.is_cursor_ = pl_prepare_ctx.is_cursor_;
    context.secondary_namespace_ = pl_prepare_ctx.secondary_ns_;
    context.session_info_ = &sess;
    context.disable_privilege_check_ = OB_SYS_TENANT_ID == sess.get_priv_tenant_id()
                                        ? PRIV_CHECK_FLAG_DISABLE
                                        : PRIV_CHECK_FLAG_IN_PL;
    context.exec_type_ = PLSql;
    context.is_prepare_protocol_ = true;
    context.is_prepare_stage_ = true;

    if (OB_FAIL(ob_write_string(allocator, sess.get_current_query_string(), cur_query))) {
      LOG_WARN("failed to write string", K(ret));
    }

    if (OB_SUCC(ret)) {
      WITH_CONTEXT(pl_prepare_result.mem_context_) {
          ObResultSet &result = *pl_prepare_result.result_set_;
          LinkExecCtxGuard link_guard(result.get_session(), result.get_exec_context());
          if (trimed_stmt.empty()) {
            ret = OB_ERR_EMPTY_QUERY;
            LOG_WARN("query is empty", K(ret));
          } else if (OB_FAIL(set_timeout_for_pl(sess, cur_timeout_us))) {
            LOG_WARN("failed to set timeout for pl", K(ret));
          } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
                                  sess.get_effective_tenant_id(), schema_guard))) {
            LOG_WARN("failed to get tenant schema guard", K(ret));
          } else if (FALSE_IT(context.schema_guard_ = &schema_guard)) {
          } else if (OB_FAIL(init_result_set(context, result))) {
            LOG_WARN("failed to init result set", K(ret));
          } else if (OB_FAIL(sess.store_query_string(sql))) {
            LOG_WARN("store query string fail", K(ret));
          } else if (OB_FAIL(parser.parse(sql, parse_result, parse_mode,
                                          false, false, true, pl_prepare_ctx.is_dbms_sql_))) {
            LOG_WARN("generate syntax tree failed", K(ret),
                     "sql", parse_result.contain_sensitive_data_ ? ObString(OB_MASKED_STR) : sql);
          } else if (is_mysql_mode() && ObSQLUtils::is_mysql_ps_not_support_stmt(parse_result)) {
            ret = OB_ER_UNSUPPORTED_PS;
            LOG_WARN("This command is not supported in the prepared statement protocol yet", K(ret));
          } else if (NULL == pl_prepare_ctx.secondary_ns_ && !pl_prepare_ctx.is_dynamic_sql_) {
            result.set_simple_ps_protocol();
          }
          context.is_sensitive_ |= parse_result.contain_sensitive_data_;

          if (OB_FAIL(ret)) {
            // do nothing
          } else if (OB_FAIL(ObResolverUtils::resolve_stmt_type(parse_result, stmt_type))) {
            LOG_WARN("failed to resolve stmt type", K(ret));
          } else if (FALSE_IT(result.set_stmt_type(stmt_type))) {
          } else if (result.is_simple_ps_protocol()
                    || (stmt::T_ANONYMOUS_BLOCK == stmt_type && context.is_prepare_protocol_
                        && context.is_prepare_stage_ && context.is_pre_execute_)) {
            if (parse_result.is_dynamic_sql_) {
              context.is_dynamic_sql_ = true;
            }
            bool for_update = false;
            if (stmt::T_SELECT == stmt_type &&
                PARSE_SELECT_MAX_IDX == parse_result.result_tree_->children_[0]->num_child_ &&
                NULL != parse_result.result_tree_->children_[0]->children_[PARSE_SELECT_FOR_UPD]) {
              for_update = true;
            }
            result.get_external_retrieve_info().is_select_for_update_ = for_update;
            param_cnt = parse_result.question_mark_ctx_.count_;
            normalized_sql = context.is_dynamic_sql_ && parse_result.no_param_sql_len_ > 0
              ? ObString(parse_result.no_param_sql_len_, parse_result.no_param_sql_) : sql;
            if (stmt::T_ANONYMOUS_BLOCK == stmt_type && context.is_prepare_protocol_
                && context.is_prepare_stage_ && context.is_pre_execute_) {
              OZ (result.reserve_param_columns(param_cnt));
              for (int64_t i = 0; OB_SUCC(ret) && i < param_cnt; ++i) {
                ObField param_field;
                param_field.type_.set_type(ObIntType);
                param_field.cname_ = ObString::make_string("?");
                OZ (result.add_param_column(param_field), K(param_field), K(i), K(param_cnt));
              }
            }
          } else {
            if (parse_result.is_dynamic_sql_) {
              context.is_dynamic_sql_ = true;
            }
            if (context.is_dynamic_sql_ && !context.is_dbms_sql_) {
              parse_result.input_sql_ = parse_result.no_param_sql_;
              parse_result.input_sql_len_ = parse_result.no_param_sql_len_;
            }
            if (OB_FAIL(generate_stmt(parse_result, NULL, context, allocator, result, basic_stmt))) {
              LOG_WARN("generate stmt failed", K(ret));
            } else if (OB_ISNULL(basic_stmt)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("generate stmt success, but stmt is NULL", K(ret));
            } else if (OB_ISNULL(basic_stmt->get_query_ctx())) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("query ctx is null", K(ret));
            } else if (stmt::T_CALL_PROCEDURE == basic_stmt->get_stmt_type()
                      && FALSE_IT(result.set_cmd(dynamic_cast<ObICmd*>(basic_stmt)))) {
            } else if (OB_FAIL(fill_result_set(result, &context, PC_PL_MODE, *basic_stmt))) {
              LOG_WARN("Failed to fill result set", K(ret));
            } else if (OB_ISNULL(result.get_param_fields())) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("invalid argument", K(result.get_param_fields()), K(ret));
            } else {
              normalized_sql = basic_stmt->get_query_ctx()->get_sql_stmt();
            }
          }

          if (OB_FAIL(ret)) {
            // do nothing
          } else if (OB_FAIL(ob_write_string(allocator, normalized_sql, result.get_stmt_ps_sql(), true))) {
            LOG_WARN("failed to write string", K(trimed_stmt), K(ret));
          }
          int tmp_ret = OB_SUCCESS;
          if ((tmp_ret = sess.store_query_string(cur_query)) != OB_SUCCESS) {
            LOG_WARN("failed to store query string", K(ret), K(tmp_ret));
            ret = OB_SUCCESS == ret ?  tmp_ret : ret;
          }
      }
    }
  }
  sess.set_query_start_time(old_query_start_time);
  return ret;
}

int ObSql::handle_sql_execute(const ObString &sql,
                              ObSqlCtx &context,
                              ObResultSet &result,
                              ParamStore &org_params,
                              PlanCacheMode mode)
{
  int ret = OB_SUCCESS;
  int get_plan_err = OB_SUCCESS;
  bool use_plan_cache = false;
  ObIAllocator &allocator = result.get_mem_pool();
  ObSQLSessionInfo *session = context.session_info_;
  ObExecContext &ectx = result.get_exec_context();
  ObPhysicalPlanCtx *pctx = ectx.get_physical_plan_ctx();
  ParamStore params( (ObWrapperAllocator(allocator)) );
  ParamStore *ab_params = NULL;

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(session) || OB_ISNULL(session->get_plan_cache())) {
    ret = OB_INVALID_ARGUMENT;
  } else if ((mode == PC_PS_MODE || mode == PC_PL_MODE) && OB_ISNULL(pctx)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    use_plan_cache = session->get_local_ob_enable_plan_cache();
  }

  ObPlanCacheCtx pc_ctx(sql, mode, allocator, context, ectx, session->get_effective_tenant_id());

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (mode == PC_PL_MODE) {
    if (OB_FAIL(reconstruct_pl_params_store(allocator, context, org_params, params, ab_params))) {
      LOG_WARN("failed to reconstruct pl params", K(ret));
    } else if (context.is_batch_params_execute() && OB_ISNULL(ab_params)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pl ab params is null", K(ret));
    } else if (OB_FAIL(construct_param_store(params, pctx->get_param_store_for_update()))) {
      LOG_WARN("construct param store failed", K(ret));
    } else if (OB_FAIL(construct_parameterized_params(params, pc_ctx))) {
      LOG_WARN("construct parameterized params failed", K(ret));
    } else {
      pc_ctx.normal_parse_const_cnt_ = params.count();
      pc_ctx.set_is_parameterized_execute();
      pc_ctx.ab_params_ = ab_params;
    }
  } else if (mode == PC_PS_MODE) {
    // TODO, not support ps now.
    ret = OB_ERR_UNEXPECTED;
  }

  if (OB_SUCC(ret)) {
    if (!use_plan_cache) {
      // do nothing
    } else if (OB_FAIL(pc_get_plan_and_fill_result(pc_ctx, result,
                         get_plan_err, ectx.get_need_disconnect_for_update()))) {
      LOG_WARN("failed to get plan", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (!result.get_is_from_plan_cache()) {
      if (mode == PC_PS_MODE || mode == PC_PL_MODE) {
        pctx->get_param_store_for_update().reset();
      }
      if (OB_FAIL(handle_physical_plan(sql, context, result, pc_ctx, get_plan_err))) {
        if (OB_ERR_PROXY_REROUTE == ret) {
          LOG_DEBUG("fail to handle physical plan", K(ret));
        } else {
          LOG_WARN("fail to handle physical plan", K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret) && !context.is_text_ps_mode_) {
    if (OB_FAIL(after_get_plan(pc_ctx, *session, result.get_physical_plan(),
                result.get_is_from_plan_cache(), &params, pc_ctx.exec_ctx_.get_min_cluster_version()))) {
      LOG_WARN("fail to handle after get plan", K(ret));
    }
  }

  return ret;
}

/*!
 * sql: 需要被执行的sql语句
 * params: 当前sql语句的参数列表
 * res: 直接结果集
 */
// TODO remove is_prepare_protocol and is_dynamic_sql
int ObSql::handle_pl_execute(const ObString &sql,
                             ObSQLSessionInfo &session,
                             ParamStore &params,
                             ObResultSet &result,
                             ObSqlCtx &context,
                             bool is_prepare_protocol,
                             bool is_dynamic_sql)
{
  int ret = OB_SUCCESS;
  int get_plan_err = OB_SUCCESS;
  TimeoutGuard timeout_guard(session);
  LinkExecCtxGuard link_guard(result.get_session(), result.get_exec_context());
  int64_t cur_timeout_us = 0;
  context.session_info_ = &session;
  ObIAllocator &allocator = result.get_mem_pool();
  ObExecContext &ectx = result.get_exec_context();
  ObPhysicalPlanCtx *pctx = NULL;

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(result.init())) {
    LOG_WARN("failed to init result_set", K(ret));
  } else if (OB_FAIL(init_result_set(context, result))) {
    LOG_WARN("failed to init result set", K(ret));
  } else {
    context.cur_sql_ = sql;
    context.is_from_pl_ = true;
    context.is_dynamic_sql_ = is_dynamic_sql;
    context.is_prepare_protocol_ = is_prepare_protocol;
    context.spm_ctx_.bl_key_.db_id_ = session.get_database_id();
    context.disable_privilege_check_ = OB_SYS_TENANT_ID == session.get_priv_tenant_id()
                                          ? PRIV_CHECK_FLAG_DISABLE
                                          : PRIV_CHECK_FLAG_IN_PL;
    pctx = ectx.get_physical_plan_ctx();
    int64_t local_tenant_schema_version = -1;
    int64_t local_sys_schema_version = -1;
    if (OB_ISNULL(context.schema_guard_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema guard is null");
    } else if (OB_FAIL(context.schema_guard_->get_schema_version(session.get_effective_tenant_id(), local_tenant_schema_version))) {
      LOG_WARN("get tenant schema version failed", K(ret), K(session.get_effective_tenant_id()));
    } else if (OB_FAIL(context.schema_guard_->get_schema_version(OB_SYS_TENANT_ID, local_sys_schema_version))) {
      LOG_WARN("get sys tenant schema version failed", K(ret), K(OB_SYS_TENANT_ID));
    } else {
      result.get_exec_context().get_task_exec_ctx().set_query_tenant_begin_schema_version(local_tenant_schema_version);
      result.get_exec_context().get_task_exec_ctx().set_query_sys_begin_schema_version(local_sys_schema_version);
    }
  }
  if (OB_SUCC(ret) && is_prepare_protocol && !is_dynamic_sql) {
    result.set_simple_ps_protocol();
  }

  LOG_TRACE("arrive handle pl execute", K(ret),
            "sql", context.is_sensitive_ ? ObString(OB_MASKED_STR) : sql,
            K(is_prepare_protocol), K(is_dynamic_sql), K(lbt()));

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(pctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguement", K(ret));
  } else if (OB_FAIL(set_timeout_for_pl(session, cur_timeout_us))) {
    LOG_WARN("failed to set timeout for pl", K(ret));
  } else if (OB_FAIL(session.store_query_string(sql))) {
    LOG_WARN("store query string fail", K(ret));
  } else if (OB_FAIL(handle_sql_execute(sql, context, result, params, PC_PL_MODE))) {
    LOG_WARN("failed to handle sql execute", K(ret));
  } else {
    result.get_session().set_exec_min_cluster_version();
  }

  if (OB_FAIL(ret) && OB_SUCCESS == result.get_errcode()) {
    result.set_errcode(ret);
  }
  //todo:@hr351303下面的逻辑后续挪到spi层
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(context.schema_guard_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema guard is null");
    } else if (OB_FAIL(session.update_query_sensitive_system_variable(*(context.schema_guard_)))) {
      LOG_WARN("update query affacted system variable failed", K(ret));
    } else if (OB_FAIL(result.open())) {
      LOG_WARN("result set open failed", K(ret));
    } else {
      // do nothing
    }
  }

#ifdef OB_BUILD_AUDIT_SECURITY
  (void)ObSecurityAuditUtils::handle_security_audit(result,
                                                    context.schema_guard_,
                                                    context.cur_stmt_,
                                                    ObString::make_string("pl/sql"),
                                                    ret);

#endif
  if (OB_SUCC(ret) && session.get_in_transaction()) {
    if (ObStmt::is_dml_write_stmt(result.get_stmt_type()) ||
        ObStmt::is_savepoint_stmt(result.get_stmt_type()) ||
        (ObStmt::is_select_stmt(result.get_stmt_type()) && OB_NOT_NULL(result.get_physical_plan()) && result.get_physical_plan()->has_for_update())) {
      session.set_has_exec_inner_dml(true);
    }
  }
  FLT_SET_TAG(sql_id, context.sql_id_);
  return ret;
}

int ObSql::handle_ps_prepare(const ObString &stmt,
                             ObSqlCtx &context,
                             ObResultSet &result,
                             bool is_inner_sql)
{
// open_cursors is 0 to indicate a special state, no limit is set
#define NEED_CHECK_SESS_MAX_PS_HANDLE_LIMIT(v) (0 == v ? false : true)
  int ret = OB_SUCCESS;
  ObString cur_query;
  // trimed_stmt仅用于query empty检查, prepare语句需要用原始语句, 避免checksum不一致
  ObString trimed_stmt = const_cast<ObString &>(stmt).trim();
  if (trimed_stmt.empty()) {
    ret = OB_ERR_EMPTY_QUERY;
    LOG_WARN("query is empty", K(ret));
  } else if (OB_FAIL(init_result_set(context, result))) {
    LOG_WARN("failed to init result set", K(ret));
  }

#ifdef ERRSIM
  // inject error for pr-ex protocol only
  if (OB_SUCC(ret)) {
    ret = OB_E(common::EventTable::COM_STMT_PREXECUTE_PREPARE_ERROR, context.is_pre_execute_) OB_SUCCESS;
  }
#endif

  if (OB_SUCC(ret)) {
    ObSQLSessionInfo &session = result.get_session();
    ObPsCache *ps_cache = session.get_ps_cache();
    ObExecContext &ectx = result.get_exec_context();
    ObIAllocator &allocator = result.get_mem_pool();
    ObPhysicalPlanCtx *pctx = ectx.get_physical_plan_ctx();
    ObSchemaGetterGuard *schema_guard = context.schema_guard_;
    ectx.set_is_ps_prepare_stage(true);

#ifndef NDEBUG
    LOG_INFO("Begin to handle prepare statement", "sess_id", session.get_sessid(),
             "proxy_sess_id", session.get_proxy_sessid(), K(stmt));
#endif

    if (OB_ISNULL(ps_cache) || OB_ISNULL(pctx) || OB_ISNULL(schema_guard)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("physical plan context or ps plan cache is NULL or schema_guard is null",
                K(ret), K(pctx), K(ps_cache));
    } else if (OB_FAIL(ob_write_string(allocator, session.get_current_query_string(), cur_query))) {
      LOG_WARN("failed to write string", K(ret));
    } else if (OB_FAIL(session.store_query_string(stmt))) {
      LOG_WARN("store query string fail", K(ret));
    } else {
      bool need_do_real_prepare = false;
      uint64_t db_id = OB_INVALID_ID;
      (void)session.get_database_id(db_id);
      ObPsSqlKey ps_key;
      ps_key.db_id_ = db_id;
      ps_key.ps_sql_ = stmt;
      ps_key.is_client_return_hidden_rowid_ = session.is_client_return_rowid();
      ObPsStmtId inner_stmt_id = OB_INVALID_STMT_ID;
      ObPsStmtId client_stmt_id = OB_INVALID_STMT_ID;
      ObPsStmtInfo *stmt_info = NULL;
      ObPsStmtItem *stmt_item = NULL;
      bool duplicate_prepare = false;
      bool is_expired = false;
      int64_t open_cursors_limit = 0;
      int64_t cur_ps_handle_size = session.get_ps_session_info_size();
      omt::ObTenantConfigGuard tenant_config(TENANT_CONF(session.get_effective_tenant_id()));
      if (!tenant_config.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant config is invalid", K(ret));
      } else if (FALSE_IT(open_cursors_limit = tenant_config->open_cursors)) {
      } else if (!is_inner_sql
                && NEED_CHECK_SESS_MAX_PS_HANDLE_LIMIT(open_cursors_limit)
                && cur_ps_handle_size >= open_cursors_limit) {
        ret = OB_ERR_OPEN_CURSORS_EXCEEDED;
        LOG_WARN("exceeds the maximum number of ps handles allowed to open on the session",
        K(ret), K(cur_ps_handle_size), K(open_cursors_limit));
      } else if (NULL != context.secondary_namespace_ || result.is_simple_ps_protocol()) {
        // pl发起的sql解析, 由于每次需要计算依赖对象等额外参数, 因此需要做do_real_prepare
        need_do_real_prepare = true;
        if (REACH_TIME_INTERVAL(1000000)) {
          LOG_INFO("need do real prepare",
                   K(db_id), K(stmt), K(need_do_real_prepare), K(context.secondary_namespace_),
                   K(result.is_simple_ps_protocol()));
        }
      } else if (OB_FAIL(ps_cache->ref_stmt_item(ps_key, stmt_item))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          need_do_real_prepare = true;
          if (REACH_TIME_INTERVAL(1000000)) {
            LOG_INFO("stmt id not exist", K(db_id), K(stmt), K(need_do_real_prepare));
          }
        } else {
          LOG_WARN("fail to get stmt id", K(ret), K(db_id), K(stmt));
        }
      } else if (OB_ISNULL(stmt_item)
                 || OB_INVALID_STMT_ID == (inner_stmt_id = stmt_item->get_ps_stmt_id())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("stmt id is invalid", K(ret), K(inner_stmt_id), K(db_id), K(stmt), K(stmt_item));
      } else if (OB_FAIL(ps_cache->ref_stmt_info(inner_stmt_id, stmt_info))) {
        //inc stmt_info ref for session
        if (OB_HASH_NOT_EXIST == ret) {
          need_do_real_prepare = true;
          if (REACH_TIME_INTERVAL(1000000)) {
            LOG_INFO("stmt info not exist", K(db_id), K(stmt), K(inner_stmt_id), K(ret));
          }
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to get stmt info", K(ret), K(db_id), K(stmt), K(inner_stmt_id));
        }
      } else if (OB_ISNULL(stmt_info)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("stmt info is null", K(ret), K(inner_stmt_id));
        //check stmt_info whether expired, if expired, do nothing
      } else if (OB_FAIL(ps_cache->check_schema_version(*context.schema_guard_,
                                                        *stmt_info,
                                                        is_expired))) {
        LOG_WARN("fail to check schema version", K(ret));
      } else if (is_expired) {
        stmt_info->set_is_expired();
        if (OB_FAIL(ps_cache->erase_stmt_item(inner_stmt_id, ps_key))) {
          LOG_WARN("fail to erase stmt item", K(ret), K(*stmt_info));
        }
        need_do_real_prepare = true;
      } else if (OB_FAIL(session.prepare_ps_stmt(inner_stmt_id,
                                                stmt_info,
                                                client_stmt_id,
                                                duplicate_prepare,
                                                is_inner_sql))) {
        LOG_WARN("add ps session info failed", K(ret), K(inner_stmt_id), K(client_stmt_id));
      } else if (OB_FAIL(fill_result_set(client_stmt_id, *stmt_info, result))) {
        //prepare ps stmt已成功，失败此处需close
        IGNORE_RETURN session.close_ps_stmt(client_stmt_id);
        LOG_WARN("fill result set failed", K(ret), K(client_stmt_id));
      }
      LOG_DEBUG("prepare done", K(ret), K(need_do_real_prepare), K(duplicate_prepare));
      if (OB_FAIL(ret)
          || need_do_real_prepare
          || duplicate_prepare) {
        if (NULL != stmt_item) {
          stmt_item->dec_ref_count();
        }
        if (NULL != stmt_info) {
          ps_cache->deref_stmt_info(inner_stmt_id); //需要决定是否摘除
        }
      }
      if (OB_SUCC(ret) && need_do_real_prepare) {
        if (OB_FAIL(do_real_prepare(stmt, context, result, is_inner_sql))) {
          LOG_WARN("do_real_prepare failed", K(ret));
        }
      } else if (OB_SUCC(ret) && NULL != stmt_info) {
        context.is_sensitive_ = stmt_info->get_is_sensitive_sql();
      }
      if (OB_SUCC(ret)) {
        if (false == need_do_real_prepare) {
          ps_cache->inc_access_and_hit_count();
        } else {
          // 没有命中ps cache的情况下，只增加access count
          // 这里的判断逻辑会导致pl每次prepare都只是增加access count，不增加hit_count
          // 所以从ps相关虚拟表中看到的ps cache命中率会比较低
          ps_cache->inc_access_count();
        }
      }
    }
    OZ (session.store_query_string(cur_query));
  }
  return ret;
}

int ObSql::add_param_to_param_store(const ObObjParam &param,
                                    ParamStore &param_store)
{
  int ret = OB_SUCCESS;
  if (lib::is_oracle_mode()
      && ( (param.is_varchar() && 0 == param.get_varchar().length())
            || (param.is_char() && 0 == param.get_char().length())
            || (param.is_nstring() && 0 == param.get_string_len()) )) {
    const_cast<ObObjParam &>(param).set_null();
    const_cast<ObObjParam &>(param).set_param_meta();
  } else if (param.is_numeric_type()) {
    const_cast<ObObjParam &>(param).set_param_meta();
  }
  if (OB_FAIL(param_store.push_back(param))) {
    LOG_WARN("pushback param failed", K(ret));
  }
  return ret;
}

int ObSql::construct_param_store_from_parameterized_params(const ObPlanCacheCtx &phy_ctx,
                                               ParamStore &param_store)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(param_store.reserve(phy_ctx.fp_result_.parameterized_params_.count()))) {
    LOG_WARN("failed to reserve array", K(ret));
  }
  for (int i = 0; OB_SUCC(ret) && i < phy_ctx.fp_result_.parameterized_params_.count(); ++i) {
    const common::ObObjParam *param = phy_ctx.fp_result_.parameterized_params_.at(i);
    if (OB_FAIL(add_param_to_param_store(*param, param_store))) {
      LOG_WARN("failed to add param to param store", K(ret));
    }
    LOG_TRACE("ps param is", KPC(param), K(i));
  }
  return ret;
}

bool ObSql::is_exist_in_fixed_param_idx(const int64_t idx,
                                        const ObIArray<int64_t> &fixed_param_idx)
{
  bool bool_ret = false;
  for (int i = 0; i < fixed_param_idx.count(); ++i) {
    if (idx == fixed_param_idx.at(i)) {
      bool_ret = true;
      break;
    }
  }
  return bool_ret;
}

int ObSql::construct_ps_param_store(const ParamStore &params,
                                    const ParamStore &fixed_params,
                                    const ObIArray<int64_t> &fixed_params_idx,
                                    ParamStore &param_store)
{
  int ret = OB_SUCCESS;
  int64_t param_idx = 0;
  int64_t fixed_param_idx = 0;
  int64_t param_count = params.count() + fixed_params.count();
  if (fixed_params.count() != fixed_params_idx.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the number of fixed param does not match", K(fixed_params), K(fixed_params_idx));
  } else if (OB_FAIL(param_store.reserve(param_count))) {
    LOG_WARN("failed to reserve array", K(ret), K(param_count));
  }
  for (int i = 0; OB_SUCC(ret) && i < param_count; ++i) {
    if (is_exist_in_fixed_param_idx(i, fixed_params_idx)) {
      if (fixed_param_idx >= fixed_params.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Invalid index", K(fixed_param_idx), K(fixed_params));
      } else if (OB_FAIL(add_param_to_param_store(fixed_params.at(fixed_param_idx),
                                                  param_store))) {
        LOG_WARN("failed to add param to param store", K(ret));
      } else {
        LOG_TRACE("ps param is", K(fixed_params.at(fixed_param_idx)), K(i));
      }
      ++fixed_param_idx;
    } else {
      if (param_idx >= params.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Invalid index", K(param_idx), K(params));
      } else if (OB_FAIL(add_param_to_param_store(params.at(param_idx), param_store))) {
        LOG_WARN("failed to add param to param store", K(ret));
      } else {
        LOG_TRACE("ps param is", K(params.at(param_idx)), K(i));
      }
      ++param_idx;
    }
  }
  return ret;
}

int ObSql::construct_param_store(const ParamStore &params,
                                 ParamStore &param_store)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(param_store.reserve(params.count()))) {
    LOG_WARN("failed to reserve array", K(ret));
  }
  for (int i = 0; OB_SUCC(ret) && i < params.count(); ++i) {
    if (OB_FAIL(add_param_to_param_store(params.at(i), param_store))) {
      LOG_WARN("failed to add param to param store", K(ret));
    }
    LOG_TRACE("ps param is", K(params.at(i)), K(i));
  }
  return ret;
}

int ObSql::construct_parameterized_params(const ParamStore &params,
                                          ObPlanCacheCtx &phy_ctx)
{
  int ret = OB_SUCCESS;
  phy_ctx.fp_result_.parameterized_params_.reset();
  phy_ctx.fp_result_.parameterized_params_.set_allocator(&phy_ctx.allocator_);
  phy_ctx.fp_result_.parameterized_params_.set_capacity(params.count());
  for (int i = 0; OB_SUCC(ret) && i < params.count(); ++i) {
    if (OB_FAIL(phy_ctx.fp_result_.parameterized_params_.push_back(&params.at(i)))) {
      LOG_WARN("add ps param failed", K(ret));
    }
  }
  return ret;
}

int ObSql::clac_fixed_param_store(const stmt::StmtType stmt_type,
                                  const ObIArray<int64_t> &raw_params_idx,
                                  const ObIArray<ObPCParam *> &raw_params,
                                  ObIAllocator &allocator,
                                  ObSQLSessionInfo &session,
                                  ParamStore &fixed_param_store)
{
  int ret = OB_SUCCESS;
  ObCollationType collation_connection = static_cast<ObCollationType>(
                                         session.get_local_collation_connection());
  ObString literal_prefix;
  ObObjParam value;
  const bool is_paramlize = false;
  int64_t server_collation = CS_TYPE_INVALID;
  bool enable_decimal_int = false;
  ObCompatType compat_type = COMPAT_MYSQL57;
  if (raw_params.empty()) {
    // do nothing
  } else if (raw_params_idx.count() != raw_params.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the number of fixed param does not match",
    K(raw_params_idx.count()), K(raw_params.count()));
  } else if (OB_FAIL(fixed_param_store.reserve(raw_params_idx.count()))) {
    LOG_WARN("failed to reserve array", K(ret), K(raw_params_idx.count()));
  } else if (OB_FAIL(session.get_compatibility_control(compat_type))) {
    LOG_WARN("failed to get compat type", K(ret));
  } else if (lib::is_oracle_mode() && OB_FAIL(
    session.get_sys_variable(share::SYS_VAR_COLLATION_SERVER, server_collation))) {
    LOG_WARN("get sys variable failed", K(ret));
  } else if (OB_FAIL(ObSQLUtils::check_enable_decimalint(&session, enable_decimal_int))) {
    LOG_WARN("fail to check enable decimal int", K(ret));
  }
  for (int i = 0; OB_SUCC(ret) && i < raw_params.count(); ++i) {
    value.reset();
    ParseNode *raw_param = NULL;;
    if (OB_ISNULL(raw_params.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("raw param is null", K(ret));
    } else if (OB_ISNULL(raw_param = raw_params.at(i)->node_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("node is null", K(ret));
    } else if (OB_FAIL(ObResolverUtils::resolve_const(raw_param,
                                                      stmt_type,
                                                      allocator,
                                                      collation_connection,
                                                      session.get_nls_collation_nation(),
                                                      session.get_timezone_info(),
                                                      value,
                                                      is_paramlize,
                                                      literal_prefix,
                                                      session.get_actual_nls_length_semantics(),
                                                      static_cast<ObCollationType>(server_collation),
                                                      NULL, session.get_sql_mode(),
                                                      enable_decimal_int,
                                                      compat_type))) {
      SQL_PC_LOG(WARN, "fail to resolve const", K(ret));
    } else if (OB_FAIL(add_param_to_param_store(value, fixed_param_store))) {
      LOG_WARN("failed to add param to param store", K(ret), K(value), K(fixed_param_store));
    } else {
      LOG_TRACE("fixed param is", K(value));
    }
  }
  return ret;
}

int ObSql::init_execute_params_for_ab(ObIAllocator &allocator,
                                      const ParamStore &params_store,
                                      ParamStore *&first_group_params)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(first_group_params)) {
    // do nothing
  } else if (OB_ISNULL(first_group_params = static_cast<ParamStore *>(allocator.alloc(sizeof(ParamStore))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else if (FALSE_IT(first_group_params = new(first_group_params)ParamStore(ObWrapperAllocator(allocator)))) {
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(ObPlanCacheValue::get_one_group_params(0, params_store, *first_group_params))) {
    LOG_WARN("fail to get the first group parameters", K(ret));
  } else {
    for (int64_t i = 0; i < first_group_params->count(); i++) {
      ObObjParam &obj_param = first_group_params->at(i);
      obj_param.get_param_flag().is_batch_parameter_ = true;
    }
  }
  LOG_DEBUG("print first_group_params", K(ret), KPC(first_group_params));
  return ret;
}

int ObSql::reconstruct_pl_params_store(ObIAllocator &allocator,
                                       ObSqlCtx &context,
                                       const ParamStore &origin_params,
                                       ParamStore &pl_params,
                                       ParamStore *&pl_ab_params)
{
  int ret = OB_SUCCESS;
  if (context.is_batch_params_execute()) {
    ParamStore *first_group_params = &pl_params;
    if (OB_FAIL(init_execute_params_for_ab(allocator, origin_params, first_group_params))) {
      LOG_WARN("fail to init first batch params", K(ret), K(origin_params));
    } else if (OB_ISNULL(pl_ab_params = static_cast<ParamStore *>(allocator.alloc(sizeof(ParamStore))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret));
    } else if (FALSE_IT(pl_ab_params = new(pl_ab_params)ParamStore(ObWrapperAllocator(allocator)))) {
      // do nothing
    } else if (OB_FAIL(construct_param_store(origin_params,
                                             *pl_ab_params))) {
      LOG_WARN("construct param store failed", K(ret));
    }
  } else {
    if (OB_FAIL(construct_param_store(origin_params, pl_params))) {
      LOG_WARN("construct param store failed", K(ret));
    }
  }
  return ret;
}

int ObSql::reconstruct_ps_params_store(ObIAllocator &allocator,
                                       ObSqlCtx &context,
                                       const ParamStore &origin_params,
                                       const ParamStore &fixed_params,
                                       ObPsStmtInfo *ps_info,
                                       ParamStore &ps_params,
                                       ParamStore *&ps_ab_params)
{
  int ret = OB_SUCCESS;
  ParamStore *first_group_params = NULL;
  if (context.is_batch_params_execute()) {
    if (OB_FAIL(init_execute_params_for_ab(allocator, origin_params, first_group_params))) {
      LOG_WARN("fail to init first batch params", K(ret), K(origin_params));
    } else if (OB_FAIL(construct_ps_param_store(*first_group_params,
                                                fixed_params,
                                                ps_info->get_raw_params_idx(),
                                                ps_params))) {
      LOG_WARN("construct param store failed", K(ret));
    } else if (OB_ISNULL(ps_ab_params = static_cast<ParamStore *>(allocator.alloc(sizeof(ParamStore))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory", K(ret));
    } else if (FALSE_IT(ps_ab_params = new(ps_ab_params)ParamStore(ObWrapperAllocator(allocator)))) {
      // do nothing
    } else if (OB_FAIL(construct_ps_param_store(origin_params,
                                                fixed_params,
                                                ps_info->get_raw_params_idx(),
                                                *ps_ab_params))) {
      LOG_WARN("construct param store failed", K(ret));
    }
  } else if (OB_FAIL(construct_ps_param_store(origin_params,
                                              fixed_params,
                                              ps_info->get_raw_params_idx(),
                                              ps_params))) {
    LOG_WARN("construct param store failed", K(ret));
  }
  return ret;
}

int ObSql::check_read_only_privilege(ParseResult &parse_result,
                                     ObExecContext &exec_ctx,
                                     ObSchemaGetterGuard &schema_guard,
                                     ObSqlTraits &sql_traits)
{
  int ret = OB_SUCCESS;
  bool read_only = false;
  ObPhysicalPlanCtx *pctx = exec_ctx.get_physical_plan_ctx();
  ObSQLSessionInfo *session = exec_ctx.get_my_session();
  sql_traits.is_readonly_stmt_ = ObSQLUtils::is_readonly_stmt(parse_result);
  sql_traits.is_modify_tenant_stmt_
      = ObSQLUtils::is_modify_tenant_stmt(parse_result);
  sql_traits.is_cause_implicit_commit_
      = ObSQLUtils::cause_implicit_commit(parse_result);
  sql_traits.is_commit_stmt_ = ObSQLUtils::is_commit_stmt(parse_result);
  sql_traits.stmt_type_ = ObSQLUtils::get_sql_item_type(parse_result);
  if (OB_ISNULL(pctx) || OB_ISNULL(session)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_read_only(MTL_ID(), read_only))) {
    LOG_WARN("fail to get tenant read only attribute", K(ret), K(MTL_ID()));
  } else if (OB_FAIL(session->check_read_only_privilege(read_only,
                                                        sql_traits))) {
    LOG_WARN("failed to check read_only privilege", K(ret));
    if (ObSQLUtils::is_end_trans_stmt(parse_result)) {
      int et_ret = OB_SUCCESS;
      exec_ctx.set_need_disconnect(false);
      //FIXME qianfu NG_TRACE_EXT(set_need_disconnect, OB_ID(need_disconnect), false);
      LOG_WARN("is commit or rollback stmt, but fail to check read_only privilege, "
              "rollback", K(ret));
      int64_t plan_timeout = 0;
      if (OB_SUCCESS != (et_ret = session->get_query_timeout(plan_timeout))) {
        LOG_ERROR("fail to get query timeout", K(ret), K(et_ret));
      } else {
        pctx->set_timeout_timestamp(session->get_query_start_time() + plan_timeout);
        // explicitly rollback the transaction, if it fails, the connection will be disconnected
        if (OB_SUCCESS != (et_ret = ObSqlTransControl::explicit_end_trans(
                    exec_ctx, true))) {
          LOG_ERROR("fail explicit rollback trans", K(ret), K(et_ret));
        }
      }
    }
  }
  return ret;
}

int ObSql::handle_ps_execute(const ObPsStmtId client_stmt_id,
                             const stmt::StmtType stmt_type,
                             const ParamStore &params,
                             ObSqlCtx &context,
                             ObResultSet &result,
                             bool is_inner_sql)
{
  int ret = OB_SUCCESS;
  ParamStore *ps_ab_params = NULL;
  int get_plan_err = OB_SUCCESS;
  context.is_prepare_protocol_ = true;
  ObPsStmtId inner_stmt_id = client_stmt_id;
  context.stmt_type_ = stmt_type;

  // normal ps execute sql also a dynamic sql, here we adjust is_dynamic_sql_.
  context.is_dynamic_sql_ = !context.is_dynamic_sql_ ? !is_inner_sql : context.is_dynamic_sql_;

  ObIAllocator &allocator = result.get_mem_pool();
  ObSQLSessionInfo &session = result.get_session();
  ObExecContext &ectx = result.get_exec_context();
  ParamStore fixed_params( (ObWrapperAllocator(allocator)) );
  ParamStore ps_params( (ObWrapperAllocator(allocator)) );
  ObPsCache *ps_cache = session.get_ps_cache();
  ObPlanCache *plan_cache = session.get_plan_cache();
  bool use_plan_cache = session.get_local_ob_enable_plan_cache();
  ObPhysicalPlanCtx *pctx = ectx.get_physical_plan_ctx();
  ObSchemaGetterGuard *schema_guard = context.schema_guard_;
  int64_t origin_params_count = params.count();
  if (OB_ISNULL(ps_cache) || OB_ISNULL(pctx) || OB_ISNULL(schema_guard) || OB_ISNULL(plan_cache)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("physical plan context or ps plan cache is NULL or schema_guard is null",
              K(ret), K(pctx), K(ps_cache));
  } else if (!is_inner_sql && OB_FAIL(session.get_inner_ps_stmt_id(client_stmt_id,
                                                                    inner_stmt_id))) {
    LOG_WARN("get_inner_ps_stmt_id failed", K(ret), K(client_stmt_id), K(inner_stmt_id));
  } else {
    context.statement_id_ = inner_stmt_id;
    ObPsStmtInfoGuard guard;
    ObPsStmtInfo *ps_info = NULL;
    pctx->set_original_param_cnt(origin_params_count);
    pctx->set_orig_question_mark_cnt(origin_params_count);
    if (OB_FAIL(ps_cache->get_stmt_info_guard(inner_stmt_id, guard))) {
      LOG_WARN("get stmt info guard failed", K(ret), K(inner_stmt_id));
    } else if (OB_ISNULL(ps_info = guard.get_stmt_info())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get stmt info is null", K(ret));
    } else if (ps_info->get_question_mark_count() != origin_params_count) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Incorrect arguments to execute",
                K(ps_info->get_question_mark_count()),
                K(ps_info->get_ps_sql()),
                K(origin_params_count), K(ret));
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "execute");
    } else if (OB_FAIL(clac_fixed_param_store(stmt_type,
                                              ps_info->get_raw_params_idx(),
                                              ps_info->get_fixed_raw_params(),
                                              allocator,
                                              session,
                                              fixed_params))) {
      LOG_WARN("failed to calc fixed param store", K(ret));
    } else if (OB_FAIL(reconstruct_ps_params_store(
        allocator, context, params, fixed_params, ps_info, ps_params, ps_ab_params))) {
      LOG_WARN("fail to reconstruct_ps_params_store", K(ret));
    } else if (context.is_batch_params_execute() &&
        OB_ISNULL(ps_ab_params)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ps_ab_params_store is null", K(ret));
    } else if (OB_FAIL(construct_param_store(ps_params, pctx->get_param_store_for_update()))) {
      LOG_WARN("construct param store failed", K(ret));
    } else {
      const ObString &sql = !ps_info->get_no_param_sql().empty() ? ps_info->get_no_param_sql() : ps_info->get_ps_sql();
      context.cur_sql_ = sql;
#ifndef NDEBUG
      LOG_INFO("Begin to handle execute statement", "sess_id", session.get_sessid(),
               "proxy_sess_id", session.get_proxy_sessid(), K(sql));
#endif

      if (!ps_info->get_fixed_raw_params().empty()) {
        pctx->set_is_ps_rewrite_sql();
      }
      if (OB_FAIL(session.store_query_string(sql))) {
        LOG_WARN("store query string fail", K(ret));
      } else if (FALSE_IT(generate_ps_sql_id(sql, context))) {
      } else if (OB_LIKELY(ObStmt::is_dml_stmt(stmt_type))) {
        //if plan not exist, generate plan
        ObPlanCacheCtx pc_ctx(sql, PC_PS_MODE, allocator, context, ectx,
                              session.get_effective_tenant_id());
        pc_ctx.fp_result_.pc_key_.key_id_ = inner_stmt_id;
        pc_ctx.normal_parse_const_cnt_ = ps_params.count();
        context.spm_ctx_.bl_key_.db_id_ = session.get_database_id();
        pc_ctx.set_is_parameterized_execute();
        pc_ctx.set_is_inner_sql(is_inner_sql);
        pc_ctx.ab_params_ = ps_ab_params;
        if (OB_FAIL(construct_parameterized_params(ps_params, pc_ctx))) {
          LOG_WARN("construct parameterized params failed", K(ret));
        } else {
          if (!use_plan_cache) {
            /*do nothing*/
          } else if (OB_FAIL(pc_get_plan_and_fill_result(pc_ctx, result, get_plan_err,
                                          ectx.get_need_disconnect_for_update()))) {
            LOG_DEBUG("fail to get plan", K(ret));
          }

          if (OB_FAIL(ret)) {//do nothing
          } else if (!result.get_is_from_plan_cache()) {
            pctx->set_original_param_cnt(origin_params_count);
            pctx->get_param_store_for_update().reset();
            if (OB_FAIL(handle_physical_plan(sql, context, result, pc_ctx, get_plan_err))) {
              if (OB_ERR_PROXY_REROUTE == ret) {
                LOG_DEBUG("fail to handle physical plan", K(ret));
              } else {
                LOG_WARN("fail to handle physical plan", K(ret));
              }
            }
          }
          if (OB_SUCC(ret) && (OB_FAIL(after_get_plan(pc_ctx,
                                                      session,
                                                      result.get_physical_plan(),
                                                      result.get_is_from_plan_cache(),
                                                      &ps_params,
                                                      ectx.get_min_cluster_version())))) {
            LOG_WARN("fail to handle after get plan", K(ret));
          }
        }
      } else if (stmt::T_ANONYMOUS_BLOCK == stmt_type && !context.is_pre_execute_) {
        ParseResult parse_result;
        MEMSET(&parse_result, 0, SIZEOF(ParseResult));
        if (OB_FAIL(generate_physical_plan(parse_result, NULL, context, result,
                                            false/*is_begin_commit_stmt*/, PC_PS_MODE))) {
          LOG_WARN("generate physical plan failed", K(ret));
        } else {
          const ObPsSqlMeta &sql_meta = ps_info->get_ps_sql_meta();
          const common::ObIArray<ObField> &param_fields = sql_meta.get_param_fields();
          int64_t field_column_cnt = 0;
          for (int64_t i = 0; OB_SUCC(ret) && i < param_fields.count(); ++i) {
            if (ObRoutineParamInOut::SP_PARAM_INOUT
                  == static_cast<ObRoutineParamInOut>(param_fields.at(i).inout_mode_)
                || ObRoutineParamInOut::SP_PARAM_OUT
                  == static_cast<ObRoutineParamInOut>(param_fields.at(i).inout_mode_)) {
              field_column_cnt++;
            }
          }
          OZ (result.reserve_field_columns(field_column_cnt));
          for (int64_t i = 0; OB_SUCC(ret) && i < field_column_cnt; ++i) {
            ObField field;
            field.type_.set_type(ObNullType);
            OZ (result.add_field_column(field));
          }
        }
      } else {
        if (stmt::T_CALL_PROCEDURE == stmt_type && !context.is_dynamic_sql_) {
          // call procedure stmt call always parse as dynamic sql
          context.is_dynamic_sql_ = true;
        }
        if (stmt::T_CALL_PROCEDURE == stmt_type && !context.is_execute_call_stmt_) {
          context.is_execute_call_stmt_ = true;
        }
        ObParser parser(allocator, session.get_sql_mode(),
                        session.get_charsets4parser());
        ParseResult parse_result;
        ObSqlTraits sql_traits;
        ParseMode parse_mode = context.is_dbms_sql_ ? DBMS_SQL_MODE :
                                context.is_dynamic_sql_ ? DYNAMIC_SQL_MODE :
                                (context.session_info_->is_for_trigger_package() ? TRIGGER_MODE : STD_MODE);
        if (OB_FAIL(parser.parse(sql, parse_result, parse_mode))) {
          LOG_WARN("failed to parse sql", K(ret), K(stmt_type),
                   "sql", parse_result.contain_sensitive_data_ ? ObString(OB_MASKED_STR) : sql);
        }
        context.is_sensitive_ |= parse_result.contain_sensitive_data_;

        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(check_read_only_privilege(parse_result, ectx, *schema_guard, sql_traits))) {
          LOG_WARN("failed to check read only privilege", K(ret));
        } else if (OB_FAIL(generate_physical_plan(parse_result, NULL, context, result,
            false /*is_begin_commit_stmt*/, PC_PS_MODE))) {
          LOG_WARN("generate physical plan failed", K(ret),
                   "sql", context.is_sensitive_ ? ObString(OB_MASKED_STR) : sql, K(stmt_type));
        } // TODO 生成物理计划的路径可x需q区分
      }
    }
  }
  return ret;
}

int ObSql::handle_remote_query(const ObRemoteSqlInfo &remote_sql_info,
                               ObSqlCtx &context,
                               ObExecContext &exec_ctx,
                               ObCacheObjGuard& guard)
{
  int ret = OB_SUCCESS;
  //trim the sql first, let 'select c1 from t' and '  select c1 from t' and hit the same plan_cache
  const ObString &trimed_stmt = remote_sql_info.remote_sql_;
  FLTSpanGuard(remote_compile);
  FLT_SET_TAG(sql_text, trimed_stmt);

  ObIAllocator &allocator = THIS_WORKER.get_sql_arena_allocator();
  ObSQLSessionInfo *session = exec_ctx.get_my_session();
  int get_plan_err = OB_SUCCESS; //used for judge whether add plan to plan cache
  bool is_from_plan_cache = false;
  ObPlanCacheCtx *pc_ctx = NULL;
  ParamStore param_store( (ObWrapperAllocator(allocator)) );
  ObSEArray<ObString, 1> queries;
  if (OB_ISNULL(session) || OB_ISNULL(remote_sql_info.ps_params_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret), K(session), K(remote_sql_info.ps_params_));
  } else if (OB_ISNULL(pc_ctx =
      static_cast<ObPlanCacheCtx *>(allocator.alloc(sizeof(ObPlanCacheCtx))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret), K(sizeof(ObPlanCacheCtx)));
  } else {
#ifndef NDEBUG
  LOG_INFO("Begin to handle remote statement",
           K(remote_sql_info), "sess_id", session->get_sessid(),
           "proxy_sess_id", session->get_proxy_sessid(),
           "execution_id", session->get_current_execution_id());
#endif
    if (exec_ctx.get_min_cluster_version() != session->get_exec_min_cluster_version()) {
      // src min cluster version may different with local current min cluster version
      // need to generate plan by src min cluster version
      LOG_INFO("remote plan cluster version changed", K(exec_ctx.get_min_cluster_version()),
                                                      K(session->get_exec_min_cluster_version()));
      exec_ctx.get_task_exec_ctx().set_min_cluster_version(session->get_exec_min_cluster_version());
    }
    const uint64_t tenant_id = session->get_effective_tenant_id();
    bool use_plan_cache = session->get_local_ob_enable_plan_cache();
    context.self_add_plan_ = false;
    PlanCacheMode mode = remote_sql_info.use_ps_ ? PC_PS_MODE : PC_TEXT_MODE;
    mode = remote_sql_info.sql_from_pl_ ? PC_PL_MODE : mode;
    context.cur_sql_ = trimed_stmt;
    pc_ctx = new (pc_ctx) ObPlanCacheCtx(trimed_stmt,
                                         mode,
                                         allocator,
                                         context,
                                         exec_ctx,
                                         tenant_id);
    pc_ctx->is_remote_executor_ = true;
    if (remote_sql_info.use_ps_) {
      //由于现在ps模式和普通的文本协议的执行计划不能复用，因此这里需要区分，避免在查询plan的时候引起一些问题
      //由于普通的文本协议key_id是OB_INVALID_ID,因此这里使用key_id=0+name=参数化SQL的方式来区分
      //@todo: shengle 普通的ps协议和文本协议的计划共享也存在同样的问题，这里需要统一解决一下
      context.is_prepare_protocol_ = remote_sql_info.use_ps_;
      context.spm_ctx_.bl_key_.db_id_ = session->get_database_id();
      pc_ctx->fp_result_.pc_key_.key_id_ = 0;
      pc_ctx->fp_result_.pc_key_.name_ = trimed_stmt;
      pc_ctx->normal_parse_const_cnt_ = remote_sql_info.ps_params_->count();
      pc_ctx->set_is_parameterized_execute();
      pc_ctx->is_original_ps_mode_ = remote_sql_info.is_original_ps_mode_;
      if (OB_FAIL(construct_param_store(*remote_sql_info.ps_params_, param_store))) {
        LOG_WARN("construct param store failed", K(ret));
      } else if (OB_FAIL(construct_parameterized_params(param_store, *pc_ctx))) {
        LOG_WARN("construct parameterized params failed", K(ret));
      }
    } else if (remote_sql_info.is_batched_stmt_) {
      //这里保持跟控制端一致，如果是batched stmt,需要先做一次parser的切分
      //切割出来的query最后走batched multi stmt的逻辑去查询plan cache和生成计划
      ObParser parser(allocator,
                      session->get_sql_mode(),
                      session->get_charsets4parser(),
                      pc_ctx->def_name_ctx_);
      ObMPParseStat parse_stat;
      if (OB_FAIL(parser.split_multiple_stmt(remote_sql_info.remote_sql_, queries, parse_stat))) {
        LOG_WARN("split multiple stmt failed", K(ret), K(remote_sql_info));
      } else {
        context.multi_stmt_item_.set_batched_queries(&queries);
      }
    }

    if (OB_SUCC(ret)) {
      ObCacheObjGuard tmp_guard(MAX_HANDLE);
      ObPhysicalPlan* plan = nullptr;
      if (OB_FAIL(session->get_database_id(context.spm_ctx_.bl_key_.db_id_))) {
        LOG_WARN("Failed to get database id", K(ret));
      } else if (!use_plan_cache) {
        if (context.is_batch_params_execute()) {
          ret = OB_BATCHED_MULTI_STMT_ROLLBACK;
          LOG_WARN("batched multi_stmt needs rollback");
        }
      } else if (OB_FAIL(pc_get_plan(*pc_ctx,
                                     tmp_guard,
                                     get_plan_err,
                                     exec_ctx.get_need_disconnect_for_update()))) {
        LOG_DEBUG("fail to get plan", K(ret));
      } else if (FALSE_IT(plan = static_cast<ObPhysicalPlan*>(tmp_guard.get_cache_obj()))) {
        // do nothing
      } else if (OB_NOT_NULL(plan)) {
        if (plan->is_local_plan()) {
          is_from_plan_cache = true;
          tmp_guard.init(pc_ctx->handle_id_);
          guard.swap(tmp_guard);
        } else {
          //如果从plan cache中选择出来的plan不是local执行计划，说明不是remote sql想要的plan
          //需要丢弃重新生成新的local plan
          is_from_plan_cache = false;
        }
      }
    }
  }

  if (OB_SUCC(ret) && !is_from_plan_cache) { //没有从plan cache中拿到plan, 走长路径生成plan
    //只需要plan，不需要其它信息，因此构造一个临时的result set
    SMART_VAR(ObResultSet, tmp_result, *session, allocator) {
      tmp_result.set_exec_context(exec_ctx);
      //经过plan cache的计算后，param_store里的值可能会增加，因为plan cache中会执行pre calculation
      //这里要再进行计划生成，需要把plan cache中pre calculation加入的param清除掉
      int64_t initial_param_count = pc_ctx->fp_result_.parameterized_params_.count();
      for (int64_t i = remote_sql_info.ps_params_->count(); i > initial_param_count; --i) {
        remote_sql_info.ps_params_->pop_back();
      }
      PlanCacheMode mode = remote_sql_info.use_ps_ ? PC_PS_MODE : PC_TEXT_MODE;
      mode = remote_sql_info.sql_from_pl_ ? PC_PL_MODE : mode;
      if (OB_FAIL(handle_physical_plan(trimed_stmt, context, tmp_result, *pc_ctx, get_plan_err))) {
        if (OB_ERR_PROXY_REROUTE == ret) {
          LOG_DEBUG("fail to handle physical plan", K(ret));
        } else {
          LOG_WARN("fail to handle physical plan", K(ret));
        }
      } else {
        // we need NOT to de some special operation for remote plan, this because we have swaped
        // the life cycle of tmp_result's guard with remote_guard, which means plan's life becomes
        // longer and we needn't to do other operation!!!
        guard.swap(tmp_result.get_cache_obj_guard());
      }
    }
  }


  // set auto-increment related param into physical plan ctx
  // if get plan from plan cache, reset its auto-increment variable here
  // do not set variable for hidden primary key; its default value is 1
  ObPhysicalPlan* plan = nullptr;
  if (OB_SUCC(ret)) {
    plan = static_cast<ObPhysicalPlan*>(guard.get_cache_obj());
    if (OB_ISNULL(plan)) {
    } else if (OB_UNLIKELY(!plan->is_local_plan())) {
      //不是本地计划，控制端发送错误，返回错误码进行重试
      ret = OB_LOCATION_NOT_EXIST;
      LOG_WARN("plan type is invalid", K(remote_sql_info), KPC(plan));
    } else if (OB_FAIL(after_get_plan(*pc_ctx,
                                      *session,
                                      plan,
                                      is_from_plan_cache,
                                      NULL /*ps param*/,
                                      exec_ctx.get_min_cluster_version()))) {
      LOG_WARN("fail to handle after get plan", K(ret));
    }
  }
  LOG_DEBUG("get remote plan", K(ret), K(is_from_plan_cache), KPC(plan));
  //清空掉warning buffer，因为生成执行计划的warning buffer都在控制端记录下来，这里不需要再记录
  //不然会导致warning消息重复
  ob_reset_tsi_warning_buffer();
  if (NULL != pc_ctx) {
    pc_ctx->~ObPlanCacheCtx();
  }
  return ret;
}

template <typename ProcessorT>
int ObSql::handle_remote_batch_req(const ObReqTimestamp &req_ts, const char* buf, int32_t size)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ProcessorT, processor, GCTX) {
    processor.set_from_batch();
    if (OB_FAIL(processor.init())) {
      LOG_WARN("init processor failed", K(ret));
    } else {
      auto &arg = processor.get_arg();
      int64_t pos = 0;
      processor.set_receive_timestamp(req_ts.receive_timestamp_);
      processor.set_run_timestamp(req_ts.run_timestamp_);
      processor.set_enqueue_timestamp(req_ts.enqueue_timestamp_);
      if (OB_FAIL(arg.deserialize(buf, size, pos))) {
        LOG_WARN("deserialize processor arg failed", K(ret), K(size), K(pos));
      } else if (OB_FAIL(processor.before_process())) {
        LOG_WARN("before process failed", K(ret));
      } else if (OB_FAIL(processor.process())) {
        LOG_WARN("process remote batch req failed", K(ret));
      } else if (OB_FAIL(processor.before_response(ret))) {
        LOG_WARN("before response remote batch req failed", K(ret));
      } else if (OB_FAIL(processor.after_process(ret))) {
        LOG_WARN("after process batch req failed", K(ret));
      }
    }
    processor.cleanup();
  }
  return ret;
}

OB_INLINE int ObSql::handle_text_query(const ObString &stmt, ObSqlCtx &context, ObResultSet &result)
{
  int ret = OB_SUCCESS;
  //trim the sql first, let 'select c1 from t' and '  select c1 from t' and hit the same plan_cache
  ObString trimed_stmt = const_cast<ObString &>(stmt).trim();
  context.is_prepare_protocol_ = false;
  FLT_SET_TAG(sql_text, trimed_stmt);
  char buf[4096];
  STATIC_ASSERT(sizeof(ObPlanCacheCtx) < sizeof(buf), "ObPlanCacheCtx is too large");
  if (OB_FAIL(init_result_set(context, result))) {
    LOG_WARN("failed to init result set", K(ret));
  } else if (trimed_stmt.empty()) {
    ret = OB_ERR_EMPTY_QUERY;
    LOG_WARN("query is empty", K(ret));
    LOG_USER_ERROR(OB_ERR_EMPTY_QUERY);
    // 空请求，可以归类到parser的已知错误，不需要断连接
    result.get_exec_context().set_need_disconnect(false);
    //FIXME qianfu NG_TRACE_EXT(set_need_disconnect, OB_ID(need_disconnect), false);
  }

  ObIAllocator &allocator = THIS_WORKER.get_sql_arena_allocator();
  ObSQLSessionInfo &session = result.get_session();
  const uint64_t tenant_id = session.get_effective_tenant_id();
  ObExecContext& ectx = result.get_exec_context();
  int get_plan_err = OB_SUCCESS; //used for judge whether add plan to plan cache
#ifndef OB_BUILD_SPM
  bool use_plan_cache = session.get_local_ob_enable_plan_cache();
#else
  bool use_plan_cache = session.get_local_ob_enable_plan_cache() && !context.spm_ctx_.is_retry_for_spm_;
#endif
  ObPlanCacheCtx *pc_ctx = NULL;
  bool is_begin_commit_stmt = false;
  if (OB_FAIL(ret)) {
    // do nothing
  //} else if (NULL == (pc_ctx = static_cast<ObPlanCacheCtx *>
  //                             (allocator.alloc(sizeof(ObPlanCacheCtx))))) {
  //  ret = OB_ALLOCATE_MEMORY_FAILED;
  //  LOG_WARN("fail to alloc memory", K(ret), K(sizeof(ObPlanCacheCtx)));
  } else {
    context.cur_sql_ = trimed_stmt;
    pc_ctx = new (buf) ObPlanCacheCtx(trimed_stmt,
                                         PC_TEXT_MODE,
                                         allocator,
                                         context,
                                         ectx,
                                         tenant_id);
    if (trimed_stmt.length() == 6) {
      //是否为COMMIT语句
      is_begin_commit_stmt = (0 == STRNCASECMP(trimed_stmt.ptr(), "commit", 6)
                              && !context.is_batch_params_execute());
    } else if (trimed_stmt.length() == 5) {
      is_begin_commit_stmt = (0 == STRNCASECMP(trimed_stmt.ptr(), "begin", 5)
                              && !context.is_batch_params_execute());
    }
    if (is_begin_commit_stmt) {
      //记录当前语句是begin/commit 语句，用于性能优化
      pc_ctx->set_begin_commit_stmt();
    }
    uint64_t database_id = OB_INVALID_ID;

    if (OB_FAIL(session.get_database_id(database_id))) {
      LOG_WARN("Failed to get database id", K(ret));
    } else if (FALSE_IT(context.spm_ctx_.bl_key_.db_id_ =
                                  (database_id == OB_INVALID_ID) ?
                                      OB_MOCK_DEFAULT_DATABASE_ID:
                                      database_id)) {
      // do nothing
    } else if (!use_plan_cache) {
#ifndef OB_BUILD_SPM
      if (context.multi_stmt_item_.is_batched_multi_stmt()) {
#else
      if (context.multi_stmt_item_.is_batched_multi_stmt() && !context.spm_ctx_.is_retry_for_spm_) {
#endif
        ret = OB_BATCHED_MULTI_STMT_ROLLBACK;
        LOG_WARN("batched multi_stmt needs rollback");
      }
      // 如果是begin/commit语句，不再从plan cache中获取plan
    } else if (!is_begin_commit_stmt
        && OB_FAIL(pc_get_plan_and_fill_result(*pc_ctx, result, get_plan_err,
                                               ectx.get_need_disconnect_for_update()))) {
      LOG_DEBUG("fail to get plan", K(ret));
    }
  }

  int tmp_ret = ret;
  if (!is_begin_commit_stmt
      && 0 == context.multi_stmt_item_.get_seq_num() /* only first item of a multi stmt, or single stmt */
#ifdef OB_BUILD_SPM
      && !context.spm_ctx_.is_retry_for_spm_
#endif
      && OB_FAIL(handle_large_query(tmp_ret,
                                    result,
                                    ectx.get_need_disconnect_for_update(),
                                    ectx))) {
    //do nothing
  }
  if (OB_SUCC(ret) && !result.get_is_from_plan_cache()) { //没有从plan cache中拿到plan, 走长路径生成plan
    if (OB_FAIL(handle_physical_plan(trimed_stmt, context, result, *pc_ctx, get_plan_err))) {
      if (OB_ERR_PROXY_REROUTE == ret) {
        LOG_DEBUG("fail to handle physical plan", K(ret));
      } else {
        LOG_WARN("fail to handle physical plan", K(ret));
      }
    }
  }


  // set auto-increment related param into physical plan ctx
  // if get plan from plan cache, reset its auto-increment variable here
  // do not set variable for hidden primary key; its default value is 1
  if (OB_SUCC(ret)) {
    if (!context.is_text_ps_mode_
        && OB_FAIL(after_get_plan(*pc_ctx, session, result.get_physical_plan(),
                                  result.get_is_from_plan_cache(), NULL,
                                  ectx.get_min_cluster_version()))) {
      LOG_WARN("fail to handle after get plan", K(ret));
    }
  }
  if (NULL != pc_ctx) {
    pc_ctx->~ObPlanCacheCtx();
  }
  return ret;
}

OB_NOINLINE int ObSql::handle_large_query(int tmp_ret,
                                          ObResultSet &result,
                                          bool &need_disconnect,
                                          ObExecContext &exec_ctx)
{
  int ret = OB_SUCCESS;
  if (tmp_ret != OB_SUCCESS
      && tmp_ret != OB_PC_LOCK_CONFLICT) {
    ret = tmp_ret;
  } else if (result.get_session().is_inner()
             || !ObStmt::is_dml_stmt(result.get_stmt_type())) {
    ret = (tmp_ret == OB_PC_LOCK_CONFLICT) ? OB_SUCCESS : tmp_ret;
  } else {
    const int64_t lqt = GCONF.large_query_threshold;
    int64_t elapsed_time = ObClockGenerator::getClock() - THIS_THWORKER.get_query_start_time();
    bool is_large_query = false;
    bool lq_from_plan = true;
    int64_t total_process_time = 0;
    int64_t exec_times = 0;
    ObPhysicalPlan *plan = NULL;
    //用来自plan cache的plan预判是否为大请求
    if (result.get_is_from_plan_cache()) {
      if (OB_ISNULL(plan = result.get_physical_plan())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(ret));
      } else {
        exec_times = plan->stat_.get_execute_count();
        total_process_time = plan->stat_.total_process_time_;
        if (exec_times > 0
            && (0 != lqt && (total_process_time / exec_times) > lqt)) {
          plan->inc_large_querys();
          is_large_query = true;
          lq_from_plan = true;
        }
      }
    }
    //实际编译时间判断是否为大请求
    if (OB_SUCC(ret) && is_large_query == false) {
      if (OB_PC_LOCK_CONFLICT == tmp_ret
          || (0 != lqt && elapsed_time > lqt)) {
        is_large_query = true;
        lq_from_plan = false;
      }
    }
    if (OB_SUCC(ret) && is_large_query && OB_FAIL(THIS_WORKER.check_large_query_quota())) {
      need_disconnect = false;
      if (lq_from_plan) {
        plan->inc_delayed_large_querys();
        LOG_INFO("It's a large query, need delay, do not need disconnect",
                 "avg_process_time", total_process_time / exec_times,
                 "exec_cnt", exec_times,
                 "large_query_threshold", lqt,
                 K(plan->get_plan_id()), K(ret));
      } else {
        LOG_INFO("compile time is too long, need delay", K(elapsed_time), K(ret));
      }
    }
#ifdef OB_BUILD_SPM
    if (OB_SUCC(ret) && !is_large_query) {
      result.get_session().reset_spm_select_plan_type();
    } else if (OB_EAGAIN == ret || is_large_query) {
      ObSpmCacheCtx& spm_ctx = exec_ctx.get_sql_ctx()->spm_ctx_;
      result.get_session().set_spm_select_plan_type(spm_ctx.select_plan_type_);
    }
#endif
  }

  return ret;
}

int ObSql::generate_stmt(ParseResult &parse_result,
                         ObPlanCacheCtx *pc_ctx,
                         ObSqlCtx &context,
                         ObIAllocator &allocator,
                         ObResultSet &result,
                         ObStmt *&stmt,
                         ParseResult *outline_parse_result)
{
  int ret = OB_SUCCESS;
  FLTSpanGuard(resolve);
  uint64_t session_id = 0;
  ObResolverParams resolver_ctx;
  ObPhysicalPlanCtx *plan_ctx = NULL;
  ObSchemaChecker *schema_checker = NULL;

  int64_t last_mem_usage = allocator.total();
  int64_t resolver_mem_usage = 0;
  if (OB_FAIL(sanity_check(context))) {
    LOG_WARN("Failed to do sanity check", K(ret));
  } else {
    if (result.get_session().get_session_type() != ObSQLSessionInfo::INNER_SESSION) {
      session_id = result.get_session().get_sessid_for_table();
    } else {
      session_id = OB_INVALID_ID; //内部session, 不受table_schema->session_id的可见性影响, 能看到查询建表过程中的表
    }
    schema_checker = OB_NEWx(ObSchemaChecker, (&allocator));
    if (OB_UNLIKELY(NULL == schema_checker)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("Failed to malloc ObSchemaChecker", K(ret));
    } else if (NULL == context.schema_guard_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("context schema guard is null", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    resolver_ctx.allocator_  = &allocator;
    resolver_ctx.schema_checker_ = schema_checker;
    resolver_ctx.secondary_namespace_ = context.secondary_namespace_;
    resolver_ctx.session_info_ = context.session_info_;
    resolver_ctx.expr_factory_ = result.get_exec_context().get_expr_factory();
    resolver_ctx.stmt_factory_ = result.get_exec_context().get_stmt_factory();
    resolver_ctx.cur_sql_ = context.cur_sql_;
    resolver_ctx.is_restore_ = context.is_restore_;
    resolver_ctx.is_ddl_from_primary_ = context.is_ddl_from_primary_;
    resolver_ctx.is_cursor_ = context.is_cursor_;
    resolver_ctx.is_batch_stmt_ = context.is_batch_params_execute();
    resolver_ctx.batch_stmt_num_ = context.get_batch_params_count();
    if (NULL != pc_ctx && pc_ctx->is_remote_executor_) {
      resolver_ctx.need_check_col_dup_
        = !(context.is_prepare_protocol_ && parse_result.question_mark_ctx_.by_ordinal_ && pc_ctx->is_original_ps_mode_);
    } else {
      resolver_ctx.need_check_col_dup_
        = !(context.is_prepare_protocol_ && parse_result.question_mark_ctx_.by_ordinal_);
    }
    resolver_ctx.external_param_info_.by_name_
        = parse_result.question_mark_ctx_.by_name_ || NULL != context.secondary_namespace_; //static sql in PL must be by name
    resolver_ctx.outline_parse_result_ = outline_parse_result;
    resolver_ctx.is_execute_call_stmt_ = context.is_execute_call_stmt_;
    if (NULL != pc_ctx) {
      resolver_ctx.select_item_param_infos_ = &pc_ctx->select_item_param_infos_;
    }
    plan_ctx = result.get_exec_context().get_physical_plan_ctx();
    if (OB_ISNULL(plan_ctx) || OB_ISNULL(result.get_exec_context().get_stmt_factory())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Plan ctx should not be NULL", K(ret), KP(plan_ctx));
    } else if (OB_ISNULL(resolver_ctx.query_ctx_ =
        result.get_exec_context().get_stmt_factory()->get_query_ctx())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("allocate query context failed", K(ret));
    } else if (OB_FAIL(resolver_ctx.session_info_->get_optimizer_features_enable_version(
                          resolver_ctx.query_ctx_->optimizer_features_enable_version_))) {
      LOG_WARN("failed to get_optimizer_features_enable_version", K(ret));
    } else {
      resolver_ctx.query_ctx_->sql_schema_guard_.set_schema_guard(context.schema_guard_);
      if (OB_FAIL(resolver_ctx.schema_checker_->init(resolver_ctx.query_ctx_->sql_schema_guard_, session_id))) {
        LOG_WARN("init schema checker failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (NULL != resolver_ctx.secondary_namespace_
        && NULL !=  resolver_ctx.secondary_namespace_->get_external_ns()) {
      resolver_ctx.package_guard_ =
        &resolver_ctx.secondary_namespace_->get_external_ns()->get_resolve_ctx().package_guard_;
    }
  }
  if (OB_SUCC(ret)) {
    resolver_ctx.is_prepare_protocol_ = context.is_prepare_protocol_;
    resolver_ctx.is_prepare_stage_ = context.is_prepare_stage_;
    resolver_ctx.is_pre_execute_ = context.is_pre_execute_;
    resolver_ctx.is_dynamic_sql_ = context.is_dynamic_sql_;
    resolver_ctx.is_dbms_sql_ = context.is_dbms_sql_;
    resolver_ctx.statement_id_ = context.statement_id_;
    resolver_ctx.param_list_ = &plan_ctx->get_param_store();
    resolver_ctx.sql_proxy_ = GCTX.sql_proxy_;
    // disable sql resouce management in:
    // 1. remote query
    // 2. inner sql
    // 3. prepare in ps
    // 4. multi stmt
    if (NULL != GCTX.cgroup_ctrl_ && GCTX.cgroup_ctrl_->is_valid()
        && context.enable_sql_resource_manage_
        && !context.is_remote_sql_
        && !result.get_session().is_inner()
        && !(context.is_prepare_protocol_ && context.is_prepare_stage_)
        && !(context.multi_stmt_item_.is_part_of_multi_stmt() && context.multi_stmt_item_.get_seq_num() > 0)) {
      resolver_ctx.enable_res_map_ = true;
      context.res_map_rule_version_ = G_RES_MGR.get_col_mapping_rule_mgr().get_column_mapping_version(
        result.get_session().get_effective_tenant_id());
    }
  }

  if (OB_FAIL(ret)) {
  } else if (stmt::T_ANONYMOUS_BLOCK == context.stmt_type_ && context.is_prepare_protocol_ && !context.is_prepare_stage_) {
    //anonymous + ps在execute阶段不会做parser, 因此不应该检查parser_result
    //do nothing...
  } else if (OB_ISNULL(parse_result.result_tree_)
        || OB_ISNULL(parse_result.result_tree_->children_)
        || OB_ISNULL(parse_result.result_tree_->children_[0])) {
      ret = OB_INVALID_ARGUMENT;
      SQL_LOG(WARN, "invalid args",
              KP(parse_result.result_tree_),
              KP(parse_result.result_tree_->children_),
              KP(parse_result.result_tree_->children_[0]));
  }

  if (OB_SUCC(ret)) {
      // set # of question marks
      if (context.is_prepare_protocol_ && !context.is_prepare_stage_) {
        resolver_ctx.query_ctx_->question_marks_count_ = plan_ctx->get_param_store().count();
        LOG_DEBUG("question mark size is ", K(plan_ctx->get_param_store()));
      } else {
        resolver_ctx.query_ctx_->question_marks_count_ = static_cast<int64_t> (parse_result.question_mark_ctx_.count_);
        LOG_DEBUG("question mark size is ", K(parse_result.question_mark_ctx_.count_));
      }

      ObResolver resolver(resolver_ctx);
      NG_TRACE(resolve_begin);
      if (OB_FAIL(plan_ctx->build_subschema_ctx_by_param_store(context.schema_guard_))) {
        // only when param has sql udt types
        SQL_LOG(WARN, "failed to build sbuschema ctx by param_store", K(ret));
      } else if (stmt::T_ANONYMOUS_BLOCK == context.stmt_type_
          && context.is_prepare_protocol_
          && !context.is_prepare_stage_
          && !context.is_pre_execute_) {
        ParseNode tmp_node;
        tmp_node.type_ = T_SP_ANONYMOUS_BLOCK;
        ret = resolver.resolve(ObResolver::IS_NOT_PREPARED_STMT, tmp_node, stmt);
      } else {
        ret = resolver.resolve(ObResolver::IS_NOT_PREPARED_STMT, *parse_result.result_tree_->children_[0], stmt);
      }
      // set const param constraint after resolving
      context.all_plan_const_param_constraints_ = &(resolver_ctx.query_ctx_->all_plan_const_param_constraints_);
      context.all_possible_const_param_constraints_ = &(resolver_ctx.query_ctx_->all_possible_const_param_constraints_);
      context.all_equal_param_constraints_ = &(resolver_ctx.query_ctx_->all_equal_param_constraints_);
      context.all_pre_calc_constraints_ = &(resolver_ctx.query_ctx_->all_pre_calc_constraints_);
      context.all_expr_constraints_ = &(resolver_ctx.query_ctx_->all_expr_constraints_);
      context.all_priv_constraints_ = &(resolver_ctx.query_ctx_->all_priv_constraints_);
      context.need_match_all_params_ = resolver_ctx.query_ctx_->need_match_all_params_;
      context.all_local_session_vars_ = &(resolver_ctx.query_ctx_->all_local_session_vars_);
      context.cur_stmt_ = stmt;
      context.res_map_rule_id_ = resolver_ctx.query_ctx_->res_map_rule_id_;
      context.res_map_rule_param_idx_ = resolver_ctx.query_ctx_->res_map_rule_param_idx_;
      LOG_DEBUG("got plan const param constraints", K(resolver_ctx.query_ctx_->all_plan_const_param_constraints_));
      LOG_DEBUG("got all const param constraints", K(resolver_ctx.query_ctx_->all_possible_const_param_constraints_));
      LOG_TRACE("set sql context rule id", K(ret), K(context.res_map_rule_id_), K(context.res_map_rule_param_idx_),
                K(&context), K(&resolver_ctx), K(context.is_prepare_stage_), K(context.is_prepare_protocol_),
                K(NULL != GCTX.cgroup_ctrl_ && GCTX.cgroup_ctrl_->is_valid()),
                K(result.get_session().is_inner()),
                K(context.multi_stmt_item_.is_part_of_multi_stmt()),
                K(context.multi_stmt_item_.get_seq_num()));
      if (result.get_session().get_group_id_not_expected()) {
        // ignore ret
        LOG_USER_WARN(OB_NEED_SWITCH_CONSUMER_GROUP);
        result.get_session().set_group_id_not_expected(false);
      }
      NG_TRACE(resolve_end);
      if (OB_SUCC(ret)) {
        // move init datum param store here
        // if `opt_param("enable_rich_vector_format", "false")` is used in hints, use_rich_format will
        // be off, we need reset value in pctx and initialize param frame accordingly.
        if (NULL != pc_ctx && NULL != pc_ctx->exec_ctx_.get_physical_plan_ctx()) {
          pc_ctx->exec_ctx_.get_physical_plan_ctx()->set_rich_format(context.session_info_->use_rich_format());
          if (OB_FAIL(pc_ctx->exec_ctx_.get_physical_plan_ctx()->init_datum_param_store())) {
            LOG_WARN("init datum param store failed", K(ret));
          }
        }
      }
      //add ref obj schema version to PL and ps info
      if (OB_SUCC(ret)) {
        if (OB_FAIL(result.get_ref_objects().assign(resolver_ctx.query_ctx_->global_dependency_tables_))) {
          LOG_WARN("assign ref obj schema version failed", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
        /* for audit */
        if (NULL != stmt) {
          result.set_stmt_type(stmt->get_stmt_type());
        }
        SQL_LOG(WARN, "failed to resolve", K(ret));
      } else {
        // process stmt
        if (NULL != stmt && NULL != resolver_ctx.query_ctx_) {
          SQL_LOG(DEBUG, "SET STMT PARAM COUNT", K(resolver.get_params().prepare_param_count_), K(&resolver_ctx));
          //secondary_namespace_不为空，说明是PL里sql的prepare阶段
          //带有returning子句的动态sql也需要rebuild,用来去除into子句
          //pl context not null indicate PL dynamic sql, only need rebuild PL dynamic sql
          bool in_pl = NULL != resolver_ctx.secondary_namespace_
            || (resolver_ctx.is_dynamic_sql_ && OB_NOT_NULL(result.get_session().get_pl_context()))
            || resolver_ctx.is_dbms_sql_;
          bool need_rebuild = (lib::is_mysql_mode() ? (resolver_ctx.is_dynamic_sql_ &&
          OB_NOT_NULL(result.get_session().get_pl_context()) && resolver_ctx.is_prepare_stage_) : resolver_ctx.is_prepare_stage_ && in_pl);
          bool is_returning_into = false;
          if (stmt->is_insert_stmt() || stmt->is_update_stmt() || stmt->is_delete_stmt()) {
            ObDelUpdStmt &dml_stmt = static_cast<ObDelUpdStmt&>(*stmt);
            if (dml_stmt.get_returning_into_exprs().count() != 0) {
              need_rebuild = true;
              is_returning_into = true;
            }
          }
          if (need_rebuild) {
            if (OB_FAIL(result.get_external_retrieve_info().build(*stmt,
                result.get_session(),
                resolver_ctx.secondary_namespace_,
                resolver_ctx.is_dynamic_sql_ || resolver_ctx.is_dbms_sql_,
                resolver.get_params().external_param_info_.params_))) {
              SQL_LOG(WARN, "failed to build external retrieve info", K(ret));
            } else {
              if (result.get_external_params().empty() && result.get_into_exprs().empty()) {
                if (resolver_ctx.query_ctx_->get_sql_stmt().empty()) {
                  resolver_ctx.query_ctx_->set_sql_stmt(parse_result.input_sql_, parse_result.input_sql_len_);
                }
                result.get_route_sql() = resolver_ctx.query_ctx_->get_sql_stmt();
                resolver_ctx.query_ctx_->set_prepare_param_count(parse_result.question_mark_ctx_.count_);
              } else if (is_returning_into && !in_pl) {
                int64_t return_into_num = static_cast<ObDelUpdStmt&>(*stmt).get_returning_into_exprs().count();
                resolver_ctx.query_ctx_->set_prepare_param_count(parse_result.question_mark_ctx_.count_- return_into_num);
              } else {
                // 对于oracle模式下pl内部的sql语句，到resolver完成后才能确定具体的prepare_param_count_
                resolver_ctx.query_ctx_->set_prepare_param_count(resolver.get_params().prepare_param_count_);
              }
            }
          } else {
            if (stmt::T_ANONYMOUS_BLOCK == context.stmt_type_ && context.is_prepare_protocol_) {
              resolver_ctx.query_ctx_->set_sql_stmt(context.cur_sql_);
            } else {
              resolver_ctx.query_ctx_->set_sql_stmt(parse_result.input_sql_, parse_result.input_sql_len_);
            }
            result.get_route_sql() = resolver_ctx.query_ctx_->get_sql_stmt();
            resolver_ctx.query_ctx_->set_prepare_param_count(parse_result.question_mark_ctx_.count_);
          }
          if (OB_SUCC(ret)) {
            result.set_stmt_type(stmt->get_stmt_type()); // used by query retry
            result.set_literal_stmt_type(resolver_ctx.query_ctx_->get_literal_stmt_type()); // used by show warn, show trace
            ObDMLStmt *dml_stmt = NULL;
            ObVariableSetStmt *variable_set_stmt = NULL;
            if ((dml_stmt = dynamic_cast<ObDMLStmt*>(stmt)) != NULL) {
              const ObGlobalHint &global_hint = resolver_ctx.query_ctx_->get_global_hint();
              result.set_is_calc_found_rows(dml_stmt->is_calc_found_rows());
              plan_ctx->set_is_affect_found_row(dml_stmt->is_affect_found_rows());
              context.force_print_trace_ = global_hint.force_trace_log_;
              if (MpQuery == context.exec_type_ && global_hint.log_level_.length() > 0) {
                const ObString &log_level = global_hint.log_level_;
                if (OB_UNLIKELY(OB_SUCCESS != process_thread_log_id_level_map(log_level.ptr(),
                                                                              log_level.length()))) {
                  LOG_WARN("Failed to process thread log id level map");
                }
              }
              ObDelUpdStmt *del_up_stmt = NULL;
              del_up_stmt = dynamic_cast<ObDelUpdStmt *>(dml_stmt);
              if (del_up_stmt != NULL && del_up_stmt->is_returning()) {
                result.set_returning(true);
              }
            } else if ((variable_set_stmt = dynamic_cast<ObVariableSetStmt*>(stmt)) != NULL) {
              result.set_has_global_variable(variable_set_stmt->has_global_variable());
            }
          }
          if (OB_SUCC(ret)) {
            SQL_LOG(DEBUG, "Generate stmt success", K(*stmt));
          } else {
            LOG_WARN("failed to generate stmt", K(ret));
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          SQL_LOG(WARN, "failed to generate stmt", K(ret));
        }
      }
  }
  resolver_mem_usage = allocator.total() - last_mem_usage;
  LOG_DEBUG("SQL MEM USAGE", K(resolver_mem_usage), K(last_mem_usage));
  return ret;
}

int ObSql::generate_physical_plan(ParseResult &parse_result,
                                  ObPlanCacheCtx *pc_ctx,
                                  ObSqlCtx &sql_ctx,
                                  ObResultSet &result,
                                  const bool is_begin_commit_stmt,
                                  const PlanCacheMode mode,
                                  ParseResult *outline_parse_result /* null */ )
{
  int ret = OB_SUCCESS;
  bool is_valid = true;
  ObStmt *basic_stmt = NULL;
  ObIAllocator &allocator = result.get_mem_pool();
  ObStmtNeedPrivs stmt_need_privs;
  ObStmtOraNeedPrivs stmt_ora_need_privs;
  stmt_need_privs.need_privs_.set_allocator(&allocator);
  stmt_ora_need_privs.need_privs_.set_allocator(&allocator);
  _LOG_DEBUG("start to generate physical plan for query.(query = %.*s)",
              parse_result.input_sql_len_, parse_result.input_sql_);
  if (OB_FAIL(sanity_check(sql_ctx))) { //check sql_ctx.session_info_ and sql_ctx.schema_guard_
    LOG_WARN("Failed to do sanity check", K(ret));
  } else if (OB_FAIL(generate_stmt(parse_result,
                                   pc_ctx,
                                   sql_ctx,
                                   allocator,
                                   result,
                                   basic_stmt,
                                   outline_parse_result))) {
    LOG_WARN("Failed to generate stmt", K(ret), K(result.get_exec_context().need_disconnect()));
  } else if (OB_ISNULL(basic_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Generate stmt success, but stmt is NULL", K(ret));
    // begin/commit 语句不需要检查privilege
  } else if (!is_begin_commit_stmt
          && OB_FAIL(ObPrivilegeCheck::check_privilege_new(sql_ctx,
                                                           basic_stmt,
                                                           stmt_need_privs,
                                                           stmt_ora_need_privs))) {
    LOG_WARN("Failed to check ora privilege info", K(ret), K(*basic_stmt));
  } else if (OB_FAIL(ObPrivilegeCheck::check_password_expired(sql_ctx,
                                                              basic_stmt->get_stmt_type()))) {
    LOG_WARN("Falied to check password expired", K(ret));
  } else if ((sql_ctx.is_batch_params_execute()) &&
             NULL != pc_ctx &&
             OB_FAIL(check_batched_multi_stmt_after_resolver(*pc_ctx,
                                                             *basic_stmt,
                                                             is_valid))) {
    LOG_WARN("failed to check batched multi_stmt after resolver", K(ret));
  } else if (!is_valid) {
    ret = OB_BATCHED_MULTI_STMT_ROLLBACK;
    LOG_WARN("batched multi_stmt needs rollback", K(ret));
  } else if (basic_stmt->is_dml_stmt()
            || basic_stmt->is_explain_stmt()
            || basic_stmt->is_help_stmt()) {
    if (OB_FAIL(generate_plan(parse_result,
                              pc_ctx,
                              sql_ctx,
                              result,
                              mode,
                              basic_stmt,
                              stmt_need_privs,
                              stmt_ora_need_privs))) {
      LOG_WARN("failed to generate plan", K(ret));
    }
  } else if (stmt::T_EXECUTE == basic_stmt->get_stmt_type() &&
             stmt::T_CALL_PROCEDURE != static_cast<ObExecuteStmt*>(basic_stmt)->get_prepare_type()) {
    if (OB_FAIL(handle_text_execute(basic_stmt, sql_ctx, result))) {
      LOG_WARN("handle_text_execute failed", K(ret));
    }
  } else {
    ObICmd *cmd = dynamic_cast<ObICmd*>(basic_stmt);
    if (OB_UNLIKELY(NULL == cmd)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail cast basic stmt to cmd", K(ret));
    } else {
      result.set_cmd(cmd);
      result.get_session().set_cur_sql_id(sql_ctx.sql_id_);
      if (!is_begin_commit_stmt
        && OB_FAIL(fill_result_set(result, &sql_ctx, mode, *basic_stmt))) {
        LOG_WARN("Failed to fill result set", K(ret));
      }
    }
  }
  // execute dml in oracle mode, regardless of success or failure, always need to maintain object dependencies
  if (OB_NOT_NULL(basic_stmt) && basic_stmt->is_dml_stmt()) {
    int tmp_ret = ret;
    ObDMLStmt *stmt = static_cast<ObDMLStmt*>(basic_stmt);
    uint64_t data_version = 0;
    const uint64_t tenant_id = result.get_session().get_effective_tenant_id();
    if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
      LOG_WARN("failed to get data version", K(ret));
    } else if (data_version < DATA_VERSION_4_1_0_0 && stmt->get_ref_obj_table()->is_inited()) {
      if (OB_FAIL(stmt->get_ref_obj_table()->process_reference_obj_table(
          tenant_id, OB_INVALID_ID, nullptr, queue_))) {
        LOG_WARN("failed to process reference obj table", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      ret = tmp_ret;
    }
  }
  return ret;
}

int ObSql::generate_plan(ParseResult &parse_result,
                        ObPlanCacheCtx *pc_ctx,
                        ObSqlCtx &sql_ctx,
                        ObResultSet &result,
                        const PlanCacheMode mode,
                        ObStmt *basic_stmt,
                        ObStmtNeedPrivs &stmt_need_privs,
                        ObStmtOraNeedPrivs &stmt_ora_need_privs)
{
  int ret = OB_SUCCESS;
  uint64_t aggregate_setting = 0;
  ObPhysicalPlanCtx *pctx = result.get_exec_context().get_physical_plan_ctx();
  bool allow_audit = false;
  ObArray<ObAuditUnit> audit_units;
  if (OB_ISNULL(pctx) || OB_ISNULL(basic_stmt) ||
      OB_ISNULL(result.get_exec_context().get_expr_factory())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Physical plan ctx should not be NULL", K(ret));
  } else if (OB_ISNULL(result.get_exec_context().get_stmt_factory()->get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("query ctx is null", K(ret));
  } else if (OB_FAIL(fill_result_set(result, &sql_ctx, mode, *basic_stmt))) {
    LOG_WARN("Failed to fill result set", K(ret));
#ifdef OB_BUILD_AUDIT_SECURITY
  } else if (OB_FAIL(ObSecurityAuditUtils::check_allow_audit(
                      *sql_ctx.session_info_, allow_audit))) {
    LOG_WARN("Failed to check allow audit", K(ret));
  } else if (allow_audit &&
             OB_FAIL(ObSecurityAuditUtils::get_audit_units(
                      basic_stmt->get_stmt_type(), basic_stmt, audit_units))) {
    LOG_WARN("Failed to get audit units", K(ret));
#endif
  } else if (OB_FAIL(sql_ctx.session_info_->get_sys_variable(
                      share::SYS_VAR__AGGREGATION_OPTIMIZATION_SETTINGS,
                      aggregate_setting))) {
    LOG_WARN("failed to get aggregate setting", K(ret));
  } else {
    ObDMLStmt *stmt = static_cast<ObDMLStmt*>(basic_stmt);
    SQL_LOG(DEBUG, "stmt", "stmt", *stmt);
    SQL_LOG(DEBUG, "stmt success", "query", SJ(*stmt));
#ifdef OB_BUILD_SPM
    stmt->get_query_ctx()->is_spm_evolution_ = sql_ctx.spm_ctx_.is_retry_for_spm_;
#endif
    stmt->get_query_ctx()->root_stmt_ = stmt;
    const ObGlobalHint &global_hint = stmt->get_query_ctx()->get_global_hint();
    sql_ctx.session_info_->set_early_lock_release(global_hint.enable_lock_early_release_);
    ObOptimizerContext optctx(sql_ctx.session_info_,
                              &result.get_exec_context(),
                              &result.get_exec_context().get_stmt_factory()->get_query_ctx()->sql_schema_guard_,
                              opt_stat_mgr_,
                              result.get_mem_pool(),
                              &pctx->get_param_store(),
                              self_addr_,
                              GCTX.srv_rpc_proxy_,
                              global_hint,
                              *result.get_exec_context().get_expr_factory(),
                              stmt,
                              result.is_ps_protocol(),
                              result.get_exec_context().get_stmt_factory()->get_query_ctx());
    optctx.set_aggregation_optimization_settings(aggregate_setting);
    pctx->set_field_array(result.get_field_columns());
    pctx->set_is_ps_protocol(result.is_ps_protocol());
    bool is_restore = false;
    uint64_t effective_tid = result.get_session().get_effective_tenant_id();
    ObOptimizer optimizer(optctx);
    bool use_jit = false;
    bool turn_on_jit = sql_ctx.need_late_compile_;
    // if (OB_FAIL(ret)) {
    // } else if (OB_FAIL(need_use_jit(turn_on_jit,
    //                                 query_hint.use_jit_policy_,
    //                                 *sql_ctx.session_info_,
    //                                 use_jit))) {
    //   use_jit = false;
    //   LOG_WARN("failed to check for needing jitted expr", K(ret));
    // } else {
    //   // do nothing
    // }

    ObLogPlan *logical_plan = NULL;
    ObPhysicalPlan *phy_plan = NULL;

    // 内部session切租户时资源处理分离不彻底
    // 当用户请求发送到一个没有对应租户资源的server上时，plan 内存算在了普通租户上，
    // 但是计划挂在了sys租户的plan cache下面，导致plan_cache_stat表数据统计异常，
    // 出现疑似内存泄漏实际上却没有泄漏的情况。这里处理为直接从plan cache
    // 中取tenant id，这样计划分配的资源就算在了plan cache所对应的租户上
    if (OB_NOT_NULL(result.get_session().get_plan_cache())) {
      effective_tid = result.get_session().get_plan_cache()->get_tenant_id();
    }

    ObCacheObjGuard& guard = result.get_cache_obj_guard();
    guard.init(PLAN_GEN_HANDLE);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObCacheObjectFactory::alloc(guard,
                                                    ObLibCacheNameSpace::NS_CRSR,
                                                    effective_tid))) {
      LOG_WARN("fail to alloc phy_plan", K(ret));
    } else if (FALSE_IT(phy_plan = static_cast<ObPhysicalPlan*>(guard.get_cache_obj()))) {
      // do nothing
    } else if (OB_UNLIKELY(NULL == phy_plan)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("Failed to alloc physical plan from tc factory", K(ret));
    } else {
      // update is_use_jit flag
      phy_plan->set_use_rich_format(sql_ctx.session_info_->use_rich_format());
      phy_plan->stat_.is_use_jit_ = use_jit;
      phy_plan->stat_.enable_early_lock_release_ = sql_ctx.session_info_->get_early_lock_release();
      // if phy_plan's tenant id, which refers the tenant who create this plan,
      // not equal to current effective_tid, plan cache must be invalid
      // and we shouldn't add this plan to plan cache.
      if (NULL != pc_ctx) {
        pc_ctx->should_add_plan_ = (effective_tid==phy_plan->get_tenant_id());
        if (!pc_ctx->rule_name_.empty() && OB_FAIL(phy_plan->set_rule_name(pc_ctx->rule_name_))) {
          LOG_WARN("failed to deep copy rule name", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      phy_plan->set_fetch_cur_time(stmt->get_fetch_cur_time());
      phy_plan->set_stmt_type(stmt->get_stmt_type());
      phy_plan->set_literal_stmt_type(stmt->get_query_ctx()->get_literal_stmt_type());
      if (phy_plan->get_fetch_cur_time() && !pctx->has_cur_time()) {
        pctx->set_cur_time(ObTimeUtility::current_time(), *(sql_ctx.session_info_));
      }
      pctx->set_last_trace_id(sql_ctx.session_info_->get_last_trace_id());
    }

    ObSQLSessionInfo *session_info = result.get_exec_context().get_my_session();
    BEGIN_OPT_TRACE(session_info, ObString(strlen(sql_ctx.sql_id_), sql_ctx.sql_id_));
    OPT_TRACE_ENV;
    OPT_TRACE_SESSION_INFO;
    OPT_TRACE_PARAMETERS;
    OPT_TRACE_TITLE("CURRENT SQL TEXT");
    OPT_TRACE("sql_id =", ObString(strlen(sql_ctx.sql_id_), sql_ctx.sql_id_));
    OPT_TRACE(ObString(parse_result.input_sql_len_, parse_result.input_sql_));

    // hint seed injected random status rand
    const ObQueryHint &query_hint = stmt->get_query_ctx()->get_query_hint();
    if (query_hint.has_outline_data() || session_info->is_inner()){
      // if there is outline data no error inject
    } else {
      int64_t tmp_ret = (OB_E(EventTable::EN_GENERATE_RANDOM_PLAN) OB_SUCCESS);
      if (OB_SUCCESS == tmp_ret) {
        // do nothing
      } else {
        time_t seed = OB_ERROR == tmp_ret ? time(NULL) : std::abs(tmp_ret);
        stmt->get_query_ctx()->set_injected_random_status(true);
        stmt->get_query_ctx()->set_random_plan_seed(seed);
        LOG_TRACE("The random seed for plan gen is ", K(seed));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(transform_stmt(&stmt->get_query_ctx()->sql_schema_guard_,
                                      opt_stat_mgr_,
                                      &self_addr_,
                                      phy_plan,
                                      result.get_exec_context(),
                                      stmt))) { //rewrite stmt
      LOG_WARN("Failed to transform stmt", K(ret));
    } else if (OB_FAIL(generate_stmt_with_reconstruct_sql(stmt,
                                                          pc_ctx,
                                                          sql_ctx,
                                                          result,
                                                          phy_plan))) {
      LOG_WARN("failed to reconstruct sql", K(ret));
    } else if (OB_FALSE_IT(optctx.set_root_stmt(stmt))) {
    } else if (OB_FAIL(optimize_stmt(optimizer, *(sql_ctx.session_info_),
                                      *stmt, logical_plan))) { //gen logical plan
      LOG_WARN("Failed to optimizer stmt", K(ret));
    } else if (OB_FAIL(create_expr_constraints(*stmt->get_query_ctx(),
                                                result.get_exec_context()))){
      LOG_WARN("Failed to create expr constraints", K(ret));
    } else if (OB_ISNULL(logical_plan)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid null logical plan", K(ret), K(logical_plan));
    } else if (OB_FAIL(code_generate(sql_ctx, result, stmt,
                                     stmt_need_privs,
                                     stmt_ora_need_privs,
                                     audit_units,
                                     logical_plan, phy_plan))) { //gen phy plan
      LOG_WARN("Failed to genenrate phy plan", K(ret));
    } else if (OB_FAIL(prepare_outline_for_phy_plan(logical_plan,
                                                    phy_plan))) {
      LOG_WARN("failed to prepare outline for phy plan", K(ret));
    } else if (logical_plan->get_optimizer_context().is_online_ddl()) {
      int tmp_ret = OB_SUCCESS;
      ObExplainDisplayOpt option;
      option.with_tree_line_ = false;
      ObSqlPlan sql_plan(logical_plan->get_allocator());
      ObSEArray<common::ObString, 32> plan_strs;
      if (OB_TMP_FAIL(sql_plan.print_sql_plan(logical_plan,
                                          EXPLAIN_EXTENDED,
                                          option,
                                          plan_strs))) {
        LOG_WARN("failed to store sql plan", K(tmp_ret));
      } else {
        LOG_INFO("ddl plan");
        for (int64_t i = 0; OB_SUCCESS == tmp_ret && i < plan_strs.count(); i++) {
          _OB_LOG(INFO, "%*s", plan_strs.at(i).length(), plan_strs.at(i).ptr());
        }
      }
    }
    END_OPT_TRACE(session_info);
    if (OB_SUCC(ret) && session_info->is_user_session()) {
      ObSqlPlan sql_plan(result.get_mem_pool());
      if (!stmt->is_explain_stmt() && !stmt->is_help_stmt()) {
        if (OB_FAIL(sql_plan.store_sql_plan(logical_plan,
                                            phy_plan))) {
          LOG_WARN("failed to store sql plan", K(ret));
        } else {
          phy_plan->set_record_plan_info(true);
        }
      }
    }

    // memory debug for sql work arena
    if (OB_UNLIKELY(result.get_mem_pool().total() > SQL_MEM_SIZE_LIMIT)) {
      int64_t total_mem_used = result.get_mem_pool().total();
      LOG_INFO("[SQL MEM USAGE] use too much memory", K(total_mem_used),
                K(ObString(parse_result.input_sql_len_, parse_result.input_sql_)));
    }

    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(stmt->get_query_ctx())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("query ctx is null", K(ret));
    } else if (stmt::T_SELECT == stmt->get_stmt_type() &&
                stmt::T_SHOW_PARAMETERS == stmt->get_query_ctx()->get_literal_stmt_type()) {
      ObSelectStmt *select_stmt = static_cast<ObSelectStmt*>(stmt);
      pctx->set_tenant_id(select_stmt->get_tenant_id());
      pctx->set_show_seed(select_stmt->get_show_seed());
      result.get_exec_context().reference_my_plan(phy_plan);
    } else {
      result.get_exec_context().reference_my_plan(phy_plan);
    }
  }
  return ret;
}

int ObSql::generate_stmt_with_reconstruct_sql(ObDMLStmt* &stmt,
                                              ObPlanCacheCtx *pc_ctx,
                                              ObSqlCtx &sql_ctx,
                                              ObResultSet &result,
                                              ObPhysicalPlan *phy_plan)
{
  int ret = OB_SUCCESS;
  ObString sql;
  bool has_dblink = false;
  ObObjPrintParams print_param;
  print_param.for_dblink_ = 1;
  ObSQLSessionInfo *session = sql_ctx.session_info_;
  ObPhysicalPlanCtx *phy_plan_ctx = NULL;
  if (OB_ISNULL(session) || OB_ISNULL(pc_ctx) ||
      OB_ISNULL(stmt) || (OB_ISNULL(stmt->get_query_ctx())) ||
      OB_ISNULL(phy_plan_ctx=pc_ctx->exec_ctx_.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null param", K(ret));
  } else if ((OB_E(EventTable::EN_GENERATE_PLAN_WITH_RECONSTRUCT_SQL) OB_SUCCESS) == OB_SUCCESS) {
    //do nothing
  } else if (!session->is_user_session()) {
    //do nothing
  } else if (OB_FAIL(ObDblinkUtils::has_reverse_link_or_any_dblink(stmt, has_dblink, true))) {
    LOG_WARN("failed to check has dblink", K(ret));
  } else if (has_dblink) {
    //do nothing
  } else if (OB_FAIL(ObSQLUtils::reconstruct_sql(pc_ctx->allocator_,
                                                stmt,
                                                sql,
                                                sql_ctx.schema_guard_,
                                                print_param,
                                                &phy_plan_ctx->get_param_store()))) {
    LOG_WARN("failed to reconstruct sql", K(ret));
  } else {
    LOG_TRACE("origin sql:", K(sql_ctx.cur_sql_));
    LOG_TRACE("stmt:", KPC(stmt));
    ObStmt *basic_stmt = NULL;
    ParseResult parse_result;
    ObParser parser(pc_ctx->allocator_,
                    session->get_sql_mode(),
                    session->get_charsets4parser(),
                    pc_ctx->def_name_ctx_);
    stmt->get_query_ctx()->global_dependency_tables_.reuse();
    if (OB_FAIL(parser.parse(sql, parse_result))) {
      LOG_WARN("failed to parser sql", K(ret));
    } else if (OB_FAIL(generate_stmt(parse_result,
                                    pc_ctx,
                                    sql_ctx,
                                    result.get_mem_pool(),
                                    result,
                                    basic_stmt,
                                    NULL))) {
      LOG_WARN("Failed to generate stmt", K(ret));
    } else if (OB_ISNULL(basic_stmt) || !basic_stmt->is_dml_stmt()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Generate stmt success, but stmt is NULL", K(ret));
    } else if (OB_FALSE_IT(stmt = static_cast<ObDMLStmt*>(basic_stmt))) {
    } else if (OB_FAIL(transform_stmt(&stmt->get_query_ctx()->sql_schema_guard_,
                                      opt_stat_mgr_,
                                      &self_addr_,
                                      phy_plan,
                                      result.get_exec_context(),
                                      stmt,
                                      true))) { //rewrite stmt
      LOG_WARN("Failed to transform stmt", K(ret));
    }
    if (OB_FAIL(ret)) {
      LOG_USER_ERROR(OB_SYNC_DDL_ERROR, sql.length(), sql.ptr());
      ret = OB_SYNC_DDL_ERROR;
    }
    LOG_TRACE("reconstruct sql:", K(sql));
  }
  return ret;
}

int ObSql::prepare_outline_for_phy_plan(ObLogPlan *logical_plan,
                                        ObPhysicalPlan *phy_plan)
{
  int ret = OB_SUCCESS;
  void *tmp_ptr = NULL;
  char *buf = NULL;

  if (OB_ISNULL(logical_plan) || OB_ISNULL(phy_plan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get log plan", K(ret), K(logical_plan));
  } else if (OB_UNLIKELY(NULL == (tmp_ptr = phy_plan->get_allocator().alloc(OB_MAX_SQL_LENGTH)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail to alloc memory", K(ret));
  } else if (FALSE_IT(buf = static_cast<char *>(tmp_ptr))) {
  } else {
    PlanText plan_text;
    plan_text.buf_ = buf;
    plan_text.buf_len_ = OB_MAX_SQL_LENGTH;
    if (OB_FAIL(ObSqlPlan::get_plan_outline_info_one_line(plan_text, logical_plan))) {
      LOG_WARN("failed to get plan outline info", K(ret));
    } else {
      phy_plan->stat_.outline_data_.assign_ptr(buf, static_cast<ObString::obstr_size_t>(plan_text.pos_));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(NULL == (tmp_ptr = phy_plan->get_allocator().alloc(OB_MAX_SQL_LENGTH)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail to alloc memory", K(ret));
  } else if (FALSE_IT(buf = static_cast<char *>(tmp_ptr))) {
  } else {
    PlanText plan_text;
    plan_text.buf_ = buf;
    plan_text.buf_len_ = OB_MAX_SQL_LENGTH;
    if (OB_FAIL(ObSqlPlan::get_plan_used_hint_info_one_line(plan_text, logical_plan))) {
      LOG_WARN("failed to get plan used hint info", K(ret));
    } else {
      phy_plan->stat_.hints_info_.assign_ptr(buf, static_cast<ObString::obstr_size_t>(plan_text.pos_));
    }
  }
  return ret;
}

// stmt 全量 const folding.
int ObSql::calc_pre_calculable_exprs(ObExecContext &exec_ctx,
                                     ObDMLStmt &stmt,
                                     ObPhysicalPlan &phy_plan)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(calc_pre_calculable_exprs(stmt.get_calculable_exprs(),
                                        exec_ctx,
                                        stmt,
                                        phy_plan))) {
    LOG_WARN("Failed to calc pre cacluable exprs");
  }
  return ret;
}

// 为了改写层 const folding 抽出来的函数
int ObSql::calc_pre_calculable_exprs(
    ObIArray<ObHiddenColumnItem> &calculable_exprs,
    ObExecContext &exec_ctx,
    ObDMLStmt &stmt,
    ObPhysicalPlan &phy_plan)
{
  int ret = OB_SUCCESS;
  stmt::StmtType stmt_type = stmt::T_NONE;
  ObDList <ObSqlExpression> pre_calc_exprs;
  ObPhysicalPlanCtx *plan_ctx = exec_ctx.get_physical_plan_ctx();
  ObSQLSessionInfo *session_info = exec_ctx.get_my_session();
  ObRawExprFactory *expr_factory = exec_ctx.get_expr_factory();
  if (OB_ISNULL(plan_ctx) || OB_ISNULL(session_info) || OB_ISNULL(expr_factory)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL pointer", K(ret), KP(session_info), KP(expr_factory));
  } else if (stmt.is_explain_stmt()) {
    ObDMLStmt *real_stmt = static_cast<ObExplainStmt&>(stmt).get_explain_query_stmt();
    if (OB_ISNULL(real_stmt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("real_stmt is null", K(ret));
    } else {
      stmt_type = real_stmt->get_stmt_type();
    }
  } else {
    stmt_type = stmt.get_stmt_type();
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < calculable_exprs.count(); i++) {
    bool transformed = false;
    if (OB_FAIL(ObTransformPreProcess::transform_expr(*expr_factory,
                                                      *session_info,
                                                      calculable_exprs.at(i).expr_,
                                                      transformed))) {
      LOG_WARN("transform expr failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    // is_ignore should be set before pre_calculate, which is used in column_conv
    bool is_ignore_stmt = false;
    bool need_fetch_cur_time = false;
    ObDelUpdStmt *modify_stmt = dynamic_cast<ObDelUpdStmt*>(&stmt);
    if (NULL != modify_stmt) {
      is_ignore_stmt = modify_stmt->is_ignore();
    }

    if (OB_FAIL(calc_pre_calculable_exprs(stmt, calculable_exprs,
                                          is_ignore_stmt, exec_ctx, phy_plan))) {
      LOG_WARN("failed to generate and calcute rt exprs", K(ret));
    }
  }
  return ret;
}

int ObSql::transform_stmt(ObSqlSchemaGuard *sql_schema_guard,
                          common::ObOptStatManager *opt_stat_mgr,
                          common::ObAddr *self_addr,
                          ObPhysicalPlan *phy_plan,
                          ObExecContext &exec_ctx,
                          ObDMLStmt *&stmt,
                          bool ignore_trace_event)
{
  int ret = OB_SUCCESS;
  FLTSpanGuard(rewrite);
  ObDMLStmt *transform_stmt = stmt;
  int64_t last_mem_usage = exec_ctx.get_allocator().total();
  int64_t transformer_mem_usage = 0;
  //get transform stmt
  if (OB_ISNULL(stmt) || OB_ISNULL(stmt->get_query_ctx()) || OB_ISNULL(sql_schema_guard) ||
      OB_ISNULL(sql_schema_guard->get_schema_guard()) ||
      OB_ISNULL(opt_stat_mgr) || OB_ISNULL(self_addr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null point", K(stmt), KP(sql_schema_guard),
                           K(opt_stat_mgr), K(self_addr), K(ret));
  } else if (OB_ISNULL(exec_ctx.get_my_session())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("session_info and schema manager in sql_ctx should not be NULL", K(ret));
  } else if (stmt->is_explain_stmt()) {
    if (OB_ISNULL(transform_stmt = static_cast<ObExplainStmt*>(stmt)->get_explain_query_stmt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("Explain query stmt is NULL", K(ret));
    }
  } else { }//do nothing

  ObTransformerCtx trans_ctx;
  ObSchemaChecker schema_checker;

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(schema_checker.init(*sql_schema_guard))) {
    LOG_WARN("fail to init schema_checker", K(ret));
  } else {
    trans_ctx.allocator_ = &exec_ctx.get_allocator();
    trans_ctx.schema_checker_ = &schema_checker;
    trans_ctx.session_info_ = exec_ctx.get_my_session();
    trans_ctx.exec_ctx_ = &exec_ctx;
    trans_ctx.expr_factory_ = exec_ctx.get_expr_factory();
    trans_ctx.stmt_factory_ = exec_ctx.get_stmt_factory();
    trans_ctx.opt_stat_mgr_ = opt_stat_mgr;
    trans_ctx.sql_schema_guard_ = sql_schema_guard;
    trans_ctx.self_addr_ = self_addr;
    // trans_ctx.merged_version_ = merged_version;

    trans_ctx.phy_plan_ = phy_plan;
  }

  NG_TRACE(transform_begin);
  OPT_TRACE_TITLE("START TRANSFORM");
  bool need_transform = transform_stmt->is_valid_transform_stmt();
  if (ignore_trace_event) {
    //do nothing
  } else if ((OB_E(EventTable::EN_GENERATE_PLAN_WITH_RECONSTRUCT_SQL)OB_SUCCESS) != OB_SUCCESS) {
    if (transform_stmt->is_dml_write_stmt()) {
      need_transform = false;
    }
  }
  if (OB_SUCC(ret) && need_transform) {
    ObTransformerImpl transformer(&trans_ctx);
    if (OB_FAIL(transformer.transform(transform_stmt))) {
      LOG_WARN("failed to transform statement", K(ret));
    } else if (stmt->is_explain_stmt()) {
      static_cast<ObExplainStmt*>(stmt)->set_explain_query_stmt(transform_stmt);
    } else {
      bool or_expansion_happened = false;
      if (OB_FAIL(transformer.get_cost_based_trans_happened(OR_EXPANSION,
                                                            or_expansion_happened))) {
        LOG_WARN("failed to check whether or_expansion happened", K(ret));
      } else if (or_expansion_happened) {
        if (OB_ISNULL(exec_ctx.get_physical_plan_ctx())) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("get unexpected null", K(exec_ctx.get_physical_plan_ctx()),
              K(ret));
        } else {
          exec_ctx.get_physical_plan_ctx()->set_or_expand_transformed(true);
        }
      }
      if (OB_SUCC(ret)) {
        stmt = transform_stmt;
      }
    }
  }

  if (OB_SUCC(ret)) {
    OPT_TRACE("after transform, sql is\n", transform_stmt);
    OPT_TRACE_TIME_USED;
    OPT_TRACE_MEM_USED;
  }
  transformer_mem_usage = exec_ctx.get_allocator().total() - last_mem_usage;
  LOG_DEBUG("SQL MEM USAGE", K(transformer_mem_usage), K(last_mem_usage));
  return ret;
}

int ObSql::optimize_stmt(
    ObOptimizer &optimizer,
    const ObSQLSessionInfo &session_info,
    ObDMLStmt &stmt,
    ObLogPlan *&logical_plan)
{
  int ret = OB_SUCCESS;
  FLTSpanGuard(optimize);
  logical_plan = NULL;
  LOG_TRACE("stmt to generate plan", K(stmt));
  OPT_TRACE_TITLE("START GENERATE PLAN");
  if (OB_FAIL(optimizer.optimize(stmt, logical_plan))) {
    LOG_WARN("Failed to optimize logical plan", K(ret));
    // do nothing(plan will be destructed in result set)
  } else if (OB_FAIL(optimizer.update_column_usage_infos())) {
    LOG_WARN("failed to update column usage infos", K(ret));
  } else if (OB_ISNULL(logical_plan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null log plan", K(ret));
  } else {
    LOG_TRACE("logical plan", KPC(logical_plan));
    OPT_TRACE_TIME_USED;
    OPT_TRACE_MEM_USED;
    OPT_TRACE_TITLE("SYSTEM STATS:");
    OPT_TRACE(logical_plan->get_optimizer_context().get_system_stat());
    OPT_TRACE(logical_plan);
  }

  return ret;
}

int ObSql::code_generate(
    ObSqlCtx &sql_ctx,
    ObResultSet &result,
    ObDMLStmt *stmt,
    ObStmtNeedPrivs &stmt_need_privs,
    ObStmtOraNeedPrivs &stmt_ora_need_privs,
    ObIArray<ObAuditUnit> &audit_units,
    ObLogPlan *logical_plan,
    ObPhysicalPlan *&phy_plan)
{
  int ret = OB_SUCCESS;
  FLTSpanGuard(code_generate);
  int64_t last_mem_usage = 0;
  int64_t codegen_mem_usage = 0;
  ObPhysicalPlanCtx *pctx = result.get_exec_context().get_physical_plan_ctx();
  bool use_jit = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(logical_plan) || OB_ISNULL(stmt->get_query_ctx())
        || OB_ISNULL(phy_plan) || OB_ISNULL(sql_ctx.session_info_)
        || OB_ISNULL(pctx)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Logical_plan or phy_plan is NULL", K(ret), K(stmt), K(logical_plan), K(phy_plan),
               "session", sql_ctx.session_info_);
  //} else if (OB_FAIL(need_use_jit(sql_ctx.need_late_compile_,
  //                                (stmt->get_stmt_hint().get_query_hint()).use_jit_policy_,
  //                                *sql_ctx.session_info_,
  //                                use_jit))) {
  //  LOG_WARN("failed to check for needing jitted expr", K(ret));
  } else {
    ObCodeGenerator code_generator(use_jit,
                                   result.get_exec_context().get_min_cluster_version(),
                                   &(pctx->get_datum_param_store()));
    phy_plan->set_is_packed(logical_plan->get_optimizer_context().is_packed());
    bool has_dblink = false;
    if (OB_FAIL(code_generator.generate(*logical_plan, *phy_plan))) {
      LOG_WARN("Failed to generate physical plan", KPC(logical_plan), K(ret));
    } else if (OB_FAIL(ObDblinkUtils::has_reverse_link_or_any_dblink(stmt, has_dblink, true))) {
      LOG_WARN("failed to check dblink in stmt", K(ret));
    } else {
      //session上的ignore_stmt状态给CG使用，在CG结束后需要清空掉
      sql_ctx.session_info_->set_ignore_stmt(false);
      LOG_DEBUG("phy plan", K(*phy_plan));
      phy_plan->stat_.is_use_jit_ = use_jit;
      phy_plan->set_returning(stmt->is_returning());
      phy_plan->set_has_link_table(has_dblink || phy_plan->has_link_udf());
      // set plan insert flag : insert into values(..); // value num is n (n >= 1);
      if (stmt->is_insert_stmt()) {
        ObInsertStmt *insert_stmt = static_cast<ObInsertStmt *>(stmt);
        phy_plan->set_is_plain_insert(!insert_stmt->value_from_select()
                                   && !insert_stmt->is_insert_up()
                                   && insert_stmt->get_subquery_exprs().empty()
                                   && !insert_stmt->is_replace());
      }
      last_mem_usage = phy_plan->get_mem_size();
    }
  }
  //add local_session_var array to phy_plan_ctx
  if (OB_SUCC(ret)) {
    if (OB_FAIL(phy_plan->set_all_local_session_vars(sql_ctx.all_local_session_vars_))) {
      LOG_WARN("set all local sesson vars failed", K(ret));
    }
  }
  NG_TRACE(cg_end);

  // set phy table location in task_exec_ctx, query_timeout in exec_context
  if (OB_SUCC(ret)) {
    ObPhyPlanHint phy_hint(logical_plan->get_optimizer_context().get_global_hint());
    // set larger query_time for IS
    if (stmt->get_query_ctx()->has_is_table_) {
      int tmp_ret = OB_SUCCESS;
      if (phy_hint.query_timeout_ <= 0) {
        if (OB_SUCCESS != (tmp_ret = sql_ctx.session_info_->get_query_timeout(
                    phy_hint.query_timeout_))) {
          LOG_WARN("failed to get sys variable value", K(tmp_ret));
        }
      }
      phy_hint.query_timeout_ *= 10;
    } else {}//do nothing
    ObSEArray<ObTablePartitionInfo *, 3> tbl_part_infos;
    phy_plan->set_phy_plan_hint(phy_hint);//remember in phy_plan
    if (OB_FAIL(ret)) {
      // do nothing
    } else {
      if (OB_FAIL(logical_plan->get_global_table_partition_info(tbl_part_infos))) {
        LOG_WARN("get_global_table_partition_info fails", K(ret));
      } else if (OB_FAIL(sql_ctx.set_partition_infos(
              tbl_part_infos,
              result.get_exec_context().get_allocator()))) {
        LOG_WARN("Failed to set table location in sql ctx", K(ret));
      } else {
        ObDASCtx &das_ctx = DAS_CTX(result.get_exec_context());
        for (int64_t i = 0; OB_SUCC(ret) && i < tbl_part_infos.count(); i++) {
          ObTableLocation &tl = tbl_part_infos.at(i)->get_table_location();
          if (!tl.use_das()) {
            const ObCandiTableLoc &candi_table_loc = tbl_part_infos.at(i)->get_phy_tbl_location_info();
            if (OB_FAIL(das_ctx.add_candi_table_loc(tl.get_loc_meta(), candi_table_loc))) {
              LOG_WARN("add candi table location failed", K(ret), K(tl.get_loc_meta()), K(candi_table_loc));
            }
          }
        } // for end
      }
    }
    // get tablet id and partition id for non partition tables
    if (OB_SUCC(ret)) {
      bool skip_non_partition_optimized = false;
      for (int64_t i = 0; OB_SUCC(ret) && i < tbl_part_infos.count(); i++) {
        ObTableLocation &tl = tbl_part_infos.at(i)->get_table_location();
        if (tl.is_partitioned() || is_virtual_table(tl.get_loc_meta().ref_table_id_)) {
          skip_non_partition_optimized = true;
          break;
        }
      }
      if (OB_SUCC(ret) && !skip_non_partition_optimized) {
        for (int64_t i = 0; OB_SUCC(ret) && i < tbl_part_infos.count(); i++) {
          ObTableLocation &tl = tbl_part_infos.at(i)->get_table_location();
          if (OB_FAIL(tl.calc_not_partitioned_table_ids(result.get_exec_context()))) {
            LOG_WARN("failed to calc not partitioned table ids", K(ret));
          } else {
            tl.set_is_non_partition_optimized(true);
          }
        }
      }
    }
    // set table location for phy_plan
    if (OB_SUCC(ret)) {
      if (OB_FAIL(phy_plan->set_table_locations(tbl_part_infos, *sql_ctx.schema_guard_))) {
        LOG_WARN("fail to set table locations", K(ret));
      }
      LOG_DEBUG("physical plan certain table location", K(tbl_part_infos));
    }
  }

  // set multi stmt info
  if (OB_SUCC(ret)) {
    if (OB_FAIL(sql_ctx.set_multi_stmt_rowkey_pos(logical_plan->get_multi_stmt_rowkey_pos(),
                                                  result.get_exec_context().get_allocator()))) {
      LOG_WARN("failed to set multi stmt rowkey pos", K(ret));
    } else {
      LOG_DEBUG("succeed to set multi stmt rowkey pos", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    bool use_plan_cache = sql_ctx.session_info_->get_local_ob_enable_plan_cache();
    ObPlanCache *plan_cache = NULL;
    if (OB_UNLIKELY(NULL == (plan_cache = sql_ctx.session_info_->get_plan_cache()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Invalid plan cache", K(ret));
    } else {
      if (use_plan_cache) {
        if (OB_FAIL(phy_plan->set_stmt_need_privs(stmt_need_privs))) {
          LOG_WARN("Failed to deep copy", K(ret), K(stmt_need_privs));
        } else if (OB_FAIL(phy_plan->set_stmt_ora_need_privs(stmt_ora_need_privs))) {
          LOG_WARN("Failed to deep copy", K(ret), K(stmt_ora_need_privs));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(phy_plan->init_operator_stats())) {
          LOG_WARN("fail to init operator stats", K(ret));
        } else {
          codegen_mem_usage = phy_plan->get_mem_size() - last_mem_usage;
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(phy_plan->set_audit_units(audit_units))) {
          LOG_WARN("Failed to set audit units", K(ret), K(audit_units));
        }
      }
    }
  }
  LOG_DEBUG("SQL MEM USAGE", K(codegen_mem_usage), K(last_mem_usage));

  return ret;
}

inline int ObSql::sanity_check(ObSqlCtx &context)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    LOG_WARN("ob sql not inited");
  } else if (OB_UNLIKELY(NULL == context.session_info_)
             || OB_UNLIKELY(NULL == context.schema_guard_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument",
             K(ret),
             "session info", context.session_info_,
             "schema manager", context.schema_guard_);
  } else {
    // do nothing
  }
  return ret;
}

int ObSql::init_result_set(ObSqlCtx &context, ObResultSet &result_set)
{
  return init_exec_context(context, result_set.get_exec_context());
}

OB_INLINE int ObSql::init_exec_context(const ObSqlCtx &context, ObExecContext &exec_ctx)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx &task_exec_ctx = exec_ctx.get_task_exec_ctx();
  task_exec_ctx.set_retry_times(context.retry_times_);
  if (OB_FAIL(exec_ctx.create_physical_plan_ctx())) {
    LOG_WARN("faile to create physical plan ctx", K(ret));
  } else {
    exec_ctx.set_my_session(context.session_info_);
    exec_ctx.set_sql_ctx(const_cast<ObSqlCtx*>(&context));
    if (OB_NOT_NULL(exec_ctx.get_physical_plan_ctx()) && OB_NOT_NULL(context.session_info_)) {
      int64_t query_timeout = 0;
      context.session_info_->get_query_timeout(query_timeout);
      exec_ctx.get_physical_plan_ctx()->set_timeout_timestamp(
        context.session_info_->get_query_start_time() + query_timeout);
      exec_ctx.get_physical_plan_ctx()->set_rich_format(
         context.session_info_->use_rich_format());
    }
  }
  return ret;
}

int ObSql::execute_get_plan(ObPlanCache &plan_cache,
                            ObPlanCacheCtx &pc_ctx,
                            ObCacheObjGuard& guard)
{
  int ret = OB_SUCCESS;
  ObIAllocator &allocator = pc_ctx.allocator_;
  ObSQLSessionInfo *session = pc_ctx.sql_ctx_.session_info_;
  ObPhysicalPlanCtx *pctx = pc_ctx.exec_ctx_.get_physical_plan_ctx();
  if (OB_ISNULL(session) || OB_ISNULL(pctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else if (PC_PS_MODE == pc_ctx.mode_ || PC_PL_MODE == pc_ctx.mode_) {
    // TODO change pl mode hit cache as text mode.
    ObPsStmtId stmt_id = pc_ctx.fp_result_.pc_key_.key_id_;
    guard.init(PS_EXEC_HANDLE);
    if (OB_FAIL(plan_cache.get_ps_plan(guard, stmt_id, pc_ctx))) {
      if (OB_SQL_PC_NOT_EXIST == ret || OB_PC_LOCK_CONFLICT == ret) {
        // do nothing
      } else {
        LOG_WARN("fail to get ps physical plan", K(ret));
      }
    }
  } else {
    guard.init(CLI_QUERY_HANDLE);
    if (OB_FAIL(plan_cache.get_plan(allocator, pc_ctx, guard))) {
      if (OB_SQL_PC_NOT_EXIST == ret || OB_PC_LOCK_CONFLICT == ret) {
        // do nothing
      } else {
        LOG_WARN("fail to get physical plan", K(ret), KPC(guard.get_cache_obj()));
      }
    }
  }
  return ret;
}

int ObSql::pc_get_plan_and_fill_result(ObPlanCacheCtx &pc_ctx,
                                       ObResultSet &result,
                                       int &get_plan_err,
                                       bool &need_disconnect)
{
  UNUSED(need_disconnect);
  int ret = OB_SUCCESS;
  ObPhysicalPlan *plan = NULL;
  ObExecContext &exec_ctx = result.get_exec_context();
  ObCacheObjGuard& guard = result.get_cache_obj_guard();

  if (OB_FAIL(pc_get_plan(pc_ctx, guard, get_plan_err,
                          exec_ctx.get_need_disconnect_for_update()))) {
    LOG_DEBUG("fail to get plan", K(ret));
  } else if (OB_SUCCESS != get_plan_err) {
    //get plan from plan cache failed
  } else if ( FALSE_IT(plan = static_cast<ObPhysicalPlan*>(guard.get_cache_obj()))) {
    // do nothing
  } else if (OB_ISNULL(plan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("plan is null", K(ret));
  } else {
    result.set_is_from_plan_cache(true);
    // set handle after get physical plan
    guard.init(pc_ctx.handle_id_);
    if (OB_FAIL(result.from_plan(*plan, pc_ctx.fp_result_.raw_params_))) {
      LOG_WARN("fail to set plan info to ResultSet", K(ret));
    }
  }
  return ret;
}

int ObSql::pc_get_plan(ObPlanCacheCtx &pc_ctx,
                       ObCacheObjGuard& guard,
                       int &get_plan_err,
                       bool &need_disconnect)
{
  ACTIVE_SESSION_FLAG_SETTER_GUARD(in_get_plan_cache);
  int ret = OB_SUCCESS;
  //NG_TRACE(cache_get_plan_begin);
  ObPlanCache *plan_cache = NULL;
  ObSQLSessionInfo *session = pc_ctx.sql_ctx_.session_info_;
  if (OB_ISNULL(session) || OB_ISNULL(plan_cache = session->get_plan_cache())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid plan cache", K(ret), K(session), K(plan_cache));
  } else if (OB_FAIL(execute_get_plan(*plan_cache, pc_ctx, guard))) {
    if (OB_EAGAIN == ret
        || OB_REACH_MAX_CONCURRENT_NUM == ret
        || OB_ARRAY_BINDING_ROLLBACK == ret
        || OB_ERR_PROXY_REROUTE == ret
        || OB_BATCHED_MULTI_STMT_ROLLBACK == ret
        || OB_NEED_SWITCH_CONSUMER_GROUP == ret) {
      /*do nothing*/
    } else if (!(PC_PS_MODE == pc_ctx.mode_ || PC_PL_MODE == pc_ctx.mode_)
               && OB_PC_LOCK_CONFLICT == ret
               && !session->is_inner()) {
      //不是ps模式, 不是inner sql, 且plan cache锁超时, 后面会放入大查询队列列,
      //是ps模式或inner sql, 则不能丢队列, 走硬解析,
      //ps暂不支持丢入大查询丢列, TODO shengle 后面单独添加,
      //inner sql不能丢入大查询队列, 因为有可能上层查询已有数据返回客户端
    } else {
      get_plan_err = ret;
      int tmp_ret = OB_SUCCESS;
      tmp_ret = OB_E(EventTable::EN_PC_NOT_SWALLOW_ERROR) OB_SUCCESS;
      if (OB_SUCCESS != tmp_ret) {
         // do nothing
        if (OB_SQL_PC_NOT_EXIST == ret || OB_REACH_MEMORY_LIMIT == ret) {
          ret = OB_SUCCESS;
        }
      } else {
        ret = OB_SUCCESS; //get plan出错, 覆盖错误码, 确保因plan cache的错误不影响正常执行路径
      }
    }
  } else { //get plan 成功
    plan_cache->inc_hit_and_access_cnt();
    ObPhysicalPlan* plan = static_cast<ObPhysicalPlan*>(guard.get_cache_obj());
    if (OB_ISNULL(plan)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get plan cache");
    } else {
      // 命中了plan cache，则不可能是commit或rollback语句，默认不断连接
      need_disconnect = false;
      //FIXME qianfu NG_TRACE_EXT(set_need_disconnect, OB_ID(need_disconnect), false);
      pc_ctx.sql_ctx_.plan_cache_hit_ = true;
      session->set_early_lock_release(plan->stat_.enable_early_lock_release_);
      //极限性能场景下(perf_event=true)，不再校验权限信息
      if (OB_SUCC(ret) && !pc_ctx.sql_ctx_.is_remote_sql_ && GCONF.enable_perf_event) {
        //如果是remote sql第二次重入plan cache，不需要再做权限检查，因为在第一次进入plan cache已经检查过了
        if (OB_FAIL(ObPrivilegeCheck::check_read_only(pc_ctx.sql_ctx_, plan->get_stmt_type(), false,
                                                      plan->get_stmt_need_privs()))) {
          LOG_WARN("database or table is read only, cannot execute this stmt");
        } else if (!ObSchemaChecker::is_ora_priv_check()) {
          if (OB_FAIL(ObPrivilegeCheck::check_privilege(
                                          pc_ctx.sql_ctx_,
                                          plan->get_stmt_need_privs()))) {
            LOG_WARN("No privilege", K(ret), "stmt_need_priv", plan->get_stmt_need_privs());
          } else {
            LOG_DEBUG("cached phy plan", K(*plan));
            NG_TRACE(check_priv);
          }
        } else if (OB_FAIL(ObPrivilegeCheck::check_ora_privilege(
                                               pc_ctx.sql_ctx_,
                                               plan->get_stmt_ora_need_privs()))) {
          LOG_WARN("No privilege", K(ret), "stmt_need_priv", plan->get_stmt_ora_need_privs());
        } else {
          LOG_DEBUG("cached phy plan", K(*plan));
          NG_TRACE(check_priv);
        }
        if (OB_SUCC(ret) && OB_FAIL(ObPrivilegeCheck::check_password_expired(pc_ctx.sql_ctx_,
                                                                            stmt::T_NONE))) {
          LOG_WARN("Falied to check password expired", K(ret));
        }
      }
    }
  }
  FLT_SET_TAG(hit_plan, pc_ctx.sql_ctx_.plan_cache_hit_);
  if (OB_ERR_PROXY_REROUTE == ret || OB_REACH_MAX_CONCURRENT_NUM == ret
      || OB_NEED_SWITCH_CONSUMER_GROUP == ret) {
    // 如果sql需要二次路由，不应该断连接
    need_disconnect = false;
  }
  return ret;
}

int ObSql::get_outline_data(ObSqlCtx &context,
                            ObPlanCacheCtx &pc_ctx,
                            const ObString &signature_sql,
                            ObOutlineState &outline_state,
                            ParseResult &outline_parse_result)
{
  NG_TRACE(transform_with_outline_begin);
  int ret = OB_SUCCESS;
  memset(&outline_parse_result, 0, sizeof(ParseResult));
  ObString outline_content;
  ObSQLSessionInfo *session = pc_ctx.sql_ctx_.session_info_;
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null session info", K(ret));
  } else if (0 != context.first_plan_hash_) {
    outline_content = context.first_outline_data_;
  } else if (OB_INVALID_ID == context.spm_ctx_.bl_key_.db_id_
             || context.is_batch_params_execute()) {
    //no outline is available when database name of session is not specified, just keep the stmt
  } else if (pc_ctx.is_begin_commit_stmt()) {
    /* do nothing */
#ifdef OB_BUILD_SPM
  } else if (pc_ctx.sql_ctx_.spm_ctx_.is_retry_for_spm_) {
    ObPlanBaselineItem* baseline_item =
        static_cast<ObPlanBaselineItem*>(pc_ctx.sql_ctx_.spm_ctx_.baseline_guard_.get_cache_obj());
    if (OB_ISNULL(baseline_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else {
      outline_content = baseline_item->get_outline_data_str();
    }
#endif
  } else if (OB_FAIL(get_outline_data(pc_ctx, signature_sql, outline_state, outline_content))) {
    LOG_WARN("failed to get outline data", K(ret));
  }

  if (OB_SUCC(ret) && !outline_content.empty()) {
    ObParser parser(pc_ctx.allocator_, session->get_sql_mode(), session->get_charsets4parser(), pc_ctx.def_name_ctx_);
    ObSqlString sql_helper;
    ObString temp_outline_sql;
    if (OB_FAIL(sql_helper.assign_fmt("select %.*s 1 from dual", outline_content.length(),
                                                                 outline_content.ptr()))) {
      LOG_WARN("failed to temp outline data sql", K(outline_content), K(ret));
    } else if (OB_FAIL(ob_write_string(pc_ctx.allocator_, sql_helper.string(), temp_outline_sql))) {
      LOG_WARN("failed to write string", K(outline_content), K(ret));
    } else if (OB_FAIL(parser.parse(temp_outline_sql, outline_parse_result))) {
#ifdef OB_BUILD_SPM
      if (pc_ctx.sql_ctx_.spm_ctx_.is_retry_for_spm_) {
        LOG_WARN("failed to parse outline data result for spm", K(ret), K(temp_outline_sql));
      } else
#endif
      {
        LOG_WARN("failed to parse outline data result", K(ret), K(temp_outline_sql));
        outline_state.reset();
        ret = OB_SUCCESS;
      }
    }
  }
  NG_TRACE(transform_with_outline_end);
  return ret;
}

int ObSql::get_outline_data(ObPlanCacheCtx &pc_ctx,
                            const ObString &signature_sql,
                            ObOutlineState &outline_state,
                            ObString &outline_content)
{
  int ret = OB_SUCCESS;
  const ObOutlineInfo *outline_info = NULL;
  ObSchemaGetterGuard *schema_guard = pc_ctx.sql_ctx_.schema_guard_;
  const uint64_t database_id = pc_ctx.sql_ctx_.spm_ctx_.bl_key_.db_id_;
  const ObString sql_id = pc_ctx.sql_ctx_.spm_ctx_.bl_key_.sql_id_;
  ObSQLSessionInfo *session = pc_ctx.sql_ctx_.session_info_;
  outline_state.reset();
  int64_t schema_version = OB_INVALID_VERSION;
  if (OB_ISNULL(session) || OB_ISNULL(schema_guard)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(session), K(schema_guard));
  } else if (OB_FAIL(schema_guard->get_schema_version(session->get_effective_tenant_id(), schema_version))) {
    LOG_WARN("fail to get schema version", K(ret), K(session->get_effective_tenant_id()));
  } else if (OB_CORE_SCHEMA_VERSION >= schema_version) {
    // local schema is fall behind, do not use outline
  } else {
    char *buf = NULL;
    int64_t pos = 0;
    int64_t size = signature_sql.get_serialize_size();
    ObString outline_key;
    ObIAllocator &allocator = CURRENT_CONTEXT->get_arena_allocator();
    if (0 == size) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("signature sql serialize size is 0", K(ret), K(signature_sql));
    } else if (OB_ISNULL(buf = (char *)allocator.alloc(size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("fail to alloc mem", K(ret));
    } else if (OB_FAIL(signature_sql.serialize(buf, size, pos))) {
      LOG_WARN("fail to serialize key", K(ret));
    } else if (OB_FALSE_IT(outline_key.assign_ptr(buf, static_cast<ObString::obstr_size_t>(pos)))) {
    } else if (OB_FAIL(schema_guard->get_outline_info_with_signature(session->get_effective_tenant_id(),
                                                                     database_id,
                                                                     outline_key,
                                                                     outline_info))) {
      LOG_WARN("failed to get outline info", K(session->get_effective_tenant_id()),
                                              K(signature_sql), K(ret));
      ret = OB_SUCCESS;
    } else if (NULL == outline_info &&
               OB_FAIL(schema_guard->get_outline_info_with_sql_id(session->get_effective_tenant_id(),
                                                                database_id,
                                                                sql_id,
                                                                outline_info))) {
      LOG_WARN("failed to get outline info", K(session->get_effective_tenant_id()), K(ret));
      ret = OB_SUCCESS;
    }
  }


  if (OB_SUCC(ret) && NULL != outline_info) {
    ObString outline_content_copy = outline_info->get_outline_content_str();
    if (OB_FAIL(ObSQLUtils::convert_sql_text_from_schema_for_resolve(pc_ctx.allocator_,
                                                                     session->get_dtc_params(),
                                                                     outline_content_copy))) {
      //outline_content is stored using UTF8, if client sql is GBK, convert may failed
      LOG_WARN("fail to convert sql text", K(ret));
      outline_content = ObString::make_empty_string();
      ret = OB_SUCCESS;
    } else {
      outline_content = outline_content_copy;
      outline_state.is_plan_fixed_ = !outline_info->get_outline_content_str().empty();
      outline_state.outline_version_.object_id_ = outline_info->get_outline_id();
      outline_state.outline_version_.version_ = outline_info->get_schema_version();
      outline_state.outline_version_.object_type_ = DEPENDENCY_OUTLINE;
      if (outline_info->has_outline_params()) {
        pc_ctx.exec_ctx_.set_outline_params_wrapper(&outline_info->get_outline_params_wrapper());
      }
    }
  }
  return ret;
}

int ObSql::parser_and_check(const ObString &outlined_stmt,
                            ObExecContext &exec_ctx,
                            ObPlanCacheCtx &pc_ctx,
                            ParseResult &parse_result,
                            int get_plan_err,
                            bool &add_plan_to_pc,
                            bool &is_enable_transform_tree)
{
  int ret = OB_SUCCESS;
  ObIAllocator &allocator = pc_ctx.allocator_;
  ObSQLSessionInfo *session = exec_ctx.get_my_session();
  ObPhysicalPlanCtx *pctx = exec_ctx.get_physical_plan_ctx();
  bool is_stack_overflow = false;
  bool is_show_variables = false;
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret), K(is_stack_overflow));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  } else if (OB_ISNULL(pctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    pctx->reset_datum_param_store();
    pctx->get_param_store_for_update().reuse();
    ObParser parser(allocator, session->get_sql_mode(), session->get_charsets4parser(), pc_ctx.def_name_ctx_);
    if (OB_FAIL(parser.parse(outlined_stmt, parse_result,
                             pc_ctx.is_rewrite_sql_ ? UDR_SQL_MODE : STD_MODE,
                             pc_ctx.sql_ctx_.handle_batched_multi_stmt(),
                             false, lib::is_mysql_mode() && NULL != session->get_pl_context()))) {
      LOG_WARN("Generate syntax tree failed", K(ret),
               "outlined_stmt", parse_result.contain_sensitive_data_ ? ObString(OB_MASKED_STR) : outlined_stmt);
    } else if ((PC_PS_MODE == pc_ctx.mode_ || PC_PL_MODE == pc_ctx.mode_)
      && OB_FAIL(construct_param_store_from_parameterized_params(
                    pc_ctx, pctx->get_param_store_for_update()))) {
      LOG_WARN("construct param store failed", K(ret));
    }
    pc_ctx.sql_ctx_.is_sensitive_ |= parse_result.contain_sensitive_data_;
    if (OB_SUCC(ret)) {
      // parser返回成功
      if (OB_ISNULL(parse_result.result_tree_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("parse result tree is NULL", K(ret));
      } else if (OB_UNLIKELY(parse_result.result_tree_->num_child_ < 1)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("child count of parse result tree is less than 1",
                  K(ret), K(parse_result.result_tree_->num_child_));
      } else if (OB_ISNULL(parse_result.result_tree_->children_[0])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("children_[0] ofparse result tree is NULL",
                  K(ret), K(parse_result.result_tree_->num_child_));
      } else {
        ObItemType parse_stmt_type = parse_result.result_tree_->children_[0]->type_;
        if (T_COMMIT == parse_stmt_type || T_ROLLBACK == parse_stmt_type) {
          // 是commit或者rollback语句，默认断连接
        } else {
          // 不是commit或者rollback语句，默认不断连接
          exec_ctx.set_need_disconnect(false);
          //FIXME qianfu NG_TRACE_EXT(set_need_disconnect, OB_ID(need_disconnect), false);
        }
      }
    } else if (!ObSQLUtils::check_need_disconnect_parser_err(ret)) {
      exec_ctx.set_need_disconnect(false);
    } else {
      // parser返回未知的错误码，需要断掉与客户端的连接
      LOG_WARN("parser error number is unexpected, need disconnect", K(ret));
    }
    if (OB_SUCC(ret)) {
      stmt::StmtType stmt_type = stmt::T_NONE;
      if (OB_ISNULL(parse_result.result_tree_)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid args", K(ret), KP(parse_result.result_tree_));
      } else if (OB_ISNULL(parse_result.result_tree_->children_)
                 || OB_UNLIKELY(parse_result.result_tree_->num_child_ < 1)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid args", K(ret), KP(parse_result.result_tree_->children_),
                 "number of children", parse_result.result_tree_->num_child_);
      } else {
        ParseNode *children_node = parse_result.result_tree_->children_[0];
        if (OB_ISNULL(children_node)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid args", K(ret), KP(children_node));
        //除了普通的dml stmt,explain stmt中存在?也需要这里一起判断
        } else if (!(PC_PS_MODE == pc_ctx.mode_ || PC_PL_MODE == pc_ctx.mode_)
                   && (children_node->type_ == T_EXPLAIN || IS_DML_STMT(children_node->type_))
                   && (children_node->value_ > 0)) {
          ret = OB_ERR_PARSE_SQL;//children_node->value_ > 0，说明具有question_mark
          const char *err_msg = "?";
          int32_t str_len = static_cast<int32_t>(strlen(err_msg));
          int32_t line_no = 1;
          LOG_USER_ERROR(OB_ERR_PARSE_SQL, ob_errpkt_strerror(OB_ERR_PARSER_SYNTAX, false), str_len, err_msg, line_no);
          LOG_WARN("the text query is invalid", K(outlined_stmt), K(children_node->value_), K(ret));
        } else if (OB_FAIL(ObResolverUtils::resolve_stmt_type(parse_result, stmt_type))) {
          LOG_WARN("failed to resolve stmt type", K(ret));
        } else {
          ObItemType type = children_node->type_;
          //如果是非DML语句, 则不进入plan cache
          ObPlanCache *plan_cache = NULL;
          if (T_SHOW_VARIABLES == type) {
            is_show_variables = true;
          }
          if (ObStmt::is_ddl_stmt(stmt_type, true)) {
            THIS_WORKER.set_timeout_ts(session->get_query_start_time() + GCONF._ob_ddl_timeout);
          }
          if (IS_DML_STMT(type) || is_show_variables) {
            if (OB_UNLIKELY(NULL == (plan_cache = session->get_plan_cache()))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("Invalid plan cache", K(ret));
            } else {
              plan_cache->inc_access_cnt();
#ifndef OB_BUILD_SPM
              if (OB_SQL_PC_NOT_EXIST == get_plan_err) {
#else
              if (OB_SQL_PC_NOT_EXIST == get_plan_err || pc_ctx.sql_ctx_.spm_ctx_.is_retry_for_spm_) {
#endif
                add_plan_to_pc = true;
              } else {
                add_plan_to_pc = false;
              }
            }
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    //租户级别的read only检查
    if ((session->is_inner() && !session->is_user_session()) || pc_ctx.is_begin_commit_stmt()) {
      // FIXME:
      // schema拆分后，为了避免建租户时获取不到租户read only属性导致建租户失败，对于inner sql
      // 暂时跳过read only检查。实际上，对于tenant space系统表，不应该检查read only属性。
    } else if (OB_ISNULL(pc_ctx.sql_ctx_.schema_guard_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid argument", K(pc_ctx.sql_ctx_.schema_guard_));
    } else if (OB_FAIL(check_read_only_privilege(parse_result, exec_ctx,
        *pc_ctx.sql_ctx_.schema_guard_, pc_ctx.sql_traits_))) {
      LOG_WARN("failed to check read only privilege", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
     // do nothing
  } else if (!is_enable_transform_tree) {
    //pc_ctx.fp_result_.pc_key_.name_
    if (OB_FAIL(ob_write_string(allocator,
                                pc_ctx.raw_sql_,
                                pc_ctx.fp_result_.pc_key_.name_))) {
      LOG_WARN("failed to deep copy string", K(pc_ctx.raw_sql_), K(ret));
    } else if (OB_FAIL(ob_write_string(allocator,
                                       pc_ctx.raw_sql_,
                                       pc_ctx.sql_ctx_.spm_ctx_.bl_key_.constructed_sql_))) {
      LOG_WARN("failed to deep copy string", K(pc_ctx.raw_sql_), K(ret));
    }
  } else {
    //对于create outline限流语句，可能会带有问题。我们需要对?做特殊处理,
    //所以也需要经过transform_systax_tree
    bool flag = false;
    if ((add_plan_to_pc && !is_show_variables)
        || ((T_CREATE_OUTLINE == parse_result.result_tree_->children_[0]->type_
             || T_ALTER_OUTLINE == parse_result.result_tree_->children_[0]->type_)
            && (INT64_MAX != parse_result.result_tree_->children_[0]->value_))) {
      flag = true;
      if (T_CREATE_OUTLINE == parse_result.result_tree_->children_[0]->type_) {
        if (1 != parse_result.result_tree_->children_[0]->children_[2]->value_) {
          flag = false;
        }
      }
    } else if (pc_ctx.sql_ctx_.multi_stmt_item_.is_batched_multi_stmt()) {
      if (T_EXPLAIN == parse_result.result_tree_->children_[0]->type_) {
        flag = true;
      }
    }
    if (flag) {
      bool is_transform_outline = (T_CREATE_OUTLINE == parse_result.result_tree_->children_[0]->type_
                                   || T_ALTER_OUTLINE == parse_result.result_tree_->children_[0]->type_);
      if (is_transform_outline) {
        LOG_WARN("is_transform_outline", K(is_transform_outline));
      }
      if (OB_FAIL(ObSqlParameterization::parameterize_syntax_tree(allocator,
                                                                  is_transform_outline,
                                                                  pc_ctx,
                                                                  parse_result.result_tree_,
                                                                  pctx->get_param_store_for_update(),
                                                                  session->get_charsets4parser()))) {
        bool need_retry_param = true;
        int tmp_ret = OB_SUCCESS;
        tmp_ret = OB_E(EventTable::EN_SQL_PARAM_FP_NP_NOT_SAME_ERROR) OB_SUCCESS;
        if (OB_SUCCESS != tmp_ret) {
          if (OB_NOT_SUPPORTED == ret) {
            need_retry_param = false;
          }
        }
        if (!need_retry_param) {
          // do nothing
        } else if (is_transform_outline) {
          LOG_WARN("fail to parameterize syntax tree", K(ret));
        } else {
          //如果是因为参数化出错, 则需要重新进行parser, 生成新的parser tree, 之前的parser tree可能部分已参数化,
          //并标记该查询不进plan cache，且下次不需要进行参数化, 从而确保参数化时出错不影响正常执行。
          pctx->reset_datum_param_store();
          is_enable_transform_tree = false;
          if (OB_FAIL(SMART_CALL(parser_and_check(outlined_stmt,
                                                  exec_ctx,
                                                  pc_ctx,
                                                  parse_result,
                                                  get_plan_err,
                                                  add_plan_to_pc,
                                                  is_enable_transform_tree)))) {
            LOG_WARN("fail to parameterize syntax tree", K(ret));
          }
          add_plan_to_pc = false;
        }
      } else {
        parse_result.question_mark_ctx_.count_ = static_cast<int> (pctx->get_param_store().count());
      }
    }
  }

  return ret;
}

int ObSql::pc_add_plan(ObPlanCacheCtx &pc_ctx,
                       ObResultSet &result,
                       ObOutlineState &outline_state,
                       ObPlanCache *plan_cache,
                       bool& plan_added)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlan *phy_plan = result.get_physical_plan();
  pc_ctx.fp_result_.pc_key_.namespace_ = ObLibCacheNameSpace::NS_CRSR;
  plan_added = false;
  bool is_batch_exec = pc_ctx.sql_ctx_.is_batch_params_execute();
  bool enable_udr = pc_ctx.sql_ctx_.get_enable_user_defined_rewrite();
  if (OB_ISNULL(phy_plan) || OB_ISNULL(plan_cache)) {
    ret = OB_NOT_INIT;
    LOG_WARN("Fail to generate plan", K(phy_plan), K(plan_cache));
  } else if (OB_USE_PLAN_CACHE_NONE == phy_plan->get_phy_plan_hint().plan_cache_policy_) {
    LOG_DEBUG("Hint not use plan cache");
    if (is_batch_exec) {
      ret = OB_BATCHED_MULTI_STMT_ROLLBACK;
      LOG_WARN("with not use plan_cache hint, batched multi_stmt needs rollback", K(ret));
    }
  } else if (OB_FAIL(result.to_plan(pc_ctx.mode_, phy_plan))) {
    LOG_WARN("Failed copy field to pplan", K(ret));
  } else if (OB_FAIL(ob_write_string(phy_plan->get_allocator(),
                                     (PC_PS_MODE == pc_ctx.mode_ || PC_PL_MODE == pc_ctx.mode_)
                                       ? pc_ctx.raw_sql_ :
                                       pc_ctx.sql_ctx_.spm_ctx_.bl_key_.constructed_sql_,
                                     phy_plan->stat_.constructed_sql_))) {
    LOG_WARN("failed to ob write string", K(ret));
  } else if (OB_FAIL(ob_write_string(phy_plan->get_allocator(),
                                     pc_ctx.sql_ctx_.spm_ctx_.bl_key_.sql_id_,
                                     phy_plan->stat_.sql_id_))) {
    LOG_WARN("failed to ob write string", K(ret));
  } else {
    sql::ObUDRMgr *rule_mgr = MTL(sql::ObUDRMgr*);
    phy_plan->set_outline_state(outline_state);
    phy_plan->stat_.db_id_ = pc_ctx.sql_ctx_.spm_ctx_.bl_key_.db_id_;
    phy_plan->stat_.is_rewrite_sql_ = pc_ctx.is_rewrite_sql_;
    phy_plan->stat_.rule_version_ = rule_mgr->get_rule_version();
    phy_plan->stat_.enable_udr_ = enable_udr;

    if (PC_PS_MODE == pc_ctx.mode_ || PC_PL_MODE == pc_ctx.mode_) {
      // pc_key_ may be modified elsewhere, so reset it before adding plan
      pc_ctx.fp_result_.pc_key_.key_id_ = pc_ctx.sql_ctx_.statement_id_;
      //远程SQL第二次进入plan，将raw_sql作为pc_key存入plan cache中，
      //然后使用ps接口直接用参数化后的sql作为key来查plan cache，可以节省一次对SQL fast parse的代价
      if (pc_ctx.sql_ctx_.is_remote_sql_) {
        //由于现在ps模式和普通的文本协议的执行计划不能复用，因此这里需要区分，避免在查询plan的时候引起一些问题
        //由于普通的文本协议key_id是OB_INVALID_ID,因此这里使用key_id=0+name=参数化SQL的方式来区分
        //@todo: shengle 普通的ps协议和文本协议的计划共享也存在同样的问题，这里需要统一解决一下
        pc_ctx.fp_result_.pc_key_.key_id_ = 0;
        pc_ctx.fp_result_.pc_key_.name_ = pc_ctx.raw_sql_;
      }
      ret = plan_cache->add_ps_plan(phy_plan, pc_ctx);
    } else {
      check_template_sql_can_be_prepare(pc_ctx, *phy_plan);
      ret = plan_cache->add_plan(phy_plan, pc_ctx);
    }
    plan_added = (OB_SUCCESS == ret);
    if (pc_ctx.is_max_curr_limit_) {
      ret = OB_REACH_MAX_CONCURRENT_NUM;
    }

    int tmp_ret = OB_SUCCESS;
    tmp_ret = OB_E(EventTable::EN_PC_NOT_SWALLOW_ERROR) OB_SUCCESS;
    if (is_batch_exec) {
      // Batch optimization cannot continue for errors other than OB_SQL_PC_PLAN_DUPLICATE.
      if (OB_SQL_PC_PLAN_DUPLICATE == ret) {
        ret = OB_SUCCESS;
        LOG_DEBUG("this plan has been added by others, need not add again", K(phy_plan));
      } else if (OB_FAIL(ret)) {
        LOG_WARN("some unexpected error occured", K(ret));
        ret = OB_BATCHED_MULTI_STMT_ROLLBACK;
      } else {
        pc_ctx.sql_ctx_.self_add_plan_ = true;
        LOG_DEBUG("Successed to add batch plan to ObPlanCache", K(phy_plan));
      }
    } else if (OB_SQL_PC_PLAN_DUPLICATE == ret) {
      ret = OB_SUCCESS;
      LOG_DEBUG("this plan has been added by others, need not add again", K(phy_plan));
    } else if (OB_REACH_MEMORY_LIMIT == ret || OB_SQL_PC_PLAN_SIZE_LIMIT == ret) {
      if (REACH_TIME_INTERVAL(1000000)) { //1s, 当内存达到上限时, 该日志打印会比较频繁, 所以以1s为间隔打印
        ObTruncatedString trunc_sql(pc_ctx.raw_sql_);
        LOG_INFO("can't add plan to plan cache",
                 K(ret), K(phy_plan->get_mem_size()), K(trunc_sql),
                 K(plan_cache->get_mem_used()));
      }
      ret = OB_SUCCESS;
    } else if (is_not_supported_err(ret)) {
      ret = OB_SUCCESS;
      LOG_DEBUG("plan cache don't support add this kind of plan now",  K(phy_plan));
    } else if (OB_FAIL(ret)) {
      if (OB_SUCCESS != tmp_ret) {

      } else {
        if (OB_REACH_MAX_CONCURRENT_NUM != ret) { //如果是达到限流上限, 则将错误码抛出去
          ret = OB_SUCCESS; //add plan出错, 覆盖错误码, 确保因plan cache失败不影响正常执行路径
          LOG_WARN("Failed to add plan to ObPlanCache", K(ret));
        }
      }
    } else {
      pc_ctx.sql_ctx_.self_add_plan_ = true;
      LOG_DEBUG("Successed to add plan to ObPlanCache", K(phy_plan));
    }
  }

  return ret;
}

//检查经过参数化的模板SQL能否被prepare
//目前有一些SQL如果走文本协议，plan cache参数化后的模板SQL并不能直接用来在远端prepare
//会报语法错误，例如:select * from t1 where a=_utf8'binary';
//模板化后的SQL为:select * from t1 where a=_utf8?;这条SQL在parser中会报语法错误
//而对于大多数参数化后的模板SQL可以直接用来在远端prepare，避免再对文本进行一次fast parser
//因此这里对模板SQL进行一次parser，增加对模板SQL的检查，用来判该模板SQL是否可以在远端被prepare
void ObSql::check_template_sql_can_be_prepare(ObPlanCacheCtx &pc_ctx, ObPhysicalPlan &plan)
{
  int ret = OB_SUCCESS;
  const ObString &temp_sql = plan.get_constructed_sql();
  ObSQLSessionInfo *session = pc_ctx.sql_ctx_.session_info_;
  if (plan.is_remote_plan() && !temp_sql.empty() && session != nullptr
      && pc_ctx.select_item_param_infos_.empty()) {
    // select * from (select 1, 2, 3 from dual);这样的SQL也不能在远端被prepare，因为select子句会被参数化
    ParseResult parse_result;
    ObParser parser(pc_ctx.allocator_,
                    session->get_sql_mode(),
                    session->get_charsets4parser(),
                    pc_ctx.def_name_ctx_);
    if (OB_FAIL(parser.parse(temp_sql, parse_result))) {
      LOG_DEBUG("generate syntax tree failed", K(temp_sql), K(ret));
    } else {
      plan.set_temp_sql_can_prepare();
    }
  }
}

int ObSql::after_get_plan(ObPlanCacheCtx &pc_ctx,
                          ObSQLSessionInfo &session,
                          ObPhysicalPlan *phy_plan,
                          bool from_plan_cache,
                          const ParamStore *ps_params,
                          uint64_t min_cluster_version)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *pctx = pc_ctx.exec_ctx_.get_physical_plan_ctx();
  bool enable_send_plan_event = EVENT_CALL(EventTable::EN_DISABLE_REMOTE_EXEC_WITH_PLAN) == 0;
  bool evolution_plan = nullptr != phy_plan && phy_plan->get_evolution();
  bool enable_send_plan = (session.get_is_in_retry() || evolution_plan) && enable_send_plan_event;
  int last_query_retry_err = session.get_retry_info().get_last_query_retry_err();
  if (OB_TRANSACTION_SET_VIOLATION == last_query_retry_err
      || OB_TRY_LOCK_ROW_CONFLICT == last_query_retry_err) {
    enable_send_plan = false;
  }
  LOG_DEBUG("before after_get_plan", K(enable_send_plan), K(enable_send_plan_event),
            "is_retry",session.get_is_in_retry(), K(last_query_retry_err), K(evolution_plan));
//  LOG_INFO("after get paln", K(pctx), K(phy_plan));
  if (NULL != pctx) {
    if (NULL != phy_plan) {
      // record the plan id in trace_event, perf_event and atomic_event
      NG_TRACE_EXT(plan_id, OB_ID(plan_id), phy_plan->get_plan_id());
      OB_ATOMIC_EVENT_SET_CAT_ID(phy_plan->get_plan_id());
      PERF_SET_CAT_ID(phy_plan->get_plan_id());
      if (OB_MAX_SQL_ID_LENGTH != phy_plan->stat_.sql_id_.length()) {
        if (OB_FAIL(ob_write_string(phy_plan->get_allocator(),
                                    pc_ctx.sql_ctx_.sql_id_,
                                    phy_plan->stat_.sql_id_))) {
          LOG_WARN("failed to ob write string", K(ret));
        }
      }
      // init auto increment param
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(pc_ctx.exec_ctx_.init_physical_plan_ctx(*phy_plan))) {
        LOG_WARN("fail init exec context", K(ret), K(phy_plan->get_stmt_type()));
      } else if (OB_FAIL(DAS_CTX(pc_ctx.exec_ctx_).init(*phy_plan, pc_ctx.exec_ctx_))) {
        LOG_WARN("init das context failed", K(ret));
      } else if (OB_FAIL(pctx->set_autoinc_params(phy_plan->get_autoinc_params()))) {
        LOG_WARN("failed to set autoinc params", K(ret));
      } else {
        pctx->set_tablet_autoinc_param(phy_plan->get_tablet_autoinc_param());
        ObIArray<AutoincParam> &autoinc_params = pctx->get_autoinc_params();
        for (int64_t i = 0; OB_SUCC(ret) && i < autoinc_params.count(); ++i) {
          AutoincParam &param = autoinc_params.at(i);
          // Since 4.0, there should be only one autoinc_param
          param.autoinc_increment_ = session.get_local_auto_increment_increment();
          param.autoinc_offset_ = session.get_local_auto_increment_offset();
          if (pc_ctx.sql_ctx_.is_do_insert_batch_opt()) {
            param.total_value_count_ = pc_ctx.sql_ctx_.get_insert_batch_row_cnt();
          } else if (phy_plan->is_plain_insert() && pctx->get_array_param_groups().count() == 1) {
            param.total_value_count_ = pctx->get_array_param_groups().at(0).row_count_;
          }
        }  // end for
      }

      if (OB_SUCC(ret)) {
        DAS_CTX(pc_ctx.exec_ctx_).unmark_need_check_server();
        bool need_reroute = false;
        if (OB_FAIL(check_need_reroute(pc_ctx, session, phy_plan, need_reroute))) {
          LOG_WARN("fail to check need reroute", K(ret));
        } else if (need_reroute) {
          ret = OB_ERR_PROXY_REROUTE;
        }
      }

      // the purpose of adding condition (!session.get_is_in_retry()) is
      // send the plan instead of continue sending sqlinfo when retrying remotely.
      // bug:
      if (OB_SUCC(ret) && phy_plan->is_remote_plan()
          && !phy_plan->contains_temp_table()
          && !enable_send_plan) {
        pctx->get_remote_sql_info().sql_from_pl_ = PC_PL_MODE == pc_ctx.mode_;
        //处理远程plan转发SQL的情况
        ParamStore &param_store = pctx->get_param_store_for_update();
        if (OB_NOT_NULL(ps_params)) {
          //本地是ps协议，远端依然走ps接口
          int64_t initial_param_count = ps_params->count();
          //对于ps协议为什么不使用用户传递下来的ps_params?因为对于Oracle模式下''等价于NULL
          //这里需要做一次转换，而param store里的param是转换后的，因此不需要再去转换
          for (int64_t i = param_store.count(); i > initial_param_count; --i) {
            //丢掉计算产生的多余参数，只保留最初的参数，避免第二次生成计划的时候重复计算引起参数位置错误
            param_store.pop_back();
          }
          pctx->get_remote_sql_info().use_ps_ = true;
          pctx->get_remote_sql_info().is_original_ps_mode_ = true;
          //从ps sql info中取出要执行的sql
          pctx->get_remote_sql_info().remote_sql_ = pc_ctx.sql_ctx_.cur_sql_;
          pctx->get_remote_sql_info().ps_params_ = &param_store;
          pctx->get_remote_sql_info().ps_param_cnt_ = static_cast<int32_t>(param_store.count());
        } else if (phy_plan->temp_sql_can_prepare()
            && pc_ctx.neg_param_index_.is_empty()
            && !pc_ctx.sql_ctx_.is_batch_params_execute()
            && !pc_ctx.exec_ctx_.has_dynamic_values_table()) {
          //本地是文本协议的SQL，并且缓存在plan中，走ps协议
          //@TODO:yuchen.wyc 文本协议中如果出现不能参数化的参数，由于param store里的值可能不是参数化对应的值
          //例如select a, b-1 from t1; 这里会参数化成select a, b-? from t1;但param store里对应的是-1
          //这里应该使用raw_params中的信息去解析并将param store中的-1替换成1
          //但是处理太麻烦，暂时先不让这类SQL走远端的ps接口
          //如果是batched_multi_stmt,也直接走文本协议，
          //因为batched update stmt的参数在param store中是一个array,比较特殊
          LOG_DEBUG("after get plan",
                    K(pc_ctx.fp_result_.raw_params_.count()),
                    K(pc_ctx.not_param_info_.count()),
                    K(param_store));
          int64_t initial_param_count = pc_ctx.fp_result_.raw_params_.count() -
              pc_ctx.not_param_index_.num_members();
          for (int64_t i = param_store.count(); i > initial_param_count; --i) {
            //丢掉计算产生的多余参数，只保留最初的参数，避免第二次生成计划的时候重复计算引起参数位置错误
            param_store.pop_back();
          }
          pctx->get_remote_sql_info().use_ps_ = true;
          pctx->get_remote_sql_info().remote_sql_ = phy_plan->get_constructed_sql();
          pctx->get_remote_sql_info().ps_params_ = &param_store;
          pctx->get_remote_sql_info().ps_param_cnt_ = static_cast<int32_t>(param_store.count());
        } else {
          //没有进plan cache，并且是文本协议，在远端再走一次文本解析
          param_store.reset(); //走文本协议不需要携带参数，清空掉
          pctx->get_remote_sql_info().use_ps_ = false;
          pctx->get_remote_sql_info().is_batched_stmt_ =
              pc_ctx.sql_ctx_.multi_stmt_item_.is_batched_multi_stmt();
          pctx->get_remote_sql_info().remote_sql_ = pc_ctx.sql_ctx_.cur_sql_;
          pctx->get_remote_sql_info().ps_params_ = &param_store;
        }
        if (GET_MIN_CLUSTER_VERSION() != min_cluster_version) {
          LOG_INFO("remote plan src diff cluster version",
                   K(GET_MIN_CLUSTER_VERSION()), K(min_cluster_version));
        }
        LOG_DEBUG("generate remote sql info", K(pctx->get_remote_sql_info()),
                  K(session.get_local_ob_enable_plan_cache()),
                  K(pc_ctx.fp_result_.raw_params_), K(pc_ctx.not_param_info_),
                  K(phy_plan->temp_sql_can_prepare()),
                  K(pc_ctx.neg_param_index_),
                  K(pc_ctx.sql_ctx_.multi_stmt_item_.is_batched_multi_stmt()),
                  K(phy_plan->get_constructed_sql()), KPC(ps_params));
      }
    }

    if (OB_SUCC(ret)) {
      // set last_insert_id
      pctx->set_last_insert_id_session(session.get_local_last_insert_id());
    }

    if (OB_SUCC(ret) && NULL != phy_plan) {
      if (from_plan_cache) {
        ObPhyPlanHint &phy_plan_hint = phy_plan->get_phy_plan_hint();
        pc_ctx.sql_ctx_.force_print_trace_ = phy_plan_hint.force_trace_log_;
        //log_level just deal mpquery now.
        if (MpQuery == pc_ctx.sql_ctx_.exec_type_ && phy_plan_hint.log_level_.length() > 0) {
          if (OB_FAIL(process_thread_log_id_level_map(phy_plan_hint.log_level_.ptr(),
                                                      phy_plan_hint.log_level_.length()))) {
            LOG_WARN("Failed to process thread log id level map", K(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret) && NULL != phy_plan && !session.get_is_deserialized() && !session.is_inner()) {
      bool has_session_tmp_table = phy_plan->is_contain_oracle_session_level_temporary_table()
        || phy_plan->contains_temp_table();
      bool has_txn_tmp_table = phy_plan->is_contain_oracle_trx_level_temporary_table();
      if (has_session_tmp_table || has_txn_tmp_table) {
        if (session.is_txn_free_route_temp()) {
          ret = OB_TRANS_FREE_ROUTE_NOT_SUPPORTED;
          LOG_WARN("access temp table is supported to be executed on txn temporary node", KR(ret), K(session.get_txn_free_route_ctx()));
        } else {
          bool is_already_set = false;
          if (OB_FAIL(session.get_session_temp_table_used(is_already_set))) {
            LOG_WARN("fail to get session temp table used", K(ret));
          } else if (is_already_set) {
            //do nothing
          } else if (OB_FAIL(session.set_session_temp_table_used(true))) {
            LOG_WARN("fail to set session temp table used", K(ret));
          }
          LOG_DEBUG("plan contain oracle session level temporary table detected", K(is_already_set));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(append_array_no_dup(session.get_gtt_session_scope_ids(),
                                        phy_plan->get_gtt_session_scope_ids()))) {
          LOG_WARN("fail to append array", K(ret));
        } else if (OB_FAIL(append_array_no_dup(session.get_gtt_trans_scope_ids(),
                                               phy_plan->get_gtt_trans_scope_ids()))) {
          LOG_WARN("fail to append array", K(ret));
        }
      }
    }
    if (OB_SUCC(ret) && NULL != phy_plan && !phy_plan->is_remote_plan()) {
      if (OB_FAIL(pctx->set_all_local_session_vars(phy_plan->get_all_local_session_vars()))) {
        LOG_WARN("fail to set all local session vars", K(ret));
      }
    }
    if (OB_SUCC(ret) && NULL != phy_plan) {
      CK (pc_ctx.exec_ctx_.get_sql_ctx()->schema_guard_ != NULL);
      for (int64_t i = 0; OB_SUCC(ret) && i < phy_plan->get_immediate_refresh_external_table_ids().count(); i++) {
        int64_t object_id = phy_plan->get_immediate_refresh_external_table_ids().at(i);
        OZ (ObExternalTableFileManager::get_instance().refresh_external_table(session.get_effective_tenant_id(),
                                                                              object_id,
                                                                              *pc_ctx.exec_ctx_.get_sql_ctx()->schema_guard_,
                                                                              pc_ctx.exec_ctx_));
      }
    }
  } else {
    // not phy_plan, ignore
  }

  return ret;
}

int ObSql::need_add_plan(const ObPlanCacheCtx &pc_ctx,
                         ObResultSet &result,
                         bool is_enable_pc,
                         bool &need_add_plan)
{
  int ret = OB_SUCCESS;
  result.get_exec_context().get_stmt_factory()->get_query_ctx();
  if (false == need_add_plan) {
    //do nothing
  } else if (!is_enable_pc || !pc_ctx.should_add_plan_) {
    need_add_plan = false;
  } else if (OB_ISNULL(result.get_exec_context().get_stmt_factory()) || OB_ISNULL(result.get_exec_context().get_stmt_factory()->get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null ptr", K(ret), KP(result.get_exec_context().get_stmt_factory()));
  } else if (result.get_exec_context().get_stmt_factory()->get_query_ctx()->has_dblink()) {
    need_add_plan = false;
  }
  return ret;
}

int ObSql::pc_add_udr_plan(const ObUDRItemMgr::UDRItemRefGuard &item_guard,
                           ObPlanCacheCtx &pc_ctx,
                           ObResultSet &result,
                           ObOutlineState &outline_state,
                           bool& plan_added)
{
  int ret = OB_SUCCESS;
  int get_plan_err = OB_SUCCESS;
  bool add_plan_to_pc = false;
  ParseResult parse_result;
  ObIAllocator &allocator = result.get_mem_pool();
  ObSQLSessionInfo &session = result.get_session();
  ObPlanCache *plan_cache = session.get_plan_cache();
  bool is_enable_transform_tree = !session.get_enable_exact_mode();
  ObExecContext &ectx = result.get_exec_context();
  ObPhysicalPlanCtx *pctx = ectx.get_physical_plan_ctx();
  ParamStore param_store( (ObWrapperAllocator(&allocator)) );
  const ObString &raw_sql = pc_ctx.raw_sql_;
  ObPlanCacheCtx tmp_pc_ctx(raw_sql, pc_ctx.mode_,
                            allocator, pc_ctx.sql_ctx_, ectx, session.get_effective_tenant_id());
  tmp_pc_ctx.fp_result_ = pc_ctx.fp_result_;
  tmp_pc_ctx.normal_parse_const_cnt_ = pc_ctx.normal_parse_const_cnt_;
  tmp_pc_ctx.set_is_rewrite_sql(true);
  tmp_pc_ctx.rule_name_ = pc_ctx.rule_name_;
  const ObUDRItem *rule_item = item_guard.get_ref_obj();
  ObParser parser(allocator, session.get_sql_mode(),
                  session.get_charsets4parser(),
                  pc_ctx.def_name_ctx_);
  if (OB_ISNULL(rule_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rule item is null", K(ret));
  } else if (OB_FAIL(tmp_pc_ctx.fixed_param_info_list_.assign(rule_item->get_fixed_param_value_array()))) {
    LOG_WARN("failed to assign fixed param info list", K(ret));
  } else if (OB_FAIL(tmp_pc_ctx.dynamic_param_info_list_.assign(rule_item->get_dynamic_param_info_array()))) {
    LOG_WARN("failed to assign dynamic param info list", K(ret));
  } else if (OB_FAIL(tmp_pc_ctx.tpl_sql_const_cons_.assign(pc_ctx.tpl_sql_const_cons_))) {
    LOG_WARN("failed to assign tpl sql const cons", K(ret));
  } else if (OB_FAIL(parser.parse(raw_sql, parse_result))) {
    LOG_WARN("failed to parse sql", K(ret), K(raw_sql));
  } else if (OB_FAIL(ObSqlParameterization::parameterize_syntax_tree(allocator,
                                                                     false/*is_transform_outline*/,
                                                                     tmp_pc_ctx,
                                                                     parse_result.result_tree_,
                                                                     param_store,
                                                                     session.get_charsets4parser()))) {
    LOG_WARN("parameterize syntax tree failed", K(ret));
  } else if (OB_FAIL(pc_add_plan(tmp_pc_ctx, result, outline_state, plan_cache, plan_added))) {
    LOG_WARN("failed to add plan", K(ret));
  }
  return ret;
}

OB_NOINLINE int ObSql::handle_physical_plan(const ObString &trimed_stmt,
                                            ObSqlCtx &context,
                                            ObResultSet &result,
                                            ObPlanCacheCtx &pc_ctx,
                                            const int get_plan_err)
{
  int ret = OB_SUCCESS;
  FLTSpanGuard(hard_parse);
  bool is_valid = true;
  PlanCacheMode mode = pc_ctx.mode_;
  ObString outlined_stmt = trimed_stmt;//use outline if available
  ObString signature_sql;
  ObOutlineState outline_state;
  ParseResult parse_result;
  ParseResult outline_parse_result;
  bool add_plan_to_pc = false;
  bool is_match_udr = false;
  ObUDRItemMgr::UDRItemRefGuard item_guard;
  UDRBackupRecoveryGuard backup_recovery_guard(context, pc_ctx);
  ObSQLSessionInfo &session = result.get_session();
  ObPlanCache *plan_cache = session.get_plan_cache();
  ObSpmCacheCtx &spm_ctx = context.spm_ctx_;
  bool use_plan_cache = session.get_local_ob_enable_plan_cache();
  // record whether needs to do parameterization at this time,
  // if exact mode is on, not do parameterizaiton
  bool is_enable_transform_tree = !session.get_enable_exact_mode();
  //重新解析前将这两个标记reset掉，避免前面查plan cache的操作导致这两个参数在重新生成plan后会出现不幂等的问题
  pc_ctx.not_param_index_.reset();
  pc_ctx.neg_param_index_.reset();
  bool plan_added = false;
  bool need_get_baseline = false;
#ifdef OB_BUILD_SPM
  spm_ctx.bl_key_.sql_cs_type_ = session.get_local_collation_connection();
#endif
  LOG_DEBUG("gen plan info", K(spm_ctx.bl_key_), K(get_plan_err));
  // for batched multi stmt, we only parse and optimize the first statement
  // only in multi_query, need do this
  if (!(PC_PS_MODE == mode || PC_PL_MODE == mode) &&
      (context.is_batch_params_execute() || pc_ctx.exec_ctx_.has_dynamic_values_table()) &&
      OB_FAIL(get_reconstructed_batch_stmt(pc_ctx, outlined_stmt))) {
    LOG_WARN("failed to get first batched stmt item", K(ret));
  } else if (OB_FAIL(ObUDRUtils::match_udr_and_refill_ctx(outlined_stmt,
                                                          context,
                                                          result,
                                                          pc_ctx,
                                                          is_match_udr,
                                                          item_guard))) {
    LOG_WARN("failed to match udr and refill ctx", K(ret));
  } else if (is_match_udr
    && FALSE_IT(outlined_stmt = item_guard.get_ref_obj()->get_replacement())) {
  } else if (OB_FAIL(handle_parser(outlined_stmt,
                                   result.get_exec_context(),
                                   pc_ctx,
                                   parse_result,
                                   get_plan_err,
                                   add_plan_to_pc,
                                   is_enable_transform_tree))) {
    LOG_WARN("fail to parser and check", K(ret));
  } else if (context.is_batch_params_execute() &&
             !(PC_PS_MODE == mode || PC_PL_MODE == mode) &&
             OB_FAIL(check_batched_multi_stmt_after_parser(pc_ctx,
                                                           parse_result,
                                                           add_plan_to_pc,
                                                           is_valid))) {
    LOG_WARN("failed to check batched multi_stmt", K(ret));
  } else if (!is_valid) {
    ret = OB_BATCHED_MULTI_STMT_ROLLBACK;
    LOG_WARN("batched multi_stmt needs rollback", K(ret));
  }
  generate_sql_id(pc_ctx, add_plan_to_pc, parse_result, signature_sql, ret);
#ifndef OB_BUILD_SPM
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(get_outline_data(context, pc_ctx, signature_sql,
                                      outline_state, outline_parse_result))) {
    LOG_WARN("failed to get outline data for query", K(ret));
  } else if (OB_FAIL(generate_physical_plan(parse_result,
                                            &pc_ctx,
                                            context,
                                            result,
                                            pc_ctx.is_begin_commit_stmt(),
                                            mode,
                                            &outline_parse_result))) {
    if (OB_ERR_PROXY_REROUTE == ret) {
      LOG_DEBUG("Failed to generate plan", K(ret));
    } else {
      LOG_WARN("Failed to generate plan", K(ret), K(result.get_exec_context().need_disconnect()));
    }
  } else if (OB_FALSE_IT(backup_recovery_guard.recovery())) {
  } else if (OB_FAIL(need_add_plan(pc_ctx,
                                   result,
                                   use_plan_cache,
                                   add_plan_to_pc))) { //加入多表分布式计划的判断，判断是否还需需要add plan
    LOG_WARN("get need_add_plan failed", K(ret));
  } else if (!add_plan_to_pc) {
    // do nothing
  } else if (is_match_udr && OB_FAIL(pc_add_udr_plan(item_guard,
                                                     pc_ctx,
                                                     result,
                                                     outline_state,
                                                     plan_added))) {
    LOG_WARN("fail to add plan to plan cache", K(ret));
  } else if (!is_match_udr && OB_FAIL(pc_add_plan(pc_ctx, result, outline_state, plan_cache, plan_added))) {
    LOG_WARN("fail to add plan to plan cache", K(ret));
  }
#else
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(get_outline_data(context, pc_ctx, signature_sql,
                                      outline_state, outline_parse_result))) {
    LOG_WARN("failed to get outline data for query", K(ret));
  } else if (OB_FAIL(generate_physical_plan(parse_result,
                                            &pc_ctx,
                                            context,
                                            result,
                                            pc_ctx.is_begin_commit_stmt(),
                                            mode,
                                            &outline_parse_result))) {
    if (OB_ERR_PROXY_REROUTE == ret) {
      LOG_DEBUG("Failed to generate plan", K(ret));
    } else if (OB_OUTLINE_NOT_REPRODUCIBLE == ret && spm_ctx.is_retry_for_spm_) {
      LOG_TRACE("spm need get baseline due to generate plan failed");
      need_get_baseline = true;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("Failed to generate plan", K(ret), K(result.get_exec_context().need_disconnect()));
    }
  } else if (OB_FALSE_IT(backup_recovery_guard.recovery())) {
  } else if (OB_FAIL(need_add_plan(pc_ctx,
                                   result,
                                   use_plan_cache,
                                   add_plan_to_pc))) { //加入多表分布式计划的判断，判断是否还需需要add plan
    LOG_WARN("get need_add_plan failed", K(ret));
  } else if (!add_plan_to_pc &&
      (context.is_batch_params_execute() && parse_result.result_tree_->children_[0]->type_ != T_EXPLAIN)) {
    ret = OB_BATCHED_MULTI_STMT_ROLLBACK;
    LOG_WARN("add_plan_to_pc is false so batched multi_stmt rollback", K(ret));
  } else if (spm_ctx.is_retry_for_spm_) {
    // handle spm baseline plan
    ObPlanBaselineItem* baseline_item = static_cast<ObPlanBaselineItem*>(spm_ctx.baseline_guard_.get_cache_obj());
    if (OB_ISNULL(result.get_physical_plan()) || OB_ISNULL(baseline_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null physical plan", K(ret), K(result.get_physical_plan()), K(baseline_item));
    } else if (result.get_physical_plan()->get_plan_hash_value() == baseline_item->get_plan_hash_value()) {
      pc_ctx.need_evolution_ = true;
      if (spm_ctx.cur_baseline_not_enable_) {
        spm_ctx.spm_stat_ = ObSpmCacheCtx::SpmStat::STAT_ACCEPT_BASELINE_PLAN;
      } else {
        spm_ctx.spm_stat_ = ObSpmCacheCtx::SpmStat::STAT_ADD_BASELINE_PLAN;
      }
      if (OB_FAIL(pc_add_plan(pc_ctx, result, outline_state, plan_cache, plan_added))) {
        LOG_WARN("fail to add plan to plan cache", K(ret));
      } else if (!plan_added && ObSpmCacheCtx::SpmStat::STAT_ADD_BASELINE_PLAN == spm_ctx.spm_stat_) {
        if (spm_ctx.evolution_task_in_two_plan_set_) {
          // now baseline plan and evolving plan may be added to differnet plan set due to different
          // constrain. In this situation, we can't compare two plan. So just keep try next baseline.
          spm_ctx.evolution_task_in_two_plan_set_ = false;
          need_get_baseline = true;
        } else {
          // add baseline plan failed, need evict unaccepted baseline in baseline cache.
          (void) ObSpmController::deny_new_plan_as_baseline(spm_ctx);
        }
      } else if (plan_added && ObSpmCacheCtx::SpmStat::STAT_ADD_BASELINE_PLAN == spm_ctx.spm_stat_) {
        spm_ctx.spm_force_disable_ = true;
        spm_ctx.spm_stat_ = ObSpmCacheCtx::STAT_FIRST_EXECUTE_PLAN;
        spm_ctx.is_retry_for_spm_ = false;
        ret = OB_SQL_RETRY_SPM;
      }
    } else {
      LOG_TRACE("spm need get baseline due to plan hash value not equal");
      need_get_baseline = true;
    }
  } else {
    // handle spm evolution plan or not spm plan
    if (add_plan_to_pc) {
      bool baseline_enable = false;
      bool baseline_exists = true;
      bool baseline_find = false;
      if (DEPENDENCY_OUTLINE == outline_state.outline_version_.object_type_ || is_match_udr) {
        // outline has higher priority than baseline
      } else if (OB_FAIL(ObSpmController::check_baseline_enable(pc_ctx, result.get_physical_plan(), baseline_enable))) {
        LOG_WARN("failed to check need capture baseline", K(ret));
      } else if (!baseline_enable) {
        // do nothing
      } else if (OB_FAIL(ObSpmController::check_baseline_exists(pc_ctx, result.get_physical_plan(), baseline_exists))) {
        LOG_WARN("failed to check baseline exists", K(ret));
      } else if (!baseline_exists) {
        pc_ctx.need_evolution_ = true;
        spm_ctx.spm_stat_ = ObSpmCacheCtx::SpmStat::STAT_ADD_EVOLUTION_PLAN;
      }
      if (OB_SUCC(ret)) {
        if (is_match_udr && OB_FAIL(pc_add_udr_plan(item_guard,
                                                    pc_ctx,
                                                    result,
                                                    outline_state,
                                                    plan_added))) {
          LOG_WARN("fail to add plan to plan cache", K(ret));
        } else if (!is_match_udr && OB_FAIL(pc_add_plan(pc_ctx, result, outline_state, plan_cache, plan_added))) {
          LOG_WARN("fail to add plan to plan cache", K(ret));
        } else if (!plan_added) {
          // plan not add to plan cache, do not check if need evolution.
          if (spm_ctx.check_execute_status_) {
            (void) ObSpmController::deny_new_plan_as_baseline(spm_ctx);
          }
        } else if (baseline_enable && !baseline_exists) {
          need_get_baseline = true;
        }
      }
    }
  }
  if (OB_SUCC(ret) && need_get_baseline) {
    ObSpmController::get_next_baseline_outline(spm_ctx);
    ret = OB_SQL_RETRY_SPM;
    LOG_TRACE("spm get next baeline outline", K(spm_ctx.baseline_guard_.get_cache_obj()), K(ret));
  }
#endif
  //if the error code is ob_timeout, we add more error info msg for dml query.
  if (OB_TIMEOUT == ret &&
      result.get_session().is_user_session() &&
      parse_result.result_tree_ != NULL &&
      parse_result.result_tree_->children_ != NULL &&
      parse_result.result_tree_->num_child_ >= 1 &&
      (parse_result.result_tree_->children_[0]->type_ == T_EXPLAIN ||
       IS_DML_STMT(parse_result.result_tree_->children_[0]->type_) ||
       IS_SHOW_STMT(parse_result.result_tree_->children_[0]->type_))) {
    LOG_USER_ERROR(OB_TIMEOUT, THIS_WORKER.get_timeout_ts() - result.get_session().get_query_start_time());
  }
  return ret;
}

int ObSql::handle_parser(const ObString &sql,
                         ObExecContext &exec_ctx,
                         ObPlanCacheCtx &pc_ctx,
                         ParseResult &parse_result,
                         int get_plan_err,
                         bool &add_plan_to_pc,
                         bool &is_enable_transform_tree)

{
  int ret = OB_SUCCESS;
  FLTSpanGuard(parse);
  int64_t last_mem_usage = pc_ctx.allocator_.total();
  int64_t parser_mem_usage = 0;
  ObPhysicalPlanCtx *pctx = exec_ctx.get_physical_plan_ctx();
  const ObSqlCtx *sql_ctx = exec_ctx.get_sql_ctx();
  if (OB_ISNULL(pc_ctx.sql_ctx_.session_info_) || OB_ISNULL(pctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret), KP(pctx), KP(pc_ctx.sql_ctx_.session_info_));
  } else if (OB_FAIL(parser_and_check(sql, exec_ctx, pc_ctx, parse_result,
                                      get_plan_err, add_plan_to_pc, is_enable_transform_tree))) {
    LOG_WARN("fail to parser normal query",
             "sql", pc_ctx.sql_ctx_.is_sensitive_ ? ObString(OB_MASKED_STR) : sql, K(ret));
  }
  if (OB_SUCC(ret)) {
    if (exec_ctx.has_dynamic_values_table()) {
      if (OB_FAIL(ObValuesTableCompression::resolve_params_for_values_clause(pc_ctx))) {
        LOG_WARN("failed to resolve batch param store for values table", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      pctx->set_original_param_cnt(pctx->get_param_store().count());
    }
  }
  LOG_DEBUG("SQL MEM USAGE", K(parser_mem_usage), K(last_mem_usage));
  return ret;
}

int ObSql::check_batched_multi_stmt_after_parser(ObPlanCacheCtx &pc_ctx,
                                                 ParseResult &parse_result,
                                                 bool add_plan_to_pc,
                                                 bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  ObItemType type = parse_result.result_tree_->children_[0]->type_;
  if (add_plan_to_pc ||
      (ObSQLUtils::is_enable_explain_batched_multi_statement() && T_EXPLAIN == type)) {
    is_valid = true;
    // only update support batched multi-stmt optimization
    if (OB_ISNULL(parse_result.result_tree_) ||
        OB_ISNULL(parse_result.result_tree_->children_)) {
      ret = OB_ERR_UNEXPECTED;;
      LOG_WARN("get unexpected null", K(ret), KP(parse_result.result_tree_));
    } else if (OB_UNLIKELY(parse_result.result_tree_->num_child_ < 1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected child number", K(ret));
    } else if (OB_ISNULL(parse_result.result_tree_->children_[0])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (!(ObSQLUtils::is_support_batch_exec(type) || T_EXPLAIN == type)) {
      is_valid = false;
    } else { /*do nothing*/ }

    if (OB_SUCC(ret) && is_valid && !pc_ctx.not_param_info_.empty()) {
      if (pc_ctx.sql_ctx_.is_do_insert_batch_opt()) {
        if (OB_FAIL(ObPlanCacheValue::check_insert_multi_values_param(pc_ctx, is_valid))) {
          LOG_WARN("failed to check insert multi values not param value", K(ret));
        }
      } else if (pc_ctx.sql_ctx_.multi_stmt_item_.is_batched_multi_stmt()) {
        if (OB_FAIL(ObPlanCacheValue::check_multi_stmt_not_param_value(pc_ctx.multi_stmt_fp_results_,
                                                                       pc_ctx.not_param_info_,
                                                                       is_valid))) {
          LOG_WARN("failed to check multi stmt not param value", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObSql::before_resolve_array_params(ObPlanCacheCtx &pc_ctx,
                                       int64_t query_num,
                                       int64_t param_num,
                                       ParamStore *&ab_params,
                                       ObBitSet<> &neg_param_index,
                                       ObBitSet<> &not_param_index,
                                       ObBitSet<> &must_be_positive_index)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ab_params = static_cast<ParamStore *>(pc_ctx.allocator_.alloc(sizeof(ParamStore))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else if (FALSE_IT(ab_params = new(ab_params)ParamStore(ObWrapperAllocator(pc_ctx.allocator_)))) {
   // do nothing
  } else if (OB_FAIL(ObSQLUtils::create_multi_stmt_param_store(pc_ctx.allocator_,
                                                               query_num,
                                                               param_num,
                                                               *ab_params))) {
    LOG_WARN("failed to create multi_stmt param store", K(query_num), K(param_num),K(ret));
  } else if (OB_FAIL(neg_param_index.add_members2(pc_ctx.neg_param_index_))) {
    LOG_WARN("failed to assign bit sets", K(ret));
  } else if (OB_FAIL(not_param_index.add_members2(pc_ctx.not_param_index_))) {
    LOG_WARN("failed to assign bit sets", K(ret));
  } else if (OB_FAIL(must_be_positive_index.add_members2(pc_ctx.must_be_positive_index_))) {
    LOG_WARN("failed to assign bit sets", K(ret));
  }
  return ret;
}

int ObSql::resolve_ins_multi_row_params(ObPlanCacheCtx &pc_ctx, const ObStmt &stmt, ParamStore *&ab_params)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = NULL;
  ObBitSet<> neg_param_index;
  ObBitSet<> not_param_index;
  ObBitSet<> must_be_positive_index;
  int64_t query_num = pc_ctx.sql_ctx_.get_insert_batch_row_cnt();
  int64_t param_num = 0;
  if (OB_ISNULL(plan_ctx = pc_ctx.exec_ctx_.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (FALSE_IT(param_num = plan_ctx->get_param_store().count())) {
    // do nothing
  } else if (OB_FAIL(before_resolve_array_params(pc_ctx,
                                                 query_num,
                                                 param_num,
                                                 ab_params,
                                                 neg_param_index,
                                                 not_param_index,
                                                 must_be_positive_index))) {
    LOG_WARN("fail to prepare resolve params info", K(ret), K(query_num), K(param_num));
  } else if (OB_FAIL(ObPlanCacheValue::resolve_insert_multi_values_param(pc_ctx,
                                                                         stmt.get_stmt_type(),
                                                                         pc_ctx.param_charset_type_,
                                                                         neg_param_index,
                                                                         not_param_index,
                                                                         must_be_positive_index,
                                                                         param_num,
                                                                         *ab_params))) {
    LOG_WARN("failed to check multi-stmt param type", K(ret));
  }
  return ret;
}

int ObSql::resolve_multi_query_params(ObPlanCacheCtx &pc_ctx, const ObStmt &stmt, ParamStore *&ab_params)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = NULL;
  ObBitSet<> neg_param_index;
  ObBitSet<> not_param_index;
  ObBitSet<> must_be_positive_index;
  int64_t query_num = pc_ctx.sql_ctx_.get_batch_params_count();
  int64_t param_num = 0;
  if (OB_ISNULL(plan_ctx = pc_ctx.exec_ctx_.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (FALSE_IT(param_num = plan_ctx->get_param_store().count())) {
    // do nothing
  } else if (OB_FAIL(before_resolve_array_params(pc_ctx,
                                                 query_num,
                                                 param_num,
                                                 ab_params,
                                                 neg_param_index,
                                                 not_param_index,
                                                 must_be_positive_index))) {
    LOG_WARN("fail to prepare resolve params info", K(ret), K(query_num), K(param_num));
  } else if (OB_FAIL(ObPlanCacheValue::check_multi_stmt_param_type(pc_ctx,
                                                                   stmt.get_stmt_type(),
                                                                   pc_ctx.param_charset_type_,
                                                                   neg_param_index,
                                                                   not_param_index,
                                                                   must_be_positive_index,
                                                                   *ab_params))) {
    LOG_WARN("failed to check multi-stmt param type", K(ret));
  }

  return ret;
}

int ObSql::check_batched_multi_stmt_after_resolver(ObPlanCacheCtx &pc_ctx,
                                                   const ObStmt &stmt,
                                                   bool &is_valid)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = NULL;
  is_valid = true;
  bool has_dblink = false;
  if (OB_ISNULL(plan_ctx = pc_ctx.exec_ctx_.get_physical_plan_ctx())
      || OB_ISNULL(pc_ctx.sql_ctx_.session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (!(stmt.is_support_batch_exec_stmt() || stmt.is_explain_stmt())) {
    is_valid = false;
  } else {
    const ObDelUpdStmt &delupd_stmt = stmt.is_explain_stmt()
        ? static_cast<const ObDelUpdStmt&>(*(static_cast<const ObExplainStmt&>(stmt).get_explain_query_stmt()))
        : static_cast<const ObDelUpdStmt&>(stmt);
    if (delupd_stmt.is_update_stmt() || delupd_stmt.is_delete_stmt()) {
      if (1 != delupd_stmt.get_table_items().count() ||
          !delupd_stmt.get_table_items().at(0)->is_basic_table()) {
        is_valid = false;
      }
    }
    if (delupd_stmt.has_order_by() || delupd_stmt.has_limit() ||
        !delupd_stmt.get_returning_exprs().empty()) {
      is_valid = false;
    }

    if (OB_FAIL(ObDblinkUtils::has_reverse_link_or_any_dblink(&delupd_stmt, has_dblink, true))) {
      LOG_WARN("failed to check dblink in stmt", K(delupd_stmt), K(ret));
    } else if (has_dblink) {
      is_valid = false;
    }

    // make sure type of all the parameters are the same
    if (OB_SUCC(ret) && is_valid) {
      ParamStore *ab_params = NULL;
      ObBitSet<> neg_param_index;
      ObBitSet<> not_param_index;
      ObBitSet<> must_be_positive_index;
      if (pc_ctx.sql_ctx_.is_do_insert_batch_opt()) {
        if (OB_FAIL(resolve_ins_multi_row_params(pc_ctx, stmt, ab_params))) {
          LOG_WARN("fail to resolve multi insert row params", K(ret));
        } else {
          pc_ctx.ab_params_ = ab_params;
          ParamStore &param_store = plan_ctx->get_param_store_for_update();
          for (int64_t i = 0; OB_SUCC(ret) && i < param_store.count(); i++) {
            ObObjParam &obj_param = param_store.at(i);
            obj_param.get_param_flag().is_batch_parameter_ = true;
          }
        }
      } else if (!pc_ctx.sql_ctx_.multi_stmt_item_.is_ab_batch_opt()) {
        if (OB_FAIL(resolve_multi_query_params(pc_ctx, stmt, ab_params))) {
          LOG_WARN("fail to resolve multi query params", K(ret));
        } else {
          pc_ctx.ab_params_ = ab_params;
          ParamStore &param_store = plan_ctx->get_param_store_for_update();
          for (int64_t i = 0; OB_SUCC(ret) && i < param_store.count(); i++) {
            ObObjParam &obj_param = param_store.at(i);
            obj_param.get_param_flag().is_batch_parameter_ = true;
          }
        }
      }
    }
  }
  return ret;
}

int ObSql::replace_const_expr(ObIArray<ObRawExpr*> &raw_exprs,
                              ParamStore &param_store)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < raw_exprs.count(); i++) {
    if (OB_FAIL(replace_const_expr(raw_exprs.at(i),
                                   param_store))) {
      LOG_WARN("failed to replace const expr", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObSql::replace_const_expr(ObRawExpr *raw_expr,
                              ParamStore &param_store)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_ISNULL(raw_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("check stack overflow failed", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret));
  } else if (raw_expr->is_const_raw_expr()) {
    ObConstRawExpr *const_expr = static_cast<ObConstRawExpr*>(raw_expr);
    if (const_expr->get_value().is_unknown()) {
      int pos = const_expr->get_value().get_unknown();
      if (pos >= 0 && pos < param_store.count()) {
        const_expr->set_param(param_store.at(pos));
      } else { /*do nothing*/ }
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < raw_expr->get_param_count(); i++) {
      if (OB_FAIL(replace_const_expr(raw_expr->get_param_expr(i),
                                     param_store))) {
        LOG_WARN("failed to replace const expr", K(ret));
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

void ObSql::generate_ps_sql_id(const ObString &raw_sql,
                               ObSqlCtx &context)
{
  (void)ObSQLUtils::md5(raw_sql, context.sql_id_, (int32_t)sizeof(context.sql_id_));
}

void ObSql::generate_sql_id(ObPlanCacheCtx &pc_ctx,
                           bool add_plan_to_pc,
                           ParseResult &parse_result,
                           ObString &signature_sql,
                           int err_code)
{
  // It has been checked during parser_and_check, there is no need to check again here
  if (OB_SUCCESS == err_code
      && PC_TEXT_MODE == pc_ctx.mode_
      && T_SP_CALL_STMT == parse_result.result_tree_->children_[0]->type_) {
    signature_sql = pc_ctx.fp_result_.pc_key_.name_;
  } else if (add_plan_to_pc == false
            || PC_PS_MODE == pc_ctx.mode_
            || PC_PL_MODE == pc_ctx.mode_
            || OB_SUCCESS != err_code) {
    signature_sql = pc_ctx.raw_sql_;
  } else {
    signature_sql = pc_ctx.sql_ctx_.spm_ctx_.bl_key_.constructed_sql_;
  }
  (void)ObSQLUtils::md5(signature_sql,
                        pc_ctx.sql_ctx_.sql_id_,
                        (int32_t)sizeof(pc_ctx.sql_ctx_.sql_id_));
  pc_ctx.sql_ctx_.spm_ctx_.bl_key_.sql_id_.assign_ptr(pc_ctx.sql_ctx_.sql_id_, strlen(pc_ctx.sql_ctx_.sql_id_));
}


int ObSql::calc_pre_calculable_exprs(const ObDMLStmt &stmt,
                                 const ObIArray<ObHiddenColumnItem> &calculable_exprs,
                                 const bool is_ignore_stmt,
                                 ObExecContext &exec_ctx,
                                 ObPhysicalPlan &phy_plan,
                                 const uint64_t calc_types) /* default PRE_CALC_DEFAULT */
{
  int ret = OB_SUCCESS;
  OB_ASSERT(NULL != exec_ctx.get_physical_plan_ctx() &&
            NULL != exec_ctx.get_my_session() &&
            NULL != exec_ctx.get_stmt_factory() &&
            NULL != exec_ctx.get_stmt_factory()->get_query_ctx());
  ObPhysicalPlanCtx *phy_plan_ctx = exec_ctx.get_physical_plan_ctx();
  ObPreCalcExprFrameInfo *pre_calc_frame = NULL;
  void *frame_buf = NULL;
  bool need_fetch_cur_time = false;
  if (OB_ISNULL(phy_plan_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("phy plan ctx is null", K(ret));
  } else if (OB_ISNULL(frame_buf = phy_plan.get_allocator().alloc(
                                                            sizeof(ObPreCalcExprFrameInfo)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else {
    phy_plan_ctx->set_ignore_stmt(is_ignore_stmt);
    DatumParamStore &datum_param_store = phy_plan_ctx->get_datum_param_store();
    ObStaticEngineExprCG expr_cg(phy_plan.get_allocator(),
                                 exec_ctx.get_my_session(),
                                 exec_ctx.get_sql_ctx()->schema_guard_,
                                 phy_plan_ctx->get_original_param_cnt(),
                                 datum_param_store.count(),
                                 exec_ctx.get_min_cluster_version());
    pre_calc_frame = new(frame_buf)ObPreCalcExprFrameInfo(phy_plan.get_allocator());
    if (OB_FAIL(expr_cg.generate_calculable_exprs(calculable_exprs,
                                                  *pre_calc_frame))) {
      LOG_WARN("failed to generate calculable exprs", K(ret));
    } else {
      phy_plan.set_fetch_cur_time(stmt.get_fetch_cur_time());
      phy_plan.set_stmt_type(stmt.get_stmt_type());
      phy_plan.set_literal_stmt_type(exec_ctx.get_stmt_factory()->get_query_ctx()->get_literal_stmt_type());

      need_fetch_cur_time = phy_plan.get_fetch_cur_time() && !phy_plan_ctx->has_cur_time();
    }
    if (OB_FAIL(ret)) {
      // set current time before do pre calculation
    } else if (need_fetch_cur_time && FALSE_IT(phy_plan_ctx->set_cur_time(
                                  ObTimeUtility::current_time(), *(exec_ctx.get_my_session())))) {
      // do nothing
    } else if (OB_FAIL(ObPlanCacheObject::pre_calculation(is_ignore_stmt,
                                                          *pre_calc_frame, exec_ctx,
                                                          calc_types))) {
      LOG_WARN("failed to pre calculate exprs", K(ret));
    } else if (OB_UNLIKELY(PRE_CALC_DEFAULT == calc_types &&
                   !phy_plan.get_pre_calc_frames().add_last(pre_calc_frame))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to add list element", K(ret));
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObSql::create_expr_constraints(ObQueryCtx &query_ctx, ObExecContext &exec_ctx)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObHiddenColumnItem, 4> pre_calc_exprs;
  ObHiddenColumnItem hidden_column_item;
  int64_t idx = -1;
  if (query_ctx.all_expr_constraints_.empty()) {
    // do nothing
  } else {
    ObIArray<ObExprConstraint> &expr_constraints = query_ctx.all_expr_constraints_;
    ObSEArray<ObHiddenColumnItem, 4> pre_calc_exprs;
    ObHiddenColumnItem hidden_column_item;
    int64_t idx = -1;
    const int64_t dummy_count = -1;
    for (int64_t i = PRE_CALC_RESULT_NULL; OB_SUCC(ret) && i <= PRE_CALC_NOT_PRECISE; ++i) {
      PreCalcExprExpectResult expect_result = static_cast<PreCalcExprExpectResult>(i);
      pre_calc_exprs.reuse();
      for (int64_t j = 0; OB_SUCC(ret) && j < expr_constraints.count(); ++j) {
        if (expr_constraints.at(j).expect_result_ == expect_result) {
          hidden_column_item.expr_ = expr_constraints.at(j).pre_calc_expr_;
          hidden_column_item.hidden_idx_ = ++idx;
          if (OB_ISNULL(hidden_column_item.expr_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpect null", K(ret), K(j));
          } else if (OB_FAIL(hidden_column_item.expr_->extract_info())) {
            LOG_WARN("failed to extract expr info", K(ret));
          } else if (!expr_constraints.at(j).ignore_const_check_ &&
                     OB_UNLIKELY(!ObOptEstUtils::is_calculable_expr(*hidden_column_item.expr_, dummy_count))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpect calculable expr", K(ret), KPC(hidden_column_item.expr_));
          } else if (OB_FAIL(pre_calc_exprs.push_back(hidden_column_item))) {
            LOG_WARN("failed to push back to array", K(ret));
          }
        }
      }
      if (OB_SUCC(ret) && !pre_calc_exprs.empty()) {
        if (OB_FAIL(create_expr_constraint(query_ctx,
                                           exec_ctx,
                                           pre_calc_exprs,
                                           expect_result))) {
          LOG_WARN("failed to create expr constraints for new engine", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObSql::create_expr_constraint(ObQueryCtx &query_ctx,
                                  ObExecContext &exec_ctx,
                                  const ObIArray<ObHiddenColumnItem> &pre_calc_exprs,
                                  const PreCalcExprExpectResult expect_result)
{
  int ret = OB_SUCCESS;
  ObPreCalcExprConstraint *pre_calc_constraint = NULL;
  void *cons_buf = NULL;
  ObPhysicalPlanCtx *plan_ctx = NULL;
  if (OB_ISNULL(exec_ctx.get_sql_ctx()) || OB_ISNULL(plan_ctx = exec_ctx.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null", K(exec_ctx.get_sql_ctx()), K(plan_ctx), K(ret));
  } else if (OB_ISNULL(cons_buf = exec_ctx.get_allocator().alloc(sizeof(ObPreCalcExprConstraint)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else {
    ObStaticEngineExprCG expr_cg(exec_ctx.get_allocator(), exec_ctx.get_my_session(),
                                 exec_ctx.get_sql_ctx()->schema_guard_,
                                 plan_ctx->get_original_param_cnt(),
                                 plan_ctx->get_datum_param_store().count(),
                                 exec_ctx.get_min_cluster_version());
    pre_calc_constraint = new(cons_buf)ObPreCalcExprConstraint(exec_ctx.get_allocator());
    pre_calc_constraint->expect_result_ = expect_result;
    if (OB_FAIL(expr_cg.generate_calculable_exprs(pre_calc_exprs,
                                                  pre_calc_constraint->pre_calc_expr_info_))) {
      LOG_WARN("failed to generate calculable exprs", K(ret));
    } else if (OB_UNLIKELY(!query_ctx.all_pre_calc_constraints_.add_last(pre_calc_constraint))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to push back pre calc constraint", K(ret));
    }
  }
  return ret;
}

// handle execute in text protocol
int ObSql::handle_text_execute(const ObStmt *basic_stmt,
                               ObSqlCtx &sql_ctx,
                               ObResultSet &result)
{
  int ret = OB_SUCCESS;
  const ObExecuteStmt *exec_stmt = static_cast<const ObExecuteStmt*>(basic_stmt);
  sql_ctx.is_text_ps_mode_ = true;
  if (OB_ISNULL(exec_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt is NULL", K(ret), KPC(exec_stmt), KPC(basic_stmt));
  } else {
    ObIAllocator &alloc = result.get_exec_context().get_allocator();
    ObObjParam obj_param;
    ParamStore param_store((ObWrapperAllocator(alloc)));
    const ObRawExpr *raw_expr = NULL;
    const ObIArray<const ObRawExpr*> &raw_expr_params = exec_stmt->get_params();
    if (OB_FAIL(param_store.reserve(raw_expr_params.count()))) {
      LOG_WARN("reserve param store failed", K(ret), K(raw_expr_params));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < raw_expr_params.count(); ++i) {
      if (OB_ISNULL(raw_expr = raw_expr_params.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is NULL", K(ret), K(raw_expr_params));
      } else if (OB_FAIL(ObSQLUtils::calc_const_expr(result.get_exec_context(),
                                                     raw_expr,
                                                     obj_param,
                                                     alloc,
                                                     param_store))) {
        LOG_WARN("calc const expr failed", K(ret), KPC(exec_stmt));
      } else {
        obj_param.set_accuracy(raw_expr->get_accuracy());
        obj_param.set_result_flag(raw_expr->get_result_flag());
        obj_param.set_collation_level(CS_LEVEL_COERCIBLE);
        obj_param.set_param_meta(obj_param.get_meta());
        if (OB_FAIL(param_store.push_back(obj_param))) {
          LOG_WARN("push back into param_store failed", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(handle_ps_execute(exec_stmt->get_prepare_id(),
                                    exec_stmt->get_prepare_type(),
                                    param_store,
                                    sql_ctx,
                                    result,
                                    false/*is_inner_sql*/))) {
        LOG_WARN("ps execute failed", K(ret));
      } else if (OB_FAIL(construct_param_store(param_store, result.get_ps_params()))) {
        LOG_WARN("construct ps params failed", K(ret));
      }
    }
    LOG_DEBUG("handle text execute done", K(ret), KPC(exec_stmt), K(param_store));
  }
  return ret;
}

int ObSql::check_need_reroute(ObPlanCacheCtx &pc_ctx, ObSQLSessionInfo &session, ObPhysicalPlan *plan, bool &need_reroute)
{
  int ret = OB_SUCCESS;
  need_reroute = false;
  ObDASCtx &das_ctx = pc_ctx.exec_ctx_.get_das_ctx();
  bool should_reroute = false;
  if (OB_NOT_NULL(plan)) {
    should_reroute = pc_ctx.sql_ctx_.can_reroute_sql_
      && (OB_PHY_PLAN_REMOTE == plan->get_plan_type()
          || (!das_ctx.is_partition_hit() && !das_ctx.get_table_loc_list().empty()));
    // check inject reroute for test
    if (!should_reroute) {
      const uint32_t sessid = session.get_sessid();
      const int reroute_retry_cnt = OB_E(EventTable::EN_LOCK_CONFLICT_RETRY_THEN_REROUTE, sessid) OB_SUCCESS;
      if (OB_UNLIKELY(reroute_retry_cnt)) {
        int last_query_retry_err = session.get_retry_info().get_last_query_retry_err();
        int64_t retry_cnt = session.get_retry_info().get_retry_cnt();
        should_reroute = last_query_retry_err == OB_TRY_LOCK_ROW_CONFLICT
          && retry_cnt >= -reroute_retry_cnt;
        LOG_INFO("inject force reroute sql",
                 K(last_query_retry_err),
                 K(retry_cnt),
                 K(reroute_retry_cnt),
                 K(should_reroute));
      }
    }
  }
  if (should_reroute) {
    // reroute request,
    // physical table location is already calculated and stored in task_exec_ctx.table_locations_
    const DependenyTableStore &dep_tables = plan->get_dependency_table();
    for (int64_t i = 0;
         should_reroute && i < dep_tables.count();
         i++) {
      const ObSchemaObjVersion &schema_obj = dep_tables.at(i);
      if (TABLE_SCHEMA == schema_obj.get_schema_type()
          && is_virtual_table(schema_obj.object_id_)) {
        should_reroute = false;
      }
    }

    // CHECK for `TXN_FREE_ROUTE`
    if (should_reroute && !session.is_inner() && session.is_in_transaction()) {
      bool ac = true;
      session.get_autocommit(ac);
      const stmt::StmtType stmt_type = plan->get_stmt_type();
      bool fixed_route = true;
      if (ac && !session.get_tx_desc()->is_explicit()) {
        fixed_route = false;
        // for autocommit txn, always allow reroute, because such
        // transaction will not corss multiple node
      } else if (pc_ctx.sql_ctx_.multi_stmt_item_.is_part_of_multi_stmt()) {
        // current is multi-stmt
      } else if (!STMT_SUPPORT_BY_TXN_FREE_ROUTE(stmt_type, false)) {
        // stmt is not DML
      } else if (plan->is_contain_oracle_session_level_temporary_table()
                 || plan->contains_temp_table()
                 || plan->is_contain_oracle_trx_level_temporary_table()) {
        // access temp table
      } else {
        // check passed: special query which can not be reroute
        //
        // check txn free route is disabled, if so, when on txn start
        // node, can not reroute
        if (OB_FAIL(session.calc_txn_free_route())) {
          LOG_WARN("cal txn free route failed", K(ret), K(session));
        } else if (session.can_txn_free_route()) {
          fixed_route = false;
        } else if (session.is_txn_free_route_temp()) {
          fixed_route = false;
        } else {
          // fixed route if txn started on this node
        }
      }
      if (fixed_route) {
        should_reroute = false;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(pc_ctx.sql_ctx_.schema_guard_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid null schema guard", K(ret));
    } else if (should_reroute) {
      if (DAS_CTX(pc_ctx.exec_ctx_).get_table_loc_list().empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null phy table location", K(ret));
      } else {
        const ObTableSchema *table_schema = NULL;
        ObDASTableLoc *first_table_loc = DAS_CTX(pc_ctx.exec_ctx_).get_table_loc_list().get_first();
        ObDASTabletLoc *first_tablet_loc = first_table_loc->get_first_tablet_loc();
        ObLSReplicaLocation ls_replica_loc;
        ObDASLocationRouter &loc_router = DAS_CTX(pc_ctx.exec_ctx_).get_location_router();
        if (OB_FAIL(pc_ctx.sql_ctx_.schema_guard_->get_table_schema(
            MTL_ID(),
            first_table_loc->loc_meta_->ref_table_id_, table_schema))) {
          LOG_WARN("failed to get table schema", K(ret));
        } else if (OB_ISNULL(table_schema)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid null table schema", K(ret));
        } else if (table_schema->is_storage_local_index_table()) {
          // Local index table has the same location as the primary table.
          // But the schema of local index table is incompleted, causing error when it is rerouted.
          // Therefore, returns the primary table to proxy for rerouting.
          const uint64_t data_table_id = table_schema->get_data_table_id();
          const ObTableSchema *data_table_schema = NULL;
          if (OB_FAIL(pc_ctx.sql_ctx_.schema_guard_->get_table_schema(
              MTL_ID(),
              data_table_id,
              data_table_schema))) {
            LOG_WARN("failed to get table schema", KR(ret), "tenant_id", MTL_ID(), K(data_table_id));
          } else if (OB_ISNULL(data_table_schema)) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid null table schema", KR(ret), "tenant_id", MTL_ID(), K(data_table_id));
          } else {
            table_schema = data_table_schema;
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_ISNULL(pc_ctx.sql_ctx_.get_or_create_reroute_info())) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("get reroute info failed", K(ret));
        } else if (OB_FAIL(loc_router.get_full_ls_replica_loc(table_schema->get_tenant_id(),
                                                              *first_tablet_loc,
                                                              ls_replica_loc))) {
          LOG_WARN("get full ls replica location failed", K(ret), KPC(first_tablet_loc));
        } else {
          bool is_weak = false;
          if (plan->is_select_plan()) {
            if (pc_ctx.sql_ctx_.is_protocol_weak_read_) {
              is_weak = true;
            } else if (OB_UNLIKELY(INVALID_CONSISTENCY != plan->get_phy_plan_hint().read_consistency_)) {
              is_weak = (WEAK == plan->get_phy_plan_hint().read_consistency_);
            } else {
              is_weak = (WEAK == pc_ctx.sql_ctx_.session_info_->get_consistency_level());
            }
          }
          // if weak delay read, no need to return reroute_info.
          if (pc_ctx.sql_ctx_.session_info_->
              get_proxy_cap_flags().is_weak_stale_feedback() && is_weak) {
            pc_ctx.sql_ctx_.reset_reroute_info();
          } else {
            pc_ctx.sql_ctx_.get_reroute_info()->server_ = ls_replica_loc.get_server();
            pc_ctx.sql_ctx_.get_reroute_info()->server_.set_port(static_cast<int32_t>(ls_replica_loc.get_sql_port()));
            pc_ctx.sql_ctx_.get_reroute_info()->role_ = ls_replica_loc.get_role();
            pc_ctx.sql_ctx_.get_reroute_info()->replica_type_ = ls_replica_loc.get_replica_type();
            pc_ctx.sql_ctx_.get_reroute_info()->set_tbl_name(table_schema->get_table_name());
            pc_ctx.sql_ctx_.get_reroute_info()->tbl_schema_version_ = table_schema->get_schema_version();
          }
          LOG_DEBUG("reroute sql", KPC(pc_ctx.sql_ctx_.get_reroute_info()));
          need_reroute = true;
        }
      }
    }
  }
  return ret;
}

int ObSql::get_reconstructed_batch_stmt(ObPlanCacheCtx &pc_ctx, ObString& stmt_sql)
{
  int ret = OB_SUCCESS;
  if (pc_ctx.sql_ctx_.is_do_insert_batch_opt()) {
    // Restore the original SQL according to the first row of parameters
    if (OB_FAIL(ObPlanCache::restore_param_to_truncated_sql(pc_ctx))) {
      LOG_WARN("fail to do construct sql",
          K(ret), K(pc_ctx.fp_result_.pc_key_.name_), K(pc_ctx.insert_batch_opt_info_.new_reconstruct_sql_));
      // if rebuild origin sql fail, this sql would rollback
      ret = OB_BATCHED_MULTI_STMT_ROLLBACK;
      LOG_WARN("change error ret to -5787", K(ret), K(pc_ctx.insert_batch_opt_info_), K(pc_ctx.fp_result_.pc_key_.name_));
    } else {
      stmt_sql = pc_ctx.insert_batch_opt_info_.new_reconstruct_sql_;
      LOG_TRACE("print new_reconstruct_sql",
          K(pc_ctx.fp_result_.pc_key_.name_), K(pc_ctx.insert_batch_opt_info_.new_reconstruct_sql_));
    }
  } else if (pc_ctx.sql_ctx_.handle_batched_multi_stmt()) {
    if (OB_ISNULL(pc_ctx.sql_ctx_.multi_stmt_item_.get_queries())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(pc_ctx.sql_ctx_.multi_stmt_item_));
    } else if (OB_UNLIKELY(pc_ctx.sql_ctx_.multi_stmt_item_.get_queries()->empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected array count", K(ret));
    } else {
      stmt_sql = pc_ctx.sql_ctx_.multi_stmt_item_.get_queries()->at(0);
    }
  } else if (pc_ctx.exec_ctx_.has_dynamic_values_table()) {
    stmt_sql = pc_ctx.new_raw_sql_;
  }
  return ret;
}

// OB_NOINLINE int ObSql::regenerate_physical_plan_with_baseline(const ObString& trimed_stmt,
//                                                               const ObPlanBaselineItem& baseline,
//                                                               ObSqlCtx& sql_ctx,
//                                                               ObResultSet& result,
//                                                               ObPlanCacheCtx& pc_ctx,
//                                                               const int get_plan_err,
//                                                               bool is_psmode)
// {
//   int ret = OB_SUCCESS;
//   ObString outlined_stmt = trimed_stmt;
//   bool add_plan_to_pc = false;
//   ObSQLSessionInfo &session = result.get_session();
//   // record whether needs to do parameterization at this time,
//   // if exact mode is on, not do parameterizaiton
//   bool is_enable_transform_tree = !session.get_enable_exact_mode();
//   //重新解析前将这两个标记reset掉，避免前面查plan cache的操作导致这两个参数在重新生成plan后会出现不幂等的问题
//   pc_ctx.not_param_index_.reset();
//   pc_ctx.neg_param_index_.reset();
//   LOG_DEBUG("gen plan info", K(pc_ctx.bl_key_), K(get_plan_err));

//   // note
//   // sql_id 不需要重新生成了


//   // for batched multi stmt, we only parse and optimize the first statement
//   if (sql_ctx.multi_stmt_item_.is_batched_multi_stmt() &&
//       OB_FAIL(get_first_batched_multi_stmt(sql_ctx.multi_stmt_item_, outlined_stmt))) {
//     LOG_WARN("failed to get first batched stmt item", K(ret));
//   } else if (OB_FAIL(ObSQLUtils::construct_outline_sql(pc_ctx.allocator_,
//                                                        session,
//                                                        baseline.outline_data_,
//                                                        outlined_stmt,
//                                                        true, //is_need_filter_hint
//                                                        outlined_stmt))) {
//     LOG_WARN("fail to construct outline sql", K(ret), K(baseline.outline_data_), K(trimed_stmt));
//   } else {
//     pc_ctx.outlined_sql_len_ = outlined_stmt.length();
//     LOG_DEBUG("contain baseline stmt", K(outlined_stmt));
//   }

//   if (OB_SUCC(ret)) {
//     ParseResult parse_result;
//     if (OB_FAIL(handle_parser(outlined_stmt,
//                               result.get_exec_context(),
//                               pc_ctx,
//                               parse_result,
//                               get_plan_err,
//                               add_plan_to_pc,
//                               is_enable_transform_tree))) {
//       LOG_WARN("fail to parser and check", K(ret));
//     } else if (OB_FAIL(generate_physical_plan(parse_result,
//                                               &pc_ctx,
//                                               sql_ctx,
//                                               result,
//                                               pc_ctx.is_begin_commit_stmt(),
//                                               is_psmode))) {
//       if (OB_ERR_PROXY_REROUTE == ret) {
//         LOG_DEBUG("Failed to generate plan", K(ret));
//       } else {
//         LOG_WARN("Failed to generate plan", K(ret), K(result.get_exec_context().need_disconnect()));
//       }
//     } else if (!add_plan_to_pc) { // no need to check need_add_plan() again
//       // 这个outline生成的计划不能加入到pc, 需要将状态设置成false
//     }
//   }
//   return ret;
// }

// If handel failed, rollback implicit started txn by current stmt
// if autocommit is on.
// this is because the txn may be started during generate plan for some
// strange Query.
// We have found one situation:
// udf contains DML stmt and with deterministic property in MySQL mode
void ObSql::rollback_implicit_trans_when_fail(ObResultSet &result, int &ret)
{
  bool ac = false;
  result.get_session().get_autocommit(ac);
  transaction::ObTxDesc *tx = result.get_session().get_tx_desc();
  if (ac && tx && tx->get_tx_id().is_valid() && !tx->is_explicit()) {
    const transaction::ObTransID txid = tx->get_tx_id();
    int tmp_ret = OB_SUCCESS;
    bool need_disconnect = false;
    if (OB_UNLIKELY(result.get_session().is_txn_free_route_temp())) {
      tmp_ret = OB_ERR_UNEXPECTED;
      LOG_WARN("implicit trans found on trans free route temp node", K(tmp_ret), K(result.get_session()), K(txid));
      tx->dump_and_print_trace();
      result.get_exec_context().set_need_disconnect(true);
      ret = tmp_ret;
    } else if (OB_TMP_FAIL(ObSqlTransControl::rollback_trans(&result.get_session(), need_disconnect))) {
      LOG_WARN("rollback transaction fail, will disconnect", K(tmp_ret), K(result.get_session()), K(txid));
      result.get_exec_context().set_need_disconnect(true);
      ret = tmp_ret;
    } else {
      LOG_INFO("rollback transaction started during get-plan success", K(ret), K(txid));
    }
  }
}

int ObSql::check_need_switch_thread(ObSqlCtx &ctx, const ObStmt *stmt, bool &need_switch)
{
  int ret = OB_SUCCESS;
  need_switch = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx.session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(stmt), K(ctx.session_info_));
  } else {
    bool is_multi_stmt = ctx.multi_stmt_item_.is_part_of_multi_stmt();
    bool in_pl = NULL != ctx.session_info_->get_pl_context();
    if (is_multi_stmt || in_pl) {
      // do nothing
    } else if (OB_FAIL(stmt->check_is_simple_lock_stmt(need_switch))) {
      LOG_WARN("failed to check stmt", K(ret), KPC(stmt));
    }
  }
  return ret;
}

}//end of namespace sql
}//end of namespace oceanbase
