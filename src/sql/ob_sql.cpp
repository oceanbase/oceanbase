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
#include "share/stat/ob_stat_manager.h"
#include "share/ob_truncated_string.h"
#include "share/partition_table/ob_partition_location.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/ob_autoincrement_service.h"
#include "share/ob_rs_mgr.h"
#include "share/config/ob_server_config.h"
#include "common/sql_mode/ob_sql_mode_utils.h"
#include "sql/ob_sql_context.h"
#include "sql/ob_result_set.h"
#include "sql/optimizer/ob_log_plan_factory.h"
#include "sql/plan_cache/ob_plan_cache.h"
#include "sql/plan_cache/ob_pcv_set.h"
#include "sql/engine/join/ob_nested_loop_join.h"
#include "sql/engine/table/ob_virtual_table_ctx.h"
#include "sql/engine/basic/ob_values.h"
#include "sql/ob_sql_init.h"
#include "sql/ob_sql_utils.h"
#include "sql/ob_sql_partition_location_cache.h"
#include "sql/optimizer/ob_log_plan.h"
#include "sql/optimizer/ob_optimizer.h"
#include "sql/optimizer/ob_optimizer_context.h"
#include "sql/optimizer/ob_optimizer_partition_location_cache.h"
#include "sql/parser/ob_parser.h"
#include "sql/parser/parse_malloc.h"
#include "sql/parser/parse_node.h"
#include "sql/parser/parse_define.h"
#include "sql/resolver/ob_cmd.h"
#include "sql/resolver/ob_resolver.h"
#include "sql/resolver/ob_schema_checker.h"
#include "sql/resolver/cmd/ob_variable_set_stmt.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/privilege_check/ob_privilege_check.h"
#include "share/system_variable/ob_system_variable_alias.h"
#include "sql/rewrite/ob_transformer_impl.h"
#include "sql/rewrite/ob_transform_simplify.h"
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
#include "sql/resolver/expr/ob_raw_expr_printer.h"
#include "sql/rewrite/ob_constraint_process.h"
#include "sql/engine/px/ob_px_admission.h"
#include "sql/code_generator/ob_code_generator.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "sql/executor/ob_executor_rpc_impl.h"
#include "sql/executor/ob_remote_executor_processor.h"
#include "common/ob_smart_call.h"

namespace oceanbase {
using namespace common;
using namespace rpc::frame;
using namespace obrpc;
using namespace share;
using namespace share::schema;

namespace sql {

const int64_t ObSql::max_error_length = 80;
const int64_t ObSql::SQL_MEM_SIZE_LIMIT = 1024 * 1024 * 64;

int ObSql::init(common::ObStatManager* stat_mgr, common::ObOptStatManager* opt_stat_mgr, ObReqTransport* transport,
    storage::ObPartitionService* partition_service, common::ObIDataAccessService* vt_partition_service,
    share::ObIPartitionLocationCache* partition_location_cache, common::ObAddr& addr, share::ObRsMgr& rs_mgr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stat_mgr) || OB_ISNULL(transport) || OB_ISNULL(partition_service) || OB_ISNULL(vt_partition_service) ||
      OB_ISNULL(partition_location_cache)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args",
        K(ret),
        KP(stat_mgr),
        KP(transport),
        KP(partition_service),
        KP(vt_partition_service),
        KP(partition_location_cache));
  } else {
    if (OB_FAIL(plan_cache_manager_.init(partition_location_cache, addr))) {
      LOG_WARN("Failed to init plan cache manager", K(ret));
    } else {
      stat_mgr_ = stat_mgr;
      opt_stat_mgr_ = opt_stat_mgr;
      transport_ = transport;
      partition_service_ = partition_service;
      vt_partition_service_ = vt_partition_service;
      self_addr_ = addr;
      rs_mgr_ = &rs_mgr;
      inited_ = true;
    }
  }
  return ret;
}

void ObSql::destroy()
{
  if (inited_) {
    plan_cache_manager_.destroy();
    inited_ = false;
  }
}

void ObSql::stat()
{
  sql::print_sql_stat();
}

int ObSql::stmt_prepare(
    const common::ObString& stmt, ObSqlCtx& context, ObResultSet& result, bool is_inner_sql /*true*/)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sanity_check(context))) {
    LOG_WARN("Failed to do sanity check", K(ret));
  } else if (OB_FAIL(handle_ps_prepare(stmt, context, result, is_inner_sql))) {
    LOG_WARN("failed to handle ps query", K(stmt), K(ret));
  }
  if (OB_FAIL(ret) && OB_SUCCESS == result.get_errcode()) {
    result.set_errcode(ret);
  }
  return ret;
}

int ObSql::stmt_query(const common::ObString& stmt, ObSqlCtx& context, ObResultSet& result)
{
  int ret = OB_SUCCESS;
  ObTruncatedString trunc_stmt(stmt);
#if !defined(NDEBUG)
  LOG_INFO("Begin to handle text statement",
      K(trunc_stmt),
      "sess_id",
      result.get_session().get_sessid(),
      "execution_id",
      result.get_session().get_current_execution_id());
#endif
  NG_TRACE_EXT(parse_begin, OB_ID(stmt), trunc_stmt.string(), OB_ID(stmt_len), stmt.length());
  // 1 check inited
  if (OB_FAIL(sanity_check(context))) {
    LOG_WARN("Failed to do sanity check", K(ret));
  } else if (OB_FAIL(handle_text_query(stmt, context, result))) {
    if (OB_EAGAIN != ret && OB_ERR_PROXY_REROUTE != ret) {
      LOG_WARN("fail to handle text query", K(stmt), K(ret));
    }
  } else {
    result.get_session().set_exec_min_cluster_version();
  }
  // LOG_DEBUG("result errno", N_ERR_CODE, result.get_errcode(), K(ret));
  if (OB_SUCCESS != ret && OB_SUCCESS == result.get_errcode()) {
    result.set_errcode(ret);
  }
  return ret;
}

int ObSql::stmt_execute(const ObPsStmtId stmt_id, const stmt::StmtType stmt_type, const ParamStore& params,
    ObSqlCtx& context, ObResultSet& result, bool is_inner_sql)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sanity_check(context))) {
    LOG_WARN("failed to do sanity check", K(ret));
  } else if (OB_FAIL(handle_ps_execute(stmt_id, stmt_type, params, context, result, is_inner_sql))) {
    if (OB_ERR_PROXY_REROUTE != ret) {
      LOG_WARN("failed to handle ps execute", K(stmt_id), K(ret));
    }
  } else {
    result.get_session().set_exec_min_cluster_version();
  }
  if (OB_FAIL(ret) && OB_SUCCESS == result.get_errcode()) {
    result.set_errcode(ret);
  }
  return ret;
}

int ObSql::stmt_list_field(
    const common::ObString& table_name, const common::ObString& wild_str, ObSqlCtx& context, ObResultSet& result)
{
  UNUSED(table_name);
  UNUSED(wild_str);
  UNUSED(context);
  UNUSED(result);
  return OB_NOT_IMPLEMENT;
}

int ObSql::fill_result_set(ObResultSet& result_set, ObSqlCtx* context, const bool is_ps_mode, ObStmt& basic_stmt)
{
  int ret = OB_SUCCESS;

  ObStmt* stmt = &basic_stmt;
  if (OB_UNLIKELY(NULL == context) || OB_UNLIKELY(NULL == context->session_info_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(context), "session", (context != NULL) ? context->session_info_ : NULL);
  } else {
    result_set.set_affected_rows(0);
    result_set.set_warning_count(0);
    result_set.set_message("");
    ObString type_name = ObString::make_string("varchar");
    number::ObNumber number;
    number.set_zero();
    ObSelectStmt* select_stmt = NULL;
    ObDelUpdStmt* del_upd_stmt = NULL;
    ObField field;
    common::ObIAllocator& alloc = result_set.get_mem_pool();
    ObCollationType collation_type = context->session_info_->get_local_collation_connection();
    switch (stmt->get_stmt_type()) {
      case stmt::T_SELECT: {
        select_stmt = static_cast<ObSelectStmt*>(stmt);
        if (select_stmt->has_select_into()) {  //  for select into, no rows return
          break;
        }
        int64_t size = select_stmt->get_select_item_size();
        if (OB_FAIL(result_set.reserve_field_columns(size))) {
          LOG_WARN("reserve field columns failed", K(ret), K(size));
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
          const SelectItem& select_item = select_stmt->get_select_item(i);
          LOG_DEBUG("select item info", K(select_item));
          ObRawExpr* expr = select_item.expr_;
          if (OB_UNLIKELY(NULL == expr)) {
            ret = OB_ERR_ILLEGAL_ID;
            LOG_WARN("fail to get expr", K(ret), K(i), K(size));
          } else {
            if (ob_is_string_or_lob_type(expr->get_data_type()) && CS_TYPE_BINARY != expr->get_collation_type()) {
              field.charsetnr_ = static_cast<uint16_t>(collation_type);
            } else {
              field.charsetnr_ = static_cast<uint16_t>(expr->get_collation_type());
            }
          }

          if (OB_SUCC(ret) && expr->get_result_type().is_ext()) {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("not supported complex type in select item", K(ret), K(expr->get_result_type().is_ext()));
          }

          if (OB_SUCC(ret)) {
            // Setup field Type and Accuracy
            field.type_.set_type(expr->get_data_type());
            field.accuracy_ = expr->get_accuracy();
            field.flags_ = static_cast<uint16_t>(expr->get_result_flag());
            // Setup Collation and Collation levl
            if (ob_is_string_or_lob_type(static_cast<ObObjType>(expr->get_data_type())) ||
                ob_is_raw(static_cast<ObObjType>(expr->get_data_type())) ||
                ob_is_enum_or_set_type(static_cast<ObObjType>(expr->get_data_type()))) {
              field.type_.set_collation_type(expr->get_collation_type());
              field.type_.set_collation_level(expr->get_collation_level());
            }
            // Setup Scale
            if (ObVarcharType == field.type_.get_type()) {
              field.type_.set_varchar(type_name);
            } else if (ObNumberType == field.type_.get_type()) {
              field.type_.set_number(number);
            }
            if (!expr->get_result_type().is_ext() && OB_FAIL(expr->get_length_for_meta_in_bytes(field.length_))) {
              LOG_WARN("get length failed", K(ret));
            } else {
              // do nothing
            }
          }

          // examples of alias name and expr name rules for SELECT ITEM:
          // SELECT field1+field2 AS f1, field1+3, "thanks", field2 AS f2, field3, "hello" as f4, field1+4 as f5 FROM t1
          // "is_alias":true,  "alias_name":"f1", "expr_name":"f1"
          // "is_alias":false, "alias_name":"", "expr_name":"field1+3"
          // "is_alias":false, "alias_name":"", "expr_name":", "thanks"",
          // "is_alias":true,  "alias_name":"f2", "expr_name":"f2",  "column_name":"field2",
          // "is_alias":true,  "alias_name":"field3", "expr_name":"field3",  "column_name":"field3",
          // "is_alias":true,  "alias_name":"f4", "expr_name":"f4"
          // "is_alias":true,  "alias_name":"f5", "expr_name":"f5"
          //
          if (OB_SUCC(ret)) {
            if (OB_FAIL(ObSQLUtils::copy_and_convert_string_charset(
                    alloc, select_item.alias_name_, field.cname_, CS_TYPE_UTF8MB4_BIN, collation_type))) {
              LOG_WARN("fail to alloc string", K(select_item.alias_name_), K(ret));
            } else {
              field.is_hidden_rowid_ = select_item.is_hidden_rowid_;
              LOG_TRACE("is_hidden_rowid", K(select_item));
            }
          }
          if (OB_SUCC(ret)) {
            if (select_stmt->get_set_op() != ObSelectStmt::NONE) {
              if (OB_FAIL(ob_write_string(alloc, select_item.alias_name_, field.org_cname_))) {
                LOG_WARN("fail to alloc string", K(select_item.alias_name_), K(ret));
              }
            } else if (expr->is_column_ref_expr()) {
              ObColumnRefRawExpr* column_expr = static_cast<ObColumnRefRawExpr*>(expr);
              uint64_t table_id = column_expr->get_table_id();
              uint64_t column_id = column_expr->get_column_id();
              if (table_id != OB_INVALID_ID) {
                ColumnItem* column_item = select_stmt->get_column_item_by_id(table_id, column_id);
                const TableItem* table_item = select_stmt->get_table_item_by_id(table_id);
                if (OB_UNLIKELY(NULL == column_item)) {
                  ret = OB_ERR_ILLEGAL_ID;
                  LOG_WARN("fail to get column item by id.", K(ret), K(table_id), K(column_id));
                } else if (OB_FAIL(ob_write_string(alloc, column_item->column_name_, field.org_cname_))) {
                  LOG_WARN("fail to alloc", K(ret), K(column_item->column_name_));
                } else if (OB_UNLIKELY(NULL == table_item)) {
                  ret = OB_ERR_ILLEGAL_ID;
                  LOG_WARN("fail to get table item by id.", K(ret), K(table_id));
                } else if (OB_FAIL(ob_write_string(alloc, table_item->database_name_, field.dname_))) {
                  LOG_WARN("fail to alloc string", K(ret), K(table_item->database_name_));
                } else if (table_item->alias_name_.length() > 0) {
                  if (OB_FAIL(ob_write_string(alloc, table_item->alias_name_, field.tname_))) {
                    LOG_WARN("fail to alloc string", K(ret), K(table_item->alias_name_));
                  }
                } else {
                  if (OB_FAIL(ob_write_string(alloc, table_item->table_name_, field.tname_))) {
                    LOG_WARN("fail to alloc string", K(ret), K(table_item->table_name_));
                  }
                }
                if (OB_FAIL(ob_write_string(alloc, table_item->table_name_, field.org_tname_))) {
                  LOG_WARN("fail to alloc string", K(ret), K(table_item->table_name_));
                }
              }
            } else if (OB_FAIL(ob_write_string(alloc, select_item.alias_name_, field.org_cname_))) {
              LOG_WARN("fail to alloc string", K(ret), K(select_item.alias_name_));
            }
          }
          if (OB_SUCC(ret) && !is_ps_mode) {
            void* buf = NULL;
            if (OB_ISNULL(buf = alloc.alloc(sizeof(ObParamedSelectItemCtx)))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("failed to allocate memory", K(ret));
            } else {
              field.paramed_ctx_ = new (buf) ObParamedSelectItemCtx();
              if (OB_FAIL(
                      ob_write_string(alloc, select_item.paramed_alias_name_, field.paramed_ctx_->paramed_cname_))) {
                LOG_WARN("failed to copy paramed cname", K(ret));
              } else if (OB_FAIL(field.paramed_ctx_->param_str_offsets_.assign(select_item.questions_pos_))) {
                LOG_WARN("failed to copy param_str_offsets_", K(ret));
              } else if (OB_FAIL(field.paramed_ctx_->param_idxs_.assign(select_item.params_idx_))) {
                LOG_WARN("failed to copy param idxs", K(ret));
              } else {
                field.paramed_ctx_->neg_param_idxs_ = select_item.neg_param_idx_;
                field.paramed_ctx_->esc_str_flag_ = select_item.esc_str_flag_;
                field.paramed_ctx_->need_check_dup_name_ = select_item.need_check_dup_name_;
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
        break;
      }
      case stmt::T_INSERT:
      case stmt::T_REPLACE:
      case stmt::T_UPDATE:
      case stmt::T_DELETE: {
        del_upd_stmt = static_cast<ObDelUpdStmt*>(stmt);
        if (!del_upd_stmt->is_returning()) {
          break;
        }
        const common::ObIArray<ObRawExpr*>* returning_exprs = &(del_upd_stmt->get_returning_exprs());
        const common::ObIArray<ObString>& returning_strs = del_upd_stmt->get_returning_strs();
        int64_t size = returning_exprs->count();
        field.charsetnr_ = CS_TYPE_UTF8MB4_GENERAL_CI;
        if (OB_FAIL(result_set.reserve_field_columns(size))) {
          LOG_WARN("reserve field columns failed", K(ret), K(size));
        }

        for (int64_t i = 0; OB_SUCC(ret) && i < size; i++) {
          ObRawExpr* expr = returning_exprs->at(i);
          if (OB_UNLIKELY(OB_ISNULL(expr))) {
            ret = OB_ERR_ILLEGAL_ID;
            LOG_WARN("fail to get expr", K(ret), K(i), K(size));
          }
          if (OB_SUCC(ret)) {
            ObCollationType charsetnr;
            if (OB_FAIL(ObCharset::get_default_collation(expr->get_collation_type(), charsetnr))) {
              LOG_WARN("fail to get table item charset collation", K(expr->get_collation_type()), K(i), K(ret));
            } else {
              field.charsetnr_ = static_cast<uint16_t>(charsetnr);
            }
          }
          if (OB_SUCC(ret)) {
            expr->deduce_type(context->session_info_);
            field.type_.set_type(expr->get_data_type());
            field.accuracy_ = expr->get_accuracy();
            field.flags_ = static_cast<uint16_t>(expr->get_result_flag());
            // Setup Collation and Collation levl
            if (ob_is_string_or_lob_type(static_cast<ObObjType>(expr->get_data_type())) ||
                ob_is_raw(static_cast<ObObjType>(expr->get_data_type())) ||
                ob_is_enum_or_set_type(static_cast<ObObjType>(expr->get_data_type()))) {
              field.type_.set_collation_type(expr->get_collation_type());
              field.type_.set_collation_level(expr->get_collation_level());
            }
            if (ObVarcharType == field.type_.get_type()) {
              field.type_.set_varchar(type_name);
            } else if (ObNumberType == field.type_.get_type()) {
              field.type_.set_number(number);
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
      default:
        break;
    }
    const int64_t question_marks_count = stmt->get_prepare_param_count();
    if (OB_SUCC(ret) && is_ps_mode && question_marks_count > 0) {  // param column is only needed in ps mode
      if (OB_FAIL(result_set.reserve_param_columns(question_marks_count))) {
        LOG_WARN("reserve param columns failed", K(ret), K(question_marks_count));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < question_marks_count; ++i) {
        ObField param_field;
        param_field.type_.set_type(ObIntType);  // @bug
        param_field.cname_ = ObString::make_string("?");
        OZ(result_set.add_param_column(param_field), param_field, i, question_marks_count);
      }
    }
  }
  return ret;
}

int ObSql::fill_result_set(const ObPsStmtId stmt_id, const ObPsStmtInfo& stmt_info, ObResultSet& result)
{
  int ret = OB_SUCCESS;
  result.set_statement_id(stmt_id);
  result.set_stmt_type(stmt_info.get_stmt_type());
  const ObPsSqlMeta& sql_meta = stmt_info.get_ps_sql_meta();
  result.set_p_param_fileds(const_cast<common::ParamsFieldIArray*>(&sql_meta.get_param_fields()));
  result.set_p_column_fileds(const_cast<common::ParamsFieldIArray*>(&sql_meta.get_column_fields()));
  // ObPsSqlMeta::const_column_iterator column_iter = sql_meta.column_begin();
  // result.reserve_field_columns(sql_meta.get_column_size());
  // for (; OB_SUCC(ret) && column_iter != sql_meta.column_end(); ++column_iter) {
  // if (OB_ISNULL(column_iter) || OB_ISNULL(*column_iter)) {
  // ret = OB_ERR_UNEXPECTED;
  // LOG_WARN("column iter is null", K(ret), K(column_iter));
  //} else if (OB_FAIL(result.add_field_column(**column_iter))) {
  // LOG_WARN("add column field failed", K(ret));
  //}
  //}

  // ObPsSqlMeta::const_param_iterator param_iter = sql_meta.param_begin();
  // for (; OB_SUCC(ret) && param_iter != sql_meta.param_end(); ++param_iter) {
  // if (OB_ISNULL(param_iter) || OB_ISNULL(param_iter)) {
  // ret = OB_ERR_UNEXPECTED;
  // LOG_WARN("param iter is null", K(ret), K(param_iter));
  //} else if (OB_FAIL(result.add_param_column(**param_iter))) {
  // LOG_WARN("add param field faield", K(ret));
  //}
  //}
  return ret;
}

int ObSql::do_add_ps_cache(const ObString& sql, int64_t param_cnt, ObSchemaGetterGuard& schema_guard,
    stmt::StmtType stmt_type, ObResultSet& result, bool is_inner_sql)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo& session = result.get_session();
  ObPsCache* ps_cache = session.get_ps_cache();
  uint64_t db_id = OB_INVALID_ID;
  (void)session.get_database_id(db_id);
  if (OB_ISNULL(ps_cache)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ps plan cache should not be null", K(ret));
  } else {
    ObPsStmtItem* ps_stmt_item = NULL;
    ObPsStmtInfo* ref_stmt_info = NULL;
    bool duplicate_prepare = false;
    // add stmt item
    if (OB_FAIL(ps_cache->get_or_add_stmt_item(db_id, sql, ps_stmt_item))) {
      LOG_WARN("get or create stmt item faield", K(ret), K(db_id), K(sql));
    } else if (OB_FAIL(ps_cache->get_or_add_stmt_info(
                   result, param_cnt, schema_guard, stmt_type, ps_stmt_item, ref_stmt_info))) {
      LOG_WARN("get or create stmt info failed", K(ret), K(ps_stmt_item), K(db_id), K(sql));
    } else if (OB_ISNULL(ps_stmt_item) || OB_ISNULL(ref_stmt_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("stmt_item or stmt_info is NULL", K(ret), KP(ps_stmt_item), KP(ref_stmt_info));
    }
    // add session info
    if (OB_SUCC(ret)) {
      ObPsStmtId inner_stmt_id = ps_stmt_item->get_ps_stmt_id();
      ObPsStmtId client_stmt_id = OB_INVALID_ID;
      if (OB_FAIL(
              session.prepare_ps_stmt(inner_stmt_id, ref_stmt_info, client_stmt_id, duplicate_prepare, is_inner_sql))) {
        LOG_WARN("prepare_ps_stmt failed", K(ret), K(inner_stmt_id), K(client_stmt_id));
      } else {
        result.set_statement_id(client_stmt_id);
        result.set_stmt_type(stmt_type);
        LOG_TRACE("add ps session info",
            K(ret),
            K(*ref_stmt_info),
            K(client_stmt_id),
            K(*ps_stmt_item),
            K(session.get_sessid()));
      }
    }
    if (OB_FAIL(ret) || duplicate_prepare) {  // dec ref count
      if (NULL != ps_stmt_item) {
        if (NULL != ref_stmt_info) {
          ObPsStmtId inner_stmt_id = ps_stmt_item->get_ps_stmt_id();
          ps_cache->deref_stmt_info(inner_stmt_id);
        }
        ps_stmt_item->dec_ref_count_check_erase();
      }
    }
  }
  return ret;
}

int ObSql::do_real_prepare(const ObString& sql, ObSqlCtx& context, ObResultSet& result, bool is_inner_sql)
{
  int ret = OB_SUCCESS;
  ParseResult parse_result;
  ObStmt* basic_stmt = NULL;
  stmt::StmtType stmt_type = stmt::T_NONE;
  int64_t param_cnt = 0;
  ObString normalized_sql;
  ObIAllocator& allocator = result.get_mem_pool();
  ObSQLSessionInfo& session = result.get_session();
  ObParser parser(allocator, session.get_sql_mode(), session.get_local_collation_connection());
  ParseMode parse_mode = context.is_dbms_sql_ ? DBMS_SQL_MODE : STD_MODE;
  CHECK_COMPATIBILITY_MODE(context.session_info_);
  if (OB_ISNULL(context.session_info_) || OB_ISNULL(context.schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is NULL", K(ret));
  } else if (OB_FAIL(parser.parse(sql, parse_result, parse_mode))) {
    LOG_WARN("generate syntax tree failed", K(sql), K(ret));
  } else if (is_mysql_mode() && ObSQLUtils::is_mysql_ps_not_support_stmt(parse_result)) {
    ret = OB_ER_UNSUPPORTED_PS;
    LOG_WARN("This command is not supported in the prepared statement protocol yet", K(ret));
  } else if (result.is_simple_ps_protocol()) {
    if (OB_FAIL(ObResolverUtils::resolve_stmt_type(parse_result, stmt_type))) {
      LOG_WARN("failed to resolve stmt type", K(ret));
    } else {
      param_cnt = parse_result.question_mark_ctx_.count_;
      normalized_sql = context.is_dynamic_sql_ && parse_result.no_param_sql_len_ > 0
                           ? ObString(parse_result.no_param_sql_len_, parse_result.no_param_sql_)
                           : sql;
    }
  } else {
    if (context.is_dynamic_sql_ && !context.is_dbms_sql_) {
      parse_result.input_sql_ = parse_result.no_param_sql_;
      parse_result.input_sql_len_ = parse_result.no_param_sql_len_;
    }
    if (OB_FAIL(generate_stmt(parse_result, NULL, context, allocator, result, basic_stmt))) {
      LOG_WARN("generate stmt failed", K(ret));
    } else if (OB_ISNULL(basic_stmt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("generate stmt success, but stmt is NULL", K(ret));
    } else if (stmt::T_CALL_PROCEDURE == basic_stmt->get_stmt_type() &&
               FALSE_IT(result.set_cmd(dynamic_cast<ObICmd*>(basic_stmt)))) {
    } else if (OB_FAIL(fill_result_set(result, &context, true, *basic_stmt))) {
      LOG_WARN("Failed to fill result set", K(ret));
    } else if (OB_ISNULL(result.get_param_fields())) {
      LOG_WARN("invalid argument", K(result.get_param_fields()), K(ret));
    } else {
      param_cnt = result.get_param_fields()->count();
      stmt_type = basic_stmt->get_stmt_type();
      // If it is inner sql, such as pl internal sql, you need to use a formatted text string,
      // Because it is necessary to replace the sql variable in the pl with the standard ps text for hard syntax parse
      // The ps text of the external request cannot be formatted, because checksum verification is required when parsing
      // the ps execute package. It is necessary to ensure that the text of prepare is consistent with the text sent by
      // the client
      if (!is_inner_sql) {
        normalized_sql = sql;
      } else {
        normalized_sql = basic_stmt->get_sql_stmt();
      }
    }
    LOG_INFO("generate new stmt", K(param_cnt), K(stmt_type), K(normalized_sql), K(sql));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(do_add_ps_cache(normalized_sql, param_cnt, *context.schema_guard_, stmt_type, result, is_inner_sql))) {
      LOG_WARN("add to ps plan cache failed", K(ret));
    }
  }
  LOG_INFO("add ps cache", K(normalized_sql), K(param_cnt), K(ret));
  return ret;
}

int ObSql::handle_ps_prepare(const ObString& stmt, ObSqlCtx& context, ObResultSet& result, bool is_inner_sql)
{
  int ret = OB_SUCCESS;
  ObString trimed_stmt = const_cast<ObString&>(stmt).trim();
  if (trimed_stmt.empty()) {
    ret = OB_ERR_EMPTY_QUERY;
    LOG_WARN("query is empty", K(ret));
  } else if (OB_FAIL(init_result_set(context, result))) {
    LOG_WARN("failed to init result set", K(ret));
  }
  if (OB_SUCC(ret)) {
    ObSQLSessionInfo& session = result.get_session();
    ObPsCache* ps_cache = session.get_ps_cache();
    ObExecContext& ectx = result.get_exec_context();
    ObPhysicalPlanCtx* pctx = ectx.get_physical_plan_ctx();
    ObSchemaGetterGuard* schema_guard = context.schema_guard_;

#ifndef NDEBUG
    LOG_INFO("Begin to handle prepare statement", K(session.get_sessid()), K(stmt));
#endif

    if (OB_ISNULL(ps_cache) || OB_ISNULL(pctx) || OB_ISNULL(schema_guard)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("physical plan context or ps plan cache is NULL or schema_guard is null", K(ret), K(pctx), K(ps_cache));
    } else {
      bool need_do_real_prepare = false;
      uint64_t db_id = OB_INVALID_ID;
      (void)session.get_database_id(db_id);
      ObPsStmtId inner_stmt_id = OB_INVALID_STMT_ID;
      ObPsStmtId client_stmt_id = OB_INVALID_STMT_ID;
      ObPsStmtInfo* stmt_info = NULL;
      ObPsStmtItem* stmt_item = NULL;
      bool duplicate_prepare = false;
      bool is_expired = false;
      if (OB_FAIL(ps_cache->ref_stmt_item(db_id, stmt, stmt_item))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          need_do_real_prepare = true;
          if (REACH_TIME_INTERVAL(1000000)) {
            LOG_INFO("stmt id not exist", K(db_id), K(stmt), K(need_do_real_prepare));
          }
        } else {
          LOG_WARN("fail to get stmt id", K(ret), K(db_id), K(stmt));
        }
      } else if (OB_ISNULL(stmt_item) || OB_INVALID_STMT_ID == (inner_stmt_id = stmt_item->get_ps_stmt_id())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("stmt id is invalid", K(ret), K(inner_stmt_id), K(db_id), K(stmt), K(stmt_item));
      } else if (OB_FAIL(ps_cache->ref_stmt_info(inner_stmt_id, stmt_info))) {
        // inc stmt_info ref for session
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
        // check stmt_info whether expired, if expired, do nothing
      } else if (OB_FAIL(ps_cache->check_schema_version(*context.schema_guard_, *stmt_info, is_expired))) {
        LOG_WARN("fail to check schema version", K(ret));
      } else if (is_expired) {
        ObPsSqlKey ps_sql_key;
        stmt_info->set_is_expired();
        ps_sql_key.set_db_id(stmt_info->get_db_id());
        ps_sql_key.set_ps_sql(stmt_info->get_ps_sql());
        if (OB_FAIL(ps_cache->erase_stmt_item(ps_sql_key))) {
          LOG_WARN("fail to erase stmt item", K(ret), K(*stmt_info));
        }
        need_do_real_prepare = true;
      } else if (OB_FAIL(session.prepare_ps_stmt(
                     inner_stmt_id, stmt_info, client_stmt_id, duplicate_prepare, is_inner_sql))) {
        LOG_WARN("add ps session info failed", K(ret), K(inner_stmt_id), K(client_stmt_id));
      } else if (OB_FAIL(fill_result_set(client_stmt_id, *stmt_info, result))) {
        IGNORE_RETURN session.close_ps_stmt(client_stmt_id);
        LOG_WARN("fill result set failed", K(ret), K(client_stmt_id));
      }
      LOG_DEBUG("prepare done", K(need_do_real_prepare), K(duplicate_prepare), K(ret));
      if (OB_FAIL(ret) || need_do_real_prepare || duplicate_prepare) {
        if (NULL != stmt_item) {
          stmt_item->dec_ref_count_check_erase();
        }
        if (NULL != stmt_info) {
          ps_cache->deref_stmt_info(inner_stmt_id);
        }
      }
      if (OB_SUCC(ret) && need_do_real_prepare) {
        if (OB_FAIL(do_real_prepare(stmt, context, result, is_inner_sql))) {
          LOG_WARN("do_real_prepare failed", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (false == need_do_real_prepare) {
          ps_cache->inc_access_and_hit_count();
        } else {
          ps_cache->inc_access_count();
        }
      }
    }
  }
  return ret;
}

int ObSql::construct_param_store(const ParamStore& params, ParamStore& param_store)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(param_store.reserve(params.count()))) {
    LOG_WARN("failed to reserve array", K(ret));
  }
  for (int i = 0; OB_SUCC(ret) && i < params.count(); ++i) {
    if (share::is_oracle_mode() && ((params.at(i).is_varchar() && 0 == params.at(i).get_varchar().length()) ||
                                       (params.at(i).is_char() && 0 == params.at(i).get_char().length()) ||
                                       (params.at(i).is_nstring() && 0 == params.at(i).get_string_len()))) {
      const_cast<ObObjParam&>(params.at(i)).set_null();
      const_cast<ObObjParam&>(params.at(i)).set_param_meta();
    }
    if (OB_FAIL(param_store.push_back(params.at(i)))) {
      LOG_WARN("pushback param failed", K(ret));
    }
    LOG_TRACE("ps param is", K(params.at(i)), K(i));
  }
  return ret;
}

int ObSql::construct_not_paramalize(
    const ObIArray<int64_t>& offsets, const ParamStore& param_store, ObPlanCacheCtx& plan_ctx)
{
  int ret = OB_SUCCESS;
  plan_ctx.not_param_var_.set_capacity(offsets.count());
  for (int i = 0; OB_SUCC(ret) && i < offsets.count(); ++i) {
    const int64_t offset = offsets.at(i);
    PsNotParamInfo ps_not_param_var;
    ps_not_param_var.idx_ = offset;
    ps_not_param_var.ps_param_ = param_store.at(offset);
    if (OB_FAIL(plan_ctx.not_param_var_.push_back(ps_not_param_var))) {
      LOG_WARN("fail to push item to array", K(ret));
    } else if (OB_FAIL(plan_ctx.not_param_index_.add_member(offset))) {
      LOG_WARN("add member failed", K(ret), K(offset));
    }
  }
  return ret;
}

int ObSql::construct_no_check_type_params(const ObIArray<int64_t>& offsets, ParamStore& params_store)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < offsets.count(); i++) {
    const int64_t offset = offsets.at(i);
    if (offset < 0 || offset >= params_store.count()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Invalid offset", K(ret), K(offset), K(params_store.count()));
    } else if (!params_store.at(offset).is_ext()) {  // extend type need to be checked
      params_store.at(offset).set_need_to_check_type(false);
    } else {
      // real type do not need to be checked
      params_store.at(offset).set_need_to_check_extend_type(false);
    }
  }  // for end

  LOG_DEBUG("ps obj param infos", K(params_store), K(offsets));
  return ret;
}

int ObSql::construct_ps_param(const ParamStore& params, ObPlanCacheCtx& phy_ctx)
{
  int ret = OB_SUCCESS;
  phy_ctx.fp_result_.ps_params_.set_allocator(&phy_ctx.allocator_);
  phy_ctx.fp_result_.ps_params_.set_capacity(params.count());
  for (int i = 0; OB_SUCC(ret) && i < params.count(); ++i) {
    if (OB_FAIL(phy_ctx.fp_result_.ps_params_.push_back(&params.at(i)))) {
      LOG_WARN("add ps param failed", K(ret));
    }
  }
  return ret;
}

int ObSql::handle_ps_execute(const ObPsStmtId client_stmt_id, const stmt::StmtType stmt_type, const ParamStore& params,
    ObSqlCtx& context, ObResultSet& result, bool is_inner_sql)
{
  int ret = OB_SUCCESS;
  int get_plan_err = OB_SUCCESS;
  context.is_prepare_protocol_ = true;
  ObPsStmtId inner_stmt_id = client_stmt_id;
  context.stmt_type_ = stmt_type;
  if (OB_FAIL(init_result_set(context, result))) {
    LOG_WARN("failed to init result set", K(ret));
  } else {
    ObIAllocator& allocator = result.get_mem_pool();
    ObSQLSessionInfo& session = result.get_session();
    ObExecContext& ectx = result.get_exec_context();
    ObPsCache* ps_cache = session.get_ps_cache();
    ObPlanCache* plan_cache = session.get_plan_cache();
    bool use_plan_cache = session.get_local_ob_enable_plan_cache();
    ObPhysicalPlanCtx* pctx = ectx.get_physical_plan_ctx();
    ObSchemaGetterGuard* schema_guard = context.schema_guard_;
    if (OB_ISNULL(ps_cache) || OB_ISNULL(pctx) || OB_ISNULL(schema_guard) || OB_ISNULL(plan_cache)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("physical plan context or ps plan cache is NULL or schema_guard is null", K(ret), K(pctx), K(ps_cache));
    } else if (!is_inner_sql && OB_FAIL(session.get_inner_ps_stmt_id(client_stmt_id, inner_stmt_id))) {
      LOG_WARN("get_inner_ps_stmt_id failed", K(ret), K(client_stmt_id), K(inner_stmt_id));
    } else {
      context.statement_id_ = inner_stmt_id;
      ObPsStmtInfoGuard guard;
      ObPsStmtInfo* ps_info = NULL;
      if (OB_FAIL(ps_cache->get_stmt_info_guard(inner_stmt_id, guard))) {
        LOG_WARN("get stmt info guard failed", K(ret), K(inner_stmt_id));
      } else if (OB_ISNULL(ps_info = guard.get_stmt_info())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get stmt info is null", K(ret));
      } else if (ps_info->get_question_mark_count() != params.count()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("Incorrect arguments to execute",
            K(ps_info->get_question_mark_count()),
            K(ps_info->get_ps_sql()),
            K(params.count()),
            K(ret));
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "execute");
      } else if (OB_FAIL(construct_param_store(params, pctx->get_param_store_for_update()))) {
        LOG_WARN("construct param store failed", K(ret));
      } else {
        LOG_DEBUG("handle execute stmt", K(*ps_info));
        const ObString& sql = ps_info->get_ps_sql();
        context.cur_sql_ = sql;
#ifndef NDEBUG
        LOG_INFO("Begin to handle execute statement", K(session.get_sessid()), K(sql));
#endif
        if (OB_FAIL(session.store_query_string(sql))) {
          LOG_WARN("store query string fail", K(ret));
        } else if (OB_LIKELY(ObStmt::is_dml_stmt(stmt_type))) {
          // if plan not exist, generate plan
          ObPlanCacheCtx pc_ctx(sql,
              true, /*is_ps_mode*/
              allocator,
              context,
              ectx,
              session.get_effective_tenant_id());
          pc_ctx.fp_result_.pc_key_.key_id_ = inner_stmt_id;
          pc_ctx.normal_parse_const_cnt_ = params.count();
          pc_ctx.bl_key_.db_id_ = session.get_database_id();
          if (OB_FAIL(construct_ps_param(params, pc_ctx))) {
            LOG_WARN("construct_ps_param failed", K(ret));
          } else {
            if (!use_plan_cache) {
              /*do nothing*/
            } else if (OB_FAIL(pc_get_plan_and_fill_result(
                           pc_ctx, result, get_plan_err, ectx.get_need_disconnect_for_update()))) {
              LOG_DEBUG("fail to get plan", K(ret));
            }

            if (OB_FAIL(ret)) {  // do nothing
            } else if (!result.get_is_from_plan_cache()) {
              pctx->get_param_store_for_update().reset();
              if (OB_FAIL(construct_param_store(params, pctx->get_param_store_for_update()))) {
                LOG_WARN("construct param store failed", K(ret));
              } else if (OB_FAIL(handle_physical_plan(sql, context, result, pc_ctx, get_plan_err))) {
                if (OB_ERR_PROXY_REROUTE == ret) {
                  LOG_DEBUG("fail to handle physical plan", K(ret));
                } else {
                  LOG_WARN("fail to handle physical plan", K(ret));
                }
              }
            }
            if (OB_SUCC(ret) &&
                (OB_FAIL(after_get_plan(
                    pc_ctx, session, result.get_physical_plan(), result.get_is_from_plan_cache(), &params)))) {
              LOG_WARN("fail to handle after get plan", K(ret));
            }
          }
        } else {
          ObParser parser(allocator, session.get_sql_mode(), session.get_local_collation_connection());
          ParseResult parse_result;
          ParseMode parse_mode = context.is_dbms_sql_ ? DBMS_SQL_MODE : STD_MODE;
          if (OB_FAIL(parser.parse(sql, parse_result, parse_mode))) {
            LOG_WARN("failed to parse sql", K(ret), K(sql), K(stmt_type));
          } else if (OB_FAIL(generate_physical_plan(parse_result, NULL, context, result, true))) {
            LOG_WARN("generate physical plan failed", K(ret), K(sql), K(stmt_type));
          }
        }
      }
    }
  }
  return ret;
}

int ObSql::handle_remote_query(const ObRemoteSqlInfo& remote_sql_info, ObSqlCtx& context, ObExecContext& exec_ctx,
    ObPhysicalPlan*& plan, CacheRefHandleID& ref_handle_id)
{
  int ret = OB_SUCCESS;
  // trim the sql first, let 'select c1 from t' and '  select c1 from t' and hit the same plan_cache
  const ObString& trimed_stmt = remote_sql_info.remote_sql_;

  ObIAllocator& allocator = THIS_WORKER.get_sql_arena_allocator();
  ObSQLSessionInfo* session = exec_ctx.get_my_session();
  int get_plan_err = OB_SUCCESS;  // used for judge whether add plan to plan cache
  bool is_from_plan_cache = false;
  ObPlanCacheCtx* pc_ctx = NULL;
  ObSEArray<ObString, 1> queries;
  if (OB_ISNULL(session) || OB_ISNULL(remote_sql_info.ps_params_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret), K(session), K(remote_sql_info.ps_params_));
  } else if (OB_ISNULL(pc_ctx = static_cast<ObPlanCacheCtx*>(allocator.alloc(sizeof(ObPlanCacheCtx))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret), K(sizeof(ObPlanCacheCtx)));
  } else {
#if !defined(NDEBUG)
    LOG_INFO("Begin to handle remote statement",
        K(remote_sql_info),
        "sess_id",
        session->get_sessid(),
        "execution_id",
        session->get_current_execution_id());
#endif
    const uint64_t tenant_id = session->get_effective_tenant_id();
    bool use_plan_cache = session->get_local_ob_enable_plan_cache();
    context.self_add_plan_ = false;
    pc_ctx = new (pc_ctx) ObPlanCacheCtx(trimed_stmt,
        remote_sql_info.use_ps_, /*is_ps_mode*/
        allocator,
        context,
        exec_ctx,
        tenant_id);
    if (remote_sql_info.use_ps_) {
      // the execution plan of the ps mode and the ordinary text protocol cannot be reused,
      // it is necessary to distinguish here to avoid some problems when querying the plan
      // because the common text protocol key_id is OB_INVALID_ID, here we use
      // key_id=0+name=parameterized SQL to distinguish
      pc_ctx->fp_result_.pc_key_.key_id_ = 0;
      pc_ctx->fp_result_.pc_key_.name_ = trimed_stmt;
      pc_ctx->normal_parse_const_cnt_ = remote_sql_info.ps_params_->count();
      pc_ctx->bl_key_.db_id_ = session->get_database_id();
      if (OB_FAIL(construct_ps_param(*remote_sql_info.ps_params_, *pc_ctx))) {
        LOG_WARN("construct_ps_param failed", K(ret));
      }
    } else if (remote_sql_info.is_batched_stmt_) {
      // keep it consistent with the control end here. If it is batched stmt,
      // you need to do a parser segmentation first the cut query finally uses
      // the logic of batched multi stmt to query the plan cache and generate the plan
      ObParser parser(allocator, session->get_sql_mode(), session->get_local_collation_connection());
      ObMPParseStat parse_stat;
      if (OB_FAIL(parser.split_multiple_stmt(remote_sql_info.remote_sql_, queries, parse_stat))) {
        LOG_WARN("split multiple stmt failed", K(ret), K(remote_sql_info));
      } else {
        context.multi_stmt_item_.set_batched_queries(&queries);
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(session->get_database_id(pc_ctx->bl_key_.db_id_))) {
        LOG_WARN("Failed to get database id", K(ret));
      } else if (!use_plan_cache) {
        if (context.multi_stmt_item_.is_batched_multi_stmt()) {
          ret = OB_BATCHED_MULTI_STMT_ROLLBACK;
          LOG_WARN("batched multi_stmt needs rollback");
        }
      } else if (OB_FAIL(pc_get_plan(*pc_ctx, plan, get_plan_err, exec_ctx.get_need_disconnect_for_update()))) {
        LOG_DEBUG("fail to get plan", K(ret));
      } else if (OB_NOT_NULL(plan)) {
        if (plan->is_local_plan()) {
          is_from_plan_cache = true;
          ref_handle_id = pc_ctx->handle_id_;
        } else {
          is_from_plan_cache = false;
          ObCacheObjectFactory::free(plan, pc_ctx->handle_id_);
          plan = nullptr;
        }
      }
    }
  }

  if (OB_SUCC(ret) && !is_from_plan_cache) {
    SMART_VAR(ObResultSet, tmp_result, *session, allocator)
    {
      tmp_result.set_exec_context(exec_ctx);
      int64_t initial_param_count = pc_ctx->fp_result_.ps_params_.count();
      for (int64_t i = remote_sql_info.ps_params_->count(); i > initial_param_count; --i) {
        remote_sql_info.ps_params_->pop_back();
      }
      if (OB_FAIL(handle_physical_plan(trimed_stmt, context, tmp_result, *pc_ctx, get_plan_err))) {
        if (OB_ERR_PROXY_REROUTE == ret) {
          LOG_DEBUG("fail to handle physical plan", K(ret));
        } else {
          LOG_WARN("fail to handle physical plan", K(ret));
        }
      } else {
        plan = tmp_result.get_physical_plan();
        ref_handle_id = tmp_result.get_ref_handle();
      }
    }
  }

  // set auto-increment related param into physical plan ctx
  // if get plan from plan cache, reset its auto-increment variable here
  // do not set variable for hidden primary key; its default value is 1
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(!plan->is_local_plan())) {
      ret = OB_LOCATION_NOT_EXIST;
      LOG_WARN("plan type is invalid", K(remote_sql_info), KPC(plan));
    } else if (OB_FAIL(after_get_plan(*pc_ctx, *session, plan, is_from_plan_cache, NULL /*ps param*/))) {
      LOG_WARN("fail to handle after get plan", K(ret));
    }
  }
  LOG_DEBUG("get remote plan", K(ret), K(is_from_plan_cache), KPC(plan));
  // clear the warning buffer, because the warning buffer that generates the execution plan
  // is recorded on the control server, and there is no need to record it here
  // Otherwise it will cause repeated warning messages
  ob_reset_tsi_warning_buffer();
  return ret;
}

int ObSql::handle_batch_req(const int type, const ObReqTimestamp& req_ts, const char* buf, int32_t size)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  switch (type) {
    case OB_SQL_REMOTE_TASK_TYPE: {
      lib::ContextParam param;
      param.set_mem_attr(lib::current_tenant_id(), ObModIds::OB_SQL_EXECUTOR, ObCtxIds::DEFAULT_CTX_ID)
          .set_properties(lib::USE_TL_PAGE_OPTIONAL)
          .set_page_size(!lib::is_mini_mode() ? OB_MALLOC_BIG_BLOCK_SIZE : OB_MALLOC_MIDDLE_BLOCK_SIZE);
      CREATE_WITH_TEMP_CONTEXT(param)
      {
        if (OB_FAIL(handle_remote_batch_req<ObRpcRemoteASyncExecuteP>(req_ts, buf, size))) {
          LOG_WARN("handle remote batch async task failed", K(ret), K(size));
        }
      }
      break;
    }
    case OB_SQL_REMOTE_RESULT_TYPE: {
      if (OB_FAIL(handle_remote_batch_req<ObRpcRemotePostResultP>(req_ts, buf, size))) {
        LOG_WARN("handle remote batch result failed", K(ret), K(size));
      }
      break;
    }
    default: {
      ret = OB_NOT_SUPPORTED;
      break;
    }
  }
  return ret;
}

template <typename ProcessorT>
int ObSql::handle_remote_batch_req(const ObReqTimestamp& req_ts, const char* buf, int32_t size)
{
  int ret = OB_SUCCESS;
  ProcessorT processor(GCTX);
  processor.set_from_batch();
  if (OB_FAIL(processor.init())) {
    LOG_WARN("init processor failed", K(ret));
  } else {
    auto& arg = processor.get_arg();
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
    } else if (OB_FAIL(processor.before_response())) {
      LOG_WARN("before response remote batch req failed", K(ret));
    } else if (OB_FAIL(processor.after_process())) {
      LOG_WARN("after process batch req failed", K(ret));
    }
  }
  processor.cleanup();
  return ret;
}

inline int ObSql::handle_text_query(const ObString& stmt, ObSqlCtx& context, ObResultSet& result)
{
  int ret = OB_SUCCESS;
  // trim the sql first, let 'select c1 from t' and '  select c1 from t' and hit the same plan_cache
  ObString trimed_stmt = const_cast<ObString&>(stmt).trim();
  if (OB_FAIL(init_result_set(context, result))) {
    LOG_WARN("failed to init result set", K(ret));
  } else if (trimed_stmt.empty()) {
    ret = OB_ERR_EMPTY_QUERY;
    LOG_WARN("query is empty", K(ret));
    LOG_USER_ERROR(OB_ERR_EMPTY_QUERY);
    result.get_exec_context().set_need_disconnect(false);
    // FIXME  NG_TRACE_EXT(set_need_disconnect, OB_ID(need_disconnect), false);
  }

  ObIAllocator& allocator = THIS_WORKER.get_sql_arena_allocator();
  ObSQLSessionInfo& session = result.get_session();
  const uint64_t tenant_id = session.get_effective_tenant_id();
  ObExecContext& ectx = result.get_exec_context();
  int get_plan_err = OB_SUCCESS;  // used for judge whether add plan to plan cache
  bool use_plan_cache = session.get_local_ob_enable_plan_cache();
  ObPlanCacheCtx* pc_ctx = NULL;
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (NULL == (pc_ctx = static_cast<ObPlanCacheCtx*>(allocator.alloc(sizeof(ObPlanCacheCtx))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret), K(sizeof(ObPlanCacheCtx)));
  } else {
    context.cur_sql_ = trimed_stmt;
    pc_ctx = new (pc_ctx) ObPlanCacheCtx(trimed_stmt,
        false, /*is_ps_mode*/
        allocator,
        context,
        ectx,
        tenant_id);
    if (OB_FAIL(session.get_database_id(pc_ctx->bl_key_.db_id_))) {
      LOG_WARN("Failed to get database id", K(ret));
    } else if (!use_plan_cache) {
      if (context.multi_stmt_item_.is_batched_multi_stmt()) {
        ret = OB_BATCHED_MULTI_STMT_ROLLBACK;
        LOG_WARN("batched multi_stmt needs rollback");
      }
    } else if (OB_FAIL(
                   pc_get_plan_and_fill_result(*pc_ctx, result, get_plan_err, ectx.get_need_disconnect_for_update()))) {
      LOG_DEBUG("fail to get plan", K(ret));
    }
  }

  int tmp_ret = ret;
  if (OB_FAIL(handle_large_query(tmp_ret, result, ectx.get_need_disconnect_for_update(), ectx))) {
    // do nothing
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (!result.get_is_from_plan_cache()) {
    // I didn't get the plan from the plan cache, take a long path to generate the plan
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
    if (OB_FAIL(after_get_plan(*pc_ctx, session, result.get_physical_plan(), result.get_is_from_plan_cache(), NULL))) {
      LOG_WARN("fail to handle after get plan", K(ret));
    }
  }
  // for inner sql, release the optimization memory
  if (!THIS_WORKER.has_req_flag()) {
    // only for inner sql
    if (NULL != pc_ctx) {
      pc_ctx->~ObPlanCacheCtx();
    }
  }

  return ret;
}

int ObSql::handle_large_query(int tmp_ret, ObResultSet& result, bool& need_disconnect, ObExecContext& exec_ctx)
{
  int ret = OB_SUCCESS;
  if (tmp_ret != OB_SUCCESS && tmp_ret != OB_PC_LOCK_CONFLICT) {
    ret = tmp_ret;
  } else if (result.get_session().is_inner() || !ObStmt::is_dml_stmt(result.get_stmt_type())) {
    ret = (tmp_ret == OB_PC_LOCK_CONFLICT) ? OB_SUCCESS : tmp_ret;
  } else {
    const int64_t curr_time = ObTimeUtility::current_time();
    const int64_t lqt = GCONF.large_query_threshold;
    int64_t elapsed_time = curr_time - THIS_THWORKER.get_query_start_time();
    bool is_large_query = false;
    bool lq_from_plan = true;
    int64_t total_process_time = 0;
    int64_t exec_times = 0;
    ObPhysicalPlan* plan = NULL;
    if (result.get_is_from_plan_cache()) {
      if (OB_ISNULL(plan = result.get_physical_plan())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(ret));
      } else {
        exec_times = plan->stat_.get_execute_count();
        total_process_time = plan->stat_.total_process_time_;
        if (exec_times > 0 && (total_process_time / exec_times) > lqt) {
          plan->inc_large_querys();
          is_large_query = true;
          lq_from_plan = true;
        } else if (plan->is_use_px() && plan->get_px_dop() > 1) {
          plan->inc_large_querys();
          is_large_query = true;
          lq_from_plan = true;
          exec_times = 1;
        }
      }
    }
    // The actual compilation time judges whether it is a large request
    if (OB_SUCC(ret) && is_large_query == false) {
      if (OB_PC_LOCK_CONFLICT == tmp_ret || elapsed_time > lqt) {
        is_large_query = true;
        lq_from_plan = false;
      }
    }
    if (OB_SUCC(ret) && is_large_query && OB_FAIL(THIS_WORKER.check_large_query_quota())) {
      need_disconnect = false;
      if (lq_from_plan) {
        plan->inc_delayed_large_querys();
        LOG_INFO("It's a large query, need delay, do not need disconnect",
            "avg_process_time",
            total_process_time / exec_times,
            "exec_cnt",
            exec_times,
            "large_query_threshold",
            lqt,
            K(plan->get_plan_id()),
            K(ret));
      } else {
        LOG_INFO("compile time is too long, need delay", K(elapsed_time), K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(exec_ctx.get_physical_plan_ctx())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("phy plan ctx is NULL", K(ret));
      } else {
        exec_ctx.get_physical_plan_ctx()->set_large_query(is_large_query);
      }
    }
  }

  return ret;
}

int ObSql::handle_parallel_query(ObResultSet& result, bool& need_disconnect)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlan* plan = NULL;
  if (OB_ISNULL(plan = result.get_physical_plan())) {
    // skip
  } else if (plan->is_use_px() && plan->get_px_dop() > 1) {
    plan->inc_large_querys();
    if (OB_FAIL(THIS_WORKER.check_large_query_quota())) {
      need_disconnect = false;
      plan->inc_delayed_large_querys();
      LOG_INFO("It's a px query, use large query thread queue");
    }
  }
  return ret;
}

int ObSql::generate_stmt(ParseResult& parse_result, ObPlanCacheCtx* pc_ctx, ObSqlCtx& context, ObIAllocator& allocator,
    ObResultSet& result, ObStmt*& stmt)
{
  int ret = OB_SUCCESS;
  uint64_t session_id = 0;
  ObResolverParams resolver_ctx;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  ObSchemaChecker* schema_checker = NULL;

  int64_t last_mem_usage = allocator.total();
  int64_t resolver_mem_usage = 0;
  if (OB_FAIL(sanity_check(context))) {
    LOG_WARN("Failed to do sanity check", K(ret));
  } else {
    if (result.get_session().get_session_type() != ObSQLSessionInfo::INNER_SESSION) {
      session_id = result.get_session().get_sessid_for_table();
    } else {
      session_id = OB_INVALID_ID;
    }
    schema_checker = OB_NEWx(ObSchemaChecker, (&allocator));
    if (OB_UNLIKELY(NULL == schema_checker)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("Failed to malloc ObSchemaChecker", K(ret));
    } else if (NULL == context.schema_guard_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("context schema guard is null", K(ret));
    } else if (FALSE_IT(context.sql_schema_guard_.set_schema_guard(context.schema_guard_))) {
    } else if (OB_FAIL(schema_checker->init(context.sql_schema_guard_, session_id))) {
      LOG_WARN("fail to init schema_checker", K(ret));
    } else {
    }  // do nothing
  }

  if (OB_SUCC(ret)) {
    resolver_ctx.allocator_ = &allocator;
    resolver_ctx.schema_checker_ = schema_checker;
    resolver_ctx.session_info_ = context.session_info_;
    resolver_ctx.expr_factory_ = result.get_exec_context().get_expr_factory();
    resolver_ctx.stmt_factory_ = result.get_exec_context().get_stmt_factory();
    resolver_ctx.cur_sql_ = context.cur_sql_;
    resolver_ctx.is_restore_ = context.is_restore_;
    resolver_ctx.is_ddl_from_primary_ = context.is_ddl_from_primary_;
    if (NULL != pc_ctx) {
      resolver_ctx.select_item_param_infos_ = &pc_ctx->select_item_param_infos_;
    }
    plan_ctx = result.get_exec_context().get_physical_plan_ctx();
    if (OB_ISNULL(plan_ctx) || OB_ISNULL(result.get_exec_context().get_stmt_factory())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Plan ctx should not be NULL", K(ret), KP(plan_ctx));
    } else if (OB_ISNULL(resolver_ctx.query_ctx_ = result.get_exec_context().get_stmt_factory()->get_query_ctx())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("allocate query context failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    resolver_ctx.is_prepare_protocol_ = context.is_prepare_protocol_;
    resolver_ctx.is_prepare_stage_ = context.is_prepare_stage_;
    resolver_ctx.is_dynamic_sql_ = context.is_dynamic_sql_;
    resolver_ctx.is_dbms_sql_ = context.is_dbms_sql_;
    resolver_ctx.statement_id_ = context.statement_id_;
    resolver_ctx.param_list_ = &plan_ctx->get_param_store();
    resolver_ctx.sql_proxy_ = context.sql_proxy_;
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(parse_result.result_tree_) || OB_ISNULL(parse_result.result_tree_->children_) ||
             OB_ISNULL(parse_result.result_tree_->children_[0])) {
    ret = OB_INVALID_ARGUMENT;
    SQL_LOG(WARN,
        "invalid args",
        KP(parse_result.result_tree_),
        KP(parse_result.result_tree_->children_),
        KP(parse_result.result_tree_->children_[0]));
  }

  if (OB_SUCC(ret)) {
    // set # of question marks
    resolver_ctx.query_ctx_->question_marks_count_ = static_cast<int64_t>(parse_result.question_mark_ctx_.count_);
    LOG_DEBUG("question mark size is ", K(parse_result.question_mark_ctx_.count_));

    // forbid px usage if the query plan is found to be a bushy tree
    // or forbid px usage in batched multi stmt
    resolver_ctx.query_ctx_->forbid_use_px_ =
        context.is_bushy_tree_ || context.multi_stmt_item_.is_batched_multi_stmt();

    ObResolver resolver(resolver_ctx);
    NG_TRACE(resolve_begin);

    ret = resolver.resolve(ObResolver::IS_NOT_PREPARED_STMT, *parse_result.result_tree_->children_[0], stmt);
    // set const param constraint after resolving
    context.all_plan_const_param_constraints_ = &(resolver_ctx.query_ctx_->all_plan_const_param_constraints_);
    context.all_possible_const_param_constraints_ = &(resolver_ctx.query_ctx_->all_possible_const_param_constraints_);
    context.all_equal_param_constraints_ = &(resolver_ctx.query_ctx_->all_equal_param_constraints_);
    context.trans_happened_route_ = &(resolver_ctx.query_ctx_->trans_happened_route_);
    context.cur_stmt_ = stmt;

    LOG_DEBUG("got plan const param constraints", K(resolver_ctx.query_ctx_->all_plan_const_param_constraints_));
    LOG_DEBUG("got all const param constraints", K(resolver_ctx.query_ctx_->all_possible_const_param_constraints_));

    NG_TRACE(resolve_end);
    if (OB_FAIL(ret)) {
      /* for audit */
      if (NULL != stmt) {
        result.set_stmt_type(stmt->get_stmt_type());
      }
      SQL_LOG(WARN, "failed to resolve", K(ret));
    } else {
      // process stmt
      if (NULL != stmt) {
        SQL_LOG(DEBUG, "SET STMT PARAM COUNT", K(resolver.get_params().prepare_param_count_), K(&resolver_ctx));
        bool need_rebuild = lib::is_mysql_mode() ? false : resolver_ctx.is_prepare_stage_;
        if (stmt->is_insert_stmt() || stmt->is_update_stmt() || stmt->is_delete_stmt()) {
          ObDelUpdStmt& dml_stmt = static_cast<ObDelUpdStmt&>(*stmt);
          if (dml_stmt.get_returning_into_exprs().count() != 0) {
            need_rebuild = true;
          }
        }
        if (need_rebuild) {
          if (OB_FAIL(result.get_external_retrieve_info().build(
                  *stmt, result.get_session(), resolver.get_params().external_param_info_))) {
            SQL_LOG(WARN, "failed to build external retrieve info", K(ret));
          } else {
            if (result.get_external_params().empty() && result.get_into_exprs().empty()) {
              if (stmt->get_sql_stmt().empty()) {
                stmt->set_sql_stmt(parse_result.input_sql_, parse_result.input_sql_len_);
              }
              result.get_route_sql() = stmt->get_sql_stmt();
              stmt->set_prepare_param_count(parse_result.question_mark_ctx_.count_);
            } else {
              stmt->set_prepare_param_count(resolver.get_params().prepare_param_count_);
            }
          }
        } else {
          stmt->set_sql_stmt(parse_result.input_sql_, parse_result.input_sql_len_);
          result.get_route_sql() = stmt->get_sql_stmt();
          stmt->set_prepare_param_count(parse_result.question_mark_ctx_.count_);
        }
        if (OB_SUCC(ret)) {
          result.set_stmt_type(stmt->get_stmt_type());                  // used by query retry
          result.set_literal_stmt_type(stmt->get_literal_stmt_type());  // used by show warn, show trace
          ObDMLStmt* dml_stmt = NULL;
          ObVariableSetStmt* variable_set_stmt = NULL;
          if ((dml_stmt = dynamic_cast<ObDMLStmt*>(stmt)) != NULL) {
            result.set_is_calc_found_rows(dml_stmt->is_calc_found_rows());
            plan_ctx->set_is_affect_found_row(dml_stmt->is_affect_found_rows());
            context.force_print_trace_ = dml_stmt->get_stmt_hint().force_trace_log_;
            if ((MpQuery == context.exec_type_) && (dml_stmt->get_stmt_hint().log_level_.length() > 0)) {
              ObString& log_level = dml_stmt->get_stmt_hint().log_level_;
              if (OB_UNLIKELY(OB_SUCCESS != process_thread_log_id_level_map(log_level.ptr(), log_level.length()))) {
                LOG_WARN("Failed to process thread log id level map");
              }
            }
            ObDelUpdStmt* del_up_stmt = NULL;
            del_up_stmt = dynamic_cast<ObDelUpdStmt*>(dml_stmt);
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

int ObSql::generate_physical_plan(ParseResult& parse_result, ObPlanCacheCtx* pc_ctx, ObSqlCtx& sql_ctx,
    ObResultSet& result, const bool is_ps_mode /* false */)
{
  int ret = OB_SUCCESS;
  bool is_valid = true;
  ObStmt* basic_stmt = NULL;
  ObIAllocator& allocator = result.get_mem_pool();
  ObStmtNeedPrivs stmt_need_privs;
  ObStmtOraNeedPrivs stmt_ora_need_privs;
  stmt_need_privs.need_privs_.set_allocator(&allocator);
  stmt_ora_need_privs.need_privs_.set_allocator(&allocator);
  _LOG_DEBUG(
      "start to generate physical plan for query.(query = %.*s)", parse_result.input_sql_len_, parse_result.input_sql_);
  if (OB_FAIL(sanity_check(sql_ctx))) {  // check sql_ctx.session_info_ and sql_ctx.schema_guard_
    LOG_WARN("Failed to do sanity check", K(ret));
  } else if (OB_FAIL(generate_stmt(parse_result, pc_ctx, sql_ctx, allocator, result, basic_stmt))) {
    LOG_WARN("Failed to generate stmt", K(ret), K(result.get_exec_context().need_disconnect()));
  } else if (OB_ISNULL(basic_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Generate stmt success, but stmt is NULL", K(ret));
  } else if (OB_FAIL(
                 ObPrivilegeCheck::check_privilege_new(sql_ctx, basic_stmt, stmt_need_privs, stmt_ora_need_privs))) {
    LOG_WARN("Failed to check ora privilege info", K(ret));
  } else if (OB_FAIL(ObPrivilegeCheck::check_password_expired(sql_ctx, basic_stmt->get_stmt_type()))) {
    LOG_WARN("Falied to check password expired", K(ret));
  } else if (sql_ctx.multi_stmt_item_.is_batched_multi_stmt() && NULL != pc_ctx) {
    ret = OB_BATCHED_MULTI_STMT_ROLLBACK;
    LOG_WARN("failed to check batched multi_stmt after resolver", K(ret));
  } else if (!is_valid) {
    ret = OB_BATCHED_MULTI_STMT_ROLLBACK;
    LOG_TRACE("batched multi_stmt needs rollback", K(ret));
  } else { /*do nothing*/
  }

  if (OB_SUCC(ret)) {
    if (basic_stmt->is_dml_stmt() || basic_stmt->is_explain_stmt()) {
      ObPhysicalPlanCtx* pctx = result.get_exec_context().get_physical_plan_ctx();
      if (OB_ISNULL(pctx) || OB_ISNULL(result.get_exec_context().get_expr_factory())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("Physical plan ctx should not be NULL", K(ret));
      } else if (OB_FAIL(fill_result_set(result, &sql_ctx, is_ps_mode, *basic_stmt))) {
        LOG_WARN("Failed to fill result set", K(ret));
      } else {
        ObDMLStmt* stmt = static_cast<ObDMLStmt*>(basic_stmt);
        SQL_LOG(DEBUG, "stmt", "stmt", *stmt);
        SQL_LOG(DEBUG, "stmt success", "query", SJ(*stmt));
        ObStmtHint& stmt_hint = stmt->get_stmt_hint();
        ObQueryHint query_hint = stmt->get_stmt_hint().get_query_hint();
        sql_ctx.loc_sensitive_hint_.update_to(stmt->get_stmt_hint());
        sql_ctx.session_info_->set_early_lock_release(stmt_hint.enable_lock_early_release_);
        ObOptimizerPartitionLocationCache optimizer_location_cache(allocator, sql_ctx.partition_location_cache_);
        ObOptimizerContext optctx(sql_ctx.session_info_,
            &result.get_exec_context(),
            &sql_ctx.sql_schema_guard_,
            stat_mgr_,
            opt_stat_mgr_,
            partition_service_,
            allocator,
            &optimizer_location_cache,
            &pctx->get_param_store(),
            self_addr_,
            GCTX.srv_rpc_proxy_,
            sql_ctx.merged_version_,
            query_hint,
            *result.get_exec_context().get_expr_factory(),
            stmt);
        bool is_restore = false;
        uint64_t effective_tid = result.get_session().get_effective_tenant_id();
        if (OB_FAIL(ret)) {
        } else if (OB_ISNULL(sql_ctx.schema_guard_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("schema guard is null", K(ret));
        } else if (OB_FAIL(sql_ctx.schema_guard_->check_tenant_is_restore(effective_tid, is_restore))) {
          LOG_WARN("fail to check if tenant is restore", K(ret), K(effective_tid));
        } else if (is_restore) {
          optctx.set_use_default_stat();
        }
        ObOptimizer optimizer(optctx);
        ObLogPlan* logical_plan = NULL;
        ObPhysicalPlan* phy_plan = NULL;
        bool same_bool_param = false;

        if (OB_NOT_NULL(result.get_session().get_plan_cache())) {
          effective_tid = result.get_session().get_plan_cache()->get_tenant_id();
        }

        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(ObCacheObjectFactory::alloc(phy_plan, PLAN_GEN_HANDLE, effective_tid))) {
          LOG_WARN("fail to alloc phy_plan", K(ret));
        } else if (OB_UNLIKELY(NULL == phy_plan)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_ERROR("Failed to alloc physical plan from tc factory", K(ret));
        } else {
          phy_plan->stat_.enable_early_lock_release_ = sql_ctx.session_info_->get_early_lock_release();
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(calc_pre_calculable_exprs(result.get_exec_context(), *stmt, *phy_plan))) {
          LOG_WARN("Failed to deal pre calculable exprs", K(ret));
        } else if (OB_FAIL(transform_stmt(&sql_ctx.sql_schema_guard_,
                       sql_ctx.partition_location_cache_,
                       partition_service_,
                       stat_mgr_,
                       opt_stat_mgr_,
                       &self_addr_,
                       sql_ctx.merged_version_,
                       phy_plan,
                       result.get_exec_context(),
                       stmt))) {  // rewrite stmt
          LOG_WARN("Failed to transforme stmt", K(ret));
          //        } else if (!optctx.use_default_stat() &&
          //                   OB_FAIL(analyze_table_stat_version(
          //                             sql_ctx.schema_guard_, optctx.get_opt_stat_manager(), *stmt))) {
          //          LOG_WARN("Failed to analyze table stat version", K(ret));
        } else if (OB_FAIL(
                       optimize_stmt(optimizer, *(sql_ctx.session_info_), *stmt, logical_plan))) {  // gen logical plan
          LOG_WARN("Failed to optimizer stmt", K(ret));
        } else if (OB_ISNULL(logical_plan)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid null logical plan", K(ret), K(logical_plan));
        } else if (OB_FAIL(code_generate(
                       sql_ctx, result, stmt, stmt_need_privs, stmt_ora_need_privs, logical_plan, phy_plan))) {  // gen
                                                                                                                 // phy
                                                                                                                 // plan
          LOG_WARN("Failed to genenrate phy plan", K(ret));
        } else if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_2100 &&
                   OB_PHY_PLAN_LOCAL != phy_plan->get_location_type() &&
                   (sql_ctx.session_info_->is_nested_session() || phy_plan->has_nested_sql())) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("non-local phy plan with nested sql is not supported in upgrade process", K(ret));
          // TODO
          // } else if (ObPlanSet::check_array_bind_same_bool_param(phy_plan->get_params_info(),
          //                                                        pctx->get_param_store(),
          //                                                        same_bool_param)) {
          //   LOG_WARN("fail to check array bind bool param", K(ret));
          // } else if (false == same_bool_param) {
          //   ret = OB_ARRAY_BINDING_ROLLBACK;
          //} else if (!sql_ctx.multi_stmt_item_.is_batched_multi_stmt() &&
          // OB_PHY_PLAN_LOCAL != phy_plan->get_plan_type()) {
          // for (int64_t i = 0; OB_SUCC(ret) && i < pctx->get_param_store().count(); i++) {
          // if (pctx->get_param_store().at(i).is_ext()) {
          // ret = OB_ARRAY_BINDING_ROLLBACK;
          // LOG_DEBUG("array binding need rollback", K(ret));
          //}
          //}
        }

        // memory debug for sql work arena
        if (OB_UNLIKELY(result.get_mem_pool().total() > SQL_MEM_SIZE_LIMIT)) {
          const char* SQL = parse_result.input_sql_;
          int64_t total_mem_used = result.get_mem_pool().total();
          LOG_INFO("[SQL MEM USAGE] use too much memory",
              K(total_mem_used),
              K(ObString(parse_result.input_sql_len_, parse_result.input_sql_)));
        }

        if (OB_SUCC(ret) && OB_FAIL(phy_plan->generate_mock_rowid_tables(*sql_ctx.schema_guard_))) {
          LOG_WARN("failed to generate mock rowid tables", K(ret));
        }

        if (OB_FAIL(ret) && NULL != phy_plan) {
          ObCacheObjectFactory::free(phy_plan, PLAN_GEN_HANDLE);
          result.set_physical_plan(PLAN_GEN_HANDLE, NULL);
        } else if (stmt::T_SELECT == stmt->get_stmt_type() &&
                   stmt::T_SHOW_PARAMETERS == stmt->get_literal_stmt_type()) {
          ObSelectStmt* select_stmt = static_cast<ObSelectStmt*>(stmt);
          pctx->set_tenant_id(select_stmt->get_tenant_id());
          pctx->set_show_seed(select_stmt->get_show_seed());
        } else {
          pctx->set_phy_plan(phy_plan);
        }
      }
    } else {
      ObICmd* cmd = dynamic_cast<ObICmd*>(basic_stmt);
      if (OB_UNLIKELY(NULL == cmd)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail cast basic stmt to cmd", K(ret));
      } else {
        result.set_cmd(cmd);
        if (OB_FAIL(fill_result_set(result, &sql_ctx, is_ps_mode, *basic_stmt))) {
          LOG_WARN("Failed to fill result set", K(ret));
        }
      }
    }
  }

  return ret;
}

int ObSql::calc_pre_calculable_exprs(ObExecContext& exec_ctx, ObDMLStmt& stmt, ObPhysicalPlan& phy_plan)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(calc_pre_calculable_exprs(stmt.get_calculable_exprs(), exec_ctx, stmt, phy_plan))) {
    LOG_WARN("Failed to calc pre cacluable exprs");
  }
  return ret;
}

int ObSql::calc_pre_calculable_exprs(
    ObIArray<ObHiddenColumnItem>& calculable_exprs, ObExecContext& exec_ctx, ObDMLStmt& stmt, ObPhysicalPlan& phy_plan)
{
  int ret = OB_SUCCESS;
  stmt::StmtType stmt_type = stmt::T_NONE;
  ObDList<ObSqlExpression> pre_calc_exprs;
  ObPhysicalPlanCtx* plan_ctx = exec_ctx.get_physical_plan_ctx();
  ObSQLSessionInfo* session_info = exec_ctx.get_my_session();
  ObRawExprFactory* expr_factory = exec_ctx.get_expr_factory();
  if (OB_ISNULL(plan_ctx) || OB_ISNULL(session_info) || OB_ISNULL(expr_factory)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL pointer", K(ret), KP(session_info), KP(expr_factory));
  } else if (stmt.is_explain_stmt()) {
    ObDMLStmt* real_stmt = static_cast<ObExplainStmt&>(stmt).get_explain_query_stmt();
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
    if (OB_FAIL(ObTransformPreProcess::transform_expr(
            *expr_factory, *session_info, calculable_exprs.at(i).expr_, transformed))) {
      LOG_WARN("transform expr failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    // is_ignore should be set before pre_calculate, which is used in column_conv
    bool is_ignore_stmt = false;
    bool need_fetch_cur_time = false;
    ObDelUpdStmt* modify_stmt = dynamic_cast<ObDelUpdStmt*>(&stmt);
    if (NULL != modify_stmt) {
      is_ignore_stmt = modify_stmt->is_ignore();
    }

    if (session_info->use_static_typing_engine()) {
      if (OB_FAIL(calc_pre_calculable_exprs(stmt, calculable_exprs, is_ignore_stmt, exec_ctx, phy_plan))) {
        LOG_WARN("failed to generate and calcute rt exprs", K(ret));
      }
    } else if (OB_FAIL(
                   ObCodeGeneratorImpl::generate_calculable_exprs(calculable_exprs, stmt, phy_plan, pre_calc_exprs))) {
      LOG_WARN("generate calculable expr failed", K(ret));
    } else if (FALSE_IT(need_fetch_cur_time = phy_plan.get_fetch_cur_time() && !plan_ctx->has_cur_time())) {
      // never reach
      // set current time before do pre calculation
    } else if (need_fetch_cur_time && FALSE_IT(plan_ctx->set_cur_time(ObTimeUtility::current_time(), *session_info))) {
      // do nothing
    } else if (OB_FAIL(ObPhysicalPlan::pre_calculation(stmt_type, is_ignore_stmt, pre_calc_exprs, exec_ctx))) {
      LOG_WARN("preliminary calculate const expr failed", K(ret));
    } else {
      DLIST_FOREACH_REMOVESAFE(node, pre_calc_exprs)
      {
        node = pre_calc_exprs.remove(node);
        if (OB_ISNULL(node)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("node is null");
        } else if (OB_FAIL(phy_plan.add_calculable_expr(node))) {
          LOG_WARN("add calcaulable expr failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObSql::transform_stmt(ObSqlSchemaGuard* sql_schema_guard,
    share::ObIPartitionLocationCache* partition_location_cache, storage::ObPartitionService* partition_service,
    common::ObStatManager* stat_mgr, common::ObOptStatManager* opt_stat_mgr, common::ObAddr* self_addr,
    int64_t merged_version, ObPhysicalPlan* phy_plan, ObExecContext& exec_ctx, ObDMLStmt*& stmt)
{
  int ret = OB_SUCCESS;
  ObDMLStmt* transform_stmt = stmt;
  int64_t last_mem_usage = exec_ctx.get_allocator().total();
  int64_t transformer_mem_usage = 0;
  // get transform stmt
  if (OB_ISNULL(stmt) || OB_ISNULL(sql_schema_guard) || OB_ISNULL(sql_schema_guard->get_schema_guard()) ||
      OB_ISNULL(partition_location_cache) || OB_ISNULL(partition_service) || OB_ISNULL(stat_mgr) ||
      OB_ISNULL(opt_stat_mgr) || OB_ISNULL(self_addr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null point",
        K(stmt),
        KP(sql_schema_guard),
        K(partition_location_cache),
        K(partition_service),
        K(stat_mgr),
        K(opt_stat_mgr),
        K(self_addr),
        K(ret));
  } else if (OB_ISNULL(exec_ctx.get_my_session()) || OB_ISNULL(exec_ctx.get_virtual_table_ctx().schema_guard_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("session_info and schema manager in sql_ctx should not be NULL", K(ret));
  } else if (stmt->is_explain_stmt()) {
    if (OB_ISNULL(transform_stmt = static_cast<ObExplainStmt*>(stmt)->get_explain_query_stmt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("Explain query stmt is NULL", K(ret));
    }
  } else {
  }  // do nothing

  ObTransformerCtx trans_ctx;
  ObSchemaChecker schema_checker;

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(replace_stmt_bool_filter(exec_ctx, transform_stmt))) {
    LOG_WARN("Failed to replace stmt bool filter", K(ret));
  } else if (OB_FAIL(schema_checker.init(*sql_schema_guard))) {
    LOG_WARN("fail to init schema_checker", K(ret));
  } else {
    trans_ctx.allocator_ = &exec_ctx.get_allocator();
    ;
    trans_ctx.schema_checker_ = &schema_checker;
    trans_ctx.session_info_ = exec_ctx.get_my_session();
    trans_ctx.exec_ctx_ = &exec_ctx;
    trans_ctx.expr_factory_ = exec_ctx.get_expr_factory();
    trans_ctx.stmt_factory_ = exec_ctx.get_stmt_factory();
    trans_ctx.partition_location_cache_ = partition_location_cache, trans_ctx.stat_mgr_ = stat_mgr;
    trans_ctx.opt_stat_mgr_ = opt_stat_mgr;
    trans_ctx.partition_service_ = partition_service;
    trans_ctx.sql_schema_guard_ = sql_schema_guard;
    trans_ctx.self_addr_ = self_addr;
    trans_ctx.merged_version_ = merged_version;

    trans_ctx.phy_plan_ = phy_plan;
  }

  NG_TRACE(transform_begin);
  if (OB_SUCC(ret) && transform_stmt->is_valid_transform_stmt()) {
    ObTransformerImpl transformer(&trans_ctx);
    transformer.set_needed_types(ObTransformRule::ALL_TRANSFORM_RULES);
    if (OB_FAIL(transformer.transform(transform_stmt))) {
      LOG_WARN("failed to transform statement", K(ret));
    } else if (stmt->is_explain_stmt()) {
      static_cast<ObExplainStmt*>(stmt)->set_explain_query_stmt(transform_stmt);
    } else {
      bool or_expansion_happened = false;
      if (OB_FAIL(transformer.get_cost_based_trans_happened(OR_EXPANSION, or_expansion_happened))) {
        LOG_WARN("failed to check whether or_expansion happened", K(ret));
      } else if (or_expansion_happened) {
        if (OB_ISNULL(exec_ctx.get_physical_plan_ctx())) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("get unexpected null", K(exec_ctx.get_physical_plan_ctx()), K(ret));
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
    ObConstraintProcess transformer(exec_ctx.get_allocator(), exec_ctx.get_expr_factory(), exec_ctx.get_my_session());
    if (stmt->is_explain_stmt()) {
      if (OB_ISNULL(transform_stmt = static_cast<ObExplainStmt*>(stmt)->get_explain_query_stmt())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("Explain query stmt is NULL", K(ret));
      } else if (OB_FAIL(transformer.after_transform(transform_stmt, *sql_schema_guard->get_schema_guard()))) {
        LOG_WARN("fail to do after_transform", K(ret));
      } else {
        static_cast<ObExplainStmt*>(stmt)->set_explain_query_stmt(transform_stmt);
      }
    } else {
      if (OB_FAIL(transformer.after_transform(stmt, *sql_schema_guard->get_schema_guard()))) {
        LOG_WARN("fail to do after_transform", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(stmt->distribute_hint_in_query_ctx(&exec_ctx.get_allocator()))) {
      LOG_WARN("Failed to distribute hint in query context", K(ret));
    } else if (OB_ISNULL(exec_ctx.get_sql_ctx()) || OB_ISNULL(exec_ctx.get_sql_ctx()->trans_happened_route_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(phy_plan));
    } else if (OB_FAIL(exec_ctx.get_sql_ctx()->trans_happened_route_->assign(trans_ctx.trans_happened_route_))) {
      LOG_WARN("fail to set transform route", K(ret), K(trans_ctx.trans_happened_route_));
    } else {
      LOG_TRACE("transform happended route", K(trans_ctx.trans_happened_route_));
    }
  }
  transformer_mem_usage = exec_ctx.get_allocator().total() - last_mem_usage;
  LOG_DEBUG("SQL MEM USAGE", K(transformer_mem_usage), K(last_mem_usage));
  NG_TRACE(transform_end);
  return ret;
}

int ObSql::optimize_stmt(
    ObOptimizer& optimizer, const ObSQLSessionInfo& session_info, ObDMLStmt& stmt, ObLogPlan*& logical_plan)
{
  int ret = OB_SUCCESS;
  logical_plan = NULL;
  ObDMLStmt* hint_stmt = &stmt;
  if (stmt.is_explain_stmt()) {
    if (OB_ISNULL(hint_stmt = static_cast<ObExplainStmt&>(stmt).get_explain_query_stmt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("Explain query stmt is NULL", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(hint_stmt->check_and_convert_hint(session_info))) {
    LOG_WARN("Failed to check and convert hint", K(ret));
  } else {
    LOG_DEBUG("stmt to generate plan", K(stmt));
    NG_TRACE(optimize_begin);

    if (OB_FAIL(optimizer.optimize(stmt, logical_plan))) {
      LOG_WARN("Failed to optimize logical plan", K(ret));
      // do nothing(plan will be destructed in result set)
    } else {
      LOG_TRACE("logical plan", KPC(logical_plan));
    }
    NG_TRACE(optimize_end);
  }

  return ret;
}

int ObSql::analyze_table_stat_version(
    share::schema::ObSchemaGetterGuard* schema_guard, ObOptStatManager* opt_stat_manager, ObDMLStmt& stmt)
{
  int ret = OB_ERR_UNEXPECTED;
  ObArray<ObSelectStmt*> child_stmts;
  bool is_stack_overflow = false;
  if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret), K(is_stack_overflow));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  } else if (OB_ISNULL(schema_guard) || OB_ISNULL(opt_stat_manager)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL param", K(schema_guard), K(opt_stat_manager), K(ret));
  } else if (OB_FAIL(stmt.get_child_stmts(child_stmts))) {
    LOG_WARN("Failed to get child stmt", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_table_size(); i++) {
    TableItem* table_item = stmt.get_table_item(i);
    const share::schema::ObTableSchema* table_schema = NULL;
    if (OB_ISNULL(table_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL table item", K(ret));
    } else if (!table_item->is_basic_table()) {
      // do nothing
    } else if (OB_FAIL(schema_guard->get_table_schema(table_item->ref_id_, table_schema))) {
      LOG_WARN("fail to get table schema", K(ret));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table schema is NULL", K(table_item), K(ret));
    } else {
      ObOptTableStatVersion table_stat_version;
      table_stat_version.key_.init(
          table_schema->get_table_id(), table_schema->get_part_level() == PARTITION_LEVEL_ZERO ? 0 : INT64_MAX);
      ObOptTableStat table_stat;
      if (OB_FAIL(opt_stat_manager->get_table_stat(table_stat_version.key_, table_stat))) {
        LOG_WARN("get table stat failed", K(ret));
      } else {
        table_stat_version.stat_version_ = table_stat.get_last_analyzed();
        if (OB_FAIL(stmt.add_table_stat_version(table_stat_version))) {
          LOG_WARN("add table statistic version failed", K(ret));
        }
      }
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); i++) {
    if (OB_ISNULL(child_stmts.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL child stmt", K(i));
    } else if (OB_FAIL(SMART_CALL(analyze_table_stat_version(schema_guard, opt_stat_manager, *child_stmts.at(i))))) {
      LOG_WARN("Failed to analyze table stat version recursively", K(i), K(ret));
    }
  }

  return ret;
}

int ObSql::code_generate(ObSqlCtx& sql_ctx, ObResultSet& result, ObDMLStmt* stmt, ObStmtNeedPrivs& stmt_need_privs,
    ObStmtOraNeedPrivs& stmt_ora_need_privs, ObLogPlan* logical_plan, ObPhysicalPlan*& phy_plan)
{
  int ret = OB_SUCCESS;
  NG_TRACE(cg_begin);
  int64_t last_mem_usage = 0;
  int64_t codegen_mem_usage = 0;
  ObPhysicalPlanCtx* pctx = result.get_exec_context().get_physical_plan_ctx();
  bool use_jit = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(logical_plan) || OB_ISNULL(phy_plan) || OB_ISNULL(sql_ctx.session_info_) ||
      OB_ISNULL(pctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Logical_plan or phy_plan is NULL",
        K(ret),
        K(stmt),
        K(logical_plan),
        K(phy_plan),
        "session",
        sql_ctx.session_info_);
  } else {
    ObCodeGenerator code_generator(use_jit,
        sql_ctx.session_info_->use_static_typing_engine(),
        result.get_exec_context().get_min_cluster_version(),
        &(pctx->get_datum_param_store()));
    if (OB_FAIL(code_generator.generate(*logical_plan, *phy_plan))) {
      LOG_WARN("Failed to generate physical plan", KPC(logical_plan), K(ret));
    } else {
      sql_ctx.session_info_->set_ignore_stmt(false);
      result.set_physical_plan(PLAN_GEN_HANDLE, phy_plan);
      LOG_DEBUG("phy plan", K(*phy_plan));
      phy_plan->set_returning(stmt->is_returning());
      phy_plan->set_has_link_table(stmt->has_link_table());
      last_mem_usage = phy_plan->get_mem_size();
    }
  }
  NG_TRACE(cg_end);

  // set phy table location in task_exec_ctx, query_timeout in exec_context
  if (OB_SUCC(ret)) {
    ObQueryHint query_hint = stmt->get_stmt_hint().get_query_hint();
    phy_plan->set_query_hint(query_hint);
    // set larger query_time for IS
    bool has_is_table = false;
    if (OB_FAIL(stmt->has_is_table(has_is_table))) {
      LOG_WARN("check stmt whether has is table failed", K(ret));
    } else if (has_is_table) {
      int tmp_ret = OB_SUCCESS;
      if (query_hint.query_timeout_ <= 0) {
        if (OB_SUCCESS != (tmp_ret = sql_ctx.session_info_->get_query_timeout(query_hint.query_timeout_))) {
          LOG_WARN("failed to get sys variable value", K(tmp_ret));
        }
      }
      query_hint.query_timeout_ *= 10;
    } else {
    }  // do nothing
    ObSEArray<ObTablePartitionInfo*, 3> tbl_part_infos;
    phy_plan->set_query_hint(query_hint);  // remember in phy_plan
    // set task_exec_ctx
    ObTaskExecutorCtx* task_exec_ctx = result.get_exec_context().get_task_executor_ctx();
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(task_exec_ctx)) {
      ret = OB_NOT_INIT;
      LOG_WARN("task_exec_ctx not inited", K(ret));
    } else {
      if (OB_FAIL(logical_plan->get_global_table_partition_info(tbl_part_infos))) {
        LOG_WARN("get_global_table_partition_info fails", K(ret));
      } else if (OB_FAIL(sql_ctx.set_partition_infos(tbl_part_infos, result.get_exec_context().get_allocator()))) {
        LOG_WARN("Failed to set table location in sql ctx", K(ret));
      } else if (OB_FAIL(task_exec_ctx->set_table_locations(tbl_part_infos))) {
        LOG_WARN("Failed to set table location in take exec ctx", K(ret));
      } else {
      }  // do nothing
    }
    // set table location for phy_plan
    if (OB_SUCC(ret)) {
      if (OB_FAIL(phy_plan->set_table_locations(tbl_part_infos))) {
        LOG_WARN("fail to set table locations", K(ret));
      }
      LOG_DEBUG("physical plan certain table location", K(tbl_part_infos));
    }
  }
  // set asc index info
  if (OB_SUCC(ret)) {
    if (OB_FAIL(sql_ctx.set_acs_index_info(
            logical_plan->get_acs_index_info(), result.get_exec_context().get_allocator()))) {
      LOG_WARN("failed to set acd index info", K(ret));
    } else {
      LOG_DEBUG("succeed to set acs index info", K(logical_plan->get_acs_index_info()), K(ret));
    }
  }

  // set multi stmt info
  if (OB_SUCC(ret)) {
    if (OB_FAIL(sql_ctx.set_multi_stmt_rowkey_pos(
            logical_plan->get_multi_stmt_rowkey_pos(), result.get_exec_context().get_allocator()))) {
      LOG_WARN("failed to set multi stmt rowkey pos", K(ret));
    } else {
      LOG_DEBUG("succeed to set multi stmt rowkey pos", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    bool use_plan_cache = sql_ctx.session_info_->get_local_ob_enable_plan_cache();
    ObPlanCache* plan_cache = NULL;
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
    }
  }

  LOG_DEBUG("SQL MEM USAGE", K(codegen_mem_usage), K(last_mem_usage));
  return ret;
}

inline int ObSql::sanity_check(ObSqlCtx& context)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    LOG_WARN("ob sql not inited");
  } else if (OB_UNLIKELY(NULL == context.session_info_) || OB_UNLIKELY(NULL == context.schema_guard_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN(
        "invalid argument", K(ret), "session info", context.session_info_, "schema manager", context.schema_guard_);
  } else {
    // do nothing
  }
  return ret;
}

inline int ObSql::init_result_set(ObSqlCtx& context, ObResultSet& result_set)
{
  int ret = OB_SUCCESS;
  ObVirtualTableCtx vt_ctx;

  if (OB_ISNULL(GCTX.rs_rpc_proxy_) || OB_ISNULL(GCTX.executor_rpc_) || OB_ISNULL(GCTX.srv_rpc_proxy_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("rs_mgr_ should not be NULL", K(ret));
  } else {
    vt_ctx.vt_iter_factory_ = context.vt_iter_factory_;
    vt_ctx.session_ = context.session_info_;
    vt_ctx.schema_guard_ = context.schema_guard_;
    vt_ctx.partition_table_operator_ = context.partition_table_operator_;

    if (OB_FAIL(init_exec_context(context,
            *GCTX.executor_rpc_,
            *GCTX.rs_rpc_proxy_,
            *GCTX.srv_rpc_proxy_,
            vt_ctx,
            result_set.get_exec_context()))) {
      LOG_WARN("Failed to init exec_context", K(ret));
    } else {
    }  // do nothing
  }
  return ret;
}

inline int ObSql::init_exec_context(const ObSqlCtx& context, ObExecutorRpcImpl& exec_rpc,
    obrpc::ObCommonRpcProxy& rs_proxy, obrpc::ObSrvRpcProxy& srv_proxy, const ObVirtualTableCtx& vt_ctx,
    ObExecContext& exec_ctx)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx* task_exec_ctx = NULL;
  if (OB_UNLIKELY(OB_INVALID_COUNT == context.retry_times_) || OB_ISNULL(context.partition_location_cache_) ||
      OB_ISNULL(transport_) || OB_ISNULL(partition_service_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("variable is not inited",
        K(ret),
        K(context.retry_times_),
        K(context.partition_location_cache_),
        K(transport_),
        K(partition_service_));
  } else if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(exec_ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task exec ctx is NULL", K(ret));
  } else if (OB_ISNULL(GCTX.sql_engine_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql engine is null in GCTX", K(ret));
  } else {
    task_exec_ctx->set_retry_times(context.retry_times_);
    task_exec_ctx->set_partition_location_cache(context.partition_location_cache_);
    task_exec_ctx->set_task_executor_rpc(exec_rpc);
    task_exec_ctx->set_rs_rpc(rs_proxy);
    task_exec_ctx->set_srv_rpc(srv_proxy);
    task_exec_ctx->set_partition_service(partition_service_);
    task_exec_ctx->set_vt_partition_service(vt_partition_service_);
    task_exec_ctx->set_self_addr(self_addr_);
    static_cast<ObSqlPartitionLocationCache*>(context.partition_location_cache_)->set_task_exec_ctx(task_exec_ctx);
    if (OB_FAIL(exec_ctx.create_physical_plan_ctx())) {
      LOG_WARN("faile to create physical plan ctx", K(ret));
    } else if (OB_ISNULL(exec_ctx.get_physical_plan_ctx())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("phy plan ctx is NULL", K(ret));
    } else {
      exec_ctx.set_addr(self_addr_);
      exec_ctx.set_my_session(context.session_info_);
      exec_ctx.set_sql_proxy(context.sql_proxy_);
      exec_ctx.set_virtual_table_ctx(vt_ctx);
      exec_ctx.set_plan_cache_manager(&plan_cache_manager_);
      exec_ctx.set_session_mgr(context.session_mgr_);
      exec_ctx.set_sql_ctx(const_cast<ObSqlCtx*>(&context));
      exec_ctx.get_physical_plan_ctx()->set_large_query(false);
    }
  }
  return ret;
}

ObPlanCache* ObSql::get_plan_cache(uint64_t tenant_id, const ObPCMemPctConf& pc_mem_conf)
{
  return plan_cache_manager_.get_or_create_plan_cache(tenant_id, pc_mem_conf);
}

ObPsCache* ObSql::get_ps_cache(const uint64_t tenant_id, const ObPCMemPctConf& pc_mem_conf)
{
  return plan_cache_manager_.get_or_create_ps_cache(tenant_id, pc_mem_conf);
}

int ObSql::revert_plan_cache(uint64_t tenant_id)
{
  return plan_cache_manager_.revert_plan_cache(tenant_id);
}

int ObSql::replace_stmt_bool_filter(ObExecContext& ctx, ObDMLStmt* stmt)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  ObSQLSessionInfo* session = NULL;
  bool is_stack_overflow = false;
  if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret), K(is_stack_overflow));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  } else if (OB_ISNULL(stmt) || OB_ISNULL(plan_ctx = ctx.get_physical_plan_ctx()) ||
             OB_ISNULL(session = ctx.get_my_session())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected null", K(stmt), K(plan_ctx), K(session), K(ret));
  } else {
    ParamStore& params = plan_ctx->get_param_store_for_update();
    // traverse condition expr
    for (int64_t idx = 0; OB_SUCC(ret) && idx < stmt->get_condition_size(); ++idx) {
      if (OB_FAIL(replace_expr_bool_filter(session, stmt->get_condition_expr(idx), params))) {
        LOG_WARN("Failed to replace expr bool filter", K(ret));
      }
    }
    // traverse joined_table join_condition
    if (stmt->is_dml_stmt()) {
      ObDMLStmt* dml_stmt = static_cast<ObDMLStmt*>(stmt);
      ObIArray<JoinedTable*>& joined_tables = dml_stmt->get_joined_tables();
      JoinedTable* joined_table = NULL;
      for (int64_t idx = 0; OB_SUCC(ret) && idx < joined_tables.count(); ++idx) {
        if (OB_ISNULL(joined_table = joined_tables.at(idx))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Joined table in joined_tables should not be NULL", K(ret));
        } else if (OB_FAIL(replace_joined_table_bool_filter(session, *joined_table, params))) {
          LOG_WARN("failed to replace joined tables bool filter", K(ret));
        } else { /*do nothing*/
        }
      }
    }
    // traverse having filter
    if (stmt->is_select_stmt()) {
      ObSelectStmt* select_stmt = static_cast<ObSelectStmt*>(stmt);
      ObIArray<ObRawExpr*>& having_exprs = select_stmt->get_having_exprs();
      for (int64_t idx = 0; OB_SUCC(ret) && idx < having_exprs.count(); ++idx) {
        if (OB_FAIL(replace_expr_bool_filter(session, having_exprs.at(idx), params))) {
          LOG_WARN("Failed to replace having expr bool filter", K(ret));
        }
      }
    }
    // traverse child stmt
    if (OB_SUCC(ret)) {
      ObArray<ObSelectStmt*> child_stmts;
      if (OB_FAIL(stmt->get_child_stmts(child_stmts))) {
        LOG_WARN("get child stmts failed", K(ret));
      }
      for (int64_t idx = 0; OB_SUCC(ret) && idx < child_stmts.count(); ++idx) {
        if (OB_FAIL(SMART_CALL(replace_stmt_bool_filter(ctx, child_stmts.at(idx))))) {
          LOG_WARN("Failed to replace stmt bool filter", K(ret));
        }
      }
    }
  }
  return OB_SUCCESS;
}

int ObSql::replace_joined_table_bool_filter(ObSQLSessionInfo* session, JoinedTable& join_table, ParamStore& params)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret), K(is_stack_overflow));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  } else if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null session info", K(ret));
  } else { /*do nothing*/
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < join_table.join_conditions_.count(); ++i) {
    if (OB_FAIL(replace_expr_bool_filter(session, join_table.join_conditions_.at(i), params))) {
      LOG_WARN("failed to replace expr bool filter", K(ret));
    } else { /*do nothing*/
    }
  }

  if (OB_SUCC(ret)) {
    if (NULL != join_table.left_table_ && join_table.left_table_->is_joined_table()) {
      JoinedTable& left_table = static_cast<JoinedTable&>(*join_table.left_table_);
      if (OB_FAIL(SMART_CALL(replace_joined_table_bool_filter(session, left_table, params)))) {
        LOG_WARN("failed to replace joined table bool filter", K(ret));
      }
    }
    if (OB_SUCC(ret) && join_table.right_table_ != NULL && join_table.right_table_->is_joined_table()) {
      JoinedTable& right_table = static_cast<JoinedTable&>(*join_table.right_table_);
      if (OB_FAIL(SMART_CALL(replace_joined_table_bool_filter(session, right_table, params)))) {
        LOG_WARN("failed to replace joined table bool filter", K(ret));
      }
    }
  }
  return ret;
}

int ObSql::replace_expr_bool_filter(ObSQLSessionInfo* session, ObRawExpr* expr, ParamStore& params)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret), K(is_stack_overflow));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  } else if (OB_ISNULL(expr) || OB_ISNULL(session)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected null", K(expr), K(ret));
  } else if (expr->is_const_expr()) {
    ObConstRawExpr* const_expr = static_cast<ObConstRawExpr*>(expr);
    bool is_true = true;
    if (T_QUESTIONMARK == const_expr->get_expr_type()) {
      int64_t param_idx = -1;
      if (OB_FAIL(const_expr->get_value().get_unknown(param_idx))) {
        LOG_WARN("Failed to get param", K(ret));
      } else if (param_idx < 0 || param_idx >= params.count()) {
        ret = OB_ERR_ILLEGAL_INDEX;
        LOG_WARN("Wrong index of question mark position", K(ret), K(param_idx));
      } else if (OB_FAIL(ObObjEvaluator::is_true(params.at(param_idx), is_true))) {
        LOG_WARN("Get param value error", K(ret));
      } else {
        params.at(param_idx).set_need_to_check_bool_value(true);
        params.at(param_idx).set_expected_bool_value(is_true);
      }
    } else {
      if (OB_FAIL(ObObjEvaluator::is_true(const_expr->get_value(), is_true))) {
        LOG_WARN("Failed to get whether is true", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      const_expr->reset();
      const_expr->set_expr_type(T_BOOL);
      const_expr->get_value().set_bool(is_true);
      const_expr->set_expr_obj_meta(const_expr->get_value().get_meta());
      if (OB_FAIL(const_expr->formalize(session))) {
        LOG_WARN("failed to formalize expr", K(ret));
      } else { /*do nothing*/
      }
    }
  } else if (T_OP_AND == expr->get_expr_type() || T_OP_OR == expr->get_expr_type()) {
    ObOpRawExpr* op_expr = static_cast<ObOpRawExpr*>(expr);
    for (int64_t idx = 0; OB_SUCC(ret) && idx < op_expr->get_param_count(); ++idx) {
      if (OB_FAIL(SMART_CALL(replace_expr_bool_filter(session, op_expr->get_param_expr(idx), params)))) {
        LOG_WARN("Failed to replace sub_expr bool filter", K(ret));
      }
    }
  } else {
    // other expr type, do nothing
  }

  return ret;
}

int ObSql::transform_stmt_with_outline(
    ObPlanCacheCtx& pc_ctx, ObOutlineState& outline_state, common::ObString& outlined_stmt)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo* session = pc_ctx.sql_ctx_.session_info_;
  if (OB_UNLIKELY(OB_INVALID_ID == pc_ctx.bl_key_.tenant_id_ || OB_INVALID_ID == pc_ctx.bl_key_.db_id_ ||
                  pc_ctx.raw_sql_.empty() || (NULL == session))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments",
        K(pc_ctx.bl_key_.tenant_id_),
        K(pc_ctx.bl_key_.db_id_),
        K(pc_ctx.raw_sql_),
        KP(session),
        K(ret));
  } else {
    const ObOutlineInfo* outline_info = NULL;
    outline_state.reset();
    ObString outline_key;

    bool has_question_mark = false;  // not used here
    bool outline_with_sql_id = false;
    if (!pc_ctx.is_ps_mode_) {
      // create a memory context to temporarily replace allocator
      lib::ContextParam param;
      param.set_mem_attr(pc_ctx.bl_key_.tenant_id_, ObModIds::OB_SQL_EXECUTOR, ObCtxIds::DEFAULT_CTX_ID)
          .set_properties(lib::USE_TL_PAGE_OPTIONAL)
          .set_page_size(OB_MALLOC_BIG_BLOCK_SIZE);
      // Here we need to get outline info with parameterized sql
      // in order to do that, we need to fast parse sql, transform syntax tree, construct paramed sql
      // outline info is the only thing we need here, all the memories used can be droped after outline info is
      // retrived.
      // So we use a memory context to do that
      CREATE_WITH_TEMP_CONTEXT(param)
      {
        ObIAllocator& tmp_allocator = CURRENT_CONTEXT.get_arena_allocator();
        ObMaxConcurrentParam::FixParamStore fix_param_store(
            OB_MALLOC_NORMAL_BLOCK_SIZE, ObWrapperAllocator(&tmp_allocator));
        if (OB_FAIL(ObSQLUtils::get_outline_key(
                tmp_allocator, session, pc_ctx.raw_sql_, outline_key, fix_param_store, FP_MODE, has_question_mark))) {
          LOG_DEBUG("fail to get outline key", K(ret));
        } else if (OB_FAIL(get_outline_info(pc_ctx, outline_key, outline_info, outline_with_sql_id))) {
          LOG_WARN("fail to get outline info", K(ret));
        } else {
          // do nothing
        }
      }
    } else if (OB_FAIL(get_outline_info(pc_ctx, pc_ctx.raw_sql_, outline_info, outline_with_sql_id))) {
      LOG_WARN("failed to get outline info", K(ret));
    }

    // if has no outline, use raw sql to query
    if (OB_SUCC(ret) && NULL == outline_info) {
      outlined_stmt = pc_ctx.raw_sql_;
    } else if (OB_SUCC(ret) && NULL != outline_info) {  // outline binding
      const ObString& outline_content = outline_info->get_outline_content_str();
      const ObString& outline_target = outline_info->get_outline_target_str();
      bool has_concurrent_limit = false;
      if (OB_FAIL(outline_info->get_outline_params_wrapper().has_concurrent_limit_param(has_concurrent_limit))) {
        LOG_WARN("fail to check has_concurrent_limit_param", K(ret));
      }
      if (OB_SUCC(ret)) {
        if (has_concurrent_limit && !outline_info->get_sql_id_str().empty()) {
          outlined_stmt = pc_ctx.raw_sql_;
        } else if (!outline_content.empty()) {
          bool is_need_filter_hint = !outline_target.empty() || outline_with_sql_id;
          if (OB_FAIL(ObSQLUtils::construct_outline_sql(
                  pc_ctx.allocator_, *session, outline_content, pc_ctx.raw_sql_, is_need_filter_hint, outlined_stmt))) {
            LOG_WARN("fail to construct outline sql", K(ret), K(outline_content), K(pc_ctx.raw_sql_));
          } else {
            outline_state.is_plan_fixed_ = true;
          }
        } else {
          outlined_stmt = pc_ctx.raw_sql_;
        }
      }

      if (OB_SUCC(ret)) {
        outline_state.outline_version_.object_id_ = outline_info->get_outline_id();
        outline_state.outline_version_.version_ = outline_info->get_schema_version();
        outline_state.outline_version_.object_type_ = DEPENDENCY_OUTLINE;
        if (outline_info->has_outline_params()) {
          pc_ctx.exec_ctx_.set_outline_params_wrapper(&outline_info->get_outline_params_wrapper());
        }
      }
    }
  }

  return ret;
}

int ObSql::get_outline_info(
    ObPlanCacheCtx& pc_ctx, const ObString& outline_key, const ObOutlineInfo*& outline_info, bool& outline_with_sql_id)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard* schema_guard = pc_ctx.sql_ctx_.schema_guard_;
  ObString sql_key;
  int64_t pos = 0;
  char sql_id_buf[OB_MAX_SQL_ID_LENGTH + 1];
  sql_id_buf[0] = '\0';
  outline_with_sql_id = false;
  if (OB_ISNULL(schema_guard)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(schema_guard), K(ret));
  } else if (pc_ctx.is_ps_mode_) {
    int64_t sql_buf_size = pc_ctx.raw_sql_.get_serialize_size();
    int64_t sql_pos = 0;
    char* sql_buf = NULL;
    ObIAllocator& allocator = CURRENT_CONTEXT.get_arena_allocator();
    if (OB_ISNULL(sql_buf = (char*)allocator.alloc(sql_buf_size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret));
    } else if (OB_FAIL(pc_ctx.raw_sql_.serialize(sql_buf, sql_buf_size, sql_pos))) {
      LOG_WARN("failed to serialize sql", K(ret));
    } else {
      ObString outline_sql_str(sql_pos, sql_buf);
      if (OB_FAIL(schema_guard->get_outline_info_with_signature(
              pc_ctx.bl_key_.tenant_id_, pc_ctx.bl_key_.db_id_, outline_sql_str, outline_info))) {
        LOG_WARN("failed to get outline info", K(ret));
      } else if (NULL != outline_info) {
        LOG_DEBUG("get outline info with sql key", K(*outline_info));
        outline_with_sql_id = true;
      }
    }
    if (OB_SUCC(ret) && NULL == outline_info) {
      // using sql id for querying outline info
      (void)ObSQLUtils::md5(pc_ctx.raw_sql_, sql_id_buf, (int32_t)sizeof(sql_id_buf));
      sql_id_buf[OB_MAX_SQL_ID_LENGTH] = '\0';
      ObString sql_id = ObString::make_string(sql_id_buf);
      if (OB_FAIL(schema_guard->get_outline_info_with_sql_id(
              pc_ctx.bl_key_.tenant_id_, pc_ctx.bl_key_.db_id_, sql_id, outline_info))) {
        LOG_WARN("failed to get outline info", K(ret));
      } else if (NULL != outline_info) {
        outline_with_sql_id = true;
      }
    }
  } else if (OB_FAIL(schema_guard->get_outline_info_with_signature(
                 pc_ctx.bl_key_.tenant_id_, pc_ctx.bl_key_.db_id_, outline_key, outline_info))) {
    LOG_WARN("failed to get outline info", K(pc_ctx.bl_key_.tenant_id_), K(ret));
  } else if (OB_LIKELY(NULL == outline_info)) {
    if (OB_FAIL(sql_key.deserialize(outline_key.ptr(), outline_key.length(), pos))) {
      LOG_WARN("fail to deserialize outline_key", K(ret));
    } else {
      (void)ObSQLUtils::md5(sql_key, sql_id_buf, (int32_t)sizeof(sql_id_buf));
      sql_id_buf[OB_MAX_SQL_ID_LENGTH] = '\0';
      ObString sql_id = ObString::make_string(sql_id_buf);
      if (OB_FAIL(schema_guard->get_outline_info_with_sql_id(
              pc_ctx.bl_key_.tenant_id_, pc_ctx.bl_key_.db_id_, sql_id, outline_info))) {
        LOG_WARN("failed to get outline info", K(pc_ctx.bl_key_.tenant_id_), K(ret));
      } else if (NULL != outline_info) {
        outline_with_sql_id = true;
      }
    }
  }

  return ret;
}

int ObSql::execute_get_plan(ObPlanCache& plan_cache, ObPlanCacheCtx& pc_ctx, ObPhysicalPlan*& plan)
{
  int ret = OB_SUCCESS;
  ObIAllocator& allocator = pc_ctx.allocator_;
  ObSQLSessionInfo* session = pc_ctx.sql_ctx_.session_info_;
  if (NULL == session) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else if (pc_ctx.is_ps_mode_) {
    ObPsStmtId stmt_id = pc_ctx.fp_result_.pc_key_.key_id_;
    if (OB_FAIL(plan_cache.get_ps_plan(PS_EXEC_HANDLE, stmt_id, pc_ctx, plan))) {
      if (OB_SQL_PC_NOT_EXIST == ret || OB_PC_LOCK_CONFLICT == ret) {
        LOG_DEBUG("fail to get ps physical plan", K(ret));
      } else {
        LOG_WARN("fail to get ps physical plan", K(ret));
      }
    }
  } else {
    if (OB_FAIL(plan_cache.get_plan(CLI_QUERY_HANDLE, allocator, pc_ctx, plan))) {
      if (OB_SQL_PC_NOT_EXIST == ret || OB_PC_LOCK_CONFLICT == ret) {
        LOG_DEBUG("fail to get physical plan", K(ret));
      } else {
        LOG_WARN("fail to get physical plan", K(ret));
      }
    }
    if (OB_SQL_PC_NOT_EXIST == ret && session->use_static_typing_engine()) {
      ret = OB_SUCCESS;
      session->set_use_static_typing_engine(false);
      ObPhysicalPlanCtx* pctx = pc_ctx.exec_ctx_.get_physical_plan_ctx();
      if (OB_ISNULL(pctx)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(pctx), K(ret));
      } else {
        pctx->reset_datum_param_store();
        pctx->get_param_store_for_update().reuse();
        pc_ctx.fp_result_.reset();
        pc_ctx.multi_stmt_fp_results_.reset();
      }
      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_FAIL(plan_cache.get_plan(CLI_QUERY_HANDLE, allocator, pc_ctx, plan))) {
        if (OB_SQL_PC_NOT_EXIST == ret || OB_PC_LOCK_CONFLICT == ret) {
          LOG_DEBUG("fail to get physical plan", K(ret));
        } else {
          LOG_WARN("fail to get physical plan", K(ret));
        }
        session->set_use_static_typing_engine(true);
      }
    }
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (pc_ctx.sql_ctx_.can_reroute_sql_ && OB_PHY_PLAN_REMOTE == plan->get_plan_type()) {
    // reroute request,
    // physical table location is already calculated and stored in task_exec_ctx.table_locations_
    bool should_reroute = true;
    const DependenyTableStore& dep_tables = plan->get_dependency_table();
    for (int64_t i = 0; should_reroute && i < dep_tables.count(); i++) {
      const ObSchemaObjVersion& schema_obj = dep_tables.at(i);
      if (TABLE_SCHEMA == schema_obj.get_schema_type() && is_virtual_table(extract_pure_id(schema_obj.object_id_))) {
        should_reroute = false;
      }
    }
    if (OB_ISNULL(pc_ctx.sql_ctx_.schema_guard_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid null schema guard", K(ret));
    } else if (should_reroute) {
      ObTaskExecutorCtx* task_exec_ctx = pc_ctx.exec_ctx_.get_task_executor_ctx();
      if (OB_ISNULL(task_exec_ctx)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid null task exec ctx", K(ret));
      } else if (task_exec_ctx->get_table_locations().count() <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null phy table location", K(ret));
      } else {
        const ObTableSchema* table_schema = NULL;
        const ObPhyTableLocation& phy_table_loc = task_exec_ctx->get_table_locations().at(0);
        const ObPartitionReplicaLocation& phy_part_loc = phy_table_loc.get_partition_location_list().at(0);
        const ObReplicaLocation& replica_loc = phy_part_loc.get_replica_location();

        if (OB_FAIL(pc_ctx.sql_ctx_.schema_guard_->get_table_schema(phy_table_loc.get_ref_table_id(), table_schema))) {
          LOG_WARN("failed to get table schema", K(ret));
        } else if (OB_ISNULL(table_schema)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid null table schema", K(ret));
        } else {
          pc_ctx.sql_ctx_.reroute_info_.server_.set_ipv4_addr(
              replica_loc.server_.get_ipv4(), static_cast<int32_t>(replica_loc.sql_port_));
          pc_ctx.sql_ctx_.reroute_info_.role_ = replica_loc.role_;
          pc_ctx.sql_ctx_.reroute_info_.replica_type_ = replica_loc.replica_type_;
          pc_ctx.sql_ctx_.reroute_info_.set_tbl_name(table_schema->get_table_name());
          pc_ctx.sql_ctx_.reroute_info_.tbl_schema_version_ = table_schema->get_schema_version();
          LOG_DEBUG("hit plan, but reroute sql", K(pc_ctx.sql_ctx_.reroute_info_));
          ret = OB_ERR_PROXY_REROUTE;
        }
      }
    }
  }
  return ret;
}

int ObSql::pc_get_plan_and_fill_result(
    ObPlanCacheCtx& pc_ctx, ObResultSet& result, int& get_plan_err, bool& need_disconnect)
{
  UNUSED(need_disconnect);
  int ret = OB_SUCCESS;
  ObPhysicalPlan* plan = NULL;
  ObExecContext& exec_ctx = result.get_exec_context();
  if (OB_FAIL(pc_get_plan(pc_ctx, plan, get_plan_err, exec_ctx.get_need_disconnect_for_update()))) {
    if (NULL != plan) {
      ObCacheObjectFactory::free(plan, pc_ctx.handle_id_);
    }
    LOG_DEBUG("fail to get plan", K(ret));
  } else if (OB_SUCCESS != get_plan_err) {
    // get plan from plan cache failed
  } else if (OB_ISNULL(plan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("plan is null", K(ret));
  } else {
    result.set_is_from_plan_cache(true);
    result.set_physical_plan(pc_ctx.handle_id_, plan);
    if (OB_FAIL(result.from_plan(*plan, pc_ctx.fp_result_.raw_params_))) {
      LOG_WARN("fail to set plan info to ResultSet", K(ret));
    }
  }
  return ret;
}

int ObSql::pc_get_plan(ObPlanCacheCtx& pc_ctx, ObPhysicalPlan*& plan, int& get_plan_err, bool& need_disconnect)
{
  int ret = OB_SUCCESS;
  NG_TRACE(cache_get_plan_begin);
  ObPlanCache* plan_cache = NULL;
  ObSQLSessionInfo* session = pc_ctx.sql_ctx_.session_info_;
  if (OB_ISNULL(session) || OB_ISNULL(plan_cache = session->get_plan_cache())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid plan cache", K(ret), K(session), K(plan_cache));
  } else if (OB_FAIL(execute_get_plan(*plan_cache, pc_ctx, plan))) {
    if (OB_EAGAIN == ret || OB_REACH_MAX_CONCURRENT_NUM == ret || OB_ARRAY_BINDING_ROLLBACK == ret ||
        OB_ERR_PROXY_REROUTE == ret || OB_BATCHED_MULTI_STMT_ROLLBACK == ret) {
      /*do nothing*/
    } else if (!pc_ctx.is_ps_mode_ && OB_PC_LOCK_CONFLICT == ret && !session->is_inner()) {
    } else {
      get_plan_err = ret;
      ret = OB_SUCCESS;
    }
  } else {
    plan_cache->inc_hit_and_access_cnt();
    if (OB_ISNULL(plan)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get plan cache");
    } else {
      need_disconnect = false;
      // FIXME  NG_TRACE_EXT(set_need_disconnect, OB_ID(need_disconnect), false);
      pc_ctx.sql_ctx_.plan_cache_hit_ = true;
      session->set_early_lock_release(plan->stat_.enable_early_lock_release_);
      if (!session->has_user_super_privilege() && !pc_ctx.sql_ctx_.is_remote_sql_) {
        // we don't care about commit or rollback here because they will not cache in plan cache
        if (ObStmt::is_write_stmt(plan->get_stmt_type(), false) &&
            OB_FAIL(pc_ctx.sql_ctx_.schema_guard_->verify_read_only(
                session->get_effective_tenant_id(), plan->get_stmt_need_privs()))) {
          LOG_WARN("database or table is read only, cannot execute this stmt");
        }
      }
      if (OB_SUCC(ret) && !pc_ctx.sql_ctx_.is_remote_sql_) {
        if (!ObSchemaChecker::is_ora_priv_check()) {
          if (OB_FAIL(ObPrivilegeCheck::check_privilege(pc_ctx.sql_ctx_, plan->get_stmt_need_privs()))) {
            LOG_WARN("No privilege", K(ret), "stmt_need_priv", plan->get_stmt_need_privs());
          } else {
            LOG_DEBUG("cached phy plan", K(*plan));
            NG_TRACE(check_priv);
          }
        } else if (OB_FAIL(ObPrivilegeCheck::check_ora_privilege(pc_ctx.sql_ctx_, plan->get_stmt_ora_need_privs()))) {
          LOG_WARN("No privilege", K(ret), "stmt_need_priv", plan->get_stmt_ora_need_privs());
        } else {
          LOG_DEBUG("cached phy plan", K(*plan));
          NG_TRACE(check_priv);
        }
        if (OB_SUCC(ret) && OB_FAIL(ObPrivilegeCheck::check_password_expired(pc_ctx.sql_ctx_, stmt::T_NONE))) {
          LOG_WARN("Falied to check password expired", K(ret));
        }
      }
    }
  }
  if (OB_ERR_PROXY_REROUTE == ret) {
    need_disconnect = false;
  }
  NG_TRACE(cache_get_plan_end);
  return ret;
}

int ObSql::rewrite_query_sql(ObPlanCacheCtx& pc_ctx, ObOutlineState& outline_state, common::ObString& outlined_stmt)
{
  NG_TRACE(transform_with_outline_begin);
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard* schema_guard = pc_ctx.sql_ctx_.schema_guard_;
  const uint64_t tenant_id = pc_ctx.bl_key_.tenant_id_;
  int64_t schema_version = OB_INVALID_VERSION;
  if (OB_ISNULL(schema_guard)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(schema_guard), K(ret));
  } else if (OB_FAIL(schema_guard->get_schema_version(tenant_id, schema_version))) {
    LOG_WARN("fail to get schema version", K(ret), K(tenant_id));
  } else if (OB_CORE_SCHEMA_VERSION >= schema_version) {
    outlined_stmt = pc_ctx.raw_sql_;
  } else if (OB_FAIL(transform_stmt_with_outline(pc_ctx, outline_state, outlined_stmt))) {
    if (OB_SQL_DML_ONLY == ret || OB_NOT_SUPPORTED == ret) {
      ret = OB_SUCCESS;
    } else {
      ret = OB_SUCCESS;
      LOG_WARN("failed to transform_stmt_with_outline", K(pc_ctx.bl_key_.tenant_id_), K(pc_ctx.raw_sql_), K(ret));
    }
    outlined_stmt = pc_ctx.raw_sql_;
  }
  NG_TRACE(transform_with_outline_end);
  return ret;
}

int ObSql::parser_and_check(const ObString& outlined_stmt, ObExecContext& exec_ctx, ObPlanCacheCtx& pc_ctx,
    ParseResult& parse_result, int get_plan_err, bool& add_plan_to_pc, bool& is_enable_transform_tree)
{
  int ret = OB_SUCCESS;
  ObIAllocator& allocator = pc_ctx.allocator_;
  ObSQLSessionInfo* session = exec_ctx.get_my_session();

  ObPhysicalPlanCtx* pctx = exec_ctx.get_physical_plan_ctx();
  bool is_stack_overflow = false;
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
    ObParser parser(allocator, session->get_sql_mode(), session->get_local_collation_connection());
    if (OB_FAIL(parser.parse(outlined_stmt, parse_result, STD_MODE, pc_ctx.sql_ctx_.handle_batched_multi_stmt()))) {
      LOG_WARN("Generate syntax tree failed", K(outlined_stmt), K(ret));
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(parse_result.result_tree_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("parse result tree is NULL", K(ret));
      } else if (OB_UNLIKELY(parse_result.result_tree_->num_child_ < 1)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("child count of parse result tree is less than 1", K(ret), K(parse_result.result_tree_->num_child_));
      } else if (OB_ISNULL(parse_result.result_tree_->children_[0])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("children_[0] ofparse result tree is NULL", K(ret), K(parse_result.result_tree_->num_child_));
      } else {
        ObItemType parse_stmt_type = parse_result.result_tree_->children_[0]->type_;
        if (T_COMMIT == parse_stmt_type || T_ROLLBACK == parse_stmt_type) {
        } else {
          exec_ctx.set_need_disconnect(false);
          // FIXME  NG_TRACE_EXT(set_need_disconnect, OB_ID(need_disconnect), false);
        }
      }
    } else if (OB_LIKELY(OB_ERR_PARSE_SQL == ret || OB_ERR_EMPTY_QUERY == ret || OB_SIZE_OVERFLOW == ret ||
                         OB_ERR_ILLEGAL_NAME == ret)) {
      exec_ctx.set_need_disconnect(false);
      // FIXME  NG_TRACE_EXT(set_need_disconnect, OB_ID(need_disconnect), false);
    } else {
      LOG_WARN("parser error number is unexpected, need disconnect", K(ret));
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(parse_result.result_tree_)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid args", K(ret), KP(parse_result.result_tree_));
      } else if (OB_ISNULL(parse_result.result_tree_->children_) ||
                 OB_UNLIKELY(parse_result.result_tree_->num_child_ < 1)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid args",
            K(ret),
            KP(parse_result.result_tree_->children_),
            "number of children",
            parse_result.result_tree_->num_child_);
      } else {
        ParseNode* children_node = parse_result.result_tree_->children_[0];
        if (OB_ISNULL(children_node)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid args", K(ret), KP(children_node));
        } else if ((children_node->type_ == T_EXPLAIN || IS_DML_STMT(children_node->type_)) &&
                   (children_node->value_ > 0)) {
          ret = OB_ERR_PARSE_SQL;  // children_node->value_ > 0, Indicating that there is question_mark
          const char* err_msg = "?";
          int32_t str_len = static_cast<int32_t>(strlen(err_msg));
          int32_t line_no = 1;
          LOG_USER_ERROR(OB_ERR_PARSE_SQL, ob_errpkt_strerror(OB_ERR_PARSER_SYNTAX, false), str_len, err_msg, line_no);
          LOG_WARN("the text query is invalid", K(outlined_stmt), K(children_node->value_), K(ret));
        } else {
          ObItemType type = children_node->type_;
          ObPlanCache* plan_cache = NULL;
          if (IS_DML_STMT(type)) {
            if (OB_UNLIKELY(NULL == (plan_cache = session->get_plan_cache()))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("Invalid plan cache", K(ret));
            } else {
              plan_cache->inc_access_cnt();
              if (OB_SQL_PC_NOT_EXIST == get_plan_err) {
                add_plan_to_pc = true;
              } else {
                add_plan_to_pc = false;
              }
            }
          } else if (T_EXPLAIN != type) {
            session->set_use_static_typing_engine(false);
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    const uint64_t tenant_id = session->get_effective_tenant_id();
    ObSqlTraits& sql_traits = pc_ctx.sql_traits_;
    sql_traits.is_readonly_stmt_ = ObSQLUtils::is_readonly_stmt(parse_result);
    sql_traits.is_modify_tenant_stmt_ = ObSQLUtils::is_modify_tenant_stmt(parse_result);
    sql_traits.is_cause_implicit_commit_ = ObSQLUtils::cause_implicit_commit(parse_result);
    sql_traits.is_commit_stmt_ = ObSQLUtils::is_commit_stmt(parse_result);

    bool read_only = false;
    if (session->is_inner()) {
    } else if (OB_ISNULL(pc_ctx.sql_ctx_.schema_guard_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid argument", K(pc_ctx.sql_ctx_.schema_guard_));
    } else if (OB_FAIL(pc_ctx.sql_ctx_.schema_guard_->get_tenant_read_only(tenant_id, read_only))) {
      LOG_WARN("fail to get tenant read only attribute", K(ret), K(tenant_id));
    } else if (OB_FAIL(session->check_read_only_privilege(read_only, sql_traits))) {
      LOG_WARN("failed to check read_only privilege", K(ret));
      if (ObSQLUtils::is_end_trans_stmt(parse_result)) {
        int et_ret = OB_SUCCESS;
        exec_ctx.set_need_disconnect(false);
        // FIXME  NG_TRACE_EXT(set_need_disconnect, OB_ID(need_disconnect), false);
        LOG_WARN("is commit or rollback stmt, but fail to check read_only privilege, "
                 "rollback",
            K(ret));
        int64_t plan_timeout = 0;
        if (OB_SUCCESS != (et_ret = session->get_query_timeout(plan_timeout))) {
          LOG_ERROR("fail to get query timeout", K(ret), K(et_ret));
        } else {
          pctx->set_timeout_timestamp(session->get_query_start_time() + plan_timeout);
          if (OB_SUCCESS != (et_ret = ObSqlTransControl::explicit_end_trans(exec_ctx, true))) {
            LOG_ERROR("fail explicit rollback trans", K(ret), K(et_ret));
          }
        }
      }
    }
  }

  if (OB_SUCC(ret) && is_enable_transform_tree) {
    bool flag = false;
    if (add_plan_to_pc || ((T_CREATE_OUTLINE == parse_result.result_tree_->children_[0]->type_ ||
                               T_ALTER_OUTLINE == parse_result.result_tree_->children_[0]->type_) &&
                              (INT64_MAX != parse_result.result_tree_->children_[0]->value_))) {
      flag = true;
      if (T_CREATE_OUTLINE == parse_result.result_tree_->children_[0]->type_) {
        if (1 != parse_result.result_tree_->children_[0]->children_[2]->value_) {
          flag = false;
        }
      }
    }
    if (flag) {
      bool is_transform_outline = (T_CREATE_OUTLINE == parse_result.result_tree_->children_[0]->type_ ||
                                   T_ALTER_OUTLINE == parse_result.result_tree_->children_[0]->type_);
      if (is_transform_outline) {
        LOG_WARN("is_transform_outline", K(is_transform_outline));
      }
      if (OB_FAIL(ObSqlParameterization::parameterize_syntax_tree(allocator,
              is_transform_outline,
              pc_ctx,
              parse_result.result_tree_,
              pctx->get_param_store_for_update()))) {
        if (is_transform_outline) {
          LOG_WARN("fail to parameterize syntax tree", K(ret));
        } else {
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
        parse_result.question_mark_ctx_.count_ = static_cast<int>(pctx->get_param_store().count());
      }
    }
  }

  return ret;
}

int ObSql::pc_add_plan(
    ObPlanCacheCtx& pc_ctx, ObResultSet& result, ObOutlineState& outline_state, ObPlanCache* plan_cache)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlan* phy_plan = result.get_physical_plan();
  pc_ctx.fp_result_.pc_key_.namespace_ = NS_CRSR;
  if (OB_ISNULL(phy_plan) || OB_ISNULL(plan_cache)) {
    ret = OB_NOT_INIT;
    LOG_WARN("Fail to generate plan", K(phy_plan), K(plan_cache));
  } else if (OB_USE_PLAN_CACHE_NONE == phy_plan->get_query_hint().plan_cache_policy_) {
    LOG_DEBUG("Hint not use plan cache");
  } else if (OB_FAIL(result.to_plan(pc_ctx.is_ps_mode_, phy_plan))) {
    LOG_WARN("Failed copy field to pplan", K(ret));
  } else {
    phy_plan->set_outline_state(outline_state);
    if (pc_ctx.is_ps_mode_) {
      // remote SQL enters the plan for the second time, and saves raw_sql as pc_key in the plan cache.
      // then use the ps interface to directly use the parameterized sql as the key to check the plan cache,
      // which can save the cost of SQL fast parse once
      if (pc_ctx.sql_ctx_.is_remote_sql_) {
        pc_ctx.fp_result_.pc_key_.key_id_ = 0;
        pc_ctx.fp_result_.pc_key_.name_ = pc_ctx.raw_sql_;
      }
      ret = plan_cache->add_ps_plan(phy_plan, pc_ctx);
    } else {
      check_template_sql_can_be_prepare(pc_ctx, *phy_plan);
      ret = plan_cache->add_plan(phy_plan, pc_ctx);
    }
    if (OB_SQL_PC_PLAN_DUPLICATE == ret) {
      ret = OB_SUCCESS;
      LOG_DEBUG("this plan has been added by others, need not add again", K(phy_plan));
    } else if (OB_REACH_MEMORY_LIMIT == ret || OB_SQL_PC_PLAN_SIZE_LIMIT == ret) {
      if (REACH_TIME_INTERVAL(1000000)) {
        ObTruncatedString trunc_sql(pc_ctx.raw_sql_);
        LOG_INFO("can't add plan to plan cache",
            K(ret),
            K(phy_plan->get_mem_size()),
            K(trunc_sql),
            K(plan_cache->get_mem_used()));
      }
      ret = OB_SUCCESS;
    } else if (is_not_supported_err(ret)) {
      ret = OB_SUCCESS;
      LOG_DEBUG("plan cache don't support add this kind of plan now", K(phy_plan));
    } else if (OB_FAIL(ret)) {
      if (OB_REACH_MAX_CONCURRENT_NUM != ret) {
        ret = OB_SUCCESS;
        LOG_WARN("Failed to add plan to ObPlanCache", K(ret));
      }
    } else {
      pc_ctx.sql_ctx_.self_add_plan_ = true;
      LOG_DEBUG("Successed to add plan to ObPlanCache", K(phy_plan));
    }
  }

  return ret;
}

void ObSql::check_template_sql_can_be_prepare(ObPlanCacheCtx& pc_ctx, ObPhysicalPlan& plan)
{
  int ret = OB_SUCCESS;
  const ObString& temp_sql = plan.get_constructed_sql();
  ObSQLSessionInfo* session = pc_ctx.sql_ctx_.session_info_;
  if (plan.is_remote_plan() && !temp_sql.empty() && session != nullptr && pc_ctx.select_item_param_infos_.empty()) {
    ParseResult parse_result;
    ObParser parser(pc_ctx.allocator_, session->get_sql_mode(), session->get_local_collation_connection());
    if (OB_FAIL(parser.parse(temp_sql, parse_result))) {
      LOG_DEBUG("generate syntax tree failed", K(temp_sql), K(ret));
    } else {
      plan.set_temp_sql_can_prepare();
    }
  }
}

int ObSql::after_get_plan(ObPlanCacheCtx& pc_ctx, ObSQLSessionInfo& session, ObPhysicalPlan* phy_plan,
    bool from_plan_cache, const ParamStore* ps_params)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* pctx = pc_ctx.exec_ctx_.get_physical_plan_ctx();
  //  LOG_INFO("after get paln", K(pctx), K(phy_plan));
  if (NULL != pctx) {
    if (NULL != phy_plan) {
      // record the plan id in trace_event, perf_event and atomic_event
      NG_TRACE_EXT(plan_id, OB_ID(plan_id), phy_plan->get_plan_id());
      OB_ATOMIC_EVENT_SET_CAT_ID(phy_plan->get_plan_id());
      PERF_SET_CAT_ID(phy_plan->get_plan_id());
      // init auto increment param
      if (OB_FAIL(pc_ctx.exec_ctx_.init_physical_plan_ctx(*phy_plan))) {
        LOG_WARN("fail init exec context", K(ret), K(phy_plan->get_stmt_type()));
      } else if (OB_FAIL(pctx->set_autoinc_params(phy_plan->get_autoinc_params()))) {
        LOG_WARN("failed to set autoinc params", K(ret));
      } else {
        ObIArray<AutoincParam>& autoinc_params = pctx->get_autoinc_params();
        for (int64_t i = 0; OB_SUCC(ret) && i < autoinc_params.count(); ++i) {
          AutoincParam& param = autoinc_params.at(i);
          if (OB_HIDDEN_PK_INCREMENT_COLUMN_ID != param.autoinc_col_id_) {
            param.autoinc_increment_ = session.get_local_auto_increment_increment();
            param.autoinc_offset_ = session.get_local_auto_increment_offset();
          }
        }  // end for
      }
      if (OB_SUCC(ret) && phy_plan->is_remote_plan() && !phy_plan->contains_temp_table() &&
          GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_2250) {
        ParamStore& param_store = pctx->get_param_store_for_update();
        if (OB_NOT_NULL(ps_params)) {
          int64_t initial_param_count = ps_params->count();
          for (int64_t i = param_store.count(); i > initial_param_count; --i) {
            param_store.pop_back();
          }
          pctx->get_remote_sql_info().use_ps_ = true;
          pctx->get_remote_sql_info().remote_sql_ = pc_ctx.sql_ctx_.cur_sql_;
          pctx->get_remote_sql_info().ps_params_ = &param_store;
        } else if (phy_plan->temp_sql_can_prepare() && pc_ctx.neg_param_index_.is_empty() &&
                   !pc_ctx.sql_ctx_.multi_stmt_item_.is_batched_multi_stmt()) {
          LOG_DEBUG("after get plan",
              K(pc_ctx.fp_result_.raw_params_.count()),
              K(pc_ctx.not_param_info_.count()),
              K(param_store));
          int64_t initial_param_count = pc_ctx.fp_result_.raw_params_.count() - pc_ctx.not_param_index_.num_members();
          for (int64_t i = param_store.count(); i > initial_param_count; --i) {
            param_store.pop_back();
          }
          pctx->get_remote_sql_info().use_ps_ = true;
          pctx->get_remote_sql_info().remote_sql_ = phy_plan->get_constructed_sql();
          pctx->get_remote_sql_info().ps_params_ = &param_store;
        } else {
          param_store.reset();
          pctx->get_remote_sql_info().use_ps_ = false;
          pctx->get_remote_sql_info().is_batched_stmt_ = pc_ctx.sql_ctx_.multi_stmt_item_.is_batched_multi_stmt();
          pctx->get_remote_sql_info().remote_sql_ = pc_ctx.sql_ctx_.cur_sql_;
          pctx->get_remote_sql_info().ps_params_ = &param_store;
        }
        LOG_DEBUG("generate remote sql info",
            K(pctx->get_remote_sql_info()),
            K(session.get_local_ob_enable_plan_cache()),
            K(pc_ctx.fp_result_.raw_params_),
            K(pc_ctx.not_param_info_),
            K(phy_plan->temp_sql_can_prepare()),
            K(pc_ctx.neg_param_index_),
            K(pc_ctx.sql_ctx_.multi_stmt_item_.is_batched_multi_stmt()),
            K(phy_plan->get_constructed_sql()),
            KPC(ps_params));
      }
    }

    if (OB_SUCC(ret)) {
      // set last_insert_id
      pctx->set_last_insert_id_session(session.get_local_last_insert_id());
    }

    if (NULL != phy_plan) {
      if (from_plan_cache) {
        ObQueryHint& query_hint = phy_plan->get_query_hint();
        pc_ctx.sql_ctx_.force_print_trace_ = query_hint.force_trace_log_;
        // log_level just deal mpquery now.
        if (MpQuery == pc_ctx.sql_ctx_.exec_type_ && query_hint.log_level_.length() > 0) {
          if (OB_FAIL(process_thread_log_id_level_map(query_hint.log_level_.ptr(), query_hint.log_level_.length()))) {
            LOG_WARN("Failed to process thread log id level map", K(ret));
          }
        }
      }
    }
  } else {
    // not phy_plan, ignore
  }

  return ret;
}

int ObSql::need_add_plan(const ObPlanCacheCtx& pc_ctx, ObResultSet& result, bool is_enable_pc, bool& need_add_plan)
{
  int ret = OB_SUCCESS;
  if (false == need_add_plan) {
    // do nothing
  } else if (!is_enable_pc || !pc_ctx.should_add_plan_) {
    need_add_plan = false;
  } else if (OB_NOT_NULL(result.get_physical_plan()) && result.get_physical_plan()->has_link_table()) {
    need_add_plan = false;
  }
  return ret;
}

int ObSql::handle_physical_plan(
    const ObString& trimed_stmt, ObSqlCtx& context, ObResultSet& result, ObPlanCacheCtx& pc_ctx, const int get_plan_err)
{
  int ret = OB_SUCCESS;
  bool is_valid = true;
  ObString outlined_stmt = trimed_stmt;  // use outline if available
  ObOutlineState outline_state;
  ParseResult parse_result;
  bool is_enable_transform_tree = true;
  bool add_plan_to_pc = false;
  ObSQLSessionInfo& session = result.get_session();
  ObPlanCache* plan_cache = session.get_plan_cache();
  bool use_plan_cache = session.get_local_ob_enable_plan_cache();
  pc_ctx.not_param_index_.reset();
  pc_ctx.neg_param_index_.reset();
  LOG_DEBUG("gen plan info", K(pc_ctx.bl_key_), K(get_plan_err), K(pc_ctx.gen_plan_way_));
  // for batched multi stmt, we only parse and optimize the first statement
  if (context.multi_stmt_item_.is_batched_multi_stmt()) {
    if (OB_ISNULL(context.multi_stmt_item_.get_queries())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_UNLIKELY(context.multi_stmt_item_.get_queries()->empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected array count", K(ret));
    } else {
      outlined_stmt = context.multi_stmt_item_.get_queries()->at(0);
    }
  }
  if (OB_FAIL(ret)) {
    /* do nothing */
  } else if (OB_INVALID_ID == pc_ctx.bl_key_.db_id_ || context.multi_stmt_item_.is_batched_multi_stmt()) {
    // no outline  is available when database name of session not specified, just keep the stmt
  } else if (OB_FAIL(rewrite_query_sql(pc_ctx, outline_state, outlined_stmt))) {
    LOG_WARN("fail to rewrite query sql", K(ret));
  } else {
    pc_ctx.outlined_sql_len_ = outlined_stmt.length();
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(handle_parser(outlined_stmt,
                 result.get_exec_context(),
                 pc_ctx,
                 parse_result,
                 get_plan_err,
                 add_plan_to_pc,
                 is_enable_transform_tree))) {
    LOG_WARN("fail to parser and check", K(ret));
  } else if (context.multi_stmt_item_.is_batched_multi_stmt() &&
             OB_FAIL(check_batched_multi_stmt_after_parser(pc_ctx, parse_result, add_plan_to_pc, is_valid))) {
    LOG_WARN("failed to check batched multi_stmt", K(ret));
  } else if (!is_valid) {
    ret = OB_BATCHED_MULTI_STMT_ROLLBACK;
    LOG_TRACE("batched multi_stmt needs rollback", K(ret));
  } else if (context.multi_stmt_item_.is_batched_multi_stmt() && is_valid && session.use_static_typing_engine()) {
    ret = STATIC_ENG_NOT_IMPLEMENT;
    LOG_WARN("static engine not implement batched multi stmt, will retry", K(ret));
  } else if (OB_FAIL(generate_sql_id(pc_ctx, add_plan_to_pc))) {
    LOG_WARN("fail to generate sql id", K(ret));
  } else if (OB_FAIL(generate_physical_plan(parse_result, &pc_ctx, context, result))) {
    if (OB_ERR_PROXY_REROUTE == ret) {
      LOG_DEBUG("Failed to generate plan", K(ret));
    } else {
      LOG_WARN("Failed to generate plan", K(ret), K(result.get_exec_context().need_disconnect()));
    }
  } else if (OB_FAIL(need_add_plan(pc_ctx, result, use_plan_cache, add_plan_to_pc))) {
    LOG_WARN("get need_add_plan failed", K(ret));
  } else if (add_plan_to_pc) {
    if (OB_FAIL(pc_add_plan(pc_ctx, result, outline_state, plan_cache))) {
      LOG_WARN("fail to add plan to plan cache", K(ret));
    } else {
      LOG_DEBUG("physical plan mem used", K(result.get_physical_plan()->get_mem_size()));
    }
  }
  return ret;
}

int ObSql::handle_parser(const ObString& sql, ObExecContext& exec_ctx, ObPlanCacheCtx& pc_ctx,
    ParseResult& parse_result, int get_plan_err, bool& add_plan_to_pc, bool& is_enable_transform_tree)

{
  int ret = OB_SUCCESS;
  int64_t last_mem_usage = pc_ctx.allocator_.total();
  int64_t parser_mem_usage = 0;
  ObPhysicalPlanCtx* pctx = exec_ctx.get_physical_plan_ctx();
  if (OB_ISNULL(pc_ctx.sql_ctx_.session_info_) || OB_ISNULL(pctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret), KP(pctx), KP(pc_ctx.sql_ctx_.session_info_));
  } else if (pc_ctx.is_ps_mode_) {
    ObPsSqlParamHelper helper;
    int64_t question_mark_count = 0;
    ObArray<int64_t> not_param_offsets;
    ObArray<int64_t> no_check_type_offsets;
    ObIAllocator& allocator = pc_ctx.allocator_;
    ObSQLSessionInfo& session = *(pc_ctx.sql_ctx_.session_info_);
    ObParser parser(allocator, session.get_sql_mode(), session.get_local_collation_connection());
    if (OB_FAIL(parser.parse(sql, parse_result))) {
      LOG_WARN("generate syntax tree failed", K(ret), K(sql));
    } else if (OB_ISNULL(parse_result.result_tree_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("result_tree should not be null", K(ret), K(sql));
    } else if (OB_FAIL(helper.find_special_paramalize(
                   *parse_result.result_tree_, question_mark_count, no_check_type_offsets, not_param_offsets))) {
      LOG_WARN("invalid argument", K(ret), KP(parse_result.result_tree_));
    } else if (OB_FAIL(construct_not_paramalize(not_param_offsets, pctx->get_param_store(), pc_ctx))) {
      LOG_WARN("construct not paramalize failed", K(ret));
    } else if (OB_FAIL(construct_no_check_type_params(no_check_type_offsets, pctx->get_param_store_for_update()))) {
      LOG_WARN("construct no check type params failed", K(ret));
    } else {
      parse_result.question_mark_ctx_.count_ = static_cast<int>(question_mark_count);
      if (OB_SQL_PC_NOT_EXIST == get_plan_err) {
        add_plan_to_pc = true;
      } else {
        add_plan_to_pc = false;
      }
    }
  } else if (OB_FAIL(parser_and_check(
                 sql, exec_ctx, pc_ctx, parse_result, get_plan_err, add_plan_to_pc, is_enable_transform_tree))) {
    LOG_WARN("fail to parser normal query", K(sql), K(ret));
  }
  if (OB_SUCC(ret)) {
    if (pc_ctx.sql_ctx_.session_info_->use_static_typing_engine() && OB_FAIL(pctx->init_datum_param_store())) {
      LOG_WARN("fail to init datum param store", K(ret));
    }
  }
  LOG_DEBUG("SQL MEM USAGE", K(parser_mem_usage), K(last_mem_usage));
  return ret;
}

int ObSql::check_batched_multi_stmt_after_parser(
    ObPlanCacheCtx& pc_ctx, ParseResult& parse_result, bool add_plan_to_pc, bool& is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  if (add_plan_to_pc) {
    is_valid = true;
    // only update support batched multi-stmt optimization
    if (OB_ISNULL(parse_result.result_tree_) || OB_ISNULL(parse_result.result_tree_->children_)) {
      ret = OB_ERR_UNEXPECTED;
      ;
      LOG_WARN("get unexpected null", K(ret), KP(parse_result.result_tree_));
    } else if (OB_UNLIKELY(parse_result.result_tree_->num_child_ < 1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected child number", K(ret));
    } else if (OB_ISNULL(parse_result.result_tree_->children_[0])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (T_UPDATE != parse_result.result_tree_->children_[0]->type_) {
      is_valid = false;
    } else { /*do nothing*/
    }

    if (OB_SUCC(ret) && is_valid && !pc_ctx.not_param_info_.empty()) {
      if (OB_FAIL(ObPlanCacheValue::check_multi_stmt_not_param_value(
              pc_ctx.multi_stmt_fp_results_, pc_ctx.not_param_info_, is_valid))) {
        LOG_WARN("failed to check multi stmt not param value", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObSql::check_batched_multi_stmt_after_resolver(ObPlanCacheCtx& pc_ctx, const ObStmt& stmt, bool& is_valid)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  is_valid = true;
  if (OB_ISNULL(plan_ctx = pc_ctx.exec_ctx_.get_physical_plan_ctx()) || OB_ISNULL(pc_ctx.sql_ctx_.session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (!stmt.is_update_stmt()) {
    is_valid = false;
  } else {
    const ObUpdateStmt& update_stmt = static_cast<const ObUpdateStmt&>(stmt);
    if (1 != update_stmt.get_table_items().count() || !update_stmt.get_table_items().at(0)->is_basic_table()) {
      is_valid = false;
    } else if (update_stmt.has_order_by() || update_stmt.has_limit() || !update_stmt.get_returning_exprs().empty()) {
      is_valid = false;
    } else { /*do nothing*/
    }

    // make sure type of all the parameters are the same
    if (OB_SUCC(ret) && is_valid) {
      ObBitSet<> neg_param_index;
      ObBitSet<> not_param_index;
      ObBitSet<> must_be_positive_index;
      ObArenaAllocator tmp_alloc;
      ParamStore final_param_store((ObWrapperAllocator(tmp_alloc)));
      int64_t query_num = pc_ctx.multi_stmt_fp_results_.count();
      int64_t param_num = plan_ctx->get_param_store().count();
      if (OB_FAIL(ObPlanCacheValue::create_multi_stmt_param_store(
              pc_ctx.allocator_, query_num, param_num, final_param_store))) {
        LOG_WARN("failed to create multi_stmt param store", K(query_num), K(param_num), K(ret));
      } else if (OB_FAIL(neg_param_index.add_members2(pc_ctx.neg_param_index_))) {
        LOG_WARN("failed to assign bit sets", K(ret));
      } else if (OB_FAIL(not_param_index.add_members2(pc_ctx.not_param_index_))) {
        LOG_WARN("failed to assign bit sets", K(ret));
      } else if (OB_FAIL(must_be_positive_index.add_members2(pc_ctx.must_be_positive_index_))) {
        LOG_WARN("failed to assign bit sets", K(ret));
      } else if (OB_FAIL(ObPlanCacheValue::check_multi_stmt_param_type(pc_ctx,
                     stmt.get_stmt_type(),
                     pc_ctx.param_charset_type_,
                     neg_param_index,
                     not_param_index,
                     must_be_positive_index,
                     final_param_store,
                     is_valid))) {
        LOG_WARN("failed to check multi-stmt param type", K(ret));
      } else if (!is_valid) {
        /*do nothing*/
      } else {
        plan_ctx->reset_datum_param_store();
        ret = plan_ctx->get_param_store_for_update().assign(final_param_store);
        if (pc_ctx.sql_ctx_.session_info_->use_static_typing_engine()) {
          if (OB_FAIL(plan_ctx->init_datum_param_store())) {
            LOG_WARN("failed to init datum store", K(ret));
          }
        }
      }
    }

    if (OB_SUCC(ret) && is_valid) {
      ObSEArray<ObRawExpr*, 16> all_exprs;
      if (OB_FAIL(update_stmt.get_relation_exprs(all_exprs))) {
        LOG_WARN("failed to get all relation exprs", K(ret));
      } else if (OB_FAIL(replace_const_expr(all_exprs, plan_ctx->get_param_store_for_update()))) {
        LOG_WARN("failed to replace const exprs", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < update_stmt.get_calculable_exprs().count(); i++) {
          if (OB_FAIL(replace_const_expr(
                  update_stmt.get_calculable_exprs().at(i).expr_, plan_ctx->get_param_store_for_update()))) {
            LOG_WARN("failed to replace const expr", K(ret));
          } else { /*do nothing*/
          }
        }
      }
    }
  }
  return ret;
}

int ObSql::replace_const_expr(ObIArray<ObRawExpr*>& raw_exprs, ParamStore& param_store)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < raw_exprs.count(); i++) {
    if (OB_FAIL(replace_const_expr(raw_exprs.at(i), param_store))) {
      LOG_WARN("failed to replace const expr", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObSql::replace_const_expr(ObRawExpr* raw_expr, ParamStore& param_store)
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
  } else if (raw_expr->is_const_expr()) {
    ObConstRawExpr* const_expr = static_cast<ObConstRawExpr*>(raw_expr);
    if (const_expr->get_value().is_unknown()) {
      int pos = const_expr->get_value().get_unknown();
      if (pos >= 0 && pos < param_store.count()) {
        const_expr->set_param(param_store.at(pos));
      } else { /*do nothing*/
      }
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < raw_expr->get_param_count(); i++) {
      if (OB_FAIL(replace_const_expr(raw_expr->get_param_expr(i), param_store))) {
        LOG_WARN("failed to replace const expr", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObSql::generate_sql_id(ObPlanCacheCtx& pc_ctx, bool add_plan_to_pc)
{
  int ret = OB_SUCCESS;
  if (add_plan_to_pc == false || pc_ctx.is_ps_mode_) {
    (void)ObSQLUtils::md5(pc_ctx.raw_sql_, pc_ctx.sql_ctx_.sql_id_, (int32_t)sizeof(pc_ctx.sql_ctx_.sql_id_));
  } else {
    (void)ObSQLUtils::md5(
        pc_ctx.bl_key_.constructed_sql_, pc_ctx.sql_ctx_.sql_id_, (int32_t)sizeof(pc_ctx.sql_ctx_.sql_id_));
  }
  return ret;
}

int ObSql::calc_pre_calculable_exprs(const ObDMLStmt& stmt, const ObIArray<ObHiddenColumnItem>& calculable_exprs,
    const bool is_ignore_stmt, ObExecContext& exec_ctx, ObPhysicalPlan& phy_plan)
{
  int ret = OB_SUCCESS;
  OB_ASSERT(NULL != exec_ctx.get_physical_plan_ctx() && NULL != exec_ctx.get_my_session());
  ObPhysicalPlanCtx* phy_plan_ctx = exec_ctx.get_physical_plan_ctx();
  ObPreCalcExprFrameInfo* pre_calc_frame = NULL;
  void* frame_buf = NULL;
  bool need_fetch_cur_time = false;
  if (OB_ISNULL(phy_plan_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("phy plan ctx is null", K(ret));
  } else if (OB_ISNULL(frame_buf = phy_plan.get_allocator().alloc(sizeof(ObPreCalcExprFrameInfo)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else {
    phy_plan_ctx->set_ignore_stmt(is_ignore_stmt);
    DatumParamStore& datum_param_store = phy_plan_ctx->get_datum_param_store();
    ObStaticEngineExprCG expr_cg(exec_ctx.get_allocator(), &datum_param_store);
    pre_calc_frame = new (frame_buf) ObPreCalcExprFrameInfo(phy_plan.get_allocator());
    expr_cg.init_operator_cg_ctx(&exec_ctx);
    if (OB_FAIL(expr_cg.generate_calculable_exprs(calculable_exprs, *pre_calc_frame))) {
      LOG_WARN("failed to generate calculable exprs", K(ret));
    } else {
      phy_plan.set_fetch_cur_time(stmt.get_fetch_cur_time());
      phy_plan.set_stmt_type(stmt.get_stmt_type());
      phy_plan.set_literal_stmt_type(stmt.get_literal_stmt_type());

      need_fetch_cur_time = phy_plan.get_fetch_cur_time() && !phy_plan_ctx->has_cur_time();
    }
    if (OB_FAIL(ret)) {
      // set current time before do pre calculation
    } else if (need_fetch_cur_time &&
               FALSE_IT(phy_plan_ctx->set_cur_time(ObTimeUtility::current_time(), *(exec_ctx.get_my_session())))) {
      // do nothing
    } else if (OB_FAIL(ObCacheObject::pre_calculation(is_ignore_stmt, *pre_calc_frame, exec_ctx))) {
      LOG_WARN("failed to pre calculate exprs", K(ret));
    } else if (OB_UNLIKELY(!phy_plan.get_pre_calc_frames().add_last(pre_calc_frame))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to add list element", K(ret));
    } else {
      // do nothing
    }
  }
  return ret;
}

}  // end of namespace sql
}  // end of namespace oceanbase
