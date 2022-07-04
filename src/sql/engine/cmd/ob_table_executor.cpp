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
#include "sql/engine/cmd/ob_table_executor.h"
#include "sql/engine/cmd/ob_index_executor.h"
#include "share/object/ob_obj_cast.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/utility/ob_tracepoint.h"
#include "share/ob_common_rpc_proxy.h"
#include "sql/resolver/ddl/ob_create_table_stmt.h"
#include "sql/resolver/ddl/ob_alter_table_stmt.h"
#include "sql/resolver/ddl/ob_drop_table_stmt.h"
#include "sql/resolver/ddl/ob_rename_table_stmt.h"
#include "sql/resolver/ddl/ob_truncate_table_stmt.h"
#include "sql/resolver/ddl/ob_create_table_like_stmt.h"
#include "sql/resolver/ddl/ob_flashback_stmt.h"
#include "sql/resolver/ddl/ob_purge_stmt.h"
#include "sql/resolver/ddl/ob_optimize_stmt.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/code_generator/ob_expr_generator_impl.h"
#include "sql/engine/cmd/ob_partition_executor_utils.h"
#include "sql/parser/ob_parser.h"

#include "sql/ob_select_stmt_printer.h"
#include "observer/ob_server_struct.h"
#include "observer/ob_server.h"
#include "share/ob_worker.h"
namespace oceanbase {
using namespace common;
using namespace share;
using namespace share::schema;
using namespace observer;
namespace sql {

ObCreateTableExecutor::ObCreateTableExecutor()
{}

ObCreateTableExecutor::~ObCreateTableExecutor()
{}

int ObCreateTableExecutor::prepare_ins_arg(
    ObCreateTableStmt& stmt, const ObSQLSessionInfo* my_session, ObSqlString& ins_sql)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::OB_SQL_EXECUTOR);
  char* buf = static_cast<char*>(allocator.alloc(OB_MAX_SQL_LENGTH));
  int64_t buf_len = OB_MAX_SQL_LENGTH;
  int64_t pos1 = 0;
  bool is_set_subquery = false;
  bool is_oracle_mode = share::is_oracle_mode();
  const ObString& db_name = stmt.get_database_name();
  const ObString& tab_name = stmt.get_table_name();
  const char sep_char = is_oracle_mode ? '"' : '`';
  ObSelectStmt* select_stmt = stmt.get_sub_select();
  ObSelectStmtPrinter select_stmt_printer(buf,
      buf_len,
      &pos1,
      select_stmt,
      select_stmt->tz_info_,
      NULL,  // column_list is null here
      is_set_subquery);
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("allocate memory failed");
  } else if (OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("select stmt should not be null", K(ret));
  } else {
    const ObStmtHint& hint = select_stmt->get_stmt_hint();
    const char* use_px = "";
    if (hint.has_px_hint_) {
      if (hint.enable_use_px()) {
        use_px = "/*+ USE_PX */";
      } else if (hint.disable_use_px()) {
        use_px = "/*+ NO_USE_PX */";
      }
    }
    // 1, generate insert into string
    if (OB_FAIL(databuff_printf(buf,
            buf_len,
            pos1,
            "insert %s into %c%.*s%c.%c%.*s%c",
            use_px,
            sep_char,
            db_name.length(),
            db_name.ptr(),
            sep_char,
            sep_char,
            tab_name.length(),
            tab_name.ptr(),
            sep_char))) {
      LOG_WARN("fail to print insert into string", K(ret), K(db_name), K(tab_name));
    }
  }
  if (OB_SUCC(ret)) {
    if (share::is_oracle_mode()) {
      ObTableSchema& table_schema = stmt.get_create_table_arg().schema_;
      int64_t used_column_count = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < table_schema.get_column_count(); ++i) {
        const ObColumnSchemaV2* column_schema = table_schema.get_column_schema_by_idx(i);
        if (OB_ISNULL(column_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get null column shcmea", K(ret));
        } else if (column_schema->get_column_id() < OB_END_RESERVED_COLUMN_ID_NUM) {
          // do nothing
        } else if (OB_FAIL(databuff_printf(buf, buf_len, pos1, (0 == used_column_count) ? "(" : ", "))) {
          LOG_WARN("failed to print insert into string", K(ret), K(i));
        } else if (OB_FAIL(databuff_printf(buf,
                       buf_len,
                       pos1,
                       "%c%.*s%c",
                       sep_char,
                       LEN_AND_PTR(column_schema->get_column_name_str()),
                       sep_char))) {
          LOG_WARN("failed to print insert into string", K(ret));
        } else {
          ++used_column_count;
        }
      }
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_select_item_size(); ++i) {
        const SelectItem& select_item = select_stmt->get_select_item(i);
        if (OB_FAIL(databuff_printf(buf, buf_len, pos1, (0 == i) ? "(" : ", "))) {
          LOG_WARN("failed to print insert into string", K(ret), K(i));
        } else { /* do nothing */
        }
        if (OB_SUCC(ret)) {
          if (!select_item.alias_name_.empty()) {
            if (OB_FAIL(databuff_printf(
                    buf, buf_len, pos1, "%c%.*s%c", sep_char, LEN_AND_PTR(select_item.alias_name_), sep_char))) {
              LOG_WARN("failed to print insert into string", K(ret));
            } else { /* do nothing */
            }
          } else {
            if (OB_FAIL(databuff_printf(
                    buf, buf_len, pos1, "%c%.*s%c", sep_char, LEN_AND_PTR(select_item.expr_name_), sep_char))) {
              LOG_WARN("failed to print insert into string", K(ret));
            } else { /* do nothing */
            }
          }
        } else { /* do nothing */
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(databuff_printf(buf, buf_len, pos1, ") "))) {
        LOG_WARN("fail to append ')'", K(ret));
      } else if (OB_FAIL(select_stmt_printer.do_print())) {
        LOG_WARN("fail to print select stmt", K(ret));
      } else if (OB_FAIL(ins_sql.append(buf, pos1))) {
        LOG_WARN("fail to append insert into string", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObCollationType client_cs_type = my_session->get_local_collation_connection();
    ObString converted_sql;
    if (OB_FAIL(ObSQLUtils::copy_and_convert_string_charset(
            allocator, ins_sql.string(), converted_sql, CS_TYPE_UTF8MB4_BIN, client_cs_type))) {
      LOG_WARN("fail to convert insert into string to client_cs_type", K(ret));
    } else if (OB_FAIL(ins_sql.assign(converted_sql))) {
      LOG_WARN("fail to assign converted insert into string", K(ret));
    }
  }
  LOG_DEBUG("ins str preparation complete!", K(ins_sql), K(ret), K(share::is_oracle_mode()));
  return ret;
}

int ObCreateTableExecutor::prepare_alter_arg(
    ObCreateTableStmt& stmt, const ObSQLSessionInfo* my_session, obrpc::ObAlterTableArg& alter_table_arg)
{
  int ret = OB_SUCCESS;
  const obrpc::ObCreateTableArg& create_table_arg = stmt.get_create_table_arg();
  ObTableSchema& table_schema = const_cast<obrpc::ObCreateTableArg&>(create_table_arg).schema_;
  AlterTableSchema* alter_table_schema = &alter_table_arg.alter_table_schema_;
  table_schema.set_session_id(my_session->get_sessid_for_table());
  alter_table_arg.session_id_ = my_session->get_sessid_for_table();
  alter_table_schema->alter_type_ = OB_DDL_ALTER_TABLE;
  // compat for old server
  alter_table_arg.tz_info_ = my_session->get_tz_info_wrap().get_tz_info_offset();
  alter_table_arg.is_inner_ = my_session->is_inner();
  alter_table_arg.exec_tenant_id_ = my_session->get_effective_tenant_id();
  if (OB_FAIL(alter_table_arg.tz_info_wrap_.deep_copy(my_session->get_tz_info_wrap()))) {
    LOG_WARN("failed to deep_copy tz info wrap", "tz_info_wrap", my_session->get_tz_info_wrap(), K(ret));
  } else if (OB_FAIL(alter_table_arg.set_nls_formats(my_session->get_local_nls_formats()))) {
    LOG_WARN("failed to set_nls_formats", K(ret));
  } else if (OB_FAIL(alter_table_schema->assign(table_schema))) {
    LOG_WARN("failed to assign alter table schema", K(ret));
  } else if (FALSE_IT(alter_table_schema->set_session_id(0))) {
    // impossible
  } else if (OB_FAIL(alter_table_schema->set_origin_table_name(stmt.get_table_name()))) {
    LOG_WARN("failed to set origin table name", K(ret));
  } else if (OB_FAIL(alter_table_schema->set_origin_database_name(stmt.get_database_name()))) {
    LOG_WARN("failed to set origin database name", K(ret));
  } else if (OB_FAIL(alter_table_schema->set_table_name(stmt.get_table_name()))) {
    LOG_WARN("failed to set table name", K(ret));
  } else if (OB_FAIL(alter_table_schema->set_database_name(stmt.get_database_name()))) {
    LOG_WARN("failed to set database name", K(ret));
  } else if (OB_FAIL(alter_table_schema->alter_option_bitset_.add_member(obrpc::ObAlterTableArg::SESSION_ID))) {
    LOG_WARN("failed to add member SESSION_ID for alter table schema", K(ret), K(alter_table_arg));
  }
  LOG_DEBUG("alter table arg preparation complete!", K(*alter_table_schema), K(ret));
  return ret;
}

int ObCreateTableExecutor::prepare_drop_arg(const ObCreateTableStmt& stmt, const ObSQLSessionInfo* my_session,
    obrpc::ObTableItem& table_item, obrpc::ObDropTableArg& drop_table_arg)
{
  int ret = OB_SUCCESS;
  const ObString& db_name = stmt.get_database_name();
  const ObString& tab_name = stmt.get_table_name();
  drop_table_arg.if_exist_ = true;
  drop_table_arg.tenant_id_ = my_session->get_login_tenant_id();
  drop_table_arg.to_recyclebin_ = false;
  drop_table_arg.table_type_ = USER_TABLE;
  drop_table_arg.session_id_ = my_session->get_sessid_for_table();
  drop_table_arg.exec_tenant_id_ = my_session->get_effective_tenant_id();
  table_item.database_name_ = db_name;
  table_item.table_name_ = tab_name;
  if (OB_FAIL(my_session->get_name_case_mode(table_item.mode_))) {
    LOG_WARN("failed to get name case mode!", K(ret));
  } else if (OB_FAIL(drop_table_arg.tables_.push_back(table_item))) {
    LOG_WARN("failed to add table item!", K(table_item), K(ret));
  }
  LOG_DEBUG("drop table arg preparation complete!", K(drop_table_arg), K(table_item), K(ret));
  return ret;
}

int ObCreateTableExecutor::execute_ctas(
    ObExecContext& ctx, ObCreateTableStmt& stmt, obrpc::ObCommonRpcProxy* common_rpc_proxy)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObMySQLProxy* sql_proxy = ctx.get_sql_proxy();
  common::ObCommonSqlProxy* user_sql_proxy;
  common::ObOracleSqlProxy oracle_sql_proxy;
  ObSQLSessionInfo* my_session = ctx.get_my_session();
  ObPhysicalPlanCtx* plan_ctx = ctx.get_physical_plan_ctx();
  obrpc::ObAlterTableArg alter_table_arg;
  obrpc::ObDropTableArg drop_table_arg;
  obrpc::ObTableItem table_item;
  ObSqlString ins_sql;
  const observer::ObGlobalContext& gctx = observer::ObServer::get_instance().get_gctx();
  obrpc::ObCreateTableRes create_table_res;
  obrpc::ObCreateTableArg& create_table_arg = stmt.get_create_table_arg();
  create_table_arg.is_inner_ = my_session->is_inner();
  bool need_clean = true;
  CK(OB_NOT_NULL(sql_proxy),
      OB_NOT_NULL(my_session),
      OB_NOT_NULL(gctx.schema_service_),
      OB_NOT_NULL(plan_ctx),
      OB_NOT_NULL(common_rpc_proxy));
  if (OB_SUCC(ret)) {
    ObInnerSQLConnectionPool* pool = static_cast<observer::ObInnerSQLConnectionPool*>(sql_proxy->get_pool());
    if (OB_ISNULL(pool)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pool is null", K(ret));
    } else if (OB_FAIL(oracle_sql_proxy.init(pool))) {
      LOG_WARN("init oracle sql proxy failed", K(ret));
    } else if (OB_FAIL(prepare_ins_arg(stmt, my_session, ins_sql))) {
      LOG_WARN("failed to prepare insert table arg", K(ret));
    } else if (OB_FAIL(prepare_alter_arg(stmt, my_session, alter_table_arg))) {
      LOG_WARN("failed to prepare alter table arg", K(ret));
    } else if (OB_FAIL(prepare_drop_arg(stmt, my_session, table_item, drop_table_arg))) {
      LOG_WARN("failed to prepare drop table arg", K(ret));
    } else if (OB_FAIL(common_rpc_proxy->create_table(create_table_arg, create_table_res))) {
      LOG_WARN("rpc proxy create table failed", K(ret), "dst", common_rpc_proxy->get_server());
    } else if (OB_INVALID_ID != create_table_res.table_id_) {
      if (OB_INVALID_VERSION == create_table_res.schema_version_) {
        // During upgrade, high version server send create table rpc to low version RS. RS will
        // return a struct UINT64. Try deserialize UINT64 to ObCreateTableRes will only get correct
        // table_id. And schema_version remain default value OB_INVALID_VERSION.
        // In this scenario, using old strategy.
        ObSchemaGetterGuard schema_guard;
        const ObTableSchema* table_schema = NULL;
        const int64_t s1 = ObTimeUtility::current_time();
        int64_t s2 = 0;
        const int64_t DDL_WAIT_TIME = 1 * 1000 * 1000;  // 1s
        const int64_t SLEEP_ON_NEED_RETRY = 10 * 1000;  // 10ms
        while (true) {
          if (OB_FAIL(
                  gctx.schema_service_->get_tenant_schema_guard(my_session->get_effective_tenant_id(), schema_guard))) {
            LOG_WARN("failed to get schema guard", K(ret));
            break;
          }
          if (OB_FAIL(schema_guard.get_table_schema(create_table_res.table_id_, table_schema))) {
            LOG_WARN("failed to get table schema", K(ret), K(create_table_res.table_id_));
            break;
          }
          s2 = ObTimeUtility::current_time();
          if (OB_NOT_NULL(table_schema)) {
            LOG_DEBUG("CTAS refresh table schema succeed!", K(ret), K(s2 - s1));
            break;
          }
          if (s2 - s1 < DDL_WAIT_TIME) {
            ret = OB_SUCCESS;
            usleep(SLEEP_ON_NEED_RETRY);
            LOG_DEBUG("CTAS refresh table schema failed, try again", K(ret), K(create_table_res.table_id_), K(s2 - s1));
          } else {
            LOG_DEBUG("CTAS refresh table schema timeout!", K(ret), K(create_table_res.table_id_), K(s2 - s1));
            break;
          }
        }
      } else {
        uint64_t tenant_id = my_session->get_effective_tenant_id();
        if (is_inner_table(create_table_res.table_id_)) {
          tenant_id = OB_SYS_TENANT_ID;
        }
        if (OB_FAIL(gctx.schema_service_->async_refresh_schema(tenant_id, create_table_res.schema_version_))) {
          LOG_WARN("failed to async refresh schema", K(ret));
        }
      }

      #ifdef ERRSIM
      {
        int tmp_ret = E(EventTable::EN_CTAS_FAIL_NO_DROP_ERROR) OB_SUCCESS;
        if (OB_FAIL(tmp_ret)) {
          ret = tmp_ret;
          need_clean = false;
        }
      }
      #else
        // do nothing...
      #endif

      if (OB_SUCC(ret)) {
        common::sqlclient::ObISQLConnection *conn = NULL;
        if (share::is_oracle_mode()) {
          user_sql_proxy = &oracle_sql_proxy;
        } else {
          user_sql_proxy = sql_proxy;
        }
        if (OB_FAIL(pool->acquire(my_session, conn))) {
          LOG_WARN("failed to acquire inner connection", K(ret));
        } else if (OB_ISNULL(conn)) {
          ret = OB_INNER_STAT_ERROR;
          LOG_WARN("connection can not be NULL", K(ret));
        } else if (OB_FAIL(conn->start_transaction())) {
          LOG_WARN("failed start transaction", K(ret));
        } else {
          if (OB_FAIL(conn->execute_write(my_session->get_effective_tenant_id(), ins_sql.ptr(),
                                          affected_rows, true))) {
            LOG_WARN("failed to exec sql", K(ins_sql), K(ret));
          }
          // transaction started, must commit or rollback
          int tmp_ret = OB_SUCCESS;
          if (OB_LIKELY(OB_SUCCESS == ret)) {
            tmp_ret = conn->commit();
          } else {
            tmp_ret = conn->rollback();
          }
          if (OB_UNLIKELY(OB_SUCCESS != tmp_ret)) {
            ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
            LOG_WARN("fail to end transaction", K(ret), K(tmp_ret));
          }
        }
        if (OB_NOT_NULL(conn)) {
          user_sql_proxy->close(conn, true);
        }
      }

      if (OB_SUCC(ret)) {
        obrpc::ObAlterTableRes res;
        if (OB_FAIL(common_rpc_proxy->alter_table(alter_table_arg, res))) {
          LOG_WARN("failed to update table session", K(ret), K(alter_table_arg));
        }
      }

      if (OB_FAIL(ret)) {
        my_session->update_last_active_time();
        if (OB_LIKELY(need_clean)) {
          int tmp_ret = OB_SUCCESS;
          if (OB_SUCCESS != (tmp_ret = common_rpc_proxy->drop_table(drop_table_arg))) {
            LOG_WARN("failed to drop table", K(drop_table_arg), K(ret));
          } else {
            LOG_INFO("table is created and dropped due to error ", K(ret));
          }
        }
      } else {
        plan_ctx->set_affected_rows(affected_rows);
        LOG_DEBUG("CTAS all done", K(ins_sql), K(affected_rows), K(share::is_oracle_mode()));
      }

    } else {
      LOG_DEBUG("table exists, no need to CTAS", K(create_table_res.table_id_));
    }
  }
  return ret;
}

int ObCreateTableExecutor::execute(ObExecContext& ctx, ObCreateTableStmt& stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx* task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy* common_rpc_proxy = NULL;
  obrpc::ObCreateTableRes res;
  obrpc::ObCreateTableArg& create_table_arg = stmt.get_create_table_arg();
  ObString first_stmt;
  ObSelectStmt* select_stmt = stmt.get_sub_select();
  ObTableSchema& table_schema = create_table_arg.schema_;
  ObSQLSessionInfo* my_session = ctx.get_my_session();
  if (OB_ISNULL(my_session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (OB_FAIL(stmt.get_first_stmt(first_stmt))) {
    LOG_WARN("get first statement failed", K(ret));
  } else {
    create_table_arg.is_inner_ = my_session->is_inner();
    const_cast<obrpc::ObCreateTableArg&>(create_table_arg).ddl_stmt_str_ = first_stmt;
    if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
      ret = OB_NOT_INIT;
      LOG_WARN("get task executor context failed", K(ret));
    } else if (OB_FAIL(ObPartitionExecutorUtils::calc_values_exprs(ctx, stmt))) {
      LOG_WARN("compare range parition expr fail", K(ret));
    } else if (OB_FAIL(set_index_arg_list(ctx, stmt))) {
      LOG_WARN("fail to set index_arg_list", K(ret));
    } else if (OB_FAIL(task_exec_ctx->get_common_rpc(common_rpc_proxy))) {
      LOG_WARN("get common rpc proxy failed", K(ret));
    } else if (OB_ISNULL(common_rpc_proxy)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("common rpc proxy should not be null", K(ret));
    } else if (OB_ISNULL(select_stmt)) {
      if (OB_FAIL(common_rpc_proxy->create_table(create_table_arg, res))) {
        LOG_WARN("rpc proxy create table failed", K(ret), "dst", common_rpc_proxy->get_server());
      } else { /* do nothing */
      }
    } else if (OB_FAIL(execute_ctas(ctx, stmt, common_rpc_proxy))) {
      LOG_WARN("execute create table as select failed", K(ret));
    }

    // only CTAS or create temperary table will make session_id != 0. If such table detected, set
    // need ctas cleanup task anyway to do some cleanup jobs
    if (0 != table_schema.get_session_id()) {
      LOG_TRACE("CTAS or temporary table create detected", K(table_schema));
      ATOMIC_STORE(&OBSERVER.need_ctas_cleanup_, true);
    }
  }
  return ret;
}

int ObCreateTableExecutor::set_index_arg_list(ObExecContext& ctx, ObCreateTableStmt& stmt)
{
  int ret = OB_SUCCESS;
  obrpc::ObCreateTableArg& create_table_arg = const_cast<obrpc::ObCreateTableArg&>(stmt.get_create_table_arg());
  if (stmt.get_index_partition_resolve_results().count() != stmt.get_index_arg_list().count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid index resolve result", K(ret), K(stmt));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_index_arg_list().count(); i++) {
    ObCreateIndexStmt index_stmt;
    ObPartitionResolveResult& resolve_result = stmt.get_index_partition_resolve_results().at(i);
    index_stmt.get_part_fun_exprs() = resolve_result.get_part_fun_exprs();
    index_stmt.get_part_values_exprs() = resolve_result.get_part_values_exprs();
    index_stmt.get_subpart_fun_exprs() = resolve_result.get_subpart_fun_exprs();
    index_stmt.get_template_subpart_values_exprs() = resolve_result.get_template_subpart_values_exprs();
    index_stmt.get_individual_subpart_values_exprs() = resolve_result.get_individual_subpart_values_exprs();
    if (OB_FAIL(index_stmt.get_create_index_arg().assign(stmt.get_index_arg_list().at(i)))) {
      LOG_WARN("fail to assign index arg", K(ret));
    } else if (OB_FAIL(ObPartitionExecutorUtils::calc_values_exprs(ctx, index_stmt))) {
      LOG_WARN("fail to compare range partition expr", K(ret));
    } else if (OB_FAIL(create_table_arg.index_arg_list_.push_back(index_stmt.get_create_index_arg()))) {
      LOG_WARN("fail to push back index_arg", K(ret));
    }
  }
  return ret;
}

int ObTableExecutorUtils::get_first_stmt(const common::ObString& stmt, common::ObString& first_stmt, ObSQLMode sql_mode)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::OB_SQL_PARSER);
  ObSEArray<ObString, 1> queries;
  ObMPParseStat parse_stat;
  ObParser parser(allocator, sql_mode);
  if (OB_FAIL(parser.split_multiple_stmt(stmt, queries, parse_stat, true /*return the first stmt*/))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("Get first statement from multiple statements failed", K(ret));
  } else if (0 == queries.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("Get non-statement from multiple statements", K(ret));
  } else {
    first_stmt = queries.at(0);
  }

  return ret;
}

/**
 *
 */

ObAlterTableExecutor::ObAlterTableExecutor()
{}

ObAlterTableExecutor::~ObAlterTableExecutor()
{}

int ObAlterTableExecutor::refresh_schema_for_table(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  share::schema::ObSchemaGetterGuard schema_guard;
  const observer::ObGlobalContext& gctx = observer::ObServer::get_instance().get_gctx();
  ObMultiVersionSchemaService* schema_service = gctx.schema_service_;
  int64_t local_version = OB_INVALID_VERSION;
  int64_t global_version = OB_INVALID_VERSION;

  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, schema service must not be NULL", K(ret));
  } else if (OB_FAIL(schema_service->get_tenant_refreshed_schema_version(tenant_id, local_version))) {
    LOG_WARN("fail to get local version", K(ret), "tenant_id", tenant_id);
  } else if (OB_FAIL(schema_service->get_tenant_received_broadcast_version(tenant_id, global_version))) {
    LOG_WARN("fail to get global version", K(ret), "tenant_id", tenant_id);
  } else if (local_version < global_version) {
    LOG_INFO("try to refresh schema", K(local_version), K(global_version));
    ObSEArray<uint64_t, 1> tenant_ids;
    if (OB_FAIL(tenant_ids.push_back(tenant_id))) {
      LOG_WARN("fail to push back tenant_id", K(ret), "tenant_id", tenant_id);
    } else if (OB_FAIL(schema_service->refresh_and_add_schema(tenant_ids))) {
      LOG_WARN("failed to refresh schema", K(ret));
    }
  }
  return ret;
}

int ObAlterTableExecutor::alter_table_rpc_v1(obrpc::ObAlterTableArg& alter_table_arg, obrpc::ObAlterTableRes& res,
    common::ObIAllocator& allocator, obrpc::ObCommonRpcProxy* common_rpc_proxy, ObSQLSessionInfo* my_session,
    const bool is_sync_ddl_user)
{
  int ret = OB_SUCCESS;
  bool alter_table_add_index = false;
  const ObSArray<obrpc::ObIndexArg*> index_arg_list = alter_table_arg.index_arg_list_;
  for (int64_t i = 0; OB_SUCC(ret) && i < index_arg_list.size(); ++i) {
    obrpc::ObIndexArg* index_arg = index_arg_list.at(i);
    if (OB_ISNULL(index_arg)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("index arg should not be null", K(ret));
    } else if (obrpc::ObIndexArg::ADD_INDEX == index_arg->index_action_type_) {
      alter_table_add_index = true;
      break;
    }
  }
  if (!alter_table_add_index) {
    if (OB_FAIL(common_rpc_proxy->alter_table(alter_table_arg, res))) {
      LOG_WARN("rpc proxy alter table failed", K(ret), "dst", common_rpc_proxy->get_server(), K(alter_table_arg));
    }
  } else {
    ObSArray<obrpc::ObIndexArg*> add_index_arg_list;
    alter_table_arg.index_arg_list_.reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < index_arg_list.size(); ++i) {
      obrpc::ObIndexArg* index_arg = index_arg_list.at(i);
      if (OB_ISNULL(index_arg)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("index arg should not be null", K(ret));
      } else if (obrpc::ObIndexArg::ADD_INDEX == index_arg->index_action_type_) {
        if (OB_FAIL(add_index_arg_list.push_back(index_arg))) {
          LOG_WARN("fail to push back to arg_for_adding_index_list", K(ret));
        }
      } else {  // not for adding index
        if (OB_FAIL(alter_table_arg.index_arg_list_.push_back(index_arg))) {
          LOG_WARN("fail to push back to arg_for_adding_index_list", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(common_rpc_proxy->alter_table(alter_table_arg, res))) {
        LOG_WARN("rpc proxy alter table for not adding index failed",
            K(ret),
            "dst",
            common_rpc_proxy->get_server(),
            K(alter_table_arg));
      }
    }
    if (OB_SUCC(ret)) {
      ObString empty_stmt;
      alter_table_arg.is_alter_columns_ = false;
      alter_table_arg.is_alter_options_ = false;
      alter_table_arg.is_alter_partitions_ = false;
      alter_table_arg.ddl_stmt_str_ = empty_stmt;
      alter_table_arg.ddl_id_str_ = empty_stmt;
      alter_table_arg.alter_constraint_type_ = obrpc::ObAlterTableArg::CONSTRAINT_NO_OPERATION;
      ObSArray<uint64_t> added_index_table_ids;
      ObCreateIndexExecutor create_index_executor;
      for (int64_t i = 0; OB_SUCC(ret) && i < add_index_arg_list.size(); ++i) {
        alter_table_arg.index_arg_list_.reset();
        if (OB_FAIL(alter_table_arg.index_arg_list_.push_back(add_index_arg_list.at(i)))) {
          LOG_WARN("fail to push back to arg_for_adding_index_list", K(ret));
        } else if (OB_FAIL(common_rpc_proxy->alter_table(alter_table_arg, res))) {
          LOG_WARN("rpc proxy alter table for adding index failed",
              K(ret),
              "dst",
              common_rpc_proxy->get_server(),
              K(alter_table_arg));
        } else {
          obrpc::ObIndexArg* index_arg = alter_table_arg.index_arg_list_.at(0);
          obrpc::ObCreateIndexArg* create_index_arg = NULL;
          if (OB_ISNULL(index_arg)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("index arg is null", K(ret), K(i));
          } else if (obrpc::ObIndexArg::ADD_INDEX != index_arg->index_action_type_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("index action type should be add index", K(ret), K(i));
          } else if (OB_ISNULL(create_index_arg = static_cast<obrpc::ObCreateIndexArg*>(index_arg))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("create index arg is null", K(ret), K(i));
          } else if (!is_sync_ddl_user) {
            create_index_arg->index_schema_.set_table_id(res.index_table_id_);
            create_index_arg->index_schema_.set_schema_version(res.schema_version_);
            if (OB_FAIL(create_index_executor.sync_check_index_status(
                    *my_session, *common_rpc_proxy, *create_index_arg, allocator))) {
              LOG_WARN("failed to sync_check_index_status", K(ret), K(*create_index_arg), K(i));
            } else {
              added_index_table_ids.push_back(res.index_table_id_);
            }
          }
        }
      }
      if (OB_FAIL(ret)) {
        int tmp_ret = OB_SUCCESS;
        for (int64_t i = 0; OB_SUCCESS == tmp_ret && i < added_index_table_ids.size(); ++i) {
          obrpc::ObDropIndexArg drop_index_arg;
          obrpc::ObCreateIndexArg* create_index_arg = static_cast<obrpc::ObCreateIndexArg*>(add_index_arg_list.at(i));
          drop_index_arg.tenant_id_ = create_index_arg->tenant_id_;
          drop_index_arg.exec_tenant_id_ = create_index_arg->tenant_id_;
          drop_index_arg.index_table_id_ = added_index_table_ids.at(i);
          drop_index_arg.session_id_ = create_index_arg->session_id_;
          drop_index_arg.index_name_ = create_index_arg->index_name_;
          drop_index_arg.table_name_ = create_index_arg->table_name_;
          drop_index_arg.database_name_ = create_index_arg->database_name_;
          drop_index_arg.index_action_type_ = obrpc::ObIndexArg::DROP_INDEX;
          drop_index_arg.to_recyclebin_ = false;
          if (OB_SUCCESS != (tmp_ret = create_index_executor.set_drop_index_stmt_str(drop_index_arg, allocator))) {
            LOG_WARN("fail to set drop index ddl_stmt_str", K(tmp_ret));
          } else if (OB_SUCCESS != (tmp_ret = common_rpc_proxy->drop_index(drop_index_arg))) {
            LOG_WARN("rpc proxy drop index failed",
                "dst",
                common_rpc_proxy->get_server(),
                K(tmp_ret),
                K(drop_index_arg.table_name_),
                K(drop_index_arg.index_name_));
          }
        }
        LOG_INFO("added indexes failed, we rolled back all indexes added in this same alter table sql. But we didn't "
                 "roll back other actions in this same alter table sql");
      }
    }
  }

  return ret;
}

int ObAlterTableExecutor::alter_table_rpc_v2(obrpc::ObAlterTableArg& alter_table_arg, obrpc::ObAlterTableRes& res,
    common::ObIAllocator& allocator, obrpc::ObCommonRpcProxy* common_rpc_proxy, ObSQLSessionInfo* my_session,
    const bool is_sync_ddl_user)
{
  int ret = OB_SUCCESS;
  const ObSArray<obrpc::ObIndexArg*> index_arg_list = alter_table_arg.index_arg_list_;
  ObSArray<obrpc::ObIndexArg*> add_index_arg_list;
  alter_table_arg.index_arg_list_.reset();

  for (int64_t i = 0; OB_SUCC(ret) && i < index_arg_list.size(); ++i) {
    obrpc::ObIndexArg* index_arg = index_arg_list.at(i);
    if (OB_ISNULL(index_arg)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("index arg should not be null", KR(ret));
    } else if (obrpc::ObIndexArg::ADD_INDEX == index_arg->index_action_type_) {
      if (OB_FAIL(add_index_arg_list.push_back(index_arg))) {
        LOG_WARN("fail to push back to arg_for_adding_index_list", KR(ret));
      }
    } else {  // for rename/drop index action
      if (OB_FAIL(alter_table_arg.index_arg_list_.push_back(index_arg))) {
        LOG_WARN("fail to push back to arg_for_adding_index_list", KR(ret));
      }
    }
  }
  // for add index action
  for (int64_t i = 0; OB_SUCC(ret) && i < add_index_arg_list.size(); ++i) {
    if (OB_FAIL(alter_table_arg.index_arg_list_.push_back(add_index_arg_list.at(i)))) {
      LOG_WARN("fail to push back to arg_for_adding_index_list", KR(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(common_rpc_proxy->alter_table(alter_table_arg, res))) {
      LOG_WARN("rpc proxy alter table failed", KR(ret), "dst", common_rpc_proxy->get_server(), K(alter_table_arg));
    } else {
      alter_table_arg.based_schema_object_infos_.reset();
    }
  }
  if (OB_SUCC(ret)) {
    ObCreateIndexExecutor create_index_executor;
    uint64_t failed_index_no = OB_INVALID_ID;
    if (!is_sync_ddl_user && alter_table_arg.is_update_global_indexes_ &&
        (obrpc::ObAlterTableArg::DROP_PARTITION == alter_table_arg.alter_part_type_ ||
            obrpc::ObAlterTableArg::DROP_SUB_PARTITION == alter_table_arg.alter_part_type_ ||
            obrpc::ObAlterTableArg::TRUNCATE_PARTITION == alter_table_arg.alter_part_type_ ||
            obrpc::ObAlterTableArg::TRUNCATE_SUB_PARTITION == alter_table_arg.alter_part_type_)) {
      common::ObSArray<ObAlterTableResArg>& res_array = res.res_arg_array_;
      for (int64_t i = 0; OB_SUCC(ret) && i < res_array.size(); ++i) {
        obrpc::ObCreateIndexArg create_index_arg;
        create_index_arg.index_schema_.set_table_id(res_array.at(i).schema_id_);
        create_index_arg.index_schema_.set_schema_version(res_array.at(i).schema_version_);
        if (OB_FAIL(create_index_executor.sync_check_index_status(
                *my_session, *common_rpc_proxy, create_index_arg, allocator))) {
          LOG_WARN("failed to sync_check_index_status", KR(ret), K(create_index_arg), K(i));
        }
      }
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < add_index_arg_list.size(); ++i) {
        obrpc::ObIndexArg* index_arg = add_index_arg_list.at(i);
        obrpc::ObCreateIndexArg* create_index_arg = NULL;
        if (OB_ISNULL(index_arg)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("index arg is null", KR(ret), K(i));
        } else if (obrpc::ObIndexArg::ADD_INDEX != index_arg->index_action_type_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("index action type should be add index", KR(ret), K(i), K(*index_arg));
        } else if (OB_ISNULL(create_index_arg = static_cast<obrpc::ObCreateIndexArg*>(index_arg))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("create index arg is null", KR(ret), K(i));
        } else if (!is_sync_ddl_user) {
          create_index_arg->index_schema_.set_table_id(res.res_arg_array_.at(i).schema_id_);
          create_index_arg->index_schema_.set_schema_version(res.res_arg_array_.at(i).schema_version_);
          if (OB_FAIL(create_index_executor.sync_check_index_status(
                  *my_session, *common_rpc_proxy, *create_index_arg, allocator))) {
            failed_index_no = i;
            LOG_WARN("failed to sync_check_index_status", KR(ret), K(*create_index_arg), K(i));
          }
        }
      }
      if (OB_FAIL(ret)) {
        int tmp_ret = OB_SUCCESS;
        for (int64_t i = 0; (OB_SUCCESS == tmp_ret) && (i < add_index_arg_list.size()); ++i) {
          if (failed_index_no == i) {
            continue;
          } else {
            obrpc::ObDropIndexArg drop_index_arg;
            obrpc::ObCreateIndexArg* create_index_arg = static_cast<obrpc::ObCreateIndexArg*>(add_index_arg_list.at(i));
            drop_index_arg.tenant_id_ = create_index_arg->tenant_id_;
            drop_index_arg.exec_tenant_id_ = create_index_arg->tenant_id_;
            drop_index_arg.index_table_id_ = res.res_arg_array_.at(i).schema_id_;
            drop_index_arg.session_id_ = create_index_arg->session_id_;
            drop_index_arg.index_name_ = create_index_arg->index_name_;
            drop_index_arg.table_name_ = create_index_arg->table_name_;
            drop_index_arg.database_name_ = create_index_arg->database_name_;
            drop_index_arg.index_action_type_ = obrpc::ObIndexArg::DROP_INDEX;
            drop_index_arg.to_recyclebin_ = false;
            if (OB_SUCCESS != (tmp_ret = create_index_executor.set_drop_index_stmt_str(drop_index_arg, allocator))) {
              LOG_WARN("fail to set drop index ddl_stmt_str", K(tmp_ret));
            } else if (OB_SUCCESS != (tmp_ret = common_rpc_proxy->drop_index(drop_index_arg))) {
              LOG_WARN("rpc proxy drop index failed",
                  "dst",
                  common_rpc_proxy->get_server(),
                  K(tmp_ret),
                  K(drop_index_arg.table_name_),
                  K(drop_index_arg.index_name_));
            }
          }
        }
        LOG_INFO("added indexes failed, we rolled back all indexes added in this same alter table sql. But we didn't "
                 "roll back other actions in this same alter table sql");
      }
    }
  }

  return ret;
}

int ObAlterTableExecutor::execute(ObExecContext& ctx, ObAlterTableStmt& stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx* task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy* common_rpc_proxy = NULL;
  ObMySQLProxy* sql_proxy = ctx.get_sql_proxy();
  obrpc::ObAlterTableArg& alter_table_arg = stmt.get_alter_table_arg();
  ObString first_stmt;
  ObSQLSessionInfo* my_session = NULL;
  obrpc::ObAlterTableRes res;
  bool is_sync_ddl_user = false;
  bool need_modify_fk_validate = false;
  ObArenaAllocator allocator(ObModIds::OB_SQL_EXECUTOR);
  if (OB_FAIL(stmt.get_first_stmt(first_stmt))) {
    LOG_WARN("get first statement failed", K(ret));
  } else {
    alter_table_arg.ddl_stmt_str_ = first_stmt;
    my_session = ctx.get_my_session();
    if (NULL == my_session) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get my session", K(ret), K(ctx));
    } else if (OB_FAIL(check_alter_partition(ctx, stmt, alter_table_arg))) {
      LOG_WARN("check alter partition failed", K(ret));
    } else if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
      ret = OB_NOT_INIT;
      LOG_WARN("get task executor context failed");
    } else if (OB_FAIL(set_index_arg_list(ctx, stmt))) {
      LOG_WARN("fail to set index_arg_list", K(ret));
    } else if (OB_FAIL(task_exec_ctx->get_common_rpc(common_rpc_proxy))) {
      LOG_WARN("get common rpc proxy failed", K(ret));
    } else if (OB_ISNULL(common_rpc_proxy)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("common rpc proxy should not be null", K(ret));
    } else if (OB_ISNULL(sql_proxy)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sql_proxy is null", K(ret));
    } else if (OB_FAIL(ObResolverUtils::check_sync_ddl_user(my_session, is_sync_ddl_user))) {
      LOG_WARN("Failed to check sync_dll_user", K(ret));
    } else if (OB_INVALID_ID == alter_table_arg.session_id_ && 0 != my_session->get_sessid_for_table() &&
               FALSE_IT(alter_table_arg.session_id_ = my_session->get_sessid_for_table())) {
      // impossible
    } else {
      if (OB_SUCC(ret) && !is_sync_ddl_user && 1 == alter_table_arg.foreign_key_arg_list_.count()) {
        if ((!alter_table_arg.foreign_key_arg_list_.at(0).is_modify_fk_state_ &&
                alter_table_arg.foreign_key_arg_list_.at(0).validate_flag_) ||
            (alter_table_arg.foreign_key_arg_list_.at(0).is_modify_validate_flag_ &&
                alter_table_arg.foreign_key_arg_list_.at(0).validate_flag_)) {
          need_modify_fk_validate = true;
          ObString empty_stmt;
          alter_table_arg.ddl_stmt_str_ = empty_stmt;
          alter_table_arg.foreign_key_arg_list_.at(0).validate_flag_ = false;
        }
      }
      if (OB_SUCC(ret) && (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_3100)) {
        if (OB_FAIL(
                alter_table_rpc_v2(alter_table_arg, res, allocator, common_rpc_proxy, my_session, is_sync_ddl_user))) {
          LOG_WARN("Failed to alter table rpc v2", K(ret));
        }
      } else if (OB_SUCC(ret) && (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_3100)) {
        if (OB_FAIL(
                alter_table_rpc_v1(alter_table_arg, res, allocator, common_rpc_proxy, my_session, is_sync_ddl_user))) {
          LOG_WARN("Failed to alter table rpc v1", K(ret));
        }
      }
    }
  }
  bool is_oracle_mode = false;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(alter_table_arg.alter_table_schema_.check_if_oracle_compat_mode(is_oracle_mode))) {
      LOG_WARN("fail to check if tenant mode is oracle mode", K(ret));
    }
  }
  if (OB_SUCC(ret) && is_oracle_mode && !is_sync_ddl_user &&
      (obrpc::ObAlterTableArg::ADD_CONSTRAINT == alter_table_arg.alter_constraint_type_ ||
          (obrpc::ObAlterTableArg::ALTER_CONSTRAINT_STATE == alter_table_arg.alter_constraint_type_))) {
    common::ObCommonSqlProxy* user_sql_proxy;
    ObOracleSqlProxy oracle_sql_proxy;
    ObTableSchema::const_constraint_iterator iter = alter_table_arg.alter_table_schema_.constraint_begin();
    bool is_data_valid = false;
    if (OB_ISNULL(ctx.get_sql_proxy())) {
      LOG_WARN("sql_proxy is null", K(ret), K(ctx.get_sql_proxy()));
    } else if (OB_FAIL(oracle_sql_proxy.init(ctx.get_sql_proxy()->get_pool()))) {
      LOG_WARN("init oracle sql proxy failed", K(ret));
    } else if (CONSTRAINT_TYPE_CHECK != (*iter)->get_constraint_type()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("only alter table add check constraint can come here now", K(ret), K((*iter)->get_constraint_type()));
    } else if ((obrpc::ObAlterTableArg::ADD_CONSTRAINT == alter_table_arg.alter_constraint_type_ &&
                   !(*iter)->get_validate_flag()) ||
               (obrpc::ObAlterTableArg::ALTER_CONSTRAINT_STATE == alter_table_arg.alter_constraint_type_ &&
                   !(*iter)->get_is_modify_validate_flag()) ||
               (obrpc::ObAlterTableArg::ALTER_CONSTRAINT_STATE == alter_table_arg.alter_constraint_type_ &&
                   (*iter)->get_is_modify_validate_flag() && !(*iter)->get_validate_flag())) {
      // do nothing, don't check if data is valid
    } else if (OB_FAIL(refresh_schema_for_table(alter_table_arg.alter_table_schema_.get_tenant_id()))) {
      LOG_WARN("refresh_schema_for_table failed", K(ret));
    } else {
      user_sql_proxy = &oracle_sql_proxy;
      if (OB_FAIL(check_check_constraint_data_validity(ctx,
              alter_table_arg,
              user_sql_proxy,
              res.schema_version_,
              (*iter)->get_check_expr_str(),
              is_data_valid))) {
        LOG_WARN("failed to check check constraint data validity", K(ret), K(alter_table_arg), K(res.schema_version_));
      } else if (!is_data_valid) {
        const ObString& origin_database_name = alter_table_arg.alter_table_schema_.get_origin_database_name();
        if (origin_database_name.empty() || (*iter)->get_check_expr_str().empty()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("database name or cst name is null",
              K(ret),
              K(origin_database_name),
              K((*iter)->get_check_expr_str().empty()));
        } else {
          ret = OB_ERR_ADD_CHECK_CONSTRAINT_VIOLATED;
          LOG_USER_ERROR(OB_ERR_ADD_CHECK_CONSTRAINT_VIOLATED,
              origin_database_name.length(),
              origin_database_name.ptr(),
              (*iter)->get_constraint_name_str().length(),
              (*iter)->get_constraint_name_str().ptr());
        }
      }
    }
    if (OB_FAIL(ret) && !is_data_valid) {
      int tmp_ret = ret;
      obrpc::ObAlterTableRes tmp_res;
      ret = OB_SUCCESS;
      if (obrpc::ObAlterTableArg::ADD_CONSTRAINT == alter_table_arg.alter_constraint_type_) {
        (*iter)->set_constraint_id(res.constriant_id_);
        if (OB_FAIL(set_drop_constraint_ddl_stmt_str(alter_table_arg, allocator))) {
          LOG_WARN("fail to set drop constraint ddl_stmt_str", K(ret));
        } else {
          alter_table_arg.alter_constraint_type_ = obrpc::ObAlterTableArg::DROP_CONSTRAINT;
        }
      } else {
        if ((*iter)->get_is_modify_enable_flag()) {
          (*iter)->set_enable_flag(!(*iter)->get_enable_flag());
        }
        if ((*iter)->get_is_modify_rely_flag()) {
          (*iter)->set_rely_flag(!(*iter)->get_rely_flag());
        }
        if ((*iter)->get_is_modify_validate_flag()) {
          (*iter)->set_validate_flag(!(*iter)->get_validate_flag());
        }
        alter_table_arg.alter_constraint_type_ = obrpc::ObAlterTableArg::ALTER_CONSTRAINT_STATE;
        if (OB_FAIL(set_alter_constraint_ddl_stmt_str_for_check(alter_table_arg, allocator))) {
          LOG_WARN("fail to set alter constraint ddl_stmt_str", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(common_rpc_proxy->alter_table(alter_table_arg, tmp_res))) {
        LOG_WARN("rpc proxy alter table failed", K(ret), "dst", common_rpc_proxy->get_server());
      } else {
        ret = tmp_ret;
      }
    }
  }
  if (OB_SUCC(ret) && !is_sync_ddl_user && 1 == alter_table_arg.foreign_key_arg_list_.count()) {
    if (!need_modify_fk_validate) {
      // do nothing, don't check if data is valid
    } else {
      common::ObCommonSqlProxy* user_sql_proxy;
      ObOracleSqlProxy oracle_sql_proxy;
      obrpc::ObCreateForeignKeyArg fk_arg = alter_table_arg.foreign_key_arg_list_.at(0);
      bool is_data_valid = false;
      if (OB_ISNULL(ctx.get_sql_proxy())) {
        LOG_WARN("sql_proxy is null", K(ret), K(ctx.get_sql_proxy()));
      } else if (OB_FAIL(oracle_sql_proxy.init(ctx.get_sql_proxy()->get_pool()))) {
        LOG_WARN("init oracle sql proxy failed", K(ret));
      } else if (OB_FAIL(refresh_schema_for_table(alter_table_arg.alter_table_schema_.get_tenant_id()))) {
        LOG_WARN("refresh_schema_for_table failed", K(ret));
      } else {
        if (is_oracle_mode) {
          user_sql_proxy = &oracle_sql_proxy;
        } else {
          user_sql_proxy = sql_proxy;  // mysql mode
        }
        if (OB_FAIL(check_fk_constraint_data_validity(
                ctx, alter_table_arg, user_sql_proxy, res.schema_version_, is_data_valid))) {
          LOG_WARN("failed to check fk constraint data validity", K(ret), K(alter_table_arg), K(res.schema_version_));
        } else if (!is_data_valid) {
          ret = OB_ERR_ORPHANED_CHILD_RECORD_EXISTS;
          LOG_USER_ERROR(OB_ERR_ORPHANED_CHILD_RECORD_EXISTS,
              alter_table_arg.alter_table_schema_.get_origin_database_name().length(),
              alter_table_arg.alter_table_schema_.get_origin_database_name().ptr(),
              fk_arg.foreign_key_name_.length(),
              fk_arg.foreign_key_name_.ptr());
        }
      }
      if (OB_FAIL(ret) && !is_data_valid) {
        int tmp_ret = ret;
        ret = OB_SUCCESS;
        obrpc::ObAlterTableRes tmp_res;
        obrpc::ObDropForeignKeyArg drop_foreign_key_arg;
        if (!alter_table_arg.foreign_key_arg_list_.at(0).is_modify_fk_state_) {
          drop_foreign_key_arg.index_action_type_ = obrpc::ObIndexArg::DROP_FOREIGN_KEY;
          drop_foreign_key_arg.foreign_key_name_ = fk_arg.foreign_key_name_;
          if (OB_FAIL(alter_table_arg.index_arg_list_.push_back(&drop_foreign_key_arg))) {
            LOG_WARN("fail to push back arg to index_arg_list", K(ret));
          } else if (OB_FAIL(set_drop_constraint_ddl_stmt_str(alter_table_arg, allocator))) {
            LOG_WARN("fail to set drop constraint ddl_stmt_str", K(ret));
          } else {
            alter_table_arg.foreign_key_arg_list_.reset();
          }
        } else {
          if (alter_table_arg.foreign_key_arg_list_.at(0).is_modify_rely_flag_) {
            alter_table_arg.foreign_key_arg_list_.at(0).rely_flag_ =
                !alter_table_arg.foreign_key_arg_list_.at(0).rely_flag_;
          }
          if (alter_table_arg.foreign_key_arg_list_.at(0).is_modify_enable_flag_) {
            alter_table_arg.foreign_key_arg_list_.at(0).enable_flag_ =
                !alter_table_arg.foreign_key_arg_list_.at(0).enable_flag_;
          }
          if (alter_table_arg.foreign_key_arg_list_.at(0).is_modify_validate_flag_) {
            alter_table_arg.foreign_key_arg_list_.at(0).validate_flag_ =
                !alter_table_arg.foreign_key_arg_list_.at(0).validate_flag_;
          }
          if (OB_FAIL(set_alter_constraint_ddl_stmt_str_for_fk(alter_table_arg, allocator))) {
            LOG_WARN("fail to set alter constraint ddl_stmt_str", K(ret));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(common_rpc_proxy->alter_table(alter_table_arg, tmp_res))) {
          LOG_WARN("alter table failed", K(ret));
        }
        alter_table_arg.index_arg_list_.reset();
        ret = tmp_ret;
      } else if (OB_SUCC(ret) && need_modify_fk_validate) {
        obrpc::ObAlterTableRes tmp_res;
        alter_table_arg.ddl_stmt_str_ = first_stmt;
        alter_table_arg.foreign_key_arg_list_.at(0).is_modify_fk_state_ = true;
        alter_table_arg.foreign_key_arg_list_.at(0).is_modify_validate_flag_ = true;
        alter_table_arg.foreign_key_arg_list_.at(0).validate_flag_ = true;
        if (OB_FAIL(common_rpc_proxy->alter_table(alter_table_arg, tmp_res))) {
          LOG_WARN("alter table modify fk validate failed", K(ret), K(alter_table_arg));
        }
      }
    }
  }

  return ret;
}

int ObAlterTableExecutor::set_alter_col_nullable_ddl_stmt_str(
    obrpc::ObAlterTableArg& alter_table_arg, common::ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;
  AlterTableSchema& alter_table_schema = alter_table_arg.alter_table_schema_;
  ObString column_name;
  const ObColumnSchemaV2* col_schema = NULL;
  char* buf = NULL;
  int64_t buf_len = OB_MAX_SQL_LENGTH;
  int64_t pos = 0;

  if (alter_table_schema.get_column_count() != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column count != 1", K(ret), K(alter_table_schema.get_column_count()));
  } else if (OB_ISNULL(col_schema = alter_table_schema.get_column_schema_by_idx(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("col_schema is null", K(ret));
  } else {
    column_name = col_schema->get_column_name_str();
  }
  if (OB_FAIL(ret)) {
  } else if (column_name.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cst_name is empty", K(ret), K(column_name));
  } else if (OB_ISNULL(buf = static_cast<char*>(allocator.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret), K(OB_MAX_SQL_LENGTH));
  } else if (OB_FAIL(databuff_printf(buf,
                 buf_len,
                 pos,
                 "ALTER TABLE \"%.*s\".\"%.*s\" MODIFY COLUMN %.*s NULL",
                 alter_table_schema.get_origin_database_name().length(),
                 alter_table_schema.get_origin_database_name().ptr(),
                 alter_table_schema.get_origin_table_name().length(),
                 alter_table_schema.get_origin_table_name().ptr(),
                 column_name.length(),
                 column_name.ptr()))) {
    LOG_WARN("fail to print ddl_stmt_str for rollback", K(ret));
  } else {
    alter_table_arg.ddl_stmt_str_.assign_ptr(buf, static_cast<int32_t>(pos));
  }

  return ret;
}

int ObAlterTableExecutor::set_alter_constraint_ddl_stmt_str_for_fk(
    obrpc::ObAlterTableArg& alter_table_arg, common::ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;
  AlterTableSchema& alter_table_schema = alter_table_arg.alter_table_schema_;
  ObString cst_name;
  char* buf = NULL;
  int64_t buf_len = OB_MAX_SQL_LENGTH;
  int64_t pos = 0;
  obrpc::ObCreateForeignKeyArg fk_arg = alter_table_arg.foreign_key_arg_list_.at(0);
  cst_name = fk_arg.foreign_key_name_;
  if (OB_FAIL(ret)) {
  } else if (cst_name.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cst_name is empty", K(ret), K(cst_name));
  } else if (OB_ISNULL(buf = static_cast<char*>(allocator.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret), K(OB_MAX_SQL_LENGTH));
  } else if (OB_FAIL(databuff_printf(buf,
                 buf_len,
                 pos,
                 "ALTER TABLE \"%.*s\".\"%.*s\" MODIFY CONSTRAINT %.*s ",
                 alter_table_schema.get_origin_database_name().length(),
                 alter_table_schema.get_origin_database_name().ptr(),
                 alter_table_schema.get_origin_table_name().length(),
                 alter_table_schema.get_origin_table_name().ptr(),
                 cst_name.length(),
                 cst_name.ptr()))) {
    LOG_WARN("fail to print ddl_stmt_str for rollback", K(ret));
  } else if (fk_arg.is_modify_rely_flag_ &&
             OB_FAIL(databuff_printf(buf, buf_len, pos, fk_arg.rely_flag_ ? "RELY " : "NORELY "))) {
    LOG_WARN("fail to print rely flag for rollback", K(ret));
  } else if (fk_arg.is_modify_enable_flag_ &&
             OB_FAIL(databuff_printf(buf, buf_len, pos, fk_arg.enable_flag_ ? "ENABLE " : "DISABLE "))) {
    LOG_WARN("fail to print enable flag for rollback", K(ret));
  } else if (fk_arg.is_modify_validate_flag_ &&
             OB_FAIL(databuff_printf(buf, buf_len, pos, fk_arg.validate_flag_ ? "VALIDATE " : "NOVALIDATE "))) {
    LOG_WARN("fail to print enable flag for rollback", K(ret));
  } else {
    alter_table_arg.ddl_stmt_str_.assign_ptr(buf, static_cast<int32_t>(pos));
  }

  return ret;
}

int ObAlterTableExecutor::set_alter_constraint_ddl_stmt_str_for_check(
    obrpc::ObAlterTableArg& alter_table_arg, common::ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;
  AlterTableSchema& alter_table_schema = alter_table_arg.alter_table_schema_;
  ObString cst_name;
  char* buf = NULL;
  int64_t buf_len = OB_MAX_SQL_LENGTH;
  int64_t pos = 0;
  ObTableSchema::const_constraint_iterator iter = alter_table_arg.alter_table_schema_.constraint_begin();
  cst_name = (*iter)->get_constraint_name_str();
  if (OB_FAIL(ret)) {
  } else if (cst_name.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cst_name is empty", K(ret), K(cst_name));
  } else if (OB_ISNULL(buf = static_cast<char*>(allocator.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret), K(OB_MAX_SQL_LENGTH));
  } else if (OB_FAIL(databuff_printf(buf,
                 buf_len,
                 pos,
                 "ALTER TABLE \"%.*s\".\"%.*s\" MODIFY CONSTRAINT %.*s ",
                 alter_table_schema.get_origin_database_name().length(),
                 alter_table_schema.get_origin_database_name().ptr(),
                 alter_table_schema.get_origin_table_name().length(),
                 alter_table_schema.get_origin_table_name().ptr(),
                 cst_name.length(),
                 cst_name.ptr()))) {
    LOG_WARN("fail to print ddl_stmt_str for rollback", K(ret));
  } else if ((*iter)->get_is_modify_rely_flag() &&
             OB_FAIL(databuff_printf(buf, buf_len, pos, (*iter)->get_rely_flag() ? "RELY " : "NORELY "))) {
    LOG_WARN("fail to print rely flag for rollback", K(ret));
  } else if ((*iter)->get_is_modify_enable_flag() &&
             OB_FAIL(databuff_printf(buf, buf_len, pos, (*iter)->get_enable_flag() ? "ENABLE " : "DISABLE "))) {
    LOG_WARN("fail to print enable flag for rollback", K(ret));
  } else if ((*iter)->get_is_modify_validate_flag() &&
             OB_FAIL(databuff_printf(buf, buf_len, pos, (*iter)->get_validate_flag() ? "VALIDATE " : "NOVALIDATE "))) {
    LOG_WARN("fail to print enable flag for rollback", K(ret));
  } else {
    alter_table_arg.ddl_stmt_str_.assign_ptr(buf, static_cast<int32_t>(pos));
  }

  return ret;
}

int ObAlterTableExecutor::set_drop_constraint_ddl_stmt_str(
    obrpc::ObAlterTableArg& alter_table_arg, common::ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;
  AlterTableSchema& alter_table_schema = alter_table_arg.alter_table_schema_;
  ObString cst_name;
  char* buf = NULL;
  int64_t buf_len = OB_MAX_SQL_LENGTH;
  int64_t pos = 0;

  if (obrpc::ObAlterTableArg::ADD_CONSTRAINT == alter_table_arg.alter_constraint_type_) {
    ObTableSchema::const_constraint_iterator iter = alter_table_arg.alter_table_schema_.constraint_begin();
    cst_name = (*iter)->get_constraint_name_str();
  } else if (1 == alter_table_arg.foreign_key_arg_list_.count() &&
             !alter_table_arg.foreign_key_arg_list_.at(0).is_modify_enable_flag_) {
    obrpc::ObCreateForeignKeyArg fk_arg = alter_table_arg.foreign_key_arg_list_.at(0);
    cst_name = fk_arg.foreign_key_name_;
  }
  if (OB_FAIL(ret)) {
  } else if (cst_name.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cst_name is empty", K(ret), K(cst_name));
  } else if (OB_ISNULL(buf = static_cast<char*>(allocator.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret), K(OB_MAX_SQL_LENGTH));
  } else if (OB_FAIL(databuff_printf(buf,
                 buf_len,
                 pos,
                 share::is_oracle_mode() ? "ALTER TABLE \"%.*s\".\"%.*s\" DROP CONSTRAINT \"%.*s\""
                                         : "ALTER TABLE `%.*s`.`%.*s` DROP FOREIGN KEY `%.*s`",
                 alter_table_schema.get_origin_database_name().length(),
                 alter_table_schema.get_origin_database_name().ptr(),
                 alter_table_schema.get_origin_table_name().length(),
                 alter_table_schema.get_origin_table_name().ptr(),
                 cst_name.length(),
                 cst_name.ptr()))) {
    LOG_WARN("fail to print ddl_stmt_str for rollback", K(ret));
  } else {
    alter_table_arg.ddl_stmt_str_.assign_ptr(buf, static_cast<int32_t>(pos));
  }

  return ret;
}

int ObAlterTableExecutor::init_build_snapshot_ctx(const common::ObIArray<PartitionServer>& partition_leader_array,
    common::ObIArray<int64_t>& invalid_snapshot_id_array, common::ObIArray<int64_t>& snapshot_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(partition_leader_array.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    invalid_snapshot_id_array.reset();
    snapshot_array.reset();
    const int64_t array_count = partition_leader_array.count();
    for (int64_t index = 0; OB_SUCC(ret) && index < array_count; ++index) {
      if (OB_FAIL(invalid_snapshot_id_array.push_back(index))) {
        LOG_WARN("fail to push back", K(ret));
      } else if (OB_FAIL(snapshot_array.push_back(0))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
  }
  return ret;
}

int ObAlterTableExecutor::generate_original_table_partition_leader_array(ObExecContext& ctx,
    share::schema::ObSchemaGetterGuard& schema_guard, const share::schema::ObTableSchema* data_schema,
    common::ObIArray<PartitionServer>& partition_leader_array)
{
  int ret = OB_SUCCESS;
  ObPartitionTableOperator* pt_operator;
  UNUSED(schema_guard);

  if (OB_ISNULL(data_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data table schema is null", K(ret));
  } else if (OB_ISNULL(ctx.get_sql_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql ctx is null", K(ret));
  } else if (OB_ISNULL(
                 pt_operator = const_cast<ObPartitionTableOperator*>(ctx.get_sql_ctx()->partition_table_operator_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition_table_operator is null", K(ret));
  } else {
    bool check_dropped_schema = false;
    ObTablePartitionKeyIter partition_key_iter(*data_schema, check_dropped_schema);
    ObPartitionKey pkey;
    ObPartitionKey phy_pkey;
    while (OB_SUCC(ret) && OB_SUCC(partition_key_iter.next_partition_key_v2(pkey))) {
      ObArenaAllocator allocator(ObModIds::OB_RS_PARTITION_TABLE_TEMP);
      ObPartitionInfo info;
      info.set_allocator(&allocator);
      PartitionServer partition_leader;
      const ObPartitionReplica* leader_replica = NULL;
      if (OB_UNLIKELY(!pkey.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pkey is invalid", K(ret));
      } else if (data_schema->is_binding_table()) {
        if (OB_FAIL(data_schema->get_pg_key(pkey, phy_pkey))) {
          LOG_WARN("fail to get pg key", K(ret), K(pkey));
        }
      } else {
        phy_pkey = pkey;
      }
      if (OB_FAIL(ret)) {
        // failed
      } else if (OB_FAIL(pt_operator->get(phy_pkey.get_table_id(), phy_pkey.get_partition_id(), info))) {
        LOG_WARN("fail to get partition info", K(phy_pkey));
      } else if (OB_FAIL(info.find_leader_v2(leader_replica))) {
        LOG_WARN("fail to find leader", K(ret));
      } else if (OB_UNLIKELY(NULL == leader_replica)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("leader replica ptr is null", K(ret));
      } else if (partition_leader.set(
                     leader_replica->server_, pkey.get_table_id(), pkey.get_partition_id(), pkey.get_partition_cnt())) {
        LOG_WARN("fail to set partition leader", K(ret), K(*leader_replica));
      } else if (OB_FAIL(partition_leader_array.push_back(partition_leader))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get leader array", K(ret));
    }
    if (OB_SUCC(ret)) {
      if (partition_leader_array.count() != data_schema->get_all_part_num()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("part num and leader cnt not match",
            K(ret),
            "table_id",
            data_schema->get_table_id(),
            "left_cnt",
            partition_leader_array.count(),
            "right_cnt",
            data_schema->get_all_part_num());
      }
    }
  }

  return ret;
}

int ObAlterTableExecutor::update_partition_leader_array(common::ObIArray<PartitionServer>& partition_leader_array,
    const common::ObIArray<int>& ret_code_array, const common::ObIArray<int64_t>& invalid_snapshot_id_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ret_code_array.count() != invalid_snapshot_id_array.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("array count not match",
        K(ret),
        "left array count",
        ret_code_array.count(),
        "right array count",
        invalid_snapshot_id_array.count());
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ret_code_array.count(); ++i) {
      int ret_code = ret_code_array.at(i);
      if (OB_SUCCESS == ret_code) {
        // already got snapshot
      } else if (OB_EAGAIN == ret_code || OB_TIMEOUT == ret_code) {
        // transaction on the partition not finish, wait and retry
      } else if (OB_NOT_MASTER == ret_code || OB_PARTITION_NOT_EXIST == ret_code) {
        int64_t part_array_idx = invalid_snapshot_id_array.at(i);
        if (part_array_idx >= invalid_snapshot_id_array.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("part array id unexpected",
              K(ret),
              K(part_array_idx),
              "invalid snapshot id array count",
              invalid_snapshot_id_array.count());
        } else {
          const ObPartitionKey& pkey = partition_leader_array.at(part_array_idx).pkey_;
          ObArenaAllocator allocator(ObModIds::OB_RS_PARTITION_TABLE_TEMP);
          ObPartitionInfo info;
          const ObPartitionReplica* leader_replica = NULL;
          ObReplicaFilterHolder filter;
          info.set_allocator(&allocator);
          if (OB_FAIL(GCTX.pt_operator_->get(pkey.get_table_id(), pkey.get_partition_id(), info))) {
            LOG_WARN("fail to get partition info", K(ret), K(pkey));
          } else if (OB_FAIL(filter.set_replica_status(REPLICA_STATUS_NORMAL))) {
            LOG_WARN("fail to set replica status", K(ret));
          } else if (OB_FAIL(filter.set_in_member_list())) {
            LOG_WARN("fail to set in member list", K(ret));
          } else if (OB_FAIL(info.filter(filter))) {
            LOG_WARN("fail to do filter", K(ret));
          } else if (OB_FAIL(info.find_leader_v2(leader_replica))) {
            if (OB_ENTRY_NOT_EXIST == ret) {
              ret = OB_SUCCESS;
            } else {
              LOG_WARN("fail to get leader", K(ret));
            }
          } else if (OB_UNLIKELY(NULL == leader_replica)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("leader replica ptr is null", K(ret));
          } else if (OB_FAIL(partition_leader_array.at(part_array_idx).set_server(leader_replica->server_))) {
            LOG_WARN("fail to set server", K(ret));
          } else {
          }  // no more to do
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected ret code", K(ret));
      }
    }
  }
  return ret;
}

int ObAlterTableExecutor::pick_build_snapshot(const common::ObIArray<int64_t>& snapshot_array, int64_t& snapshot)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(snapshot_array.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    snapshot = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < snapshot_array.count(); ++i) {
      const int64_t this_snapshot = snapshot_array.at(i);
      if (this_snapshot > snapshot) {
        snapshot = this_snapshot;
      }
    }
  }
  return ret;
}

template <typename PROXY>
int ObAlterTableExecutor::update_build_snapshot_ctx(PROXY& proxy, const common::ObIArray<int>& ret_code_array,
    common::ObIArray<int64_t>& invalid_snapshot_id_array, common::ObIArray<int64_t>& snapshot_array)
{
  int ret = OB_SUCCESS;
  if (invalid_snapshot_id_array.count() != ret_code_array.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("array count not match",
        K(ret),
        "left array count",
        invalid_snapshot_id_array.count(),
        "right array count",
        ret_code_array.count());
  } else if (proxy.get_results().count() != ret_code_array.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("array count not match",
        K(ret),
        "left array count",
        proxy.get_results().count(),
        "right array count",
        ret_code_array.count());
  } else {
    common::ObArray<int64_t> tmp_invalid_snapshot_id_array;
    for (int64_t i = 0; OB_SUCC(ret) && i < ret_code_array.count(); ++i) {
      int ret_code = ret_code_array.at(i);
      int64_t snapshot_array_idx = invalid_snapshot_id_array.at(i);
      if (OB_SUCCESS == ret_code) {
        if (snapshot_array_idx >= snapshot_array.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("snapshot array idx unexpected",
              K(ret),
              K(snapshot_array_idx),
              "snapshot_array count",
              snapshot_array.count());
        } else if (NULL == proxy.get_results().at(i)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("result ptr is null", K(ret));
        } else {
          snapshot_array.at(snapshot_array_idx) = proxy.get_results().at(i)->snapshot_;
        }
      } else if (OB_EAGAIN == ret_code || OB_NOT_MASTER == ret_code || OB_PARTITION_NOT_EXIST == ret_code ||
                 OB_TIMEOUT == ret_code) {
        if (OB_FAIL(tmp_invalid_snapshot_id_array.push_back(snapshot_array_idx))) {
          LOG_WARN("fail to push back", K(ret));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("rpc invoking failed", K(ret), K(ret_code));
      }
    }
    if (OB_FAIL(ret)) {
    } else {
      invalid_snapshot_id_array.reset();
      if (OB_FAIL(invalid_snapshot_id_array.assign(tmp_invalid_snapshot_id_array))) {
        LOG_WARN("fail to assign array", K(ret));
      }
    }
  }
  return ret;
}

template <typename PROXY, typename ARG>
int ObAlterTableExecutor::do_get_associated_snapshot(PROXY& rpc_proxy, ARG& rpc_arg, int64_t schema_version,
    const share::schema::ObTableSchema* table_schema, common::ObIArray<PartitionServer>& partition_leader_array,
    int64_t& snapshot)
{
  int ret = OB_SUCCESS;
  common::ObArray<int64_t> snapshot_array;
  common::ObArray<int64_t> invalid_snapshot_id_array;

  if (OB_UNLIKELY(NULL == table_schema || partition_leader_array.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(table_schema), "array_cnt", partition_leader_array.count());
  } else if (OB_FAIL(init_build_snapshot_ctx(partition_leader_array, invalid_snapshot_id_array, snapshot_array))) {
    LOG_WARN("fail to init invalid snapshot id array", K(ret));
  } else {
    bool got_snapshot = false;
    common::ObArray<int> ret_code_array;
    int64_t timeout = table_schema->get_all_part_num() * TIME_INTERVAL_PER_PART_US;
    const int64_t max_timeout = MAX_WAIT_CHECK_SCHEMA_VERSION_INTERVAL_US;
    const int64_t min_timeout = MIN_WAIT_CHECK_SCHEMA_VERSION_INTERVAL_US;
    timeout = std::min(timeout, max_timeout);
    timeout = std::max(timeout, min_timeout);
    int64_t timeout_ts = timeout + ObTimeUtility::current_time();
    rootserver::ObGlobalIndexTask task;
    task.schema_version_ = schema_version;
    while (OB_SUCC(ret) && !got_snapshot && ObTimeUtility::current_time() < timeout_ts) {
      rpc_proxy.reuse();
      ret_code_array.reset();
      for (int64_t i = 0; OB_SUCC(ret) && i < invalid_snapshot_id_array.count(); ++i) {
        int64_t index = invalid_snapshot_id_array.at(i);
        rpc_arg.reuse();
        if (index >= partition_leader_array.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("index unexpected", K(ret), K(index), "array count", partition_leader_array.count());
        } else if (OB_FAIL(rpc_arg.build(&task, partition_leader_array.at(index).pkey_))) {
          LOG_WARN("fail to build rpc arg", K(ret));
        } else if (OB_FAIL(rpc_proxy.call(
                       partition_leader_array.at(index).server_, GET_ASSOCIATED_SNAPSHOT_TIMEOUT, rpc_arg))) {
          LOG_WARN("fail to call rpc", K(ret));
        }
      }
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = rpc_proxy.wait_all(ret_code_array))) {
        LOG_WARN("rpc_proxy wait failed", K(ret), K(tmp_ret));
        ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
      } else if (OB_SUCC(ret)) {  // wait_all SUCC and the above process SUCC
        common::ObArray<int64_t> pre_invalid_snapshot_id_array;
        if (OB_FAIL(pre_invalid_snapshot_id_array.assign(invalid_snapshot_id_array))) {
          LOG_WARN("fail to assign invalid snapshot id array", K(ret));
        } else if (OB_FAIL(update_build_snapshot_ctx(
                       rpc_proxy, ret_code_array, invalid_snapshot_id_array, snapshot_array))) {
          LOG_WARN("fail to update build snapshot ctx", K(ret));
        } else if (invalid_snapshot_id_array.count() <= 0) {
          if (OB_FAIL(pick_build_snapshot(snapshot_array, snapshot))) {
            LOG_WARN("fail to pick snapshot array", K(ret));
          } else {
            LOG_INFO("get snapshot", K(ret));
            got_snapshot = true;
          }
        } else {
          int64_t regular_wait_us = WAIT_US;
          int64_t sleep_us = timeout_ts - ObTimeUtility::current_time();
          sleep_us = std::max(sleep_us, 1L);
          sleep_us = std::min(sleep_us, regular_wait_us);
          usleep(static_cast<uint32_t>(sleep_us));
          if (OB_FAIL(update_partition_leader_array(
                  partition_leader_array, ret_code_array, pre_invalid_snapshot_id_array))) {
            LOG_WARN("fail to update partition leader array", K(ret));
          }
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (!got_snapshot) {
      ret = OB_TIMEOUT;
      LOG_WARN("get snapshot timeout", K(ret));
    }
  }

  return ret;
}

int ObAlterTableExecutor::get_constraint_check_snapshot(ObExecContext& ctx, int64_t schema_version,
    share::schema::ObSchemaGetterGuard& schema_guard, const uint64_t table_id, int64_t& snapshot)
{
  int ret = OB_SUCCESS;

  common::ObArray<PartitionServer> partition_leader_array;
  const share::schema::ObTableSchema* data_schema = NULL;
  if (OB_UNLIKELY(NULL == GCTX.srv_rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rpc_proxy_ ptr is null", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(table_id, data_schema))) {
    LOG_WARN("fail to get table schema", K(ret), "table_id", table_id);
  } else if (OB_UNLIKELY(NULL == data_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("data schema not exist", K(ret), "table_id", table_id);
  } else if (OB_FAIL(generate_original_table_partition_leader_array(
                 ctx, schema_guard, data_schema, partition_leader_array))) {
    LOG_WARN("fail to generate original leader array", K(ret));
  } else {
    rootserver::ObCheckSchemaVersionElapsedProxy proxy(
        *GCTX.srv_rpc_proxy_, &obrpc::ObSrvRpcProxy::check_schema_version_elapsed);
    obrpc::ObCheckSchemaVersionElapsedArg arg;
    if (OB_FAIL(
            do_get_associated_snapshot(proxy, arg, schema_version, data_schema, partition_leader_array, snapshot))) {
      LOG_WARN("fail to do get snapshot", K(ret));
    } else {
    }  // no more to do
  }

  return ret;
}

int ObAlterTableExecutor::check_data_validity_for_check_by_inner_sql(
    const share::schema::AlterTableSchema& alter_table_schema, ObCommonSqlProxy* sql_proxy,
    const ObString& check_expr_str, bool& is_data_valid)
{
  int ret = OB_SUCCESS;
  is_data_valid = false;
  ObSqlString sql_string;
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    common::sqlclient::ObMySQLResult* result = NULL;

    if (OB_SUCC(ret)) {
      if (check_expr_str.empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("check_expr_str is empty", K(ret));
      } else if (OB_FAIL(sql_string.assign_fmt("SELECT 1 FROM \"%.*s\".\"%.*s\" WHERE NOT (%.*s) AND ROWNUM = 1",
                     static_cast<int>(alter_table_schema.get_origin_database_name().length()),
                     alter_table_schema.get_origin_database_name().ptr(),
                     static_cast<int>(alter_table_schema.get_origin_table_name().length()),
                     alter_table_schema.get_origin_table_name().ptr(),
                     static_cast<int>(check_expr_str.length()),
                     check_expr_str.ptr()))) {
        LOG_WARN("fail to assign format", K(ret));
      } else if (OB_FAIL(sql_proxy->read(res, alter_table_schema.get_tenant_id(), sql_string.ptr()))) {
        LOG_WARN("execute sql failed", K(ret), K(sql_string.ptr()));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("execute sql failed", K(ret), K(alter_table_schema.get_tenant_id()), K(sql_string));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          is_data_valid = true;
        } else {
          LOG_WARN("iterate next result fail", K(ret), K(sql_string));
        }
      } else {
        LOG_WARN("old data is not valid for this new check constraint", K(ret), K(is_data_valid), K(sql_string));
      }
    }
  }
  return ret;
}

int ObAlterTableExecutor::check_check_constraint_data_validity(ObExecContext& ctx,
    const obrpc::ObAlterTableArg& alter_table_arg, ObCommonSqlProxy* sql_proxy, int64_t schema_version,
    const ObString& check_expr_str, bool& is_data_valid)
{
  int ret = OB_SUCCESS;
  is_data_valid = false;
  int64_t snapshot = 0;
  ObSchemaGetterGuard schema_guard;
  const AlterTableSchema& alter_table_schema = alter_table_arg.alter_table_schema_;
  const uint64_t tenant_id = alter_table_schema.get_tenant_id();
  const observer::ObGlobalContext& gctx = observer::ObServer::get_instance().get_gctx();
  const ObString& origin_database_name = alter_table_schema.get_origin_database_name();
  const ObString& origin_table_name = alter_table_schema.get_origin_table_name();
  const ObTableSchema* orig_table_schema = NULL;

  if (OB_ISNULL(gctx.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, schema service must not be NULL", K(ret));
  } else if (OB_FAIL(gctx.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", K(ret));
  }
  if (OB_SUCC(ret)) {
    if (origin_database_name.empty() || origin_table_name.empty()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("database name or table name is null",
          K(ret),
          K(alter_table_schema),
          K(origin_database_name),
          K(origin_table_name));
    } else {
      if (OB_FAIL(schema_guard.get_table_schema(
              tenant_id, origin_database_name, origin_table_name, false, orig_table_schema))) {
        LOG_WARN("fail to get table schema", K(ret), K(tenant_id), K(origin_database_name), K(origin_table_name));
      } else if (OB_ISNULL(orig_table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(ret), K(orig_table_schema));
      } else {
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(get_constraint_check_snapshot(
            ctx, schema_version, schema_guard, orig_table_schema->get_table_id(), snapshot))) {
      LOG_WARN("fail to get global index build snapshot", K(ret));
    } else if (OB_FAIL(check_data_validity_for_check_by_inner_sql(
                   alter_table_schema, sql_proxy, check_expr_str, is_data_valid))) {
      LOG_WARN("fail to check data validity by inner sql", K(ret));
    }
  }

  return ret;
}

int ObAlterTableExecutor::check_data_validity_for_fk_by_inner_sql(
    const share::schema::AlterTableSchema& alter_table_schema, const obrpc::ObCreateForeignKeyArg& fk_arg,
    ObCommonSqlProxy* sql_proxy, bool& is_data_valid)
{
  int ret = OB_SUCCESS;
  is_data_valid = false;
  ObSqlString sql_string;
  int64_t i = 0;
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    common::sqlclient::ObMySQLResult* result = NULL;
    // print str like "select c1, c2 from db.t2 where c1 is not null and c2 is not null minus select c3, c4 from db.t1"
    if (OB_SUCC(ret)) {
      // print "select "
      if (OB_FAIL(sql_string.assign_fmt("SELECT "))) {
        LOG_WARN("fail to assign format", K(ret));
      }
      // print "c1, "
      for (i = 0; OB_SUCC(ret) && i < fk_arg.child_columns_.count() - 1; ++i) {
        if (OB_FAIL(sql_string.append_fmt(share::is_oracle_mode() ? "\"%.*s\", " : "`%.*s`, ",
                static_cast<int>(fk_arg.child_columns_.at(i).length()),
                fk_arg.child_columns_.at(i).ptr()))) {
          LOG_WARN("fail to append format", K(ret));
        }
      }
      // print "c2 from db.t2 where "
      if (OB_SUCC(ret)) {
        if (OB_FAIL(sql_string.append_fmt(
                share::is_oracle_mode() ? "\"%.*s\" FROM \"%.*s\".\"%.*s\" WHERE " : "`%.*s` FROM `%.*s`.`%.*s` WHERE ",
                static_cast<int>(fk_arg.child_columns_.at(i).length()),
                fk_arg.child_columns_.at(i).ptr(),
                static_cast<int>(alter_table_schema.get_origin_database_name().length()),
                alter_table_schema.get_origin_database_name().ptr(),
                static_cast<int>(alter_table_schema.get_origin_table_name().length()),
                alter_table_schema.get_origin_table_name().ptr()))) {
          LOG_WARN("fail to append format", K(ret));
        }
      }
      // print "c1 is not null and "
      for (i = 0; OB_SUCC(ret) && i < fk_arg.child_columns_.count() - 1; ++i) {
        if (OB_FAIL(
                sql_string.append_fmt(share::is_oracle_mode() ? "\"%.*s\" IS NOT NULL AND " : "`%.*s` IS NOT NULL AND ",
                    static_cast<int>(fk_arg.child_columns_.at(i).length()),
                    fk_arg.child_columns_.at(i).ptr()))) {
          LOG_WARN("fail to append format", K(ret));
        }
      }
      // print "c2 is not null minus select "
      if (OB_SUCC(ret)) {
        if (OB_FAIL(sql_string.append_fmt(
                share::is_oracle_mode() ? "\"%.*s\" IS NOT NULL MINUS SELECT " : "`%.*s` IS NOT NULL MINUS SELECT ",
                static_cast<int>(fk_arg.child_columns_.at(i).length()),
                fk_arg.child_columns_.at(i).ptr()))) {
          LOG_WARN("fail to append format", K(ret));
        }
      }
      // print "c3, "
      for (i = 0; OB_SUCC(ret) && i < fk_arg.parent_columns_.count() - 1; ++i) {
        if (OB_FAIL(sql_string.append_fmt(share::is_oracle_mode() ? "\"%.*s\", " : "`%.*s`, ",
                static_cast<int>(fk_arg.parent_columns_.at(i).length()),
                fk_arg.parent_columns_.at(i).ptr()))) {
          LOG_WARN("fail to append format", K(ret));
        }
      }
      // print "c4 from db.t1"
      if (OB_SUCC(ret)) {
        if (OB_FAIL(sql_string.append_fmt(
                share::is_oracle_mode() ? "\"%.*s\" FROM \"%.*s\".\"%.*s\"" : "`%.*s` FROM `%.*s`.`%.*s`",
                static_cast<int>(fk_arg.parent_columns_.at(i).length()),
                fk_arg.parent_columns_.at(i).ptr(),
                static_cast<int>(fk_arg.parent_database_.length()),
                fk_arg.parent_database_.ptr(),
                static_cast<int>(fk_arg.parent_table_.length()),
                fk_arg.parent_table_.ptr()))) {
          LOG_WARN("fail to append format", K(ret));
        }
      }
    }
    // check data valid
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(sql_proxy->read(res, alter_table_schema.get_tenant_id(), sql_string.ptr()))) {
      LOG_WARN("execute sql failed", K(ret), K(sql_string.ptr()));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("execute sql failed", K(ret), K(alter_table_schema.get_tenant_id()), K(sql_string));
    } else if (OB_FAIL(result->next())) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        is_data_valid = true;
      } else {
        LOG_WARN("iterate next result fail", K(ret), K(sql_string));
      }
    } else {
      LOG_WARN("add fk failed, because the table has orphaned child records", K(ret), K(is_data_valid), K(sql_string));
    }
  }
  return ret;
}

int ObAlterTableExecutor::check_fk_constraint_data_validity(ObExecContext& ctx,
    const obrpc::ObAlterTableArg& alter_table_arg, ObCommonSqlProxy* sql_proxy, int64_t schema_version,
    bool& is_data_valid)
{
  int ret = OB_SUCCESS;
  is_data_valid = false;
  int64_t snapshot = 0;
  ObSchemaGetterGuard schema_guard;
  const AlterTableSchema& alter_table_schema = alter_table_arg.alter_table_schema_;
  const uint64_t tenant_id = alter_table_schema.get_tenant_id();
  const observer::ObGlobalContext& gctx = observer::ObServer::get_instance().get_gctx();
  const ObString& origin_database_name = alter_table_schema.get_origin_database_name();
  const ObString& origin_table_name = alter_table_schema.get_origin_table_name();
  const ObTableSchema* orig_table_schema = NULL;
  THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + OB_MAX_USER_SPECIFIED_TIMEOUT);
  const int64_t start_time = ObTimeUtility::current_time();

  if (OB_ISNULL(gctx.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, schema service must not be NULL", K(ret));
  } else if (OB_FAIL(gctx.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", K(ret));
  }
  if (OB_SUCC(ret)) {
    if (origin_database_name.empty() || origin_table_name.empty()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("database name or table name is null",
          K(ret),
          K(alter_table_schema),
          K(origin_database_name),
          K(origin_table_name));
    } else {
      if (OB_FAIL(schema_guard.get_table_schema(
              tenant_id, origin_database_name, origin_table_name, false, orig_table_schema))) {
        LOG_WARN("fail to get table schema", K(ret), K(tenant_id), K(origin_database_name), K(origin_table_name));
      } else if (OB_ISNULL(orig_table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(ret), K(orig_table_schema));
      } else {
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(get_constraint_check_snapshot(
            ctx, schema_version, schema_guard, orig_table_schema->get_table_id(), snapshot))) {
      LOG_WARN("fail to get snapshot", K(ret));
    } else if (OB_FAIL(check_data_validity_for_fk_by_inner_sql(
                   alter_table_schema, alter_table_arg.foreign_key_arg_list_.at(0), sql_proxy, is_data_valid))) {
      LOG_WARN("fail to check data validity by inner sql", K(ret));
    }
  }
  
  const int64_t end_time = ObTimeUtility::current_time();
  LOG_DEBUG("elapsed time for check_fk_constraint_data_validity:", K(start_time), K(end_time), K(end_time-start_time));
  return ret;
}

int ObAlterTableExecutor::check_alter_partition(
    ObExecContext& ctx, ObAlterTableStmt& stmt, const obrpc::ObAlterTableArg& arg)
{
  int ret = OB_SUCCESS;

  if (arg.is_alter_partitions_) {
    AlterTableSchema& table_schema = const_cast<AlterTableSchema&>(arg.alter_table_schema_);
    if (obrpc::ObAlterTableArg::PARTITIONED_TABLE == arg.alter_part_type_ ||
        obrpc::ObAlterTableArg::REORGANIZE_PARTITION == arg.alter_part_type_ ||
        obrpc::ObAlterTableArg::SPLIT_PARTITION == arg.alter_part_type_) {
      ObPartition** partition_array = table_schema.get_part_array();
      int64_t realy_part_num = OB_INVALID_PARTITION_ID;
      if (obrpc::ObAlterTableArg::SPLIT_PARTITION == arg.alter_part_type_) {
        realy_part_num = table_schema.get_part_option().get_part_num();
      } else {
        realy_part_num = table_schema.get_partition_num();
      }
      if (table_schema.is_range_part()) {
        if (OB_FAIL(ObPartitionExecutorUtils::set_range_part_high_bound(
                ctx, stmt::T_CREATE_TABLE, table_schema, stmt, false /*is_subpart*/))) {
          LOG_WARN("partition_array is NULL", K(ret));
        }
      } else if (table_schema.is_list_part()) {
        if (OB_ISNULL(partition_array)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL ptr", K(ret));
        } else if (OB_FAIL(ObPartitionExecutorUtils::cast_list_expr_to_obj(ctx,
                       stmt::T_CREATE_TABLE,
                       false,  // is_subpart
                       realy_part_num,
                       partition_array,
                       NULL,
                       stmt.get_part_fun_exprs(),
                       stmt.get_part_values_exprs()))) {
          LOG_WARN("partition_array is NULL", K(ret));
        }
      } else if (obrpc::ObAlterTableArg::PARTITIONED_TABLE != arg.alter_part_type_) {
        ret = OB_ERR_ONLY_ON_RANGE_LIST_PARTITION;
        LOG_WARN("only support range or list part",
            K(ret),
            K(arg.alter_part_type_),
            "partition type",
            table_schema.get_part_option().get_part_func_type());
      }
      if (OB_FAIL(ret)) {
      } else if (obrpc::ObAlterTableArg::SPLIT_PARTITION == arg.alter_part_type_) {
        const_cast<AlterTableSchema&>(table_schema).get_part_option().set_part_num(table_schema.get_partition_num());
      }
    } else if (obrpc::ObAlterTableArg::ADD_PARTITION == arg.alter_part_type_) {
      if (table_schema.is_range_part() || table_schema.is_list_part()) {
        if (OB_FAIL(ObPartitionExecutorUtils::calc_values_exprs_for_alter_table(ctx, table_schema, stmt))) {
          LOG_WARN("failed to calc values exprs for alter table", K(ret));
        }
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_WARN(OB_NOT_SUPPORTED, "add hash partition");
      }
    } else if (obrpc::ObAlterTableArg::ADD_SUB_PARTITION == arg.alter_part_type_) {
      if (table_schema.is_range_subpart()) {
        if (OB_FAIL(ObPartitionExecutorUtils::set_individual_range_part_high_bound(
                ctx, stmt::T_CREATE_TABLE, table_schema, stmt))) {
          LOG_WARN("failed to set individual range part high bound", K(ret));
        }
      } else if (table_schema.is_list_subpart()) {
        if (OB_FAIL(ObPartitionExecutorUtils::set_individual_list_part_rows(ctx,
                stmt::T_CREATE_TABLE,
                table_schema,
                stmt.get_subpart_fun_exprs(),
                stmt.get_individual_subpart_values_exprs()))) {
          LOG_WARN("failed to set individual list part rows", K(ret));
        }
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_WARN(OB_NOT_SUPPORTED, "add hash subpartition");
      }
    } else if (obrpc::ObAlterTableArg::DROP_PARTITION == arg.alter_part_type_ ||
               obrpc::ObAlterTableArg::DROP_SUB_PARTITION == arg.alter_part_type_ ||
               obrpc::ObAlterTableArg::TRUNCATE_PARTITION == arg.alter_part_type_ ||
               obrpc::ObAlterTableArg::TRUNCATE_SUB_PARTITION == arg.alter_part_type_) {
      // do-nothing
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("no operation", K(arg.alter_part_type_), K(ret));
    }
    LOG_DEBUG("dump table schema", K(table_schema));
  }

  return ret;
}

int ObAlterTableExecutor::set_index_arg_list(ObExecContext& ctx, ObAlterTableStmt& stmt)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo* my_session = ctx.get_my_session();
  obrpc::ObAlterTableArg& alter_table_arg = const_cast<obrpc::ObAlterTableArg&>(stmt.get_alter_table_arg());
  if (OB_ISNULL(my_session)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("session is null", K(ret));
  } else if (stmt.get_index_partition_resolve_results().count() != stmt.get_index_arg_list().count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid index resolve result", K(ret), K(stmt));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_index_arg_list().count(); i++) {
    ObCreateIndexStmt index_stmt;
    obrpc::ObCreateIndexArg& create_index_arg = *(stmt.get_index_arg_list().at(i));
    ObPartitionResolveResult& resolve_result = stmt.get_index_partition_resolve_results().at(i);
    index_stmt.get_part_fun_exprs() = resolve_result.get_part_fun_exprs();
    index_stmt.get_part_values_exprs() = resolve_result.get_part_values_exprs();
    index_stmt.get_subpart_fun_exprs() = resolve_result.get_subpart_fun_exprs();
    index_stmt.get_template_subpart_values_exprs() = resolve_result.get_template_subpart_values_exprs();
    index_stmt.get_individual_subpart_values_exprs() = resolve_result.get_individual_subpart_values_exprs();
    if (OB_FAIL(index_stmt.get_create_index_arg().assign(create_index_arg))) {
      LOG_WARN("fail to assign create index arg", K(ret));
    } else if (OB_FAIL(ObPartitionExecutorUtils::calc_values_exprs(ctx, index_stmt))) {
      LOG_WARN("fail to compare range partition expr", K(ret));
    } else {
      create_index_arg.is_inner_ = my_session->is_inner();
      if (OB_FAIL(create_index_arg.assign(index_stmt.get_create_index_arg()))) {
        LOG_WARN("fail to assign create index arg", K(ret));
      } else if (OB_FAIL(alter_table_arg.index_arg_list_.push_back(&create_index_arg))) {
        LOG_WARN("fail to push back index_arg", K(ret));
      }
    }
  }
  return ret;
}

/**
 *
 */

ObDropTableExecutor::ObDropTableExecutor()
{}

ObDropTableExecutor::~ObDropTableExecutor()
{}

int ObDropTableExecutor::execute(ObExecContext& ctx, ObDropTableStmt& stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx* task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy* common_rpc_proxy = NULL;
  const obrpc::ObDropTableArg& drop_table_arg = stmt.get_drop_table_arg();
  ObString first_stmt;
  ObSQLSessionInfo* my_session = NULL;
  if (OB_FAIL(stmt.get_first_stmt(first_stmt))) {
    LOG_WARN("get first statement failed", K(ret));
  } else {
    const_cast<obrpc::ObDropTableArg&>(drop_table_arg).ddl_stmt_str_ = first_stmt;
    my_session = ctx.get_my_session();
    if (NULL == my_session) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get my session", K(ret), K(ctx));
    } else if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
      ret = OB_NOT_INIT;
      LOG_WARN("get task executor context failed");
    } else if (OB_FAIL(task_exec_ctx->get_common_rpc(common_rpc_proxy))) {
      LOG_WARN("get common rpc proxy failed", K(ret));
    } else if (OB_ISNULL(common_rpc_proxy)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("common rpc proxy should not be null", K(ret));
    } else if (OB_INVALID_ID == drop_table_arg.session_id_ &&
               FALSE_IT(const_cast<obrpc::ObDropTableArg&>(drop_table_arg).session_id_ =
                            my_session->get_sessid_for_table())) {
      // impossible
    } else if (OB_FAIL(common_rpc_proxy->drop_table(drop_table_arg))) {
      LOG_WARN("rpc proxy drop table failed", K(ret), "dst", common_rpc_proxy->get_server());
    }
  }
  return ret;
}

/**
 *
 */
ObRenameTableExecutor::ObRenameTableExecutor()
{}

ObRenameTableExecutor::~ObRenameTableExecutor()
{}

int ObRenameTableExecutor::execute(ObExecContext& ctx, ObRenameTableStmt& stmt)
{
  int ret = OB_SUCCESS;
  const obrpc::ObRenameTableArg& rename_table_arg = stmt.get_rename_table_arg();
  ObTaskExecutorCtx* task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy* common_rpc_proxy = NULL;
  if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_FAIL(task_exec_ctx->get_common_rpc(common_rpc_proxy))) {
    LOG_WARN("get common rpc proxy failed", K(ret));
  } else if (OB_ISNULL(common_rpc_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("common rpc proxy should not be null", K(ret));
  } else if (OB_FAIL(common_rpc_proxy->rename_table(rename_table_arg))) {
    LOG_WARN("rpc proxy rename table failed", K(ret));
  }
  return ret;
}

/**
 *
 */

ObTruncateTableExecutor::ObTruncateTableExecutor()
{}

ObTruncateTableExecutor::~ObTruncateTableExecutor()
{}

int ObTruncateTableExecutor::execute(ObExecContext& ctx, ObTruncateTableStmt& stmt)
{
  int ret = OB_SUCCESS;
  const obrpc::ObTruncateTableArg& truncate_table_arg = stmt.get_truncate_table_arg();
  ObString first_stmt;
  ObSQLSessionInfo* my_session = ctx.get_my_session();
  if (OB_FAIL(stmt.get_first_stmt(first_stmt))) {
    LOG_WARN("get first statement failed", K(ret));
  } else {
    const_cast<obrpc::ObTruncateTableArg&>(truncate_table_arg).ddl_stmt_str_ = first_stmt;

    ObTaskExecutorCtx* task_exec_ctx = NULL;
    obrpc::ObCommonRpcProxy* common_rpc_proxy = NULL;
    if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
      ret = OB_NOT_INIT;
      LOG_WARN("get task executor context failed");
    } else if (OB_FAIL(task_exec_ctx->get_common_rpc(common_rpc_proxy))) {
      LOG_WARN("get common rpc proxy failed", K(ret));
    } else if (OB_ISNULL(common_rpc_proxy)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("common rpc proxy should not be null", K(ret));
    } else if (OB_ISNULL(my_session)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get my session", K(ret), K(ctx));
    } else if (OB_INVALID_ID == truncate_table_arg.session_id_ &&
               FALSE_IT(const_cast<obrpc::ObTruncateTableArg&>(truncate_table_arg).session_id_ =
                            my_session->get_sessid_for_table())) {
      // impossible
    } else if (OB_FAIL(common_rpc_proxy->truncate_table(truncate_table_arg))) {
      LOG_WARN("rpc proxy alter table failed", K(ret));
    }
  }
  return ret;
}

ObCreateTableLikeExecutor::ObCreateTableLikeExecutor()
{}

ObCreateTableLikeExecutor::~ObCreateTableLikeExecutor()
{}

int ObCreateTableLikeExecutor::execute(ObExecContext& ctx, ObCreateTableLikeStmt& stmt)
{
  int ret = OB_SUCCESS;
  const obrpc::ObCreateTableLikeArg& create_table_like_arg = stmt.get_create_table_like_arg();
  ObString first_stmt;
  if (OB_FAIL(stmt.get_first_stmt(first_stmt))) {
    LOG_WARN("get first statement failed", K(ret));
  } else {
    const_cast<obrpc::ObCreateTableLikeArg&>(create_table_like_arg).ddl_stmt_str_ = first_stmt;

    ObTaskExecutorCtx* task_exec_ctx = NULL;
    obrpc::ObCommonRpcProxy* common_rpc_proxy = NULL;
    if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
      ret = OB_NOT_INIT;
      LOG_WARN("get task executor context failed");
    } else if (OB_FAIL(task_exec_ctx->get_common_rpc(common_rpc_proxy))) {
      LOG_WARN("get common rpc proxy failed", K(ret));
    } else if (OB_ISNULL(common_rpc_proxy)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("common rpc proxy should not be null", K(ret));
    } else if (OB_FAIL(common_rpc_proxy->create_table_like(create_table_like_arg))) {
      LOG_WARN("rpc proxy create table like failed", K(ret));
    }
  }
  return ret;
}

int ObFlashBackTableFromRecyclebinExecutor::execute(ObExecContext &ctx, ObFlashBackTableFromRecyclebinStmt &stmt)		
{		
  int ret = OB_SUCCESS;
  const obrpc::ObFlashBackTableFromRecyclebinArg &flashback_table_arg = stmt.get_flashback_table_arg();		
  ObString first_stmt;		
  if (OB_FAIL(stmt.get_first_stmt(first_stmt))) {		
    LOG_WARN("get first statement failed", K(ret));		
  } else {		
    const_cast<obrpc::ObFlashBackTableFromRecyclebinArg&>(flashback_table_arg).ddl_stmt_str_ = first_stmt;		
    ObTaskExecutorCtx *task_exec_ctx = NULL;
    obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
    if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
      ret = OB_NOT_INIT;
      LOG_WARN("get task executor context failed");
    } else if (OB_FAIL(task_exec_ctx->get_common_rpc(common_rpc_proxy))) {
      LOG_WARN("get common rpc proxy failed", K(ret));
    } else if (OB_ISNULL(common_rpc_proxy)){
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("common rpc proxy should not be null", K(ret));
    } else if (OB_FAIL(common_rpc_proxy->flashback_table_from_recyclebin(flashback_table_arg))) {
      LOG_WARN("rpc proxy flashback table failed", K(ret));
    }
  }
  return ret;
}

int ObPurgeTableExecutor::execute(ObExecContext& ctx, ObPurgeTableStmt& stmt)
{
  int ret = OB_SUCCESS;
  const obrpc::ObPurgeTableArg& purge_table_arg = stmt.get_purge_table_arg();
  ObString first_stmt;
  if (OB_FAIL(stmt.get_first_stmt(first_stmt))) {
    LOG_WARN("get first statement failed", K(ret));
  } else {
    const_cast<obrpc::ObPurgeTableArg&>(purge_table_arg).ddl_stmt_str_ = first_stmt;

    ObTaskExecutorCtx* task_exec_ctx = NULL;
    obrpc::ObCommonRpcProxy* common_rpc_proxy = NULL;
    if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
      ret = OB_NOT_INIT;
      LOG_WARN("get task executor context failed");
    } else if (OB_FAIL(task_exec_ctx->get_common_rpc(common_rpc_proxy))) {
      LOG_WARN("get common rpc proxy failed", K(ret));
    } else if (OB_ISNULL(common_rpc_proxy)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("common rpc proxy should not be null", K(ret));
    } else if (OB_FAIL(common_rpc_proxy->purge_table(purge_table_arg))) {
      LOG_WARN("rpc proxy purge table failed", K(ret));
    }
  }
  return ret;
}

int ObOptimizeTableExecutor::execute(ObExecContext& ctx, ObOptimizeTableStmt& stmt)
{
  int ret = OB_SUCCESS;
  obrpc::ObOptimizeTableArg& arg = stmt.get_optimize_table_arg();
  ObString first_stmt;
  if (OB_FAIL(stmt.get_first_stmt(first_stmt))) {
    LOG_WARN("fail to get first stmt", K(ret));
  } else {
    arg.ddl_stmt_str_ = first_stmt;
    ObTaskExecutorCtx* task_exec_ctx = nullptr;
    obrpc::ObCommonRpcProxy* common_rpc_proxy = nullptr;
    if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, task executor must not be NULL", K(ret));
    } else if (OB_FAIL(task_exec_ctx->get_common_rpc(common_rpc_proxy))) {
      LOG_WARN("fail to get common rpc", K(ret));
    } else if (OB_ISNULL(common_rpc_proxy)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, common rpc proxy must not be NULL", K(ret));
    } else if (OB_FAIL(common_rpc_proxy->optimize_table(arg))) {
      LOG_WARN("fail to optimize table", K(ret));
    }
  }
  return ret;
}

int ObOptimizeTenantExecutor::execute(ObExecContext& ctx, ObOptimizeTenantStmt& stmt)
{
  int ret = OB_SUCCESS;
  obrpc::ObOptimizeTenantArg& arg = stmt.get_optimize_tenant_arg();
  ObString first_stmt;
  ObSQLSessionInfo* my_session = ctx.get_my_session();
  if (OB_FAIL(stmt.get_first_stmt(first_stmt))) {
    LOG_WARN("fail to get first stmt", K(ret));
  } else if (OB_ISNULL(my_session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, my session must not be NULL", K(ret));
  } else {
    arg.ddl_stmt_str_ = first_stmt;
    ObTaskExecutorCtx* task_exec_ctx = nullptr;
    obrpc::ObCommonRpcProxy* common_rpc_proxy = nullptr;
    const observer::ObGlobalContext& gctx = observer::ObServer::get_instance().get_gctx();
    const int64_t effective_tenant_id = my_session->get_effective_tenant_id();
    if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, task executor must not be NULL", K(ret));
    } else if (OB_FAIL(task_exec_ctx->get_common_rpc(common_rpc_proxy))) {
      LOG_WARN("fail to get common rpc", K(ret));
    } else if (OB_ISNULL(common_rpc_proxy)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, common rpc proxy must not be NULL", K(ret));
    } else if (OB_ISNULL(gctx.schema_service_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, schema service must not be NULL", K(ret));
    } else if (OB_FAIL(optimize_tenant(arg, effective_tenant_id, *gctx.schema_service_, common_rpc_proxy))) {
      LOG_WARN("fail to optimize tenant", K(ret));
    }
  }
  return ret;
}

int ObOptimizeTenantExecutor::optimize_tenant(const obrpc::ObOptimizeTenantArg& arg, const uint64_t effective_tenant_id,
    ObMultiVersionSchemaService& schema_service, obrpc::ObCommonRpcProxy* common_rpc_proxy)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = OB_INVALID_ID;
  ObSchemaGetterGuard schema_guard;
  LOG_INFO("receive optimize tenant request", K(arg));
  if (!arg.is_valid() || NULL == common_rpc_proxy) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(arg), KP(common_rpc_proxy));
  } else if (OB_FAIL(schema_service.get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_id(arg.tenant_name_, tenant_id))) {
    LOG_WARN("fail to get tenant id", K(ret));
  } else if (OB_SYS_TENANT_ID != effective_tenant_id && tenant_id != effective_tenant_id) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("tenant id mismatch", K(tenant_id), K(effective_tenant_id));
  } else {
    ObArray<const ObTableSchema*> table_schemas;
    if (OB_FAIL(schema_service.get_tenant_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("fail to get tenant schema guard", K(ret));
    } else if (OB_FAIL(schema_guard.get_table_schemas_in_tenant(tenant_id, table_schemas))) {
      LOG_WARN("fail to get table schemas in tenant", K(ret));
    } else {
      LOG_INFO("optimize tenant, table schema count", K(table_schemas.count()));
      for (int64_t i = 0; OB_SUCC(ret) && i < table_schemas.count(); ++i) {
        const ObTableSchema* table_schema = table_schemas.at(i);
        const ObDatabaseSchema* database_schema = nullptr;
        obrpc::ObOptimizeTableArg optimize_table_arg;
        if (OB_ISNULL(table_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("error unexpected, table schema must not be NULL", K(ret));
        } else if (table_schema->is_index_table() || table_schema->is_vir_table() || table_schema->is_view_table()) {
          // do nothing
        } else if (OB_FAIL(schema_guard.get_database_schema(table_schema->get_database_id(), database_schema))) {
          LOG_WARN("fail to get database schema", K(ret));
        } else {
          obrpc::ObTableItem table_item;
          optimize_table_arg.tenant_id_ = tenant_id;
          optimize_table_arg.exec_tenant_id_ = tenant_id;
          table_item.database_name_ = database_schema->get_database_name();
          table_item.table_name_ = table_schema->get_table_name();
          if (OB_FAIL(optimize_table_arg.tables_.push_back(table_item))) {
            LOG_WARN("fail to push back optimize table arg", K(ret));
          } else if (OB_FAIL(common_rpc_proxy->optimize_table(optimize_table_arg))) {
            LOG_WARN("fail to optimize table", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObOptimizeAllExecutor::execute(ObExecContext& ctx, ObOptimizeAllStmt& stmt)
{
  int ret = OB_SUCCESS;
  obrpc::ObOptimizeAllArg& arg = stmt.get_optimize_all_arg();
  ObString first_stmt;
  if (OB_FAIL(stmt.get_first_stmt(first_stmt))) {
    LOG_WARN("fail to get first stmt", K(ret));
  } else {
    arg.ddl_stmt_str_ = first_stmt;
    ObTaskExecutorCtx* task_exec_ctx = nullptr;
    obrpc::ObCommonRpcProxy* common_rpc_proxy = nullptr;
    ObSchemaGetterGuard schema_guard;
    ObArray<uint64_t> tenant_ids;
    const observer::ObGlobalContext& gctx = observer::ObServer::get_instance().get_gctx();
    ObSQLSessionInfo* my_session = ctx.get_my_session();
    if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, task executor must not be NULL", K(ret));
    } else if (OB_FAIL(task_exec_ctx->get_common_rpc(common_rpc_proxy))) {
      LOG_WARN("fail to get common rpc", K(ret));
    } else if (OB_ISNULL(common_rpc_proxy)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, common rpc proxy must not be NULL", K(ret));
    } else if (OB_ISNULL(my_session)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, my session must not be NULL", K(ret));
    } else if (OB_ISNULL(gctx.schema_service_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, schema service must not be NULL", K(ret));
    } else if (OB_FAIL(gctx.schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
      LOG_WARN("fail to get tenant schema guard", K(ret));
    } else if (OB_FAIL(schema_guard.get_tenant_ids(tenant_ids))) {
      LOG_WARN("fail to get tenant ids", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.count(); ++i) {
        obrpc::ObOptimizeTenantArg tenant_arg;
        const ObTenantSchema* tenant_schema = nullptr;
        if (OB_FAIL(schema_guard.get_tenant_info(tenant_ids.at(i), tenant_schema))) {
          LOG_WARN("fail to get tenant name", K(ret));
        } else {
          tenant_arg.tenant_name_ = tenant_schema->get_tenant_name();
          if (OB_FAIL(ObOptimizeTenantExecutor::optimize_tenant(
                  tenant_arg, my_session->get_effective_tenant_id(), *gctx.schema_service_, common_rpc_proxy))) {
            LOG_WARN("fail to optimize tenant", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

}  // end namespace sql
}  // end namespace oceanbase
