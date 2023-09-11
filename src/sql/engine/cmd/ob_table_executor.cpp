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

#include "share/ob_cluster_version.h"
#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/cmd/ob_table_executor.h"
#include "sql/engine/cmd/ob_index_executor.h"
#include "sql/engine/cmd/ob_ddl_executor_util.h"
#include "share/object/ob_obj_cast.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/utility/ob_tracepoint.h"
#include "share/ob_common_rpc_proxy.h"
#include "share/ob_ddl_error_message_table_operator.h"
#include "sql/resolver/ddl/ob_create_table_stmt.h"
#include "sql/resolver/ddl/ob_alter_table_stmt.h"
#include "sql/resolver/ddl/ob_drop_table_stmt.h"
#include "sql/resolver/ddl/ob_rename_table_stmt.h"
#include "sql/resolver/ddl/ob_truncate_table_stmt.h"
#include "sql/resolver/ddl/ob_create_table_like_stmt.h"
#include "sql/resolver/ddl/ob_flashback_stmt.h"
#include "sql/resolver/ddl/ob_purge_stmt.h"
#include "sql/resolver/ddl/ob_optimize_stmt.h"
#include "sql/resolver/dml/ob_delete_stmt.h"
#include "sql/resolver/dml/ob_delete_resolver.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/code_generator/ob_expr_generator_impl.h"
#include "sql/engine/cmd/ob_partition_executor_utils.h"
#include "sql/parser/ob_parser.h"
#include "share/system_variable/ob_sys_var_class_type.h"

#include "sql/ob_select_stmt_printer.h"
#include "observer/ob_server_struct.h"
#include "observer/ob_server.h"
#include "lib/worker.h"
#include "share/external_table/ob_external_table_file_mgr.h"
#include "share/external_table/ob_external_table_file_task.h"
#include "share/external_table/ob_external_table_file_rpc_proxy.h"
#include "observer/ob_srv_network_frame.h"
#include "observer/dbms_job/ob_dbms_job_master.h"
#include "observer/ob_inner_sql_connection_pool.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "share/external_table/ob_external_table_file_rpc_processor.h"
#include "share/external_table/ob_external_table_utils.h"
#include "share/ob_debug_sync.h"
#include "share/schema/ob_schema_utils.h"
namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;
using namespace observer;
namespace sql
{

ObCreateTableExecutor::ObCreateTableExecutor()
{
}

ObCreateTableExecutor::~ObCreateTableExecutor()
{
}

int ObCreateTableExecutor::prepare_stmt(ObCreateTableStmt &stmt,
                                        const ObSQLSessionInfo &my_session,
                                        ObString &create_table_name)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("CreateTableExec");
  const int64_t buf_len = OB_MAX_SQL_LENGTH;
  char *buf = static_cast<char*>(allocator.alloc(buf_len));
  int64_t pos = 0;
  const int64_t session_id = my_session.get_sessid();
  const int64_t timestamp = ObTimeUtility::current_time();
  obrpc::ObCreateTableArg &create_table_arg = stmt.get_create_table_arg();
  create_table_name = create_table_arg.schema_.get_table_name_str();
  if (OB_FAIL(databuff_printf(buf, buf_len, pos, "__ctas_%ld_%ld", session_id, timestamp))) {
    LOG_WARN("failed to print tmp table name", K(ret));
  } else {
    ObString tmp_table_name(pos, buf);
    if (OB_FAIL(create_table_arg.schema_.set_table_name(tmp_table_name))) {
      LOG_WARN("failed to set tmp table name", K(ret));
    }
  }
  return ret;
}

int ObCreateTableExecutor::ObInsSQLPrinter::inner_print(char *buf, int64_t buf_len, int64_t &res_len)
{
  int ret = OB_SUCCESS;
  const char sep_char = lib::is_oracle_mode()? '"': '`';
  const ObSelectStmt *select_stmt = NULL;
  int64_t pos1 = 0;
  if (OB_ISNULL(stmt_) || OB_ISNULL(select_stmt= stmt_->get_sub_select())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null stmt", K(ret));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos1,
                              do_osg_
                              ? "insert /*+GATHER_OPTIMIZER_STATISTICS*/ into %c%.*s%c.%c%.*s%c"
                              : "insert /*+NO_GATHER_OPTIMIZER_STATISTICS*/ into %c%.*s%c.%c%.*s%c",
                              sep_char,
                              stmt_->get_database_name().length(),
                              stmt_->get_database_name().ptr(),
                              sep_char,
                              sep_char,
                              stmt_->get_table_name().length(),
                              stmt_->get_table_name().ptr(),
                              sep_char))) {
    LOG_WARN("fail to print insert into string", K(ret));
  } else if (lib::is_oracle_mode()) {
    const ObTableSchema &table_schema = stmt_->get_create_table_arg().schema_;
    int64_t used_column_count = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < table_schema.get_column_count(); ++i) {
      const ObColumnSchemaV2 *column_schema = table_schema.get_column_schema_by_idx(i);
      if (OB_ISNULL(column_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null column schema", K(ret));
      } else if (column_schema->get_column_id() < OB_END_RESERVED_COLUMN_ID_NUM ||
                 column_schema->is_udt_hidden_column()) {
        // do nothing
      } else if (OB_FAIL(databuff_printf(buf, buf_len, pos1, (0 == used_column_count)? "(": ", "))) {
        LOG_WARN("failed to print insert into string", K(ret), K(i));
      } else if (OB_FAIL(databuff_printf(buf, buf_len, pos1, "%c%.*s%c", sep_char, LEN_AND_PTR(column_schema->get_column_name_str()), sep_char))) {
        LOG_WARN("failed to print insert into string", K(ret));
      } else {
        ++ used_column_count;
      }
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_select_item_size(); ++i) {
      const SelectItem &select_item = select_stmt->get_select_item(i);
      if (OB_FAIL(databuff_printf(buf, buf_len, pos1, (0 == i)? "(": ", "))) {
        LOG_WARN("failed to print insert into string", K(ret), K(i));
      } else { /* do nothing */ }
      if (OB_SUCC(ret)) {
        if (!select_item.alias_name_.empty()) {
          if (OB_FAIL(databuff_printf(buf, buf_len, pos1, "%c%.*s%c", sep_char, LEN_AND_PTR(select_item.alias_name_), sep_char))) {
            LOG_WARN("failed to print insert into string", K(ret));
          } else { /* do nothing */ }
        } else {
          if (OB_FAIL(databuff_printf(buf, buf_len, pos1, "%c%.*s%c", sep_char, LEN_AND_PTR(select_item.expr_name_), sep_char))) {
            LOG_WARN("failed to print insert into string", K(ret));
          } else { /* do nothing */ }
        }
      } else { /* do nothing */ }
    }
  }

  if (OB_SUCC(ret)) {
    ObSelectStmtPrinter select_stmt_printer(buf, buf_len, &pos1, select_stmt,
                                            schema_guard_,
                                            print_params_,
                                            param_store_,
                                            true);
    select_stmt_printer.set_is_first_stmt_for_hint(true);  // need print global hint
    if (OB_FAIL(databuff_printf(buf, buf_len, pos1, ") "))) {
      LOG_WARN("fail to append ')'", K(ret));
    } else if (OB_FAIL(select_stmt_printer.do_print())) {
      LOG_WARN("fail to print select stmt", K(ret));
    } else {
      res_len = pos1;
    }
  }
  return ret;
}

//准备查询插入的脚本
int ObCreateTableExecutor::prepare_ins_arg(ObCreateTableStmt &stmt,
                                           const ObSQLSessionInfo *my_session,
                                           ObSchemaGetterGuard *schema_guard,
                                           const ParamStore *param_store,
                                           ObSqlString &ins_sql) //out, 最终的查询插入语句
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("CreateTableExec");
  char *buf = static_cast<char*>(allocator.alloc(OB_MAX_SQL_LENGTH));
  int64_t buf_len = OB_MAX_SQL_LENGTH;
  int64_t pos1 = 0;
  bool is_oracle_mode = lib::is_oracle_mode();
  bool no_osg_hint = false;
  bool online_sys_var = false;
  ObSelectStmt *select_stmt = stmt.get_sub_select();
  ObObjPrintParams obj_print_params(select_stmt->get_query_ctx()->get_timezone_info());
  obj_print_params.print_origin_stmt_ = true;
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("allocate memory failed");
  } else if (OB_ISNULL(select_stmt) || OB_ISNULL(select_stmt->get_query_ctx()) || OB_ISNULL(my_session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("select stmt should not be null", K(ret));
  } else {
    //get hint
    no_osg_hint = select_stmt->get_query_ctx()->get_global_hint().has_no_gather_opt_stat_hint();

    //get system variable
    ObObj online_sys_var_obj;
    if (OB_FAIL(OB_FAIL(my_session->get_sys_variable(SYS_VAR__OPTIMIZER_GATHER_STATS_ON_LOAD, online_sys_var_obj)))) {
      LOG_WARN("fail to get sys var", K(ret));
    } else {
      online_sys_var = online_sys_var_obj.get_bool();
      LOG_DEBUG("online opt stat gather", K(online_sys_var), K(no_osg_hint));
    }
  }

  if (OB_SUCC(ret)) {
    ObInsSQLPrinter sql_printer(&stmt, schema_guard, obj_print_params, param_store, !no_osg_hint && online_sys_var);
    ObString sql;
    if (OB_FAIL(sql_printer.do_print(allocator, sql))) {
      LOG_WARN("failed  to print", K(ret));
    } else if (OB_FAIL(ins_sql.append(sql))){
      LOG_WARN("fail to append insert into string", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    ObString converted_sql = ins_sql.string();
    if (OB_FAIL(ObSQLUtils::convert_sql_text_from_schema_for_resolve(allocator,
                                                                     my_session->get_dtc_params(),
                                                                     converted_sql,
                                                                     ObCharset::COPY_STRING_ON_SAME_CHARSET))) {
      LOG_WARN("fail to convert insert into string to client_cs_type", K(ret));
    } else if (OB_FAIL(ins_sql.assign(converted_sql))) {
      LOG_WARN("fail to assign converted insert into string", K(ret));
    }
  }
  LOG_DEBUG("ins str preparation complete!", K(ins_sql), K(ret), K(lib::is_oracle_mode()));
  return ret;
}

//准备alter table 的参数
int ObCreateTableExecutor::prepare_alter_arg(ObCreateTableStmt &stmt,
                                             const ObSQLSessionInfo *my_session,
                                             const ObString &create_table_name,
                                             obrpc::ObAlterTableArg &alter_table_arg) //out, 最终的alter table arg, set session_id = 0;
{
  int ret = OB_SUCCESS;
  const obrpc::ObCreateTableArg &create_table_arg = stmt.get_create_table_arg();
  ObTableSchema &table_schema = const_cast<obrpc::ObCreateTableArg&>(create_table_arg).schema_;
  AlterTableSchema *alter_table_schema = &alter_table_arg.alter_table_schema_;
  table_schema.set_session_id(my_session->get_sessid_for_table());
  alter_table_arg.session_id_ = my_session->get_sessid_for_table();
  alter_table_schema->alter_type_ = OB_DDL_ALTER_TABLE;
  //compat for old server
  alter_table_arg.tz_info_ = my_session->get_tz_info_wrap().get_tz_info_offset();
  alter_table_arg.is_inner_ = my_session->is_inner();
  alter_table_arg.exec_tenant_id_ = my_session->get_effective_tenant_id();
  if (OB_FAIL(alter_table_arg.tz_info_wrap_.deep_copy(my_session->get_tz_info_wrap()))) {
    LOG_WARN("failed to deep_copy tz info wrap", "tz_info_wrap", my_session->get_tz_info_wrap(), K(ret));
  } else if (OB_FAIL(alter_table_arg.set_nls_formats(
      my_session->get_local_nls_date_format(),
      my_session->get_local_nls_timestamp_format(),
      my_session->get_local_nls_timestamp_tz_format()))) {
    LOG_WARN("failed to set_nls_formats", K(ret));
  } else if (OB_FAIL(alter_table_schema->assign(table_schema))) {
    LOG_WARN("failed to assign alter table schema", K(ret));
  } else if (!table_schema.is_mysql_tmp_table()
             && FALSE_IT(alter_table_schema->set_session_id(0))) {
    //impossible
  } else if (OB_FAIL(alter_table_schema->set_origin_table_name(stmt.get_table_name()))) {
    LOG_WARN("failed to set origin table name", K(ret));
  } else if (OB_FAIL(alter_table_schema->set_origin_database_name(stmt.get_database_name()))) {
    LOG_WARN("failed to set origin database name", K(ret));
  } else if (OB_FAIL(alter_table_schema->set_table_name(create_table_name))) {
    LOG_WARN("failed to set table name", K(ret));
  } else if (OB_FAIL(alter_table_schema->set_database_name(stmt.get_database_name()))) {
    LOG_WARN("failed to set database name", K(ret));
  } else if (!table_schema.is_mysql_tmp_table()
             && OB_FAIL(alter_table_schema->alter_option_bitset_.add_member(obrpc::ObAlterTableArg::SESSION_ID))) {
    LOG_WARN("failed to add member SESSION_ID for alter table schema", K(ret), K(alter_table_arg));
  } else if (OB_FAIL(alter_table_schema->alter_option_bitset_.add_member(obrpc::ObAlterTableArg::TABLE_NAME))) {
    LOG_WARN("failed to add member TABLE_NAME for alter table schema", K(ret), K(alter_table_arg));
  }
  LOG_DEBUG("alter table arg preparation complete!", K(*alter_table_schema), K(ret));
  return ret;
}

//准备drop table的参数
int ObCreateTableExecutor::prepare_drop_arg(const ObCreateTableStmt &stmt,
                                            const ObSQLSessionInfo *my_session,
                                            obrpc::ObTableItem &table_item,
                                            obrpc::ObDropTableArg &drop_table_arg) //out, drop table的参数
{
  int ret = OB_SUCCESS;
  const ObString &db_name = stmt.get_database_name();
  const ObString &tab_name = stmt.get_table_name();
  drop_table_arg.if_exist_ = true;
  drop_table_arg.tenant_id_ = my_session->get_login_tenant_id();
  drop_table_arg.to_recyclebin_ = false;
  drop_table_arg.table_type_ = USER_TABLE;
  drop_table_arg.session_id_ = my_session->get_sessid_for_table();
  drop_table_arg.exec_tenant_id_ = my_session->get_effective_tenant_id();
  int64_t foreign_key_checks = 0;
  my_session->get_foreign_key_checks(foreign_key_checks);
  drop_table_arg.foreign_key_checks_ = is_oracle_mode() || (is_mysql_mode() && foreign_key_checks);
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

//查询建表的处理, 通过内部session执行查询插入代码参考了 ObTableModify::ObTableModifyCtx::open_inner_conn() 实现
int ObCreateTableExecutor::execute_ctas(ObExecContext &ctx,
                                        ObCreateTableStmt &stmt,
                                        obrpc::ObCommonRpcProxy *common_rpc_proxy)
{
  int ret = OB_SUCCESS;
  ObString cur_query;
  int64_t affected_rows = 0;
  ObMySQLProxy *sql_proxy = ctx.get_sql_proxy();
  common::ObCommonSqlProxy *user_sql_proxy;
  common::ObOracleSqlProxy oracle_sql_proxy;
  ObSQLSessionInfo *my_session = ctx.get_my_session();
  ObPhysicalPlanCtx *plan_ctx = ctx.get_physical_plan_ctx();
  ObArenaAllocator allocator("CreateTableExec");
  HEAP_VAR(obrpc::ObAlterTableArg, alter_table_arg) {
    obrpc::ObDropTableArg drop_table_arg;
    obrpc::ObTableItem table_item;
    ObString create_table_name;
    ObSqlString ins_sql;
    const observer::ObGlobalContext &gctx = observer::ObServer::get_instance().get_gctx();
    obrpc::ObCreateTableRes create_table_res;
    obrpc::ObCreateTableArg &create_table_arg = stmt.get_create_table_arg();
    create_table_arg.is_inner_ = my_session->is_inner();
    bool need_clean = true;
    CK(OB_NOT_NULL(sql_proxy),
      OB_NOT_NULL(my_session),
      OB_NOT_NULL(gctx.schema_service_),
      OB_NOT_NULL(plan_ctx),
      OB_NOT_NULL(common_rpc_proxy),
      OB_NOT_NULL(ctx.get_sql_ctx()));
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ob_write_string(allocator, my_session->get_current_query_string(), cur_query))) {
        LOG_WARN("failed to write string to session", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      ObInnerSQLConnectionPool *pool = static_cast<observer::ObInnerSQLConnectionPool*>(sql_proxy->get_pool());
      if (OB_ISNULL(pool)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pool is null", K(ret));
      } else if (OB_FAIL(oracle_sql_proxy.init(pool))) {
        LOG_WARN("init oracle sql proxy failed", K(ret));
      } else if (OB_FAIL(prepare_stmt(stmt, *my_session, create_table_name))) {
        LOG_WARN("failed to prepare stmt", K(ret));
      } else if (OB_FAIL(prepare_ins_arg(stmt, my_session, ctx.get_sql_ctx()->schema_guard_, &plan_ctx->get_param_store(), ins_sql))) { //1, 参数准备;
        LOG_WARN("failed to prepare insert table arg", K(ret));
      } else if (OB_FAIL(prepare_alter_arg(stmt, my_session, create_table_name, alter_table_arg))) {
        LOG_WARN("failed to prepare alter table arg", K(ret));
      } else if (OB_FAIL(prepare_drop_arg(stmt, my_session, table_item, drop_table_arg))) {
        LOG_WARN("failed to prepare drop table arg", K(ret));
      } else if (OB_FAIL(ctx.get_sql_ctx()->schema_guard_->reset())){
        LOG_WARN("schema_guard reset failed", K(ret));
      } else if (OB_FAIL(common_rpc_proxy->create_table(create_table_arg, create_table_res))) { //2, 建表;
        LOG_WARN("rpc proxy create table failed", K(ret), "dst", common_rpc_proxy->get_server());
      } else if (OB_INVALID_ID != create_table_res.table_id_) { //如果表已存在则后续的查询插入不进行
        if (OB_INVALID_VERSION == create_table_res.schema_version_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected schema version", K(ret), K(create_table_res));
        } else {
          uint64_t tenant_id = my_session->get_effective_tenant_id();
          if (OB_FAIL(gctx.schema_service_->async_refresh_schema(tenant_id,
                                                                create_table_res.schema_version_))) {
            LOG_WARN("failed to async refresh schema", K(ret));
          }
        }
        #ifdef ERRSIM
        {
          int tmp_ret = OB_E(EventTable::EN_CTAS_FAIL_NO_DROP_ERROR) OB_SUCCESS; //错误注入, 使表不能清理
          if (OB_FAIL(tmp_ret)) {
            ret = tmp_ret;
            need_clean = false;
          }
        }
        #else
          //do nothing...
        #endif

        //3, 插入数据
        if (OB_SUCC(ret)) {
          bool is_mysql_temp_table = stmt.get_create_table_arg().schema_.is_mysql_tmp_table();
          bool in_trans = my_session->is_in_transaction();
          ObBasicSessionInfo::UserScopeGuard user_scope_guard(my_session->get_sql_scope_flags());
          common::sqlclient::ObISQLConnection *conn = NULL;
          const uint64_t tenant_id = my_session->get_effective_tenant_id();
          if (lib::is_oracle_mode()) {
            user_sql_proxy = &oracle_sql_proxy;
          } else {
            user_sql_proxy = sql_proxy;
          }
          if (OB_FAIL(pool->acquire(my_session, conn))) {
            LOG_WARN("failed to acquire inner connection", K(ret));
          } else if (OB_ISNULL(conn)) {
            ret = OB_INNER_STAT_ERROR;
            LOG_WARN("connection can not be NULL", K(ret));
          } else if ((!is_mysql_temp_table || !in_trans)
                     && OB_FAIL(conn->start_transaction(tenant_id))) {
            LOG_WARN("failed start transaction", K(ret), K(tenant_id));
          } else {
            if (OB_FAIL(conn->execute_write(tenant_id, ins_sql.ptr(),
                                            affected_rows, true))) {
              LOG_WARN("failed to exec sql", K(tenant_id), K(ins_sql), K(ret));
            }
            // transaction started, must commit or rollback
            int tmp_ret = OB_SUCCESS;
            if (!is_mysql_temp_table || !in_trans) {
              if (OB_LIKELY(OB_SUCCESS == ret)) {
                tmp_ret = conn->commit();
              } else {
                int64_t MIN_ROLLBACK_TIMEOUT = 10 * 1000 * 1000;// 10s
                int64_t origin_timeout_ts = THIS_WORKER.get_timeout_ts();
                if (INT64_MAX != origin_timeout_ts &&
                    origin_timeout_ts < ObTimeUtility::current_time() + MIN_ROLLBACK_TIMEOUT) {
                  THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + MIN_ROLLBACK_TIMEOUT);
                  LOG_INFO("set timeout for rollback", K(origin_timeout_ts),
                      K(ObTimeUtility::current_time() + MIN_ROLLBACK_TIMEOUT));
                }
                tmp_ret = conn->rollback();
              }
              if (OB_UNLIKELY(OB_SUCCESS != tmp_ret)) {
                ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
                LOG_WARN("fail to end transaction", K(ret), K(tmp_ret));
              }
            }
          }
          if (OB_NOT_NULL(conn)) {
            user_sql_proxy->close(conn, true);
          }
        }

        DEBUG_SYNC(BEFORE_EXECUTE_CTAS_CLEAR_SESSION_ID);

        //4, 刷新schema, 将table的sess id重置为0
        if (OB_SUCC(ret)) {
          obrpc::ObAlterTableRes res;
          alter_table_arg.compat_mode_ = ORACLE_MODE == my_session->get_compatibility_mode() ?
            lib::Worker::CompatMode::ORACLE : lib::Worker::CompatMode::MYSQL;
          if (OB_FAIL(common_rpc_proxy->alter_table(alter_table_arg, res))) {
            LOG_WARN("failed to update table session", K(ret), K(alter_table_arg));
            if (alter_table_arg.compat_mode_ == lib::Worker::CompatMode::ORACLE && OB_ERR_TABLE_EXIST == ret) {
              ret = OB_ERR_EXIST_OBJECT;
            }
          }
        }

        if (OB_FAIL(ret)) { //5, 查询建表失败, 需要清理环境即DROP TABLE
          my_session->update_last_active_time();
          if (OB_LIKELY(need_clean)) {
            int tmp_ret = OB_SUCCESS;
            obrpc::ObDDLRes res;
            drop_table_arg.compat_mode_ = ORACLE_MODE == my_session->get_compatibility_mode() ?
              lib::Worker::CompatMode::ORACLE : lib::Worker::CompatMode::MYSQL;
            if (OB_SUCCESS != (tmp_ret = common_rpc_proxy->drop_table(drop_table_arg, res))) {
              LOG_WARN("failed to drop table", K(drop_table_arg), K(ret));
            } else {
              LOG_INFO("table is created and dropped due to error ", K(ret));
            }
          }
        } else {
          plan_ctx->set_affected_rows(affected_rows);
          LOG_DEBUG("CTAS all done", K(ins_sql), K(affected_rows), K(lib::is_oracle_mode()));
        }

        if (OB_ERR_TABLE_EXIST == ret && create_table_arg.if_not_exist_) {
          ret = OB_SUCCESS;
          LOG_DEBUG("table exists, force return success after cleanup", K(create_table_name));
        }
      } else {
        LOG_DEBUG("table exists, no need to CTAS", K(create_table_res.table_id_));
      }
    }
    OZ(my_session->store_query_string(cur_query));
  }
  return ret;
}

int ObCreateTableExecutor::execute(ObExecContext &ctx, ObCreateTableStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  obrpc::ObCreateTableRes res;
  obrpc::ObCreateTableArg &create_table_arg = stmt.get_create_table_arg();
  ObString first_stmt;
  ObSelectStmt *select_stmt = stmt.get_sub_select();
  ObTableSchema &table_schema = create_table_arg.schema_;
  ObSQLSessionInfo *my_session = ctx.get_my_session();
  uint64_t tenant_id = table_schema.get_tenant_id();
  uint64_t data_version = 0;
  if (OB_ISNULL(my_session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (OB_FAIL(stmt.get_first_stmt(first_stmt))) {
    LOG_WARN("get first statement failed", K(ret));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("fail to get data version", KR(ret), K(tenant_id));
  } else if (table_schema.is_duplicate_table()) {
    bool is_compatible = false;
    if (OB_FAIL(ObShareUtil::check_compat_version_for_readonly_replica(tenant_id, is_compatible))) {
      LOG_WARN("fail to check data version for duplicate table", KR(ret), K(tenant_id));
    } else if (!is_compatible) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("duplicate table is not supported below 4.2", KR(ret), K(table_schema), K(is_compatible));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "create duplicate table below 4.2");
    } else if (is_sys_tenant(tenant_id) || is_meta_tenant(tenant_id)) {
    // TODO@jingyu_cr: make sure whether sys log stream have to be duplicated
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "create duplicate table under sys or meta tenant");
      LOG_WARN("create dup table not supported", KR(ret), K(table_schema));
    }
  }

  if (OB_FAIL(ret)) {
  } else {
    create_table_arg.is_inner_ = my_session->is_inner();
    create_table_arg.consumer_group_id_ = THIS_WORKER.get_group_id();
    const_cast<obrpc::ObCreateTableArg&>(create_table_arg).ddl_stmt_str_ = first_stmt;
    bool enable_parallel_create_table = false;
    {
      omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
      enable_parallel_create_table = tenant_config.is_valid()
                                     && tenant_config->_enable_parallel_table_creation;

    }
    if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
      ret = OB_NOT_INIT;
      LOG_WARN("get task executor context failed", K(ret));
    } else if (!table_schema.is_external_table() //external table can not define partitions by create table stmt
               && OB_FAIL(ObPartitionExecutorUtils::calc_values_exprs(ctx, stmt))) {
      LOG_WARN("compare range parition expr fail", K(ret));
    } else if (OB_FAIL(set_index_arg_list(ctx, stmt))) {
      LOG_WARN("fail to set index_arg_list", K(ret));
    } else if (OB_FAIL(task_exec_ctx->get_common_rpc(common_rpc_proxy))) {
      LOG_WARN("get common rpc proxy failed", K(ret));
    } else if (OB_ISNULL(common_rpc_proxy)){
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("common rpc proxy should not be null", K(ret));
    } else if (OB_ISNULL(select_stmt)) { // 普通建表的处理
      if (OB_FAIL(ctx.get_sql_ctx()->schema_guard_->reset())){
        LOG_WARN("schema_guard reset failed", KR(ret));
      } else if (table_schema.is_view_table()
                 || data_version < DATA_VERSION_4_2_1_0
                 || !enable_parallel_create_table) {
        if (OB_FAIL(common_rpc_proxy->create_table(create_table_arg, res))) {
          LOG_WARN("rpc proxy create table failed", KR(ret), "dst", common_rpc_proxy->get_server());
        }
      } else {
        int64_t start_time = ObTimeUtility::current_time();
        ObTimeoutCtx ctx;
        if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, GCONF._ob_ddl_timeout))) {
          LOG_WARN("fail to set timeout ctx", KR(ret));
        } else if (OB_FAIL(common_rpc_proxy->parallel_create_table(create_table_arg, res))) {
          LOG_WARN("rpc proxy create table failed", KR(ret), "dst", common_rpc_proxy->get_server());
        } else {
          int64_t refresh_time = ObTimeUtility::current_time();
          if (OB_FAIL(ObSchemaUtils::try_check_parallel_ddl_schema_in_sync(
              ctx, tenant_id, res.schema_version_))) {
            LOG_WARN("fail to check paralleld ddl schema in sync", KR(ret), K(res));
          }
          int64_t end_time = ObTimeUtility::current_time();
          LOG_INFO("[parallel_create_table]", KR(ret),
                   "cost", end_time - start_time,
                   "execute_time", refresh_time - start_time,
                   "wait_schema", end_time - refresh_time,
                   "table_name", create_table_arg.schema_.get_table_name());
        }
      }
      if (OB_SUCC(ret) && table_schema.is_external_table()) {
        //auto refresh after create external table
        OZ (ObAlterTableExecutor::update_external_file_list(
              table_schema.get_tenant_id(), res.table_id_,
              table_schema.get_external_file_location(),
              table_schema.get_external_file_location_access_info(),
              table_schema.get_external_file_pattern(),
              ctx));
      }
    } else {
      if (table_schema.is_external_table()) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "create external table as select");
      } else if (OB_FAIL(execute_ctas(ctx, stmt, common_rpc_proxy))){  // 查询建表的处理
        LOG_WARN("execute create table as select failed", KR(ret));
      }
    }

    // only CTAS or create temporary table will make session_id != 0. If such table detected, set
    // need ctas cleanup task anyway to do some cleanup jobs
    if (0 != table_schema.get_session_id()) {
      LOG_TRACE("CTAS or temporary table create detected", K(table_schema));
      ATOMIC_STORE(&OBSERVER.need_ctas_cleanup_, true);
    }
  }
  return ret;
}

int ObCreateTableExecutor::set_index_arg_list(ObExecContext &ctx, ObCreateTableStmt &stmt)
{
  int ret = OB_SUCCESS;
  obrpc::ObCreateTableArg &create_table_arg = const_cast<obrpc::ObCreateTableArg &>(stmt.get_create_table_arg());
  if (stmt.get_index_partition_resolve_results().count()
      != stmt.get_index_arg_list().count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid index resolve result", K(ret), K(stmt));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_index_arg_list().count(); i++) {
    HEAP_VAR(ObCreateIndexStmt, index_stmt) {
      ObPartitionResolveResult &resolve_result = stmt.get_index_partition_resolve_results().at(i);
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
  }
  return ret;
}

ObAlterTableExecutor::ObAlterTableExecutor()
{
}

ObAlterTableExecutor::~ObAlterTableExecutor()
{
}

int ObAlterTableExecutor::refresh_schema_for_table(
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  share::schema::ObSchemaGetterGuard schema_guard;
  const observer::ObGlobalContext &gctx = observer::ObServer::get_instance().get_gctx();
  ObMultiVersionSchemaService *schema_service = gctx.schema_service_;
  int64_t local_version = OB_INVALID_VERSION;
  int64_t global_version = OB_INVALID_VERSION;

  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, schema service must not be NULL", K(ret));
  } else if (OB_FAIL(schema_service->get_tenant_refreshed_schema_version(
          tenant_id, local_version))) {
    LOG_WARN("fail to get local version", K(ret), "tenant_id", tenant_id);
  } else if (OB_FAIL(schema_service->get_tenant_received_broadcast_version(
          tenant_id, global_version))) {
    LOG_WARN("fail to get global version", K(ret), "tenant_id", tenant_id);
  } else if (local_version < global_version) {
    LOG_INFO("try to refresh schema", K(local_version), K(global_version));
    // force refresh schema最新版本
    ObSEArray<uint64_t, 1> tenant_ids;
    if (OB_FAIL(tenant_ids.push_back(tenant_id))) {
      LOG_WARN("fail to push back tenant_id", K(ret), "tenant_id", tenant_id);
    } else if (OB_FAIL(schema_service->refresh_and_add_schema(tenant_ids))) {
      LOG_WARN("failed to refresh schema", K(ret));
    }
  }
  return ret;
}

/* 从 3100 开始的版本 alter table 逻辑是将建索引和其他操作放到同一个 rpc 里发到 rs，返回后对每个创建的索引进行同步等，如果一个索引创建失败，则回滚全部索引
   mysql 模式下支持 alter table 同时做建索引操作和其他操作，需要保证 rs 在处理 drop index 之后再处理 add index
   否则前缀索引会有问题：
*/
int ObAlterTableExecutor::alter_table_rpc_v2(
    obrpc::ObAlterTableArg &alter_table_arg,
    obrpc::ObAlterTableRes &res,
    common::ObIAllocator &allocator,
    obrpc::ObCommonRpcProxy *common_rpc_proxy,
    ObSQLSessionInfo *my_session,
    const bool is_sync_ddl_user)
{
  int ret = OB_SUCCESS;
  // do not support cancel drop_index_task.
  bool is_support_cancel = true;
  const ObSArray<obrpc::ObIndexArg *> index_arg_list = alter_table_arg.index_arg_list_;
  ObSArray<obrpc::ObIndexArg *> add_index_arg_list;
  ObSArray<obrpc::ObIndexArg *> drop_index_args;
  alter_table_arg.index_arg_list_.reset();

  if (OB_ISNULL(my_session)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    alter_table_arg.compat_mode_ = ORACLE_MODE == my_session->get_compatibility_mode() ?
            lib::Worker::CompatMode::ORACLE : lib::Worker::CompatMode::MYSQL;
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < index_arg_list.size(); ++i) {
    obrpc::ObIndexArg *index_arg = index_arg_list.at(i);
    if (OB_ISNULL(index_arg)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("index arg should not be null", KR(ret));
    } else if (obrpc::ObIndexArg::ADD_INDEX == index_arg->index_action_type_) {
      if (OB_FAIL(add_index_arg_list.push_back(index_arg))) {
        LOG_WARN("fail to push back to arg_for_adding_index_list", KR(ret));
      }
    } else if (obrpc::ObIndexArg::DROP_INDEX == index_arg->index_action_type_) {
      if (OB_FAIL(drop_index_args.push_back(index_arg))) {
        LOG_WARN("push back drop index arg failed", K(ret));
      } else if (OB_FAIL(alter_table_arg.index_arg_list_.push_back(index_arg))) {
        LOG_WARN("push back index arg failed", K(ret));
      } else {
        ObDropIndexArg *drop_index_arg = static_cast<ObDropIndexArg *>(index_arg);
        drop_index_arg->is_add_to_scheduler_ = true;
        is_support_cancel = false;
      }
    } else { // for rename/drop index action
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
    if (obrpc::ObAlterTableArg::SET_INTERVAL == alter_table_arg.alter_part_type_
        || obrpc::ObAlterTableArg::INTERVAL_TO_RANGE == alter_table_arg.alter_part_type_) {
      alter_table_arg.is_alter_partitions_ = true;
    }
    if (OB_FAIL(common_rpc_proxy->alter_table(alter_table_arg, res))) {
      LOG_WARN("rpc proxy alter table failed", KR(ret), "dst", common_rpc_proxy->get_server(), K(alter_table_arg));
    } else {
      // 在回滚时不会重试，也不检查 schema version
      alter_table_arg.based_schema_object_infos_.reset();
    }
  }

  if (OB_SUCC(ret)) {
    ObIArray<obrpc::ObDDLRes> &ddl_ress = res.ddl_res_array_;
    for (int64_t i = 0; OB_SUCC(ret) && i < ddl_ress.count(); ++i) {
      ObDDLRes &ddl_res = ddl_ress.at(i);
      if (OB_FAIL(ObDDLExecutorUtil::wait_ddl_finish(ddl_res.tenant_id_, ddl_res.task_id_, my_session, common_rpc_proxy, is_support_cancel))) {
        LOG_WARN("wait drop index finish", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObCreateIndexExecutor create_index_executor;
    uint64_t failed_index_no = OB_INVALID_ID;
    // 对drop/truncate分区全局索引的处理
    if (!is_sync_ddl_user && alter_table_arg.is_update_global_indexes_
        && (obrpc::ObAlterTableArg::DROP_PARTITION == alter_table_arg.alter_part_type_
        || obrpc::ObAlterTableArg::DROP_SUB_PARTITION == alter_table_arg.alter_part_type_
        || obrpc::ObAlterTableArg::TRUNCATE_PARTITION == alter_table_arg.alter_part_type_
        || obrpc::ObAlterTableArg::TRUNCATE_SUB_PARTITION == alter_table_arg.alter_part_type_)) {
      common::ObSArray<ObAlterTableResArg> &res_array = res.res_arg_array_;
      for (int64_t i = 0; OB_SUCC(ret) && i < res_array.size(); ++i) {
        SMART_VAR(obrpc::ObCreateIndexArg, create_index_arg) {
          create_index_arg.index_schema_.set_table_id(res_array.at(i).schema_id_);
          create_index_arg.index_schema_.set_schema_version(res_array.at(i).schema_version_);
          if (OB_FAIL(create_index_executor.sync_check_index_status(*my_session, *common_rpc_proxy, create_index_arg, res, allocator))) {
            LOG_WARN("failed to sync_check_index_status", KR(ret), K(create_index_arg), K(i));
          }
        }
      }
    } else if (DDL_CREATE_INDEX == res.ddl_type_ || DDL_NORMAL_TYPE == res.ddl_type_) {
      // TODO(shuangcan): alter table create index returns DDL_NORMAL_TYPE now, check if we can fix this later
      // 同步等索引建成功
      for (int64_t i = 0; OB_SUCC(ret) && i < add_index_arg_list.size(); ++i) {
        obrpc::ObIndexArg *index_arg = add_index_arg_list.at(i);
        obrpc::ObCreateIndexArg *create_index_arg = NULL;
        if (OB_ISNULL(index_arg)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("index arg is null", KR(ret), K(i));
        } else if (obrpc::ObIndexArg::ADD_INDEX != index_arg->index_action_type_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("index action type should be add index", KR(ret), K(i), K(*index_arg));
        } else if (OB_ISNULL(create_index_arg = static_cast<obrpc::ObCreateIndexArg *>(index_arg))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("create index arg is null", KR(ret), K(i));
        } else if (INDEX_TYPE_PRIMARY == create_index_arg->index_type_) {
          // do nothing
        } else if (!is_sync_ddl_user) {
          // 只考虑非备份恢复时的索引同步检查
          create_index_arg->index_schema_.set_table_id(res.res_arg_array_.at(i).schema_id_);
          create_index_arg->index_schema_.set_schema_version(res.res_arg_array_.at(i).schema_version_);
          // 只考虑非备份恢复时的索引同步检查
          if (OB_FAIL(create_index_executor.sync_check_index_status(*my_session, *common_rpc_proxy, *create_index_arg, res, allocator))) {
            failed_index_no = i;
            LOG_WARN("failed to sync_check_index_status", KR(ret), K(*create_index_arg), K(i));
          }
        }
      }
      // 回滚所有已经建立的 index
      if (OB_FAIL(ret)) {
        int tmp_ret = OB_SUCCESS;
        uint64_t tenant_id = OB_INVALID_ID;
        for (int64_t i = 0; (OB_SUCCESS == tmp_ret) && (i < add_index_arg_list.size()); ++i) {
          if (failed_index_no == i) {
            // 同步建索引逻辑里已经把这个失败的删掉了
            continue;
          } else {
            obrpc::ObDropIndexArg drop_index_arg;
            obrpc::ObDropIndexRes drop_index_res;
            obrpc::ObCreateIndexArg *create_index_arg = static_cast<obrpc::ObCreateIndexArg *>(add_index_arg_list.at(i));
            drop_index_arg.tenant_id_ = create_index_arg->tenant_id_;
            drop_index_arg.exec_tenant_id_ = create_index_arg->tenant_id_;
            drop_index_arg.index_table_id_ = res.res_arg_array_.at(i).schema_id_;
            drop_index_arg.session_id_ = create_index_arg->session_id_;
            drop_index_arg.index_name_ = create_index_arg->index_name_;
            drop_index_arg.table_name_ = create_index_arg->table_name_;
            drop_index_arg.database_name_ = create_index_arg->database_name_;
            drop_index_arg.index_action_type_ = obrpc::ObIndexArg::DROP_INDEX;
            drop_index_arg.is_add_to_scheduler_ = false;
            tenant_id = drop_index_arg.tenant_id_;
            if (OB_SUCCESS != (tmp_ret = create_index_executor.set_drop_index_stmt_str(drop_index_arg, allocator))) {
              LOG_WARN("fail to set drop index ddl_stmt_str", K(tmp_ret));
            } else if (OB_SUCCESS != (tmp_ret = common_rpc_proxy->drop_index(drop_index_arg, drop_index_res))) {
              LOG_WARN("rpc proxy drop index failed", "dst", common_rpc_proxy->get_server(),
                                                      K(tmp_ret),
                                                      K(drop_index_arg.table_name_),
                                                      K(drop_index_arg.index_name_));
            }
          }
        }
        if (OB_SUCCESS != tmp_ret && OB_INVALID_ID != failed_index_no) {    // rewrite LOG_USER_ERROR message
          uint64_t index_table_id = res.res_arg_array_.at(failed_index_no).schema_id_;
          int64_t schema_version = res.res_arg_array_.at(failed_index_no).schema_version_;
          bool is_finish = false;
          if (OB_SUCCESS != (tmp_ret = ObDDLExecutorUtil::wait_build_index_finish(tenant_id, res.task_id_, is_finish))) {
            LOG_WARN("wait build index finish failed", K(tmp_ret), K(tenant_id), K(res.task_id_));
          }
        }
        LOG_INFO("added indexes failed, we rolled back all indexes added in this same alter table sql. But we didn't roll back other actions in this same alter table sql");
      }
    }
  }

  return ret;
}

int ObAlterTableExecutor::get_external_file_list(const ObString &location,
                                                 ObIArray<ObString> &file_urls,
                                                 ObIArray<int64_t> &file_sizes,
                                                 const ObString &access_info,
                                                 ObIAllocator &allocator,
                                                 common::ObStorageType &storage_type)
{
  int ret = OB_SUCCESS;
  ObExternalDataAccessDriver driver;
  if (OB_FAIL(driver.init(location, access_info))) {
    LOG_WARN("init external data access driver failed", K(ret));
  } else if (OB_FAIL(driver.get_file_list(location, file_urls, allocator))) {
    LOG_WARN("get file urls failed", K(ret));
  } else if (OB_FAIL(driver.get_file_sizes(location, file_urls, file_sizes))) {
    LOG_WARN("get file sizes failed", K(ret));
  }
  if (driver.is_opened()) {
    storage_type = driver.get_storage_type();
    driver.close();
  }

  LOG_DEBUG("show external table files", K(file_urls), K(storage_type), K(access_info));
  return ret;
}

int ObAlterTableExecutor::filter_and_sort_external_files(const ObString &pattern,
                                                         ObExecContext &exec_ctx,
                                                         ObIArray<ObString> &file_urls,
                                                         ObIArray<int64_t> &file_sizes) {
  int ret = OB_SUCCESS;
  const int64_t count = file_urls.count();
  ObSEArray<int64_t, 8> tmp_file_sizes;
  hash::ObHashMap<ObString, int64_t> file_map;
  if (0 == count) {
    /* do nothing */
  } else if (OB_UNLIKELY(count != file_sizes.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("array size error", K(ret));
  } else if (OB_FAIL(file_map.create(count, "ExtFileMap", "ExtFileMap"))) {
      LOG_WARN("fail to init hashmap", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      if (OB_FAIL(file_map.set_refactored(file_urls.at(i), file_sizes.at(i)))) {
        LOG_WARN("failed to set refactored to file_map", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObExternalTableUtils::filter_external_table_files(pattern, exec_ctx, file_urls))) {
        LOG_WARN("failed to filter external table files");
      }
    }
    if (OB_SUCC(ret)) {
      std::sort(file_urls.get_data(), file_urls.get_data() + file_urls.count());
      for (int64_t i = 0; OB_SUCC(ret) && i < file_urls.count(); ++i) {
        int64_t file_size = 0;
        if (OB_FAIL(file_map.get_refactored(file_urls.at(i), file_size))) {
          if (OB_UNLIKELY(OB_HASH_NOT_EXIST == ret)) {
            ret = OB_ERR_UNEXPECTED;
          }
          LOG_WARN("failed to get key meta", K(ret));
        } else if (OB_FAIL(tmp_file_sizes.push_back(file_size))) {
          LOG_WARN("failed to push back into tmp_file_sizes", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(file_sizes.assign(tmp_file_sizes))) {
        LOG_WARN("failed to assign file_sizes", K(ret));
      } else if (OB_FAIL(file_map.destroy())) {
        LOG_WARN("failed to destory file_map");
      }
    }
  }
  LOG_TRACE("after filter external table files", K(ret), K(file_urls));
  return ret;
}

int ObAlterTableExecutor::flush_external_file_cache(
    const uint64_t tenant_id,
    const uint64_t table_id,
    const ObIArray<ObAddr> &all_servers)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObAsyncRpcTaskWaitContext<ObRpcAsyncFlushExternalTableKVCacheCallBack> context;
  int64_t send_task_count = 0;
  OZ (context.init());
  OZ (context.get_cb_list().reserve(all_servers.count()));
  for (int64_t i = 0; OB_SUCC(ret) && i < all_servers.count(); i++) {
    ObFlushExternalTableFileCacheReq req;
    int64_t timeout = ObExternalTableFileManager::CACHE_EXPIRE_TIME;
    req.tenant_id_ = tenant_id;
    req.table_id_ = table_id;
    req.partition_id_ = 0;
    ObRpcAsyncFlushExternalTableKVCacheCallBack* async_cb = nullptr;
    if (OB_ISNULL(async_cb = OB_NEWx(ObRpcAsyncFlushExternalTableKVCacheCallBack, (&allocator), (&context)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate async cb memory", K(ret));
    }
    OZ (context.get_cb_list().push_back(async_cb));
    OZ (GCTX.external_table_proxy_->to(all_servers.at(i))
                                            .by(tenant_id)
                                            .timeout(timeout)
                                            .flush_file_kvcahce(req, async_cb));
    if (OB_SUCC(ret)) {
      send_task_count++;
    }
  }

  context.set_task_count(send_task_count);

  do {
    int temp_ret = context.wait_executing_tasks();
    if (OB_SUCCESS != temp_ret) {
      LOG_WARN("fail to wait executing task", K(temp_ret));
      if (OB_SUCC(ret)) {
        ret = temp_ret;
      }
    }
  } while(0);

  for (int64_t i = 0; OB_SUCC(ret) && i < context.get_cb_list().count(); i++) {
    ret = context.get_cb_list().at(i)->get_task_resp().rcode_.rcode_;
    if (OB_FAIL(ret)) {
      if (OB_TIMEOUT == ret) {
        // flush timeout is OK, because the file cache has already expire
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("async flush kvcache process failed", K(ret));
      }
    }
  }
  for (int64_t i = 0; i < context.get_cb_list().count(); i++) {
    context.get_cb_list().at(i)->~ObRpcAsyncFlushExternalTableKVCacheCallBack();
  }
  return ret;
}

int ObAlterTableExecutor::collect_local_files_on_servers(
    const uint64_t tenant_id,
    const ObString &location,
    ObIArray<ObAddr> &all_servers,
    ObIArray<ObString> &file_urls,
    ObIArray<int64_t> &file_sizes,
    ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObAddr, 8> target_servers;
  ObArray<ObString> server_ip_port;

  bool is_absolute_path = false;
  const int64_t PREFIX_LEN = STRLEN(OB_FILE_PREFIX);
  if (location.length() <= PREFIX_LEN) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid location", K(ret), K(location));
  } else {
    is_absolute_path = ('/' == location.ptr()[PREFIX_LEN]);
  }

  if (OB_SUCC(ret)) {
    if (is_absolute_path) {
      std::sort(all_servers.get_data(), all_servers.get_data() + all_servers.count(),
                [](const ObAddr &l, const ObAddr &r) -> bool { return l < r; });
      ObAddr pre_addr;
      for (int64_t i = 0; OB_SUCC(ret) && i < all_servers.count(); i++) {
        ObAddr &cur_addr = all_servers.at(i);
        if (!cur_addr.is_equal_except_port(pre_addr)) {
          pre_addr = cur_addr;
          OZ(target_servers.push_back(cur_addr));
        }
      }
    } else {
      OZ (target_servers.assign(all_servers));
    }
  }

  if (OB_SUCC(ret)) {
    ObAsyncRpcTaskWaitContext<ObRpcAsyncLoadExternalTableFileCallBack> context;
    int64_t send_task_count = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < target_servers.count(); i++) {
      const int64_t ip_len = 64;
      char *ip_port_buffer = nullptr;
      if (OB_ISNULL(ip_port_buffer = (char*)(allocator.alloc(ip_len)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate ip memory", K(ret));
      }
      OZ (target_servers.at(i).ip_port_to_string(ip_port_buffer, ip_len));
      OZ (server_ip_port.push_back(ObString(ip_port_buffer)));
    }
    OZ (context.init());
    OZ (context.get_cb_list().reserve(target_servers.count()));
    for (int64_t i = 0; OB_SUCC(ret) && i < target_servers.count(); i++) {
      const int64_t timeout = 10 * 1000000L; //10s
      ObRpcAsyncLoadExternalTableFileCallBack* async_cb = nullptr;
      ObLoadExternalFileListReq req;
      req.location_ = location;

      if (OB_ISNULL(async_cb = OB_NEWx(ObRpcAsyncLoadExternalTableFileCallBack, (&allocator), (&context)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate async cb memory", K(ret));
      }
      OZ (context.get_cb_list().push_back(async_cb));
      OZ (GCTX.external_table_proxy_->to(target_servers.at(i))
                                        .by(tenant_id)
                                        .timeout(timeout)
                                        .load_external_file_list(req, async_cb));
      if (OB_SUCC(ret)) {
        send_task_count++;
      }
    }

    context.set_task_count(send_task_count);

    do {
      int temp_ret = context.wait_executing_tasks();
      if (OB_SUCCESS != temp_ret) {
        LOG_WARN("fail to wait executing task", K(temp_ret));
        if (OB_SUCC(ret)) {
          ret = temp_ret;
        }
      }
    } while(0);

    for (int64_t i = 0; OB_SUCC(ret) && i < context.get_cb_list().count(); i++) {
      if (OB_FAIL(context.get_cb_list().at(i)->get_task_resp().rcode_.rcode_)) {
        LOG_WARN("async load files process failed", K(ret));
      } else {
        const ObIArray<ObString> &resp_array = context.get_cb_list().at(i)->get_task_resp().file_urls_;
        OZ (append(file_sizes, context.get_cb_list().at(i)->get_task_resp().file_sizes_));
        for (int64_t j = 0; OB_SUCC(ret) && j < resp_array.count(); j++) {
          ObSqlString tmp_file_url;
          ObString file_url;
          OZ (tmp_file_url.append(server_ip_port.at(i)));
          OZ (tmp_file_url.append("%"));
          OZ (tmp_file_url.append(resp_array.at(j)));
          OZ (ob_write_string(allocator, tmp_file_url.string(), file_url));
          OZ (file_urls.push_back(file_url));
        }
      }
      LOG_DEBUG("get external table file", K(context.get_cb_list().at(i)->get_task_resp().file_urls_));
    }

    for (int64_t i = 0; i < context.get_cb_list().count(); i++) {
      context.get_cb_list().at(i)->~ObRpcAsyncLoadExternalTableFileCallBack();
    }
  }
  LOG_DEBUG("update external table file list", K(ret), K(file_urls));
  return ret;
}

int ObAlterTableExecutor::update_external_file_list(
    const uint64_t tenant_id,
    const uint64_t table_id,
    const ObString &location,
    const ObString &access_info,
    const ObString &pattern,
    ObExecContext &exec_ctx)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObString, 8> file_urls;
  ObSEArray<int64_t, 8> file_sizes;
  ObArenaAllocator allocator;
  ObSEArray<ObAddr, 8> all_servers;
  OZ (GCTX.location_service_->external_table_get(tenant_id, table_id, all_servers));

  if (ObSQLUtils::is_external_files_on_local_disk(location)) {
    OZ (collect_local_files_on_servers(tenant_id, location, all_servers, file_urls, file_sizes, allocator));
  } else {
    OZ (ObExternalTableFileManager::get_instance().get_external_file_list_on_device(
          location, file_urls, file_sizes, access_info, allocator));
  }

  OZ (filter_and_sort_external_files(pattern, exec_ctx, file_urls, file_sizes));

  //TODO [External Table] opt performance
  OZ (ObExternalTableFileManager::get_instance().update_inner_table_file_list(tenant_id, table_id, file_urls, file_sizes));

  OZ (flush_external_file_cache(tenant_id, table_id, all_servers));
  return ret;
}

int ObAlterTableExecutor::execute_alter_external_table(ObExecContext &ctx, ObAlterTableStmt &stmt)
{
  int ret = OB_SUCCESS;
  obrpc::ObAlterTableArg &arg = stmt.get_alter_table_arg();
  int64_t option = stmt.get_alter_external_table_type();
  switch (option) {
    case T_ALTER_REFRESH_EXTERNAL_TABLE: {
      OZ (update_external_file_list(stmt.get_tenant_id(),
                                  arg.alter_table_schema_.get_table_id(),
                                  arg.alter_table_schema_.get_external_file_location(),
                                  arg.alter_table_schema_.get_external_file_location_access_info(),
                                  arg.alter_table_schema_.get_external_file_pattern(),
                                  ctx));
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected option", K(ret), K(option));
    }

  }
  return ret;
}

int ObAlterTableExecutor::execute(ObExecContext &ctx, ObAlterTableStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  obrpc::ObAlterTableArg &alter_table_arg = stmt.get_alter_table_arg();
  LOG_DEBUG("start of alter table execute", K(alter_table_arg));
  ObString first_stmt;
  OZ (stmt.get_first_stmt(first_stmt));
  OV (OB_NOT_NULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx)), OB_NOT_INIT);
  OZ (task_exec_ctx->get_common_rpc(common_rpc_proxy));
  OV (OB_NOT_NULL(common_rpc_proxy));
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (stmt.is_alter_triggers()) {
    if (stmt.get_tg_arg().trigger_infos_.count() > 0) {
      stmt.get_tg_arg().exec_tenant_id_ = alter_table_arg.exec_tenant_id_;
      stmt.get_tg_arg().ddl_id_str_ = alter_table_arg.ddl_id_str_;
      stmt.get_tg_arg().ddl_stmt_str_ = first_stmt;
      OZ (common_rpc_proxy->alter_trigger(stmt.get_tg_arg()), common_rpc_proxy->get_server());
    }
  } else if (alter_table_arg.alter_table_schema_.is_external_table()) {
    OZ (execute_alter_external_table(ctx, stmt));
  } else {
    ObSQLSessionInfo *my_session = NULL;
    obrpc::ObAlterTableRes res;
    bool is_sync_ddl_user = false;
    bool need_modify_fk_validate = false;
    bool need_check = false;
    bool need_modify_notnull_validate = false;
    bool is_oracle_mode = false;
    const int64_t tenant_id = alter_table_arg.alter_table_schema_.get_tenant_id();
    ObArenaAllocator allocator(ObModIds::OB_SQL_EXECUTOR);
    if (OB_FAIL(stmt.get_first_stmt(first_stmt))) {
      LOG_WARN("get first statement failed", K(ret));
    } else {
      alter_table_arg.ddl_stmt_str_ = first_stmt;
      my_session = ctx.get_my_session();
      if (NULL == my_session) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get my session", K(ret), K(ctx));
      } else if (FALSE_IT(alter_table_arg.sql_mode_ = my_session->get_sql_mode())) {
        // do nothing
      } else if (FALSE_IT(alter_table_arg.parallelism_ = stmt.get_parallelism())) {
      } else if (FALSE_IT(alter_table_arg.consumer_group_id_ = THIS_WORKER.get_group_id())) {
      } else if (OB_FAIL(check_alter_partition(ctx, stmt, alter_table_arg))) {
        LOG_WARN("check alter partition failed", K(ret));
      } else if (OB_FAIL(alter_table_arg.alter_table_schema_.check_if_oracle_compat_mode(is_oracle_mode))) {
        LOG_WARN("fail to check if tenant mode is oracle mode", K(ret));
      } else if (!is_oracle_mode && OB_FAIL(check_alter_part_key(ctx, alter_table_arg))) {
        LOG_WARN("check alter part key failed", K(ret));
      } else if (OB_FAIL(set_index_arg_list(ctx, stmt))) {
        LOG_WARN("fail to set index_arg_list", K(ret));
      } else if (OB_FAIL(ObResolverUtils::check_sync_ddl_user(my_session, is_sync_ddl_user))) {
        LOG_WARN("Failed to check sync_dll_user", K(ret));
      } else if (OB_INVALID_ID == alter_table_arg.session_id_
                && 0 != my_session->get_sessid_for_table()
                && FALSE_IT(alter_table_arg.session_id_ = my_session->get_sessid_for_table())) {
        //impossible
      } else {
        int64_t foreign_key_checks = 0;
        my_session->get_foreign_key_checks(foreign_key_checks);
        alter_table_arg.foreign_key_checks_ = is_oracle_mode || (!is_oracle_mode && foreign_key_checks);
        if ((obrpc::ObAlterTableArg::ADD_CONSTRAINT == alter_table_arg.alter_constraint_type_
            || (obrpc::ObAlterTableArg::ALTER_CONSTRAINT_STATE == alter_table_arg.alter_constraint_type_))) {
          if (OB_FAIL(need_check_constraint_validity(alter_table_arg, need_check))) {
            LOG_WARN("check whether need check failed", K(ret));
          }
        }
        // 如果追加 validate 属性的外键或者 modify 外键为 validate 属性时，不立即生效
        // 校验已有数据满足外键的 validate 属性要求之后再生效，确保优化器可以正确地根据外键的 validate 属性进行优化
        if (OB_SUCC(ret) && alter_table_arg.foreign_key_checks_ && 1 == alter_table_arg.foreign_key_arg_list_.count()) {
          if ((!alter_table_arg.foreign_key_arg_list_.at(0).is_modify_fk_state_
              && alter_table_arg.foreign_key_arg_list_.at(0).validate_flag_)
              || (alter_table_arg.foreign_key_arg_list_.at(0).is_modify_validate_flag_
                  && alter_table_arg.foreign_key_arg_list_.at(0).validate_flag_)) {
            need_modify_fk_validate = true;
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(alter_table_rpc_v2(
                      alter_table_arg,
                      res,
                      allocator,
                      common_rpc_proxy,
                      my_session,
                      is_sync_ddl_user))) {
            LOG_WARN("Failed to alter table rpc v2", K(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (!need_check) {
        // do nothing, don't check if data is valid
      } else if (OB_FAIL(refresh_schema_for_table(tenant_id))) {
        LOG_WARN("refresh_schema_for_table failed", K(ret));
      } else if (OB_FAIL(ObDDLExecutorUtil::wait_ddl_finish(tenant_id, res.task_id_, my_session, common_rpc_proxy))) {
        LOG_WARN("wait check constraint finish", K(ret));
      }
    }
    if (OB_SUCC(ret)
        && 1 == alter_table_arg.foreign_key_arg_list_.count()) {
      if (!need_modify_fk_validate) {
        // do nothing, don't check if data is valid
      } else {
        if (OB_FAIL(refresh_schema_for_table(tenant_id))) {
          LOG_WARN("refresh_schema_for_table failed", K(ret));
        } else if (OB_FAIL(ObDDLExecutorUtil::wait_ddl_finish(tenant_id, res.task_id_, my_session, common_rpc_proxy))) {
          LOG_WARN("wait fk constraint finish", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      const bool need_wait_ddl_finish = is_double_table_long_running_ddl(res.ddl_type_)
                                     || is_simple_table_long_running_ddl(res.ddl_type_);
      if (OB_SUCC(ret) && need_wait_ddl_finish) {
        int64_t affected_rows = 0;
        if (OB_FAIL(refresh_schema_for_table(alter_table_arg.exec_tenant_id_))) {
          LOG_WARN("refresh_schema_for_table failed", K(ret));
        } else if (OB_FAIL(ObDDLExecutorUtil::wait_ddl_finish(tenant_id, res.task_id_, my_session, common_rpc_proxy))) {
          LOG_WARN("fail to wait ddl finish", K(ret), K(tenant_id), K(res));
        }
      }
    }
  }
  return ret;
}

int ObAlterTableExecutor::need_check_constraint_validity(obrpc::ObAlterTableArg &alter_table_arg, bool &need_check)
{
  int ret = OB_SUCCESS;
  need_check = false;
  if (obrpc::ObAlterTableArg::ADD_CONSTRAINT != alter_table_arg.alter_constraint_type_
      && obrpc::ObAlterTableArg::ALTER_CONSTRAINT_STATE != alter_table_arg.alter_constraint_type_) {
  } else {
    ObTableSchema::const_constraint_iterator iter =
        alter_table_arg.alter_table_schema_.constraint_begin();
    for(; iter != alter_table_arg.alter_table_schema_.constraint_end() && OB_SUCC(ret) && !need_check; iter++) {
      if (obrpc::ObAlterTableArg::ALTER_CONSTRAINT_STATE == alter_table_arg.alter_constraint_type_) {
        if ((*iter)->get_is_modify_validate_flag() && (*iter)->is_validated()) {
          need_check = true;
        }
      } else if (CONSTRAINT_TYPE_CHECK == (*iter)->get_constraint_type()) {
        if ((*iter)->is_validated()) {
          need_check = (*iter)->is_validated();
        }
      } else if (CONSTRAINT_TYPE_NOT_NULL == (*iter)->get_constraint_type()) {
        if (1 != (*iter)->get_column_cnt()) {
          ret = OB_ERR_UNEXPECTED;
        } else if (OB_INVALID_ID == *(*iter)->cst_col_begin()) {
          // alter table add column not null.
          ObTableSchema::const_column_iterator target_col_iter = NULL;
          ObTableSchema::const_column_iterator cst_col_iter =
                          alter_table_arg.alter_table_schema_.column_begin();
          ObString cst_col_name;
          if (OB_FAIL((*iter)->get_not_null_column_name(cst_col_name))) {
            LOG_WARN("get not null column name failed", K(ret));
          } else {
            for(; NULL == target_col_iter
                  && cst_col_iter != alter_table_arg.alter_table_schema_.column_end();
                cst_col_iter++) {
              if ((*cst_col_iter)->get_column_name_str().length() == cst_col_name.length()
                  && 0 == (*cst_col_iter)->get_column_name_str().compare(cst_col_name)) {
                target_col_iter = cst_col_iter;
              }
            }
            if (OB_ISNULL(target_col_iter)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("column schema not found", K(ret),K(alter_table_arg.alter_table_schema_),
                        K(cst_col_name));
            } else {
              const ObObj &cur_default_value = (*target_col_iter)->get_cur_default_value();
              need_check = cur_default_value.is_null() ||
                (cur_default_value.is_string_type()
                  && (0 == cur_default_value.get_string().case_compare(N_NULL)
                      || 0 == cur_default_value.get_string().case_compare("''")));
            }
          }
        } else {
          // alter table modify column not null.
          need_check = (*iter)->is_validated();
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected constraint type", K(ret));
      }
      if (OB_SUCC(ret) && !need_check) {
        (*iter)->set_need_validate_data(false);
      }
    }
  }
  return ret;
}

int ObAlterTableExecutor::set_alter_col_nullable_ddl_stmt_str(
    obrpc::ObAlterTableArg &alter_table_arg,
    common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  AlterTableSchema &alter_table_schema = alter_table_arg.alter_table_schema_;
  ObString column_name;
  const ObColumnSchemaV2 *col_schema = NULL;
  char *buf = NULL;
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
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret), K(OB_MAX_SQL_LENGTH));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos,
                     "ALTER TABLE \"%.*s\".\"%.*s\" MODIFY COLUMN %.*s NULL",
                     alter_table_schema.get_origin_database_name().length(),
                     alter_table_schema.get_origin_database_name().ptr(),
                     alter_table_schema.get_origin_table_name().length(),
                     alter_table_schema.get_origin_table_name().ptr(),
                     column_name.length(), column_name.ptr()))) {
    LOG_WARN("fail to print ddl_stmt_str for rollback", K(ret));
  } else {
    alter_table_arg.ddl_stmt_str_.assign_ptr(buf, static_cast<int32_t>(pos));
  }

  return ret;
}

int ObAlterTableExecutor::resolve_alter_column_partition_expr(
    const share::schema::ObColumnSchemaV2 &col_schema,
    const share::schema::ObTableSchema &table_schema,
    ObSchemaGetterGuard &schema_guard,
    ObSQLSessionInfo &session_info,
    common::ObIAllocator &allocator,
    const bool is_sub_part,
    ObExprResType &dst_res_type)
{
  int ret = OB_SUCCESS;
  ObRawExpr *part_expr = NULL;
  ObRawExprFactory expr_factory(allocator);
  ObSchemaChecker schema_checker;
  if (OB_FAIL(schema_checker.init(schema_guard))) {
    LOG_WARN("fail to init schema_checker", K(ret));
  } else {
    ObResolverParams resolver_ctx;
    ObStmtFactory stmt_factory(allocator);
    TableItem table_item;
    resolver_ctx.allocator_ = &allocator;
    resolver_ctx.schema_checker_ = &schema_checker;
    resolver_ctx.session_info_ = &session_info;
    resolver_ctx.disable_privilege_check_ = PRIV_CHECK_FLAG_DISABLE;
    resolver_ctx.expr_factory_ = &expr_factory;
    resolver_ctx.stmt_factory_ = &stmt_factory;
    resolver_ctx.query_ctx_ = stmt_factory.get_query_ctx();
    table_item.table_id_ = table_schema.get_table_id();
    table_item.ref_id_ = table_schema.get_table_id();
    table_item.type_ = TableItem::BASE_TABLE;
    // This is just to use the resolver to resolve the partition expr interface.
    // The resolver of any statement has this ability. The reason for using the delete
    // resolver is that the delete resolver is the simplest
    ObPartitionFuncType part_type;
    SMART_VAR (ObDeleteResolver, delete_resolver, resolver_ctx) {
      ObDeleteStmt *delete_stmt = delete_resolver.create_stmt<ObDeleteStmt>();
      CK (OB_NOT_NULL(delete_stmt));
      CK (OB_NOT_NULL(resolver_ctx.query_ctx_));
      OZ (delete_stmt->get_table_items().push_back(&table_item));
      OZ (delete_stmt->set_table_bit_index(table_schema.get_table_id()));
      if (!is_sub_part) {
        const ObString &part_str = table_schema.get_part_option().get_part_func_expr_str();
        part_type = table_schema.get_part_option().get_part_func_type();
        OZ (delete_resolver.resolve_partition_expr(table_item, table_schema,
        part_type, part_str, part_expr));
      } else {
        const ObString &part_str = table_schema.get_sub_part_option().get_part_func_expr_str();
        part_type = table_schema.get_sub_part_option().get_part_func_type();
        OZ (delete_resolver.resolve_partition_expr(table_item, table_schema,
        part_type, part_str, part_expr));
      }
      CK (OB_NOT_NULL(part_expr));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (part_expr->is_column_ref_expr()) {
    ObColumnRefRawExpr *column_ref = static_cast<ObColumnRefRawExpr*>(part_expr);
    if (column_ref->get_column_id() == col_schema.get_column_id()) {
      if (OB_FAIL(ObRawExprUtils::init_column_expr(col_schema, *column_ref))) {
        LOG_WARN("init column expr failed", K(ret));
      } else if (CS_TYPE_INVALID == column_ref->get_collation_type()) {
        column_ref->set_collation_type(table_schema.get_collation_type());
      }
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < part_expr->get_param_count(); ++i) {
      ObRawExpr *sub_expr = part_expr->get_param_expr(i);
      if (OB_ISNULL(sub_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sub_expr should not be null", K(ret));
      } else if (sub_expr->is_column_ref_expr()) {
        ObColumnRefRawExpr *column_ref = static_cast<ObColumnRefRawExpr*>(sub_expr);
        if (column_ref->get_column_id() == col_schema.get_column_id()) {
          if (OB_FAIL(ObRawExprUtils::init_column_expr(col_schema, *column_ref))) {
            LOG_WARN("init column expr failed", K(ret));
          } else if (CS_TYPE_INVALID == column_ref->get_collation_type()) {
            column_ref->set_collation_type(table_schema.get_collation_type());
          }
        }
      }
    }
  }
  OZ (part_expr->formalize(&session_info));
  OX (dst_res_type = part_expr->get_result_type());
  return ret;
}

template<class T>
int ObAlterTableExecutor::calc_range_part_high_bound(
    const ObPartitionFuncType part_func_type,
    const ObString &col_name,
    const ObExprResType &dst_res_type,
    T &part,
    ObExecContext &ctx)
{
  int ret = OB_SUCCESS;
  ObExprCtx expr_ctx;
  const ObObjType fun_expr_type = dst_res_type.get_type();
  const ObCollationType fun_collation_type = dst_res_type.get_collation_type();
  ObObjType expected_obj_type = fun_expr_type;
  ObSEArray<ObObj, OB_DEFAULT_ARRAY_SIZE> range_partition_obj;
  if (OB_FAIL(ObSQLUtils::wrap_expr_ctx(stmt::T_ALTER_TABLE, ctx, ctx.get_allocator(), expr_ctx))) {
    LOG_WARN("Failed to wrap expr ctx", K(ret));
  } else {
    expr_ctx.cast_mode_ = CM_WARN_ON_FAIL; //always set to WARN_ON_FAIL to allow calculate
    EXPR_SET_CAST_CTX_MODE(expr_ctx);
    const common::ObRowkey &row_key = part.get_high_bound_val();
    int64_t obj_cnt = row_key.get_obj_cnt();
    for (int64_t i = 0; OB_SUCC(ret) && i < obj_cnt; ++i) {
      const ObObj &src_obj = row_key.get_obj_ptr()[i];
      if (src_obj.is_max_value()) {
        if (OB_FAIL(range_partition_obj.push_back(src_obj))) {
          LOG_WARN("array push back fail", K(ret));
        }
      } else {
        if (ob_is_integer_type(fun_expr_type)) {
          // Type promotion to int64 or uint64
          expected_obj_type = ob_is_int_tc(fun_expr_type) ? ObIntType : ObUInt64Type;
        }
        const ObObj *dst_obj = NULL;
        EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
        cast_ctx.dest_collation_ = fun_collation_type;
        EXPR_CAST_OBJ_V2(expected_obj_type, src_obj, dst_obj);
        if (OB_SUCC(ret)) {
          if (OB_ISNULL(dst_obj)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("succ to cast obj, but dst_obj is NULL", K(ret),
                    K(expected_obj_type), K(fun_expr_type), K(src_obj));
          } else if ((PARTITION_FUNC_TYPE_RANGE_COLUMNS == part_func_type
                    || PARTITION_FUNC_TYPE_LIST_COLUMNS == part_func_type)
                    && OB_FAIL(ObResolverUtils::check_partition_range_value_result_type(part_func_type,
                                                                                        dst_res_type,
                                                                                        col_name,
                                                                                        const_cast<ObObj&>(*dst_obj)))) {
              LOG_WARN("get partition range value result type failed", K(ret));
          } else if (OB_FAIL(range_partition_obj.push_back(*dst_obj))) {
            LOG_WARN("array push back fail", K(ret));
          }
        } else if (OB_ERR_UNEXPECTED != ret) {
          ret = OB_ERR_WRONG_TYPE_COLUMN_VALUE_ERROR;
          LOG_WARN("failed to cast obj", K(expected_obj_type), K(fun_expr_type), K(src_obj));
        }
      }
    }
    if (OB_SUCC(ret)) {
      ObRowkey high_rowkey(&range_partition_obj.at(0), range_partition_obj.count());
      part.reset_high_bound_val();
      if (OB_FAIL(part.set_high_bound_val(high_rowkey))) {
        LOG_WARN("deep_copy_str fail", K(ret));
      }
    }
  }
  return ret;
}

int ObAlterTableExecutor::calc_range_values_exprs(
    const ObColumnSchemaV2 &col_schema,
    const share::schema::ObTableSchema &orig_table_schema,
    share::schema::ObTableSchema &new_table_schema,
    ObSchemaGetterGuard &schema_guard,
    ObSQLSessionInfo &session_info,
    common::ObIAllocator &allocator,
    ObExecContext &ctx,
    const bool is_subpart)
{
  int ret = OB_SUCCESS;
  ObExprResType dst_res_type;
  if (OB_FAIL(resolve_alter_column_partition_expr(col_schema, orig_table_schema, schema_guard,
              session_info, allocator, is_subpart, dst_res_type))) {
    LOG_WARN("failed to resolve alter column partition expr", K(ret));
  } else if (is_subpart) {
    const int64_t part_num = new_table_schema.get_partition_num();
    const ObPartitionFuncType part_func_type = orig_table_schema.get_sub_part_option().get_part_func_type();
    ObPartition **part_array = new_table_schema.get_part_array();
    CK (OB_NOT_NULL(part_array));
    for (int64_t i = 0; i < part_num && OB_SUCC(ret); ++i) {
      ObPartition *part = part_array[i];
      CK (OB_NOT_NULL(part));
      CK (OB_NOT_NULL(part->get_subpart_array()));
      for (int64_t j = 0; OB_SUCC(ret) && j < part->get_subpartition_num(); j++) {
        ObSubPartition *sub_part = part->get_subpart_array()[j];
        CK (OB_NOT_NULL(sub_part));
        OZ (calc_range_part_high_bound(part_func_type,
                                       col_schema.get_column_name_str(),
                                       dst_res_type,
                                       *sub_part,
                                       ctx));
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(ObPartitionExecutorUtils::check_increasing_range_value(part->get_subpart_array(),
                                                                           part->get_subpartition_num(),
                                                                           stmt::T_ALTER_TABLE))) {
          LOG_WARN("check increasing range value failed", K(ret));
        }
      }
    }
  } else {
    const int64_t part_num = new_table_schema.get_partition_num();
    const ObPartitionFuncType part_func_type = orig_table_schema.get_part_option().get_part_func_type();
    ObPartition **part_array = new_table_schema.get_part_array();
    CK (OB_NOT_NULL(part_array));
    for (int64_t i = 0; i < part_num && OB_SUCC(ret); ++i) {
      ObPartition *part = part_array[i];
      CK (OB_NOT_NULL(part));
      OZ (calc_range_part_high_bound(part_func_type,
                                     col_schema.get_column_name_str(),
                                     dst_res_type,
                                     *part,
                                     ctx));
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObPartitionExecutorUtils::check_increasing_range_value(part_array,
                                                                         part_num,
                                                                         stmt::T_ALTER_TABLE))) {
        LOG_WARN("check increasing range value failed", K(ret));
      }
    }
  }
  return ret;
}

template<class T>
int ObAlterTableExecutor::calc_list_part_rows(
    const ObPartitionFuncType part_func_type,
    const ObString &col_name,
    const ObExprResType &dst_res_type,
    const T &orig_part,
    T &new_part,
    ObExecContext &ctx,
    common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObExprCtx expr_ctx;
  const ObObjType fun_expr_type = dst_res_type.get_type();
  const ObCollationType fun_collation_type = dst_res_type.get_collation_type();
  ObObjType expected_obj_type = fun_expr_type;
  ObSEArray<ObObj, OB_DEFAULT_ARRAY_SIZE> range_partition_obj;
  if (OB_FAIL(ObSQLUtils::wrap_expr_ctx(stmt::T_ALTER_TABLE, ctx, ctx.get_allocator(), expr_ctx))) {
    LOG_WARN("Failed to wrap expr ctx", K(ret));
  } else {
    new_part.reset_list_row_values();
    expr_ctx.cast_mode_ = CM_WARN_ON_FAIL; //always set to WARN_ON_FAIL to allow calculate
    EXPR_SET_CAST_CTX_MODE(expr_ctx);
    const common::ObIArray<common::ObNewRow>& row_list = orig_part.get_list_row_values();
    for (int64_t i = 0; OB_SUCC(ret) && i < row_list.count(); i ++) {
      const common::ObNewRow &row = row_list.at(i);
      for (int64_t j = 0; OB_SUCC(ret) && j < row.get_count(); ++j) {
        const ObObj &src_obj = row.get_cell(j);
        if (src_obj.is_max_value()) {
          if (OB_FAIL(range_partition_obj.push_back(src_obj))) {
            LOG_WARN("array push back fail", K(ret));
          }
        } else {
          if (ob_is_integer_type(fun_expr_type)) {
            // Type promotion to int64 or uint64
            expected_obj_type = ob_is_int_tc(fun_expr_type) ? ObIntType : ObUInt64Type;
          }
          const ObObj *dst_obj = NULL;
          EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
          cast_ctx.dest_collation_ = fun_collation_type;
          EXPR_CAST_OBJ_V2(expected_obj_type, src_obj, dst_obj);
          if (OB_SUCC(ret)) {
            if (OB_ISNULL(dst_obj)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("succ to cast obj, but dst_obj is NULL", K(ret),
                      K(expected_obj_type), K(fun_expr_type), K(src_obj));
            } else if ((PARTITION_FUNC_TYPE_RANGE_COLUMNS == part_func_type
                      || PARTITION_FUNC_TYPE_LIST_COLUMNS == part_func_type)
                      && OB_FAIL(ObResolverUtils::check_partition_range_value_result_type(part_func_type,
                                                                                          dst_res_type,
                                                                                          col_name,
                                                                                          const_cast<ObObj&>(*dst_obj)))) {
              LOG_WARN("get partition range value result type failed", K(ret));
            } else if (OB_FAIL(range_partition_obj.push_back(*dst_obj))) {
              LOG_WARN("array push back fail", K(ret));
            }
          } else if (OB_ERR_UNEXPECTED != ret) {
            ret = OB_ERR_WRONG_TYPE_COLUMN_VALUE_ERROR;
            LOG_WARN("failed to cast obj", K(expected_obj_type), K(fun_expr_type), K(src_obj));
          }
        }
      }
      if (OB_SUCC(ret)) {
        ObNewRow row;
        ObObj* obj_array = (ObObj *)allocator.alloc(range_partition_obj.count() * sizeof(ObObj));
        CK (OB_NOT_NULL(obj_array));
        for (int64_t k = 0; OB_SUCC(ret) && k < range_partition_obj.count(); k ++) {
          new (obj_array + k) ObObj();
          obj_array[k] = (&range_partition_obj.at(0))[k];
        }
        OX (row.assign(obj_array, range_partition_obj.count()));
        OZ (new_part.add_list_row(row));
        OX (range_partition_obj.reuse());
      }
    }
  }
  return ret;
}

int ObAlterTableExecutor::calc_list_values_exprs(
    const ObColumnSchemaV2 &col_schema,
    const share::schema::ObTableSchema &orig_table_schema,
    share::schema::ObTableSchema &new_table_schema,
    ObSchemaGetterGuard &schema_guard,
    ObSQLSessionInfo &session_info,
    common::ObIAllocator &allocator,
    ObExecContext &ctx,
    const bool is_subpart)
{
  int ret = OB_SUCCESS;
  ObExprResType dst_res_type;
  if (OB_FAIL(resolve_alter_column_partition_expr(col_schema, orig_table_schema, schema_guard,
              session_info, allocator, is_subpart, dst_res_type))) {
    LOG_WARN("failed to resolve alter column partition expr", K(ret));
  } else if (is_subpart) {
    const int64_t part_num = orig_table_schema.get_partition_num();
    const ObPartitionFuncType part_func_type = orig_table_schema.get_sub_part_option().get_part_func_type();
    ObPartition **orig_part_array = orig_table_schema.get_part_array();
    ObPartition **new_part_array = new_table_schema.get_part_array();
    CK (OB_NOT_NULL(orig_part_array) && OB_NOT_NULL(new_part_array));
    for (int64_t i = 0; i < part_num && OB_SUCC(ret); ++i) {
      ObPartition *orig_part = orig_part_array[i];
      ObPartition *new_part = new_part_array[i];
      CK (OB_NOT_NULL(orig_part) && OB_NOT_NULL(new_part));
      CK (OB_NOT_NULL(orig_part->get_subpart_array()));
      CK (OB_NOT_NULL(orig_part->get_subpart_array()));
      for (int64_t j = 0; OB_SUCC(ret) && j < orig_part->get_subpartition_num(); j++) {
        ObSubPartition *orig_sub_part = orig_part->get_subpart_array()[j];
        ObSubPartition *new_sub_part = new_part->get_subpart_array()[j];
        CK (OB_NOT_NULL(orig_sub_part) && OB_NOT_NULL(new_sub_part));
        OZ (calc_list_part_rows(part_func_type,
                                col_schema.get_column_name_str(),
                                dst_res_type,
                                *orig_sub_part,
                                *new_sub_part,
                                ctx,
                                allocator));
      }
    }
  } else {
    const int64_t part_num = orig_table_schema.get_partition_num();
    const ObPartitionFuncType part_func_type = orig_table_schema.get_part_option().get_part_func_type();
    ObPartition **orig_part_array = orig_table_schema.get_part_array();
    ObPartition **new_part_array = new_table_schema.get_part_array();
    CK (OB_NOT_NULL(orig_part_array) && OB_NOT_NULL(new_part_array));
    for (int64_t i = 0; i < part_num && OB_SUCC(ret); ++i) {
      ObPartition *orig_part = orig_part_array[i];
      ObPartition *new_part = new_part_array[i];
      CK (OB_NOT_NULL(orig_part) && OB_NOT_NULL(new_part));
      OZ (calc_list_part_rows(part_func_type,
                              col_schema.get_column_name_str(),
                              dst_res_type,
                              *orig_part,
                              *new_part,
                              ctx,
                              allocator));
    }
  }
  return ret;
}

int ObAlterTableExecutor::check_alter_part_key(ObExecContext &ctx,
                                               obrpc::ObAlterTableArg &arg)
{
  int ret = OB_SUCCESS;
  if (arg.is_alter_columns_) {
    bool is_contain_part_key = false;
    ObSchemaGetterGuard schema_guard;
    common::ObIAllocator &allocator = arg.allocator_;
    AlterTableSchema &table_schema = const_cast<AlterTableSchema &>(arg.alter_table_schema_);
    share::schema::ObTableSchema::const_column_iterator it_begin = table_schema.column_begin();
    share::schema::ObTableSchema::const_column_iterator it_end = table_schema.column_end();
    const uint64_t tenant_id = table_schema.get_tenant_id();
    schema_guard.set_session_id(arg.session_id_);
    const ObString &origin_database_name = table_schema.get_origin_database_name();
    const ObString &origin_table_name = table_schema.get_origin_table_name();
    ObSQLSessionInfo *my_session = ctx.get_my_session();
    const share::schema::ObTableSchema *orig_table_schema = NULL;
    AlterColumnSchema *alter_column_schema = NULL;
    const ObColumnSchemaV2 *orig_column_schema = NULL;
    bool is_oracle_mode = false;
    CK (!origin_database_name.empty() && !origin_table_name.empty());
    CK (OB_NOT_NULL(my_session));
    OZ (ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(
    tenant_id, schema_guard));
    if (FAILEDx(schema_guard.get_table_schema(tenant_id,
                                              origin_database_name,
                                              origin_table_name,
                                              false/*is_index*/,
                                              orig_table_schema))) {
      LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(origin_database_name), K(origin_table_name));
    } else if (OB_ISNULL(orig_table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("table is not exist", KR(ret), K(tenant_id), K(origin_database_name), K(origin_table_name));
    } else if (OB_FAIL(orig_table_schema->check_if_oracle_compat_mode(is_oracle_mode))) {
      LOG_WARN("fail to check oracle mode", KR(ret), KPC(orig_table_schema));
    } else if (is_oracle_mode) {
      // skip
    } else {
      OZ (table_schema.assign_partition_schema(*orig_table_schema));
      for(;OB_SUCC(ret) && it_begin != it_end; it_begin++) {
        if (OB_ISNULL(alter_column_schema = static_cast<AlterColumnSchema *>(*it_begin))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("*it_begin is NULL", K(ret));
        } else if (OB_DDL_CHANGE_COLUMN == alter_column_schema->alter_type_
          || OB_DDL_MODIFY_COLUMN == alter_column_schema->alter_type_) {
          bool is_same = false;
          const ObString &orig_column_name = alter_column_schema->get_origin_column_name();
          orig_column_schema = orig_table_schema->get_column_schema(orig_column_name);
          if (OB_ISNULL(orig_column_schema)) {
            ret = OB_ERR_CANT_DROP_FIELD_OR_KEY;
            LOG_USER_ERROR(OB_ERR_CANT_DROP_FIELD_OR_KEY, orig_column_name.length(), orig_column_name.ptr());
          } else if (OB_FAIL(ObTableSchema::check_is_exactly_same_type(
            *orig_column_schema, *alter_column_schema, is_same))) {
            LOG_WARN("failed to check is exactly same type", K(ret));
          } else if (!is_same && orig_column_schema->is_tbl_part_key_column()) {
            const bool is_part = orig_column_schema->is_part_key_column();
            const bool is_subpart = orig_column_schema->is_subpart_key_column();
            if (is_contain_part_key) {
              ret = OB_NOT_SUPPORTED;
              LOG_USER_ERROR(OB_NOT_SUPPORTED, "There are several mutually exclusive "
              "DDL in single statement");
            }
            // The column may be part key column, and may be subpart key column too.
            if (OB_SUCC(ret) && is_part) {
              if (orig_table_schema->is_list_part()) {
                OZ (calc_list_values_exprs(*alter_column_schema, *orig_table_schema, table_schema,
                  schema_guard, *my_session, allocator, ctx, false));
              } else if (orig_table_schema->is_range_part()) {
                OZ (calc_range_values_exprs(*alter_column_schema, *orig_table_schema, table_schema,
                  schema_guard, *my_session, allocator, ctx, false));
              }
            }
            if (OB_SUCC(ret) && is_subpart) {
              if (orig_table_schema->is_list_subpart()) {
                OZ (calc_list_values_exprs(*alter_column_schema, *orig_table_schema, table_schema,
                  schema_guard, *my_session, allocator, ctx, true));
              } else if (orig_table_schema->is_range_subpart()) {
                OZ (calc_range_values_exprs(*alter_column_schema, *orig_table_schema, table_schema,
                  schema_guard, *my_session, allocator, ctx, true));
              }
            }
            is_contain_part_key = true;
          }
        }
      }
    }
  }
  return ret;
}

int ObAlterTableExecutor::check_alter_partition(ObExecContext &ctx,
                                                ObAlterTableStmt &stmt,
                                                const obrpc::ObAlterTableArg &arg)
{
  int ret = OB_SUCCESS;
  AlterTableSchema &table_schema = const_cast<AlterTableSchema &>(arg.alter_table_schema_);

  if (arg.is_alter_partitions_) {
    if (obrpc::ObAlterTableArg::PARTITIONED_TABLE == arg.alter_part_type_
        || obrpc::ObAlterTableArg::REORGANIZE_PARTITION == arg.alter_part_type_
        || obrpc::ObAlterTableArg::SPLIT_PARTITION == arg.alter_part_type_) {
      ObPartition **partition_array = table_schema.get_part_array();
      int64_t realy_part_num = OB_INVALID_PARTITION_ID;
      if (obrpc::ObAlterTableArg::SPLIT_PARTITION == arg.alter_part_type_) {
        realy_part_num = table_schema.get_part_option().get_part_num();
      } else {
        realy_part_num = table_schema.get_partition_num();
      }
      if (table_schema.is_range_part()) {
        if (OB_FAIL(ObPartitionExecutorUtils::set_range_part_high_bound(ctx,
                                                                        stmt::T_CREATE_TABLE,
                                                                        table_schema,
                                                                        stmt,
                                                                        false /*is_subpart*/))) {
          LOG_WARN("partition_array is NULL", K(ret));
        }
      } else if (table_schema.is_list_part()) {
        if (OB_ISNULL(partition_array)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL ptr", K(ret));
        } else if (OB_FAIL(ObPartitionExecutorUtils::cast_list_expr_to_obj(ctx,
                                                                           stmt::T_CREATE_TABLE,
                                                                           false, // is_subpart
                                                                           realy_part_num,
                                                                           partition_array,
                                                                           NULL,
                                                                           stmt.get_part_fun_exprs(),
                                                                           stmt.get_part_values_exprs()))) {
          LOG_WARN("partition_array is NULL", K(ret));
        }
      } else if (obrpc::ObAlterTableArg::PARTITIONED_TABLE != arg.alter_part_type_) {
        ret = OB_ERR_ONLY_ON_RANGE_LIST_PARTITION;
        LOG_WARN("only support range or list part", K(ret), K(arg.alter_part_type_),
                 "partition type", table_schema.get_part_option().get_part_func_type());
      }
      if (OB_FAIL(ret)) {
      } else if (obrpc::ObAlterTableArg::SPLIT_PARTITION == arg.alter_part_type_) {
        const_cast<AlterTableSchema&>(table_schema).get_part_option().set_part_num(table_schema.get_partition_num());
      }
    } else if (obrpc::ObAlterTableArg::REPARTITION_TABLE == arg.alter_part_type_) {
      if (table_schema.is_range_part() || table_schema.is_list_part()
         || table_schema.is_range_subpart() || table_schema.is_list_subpart()) {
        if (OB_FAIL(ObPartitionExecutorUtils::calc_values_exprs_for_alter_table(ctx,
                                                                                table_schema,
                                                                                stmt))) {
          LOG_WARN("failed to calc values exprs for alter partition by", K(ret));
        }
      }
    } else if (obrpc::ObAlterTableArg::ADD_PARTITION == arg.alter_part_type_) {
      if (table_schema.is_range_part() || table_schema.is_list_part()) {
        if (OB_FAIL(ObPartitionExecutorUtils::calc_values_exprs_for_alter_table(ctx,
                                                                                table_schema,
                                                                                stmt))) {
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
        if (OB_FAIL(ObPartitionExecutorUtils::set_individual_list_part_rows(
                    ctx, stmt, stmt::T_CREATE_TABLE, table_schema,
                    stmt.get_subpart_fun_exprs(),
                    stmt.get_individual_subpart_values_exprs()))) {
          LOG_WARN("failed to set individual list part rows", K(ret));
        }
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_WARN(OB_NOT_SUPPORTED, "add hash subpartition");
      }
    } else if (obrpc::ObAlterTableArg::DROP_PARTITION == arg.alter_part_type_
               || obrpc::ObAlterTableArg::DROP_SUB_PARTITION == arg.alter_part_type_
               || obrpc::ObAlterTableArg::RENAME_PARTITION == arg.alter_part_type_
               || obrpc::ObAlterTableArg::RENAME_SUB_PARTITION == arg.alter_part_type_
               || obrpc::ObAlterTableArg::TRUNCATE_PARTITION == arg.alter_part_type_
               || obrpc::ObAlterTableArg::TRUNCATE_SUB_PARTITION == arg.alter_part_type_) {
      // do-nothing
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("no operation", K(arg.alter_part_type_), K(ret));
    }
    LOG_DEBUG("dump table schema", K(table_schema));
  } else if (stmt.get_interval_expr() != NULL) {
    CK (NULL != stmt.get_transition_expr());
    OZ (ObPartitionExecutorUtils::check_transition_interval_valid(
                                        stmt::T_CREATE_TABLE,
                                        ctx,
                                        stmt.get_transition_expr(),
                                        stmt.get_interval_expr()));
    OZ (ObPartitionExecutorUtils::set_interval_value(ctx,
                                                     stmt::T_CREATE_TABLE,
                                                     table_schema,
                                                     stmt.get_interval_expr()));
  }

  return ret;
}

int ObAlterTableExecutor::set_index_arg_list(ObExecContext &ctx, ObAlterTableStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *my_session = ctx.get_my_session();
  obrpc::ObAlterTableArg &alter_table_arg = const_cast<obrpc::ObAlterTableArg &>(stmt.get_alter_table_arg());
  if (OB_ISNULL(my_session)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("session is null", K(ret));
  } else if (stmt.get_index_partition_resolve_results().count()
      != stmt.get_index_arg_list().count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid index resolve result", K(ret), K(stmt));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_index_arg_list().count(); i++) {
    HEAP_VAR(ObCreateIndexStmt, index_stmt) {
      obrpc::ObCreateIndexArg &create_index_arg = *(stmt.get_index_arg_list().at(i));
      ObPartitionResolveResult &resolve_result = stmt.get_index_partition_resolve_results().at(i);
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
  }
  return ret;
}

/**
 *
 */

ObDropTableExecutor::ObDropTableExecutor()
{
}

ObDropTableExecutor::~ObDropTableExecutor()
{
}

int ObDropTableExecutor::execute(ObExecContext &ctx, ObDropTableStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  obrpc::ObDDLRes res;
  const obrpc::ObDropTableArg &drop_table_arg = stmt.get_drop_table_arg();
  obrpc::ObDropTableArg &tmp_arg = const_cast<obrpc::ObDropTableArg&>(drop_table_arg);
  ObString first_stmt;
  ObSQLSessionInfo *my_session = NULL;
  int64_t foreign_key_checks = 0;
  if (OB_FAIL(stmt.get_first_stmt(first_stmt))) {
    LOG_WARN("get first statement failed", K(ret));
  } else {
    int64_t affected_rows = 0;
    tmp_arg.ddl_stmt_str_ = first_stmt;
    tmp_arg.consumer_group_id_ = THIS_WORKER.get_group_id();
    my_session = ctx.get_my_session();
    if (NULL == my_session) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get my session", K(ret), K(ctx));
    } else if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
      ret = OB_NOT_INIT;
      LOG_WARN("get task executor context failed");
    } else if (OB_FAIL(task_exec_ctx->get_common_rpc(common_rpc_proxy))) {
      LOG_WARN("get common rpc proxy failed", K(ret));
    } else if (OB_ISNULL(common_rpc_proxy)){
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("common rpc proxy should not be null", K(ret));
    } else if (OB_INVALID_ID == drop_table_arg.session_id_
               && FALSE_IT(tmp_arg.session_id_ = my_session->get_sessid_for_table())) {
      //impossible
    } else if (FALSE_IT(my_session->get_foreign_key_checks(foreign_key_checks))) {
    } else if (FALSE_IT(tmp_arg.foreign_key_checks_ = is_oracle_mode() || (is_mysql_mode() && foreign_key_checks))) {
    } else if (FALSE_IT(tmp_arg.compat_mode_ = ORACLE_MODE == my_session->get_compatibility_mode() ?
        lib::Worker::CompatMode::ORACLE : lib::Worker::CompatMode::MYSQL)) {
    } else if (OB_FAIL(common_rpc_proxy->drop_table(drop_table_arg, res))) {
      LOG_WARN("rpc proxy drop table failed", K(ret), "dst", common_rpc_proxy->get_server());
    } else if (res.is_valid() && OB_FAIL(ObDDLExecutorUtil::wait_ddl_retry_task_finish(res.tenant_id_, res.task_id_, *my_session, common_rpc_proxy, affected_rows))) {
      LOG_WARN("wait ddl finish failed", K(ret), K(res.tenant_id_), K(res.task_id_));
    } else {
      //do nothing
    }
  }
  return ret;
}

/**
 *
 */
ObRenameTableExecutor::ObRenameTableExecutor()
{
}

ObRenameTableExecutor::~ObRenameTableExecutor()
{
}

int ObRenameTableExecutor::execute(ObExecContext &ctx, ObRenameTableStmt &stmt)
{
  int ret = OB_SUCCESS;
  const obrpc::ObRenameTableArg &rename_table_arg = stmt.get_rename_table_arg();
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
  } else if (OB_FAIL(common_rpc_proxy->rename_table(rename_table_arg))) {
    LOG_WARN("rpc proxy rename table failed", K(ret));
  }
  return ret;
}

/**
 *
 */

ObTruncateTableExecutor::ObTruncateTableExecutor()
{
}

ObTruncateTableExecutor::~ObTruncateTableExecutor()
{
}

int ObTruncateTableExecutor::check_use_parallel_truncate(const obrpc::ObTruncateTableArg &arg, bool &use_parallel_truncate)
{
  int ret = OB_SUCCESS;
  uint64_t compat_version = 0;
  use_parallel_truncate = false;
  const ObTableSchema *table_schema = NULL;
  const uint64_t tenant_id = arg.tenant_id_;
  const ObString table_name = arg.table_name_;
  const ObString database_name = arg.database_name_;
  share::schema::ObSchemaGetterGuard schema_guard;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
    LOG_WARN("get min data_version failed", K(ret), K(tenant_id));
  } else if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("GCTX schema_service not init", K(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", K(ret), K(tenant_id));
  } else if (FALSE_IT(schema_guard.set_session_id(arg.session_id_))) {
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, database_name, table_name, false, table_schema))) {
    LOG_WARN("fail to get table schema", K(ret), K(database_name), K(table_name));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table is not exist", K(ret), K(database_name), K(table_name));
  } else {
    use_parallel_truncate = (table_schema->get_autoinc_column_id() == 0 && compat_version >= DATA_VERSION_4_1_0_0)
                            || compat_version >= DATA_VERSION_4_1_0_2;
  }
  return ret;
}

int ObTruncateTableExecutor::execute(ObExecContext &ctx, ObTruncateTableStmt &stmt)
{
  int ret = OB_SUCCESS;
  const obrpc::ObTruncateTableArg &truncate_table_arg = stmt.get_truncate_table_arg();
  obrpc::ObTruncateTableArg &tmp_arg = const_cast<obrpc::ObTruncateTableArg&>(truncate_table_arg);
  ObString first_stmt;
  obrpc::ObDDLRes res;
  ObSQLSessionInfo *my_session = ctx.get_my_session();
  if (OB_FAIL(stmt.get_first_stmt(first_stmt))) {
    LOG_WARN("get first statement failed", K(ret));
  } else {
    tmp_arg.ddl_stmt_str_ = first_stmt;
    tmp_arg.consumer_group_id_ = THIS_WORKER.get_group_id();

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
    } else if (OB_ISNULL(my_session)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get my session", K(ret), K(ctx));
    } else if (OB_INVALID_ID == truncate_table_arg.session_id_
               && FALSE_IT(tmp_arg.session_id_ = my_session->get_sessid_for_table())) {
      //impossible
    } else if (!stmt.is_truncate_oracle_temp_table()) {
      int64_t foreign_key_checks = 0;
      my_session->get_foreign_key_checks(foreign_key_checks);
      tmp_arg.foreign_key_checks_ = is_oracle_mode() || (is_mysql_mode() && foreign_key_checks);
      tmp_arg.compat_mode_ = ORACLE_MODE == my_session->get_compatibility_mode()
        ? lib::Worker::CompatMode::ORACLE : lib::Worker::CompatMode::MYSQL;
      int64_t affected_rows = 0;
      bool use_parallel_truncate = false;
      const uint64_t tenant_id = truncate_table_arg.tenant_id_;
      if (OB_FAIL(check_use_parallel_truncate(truncate_table_arg, use_parallel_truncate))) {
        LOG_WARN("fail to check use parallel truncate", KR(ret), K(truncate_table_arg));
      } else if (!use_parallel_truncate) {
        if (OB_FAIL(common_rpc_proxy->truncate_table(truncate_table_arg, res))) {
          LOG_WARN("rpc proxy alter table failed", K(ret));
        } else if (res.is_valid()
          && OB_FAIL(ObDDLExecutorUtil::wait_ddl_retry_task_finish(tenant_id, res.task_id_, *my_session, common_rpc_proxy, affected_rows))) {
          LOG_WARN("wait ddl finish failed", K(ret));
        }
      } else {
        // new parallel truncate
        ObTimeoutCtx ctx;
        if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, (static_cast<obrpc::ObRpcProxy*>(common_rpc_proxy))->timeout()))) {
          LOG_WARN("fail to set timeout ctx", K(ret));
        } else {
          int64_t start_time = ObTimeUtility::current_time();
          while (OB_SUCC(ret)) {
            DEBUG_SYNC(BEFORE_PARELLEL_TRUNCATE);
            if (OB_FAIL(common_rpc_proxy->timeout(ctx.get_timeout()).truncate_table_v2(truncate_table_arg, res))) {
              LOG_WARN("rpc proxy truncate table failed", K(ret));
              if ((OB_TRY_LOCK_ROW_CONFLICT == ret || OB_TIMEOUT == ret || OB_NOT_MASTER == ret
                    || OB_RS_NOT_MASTER == ret || OB_RS_SHUTDOWN == ret || OB_TENANT_NOT_IN_SERVER == ret) && ctx.get_timeout() > 0) {
                ob_usleep(1 * 1000 * 1000);
                // retry
                ret = OB_SUCCESS;
              }
            } else {
              // success
              break;
            }
          }
          int64_t step_time = ObTimeUtility::current_time();
          LOG_INFO("truncate_table_v2 finish trans", K(ret), "cost", step_time-start_time, "table_name", truncate_table_arg.table_name_, K(res));
          if (OB_FAIL(ret)) {
          } else if (!res.is_valid()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("truncate invalid ddl_res", KR(ret), K(res));
          } else if (OB_FAIL(ObSchemaUtils::try_check_parallel_ddl_schema_in_sync(
                     ctx, tenant_id, res.task_id_))) {
            LOG_WARN("fail to check parallel ddl schema in sync", KR(ret), K(res));
          }
          int64_t end_time = ObTimeUtility::current_time();
          LOG_INFO("truncate_table_v2", KR(ret), "cost", end_time-start_time,
                                                "trans_cost", step_time - start_time,
                                                "wait_refresh", end_time - step_time,
                                                "table_name", truncate_table_arg.table_name_,
                                                K(res));
        }
      }
    } else if (stmt.get_oracle_temp_table_type() == share::schema::TMP_TABLE_ORA_TRX) {
      //do nothing
    } else {
      ObSqlString sql;
      int64_t affect_rows = 0;
      common::ObOracleSqlProxy oracle_sql_proxy;
      uint64_t tenant_id = stmt.get_tenant_id();
      ObString db_name = stmt.get_database_name();
      ObString tab_name = stmt.get_table_name();
      uint64_t unique_id = my_session->get_gtt_session_scope_unique_id();

      if (OB_FAIL(oracle_sql_proxy.init(GCTX.sql_proxy_->get_pool()))) {
        LOG_WARN("init oracle sql proxy failed", K(ret));
      } else if (OB_FAIL(sql.assign_fmt("DELETE FROM \"%.*s\".\"%.*s\" WHERE "
                                        "%s = %ld",
                                        db_name.length(), db_name.ptr(),
                                        tab_name.length(), tab_name.ptr(),
                                        OB_HIDDEN_SESSION_ID_COLUMN_NAME, unique_id))) {
        LOG_WARN("fail to assign sql", K(ret));
      } else if (OB_FAIL(oracle_sql_proxy.write(tenant_id, sql.ptr(), affect_rows))) {
        LOG_WARN("execute sql failed", K(ret), K(sql), K(affect_rows));
      } else {
        int64_t query_timeout = 0;
        my_session->get_query_timeout(query_timeout);
        LOG_INFO("succeed to truncate table using delete", K(sql), K(affect_rows),
                 K(query_timeout), K(THIS_WORKER.get_timeout_remain()));
      }
    }
  }
  return ret;
}

ObCreateTableLikeExecutor::ObCreateTableLikeExecutor()
{
}

ObCreateTableLikeExecutor::~ObCreateTableLikeExecutor()
{
}

int ObCreateTableLikeExecutor::execute(ObExecContext &ctx, ObCreateTableLikeStmt &stmt)
{
  int ret = OB_SUCCESS;
  const obrpc::ObCreateTableLikeArg &create_table_like_arg = stmt.get_create_table_like_arg();
  obrpc::ObCreateTableLikeArg &tmp_arg = const_cast<obrpc::ObCreateTableLikeArg&>(create_table_like_arg);
  ObString first_stmt;
  if (OB_FAIL(stmt.get_first_stmt(first_stmt))) {
    LOG_WARN("get first statement failed", K(ret));
  } else {
    tmp_arg.ddl_stmt_str_ = first_stmt;
    tmp_arg.consumer_group_id_ = THIS_WORKER.get_group_id();
    tmp_arg.session_id_ = ctx.get_my_session()->get_sessid_for_table();
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
  obrpc::ObFlashBackTableFromRecyclebinArg &tmp_arg = const_cast<obrpc::ObFlashBackTableFromRecyclebinArg&>(flashback_table_arg);
  ObString first_stmt;
  if (OB_FAIL(stmt.get_first_stmt(first_stmt))) {
    LOG_WARN("get first statement failed", K(ret));
  } else {
    tmp_arg.ddl_stmt_str_ = first_stmt;
    tmp_arg.consumer_group_id_ = THIS_WORKER.get_group_id();
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

int ObFlashBackTableToScnExecutor::execute(ObExecContext &ctx, ObFlashBackTableToScnStmt &stmt) {
  int ret = OB_SUCCESS;
  obrpc::ObFlashBackTableToScnArg &arg = stmt.flashback_table_to_scn_arg_;
  arg.consumer_group_id_ = THIS_WORKER.get_group_id();
  RowDesc row_desc;
  ObTempExpr *temp_expr = NULL;
  CK(OB_NOT_NULL(ctx.get_sql_ctx()));
  OZ(ObStaticEngineExprCG::gen_expr_with_row_desc(stmt.get_time_expr(),
     row_desc, ctx.get_allocator(), ctx.get_my_session(),
     ctx.get_sql_ctx()->schema_guard_, temp_expr));
  CK(OB_NOT_NULL(temp_expr));
  if (OB_SUCC(ret)) {
    ObNewRow empty_row;
    ObObj tmp_obj;
    ObExprCtx expr_ctx;
    expr_ctx.calc_buf_ = &ctx.get_allocator();
    expr_ctx.phy_plan_ctx_ = ctx.get_physical_plan_ctx();
    expr_ctx.my_session_ = ctx.get_my_session();
    expr_ctx.exec_ctx_ = &ctx;
    const int64_t cur_time = expr_ctx.phy_plan_ctx_->has_cur_time() ?
        expr_ctx.phy_plan_ctx_->get_cur_time().get_timestamp() : ObTimeUtility::current_time();
    expr_ctx.phy_plan_ctx_->set_cur_time(cur_time, *expr_ctx.my_session_);
    ObObj result_obj;
    if (OB_FAIL(temp_expr->eval(ctx, empty_row, result_obj))) {
      LOG_WARN("failed to calculate", K(ret));
    } else if (ObFlashBackTableToScnStmt::TIME_TIMESTAMP == stmt.get_time_type()) {
      EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
      if (OB_FAIL(ObObjCaster::to_type(ObTimestampTZType, cast_ctx, tmp_obj, result_obj))) {
        LOG_WARN("failed to cast object", K(ret), K(tmp_obj));
      } else {
        arg.time_point_ = result_obj.v_.datetime_;
        LOG_DEBUG("timestamp_val result", K(tmp_obj), K(result_obj), K(arg.time_point_));
      }
    } else if (ObFlashBackTableToScnStmt::TIME_SCN == stmt.get_time_type()) {
      EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
      if (OB_FAIL(ObObjCaster::to_type(ObUInt64Type, cast_ctx, tmp_obj, result_obj))) {
        LOG_WARN("failed to cast object", K(ret), K(tmp_obj));
      } else {
        arg.time_point_ = result_obj.v_.uint64_;
        LOG_DEBUG("timestamp_val result", K(tmp_obj), K(result_obj), K(arg.time_point_));
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObTaskExecutorCtx *task_exec_ctx = nullptr;
    obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
    if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get task executor context failed", K(ret));
    } else if (OB_FAIL(task_exec_ctx->get_common_rpc(common_rpc_proxy))) {
      LOG_WARN("get common rpc proxy failed", K(ret));
    } else if (OB_ISNULL(common_rpc_proxy)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("common rpc proxy should not be null", K(ret));
    } else if (OB_FAIL(common_rpc_proxy->flashback_table_to_time_point(arg))) {
      LOG_WARN("rpc proxy flashback table failed", K(ret));
    }
  }

  return ret;
}

int ObPurgeTableExecutor::execute(ObExecContext &ctx, ObPurgeTableStmt &stmt)
{
  int ret = OB_SUCCESS;
  const obrpc::ObPurgeTableArg &purge_table_arg = stmt.get_purge_table_arg();
  obrpc::ObPurgeTableArg &tmp_arg = const_cast<obrpc::ObPurgeTableArg&>(purge_table_arg);
  ObString first_stmt;
  if (OB_FAIL(stmt.get_first_stmt(first_stmt))) {
    LOG_WARN("get first statement failed", K(ret));
  } else {
    tmp_arg.ddl_stmt_str_ = first_stmt;
    tmp_arg.consumer_group_id_ = THIS_WORKER.get_group_id();

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
    } else if (OB_FAIL(common_rpc_proxy->purge_table(purge_table_arg))) {
      LOG_WARN("rpc proxy purge table failed", K(ret));
    }
  }
  return ret;
}

int ObOptimizeTableExecutor::execute(ObExecContext &ctx, ObOptimizeTableStmt &stmt)
{
  int ret = OB_SUCCESS;
  obrpc::ObOptimizeTableArg &arg = stmt.get_optimize_table_arg();
  ObString first_stmt;
  if (OB_FAIL(stmt.get_first_stmt(first_stmt))) {
    LOG_WARN("fail to get first stmt", K(ret));
  } else {
    arg.ddl_stmt_str_ = first_stmt;
    arg.consumer_group_id_ = THIS_WORKER.get_group_id();
    ObTaskExecutorCtx *task_exec_ctx = nullptr;
    obrpc::ObCommonRpcProxy *common_rpc_proxy = nullptr;
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

int ObOptimizeTenantExecutor::execute(ObExecContext &ctx, ObOptimizeTenantStmt &stmt)
{
  int ret = OB_SUCCESS;
  obrpc::ObOptimizeTenantArg &arg = stmt.get_optimize_tenant_arg();
  ObString first_stmt;
  ObSQLSessionInfo *my_session = ctx.get_my_session();
  if (OB_FAIL(stmt.get_first_stmt(first_stmt))) {
    LOG_WARN("fail to get first stmt", K(ret));
  } else if (OB_ISNULL(my_session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, my session must not be NULL", K(ret));
  } else {
    arg.ddl_stmt_str_ = first_stmt;
    arg.consumer_group_id_ = THIS_WORKER.get_group_id();
    ObTaskExecutorCtx *task_exec_ctx = nullptr;
    obrpc::ObCommonRpcProxy *common_rpc_proxy = nullptr;
    const observer::ObGlobalContext &gctx = observer::ObServer::get_instance().get_gctx();
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

int ObOptimizeTenantExecutor::optimize_tenant(const obrpc::ObOptimizeTenantArg &arg,
    const uint64_t effective_tenant_id, ObMultiVersionSchemaService &schema_service, obrpc::ObCommonRpcProxy *common_rpc_proxy)
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
    ObSEArray<const ObSimpleTableSchemaV2 *, 512> table_schemas;
    if (OB_FAIL(schema_service.get_tenant_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("fail to get tenant schema guard", K(ret));
    } else if (OB_FAIL(schema_guard.get_table_schemas_in_tenant(tenant_id, table_schemas))) {
      LOG_WARN("fail to get table schemas in tenant", K(ret));
    } else {
      LOG_INFO("optimize tenant, table schema count", K(table_schemas.count()));
      for (int64_t i = 0; OB_SUCC(ret) && i < table_schemas.count(); ++i) {
        const ObSimpleTableSchemaV2 *table_schema = table_schemas.at(i);
        const ObDatabaseSchema *database_schema = nullptr;
        obrpc::ObOptimizeTableArg optimize_table_arg;
        if (OB_ISNULL(table_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("error unexpected, table schema must not be NULL", K(ret));
        } else if (table_schema->is_index_table() || table_schema->is_vir_table() || table_schema->is_view_table()) {
          // do nothing
        } else if (OB_FAIL(schema_guard.get_database_schema(tenant_id, table_schema->get_database_id(), database_schema))) {
          LOG_WARN("fail to get database schema", K(ret), K(tenant_id));
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

int ObOptimizeAllExecutor::execute(ObExecContext &ctx, ObOptimizeAllStmt &stmt)
{
  int ret = OB_SUCCESS;
  obrpc::ObOptimizeAllArg &arg = stmt.get_optimize_all_arg();
  ObString first_stmt;
  if (OB_FAIL(stmt.get_first_stmt(first_stmt))) {
    LOG_WARN("fail to get first stmt", K(ret));
  } else {
    arg.ddl_stmt_str_ = first_stmt;
    arg.consumer_group_id_ = THIS_WORKER.get_group_id();
    ObTaskExecutorCtx *task_exec_ctx = nullptr;
    obrpc::ObCommonRpcProxy *common_rpc_proxy = nullptr;
    ObSchemaGetterGuard schema_guard;
    ObArray<uint64_t> tenant_ids;
    const observer::ObGlobalContext &gctx = observer::ObServer::get_instance().get_gctx();
    ObSQLSessionInfo *my_session = ctx.get_my_session();
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
        const ObTenantSchema *tenant_schema = nullptr;
        if (OB_FAIL(schema_guard.get_tenant_info(tenant_ids.at(i), tenant_schema))) {
          LOG_WARN("fail to get tenant name", K(ret));
        } else {
          tenant_arg.tenant_name_ = tenant_schema->get_tenant_name();
          if (OB_FAIL(ObOptimizeTenantExecutor::optimize_tenant(tenant_arg, my_session->get_effective_tenant_id(), *gctx.schema_service_, common_rpc_proxy))) {
            LOG_WARN("fail to optimize tenant", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

} //end namespace sql
} //end namespace oceanbase
