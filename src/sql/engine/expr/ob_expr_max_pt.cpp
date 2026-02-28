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

#define USING_LOG_PREFIX  SQL_ENG

#include "sql/engine/expr/ob_expr_max_pt.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "observer/ob_inner_sql_connection.h"
#include "observer/ob_sql_client_decorator.h"
#include "lib/mysqlclient/ob_isql_client.h"
#include "sql/ob_sql_utils.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/ob_sql_context.h"
#include "sql/engine/expr/ob_expr.h"
#include "sql/ob_sql.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/session/ob_sql_session_info.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "sql/privilege_check/ob_privilege_check.h"
#include "sql/privilege_check/ob_ora_priv_check.h"
namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprMaxPt::ObExprMaxPt(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_MAX_PT, N_MAX_PT, 1, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION,
                         false, false)
{
}

ObExprMaxPt::~ObExprMaxPt()
{
}

void ObExprMaxPt::upper_db_table_name(
  const ObNameCaseMode case_mode, const bool is_oracle_mode, ObString &name) {
  if (is_oracle_mode) {
    // do nothing
    // in oracle mode, table name is case-sensitive
    // if "create table "t1" XXX;" table name is "t1" in schema
    // if "create table t1 XXX;" table name is "T1" in schema
    // so user need input suitable table_name in max_pt function
  } else {
    if (OB_LOWERCASE_AND_INSENSITIVE == case_mode) {
      str_tolower(name.ptr(), name.length());
    }
  }
}

int ObExprMaxPt::check_max_pt_privilege(
  ObEvalCtx &ctx,
  const ObString &db_name,
  const ObString &table_name,
  const ObTableSchema *table_schema)
{
  int ret = OB_SUCCESS;
  const ObSQLSessionInfo *session = nullptr;
  ObSchemaGetterGuard *schema_guard = nullptr;
  if (OB_ISNULL(session = ctx.exec_ctx_.get_my_session())
      || OB_ISNULL(session->get_cur_exec_ctx())
      || OB_ISNULL(session->get_cur_exec_ctx()->get_sql_ctx())
      || OB_ISNULL(schema_guard = session->get_cur_exec_ctx()->get_sql_ctx()->schema_guard_)
      || OB_ISNULL(table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unexpected null", K(ret), KP(session), KP(schema_guard), KP(table_schema));
  } else {
    ObSessionPrivInfo session_priv;
    if (OB_FAIL(session->get_session_priv_info(session_priv))) {
      LOG_WARN("fail to get session priv info", K(ret));
    } else if (OB_UNLIKELY(!session_priv.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("session priv is invalid", "tenant_id", session_priv.tenant_id_,
               "user_id", session_priv.user_id_, K(ret));
    } else {
      const common::ObIArray<uint64_t> &role_array = session->get_enable_role_array();
      const bool is_oracle_mode = lib::is_oracle_mode();

      if (is_oracle_mode) {
        // Oracle mode: check object privilege
        ObOraNeedPriv ora_need_priv;
        ObPackedObjPriv packed_privs = 0;
        const uint64_t tenant_id = session_priv.tenant_id_;

        if (OB_FAIL(ObPrivPacker::pack_raw_obj_priv(NO_OPTION, OBJ_PRIV_ID_SELECT, packed_privs))) {
          LOG_WARN("fail to pack raw obj priv", K(ret));
        } else {
          ora_need_priv.db_name_ = db_name;
          ora_need_priv.grantee_id_ = session_priv.user_id_;
          ora_need_priv.obj_id_ = table_schema->get_table_id();
          ora_need_priv.obj_level_ = OBJ_LEVEL_FOR_TAB_PRIV;
          ora_need_priv.obj_type_ = static_cast<uint64_t>(share::schema::ObObjectType::TABLE);
          ora_need_priv.obj_privs_ = packed_privs;
          ora_need_priv.check_flag_ = CHECK_FLAG_NORMAL;
          ora_need_priv.owner_id_ = OB_ORA_SYS_USER_ID;
          ObStmtOraNeedPrivs need_privs;
          need_privs.need_privs_.set_allocator(&ctx.get_expr_res_alloc());
          need_privs.need_privs_.set_capacity(10);
          need_privs.need_privs_.push_back(ora_need_priv);

          if (OB_FAIL(schema_guard->check_ora_priv(
                  session_priv.tenant_id_, session_priv.user_id_, need_privs, role_array))) {
            LOG_WARN("check oracle mode table privilege failed", K(ret), K(need_privs));
          }
        }
      } else {
        // MySQL mode: check table privilege
        const ObNeedPriv need_priv(db_name, table_name, OB_PRIV_TABLE_LEVEL, OB_PRIV_SELECT, false);
        if (OB_FAIL(schema_guard->check_single_table_priv(session_priv, role_array, need_priv))) {
          LOG_WARN("check mysql mode table privilege failed", K(ret), K(need_priv));
        }
      }
    }
  }
  return ret;
}

int ObExprMaxPt::resolve_table_name_and_schema(
  const ObCollationType cs_type, const ObSQLSessionInfo *session,
  const ObString &name, ObString &database_name, ObString &table_name,
  const ObTableSchema *&table_schema) {
  int ret = OB_SUCCESS;
  static const char split_character = '.';
  bool is_oracle_mode = lib::is_oracle_mode();
  database_name.reset();
  table_name.reset();
  if (OB_UNLIKELY(name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(name));
  } else {
    ObString name_str = name;
    const char *p = name_str.find(split_character);
    if (p == nullptr) {
      table_name = name_str;
    } else {
      database_name = name_str.split_on(p);
      table_name = name_str;
      if (OB_UNLIKELY(database_name.empty() || table_name.empty() ||
                      nullptr != table_name.find(split_character))) {
        ret = OB_WRONG_TABLE_NAME;
        LOG_WARN("wrong table name", KR(ret), K(name));
      }
    }
    ObNameCaseMode case_mode = OB_NAME_CASE_INVALID;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(session->get_name_case_mode(case_mode))) {
      LOG_WARN("fail to get name case mode", K(ret));
    } else {
      const bool preserve_lettercase =
          is_oracle_mode ? true : (case_mode != OB_LOWERCASE_AND_INSENSITIVE);
      upper_db_table_name(case_mode, is_oracle_mode, database_name);
      upper_db_table_name(case_mode, is_oracle_mode, table_name);
      if (!database_name.empty() &&
          OB_FAIL(ObSQLUtils::check_and_convert_db_name(
              cs_type, preserve_lettercase, database_name))) {
        LOG_WARN("fail to check and convert database name", KR(ret),
                K(database_name));
      } else if (OB_FAIL(ObSQLUtils::check_and_convert_table_name(
                    cs_type, preserve_lettercase, table_name,
                    is_oracle_mode))) {
        LOG_WARN("fail to check and convert table name", KR(ret), K(cs_type),
                K(preserve_lettercase), K(table_name));
      }
    }

    const uint64_t tenant_id = session->get_effective_tenant_id();
    const uint64_t database_id = session->get_database_id();
    ObSchemaGetterGuard *schema_guard = session->get_cur_exec_ctx()->get_sql_ctx()->schema_guard_;
    if (OB_FAIL(ret)) {
    } else if (database_name.empty()
      && OB_FAIL(schema_guard->get_table_schema(tenant_id, database_id, table_name, false, table_schema))) {
      LOG_WARN("fail to get table schema", K(ret));
    } else if (!database_name.empty()
      && OB_FAIL(schema_guard->get_table_schema(tenant_id, database_name, table_name, false, table_schema))) {
      LOG_WARN("fail to get table schema", K(ret));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("table schema is null", K(ret), K(tenant_id), K(database_id), K(table_name));
    }
  }
  return ret;
}

int ObExprMaxPt::calc_result_type1(ObExprResType &type,
                                   ObExprResType &type1,
                                   ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  ObString partition_key_column_name;
  const ObRawExpr *raw_expr = type_ctx.get_raw_expr();
  if (OB_ISNULL(raw_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(raw_expr));
  } else {
    const ObRawExpr *name_expr = raw_expr->get_param_expr(0);
    if (OB_ISNULL(name_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null name expr", K(ret));
    } else if (!name_expr->is_const_expr()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table name must be constant", K(ret));
    } else {
      ObObj name_obj = static_cast<const ObConstRawExpr *>(name_expr)->get_value();
      ObString name_str;
      ObString db_name;
      ObString table_name;
      const ObTableSchema *table_schema = NULL;
      int64_t part_func_expr_num = 0;
      if (OB_FAIL(name_obj.get_string(name_str))) {
        LOG_WARN("fail to get string", K(ret));
      } else if (OB_FAIL(resolve_table_name_and_schema(name_expr->get_collation_type(), type_ctx.get_session(),
                                                       name_str, db_name, table_name, table_schema))) {
        LOG_WARN("fail to resolve table name", K(ret));
      } else if (!table_schema->is_partitioned_table()
                 || table_schema->is_external_table()
                 || table_schema->is_link_table()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "table name, MAX_PT only supports partitioned inner_table");
        LOG_WARN("this table is not partitioned inner table", K(ret), K(table_schema));
      } else if (!table_schema->get_part_option().is_list_part()
                 && !table_schema->get_part_option().is_range_part()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "table name, MAX_PT only supports range or list partition");
        LOG_WARN("MAX_PT only supports range or list partition", K(ret), K(table_schema));
      } else if (OB_FAIL(table_schema->calc_part_func_expr_num(part_func_expr_num))) {
        LOG_WARN("fail to calc part func expr num", K(ret));
      } else if (part_func_expr_num != 1) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "table name, MAX_PT only supports single-column partition key");
        LOG_WARN("MAX_PT only supports single column partition", K(ret), K(part_func_expr_num));
      } else {
        // 检查是否为表达式分区（如 c1 + 1）
        ObString part_func_expr = table_schema->get_part_option().get_part_func_expr_str();
        if (!part_func_expr.empty()) {
          ObArray<uint64_t> partition_key_ids;
          if (OB_FAIL(table_schema->get_partition_keys_by_part_func_expr(part_func_expr, partition_key_ids))) {
            ret = OB_INVALID_ARGUMENT;
            LOG_USER_ERROR(OB_INVALID_ARGUMENT, "table name, MAX_PT does not support expression partition key");
            LOG_WARN("MAX_PT does not support expression partition", K(ret),
                     K(part_func_expr));
          }
        }
        if (OB_SUCC(ret)) {
          const ObPartitionKeyInfo &partition_key_info = table_schema->get_partition_key_info();
          uint64_t partition_key_column_id = OB_INVALID_ID;
          if (OB_FAIL(partition_key_info.get_column_id(0, partition_key_column_id))) {
            LOG_WARN("fail to get partition key column id", K(ret));
          } else {
            const ObColumnSchemaV2 *partition_key_column = table_schema->get_column_schema(partition_key_column_id);
            if (OB_ISNULL(partition_key_column)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("column schema is null", K(ret), K(partition_key_column_id));
            } else {
              type.set_type(partition_key_column->get_data_type());
              type.set_collation_type(partition_key_column->get_collation_type());
              type.set_collation_level(
                ObRawExprUtils::get_column_collation_level(partition_key_column->get_data_type()));
              type.set_scale(partition_key_column->get_data_scale());
              type.set_precision(partition_key_column->get_data_precision());
              type.set_length(partition_key_column->get_data_length());
            }
          }
        }
      }
    }
  }

  return ret;
}

int ObExprMaxPt::eval_max_pt(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *table_name_param = NULL;
  ObExpr *table_name_expr = expr.args_[0];
  if (OB_FAIL(expr.eval_param_value(ctx, table_name_param))) {
    LOG_WARN("eval arg failed", K(ret));
  } else if (OB_ISNULL(table_name_param) || OB_ISNULL(table_name_expr) || table_name_param->is_null()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("name or value is NULL", K(ret), K(table_name_param), K(table_name_expr));
  } else {
    const ObTableSchema *table_schema = NULL;
    ObString name_str = table_name_param->get_string();
    ObString db_name;
    ObString table_name;
    ObString cur_db_name = ctx.exec_ctx_.get_my_session()->get_database_name();
    const uint64_t tenant_id = ctx.exec_ctx_.get_my_session()->get_effective_tenant_id();
    if (OB_FAIL(resolve_table_name_and_schema(table_name_expr->obj_meta_.get_collation_type(),
                                              ctx.exec_ctx_.get_my_session(),
                                              name_str, db_name, table_name, table_schema))) {
      LOG_WARN("fail to get table schema", K(ret));
    } else if (OB_FAIL(check_max_pt_privilege(ctx, db_name.empty() ? cur_db_name : db_name, table_name, table_schema))) {
      LOG_WARN("max_pt privilege check failed", K(ret), K(db_name), K(table_name), KP(table_schema));
    } else {
      const ObPartitionKeyInfo &partition_key_info = table_schema->get_partition_key_info();
      uint64_t partition_key_column_id = OB_INVALID_ID;
      if (OB_FAIL(partition_key_info.get_column_id(0, partition_key_column_id))) {
        LOG_WARN("fail to get partition key column id", K(ret));
      } else {
        const ObColumnSchemaV2 *partition_key_column = table_schema->get_column_schema(partition_key_column_id);
        if (OB_ISNULL(partition_key_column)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column schema is null", K(ret), K(partition_key_column_id));
        } else {
          ObString partition_key_name = partition_key_column->get_column_name_str();
          ObSqlString hint_str;
          ObSqlString inner_sql;
          bool is_oracle_mode = lib::is_oracle_mode();

          int64_t remaining_timeout_us = THIS_WORKER.get_timeout_remain();
          if (remaining_timeout_us <= 0) {
            ret = OB_TIMEOUT;
            LOG_WARN("remaining timeout is less than 0", K(ret), K(remaining_timeout_us));
          } else if (OB_FAIL(hint_str.assign_fmt("/*+ opt_param('partition_ordered', 'desc') parallel(1) \
                                          full(%.*s) no_use_das(%.*s) query_timeout(%ld) */",
                                          table_name.length(), table_name.ptr(),
                                          table_name.length(), table_name.ptr(),
                                          remaining_timeout_us))) {
            LOG_WARN("fail to assign hint str", K(ret), K(table_name), K(remaining_timeout_us));
          } else if (is_oracle_mode && OB_FAIL(inner_sql.append_fmt("SELECT %.*s %.*s FROM \"%.*s\".\"%.*s\" WHERE ROWNUM = 1",
                                          hint_str.length(), hint_str.ptr(),
                                          partition_key_name.length(), partition_key_name.ptr(),
                                          (db_name.empty() ? cur_db_name.length() : db_name.length()),
                                          (db_name.empty() ? cur_db_name.ptr() : db_name.ptr()),
                                          table_name.length(), table_name.ptr()))) {
            LOG_WARN("fail to append inner sql", K(ret), K(partition_key_name), K(db_name), K(table_name), K(name_str));
          } else if (!is_oracle_mode && OB_FAIL(inner_sql.append_fmt("SELECT %.*s %.*s FROM `%.*s`.`%.*s` LIMIT 1",
                                          hint_str.length(), hint_str.ptr(),
                                          partition_key_name.length(), partition_key_name.ptr(),
                                          (db_name.empty() ? cur_db_name.length() : db_name.length()),
                                          (db_name.empty() ? cur_db_name.ptr() : db_name.ptr()),
                                          table_name.length(), table_name.ptr()))) {
            LOG_WARN("fail to append inner sql", K(ret), K(partition_key_name), K(db_name), K(table_name), K(name_str));
          } else {
            ObMySQLProxy *sql_proxy = ctx.exec_ctx_.get_sql_proxy();
            ObOracleSqlProxy oracle_sql_proxy(*sql_proxy);
            ObMySQLTransaction trans;
            if (OB_ISNULL(sql_proxy)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("sql proxy is null", K(ret));
            } else if (is_oracle_mode && OB_FAIL(trans.start(&oracle_sql_proxy, tenant_id))) {
              LOG_WARN("fail to start transaction", K(ret));
            } else if (!is_oracle_mode && OB_FAIL(trans.start(sql_proxy, tenant_id))) {
              LOG_WARN("fail to start transaction", K(ret));
            } else {
              ObSQLClientRetryWeak sql_client_retry_weak(&trans, tenant_id, OB_ALL_SYS_STAT_TID);
              SMART_VAR(ObISQLClient::ReadResult, read_result) {
                common::sqlclient::ObMySQLResult *result = NULL;
                if (OB_FAIL(sql_client_retry_weak.read(read_result, tenant_id, inner_sql.ptr()))) {
                  LOG_WARN("failed to execute inner sql", K(ret), K(inner_sql));
                } else if (OB_ISNULL(result = read_result.get_result())) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("result is null", K(ret), K(inner_sql));
                } else if (OB_FAIL(result->next())) {
                  if (OB_ITER_END == ret) {
                    ret = OB_SUCCESS;
                    res.set_null();
                  } else {
                    LOG_WARN("failed to get next row", K(ret));
                  }
                } else {
                  ObObj partition_value;
                  ObDatum partition_datum = res;
                  if (OB_FAIL(result->get_obj(0, partition_value, NULL))) {
                    LOG_WARN("failed to get obj", K(ret));
                  } else if (partition_value.is_null()) {
                    res.set_null();
                  } else if (OB_FAIL(partition_datum.from_obj(partition_value))) {
                      LOG_WARN("failed to convert obj to datum", K(ret), K(partition_value));
                  } else if (OB_FAIL(res.deep_copy(partition_datum, ctx.get_expr_res_alloc()))){
                    LOG_WARN("failed to deep copy datum", K(ret));
                  }
                }
              }
            }
            if (trans.is_started()) {
              int tmp_ret = OB_SUCCESS;
              if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
                LOG_WARN("failed to commit trans", KR(ret), KR(tmp_ret));
                ret = OB_SUCC(ret) ? tmp_ret : ret;
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObExprMaxPt::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_5_1_0) {
    if (OB_UNLIKELY((1 != rt_expr.arg_cnt_) || OB_ISNULL(rt_expr.args_))) {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "args_ is NULL or arg_cnt_ is invalid", K(ret), K(rt_expr));
    } else {
      rt_expr.eval_func_ = eval_max_pt;
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("MAX_PT not supported", K(GET_MIN_CLUSTER_VERSION()),
             K(CLUSTER_VERSION_4_5_1_0), K(ret));
  }
  return ret;
}

} //namespace sql
} //namespace oceanbase
