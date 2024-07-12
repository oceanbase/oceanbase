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

#include "share/rc/ob_tenant_base.h"
#define USING_LOG_PREFIX  SQL_ENG

//#define TEST_MODE


#include "sql/engine/cmd/ob_load_data_impl.h"

#include <math.h>
#include "observer/omt/ob_multi_tenant.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_sql_string.h"
#include "storage/access/ob_dml_param.h"
#include "sql/parser/ob_parser.h"
#include "sql/resolver/ob_resolver.h"
#include "sql/resolver/dml/ob_insert_stmt.h"
#include "sql/plan_cache/ob_sql_parameterization.h"
#include "sql/code_generator/ob_expr_generator_impl.h"
#include "sql/code_generator/ob_code_generator.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/cmd/ob_load_data_utils.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/das/ob_das_location_router.h"
#include "share/ob_tenant_mgr.h"
#include "share/ob_tenant_memstore_info_operator.h"
#include "sql/resolver/ob_schema_checker.h"
#include "observer/ob_inner_sql_connection_pool.h"
#include "observer/ob_inner_sql_result.h"
#include "share/ob_device_manager.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "storage/tx_storage/ob_tenant_freezer.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "observer/omt/ob_tenant_timezone_mgr.h"
#include "share/config/ob_config_helper.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::storage;
using namespace oceanbase::observer;

namespace oceanbase
{
namespace sql
{

#ifdef TEST_MODE
static const int64_t INSERT_TASK_DROP_RATE = 1;
static void delay_process_by_probability(int64_t percentage) {
  if (OB_UNLIKELY(ObRandom::rand(1, 100) <= percentage)) {
    ob_usleep(RPC_BATCH_INSERT_TIMEOUT_US);
  }
}
#endif

#define OW(statement) \
  do {\
    int inner_ret = statement;\
    if (OB_UNLIKELY(OB_SUCCESS != inner_ret)) {\
      LOG_WARN("fail to exec"#statement, K(inner_ret));\
      if (OB_SUCC(ret)) { ret = inner_ret; }\
    }\
  } while (0)

const char *ObLoadDataBase::SERVER_TENANT_MEMORY_EXAMINE_SQL =
    "SELECT case when memstore_used < freeze_trigger * 1.02 then false else true end"
    " as need_wait_freeze"
    " FROM oceanbase.__all_virtual_tenant_memstore_info WHERE tenant_id = %ld"
    " and svr_ip = '%s' and svr_port = %d";

const char *log_file_column_names = "\nBatchId\tLineNum\tType\tErrCode\tErrMsg\t\n";
const char *log_file_row_fmt = "%ld\t%ld\t%s\t%d\t%.*s\t\n";
static const int64_t WAIT_INTERVAL_US = 1 * 1000 * 1000;  //1s

int ObLoadDataBase::generate_fake_field_strs(ObIArray<ObString> &file_col_values,
                                             ObIAllocator &allocator,
                                             const char id_char)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;

  /* to generate string like "F1F2F3...F99....Fn" into buf
   * maxn = 512
   */

  int64_t buf_len = 6 * file_col_values.count();
  int64_t pos = 0;
  if (OB_ISNULL(buf = static_cast<char*>(allocator.alloc(buf_len * sizeof(char))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc memory failed", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < file_col_values.count(); ++i) {
      int64_t pos_bak = pos;
      if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%c%ld", id_char, i))) { //F1 F2 ..
        LOG_WARN("generate str failed", K(ret), K(pos), K(buf_len));
      } else {
        file_col_values.at(i).assign_ptr(buf + pos_bak, pos - pos_bak);
      }
    }
    LOG_DEBUG("generate fake field result", K(file_col_values));
  }
  return ret;
}

int ObLoadDataBase::construct_insert_sql(ObSqlString &insert_sql,
                                         const ObString &q_name,
                                         ObIArray<ObLoadTableColumnDesc> &desc,
                                         ObIArray<ObString> &insert_values,
                                         int64_t num_rows)
{
  int ret = OB_SUCCESS;
  insert_sql.reuse();
  char q = lib::is_oracle_mode() ? '"' : '`';

  if (OB_UNLIKELY(q_name.empty())
      || OB_UNLIKELY(desc.count() * num_rows != insert_values.count())) {
    ret = OB_INVALID_ARGUMENT;
  }

  OX (ret = insert_sql.assign("INSERT INTO "));
  OX (ret = insert_sql.append(q_name));
  for (int64_t i = 0; OB_SUCC(ret) && i < desc.count(); ++i) {
    OX (ret = insert_sql.append(0 == i ? "(" : ","));
    OX (ret = insert_sql.append_fmt("%c%.*s%c", q,
                                   desc.at(i).column_name_.length(), desc.at(i).column_name_.ptr(),
                                   q));
  }
  OX (ret = insert_sql.append(") VALUES "));
  for (int64_t i = 0; OB_SUCC(ret) && i < insert_values.count(); ++i) {
    OX (ret = insert_sql.append(i % desc.count() == 0 ? (i == 0 ? "(" : "),(") : ","));
    OX (ret = insert_sql.append_fmt("'%.*s'",
                                   insert_values.at(i).length(), insert_values.at(i).ptr()));
  }
  OX (ret = insert_sql.append(")"));

  if (OB_FAIL(ret)) {
    LOG_WARN("fail to append data", K(ret), K(insert_sql));
  } else {
    LOG_DEBUG("insert sql generated", K(insert_sql));
  }

  return ret;
}

int ObLoadDataBase::make_parameterize_stmt(ObExecContext &ctx,
                                           ObSqlString &insertsql,
                                           ParamStore &param_store,
                                           ObInsertStmt *&insert_stmt)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = NULL;

  if (OB_ISNULL(session = ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (OB_ISNULL(ctx.get_sql_ctx())
             || OB_ISNULL(ctx.get_sql_ctx()->schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql ctx is null", K(ret));
  } else {
    ObParser parser(ctx.get_allocator(), session->get_sql_mode());
    ParseResult parse_result;

    SqlInfo not_param_info;
    bool is_transform_outline = false;
    ObMaxConcurrentParam::FixParamStore fixed_param_store(OB_MALLOC_NORMAL_BLOCK_SIZE,
                                                          ObWrapperAllocator(&ctx.get_allocator()));
    if (OB_FAIL(parser.parse(insertsql.string(), parse_result))) {
      LOG_WARN("parser template insert sql failed", K(ret));
    } else if (OB_FAIL(ObSqlParameterization::transform_syntax_tree(ctx.get_allocator(),
                                                                    *session,
                                                                    NULL,
                                                                    parse_result.result_tree_,
                                                                    not_param_info,
                                                                    param_store,
                                                                    NULL,
                                                                    fixed_param_store,
                                                                    is_transform_outline))) {
      LOG_WARN("parameterize parser tree failed", K(ret));
    } else {
      SMART_VAR(ObResolverParams, resolver_ctx) {
        ObSchemaChecker schema_checker;
        schema_checker.init(*(ctx.get_sql_ctx()->schema_guard_));
        resolver_ctx.allocator_  = &ctx.get_allocator();
        resolver_ctx.schema_checker_ = &schema_checker;
        resolver_ctx.session_info_ = session;
        resolver_ctx.param_list_ = &param_store;
        resolver_ctx.database_id_ = session->get_database_id();
        resolver_ctx.disable_privilege_check_ = PRIV_CHECK_FLAG_DISABLE;
        resolver_ctx.expr_factory_ = ctx.get_expr_factory();
        resolver_ctx.stmt_factory_ = ctx.get_stmt_factory();
        if (OB_ISNULL(ctx.get_stmt_factory())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid argument", K(ret), KP(ctx.get_stmt_factory()));
        } else if (OB_ISNULL(ctx.get_stmt_factory()->get_query_ctx())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid argument", K(ret), KP(ctx.get_stmt_factory()->get_query_ctx()));
        } else {
          resolver_ctx.query_ctx_ = ctx.get_stmt_factory()->get_query_ctx();
          resolver_ctx.query_ctx_->question_marks_count_ = param_store.count();
          resolver_ctx.query_ctx_->sql_schema_guard_.set_schema_guard(ctx.get_sql_ctx()->schema_guard_);
          ObResolver resolver(resolver_ctx);
          ObStmt *astmt = NULL;
          ParseNode *stmt_tree = parse_result.result_tree_->children_[0];
          if (OB_ISNULL(stmt_tree) || OB_ISNULL(ctx.get_stmt_factory()->get_query_ctx())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid argument", K(ret), K(stmt_tree));
          } else if (OB_FAIL(resolver.resolve(ObResolver::IS_NOT_PREPARED_STMT,
                                              *stmt_tree,
                                              astmt))) {
            LOG_WARN("resolve sql failed", K(ret), K(insertsql));
          } else {
            insert_stmt = static_cast<ObInsertStmt*>(astmt);
            ctx.get_stmt_factory()->get_query_ctx()->reset();
          }
        }
      }
    }
  }
  return ret;
}

int ObLoadDataBase::memory_check_remote(uint64_t tenant_id, bool &need_wait_minor_freeze)
{
  int ret = OB_SUCCESS;

  MTL_SWITCH(tenant_id) {
    storage::ObTenantFreezer *freezer = nullptr;
    if (FALSE_IT(freezer = MTL(storage::ObTenantFreezer *))) {
    } else {
      int64_t active_memstore_used = 0;
      int64_t total_memstore_used = 0;
      int64_t major_freeze_trigger = 0;
      int64_t memstore_limit = 0;
      int64_t freeze_cnt = 0;

      if (OB_FAIL(freezer->get_tenant_memstore_cond(active_memstore_used,
                                                    total_memstore_used,
                                                    major_freeze_trigger,
                                                    memstore_limit,
                                                    freeze_cnt))) {
        LOG_WARN("fail to get memstore used", K(ret));
      } else {
        if (total_memstore_used > (memstore_limit - major_freeze_trigger)/2 + major_freeze_trigger) {
          need_wait_minor_freeze = true;
        } else {
          need_wait_minor_freeze = false;
        }
      }
      LOG_DEBUG("load data check tenant memory usage", K(active_memstore_used),
                                                       K(total_memstore_used),
                                                       K(major_freeze_trigger),
                                                       K(memstore_limit),
                                                       K(freeze_cnt),
                                                       K(need_wait_minor_freeze));
    }
  } else {
    LOG_ERROR("switch tenant failed", K(tenant_id), K(ret));
  }
  return ret;
}

/*
 * if param_a != param_b: this variable is from a field of data file,
 * calc the corresponding field index via param string value
 * return the index
*/
int ObLoadDataBase::calc_param_offset(const ObObj &param_a,
                                      const ObObj &param_b,
                                      int64_t &idx)
{
  int ret = OB_SUCCESS;
  if (!param_a.is_varchar_or_char() || !param_b.is_varchar_or_char()) {
    idx = OB_INVALID_INDEX_INT64;
  } else if (param_a.get_string().compare(param_b.get_string()) != 0) {
    const ObObj &value = param_a;
    const char *value_ptr = value.get_string_ptr();
    /* 这里处理的insert模板中的数据，是自己造的，
       一定是string类型，字符串内容是 "[F|f][0-9]+"，因此长度大于等于2 */
    if (value.get_string_len() < 2 || NULL == value_ptr) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("no possible, the values are changed", K(ret));
    } else {
      int64_t temp_idx = 0;
      for (int32_t j = 1; OB_SUCC(ret) && j < value.get_string_len(); ++j) {
        char cur_char = *(value_ptr + j);
        if (cur_char > '9' || cur_char < '0') {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("no possible, the values are changed", K(ret));
        } else {
          temp_idx *= 10;
          temp_idx += cur_char - '0';
        }
      }
      idx = temp_idx;
    }
  }
  return ret;
}

int ObLoadDataBase::memory_wait_local(ObExecContext &ctx,
                                      const ObTabletID &tablet_id,
                                      ObAddr &server_addr,
                                      int64_t &total_wait_secs,
                                      bool &is_leader_changed)
{
  int ret = OB_SUCCESS;
  static const int64_t WAIT_INTERVAL_US = 1 * 1000 * 1000;  //1s
  ObSQLSessionInfo *session = NULL;
  ObMySQLProxy *sql_proxy_ = NULL;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    sqlclient::ObMySQLResult *result = NULL;
    ObSqlString sql;
    int64_t start_wait_ts = ObTimeUtil::current_time();
    int64_t wait_timeout_ts = 0;
    uint64_t tenant_id = OB_INVALID_TENANT_ID;

    if (OB_UNLIKELY(!tablet_id.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid server addr", K(ret), K(tablet_id));
    }  else if (OB_ISNULL((sql_proxy_ = GCTX.sql_proxy_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sql_proxy is null", K(ret));
    } else if (OB_ISNULL(session = ctx.get_my_session())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session is null", K(ret));
    } else {
      session->get_query_timeout(wait_timeout_ts);
      tenant_id = session->get_effective_tenant_id();
      //print info
      LOG_INFO("LOAD DATA is suspended until the memory is available",
               K(tablet_id), K(server_addr), K(total_wait_secs));
    }

    bool need_wait_freeze = true;
    ObAddr leader_addr;
    ObDASLocationRouter &loc_router = DAS_CTX(ctx).get_location_router();

    while (OB_SUCC(ret) && need_wait_freeze) {

      ob_usleep(WAIT_INTERVAL_US);

      leader_addr.reset();
      res.reuse();
      char leader_ip_str[MAX_IP_ADDR_LENGTH];
      const int64_t retry_us = 200 * 1000;
      //Try to use the results in the cache as much as possible, without forcing a cache refresh.
      const int64_t expire_renew_time = 0;
      if (OB_FAIL(ObLoadDataUtils::check_session_status(*session))) {
        LOG_WARN("session is not valid during wait", K(ret));
      } else if (OB_FAIL(loc_router.get_leader(tenant_id, tablet_id, leader_addr, expire_renew_time))) {
        LOG_WARN("failed to get location", K(ret));
        ob_usleep(retry_us);
      } else {
        LOG_DEBUG("get participants", K(tablet_id), K(leader_addr));
      }

      if (OB_FAIL(ret)) {
      } else if (!leader_addr.ip_to_string(leader_ip_str, sizeof(leader_ip_str))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("format leader ip failed", K(ret), K(leader_addr));
      } else if (OB_FAIL(sql.assign_fmt(SERVER_TENANT_MEMORY_EXAMINE_SQL,
                                        tenant_id,
                                        leader_ip_str,
                                        leader_addr.get_port()))) {
        LOG_WARN("fail to append sql", K(ret), K(tenant_id), K(leader_addr));
      } else if (OB_FAIL(sql_proxy_->read(res, OB_SYS_TENANT_ID, sql.ptr()))) {
        LOG_WARN("fail to execute sql", K(ret), K(sql));
      } else if (NULL == (result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get sql result", K(ret));
      } else if (OB_FAIL(result->next())) {
        LOG_WARN("fail to get result, force renew location", K(ret), K(leader_addr));
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
      } else {
        EXTRACT_BOOL_FIELD_MYSQL(*result, "need_wait_freeze", need_wait_freeze);
        //LOG_INFO("LOAD DATA is waiting for tenant memory available",
                 //K(waited_seconds), K(total_wait_secs), K(tenant_id));
      }
      //if it is location exception, refresh location cache with block interface
      //because load data can only local retry
      loc_router.refresh_location_cache_by_errno(false, ret);
    }

    //print info
    if (OB_SUCC(ret)) {
      int64_t wait_secs = (ObTimeUtil::current_time() - start_wait_ts) / 1000000;
      total_wait_secs += wait_secs;
      if (leader_addr != server_addr) {
        LOG_INFO("LOAD DATA location change",
                 "old_addr", server_addr,
                 "new_addr", leader_addr);
        server_addr = leader_addr;
        is_leader_changed = true;
      } else {
        is_leader_changed = false;
      }
      LOG_INFO("LOAD DATA is resumed",
               "waited_seconds", wait_secs,
               K(total_wait_secs));
    }

  }
  return ret;
}

int ObLoadDataBase::pre_parse_lines(ObLoadFileBuffer &buffer,
                                    ObCSVGeneralParser &parser,
                                    bool is_last_buf,
                                    int64_t &valid_len,
                                    int64_t &line_count)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!buffer.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid buffer", K(ret));
  } else if (parser.get_opt_params().is_simple_format_) {
    const ObCSVGeneralFormat &format = parser.get_format();
    char *cur_pos = buffer.begin_ptr();
    int64_t cur_lines = 0;
    for (char *p = buffer.begin_ptr(); p < buffer.current_ptr(); ++p) {
      char cur_char = *p;
      if (format.field_escaped_char_ == cur_char && p + 1 < buffer.current_ptr()) {
        p++;
      } else if (parser.get_opt_params().line_term_c_ == cur_char) {
        cur_lines++;
        cur_pos = p + 1;
        if (cur_lines >= line_count) {
          break;
        }
      }
    }
    if (is_last_buf && cur_lines < line_count && buffer.current_ptr() > cur_pos) {
      cur_lines++;
      cur_pos = buffer.current_ptr();
    }
    valid_len = cur_pos - buffer.begin_ptr();
    line_count = cur_lines;
  } else {
    ObSEArray<ObCSVGeneralParser::LineErrRec, 128> err_records;
    const char *ptr = buffer.begin_ptr();
    const char *end = ptr + buffer.get_data_len();
    auto unused_handler = [](ObIArray<ObCSVGeneralParser::FieldValue> &fields_per_line) -> int {
      UNUSED(fields_per_line);
      return OB_SUCCESS;
    };
    if (OB_FAIL(parser.scan(ptr, end, line_count, NULL, NULL, unused_handler, err_records, is_last_buf))) {
      LOG_WARN("fail to scan buf", K(ret));
    } else {
      valid_len = ptr - buffer.begin_ptr();
    }
  }

  return ret;
}

int ObInsertValueGenerator::fill_field_expr(ObIArray<ObCSVGeneralParser::FieldValue> &field_values,
                                            const ObBitSet<> &string_values)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(field_values.count() != field_exprs_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input", K(ret), K(field_values), K(field_exprs_));
  } else {
    for (int i = 0; i < field_values.count(); ++i) {
      auto expr = static_cast<ObConstRawExpr *>(field_exprs_.at(i));
      ObLoadDataBase::field_to_obj(expr->get_value(),
                                   field_values.at(i),
                                   cs_type_,
                                   string_values.has_member(i));
    }
  }
  return ret;
}

int ObInsertValueGenerator::gen_insert_values(ObIArray<ObString> &insert_values,
                                              ObStringBuf &str_buf)
{

  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < insert_exprs_.count(); ++i) {
    auto expr = insert_exprs_.at(i);
    ObString store_value;
    data_buffer_->reset();
    ObConstRawExpr *const_expr = NULL;

    if (expr->get_expr_type() == T_DEFAULT) {
      OZ (str_buf.write_string("DEFAULT", &store_value));
    } else if (expr->is_const_raw_expr()
               && (const_expr = static_cast<ObConstRawExpr *>(expr))->get_value().is_string_type()) {
      ObString const_string = const_expr->get_value().get_string();
      ObCollationType coll_type = const_expr->get_value().get_collation_type();
      uint32_t pos = 0;
      if (ObCharset::charset_type_by_coll(coll_type) != CHARSET_UTF8MB4) {
        if (OB_FAIL(ObCharset::charset_convert(
          coll_type, const_string.ptr(), const_string.length(),
          CS_TYPE_UTF8MB4_BIN, data_buffer_->begin_ptr(), data_buffer_->get_remain_len(), pos, false))) {
          LOG_WARN("fail to convert charset", K(ret));
        } else {
          const_string.assign_ptr(data_buffer_->begin_ptr(), pos);
          data_buffer_->update_pos(pos);
        }
      }
      if (OB_SUCC(ret)) {
        ObHexEscapeSqlStr escape_str(const_string, !!(SMO_NO_BACKSLASH_ESCAPES & sql_mode_));
        int64_t len = escape_str.to_string(data_buffer_->current_ptr() + 1, data_buffer_->get_remain_len() - 1);
        if (OB_UNLIKELY(len + 2 >= data_buffer_->get_remain_len())) {
          ret = OB_SIZE_OVERFLOW;
          LOG_WARN("fail to print string", K(ret), K(len), K(data_buffer_->get_remain_len()));
        } else {
          *data_buffer_->current_ptr() = '\'';
          *(data_buffer_->current_ptr() + 1 + len) = '\'';
          OZ (str_buf.write_string(ObString(static_cast<int32_t>(len + 2),
                                            data_buffer_->current_ptr()), &store_value));
        }
      }
    } else {
      OZ (expr_printer_.do_print(expr, T_NONE_SCOPE));
      OZ (str_buf.write_string(ObString(static_cast<int32_t>(data_buffer_->get_data_len()),
                                        data_buffer_->begin_ptr()), &store_value));
    }
    OX (insert_values.at(i) = store_value);
    //OZ (insert_values.push_back(store_value));
  }
  LOG_DEBUG("LOAD DATA insert values generated", K(insert_values));
  return ret;
}

int ObInsertValueGenerator::gen_insert_sql(ObSqlString &insert_sql)
{
  int ret = OB_SUCCESS;

  OZ (insert_sql.append(insert_header_));
  OZ (insert_sql.append(" VALUES("));

  for (int i = 0; OB_SUCC(ret) && i < insert_exprs_.count(); ++i) {
    auto expr = insert_exprs_.at(i);
    if (i != 0) {
      OZ (insert_sql.append(","));
    }
    /*if (expr->is_const_raw_expr() && static_cast<ObConstRawExpr *>(expr)->get_value().is_string_type()) {
      auto const_expr = static_cast<ObConstRawExpr *>(expr);
      OZ (insert_sql.append_fmt("'%.*s'",
                                const_expr->get_value().get_string_len(),
                                const_expr->get_value().get_string_ptr()));
    } else {
    */
      data_buffer_->reset();
      OZ (expr_printer_.do_print(expr, T_NONE_SCOPE));
      OZ (insert_sql.append(ObString(data_buffer_->get_data_len(), data_buffer_->begin_ptr())));
    //}
  }
  OZ (insert_sql.append(")"));

  return ret;
}

int ObInsertValueGenerator::set_params(ObString &insert_header, ObCollationType cs_type, int64_t sql_mode)
{
  insert_header_ = insert_header;
  cs_type_ = cs_type;
  sql_mode_ = sql_mode;
  return OB_SUCCESS;
}

int ObInsertValueGenerator::init(ObSQLSessionInfo &session,
                                 ObLoadFileBuffer *data_buffer,
                                 ObSchemaGetterGuard *schema_guard)
{
  ObObjPrintParams param = session.create_obj_print_params();
  param.cs_type_ = CS_TYPE_UTF8MB4_BIN;
  expr_printer_.init(data_buffer->begin_ptr(),
                     data_buffer->get_buffer_size(),
                     data_buffer->get_pos(),
                     schema_guard,
                     param);
  data_buffer_ = data_buffer;
  return OB_SUCCESS;
}

int ObLoadDataSPImpl::gen_insert_columns_names_buff(ObExecContext &ctx,
                                                    const ObLoadArgument &load_args,
                                                    ObIArray<ObLoadTableColumnDesc> &insert_infos,
                                                    ObString &data_buff,
                                                    bool need_online_osg)
{
  int ret = OB_SUCCESS;

  ObSqlString insert_stmt;

  ObSEArray<ObString, 16> insert_column_names;
  if (OB_FAIL(insert_column_names.reserve(insert_infos.count()))) {
    LOG_WARN("fail to reserve", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < insert_infos.count(); ++i) {
    if (OB_FAIL(insert_column_names.push_back(insert_infos.at(i).column_name_))) {
      LOG_WARN("fail to push back", K(ret));
    }
  }
  /*
  if (OB_SUCC(ret)) {
    int64_t len = 0;
    char *buf = 0;
    OB_UNIS_ADD_LEN(insert_column_names);
    if (OB_ISNULL(buf = static_cast<char *>(ctx.get_allocator().alloc(len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc memory failed", K(ret));
    } else {
      data_buff.set_data(buf, len);
      int64_t buf_len = len;
      int64_t pos = 0;
      OB_UNIS_ENCODE(insert_column_names);
    }
  }
  */

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObLoadDataUtils::build_insert_sql_string_head(load_args.dupl_action_,
                                                              load_args.combined_name_,
                                                              insert_column_names,
                                                              insert_stmt,
                                                              need_online_osg))) {
      LOG_WARN("gen insert sql column_names failed", K(ret));
    } else if (OB_FAIL(ob_write_string(ctx.get_allocator(), insert_stmt.string(), data_buff))) {
      LOG_WARN("fail to write string", K(ret));
    }
  }

  return ret;
}

class ReplaceVariables : public ObIRawExprReplacer
{
public:
  ReplaceVariables(ObExecContext &ctx,
                   ObLoadDataStmt &stmt,
                   ObIArray<ObRawExpr *> &fields)
    : ctx_(ctx), load_stmt_(stmt), field_exprs_(fields) {}

  int generate_new_expr(ObRawExprFactory &expr_factory, ObRawExpr *raw_expr, ObRawExpr *&new_expr)
  {
    int ret = OB_SUCCESS;
    UNUSED(expr_factory);
    ObSQLSessionInfo *session = NULL;
    if (OB_ISNULL(raw_expr)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K((ret)));
    } else if (OB_ISNULL(session = ctx_.get_my_session())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session is null", K(ret));
    } else if (raw_expr->get_expr_type() == T_REF_COLUMN
               || raw_expr->get_expr_type() == T_OP_GET_USER_VAR) {
      ObRawExpr *orig_expr = raw_expr;
      bool is_user_variable = false;
      //1. get variable name
      ObString ref_name;
      if (raw_expr->get_expr_type() == T_REF_COLUMN) {
        ObColumnRefRawExpr *column_ref = static_cast<ObColumnRefRawExpr*>(raw_expr);
        ref_name = column_ref->get_column_name();
      } else {
        is_user_variable = true;
        ObSysFunRawExpr *func_expr = static_cast<ObSysFunRawExpr*>(raw_expr);
        if (func_expr->get_children_count() != 1) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("sys func expr child num is not correct", K(ret));
        } else {
          ObConstRawExpr *c_expr = static_cast<ObConstRawExpr*>(func_expr->get_param_expr(0));
          if (c_expr->get_value().get_type() != ObVarcharType) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("const expr child type is not correct", K(ret));
          } else {
            ref_name = c_expr->get_value().get_string();
          }
        }
      }
      //2. find and replace
      int64_t idx = OB_INVALID_INDEX;

      if (OB_SUCC(ret)) {
        for (int64_t i = 0; i < load_stmt_.get_field_or_var_list().count(); ++i) {
          if (0 == load_stmt_.get_field_or_var_list().at(i).field_or_var_name_.compare(ref_name)) {
            idx = i;
            break;
          }
        }

        if (OB_INVALID_INDEX != idx) {
          new_expr = field_exprs_.at(idx);
        } else {
          if (!is_user_variable) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unknown column name in set right expr, do nothing", K(ret), K(ref_name));
          } else {
            ObConstRawExpr *c_expr = NULL;
            //find the real value from session
            if (OB_ISNULL(c_expr = OB_NEWx(ObConstRawExpr, (&ctx_.get_allocator())))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("allocate const raw expr failed", K(ret));
            } else {
              ObObj var_obj;
              ObSessionVariable user_var;
              if (OB_FAIL(session->get_user_variable(ref_name, user_var))) {
                LOG_WARN("get user variable failed", K(ret), K(ref_name));
              } else {
                var_obj = user_var.value_;
                var_obj.set_meta_type(user_var.meta_);
                c_expr->set_value(var_obj);
                new_expr = c_expr;
              }
            }
          }
        }
      }
      /*
    if (OB_SUCC(ret) && need_replaced_to_loaded_data_from_file) {
      raw_expr = c_expr;
      ObLoadDataReplacedExprInfo varable_info;
      varable_info.replaced_expr = c_expr;
      varable_info.correspond_file_field_idx = idx;
      if (OB_FAIL(generator.add_file_column_replace_info(varable_info))) {
        LOG_WARN("push back replaced variable infos array failed", K(ret));
      }
    }
*/
      LOG_DEBUG("replace variable name to field value",
                K(ref_name), K(idx), KPC(orig_expr), KPC(raw_expr), KPC(new_expr));

    }
    return ret;
  }

  ObExecContext &ctx_;
  ObLoadDataStmt &load_stmt_;
  ObIArray<ObRawExpr *> &field_exprs_;
};

int ObLoadDataSPImpl::copy_exprs_for_shuffle_task(ObExecContext &ctx,
                                                  ObLoadDataStmt &load_stmt,
                                                  ObIArray<ObLoadTableColumnDesc> &insert_infos,
                                                  ObIArray<ObRawExpr *> &field_exprs,
                                                  ObIArray<ObRawExpr *> &insert_exprs)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(ctx.get_expr_factory())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr factory is null", K(ret));
  }

  OZ (field_exprs.reserve(load_stmt.get_field_or_var_list().count()));

  for (int i = 0; OB_SUCC(ret) && i < load_stmt.get_field_or_var_list().count(); ++i) {
    ObConstRawExpr *field_expr = NULL;
    OZ (ObRawExprUtils::build_const_string_expr(*ctx.get_expr_factory(),
                                                ObVarcharType,
                                                ObString(),
                                                load_stmt.get_load_arguments().file_cs_type_,
                                                field_expr));
    OZ (field_exprs.push_back(field_expr));
  }

  OZ (insert_exprs.reserve(insert_infos.count()));

  if (OB_SUCC(ret)) {
    ObRawExprCopier copier(*ctx.get_expr_factory());
    ReplaceVariables replacer(ctx, load_stmt, field_exprs);
    for (int i = 0; OB_SUCC(ret) && i < insert_infos.count(); ++i) {
      ObRawExpr *insert_expr = nullptr;
      ObLoadTableColumnDesc &desc = insert_infos.at(i);
      if (OB_NOT_NULL(desc.expr_value_)) {
        OZ (copier.copy_on_replace(desc.expr_value_, insert_expr, &replacer));
      } else {
        insert_expr = field_exprs.at(desc.array_ref_idx_);
      }
      OZ (insert_exprs.push_back(insert_expr));
      LOG_DEBUG("push final insert expr", KPC(insert_expr));
    }
  }
  return ret;
}

int ObLoadDataSPImpl::gen_load_table_column_desc(ObExecContext &ctx,
                                                 ObLoadDataStmt &load_stmt,
                                                 ObIArray<ObLoadTableColumnDesc> &insert_infos)
{
  UNUSED(ctx);
  int ret = OB_SUCCESS;

  //e.g. general stmt like "INTO TABLE t1 (c1, c2, @a, @b) SET c3 = @a + @b"
  // step 1: add c1 and c2
  //     the first column of file will be written to t1.c1, so c1 will be added to the generator
  //     similarly, the second column to t1.c2 which also will be added to the generator
  // step 2: add c3 (calced by the first assign)
  //     @a, @b is not match column name, but their data will produce c3 by the "SET" clause,
  //     in result, c3 will be added
  //     in addition, replace expr @a with a const string expr which refer to a column from file
  //     do the same replace to @b

  //step 1
  for (int64_t i = 0; OB_SUCC(ret) && i < load_stmt.get_field_or_var_list().count(); ++i) {
    ObLoadDataStmt::FieldOrVarStruct &item = load_stmt.get_field_or_var_list().at(i);
    if (item.is_table_column_) {
      ObLoadTableColumnDesc tmp_info;
      tmp_info.is_set_values_ = false;
      tmp_info.column_name_ = item.field_or_var_name_;
      tmp_info.column_id_ = item.column_id_;
      tmp_info.column_type_ = item.column_type_;
      tmp_info.array_ref_idx_ = i; //array offset
      tmp_info.expr_value_ = NULL;
      if (OB_FAIL(insert_infos.push_back(tmp_info))) {
        LOG_WARN("push str failed", K(ret));
      }
    } else {
      //do nothing
      //ignore variables temporarily
    }
  }

  //step 2
  for (int64_t i = 0; OB_SUCC(ret) && i < load_stmt.get_table_assignment().count(); ++i) {
    const ObAssignment &assignment = load_stmt.get_table_assignment().at(i);
    ObColumnRefRawExpr *left = assignment.column_expr_;
    ObRawExpr *right = assignment.expr_;
    if (OB_ISNULL(left)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("set assign expr is null", K(ret));
    } /*else if (OB_FAIL(ObRawExprUtils::copy_expr(*ctx.get_expr_factory(),
                                                 assignment.expr_,
                                                 right,
                                                 COPY_REF_SHARED))) {
      LOG_WARN("fail to copy expr", K(ret));
    } */else {
      int64_t found_index = OB_INVALID_INDEX_INT64;
      for (int64_t j = 0; j < insert_infos.count(); ++j) {
        if (insert_infos.at(j).column_id_ == left->get_column_id()) {
          found_index = j;
          break;
        }
      }

      if (found_index != OB_INVALID_INDEX_INT64) {
        //overwrite
        ObLoadTableColumnDesc &tmp_info = insert_infos.at(found_index);
        tmp_info.is_set_values_ = true;
        tmp_info.array_ref_idx_ = OB_INVALID_INDEX_INT64;
        tmp_info.expr_value_ = right;
      } else {
        //a new insert column is defined by set expr
        ObLoadTableColumnDesc tmp_info;
        tmp_info.column_name_ = left->get_column_name();
        tmp_info.column_id_ = left->get_column_id();
        tmp_info.column_type_ = left->get_result_type().get_type();
        tmp_info.is_set_values_ = true;
        tmp_info.expr_value_ = right;
        if (OB_FAIL(insert_infos.push_back(tmp_info))) {
          LOG_WARN("push str failed", K(ret));
        }
      }
    }
  }

  LOG_DEBUG("generate insert info", K(insert_infos));

  return ret;
}



void ObCSVFormats::init(const ObDataInFileStruct &file_formats)
{
  field_term_char_ = file_formats.field_term_str_.empty() ?
        INT64_MAX : file_formats.field_term_str_[0];
  line_term_char_ = file_formats.line_term_str_.empty() ?
        INT64_MAX : file_formats.line_term_str_[0];
  enclose_char_ = file_formats.field_enclosed_char_;
  escape_char_ = file_formats.field_escaped_char_;
  null_column_fill_zero_string_ = lib::is_mysql_mode();

  if (!file_formats.field_term_str_.empty()
      && file_formats.line_term_str_.empty()) {
    is_line_term_by_counting_field_ = true;
    line_term_char_ = field_term_char_;
  }
  is_simple_format_ =
      !is_line_term_by_counting_field_
      && (field_term_char_ != INT64_MAX)
      && (line_term_char_ != INT64_MAX)
      && (field_term_char_ != line_term_char_)
      && (enclose_char_ == INT64_MAX);

}

ObShuffleTaskHandle::ObShuffleTaskHandle(ObDataFragMgr &main_datafrag_mgr,
                                         ObBitSet<> &main_string_values,
                                         uint64_t tenant_id)
  : allocator(ObMemAttr(tenant_id, ObModIds::OB_SQL_LOAD_DATA)),
    exec_ctx(allocator, GCTX.session_mgr_),
    data_buffer(NULL),
    escape_buffer(NULL),
    calc_tablet_id_expr(NULL),
    datafrag_mgr(main_datafrag_mgr),
    string_values(main_string_values)
{
  attr = ObMemAttr(tenant_id, ObModIds::OB_SQL_LOAD_DATA);
}

ObShuffleTaskHandle::~ObShuffleTaskHandle()
{
  if (OB_NOT_NULL(data_buffer)) {
    ob_free(data_buffer);
  }
  if (OB_NOT_NULL(escape_buffer)) {
    ob_free(escape_buffer);
  }
}

int ObShuffleTaskHandle::expand_buf(const int64_t max_size, const int64_t to_buffer_size)
{
  int ret = OB_SUCCESS;
  int64_t new_size = to_buffer_size;
  if (new_size > max_size) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("buffer size not enough", K(ret));
  } else {
    void *buf1 = NULL;
    void *buf2 = NULL;
    if (OB_ISNULL(buf1 = ob_malloc(new_size, attr))
        || OB_ISNULL(buf2 = ob_malloc(new_size, attr))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      if (OB_NOT_NULL(data_buffer)) {
        ob_free(data_buffer);
      }
      data_buffer = new(buf1) ObLoadFileBuffer(
            new_size - sizeof(ObLoadFileBuffer));
      if (OB_NOT_NULL(escape_buffer)) {
        ob_free(escape_buffer);
      }
      escape_buffer = new(buf2) ObLoadFileBuffer(
            new_size - sizeof(ObLoadFileBuffer));
    }
  }
  LOG_DEBUG("expand buf to", K(new_size));
  return ret;
}

int ObLoadDataSPImpl::exec_shuffle(int64_t task_id, ObShuffleTaskHandle *handle)
{
  int ret = OB_SUCCESS;

  int64_t tenant_id = OB_INVALID_TENANT_ID;
  void *expr_buf = NULL;
  ObLoadFileBuffer *expr_buffer = NULL;
  ObArrayHashMap<ObTabletID, ObDataFrag *> part_buf_mgr;
  ObSEArray<ObString, 32> insert_values;
  int64_t parsed_line_num = 0;
  ObStringBuf str_buf("LoadDataStrBuf", OB_MALLOC_MIDDLE_BLOCK_SIZE);

  //为了调用 part_buf_mgr.for_each，使用了匿名函数, &引用了外部的 frag_mgr
  auto save_frag = [&] (ObTabletID tablet_id, ObDataFrag *frag) -> bool
  {
    //将存满数据的frag按照分区放入frag_mgr
    int ret = OB_SUCCESS;
    ObPartDataFragMgr *part_datafrag_mgr = NULL;
    if (OB_FAIL(handle->datafrag_mgr.get_part_datafrag(tablet_id,
                                                       part_datafrag_mgr))) {
      LOG_WARN("fail to get part datafrag", K(ret), K(tablet_id));
    } else if (OB_ISNULL(part_datafrag_mgr)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(part_datafrag_mgr->queue_.push(frag))) {
      LOG_WARN("fail to push frag", K(ret));
    } else {
      ATOMIC_AAF(&(part_datafrag_mgr->total_row_proceduced_), frag->row_cnt);
      LOG_DEBUG("saving frag", K(tablet_id), K(*frag));
    }
    return OB_SUCCESS == ret;
  };

  auto free_frag = [&] (ObTabletID tablet_id, ObDataFrag *frag) -> bool
  {
    if (OB_NOT_NULL(frag)) {
      handle->datafrag_mgr.distory_datafrag(frag);
    }
    return true;
  };

    const int64_t buf_len = handle->data_buffer->get_buffer_size() + sizeof(ObLoadFileBuffer);
  if (OB_ISNULL(handle)
      || OB_ISNULL(handle->data_buffer)
      || OB_ISNULL(handle->escape_buffer)
      || OB_ISNULL(handle->exec_ctx.get_my_session())
      || OB_ISNULL(handle->exec_ctx.get_sql_ctx())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP(handle));
//  } else if (FALSE_IT(handle->exec_ctx.get_allocator().reuse())) {
  } else if (FALSE_IT(tenant_id = handle->exec_ctx.get_my_session()->get_effective_tenant_id())) {
  } else if (OB_FAIL(part_buf_mgr.init(ObMemAttr(tenant_id, ObModIds::OB_SQL_LOAD_DATA),
                                       handle->datafrag_mgr.get_total_part_cnt()))) {
    LOG_WARN("fail to init part buf mgr", K(ret));
  } else if (OB_FAIL(insert_values.prepare_allocate(
                       handle->generator.get_insert_exprs().count()))) {
    LOG_WARN("fail to prealloc", K(ret),
             "insert values count", handle->generator.get_insert_exprs().count());
  } else if (OB_ISNULL(expr_buf = ob_malloc(handle->data_buffer->get_buffer_size() + sizeof(ObLoadFileBuffer),
                                            ObMemAttr(tenant_id, ObModIds::OB_SQL_LOAD_DATA)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("not enough memory", K(ret));
  } else {
    handle->err_records.reuse();
    expr_buffer = new(expr_buf) ObLoadFileBuffer(handle->data_buffer->get_buffer_size());
    ObSEArray<ObCSVGeneralParser::LineErrRec, 1> err_records;
    ObSEArray<ObObj, 32> parse_result;

    int64_t nrows = 1;
    const char *ptr = handle->data_buffer->begin_ptr();
    const char *end = handle->data_buffer->begin_ptr() + handle->data_buffer->get_data_len();

    auto handle_one_line = [](ObIArray<ObCSVGeneralParser::FieldValue> &fields_per_line) -> int {
      UNUSED(fields_per_line);
      return common::OB_SUCCESS;
    };

    if (OB_FAIL(handle->generator.init(*(handle->exec_ctx.get_my_session()), expr_buffer,
                                       handle->exec_ctx.get_sql_ctx()->schema_guard_))) {
      LOG_WARN("fail to init buffer", K(ret));
    } else if (OB_FAIL(parse_result.prepare_allocate(handle->generator.get_field_exprs().count()))) {
      LOG_WARN("fail to allocate", K(ret));
    } else {
      handle->exec_ctx.set_use_temp_expr_ctx_cache(true);
    }

    while (OB_SUCC(ret) && ptr < end) {
      const char *prev_ptr = ptr; //save the old value of ptr
      err_records.reuse();
      ret = handle->parser.scan<decltype(handle_one_line), true>(ptr, end, nrows,
                                                                 handle->escape_buffer->begin_ptr(),
                                                                 handle->escape_buffer->begin_ptr() + handle->escape_buffer->get_buffer_size(),
                                                                 handle_one_line, err_records, true);
      if (OB_FAIL(ret)) {
        LOG_WARN("fail to scan", K(ret));
      } else {
        if (err_records.count() > 0) {
          ObParserErrRec rec;
          rec.row_offset_in_task = parsed_line_num;
          rec.ret = err_records[0].err_code;
          if (OB_FAIL(handle->err_records.push_back(rec))) {
            LOG_WARN("fail to push back", K(ret));
          }
        }
      }
      if (OB_SUCC(ret) && nrows > 0) {
        int64_t cur_line_num = parsed_line_num++;
        //计算partition id
        ObObj result;
        ObTabletID tablet_id;
        //insert_values.reuse();
        str_buf.reuse();
        if (OB_FAIL(handle->generator.fill_field_expr(handle->parser.get_fields_per_line(),
                                                      handle->string_values))) {
          LOG_WARN("fail to fill field expr", K(ret));
        } else if (OB_FAIL(handle->generator.gen_insert_values(insert_values, str_buf))) {
          LOG_WARN("fail to generate insert values", K(ret));
        } else if (nullptr == handle->calc_tablet_id_expr) {
          int64_t idx = task_id % handle->datafrag_mgr.get_tablet_ids().count();
          tablet_id = handle->datafrag_mgr.get_tablet_ids().at(idx);
        } else {
          for (int i = 0; i < handle->parser.get_fields_per_line().count(); ++i) {
            ObCSVGeneralParser::FieldValue &str_v = handle->parser.get_fields_per_line().at(i);
            handle->row_in_file.get_cell(i) =
              static_cast<ObConstRawExpr *>(handle->generator.get_field_exprs().at(i))->get_value();
          }
          if (OB_FAIL(handle->calc_tablet_id_expr->eval(handle->exec_ctx, handle->row_in_file, result))) {
            LOG_WARN("fail to calc tablet id", K(ret));
          } else {
            tablet_id = ObTabletID(result.get_uint64());
            if (OB_UNLIKELY(!tablet_id.is_valid())) {
              ret = OB_NO_PARTITION_FOR_GIVEN_VALUE;
              LOG_WARN("invalid partition for given value", K(ret));
            }
          }
        }

        LOG_DEBUG("LOAD DATA", "TheadId", get_tid_cache(), K(cur_line_num), K(tablet_id),
                  "line", handle->parser.get_fields_per_line(), "values", insert_values);

        //序列化到DataFrag
        int64_t len = 0;
        OB_UNIS_ADD_LEN(insert_values);
        OB_UNIS_ADD_LEN(cur_line_num);
        int64_t row_ser_size = len;
        OB_UNIS_ADD_LEN(row_ser_size);

        ObDataFrag *frag = NULL;
        if (OB_SUCC(ret)) {
          int temp_ret = part_buf_mgr.get(tablet_id, frag);
          bool frag_exist = (OB_SUCCESS == temp_ret);
          if (!frag_exist || len > frag->get_remain()) {
            //新建一个
            ObDataFrag *new_frag = NULL;
            if (OB_FAIL(handle->datafrag_mgr.create_datafrag(new_frag, len))) {
              LOG_WARN("fail to create data fragment", K(ret));
            } else {
              if (frag_exist) {
                if (OB_UNLIKELY(!save_frag(tablet_id, frag))) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("fail to save frag", K(ret));
                } else if (OB_FAIL(part_buf_mgr.update(tablet_id, new_frag))) {
                  //never goes here
                  LOG_ERROR("fail to install new frag", K(ret));
                }
              } else {
                if (OB_FAIL(part_buf_mgr.insert(tablet_id, new_frag))) {
                  LOG_ERROR("fail to insert new frag", K(ret));
                }
              }
              if (OB_SUCC(ret)) {
                frag = new_frag;
                frag->shuffle_task_id = task_id;
              } else {
                handle->datafrag_mgr.distory_datafrag(new_frag);
              }
            }
          }
        }

        if (OB_SUCC(ret)) {
          char *buf = frag->get_current();
          int64_t buf_len = frag->get_remain();
          int64_t pos = 0;
          OB_UNIS_ENCODE(row_ser_size);
          OB_UNIS_ENCODE(cur_line_num);
          OB_UNIS_ENCODE(insert_values);
          if (OB_SUCC(ret)) {
            frag->add_pos(pos);
            frag->add_row_cnt(1);
            //use the pointer change to calculate the original data size read to the frag
            frag->add_orig_data_size(static_cast<int64_t>(ptr - prev_ptr));
          }
        }
      }//end if yield
    } //end while

    if (OB_SUCC(ret)) {
      if (OB_FAIL(part_buf_mgr.for_each(save_frag))) {
        LOG_WARN("fail to for each", K(ret));
      }
    }

  }

  if (OB_FAIL(ret)) {
    part_buf_mgr.for_each(free_frag);
  }

  if (OB_NOT_NULL(expr_buf)) {
    ob_free(expr_buf);
  }

  handle->result.row_cnt_ = parsed_line_num;

  return ret;
}

int ObLoadDataSPImpl::exec_insert(ObInsertTask &task, ObInsertResult& result)
{
  UNUSED(result);
  int ret = OB_SUCCESS;
  int64_t sql_buff_len_init = OB_MALLOC_BIG_BLOCK_SIZE; //2M
  int64_t field_buf_len = OB_MAX_VARCHAR_LENGTH;
  char *field_buff = NULL;
  ObMemAttr attr(task.tenant_id_, ObModIds::OB_SQL_LOAD_DATA);
  ObSqlString sql_str;
  ObSEArray<ObString, 1> single_row_values;
  sql_str.set_attr(attr);

#ifdef TEST_MODE
  delay_process_by_probability(INSERT_TASK_DROP_RATE);
#endif

  if (OB_ISNULL(field_buff = static_cast<char*>(ob_malloc(field_buf_len, attr)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to ob malloc", K(ret), K(field_buf_len));
  }
  OZ (single_row_values.reserve(task.column_count_));
  OZ (sql_str.extend(sql_buff_len_init));
  OZ (sql_str.append(task.insert_stmt_head_));
  OZ (sql_str.append(ObString(" values ")));

  int64_t deserialized_rows = 0;
  for (int64_t buf_i = 0; OB_SUCC(ret) && buf_i < task.insert_value_data_.count(); ++buf_i) {
    int64_t pos = 0;
    const char* buf = task.insert_value_data_[buf_i].ptr();
    int64_t data_len = task.insert_value_data_[buf_i].length();
    while (OB_SUCC(ret) && pos < data_len) {
      int64_t row_ser_size = 0;
      int64_t row_num = 0;
      OB_UNIS_DECODE(row_ser_size);
      int64_t pos_back = pos;
      OB_UNIS_DECODE(row_num);
      single_row_values.reuse();
      OB_UNIS_DECODE(single_row_values);
      if (OB_SUCC(ret) && (pos - pos_back != row_ser_size
                           || single_row_values.count() != task.column_count_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("row size is not as expected", "pos diff", pos - pos_back, K(row_ser_size),
                 "single row values count", single_row_values.count(), K(task.column_count_));
      }

      //print row
      if (deserialized_rows != 0) {
        OZ (sql_str.append(",", 1));
      }
      OZ (sql_str.append("(", 1));
      for (int64_t c = 0; OB_SUCC(ret) && c < single_row_values.count(); ++c) {
        //bool is_set_value = task.set_values_bitset_.has_member(c);
        if (c != 0) {
          OZ (sql_str.append(",", 1));
        }
        OZ (sql_str.append(single_row_values[c]));
      }
      OZ (sql_str.append(")", 1));

      deserialized_rows++;
    }

  } //end for

  if (OB_SUCC(ret) && deserialized_rows != task.row_count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data in task not match deserialized result",
             K(ret), K(deserialized_rows), K(task.row_count_));
  }

  if (OB_SUCC(ret)) {
    ObTZMapWrap tz_map_wrap;
    if (OB_FAIL(OTTZ_MGR.get_tenant_tz(task.tenant_id_, tz_map_wrap))) {
      LOG_WARN("get tenant timezone map failed", K(ret));
    } else {
      task.timezone_.set_tz_info_map(tz_map_wrap.get_tz_map());
    }
  }

  int64_t affected_rows = 0;
  ObSessionParam param;
  param.is_load_data_exec_ = true;

  param.sql_mode_ = &task.sql_mode_;
  param.tz_info_wrap_ = &task.timezone_;

  if (OB_SUCC(ret) && OB_FAIL(GCTX.sql_proxy_->write(task.tenant_id_,
                                                     sql_str.string(),
                                                     affected_rows,
                                                     get_compatibility_mode(),
                                                     &param))) {
    LOG_WARN("fail to exec insert remote", K(ret), "task_id", task.task_id_);
  }

  LOG_DEBUG("LOAD DATA remote process", K(affected_rows), K(task.task_id_), K(ret));

#ifdef TEST_MODE
  delay_process_by_probability(INSERT_TASK_DROP_RATE);
#endif

  if (OB_NOT_NULL(field_buff)) {
    ob_free(field_buff);
  }

  return ret;
}

int ObLoadDataSPImpl::wait_shuffle_task_return(ToolBox &box)
{
  int ret = OB_SUCCESS;
  int ret_bak = OB_SUCCESS;
  for (int64_t i = 0; i < box.parallel; ++i) {
    //ret失败也要循环，保证所有发出的task都返回或超时
    ObShuffleTaskHandle *handle = NULL;
    if (OB_FAIL(box.shuffle_task_controller.on_next_task())) {
      LOG_WARN("fail to on next task", K(ret));
    } else if (OB_FAIL(box.shuffle_task_reserve_queue.pop(handle))) {
      LOG_WARN("fail to pop shuffle handle", K(ret));
    } else if (OB_ISNULL(handle)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("shuffle task handle is null", K(ret));
    } else if (OB_UNLIKELY(handle->result.flags_.test_bit(ObTaskResFlag::RPC_TIMEOUT))) {
      ret = OB_TRANS_RPC_TIMEOUT;
      LOG_WARN("shuffle task rpc timeout handle", K(ret));
    } else if (OB_FAIL(handle->result.exec_ret_)) {
      LOG_WARN("shuffle remote exec failed", K(ret));
    } else if (handle->err_records.count() > 0
               && OB_FAIL(handle_returned_shuffle_task(box, *handle))) {
      LOG_WARN("fail to handle returned shuffle task", K(ret));
    } else {
      box.suffle_rt_sum += handle->result.process_us_;
    }
    if (OB_FAIL(ret) && OB_SUCCESS == ret_bak) {
      ret_bak = ret;
    }
  }

  if (OB_SUCCESS != ret_bak) {
    ret = ret_bak;
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < box.parallel; ++i) {
    ObShuffleTaskHandle *handle = box.shuffle_resource[i];
    if (OB_FAIL(box.shuffle_task_controller.on_task_finished())) {
      LOG_WARN("fail to on next task", K(ret));
    } else if (OB_FAIL(box.shuffle_task_reserve_queue.push_back(handle))) {
      LOG_WARN("fail to push back", K(ret));
    } else if (OB_ISNULL(handle)) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      handle->result.reset();
      handle->err_records.reuse();
    }
  }

  return ret;
}

int ObLoadDataSPImpl::handle_returned_shuffle_task(ToolBox &box, ObShuffleTaskHandle &handle)
{
  UNUSED(box);
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(handle.result.task_id_ >= box.file_buf_row_num.count()
                  || handle.result.task_id_ < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid array index", K(ret),
             K(handle.result.task_id_), K(box.file_buf_row_num.count()));
  } else if (!box.file_appender.is_opened()
             && OB_FAIL(create_log_file(box))) {
    LOG_WARN("fail to create log file", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < handle.err_records.count(); ++i) {
    int64_t line_num = box.file_buf_row_num.at(handle.result.task_id_)
                       + handle.err_records.at(i).row_offset_in_task;
    if (OB_FAIL(log_failed_line(box,
                                TaskType::ShuffleTask,
                                handle.result.task_id_,
                                line_num,
                                handle.err_records.at(i).ret,
                                ObString()))) {
      LOG_WARN("fail to log failed line", K(ret));
    }
  }

  return ret;
}

int ObLoadDataSPImpl::next_file_buffer(ObExecContext &ctx,
                                       ToolBox &box,
                                       ObShuffleTaskHandle *handle,
                                       int64_t limit)
{
  int ret = OB_SUCCESS;
  bool has_valid_data = true;

  CK (OB_NOT_NULL(handle) && OB_NOT_NULL(handle->data_buffer));

  do {

    if (OB_UNLIKELY(handle->data_buffer->get_struct_size() < box.data_trimer.get_buffer_size())) {
      OZ (handle->expand_buf(box.batch_buffer_size, box.data_trimer.get_buffer_size()));
    }

    //从data_trimer中恢复出上次读取剩下的数据
    OZ (box.data_trimer.recover_incomplate_data(*handle->data_buffer));

    OZ (box.file_reader->readn(handle->data_buffer->current_ptr(),
                               handle->data_buffer->get_remain_len(),
                               box.read_cursor.read_size_));

    if (OB_SUCC(ret)) {
      if (OB_LIKELY(box.read_cursor.read_size_ > 0)) {
        handle->data_buffer->update_pos(box.read_cursor.read_size_); //更新buffer中数据长度
        int64_t last_proccessed_GBs = box.read_cursor.get_total_read_GBs();
        box.read_cursor.commit_read();
        int64_t processed_GBs = box.read_cursor.get_total_read_GBs();
        if (processed_GBs != last_proccessed_GBs) {
          LOG_INFO("LOAD DATA file read progress: ", K(processed_GBs));
        }

        box.job_status->read_bytes_ += box.read_cursor.read_size_;
      } else if (box.file_reader->eof()) {
        box.read_cursor.is_end_file_ = true;
        LOG_DEBUG("LOAD DATA reach file end", K(box.read_cursor));
      }
    }

    //从buffer中找出完整的行，剩下的备份到 data_trimer
    if (OB_SUCC(ret) && OB_LIKELY(handle->data_buffer->is_valid())) {
      int64_t complete_cnt = limit;
      int64_t complete_len = 0;
      if (OB_FAIL(pre_parse_lines(*handle->data_buffer, box.parser,
                                  box.read_cursor.is_end_file(),
                                  complete_len, complete_cnt))) {
        LOG_WARN("fail to fast_lines_parse", K(ret));
      } else if (OB_FAIL(box.data_trimer.backup_incomplate_data(*handle->data_buffer,
                                                                complete_len))) {
        LOG_WARN("fail to back up data", K(ret));
      } else {
        box.data_trimer.commit_line_cnt(complete_cnt);
        has_valid_data = complete_cnt > 0;
        LOG_DEBUG("LOAD DATA",
            "split offset", box.read_cursor.file_offset_ - box.data_trimer.get_incomplate_data_string().length(),
            K(complete_len), K(complete_cnt),
            "incomplate data length", box.data_trimer.get_incomplate_data_string().length(),
            "incomplate data", box.data_trimer.get_incomplate_data_string());
      }
    }
  } while (OB_SUCC(ret) && !has_valid_data && !box.read_cursor.is_end_file_
           && OB_SUCC(box.data_trimer.expand_buf(ctx.get_allocator())));
  return ret;
}

int ObLoadDataSPImpl::shuffle_task_gen_and_dispatch(ObExecContext &ctx, ToolBox &box)
{
  UNUSED(ctx);
  int ret = OB_SUCCESS;
  ObShuffleTaskHandle *handle = nullptr;
  int64_t task_id = 0;

  for (int64_t i = 0;
       OB_SUCC(ret) && !box.read_cursor.is_end_file() && i < box.data_frag_mem_usage_limit;
       ++i) {

    // wait a buffer from controller
    if (OB_FAIL(box.shuffle_task_controller.on_next_task())) {
      LOG_WARN("fail to get task id", K(ret));
    } else if (OB_FAIL(box.shuffle_task_reserve_queue.pop(handle))) {
      LOG_WARN("fail to pop buffer", K(ret));
    } else if (OB_ISNULL(handle)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("handle is null", K(ret));
    } else if (OB_UNLIKELY(handle->result.flags_.test_bit(ObTaskResFlag::RPC_TIMEOUT))) {
      ret = OB_TRANS_RPC_TIMEOUT;
      LOG_WARN("shuffle task rpc timeout handle", K(ret));
    } else if (OB_FAIL(handle->result.exec_ret_)) {
      LOG_WARN("shuffle task exec failed", K(ret), "task_id", handle->result.task_id_);
    } else if (OB_UNLIKELY(handle->err_records.count() > 0)
               && OB_FAIL(handle_returned_shuffle_task(box, *handle))) {
      LOG_WARN("handle returned shuffle task", K(ret));
    } else {
      box.suffle_rt_sum += handle->result.process_us_;
      task_id = box.shuffle_task_controller.get_next_task_id();
      handle->data_buffer->reset();
      handle->result = ObShuffleResult();
      handle->result.task_id_ = task_id;
      handle->err_records.reuse();

      box.job_status->shuffle_rt_sum_ = box.suffle_rt_sum;
      box.job_status->total_shuffle_task_ = box.shuffle_task_controller.get_total_task_cnt();
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(box.file_buf_row_num.push_back(box.data_trimer.get_lines_count()))) {
        LOG_WARN("fail to push back", K(ret));
      } else if (OB_FAIL(next_file_buffer(ctx, box, handle))) {
        LOG_WARN("fail get next file buffer", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      ObRpcLoadDataShuffleTaskCallBack mycallback(box.shuffle_task_controller,
                                                  box.shuffle_task_reserve_queue,
                                                  handle);

      if (OB_UNLIKELY(handle->data_buffer->get_data_len() <= 0)) {
        ret = mycallback.release_resouce();
      } else {
        ObShuffleTask task;
        task.task_id_ = task_id;
        task.gid_ = box.gid;
        if (OB_FAIL(task.shuffle_task_handle_.set_arg(handle))) {
          LOG_WARN("fail to set arg", K(ret));
        } else {
          if (OB_FAIL(GCTX.load_data_proxy_->to(box.self_addr)
                                             .by(box.tenant_id)
                                             .timeout(box.txn_timeout)
                                             .ap_load_data_shuffle(task, &mycallback))) {
            LOG_WARN("load data proxy post rpc failed", K(ret));
          }
        }
      }
    }

    if (OB_FAIL(ret)) {
      box.shuffle_task_controller.on_task_finished();
    }
  }


  return ret;
}

int ObLoadDataSPImpl::create_log_file(ToolBox &box)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(box.file_appender.open(box.log_file_name, false, true))) {
    LOG_WARN("fail to open file", K(ret), K(box.log_file_name));
  } else if (OB_FAIL(box.file_appender.append(box.load_info.ptr(),
                                              box.load_info.length(),
                                              false))) {
    LOG_WARN("fail to append file", K(ret));
  } else if (OB_FAIL(box.file_appender.append(log_file_column_names,
                                              strlen(log_file_column_names),
                                              false))) {
    LOG_WARN("fail to append file", K(ret));
  }
  return ret;
}

int ObLoadDataSPImpl::log_failed_line(ToolBox &box,
                                      TaskType task_type,
                                      int64_t task_id,
                                      int64_t line_num,
                                      int err_code,
                                      ObString err_msg)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(box.expr_buffer)
      || !box.file_appender.is_opened()) {
    ret = OB_NOT_INIT;
    LOG_WARN("box not init", K(ret));
  } else {
    box.expr_buffer->reset();
    int64_t log_buf_pos = 0;
    //int err_no = ob_errpkt_errno(err_code, box.is_oracle_mode);
    if (err_msg.empty()) {
      err_msg = ob_errpkt_strerror(err_code, box.is_oracle_mode);
    }
    if (OB_FAIL(databuff_printf(box.expr_buffer->begin_ptr(),
                                box.expr_buffer->get_buffer_size(),
                                log_buf_pos,
                                log_file_row_fmt,
                                task_id + 1,
                                line_num + 1,
                                task_type == TaskType::ShuffleTask ? "WARN" : "ERROR",
                                err_code,
                                err_msg.length(),
                                err_msg.ptr()))) {
      LOG_WARN("fail to printf", K(ret), K(err_msg));
    } else if (OB_FAIL(box.file_appender.append(box.expr_buffer->begin_ptr(),
                                                log_buf_pos,
                                                false))) {
      LOG_WARN("fail to append file", K(ret), K(log_buf_pos));
    } else {
      LOG_DEBUG("LOAD DATA log failed rows", K(task_id), K(line_num), K(task_type));
    }

  }
  return ret;
}

int ObLoadDataSPImpl::log_failed_insert_task(ToolBox &box, ObInsertTask &task)
{
  int ret = OB_SUCCESS;
  int log_err = OB_SUCCESS;
  int row_counter = 0;

  if (!box.file_appender.is_opened()
      && OB_FAIL(create_log_file(box))) {
    LOG_WARN("fail to create log file", K(ret));
  } else {
    log_err = task.result_.exec_ret_;
    LOG_DEBUG("check task result", K(task.result_));
  }

  for (int64_t buf_i = 0; OB_SUCC(ret) && buf_i < task.insert_value_data_.count(); ++buf_i) {
    int64_t pos = 0;
    const char* buf = task.insert_value_data_[buf_i].ptr();
    int64_t data_len = task.insert_value_data_[buf_i].length();
    ObDataFrag *frag = NULL;
    int64_t line_num_base = 0;

    if (OB_ISNULL(frag = static_cast<ObDataFrag *>(task.source_frag_[buf_i]))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("source data frag is NULL", K(buf_i), K(ret), K(task));
    } else if (OB_UNLIKELY(OB_INVALID_ID == frag->shuffle_task_id
                           || frag->shuffle_task_id >= box.file_buf_row_num.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("shuffle task id is invalid", K(ret), K(frag->shuffle_task_id));
    } else {
      line_num_base = box.file_buf_row_num.at(frag->shuffle_task_id);
    }
    while (OB_SUCC(ret) && pos < data_len) {
      int64_t row_ser_size = 0;
      int64_t row_num = 0;
      OB_UNIS_DECODE(row_ser_size);
      int64_t pos_back = pos;
      OB_UNIS_DECODE(row_num);
      int64_t line_num = line_num_base + row_num;
      row_counter++;
      if (task.result_.err_line_no_ == row_counter) {
        OZ (log_failed_line(box, TaskType::InsertTask, task.task_id_, line_num, log_err,
                            task.result_.err_msg_));
      }

      pos = pos_back + row_ser_size;
    }

  } //end for
  return ret;
}

int ObLoadDataSPImpl::handle_returned_insert_task(ObExecContext &ctx,
                                                  ToolBox &box,
                                                  ObInsertTask &insert_task,
                                                  bool &need_retry)
{
  int ret = OB_SUCCESS;
  ObPartDataFragMgr *part_mgr = NULL;
  ObLoadServerInfo *server_info = NULL;
  ObInsertResult &result = insert_task.result_;
  enum TASK_STATUS {TASK_SUCC, TASK_NEED_RETRY, TASK_FAILED} task_status = TASK_FAILED;

  if (OB_ISNULL(part_mgr = insert_task.part_mgr)
      || OB_ISNULL(server_info = box.server_infos.at(insert_task.token_server_idx_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid insert task", K(ret), K(insert_task));
  }

  if (OB_SUCC(ret)
      && result.flags_.test_bit(ObTaskResFlag::NEED_WAIT_MINOR_FREEZE)) {
    int64_t last_ts = 0;
    ObAddr &addr = part_mgr->get_leader_addr();
    bool found = (OB_SUCCESS == box.server_last_available_ts.get(addr, last_ts));
    if (insert_task.result_recv_ts_ > last_ts) {
      bool is_leader_changed = false;
      if (OB_FAIL(memory_wait_local(ctx, part_mgr->tablet_id_,
                                    addr, box.wait_secs_for_mem_release,
                                    is_leader_changed))) {
        LOG_WARN("fail to memory_wait_local", K(ret));
      } else {
        int64_t curr_time = ObTimeUtil::current_time();
        if (is_leader_changed) {
          found = (OB_SUCCESS == box.server_last_available_ts.get(addr, last_ts));
        }
        ret = found ? box.server_last_available_ts.update(addr, curr_time)
                    : box.server_last_available_ts.insert(addr, curr_time);
        if (OB_FAIL(ret)) {
          LOG_WARN("failed to update server_last_available_ts",
                   K(ret), K(addr), K(found), K(is_leader_changed));
        }
      }
    }
  }

  bool can_retry = (ObLoadDupActionType::LOAD_REPLACE == box.insert_mode
                    || ObLoadDupActionType::LOAD_IGNORE == box.insert_mode)
                   && insert_task.retry_times_ < ObInsertTask::RETRY_LIMIT;
  if (OB_SUCC(ret)) {
    int err = result.exec_ret_;
    if (OB_LIKELY(OB_SUCCESS == err
                  && !result.flags_.test_bit(ObTaskResFlag::RPC_TIMEOUT))) {
      task_status = TASK_SUCC;
    } else if (result.flags_.test_bit(ObTaskResFlag::RPC_TIMEOUT)) {
      task_status = can_retry ? TASK_NEED_RETRY : TASK_FAILED;
      if (TASK_FAILED == task_status) {
        result.exec_ret_ = OB_TIMEOUT;
      }
    } else if (is_server_down_error(err)
               || is_master_changed_error(err)
               || is_partition_change_error(err)) {
      task_status = can_retry ? TASK_NEED_RETRY : TASK_FAILED;
      if (OB_FAIL(part_mgr->update_part_location(ctx))) {
        LOG_WARN("fail to update location cache", K(ret));
      }
    } else {
      //由于意外错误导致失败，默认
      task_status = TASK_FAILED;
    }
  }

  if (OB_SUCC(ret)) {
    switch (task_status) {
    case TASK_SUCC:
      box.affected_rows += insert_task.row_count_;
      box.insert_rt_sum += insert_task.process_us_;
      /* RESERVE FOR DEBUG
      box.handle_returned_insert_task_count++;
      if (insert_task.row_count_ != DEFAULT_BUFFERRED_ROW_COUNT) {
        LOG_WARN("LOAD DATA task return",
                 "task_id", insert_task.task_id_,
                 "affected_rows", box.affected_rows,
                 "row_count", insert_task.row_count_);
      }
      */
     
      box.job_status->parsed_rows_ = box.affected_rows;
      box.job_status->parsed_bytes_ += insert_task.data_size_;
      box.job_status->total_insert_task_ = box.insert_task_controller.get_total_task_cnt();
      box.job_status->insert_rt_sum_ = box.insert_rt_sum;
      box.job_status->total_wait_secs_ = box.wait_secs_for_mem_release;

      break;
    case TASK_NEED_RETRY:
      insert_task.retry_times_++;
      need_retry = true;
      LOG_WARN("LOAD DATA task need retry",
               "execute server", server_info->addr,
               "task_id", insert_task.task_id_,
               "ret", result.exec_ret_,
               "row_count", insert_task.row_count_);
      break;
    case TASK_FAILED:
      if (OB_SUCCESS != log_failed_insert_task(box, insert_task)) {
        LOG_WARN("fail to log failed insert task");
      }
      LOG_WARN("LOAD DATA task failed",
               "execute server", server_info->addr,
               "task_id", insert_task.task_id_,
               "ret", result.exec_ret_,
               "row_count", insert_task.row_count_);
      ret = result.exec_ret_;
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      break;
    }
  }

  return ret;
}

int ObLoadDataSPImpl::wait_insert_task_return(ObExecContext &ctx, ToolBox &box)
{
  int ret = OB_SUCCESS;
  int ret_bak = OB_SUCCESS;
  for (int64_t returned_cnt = 0; returned_cnt < box.parallel; ++returned_cnt) {
    //ret失败也要循环，保证所有发出的task都返回或超时
    ObInsertTask *insert_task = NULL;
    bool need_retry = false;
    if (OB_FAIL(box.insert_task_controller.on_next_task())) {
      LOG_WARN("fail to get next task id", K(ret));
    } else if (OB_FAIL(box.insert_task_reserve_queue.pop(insert_task))) {
      LOG_WARN("fail to pop", K(ret));
    } else if (OB_ISNULL(insert_task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("insert task is null", K(ret));
    } else if (!insert_task->is_empty_task()
               && OB_FAIL(handle_returned_insert_task(ctx,
                                                      box,
                                                      *insert_task,
                                                      need_retry))) {
      LOG_WARN("fail to handle returned insert task", K(ret));
    } else if (OB_LIKELY(!need_retry)) {
      //do nothing
    } else {
      ObRpcLoadDataInsertTaskCallBack mycallback(box.insert_task_controller,
                                                 box.insert_task_reserve_queue,
                                                 insert_task);
      OZ (ObLoadDataUtils::check_session_status(*ctx.get_my_session()));
      if (OB_SUCC(ret)) {
        if (OB_FAIL(GCTX.load_data_proxy_->to(insert_task->part_mgr->get_leader_addr())
                                           .by(box.tenant_id)
                                           .timeout(box.txn_timeout)
                                           .ap_load_data_insert(*insert_task, &mycallback))) {
          LOG_WARN("load data proxy post rpc failed", K(ret));
        } else {
          --returned_cnt;
        }
      }
    }
    if (OB_FAIL(ret) && OB_SUCCESS == ret_bak) {
      ret_bak = ret;
    }
  }

  if (OB_SUCCESS != ret_bak) {
    ret = ret_bak;
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < box.parallel; ++i) {
    ObInsertTask *insert_task = box.insert_resource[i];
    if (OB_FAIL(box.insert_task_controller.on_task_finished())) {
      LOG_WARN("fail to on task finish", K(ret));
    } else if (OB_FAIL(box.insert_task_reserve_queue.push_back(insert_task))) {
      LOG_WARN("fail to push back", K(ret));
    } else if (OB_ISNULL(insert_task)) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      insert_task->reuse();
    }
  }
  return ret;
}

int ObLoadDataSPImpl::insert_task_send(ObInsertTask *insert_task, ToolBox &box)
{
  int ret = OB_SUCCESS;
  ObRpcLoadDataInsertTaskCallBack mycallback(box.insert_task_controller,
                                             box.insert_task_reserve_queue,
                                             insert_task);
  if (OB_ISNULL(insert_task)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(GCTX.load_data_proxy_->to(insert_task->part_mgr->get_leader_addr())
                                           .by(box.tenant_id)
                                           .timeout(box.txn_timeout)
                                           .ap_load_data_insert(*insert_task, &mycallback))) {
    LOG_WARN("load data proxy post rpc failed", K(ret));
  }
  return ret;
}

int ObLoadDataSPImpl::insert_task_gen_and_dispatch(ObExecContext &ctx, ToolBox &box)
{
  int ret = OB_SUCCESS;

  const int64_t total_server_n = box.server_infos.count();
  int64_t part_iters[total_server_n];
  MEMSET(part_iters, 0, sizeof(part_iters));
  int64_t token_cnt = box.insert_task_controller.get_max_parallelism();

  while (token_cnt > 0) {
    ObInsertTask *insert_task = NULL;
    bool need_retry = false;
    bool task_send_out = false;

    OW (box.insert_task_controller.on_next_task());

    if (OB_SUCC(ret)) {
      if (OB_FAIL(box.insert_task_reserve_queue.pop(insert_task))) {
        LOG_WARN("fail to pop", K(ret));
      } else if (OB_ISNULL(insert_task)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("insert task is null", K(ret));
      } else if (!insert_task->is_empty_task()
                   && OB_FAIL(handle_returned_insert_task(ctx,
                                                          box,
                                                          *insert_task,
                                                          need_retry))) {
        LOG_WARN("fail to handle returned insert task", K(ret));
      } else if (OB_UNLIKELY(need_retry)) {
        //CASE1: for retry old insert task
        LOG_DEBUG("LOAD DATA need retry", KPC(insert_task));
        if (OB_FAIL(insert_task_send(insert_task, box))) {
          LOG_WARN("fail to send insert task", K(ret));
        } else {
          task_send_out = true;
        }
      } else {
        int64_t &part_iter = part_iters[insert_task->token_server_idx_];
        ObLoadServerInfo *server_info = box.server_infos.at(insert_task->token_server_idx_);
        ObPartDataFragMgr *part_datafrag_mgr = nullptr;
        int64_t row_count = box.batch_row_count;
        bool iter_end = true;

        //find next batch data on this server
        for (; part_iter < server_info->part_datafrag_group.count(); ++part_iter) {
          part_datafrag_mgr = server_info->part_datafrag_group.at(part_iter);
          row_count = box.batch_row_count;
          if (part_datafrag_mgr->has_data(row_count)
              || (box.read_cursor.is_end_file()
                  && 0 != (row_count = part_datafrag_mgr->remain_row_count()))) {
            iter_end = false;
            break;
          }
        }

        if (!insert_task->is_empty_task()) {
          insert_task->reuse();
        }

        if (iter_end) {
          //CASE2: all task on this server are done
          task_send_out = false;
          LOG_DEBUG("LOAD DATA all jobs are finish", K(server_info->addr), K(token_cnt));
        } else {
          //CASE3: for new insert task
          insert_task->part_mgr = part_datafrag_mgr;
          insert_task->task_id_ = box.insert_task_controller.get_next_task_id();
          if (OB_FAIL(part_datafrag_mgr->next_insert_task(row_count, *insert_task))) {
            LOG_WARN("fail to generate insert task", K(ret));
          } else {
            box.insert_dispatch_rows += row_count;
            box.insert_task_count++;
            if (row_count != DEFAULT_BUFFERRED_ROW_COUNT) {
              LOG_DEBUG("LOAD DATA task generate",
                        "task_id", insert_task->task_id_,
                        "affected_rows", box.affected_rows,
                        K(row_count));
            }
            if (OB_FAIL(insert_task_send(insert_task, box))) {
              LOG_WARN("fail to send insert task", K(ret));
            } else {
              task_send_out = true;
            }
          }
        }
      }
    }

    if (!task_send_out) {
      token_cnt--;
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < box.insert_task_controller.get_max_parallelism(); ++i) {
    ObInsertTask *insert_task = box.insert_resource[i];
    if (OB_FAIL(box.insert_task_controller.on_task_finished())) {
      LOG_WARN("fail to on task finish", K(ret));
    } else if (OB_FAIL(box.insert_task_reserve_queue.push_back(insert_task))) {
      LOG_WARN("fail to push back", K(ret));
    } else if (OB_ISNULL(insert_task)) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      insert_task->reuse();
    }
  }

  return ret;
}

int ObLoadDataSPImpl::execute(ObExecContext &ctx, ObLoadDataStmt &load_stmt)
{
  int ret = OB_SUCCESS;

  HEAP_VAR(ToolBox, box) {
    //init toolbox
    OZ (box.init(ctx, load_stmt));

    LOG_INFO("LOAD DATA start report"
             , "file_path", load_stmt.get_load_arguments().file_name_
             , "table_name", load_stmt.get_load_arguments().combined_name_
             , "batch_size", box.batch_row_count
             , "parallel", box.parallel
             , "load_mode", box.insert_mode
             , "transaction_timeout", box.txn_timeout
             );

    //ignore rows
    while (OB_SUCC(ret)
           && !box.read_cursor.is_end_file()
           && box.data_trimer.get_lines_count() < box.ignore_rows) {
      OZ (next_file_buffer(ctx, box, box.temp_handle,
                           box.ignore_rows - box.data_trimer.get_lines_count()));
      LOG_DEBUG("LOAD DATA ignore rows", K(box.ignore_rows), K(box.data_trimer.get_lines_count()));
    }

    //main while
    while (OB_SUCC(ret) && !box.read_cursor.is_end_file()) {
      /* 执行分两步并行
       * 1. 并行计算分区 (shuffle_task_gen_and_dispatch)
       * 2. 并行插入 (insert_task_gen_and_dispatch)
       * 每次循环从文件读取 data_frag_mem_usage_limit * MAX_BUFFER_SIZE = 100M 在内存缓存
       */
      OZ (shuffle_task_gen_and_dispatch(ctx, box));
      OW (wait_shuffle_task_return(box));
      OZ (insert_task_gen_and_dispatch(ctx, box));
      //OW (wait_insert_task_return(ctx, box));

      /* 所有异步task都已经返回了，这些task依赖的datafrag可以被释放
       */
      OW (box.data_frag_mgr.free_unused_datafrag());

      /* 检查session是否有效，无效时可直接退出
       */
      OZ (ObLoadDataUtils::check_session_status(*ctx.get_my_session()));
    }

    //release
    OW (box.release_resources());

    if (OB_SUCC(ret) && OB_NOT_NULL(ctx.get_physical_plan_ctx())) {
      ctx.get_physical_plan_ctx()->set_affected_rows(box.affected_rows);
      ctx.get_physical_plan_ctx()->set_row_matched_count(box.data_trimer.get_lines_count());
    }

    if (OB_NOT_NULL(ctx.get_my_session())) {
      ctx.get_my_session()->reset_cur_phy_plan_to_null();
    }

    if (OB_FAIL(ret)) {
      LOG_WARN("LOAD DATA execute failed, ", K(ret));
    }

    if (box.file_appender.is_opened()) {
      LOG_WARN("LOAD DATA error log generated");
    }

    LOG_INFO("LOAD DATA finish report"
             , "total shuffle task", box.shuffle_task_controller.get_total_task_cnt()
             , "total insert task", box.insert_task_controller.get_total_task_cnt()
             , "insert rt sum", box.insert_rt_sum
             , "suffle rt sum", box.suffle_rt_sum
             , "total wait secs", box.wait_secs_for_mem_release
             , "datafrag info", box.data_frag_mgr
             );
  }

  return ret;
}

int ObLoadFileDataTrimer::recover_incomplate_data(ObLoadFileBuffer &buffer)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  if (OB_ISNULL(buf = buffer.begin_ptr())
      || OB_UNLIKELY(buffer.get_buffer_size() < incomplate_data_len_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(buffer.get_buffer_size()));
  } else if (incomplate_data_len_ > 0) {
    MEMCPY(buf, incomplate_data_, incomplate_data_len_);
    buffer.update_pos(incomplate_data_len_);
  }
  return ret;
}

int ObLoadFileDataTrimer::backup_incomplate_data(ObLoadFileBuffer &buffer, int64_t valid_data_len)
{
  int ret = OB_SUCCESS;
  incomplate_data_len_ = buffer.get_data_len() - valid_data_len;
  if (incomplate_data_len_ > incomplate_data_buf_len_) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("size over flow", K(ret), K(incomplate_data_len_), K(incomplate_data_buf_len_));
  } else if (incomplate_data_len_ > 0 && NULL != incomplate_data_) {
    MEMCPY(incomplate_data_, buffer.begin_ptr() + valid_data_len, incomplate_data_len_);
    buffer.update_pos(-incomplate_data_len_);
  }
  return ret;
}

int ObPartDataFragMgr::rowoffset2pos(ObDataFrag *frag, int64_t row_num, int64_t &pos)
{
  int ret = OB_SUCCESS;
  pos = 0;

  if (OB_ISNULL(frag)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    char *buf = frag->data;
    int64_t data_len = frag->frag_pos;
    for (int64_t i = 0; OB_SUCC(ret) && i < row_num; ++i) {
      int64_t row_len = 0;
      OB_UNIS_DECODE(row_len);
      pos+=row_len;
    }
  }

  return ret;
}

int ObPartDataFragMgr::free_frags()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < frag_free_list_.count(); ++i) {
    data_frag_mgr_.distory_datafrag(frag_free_list_[i]);
  }
  frag_free_list_.reuse();
  return ret;
}

int ObPartDataFragMgr::clear()
{
  int ret = OB_SUCCESS;
  ObLink *link = NULL;

  if (!has_data(1)) {
    //do nothing
  } else {
    while (OB_SUCC(ret) && OB_EAGAIN != queue_.pop(link)) {
      data_frag_mgr_.distory_datafrag(static_cast<ObDataFrag *>(link));
    }
  }
  return ret;
}

int ObPartDataFragMgr::next_insert_task(int64_t batch_row_count, ObInsertTask &task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(batch_row_count <= 0)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_UNLIKELY(!has_data(batch_row_count))) {
    ret = OB_EAGAIN; //for now, never reach here
  } else {
    total_row_consumed_ += batch_row_count;
  }

  ObLink *link = NULL;
  ObDataFrag *frag = NULL;
  int64_t row_count = -queue_top_begin_point_.frag_row_pos_;
  InsertTaskSplitPoint new_top_begin_point;

  while (OB_SUCC(ret) && row_count < batch_row_count) {
    new_top_begin_point.reset();
    //handle one frag from head
    while (OB_EAGAIN == queue_.top(link)) { pause(); }
    if (OB_ISNULL(frag = static_cast<ObDataFrag *>(link))) {
      ret = OB_ERR_UNEXPECTED;
    } else if ((row_count += frag->row_cnt) > batch_row_count) {
      //case1 frag has data remained，do not pop
      new_top_begin_point.frag_row_pos_ = frag->row_cnt - (row_count - batch_row_count);
      if (OB_FAIL(rowoffset2pos(frag,
                                new_top_begin_point.frag_row_pos_,
                                new_top_begin_point.frag_data_pos_))) {
        LOG_WARN("fail to rowoffset to pos", K(ret));
      } else if (OB_FAIL(task.insert_value_data_.push_back(
         ObString(new_top_begin_point.frag_data_pos_ - queue_top_begin_point_.frag_data_pos_,
         frag->data + queue_top_begin_point_.frag_data_pos_)))) {
        LOG_WARN("fail to do push back", K(ret));
      } else if (OB_FAIL(task.source_frag_.push_back(frag))) {
        LOG_WARN("fail to push back frag", K(ret));
      }
    } else {
      //case2 frag is empty，need pop
      if (OB_FAIL(queue_.pop(link))) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(frag_free_list_.push_back(frag))) {
        //TODO free frag for failure
        LOG_WARN("fail to push back", K(ret));
      } else {
        if (OB_FAIL(task.insert_value_data_.push_back(
                      ObString(frag->frag_pos - queue_top_begin_point_.frag_data_pos_,
                               frag->data + queue_top_begin_point_.frag_data_pos_)))) {
          LOG_WARN("fail to do push back", K(ret));
        } else if (OB_FAIL(task.source_frag_.push_back(frag))) {
          LOG_WARN("fail to push back frag", K(ret));
        }

        task.data_size_ += frag->orig_data_size;
      }
    }
    queue_top_begin_point_ = new_top_begin_point;
  }

  task.row_count_ = batch_row_count;

  LOG_DEBUG("next_insert_task", K(task));

  return ret;
}

int ObDataFragMgr::free_unused_datafrag()
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids_.count(); ++i) {
    ObTabletID tablet_id = tablet_ids_[i];
    ObPartDataFragMgr *part_data_frag = NULL;

    if (OB_FAIL(get_part_datafrag(tablet_id, part_data_frag))) {
      LOG_WARN("fail to get part datafrag", K(ret), K(tablet_id));
    } else if (OB_ISNULL(part_data_frag)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("part data frag is null", K(ret));
    } else if (OB_FAIL(part_data_frag->free_frags())) {
      LOG_WARN("fail to free frag", K(ret));
    }
  }

  return ret;
}


int ObDataFragMgr::clear_all_datafrag()
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids_.count(); ++i) {
    ObTabletID tablet_id = tablet_ids_[i];
    ObPartDataFragMgr *part_data_frag = NULL;

    if (OB_FAIL(get_part_datafrag(tablet_id, part_data_frag))) {
      LOG_WARN("fail to get part datafrag", K(ret), K(tablet_id));
    } else if (OB_ISNULL(part_data_frag)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("part data frag is null", K(ret));
    } else if (OB_FAIL(part_data_frag->clear())) {
      LOG_WARN("fail to free frag", K(ret));
    } else {
      part_data_frag->~ObPartDataFragMgr();
    }
  }

  return ret;
}

int ObDataFragMgr::init(ObExecContext &ctx, uint64_t table_id)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard *schema_guard = NULL;
  const ObTableSchema *table_schema = NULL;
  ObSEArray<ObObjectID, 4> part_ids;
  tablet_ids_.reset();
  if (OB_ISNULL(ctx.get_sql_ctx())
      || OB_ISNULL(schema_guard = ctx.get_sql_ctx()->schema_guard_)
      || OB_ISNULL(ctx.get_my_session())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sql ctx is null", K(ret), KP(ctx.get_sql_ctx()));
  } else if (OB_FAIL(schema_guard->get_table_schema(
             ctx.get_my_session()->get_effective_tenant_id(),
             table_id, table_schema))) {
    LOG_WARN("fail to get partition count", K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is NULL", K(ret));
  } else if (OB_FAIL(table_schema->get_all_tablet_and_object_ids(tablet_ids_, part_ids))) {
    LOG_WARN("failed to get partition ids", K(ret));
  } else {
    LOG_INFO("table partition ids", K(tablet_ids_));
    total_part_cnt_ = tablet_ids_.count();
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids_.count(); ++i) {
    ObTabletID tablet_id = tablet_ids_[i];
    ObPartDataFragMgr *part_data_frag = NULL;

    if (OB_ISNULL(part_data_frag
                  = OB_NEWx(ObPartDataFragMgr,
                            (&ctx.get_allocator()),
                            *this,
                            ctx.get_my_session()->get_effective_tenant_id(),
                            tablet_id))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else if (FALSE_IT(part_data_frag->tablet_id_ = tablet_id)) {
    } else if (OB_FAIL(part_data_frag->update_part_location(ctx))) {
      LOG_WARN("fail to update part locatition", K(ret));
    } else if (OB_FAIL(part_datafrag_map_.set_refactored(part_data_frag))) {
      LOG_WARN("fail to set hash map", K(ret));
    } else if (OB_FAIL(part_bitset_.add_member(i))) {
      LOG_WARN("fail to add bitset", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    attr_.tenant_id_ = ctx.get_my_session()->get_effective_tenant_id();
    attr_.label_ = common::ObModIds::OB_SQL_LOAD_DATA;
    //attr_.ctx_id_ = common::ObCtxIds::WORK_AREA;
    total_alloc_cnt_ = 0;
    total_free_cnt_ = 0;
  }

  return ret;
}

int ObDataFragMgr::get_part_datafrag(ObTabletID tablet_id,
                                     ObPartDataFragMgr *&part_datafrag_mgr)
{
  return part_datafrag_map_.get_refactored(tablet_id, part_datafrag_mgr);
}

int ObDataFragMgr::create_datafrag(ObDataFrag *&frag, int64_t min_len) {
  int ret = OB_SUCCESS;
  frag = NULL;
  void *buf = NULL;
  int64_t min_alloc_size = sizeof(ObDataFrag) + min_len;
  int64_t opt_alloc_size = 0;

  if (min_alloc_size <= ObDataFrag::DEFAULT_STRUCT_SIZE) {
    opt_alloc_size = ObDataFrag::DEFAULT_STRUCT_SIZE;
  } else if (min_alloc_size >= OB_MALLOC_BIG_BLOCK_SIZE) {
    opt_alloc_size = min_alloc_size;
  } else {
    opt_alloc_size = ObDataFrag::DEFAULT_STRUCT_SIZE
        * ((min_alloc_size - 1) / ObDataFrag::DEFAULT_STRUCT_SIZE + 1);
  }

  if (OB_ISNULL(buf = ob_malloc(opt_alloc_size, attr_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to malloc", K(ret), KP(this));
  } else {
    frag = new(buf) ObDataFrag(opt_alloc_size);
    ATOMIC_AAF(&total_alloc_cnt_, 1);
  }
  return ret;
}

void ObDataFragMgr::distory_datafrag(ObDataFrag *frag) {
  if (OB_ISNULL(frag)) {
    //do nothing
  } else {
    frag->~ObDataFrag();
    ob_free(frag);
    total_free_cnt_++;
  }
}

int ObPartDataFragMgr::update_part_location(ObExecContext &ctx)
{
  int ret = OB_SUCCESS;
  const int64_t retry_us = 200 * 1000;
  const int64_t retry_timeout =
    std::min(ObTimeUtil::current_time() + 30 * USECS_PER_SEC, // the RTO is 30s
             ctx.get_my_session()->get_query_timeout_ts());

  if (OB_UNLIKELY(!tablet_id_.is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("invalid partition key", K(ret));
  } else {
    bool force_renew = false;
    ObDASLocationRouter &loc_router = DAS_CTX(ctx).get_location_router();
    do {
      const int64_t expire_renew_time = force_renew ? INT64_MAX : 0;
      if (OB_FAIL(loc_router.get_leader(tenant_id_, tablet_id_, leader_addr_, expire_renew_time))) {
        if (is_location_service_renew_error(ret) && !force_renew) {
          // retry one time
          force_renew = true;
          LOG_WARN("failed to get location and force renew", K(ret), K(tablet_id_));
        } else {
          LOG_WARN("failed to get location", K(ret), K(tablet_id_));
          if (ObTimeUtil::current_time() + retry_us > retry_timeout) {
            force_renew = false;
          } else {
            ob_usleep(retry_us);
          }
        }
      } else {
        LOG_DEBUG("get participants", K(tablet_id_), K(leader_addr_));
      }
    } while (is_location_service_renew_error(ret) && force_renew);
  }

  return ret;
}

int ObLoadFileDataTrimer::expand_buf(ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;

  int64_t new_buf_len = 0;

  if (NULL == incomplate_data_) {
    new_buf_len = ObLoadFileBuffer::MAX_BUFFER_SIZE;
  } else {
    new_buf_len = incomplate_data_buf_len_ * 2;
  }

  char *new_buf = NULL;
  if (OB_ISNULL(new_buf = static_cast<char*>(allocator.alloc(new_buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("no memory", K(ret));
  } else {
    if (NULL != incomplate_data_) {
      MEMCPY(new_buf, incomplate_data_, incomplate_data_len_);
    }
    incomplate_data_ = new_buf;
    incomplate_data_buf_len_ = new_buf_len;
  }
  return ret;
}

int ObLoadFileDataTrimer::init(ObIAllocator &allocator, const ObCSVFormats &formats)
{
  formats_ = formats;
  return expand_buf(allocator);
}

int ObLoadDataSPImpl::ToolBox::release_resources()
{
  int ret = OB_SUCCESS;

  if (gid.is_valid()) {
    ObLoadDataStat *job_status = nullptr;
    if (OB_FAIL(ObGlobalLoadDataStatMap::getInstance()->unregister_job(gid, job_status))) {
      LOG_ERROR("fail to unregister job", K(ret), K(gid));
    } else if (OB_ISNULL(job_status)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("fail to unregister job", K(ret), K(gid));
    } else {
      int64_t log_print_cnt = 0;
      int64_t ref_cnt = 0;
      while ((ref_cnt = job_status->get_ref_cnt()) > 0) {
        ob_usleep(WAIT_INTERVAL_US); //1s
        if ((log_print_cnt++) % 10 == 0) {
          LOG_WARN("LOAD DATA wait job handle release",
                   K(ret), "wait_seconds", log_print_cnt * 10, K(gid), K(ref_cnt));
        }
      }
      job_status->~ObLoadDataStat();
    }
  }

  //release sessions in shuffle task
  for (int64_t i = 0; i < shuffle_resource.count(); ++i) {
    ObShuffleTaskHandle *handle = NULL;
    int tmp_ret = OB_SUCCESS;

    if (OB_ISNULL(handle = shuffle_resource[i])) {
      tmp_ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("shuffle task handle is null, can not release the memory", K(tmp_ret));
    } else {
      handle->~ObShuffleTaskHandle();
    }

    if (OB_SUCC(ret) && OB_SUCCESS != tmp_ret) {
      ret = tmp_ret;
    }
  }

  for (int64_t i = 0; i < insert_resource.count(); ++i) {
    ObInsertTask *task = NULL;
    int tmp_ret = OB_SUCCESS;

    if (OB_ISNULL(task = insert_resource[i])) {
      tmp_ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("insert task is null, can not release the memory", K(tmp_ret));
    } else {
      task->~ObInsertTask();
    }

    if (OB_SUCC(ret) && OB_SUCCESS != tmp_ret) {
      ret = tmp_ret;
    }
  }

  /*
  for (int64_t i = 0; i < insert_resource.count(); ++i) {
    ObAllocatorSwitch *allocator = NULL;
    int tmp_ret = OB_SUCCESS;

    if (OB_ISNULL(allocator = ctx_allocators[i])) {
      tmp_ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("insert task is null, can not release the memory", K(tmp_ret));
    } else {
      allocator->~ObAllocatorSwitch();
    }

    if (OB_SUCC(ret) && OB_SUCCESS != tmp_ret) {
      ret = tmp_ret;
    }
  }
  */

  int tmp_ret = data_frag_mgr.clear_all_datafrag();
  if (OB_SUCCESS != tmp_ret) {
    LOG_WARN("fail to clear all data frag", K(tmp_ret));
    if (OB_SUCC(ret)) {
      ret = tmp_ret;
    }
  }

  for (int64_t i = 0; i < server_infos.count(); ++i) {
    if (OB_NOT_NULL(server_infos.at(i))) {
      server_infos.at(i)->~ObLoadServerInfo();
    }
  }

  if (OB_NOT_NULL(expr_buffer)) {
    ob_free(expr_buffer);
  }

  //release file reader
  if (OB_NOT_NULL(file_reader)) {
    file_reader->~ObFileReader();
    file_reader = NULL;
  }

  if (OB_NOT_NULL(temp_handle)) {
    temp_handle->~ObShuffleTaskHandle();
  }

  return ret;
}

int ObLoadDataSPImpl::ToolBox::build_calc_partid_expr(ObExecContext &ctx,
                                                      ObLoadDataStmt &load_stmt,
                                                      ObTempExpr *&calc_tablet_id_expr)
{
  int ret = OB_SUCCESS;
  ParamStore paramstore(ObWrapperAllocator(ctx.get_allocator()));
  ObInsertStmt *insert_stmt = nullptr;
  ObSqlString insert_sql;
  ObSEArray<ObString, 16> column_names;
  ObLoadArgument &load_args = load_stmt.get_load_arguments();
  bool need_online_osg = false;

  for (int i = 0; OB_SUCC(ret) && i < insert_infos.count(); ++i) {
    OZ (column_names.push_back(insert_infos.at(i).column_name_));
  }
  OZ (ObLoadDataUtils::check_need_opt_stat_gather(ctx, load_stmt, need_online_osg));
  OZ (ObLoadDataUtils::build_insert_sql_string_head(load_args.dupl_action_,
                                                    load_args.combined_name_,
                                                    column_names,
                                                    insert_sql,
                                                    need_online_osg));
  OZ (insert_sql.append(" VALUES("));
  for (int i = 0; OB_SUCC(ret) && i < insert_infos.count(); ++i) {
    if (i != 0) {
      OZ (insert_sql.append(","));
    }
    OZ (insert_sql.append_fmt("'%d'", i));
  }
  OZ (insert_sql.append(")"));

  OZ (ObLoadDataBase::make_parameterize_stmt(ctx, insert_sql, paramstore, insert_stmt));

  if (OB_SUCC(ret)) {
    ObIArray<ObRawExpr*> &column_convert_exprs = insert_stmt->get_column_conv_exprs();
    ObIArray<ObColumnRefRawExpr*> &column_exprs = insert_stmt->get_insert_table_info().column_exprs_;
    ObRawExpr *part_expr = nullptr;
    ObRawExpr *subpart_expr = nullptr;
    ObRawExpr *calc_partid_expr = NULL;
    ObTempExpr *temp_expr = nullptr;
    TableItem *table_item = nullptr;
    RowDesc row_desc;
    ObSEArray<ObRawExpr *, 16> insert_columns;
    ObSEArray<ObRawExpr *, 16> value_mock_columns;
    ObSEArray<ObRawExpr *, 16> field_exprs;
    ObSEArray<ObRawExpr *, 16> insert_exprs;

    if (insert_stmt->get_table_items().count() != 1
        || OB_ISNULL(table_item = insert_stmt->get_table_items().at(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected table items", K(ret));
    } else {
      if (schema::PARTITION_LEVEL_ZERO != load_args.part_level_) {
        part_expr = insert_stmt->get_part_expr(table_item->table_id_, table_item->ref_id_);
        if (schema::PARTITION_LEVEL_ONE != load_args.part_level_) {
          subpart_expr = insert_stmt->get_subpart_expr(table_item->table_id_, table_item->ref_id_);
        }
      }
    }

    for (int i = 0; OB_SUCC(ret) && i < num_of_file_column; i++) {
      ObColumnRefRawExpr *field_expr = nullptr;
      if (OB_FAIL(ctx.get_expr_factory()->create_raw_expr(T_REF_COLUMN, field_expr))) {
        LOG_WARN("create column ref raw expr failed", K(ret));
      } else if (OB_ISNULL(field_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN(("field_expr is null"));
      } else {
        field_expr->set_data_type(ObVarcharType);
        field_expr->set_collation_type(load_args.file_cs_type_);
        field_expr->set_column_attr("__field", ObCharsetUtils::get_const_str(CS_TYPE_UTF8MB4_BIN, '0' + i));
        if (OB_FAIL(field_expr->add_flag(IS_COLUMN))) {
          LOG_WARN("failed to add flag IS_COLUMN", K(ret));
        } else if (OB_FAIL(field_exprs.push_back(field_expr))) {
          LOG_WARN("failed to push back", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      ObRawExprCopier copier(*ctx.get_expr_factory());
      ReplaceVariables replacer(ctx, load_stmt, field_exprs);
      for (int i = 0; OB_SUCC(ret) && i < insert_infos.count(); ++i) {
        ObRawExpr *insert_expr = nullptr;
        ObLoadTableColumnDesc &desc = insert_infos.at(i);
        if (OB_NOT_NULL(desc.expr_value_)) {
          OZ (copier.copy_on_replace(desc.expr_value_, insert_expr, &replacer));
        } else {
          insert_expr = field_exprs.at(desc.array_ref_idx_);
        }
        OZ (insert_exprs.push_back(insert_expr));
        LOG_DEBUG("push final insert expr", KPC(insert_expr));
      }
    }

    OZ (row_desc.init());

    for (int i = 0; OB_SUCC(ret) && i < field_exprs.count(); i++) {
      if (OB_FAIL(row_desc.add_column(field_exprs.at(i)))) {
        LOG_WARN("fail to add column", K(ret));
      }
    }

    for (int i = 0; OB_SUCC(ret) && i < insert_stmt->get_values_desc().count(); i++) {
      OZ (value_mock_columns.push_back(insert_stmt->get_values_desc().at(i)));
    }

    for (int i = 0; OB_SUCC(ret) && i < column_exprs.count(); i++) {
      OZ (insert_columns.push_back(column_exprs.at(i)));
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObRawExprUtils::build_calc_tablet_id_expr(*ctx.get_expr_factory(),
                                                          *ctx.get_my_session(),
                                                          load_args.table_id_,
                                                          load_args.part_level_,
                                                          part_expr,
                                                          subpart_expr,
                                                          calc_partid_expr))) {
        LOG_WARN("fail to build table location expr", K(ret));
      } else if (OB_FAIL(ObTransformUtils::replace_exprs(value_mock_columns,
                                                         insert_exprs,
                                                         column_convert_exprs))) {
        LOG_WARN("fail to replace exprs", K(ret));
      } else if (OB_FAIL(ObTransformUtils::replace_expr(insert_columns,
                                                        column_convert_exprs,
                                                        calc_partid_expr))) {
        LOG_WARN("fail to replace exprs", K(ret));
      } else if (OB_FAIL(calc_partid_expr->formalize(ctx.get_my_session()))) {
        LOG_WARN("fail to formalize expr", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_ISNULL(ctx.get_sql_ctx())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sql ctx is null", K(ret));
      } else if (OB_FAIL(ObStaticEngineExprCG::gen_expr_with_row_desc(calc_partid_expr,
                                                               row_desc,
                                                               ctx.get_allocator(),
                                                               ctx.get_my_session(),
                                                               ctx.get_sql_ctx()->schema_guard_,
                                                               temp_expr))) {
        LOG_WARN("fail to gen temp expr", K(ret));
      } else {
        calc_tablet_id_expr = temp_expr;
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_ISNULL(ctx.get_physical_plan_ctx())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("plan ctx is null", K(ret));
      } else {
        ctx.get_physical_plan_ctx()->set_autoinc_params(insert_stmt->get_autoinc_params());
      }
    }

    if (OB_SUCC(ret)) {
      bool part_key_has_autoinc = false;
      OZ (insert_stmt->part_key_has_auto_inc(part_key_has_autoinc));
      if (part_key_has_autoinc) {
        calc_tablet_id_expr = NULL;
      }
    }


    LOG_DEBUG("LOAD DATA check insert info",
              K(column_convert_exprs), K(column_exprs), KPC(calc_partid_expr),
              KPC(part_expr), KPC(subpart_expr),
              K(insert_stmt->get_values_vector()),
              K(insert_stmt->get_values_desc()));
  }

  return ret;
}

int ObLoadDataSPImpl::ToolBox::init(ObExecContext &ctx, ObLoadDataStmt &load_stmt)
{
  int ret = OB_SUCCESS;
  const ObLoadArgument &load_args = load_stmt.get_load_arguments();
  const ObDataInFileStruct &file_formats = load_stmt.get_data_struct_in_file();
  const ObLoadDataHint &hint = load_stmt.get_hints();
  ObIODOpt opt;
  ObIODOpts iod_opts;
  ObBackupIoAdapter util;
  bool need_online_osg = false;

  iod_opts.opts_ = &opt;
  iod_opts.opt_cnt_ = 0;

  formats.init(file_formats);
  self_addr = ctx.get_task_executor_ctx()->get_self_addr();
  //batch_row_count = DEFAULT_BUFFERRED_ROW_COUNT;
  data_frag_mem_usage_limit = 50; //50*2M = 100M
  is_oracle_mode = lib::is_oracle_mode();
  tenant_id = load_args.tenant_id_;
  wait_secs_for_mem_release = 0;
  affected_rows = 0;
  insert_rt_sum = 0;
  suffle_rt_sum = 0;
  insert_dispatch_rows = 0;
  insert_task_count = 0;
  handle_returned_insert_task_count = 0;
  insert_mode = load_args.dupl_action_;
  load_file_storage = load_args.load_file_storage_;
  ignore_rows = load_args.ignore_rows_;
  last_session_check_ts = 0;

  ObSQLSessionInfo *session = NULL;
  ObTempExpr *calc_tablet_id_expr = nullptr;

  if (OB_ISNULL(session = ctx.get_my_session()) ||
      OB_ISNULL(ctx.get_sql_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (OB_FAIL(data_trimer.init(ctx.get_allocator(), formats))) {
    LOG_WARN("fail to init data_trimer", K(ret));
  } else if (OB_FAIL(gen_load_table_column_desc(ctx, load_stmt, insert_infos))) {
    LOG_WARN("fail to build load table column desc", K(ret));
  } else if (OB_FAIL(ObLoadDataUtils::check_need_opt_stat_gather(ctx, load_stmt, need_online_osg))) {
    LOG_WARN("fail to check need online stats gather", K(ret));
  } else if (OB_FAIL(gen_insert_columns_names_buff(ctx, load_args,
                                                   insert_infos,
                                                   insert_stmt_head_buff,
                                                   need_online_osg))) {
    LOG_WARN("fail to gen insert column names buff", K(ret));
  } else if (OB_FAIL(data_frag_mgr.init(ctx, load_args.table_id_))) {
    LOG_WARN("fail to init data frag mgr", K(ret));
  }

  //init server_info_map
  if (OB_SUCC(ret)) {
    if (OB_FAIL(server_info_map.init("serverinfomap", MAX_SERVER_COUNT))) {
      LOG_WARN("fail to init server info map", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < data_frag_mgr.get_tablet_ids().count(); ++i) {
    ObTabletID tablet_id = data_frag_mgr.get_tablet_ids().at(i);
    ObPartDataFragMgr *part_frag_mgr = nullptr;
    if (OB_FAIL(data_frag_mgr.get_part_datafrag(tablet_id, part_frag_mgr))) {
      LOG_WARN("fail to get part data frag", K(ret), K(tablet_id));
    } else if (OB_UNLIKELY(!part_frag_mgr->get_leader_addr().is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("part leader addr is not valid", K(ret), K(tablet_id));
    } else {
      ObLoadServerInfo *server_info = nullptr;
      if (OB_SUCCESS != server_info_map.get(part_frag_mgr->get_leader_addr(), server_info)) {
        //no find, create one
        if (OB_ISNULL(server_info = OB_NEWx(ObLoadServerInfo, (&ctx.get_allocator())))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("Failed to alloc", K(ret));
        } else if (OB_FAIL(server_info_map.insert(part_frag_mgr->get_leader_addr(), server_info))) {
          LOG_WARN("fail to insert hash map", K(ret));
        } else {
          server_info->addr = part_frag_mgr->get_leader_addr();
        }
      } else {
        if (OB_FAIL(server_info_map.get(part_frag_mgr->get_leader_addr(), server_info))) {
          LOG_WARN("fail to get server info", K(ret));
        }
      }
      //save part index to server info
      if (OB_SUCC(ret)) {
        if (OB_FAIL(server_info->part_datafrag_group.push_back(part_frag_mgr))) {
          LOG_WARN("fail to add member", K(ret));
        }
      }
    }
  }

  //init server_info
  if (OB_SUCC(ret)) {
    auto push_to_array = [&] (const ObAddr &key, ObLoadServerInfo *value) -> bool {
      UNUSED(key);
      return OB_SUCC(server_infos.push_back(value));
    };
    if (OB_FAIL(server_infos.reserve(server_info_map.size()))) {
      LOG_WARN("fail to pre allocate", K(ret));
    } else if (OB_FAIL(server_info_map.for_each(push_to_array))) {
      LOG_WARN("fail to for each", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(session->get_tx_timeout(txn_timeout))) {
      LOG_WARN("fail to get transaction timeout", K(ret));
    } else {
      txn_timeout = std::max(txn_timeout, RPC_BATCH_INSERT_TIMEOUT_US);
      txn_timeout = std::min(txn_timeout, MIN_TO_USEC(10));
    }
  }

  if (OB_SUCC(ret)) {
    file_read_param.file_location_   = load_file_storage;
    file_read_param.filename_        = load_args.file_name_;
    file_read_param.access_info_     = load_args.access_info_;
    file_read_param.packet_handle_   = &ctx.get_my_session()->get_pl_query_sender()->get_packet_sender();
    file_read_param.session_         = ctx.get_my_session();
    file_read_param.timeout_ts_      = THIS_WORKER.get_timeout_ts();

    if (OB_UNLIKELY(ObLoadFileLocation::OSS == load_file_storage &&
                    ObLoadDataFormat::CSV != load_args.access_info_.get_load_data_format())) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("load data format not support", KR(ret), K(load_args.access_info_));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "non-csv format in load data is");
    } else if (OB_FAIL(ObFileReader::open(file_read_param, ctx.get_allocator(), file_reader))) {
      LOG_WARN("failed to open file.", KR(ret), K(file_read_param), K(load_args.file_name_));

    } else if (!file_reader->seekable()) {
      file_size = -1;
    } else if (OB_FAIL(file_reader->get_file_size(file_size))) {
      LOG_WARN("fail to get io device file size", KR(ret), K(file_size));
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < insert_infos.count(); ++i) {
    const ObLoadTableColumnDesc &desc = insert_infos.at(i);
    if (!desc.is_set_values_ && (ob_is_string_tc(desc.column_type_) || ob_is_enumset_tc(desc.column_type_))) {
      if (OB_FAIL(string_type_column_bitset.add_member(i))) {
        LOG_WARN("fail to add bitset", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    void *buf = NULL;
    num_of_file_column = load_stmt.get_field_or_var_list().count();
    num_of_table_column = insert_infos.count();
    if (OB_FAIL(insert_values.prepare_allocate(num_of_table_column))) {
      LOG_WARN("fail to reserve array", K(ret));
    } else if (OB_FAIL(field_values_in_file.prepare_allocate(num_of_file_column))) {
      LOG_WARN("fail to reserve array", K(ret));
    } else if (OB_ISNULL(buf = ob_malloc(ObLoadFileBuffer::MAX_BUFFER_SIZE,
                                         ObMemAttr(tenant_id, ObModIds::OB_SQL_LOAD_DATA)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else if (FALSE_IT(expr_buffer = new(buf) ObLoadFileBuffer(
                          ObLoadFileBuffer::MAX_BUFFER_SIZE - sizeof(ObLoadFileBuffer)))) {
    } else if (OB_FAIL(generator.init(*session, expr_buffer, ctx.get_sql_ctx()->schema_guard_))) {
      LOG_WARN("fail to init generator", K(ret));
    } else if (OB_FAIL(generator.set_params(insert_stmt_head_buff, load_args.file_cs_type_,
                                            session->get_sql_mode()))) {
      LOG_WARN("fail to set params", K(ret));
    } else if (OB_FAIL(copy_exprs_for_shuffle_task(ctx, load_stmt, insert_infos,
                                                   generator.get_field_exprs(),
                                                   generator.get_insert_exprs()))) {
      LOG_WARN("fail to copy exprs", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    plan.set_vars(ctx.get_stmt_factory()->get_query_ctx()->variables_);
    ctx.get_my_session()->set_cur_phy_plan(&plan);
    OX(ctx.reference_my_plan(&plan));
    OZ(ctx.init_phy_op(1));


    if (OB_SUCC(ret) && load_args.part_level_ != PARTITION_LEVEL_ZERO) {
      if (OB_FAIL(build_calc_partid_expr(ctx, load_stmt, calc_tablet_id_expr))) {
        LOG_WARN("fail to build expr", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      char *buf = NULL;
      int64_t size = ctx.get_serialize_size();
      int64_t pos = 0;
      if (OB_ISNULL(buf = static_cast<char *>(ctx.get_allocator().alloc(size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory", K(ret), K(size));
      } else if (OB_FAIL(ctx.serialize(buf, size, pos))) {
        LOG_WARN("fail to serialize ctx", K(ret), K(size), K(pos));
      } else {
        exec_ctx_serialized_data = ObString(size, buf);
      }
    }
  }

  if (OB_SUCC(ret)) {
    double min_cpu;
    double max_cpu;
    if (OB_ISNULL(GCTX.omt_)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(GCTX.omt_->get_tenant_cpu(load_args.tenant_id_, min_cpu, max_cpu))) {
      LOG_WARN("fail to get tenant cpu", K(ret));
    } else {
      max_cpus = std::max(1L, lround(min_cpu));
    }
  }

  if (OB_SUCC(ret)) {
    int64_t hint_parallel = 0;
    if (OB_FAIL(hint.get_value(ObLoadDataHint::PARALLEL_THREADS, hint_parallel))) {
      LOG_WARN("fail to get value", K(ret));
    } else {
      LOG_DEBUG("parallel calc", K(hint_parallel), K(max_cpus));
      parallel = hint_parallel > 0 ? hint_parallel : DEFAULT_PARALLEL_THREAD_COUNT;
      //parallel = std::min(parallel, max_cpus);
    }
  }

  if (OB_SUCC(ret)) {
    int64_t hint_batch_size = 0;
    int64_t hint_max_batch_buffer_size = 0;
    ObString hint_batch_buffer_size_str;
    if (OB_FAIL(hint.get_value(ObLoadDataHint::BATCH_SIZE, hint_batch_size))) {
      LOG_WARN("fail to get value", K(ret));
    } else if (0 == hint_batch_size) {
      batch_row_count = DEFAULT_BUFFERRED_ROW_COUNT;
    } else {
      batch_row_count = std::max(1L, std::min(DEFAULT_BUFFERRED_ROW_COUNT, hint_batch_size));
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(hint.get_value(ObLoadDataHint::BATCH_BUFFER_SIZE, hint_batch_buffer_size_str))) {
        LOG_WARN("fail to get value", K(ret));
      } else {
        bool is_valid = false;
        hint_batch_buffer_size_str = hint_batch_buffer_size_str.trim();
        if (!hint_batch_buffer_size_str.empty()) {
          hint_max_batch_buffer_size = ObConfigCapacityParser::get(to_cstring(hint_batch_buffer_size_str), is_valid);
        }
        if (!is_valid) {
          hint_max_batch_buffer_size = 1L << 30; // 1G
        }
        batch_buffer_size = MAX(ObLoadFileBuffer::MAX_BUFFER_SIZE, hint_max_batch_buffer_size);
      }
    }
    LOG_DEBUG("batch size", K(hint_batch_size), K(batch_row_count), K(batch_buffer_size));
  }

  if (OB_SUCC(ret)) {
    int64_t query_timeout = 0;
    if (OB_FAIL(hint.get_value(ObLoadDataHint::QUERY_TIMEOUT, query_timeout))) {
      LOG_WARN("fail to get value", K(ret));
    } else if (0 == query_timeout) {
      if (OB_FAIL(ctx.get_my_session()->get_query_timeout(query_timeout))) {
        LOG_WARN("fail to get query timeout", KR(ret));
      } else {
        query_timeout = MAX(query_timeout, RPC_BATCH_INSERT_TIMEOUT_US);
        THIS_WORKER.set_timeout_ts(ctx.get_my_session()->get_query_start_time() + query_timeout);
      }
    } else if (query_timeout > 0) {
      THIS_WORKER.set_timeout_ts(ctx.get_my_session()->get_query_start_time() + query_timeout);
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(parser.init(file_formats, num_of_file_column, load_args.file_cs_type_))) {
      LOG_WARN("fail to init parser", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(shuffle_task_controller.init(parallel))) {
      LOG_WARN("fail to init shuffle task controller", K(ret));
    } else if (OB_FAIL(shuffle_task_reserve_queue.init(parallel + 1))) {
      LOG_WARN("fail to init shuffle_task_reserve_queue", K(ret));
    } else if (OB_FAIL(insert_task_controller.init(parallel * server_infos.count()))) {
      LOG_WARN("fail to init insert task controller", K(ret));
    } else if (OB_FAIL(insert_task_reserve_queue.init(parallel * server_infos.count() + 1))) {
      LOG_WARN("fail to init insert_task_reserve_queue", K(ret));
    } else if (OB_FAIL(ctx_allocators.reserve(parallel))) {
      LOG_WARN("fail to pre alloc allocators", K(ret));
    }
/*
    for (int i = 0; OB_SUCC(ret) && i <parallel; ++i) {
      ObAllocatorSwitch *allocator = NULL;
      if (OB_ISNULL(allocator = OB_NEWx(ObAllocatorSwitch, (&ctx.get_allocator())))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Failed to alloc", K(ret));
      } else if (OB_FAIL(ctx_allocators.push_back(allocator))) {
        allocator->~ObAllocatorSwitch();
        LOG_WARN("fail to push back", K(ret));
      }
    }
*/



    if (OB_SUCC(ret)) {
      if (OB_ISNULL(temp_handle = OB_NEWx(ObShuffleTaskHandle, (&ctx.get_allocator()),
                                          data_frag_mgr, string_type_column_bitset, tenant_id))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Failed to alloc", K(ret));
      } else if (OB_FAIL(temp_handle->expand_buf(batch_buffer_size, ObLoadFileBuffer::MAX_BUFFER_SIZE))) {
        LOG_WARN("fail to expand buf", K(ret));
      }
    }

    for (int i = 0; OB_SUCC(ret) && i < shuffle_task_controller.get_max_parallelism(); ++i) {
      ObShuffleTaskHandle *handle = nullptr;
      int64_t pos = 0;

      if (OB_ISNULL(handle = OB_NEWx(ObShuffleTaskHandle, (&ctx.get_allocator()),
                                     data_frag_mgr, string_type_column_bitset, tenant_id))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Failed to alloc", K(ret));
      } else {
        if (OB_FAIL(handle->expand_buf(batch_buffer_size, ObLoadFileBuffer::MAX_BUFFER_SIZE))) {
          LOG_WARN("fail to expand buf", K(ret));
        } else if (OB_FAIL(handle->exec_ctx.deserialize(exec_ctx_serialized_data.ptr(),
                                                        exec_ctx_serialized_data.length(), pos))) {
          LOG_WARN("fail to deserialize", K(ret));
        } else if (OB_FAIL(handle->parser.init(file_formats, num_of_file_column, load_args.file_cs_type_))) {
          LOG_WARN("fail to init parser", K(ret));
        } else if (OB_FAIL(handle->generator.set_params(insert_stmt_head_buff, load_args.file_cs_type_, session->get_sql_mode()))) {
          LOG_WARN("fail to set params", K(ret));
        } else if (OB_FAIL(copy_exprs_for_shuffle_task(ctx, load_stmt, insert_infos,
                                                       handle->generator.get_field_exprs(),
                                                       handle->generator.get_insert_exprs()))) {
          LOG_WARN("fail to copy exprs", K(ret));
        } else if (OB_FAIL(shuffle_task_reserve_queue.push_back(handle))) {
          LOG_WARN("fail to push back", K(ret));
        }
        if (OB_SUCC(ret)) {
          handle->calc_tablet_id_expr = calc_tablet_id_expr;
          ObObj *obj_array = nullptr;
          if (OB_ISNULL(obj_array = static_cast<ObObj*>(
                          handle->allocator.alloc(sizeof(ObObj) * num_of_file_column)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to allocate memory", K(ret));
          } else {
            for (ObObj *ptr = obj_array; ptr < obj_array + num_of_file_column; ++ptr) {
              new(ptr)ObObj();
              ptr->set_type(ObVarcharType);
              ptr->set_collation_type(load_args.file_cs_type_);
            }
            handle->row_in_file.assign(obj_array, num_of_file_column);
          }
        }

        if (OB_FAIL(ret) || OB_FAIL(shuffle_resource.push_back(handle))) {
          handle->~ObShuffleTaskHandle();
          LOG_WARN("init shuffle handle failed", K(ret));
        }
      }
    }

    for (int i = 0; OB_SUCC(ret) && i < insert_task_controller.get_max_parallelism(); ++i) {
      int64_t server_j = i % server_infos.count();
      ObInsertTask *insert_task = nullptr;
      if (OB_ISNULL(insert_task = OB_NEWx(ObInsertTask, (&ctx.get_allocator())))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Failed to alloc", K(ret));
      } else if (OB_FAIL(insert_task->timezone_.deep_copy(ctx.get_my_session()->get_tz_info_wrap()))) {
        LOG_WARN("fail to copy timezone", K(ret));
      } else {
        //insert的column name都是一样的，所有的task共用一块儿buf做序列化就可以了
        insert_task->insert_stmt_head_ = insert_stmt_head_buff;
        insert_task->column_count_ = insert_infos.count();
        insert_task->row_count_ = batch_row_count;
        insert_task->tenant_id_ = ctx.get_my_session()->get_effective_tenant_id();
        insert_task->token_server_idx_ = server_j;
        insert_task->sql_mode_ = ctx.get_my_session()->get_sql_mode();
        if (OB_FAIL(insert_resource.push_back(insert_task))) {
          insert_task->~ObInsertTask();
          LOG_WARN("fail to push back", K(ret));
        } else if (OB_FAIL(insert_task_reserve_queue.push_back(insert_task))) {
          LOG_WARN("fail to push back", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(server_last_available_ts.init(ObMemAttr(tenant_id, ObModIds::OB_SQL_LOAD_DATA), MAX_SERVER_COUNT))) {
        LOG_WARN("fail to create server map", K(ret));
      }
    }
  }

  constexpr const char* dict = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
  constexpr int word_base = 62; //length of dict
  const int64_t file_id_len = 6;
  int64_t cur_ts = ObTimeUtil::current_time();


  if (OB_SUCC(ret)) {
    char *buf = NULL;
    static const char* loadlog_str = "log/obloaddata.log.";
    int64_t pre_len = strlen(loadlog_str);
    int64_t buf_len = file_id_len + pre_len;
    int64_t pos = 0;

    if (OB_ISNULL(buf = static_cast<char*>(ctx.get_allocator().alloc(buf_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("no memory", K(ret), K(buf_len));
    } else {
      MEMCPY(buf + pos, loadlog_str, pre_len);
      pos += pre_len;
      uint32_t hash_ts = ::murmurhash2(&cur_ts, sizeof(cur_ts), 0);
      for (int i = 0; i < file_id_len && pos < buf_len; ++i) {
        buf[pos++] = dict[hash_ts % word_base];
        hash_ts /= word_base;
      }
    }
    if (OB_SUCC(ret)) {
      log_file_name = ObString(pos, buf);
    }
  }

  if (OB_SUCC(ret)) {
    const int64_t fake_file_size = (file_size > 0) ? file_size : (2 << 30); // use 2G as default in load local mode
    int64_t max_task_count = (fake_file_size / ObLoadFileBuffer::MAX_BUFFER_SIZE + 1) * 2;
    file_buf_row_num.set_attr(ObMemAttr(tenant_id, ObModIds::OB_SQL_LOAD_DATA));
    if (OB_FAIL(file_buf_row_num.reserve(max_task_count))) {
      LOG_WARN("fail to reserve", K(ret));
    }
  }

  int64_t buf_len = DEFAULT_BUF_LENGTH;
  bool need_extend = true;
  while (OB_SUCC(ret) && need_extend) {
    char *buf = NULL;
    int64_t pos = 0;
    buf_len *= 2;
    if (OB_ISNULL(buf = static_cast<char*>(ctx.get_allocator().alloc(buf_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("no memory", K(ret), K(buf_len));
    } else {
      const ObString &cur_query_str = ctx.get_my_session()->get_current_query_string();
      OZ (databuff_printf(buf, buf_len, pos,
                          "Tenant name:\t%.*s\n"
                          "File name:\t%.*s\n"
                          "Into table:\t%.*s\n"
                          "Parallel:\t%ld\n"
                          "Batch size:\t%ld\n"
                          "SQL trace:\t%s\n",
                          session->get_tenant_name().length(), session->get_tenant_name().ptr(),
                          load_args.file_name_.length(), load_args.file_name_.ptr(),
                          load_args.combined_name_.length(), load_args.combined_name_.ptr(),
                          parallel,
                          batch_row_count,
                          ObCurTraceId::get_trace_id_str()
                          ));
      OZ (databuff_printf(buf, buf_len, pos, "Start time:\t"));
      OZ (ObTimeConverter::datetime_to_str(cur_ts,
                                           TZ_INFO(session),
                                           ObString(),
                                           MAX_SCALE_FOR_TEMPORAL,
                                           buf, buf_len, pos, true));
      OZ (databuff_printf(buf, buf_len, pos, "\n"));
      OZ (databuff_printf(buf, buf_len, pos, "Load query: \n%.*s\n",
                                cur_query_str.length(), cur_query_str.ptr()));
      OX (load_info.assign_ptr(buf, pos));
    }
    if (OB_SUCC(ret)) {
      need_extend = false;
    } else {
      if (OB_SIZE_OVERFLOW == ret) {
        ret = OB_SUCCESS;
        need_extend = true;
      }
    }
  }

  if (OB_SUCC(ret)) {
    job_status = nullptr;
    if (OB_ISNULL(job_status = OB_NEWx(ObLoadDataStat, (&ctx.get_allocator())))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", K(ret));
    } else {
      ObLoadDataGID temp_gid;
      ObLoadDataGID::generate_new_id(temp_gid);
      job_status->tenant_id_ = tenant_id;
      job_status->job_id_ = temp_gid.id;
      job_status->allocator_.set_tenant_id(tenant_id);
      OZ(ob_write_string(job_status->allocator_,
                         load_args.combined_name_, job_status->table_name_));
      OZ(ob_write_string(job_status->allocator_,
                         load_args.file_name_, job_status->file_path_));
      job_status->file_column_ = num_of_file_column;
      job_status->table_column_ = num_of_table_column;
      job_status->batch_size_ = batch_row_count;
      job_status->parallel_ = parallel;
      job_status->load_mode_ = static_cast<int64_t>(insert_mode);
      job_status->start_time_ = common::ObTimeUtility::current_time();
      job_status->total_bytes_ = file_size;
      if (OB_FAIL(ObGlobalLoadDataStatMap::getInstance()->register_job(temp_gid, job_status))) {
        LOG_WARN("fail to register job", K(ret));
      } else {
        gid = temp_gid;
      }
    }
  }

  return ret;
}

} // sql
} // oceanbase
