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

//#define TEST_MODE

#include "sql/engine/cmd/ob_load_data_impl.h"

#include <math.h>
#include "observer/omt/ob_multi_tenant.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_sql_string.h"
#include "storage/ob_partition_service.h"
#include "storage/ob_dml_param.h"
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
#include "share/ob_tenant_mgr.h"
#include "share/ob_tenant_memstore_info_operator.h"
#include "sql/resolver/ob_schema_checker.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::storage;

namespace oceanbase {
namespace sql {

#ifdef TEST_MODE
static const int64_t INSERT_TASK_DROP_RATE = 1;
static void delay_process_by_probability(int64_t percentage)
{
  if (OB_UNLIKELY(ObRandom::rand(1, 100) <= percentage)) {
    usleep(RPC_BATCH_INSERT_TIMEOUT_US);
  }
}
#endif

#define OW(statement)                                    \
  do {                                                   \
    int inner_ret = statement;                           \
    if (OB_UNLIKELY(OB_SUCCESS != inner_ret)) {          \
      LOG_WARN("fail to exec" #statement, K(inner_ret)); \
      if (OB_SUCC(ret)) {                                \
        ret = inner_ret;                                 \
      }                                                  \
    }                                                    \
  } while (0)

const char* ObLoadDataImpl::ZERO_FIELD_STRING = "0";
const char* ObLoadDataBase::SERVER_TENANT_MEMORY_EXAMINE_SQL =
    "SELECT case when total_memstore_used < major_freeze_trigger * 1.02 then false else true end"
    " as need_wait_freeze"
    " FROM oceanbase.__all_virtual_tenant_memstore_info WHERE tenant_id = %ld"
    " and svr_ip = '%d.%d.%d.%d' and svr_port = %d";

const char* log_file_column_names = "\nRow\tErrCode\tErrMsg\t\n";
const char* log_file_row_fmt = "%ld\t%d\t%s\t\n";
static const int64_t WAIT_INTERVAL_US = 1 * 1000 * 1000;  // 1s

/*
 * cat field strs into insert values
 * source: expr_strs_, parsed_field_strs_
 * target: insert_values_per_line_
 * according to: valid_insert_column_info_store_
 */
int ObLoadDataImpl::collect_insert_row_strings()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < insert_column_number_; ++i) {
    const ObLoadTableColumnDesc& value_info = valid_insert_column_info_store_.at(i);
    ObString& target = insert_values_per_line_.at(i);
    if (value_info.is_set_values_) {  // set expr
      if (OB_LIKELY(value_info.array_ref_idx_ < set_assigned_column_number_)) {
        target = expr_strs_.at(value_info.array_ref_idx_);
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid array index", K(ret), K(value_info.array_ref_idx_), K(expr_strs_.count()));
      }
    } else {
      if (OB_LIKELY(value_info.array_ref_idx_ < file_column_number_)) {
        target = parsed_field_strs_.at(value_info.array_ref_idx_);
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid array index", K(ret), K(value_info.array_ref_idx_), K(parsed_field_strs_.count()));
      }
    }
  }
  return ret;
}

bool ObLoadDataImpl::find_insert_column_info(const uint64_t target_column_id, int64_t& found_idx)
{
  int ret_bool = false;
  for (int64_t i = 0; i < valid_insert_column_info_store_.count(); ++i) {
    if (valid_insert_column_info_store_.at(i).column_id_ == target_column_id) {
      found_idx = i;
      ret_bool = true;
      break;
    }
  }
  return ret_bool;
}

int ObLoadDataImpl::recursively_replace_varables(
    ObRawExpr*& raw_expr, ObIAllocator& allocator, const ObSQLSessionInfo& session_info)
{
  int ret = OB_SUCCESS;
  bool is_user_variable = false;
  bool is_stack_overflow = false;
  if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("stack overflow, maybe too deep recursive", K(ret));
  } else if (OB_ISNULL(raw_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K((ret)));
  } else if (raw_expr->get_expr_type() == T_REF_COLUMN ||
             raw_expr->get_expr_type() == T_OP_GET_USER_VAR) {  // sys var is stopped at resolve
    // 1. get variable name
    ObString variable_name;
    if (raw_expr->get_expr_type() == T_REF_COLUMN) {
      ObColumnRefRawExpr* column_ref = static_cast<ObColumnRefRawExpr*>(raw_expr);
      variable_name = column_ref->get_column_name();
    } else {
      is_user_variable = true;
      ObSysFunRawExpr* func_expr = static_cast<ObSysFunRawExpr*>(raw_expr);
      if (func_expr->get_children_count() != 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sys func expr child num is not correct", K(ret));
      } else {
        ObConstRawExpr* c_expr = static_cast<ObConstRawExpr*>(func_expr->get_param_expr(0));
        if (c_expr->get_value().get_type() != ObVarcharType) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("const expr child type is not correct", K(ret));
        } else {
          variable_name = c_expr->get_value().get_string();
        }
      }
    }
    // 2. replace it
    bool need_replaced_to_loaded_data_from_file = false;
    if (OB_SUCC(ret)) {
      ObConstRawExpr* c_expr = NULL;  // build a new const expr
      int hash_ret = OB_SUCCESS;
      int64_t idx = OB_INVALID_INDEX;
      if (OB_ISNULL(c_expr = OB_NEWx(ObConstRawExpr, (&allocator)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate const raw expr failed", K(ret));
      } else if (OB_SUCCESS != (hash_ret = varname_field_idx_hashmap_.get_refactored(variable_name, idx))) {
        if (OB_HASH_NOT_EXIST != hash_ret) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected hash ret", K(ret), K(hash_ret));
        } else if (!is_user_variable) {  // is a unknown column ref in right value, TODO arguable
          LOG_WARN("unknown column name in set right expr, do nothing");
        } else {  // session user variable
          // find the real value from session
          ObObj var_obj;
          ObSessionVariable tmp_sys_var;
          if (OB_SUCC(session_info.get_user_variable(variable_name, tmp_sys_var))) {
            var_obj = tmp_sys_var.value_;
            var_obj.set_meta_type(tmp_sys_var.meta_);
          } else {
            if (OB_ERR_USER_VARIABLE_UNKNOWN == ret) {
              LOG_WARN("no user variable: ", K(variable_name));
              var_obj.set_null();
              ret = OB_SUCCESS;  // always return success no matter found or not
            } else {
              LOG_WARN("Unexpected ret code", K(ret), K(variable_name), K(tmp_sys_var));
            }
          }
          if (OB_SUCC(ret)) {
            c_expr->set_value(var_obj);
            need_replaced_to_loaded_data_from_file = true;
          }
        }
      } else {
        if (OB_UNLIKELY(OB_INVALID_INDEX == idx)) {  // double check
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected idx", K(ret));
        } else {
          // got it from stmt description
          // replace it using an const expr with a blank string, waiting to be filling real str during executing
          need_replaced_to_loaded_data_from_file = true;
          ObObj obj;
          obj.set_string(ObVarcharType, ObString::make_empty_string());
          obj.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          c_expr->set_value(obj);
        }
      }

      if (OB_SUCC(ret) && need_replaced_to_loaded_data_from_file) {
        LOG_DEBUG("replace variable name to field value", K(variable_name), K(idx), K(*raw_expr), K(*c_expr));
        raw_expr = c_expr;
        ObLoadDataReplacedExprInfo varable_info;
        varable_info.replaced_expr = c_expr;
        varable_info.correspond_file_field_idx = idx;
        if (OB_FAIL(replaced_expr_value_index_store_.push_back(varable_info))) {
          LOG_WARN("push back replaced variable infos array failed", K(ret));
        }
      }
    }
  } else {
    // go recursively
    int64_t child_cnt = raw_expr->get_param_count();
    for (int64_t i = 0; OB_SUCC(ret) && i < child_cnt; ++i) {
      ObRawExpr*& child_expr = raw_expr->get_param_expr(i);
      if (OB_FAIL(recursively_replace_varables(child_expr, allocator, session_info))) {
        LOG_WARN("replace reference column failed", K(ret));
      }
    }  // end for
  }
  return ret;
}

int ObLoadDataImpl::generate_set_expr_strs(const ObSQLSessionInfo* session_info)
{
  int ret = OB_SUCCESS;
  // replace exprs
  int64_t fast_var_access_count = replaced_expr_value_index_store_.count();
  ObObj obj;
  for (int64_t i = 0; OB_SUCC(ret) && i < fast_var_access_count; ++i) {
    const ObLoadDataReplacedExprInfo& info = replaced_expr_value_index_store_.at(i);
    if (OB_ISNULL(info.replaced_expr) || OB_UNLIKELY(OB_INVALID_INDEX_INT64 == info.correspond_file_field_idx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("replaced_expr_value_index_store_ array is not inited", K(ret), K(info));
    } else {
      obj.set_string(ObVarcharType, parsed_field_strs_.at(info.correspond_file_field_idx));
      obj.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
      info.replaced_expr->set_value(obj);
    }
  }

  // build to sql string
  for (int64_t i = 0; OB_SUCC(ret) && i < set_assigned_column_number_; ++i) {
    ObRawExpr* right_expr = set_assigns_.at(i).expr_;
    if (OB_ISNULL(right_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null, impossible", K(ret));
    } else {
      char* buf = NULL;
      int64_t str_len = 0;
      ObString& expr_def = expr_strs_.at(i);
      expr_printer_.init(expr_to_string_buf_.get_data(),
          expr_to_string_buf_.get_capacity(),
          &str_len,
          get_timezone_info(session_info));
      if (OB_FAIL(expr_printer_.do_print(right_expr, T_NONE_SCOPE, true))) {
        LOG_WARN("print expr definition failed", K(ret));
      } else if (OB_ISNULL(buf = static_cast<char*>(expr_calc_allocator_.alloc(str_len * sizeof(char))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret));
      } else {
        MEMCPY(buf, expr_to_string_buf_.get_data(), str_len);
        expr_def.assign_ptr(buf, static_cast<int32_t>(str_len));
      }
    }
  }
  return ret;
}

// replace each variable in table assignment exprs into a const value
// eg: suppose that we have a stmt "load file 'mydata.csv' into table t1 (c1, c2) set c3 = c1 + c2"
//    we can get the expr of 'c3 = c1 + c2' after resolve set clause
//    when c1 c2 loaded from 'mydata.csv', say c1 = a and c2 = b, we need to take a b into expr to calculate c3
//    to locate c1 c2 from expr 'c3 = c1 + c2' in a short time, we store all the columns/user variables into
//    fast_variables_access
int ObLoadDataImpl::analyze_exprs_and_replace_variables(ObIAllocator& allocator, const ObSQLSessionInfo& session_info)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < set_assigns_.count(); ++i) {
    ObRawExpr*& right_expr = set_assigns_.at(i).expr_;
    if (OB_FAIL(recursively_replace_varables(right_expr, allocator, session_info))) {
      LOG_WARN("recursively replace var expr failed", K(ret));
    }
  }
  return ret;
}

int ObLoadDataBase::generate_fake_field_strs(
    ObIArray<ObString>& file_col_values, ObIAllocator& allocator, const char id_char)
{
  int ret = OB_SUCCESS;
  char* buf = NULL;

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
      if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%c%ld%c", id_char, i, '\0'))) {  // F1 F2 ..
        LOG_WARN("generate str failed", K(ret), K(pos), K(buf_len));
      } else {
        file_col_values.at(i).assign(buf + pos_bak, pos - pos_bak);
      }
    }
    LOG_DEBUG("generate fake field result", K(file_col_values));
  }
  return ret;
}

int ObLoadDataBase::construct_insert_sql(ObSqlString& insert_sql, const ObString& q_name,
    ObIArray<ObLoadTableColumnDesc>& desc, ObIArray<ObString>& insert_values, int64_t num_rows)
{
  int ret = OB_SUCCESS;
  insert_sql.reuse();
  char q = share::is_oracle_mode() ? '"' : '`';

  if (OB_UNLIKELY(q_name.empty()) || OB_UNLIKELY(desc.count() * num_rows != insert_values.count())) {
    ret = OB_INVALID_ARGUMENT;
  }

  OX(ret = insert_sql.assign("INSERT INTO "));
  OX(ret = insert_sql.append(q_name));
  for (int64_t i = 0; OB_SUCC(ret) && i < desc.count(); ++i) {
    OX(ret = insert_sql.append(0 == i ? "(" : ","));
    OX(ret = insert_sql.append_fmt("%c%.*s%c", q, desc.at(i).column_name_.length(), desc.at(i).column_name_.ptr(), q));
  }
  OX(ret = insert_sql.append(") VALUES "));
  for (int64_t i = 0; OB_SUCC(ret) && i < insert_values.count(); ++i) {
    OX(ret = insert_sql.append(i % desc.count() == 0 ? (i == 0 ? "(" : "),(") : ","));
    OX(ret = insert_sql.append_fmt("'%.*s'", insert_values.at(i).length(), insert_values.at(i).ptr()));
  }
  OX(ret = insert_sql.append(")"));

  if (OB_FAIL(ret)) {
    LOG_WARN("fail to append data", K(ret), K(insert_sql));
  }

  return ret;
}

int ObLoadDataBase::make_parameterize_stmt(
    ObExecContext& ctx, ObSqlString& insertsql, ParamStore& param_store, ObDMLStmt*& insert_stmt)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo* session = NULL;

  if (OB_ISNULL(session = ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (OB_ISNULL(ctx.get_sql_ctx()) || OB_ISNULL(ctx.get_sql_ctx()->schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql ctx is null", K(ret));
  } else {
    ObParser parser(ctx.get_allocator(), session->get_sql_mode());
    ParseResult parse_result;

    SqlInfo not_param_info;
    bool is_transform_outline = false;
    ObMaxConcurrentParam::FixParamStore fixed_param_store(
        ObNewModIds::OB_COMMON_ARRAY, ObWrapperAllocator(&ctx.get_allocator()));
    if (OB_FAIL(parser.parse(insertsql.string(), parse_result))) {
      LOG_WARN("paser template insert sql failed", K(ret));
    } else if (OB_FAIL(ObSqlParameterization::transform_syntax_tree(ctx.get_allocator(),
                   *session,
                   NULL,
                   0,  // no rewrite sql here
                   parse_result.result_tree_,
                   not_param_info,
                   param_store,
                   NULL,
                   fixed_param_store,
                   is_transform_outline))) {
      LOG_WARN("parameterize parser tree failed", K(ret));
    } else {
      ObResolverParams resolver_ctx;
      ObSchemaChecker schema_checker;
      schema_checker.init(*(ctx.get_sql_ctx()->schema_guard_));
      resolver_ctx.allocator_ = &ctx.get_allocator();
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
        ObResolver resolver(resolver_ctx);
        ObStmt* astmt = NULL;
        ParseNode* stmt_tree = parse_result.result_tree_->children_[0];
        if (OB_ISNULL(stmt_tree) || OB_ISNULL(ctx.get_stmt_factory()->get_query_ctx())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid argument", K(ret), K(stmt_tree));
        } else if (OB_FAIL(resolver.resolve(ObResolver::IS_NOT_PREPARED_STMT, *stmt_tree, astmt))) {
          LOG_WARN("resolve sql failed", K(ret), K(insertsql));
        } else {
          insert_stmt = static_cast<ObDMLStmt*>(astmt);
          ctx.get_stmt_factory()->get_query_ctx()->reset();
        }
      }
    }
  }
  return ret;
}

int ObLoadDataBase::memory_check_remote(uint64_t tenant_id, bool& need_wait_minor_freeze)
{
  int ret = OB_SUCCESS;
  ObTenantManager& tenant_mgr = ObTenantManager::get_instance();
  if (OB_UNLIKELY(!tenant_mgr.has_tenant(tenant_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("tenant no found", K(tenant_id), K(ret));
  } else {
    int64_t active_memstore_used = 0;
    int64_t total_memstore_used = 0;
    int64_t major_freeze_trigger = 0;
    int64_t memstore_limit = 0;
    int64_t freeze_cnt = 0;

    if (OB_FAIL(tenant_mgr.get_tenant_memstore_cond(
            tenant_id, active_memstore_used, total_memstore_used, major_freeze_trigger, memstore_limit, freeze_cnt))) {
      LOG_WARN("fail to get memstore used", K(ret));
    } else {
      if (total_memstore_used > (memstore_limit - major_freeze_trigger) / 2 + major_freeze_trigger) {
        need_wait_minor_freeze = true;
      } else {
        need_wait_minor_freeze = false;
      }
    }
    LOG_DEBUG("load data check tenant memory usage",
        K(active_memstore_used),
        K(total_memstore_used),
        K(major_freeze_trigger),
        K(memstore_limit),
        K(freeze_cnt),
        K(need_wait_minor_freeze));
  }
  return ret;
}

/*
  description:
  figure out how much columns should to be inserted, who are they, and where are they from
  all the information will be saved into valid_insert_column_infos array

    in:
    file_field_def_
    set_assigns_

    out:
    valid_insert_column_info_store_
    insert_column_names_

*/
int ObLoadDataImpl::summarize_insert_columns()
{
  int ret = OB_SUCCESS;
  // 1. a valid insert column, from file_field_def array, is defined in col_name_or_user_var clause of load sql
  //   e.g. INTO TABLE t1 (c1, c2, @a, @b), we got c1,c2 and discard @a, @b
  for (int64_t i = 0; OB_SUCC(ret) && i < file_field_def_.count(); ++i) {
    ObLoadDataStmt::FieldOrVarStruct& one_file_field_def = file_field_def_.at(i);
    if (one_file_field_def.is_table_column_) {
      ObLoadTableColumnDesc tmp_info;
      tmp_info.is_set_values_ = false;
      tmp_info.column_name_ = one_file_field_def.field_or_var_name_;
      tmp_info.column_id_ = one_file_field_def.column_id_;
      tmp_info.column_type_ = one_file_field_def.column_type_;
      tmp_info.array_ref_idx_ = i;  // array offset
      if (OB_FAIL(valid_insert_column_info_store_.push_back(tmp_info))) {
        LOG_WARN("push str failed", K(ret));
      } else if (OB_FAIL(insert_column_names_.push_back(tmp_info.column_name_))) {
        LOG_WARN("push back empty string failed", K(ret));
      }
    }
  }
  // 2. a valid insert column, from set_assigns array, is defined in set clause of load sql
  for (int64_t i = 0; OB_SUCC(ret) && i < set_assigns_.count(); ++i) {
    ObAssignment& one_set_assign = set_assigns_.at(i);
    if (OB_ISNULL(one_set_assign.column_expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("set assign expr is null", K(ret));
    } else {
      uint64_t set_col_id = one_set_assign.column_expr_->get_column_id();
      int64_t found_index = OB_INVALID_INDEX_INT64;
      if (find_insert_column_info(set_col_id, found_index)) {
        // overwrite the origin insert column, defined in 1
        ObLoadTableColumnDesc& tmp_info = valid_insert_column_info_store_.at(found_index);
        tmp_info.is_set_values_ = true;
        tmp_info.array_ref_idx_ = i;
      } else {
        // a new insert column is defined by set expr
        ObLoadTableColumnDesc tmp_info;
        tmp_info.column_name_ = one_set_assign.column_expr_->get_column_name();
        tmp_info.column_id_ = one_set_assign.column_expr_->get_column_id();
        tmp_info.column_type_ = one_set_assign.column_expr_->get_result_type().get_type();
        tmp_info.is_set_values_ = true;
        tmp_info.array_ref_idx_ = i;
        if (OB_FAIL(valid_insert_column_info_store_.push_back(tmp_info))) {
          LOG_WARN("push str failed", K(ret));
        } else if (OB_FAIL(insert_column_names_.push_back(tmp_info.column_name_))) {
          LOG_WARN("push back empty string failed", K(ret));
        }
      }
    }
  }
  // now wet got valid_insert_column_info_store_
  if (OB_SUCC(ret)) {
    insert_column_number_ = valid_insert_column_info_store_.count();
    if (OB_FAIL(ObLoadDataUtils::init_empty_string_array(insert_values_per_line_, insert_column_number_))) {
      LOG_WARN("init field str array failed", K(ret));
    }
    LOG_DEBUG("LOAD DATA insert column information is ", K_(valid_insert_column_info_store));
  }
  // aux1. figure out which column, insert directly, is not a string type.
  for (int64_t i = 0; OB_SUCC(ret) && i < file_field_def_.count(); ++i) {
    ObLoadDataStmt::FieldOrVarStruct& one_file_field_def = file_field_def_.at(i);
    if (one_file_field_def.is_table_column_) {
      if (one_file_field_def.column_type_ != ObVarcharType && one_file_field_def.column_type_ != ObCharType &&
          one_file_field_def.column_type_ != ObHexStringType && one_file_field_def.column_type_ != ObTextType &&
          one_file_field_def.column_type_ != ObRawType) {
        if (OB_FAIL(field_type_bit_set_.add_member(i))) {
          LOG_WARN("add field index to bitset failed", K(ret));
        } else {
          non_string_type_direct_insert_column_count_++;
        }
      }
    }
  }
  // aux2. find out the expr value
  for (int64_t i = 0; OB_SUCC(ret) && i < insert_column_number_; ++i) {
    const ObLoadTableColumnDesc& insert_info = valid_insert_column_info_store_.at(i);
    if (insert_info.is_set_values_) {
      if (OB_FAIL(expr_value_bitset_.add_member(i))) {
        LOG_WARN("add field index to bitset failed", K(ret));
      }
    }
  }
  return ret;
}

int ObLoadDataImpl::build_file_field_var_hashmap()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(varname_field_idx_hashmap_.create(COLUMN_MAP_BUCKET_NUM, ObModIds::OB_SQL_EXECUTOR))) {
    LOG_WARN("failed to create hash map", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < file_field_def_.count(); ++i) {
    int hash_ret = OB_SUCCESS;
    ObString& variable_name = file_field_def_.at(i).field_or_var_name_;
    if (OB_SUCCESS != (hash_ret = varname_field_idx_hashmap_.set_refactored(variable_name, i))) {
      if (hash_ret != OB_HASH_EXIST) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("set varname_to_field_id_ failed", K(hash_ret), K(ret), K(variable_name));
      } else {
        LOG_WARN("loaded column name duplicated", K(hash_ret), K(ret), K(variable_name));
        // choose latter
        if (OB_FAIL(varname_field_idx_hashmap_.set_refactored(variable_name, i, 1))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("set varname_to_field_id_ failed", K(hash_ret), K(ret), K(variable_name));
        }
      }
    }
  }
  return ret;
}

/*
 * if param_a != param_b: this variable is from a field of data file,
 * calc the correspond field index via param string value
 * return the index
 */
int ObLoadDataBase::calc_param_offset(const ObObjParam& param_a, const ObObjParam& param_b, int64_t& idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!param_a.is_varchar_or_char() || !param_b.is_varchar_or_char())) {
    ret = OB_ERR_UNEXPECTED;
  } else if (param_a.get_string().compare(param_b.get_string()) != 0) {
    const ObObjParam& value = param_a;
    const char* value_ptr = value.get_string_ptr();
    /* string should like "[F|f][0-9]+" */
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

int ObLoadDataImpl::do_local_sync_insert(ObPhysicalPlanCtx& plan_ctx)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  int64_t values_count = insert_values_per_line_.count();
  if (values_count <= 0 || insert_column_number_ <= 0 ||
      values_count != insert_column_number_ * (values_count / insert_column_number_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("insert values are invalid", K(ret), K(values_count), K(insert_column_number_));
  } else {
    ObSqlString insert_once_sql;
    if (OB_FAIL(ObLoadDataUtils::build_insert_sql_string_head(
            load_args_.dupl_action_, back_quoted_db_table_name_, insert_column_names_, insert_once_sql))) {
      LOG_WARN("gen insert sql keys failed", K(ret));
    } else if (OB_FAIL(ObLoadDataUtils::append_values_in_local_process(insert_column_number_,
                   values_count,
                   insert_values_per_line_,
                   expr_value_bitset_,
                   insert_once_sql,
                   escape_data_buffer_))) {
      LOG_WARN("append values failed", K(ret));
    } else {
      if (OB_FAIL(GCTX.sql_proxy_->write(load_args_.tenant_id_, insert_once_sql.ptr(), affected_rows))) {
        LOG_WARN("fail to write sql as one stmt", K(ret));
      }
    }
  }
  plan_ctx.add_affected_rows(affected_rows);
  if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
    plan_ctx.add_row_duplicated_count(1);
  }
  return ret;
}

int ObLoadDataImpl::take_record_for_failed_rows(ObPhysicalPlanCtx& plan_ctx, ObLoadbuffer* complete_task)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(complete_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    ObIArray<int64_t>& file_line_nums = complete_task->get_file_line_number();  // line number in file
    ObIArray<int16_t>& failed_row_idxs = complete_task->get_failed_row_idx();   // row idx in buff [0, 1000)
    ObIArray<int>& err_codes = complete_task->get_error_code_array();

    int64_t task_total_row_count = complete_task->get_stored_row_count();
    int64_t err_rows_count = 0;
    int64_t skipped_row_count = 0;

    bool is_all_rows_failed = ObLoadDataUtils::has_flag(
        complete_task->get_task_status(), static_cast<int64_t>(ObLoadTaskResultFlag::ALL_ROWS_FAILED));
    bool remote_exec_failed = ObLoadDataUtils::has_flag(
        complete_task->get_task_status(), static_cast<int64_t>(ObLoadTaskResultFlag::RPC_REMOTE_PROCESS_ERROR));
    if (remote_exec_failed) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      if (!is_all_rows_failed) {  // normal fail return
        const ObWarningBuffer* warnings_buf = common::ob_get_tsi_warning_buffer();
        int64_t warn_buff_size = 0;
        if (OB_ISNULL(warnings_buf)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("can not get thread warnings buffer");
        } else {
          warn_buff_size = static_cast<int64_t>(warnings_buf->get_buffer_size());
        }
        int64_t failed_rows_count = complete_task->get_failed_row_idx().count();
        for (int64_t i = 0; OB_SUCC(ret) && i < failed_rows_count; ++i) {
          int err_code = err_codes.at(i);
          int16_t row_idx = failed_row_idxs.at(i);
          int64_t line_number = -1;
          if (OB_ERR_PRIMARY_KEY_DUPLICATE == err_code) {
            skipped_row_count++;
          } else {
            err_rows_count++;
          }
          if (OB_UNLIKELY(row_idx >= file_line_nums.count())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid row index", K(ret));
          } else {
            line_number = file_line_nums.at(row_idx);
            const char* strerr = ::oceanbase::common::ob_strerror(err_code);
            total_err_lines_++;
            if (total_err_lines_ < warn_buff_size) {
              FORWARD_USER_ERROR_MSG(err_code, "%s at row %ld", strerr, line_number);
            } else {
              LOG_WARN("load data warning buff is full", K(strerr), "at row", line_number);
            }
          }
          LOG_DEBUG("error code is", K(line_number), K(err_code));
        }
      } else {
        // TODO dump file
        LOG_WARN("load data task failed", "task_id", complete_task->get_task_id());
        err_rows_count = task_total_row_count;
      }
    }
    plan_ctx.add_affected_rows(task_total_row_count - skipped_row_count - err_rows_count);
    plan_ctx.add_row_duplicated_count(skipped_row_count);
  }
  return ret;
}

int ObLoadDataBase::memory_wait_local(
    ObExecContext& ctx, const ObPartitionKey& part_key, ObAddr& server_addr, int64_t& total_wait_secs)
{
  int ret = OB_SUCCESS;
  static const int64_t WAIT_INTERVAL_US = 1 * 1000 * 1000;  // 1s
  ObSQLSessionInfo* session = NULL;
  ObMySQLProxy* sql_proxy_ = NULL;
  ObIPartitionLocationCache* loc_cache = NULL;
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    sqlclient::ObMySQLResult* result = NULL;
    ObSqlString sql;
    int64_t start_wait_ts = ObTimeUtil::current_time();
    int64_t wait_timeout_ts = 0;
    uint64_t tenant_id = OB_INVALID_TENANT_ID;

    if (OB_UNLIKELY(!part_key.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid server addr", K(ret), K(part_key));
    } else if (OB_ISNULL(loc_cache = ctx.get_task_exec_ctx().get_partition_location_cache())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid partition locatition cache", K(ret));
    } else if (OB_ISNULL((sql_proxy_ = GCTX.sql_proxy_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sql_proxy is null", K(ret));
    } else if (OB_ISNULL(session = ctx.get_my_session())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session is null", K(ret));
    } else {
      session->get_query_timeout(wait_timeout_ts);
      tenant_id = session->get_effective_tenant_id();
      // print info
      LOG_INFO("LOAD DATA is suspended until the memory is available", K(part_key), K(server_addr), K(total_wait_secs));
    }

    bool force_renew = false;
    bool need_wait_freeze = true;
    ObAddr leader_addr;

    while (OB_SUCC(ret) && need_wait_freeze) {

      usleep(WAIT_INTERVAL_US);

      leader_addr.reset();
      res.reuse();

      if (OB_FAIL(ObLoadDataUtils::check_session_status(*session))) {
        LOG_WARN("session is not valid during wait", K(ret));
      } else if (OB_FAIL(loc_cache->get_strong_leader(part_key, leader_addr, force_renew))) {
        LOG_WARN("get partition location cache failed", K(ret), K(part_key));
      } else if (OB_FAIL(sql.assign_fmt(SERVER_TENANT_MEMORY_EXAMINE_SQL,
                     tenant_id,
                     (leader_addr.get_ipv4() >> 24) & 0XFF,
                     (leader_addr.get_ipv4() >> 16) & 0XFF,
                     (leader_addr.get_ipv4() >> 8) & 0XFF,
                     (leader_addr.get_ipv4()) & 0XFF,
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
          force_renew = true;
          ret = OB_SUCCESS;
        }
      } else {
        force_renew = false;
        EXTRACT_BOOL_FIELD_MYSQL(*result, "need_wait_freeze", need_wait_freeze);
        // LOG_INFO("LOAD DATA is waiting for tenant memory available",
        // K(waited_seconds), K(total_wait_secs), K(tenant_id));
      }
    }

    // print info
    if (OB_SUCC(ret)) {
      int64_t wait_secs = (ObTimeUtil::current_time() - start_wait_ts) / 1000000;
      total_wait_secs += wait_secs;
      if (leader_addr != server_addr) {
        LOG_INFO("LOAD DATA location change", K(part_key), "old_addr", server_addr, "new_addr", leader_addr);
        server_addr = leader_addr;
      }
      LOG_INFO("LOAD DATA is resumed", "waited_seconds", wait_secs, K(total_wait_secs));
    }
  }
  return ret;
}

// wait minor/major freeze, assuming partition id is not changed
int ObLoadDataImpl::wait_server_memory_dump(ObExecContext& ctx, uint64_t partition_id)
{
  int ret = OB_SUCCESS;
  ObPartitionKey part_key;
  ObSQLSessionInfo* session = NULL;
  ObIPartitionLocationCache* partition_locatition_cache = NULL;
  ObMySQLProxy* sql_proxy_ = NULL;
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    sqlclient::ObMySQLResult* result = NULL;
    ObAddr leader_addr;
    ObSqlString sql;
    int64_t start_wait_ts = ObTimeUtil::current_time();
    int64_t current_ts = 0;
    int64_t waited_seconds = 0;
    bool need_wait_freeze = true;
    int64_t wait_timeout_ts = 0;
    int64_t total_wait_secs_back = total_wait_secs_;
    uint64_t tenant_id = OB_INVALID_TENANT_ID;

    if (OB_FAIL(part_key.init(load_args_.table_id_, partition_id, 0))) {
      LOG_WARN("partition key init failed", K(load_args_.table_id_), K(partition_id), K(part_num_));
    } else if (OB_ISNULL(partition_locatition_cache = ctx.get_task_exec_ctx().get_partition_location_cache())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid partition locatition cache", K(ret));
    } else if (OB_ISNULL((sql_proxy_ = GCTX.sql_proxy_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sql_proxy is null", K(ret));
    } else if (OB_ISNULL(session = ctx.get_my_session())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session is null", K(ret));
    } else {
      session->get_query_timeout(wait_timeout_ts);
      tenant_id = session->get_effective_tenant_id();
    }

    while (OB_SUCC(ret) && need_wait_freeze) {
      leader_addr.reset();
      res.reuse();
      if (OB_FAIL(partition_locatition_cache->get_strong_leader(part_key, leader_addr))) {
        LOG_WARN("get partition location cache failed", K(ret), K(part_key));
      } else if (OB_FAIL(sql.assign_fmt(SERVER_TENANT_MEMORY_EXAMINE_SQL,
                     tenant_id,
                     (leader_addr.get_ipv4() >> 24) & 0XFF,
                     (leader_addr.get_ipv4() >> 16) & 0XFF,
                     (leader_addr.get_ipv4() >> 8) & 0XFF,
                     (leader_addr.get_ipv4()) & 0XFF,
                     leader_addr.get_port()))) {
        LOG_WARN("fail to append sql", K(ret), K(tenant_id), K(leader_addr));
      } else if (OB_FAIL(sql_proxy_->read(res, OB_SYS_TENANT_ID, sql.ptr()))) {
        LOG_WARN("fail to execute sql", K(ret), K(sql));
      } else if (NULL == (result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get sql result", K(ret));
      } else if (OB_FAIL(result->next())) {
        LOG_WARN("fail to get result", K(ret));
      } else {
        EXTRACT_BOOL_FIELD_MYSQL(*result, "need_wait_freeze", need_wait_freeze);
        if (OB_SUCC(ret) && need_wait_freeze) {
          current_ts = ObTimeUtil::current_time();
          waited_seconds = (current_ts - start_wait_ts) / (1000 * 1000);
          if (OB_FAIL(ObLoadDataUtils::check_session_status(*session))) {
            LOG_WARN("session is invalid failed during waiting for tenant memory available ",
                K(ret),
                K(tenant_id),
                K(waited_seconds),
                K(current_ts),
                K(wait_timeout_ts));
          } else {
            total_wait_secs_ = total_wait_secs_back + waited_seconds;
            LOG_INFO("LOAD DATA is waiting for tenant memory available",
                K(waited_seconds),
                K_(total_wait_secs),
                K(tenant_id));
            usleep(WAIT_INTERVAL_US);
          }
        }
      }
    };

    // Any wait signal, proposed before the current timestamp
    // by other returned tasks from the same server, will be ignored
    if (OB_SUCC(ret)) {
      if (OB_FAIL(server_last_freeze_ts_hashmap_.set_refactored(leader_addr, ObTimeUtil::current_time(), 1))) {
        LOG_WARN("set server last freeze timestamp failed", K(ret));
      } else {
        LOG_INFO("LOAD DATA finish waiting, restart again", K(waited_seconds), K(tenant_id));
      }
    }
  }
  return ret;
}

int ObLoadDataImpl::handle_complete_task(ObPhysicalPlanCtx& plan_ctx, ObLoadbuffer* complete_task)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(complete_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    int64_t task_status = complete_task->get_task_status();
    if (OB_LIKELY(
            !ObLoadDataUtils::has_flag(task_status, static_cast<int64_t>(ObLoadTaskResultFlag::HAS_FAILED_ROW))) &&
        OB_LIKELY(!ObLoadDataUtils::has_flag(task_status, static_cast<int64_t>(ObLoadTaskResultFlag::TIMEOUT)))) {
      // succ
      plan_ctx.add_affected_rows(complete_task->get_stored_row_count());
    } else {
      // handle fail
      if (OB_FAIL(take_record_for_failed_rows(plan_ctx, complete_task))) {
        LOG_WARN("failed to record failed rows", K(ret));
      }
    }
  }
  LOG_DEBUG("handle one stale task", KPC(complete_task));
  return ret;
}

int ObLoadDataImpl::get_server_last_freeze_ts(const ObAddr& server_addr, int64_t& last_freeze_ts)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  if (OB_SUCCESS != (hash_ret = server_last_freeze_ts_hashmap_.get_refactored(server_addr, last_freeze_ts))) {
    if (OB_HASH_NOT_EXIST == hash_ret) {
      if (OB_FAIL(server_last_freeze_ts_hashmap_.set_refactored(server_addr, 0))) {
        LOG_WARN("set server last freeze timestamp failed", K(ret), K(server_addr));
      } else {
        last_freeze_ts = 0;  // is illegal but works
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get server last freeze timestamp failed", K(ret), K(server_addr));
    }
  }
  return ret;
}

/*
 * 1. handle one complete task
 * 2. send one task
 * 3. switch the complete task buffer to current
 */

int ObLoadDataImpl::send_and_switch_buffer(
    ObExecContext& ctx, ObPhysicalPlanCtx& plan_ctx, ObPartitionBufferCtrl* part_buf_ctrl)
{
  int ret = OB_SUCCESS;
  bool need_failed_callback = true;
  ObLoadbuffer* buffer = NULL;
  ObIPartitionLocationCache* partition_locatition_cache = NULL;
  if (OB_ISNULL(part_buf_ctrl)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_ISNULL(buffer = part_buf_ctrl->get_buffer<ObLoadbuffer>())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("buffer is null from partition buffer control", K(ret), KP(part_buf_ctrl));
  } else if (OB_ISNULL(partition_locatition_cache = ctx.get_task_exec_ctx().get_partition_location_cache())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid partition locatition cache", K(ret));
  } else {

    //  a) push finished buffer --syncThd--> b) on_task_finished
    //           ^                                    |
    //           |                                    |
    //      RPC callback                          Thd wakeup
    //           |                                    |
    //           |                                    v
    //  d) pop finished buffer  <--syncThd-- c) on_next_task_id

    // 1. wait next task id and pop an complete buffer
    wait_task_timer_.start_stat();
    int64_t task_id = -1;
    ObLoadbuffer* complete_buffer = NULL;
    if (OB_FAIL(task_controller_.on_next_task())) {
      need_failed_callback = false;
      LOG_WARN("get next task id failed", K(ret));
    } else if (OB_FAIL(complete_task_array_.pop(complete_buffer))) {
      LOG_WARN("pop complete buffer failed", K(ret));
    } else if (OB_ISNULL(complete_buffer)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("complete buffer is NULL, unexpected", K(ret));
    } else {
      task_id = task_controller_.get_next_task_id();
    }
    wait_task_timer_.end_stat();

    //+. check the minor freeze flag, and wait if nessesary
    if (OB_SUCC(ret)) {
      int64_t task_status = complete_buffer->get_task_status();
      if (OB_UNLIKELY(ObLoadDataUtils::has_flag(
              task_status, static_cast<int64_t>(ObLoadTaskResultFlag::NEED_WAIT_MINOR_FREEZE)))) {
        int64_t last_freeze_ts = 0;
        if (OB_FAIL(get_server_last_freeze_ts(complete_buffer->get_addr(), last_freeze_ts))) {
          LOG_WARN("failed to get server last freeze timestamp", K(ret));
        } else if (complete_buffer->get_returned_timestamp() > last_freeze_ts) {
          if (OB_FAIL(wait_server_memory_dump(ctx, complete_buffer->get_part_id()))) {
            LOG_WARN("failed to wait partition release the memory", K(ret));
          }
        }
      }
    }

    // 2. handle stale buffer first
    handle_result_timer_.start_stat();
    if (OB_SUCC(ret)) {
      if (OB_FAIL(handle_complete_task(plan_ctx, complete_buffer))) {
        LOG_WARN("handle complete task failed", K(ret));
      } else {
        complete_buffer->reuse();
      }
    }
    handle_result_timer_.end_stat();

    // 3. send new buffer
    if (OB_SUCC(ret)) {
      int64_t part_id = part_buf_ctrl->part_id_;
      buffer->set_task_id(task_id);
      buffer->set_part_id(part_id);
      ObPartitionKey part_key;
      ObAddr leader_addr;
      if (OB_FAIL(part_key.init(load_args_.table_id_, part_id, 0))) {
        LOG_WARN("partition key init failed", K(load_args_.table_id_), K(part_id), K(part_num_));
      } else if (OB_FAIL(partition_locatition_cache->get_strong_leader(part_key, leader_addr))) {
        LOG_WARN("get partition location cache failed", K(ret), K(part_key));
      } else {
        // async rpc call send used buffer
        buffer->set_addr(leader_addr);
        ObRpcLoadDataTaskCallBack mycallback(task_controller_, complete_task_array_, buffer);
        serialize_timer_.start_stat();
        if (OB_FAIL(GCTX.load_data_proxy_->to(leader_addr)
                        .by(load_args_.tenant_id_)
                        .timeout(RPC_BATCH_INSERT_TIMEOUT_US)
                        .ap_load_data_execute(*buffer, &mycallback))) {
          LOG_WARN("load data proxy post rpc failed");
        } else {
          need_failed_callback = false;  // handle over to RPC framework
          LOG_DEBUG("LOAD DATA succ post one buffer to", K(leader_addr), K(part_key));
        }
        serialize_timer_.end_stat();
      }
    }

    // 4. switch to cleaned complete buffer
    if (OB_SUCC(ret)) {
      part_buf_ctrl->set_buffer<ObLoadbuffer>(complete_buffer);
    }

    // 5. handle failed case
    if (OB_FAIL(ret) && need_failed_callback) {
      int second_ret = OB_SUCCESS;
      if (OB_SUCCESS != (second_ret = task_controller_.on_task_finished())) {
        LOG_WARN("failed to call on task finished", K(ret), K(second_ret));
      }
    }
  }

  return ret;
}

bool ObLoadDataImpl::scan_for_line_end(char*& cur_pos, const char* buf_end)
{
  bool matched = false;
  char* end_field_pos = NULL;

  for (; !matched && cur_pos != buf_end; cur_pos++) {
    char cur_char = *cur_pos;
    if (is_enclosed_field_start(cur_pos, cur_char)) {
      in_enclose_flag_ = true;
      // cur_field_begin_pos_++;
    }
    end_field_pos = cur_pos;
    if (OB_LIKELY(field_term_char_ != INVALID_CHAR) && is_terminate(cur_char, field_term_detector_, end_field_pos)) {
      handle_one_field(end_field_pos);
      cur_field_begin_pos_ = cur_pos + 1;
    }
    end_field_pos = cur_pos;
    if (OB_LIKELY(line_term_char_ != INVALID_CHAR) && is_terminate(cur_char, line_term_detector_, end_field_pos)) {
      if (cur_field_begin_pos_ < end_field_pos) {  // field terminated by a line terminator
        handle_one_field(end_field_pos);
        cur_field_begin_pos_ = cur_pos + 1;
      }
      if (!flag_line_term_by_counting_field_ || (field_id_ == file_column_number_)) {
        matched = true;
      }
    }
    escape_sm_.shift_by_input(cur_char);
  }
  return matched;
}

void ObLoadDataImpl::deal_with_irregular_line()
{
  if (field_id_ > file_column_number_) {
    LOG_WARN("Row was truncated; it contained more data than there were input columns",
        K(field_id_),
        K(file_column_number_));
    LOG_USER_WARN(OB_WARN_TOO_MANY_RECORDS, parsed_line_count_);
  } else if (field_id_ < file_column_number_) {
    for (int64_t i = field_id_; i < file_column_number_; ++i) {
      deal_with_empty_field(parsed_field_strs_.at(i), i);
    }
    LOG_WARN("Row doesn't contain data for all columns", K(field_id_), K(file_column_number_));
    LOG_USER_WARN(OB_WARN_TOO_FEW_RECORDS, parsed_line_count_);
  } else {
    // do nothing
  }
}

int ObLoadDataImpl::handle_one_line_local(ObPhysicalPlanCtx& plan_ctx)
{
  int ret = OB_SUCCESS;
  // LOAD_STOP_ON_DUP mode, single thread loading
  if (OB_FAIL(collect_insert_row_strings())) {
    LOG_WARN("cat strings into insert values failed", K(ret));
  } else if (OB_FAIL(do_local_sync_insert(plan_ctx))) {
    LOG_WARN("do local sync insert failed", K(ret));
  }
  return ret;
}

int ObLoadDataImpl::handle_one_line(ObExecContext& ctx, ObPhysicalPlanCtx& plan_ctx)
{
  int ret = OB_SUCCESS;
  // 1. replace expr and save them as strings
  if (set_assigned_column_number_ > 0) {
    if (OB_FAIL(generate_set_expr_strs(ctx.get_my_session()))) {
      LOG_WARN("assemble set expr strings failed", K(ret));
    }
  }
  // 2. calc partition id
  int64_t part_id = OB_INVALID_ID;
  if (OB_SUCC(ret)) {
    if (PARTITION_LEVEL_ZERO == part_level_) {
      part_id = 0;
    } else {
      calc_timer_.start_stat();
      // fill runtime_param_store
      ParamStore& runtime_param_store = plan_ctx.get_param_store_for_update();
      for (int64_t i = 0; OB_SUCC(ret) && i < params_count_; ++i) {
        int64_t field_offset = OB_INVALID_INDEX_INT64;
        if (OB_INVALID_INDEX_INT64 != (field_offset = params_value_index_store_.at(i))) {
          if (OB_UNLIKELY(field_offset > file_column_number_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected field offset", K(ret));
          } else {
            ObObjParam& param = runtime_param_store.at(i);
            ObString& tar_str = parsed_field_strs_.at(field_offset);
            ObLoadDataBase::set_one_field_objparam(param, tar_str);
          }
        } else {
          // do nothing, const variable
        }
      }
      if (OB_SUCC(ret)) {
        partition_ids_.reuse();
        // TODO check which allocator is used in table_location expr calc
        if (OB_FAIL(table_location_.calculate_partition_ids(
                ctx, &schema_guard_, runtime_param_store, partition_ids_, dtc_params_))) {
          LOG_WARN("calc partition ids failed", K(ret));
        } else {
          part_id = partition_ids_.at(0);
        }
      }
      calc_timer_.end_stat();
    }
  }
  buffer_copy_timer_.start_stat();

  // 4. store row to buffer of the partition with part_id
  ObPartitionBufferCtrl* part_buf_ctrl = NULL;
  ObLoadbuffer* buffer = NULL;
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(part_buf_ctrl = get_part_buf_ctrl(part_id))) {
      if (OB_FAIL(create_part_buffer_ctrl(part_id, &ctx.get_allocator(), part_buf_ctrl))) {
        LOG_WARN("create partition buffer info failed", K(ret), K(part_id));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(buffer = part_buf_ctrl->get_buffer<ObLoadbuffer>())) {
      if (OB_FAIL(create_buffer(ctx.get_allocator(), buffer))) {
        LOG_WARN("create buffer failed", K(ret));
      } else {
        part_buf_ctrl->set_buffer<ObLoadbuffer>(buffer);
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(collect_insert_row_strings())) {
      LOG_WARN("cat field strs into values failed", K(ret));
    } else if (OB_FAIL(buffer->store_row(insert_values_per_line_, parsed_line_count_))) {
      LOG_WARN("assign temp row failed", K(ret));
    }
  }
  buffer_copy_timer_.end_stat();

  // 5. send the buffer if full
  if (OB_SUCC(ret) && buffer->is_full()) {
    rpc_timer_.start_stat();
    if (OB_FAIL(send_and_switch_buffer(ctx, plan_ctx, part_buf_ctrl))) {
      LOG_WARN("send buffer and switch buffer failed", K(ret));
    }
    rpc_timer_.end_stat();
  }
  return ret;
}

const char* ObCSVParser::ZERO_STRING = "0";

int ObCSVParser::init(
    int64_t file_column_nums, const ObCSVFormats& formats, const common::ObBitSet<>& string_type_column)
{
  int ret = OB_SUCCESS;
  total_field_nums_ = file_column_nums;
  formats_ = formats;
  if (OB_FAIL(values_in_line_.prepare_allocate(file_column_nums))) {
    LOG_WARN("fail to reserve array", K(ret));
  } else if (OB_FAIL(string_type_column_.add_members(string_type_column))) {
    LOG_WARN("fail to add member", K(ret));
  }
  return ret;
}

void ObCSVParser::next_buf(char* buf_start, const int64_t buf_len, bool is_last_buf)
{
  is_last_buf_ = is_last_buf;
  cur_pos_ = buf_start + (cur_pos_ - cur_line_begin_pos_);
  cur_field_begin_pos_ = buf_start + (cur_field_begin_pos_ - cur_line_begin_pos_);
  cur_field_end_pos_ = buf_start + (cur_field_end_pos_ - cur_line_begin_pos_);
  cur_line_begin_pos_ = buf_start;
  buf_begin_pos_ = buf_start;
  buf_end_pos_ = buf_start + buf_len;
}

int ObCSVParser::next_line(bool& yield_line)
{
  int ret = OB_SUCCESS;
  bool yield = false;
  int with_back_slash = 0;

  for (; !yield && cur_pos_ != buf_end_pos_; ++cur_pos_) {

    if (*cur_pos_ == formats_.enclose_char_ && !in_enclose_flag_ && cur_pos_ == cur_field_begin_pos_) {
      in_enclose_flag_ = true;
    }

    if (!is_escaped_flag_ && *cur_pos_ == formats_.escape_char_) {
      is_escaped_flag_ = true;
    } else {
      char escaped_res = *cur_pos_;
      if (is_escaped_flag_) {
        escaped_res = escaped_char(*cur_pos_, &with_back_slash);
      }
      if (cur_field_end_pos_ != cur_pos_ && !is_fast_parse_) {
        *cur_field_end_pos_ = escaped_res;
      }

      bool line_term_matched = false;

      if (is_terminate_char(*cur_pos_, cur_field_end_pos_, line_term_matched)) {
        if (!line_term_matched || cur_field_begin_pos_ < cur_pos_) {
          handle_one_field(cur_field_end_pos_);
          field_id_++;
        }
        char* next_pos = cur_pos_ + 1;
        cur_field_begin_pos_ = next_pos;
        cur_field_end_pos_ = cur_pos_;
        if (line_term_matched && (!formats_.is_line_term_by_counting_field_ || field_id_ == total_field_nums_)) {
          if (OB_UNLIKELY(field_id_ != total_field_nums_)) {
            ret = deal_with_irregular_line();
          }
          yield = true;
          // reset runtime variables
          field_id_ = 0;
          cur_line_begin_pos_ = next_pos;
        }
      }

      if (is_escaped_flag_) {
        is_escaped_flag_ = false;
      }

      ++cur_field_end_pos_;
    }
  }

  if (!yield && is_last_buf_ && cur_pos_ == buf_end_pos_) {
    if (cur_field_begin_pos_ < cur_pos_) {
      // new field, terminated with an eof
      handle_one_field(cur_field_end_pos_);
      field_id_++;
    }
    cur_field_begin_pos_ = cur_pos_;
    cur_field_end_pos_ = cur_pos_;
    if (field_id_ > 0) {  // unfinished rows at the end
      if (OB_UNLIKELY(field_id_ != total_field_nums_)) {
        ret = deal_with_irregular_line();
      }
      yield = true;
      // reset runtime variables
      field_id_ = 0;
      cur_field_begin_pos_ = cur_pos_;
      cur_line_begin_pos_ = cur_pos_;
    }
  }

  yield_line = yield;

  return ret;
}

int ObCSVParser::deal_with_irregular_line()
{
  int ret = OB_SUCCESS;
  if (field_id_ > total_field_nums_) {
    ret = OB_WARN_TOO_MANY_RECORDS;
  } else if (field_id_ < total_field_nums_) {
    for (int64_t i = field_id_; i < total_field_nums_; ++i) {
      deal_with_empty_field(values_in_line_.at(i), i);
    }
    ret = OB_WARN_TOO_FEW_RECORDS;
  } else {
    // do nothing
  }
  return ret;
}

void ObCSVParser::deal_with_empty_field(ObString& field_str, int64_t index)
{
  if (formats_.null_column_fill_zero_string_ && !string_type_column_.has_member(index)) {
    // a non-string value will be set to "0", "0" will be cast to zero value of target types
    field_str.assign_ptr(ZERO_STRING, 1);
  } else {
    // a string value will be set to ''
    field_str.reset();
  }
}

int ObCSVParser::fast_parse_lines(
    ObLoadFileBuffer& buffer, ObCSVParser& parser, bool is_last_buf, int64_t& valid_len, int64_t& line_count)
{
  int ret = OB_SUCCESS;
  int64_t cur_lines = 0;
  ObCSVFormats& formats = parser.get_format();

  if (OB_UNLIKELY(!buffer.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
  } else if (formats.is_simple_format_) {
    char* cur_pos = buffer.begin_ptr();
    bool in_escaped = false;
    for (char* p = buffer.begin_ptr(); p != buffer.current_ptr(); ++p) {
      char cur_char = *p;
      if (!in_escaped) {
        if (formats.enclose_char_ == cur_char) {
          in_escaped = true;
        } else if (formats.line_term_char_ == cur_char) {
          cur_lines++;
          cur_pos = p + 1;
          if (cur_lines >= line_count) {
            break;
          }
        }
      }
    }
    if (is_last_buf && buffer.current_ptr() > cur_pos) {
      cur_lines++;
      cur_pos = buffer.current_ptr();
    }
    valid_len = cur_pos - buffer.begin_ptr();
  } else {
    parser.reuse();
    parser.next_buf(buffer.begin_ptr(), buffer.get_data_len(), is_last_buf);

    bool yield = true;
    while (parser.next_line(yield), yield) {
      cur_lines++;
      if (cur_lines >= line_count) {
        break;
      }
    }
    valid_len = parser.get_complete_lines_len();
  }

  line_count = cur_lines;

  return ret;
}

/*
 * executing short path when:
 * 1. lines start by '', or not defined (default '')
 * 2. fields terminate by 'x' where x is a single char, or not defined (default '\t')
 * 3. lines terminate by 'x' where x is a single char, or not defined (default '\n')
 */
int ObLoadDataImpl::handle_one_file_buf_fast(
    ObExecContext& ctx, ObPhysicalPlanCtx& plan_ctx, char* parsing_begin_pos, const int64_t data_len, bool is_eof)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(data_len <= 0) || OB_ISNULL(parsing_begin_pos)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input argument", K(ret), K(data_len), KP(parsing_begin_pos));
  } else {
    char* cur_pos = parsing_begin_pos;
    const char* buf_end = parsing_begin_pos + data_len;
    char* end_field_pos = NULL;
    // scan buf by char
    for (; cur_pos != buf_end; cur_pos++) {
      char cur_char = *cur_pos;
      if (is_enclosed_field_start(cur_pos, cur_char)) {
        in_enclose_flag_ = true;
      }
      end_field_pos = cur_pos;
      if (is_terminate_char(cur_char, field_term_char_, end_field_pos)) {  // meet a field terminator
        handle_one_field(end_field_pos);
        cur_field_begin_pos_ = cur_pos + 1;
      }
      end_field_pos = cur_pos;
      if (is_terminate_char(cur_char, line_term_char_, end_field_pos)) {  // meet a line terminator
        if (cur_field_begin_pos_ < cur_pos) {                             // field terminated by a line terminator
          handle_one_field(end_field_pos);
          cur_field_begin_pos_ = cur_pos + 1;
        }
        if (!flag_line_term_by_counting_field_ || (field_id_ == file_column_number_)) {
          parsed_line_count_++;
          if (OB_LIKELY(parsed_line_count_ > load_args_.ignore_rows_)) {  // TODO need opt
            if (OB_UNLIKELY(field_id_ != file_column_number_)) {
              deal_with_irregular_line();
            }
            if (OB_FAIL(handle_one_line(ctx, plan_ctx))) {
              LOG_WARN("failed to handle one line", K(ret));
              //------for efficient loop-------
              break;  // the load process will stop on failed, not need to reset any runtime value
              //-------------------------------
            }
          }
          // reset runtime variables
          field_id_ = 0;
          cur_field_begin_pos_ = cur_pos + 1;
          cur_line_begin_pos_ = cur_pos + 1;
          // the assert(in_enclose_flag_ == false) is ensured here
        }
      }
      escape_sm_.shift_by_input(cur_char);
    }  // end for

    if (OB_SUCC(ret) && OB_UNLIKELY(is_eof)) {
      if (cur_field_begin_pos_ < cur_pos) {
        // new field, terminated with an eof
        handle_one_field(cur_pos);
      }
      if (field_id_ > 0) {  // unfinished rows at the end
        parsed_line_count_++;
        if (OB_UNLIKELY(field_id_ != file_column_number_)) {
          deal_with_irregular_line();
        }
        if (OB_LIKELY(parsed_line_count_ > load_args_.ignore_rows_)) {
          if (OB_FAIL(handle_one_line(ctx, plan_ctx))) {
            LOG_WARN("failed to handle one line", K(ret));
          }
        }
        // reset runtime variables
        field_id_ = 0;
        cur_field_begin_pos_ = cur_pos + 1;
        cur_line_begin_pos_ = cur_pos + 1;
      }
    }
  }

  return ret;
}

int ObLoadDataImpl::handle_one_file_buf(
    ObExecContext& ctx, ObPhysicalPlanCtx& plan_ctx, char* parsing_begin_pos, const int64_t data_len, bool is_eof)
{
  int ret = OB_SUCCESS;
  char* cur_pos = parsing_begin_pos;
  char* buf_end = parsing_begin_pos + data_len;

  while (OB_SUCC(ret) && cur_pos != buf_end) {
    switch (cur_step_) {
      case FIND_LINE_START:
        if (scan_for_line_start(cur_pos, buf_end)) {
          cur_step_ = FIND_LINE_TERM;
          cur_field_begin_pos_ = cur_pos;
          cur_line_begin_pos_ = cur_pos;
        } else {
          break;
        }
        // line start matched, fall into next step
      case FIND_LINE_TERM:
        if (scan_for_line_end(cur_pos, buf_end)) {
          parsed_line_count_++;
          if (field_id_ != file_column_number_) {
            deal_with_irregular_line();
          }
          if (parsed_line_count_ > load_args_.ignore_rows_) {
            if (OB_FAIL(handle_one_line(ctx, plan_ctx))) {
              LOG_WARN("failed to handle one line", K(ret));
            }
          }
          // reset runtime variables
          field_id_ = 0;
          cur_field_begin_pos_ = cur_pos;
          cur_line_begin_pos_ = cur_pos;
          cur_step_ = initial_step_;
        }
        break;

      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("unexpected step value", K(cur_step_));
        break;
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(is_eof) && cur_step_ != FIND_LINE_START) {
      if (cur_field_begin_pos_ < buf_end) {
        // new field, terminated with an eof
        handle_one_field(buf_end);
      }
      if (field_id_ > 0) {  // unfinished rows at the end
        parsed_line_count_++;
        if (field_id_ != file_column_number_) {
          deal_with_irregular_line();
        }
        if (parsed_line_count_ > load_args_.ignore_rows_) {
          if (OB_FAIL(handle_one_line(ctx, plan_ctx))) {
            LOG_WARN("failed to handle one line", K(ret));
          }
        }
        // reset runtime variables
        field_id_ = 0;
        cur_field_begin_pos_ = cur_pos;
        cur_line_begin_pos_ = cur_pos;
        cur_step_ = initial_step_;
      }
    }
  }
  return ret;
}

int ObLoadDataImpl::init_from_load_stmt(const ObLoadDataStmt& load_stmt)
{
  int ret = OB_SUCCESS;
  // load arguments first
  load_args_.assign(load_stmt.get_load_arguments());
  data_struct_in_file_.assign(load_stmt.get_data_struct_in_file());
  // fast chars
  field_term_char_ = data_struct_in_file_.field_term_str_.empty()
                         ? INVALID_CHAR
                         : static_cast<int64_t>(data_struct_in_file_.field_term_str_[0]);
  line_term_char_ = data_struct_in_file_.line_term_str_.empty()
                        ? INVALID_CHAR
                        : static_cast<int64_t>(data_struct_in_file_.line_term_str_[0]);
  enclose_char_ = data_struct_in_file_.field_enclosed_char_;
  escape_char_ = data_struct_in_file_.field_escaped_char_;
  escape_sm_.set_escape_char(escape_char_);
  // init allocator tenant id
  expr_calc_allocator_.set_tenant_id(load_args_.tenant_id_);
  array_allocator_.set_tenant_id(load_args_.tenant_id_);
  // init back quoted database table name
  back_quoted_db_table_name_ = load_args_.combined_name_;

  // if line terminator is empty, a line is finished when field count equal to # of table columns to be inserted
  if (data_struct_in_file_.line_term_str_.empty() && !data_struct_in_file_.field_term_str_.empty()) {
    flag_line_term_by_counting_field_ = true;
    // use field term instead
    data_struct_in_file_.line_term_str_ = data_struct_in_file_.field_term_str_;
    line_term_char_ = field_term_char_;
  }

  // init for set assignment
  const ObAssignments& set_assigns = load_stmt.get_table_assignment();
  for (int64_t i = 0; OB_SUCC(ret) && i < set_assigns.count(); ++i) {
    if (OB_FAIL(set_assigns_.push_back(set_assigns.at(i)))) {
      LOG_WARN("failed to push back set assigns");
    }
  }
  set_assigned_column_number_ = set_assigns_.count();

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObLoadDataUtils::init_empty_string_array(expr_strs_, set_assigned_column_number_))) {
      LOG_WARN("init assign str array failed", K(ret));
    }
  }

  // init for fields
  const ObIArray<ObLoadDataStmt::FieldOrVarStruct>& file_field_def = load_stmt.get_field_or_var_list();
  for (int64_t i = 0; OB_SUCC(ret) && i < file_field_def.count(); ++i) {
    if (OB_FAIL(file_field_def_.push_back(file_field_def.at(i)))) {
      LOG_WARN("failed to push back set assigns");
    }
  }
  file_column_number_ = file_field_def_.count();

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObLoadDataUtils::init_empty_string_array(parsed_field_strs_, file_column_number_))) {
      LOG_WARN("init field str array failed", K(ret));
    }
  }
  return ret;
}

int ObLoadDataImpl::init_table_location_via_fake_insert_stmt(
    ObExecContext& ctx, ObPhysicalPlanCtx& plan_ctx, ObSQLSessionInfo& session_info)
{
  int ret = OB_SUCCESS;
  ParamStore& runtime_param_store = plan_ctx.get_param_store_for_update();
  ParamStore* param_store[2];
  void* param_store_buf1 = NULL;
  void* param_store_buf2 = NULL;
  int64_t buf_size = sizeof(ParamStore);
  if (OB_ISNULL(param_store_buf1 = ctx.get_allocator().alloc(buf_size)) ||
      OB_ISNULL(param_store_buf2 = ctx.get_allocator().alloc(buf_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else {
    param_store[0] = new (param_store_buf1) ParamStore(ObWrapperAllocator(ctx.get_allocator()));
    param_store[1] = new (param_store_buf2) ParamStore(ObWrapperAllocator(ctx.get_allocator()));
  }
  ObDMLStmt* insert_stmt[2] = {NULL};
  char identity_char[2] = {'f', 'F'};
  ObSqlString insert_sql;

  // 1. generate two fake insert stmt with self-made identity_chars, and transform both into a parameterized stmt
  for (int64_t i = 0; OB_SUCC(ret) && i < 2; ++i) {
    insert_sql.reset();
    if (OB_FAIL(ObLoadDataBase::generate_fake_field_strs(parsed_field_strs_, ctx.get_allocator(), identity_char[i]))) {
      LOG_WARN("generate fake field failed", K(ret));
    } else if (OB_FAIL(generate_set_expr_strs(ctx.get_my_session()))) {
      LOG_WARN("assemble set expr strings failed", K(ret));
    } else if (OB_FAIL(collect_insert_row_strings())) {
      LOG_WARN("cat strings into insert values failed", K(ret));
    } else if (OB_FAIL(ObLoadDataBase::construct_insert_sql(insert_sql,
                   back_quoted_db_table_name_,
                   valid_insert_column_info_store_,
                   insert_values_per_line_,
                   insert_column_number_))) {
      LOG_WARN("construct insert sql failed", K(ret));
    } else if (OB_FAIL(ObLoadDataBase::make_parameterize_stmt(ctx, insert_sql, *param_store[i], insert_stmt[i]))) {
      LOG_WARN("make parameterize statement failed", K(ret));
    }
  }
  // 2. calc param offset
  //   if the param in param store is from the fields of the load file, calculate correspond the column idx (offset) in
  //   the file
  if (OB_SUCC(ret)) {
    if (param_store[0]->count() != param_store[1]->count() || OB_ISNULL(insert_stmt[0]) || OB_ISNULL(insert_stmt[1])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("parser error", K(ret), K(insert_stmt[0]), K(insert_stmt[1]));
    } else {
      int64_t param_store_size = param_store[0]->count();
      int64_t idx = OB_INVALID_INDEX_INT64;
      for (int64_t i = 0; OB_SUCC(ret) && i < param_store_size; ++i) {
        if (OB_FAIL(ObLoadDataBase::calc_param_offset(param_store[0]->at(i), param_store[1]->at(i), idx))) {
          LOG_WARN("calc param offset failed", K(ret));
        } else if (OB_FAIL(params_value_index_store_.push_back(idx))) {
          LOG_WARN("push back failed", K(ret));
        } else if (OB_FAIL(runtime_param_store.push_back(param_store[0]->at(i)))) {
          LOG_WARN("push back param store failed", K(ret));
        }
      }
      params_count_ = param_store_size;
      LOG_DEBUG("load data runtime params", K(runtime_param_store), K(params_value_index_store_));
    }
  }
  // 3. init table locatition use param store 0
  if (OB_SUCC(ret)) {
    ObArray<ObRawExpr*> filter_exprs;
    const common::ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params(&session_info);
    if (OB_FAIL(table_location_.init(schema_guard_,
            *(insert_stmt[0]),
            ctx.get_my_session(),
            filter_exprs,
            load_args_.table_id_,
            load_args_.table_id_,
            NULL,
            dtc_params,
            true))) {
      LOG_WARN("fail to init table location", K(ret));
    }
  }
  return ret;
}

int ObLoadDataImpl::init_separator_detectors(ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;
  int32_t line_start_len = data_struct_in_file_.line_start_str_.length();
  int32_t line_term_len = data_struct_in_file_.line_term_str_.length();
  int32_t field_term_len = data_struct_in_file_.field_term_str_.length();
  if (line_start_len > 0 && OB_FAIL(line_start_detector_.init(allocator, data_struct_in_file_.line_start_str_))) {
    LOG_WARN("init line start str kmp detector failed");
  } else if (line_term_len > 0 && OB_FAIL(line_term_detector_.init(allocator, data_struct_in_file_.line_term_str_))) {
    LOG_WARN("init line term str kmp detector failed");
  } else if (field_term_len > 0 &&
             OB_FAIL(field_term_detector_.init(allocator, data_struct_in_file_.field_term_str_))) {
    LOG_WARN("init field term str kmp detector failed");
  }
  // skep find line start step is no line start separate
  initial_step_ = (line_start_len > 0) ? FIND_LINE_START : FIND_LINE_TERM;
  return ret;
}

int ObLoadDataImpl::init_data_buf(ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;
  char* buf = NULL;
  char* buf2 = NULL;
  if (OB_ISNULL(buf = static_cast<char*>(allocator.alloc(OB_MAX_DEFAULT_VALUE_LENGTH * sizeof(char))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate buf failed", K(ret));
  } else if (!expr_to_string_buf_.set_data(buf, OB_MAX_DEFAULT_VALUE_LENGTH)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("set expr string buf data failed", K(ret));
  } else if (OB_ISNULL(buf2 = static_cast<char*>(allocator.alloc(OB_MAX_DEFAULT_VALUE_LENGTH * sizeof(char))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate buf failed", K(ret));
  } else if (!escape_data_buffer_.set_data(buf2, OB_MAX_DEFAULT_VALUE_LENGTH)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("set expr string buf data failed", K(ret));
  }
  return ret;
}

int ObLoadDataImpl::send_all_buffer_finally(ObExecContext& ctx, ObPhysicalPlanCtx& plan_ctx)
{
  int ret = OB_SUCCESS;
  ObPartitionBufferCtrl* part_buf_ctrl = NULL;
  ObLoadbuffer* buffer = NULL;
  for (PartitionBufferHashMap::iterator iter = part_buffer_map_.begin(); OB_SUCC(ret) && iter != part_buffer_map_.end();
       ++iter) {
    if (OB_ISNULL(part_buf_ctrl = iter.value_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("buff info is null, impossible", K(ret));
    } else if (OB_ISNULL(buffer = part_buf_ctrl->get_buffer<ObLoadbuffer>())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("buffer is null, impossible", K(ret));
    } else {
      if (!buffer->is_empty()) {
        if (OB_FAIL(send_and_switch_buffer(ctx, plan_ctx, part_buf_ctrl))) {
          LOG_WARN("send buffer and switch buffer failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObLoadDataImpl::wait_all_task_finished(ObPhysicalPlanCtx& plan_ctx)
{
  int ret = OB_SUCCESS;
  ObLoadbuffer* complete_buffer = NULL;
  task_controller_.wait_all_task_finish();
  MEM_BARRIER();
  while (OB_SUCC(ret) && complete_task_array_.count() > 0) {  // TODO handle wait timeout
    if (OB_FAIL(complete_task_array_.pop(complete_buffer))) {
      LOG_WARN("get stale buffer failed", K(ret));
    } else if (OB_FAIL(handle_complete_task(plan_ctx, complete_buffer))) {
      LOG_WARN("handle complete task failed", K(ret));
    }
  }
  return ret;
}

void ObLoadDataImpl::destroy_all_buffer()
{
  ObLoadbuffer* load_buffer = NULL;
  for (int64_t i = 0; i < allocated_buffer_store_.count(); ++i) {
    load_buffer = allocated_buffer_store_.at(i);
    if (NULL != load_buffer) {
      load_buffer->~ObLoadbuffer();
    }
  }
}

int ObLoadDataImpl::init_everything_first(
    ObExecContext& ctx, const ObLoadDataStmt& load_stmt, ObSQLSessionInfo& session)
{
  int ret = OB_SUCCESS;
  const ObTableSchema* tbschema = NULL;
  ObMultiVersionSchemaService* schema_service = NULL;
  ObTaskExecutorCtx& task_exec_ctx = ctx.get_task_exec_ctx();

  if (OB_FAIL(init_from_load_stmt(load_stmt))) {
    LOG_WARN("init everything failed", K(ret));
  } else if (OB_FAIL(init_data_buf(ctx.get_allocator()))) {
    LOG_WARN("failed to init data buf");
  } else if (OB_FAIL(init_separator_detectors(ctx.get_allocator()))) {
    LOG_WARN("failed to init kmp detectors");
  } else if (OB_FAIL(task_controller_.init(DEFAULT_PARALLEL_THREAD_COUNT))) {
    LOG_WARN("init task controller failed", K(ret));
  } else if (OB_FAIL(complete_task_array_.init(DEFAULT_PARALLEL_THREAD_COUNT))) {
    LOG_WARN("init complete task array failed", K(ret));
  } else if (OB_ISNULL(schema_service = task_exec_ctx.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is null", K(ret));
  } else if (OB_FAIL(schema_service->get_tenant_schema_guard(
                 ctx.get_my_session()->get_effective_tenant_id(), schema_guard_))) {
    LOG_WARN("failed to get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard_.get_table_schema(load_args_.table_id_, tbschema))) {
    LOG_WARN("get table schema failed");
  } else if (OB_ISNULL(tbschema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(ret));
  } else {
    part_level_ = tbschema->get_part_level();
    part_num_ = tbschema->get_partition_num();
    ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params(&session);
    dtc_params_ = dtc_params;
  }
  return ret;
}

int ObLoadDataImpl::execute(ObExecContext& ctx, ObLoadDataStmt& load_stmt)
{
  int ret = OB_SUCCESS;

  ObPhysicalPlanCtx* plan_ctx = NULL;
  ObSQLSessionInfo* session = NULL;

  LOG_DEBUG("In load data executor!", K(load_stmt));

  // init
  if (OB_ISNULL(session = ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get session null", K(ret));
  } else if (OB_ISNULL(plan_ctx = ctx.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("physical plan ctx is null", K(ctx));
  } else if (OB_FAIL(init_everything_first(ctx, load_stmt, *session))) {
    LOG_WARN("init every thing failed");
  }

  // prepare
  if (OB_SUCC(ret)) {
    if (OB_FAIL(summarize_insert_columns())) {
      LOG_WARN("get insert columns failed", K(ret));
    } else if (OB_FAIL(build_file_field_var_hashmap())) {
      LOG_WARN("build file field hash map failed", K(ret));
    } else if (OB_FAIL(analyze_exprs_and_replace_variables(ctx.get_allocator(), *session))) {
      LOG_WARN("generate new expr for replace the old expr failed", K(ret));
    } else {
      if (part_level_ != PARTITION_LEVEL_ZERO) {
        if (OB_FAIL(init_table_location_via_fake_insert_stmt(ctx, *plan_ctx, *session))) {
          LOG_WARN("failed to init table locatition");
        }
      } else {
        // do nothing
      }
    }
  }

  // open file
  if (OB_SUCC(ret)) {
    if (OB_FAIL(reader_.open(load_args_.file_name_, false))) {
      if (OB_FILE_NOT_EXIST == ret) {

        LOG_WARN("file not exist", K(load_args_.file_name_), K(ret));
      } else {
        // TODO: handle no authority
        LOG_WARN("open file error", K(load_args_.file_name_), K(ret));
      }
    }
  }

  // allocate buffer for file read
  char* load_buf[FILE_BUFFER_NUM];
  char* load_buf_start[FILE_BUFFER_NUM];
  for (int64_t i = 0; i < FILE_BUFFER_NUM; ++i) {
    load_buf[i] = NULL;
    load_buf_start[i] = NULL;
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < FILE_BUFFER_NUM; ++i) {
    if (OB_ISNULL(load_buf[i] = static_cast<char*>(ob_malloc_align(
                      CACHE_ALIGN_SIZE, FILE_READ_BUFFER_SIZE + RESERVED_BYTES_SIZE, ObModIds::OB_SQL_EXECUTOR)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc file buffer failed", K(ret), K(i));
    } else {
      // init reserved bytes at the beginning
      for (int64_t j = 0; j < RESERVED_BYTES_SIZE; ++j) {
        load_buf[i][j] = INT8_MIN;
      }
      load_buf_start[i] = load_buf[i] + RESERVED_BYTES_SIZE;
    }
  }

  // allocate buffer for task buffers
  for (int64_t i = 0; OB_SUCC(ret) && i < DEFAULT_PARALLEL_THREAD_COUNT; ++i) {
    ObLoadbuffer* buffer = NULL;
    if (OB_FAIL(create_buffer(ctx.get_allocator(), buffer))) {
      LOG_WARN("create buffer failed", K(ret));
    } else if (OB_FAIL(complete_task_array_.push_back(buffer))) {
      LOG_WARN("push back buffer failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(server_last_freeze_ts_hashmap_.create(MAX_SERVER_COUNT, ObModIds::OB_SQL_EXECUTOR))) {
      LOG_WARN("create hash map failed", K(ret));
    }
  }
  total_timer_.start_stat();

  //  is_simple_load is TRUE in the following two situations:
  //  1. field terminator is a single char, line terminator is empty
  //  2. both line terminator and field terminator are single chars
  //  note that, for all the situations, line start string is empty
  bool is_simple_load =
      (1 == data_struct_in_file_.line_term_str_.length() && 1 == data_struct_in_file_.field_term_str_.length() &&
          0 == data_struct_in_file_.line_start_str_.length());
  LOG_INFO("LOAD DATA start info print: ",
      "table_name",
      load_args_.table_name_,
      "file_name",
      load_args_.file_name_,
      K(is_simple_load),
      "is_counting_field_mode",
      flag_line_term_by_counting_field_,
      "expected_file_column_nums",
      file_column_number_,
      "insert_column_nums",
      insert_column_number_,
      "expr_value_nums",
      set_assigned_column_number_);
  LOG_DEBUG("LOAD DATA detectors", K_(data_struct_in_file), K_(field_term_char), K_(line_term_char));

  if (OB_SUCC(ret)) {

    // init runtime variables
    int64_t read_size = 0;
    int64_t file_offset = 0;
    int64_t buf_id = 0;
    bool is_end_file = false;
    char* cur_buf = NULL;
    char* last_buf = NULL;
    char* new_data_start = NULL;
    int64_t succ_parsed_offset = 0;
    int64_t continue_offset = 0;
    int64_t expected_read_size = 0;

    cur_step_ = initial_step_;
    field_id_ = 0;
    parsed_line_count_ = 0;
    cur_line_begin_pos_ = load_buf_start[buf_id & 1] + FILE_READ_BUFFER_SIZE;
    cur_field_begin_pos_ = load_buf_start[buf_id & 1] + FILE_READ_BUFFER_SIZE;

    // main while
    while (OB_SUCC(ret) && (!is_end_file)) {
      // prepare next read buf and size
      buf_id++;
      cur_buf = load_buf_start[buf_id & 1];
      last_buf = load_buf_start[!(buf_id & 1)];
      succ_parsed_offset = cur_line_begin_pos_ - last_buf;
      continue_offset = FILE_READ_BUFFER_SIZE - succ_parsed_offset;  // remain data size in last buf

      // check if current line size > FILE_READ_BUFFER_SIZE
      if (OB_UNLIKELY(succ_parsed_offset == 0)) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("line size is overflow", K(ret));
      }

      // move the rest data in last buf to cur buf
      if (OB_SUCC(ret) && OB_LIKELY(succ_parsed_offset != FILE_READ_BUFFER_SIZE)) {
        memcpy(cur_buf, &last_buf[succ_parsed_offset], continue_offset);
      }
      file_read_timer_.start_stat();

      // read file
      expected_read_size = succ_parsed_offset;
      new_data_start = cur_buf + continue_offset;
      if (OB_SUCC(ret)) {
        if (OB_FAIL(reader_.pread(new_data_start, expected_read_size, file_offset, read_size))) {
          LOG_WARN("read file error", K(ret));
        } else {
          file_offset += read_size;
          is_end_file = (read_size != expected_read_size);
          total_buf_read_++;
          int64_t processed_MBs = (file_offset - continue_offset) / 1024 / 1024;
          LOG_INFO("LOAD DATA file read progress: ", K(processed_MBs), K_(total_buf_read), K(is_end_file));
        }
      }

      file_read_timer_.end_stat();
      parsing_execute_timer_.start_stat();

      // set runtime variables for this loop
      cur_field_begin_pos_ = cur_buf + (cur_field_begin_pos_ - cur_line_begin_pos_);
      cur_line_begin_pos_ = cur_buf;
      expr_calc_allocator_.reset_remain_one_page();  // release temp memory in batch

      // handle buf
      if (OB_SUCC(ret)) {
        if (is_simple_load) {
          ret = handle_one_file_buf_fast(ctx, *plan_ctx, new_data_start, read_size, is_end_file);
        } else {
          ret = handle_one_file_buf(ctx, *plan_ctx, new_data_start, read_size, is_end_file);
        }
        if (OB_FAIL(ret)) {
          LOG_WARN("failed to handle one file buffer", K(ret), K(is_simple_load));
        }
      }
      parsing_execute_timer_.end_stat();

      if (OB_SUCC(ret)) {
        if (OB_FAIL(ObLoadDataUtils::check_session_status(*session))) {
          LOG_WARN("session is invalid failed during waiting for tenant memory available ", K(ret));
        }
      }
    }  // end while

    // send non-full buffer
    if (OB_SUCC(ret)) {
      if (OB_FAIL(send_all_buffer_finally(ctx, *plan_ctx))) {
        LOG_WARN("send all buffer failed", K(ret));
      }
    }

    // waiting unfinished task
    do {
      int temp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (temp_ret = (wait_all_task_finished(*plan_ctx)))) {
        if (OB_SUCCESS == ret) {
          ret = temp_ret;
        }
        LOG_ERROR("wait unfinished task failed", K(temp_ret));
      }
    } while (0);

    // release buffer
    destroy_all_buffer();

    plan_ctx->set_row_matched_count(parsed_line_count_);
    int64_t total_partition_cnt = part_buffer_map_.count();
    int64_t total_task_cnt = task_controller_.get_total_task_cnt();
    LOG_INFO("LOAD DATA finish report: ",
        K_(parsed_line_count),
        K(total_partition_cnt),
        K_(allocated_buffer_cnt),
        K(total_task_cnt),
        K_(total_wait_secs));
    if (OB_SUCC(ret)) {
      LOG_INFO("LOAD DATA execute succ");
    } else {
      LOG_WARN("LOAD DATA execute error, data has changed please check dump file", K(ret));
    }
  }  // end if
  total_timer_.end_stat();

#ifdef TIME_STAT_ON
  LOG_INFO("LOAD DATA timer report: ",
      K_(total_timer),
      K_(file_read_timer),
      K_(parsing_execute_timer),
      K_(rpc_timer),
      K_(calc_timer),
      K_(buffer_copy_timer),
      K_(serialize_timer),
      K_(wait_task_timer),
      K_(handle_result_timer));
#endif
  // free load buf
  for (int64_t i = 0; i < FILE_BUFFER_NUM; ++i) {
    if (NULL != load_buf[i]) {
      ob_free_align(load_buf[i]);
    }
  }

  return ret;
}

int ObLoadDataImpl::create_part_buffer_ctrl(int64_t part_id, ObIAllocator* allocator, ObPartitionBufferCtrl*& buf_mgr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(part_id < 0 || OB_ISNULL(allocator) || OB_NOT_NULL(buf_mgr))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(part_id), K(allocator));
  } else {
    // new partition, create a new buff info
    ObPartitionBufferCtrl* new_buf_ctrl = NULL;
    int hash_ret = OB_SUCCESS;
    if (OB_ISNULL(new_buf_ctrl = OB_NEWx(ObPartitionBufferCtrl, allocator, part_id))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate bufferinfo failed", K(ret));
    } else {
      new_buf_ctrl->part_id_ = part_id;
      if (OB_SUCCESS != (hash_ret = part_buffer_map_.set_refactored(new_buf_ctrl))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected hash ret", K(ret), K(hash_ret));
        new_buf_ctrl->~ObPartitionBufferCtrl();
        allocator->free(new_buf_ctrl);
      }
      buf_mgr = new_buf_ctrl;
      LOG_DEBUG("new buffer info created!", KPC(new_buf_ctrl));
    }
  }
  return ret;
}

int ObLoadDataImpl::create_buffer(ObIAllocator& allocator, ObLoadbuffer*& buffer)
{
  // check buff info, assign load buffer
  int ret = OB_SUCCESS;
  ObLoadbuffer* newbuffer = NULL;
  if (OB_ISNULL(newbuffer = OB_NEWx(ObLoadbuffer, (&allocator)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate ObLoadbuffer failed", K(ret));
  } else {
    newbuffer->set_table_name(back_quoted_db_table_name_);
    newbuffer->set_table_id(load_args_.table_id_);
    newbuffer->set_tenant_id(load_args_.tenant_id_);
    newbuffer->set_column_num(insert_column_number_);
    newbuffer->set_load_mode(load_args_.dupl_action_);
    if (OB_FAIL(newbuffer->set_allocator_tenant())) {
      LOG_WARN("fail to set tenant id", K(ret));
    } else if (OB_FAIL(newbuffer->prepare_insert_info(insert_column_names_, expr_value_bitset_))) {
      LOG_WARN("set column names failed", K(ret));
    } else if (OB_FAIL(newbuffer->init_array())) {
      LOG_WARN("init array failed", K(ret), K(newbuffer));
    } else if (OB_FAIL(allocated_buffer_store_.push_back(newbuffer))) {
      LOG_WARN("failed to store buffer", K(ret));
      newbuffer->~ObLoadbuffer();
      allocator.free(newbuffer);
    } else {
      buffer = newbuffer;
      allocated_buffer_cnt_++;
      LOG_DEBUG("new data buffer created!", KPC(newbuffer));
    }
  }
  return ret;
}

int ObInsertValueGenerator::replace_value_for_file_col_expr(const common::ObIArray<ObString>& file_col_values) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs_ref_file_col_.count(); ++i) {
    const ObLoadDataReplacedExprInfo& info = exprs_ref_file_col_.at(i);
    if (OB_UNLIKELY(info.correspond_file_field_idx < 0 || info.correspond_file_field_idx > file_col_values.count()) ||
        OB_ISNULL(info.replaced_expr)) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      ObObj obj;
      obj.set_string(ObVarcharType, file_col_values.at(info.correspond_file_field_idx));
      obj.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
      info.replaced_expr->set_value(obj);
    }
  }
  return ret;
}

int ObInsertValueGenerator::gen_values(const ObIArray<ObString>& file_col_values,
    ObIArray<ObString>& table_column_values, ObExecContext& exec_ctx, ObLoadFileBuffer& data_buf) const
{
  int ret = OB_SUCCESS;

  data_buf.reset();  // buf for generate expr values

  if (OB_SUCC(ret) && exprs_ref_file_col_.count() > 0) {
    if (OB_FAIL(replace_value_for_file_col_expr(file_col_values))) {
      LOG_WARN("fail to set values", K(ret));
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < table_column_value_desc_.count(); ++i) {
    const ObLoadTableColumnDesc& desc = table_column_value_desc_.at(i);
    if (!desc.is_set_values_) {
      if (OB_UNLIKELY(desc.array_ref_idx_ < 0 || desc.array_ref_idx_ >= file_col_values.count())) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        table_column_values.at(i) = file_col_values.at(desc.array_ref_idx_);
      }
    } else {
      if (OB_ISNULL(desc.expr_value_)) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        int64_t len = 0;
        ObRawExprPrinter expr_printer(
            data_buf.current_ptr(), data_buf.get_remain_len(), &len, TZ_INFO(GET_MY_SESSION(exec_ctx)));
        if (OB_FAIL(expr_printer.do_print(desc.expr_value_, T_NONE_SCOPE))) {
          LOG_WARN("fail to print expr");
        } else {
          table_column_values.at(i) = ObString(len, data_buf.current_ptr());
          data_buf.update_pos(len);
        }
      }
    }
  }

  return ret;
}

bool ObInsertValueGenerator::find_table_column_value_desc_by_column_id(const uint64_t column_id, int64_t& idx)
{
  bool found = false;
  for (int64_t i = 0; !found && i < table_column_value_desc_.count(); ++i) {
    if (table_column_value_desc_.at(i).column_id_ == column_id) {
      idx = i;
      found = true;
    }
  }
  return found;
}

int ObInsertValueGenerator::gen_insert_columns_names_buff(
    ObExecContext& ctx, const ObLoadArgument& load_args, ObString& data_buff)
{
  int ret = OB_SUCCESS;

  ObSqlString insert_stmt;

  ObSEArray<ObString, 16> insert_column_names;
  if (OB_FAIL(insert_column_names.reserve(table_column_value_desc_.count()))) {
    LOG_WARN("fail to reserve", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < table_column_value_desc_.count(); ++i) {
    if (OB_FAIL(insert_column_names.push_back(table_column_value_desc_[i].column_name_))) {
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
    if (OB_FAIL(ObLoadDataUtils::build_insert_sql_string_head(
            load_args.dupl_action_, load_args.combined_name_, insert_column_names, insert_stmt))) {
      LOG_WARN("gen insert sql column_names failed", K(ret));
    } else if (OB_FAIL(ob_write_string(ctx.get_allocator(), insert_stmt.string(), data_buff))) {
      LOG_WARN("fail to write string", K(ret));
    }
  }

  return ret;
}

int ObPartIdCalculator::init(ObExecContext& ctx, const ObString& q_name, const uint64_t& table_id,
    ObInsertValueGenerator& generator, ObLoadFileBuffer& data_buf, ObIArray<ObString>& insert_values,
    ObIArray<ObString>& file_col_values)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  ObSQLSessionInfo* session_info = NULL;
  ObSchemaGetterGuard* schema_guard = NULL;
  ParamStore ps1(ObWrapperAllocator(ctx.get_allocator()));
  ParamStore ps2(ObWrapperAllocator(ctx.get_allocator()));
  ParamStore* param_store[2] = {&ps1, &ps2};
  ObDMLStmt* insert_stmt[2] = {NULL};
  char identity_char[2] = {'f', 'F'};
  ObSqlString insert_sql;
  data_buf.reset();

  if (OB_ISNULL(plan_ctx = GET_PHY_PLAN_CTX(ctx)) || OB_ISNULL(session_info = GET_MY_SESSION(ctx)) ||
      OB_ISNULL(ctx.get_sql_ctx()) || OB_ISNULL(schema_guard = ctx.get_sql_ctx()->schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx not init", K(ret));
  }

  // 1. generate two insert stmt
  for (int64_t i = 0; OB_SUCC(ret) && i < 2; ++i) {
    insert_sql.reuse();
    if (OB_FAIL(ObLoadDataBase::generate_fake_field_strs(file_col_values, ctx.get_allocator(), identity_char[i]))) {
      LOG_WARN("generate fake field failed", K(ret));
    } else if (OB_FAIL(generator.gen_values(file_col_values, insert_values, ctx, data_buf))) {
      LOG_WARN("fail to gen values", K(ret));
    } else if (OB_FAIL(ObLoadDataBase::construct_insert_sql(
                   insert_sql, q_name, generator.get_table_column_value_descs(), insert_values, 1))) {
      LOG_WARN("construct insert sql failed", K(ret));
    } else if (OB_FAIL(ObLoadDataBase::make_parameterize_stmt(ctx, insert_sql, *param_store[i], insert_stmt[i]))) {
      LOG_WARN("make parameterize statement failed", K(ret));
    }
  }
  // 2. calc param store mapping to input value
  if (OB_SUCC(ret)) {
    if (param_store[0]->count() != param_store[1]->count() || OB_ISNULL(insert_stmt[0]) || OB_ISNULL(insert_stmt[1])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("parser error", K(ret), K(insert_stmt[0]), K(insert_stmt[1]));
    } else {
      ParamStore& runtime_param_store = plan_ctx->get_param_store_for_update();
      int64_t param_store_size = param_store[0]->count();
      int64_t idx = OB_INVALID_INDEX_INT64;
      for (int64_t i = 0; OB_SUCC(ret) && i < param_store_size; ++i) {
        if (OB_FAIL(ObLoadDataBase::calc_param_offset(param_store[0]->at(i), param_store[1]->at(i), idx))) {
          LOG_WARN("calc param offset failed", K(ret));
        } else if (OB_FAIL(param_to_file_col_idx_.push_back(idx))) {
          LOG_WARN("push back failed", K(ret));
        } else if (OB_FAIL(runtime_param_store.push_back(param_store[0]->at(i)))) {
          LOG_WARN("push back param store failed", K(ret));
        }
      }
      LOG_DEBUG("load data runtime params", K(ret), K(runtime_param_store), K(param_to_file_col_idx_));
    }
  }
  // 3. init table location
  if (OB_SUCC(ret)) {
    ObSEArray<ObRawExpr*, 1> filter_exprs;
    ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params(session_info);
    if (OB_FAIL(table_location_.init(*schema_guard,
            *(insert_stmt[0]),
            ctx.get_my_session(),
            filter_exprs,
            table_id,
            table_id,
            NULL,
            dtc_params,
            true))) {
      LOG_WARN("fail to init table location", K(ret));
    }
  }

  return ret;
}

int ObPartIdCalculator::calc_partition_id(
    ObExecContext& ctx, ObSchemaGetterGuard& schema_guard, const ObIArray<ObString>& insert_values, int64_t& part_id)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  ObSQLSessionInfo* session_info = NULL;

  ObSEArray<int64_t, 1, ObNullAllocator> partition_ids;

  if (OB_ISNULL(plan_ctx = GET_PHY_PLAN_CTX(ctx)) || OB_ISNULL(session_info = GET_MY_SESSION(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx not init", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < param_to_file_col_idx_.count(); ++i) {
    int64_t field_offset = param_to_file_col_idx_.at(i);
    if (OB_INVALID_INDEX_INT64 == field_offset) {
      // do nothing
    } else if (OB_UNLIKELY(field_offset >= insert_values.count())) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      ObLoadDataBase::set_one_field_objparam(
          plan_ctx->get_param_store_for_update().at(i), insert_values.at(field_offset));
    }
  }

  partition_ids.reuse();
  ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params(session_info);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_location_.calculate_partition_ids(
            ctx, &schema_guard, plan_ctx->get_param_store_for_update(), partition_ids, dtc_params))) {
      LOG_WARN("calc partition ids failed", K(ret));
    } else {
      part_id = partition_ids.at(0);
    }
  }

  return ret;
}

int ObLoadDataSPImpl::recursively_replace_variables(
    ObExecContext& ctx, ObLoadDataStmt& load_stmt, ObInsertValueGenerator& generator, ObRawExpr*& raw_expr)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo* session = NULL;
  ObRawExprFactory* expr_factory = NULL;
  bool is_user_variable = false;
  if (OB_ISNULL(raw_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K((ret)));
  } else if (OB_ISNULL(session = ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (OB_ISNULL(expr_factory = ctx.get_expr_factory())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr factory is null", K(ret));
  } else if (raw_expr->get_expr_type() == T_REF_COLUMN || raw_expr->get_expr_type() == T_OP_GET_USER_VAR) {
    // 1. get variable name
    ObString ref_name;
    if (raw_expr->get_expr_type() == T_REF_COLUMN) {
      ObColumnRefRawExpr* column_ref = static_cast<ObColumnRefRawExpr*>(raw_expr);
      ref_name = column_ref->get_column_name();
    } else {
      is_user_variable = true;
      ObSysFunRawExpr* func_expr = static_cast<ObSysFunRawExpr*>(raw_expr);
      if (func_expr->get_children_count() != 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sys func expr child num is not correct", K(ret));
      } else {
        ObConstRawExpr* c_expr = static_cast<ObConstRawExpr*>(func_expr->get_param_expr(0));
        if (c_expr->get_value().get_type() != ObVarcharType) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("const expr child type is not correct", K(ret));
        } else {
          ref_name = c_expr->get_value().get_string();
        }
      }
    }
    // 2. replace it
    bool need_replaced_to_loaded_data_from_file = false;
    ObConstRawExpr* c_expr = NULL;  // build a new const expr
    int64_t idx = OB_INVALID_INDEX;

    if (OB_SUCC(ret)) {
      for (int64_t i = 0; i < load_stmt.get_field_or_var_list().count(); ++i) {
        if (0 == load_stmt.get_field_or_var_list().at(i).field_or_var_name_.compare(ref_name)) {
          idx = i;
          break;
        }
      }

      if (OB_INVALID_INDEX != idx) {
        if (OB_FAIL(ObRawExprUtils::build_const_string_expr(*expr_factory,
                ObVarcharType,
                ObString(),
                ObCharset::get_default_collation(ObCharset::get_default_charset()),
                c_expr))) {
          LOG_WARN("fail to build const string expr", K(ret));
        } else {
          need_replaced_to_loaded_data_from_file = true;
        }
      } else {
        if (!is_user_variable) {
          // do not replace this param, stay the column name as it is
          // need_replaced_to_loaded_data_from_file = false
          LOG_WARN("unknown column name in set right expr, do nothing");
        } else {
          // find the real value from session
          if (OB_ISNULL(c_expr = OB_NEWx(ObConstRawExpr, (&ctx.get_allocator())))) {
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
              need_replaced_to_loaded_data_from_file = true;
            }
          }
        }
      }
    }

    if (OB_SUCC(ret) && need_replaced_to_loaded_data_from_file) {
      LOG_DEBUG("replace variable name to field value", K(ref_name), K(idx), K(*raw_expr), K(*c_expr));
      raw_expr = c_expr;
      ObLoadDataReplacedExprInfo varable_info;
      varable_info.replaced_expr = c_expr;
      varable_info.correspond_file_field_idx = idx;
      if (OB_FAIL(generator.add_file_column_replace_info(varable_info))) {
        LOG_WARN("push back replaced variable infos array failed", K(ret));
      }
    }

  } else {
    // go recursively
    int64_t child_cnt = raw_expr->get_param_count();
    for (int64_t i = 0; OB_SUCC(ret) && i < child_cnt; ++i) {
      ObRawExpr*& child_expr = raw_expr->get_param_expr(i);
      if (OB_FAIL(recursively_replace_variables(ctx, load_stmt, generator, child_expr))) {
        LOG_WARN("replace reference column failed", K(ret));
      }
    }  // end for
  }
  return ret;
}

int ObLoadDataSPImpl::build_insert_values_generator(
    ObExecContext& ctx, ObLoadDataStmt& load_stmt, ObInsertValueGenerator& generator)
{
  int ret = OB_SUCCESS;

  // e.g. general stmt like "INTO TABLE t1 (c1, c2, @a, @b) SET c3 = @a + @b"
  // step 1: add c1 and c2
  //     the first column of file will be written to t1.c1, so c1 will be added to the generator
  //     similarly, the second column to t1.c2 which also will be added to the generator
  // step 2: add c3 (calced by the first assign)
  //     @a, @b is not match column name, but their data will produce c3 by the "SET" clause,
  //     in result, c3 will be added
  //     in addition, replace expr @a with a const string expr which refer to a column from file
  //     do the same replace to @b

  // step 1
  for (int64_t i = 0; OB_SUCC(ret) && i < load_stmt.get_field_or_var_list().count(); ++i) {
    ObLoadDataStmt::FieldOrVarStruct& item = load_stmt.get_field_or_var_list().at(i);
    if (item.is_table_column_) {
      ObLoadTableColumnDesc tmp_info;
      tmp_info.is_set_values_ = false;
      tmp_info.column_name_ = item.field_or_var_name_;
      tmp_info.column_id_ = item.column_id_;
      tmp_info.column_type_ = item.column_type_;
      tmp_info.array_ref_idx_ = i;  // array offset
      if (OB_FAIL(generator.add_table_column_value_desc(tmp_info))) {
        LOG_WARN("push str failed", K(ret));
      }
    } else {
      // do nothing
      // ignore variables temporarily
    }
  }

  // step 2
  for (int64_t i = 0; OB_SUCC(ret) && i < load_stmt.get_table_assignment().count(); ++i) {
    const ObAssignment& assignment = load_stmt.get_table_assignment().at(i);
    ObColumnRefRawExpr* left = assignment.column_expr_;
    ObRawExpr* right = assignment.expr_;
    if (OB_ISNULL(left)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("set assign expr is null", K(ret));
    } else if (OB_FAIL(recursively_replace_variables(ctx, load_stmt, generator, right))) {
      LOG_WARN("fail to recursive replace variables", K(ret));
    } else {
      int64_t found_index = OB_INVALID_INDEX_INT64;
      if (generator.find_table_column_value_desc_by_column_id(left->get_column_id(), found_index)) {
        // overwrite
        ObLoadTableColumnDesc& tmp_info = generator.get_table_column_value_descs().at(found_index);
        tmp_info.is_set_values_ = true;
        tmp_info.array_ref_idx_ = OB_INVALID_INDEX_INT64;
        tmp_info.expr_value_ = right;
      } else {
        // a new insert column is defined by set expr
        ObLoadTableColumnDesc tmp_info;
        tmp_info.column_name_ = left->get_column_name();
        tmp_info.column_id_ = left->get_column_id();
        tmp_info.column_type_ = left->get_result_type().get_type();
        tmp_info.is_set_values_ = true;
        tmp_info.expr_value_ = right;
        if (OB_FAIL(generator.add_table_column_value_desc(tmp_info))) {
          LOG_WARN("push str failed", K(ret));
        }
      }
    }
  }

  return ret;
}

void ObCSVFormats::init(const ObDataInFileStruct& file_formats)
{
  field_term_char_ = file_formats.field_term_str_.empty() ? INT64_MAX : file_formats.field_term_str_[0];
  line_term_char_ = file_formats.line_term_str_.empty() ? INT64_MAX : file_formats.line_term_str_[0];
  enclose_char_ = file_formats.field_enclosed_char_;
  escape_char_ = file_formats.field_escaped_char_;
  null_column_fill_zero_string_ = share::is_mysql_mode();

  if (!file_formats.field_term_str_.empty() && file_formats.line_term_str_.empty()) {
    is_line_term_by_counting_field_ = true;
    field_term_char_ = line_term_char_;
  }
  is_simple_format_ = !is_line_term_by_counting_field_ && (field_term_char_ != INT64_MAX) &&
                      (line_term_char_ != INT64_MAX) && (field_term_char_ != line_term_char_) &&
                      (enclose_char_ == INT64_MAX);
}

ObShuffleTaskHandle::ObShuffleTaskHandle(
    ObInsertValueGenerator& main_generator, ObPartIdCalculator& main_calculator, ObDataFragMgr& main_datafrag_mgr)
    : exec_ctx(GCTX.session_mgr_),
      data_buffer(NULL),
      generator(main_generator),
      calculator(main_calculator),
      datafrag_mgr(main_datafrag_mgr)
{
  void* buf = NULL;
  buf = ob_malloc(ObLoadFileBuffer::MAX_BUFFER_SIZE, ObModIds::OB_SQL_LOAD_DATA);
  if (OB_NOT_NULL(buf)) {
    data_buffer = new (buf) ObLoadFileBuffer(ObLoadFileBuffer::MAX_BUFFER_SIZE - sizeof(ObLoadFileBuffer));
  }
}

ObShuffleTaskHandle::~ObShuffleTaskHandle()
{
  if (OB_NOT_NULL(data_buffer)) {
    ob_free(data_buffer);
  }
}

int ObLoadDataSPImpl::exec_shuffle(int64_t task_id, ObShuffleTaskHandle* handle)
{
  int ret = OB_SUCCESS;
  int64_t tenant_id = OB_INVALID_TENANT_ID;
  void* expr_buf = NULL;
  ObLoadFileBuffer* expr_buffer = NULL;
  ObArrayHashMap<ObPartitionKey, ObDataFrag*> part_buf_mgr;
  ObSEArray<ObString, 32> insert_values;
  int64_t parsed_line_num = 0;

  auto save_frag = [&](ObPartitionKey part_key, ObDataFrag* frag) -> bool {
    // store full frag into frag_mgr
    int ret = OB_SUCCESS;
    ObPartDataFragMgr* part_datafrag_mgr = NULL;
    if (OB_FAIL(handle->datafrag_mgr.get_part_datafrag(part_key.get_partition_id(), part_datafrag_mgr))) {
      LOG_WARN("fail to get part datafrag", K(ret));
    } else if (OB_ISNULL(part_datafrag_mgr)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(part_datafrag_mgr->queue_.push(frag))) {
      LOG_WARN("fail to push frag", K(ret));
    } else {
      ATOMIC_AAF(&(part_datafrag_mgr->total_row_proceduced_), frag->row_cnt);
      LOG_DEBUG("saving frag", K(part_key.get_partition_id()), K(*frag));
    }
    return OB_SUCCESS == ret;
  };

  auto free_frag = [&](ObPartitionKey part_key, ObDataFrag* frag) -> bool {
    UNUSED(part_key);
    if (OB_NOT_NULL(frag)) {
      handle->datafrag_mgr.distory_datafrag(frag);
    }
    return true;
  };

  ((ObArenaAllocator*)(&handle->exec_ctx.get_allocator()))->revert_tracer();

  if (OB_ISNULL(handle) || OB_ISNULL(handle->data_buffer) || OB_ISNULL(handle->exec_ctx.get_my_session())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP(handle));
    //  } else if (FALSE_IT(handle->exec_ctx.get_allocator().reuse())) {
  } else if (OB_FAIL(part_buf_mgr.init(ObModIds::OB_SQL_LOAD_DATA, handle->datafrag_mgr.get_total_part_cnt()))) {
    LOG_WARN("fail to init part buf mgr", K(ret));
  } else if (OB_FAIL(insert_values.prepare_allocate(handle->generator.get_table_column_value_descs().count()))) {
    LOG_WARN(
        "fail to prealloc", K(ret), "insert values count", handle->generator.get_table_column_value_descs().count());
  } else if (FALSE_IT(tenant_id = handle->exec_ctx.get_my_session()->get_effective_tenant_id())) {
  } else if (OB_ISNULL(expr_buf = ob_malloc(
                           ObLoadFileBuffer::MAX_BUFFER_SIZE, ObMemAttr(tenant_id, ObModIds::OB_SQL_LOAD_DATA)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("not enough memory", K(ret));
  } else {
    handle->err_records.reuse();
    expr_buffer = new (expr_buf) ObLoadFileBuffer(ObLoadFileBuffer::MAX_BUFFER_SIZE - sizeof(ObLoadFileBuffer));
    handle->parser.reuse();
    handle->parser.next_buf(handle->data_buffer->begin_ptr(), handle->data_buffer->get_data_len(), true);
    bool yield = true;
    while (OB_SUCC(ret) && yield) {
      int temp_ret = handle->parser.next_line(yield);
      if (OB_UNLIKELY(OB_SUCCESS != temp_ret)) {
        ObParserErrRec rec;
        rec.row_offset_in_task = parsed_line_num;
        rec.ret = temp_ret;
        int push_ret = handle->err_records.push_back(rec);
        if (OB_ERR_UNEXPECTED == temp_ret) {
          ret = temp_ret;
        } else if (OB_SUCCESS != push_ret) {
          ret = push_ret;
        }
      }
      if (OB_SUCC(ret) && yield) {
        int64_t cur_line_num = parsed_line_num++;
        // calc partition id
        int64_t part_id = 0;
        if (OB_FAIL(handle->generator.gen_values(
                handle->parser.get_line_store(), insert_values, handle->exec_ctx, *expr_buffer))) {
          LOG_WARN("fail to gen values", K(ret));
        } else if (OB_FAIL(handle->calculator.calc_partition_id(
                       handle->exec_ctx, handle->schema_guard, insert_values, part_id))) {
          LOG_WARN("fail to calc partition id", K(ret));
        }
        LOG_DEBUG("LOAD DATA",
            "TheadId",
            get_tid_cache(),
            K(cur_line_num),
            K(part_id),
            "line",
            handle->parser.get_line_store(),
            "values",
            insert_values);

        // serialize to DataFrag
        int64_t len = 0;
        OB_UNIS_ADD_LEN(insert_values);
        OB_UNIS_ADD_LEN(cur_line_num);
        int64_t row_ser_size = len;
        OB_UNIS_ADD_LEN(row_ser_size);

        ObDataFrag* frag = NULL;
        if (OB_SUCC(ret)) {
          ObPartitionKey part_key(handle->calculator.get_table_id(), part_id, 0);
          int temp_ret = part_buf_mgr.get(part_key, frag);
          bool frag_exist = (OB_SUCCESS == temp_ret);
          if (!frag_exist || len > frag->get_remain()) {
            ObDataFrag* new_frag = NULL;
            if (OB_FAIL(handle->datafrag_mgr.create_datafrag(new_frag, len))) {
              LOG_WARN("fail to create data fragment", K(ret));
            } else {
              if (frag_exist) {
                if (OB_UNLIKELY(!save_frag(part_key, frag))) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("fail to save frag", K(ret));
                } else if (OB_FAIL(part_buf_mgr.update(part_key, new_frag))) {
                  // never goes here
                  LOG_ERROR("fail to install new frag", K(ret));
                }
              } else {
                if (OB_FAIL(part_buf_mgr.insert(part_key, new_frag))) {
                  LOG_ERROR("fail to insert new frag", K(ret));
                }
              }
              if (OB_SUCC(ret)) {
                frag = new_frag;
                frag->shuffle_task_id = task_id;
              } else {
                handle->datafrag_mgr.distory_datafrag(frag);
              }
            }
          }
        }

        if (OB_SUCC(ret)) {
          char* buf = frag->get_current();
          int64_t buf_len = frag->get_remain();
          int64_t pos = 0;
          OB_UNIS_ENCODE(row_ser_size);
          OB_UNIS_ENCODE(cur_line_num);
          OB_UNIS_ENCODE(insert_values);
          if (OB_SUCC(ret)) {
            frag->add_pos(pos);
            frag->add_row_cnt(1);
          }
        }
      }  // end if yield
    }    // end while

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

int ObLoadDataSPImpl::exec_insert(ObInsertTask& task, ObInsertResult& result)
{
  UNUSED(result);
  int ret = OB_SUCCESS;
  int64_t sql_buff_len_init = OB_MALLOC_BIG_BLOCK_SIZE;  // 2M
  int64_t field_buf_len = OB_MAX_VARCHAR_LENGTH;
  char* field_buff = NULL;
  ObSqlString sql_str(ObModIds::OB_SQL_LOAD_DATA);
  ObSEArray<ObString, 1> single_row_values;
  ObMemAttr attr(task.tenant_id_, ObModIds::OB_SQL_LOAD_DATA);

#ifdef TEST_MODE
  delay_process_by_probability(INSERT_TASK_DROP_RATE);
#endif

  if (OB_ISNULL(field_buff = static_cast<char*>(ob_malloc(field_buf_len, attr)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to ob malloc", K(ret), K(field_buf_len));
  }
  OZ(single_row_values.reserve(task.column_count_));
  OZ(sql_str.extend(sql_buff_len_init));
  OZ(sql_str.append(task.insert_stmt_head_));
  OZ(sql_str.append(ObString(" values ")));

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
      if (OB_SUCC(ret) && (pos - pos_back != row_ser_size || single_row_values.count() != task.column_count_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("row size is not as expected", "pos diff", pos - pos_back, K(row_ser_size));
      }

      // print row
      if (deserialized_rows != 0) {
        OZ(sql_str.append(",", 1));
      }
      OZ(sql_str.append("(", 1));
      for (int64_t c = 0; OB_SUCC(ret) && c < single_row_values.count(); ++c) {
        bool is_string_column = !task.non_string_fields_.has_member(c);
        if (c != 0) {
          OZ(sql_str.append(",", 1));
        }
        if (ObLoadDataUtils::is_null_field(single_row_values[c])) {
          OZ(sql_str.append(ObString(ObLoadDataUtils::NULL_STRING)));
        } else {
          if (is_string_column) {
            OZ(sql_str.append("'", 1));
          }
          int64_t field_data_pos = ObHexEscapeSqlStr(single_row_values[c]).to_string(field_buff, field_buf_len);
          OZ(sql_str.append(ObString(field_data_pos, field_buff)));
          if (is_string_column) {
            OZ(sql_str.append("'", 1));
          }
        }
      }
      OZ(sql_str.append(")", 1));

      deserialized_rows++;
    }

  }  // end for

  if (OB_SUCC(ret) && deserialized_rows != task.row_count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data in task not match deserialized result", K(ret), K(deserialized_rows), K(task.row_count_));
  }

  int64_t affected_rows = 0;

  int insert_ret = OB_SUCCESS;
  int retry_times = 0;  // retry in remote
  do {
    OX(insert_ret = GCTX.sql_proxy_->write(task.tenant_id_, sql_str.ptr(), affected_rows, get_compatibility_mode()));
    if (OB_UNLIKELY(insert_ret != OB_SUCCESS)) {
      LOG_WARN("fail to exec insert remote", K(insert_ret), "task_id", task.task_id_, K(retry_times));
    }
  } while (OB_UNLIKELY(OB_AUTOINC_SERVICE_BUSY == insert_ret) && (retry_times++ < 3) && !FALSE_IT(usleep(10 * 1000)));

  LOG_DEBUG("LOAD DATA remote process", K(affected_rows), K(task.task_id_), K(ret));

  if (OB_SUCCESS != insert_ret) {
    ret = insert_ret;
  }

#ifdef TEST_MODE
  delay_process_by_probability(INSERT_TASK_DROP_RATE);
#endif

  if (OB_NOT_NULL(field_buff)) {
    ob_free(field_buff);
  }

  return ret;
}

int ObLoadDataSPImpl::wait_shuffle_task_return(ToolBox& box)
{
  int ret = OB_SUCCESS;
  int ret_bak = OB_SUCCESS;
  for (int64_t i = 0; i < box.parallel; ++i) {
    ObShuffleTaskHandle* handle = NULL;
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
    } else if (handle->err_records.count() > 0 && OB_FAIL(handle_returned_shuffle_task(box, *handle))) {
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
    ObShuffleTaskHandle* handle = box.shuffle_resource[i];
    if (OB_FAIL(box.shuffle_task_controller.on_task_finished())) {
      LOG_WARN("fail to on next task", K(ret));
    } else if (OB_FAIL(box.shuffle_task_reserve_queue.push_back(handle))) {
      LOG_WARN("fail to push back", K(ret));
    } else if (OB_ISNULL(handle)) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      handle->result.reset();
    }
  }

  return ret;
}

int ObLoadDataSPImpl::handle_returned_shuffle_task(ToolBox& box, ObShuffleTaskHandle& handle)
{
  UNUSED(box);
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(handle.result.task_id_ >= box.file_buf_row_num.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid array index", K(ret), K(handle.result.task_id_), K(box.file_buf_row_num.count()));
  } else if (!box.file_appender.is_opened() && OB_FAIL(create_log_file(box))) {
    LOG_WARN("fail to create log file", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < handle.err_records.count(); ++i) {
    int64_t line_num = box.file_buf_row_num.at(handle.result.task_id_) + handle.err_records.at(i).row_offset_in_task;
    if (OB_FAIL(log_failed_line(box, "shuffle_task", handle.result.task_id_, line_num, handle.err_records.at(i).ret))) {
      LOG_WARN("fail to log failed line", K(ret));
    }
  }

  return ret;
}

int ObLoadDataSPImpl::next_file_buffer(ToolBox& box, ObLoadFileBuffer& data_buffer, int64_t limit)
{
  int ret = OB_SUCCESS;

  OZ(box.data_trimer.recover_incomplate_data(data_buffer));

  if (ObLoadFileLocation::SERVER_DISK == box.load_file_storage) {
    OZ(box.file_reader.pread(data_buffer.current_ptr(),
        data_buffer.get_remain_len(),
        box.read_cursor.file_offset_,
        box.read_cursor.read_size_));
  } else {
#ifdef _WITH_OSS
    OZ(box.oss_reader.pread(data_buffer.current_ptr(),
        data_buffer.get_remain_len(),
        box.read_cursor.file_offset_,
        box.read_cursor.read_size_));
#else
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported file storage", K(ret), K(box.load_file_storage));
#endif
  }

  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(0 == box.read_cursor.read_size_)) {
      box.read_cursor.is_end_file_ = true;
      LOG_DEBUG("LOAD DATA reach file end", K(box.read_cursor));
    } else {
      data_buffer.update_pos(box.read_cursor.read_size_);
      int64_t last_proccessed_GBs = box.read_cursor.get_total_read_GBs();
      box.read_cursor.commit_read();
      int64_t processed_GBs = box.read_cursor.get_total_read_GBs();
      if (processed_GBs != last_proccessed_GBs) {
        LOG_INFO("LOAD DATA file read progress: ", K(processed_GBs));
      }
    }
  }

  if (OB_SUCC(ret) && OB_LIKELY(data_buffer.is_valid())) {
    int64_t complete_cnt = limit;
    int64_t complete_len = 0;
    if (OB_FAIL(ObCSVParser::fast_parse_lines(
            data_buffer, box.parser, box.read_cursor.is_end_file(), complete_len, complete_cnt))) {
      LOG_WARN("fail to fast_lines_parse", K(ret));
    } else if (OB_FAIL(box.data_trimer.backup_incomplate_data(data_buffer, complete_len))) {
      LOG_WARN("fail to back up data", K(ret));
    } else {
      box.data_trimer.commit_line_cnt(complete_cnt);
      LOG_DEBUG("LOAD DATA", "backup", box.data_trimer.get_incomplate_data_string());
    }
  }
  return ret;
}

int ObLoadDataSPImpl::shuffle_task_gen_and_dispatch(ObExecContext& ctx, ToolBox& box)
{
  UNUSED(ctx);
  int ret = OB_SUCCESS;
  ObShuffleTaskHandle* handle = nullptr;
  int64_t task_id = 0;

  for (int64_t i = 0; OB_SUCC(ret) && !box.read_cursor.is_end_file() && i < box.data_frag_mem_usage_limit; ++i) {

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
    } else if (OB_UNLIKELY(handle->err_records.count() > 0) && OB_FAIL(handle_returned_shuffle_task(box, *handle))) {
      LOG_WARN("handle returned shuffle task", K(ret));
    } else {
      box.suffle_rt_sum += handle->result.process_us_;
      task_id = box.shuffle_task_controller.get_next_task_id();
      handle->data_buffer->reset();
      handle->result = ObShuffleResult();
      handle->result.task_id_ = task_id;
      handle->err_records.reuse();
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(box.file_buf_row_num.push_back(box.data_trimer.get_lines_count()))) {
        LOG_WARN("fail to push back", K(ret));
      } else if (OB_FAIL(next_file_buffer(box, *(handle->data_buffer)))) {
        LOG_WARN("fail get next file buffer", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      ObRpcLoadDataShuffleTaskCallBack mycallback(box.shuffle_task_controller, box.shuffle_task_reserve_queue, handle);

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

int ObLoadDataSPImpl::create_log_file(ToolBox& box)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(box.file_appender.open(box.log_file_name, false, true))) {
    LOG_WARN("fail to open file", K(ret), K(box.log_file_name));
  } else if (OB_FAIL(box.file_appender.append(box.load_info.ptr(), box.load_info.length(), false))) {
    LOG_WARN("fail to append file", K(ret));
  } else if (OB_FAIL(box.file_appender.append(log_file_column_names, strlen(log_file_column_names), false))) {
    LOG_WARN("fail to append file", K(ret));
  }
  return ret;
}

int ObLoadDataSPImpl::log_failed_line(
    ToolBox& box, const char* task_type, int64_t task_id, int64_t line_num, int err_code)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(box.expr_buffer) || !box.file_appender.is_opened()) {
    ret = OB_NOT_INIT;
    LOG_WARN("box not init", K(ret));
  } else {
    box.expr_buffer->reset();
    int64_t log_buf_pos = 0;
    const char* err_msg = ob_errpkt_strerror(err_code, box.is_oracle_mode);
    int err_no = ob_errpkt_errno(err_code, box.is_oracle_mode);
    if (OB_FAIL(databuff_printf(box.expr_buffer->begin_ptr(),
            box.expr_buffer->get_buffer_size(),
            log_buf_pos,
            log_file_row_fmt,
            line_num + 1,
            err_no,
            err_msg))) {
      LOG_WARN("fail to printf", K(ret), K(err_msg));
    } else if (OB_FAIL(box.file_appender.append(box.expr_buffer->begin_ptr(), log_buf_pos, false))) {
      LOG_WARN("fail to append file", K(ret), K(log_buf_pos));
    } else {
      LOG_DEBUG("LOAD DATA log failed rows", K(task_id), K(line_num), K(task_type));
    }
  }
  return ret;
}

int ObLoadDataSPImpl::log_failed_insert_task(ToolBox& box, ObInsertTask& task)
{
  int ret = OB_SUCCESS;
  int log_err = OB_SUCCESS;

  if (!box.file_appender.is_opened() && OB_FAIL(create_log_file(box))) {
    LOG_WARN("fail to create log file", K(ret));
  } else {
    log_err = task.result_.exec_ret_;
  }

  for (int64_t buf_i = 0; OB_SUCC(ret) && buf_i < task.insert_value_data_.count(); ++buf_i) {
    int64_t pos = 0;
    const char* buf = task.insert_value_data_[buf_i].ptr();
    int64_t data_len = task.insert_value_data_[buf_i].length();
    ObDataFrag* frag = NULL;
    int64_t line_num_base = 0;

    if (OB_ISNULL(frag = static_cast<ObDataFrag*>(task.source_frag_[buf_i]))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("source data frag is NULL", K(buf_i), K(ret), K(task));
    } else if (OB_UNLIKELY(
                   OB_INVALID_ID == frag->shuffle_task_id || frag->shuffle_task_id >= box.file_buf_row_num.count())) {
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

      OZ(log_failed_line(box, "insert_task", task.task_id_, line_num, log_err));

      pos = pos_back + row_ser_size;
    }

  }  // end for
  return ret;
}

int ObLoadDataSPImpl::handle_returned_insert_task(
    ObExecContext& ctx, ToolBox& box, ObInsertTask& insert_task, bool& need_retry)
{
  int ret = OB_SUCCESS;
  ObPartDataFragMgr* part_mgr = NULL;
  ObLoadServerInfo* server_info = NULL;
  ObInsertResult& result = insert_task.result_;
  enum TASK_STATUS { TASK_SUCC, TASK_NEED_RETRY, TASK_FAILED } task_status = TASK_FAILED;

  if (OB_ISNULL(part_mgr = insert_task.part_mgr) ||
      OB_ISNULL(server_info = box.server_infos.at(insert_task.token_server_idx_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid insert task", K(ret), K(insert_task));
  }

  if (OB_SUCC(ret) && result.flags_.test_bit(ObTaskResFlag::NEED_WAIT_MINOR_FREEZE)) {
    int64_t last_ts = 0;
    ObAddr& addr = part_mgr->get_leader_addr();
    bool found = (OB_SUCCESS == box.server_last_available_ts.get(addr, last_ts));
    if (insert_task.result_recv_ts_ > last_ts) {
      if (OB_FAIL(memory_wait_local(ctx, part_mgr->get_part_key(), addr, box.wait_secs_for_mem_release))) {
        LOG_WARN("fail to memory_wait_local", K(ret));
      } else {
        int64_t curr_time = ObTimeUtil::current_time();
        ret = found ? box.server_last_available_ts.update(addr, curr_time)
                    : box.server_last_available_ts.insert(addr, curr_time);
      }
    }
  }

  bool can_retry =
      (ObLoadDupActionType::LOAD_REPLACE == box.insert_mode || ObLoadDupActionType::LOAD_IGNORE == box.insert_mode) &&
      insert_task.retry_times_ < ObInsertTask::RETRY_LIMIT;
  if (OB_SUCC(ret)) {
    int err = result.exec_ret_;
    if (OB_LIKELY(OB_SUCCESS == err && !result.flags_.test_bit(ObTaskResFlag::RPC_TIMEOUT))) {
      task_status = TASK_SUCC;
    } else if (result.flags_.test_bit(ObTaskResFlag::RPC_TIMEOUT)) {
      task_status = can_retry ? TASK_NEED_RETRY : TASK_FAILED;
      if (TASK_FAILED == task_status) {
        result.exec_ret_ = OB_TIMEOUT;
      }
    } else if (is_server_down_error(err) || is_master_changed_error(err) || is_partition_change_error(err)) {
      task_status = can_retry ? TASK_NEED_RETRY : TASK_FAILED;
      if (OB_FAIL(part_mgr->update_part_location(ctx))) {
        LOG_WARN("fail to update location cache", K(ret));
      }
    } else {
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
        break;
      case TASK_NEED_RETRY:
        insert_task.retry_times_++;
        need_retry = true;
        LOG_WARN("LOAD DATA task need retry",
            "task_id",
            insert_task.task_id_,
            "ret",
            result.exec_ret_,
            "row_count",
            insert_task.row_count_);
        break;
      case TASK_FAILED:
        if (OB_SUCCESS != log_failed_insert_task(box, insert_task)) {
          LOG_WARN("fail to log failed insert task");
        }
        LOG_WARN("LOAD DATA task failed",
            "task_id",
            insert_task.task_id_,
            "ret",
            result.exec_ret_,
            "row_count",
            insert_task.row_count_);
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        break;
    }
  }

  return ret;
}

int ObLoadDataSPImpl::wait_insert_task_return(ObExecContext& ctx, ToolBox& box)
{
  int ret = OB_SUCCESS;
  int ret_bak = OB_SUCCESS;
  for (int64_t returned_cnt = 0; returned_cnt < box.parallel; ++returned_cnt) {
    ObInsertTask* insert_task = NULL;
    bool need_retry = false;
    if (OB_FAIL(box.insert_task_controller.on_next_task())) {
      LOG_WARN("fail to get next task id", K(ret));
    } else if (OB_FAIL(box.insert_task_reserve_queue.pop(insert_task))) {
      LOG_WARN("fail to pop", K(ret));
    } else if (OB_ISNULL(insert_task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("insert task is null", K(ret));
    } else if (!insert_task->is_empty_task() &&
               OB_FAIL(handle_returned_insert_task(ctx, box, *insert_task, need_retry))) {
      LOG_WARN("fail to handle returned insert task", K(ret));
    } else if (OB_LIKELY(!need_retry)) {
      // do nothing
    } else {
      ObRpcLoadDataInsertTaskCallBack mycallback(
          box.insert_task_controller, box.insert_task_reserve_queue, insert_task);
      OZ(ObLoadDataUtils::check_session_status(*ctx.get_my_session()));
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
    ObInsertTask* insert_task = box.insert_resource[i];
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

int ObLoadDataSPImpl::insert_task_send(ObInsertTask* insert_task, ToolBox& box)
{
  int ret = OB_SUCCESS;
  ObRpcLoadDataInsertTaskCallBack mycallback(box.insert_task_controller, box.insert_task_reserve_queue, insert_task);
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

int ObLoadDataSPImpl::insert_task_gen_and_dispatch(ObExecContext& ctx, ToolBox& box)
{
  int ret = OB_SUCCESS;

  const int64_t total_server_n = box.server_infos.count();
  int64_t part_iters[total_server_n];
  MEMSET(part_iters, 0, sizeof(part_iters));
  int64_t token_cnt = box.insert_task_controller.get_max_parallelism();

  while (token_cnt > 0) {
    ObInsertTask* insert_task = NULL;
    bool need_retry = false;
    bool task_send_out = false;

    OW(box.insert_task_controller.on_next_task());

    if (OB_SUCC(ret)) {
      if (OB_FAIL(box.insert_task_reserve_queue.pop(insert_task))) {
        LOG_WARN("fail to pop", K(ret));
      } else if (OB_ISNULL(insert_task)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("insert task is null", K(ret));
      } else if (!insert_task->is_empty_task() &&
                 OB_FAIL(handle_returned_insert_task(ctx, box, *insert_task, need_retry))) {
        LOG_WARN("fail to handle returned insert task", K(ret));
      } else if (OB_UNLIKELY(need_retry)) {
        // CASE1: for retry old insert task
        LOG_DEBUG("LOAD DATA need retry", KPC(insert_task));
        if (OB_FAIL(insert_task_send(insert_task, box))) {
          LOG_WARN("fail to send insert task", K(ret));
        } else {
          task_send_out = true;
        }
      } else {
        int64_t& part_iter = part_iters[insert_task->token_server_idx_];
        ObLoadServerInfo* server_info = box.server_infos.at(insert_task->token_server_idx_);
        ObPartDataFragMgr* part_datafrag_mgr = nullptr;
        int64_t row_count = box.batch_row_count;
        bool iter_end = true;

        // find next batch data on this server
        for (; part_iter < server_info->part_datafrag_group.count(); ++part_iter) {
          part_datafrag_mgr = server_info->part_datafrag_group.at(part_iter);
          row_count = box.batch_row_count;
          if (part_datafrag_mgr->has_data(row_count) ||
              (box.read_cursor.is_end_file() && 0 != (row_count = part_datafrag_mgr->remain_row_count()))) {
            iter_end = false;
            break;
          }
        }

        if (!insert_task->is_empty_task()) {
          insert_task->reuse();
        }

        if (iter_end) {
          // CASE2: all task on this server are done
          task_send_out = false;
          LOG_DEBUG("LOAD DATA all jobs are finish", K(server_info->addr), K(token_cnt));
        } else {
          // CASE3: for new insert task
          insert_task->part_mgr = part_datafrag_mgr;
          insert_task->task_id_ = box.insert_task_controller.get_next_task_id();
          if (OB_FAIL(part_datafrag_mgr->next_insert_task(row_count, *insert_task))) {
            LOG_WARN("fail to generate insert task", K(ret));
          } else {
            box.insert_dispatch_rows += row_count;
            box.insert_task_count++;
            if (row_count != DEFAULT_BUFFERRED_ROW_COUNT) {
              LOG_DEBUG("LOAD DATA task generate",
                  "task_id",
                  insert_task->task_id_,
                  "affected_rows",
                  box.affected_rows,
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
    ObInsertTask* insert_task = box.insert_resource[i];
    if (OB_FAIL(box.insert_task_controller.on_task_finished())) {
      LOG_WARN("fail to on task finish", K(ret));
    } else if (OB_FAIL(box.insert_task_reserve_queue.push_back(insert_task))) {
      LOG_WARN("fail to push back", K(ret));
    }
  }

  return ret;
}

int ObLoadDataSPImpl::execute(ObExecContext& ctx, ObLoadDataStmt& load_stmt)
{
  int ret = OB_SUCCESS;

  ToolBox box;

  // init toolbox
  OZ(box.init(ctx, load_stmt));

  LOG_INFO("LOAD DATA start report",
      "file_path",
      load_stmt.get_load_arguments().file_name_,
      "table_name",
      load_stmt.get_load_arguments().combined_name_,
      "batch_size",
      box.batch_row_count,
      "parallel",
      box.parallel,
      "load_mode",
      box.insert_mode,
      "transaction_timeout",
      box.txn_timeout);

  // ignore rows
  while (OB_SUCC(ret) && !box.read_cursor.is_end_file() && box.data_trimer.get_lines_count() < box.ignore_rows) {
    box.expr_buffer->reset();
    OZ(next_file_buffer(box, *box.expr_buffer, box.ignore_rows - box.data_trimer.get_lines_count()));
    LOG_DEBUG("LOAD DATA ignore rows", K(box.ignore_rows), K(box.data_trimer.get_lines_count()));
  }

  // main while
  while (OB_SUCC(ret) && !box.read_cursor.is_end_file()) {
    /*
     * 1. calc partition in parrallel (shuffle_task_gen_and_dispatch)
     * 2. insert in parrallel (insert_task_gen_and_dispatch)
     */
    OZ(shuffle_task_gen_and_dispatch(ctx, box));
    OW(wait_shuffle_task_return(box));
    OZ(insert_task_gen_and_dispatch(ctx, box));
    // OW (wait_insert_task_return(ctx, box));
    OW(box.data_frag_mgr.free_unused_datafrag());

    /* check session available
     */
    OZ(ObLoadDataUtils::check_session_status(*ctx.get_my_session()));
  }

  // release
  OW(box.release_resources());

  if (OB_SUCC(ret) && OB_NOT_NULL(ctx.get_physical_plan_ctx())) {
    ctx.get_physical_plan_ctx()->set_affected_rows(box.affected_rows);
    ctx.get_physical_plan_ctx()->set_row_matched_count(box.data_trimer.get_lines_count());
  }

  if (OB_FAIL(ret)) {
    LOG_WARN("LOAD DATA execute failed, ", K(ret));
  }

  if (box.file_appender.is_opened()) {
    LOG_WARN("LOAD DATA error log generated");
  }

  LOG_INFO("LOAD DATA finish report",
      "total shuffle task",
      box.shuffle_task_controller.get_total_task_cnt(),
      "total insert task",
      box.insert_task_controller.get_total_task_cnt(),
      "insert rt sum",
      box.insert_rt_sum,
      "suffle rt sum",
      box.suffle_rt_sum,
      "total wait secs",
      box.wait_secs_for_mem_release,
      "datafrag info",
      box.data_frag_mgr);

  return ret;
}

int ObLoadFileDataTrimer::recover_incomplate_data(ObLoadFileBuffer& buffer)
{
  int ret = OB_SUCCESS;
  char* buf = NULL;
  if (OB_ISNULL(buf = buffer.begin_ptr())) {
    ret = OB_INVALID_ARGUMENT;
  } else if (incomplate_data_len_ > 0) {
    MEMCPY(buf, incomplate_data_, incomplate_data_len_);
    buffer.update_pos(incomplate_data_len_);
  }
  return ret;
}

int ObLoadFileDataTrimer::backup_incomplate_data(ObLoadFileBuffer& buffer, int64_t valid_data_len)
{
  int ret = OB_SUCCESS;
  incomplate_data_len_ = buffer.get_data_len() - valid_data_len;
  if (incomplate_data_len_ > ObLoadFileBuffer::MAX_BUFFER_SIZE) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("size over flow", K(ret), K(incomplate_data_len_));
  } else if (incomplate_data_len_ > 0 && NULL != incomplate_data_) {
    MEMCPY(incomplate_data_, buffer.begin_ptr() + valid_data_len, incomplate_data_len_);
    buffer.update_pos(-incomplate_data_len_);
  }
  return ret;
}

int ObPartDataFragMgr::rowoffset2pos(ObDataFrag* frag, int64_t row_num, int64_t& pos)
{
  int ret = OB_SUCCESS;
  pos = 0;

  if (OB_ISNULL(frag)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    char* buf = frag->data;
    int64_t data_len = frag->frag_pos;
    for (int64_t i = 0; OB_SUCC(ret) && i < row_num; ++i) {
      int64_t row_len = 0;
      OB_UNIS_DECODE(row_len);
      pos += row_len;
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
  ObLink* link = NULL;

  if (!has_data(1)) {
    // do nothing
  } else {
    while (OB_SUCC(ret) && OB_EAGAIN != queue_.pop(link)) {
      data_frag_mgr_.distory_datafrag(static_cast<ObDataFrag*>(link));
    }
  }
  return ret;
}

int ObPartDataFragMgr::next_insert_task(int64_t batch_row_count, ObInsertTask& task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(batch_row_count <= 0)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_UNLIKELY(!has_data(batch_row_count))) {
    ret = OB_EAGAIN;  // for now, never reach here
  } else {
    total_row_consumed_ += batch_row_count;
  }

  ObLink* link = NULL;
  ObDataFrag* frag = NULL;
  int64_t row_count = -queue_top_begin_point_.frag_row_pos_;
  InsertTaskSplitPoint new_top_begin_point;

  while (OB_SUCC(ret) && row_count < batch_row_count) {
    new_top_begin_point.reset();
    while (OB_EAGAIN == queue_.top(link)) {
      pause();
    }
    if (OB_ISNULL(frag = static_cast<ObDataFrag*>(link))) {
      ret = OB_ERR_UNEXPECTED;
    } else if ((row_count += frag->row_cnt) > batch_row_count) {
      new_top_begin_point.frag_row_pos_ = frag->row_cnt - (row_count - batch_row_count);
      if (OB_FAIL(rowoffset2pos(frag, new_top_begin_point.frag_row_pos_, new_top_begin_point.frag_data_pos_))) {
        LOG_WARN("fail to rowoffset to pos", K(ret));
      } else if (OB_FAIL(task.insert_value_data_.push_back(
                     ObString(new_top_begin_point.frag_data_pos_ - queue_top_begin_point_.frag_data_pos_,
                         frag->data + queue_top_begin_point_.frag_data_pos_)))) {
        LOG_WARN("fail to do push back", K(ret));
      } else if (OB_FAIL(task.source_frag_.push_back(frag))) {
        LOG_WARN("fail to push back frag", K(ret));
      }
    } else {
      if (OB_FAIL(queue_.pop(link))) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(frag_free_list_.push_back(frag))) {
        // TODO free frag for failure
        LOG_WARN("fail to push back", K(ret));
      } else {
        if (OB_FAIL(task.insert_value_data_.push_back(ObString(frag->frag_pos - queue_top_begin_point_.frag_data_pos_,
                frag->data + queue_top_begin_point_.frag_data_pos_)))) {
          LOG_WARN("fail to do push back", K(ret));
        } else if (OB_FAIL(task.source_frag_.push_back(frag))) {
          LOG_WARN("fail to push back frag", K(ret));
        }
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

  for (int64_t i = 0; OB_SUCC(ret) && i < part_ids_.count(); ++i) {
    int64_t part_id = part_ids_[i];
    ObPartDataFragMgr* part_data_frag = NULL;

    if (OB_FAIL(get_part_datafrag(part_id, part_data_frag))) {
      LOG_WARN("fail to get part datafrag", K(ret), K(part_id));
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

  for (int64_t i = 0; OB_SUCC(ret) && i < part_ids_.count(); ++i) {
    int64_t part_id = part_ids_[i];
    ObPartDataFragMgr* part_data_frag = NULL;

    if (OB_FAIL(get_part_datafrag(part_id, part_data_frag))) {
      LOG_WARN("fail to get part datafrag", K(ret), K(part_id));
    } else if (OB_ISNULL(part_data_frag)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("part data frag is null", K(ret));
    } else if (OB_FAIL(part_data_frag->clear())) {
      LOG_WARN("fail to free frag", K(ret));
    }
  }

  return ret;
}

int ObDataFragMgr::init(ObExecContext& ctx, uint64_t table_id)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard* schema_guard = NULL;
  const ObTableSchema* table_schema = NULL;
  part_ids_.reset();

  if (OB_ISNULL(ctx.get_sql_ctx()) || OB_ISNULL(schema_guard = ctx.get_sql_ctx()->schema_guard_) ||
      OB_ISNULL(ctx.get_my_session())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sql ctx is null", K(ret), KP(ctx.get_sql_ctx()));
  } else if (OB_FAIL(schema_guard->get_table_schema(table_id, table_schema))) {
    LOG_WARN("fail to get partition count", K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is NULL", K(ret));
  } else if (OB_FAIL(table_schema->get_physical_partition_ids(part_ids_))) {
    LOG_WARN("failed to get partition ids", K(ret));
  } else {
    total_part_cnt_ = part_ids_.count();
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < part_ids_.count(); ++i) {
    int64_t partition_id = part_ids_[i];
    ObPartDataFragMgr* part_data_frag = NULL;

    if (OB_ISNULL(part_data_frag = OB_NEWx(ObPartDataFragMgr, (&ctx.get_allocator()), *this, partition_id))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else if (OB_FAIL(part_data_frag->get_part_key().init(table_id, partition_id, total_part_cnt_))) {
      LOG_WARN("fail to init part key", K(ret));
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
    // attr_.ctx_id_ = common::ObCtxIds::WORK_AREA;
    total_alloc_cnt_ = 0;
    total_free_cnt_ = 0;
  }

  return ret;
}

int ObDataFragMgr::get_part_datafrag(int64_t part_id, ObPartDataFragMgr*& part_datafrag_mgr)
{
  return part_datafrag_map_.get_refactored(part_id, part_datafrag_mgr);
}

int ObDataFragMgr::create_datafrag(ObDataFrag*& frag, int64_t min_len)
{
  int ret = OB_SUCCESS;
  frag = NULL;
  void* buf = NULL;
  int64_t min_alloc_size = sizeof(ObDataFrag) + min_len;
  int64_t opt_alloc_size = 0;

  if (min_alloc_size <= ObDataFrag::DEFAULT_STRUCT_SIZE) {
    opt_alloc_size = ObDataFrag::DEFAULT_STRUCT_SIZE;
  } else if (min_alloc_size >= OB_MALLOC_BIG_BLOCK_SIZE) {
    opt_alloc_size = min_alloc_size;
  } else {
    opt_alloc_size = ObDataFrag::DEFAULT_STRUCT_SIZE * ((min_alloc_size - 1) / ObDataFrag::DEFAULT_STRUCT_SIZE + 1);
  }

  if (OB_ISNULL(buf = ob_malloc(opt_alloc_size, attr_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to malloc", K(ret), KP(this));
  } else {
    frag = new (buf) ObDataFrag(opt_alloc_size);
    ATOMIC_AAF(&total_alloc_cnt_, 1);
  }
  return ret;
}

void ObDataFragMgr::distory_datafrag(ObDataFrag* frag)
{
  if (OB_ISNULL(frag)) {
    // do nothing
  } else {
    frag->~ObDataFrag();
    ob_free(frag);
    total_free_cnt_++;
  }
}

int ObPartDataFragMgr::update_part_location(ObExecContext& ctx)
{
  int ret = OB_SUCCESS;
  ObIPartitionLocationCache* location_cache = NULL;

  if (OB_UNLIKELY(!part_key_.is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("invalid partition key", K(ret));
  } else if (OB_ISNULL(location_cache = ctx.get_task_exec_ctx().get_partition_location_cache())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid partition locatition cache", K(ret));
  } else {
    bool force_renew = false;
    do {
      if (OB_FAIL(location_cache->get_strong_leader(part_key_, leader_addr_, force_renew))) {
        if (OB_LOCATION_LEADER_NOT_EXIST == ret && !force_renew) {
          // retry one time
          force_renew = true;
          LOG_WARN("failed to get location and force renew", K(ret), K(part_key_));
        } else {
          force_renew = false;
          LOG_WARN("failed to get location", K(ret), K(part_key_));
        }
      } else {
        LOG_DEBUG("get participants", K(part_key_), K(leader_addr_));
      }
    } while (OB_LOCATION_LEADER_NOT_EXIST == ret && force_renew);
  }

  return ret;
}

int ObLoadFileDataTrimer::init(ObIAllocator& allocator, const ObCSVFormats& formats)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(incomplate_data_ = static_cast<char*>(allocator.alloc(ObLoadFileBuffer::MAX_BUFFER_SIZE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("no memory", K(ret));
  }
  formats_ = formats;
  return ret;
}

int ObLoadDataSPImpl::ToolBox::release_resources()
{
  int ret = OB_SUCCESS;

  if (gid.is_valid()) {
    ObLoadDataStat* job_status = nullptr;
    if (OB_FAIL(ObGlobalLoadDataStatMap::getInstance()->unregister_job(gid, job_status))) {
      LOG_ERROR("fail to unregister job", K(ret), K(gid));
    } else if (OB_ISNULL(job_status)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("fail to unregister job", K(ret), K(gid));
    } else {
      int64_t log_print_cnt = 0;
      int64_t ref_cnt = 0;
      while ((ref_cnt = job_status->get_ref_cnt()) > 0) {
        usleep(WAIT_INTERVAL_US);  // 1s
        if ((log_print_cnt++) % 10 == 0) {
          LOG_WARN("LOAD DATA wait job handle release", K(ret), "wait_seconds", log_print_cnt * 10, K(gid), K(ref_cnt));
        }
      }
    }
  }

  // release sessions in shuffle task
  for (int64_t i = 0; i < shuffle_resource.count(); ++i) {
    ObShuffleTaskHandle* handle = NULL;
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
    ObInsertTask* task = NULL;
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

  return ret;
}

int ObLoadDataSPImpl::ToolBox::init(ObExecContext& ctx, ObLoadDataStmt& load_stmt)
{
  int ret = OB_SUCCESS;
  const ObLoadArgument& load_args = load_stmt.get_load_arguments();
  const ObDataInFileStruct& file_formats = load_stmt.get_data_struct_in_file();
  const ObLoadDataHint& hint = load_stmt.get_hints();

  formats.init(file_formats);
  self_addr = ctx.get_task_executor_ctx()->get_self_addr();
  // batch_row_count = DEFAULT_BUFFERRED_ROW_COUNT;
  data_frag_mem_usage_limit = 50;  // 50*2M = 100M
  is_oracle_mode = share::is_oracle_mode();
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

  ObSQLSessionInfo* session = NULL;

  if (OB_ISNULL(session = ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (OB_FAIL(data_trimer.init(ctx.get_allocator(), formats))) {
    LOG_WARN("fail to init data_trimer", K(ret));
  } else if (OB_FAIL(build_insert_values_generator(ctx, load_stmt, generator))) {
    LOG_WARN("fail to build insert values generator", K(ret));
  } else if (OB_FAIL(generator.gen_insert_columns_names_buff(ctx, load_args, insert_stmt_head_buff))) {
    LOG_WARN("fail to gen insert column names buff", K(ret));
  } else if (OB_FAIL(data_frag_mgr.init(ctx, load_args.table_id_))) {
    LOG_WARN("fail to init data frag mgr", K(ret));
  }

  // init server_info_map
  if (OB_SUCC(ret)) {
    if (OB_FAIL(server_info_map.init("serverinfomap", MAX_SERVER_COUNT))) {
      LOG_WARN("fail to init server info map", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < data_frag_mgr.get_part_ids().count(); ++i) {
    int64_t part_id = data_frag_mgr.get_part_ids().at(i);
    ObPartDataFragMgr* part_frag_mgr = nullptr;
    if (OB_FAIL(data_frag_mgr.get_part_datafrag(part_id, part_frag_mgr))) {
      LOG_WARN("fail to get part data frag", K(ret), K(part_id));
    } else if (OB_UNLIKELY(!part_frag_mgr->get_leader_addr().is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("part leader addr is not valid", K(ret), K(part_id));
    } else {
      ObLoadServerInfo* server_info = nullptr;
      if (OB_SUCCESS != server_info_map.get(part_frag_mgr->get_leader_addr(), server_info)) {
        // no find, create one
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
      // save part index to server info
      if (OB_SUCC(ret)) {
        if (OB_FAIL(server_info->part_datafrag_group.push_back(part_frag_mgr))) {
          LOG_WARN("fail to add member", K(ret));
        }
      }
    }
  }

  // init server_info
  if (OB_SUCC(ret)) {
    auto push_to_array = [&](const ObAddr& key, ObLoadServerInfo* value) -> bool {
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
    if (ObLoadFileLocation::SERVER_DISK == load_file_storage) {
      OZ(file_reader.open(load_args.file_name_, false));
      OX(file_size = get_file_size(load_args.file_name_.ptr()));
    } else {
#ifdef _WITH_OSS
      OZ(oss_reader.open(load_args.file_name_, load_args.access_info_));
      OX(file_size = oss_reader.get_length());
#else
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not supported file storage", K(ret), K(load_file_storage));
#endif
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < generator.get_table_column_value_descs().count(); ++i) {
    const ObLoadTableColumnDesc& desc = generator.get_table_column_value_descs().at(i);
    if (!desc.is_set_values_ && ob_is_string_tc(desc.column_type_)) {
      if (OB_FAIL(string_type_column_bitset.add_member(i))) {
        LOG_WARN("fail to add bitset", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    void* buf = NULL;
    num_of_file_column = load_stmt.get_field_or_var_list().count();
    num_of_table_column = generator.get_table_column_value_descs().count();
    if (OB_FAIL(insert_values.prepare_allocate(num_of_table_column))) {
      LOG_WARN("fail to reserve array", K(ret));
    } else if (OB_FAIL(field_values_in_file.prepare_allocate(num_of_file_column))) {
      LOG_WARN("fail to reserve array", K(ret));
    } else if (OB_ISNULL(buf = ob_malloc(ObLoadFileBuffer::MAX_BUFFER_SIZE, ObModIds::OB_SQL_LOAD_DATA))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else if (FALSE_IT(expr_buffer =
                            new (buf) ObLoadFileBuffer(ObLoadFileBuffer::MAX_BUFFER_SIZE - sizeof(ObLoadFileBuffer)))) {
    } else if (OB_FAIL(calculator.init(ctx,
                   load_args.combined_name_,
                   load_args.table_id_,
                   generator,
                   *expr_buffer,
                   insert_values,
                   field_values_in_file))) {
      LOG_WARN("fail to init calculator", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    plan.set_vars(ctx.get_stmt_factory()->get_query_ctx()->variables_);
    ctx.get_my_session()->set_cur_phy_plan(&plan);
    if (OB_FAIL(ctx.init_phy_op(1))) {
      LOG_WARN("fail to init phy op", K(ret));
    } else {
      char* buf = NULL;
      int64_t size = ctx.get_serialize_size();
      int64_t pos = 0;
      if (OB_ISNULL(buf = static_cast<char*>(ctx.get_allocator().alloc(size)))) {
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
      // parallel = std::min(parallel, max_cpus);
    }
  }

  if (OB_SUCC(ret)) {
    int64_t hint_batch_size = 0;
    if (OB_FAIL(hint.get_value(ObLoadDataHint::BATCH_SIZE, hint_batch_size))) {
      LOG_WARN("fail to get value", K(ret));
    } else if (0 == hint_batch_size) {
      batch_row_count = DEFAULT_BUFFERRED_ROW_COUNT;
    } else {
      batch_row_count = std::max(1L, std::min(DEFAULT_BUFFERRED_ROW_COUNT, hint_batch_size));
    }
    LOG_DEBUG("batch size", K(hint_batch_size), K(batch_row_count));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(parser.init(num_of_file_column, formats, string_type_column_bitset))) {
      LOG_WARN("fail to init parser", K(ret));
    } else {
      parser.set_fast_parse();
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(shuffle_task_controller.init(parallel))) {
      LOG_WARN("fail to init shuffle task controller", K(ret));
    } else if (OB_FAIL(shuffle_task_reserve_queue.init(parallel))) {
      LOG_WARN("fail to init shuffle_task_reserve_queue", K(ret));
    } else if (OB_FAIL(insert_task_controller.init(parallel * server_infos.count()))) {
      LOG_WARN("fail to init insert task controller", K(ret));
    } else if (OB_FAIL(insert_task_reserve_queue.init(parallel * server_infos.count()))) {
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

    for (int i = 0; OB_SUCC(ret) && i < shuffle_task_controller.get_max_parallelism(); ++i) {
      ObShuffleTaskHandle* handle = nullptr;
      int64_t pos = 0;

      if (OB_ISNULL(
              handle = OB_NEWx(ObShuffleTaskHandle, (&ctx.get_allocator()), generator, calculator, data_frag_mgr)) ||
          OB_ISNULL(handle->data_buffer)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Failed to alloc", K(ret));
      } else {
        if (OB_FAIL(
                handle->exec_ctx.deserialize(exec_ctx_serialized_data.ptr(), exec_ctx_serialized_data.length(), pos))) {
          LOG_WARN("fail to deserialize", K(ret));
        } else if (OB_FAIL(handle->parser.init(num_of_file_column, formats, string_type_column_bitset))) {
          LOG_WARN("fail to init parser", K(ret));
        } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(load_args.tenant_id_, handle->schema_guard))) {
          LOG_WARN("fail to get schema guard", K(ret));
        } else if (OB_FAIL(shuffle_task_reserve_queue.push_back(handle))) {
          LOG_WARN("fail to push back", K(ret));
        } else {
          if (!((ObArenaAllocator*)(&handle->exec_ctx.get_allocator()))->set_tracer()) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to set tracer", K(ret));
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
      ObInsertTask* insert_task = nullptr;
      if (OB_ISNULL(insert_task = OB_NEWx(ObInsertTask, (&ctx.get_allocator())))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Failed to alloc", K(ret));
      } else {
        insert_task->insert_stmt_head_ = insert_stmt_head_buff;
        insert_task->column_count_ = generator.get_table_column_value_descs().count();
        insert_task->row_count_ = batch_row_count;
        insert_task->tenant_id_ = ctx.get_my_session()->get_effective_tenant_id();
        insert_task->token_server_idx_ = server_j;
        if (OB_FAIL(insert_resource.push_back(insert_task))) {
          insert_task->~ObInsertTask();
          LOG_WARN("fail to push back", K(ret));
        } else if (OB_FAIL(insert_task_reserve_queue.push_back(insert_task))) {
          LOG_WARN("fail to push back", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(server_last_available_ts.init(ObModIds::OB_SQL_LOAD_DATA, MAX_SERVER_COUNT))) {
        LOG_WARN("fail to create server map", K(ret));
      }
    }
  }

  constexpr const char* dict = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
  constexpr int word_base = 62;  // length of dict
  const int64_t file_id_len = 6;
  int64_t cur_ts = ObTimeUtil::current_time();

  if (OB_SUCC(ret)) {
    char* buf = NULL;
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
    int64_t max_task_count = (file_size / ObLoadFileBuffer::MAX_BUFFER_SIZE + 1) * 2;
    if (OB_FAIL(file_buf_row_num.reserve(max_task_count))) {
      LOG_WARN("fail to reserve", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    char* buf = NULL;
    int64_t buf_len = DEFAULT_BUF_LENGTH;
    int64_t pos = 0;
    if (OB_ISNULL(buf = static_cast<char*>(ctx.get_allocator().alloc(buf_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("no memory", K(ret), K(buf_len));
    } else {
      const ObString& cur_query_str = ctx.get_my_session()->get_current_query_string();
      const uint64_t* trace_id = ObCurTraceId::get();
      uint64_t trace_id_0 = (OB_ISNULL(trace_id)) ? OB_INVALID_ID : trace_id[0];
      uint64_t trace_id_1 = (OB_ISNULL(trace_id)) ? OB_INVALID_ID : trace_id[1];
      OZ(databuff_printf(buf,
          buf_len,
          pos,
          "Tenant name:\t%.*s\n"
          "File name:\t%.*s\n"
          "Into table:\t%.*s\n"
          "Parallel:\t%ld\n"
          "Batch size:\t%ld\n"
          "SQL trace:\t" TRACE_ID_FORMAT "\n",
          session->get_tenant_name().length(),
          session->get_tenant_name().ptr(),
          load_args.file_name_.length(),
          load_args.file_name_.ptr(),
          load_args.combined_name_.length(),
          load_args.combined_name_.ptr(),
          parallel,
          batch_row_count,
          trace_id_0,
          trace_id_1));
      OZ(databuff_printf(buf, buf_len, pos, "Start time:\t"));
      OZ(ObTimeConverter::datetime_to_str(
          cur_ts, TZ_INFO(session), ObString(), MAX_SCALE_FOR_TEMPORAL, buf, buf_len, pos, true));
      OZ(databuff_printf(buf, buf_len, pos, "\n"));
      OZ(databuff_printf(buf, buf_len, pos, "Load query: \n%.*s\n", cur_query_str.length(), cur_query_str.ptr()));
      OX(load_info.assign_ptr(buf, pos));
    }
  }

  if (OB_SUCC(ret)) {
    ObLoadDataStat* job_status = nullptr;
    if (OB_ISNULL(job_status = OB_NEWx(ObLoadDataStat, (&ctx.get_allocator())))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", K(ret));
    } else {
      ObLoadDataGID temp_gid;
      ObLoadDataGID::generate_new_id(temp_gid);
      if (OB_FAIL(ObGlobalLoadDataStatMap::getInstance()->register_job(temp_gid, job_status))) {
        LOG_WARN("fail to register job", K(ret));
      } else {
        gid = temp_gid;
      }
    }
  }

  return ret;
}
/*
ObLoadDataOssReader::~ObLoadDataOssReader()
{
  if (OB_NOT_NULL(cached_data_)) {
    ob_free(cached_data_);
  }
}

int ObLoadDataOssReader::init(int64_t cached_limit)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(cached_limit < 0 || cached_limit > (1<<30))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid cached size", K(cached_limit));
  } else {
    cached_limit_ = cached_limit;
  }

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(cached_data_ =
                  static_cast<char*>(ob_malloc(cached_limit, ObModIds::OB_SQL_LOAD_DATA)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("not enough memory", K(ret));
    }
  }

  return ret;
}

int ObLoadDataOssReader::cached_pread(char *buf,
                               const int64_t buf_size,
                               int64_t offset,
                               int64_t &read_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(buf_size > cached_limit_)) {
    OZ (ObStorageOssReader::pread(buf, buf_size, offset, read_size));
    LOG_WARN("LOAD DATA oss no cache");
  } else {
    if ((offset + buf_size) > (offset_ + cached_size_)
        || offset < offset_) {
      OZ (ObStorageOssReader::pread(cached_data_, cached_limit_, offset, cached_size_));
      offset_ = offset;
      read_size = std::min(buf_size, cached_size_);
      LOG_WARN("LOAD DATA oss cache reload");
    } else {
      read_size = buf_size;
      LOG_WARN("LOAD DATA oss cache hit");
    }
    OX (MEMCPY(buf, cached_data_ + (offset - offset_), read_size));
  }
  return ret;
}
*/
}  // namespace sql
}  // namespace oceanbase
