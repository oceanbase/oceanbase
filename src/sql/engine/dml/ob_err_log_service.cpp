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
#include "ob_err_log_service.h"
#include "observer/ob_inner_sql_connection_pool.h"
#include "lib/mysqlclient/ob_isql_connection.h"
#include "lib/rc/ob_rc.h"
namespace oceanbase
{
namespace sql
{

int ObErrLogService::gen_insert_sql_str(ObIAllocator &alloc,
                                        int first_err_ret,
                                        const ObErrLogCtDef &err_log_ct_def,
                                        ObString &dynamic_column_name,
                                        ObString &dynamic_column_value,
                                        char *&sql_str, ObDASOpType type)
{
  int ret = OB_SUCCESS;
  const char *ora_err_number = "ORA_ERR_NUMBER$";
  const char *ora_err_mesg = "ORA_ERR_MESG$";
  //const char *ora_err_row_id = "ORA_ERR_ROWID$";
  const char *ora_err_optyp = "ORA_ERR_OPTYP$";
  //const char *ora_err_tag = "ORA_ERR_TAG$";
  const char *DELIMITER = " , ";
  const char *ESCAPE_CHARACTER = "'";
  const char *err_type = NULL;
  const int64_t DEFAULT_COL_NAME_LENGTH = 258;
  const int64_t DEFAULT_COL_VALUE_LENGTH = 2 * 1024;

  const int64_t insert_buf_len = dynamic_column_name.length() + dynamic_column_value.length() +
                                 DEFAULT_COL_NAME_LENGTH + DEFAULT_COL_VALUE_LENGTH;
  char *insert_buf = NULL;
  char *default_column_name_buf = NULL;
  char *default_column_value_buf = NULL;
  int64_t insert_pos = 0;
  int64_t default_column_name_pos = 0;
  int64_t default_column_value_pos = 0;

  const int err_no = ob_oracle_errno(first_err_ret);
  // ObString msg = ob_get_tsi_err_msg(first_err_ret);
  // Because there are escape characters in dynamic_msg, use static error messages now
  ObString msg = ObString::make_string(ob_oracle_strerror(first_err_ret));

  // %.*s 1:database_name
  // %.*s 2:table_name
  // %.*s 3:default_column_name
  // %.*s 4:dynamic_column_name
  // %.*s 5:default_column_value
  // %.*s 6:dynamic_column_value
  const char *INSERT_FMT = "insert into \"%.*s\".\"%.*s\"(%.*s %.*s) values (%.*s %.*s)";
  const char *INSERT_ONLY_DEFAULT_COLUMN_FMT = "insert into \"%.*s\".\"%.*s\"(%.*s) values (%.*s)";

  switch(type) {
  case DAS_OP_TABLE_INSERT:
    err_type = "I";
    break;
  case DAS_OP_TABLE_UPDATE:
    err_type = "U";
    break;
  case DAS_OP_TABLE_DELETE:
    err_type = "D";
    break;
  default:
    ret = OB_ERR_UNEXPECTED;
    break;
  }

  if (OB_FAIL(ret)) {

  } else if (msg.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("err msg is empty", K(first_err_ret));
  } else if (OB_ISNULL(insert_buf = static_cast<char *>(alloc.alloc(insert_buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory for insert_buf", K(ret), K(insert_buf_len));
  } else if (OB_ISNULL(default_column_name_buf = static_cast<char *>(alloc.alloc(DEFAULT_COL_NAME_LENGTH)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory for default_column_name_buf", K(ret), K(DEFAULT_COL_NAME_LENGTH));
  } else if (OB_ISNULL(default_column_value_buf = static_cast<char *>(alloc.alloc(DEFAULT_COL_VALUE_LENGTH)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory for default_column_value_buf", K(ret), K(DEFAULT_COL_VALUE_LENGTH));
  }

  // generate default_column_value and default_column_name sql string
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(databuff_printf(default_column_name_buf, DEFAULT_COL_NAME_LENGTH,
                                     default_column_name_pos, "%s",ora_err_number))) {
    LOG_WARN("fail to append column_buf for ora_err_number", K(ret));
  } else if (OB_FAIL(databuff_printf(default_column_name_buf, DEFAULT_COL_NAME_LENGTH,
                                     default_column_name_pos, "%s", DELIMITER))) {
    LOG_WARN("fail to append column_buf for DELIMITER", K(ret));
  } else if (OB_FAIL(databuff_printf(default_column_value_buf, DEFAULT_COL_VALUE_LENGTH,
                                     default_column_value_pos, "%s", ESCAPE_CHARACTER))) {
    LOG_WARN("fail to append column_buf for ESCAPE_CHARACTER", K(ret));
  } else if (OB_FAIL(databuff_printf(default_column_value_buf, DEFAULT_COL_VALUE_LENGTH,
                                     default_column_value_pos, "%d", static_cast<int32_t>(err_no)))) {
    LOG_WARN("fail to append values_buf for ora_err_number", K(ret));
  } else if (OB_FAIL(databuff_printf(default_column_value_buf, DEFAULT_COL_VALUE_LENGTH,
                                     default_column_value_pos, "%s", ESCAPE_CHARACTER))) {
    LOG_WARN("fail to append column_buf for ESCAPE_CHARACTER", K(ret));
  } else if (OB_FAIL(databuff_printf(default_column_value_buf, DEFAULT_COL_VALUE_LENGTH,
                                     default_column_value_pos, "%s", DELIMITER))) {
    LOG_WARN("fail to append values_buf for DELIMITER", K(ret));
  }
  // ---------------------
    else if (OB_FAIL(databuff_printf(default_column_name_buf, DEFAULT_COL_NAME_LENGTH,
                                     default_column_name_pos, "%s", ora_err_mesg))) {
    LOG_WARN("fail to append column_buf for ora_err_mesg", K(ret));
  } else if (OB_FAIL(databuff_printf(default_column_name_buf, DEFAULT_COL_NAME_LENGTH,
                                     default_column_name_pos, "%s", DELIMITER))) {
    LOG_WARN("fail to append column_buf for DELIMITER", K(ret));
  } else if (OB_FAIL(databuff_printf(default_column_value_buf, DEFAULT_COL_VALUE_LENGTH,
                                     default_column_value_pos, "%s", ESCAPE_CHARACTER))) {
    LOG_WARN("fail to append column_buf for ESCAPE_CHARACTER", K(ret));
  } else if (OB_FAIL(databuff_printf(default_column_value_buf, DEFAULT_COL_VALUE_LENGTH,
                                     default_column_value_pos, "%s", to_cstring(msg)))) {
    LOG_WARN("fail to append values_buf for error_msg", K(ret));
  } else if (OB_FAIL(databuff_printf(default_column_value_buf, DEFAULT_COL_VALUE_LENGTH,
                                     default_column_value_pos, "%s", ESCAPE_CHARACTER))) {
    LOG_WARN("fail to append column_buf for ESCAPE_CHARACTER", K(ret));
  } else if (OB_FAIL(databuff_printf(default_column_value_buf, DEFAULT_COL_VALUE_LENGTH,
                                     default_column_value_pos, "%s", DELIMITER))) {
    LOG_WARN("fail to append values_buf for DELIMITER", K(ret));
  }
  // -----------------
  else if (OB_FAIL(databuff_printf(default_column_name_buf, DEFAULT_COL_NAME_LENGTH,
                                   default_column_name_pos, "%s", ora_err_optyp))) {
    LOG_WARN("fail to execute databuff_printf for ora_err_mesg", K(ret));
  } else if (OB_FAIL(databuff_printf(default_column_value_buf, DEFAULT_COL_VALUE_LENGTH,
                                     default_column_value_pos, "%s", ESCAPE_CHARACTER))) {
    LOG_WARN("fail to append column_buf for ESCAPE_CHARACTER", K(ret));
  } else if (OB_FAIL(databuff_printf(default_column_value_buf, DEFAULT_COL_VALUE_LENGTH,
                                     default_column_value_pos, "%s", err_type))) {
    LOG_WARN("fail to append values_buf for error_msg", K(ret));
  } else if (OB_FAIL(databuff_printf(default_column_value_buf, DEFAULT_COL_VALUE_LENGTH,
                                     default_column_value_pos, "%s", ESCAPE_CHARACTER))) {
    LOG_WARN("fail to append column_buf for ESCAPE_CHARACTER", K(ret));
  }  // last not need DELIMITER because of INSERT_FMT add ' , '

  // generate all stmt sql
  if (OB_FAIL(ret)) {

  } else if (0 == dynamic_column_name.length() && 0 == dynamic_column_value.length()) {
    if (OB_FAIL(databuff_printf(insert_buf, insert_buf_len, insert_pos, INSERT_ONLY_DEFAULT_COLUMN_FMT,
                                err_log_ct_def.err_log_database_name_.length(),
                                err_log_ct_def.err_log_database_name_.ptr(),
                                err_log_ct_def.err_log_table_name_.length(),
                                err_log_ct_def.err_log_table_name_.ptr(),
                                default_column_name_pos, default_column_name_buf,
                                default_column_value_pos, default_column_value_buf))) {
      LOG_WARN("failed to print insert buf only default column", K(ret));
    }
  } else if (OB_FAIL(databuff_printf(insert_buf, insert_buf_len, insert_pos, INSERT_FMT,
                                     err_log_ct_def.err_log_database_name_.length(),
                                     err_log_ct_def.err_log_database_name_.ptr(),
                                     err_log_ct_def.err_log_table_name_.length(),
                                     err_log_ct_def.err_log_table_name_.ptr(),
                                     default_column_name_pos, default_column_name_buf,
                                     dynamic_column_name.length(), dynamic_column_name.ptr(),
                                     default_column_value_pos, default_column_value_buf,
                                     dynamic_column_value.length(), dynamic_column_value.ptr()))) {
    LOG_WARN("failed to print insert buf", K(ret));
  }

  sql_str = insert_buf;
  LOG_DEBUG("after generate all_sql = ", K(insert_buf), K(sql_str));
  return ret;
}

int ObErrLogService::catch_err_and_gen_sql(ObIAllocator &alloc, const ObSQLSessionInfo *session,
                                           ObString &dynamic_column_name,
                                           ObString &dynamic_column_value,
                                           const ObErrLogCtDef &err_log_ct_def)
{
  int ret = OB_SUCCESS;
  const char *DELIMITER = " , ";
  const char *QUOTATION_MARK = "\"";
  char *column_name_buf = NULL;
  char *column_value_buf = NULL;
  int64_t column_count = err_log_ct_def.err_log_values_.count();
  int64_t column_name_size = column_count * 128;
  // each column value max 4000 Bytes
  int64_t column_value_size = column_count * 4000;
  int64_t column_name_pos = 0;
  int64_t column_value_pos = 0;
  ObDatum *col_datum = NULL;

   if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session should not be null");
  } else if (column_name_size == 0) {
    LOG_DEBUG("no column hit, not need to generate dynamic_column_name");
  } else if (OB_ISNULL(column_name_buf =
                       static_cast<char *>(alloc.alloc(column_name_size)))) {
    // allocate buff for dynamic_column_name
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate column_name_buf", K(ret), K(column_name_size));
  } else if (OB_ISNULL(column_value_buf =
                       static_cast<char *>(alloc.alloc(column_value_size)))) {
    // allocate buff for dynamic_column_value
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate column_nvalue_buf", K(ret), K(column_value_size));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < err_log_ct_def.err_log_values_.count(); i++) {
      ObObj col_obj;
      OZ(err_log_ct_def.err_log_values_.at(i)->eval(eval_ctx_, col_datum));
      ObString dst_column_name;
      const ObObjMeta &col_obj_meta = err_log_ct_def.err_log_values_.at(i)->obj_meta_;
      const ObString &col_name = err_log_ct_def.err_log_column_names_.at(i);
      if (col_datum->is_null()) {
        continue;
      }
      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_FAIL(databuff_printf(column_name_buf, column_name_size,
                                         column_name_pos, "%s", DELIMITER))) {
        LOG_WARN("failed to print delimiter", K(ret), K(DELIMITER));
      }  else if (OB_FAIL(databuff_printf(column_name_buf, column_name_size,
                                          column_name_pos, "%s", QUOTATION_MARK))) {
        LOG_WARN("failed to print QUOTATION_MARK", K(ret), K(QUOTATION_MARK));
      } else if (OB_FAIL(ObSQLUtils::generate_new_name_with_escape_character(alloc,
                                                                             col_name,
                                                                             dst_column_name,
                                                                             true))) {

      } else if (OB_FAIL(databuff_printf(column_name_buf, column_name_size,
                                         column_name_pos, "%s", to_cstring(dst_column_name)))) {
        LOG_WARN("fail to append column_buf for dst_column_name", K(ret), K(col_name));
      } else if (OB_FAIL(databuff_printf(column_name_buf, column_name_size,
                                         column_name_pos, "%s", QUOTATION_MARK))) {
        LOG_WARN("failed to print QUOTATION_MARK", K(ret), K(QUOTATION_MARK));
      } else if (OB_FAIL(databuff_printf(column_value_buf, column_value_size,
                                         column_value_pos, "%s", DELIMITER))) {
        LOG_WARN("failed to print delimiter", K(ret), K(DELIMITER));
      } else if (OB_FAIL(col_datum->to_obj(col_obj, col_obj_meta))) {
        LOG_WARN("to_obj failed", K(ret), K(*col_datum), K(col_obj_meta));
      } else if (OB_FAIL(col_obj.print_sql_literal(column_value_buf, column_value_size,
                                                   column_value_pos, get_obj_print_params(session)))) {
        LOG_WARN("failed to print column value", K(ret), K(*col_datum), K(col_obj));
      }
    }
    dynamic_column_name.assign(column_name_buf, column_name_pos);
    dynamic_column_value.assign(column_value_buf, column_value_pos);
    LOG_DEBUG("after generate dynamic sql info ====",K(dynamic_column_name), K(dynamic_column_value));
  }
  return ret;
}

int ObErrLogService::execute_write(uint64_t tenant_id, char *sql_str)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  common::ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
  common::ObOracleSqlProxy oracle_sql_proxy;
  if (OB_ISNULL(sql_proxy) || OB_ISNULL(sql_str)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy or sql_str should not be null");
  } else if (OB_FAIL(oracle_sql_proxy.init(sql_proxy->get_pool()))) {
    LOG_WARN("init oracle sql proxy failed", K(ret));
  } else if (OB_FAIL(oracle_sql_proxy.write(tenant_id, sql_str, affected_rows))) {
    LOG_WARN("execute sql failed", K(ret), K(sql_str));
  }
  return ret;
}

int ObErrLogService::insert_err_log_record(const ObSQLSessionInfo *session,
                                           const ObErrLogCtDef &err_log_ct_def,
                                           ObErrLogRtDef &err_log_rt_def,
                                           ObDASOpType type)
{
  int ret = OB_SUCCESS;
  char *sql_str = NULL;
  ObString dynamic_name_str;
  ObString dynamic_value_str;
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null");
  } else if (-1 != err_log_ct_def.reject_limit_ &&
             err_log_rt_def.curr_err_log_record_num_ >= err_log_ct_def.reject_limit_) {
    // Reached the limit, report an error directly to the upper layer
    ret = err_log_rt_def.first_err_ret_;
    LOG_WARN("limit is reach reject_limit", K(ret),
             K(err_log_ct_def.reject_limit_), K(err_log_rt_def.curr_err_log_record_num_));
  } else {
    lib::ContextParam param;
    param.set_mem_attr(MTL_ID(),
                       ObModIds::OB_SQL_INSERT,
                       ObCtxIds::DEFAULT_CTX_ID)
      .set_properties(lib::USE_TL_PAGE_OPTIONAL)
      .set_page_size(OB_MALLOC_NORMAL_BLOCK_SIZE)
      .set_ablock_size(lib::INTACT_MIDDLE_AOBJECT_SIZE);
    CREATE_WITH_TEMP_CONTEXT(param) {
      if (OB_FAIL(catch_err_and_gen_sql(CURRENT_CONTEXT->get_arena_allocator(),
                                        session,
                                        dynamic_name_str,
                                        dynamic_value_str,
                                        err_log_ct_def))) {
        LOG_WARN("fail to execute catch_err_and_gen_sql", K(ret));
      } else if (OB_FAIL(gen_insert_sql_str(CURRENT_CONTEXT->get_arena_allocator(),
                                            err_log_rt_def.first_err_ret_,
                                            err_log_ct_def,
                                            dynamic_name_str,
                                            dynamic_value_str,
                                            sql_str,
                                            type))) {
        LOG_WARN("fail to execute gen_insert_sql_str", K(ret));
      } else if (OB_FAIL(execute_write(session->get_effective_tenant_id(), sql_str))) {
        LOG_WARN("fail to execute execute_write", K(ret), K(sql_str));
      }
    }
  }
  return ret;
}
}  // namespace sql
}  // namespace oceanbase
