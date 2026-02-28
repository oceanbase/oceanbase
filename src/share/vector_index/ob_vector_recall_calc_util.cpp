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

#define USING_LOG_PREFIX SHARE

#include "ob_vector_recall_calc_util.h"
#include "ob_vector_index_util.h"
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/time/ob_time_utility.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "observer/ob_server_struct.h"
#include <cctype>

namespace oceanbase
{
namespace sql
{

using namespace common;
using namespace common::sqlclient;

// Definition for static constant member
const int64_t ObVectorRecallCalcUtil::VECTOR_RECALL_RATE_CALC_SQL_TIMEOUT;

// ===================== ObVectorSearchResult =====================

ObVectorSearchResult::ObVectorSearchResult()
  : rowkeys_(),
    query_latency_us_(0)
{
}

void ObVectorSearchResult::reset()
{
  rowkeys_.reset();
  query_latency_us_ = 0;
}

int ObVectorSearchResult::assign(const ObVectorSearchResult &other)
{
  int ret = OB_SUCCESS;

  rowkeys_.reset();
  if (OB_FAIL(rowkeys_.assign(other.rowkeys_))) {
    LOG_WARN("fail to assign rowkeys", K(ret));
  } else {
    query_latency_us_ = other.query_latency_us_;
  }

  return ret;
}

int sql::ObVectorRecallCalcUtil::execute_search_sql(
    uint64_t tenant_id,
    const ObSqlString &sql,
    ObMySQLProxy *sql_proxy,
    const ObRowkeyInfo *rowkey_info,
    ObIAllocator *allocator,
    ObVectorSearchResult &result,
    const int32_t group_id)
{
  int ret = OB_SUCCESS;
  int64_t start_time = ObTimeUtility::current_time();

  if (OB_INVALID_TENANT_ID == tenant_id || sql.empty() || OB_ISNULL(sql_proxy) ||
      OB_ISNULL(rowkey_info) || OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), KP(sql_proxy),
             KP(rowkey_info), KP(allocator));
  } else {
    std::unique_ptr<ObMySQLProxy::MySQLResult> res_ptr(new ObMySQLProxy::MySQLResult());
    ObMySQLProxy::MySQLResult &res = *res_ptr;
    ObMySQLResult *mysql_result = nullptr;
    // Create session param for timeout setting
    ObSessionParam session_param;
    session_param.consumer_group_id_ = group_id;

    result.reset();

    if (OB_FAIL(sql_proxy->read(res, tenant_id, sql.ptr(), &session_param))) {
      LOG_WARN("fail to execute sql", K(ret), K(tenant_id), K(group_id));
    } else if (OB_ISNULL(mysql_result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mysql result is null", K(ret));
    } else if (OB_FAIL(sql::ObVectorRecallCalcUtil::extract_rowkeys_from_result(
                         mysql_result, rowkey_info, allocator, result.rowkeys_))) {
        LOG_WARN("fail to extract rowkeys from result", K(ret));
    } else {
      result.query_latency_us_ = ObTimeUtility::current_time() - start_time;
      LOG_DEBUG("execute search sql success", K(tenant_id),
                "rowkey_count", result.rowkeys_.count(),
                K(result.query_latency_us_));
    }
  }

  return ret;
}

int sql::ObVectorRecallCalcUtil::calculate_recall_rate(
    const ObVectorSearchResult &approx_result,
    const ObVectorSearchResult &brute_result,
    double &recall_rate)
{
  int ret = OB_SUCCESS;
  recall_rate = 0.0;

  if (brute_result.rowkeys_.count() == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("brute result is empty, table has no data", K(ret));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "calc, brute result is empty");
  } else {
    // Count how many rowkeys in approximate result match the brute-force result
    int64_t match_count = 0;

    for (int64_t i = 0; OB_SUCC(ret) && i < approx_result.rowkeys_.count(); ++i) {
      const ObRowkey &approx_rowkey = approx_result.rowkeys_.at(i);
      for (int64_t j = 0; OB_SUCC(ret) && j < brute_result.rowkeys_.count(); ++j) {
        const ObRowkey &brute_rowkey = brute_result.rowkeys_.at(j);
        bool is_equal = false;
        if (OB_FAIL(approx_rowkey.equal(brute_rowkey, is_equal))) {
          LOG_WARN("fail to compare rowkeys", K(ret), K(approx_rowkey), K(brute_rowkey));
        } else if (is_equal) {
          match_count++;
          break;
        }
      }
    }

    if (OB_SUCC(ret)) {
      // Recall rate = matched count / brute-force result count
      recall_rate = static_cast<double>(match_count) /
                    static_cast<double>(brute_result.rowkeys_.count());

      LOG_DEBUG("calculate recall rate", K(match_count),
                "approx_count", approx_result.rowkeys_.count(),
                "brute_count", brute_result.rowkeys_.count(),
                K(recall_rate));
    }
  }

  return ret;
}

int sql::ObVectorRecallCalcUtil::parse_table_name_from_sql(
    const ObString &sql_query,
    const ObString &default_db_name,
    ObIAllocator *allocator,
    ObString &table_name,
    ObString &db_name)
{
  int ret = OB_SUCCESS;

  if (sql_query.empty() || OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(allocator));
  } else {
    // Convert SQL to uppercase for keyword matching
    ObString sql_upper;
    char *upper_buf = nullptr;
    if (OB_ISNULL(upper_buf = static_cast<char*>(allocator->alloc(sql_query.length())))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", K(ret), K(sql_query.length()));
    } else {
      for (int64_t i = 0; i < sql_query.length(); ++i) {
        upper_buf[i] = static_cast<char>(toupper(static_cast<unsigned char>(sql_query.ptr()[i])));
      }
      sql_upper.assign_ptr(upper_buf, sql_query.length());

      const char *sql_ptr = sql_upper.ptr();
      const char *from_pos = strstr(sql_ptr, "FROM ");

      if (OB_ISNULL(from_pos)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "calc, no FROM clause found in sql");
        LOG_WARN("no FROM clause found in sql", K(ret));
      } else {
        // Find the table name after FROM
        const char *table_start = from_pos + 5; // skip "FROM "

        // Skip whitespace
        while (table_start < sql_ptr + sql_upper.length() && isspace(*table_start)) {
          table_start++;
        }

        // Find end of table name
        const char *table_end = table_start;
        while (table_end < sql_ptr + sql_upper.length() &&
               !isspace(*table_end) &&
               *table_end != ',' &&
               *table_end != '(' &&
               *table_end != ';' &&
               *table_end != ')' &&
               strncmp(table_end, "WHERE", 5) != 0 &&
               strncmp(table_end, "ORDER", 5) != 0 &&
               strncmp(table_end, "GROUP", 5) != 0 &&
               strncmp(table_end, "HAVING", 6) != 0 &&
               strncmp(table_end, "LIMIT", 5) != 0 &&
               strncmp(table_end, "UNION", 5) != 0) {
          table_end++;
        }

        // Get the original case table name from original SQL
        const char *orig_sql_ptr = sql_query.ptr();
        const char *orig_table_start = orig_sql_ptr + (table_start - sql_ptr);
        const char *orig_table_end = orig_sql_ptr + (table_end - sql_ptr);
        int64_t table_len = orig_table_end - orig_table_start;

        if (table_len <= 0) {
          ret = OB_INVALID_ARGUMENT;
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "calc, did not find table name in sql");
          LOG_WARN("invalid table name length", K(ret), K(table_len));
        } else {
          table_name.assign_ptr(orig_table_start, static_cast<int32_t>(table_len));

          // Check if table name contains database prefix (db.table)
          const char *dot_pos = nullptr;
          for (const char *p = table_name.ptr(); p < table_name.ptr() + table_name.length(); p++) {
            if (*p == '.') {
              dot_pos = p;
              break;
            }
          }

          // If table name contains database prefix, extract database and table parts
          db_name = default_db_name;
          if (dot_pos != nullptr) {
            int64_t db_len = dot_pos - table_name.ptr();
            db_name.assign_ptr(table_name.ptr(), static_cast<int32_t>(db_len));
            table_name.assign_ptr(dot_pos + 1, static_cast<int32_t>(table_name.length() - db_len - 1));
          }

          LOG_DEBUG("parsed table name from sql", K(db_name), K(table_name));
        }
      }
    }
  }

  return ret;
}

int sql::ObVectorRecallCalcUtil::get_table_schema_by_name(
    uint64_t tenant_id,
    const ObString &db_name,
    const ObString &table_name,
    const share::schema::ObTableSchema *&table_schema,
    share::schema::ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;

  if (OB_INVALID_TENANT_ID == tenant_id || db_name.empty() || table_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(db_name), K(table_name));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("get schema guard failed", K(ret), K(tenant_id));
  } else {
    uint64_t database_id = OB_INVALID_ID;
    if (OB_FAIL(schema_guard.get_database_id(tenant_id, db_name, database_id))) {
      LOG_WARN("get database id failed", K(ret), K(tenant_id), K(db_name));
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, database_id, table_name,
                                                     false/*is_index*/, table_schema))) {
      LOG_WARN("get table schema failed", K(ret), K(tenant_id), K(database_id), K(table_name));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("table not exist", K(ret), K(db_name), K(table_name));
    } else {
      LOG_DEBUG("get table schema success", K(table_name), K(database_id));
    }
  }

  return ret;
}

int sql::ObVectorRecallCalcUtil::build_pk_select_sql(
    const ObString &sql_query,
    const share::schema::ObTableSchema *table_schema,
    const ObString &db_name,
    const ObString &table_name,
    ObSqlString &modified_sql)
{
  int ret = OB_SUCCESS;

  if (sql_query.empty() || OB_ISNULL(table_schema) || db_name.empty() || table_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(table_schema), K(db_name), K(table_name));
  } else {
    const ObRowkeyInfo *rowkey_info = &(table_schema->get_rowkey_info());
    int64_t rowkey_size = rowkey_info->get_size();

    // Parse the original SQL to extract the remaining clause after table name
    const char *orig_sql_ptr = sql_query.ptr();
    const char *sql_end = orig_sql_ptr + sql_query.length();
    const char *from_pos = strstr(orig_sql_ptr, "FROM ");
    if (OB_ISNULL(from_pos)) {
      from_pos = strstr(orig_sql_ptr, "from ");
    }

    if (OB_ISNULL(from_pos)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "calc, no FROM clause found in sql");
      LOG_WARN("no FROM clause found in sql", K(ret));
    } else {
      // Extract user hint from original SQL (between SELECT and FROM)
      // Look for /*+ ... */ pattern after SELECT keyword
      ObString user_hint;
      const char *select_pos = strstr(orig_sql_ptr, "SELECT ");
      if (OB_ISNULL(select_pos)) {
        select_pos = strstr(orig_sql_ptr, "select ");
      }
      if (OB_NOT_NULL(select_pos)) {
        const char *after_select = select_pos + 7; // skip "SELECT "
        // Skip whitespace after SELECT
        while (after_select < from_pos && isspace(*after_select)) {
          after_select++;
        }
        // Check if there's a hint starting with /*+
        if (after_select + 3 < from_pos &&
            after_select[0] == '/' && after_select[1] == '*' && after_select[2] == '+') {
          const char *hint_start = after_select;
          // Find the end of hint */
          const char *hint_end = strstr(hint_start, "*/");
          if (OB_NOT_NULL(hint_end) && hint_end < from_pos) {
            hint_end += 2; // include */
            user_hint.assign_ptr(hint_start, static_cast<int32_t>(hint_end - hint_start));
          }
        }
      }

      // Find the start of table name after FROM
      const char *after_from = from_pos + 5; // skip "FROM " or "from "
      while (after_from < sql_end && isspace(*after_from)) {
        after_from++;
      }

      // Find the end of table name
      const char *after_table = after_from;
      while (after_table < sql_end &&
             !isspace(*after_table) &&
             *after_table != ',' &&
             *after_table != '(') {
        after_table++;
      }

      // Find the remaining clause (WHERE, ORDER BY, etc.)
      const char *remaining_clause = after_table;
      int64_t remaining_len = sql_query.length() - (remaining_clause - orig_sql_ptr);
      ObString remaining_sql;
      if (remaining_len > 0) {
        remaining_sql.assign_ptr(remaining_clause, static_cast<int32_t>(remaining_len));
      }

      modified_sql.reset();

      if (rowkey_size > 0) {
        // Table has primary key, build SQL with primary key columns
        if (OB_FAIL(modified_sql.append("SELECT "))) {
          LOG_WARN("fail to append SELECT", K(ret));
        } else if (user_hint.length() > 0 && OB_FAIL(modified_sql.append(user_hint))) {
          // Append user hint if exists
          LOG_WARN("fail to append user hint", K(ret), K(user_hint));
        } else if (user_hint.length() > 0 && OB_FAIL(modified_sql.append(" "))) {
          LOG_WARN("fail to append space after hint", K(ret));
        } else {
          // Add all primary key columns
          for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_size; ++i) {
            uint64_t column_id = OB_INVALID_ID;
            if (OB_FAIL(rowkey_info->get_column_id(i, column_id))) {
              LOG_WARN("fail to get column id", K(ret), K(i));
            } else {
              const share::schema::ObColumnSchemaV2 *column_schema = table_schema->get_column_schema(column_id);
              if (OB_ISNULL(column_schema)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("column schema is null", K(ret), K(column_id));
              } else {
                ObString column_name = column_schema->get_column_name_str();
                if (i > 0) {
                  if (OB_FAIL(modified_sql.append(", "))) {
                    LOG_WARN("fail to append comma", K(ret));
                  }
                }
                if (OB_SUCC(ret)) {
                  if (OB_FAIL(modified_sql.append("`"))) {
                    LOG_WARN("fail to append backtick", K(ret));
                  } else if (OB_FAIL(modified_sql.append(column_name))) {
                    LOG_WARN("fail to append column name", K(ret), K(column_name));
                  } else if (OB_FAIL(modified_sql.append("`"))) {
                    LOG_WARN("fail to append backtick", K(ret));
                  }
                }
              }
            }
          }
        }
      } else {
        // Table has no primary key, use hidden column __pk_increment
        // Need to merge user hint with hidden_column_visible hint
        if (user_hint.length() > 0) {
          // Extract hint content from user hint (remove /*+ and */)
          ObString user_hint_content;
          if (user_hint.length() > 5) { // at least "/*+ */" = 6 chars
            user_hint_content.assign_ptr(user_hint.ptr() + 3, user_hint.length() - 5);
          }
          if (OB_FAIL(modified_sql.append("SELECT /*+ opt_param('hidden_column_visible','true') "))) {
            LOG_WARN("fail to append SELECT with hidden column hint start", K(ret));
          } else if (user_hint_content.length() > 0 && OB_FAIL(modified_sql.append(user_hint_content))) {
            LOG_WARN("fail to append user hint content", K(ret), K(user_hint_content));
          } else if (OB_FAIL(modified_sql.append_fmt("*/ `%s`", OB_HIDDEN_PK_INCREMENT_COLUMN_NAME))) {
            LOG_WARN("fail to append hidden column hint end", K(ret));
          }
        } else {
          if (OB_FAIL(modified_sql.append_fmt("SELECT /*+ opt_param('hidden_column_visible','true') */ `%s`", OB_HIDDEN_PK_INCREMENT_COLUMN_NAME))) {
            LOG_WARN("fail to append SELECT with hidden column", K(ret));
          }
        }
      }

      // Append FROM database_name.table_name
      if (OB_SUCC(ret)) {
        if (OB_FAIL(modified_sql.append(" FROM "))) {
          LOG_WARN("fail to append FROM", K(ret));
        } else if (OB_FAIL(modified_sql.append(db_name))) {
          LOG_WARN("fail to append database name", K(ret), K(db_name));
        } else if (OB_FAIL(modified_sql.append("."))) {
          LOG_WARN("fail to append dot", K(ret));
        } else if (OB_FAIL(modified_sql.append(table_name))) {
          LOG_WARN("fail to append table name", K(ret), K(table_name));
        } else if (remaining_sql.length() > 0 && OB_FAIL(modified_sql.append(remaining_sql))) {
          LOG_WARN("fail to append remaining clause", K(ret));
        }
      }

      LOG_DEBUG("build pk select sql success", K(rowkey_size), K(user_hint));
    }
  }

  return ret;
}

static int64_t find_parameters_clause(const ObString &sql)
{
  const char *p = sql.ptr();
  const int64_t len = sql.length();
  if (len == 0) return -1;

  const char *key_approx_long = "APPROXIMATE";
  const char *key_approx_short = "APPROX";
  const size_t len_approx_long = 10;
  const size_t len_approx_short = 5;

  // Find last APPROXIMATE (from end backward, case-insensitive)
  int64_t last_approx_long = -1;
  for (int64_t i = len - (int64_t)len_approx_long; i >= 0; --i) {
    bool match = true;
    for (size_t j = 0; j < len_approx_long; ++j) {
      if (toupper(static_cast<unsigned char>(p[i + j])) != key_approx_long[j]) {
        match = false;
        break;
      }
    }
    if (match) {
      last_approx_long = i;
      break;
    }
  }

  // Find last APPROX (from end backward, case-insensitive)
  int64_t last_approx_short = -1;
  for (int64_t i = len - (int64_t)len_approx_short; i >= 0; --i) {
    bool match = true;
    for (size_t j = 0; j < len_approx_short; ++j) {
      if (toupper(static_cast<unsigned char>(p[i + j])) != key_approx_short[j]) {
        match = false;
        break;
      }
    }
    if (match) {
      last_approx_short = i;
      break;
    }
  }

  // Take the rightmost keyword (larger position wins)
  int64_t approx_pos = -1;
  size_t keyword_len = 0;
  if (last_approx_long >= 0 && (last_approx_short < 0 || last_approx_long >= last_approx_short)) {
    approx_pos = last_approx_long;
    keyword_len = len_approx_long;
  } else if (last_approx_short >= 0) {
    approx_pos = last_approx_short;
    keyword_len = len_approx_short;
  }

  if (approx_pos < 0) return -1;

  // Search for "PARAMETERS(" only in the suffix after approx/approximate (case-insensitive)
  const char *suffix_start = p + approx_pos + keyword_len;
  const int64_t suffix_len = len - (approx_pos + (int64_t)keyword_len);
  const char *key_params = "PARAMETERS(";
  const size_t key_params_len = 10;
  if (suffix_len < (int64_t)key_params_len) return -1;

  const char *end = p + len;
  for (const char *s = suffix_start; s <= end - (int64_t)key_params_len; ++s) {
    bool match = true;
    for (size_t i = 0; i < key_params_len; ++i) {
      if (toupper(static_cast<unsigned char>(s[i])) != key_params[i]) {
        match = false;
        break;
      }
    }
    if (match) return static_cast<int64_t>(s - p);
  }
  return -1;
}

// Find first occurrence of needle in buf[0..buf_len); returns start index or -1. Safe for non-null-terminated buf.
static int64_t find_substr_in_buf(const char *buf, int64_t buf_len,
                                  const char *needle, int64_t needle_len)
{
  if (buf == nullptr || needle == nullptr || buf_len < needle_len || needle_len <= 0) {
    return -1;
  }
  for (int64_t i = 0; i <= buf_len - needle_len; ++i) {
    bool match = true;
    for (int64_t j = 0; j < needle_len; ++j) {
      if (buf[i + j] != needle[j]) {
        match = false;
        break;
      }
    }
    if (match) return i;
  }
  return -1;
}

static int merge_params_string(const ObString &existing,
                               uint64_t nprobes,
                               uint64_t ef_search,
                               ObSqlString &out)
{
  int ret = OB_SUCCESS;
  out.reset();
  ObString rest(existing);
  bool has_nprobes = false;
  bool has_ef = false;
  while (!rest.empty() && OB_SUCC(ret)) {
    int32_t rest_len = rest.length();
    const char *rest_ptr = rest.ptr();
    int32_t comma_offset = -1;
    for (int32_t k = 0; k < rest_len; ++k) {
      if (rest_ptr[k] == ',') {
        comma_offset = k;
        break;
      }
    }
    ObString token;
    if (comma_offset >= 0) {
      token.assign_ptr(rest_ptr, comma_offset);
      rest.assign_ptr(rest_ptr + comma_offset + 1, rest_len - comma_offset - 1);
    } else {
      token = rest;
      rest.reset();
    }
    int32_t tl = token.length();
    int32_t trim_start = 0;
    while (trim_start < tl) {
      unsigned char c = static_cast<unsigned char>(token.ptr()[trim_start]);
      if (c != ' ' && c != '\t') break;
      ++trim_start;
    }
    if (trim_start >= tl) continue;
    token.assign_ptr(token.ptr() + trim_start, tl - trim_start);
    tl = token.length();
    if (tl == 0) continue;
    const char *tp = token.ptr();
    bool is_nprobes = (tl >= 12 && strncasecmp(tp, "IVF_NPROBES=", 12) == 0);
    bool is_ef = (tl >= 10 && strncasecmp(tp, "EF_SEARCH=", 10) == 0);
    if (is_nprobes) {
      has_nprobes = true;
      if (out.length() > 0 && OB_FAIL(out.append(","))) return ret;
      if (OB_FAIL(out.append_fmt("IVF_NPROBES=%lu", nprobes))) return ret;
    } else if (is_ef) {
      has_ef = true;
      if (out.length() > 0 && OB_FAIL(out.append(","))) return ret;
      if (OB_FAIL(out.append_fmt("EF_SEARCH=%lu", ef_search))) return ret;
    } else {
      if (out.length() > 0 && OB_FAIL(out.append(","))) return ret;
      if (OB_FAIL(out.append(tp, tl))) return ret;
    }
  }
  if (OB_SUCC(ret) && !has_nprobes) {
    if (out.length() > 0 && OB_FAIL(out.append(","))) return ret;
    if (OB_FAIL(out.append_fmt("IVF_NPROBES=%lu", nprobes))) return ret;
  }
  if (OB_SUCC(ret) && !has_ef) {
    if (out.length() > 0 && OB_FAIL(out.append(","))) return ret;
    if (OB_FAIL(out.append_fmt("EF_SEARCH=%lu", ef_search))) return ret;
  }
  return ret;
}

int sql::ObVectorRecallCalcUtil::inject_session_params_into_approx_sql(
    const common::ObString &approx_sql,
    uint64_t session_ivf_nprobes,
    uint64_t session_hnsw_ef_search,
    common::ObIAllocator *allocator,
    common::ObSqlString &output_sql)
{
  int ret = OB_SUCCESS;
  output_sql.reset();
  if (approx_sql.empty() || OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(allocator));
    return ret;
  }
  const char *sql_ptr = approx_sql.ptr();
  const int64_t sql_len = approx_sql.length();
  const int64_t params_pos = find_parameters_clause(approx_sql);
  if (params_pos >= 0) {
    const int64_t content_start_idx = params_pos + 10;
    if (content_start_idx >= sql_len) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("PARAMETERS( has no content", K(ret));
      return ret;
    }
    int depth = 1;
    int64_t content_end_idx = -1;
    for (int64_t i = content_start_idx; i < sql_len; ++i) {
      unsigned char c = static_cast<unsigned char>(sql_ptr[i]);
      if (c == '(') {
        depth++;
      } else if (c == ')') {
        depth--;
        if (depth == 0) {
          content_end_idx = i;
          break;
        }
      }
    }
    if (content_end_idx < 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("PARAMETERS clause has no matching )", K(ret));
      return ret;
    }
    const int32_t content_len = static_cast<int32_t>(content_end_idx - content_start_idx);
    ObString existing_content;
    existing_content.assign_ptr(sql_ptr + content_start_idx, content_len);
    ObSqlString merged_content;
    if (OB_FAIL(merge_params_string(existing_content, session_ivf_nprobes, session_hnsw_ef_search, merged_content))) {
      LOG_WARN("fail to merge params string", K(ret));
      return ret;
    }
    if (OB_FAIL(output_sql.append(sql_ptr, params_pos))) return ret;
    if (OB_FAIL(output_sql.append("PARAMETERS("))) return ret;
    if (OB_FAIL(output_sql.append(merged_content.ptr(), merged_content.length()))) return ret;
    if (OB_FAIL(output_sql.append(")"))) return ret;
    const int64_t suffix_start_idx = content_end_idx + 1;
    const int64_t suffix_len = sql_len - suffix_start_idx;
    if (suffix_len > 0 && OB_FAIL(output_sql.append(sql_ptr + suffix_start_idx, suffix_len))) return ret;
  } else {
    // No PARAMETERS clause; find "LIMIT <n>" after APPROXIMATE and insert PARAMETERS after the number.
    // Use length-bounded search (upper_buf is not null-terminated).
    char *upper_buf = static_cast<char *>(allocator->alloc(sql_len));
    if (OB_ISNULL(upper_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc upper buf", K(ret));
      return ret;
    }
    for (int64_t i = 0; i < sql_len; ++i) {
      upper_buf[i] = static_cast<char>(toupper(static_cast<unsigned char>(sql_ptr[i])));
    }
    static const char APPROXIMATE[] = "APPROXIMATE";
    static const char APPROX_SP[] = "APPROX ";
    static const char SP_APPROX[] = " APPROX ";
    static const char LIMIT_SP[] = "LIMIT ";
    int64_t approx_idx = find_substr_in_buf(upper_buf, sql_len, APPROXIMATE, sizeof(APPROXIMATE) - 1);
    if (approx_idx < 0) approx_idx = find_substr_in_buf(upper_buf, sql_len, APPROX_SP, sizeof(APPROX_SP) - 1);
    if (approx_idx < 0) approx_idx = find_substr_in_buf(upper_buf, sql_len, SP_APPROX, sizeof(SP_APPROX) - 1);
    if (approx_idx < 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("no APPROXIMATE/APPROX found in sql", K(ret));
      return ret;
    }
    const int64_t search_start = approx_idx;
    const int64_t search_len = sql_len - search_start;
    int64_t limit_idx = find_substr_in_buf(upper_buf + search_start, search_len, LIMIT_SP, sizeof(LIMIT_SP) - 1);
    if (limit_idx < 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("no LIMIT found after APPROXIMATE in sql", K(ret));
      return ret;
    }
    int64_t num_start_idx = search_start + limit_idx + static_cast<int64_t>(sizeof(LIMIT_SP) - 1);
    while (num_start_idx < sql_len) {
      unsigned char c = static_cast<unsigned char>(upper_buf[num_start_idx]);
      if (c != ' ' && c != '\t') break;
      ++num_start_idx;
    }
    int64_t num_end_idx = num_start_idx;
    while (num_end_idx < sql_len) {
      unsigned char c = static_cast<unsigned char>(upper_buf[num_end_idx]);
      if (c < '0' || c > '9') break;
      ++num_end_idx;
    }
    const int64_t insert_offset = num_end_idx;
    if (OB_FAIL(output_sql.append(sql_ptr, insert_offset))) {
      LOG_WARN("fail to append sql", K(ret));
    } else if (OB_FAIL(output_sql.append_fmt(" PARAMETERS(IVF_NPROBES=%lu, EF_SEARCH=%lu)",
                                      session_ivf_nprobes, session_hnsw_ef_search))) {
      LOG_WARN("fail to append parameters", K(ret));
    } else if (insert_offset < sql_len && OB_FAIL(output_sql.append(sql_ptr + insert_offset, sql_len - insert_offset))) {
      LOG_WARN("fail to append sql", K(ret), K(insert_offset), K(sql_len));
    } else {
      // do nothing
    }
  }
  return ret;
}

int sql::ObVectorRecallCalcUtil::extract_rowkeys_from_result(
    ObMySQLResult *result,
    const ObRowkeyInfo *rowkey_info,
    ObIAllocator *allocator,
    ObIArray<ObRowkey> &rowkeys)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(result) || OB_ISNULL(rowkey_info) || OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(result), KP(rowkey_info), KP(allocator));
  } else {
    rowkeys.reset();
    const int64_t rowkey_cnt = rowkey_info->get_size();

    if (rowkey_cnt <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid rowkey count", K(ret), K(rowkey_cnt));
    }

    while (OB_SUCC(ret) && OB_SUCC(result->next())) {
      ObObj *obj_ptr = nullptr;
      void *buf = nullptr;

      // Allocate memory for ObObj array
      if (OB_ISNULL(buf = allocator->alloc(sizeof(ObObj) * rowkey_cnt))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret), K(rowkey_cnt));
      } else {
        obj_ptr = new (buf) ObObj[rowkey_cnt];

        // Extract each rowkey column
        for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_cnt; ++i) {
          ObObj tmp_obj;
          if (OB_FAIL(result->get_obj(i, tmp_obj))) {
            LOG_WARN("fail to get obj from result", K(ret), K(i));
          } else if (OB_FAIL(ob_write_obj(*allocator, tmp_obj, obj_ptr[i]))) {
            LOG_WARN("deep copy obj failed", K(ret));
          }
        }

        // Construct ObRowkey and push to array
        if (OB_SUCC(ret)) {
          ObRowkey rowkey(obj_ptr, rowkey_cnt);
          if (OB_FAIL(rowkeys.push_back(rowkey))) {
            LOG_WARN("fail to push rowkey", K(ret), K(rowkey));
          }
        }
      }
    }

    // OB_ITER_END is expected
    if (ret == OB_ITER_END) {
      ret = OB_SUCCESS;
    }

    LOG_DEBUG("extract rowkeys from result", K(ret), "rowkey_count", rowkeys.count());
  }

  return ret;
}

} // end namespace sql
} // end namespace oceanbase
