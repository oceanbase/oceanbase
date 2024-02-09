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

#define USING_LOG_PREFIX SQL_QRR
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/schema/ob_schema_utils.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_sql_string.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/mysqlclient/ob_mysql_connection.h"
#include "lib/mysqlclient/ob_mysql_statement.h"
#include "lib/mysqlclient/ob_mysql_connection_pool.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/ob_max_id_fetcher.h"
#include "observer/ob_sql_client_decorator.h"
#include "sql/printer/ob_raw_expr_printer.h"
#include "sql/udr/ob_udr_sql_service.h"

namespace oceanbase
{
using namespace share;
using namespace share::schema;
using namespace common::sqlclient;
namespace sql
{

#define UDR_START_TRANS(tenant_id)                            \
ObMySQLTransaction trans;                                     \
if (OB_FAIL(ret)) {                                           \
} else if (OB_ISNULL(sql_proxy_)) {                           \
  ret = OB_ERR_UNEXPECTED;                                    \
  LOG_WARN("get unexpected null", K(ret), K(sql_proxy_));     \
} else if (OB_FAIL(trans.start(sql_proxy_, tenant_id))) {     \
  LOG_WARN("fail to start transaction", K(ret));              \
}                                                             \

#define UDR_END_TRANS                                         \
if (trans.is_started()) {                                     \
  const bool is_commit = (OB_SUCCESS == ret);                 \
  int tmp_ret = trans.end(is_commit);                         \
  if (OB_SUCCESS != tmp_ret) {                                \
    LOG_WARN("end trans failed", K(tmp_ret), K(is_commit));   \
    ret = (OB_SUCCESS == ret) ? tmp_ret : ret;                \
  }                                                           \
}                                                             \

int ObUDRSqlService::init(ObMySQLProxy *proxy)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("rewrite rule sql service init twice", K(ret));
  } else if (OB_ISNULL(proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null mysql proxy", K(ret));
  } else {
    sql_proxy_ = proxy;
    inited_ = true;
  }
  return ret;
}

int ObUDRSqlService::gen_insert_rule_dml(const ObUDRInfo &arg,
                                         const uint64_t tenant_id,
                                         oceanbase::share::ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(tenant_id,
                                                                                  arg.tenant_id_)))
    || OB_FAIL(dml.add_pk_column("rule_name", ObHexEscapeSqlStr(arg.rule_name_)))
    || OB_FAIL(dml.add_column("rule_id", arg.rule_id_))
    || OB_FAIL(dml.add_column("pattern", ObHexEscapeSqlStr(arg.pattern_)))
    || OB_FAIL(dml.add_column("db_name", ObHexEscapeSqlStr(arg.db_name_)))
    || OB_FAIL(dml.add_column("replacement", ObHexEscapeSqlStr(arg.replacement_)))
    || OB_FAIL(dml.add_column("normalized_pattern", ObHexEscapeSqlStr(arg.normalized_pattern_)))
    || OB_FAIL(dml.add_column("status", static_cast<int64_t>(arg.rule_status_)))
    || OB_FAIL(dml.add_column("version", arg.rule_version_))
    || OB_FAIL(dml.add_uint64_column("pattern_digest", arg.pattern_digest_))
    || OB_FAIL(dml.add_column("fixed_param_infos", ObHexEscapeSqlStr(arg.fixed_param_infos_str_.empty() ?
                                                   ObString("") : arg.fixed_param_infos_str_)))
    || OB_FAIL(dml.add_column("dynamic_param_infos", ObHexEscapeSqlStr(arg.dynamic_param_infos_str_.empty() ?
                                                     ObString("") : arg.dynamic_param_infos_str_)))
    || OB_FAIL(dml.add_column("def_name_ctx_str", ObHexEscapeSqlStr(arg.question_mark_ctx_str_.empty() ?
                                                     ObString("") : arg.question_mark_ctx_str_)))) {
    LOG_WARN("add column failed", K(ret), K(arg));
  }
  return ret;
}

int ObUDRSqlService::gen_modify_rule_status_dml(const ObUDRInfo &arg,
                                                const uint64_t tenant_id,
                                                oceanbase::share::ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(tenant_id,
                                                                                  arg.tenant_id_)))
    || OB_FAIL(dml.add_pk_column("rule_name", ObHexEscapeSqlStr(arg.rule_name_)))
    || OB_FAIL(dml.add_column("version", arg.rule_version_))
    || OB_FAIL(dml.add_column("status", static_cast<int64_t>(arg.rule_status_)))) {
    LOG_WARN("modify column failed", K(ret), K(arg));
  }
  return ret;
}

int ObUDRSqlService::fetch_new_rule_version(const uint64_t tenant_id,
                                            int64_t &new_rule_version)
{
  int ret = OB_SUCCESS;
  uint64_t rule_version = OB_INVALID_VERSION;
  ObMaxIdFetcher id_fetcher(*sql_proxy_);
  if (OB_FAIL(id_fetcher.fetch_new_max_id(tenant_id,
                                          OB_MAX_USED_REWRITE_RULE_VERSION_TYPE,
                                          rule_version, OB_INIT_REWRITE_RULE_VERSION))) {
    LOG_WARN("fetch_new_max_id failed", K(ret), "id_type", OB_MAX_USED_REWRITE_RULE_VERSION_TYPE);
  } else {
    new_rule_version = rule_version;
  }
  return ret;
}

int ObUDRSqlService::fetch_max_rule_version(const uint64_t tenant_id,
                                            int64_t &max_rule_version)
{
  int ret = OB_SUCCESS;
  uint64_t rule_version = OB_INVALID_VERSION;
  ObMaxIdFetcher id_fetcher(*sql_proxy_);
  if (OB_FAIL(id_fetcher.fetch_max_id(*sql_proxy_, tenant_id,
              OB_MAX_USED_REWRITE_RULE_VERSION_TYPE, rule_version))) {
    LOG_WARN("failed to fetch max rule version", K(ret), K(tenant_id));
  } else {
    max_rule_version = rule_version;
  }
  return ret;
}

int ObUDRSqlService::fetch_new_rule_id(const uint64_t tenant_id,
                                       int64_t &new_rule_id)
{
  int ret = OB_SUCCESS;
  uint64_t rule_id = OB_INVALID_ID;
  ObMaxIdFetcher id_fetcher(*sql_proxy_);
  if (OB_FAIL(id_fetcher.fetch_new_max_id(tenant_id,
                                          OB_MAX_USED_OBJECT_ID_TYPE,
                                          rule_id))) {
    LOG_WARN("fetch_new_max_id failed", K(ret), "id_type", OB_MAX_USED_OBJECT_ID_TYPE);
  } else {
    new_rule_id = rule_id;
  }
  return ret;
}

int ObUDRSqlService::insert_rule(ObUDRInfo &arg)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  int64_t affected_rows = 0;
  int64_t rule_id = OB_INVALID_ID;
  int64_t rule_version = OB_INVALID_VERSION;
  UDR_START_TRANS(arg.tenant_id_);
  LOG_INFO("insert rule", K(arg));
  ObDMLExecHelper exec(trans, arg.tenant_id_);
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(fetch_new_rule_version(arg.tenant_id_, rule_version))) {
    LOG_WARN("failed to fetch new rule version", K(ret));
  } else if (FALSE_IT(arg.rule_version_ = rule_version)) {
  } else if (OB_FAIL(fetch_new_rule_id(arg.tenant_id_, rule_id))) {
    LOG_WARN("failed to fetch new rule id", K(ret));
  } else if (FALSE_IT(arg.rule_id_ = rule_id)) {
  } else if (OB_FAIL(gen_insert_rule_dml(arg, arg.tenant_id_, dml))) {
    LOG_WARN("failed to gen rewrite rule dml", K(ret));
  } else if (OB_FAIL(exec.exec_insert(OB_ALL_TENANT_REWRITE_RULES_TNAME, dml, affected_rows))) {
    LOG_WARN("execute insert failed", K(ret));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("affected_rows unexpected to be one", K(affected_rows), K(ret));
  }
  UDR_END_TRANS;
  return ret;
}

int ObUDRSqlService::alter_rule_status(ObUDRInfo &arg)
{
  int ret = OB_SUCCESS;
  UDR_START_TRANS(arg.tenant_id_);
  ObDMLSqlSplicer dml;
  int64_t affected_rows = 0;
  ObDMLExecHelper exec(trans, arg.tenant_id_);
  int64_t rule_version = OB_INVALID_VERSION;
  LOG_INFO("alter rule status", K(arg));
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(fetch_new_rule_version(arg.tenant_id_, rule_version))) {
    LOG_WARN("failed to fetch new rule version", K(ret));
  } else if (FALSE_IT(arg.rule_version_ = rule_version)) {
  } else if (OB_FAIL(gen_modify_rule_status_dml(arg, arg.tenant_id_, dml))) {
    LOG_WARN("failed to gen rewrite rule dml", K(ret));
  } else if (OB_FAIL(exec.exec_update(OB_ALL_TENANT_REWRITE_RULES_TNAME, dml, affected_rows))) {
    LOG_WARN("execute insert failed", K(ret));
  } else if (!is_single_row(affected_rows) && !is_zero_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("affected_rows unexpected", K(affected_rows), K(ret));
  }
  UDR_END_TRANS;
  return ret;
}

int ObUDRSqlService::gen_recyclebin_rule_name(const int64_t rule_version,
                                              const int64_t buf_len,
                                              char *buf,
                                              ObString &recyclebin_rule_name)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s", "__recyclebin_"))) {
    LOG_WARN("append name to buf error", K(ret));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%ld", rule_version))) {
    LOG_WARN("append name to buf error", K(ret));
  } else {
    recyclebin_rule_name.assign_ptr(buf, pos);
  }
  return ret;
}

int ObUDRSqlService::remove_rule(ObUDRInfo &arg)
{
  int ret = OB_SUCCESS;
  UDR_START_TRANS(arg.tenant_id_);
  ObSqlString sql;
  ObSqlString rule_name_sql_str;
  int64_t affected_rows = 0;
  int64_t rule_version = OB_INVALID_VERSION;
  ObString recyclebin_rule_name;
  char buf[OB_MAX_ORIGINAL_NANE_LENGTH] = {0};
  const int64_t buf_len = OB_MAX_ORIGINAL_NANE_LENGTH;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(fetch_new_rule_version(arg.tenant_id_, rule_version))) {
    LOG_WARN("failed to fetch new rule version", K(ret));
  } else if (OB_FAIL(gen_recyclebin_rule_name(rule_version, buf_len, buf, recyclebin_rule_name))) {
    LOG_WARN("failed to gen recyclebin rule name", K(ret));
  } else if (OB_FAIL(sql_append_hex_escape_str(arg.rule_name_, rule_name_sql_str))) {
    LOG_WARN("failed to process rule name", K(ret));
  } else if (sql.assign_fmt("update %s set gmt_modified = now(), rule_name = '%.*s' ,\
                            version = %ld, status = %ld \
                            where tenant_id = %lu and rule_name = %.*s",
            OB_ALL_TENANT_REWRITE_RULES_TNAME,
            LEN_AND_PTR(recyclebin_rule_name),
            rule_version,
            static_cast<int64_t>(ObUDRInfo::DELETE_STATUS),
            ObSchemaUtils::get_extract_tenant_id(arg.tenant_id_, arg.tenant_id_),
            LEN_AND_PTR(rule_name_sql_str.string()))) {
    LOG_WARN("update from __all_tenant_rewrite_rules table failed.", K(ret));
  } else if (OB_FAIL(trans.write(arg.tenant_id_, sql.ptr(), affected_rows))) {
    LOG_WARN("fail to exec sql", K(ret), K(sql));
  } else if (!is_single_row(affected_rows) && !is_zero_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("affected_rows unexpected", K(affected_rows), K(ret), K(sql));
  }
  UDR_END_TRANS;
  return ret;
}

int ObUDRSqlService::clean_up_items_marked_for_deletion(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  UDR_START_TRANS(tenant_id);
  ObSqlString sql;
  int64_t affected_rows = 0;
  if (OB_FAIL(ret)) {
  } else if (sql.assign_fmt("delete FROM %s WHERE status = %ld \
                             AND DATEDIFF(now(), gmt_modified) >= %ld",
            OB_ALL_TENANT_REWRITE_RULES_TNAME,
            static_cast<int64_t>(ObUDRInfo::DELETE_STATUS),
            DELETE_DATE_INTERVAL_THRESHOLD)) {
    LOG_WARN("delete from __all_tenant_rewrite_rules table failed.", K(ret));
  } else if (OB_FAIL(trans.write(tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("fail to exec sql", K(ret));
  } else {
    LOG_INFO("affected rows", K(affected_rows));
  }
  UDR_END_TRANS;
  return ret;
}

int ObUDRSqlService::get_need_sync_rule_infos(ObIAllocator& allocator,
                                              const uint64_t tenant_id,
                                              const int64_t local_rule_version,
                                              ObIArray<ObUDRInfo>& rule_infos)
{
  int ret = OB_SUCCESS;
  ObSQLClientRetryWeak sql_client_retry_weak(sql_proxy_);
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    sqlclient::ObMySQLResult *result = NULL;
    ObSqlString sql;
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    int64_t affected_rows = 0;
    const char* sql_fmt = "select rule_id, rule_name, pattern, db_name, replacement, normalized_pattern, "
                          "status, version, pattern_digest, fixed_param_infos, "
                          "dynamic_param_infos, def_name_ctx_str "
                          "from %s where tenant_id = %lu and version > %ld;";
    if (OB_ISNULL(sql_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(sql.assign_fmt(sql_fmt, OB_ALL_TENANT_REWRITE_RULES_TNAME,
                                      ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id),
                                      local_rule_version))) {
      LOG_WARN("failed to assign format sql", K(ret));
    } else if (OB_FAIL(sql_client_retry_weak.read(res, exec_tenant_id, sql.ptr()))) {
      LOG_WARN("execute sql failed", "sql", sql.ptr(), K(ret));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to execute ", "sql", sql.ptr(), K(ret));
    } else {
      while (OB_SUCC(result->next())) {
        ObUDRInfo rule_info;
        rule_info.tenant_id_ = tenant_id;
        EXTRACT_INT_FIELD_MYSQL(*result, "status", rule_info.rule_status_, ObUDRInfo::RuleStatus);
        EXTRACT_INT_FIELD_MYSQL(*result, "rule_id", rule_info.rule_id_, int64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "version", rule_info.rule_version_, int64_t);
        EXTRACT_UINT_FIELD_MYSQL(*result, "pattern_digest", rule_info.pattern_digest_, uint64_t);
        EXTRACT_VARCHAR_FIELD_MYSQL(*result, "rule_name", rule_info.rule_name_);
        EXTRACT_VARCHAR_FIELD_MYSQL(*result, "pattern", rule_info.pattern_);
        EXTRACT_VARCHAR_FIELD_MYSQL(*result, "db_name", rule_info.db_name_);
        EXTRACT_VARCHAR_FIELD_MYSQL(*result, "replacement", rule_info.replacement_);
        EXTRACT_VARCHAR_FIELD_MYSQL(*result, "normalized_pattern", rule_info.normalized_pattern_);
        EXTRACT_VARCHAR_FIELD_MYSQL(*result, "fixed_param_infos", rule_info.fixed_param_infos_str_);
        EXTRACT_VARCHAR_FIELD_MYSQL(*result, "dynamic_param_infos", rule_info.dynamic_param_infos_str_);
        EXTRACT_VARCHAR_FIELD_MYSQL(*result, "def_name_ctx_str", rule_info.question_mark_ctx_str_);

        if (OB_SUCC(ret)) {
          if (OB_FAIL(ob_write_string(allocator, rule_info.rule_name_, rule_info.rule_name_))) {
            LOG_WARN("failed to write string");
          } else if (OB_FAIL(ob_write_string(allocator, rule_info.pattern_, rule_info.pattern_))) {
            LOG_WARN("failed to write string");
          } else if (OB_FAIL(ob_write_string(allocator, rule_info.db_name_, rule_info.db_name_))) {
            LOG_WARN("failed to write string");
          } else if (OB_FAIL(ob_write_string(allocator, rule_info.replacement_, rule_info.replacement_))) {
            LOG_WARN("failed to write string");
          } else if (OB_FAIL(ob_write_string(allocator, rule_info.normalized_pattern_, rule_info.normalized_pattern_))) {
            LOG_WARN("failed to write string");
          } else if (OB_FAIL(ob_write_string(allocator, rule_info.fixed_param_infos_str_, rule_info.fixed_param_infos_str_))) {
            LOG_WARN("failed to write string");
          } else if (OB_FAIL(ob_write_string(allocator, rule_info.dynamic_param_infos_str_, rule_info.dynamic_param_infos_str_))) {
            LOG_WARN("failed to write string");
          } else if (OB_FAIL(ob_write_string(allocator, rule_info.question_mark_ctx_str_, rule_info.question_mark_ctx_str_))) {
            LOG_WARN("failed to write string");
          } else if (OB_FAIL(rule_infos.push_back(rule_info))) {
            LOG_WARN("failed to push back rule info", K(ret));
          }
        }
      }
    }
    if (OB_ITER_END != ret) {
      LOG_WARN("get next row failed", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

#undef UDR_START_TRANS
#undef UDR_END_TRANS

} // namespace oceanbase end
} // namespace sql end
