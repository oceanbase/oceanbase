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
#include "share/ob_global_context_operator.h"
#include "lib/string/ob_string.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_schema_struct.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "share/schema/ob_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_service_sql_impl.h"
#include "rootserver/ob_ddl_operator.h"

using namespace oceanbase::common;
using namespace oceanbase::common::sqlclient;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

ObGlobalContextOperator::ObGlobalContextOperator()
{
}

ObGlobalContextOperator::~ObGlobalContextOperator()
{
}

int ObGlobalContextOperator::clean_global_context(const ObIArray<uint64_t> &tenant_ids,
                                                  ObISQLClient &sql_proxy,
                                                  ObMultiVersionSchemaService &schema_service)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.count(); i++) {
    uint64_t curr_tenant_id = tenant_ids.at(i);
    const uint64_t exec_tenant_id = gen_meta_tenant_id(curr_tenant_id);
    uint64_t lower_bound_ctx_id = 0;
    const int64_t batch_count = 500;
    bool clean_finish = false;
    while (OB_SUCC(ret) && !clean_finish) {
      ObArray<uint64_t> ctx_ids;
      SMART_VAR(common::ObMySQLProxy::MySQLResult, res) {
        common::sqlclient::ObMySQLResult *result = NULL;
        if (OB_FAIL(sql.assign_fmt("SELECT DISTINCT context_id FROM %s WHERE tenant_id = %lu AND context_id > %lu LIMIT %lu",
                          OB_ALL_GLOBAL_CONTEXT_VALUE_TNAME,
                          curr_tenant_id,
                          lower_bound_ctx_id,
                          batch_count))) {
          LOG_WARN("failed to assign sql", K(ret));
        } else if (OB_FAIL(sql_proxy.read(res, exec_tenant_id, sql.ptr()))) {
          LOG_WARN("execute sql failed", K(ret), K(sql));
        } else if (OB_ISNULL(result = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("result is null", K(ret));
        } else {
          while (OB_SUCC(result->next())) {
            uint64_t id;
            EXTRACT_INT_FIELD_MYSQL(*result, "context_id", id, uint64_t);
            if (OB_FAIL(ret)) {
            } else if (OB_FAIL(ctx_ids.push_back(id))) {
              LOG_WARN("failed to push back context id", K(ret));
            } else {
              lower_bound_ctx_id = id;
            }
          }
          if (OB_ITER_END != ret) {
            LOG_WARN("failed to get next row", K(ret));
          } else {
            ret = OB_SUCCESS;
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (ctx_ids.empty()) {
        // read end
        clean_finish = true;
      } else {
        ObSchemaGetterGuard tenant_schema_guard;
        ObArray<uint64_t> delete_ctx_ids;
        if (OB_FAIL(schema_service.get_tenant_schema_guard(
                curr_tenant_id, tenant_schema_guard))) {
          LOG_WARN("get_schema_guard with version in inner table failed", K(ret), K(curr_tenant_id));
        }
        for (int64_t j = 0; OB_SUCC(ret) && j < ctx_ids.count(); ++j) {
          const ObContextSchema *ctx_schema = nullptr;
          bool exist = false;
          if (OB_FAIL(tenant_schema_guard.check_context_exist_by_id(curr_tenant_id, ctx_ids.at(j), ctx_schema, exist))) {
            LOG_WARN("failed to check schema exist", K(ret));
          } else if (OB_UNLIKELY(!exist)) {
            delete_ctx_ids.push_back(ctx_ids.at(j));
          }
        }
        if (OB_FAIL(ret) || delete_ctx_ids.empty()) {
        } else if (OB_FAIL(delete_global_contexts_by_ids(curr_tenant_id, delete_ctx_ids, sql_proxy))) {
          LOG_WARN("failed to delete global contexts", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObGlobalContextOperator::delete_global_context(const uint64_t tenant_id,
                                                   const uint64_t context_id,
                                                   const ObString &attribute,
                                                   const ObString &client_id,
                                                   ObISQLClient &sql_proxy)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  int64_t affected_rows = 0;
  const uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id);
  ObDMLExecHelper exec(sql_proxy, exec_tenant_id);
  if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == context_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invaid context info", K(tenant_id), K(context_id), K(ret));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", tenant_id))) {
    LOG_WARN("add column failed", K(ret));
  } else if (OB_FAIL(dml.add_pk_column("context_id", context_id))
      || (!attribute.empty() && OB_FAIL(dml.add_pk_column("attribute", ObHexEscapeSqlStr(attribute))))
      || (OB_FAIL(dml.add_pk_column("client_identifier", ObHexEscapeSqlStr(client_id))))) {
    LOG_WARN("add column failed", K(ret));
  } else if (OB_FAIL(exec.exec_delete(OB_ALL_GLOBAL_CONTEXT_VALUE_TNAME,
                                      dml, affected_rows))) {
    LOG_WARN("execute delete failed", K(tenant_id), K(ret));
  }
  return ret;
}
int ObGlobalContextOperator::delete_global_contexts_by_id(const uint64_t tenant_id,
                                                          const uint64_t context_id,
                                                          ObISQLClient &sql_proxy)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  int64_t affected_rows = 0;
  const uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id);
  ObDMLExecHelper exec(sql_proxy, exec_tenant_id);
  if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == context_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invaid context info", K(tenant_id), K(context_id), K(ret));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", tenant_id))) {
    LOG_WARN("add column failed", K(ret));
  } else if (OB_FAIL(dml.add_pk_column("context_id", context_id))) {
    LOG_WARN("add column failed", K(ret));
  } else if (OB_FAIL(exec.exec_delete(OB_ALL_GLOBAL_CONTEXT_VALUE_TNAME,
                                      dml, affected_rows))) {
    LOG_WARN("execute delete failed", K(tenant_id), K(ret));
  }
  return ret;
}
int ObGlobalContextOperator::delete_global_contexts_by_ids(const uint64_t tenant_id,
                                                           const common::ObIArray<uint64_t> &context_ids,
                                                           ObISQLClient &sql_proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id);
  if (context_ids.empty()) {
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invaid context info", K(tenant_id), K(ret));
  } else if (OB_FAIL(sql.assign_fmt(
            "DELETE FROM %s WHERE tenant_id = %lu and context_id in (",
            OB_ALL_GLOBAL_CONTEXT_VALUE_TNAME,
            tenant_id))) {
    LOG_WARN("assign sql fmt failed",
          K(tenant_id), K(ret));
  } else {
    int64_t affected_rows = 0;
    int64_t j = 0;
    for (; OB_SUCC(ret) && j < context_ids.count() - 1; ++j) {
      if (OB_INVALID_ID == context_ids.at(j)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invaid context info", K(context_ids.at(j)), K(ret));
      } else if (OB_FAIL(sql.append_fmt("%lu, ", context_ids.at(j)))) {
        LOG_WARN("failed to append fmt", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(sql.append_fmt("%lu)", context_ids.at(j)))) {
      LOG_WARN("failed to append fmt", K(ret));
    } else if (OB_FAIL(sql_proxy.write(exec_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("execute sql fail", K(sql), K(ret));
    } 
  }
  return ret; 
}

int ObGlobalContextOperator::insert_update_context(const uint64_t tenant_id,
                                                   const uint64_t context_id,
                                                   const ObString &context_name,
                                                   const ObString &attribute,
                                                   const ObString &client_id,
                                                   const ObString &username,
                                                   const ObString &value,
                                                   ObISQLClient &sql_proxy)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  int64_t affected_rows = 0;
  const uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id);
  ObDMLExecHelper exec(sql_proxy, exec_tenant_id);
  if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == context_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invaid context info", K(tenant_id), K(context_id), K(ret));
  } else if (OB_FAIL(dml.add_column("namespace", ObHexEscapeSqlStr(context_name)))
      || OB_FAIL(dml.add_column("attribute", ObHexEscapeSqlStr(attribute)))
      || OB_FAIL(dml.add_column("client_identifier", ObHexEscapeSqlStr(client_id)))
      || OB_FAIL(dml.add_column("tenant_id", tenant_id))
      || OB_FAIL(dml.add_column("context_id", context_id))
      || OB_FAIL(dml.add_column("value", ObHexEscapeSqlStr(value)))
      || OB_FAIL(dml.add_column("username", ObHexEscapeSqlStr(username)))) {
    LOG_WARN("add column failed", K(ret));
  } else if (OB_FAIL(exec.exec_insert_update(OB_ALL_GLOBAL_CONTEXT_VALUE_TNAME,
                                              dml, affected_rows))) {
    LOG_WARN("execute insert update failed", K(tenant_id), K(ret));
  } else if (affected_rows > 2) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected affected rows", K(tenant_id),
                                      K(affected_rows), K(ret));
  }
  return ret;
}

int ObGlobalContextOperator::read_global_context(const uint64_t tenant_id,
                                                 const uint64_t context_id,
                                                 const ObString &attribute,
                                                 const ObString &client_id,
                                                 const ObString &username, 
                                                 ObString &value,
                                                 bool &exist,
                                                 ObISQLClient &sql_proxy,
                                                 ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  ObSqlString attr;
  ObSqlString cid;
  ObSqlString sql;
  exist = false;
  const uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id);
  SMART_VAR(common::ObMySQLProxy::MySQLResult, res) {
    common::sqlclient::ObMySQLResult *result = NULL;
    if (OB_FAIL(sql_append_hex_escape_str(attribute, attr))) {
      LOG_WARN("failed to process attribute", K(ret));
    } else if (OB_FAIL(sql_append_hex_escape_str(client_id, cid))) {
      LOG_WARN("failed to process client id", K(ret));
    } else if (OB_FAIL(sql.assign_fmt("SELECT username, value FROM %s WHERE tenant_id = %lu AND context_id = %lu AND attribute = %.*s AND client_identifier = %.*s",
                                      OB_ALL_GLOBAL_CONTEXT_VALUE_TNAME,
                                      tenant_id,
                                      context_id,
                                      attr.string().length(), attr.string().ptr(),
                                      cid.string().length(), cid.string().ptr()))) {
      LOG_WARN("failed to assign sql", K(ret));
    } else if (OB_FAIL(sql_proxy.read(res, exec_tenant_id, sql.ptr()))) {
      LOG_WARN("execute sql failed", K(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result is null", K(ret));
    } else {
      if (OB_SUCC(result->next())) {
        ObObj record_username;
        if (OB_FAIL(result->get_obj(0l, record_username))) {
          LOG_WARN("failed to get username", K(ret));
        } else if (record_username.is_null() || 0 == record_username.val_len_
                   || 0 == username.case_compare(record_username.get_varchar())) {
          if (OB_FAIL(result->get_varchar(1l, value))) {
            LOG_WARN("failed to get value", K(ret));
          } else if (OB_FAIL(deep_copy_ob_string(alloc, value, value))) {
            LOG_WARN("failed to deeo copy obj", K(ret));
          } else {
            exist = true;
          }
        } else {
          // do nothing
        }
      } else if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("read global value failed", K(ret));
      }
    }
  }
  return ret;
}
