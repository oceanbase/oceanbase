// Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#include "ob_xa_inner_sql_client.h"
#include "storage/tx/ob_xa_service.h"

namespace oceanbase
{
using namespace common;
using namespace common::sqlclient;
namespace transaction
{
ObXAInnerSQLClient::~ObXAInnerSQLClient()
{
  reset();
  close();
}

void ObXAInnerSQLClient::reset()
{
  start_ts_ = 0;
  has_started_ = false;
}

int ObXAInnerSQLClient::start(ObISQLClient *sql_client)
{
  int ret = OB_SUCCESS;
  const int32_t group_id = 0;
  if (OB_UNLIKELY(NULL == sql_client)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid arguments", K(ret), KP(sql_client));
  } else if (has_started_) {
    // return success
    ret = OB_SUCCESS;
  } else {
    const uint64_t exec_tenant_id = gen_meta_tenant_id(MTL_ID());
    if (OB_FAIL(connect(exec_tenant_id, group_id, sql_client))) {
      TRANS_LOG(WARN, "failed to init", K(ret));
    } else {
      start_ts_ = ObTimeUtility::current_time();
      has_started_ = true;
    }
  }
  return ret;
}

int ObXAInnerSQLClient::end()
{
  int ret = OB_SUCCESS;
  start_ts_ = 0;
  has_started_ = false;
  close();
  return ret;
}

#define QUERY_MYSQLXA_SQL "\
   SELECT coordinator, trans_id, is_readonly FROM %s WHERE \
   tenant_id = %lu AND gtrid = x'%.*s' AND bqual = x'%.*s' AND format_id = %ld"

int ObXAInnerSQLClient::query_xa_coord_for_mysql(const ObXATransID &xid,
                                                 share::ObLSID &coord_id,
                                                 ObTransID &tx_id,
                                                 bool &is_readonly)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    ObMySQLResult *result = NULL;
    char gtrid_str[128] = {0};
    int64_t gtrid_len = 0;
    char bqual_str[128] = {0};
    int64_t bqual_len = 0;
    const uint64_t tenant_id = MTL_ID();
    const uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id);

    const int64_t start_ts = ObTimeUtility::current_time();
    ObXAInnerSqlStatGuard stat_guard(start_ts);

    if (!has_started_ || NULL == get_connection()) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "unexpected xa inner sql client", K(ret), K(xid));
    } else if (xid.empty()) {
      ret = OB_INVALID_ARGUMENT;
      TRANS_LOG(WARN, "invalid argument", K(ret), K(xid));
    } else if (OB_FAIL(hex_print(xid.get_gtrid_str().ptr(),
                                 xid.get_gtrid_str().length(),
                                 gtrid_str,
                                 128,
                                 gtrid_len))) {
      TRANS_LOG(WARN, "fail to convert gtrid to hex", K(ret), K(xid));
    } else if (OB_FAIL(hex_print(xid.get_bqual_str().ptr(),
                                 xid.get_bqual_str().length(),
                                 bqual_str,
                                 128,
                                 bqual_len))) {
      TRANS_LOG(WARN, "fail to convert bqual to hex", K(ret), K(xid));
    } else if (OB_FAIL(sql.assign_fmt(QUERY_MYSQLXA_SQL,
                                      share::OB_ALL_TENANT_GLOBAL_TRANSACTION_TNAME,
                                      tenant_id,
                                      (int)gtrid_len, gtrid_str,
                                      (int)bqual_len, bqual_str,
                                      xid.get_format_id()))) {
      TRANS_LOG(WARN, "generate query coordinator sql fail", K(ret));
    } else if (OB_FAIL(read(res, exec_tenant_id, sql.ptr()))) {
      TRANS_LOG(WARN, "execute sql read fail", KR(ret), K(exec_tenant_id), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "execute sql fail", K(ret), K(sql));
    } else if (OB_FAIL(result->next())) {
      if (OB_ITER_END != ret) {
        TRANS_LOG(WARN, "iterate next result fail", K(ret), K(sql));
      }
    } else {
      int64_t tx_id_value = 0;
      EXTRACT_INT_FIELD_MYSQL(*result, "trans_id", tx_id_value, int64_t);
      EXTRACT_BOOL_FIELD_MYSQL(*result, "is_readonly", is_readonly);
      tx_id = ObTransID(tx_id_value);
      if (OB_FAIL(ret)) {
        TRANS_LOG(WARN, "fail to extract field from result", K(ret));
      } else {
        int64_t ls_id_value = 0;
        EXTRACT_INT_FIELD_MYSQL(*result, "coordinator", ls_id_value, int64_t);
        if (OB_FAIL(ret)) {
          if (OB_ERR_NULL_VALUE == ret) {
            // rewrite code, and caller would check validity of coordinator
            ret = OB_SUCCESS;
          } else {
            TRANS_LOG(WARN, "fail to extract coordinator from result", K(ret), K(xid));
          }
        } else {
          coord_id = share::ObLSID(ls_id_value);
        }
      }
    }
  }
  return ret;
}


#define DELETE_XA_TRANS_SQL "delete from %s where \
  tenant_id = %lu and gtrid = x'%.*s' and bqual = x'%.*s' and format_id = %ld"

int ObXAInnerSQLClient::delete_xa_branch_for_mysql(const ObXATransID &xid)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  char gtrid_str[128] = {0};
  int64_t gtrid_len = 0;
  char bqual_str[128] = {0};
  int64_t bqual_len = 0;
  const uint64_t tenant_id = MTL_ID();
  const uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id);
  const int64_t start_ts = ObTimeUtility::current_time();
  ObXAInnerSqlStatGuard stat_guard(start_ts);

  if (!has_started_ || NULL == get_connection()) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "unexpected xa inner sql client", K(ret), K(xid));
  } else if (xid.empty()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(xid));
  } else if (OB_FAIL(hex_print(xid.get_gtrid_str().ptr(),
                               xid.get_gtrid_str().length(),
                               gtrid_str,
                               128,
                               gtrid_len))) {
    TRANS_LOG(WARN, "fail to convert gtrid to hex", K(ret), K(xid));
  } else if (OB_FAIL(hex_print(xid.get_bqual_str().ptr(),
                               xid.get_bqual_str().length(),
                               bqual_str,
                               128,
                               bqual_len))) {
    TRANS_LOG(WARN, "fail to convert bqual to hex", K(ret), K(xid));
  } else {
    if (OB_FAIL(sql.assign_fmt(DELETE_XA_TRANS_SQL,
                               share::OB_ALL_TENANT_GLOBAL_TRANSACTION_TNAME,
                               tenant_id,
                               (int)gtrid_len, gtrid_str,
                               (int)bqual_len, bqual_str,
                               xid.get_format_id()))) {
      TRANS_LOG(WARN, "generate delete xa trans sql fail", K(ret), K(sql));
    } else if (OB_FAIL(write(exec_tenant_id, sql.ptr(), affected_rows))) {
      TRANS_LOG(WARN, "execute delete xa trans sql fail",
                KR(ret), K(exec_tenant_id), K(sql), K(affected_rows));
    } else if (OB_UNLIKELY(1 < affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "delete multiple rows", K(xid), K(affected_rows), K(sql));
    } else if (OB_UNLIKELY(0 == affected_rows)) {
      TRANS_LOG(WARN, "delete no rows", K(xid), K(sql));
      // return OB_SUCCESS
    } else {
      // do nothing
    }
  }
  return ret;
}

} // end namespace transaction
} // end namespace oceanbase
