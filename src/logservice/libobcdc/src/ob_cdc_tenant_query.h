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

#ifndef OCEANBASE_LIBOBCDC_TENANT_QUERYER_H_
#define OCEANBASE_LIBOBCDC_TENANT_QUERYER_H_

#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "ob_log_utils.h"
namespace oceanbase
{
using namespace share;
namespace libobcdc
{
// ResultType = T
template<typename T>
class ObCDCQueryResult
{
public:
  ObCDCQueryResult(T &result) : affect_rows_(0), data_(result) {}
  virtual ~ObCDCQueryResult() { affect_rows_ = 0; }
public:
  OB_INLINE void inc_affect_rows() { affect_rows_++; }
  OB_INLINE int64_t get_affect_rows() const { return affect_rows_; }
  OB_INLINE bool has_data() const { return affect_rows_ > 0; }
  OB_INLINE bool is_empty() const { return ! has_data(); }
  OB_INLINE T& get_data() const { return data_; }

TO_STRING_KV(K_(affect_rows));

private:
  int64_t affect_rows_;
  T &data_;
};

// ResultType = T
template<typename T>
class ObCDCTenantQuery
{
public:
  ObCDCTenantQuery(common::ObMySQLProxy &sql_proxy) : sql_proxy_(&sql_proxy) {}
  virtual ~ObCDCTenantQuery() { sql_proxy_ = nullptr; }
public:
  int query(const uint64_t tenant_id, ObCDCQueryResult<T> &query_result, const int64_t retry_timeout);
  int query(ObCDCQueryResult<T> &result, const int64_t retry_timeout)
  { return query(OB_SYS_TENANT_ID, result, retry_timeout); }
protected:
  virtual int build_sql_statement_(const uint64_t tenant_id, ObSqlString &sql) = 0;
  // convert query rusult from sql_result to result and deepcopy is required if necessory
  virtual int parse_row_(common::sqlclient::ObMySQLResult &sql_result, ObCDCQueryResult<T> &result) = 0;
private:

private:
  int build_sql_statement_(ObSqlString &sql) { return build_sql_statement_(OB_SYS_TENANT_ID, sql); }
  int parse_sql_result_(common::sqlclient::ObMySQLResult &sql_result, ObCDCQueryResult<T> &result);

private:
  static const int64_t DEFAULT_RETRY_TIMEOUT = 1000;

private:
  common::ObMySQLProxy *sql_proxy_;
};

template<typename T>
int ObCDCTenantQuery<T>::query(const uint64_t tenant_id, ObCDCQueryResult<T> &query_result, const int64_t retry_timeout)
{
  int ret = OB_SUCCESS;
  common::ObSqlString sql;

  if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    OBLOG_LOG(ERROR, "invalid sql proxy", KR(ret));
  } else if (OB_FAIL(build_sql_statement_(tenant_id, sql))) {
    OBLOG_LOG(ERROR, "build_sql_statement_ failed", KR(ret), K(tenant_id), K(sql));
  } else {
    bool query_done = false;
    int retry_cnt = 0;
    const int64_t retry_fail_sleep_time = 100 * _MSEC_;
    const int64_t retry_warn_interval = 5 * _SEC_;
    const int64_t start_time = get_timestamp();
    const int64_t end_time = start_time + retry_timeout;

    while (! query_done) {
      common::sqlclient::ObMySQLResult *sql_result = nullptr;

      SMART_VAR(ObISQLClient::ReadResult, result) {
        if (OB_FAIL(sql_proxy_->read(result, tenant_id, sql.ptr()))) {
          OBLOG_LOG(ERROR, "sql read failed from sql_proxy", KR(ret), K(tenant_id), K(sql));
        } else if (OB_ISNULL(sql_result = result.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          OBLOG_LOG(ERROR, "invalid sql_result", KR(ret), K(tenant_id), K(sql));
        } else {
          query_done = true;

          if (OB_FAIL(parse_sql_result_(*sql_result, query_result))) {
            OBLOG_LOG(ERROR, "parse_sql_result_ failed", KR(ret), K(tenant_id));
          }
        }
      }

      if (! query_done) {
        int64_t cur_time = get_timestamp();

        if (cur_time < end_time) {
          retry_cnt ++;
          if (TC_REACH_TIME_INTERVAL(retry_warn_interval)) {
            OBLOG_LOG(INFO, "tenant query retring", KR(ret),
                K(tenant_id), K(retry_cnt), "remain_retry_time", end_time - cur_time, K(sql));
          }
          ob_usleep(retry_fail_sleep_time);
        } else {
          // query_done but failed.
          query_done = true;
          OBLOG_LOG(WARN, "tenant query failed after retry", KR(ret), K(tenant_id), K(sql), K(retry_cnt));
        }
      }
    }
  }

  return ret;
}

template<typename T>
int ObCDCTenantQuery<T>::parse_sql_result_(common::sqlclient::ObMySQLResult &sql_result, ObCDCQueryResult<T> &result)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(result.has_data())) {
    ret = OB_STATE_NOT_MATCH;
    OBLOG_LOG(ERROR, "expect empty query_result before query begin", KR(ret), K(result));
  } else {
    while (OB_SUCC(ret)) {
      if (OB_FAIL(sql_result.next())) {
        if (OB_ITER_END != ret) {
          OBLOG_LOG(ERROR, "iterate sql result failed", KR(ret), K(result));
        }
      } else {
        result.inc_affect_rows();

        if (OB_FAIL(parse_row_(sql_result, result))) {
          OBLOG_LOG(ERROR, "parse_row_ failed", KR(ret), K(result));
        }
      }
    }
  }

  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }

  return ret;
}

} // namespace libobcdc
} // namespace oceanbase
#endif