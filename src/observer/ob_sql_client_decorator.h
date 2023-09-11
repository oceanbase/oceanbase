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

#ifndef _OB_SQL_CLIENT_DECORATOR_H
#define _OB_SQL_CLIENT_DECORATOR_H 1
#include "lib/mysqlclient/ob_isql_client.h"
#include "lib/mysqlclient/ob_single_connection_proxy.h"
namespace oceanbase
{
namespace common
{
// read will retry `retry_limit' times when failed
class ObSQLClientRetry: public ObISQLClient
{
public:
  ObSQLClientRetry(ObISQLClient *sql_client, int32_t retry_limit)
      :sql_client_(sql_client),
       retry_limit_(retry_limit)
  {}
  virtual ~ObSQLClientRetry() {}

  virtual int escape(const char *from, const int64_t from_size,
      char *to, const int64_t to_size, int64_t &out_size) override;
  virtual int read(ReadResult &res, const uint64_t tenant_id, const char *sql) override { return this->read(res, tenant_id, sql, 0 /*group_id*/); }
  virtual int read(ReadResult &res, const uint64_t tenant_id, const char *sql, const int32_t group_id) override;
  virtual int read(ReadResult &res, const int64_t cluster_id, const uint64_t tenant_id, const char *sql) override;
  virtual int write(const uint64_t tenant_id, const char *sql, int64_t &affected_rows) override { return this->write(tenant_id, sql, 0/*group_id*/, affected_rows); }
  virtual int write(const uint64_t tenant_id, const char *sql, const int32_t group_id, int64_t &affected_rows) override;

  virtual sqlclient::ObISQLConnectionPool *get_pool() override;
  virtual sqlclient::ObISQLConnection *get_connection() override;
  using ObISQLClient::read;
  using ObISQLClient::write;

  void set_retry_limit(int32_t retry_limit) { retry_limit_ = retry_limit; }
  int32_t get_retry_limit() const { return retry_limit_; }
  bool is_oracle_mode() const override
  {
    return NULL == sql_client_ ? false : sql_client_->is_oracle_mode();
  }
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObSQLClientRetry);
private:
  ObISQLClient *sql_client_;
  int32_t retry_limit_;
};

class ObMySQLProxy;
class ObSQLClientRetryWeak: public ObISQLClient
{
public:
  // only check_sys_variable is still useful
  ObSQLClientRetryWeak(ObISQLClient *sql_client,
                       bool did_use_retry = false,
                       int64_t snapshot_timestamp = OB_INVALID_TIMESTAMP,
                       bool check_sys_variable = true)
      :sql_client_(sql_client),
       snapshot_timestamp_(snapshot_timestamp),
       check_sys_variable_(check_sys_variable),
       tenant_id_(OB_INVALID_TENANT_ID),
       table_id_(OB_INVALID_ID)
  {
    UNUSED(did_use_retry);
  }
  // not useful, it just use sql_client directly
  ObSQLClientRetryWeak(ObISQLClient *sql_client,
                       bool did_use_retry,
                       const uint64_t tenant_id,
                       const uint64_t table_id)
      : sql_client_(sql_client),
        snapshot_timestamp_(OB_INVALID_TIMESTAMP),
        check_sys_variable_(true),
        tenant_id_(tenant_id),
        table_id_(table_id)
  {
    UNUSED(did_use_retry);
  }
  virtual ~ObSQLClientRetryWeak() {}

  virtual int escape(const char *from, const int64_t from_size,
      char *to, const int64_t to_size, int64_t &out_size) override;
  virtual int read(ReadResult &res, const uint64_t tenant_id, const char *sql) override { return this->read(res, tenant_id, sql, 0 /*group_id*/); }
  virtual int read(ReadResult &res, const uint64_t tenant_id, const char *sql, const int32_t group_id) override;
  virtual int read(ReadResult &res, const int64_t cluster_id, const uint64_t tenant_id, const char *sql) override;
  virtual int write(const uint64_t tenant_id, const char *sql, int64_t &affected_rows) override { return this->write(tenant_id, sql, 0/*group_id*/, affected_rows); }
  virtual int write(const uint64_t tenant_id, const char *sql, const int32_t group_id, int64_t &affected_rows) override;
  using ObISQLClient::read;
  using ObISQLClient::write;

  virtual sqlclient::ObISQLConnectionPool *get_pool() override;
  virtual sqlclient::ObISQLConnection *get_connection() override;

  bool is_oracle_mode() const override
  {
    return NULL == sql_client_ ? false : sql_client_->is_oracle_mode();
  }
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObSQLClientRetryWeak);
  // functions
  int read_without_check_sys_variable(
      sqlclient::ObISQLConnection *conn,
      ReadResult &res,
      const uint64_t tenant_id,
      const char *sql);
private:
  ObISQLClient *sql_client_;
  int64_t snapshot_timestamp_;  // deprecated
  bool check_sys_variable_;
  uint64_t tenant_id_;          // deprecated
  uint64_t table_id_;           // deprecated
};

} // end namespace common
} // end namespace oceanbase

#endif /* _OB_SQL_CLIENT_DECORATOR_H */
