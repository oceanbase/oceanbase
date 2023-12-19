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

#ifndef _OB_SINGLE_CONNECTION_PROXY_H
#define _OB_SINGLE_CONNECTION_PROXY_H 1
#include "lib/mysqlclient/ob_isql_client.h"
#include "lib/mysqlclient/ob_mysql_connection.h"

namespace oceanbase
{
namespace common
{
namespace sqlclient
{
class ObISQLConnection;
class ObISQLConnectionPool;
} // end namespace sqlclient


// use one connection to execute multiple statements
// @note not thread safe
class ObSingleConnectionProxy : public ObISQLClient
{
public:
  ObSingleConnectionProxy();
  virtual ~ObSingleConnectionProxy();
public:
  virtual int escape(const char *from, const int64_t from_size,
                     char *to, const int64_t to_size, int64_t &out_size) override;
  // %res should be destructed before execute other sql
  virtual int read(ReadResult &res, const uint64_t tenant_id, const char *sql) override { return this->read(res, tenant_id, sql, 0/*group_id*/); }
  virtual int read(ReadResult &res, const uint64_t tenant_id, const char *sql, const int32_t group_id) override;
  virtual int read(ReadResult &res, const int64_t cluster_id, const uint64_t tenant_id, const char *sql) override;
  virtual int write(const uint64_t tenant_id, const char *sql, int64_t &affected_rows) override { return this->write(tenant_id, sql, 0/*group_id*/, affected_rows); }
  virtual int write(const uint64_t tenant_id, const char *sql, const int32_t group_id, int64_t &affected_rows) override;
  using ObISQLClient::read;
  using ObISQLClient::write;

  int connect(const uint64_t tenant_id, const int32_t group_id, ObISQLClient *sql_client);
  virtual sqlclient::ObISQLConnectionPool *get_pool() override { return pool_; }
  virtual sqlclient::ObISQLConnection *get_connection() override { return conn_; }

  virtual bool is_oracle_mode() const override { return oracle_mode_; }
  // in some situation, it allows continuation of SQL execution after failure in transaction,
  // and last_error should be reset.
  //
  void reset_last_error() { errno_ = common::OB_SUCCESS; }

protected:
  void close();
  void set_errno(int err) { errno_ = err; }
  int get_errno() const { return errno_; }
public:
  bool check_inner_stat() const;
protected:
  int errno_;
  int64_t statement_count_;
  sqlclient::ObISQLConnection *conn_;
  sqlclient::ObISQLConnectionPool *pool_;
  ObISQLClient *sql_client_;
  bool oracle_mode_;
  DISALLOW_COPY_AND_ASSIGN(ObSingleConnectionProxy);
};

inline bool ObSingleConnectionProxy::check_inner_stat() const
{
  bool bret = (OB_SUCCESS == errno_ && NULL != pool_ && NULL != conn_);
  if (!bret) {
    COMMON_MYSQLP_LOG_RET(WARN, errno_, "invalid inner stat", "errno", errno_, K_(pool), K_(conn));
  }
  return bret;
}


} // end namespace common
} // end namespace oceanbase

#endif /* _OB_SINGLE_CONNECTION_PROXY_H */
