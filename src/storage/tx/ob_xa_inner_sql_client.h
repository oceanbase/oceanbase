// Copyright (c) 2021 OceanBase
// SPDX-License-Identifier: Apache-2.0

#ifndef OCEANBASE_XA_INNER_SQL_CLIENT_H_
#define OCEANBASE_XA_INNER_SQL_CLIENT_H_

#include "lib/string/ob_sql_string.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/mysqlclient/ob_isql_client.h"
#include "lib/mysqlclient/ob_mysql_connection.h"
#include "lib/mysqlclient/ob_single_connection_proxy.h"
#include "storage/tx/ob_trans_define.h"

namespace oceanbase
{
namespace common
{
namespace sqlclient
{
class ObISQLConnection;
class ObISQLConnectionPool;
}
}
namespace transaction
{
// the object of this class is not thread-safe
class ObXAInnerSQLClient: public ObSingleConnectionProxy
{
public:
  ObXAInnerSQLClient() : start_ts_(0), has_started_(false) {};
  ~ObXAInnerSQLClient();
  void reset();
public:
  int start(ObISQLClient *sql_client);
  int end();
  bool has_started() const { return has_started_; }
  int query_xa_coord_for_mysql(const ObXATransID &xid,
                               share::ObLSID &coord_id,
                               ObTransID &tx_id,
                               bool &is_readonly);
  int delete_xa_branch_for_mysql(const ObXATransID &xid);
private:
  int64_t start_ts_;
  bool has_started_;
};

} // end namespace transaction
} // end namespace oceanbase

#endif // OCEANBASE_XA_INNER_SQL_CLIENT_H_
