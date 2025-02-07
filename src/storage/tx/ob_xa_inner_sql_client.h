// Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.

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
