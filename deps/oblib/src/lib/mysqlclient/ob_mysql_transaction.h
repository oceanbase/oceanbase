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

#ifndef OCEANBASE_MYSQL_TRANSACTION_H_
#define OCEANBASE_MYSQL_TRANSACTION_H_

#include "lib/oblog/ob_log_module.h"
#include "lib/mysqlclient/ob_isql_client.h"
#include "lib/mysqlclient/ob_mysql_connection.h"
#include "lib/mysqlclient/ob_single_connection_proxy.h"

namespace oceanbase {
namespace common {
namespace sqlclient {
class ObISQLConnection;
class ObISQLConnectionPool;
}  // namespace sqlclient

// not thread safe sql transaction execution
// use one connection
class ObMySQLTransaction : public ObSingleConnectionProxy {
public:
  ObMySQLTransaction();
  virtual ~ObMySQLTransaction();

public:
  // start transaction
  virtual int start(ObISQLClient* proxy, bool with_snapshot = false);
  // end the transaction
  virtual int end(const bool commit);
  virtual bool is_started() const
  {
    return in_trans_;
  }

protected:
  int start_transaction(bool with_snap_shot);
  int end_transaction(const bool commit);

protected:
  int64_t start_time_;
  bool in_trans_;
};

}  // namespace common
}  // end namespace oceanbase

#endif  // OCEANBASE_MYSQL_TRANSACTION_H_
