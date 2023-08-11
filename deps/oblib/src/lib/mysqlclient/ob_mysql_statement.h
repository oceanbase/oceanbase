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

#ifndef __OB_COMMON_SQLCLIENT_OB_MYSQL_STATEMENT__
#define __OB_COMMON_SQLCLIENT_OB_MYSQL_STATEMENT__

#include "lib/mysqlclient/ob_mysql_result_impl.h"

namespace oceanbase
{
namespace common
{
namespace sqlclient
{
class ObMySQLConnection;

class ObMySQLStatement
{
public:
  ObMySQLStatement();
  ~ObMySQLStatement();
  ObMySQLConnection *get_connection();
  MYSQL *get_stmt_handler();
  MYSQL *get_conn_handler();
  int init(ObMySQLConnection &conn, const char *sql);

  /*
   * close statement
   */
  void close();

  /*
   * execute a SQL command, such as
   *  - set @@session.ob_query_timeout=10
   *  - commit
   *  - insert into t values (v1,v2),(v3,v4)
   */
  int execute_update(int64_t &affected_rows);
  /*
   * same as execute_update(affected_rows)
   * but ignore affected_rows
   */
  int execute_update();
  static bool is_need_disconnect_error(int ret);

  /*
   * ! Deprecated
   * use prepare method to read data instead
   * reference ObMySQLPrepareStatement
   */
  ObMySQLResult *execute_query(bool enable_use_result = false);

private:
  ObMySQLConnection *conn_;
  ObMySQLResultImpl result_;
  MYSQL *stmt_;
  const char *sql_str_;
};
} //namespace sqlclient
}
}
#endif
