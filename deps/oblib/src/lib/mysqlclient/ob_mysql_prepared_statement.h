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

#ifndef __OB_COMMON_SQLCLIENT_OB_MYSQL_PREPARED_STATEMENT__
#define __OB_COMMON_SQLCLIENT_OB_MYSQL_PREPARED_STATEMENT__

#include <mariadb/mysql.h>
#include "lib/string/ob_string.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/mysqlclient/ob_mysql_connection.h"
#include "lib/mysqlclient/ob_mysql_prepared_param.h"
#include "lib/mysqlclient/ob_mysql_prepared_result.h"

namespace oceanbase {
namespace common {
namespace sqlclient {
class ObMySQLPreparedStatement {
public:
  ObMySQLPreparedStatement();
  ~ObMySQLPreparedStatement();
  ObIAllocator& get_allocator();
  ObMySQLConnection* get_connection();
  MYSQL_STMT* get_stmt_handler();
  MYSQL* get_conn_handler();
  int init(ObMySQLConnection& conn, const char* sql);
  /*
   * close statement
   */
  int close();

  int bind_param_int(const int64_t col_idx, int64_t* out_buf);
  int bind_param_varchar(const int64_t col_idx, char* out_buf, unsigned long& res_len);

  int bind_result_int(const int64_t col_idx, int64_t* out_buf);
  int bind_result_varchar(const int64_t col_idx, char* out_buf, const int buf_len, unsigned long& res_len);
  /*
   * bind input param to sql
   * must called before execute_update, execute_query
   */
  int set_int(const int64_t col_idx, const int64_t int_val);
  int set_varchar(const int64_t col_idx, const ObString& varchar);
  // TODO: more types

  /*
   * execute a SQL command, such as
   *  - set @@session.ob_query_timeout=10
   *  - commit
   *  - insert into t values (v1,v2),(v3,v4)
   */
  int execute_update();

  /*
   * ! Deprecated
   * use prepare method to read data instead
   * reference ObMySQLPrepareStatement
   */
  ObMySQLPreparedResult* execute_query();

private:
  ObMySQLConnection* conn_;
  ObArenaAllocator arena_allocator_;  // TODO: used right allocator?
  ObIAllocator* alloc_;               // bind to arena_allocator_
  ObMySQLPreparedParam param_;
  ObMySQLPreparedResult result_;
  int64_t stmt_param_count_;
  MYSQL_STMT* stmt_;
};
}  // namespace sqlclient
}  // namespace common
}  // namespace oceanbase
#endif
