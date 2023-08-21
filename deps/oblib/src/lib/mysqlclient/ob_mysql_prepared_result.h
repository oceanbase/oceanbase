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

#ifndef __OB_COMMON_SQLCLIENT_OB_MYSQL_PREPARED_RESULT__
#define __OB_COMMON_SQLCLIENT_OB_MYSQL_PREPARED_RESULT__

#include <mysql.h>
#include "lib/string/ob_string.h"
#include "lib/mysqlclient/ob_mysql_connection.h"
#include "lib/mysqlclient/ob_mysql_result.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
namespace sqlclient
{
class ObBindParam;
class ObMySQLPreparedStatement;
class ObMySQLPreparedResult
{
public:
  explicit ObMySQLPreparedResult(ObMySQLPreparedStatement &stmt);
  ~ObMySQLPreparedResult();
  int init();
  int bind_result_param();
  /*
   * close result
   */
  void close();
  /*
   * move next
   */
  int next();
  /*
   * get result values
   */
  int get_int(const int64_t col_idx, int64_t &int_val) const;
  int get_varchar(const int64_t col_idx, common::ObString &varchar_val) const;
  int64_t get_result_column_count() const { return result_column_count_; }

  int bind_result(ObBindParam &param);
private:
  ObMySQLPreparedStatement &stmt_;
  common::ObIAllocator &alloc_;
  int64_t result_column_count_;
  MYSQL_BIND *bind_;
};
}
}
}

#endif

