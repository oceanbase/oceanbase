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

#ifndef OCEANBASE_ROOT_MYSQL_PROXY_UTIL_H_
#define OCEANBASE_ROOT_MYSQL_PROXY_UTIL_H_

namespace oceanbase
{
namespace common
{
namespace sqlclient
{
class ObISQLConnectionPool;
}
// mysql lib function utility class
class ObMySQLProxyUtil
{
public:
  ObMySQLProxyUtil();
  virtual ~ObMySQLProxyUtil();
public:
  // init the connection pool
  int init(sqlclient::ObISQLConnectionPool *pool);
  // escape the old string convert from to to
  int escape(const char *from, const uint64_t length, char *to, const uint64_t size);
private:
  sqlclient::ObISQLConnectionPool *pool_;
};
}
}

#endif // OCEANBASE_ROOT_MYSQL_PROXY_UTIL_H_
