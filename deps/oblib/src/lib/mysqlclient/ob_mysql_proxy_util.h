/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
