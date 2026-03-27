/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_MOCK_MYSQL_PROXY_H_
#define OCEANBASE_SHARE_MOCK_MYSQL_PROXY_H_

#include <gmock/gmock.h>
#include "lib/mysqlclient/ob_mysql_proxy.h"

namespace oceanbase
{
namespace common
{

class MockMySQLProxy : public ObMySQLProxy
{
public:
  MOCK_METHOD2(read, int(ObMySQLProxy::ReadResult &, const char *));
  MOCK_METHOD3(read, int(ObMySQLProxy::ReadResult &, const uint64_t, const char *));

  MOCK_METHOD2(write, int(const char *, int64_t &));
  MOCK_METHOD3(write, int(const uint64_t, const char *, int64_t &));
};

} // end namespace common
} // end namespace oceanbase

#endif // OCEANBASE_SHARE_MOCK_MYSQL_PROXY_H_
