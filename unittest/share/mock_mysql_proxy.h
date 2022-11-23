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
