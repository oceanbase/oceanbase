/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX COMMON_MYSQLP

#include "lib/ob_define.h"
#include "lib/mysqlclient/ob_mysql_proxy_util.h"
#include "lib/mysqlclient/ob_isql_connection_pool.h"

using namespace oceanbase::common;
using namespace oceanbase::common::sqlclient;

ObMySQLProxyUtil::ObMySQLProxyUtil() : pool_(NULL)
{
}

ObMySQLProxyUtil::~ObMySQLProxyUtil()
{
}

int ObMySQLProxyUtil::init(ObISQLConnectionPool *pool)
{
  int ret = OB_SUCCESS;
  if (NULL == pool) {
    _OB_LOG(WARN, "check init mysql conn pool failed");
    ret = OB_INVALID_ARGUMENT;
  } else {
    pool_ = pool;
  }
  return ret;
}

int ObMySQLProxyUtil::escape(const char *from, const uint64_t length, char *to, const uint64_t size)
{
  int ret = OB_SUCCESS;
  if ((NULL == from) || (NULL == to) || (0 == size)) {
    _OB_LOG(WARN, "check input param failed:from[%s], len[%lu], to[%p], size[%ld]",
              from, length, to, size);
    ret = OB_INVALID_ARGUMENT;
  } else {
    int64_t out_size = 0;
    if (OB_FAIL(pool_->escape(from, length, to, size, out_size))) {
      LOG_WARN("escapse serint failed", K(ret));
    }
  }
  return ret;
}

