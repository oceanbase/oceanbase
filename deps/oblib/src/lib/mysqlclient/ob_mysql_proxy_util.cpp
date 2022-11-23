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

#define USING_LOG_PREFIX COMMON_MYSQLP

#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/mysqlclient/ob_mysql_proxy_util.h"
#include "lib/mysqlclient/ob_isql_connection_pool.h"
#include "lib/oblog/ob_log.h"

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

