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

#include "lib/mysqlclient/ob_isql_connection.h"
#include "lib/mysqlclient/ob_isql_connection_pool.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"

namespace oceanbase {
namespace common {
using namespace sqlclient;

ObMySQLTransaction::ObMySQLTransaction() : start_time_(0), in_trans_(false)
{}

ObMySQLTransaction::~ObMySQLTransaction()
{
  int ret = OB_SUCCESS;
  if (in_trans_) {
    if (OB_FAIL(end(OB_SUCCESS == get_errno()))) {
      // Although the ret cannot be transmitted here, it still plays WARN, because sometimes it is normal to fail here.
      // If it is not normal, the transaction module will log ERROR, here it is better to just log WARN
      LOG_WARN("fail to end", K(ret));
    }
  }
}

int ObMySQLTransaction::start_transaction(bool with_snapshot)
{
  int ret = OB_SUCCESS;
  if (NULL == get_connection()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("conn_ is NULL");
  } else if (OB_FAIL(get_connection()->start_transaction(with_snapshot))) {
    LOG_WARN("fail to start transaction", K(ret), K(with_snapshot));
  }
  return ret;
}

int ObMySQLTransaction::start(ObISQLClient* sql_client, bool with_snapshot /* = false*/)
{
  int ret = OB_SUCCESS;
  start_time_ = ::oceanbase::common::ObTimeUtility::current_time();
  if (OB_FAIL(connect(sql_client))) {
    LOG_WARN("failed to init", K(ret));
  } else {
    if (OB_FAIL(start_transaction(with_snapshot))) {
      set_errno(ret);
      close();
      LOG_WARN("failed to start transaction", K(ret), K(with_snapshot));
    } else {
      in_trans_ = true;
      LOG_DEBUG("start transaction success", K(with_snapshot));
    }
  }
  return ret;
}

int ObMySQLTransaction::end_transaction(const bool commit)
{
  int ret = OB_SUCCESS;
  if (NULL != get_connection()) {
    if (commit) {
      ret = get_connection()->commit();
    } else {
      ret = get_connection()->rollback();
    }
    if (OB_SUCCESS == get_errno()) {
      set_errno(ret);
    }
  }
  return ret;
}

int ObMySQLTransaction::end(const bool commit)
{
  int ret = OB_SUCCESS;
  if (in_trans_) {
    ret = end_transaction(commit);
    if (OB_FAIL(ret)) {
      LOG_WARN("fail to end transation", K(ret));
    } else {
      LOG_DEBUG("end transaction success", K(commit));
    }
    in_trans_ = false;
  }
  close();
  return ret;
}

}  // namespace common
}  // end namespace oceanbase
