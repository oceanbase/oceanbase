/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "lib/mysqlclient/ob_isql_client.h"
#include "lib/mysqlclient/ob_isql_result_handler.h"
#include "lib/mysqlclient/ob_mysql_result.h"

namespace oceanbase
{
namespace common
{

using namespace sqlclient;

int ObISQLClient::escape(const char *from, const int64_t from_size,
      char *to, const int64_t to_size)
{
  int64_t out_size = 0;
  return escape(from, from_size, to, to_size, out_size);
}

ObMySQLResult *ObISQLClient::ReadResult::mysql_result()
{
  ObMySQLResult *res = NULL;
  if (NULL != result_handler_) {
    res = result_handler_->mysql_result();
  }
  return res;
}

int ObISQLClient::ReadResult::close()
{
  int ret = OB_SUCCESS;
  if (get_result() != NULL) {
    ret = get_result()->close();
  }
  return ret;
}

ObISQLClient::ReadResult::ReadResult()
    : result_handler_(NULL),
      enable_use_result_(false)
{
}

ObISQLClient::ReadResult::~ReadResult()
{
  reset();
}

void ObISQLClient::ReadResult::reset()
{
  if (NULL != result_handler_) {
    result_handler_->~ObISQLResultHandler();
    result_handler_ = NULL;
    enable_use_result_ = false;
  }
}

void ObISQLClient::ReadResult::reuse()
{
  if (NULL != result_handler_) {
    result_handler_->~ObISQLResultHandler();
    result_handler_ = NULL;
    enable_use_result_ = false;
  }
}

void ObISQLClient::set_inactive()
{
  active_ = false;
  if (NULL != get_pool()) {
    int ret = get_pool()->on_client_inactive(this);
    if (OB_FAIL(ret)) {
      COMMON_LOG(WARN, "connection pool on client inactive failed", K(ret));
    }
  }
}

} // end namespace common
} // end namespace oceanbase
