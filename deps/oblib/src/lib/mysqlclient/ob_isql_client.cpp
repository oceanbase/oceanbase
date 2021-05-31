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

#include "lib/mysqlclient/ob_isql_client.h"
#include "lib/allocator/ob_mod_define.h"
#include "lib/mysqlclient/ob_isql_result_handler.h"
#include "lib/mysqlclient/ob_isql_connection_pool.h"
#include "lib/mysqlclient/ob_mysql_result.h"

namespace oceanbase {
namespace common {

using namespace sqlclient;

int ObISQLClient::escape(const char* from, const int64_t from_size, char* to, const int64_t to_size)
{
  int64_t out_size = 0;
  return escape(from, from_size, to, to_size, out_size);
}

ObMySQLResult* ObISQLClient::ReadResult::mysql_result()
{
  ObMySQLResult* res = NULL;
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

ObISQLClient::ReadResult::ReadResult() : result_handler_(NULL)
{}

ObISQLClient::ReadResult::~ReadResult()
{
  reset();
}

void ObISQLClient::ReadResult::reset()
{
  if (NULL != result_handler_) {
    result_handler_->~ObISQLResultHandler();
    result_handler_ = NULL;
  }
}

void ObISQLClient::ReadResult::reuse()
{
  if (NULL != result_handler_) {
    result_handler_->~ObISQLResultHandler();
    result_handler_ = NULL;
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

// Cross-cluster reading is not supported by default
int ObISQLClient::read(ReadResult& res, const int64_t cluster_id, const uint64_t tenant_id, const char* sql)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == cluster_id) {
    ret = this->read(res, tenant_id, sql);
  } else {
    ret = OB_NOT_SUPPORTED;
    COMMON_LOG(WARN, "read with cluster_id not supported", K(ret), K(cluster_id), K(tenant_id));
  }
  return ret;
}

}  // end namespace common
}  // end namespace oceanbase
