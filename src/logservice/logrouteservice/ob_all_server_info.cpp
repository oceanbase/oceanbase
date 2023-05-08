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

#define USING_LOG_PREFIX OBLOG
#include "ob_all_server_info.h"
#include "logservice/common_util/ob_log_time_utils.h"

namespace oceanbase
{
namespace logservice
{
int AllServerRecord::init(const uint64_t svr_id,
    const common::ObAddr &server,
    StatusType &status,
    ObString &zone)
{
  int ret = OB_SUCCESS;
  svr_id_ = svr_id;
  server_ = server;
  status_ = status;

  if (OB_FAIL(zone_.assign(zone))) {
    LOG_ERROR("zone assign fail", KR(ret), K(zone));
  }

  return ret;
}

void ObAllServerInfo::reset()
{
  cluster_id_ = 0;
  all_srv_array_.reset();
}

int ObAllServerInfo::init(const int64_t cluster_id)
{
  int ret = OB_SUCCESS;

  cluster_id_ = cluster_id;
  all_srv_array_.reset();

  return ret;
}

int ObAllServerInfo::add(AllServerRecord &record)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(all_srv_array_.push_back(record))) {
    LOG_ERROR("all_srv_array_ push_back failed", KR(ret), K(record));
  }

  return ret;
}

} // namespace logservice
} // namespace oceanbase

