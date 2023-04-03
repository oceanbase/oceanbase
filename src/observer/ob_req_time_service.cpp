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

#include "ob_req_time_service.h"

namespace oceanbase
{
namespace observer
{
ObReqTimeInfo::ObReqTimeInfo()
  : start_time_(0), end_time_(0), reentrant_cnt_(0)
{
  int ret = common::OB_SUCCESS;
  if (OB_FAIL(ObGlobalReqTimeService::get_instance().add_req_time_info(this))) {
    SERVER_LOG(ERROR, "failed to add req time info to list", K(ret));
  }
}

ObReqTimeInfo::~ObReqTimeInfo()
{
  int ret = common::OB_SUCCESS;
  if (OB_FAIL(ObGlobalReqTimeService::get_instance().rm_req_time_info(this))) {
    SERVER_LOG(ERROR, "failed to remove req time info from list", K(ret));
  }
  if (0 != reentrant_cnt_) {
    SERVER_LOG(ERROR, "invalid reentrant cnt", K(reentrant_cnt_));
  }
  start_time_ = 0;
  end_time_ = 0;
  reentrant_cnt_ = 0;
}

void ObGlobalReqTimeService::check_req_timeinfo()
{
#if !defined(NDEBUG)
  observer::ObReqTimeInfo &req_timeinfo =  observer::ObReqTimeInfo::get_thread_local_instance();

  OB_ASSERT(req_timeinfo.reentrant_cnt_ > 0);
#endif
}
} // end namespace server
} // end namesapce oceanbase

