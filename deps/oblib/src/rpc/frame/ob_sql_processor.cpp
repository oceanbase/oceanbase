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

#define USING_LOG_PREFIX RPC_FRAME

#include "rpc/frame/ob_sql_processor.h"

#include "lib/time/ob_time_utility.h"
#include "rpc/ob_request.h"
#include "lib/oblog/ob_trace_log.h"
#include "lib/rc/context.h"
#include "rpc/obmysql/ob_mysql_request_utils.h"
using namespace oceanbase::common;
using namespace oceanbase::rpc::frame;

int ObSqlProcessor::run()
{
  int ret = OB_SUCCESS;
  bool deseri_succ = true;

  run_timestamp_ = ObTimeUtility::current_time();
  if (OB_FAIL(setup_packet_sender())) {
    deseri_succ = false;
    LOG_WARN("setup packet sender fail", K(ret));
  } else if (OB_FAIL(deserialize())) {
    deseri_succ = false;
    LOG_WARN("deserialize argument fail", K(ret));
  } else if (OB_FAIL(before_process())) {
    LOG_WARN("before process fail", K(ret));
  } else {
    req_->set_trace_point(ObRequest::OB_EASY_REQUEST_SQL_PROCESSOR_RUN);
    if (OB_FAIL(process())) {
      LOG_DEBUG("process fail", K(ret));
    } else {
    }
  }
  int tmp_ret = OB_SUCCESS;
  int tmp_ret_2 = OB_SUCCESS;
  if (OB_TMP_FAIL(response(ret))) {
    ret = (OB_SUCCESS != ret) ? ret : tmp_ret;
    LOG_WARN("response rpc result fail", K(ret), K(tmp_ret));
  }

  if (deseri_succ && OB_UNLIKELY(OB_SUCCESS != (tmp_ret_2 = after_process(ret)))) {
    ret = (OB_SUCCESS != ret) ? ret : tmp_ret_2;
    LOG_WARN("after process fail", K(ret), K(tmp_ret_2));
  }

  cleanup();

  return ret;
}

ObAddr ObSqlProcessor::get_peer() const
{
  return SQL_REQ_OP.get_peer(req_);
}

