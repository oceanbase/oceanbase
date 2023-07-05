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

#include "rpc/ob_request.h"
using namespace oceanbase::common;

namespace oceanbase
{
namespace rpc
{
void __attribute__((weak)) response_rpc_error_packet(ObRequest* req, int ret)
{
  UNUSED(ret);
  RPC_REQ_OP.response_result(req, NULL);
}

void on_translate_fail(ObRequest* req, int ret)
{
  ObRequest::Type req_type = req->get_type();
  if (ObRequest::OB_RPC == req_type) {
    response_rpc_error_packet(req, ret);
  } else if (ObRequest::OB_MYSQL == req_type) {
    SQL_REQ_OP.disconnect_sql_conn(req);
    SQL_REQ_OP.finish_sql_request(req);
  }
}

int ObRequest::set_trace_point(int trace_point)
{
  if (ez_req_ != NULL) {
    if (trace_point != 0) {
      ez_req_->trace_point = trace_point;
    } else {
      snprintf(ez_req_->trace_bt, EASY_REQ_TRACE_BT_SIZE, "%s", lbt());
    }
  } else {
    handling_state_ = trace_point;
  }
  return OB_SUCCESS;
}

} //end of namespace rpc
} //end of namespace oceanbase
