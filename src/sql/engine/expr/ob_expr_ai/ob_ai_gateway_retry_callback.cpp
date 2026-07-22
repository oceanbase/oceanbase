/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_ai/ob_ai_gateway_retry_callback.h"
#include "sql/engine/expr/ob_expr_ai/ob_ai_func_utils.h"
#include "share/ai_service/ob_ai_gateway_route_session.h"

namespace oceanbase
{
namespace common
{

int ObAiGatewayRetryCallbackBase::before_retry()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(client_)) {
    // set_client() must have run via set_before_retry_cb(); null means a coding bug.
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("gateway retry callback client_ is null", K(ret));
  } else {
    const int tmp_ret = session_.on_failure(client_->get_last_http_code());
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("failed to record gateway failure before retry", K(tmp_ret));
    }
    if (OB_FAIL(ObAIFuncUtils::gateway_next_config(allocator_, session_,
                    model_config_info_.get_op_type(), gw_endpoint_, ep_config_))) {
      LOG_WARN("failed to advance gateway endpoint for retry", K(ret));
      session_.cancel_pending();
    } else {
      client_->set_current_endpoint_name(gw_endpoint_.endpoint_name_);
      ep_headers_.reset();
      if (OB_FAIL(rebuild_headers())) {
        LOG_WARN("failed to rebuild headers for retry", K(ret));
        session_.cancel_pending();
      } else if (OB_FAIL(client_->prepare_retry(allocator_, ep_config_.get_url(), ep_headers_))) {
        LOG_WARN("failed to prepare retry", K(ret));
        session_.cancel_pending();
      }
    }
  }
  return ret;
}

} // namespace common
} // namespace oceanbase
