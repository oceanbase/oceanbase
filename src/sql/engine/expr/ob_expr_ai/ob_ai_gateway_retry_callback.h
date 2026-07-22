/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_AI_GATEWAY_RETRY_CALLBACK_H_
#define OCEANBASE_SQL_OB_AI_GATEWAY_RETRY_CALLBACK_H_

#include "sql/engine/expr/ob_expr_ai/ob_ai_func_client.h"

namespace oceanbase
{
namespace share
{
class ObAiGatewayRouteSession;
struct ObAiGatewayEndpoint;
struct ObAIModelConfigInfo;
}
namespace common
{

// Base for the three gateway retry callbacks (complete / embed / rerank).
// before_retry() is the shared template: record the just-failed endpoint, advance
// to the next endpoint, rebuild endpoint-specific headers, and re-arm the client.
// Subclasses supply only rebuild_headers(). before_retry() lives in the .cpp to
// avoid a circular include with ob_ai_func_utils.h.
class ObAiGatewayRetryCallbackBase : public ObAIRetryCallback
{
public:
  ObAiGatewayRetryCallbackBase(share::ObAiGatewayRouteSession &session,
                               share::ObAiGatewayEndpoint &gw_endpoint,
                               share::ObAIModelConfigInfo &ep_config,
                               common::ObIAllocator &allocator,
                               const share::ObAIModelConfigInfo &model_config_info)
    : session_(session), client_(nullptr), gw_endpoint_(gw_endpoint),
      ep_config_(ep_config), allocator_(allocator), model_config_info_(model_config_info)
  {}
  virtual ~ObAiGatewayRetryCallbackBase() {}

  void set_client(ObAIFuncClient &client) override { client_ = &client; }

  // Shared retry template; subclasses must not override it.
  int before_retry() override;

  // Header array for the current endpoint; the rerank batch loop reads it by ref.
  ObArray<ObString> &get_headers() { return ep_headers_; }

protected:
  // Rebuild ep_headers_ from the (already advanced) ep_config_. Called only by
  // before_retry(); initial headers are built by each path's own request builder.
  virtual int rebuild_headers() = 0;

  share::ObAiGatewayRouteSession &session_;
  ObAIFuncClient *client_;
  share::ObAiGatewayEndpoint &gw_endpoint_;
  share::ObAIModelConfigInfo &ep_config_;
  common::ObIAllocator &allocator_;
  const share::ObAIModelConfigInfo &model_config_info_;
  ObArray<ObString> ep_headers_;
};

} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_SQL_OB_AI_GATEWAY_RETRY_CALLBACK_H_
