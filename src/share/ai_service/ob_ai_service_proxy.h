/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_SHARE_AI_SERVICE_OB_AI_SERVICE_PROXY_H_
#define OCEANBASE_SHARE_AI_SERVICE_OB_AI_SERVICE_PROXY_H_

#include "share/ai_service/ob_ai_service_struct.h"

namespace oceanbase
{
namespace share
{

class ObAiServiceProxy
{
public:
  ObAiServiceProxy() = default;
  ~ObAiServiceProxy() = default;
  // ai endpoint
  static int insert_ai_endpoint(const uint64_t tenant_id, ObMySQLTransaction &trans, const int64_t new_endpoint_version, const ObAiModelEndpointInfo &endpoint);
  static int select_ai_endpoint(const uint64_t tenant_id, common::ObArenaAllocator &allocator, ObISQLClient &sql_proxy,
                                const ObString &name, ObAiModelEndpointInfo &endpoint, bool for_update = false);
  static int drop_ai_model_endpoint(const uint64_t tenant_id, ObMySQLTransaction &trans, const ObString &name);
  static int update_ai_endpoint(const uint64_t tenant_id, ObMySQLTransaction &trans, const int64_t new_endpoint_version, const ObAiModelEndpointInfo &endpoint);
  static int select_ai_endpoint_by_ai_model_name(const uint64_t tenant_id, common::ObArenaAllocator &allocator, ObISQLClient &sql_proxy,
                                                 const ObString &ai_model_name, common::ObNameCaseMode name_case_mode, ObAiModelEndpointInfo &endpoint);
  static int check_ai_endpoint_exists(const uint64_t tenant_id, common::ObArenaAllocator &allocator, ObISQLClient &sql_proxy, const ObString &name, bool &is_exists);

private:
  // ai endpoint
  static int build_ai_endpoint_(common::ObArenaAllocator &allocator, common::sqlclient::ObMySQLResult &result, ObAiModelEndpointInfo &endpoint);
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_AI_SERVICE_OB_AI_SERVICE_PROXY_H_
