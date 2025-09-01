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

#ifndef OCEANBASE_SHARE_AI_SERVICE_OB_AI_SERVICE_EXECUTOR_H_
#define OCEANBASE_SHARE_AI_SERVICE_OB_AI_SERVICE_EXECUTOR_H_

#include "share/ai_service/ob_ai_service_struct.h"

namespace oceanbase
{
namespace share
{

class ObAiServiceExecutor
{
public:
  ObAiServiceExecutor() = default;
  ~ObAiServiceExecutor() = default;

  // ai endpoint
  static int create_ai_model_endpoint(common::ObArenaAllocator &allocator, const ObString &endpoint_name, const ObIJsonBase &create_jbase);
  static int alter_ai_model_endpoint(ObArenaAllocator &allocator, const ObString &endpoint_name, const ObIJsonBase &alter_jbase);
  static int drop_ai_model_endpoint(const ObString &endpoint_name);
  static int read_ai_endpoint(ObArenaAllocator &allocator, const ObString &endpoint_name, ObAiModelEndpointInfo &endpoint_info);
  static int read_ai_endpoint_by_ai_model_name(ObArenaAllocator &allocator, const ObString &ai_model_name, ObAiModelEndpointInfo &endpoint_info);

private:
  static const int64_t SPECIAL_ENDPOINT_ID_FOR_VERSION;
  static const int64_t INIT_ENDPOINT_VERSION;
  static const char *SPECIAL_ENDPOINT_SCOPE_FOR_VERSION;
  static int construct_new_endpoint(common::ObArenaAllocator &allocator,
                                    const ObAiModelEndpointInfo &old_endpoint,
                                    const ObIJsonBase &alter_jbase,
                                    ObAiModelEndpointInfo &new_endpoint);
  static int fetch_new_ai_model_endpoint_id(const uint64_t tenant_id, uint64_t &new_ai_model_endpoint_id);
  static int lock_and_fetch_endpoint_version(ObMySQLTransaction &trans, const uint64_t tenant_id, int64_t &endpoint_version);
  static int insert_special_endpoint_for_version(ObMySQLTransaction &trans, const uint64_t tenant_id);
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_AI_SERVICE_OB_AI_SERVICE_EXECUTOR_H_
