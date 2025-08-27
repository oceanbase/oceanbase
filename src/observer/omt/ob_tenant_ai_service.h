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

#ifndef OCEANBASE_OBSERVER_OMT_OB_TENANT_AI_SERVICE_H_
#define OCEANBASE_OBSERVER_OMT_OB_TENANT_AI_SERVICE_H_

#include "share/ai_service/ob_ai_service_struct.h"
#include "share/rc/ob_tenant_base.h"
#include "lib/lock/ob_tc_rwlock.h"
#include "lib/container/ob_se_array.h"
#include "sql/privilege_check/ob_ai_model_priv_util.h"
#include "share/ob_server_struct.h"
#include "deps/oblib/src/lib/worker.h"

namespace oceanbase
{
namespace omt
{

class ObAiServiceGuard
{
public:
  ObAiServiceGuard();
  ~ObAiServiceGuard();
  int get_ai_endpoint(const common::ObString &name, share::ObAiModelEndpointInfo *&endpoint_info);
  int get_ai_endpoint_by_ai_model_name(const common::ObString &ai_model_name, share::ObAiModelEndpointInfo *&endpoint_info);
private:
  int check_access_privilege();
  ObArenaAllocator local_allocator_;
  DISALLOW_COPY_AND_ASSIGN(ObAiServiceGuard);
};

class ObTenantAiService
{
public:
  ObTenantAiService();
  virtual ~ObTenantAiService();
  static int mtl_init(ObTenantAiService* &tenant_ai_service);
  int init();
  int start() { return OB_SUCCESS; }
  void stop() {};
  void wait() {};
  void destroy();
public:
  int get_ai_service_guard(ObAiServiceGuard &ai_service_guard);
private:
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObTenantAiService);
};

} // namespace omt
} // namespace oceanbase

#endif // OCEANBASE_OBSERVER_OMT_OB_TENANT_AI_SERVICE_H_
