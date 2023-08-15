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

#ifndef SRC_SHARE_ERRSIM_OB_TENANT_ERRSIM_MODULE_MGR_H_
#define SRC_SHARE_ERRSIM_OB_TENANT_ERRSIM_MODULE_MGR_H_

#include "lib/ob_define.h"
#include "lib/utility/ob_print_utils.h"
#include "common/errsim_module/ob_errsim_module_type.h"
#include "lib/hash/ob_hashset.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/lock/ob_bucket_lock.h"
#include "lib/utility/ob_backtrace.h"
#include "common/errsim_module/ob_tenant_errsim_event.h"

namespace oceanbase
{
namespace share
{

class ObTenantErrsimEventMgr
{
public:
  ObTenantErrsimEventMgr();
  virtual ~ObTenantErrsimEventMgr();
  static int mtl_init(ObTenantErrsimEventMgr *&errsim_module_mgr);
  int init();
  int add_tenant_event(
      const ObTenantErrsimEvent &event);
  void destroy();
private:
  bool is_inited_;
  common::SpinRWLock lock_;
  ObArray<ObTenantErrsimEvent> event_array_;
  DISALLOW_COPY_AND_ASSIGN(ObTenantErrsimEventMgr);
};

} // namespace share
} // namespace oceanbase
#endif
