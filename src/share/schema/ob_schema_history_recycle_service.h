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
#ifndef OCEANBASE_SHARE_SCHEMA_OB_SCHEMA_HISTORY_RECYCLE_SERVICE_H_
#define OCEANBASE_SHARE_SCHEMA_OB_SCHEMA_HISTORY_RECYCLE_SERVICE_H_

#include "lib/lock/ob_mutex.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
namespace share
{
namespace schema
{

class ObSchemaHistoryRecycleService
{
public:
  ObSchemaHistoryRecycleService();
  ~ObSchemaHistoryRecycleService() = default;

  static int mtl_init(ObSchemaHistoryRecycleService *&svc);
  void destroy();
  int init();
  // must be called under mtl context
  int run_once();
private:
  int check_tenant_valid_(bool &valid_tenant) const;
private:
  bool inited_;
  lib::ObMutex mutex_; // local reentrancy guard, held during one run_once execution
  int64_t last_completed_version_; // used to avoid duplicate recycle
};

} // namespace schema
} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_SCHEMA_OB_SCHEMA_HISTORY_RECYCLE_SERVICE_H_
