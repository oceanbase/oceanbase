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

#ifndef OB_TENANT_DUTY_TASK_H
#define OB_TENANT_DUTY_TASK_H
#include <stdint.h>
#include "lib/task/ob_timer.h"
#include "lib/allocator/page_arena.h"
#include "common/object/ob_object.h"
#include "share/system_variable/ob_sys_var_class_type.h"

namespace oceanbase {
namespace observer {

class ObTenantDutyTask
    : private common::ObTimerTask
{
  static constexpr int64_t SCHEDULE_PERIOD = 10 * 1000L * 1000L;
public:
  int schedule(int tg_id);
  ObTenantDutyTask();
private:
  void runTimerTask() override;
  void update_all_tenants();

private:
  int read_obj(uint64_t tenant_id, share::ObSysVarClassType sys_var, common::ObObj &obj);
  int read_int64(uint64_t tenant_id, share::ObSysVarClassType sys_var, int64_t &val);
  // There's no system variable with double type, so here the function
  // would transform variable with decimal type to double instead. If
  // someone adds double type system variable in the future, be
  // careful when using this method.
  int read_double(uint64_t tenant_id, share::ObSysVarClassType sys_var, double &val);

  // Update tenant work area settings.
  int update_tenant_wa_percentage(uint64_t tenant_id);
  // Read tenant sql throttle settings from tenant system variables
  // and set to corresponding tenant.
  int update_tenant_sql_throttle(uint64_t tenant_id);
  // Read tenant ctx memory limit settings from tenant system variables
  // and set to corresponding tenant.
  int update_tenant_ctx_memory_throttle(uint64_t tenant_id);
  // Read tenant work area memory settings from tenant system variables.
  int read_tenant_wa_percentage(uint64_t tenant_id, int64_t &pctg);
  int update_tenant_rpc_percentage(uint64_t tenant_id);
private:
  common::ObArenaAllocator allocator_;
};

class ObTenantSqlMemoryTimerTask : private common::ObTimerTask
{
public:
  int schedule(int tg_id);
private:
  void runTimerTask() override;
private:
  static constexpr int64_t SCHEDULE_PERIOD = 3 * 1000L * 1000L;
};

}  // observer
}  // oceanbase

#endif /* OB_TENANT_DUTY_TASK_H */
