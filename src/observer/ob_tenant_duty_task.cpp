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

#define USING_LOG_PREFIX SERVER
#include "ob_tenant_duty_task.h"
#include "lib/alloc/alloc_func.h"
#include "lib/thread/thread_mgr.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/system_variable/ob_system_variable_alias.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/rc/ob_tenant_base.h"
#include "observer/ob_server_struct.h"
#include "observer/omt/ob_multi_tenant.h"
#include "sql/engine/ob_tenant_sql_memory_manager.h"
#include "observer/omt/ob_tenant.h"
#include "observer/omt/ob_tenant_config_mgr.h"

using namespace oceanbase::common;

namespace oceanbase {
using namespace share;
using namespace share::schema;
using namespace sql;
namespace observer {

ObTenantDutyTask::ObTenantDutyTask()
  : allocator_(ObModIds::OB_DUTY_TASK)
{
}

void ObTenantDutyTask::runTimerTask()
{
  allocator_.reset_remain_one_page();
  update_all_tenants();
}

int ObTenantDutyTask::schedule(int tg_id)
{
  return TG_SCHEDULE(tg_id, *this, SCHEDULE_PERIOD, true);
}

void ObTenantDutyTask::update_all_tenants()
{
  int ret = OB_SUCCESS;
  omt::TenantIdList ids(nullptr, ObModIds::OB_DUTY_TASK);
  GCTX.omt_->get_tenant_ids(ids);

  for (int64_t i = 0; i < ids.size(); i++) {
    if (ids[i] <= OB_USER_TENANT_ID) {
      continue;
    } else {
      if (OB_FAIL(update_tenant_wa_percentage(ids[i]))) {
        LOG_WARN("update tenant work area memory fail", K(ret));
        // Ignore this error code since successive operations
        // shouldn't relay on it.
        ret = OB_SUCCESS;
      }
      if (OB_FAIL(update_tenant_sql_throttle(ids[i]))) {
        LOG_WARN("update tenant sql throttle fail", K(ret));
         // Ignore this error code since successive operations
        // shouldn't relay on it.
        ret = OB_SUCCESS;
      }
      if (OB_FAIL(update_tenant_ctx_memory_throttle(ids[i]))) {
        LOG_WARN("update tenant ctx throttle fail", K(ret));
        ret = OB_SUCCESS;
      }
      if (OB_FAIL(update_tenant_rpc_percentage(ids[i]))) {
        LOG_WARN("update tenant rpc percentage fail", K(ret));
        ret = OB_SUCCESS;
      }
    }
  }
}

int ObTenantDutyTask::update_tenant_sql_throttle(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  omt::ObSqlThrottleMetrics metrics;
  if (OB_SUCC(read_int64(
                  tenant_id,
                  SYS_VAR_SQL_THROTTLE_PRIORITY, metrics.priority_)) &&
      OB_SUCC(read_double(
                  tenant_id,
                  SYS_VAR_SQL_THROTTLE_RT, metrics.rt_)) &&
      OB_SUCC(read_double(
                  tenant_id,
                  SYS_VAR_SQL_THROTTLE_CPU, metrics.cpu_)) &&
      OB_SUCC(read_int64(
                  tenant_id,
                  SYS_VAR_SQL_THROTTLE_IO, metrics.io_)) &&
      OB_SUCC(read_int64(
                  tenant_id,
                  SYS_VAR_SQL_THROTTLE_LOGICAL_READS, metrics.logical_reads_)) &&
      OB_SUCC(read_double(
                  tenant_id,
                  SYS_VAR_SQL_THROTTLE_NETWORK, metrics.queue_time_))) {
    GCTX.omt_->update_tenant(tenant_id, [&metrics] (omt::ObTenant &tenant) {
      tenant.update_sql_throttle_metrics(metrics);
      if (metrics.priority_ > 0) {
        LOG_INFO("SQL throttle start", "tenant_id", tenant.id(), K(metrics));
      }
      return OB_SUCCESS;
    });
  } else {
    LOG_WARN("get tenant sql throttle metrics fail", K(ret));
  }
  if (OB_SUCC(ret)) {
    int64_t throughput = -1;
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
    if (OB_LIKELY(tenant_config.is_valid())) {
      throughput = tenant_config->_ob_query_rate_limit;
    }
    GCTX.omt_->update_tenant(tenant_id, [&throughput] (omt::ObTenant &tenant) {
      tenant.update_sql_throughput(throughput);
      return OB_SUCCESS;
    });
  }
  return ret;
}

int ObTenantDutyTask::update_tenant_ctx_memory_throttle(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  if (!tenant_config.is_valid()) {
    // do nothing
  } else {
    ObCtxMemoryLimitChecker checker;
    uint64_t ctx_id = 0;
    int64_t limit = 0;
    ObMallocAllocator *alloc = ObMallocAllocator::get_instance();
    if (!checker.check(tenant_config->_ctx_memory_limit, ctx_id, limit)) {
      // do nothing
    } else {
      if ('\0' == tenant_config->_ctx_memory_limit[0]) {
        ctx_id = ObCtxIds::MAX_CTX_ID;
        limit = INT64_MAX; // empty str means no limit, and not care ctx_id.
      }
      for (int i = 0; i < ObCtxIds::MAX_CTX_ID; i++) {
        if (ObCtxIds::WORK_AREA == i ||
            ObCtxIds::META_OBJ_CTX_ID == i ||
            ObCtxIds::DO_NOT_USE_ME == i) {
          // use sql_work_area
          continue;
        }
        auto ta = alloc->get_tenant_ctx_allocator(tenant_id, i);
        if (OB_NOT_NULL(ta)) {
          if (OB_FAIL(ta->set_limit(ctx_id == i ? limit : INT64_MAX))) {
            LOG_ERROR("set_limit failed", K(ret), K(tenant_id), K(ctx_id), K(limit));
          }
        }
      }
    }
  }
  return ret;
}

int ObTenantDutyTask::read_tenant_wa_percentage(uint64_t tenant_id, int64_t &pctg)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObSysVarSchema *var_schema = NULL;
  ObObj value;
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null");
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("get schema guard failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_system_variable(tenant_id, SYS_VAR_OB_SQL_WORK_AREA_PERCENTAGE, var_schema))) {
    LOG_WARN("get tenant system variable failed", K(ret), K(tenant_id));
  } else if (OB_ISNULL(var_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("var_schema is null");
  } else if (OB_FAIL(var_schema->get_value(&allocator_, NULL, value))) {
    LOG_WARN("get value from var_schema failed", K(ret), K(*var_schema));
  } else if (OB_FAIL(value.get_int(pctg))) {
    LOG_WARN("get int from value failed", K(ret), K(value));
  }
  return ret;
}

//////////////////////////////////////////////////////////////////////////////////////////

int ObTenantSqlMemoryTimerTask::schedule(int tg_id)
{
  return TG_SCHEDULE(tg_id, *this, SCHEDULE_PERIOD, true);
}

void ObTenantSqlMemoryTimerTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  omt::TenantIdList ids(nullptr, ObModIds::OB_DUTY_TASK);
  GCTX.omt_->get_tenant_ids(ids);
  // Each tenant must calculate the global bound size regularly, so the failure of one tenant should not affect other tenants, so there is no judgment OB_SUCC(ret) to end
  for (int64_t i = 0; i < ids.size(); i++) {
    if (ids[i] <= OB_MAX_RESERVED_TENANT_ID) {
      continue;
    } else {
      MTL_SWITCH(ids[i]) {
        ObTenantSqlMemoryManager *sql_mem_mgr = MTL(ObTenantSqlMemoryManager*);
        if (OB_UNLIKELY(nullptr == sql_mem_mgr)) {
          LOG_WARN("sql memory manager is null", K(ids[i]));
        } else if (OB_FAIL(sql_mem_mgr->calculate_global_bound_size())) {
          LOG_WARN("failed to calculate global bound size", K(ret), K(ids[i]));
        }
      }
    }
  }
}

int ObTenantDutyTask::update_tenant_wa_percentage(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  int64_t wa_pctg = 0;
  if (OB_FAIL(read_tenant_wa_percentage(tenant_id, wa_pctg))) {
    LOG_WARN("read variable tenant work area percentage fail", K(ret));
  } else if (wa_pctg < 0 || wa_pctg > 100) {
    LOG_WARN("work area memroy percentage "
             "shouldn't greater than 100 or be negative",
             K(wa_pctg));
  } else {
    auto ta = lib::ObMallocAllocator::get_instance()->get_tenant_ctx_allocator(
        tenant_id, common::ObCtxIds::WORK_AREA);
    if (ta != nullptr) {
      if (OB_FAIL(lib::set_wa_limit(tenant_id, wa_pctg))) {
        LOG_WARN("set tenant work area memory",
                 K(tenant_id), K(wa_pctg), K(ret));
      } else {
        LOG_INFO("set tenant work area memory",
                 K(tenant_id),
                 K(wa_pctg),
                 "limit", ta->get_limit(),
                 K(ret));
       }
     }
  }
  return ret;
}

int ObTenantDutyTask::update_tenant_rpc_percentage(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  if (!tenant_config.is_valid()) {
    // do nothing
  } else {
    int64_t rpc_pct_lmt = tenant_config->rpc_memory_limit_percentage;
    if (0 == rpc_pct_lmt) {
      rpc_pct_lmt = 100;
    }
    if (OB_FAIL(set_rpc_limit(tenant_id, rpc_pct_lmt))) {
      LOG_WARN("failed to set tenant rpc ctx limit", K(ret), K(tenant_id), K(rpc_pct_lmt));
    }
  }
  return ret;
}

int ObTenantDutyTask::read_obj(
    uint64_t tenant_id, ObSysVarClassType sys_var, common::ObObj &obj)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObSysVarSchema *var_schema = NULL;
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null");
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("get schema guard failed", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_tenant_system_variable(
                         tenant_id, sys_var, var_schema))) {
    LOG_WARN("get tenant system variable failed", K(ret), K(tenant_id));
  } else if (OB_ISNULL(var_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("var_schema is null");
  } else if (OB_FAIL(var_schema->get_value(&allocator_, NULL, obj))) {
    LOG_WARN("get value from var_schema failed", K(ret), K(*var_schema));
  }
  return ret;
}

int ObTenantDutyTask::read_int64(
    uint64_t tenant_id, ObSysVarClassType sys_var, int64_t &val)
{
  int ret = OB_SUCCESS;
  ObObj obj;
  if (OB_FAIL(read_obj(tenant_id, sys_var, obj))) {
    LOG_WARN("get object from tenant system variable fail", K(ret));
  } else if (OB_FAIL(obj.get_int(val))) {
    LOG_WARN("get int value from object fail", K(ret), K(obj));
  }
  return ret;
}

int ObTenantDutyTask::read_double(
    uint64_t tenant_id, ObSysVarClassType sys_var, double &val)
{
  int ret = OB_SUCCESS;
  ObObj obj;
  number::ObNumber num;
  if (OB_FAIL(read_obj(tenant_id, sys_var, obj))) {
    LOG_WARN("get object from tenant system variable fail", K(ret));
  } else if (OB_FAIL(obj.get_number(num))) {
    LOG_WARN("get number value from object fail", K(ret), K(obj));
  } else {
    char buf[32] = {};
    int64_t pos = 0;
    if (OB_SUCC(num.format(buf, sizeof (buf), pos, 6))) {
      if (pos < sizeof (buf)) {
          val = atof(buf);
      } else {
        ret = OB_SIZE_OVERFLOW;
      }
    }
  }
  return ret;
}

}  // observer
}  // oceanbase
