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

#define USING_LOG_PREFIX LIB
#include "sql/executor/ob_memory_tracker.h"
#include "sql/session/ob_sql_session_info.h"
#include "lib/rc/context.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "share/rc/ob_tenant_base.h"

using namespace oceanbase::lib;

thread_local ObMemTracker ObMemTrackerGuard::mem_tracker_;

void ObMemTrackerGuard::reset_try_check_tick()
{
  mem_tracker_.try_check_tick_ = 0;
}

void ObMemTrackerGuard::dump_mem_tracker_info()
{
  int64_t tenant_mem_limit = lib::get_tenant_memory_limit(MTL_ID());
  int64_t mem_quota_pct = 100;
  int64_t tree_mem_hold = 0;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  if (OB_UNLIKELY(tenant_config.is_valid())) {
    mem_quota_pct = tenant_config->query_memory_limit_percentage;
  }
  if (nullptr != mem_tracker_.mem_context_) {
    tree_mem_hold = mem_tracker_.mem_context_->tree_mem_hold();
  }
  int64_t mem_limit = tenant_mem_limit / 100 * mem_quota_pct;
  SQL_LOG(INFO, "dump memory tracker info", K(MTL_ID()), K(tenant_mem_limit), K(mem_limit),
          K(tree_mem_hold));
}

void ObMemTrackerGuard::update_config()
{
  int ret = common::OB_SUCCESS;
  int64_t tenant_mem_limit = lib::get_tenant_memory_limit(MTL_ID());
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  if (OB_UNLIKELY(tenant_config.is_valid())) {
    mem_tracker_.mem_quota_pct_ = tenant_config->query_memory_limit_percentage;
  }
  mem_tracker_.cache_mem_limit_ = (0 == mem_tracker_.mem_quota_pct_) ?
                                    0 :
                                    tenant_mem_limit / 100 * mem_tracker_.mem_quota_pct_;
}
int ObMemTrackerGuard::check_status()
{
  int ret = common::OB_SUCCESS;
  if (-1 == mem_tracker_.cache_mem_limit_
    || (++mem_tracker_.check_status_times_ % UPDATE_MEM_LIMIT_THRESHOLD == 0)) {
    update_config();
  }
  if (nullptr != mem_tracker_.mem_context_ && (UNLIMITED_MEM_QUOTA_PCT != mem_tracker_.mem_quota_pct_)) {
    int64_t tree_mem_hold = mem_tracker_.mem_context_->tree_mem_hold();
    mem_tracker_.cur_mem_used_ = tree_mem_hold;
    mem_tracker_.peek_mem_used_ = max(mem_tracker_.peek_mem_used_, mem_tracker_.cur_mem_used_);
    // update session info mem used
    sql::ObSQLSessionInfo *session = THIS_WORKER.get_session();
    if (nullptr != session) {
      session->set_sql_mem_used(mem_tracker_.cur_mem_used_);
    }
    if (tree_mem_hold >= mem_tracker_.cache_mem_limit_) {
      ret = OB_EXCEED_QUERY_MEM_LIMIT;
      SQL_LOG(WARN, "Exceeded memory usage limit", K(ret), K(tree_mem_hold),
              K(mem_tracker_.cache_mem_limit_));
      LOG_USER_ERROR(OB_EXCEED_QUERY_MEM_LIMIT, mem_tracker_.cache_mem_limit_, tree_mem_hold);
    }
  }
  return ret;
}
int ObMemTrackerGuard::try_check_status(int64_t check_try_times)
{
  int ret = common::OB_SUCCESS;
  if (nullptr != mem_tracker_.mem_context_
      && ((++mem_tracker_.try_check_tick_) % check_try_times == 0)) {
    ret = check_status();
  }
  return ret;
}
