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

#define USING_LOG_PREFIX SERVER_OMT

#include "ob_multi_tenant.h"

#include "lib/oblog/ob_log.h"
#include "lib/alloc/ob_malloc_allocator.h"
#include "lib/ob_running_mode.h"
#include "share/ob_tenant_mgr.h"
#include "observer/ob_server.h"
#include "observer/ob_server_struct.h"
#include "observer/omt/ob_cgroup_ctrl.h"
#include "ob_tenant.h"
#include "rpc/ob_request.h"
#include "storage/transaction/ob_ts_mgr.h"
#include "storage/ob_tenant_config_mgr.h"
#include "observer/mysql/ob_mysql_request_manager.h"
#include "sql/engine/ob_tenant_sql_memory_manager.h"
#include "sql/engine/px/ob_px_admission.h"
#include "share/ob_get_compat_mode.h"
#include "storage/transaction/ob_tenant_weak_read_service.h"  // ObTenantWeakReadService
#include "storage/ob_partition_service.h"
#include "storage/transaction/ob_trans_audit_record_mgr.h"  // ObTenantWeakReadService
#include "lib/thread/ob_thread_name.h"

using namespace oceanbase;
using namespace oceanbase::lib;
using namespace oceanbase::common;
using namespace oceanbase::omt;
using namespace oceanbase::rpc;
using namespace oceanbase::share;
using namespace oceanbase::storage;
using namespace oceanbase::obmysql;
using namespace oceanbase::sql;
using namespace oceanbase::transaction;

namespace oceanbase {
namespace share {
// Declared in share/ob_context.h
// Obtain tenant_ctx according to tenant_id (obtained from omt)
int __attribute__((weak))
get_tenant_ctx_with_tenant_lock(const uint64_t tenant_id, ObLDHandle& handle, ObTenantSpace*& tenant_ctx)
{
  int ret = OB_SUCCESS;
  tenant_ctx = nullptr;

  omt::ObTenant* tenant = nullptr;
  if (OB_ISNULL(GCTX.omt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null ptr", K(ret));
  } else if (OB_FAIL(GCTX.omt_->get_tenant_with_tenant_lock(tenant_id, handle, tenant))) {
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      LOG_WARN("get tenant from omt failed", K(ret), K(tenant_id));
    }
  } else if (OB_ISNULL(tenant)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null ptr", K(ret), K(tenant_id));
  } else {
    tenant_ctx = &tenant->ctx();
    ;
  }

  return ret;
}
}  // end of namespace share
}  // end of namespace oceanbase

bool compare_tenant(const ObTenant* lhs, const ObTenant* rhs)
{
  return lhs->id() < rhs->id();
}

bool equal_tenant(const ObTenant* lhs, const ObTenant* rhs)
{
  return lhs->id() == rhs->id();
}

bool compare_with_tenant_id(const ObTenant* lhs, const uint64_t& tenant_id)
{
  return NULL != lhs ? (lhs->id() < tenant_id) : false;
}

bool equal_with_tenant_id(const ObTenant* lhs, const uint64_t& tenant_id)
{
  return NULL != lhs ? (lhs->id() == tenant_id) : false;
}

int ObCtxMemConfigGetter::get(common::ObIArray<ObCtxMemConfig>& configs)
{
  UNUSED(configs);
  return OB_SUCCESS;
}

ObCtxMemConfigGetter g_default_mcg;
ObICtxMemConfigGetter* ObMultiTenant::mcg_ = &g_default_mcg;

ObMultiTenant::ObMultiTenant(ObIWorkerProcessor& procor)
    : lock_(ObLatchIds::MULTI_TENANT_LOCK),
      quota2token_(DEFAULT_QUOTA2THREAD),
      worker_pool_(procor),
      tenants_(0, nullptr, ObModIds::OMT),
      token_calcer_(*this),
      node_quota_(0),
      times_of_workers_(0),
      balancer_(nullptr),
      myaddr_(),
      cpu_dump_(false),
      has_synced_(false)
{
  node_quota_ = DEFAULT_NODE_QUOTA;
}

static int init_compat_mode(ObWorker::CompatMode& compat_mode)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = CURRENT_ENTITY(TENANT_SPACE)->get_tenant_id();

  if (OB_SUCC(ret)) {
    if (is_virtual_tenant_id(tenant_id) || OB_SYS_TENANT_ID == tenant_id) {
      compat_mode = ObWorker::CompatMode::MYSQL;
    } else {
      while (OB_FAIL(ObCompatModeGetter::get_tenant_mode(tenant_id, compat_mode))) {
        if (OB_TENANT_NOT_EXIST != ret || THIS_WORKER.is_timeout()) {
          const bool is_timeout = THIS_WORKER.is_timeout();
          ret = OB_TIMEOUT;
          LOG_WARN("get tenant compatibility mode fail", K(ret), K(tenant_id), K(is_timeout));
          break;
        } else {
          usleep(200 * 1000L);
        }
      }
      if (OB_SUCC(ret)) {
        LOG_INFO("get tenant compatibility mode", K(tenant_id), K(compat_mode));
      }
    }
  }
  return ret;
}

int ObMultiTenant::init(ObAddr myaddr, double node_quota, int64_t times_of_workers, ObMySQLProxy* sql_proxy)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  myaddr_ = myaddr;
  node_quota_ = node_quota;
  const int64_t total_reserved_quota = static_cast<int64_t>(
      GCONF.system_cpu_quota + GCONF.election_cpu_quota + GCONF.user_location_cpu_quota() +
      GCONF.sys_location_cpu_quota() + GCONF.root_location_cpu_quota() + GCONF.core_location_cpu_quota() +
      EXT_LOG_TENANT_CPU + OB_MONITOR_CPU + OB_SVR_BLACKLIST_CPU);

  int64_t init_workers_cnt =
      (static_cast<int64_t>(node_quota) + static_cast<int64_t>(GCONF.server_cpu_quota_min)) * DEFAULT_TIMES_OF_WORKERS +
      total_reserved_quota * static_cast<int64_t>(quota2token_);
  if (lib::is_mini_mode()) {
    init_workers_cnt = init_workers_cnt / 2;
  }
  int64_t idle_workers_cnt = init_workers_cnt;
  auto max_workers_cnt =
      (static_cast<int64_t>(node_quota) + static_cast<int64_t>(GCONF.server_cpu_quota_max)) * times_of_workers +
      total_reserved_quota * static_cast<int64_t>(quota2token_) + static_cast<int64_t>(node_quota_) * 16;
  max_workers_cnt = std::max(std::min(max_workers_cnt, OB_MAX_THREAD_NUM), init_workers_cnt);

  LOG_INFO("tenant threads info:",
      K(node_quota),
      K(total_reserved_quota),
      K(times_of_workers),
      K_(quota2token),
      K(init_workers_cnt),
      K(idle_workers_cnt),
      K(max_workers_cnt));

  if (node_quota_ <= 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("node quota should greater than 1", K(node_quota_), K(ret));
  } else if (times_of_workers < 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("times of workers should greater than or equal to 1", K(times_of_workers), K(ret));
  } else if (OB_FAIL(worker_pool_.init(init_workers_cnt, idle_workers_cnt, max_workers_cnt))) {
    LOG_WARN("init worker pool fail",
        K(ret),
        K(init_workers_cnt),
        K(idle_workers_cnt),
        K(max_workers_cnt),
        K(node_quota),
        K(times_of_workers));
  } else {
    if (NULL != sql_proxy) {
      if (OB_FAIL(ObTenantNodeBalancer::get_instance().init(this, *sql_proxy, myaddr))) {
        LOG_WARN("failed to init tenant node balancer", K(ret));
      }
    } else {
      // unset sql_proxy to disable quota balance among nodes
    }

    if (!OB_SUCC(ret)) {
      worker_pool_.destroy();
    }
  }

  times_of_workers_ = times_of_workers;

  MTL_BIND(ObPxPools::mtl_init, ObPxPools::mtl_destroy);
  MTL_BIND(ObPxPoolStat::mtl_init, ObPxPoolStat::mtl_destroy);
  MTL_BIND(init_compat_mode, nullptr);
  MTL_BIND(ObMySQLRequestManager::mtl_init, ObMySQLRequestManager::mtl_destroy);
  MTL_BIND(ObTenantWeakReadService::mtl_init, ObTenantWeakReadService::mtl_destroy);
  MTL_BIND(storage::ObPartitionService::mtl_init, storage::ObPartitionService::mtl_destroy);
  MTL_BIND(ObTransAuditRecordMgr::mtl_init, ObTransAuditRecordMgr::mtl_destroy);
  MTL_BIND(ObTenantSqlMemoryManager::mtl_init, ObTenantSqlMemoryManager::mtl_destroy);
  MTL_BIND(ObPlanMonitorNodeList::mtl_init, ObPlanMonitorNodeList::mtl_destroy);

  return ret;
}

int ObMultiTenant::start()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObThreadPool::start())) {
    LOG_ERROR("start multi tenant thread fail", K(ret));
  } else if (OB_FAIL(ObTenantNodeBalancer::get_instance().start())) {
    LOG_ERROR("start tenant node balancer thread fail", K(ret));
  }

  if (!OB_SUCC(ret)) {
    stop();
  }
  return ret;
}

void ObMultiTenant::stop()
{
  // Stop balancer so that tenants' quota will fixed. It's not
  // necessary to put ahead, but it isn't harmful and can exclude
  // affection for balancer.
  ObTenantNodeBalancer::get_instance().stop();
  // Stop workers of all tenants thus no request of tenant would be
  // processed any more. All tenants will be removed indeed.
  {
    TenantIdList ids;
    ids.set_label(ObModIds::OMT);
    get_tenant_ids(ids);
    while (ids.size() > 0) {
      LOG_INFO("there're some tenants need destroy", "count", ids.size());

      for (TenantIdList::iterator it = ids.begin(); it != ids.end(); it++) {
        uint64_t id = *it;
        del_tenant(id);
      }
      get_tenant_ids(ids);
    }
  }
  // No tenant exist right now, so we just stop the scheduler.
  ObThreadPool::stop();
}

void ObMultiTenant::wait()
{
  ObTenantNodeBalancer::get_instance().wait();
  ObThreadPool::wait();
}

void ObMultiTenant::destroy()
{
  {
    SpinWLockGuard guard(lock_);
    tenants_.clear();
    worker_pool_.destroy();
  }
}

int ObMultiTenant::add_tenant(const uint64_t tenant_id, const double min_cpu, const double max_cpu)
{
  int ret = OB_SUCCESS;

  ObTenant* tenant = nullptr;
  {
    SpinWLockGuard guard(lock_);
    if (OB_FAIL(get_tenant_unsafe(tenant_id, tenant))) {
      ret = OB_SUCCESS;
    } else {
      ret = OB_TENANT_EXIST;
    }
  }

  ObMallocAllocator* malloc_allocator = ObMallocAllocator::get_instance();
  // tenant not exist
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(malloc_allocator)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("malloc allocator is NULL", K(ret));
    }
    for (uint64_t ctx_id = 0; OB_SUCC(ret) && ctx_id < ObCtxIds::MAX_CTX_ID; ctx_id++) {
      if (OB_FAIL(malloc_allocator->create_tenant_ctx_allocator(tenant_id, ctx_id))) {
        LOG_ERROR("create tenant allocator fail", K(ret), K(ctx_id));
      }
    }
    if (OB_SUCC(ret)) {
      // If it is a newly added tenant to limit memory, please modify the is_virtual_tenant_for_memory() function
      // synchronously
      if (OB_SERVER_TENANT_ID == tenant_id) {
        // malloc_allocator->set_tenant_limit(tenant_id, GCONF.get_reserved_server_memory());
      } else if (OB_EXT_LOG_TENANT_ID == tenant_id) {
        malloc_allocator->set_tenant_limit(tenant_id, EXT_LOG_TENANT_MEMORY_LIMIT);
      } else if (OB_DIAG_TENANT_ID == tenant_id) {
        malloc_allocator->set_tenant_limit(tenant_id, OB_DIAG_MEMORY);
      } else if (OB_RS_TENANT_ID == tenant_id) {
        const int64_t rootservice_memory_limit = GCONF.rootservice_memory_limit;
        malloc_allocator->set_tenant_limit(tenant_id, rootservice_memory_limit);
      } else if (tenant_id > OB_SERVER_TENANT_ID && tenant_id < OB_USER_TENANT_ID) {
        static const int64_t VIRTUAL_TENANT_MEMORY_LIMTI = 1L << 30;
        malloc_allocator->set_tenant_limit(tenant_id, VIRTUAL_TENANT_MEMORY_LIMTI);
      }
    }
    if (OB_SUCC(ret)) {
      ObSEArray<ObCtxMemConfig, ObCtxIds::MAX_CTX_ID> configs;
      if (OB_FAIL(mcg_->get(configs))) {
        LOG_ERROR("get ctx mem config failed", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < configs.count(); i++) {
        const uint64_t ctx_id = configs.at(i).ctx_id_;
        const int64_t idle_size = configs.at(i).idle_size_;
        const bool reserve = true;
        if (OB_FAIL(malloc_allocator->set_tenant_ctx_idle(tenant_id, ctx_id, idle_size, reserve))) {
          LOG_ERROR("set tenant ctx idle failed", K(ret));
        }
        LOG_INFO("init ctx memory finish", K(ret), K(tenant_id), K(i), K(configs.at(i)));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(GCTX.cgroup_ctrl_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("group ctrl not init", K(ret));
  } else {
    tenant = OB_NEW(ObTenant, ObModIds::OMT, tenant_id, times_of_workers_, worker_pool_, *GCTX.cgroup_ctrl_);
    if (NULL != tenant) {
      if (OB_FAIL(tenant->init_ctx())) {
        LOG_WARN("init ctx fail", K(tenant_id), K(ret));
      } else {
        CREATE_WITH_TEMP_ENTITY(RESOURCE_OWNER, tenant->id())
        {
          WITH_ENTITY(&tenant->ctx())
          {
            if (OB_FAIL(tenant->init())) {
              LOG_WARN("init tenant fail", K(tenant_id), K(ret));
            } else {
              tenant->set_unit_min_cpu(min_cpu);
              tenant->set_unit_max_cpu(max_cpu);
              tenant->set_compat_mode(MTL_GET(ObWorker::CompatMode));
              SpinWLockGuard guard(lock_);
              ObTenant* tmp_tenant = NULL;
              if (OB_FAIL(get_tenant_unsafe(tenant_id, tmp_tenant))) {
                // tenant not exist
                ret = OB_SUCCESS;
                TenantIterator iter;
                if (OB_FAIL(tenants_.insert(tenant, iter, compare_tenant))) {
                  LOG_WARN("push tenant fail", K(ret));
                  tenant->destroy();
                }
              } else {
                ret = OB_TENANT_EXIST;
              }
            }
          }
        }
      }
      if (!OB_SUCC(ret)) {
        tenant->destroy();
        ob_delete(tenant);
      }
    } else {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("new tenant fail", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    LOG_INFO("add tenant success", K(tenant_id));
    if (tenant_id >= OB_SERVER_TENANT_ID && tenant_id < OB_USER_TENANT_ID) {
      tenant->disable_user_sched();
    }
  } else {
    LOG_WARN("add tenant failed", K(tenant_id), K(ret));
  }
  if (OB_SUCC(ret)) {
    if (is_virtual_tenant_id(tenant_id)) {
      // virtual tenant don't need ts
      LOG_INFO("virtual tenant don't need ts", K(tenant_id), K(ret));
    } else {
      LOG_INFO("ts mgr add tenant success", K(tenant_id), K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(GCTX.par_ser_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("invalid argument", K(GCTX.par_ser_), K(ret));
    } else if (OB_FAIL(GCTX.par_ser_->activate_tenant(tenant_id))) {
      LOG_ERROR("activate tenant fail", K(tenant_id), K(ret));
    } else if (OB_FAIL(OTC_MGR.add_tenant_config(tenant_id))) {
      LOG_ERROR("add tenant config fail", K(tenant_id), K(ret));
#ifdef OMT_UNITTEST
    } else if (!is_virtual_tenant_id(tenant_id) &&
               OB_FAIL(OTC_MGR.got_version(tenant_id, common::ObSystemConfig::INIT_VERSION))) {
      LOG_ERROR("failed to got version", K(tenant_id), K(ret));
#endif
    } else {
      LOG_INFO("activate tenant done", K(tenant_id), K(ret));
    }
  }

  // cleanup
  if (OB_FAIL(ret)) {
    for (uint64_t ctx_id = 0; ctx_id < ObCtxIds::MAX_CTX_ID; ctx_id++) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = malloc_allocator->set_tenant_ctx_idle(tenant_id, ctx_id, 0))) {
        LOG_ERROR("cleanup failed", K(tmp_ret), K(tenant_id), K(ctx_id));
      }
    }
  }

  return ret;
}

int ObMultiTenant::modify_tenant(const uint64_t tenant_id, const double min_cpu, const double max_cpu)
{
  int ret = OB_SUCCESS;

  ObTenant* tenant = NULL;
  bool do_modify = false;

  if (OB_FAIL(get_tenant(tenant_id, tenant))) {
    LOG_WARN("can't modify tenant which doesn't exist", K(tenant_id), K(ret));
  } else if (OB_ISNULL(tenant)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected condition, tenant is NULL", K(tenant));
  } else {
    if (tenant->unit_min_cpu() != min_cpu) {
      tenant->set_unit_min_cpu(min_cpu);
      do_modify = true;
    }
    if (tenant->unit_max_cpu() != max_cpu) {
      tenant->set_unit_max_cpu(max_cpu);
      do_modify = true;
    }
  }

  if (!OB_SUCC(ret)) {
    LOG_ERROR("modify tenant failed", K(tenant_id), K(ret));
  } else if (do_modify) {
    LOG_INFO("modify tenant", K(tenant_id), K(min_cpu), K(max_cpu), K(ret));
  }

  return ret;
}

bool ObMultiTenant::has_tenant(uint64_t tenant_id) const
{
  ObTenant* tenant = NULL;
  int ret = get_tenant(tenant_id, tenant);
  return OB_SUCCESS == ret && NULL != tenant;
}

int ObMultiTenant::del_tenant(const uint64_t tenant_id, const bool wait)
{
  int ret = OB_SUCCESS;

  ObTenant* removed_tenant = NULL;

  {
    SpinWLockGuard guard(lock_);
    if (OB_FAIL(tenants_.remove_if(tenant_id, compare_with_tenant_id, equal_with_tenant_id, removed_tenant))) {
      LOG_WARN("delete tenant failed", K(tenant_id), K(ret));
    } else if (OB_ISNULL(removed_tenant)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected condition", K(ret));
    } else {
      removed_tenant->stop();
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(GCTX.session_mgr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected condition", K(ret));
    } else if (OB_FAIL(GCTX.session_mgr_->kill_tenant(tenant_id))) {
      SpinWLockGuard guard(lock_);
      TenantIterator iter;
      tenants_.insert(removed_tenant, iter, compare_tenant);
      LOG_ERROR("kill session of tenant fail", K(tenant_id));
    }
  }

  if (OB_SUCC(ret)) {
    static const int DEL_TRY_TIMES = 30;
    bool locked = false;
    ObLDHandle handle;
    for (int i = 0; i < DEL_TRY_TIMES && !locked; ++i) {
      if (!OB_FAIL(removed_tenant->try_wrlock(handle))) {
        locked = true;
      } else {
        usleep(TIME_SLICE_PERIOD);
      }
    }
    if (!OB_SUCC(ret)) {
      LOG_WARN("can't get tenant lock to delete tenant", K(tenant_id), K(ret));
      removed_tenant->lock_.ld_.print();
      SpinWLockGuard guard(lock_);
      int ret_bk = ret;
      TenantIterator iter;
      if (OB_FAIL(tenants_.insert(removed_tenant, iter, compare_tenant))) {
        LOG_ERROR("add tenant back fail", K(ret));
      }
      ret = ret_bk;
    }
  }

  if (OB_SUCC(ret)) {
    ObMallocAllocator* malloc_allocator = ObMallocAllocator::get_instance();
    if (OB_ISNULL(malloc_allocator)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("malloc allocator is NULL", K(ret));
    } else {
      // ignore ret
      for (uint64_t ctx_id = 0; ctx_id < ObCtxIds::MAX_CTX_ID; ctx_id++) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = malloc_allocator->set_tenant_ctx_idle(tenant_id, ctx_id, 0))) {
          LOG_ERROR("cleanup failed", K(tmp_ret), K(ctx_id));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    removed_tenant->stop();
    if (wait) {
      removed_tenant->wait();
      removed_tenant->destroy();
      ob_delete(removed_tenant);
    }
    LOG_INFO("delete tenant success", K(tenant_id), K(wait));
  }

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(GCTX.par_ser_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected condition", K(ret));
    } else if (OB_FAIL(GCTX.par_ser_->inactivate_tenant(tenant_id))) {
      if (OB_NOT_RUNNING == ret) {
        ret = OB_SUCCESS;
        LOG_INFO("not need to inactivate tenant");
      } else {
        LOG_ERROR("inactivate tenant fail", K(tenant_id), K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      LOG_INFO("inactivate tenant done", K(tenant_id), K(ret));
      if (NULL == GCTX.sql_engine_ || NULL == GCTX.sql_engine_->get_plan_cache_manager()) {
        ret = OB_ERR_NULL_VALUE;
        LOG_ERROR("wrong sql engine and plan cache manager", K(ret));
      } else if (OB_FAIL(GCTX.sql_engine_->get_plan_cache_manager()->revert_plan_cache(tenant_id))) {
        LOG_WARN("failed to delete tenant's plan cache value");
      } else if (OB_FAIL(GCTX.sql_engine_->get_plan_cache_manager()->revert_ps_cache(tenant_id))) {
        LOG_WARN("failed to delete tenant's ps plan cache value");
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObTenantManager::get_instance().del_tenant(tenant_id))) {
      LOG_WARN("tenant manager delete tenant failed", K(ret), K(tenant_id));
    }
  }

  return ret;
}

int ObMultiTenant::update_tenant(uint64_t tenant_id, std::function<int(ObTenant&)>&& func)
{
  int ret = OB_SUCCESS;
  ObTenant* tenant = nullptr;
  SpinRLockGuard guard(lock_);
  if (OB_FAIL(get_tenant_unsafe(tenant_id, tenant))) {
    LOG_WARN("get tenant by tenant id fail", K(ret));
  } else {
    ret = func(*tenant);
  }
  return ret;
}

int ObMultiTenant::get_tenant(const uint64_t tenant_id, ObTenant*& tenant) const
{
  SpinRLockGuard guard(lock_);
  return get_tenant_unsafe(tenant_id, tenant);
}

int ObMultiTenant::get_tenant_with_tenant_lock(const uint64_t tenant_id, ObLDHandle& handle, ObTenant*& tenant) const
{
  SpinRLockGuard guard(lock_);
  int ret = get_tenant_unsafe(tenant_id, tenant);
  if (OB_SUCC(ret)) {
    ret = tenant->rdlock(handle);
  }
  return ret;
}

int ObMultiTenant::get_tenant_unsafe(const uint64_t tenant_id, ObTenant*& tenant) const
{
  int ret = OB_SUCCESS;

  tenant = NULL;
  for (TenantList::iterator it = tenants_.begin(); it != tenants_.end() && NULL == tenant; it++) {
    if (OB_ISNULL(*it)) {
      // process the remains anyway
      LOG_ERROR("unexpected condition");
    } else if ((*it)->id() == tenant_id) {
      tenant = *it;
    }
  }

  if (NULL == tenant) {
    ret = OB_TENANT_NOT_IN_SERVER;
  }

  return ret;
}

int ObMultiTenant::recv_request(const uint64_t tenant_id, ObRequest& req)
{
  int ret = OB_SUCCESS;
  ObTenant* tenant = NULL;
  SpinRLockGuard guard(lock_);
  if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(get_tenant_unsafe(tenant_id, tenant))) {
    LOG_ERROR("get tenant failed", K(ret), K(tenant_id));
  } else if (NULL == tenant) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("tenant is null", K(ret), K(tenant_id));
  } else if (OB_FAIL(tenant->recv_request(req))) {
    LOG_ERROR("recv request failed", K(ret), K(tenant_id));
  } else {
    // do nothing
  }
  return ret;
}

void ObMultiTenant::get_tenant_ids(TenantIdList& id_list)
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  id_list.clear();
  for (TenantList::iterator it = tenants_.begin(); it != tenants_.end() && OB_SUCCESS == ret; it++) {
    if (OB_ISNULL(*it)) {
      LOG_ERROR("unexpected condition", K(*it));
    } else if (OB_FAIL(id_list.push_back((*it)->id()))) {
      LOG_ERROR("push tenant id to id list fail", K(ret));
    }
    ret = OB_SUCCESS;  // process anyway
  }
}

int ObMultiTenant::for_each(std::function<int(ObTenant&)> func)
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  for (TenantList::iterator it = tenants_.begin(); it != tenants_.end() && OB_SUCCESS == ret; it++) {
    if (OB_ISNULL(*it)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected condition", K(ret), K(*it));
    } else if (OB_FAIL(func(**it))) {
      LOG_ERROR("invoke func failed", K(ret), K(**it));
    }
  }
  return ret;
}

int ObMultiTenant::get_tenant_cpu_usage(const uint64_t tenant_id, double& usage) const
{
  int ret = OB_SUCCESS;
  ObTenant* tenant = nullptr;
  SpinRWLock& lock = const_cast<SpinRWLock&>(lock_);
  usage = 0.;
  if (!lock.try_rdlock()) {
    ret = OB_EAGAIN;
  } else {
    if (OB_FAIL(get_tenant_unsafe(tenant_id, tenant))) {
    } else {
      usage = tenant->get_token_usage() * get_token2quota();
    }
    lock.unlock();
  }

  return ret;
}

int ObMultiTenant::get_tenant_cpu(const uint64_t tenant_id, double& min_cpu, double& max_cpu) const
{
  int ret = OB_SUCCESS;
  ObTenant* tenant = NULL;
  SpinRWLock& lock = const_cast<SpinRWLock&>(lock_);

  void* ptr = nullptr;
  if (!lock.try_rdlock()) {
    ret = OB_EAGAIN;
  } else {
    if (OB_FAIL(get_tenant_unsafe(tenant_id, tenant))) {
    } else if (NULL != tenant) {
      min_cpu = tenant->unit_min_cpu();
      max_cpu = tenant->unit_max_cpu();
    }
    lock.unlock();
  }

  return ret;
}

void ObMultiTenant::set_workers_per_cpu(int64_t v)
{
  times_of_workers_ = v;
  const int64_t total_reserved_quota = static_cast<int64_t>(
      GCONF.system_cpu_quota + GCONF.election_cpu_quota + GCONF.user_location_cpu_quota() +
      GCONF.sys_location_cpu_quota() + GCONF.root_location_cpu_quota() + GCONF.core_location_cpu_quota() +
      EXT_LOG_TENANT_CPU + OB_MONITOR_CPU + OB_SVR_BLACKLIST_CPU);
  auto max_workers_cnt = static_cast<int64_t>(node_quota_) * times_of_workers_ +
                         total_reserved_quota * static_cast<int64_t>(quota2token_) +
                         static_cast<int64_t>(node_quota_) * 16;
  max_workers_cnt = std::min(max_workers_cnt, OB_MAX_THREAD_NUM);
  worker_pool_.set_max(max_workers_cnt);

  LOG_INFO("set max workers", K(max_workers_cnt));
}

void ObMultiTenant::run1()
{
  lib::set_thread_name("MultiTenant");
  while (!has_set_stop()) {
    {
      SpinRLockGuard guard(lock_);
      token_calcer_.calculate();
      for (TenantList::iterator it = tenants_.begin(); it != tenants_.end(); it++) {
        if (OB_ISNULL(*it)) {
          LOG_ERROR("unexpected condition");
        } else {
          (*it)->timeup();
        }
      }
    }
    usleep(TIME_SLICE_PERIOD);

    if (REACH_TIME_INTERVAL(10000000L)) {  // every 10s
      SpinRLockGuard guard(lock_);
      for (TenantList::iterator it = tenants_.begin(); it != tenants_.end(); it++) {
        if (!OB_ISNULL(*it)) {
          ObTaskController::get().allow_next_syslog();
          LOG_INFO("dump tenant info", "tenant", **it);
        }
      }
    }
  }
  LOG_INFO("OMT quit");
}
