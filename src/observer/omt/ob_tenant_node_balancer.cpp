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
#include "ob_tenant_node_balancer.h"
#include "lib/time/ob_time_utility.h"
#include "lib/oblog/ob_log.h"
#include "lib/alloc/ob_malloc_allocator.h"
#include "lib/container/ob_se_array_iterator.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/ob_tenant_mgr.h"
#include "share/ob_debug_sync.h"
#include "share/system_variable/ob_sys_var_class_type.h"
#include "observer/ob_inner_sql_result.h"
#include "ob_tenant.h"
#include "ob_multi_tenant.h"
#include "share/allocator/ob_tenant_mutil_allocator.h"
#include "share/allocator/ob_tenant_mutil_allocator_mgr.h"
#include "share/config/ob_server_config.h"
#include "observer/ob_server_struct.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "storage/blocksstable/ob_store_file.h"
#include "storage/ob_partition_service.h"
#include "storage/ob_tenant_config_mgr.h"
#include "storage/ob_tenant_meta_memory_mgr.h"

using namespace oceanbase::obsys;
using namespace oceanbase::lib;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::omt;
using namespace oceanbase::observer;
using namespace oceanbase::storage;

ObTenantNodeBalancer::ObTenantNodeBalancer()
    : omt_(NULL),
      myaddr_(),
      unit_getter_(),
      lock_(common::ObLatchIds::CONFIG_LOCK),
      refresh_interval_(10L * 1000L * 1000L)
{
  if (lib::is_mini_mode()) {
    refresh_interval_ /= 2;
  }
}

ObTenantNodeBalancer::~ObTenantNodeBalancer()
{
  omt_ = NULL;
  myaddr_.reset();
}

int ObTenantNodeBalancer::init(ObMultiTenant* omt, common::ObMySQLProxy& sql_proxy, const common::ObAddr& myaddr)
{
  int ret = OB_SUCCESS;
  myaddr_ = myaddr;
  if (OB_FAIL(unit_getter_.init(sql_proxy, &GCONF))) {
    LOG_ERROR("init unit getter fail", K(ret));
  } else {
    omt_ = omt;
    myaddr_ = myaddr;
  }
  return ret;
}

void ObTenantNodeBalancer::run1()
{
  int ret = OB_SUCCESS;
  lib::set_thread_name("OmtNodeBalancer");
  while (!has_set_stop()) {
    TenantUnits units;
    if (OB_FAIL(unit_getter_.get_server_tenant_configs(myaddr_, units))) {
      LOG_WARN("get server tenant units fail", K(myaddr_), K(ret));
    } else if (OB_FAIL(refresh_tenant(units))) {
      LOG_WARN("failed to refresh tenant", K(ret), K(units));
    } else if (FALSE_IT(periodically_check_tenant())) {
      // never reach here
    }

    // will try to update tma whether tenant unit is changed or not,
    // because memstore_limit_percentage may be changed
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = TMA_MGR_INSTANCE.update_tenant_mem_limit(units))) {
      LOG_WARN("TMA_MGR_INSTANCE.update_tenant_mem_limit failed", K(tmp_ret));
    }

    // check whether tenant unit is changed, try to update unit config of tenant
    ObSEArray<uint64_t, 10> tenants;
    if (OB_FAIL(unit_getter_.get_tenants(tenants))) {
      LOG_WARN("get cluster tenants fail", K(ret));
    } else if (OB_FAIL(OTC_MGR.refresh_tenants(tenants))) {
      LOG_WARN("fail refresh tenant config", K(tenants), K(ret));
    }

    USLEEP(refresh_interval_);  // sleep 10s
  }
}

// tenant units loaded from file must be updated successfully
int ObTenantNodeBalancer::update_tenant(TenantUnits& units, const bool is_local)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  // no matter whether success or not before
  if (OB_FAIL(check_new_tenant(units))) {
    tmp_ret = ret;
    LOG_WARN("check and add new tenant fail", K(ret));
  } else if (0 != units.count() || !is_local) {
    omt_->set_synced();
  }

  if (OB_FAIL(check_del_tenant(units))) {
    tmp_ret = ret;
    LOG_WARN("check delete tenant fail", K(ret));
  }

  ret = tmp_ret;
  return ret;
}

int ObTenantNodeBalancer::notify_create_tenant(const obrpc::TenantServerUnitConfig& unit)
{
  LOG_INFO("succ to receive notify of creating tenant", K(unit));
  int ret = OB_SUCCESS;

  if (!unit.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(unit));
  } else if (ObTenantManager::get_instance().has_tenant(unit.tenant_id_) && OB_SYS_TENANT_ID != unit.tenant_id_) {
    ret = OB_TENANT_EXIST;
    LOG_WARN("tenant has exist", K(ret), K(unit));
  } else {
    ObUnitInfoGetter::ObTenantConfig config;
    config.tenant_id_ = unit.tenant_id_;
    config.has_memstore_ = (unit.replica_type_ != REPLICA_TYPE_LOGONLY);
    config.unit_stat_ = ObUnitInfoGetter::ObUnitStatus::UNIT_NORMAL;
    config.config_ = unit.unit_config_;
    config.mode_ = unit.compat_mode_;
    config.create_timestamp_ = ObTimeUtility::current_time();

    if (!unit.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(unit));
    } else if (OB_FAIL(create_new_tenant(config))) {
      LOG_WARN("failed to refresh tenant", K(ret), K(unit), K(config));
    } else {
      LOG_INFO("succ to create new tenant", K(unit), K(config));
    }
  }
  return ret;
}

// Only mark a deletion here, delete tenant will be excute in refresh_tenant
int ObTenantNodeBalancer::try_notify_drop_tenant(const int64_t tenant_id)
{
  LOG_INFO("succ to receive notify of dropping tenant", K(tenant_id));
  int ret = OB_SUCCESS;
  TenantUnits units;
  TCWLockGuard guard(lock_);

  if (OB_FAIL(storage::ObTenantConfigMgr::get_instance().get_tenant_units(units))) {
    LOG_WARN("failed to get tenant units", K(ret));
  } else {
    for (int64_t i = 0; i < units.count(); i++) {
      if (units.at(i).tenant_id_ == tenant_id) {
        units.at(i).is_removed_ = true;
      }
    }

    if (OB_FAIL(storage::ObTenantConfigMgr::get_instance().write_tenant_units(units))) {
      LOG_WARN("failed to write tenant units", K(ret));
    } else {
      LOG_INFO("succ to mark drop tenant", K(tenant_id), K(units));
    }
  }

  return ret;
}

int ObTenantNodeBalancer::get_server_allocated_resource(ServerResource& server_resource)
{
  int ret = OB_SUCCESS;
  server_resource.reset();
  TenantUnits tenant_units;

  if (OB_FAIL(storage::ObTenantConfigMgr::get_instance().get_tenant_units(tenant_units))) {
    LOG_WARN("failed to get tenant units");
  } else {
    for (int64_t i = 0; i < tenant_units.count(); i++) {
      server_resource.max_cpu_ += tenant_units.at(i).config_.max_cpu_;
      server_resource.min_cpu_ += tenant_units.at(i).config_.min_cpu_;
      server_resource.max_memory_ += tenant_units.at(i).config_.max_memory_;
      server_resource.min_memory_ += tenant_units.at(i).config_.min_memory_;
    }
  }
  return ret;
}

// locked in the caller
bool ObTenantNodeBalancer::is_tenant_exist(const int64_t tenant_id)
{
  int ret = OB_SUCCESS;
  bool is_exist = false;
  TenantUnits tenant_units;

  if (OB_FAIL(storage::ObTenantConfigMgr::get_instance().get_tenant_units(tenant_units))) {
    LOG_WARN("failed to get tenant units", K(ret));
  } else {
    for (int64_t i = 0; i < tenant_units.count(); i++) {
      if (tenant_id == tenant_units.at(i).tenant_id_) {
        is_exist = true;
        break;
      }
    }
  }

  return is_exist;
}

int ObTenantNodeBalancer::lock_tenant_balancer()
{
  return lock_.rdlock();
}

int ObTenantNodeBalancer::unlock_tenant_balancer()
{
  return lock_.unlock();
}

int ObTenantNodeBalancer::check_del_tenant(TenantUnits& units)
{
  int ret = OB_SUCCESS;
  TenantIdList ids(nullptr, ObModIds::OMT);
  ids.set_label(ObModIds::OMT);
  omt_->get_tenant_ids(ids);

  for (auto it = ids.begin(); it != ids.end(); it++) {
    const auto id = *it;
    if (OB_SYS_TENANT_ID == id) {
      // TODO: sys tenant can't be deleted right now.
      continue;
    } else if (OB_SERVER_TENANT_ID == id) {
      omt_->modify_tenant(id, GCONF.system_cpu_quota, GCONF.system_cpu_quota);
    } else if (OB_ELECT_TENANT_ID == id) {
      omt_->modify_tenant(id, GCONF.election_cpu_quota, GCONF.election_cpu_quota);
    } else if (OB_LOC_CORE_TENANT_ID == id) {
      omt_->modify_tenant(id, GCONF.core_location_cpu_quota(), GCONF.core_location_cpu_quota());
    } else if (OB_LOC_ROOT_TENANT_ID == id) {
      omt_->modify_tenant(id, GCONF.root_location_cpu_quota(), GCONF.root_location_cpu_quota());
    } else if (OB_LOC_SYS_TENANT_ID == id) {
      omt_->modify_tenant(id, GCONF.sys_location_cpu_quota(), GCONF.sys_location_cpu_quota());
    } else if (OB_LOC_USER_TENANT_ID == id) {
      omt_->modify_tenant(id, GCONF.user_location_cpu_quota(), GCONF.user_location_cpu_quota());
    } else if (OB_EXT_LOG_TENANT_ID == id) {
      continue;
    } else if (OB_MONITOR_TENANT_ID == id) {
      continue;
    } else if (OB_DTL_TENANT_ID == id) {
      continue;
    } else if (OB_RS_TENANT_ID == id) {
      continue;
    } else if (OB_SVR_BLACKLIST_TENANT_ID == id) {
      continue;
    } else if (OB_DATA_TENANT_ID == id) {
      continue;
    } else if (OB_DIAG_TENANT_ID == id) {
      continue;
    } else {
      bool tenant_exists = false;
      for (auto punit = units.begin(); punit != units.end(); punit++) {
        if (id == punit->tenant_id_) {
          tenant_exists = true;
          break;
        }
      }
      if (!tenant_exists) {
        if (OB_FAIL(omt_->del_tenant(id))) {
          LOG_WARN("delete tenant fail", K(id), K(ret));
          ret = OB_SUCCESS;
        }
      }
    }
  }
  return ret;
}

int ObTenantNodeBalancer::check_new_tenant(TenantUnits& units)
{
  int ret = OB_SUCCESS;
  ObTenantManager& omti = ObTenantManager::get_instance();

  DEBUG_SYNC(CHECK_NEW_TENANT);

  // check all units of tenants.
  for (TenantUnits::iterator it = units.begin(); it != units.end(); it++) {
    if (OB_FAIL(check_new_tenant(*it))) {
      LOG_WARN("failed to check new tenant", K(ret));
    }
  }

  return ret;
}

int ObTenantNodeBalancer::check_new_tenant(const ObUnitInfoGetter::ObTenantConfig& config)
{
  int ret = OB_SUCCESS;
  ObTenantManager& omti = ObTenantManager::get_instance();

  DEBUG_SYNC(CHECK_NEW_TENANT);

  const int64_t id = config.tenant_id_;
  int64_t mem_limit = config.config_.max_memory_;
  if (!omti.has_tenant(id)) {
    if (OB_FAIL(omti.add_tenant(id))) {
      LOG_WARN("failed to add tenant", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    ObMallocAllocator* malloc_allocator = ObMallocAllocator::get_instance();
    if (OB_ISNULL(malloc_allocator)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("malloc allocator is NULL", K(ret));
    } else {
      for (uint64_t ctx_id = 0; OB_SUCC(ret) && ctx_id < ObCtxIds::MAX_CTX_ID; ctx_id++) {
        if (OB_FAIL(malloc_allocator->create_tenant_ctx_allocator(id, ctx_id))) {
          LOG_ERROR("create tenant allocator fail", K(ret), K(ctx_id));
        }
      }

      if (OB_SUCC(ret)) {
        const int64_t pre_mem_limit = malloc_allocator->get_tenant_limit(id);
        const int64_t mem_hold = malloc_allocator->get_tenant_hold(id);
        const int64_t target_mem_limit = mem_limit;
        // make sure half reserve memory available
        if (target_mem_limit < pre_mem_limit) {
          mem_limit =
              mem_hold + static_cast<int64_t>(static_cast<double>(target_mem_limit) * TENANT_RESERVE_MEM_RATIO / 2.);
          if (mem_limit < target_mem_limit) {
            mem_limit = target_mem_limit;
          }
          if (mem_limit < pre_mem_limit) {
            LOG_INFO("reduce memory quota", K(mem_limit), K(pre_mem_limit), K(target_mem_limit), K(mem_hold));
          } else {
            mem_limit = pre_mem_limit;
            LOG_WARN("try to reduce memory quota, but free memory not enough",
                K(mem_limit),
                K(pre_mem_limit),
                K(target_mem_limit),
                K(mem_hold));
          }
        }
        if (mem_limit != pre_mem_limit) {
          malloc_allocator->set_tenant_limit(id, mem_limit);
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    int64_t min_mem = 0;
    int64_t max_mem = 0;
    if (OB_FAIL(omti.get_tenant_mem_limit(id, min_mem, max_mem))) {
      if (OB_NOT_REGISTERED == ret) {  // tenant mem limit has not been setted
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("get tenant memory fail", K(id));
      }
    }
    if (OB_SUCC(ret)) {
      if (min_mem != config.config_.min_memory_ || max_mem != mem_limit) {
        LOG_INFO("tenant memory changed",
            "before_min",
            min_mem,
            "before_max",
            max_mem,
            "after_min",
            config.config_.min_memory_,
            "after_max",
            mem_limit);
        omti.set_tenant_mem_limit(id, config.config_.min_memory_, mem_limit);
      }
    }
  }

  if (OB_SUCC(ret)) {
    const double min_cpu = static_cast<double>(config.config_.min_cpu_);
    const double max_cpu = static_cast<double>(config.config_.max_cpu_);
    if (!omt_->has_tenant(id)) {
      if (OB_FAIL(omt_->add_tenant(id, min_cpu, max_cpu))) {
        if (ret != OB_TENANT_EXIST) {
          LOG_WARN("add tenant fail", K(id), K(min_cpu), K(max_cpu), K(ret));
        } else {
          LOG_WARN("tenant exist, need retry", K(id), K(min_cpu), K(max_cpu), K(ret));
        }
      } else {
        LOG_INFO("add new tenant", K(id), K(min_cpu), K(max_cpu));
      }
    } else {
      if (OB_FAIL(omt_->modify_tenant(id, min_cpu, max_cpu))) {
        LOG_WARN("modify tenant failed", K(id), K(min_cpu), K(max_cpu));
      }
    }
  }

  return ret;
}

void ObTenantNodeBalancer::periodically_check_tenant()
{
  int ret = OB_SUCCESS;
  TenantList tmp_tenants(0, nullptr, ObModIds::OMT);
  omt_->lock_tenant_list();
  TenantList& tenants = omt_->get_tenant_list();
  ObArray<ObLDHandle*> handles;
  ObArenaAllocator alloc("lock_diagnose");
  for (TenantList::iterator it = tenants.begin(); OB_SUCC(ret) && it != tenants.end(); it++) {
    void* ptr = nullptr;
    ObLDHandle* handle = nullptr;
    if (!OB_ISNULL(*it)) {
      if (OB_FAIL(tmp_tenants.push_back(*it))) {
        LOG_WARN("failed to push back tenant", K(ret));
      } else if (OB_ISNULL(ptr = alloc.alloc(sizeof(ObLDHandle)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else if (FALSE_IT(handle = new (ptr) ObLDHandle())) {
      } else if (OB_FAIL((*it)->rdlock(*handle))) {
        LOG_WARN("failed to rd lock tenant", K(ret));
      } else {
        ret = handles.push_back(handle);
      }
    }
  }
  omt_->unlock_tenant_list();

  int i = 0;
  for (TenantList::iterator it = tmp_tenants.begin(); OB_SUCC(ret) && it != tmp_tenants.end(); it++) {
    if (!OB_ISNULL(*it)) {
      (*it)->periodically_check();
      IGNORE_RETURN(*it)->unlock(*handles[i++]);
    }
  }
}

// Although unit has been deleted, the local cached unit cannot be deleted if the tenant still has partition in this
// server
int ObTenantNodeBalancer::fetch_effective_tenants(const TenantUnits& old_tenants, TenantUnits& new_tenants)
{
  int ret = OB_SUCCESS;
  bool found = false;
  bool is_exist = false;
  TenantUnits tenants;

  for (int64_t i = 0; OB_SUCC(ret) && i < old_tenants.count(); i++) {
    found = false;
    for (int64_t j = 0; j < new_tenants.count(); j++) {
      if (old_tenants.at(i).tenant_id_ == new_tenants.at(j).tenant_id_) {
        new_tenants.at(j).create_timestamp_ = old_tenants.at(i).create_timestamp_;
        new_tenants.at(j).is_removed_ = old_tenants.at(i).is_removed_;
        found = true;
        break;
      }
    }

    if (!found) {
      if (OB_FAIL(ObPartitionService::get_instance().check_tenant_pg_exist(old_tenants.at(i).tenant_id_, is_exist))) {
        LOG_WARN("failed to check tenant pg exist", K(ret), "tenant id", old_tenants.at(i).tenant_id_);
      } else if (is_exist) {
        // do nothing
      } else if (OB_FAIL(storage::ObTableMgr::get_instance().check_tenant_sstable_exist(
                     old_tenants.at(i).tenant_id_, is_exist))) {
        LOG_WARN("failed to check tenant sstable exist", K(ret));
      }

      if (OB_SUCC(ret)) {
        // remove local units after RECYCLE_LATENCY to avoid removing by mistake
        // but if marked removed, remove it directly without waiting
        if ((!old_tenants.at(i).is_removed_ &&
                ObTimeUtility::current_time() - old_tenants.at(i).create_timestamp_ < RECYCLE_LATENCY) ||
            is_exist) {
          if (OB_FAIL(tenants.push_back(old_tenants.at(i)))) {
            LOG_WARN("failed to push back tenant", K(ret));
          } else {
            LOG_INFO("old tenant can't be dropped",
                K(is_exist),
                "create timestamp",
                old_tenants.at(i).create_timestamp_,
                "tenant id",
                old_tenants.at(i).tenant_id_);
          }
        }
      }
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < tenants.count(); i++) {
    if (OB_FAIL(new_tenants.push_back(tenants.at(i)))) {
      LOG_WARN("failed to add new tenant", K(ret));
    }
  }

  return ret;
}

int ObTenantNodeBalancer::refresh_tenant(TenantUnits& units)
{
  int ret = OB_SUCCESS;
  TenantUnits local_units;

  {
    TCWLockGuard guard(lock_);
    if (OB_FAIL(storage::ObTenantConfigMgr::get_instance().get_tenant_units(local_units))) {
      LOG_WARN("failed to get tenant units");
    } else if (OB_FAIL(fetch_effective_tenants(local_units, units))) {
      LOG_WARN("failed to fetch effective tenants", K(local_units));
    } else if (OB_FAIL(storage::ObTenantConfigMgr::get_instance().write_tenant_units(units))) {
      LOG_WARN("failed to write tenant units", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(update_tenant(units, false /*is_local*/))) {
      LOG_WARN("failed to update tenant units", K(ret), K(local_units), K(units));
    } else {
      LOG_INFO("succ to update tenant", K(local_units), K(units));
    }
  }
  return ret;
}

int ObTenantNodeBalancer::create_new_tenant(ObUnitInfoGetter::ObTenantConfig& unit)
{
  int ret = OB_SUCCESS;
  TenantUnits units;
  TCWLockGuard guard(lock_);

  if (OB_FAIL(storage::ObTenantConfigMgr::get_instance().get_tenant_units(units))) {
    LOG_WARN("failed to get tenant units", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < units.count(); i++) {
      if (units.at(i).tenant_id_ == unit.tenant_id_ && OB_SYS_TENANT_ID != unit.tenant_id_) {
        ret = OB_TENANT_EXIST;
        LOG_WARN("tenant has exist", K(ret), K(unit));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(units.push_back(unit))) {
        LOG_WARN("failed to push back unit", K(ret));
      } else if (OB_FAIL(storage::ObTenantConfigMgr::get_instance().write_tenant_units(units))) {
        LOG_WARN("failed to write tenant units", K(ret));
      } else if (OB_FAIL(check_new_tenant(unit))) {
        LOG_WARN("failed to update tenant units", K(ret));
      } else {
        LOG_INFO("succ to create new tenant", K(unit), K(units));
      }
    }
  }
  return ret;
}

int ObTenantNodeBalancer::update_tenant_memory(const obrpc::ObTenantMemoryArg& tenant_memory)
{
  int ret = OB_SUCCESS;
  const int64_t tenant_id = tenant_memory.tenant_id_;
  const int64_t memory_size = tenant_memory.memory_size_;
  const int64_t refresh_interval = tenant_memory.refresh_interval_;
  TenantUnits units;
  TCWLockGuard guard(lock_);

  if (!tenant_memory.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_memory));
  } else if (OB_FAIL(storage::ObTenantConfigMgr::get_instance().get_tenant_units(units))) {
    LOG_WARN("failed to get tenant units", K(ret), K(tenant_memory), K(units));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < units.count(); i++) {
      if (units.at(i).tenant_id_ == tenant_id) {
        units.at(i).config_.max_memory_ = memory_size;
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(update_tenant(units, true /*is_local*/))) {
        LOG_WARN("failed to update tenant units", K(ret), K(tenant_memory), K(units));
      } else {
        refresh_interval_ = refresh_interval * 1000L * 1000L;
        LOG_INFO("succ to admin update tenant", K(tenant_id), K(memory_size), K(units));
      }
    }
  }

  return ret;
}
