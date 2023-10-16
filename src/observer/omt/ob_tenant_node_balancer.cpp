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
#include "storage/blocksstable/ob_block_manager.h"
#include "logservice/palf/palf_options.h"
#include "logservice/ob_server_log_block_mgr.h"
#include "storage/tx_storage/ob_tenant_freezer.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/slog_ckpt/ob_server_checkpoint_slog_handler.h"
#include "observer/ob_server_event_history_table_operator.h"
#ifdef OB_BUILD_TDE_SECURITY
#include "share/ob_master_key_getter.h"
#endif

using namespace oceanbase::obsys;
using namespace oceanbase::lib;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::omt;
using namespace oceanbase::observer;
using namespace oceanbase::storage;

ObTenantNodeBalancer::ObTenantNodeBalancer()
    : omt_(NULL), myaddr_(), unit_getter_(), lock_(common::ObLatchIds::CONFIG_LOCK),
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

int ObTenantNodeBalancer::init(ObMultiTenant *omt, common::ObMySQLProxy &sql_proxy,
    const common::ObAddr &myaddr)
{
  int ret = OB_SUCCESS;
  myaddr_ = myaddr;
  if (OB_FAIL(unit_getter_.init(sql_proxy, &GCONF))) {
    LOG_ERROR("init unit getter fail", K(ret));
  } else {
    omt_= omt;
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
    int64_t sys_unit_cnt = 0;
    ObCurTraceId::init(GCONF.self_addr_);
    if (!ObServerCheckpointSlogHandler::get_instance().is_started()) {
      // do nothing if not finish replaying slog
      LOG_INFO("server slog not finish replaying, need wait");
      ret = OB_NEED_RETRY;
    } else if (OB_FAIL(unit_getter_.get_sys_unit_count(sys_unit_cnt))) {
      LOG_WARN("get sys unit count fail", KR(ret));
    } else if (sys_unit_cnt <= 0) {
      // check wether sys tenant has been created, do nothing if sys tenant has not been created
      LOG_INFO("sys tenant has not been created, tenant node balancer can not run, need wait",
          K(sys_unit_cnt));
      ret = OB_NEED_RETRY;
    } else if (OB_FAIL(unit_getter_.get_server_tenant_configs(myaddr_, units))) {
      LOG_WARN("get server tenant units fail", K(myaddr_), K(ret));
    } else if (OB_FAIL(refresh_tenant(units))) {
      LOG_WARN("failed to refresh tenant", K(ret), K(units));
    } else if (FALSE_IT(periodically_check_tenant())) {
      // never reach here
    }

    FLOG_INFO("refresh tenant units", K(sys_unit_cnt), K(units), KR(ret));

    // will try to update tma whether tenant unit is changed or not,
    // because memstore_limit_percentage may be changed
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = TMA_MGR_INSTANCE.update_tenant_mem_limit(units))) {
      LOG_WARN("TMA_MGR_INSTANCE.update_tenant_mem_limit failed", K(tmp_ret));
    }

    // check whether tenant unit is changed, try to update unit config of tenant
    ObSEArray<uint64_t, 10> tenants;
    if (!ObServerCheckpointSlogHandler::get_instance().is_started()) {
      // do nothing if not finish replaying slog
      LOG_INFO("server slog not finish replaying, need wait");
      ret = OB_NEED_RETRY;
    } else if (OB_FAIL(unit_getter_.get_tenants(tenants))) {
      LOG_WARN("get cluster tenants fail", K(ret));
    } else if (OB_FAIL(OTC_MGR.refresh_tenants(tenants))) {
      LOG_WARN("fail refresh tenant config", K(tenants), K(ret));
    }
    if (OB_SUCCESS != (tmp_ret = GCTX.log_block_mgr_->try_resize())) {
      LOG_WARN("ObServerLogBlockMgr try_resize failed", K(tmp_ret));
    }

    FLOG_INFO("refresh tenant config", K(tenants), K(ret));


    USLEEP(refresh_interval_);  // sleep 10s
  }
}

int ObTenantNodeBalancer::notify_create_tenant(const obrpc::TenantServerUnitConfig &unit)
{
  LOG_INFO("succ to receive notify of creating tenant", K(unit));
  int ret = OB_SUCCESS;
  bool is_hidden_sys = false;
  bool unit_id_exist = false;

  if (!unit.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(unit));
  } else if (!ObServerCheckpointSlogHandler::get_instance().is_started()) {
    ret = OB_SERVER_IS_INIT;
    LOG_WARN("slog replay not finish", KR(ret),K(unit));
  } else if (is_meta_tenant(unit.tenant_id_)) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("can not create meta tenant", K(ret), K(unit));
  } else if (OB_FAIL(omt_->check_if_hidden_sys(unit.tenant_id_, is_hidden_sys))) {
    LOG_WARN("fail to check_if_hidden_sys", KR(ret), K(unit));
  } else if (omt_->has_tenant(unit.tenant_id_) && !is_hidden_sys) {
    ret = OB_TENANT_EXIST;
    LOG_WARN("tenant has exist", KR(ret), K(unit));
  } else if (is_user_tenant(unit.tenant_id_) && omt_->has_tenant(gen_meta_tenant_id(unit.tenant_id_))) {
    ret = OB_TENANT_EXIST;
    LOG_WARN("meta tenant has exist", KR(ret), K(unit));
   // TODO(fenggu.yh) 临时注释，防止创建租户失败
  //} else if (OB_FAIL(omt_->check_if_unit_id_exist(unit.unit_id_, unit_id_exist))) {
  //  LOG_WARN("fail to check_if_unit_id_exist", KR(ret), K(unit));
  } else if (unit_id_exist) { // the unit may be wait_gc status
    ret = OB_ENTRY_EXIST;
    LOG_WARN("unit_id exist", KR(ret), K(unit));
  } else {
    const uint64_t tenant_id = unit.tenant_id_;
    ObUnitInfoGetter::ObTenantConfig basic_tenant_unit;
    ObUnitInfoGetter::ObTenantConfig meta_tenant_unit;
    const bool has_memstore = (unit.replica_type_ != REPLICA_TYPE_LOGONLY);
    const int64_t create_timestamp = ObTimeUtility::current_time();
    basic_tenant_unit.unit_status_ = ObUnitInfoGetter::ObUnitStatus::UNIT_NORMAL;
    const int64_t create_tenant_timeout_ts = THIS_WORKER.get_timeout_ts();

    if (create_tenant_timeout_ts < create_timestamp) {
      ret = OB_TIMEOUT;
      LOG_WARN("notify_create_tenant has timeout", K(ret), K(create_timestamp), K(create_tenant_timeout_ts));
    } else if (OB_FAIL(basic_tenant_unit.init(tenant_id,
                                       unit.unit_id_,
                                       ObUnitInfoGetter::ObUnitStatus::UNIT_NORMAL,
                                       unit.unit_config_,
                                       unit.compat_mode_,
                                       create_timestamp,
                                       has_memstore,
                                       false /*is_removed*/))) {
      LOG_WARN("fail to init user tenant config", KR(ret), K(unit));
    } else if (is_user_tenant(tenant_id)
        && OB_FAIL(basic_tenant_unit.divide_meta_tenant(meta_tenant_unit))) {
      LOG_WARN("divide meta tenant failed", KR(ret), K(unit), K(basic_tenant_unit));
    } else if (OB_FAIL(check_new_tenant(basic_tenant_unit, create_tenant_timeout_ts))) {
      LOG_WARN("failed to create new tenant", KR(ret), K(basic_tenant_unit), K(create_tenant_timeout_ts));
    } else {
      ret = OB_SUCCESS;
      LOG_INFO("succ to create new user tenant", KR(ret), K(unit), K(basic_tenant_unit), K(create_tenant_timeout_ts));
    }
#ifdef OB_BUILD_TDE_SECURITY
    if (OB_SUCC(ret) && is_user_tenant(tenant_id)) {
      ObRootKey root_key;
      if (OB_FAIL(ObMasterKeyGetter::instance().get_root_key(tenant_id, root_key, true))) {
        LOG_WARN("failed to get root key", K(ret));
      }
    }
#endif
    // create meta tenant
    if (OB_SUCC(ret) && is_user_tenant(tenant_id)) {
      if (OB_FAIL(check_new_tenant(meta_tenant_unit, create_tenant_timeout_ts))) {
        LOG_WARN("failed to create meta tenant", KR(ret), K(meta_tenant_unit), K(create_tenant_timeout_ts));
      } else {
        ret = OB_SUCCESS;
        LOG_INFO("succ to create meta tenant", KR(ret), K(meta_tenant_unit), K(create_tenant_timeout_ts));
      }
    }
  }

  // In standby cluster, may repeat create tenant, if if_not_grant_ is true, ignore OB_TENANT_EXIST
  if (OB_TENANT_EXIST == ret && unit.if_not_grant_) {
    if (GCTX.is_standby_cluster()) {
      ret = OB_SUCCESS;
    }
  }

  return ret;
}

// 标记删除，而不是直接删，是因为并发时，另一个线程可能刷到tenant了，但是还没有refresh tenant，
// 此时drop tenant将tenant删除了，另一个线程过一会refresh tenant时，又给加回来了
// 所以这里只做标记，删除tenant统一在refresh tenant里做
int ObTenantNodeBalancer::try_notify_drop_tenant(const int64_t tenant_id)
{
  LOG_INFO("[DELETE_TENANT] succ to receive notify of dropping tenant", K(tenant_id));
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  TCWLockGuard guard(lock_);
  uint64_t meta_tenant_id = OB_INVALID_TENANT_ID;
  if (OB_UNLIKELY(is_meta_tenant(tenant_id))) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("meta tenant is not allowed", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(omt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("omt_ is null", KR(ret),KP(omt_));
  } else {
    if (OB_TMP_FAIL(omt_->mark_del_tenant(tenant_id))) {
      LOG_WARN("fail to mark del user_tenant", KR(ret), KR(tmp_ret), K(tenant_id));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
    meta_tenant_id = gen_meta_tenant_id(tenant_id);
    if (OB_TMP_FAIL(omt_->mark_del_tenant(meta_tenant_id))) {
      LOG_WARN("fail to mark del meta_tenant", KR(ret), KR(tmp_ret), K(meta_tenant_id));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }
  LOG_INFO("[DELETE_TENANT] mark drop tenant", KR(ret), K(tenant_id), K(meta_tenant_id));
  return ret;
}

int ObTenantNodeBalancer::get_server_allocated_resource(ServerResource &server_resource)
{
  int ret = OB_SUCCESS;
  server_resource.reset();
  TenantUnits tenant_units;

  if (OB_FAIL(omt_->get_tenant_units(tenant_units, false))) {
    LOG_WARN("failed to get tenant units");
  } else {
    for (int64_t i = 0; i < tenant_units.count(); i++) {
      // META tenant and USER tenant share CPU resource, so skip META tenant
      if (! is_meta_tenant(tenant_units.at(i).tenant_id_)) {
        server_resource.max_cpu_ += tenant_units.at(i).config_.max_cpu();
        server_resource.min_cpu_ += tenant_units.at(i).config_.min_cpu();
      }
      int64_t extra_memory = is_sys_tenant(tenant_units.at(i).tenant_id_) ? GMEMCONF.get_extra_memory() : 0;
      server_resource.memory_size_ += max(ObMallocAllocator::get_instance()->get_tenant_limit(tenant_units.at(i).tenant_id_) - extra_memory,
                                          tenant_units.at(i).config_.memory_size());
      server_resource.log_disk_size_ += tenant_units.at(i).config_.log_disk_size();
    }
  }
  return ret;
}


int ObTenantNodeBalancer::lock_tenant_balancer()
{
  return lock_.rdlock();
}

int ObTenantNodeBalancer::unlock_tenant_balancer()
{
  return lock_.unlock();
}

int ObTenantNodeBalancer::check_del_tenants(const TenantUnits &local_units, TenantUnits &units)
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; i < local_units.count(); i++) {
    bool tenant_exists = false;
    const ObUnitInfoGetter::ObTenantConfig &local_unit = local_units.at(i);
    for (auto punit = units.begin(); punit != units.end(); punit++) {
      if (local_unit.tenant_id_ == punit->tenant_id_) {
        tenant_exists = true;
        break;
      }
    }
    if (!tenant_exists) {
      LOG_INFO("[DELETE_TENANT] begin to delete tenant", K(local_unit));
      if (OB_SYS_TENANT_ID == local_unit.tenant_id_) {
        LOG_INFO("[DELETE_TENANT] need convert_real_to_hidden_sys_tenant");
        if (OB_FAIL(omt_->convert_real_to_hidden_sys_tenant())) {
          LOG_INFO("fail to convert_real_to_hidden_sys_tenant", K(ret));
        }
      } else if (OB_FAIL(omt_->del_tenant(local_unit.tenant_id_))) {
        LOG_WARN("delete tenant fail", K(local_unit), K(ret));
      }
    }
  }

  return ret;
}

int ObTenantNodeBalancer::check_new_tenants(TenantUnits &units)
{
  int ret = OB_SUCCESS;

  DEBUG_SYNC(CHECK_NEW_TENANT);

  // check all units of tenants.
  for (TenantUnits::iterator it = units.begin(); it != units.end(); it++) {
    if (OB_FAIL(check_new_tenant(*it))) {
      LOG_WARN("failed to check new tenant", K(ret));
    }
  }

  return ret;
}

int ObTenantNodeBalancer::check_new_tenant(const ObUnitInfoGetter::ObTenantConfig &unit, const int64_t abs_timeout_us)
{
  int ret = OB_SUCCESS;

  const int64_t tenant_id = unit.tenant_id_;
  ObTenant *tenant = nullptr;

  if (OB_FAIL(omt_->get_tenant(tenant_id, tenant))) {
    if (is_sys_tenant(tenant_id)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("real or hidden sys tenant must be exist", K(ret));
    } else {
      ret = OB_SUCCESS;
      ObTenantMeta tenant_meta;
      ObTenantSuperBlock super_block(tenant_id, false /*is_hidden*/);  // empty super block
      if (OB_FAIL(tenant_meta.build(unit, super_block))) {
        LOG_WARN("fail to build tenant meta", K(ret));
      } else if (OB_FAIL(omt_->create_tenant(tenant_meta, true /* write_slog */, abs_timeout_us))) {
        LOG_WARN("fail to create new tenant", K(ret), K(tenant_id));
      }
    }
  } else {
    int64_t extra_memory = 0;
    if (is_sys_tenant(tenant_id)) {
      if (tenant->is_hidden() && OB_FAIL(omt_->convert_hidden_to_real_sys_tenant(unit, abs_timeout_us))) {
        LOG_WARN("fail to create real sys tenant", K(unit));
      }
      extra_memory = GMEMCONF.get_extra_memory();
    }
    if (OB_SUCC(ret) && !(unit == tenant->get_unit())) {
      if (OB_FAIL(omt_->update_tenant_unit(unit))) {
        LOG_WARN("fail to update tenant unit", K(ret), K(tenant_id));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(omt_->update_tenant_memory(unit, extra_memory))) {
      LOG_ERROR("fail to update tenant memory", K(ret), K(tenant_id));
    }
  }
  if (OB_SUCC(ret) && !is_virtual_tenant_id(tenant_id)) {
    if (OB_FAIL(omt_->modify_tenant_io(tenant_id, unit.config_))) {
      LOG_WARN("modify tenant io config failed", K(ret), K(tenant_id), K(unit.config_));
    }
  }
  return ret;
}

int ObTenantNodeBalancer::refresh_hidden_sys_memory()
{
  int ret = OB_SUCCESS;
  int64_t allowed_mem_limit = 0;
  ObTenant *tenant = nullptr;
  if (OB_FAIL(omt_->get_tenant(OB_SYS_TENANT_ID, tenant))) {
    LOG_WARN("get sys tenant failed", K(ret));
  } else if (OB_ISNULL(tenant) || !tenant->is_hidden()) {
    // do nothing
  } else if (OB_FAIL(omt_->update_tenant_memory(OB_SYS_TENANT_ID, GMEMCONF.get_hidden_sys_memory(), allowed_mem_limit))) {
    LOG_WARN("update hidden sys tenant memory failed", K(ret));
  } else {
    LOG_INFO("update hidden sys tenant memory succeed ", K(allowed_mem_limit));
  }
  return ret;
}

void ObTenantNodeBalancer::periodically_check_tenant()
{
  int ret = OB_SUCCESS;
  struct TenantHandlePair {
    ObTenant *tenant_;
    ObLDHandle *handle_;
    TO_STRING_KV(KP(tenant_));
  };
  ObSEArray<TenantHandlePair, 32> pairs;
  omt_->lock_tenant_list();
  TenantList &tenants = omt_->get_tenant_list();
  ObArenaAllocator alloc("lock_diagnose");
  for (TenantList::iterator it = tenants.begin();
       it != tenants.end();
       it++) {
    void *ptr = nullptr;
    ObLDHandle *handle = nullptr;
    if (!OB_ISNULL(*it) && !(*it)->has_stopped()) {
      if (OB_ISNULL(ptr = alloc.alloc(sizeof(ObLDHandle)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc", K(ret));
      } else if (FALSE_IT(handle = new (ptr) ObLDHandle())) {
      } else if (OB_FAIL((*it)->rdlock(*handle))) {
        LOG_WARN("failed to rd lock tenant", K(ret));
      } else {
        TenantHandlePair pair;
        pair.tenant_ = *it;
        pair.handle_ = handle;
        if (OB_FAIL(pairs.push_back(pair))) {
          LOG_WARN("failed to push back tenant", K(ret));
        } else {/*do-nothing*/}
        // cleanup
        if (OB_FAIL(ret)) {
          IGNORE_RETURN (*it)->unlock(*handle);
        }
      }
    }
  }
  omt_->unlock_tenant_list();

  int i = 0;
  for (auto it = pairs.begin();
       it != pairs.end();
       it++) {
    (*it).tenant_->periodically_check();
    IGNORE_RETURN (*it).tenant_->unlock(*(*it).handle_);
  }
}

// Although unit has been deleted, the local cached unit cannot be deleted if the tenant still holds resource
int ObTenantNodeBalancer::fetch_effective_tenants(const TenantUnits &old_tenants, TenantUnits &new_tenants)
{
  int ret = OB_SUCCESS;
  bool found = false;
  bool is_released = false;
  TenantUnits tenants;

  for (int64_t i = 0; OB_SUCC(ret) && i < old_tenants.count(); i++) {
    found = false;
    const ObUnitInfoGetter::ObTenantConfig &tenant_config = old_tenants.at(i);
    const ObUnitInfoGetter::ObUnitStatus local_unit_status = tenant_config.unit_status_;
    for (int64_t j = 0; j < new_tenants.count(); j++) {
      if (tenant_config.tenant_id_ == new_tenants.at(j).tenant_id_) {
        new_tenants.at(j).create_timestamp_ = tenant_config.create_timestamp_;
        new_tenants.at(j).is_removed_ = tenant_config.is_removed_;
        found = true;
        break;
      }
    }

    if (!found) {
      ObTenant *tenant = nullptr;
      MTL_SWITCH(tenant_config.tenant_id_) {
        if (OB_FAIL(MTL(ObTenantMetaMemMgr*)->check_all_meta_mem_released(is_released, "[DELETE_TENANT]"))) {
          LOG_WARN("fail to check_all_meta_mem_released", K(ret), K(tenant_config));
        } else if (!is_released) {
          // can not release now. dump some debug info
          const uint64_t interval = 180 * 1000 * 1000; // 180s
          if (!is_released && REACH_TIME_INTERVAL(interval)) {
            MTL(ObTenantMetaMemMgr*)->dump_tablet_info();
            MTL(ObLSService *)->dump_ls_info();
            PRINT_OBJ_LEAK(MTL_ID(), share::LEAK_CHECK_OBJ_MAX_NUM);
          }
        } else {
          // check ls service safe to destroy.
          is_released = MTL(ObLSService *)->safe_to_destroy();
        }
      }

      if (OB_SUCC(ret)) {
        // remove local units after RECYCLE_LATENCY to avoid removing by mistake
        // but if marked removed, remove it directly without waiting
        const int64_t now_time = ObTimeUtility::current_time();
        const int64_t life_time = now_time - tenant_config.create_timestamp_;
        if ((!tenant_config.is_removed_ && life_time < RECYCLE_LATENCY) || !is_released) {
          if (OB_FAIL(tenants.push_back(tenant_config))) {
            LOG_WARN("failed to push back tenant", KR(ret));
          } else {
            // update tenant unit status which need be deleted
            // need wait gc in observer
            // NOTE: only update unit status when can not release resource
            if (!is_released) {
              tenants.at(tenants.count() - 1).unit_status_ = ObUnitInfoGetter::UNIT_WAIT_GC_IN_OBSERVER;
              // add a event when try to gc for the first time
              if (local_unit_status != ObUnitInfoGetter::ObUnitStatus::UNIT_WAIT_GC_IN_OBSERVER &&
                  local_unit_status != ObUnitInfoGetter::ObUnitStatus::UNIT_DELETING_IN_OBSERVER) {
                SERVER_EVENT_ADD("unit", "start unit gc", "tenant_id", tenant_config.tenant_id_,
                    "unit_id", tenant_config.unit_id_, "unit_status", "WAIT GC");
              }
            }

            LOG_INFO("[DELETE_TENANT] tenant has been dropped. can not delete tenant",
                K(is_released), "local_unit_status", ObUnitInfoGetter::get_unit_status_str(local_unit_status),
                "is_removed", tenant_config.is_removed_,
                "create_timestamp", tenant_config.create_timestamp_,
                K(life_time), K(tenant_config));
          }
        } else {
            LOG_INFO("[DELETE_TENANT] tenant has been dropped. can delete tenant",
                K(is_released), "local_unit_status", ObUnitInfoGetter::get_unit_status_str(local_unit_status),
                "is_removed", tenant_config.is_removed_,
                "create_timestamp", tenant_config.create_timestamp_,
                K(life_time), K(tenant_config));
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

int ObTenantNodeBalancer::refresh_tenant(TenantUnits &units)
{
  int ret = OB_SUCCESS;

  TenantUnits local_units;
  if (OB_FAIL(omt_->get_tenant_units(local_units, false))) {
    LOG_WARN("failed to get local tenant units");
  } else if (OB_FAIL(fetch_effective_tenants(local_units, units))) {
    LOG_WARN("failed to fetch effective tenants", K(local_units));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(check_new_tenants(units))) {
      LOG_WARN("check and add new tenant fail", K(ret));
    } else if (FALSE_IT(omt_->set_synced())) {
    } else if (OB_FAIL(check_del_tenants(local_units, units))) {
      LOG_WARN("check delete tenant fail", K(ret));
    } else if (OB_FAIL(refresh_hidden_sys_memory())) {
      LOG_WARN("refresh hidden sys memory failed", K(ret));
    }
  }

  return ret;
}

int ObTenantNodeBalancer::update_tenant_memory(const obrpc::ObTenantMemoryArg &tenant_memory)
{
  int ret = OB_SUCCESS;
  const int64_t tenant_id = tenant_memory.tenant_id_;
  const int64_t memory_size = tenant_memory.memory_size_;
  const int64_t refresh_interval = tenant_memory.refresh_interval_;

  ObUnitInfoGetter::ObTenantConfig unit;
  int64_t allowed_mem_limit = 0;

  TCWLockGuard guard(lock_);

  if (!tenant_memory.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_memory));
  } else if (OB_FAIL(omt_->get_tenant_unit(tenant_id, unit))) {
    LOG_WARN("failed to get tenant config", K(ret), K(tenant_id));
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(omt_->update_tenant_memory(tenant_id, memory_size, allowed_mem_limit))) {
    LOG_WARN("failed to update tenant memory", K(ret), K(tenant_id), K(memory_size));
  } else if (OB_FAIL(omt_->update_tenant_freezer_mem_limit(tenant_id, unit.config_.memory_size(), allowed_mem_limit))) {
    LOG_WARN("set_tenant_freezer_mem_limit failed", K(ret), K(tenant_id));
  } else {
    refresh_interval_ = refresh_interval * 1000L * 1000L;
    LOG_INFO("succ to admin update tenant memory", K(tenant_id), K(memory_size));
  }

  return ret;
}
