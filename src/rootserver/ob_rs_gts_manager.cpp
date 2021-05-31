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

#define USING_LOG_PREFIX RS

#include "ob_rs_gts_manager.h"
#include "lib/allocator/ob_mod_define.h"
#include "ob_zone_manager.h"
#include "ob_unit_manager.h"
#include "ob_server_manager.h"
#include "share/ob_max_id_fetcher.h"

namespace oceanbase {
using namespace common;
using namespace share;

namespace rootserver {

void RsGtsInstance::reset()
{
  gts_id_ = OB_INVALID_ID;
  gts_name_.reset();
  region_.reset();
  tenant_id_array_.reset();
}

int RsGtsInstance::assign(const RsGtsInstance& other)
{
  int ret = OB_SUCCESS;
  reset();
  gts_id_ = other.gts_id_;
  gts_name_ = other.gts_name_;
  region_ = other.region_;
  if (OB_FAIL(tenant_id_array_.assign(other.tenant_id_array_))) {
    LOG_WARN("fail to assign tenant id array", K(ret));
  }
  return ret;
}

int RsGtsInstance::insert_tenant(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (has_exist_in_array(tenant_id_array_, tenant_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant_id exist in array", K(ret), K(tenant_id), "instance", *this);
  } else if (OB_FAIL(tenant_id_array_.push_back(tenant_id))) {
    LOG_WARN("fail to push back", K(ret));
  }
  return ret;
}

int RsGtsInstance::try_remove_tenant(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    bool find = false;
    int64_t index = 0;
    for (/*nop*/; !find && index < tenant_id_array_.count(); ++index) {
      if (tenant_id == tenant_id_array_.at(index)) {
        find = true;
        break;
      } else {
        // go on check next
      }
    }
    if (!find) {
      // print a warn log only
      LOG_WARN("tenant id not found", K(ret), "instance", *this);
    } else if (index >= tenant_id_array_.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("index unexpected", K(ret), "array_count", tenant_id_array_.count(), "instance", *this);
    } else if (OB_FAIL(tenant_id_array_.remove(index))) {
      LOG_WARN("fail to remove from tenant", K(ret), K(index), "instance", *this);
    }
  }
  return ret;
}

ObRsGtsManager::ObRsGtsManager()
    : lock_(),
      zone_mgr_(nullptr),
      unit_mgr_(nullptr),
      server_mgr_(nullptr),
      sql_proxy_(nullptr),
      gts_instance_allocator_(OB_MALLOC_NORMAL_BLOCK_SIZE, ObMalloc(ObModIds::OB_RS_GTS_MANAGER)),
      id_array_allocator_(OB_MALLOC_NORMAL_BLOCK_SIZE, ObMalloc(ObModIds::OB_RS_GTS_MANAGER)),
      gts_table_operator_(),
      gts_instance_map_(),
      region_gts_map_(),
      tenant_gts_map_(),
      inited_(false),
      loaded_(false)
{}

ObRsGtsManager::~ObRsGtsManager()
{}

int ObRsGtsManager::check_inner_stat()
{
  int ret = OB_SUCCESS;
  if (!inited_ || !loaded_) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner_stat_error", K(ret), K(inited_), K(loaded_));
  }
  return ret;
}

int ObRsGtsManager::init(common::ObMySQLProxy* sql_proxy, rootserver::ObZoneManager* zone_mgr,
    rootserver::ObUnitManager* unit_mgr, rootserver::ObServerManager* server_mgr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(nullptr == sql_proxy || nullptr == zone_mgr || nullptr == unit_mgr || nullptr == server_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, null ptr", K(ret), KP(sql_proxy), KP(zone_mgr), KP(unit_mgr));
  } else if (OB_FAIL(gts_table_operator_.init(sql_proxy))) {
    LOG_WARN("fail to init gts table operator", K(ret));
  } else if (OB_FAIL(gts_instance_map_.create(MAX_GTS_INSTANCE_CNT, ObModIds::OB_RS_GTS_MANAGER))) {
    LOG_WARN("fail to create gts instance map", K(ret));
  } else if (OB_FAIL(region_gts_map_.create(MAX_ZONE_NUM * 2, ObModIds::OB_RS_GTS_MANAGER))) {
    LOG_WARN("fail to create region gts map", K(ret));
  } else if (OB_FAIL(tenant_gts_map_.create(MAX_TENANT_CNT, ObModIds::OB_RS_GTS_MANAGER))) {
    LOG_WARN("fail to create tenant gts map", K(ret));
  } else {
    sql_proxy_ = sql_proxy;
    zone_mgr_ = zone_mgr;
    unit_mgr_ = unit_mgr;
    server_mgr_ = server_mgr;
    loaded_ = false;
    inited_ = true;
  }
  return ret;
}

int ObRsGtsManager::load()
{
  int ret = OB_SUCCESS;
  SpinWLockGuard lock_guard(lock_);
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRsGtsManager not init", K(ret));
  } else {
    loaded_ = false;
    tenant_gts_map_.reuse();
    for (auto iter = gts_instance_map_.begin(); iter != gts_instance_map_.end(); ++iter) {
      RsGtsInstance* gts_instance = iter->second;
      if (nullptr != gts_instance) {
        gts_instance->~RsGtsInstance();  // destruct here
        gts_instance = nullptr;
      }
    }
    gts_instance_allocator_.reset();  // free memory here
    gts_instance_map_.reuse();
    for (auto iter = region_gts_map_.begin(); iter != region_gts_map_.end(); ++iter) {
      common::ObSEArray<uint64_t, 7>* this_gts_id_array = iter->second;
      if (nullptr != this_gts_id_array) {
        this_gts_id_array->reset();  // reset help to free memory
        this_gts_id_array = nullptr;
      }
    }
    id_array_allocator_.reset();  // free memory here
    region_gts_map_.reuse();
    common::ObArray<common::ObGtsInfo> gts_info_array;
    common::ObArray<common::ObGtsTenantInfo> tenant_gts_info_array;
    if (OB_FAIL(gts_table_operator_.get_gts_infos(gts_info_array))) {
      LOG_WARN("fail to get gts infos", K(ret));
    } else if (OB_FAIL(gts_table_operator_.get_gts_tenant_infos(tenant_gts_info_array))) {
      LOG_WARN("fail to get tenant gts info", K(ret));
    } else if (OB_FAIL(load_gts_instance(gts_info_array))) {
      LOG_WARN("fail to load gts instance", K(ret));
    } else if (OB_FAIL(load_tenant_gts_info(tenant_gts_info_array))) {
      LOG_WARN("fail to load tenant gts info", K(ret));
    } else {
      loaded_ = true;
    }
  }
  return ret;
}

int ObRsGtsManager::inner_create_gts_instance(
    const uint64_t gts_id, const common::ObGtsName& gts_name, const common::ObRegion& region)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", K(ret));
  } else if (OB_INVALID_ID == gts_id || gts_name.is_empty() || region.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(gts_id), K(gts_name), K(region));
  } else {
    RsGtsInstance* gts_instance = gts_instance_allocator_.alloc();
    if (OB_UNLIKELY(nullptr == gts_instance)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", K(ret));
    } else {
      gts_instance->set_gts_id(gts_id);
      gts_instance->set_gts_name(gts_name);
      gts_instance->set_region(region);
      if (OB_FAIL(gts_instance_map_.set_refactored(gts_instance->get_gts_id(), gts_instance))) {
        LOG_WARN("fail to set refactored", K(ret));
      }
    }
    if (nullptr != gts_instance && OB_FAIL(ret)) {
      // the destuctor is invoked inside the allocator
      gts_instance_allocator_.free(gts_instance);
      gts_instance = nullptr;
    }
    if (OB_SUCC(ret)) {
      common::ObSEArray<uint64_t, 7>* gts_id_array = nullptr;
      int tmp_ret = region_gts_map_.get_refactored(gts_instance->get_region(), gts_id_array);
      if (OB_HASH_NOT_EXIST == tmp_ret) {
        const int32_t write_flag = 0;
        gts_id_array = id_array_allocator_.alloc();
        if (OB_UNLIKELY(nullptr == gts_id_array)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc memory", K(ret));
        } else if (OB_FAIL(gts_id_array->push_back(gts_instance->get_gts_id()))) {
          LOG_WARN("fail to push back", K(ret));
        } else if (OB_FAIL(region_gts_map_.set_refactored(gts_instance->get_region(), gts_id_array, write_flag))) {
          LOG_WARN("fail to set refactored", K(ret));
        }
        if (OB_FAIL(ret) && nullptr != gts_id_array) {
          // the destuctor is invoked inside the allocator
          id_array_allocator_.free(gts_id_array);
          gts_id_array = nullptr;
        }
      } else if (OB_SUCCESS == tmp_ret) {
        if (OB_UNLIKELY(nullptr == gts_id_array)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("gts id array is nullptr", K(ret));
        } else if (OB_FAIL(gts_id_array->push_back(gts_instance->get_gts_id()))) {
          LOG_WARN("fail to push back", K(ret));
        }
      } else {
        ret = tmp_ret;
        LOG_WARN("fail to get gts id array from map", K(ret));
      }
    }
  }
  return ret;
}

int ObRsGtsManager::create_gts_instance(const uint64_t gts_id, const common::ObGtsName& gts_name,
    const common::ObRegion& region, const common::ObMemberList& member_list, const common::ObAddr& standby)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard lock_guard(lock_);
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", K(ret));
  } else if (OB_INVALID_ID == gts_id || gts_name.is_empty() || region.is_empty() || !member_list.is_valid()) {
    // ignore for standby check
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(gts_id), K(gts_name), K(region), K(member_list));
  } else {
    common::ObGtsInfo gts_info;
    gts_info.gts_id_ = gts_id;
    gts_info.gts_name_ = gts_name;
    gts_info.region_ = region;
    gts_info.member_list_ = member_list;
    gts_info.standby_ = standby;
    gts_info.epoch_id_ = ObTimeUtility::current_time();
    gts_info.heartbeat_ts_ = ObTimeUtility::current_time();
    if (OB_FAIL(gts_table_operator_.insert_gts_instance(gts_info))) {
      LOG_WARN("fail to insert gts instance", K(ret));
    } else if (OB_FAIL(inner_create_gts_instance(gts_id, gts_name, region))) {
      LOG_WARN("fail to inner create gts instance", K(ret));
    }
  }
  return ret;
}

int ObRsGtsManager::inner_alloc_gts_instance_for_tenant(const uint64_t tenant_id, const uint64_t gts_id)
{
  int ret = OB_SUCCESS;
  RsGtsInstance* gts_instance = nullptr;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", K(ret));
  } else if (OB_FAIL(gts_instance_map_.get_refactored(gts_id, gts_instance))) {
    LOG_WARN("fail to get refactored", K(ret), K(gts_id));
  } else if (OB_UNLIKELY(nullptr == gts_instance)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("gts instance ptr is null", K(ret));
  } else if (OB_FAIL(gts_instance->insert_tenant(tenant_id))) {
    LOG_WARN("fail to insert tenant", K(ret));
  } else {
    ret = tenant_gts_map_.set_refactored(tenant_id, gts_id);
    if (OB_SUCC(ret)) {
      // good
    } else {
      // rollback insert tenant operation
      if (OB_SUCCESS != gts_instance->try_remove_tenant(tenant_id)) {
        LOG_ERROR("remove tenant failed", K(ret), K(tenant_id));
      }
    }
  }
  return ret;
}

int ObRsGtsManager::inner_erase_tenant_gts(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else {
    if (OB_SUCCESS != tenant_gts_map_.erase_refactored(tenant_id)) {
      LOG_WARN("fail to erase from tenant gts map", K(tenant_id));
    }
    // ignore the ret code
    for (auto iter = gts_instance_map_.begin(); iter != gts_instance_map_.end(); ++iter) {
      RsGtsInstance* gts_instance = iter->second;
      if (nullptr == gts_instance) {
        // bypass
      } else {
        if (OB_SUCCESS != gts_instance->try_remove_tenant(tenant_id)) {
          LOG_WARN("try remove tennat failed", K(ret), K(tenant_id));
        }
      }
    }
  }
  return ret;
}

// TODO: some more effective strategy for picking gts instance id
int ObRsGtsManager::pick_gts_instance_id(const common::ObRegion& primary_region, uint64_t& gts_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", K(ret));
  } else {
    gts_id = OB_INVALID_ID;
    common::ObSEArray<uint64_t, 7>* id_array = nullptr;
    int tmp_ret = region_gts_map_.get_refactored(primary_region, id_array);
    if (OB_HASH_NOT_EXIST == tmp_ret) {
      // gts for this region not exist, pick one randomly
      if (gts_instance_map_.size() <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("no available gts instance", K(ret));
      } else {
        gts_id = gts_instance_map_.begin()->first;
      }
    } else if (OB_SUCCESS == tmp_ret) {
      if (nullptr == id_array) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("id array ptr is null", K(ret));
      } else {
        int64_t service_min_tenant_cnt = INT64_MAX;
        for (int64_t i = 0; OB_SUCC(ret) && i < id_array->count(); ++i) {
          const uint64_t this_gts_id = id_array->at(i);
          RsGtsInstance* rs_gts_instance = nullptr;
          if (OB_FAIL(gts_instance_map_.get_refactored(this_gts_id, rs_gts_instance))) {
            LOG_WARN("fail to get refactored", K(ret), K(this_gts_id));
          } else if (nullptr == rs_gts_instance) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("rs_gts instance ptr is null", K(ret), K(this_gts_id));
          } else if (rs_gts_instance->get_tenant_id_array().count() < service_min_tenant_cnt) {
            gts_id = this_gts_id;
            service_min_tenant_cnt = rs_gts_instance->get_tenant_id_array().count();
          } else {
          }  // go on check next
        }
      }
    } else {
      ret = tmp_ret;
      LOG_WARN("fail to get from region gts map", K(ret));
    }
  }
  return ret;
}

int ObRsGtsManager::alloc_gts_instance_for_tenant(const uint64_t tenant_id, const common::ObRegion& primary_region)
{
  int ret = OB_SUCCESS;
  int64_t start = ObTimeUtility::current_time();
  SpinWLockGuard lock_guard(lock_);
  uint64_t gts_id = OB_INVALID_ID;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", K(ret));
  } else if (OB_FAIL(pick_gts_instance_id(primary_region, gts_id))) {
    LOG_WARN("fail to pick instance id", K(ret));
  } else if (OB_FAIL(gts_table_operator_.insert_tenant_gts(tenant_id, gts_id))) {
    LOG_WARN("fail to insert tenant gts", K(ret));
  } else if (OB_FAIL(inner_alloc_gts_instance_for_tenant(tenant_id, gts_id))) {
    LOG_WARN("fail to inner alloc gts instance for tenant", K(ret), K(tenant_id));
  }
  LOG_INFO("alloc gts instance for tenant", K(ret), K(tenant_id), "cost", ObTimeUtility::current_time() - start);
  return ret;
}

int ObRsGtsManager::check_tenant_ha_gts_exist(const uint64_t tenant_id, bool& ha_gts_exist)
{
  int ret = OB_SUCCESS;
  SpinRLockGuard lock_guard(lock_);
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    uint64_t gts_id = OB_INVALID_ID;
    int tmp_ret = tenant_gts_map_.get_refactored(tenant_id, gts_id);
    if (OB_HASH_NOT_EXIST == tmp_ret) {
      ha_gts_exist = false;
    } else if (OB_SUCCESS == tmp_ret) {
      ha_gts_exist = true;
    } else {
      ret = tmp_ret;
      LOG_WARN("check tenant exist failed", K(ret));
    }
  }
  return ret;
}

int ObRsGtsManager::erase_tenant_gts(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  int64_t start = ObTimeUtility::current_time();
  SpinWLockGuard lock_guard(lock_);
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(gts_table_operator_.erase_tenant_gts(tenant_id))) {
    LOG_WARN("fail to erase tenant gts", K(ret));
  } else if (OB_FAIL(inner_erase_tenant_gts(tenant_id))) {
    LOG_WARN("fail to inner erase tenant gts", K(ret), K(tenant_id));
  }
  LOG_INFO("erase tenant gts succeed", K(ret), K(tenant_id), "cost", ObTimeUtility::current_time() - start);
  return ret;
}

int ObRsGtsManager::upgrade_cluster_create_region_ha_gts(const common::ObRegion& region)
{
  int ret = OB_SUCCESS;
  common::ObArray<common::ObZone> zone_list;
  common::ObArray<share::ObUnitInfo> all_gts_units;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", K(ret));
  } else if (OB_UNLIKELY(nullptr == zone_mgr_ || nullptr == unit_mgr_ || nullptr == server_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("zone_mgr_ or unit_mgr_ or server_mgr_ ptr is null", K(ret));
  } else if (OB_FAIL(zone_mgr_->get_zone(region, zone_list))) {
    LOG_WARN("fail to get zone in region", K(region));
  } else if (OB_UNLIKELY(zone_list.count() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("zone list unexpected", K(ret), K(zone_list));
  } else if (OB_FAIL(unit_mgr_->get_all_gts_unit_infos(all_gts_units))) {
    LOG_WARN("fail to get all unit infos of gts", K(ret));
  } else {
    common::ObMemberList member_list;
    common::ObAddr standby_server;
    const int64_t now = common::ObTimeUtility::current_time();
    for (int64_t i = 0; OB_SUCC(ret) && i < all_gts_units.count(); ++i) {
      bool is_active = false;
      const share::ObUnitInfo& this_unit = all_gts_units.at(i);
      const common::ObZone& this_zone = this_unit.unit_.zone_;
      if (!has_exist_in_array(zone_list, this_zone)) {
        // bypass, since this unit not in region
      } else if (OB_FAIL(server_mgr_->check_server_active(this_unit.unit_.server_, is_active))) {
        LOG_WARN("fail to check server active", K(ret), "server", this_unit.unit_.server_);
      } else if (!is_active) {
        // bypass, since unit server not active
      } else if (member_list.get_member_number() < OB_GTS_QUORUM) {
        common::ObMember this_member(this_unit.unit_.server_, now);
        if (OB_FAIL(member_list.add_member(this_member))) {
          LOG_WARN("fail to add member", K(ret), K(this_member));
        }
      } else if (!standby_server.is_valid()) {
        standby_server = this_unit.unit_.server_;
      } else {
        break;
      }
    }
    if (OB_SUCC(ret)) {
      common::ObGtsInfo gts_info;
      if (OB_FAIL(upgrade_cluster_construct_region_gts_info(region, member_list, standby_server, gts_info))) {
        LOG_WARN("fail to update cluster construct region gts info", K(ret));
      } else if (OB_FAIL(gts_table_operator_.insert_gts_instance(gts_info))) {
        LOG_WARN("fail to insert gts instance", K(ret));
      } else if (OB_FAIL(inner_create_gts_instance(gts_info.gts_id_, gts_info.gts_name_, region))) {
        LOG_WARN("fail to inner create gts instance", K(ret));
      }
    }
  }
  return ret;
}

int ObRsGtsManager::fetch_new_ha_gts_id(uint64_t& new_gts_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy ptr is null", K(ret));
  } else {
    uint64_t tmp_id = OB_INVALID_ID;
    share::ObMaxIdFetcher id_fetcher(*sql_proxy_);
    if (OB_FAIL(id_fetcher.fetch_new_max_id(
            OB_SYS_TENANT_ID, OB_MAX_USED_HA_GTS_ID_TYPE, tmp_id, common::OB_ORIGINAL_GTS_ID + 1))) {
      LOG_WARN("fail to fetch new max id", K(ret));
    } else {
      new_gts_id = tmp_id;
    }
  }
  return ret;
}

int ObRsGtsManager::upgrade_cluster_construct_region_gts_info(const common::ObRegion& region,
    const common::ObMemberList& member_list, const common::ObAddr& standby_server, common::ObGtsInfo& gts_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(region.is_empty() || member_list.get_member_number() <= 0)) {
    // ignore to check standby server validation
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(region), K(member_list));
  } else {
    uint64_t new_gts_id = OB_INVALID_ID;
    gts_info.region_ = region;
    gts_info.member_list_ = member_list;
    gts_info.standby_ = standby_server;
    gts_info.epoch_id_ = ObTimeUtility::current_time();
    gts_info.heartbeat_ts_ = ObTimeUtility::current_time();
    if (OB_FAIL(
            databuff_printf(gts_info.gts_name_.ptr(), common::MAX_GTS_NAME_LENGTH, "gts_instance_%s", region.ptr()))) {
      LOG_WARN("fail to do data buff printf", K(ret));
    } else if (OB_FAIL(fetch_new_ha_gts_id(new_gts_id))) {
      LOG_WARN("fail to fetch new ha gts id", K(ret));
    } else {
      gts_info.gts_id_ = new_gts_id;
    }
  }
  return ret;
}

int ObRsGtsManager::upgrade_cluster_create_ha_gts_util()
{
  int ret = OB_SUCCESS;
  common::ObArray<common::ObRegion> region_list;
  SpinWLockGuard lock_guard(lock_);
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", K(ret));
  } else if (OB_UNLIKELY(nullptr == zone_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("zone_mgr_ ptr is null, unexpected", K(ret));
  } else if (OB_FAIL(zone_mgr_->get_all_region(region_list))) {
    LOG_WARN("fail to get all region", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < region_list.count(); ++i) {
      const common::ObRegion& region = region_list.at(i);
      common::ObSEArray<uint64_t, 7>* id_array = nullptr;
      int tmp_ret = region_gts_map_.get_refactored(region, id_array);
      if (OB_HASH_NOT_EXIST == tmp_ret) {
        if (OB_FAIL(upgrade_cluster_create_region_ha_gts(region))) {
          LOG_WARN("fail to update cluster create region ha gts", K(ret), K(region));
        }
      } else if (OB_SUCCESS == tmp_ret) {
        // bypass, since ha gts exists on this region
      } else {
        ret = tmp_ret;
        LOG_WARN("fail to check region exist", K(ret));
      }
    }
  }
  return ret;
}

int ObRsGtsManager::load_gts_instance(const common::ObIArray<common::ObGtsInfo>& gts_info_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("RsGtsManager not init", K(ret));
  } else {
    // if the load instance fails, the load_gts_instance shall be invoked by a upper level,
    // the upper level code guarantees to free associated memory to ensure no resource leak occurs
    for (int64_t i = 0; OB_SUCC(ret) && i < gts_info_array.count(); ++i) {
      const ObGtsInfo& gts_info = gts_info_array.at(i);
      RsGtsInstance* gts_instance = gts_instance_allocator_.alloc();
      if (OB_UNLIKELY(nullptr == gts_instance)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory", K(ret));
      } else {
        gts_instance->set_gts_id(gts_info.gts_id_);
        gts_instance->set_gts_name(gts_info.gts_name_);
        gts_instance->set_region(gts_info.region_);
        if (OB_FAIL(gts_instance_map_.set_refactored(gts_instance->get_gts_id(), gts_instance))) {
          LOG_WARN("fail to set refactored", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        common::ObSEArray<uint64_t, 7>* gts_id_array = nullptr;
        int tmp_ret = region_gts_map_.get_refactored(gts_info.region_, gts_id_array);
        if (OB_HASH_NOT_EXIST == tmp_ret) {
          const int32_t write_flag = 0;
          gts_id_array = id_array_allocator_.alloc();
          if (OB_UNLIKELY(nullptr == gts_id_array)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to alloc memory", K(ret));
          } else if (OB_FAIL(gts_id_array->push_back(gts_info.gts_id_))) {
            LOG_WARN("fail to push back", K(ret));
          } else if (OB_FAIL(region_gts_map_.set_refactored(gts_info.region_, gts_id_array, write_flag))) {
            LOG_WARN("fail to set refactored", K(ret));
          }
          if (OB_FAIL(ret) && nullptr != gts_id_array) {
            // destructor is invoked inside the allocator
            id_array_allocator_.free(gts_id_array);
            gts_id_array = nullptr;
          }
        } else if (OB_SUCCESS == tmp_ret) {
          if (OB_UNLIKELY(nullptr == gts_id_array)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("gts id array is nullptr", K(ret));
          } else if (OB_FAIL(gts_id_array->push_back(gts_info.gts_id_))) {
            LOG_WARN("fail to push back", K(ret));
          }
        } else {
          ret = tmp_ret;
          LOG_WARN("fail to get gts id array from map", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObRsGtsManager::load_tenant_gts_info(const common::ObIArray<common::ObGtsTenantInfo>& gts_tenant_info_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("RsGtsManager not int", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < gts_tenant_info_array.count(); ++i) {
      const common::ObGtsTenantInfo& gts_tenant_info = gts_tenant_info_array.at(i);
      RsGtsInstance* gts_instance = nullptr;
      if (!gts_tenant_info.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("gts tenant info invalid", K(ret), K(gts_tenant_info));
      } else if (OB_FAIL(gts_instance_map_.get_refactored(gts_tenant_info.gts_id_, gts_instance))) {
        LOG_WARN("fail to get refactored", K(ret));
      } else if (OB_UNLIKELY(nullptr == gts_instance)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("gts instance ptr is null", K(ret), KP(gts_instance));
      } else if (OB_FAIL(gts_instance->insert_tenant(gts_tenant_info.tenant_id_))) {
        LOG_WARN("fail to insert tenant", K(ret));
      } else if (OB_FAIL(tenant_gts_map_.set_refactored(gts_tenant_info.tenant_id_, gts_tenant_info.gts_id_))) {
        LOG_WARN("fail to set hash map", K(ret));
      }
    }
  }
  return ret;
}

}  // namespace rootserver
}  // namespace oceanbase
