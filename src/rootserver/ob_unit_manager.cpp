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



#include "ob_unit_manager.h"
#include "share/ob_max_id_fetcher.h"
#include "share/ob_tenant_memstore_info_operator.h"
#include "rootserver/ob_root_service.h"
#include "rootserver/ob_heartbeat_service.h"
#include "rootserver/ob_ls_balance_helper.h"
#include "share/ob_cluster_version.h"

namespace oceanbase
{
using namespace common;
using namespace common::sqlclient;
using namespace common::hash;
using namespace share;
using namespace share::schema;
namespace rootserver
{

#define LOG_USER_ERROR_WITH_ARGS(errno, format_str, args...) \
do { \
  const int64_t BUF_LEN = 256; \
  char buf[BUF_LEN] = {0}; \
  int64_t n = snprintf(buf, BUF_LEN, format_str, ##args); \
  if (n < 0 || n >= BUF_LEN) { \
    int64_t tmp_ret = OB_BUF_NOT_ENOUGH; \
    LOG_WARN("snprintf error or buf not enough", KR(ret), KR(tmp_ret), K(n), K(BUF_LEN)); \
  } else { \
    LOG_USER_ERROR(errno, buf); \
  } \
} while (0);

int ObUnitManager::ZoneUnitPtr::assign(const ZoneUnitPtr &other)
{
  int ret = OB_SUCCESS;
  zone_ = other.zone_;
  if (OB_FAIL(copy_assign(unit_ptrs_, other.unit_ptrs_))) {
    LOG_WARN("fail to assign unit_ptrs", K(ret));
  }
  return ret;
}

int ObUnitManager::ZoneUnitPtr::sort_by_unit_id()
{
  UnitIdCmp cmp;
  lib::ob_sort(unit_ptrs_.begin(), unit_ptrs_.end(), cmp);
  return cmp.get_ret();
}

// Priority order of ug_id for deleting:
// 1. non-gts units on inactive server,
// 2. non-gts units on active server
// 3. gts units on inactive server
// 4. gts units on active server
bool ObUnitManager::DeleteUnitPtrCmp::operator()(const share::ObUnit *left, const share::ObUnit *right)
{
  int ret = OB_SUCCESS;
  bool bret = true;
  bool is_left_is_gts_unit = false;
  bool is_left_server_active = false;
  bool is_right_gts_unit = false;
  bool is_right_server_active = false;
  if (OB_ISNULL(left) || OB_ISNULL(right)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unit is null ptr", KP(left), KP(right));
  // 1. gts VS non-gts, non-gts units first
  } else if (FALSE_IT(is_left_is_gts_unit = has_exist_in_array(gts_unit_ids_, left->unit_id_))) {
  } else if (FALSE_IT(is_right_gts_unit = has_exist_in_array(gts_unit_ids_, right->unit_id_))) {
  } else if (is_left_is_gts_unit != is_right_gts_unit) {
    bret = is_right_gts_unit ? true : false;
  // 2. active VS inactive server, inactive server first
  } else if (OB_FAIL(check_unit_server_active(*left, is_left_server_active))) {
    LOG_WARN("fail to check unit server active", KR(ret), KPC(left));
  } else if (OB_FAIL(check_unit_server_active(*right, is_right_server_active))) {
    LOG_WARN("fail to check unit server active", KR(ret), KPC(right));
  } else if (is_left_server_active != is_right_server_active) {
    bret = is_right_server_active ? true : false;
  // 3. if no winner in conditions above, unit with smaller id first
  } else if (left->unit_group_id_ != right->unit_group_id_) {
    bret = left->unit_group_id_ < right->unit_group_id_;
  } else {
    bret = left->unit_id_ < right->unit_id_;
  }
  // update ret_ only if failed
  if (OB_FAIL(ret)) {
    ret_ = ret;
  }
  return bret;
}

int ObUnitManager::DeleteUnitPtrCmp::check_unit_server_active(const share::ObUnit &unit, bool &is_active)
{
  int ret = OB_SUCCESS;
  ObServerInfoInTable server_info;
  if (OB_FAIL(SVR_TRACER.get_server_info(unit.server_, server_info))) {
    LOG_WARN("fail to get_server_info", KR(ret), K(unit));
  } else {
    is_active = server_info.is_active();
  }
  return ret;
}

int ObUnitManager::ZoneUnit::assign(const ZoneUnit &other)
{
  int ret = OB_SUCCESS;
  zone_ = other.zone_;
  if (OB_FAIL(copy_assign(unit_infos_, other.unit_infos_))) {
    LOG_WARN("failed to assign unit_infos_", K(ret));
  }
  return ret;
}
////////////////////////////////////////////////////////////////
double ObUnitManager::ObUnitLoad::get_demand(ObResourceType resource_type) const
{
  double ret = -1;
  switch (resource_type) {
    case RES_CPU:
      ret = unit_config_->min_cpu();
      break;
    case RES_MEM:
      ret = static_cast<double>(unit_config_->memory_size());
      break;
    case RES_LOG_DISK:
      ret = static_cast<double>(unit_config_->log_disk_size());
      break;
    default:
      ret = -1;
      break;
  }
  return ret;
}

////////////////////////////////////////////////////////////////
int ObUnitManager::ObUnitNumCountMap::inc_unit_num_cnt(int64_t unit_num)
{
  int64_t curr_cnt = 0;
  int ret = OB_SUCCESS;
  int tmp_ret = get_refactored(unit_num, curr_cnt);
  if (OB_SUCCESS == tmp_ret) {
    if (OB_FAIL(set_refactored(unit_num, curr_cnt + 1, 1/*flag*/))) {
      LOG_WARN("fail to set refactored", KR(ret), K(unit_num), K(curr_cnt));
    }
  } else if (OB_HASH_NOT_EXIST == tmp_ret) {
    if (OB_FAIL(set_refactored(unit_num, 1))) {
      LOG_WARN("fail to set refactored", KR(ret), K(unit_num), K(curr_cnt));
    }
  } else {
    ret = tmp_ret;
    LOG_WARN("fail to get_refactored", KR(ret), K(unit_num));
  }
  return ret;
}

int ObUnitManager::ObUnitNumCountMap::dec_unit_num_cnt(int64_t unit_num)
{
  int64_t curr_cnt = 0;
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_refactored(unit_num, curr_cnt))) {
    LOG_WARN("fail to get_refactored", KR(ret), K(unit_num));
  } else {
    if (curr_cnt > 1) {
      if (OB_FAIL(set_refactored(unit_num, curr_cnt - 1, 1/*flag*/))) {
        LOG_WARN("fail to set refactored", KR(ret), K(unit_num), K(curr_cnt));
      }
    } else if (1 == curr_cnt) {
      if (OB_FAIL(erase_refactored(unit_num))) {
        LOG_WARN("fail to erase refactored", KR(ret), K(unit_num), K(curr_cnt));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected count of unit_num, should be positive value", KR(ret), K(unit_num), K(curr_cnt));
    }
  }
  return ret;
}
////////////////////////////////////////////////////////////////
template<typename T>
bool has_duplicates(const ObIArray<T> &array)
{
  bool result = false;
  for (int64_t i = 0; !result && i < array.count(); ++i) {
    for (int64_t j = i + 1; !result && j < array.count(); ++j) {
      if (array.at(i) == array.at(j)) {
        result = true;
      }
    }
  }
  return result;
}

////////////////////////////////////////////////////////////////

ObUnitManager::ObUnitManager(ObServerManager &server_mgr, ObZoneManager &zone_mgr)
: inited_(false), loaded_(false), proxy_(NULL), server_config_(NULL),
    srv_rpc_proxy_(NULL), server_mgr_(server_mgr),
    zone_mgr_(zone_mgr), ut_operator_(), id_config_map_(),
    name_config_map_(), config_ref_count_map_(), config_pools_map_(),
    config_pools_allocator_(OB_MALLOC_NORMAL_BLOCK_SIZE, ObMalloc(ObModIds::OB_RS_UNIT_MANAGER)),
    config_allocator_(OB_MALLOC_NORMAL_BLOCK_SIZE, ObMalloc(ObModIds::OB_RS_UNIT_MANAGER)),
    id_pool_map_(), name_pool_map_(),
    pool_allocator_(OB_MALLOC_NORMAL_BLOCK_SIZE, ObMalloc(ObModIds::OB_RS_UNIT_MANAGER)),
    pool_unit_map_(),
    pool_unit_allocator_(OB_MALLOC_NORMAL_BLOCK_SIZE, ObMalloc(ObModIds::OB_RS_UNIT_MANAGER)),
    id_unit_map_(),
    allocator_(OB_MALLOC_NORMAL_BLOCK_SIZE, ObMalloc(ObModIds::OB_RS_UNIT_MANAGER)),
    server_loads_(),
    load_allocator_(OB_MALLOC_NORMAL_BLOCK_SIZE, ObMalloc(ObModIds::OB_RS_UNIT_MANAGER)),
    tenant_pools_map_(),
    tenant_pools_allocator_(OB_MALLOC_NORMAL_BLOCK_SIZE, ObMalloc(ObModIds::OB_RS_UNIT_MANAGER)),
    server_migrate_units_map_(),
    migrate_units_allocator_(OB_MALLOC_NORMAL_BLOCK_SIZE, ObMalloc(ObModIds::OB_RS_UNIT_MANAGER)),
    lock_(ObLatchIds::UNIT_MANAGER_LOCK),
    schema_service_(NULL), root_balance_(NULL)
{
}

ObUnitManager::~ObUnitManager()
{
  ObHashMap<uint64_t, ObArray<share::ObResourcePool *> *>::iterator iter1;
  for (iter1 = config_pools_map_.begin(); iter1 != config_pools_map_.end(); ++iter1) {
    ObArray<share::ObResourcePool *> *ptr = iter1->second;
    if (NULL != ptr) {
      ptr->reset();
      ptr = NULL;
    }
  }
  ObHashMap<uint64_t, ObArray<ObUnit *> *>::iterator iter2;
  for (iter2 = pool_unit_map_.begin(); iter2 != pool_unit_map_.end(); ++iter2) {
    ObArray<share::ObUnit *> *ptr = iter2->second;
    if (NULL != ptr) {
      ptr->reset();
      ptr = NULL;
    }
  }
  ObHashMap<ObAddr, ObArray<ObUnitLoad> *>::iterator iter3;
  for (iter3 = server_loads_.begin(); iter3 != server_loads_.end(); ++iter3) {
    ObArray<ObUnitLoad> *ptr = iter3->second;
    if (NULL != ptr) {
      ptr->reset();
      ptr = NULL;
    }
  }
  TenantPoolsMap::iterator iter4;
  for (iter4 = tenant_pools_map_.begin(); iter4 != tenant_pools_map_.end(); ++iter4) {
    common::ObArray<share::ObResourcePool *> *ptr = iter4->second;
    if (NULL != ptr) {
      ptr->reset();
      ptr = NULL;
    }
  }
  ObHashMap<ObAddr, ObArray<uint64_t> *>::iterator iter5;
  for (iter5 = server_migrate_units_map_.begin();
       iter5 != server_migrate_units_map_.end();
       ++iter5) {
    common::ObArray<uint64_t> *ptr = iter5->second;
    if (NULL != ptr) {
      ptr->reset();
      ptr = NULL;
    }
  }
}

int ObUnitManager::init(ObMySQLProxy &proxy,
                        ObServerConfig &server_config,
                        obrpc::ObSrvRpcProxy &srv_rpc_proxy,
                        share::schema::ObMultiVersionSchemaService &schema_service,
                        ObRootBalancer &root_balance,
                        ObRootService &root_service)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(ut_operator_.init(proxy))) {
    LOG_WARN("init unit table operator failed", K(ret));
  } else if (OB_FAIL(pool_unit_map_.create(
              POOL_MAP_BUCKET_NUM, ObModIds::OB_HASH_BUCKET_POOL_UNIT_MAP))) {
    LOG_WARN("pool_unit_map_ create failed", LITERAL_K(POOL_MAP_BUCKET_NUM), K(ret));
  } else if (OB_FAIL(id_unit_map_.create(
              UNIT_MAP_BUCKET_NUM, ObModIds::OB_HASH_BUCKET_ID_UNIT_MAP))) {
    LOG_WARN("id_unit_map_ create failed",
             LITERAL_K(UNIT_MAP_BUCKET_NUM), K(ret));
  } else if (OB_FAIL(id_config_map_.create(
              CONFIG_MAP_BUCKET_NUM, ObModIds::OB_HASH_BUCKET_ID_CONFIG_MAP))) {
    LOG_WARN("id_config_map_ create failed",
             LITERAL_K(CONFIG_MAP_BUCKET_NUM), K(ret));
  } else if (OB_FAIL(name_config_map_.create(
              CONFIG_MAP_BUCKET_NUM, ObModIds::OB_HASH_BUCKET_NAME_CONFIG_MAP))) {
    LOG_WARN("name_config_map_ create failed",
             LITERAL_K(CONFIG_MAP_BUCKET_NUM), K(ret));
  } else if (OB_FAIL(config_ref_count_map_.create(
              CONFIG_REF_COUNT_MAP_BUCKET_NUM, ObModIds::OB_HASH_BUCKET_CONFIG_REF_COUNT_MAP))) {
    LOG_WARN("config_ref_count_map_ create failed",
             LITERAL_K(CONFIG_REF_COUNT_MAP_BUCKET_NUM), K(ret));
  } else if (OB_FAIL(config_pools_map_.create(CONFIG_POOLS_MAP_BUCKET_NUM,
                                              ObModIds::OB_HASH_BUCKET_CONFIG_POOLS_MAP))) {
    LOG_WARN("create config_pools_map failed",
             LITERAL_K(CONFIG_POOLS_MAP_BUCKET_NUM), K(ret));
  } else if (OB_FAIL(id_pool_map_.create(
              POOL_MAP_BUCKET_NUM, ObModIds::OB_HASH_BUCKET_ID_POOL_MAP))) {
    LOG_WARN("id_pool_map_ create failed",
             LITERAL_K(POOL_MAP_BUCKET_NUM), K(ret));
  } else if (OB_FAIL(name_pool_map_.create(
              POOL_MAP_BUCKET_NUM, ObModIds::OB_HASH_BUCKET_NAME_POOL_MAP))) {
    LOG_WARN("name_pool_map_ create failed",
             LITERAL_K(POOL_MAP_BUCKET_NUM), K(ret));
  } else if (OB_FAIL(server_loads_.create(
              UNITLOAD_MAP_BUCKET_NUM, ObModIds::OB_HASH_BUCKET_SERVER_UNITLOAD_MAP))) {
    LOG_WARN("server_loads_ create failed",
             LITERAL_K(UNITLOAD_MAP_BUCKET_NUM), K(ret));
  } else if (OB_FAIL(tenant_pools_map_.create(
              TENANT_POOLS_MAP_BUCKET_NUM, ObModIds::OB_HASH_BUCKET_TENANT_POOLS_MAP))) {
    LOG_WARN("tenant_pools_map_ create failed",
             LITERAL_K(TENANT_POOLS_MAP_BUCKET_NUM), K(ret));
  } else if (OB_FAIL(server_migrate_units_map_.create(
              SERVER_MIGRATE_UNITS_MAP_BUCKET_NUM, ObModIds::OB_HASH_BUCKET_SERVER_MIGRATE_UNIT_MAP))) {
    LOG_WARN("server_migrate_units_map_ create failed",
             LITERAL_K(SERVER_MIGRATE_UNITS_MAP_BUCKET_NUM), K(ret));
  } else {
    proxy_ = &proxy;
    server_config_ = &server_config;
    srv_rpc_proxy_ = &srv_rpc_proxy;
    root_service_ = &root_service;
    schema_service_ = &schema_service;
    root_balance_ = &root_balance;
    loaded_ = false;
    inited_ = true;
  }
  return ret;
}

// make sure lock_ is held when calling this method
int ObUnitManager::check_inner_stat_() const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(proxy_) || OB_ISNULL(server_config_)
      || OB_ISNULL(srv_rpc_proxy_) || OB_ISNULL(root_service_)
      || OB_ISNULL(schema_service_) || OB_ISNULL(root_balance_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inner stat error", KR(ret), KP(proxy_), KP(server_config_),
        KP(srv_rpc_proxy_), KP(root_service_), KP(schema_service_), KP(root_balance_));
  } else if (OB_UNLIKELY(!loaded_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("not loaded", KR(ret));
    // need to reload
    int tmp_ret = OB_SUCCESS;
    if (OB_ISNULL(root_service_)) {
      tmp_ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("inited but root_service_ is nullptr", KR(ret), KR(tmp_ret), KP(root_service_));
    } else if (OB_TMP_FAIL(root_service_->submit_reload_unit_manager_task())) {
      if (OB_CANCELED != tmp_ret) {
        LOG_ERROR("fail to reload unit_manager, please try 'alter system reload unit'", KR(tmp_ret), KR(ret));
      }
    }
  } else {
    // stat is normal
  }
  return ret;
}

int ObUnitManager::load()
{
  DEBUG_SYNC(BEFORE_RELOAD_UNIT);
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  LOG_INFO("unit manager load start", K(ret));
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    loaded_ = false;
    ObHashMap<uint64_t, ObArray<share::ObResourcePool *> *>::iterator iter1;
    for (iter1 = config_pools_map_.begin(); iter1 != config_pools_map_.end(); ++iter1) {
      ObArray<share::ObResourcePool *> *ptr = iter1->second;
      if (NULL != ptr) {
        ptr->reset();
        ptr = NULL;
      }
    }
    ObHashMap<uint64_t, ObArray<ObUnit *> *>::iterator iter2;
    for (iter2 = pool_unit_map_.begin(); iter2 != pool_unit_map_.end(); ++iter2) {
      ObArray<share::ObUnit *> *ptr = iter2->second;
      if (NULL != ptr) {
        ptr->reset();
        ptr = NULL;
      }
    }
    ObHashMap<ObAddr, ObArray<ObUnitLoad> *>::iterator iter3;
    for (iter3 = server_loads_.begin(); iter3 != server_loads_.end(); ++iter3) {
      ObArray<ObUnitLoad> *ptr = iter3->second;
      if (NULL != ptr) {
        ptr->reset();
        ptr = NULL;
      }
    }
    TenantPoolsMap::iterator iter4;
    for (iter4 = tenant_pools_map_.begin(); iter4 != tenant_pools_map_.end(); ++iter4) {
      common::ObArray<share::ObResourcePool *> *ptr = iter4->second;
      if (NULL != ptr) {
        ptr->reset();
        ptr = NULL;
      }
    }
    ObHashMap<ObAddr, ObArray<uint64_t> *>::iterator iter5;
    for (iter5 = server_migrate_units_map_.begin();
         iter5 != server_migrate_units_map_.end();
         ++iter5) {
      common::ObArray<uint64_t> *ptr = iter5->second;
      if (NULL != ptr) {
        ptr->reset();
        ptr = NULL;
      }
    }
    if (OB_FAIL(id_config_map_.clear())) {
      LOG_WARN("id_config_map_ clear failed", K(ret));
    } else if (OB_FAIL(name_config_map_.clear())) {
      LOG_WARN("name_pool_map_  clear failed", K(ret));
    } else if (OB_FAIL(config_ref_count_map_.clear())) {
      LOG_WARN("config_ref_count_map_ clear failed", K(ret));
    } else if (OB_FAIL(config_pools_map_.clear())) {
      LOG_WARN("config_pools_map_ clear failed", K(ret));
    } else if (OB_FAIL(id_pool_map_.clear())) {
      LOG_WARN("id_pool_map_ clear failed", K(ret));
    } else if (OB_FAIL(name_pool_map_.clear())) {
      LOG_WARN("name_pool_map_ clear failed", K(ret));
    } else if (OB_FAIL(pool_unit_map_.clear())) {
      LOG_WARN("pool_unit_map_ clear failed", K(ret));
    } else if (OB_FAIL(id_unit_map_.clear())) {
      LOG_WARN("id_unit_map_ clear failed", K(ret));
    } else if (OB_FAIL(server_loads_.clear())) {
      LOG_WARN("server_loads_ clear failed", K(ret));
    } else if (OB_FAIL(tenant_pools_map_.clear())) {
      LOG_WARN("tenant_pools_map_ clear failed", K(ret));
    } else if (OB_FAIL(server_migrate_units_map_.clear())) {
      LOG_WARN("server_migrate_units_map_ clear failed", K(ret));
    }

    // free all memory
    if (OB_SUCC(ret)) {
      config_allocator_.reset();
      config_pools_allocator_.reset();
      pool_allocator_.reset();
      pool_unit_allocator_.reset();
      allocator_.reset();
      load_allocator_.reset();
      tenant_pools_allocator_.reset();
      migrate_units_allocator_.reset();
    }

    // load unit config
    ObArray<ObUnitConfig> configs;
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ut_operator_.get_unit_configs(configs))) {
        LOG_WARN("get_unit_configs failed", K(ret));
      } else if (OB_FAIL(build_config_map(configs))) {
        LOG_WARN("build_config_map failed", K(ret));
      }
    }

    // load resource pool
    ObArray<share::ObResourcePool> pools;
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ut_operator_.get_resource_pools(pools))) {
        LOG_WARN("get_resource_pools failed", K(ret));
      } else if (OB_FAIL(build_pool_map(pools))) {
        LOG_WARN("build_pool_map failed", K(ret));
      }
    }

    // load unit
    ObArray<ObUnit> units;
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ut_operator_.get_units(units))) {
        LOG_WARN("get_units failed", K(ret));
      } else if (OB_FAIL(build_unit_map(units))) {
        LOG_WARN("build_unit_map failed", K(ret));
      }
    }

    // build tenant pools
    if (OB_SUCC(ret)) {
      for (ObHashMap<uint64_t, share::ObResourcePool *>::iterator it = id_pool_map_.begin();
           OB_SUCCESS == ret && it != id_pool_map_.end(); ++it) {
        // pool not grant to tenant don't add to tenant_pools_map
        if (NULL == it->second) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("it->second is null", KP(it->second), K(ret));
        } else if (it->second->is_granted_to_tenant()) {
          if (OB_FAIL(insert_tenant_pool(it->second->tenant_id_, it->second))) {
            LOG_WARN("insert_tenant_pool failed", "tenant_id", it->second->tenant_id_,
                     "pool", *(it->second), K(ret));
          }
        }
      }
    }

    // build server migrate units
    if (OB_SUCC(ret)) {
      FOREACH_CNT_X(unit, units, OB_SUCCESS == ret) {
        if (unit->migrate_from_server_.is_valid()) {
          if (OB_FAIL(insert_migrate_unit(unit->migrate_from_server_, unit->unit_id_))) {
            LOG_WARN("insert_migrate_unit failed", "server", unit->migrate_from_server_,
                     "unit_id", unit->unit_id_, K(ret));
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      loaded_ = true;
    }
  }
  LOG_INFO("unit manager load finish", K(ret));
  return ret;
}

int ObUnitManager::create_unit_config(const ObUnitConfig &unit_config, const bool if_not_exist)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  if (OB_FAIL(inner_create_unit_config_(unit_config, if_not_exist))) {
    LOG_WARN("fail to create unit config", KR(ret), K(unit_config), K(if_not_exist));
  }
  return ret;
}

int ObUnitManager::inner_create_unit_config_(const ObUnitConfig &unit_config, const bool if_not_exist)
{
  int ret = OB_SUCCESS;
  ObUnitConfig *temp_config = NULL;
  ObUnitConfig *new_config = NULL;
  uint64_t unit_config_id = unit_config.unit_config_id();
  const ObUnitConfigName &name = unit_config.name();
  const ObUnitResource &rpc_ur = unit_config.unit_resource();
  ObUnitResource ur = rpc_ur;

  LOG_INFO("start create unit config", K(name), K(rpc_ur), K(if_not_exist));

  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(inited_), K(loaded_), K(ret));
  } else if (name.is_empty()) {
    ret = OB_MISS_ARGUMENT;
    LOG_WARN("miss 'name' argument", KR(ret), K(name));
    LOG_USER_ERROR(OB_MISS_ARGUMENT, "resource unit name");
  } else if (OB_FAIL(ur.init_and_check_valid_for_unit(rpc_ur))) {
    LOG_WARN("init from user specified unit resource and check valid fail", KR(ret), K(rpc_ur));
  } else if (OB_SUCCESS == (ret = get_unit_config_by_name(name, temp_config))) {
    if (NULL == temp_config) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("temp_config is null", KP(temp_config), K(ret));
    } else if (if_not_exist) {
      ObCStringHelper helper;
      LOG_USER_NOTE(OB_RESOURCE_UNIT_EXIST, helper.convert(name));
      LOG_INFO("unit config already exist", K(name));
    } else {
      ret = OB_RESOURCE_UNIT_EXIST;
      ObCStringHelper helper;
      LOG_USER_ERROR(OB_RESOURCE_UNIT_EXIST, helper.convert(name));
      LOG_WARN("unit config already exist", K(name), KR(ret));
    }
  } else if (OB_ENTRY_NOT_EXIST != ret) {
    LOG_WARN("get_unit_config_by_name failed", "config_name", name, KR(ret));
  }
  // allocate new unit config id
  else if (OB_INVALID_ID == unit_config_id && OB_FAIL(fetch_new_unit_config_id(unit_config_id))) {
    LOG_WARN("fetch_new_unit_config_id failed", KR(ret), K(unit_config));
  } else {
    if (OB_ISNULL(new_config = config_allocator_.alloc())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc memory failed", KR(ret));
    } else if (OB_FAIL(new_config->init(unit_config_id, name, ur))) {
      LOG_WARN("init unit config fail", KR(ret), K(unit_config_id), K(name), K(ur));
    } else if (OB_FAIL(ut_operator_.update_unit_config(*proxy_, *new_config))) {
      LOG_WARN("update_unit_config failed", "unit config", *new_config, K(ret));
    } else if (OB_FAIL(insert_unit_config(new_config))) {
      LOG_WARN("insert_unit_config failed", "unit config", *new_config,  K(ret));
    }

    // avoid memory leak
    if (OB_FAIL(ret) && NULL != new_config) {
      config_allocator_.free(new_config);
      new_config = NULL;
    }
  }
  LOG_INFO("finish create unit config", KR(ret), K(name), K(rpc_ur), K(if_not_exist), KPC(new_config));
  return ret;
}

int ObUnitManager::alter_unit_config(const ObUnitConfig &unit_config)
{
  int ret = OB_SUCCESS;
  ObUnitConfig *old_config = NULL;
  ObArray<share::ObResourcePool *> *pools = NULL;
  SpinWLockGuard guard(lock_);
  const ObUnitConfigName &name = unit_config.name();
  const ObUnitResource &rpc_ur = unit_config.unit_resource();

  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(inited_), K(loaded_), K(ret));
  } else if (name.is_empty()) {
    ret = OB_MISS_ARGUMENT;
    LOG_WARN("miss 'name' argument", KR(ret));
    LOG_USER_ERROR(OB_MISS_ARGUMENT, "resource unit name");
  } else if (OB_FAIL(get_unit_config_by_name(name, old_config))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("get_unit_config_by_name failed", K(name), K(ret));
    } else {
      // overwrite ret on purpose
      ret = OB_RESOURCE_UNIT_NOT_EXIST;
      LOG_WARN("config does not exist", K(name), KR(ret));
      ObCStringHelper helper;
      LOG_USER_ERROR(OB_RESOURCE_UNIT_NOT_EXIST, helper.convert(name));
    }
  } else if (OB_ISNULL(old_config)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("old_config is null", KP(old_config), KR(ret));
  } else {
    // copy unit resource
    ObUnitResource new_ur = old_config->unit_resource();
    const ObUnitResource old_ur = old_config->unit_resource();

    // update based on user specified
    if (OB_FAIL(new_ur.update_and_check_valid_for_unit(rpc_ur))) {
      LOG_WARN("update and check valid for unit fail", KR(ret), K(new_ur), K(rpc_ur));
    } else {
      if (OB_FAIL(get_pools_by_config(old_config->unit_config_id(), pools))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          LOG_WARN("get_pools_by_config failed", "config_id", old_config->unit_config_id(), KR(ret));
        } else {
          ret = OB_SUCCESS;
          // this unit config is not used by any resource pools
          // update the config directly
        }
      } else if (NULL == pools) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pools is null", KP(pools), K(ret));
      } else {
        if (OB_FAIL(check_expand_resource_("ALTER_RESOURCE_UNIT", *pools, old_ur, new_ur))) {
          LOG_WARN("check expand config failed", K(old_ur), K(new_ur), KR(ret));
        } else if (OB_FAIL(check_shrink_resource_(*pools, old_ur, new_ur))) {
          LOG_WARN("check shrink config failed", K(old_ur), K(new_ur), KR(ret));
        } else if (OB_FAIL(check_full_resource_pool_memory_condition(*pools, new_ur.memory_size()))) {
          LOG_WARN("fail to check full resource pool memory condition", K(ret), K(new_ur), KPC(pools));
        }
      }
    }

    if (OB_SUCC(ret)) {
      ObUnitConfig new_config;
      if (OB_FAIL(new_config.init(old_config->unit_config_id(), old_config->name(), new_ur))) {
        LOG_WARN("init new unit config fail", KR(ret), KPC(old_config), K(new_ur));
      } else if (OB_FAIL(ut_operator_.update_unit_config(*proxy_, new_config))) {
        LOG_WARN("update_unit_config failed", K(new_config), KR(ret));
      } else if (OB_FAIL(old_config->update_unit_resource(new_ur))) {
        LOG_WARN("update unit resource of unit config fail", KR(ret), KPC(old_config), K(new_ur));
      }
    }

    if (OB_SUCC(ret)) {
      ROOTSERVICE_EVENT_ADD("unit", "alter_resource_unit",
                            "name", old_config->name(),
                            "unit_config_id", old_config->unit_config_id(),
                            "old_resource", old_ur,
                            "new_resource", new_ur);
    }
  }
  return ret;
}

int ObUnitManager::drop_unit_config(const ObUnitConfigName &name, const bool if_exist)
{
  int ret = OB_SUCCESS;
  LOG_INFO("start drop unit config", K(name));
  SpinWLockGuard guard(lock_);
  ObUnitConfig *config = NULL;
  int64_t ref_count = 0;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(inited_), K(loaded_), K(ret));
  } else if (name.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "resource unit name");
    LOG_WARN("invalid argument", K(name), K(ret));
  } else if (OB_FAIL(get_unit_config_by_name(name, config))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("get_unit_config_by_name failed", K(name), K(ret));
    } else {
      if (if_exist) {
        ret = OB_SUCCESS;
        ObCStringHelper helper;
        LOG_USER_NOTE(OB_RESOURCE_UNIT_NOT_EXIST, helper.convert(name));
        LOG_INFO("unit config not exist, no need to delete it", K(name));
      } else {
        ret = OB_RESOURCE_UNIT_NOT_EXIST;
        ObCStringHelper helper;
        LOG_USER_ERROR(OB_RESOURCE_UNIT_NOT_EXIST, helper.convert(name));
        LOG_WARN("unit config not exist", K(name), K(ret));
      }
    }
  } else if (NULL == config) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("config is null", KP(config), K(ret));
  } else if (OB_FAIL(get_config_ref_count(config->unit_config_id(), ref_count))) {
    LOG_WARN("get_config_ref_count failed", "config_id", config->unit_config_id(), K(ret));
  } else if (0 != ref_count) {
    ret = OB_RESOURCE_UNIT_IS_REFERENCED;
    ObCStringHelper helper;
    LOG_USER_ERROR(OB_RESOURCE_UNIT_IS_REFERENCED, helper.convert(name));
    LOG_WARN("some resource pool is using this unit config, can not delete it",
             K(ref_count), K(ret));
  } else if (OB_FAIL(ut_operator_.remove_unit_config(*proxy_, config->unit_config_id()))) {
    LOG_WARN("remove_unit_config failed", "config_id", config->unit_config_id(), K(ret));
  } else if (OB_FAIL(delete_unit_config(config->unit_config_id(), config->name()))) {
    LOG_WARN("delete_unit_config failed", "config id", config->unit_config_id(),
             "name", config->name(), K(ret));
  } else {
    ROOTSERVICE_EVENT_ADD("unit", "drop_resource_unit",
                          "name", config->name());
    // free memory
    config_allocator_.free(config);
    config = NULL;
  }
  LOG_INFO("finish drop unit config", K(name), K(ret));
  return ret;
}

int ObUnitManager::check_tenant_pools_altering_unit_num(
    const uint64_t tenant_id,
    bool &check_ret)
{
  int ret = OB_SUCCESS;
  common::ObArray<share::ObResourcePool *> *pools = NULL;
  SpinRLockGuard guard(lock_);
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check inner stat failed", K(ret), K(inited_), K(loaded_));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(get_pools_by_tenant_(tenant_id, pools))) {
    LOG_WARN("fail to get pools by tenant", K(ret), K(tenant_id));
  } else if (OB_UNLIKELY(NULL == pools)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pools ptr is null", K(ret), KP(pools));
  } else {
    check_ret = false;
    for (int64_t i = 0; !check_ret && OB_SUCC(ret) && i < pools->count(); ++i) {
      common::ObArray<share::ObUnit *> *units = NULL;
      const share::ObResourcePool *pool = pools->at(i);
      if (NULL == pool) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pool ptr is null", K(ret), KP(pool));
      } else if (OB_FAIL(inner_check_pool_altering_unit_num(pool->resource_pool_id_, check_ret))) {
        LOG_WARN("fail to inner check pool in shrinking or expanding", KR(ret), KPC(pool));
      }
    }
  }
  return ret;
}

int ObUnitManager::inner_check_pool_altering_unit_num(
    const uint64_t pool_id,
    bool &altering_unit_num)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check inner stat failed", K(ret), K(inited_), K(loaded_));
  } else if (OB_UNLIKELY(OB_INVALID_ID == pool_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(pool_id));
  } else {
    common::ObArray<share::ObUnit *> *units = NULL;
    if (OB_FAIL(get_units_by_pool(pool_id, units))) {
      LOG_WARN("fail to get units by pool", K(ret), K(pool_id));
    } else if (NULL == units) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("units ptr is null", K(ret), KP(units));
    } else {
      altering_unit_num = false;
      for (int64_t i = 0; !altering_unit_num && OB_SUCC(ret) && i < units->count(); ++i) {
        const ObUnit *unit = units->at(i);
        if (OB_UNLIKELY(NULL == unit)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unit ptr is null", K(ret));
        } else if (ObUnit::UNIT_STATUS_DELETING == unit->status_ ||
                   ObUnit::UNIT_STATUS_ADDING == unit->status_) {
          altering_unit_num = true;
        } else if (ObUnit::UNIT_STATUS_ACTIVE == unit->status_) {
          // a normal unit, go on and check next
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unit status unexpected", K(ret), "unit_status", unit->status_);
        }
      }
    }
  }
  return ret;
}

int ObUnitManager::create_resource_pool(
    share::ObResourcePool &resource_pool,
    const ObUnitConfigName &config_name,
    const bool if_not_exist)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  if (OB_FAIL(inner_create_resource_pool_(resource_pool, config_name, if_not_exist))) {
    LOG_WARN("fail to inner create resource pool", K(ret));
  }
  return ret;
}

int ObUnitManager::inner_create_resource_pool_(
    share::ObResourcePool &resource_pool,
    const ObUnitConfigName &config_name,
    const bool if_not_exist)
{
  int ret = OB_SUCCESS;
  LOG_INFO("start create resource pool", K(resource_pool), K(config_name));
  ObUnitConfig *config = NULL;
  share::ObResourcePool *pool = NULL;
  const char *module = "CREATE_RESOURCE_POOL";
  bool is_bootstrap_pool = (ObUnitConfig::SYS_UNIT_CONFIG_ID == resource_pool.unit_config_id_);
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(inited_), K(loaded_), K(ret));
  } else if (is_bootstrap_pool && OB_FAIL(check_bootstrap_pool(resource_pool))) {
    LOG_WARN("check bootstrap pool failed", K(resource_pool), K(ret));
  } else if (!is_bootstrap_pool && OB_FAIL(check_resource_pool(resource_pool))) {
    LOG_WARN("check_resource_pool failed", K(resource_pool), K(ret));
  } else if (config_name.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "resource unit name");
    LOG_WARN("invalid config_name", K(config_name), K(ret));
  } else if (OB_FAIL(get_unit_config_by_name(config_name, config))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("get_unit_config_by_name failed", K(config_name), K(ret));
    } else {
      ret = OB_RESOURCE_UNIT_NOT_EXIST;
      ObCStringHelper helper;
      LOG_USER_ERROR(OB_RESOURCE_UNIT_NOT_EXIST, helper.convert(config_name));
      LOG_WARN("config not exist", K(config_name), K(ret));
    }
  } else if (NULL == config) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("config is null", KP(config), K(ret));
  } else if (OB_SUCCESS == (ret = inner_get_resource_pool_by_name(resource_pool.name_, pool))) {
    if (NULL == pool) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pool is null", K_(resource_pool.name), KP(pool), K(ret));
    } else if (if_not_exist) {
      ObCStringHelper helper;
      LOG_USER_NOTE(OB_RESOURCE_POOL_EXIST, helper.convert(resource_pool.name_));
      LOG_INFO("resource_pool already exist, no need to create", K(resource_pool.name_));
    } else {
      ret = OB_RESOURCE_POOL_EXIST;
      ObCStringHelper helper;
      LOG_USER_ERROR(OB_RESOURCE_POOL_EXIST, helper.convert(resource_pool.name_));
      LOG_WARN("resource_pool already exist", "name", resource_pool.name_, K(ret));
    }
  } else if (OB_ENTRY_NOT_EXIST != ret) {
    LOG_WARN("get resource pool by name failed", "name", resource_pool.name_, K(ret));
  } else {
    ret = OB_SUCCESS;
    share::ObUnitTableTransaction trans;
    share::ObResourcePool *new_pool = NULL;
    const int64_t min_full_resource_pool_memory = GCONF.__min_full_resource_pool_memory;
    if (NULL == (new_pool = pool_allocator_.alloc())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc memory failed", K(ret));
    } else if (REPLICA_TYPE_FULL == resource_pool.replica_type_
        && config->memory_size() < min_full_resource_pool_memory) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("full resource pool min memory illegal", KR(ret), K(config->memory_size()),
          K(min_full_resource_pool_memory));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "unit MEMORY_SIZE less than __min_full_resource_pool_memory");
    } else {
      if (OB_FAIL(trans.start(proxy_, OB_SYS_TENANT_ID))) {
        LOG_WARN("start transaction failed", K(ret));
      } else {
        if (OB_FAIL(new_pool->assign(resource_pool))) {
          LOG_WARN("failed to assign new_pool", K(ret));
        } else {
          new_pool->unit_config_id_ = config->unit_config_id();
          if (OB_INVALID_ID == new_pool->resource_pool_id_) {
            if (OB_FAIL(fetch_new_resource_pool_id(new_pool->resource_pool_id_))) {
              LOG_WARN("fetch_new_resource_pool_id failed", K(ret));
            }
          } else {
            // sys resource pool with pool_id set
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(ut_operator_.update_resource_pool(trans, *new_pool))) {
          LOG_WARN("update_resource_pool failed", "resource_pool", *new_pool, K(ret));
        } else if (OB_FAIL(update_pool_map(new_pool))) {
          LOG_WARN("update pool map failed", "resource_pool", *new_pool, K(ret));
        }

        if (OB_SUCCESS == ret && !is_bootstrap_resource_pool(new_pool->resource_pool_id_)) {
          if (OB_FAIL(allocate_new_pool_units_(trans, *new_pool, module))) {
            LOG_WARN("arrange pool units failed", K(module), KR(ret), "resource_pool", *new_pool);
          }
        }
      }

      if (trans.is_started()) {
        const bool commit = (OB_SUCC(ret));
        int temp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (temp_ret = trans.end(commit))) {
          LOG_WARN("trans end failed", K(commit), K(temp_ret));
          ret = (OB_SUCCESS == ret) ? temp_ret : ret;
        }
      }
      if (OB_SUCC(ret)) {
        ret = OB_E(EventTable::EN_UNIT_MANAGER) OB_SUCCESS;
        DEBUG_SYNC(UNIT_MANAGER_WAIT_FOR_TIMEOUT);
      }

      if (OB_FAIL(ret)) {
        if (OB_INVALID_ID == new_pool->resource_pool_id_) {
          // do nothing, fetch new resource pool id failed
        } else {
          int temp_ret = OB_SUCCESS; // avoid ret overwritten
          // some error occur during doing the transaction, rollback change occur in memory
          ObArray<ObUnit *> *units = NULL;
          if (OB_SUCCESS == (temp_ret = get_units_by_pool(new_pool->resource_pool_id_, units))) {
            if (NULL == units) {
              temp_ret = OB_ERR_UNEXPECTED;
              LOG_WARN("units is null", KP(units), K(temp_ret));
            } else if (OB_SUCCESS != (temp_ret = delete_units_of_pool(
                        new_pool->resource_pool_id_))) {
              LOG_WARN("delete_units_of_pool failed", "resource_pool_id",
                       new_pool->resource_pool_id_, K(temp_ret));
            }
          } else if (OB_ENTRY_NOT_EXIST != temp_ret) {
            LOG_WARN("get_units_by_pool failed",
                     "resource_pool_id", new_pool->resource_pool_id_, K(temp_ret));
          } else {
            temp_ret = OB_SUCCESS;
          }

          share::ObResourcePool *temp_pool = NULL;
          if (OB_SUCCESS != temp_ret) {
          } else if (OB_SUCCESS != (temp_ret = get_resource_pool_by_id(
                      new_pool->resource_pool_id_, temp_pool))) {
            if (OB_ENTRY_NOT_EXIST != temp_ret) {
              LOG_WARN("get_resource_pool_by_id failed", "pool_id", new_pool->resource_pool_id_,
                       K(temp_ret));
            } else {
              temp_ret = OB_SUCCESS;
              // do nothing, no need to delete from id_map and name_map
            }
          } else if (NULL == temp_pool) {
            temp_ret = OB_ERR_UNEXPECTED;
            LOG_WARN("temp_pool is null", KP(temp_pool), K(temp_ret));
          } else if (OB_SUCCESS != (temp_ret = delete_resource_pool(
                      new_pool->resource_pool_id_, new_pool->name_))) {
            LOG_WARN("delete_resource_pool failed", "new pool", *new_pool, K(temp_ret));
          }
        }
        // avoid memory leak
        pool_allocator_.free(new_pool);
        new_pool = NULL;
      } else {
        // inc unit config ref count at last
        if (OB_FAIL(inc_config_ref_count(config->unit_config_id()))) {
          LOG_WARN("inc_config_ref_count failed", "config id", config->unit_config_id(), K(ret));
        } else if (OB_FAIL(insert_config_pool(config->unit_config_id(), new_pool))) {
          LOG_WARN("insert config pool failed", "config id", config->unit_config_id(), K(ret));
        } else {
          ROOTSERVICE_EVENT_ADD("unit", "create_resource_pool",
                                "name", new_pool->name_,
                                "unit", config_name,
                                "zone_list", new_pool->zone_list_);
        }
      }
    }
  }
  LOG_INFO("finish create resource pool", K(resource_pool), K(config_name), K(ret));
  return ret;
}

int ObUnitManager::convert_pool_name_list(
    const common::ObIArray<common::ObString> &split_pool_list,
    common::ObIArray<share::ObResourcePoolName> &split_pool_name_list)
{
  int ret = OB_SUCCESS;
  split_pool_name_list.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < split_pool_list.count(); ++i) {
    share::ObResourcePoolName pool_name;
    if (OB_FAIL(pool_name.assign(split_pool_list.at(i).ptr()))) {
      LOG_WARN("fail to assign pool name", K(ret));
    } else if (OB_FAIL(split_pool_name_list.push_back(pool_name))) {
      LOG_WARN("fail to push back", K(ret));
    }
  }
  return ret;
}

int ObUnitManager::check_split_pool_name_condition(
    const common::ObIArray<share::ObResourcePoolName> &split_pool_name_list)
{
  int ret = OB_SUCCESS;
  // Check whether the pool name already exists,
  // and check whether the pool name is duplicate
  const int64_t POOL_NAME_SET_BUCKET_NUM = 16;
  ObHashSet<share::ObResourcePoolName> pool_name_set;
  if (OB_FAIL(pool_name_set.create(POOL_NAME_SET_BUCKET_NUM))) {
    LOG_WARN("fail to create hash set", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < split_pool_name_list.count(); ++i) {
      const share::ObResourcePoolName &pool_name = split_pool_name_list.at(i);
      share::ObResourcePool *pool = NULL;
      int tmp_ret = inner_get_resource_pool_by_name(pool_name, pool);
      if (OB_ENTRY_NOT_EXIST == tmp_ret) {
        // good, go on next, this pool name not exist
      } else if (OB_SUCCESS == tmp_ret) {
        if (NULL == pool) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("pool is null", K(ret), KP(pool), K(pool_name));
        } else {
          ret = OB_RESOURCE_POOL_EXIST;
          LOG_WARN("resource pool already exist", K(ret), K(pool_name));
          ObCStringHelper helper;
          LOG_USER_ERROR(OB_RESOURCE_POOL_EXIST, helper.convert(pool_name));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error when get pool by name", K(ret), K(pool_name));
      }
      if (OB_SUCC(ret)) {
        int tmp_ret = pool_name_set.exist_refactored(pool_name);
        if (OB_HASH_NOT_EXIST == tmp_ret) {
          if (OB_FAIL(pool_name_set.set_refactored(pool_name))) {
            LOG_WARN("fail to set", K(ret), K(pool_name));
          }
        } else if (OB_HASH_EXIST == tmp_ret) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid argument to split resource pool with duplicated name");
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "resource pool name");
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected error when set hashset", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObUnitManager::check_split_pool_zone_condition(
    const common::ObIArray<common::ObZone> &split_zone_list,
    const share::ObResourcePool &pool)
{
  int ret = OB_SUCCESS;
  const common::ObIArray<common::ObZone> &pool_zone_list = pool.zone_list_;
  // Check whether the zone is included in the pool zone list,
  // and check whether the zone is duplicated
  const int64_t ZONE_SET_BUCKET_NUM = 16;
  ObHashSet<common::ObZone> zone_set;
  if (pool_zone_list.count() != split_zone_list.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument to split zone list", K(ret));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "corresponding zone vector");
  } else if (OB_FAIL(zone_set.create(ZONE_SET_BUCKET_NUM))) {
    LOG_WARN("fail to create hash set", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < split_zone_list.count(); ++i) {
      const common::ObZone &this_zone = split_zone_list.at(i);
      if (!has_exist_in_array(pool_zone_list, this_zone)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument to non-exist zone in splitting zone vector", K(ret));
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "non-exist zone in corresponding zone vector");
      } else {
        int tmp_ret = zone_set.exist_refactored(this_zone);
        if (OB_HASH_NOT_EXIST == tmp_ret) {
          if (OB_FAIL(zone_set.set_refactored(this_zone))) {
            LOG_WARN("fail to set", K(ret), K(this_zone));
          }
        } else if (OB_HASH_EXIST == tmp_ret) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid argument to duplicate zones in splitting zone vector", K(ret));
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "duplidate zones in corresponding zone vector");
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected error when set hashset", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObUnitManager::split_resource_pool(
    const share::ObResourcePoolName &pool_name,
    const common::ObIArray<common::ObString> &split_pool_list,
    const common::ObIArray<common::ObZone> &split_zone_list)
{
  int ret = OB_SUCCESS;
  LOG_INFO("start split resource pool", K(pool_name), K(split_pool_list), K(split_zone_list));
  SpinWLockGuard guard(lock_);
  share::ObResourcePool *pool = NULL;
  common::ObArray<share::ObResourcePoolName> split_pool_name_list;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(ret), K(inited_), K(loaded_));
  } else if (pool_name.is_empty()
             || split_pool_list.count() <= 0
             || split_zone_list.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("resource pool name is empty", K(ret), K(split_zone_list), K(split_pool_list));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "resource pool");
  } else if (OB_FAIL(inner_get_resource_pool_by_name(pool_name, pool))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("get resource pool by name failed", K(ret), K(pool_name));
    } else {
      ret = OB_RESOURCE_POOL_NOT_EXIST;
      LOG_WARN("resource pool not exist", K(ret), K(pool_name));
      ObCStringHelper helper;
      LOG_USER_ERROR(OB_RESOURCE_POOL_NOT_EXIST, helper.convert(pool_name));
    }
  } else if (OB_UNLIKELY(NULL == pool)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pool ptr is null", K(ret));
  } else if (OB_FAIL(convert_pool_name_list(split_pool_list, split_pool_name_list))) {
    LOG_WARN("fail to convert pool name list", K(ret));
  } else if (OB_FAIL(check_split_pool_name_condition(split_pool_name_list))) {
    LOG_WARN("fail to check pool list name duplicate", K(ret));
  } else if (split_pool_name_list.count() != split_zone_list.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument to split pool and zone count", K(ret));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "split pool and zone count");
  } else if (OB_FAIL(check_split_pool_zone_condition(split_zone_list, *pool))) {
    LOG_WARN("fail to check split pool zone condition", K(ret));
  } else if (OB_FAIL(do_split_resource_pool(
          pool, split_pool_name_list, split_zone_list))) {
    LOG_WARN("fail to do split resource pool", K(ret));
  } else {
    LOG_INFO("succeed to split resource pool", K(pool_name),
             "new_pool_name", split_pool_list,
             "corresponding_zone", split_zone_list);
  }
  return ret;
}

int ObUnitManager::do_split_pool_persistent_info(
    share::ObResourcePool *pool,
    const common::ObIArray<share::ObResourcePoolName> &split_pool_name_list,
    const common::ObIArray<common::ObZone> &split_zone_list,
    common::ObIArray<share::ObResourcePool *> &allocate_pool_ptrs)
{
  int ret = OB_SUCCESS;
  share::ObUnitTableTransaction trans;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(ret), K(inited_), K(loaded_));
  } else if (OB_FAIL(trans.start(proxy_, OB_SYS_TENANT_ID))) {
    LOG_WARN("fail to start transaction", K(ret));
  } else if (OB_UNLIKELY(split_zone_list.count() != split_pool_name_list.count()
                         || NULL == pool)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    // Write down the resource pool allocated during execution
    allocate_pool_ptrs.reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < split_pool_name_list.count(); ++i) {
      const share::ObResourcePoolName &new_pool_name = split_pool_name_list.at(i);
      const common::ObZone &zone = split_zone_list.at(i);
      share::ObResourcePool *new_pool = NULL;
      if (NULL == (new_pool = pool_allocator_.alloc())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret));
      } else if (OB_FAIL(allocate_pool_ptrs.push_back(new_pool))) {
        LOG_WARN("fail to push back", K(ret));
      } else if (OB_FAIL(fill_splitting_pool_basic_info(new_pool_name, new_pool, zone, pool))) {
        LOG_WARN("fail to fill splitting pool basic info", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < allocate_pool_ptrs.count(); ++i) {
      share::ObResourcePool *new_pool = allocate_pool_ptrs.at(i);
      if (OB_UNLIKELY(NULL == new_pool)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pool ptr is null", K(ret));
      } else if (new_pool->zone_list_.count() != 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("zone list count unexpected", K(ret), "zone_list", new_pool->zone_list_);
      } else if (OB_FAIL(ut_operator_.update_resource_pool(trans, *new_pool))) {
        LOG_WARN("fail to update resource pool", K(ret));
      } else if (OB_FAIL(split_pool_unit_persistent_info(
              trans, new_pool->zone_list_.at(0), new_pool, pool))) {
        LOG_WARN("fail to split pool unit persistent info", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ut_operator_.remove_resource_pool(trans, pool->resource_pool_id_))) {
      LOG_WARN("fail to remove resource pool persistent info", K(ret),
               "pool_id", pool->resource_pool_id_);
    } else {} // all persistent infos update finished
    const bool commit = (OB_SUCCESS == ret);
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(commit))) {
      LOG_WARN("fail to end trans", K(tmp_ret), K(commit));
      ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
    }
    if (OB_FAIL(ret)) {
      // 1. The transaction did fail to commit.
      //    the internal table and the memory state remain the same,
      //    as long as the memory of allocate_pool_ptrs is released.
      //
      // 2. The transaction submission timed out, but the final submission was successful.
      //    The internal table and memory state are inconsistent,
      //    and the outer layer will call the reload unit manager.
      //    Still need to release the memory of allocate_pool_ptrs
      for (int64_t i = 0; i < allocate_pool_ptrs.count(); ++i) {
        share::ObResourcePool *new_pool = allocate_pool_ptrs.at(i);
        if (NULL != new_pool) {
          pool_allocator_.free(new_pool);
          new_pool = NULL;
        }
      }
    }
  }
  return ret;
}

int ObUnitManager::do_split_pool_inmemory_info(
    share::ObResourcePool *pool,
    common::ObIArray<share::ObResourcePool *> &allocate_pool_ptrs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(ret), K(inited_), K(loaded_));
  } else if (NULL == pool) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < allocate_pool_ptrs.count(); ++i) {
      share::ObResourcePool *new_pool = allocate_pool_ptrs.at(i);
      if (OB_UNLIKELY(NULL == new_pool)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pool ptr is null", K(ret));
      } else if (new_pool->zone_list_.count() != 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("zone list count unexpected", K(ret), "zone_list", new_pool->zone_list_);
      } else if (OB_FAIL(inc_config_ref_count(new_pool->unit_config_id_))) {
        LOG_WARN("fail to inc config ref count", K(ret),
                 "unit_config_id", new_pool->unit_config_id_);
      } else if (OB_FAIL(insert_config_pool(new_pool->unit_config_id_, new_pool))) {
        LOG_WARN("fail to insert config pool", K(ret),
                 "unit_config_id", new_pool->unit_config_id_);
      } else if (OB_FAIL(update_pool_map(new_pool))) {
        LOG_WARN("fail to update pool map", K(ret),
                 "resource_pool_id", new_pool->resource_pool_id_);
      } else if (!new_pool->is_granted_to_tenant()) {
        // bypass
      } else if (OB_FAIL(insert_tenant_pool(new_pool->tenant_id_, new_pool))) {
        LOG_WARN("fail to insert tenant pool", K(ret), "tenant_id", new_pool->tenant_id_);
      }
      if (OB_FAIL(ret)) {
        // failed
      } else if (OB_FAIL(split_pool_unit_inmemory_info(
              new_pool->zone_list_.at(0), new_pool, pool))) {
        LOG_WARN("fail to split pool unit inmemory info", K(ret));
      } else {} // no more
    }
    common::ObArray<share::ObUnit *> *pool_units = NULL;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(dec_config_ref_count(pool->unit_config_id_))) {
      LOG_WARN("fail to dec config ref count", K(ret));
    } else if (OB_FAIL(delete_config_pool(pool->unit_config_id_, pool))) {
      LOG_WARN("fail to delete config pool", K(ret));
    } else if (OB_FAIL(delete_resource_pool(pool->resource_pool_id_, pool->name_))) {
      LOG_WARN("fail to delete resource pool", K(ret));
    } else if (!pool->is_granted_to_tenant()) {
      // bypass
    } else if (OB_FAIL(delete_tenant_pool(pool->tenant_id_, pool))) {
      LOG_WARN("fail to delete tenant pool", K(ret), "tenant_id", pool->tenant_id_);
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(get_units_by_pool(pool->resource_pool_id_, pool_units))) {
      LOG_WARN("fail to get units by pool", K(ret), "pool_id", pool->resource_pool_id_);
    } else if (OB_UNLIKELY(NULL == pool_units)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pool units ptr is null", K(ret));
    } else if (OB_FAIL(pool_unit_map_.erase_refactored(pool->resource_pool_id_))) {
      LOG_WARN("fail to erase map", K(ret), "pool_id", pool->resource_pool_id_);
    } else {
      pool_unit_allocator_.free(pool_units);
      pool_units = NULL;
      pool_allocator_.free(pool);
      pool = NULL;
    }
    if (OB_FAIL(ret)) {
      // reload
      rootserver::ObRootService *root_service = NULL;
      if (OB_UNLIKELY(NULL == (root_service = GCTX.root_service_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("rootservice is null", K(ret));
      } else {
        int tmp_ret = root_service->submit_reload_unit_manager_task();
        if (OB_SUCCESS != tmp_ret) {
          LOG_ERROR("fail to reload unit manager", K(tmp_ret));
        }
      }
    }
  }
  return ret;
}

int ObUnitManager::do_split_resource_pool(
    share::ObResourcePool *pool,
    const common::ObIArray<share::ObResourcePoolName> &split_pool_name_list,
    const common::ObIArray<common::ObZone> &split_zone_list)
{
  int ret = OB_SUCCESS;
  common::ObArray<share::ObResourcePool *> allocate_pool_ptrs;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(ret), K(inited_), K(loaded_));
  } else if (OB_UNLIKELY(NULL == pool)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (split_pool_name_list.count() != split_zone_list.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("split pool name and zone count not match",
             K(ret), K(split_pool_name_list), K(split_zone_list));
  } else if (OB_FAIL(do_split_pool_persistent_info(
          pool, split_pool_name_list, split_zone_list, allocate_pool_ptrs))) {
    LOG_WARN("fail to do split pool persistent info", K(ret));
  } else if (OB_FAIL(do_split_pool_inmemory_info(pool, allocate_pool_ptrs))) {
    LOG_WARN("fail to do split pool inmemory info", K(ret));
  } else {} // no more
  return ret;
}

int ObUnitManager::fill_splitting_pool_basic_info(
    const share::ObResourcePoolName &new_pool_name,
    share::ObResourcePool *new_pool,
    const common::ObZone &zone,
    share::ObResourcePool *orig_pool)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(ret), K(inited_), K(loaded_));
  } else if (OB_UNLIKELY(new_pool_name.is_empty() || NULL == new_pool
                         || zone.is_empty() || NULL == orig_pool)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(new_pool_name), KP(new_pool), K(zone), K(orig_pool));
  } else {
    new_pool->name_ = new_pool_name;
    new_pool->unit_count_ = orig_pool->unit_count_;
    new_pool->unit_config_id_ = orig_pool->unit_config_id_;
    new_pool->tenant_id_ = orig_pool->tenant_id_;
    new_pool->replica_type_ = orig_pool->replica_type_;
    if (OB_FAIL(new_pool->zone_list_.push_back(zone))) {
      LOG_WARN("fail to push back to zone list", K(ret));
    } else if (OB_FAIL(fetch_new_resource_pool_id(new_pool->resource_pool_id_))) {
      LOG_WARN("fail to fetch new resource pool id", K(ret));
    } else {} // finish fill splitting pool basic info
  }
  return ret;
}

int ObUnitManager::split_pool_unit_inmemory_info(
    const common::ObZone &zone,
    share::ObResourcePool *new_pool,
    share::ObResourcePool *orig_pool)
{
  int ret = OB_SUCCESS;
  common::ObArray<share::ObUnit *> *new_units = NULL;
  common::ObArray<share::ObUnit *> *orig_units = NULL;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(ret), K(inited_), K(loaded_));
  } else if (OB_UNLIKELY(NULL == new_pool || NULL == orig_pool || zone.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(new_pool), K(orig_pool), K(zone));
  } else if (OB_FAIL(get_units_by_pool(orig_pool->resource_pool_id_, orig_units))) {
    LOG_WARN("fail to get units by pool", K(ret));
  } else if (OB_UNLIKELY(NULL == orig_units)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("units ptr is null", K(ret));
  } else {
    ret = get_units_by_pool(new_pool->resource_pool_id_, new_units);
    if (OB_SUCCESS == ret) {
      // got new units
    } else if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("fail to get units by pool", K(ret));
    } else {
      if (NULL == (new_units = pool_unit_allocator_.alloc())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret));
      } else if (OB_FAIL(pool_unit_map_.set_refactored(new_pool->resource_pool_id_, new_units))) {
        LOG_WARN("fail to set refactored", K(ret), "pool_id", new_pool->resource_pool_id_);
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < orig_units->count(); ++i) {
      share::ObUnit *unit = orig_units->at(i);
      if (OB_UNLIKELY(NULL == unit)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit ptr is null", K(ret));
      } else if (zone != unit->zone_) {
        // bypass
      } else if (NULL == new_units) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("new units ptr is null", K(ret));
      } else {
        unit->resource_pool_id_ = new_pool->resource_pool_id_;
        if (OB_FAIL(new_units->push_back(unit))) {
          LOG_WARN("fail to push back", K(ret));
        } else if (OB_FAIL(update_unit_load(unit, new_pool))) {
          LOG_WARN("fail to update unit load", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObUnitManager::split_pool_unit_persistent_info(
    common::ObMySQLTransaction &trans,
    const common::ObZone &zone,
    share::ObResourcePool *new_pool,
    share::ObResourcePool *orig_pool)
{
  int ret = OB_SUCCESS;
  common::ObArray<share::ObUnit *> *units = NULL;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(ret), K(inited_), K(loaded_));
  } else if (OB_UNLIKELY(NULL == new_pool || NULL == orig_pool || zone.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(new_pool), KP(orig_pool), K(zone));
  } else if (OB_FAIL(get_units_by_pool(orig_pool->resource_pool_id_, units))) {
    LOG_WARN("fail to get units by pool", K(ret));
  } else if (OB_UNLIKELY(NULL == units)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("units ptr is null", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < units->count(); ++i) {
      const share::ObUnit *unit = units->at(i);
      if (OB_UNLIKELY(NULL == unit)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit ptr is null", K(ret));
      } else if (unit->zone_ != zone) {
        // bypass
      } else {
        share::ObUnit new_unit = *unit;
        new_unit.resource_pool_id_ = new_pool->resource_pool_id_;
        if (OB_FAIL(ut_operator_.update_unit_exclude_ug_id(trans, new_unit))) {
          LOG_WARN("fail to update unit", K(ret), K(new_unit));
        } else {
          ROOTSERVICE_EVENT_ADD("unit", "split_pool",
                                "unit_id", unit->unit_id_,
                                "server", unit->server_,
                                "prev_pool_id", orig_pool->resource_pool_id_,
                                "curr_pool_id", new_pool->resource_pool_id_);
        }
      }
    }
  }
  return ret;
}

int ObUnitManager::register_alter_resource_tenant_unit_num_rs_job(
    const uint64_t tenant_id,
    const int64_t new_unit_num,
    const common::ObString &sql_text,
    common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    int64_t job_id = 0;
    if (OB_FAIL(cancel_alter_resource_tenant_unit_num_rs_job(tenant_id, trans))) {
      LOG_WARN("fail to execute cancel_alter_resource_tenant_unit_num_rs_job",
          KR(ret), K(tenant_id), K(sql_text));
    } else {
      ret = create_alter_resource_tenant_unit_num_rs_job(tenant_id,
          new_unit_num, job_id, sql_text, trans);
      FLOG_INFO("[ALTER_RESOURCE_TENANT_UNIT_NUM NOTICE] create a new rs job",
          KR(ret), K(tenant_id), K(sql_text), K(job_id));
    }
  }
  return ret;
}

int ObUnitManager::find_alter_resource_tenant_unit_num_rs_job(
    const uint64_t tenant_id,
    int64_t &job_id,
    common::ObISQLClient &sql_proxy)
{
  int ret = OB_SUCCESS;
  ret = RS_JOB_FIND(
      ALTER_RESOURCE_TENANT_UNIT_NUM,
      job_id,
      sql_proxy,
      "tenant_id", tenant_id);
  return ret;
}

int ObUnitManager::cancel_alter_resource_tenant_unit_num_rs_job(
  const uint64_t tenant_id,
  common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  int64_t job_id = 0;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(find_alter_resource_tenant_unit_num_rs_job(tenant_id, job_id, trans))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      FLOG_INFO("[ALTER_RESOURCE_TENANT_UNIT_NUM NOTICE] there is no rs job in table",KR(ret), K(tenant_id));
    } else {
      LOG_WARN("fail to execute find_alter_resource_tenant_unit_num_rs_job", KR(ret), K(tenant_id));
    }
  } else {
    ret = RS_JOB_COMPLETE(job_id, OB_CANCELED, trans);
    FLOG_INFO("[ALTER_RESOURCE_TENANT_UNIT_NUM NOTICE] cancel an inprogress rs job", KR(ret),
        K(tenant_id), K(job_id));
  }
  return ret;
}

int ObUnitManager::create_alter_resource_tenant_unit_num_rs_job(
    const uint64_t tenant_id,
    const int64_t new_unit_num,
    int64_t &job_id,
    const common::ObString &sql_text,
    common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  job_id = 0;
  const int64_t extra_info_len = common::MAX_ROOTSERVICE_JOB_EXTRA_INFO_LENGTH;
  HEAP_VAR(char[extra_info_len], extra_info) {
    memset(extra_info, 0, extra_info_len);
    int64_t pos = 0;
    share::schema::ObSchemaGetterGuard schema_guard;
    const ObSimpleTenantSchema *tenant_schema;
    // TODO: temp extra info
    if (OB_FAIL(databuff_printf(extra_info, extra_info_len, pos,
            "new unit num: '%ld'", new_unit_num))) {
      if (OB_SIZE_OVERFLOW == ret) {
        LOG_WARN("format to buff size overflow", K(ret));
      } else {
        LOG_WARN("format new unit num failed", K(ret));
      }
    } else if (OB_ISNULL(schema_service_)) {
      ret = OB_NOT_INIT;
      LOG_WARN("schema service is null", KR(ret), KP(schema_service_));
    } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("fail to get tenant schema guard", KR(ret), K(tenant_id));
    } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_schema))) {
      LOG_WARN("fail to get tenant info", K(ret), K(tenant_id));
    } else if (OB_ISNULL(tenant_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant schema is null", KR(ret), KP(tenant_schema));
    } else if (OB_FAIL(RS_JOB_CREATE_WITH_RET(
        job_id,
        ObRsJobType::JOB_TYPE_ALTER_RESOURCE_TENANT_UNIT_NUM,
        trans,
        "tenant_id", tenant_id,
        "tenant_name", tenant_schema->get_tenant_name(),
        "sql_text", ObHexEscapeSqlStr(sql_text),
        "extra_info", ObHexEscapeSqlStr(extra_info)))) {
      LOG_WARN("fail to create rs job", KR(ret), K(tenant_id));
    }
  }
  return ret;
}

int ObUnitManager::get_to_be_deleted_units_in_homo_mode(
    const uint64_t tenant_id,
    const PoolOperationInfo &pool_op_info,
    const common::ObIArray<uint64_t> &deleted_unit_group_id_array,
    common::ObIArray<share::ObUnit> &to_be_deleted_units)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) ||
      pool_op_info.new_unit_num_ >= pool_op_info.old_unit_num_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(pool_op_info));
  } else if (has_duplicates(deleted_unit_group_id_array)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("delete same unit group more than once not supported", KR(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "delete same unit group more than once");
  } else {
    ObArray<ObUnit> tenant_units; // get units from inner-table for accurate unit_group_id_
    common::ObArray<uint64_t> all_unit_group_id_array;
    common::ObArray<uint64_t> to_be_deleted_unit_group;
    const int64_t to_be_deleted_num = pool_op_info.old_unit_num_ - pool_op_info.new_unit_num_;
    if (OB_FAIL(get_tenant_units_and_unit_group_ids_(*proxy_, tenant_id, tenant_units, all_unit_group_id_array))) {
      LOG_WARN("fail to get all unit group id", KR(ret), K(tenant_id));
    } else if (to_be_deleted_num > all_unit_group_id_array.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unit num to match", KR(ret), K(tenant_id), K(pool_op_info),
               "tenant_unit_group_num", all_unit_group_id_array.count());
    } else if (deleted_unit_group_id_array.count() <= 0) {
      // user has not specified unit groups to delete, we choose automatically.
      ObArray<share::ObUnit*> sorted_tenant_unit_ptrs;
      FOREACH_CNT_X(unit, tenant_units, OB_SUCC(ret)) {
        if (OB_FAIL(sorted_tenant_unit_ptrs.push_back(unit))) {
          LOG_WARN("fail to push back", KR(ret), KPC(unit));
        }
      }
      // sort units, inactive server units and non-gts standalone units taks priority to delete.
      ObArray<uint64_t> gts_unit_ids;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(get_tenant_gts_unit_ids(tenant_id, gts_unit_ids))) {
        LOG_WARN("fail to get tenant gts unit ids", KR(ret), K(tenant_id));
      } else {
        DeleteUnitPtrCmp cmp(gts_unit_ids);
        lib::ob_sort(sorted_tenant_unit_ptrs.begin(), sorted_tenant_unit_ptrs.end(), cmp);
        if (OB_FAIL(cmp.ret_)) {
          LOG_WARN("fail to sort", KR(ret));
        }
      }
      ARRAY_FOREACH_X(sorted_tenant_unit_ptrs, i, cnt, OB_SUCC(ret)
          && to_be_deleted_unit_group.count() < to_be_deleted_num) {
        const share::ObUnit *unit = sorted_tenant_unit_ptrs.at(i);
        CK(OB_NOT_NULL(unit));
        if (FAILEDx(add_var_to_array_no_dup(to_be_deleted_unit_group, unit->unit_group_id_))) {
          LOG_WARN("fail to push unit_group_id to to_be_deleted_unit_group", KR(ret), KPC(unit));
        }
      }
      LOG_INFO("Automatically determined on unit_group to delete for shrinking tenant unit num.",
               KR(ret), K(tenant_id), K(pool_op_info), K(to_be_deleted_unit_group));
    } else if (deleted_unit_group_id_array.count() == to_be_deleted_num) {
      // the deleted unit groups are specified by the client. Check specified ug_ids belong to this tenant.
      ARRAY_FOREACH(deleted_unit_group_id_array, i) {
        if (!has_exist_in_array(all_unit_group_id_array, deleted_unit_group_id_array.at(i))) {
          ret = OB_OP_NOT_ALLOW;
          LOG_USER_ERROR(OB_OP_NOT_ALLOW, "delete unit group which does not belong to this tenant");
        }
      }
      if (FAILEDx(to_be_deleted_unit_group.assign(deleted_unit_group_id_array))) {
        LOG_WARN("failed to assign to_be_deleted_unit_group", KR(ret));
      }
    } else {
      // deleted unit groups num is not match to the specified num.
      ret = OB_OP_NOT_ALLOW;
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "to be deleted unit num not match");
    }

    // extract to_be_deleted_units by to_be_deleted_unit_group
    FOREACH_CNT_X(unit, tenant_units, OB_SUCC(ret)) {
      if (has_exist_in_array(to_be_deleted_unit_group, unit->unit_group_id_)) {
        if (OB_FAIL(to_be_deleted_units.push_back(*unit))) {
          LOG_WARN("fail to push back", KR(ret), KPC(unit));
        }
      }
    }
  }
  return ret;
}

int ObUnitManager::determine_new_added_unit_status(
    const share::ObResourcePool &pool,
    share::ObUnit::Status &unit_status)
{
  int ret = OB_SUCCESS;
  if (!pool.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid pool", KR(ret), K(pool));
  } else if (!pool.is_granted_to_tenant()) {
    unit_status = share::ObUnit::UNIT_STATUS_ACTIVE;
  } else {
    uint64_t data_version = 0;
    if (OB_FAIL(GET_MIN_DATA_VERSION(OB_SYS_TENANT_ID, data_version))) {
      LOG_WARN("fail to get sys tenant data version", KR(ret), K(data_version));
    } else {
      if (DATA_VERSION_4_2_5_5 > data_version) {
        unit_status = ObUnit::UNIT_STATUS_ACTIVE;
        LOG_INFO("sys data version below 4.2.5.5, unit_status can not be ADDING yet, still ACTIVE",
                 KR(ret), K(data_version));
      } else {
        unit_status = ObUnit::UNIT_STATUS_ADDING;
      }
    }
  }
  return ret;
}

int ObUnitManager::rollback_pool_unit_num_(
    common::ObMySQLTransaction &trans,
    share::ObResourcePool *pool,
    const int64_t new_unit_num)
{
  int ret = OB_SUCCESS;
  common::ObArray<share::ObUnit *> *units = nullptr;
  if (OB_UNLIKELY(nullptr == pool)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pool ptr is null", KR(ret));
  } else if (OB_UNLIKELY(!pool->is_granted_to_tenant() || new_unit_num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KPC(pool), K(new_unit_num));
  } else {
    share::ObResourcePool new_pool;
    const uint64_t tenant_id = pool->tenant_id_;
    ObUnit::Status deleting_unit_rollback_status = ObUnit::UNIT_STATUS_MAX;
    if (FAILEDx(new_pool.assign(*pool))) {
      LOG_WARN("fail to assign new pool", KR(ret), KPC(pool));
    } else if (FALSE_IT(new_pool.unit_count_ = new_unit_num)) {
      // shall never be here
    } else if (OB_FAIL(ut_operator_.update_resource_pool(trans, new_pool))) {
      LOG_WARN("fail to update resource pool", KR(ret), K(new_pool));
    } else if (OB_FAIL(get_units_by_pool(pool->resource_pool_id_, units))) {
      LOG_WARN("fail to get units by pool", KR(ret), "pool_id", pool->resource_pool_id_);
    } else if (OB_UNLIKELY(nullptr == units)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("units is null", KR(ret), K(tenant_id), "pool_id", pool->resource_pool_id_);
    } else if (OB_FAIL(determine_new_added_unit_status(*pool, deleting_unit_rollback_status))) {
      LOG_WARN("fail to determine new added unit status", KR(ret), KPC(pool));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < units->count(); ++i) {
        const ObUnit *this_unit = units->at(i);
        if (OB_UNLIKELY(NULL == this_unit)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unit ptr is null", KR(ret));
        } else if (ObUnit::UNIT_STATUS_ACTIVE == this_unit->status_) {
          // go and process the unit in deleting
        } else if (ObUnit::UNIT_STATUS_ADDING == this_unit->status_) {
          ObUnit new_unit = *this_unit;
          new_unit.status_ = ObUnit::UNIT_STATUS_DELETING;
          if (OB_FAIL(ut_operator_.update_unit_exclude_ug_id(trans, new_unit))) {
            LOG_WARN("fail to update unit", K(ret), K(new_unit), "cur_unit", *this_unit);
          }
        } else if (ObUnit::UNIT_STATUS_DELETING == this_unit->status_) {
          ObUnit new_unit = *this_unit;
          new_unit.status_ = deleting_unit_rollback_status;
          if (OB_FAIL(ut_operator_.update_unit_exclude_ug_id(trans, new_unit))) {
            LOG_WARN("fail to update unit", KR(ret), K(new_unit), "cur_unit", *this_unit);
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected unit status", KR(ret), "unit", *this_unit);
        }
      }
    }
    // update pool info and unit status in-memory
    if (OB_SUCC(ret)) {
      pool->unit_count_ = new_unit_num;
      for (int64_t i = 0; OB_SUCC(ret) && i < units->count(); ++i) {
        ObUnit *this_unit = units->at(i);
        if (OB_UNLIKELY(NULL == this_unit)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unit ptr is null", KR(ret));
        } else if (ObUnit::UNIT_STATUS_ACTIVE == this_unit->status_) {
          // go and process the unit in deleting or adding
        } else if (ObUnit::UNIT_STATUS_ADDING == this_unit->status_) {
          this_unit->status_ = ObUnit::UNIT_STATUS_DELETING;
        } else if (ObUnit::UNIT_STATUS_DELETING == this_unit->status_) {
          this_unit->status_ = deleting_unit_rollback_status;
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected unit status", KR(ret), "unit", *this_unit);
        }
      }
    }
  }
  return ret;
}

int ObUnitManager::alter_resource_tenant(
    const uint64_t tenant_id,
    const int64_t new_unit_num,
    const common::ObIArray<uint64_t> &delete_unit_group_id_array,
    const common::ObString &sql_text)
{
  int ret = OB_SUCCESS;
  LOG_INFO("start to alter resource tenant", K(tenant_id));
  SpinWLockGuard guard(lock_);
  // related variables
  common::ObArray<share::ObResourcePool *> *pools = nullptr;
  bool in_hetero_deploy_mode = false;
  bool is_gts_standalone = false;
  share::ObUnitTableTransaction trans;

  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret), K(inited_), K(loaded_));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || new_unit_num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(new_unit_num));
  } else if (OB_FAIL(trans.start(proxy_, OB_SYS_TENANT_ID))) {
    LOG_WARN("fail to start transaction", KR(ret));
  } else if (OB_FAIL(check_tenant_gts_standalone_enabled(tenant_id, is_gts_standalone))) {
    LOG_WARN("fail to check tenant gts standalone enabled", KR(ret), K(tenant_id));
  } else if (is_gts_standalone && new_unit_num < 2) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("tenant enabled gts_standalone, unit_num can not be less than 2", KR(ret), K(tenant_id));
    LOG_USER_ERROR_WITH_ARGS(OB_OP_NOT_ALLOW, "Tenant %lu enabled GTS standalone, unit_num less than 2", tenant_id);
  } else if (OB_FAIL(get_pools_by_tenant_(tenant_id, pools))) {
    LOG_WARN("fail to get pools by tenant", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(nullptr == pools)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pools ptr is null", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_tenant_in_heterogeneous_deploy_mode(tenant_id, in_hetero_deploy_mode))) {
    LOG_WARN("fail to check tenant in hetero deploy mode", KR(ret), K(tenant_id));
  } else {
    // 1. determine all pools alter info
    ObArray<share::ObResourcePool *> altered_pools;
    ObArray<PoolOperationInfo> pool_op_info_array;
    if (OB_FAIL(batch_determine_altered_pools_op_info_(*pools, new_unit_num, altered_pools, pool_op_info_array))) {
      LOG_WARN("fail to batch determine altered pools op info", KR(ret), K(new_unit_num));
    } else if (altered_pools.count() != pool_op_info_array.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("altered pools count not match pool op info array count", KR(ret),
               K(altered_pools.count()), K(pool_op_info_array.count()));
    } else if (!in_hetero_deploy_mode) {
      // if in homo mode, defense check all pool unit_num the same
      if (altered_pools.count() != 0 && altered_pools.count() != pools->count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant pools unit num not expected, neither equal to all pools' count or equal to 0",
                 KR(ret), K(altered_pools.count()), K(pools->count()), K(pool_op_info_array));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < pool_op_info_array.count(); ++i) {
        if (OB_UNLIKELY(pool_op_info_array.at(i) != pool_op_info_array.at(0))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("alter type not same in homo mode", KR(ret), K(pool_op_info_array));
        }
      }
    }

    // 2. process to be deleted unit group
    ObArray<ObArray<uint64_t>> delete_unit_id_arrays;
    if (OB_FAIL(ret)) {
    } else if (in_hetero_deploy_mode) {
      // in hetero mode, do not yet support user specify unit to delete in alter resource tenant command.
      if (!delete_unit_group_id_array.empty()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("attribute delete_unit_group not supported in hetero mode", KR(ret), K(tenant_id), K(delete_unit_group_id_array));
        LOG_USER_ERROR_WITH_ARGS(OB_NOT_SUPPORTED, "Tenant %lu zone_deploy_mode is 'hetero' , not 'homo', "
            "delete unit_group", tenant_id);
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < altered_pools.count(); ++i) {
          if (OB_FAIL(delete_unit_id_arrays.push_back(ObArray<uint64_t>()))) {
            LOG_WARN("fail to push back empty array", KR(ret));
          }
        }
      }
    } else if (OB_FAIL(process_delete_unit_group_in_homo_mode_(tenant_id, altered_pools,
        pool_op_info_array, delete_unit_group_id_array, delete_unit_id_arrays))) {
      LOG_WARN("fail to process delete unit group", KR(ret), K(tenant_id), K(in_hetero_deploy_mode), K(altered_pools),
              K(pool_op_info_array), K(delete_unit_group_id_array));
    }

    // 3. do alter per pool unit_num
    if (OB_SUCC(ret) && !pool_op_info_array.empty()) {
      if (delete_unit_id_arrays.count() != altered_pools.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("delete unit id array count not match", KR(ret), K(delete_unit_id_arrays), K(altered_pools.count()));
      } else if (OB_FAIL(register_alter_resource_tenant_unit_num_rs_job(tenant_id, new_unit_num, sql_text, trans))) {
        LOG_WARN("fail to register alter resource tenant unit num rs job", KR(ret),
            K(tenant_id), K(sql_text), K(new_unit_num));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < altered_pools.count(); ++i) {
        share::ObResourcePool *pool = altered_pools.at(i);
        if (OB_FAIL(do_alter_pool_unit_num_(trans, pool, pool_op_info_array.at(i), delete_unit_id_arrays.at(i)))) {
          LOG_WARN("fail to do_alter_pool_unit_num", KR(ret), KPC(pool), K(pool_op_info_array.at(i)), K(delete_unit_id_arrays));
        }
      }
      // re-adjust unit group id at the end
      if (OB_FAIL(ret)) {
      } else if (in_hetero_deploy_mode) {
        if (OB_FAIL(adjust_altered_pools_unit_group_id_(trans, tenant_id, altered_pools))) {
          LOG_WARN("fail to adjust_altered_pools_unit_group_id", KR(ret), K(tenant_id), K(pool_op_info_array));
        }
      } else {
        if (OB_FAIL(alter_homo_resource_tenant_adjust_ug_(trans, pool_op_info_array.at(0), *pools))) {
          LOG_WARN("fail to adjust unit_group_id", KR(ret), K(pool_op_info_array));
        }
      }
    }
  }

  if (trans.is_started()) {
    const bool commit = (OB_SUCCESS == ret);
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(trans.end(commit))) {
      LOG_WARN("fail to trans end", KR(tmp_ret), K(commit));
      ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
    }
  }
  return ret;
}

int ObUnitManager::batch_determine_altered_pools_op_info_(
    common::ObArray<share::ObResourcePool *> &pools,
    const int64_t new_unit_num,
    ObArray<share::ObResourcePool *> &altered_pools,
    ObArray<ObUnitManager::PoolOperationInfo> &pool_op_info_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(new_unit_num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(new_unit_num));
  } else if (OB_UNLIKELY(pools.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(pools.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < pools.count(); ++i) {
      share::ObResourcePool *pool = pools.at(i);
      AlterUnitNumType alter_unit_num_type = AUN_MAX;
      if (nullptr == pool) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pool is null ptr", KR(ret));
      } else if (OB_FAIL(determine_alter_unit_num_type(pool, new_unit_num, alter_unit_num_type))) {
        LOG_WARN("fail to determine alter unit num type", KR(ret));
      } else if (AUN_MAX == alter_unit_num_type) {
        ret = OB_OP_NOT_ALLOW;
        LOG_USER_ERROR(OB_OP_NOT_ALLOW, "alter unit num while the previous operation is in progress");
      } else if (AUN_NOP == alter_unit_num_type) {
        // no change, skip
      } else {
        if (OB_FAIL(pool_op_info_array.push_back(PoolOperationInfo(pool->unit_count_,
            new_unit_num, alter_unit_num_type, pool->replica_type_)))) {
          LOG_WARN("fail to push back pool operation info", KR(ret), KPC(pool));
        } else if (OB_FAIL(altered_pools.push_back(pool))) {
          LOG_WARN("fail to push back altered pool", KR(ret), KPC(pool));
        }
      }
    }
  }
  return ret;
}

// 1. if homo mode, and OP is not SHRINK, check if delete_unit_group_id_array is empty
// 2. if homo mode, and OP is SHRINK, process delete unit group id, and convert to units
int ObUnitManager::process_delete_unit_group_in_homo_mode_(
    const uint64_t tenant_id,
    const ObIArray<share::ObResourcePool *> &pools,
    const ObIArray<ObUnitManager::PoolOperationInfo> &pool_op_info_array,
    const ObIArray<uint64_t> &delete_unit_group_id_array,  // user specified
    ObIArray<ObArray<uint64_t>> &delete_unit_id_arrays)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || pools.count() != pool_op_info_array.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), "pools_count", pools.count(), K(pool_op_info_array));
  } else if (pool_op_info_array.empty() || AUN_SHRINK != pool_op_info_array.at(0).alter_type_) {
    // not shrinking operation, can not specify unit_group to delete.
    if (!delete_unit_group_id_array.empty()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("do not support deleting unit except for shrink unit num", KR(ret), K(pool_op_info_array));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "unit num is not shrinking, deleting unit");
    } else {
      // fill with empty arrays
      for (int64_t i = 0; OB_SUCC(ret) && i < pools.count(); ++i) {
        if (OB_FAIL(delete_unit_id_arrays.push_back(ObArray<uint64_t>()))) {
          LOG_WARN("fail to push back empty array", KR(ret));
        }
      }
    }
  } else {
    const PoolOperationInfo &pool_op_info = pool_op_info_array.at(0);   // use first op info to represent all
    ObArray<ObUnit> to_be_deleted_units;
    if (OB_FAIL(get_to_be_deleted_units_in_homo_mode(
          tenant_id, pool_op_info, delete_unit_group_id_array, to_be_deleted_units))) {
      LOG_WARN("fail to get to be deleted unit group", KR(ret), K(tenant_id));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < pools.count(); ++i) {
      share::ObResourcePool *pool = pools.at(i);
      ObArray<uint64_t> pool_delete_unit_id_array;
      if (OB_ISNULL(pool)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pool is null", KR(ret));
      }
      FOREACH_CNT_X(unit, to_be_deleted_units, OB_SUCC(ret)) {
        if (unit->resource_pool_id_ == pool->resource_pool_id_
            && OB_FAIL(pool_delete_unit_id_array.push_back(unit->unit_id_))) {
          LOG_WARN("fail to push_back", KR(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (pool_delete_unit_id_array.count() !=
          (pool_op_info.old_unit_num_ - pool_op_info.new_unit_num_) * pool->zone_list_.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pool_delete_unit_id_array count unexpected", KR(ret), K(to_be_deleted_units),
                  KPC(pool), K(pool_op_info), K(pool_delete_unit_id_array));
      } else if (OB_FAIL(delete_unit_id_arrays.push_back(pool_delete_unit_id_array))) {
        LOG_WARN("fail to push_back", KR(ret));
      }
    }
  }
  return ret;
}

// This function only used for adjusting unit_group_id_ when altering resource tenant in HOMO mode,
// and try to keep old ug_ids, only allocate new ug_ids for new expanded units.
int ObUnitManager::alter_homo_resource_tenant_adjust_ug_(
    common::ObMySQLTransaction &trans,
    const ObUnitManager::PoolOperationInfo &pool_op_info,
    common::ObIArray<share::ObResourcePool *> &pools)
{
  int ret = OB_SUCCESS;
  if (AUN_SHRINK == pool_op_info.alter_type_ || AUN_ROLLBACK == pool_op_info.alter_type_) {
    // do nothing, no need to adjust unit group id
  } else if (AUN_EXPAND == pool_op_info.alter_type_) {
    // only allocate new ug_ids for new added units, ug_ids of which are currently 0
    // We have assured that in homo mode, all pools have unit group id. So do not need to check pool align to ug.
    ObArray<uint64_t> new_ug_ids;
    if (OB_ISNULL(pools.at(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null pool ptr", KR(ret));
    } else if (OB_FAIL(ut_operator_.get_pool_unit_group_ids(trans, pools.at(0)->resource_pool_id_, new_ug_ids))) {
      LOG_WARN("fail to get pool ug_ids", KR(ret), KPC(pools.at(0)));
    } else if (OB_UNLIKELY(new_ug_ids.count() != pool_op_info.old_unit_num_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("in homo mode, pool ug_ids does not match unit_count",
               "pool", *pools.at(0), "pool_ug_ids", new_ug_ids);
    } else if (OB_FAIL(batch_fetch_new_unit_group_id(
        pool_op_info.new_unit_num_ - pool_op_info.old_unit_num_, new_ug_ids))) {
      LOG_WARN("fail to batch fetch unit group id", KR(ret), K(pool_op_info));
    } else {
      const share::ObResourcePool *pool = nullptr;
      ARRAY_FOREACH(pools, i) {
        CK(OB_NOT_NULL(pool = pools.at(i)));
        if (FAILEDx(reallocate_pool_unit_group_id_(trans, pool, new_ug_ids))) {
          LOG_WARN("fail to reallocate pool unit group id", KR(ret), KPC(pool), K(new_ug_ids));
        }
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected pool_op_info", KR(ret), K(pool_op_info));
  }
  return ret;
}

int ObUnitManager::merge_resource_pool(
    const common::ObIArray<common::ObString> &old_pool_list,
    const common::ObIArray<common::ObString> &new_pool_list)
{
  int ret = OB_SUCCESS;
  LOG_INFO("start merge resource pool", K(old_pool_list), K(new_pool_list));
  SpinWLockGuard guard(lock_);
  common::ObArray<share::ObResourcePoolName> old_pool_name_list;
  share::ObResourcePoolName merge_pool_name;
  common::ObArray<share::ObResourcePool *> old_pool;//Pool to be merged
  common::ObArray<common::ObZone> merge_zone_list;//zone list to be merged
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(ret), K(inited_), K(loaded_));
  } else if (new_pool_list.count() <= 0
             || old_pool_list.count() <= 0
             || old_pool_list.count() < 2
             //Preventing only one pool from being merged is meaningless
             || new_pool_list.count() > 1) {
    //Can only be merged into one resource pool
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("resource pool zone list is illeagle", K(old_pool_list), K(new_pool_list));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "resource pool");
  } else if (OB_FAIL(convert_pool_name_list(old_pool_list, old_pool_name_list,
                                            new_pool_list, merge_pool_name))) {//1.parse pool name
    LOG_WARN("fail to convert pool name list", K(ret));
  } else if (OB_FAIL(check_old_pool_name_condition(
                     old_pool_name_list, merge_zone_list, old_pool))) {
    //2. check the pool that in old_pool_list is whether valid
    LOG_WARN("fail to check old pool name condition", K(ret));
  } else if (OB_FAIL(check_merge_pool_name_condition(merge_pool_name))) {
    //3. check the merge_pool_name is whether the new pool
    LOG_WARN("fail to check merge pool name condition", K(ret));
  } else if (OB_FAIL(do_merge_resource_pool(merge_pool_name, merge_zone_list, old_pool))) {
    LOG_WARN("fail to do merge resource pool", K(ret));
  } else {
    LOG_INFO("success to merge resource pool", K(merge_pool_name),
             "old_pool_name", old_pool_list);
  }
  return ret;
}

int ObUnitManager::convert_pool_name_list(
    const common::ObIArray<common::ObString> &old_pool_list,
    common::ObIArray<share::ObResourcePoolName> &old_pool_name_list,
    const common::ObIArray<common::ObString> &new_pool_list,
    share::ObResourcePoolName &merge_pool_name)
{
  int ret = OB_SUCCESS;
  old_pool_name_list.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < old_pool_list.count(); ++i) {
    share::ObResourcePoolName pool_name;
    if (OB_FAIL(pool_name.assign(old_pool_list.at(i).ptr()))) {
      LOG_WARN("fail to assign pool name", K(ret));
    } else if (has_exist_in_array(old_pool_name_list, pool_name)) {//Check for duplication
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("not allow merge resource pool repeat", K(ret));
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "merge resource pool repeat");
    } else if (OB_FAIL(old_pool_name_list.push_back(pool_name))) {
      LOG_WARN("fail to push back", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(merge_pool_name.assign(new_pool_list.at(0).ptr()))) {
      LOG_WARN("fail to assign pool name", K(ret));
    }
  }
  return ret;
}

int ObUnitManager::check_merge_pool_name_condition(
    const share::ObResourcePoolName &merge_pool_name)
{
  int ret = OB_SUCCESS;
  // check the pool name is whether exist,
  // and check the pool name is whether duplication.
  share::ObResourcePool *pool = NULL;
  int tmp_ret = inner_get_resource_pool_by_name(merge_pool_name, pool);
  if (OB_ENTRY_NOT_EXIST == tmp_ret) {
    // good, go on next, this pool name not exist
  } else if (OB_SUCCESS == tmp_ret) {
    if (NULL == pool) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pool is null", K(ret), KP(pool), K(merge_pool_name));
    } else {
      ret = OB_RESOURCE_POOL_EXIST;
      LOG_WARN("resource pool already exist", K(ret), K(merge_pool_name));
      ObCStringHelper helper;
      LOG_USER_ERROR(OB_RESOURCE_POOL_EXIST, helper.convert(merge_pool_name));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error when get pool by name", K(ret), K(merge_pool_name));
  }
  return ret;
}

int ObUnitManager::check_old_pool_name_condition(
    common::ObIArray<share::ObResourcePoolName> &old_pool_name_list,
    common::ObIArray<common::ObZone> &merge_zone_list,
    common::ObIArray<share::ObResourcePool*> &old_pool)
{
  int ret = OB_SUCCESS;
  common::ObReplicaType replica_type = REPLICA_TYPE_MAX;
  uint64_t tenant_id = OB_INVALID_ID;
  share::ObUnitConfig *unit_config = NULL;
  int64_t unit_count = 0;
  const int64_t POOL_NAME_SET_BUCKET_NUM = 16;
  ObHashSet<share::ObResourcePoolName> pool_name_set;
  if (OB_FAIL(pool_name_set.create(POOL_NAME_SET_BUCKET_NUM))) {
    LOG_WARN("fail to create hash set", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < old_pool_name_list.count(); ++i) {
      const share::ObResourcePoolName &pool_name = old_pool_name_list.at(i);
      share::ObUnitConfig *this_unit_config = NULL;
      share::ObResourcePool *pool = NULL;
      if (OB_FAIL(inner_get_resource_pool_by_name(pool_name, pool))) {
        LOG_WARN("fail to get resource pool by name", K(ret));
      } else if (OB_ISNULL(pool)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pool is null", K(ret), K(pool), K(pool_name));
      } else {
        if (0 == i) {
          replica_type = pool->replica_type_;
          tenant_id = pool->tenant_id_;
          unit_count = pool->unit_count_;
          if (OB_FAIL(get_unit_config_by_id(pool->unit_config_id_, unit_config))) {
            if (OB_ENTRY_NOT_EXIST == ret) {
              ret = OB_RESOURCE_UNIT_NOT_EXIST;
            }
            LOG_WARN("can not find config for unit",
                     "unit_config_id", pool->unit_config_id_,
                      K(ret));
          } else if (NULL == unit_config) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("config is null", K(*unit_config), K(ret));
          } else {} //no more
        } else {
          if (replica_type != pool->replica_type_) {
            // Determine whether the replica_type is the same
            ret = OB_OP_NOT_ALLOW;
            LOG_WARN("not allow pool replica type different",
                      K(ret), K(replica_type), K(pool->replica_type_));
            LOG_USER_ERROR(OB_OP_NOT_ALLOW, "pool replica type different");
          } else if (tenant_id != pool->tenant_id_) {
            // Determine whether the tenant_id is the same
            ret = OB_OP_NOT_ALLOW;
            LOG_WARN("not allow pool tenant id different",
                      K(ret), K(tenant_id), K(pool->tenant_id_));
            LOG_USER_ERROR(OB_OP_NOT_ALLOW, "pool tenant id different");
          } else if (unit_count != pool->unit_count_) {
            // Determine whether the unit_count is the same
            ret = OB_OP_NOT_ALLOW;
            LOG_WARN("not allow pool unit count different",
                      K(ret), K(unit_count), K(pool->unit_count_));
            LOG_USER_ERROR(OB_OP_NOT_ALLOW, "pool unit count different");
          } else if (OB_FAIL(get_unit_config_by_id(pool->unit_config_id_, this_unit_config))) {
            if (OB_ENTRY_NOT_EXIST == ret) {
              ret = OB_RESOURCE_UNIT_NOT_EXIST;
            }
            LOG_WARN("can not find config for unit",
                     "unit_config_id", pool->unit_config_id_,
                      K(ret));
          } else if (NULL == this_unit_config) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("config is null", KP(this_unit_config), K(ret));
          } else {//Determine whether the unit config is the same
            if (unit_config->unit_config_id() == this_unit_config->unit_config_id()) {//nothing todo
            } else {
              ret = OB_OP_NOT_ALLOW;
              LOG_WARN("not allow pool unit config different",
                        K(ret), K(*this_unit_config), K(*unit_config));
              LOG_USER_ERROR(OB_OP_NOT_ALLOW, "pool unit config different");
            }
          }
        }
        if (OB_SUCC(ret)) {
          int tmp_ret = pool_name_set.exist_refactored(pool_name);
          if (OB_HASH_NOT_EXIST == tmp_ret) {
            if (OB_FAIL(pool_name_set.set_refactored(pool_name))) {
              LOG_WARN("fail to set", K(ret), K(pool_name));
            }
          } else if (OB_HASH_EXIST == tmp_ret) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid argument to merge resource pool duplicate");
            LOG_USER_ERROR(OB_INVALID_ARGUMENT, "resource pool name duplicate");
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected error when set hashset", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          // Judge whether the zone has crossover,
          // it is implicitly judged whether the pool_name is repeated, no need to repeat the judgment
          for (int64_t j = 0; OB_SUCC(ret) && j < pool->zone_list_.count(); ++j) {
            const common::ObZone &this_zone = pool->zone_list_.at(j);
            if (has_exist_in_array(merge_zone_list, this_zone)) {
              ret = OB_OP_NOT_ALLOW;
              LOG_WARN("not allow to merge resource pool with duplicated zone");
              LOG_USER_ERROR(OB_OP_NOT_ALLOW, "resource pool with duplicated zone");
            } else if (OB_FAIL(merge_zone_list.push_back(this_zone))) {
              LOG_WARN("fail to push back", K(ret));
            }
          }
        }
        if (OB_SUCC(ret)) {
          ObArray<ObUnit *> *units = nullptr;
          if (OB_FAIL(get_units_by_pool(pool->resource_pool_id_, units))) {
            LOG_WARN("fail to get units by pool", KR(ret), KPC(pool));
          } else if (OB_UNLIKELY(nullptr == units)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("units ptr is null", KR(ret), KPC(pool));
          } else {
            for (int64_t j = 0; OB_SUCC(ret) && j < units->count(); ++j) {
              ObUnit *unit = units->at(j);
              if (OB_UNLIKELY(nullptr == unit)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unit ptr is null", KR(ret));
              } else if (unit->status_ != ObUnit::UNIT_STATUS_ACTIVE) {
                ret = OB_OP_NOT_ALLOW;
                LOG_WARN("merging pools when any pool in shrinking or expanding not allowed", KR(ret), KPC(pool));
                LOG_USER_ERROR(OB_OP_NOT_ALLOW, "merging pools when any pool in shrinking or expanding");
              }
            }
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(old_pool.push_back(pool))) {
            LOG_WARN("fail to push back", K(ret));
          }
        }
      }
    }
  }
 return ret;
}


int ObUnitManager::do_merge_resource_pool(
    const share::ObResourcePoolName &merge_pool_name,
    const common::ObIArray<common::ObZone> &merge_zone_list,
    common::ObIArray<share::ObResourcePool*> &old_pool)
{
  int ret = OB_SUCCESS;
  share::ObResourcePool *allocate_pool_ptr = nullptr;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(ret), K(inited_), K(loaded_));
  } else if (OB_FAIL(do_merge_pool_persistent_info(
                     allocate_pool_ptr, merge_pool_name,
                     merge_zone_list, old_pool))) {
    LOG_WARN("fail to do merge pool persistent info", K(ret));
  } else if (OB_FAIL(do_merge_pool_inmemory_info(allocate_pool_ptr, old_pool))) {
    LOG_WARN("fail to do merge pool inmemory info", K(ret));
  } else {} //no more
  return ret;
}

int ObUnitManager::do_merge_pool_persistent_info(
    share::ObResourcePool *&allocate_pool_ptr,
    const share::ObResourcePoolName &merge_pool_name,
    const common::ObIArray<common::ObZone> &merge_zone_list,
    const common::ObIArray<share::ObResourcePool*> &old_pool)
{
  int ret = OB_SUCCESS;
  share::ObUnitTableTransaction trans;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(ret), K(inited_), K(loaded_));
  } else if (OB_FAIL(trans.start(proxy_, OB_SYS_TENANT_ID))) {
    LOG_WARN("fail to start transaction", K(ret));
  } else {
    if (NULL == (allocate_pool_ptr = pool_allocator_.alloc())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("fail to alloc memory", K(ret));
    } else if (OB_FAIL(fill_merging_pool_basic_info(allocate_pool_ptr, merge_pool_name,
                                                    merge_zone_list, old_pool))) {
      // The specifications of the pools to be merged are the same, so select the first pool here
      LOG_WARN("fail to fill merging pool basic info", K(ret));
    } else if (OB_FAIL(ut_operator_.update_resource_pool(trans, *allocate_pool_ptr))) {
      LOG_WARN("fail to update resource pool", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < old_pool.count(); ++i) {
        if (OB_FAIL(merge_pool_unit_persistent_info(
                trans, allocate_pool_ptr, old_pool.at(i)))) {
          LOG_WARN("fail to split pool unit persistent info", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; i < old_pool.count() && OB_SUCC(ret); ++i) {
        if (OB_FAIL(ut_operator_.remove_resource_pool(trans, old_pool.at(i)->resource_pool_id_))) {
          LOG_WARN("fail to remove resource pool persistent info", K(ret),
                   "pool_id", old_pool.at(i)->resource_pool_id_);
        } else {} // all persistent infos update finished
      }
    }
    const bool commit = (OB_SUCCESS == ret);
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(commit))) {
      LOG_WARN("fail to end trans", K(tmp_ret), K(commit));
      ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
    }
    if (OB_FAIL(ret)) {
      // 1. The transaction did fail to commit.
      //    the internal table and the memory state remain the same,
      //    as long as the memory of allocate_pool_ptrs is released.
      //
      // 2. The transaction submission timed out, but the final submission was successful.
      //    The internal table and memory state are inconsistent,
      //    and the outer layer will call the reload unit manager.
      //    Still need to release the memory of allocate_pool_ptrs
      if (NULL != allocate_pool_ptr) {
        pool_allocator_.free(allocate_pool_ptr);
        allocate_pool_ptr = NULL;
      }
    }
  }
  return ret;
}

int ObUnitManager::fill_merging_pool_basic_info(
    share::ObResourcePool *&allocate_pool_ptr,
    const share::ObResourcePoolName &merge_pool_name,
    const common::ObIArray<common::ObZone> &merge_zone_list,
    const common::ObIArray<share::ObResourcePool*> &old_pool)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(ret), K(inited_), K(loaded_));
  } else if (OB_UNLIKELY(old_pool.count() <= 1)) {
    //It doesn't make sense to merge only one pool
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  }
  if (OB_SUCC(ret)) {
    share::ObResourcePool *orig_pool = old_pool.at(0);
    if (OB_UNLIKELY(merge_pool_name.is_empty() || NULL == allocate_pool_ptr
                    || NULL == orig_pool)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(merge_pool_name), KP(allocate_pool_ptr), K(orig_pool));
    } else {
      allocate_pool_ptr->name_ = merge_pool_name;
      allocate_pool_ptr->unit_count_ = orig_pool->unit_count_;
      allocate_pool_ptr->unit_config_id_ = orig_pool->unit_config_id_;
      allocate_pool_ptr->tenant_id_ = orig_pool->tenant_id_;
      allocate_pool_ptr->replica_type_ = orig_pool->replica_type_;
      if (OB_FAIL(fetch_new_resource_pool_id(allocate_pool_ptr->resource_pool_id_))) {
        LOG_WARN("fail to fetch new resource pool id", K(ret));
      } else {
        for (int64_t i = 0; i < merge_zone_list.count() && OB_SUCC(ret); ++i) {
          if (OB_UNLIKELY(merge_zone_list.at(i).is_empty())) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid argument", K(ret), K(merge_zone_list.at(i)));
          } else if (OB_FAIL(allocate_pool_ptr->zone_list_.push_back(merge_zone_list.at(i)))) {
            LOG_WARN("fail to push back to zone list", K(ret));
          }
        }
      } // finish fill splitting pool basic info
    }
  }
  return ret;
}

int ObUnitManager::merge_pool_unit_persistent_info(
    common::ObMySQLTransaction &trans,
    share::ObResourcePool *new_pool,
    share::ObResourcePool *orig_pool)
{
  int ret = OB_SUCCESS;
  common::ObArray<share::ObUnit *> *units = NULL;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(ret), K(inited_), K(loaded_));
  } else if (OB_UNLIKELY(NULL == new_pool || NULL == orig_pool)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(new_pool), KP(orig_pool));
  } else if (OB_FAIL(get_units_by_pool(orig_pool->resource_pool_id_, units))) {
    LOG_WARN("fail to get units by pool", K(ret));
  } else if (OB_UNLIKELY(NULL == units)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("units ptr is null", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < units->count(); ++i) {
      const share::ObUnit *unit = units->at(i);
      if (OB_UNLIKELY(NULL == unit)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit ptr is null", K(ret));
      } else {
        share::ObUnit new_unit = *unit;
        new_unit.resource_pool_id_ = new_pool->resource_pool_id_;
        if (OB_FAIL(ut_operator_.update_unit_exclude_ug_id(trans, new_unit))) {
          LOG_WARN("fail to update unit", K(ret), K(new_unit));
        }
        ROOTSERVICE_EVENT_ADD("unit", "merge_pool",
                              "unit_id", unit->unit_id_,
                              "server", unit->server_,
                              "prev_pool_id", orig_pool->resource_pool_id_,
                              "curr_pool_id", new_pool->resource_pool_id_,K(ret));
      }
    }
  }
  return ret;
}

int ObUnitManager::do_merge_pool_inmemory_info(
    share::ObResourcePool *new_pool/*allocate_pool_ptr*/,
    common::ObIArray<share::ObResourcePool*> &old_pool)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(ret), K(inited_), K(loaded_));
  } else if (NULL == new_pool) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    if (OB_FAIL(inc_config_ref_count(new_pool->unit_config_id_))) {
      LOG_WARN("fail to inc config ref count", K(ret),
               "unit_config_id", new_pool->unit_config_id_);
    } else if (OB_FAIL(insert_config_pool(new_pool->unit_config_id_, new_pool))) {
      LOG_WARN("fail to insert config pool", K(ret),
               "unit_config_id", new_pool->unit_config_id_);
    } else if (OB_FAIL(update_pool_map(new_pool))) {
      LOG_WARN("fail to update pool map", K(ret),
               "resource_pool_id", new_pool->resource_pool_id_);
    } else if (!new_pool->is_granted_to_tenant()) {
      // bypass
    } else if (OB_FAIL(insert_tenant_pool(new_pool->tenant_id_, new_pool))) {
      LOG_WARN("fail to insert tenant pool", K(ret), "tenant_id", new_pool->tenant_id_);
    }
    if (OB_FAIL(ret)) {
      // failed
    } else if (OB_FAIL(merge_pool_unit_inmemory_info(new_pool, old_pool))) {
      LOG_WARN("fail to split pool unit inmemory info", K(ret));
    } else {} // no more
    for (int64_t i = 0 ; i < old_pool.count() && OB_SUCC(ret); ++i) {
      common::ObArray<share::ObUnit *> *pool_units = NULL;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(dec_config_ref_count(old_pool.at(i)->unit_config_id_))) {
        LOG_WARN("fail to dec config ref count", K(ret));
      } else if (OB_FAIL(delete_config_pool(old_pool.at(i)->unit_config_id_, old_pool.at(i)))) {
        LOG_WARN("fail to delete config pool", K(ret));
      } else if (OB_FAIL(delete_resource_pool(old_pool.at(i)->resource_pool_id_,
                                              old_pool.at(i)->name_))) {
        LOG_WARN("fail to delete resource pool", K(ret));
      } else if (OB_INVALID_ID == old_pool.at(i)->tenant_id_) {
        // bypass
      } else if (OB_FAIL(delete_tenant_pool(old_pool.at(i)->tenant_id_, old_pool.at(i)))) {
        LOG_WARN("fail to delete tenant pool", K(ret), "tenant_id", old_pool.at(i)->tenant_id_);
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(get_units_by_pool(old_pool.at(i)->resource_pool_id_, pool_units))) {
        LOG_WARN("fail to get units by pool", K(ret), "pool_id", old_pool.at(i)->resource_pool_id_);
      } else if (OB_UNLIKELY(NULL == pool_units)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pool units ptr is null", K(ret));
      } else if (OB_FAIL(pool_unit_map_.erase_refactored(old_pool.at(i)->resource_pool_id_))) {
        LOG_WARN("fail to erase map", K(ret), "pool_id", old_pool.at(i)->resource_pool_id_);
      } else {
        pool_unit_allocator_.free(pool_units);
        pool_units = NULL;
        pool_allocator_.free(old_pool.at(i));
        old_pool.at(i) = NULL;
      }
    }
    if (OB_FAIL(ret)) {
      // reload
      rootserver::ObRootService *root_service = NULL;
      if (OB_UNLIKELY(NULL == (root_service = GCTX.root_service_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("rootservice is null", K(ret));
      } else {
        int tmp_ret = root_service->submit_reload_unit_manager_task();
        if (OB_SUCCESS != tmp_ret) {
          LOG_ERROR("fail to reload unit manager", K(tmp_ret));
        }
      }
    }
  }
  return ret;
}

int ObUnitManager::merge_pool_unit_inmemory_info(
    share::ObResourcePool *new_pool/*allocate_pool_ptr*/,
    common::ObIArray<share::ObResourcePool*> &old_pool)
{
  int ret = OB_SUCCESS;
  common::ObArray<share::ObUnit *> *new_units = NULL;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(ret), K(inited_), K(loaded_));
  } else if (OB_UNLIKELY(NULL == new_pool)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(new_pool));
  } else {
    ret = get_units_by_pool(new_pool->resource_pool_id_, new_units);
    if (OB_SUCCESS == ret) {
      // got new units
    } else if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("fail to get units by pool", K(ret));
    } else {
      if (NULL == (new_units = pool_unit_allocator_.alloc())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret));
      } else if (OB_FAIL(pool_unit_map_.set_refactored(new_pool->resource_pool_id_, new_units))) {
        LOG_WARN("fail to set refactored", K(ret), "pool_id", new_pool->resource_pool_id_);
      }
    }
    for (int64_t i = 0; i < old_pool.count() && OB_SUCC(ret); ++i) {
      common::ObArray<share::ObUnit *> *orig_units = NULL;
      if (OB_ISNULL(old_pool.at(i))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(ret), KP(old_pool.at(i)));
      } else if (OB_FAIL(get_units_by_pool(old_pool.at(i)->resource_pool_id_, orig_units))) {
        LOG_WARN("fail to get units by pool", K(ret));
      } else if (OB_UNLIKELY(NULL == orig_units)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("units ptr is null", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < orig_units->count(); ++i) {
          share::ObUnit *unit = orig_units->at(i);
          if (OB_UNLIKELY(NULL == unit)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unit ptr is null", K(ret));
          } else if (NULL == new_units) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("new units ptr is null", K(ret));
          } else {
            unit->resource_pool_id_ = new_pool->resource_pool_id_;
            if (OB_FAIL(new_units->push_back(unit))) {
              LOG_WARN("fail to push back", K(ret));
            } else if (OB_FAIL(update_unit_load(unit, new_pool))) {
              LOG_WARN("fail to update unit load", K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObUnitManager::alter_resource_pool(const share::ObResourcePool &alter_pool,
                                       const ObUnitConfigName &config_name,
                                       const common::ObIArray<uint64_t> &delete_unit_id_array,
                                       const common::ObString &sql_text)
{
  int ret = OB_SUCCESS;
  LOG_INFO("start alter resource pool", K(alter_pool), K(config_name), K(delete_unit_id_array));
  SpinWLockGuard guard(lock_);
  share::ObResourcePool *pool = NULL;
  share::ObResourcePool  pool_bak;
  // don't invoke alter_pool.is_valid() here, alter_pool.unit_count may be 0
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(inited_), K(loaded_), K(ret));
  } else if (alter_pool.name_.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "resource pool name");
    LOG_WARN("resource pool name is empty", "resource pool name", alter_pool.name_, K(ret));
  } else if (alter_pool.unit_count_ < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "resource unit num");
    LOG_WARN("invalid resource pool unit num", "unit num", alter_pool.unit_count_, K(ret));
  } else if (OB_FAIL(inner_get_resource_pool_by_name(alter_pool.name_, pool))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("get resource pool by name failed", "resource_pool name", alter_pool.name_, K(ret));
    } else {
      ret = OB_RESOURCE_POOL_NOT_EXIST;
      ObCStringHelper helper;
      LOG_USER_ERROR(OB_RESOURCE_POOL_NOT_EXIST, helper.convert(alter_pool.name_));
      LOG_WARN("resource pool not exist", "resource pool name", alter_pool.name_, K(ret));
    }
  } else if (NULL == pool) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pool is null", KP(pool), K(ret));
  } else {
    int64_t alter_count = 0;
    if (!config_name.is_empty()) {
      ++alter_count;
    }
    if (0 != alter_pool.unit_count_) {
      ++alter_count;
    }
    if (alter_pool.zone_list_.count() > 0) {
      ++alter_count;
    }
    if (alter_count > 1) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "alter unit_num, resource_unit, zone_list in one cmd");
      LOG_WARN("only support alter one item one time", K(alter_pool), K(config_name), K(ret));
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(pool_bak.assign(*pool))) {
      LOG_WARN("failed to assign pool_bak", K(ret));
    }

    //TODO: modiry replica_type not support;
    // alter unit config
    if (OB_FAIL(ret)) {
    } else if (config_name.is_empty()) {
      // config not change
    } else if (OB_FAIL(alter_pool_unit_config(pool, config_name))) {
      LOG_WARN("alter_pool_unit_config failed", "pool", *pool, K(config_name), K(ret));
    }

    // alter unit num
    if (OB_FAIL(ret)) {
    } else if (0 == alter_pool.unit_count_) {
      // unit num not change
    } else if (OB_FAIL(alter_pool_unit_num(pool, alter_pool.unit_count_, delete_unit_id_array, sql_text))) {
      LOG_WARN("alter_pool_unit_num failed", "pool", *pool,
               "unit_num", alter_pool.unit_count_, K(ret));
    }

    // alter zone list
    if (OB_FAIL(ret)) {
    } else if (alter_pool.zone_list_.count() <=0) {
      // zone list not change
    } else if (OB_FAIL(alter_pool_zone_list(pool, alter_pool.zone_list_))) {
      LOG_WARN("alter_pool_zone_list failed", "pool", *pool,
               "zone_list", alter_pool.zone_list_, K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    ROOTSERVICE_EVENT_ADD("unit", "alter_resource_pool",
                          "name", pool_bak.name_);
  }
  LOG_INFO("finish alter resource pool", KR(ret), K(alter_pool), K(config_name),
      K(delete_unit_id_array), K(sql_text));
  return ret;
}

int ObUnitManager::drop_resource_pool(const ObResourcePoolName &name, const bool if_exist)
{
  int ret = OB_SUCCESS;
  LOG_INFO("start drop resource pool", K(name));
  SpinWLockGuard guard(lock_);
  share::ObResourcePool *pool = NULL;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(inited_), K(loaded_), K(ret));
  } else if (name.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "resource pool name");
    LOG_WARN("invalid argument", K(name), K(ret));
  } else if (OB_FAIL(inner_get_resource_pool_by_name(name, pool))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("get resource pool by name failed", K(name), K(ret));
    } else {
      if (if_exist) {
        ret = OB_SUCCESS;
        ObCStringHelper helper;
        LOG_USER_NOTE(OB_RESOURCE_POOL_NOT_EXIST, helper.convert(name));
        LOG_INFO("resource_pool not exist, but no need drop it", K(name));
      } else {
        ret = OB_RESOURCE_POOL_NOT_EXIST;
        ObCStringHelper helper;
        LOG_USER_ERROR(OB_RESOURCE_POOL_NOT_EXIST, helper.convert(name));
        LOG_WARN("resource_pool not exist", K(name), K(ret));
      }
    }
  } else if (NULL == pool) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pool is null", KP(pool), K(ret));
  } else if (pool->is_granted_to_tenant()) {
    ret = OB_RESOURCE_POOL_ALREADY_GRANTED;
    ObCStringHelper helper;
    LOG_USER_ERROR(OB_RESOURCE_POOL_ALREADY_GRANTED, helper.convert(name));
    LOG_WARN("resource pool is granted to tenant, can't not delete it",
             "tenant_id", pool->tenant_id_, K(ret));
  } else {
    share::ObUnitTableTransaction trans;
    if (OB_FAIL(trans.start(proxy_, OB_SYS_TENANT_ID))) {
      LOG_WARN("start transaction failed", K(ret));
    } else if (OB_FAIL(remove_resource_pool_unit_in_trans(pool->resource_pool_id_,
                                                          trans))) {
      LOG_WARN("failed to remove reource pool and unit", K(ret), K(pool));
    }
    if (trans.is_started()) {
      const bool commit = (OB_SUCC(ret));
      int temp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (temp_ret = trans.end(commit))) {
        LOG_WARN("trans end failed", K(commit), K(temp_ret));
        ret = (OB_SUCCESS == ret) ? temp_ret : ret;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(delete_resource_pool_unit(pool))) {
      LOG_WARN("failed to delete resource pool or unit", K(ret), K(pool));
    }
  }
  LOG_INFO("finish drop resource pool", K(name), K(ret));
  return ret;
}
int ObUnitManager::remove_resource_pool_unit_in_trans(const int64_t resource_pool_id,
                                                     ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  int migrate_unit_ret = OB_CANCELED;
  if (OB_INVALID_ID == resource_pool_id
      || !trans.is_started()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(resource_pool_id),
             "is_started", trans.is_started());
  } else if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(inited_), K(loaded_), K(ret));
  } else if (OB_FAIL(ut_operator_.remove_units(trans, resource_pool_id))) {
    LOG_WARN("remove_units failed", K(ret), K(resource_pool_id));
  } else if (OB_FAIL(ut_operator_.remove_resource_pool(trans, resource_pool_id))) {
    LOG_WARN("remove_resource_pool failed", K(ret), K(resource_pool_id));
  } else if (OB_FAIL(complete_migrate_unit_rs_job_in_pool(resource_pool_id,
                                                          migrate_unit_ret,
                                                          trans))) {
    LOG_WARN("failed to complete migrate unit in pool", K(ret), K(resource_pool_id));
  }
  return ret;
}
// site lock at the call
int ObUnitManager::delete_resource_pool_unit(share::ObResourcePool *pool)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(pool)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("pool is null or invalid", K(ret), K(pool));
  } else if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(inited_), K(loaded_), K(ret));
  } else {
    if (OB_FAIL(delete_units_of_pool(pool->resource_pool_id_))) {
      LOG_WARN("delete_units_of_pool failed", "pool id", pool->resource_pool_id_, K(ret));
    } else if (OB_FAIL(delete_resource_pool(pool->resource_pool_id_, pool->name_))) {
      LOG_WARN("delete_resource_pool failed", "pool", *pool, K(ret));
    } else {
      const uint64_t config_id = pool->unit_config_id_;
      if (OB_FAIL(dec_config_ref_count(config_id))) {
        LOG_WARN("dec_config_ref_count failed", K(config_id), K(ret));
      } else if (OB_FAIL(delete_config_pool(config_id, pool))) {
        LOG_WARN("delete config pool failed", K(config_id), "pool", *pool, K(ret));
      } else {
        ROOTSERVICE_EVENT_ADD("unit", "drop_resource_pool",
                              "name", pool->name_);
      }
      pool_allocator_.free(pool);
      pool = NULL;
    }
  }
  return ret;
}

//After the 14x version,
//the same tenant is allowed to have multiple unit specifications in a zone,
//but it is necessary to ensure that these units can be scattered on each server in the zone,
//and multiple units of the same tenant cannot be located on the same machine;
int ObUnitManager::check_server_enough(const uint64_t tenant_id,
                                       const ObIArray<ObResourcePoolName> &pool_names,
                                       bool &enough)
{
  int ret = OB_SUCCESS;
  enough = true;
  share::ObResourcePool *pool = NULL;
  ObArray<ObUnitInfo> unit_infos;
  ObArray<ObUnitInfo> total_unit_infos;
  common::ObArray<share::ObResourcePool *> *pools = NULL;;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(inited_), K(loaded_), K(ret));
  } else if (pool_names.count() <= 0 || !is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(pool_names), K(tenant_id), K(ret));
  } else {
    //Count the number of newly added units
    for (int64_t i = 0; i < pool_names.count() && OB_SUCC(ret); i++) {
      unit_infos.reset();
      if (OB_FAIL(inner_get_resource_pool_by_name(pool_names.at(i), pool))) {
        LOG_WARN("fail to get resource pool by name", K(ret), "pool_name", pool_names.at(i));
      } else if (OB_ISNULL(pool)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid pool", K(ret), K(pool));
      } else if (OB_FAIL(inner_get_unit_infos_of_pool_(pool->resource_pool_id_, unit_infos))) {
        LOG_WARN("fail to get unit infos", K(ret), K(*pool));
      } else {
        for (int64_t j = 0; j < unit_infos.count() && OB_SUCC(ret); j++) {
          if (OB_FAIL(total_unit_infos.push_back(unit_infos.at(j)))) {
            LOG_WARN("fail to push back unit", K(ret), K(total_unit_infos), K(j), K(unit_infos));
          } else {
            LOG_DEBUG("add unit infos", K(ret), K(total_unit_infos), K(unit_infos));
          }
        }
      } //end else
    } // end for
  }
  //Count the number of existing units
  if (FAILEDx(get_pools_by_tenant_(tenant_id, pools))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      // a new tenant, without resource pool already granted
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get pools by tenant", K(ret), K(tenant_id));
    }
  } else if (OB_UNLIKELY(NULL == pools)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pools is null", K(ret), KP(pools));
  } else {
    for (int64_t i = 0; i < pools->count() && OB_SUCC(ret); i++) {
      unit_infos.reset();
      const share::ObResourcePool *pool = pools->at(i);
      if (OB_UNLIKELY(NULL == pool)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pool ptr is null", K(ret), KP(pool));
      } else if (OB_FAIL(inner_get_unit_infos_of_pool_(pool->resource_pool_id_, unit_infos))) {
        LOG_WARN("fail to get unit infos", K(ret), K(*pool));
      } else {
        for (int64_t j = 0; j < unit_infos.count() && OB_SUCC(ret); j++) {
          if (OB_FAIL(total_unit_infos.push_back(unit_infos.at(j)))) {
            LOG_WARN("fail to push back unit", K(ret), K(total_unit_infos), K(j), K(unit_infos));
          } else {
            LOG_WARN("add unit infos", K(ret), K(total_unit_infos), K(unit_infos));
          }
        }
      }
    }
  }
  ObArray<ObZoneInfo> zone_infos;
  if (FAILEDx(zone_mgr_.get_zone(zone_infos))) {
    LOG_WARN("fail to get zone infos", K(ret));
  } else {
    //Count the number of units in zone
    for (int64_t i = 0; i < zone_infos.count() && OB_SUCC(ret) && enough; i++) {
      const ObZone &zone = zone_infos.at(i).zone_;
      int64_t unit_count = 0;
      int64_t alive_server_count = 0;
      for (int64_t j = 0; j < total_unit_infos.count() && OB_SUCC(ret); j++) {
        if (total_unit_infos.at(j).unit_.zone_ == zone) {
          unit_count ++;
        }
      }
      if (unit_count > 0) {
        if (OB_FAIL(SVR_TRACER.get_alive_servers_count(zone, alive_server_count))) {
          LOG_WARN("fail to get alive server count", KR(ret), K(zone));
        } else if (alive_server_count < unit_count) {
          //ret = OB_UNIT_NUM_OVER_SERVER_COUNT;
          enough = false;
          LOG_WARN("resource pool unit num over zone server count", K(ret), K(unit_count), K(alive_server_count),
                   K(total_unit_infos));
        }
      }
    }
  }
  return ret;
}

//The F/L scheme has new restrictions.
//If the logonly replica exists in the locality before adding the Logonly pool, the change is not allowed
int ObUnitManager::check_locality_for_logonly_unit(const share::schema::ObTenantSchema &tenant_schema,
                                                   const ObIArray<ObResourcePoolName> &pool_names,
                                                   bool &is_permitted)
{
  int ret = OB_SUCCESS;
  is_permitted = true;
  ObArray<ObZone> zone_with_logonly_unit;
  ObArray<share::ObZoneReplicaNumSet> zone_locality;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(inited_), K(loaded_), K(ret));
  } else if (pool_names.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(pool_names), K(ret));
  } else {
    FOREACH_CNT_X(pool_name, pool_names, OB_SUCCESS == ret) {
      share::ObResourcePool *pool = NULL;
      if (OB_FAIL(inner_get_resource_pool_by_name(*pool_name, pool))) {
        LOG_WARN("get resource pool by name failed", "pool_name", *pool_name, K(ret));
      } else if (NULL == pool) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pool is null", KP(pool), K(ret));
      } else if (REPLICA_TYPE_LOGONLY != pool->replica_type_) {
        //nothing todo
      } else {
        for (int64_t i = 0; i < pool->zone_list_.count() && OB_SUCC(ret); i++) {
          if (OB_FAIL(zone_with_logonly_unit.push_back(pool->zone_list_.at(i)))) {
            LOG_WARN("fail to push back", K(ret));
          }
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(tenant_schema.get_zone_replica_attr_array(zone_locality))) {
    LOG_WARN("fail to get zone replica attr array", K(ret));
  } else {
    for (int64_t i = 0; i < zone_locality.count() && OB_SUCC(ret); i++) {
      if ((zone_locality.at(i).replica_attr_set_.get_logonly_replica_num() == 1
           || zone_locality.at(i).replica_attr_set_.get_encryption_logonly_replica_num() == 1)
          && has_exist_in_array(zone_with_logonly_unit, zone_locality.at(i).zone_)) {
        is_permitted = false;
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("logonly replica already exist before logonly pool create", K(ret), K(zone_locality),
                 K(zone_with_logonly_unit));
      }
    }
  }
  return ret;
}

/* when expand zone resource for tenant this func is invoked,
 * we need to check whether the tenant units are in deleting or adding.
 * if any tenant unit is in deleting or adding,
 * @is_allowed returns false
 */
int ObUnitManager::check_expand_zone_resource_allowed_by_old_unit_stat_(
    const uint64_t tenant_id,
    bool &is_allowed)
{
  int ret = OB_SUCCESS;
  ObArray<share::ObResourcePool *> *cur_pool_array = nullptr;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(inited_), K(loaded_), KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    int tmp_ret = get_pools_by_tenant_(tenant_id, cur_pool_array);
    if (OB_ENTRY_NOT_EXIST == tmp_ret) {
      is_allowed = true;
    } else if (OB_UNLIKELY(nullptr == cur_pool_array)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cur_pool is null", KR(ret), K(tenant_id));
    } else {
      is_allowed = true;
      for (int64_t i = 0; is_allowed && OB_SUCC(ret) && i < cur_pool_array->count(); ++i) {
        share::ObResourcePool *cur_pool = cur_pool_array->at(i);
        ObArray<share::ObUnit *> *units = nullptr;
        if (OB_UNLIKELY(nullptr == cur_pool)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("cur pool is null", KR(ret));
        } else if (OB_FAIL(get_units_by_pool(cur_pool->resource_pool_id_, units))) {
          LOG_WARN("fail to get units by pool", KR(ret),
                   "pool_id", cur_pool->resource_pool_id_);
        } else if (OB_UNLIKELY(nullptr == units)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("units ptrs is null", KR(ret), K(tenant_id), KPC(cur_pool));
        } else {
          for (int64_t j = 0; is_allowed && OB_SUCC(ret) && j < units->count(); ++j) {
            if (OB_UNLIKELY(nullptr == units->at(j))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("units ptrs is null", KR(ret), K(tenant_id), KPC(cur_pool));
            } else {
              is_allowed = ObUnit::UNIT_STATUS_ACTIVE == units->at(j)->status_;
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObUnitManager::check_expand_zone_resource_allowed_by_new_unit_stat_(
    const common::ObIArray<share::ObResourcePoolName> &pool_names)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < pool_names.count(); ++i) {
    share::ObResourcePool *pool = NULL;
    ObArray<ObUnit *> *units = nullptr;
    if (OB_FAIL(inner_get_resource_pool_by_name(pool_names.at(i), pool))) {
      LOG_WARN("get resource pool by name failed", "pool_name", pool_names.at(i), K(ret));
    } else if (NULL == pool) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pool is null", KP(pool), K(ret));
    } else if (OB_FAIL(get_units_by_pool(pool->resource_pool_id_, units))) {
      LOG_WARN("fail to get units by pool", K(ret));
    } else if (OB_UNLIKELY(nullptr == units)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("units ptr is null", K(ret));
    } else {
      for (int64_t j = 0; OB_SUCC(ret) && j < units->count(); ++j) {
        ObUnit *unit = units->at(j);
        if (OB_UNLIKELY(nullptr == unit)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unit ptr is null", KR(ret), KPC(pool));
        } else if (unit->status_ != ObUnit::UNIT_STATUS_ACTIVE) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected unit status", KR(ret), KPC(pool), KPC(unit));
        } else {/* good */}
      }
    }
  }
  return ret;
}
int ObUnitManager::check_grant_pools_unit_num_legal_(
    const uint64_t tenant_id,
    const common::ObIArray<share::ObResourcePoolName> &pool_names)
{
  int ret = OB_SUCCESS;
  ObArray<PoolOperationInfo> pool_op_info_array;
  bool in_hetero_deploy_mode = false;
  bool is_gts_standalone = false;
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || pool_names.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(pool_names));
  } else if (OB_FAIL(check_tenant_in_heterogeneous_deploy_mode(tenant_id, in_hetero_deploy_mode))) {
    LOG_WARN("fail to check_tenant_in_heterogeneous_deploy_mode", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_tenant_gts_standalone_enabled(tenant_id, is_gts_standalone))) {
    LOG_WARN("fail to check_tenant_gts_standalone_enabled", KR(ret), K(tenant_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < pool_names.count(); ++i) {
      const share::ObResourcePoolName &pool_name = pool_names.at(i);
      share::ObResourcePool *pool = nullptr;
      if (OB_FAIL(inner_get_resource_pool_by_name(pool_names.at(i), pool))) {
        LOG_WARN("get resource pool by name failed", "pool_name", pool_names.at(i), KR(ret));
      } else if (OB_ISNULL(pool)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pool is null", KP(pool), KR(ret));
      } else if (OB_FAIL(pool_op_info_array.push_back(PoolOperationInfo(
          0, pool->unit_count_, AUN_EXPAND, pool->replica_type_)))) {
        LOG_WARN("push back pool operation info failed", KR(ret));
      }
    }
    if (FAILEDx(check_unit_num_legal_after_pools_operation_(tenant_id, pool_op_info_array,
        in_hetero_deploy_mode, is_gts_standalone))) {
      LOG_WARN("check unit num legal after pools operation failed", KR(ret), K(tenant_id),
               K(pool_op_info_array), K(in_hetero_deploy_mode), K(is_gts_standalone));
    }
  }
  return ret;
}

int ObUnitManager::check_grant_pools_replica_type_legal_(
    const uint64_t tenant_id,
    const common::ObIArray<share::ObResourcePoolName> &pool_names)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || pool_names.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(pool_names));
  } else {
    bool has_logonly_replica = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < pool_names.count(); ++i) {
      const share::ObResourcePoolName &pool_name = pool_names.at(i);
      share::ObResourcePool *pool = nullptr;
      if (OB_FAIL(inner_get_resource_pool_by_name(pool_names.at(i), pool))) {
        LOG_WARN("get resource pool by name failed", "pool_name", pool_names.at(i), KR(ret));
      } else if (OB_ISNULL(pool)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pool is null", KP(pool), KR(ret));
      } else if (REPLICA_TYPE_LOGONLY == pool->replica_type_) {
        has_logonly_replica = true;
      }
    }
    if (OB_SUCC(ret) && has_logonly_replica) {
      bool enable_logonly_replica = false;
      bool in_hetero_deploy_mode = false;
      if (OB_FAIL(check_tenant_enable_logonly_replica_(tenant_id, enable_logonly_replica))) {
        LOG_WARN("fail to check tenant enable logonly replica", KR(ret), K(tenant_id));
      } else if (!enable_logonly_replica) {
        ret = OB_OP_NOT_ALLOW;
        LOG_USER_ERROR(OB_OP_NOT_ALLOW, "ENABLE_LOGONLY_REPLICA is false, tenant with LOGONLY pools");
        LOG_WARN("tenant with L-pool is not supported when enable_logonly_replica is false",
                KR(ret), K(pool_names), K(tenant_id), K(enable_logonly_replica));
      } else if (OB_FAIL(check_tenant_in_heterogeneous_deploy_mode(tenant_id, in_hetero_deploy_mode))) {
        LOG_WARN("fail to check tenant in heterogeneous deploy mode", KR(ret), K(tenant_id));
      } else if (!in_hetero_deploy_mode) {
        ret = OB_OP_NOT_ALLOW;
        LOG_USER_ERROR_WITH_ARGS(OB_OP_NOT_ALLOW, "Tenant %lu zone_deploy_mode is 'homo', not 'hetero', "
            "LOGONLY pool", tenant_id);
        LOG_WARN("can not grant logonly pool, zone_deploy_mode must be hetero", KR(ret), K(tenant_id), K(pool_names));
      }
    }
  }
  return ret;
}

int ObUnitManager::check_tenant_exist_(
    const uint64_t tenant_id,
    bool &is_exist)
{
  int ret = OB_SUCCESS;
  share::schema::ObSchemaGetterGuard guard;
  is_exist = false;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", KP(GCTX.schema_service_), KR(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, guard))) {
    LOG_WARN("fail to get schema guard", KR(ret));
  } else if (OB_FAIL(guard.check_tenant_exist(tenant_id, is_exist))) {
    LOG_WARN("check tenant exist failed", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObUnitManager::read_parameter_from_seed_tenant_(
    const char * parameter_name,
    ObString &parameter_value)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  parameter_value.reset();
  if (OB_ISNULL(parameter_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else if (OB_FAIL(sql.append_fmt("SELECT * FROM %s WHERE name = \"%s\"",
                         OB_ALL_SEED_PARAMETER_TNAME, parameter_name))) {
    LOG_WARN("fail to append sql", KR(ret));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(GCTX.sql_proxy_->read(res, sql.ptr()))) {
        LOG_WARN("fail to execute sql", KR(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", KR(ret));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to get next row", KR(ret));
        }
      } else {
        EXTRACT_VARCHAR_FIELD_MYSQL(*result, "value", parameter_value);
      }
    }
  }
  return ret;
}

int ObUnitManager::check_tenant_enable_logonly_replica_(
    const uint64_t tenant_id, bool &enable_logonly_replica)
{
  int ret = OB_SUCCESS;
  enable_logonly_replica = false;
  bool is_exist = false;
  ObString parameter_value;
  const char * parameter_name = ENABLE_LOGONLY_REPLICA;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_tenant_exist_(tenant_id, is_exist))) {
    LOG_WARN("fail to check tenant exist", KR(ret), K(tenant_id));
  } else {
    if (is_exist) {
      omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
      if (OB_UNLIKELY(!tenant_config.is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("tenant_config is not valid", KR(ret), K(tenant_id));
      } else {
        enable_logonly_replica = tenant_config->enable_logonly_replica;
      }
    } else {
      ObSqlString param_value;
      // tenant not exist yet, it means it's being created and value of seed tenant will be used
      // as this tenant's initial value. So try to get value from __all_seed_parameter.
      if (OB_FAIL(read_parameter_from_seed_tenant_(parameter_name , parameter_value))) {
        LOG_WARN("fail to read enable_logonyl_replica from seed tenant", KR(ret));
      } else if (parameter_value.empty()) {
        enable_logonly_replica = false;
        LOG_INFO("seed not set, use default value", KR(ret));
      } else if (OB_FAIL(param_value.assign(parameter_value))) {
        LOG_WARN("fail to assign param value", KR(ret), K(parameter_value));
      } else {
        bool is_valid = false;
        enable_logonly_replica = ObConfigBoolParser::get(param_value.ptr(), is_valid);
        if (OB_UNLIKELY(!is_valid)) {
          // defensive check
          enable_logonly_replica = false;
          LOG_INFO("parameter value is not expected", K(param_value));
        }
        LOG_INFO("tenant does not exist yet, try to get enable_logonly_replica from seed",
                 KR(ret), K(tenant_id), K(enable_logonly_replica), K(parameter_value), K(is_valid), K(param_value));
      }
    }
  }
  return ret;
}

int ObUnitManager::check_tenant_in_heterogeneous_deploy_mode(
    const uint64_t tenant_id, bool &in_hetero_deploy_mode)
{
  int ret = OB_SUCCESS;
  bool is_exist = false;
  ObString parameter_value;
  const char * parameter_name = ZONE_DEPLOY_MODE;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_tenant_exist_(tenant_id, is_exist))) {
    LOG_WARN("fail to check tenant exist", KR(ret), K(tenant_id));
  } else {
    if (is_exist) {
      omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
      if (OB_UNLIKELY(!tenant_config.is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("tenant_config is not valid", KR(ret), K(tenant_id));
      } else {
        ObString value = tenant_config->zone_deploy_mode.get_value_string();
        in_hetero_deploy_mode = 0 == value.case_compare(ObConfigZoneDeployModeChecker::HETERO_MODE_STR)
            ? true : false;
      }
    } else {
      // tenant not exist yet, it means it's being created and value of seed tenant will be used
      // as this tenant's initial value. So try to get value from __all_seed_parameter.
      if (OB_FAIL(read_parameter_from_seed_tenant_(parameter_name, parameter_value))) {
        LOG_WARN("fail to read zone_deploy_mode from seed tenant", KR(ret));
      } else if (parameter_value.empty()) {
        in_hetero_deploy_mode = false;
        LOG_INFO("seed not set, use default value HOMO", KR(ret));
      } else {
        in_hetero_deploy_mode = 0 == parameter_value.case_compare(ObConfigZoneDeployModeChecker::HETERO_MODE_STR)
            ? true : false;
      }
      LOG_INFO("tenant does not exist yet, try to get zone_deploy_mode from seed",
          KR(ret), K(tenant_id), K(in_hetero_deploy_mode));
    }
  }
  return ret;
}

int ObUnitManager::check_tenant_gts_standalone_enabled(
    const uint64_t tenant_id, bool &enabled)
{
  int ret = OB_SUCCESS;
  share::schema::ObSchemaGetterGuard guard;
  bool is_exist = false;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
    if (OB_UNLIKELY(!tenant_config.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("tenant_config is not valid", KR(ret), K(tenant_id));
    } else {
      enabled = tenant_config->enable_gts_standalone;
    }
  }
  return ret;
}

/*
 * Check if tenant's unit_num legal after grant pools or alter pool unit_num:
 *    In homo_mode, tenant's resource pools must have same unit_num;
 *    In hetero_mode, unit_num can be different, but no more than 2 distinct unit_num.
 */
int ObUnitManager::check_unit_num_legal_after_pools_operation_(
    const uint64_t tenant_id,
    const common::ObIArray<ObUnitManager::PoolOperationInfo> &pool_op_info_array,
    const bool in_hetero_deploy_mode,
    const bool is_gts_standalone)
{
  int ret = OB_SUCCESS;
  ObUnitNumCountMap unit_num_count_map;
  const int64_t BUCKET_SIZE = 32;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(inited_), K(loaded_), KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || pool_op_info_array.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(pool_op_info_array));
  } else if (OB_FAIL(unit_num_count_map.create(BUCKET_SIZE, ObModIds::OB_RS_UNIT_MANAGER))) {
    LOG_WARN("fail to create unit_num_count_map", KR(ret), K(BUCKET_SIZE));
  } else {
    // stat pools unit count before operation
    ObArray<share::ObResourcePool *> *cur_pool_array = nullptr;
    if (OB_FAIL(get_pools_by_tenant_(tenant_id, cur_pool_array))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        // when create tenant pools belong to this tenant is empty
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to get pools by tenant", KR(ret), K(tenant_id));
      }
    } else if (OB_UNLIKELY(nullptr == cur_pool_array)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cur_pool is null", KR(ret), K(tenant_id));
    } else if (OB_UNLIKELY(cur_pool_array->count() <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pool_array", KR(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < cur_pool_array->count(); ++i) {
        if (OB_UNLIKELY(nullptr == cur_pool_array->at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("pool ptr is null", KR(ret));
        } else if (!ObReplicaTypeCheck::need_to_align_to_ug(cur_pool_array->at(i)->replica_type_)) {
          // no need to check unit_num
        } else if (OB_FAIL(unit_num_count_map.inc_unit_num_cnt(cur_pool_array->at(i)->unit_count_))) {
          LOG_WARN("fail to inc_unit_num_cnt", KR(ret));
        }
      }
    }
    // stat pool operation diff on unit count
    for (int64_t i = 0; OB_SUCC(ret) && i < pool_op_info_array.count(); ++i) {
      const PoolOperationInfo &pool_op_info = pool_op_info_array.at(i);
      if (ObReplicaTypeCheck::gts_standalone_applicable(pool_op_info.replica_type_)
          && is_gts_standalone && pool_op_info.new_unit_num_ < 2) {
        ret = OB_OP_NOT_ALLOW;
        LOG_WARN("tenant enabled gts_standalone, unit_num can not be less than 2", KR(ret), K(tenant_id));
        LOG_USER_ERROR_WITH_ARGS(OB_OP_NOT_ALLOW, "Tenant %lu enabled GTS standalone, unit_num less than 2", tenant_id);
      } else if (!ObReplicaTypeCheck::need_to_align_to_ug(pool_op_info.replica_type_)) {
        // no need to check unit_num
      } else if (OB_FAIL(unit_num_count_map.inc_unit_num_cnt(pool_op_info.new_unit_num_))) {
        LOG_WARN("fail to inc_unit_num_cnt", KR(ret), K(pool_op_info));
      } else if (0 == pool_op_info.old_unit_num_)  {
        // operation is grant pool, no need to dec old_unit_num
      } else if (OB_FAIL(unit_num_count_map.dec_unit_num_cnt(pool_op_info.old_unit_num_))) {
        LOG_WARN("fail to dec_unit_num_cnt", KR(ret), K(pool_op_info));
      }
    }
    // check if stat result legal
    if (OB_FAIL(ret)) {
    } else if (!in_hetero_deploy_mode && unit_num_count_map.size() > 1) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("homo mode, unit_num can not be different", KR(ret), K(tenant_id));
      LOG_USER_ERROR_WITH_ARGS(OB_OP_NOT_ALLOW, "Tenant %lu zone_deploy_mode is 'homo', not 'hetero', "
          "resource pools with different unit_num", tenant_id);
    } else if (in_hetero_deploy_mode && unit_num_count_map.size() > 2) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("tenant in hetero mode, unit_num count more than 2 not allowed", KR(ret), K(tenant_id));
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "More than two distinct unit_num in a single tenant");
    } else {
      // good
    }
  }
  return ret;
}

// get pool units and unit group ids through one query from inner_table
int ObUnitManager::get_tenant_units_and_unit_group_ids_(
    ObISQLClient &client,
    const uint64_t tenant_id,
    common::ObIArray<share::ObUnit> &tenant_units,
    common::ObIArray<uint64_t> &unit_group_id_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ut_operator_.get_units_by_tenant(tenant_id, tenant_units))) {
    LOG_WARN("fail to get units by tenant", KR(ret), K(tenant_id));
  } else {
    FOREACH_CNT_X(unit, tenant_units, OB_SUCC(ret)) {
      if (OB_FAIL(add_var_to_array_no_dup(unit_group_id_array, unit->unit_group_id_))) {
        LOG_WARN("fail to push back", KR(ret));
      }
    }
  }
  return ret;
}

// TODO: disable resource pools intersect for one tenant
//       NEED to disable logics to handle resource pool intersect in server_balancer
int ObUnitManager::grant_pools(common::ObMySQLTransaction &trans,
                               const lib::Worker::CompatMode compat_mode,
                               const ObIArray<ObResourcePoolName> &pool_names,
                               const uint64_t tenant_id,
                               /*arg "const bool skip_offline_server" is no longer supported*/
                               const bool check_data_version)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  const bool grant = true;
  bool intersect = false;
  bool server_enough = true;
  bool enable_logonly_replica = false;
  bool is_grant_pool_allowed = false;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(inited_), K(loaded_), KR(ret));
  } else if (OB_FAIL(ObUnitTableTransaction::lock_service_epoch(trans))) {
    LOG_WARN("fail to lock unit service epoch", KR(ret));
  } else if (pool_names.count() <= 0 || !is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(pool_names), K(tenant_id), K(ret));
  } else if (OB_FAIL(check_pool_ownership_(tenant_id, pool_names, true/*is_grant*/))) {
    LOG_WARN("check pool ownership failed", KR(ret), K(pool_names));
  } else if (OB_FAIL(check_pool_intersect_(tenant_id, pool_names, intersect))) {
    LOG_WARN("check pool intersect failed", K(pool_names), KR(ret));
  } else if (intersect) {
    ret = OB_POOL_SERVER_INTERSECT;
    ObCStringHelper helper;
    LOG_USER_ERROR(OB_POOL_SERVER_INTERSECT, helper.convert(pool_names));
    LOG_WARN("resource pool unit server intersect", K(pool_names), KR(ret));
  } else if (OB_FAIL(check_server_enough(tenant_id, pool_names, server_enough))) {
    LOG_WARN("fail to check server enough", KR(ret), K(tenant_id), K(pool_names));
  } else if (!server_enough) {
    ret = OB_UNIT_NUM_OVER_SERVER_COUNT;
    LOG_WARN("resource pool unit num over zone server count", K(ret), K(pool_names), K(tenant_id));
  } else if (OB_FAIL(check_expand_zone_resource_allowed_by_old_unit_stat_(
          tenant_id, is_grant_pool_allowed))) {
    LOG_WARN("fail to check grant pools allowed by unit stat", KR(ret), K(tenant_id));
  } else if (!is_grant_pool_allowed) {
    ret = OB_OP_NOT_ALLOW;
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "grant pool when pools in shrinking or expanding");
  } else if (OB_FAIL(check_expand_zone_resource_allowed_by_new_unit_stat_(pool_names))) {
    LOG_WARN("fail to check_tenant_pools_unit_num_legal_check grant pools allowed by unit stat", KR(ret));
  } else if (OB_FAIL(check_grant_pools_unit_num_legal_(tenant_id, pool_names))) {
    LOG_WARN("fail to check pools unit num legal", KR(ret), K(tenant_id), K(pool_names));
  } else if (OB_FAIL(check_grant_pools_replica_type_legal_(tenant_id, pool_names))) {
    LOG_WARN("fail to check grant pools replica type legal", KR(ret), K(tenant_id), K(pool_names));
  } else if (OB_FAIL(do_grant_pools_(trans, compat_mode, pool_names, tenant_id, check_data_version))) {
    LOG_WARN("do grant pools failed", KR(ret), K(grant), K(pool_names), K(tenant_id), K(compat_mode));
  }
  LOG_INFO("grant resource pools to tenant", KR(ret), K(pool_names), K(tenant_id));
  return ret;
}

int ObUnitManager::revoke_pools(common::ObMySQLTransaction &trans,
                                const ObIArray<ObResourcePoolName> &pool_names,
                                const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(inited_), K(loaded_), K(ret));
  } else if (OB_FAIL(ObUnitTableTransaction::lock_service_epoch(trans))) {
    LOG_WARN("fail to lock unit service epoch", KR(ret));
  } else if (pool_names.count() <= 0 || !is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(pool_names), K(tenant_id), K(ret));
  } else if (OB_FAIL(check_pool_ownership_(tenant_id, pool_names, false/*is_grant*/))) {
    LOG_WARN("check pool ownership failed", KR(ret), K(pool_names));
  } else if (OB_FAIL(do_revoke_pools_(trans, pool_names, tenant_id))) {
    LOG_WARN("do revoke pools failed", KR(ret), K(pool_names), K(tenant_id));
  }
  LOG_INFO("revoke resource pools from tenant", K(pool_names), K(ret));
  return ret;
}

int ObUnitManager::inner_get_pool_ids_of_tenant(const uint64_t tenant_id,
                                                ObIArray<uint64_t> &pool_ids) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(inited_), K(loaded_), K(ret));
  } else if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(ret));
  } else {
    pool_ids.reuse();
    ObArray<share::ObResourcePool  *> *pools = NULL;
    if (OB_FAIL(get_pools_by_tenant_(tenant_id, pools))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_DEBUG("get_pools_by_tenant failed", K(tenant_id), K(ret));
      } else {
        // just return empty pool_ids
        LOG_INFO("tenant doesn't own any pool", K(tenant_id), KR(ret));
        ret = OB_SUCCESS;
      }
    } else if (NULL == pools) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pools is null", KP(pools), K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < pools->count(); ++i) {
        if (NULL == pools->at(i)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("pool is null", "pool", OB_P(pools->at(i)), K(ret));
        } else if (OB_FAIL(pool_ids.push_back(pools->at(i)->resource_pool_id_))) {
          LOG_WARN("push_back failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObUnitManager::get_tenant_alive_servers_non_block(const uint64_t tenant_id,
                                                      common::ObIArray<ObAddr> &servers)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    ObArray<ObUnit> tenant_units;
    uint64_t valid_tnt_id = is_meta_tenant(tenant_id) ? gen_user_tenant_id(tenant_id) : tenant_id;
    // Try lock and get units from inmemory data. If lock failed, get from inner table.
    if (lock_.try_rdlock()) {
      // Get from inmemory data
      ObArray<share::ObResourcePool *> *pools = nullptr;
      if (OB_FAIL(check_inner_stat_())) {
        LOG_WARN("check_inner_stat failed", KR(ret), K(inited_), K(loaded_));
      } else if (OB_FAIL(get_pools_by_tenant_(valid_tnt_id, pools))) {
        LOG_WARN("failed to get pools by tenant", KR(ret), K(valid_tnt_id));
      } else if (OB_ISNULL(pools)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pools is nullptr", KR(ret), KP(pools));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < pools->count(); ++i) {
          const share::ObResourcePool *pool = pools->at(i);
          ObArray<ObUnit *> *pool_units;
          if (OB_ISNULL(pool)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("pool is nullptr", KR(ret), KP(pool));
          } else if (OB_FAIL(get_units_by_pool(pool->resource_pool_id_, pool_units))) {
            LOG_WARN("fail to get_units_by_pool", KR(ret),
                    "pool_id", pool->resource_pool_id_);
          } else if (OB_ISNULL(pool_units)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("pool_units is nullptr", KR(ret), KP(pool_units));
          } else {
            ARRAY_FOREACH_X(*pool_units, idx, cnt, OB_SUCC(ret)) {
              const ObUnit *unit = pool_units->at(idx);
              if (OB_ISNULL(unit)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unit is nullptr", KR(ret), KP(unit));
              } else if (OB_FAIL(tenant_units.push_back(*unit))) {
                LOG_WARN("failed to push_back unit", KR(ret), KPC(unit));
              }
            }
          }
        }
      }
      lock_.unlock();
    } else {
      // Get from inner_table
      tenant_units.reuse();
      if (OB_FAIL(ut_operator_.get_units_by_tenant(valid_tnt_id, tenant_units))) {
        LOG_WARN("fail to get_units_by_tenant from inner_table",
                KR(ret), K(tenant_id), K(valid_tnt_id));
      }
    }

    // Filter alive servers
    if (OB_SUCC(ret)) {
      servers.reuse();
      FOREACH_X(unit, tenant_units, OB_SUCC(ret)) {
        bool is_alive = false;
        if (OB_FAIL(SVR_TRACER.check_server_alive(unit->server_, is_alive))) {
          LOG_WARN("check_server_alive failed", KR(ret), K(unit->server_));
        } else if (is_alive) {
          if (has_exist_in_array(servers, unit->server_)) {
            // server exist
          } else if (OB_FAIL(servers.push_back(unit->server_))) {
            LOG_WARN("push_back failed", KR(ret), K(unit->server_));
          }
        }
        if (OB_FAIL(ret) || !unit->migrate_from_server_.is_valid()) {
          // skip
        } else if (OB_FAIL(SVR_TRACER.check_server_alive(unit->migrate_from_server_, is_alive))) {
          LOG_WARN("check_server_alive failed", KR(ret), K(unit->migrate_from_server_));
        } else if (is_alive) {
          if (has_exist_in_array(servers, unit->migrate_from_server_)) {
            // server exist
          } else if (OB_FAIL(servers.push_back(unit->migrate_from_server_))) {
            LOG_WARN("push_back failed", KR(ret), K(unit->migrate_from_server_));
          }
        }
      }
    }
  }
  return ret;
}

int ObUnitManager::get_pool_ids_of_tenant(const uint64_t tenant_id,
                                          ObIArray<uint64_t> &pool_ids) const
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(inited_), K(loaded_), K(ret));
  } else if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(ret));
  } else if (OB_FAIL(inner_get_pool_ids_of_tenant(tenant_id, pool_ids))) {
    LOG_WARN("fail to inner get pool ids of tenant", K(ret));
  }
  return ret;
}

int ObUnitManager::get_pool_names_of_tenant(const uint64_t tenant_id,
                                            ObIArray<ObResourcePoolName> &pool_names) const
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(inited_), K(loaded_), K(ret));
  } else if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(ret));
  } else {
    ObArray<share::ObResourcePool  *> *pools = NULL;
    if (OB_FAIL(get_pools_by_tenant_(tenant_id, pools))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("get_pools_by_tenant failed", K(tenant_id), K(ret));
      } else {
        // just return empty pool_ids
        ret = OB_SUCCESS;
        LOG_WARN("tenant doesn't own any pool", K(tenant_id), K(ret));
      }
    } else if (NULL == pools) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pools is null", KP(pools), K(ret));
    } else {
      pool_names.reuse();
      for (int64_t i = 0; OB_SUCC(ret) && i < pools->count(); ++i) {
        if (NULL == pools->at(i)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("pool is null", "pool", OB_P(pools->at(i)), K(ret));
        } else if (OB_FAIL(pool_names.push_back(pools->at(i)->name_))) {
          LOG_WARN("push_back failed", K(ret));
        }
      }
    }
  }
  return ret;
}


int ObUnitManager::get_unit_config_by_pool_name(
    const ObString &pool_name,
    share::ObUnitConfig &unit_config) const
{
  int ret = OB_SUCCESS;
  share::ObResourcePool *pool = NULL;
  ObUnitConfig *config = NULL;
  SpinRLockGuard guard(lock_);
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(inited_), K(loaded_), K(ret));
  } else if (OB_FAIL(inner_get_resource_pool_by_name(pool_name, pool))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("get resource pool by name failed", K(pool_name), K(ret));
    } else {
      ret = OB_RESOURCE_POOL_NOT_EXIST;
      LOG_WARN("pool not exist", K(ret), K(pool_name));
    }
  } else if (NULL == pool) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("pool is null", KP(pool), K(ret));
  } else if (OB_FAIL(get_unit_config_by_id(pool->unit_config_id_, config))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_RESOURCE_UNIT_NOT_EXIST;
    }
    LOG_WARN("can not find config for unit",
             "unit_config_id", pool->unit_config_id_,
             K(ret));
  } else if (NULL == config) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("config is null", KP(config), K(ret));
  } else {
    unit_config = *config;
  }
  return ret;
}


int ObUnitManager::get_zones_of_pools(const ObIArray<ObResourcePoolName> &pool_names,
                                      ObIArray<ObZone> &zones) const
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(inited_), K(loaded_), K(ret));
  } else if (pool_names.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("pool_names is empty", K(pool_names), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < pool_names.count(); ++i) {
      share::ObResourcePool  *pool = NULL;
      if (pool_names.at(i).is_empty()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "resource pool name");
        LOG_WARN("invalid pool name", "pool name", pool_names.at(i), K(ret));
      } else if (OB_FAIL(inner_get_resource_pool_by_name(pool_names.at(i), pool))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          LOG_WARN("get resource pool by name failed", "name", pool_names.at(i), K(ret));
        } else {
          ret = OB_RESOURCE_POOL_NOT_EXIST;
          ObCStringHelper helper;
          LOG_USER_ERROR(OB_RESOURCE_POOL_NOT_EXIST, helper.convert(pool_names.at(i)));
          LOG_WARN("pool not exist", "pool_name", pool_names.at(i), K(ret));
        }
      } else if (NULL == pool) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pool is null", KP(pool), K(ret));
      } else if (OB_FAIL(append(zones, pool->zone_list_))) {
        LOG_WARN("append failed", "zone_list", pool->zone_list_, K(ret));
      }
    }
  }
  return ret;
}

int ObUnitManager::get_pools(common::ObIArray<share::ObResourcePool> &pools) const
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(inited_), K(loaded_), K(ret));
  } else {
    ObHashMap<uint64_t, share::ObResourcePool  *>::const_iterator iter = id_pool_map_.begin();
    for ( ; OB_SUCC(ret) && iter != id_pool_map_.end(); ++iter) {
      if (NULL == iter->second) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("iter->second is null", KP(iter->second), K(ret));
      } else if (OB_FAIL(pools.push_back(*(iter->second)))) {
        LOG_WARN("push_back failed", K(ret));
      }
    }
  }
  return ret;
}

int ObUnitManager::create_sys_units(const ObIArray<ObUnit> &sys_units)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(inited_), K(loaded_), K(ret));
  } else if (sys_units.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sys_units is empty", K(sys_units), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < sys_units.count(); ++i) {
      if (OB_FAIL(add_unit(*proxy_, sys_units.at(i)))) {
        LOG_WARN("add_unit failed", "unit", sys_units.at(i), K(ret));
      }
    }
  }
  return ret;
}

int ObUnitManager::inner_get_tenant_pool_zone_list(
    const uint64_t tenant_id,
    common::ObIArray<common::ObZone> &zone_list) const
{
  int ret = OB_SUCCESS;
  ObArray<share::ObResourcePool *> *pools = NULL;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check inner stat failed", K(ret), K(inited_), K(loaded_));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(get_pools_by_tenant_(tenant_id, pools))) {
    LOG_WARN("fail to get pools by tenant", K(ret), K(tenant_id));
  } else if (OB_UNLIKELY(NULL == pools)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pools ptr is null", K(ret), K(tenant_id));
  } else if (pools->count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pools array is empty", K(ret), K(*pools));
  } else {
    zone_list.reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < pools->count(); ++i) {
      const share::ObResourcePool *pool = pools->at(i);
      if (NULL == pool) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pool is null", K(ret), KP(pool));
      } else if (OB_FAIL(append(zone_list, pool->zone_list_))) {
        LOG_WARN("fail to append", K(ret));
      } else {} // no more to do
    }
  }
  return ret;
}

int ObUnitManager::get_tenant_pool_zone_list(
    const uint64_t tenant_id,
    common::ObIArray<common::ObZone> &zone_list) const
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  if (OB_FAIL(inner_get_tenant_pool_zone_list(tenant_id, zone_list))) {
    LOG_WARN("fail to inner get tenant pool zone list", K(ret));
  }
  return ret;
}

// cancel migrate units on server to other servers
int ObUnitManager::cancel_migrate_out_units(const ObAddr &server)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  ObArray<uint64_t> migrate_units;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K_(inited), K_(loaded), K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(server), K(ret));
  } else if (OB_FAIL(get_migrate_units_by_server(server, migrate_units))) {
    LOG_WARN("get_migrate_units_by_server failed", K(server), K(ret));
  } else {
    const EndMigrateOp op = REVERSE;
    for (int64_t i = 0; OB_SUCC(ret) && i < migrate_units.count(); ++i) {
      if (OB_FAIL(end_migrate_unit(migrate_units.at(i), op))) {
        LOG_WARN("end_migrate_unit failed", "unit_id", migrate_units.at(i), K(op), K(ret));
      }
    }
  }
  LOG_INFO("cancel migrate out units", K(server), K(ret));
  return ret;
}

int ObUnitManager::check_server_empty(const ObAddr &server, bool &empty) const
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  ObArray<ObUnitLoad> *loads = NULL;
  empty = false;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K_(inited), K_(loaded), K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), K(ret));
  } else if (OB_FAIL(get_loads_by_server(server, loads))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("get_loads_by_server failed", K(server), K(ret));
    } else {
      ret = OB_SUCCESS;
      empty = true;
    }
  } else if (NULL == loads) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("loads is null", KP(loads), K(ret));
  } else {
    empty = false;
  }
  return ret;
}

int ObUnitManager::finish_migrate_unit_not_in_tenant(
                   share::ObResourcePool *pool)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  //Not in tenant unit
  //Using pool, take the unit corresponding to the pool whose tenant_id is -1
  if (-1 != pool->tenant_id_) {
    //in tenant
    //ignore in tenant unit
  } else {
    ObArray<ObUnitInfo> unit_infos;
    if (OB_FAIL(inner_get_unit_infos_of_pool_(pool->resource_pool_id_, unit_infos))) {
      LOG_WARN("fail to get units by pool", K(ret));
    } else {
      FOREACH_CNT_X(unit_info, unit_infos, OB_SUCC(ret)) {
        if ((*unit_info).unit_.migrate_from_server_.is_valid()) {
          if (OB_INVALID_ID == (*unit_info).unit_.unit_id_) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid argument", K((*unit_info).unit_.unit_id_), K(ret));
          } else {
            const EndMigrateOp op = COMMIT;
            if (OB_FAIL(end_migrate_unit((*unit_info).unit_.unit_id_, op))) {
              LOG_WARN("end migrate unit failed", K(ret), K((*unit_info).unit_.unit_id_), K(op));
            } else {
              LOG_INFO("finish migrate unit not in tenant", K(ret),
                       "unit_id", (*unit_info).unit_.unit_id_);
            }
          }
        } else {}//ignore not in migrate unit
      }
    }
  }
  return ret;
}

int ObUnitManager::finish_migrate_unit_not_in_locality(
                   uint64_t tenant_id,
                   share::schema::ObSchemaGetterGuard *schema_guard,
                   ObArray<common::ObZone> zone_list)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  const ObTenantSchema *tenant_schema = NULL;
  ObArray<ObUnitInfo> unit_infos;
  ObArray<common::ObZone> zone_locality_list;
  if (OB_FAIL((*schema_guard).get_tenant_info(tenant_id, tenant_schema))) {
    LOG_WARN("fail to get tenant schema", K(ret), K(tenant_id));
  } else if (OB_ISNULL(tenant_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant schema is null", K(ret));
  } else if (OB_FAIL(tenant_schema->get_zone_list(zone_locality_list))) {
    LOG_WARN("fail to get zone replica attr array", K(ret));
  } else {
    for (int64_t i = 0; i < zone_list.count() && OB_SUCC(ret); i++) {
      if (!has_exist_in_array(zone_locality_list, zone_list.at(i))) {
        //Get the unit that is in the zone locality but not in the zone_list,
        //the zone that is not in the locality
        if (OB_FAIL(inner_get_zone_alive_unit_infos_by_tenant(
                    tenant_id, zone_list.at(i), unit_infos))) {
          LOG_WARN("fail to get zone alive unit infos by tenant", K(ret));
        } else {
          FOREACH_CNT_X(unit_info, unit_infos, OB_SUCC(ret)) {
            if ((*unit_info).unit_.migrate_from_server_.is_valid()) {
              if (OB_INVALID_ID == (*unit_info).unit_.unit_id_) {
                ret = OB_INVALID_ARGUMENT;
                LOG_WARN("invalid argument", K((*unit_info).unit_.unit_id_), K(ret));
              } else {
                const EndMigrateOp op = COMMIT;
                if (OB_FAIL(end_migrate_unit((*unit_info).unit_.unit_id_, op))) {
                  LOG_WARN("end migrate unit failed", K(ret),
                            K((*unit_info).unit_.unit_id_), K(op));
                } else {
                  LOG_INFO("finish migrate unit not in locality", K(ret),
                           "unit_id", (*unit_info).unit_.unit_id_);
                }
              }
            } else {} //ignore not in migrate unit
          } //end FOREACH
        }
      } else {} //ignore in locality unit
    } //end for
  }
  return ret;
}

int ObUnitManager::finish_migrate_unit(const uint64_t unit_id)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(inited_), K(loaded_), K(ret));
  } else if (OB_INVALID_ID == unit_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(unit_id), K(ret));
  } else {
    const EndMigrateOp op = COMMIT;
    if (OB_FAIL(end_migrate_unit(unit_id, op))) {
      LOG_WARN("end_migrate_unit failed", K(unit_id), K(op), K(ret));
    }
  }
  LOG_INFO("finish migrate unit", K(unit_id), K(ret));
  return ret;
}

int ObUnitManager::inner_get_zone_alive_unit_infos_by_tenant(
    const uint64_t tenant_id,
    const common::ObZone &zone,
    common::ObIArray<share::ObUnitInfo> &unit_infos) const
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> rs_pool;
  ObArray<ObUnitInfo> unit_array;
  unit_infos.reset();
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))
      || OB_UNLIKELY(zone.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(zone));
  } else if (OB_FAIL(inner_get_pool_ids_of_tenant(tenant_id, rs_pool))) {
    LOG_WARN("fail to get pool ids by tennat", K(ret), K(tenant_id));
  } else {
    FOREACH_X(pool, rs_pool, OB_SUCCESS == ret) {
      unit_array.reuse();
      if (OB_FAIL(check_inner_stat_())) {
        LOG_WARN("check inner stat failed", K(ret), K(inited_), K(loaded_));
      } else if (OB_UNLIKELY(NULL == pool)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pool is null", K(ret));
      } else if (OB_FAIL(inner_get_unit_infos_of_pool_(*pool, unit_array))) {
        LOG_WARN("fail to get unit infos of pool", K(ret));
      } else if (unit_array.count() > 0) {
        FOREACH_X(u, unit_array, OB_SUCCESS == ret) {
          bool is_alive = false;
          bool is_in_service = false;
          if (OB_UNLIKELY(NULL == u)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unit is empty", K(ret));
          } else if (zone != u->unit_.zone_) {
            // do not belong to this zone
            } else if (OB_FAIL(SVR_TRACER.check_server_alive(u->unit_.server_, is_alive))) {
            LOG_WARN("check_server_alive failed", "server", u->unit_.server_, K(ret));
          } else if (OB_FAIL(SVR_TRACER.check_in_service(u->unit_.server_, is_in_service))) {
            LOG_WARN("check server in service failed", "server", u->unit_.server_, K(ret));
          } else if (!is_alive || !is_in_service) {
            // ignore unit on not-alive server
          } else if (ObUnit::UNIT_STATUS_DELETING == u->unit_.status_) {
            // ignore the unit which is in deleting status
          } else if (OB_FAIL(unit_infos.push_back(*u))) {
            LOG_WARN("fail to push back", K(ret));
          } else {} // no more to do
        }
      } else {} // empty array
    }
  }
  return ret;
}

int ObUnitManager::commit_alter_tenant_resource_unit_num(
    const uint64_t tenant_id,
    const ObIArray<share::ObUnit> &unit_array)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  share::ObUnitTableTransaction trans;
  common::ObArray<share::ObResourcePool *> *pools = nullptr;
  common::ObArray<uint64_t> adding_unit_ids;
  common::ObArray<common::ObArray<uint64_t>> deleting_unit_ids_by_pool;
  ObUnitTableOperator unit_operator;
  ObArray<share::ObUnit> unit_array_in_trans;

  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", KR(ret), K(loaded_), K(inited_));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || 0 == unit_array.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(trans.start(proxy_, OB_SYS_TENANT_ID))) {
    LOG_WARN("start transaction failed", KR(ret));
  //由于存在多个resource_pool同时修改，在当时判断resource_pool变更结束，不代表现在所有
  //的resource_pool还是一样的，需要在锁内再次获取unit_array，判断unit_array没有发生任何变化才可以
  } else if (OB_FAIL(unit_operator.init(trans))) {
    LOG_WARN("failed to init unit operator", KR(ret));
  } else if (OB_FAIL(unit_operator.get_units_by_tenant(tenant_id, unit_array_in_trans))) {
    LOG_WARN("failed to get unit array", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObLSBalanceTaskHelper::check_unit_array_same(unit_array, unit_array_in_trans))) {
    LOG_WARN("unit array not same, try again", KR(ret), K(unit_array), K(unit_array_in_trans), K(unit_array));
  } else if (OB_FAIL(get_pools_by_tenant_(tenant_id, pools))) {
    LOG_WARN("failed to get pool by tenant", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(NULL == pools)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pool ptr is null", KR(ret), KP(pools));
  } else if (OB_FAIL(commit_alter_unit_num_in_trans_(*pools, trans, adding_unit_ids, deleting_unit_ids_by_pool))) {
    LOG_WARN("failed to shrink in trans", KR(ret), KPC(pools));
  }
  const bool commit = (OB_SUCCESS == ret);
  int tmp_ret = OB_SUCCESS;
  if (OB_SUCCESS != (tmp_ret = trans.end(commit))) {
    LOG_WARN("trans end failed", K(tmp_ret), K(commit));
    ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
  }
  // update in-memory unit info
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(pools->count() != deleting_unit_ids_by_pool.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pools count must equal to unit count", KR(ret), KPC(pools), K(deleting_unit_ids_by_pool));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < pools->count(); ++i) {
      share::ObResourcePool *pool = pools->at(i);
      if (OB_ISNULL(pool)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pool is null", KR(ret), K(i), KPC(pools));
      } else if (OB_FAIL(delete_inmemory_units(pool->resource_pool_id_, deleting_unit_ids_by_pool.at(i)))) {
        LOG_WARN("failed to delete inmemory units", KR(ret), KPC(pool), K(i), K(deleting_unit_ids_by_pool));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < adding_unit_ids.count(); ++i) {
      ObUnit *unit = nullptr;
      if (OB_FAIL(get_unit_by_id(adding_unit_ids.at(i), unit))) {
        LOG_WARN("get_unit_by_id failed", K(adding_unit_ids.at(i)), KR(ret));
      } else if (OB_ISNULL(unit)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit is null", KP(unit), K(ret));
      } else {
        unit->status_ = ObUnit::UNIT_STATUS_ACTIVE;
      }
    }
  }
  return ret;
}

int ObUnitManager::commit_alter_unit_num_in_trans_(
  const common::ObIArray<share::ObResourcePool *> &pools,
  common::ObMySQLTransaction &trans,
  common::ObIArray<uint64_t> &adding_unit_ids,
  common::ObIArray<common::ObArray<uint64_t>> &pools_deleting_unit_ids)
{
  int ret = OB_SUCCESS;
  ObArray<ObUnit *> *units = NULL;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", KR(ret), K(loaded_), K(inited_));
  } else if (OB_UNLIKELY(0 == pools.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("pool ptr is null", KR(ret), K(pools));
  } else {
    for (int64_t index = 0; OB_SUCC(ret) && index < pools.count(); ++index) {
      // Commit shrink resource pool only needs to change the state of the unit,
      // the state of the resource pool itself has been changed before,
      // and there is no need to adjust it again.
      units = NULL;
      share::ObResourcePool *pool = pools.at(index);
      if (OB_ISNULL(pool)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pool is null", KR(ret), K(index), K(pools));
      } else if (OB_FAIL(get_units_by_pool(pool->resource_pool_id_, units))) {
        LOG_WARN("failed to get pool unit", KR(ret), KPC(pool));
      } else if (OB_ISNULL(units)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("units is null of the pool", KR(ret), KPC(pool));
      } else {
        common::ObArray<uint64_t> deleting_unit_ids;
        const int64_t unit_count = pool->unit_count_;
        if (unit_count <= 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unit count unexpected", KR(ret), K(unit_count));
        } else if (units->count() <= 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("zone unit ptrs has no element", KR(ret), "unit_cnt",
                   units->count());
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < units->count(); ++i) {
            const ObUnit *unit = units->at(i);
            if (OB_ISNULL(unit)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unit ptr is null", KR(ret), KP(unit), K(i), KPC(units));
            } else if (ObUnit::UNIT_STATUS_DELETING == unit->status_) {
              if (OB_FAIL(ut_operator_.remove_unit(trans, *unit))) {
                LOG_WARN("fail to remove unit", KR(ret), "unit", *unit);
              } else if (OB_FAIL(deleting_unit_ids.push_back(unit->unit_id_))) {
                LOG_WARN("fail to push back", KR(ret), KPC(unit));
              } else {} // no more to do
            } else if (ObUnit::UNIT_STATUS_ADDING == unit->status_) {
              ObUnit new_unit = *unit;
              new_unit.status_ = ObUnit::UNIT_STATUS_ACTIVE;
              if (OB_FAIL(ut_operator_.update_unit_exclude_ug_id(trans, new_unit))) {
                LOG_WARN("fail to update unit", KR(ret), "unit", *unit);
              } else if (OB_FAIL(adding_unit_ids.push_back(unit->unit_id_))) {
                LOG_WARN("fail to push back", KR(ret), KPC(unit));
              } else {} // no more to do
            } else if (ObUnit::UNIT_STATUS_ACTIVE == unit->status_) {
              // active unit, do nothing
            } else {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected unit status", K(ret), KPC(unit));
            }
          }// end for each unit
          if (FAILEDx(pools_deleting_unit_ids.push_back(deleting_unit_ids))) {
            LOG_WARN("failed to push back units", KR(ret), K(deleting_unit_ids));
          }
        }
      }
    }//end for each pool
  }
  return ret;
}

// NOTE: get from inner-table for accurate unit_group_id_
int ObUnitManager::get_deleting_units_of_pool(
    const uint64_t resource_pool_id,
    ObIArray<share::ObUnit> &units) const
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  ObArray<uint64_t> pool_ids;
  ObArray<ObUnit> pool_units;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(ret), K(loaded_), K(inited_));
  } else if (OB_UNLIKELY(OB_INVALID_ID == resource_pool_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(resource_pool_id));
  } else if (OB_FAIL(pool_ids.push_back(resource_pool_id))) {
    LOG_WARN("fail to push back", KR(ret));
  } else if (OB_FAIL(ut_operator_.get_units_by_resource_pool_ids(pool_ids, pool_units))) {
    LOG_WARN("fail to get units by resource pool ids", KR(ret), K(resource_pool_id));
  } else {
    units.reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < pool_units.count(); ++i) {
      const share::ObUnit &this_unit = pool_units.at(i);
      if (ObUnit::UNIT_STATUS_DELETING == this_unit.status_) {
        if (OB_FAIL(units.push_back(this_unit))) {
          LOG_WARN("fail to push back", K(ret));
        } else {} // no more to do
      } else if (ObUnit::UNIT_STATUS_ACTIVE == this_unit.status_ ||
                 ObUnit::UNIT_STATUS_ADDING == this_unit.status_) {
        // ignore
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected unit status", K(ret), "unit_status", this_unit.status_);
      }
    }
    LOG_INFO("get deleting units of pool", K(ret), K(resource_pool_id), K(units));
  }
  return ret;
}

int ObUnitManager::get_unit_infos_of_pool(const uint64_t resource_pool_id,
                                          ObIArray<ObUnitInfo> &unit_infos) const
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(inited_), K(loaded_), K(ret));
  } else if (OB_INVALID_ID == resource_pool_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(resource_pool_id), K(ret));
  } else if (OB_FAIL(inner_get_unit_infos_of_pool_(resource_pool_id, unit_infos))) {
    LOG_WARN("inner_get_unit_infos_of_pool failed", K(resource_pool_id), K(ret));
  }
  return ret;
}

int ObUnitManager::inner_get_unit_infos_of_pool_(const uint64_t resource_pool_id,
                                                ObIArray<ObUnitInfo> &unit_infos) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(inited_), K(loaded_), K(ret));
  } else if (OB_INVALID_ID == resource_pool_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(resource_pool_id), K(ret));
  } else {
    unit_infos.reuse();
    ObUnitInfo unit_info;
    share::ObResourcePool  *pool = NULL;
    ObUnitConfig *config = NULL;
    if (OB_FAIL(get_resource_pool_by_id(resource_pool_id, pool))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_RESOURCE_POOL_NOT_EXIST;
      }
      LOG_WARN("get_resource_pool_by_id failed", K(resource_pool_id), K(ret));
    } else if (NULL == pool) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pool is null", KP(pool), K(ret));
    } else if (OB_FAIL(get_unit_config_by_id(pool->unit_config_id_, config))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_RESOURCE_UNIT_NOT_EXIST;
      }
      LOG_WARN("get_unit_config_by_id failed", "unit config id", pool->unit_config_id_, K(ret));
    } else if (NULL == config) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("config is null", KP(config), K(ret));
    } else {
      unit_info.reset();
      if (OB_FAIL(unit_info.pool_.assign(*pool))) {
        LOG_WARN("failed to assign unit_info.pool_", K(ret));
      } else {
        unit_info.config_ = *config;
        ObArray<ObUnit *> *units = NULL;
        if (OB_FAIL(get_units_by_pool(resource_pool_id, units))) {
          LOG_WARN("get_units_by_pool failed", K(resource_pool_id), K(ret));
        } else if (NULL == units) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("units is null", KP(units), K(ret));
        } else if (units->count() <= 0) {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("units of resource pool not exist", K(resource_pool_id), K(ret));
        } else if (OB_FAIL(unit_infos.reserve(units->count()))) {
          LOG_WARN("failed to reserve for unit_infos", KR(ret));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < units->count(); ++i) {
            if (NULL == units->at(i)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unit is null", "unit", OB_P(units->at(i)), K(ret));
            } else {
              unit_info.unit_ = *units->at(i);
              if (OB_FAIL(unit_infos.push_back(unit_info))) {
                LOG_WARN("push_back failed", K(ret));
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObUnitManager::inner_get_unit_info_by_id(const uint64_t unit_id, ObUnitInfo &unit_info) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(inited_), K(loaded_), K(ret));
  } else if (OB_INVALID_ID == unit_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(unit_id), K(ret));
  } else {
    ObUnit *unit = NULL;
    ObUnitConfig *config = NULL;
    share::ObResourcePool  *pool = NULL;
    if (OB_FAIL(get_unit_by_id(unit_id, unit))) {
      LOG_WARN("get_unit_by_id failed", K(unit_id), K(ret));
    } else if (NULL == unit) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unit is null", KP(unit), K(ret));
    } else if (OB_FAIL(get_resource_pool_by_id(unit->resource_pool_id_, pool))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_RESOURCE_POOL_NOT_EXIST;
      }
      LOG_WARN("get_resource_pool_by_id failed", "pool id", unit->resource_pool_id_, K(ret));
    } else if (NULL == pool) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pool is null", KP(pool), K(ret));
    } else if (OB_FAIL(get_unit_config_by_id(pool->unit_config_id_, config))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_RESOURCE_UNIT_NOT_EXIST;
      }
      LOG_WARN("get_unit_config_by_id failed", "unit config id", pool->unit_config_id_, K(ret));
    } else if (NULL == config) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("config is null", KP(config), K(ret));
    } else {
      if (OB_FAIL(unit_info.pool_.assign(*pool))) {
        LOG_WARN("failed to assign unit_info.pool_", K(ret));
      } else {
        unit_info.unit_ = *unit;
        unit_info.config_ = *config;
      }
    }
  }
  return ret;
}

int ObUnitManager::extract_unit_ids(
    const common::ObIArray<share::ObUnit *> &units,
    common::ObIArray<uint64_t> &unit_ids)
{
  int ret = OB_SUCCESS;
  unit_ids.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < units.count(); ++i) {
    if (OB_UNLIKELY(NULL == units.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unit ptr is null", K(ret));
    } else if (OB_FAIL(unit_ids.push_back(units.at(i)->unit_id_))) {
      LOG_WARN("fail to push back", K(ret));
    } else {} // no more to do
  }
  return ret;
}

int ObUnitManager::inner_get_unit_ids(ObIArray<uint64_t> &unit_ids) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(inited_), K(loaded_), K(ret));
  }
  for (ObHashMap<uint64_t, ObArray<ObUnit *> *>::const_iterator it = pool_unit_map_.begin();
      OB_SUCCESS == ret && it != pool_unit_map_.end(); ++it) {
    if (NULL == it->second) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("pointer of ObArray<ObUnit *> is null", "pool_id", it->first, K(ret));
    } else if (it->second->count() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("array of unit is empty", "pool_id", it->first, K(ret));
    } else {
      const ObArray<ObUnit *> units = *it->second;
      for (int64_t i = 0; OB_SUCC(ret) && i < units.count(); ++i) {
        uint64_t unit_id = units.at(i)->unit_id_;
        if (OB_FAIL(unit_ids.push_back(unit_id))) {
          LOG_WARN("fail push back it", K(unit_id), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObUnitManager::get_unit_ids(ObIArray<uint64_t> &unit_ids) const
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  if (OB_FAIL(inner_get_unit_ids(unit_ids))) {
    LOG_WARN("fail to inner get unit ids", K(ret));
  }
  return ret;
}

int ObUnitManager::calc_sum_load(const ObArray<ObUnitLoad> *unit_loads,
                                 ObUnitConfig &sum_load,
                                 const bool include_ungranted_unit)
{
  int ret = OB_SUCCESS;
  sum_load.reset();
  if (NULL == unit_loads) {
    // all be zero
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < unit_loads->count(); ++i) {
      if (!unit_loads->at(i).is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid unit_load", "unit_load", unit_loads->at(i), K(ret));
      } else if (!is_valid_tenant_id(unit_loads->at(i).get_tenant_id())
                 && !include_ungranted_unit) {
        // skip this unit_load
      } else {
        sum_load += *unit_loads->at(i).unit_config_;
      }
    }
  }
  return ret;
}

int ObUnitManager::check_resource_pool(share::ObResourcePool &resource_pool) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(inited_), K(loaded_), K(ret));
  } else if (resource_pool.name_.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "resource pool name");
    LOG_WARN("invalid resource pool name", "resource pool name", resource_pool.name_, K(ret));
  } else if (resource_pool.unit_count_ <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "unit num");
    LOG_WARN("invalid resource unit num", "unit num", resource_pool.unit_count_, K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < resource_pool.zone_list_.count(); ++i) {
      for (int64_t j = i + 1; OB_SUCC(ret) && j < resource_pool.zone_list_.count(); ++j) {
        if (resource_pool.zone_list_[i] == resource_pool.zone_list_[j]) {
          ret = OB_ZONE_DUPLICATED;
          ObCStringHelper helper;
          LOG_USER_ERROR(OB_ZONE_DUPLICATED, helper.convert(resource_pool.zone_list_[i]),
              helper.convert(resource_pool.zone_list_));
          LOG_WARN("duplicate zone in zone list", "zone_list", resource_pool.zone_list_, K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      FOREACH_CNT_X(zone, resource_pool.zone_list_, OB_SUCCESS == ret) {
        bool zone_exist = false;
        if (OB_FAIL(zone_mgr_.check_zone_exist(*zone, zone_exist))) {
          LOG_WARN("check_zone_exist failed", KPC(zone), K(ret));
        } else if (!zone_exist) {
          ret = OB_ZONE_INFO_NOT_EXIST;
          ObCStringHelper helper;
          LOG_USER_ERROR(OB_ZONE_INFO_NOT_EXIST, helper.convert(*zone));
          LOG_WARN("zone not exist", "zone", *zone, K(ret));
        }
      }
    }
    if (OB_SUCCESS == ret && 0 == resource_pool.zone_list_.count()) {
      ObArray<ObZoneInfo> zone_infos;
      if (OB_FAIL(zone_mgr_.get_zone(zone_infos))) {
        LOG_WARN("get_zone failed", K(ret));
      } else {
        FOREACH_CNT_X(zone_info, zone_infos, OB_SUCCESS == ret) {
          if (OB_FAIL(resource_pool.zone_list_.push_back(zone_info->zone_))) {
            LOG_WARN("push_back failed", K(ret));
          }
        }
        if (OB_SUCCESS == ret && resource_pool.zone_list_.count() <= 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("not active zone found", K(ret));
        }
      }
    }
    FOREACH_CNT_X(zone, resource_pool.zone_list_, OB_SUCCESS == ret) {
      int64_t alive_server_count = 0;
      if (OB_FAIL(SVR_TRACER.get_alive_servers_count(*zone, alive_server_count))) {
        LOG_WARN("get_alive_servers failed", KR(ret), KPC(zone));
      } else if (alive_server_count < resource_pool.unit_count_) {
        ret = OB_UNIT_NUM_OVER_SERVER_COUNT;
        LOG_WARN("resource pool unit num over zone server count", "unit_count",
            resource_pool.unit_count_, K(alive_server_count), K(ret), "zone", *zone);
      }
    }
  }
  return ret;
}

int ObUnitManager::allocate_new_pool_units_(
    ObISQLClient &client,
    const share::ObResourcePool  &pool,
    const char *module)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(inited_), K(loaded_), K(ret));
  } else if (!pool.is_valid() || pool.zone_list_.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid pool", K(pool), K(ret));
  } else {
    const bool new_allocate_pool = true;
    const int64_t delta_unit_num = pool.unit_count_;
    ObArray<ObUnit> new_units;
    if (OB_FAIL(allocate_pool_units_(
            client, pool, pool.zone_list_, nullptr, new_allocate_pool, delta_unit_num, ObUnit::UNIT_STATUS_ACTIVE, module, new_units))) {
      LOG_WARN("allocate pool units failed", K(module), KR(ret), K(delta_unit_num), K(pool));
    }
  }
  return ret;
}

/* Notify creating or dropping unit on ObServer.
 * 1. Specify @is_delete as true when dropping unit,
 *    and only @tenant_id and @unit will be used in this case.
 * 2. This function merely sends RPC call, so executor should make sure waiting is called later.
 *    But there is one exception that when @unit is on this server where RS Leader locates,
 *    notification will be locally executed without RPC, which means no need to wait proxy.
 */
int ObUnitManager::try_notify_tenant_server_unit_resource_(
    const uint64_t tenant_id,
    const bool is_delete,
    ObNotifyTenantServerResourceProxy &notify_proxy,
    const uint64_t unit_config_id,
    const lib::Worker::CompatMode compat_mode,
    const share::ObUnit &unit,
    const bool if_not_grant,
    const bool skip_offline_server,
    const bool check_data_version)
{
  int ret = OB_SUCCESS;
  bool is_alive = false;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(ret), K(inited_), K(loaded_));
  } else if (OB_FAIL(SVR_TRACER.check_server_alive(unit.server_, is_alive))) {
    LOG_WARN("fail to get server_info", KR(ret), K(unit.server_));
  } else if (!is_alive && (is_delete || skip_offline_server)) {
    // do nothing
    LOG_INFO("ignore not alive server when is_delete or skip_offline_server is true",
              "server", unit.server_, K(is_delete), K(skip_offline_server));
  } else if (!is_valid_tenant_id(tenant_id)) {
    // do nothing, unit not granted
  } else {
    // STEP 1: Get and init notifying arg
    obrpc::TenantServerUnitConfig tenant_unit_server_config;
    if (!is_delete) {
      const bool should_check_data_version = check_data_version && is_user_tenant(tenant_id);
      if (OB_FAIL(build_notify_create_unit_resource_rpc_arg_(
                    tenant_id, unit, compat_mode, unit_config_id, if_not_grant,
                    tenant_unit_server_config))) {
        LOG_WARN("fail to init tenant_unit_server_config", KR(ret), K(tenant_id), K(is_delete));
      } else if (should_check_data_version
                 && OB_FAIL(check_dest_data_version_is_loaded_(tenant_id, unit.server_))) {
        LOG_WARN("fail to check dest data_version is loaded", KR(ret), K(tenant_id), "dst", unit.server_);
      }
    } else {
      if (OB_FAIL(tenant_unit_server_config.init_for_dropping(tenant_id, is_delete))) {
        LOG_WARN("fail to init tenant_unit_server_config", KR(ret), K(tenant_id), K(is_delete));
      }
    }

    // STEP 2: Do notification
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(do_notify_unit_resource_(
                unit.server_, tenant_unit_server_config, notify_proxy))) {
      LOG_WARN("failed to do_notify_unit_resource", "dst", unit.server_, K(tenant_unit_server_config));
      if (OB_TENANT_EXIST == ret) {
        ret = OB_TENANT_RESOURCE_UNIT_EXIST;
        ObCStringHelper helper;
        LOG_USER_ERROR(OB_TENANT_RESOURCE_UNIT_EXIST, tenant_id, helper.convert(unit.server_));
      }
    }
  }
  return ret;
}

int ObUnitManager::check_dest_data_version_is_loaded_(
    const uint64_t tenant_id, const ObAddr &addr)
{
 int ret = OB_SUCCESS;
 ObTimeoutCtx ctx;
 const int64_t DEFTAULT_TIMEOUT_TS = 5 * GCONF.rpc_timeout;
 char ip_buf[OB_IP_STR_BUFF] = "";
 if (OB_FAIL(check_inner_stat_())) {
   LOG_WARN("check_inner_stat failed", KR(ret), K(inited_), K(loaded_));
 } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
            || !addr.is_valid()
            || OB_ISNULL(proxy_))) {
   ret = OB_INVALID_ARGUMENT;
   LOG_WARN("invalid arg", KR(ret), K(tenant_id), K(addr), KP_(proxy));
 } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, DEFTAULT_TIMEOUT_TS))) {
   LOG_WARN("fail to set default timeout ctx", KR(ret));
 } else if (OB_UNLIKELY(!addr.ip_to_string(ip_buf, sizeof(ip_buf)))) {
   ret = OB_ERR_UNEXPECTED;
   LOG_WARN("fail to convert ip to string", KR(ret), K(tenant_id), K(addr));
 } else {
   const int64_t start_timeout_ts = ObTimeUtility::current_time();
   const int64_t CHECK_INTERVAL_TS = 500 * 1000L; // 500ms
   const int64_t SLEEP_TS = 100 * 1000L; // 100ms
   ObSqlString sql;
   if (OB_FAIL(sql.assign_fmt("SELECT IF(value = '0.0.0.0', 0, 1) AS loaded "
                              "FROM %s WHERE tenant_id = %lu AND name = 'compatible' "
                              "AND svr_ip = '%s' AND svr_port = %d",
                              OB_ALL_VIRTUAL_TENANT_PARAMETER_INFO_TNAME,
                              tenant_id, ip_buf, addr.get_port()))) {
     LOG_WARN("fail to assign fmt", KR(ret), K(tenant_id), K(addr));
   }
   while (OB_SUCC(ret)) {
     if (OB_UNLIKELY(ctx.is_timeouted())) {
       ret = OB_TIMEOUT;
       LOG_WARN("check dest data version timeout", KR(ret),
                K(start_timeout_ts), "abs_timeout", ctx.get_abs_timeout());
     } else {
       SMART_VAR(ObMySQLProxy::MySQLResult, res) {
         sqlclient::ObMySQLResult *result = NULL;
         if (OB_FAIL(proxy_->read(res, OB_SYS_TENANT_ID, sql.ptr()))) {
           LOG_WARN("fail to read by sql", KR(ret), K(sql));
         } else if (OB_ISNULL(result = res.get_result())) {
           ret = OB_ERR_UNEXPECTED;
           LOG_WARN("result is null", KR(ret));
         } else if (OB_FAIL(result->next())) {
           if (OB_ITER_END == ret) {
             ret = OB_SUCCESS;
             if (REACH_TIME_INTERVAL(CHECK_INTERVAL_TS)) {
               LOG_WARN_RET(OB_EAGAIN, "check data_version is loaded, but result is empty, try later",
                            K(tenant_id), K(addr));
             }
           } else {
             LOG_WARN("fail to get next row", KR(ret));
           }
         } else {
           int64_t loaded = 0;
           EXTRACT_INT_FIELD_MYSQL(*result, "loaded", loaded, int64_t);
           if (OB_SUCC(ret)) {
             if (1 == loaded) {
               break;
             } else if (REACH_TIME_INTERVAL(CHECK_INTERVAL_TS))
               LOG_WARN_RET(OB_EAGAIN, "check data_version is loaded, but it's not refreshed yet, try later",
                            K(tenant_id), K(addr));
           }
         }
       } // end SMART_VAR

       if (OB_SUCC(ret)) {
         ob_usleep(SLEEP_TS);
       }
     }
   } // end while
 }
 return ret;
}

int ObUnitManager::build_notify_create_unit_resource_rpc_arg_(
    const uint64_t tenant_id,
    const share::ObUnit &unit,
    const lib::Worker::CompatMode compat_mode,
    const uint64_t unit_config_id,
    const bool if_not_grant,
    obrpc::TenantServerUnitConfig &rpc_arg) const
{
  int ret = OB_SUCCESS;
  // get unit_config
  share::ObUnitConfig *unit_config = nullptr;
  if (OB_FAIL(get_unit_config_by_id(unit_config_id, unit_config))) {
    LOG_WARN("fail to get unit config by id", KR(ret));
  } else if (OB_ISNULL(unit_config)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unit config is null", KR(ret), "unit_config_id", unit_config_id);
  }

#ifdef OB_BUILD_TDE_SECURITY
  // get root_key
  obrpc::ObRootKeyResult root_key;
  obrpc::ObRootKeyArg get_rootkey_arg;
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_ISNULL(root_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("root_service_ is nullptr", KR(ret), KP(root_service_));
  } else if (OB_FAIL(get_rootkey_arg.init_for_get(tenant_id))) {
    LOG_WARN("failed to init get_root_key arg", KR(ret), K(tenant_id));
  } else if (OB_FAIL(root_service_->handle_get_root_key(get_rootkey_arg, root_key))) {
    LOG_WARN("fail to get root_key", KR(ret), K(get_rootkey_arg));
  }
#endif

  // init rpc_arg
  const bool is_delete = false;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(rpc_arg.init(tenant_id,
                                  unit.unit_id_,
                                  compat_mode,
                                  *unit_config,
                                  unit.replica_type_,
                                  if_not_grant,
                                  is_delete
#ifdef OB_BUILD_TDE_SECURITY
                                  , root_key
#endif
                                  ))) {
    LOG_WARN("fail to init rpc_arg", KR(ret), K(tenant_id), K(is_delete));
  }
  return ret;
}

int ObUnitManager::do_notify_unit_resource_(
  const common::ObAddr server,
  const obrpc::TenantServerUnitConfig &notify_arg,
  ObNotifyTenantServerResourceProxy &notify_proxy)
{
  int ret = OB_SUCCESS;
  if (GCONF.self_addr_ == server) {
    // Directly call local interface without using RPC
    if (OB_FAIL(omt::ObTenantNodeBalancer::get_instance().handle_notify_unit_resource(notify_arg))) {
      LOG_WARN("fail to handle_notify_unit_resource", K(ret), K(notify_arg));
    } else {
      LOG_INFO("call notify resource to server (locally)", KR(ret), "dst", server, K(notify_arg));
    }
  } else {
    int64_t start = ObTimeUtility::current_time();
    int64_t rpc_timeout = NOTIFY_RESOURCE_RPC_TIMEOUT;
    if (INT64_MAX != THIS_WORKER.get_timeout_ts()) {
      rpc_timeout = max(rpc_timeout, THIS_WORKER.get_timeout_remain());
    }
    if (OB_FAIL(notify_proxy.call(server, rpc_timeout, notify_arg))) {
      LOG_WARN("fail to call notify resource to server", KR(ret), K(rpc_timeout),
               "dst", server, "cost", ObTimeUtility::current_time() - start);
    } else {
      LOG_INFO("call notify resource to server", KR(ret), "dst", server, K(notify_arg),
               "cost", ObTimeUtility::current_time() - start);
    }
  }
  return ret;
}

int ObUnitManager::rollback_persistent_units_(
    const uint64_t tenant_id,
    const common::ObArray<share::ObUnit> &units,
    ObNotifyTenantServerResourceProxy &notify_proxy)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const bool is_delete = true;
  ObArray<int> return_ret_array;
  notify_proxy.reuse();
  for (int64_t i = 0; i < units.count(); i++) {
    const ObUnit & unit = units.at(i);
    const lib::Worker::CompatMode dummy_mode = lib::Worker::CompatMode::INVALID;
    const uint64_t dummy_config_id = OB_INVALID_ID;   // delete unit no need config
    if (OB_TMP_FAIL(try_notify_tenant_server_unit_resource_(
        tenant_id, is_delete, notify_proxy,
        dummy_config_id, dummy_mode, unit,
        false/*if_not_grant*/, false/*skip_offline_server*/,
        false /*check_data_version*/))) {
      ret = OB_SUCC(ret) ? tmp_ret : ret;
      LOG_WARN("fail to try notify server unit resource", KR(ret), KR(tmp_ret),
               K(is_delete), K(tenant_id), K(dummy_mode), K(unit));
    }
  }
  if (OB_TMP_FAIL(notify_proxy.wait_all(return_ret_array))) {
    LOG_WARN("fail to wait notify resource", KR(ret), K(tmp_ret));
    ret = OB_SUCC(ret) ? tmp_ret : ret;
  } else if (OB_FAIL(ret)) {
  } else {
    // don't use arg/dest/result here because call() may has failure.
    ObAddr invalid_addr;
    for (int64_t i = 0; OB_SUCC(ret) && i < return_ret_array.count(); i++) {
      const int ret_i = return_ret_array.at(i);
      const ObAddr &addr = return_ret_array.count() != notify_proxy.get_dests().count() ?
                           invalid_addr : notify_proxy.get_dests().at(i);
      // if (OB_SUCCESS != ret_i && OB_TENANT_NOT_IN_SERVER != ret_i) {
      if (OB_SUCCESS != ret_i && OB_TENANT_NOT_IN_SERVER != ret_i) {
        ret = ret_i;
        LOG_WARN("fail to mark tenant removed", KR(ret), KR(ret_i), K(addr));
      }
    }
  }
  LOG_WARN("rollback persistent unit", KR(ret), K(tenant_id), K(units));
  return ret;
}

int ObUnitManager::get_tenant_unit_servers(
    const uint64_t tenant_id,
    const common::ObZone &zone,
    common::ObIArray<common::ObAddr> &server_array) const
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  if (OB_FAIL(get_tenant_unit_servers_(tenant_id, zone, server_array))) {
    LOG_WARN("fail to get_tenant_unit_servers_", KR(ret), K(tenant_id), K(zone));
  }
  return ret;
}

int ObUnitManager::get_tenant_unit_servers_(
    const uint64_t tenant_id,
    const common::ObZone &zone,
    common::ObIArray<common::ObAddr> &server_array) const
{
  int ret = OB_SUCCESS;
  ObArray<share::ObResourcePool *> *pools = nullptr;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else {
    server_array.reset();
    int tmp_ret = get_pools_by_tenant_(tenant_id, pools);
    if (OB_ENTRY_NOT_EXIST == tmp_ret) {
      // pass, and return empty server array
    } else if (OB_SUCCESS != tmp_ret) {
      ret = tmp_ret;
      LOG_WARN("fail to get pools by tenant", K(ret), K(tenant_id));
    } else if (nullptr == pools) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pools ptr is null, unexpected", K(ret), K(tenant_id));
    } else {
      common::ObArray<common::ObAddr> this_server_array;
      for (int64_t i = 0; OB_SUCC(ret) && i < pools->count(); ++i) {
        this_server_array.reset();
        const share::ObResourcePool *pool = pools->at(i);
        if (OB_UNLIKELY(nullptr == pool)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("pool ptr is null", K(ret), K(tenant_id));
        } else if (OB_FAIL(get_pool_servers(
                pool->resource_pool_id_, zone, this_server_array))) {
          LOG_WARN("fail to get pool server", K(ret),
                   "pool_id", pool->resource_pool_id_, K(zone));
        } else if (OB_FAIL(append(server_array, this_server_array))) {
          LOG_WARN("fail to append", K(ret));
        }
      }
    }
  }
  return ret;
}
ERRSIM_POINT_DEF(ERRSIM_UNIT_PERSISTENCE_ERROR);

// allocate unit on target zones for specified resource pool
//
// @param [in] client                     SQL client
// @param [in] pool                       resource pool which new allocated units belong to
// @param [in] zones                      target zones to allocate units
// @param [in] unit_group_id_array        unit group id array for new allocated units
// @param [in] new_allocate_pool          whether to allocate new pool
// @param [in] increase_delta_unit_num    unit number to be increased
// @param [in] module                     module name for print log
// @param [out] new_servers               allocated servers for new units
//
// @ret OB_ZONE_RESOURCE_NOT_ENOUGH   zone resource not enough to hold all units
// @ret OB_ZONE_SERVER_NOT_ENOUGH     zone server not enough to hold all units
// @ret OB_SUCCESS                    success
int ObUnitManager::allocate_pool_units_(
    ObISQLClient &client,
    const share::ObResourcePool &pool,
    const common::ObIArray<common::ObZone> &zones,
    common::ObIArray<uint64_t> *unit_group_id_array,
    const bool new_allocate_pool,
    const int64_t increase_delta_unit_num,
    const ObUnit::Status new_unit_status,
    const char *module,
    ObIArray<ObUnit> &new_units)
{
  int ret = OB_SUCCESS;
  ObUnitConfig *config = NULL;
  lib::Worker::CompatMode compat_mode = lib::Worker::CompatMode::INVALID;
  ObArray<ObServerInfoInTable> servers_info;
  ObArray<ObServerInfoInTable> active_servers_info_of_zone;
  ObArray<obrpc::ObGetServerResourceInfoResult> active_servers_resource_info_of_zone;

  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(inited_), K(loaded_), K(ret));
  } else if (!pool.is_valid() || zones.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid resource pool or zones", K(pool), K(zones), KR(ret));
  } else if (increase_delta_unit_num <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(increase_delta_unit_num), K(ret));
  } else if (nullptr != unit_group_id_array
      && unit_group_id_array->count() != increase_delta_unit_num) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("new unit group id array status not match",
        KR(ret), K(increase_delta_unit_num), KPC(unit_group_id_array));
  } else if (!ObUnit::is_valid_status(new_unit_status)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid new unit status", K(new_unit_status), KR(ret));
  } else if (OB_ISNULL(srv_rpc_proxy_) || OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("srv_rpc_proxy_ or GCTX.sql_proxy_ is null", KR(ret), KP(srv_rpc_proxy_), KP(GCTX.sql_proxy_));
  } else if (is_valid_tenant_id(pool.tenant_id_)
      && OB_FAIL(ObCompatModeGetter::get_tenant_mode(pool.tenant_id_, compat_mode))) {
    LOG_WARN("fail to get tenant compat mode", KR(ret), K(pool.tenant_id_));
  } else if (OB_FAIL(get_unit_config_by_id(pool.unit_config_id_, config))) {
    LOG_WARN("get_unit_config_by_id failed", "unit_config_id", pool.unit_config_id_, KR(ret));
  } else if (OB_ISNULL(config)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("config is null", KP(config), K(ret));
  } else {
    ObNotifyTenantServerResourceProxy notify_proxy(
        *srv_rpc_proxy_,
        &obrpc::ObSrvRpcProxy::notify_tenant_server_unit_resource);

    ObArray<ObAddr> excluded_servers;
    ObArray<ObUnit> units;
    for (int64_t i = 0; OB_SUCC(ret) && i < zones.count(); ++i) { // for each zone
      const ObZone &zone = zones.at(i);
      excluded_servers.reuse();
      active_servers_info_of_zone.reuse();
      active_servers_resource_info_of_zone.reuse();
      const bool ONLY_ACTIVE_SERVERS = true;
      if (FAILEDx(get_excluded_servers(pool.resource_pool_id_, zone, module,
              new_allocate_pool, excluded_servers))) {
        LOG_WARN("get excluded servers fail", KR(ret), K(pool.resource_pool_id_), K(zone),
            K(module), K(new_allocate_pool));
      } else if (OB_FAIL(ObServerTableOperator::get_servers_info_of_zone(
          *GCTX.sql_proxy_,
          zone,
          ONLY_ACTIVE_SERVERS,
          active_servers_info_of_zone))) {
        LOG_WARN("fail to get servers info of zone", KR(ret), K(zone));
      } else if (OB_FAIL(get_servers_resource_info_via_rpc(
          active_servers_info_of_zone,
          active_servers_resource_info_of_zone))) {
        LOG_WARN("fail to get active_servers_resource_info_of_zone", KR(ret), K(active_servers_info_of_zone));
      }

      for (int64_t j = 0; OB_SUCC(ret) && j < increase_delta_unit_num; ++j) {
        uint64_t unit_id = OB_INVALID_ID;
        std::string resource_not_enough_reason;
        ObAddr server;
        if (OB_FAIL(choose_server_for_unit(
            config->unit_resource(),
            zone,
            excluded_servers,
            module,
            active_servers_info_of_zone,
            active_servers_resource_info_of_zone,
            server,
            resource_not_enough_reason))) {
          LOG_WARN("choose server for unit failed", K(module), KR(ret), "unit_idx", j, K(increase_delta_unit_num),
              K(zone), K(excluded_servers), KPC(config));
          // handle return error info
          if (OB_ZONE_RESOURCE_NOT_ENOUGH == ret) {
            ObCStringHelper helper;
            LOG_USER_ERROR(OB_ZONE_RESOURCE_NOT_ENOUGH,
                helper.convert(zone), increase_delta_unit_num, resource_not_enough_reason.c_str());
          } else if (OB_ZONE_SERVER_NOT_ENOUGH == ret) {
            ObCStringHelper helper;
            LOG_USER_ERROR(OB_ZONE_SERVER_NOT_ENOUGH, helper.convert(zone),
                increase_delta_unit_num);
          }
        } else if (OB_FAIL(excluded_servers.push_back(server))) {
          LOG_WARN("push_back failed", K(ret));
        } else if (OB_FAIL(fetch_new_unit_id(unit_id))) {
          LOG_WARN("fetch_new_unit_id failed", K(ret));
        } else {
          const bool is_delete = false; // is_delete is false when allocate new unit
          ObUnit unit;
          unit.reset();
          unit.unit_id_ = unit_id;
          unit.resource_pool_id_ = pool.resource_pool_id_;
          unit.unit_group_id_ = (nullptr == unit_group_id_array) ? 0 : unit_group_id_array->at(j);
          unit.server_ = server;
          unit.status_ = new_unit_status;
          unit.replica_type_ = pool.replica_type_;
          if (OB_FAIL(unit.zone_.assign(zone))) {
            LOG_WARN("fail to assign zone", KR(ret), K(zone));
          } else if (OB_FAIL(try_notify_tenant_server_unit_resource_(
              pool.tenant_id_, is_delete, notify_proxy,
              pool.unit_config_id_, compat_mode, unit, false/*if not grant*/,
              false/*skip offline server*/, true/*check_data_version*/))) {
            LOG_WARN("fail to try notify server unit resource", K(ret));
          } else if (OB_FAIL(add_unit(client, unit))) {
            LOG_WARN("add_unit failed", K(unit), K(ret));
          } else if (OB_FAIL(new_units.push_back(unit))) {
            LOG_WARN("push_back failed", K(ret));
          } else if (OB_FAIL(units.push_back(unit))) {
            LOG_WARN("fail to push an element into units", KR(ret), K(unit));
          }
        }
      }
    }
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(notify_proxy.wait())) {
      LOG_WARN("fail to wait notify resource", K(ret), K(tmp_ret));
      ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
    } else if (OB_SUCC(ret)) {
      // arg/dest/result can be used here.
    }
    if (is_valid_tenant_id(pool.tenant_id_)) {
      ret = ERRSIM_UNIT_PERSISTENCE_ERROR ? : ret;
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("start to rollback unit persistence", KR(ret), K(units), K(pool));
      if(OB_TMP_FAIL(rollback_persistent_units_(pool.tenant_id_, units, notify_proxy))) {
        LOG_WARN("fail to rollback unit persistence", KR(ret), KR(tmp_ret), K(units),
            K(pool), K(compat_mode));
      }
    }
  }
  return ret;
}

int ObUnitManager::get_excluded_servers(
    const ObUnit &unit,
    const ObUnitStat &unit_stat,
    const char *module,
    const ObIArray<ObServerInfoInTable> &servers_info, // servers info in unit.zone_
    const ObIArray<obrpc::ObGetServerResourceInfoResult> &report_servers_resource_info, // active servers' resource info in unit.zone_
    ObIArray<ObAddr> &servers) const
{
  int ret = OB_SUCCESS;
  //Add all OBS whose disks do not meet the requirements
  ObArray<share::ObServerStatus> server_list;
  ObServerResourceInfo server_resource_info;
  const bool new_allocate_pool = false;
  if (OB_FAIL(get_excluded_servers(unit.resource_pool_id_, unit.zone_, module,
          new_allocate_pool, servers))) {
    LOG_WARN("fail to get excluded_servers", K(ret), K(unit), K(new_allocate_pool));
  } else {
    for (int64_t i = 0; i < servers_info.count() && OB_SUCC(ret); i++) {
      const ObServerInfoInTable &server_info = servers_info.at(i);
      const ObAddr &server = server_info.get_server();
      bool is_exclude = false;
      server_resource_info.reset();
      if (!server_info.can_migrate_in()) {
        is_exclude = true;
        LOG_INFO("server can't migrate in, push into excluded_array", K(server_info), K(module));
      } else if (OB_FAIL(ObRootUtils::get_server_resource_info(
          report_servers_resource_info,
          server,
          server_resource_info))) {
        // server which can be migrated in must have its resource_info
        LOG_WARN("fail to get server_resource_info", KR(ret), K(report_servers_resource_info), K(server));
      } else {
        int64_t required_size = unit_stat.get_required_size() + server_resource_info.disk_in_use_;
        int64_t total_size = server_resource_info.disk_total_;
        if (total_size <= required_size || total_size <= 0) {
          is_exclude = true;
          LOG_INFO("server total size no bigger than required size", K(module), K(required_size),
              K(total_size), K(unit_stat), K(server_resource_info));
        } else if (required_size <= 0) {
          //nothing todo
        } else {
          int64_t required_percent = (100 * required_size) / total_size;
          int64_t limit_percent = GCONF.data_disk_usage_limit_percentage;
          if (required_percent > limit_percent) {
            is_exclude = true;
            LOG_INFO("server disk percent will out of control;", K(module), K(required_percent), K(limit_percent),
                     K(required_size), K(total_size));
          }
        }
      }
      if (!is_exclude) {
        //nothing todo
      } else if (has_exist_in_array(servers, server)) {
        //nothing todo
      } else if (OB_FAIL(servers.push_back(server))) {
        LOG_WARN("fail to push back", KR(ret), K(server));
      }
    }
  }
  return ret;
}

// get excluded servers for resource pool on target zone.
//
// 1. resource pool units servers on target zone
// 2. tenant all resource pool units servers on target zone
//
// FIXME: 4.0 not support resource pool intersect in zone, here need not consider multiple pools of tenant
//
// @param [in] resource_pool_id   specified resource pool id
// @param [in] zone               specified zone which may be empty that means all zones
// @param [in] new_allocate_pool  new allocate pool 
// @param [out] servers           returned excluded servers
int ObUnitManager::get_excluded_servers(const uint64_t resource_pool_id,
    const ObZone &zone,
    const char *module,
    const bool new_allocate_pool, 
    ObIArray<ObAddr> &excluded_servers) const
{
  int ret = OB_SUCCESS;
  share::ObResourcePool *pool = NULL;
  int64_t tenant_id = OB_INVALID_ID;
  common::ObArray<common::ObAddr> sys_standalone_servers;
  common::ObArray<share::ObResourcePool *> *all_pools = NULL;

  excluded_servers.reset();
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(inited_), K(loaded_), K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == resource_pool_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid resource pool id", KR(ret), K(resource_pool_id));
  } else if (new_allocate_pool) {
    //nothing todo
  } else if (OB_FAIL(get_resource_pool_by_id(resource_pool_id, pool))) {
    LOG_WARN("fail to get resource pool by id", K(ret), K(resource_pool_id));
  } else if (FALSE_IT(tenant_id = pool->tenant_id_)) {
    //nothing todo
  } else if (! is_valid_tenant_id(tenant_id)) {
    // The pool does not belong to any tenant, only get resource pool servers
    if (OB_FAIL(get_pool_servers(resource_pool_id, zone, excluded_servers))) {
      LOG_WARN("get_pool_servers failed", "resource pool id",
          resource_pool_id, K(zone), KR(ret));
    }
  }
  // get all tenant resource pool related servers on target zone
  else if (OB_FAIL(get_tenant_unit_servers_(tenant_id, zone, excluded_servers))) {
    LOG_WARN("get tennat unit server fail", KR(ret), K(tenant_id), K(zone));
  }

  if (OB_SUCC(ret) && GCONF.enable_sys_unit_standalone) {
    // When the system tenant is deployed independently,
    // the server where the unit of the system tenant is located is also required as the executed servers
    if (OB_FAIL(get_tenant_unit_servers_(OB_SYS_TENANT_ID, zone, sys_standalone_servers))) {
      LOG_WARN("fail to get tenant unit servers", KR(ret), K(zone));
    } else if (OB_FAIL(append(excluded_servers, sys_standalone_servers))) {
      LOG_WARN("fail to append other excluded servers", K(ret));
    }
  }

  LOG_INFO("get tenant resource pool servers and sys standalone servers as excluded servers",
      K(module), K(new_allocate_pool),
      KR(ret),
      K(resource_pool_id),
      K(tenant_id),
      K(zone),
      "enable_sys_unit_standalone", GCONF.enable_sys_unit_standalone,
      K(excluded_servers),
      K(sys_standalone_servers));
  return ret;
}


int ObUnitManager::get_pools_servers(const common::ObIArray<share::ObResourcePool  *> &pools,
    common::hash::ObHashMap<common::ObAddr, int64_t> &server_ref_count_map) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(inited_), K(loaded_), K(ret));
  } else if (pools.count() <= 0 || !server_ref_count_map.created()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("pools is empty", K(pools),
        "server_ref_count_map created", server_ref_count_map.created(), K(ret));
  } else {
    ObArray<ObAddr> servers;
    const ObZone all_zone;
    FOREACH_CNT_X(pool, pools, OB_SUCCESS == ret) {
      servers.reuse();
      if (NULL == *pool) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("pool is null", "pool", *pool, K(ret));
      } else if (OB_FAIL(get_pool_servers((*pool)->resource_pool_id_, all_zone, servers))) {
        LOG_WARN("get pool servers failed",
            "pool id", (*pool)->resource_pool_id_, K(all_zone), K(ret));
      } else {
        FOREACH_CNT_X(server, servers, OB_SUCCESS == ret) {
          int64_t server_ref_count = 0;
          if (OB_FAIL(get_server_ref_count(server_ref_count_map, *server, server_ref_count))) {
            if (OB_ENTRY_NOT_EXIST == ret) {
              server_ref_count = 1;
              ret = OB_SUCCESS;
            } else {
              LOG_WARN("get server ref count failed", "server", *server, K(ret));
            }
          } else {
            ++server_ref_count;
          }
          if (OB_SUCC(ret)) {
            if (OB_FAIL(set_server_ref_count(server_ref_count_map, *server, server_ref_count))) {
              LOG_WARN("set server ref count failed",
                  "server", *server, K(server_ref_count), K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObUnitManager::get_pool_servers(const uint64_t resource_pool_id,
                                    const ObZone &zone,
                                    ObIArray<ObAddr> &servers) const
{
  int ret = OB_SUCCESS;
  ObArray<ObUnit *> *units = NULL;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(inited_), K(loaded_), K(ret));
  } else if (OB_INVALID_ID == resource_pool_id) {
    // don't need to check zone, can be empty
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid resource_pool_id", K(resource_pool_id), K(ret));
  } else if (OB_FAIL(get_units_by_pool(resource_pool_id, units))) {
    LOG_WARN("get_units_by_pool failed", K(resource_pool_id), K(ret));
  } else if (NULL == units) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("units is null", KP(units), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < units->count(); ++i) {
      if (NULL == units->at(i)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit is null", "unit", OB_P(units->at(i)), K(ret));
      } else if (!zone.is_empty() && zone != units->at(i)->zone_) {
        continue;
      } else {
        if (OB_SUCCESS == ret && units->at(i)->migrate_from_server_.is_valid()) {
          if (OB_FAIL(servers.push_back(units->at(i)->migrate_from_server_))) {
            LOG_WARN("push_back failed", K(ret));
          }
        }

        if (OB_SUCCESS == ret && units->at(i)->server_.is_valid()) {
          if (OB_FAIL(servers.push_back(units->at(i)->server_))) {
            LOG_WARN("push_back failed", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

// choose server for target one unit
//
// @param [in] config                         target unit resource
// @param [in] zone                           target zone
// @param [in] excluded_servers               excluded servers which can not allocate unit
// @param [in] module                         module name for log print
// @param [out] server                        choosed server for unit
// @param [out] resource_not_enough_reason    reason for resource not enough,
//                                            is valid when ret = OB_ZONE_RESOURCE_NOT_ENOUGH
//
// @ret OB_SUCCESS                    on success
// @ret OB_ZONE_RESOURCE_NOT_ENOUGH   zone resource not enough to hold new unit
// @ret OB_ZONE_SERVER_NOT_ENOUGH     all valid servers are excluded, no server to hold new unit
int ObUnitManager::choose_server_for_unit(
    const ObUnitResource &config,
    const ObZone &zone,
    const ObArray<ObAddr> &excluded_servers,
    const char *module,
    const ObIArray<ObServerInfoInTable> &active_servers_info, // active_servers_info of the give zone,
    const ObIArray<obrpc::ObGetServerResourceInfoResult> &active_servers_resource_info, // active_servers_resource_info of the give zone
    ObAddr &choosed_server,
    std::string &resource_not_enough_reason) const
{
  int ret = OB_SUCCESS;
  ObArray<ObServerStatus> statuses;
  ObArray<ObUnitPlacementStrategy::ObServerResource> server_resources;
  ObArray<ObServerInfoInTable> servers_info;
  ObArray<obrpc::ObGetServerResourceInfoResult> report_servers_resource_info;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(inited_), K(loaded_), K(ret));
  } else if (!config.is_valid() || zone.is_empty()) {
    // excluded_servers can be empty
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid config", KR(ret), K(config), K(zone));
  } else if (OB_FAIL(build_server_resources_(active_servers_resource_info, server_resources))) {
    LOG_WARN("fail to build server resources", KR(ret), K(active_servers_resource_info));
  } else if (OB_FAIL(do_choose_server_for_unit_(config, zone, excluded_servers, active_servers_info,
      server_resources, module, choosed_server, resource_not_enough_reason))) {
    LOG_WARN("fail to choose server for unit", K(module), KR(ret), K(config), K(zone),
        K(excluded_servers), K(servers_info), K(server_resources),
        "resource_not_enough_reason", resource_not_enough_reason.c_str());
  }
  return ret;
}


int ObUnitManager::do_choose_server_for_unit_(const ObUnitResource &config,
                                              const ObZone &zone,
                                              const ObArray<ObAddr> &excluded_servers,
                                              const ObIArray<share::ObServerInfoInTable> &servers_info,
                                              const ObIArray<ObUnitPlacementStrategy::ObServerResource> &server_resources,
                                              const char *module,
                                              ObAddr &choosed_server,
                                              std::string &resource_not_enough_reason) const
{
  LOG_INFO("[CHOOSE_SERVER_FOR_UNIT] begin", K(module), K(zone), K(excluded_servers), K(config));

  int ret = OB_SUCCESS;
  double hard_limit = 1.0;
  ObArray<ObUnitPlacementStrategy::ObServerResource> valid_server_resources;

  choosed_server.reset();

  if (OB_UNLIKELY(zone.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("zone is empty, unexpected", KR(ret), K(zone));
  } else if (servers_info.count() != server_resources.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid servers_info and server_resources array", KR(ret), K(servers_info), K(server_resources));
  } else if (OB_FAIL(get_hard_limit(hard_limit))) {
    LOG_WARN("get_hard_limit failed", K(ret));
  } else {
    int64_t not_excluded_server_count = 0;
    // 1. construct valid servers resource
    for (int64_t i = 0; OB_SUCC(ret) && i < servers_info.count(); ++i) { // for each active servers
      ObResourceType not_enough_resource = RES_MAX;
      AlterResourceErr not_enough_resource_config = ALT_ERR;
      const ObServerInfoInTable &server_info = servers_info.at(i);
      const ObAddr &server = server_info.get_server();
      const ObUnitPlacementStrategy::ObServerResource &server_resource = server_resources.at(i);
      char server_str[OB_IP_PORT_STR_BUFF] = {'\0'};
      int64_t pos = 0;

      if (has_exist_in_array(excluded_servers, server)) {
        // excluded servers are expected, need not show in reason
        continue;
      } else if (OB_FAIL(databuff_printf(server_str, sizeof(server_str), pos, server))) {
        LOG_WARN("fail to print server to databuff", K(ret), K(server), K(pos));
      } else {
        not_excluded_server_count++;

        if (!server_info.can_migrate_in()) {
          if (!server_info.is_active()) {
            resource_not_enough_reason =
                resource_not_enough_reason + "server '" + server_str + "' is not active\n";
          } else {
            // server is block-migrate-in
            resource_not_enough_reason =
                resource_not_enough_reason + "server '" + server_str + "' is blocked migrate-in\n";
          }
          LOG_WARN("[CHOOSE_SERVER_FOR_UNIT] server can not migrate in", K(module), K(i), "server", server_info);
          continue;
        } else {
          bool is_resource_enough =
              check_resource_enough_for_unit_(server_resource, config, hard_limit,
                  not_enough_resource, not_enough_resource_config);

          if (is_resource_enough) {
            LOG_INFO("[CHOOSE_SERVER_FOR_UNIT] find available server", K(module), K(i),
                K(server_resource), "request_unit_config", config);

            if (OB_FAIL(valid_server_resources.push_back(server_resource))) {
              LOG_WARN("failed to push into array", KR(ret), K(server_resource));
            }
          } else {
            LOG_INFO("[CHOOSE_SERVER_FOR_UNIT] server resource not enough", K(module), K(i),
                "not_enough_resource", resource_type_to_str(not_enough_resource),
                "not_enough_resource_config", alter_resource_err_to_str(not_enough_resource_config),
                K(server_resource), "request_unit_config", config);
            resource_not_enough_reason =
                resource_not_enough_reason + "server '" + server_str + "' "
                + resource_type_to_str(not_enough_resource) + " resource not enough\n";
          }
        }
      }
    }  // end for

    if (OB_SUCCESS == ret) {
      if (0 == not_excluded_server_count) {
        ret = OB_ZONE_SERVER_NOT_ENOUGH;
        LOG_WARN("zone server not enough to hold all units", K(module), KR(ret), K(zone), K(excluded_servers),
            K(servers_info));
      } else if (valid_server_resources.count() <= 0) {
        ret = OB_ZONE_RESOURCE_NOT_ENOUGH;
        LOG_WARN("zone resource is not enough to hold a new unit", K(module), KR(ret), K(zone),
            K(config), K(excluded_servers),
            "resource_not_enough_reason", resource_not_enough_reason.c_str());
      } else {
        // 2. choose the server
        ObUnitPlacementDPStrategy unit_placement;
        if (OB_FAIL(unit_placement.choose_server(valid_server_resources, config, module, choosed_server))) {
          LOG_WARN("failed to choose server for unit", K(ret), K(config));
        }
      }
    }
  }
  LOG_INFO("[CHOOSE_SERVER_FOR_UNIT] end", K(module), KR(ret), K(choosed_server), K(zone), K(excluded_servers),
      K(config), K(valid_server_resources),
      "resource_not_enough_reason", resource_not_enough_reason.c_str());
  return ret;
}

int ObUnitManager::compute_server_resource_(
    const obrpc::ObGetServerResourceInfoResult &report_server_resource_info,
    ObUnitPlacementStrategy::ObServerResource &server_resource) const
{
  int ret = OB_SUCCESS;
  ObUnitConfig sum_load;
  ObArray<ObUnitLoad> *unit_loads = NULL;
  const ObAddr &server = report_server_resource_info.get_server();
  const ObServerResourceInfo &report_resource = report_server_resource_info.get_resource_info();
  if (OB_UNLIKELY(!report_server_resource_info.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", KR(ret), K(report_server_resource_info));
  } else if (OB_FAIL(get_loads_by_server(server, unit_loads))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("get_loads_by_server failed", "server", server, KR(ret));
    } else {
      ret = OB_SUCCESS;
    }
  } else if (OB_ISNULL(unit_loads)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unit_loads is null", KR(ret), KP(unit_loads));
  } else if (OB_FAIL(calc_sum_load(unit_loads, sum_load))) {
    LOG_WARN("calc_sum_load failed", KR(ret), KP(unit_loads));
  }

  if (OB_SUCC(ret)) {
    // Unit resource information is persisted on the observer side,
    // The unit_load seen on the rs side is only the resource view managed by rs itself,
    // It is actually inaccurate to judge whether the resources are sufficient based on the resource view of rs itself,
    // Need to consider the persistent resources of the unit resource information on the observer side.
    // The persistent information of the unit on the observer side is regularly reported to rs by the observer through the heartbeat.
    // When performing allocation, rs reports the maximum value of resource information from its own resource view
    // and observer side as a reference for unit resource allocation
    server_resource.addr_ = server;
    server_resource.assigned_[RES_CPU] = sum_load.min_cpu() > report_resource.report_cpu_assigned_
                                         ? sum_load.min_cpu() : report_resource.report_cpu_assigned_;
    server_resource.max_assigned_[RES_CPU] = sum_load.max_cpu() > report_resource.report_cpu_max_assigned_
                                         ? sum_load.max_cpu() : report_resource.report_cpu_max_assigned_;
    server_resource.capacity_[RES_CPU] = report_resource.cpu_;
    server_resource.assigned_[RES_MEM] = sum_load.memory_size() > report_resource.report_mem_assigned_
                                         ? static_cast<double>(sum_load.memory_size())
                                         : static_cast<double>(report_resource.report_mem_assigned_);
    server_resource.max_assigned_[RES_MEM] = server_resource.assigned_[RES_MEM];
    server_resource.capacity_[RES_MEM] = static_cast<double>(report_resource.mem_total_);
    server_resource.assigned_[RES_LOG_DISK] = static_cast<double>(sum_load.log_disk_size());
    server_resource.max_assigned_[RES_LOG_DISK] = static_cast<double>(sum_load.log_disk_size());
    server_resource.capacity_[RES_LOG_DISK] = static_cast<double>(report_resource.log_disk_total_);
  }

  LOG_INFO("compute server resource", KR(ret),
            "server", server,
            K(server_resource),
            "report_resource_info", report_resource,
            "valid_unit_sum", sum_load,
            "valid_unit_count", unit_loads != NULL ? unit_loads->count(): 0);
  return ret;
}

// check resource enough for unit
//
// @param [in] u                              demands resource that may have some invalid items, need not check valid
// @param [out] not_enough_resource           returned resource type that is not enough
// @param [out] not_enough_resource_config    returned resource config type that is not enough
bool ObUnitManager::check_resource_enough_for_unit_(
    const ObUnitPlacementStrategy::ObServerResource &r,
    const ObUnitResource &u,
    const double hard_limit,
    ObResourceType &not_enough_resource,
    AlterResourceErr &not_enough_resource_config) const
{
  bool is_enough = false; // default is false

  if (u.is_max_cpu_valid() &&
      r.capacity_[RES_CPU] * hard_limit < r.max_assigned_[RES_CPU] + u.max_cpu()) {
    not_enough_resource = RES_CPU;
    not_enough_resource_config = MAX_CPU;
  } else if (u.is_min_cpu_valid() &&
             r.capacity_[RES_CPU] < r.assigned_[RES_CPU] + u.min_cpu()) {
    not_enough_resource = RES_CPU;
    not_enough_resource_config = MIN_CPU;
  } else if (u.is_memory_size_valid() &&
             r.capacity_[RES_MEM] < r.assigned_[RES_MEM] + u.memory_size()) {
    not_enough_resource = RES_MEM;
    not_enough_resource_config = MEMORY;
  } else if (u.is_log_disk_size_valid() &&
             r.capacity_[RES_LOG_DISK] < r.assigned_[RES_LOG_DISK] + u.log_disk_size()) {
    not_enough_resource = RES_LOG_DISK;
    not_enough_resource_config = LOG_DISK;
  } else {
    is_enough = true;
    not_enough_resource = RES_MAX;
    not_enough_resource_config = ALT_ERR;
  }

  if (! is_enough) {
    ObCStringHelper helper;
    _LOG_INFO("server %s resource '%s' is not enough for unit. hard_limit=%.6g, server_resource=%s, "
        "demands=%s",
        resource_type_to_str(not_enough_resource),
        alter_resource_err_to_str(not_enough_resource_config),
        hard_limit,
        helper.convert(r),
        helper.convert(u));
  }
  return is_enough;
}


// demand_resource may have some invalid items, need not check valid for demand_resource
int ObUnitManager::have_enough_resource(const obrpc::ObGetServerResourceInfoResult &report_server_resource_info,
                                        const ObUnitResource &demand_resource,
                                        const double hard_limit,
                                        bool &is_enough,
                                        AlterResourceErr &err_index) const
{
  int ret = OB_SUCCESS;
  ObResourceType not_enough_resource = RES_MAX;
  ObUnitPlacementStrategy::ObServerResource server_resource;
  err_index = ALT_ERR;

  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(inited_), K(loaded_), K(ret));
  } else if (!report_server_resource_info.is_valid() || hard_limit <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(report_server_resource_info), K(hard_limit), K(ret));
  } else if (OB_FAIL(compute_server_resource_(report_server_resource_info, server_resource))) {
    LOG_WARN("compute server resource fail", KR(ret), K(report_server_resource_info));
  } else {
    is_enough = check_resource_enough_for_unit_(server_resource, demand_resource, hard_limit,
        not_enough_resource, err_index);
  }
  return ret;
}
int ObUnitManager::check_enough_resource_for_delete_server(
    const ObAddr &server,
    const ObZone &zone)
{
  int ret = OB_SUCCESS;
  // get_servers_of_zone
  ObArray<obrpc::ObGetServerResourceInfoResult> report_servers_resource_info;
  ObArray<ObServerInfoInTable> servers_info_of_zone;
  bool empty = false;
  const bool ONLY_ACTIVE_SERVERS = true;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.sql_proxy_ is null", KR(ret), KP(GCTX.sql_proxy_));
  } else if (OB_UNLIKELY(!server.is_valid() || zone.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server or zone", KR(ret), K(server), K(zone));
  } else if (OB_FAIL(ut_operator_.check_server_empty(server, empty))) {
    // the validity of the server is checked here
    LOG_WARN("fail to check whether the server is empty", KR(ret));
  } else if (empty) {
    //nothing todo
  } else if (OB_FAIL(ObServerTableOperator::get_servers_info_of_zone(
      *GCTX.sql_proxy_,
      zone,
      ONLY_ACTIVE_SERVERS,
      servers_info_of_zone))) {
    LOG_WARN("fail to get servers info of zone", KR(ret), K(zone));
  } else if (OB_FAIL(get_servers_resource_info_via_rpc(servers_info_of_zone, report_servers_resource_info))) {
    LOG_WARN("fail to get servers_resouce_info via rpc", KR(ret), K(servers_info_of_zone), K(report_servers_resource_info));
  } else if (OB_FAIL(check_enough_resource_for_delete_server_(
      server,
      zone,
      servers_info_of_zone,
      report_servers_resource_info))) {
    LOG_WARN("fail to check enough resource for delete server", KR(ret), K(server), K(zone),
        K(servers_info_of_zone), K(report_servers_resource_info));
  } else {}
  return ret;
}
int ObUnitManager::get_servers_resource_info_via_rpc(
    const ObIArray<share::ObServerInfoInTable> &servers_info,
    ObIArray<obrpc::ObGetServerResourceInfoResult> &report_servers_resource_info) const
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTimeoutCtx ctx;
  obrpc::ObGetServerResourceInfoArg arg;
  ObArray<obrpc::ObGetServerResourceInfoResult> tmp_report_servers_resource_info;
  report_servers_resource_info.reset();
  if (OB_UNLIKELY(servers_info.count() < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("servers_info.count() should be >= 0", KR(ret), K(servers_info.count()));
  } else if (0 == servers_info.count()) {
    // do nothing
  } else if (!ObHeartbeatService::is_service_enabled()) { // old logic
    ObServerResourceInfo resource_info;
    obrpc::ObGetServerResourceInfoResult result;
    for (int64_t i = 0; OB_SUCC(ret) && i < servers_info.count(); i++) {
      const ObAddr &server = servers_info.at(i).get_server();
      resource_info.reset();
      result.reset();
      if (OB_FAIL(server_mgr_.get_server_resource_info(server, resource_info))) {
        LOG_WARN("fail to get server resource info", KR(ret), K(server));
      } else if (OB_FAIL(result.init(server, resource_info))) {
        LOG_WARN("fail to init", KR(ret), K(server));
      } else if (OB_FAIL(report_servers_resource_info.push_back(result))) {
        LOG_WARN("fail to push an element into report_servers_resource_info", KR(ret), K(result));
      }
    }
  } else { // new logic
    if (OB_ISNULL(srv_rpc_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("srv_rpc_proxy_ is null", KR(ret), KP(srv_rpc_proxy_));
    } else if (OB_FAIL(ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
      LOG_WARN("fail to get timeout ctx", KR(ret), K(ctx));
    } else {
      ObGetServerResourceInfoProxy proxy(*srv_rpc_proxy_, &obrpc::ObSrvRpcProxy::get_server_resource_info);
      for (int64_t i = 0; OB_SUCC(ret) && i < servers_info.count(); i++) {
        const ObServerInfoInTable & server_info = servers_info.at(i);
        arg.reset();
        if (OB_UNLIKELY(!server_info.is_valid())) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid server_info", KR(ret), K(server_info));
        } else {
          const ObAddr &server = server_info.get_server();
          const int64_t time_out = ctx.get_timeout();
          if (OB_FAIL(arg.init(GCTX.self_addr()))) {
            LOG_WARN("fail to init arg", KR(ret), K(GCTX.self_addr()));
          } else if (OB_FAIL(proxy.call(
              server,
              time_out,
              GCONF.cluster_id,
              OB_SYS_TENANT_ID,
              arg))) {
            LOG_WARN("fail to send get_server_resource_info rpc",  KR(ret), KR(tmp_ret), K(server),
                K(time_out), K(arg));
          }
        }
      }
      if (OB_TMP_FAIL(proxy.wait())) {
        LOG_WARN("fail to wait all batch result", KR(ret), KR(tmp_ret));
        ret = OB_SUCC(ret) ? tmp_ret : ret;
      } else if (OB_SUCC(ret)) {
        ARRAY_FOREACH_X(proxy.get_results(), idx, cnt, OB_SUCC(ret)) {
          const obrpc::ObGetServerResourceInfoResult *rpc_result = proxy.get_results().at(idx);
          if (OB_ISNULL(rpc_result)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("rpc_result is null", KR(ret), KP(rpc_result));
          } else if (OB_UNLIKELY(!rpc_result->is_valid())) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("rpc_result is invalid", KR(ret), KPC(rpc_result));
          } else if (OB_FAIL(tmp_report_servers_resource_info.push_back(*rpc_result))) {
            LOG_WARN("fail to push an element into tmp_report_servers_resource_info", KR(ret), KPC(rpc_result));
          }
        }
      }
    }
    // get ordered report_servers_resource_info: since when processing resource_info,
    // we assume servers_info.at(i).get_server() = report_servers_resource_info.at(i).get_server()
    if (FAILEDx(order_report_servers_resource_info_(
        servers_info,
        tmp_report_servers_resource_info,
        report_servers_resource_info ))) {
      LOG_WARN("fail to order report_servers_resource_info", KR(ret),
          K(servers_info.count()), K(tmp_report_servers_resource_info.count()),
          K(servers_info), K(tmp_report_servers_resource_info));
    }
  }
  return ret;
}

int ObUnitManager::order_report_servers_resource_info_(
    const ObIArray<share::ObServerInfoInTable> &servers_info,
    const ObIArray<obrpc::ObGetServerResourceInfoResult> &report_servers_resource_info,
    ObIArray<obrpc::ObGetServerResourceInfoResult> &ordered_report_servers_resource_info)
{
  // target: servers_info.at(i).get_server() = ordered_report_servers_resource_info.at(i).get_server()
  int ret = OB_SUCCESS;
  ordered_report_servers_resource_info.reset();
  if (servers_info.count() != report_servers_resource_info.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("the size of servers_info should be equal to the size of report_servers_resource_info",
        KR(ret), K(servers_info.count()), K(report_servers_resource_info.count()),
        K(servers_info), K(report_servers_resource_info));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < servers_info.count(); i++) {
      bool find_server = false;
      for (int64_t j = 0; OB_SUCC(ret) && !find_server && j < report_servers_resource_info.count(); j++) {
        const obrpc::ObGetServerResourceInfoResult &server_resource_info = report_servers_resource_info.at(j);
        if (servers_info.at(i).get_server() == server_resource_info.get_server()) {
          find_server = true;
          if (OB_FAIL(ordered_report_servers_resource_info.push_back(server_resource_info))) {
            LOG_WARN("fail to push an element into ordered_report_servers_resource_info",
                KR(ret), K(server_resource_info));
          }
        }
      }
      if(OB_SUCC(ret) && !find_server) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("server not exists in report_servers_resource_info",
            K(servers_info.at(i)), K(report_servers_resource_info));
      }
    }
  }
  return ret;
}
int ObUnitManager::get_server_resource_info_via_rpc(
    const share::ObServerInfoInTable &server_info,
    obrpc::ObGetServerResourceInfoResult &report_server_resource_info) const
{
  int ret = OB_SUCCESS;
  ObArray<share::ObServerInfoInTable> servers_info;
  ObArray<obrpc::ObGetServerResourceInfoResult> report_resource_info_array;
  report_server_resource_info.reset();
  if (OB_UNLIKELY(!server_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("server_info is invalid", KR(ret), K(server_info));
  } else if (OB_FAIL(servers_info.push_back(server_info))) {
    LOG_WARN("fail to push an element into servers_info", KR(ret), K(server_info));
  } else if (OB_FAIL(get_servers_resource_info_via_rpc(servers_info, report_resource_info_array))) {
    LOG_WARN("fail to execute get_servers_resource_info_via_rpc", KR(ret), K(servers_info));
  } else if (OB_UNLIKELY(1 != report_resource_info_array.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("report_resource_info_array.count() should be one", KR(ret), K(report_resource_info_array.count()));
  } else if (OB_FAIL(report_server_resource_info.assign(report_resource_info_array.at(0)))) {
    LOG_WARN("fail to assign report_server_resource_info", KR(ret), K(report_resource_info_array.at(0)));
  }
  return ret;
}
int ObUnitManager::check_enough_resource_for_delete_server_(
    const ObAddr &server,
    const ObZone &zone,
    const ObIArray<share::ObServerInfoInTable> &servers_info,
    const ObIArray<obrpc::ObGetServerResourceInfoResult> &report_servers_resource_info)
{
  int ret = OB_SUCCESS;
  ObArray<ObUnitManager::ObUnitLoad> *unit_loads = NULL;
  ObArray<ObUnitPlacementStrategy::ObServerResource> initial_servers_resources;
  SpinRLockGuard guard(lock_);
  bool empty = false;
  if (OB_UNLIKELY(zone.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("zone is invalid", KR(ret), K(zone), K(server));
  } else if (OB_FAIL(ut_operator_.check_server_empty(server, empty))) {
    LOG_WARN("fail to check server empty", K(ret));
  } else if (empty) {
    //nothing todo
  } else {
    if (OB_FAIL(get_loads_by_server(server, unit_loads))) {
      LOG_WARN("fail to get loads by server", K(ret));
    } else if (OB_FAIL(build_server_resources_(report_servers_resource_info, initial_servers_resources))) {
      LOG_WARN("fail to build server resources", KR(ret), K(report_servers_resource_info));
    } else {
      for (int64_t i = 0; i < unit_loads->count() && OB_SUCC(ret); ++i) {
        std::string resource_not_enough_reason;
        if (OB_FAIL(check_server_have_enough_resource_for_delete_server_(
            unit_loads->at(i),
            zone,
            servers_info,
            initial_servers_resources,
            resource_not_enough_reason))) {
          LOG_WARN("fail to check server have enough resource for delete server",
              K(ret),
              K(zone),
              K(initial_servers_resources),
              K(i),
              K(unit_loads),
              K(resource_not_enough_reason.c_str()));

          // handle return error info
          char zone_str[MAX_ZONE_LENGTH] = {'\0'};
          int64_t zone_pos = 0;
          (void)databuff_printf(zone_str, sizeof(zone_str), zone_pos, zone);
          if (OB_ZONE_SERVER_NOT_ENOUGH == ret) {
            std::string err_msg;
            const ObUnit *unit = unit_loads->at(i).unit_;
            uint64_t unit_id = (NULL == unit ? 0 : unit->unit_id_);
            char unit_id_str[32] = {'\0'};
            int64_t unit_id_pos = 0;
            (void) databuff_printf(unit_id_str, sizeof(unit_id_str), unit_id_pos, "%lu", unit_id);

            err_msg = err_msg + "can not migrate out unit '" + unit_id_str +
                "', no other available servers on zone '" +  zone_str +
                "', delete server not allowed";
            LOG_USER_ERROR(OB_DELETE_SERVER_NOT_ALLOWED, err_msg.c_str());
          } else if (OB_ZONE_RESOURCE_NOT_ENOUGH == ret) {
            std::string err_msg;
            const ObUnit *unit = unit_loads->at(i).unit_;
            uint64_t unit_id = (NULL == unit ? 0 : unit->unit_id_);

            err_msg = err_msg + "can not migrate out all units, zone '" + zone_str +
                "' resource not enough, delete server not allowed. "
                "You can check resource info by views: DBA_OB_UNITS, GV$OB_UNITS, GV$OB_SERVERS.\n"
                + resource_not_enough_reason.c_str();
            LOG_USER_ERROR(OB_DELETE_SERVER_NOT_ALLOWED, err_msg.c_str());
          }
        }
      } //end for unit_loads
    }
  }
  return ret;
}
int ObUnitManager::check_server_have_enough_resource_for_delete_server_(
    const ObUnitLoad &unit_load,
    const common::ObZone &zone,
    const ObIArray<share::ObServerInfoInTable> &servers_info,
    ObIArray<ObUnitPlacementStrategy::ObServerResource> &initial_servers_resources,
    std::string &resource_not_enough_reason)
{
  int ret = OB_SUCCESS;
  double hard_limit = 1.0;
  ObArray<ObUnitPlacementStrategy::ObServerResource> servers_resources;
  ObArray<ObAddr> excluded_servers;
  ObAddr choosed_server;
  const ObUnitConfig *config = unit_load.unit_config_;
  const char *module = "DELETE_SERVER";
  const bool new_allocate_pool = false;

  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(inited_), K(loaded_), K(ret));
  } else if (OB_ISNULL(config) || OB_UNLIKELY(! config->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unit config ptr is null", KR(ret), KPC(config));
  } else if (OB_UNLIKELY(zone.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("zone is empty, unexpected", KR(ret), K(zone));
  } else if (servers_info.count() != initial_servers_resources.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("servers_info does not match initial_servers_resources", KR(ret), K(servers_info),
        K(initial_servers_resources));
  } else if (OB_ISNULL(unit_load.unit_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unit ptr is null", K(ret));
  } else if (unit_load.unit_->migrate_from_server_.is_valid()) {
    // In a state of migration, there must be a place to migrate
  } else if (OB_FAIL(get_excluded_servers(unit_load.unit_->resource_pool_id_, unit_load.unit_->zone_, module,
          new_allocate_pool, excluded_servers))) {
    LOG_WARN("fail to get excluded server", K(module), KR(ret), KPC(unit_load.unit_), K(new_allocate_pool));
  } else if (OB_FAIL(do_choose_server_for_unit_(config->unit_resource(), zone,
      excluded_servers, servers_info, initial_servers_resources,
      module, choosed_server, resource_not_enough_reason))) {
    // choose right server for target unit
    LOG_WARN("choose server for unit fail", K(module), KR(ret), K(zone), KPC(config),
        K(excluded_servers), K(servers_info), K(initial_servers_resources),
        "resource_not_enough_reason", resource_not_enough_reason.c_str());
  } else {
    // sum target unit resource config on choosed server resource
    int64_t choosed_index = -1;
    for (int64_t i = 0; choosed_index < 0 && i < initial_servers_resources.count(); i++) {
      if (initial_servers_resources.at(i).addr_ == choosed_server) {
        choosed_index = i;
      }
    }

    if (OB_UNLIKELY(choosed_index < 0 || choosed_index >= initial_servers_resources.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("can not find choosed_server in initial_servers_resources, or invalid choosed_index", KR(ret),
          K(choosed_index), K(choosed_server), K(initial_servers_resources));
    } else if (OB_FAIL(sum_servers_resources(initial_servers_resources.at(choosed_index), *config))) {
      LOG_WARN("fail to sum servers resources", KR(ret), K(initial_servers_resources),
          KPC(config));
    }
  }
  return ret;
}

int ObUnitManager::build_server_resources_(
    const ObIArray<obrpc::ObGetServerResourceInfoResult> &report_servers_resource_info,
    ObIArray<ObUnitPlacementStrategy::ObServerResource> &servers_resources) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < report_servers_resource_info.count(); ++i) {
    const obrpc::ObGetServerResourceInfoResult &report_resource_info = report_servers_resource_info.at(i);
    ObUnitPlacementStrategy::ObServerResource server_resource;

    if (OB_FAIL(compute_server_resource_(report_resource_info, server_resource))) {
      LOG_WARN("compute server resource fail", KR(ret), K(report_resource_info));
    } else if (OB_FAIL(servers_resources.push_back(server_resource))) {
      LOG_WARN("fail to push back", KR(ret), K(server_resource));
    }
  }
  return ret;
}

int ObUnitManager::sum_servers_resources(ObUnitPlacementStrategy::ObServerResource &server_resource,
                                         const share::ObUnitConfig &unit_config)
{
  int ret = OB_SUCCESS;
  server_resource.assigned_[RES_CPU] = server_resource.assigned_[RES_CPU] +
                                       unit_config.min_cpu();
  server_resource.max_assigned_[RES_CPU] = server_resource.max_assigned_[RES_CPU] +
                                           unit_config.max_cpu();
  server_resource.assigned_[RES_MEM] = server_resource.assigned_[RES_MEM] +
                                       static_cast<double>(unit_config.memory_size());
  server_resource.max_assigned_[RES_MEM] = server_resource.max_assigned_[RES_MEM] +
                                           static_cast<double>(unit_config.memory_size());
  server_resource.assigned_[RES_LOG_DISK] = server_resource.assigned_[RES_LOG_DISK] +
                                        static_cast<double>(unit_config.log_disk_size());
  server_resource.max_assigned_[RES_LOG_DISK] = server_resource.max_assigned_[RES_LOG_DISK] +
                                            static_cast<double>(unit_config.log_disk_size());
  return ret;
}

int ObUnitManager::add_unit(ObISQLClient &client, const ObUnit &unit)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(inited_), K(loaded_), K(ret));
  } else if (!unit.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(unit), K(ret));
  } else {
    ObUnit *new_unit = NULL;
    if (OB_SUCCESS == (ret = get_unit_by_id(unit.unit_id_, new_unit))) {
      ret = OB_ENTRY_EXIST;
      LOG_WARN("unit already exist, can't add", K(unit), K(ret));
    } else if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("get_unit_by_id failed", "unit_id", unit.unit_id_, K(ret));
    } else {
      ret = OB_SUCCESS;
    }

    if (OB_FAIL(ret)) {
    } else if (NULL == (new_unit = allocator_.alloc())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc unit failed", K(ret));
    } else {
      if (OB_FAIL(ut_operator_.insert_unit(client, unit))) {
        LOG_WARN("insert unit failed", K(unit), KR(ret));
      } else {
        *new_unit = unit;
        if (OB_FAIL(insert_unit(new_unit))) {
          LOG_WARN("insert_unit failed", "new unit", *new_unit, K(ret));
        } else {
          ROOTSERVICE_EVENT_ADD("unit", "create_unit",
              "unit_id", unit.unit_id_,
              "server", unit.server_);
        }
      }
      if (OB_FAIL(ret)) {
        //avoid memory leak
        allocator_.free(new_unit);
        new_unit = NULL;
      }
    }
  }
  return ret;
}

int ObUnitManager::alter_pool_unit_config(share::ObResourcePool  *pool,
                                          const ObUnitConfigName &config_name)
{
  int ret = OB_SUCCESS;
  ObUnitConfig *config = NULL;
  ObUnitConfig *alter_config = NULL;
  common::ObSEArray<share::ObResourcePool *, 1> pools;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(inited_), K(loaded_), K(ret));
  } else if (NULL == pool) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("pool is null", K(ret));
  } else if (config_name.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("config_name is empty", K(config_name), K(ret));
  } else if (OB_FAIL(get_unit_config_by_id(pool->unit_config_id_, config))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("get_unit_config_by_id failed", "unit_config_id", pool->unit_config_id_, K(ret));
    } else {
      ret = OB_RESOURCE_UNIT_NOT_EXIST;
      LOG_WARN("unit config not exist", "config_id", pool->unit_config_id_, K(ret));
    }
  } else if (NULL == config) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("config is null", KP(config), K(ret));
  } else if (OB_FAIL(get_unit_config_by_name(config_name, alter_config))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("get_unit_config_by_name failed", K(config_name), K(ret));
    } else {
      ret = OB_RESOURCE_UNIT_NOT_EXIST;
      ObCStringHelper helper;
      LOG_USER_ERROR(OB_RESOURCE_UNIT_NOT_EXIST, helper.convert(config_name));
      LOG_WARN("unit config not exist", K(config_name), K(ret));
    }
  } else if (NULL == alter_config) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("alter_config is null", KP(alter_config), K(ret));
  } else if (config_name == config->name()) {
    // do nothing
  } else if (REPLICA_TYPE_FULL == pool->replica_type_
      && alter_config->unit_resource().memory_size() < GCONF.__min_full_resource_pool_memory) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("full resource pool memory size illegal", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "unit MEMORY_SIZE less than __min_full_resource_pool_memory");
  } else if (OB_FAIL(pools.push_back(pool))) {
    LOG_WARN("push back pool into array fail", KR(ret), K(pool), K(pools));
  } else if (OB_FAIL(check_expand_resource_(
      "ALTER_RESOURCE_POOL_UNIT_CONFIG",
      pools,
      config->unit_resource(),
      alter_config->unit_resource()))) {
    LOG_WARN("check_expand_config failed", KR(ret), KPC(pool), KPC(config), KPC(alter_config));
  } else if (OB_FAIL(check_shrink_resource_(pools, config->unit_resource(),
      alter_config->unit_resource()))) {
    LOG_WARN("check_shrink_resource_ failed", KPC(pool), KPC(config), KPC(alter_config), KR(ret));
  } else if (OB_FAIL(change_pool_config(pool, config, alter_config))) {
    LOG_WARN("change_pool_config failed", "pool", *pool, "config", *config,
        "alter_config", *alter_config, K(ret));
  }
  return ret;
}

int ObUnitManager::build_sorted_zone_unit_ptr_array(
    const share::ObResourcePool *pool,
    common::ObIArray<ZoneUnitPtr> &zone_unit_ptrs) const
{
  int ret = OB_SUCCESS;
  common::ObArray<share::ObUnit *> *units = NULL;
  zone_unit_ptrs.reset();
  if (OB_UNLIKELY(NULL == pool)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(pool));
  } else if (OB_FAIL(get_units_by_pool(pool->resource_pool_id_, units))) {
    LOG_WARN("fail to get units by pool", K(ret), "pool_id", pool->resource_pool_id_);
  } else if (OB_UNLIKELY(NULL == units)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("units ptr is null", K(ret), KP(units));
  } else {
    // traverse all unit in units
    for (int64_t i = 0; OB_SUCC(ret) && i < units->count(); ++i) {
      ObUnit *this_unit = units->at(i);
      if (OB_UNLIKELY(NULL == this_unit)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit ptr is null", K(ret), KP(this_unit));
      } else {
        // aggregate units with the same zone
        int64_t index = 0;
        for (; OB_SUCC(ret) && index < zone_unit_ptrs.count(); ++index) {
          if (this_unit->zone_ == zone_unit_ptrs.at(index).zone_) {
            break;
          }
        }
        if (OB_FAIL(ret)) {
        } else if (index >= zone_unit_ptrs.count()) {
          ZoneUnitPtr zone_unit_ptr;
          zone_unit_ptr.zone_ = this_unit->zone_;
          if (OB_FAIL(zone_unit_ptrs.push_back(zone_unit_ptr))) {
            LOG_WARN("fail to push back", K(ret));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (index >= zone_unit_ptrs.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected index", K(ret), K(index),
                   "zone_unit_count", zone_unit_ptrs.count());
        } else if (this_unit->zone_ != zone_unit_ptrs.at(index).zone_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("zone not match", K(ret), "left_zone", this_unit->zone_,
                   "right_zone", zone_unit_ptrs.at(index).zone_);
        } else if (OB_FAIL(zone_unit_ptrs.at(index).unit_ptrs_.push_back(this_unit))) {
          LOG_WARN("fail to push back", K(ret), K(index));
        } else {} // good, no more to do
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < zone_unit_ptrs.count(); ++i) {
      // sort each zone unit ptr using group id
      ZoneUnitPtr &zone_unit_ptr = zone_unit_ptrs.at(i);
      if (OB_FAIL(zone_unit_ptr.sort_by_unit_id())) {
        LOG_WARN("fail to sort unit", K(ret));
      } else {} // good, unit num match
    }
  }
  return ret;
}

int ObUnitManager::check_shrink_unit_num_zone_condition(
    share::ObResourcePool *pool,
    const int64_t new_unit_num,
    const common::ObIArray<uint64_t> &delete_unit_id_array)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> gts_unit_ids;
  if (OB_UNLIKELY(NULL == pool || new_unit_num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(pool), K(new_unit_num));
  } else if (delete_unit_id_array.count() <= 0) {
    // good, we choose deleting set all by ourselves
  } else if (pool->unit_count_ <= new_unit_num) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should be a shrink pool operation", K(ret),
             "cur_unit_num", pool->unit_count_,
             "new_unit_num", new_unit_num);
  } else if (pool->is_granted_to_tenant()
      && OB_FAIL(get_tenant_gts_unit_ids(pool->tenant_id_, gts_unit_ids))) {
    LOG_WARN("fail to get tenant gts unit ids", KR(ret), KPC(pool));
  } else {
    int64_t delta = pool->unit_count_ - new_unit_num;
    const common::ObIArray<common::ObZone> &zone_list = pool->zone_list_;
    common::ObArray<ZoneUnitPtr> delete_zone_unit_ptrs;
    for (int64_t i = 0; OB_SUCC(ret) && i < delete_unit_id_array.count(); ++i) {
      ObUnit *this_unit = NULL;
      if (has_exist_in_array(gts_unit_ids, delete_unit_id_array.at(i))) {
        ret = OB_OP_NOT_ALLOW;
        LOG_WARN("deleting gts unit when shrinking pool not allowed", KR(ret),
            "unit_id", delete_unit_id_array.at(i));
        LOG_USER_ERROR_WITH_ARGS(OB_OP_NOT_ALLOW, "Tenant %lu GTS standalone enabled, "
            "deleting unit where GTS located is", pool->tenant_id_);
      } else if (OB_FAIL(get_unit_by_id(delete_unit_id_array.at(i), this_unit))) {
        LOG_WARN("fail to get unit by id", K(ret));
      } else if (OB_UNLIKELY(NULL == this_unit)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit ptr is null", K(ret), KP(this_unit));
      } else if (this_unit->resource_pool_id_ != pool->resource_pool_id_) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("do not support shrink unit belonging to other pool");
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "shrink unit belonging to other pool");
      } else if (!has_exist_in_array(zone_list, this_unit->zone_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit zone not match", K(ret), K(zone_list), "unit_zone", this_unit->zone_);
      } else {
        int64_t index = 0;
        for (; OB_SUCC(ret) && index < delete_zone_unit_ptrs.count(); ++index) {
          if (this_unit->zone_ == delete_zone_unit_ptrs.at(index).zone_) {
            break;
          }
        }
        if (OB_FAIL(ret)) {
        } else if (index >= delete_zone_unit_ptrs.count()) {
          ZoneUnitPtr delete_zone_unit_ptr;
          delete_zone_unit_ptr.zone_ = this_unit->zone_;
          if (OB_FAIL(delete_zone_unit_ptrs.push_back(delete_zone_unit_ptr))) {
            LOG_WARN("fail to push back", K(ret));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (index >= delete_zone_unit_ptrs.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected index", K(ret), K(index),
                   "delete_zone_unit_count", delete_zone_unit_ptrs.count());
        } else if (this_unit->zone_ != delete_zone_unit_ptrs.at(index).zone_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("zone not match", K(ret), "left_zone", this_unit->zone_,
                   "right_zone", delete_zone_unit_ptrs.at(index).zone_);
        } else if (OB_FAIL(delete_zone_unit_ptrs.at(index).unit_ptrs_.push_back(this_unit))) {
          LOG_WARN("fail to push back", K(ret), K(index));
        } else {} // good, no more to do
      }
    }
    if (OB_SUCC(ret)) {
      if (delete_zone_unit_ptrs.count() != zone_list.count()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("do not support shrink unit num to different value on different zone", K(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "shrink unit num to different value on different zone");
      }
    }
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < delete_zone_unit_ptrs.count(); ++i) {
        if (delta != delete_zone_unit_ptrs.at(i).unit_ptrs_.count()) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("shrink mismatching unit num and unit id list not support", K(ret));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "shrink mismatching unit num and unit id list");
        } else {} // good, go on to check next
      }
    }
  }
  return ret;
}

// get by sys ls unit_list. Units that not applicable for gts standalone are excluded, such as L-zone.
int ObUnitManager::get_tenant_gts_unit_ids(
    const uint64_t tenant_id,
    ObIArray<uint64_t> &unit_ids)
{
  int ret = OB_SUCCESS;
  bool is_gts_standalone = false;
  unit_ids.reset();
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_tenant_gts_standalone_enabled(tenant_id, is_gts_standalone))) {
    LOG_WARN("fail to check tenant gts standalone enabled", KR(ret), K(tenant_id));
  } else if (!is_gts_standalone) {
    // gts_standalone disabled, result should be empty
  } else {
    ObArray<uint64_t> unit_ids_tmp;
    ObLSStatusOperator ls_status_op;
    ObLSStatusInfo sys_ls_info;
    ObUnitTableOperator ut_operator;
    ObArray<ObUnit> units_tmp;
    if (OB_ISNULL(GCTX.sql_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sql proxy is null", KR(ret), KP(GCTX.sql_proxy_));
    } else if (OB_FAIL(ls_status_op.get_ls_status_info(tenant_id, SYS_LS, sys_ls_info, *GCTX.sql_proxy_))) {
      LOG_WARN("failed to get ls status info", KR(ret), K(tenant_id));
    } else if (OB_FAIL(sys_ls_info.get_unit_list(unit_ids_tmp))) {
      LOG_WARN("fail to get unit list ids", KR(ret), K(sys_ls_info));
    } else if (OB_FAIL(ut_operator.init(*GCTX.sql_proxy_))) {
      LOG_WARN("fail to init unit table operator", KR(ret));
    } else if (OB_FAIL(ut_operator.get_units_by_unit_ids(unit_ids_tmp, units_tmp))) {
      LOG_WARN("fail to get unit by ids", KR(ret), K(tenant_id), K(unit_ids_tmp));
    }
    // SYS LS L-replicas do not standalone. filter out LOGONLY units in sys ls unit_list.
    ARRAY_FOREACH(units_tmp, idx) {
      if (!ObReplicaTypeCheck::gts_standalone_applicable(units_tmp.at(idx).replica_type_)) {
        // skip
      } else if (OB_FAIL(unit_ids.push_back(units_tmp.at(idx).unit_id_))) {
        LOG_WARN("fail to push back", KR(ret), K(units_tmp.at(idx).unit_id_));
      }
    }
  }
  return ret;
}

int ObUnitManager::fill_delete_unit_ptr_array(
    share::ObResourcePool *pool,
    const common::ObIArray<uint64_t> &delete_unit_id_array,
    const int64_t new_unit_num,
    common::ObIArray<ObUnit *> &output_delete_unit_ptr_array)
{
  int ret = OB_SUCCESS;
  output_delete_unit_ptr_array.reset();
  if (delete_unit_id_array.count() > 0) {
    // The alter resource pool shrinkage specifies the deleted unit, just fill it in directly
    for (int64_t i = 0; OB_SUCC(ret) && i < delete_unit_id_array.count(); ++i) {
      ObUnit *unit = NULL;
      if (OB_FAIL(get_unit_by_id(delete_unit_id_array.at(i), unit))) {
        LOG_WARN("fail to get unit by id", K(ret));
      } else if (NULL == unit) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit ptr is null", K(ret), KP(unit));
      } else if (OB_FAIL(output_delete_unit_ptr_array.push_back(unit))) {
        LOG_WARN("fail to push back", K(ret));
      } else {} // no more to do
    }
  } else {
    common::ObArray<ZoneUnitPtr> zone_unit_ptrs;
    ObArray<uint64_t> gts_unit_ids;
    if (OB_UNLIKELY(NULL == pool)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid argument", K(ret), KP(pool));
    } else if (pool->is_granted_to_tenant()
        && OB_FAIL(get_tenant_gts_unit_ids(pool->tenant_id_, gts_unit_ids))) {
      LOG_WARN("fail to get tenant gts unit ids", KR(ret), KPC(pool));
    } else if (OB_FAIL(build_sorted_zone_unit_ptr_array(pool, zone_unit_ptrs))) {
      LOG_WARN("fail to build sorted zone unit ptr array", K(ret));
    } else if (zone_unit_ptrs.count() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("zone unit ptrs has no element", K(ret), "zone_unit_cnt", zone_unit_ptrs.count());
    } else {
      FOREACH_CNT_X(zone_unit_ptr, zone_unit_ptrs, OB_SUCC(ret)) {
        int64_t delete_unit_count = 0;
        DeleteUnitPtrCmp cmp(gts_unit_ids);
        lib::ob_sort(zone_unit_ptr->unit_ptrs_.begin(), zone_unit_ptr->unit_ptrs_.end(), cmp);
        if (OB_FAIL(cmp.ret_)) {
          LOG_WARN("sort failed", KR(ret), K(zone_unit_ptr->zone_));
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < zone_unit_ptr->unit_ptrs_.count()
            && delete_unit_count < (pool->unit_count_ - new_unit_num); ++i) {
          ObUnit *unit = zone_unit_ptr->unit_ptrs_.at(i);
          if (OB_ISNULL(unit)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unit ptr is null", KR(ret), KP(unit));
          } else if (OB_FAIL(output_delete_unit_ptr_array.push_back(unit))) {
            LOG_WARN("fail to push back", K(ret));
          } else {
            delete_unit_count++;
          }
        }//end for unit
      }//end for zone
    }
  }
  return ret;
}


// for granted pools, update unit to DELETING status and update unit groups.
// for ungranted pools, remove unit directly.
// update pool to new unit num in both situations.
int ObUnitManager::shrink_pool_unit_num_(
    common::ObMySQLTransaction &trans,
    share::ObResourcePool *pool,
    const int64_t new_unit_num,
    const common::ObIArray<uint64_t> &delete_unit_id_array)
{
  int ret = OB_SUCCESS;
  const bool is_granted = pool->is_granted_to_tenant();
  bool is_shrinking_allowed = true;
  if (OB_UNLIKELY(NULL == pool || new_unit_num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(pool), K(new_unit_num));
  } else if (pool->unit_count_ <= new_unit_num) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should be a shrink pool operation", K(ret),
             "cur_unit_num", pool->unit_count_,
             "new_unit_num", new_unit_num);
  } else {
    common::ObArray<ObUnit *> output_delete_unit_ptr_array;
    common::ObArray<uint64_t> output_delete_unit_id_array;
    share::ObResourcePool new_pool;
    if (OB_FAIL(new_pool.assign(*pool))) {
      LOG_WARN("fail to assign new pool", K(ret));
    } else {
      new_pool.unit_count_ = new_unit_num;
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ut_operator_.update_resource_pool(trans, new_pool))) {
      LOG_WARN("fail to update resource pool", K(ret));
    } else if (OB_FAIL(check_shrink_unit_num_zone_condition(pool, new_unit_num, delete_unit_id_array))) {
      LOG_WARN("fail to check shrink unit num zone condition", K(ret));
    } else if (OB_FAIL(fill_delete_unit_ptr_array(
            pool, delete_unit_id_array, new_unit_num, output_delete_unit_ptr_array))) {
      LOG_WARN("fail to fill delete unit id array", K(ret));
    } else if (output_delete_unit_ptr_array.count() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("zone unit ptrs has no element", K(ret),
               "zone_unit_cnt", output_delete_unit_ptr_array.count());
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < output_delete_unit_ptr_array.count(); ++i) {
        ObUnit *unit = output_delete_unit_ptr_array.at(i);
        if (OB_UNLIKELY(NULL == unit)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unit ptr is null", K(ret), KP(unit));
        } else if (OB_FAIL(output_delete_unit_id_array.push_back(unit->unit_id_))) {
          LOG_WARN("fail to push back", K(ret));
        } else {
          if (is_granted) {
            unit->status_ = ObUnit::UNIT_STATUS_DELETING;
            if (OB_FAIL(ut_operator_.update_unit_exclude_ug_id(trans, *unit))) {
              LOG_WARN("fail to update unit", KR(ret), KPC(unit));
            }
            FLOG_INFO("set unit deleting", KPC(unit));
          } else {
            if (OB_FAIL(ut_operator_.remove_unit(trans, *unit))) {
              LOG_WARN("fail to remove unit", KR(ret), "unit", *unit);
            }
          }
        } // no more to do
      }
    }
    if (OB_FAIL(ret)) {
    } else if (!is_granted
               && OB_FAIL(delete_inmemory_units(pool->resource_pool_id_, output_delete_unit_id_array))) {
      LOG_WARN("fail to delete unit groups", K(ret));
    } else {
      pool->unit_count_ = new_unit_num;
    }
  }
  return ret;
}

int ObUnitManager::check_shrink_granted_pool_allowed_by_migrate_unit(
    share::ObResourcePool *pool,
    bool &is_allowed)
{
  int ret = OB_SUCCESS;
  is_allowed = true;
  common::ObArray<share::ObUnit *> *units = NULL;
  if (OB_UNLIKELY(NULL == pool)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(pool));
  } else if (OB_FAIL(get_units_by_pool(pool->resource_pool_id_, units))) {
    LOG_WARN("fail to get units by pool", K(ret));
  } else if (OB_UNLIKELY(NULL == units)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unit ptr is null", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && is_allowed && i < units->count(); ++i) {
      const ObUnit *unit = units->at(i);
      if (OB_UNLIKELY(NULL == unit)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit ptr is null", K(ret), KP(unit));
      } else if (unit->migrate_from_server_.is_valid()) {
        is_allowed = false;
      } else {} // unit not in migrating, check next
    }
  }
  return ret;
}

int ObUnitManager::check_all_pools_granted(
    const common::ObIArray<share::ObResourcePool *> &pools,
    bool &all_granted)
{
  int ret = OB_SUCCESS;
  all_granted = true;
  for (int64_t i = 0; all_granted && OB_SUCC(ret) && i < pools.count(); ++i) {
    if (NULL == pools.at(i)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pool ptr is null", K(ret), K(i));
    } else if (!is_valid_tenant_id(pools.at(i)->tenant_id_)) {
      all_granted = false;
    } else {} // go on check next
  }
  return ret;
}

//alter 14x add the new locality F/L@ZONE;
//temporarily logonly_unit = 1 does not support scaling,
//so only the unit num of non-logonly_unit can be reduced;
//Here it is only judged whether the number of units on non-logonly_unit satisfies locality;
//The goal of this function is to determine whether the distribution of the current unit meets the requirements of locality
//check in two parts:
//1. First check whether the logonly unit meets the requirements
//2. second check whether the non-logonly unit meets the requirements
int ObUnitManager::do_check_shrink_granted_pool_allowed_by_locality(
    const common::ObIArray<share::ObResourcePool *> &pools,
    const common::ObIArray<common::ObZone> &schema_zone_list,
    const ZoneLocalityIArray &zone_locality,
    const ObIArray<int64_t> &new_unit_nums,
    bool &is_allowed)
{
  int ret = OB_SUCCESS;
  UNUSED(schema_zone_list);
  is_allowed = true;
  common::hash::ObHashMap<common::ObZone, UnitNum> zone_unit_num_map;
  const int64_t BUCKET_SIZE = 2 * MAX_ZONE_NUM;
  if (OB_UNLIKELY(pools.count() <= 0
                  || new_unit_nums.count() <= 0
                  || new_unit_nums.count() != pools.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(pools.count()), K(new_unit_nums.count()));
  } else if (OB_FAIL(zone_unit_num_map.create(BUCKET_SIZE, ObModIds::OB_RS_UNIT_MANAGER))) {
    LOG_WARN("fail to create map", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < pools.count(); ++i) {
      const int64_t pool_unit_num = new_unit_nums.at(i);
      const share::ObResourcePool *pool = pools.at(i);
      if (OB_UNLIKELY(nullptr == pool)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pool ptr is null", K(ret));
      } else {
        const common::ObIArray<common::ObZone> &pool_zone_list = pool->zone_list_;
        for (int64_t j = 0; OB_SUCC(ret) && j < pool_zone_list.count(); ++j) {
          const common::ObZone &zone = pool_zone_list.at(j);
          const int32_t overwrite = 1;
          UnitNum unit_num_set;
          int tmp_ret = zone_unit_num_map.get_refactored(zone, unit_num_set);
          if (OB_SUCCESS == tmp_ret) {
            // bypass
          } else if (OB_HASH_NOT_EXIST == tmp_ret) {
            unit_num_set.reset();
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("fail to get from map", K(ret));
          }
          if (OB_FAIL(ret)) {
          } else if (REPLICA_TYPE_FULL == pool->replica_type_) {
            unit_num_set.full_unit_num_ += pool_unit_num;
          } else if (REPLICA_TYPE_LOGONLY == pool->replica_type_) {
            unit_num_set.logonly_unit_num_ += pool_unit_num;
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("pool replica type unexpected", K(ret), "pool", *pool);
          }
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(zone_unit_num_map.set_refactored(zone, unit_num_set, overwrite))) {
            LOG_WARN("fail to set map", K(ret));
          }
        }
      }
    }
    for (int64_t i = 0; is_allowed && OB_SUCC(ret) && i < zone_locality.count(); ++i) {
      const share::ObZoneReplicaAttrSet &zone_replica_attr = zone_locality.at(i);
      int64_t full_unit_num = 0;
      int64_t logonly_unit_num = 0;
      for (int64_t j = 0; OB_SUCC(ret) && j < zone_replica_attr.zone_set_.count(); ++j) {
        const common::ObZone &zone = zone_replica_attr.zone_set_.at(j);
        UnitNum unit_num_set;
        if (OB_FAIL(zone_unit_num_map.get_refactored(zone, unit_num_set))) {
          LOG_WARN("fail to get refactored", K(ret));
        } else {
          full_unit_num += unit_num_set.full_unit_num_;
          logonly_unit_num += unit_num_set.logonly_unit_num_;
        }
      }
      if (OB_SUCC(ret)) {
        const int64_t specific_num = zone_replica_attr.get_specific_replica_num();
        const int64_t except_l_specific_num = specific_num
                                              - zone_replica_attr.get_logonly_replica_num()
                                              - zone_replica_attr.get_encryption_logonly_replica_num();
        const int64_t total_unit_num = full_unit_num + logonly_unit_num;
        is_allowed = (total_unit_num >= specific_num && full_unit_num >= except_l_specific_num);
      }
    }
  }
  return ret;
}

// Scaling also needs to consider the locality of the tenant or table under the tenant.
// There are several situations that need to be considered:
// 1 Although the resource pool has been granted to a tenant,
//   but the zone of the resource pool may not be in the tenant's zone list.
//   For example: tenant has two pools : pool1(z1,z2,z3),pool2(z4,z5,z6),
//                The locality of tenant is F@z1,F@z2,F@z3,F@z4,
//                tenant the zone_list is (z1, z2, z3, z4). In this case,
//                only z4 needs to be considered when shrinkg pool2 unit num.
// 2 The locality of the tenant may include zone locality and region locality.
//   For zone locality, it is only necessary to consider that
//   the unit num of the zone after scaling is not less than the number of copies of the locality in this zone.
// 3 For region locality,
//   it is necessary to satisfy that the sum of unit num of all zones in the region under
//   this tenant is not less than the number of locality replicas on the region.
//   For example: tenant has two pools, pool1(z1,z2) and pool2(z3,z4),
//                the current pool1_unit_num = 2, pool2_unit_num = 2,
//                where z1, z4 belong to region SH, z2, z3 belong to region HZ, and locality is F{2},R{2}@SH,F{2},R{2}@HZ.
//   We try to compress the unit_num of pool2 to 1,
//   the total number of units in region SH after compression is 3,
//   and the total number of units in region HZ is 3,
//   which is not enough to accommodate the number of copies of locality.
//   Therefore, compressing the unit num of pool2 to 1 is not allowed.
//The specific implementation is to traverse each zone in the zone list of the resource pool:
//1 The zone does not exist in the tenant's zone list, skip it directly;
//2 The zone exists in the tenant's zone locality.
//  It is necessary to compare whether the compressed unit num is enough to accommodate
//  the number of locality replicas in the zone;
//3 If the zone does not exist in the tenant's zone locality, save the unit num in the region_unit_num_container.
//4 Save the zones of other resource pools under the tenant to the region zone container,
//  and compare whether the unit num is sufficient
int ObUnitManager::check_shrink_granted_pool_allowed_by_tenant_locality(
    share::schema::ObSchemaGetterGuard &schema_guard,
    const uint64_t tenant_id,
    const common::ObIArray<share::ObResourcePool *> &pools,
    const common::ObIArray<int64_t> &new_unit_nums,
    bool &is_allowed)
{
  int ret = OB_SUCCESS;
  const share::schema::ObTenantSchema *tenant_schema = NULL;
  common::ObArray<common::ObZone> tenant_zone_list;
  if (OB_UNLIKELY(! is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_schema))) {
    LOG_WARN("fail to get tenant info", K(ret), K(tenant_id));
  } else if (NULL == tenant_schema) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant schema is null", K(ret), KP(tenant_schema));
  } else if (OB_FAIL(tenant_schema->get_zone_list(tenant_zone_list))) {
    LOG_WARN("fail to get zone list", K(ret));
  } else {
    ObArray<ObZoneReplicaNumSet> zone_locality;
    if (OB_FAIL(tenant_schema->get_zone_replica_attr_array(zone_locality))) {
      LOG_WARN("fail to assign", K(ret));
    } else if (OB_FAIL(do_check_shrink_granted_pool_allowed_by_locality(
                pools, tenant_zone_list, zone_locality, new_unit_nums, is_allowed))) {
      LOG_WARN("fail to do check shrink by locality", K(ret));
    } else {} // no more to do
  }
  return ret;
}

//  Currently supports tenant-level and table-level locality,
//  and check shrinkage needs to be checked at the same time two levels of locality, tenant and table.
//  1 The tenant level is more intuitive.
//    directly compare the zone/region locality involved with the reduced unit num.
//  2 Three situations need to be considered at the table level:
//    2.1 Virtual tables, index tables, etc. without entity partitions, ignore them
//    2.2 The table with entity partition whose table locality is empty,
//        inherited from tenant, has been checked, and skipped;
//    2.3 Tables with entity partitions whose table locality is not empty are checked according to table locality
int ObUnitManager::check_shrink_granted_pool_allowed_by_locality(
    share::ObResourcePool *pool,
    const int64_t new_unit_num,
    bool &is_allowed)
{
  int ret = OB_SUCCESS;
  share::schema::ObSchemaGetterGuard schema_guard;
  const share::schema::ObTenantSchema *tenant_schema = NULL;
  common::ObArray<share::ObResourcePool *> *pool_list = nullptr;
  common::ObArray<share::ObResourcePool *> new_pool_list;
  common::ObArray<int64_t> new_unit_nums;
  if (OB_UNLIKELY(NULL == pool || new_unit_num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(pool), K(new_unit_num));
  } else if (OB_ISNULL(schema_service_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema service is null", K(schema_service_), KR(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(pool->tenant_id_, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", KR(ret), K(pool->tenant_id_));
  } else if (OB_FAIL(schema_guard.get_tenant_info(pool->tenant_id_, tenant_schema))) {
    LOG_WARN("fail to get tenant info", K(ret), "tenant_id", pool->tenant_id_);
  } else if (OB_UNLIKELY(NULL == tenant_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant schema is null", K(ret), KP(tenant_schema));
  } else if (OB_FAIL(get_pools_by_tenant_(tenant_schema->get_tenant_id(), pool_list))) {
    LOG_WARN("fail to get pools by tenant", K(ret));
  } else if (OB_UNLIKELY(nullptr == pool_list)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pool list ptr is null", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < pool_list->count(); ++i) {
      share::ObResourcePool *this_pool = pool_list->at(i);
      if (OB_UNLIKELY(nullptr == this_pool)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("this pool ptr is null", K(ret));
      } else if (OB_FAIL(new_pool_list.push_back(this_pool))) {
        LOG_WARN("fail to to push back", K(ret));
      } else if (OB_FAIL(new_unit_nums.push_back(
              this_pool->resource_pool_id_ == pool->resource_pool_id_
              ? new_unit_num : this_pool->unit_count_))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(check_shrink_granted_pool_allowed_by_tenant_locality(
            schema_guard, pool->tenant_id_, new_pool_list, new_unit_nums, is_allowed))) {
      LOG_WARN("fail to check shrink by tenant locality", K(ret));
    } else {} // no more to do
  }
  return ret;
}

int ObUnitManager::check_shrink_granted_pool_allowed_by_alter_locality(
    share::ObResourcePool *pool,
    bool &is_allowed)
{
  int ret = OB_SUCCESS;
  rootserver::ObRootService *root_service = NULL;
  bool in_alter_locality = true;
  if (OB_UNLIKELY(NULL == pool)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(pool));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(pool->tenant_id_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", K(ret), "tenant_id", pool->tenant_id_);
  } else if (OB_UNLIKELY(NULL == (root_service = GCTX.root_service_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rootservice is null", K(ret));
  } else if (OB_FAIL(root_service->check_tenant_in_alter_locality(
          pool->tenant_id_, in_alter_locality))) {
    LOG_WARN("fail to check tenant in alter locality", K(ret), "tenant_id", pool->tenant_id_);
  } else if (in_alter_locality) {
    is_allowed = false;
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "shrink pool unit num while altering locality");
  } else {} // no more to do
  return ret;
}

//  The shrinking operation of the granted resource pool requires the following pre-checks
//  1 The design avoids simultaneous migration and shrinkage of units.
//    Therefore, it is necessary to check whether all the units contained in the current resource pool are in the migration state before shrinking.
//    If so, shrinking operations are temporarily not allowed, and the migration needs to be completed.
//  2 The scaling operation also needs to consider locality.
//    The number of copies of locality cannot exceed the number of units after scaling.
//    For example, when the locality is F, R@zone1, the unit num in zone1 cannot be reduced to 1.
//  3 Locality changes and scaling under the same tenant are not allowed to be performed at the same time
int ObUnitManager::check_shrink_granted_pool_allowed(
    share::ObResourcePool *pool,
    const int64_t new_unit_num)
{
  int ret = OB_SUCCESS;
  bool is_allowed = false;
  if (OB_UNLIKELY(NULL == pool || new_unit_num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(pool), K(new_unit_num));
  } else if (OB_FAIL(check_shrink_granted_pool_allowed_by_migrate_unit(pool, is_allowed))) {
    LOG_WARN("fail to check by migrate unit", K(ret));
  } else if (!is_allowed) {
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "shrink pool unit num while unit migrating");
  } else if (OB_FAIL(check_shrink_granted_pool_allowed_by_locality(pool, new_unit_num, is_allowed))) {
    LOG_WARN("fail to check by locality", K(ret));
  } else if (!is_allowed) {
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "shrink pool unit num causing not enough locality");
  } else if (OB_FAIL(check_shrink_granted_pool_allowed_by_alter_locality(pool, is_allowed))) {
    LOG_WARN("fail to check shrink granted pool allowed by alter locality", K(ret));
  } else if (!is_allowed) {
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "shrink pool unit num while altering locality");
  }
  if (OB_SUCC(ret) && !is_allowed) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("check shrink granted pool not allowed", KR(ret), KPC(pool));
  }
  return ret;
}

int ObUnitManager::complete_migrate_unit_rs_job_in_pool(
      const int64_t resource_pool_id,
      const int result_ret,
      common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObArray<ObUnit *> *units = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == resource_pool_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(resource_pool_id), K(ret));
  } else if (OB_FAIL(get_units_by_pool(resource_pool_id, units))) {
    LOG_WARN("fail to get unit by pool", K(resource_pool_id), K(ret));
  } else if (OB_UNLIKELY(NULL == units)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("units is null", KP(units), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < units->count(); ++i) {
      const ObUnit *unit = units->at(i);
      if (OB_UNLIKELY(NULL == unit)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit should not be null", K(ret));
      } else if (unit->is_manual_migrate()) {
        char ip_buf[common::MAX_IP_ADDR_LENGTH] = {"\0"};
        (void)unit->server_.ip_to_string(ip_buf, common::MAX_IP_ADDR_LENGTH);
        int64_t job_id = 0;
        int tmp_ret = RS_JOB_FIND(MIGRATE_UNIT, job_id, *proxy_,
                                  "unit_id", unit->unit_id_,
                                  "svr_ip", ip_buf,
                                  "svr_port", unit->server_.get_port());
        if (OB_SUCCESS == tmp_ret && job_id > 0) {
          if (OB_FAIL(RS_JOB_COMPLETE(job_id, result_ret, trans))) {
            LOG_WARN("all_rootservice_job update failed", K(ret), K(result_ret), K(job_id));
          }
        }
      }
    }
  }
  return ret;
}

int ObUnitManager::expand_pool_unit_num_(
    common::ObMySQLTransaction &trans,
    share::ObResourcePool *pool,
    const int64_t new_unit_num)
{
  int ret = OB_SUCCESS;
  const char *module = "ALTER_RESOURCE_POOL_UNIT_NUM";
  const bool new_allocate_pool = false;
  ObArray<ObUnit> new_units;
  ObArray<share::ObUnit *> *units = NULL;
  ObArray<uint64_t> bak_unit_ids;
  ObUnit::Status new_unit_status = ObUnit::UNIT_STATUS_MAX;
  if (OB_ISNULL(pool)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("pool ptr is null", KR(ret));
  } else if (OB_UNLIKELY(new_unit_num <= pool->unit_count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("new_unit_num less than current unit_num", KR(ret), KPC(pool), K(new_unit_num));
  } else if (OB_FAIL(get_units_by_pool(pool->resource_pool_id_, units))) {
    LOG_WARN("fail to get units by pool", K(ret));
  } else if (OB_FAIL(extract_unit_ids(*units, bak_unit_ids))) {
    LOG_WARN("fail to extract unit ids", K(ret));
  } else if (OB_FAIL(determine_new_added_unit_status(*pool, new_unit_status))) {
    LOG_WARN("fail to determine new added unit status", KR(ret), KPC(pool));
  } else if (OB_FAIL(allocate_pool_units_(
          trans, *pool, pool->zone_list_, nullptr, new_allocate_pool,
          new_unit_num - pool->unit_count_, new_unit_status, module, new_units))) {
    LOG_WARN("arrange units failed", "pool", *pool, KR(ret), KPC(pool), K(new_allocate_pool),
             K(new_unit_status), K(new_unit_num));
  } else {
    share::ObResourcePool new_pool;
    if (OB_FAIL(new_pool.assign(*pool))) {
      LOG_WARN("failed to assign new_poll", K(ret));
    } else {
      new_pool.unit_count_ = new_unit_num;
      if (OB_FAIL(ut_operator_.update_resource_pool(trans, new_pool))) {
        LOG_WARN("update_resource_pool failed", K(new_pool), K(ret));
      }
    }
  }

  if (OB_FAIL(ret)) {
    // avoid overwrite ret
    int64_t temp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (temp_ret = delete_invalid_inmemory_units(pool->resource_pool_id_, bak_unit_ids))) {
      LOG_WARN("delete unit groups failed", "resource pool id", pool->resource_pool_id_, K(temp_ret));
    }
  } else {
    pool->unit_count_ = new_unit_num;
  }
  return ret;
}

int ObUnitManager::get_unit_count_per_zone_by_status_(
    const share::ObResourcePool *pool,
    int64_t &active_unit_cnt,
    int64_t &adding_unit_cnt,
    int64_t &deleting_unit_cnt)
{
  int ret = OB_SUCCESS;
  common::ObArray<ZoneUnitPtr> zone_unit_ptrs;
  if (OB_ISNULL(pool)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pool is nullptr", KR(ret));
  } else if (OB_FAIL(build_sorted_zone_unit_ptr_array(pool, zone_unit_ptrs))) {
    LOG_WARN("fail to build sorted zone_unit_ptr array", KPC(pool));
  } else if (zone_unit_ptrs.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("zone unit ptrs has no element", K(ret), "zone_unit_cnt", zone_unit_ptrs.count());
  } else {
    int64_t unit_cnt_per_zone_by_status[ObUnit::UNIT_STATUS_MAX] = {0};
    ARRAY_FOREACH(zone_unit_ptrs, zone_idx) {
      const ZoneUnitPtr &zone_unit_ptr = zone_unit_ptrs.at(zone_idx);
      int64_t curr_zone_unit_cnt_by_status[ObUnit::UNIT_STATUS_MAX] = {0};
      ARRAY_FOREACH(zone_unit_ptr.unit_ptrs_, j) {
        const ObUnit *unit = zone_unit_ptr.unit_ptrs_.at(j);
        if (OB_UNLIKELY(NULL == unit)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unit ptr is null", K(ret), KP(unit), K(zone_unit_ptr));
        } else if (!unit->is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unit is invalid", KR(ret), KPC(unit));
        } else {
          curr_zone_unit_cnt_by_status[unit->status_]++;
        }
      } // end for unit
      for (int64_t j = 0; OB_SUCC(ret) && j < ObUnit::UNIT_STATUS_MAX; ++j) {
        if (0 == zone_idx) {
          unit_cnt_per_zone_by_status[j] = curr_zone_unit_cnt_by_status[j];
        } else if (unit_cnt_per_zone_by_status[j] != curr_zone_unit_cnt_by_status[j]) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unit cnt in each zone not match", K(unit_cnt_per_zone_by_status),
                   "curr_zone", zone_unit_ptr.zone_, K(curr_zone_unit_cnt_by_status));
        }
      } // end for unit_status
    } // end for zone
    if (OB_SUCC(ret)) {
      active_unit_cnt = unit_cnt_per_zone_by_status[ObUnit::UNIT_STATUS_ACTIVE];
      deleting_unit_cnt = unit_cnt_per_zone_by_status[ObUnit::UNIT_STATUS_DELETING];
      adding_unit_cnt = unit_cnt_per_zone_by_status[ObUnit::UNIT_STATUS_ADDING];
      // defense check
      if (OB_UNLIKELY(adding_unit_cnt > 0 && deleting_unit_cnt > 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("one single pool can not have deleting and adding unit simultaneously", KR(ret),
                KPC(pool), K(active_unit_cnt), K(adding_unit_cnt), K(deleting_unit_cnt));
      } else if (OB_UNLIKELY(active_unit_cnt + adding_unit_cnt != pool->unit_count_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected unit count", KR(ret), KPC(pool), K(active_unit_cnt), K(adding_unit_cnt));
      }
    }
  }
  return ret;
}

// Determine type of incoming operation according to current unit_num
//   and also previous unit_num if pool is during expanding or shrinking.
// Let:
//   cur_unit_num: pool->unit_count_, the current unit_num of pool
//   prev_unit_num: the previous unit_num before in-process expanding of shrinking,
//                  equal to sum of ACTIVE and DELETING status unit per zone.
//   new_unit_num: the num that new in-coming operation trying to set.
// The principle is as follows:
// 1 cur_unit_num == new_unit_num, no change, no modification, new operation is NOP
// 2 cur_unit_num <new_unit_num, and pool is not shrinking or expanding,
//   then new operation is EXPAND.
// 3 cur_unit_num <new_unit_num, and the resource pool is shrinking,
//   and new_unit_num is equal to prev_unit_num, then new operation is ROLLBACK.
// 4 cur_unit_num> new_unit_num, and pool is not shrinking or expanding,
//   then new operation is SHRINK
// 5 cur_unit_num >new_unit_num, and pool is expanding,
//   and new_unit_num is equal to prev_unit_num, then new operation is ROLLBACK
// 6 all other situation are not allowed, represented by MAX.
int ObUnitManager::determine_alter_unit_num_type(
    share::ObResourcePool *pool,
    const int64_t new_unit_num,
    AlterUnitNumType &alter_unit_num_type)
{
  int ret = OB_SUCCESS;
  const int64_t cur_unit_num = pool->unit_count_;
  int64_t active_unit_cnt = 0;
  int64_t adding_unit_cnt = 0;
  int64_t deleting_unit_cnt = 0;
  if (OB_UNLIKELY(NULL == pool)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("pool is null", K(ret));
  } else if (new_unit_num == cur_unit_num) {
    alter_unit_num_type = AUN_NOP; // do nothing
  } else if (OB_FAIL(get_unit_count_per_zone_by_status_(pool, active_unit_cnt, adding_unit_cnt, deleting_unit_cnt))) {
    LOG_WARN("fail to get unit count per zone by status", KR(ret), KPC(pool));
  } else {
    if (adding_unit_cnt > 0 || deleting_unit_cnt > 0) {
      // A unit num change is taking place, new operation can only be ROLLBACK.
      // previous unit_num include DELETING units, exclude ADDING units
      const int64_t prev_unit_num = active_unit_cnt + deleting_unit_cnt;
      if (new_unit_num == prev_unit_num) {
        alter_unit_num_type = AUN_ROLLBACK;
      } else {
        alter_unit_num_type = AUN_MAX;
      }
    } else {
      if (new_unit_num > cur_unit_num) {
        alter_unit_num_type = AUN_EXPAND;
      } else if (new_unit_num < cur_unit_num) {
        alter_unit_num_type = AUN_SHRINK;
      } else {
        alter_unit_num_type = AUN_NOP;
      }
    }
  }
  return ret;
}

int ObUnitManager::precheck_for_alter_pool_unit_num_(
    share::ObResourcePool *pool,
    const int64_t new_unit_num,
    const ObIArray<ObUnitManager::PoolOperationInfo> &pool_op_info_array)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(pool)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pool is null ptr", KR(ret));
  } else if (new_unit_num < 0 || pool_op_info_array.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(new_unit_num), K(pool_op_info_array));
  } else if (pool->is_granted_to_tenant()) {
    bool in_hetero_deploy_mode = false;
    bool is_gts_standalone = false;
    if (OB_FAIL(check_tenant_in_heterogeneous_deploy_mode(pool->tenant_id_, in_hetero_deploy_mode))) {
      LOG_WARN("fail to check_tenant_in_heterogeneous_deploy_mode", KR(ret), KPC(pool));
    } else if (OB_FAIL(check_tenant_gts_standalone_enabled(pool->tenant_id_, is_gts_standalone))) {
      LOG_WARN("fail to check_tenant_gts_standalone_enabled", KR(ret), KPC(pool));
    } else if (!in_hetero_deploy_mode) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("zone deploy mode is homo mode, alter resource pool not allowed", KR(ret), KPC(pool));
      LOG_USER_ERROR_WITH_ARGS(OB_OP_NOT_ALLOW, "Tenant %lu zone_deploy_mode is 'homo', not 'hetero', "
          "alter resource pool unit_num", pool->tenant_id_);
    } else if (OB_FAIL(check_unit_num_legal_after_pools_operation_(pool->tenant_id_, pool_op_info_array,
        in_hetero_deploy_mode, is_gts_standalone))) {
      LOG_WARN("fail to check tenant pools unit_num legal", KR(ret), KPC(pool),
              K(in_hetero_deploy_mode), K(new_unit_num), K(in_hetero_deploy_mode), K(is_gts_standalone));
    }
  }
  return ret;
}

// alter_pool_unit_num changes a single pool's unit_num,
int ObUnitManager::alter_pool_unit_num(
    share::ObResourcePool *pool,
    int64_t new_unit_num,
    const common::ObIArray<uint64_t> &delete_unit_id_array,
    const common::ObString &sql_text)
{
  int ret = OB_SUCCESS;
  share::ObUnitTableTransaction trans;
  ObArray<share::ObResourcePool *> pools;
  ObArray<share::ObResourcePool *> altered_pools;
  ObArray<PoolOperationInfo> pool_op_info_array;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(inited_), K(loaded_), K(ret));
  } else if (NULL == pool) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("pool is null", K(ret));
  } else if (new_unit_num <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid new_unit_num", K(new_unit_num), K(ret));
  } else if (has_duplicates(delete_unit_id_array)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("delete same unit more than once not supported", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "delete same unit more than once");
  } else if (OB_FAIL(trans.start(proxy_, OB_SYS_TENANT_ID))) {
    LOG_WARN("fail to start transaction", KR(ret));
  } else if (OB_FAIL(pools.push_back(pool))) {
    LOG_WARN("fail to push pool to pools", KR(ret), KPC(pool));
  } else if (OB_FAIL(batch_determine_altered_pools_op_info_(pools, new_unit_num, altered_pools, pool_op_info_array))) {
    LOG_WARN("fail to batch determine altered pools op info", KR(ret), K(new_unit_num));
  } else if (altered_pools.count() != pool_op_info_array.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("altered pools count not match pool op info array count", KR(ret),
              K(altered_pools.count()), K(pool_op_info_array.count()));
  } else {
    // 1. check argument delete_unit_id_array is valid for operation
    if (pool_op_info_array.empty() || AUN_SHRINK != pool_op_info_array.at(0).alter_type_) {
      if (!delete_unit_id_array.empty()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("do not support expand or rollback pool unit num combined with deleting unit", K(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "expand or rollback pool unit num combined with deleting unit");
      }
    }
    // 2. do alter pool unit num
    if (OB_SUCC(ret) && !pool_op_info_array.empty()) {
      if (OB_FAIL(precheck_for_alter_pool_unit_num_(pool, new_unit_num, pool_op_info_array))) {
        LOG_WARN("fail to precheck for alter unit num", KR(ret), KPC(pool), K(new_unit_num), K(pool_op_info_array));
      } else if (pool->is_granted_to_tenant()
          && OB_FAIL(register_alter_resource_tenant_unit_num_rs_job(pool->tenant_id_, new_unit_num, sql_text, trans))) {
        LOG_WARN("fail to register alter resource tenant unit num rs job", KR(ret),
            KPC(pool), K(sql_text), K(new_unit_num));
      } else if (OB_FAIL(do_alter_pool_unit_num_(trans, pool, pool_op_info_array.at(0), delete_unit_id_array))) {
        LOG_WARN("fail to do_alter_pool_unit_num_", KR(ret), KPC(pool),
                "pool_op_info", pool_op_info_array.at(0), K(delete_unit_id_array));
      } else if (pool->is_granted_to_tenant()
          && ObReplicaTypeCheck::need_to_align_to_ug(pool->replica_type_)
          && OB_FAIL(adjust_altered_pools_unit_group_id_(trans, pool->tenant_id_, altered_pools))) {
        LOG_WARN("fail to adjust_altered_pools_unit_group_id", KR(ret), K(pool->tenant_id_), K(pool_op_info_array));
      }
    }
  }
  if (trans.is_started()) {
    const bool commit = (OB_SUCCESS == ret);
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(trans.end(commit))) {
      LOG_WARN("fail to trans end", KR(tmp_ret), K(commit));
      ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
    }
  }
  return ret;
}

int ObUnitManager::adjust_altered_pools_unit_group_id_(
    common::ObMySQLTransaction &trans,
    const uint64_t tenant_id,
    common::ObIArray<share::ObResourcePool *> &altered_pools)
{
  int ret = OB_SUCCESS;
  if (!is_valid_tenant_id(tenant_id) || altered_pools.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id or altered_pools", KR(ret), K(tenant_id), K(altered_pools));
  } else {
    ObArray<ObArray<uint64_t>> ref_pools_ug_id_arrays;
    // construct ref pools, tenant's other pools except for altered pools
    ObArray<share::ObResourcePool *> *tenant_pools = nullptr;
    if (OB_FAIL(get_pools_by_tenant_(tenant_id, tenant_pools))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        // tenant is in creating, granting initial resource pools
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to get tenant_pools by tenant", KR(ret), K(tenant_id));
      }
    } else if (OB_ISNULL(tenant_pools)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant_pools ptr is null", KR(ret), K(tenant_id));
    } else {
      // for each tenant pool, if not one of altered_pools, push to ref_pools_ug_ids
      ObArray<uint64_t> ref_pool_ug_id_array;
      ARRAY_FOREACH(*tenant_pools, i) {
        share::ObResourcePool *pool = tenant_pools->at(i);
        ref_pool_ug_id_array.reuse();
        if (OB_ISNULL(pool)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("pool ptr is null", KR(ret), K(i));
        } else if (!ObReplicaTypeCheck::need_to_align_to_ug(pool->replica_type_)) {
          // skip. all ug_ids of pool are 0.
        } else if (!has_exist_in_array(altered_pools, pool)) {
          if (OB_FAIL(ut_operator_.get_pool_unit_group_ids(trans, pool->resource_pool_id_, ref_pool_ug_id_array))) {
            LOG_WARN("fail to get pool unit group ids", KR(ret), K(pool));
          } else if (OB_FAIL(ref_pools_ug_id_arrays.push_back(ref_pool_ug_id_array))) {
            LOG_WARN("fail to push_back", KR(ret));
          }
        }
      }// end of each of tenant pools
    }
    // for each altered pool, adjust unit group id
    ARRAY_FOREACH(altered_pools, i) {
      share::ObResourcePool *pool = altered_pools.at(i);
      ObArray<uint64_t> unit_group_id_array;
      if (OB_ISNULL(pool)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pool is null", KR(ret));
      } else if (!ObReplicaTypeCheck::need_to_align_to_ug(pool->replica_type_)) {
          // skip. all ug_ids of pool are 0. No need to adjust.
      } else if (OB_FAIL(determine_pool_unit_group_id_(trans, pool, ref_pools_ug_id_arrays, unit_group_id_array))) {
        LOG_WARN("fail to determine pool ug_id", KR(ret), KPC(pool), K(ref_pools_ug_id_arrays));
      } else if (OB_FAIL(reallocate_pool_unit_group_id_(trans, pool, unit_group_id_array))) {
        LOG_WARN("fail to reallocate pool unit group id", KR(ret), KPC(pool), K(unit_group_id_array));
      // for adjusted altered pool, make it a ref_pool as well
      } else if (OB_FAIL(ref_pools_ug_id_arrays.push_back(unit_group_id_array))) {
        LOG_WARN("fail to push back", KR(ret));
      }
    }// end for each of altered_pools
  }
  return ret;
}

// Determine expected unit_group_id_array for pool refer to ref_pools of same tenant.
// In one tenant, pools with same unit_count are homo pools, else hetero pools.
// The principle of unit_group_id is:
// 1. homo pools must share exactly the same unit_group_ids;
// 2. hetero pools can not have any shared unit_group_id;
// 3. unit_group_id of DELETING units are not considered.
int ObUnitManager::determine_pool_unit_group_id_(
    common::ObMySQLTransaction &trans,
    const share::ObResourcePool *pool,
    const ObIArray<ObArray<uint64_t>> &ref_pools_ug_id_arrays,
    ObArray<uint64_t> &unit_group_id_array)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(pool)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pool is nullptr", KR(ret));
  } else {
    // 1. for each ref pools, check whether homo or hetero pool
    bool found_homo_ref_pool = false;
    for (int64_t i = 0; OB_SUCC(ret) && !found_homo_ref_pool && i < ref_pools_ug_id_arrays.count(); ++i) {
      const ObArray<uint64_t> &ref_pool_ug_ids = ref_pools_ug_id_arrays.at(i);
      if (ref_pool_ug_ids.count() == pool->unit_count_) {
        found_homo_ref_pool = true;
        if (OB_FAIL(unit_group_id_array.assign(ref_pool_ug_ids))) {
          LOG_WARN("fail to assign unit group id array", KR(ret));
        }
      }
    }
    // 2. if no homo_ref_pool, fetch new unit group ids
    if (OB_SUCC(ret) && !found_homo_ref_pool) {
      if (OB_FAIL(batch_fetch_new_unit_group_id(pool->unit_count_, unit_group_id_array))) {
        LOG_WARN("fail to batch fetch unit group id", KR(ret), K(unit_group_id_array), KPC(pool));
      }
    }
    LOG_INFO("determined pool unit_group_id array", KR(ret), KPC(pool), K(ref_pools_ug_id_arrays),
        K(unit_group_id_array));
  }
  return ret;
}

// Reallocate pool unit_group_id of units in inner_table.
// Ensure units in each zone use exactly the same ug_ids with new_unit_group_id_array,
//  except for DELETING units.
// Some units may already match with new_ug_id_array, just update those units that do not match.
int ObUnitManager::reallocate_pool_unit_group_id_(
    common::ObMySQLTransaction &trans,
    const share::ObResourcePool *pool,
    const common::ObIArray<uint64_t> &new_unit_group_id_array)
{
  int ret = OB_SUCCESS;
  common::ObArray<ObUnit> pool_units; // get from inner_table for accurate ug_id
  if (OB_ISNULL(pool)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pool is null ptr", KR(ret));
  } else if (OB_UNLIKELY(new_unit_group_id_array.count() != pool->unit_count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("new_unit_group_id_array count does not match pool unit_count", KR(ret),
        KPC(pool), K(new_unit_group_id_array));
  } else if (OB_FAIL(ut_operator_.get_units_by_pool_id(trans, pool->resource_pool_id_, pool_units))) {
    LOG_WARN("fail to get units by pool id", KR(ret), K(pool->resource_pool_id_));
  } else {
    ObArray<const ObUnit *> zone_unit_array;
    common::ObArray<uint64_t> zone_ug_id_array;
    ARRAY_FOREACH(pool->zone_list_, i) {
      const ObZone &curr_zone = pool->zone_list_.at(i);
      zone_unit_array.reuse();
      if (OB_FAIL(zone_ug_id_array.assign(new_unit_group_id_array))) {
        LOG_WARN("failed to assign new_unit_group_id_array", KR(ret), K(new_unit_group_id_array));
      }
      // filter units of curr zone that need to reallocate
      ARRAY_FOREACH(pool_units, j) {
        const ObUnit &unit = pool_units.at(j);
        int64_t matched_idx = 0;
        if (unit.zone_ != curr_zone || ObUnit::UNIT_STATUS_DELETING == unit.status_) {
          // ignore DELETING unit or unit of other zone
        } else if (has_exist_in_array(zone_ug_id_array, unit.unit_group_id_, &matched_idx)) {
          // unit already matched with one ug_id, no need to update this unit
          // one ug_id can only match with one unit, so remove this ug_id
          if (OB_FAIL(zone_ug_id_array.remove(matched_idx))) {
            LOG_WARN("fail to remove matched unit group id", KR(ret), K(matched_idx));
          }
        } else if (OB_FAIL(zone_unit_array.push_back(&unit))) {
            LOG_WARN("fail to push back unit", KR(ret), K(unit));
        }
      }// end for each pool_unit
      // reallocate units in curr zone
      if (OB_FAIL(ret)) {
      } else if (OB_UNLIKELY(zone_ug_id_array.count() != zone_unit_array.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("filtered unit count not match", KR(ret), K(zone_ug_id_array), K(zone_unit_array),
            K(curr_zone), KPC(pool), K(pool_units), K(new_unit_group_id_array));
      } else {
        ARRAY_FOREACH(zone_unit_array, j) {
          const ObUnit *unit = zone_unit_array.at(j);
          CK(OB_NOT_NULL(unit));
          if (OB_SUCC(ret)) {
            ObUnitUGOp ug_op(unit->unit_id_, unit->unit_group_id_, zone_ug_id_array.at(j));
            if (OB_FAIL(ut_operator_.update_unit_ug_id(trans, ug_op))) {
              LOG_WARN("fail to update unit", KR(ret), K(unit), K(ug_op));
            } else {
              ROOTSERVICE_EVENT_ADD("unit", "reallocate_ug",
                  "tenant_id", pool->tenant_id_,
                  "unit_id", unit->unit_id_,
                  "old_ug_id", ug_op.get_old_unit_group_id(),
                  "new_ug_id", ug_op.get_new_unit_group_id());
            }
            LOG_INFO("update unit ug_id", KR(ret), K(curr_zone), K(unit), K(ug_op));
          }
        }//end for each unit to update in curr_zone
      }
    }//end for each zone of pool zone_list
    LOG_INFO("reallocated pool units ug_ids", KR(ret), KPC(pool), K(pool_units), K(new_unit_group_id_array));
  }
  return ret;
}

int ObUnitManager::do_alter_pool_unit_num_(
    common::ObMySQLTransaction &trans,
    share::ObResourcePool *pool,
    const PoolOperationInfo &pool_op_info,
    const common::ObIArray<uint64_t> &delete_unit_id_array)
{
  int ret = OB_SUCCESS;
  const char *module = "ALTER_RESOURCE_POOL_UNIT_NUM";
  AlterUnitNumType alter_unit_num_type = pool_op_info.alter_type_;
  const int64_t new_unit_num = pool_op_info.new_unit_num_;
  if (OB_ISNULL(pool)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pool is null ptr", KR(ret));
  } else if (OB_UNLIKELY(pool->unit_count_ != pool_op_info.old_unit_num_
      || (AUN_SHRINK != alter_unit_num_type && !delete_unit_id_array.empty()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KPC(pool), K(pool_op_info), K(delete_unit_id_array));
  } else if (pool->is_granted_to_tenant()
      && pool_op_info.new_unit_num_ < pool_op_info.old_unit_num_  // could be SHRINK or ROLLBACK
      && OB_FAIL(check_shrink_granted_pool_allowed(pool, new_unit_num)))  {
      LOG_WARN("fail to check shrink granted pool allowed", KR(ret), KPC(pool), K(new_unit_num));
  } else if (AUN_EXPAND == alter_unit_num_type) {
    if (OB_FAIL(expand_pool_unit_num_(trans, pool, new_unit_num))) {
      LOG_WARN("fail to expend pool unit num", KR(ret), K(new_unit_num));
    }
  } else if (AUN_SHRINK == alter_unit_num_type) {
    if (OB_FAIL(shrink_pool_unit_num_(trans, pool, new_unit_num, delete_unit_id_array))) {
      LOG_WARN("fail to shrink pool unit num", KR(ret), K(new_unit_num));
    }
  } else if (AUN_ROLLBACK == alter_unit_num_type) {
    if (OB_FAIL(rollback_pool_unit_num_(trans, pool, new_unit_num))) {
      LOG_WARN("fail to rollback shrink pool unit num", KR(ret), KPC(pool), K(new_unit_num));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected alter unit num type", K(ret), K(alter_unit_num_type));
  }
  return ret;
}

int ObUnitManager::get_zone_pools_unit_num(
    const common::ObZone &zone,
    const common::ObIArray<share::ObResourcePoolName> &new_pool_name_list,
    int64_t &total_unit_num,
    int64_t &full_unit_num,
    int64_t &logonly_unit_num)
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (zone.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(zone));
  } else {
    common::ObArray<share::ObResourcePool *> pool_list;
    for (int64_t i = 0; OB_SUCC(ret) && i < new_pool_name_list.count(); ++i) {
      const share::ObResourcePoolName &pool_name = new_pool_name_list.at(i);
      share::ObResourcePool *pool = NULL;
      if (OB_FAIL(inner_get_resource_pool_by_name(pool_name, pool))) {
        LOG_WARN("fail to get resource pool by name", K(ret));
      } else if (OB_UNLIKELY(NULL == pool)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pool ptr is null", K(ret), K(pool_name));
      } else if (OB_FAIL(pool_list.push_back(pool))) {
        LOG_WARN("fail to push back", K(ret));
      } else {} // no more to do
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(inner_get_zone_pools_unit_num(
            zone, pool_list, total_unit_num, full_unit_num, logonly_unit_num))) {
      LOG_WARN("fail to inner get pools unit num", K(ret));
    }
  }
  return ret;
}

int ObUnitManager::check_can_remove_pool_zone_list(
    const share::ObResourcePool *pool,
    const common::ObIArray<common::ObZone> &to_be_removed_zones,
    bool &can_remove)
{
  int ret = OB_SUCCESS;
  can_remove = true;
  common::ObArray<share::ObResourcePool *> *pool_list = NULL;
  share::schema::ObSchemaGetterGuard schema_guard;
  const share::schema::ObTenantSchema *tenant_schema = NULL;
  if (OB_UNLIKELY(NULL == pool) || OB_UNLIKELY(to_be_removed_zones.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(pool), K(to_be_removed_zones));
  } else if (!pool->is_granted_to_tenant()) {
    can_remove = true; // this pool do not grant to any tenant, can remove zone unit
  } else if (OB_FAIL(get_pools_by_tenant_(pool->tenant_id_, pool_list))) {
    LOG_WARN("fail to get pools by tenant", K(ret));
  } else if (OB_UNLIKELY(NULL == pool_list)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pool list ptr is null", K(ret));
  } else if (OB_ISNULL(schema_service_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema service is null", K(schema_service_), KR(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(pool->tenant_id_, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", KR(ret), K(pool->tenant_id_));
  } else if (OB_FAIL(schema_guard.get_tenant_info(pool->tenant_id_, tenant_schema))) {
    LOG_WARN("fail to get tenant schema", K(ret), "tenant_id", pool->tenant_id_);
  } else if (NULL == tenant_schema) {
    ret = OB_TENANT_NOT_EXIST;
    LOG_WARN("tenant schema is null", K(ret));
  } else if (!tenant_schema->get_previous_locality_str().empty()) {
    can_remove = false;
    LOG_WARN("alter pool zone list is not allowed while locality modification",
             "tenant_id", tenant_schema->get_tenant_id());
  } else {
    for (int64_t i = 0; can_remove && OB_SUCC(ret) && i < to_be_removed_zones.count(); ++i) {
      int64_t total_unit_num = 0;
      int64_t full_unit_num = 0;
      int64_t logonly_unit_num = 0;
      const common::ObZone &zone = to_be_removed_zones.at(i);
      bool enough = false;
      if (OB_FAIL(inner_get_zone_pools_unit_num(
              zone, *pool_list, total_unit_num, full_unit_num, logonly_unit_num))) {
        LOG_WARN("fail to get zone pools unit num", K(ret));
      } else if (total_unit_num != full_unit_num + logonly_unit_num) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit num value not match", K(ret),
                 K(total_unit_num), K(full_unit_num), K(logonly_unit_num));
      } else {
        // Reduce the unit of this pool on to be remove zone
        total_unit_num -= pool->unit_count_;
        if (common::REPLICA_TYPE_FULL == pool->replica_type_) {
          full_unit_num -= pool->unit_count_;
        } else if (common::REPLICA_TYPE_LOGONLY == pool->replica_type_) {
          logonly_unit_num -= pool->unit_count_;
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("do not support this pool type", K(ret), K(*pool));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(inner_check_schema_zone_unit_enough(
            zone, total_unit_num, full_unit_num, logonly_unit_num,
            *tenant_schema, schema_guard, enough))) {
        LOG_WARN("fail to inner check schema zone unit enough", K(ret));
      } else if (!enough) {
        can_remove = false;
      } else {}
    }
  }
  return ret;
}

int ObUnitManager::inner_get_zone_pools_unit_num(
    const common::ObZone &zone,
    const common::ObIArray<share::ObResourcePool *> &pool_list,
    int64_t &total_unit_num,
    int64_t &full_unit_num,
    int64_t &logonly_unit_num)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check inner stat failed", K(ret));
  } else if (zone.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(zone));
  } else {
    total_unit_num = 0;
    full_unit_num = 0;
    logonly_unit_num = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < pool_list.count(); ++i) {
      share::ObResourcePool *pool = pool_list.at(i);
      if (OB_UNLIKELY(NULL == pool)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pool ptr is null", K(ret));
      } else if (!has_exist_in_array(pool->zone_list_, zone)) {
        //ignore
      } else {
        total_unit_num += pool->unit_count_;
        if (common::REPLICA_TYPE_FULL == pool->replica_type_) {
          full_unit_num += pool->unit_count_;
        } else if (common::REPLICA_TYPE_LOGONLY == pool->replica_type_) {
          logonly_unit_num += pool->unit_count_;
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("do not support this pool type", K(ret), K(*pool));
        }
      }
    }
  }
  return ret;
}

int ObUnitManager::alter_pool_zone_list(
    share::ObResourcePool *pool,
    const ObIArray<ObZone> &zone_list)
{
  // The requirement of alter pool_zone_list is currently only for cluster relocation.
  // Before deleting the zone,
  // the pool_zone_list that contains the zone in all resource pools needs to be removed from the zone_list..
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(inited_), K(loaded_), K(ret));
  } else if (OB_UNLIKELY(NULL == pool || zone_list.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(pool), "new zone list", zone_list);
  } else {
    ObArray<ObZone> new_zone_list;
    //check repeat
    for (int64_t i = 0; i < zone_list.count() && OB_SUCC(ret); ++i) {
      if (has_exist_in_array(new_zone_list, zone_list.at(i))) {
        ret = OB_OP_NOT_ALLOW;
        LOG_USER_ERROR(OB_OP_NOT_ALLOW, "alter resource pool zone repeat");
        LOG_WARN("alter resource pool zone repeat not allow", K(ret));
      } else if (OB_FAIL(new_zone_list.push_back(zone_list.at(i)))) {
        LOG_WARN("fail to push back", K(ret));
      } else {}//no more to do
    }
    if (OB_SUCC(ret)) {
      lib::ob_sort(new_zone_list.begin(), new_zone_list.end());
      bool is_add_pool_zone = false;
      bool is_remove_pool_zone = false;
      for (int64_t i = 0; i < new_zone_list.count() && OB_SUCC(ret); ++i) {
        if (!has_exist_in_array(pool->zone_list_, new_zone_list.at(i))) {
          is_add_pool_zone = true;
        } else {}//nothing todo
      }
      for (int64_t i = 0; i < pool->zone_list_.count() && OB_SUCC(ret); ++i) {
        if (!has_exist_in_array(new_zone_list, pool->zone_list_.at(i))) {
          is_remove_pool_zone = true;
        } else {}//nothing todo
      }
      if (is_add_pool_zone && is_remove_pool_zone) {
        ret = OB_OP_NOT_ALLOW;
        LOG_USER_ERROR(OB_OP_NOT_ALLOW, "Cannot add and delete zones at the same time");
        LOG_WARN("Cannot add and delete zones at the same time", K(ret));
      } else if (is_add_pool_zone) {
        if (OB_FAIL(add_pool_zone_list(pool, new_zone_list))) {
          LOG_WARN("fail to add pool zone list", K(ret));
        }
      } else {
        if (OB_FAIL(remove_pool_zone_list(pool, new_zone_list))) {
          LOG_WARN("fail to remoce pool zone list", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObUnitManager::remove_pool_zone_list(
    share::ObResourcePool *pool,
    const ObIArray<ObZone> &zone_list)
{
  int ret = OB_SUCCESS;
  bool can_remove = false;
  common::ObArray<common::ObZone> zones_to_be_removed;
  const common::ObIArray<common::ObZone> &prev_zone_list = pool->zone_list_;
  if (OB_FAIL(cal_to_be_removed_pool_zone_list(
          prev_zone_list, zone_list, zones_to_be_removed))) {
    LOG_WARN("fail to calculate to be removed pool zone list", K(ret));
  } else if (zones_to_be_removed.count() <= 0) {
    // no zones need to be removed, return SUCC directly
  } else if (OB_FAIL(check_can_remove_pool_zone_list(pool, zones_to_be_removed, can_remove))) {
    LOG_WARN("fail to check can remove pool zon list", K(ret));
  } else if (!can_remove) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("cannot alter resource pool zone list", K(ret),
             "pool", *pool, K(zones_to_be_removed));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "alter resource pool zone list with not empty unit");
  } else if (OB_FAIL(do_remove_pool_zone_list(pool, zone_list, zones_to_be_removed))) {
    LOG_WARN("fail to do remove pool zone list", K(ret), "pool", *pool, K(zones_to_be_removed));
  } else {} // no more to do
  return ret;
}

int ObUnitManager::add_pool_zone_list(
    share::ObResourcePool *pool,
    const ObIArray<ObZone> &zone_list)
{
  int ret = OB_SUCCESS;
  bool can_add = false;
  common::ObArray<common::ObZone> zones_to_be_add;
  bool is_add_pool_zone_list_allowed = true;
  if (OB_UNLIKELY(nullptr == pool)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(pool));
  } else if (! pool->is_granted_to_tenant()) {
    // good, can do add pool zone list directly
    is_add_pool_zone_list_allowed = true;
  } else {
    if (OB_FAIL(check_expand_zone_resource_allowed_by_old_unit_stat_(
            pool->tenant_id_, is_add_pool_zone_list_allowed))) {
      LOG_WARN("fail to check grant pools allowed by unit stat",
               KR(ret), "tenant_id", pool->tenant_id_);
    }
  }

  if (OB_FAIL(ret)) {
    // bypass
  } else if (!is_add_pool_zone_list_allowed) {
    ret = OB_OP_NOT_ALLOW;
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "add pool zone when granted pools of tenant in shrinking or expanding");
  } else if (OB_FAIL(cal_to_be_add_pool_zone_list(
          pool->zone_list_, zone_list, zones_to_be_add))) {
    LOG_WARN("fail to calculate to be add pool zone list", K(ret));
  } else if (zones_to_be_add.count() <= 0) {
    //no zones need to be add, return SUCC directly
  } else if (OB_FAIL(check_can_add_pool_zone_list_by_locality(pool, zones_to_be_add, can_add))) {
    LOG_WARN("fail to check can add pool zone list", K(ret));
  } else if (!can_add) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("cannot alter resource pool zone list", K(ret),
             "pool", *pool, K(zones_to_be_add));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "alter resource pool zone list with not empty unit");
  } else if (OB_FAIL(do_add_pool_zone_list(pool, zone_list, zones_to_be_add))) {
    LOG_WARN("fail to do add pool zone list", K(ret), "pool", *pool, K(zones_to_be_add));
  } else {} // no more to do
  return ret;
}

int ObUnitManager::cal_to_be_add_pool_zone_list(
    const common::ObIArray<common::ObZone> &prev_zone_list,
    const common::ObIArray<common::ObZone> &cur_zone_list,
    common::ObIArray<common::ObZone> &to_be_add_zones) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(prev_zone_list.count() <= 0 || cur_zone_list.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret),
             "prev zone list", prev_zone_list,
             "cur zone list", cur_zone_list);
  } else {
    to_be_add_zones.reset();
    //Check if the added zone exists
    for (int64_t i = 0; OB_SUCC(ret) && i < cur_zone_list.count(); ++i) {
      const common::ObZone &this_zone = cur_zone_list.at(i);
      bool zone_exist = false;
      if (OB_FAIL(zone_mgr_.check_zone_exist(this_zone, zone_exist))) {
        LOG_WARN("failed to check zone exists", K(ret), K(this_zone));
      } else if (!zone_exist) {
        ret = OB_ZONE_INFO_NOT_EXIST;
        LOG_WARN("zone not exists", K(ret), K(this_zone));
      } else {
        if (has_exist_in_array(prev_zone_list, this_zone)) {
          //still exist, do nothing
        } else if (has_exist_in_array(to_be_add_zones, this_zone)) {
          //just add one time
          ret = OB_OP_NOT_ALLOW;
          LOG_USER_ERROR(OB_OP_NOT_ALLOW, "add repeat zone");
          LOG_WARN("not allow add repeat zone", K(ret));
        } else if (OB_FAIL(to_be_add_zones.push_back(this_zone))) {
          LOG_WARN("fail to push back", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObUnitManager::check_can_add_pool_zone_list_by_locality(
    const share::ObResourcePool *pool,
    const common::ObIArray<common::ObZone> &to_be_add_zones,
    bool &can_add)
{
  int ret = OB_SUCCESS;
  can_add = true;
  common::ObArray<share::ObResourcePool *> *pool_list = NULL;
  share::schema::ObSchemaGetterGuard schema_guard;
  const share::schema::ObTenantSchema *tenant_schema = NULL;
  if (OB_UNLIKELY(NULL == pool) || OB_UNLIKELY(to_be_add_zones.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ivnalid argument", K(ret), KP(pool), K(to_be_add_zones));
  } else if (! pool->is_granted_to_tenant()) {
    can_add = true; // not in tenant, can add zone unit
  } else if (OB_FAIL(get_pools_by_tenant_(pool->tenant_id_, pool_list))) {
    LOG_WARN("fail to get pools by tenant", K(ret));
  } else if (OB_ISNULL(schema_service_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema service is null", K(schema_service_), KR(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(pool->tenant_id_, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", KR(ret), K(pool->tenant_id_));
  } else if (OB_FAIL(schema_guard.get_tenant_info(pool->tenant_id_, tenant_schema))) {
    LOG_WARN("fail to get tenant schema", K(ret), "tenant_id", pool->tenant_id_);
  } else if (NULL == tenant_schema) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("tenant schema is null", K(ret));
  } else if (!tenant_schema->get_previous_locality_str().empty()) {//No locality changes are allowed at this time
    can_add = false;
    LOG_WARN("alter pool zone list is not allowed while locality modification",
             "tenant_id", tenant_schema->get_tenant_id());
  } else {} //nothing todo
  return ret;
}

int ObUnitManager::do_add_pool_zone_list(
    share::ObResourcePool *pool,
    const common::ObIArray<common::ObZone> &new_zone_list,
    const common::ObIArray<common::ObZone> &to_be_add_zones)
{
  int ret = OB_SUCCESS;
  share::ObUnitTableTransaction trans;
  share::ObResourcePool new_pool;
  const char *module = "ALTER_RESOURCE_POOL_ZONE_LIST";
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(inited_), K(loaded_), K(ret));
  } else if (OB_UNLIKELY(NULL == pool)
             || OB_UNLIKELY(to_be_add_zones.count() <= 0)
             || OB_UNLIKELY(new_zone_list.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(pool), K(to_be_add_zones), K(new_zone_list));
  } else {
    if (OB_FAIL(new_pool.assign(*pool))) {
      LOG_WARN("fail to assign new pool", K(ret));
    } else if (OB_FAIL(new_pool.zone_list_.assign(new_zone_list))) {
      LOG_WARN("fail to assign new pool zone list", K(ret));
    } else if (OB_FAIL(trans.start(proxy_, OB_SYS_TENANT_ID))) {
      LOG_WARN("start transaction failed", K(ret));
    } else {
      if (OB_FAIL(increase_units_in_zones_(trans, *pool, to_be_add_zones, module))) {
        LOG_WARN("fail to add units in zones", K(module), KR(ret),
                 "pool id", pool->resource_pool_id_, K(to_be_add_zones));
      } else if (OB_FAIL(pool->zone_list_.assign(new_zone_list))) {
        LOG_WARN("fail to update pool zone list in memory", K(ret));
      } else if (OB_FAIL(ut_operator_.update_resource_pool(trans, new_pool))) {
        LOG_WARN("fail to update resource pool", K(ret));
      } else {} //no more to do
      if (trans.is_started()) {
        const bool commit = (OB_SUCC(ret));
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = trans.end(commit))) {
          LOG_WARN("fail to end trans", K(commit), K(tmp_ret));
          ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
        }
      } else {}//nothing todo
    }
  }
  return ret;
}

// allocate new units on new added zones for resource pool
int ObUnitManager::increase_units_in_zones_(
    common::ObISQLClient &client,
    share::ObResourcePool &pool,
    const common::ObIArray<common::ObZone> &to_be_add_zones,
    const char *module)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> new_unit_group_id_array;
  const bool new_allocate_pool = false;   // whether to alloacate new resource pool
  const int64_t new_unit_count = pool.unit_count_;  // new unit count on every zone
  ObArray<ObUnit> new_units;

  for (int64_t i = 0; OB_SUCC(ret) && i < to_be_add_zones.count(); ++i) {
    bool zone_exist = false;
    if (OB_FAIL(zone_mgr_.check_zone_exist(to_be_add_zones.at(i), zone_exist))) {
      LOG_WARN("failed to check zone exists", K(ret), K(to_be_add_zones.at(i)));
    } else if (!zone_exist) {
      ret = OB_ZONE_INFO_NOT_EXIST;
      LOG_WARN("zone not exists", K(ret), K(to_be_add_zones.at(i)));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (!pool.is_granted_to_tenant() || !ObReplicaTypeCheck::need_to_align_to_ug(pool.replica_type_)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < pool.unit_count_; ++i) {
      if (OB_FAIL(new_unit_group_id_array.push_back(0))) {
        LOG_WARN("fail to push back", KR(ret));
      }
    }
  } else if (OB_FAIL(ut_operator_.get_pool_unit_group_ids(client, pool.resource_pool_id_, new_unit_group_id_array))) {
    LOG_WARN("fail to get pool unit group ids", KR(ret), K(pool));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(allocate_pool_units_(client, pool, to_be_add_zones, &new_unit_group_id_array,
      new_allocate_pool, new_unit_count, ObUnit::UNIT_STATUS_ACTIVE, module, new_units))) {
    LOG_WARN("fail to allocate new units for new added zones", K(module), KR(ret), K(to_be_add_zones), K(pool),
        K(new_unit_count), K(new_unit_group_id_array));
  }

  return ret;
}

int ObUnitManager::do_remove_pool_zone_list(
    share::ObResourcePool *pool,
    const common::ObIArray<common::ObZone> &new_zone_list,
    const common::ObIArray<common::ObZone> &to_be_removed_zones)
{
  int ret = OB_SUCCESS;
  share::ObUnitTableTransaction trans;
  share::ObResourcePool new_pool;
  common::ObArray<common::ObZone> new_zone_list1;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(inited_), K(loaded_), K(ret));
  } else if (OB_UNLIKELY(NULL == pool)
             || OB_UNLIKELY(to_be_removed_zones.count() <= 0)
             || OB_UNLIKELY(new_zone_list.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(pool), K(to_be_removed_zones), K(new_zone_list));
  } else {
    if (OB_FAIL(new_pool.assign(*pool))) {
      LOG_WARN("fail to assign new pool", K(ret));
    } else if (OB_FAIL(new_pool.zone_list_.assign(new_zone_list))) {
      LOG_WARN("fail to assign new pool zone list", K(ret));
    } else if (OB_FAIL(trans.start(proxy_, OB_SYS_TENANT_ID))) {
      LOG_WARN("start transaction failed", K(ret));
    } else {
      if (OB_FAIL(ut_operator_.remove_units_in_zones(
              trans, pool->resource_pool_id_, to_be_removed_zones))) {
        LOG_WARN("fail to remove units in zones", K(ret),
                 "pool id", pool->resource_pool_id_, K(to_be_removed_zones));
      } else if (OB_FAIL(ut_operator_.update_resource_pool(trans, new_pool))) {
        LOG_WARN("fail to update resource pool", K(ret));
      } else {} // no more to do
      if (trans.is_started()) {
        const bool commit = (OB_SUCC(ret));
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = trans.end(commit))) {
          LOG_WARN("fail to end trans", K(commit), K(tmp_ret));
          ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(delete_units_in_zones(pool->resource_pool_id_, to_be_removed_zones))) {
        LOG_WARN("fail to delete units in zones", K(ret),
                 "pool id", pool->resource_pool_id_, K(to_be_removed_zones));
      } else if (OB_FAIL(pool->zone_list_.assign(new_zone_list))) {
        LOG_WARN("fail to update pool zone list in memory", K(ret));
      } else {} // no more to do
    }
  }
  return ret;
}

int ObUnitManager::cal_to_be_removed_pool_zone_list(
    const common::ObIArray<common::ObZone> &prev_zone_list,
    const common::ObIArray<common::ObZone> &cur_zone_list,
    common::ObIArray<common::ObZone> &to_be_removed_zones) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(prev_zone_list.count() <= 0 || cur_zone_list.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret),
             "prev zone list", prev_zone_list,
             "cur zone list", cur_zone_list);
  } else {
    to_be_removed_zones.reset();
    // Each zone in cur_zone_list must be included in prev_zone_list
    for (int64_t i = 0; OB_SUCC(ret) && i < cur_zone_list.count(); ++i) {
      const common::ObZone &this_zone = cur_zone_list.at(i);
      if (!has_exist_in_array(prev_zone_list, this_zone)) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "alter resource pool zone list with a new zone");
        LOG_WARN("alter resource pool zone list with a new zone is not supported", K(ret));
      } else {} // good
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < prev_zone_list.count(); ++i) {
      const common::ObZone &this_zone = prev_zone_list.at(i);
      if (has_exist_in_array(cur_zone_list, this_zone)) {
        // still exist, do nothing
      } else if (OB_FAIL(to_be_removed_zones.push_back(this_zone))) {
        LOG_WARN("fail to push back", K(ret));
      } else {} // no more to do
    }
  }
  return ret;
}

int ObUnitManager::check_full_resource_pool_memory_condition(
    const common::ObIArray<share::ObResourcePool *> &pools,
    const int64_t memory_size) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < pools.count(); ++i) {
    const share::ObResourcePool *pool = pools.at(i);
    if (OB_UNLIKELY(nullptr == pool)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pool ptr is null", K(ret), KP(pool));
    } else if (REPLICA_TYPE_FULL != pool->replica_type_) {
      // bypass
    } else if (memory_size < GCONF.__min_full_resource_pool_memory) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("full resource pool min memory illegal", KR(ret), K(memory_size),
          "__min_full_resource_pool_memory", (int64_t)GCONF.__min_full_resource_pool_memory);
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "unit MEMORY_SIZE less than __min_full_resource_pool_memory");
    }
  }
  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_USE_DUMMY_SERVER);
int ObUnitManager::check_expand_resource_(
    const char *module,
    const common::ObIArray<share::ObResourcePool  *> &pools,
    const share::ObUnitResource &old_resource,
    const share::ObUnitResource &new_resource) const
{
  int ret = OB_SUCCESS;
  common::hash::ObHashMap<ObAddr, int64_t> server_ref_count_map;
  ObString err_str;
  AlterResourceErr err_index = ALT_ERR;
  int temp_ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K_(inited), K_(loaded), K(ret));
  } else if (pools.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("pools is empty", K(pools), K(ret));
  } else if (!old_resource.is_valid() || !new_resource.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid old_resource or invalid new_resource", K(old_resource), K(new_resource), K(ret));
  } else if (!new_resource.has_expanded_resource_than(old_resource)) {
    // skip, no need to check
    LOG_INFO("new unit_resource has no expanded resource, skip check_expand_resource",
             KR(ret), K(old_resource), K(new_resource));
  } else if (OB_FAIL(server_ref_count_map.create(
      SERVER_REF_COUNT_MAP_BUCKET_NUM, ObModIds::OB_HASH_BUCKET_SERVER_REF_COUNT_MAP))) {
    LOG_WARN("pool_unit_map_ create failed",
        "bucket_num", static_cast<int64_t>(SERVER_REF_COUNT_MAP_BUCKET_NUM), K(ret));
  } else if (OB_FAIL(get_pools_servers(pools, server_ref_count_map))) {
    LOG_WARN("get pools server failed", K(pools), K(ret));
  } else {
    bool can_expand = true;
    const ObUnitResource delta = new_resource - old_resource;
    ObUnitResource expand_resource;
    ObServerInfoInTable server_info;
    ObCStringHelper helper;
    _LOG_INFO("[%s] check_expand_resource begin. old=%s, new=%s, delta=%s", module,
        helper.convert(old_resource), helper.convert(new_resource), helper.convert(delta));

    FOREACH_X(iter, server_ref_count_map, OB_SUCCESS == ret) {
      expand_resource = delta * (iter->second);
      const ObAddr &server = iter->first;
      server_info.reset();
      helper.reset();
      _LOG_INFO("[%s] check_expand_resource. svr=%s, pools=%ld, expand_resource=%s", module,
          helper.convert(server), iter->second, helper.convert(expand_resource));
      if (OB_FAIL(SVR_TRACER.get_server_info(server, server_info))) {
        LOG_WARN("fail to get server_info", KR(ret), K(server));
      } else if (OB_UNLIKELY(!server_info.is_active())) {
        ret = OB_OP_NOT_ALLOW;
        LOG_WARN("server is inactive, cannot check_expand_resource", KR(ret), K(server), K(server_info));
        const int64_t ERR_MSG_LEN = 256;
        char err_msg[ERR_MSG_LEN] = {'\0'};
        int tmp_ret = OB_SUCCESS;
        int64_t pos = 0;
        if (OB_TMP_FAIL(databuff_print_multi_objs(err_msg, ERR_MSG_LEN, pos, "Server ",
            OB_SUCCESS != ERRSIM_USE_DUMMY_SERVER ? ObAddr() : server,
            " is inactive, expanding resource"))){
          LOG_WARN("format err_msg failed", KR(tmp_ret), KR(ret));
        } else {
          LOG_USER_ERROR(OB_OP_NOT_ALLOW, err_msg);
        }
      } else if (OB_FAIL(check_expand_resource_(server_info, expand_resource, can_expand, err_index))) {
        LOG_WARN("check expand resource failed", KR(ret), K(server_info));
      } else if (!can_expand) {
        const ObZone &zone = server_info.get_zone();
        helper.reset();
        LOG_USER_ERROR(OB_MACHINE_RESOURCE_NOT_ENOUGH, helper.convert(zone), helper.convert(server),
            alter_resource_err_to_str(err_index));
        // return ERROR
        ret = OB_MACHINE_RESOURCE_NOT_ENOUGH;
      }
    }
  }
  return ret;
}

int ObUnitManager::check_expand_resource_(
    const share::ObServerInfoInTable &server_info,
    const ObUnitResource &expand_resource,
    bool &can_expand,
    AlterResourceErr &err_index) const
{
  int ret = OB_SUCCESS;
  double hard_limit = 0;
  bool can_hold_unit = false;
  can_expand = true;
  obrpc::ObGetServerResourceInfoResult report_server_resource_info;
  // some item of expand_resource may be negative, so we don't check expand_resource here
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K_(inited), K_(loaded), K(ret));
  } else if (!server_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server_info", KR(ret), K(server_info));
  } else if (OB_FAIL(get_hard_limit(hard_limit))) {
    LOG_WARN("get_hard_limit failed", K(ret));
  } else if (OB_FAIL(get_server_resource_info_via_rpc(server_info, report_server_resource_info))) {
    LOG_WARN("get_server_resource_info_via_rpc failed", KR(ret), K(server_info));
  } else if (OB_FAIL(have_enough_resource(
      report_server_resource_info,
      expand_resource,
      hard_limit,
      can_hold_unit,
      err_index))) {
    LOG_WARN("fail to check have enough resource", KR(ret), K(hard_limit),
        K(report_server_resource_info), K(expand_resource));
  } else if (!can_hold_unit) {
    can_expand = false;
    // don't need to set ret
    LOG_WARN("find server can't hold expanded resource", KR(ret), K(server_info),
        K(report_server_resource_info), K(expand_resource));
  } else {
    can_expand = true;
  }
  return ret;
}

int ObUnitManager::check_shrink_resource_(const ObIArray<share::ObResourcePool *> &pools,
                                          const ObUnitResource &resource,
                                          const ObUnitResource &new_resource) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K_(inited), K_(loaded), K(ret));
  } else if (pools.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("pools is empty", K(pools), K(ret));
  } else if (!resource.is_valid() || !new_resource.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid resource or invalid new_resource", K(resource), K(new_resource), K(ret));
  } else if (!new_resource.has_shrunk_resource_than(resource)) {
    // skip, no need to check
    LOG_INFO("new unit_resource has no shrunk resource, skip check_shrink_resource",
             KR(ret), K(resource), K(new_resource));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < pools.count(); ++i) {
      const share::ObResourcePool *pool = pools.at(i);
      if (OB_UNLIKELY(NULL == pool)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pool ptr is null", K(ret));
      } else if (OB_FAIL(check_shrink_resource_(*pool, resource, new_resource))) {
        LOG_WARN("fail to check shrink resource", KR(ret));
      } else {} // no more to do
    }
  }
  return ret;
}

int ObUnitManager::check_shrink_resource_(const share::ObResourcePool &pool,
                                          const ObUnitResource &resource,
                                          const ObUnitResource &new_resource) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K_(inited), K_(loaded), K(ret));
  } else if (!pool.is_valid() || !resource.is_valid() || !new_resource.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid pool or invalid resource or invalid new_resource", K(pool),
        K(resource), K(new_resource), K(ret));
  } else {
    if (new_resource.max_cpu() < resource.max_cpu()) {
      // cpu don't need check
    }

    if (new_resource.memory_size() < resource.memory_size()) {
      if (!pool.is_granted_to_tenant()) {
        // do nothing
      } else if (OB_FAIL(check_shrink_memory(pool,resource.memory_size(), new_resource.memory_size()))) {
        LOG_WARN("check_shrink_memory failed", "tenant_id", pool.tenant_id_,
            "old_resource_mem", resource.memory_size(),
            "new_resource_mem", new_resource.memory_size(), KR(ret));
      }
    }

    if (new_resource.log_disk_size() < resource.log_disk_size()) {
      // log disk don't need check.
    } 
  }
  return ret;
}

int ObUnitManager::check_shrink_memory(
    const share::ObResourcePool &pool,
    const int64_t old_memory,
    const int64_t new_memory) const
{
  int ret = OB_SUCCESS;
  ObArray<ObUnit *> *units = NULL;
  ObArray<common::ObAddr> unit_servers;

  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K_(inited), K_(loaded), K(ret));
  } else if (! pool.is_granted_to_tenant()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("pool is not granted to valid tenant, invalid pool", KR(ret), K(pool));
  } else if (old_memory <= 0 || new_memory <= 0 || new_memory >= old_memory) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid old_memory or invalid new_memory",
        K(old_memory), K(new_memory), K(ret));
  } else if (OB_FAIL(get_units_by_pool(pool.resource_pool_id_, units))) {
    LOG_WARN("fail to get units by pool", K(ret));
  } else if (OB_UNLIKELY(NULL == units)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("units ptr is null", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < units->count(); ++i) {
      ObUnit *unit = units->at(i);
      if (OB_UNLIKELY(NULL == unit)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit ptr is null", K(ret));
      } else if (OB_FAIL(unit_servers.push_back(unit->server_))) {
        LOG_WARN("fail to push back", K(ret));
      } else if (!unit->migrate_from_server_.is_valid()) {
        // this unit is not in migrating
      } else if (OB_FAIL(unit_servers.push_back(unit->migrate_from_server_))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
    const double max_used_ratio = 0.9;
    const double shrink_ratio = static_cast<double>(new_memory)
        / static_cast<double>(old_memory);
    int64_t max_used_memory = 0;
    ObArray<ObTenantMemstoreInfoOperator::TenantServerMemInfo> mem_infos;
    ObTenantMemstoreInfoOperator mem_info_operator(*proxy_);
    if (OB_FAIL(ret)) {
      // failed
    } else if (OB_FAIL(mem_info_operator.get(pool.tenant_id_, unit_servers, mem_infos))) {
      LOG_WARN("mem_info_operator get failed", K(ret));
    } else {
      FOREACH_CNT_X(mem_info, mem_infos, OB_SUCCESS == ret) {
        max_used_memory = static_cast<int64_t>(static_cast<double>(mem_info->memstore_limit_)
            * shrink_ratio * max_used_ratio);
        if (mem_info->total_memstore_used_ > max_used_memory) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "requested memory over 90 percent of total available memory");
          LOG_WARN("new memory will cause memory use percentage over ninety percentage",
              "mem_info", *mem_info, K(old_memory), K(new_memory),
              K(max_used_ratio), K(max_used_memory), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObUnitManager::change_pool_config(share::ObResourcePool *pool, ObUnitConfig *config,
                                      ObUnitConfig *new_config)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K_(inited), K_(loaded), K(ret));
  } else if (NULL == pool || NULL == config || NULL == new_config) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP(pool), KP(config), KP(new_config), K(ret));
  } else {
    share::ObResourcePool new_pool;
    if (OB_FAIL(new_pool.assign(*pool))) {
      LOG_WARN("failed to assign new_pool", K(ret));
    }
    if (OB_SUCC(ret)) {
      new_pool.unit_config_id_ = new_config->unit_config_id();
      if (OB_FAIL(ut_operator_.update_resource_pool(*proxy_, new_pool))) {
        LOG_WARN("ut_operator_ update_resource_pool failed", K(new_pool), K(ret));
      } else if (OB_FAIL(dec_config_ref_count(config->unit_config_id()))) {
        LOG_WARN("dec_config_ref_count failed", "unit_config_id",
            config->unit_config_id(), K(ret));
      } else if (OB_FAIL(inc_config_ref_count(new_config->unit_config_id()))) {
        LOG_WARN("inc_config_ref_count failed", "unit_config_id",
            new_config->unit_config_id(), K(ret));
      } else if (OB_FAIL(delete_config_pool(config->unit_config_id(), pool))) {
        LOG_WARN("delete config pool failed", "config id", config->unit_config_id(), K(ret));
      } else if (OB_FAIL(insert_config_pool(new_config->unit_config_id(), pool))) {
        LOG_WARN("insert config pool failed", "config id", new_config->unit_config_id(), K(ret));
      } else if (OB_FAIL(update_pool_load(pool, new_config))) {
        LOG_WARN("update resource pool load failed", K(ret), "resource_pool", *pool,
            "unit_config", *new_config);
      } else {
        pool->unit_config_id_ = new_config->unit_config_id();
      }
    }
  }
  return ret;
}

// The zones of multiple pools have no intersection
// 14x new semantics. If it is the source_pool used to store the copy of L, it can be compared
int ObUnitManager::check_pool_intersect_(
    const uint64_t tenant_id,
    const ObIArray<ObResourcePoolName> &pool_names,
    bool &intersect)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObString, OB_DEFAULT_REPLICA_NUM> zones;
  common::ObArray<share::ObResourcePool *> *pools = NULL;;
  intersect = false;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K_(inited), K_(loaded), KR(ret));
  } else if (pool_names.count() <= 0 || !is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("pool_names is empty", K(pool_names), K(tenant_id), KR(ret));
  } else {
    FOREACH_CNT_X(pool_name, pool_names, OB_SUCCESS == ret && !intersect) {
      share::ObResourcePool *pool = NULL;
      if (OB_FAIL(inner_get_resource_pool_by_name(*pool_name, pool))) {
        LOG_WARN("get resource pool by name failed", "pool_name", *pool_name, KR(ret));
      } else if (NULL == pool) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pool is null", KP(pool), KR(ret));
      } else {
        FOREACH_CNT_X(zone, pool->zone_list_, OB_SUCCESS == ret && !intersect) {
          if (NULL == zone) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unit is null", KR(ret));
          } else {
            ObString zone_str;
            zone_str.assign_ptr(zone->ptr(), static_cast<int32_t>(zone->size()));
            if (has_exist_in_array(zones, zone_str)) {
              intersect = true;
            } else if (OB_FAIL(zones.push_back(zone_str))) {
              LOG_WARN("push_back failed", KR(ret));
            }
          }
        }  // end foreach zone
      }
    }  // end foreach pool
    if (OB_FAIL(ret)) {
    } else if (intersect) {
    } else if (OB_FAIL(get_pools_by_tenant_(tenant_id, pools))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        // a new tenant, without resource pool already granted
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to get pools by tenant", KR(ret), K(tenant_id));
      }
    } else if (OB_UNLIKELY(NULL == pools)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pools is null", KR(ret), KP(pools));
    } else {
      for (int64_t i = 0; !intersect && OB_SUCC(ret) && i < pools->count(); ++i) {
        const share::ObResourcePool *pool = pools->at(i);
        if (OB_UNLIKELY(NULL == pool)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("pool ptr is null", KR(ret), KP(pool));
        } else {
          for (int64_t j = 0; !intersect && OB_SUCC(ret) && j < zones.count(); ++j) {
            common::ObZone zone;
            if (OB_FAIL(zone.assign(zones.at(j).ptr()))) {
              LOG_WARN("fail to assign zone", KR(ret));
            } else if (has_exist_in_array(pool->zone_list_, zone)) {
              intersect = true;
            } else {} // good
          }
        }
      }
    }
  }
  return ret;
}

int ObUnitManager::check_pool_ownership_(const uint64_t tenant_id,
                                        const common::ObIArray<share::ObResourcePoolName> &pool_names,
                                        const bool grant)
{
  int ret = OB_SUCCESS;
  share::ObResourcePool *pool = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < pool_names.count(); ++i) {
    if (OB_FAIL(inner_get_resource_pool_by_name(pool_names.at(i), pool))) {
      LOG_WARN("get resource pool by name failed", "pool_name", pool_names.at(i), KR(ret));
    } else if (OB_ISNULL(pool)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pool is null", KP(pool), KR(ret));
    }
    if (OB_FAIL(ret)) {
    } else if (grant) {
      if (pool->is_granted_to_tenant()) {
        ret = OB_RESOURCE_POOL_ALREADY_GRANTED;
        ObCStringHelper helper;
        LOG_USER_ERROR(OB_RESOURCE_POOL_ALREADY_GRANTED, helper.convert(pool_names.at(i)));
        LOG_WARN("pool has already granted to other tenant, can't grant again",
                  KR(ret), K(tenant_id), "pool", *pool);
      } else {/*good*/}
    } else {
      if (!pool->is_granted_to_tenant()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("find pool not granted to any tenant, can not revoke",
            "pool", *pool, K(tenant_id), KR(ret));
      } else if (pool->tenant_id_ != tenant_id) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("find pool already granted to other tenant, can not revoke",
            "pool", *pool, K(tenant_id), KR(ret));
      } else {/*good*/}
    }
  }
  return ret;
}

int ObUnitManager::do_grant_pools_(
    ObMySQLTransaction &trans,
    const lib::Worker::CompatMode compat_mode,
    const ObIArray<share::ObResourcePoolName> &pool_names,
    const uint64_t tenant_id,
    const bool check_data_version)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K_(inited), K_(loaded), KR(ret));
  } else if (!is_valid_tenant_id(tenant_id) || pool_names.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(pool_names), KR(ret));
  } else if (OB_UNLIKELY(nullptr == srv_rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("srv_rpc_proxy_ ptr is null", KR(ret));
  } else {
    ObNotifyTenantServerResourceProxy notify_proxy(
                                      *srv_rpc_proxy_,
                                      &obrpc::ObSrvRpcProxy::notify_tenant_server_unit_resource);
    share::ObResourcePool new_pool;
    ObArray<share::ObResourcePool *> pool_ptrs;
    ObArray<ObUnit> persistent_units;
    for (int64_t i = 0; OB_SUCC(ret) && i < pool_names.count(); ++i) {
      share::ObResourcePool *pool = nullptr;
      common::ObArray<ObUnit *> zone_sorted_unit_array;
      if (OB_FAIL(inner_get_resource_pool_by_name(pool_names.at(i), pool))) {
        LOG_WARN("get resource pool by name failed", "pool_name", pool_names.at(i), KR(ret));
      } else if (OB_ISNULL(pool)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pool is null", KP(pool), KR(ret));
      } else if (OB_FAIL(pool_ptrs.push_back(pool))) {
        LOG_WARN("fail to push back", KR(ret));
      } else if (OB_FAIL(new_pool.assign(*pool))) {
        LOG_WARN("failed to assign new_pool", KR(ret));
      } else if (FALSE_IT(new_pool.tenant_id_ = tenant_id)) {
        // shall never be here
      } else if (OB_FAIL(ut_operator_.update_resource_pool(trans, new_pool))) {
        LOG_WARN("update_resource_pool failed", K(new_pool), KR(ret));
      } else if (OB_FAIL(build_zone_sorted_unit_array_(pool, zone_sorted_unit_array))) {
        LOG_WARN("failed to generate zone_sorted_unit_array", KR(ret), KPC(pool));
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < zone_sorted_unit_array.count(); ++j) {
          ObUnit *unit = zone_sorted_unit_array.at(j);
          if (OB_ISNULL(unit)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unit ptr is null", KR(ret));
          } else if (OB_FAIL(try_notify_tenant_server_unit_resource_(
                  tenant_id, false /*is_delete*/, notify_proxy,
                  new_pool.unit_config_id_, compat_mode, *unit,
                  false/*if_not_grant*/, false/*skip_offline_server*/,
                  check_data_version))) {
            LOG_WARN("fail to try notify server unit resource", KR(ret));
          } else if (OB_FAIL(persistent_units.push_back(*unit))) {
            LOG_WARN("fail to push an element into persistent_units", KR(ret));
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(adjust_altered_pools_unit_group_id_(trans, tenant_id, pool_ptrs))) {
        LOG_WARN("fail to adjust_altered_pools_unit_group_id", KR(ret), K(tenant_id));
      }
    }

    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(notify_proxy.wait())) {
      LOG_WARN("fail to wait notify resource", KR(ret), K(tmp_ret));
      ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
    } else if (OB_SUCC(ret)) {
      // arg/dest/result can be used here.
    }
    ret = ERRSIM_UNIT_PERSISTENCE_ERROR ? : ret;
    if (OB_FAIL(ret)) {
      LOG_WARN("start to rollback unit persistence", KR(ret), K(persistent_units), K(tenant_id));
      if (OB_TMP_FAIL(rollback_persistent_units_(tenant_id, persistent_units, notify_proxy))) {
        LOG_WARN("fail to rollback unit persistence", KR(ret), KR(tmp_ret), K(persistent_units));
      }
    }
  }
  return ret;
}

int ObUnitManager::do_revoke_pools_(
    ObMySQLTransaction &trans,
    const ObIArray<ObResourcePoolName> &pool_names,
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  const lib::Worker::CompatMode dummy_mode = lib::Worker::CompatMode::INVALID;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K_(inited), K_(loaded), KR(ret));
  } else if (!is_valid_tenant_id(tenant_id) || pool_names.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(pool_names), KR(ret));
  } else if (OB_UNLIKELY(nullptr == srv_rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("srv_rpc_proxy_ ptr is null", KR(ret));
  } else {
    ObNotifyTenantServerResourceProxy notify_proxy(
                                      *srv_rpc_proxy_,
                                      &obrpc::ObSrvRpcProxy::notify_tenant_server_unit_resource);
    share::ObResourcePool new_pool;
    ObArray<share::ObResourcePool *> shrinking_expanding_pools;
    ObArray<share::ObResourcePool> pools;
    for (int64_t i = 0; OB_SUCC(ret) && i < pool_names.count(); ++i) {
      share::ObResourcePool *pool = NULL;
      bool altering_unit_num = false;
      common::ObArray<ObUnit *> zone_sorted_unit_array;
      if (OB_FAIL(inner_get_resource_pool_by_name(pool_names.at(i), pool))) {
        LOG_WARN("get resource pool by name failed", "pool_name", pool_names.at(i), KR(ret));
      } else if (OB_ISNULL(pool)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pool is null", KP(pool), KR(ret));
      } else if (OB_FAIL(inner_check_pool_altering_unit_num(pool->resource_pool_id_, altering_unit_num))) {
        LOG_WARN("inner check pool in shrinking or expanding failed", KR(ret), "pool", *pool, K(altering_unit_num));
      } else if (altering_unit_num && OB_FAIL(shrinking_expanding_pools.push_back(pool))) {
        LOG_WARN("fail to push back shrinking resource pool before revoked", KR(ret), K(tenant_id), "pool", *pool);
      } else if (OB_FAIL(new_pool.assign(*pool))) {
        LOG_WARN("failed to assign new_pool", KR(ret));
      } else if (FALSE_IT(new_pool.tenant_id_ = OB_INVALID_ID)) {
        // shall never be here
      } else if (OB_FAIL(ut_operator_.update_resource_pool(trans, new_pool))) {
        LOG_WARN("update_resource_pool failed", K(new_pool), KR(ret));
      } else if (OB_FAIL(build_zone_sorted_unit_array_(pool, zone_sorted_unit_array))) {
        LOG_WARN("failed to generate zone_sorted_unit_array", KR(ret), KPC(pool));
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < zone_sorted_unit_array.count(); ++j) {
          ObUnit *unit = zone_sorted_unit_array.at(j);
          if (OB_ISNULL(unit)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unit ptr is null", KR(ret));
          } else if (OB_FAIL(try_notify_tenant_server_unit_resource_(
                  tenant_id, true /*is_delete*/, notify_proxy,
                  new_pool.unit_config_id_, dummy_mode, *unit,
                  false/*if_not_grant*/, false/*skip_offline_server*/,
                  false /*check_data_version*/))) {
            LOG_WARN("fail to try notify server unit resource", KR(ret));
          } else if (!ObReplicaTypeCheck::need_to_align_to_ug(unit->replica_type_)) {
            // no need to update ug_id, because it's always 0
          } else if (OB_FAIL(ut_operator_.update_unit_ug_id(trans, ObUnitUGOp(unit->unit_id_, 0)))) {
            LOG_WARN("fail to update unit", KR(ret), K(ObUnitUGOp(unit->unit_id_, 0)));
          }
        }
      }
    }
    // If some of the pools are shrinking, commit these shrinking pools now.
    if (OB_SUCCESS != ret) {
    } else if (shrinking_expanding_pools.count() > 0 &&
               OB_FAIL(inner_commit_alter_unit_num_(trans, tenant_id, shrinking_expanding_pools))) {
      LOG_WARN("failed to commit shrinking pools in revoking", KR(ret), K(tenant_id));
    }
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(notify_proxy.wait())) {
      LOG_WARN("fail to wait notify resource", KR(ret), K(tmp_ret));
      ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
    } else if (OB_SUCC(ret)) {
      // arg/dest/result can be used here.
    }
  }
  return ret;
}

int ObUnitManager::build_zone_sorted_unit_array_(const share::ObResourcePool *pool,
                                             common::ObArray<share::ObUnit*> &zone_sorted_units)
{
  int ret = OB_SUCCESS;
  ObArray<share::ObUnit*> *units;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K_(inited), K_(loaded), KR(ret));
  } else if (OB_ISNULL(pool)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("pool is nullptr", KR(ret));
  } else if (OB_FAIL(get_units_by_pool(pool->resource_pool_id_, units))) {
    LOG_WARN("fail to get units by pool", KR(ret));
  } else if (OB_ISNULL(units)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("units ptr is null", KR(ret));
  } else if (OB_FAIL(zone_sorted_units.assign(*units))) {
    LOG_WARN("fail to assign zone unit array", KR(ret));
  } else {
    UnitZoneOrderCmp cmp_operator;
    lib::ob_sort(zone_sorted_units.begin(), zone_sorted_units.end(), cmp_operator);
    // check unit count in each zone
    ObUnit *curr_unit = NULL;
    ObUnit *prev_unit = NULL;
    int64_t unit_count_per_zone = 0;
    for (int64_t j = 0; OB_SUCC(ret) && j < zone_sorted_units.count(); ++j) {
      prev_unit = curr_unit;
      curr_unit = zone_sorted_units.at(j);
      if (OB_ISNULL(curr_unit)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit ptr is null", KR(ret));
      } else if (0 == j || curr_unit->zone_ == prev_unit->zone_) {
        unit_count_per_zone += ObUnit::UNIT_STATUS_DELETING == curr_unit->status_ ? 0 : 1;
      } else if (unit_count_per_zone != pool->unit_count_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("zone unit num doesn't match resource pool's unit_count",
                 KR(ret), "zone", prev_unit->zone_, K(unit_count_per_zone), K(pool->unit_count_));
      } else {
        unit_count_per_zone = ObUnit::UNIT_STATUS_DELETING == curr_unit->status_ ? 0 : 1;
      }
    }
    if (OB_SUCC(ret) && unit_count_per_zone != pool->unit_count_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("zone unit num doesn't match resource pool's unit_count",
                KR(ret), "zone", curr_unit->zone_, K(unit_count_per_zone), K(pool->unit_count_));
    }
  }
  return ret;
}

// NOTE: this function is only used to commit part of shrinking pools of a tenant when these pools are revoked.
// in-memory info is not updated in this function, it's updated through unit_mgr_->load() by uplevel caller.
int ObUnitManager::inner_commit_alter_unit_num_(
  common::ObMySQLTransaction &trans, const uint64_t tenant_id, const common::ObArray<share::ObResourcePool *> &pools)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", KR(ret), K(loaded_), K(inited_));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (pools.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("pools is empty", KR(ret), K(pools));
  } else {
    // first check that pools are all owned by specified tenant
    FOREACH_CNT_X(pool, pools, OB_SUCCESS == ret) {
      if (OB_ISNULL(*pool)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pool is null pointer", KR(ret), KP(*pool));
      } else if (tenant_id != (*pool)->tenant_id_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pool is not owned by specified tenant", KR(ret), K(tenant_id), KPC(*pool));
      }
    }
    ObArray<uint64_t> adding_unit_ids;
    ObArray<ObArray<uint64_t>> deleting_unit_ids_group_by_pools;
    if (FAILEDx(commit_alter_unit_num_in_trans_(pools, trans,
                adding_unit_ids, deleting_unit_ids_group_by_pools))) {
      LOG_WARN("failed to shrink in trans", KR(ret), K(pools));
    } else if (OB_UNLIKELY(deleting_unit_ids_group_by_pools.count() != pools.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("deleting_unit_ids array count does not match pools count", KR(ret),
               K(deleting_unit_ids_group_by_pools), K(pools.count()));
    } else {/* good */}
  }
  return ret;
}

int ObUnitManager::get_zone_units(const ObArray<share::ObResourcePool *> &pools,
                                  ObArray<ZoneUnit> &zone_units) const
{
  int ret = OB_SUCCESS;
  ObArray<ObZone> zones;
  zone_units.reuse();
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K_(inited), K_(loaded), K(ret));
  } else if (pools.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("pools is empty", K(pools), K(ret));
  } else {
    FOREACH_CNT_X(pool, pools, OB_SUCCESS == ret) {
      if (NULL == *pool) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("pool is null", "pool", OB_P(*pool), K(ret));
      } else {
        FOREACH_CNT_X(pool_zone, (*pool)->zone_list_, OB_SUCCESS == ret) {
          bool find = false;
          FOREACH_CNT_X(zone, zones, !find) {
            if (*zone == *pool_zone) {
              find = true;
            }
          }
          if (!find) {
            if (OB_FAIL(zones.push_back(*pool_zone))) {
              LOG_WARN("push_back failed", K(ret));
            }
          }
        }
      }
    }

    ZoneUnit zone_unit;
    ObArray<ObUnitInfo> unit_infos;
    FOREACH_CNT_X(zone, zones, OB_SUCCESS == ret) {
      zone_unit.reset();
      zone_unit.zone_ = *zone;
      FOREACH_CNT_X(pool, pools, OB_SUCCESS == ret) {
        unit_infos.reuse();
        if (NULL == *pool) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("pool is null", "pool", OB_P(*pool), K(ret));
        } else if (OB_FAIL(inner_get_unit_infos_of_pool_((*pool)->resource_pool_id_, unit_infos))) {
          LOG_WARN("inner_get_unit_infos_of_pool failed",
              "pool id", (*pool)->resource_pool_id_, K(ret));
        } else {
          FOREACH_CNT_X(unit_info, unit_infos, OB_SUCCESS == ret) {
            if (unit_info->unit_.zone_ == *zone) {
              if (OB_FAIL(zone_unit.unit_infos_.push_back(*unit_info))) {
                LOG_WARN("push_back failed", K(ret));
              }
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(zone_units.push_back(zone_unit))) {
          LOG_WARN("push_back failed", K(zone_unit), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObUnitManager::get_tenants_of_server(const common::ObAddr &server,
    common::hash::ObHashSet<uint64_t> &tenant_id_set) const
{
  int ret = OB_SUCCESS;
  ObArray<ObUnitLoad> *unit_loads = NULL;
  {
    SpinRLockGuard guard(lock_);
    if (OB_FAIL(check_inner_stat_())) {
      LOG_WARN("check inner stat failed", K_(inited), K_(loaded), K(ret));
    } else if (!server.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("server is invalid", K(server), K(ret));
    } else if (OB_FAIL(get_loads_by_server(server, unit_loads))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("get_loads_by_server failed", K(server), K(ret));
      } else {
        ret = OB_SUCCESS;
        // just return empty set
      }
    } else if (NULL == unit_loads) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unit_loads is null", KP(unit_loads), K(ret));
    }
    if (OB_SUCC(ret) && !OB_ISNULL(unit_loads)) {
      FOREACH_CNT_X(unit_load, *unit_loads, OB_SUCCESS == ret) {
        if (!unit_load->is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid unit_load", "unit_load", *unit_load, K(ret));
        } else {
          const uint64_t tenant_id = unit_load->pool_->tenant_id_;
          if (!is_valid_tenant_id(tenant_id)) {
            //do nothing
          } else if (OB_FAIL(tenant_id_set.set_refactored(tenant_id))) {
            if (OB_HASH_EXIST == ret) {
              ret = OB_SUCCESS;
            } else {
              LOG_WARN("set tenant id failed", K(tenant_id), K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObUnitManager::check_tenant_on_server(const uint64_t tenant_id,
    const ObAddr &server, bool &on_server) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check inner stat failed", K_(inited), K_(loaded), K(ret));
  } else if (!is_valid_tenant_id(tenant_id) || !server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id or invalid server", K(tenant_id), K(server), K(ret));
  } else {
    ObArray<uint64_t> pool_ids;
    ObZone zone;
    ObArray<ObAddr> servers;
    if (OB_FAIL(get_pool_ids_of_tenant(tenant_id, pool_ids))) {
      LOG_WARN("get_pool_ids_of_tenant failed", K(tenant_id), K(ret));
    } else if (OB_FAIL(SVR_TRACER.get_server_zone(server, zone))) {
      LOG_WARN("get_server_zone failed", K(server), K(ret));
    } else {
      SpinRLockGuard guard(lock_);
      FOREACH_CNT_X(pool_id, pool_ids, OB_SUCCESS == ret && !on_server) {
        if (OB_FAIL(get_pool_servers(*pool_id, zone, servers))) {
          LOG_WARN("get_pool_servers failed", "pool_id", *pool_id, K(zone), K(ret));
        } else if (has_exist_in_array(servers, server)) {
          on_server = true;
        }
      }
    }
  }
  return ret;
}

int ObUnitManager::admin_migrate_unit(
    const uint64_t unit_id,
    const ObAddr &dst,
    bool is_cancel)
{
  int ret = OB_SUCCESS;
  ObUnitInfo unit_info;
  ObArray<ObAddr> excluded_servers;
  ObServerInfoInTable dst_server_info;
  obrpc::ObGetServerResourceInfoResult report_dst_server_resource_info;
  ObZone src_zone;
  ObZone dst_zone;
  double hard_limit = 0;
  bool can_hold_unit = false;
  SpinWLockGuard guard(lock_);
  AlterResourceErr err_index = ALT_ERR;
  const char *module = "ADMIN_MIGRATE_UNIT";
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check inner stat failed", K_(inited), K_(loaded), KR(ret));
  } else if (OB_INVALID_ID == unit_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unit id is invalid", K(unit_id), KR(ret));
  } else if (!dst.is_valid() && !is_cancel) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("dst is invalid", K(dst), KR(ret));
  } else if (OB_FAIL(get_hard_limit(hard_limit))) {
    LOG_WARN("get_hard_limit failed", KR(ret));
  } else if (OB_FAIL(inner_get_unit_info_by_id(unit_id, unit_info))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      LOG_USER_ERROR(OB_ENTRY_NOT_EXIST, "unit_id not existed");
    }
    LOG_WARN("get unit info failed", K(unit_id), KR(ret));
  } else if (ObUnit::UNIT_STATUS_DELETING == unit_info.unit_.status_) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("migrate a unit which is in deleting status", KR(ret), K(unit_id));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "migrate a unit which is in deleting status");
  } else if (dst == unit_info.unit_.server_) {
    // nothing need to do
    LOG_INFO("migration dst same to src", KR(ret), K(dst), K(unit_info));
  } else if (dst == unit_info.unit_.migrate_from_server_ || is_cancel) {
    // cancel migrate unit
    bool can_migrate_in = false;
    if (is_cancel && !unit_info.unit_.migrate_from_server_.is_valid()) {
	    ret = OB_ERR_UNEXPECTED;
	    LOG_WARN("failed to cancel migrate unit, may be no migrate task", KR(ret), K(unit_info));
      LOG_USER_ERROR(OB_ERR_UNEXPECTED,"no migrate task to cancel");
	  } else if (OB_FAIL(SVR_TRACER.check_server_can_migrate_in(
        unit_info.unit_.migrate_from_server_,
        can_migrate_in))) {
      LOG_WARN("fail to check server can_migrate_in", KR(ret), K(unit_info.unit_.migrate_from_server_));
    } else if (OB_FAIL(cancel_migrate_unit(
            unit_info.unit_, can_migrate_in))) {
		LOG_WARN("failed to cancel migrate unit", KR(ret), K(unit_info), K(can_migrate_in));
    }
  } else if (OB_FAIL(SVR_TRACER.get_server_zone(unit_info.unit_.server_, src_zone))) {
    LOG_WARN("get server zone failed", "server", unit_info.unit_.server_, KR(ret));
  } else if (OB_FAIL(SVR_TRACER.get_server_zone(dst, dst_zone))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      LOG_USER_ERROR(OB_ENTRY_NOT_EXIST, "destination server not found in the cluster");
    }
    LOG_WARN("get server zone failed", "server", dst, KR(ret));
  } else if (src_zone != dst_zone) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("migrate unit between zones is not supported", KR(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED,"migrate unit between zones");
  } else if (OB_FAIL(get_excluded_servers(unit_info.unit_.resource_pool_id_, unit_info.unit_.zone_,
      module, false/*new_allocate_pool*/, excluded_servers))) {
    LOG_WARN("get_excluded_servers failed", "unit", unit_info.unit_, KR(ret));
  } else if (has_exist_in_array(excluded_servers, dst)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED,"hold two units of a tenant in the same server");
    LOG_WARN("hold two units of a tenant in the same server is not supported", KR(ret));
  } else if (OB_FAIL(SVR_TRACER.get_server_info(dst, dst_server_info))) {
    LOG_WARN("get dst_server_info failed", KR(ret), K(dst));
  } else if (!dst_server_info.can_migrate_in()) {
    ret = OB_SERVER_MIGRATE_IN_DENIED;
    LOG_WARN("server can not migrate in", K(dst), K(dst_server_info), KR(ret));
  } else if (OB_FAIL(get_server_resource_info_via_rpc(dst_server_info, report_dst_server_resource_info))) {
    LOG_WARN("fail to execute get_server_resource_info_via_rpc", KR(ret), K(dst_server_info));
  } else if (OB_FAIL(have_enough_resource(
      report_dst_server_resource_info,
      unit_info.config_.unit_resource(),
      hard_limit,
      can_hold_unit,
      err_index))) {
    LOG_WARN("calculate_left_resource failed", KR(ret), K(report_dst_server_resource_info),
        K(hard_limit), K(err_index));
  } else if (!can_hold_unit) {
    ret = OB_MACHINE_RESOURCE_NOT_ENOUGH;
    if (OB_SUCCESS != ERRSIM_USE_DUMMY_SERVER) {
      LOG_USER_ERROR(OB_MACHINE_RESOURCE_NOT_ENOUGH, "dummy_zone", "127.0.0.1:1000",
          alter_resource_err_to_str(err_index));
    } else {
      ObCStringHelper helper;
      LOG_USER_ERROR(OB_MACHINE_RESOURCE_NOT_ENOUGH, helper.convert(dst_zone), helper.convert(dst),
          alter_resource_err_to_str(err_index));
    }
    LOG_WARN("left resource can't hold unit", "server", dst,
        K(hard_limit), "config", unit_info.config_, KR(ret));
  } else if (OB_FAIL(migrate_unit_(unit_id, dst, true/*is_manual*/))) {
    LOG_WARN("migrate unit failed", K(unit_id), "destination", dst, KR(ret));
  }

  return ret;
}

int ObUnitManager::cancel_migrate_unit(
    const share::ObUnit &unit,
    const bool migrate_from_server_can_migrate_in)
{
  int ret = OB_SUCCESS;
  if (!migrate_from_server_can_migrate_in) {
    ret = OB_SERVER_MIGRATE_IN_DENIED;
    LOG_WARN("server can not migrate in", K(unit.migrate_from_server_), K(migrate_from_server_can_migrate_in), KR(ret));
  } else {
    const EndMigrateOp op = REVERSE;
    if (OB_FAIL(end_migrate_unit(unit.unit_id_, op))) {
      LOG_WARN("end_migrate_unit failed", "unit_id", unit.unit_id_, K(op), K(ret));
    } else {
      LOG_INFO("cancel migrate unit", K(unit));
    }
  }
  return ret;
}

int ObUnitManager::try_cancel_migrate_unit(const share::ObUnit &unit, bool &is_canceled)
{
  int ret = OB_SUCCESS;
  bool migrate_from_server_can_migrate_in = false;
  bool server_can_migrate_in = false;
  is_canceled = false;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check inner stat failed", K_(inited), K_(loaded), K(ret));
  } else if (!unit.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid unit", K(unit), K(ret));
  } else if (OB_FAIL(SVR_TRACER.check_server_can_migrate_in(unit.server_, server_can_migrate_in))) {
    LOG_WARN("check_server_can_migrate_in failed", "server", unit.server_, K(ret));
  } else if (server_can_migrate_in) {
    // ignore, do nothing
  } else if (OB_FAIL(SVR_TRACER.check_server_can_migrate_in(
      unit.migrate_from_server_,
      migrate_from_server_can_migrate_in))) {
    LOG_WARN("get_server_status failed", "server", unit.migrate_from_server_, K(ret));
  } else if (migrate_from_server_can_migrate_in) {
    LOG_INFO("unit migrate_from_server can migrate in, "
        "migrate unit back to migrate_from_server", K(unit), K(migrate_from_server_can_migrate_in));
    const EndMigrateOp op = REVERSE;
    if (OB_FAIL(end_migrate_unit(unit.unit_id_, op))) {
      LOG_WARN("end_migrate_unit failed", "unit_id", unit.unit_id_, K(op), K(ret));
    } else {
      is_canceled = true;
      LOG_INFO("reverse unit migrate success", K(ret), "unit_id", unit.unit_id_, K(op));
    }
  }
  return ret;
}

int ObUnitManager::get_hard_limit(double &hard_limit) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check inner stat failed", K_(inited), K_(loaded), K(ret));
  } else {
    hard_limit = static_cast<double>(server_config_->resource_hard_limit) / 100;
  }
  return ret;
}

// bug#11873101 issue/11873101
// Before attempting to migrate the unit,
// check whether the target unit space is sufficient,
// if it is insufficient, do not migrate,
// and return OB_OP_NOT_ALLOW
int ObUnitManager::try_migrate_unit(const uint64_t unit_id,
                                    const uint64_t tenant_id,
                                    const ObUnitStat &unit_stat,
                                    const ObIArray<ObUnitStat> &migrating_unit_stat,
                                    const ObAddr &dst,
                                    const ObServerResourceInfo &dst_resource_info,
                                    const bool is_manual)
{
  int ret = OB_SUCCESS;
  ObServerStatus server_status;
  if (unit_id != unit_stat.get_unit_id()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid unit stat", K(unit_id), K(unit_stat), K(ret));
  } else {
    int64_t mig_required_size = 0;
    for (int64_t i = 0; i < migrating_unit_stat.count(); ++i) {
      mig_required_size +=  migrating_unit_stat.at(i).get_required_size();
    }
    // sstable Space constraints
    int64_t required_size =
        mig_required_size + unit_stat.get_required_size() + dst_resource_info.disk_in_use_;
    int64_t total_size = dst_resource_info.disk_total_;
    int64_t required_percent = (100 * required_size) / total_size;
    int64_t limit_percent = GCONF.data_disk_usage_limit_percentage;
    if (required_percent >= limit_percent) {
      ret = OB_OP_NOT_ALLOW;
      LOG_ERROR("migrate unit fail. dest server out of space",
                K(unit_id), K(unit_stat), K(dst),
                K(required_size), K(total_size), K(limit_percent), K(ret));
    }

    if (FAILEDx(migrate_unit_(unit_id, dst, is_manual))) {
      LOG_WARN("fail migrate unit", K(unit_id), K(dst), K(ret));
    }
  }
  return ret;
}

int ObUnitManager::migrate_unit_(const uint64_t unit_id, const ObAddr &dst, const bool is_manual)
{
  int ret = OB_SUCCESS;
  LOG_INFO("start to migrate unit", KR(ret), K(unit_id), K(dst), K(is_manual));
  ObUnit *unit = NULL;
  share::ObResourcePool *pool = NULL;
  ObZone zone;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(inited_), K(loaded_), K(ret));
  } else if (OB_INVALID_ID == unit_id || !dst.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(unit_id), K(dst), K(ret));
  } else if (OB_FAIL(get_unit_by_id(unit_id, unit))) {
    LOG_WARN("get_unit_by_id failed", K(unit_id), K(ret));
  } else if (OB_ISNULL(unit)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unit is null", KP(unit), K(ret));
  } else if (ObUnit::UNIT_STATUS_DELETING == unit->status_) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("cannot migrate unit which is in deleting", K(ret), K(unit_id));
  } else if (unit->server_ == dst) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unit->server same as migrate destination server",
        "unit", *unit, K(dst), K(ret));
  } else if (OB_FAIL(SVR_TRACER.get_server_zone(dst, zone))) {
    LOG_WARN("get_server_zone failed", KR(ret), K(dst));
  } else if (OB_UNLIKELY(zone != unit->zone_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("migrate unit between zones not supported", KR(ret), KP(unit), K(dst), K(zone));
  } else if (unit->migrate_from_server_.is_valid()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unit is already migrating, cannot migrate any more", "unit", *unit, K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "migrate unit already in migrating status");
  } else if (OB_FAIL(get_resource_pool_by_id(unit->resource_pool_id_, pool))) {
    LOG_WARN("get_resource_pool_by_id failed",
        "resource pool id", unit->resource_pool_id_, K(ret));
  } else if (OB_ISNULL(pool)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pool is null", KP(pool), K(ret));
  } else {
    const bool granted = pool->is_granted_to_tenant();
    const ObAddr src = unit->server_;
    ObUnit new_unit = *unit;
    new_unit.server_ = dst;
    new_unit.migrate_from_server_ = granted ? src : ObAddr();
    new_unit.is_manual_migrate_ = is_manual;

    LOG_INFO("do migrate unit", KPC(unit), K(new_unit), KPC(pool), K(granted));

    // STEP 1: try notify unit persistence on destination ObServer
    if (OB_FAIL(do_migrate_unit_notify_resource_(*pool, new_unit, is_manual, granted))) {
      LOG_WARN("do_migrate_unit_notify_resource failed", KR(ret), KPC(pool), K(new_unit), K(is_manual), K(granted));
    }

    // STEP 2: Update info in inner_table in trans
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(do_migrate_unit_in_trans_(*pool, new_unit, is_manual, granted))) {
      LOG_WARN("do_migrate_unit_in_trans failed", KR(ret), KPC(pool), K(new_unit), K(is_manual), K(granted));
    }

    // STEP 3: Update in-memory info (unit & unit_load & migrate_unit)
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(do_migrate_unit_inmemory_(new_unit, unit, is_manual, granted))) {
      LOG_WARN("do_migrate_unit_inmemory failed", KR(ret), K(dst), KPC(unit), K(is_manual), K(granted));
    }

    // STEP 4: migration succeed, do some postprocess
    if (OB_SUCC(ret)) {
      // wakeup rootbalance thread to make disaster_recovery process more quickly
      root_balance_->wakeup();
      // add migrate_unit rootservice event
      ROOTSERVICE_EVENT_ADD("unit", "migrate_unit",
          "unit_id", unit->unit_id_,
          "migrate_from_server", unit->migrate_from_server_,
          "server", unit->server_,
          "tenant_id", pool->tenant_id_);
    }
  }
  LOG_INFO("finish migrate unit", KR(ret), K(unit_id), K(dst), K(is_manual));
  return ret;
}

int ObUnitManager::do_migrate_unit_notify_resource_(const share::ObResourcePool &pool,
                                                    const share::ObUnit &new_unit,
                                                    const bool is_manual,
                                                    const bool granted)
{
  int ret = OB_SUCCESS;
  lib::Worker::CompatMode compat_mode = lib::Worker::CompatMode::INVALID;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(inited_), K(loaded_), K(ret));
  } else if (!granted) {
    // do nothing. If unit is not granted, there's no need to notify observer.
  } else if (OB_UNLIKELY(nullptr == srv_rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("srv_rpc_proxy_ ptr is null", KR(ret));
  } else if (OB_UNLIKELY(new_unit.resource_pool_id_ != pool.resource_pool_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("new_unit not belong to pool", KR(ret), K(pool), K(new_unit));
  } else if (OB_FAIL(ObCompatModeGetter::get_tenant_mode(pool.tenant_id_, compat_mode))) {
    LOG_WARN("fail to get tenant compat mode", KR(ret), K(pool));
  } else {
    ObNotifyTenantServerResourceProxy notify_proxy(*srv_rpc_proxy_,
                                                  &obrpc::ObSrvRpcProxy::notify_tenant_server_unit_resource);
    // only notify new unit resource on dst server here.
    // Old unit on src server will be delete later when doing end_migrate
    if (OB_FAIL(try_notify_tenant_server_unit_resource_(
            pool.tenant_id_, false/*is_delete*/, notify_proxy, // is_delete is false when migrate unit
            pool.unit_config_id_, compat_mode, new_unit, false/*if not grant*/,
            false/*skip offline server*/,
            true /*check_data_version*/))) {
      LOG_WARN("fail to try notify server unit resource", K(ret));
    }
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(notify_proxy.wait())) {
      LOG_WARN("fail to wait notify resource", K(ret), K(tmp_ret));
      ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
    } else if (OB_SUCC(ret)) {
      // arg/dest/result can be used here.
    }

    // Rollback persistent unit if persistence failed
    ret = ERRSIM_UNIT_PERSISTENCE_ERROR ? : ret;
    if (OB_FAIL(ret)) {
      LOG_WARN("start to rollback unit persistence", KR(ret), K(new_unit), K(pool.tenant_id_));
      int tmp_ret = OB_SUCCESS;
      ObArray<ObUnit> units;
      if (OB_TMP_FAIL(units.push_back(new_unit))) {
        LOG_WARN("fail to push an element into units", KR(ret), KR(tmp_ret), K(new_unit));
      } else if (OB_TMP_FAIL(rollback_persistent_units_(pool.tenant_id_, units, notify_proxy))) {
        LOG_WARN("fail to rollback unit persistence", KR(ret), KR(tmp_ret),
            K(units), K(pool), K(compat_mode));
      }
    }
  }
  return ret;
}

int ObUnitManager::do_migrate_unit_in_trans_(const share::ObResourcePool &pool,
                                             const share::ObUnit &new_unit,
                                             const bool is_manual,
                                             const bool granted)
{
  int ret = OB_SUCCESS;
  share::ObUnitTableTransaction trans;
  share::ObResourcePool real_pool;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(inited_), K(loaded_), K(ret));
  } else if (OB_FAIL(trans.start(proxy_, OB_SYS_TENANT_ID))) {
    LOG_WARN("failed to start trans", K(ret));
  }
  // Double check whether exactly unit is granted by querying pool from inner_table.
  // Because during creating or dropping tenant, there's a period when inner_table
  //     and persistence units are updated while in-memory info not updated yet.
  // We need lock by SELECT FOR UPDATE to assure no other trans is still updating pool when checking.
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ut_operator_.get_resource_pool(trans,
                                                    pool.resource_pool_id_,
                                                    true/*select_for_update*/,
                                                    real_pool))) {
    LOG_WARN("fail to get resource_pools from table", KR(ret));
  } else {
    if (pool.is_granted_to_tenant() != real_pool.is_granted_to_tenant()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("in-memory pool info not accurate, cannot migrate unit now.",
                KR(ret), K(new_unit), K(pool), K(real_pool));
    }
  }
  // Create migrate RS_JOB if it's a manual migration:
  // * If not granted, the job will be set as complete on creating.
  // * If granted, the job will be completed when end_migrate_unit is called.
  if (OB_FAIL(ret)) {
  } else if (is_manual) {
    char ip_buf[common::MAX_IP_ADDR_LENGTH] = {"\0"};
    const ObAddr &dst = new_unit.server_;
    (void)dst.ip_to_string(ip_buf, common::MAX_IP_ADDR_LENGTH);
    int64_t job_id = 0;
    if (OB_FAIL(RS_JOB_CREATE_WITH_RET(
        job_id,
        ObRsJobType::JOB_TYPE_MIGRATE_UNIT,
        trans,
        "unit_id", new_unit.unit_id_,
        "svr_ip", ip_buf,
        "svr_port", dst.get_port(),
        "tenant_id", pool.tenant_id_))) {
      LOG_WARN("fail to create rs job MIGRATE_UNIT", KR(ret),
               "tenant_id", pool.tenant_id_,
               "unit_id", new_unit.unit_id_);
    } else if (!granted) {
      // not granted, migration can be done at once, so mark RS_JOB completed
      if (OB_FAIL(RS_JOB_COMPLETE(job_id, OB_SUCCESS, trans))) {
        LOG_WARN("all_rootservice_job update failed", K(ret), K(job_id));
      }
    }
  }
  // Update unit info
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ut_operator_.update_unit_exclude_ug_id(trans, new_unit))) {
    LOG_WARN("update_unit ignore ug_id failed", K(new_unit), K(ret));
  }
  // End this transaction
  DEBUG_SYNC(BEFORE_MIGRATE_FINISH);
  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
      ret = OB_SUCC(ret) ? tmp_ret : ret;
      LOG_WARN("trans commit failed", K(tmp_ret), K(ret));
    }
  }
  return ret;
}

int ObUnitManager::do_migrate_unit_inmemory_(const share::ObUnit &new_unit,
                                             share::ObUnit *unit,
                                             const bool is_manual,
                                             const bool granted)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(inited_), K(loaded_), K(ret));
  } else if (OB_ISNULL(unit)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unit ptr is null", KR(ret), KP(unit));
  } else if (OB_UNLIKELY(unit->unit_id_ != new_unit.unit_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("old and new unit id not the same", KR(ret), KPC(unit), K(new_unit));
  } else {
    const ObAddr src = unit->server_;
    const ObAddr dst = new_unit.server_;
    // do update unit
    *unit = new_unit;
    // update unit_load
    //    delete old unit load imediately if pool not granted
    if (!granted) {
      if (OB_FAIL(delete_unit_load(src, unit->unit_id_))) {
        LOG_WARN("delete_unit_load failed", K(src), "unit_id", unit->unit_id_, KR(ret));
      }
    }
    //    insert new unit load
    ObUnitLoad load;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(gen_unit_load(unit, load))) {
      LOG_WARN("gen_unit_load failed", "unit", *unit, K(ret));
    } else if (OB_FAIL(insert_unit_load(dst, load))) {
      LOG_WARN("insert_unit_load failed", K(dst), K(ret));
    }
    // update migrate_unit list
    if (OB_FAIL(ret)) {
    } else if (granted) {
      if (OB_FAIL(insert_migrate_unit(unit->migrate_from_server_, unit->unit_id_))) {
        LOG_WARN("insert_migrate_unit failed", "unit", *unit, K(ret));
      }
    }
  }
  return ret;
}

int ObUnitManager::inner_try_delete_migrate_unit_resource(
    const uint64_t unit_id,
    const common::ObAddr &migrate_from_server)
{
  int ret = OB_SUCCESS;
  ObUnit *unit = NULL;
  share::ObResourcePool *pool = NULL;
  share::ObUnitConfig *unit_config = nullptr;
  lib::Worker::CompatMode compat_mode = lib::Worker::CompatMode::INVALID;
  bool is_alive = false;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("inner stat unexpected", K(ret), K(inited_), K(loaded_));
  } else if (OB_UNLIKELY(OB_INVALID_ID == unit_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(unit_id));
  } else if (OB_FAIL(get_unit_by_id(unit_id, unit))) {
    LOG_WARN("fail to get unit by id", K(ret), K(unit_id));
  } else if (OB_ISNULL(unit)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unit ptr is null", K(ret), KP(unit));
  } else if (!migrate_from_server.is_valid()) {
    LOG_INFO("unit not in migrating, no need to delete src resource", K(unit_id));
  } else if (OB_FAIL(SVR_TRACER.check_server_alive(migrate_from_server, is_alive))) {
    LOG_WARN("fail to check server alive", K(ret), "server", migrate_from_server);
  } else if (!is_alive) {
    LOG_INFO("src server not alive, ignore notify",
             K(unit_id), "server", migrate_from_server);
  } else if (OB_FAIL(get_resource_pool_by_id(unit->resource_pool_id_, pool))) {
    LOG_WARN("failed to get pool", K(ret), K(unit));
  } else if (OB_ISNULL(pool)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pool ptr is null", K(ret), KP(pool));
  } else if (OB_FAIL(get_unit_config_by_id(pool->unit_config_id_, unit_config))) {
    LOG_WARN("fail to get unit config by id", K(ret));
  } else if (OB_UNLIKELY(nullptr == unit_config)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unit config is null", K(ret), "unit_config_id", pool->unit_config_id_);
  } else if (!pool->is_granted_to_tenant()) {
    LOG_INFO("unit is not granted to any tenant", K(ret), "tenant_id", pool->tenant_id_);
  } else if (OB_FAIL(ObCompatModeGetter::get_tenant_mode(pool->tenant_id_, compat_mode))) {
    LOG_WARN("fail to get tenant compat mode", K(ret),
             "tenant_id", pool->tenant_id_, K(unit_id), "pool", *pool);
  } else {
    const int64_t rpc_timeout = NOTIFY_RESOURCE_RPC_TIMEOUT;
    obrpc::TenantServerUnitConfig tenant_unit_server_config;
    ObNotifyTenantServerResourceProxy notify_proxy(
                                      *srv_rpc_proxy_,
                                      &obrpc::ObSrvRpcProxy::notify_tenant_server_unit_resource);
    if (OB_FAIL(tenant_unit_server_config.init_for_dropping(pool->tenant_id_, true/*delete*/))) {
      LOG_WARN("fail to init tenant server unit config", K(ret), "tenant_id", pool->tenant_id_);
    } else if (OB_FAIL(notify_proxy.call(
            migrate_from_server, rpc_timeout, tenant_unit_server_config))) {
      LOG_WARN("fail to call notify resource to server",
               K(ret), K(rpc_timeout), "unit", *unit, "dest", migrate_from_server);
    }
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(notify_proxy.wait())) {
      LOG_WARN("fail to wait notify resource", K(ret), K(tmp_ret));
      ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
    }
    if (OB_SUCC(ret)) {
      // arg/dest/result can be used here.
      LOG_INFO("notify resource to server succeed", "unit", *unit, "dest", migrate_from_server);
    }
  }
  return ret;
}

int ObUnitManager::end_migrate_unit(const uint64_t unit_id, const EndMigrateOp end_migrate_op)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = OB_INVALID_ID;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(inited_), K(loaded_), K(ret));
  } else if (OB_INVALID_ID == unit_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(unit_id), K(ret));
  } else if (end_migrate_op < COMMIT || end_migrate_op > REVERSE) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid end_migrate_op", K(end_migrate_op), K(ret));
  } else {
    ObUnit *unit = NULL;
    share::ObUnitTableTransaction trans;
    if (OB_FAIL(get_unit_by_id(unit_id, unit))) {
      LOG_WARN("get_unit_by_id failed", K(unit_id), K(ret));
    } else if (NULL == unit) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unit is null", KP(unit), K(ret));
    } else if (!unit->migrate_from_server_.is_valid()) {
      // FIXME(jingqian): when can this happened, figure it out
      ret = OB_SUCCESS;
      LOG_WARN("unit is not in migrating status, maybe end_migrate_unit has ever called",
               "unit", *unit, K(ret));
    } else {
      const ObAddr migrate_from_server = unit->migrate_from_server_;
      const ObAddr unit_server = unit->server_;
      ObUnit new_unit = *unit;
      new_unit.is_manual_migrate_ = false;  // clear manual_migrate
      // generate new unit
      if (COMMIT == end_migrate_op) {
        new_unit.migrate_from_server_.reset();
      } else if (ABORT == end_migrate_op) {
        new_unit.server_ = unit->migrate_from_server_;
        new_unit.migrate_from_server_.reset();
      } else {
        new_unit.server_ = unit->migrate_from_server_;
        new_unit.migrate_from_server_ = unit->server_;
      }

      // update unit in sys_table and in memory
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(trans.start(proxy_, OB_SYS_TENANT_ID))) {
        LOG_WARN("failed to start transaction ", K(ret));
      } else if (OB_FAIL(ut_operator_.update_unit_exclude_ug_id(trans, new_unit))) {
        LOG_WARN("ut_operator update unit failed", K(new_unit), K(ret));
      } else {
        if (ABORT == end_migrate_op) {
          if (OB_FAIL(delete_unit_load(unit->server_, unit->unit_id_))) {
            LOG_WARN("delete_unit_load failed", "unit", *unit, K(ret));
          }
        } else if (COMMIT == end_migrate_op) {
          if (OB_FAIL(delete_unit_load(unit->migrate_from_server_, unit->unit_id_))) {
            LOG_WARN("delete_unit_load failed", "unit", *unit, K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          *unit = new_unit;
        }
      }

      // delete migrating unit from migrate_units of migrate_from_server,
      // if REVERSE == op, add migrating unit to migrate_units of unit_server
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(delete_migrate_unit(migrate_from_server, unit->unit_id_))) {
        LOG_WARN("delete_migrate_unit failed", K(migrate_from_server),
                 "unit_id", unit->unit_id_, K(ret));
      } else if (REVERSE == end_migrate_op) {
        if (OB_FAIL(insert_migrate_unit(unit_server, unit->unit_id_))) {
          LOG_WARN("insert_migrate_unit failed", K(unit_server), "unit_id",
                   unit->unit_id_, K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        int tmp_ret = OB_SUCCESS;
        share::ObResourcePool *pool = NULL;
        if (OB_SUCCESS != (tmp_ret = get_resource_pool_by_id(unit->resource_pool_id_, pool))) {
          LOG_WARN("failed to get pool", K(tmp_ret), K(unit));
        } else {
          tenant_id = pool->tenant_id_;
        }
        ROOTSERVICE_EVENT_ADD("unit", "finish_migrate_unit",
                              "unit_id", unit_id,
                              "end_op", end_migrate_op,
                              "migrate_from_server", migrate_from_server,
                              "server", unit_server,
                              "tenant_id", tenant_id);

        // complete the job if exists
        char ip_buf[common::MAX_IP_ADDR_LENGTH] = {"\0"};
        (void)unit_server.ip_to_string(ip_buf, common::MAX_IP_ADDR_LENGTH);
        int64_t job_id = 0;
        tmp_ret = RS_JOB_FIND(MIGRATE_UNIT, job_id, trans, "unit_id", unit_id,
                              "svr_ip", ip_buf, "svr_port", unit_server.get_port());
        if (OB_SUCCESS == tmp_ret && job_id > 0) {
          tmp_ret = (end_migrate_op == COMMIT) ? OB_SUCCESS :
              (end_migrate_op == REVERSE ? OB_CANCELED : OB_TIMEOUT);
          if (OB_FAIL(RS_JOB_COMPLETE(job_id, tmp_ret, trans))) {
            LOG_WARN("all_rootservice_job update failed", K(ret), K(job_id));
          }
        } else {
          //Can not find the situation, only the user manually opened will write rs_job
          LOG_WARN("no rs job", K(ret), K(tmp_ret), K(unit_id));
        }
      }
      const bool commit = OB_SUCC(ret) ? true:false;
      int tmp_ret = OB_SUCCESS ;
      if (OB_SUCCESS != (tmp_ret = trans.end(commit))) {
        LOG_WARN("tran commit failed", K(tmp_ret));
      }
      ret = OB_SUCC(ret) ? tmp_ret : ret;

      if (OB_SUCC(ret) && COMMIT == end_migrate_op && OB_INVALID_ID != tenant_id) {
        (void)inner_try_delete_migrate_unit_resource(unit_id, migrate_from_server);
      }
    }
  }

  LOG_INFO("end migrate unit", K(unit_id), K(end_migrate_op), K(ret));
  return ret;
}

#define INSERT_ITEM_TO_MAP(map, key, pvalue) \
  do { \
    if (OB_FAIL(ret)) { \
    } else if (OB_FAIL(map.set_refactored(key, pvalue))) { \
      if (OB_HASH_EXIST == ret) { \
        LOG_WARN("key already exist", K(key), K(ret)); \
      } else { \
        LOG_WARN("map set failed", K(ret)); \
      } \
    } else { \
    } \
  } while (false)

#define SET_ITEM_TO_MAP(map, key, value) \
  do { \
    const int overwrite = 1; \
    if (OB_FAIL(ret)) { \
    } else if (OB_FAIL(map.set_refactored(key, value, overwrite))) { \
      LOG_WARN("map set failed", K(ret)); \
    } else { \
    } \
  } while (false)

#define INSERT_ARRAY_TO_MAP(map, key, array) \
  do { \
    if (OB_FAIL(ret)) { \
    } else if (OB_FAIL(map.set_refactored(key, array))) { \
      if (OB_HASH_EXIST == ret) { \
        LOG_WARN("key already exist", K(key), K(ret)); \
      } else { \
        LOG_WARN("map set failed", K(ret)); \
      } \
    } else { \
    } \
  } while (false)

int ObUnitManager::build_unit_map(const ObIArray<ObUnit> &units)
{
  int ret = OB_SUCCESS;
  // units is empty if invoked during bootstrap
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(id_unit_map_.clear())) {
    LOG_WARN("id_unit_map_ clear failed", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < units.count(); ++i) {
      ObUnit *unit = NULL;
      if (NULL == (unit = allocator_.alloc())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("alloc unit failed", K(ret));
      } else {
        *unit = units.at(i);
        if (OB_FAIL(insert_unit(unit))) {
          LOG_WARN("insert_unit failed", "unit", *unit, K(ret));
        }

        if (OB_FAIL(ret)) {
          //avoid memory leak
          allocator_.free(unit);
          unit = NULL;
        }
      }
    }
  }
  return ret;
}

int ObUnitManager::build_config_map(const ObIArray<ObUnitConfig> &configs)
{
  int ret = OB_SUCCESS;
  // configs is empty if invoked during bootstrap
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(id_config_map_.clear())) {
    LOG_WARN("id_config_map_ clear failed", K(ret));
  } else if (OB_FAIL(name_config_map_.clear())) {
    LOG_WARN("name_config_map_ clear failed", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < configs.count(); ++i) {
      ObUnitConfig *config = NULL;
      if (NULL == (config = config_allocator_.alloc())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("alloc unit config failed", K(ret));
      } else {
        *config = configs.at(i);
        if (OB_FAIL(insert_unit_config(config))) {
          LOG_WARN("insert_unit_config failed", KP(config), K(ret));
        }

        if (OB_FAIL(ret)) {
          config_allocator_.free(config);
          config = NULL;
        }
      }
    }
  }
  return ret;
}

int ObUnitManager::build_pool_map(const ObIArray<share::ObResourcePool> &pools)
{
  int ret = OB_SUCCESS;
  // pools is empty if invoked during bootstrap
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(id_pool_map_.clear())) {
    LOG_WARN("id_pool_map_ clear failed", K(ret));
  } else if (OB_FAIL(name_pool_map_.clear())) {
    LOG_WARN("name_pool_map_ clear failed", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < pools.count(); ++i) {
      share::ObResourcePool *pool = NULL;
      if (NULL == (pool = pool_allocator_.alloc())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("alloc resource pool failed", K(ret));
      } else {
        if (OB_FAIL(pool->assign(pools.at(i)))) {
          LOG_WARN("failed to assign pool", K(ret));
        } else {
          INSERT_ITEM_TO_MAP(id_pool_map_, pool->resource_pool_id_, pool);
          INSERT_ITEM_TO_MAP(name_pool_map_, pool->name_, pool);
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(inc_config_ref_count(pool->unit_config_id_))) {
            LOG_WARN("inc_config_ref_count failed", "config id", pool->unit_config_id_, K(ret));
          } else if (OB_FAIL(insert_config_pool(pool->unit_config_id_, pool))) {
            LOG_WARN("insert config pool failed", "config id", pool->unit_config_id_, K(ret));
          }
        }

        if (OB_FAIL(ret)) {
          pool_allocator_.free(pool);
          pool = NULL;
        }
      }
    }
  }
  return ret;
}

int ObUnitManager::insert_unit_config(ObUnitConfig *config)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (NULL == config) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("config is null", KP(config), K(ret));
  } else if (!config->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid config", "config", *config, K(ret));
  } else {
    INSERT_ITEM_TO_MAP(id_config_map_, config->unit_config_id(), config);
    INSERT_ITEM_TO_MAP(name_config_map_, config->name(), config);
    int64_t ref_count = 0;
    SET_ITEM_TO_MAP(config_ref_count_map_, config->unit_config_id(), ref_count);
  }
  return ret;
}

int ObUnitManager::inc_config_ref_count(const uint64_t config_id)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == config_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid config_id", K(config_id), K(ret));
  } else {
    int64_t ref_count = 0;
    if (OB_FAIL(get_config_ref_count(config_id, ref_count))) {
      LOG_WARN("get_config_ref_count failed", K(config_id), K(ret));
    } else {
      ++ref_count;
      SET_ITEM_TO_MAP(config_ref_count_map_, config_id, ref_count);
    }
  }
  return ret;
}

int ObUnitManager::dec_config_ref_count(const uint64_t config_id)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == config_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid config_id", K(config_id), K(ret));
  } else {
    int64_t ref_count = 0;
    if (OB_FAIL(get_config_ref_count(config_id, ref_count))) {
      LOG_WARN("get_config_ref_count failed", K(config_id), K(ret));
    } else {
      --ref_count;
      SET_ITEM_TO_MAP(config_ref_count_map_, config_id, ref_count);
    }
  }
  return ret;
}

int ObUnitManager::update_pool_map(share::ObResourcePool *resource_pool)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (NULL == resource_pool) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("resource_pool is null", KP(resource_pool), K(ret));
  } else if (!resource_pool->is_valid() || OB_INVALID_ID == resource_pool->resource_pool_id_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid resource_pool", "resource_pool", *resource_pool, K(ret));
  } else {
    INSERT_ITEM_TO_MAP(id_pool_map_, resource_pool->resource_pool_id_, resource_pool);
    INSERT_ITEM_TO_MAP(name_pool_map_, resource_pool->name_, resource_pool);
  }
  return ret;
}

int ObUnitManager::insert_unit(ObUnit *unit)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_INFO("not init", K(ret));
  } else if (NULL == unit) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unit is null", KP(unit), K(ret));
  } else if (!unit->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid unit", "unit", *unit, K(ret));
  } else {
    ObArray<ObUnit *> *units = NULL;
    if (OB_FAIL(get_units_by_pool(unit->resource_pool_id_, units))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("get_units_by_pool failed", K(ret));
      } else {
        ret = OB_SUCCESS;
        if (NULL == (units = pool_unit_allocator_.alloc())) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_ERROR("alloc ObArray<ObUnit *> failed", K(ret));
        } else {
          if (OB_FAIL(units->push_back(unit))) {
            LOG_WARN("push_back failed", K(ret));
          } else {
            INSERT_ARRAY_TO_MAP(pool_unit_map_, unit->resource_pool_id_, units);
          }
          if (OB_FAIL(ret)) {
            pool_unit_allocator_.free(units);
            units = NULL;
          }
        }
      }
    } else if (NULL == units) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("units is null", KP(units), K(ret));
    } else if (OB_FAIL(units->push_back(unit))) {
      LOG_WARN("push_back failed", K(ret));
    }

    if (OB_SUCC(ret)) {
      INSERT_ITEM_TO_MAP(id_unit_map_, unit->unit_id_, unit);

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(insert_unit_loads(unit))) {
        LOG_WARN("insert_unit_loads failed", "unit", *unit, K(ret));
      }
    }
  }
  return ret;
}

int ObUnitManager::insert_unit_loads(ObUnit *unit)
{
  int ret = OB_SUCCESS;
  ObUnitLoad load;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (NULL == unit) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unit is null", KP(unit), K(ret));
  } else if (!unit->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid unit", "unit", *unit, K(ret));
  } else if (OB_FAIL(gen_unit_load(unit, load))) {
    LOG_WARN("gen_unit_load failed", "unit", *unit, K(ret));
  } else if (OB_FAIL(insert_unit_load(unit->server_, load))) {
    LOG_WARN("insert_unit_load failed", "server", unit->server_, K(ret));
  } else {
    if (OB_SUCCESS == ret && unit->migrate_from_server_.is_valid()) {
      if (OB_FAIL(insert_unit_load(unit->migrate_from_server_, load))) {
        LOG_WARN("insert_unit_load failed", "server", unit->migrate_from_server_, K(ret));
      }
    }
  }
  return ret;
}

int ObUnitManager::insert_unit_load(const ObAddr &server, const ObUnitLoad &load)
{
  int ret = OB_SUCCESS;
  ObArray<ObUnitLoad> *loads = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!server.is_valid() || !load.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(server), K(load), K(ret));
  } else if (OB_FAIL(get_loads_by_server(server, loads))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("get_loads_by_server failed", K(server), K(ret));
    } else {
      ret = OB_SUCCESS;
      // not exist, alloc new array, add to hash map
      if (NULL == (loads = load_allocator_.alloc())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("alloc ObArray<ObUnitLoad> failed", K(ret));
      } else {
        if (OB_FAIL(loads->push_back(load))) {
          LOG_WARN("push_back failed", K(ret));
        } else if (OB_FAIL(insert_load_array(server, loads))) {
          LOG_WARN("insert_unit_load failed", K(server), K(ret));
        }
        if (OB_FAIL(ret)) {
          // avoid memory leak
          load_allocator_.free(loads);
          loads = NULL;
        }
      }
    }
  } else if (NULL == loads) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("loads is null", KP(loads), K(ret));
  } else {
    if (OB_FAIL(loads->push_back(load))) {
      LOG_WARN("push_back failed", K(ret));
    }
  }
  return ret;
}

int ObUnitManager::insert_load_array(const ObAddr &addr, ObArray<ObUnitLoad> *loads)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!addr.is_valid() || NULL == loads) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(addr), KP(loads), K(ret));
  } else if (loads->count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("loads is empty", "loads count", loads->count(), K(ret));
  } else {
    if (OB_FAIL(server_loads_.set_refactored(addr, loads))) {
      if (OB_HASH_EXIST == ret) {
        LOG_WARN("load array is not expect to exist", K(addr), K(ret));
      } else {
        LOG_WARN("set failed", K(addr), K(ret));
      }
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObUnitManager::update_pool_load(share::ObResourcePool *pool,
    share::ObUnitConfig *new_config)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (NULL == pool || NULL == new_config) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(pool), KP(new_config));
  } else {
    FOREACH_X(sl, server_loads_, OB_SUCC(ret)) {
      if (NULL == sl->second) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL value", K(ret));
      }
      FOREACH_X(l, *sl->second, OB_SUCC(ret)) {
        if (l->pool_ == pool) {
          l->unit_config_ = new_config;
        }
      }
    }
  }
  return ret;
}

int ObUnitManager::gen_unit_load(ObUnit *unit, ObUnitLoad &load) const
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (NULL == unit) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unit is null", KP(unit), K(ret));
  } else if (!unit->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unit is invalid", "unit", *unit, K(ret));
  } else {
    ObUnitConfig *config = NULL;
    share::ObResourcePool *pool = NULL;
    if (OB_FAIL(get_resource_pool_by_id(unit->resource_pool_id_, pool))) {
      LOG_WARN("get_resource_pool_by_id failed", "pool id", unit->resource_pool_id_, K(ret));
    } else if (NULL == pool) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pools is null", KP(pool), K(ret));
    } else if (OB_FAIL(get_unit_config_by_id(pool->unit_config_id_, config))) {
      LOG_WARN("get_unit_config_by_id failed", "unit config id", pool->unit_config_id_, K(ret));
    } else if (NULL == config) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("config is null", KP(config), K(ret));
    } else {
      load.unit_ = unit;
      load.pool_ = pool;
      load.unit_config_ = config;
    }
  }
  return ret;
}

int ObUnitManager::gen_unit_load(const uint64_t unit_id, ObUnitLoad &load) const
{
  int ret = OB_SUCCESS;
  ObUnit *unit = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == unit_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid unit_id", K(unit_id), K(ret));
  } else if (OB_FAIL(get_unit_by_id(unit_id, unit))) {
    LOG_WARN("get_unit_by_id failed", K(unit_id), K(ret));
  } else if (NULL == unit) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unit is null", KP(unit), K(ret));
  } else if (OB_FAIL(gen_unit_load(unit, load))) {
    LOG_WARN("gen_unit_load failed", "unit", *unit, K(ret));
  }
  return ret;
}

int ObUnitManager::insert_tenant_pool(const uint64_t tenant_id, share::ObResourcePool *pool)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!is_valid_tenant_id(tenant_id) || NULL == pool) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), KP(pool), K(ret));
  } else if (OB_FAIL(insert_id_pool(tenant_pools_map_,
      tenant_pools_allocator_, tenant_id, pool))) {
    LOG_WARN("insert tenant pool failed", K(tenant_id), KP(pool), K(ret));
  }

  return ret;
}

int ObUnitManager::insert_config_pool(const uint64_t config_id, share::ObResourcePool *pool)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == config_id || NULL == pool) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(config_id), KP(pool), K(ret));
  } else if (OB_FAIL(insert_id_pool(config_pools_map_,
      config_pools_allocator_, config_id, pool))) {
    LOG_WARN("insert config pool failed", K(config_id), KP(pool), K(ret));
  }

  return ret;
}

int ObUnitManager::insert_id_pool(
    common::hash::ObHashMap<uint64_t, common::ObArray<share::ObResourcePool *> *> &map,
    common::ObPooledAllocator<common::ObArray<share::ObResourcePool *> > &allocator,
    const uint64_t id,
    share::ObResourcePool *pool)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!map.created() || OB_INVALID_ID == id || NULL == pool) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", "map created", map.created(), K(id), KP(pool), K(ret));
  } else {
    ObArray<share::ObResourcePool *> *pools = NULL;
    if (OB_FAIL(get_pools_by_id(map, id, pools))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("get_pools_by_id failed", K(id), K(ret));
      } else {
        ret = OB_SUCCESS;
        if (NULL == (pools = allocator.alloc())) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_ERROR("alloc pools failed", K(ret));
        } else if (OB_FAIL(pools->push_back(pool))) {
          LOG_WARN("push_back failed", K(ret));
        } else if (OB_FAIL(insert_id_pool_array(map, id, pools))) {
          LOG_WARN("insert_id_pool_array failed", K(id), K(ret));
        }

        // avoid memory leak
        if (OB_SUCCESS != ret && NULL != pools) {
          allocator.free(pools);
          pools = NULL;
        }
      }
    } else if (NULL == pools) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pools is null", KP(pools), K(ret));
    } else if (!has_exist_in_array(*pools, pool)) {
      if (OB_FAIL(pools->push_back(pool))) {
        LOG_WARN("push_back failed", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("resource pool already exists", K(ret), K(id), K(pools), K(pool));
    }
  }
  return ret;
}

int ObUnitManager::insert_id_pool_array(
    common::hash::ObHashMap<uint64_t, common::ObArray<share::ObResourcePool *> *> &map,
    const uint64_t id,
    ObArray<share::ObResourcePool *> *pools)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!map.created() || OB_INVALID_ID == id || NULL == pools) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", "map created", map.created(), K(id), KP(pools), K(ret));
  } else {
    if (OB_FAIL(map.set_refactored(id, pools))) {
      if (OB_HASH_EXIST == ret) {
        LOG_WARN("pools is not expect to exist", K(id), K(ret));
      } else {
        LOG_WARN("set failed", K(id), K(ret));
      }
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObUnitManager::insert_migrate_unit(const ObAddr &src_server, const uint64_t unit_id)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!src_server.is_valid() || OB_INVALID_ID == unit_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(src_server), K(unit_id), K(ret));
  } else {
    ObArray<uint64_t> *migrate_units = NULL;
    if (OB_FAIL(get_migrate_units_by_server(src_server, migrate_units))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("get_migrate_units_by_server failed", K(src_server), K(ret));
      } else {
        ret= OB_SUCCESS;
        if (NULL == (migrate_units = migrate_units_allocator_.alloc())) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_ERROR("alloc  failed", K(ret));
        } else if (OB_FAIL(migrate_units->push_back(unit_id))) {
          LOG_WARN("push_back failed", K(ret));
        } else {
          INSERT_ARRAY_TO_MAP(server_migrate_units_map_, src_server, migrate_units);
        }

        // avoid memory leak
        if (OB_SUCCESS != ret && NULL != migrate_units) {
          migrate_units_allocator_.free(migrate_units);
          migrate_units = NULL;
        }
      }
    } else if (NULL == migrate_units) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("migrate_units is null", KP(migrate_units), K(ret));
    } else {
      if (OB_FAIL(migrate_units->push_back(unit_id))) {
        LOG_WARN("push_back failed", K(ret));
      }
    }
  }
  return ret;
}

int ObUnitManager::set_server_ref_count(common::hash::ObHashMap<ObAddr, int64_t> &map,
    const ObAddr &server, const int64_t ref_count) const
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!map.created() || !server.is_valid() || ref_count <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", "map_created", map.created(), K(server), K(ref_count), K(ret));
  } else {
    SET_ITEM_TO_MAP(map, server, ref_count);
  }
  return ret;
}


#undef INSERT_ITEM_TO_MAP
#undef SET_ITEM_TO_MAP
#undef INSERT_ARRAY_TO_MAP

#define DELETE_ITEM_FROM_MAP(map, key) \
  do { \
    if (OB_FAIL(ret)) { \
    } else if (OB_FAIL(map.erase_refactored(key))) { \
      LOG_WARN("map erase failed", K(key), K(ret)); \
    } else { \
    } \
  } while (false)

int ObUnitManager::delete_unit_config(const uint64_t config_id,
                                      const ObUnitConfigName &name)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == config_id || name.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(config_id), K(name), K(ret));
  } else {
    DELETE_ITEM_FROM_MAP(id_config_map_, config_id);
    DELETE_ITEM_FROM_MAP(name_config_map_, name);
    DELETE_ITEM_FROM_MAP(config_ref_count_map_, config_id);
  }
  return ret;
}

int ObUnitManager::delete_resource_pool(const uint64_t pool_id,
                                        const ObResourcePoolName &name)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == pool_id || name.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(pool_id), K(name), K(ret));
  } else {
    DELETE_ITEM_FROM_MAP(id_pool_map_, pool_id);
    DELETE_ITEM_FROM_MAP(name_pool_map_, name);
  }
  return ret;
}

int ObUnitManager::delete_units_in_zones(
    const uint64_t resource_pool_id,
    const common::ObIArray<common::ObZone> &to_be_removed_zones)
{
  int ret = OB_SUCCESS;
  ObArray<ObUnit *> *units = NULL;
  ObArray<ObUnit *> left_units;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == resource_pool_id)
             || OB_UNLIKELY(to_be_removed_zones.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(resource_pool_id), K(to_be_removed_zones));
  } else if (OB_FAIL(get_units_by_pool(resource_pool_id, units))) {
    LOG_WARN("fail to get units by pool", K(ret), K(resource_pool_id));
  } else if (OB_UNLIKELY(NULL == units)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("units is null", K(ret), KP(units));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < units->count(); ++i) {
      const ObUnit *unit = units->at(i);
      if (OB_UNLIKELY(NULL == unit)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit should not be null", "unit", OB_P(units->at(i)), K(ret));
      } else if (!has_exist_in_array(to_be_removed_zones, unit->zone_)) {
        if (OB_FAIL(left_units.push_back(units->at(i)))) {
          LOG_WARN("fail to push back", K(ret));
        } else {} // no more to do
      } else if (OB_FAIL(delete_unit_loads(*unit))) {
        LOG_WARN("fail to delete unit load", K(ret), "unit", *units->at(i));
      } else {
        DELETE_ITEM_FROM_MAP(id_unit_map_, unit->unit_id_);
        if (OB_SUCC(ret)) {
          if (unit->migrate_from_server_.is_valid()) {
            //If the unit is being migrated, delete it from the state in memory
            if (OB_FAIL(delete_migrate_unit(unit->migrate_from_server_,
                                            unit->unit_id_))) {
              LOG_WARN("failed to delete migrate unit", K(ret), "unit", *unit);
            }
          }
        }
        if (OB_SUCC(ret)) {
          ROOTSERVICE_EVENT_ADD("unit", "drop_unit",
              "unit_id", units->at(i)->unit_id_);
          allocator_.free(units->at(i));
          units->at(i) = NULL;;
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (left_units.count() <= 0) {
        DELETE_ITEM_FROM_MAP(pool_unit_map_, resource_pool_id);
        if (OB_SUCC(ret)) {
          pool_unit_allocator_.free(units);
          units = NULL;
        }
      } else {
        if (OB_FAIL(units->assign(left_units))) {
          LOG_WARN("assign failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObUnitManager::delete_units_of_pool(const uint64_t resource_pool_id)
{
  int ret = OB_SUCCESS;
  ObArray<ObUnit *> *units = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == resource_pool_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(resource_pool_id), K(ret));
  } else if (OB_FAIL(get_units_by_pool(resource_pool_id, units))) {
    LOG_WARN("fail to get unit by pool", K(resource_pool_id), K(ret));
  } else if (OB_UNLIKELY(NULL == units)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("units is null", KP(units), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < units->count(); ++i) {
      const ObUnit *unit = units->at(i);
      if (OB_UNLIKELY(NULL == unit)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit should not be null", K(ret));
      } else if (OB_FAIL(delete_unit_loads(*unit))) {
        LOG_WARN("fail to delete unit load", K(ret));
      } else {
        DELETE_ITEM_FROM_MAP(id_unit_map_, unit->unit_id_);
        if (OB_SUCC(ret)) {
          if (unit->migrate_from_server_.is_valid()) {
            if (OB_FAIL(delete_migrate_unit(unit->migrate_from_server_,
                                            unit->unit_id_))) {
              LOG_WARN("failed to delete migrate unit", K(ret), "unit", *unit);
            }
          }

        }

        if (OB_SUCC(ret)) {
          ROOTSERVICE_EVENT_ADD("unit", "drop_unit",
                                "unit_id", units->at(i)->unit_id_);
          allocator_.free(units->at(i));
          units->at(i) = NULL;
        }
      }
    }
    if (OB_SUCC(ret)) {
      DELETE_ITEM_FROM_MAP(pool_unit_map_, resource_pool_id);
      if (OB_SUCC(ret)) {
        pool_unit_allocator_.free(units);
        units = NULL;
      }
    }
  }
  return ret;
}

int ObUnitManager::delete_invalid_inmemory_units(
    const uint64_t resource_pool_id,
    const common::ObIArray<uint64_t> &valid_unit_ids)
{
  int ret = OB_SUCCESS;
  ObArray<share::ObUnit *> *units = NULL;
  ObArray<share::ObUnit *> left_units;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == resource_pool_id || valid_unit_ids.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(resource_pool_id),
             "valid_unit_cnt", valid_unit_ids.count());
  } else if (OB_FAIL(get_units_by_pool(resource_pool_id, units))) {
    LOG_WARN("fail to get units by pool", K(ret), K(resource_pool_id));
  } else if (OB_UNLIKELY(NULL == units)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("units is null", KP(units), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < units->count(); ++i) {
      if (NULL == units->at(i)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit ptr is null", K(ret));
      } else if (has_exist_in_array(valid_unit_ids, units->at(i)->unit_id_)) {
        if (OB_FAIL(left_units.push_back(units->at(i)))) {
          LOG_WARN("push_back failed", K(ret));
        }
      } else if (OB_FAIL(delete_unit_loads(*units->at(i)))) {
        LOG_WARN("fail to delete unit loads", K(ret), "unit", *units->at(i));
      } else {
        DELETE_ITEM_FROM_MAP(id_unit_map_, units->at(i)->unit_id_);
        if (OB_SUCC(ret)) {
          ROOTSERVICE_EVENT_ADD("unit", "drop_unit",
              "unit_id", units->at(i)->unit_id_);
          allocator_.free(units->at(i));
          units->at(i) = NULL;
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (left_units.count() <= 0) {
        DELETE_ITEM_FROM_MAP(pool_unit_map_, resource_pool_id);
        if (OB_SUCC(ret)) {
          pool_unit_allocator_.free(units);
          units = NULL;
        }
      } else {
        if (OB_FAIL(units->assign(left_units))) {
          LOG_WARN("assign failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObUnitManager::delete_inmemory_units(
    const uint64_t resource_pool_id,
    const common::ObIArray<uint64_t> &unit_ids)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == resource_pool_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(resource_pool_id), K(ret));
  } else {
    ObArray<ObUnit *> *units = NULL;
    ObArray<ObUnit *> left_units;
    if (OB_FAIL(get_units_by_pool(resource_pool_id, units))) {
      LOG_WARN("get_units_by_pool failed", K(resource_pool_id), K(ret));
    } else if (NULL == units) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("units is null", KP(units), K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < units->count(); ++i) {
        if (NULL == units->at(i)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unit should not be null", "unit", OB_P(units->at(i)), K(ret));
        } else if (!has_exist_in_array(unit_ids, units->at(i)->unit_id_)) {
          if (OB_FAIL(left_units.push_back(units->at(i)))) {
            LOG_WARN("push_back failed", K(ret));
          }
        } else {
          if (OB_FAIL(delete_unit_loads(*units->at(i)))) {
            LOG_WARN("delete_unit_load failed", "unit", *units->at(i), K(ret));
          } else {
            DELETE_ITEM_FROM_MAP(id_unit_map_, units->at(i)->unit_id_);
            if (OB_SUCC(ret)) {
              ROOTSERVICE_EVENT_ADD("unit", "drop_unit",
                  "unit_id", units->at(i)->unit_id_);
              allocator_.free(units->at(i));
              units->at(i) = NULL;
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        // if all units of pool are deleted, delete item from hashmap
        if (left_units.count() <= 0) {
          DELETE_ITEM_FROM_MAP(pool_unit_map_, resource_pool_id);
          if (OB_SUCC(ret)) {
            pool_unit_allocator_.free(units);
            units = NULL;
          }
        } else {
          if (OB_FAIL(units->assign(left_units))) {
            LOG_WARN("assign failed", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObUnitManager::delete_unit_loads(const ObUnit &unit)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!unit.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid unit", K(unit), K(ret));
  } else {
    const uint64_t unit_id = unit.unit_id_;
    if (OB_FAIL(delete_unit_load(unit.server_, unit_id))) {
      LOG_WARN("delete_unit_load failed", "server", unit.server_, K(unit_id), K(ret));
    } else {
      if (unit.migrate_from_server_.is_valid()) {
        if (OB_FAIL(delete_unit_load(unit.migrate_from_server_, unit_id))) {
          LOG_WARN("delete_unit_load failed", "server", unit.migrate_from_server_,
              K(unit_id), K(ret));
        } else {
          // do nothing
        }
      }
    }
  }
  return ret;
}

int ObUnitManager::delete_unit_load(const ObAddr &server, const uint64_t unit_id)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!server.is_valid() || OB_INVALID_ID == unit_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(server), K(unit_id), K(ret));
  } else {
    ObArray<ObUnitLoad> *loads = NULL;
    if (OB_FAIL(get_loads_by_server(server, loads))) {
      LOG_WARN("get_loads_by_server failed", K(server), K(ret));
    } else if (NULL == loads) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("loads is null", KP(loads), K(ret));
    } else {
      int64_t index = -1;
      for (int64_t i = 0; OB_SUCC(ret) && i < loads->count(); ++i) {
        if (loads->at(i).unit_->unit_id_ == unit_id) {
          index = i;
          break;
        }
      }
      if (-1 == index) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("unit load not exist", K(server), K(unit_id), K(ret));
      } else {
        for (int64_t i = index; i < loads->count() - 1; ++i) {
          loads->at(i) = loads->at(i+1);
        }
        loads->pop_back();
        if (0 == loads->count()) {
          load_allocator_.free(loads);
          loads = NULL;
          DELETE_ITEM_FROM_MAP(server_loads_, server);
        }
      }
    }
  }
  return ret;
}

int ObUnitManager::delete_tenant_pool(const uint64_t tenant_id, share::ObResourcePool *pool)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!is_valid_tenant_id(tenant_id) || NULL == pool) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), KP(pool), K(ret));
  } else if (OB_FAIL(delete_id_pool(tenant_pools_map_,
      tenant_pools_allocator_, tenant_id, pool))) {
    LOG_WARN("delete tenant pool failed", K(ret), K(tenant_id), "pool", *pool);
  }
  return ret;
}

int ObUnitManager::delete_config_pool(const uint64_t config_id, share::ObResourcePool *pool)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == config_id || NULL == pool) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(config_id), KP(pool), K(ret));
  } else if (OB_FAIL(delete_id_pool(config_pools_map_,
      config_pools_allocator_, config_id, pool))) {
    LOG_WARN("delete config pool failed", K(ret), K(config_id), "pool", *pool);
  }
  return ret;
}

int ObUnitManager::delete_id_pool(
    common::hash::ObHashMap<uint64_t, common::ObArray<share::ObResourcePool *> *> &map,
    common::ObPooledAllocator<common::ObArray<share::ObResourcePool *> > &allocator,
    const uint64_t id,
    share::ObResourcePool *pool)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!map.created() || OB_INVALID_ID == id || NULL == pool) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", "map created", map.created(), K(id), KP(pool), K(ret));
  } else {
    ObArray<share::ObResourcePool *> *pools = NULL;
    if (OB_FAIL(get_pools_by_id(map, id, pools))) {
      LOG_WARN("get_pools_by_id failed", K(id), K(ret));
    } else if (NULL == pools) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pools is null", KP(pools), K(ret));
    } else {
      int64_t index = -1;
      for (int64_t i = 0; i < pools->count(); ++i) {
        if (pools->at(i) == pool) {
          index = i;
          break;
        }
      }
      if (-1 == index) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("pool not exist", K(id), K(ret));
      } else if (OB_FAIL(pools->remove(index))) {
        LOG_WARN("remove failed", K(index), K(ret));
      } else if (0 == pools->count()) {
        allocator.free(pools);
        pools = NULL;
        DELETE_ITEM_FROM_MAP(map, id);
      }
    }
  }
  return ret;
}

int ObUnitManager::delete_migrate_unit(const ObAddr &src_server,
                                       const uint64_t unit_id)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!src_server.is_valid() || OB_INVALID_ID == unit_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(src_server), K(unit_id), K(ret));
  } else {
    ObArray<uint64_t> *migrate_units = NULL;
    if (OB_FAIL(get_migrate_units_by_server(src_server, migrate_units))) {
      LOG_WARN("get_migrate_units_by_server failed", K(src_server), K(ret));
    } else if (NULL == migrate_units) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("migrate_units is null", KP(migrate_units), K(ret));
    } else {
      int64_t index = -1;
      for (int64_t i = 0; i < migrate_units->count(); ++i) {
        if (migrate_units->at(i) == unit_id) {
          index = i;
          break;
        }
      }
      if (-1 == index) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("migrate_unit not exist", K(unit_id), K(ret));
      } else if (OB_FAIL(migrate_units->remove(index))) {
        LOG_WARN("remove failed", K(index), K(ret));
      } else if (0 == migrate_units->count()) {
        migrate_units_allocator_.free(migrate_units);
        migrate_units = NULL;
        DELETE_ITEM_FROM_MAP(server_migrate_units_map_, src_server);
      }
    }
  }
  return ret;
}

#undef DELETE_ITEM_FROM_MAP

#define GET_ITEM_FROM_MAP(map, key, value) \
  do { \
    if (OB_FAIL(map.get_refactored(key, value))) { \
      if (OB_HASH_NOT_EXIST == ret) { \
        ret = OB_ENTRY_NOT_EXIST; \
      } else { \
        LOG_WARN("map get failed", K(key), K(ret)); \
      } \
    } else { \
    } \
  } while (false)

int ObUnitManager::update_unit_load(
    share::ObUnit *unit,
    share::ObResourcePool *new_pool)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (NULL == unit || NULL == new_pool) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(unit), KP(new_pool));
  } else {
    ObArray<ObUnitManager::ObUnitLoad> *server_load = NULL;
    GET_ITEM_FROM_MAP(server_loads_, unit->server_, server_load);
    if (OB_FAIL(ret)) {
      LOG_WARN("fail to get server load", K(ret), "server", unit->server_);
    } else if (NULL == server_load) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("loads ptr is null", K(ret));
    } else {
      FOREACH_X(l, *server_load, OB_SUCC(ret)) {
        if (l->unit_ == unit) {
          l->pool_ = new_pool;
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (unit->migrate_from_server_.is_valid()) {
      ObArray<ObUnitManager::ObUnitLoad> *migrate_from_load = NULL;
      GET_ITEM_FROM_MAP(server_loads_, unit->migrate_from_server_, migrate_from_load);
      if (OB_FAIL(ret)) {
         LOG_WARN("fail to get server load", K(ret),
                  "migrate_from_server", unit->migrate_from_server_);
      } else if (NULL == migrate_from_load) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("loads ptr is null", K(ret));
      } else {
        FOREACH_X(l, *migrate_from_load, OB_SUCC(ret)) {
          if (l->unit_ == unit) {
            l->pool_ = new_pool;
          }
        }
      }
    }
  }
  return ret;
}

int ObUnitManager::get_unit_config_by_name(const ObUnitConfigName &name,
                                           ObUnitConfig *&config) const
{
  int ret = OB_SUCCESS;
  config = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (name.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid name", K(name), K(ret));
  } else {
    GET_ITEM_FROM_MAP(name_config_map_, name, config);
  }
  return ret;
}

int ObUnitManager::get_unit_config_by_id(const uint64_t config_id, ObUnitConfig *&config) const
{
  int ret = OB_SUCCESS;
  config = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == config_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(config_id), K(ret));
  } else {
    GET_ITEM_FROM_MAP(id_config_map_, config_id, config);
  }
  return ret;
}

int ObUnitManager::get_config_ref_count(const uint64_t config_id, int64_t &ref_count) const
{
  int ret = OB_SUCCESS;
  ref_count = 0;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == config_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(config_id), K(ret));
  } else {
    GET_ITEM_FROM_MAP(config_ref_count_map_, config_id, ref_count);
    if (OB_FAIL(ret)) {
      if (OB_ENTRY_NOT_EXIST == ret) {
      } else {
        LOG_WARN("GET_ITEM_FROM_MAP failed", K(config_id), K(ret));
      }
    }
  }
  return ret;
}

int ObUnitManager::get_server_ref_count(common::hash::ObHashMap<ObAddr, int64_t> &map,
    const ObAddr &server, int64_t &ref_count) const
{
  int ret = OB_SUCCESS;
  ref_count = 0;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!map.created() || !server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", "map created", map.created(), K(server), K(ret));
  } else {
    GET_ITEM_FROM_MAP(map, server, ref_count);
    if (OB_FAIL(ret)) {
      if (OB_ENTRY_NOT_EXIST == ret) {
      } else {
        LOG_WARN("GET_ITEM_FROM_MAP failed", K(server), K(ret));
      }
    }
  }
  return ret;
}

int ObUnitManager::inner_get_resource_pool_by_name(
    const ObResourcePoolName &name,
    share::ObResourcePool *&pool) const
{
  int ret = OB_SUCCESS;
  pool = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (name.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid name", K(name), K(ret));
  } else {
    GET_ITEM_FROM_MAP(name_pool_map_, name, pool);
  }
  return ret;
}

int ObUnitManager::get_resource_pool_by_id(const uint64_t pool_id, share::ObResourcePool *&pool) const
{
  int ret = OB_SUCCESS;
  pool = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == pool_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(pool_id), K(ret));
  } else {
    GET_ITEM_FROM_MAP(id_pool_map_, pool_id, pool);
  }
  return ret;
}

int ObUnitManager::get_units_by_pool(const uint64_t pool_id, ObArray<ObUnit *> *&units) const
{
  int ret = OB_SUCCESS;
  units = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == pool_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(pool_id), K(ret));
  } else {
    GET_ITEM_FROM_MAP(pool_unit_map_, pool_id, units);
  }
  return ret;
}

int ObUnitManager::get_unit_by_id(const uint64_t unit_id, ObUnit *&unit) const
{
  int ret = OB_SUCCESS;
  unit = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == unit_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(unit_id), K(ret));
  } else {
    GET_ITEM_FROM_MAP(id_unit_map_, unit_id, unit);
  }
  return ret;
}

int ObUnitManager::get_loads_by_server(const ObAddr &addr,
                                       ObArray<ObUnitManager::ObUnitLoad> *&loads) const
{
  int ret = OB_SUCCESS;
  loads = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!addr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(addr), K(ret));
  } else {
    GET_ITEM_FROM_MAP(server_loads_, addr, loads);
  }
  return ret;
}

int ObUnitManager::get_pools_by_tenant_(const uint64_t tenant_id,
                                       ObArray<share::ObResourcePool *> *&pools) const
{
  int ret = OB_SUCCESS;
  // meta tenant has no self resource pool, here return user tenant resource pool
  uint64_t valid_tnt_id = is_meta_tenant(tenant_id) ? gen_user_tenant_id(tenant_id) : tenant_id;

  pools = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(ret));
  } else if (OB_FAIL(get_pools_by_id(tenant_pools_map_, valid_tnt_id, pools))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("get_pools_by_id failed", K(valid_tnt_id), K(ret));
    }
  }
  return ret;
}

int ObUnitManager::get_pools_by_config(const uint64_t config_id,
                                       ObArray<share::ObResourcePool *> *&pools) const
{
  int ret = OB_SUCCESS;
  pools = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == config_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(config_id), K(ret));
  } else if (OB_FAIL(get_pools_by_id(config_pools_map_, config_id, pools))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("get_pools_by_id failed", K(config_id), K(ret));
    }
  }
  return ret;
}

int ObUnitManager::get_pools_by_id(
    const common::hash::ObHashMap<uint64_t, common::ObArray<share::ObResourcePool *> *> &map,
    const uint64_t id, ObArray<share::ObResourcePool *> *&pools) const
{
  int ret = OB_SUCCESS;
  pools = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!map.created() || OB_INVALID_ID == id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", "map created", map.created(), K(id), K(ret));
  } else {
    GET_ITEM_FROM_MAP(map, id, pools);
  }
  return ret;
}

int ObUnitManager::inner_check_single_logonly_pool_for_locality(
    const share::ObResourcePool &pool,
    const common::ObIArray<share::ObZoneReplicaAttrSet> &zone_locality,
    bool &is_legal)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObUnitManager not init", K(ret), K(inited_));
  } else if (REPLICA_TYPE_LOGONLY != pool.replica_type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pool replica type unexpected", K(ret), K(pool));
  } else {
    is_legal = true;
    for (int64_t i = 0; is_legal && i < pool.zone_list_.count(); ++i) {
      const common::ObZone &zone = pool.zone_list_.at(i);
      for (int64_t j = 0; is_legal && j < zone_locality.count(); ++j) {
        const common::ObIArray<common::ObZone> &zone_set = zone_locality.at(j).zone_set_;
        if (zone_set.count() <= 1) {
          // bypass Non-mixed locality
        } else { // mixed locality
          is_legal = !has_exist_in_array(zone_set, zone);
        }
      }
    }
  }
  return ret;
}

int ObUnitManager::inner_check_logonly_pools_for_locality(
    const common::ObIArray<share::ObResourcePool *> &pools,
    const common::ObIArray<share::ObZoneReplicaAttrSet> &zone_locality,
    bool &is_legal)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObUnitManager not init", K(ret), K(inited_));
  } else if (OB_UNLIKELY(pools.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguemnt", K(ret), "pool_count", pools.count());
  } else {
    is_legal = true;
    for (int64_t i = 0; is_legal && OB_SUCC(ret) && i < pools.count(); ++i) {
      share::ObResourcePool *pool = pools.at(i);
      if (OB_UNLIKELY(nullptr == pool)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pool ptr is null", K(ret), KP(pool));
      } else if (REPLICA_TYPE_LOGONLY != pool->replica_type_) {
        // bypass, since this is not logonly pool
      } else if (OB_FAIL(inner_check_single_logonly_pool_for_locality(
              *pool, zone_locality, is_legal))) {
        LOG_WARN("fail to inner check single logonly pool for locality", K(ret));
      }
    }
  }
  return ret;
}

int ObUnitManager::inner_check_pools_unit_num_enough_for_locality(
    const common::ObIArray<share::ObResourcePool *> &pools,
    const common::ObIArray<common::ObZone> &schema_zone_list,
    const ZoneLocalityIArray &zone_locality,
    bool &is_enough)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObUnitManager not init", K(ret), K(inited_));
  } else if (pools.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(pools.count()));
  } else {
    common::ObArray<int64_t> unit_nums;
    for (int64_t i = 0; OB_SUCC(ret) && i < pools.count(); ++i) {
      share::ObResourcePool *pool = pools.at(i);
      if (NULL == pool) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pool ptr is null", K(ret), KP(pool));
      } else if (OB_FAIL(unit_nums.push_back(pool->unit_count_))) {
        LOG_WARN("fail to push back", K(ret));
      } else {} // no more to do
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(do_check_shrink_granted_pool_allowed_by_locality(
            pools, schema_zone_list, zone_locality, unit_nums, is_enough))) {
      LOG_WARN("fail to check pools unit num enough for locality", K(ret));
    } else {} // no more to do
  }
  return ret;
}

// The legality check consists of two parts
// 1 In the locality of the mixed scene, there can be no logonly resource pool
// 2 Can the unit num of the resource pool fit all locality copies
int ObUnitManager::check_pools_unit_legality_for_locality(
    const common::ObIArray<share::ObResourcePoolName> &pools,
    const common::ObIArray<common::ObZone> &schema_zone_list,
    const ZoneLocalityIArray &zone_locality,
    bool &is_legal)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObUnitManager not init", K(ret), K(inited_));
  } else if (pools.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(pools.count()));
  } else {
    SpinRLockGuard guard(lock_);
    common::ObArray<share::ObResourcePool *> pool_ptrs;
    for (int64_t i = 0; OB_SUCC(ret) && i < pools.count(); ++i) {
      share::ObResourcePool *pool_ptr = NULL;
      if (OB_FAIL(inner_get_resource_pool_by_name(pools.at(i), pool_ptr))) {
        LOG_WARN("fail to get resource pool by name", K(ret), "pool_name", pools.at(i));
      } else if (OB_FAIL(pool_ptrs.push_back(pool_ptr))) {
        LOG_WARN("fail to push back", K(ret));
      } else {} // no more to do
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(inner_check_logonly_pools_for_locality(
            pool_ptrs, zone_locality, is_legal))) {
      LOG_WARN("fail to check logonly pools for locality", K(ret));
    } else if (!is_legal) {
      // no need to check any more
    } else if (OB_FAIL(inner_check_pools_unit_num_enough_for_locality(
            pool_ptrs, schema_zone_list, zone_locality, is_legal))) {
      LOG_WARN("fail to check pools unit num enough for locality", K(ret));
    } else {} // no more to do
  }
  return ret;
}

int ObUnitManager::get_migrate_units_by_server(const ObAddr &server,
                                               common::ObIArray<uint64_t> &migrate_units) const
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> *units= NULL;
  if (OB_FAIL(get_migrate_units_by_server(server, units))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS; // no migrating units
    } else {
      LOG_WARN("fail get migrate units by server", K(server), K(ret));
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < units->count(); ++i) {
      const uint64_t unit_id = units->at(i);
      if (OB_FAIL(migrate_units.push_back(unit_id))) {
        LOG_WARN("fail push back unit id to array", K(unit_id), K(i), K(ret));
      }
    }
  }
  return ret;
}

int ObUnitManager::get_migrate_units_by_server(const ObAddr &server,
                                               common::ObArray<uint64_t> *&migrate_units) const
{
  int ret = OB_SUCCESS;
  migrate_units = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), K(ret));
  } else {
    GET_ITEM_FROM_MAP(server_migrate_units_map_, server, migrate_units);
  }
  return ret;
}

#undef DELETE_ITEM_FROM_MAP

int ObUnitManager::fetch_new_unit_config_id(uint64_t &config_id)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObMaxIdFetcher id_fetcher(*proxy_);
    uint64_t combine_id = OB_INVALID_ID;
    if (OB_FAIL(id_fetcher.fetch_new_max_id(
        OB_SYS_TENANT_ID, OB_MAX_USED_UNIT_CONFIG_ID_TYPE, combine_id))) {
      LOG_WARN("fetch_max_id failed", "id_type", OB_MAX_USED_UNIT_CONFIG_ID_TYPE, K(ret));
    } else {
      config_id = combine_id;
    }
  }
  return ret;
}

int ObUnitManager::fetch_new_resource_pool_id(uint64_t &resource_pool_id)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    uint64_t combine_id = OB_INVALID_ID;
    ObMaxIdFetcher id_fetcher(*proxy_);
    if (OB_FAIL(id_fetcher.fetch_new_max_id(OB_SYS_TENANT_ID,
        OB_MAX_USED_RESOURCE_POOL_ID_TYPE, combine_id))) {
      LOG_WARN("fetch_new_max_id failed", "id_type", OB_MAX_USED_RESOURCE_POOL_ID_TYPE, K(ret));
    } else {
      resource_pool_id = combine_id;
    }
  }
  return ret;
}

int ObUnitManager::batch_fetch_new_unit_group_id(const int64_t count, common::ObArray<uint64_t> &unit_group_ids)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(count));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      uint64_t unit_group_id = OB_INVALID_ID;
      if (OB_FAIL(fetch_new_unit_group_id(unit_group_id))) {
        LOG_WARN("fail to fetch new unit group id", KR(ret));
      } else if (OB_FAIL(unit_group_ids.push_back(unit_group_id))) {
        LOG_WARN("fail to push back", KR(ret));
      }
    }
  }
  return ret;
}

int ObUnitManager::fetch_new_unit_group_id(uint64_t &unit_group_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    uint64_t combine_id = OB_INVALID_ID;
    ObMaxIdFetcher id_fetcher(*proxy_);
    if (OB_FAIL(id_fetcher.fetch_new_max_id(OB_SYS_TENANT_ID,
        OB_MAX_USED_UNIT_GROUP_ID_TYPE, combine_id))) {
      LOG_WARN("fetch_new_max_id failed", "id_type", OB_MAX_USED_UNIT_ID_TYPE, K(ret));
    } else {
      unit_group_id = combine_id;
    }
  }
  return ret;
}

int ObUnitManager::fetch_new_unit_id(uint64_t &unit_id)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    uint64_t combine_id = OB_INVALID_ID;
    ObMaxIdFetcher id_fetcher(*proxy_);
    if (OB_FAIL(id_fetcher.fetch_new_max_id(OB_SYS_TENANT_ID,
        OB_MAX_USED_UNIT_ID_TYPE, combine_id))) {
      LOG_WARN("fetch_new_max_id failed", "id_type", OB_MAX_USED_UNIT_ID_TYPE, K(ret));
    } else {
      unit_id = combine_id;
    }
  }
  return ret;
}

int ObUnitManager::check_bootstrap_pool(const share::ObResourcePool &pool)
{
  int ret = OB_SUCCESS;
  if (!pool.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid pool", K(pool), K(ret));
  } else if (ObUnitConfig::SYS_UNIT_CONFIG_ID == pool.unit_config_id_) {
    // good
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("pool not sys pool", K(pool), K(ret));
  }
  return ret;
}

int ObUnitManager::inner_get_tenant_zone_full_unit_num(
    const int64_t tenant_id,
    const common::ObZone &zone,
    int64_t &unit_num)
{
  int ret = OB_SUCCESS;
  ObArray<share::ObResourcePool *> *pools = NULL;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(get_pools_by_tenant_(tenant_id, pools))) {
    LOG_WARN("fail to get pools by tenant", K(ret), K(tenant_id));
  } else if (OB_UNLIKELY(NULL == pools)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pools ptr is null", K(ret), KP(pools));
  } else {
    bool find = false;
    for (int64_t i = 0; !find && OB_SUCC(ret) && i < pools->count(); ++i) {
      share::ObResourcePool *pool = pools->at(i);
      if (NULL == pool) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pool ptr is null", K(ret), KP(pool));
      } else if (!has_exist_in_array(pool->zone_list_, zone)) {
        // not in this pool
      } else if (REPLICA_TYPE_FULL == pool->replica_type_) {
        unit_num = pool->unit_count_;
        find = true;
      } else {} // not a full replica type resource pool, go on to check next
    }
    if (OB_FAIL(ret)) {
    } else if (!find) {
      unit_num = 0;
    }
  }
  return ret;
}

int ObUnitManager::get_tenant_zone_unit_loads(
    const int64_t tenant_id,
    const common::ObZone &zone,
    const common::ObReplicaType replica_type,
    common::ObIArray<ObUnitManager::ObUnitLoad> &unit_loads)
{
  int ret = OB_SUCCESS;
  ObArray<share::ObResourcePool *> *pools = NULL;
  unit_loads.reset();
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
                         || zone.is_empty()
                         || !ObReplicaTypeCheck::is_replica_type_valid(replica_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(zone), K(replica_type));
  } else if (OB_FAIL(get_pools_by_tenant_(tenant_id, pools))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get pools by tenant", K(ret), K(tenant_id));
    }
  } else if (OB_UNLIKELY(NULL == pools)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pool ptr is null", K(ret), KP(pools));
  } else {
    unit_loads.reset();
    share::ObResourcePool *target_pool = NULL;
    for (int64_t i = 0; NULL == target_pool && OB_SUCC(ret) && i < pools->count(); ++i) {
      share::ObResourcePool *pool = pools->at(i);
      if (NULL == pool) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pool ptr is null", K(ret), KP(pool));
      } else if (!has_exist_in_array(pool->zone_list_, zone)) {
        // not in this pool
      } else if (replica_type == pool->replica_type_) {
        target_pool = pool;
      } else {} // not a full replica type resource pool, go on to check next
    }
    if (OB_FAIL(ret)) {
    } else if (NULL == target_pool) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      ObArray<share::ObUnit *> *units = NULL;
      if (OB_FAIL(get_units_by_pool(target_pool->resource_pool_id_, units))) {
        LOG_WARN("fail to get units by pool", K(ret),
                 "pool_id", target_pool->resource_pool_id_);
      } else if (OB_UNLIKELY(NULL == units)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("units ptr is null", K(ret), KP(units));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < units->count(); ++i) {
          ObUnitLoad unit_load;
          ObUnit *unit = units->at(i);
          if (NULL == unit) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unit ptr is null", K(ret));
          } else if (unit->zone_ != zone) {
            // not this zone, ignore
          } else if (OB_FAIL(gen_unit_load(unit, unit_load))) {
            LOG_WARN("fail to gen unit load", K(ret));
          } else if (OB_FAIL(unit_loads.push_back(unit_load))) {
            LOG_WARN("fail to push back", K(ret));
          } else {} // no more to do
        }
      }
    }
  }
  return ret;
}

int ObUnitManager::get_tenant_zone_all_unit_loads(
    const int64_t tenant_id,
    const common::ObZone &zone,
    common::ObIArray<ObUnitManager::ObUnitLoad> &unit_loads)
{
  int ret = OB_SUCCESS;
  ObArray<share::ObResourcePool *> *pools = NULL;
  unit_loads.reset();
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || zone.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(zone));
  } else if (OB_FAIL(get_pools_by_tenant_(tenant_id, pools))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get pools by tenant", K(ret), K(tenant_id));
    }
  } else if (OB_UNLIKELY(NULL == pools)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pool ptr is null", K(ret), KP(pools));
  } else {
    unit_loads.reset();
    common::ObArray<share::ObResourcePool *> target_pools;
    for (int64_t i = 0; OB_SUCC(ret) && i < pools->count(); ++i) {
      share::ObResourcePool *pool = pools->at(i);
      if (NULL == pool) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pool ptr is null", K(ret), KP(pool));
      } else if (!has_exist_in_array(pool->zone_list_, zone)) {
        // not in this pool
      } else if (OB_FAIL(target_pools.push_back(pool))) {
        LOG_WARN("fail to push back", K(ret));
      } else {} // no more to do
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < target_pools.count(); ++i) {
      ObArray<share::ObUnit *> *units = NULL;
      share::ObResourcePool *target_pool = target_pools.at(i);
      if (OB_UNLIKELY(NULL == target_pool)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("target pool ptr is null", K(ret), KP(target_pool));
      } else if (OB_FAIL(get_units_by_pool(target_pool->resource_pool_id_, units))) {
        LOG_WARN("fail to get units by pool", K(ret),
                 "pool_id", target_pool->resource_pool_id_);
      } else if (OB_UNLIKELY(NULL == units)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("units ptr is null", K(ret), KP(units));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < units->count(); ++i) {
          ObUnitLoad unit_load;
          ObUnit *unit = units->at(i);
          if (NULL == unit) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unit ptr is null", K(ret));
          } else if (unit->zone_ != zone) {
            // not this zone, ignore
          } else if (OB_FAIL(gen_unit_load(unit, unit_load))) {
            LOG_WARN("fail to gen unit load", K(ret));
          } else if (OB_FAIL(unit_loads.push_back(unit_load))) {
            LOG_WARN("fail to push back", K(ret));
          } else {} // no more to do
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (unit_loads.count() <= 0) {
      ret = OB_ENTRY_NOT_EXIST;
    }
  }
  return ret;
}

int ObUnitManager::get_logonly_unit_by_tenant(const int64_t tenant_id,
                                              ObIArray<ObUnitInfo> &logonly_unit_infos)
{
  int ret = OB_SUCCESS;
  share::schema::ObSchemaGetterGuard schema_guard;
  if (OB_ISNULL(schema_service_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema service is null", K(schema_service_), KR(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_logonly_unit_by_tenant(schema_guard, tenant_id, logonly_unit_infos))) {
    LOG_WARN("get logonly unit by tenant fail", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObUnitManager::get_logonly_unit_by_tenant(share::schema::ObSchemaGetterGuard &schema_guard,
                                              const int64_t tenant_id,
                                              ObIArray<ObUnitInfo> &logonly_unit_infos)
{
  int ret = OB_SUCCESS;
  const ObTenantSchema *tenant_schema = NULL;
  logonly_unit_infos.reset();
  ObArray<ObUnitInfo> unit_infos;
  SpinRLockGuard guard(lock_);
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(inited_), K(loaded_), K(ret));
  } else if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_schema))) {
    LOG_WARN("fail to get tenant info", K(ret), K(tenant_id));
  } else if (OB_ISNULL(tenant_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid tenant_schema", K(ret), K(tenant_id));
  } else if (OB_FAIL(inner_get_active_unit_infos_of_tenant(*tenant_schema, unit_infos))) {
    LOG_WARN("fail to get active unit", K(ret), K(tenant_id));
  } else {
    for (int64_t i = 0; i < unit_infos.count() && OB_SUCC(ret); i++) {
      if (REPLICA_TYPE_LOGONLY != unit_infos.at(i).unit_.replica_type_) {
        //nothing todo
      } else if (OB_FAIL(logonly_unit_infos.push_back(unit_infos.at(i)))) {
        LOG_WARN("fail to push back", K(ret), K(unit_infos));
      }
    }
  }
  return ret;
}

int ObUnitManager::get_unit_infos(const common::ObIArray<share::ObResourcePoolName> &pools,
                                  ObIArray<ObUnitInfo> &unit_infos)
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  unit_infos.reset();
  if (pools.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(pools));
  } else {
    ObArray<ObUnitInfo> pool_units;
    share::ObResourcePool *pool = NULL;
    for (int64_t i = 0; i < pools.count() && OB_SUCC(ret); i++) {
      pool_units.reset();
      if (OB_FAIL(inner_get_resource_pool_by_name(pools.at(i), pool))) {
        LOG_WARN("fail to get resource pool", K(ret), K(i), K(pools));
      } else if (OB_ISNULL(pool)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid pool", K(ret), K(pools));
      } else if (OB_FAIL(inner_get_unit_infos_of_pool_(pool->resource_pool_id_, pool_units))) {
        LOG_WARN("fail to get unit infos", K(ret), K(*pool));
      } else {
        for (int64_t j = 0; j < pool_units.count() && OB_SUCC(ret); j++) {
          if (OB_FAIL(unit_infos.push_back(pool_units.at(j)))) {
            LOG_WARN("fail to push back", K(ret), K(pool_units));
          }
        }
      }
    }
  }
  return ret;
}

int ObUnitManager::get_servers_by_pools(
    const common::ObIArray<share::ObResourcePoolName> &pools,
    common::ObIArray<ObAddr> &addrs)
{
  int ret = OB_SUCCESS;
  addrs.reset();
  ObArray<share::ObUnitInfo> unit_infos;
  if (OB_FAIL(get_unit_infos(pools, unit_infos))) {
    LOG_WARN("fail to get unit infos", KR(ret), K(pools));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < unit_infos.count(); i++) {
    const share::ObUnitInfo &unit_info = unit_infos.at(i);
    if (OB_FAIL(addrs.push_back(unit_info.unit_.server_))) {
      LOG_WARN("fail to push back addr", KR(ret), K(unit_info));
    }
  } // end for
  return ret;
}

int ObUnitManager::inner_get_active_unit_infos_of_tenant(const ObTenantSchema &tenant_schema,
                                                  ObIArray<ObUnitInfo> &unit_info)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> pool_ids;
  uint64_t tenant_id = tenant_schema.get_tenant_id();
  common::ObArray<common::ObZone> tenant_zone_list;
  ObArray<share::ObResourcePool  *> *pools = NULL;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(inited_), K(loaded_), K(ret));
  } else if (OB_FAIL(tenant_schema.get_zone_list(tenant_zone_list))) {
    LOG_WARN("fail to get zone list", K(ret));
  } else if (OB_FAIL(get_pools_by_tenant_(tenant_id, pools))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("get_pools_by_tenant failed", K(tenant_id), K(ret));
    } else {
      // just return empty pool_ids
      ret = OB_SUCCESS;
      LOG_WARN("tenant doesn't own any pool", K(tenant_id), K(ret));
    }
  } else if (NULL == pools) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pools is null", KP(pools), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < pools->count(); ++i) {
      if (NULL == pools->at(i)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pool is null", "pool", OB_P(pools->at(i)), K(ret));
      } else if (OB_FAIL(pool_ids.push_back(pools->at(i)->resource_pool_id_))) {
        LOG_WARN("push_back failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObArray<ObUnitInfo> unit_in_pool;
    for (int64_t i = 0; i < pool_ids.count() && OB_SUCC(ret); i++) {
      uint64_t pool_id = pool_ids.at(i);
      unit_in_pool.reset();
      if (OB_FAIL(inner_get_unit_infos_of_pool_(pool_id, unit_in_pool))) {
        LOG_WARN("fail to inner get unit infos", K(ret), K(pool_id));
      } else {
        for (int64_t j = 0; j < unit_in_pool.count() && OB_SUCC(ret); j++) {
          if (ObUnit::UNIT_STATUS_DELETING == unit_in_pool.at(j).unit_.status_) {
            //nothing todo
          } else if (!has_exist_in_array(tenant_zone_list, unit_in_pool.at(j).unit_.zone_)) {
            //nothing todo
          } else if (OB_FAIL(unit_info.push_back(unit_in_pool.at(j)))) {
            LOG_WARN("fail to push back", K(ret), K(unit_in_pool), K(j));
          }
        }
      }
    }
  }
  return ret;
}

}//end namespace share
}//end namespace oceanbase
