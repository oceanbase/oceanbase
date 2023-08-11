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

#define USING_LOG_PREFIX SHARE

#include "share/ob_unit_getter.h"
#include "share/ob_get_compat_mode.h"

namespace oceanbase
{
using namespace common;
using namespace common::sqlclient;
namespace share
{

OB_SERIALIZE_MEMBER(ObUnitInfoGetter::ObTenantConfig,
                    tenant_id_,
                    unit_id_,
                    unit_status_,
                    config_,
                    mode_,
                    create_timestamp_,
                    has_memstore_,
                    is_removed_);

const char* ObUnitInfoGetter::unit_status_strs_[] = {
    "NORMAL",
    "MIGRATE IN",
    "MIGRATE OUT",
    "MARK DELETING",
    "WAIT GC",
    "DELETING",
    "ERROR"
};

ObUnitInfoGetter::ObTenantConfig::ObTenantConfig()
  : tenant_id_(common::OB_INVALID_ID),
    unit_id_(common::OB_INVALID_ID),
    unit_status_(UNIT_ERROR_STAT),
    config_(),
    mode_(lib::Worker::CompatMode::INVALID),
    create_timestamp_(0),
    has_memstore_(true),
    is_removed_(false)
{}

int ObUnitInfoGetter::ObTenantConfig::init(
    const uint64_t tenant_id,
    const uint64_t unit_id,
    const ObUnitStatus unit_status,
    const ObUnitConfig &config,
    lib::Worker::CompatMode compat_mode,
    const int64_t create_timestamp,
    const bool has_memstore,
    const bool is_remove)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(config_.assign(config))) {
    LOG_WARN("fail to assign config", KR(ret), K(config));
  } else {
    tenant_id_ = tenant_id;
    unit_id_ = unit_id;
    unit_status_ = unit_status;
    mode_ = compat_mode;
    create_timestamp_ = create_timestamp;
    has_memstore_ = has_memstore;
    is_removed_ = is_remove;
  }
  return ret;
}

template <typename T> static T limit_(T value, T left, T right)
{
  if (value > right) {
    return right;
  } else if (value > left) {
    return value;
  } else {
    return left;
  }
}

int ObUnitInfoGetter::ObTenantConfig::divide_meta_tenant(ObTenantConfig& meta_tenant_config)
{
  int ret = OB_SUCCESS;
  ObUnitResource meta_resource;
  ObUnitConfig meta_config;
  ObUnitResource self_resource = config_.unit_resource(); // get a copy

  if (OB_UNLIKELY(! is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not valid tenant config, can't divide meta tenant", KR(ret), KPC(this));
  } else if (OB_FAIL(self_resource.divide_meta_tenant(meta_resource))) {
    LOG_WARN("divide meta tenant resource fail", KR(ret), K(config_));
  } else if (OB_FAIL(meta_config.init(
      config_.unit_config_id(),
      config_.name(),
      meta_resource))) {
    LOG_WARN("init meta config fail", KR(ret), K(config_), K(meta_resource));
  } else if (OB_FAIL(meta_tenant_config.init(
      gen_meta_tenant_id(tenant_id_),       // meta tenant ID
      unit_id_,
      unit_status_,
      meta_config,
      lib::Worker::CompatMode::MYSQL,       // always MYSQL mode
      create_timestamp_,
      has_memstore_,
      is_removed_))) {
    LOG_WARN("init meta tenant config fail", KR(ret), KPC(this), K(meta_config));
  }
  // update self unit resource
  else if (OB_FAIL(config_.update_unit_resource(self_resource))) {
    LOG_WARN("update unit resource fail", KR(ret), K(self_resource), K(config_));
  }

  LOG_INFO("divide meta tenant finish", KR(ret), K(meta_tenant_config), "user_config", *this);
  return ret;
}

void ObUnitInfoGetter::ObTenantConfig::reset()
{
  tenant_id_ = common::OB_INVALID_ID;
  unit_id_ = common::OB_INVALID_ID;
  config_.reset();
  mode_ = lib::Worker::CompatMode::INVALID;
  create_timestamp_ = 0;
  is_removed_ = false;
}

bool ObUnitInfoGetter::ObTenantConfig::operator==(const ObTenantConfig &other) const
{
  return (tenant_id_ == other.tenant_id_ &&
          unit_id_ == other.unit_id_ &&
          unit_status_ == other.unit_status_ &&
          config_ == other.config_ &&
          mode_ == other.mode_ &&
          create_timestamp_ == other.create_timestamp_ &&
          has_memstore_ == other.has_memstore_ &&
          is_removed_ == other.is_removed_);
}

int ObUnitInfoGetter::ObTenantConfig::assign(const ObUnitInfoGetter::ObTenantConfig &other)
{
  int ret = OB_SUCCESS;
  if (this == &other) {
    // skip
  } else if (OB_FAIL(config_.assign(other.config_))) {
    LOG_WARN("fail to assign config", KR(ret), K(other));
  } else {
    tenant_id_ = other.tenant_id_;
    unit_id_ = other.unit_id_;
    unit_status_ = other.unit_status_;
    mode_ = other.mode_;
    create_timestamp_ = other.create_timestamp_;
    has_memstore_ = other.has_memstore_;
    is_removed_ = other.is_removed_;
  }
  return ret;
}

ObUnitInfoGetter::ObUnitInfoGetter()
  : inited_(false),
    ut_operator_()
{
}

ObUnitInfoGetter::~ObUnitInfoGetter()
{
}

int ObUnitInfoGetter::init(ObMySQLProxy &proxy, common::ObServerConfig *config)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(ut_operator_.init(proxy))) {
    LOG_WARN("init unit table operator failed", K(ret));
  } else {
    inited_ = true;
  }
  return ret;
}

int ObUnitInfoGetter::get_tenants(common::ObIArray<uint64_t> &tenants)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ut_operator_.get_tenants(tenants))) {
    LOG_WARN("ut_operator get_resource_pools failed", K(ret));
  }
  return ret;
}

// only used by ObTenantNodeBalancer
int ObUnitInfoGetter::get_server_tenant_configs(const common::ObAddr &server,
                                                common::ObIArray<ObTenantConfig> &tenant_configs)
{
  int ret = OB_SUCCESS;
  ObArray<ObUnit> units;
  ObArray<ObUnitConfig> configs;
  ObArray<ObResourcePool> pools;
  ObArray<ObUnitInfo> unit_infos;
  tenant_configs.reuse();
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), KR(ret));
  } else if (OB_FAIL(get_units_of_server(server, units))) {
    LOG_WARN("get_units_of_server failed", K(server), KR(ret));
  } else if (units.count() <= 0) {
    // don't need to set ret, just return empty result
    LOG_WARN("no unit on server", K(server));
  } else if (OB_FAIL(get_pools_of_units(units, pools))) {
    LOG_WARN("get_pools_of_units failed", K(units), KR(ret));
  } else if (OB_FAIL(get_configs_of_pools(pools, configs))) {
    LOG_WARN("get_configs_of_pools failed", K(pools), KR(ret));
  } else if (OB_FAIL(build_unit_infos(units, configs, pools, unit_infos))) {
    LOG_WARN("build_unit_infos failed", K(units), K(configs), K(pools), KR(ret));
  } else {
    LOG_INFO("get_server_tenant_configs", K(unit_infos));

    ObTenantConfig tenant_config;
    ObTenantConfig meta_tenant_config;

    for (int64_t i = 0; OB_SUCC(ret) && i < unit_infos.count(); ++i) {
      common::ObArray<ObTenantConfig*> config_arr;

      const uint64_t tenant_id = unit_infos.at(i).pool_.tenant_id_;
      const uint64_t unit_id = unit_infos.at(i).unit_.unit_id_;

      tenant_config.reset();
      tenant_config.tenant_id_ = tenant_id;
      tenant_config.unit_id_ = unit_id;
      if (common::REPLICA_TYPE_LOGONLY == unit_infos.at(i).unit_.replica_type_) {
        // Logonly unit can hold logonly replicas only,
        // no need to reserve memory for memstore
        tenant_config.has_memstore_ = false;
      } else {
        tenant_config.has_memstore_ = true;
      }
      build_unit_stat(server, unit_infos.at(i).unit_, tenant_config.unit_status_);
      tenant_config.config_ = unit_infos.at(i).config_;
      if (OB_FAIL(get_compat_mode(tenant_id, tenant_config.mode_))) {
        LOG_WARN("failed to get compat mode", KR(ret), K(tenant_id));
      } else if (is_user_tenant(tenant_id)) {
        meta_tenant_config.reset();
        if (OB_FAIL(tenant_config.divide_meta_tenant(meta_tenant_config))) {
          LOG_WARN("divide_meta_tenant failed", KR(ret), K(tenant_config), K(tenant_id));
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(config_arr.push_back(&tenant_config))) {
          LOG_ERROR("config arr push back failed", KR(ret), K(tenant_id));
        } else {
          if (is_user_tenant(tenant_id)
             && OB_FAIL(config_arr.push_back(&meta_tenant_config))) {
            LOG_ERROR("config arr push back failed", KR(ret), K(tenant_id));
          }
        }
      }

      for (int64_t config_arr_idx = 0; OB_SUCC(ret) && config_arr_idx < config_arr.count(); config_arr_idx++) {
        ObTenantConfig* tc_ptr = config_arr.at(config_arr_idx);
        if (NULL == tc_ptr) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("tc_ptr is nullptr", K(tenant_configs), K(config_arr_idx), KR(ret));
          break;
        }

        int64_t idx = OB_INVALID_INDEX;
        if (OB_FAIL(find_tenant_config_idx(tenant_configs, tc_ptr->tenant_id_, idx))) {
          if (OB_ENTRY_NOT_EXIST != ret) {
            LOG_WARN("find_tenant_config_idx failed", K(tenant_configs), K(tc_ptr->tenant_id_), KR(ret));
          } else {
            ret = OB_SUCCESS;
            if (OB_FAIL(tenant_configs.push_back(*tc_ptr))) {
              LOG_WARN("push_back failed", KR(ret));
            }
          }
        } else if (OB_INVALID_INDEX == idx) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid index", K(idx), KR(ret));
        } else {
          tenant_configs.at(idx).config_ += tc_ptr->config_;
        }
      }
    }
  }

  return ret;
}

int ObUnitInfoGetter::get_tenant_server_configs(const uint64_t tenant_id,
                                                ObIArray<ObServerConfig> &server_configs)
{
  int ret = OB_SUCCESS;
  ObArray<ObUnit> units;
  ObArray<ObUnitConfig> configs;
  ObArray<ObResourcePool> pools;
  ObArray<ObUnitInfo> unit_infos;
  server_configs.reuse();
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(tenant_id), K(ret));
  } else if (OB_FAIL(get_pools_of_tenant(tenant_id, pools))) {
    LOG_WARN("get_pools_of_tenant failed", K(tenant_id), K(ret));
  } else if (pools.count() <= 0) {
    // don't need to set ret, just return empty result
    LOG_DEBUG("tenant doesn't own any pool, maybe tenant has been deleted", K(tenant_id));
  } else if (OB_FAIL(get_units_of_pools(pools, units))) {
    LOG_WARN("get_units_of_pools failed", K(pools), K(ret));
  } else if (OB_FAIL(get_configs_of_pools(pools, configs))) {
    LOG_WARN("get_configs_of_pools failed", K(pools), K(ret));
  } else if (OB_FAIL(build_unit_infos(units, configs, pools, unit_infos))) {
    LOG_WARN("build_unit_infos failed", K(units), K(configs), K(pools), K(ret));
  } else {
    ObServerConfig server_config;
    ObServerConfig migrate_from_server_config;
    for (int64_t i = 0; OB_SUCC(ret) && i < unit_infos.count(); ++i) {
      server_config.reset();
      server_config.server_ = unit_infos.at(i).unit_.server_;
      server_config.config_ = unit_infos.at(i).config_;
      if (OB_FAIL(add_server_config(server_config, server_configs))) {
        LOG_WARN("add_server_config failed", K(server_config), K(ret));
      } else {
        if (unit_infos.at(i).unit_.migrate_from_server_.is_valid()) {
          migrate_from_server_config.reset();
          migrate_from_server_config.server_ = unit_infos.at(i).unit_.migrate_from_server_;
          migrate_from_server_config.config_ = unit_infos.at(i).config_;
          if (OB_FAIL(add_server_config(migrate_from_server_config, server_configs))) {
            LOG_WARN("add_server_config failed", K(migrate_from_server_config), K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObUnitInfoGetter::get_tenant_servers(const uint64_t tenant_id,
                                         ObIArray<ObAddr> &servers)
{
  int ret = OB_SUCCESS;
  ObArray<ObUnit> units;
  ObArray<ObResourcePool> pools;
  servers.reuse();
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(tenant_id), K(ret));
  } else if (OB_FAIL(get_pools_of_tenant(tenant_id, pools))) {
    LOG_WARN("get_pools_of_tenant failed", K(tenant_id), K(ret));
  } else if (pools.count() <= 0) {
    // don't need to set ret, just return empty result
    LOG_WARN("tenant doesn't own any pool, maybe tenant has been deleted", K(tenant_id));
  } else if (OB_FAIL(get_units_of_pools(pools, units))) {
    LOG_WARN("get_units_of_pools failed", K(pools), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < units.count(); ++i) {
      const ObUnit &unit = units.at(i);
      bool server_exist = has_exist_in_array(servers, unit.server_);
      if (!server_exist) {
        if (OB_FAIL(servers.push_back(unit.server_))) {
          LOG_WARN("push_back failed", K(ret));
        }
      }

      if (OB_SUCCESS == ret && unit.migrate_from_server_.is_valid()) {
        server_exist = has_exist_in_array(servers, unit.migrate_from_server_);
        if (!server_exist) {
          if (OB_FAIL(servers.push_back(unit.migrate_from_server_))) {
            LOG_WARN("push_back failed", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObUnitInfoGetter::check_tenant_small(const uint64_t tenant_id, bool &small_tenant)
{
  int ret = OB_SUCCESS;
  small_tenant = true;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(tenant_id), K(ret));
  } else {
    ObArray<ObResourcePool> pools;
    if (OB_FAIL(get_pools_of_tenant(tenant_id, pools))) {
      LOG_WARN("get_pools_of_tenant failed", K(tenant_id), K(ret));
    } else if (pools.count() <= 0) {
      ret = OB_TENANT_NOT_EXIST;
      LOG_WARN("pools of tenant not exist", K(tenant_id), K(ret));
    } else if (1 == pools.count()) {
      if (pools.at(0).unit_count_ < 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("find pool's unit_count small than one", "pool", pools.at(0), K(ret));
      } else {
        small_tenant = (1 == pools.at(0).unit_count_);
      }
    } else {
      small_tenant = false;
    }
  }
  return ret;
}

int ObUnitInfoGetter::get_sys_unit_count(int64_t &sys_unit_cnt)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(ut_operator_.get_sys_unit_count(sys_unit_cnt))) {
    LOG_WARN("ut_operator get sys unit count failed", KR(ret));
  }
  return ret;
}

int ObUnitInfoGetter::get_units_of_server(const ObAddr &server,
                                          ObIArray<ObUnit> &units)
{
  int ret = OB_SUCCESS;
  units.reuse();
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), K(ret));
  } else if (OB_FAIL(ut_operator_.get_units(server, units))) {
    LOG_WARN("ut_operator get_units failed", K(server), K(ret));
  }
  return ret;
}

int ObUnitInfoGetter::get_pools_of_units(const ObIArray<ObUnit> &units,
                                         ObIArray<ObResourcePool> &pools)
{
  int ret = OB_SUCCESS;
  pools.reuse();
  ObArray<uint64_t> pool_ids;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (units.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("units is empty", K(units), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < units.count(); ++i) {
      int64_t j = 0;
      for ( ; j < pool_ids.count(); ++j) {
        if (pool_ids.at(j) == units.at(i).resource_pool_id_) {
          break;
        }
      }
      if (j == pool_ids.count()) {
        // not exist in pool_ids, push_back
        if (OB_FAIL(pool_ids.push_back(units.at(i).resource_pool_id_))) {
          LOG_WARN("push_back failed", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (pool_ids.count() <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pool_ids is empty", K(pool_ids), K(ret));
      } else if (OB_FAIL(ut_operator_.get_resource_pools(pool_ids, pools))) {
        LOG_WARN("ut_operator get_resource_pools failed", K(pool_ids), K(ret));
      }
    }
  }
  return ret;
}

int ObUnitInfoGetter::get_configs_of_pools(const ObIArray<ObResourcePool> &pools,
                                           ObIArray<ObUnitConfig> &configs)
{
  int ret = OB_SUCCESS;
  configs.reuse();
  ObArray<uint64_t> unit_config_ids;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (pools.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("pools is empty", K(pools), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < pools.count(); ++i) {
      int64_t j = 0;
      for ( ; j < unit_config_ids.count(); ++j) {
        if (unit_config_ids.at(j) == pools.at(i).unit_config_id_) {
          break;
        }
      }
      if (j == unit_config_ids.count()) {
        // not exist in unit_config_ids, push_back
        if (OB_FAIL(unit_config_ids.push_back(pools.at(i).unit_config_id_))) {
          LOG_WARN("push_back failed", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (unit_config_ids.count() <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit_config_ids is empty", K(unit_config_ids), K(ret));
      } else if (OB_FAIL(ut_operator_.get_unit_configs(unit_config_ids, configs))) {
        LOG_WARN("ut_operator_ get_unit_configs failed", K(unit_config_ids), K(ret));
      }
    }
  }
  return ret;
}

int ObUnitInfoGetter::get_pools_of_tenant(const uint64_t tenant_id,
                                          ObIArray<ObResourcePool> &pools)
{
  int ret = OB_SUCCESS;
  pools.reuse();
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(ret));
  } else if (OB_FAIL(ut_operator_.get_resource_pools(tenant_id, pools))) {
    LOG_WARN("ut_operator get_resource_pools failed", K(tenant_id), K(ret));
  }
  return ret;
}

int ObUnitInfoGetter::get_units_of_pools(const ObIArray<ObResourcePool> &pools,
                                         ObIArray<ObUnit> &units)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> pool_ids;
  units.reuse();
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (pools.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("pools is empty", K(pools), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < pools.count(); ++i) {
      if (OB_FAIL(pool_ids.push_back(pools.at(i).resource_pool_id_))) {
        LOG_WARN("push_back failed", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (pool_ids.count() <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pool_ids is empty", K(pool_ids), K(ret));
      } else if (OB_FAIL(ut_operator_.get_units(pool_ids, units))) {
        LOG_WARN("get_units failed", K(pool_ids), K(ret));
      }
    }
  }
  return ret;
}

int ObUnitInfoGetter::build_unit_infos(const ObIArray<ObUnit> &units,
                                       const ObIArray<ObUnitConfig> &configs,
                                       const ObIArray<ObResourcePool> &pools,
                                       ObIArray<ObUnitInfo> &unit_infos) const
{
  int ret = OB_SUCCESS;
  unit_infos.reuse();
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (units.count() <= 0 || configs.count() <= 0 || pools.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(units), K(configs), K(pools), K(ret));
  } else {
    ObUnitInfo info;
    for (int64_t i = 0; OB_SUCC(ret) && i < units.count(); ++i) {
      int64_t pool_index = OB_INVALID_INDEX;
      int64_t config_index = OB_INVALID_INDEX;
      if (OB_FAIL(find_pool_idx(pools, units.at(i).resource_pool_id_, pool_index))) {
        LOG_WARN("find_pool_idx failed", K(pools), "resource_pool_id",
            units.at(i).resource_pool_id_, K(ret));
      } else if (OB_INVALID_INDEX == pool_index) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pool_index is invalid", K(pool_index), K(ret));
      } else if (OB_FAIL(find_config_idx(configs,
          pools.at(pool_index).unit_config_id_, config_index))) {
        LOG_WARN("find_config_idx", K(configs), "unit_config_id",
            pools.at(pool_index).unit_config_id_, K(ret));
      } else if (OB_INVALID_INDEX == config_index) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("config_index is invalid", K(config_index), K(ret));
      } else if (OB_INVALID_ID == pools.at(pool_index).tenant_id_) {
        // ignore unit not grant to any tenant
        continue;
      } else {
        info.reset();
        info.unit_ = units.at(i);
        info.config_ = configs.at(config_index);
        if (OB_FAIL(info.pool_.assign(pools.at(pool_index)))) {
          LOG_WARN("failed to assign info.pool_", K(ret));
        } else if (OB_FAIL(unit_infos.push_back(info))) {
          LOG_WARN("push_back failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObUnitInfoGetter::add_server_config(const ObServerConfig &server_config,
                                        ObIArray<ObServerConfig> &server_configs)
{
  int ret = OB_SUCCESS;
  int64_t idx = OB_INVALID_INDEX;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!server_config.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server_config", K(server_config), K(ret));
  } else if (OB_FAIL(find_server_config_idx(server_configs, server_config.server_, idx))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("find_server_config_idx failed", K(server_configs),
          "server", server_config.server_, K(ret));
    } else {
      ret = OB_SUCCESS;
      if (OB_FAIL(server_configs.push_back(server_config))) {
        LOG_WARN("push_back failed", K(ret));
      }
    }
  } else if (OB_INVALID_INDEX == idx) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid index", K(idx), K(ret));
  } else {
    server_configs.at(idx).config_ += server_config.config_;
  }
  return ret;
}

int ObUnitInfoGetter::find_pool_idx(const ObIArray<ObResourcePool> &pools,
                                    const uint64_t pool_id, int64_t &index) const
{
  int ret = OB_SUCCESS;
  index = OB_INVALID_INDEX;
  // pools can be empty
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == pool_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(pool_id), K(ret));
  } else {
    bool found = false;
    for (int64_t i = 0; !found && i < pools.count(); ++i) {
      if (pools.at(i).resource_pool_id_ == pool_id) {
        index = i;
        found = true;
      }
    }
    if (!found) {
      ret = OB_ENTRY_NOT_EXIST;
    }
  }
  return ret;
}

int ObUnitInfoGetter::find_config_idx(const ObIArray<ObUnitConfig> &configs,
                                      const uint64_t config_id, int64_t &index) const
{
  int ret = OB_SUCCESS;
  index = OB_INVALID_INDEX;
  // configs can be empty
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == config_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(config_id), K(ret));
  } else {
    bool found = false;
    for (int64_t i = 0; !found && i < configs.count(); ++i) {
      if (configs.at(i).unit_config_id() == config_id) {
        index = i;
        found = true;
      }
    }
    if (!found) {
      ret = OB_ENTRY_NOT_EXIST;
    }
  }
  return ret;
}

int ObUnitInfoGetter::find_tenant_config_idx(const ObIArray<ObTenantConfig> &tenant_configs,
                                             const uint64_t tenant_id, int64_t &index) const
{
  int ret = OB_SUCCESS;
  index = OB_INVALID_INDEX;
  // tenant_configs can be empty
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(ret));
  } else {
    bool found = false;
    for (int64_t i = 0; !found && i < tenant_configs.count(); ++i) {
      if (tenant_configs.at(i).tenant_id_ == tenant_id) {
        index = i;
        found = true;
      }
    }
    if (!found) {
      ret = OB_ENTRY_NOT_EXIST;
    }
  }
  return ret;
}

int ObUnitInfoGetter::find_server_config_idx(const ObIArray<ObServerConfig> &server_configs,
                                             const ObAddr &server, int64_t &index) const
{
  int ret = OB_SUCCESS;
  index = OB_INVALID_INDEX;
  // server_configs can be empty
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(server), K(ret));
  } else {
    bool found = false;
    for (int64_t i = 0; !found && i < server_configs.count(); ++i) {
      if (server_configs.at(i).server_ == server) {
        index = i;
        found = true;
      }
    }
    if (!found) {
      ret = OB_ENTRY_NOT_EXIST;
    }
  }
  return ret;
}

void ObUnitInfoGetter::build_unit_stat(const ObAddr &server,
                                       const ObUnit &unit,
                                       ObUnitStatus &unit_stat) const
{
  unit_stat = UNIT_NORMAL;
  if (unit.migrate_from_server_.is_valid()) {
    if (server == unit.server_) {
      unit_stat = UNIT_MIGRATE_IN;
    } else if (server == unit.migrate_from_server_) {
      unit_stat = UNIT_MIGRATE_OUT;
    } else {
      unit_stat = UNIT_ERROR_STAT;
    }
  } else if (ObUnit::UNIT_STATUS_DELETING == unit.status_) {
    unit_stat = UNIT_MARK_DELETING;
  }
}

int ObUnitInfoGetter::get_compat_mode(const int64_t tenant_id, lib::Worker::CompatMode &compat_mode) const
{
  int ret = OB_SUCCESS;
  if (is_virtual_tenant_id(tenant_id)
      || is_sys_tenant(tenant_id)
      || is_meta_tenant(tenant_id)) {
    compat_mode = lib::Worker::CompatMode::MYSQL;
  } else {
    while (OB_FAIL(ObCompatModeGetter::get_tenant_mode(tenant_id, compat_mode))) {
      if (OB_TENANT_NOT_EXIST != ret || THIS_WORKER.is_timeout()) {
        const bool is_timeout = THIS_WORKER.is_timeout();
        ret = OB_TIMEOUT;
        LOG_WARN("get tenant compatibility mode fail", K(ret), K(tenant_id), K(is_timeout));
        break;
      } else {
        ob_usleep(200 * 1000L);
      }
    }
    if (OB_SUCC(ret)) {
      LOG_INFO("jx_debug: get tenant compatibility mode", K(tenant_id), K(compat_mode));
    }
  }
  return ret;
}

}//end namespace share
}//end namespace oceanbase
