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

#ifndef OCEANBASE_SHARE_OB_UNIT_GETTER_H_
#define OCEANBASE_SHARE_OB_UNIT_GETTER_H_

#include "lib/container/ob_array.h"
#include "share/partition_table/ob_partition_table_operator.h"
#include "share/ob_unit_table_operator.h"
#include "share/ob_unit_stat_table_operator.h"
#include "share/ob_unit_stat.h"
#include "share/ob_check_stop_provider.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase {
namespace common {
class ObMySQLProxy;
class ObServerConfig;
}  // namespace common

namespace share {
class ObUnitInfoGetter {
public:
  enum ObUnitStatus {
    UNIT_NORMAL = 0,
    UNIT_MIGRATE_IN,
    UNIT_MIGRATE_OUT,
    UNIT_DELETING,
    UNIT_ERROR_STAT,
  };
  struct ObTenantConfig {
    OB_UNIS_VERSION(1);

  public:
    ObTenantConfig()
        : tenant_id_(common::OB_INVALID_ID),
          unit_stat_(UNIT_ERROR_STAT),
          config_(),
          mode_(ObWorker::CompatMode::INVALID),
          create_timestamp_(0),
          has_memstore_(true),
          is_removed_(false)
    {}
    ~ObTenantConfig()
    {}

    void reset()
    {
      tenant_id_ = common::OB_INVALID_ID;
      config_.reset();
      mode_ = ObWorker::CompatMode::INVALID;
      create_timestamp_ = 0;
      is_removed_ = false;
    }
    bool operator==(const ObTenantConfig& other) const
    {
      return tenant_id_ == other.tenant_id_ && unit_stat_ == other.unit_stat_ && config_ == other.config_ &&
             mode_ == other.mode_ && create_timestamp_ == other.create_timestamp_ &&
             has_memstore_ == other.has_memstore_ && is_removed_ == other.is_removed_;
    }
    TO_STRING_KV(
        K_(tenant_id), K_(has_memstore), K_(unit_stat), K_(config), K_(mode), K_(create_timestamp), K_(is_removed));

    uint64_t tenant_id_;
    ObUnitStatus unit_stat_;
    ObUnitConfig config_;
    ObWorker::CompatMode mode_;
    int64_t create_timestamp_;
    bool has_memstore_;  // make if the unit contains replicas have memstore(Logonly replicas have no memstore)
    bool is_removed_;
  };

  struct ObServerConfig {
    ObServerConfig() : server_(), config_()
    {}
    ~ObServerConfig()
    {}

    bool is_valid() const
    {
      return server_.is_valid() && config_.is_valid();
    }
    void reset()
    {
      server_.reset();
      config_.reset();
    }
    TO_STRING_KV(K_(server), K_(config));

    common::ObAddr server_;
    ObUnitConfig config_;
  };

  ObUnitInfoGetter();
  virtual ~ObUnitInfoGetter();
  int init(common::ObMySQLProxy& proxy, common::ObServerConfig* config = NULL);
  virtual int get_tenants(common::ObIArray<uint64_t>& tenants);
  virtual int get_server_tenant_configs(const common::ObAddr& server, common::ObIArray<ObTenantConfig>& tenant_configs);
  virtual int get_tenant_server_configs(const uint64_t tenant_id, common::ObIArray<ObServerConfig>& server_configs);
  virtual int get_tenant_servers(const uint64_t tenant_id, common::ObIArray<common::ObAddr>& servers);
  virtual int check_tenant_small(const uint64_t tenant_id, bool& small_tenant);
  int get_pools_of_tenant(const uint64_t tenant_id, common::ObIArray<ObResourcePool>& pools);

private:
  int get_units_of_server(const common::ObAddr& server, common::ObIArray<ObUnit>& units);
  int get_pools_of_units(const common::ObIArray<ObUnit>& units, common::ObIArray<ObResourcePool>& pools);
  int get_configs_of_pools(const common::ObIArray<ObResourcePool>& pools, common::ObIArray<ObUnitConfig>& configs);
  int get_units_of_pools(const common::ObIArray<ObResourcePool>& pools, common::ObIArray<ObUnit>& units);

  int build_unit_infos(const common::ObIArray<ObUnit>& units, const common::ObIArray<ObUnitConfig>& configs,
      const common::ObIArray<ObResourcePool>& pools, common::ObIArray<ObUnitInfo>& unit_infos) const;

  int add_server_config(const ObServerConfig& server_config, common::ObIArray<ObServerConfig>& server_configs);
  int find_pool_idx(const common::ObIArray<ObResourcePool>& pools, const uint64_t pool_id, int64_t& index) const;
  int find_config_idx(const common::ObIArray<ObUnitConfig>& configs, const uint64_t config_id, int64_t& index) const;
  int find_tenant_config_idx(
      const common::ObIArray<ObTenantConfig>& tenant_configs, const uint64_t tenant_id, int64_t& index) const;
  int find_server_config_idx(
      const common::ObIArray<ObServerConfig>& server_configs, const common::ObAddr& server, int64_t& index) const;
  void build_unit_stat(const common::ObAddr& server, const ObUnit& unit, ObUnitStatus& unit_stat) const;

  int get_compat_mode(const int64_t tenant_id, ObWorker::CompatMode& compat_mode) const;

private:
  bool inited_;
  ObUnitTableOperator ut_operator_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObUnitInfoGetter);
};

typedef common::ObSEArray<ObUnitInfoGetter::ObTenantConfig, 16> TenantUnits;

class ObUnitStatGetter {
public:
  ObUnitStatGetter();
  virtual ~ObUnitStatGetter();
  int init(share::ObPartitionTableOperator& pt_operator, share::schema::ObMultiVersionSchemaService& schema_service,
      share::ObCheckStopProvider& check_stop_provider);
  virtual int get_unit_stat(uint64_t tenant_id, uint64_t unit_id, ObUnitStat& unit_stat) const;
  virtual int get_unit_stat(uint64_t tenant_id, share::ObUnitStatMap& unit_stat_map) const;

private:
  bool inited_;
  ObUnitStatTableOperator ut_stat_operator_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObUnitStatGetter);
};

}  // end namespace share
}  // end namespace oceanbase

#endif  // OCEANBASE_SHARE_OB_UNIT_GETTER_H_
