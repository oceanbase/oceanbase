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

#ifndef OCEANBASE_SHARE_OB_UNIT_TABLE_OPERATOR_H_
#define OCEANBASE_SHARE_OB_UNIT_TABLE_OPERATOR_H_

#include "lib/string/ob_sql_string.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "share/unit/ob_unit_info.h"
#include "share/ob_unit_stat.h"

namespace oceanbase
{
namespace common
{
class ObServerConfig;
} // end namespace common
namespace share
{
class ObDMLSqlSplicer;
class ObUnitTableTransaction : public common::ObMySQLTransaction
{
public:
  ObUnitTableTransaction() : ObMySQLTransaction() {}
  virtual ~ObUnitTableTransaction();
  virtual int start(ObISQLClient *proxy,
                    const uint64_t tenant_id,
                    bool with_snapshot = false,
                    const int32_t group_id = 0) override;
  static int lock_service_epoch(common::ObMySQLTransaction &trans);
};

struct ObUnitUGOp
{
  ObUnitUGOp() : unit_id_(0), old_unit_group_id_(OB_INVALID_ID), new_unit_group_id_(OB_INVALID_ID) {}
  ObUnitUGOp(uint64_t unit_id, uint64_t new_unit_group_id)
   : unit_id_(unit_id), old_unit_group_id_(OB_INVALID_ID), new_unit_group_id_(new_unit_group_id) {}
  ObUnitUGOp(uint64_t unit_id, uint64_t old_unit_group_id, uint64_t new_unit_group_id)
   : unit_id_(unit_id), old_unit_group_id_(old_unit_group_id), new_unit_group_id_(new_unit_group_id) {}
  ~ObUnitUGOp() {}
  int assign(const ObUnitUGOp &other);
  bool is_valid() const
  {
    return unit_id_ > 0 && is_valid_id(unit_id_)
        && old_unit_group_id_ >= 0
        && new_unit_group_id_ >= 0 && is_valid_id(new_unit_group_id_)
        && old_unit_group_id_ != new_unit_group_id_;
  }
  bool operator==(const ObUnitUGOp &other) const
  {
    return unit_id_ == other.unit_id_ && old_unit_group_id_ == other.old_unit_group_id_
      && new_unit_group_id_ == other.new_unit_group_id_;
  }
  uint64_t get_unit_id() const { return unit_id_; }
  uint64_t get_old_unit_group_id() const { return old_unit_group_id_; }
  uint64_t get_new_unit_group_id() const { return new_unit_group_id_; }
  TO_STRING_KV(K_(unit_id), K_(old_unit_group_id), K_(new_unit_group_id));
private:
  uint64_t unit_id_;
  // if old_unit_group_id is OB_INVALID_ID, means do not care old_unit_group_id, just set new_unit_id
  uint64_t old_unit_group_id_;
  uint64_t new_unit_group_id_;
};

class ObUnitTableOperator
{
public:
  ObUnitTableOperator();
  virtual ~ObUnitTableOperator();

  int init(common::ObISQLClient &proxy);
  // return unit count of sys resource pool
  virtual int get_sys_unit_count(int64_t &cnt) const;
  virtual int get_units(common::ObIArray<ObUnit> &units) const;
  virtual int get_units(const common::ObAddr &server,
                        common::ObIArray<ObUnit> &units) const;
  int get_units_by_pool_id(
      common::ObISQLClient &client,
      const uint64_t pool_id,
      common::ObIArray<ObUnit> &units) const;
  virtual int get_units(const common::ObIArray<uint64_t> &pool_ids,
                        common::ObIArray<ObUnit> &units) const;
  virtual int insert_unit(common::ObISQLClient &client,
                          const ObUnit &unit);
  // Following two functions about updating unit are used by unit_manager, and ug_id is updated
  //   separately from other columns because ug_id in unit_manager memory is not accurate.
  //   Refer to more details in ObUnitManager.
  virtual int update_unit_exclude_ug_id(common::ObISQLClient &client,
                          const ObUnit &unit);
  virtual int update_unit_ug_id(common::ObISQLClient &client, const ObUnitUGOp &op);
  virtual int remove_units(common::ObISQLClient &client,
                           const uint64_t resource_pool_id);
  virtual int remove_unit(common::ObISQLClient &client,
                          const ObUnit &unit);

  virtual int remove_units_in_zones(common::ObISQLClient &client,
                                    const uint64_t resource_pool_id,
                                    const common::ObIArray<common::ObZone> &to_be_removed_zones);
  virtual int get_resource_pools(common::ObIArray<ObResourcePool> &pools) const;
  virtual int get_resource_pools(const uint64_t tenant_id,
                                 common::ObIArray<ObResourcePool> &pools) const;
  virtual int get_resource_pools(const common::ObIArray<uint64_t> &pool_ids,
                                 common::ObIArray<ObResourcePool> &pools) const;
  int get_resource_pool(ObISQLClient &sql_client,
                        const uint64_t pool_id,
                        const bool select_for_update,
                        ObResourcePool &resource_pool) const;
  virtual int update_resource_pool(common::ObISQLClient &client,
                                   const ObResourcePool &resource_pool);
  virtual int remove_resource_pool(common::ObISQLClient &client,
                                   const uint64_t resource_pool_id);

  virtual int get_unit_configs(common::ObIArray<ObUnitConfig> &configs) const;
  virtual int get_unit_configs(const common::ObIArray<uint64_t> &config_ids,
                               common::ObIArray<ObUnitConfig> &configs) const;
  virtual int update_unit_config(common::ObISQLClient &client,
                                 const ObUnitConfig &config);
  virtual int remove_unit_config(common::ObISQLClient &client,
                                 const uint64_t unit_config_id);
  virtual int get_tenants(common::ObIArray<uint64_t> &tenants) const;
  virtual int get_unit_config_by_name(const common::ObString &unit_name,
                                      ObUnitConfig &unit_config);

  int get_units_by_unit_group_id(const uint64_t unit_group_id,
                                     common::ObIArray<ObUnit> &units);
  int get_units_by_unit_group_id(
      const uint64_t tenant_id,
      const uint64_t unit_group_id,
      common::ObIArray<ObUnit> &units) const;

  // get unit in specific unit_group and zone, if such a unit does not exist,
  // then return ret == OB_ENTRY_NOT_EXIST
  // @param [in] unit_group_id, target unit_group_id
  // @param [in] zone, target zone
  // @param [out] unit, unit in specific unit_group_id and zone
  int get_unit_in_group(const uint64_t unit_group_id,
                        const common::ObZone &zone,
                        share::ObUnit &unit);

  int get_units_by_resource_pool_ids(const ObIArray<uint64_t> &pool_ids,
                                      common::ObIArray<ObUnit> &units) const;
  int get_units_by_resource_pools(const ObIArray<share::ObResourcePoolName> &pools,
                                      common::ObIArray<ObUnit> &units);
  int get_units_by_tenant(const uint64_t tenant_id,
                          common::ObIArray<ObUnit> &units) const;
  int get_units_by_unit_ids(const ObIArray<uint64_t> &unit_ids,
                            common::ObIArray<ObUnit> &units);
  int get_pool_unit_group_ids(common::ObISQLClient &client,
                              const uint64_t resource_pool_id,
                              ObIArray<uint64_t> &unit_group_ids) const;
  int check_tenant_has_logonly_pools(
      const uint64_t &tenant_id,
      bool &has_logonly_pools) const;
  int check_has_logonly_pools(
      const ObIArray<share::ObResourcePoolName> &pools,
      bool &has_logonly_pools) const;

  virtual int get_unit_stats(common::ObIArray<ObUnitStat> &unit_stats) const;
  virtual int check_server_empty(const common::ObAddr &server, bool &is_empty);
private:
  int construct_unit_dml_(const ObUnit &unit, const bool include_ug_id, share::ObDMLSqlSplicer &dml);
  static int zone_list2str(const common::ObIArray<common::ObZone> &zone_list,
                           char *str, const int64_t buf_size);
  static int str2zone_list(const char *str,
                           common::ObIArray<common::ObZone> &zone_list);

  int read_unit(const common::sqlclient::ObMySQLResult &result, ObUnit &unit) const;
  int read_units(common::ObSqlString &sql,
                 common::ObIArray<ObUnit> &units) const;
  int read_unit_config(const common::sqlclient::ObMySQLResult &result,
                       ObUnitConfig &unit_config) const;
  int read_unit_configs(common::ObSqlString &sql,
                        common::ObIArray<ObUnitConfig> &units) const;
  int read_resource_pool(const common::sqlclient::ObMySQLResult &result,
                         ObResourcePool &resource_pool) const;
  int read_resource_pools(common::ObSqlString &sql,
                          common::ObIArray<ObResourcePool> &resource_pools) const;
  int read_tenant(const common::sqlclient::ObMySQLResult &result, uint64_t &tenant_id) const;
  int read_tenants(common::ObSqlString &sql,
                   common::ObIArray<uint64_t> &tenants) const;
  int read_unit_stat(const common::sqlclient::ObMySQLResult &result, ObUnitStat &unit_stat) const;
  int read_unit_stats(common::ObSqlString &sql,
                 common::ObIArray<ObUnitStat> &unit_stats) const;
  int inner_check_has_logonly_pool_(
      const common::ObIArray<ObResourcePool> &pools,
      bool &has_logonly_pools) const;
private:
  bool inited_;
  common::ObISQLClient *proxy_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObUnitTableOperator);
};
}//end namespace share
}//end namespace oceanbase

#endif //OCEANBASE_SHARE_OB_UNIT_TABLE_OPERATOR_H_
