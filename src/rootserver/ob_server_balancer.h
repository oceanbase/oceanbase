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

#ifndef _OB_SERVER_BALANCER_H
#define _OB_SERVER_BALANCER_H 1
#include "ob_unit_manager.h"
#include "ob_unit_stat_manager.h"
namespace oceanbase
{
namespace rootserver
{
template <typename T>
class Matrix
{
public:
  Matrix()
    : inner_array_(),
      row_count_(0),
      column_count_(0),
      is_inited_(false) {}
  virtual ~Matrix() {}
  int init(const int64_t row_count, const int64_t column_count);
public:
  int set(const int64_t row, const int64_t column, const T &element);
  int get(const int64_t row, const int64_t column, T &element) const;
  const T *get(const int64_t row, const int64_t column) const;
  T *get(const int64_t row, const int64_t column);
  int64_t get_row_count() const;
  int64_t get_column_count() const;
  int64_t get_element_count() const;
  bool is_valid() const;
  bool is_contain(const T &element) const;
  template <typename Func>
  int sort_column_group(
      const int64_t start_column_idx,
      const int64_t column_count,
      Func &func);
  TO_STRING_KV(K(is_inited_), K(row_count_), K(column_count_), K(inner_array_));
private:
  ObArray<T> inner_array_;
  int64_t row_count_;
  int64_t column_count_;
  bool is_inited_;
};

template <typename T>
int Matrix<T>::init(
    const int64_t row_count,
    const int64_t column_count)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    RS_LOG(WARN, "matrix init twice", K(ret));
  } else if (OB_UNLIKELY(row_count <= 0
                         || column_count <= 0
                         || row_count >= INT32_MAX
                         || column_count >= INT32_MAX)) {
    ret = common::OB_ERR_UNEXPECTED;
    RS_LOG(WARN, "row or column count invalid", K(ret), K(row_count), K(column_count));
  } else {
    row_count_ = row_count;
    column_count_ = column_count;
    int64_t product = row_count_ * column_count_;
    inner_array_.reset();
    T pure_element;
    for (int64_t i = 0; OB_SUCC(ret) && i < product; ++i) {
      if (OB_FAIL(inner_array_.push_back(pure_element))) {
        RS_LOG(WARN, "fail to push back", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }
  return ret;
}

template <typename T>
int Matrix<T>::set(
    const int64_t row,
    const int64_t column,
    const T &element)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    RS_LOG(WARN, "matrix not init", K(ret));
  } else if (OB_UNLIKELY(row < 0
                         || column < 0
                         || row >= row_count_
                         || column >= column_count_)) {
    ret = common::OB_INVALID_ARGUMENT;
    RS_LOG(WARN, "invalid argument", K(ret), K(row), K(column), K(row_count_), K(column_count_));
  } else {
    const int64_t index = column + row * column_count_;
    inner_array_.at(index) = element;
  }
  return ret;
}

template <typename T>
int Matrix<T>::get(
    const int64_t row,
    const int64_t column,
    T &element) const
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    RS_LOG(WARN, "matrix not init", K(ret));
  } else if (OB_UNLIKELY(row < 0
                         || column < 0
                         || row >= row_count_
                         || column >= column_count_)) {
    ret = common::OB_INVALID_ARGUMENT;
    RS_LOG(WARN, "invalid argument", K(ret), K(row), K(column), K(row_count_), K(column_count_));
  } else {
    const int64_t index = column + row * column_count_;
    element = inner_array_.at(index);
  }
  return ret;
}

template <typename T>
const T *Matrix<T>::get(const int64_t row, const int64_t column) const
{
  const T *ptr_ret = NULL;
  int err = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    err = common::OB_NOT_INIT;
    RS_LOG_RET(WARN, err, "matrix not init", K(err));
  } else if (OB_UNLIKELY(row < 0
                         || column < 0
                         || row >= row_count_
                         || column >= column_count_)) {
    err = common::OB_INVALID_ARGUMENT;
    RS_LOG_RET(WARN, err, "invalid argument", K(err), K(row), K(column), K(row_count_), K(column_count_));
  } else {
    const int64_t index = column + row * column_count_;
    ptr_ret = &inner_array_.at(index);
  }
  return ptr_ret;
}

template <typename T>
T *Matrix<T>::get(const int64_t row, const int64_t column)
{
  T *ptr_ret = NULL;
  int err = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    err = common::OB_NOT_INIT;
    RS_LOG_RET(WARN, err, "matrix not init", K(err));
  } else if (OB_UNLIKELY(row < 0
                         || column < 0
                         || row >= row_count_
                         || column >= column_count_)) {
    err = common::OB_INVALID_ARGUMENT;
    RS_LOG_RET(WARN, err, "invalid argument", K(err), K(row), K(column), K(row_count_), K(column_count_));
  } else {
    const int64_t index = column + row * column_count_;
    ptr_ret = &inner_array_.at(index);
  }
  return ptr_ret;
}

template <typename T>
int64_t Matrix<T>::get_row_count() const
{
  return row_count_;
}

template <typename T>
int64_t Matrix<T>::get_column_count() const
{
  return column_count_;
}

template <typename T>
int64_t Matrix<T>::get_element_count() const
{
  return inner_array_.count();
}

template <typename T>
bool Matrix<T>::is_valid() const
{
  return is_inited_
         && (row_count_ > 0 && row_count_ < INT32_MAX)
         && (column_count_ > 0 && column_count_ < INT32_MAX)
         && (inner_array_.count() == row_count_ * column_count_);
}

template <typename T>
bool Matrix<T>::is_contain(const T &element) const
{
  return common::has_exist_in_array(inner_array_, element);
}

// Column_count elements starting from start_column_idx in each row,
// Sort according to the rules specified by Func operator
template <typename T>
template <typename Func>
int Matrix<T>::sort_column_group(
    const int64_t start_column_idx,
    const int64_t column_count,
    Func &func)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    RS_LOG(WARN, "not init", K(ret));
  } else if (OB_UNLIKELY(start_column_idx < 0
        || column_count <= 0
        || start_column_idx + column_count > column_count_)) {
    ret = common::OB_INVALID_ARGUMENT;
    RS_LOG(WARN, "invalid argument", K(ret));
  } else {
    for (int64_t row = 0; OB_SUCC(ret) && row < row_count_; ++row) {
      const int64_t end_column_idx = start_column_idx + column_count;
      std::sort(inner_array_.begin() + row * column_count_ + start_column_idx,
                inner_array_.begin() + row * column_count_ + end_column_idx,
                func);
      if (OB_FAIL(func.get_ret())) {
        ret = common::OB_ERR_UNEXPECTED;
        RS_LOG(WARN, "sort error", K(ret));
      } else {} // good, go on to sort the next row
    }
  }
  return ret;
}

class ObZoneManager;
// To balance the load of servers by migrating units.
class ObServerBalancer
{
public:
  struct UnitMigrateStat
  {
    UnitMigrateStat() : tenant_id_(common::OB_INVALID_ID),
                        original_pos_(),
                        arranged_pos_(),
                        unit_load_() {}
    uint64_t tenant_id_; // tenant_id_ may invalid, The pool hasn't granted the tenant yet
    common::ObAddr original_pos_; 
    // The address of the server where the unit is located before the balance starts
    common::ObAddr arranged_pos_; 
    // During the equalization process, the position where the unit is arranged, 
    // the initial value is the same as original_pos_
    ObUnitManager::ObUnitLoad unit_load_; // unit_ info
    TO_STRING_KV(K(tenant_id_), K(original_pos_), K(arranged_pos_), K(unit_load_));
  };
public:
  ObServerBalancer();
  virtual ~ObServerBalancer();

  int init(share::schema::ObMultiVersionSchemaService &schema_service,
           ObUnitManager &unit_manager,
           ObZoneManager &zone_mgr,
           ObServerManager &server_mgr,
           common::ObMySQLProxy &sql_proxy);
  int build_active_servers_resource_info();
  // 1. migrate units to balance the load
  // 2. migrate units from offline servers
  int balance_servers();

  int tenant_group_balance();

  int check_tenant_group_config_legality(
      common::ObIArray<ObTenantGroupParser::TenantNameGroup> &tenant_groups,
      bool &legal);
private:
  bool check_inner_stat() const { return inited_; }
  // distribute for server online/permanent_offline/migrate_in_blocked
  int distribute_for_server_status_change();
  int check_has_unit_in_migration(
      const common::ObIArray<ObUnitManager::ObUnitLoad> *unit_load_array,
      bool &has_unit_in_migration);
  int distribute_by_tenant(const uint64_t tenant_id,
                           const common::ObArray<share::ObResourcePool *> &pools);
  // distribute by pool, for pools not granted to tenant
  int distribute_by_pool(share::ObResourcePool *pool);
  int distribute_for_migrate_in_blocked(const share::ObUnitInfo &unit_info);
  int distribute_zone_unit(const ObUnitManager::ZoneUnit &zone_unit);
  int distribute_for_active(
      const share::ObServerInfoInTable &server_info,
      const share::ObUnitInfo &unit_info);
  int distribute_for_permanent_offline_or_delete(
      const share::ObServerInfoInTable &server_info,
      const share::ObUnitInfo &unit_info);

  int distribute_for_standalone_sys_unit();

  int distribute_pool_for_standalone_sys_unit(
      const share::ObResourcePool &pool,
      const common::ObIArray<common::ObAddr> &sys_unit_server_array);

  int get_unit_resource_reservation(uint64_t unit_id,
                                    const common::ObAddr &new_server,
                                    common::ObIArray<share::ObUnitStat> &in_migrate_unit_stat);

  int try_migrate_unit(const uint64_t unit_id,
                       const uint64_t tenant_id,
                       const share::ObUnitStat &unit_stat,
                       const common::ObIArray<share::ObUnitStat> &migrating_unit_stat,
                       const common::ObAddr &dst);

  int try_cancel_migrate_unit(const share::ObUnit &unit, bool &is_canceled);
  int get_active_servers_info_and_resource_info_of_zone(
      const ObZone &zone,
      ObIArray<share::ObServerInfoInTable> &servers_info,
      ObIArray<obrpc::ObGetServerResourceInfoResult> &server_resources_info);

  // the new version server balance
private:
  static const double CPU_EPSILON;
  static const double EPSILON;
  struct UnitMigrateStatCmp
  {
    UnitMigrateStatCmp() : ret_(common::OB_SUCCESS) {}
    bool operator()(const UnitMigrateStat &left, const UnitMigrateStat &right);
    int get_ret() const;
    int ret_;
  };

  struct ServerLoad;
  struct ServerResourceLoad;
  struct LoadSum
  {
    LoadSum() : load_sum_() {}
    bool is_valid() const;
    void reset();
    TO_STRING_KV(K(load_sum_));
    double get_required(const ObResourceType resource_type) const;
    int append_load(const share::ObUnitConfig &load);
    int append_load(const ObUnitManager::ObUnitLoad &unit_load);
    int append_load(const common::ObIArray<ObUnitManager::ObUnitLoad> &unit_loads);
    int append_load(const LoadSum &load);
    int remove_load(const ObUnitManager::ObUnitLoad &unit_load);
    int calc_load_value(
        double *const res_weights,
        const int64_t weights_count,
        const ServerResourceLoad &server_resource,
        double &load_value);

    share::ObUnitConfig load_sum_;
  };

  struct SimpleUgLoad
  {
    SimpleUgLoad() : column_tenant_id_(common::OB_INVALID_ID),
                     original_server_(),
                     arranged_server_(),
                     load_sum_() {}

    bool is_valid() const {
      return common::OB_INVALID_ID != column_tenant_id_
             && original_server_.is_valid()
             && arranged_server_.is_valid()
             && load_sum_.is_valid();
    }
    void reset() {
      column_tenant_id_ = common::OB_INVALID_ID;
      original_server_.reset();
      arranged_server_.reset();
      load_sum_.reset();
    }
    TO_STRING_KV(K(column_tenant_id_),
                 K(original_server_),
                 K(arranged_server_),
                 K(load_sum_));
    uint64_t column_tenant_id_;
    common::ObAddr original_server_;
    common::ObAddr arranged_server_;
    LoadSum load_sum_;
  };

  struct UnitGroupLoad
  {
    UnitGroupLoad() : column_tenant_id_(OB_INVALID_ID),
                      start_column_idx_(-1),
                      column_count_(0),
                      server_(),
                      server_load_(NULL), // empty for sys tenant
                      unit_loads_(),
                      load_sum_() {}
    bool is_valid() const;
    void reset();
    int sum_group_load();
    int get_intra_ttg_load_value(
        const ServerLoad &target_server_load,
        double &intra_ttg_load_value) const;
    int get_inter_ttg_load_value(
        const ServerLoad &target_server_load,
        double &inter_ttg_load_value) const;
    TO_STRING_KV(K(column_tenant_id_),
                 K(start_column_idx_),
                 K(column_count_),
                 K(server_),
                 KP(server_load_),
                 K(unit_loads_),
                 K(load_sum_));

    uint64_t column_tenant_id_; // Fill in the tenant_id of the tenant in the first row of this column
    // When unitgroup load comes from the same group (belonging to the same tenant),
    // start_column_idx_ and column_count_ are used to record 
    // the starting position and number of this group of unitgroups in the entire unitgroup array
    int64_t start_column_idx_;
    int64_t column_count_;
    common::ObAddr server_;
    const ServerLoad *server_load_;
    common::ObSEArray<share::ObUnitConfig, 8> unit_loads_;  
    LoadSum load_sum_;
  };

  struct UnitGroupLoadCmp
  {
    UnitGroupLoadCmp(ServerLoad &server_load) : server_load_(server_load),
                                                ret_(common::OB_SUCCESS) {}
    bool operator()(const UnitGroupLoad *left, const UnitGroupLoad *right);
    int get_ret() const;
    ServerLoad &server_load_;
    int ret_;
  };

  struct ResourceSum
  {
    ResourceSum(): resource_sum_() {}
    bool is_valid() const;
    void reset();
    TO_STRING_KV(K(resource_sum_));
    double get_capacity(const ObResourceType resource_type) const;
    int append_resource(const share::ObServerResourceInfo &resource);
    int append_resource(const ResourceSum &resource);

    share::ObServerResourceInfo resource_sum_;
  };

  struct ServerResourceLoad
  {
    ServerResourceLoad() : resource_info_(), server_(), inter_ttg_load_value_(0.0) {}
    virtual ~ServerResourceLoad() {}
    bool is_valid() const {
      return server_.is_valid() && resource_info_.is_valid();
    }
    void reset() {
      server_.reset();
      inter_ttg_load_value_ = 0.0;
      resource_info_.reset();
    }
    virtual int update_load_value() = 0;
    double get_true_capacity(const ObResourceType resource_type) const;
    TO_STRING_KV(K(server_), K(inter_ttg_load_value_), K(resource_info_));
    share::ObServerResourceInfo resource_info_;
    common::ObAddr server_;
    double inter_ttg_load_value_;
  };

  struct ServerLoad : public ServerResourceLoad
  {
    ServerLoad() : ServerResourceLoad(),
                   unitgroup_loads_(),
                   alien_ug_loads_(),
                   intra_ttg_load_value_(0.0),
                   intra_weights_(),
                   inter_weights_(),
                   intra_ttg_resource_info_() {}
    virtual ~ServerLoad() {}
    bool is_valid() const;
    void reset() {
      ServerResourceLoad::reset();
      unitgroup_loads_.reset();
      intra_ttg_load_value_ = 0.0;
    }
    double get_intra_ttg_resource_capacity(const ObResourceType resource_type) const;
    TO_STRING_KV(K(server_),
                 K(unitgroup_loads_),
                 K(alien_ug_loads_),
                 K(resource_info_),
                 K(inter_ttg_load_value_),
                 K(intra_ttg_load_value_));
    virtual int update_load_value() override;

    // Save the unitgroup loads of this group during intra equilibrium
    common::ObSEArray<UnitGroupLoad *, 64> unitgroup_loads_;
    // Save the unitgroup loads of other groups during inter equilibrium
    common::ObSEArray<UnitGroupLoad *, 64> alien_ug_loads_;
    double intra_ttg_load_value_;
    double intra_weights_[RES_MAX];
    double inter_weights_[RES_MAX];
    // Take the smallest value of cpu and mem in intra ttg, and fill in intra_ttg_resource_info_
    share::ObServerResourceInfo intra_ttg_resource_info_;
  };

  struct ServerTotalLoad : public ServerResourceLoad
  {
    ServerTotalLoad() : ServerResourceLoad(),
                        load_sum_(),
                        wild_server_(false),
                        resource_weights_() {}
    virtual ~ServerTotalLoad() {}
    bool is_valid() const {
      return ServerResourceLoad::is_valid() && load_sum_.is_valid();
    }
    void reset() {
      ServerResourceLoad::reset();
      load_sum_.reset();
    }
    virtual int update_load_value() override;
    TO_STRING_KV(K(server_), K(load_sum_), K(resource_info_), K(inter_ttg_load_value_));
    LoadSum load_sum_;
    bool wild_server_; // not in available server
    double resource_weights_[RES_MAX];
  };

  struct ServerTotalLoadCmp
  {
    ServerTotalLoadCmp() : ret_(common::OB_SUCCESS) {}
    bool operator()(const ServerTotalLoad *left, const ServerTotalLoad *right);
    bool operator()(const ServerTotalLoad &left, const ServerTotalLoad &right);
    int get_ret() const;
    int ret_;
  };

  struct IntraServerLoadCmp
  {
    IntraServerLoadCmp() : ret_(common::OB_SUCCESS) {}
    bool operator()(const ServerLoad *left, const ServerLoad *right);
    bool operator()(const ServerLoad &left, const ServerLoad &right);
    int get_ret() const;
    int ret_;
  };

  struct InterServerLoadCmp
  {
    InterServerLoadCmp() : ret_(common::OB_SUCCESS) {}
    bool operator()(const ServerLoad *left, const ServerLoad *right);
    bool operator()(const ServerLoad &left, const ServerLoad &right);
    int get_ret() const;
    int ret_;
  };

  struct ServerLoadUgCntCmp
  {
    ServerLoadUgCntCmp() : ret_(common::OB_SUCCESS) {}
    bool operator()(const ServerLoad *left, const ServerLoad *right);
    int get_ret() const;
    int ret_;
  };

  struct ServerLoadSum
  {
    ServerLoadSum() : server_(), load_sum_(), disk_in_use_(0) {}
    TO_STRING_KV(K(server_), K(load_sum_), K(disk_in_use_));
    common::ObAddr server_;
    LoadSum load_sum_;
    int64_t disk_in_use_;
  };

  struct PoolOccupation
  {
    PoolOccupation() : tenant_id_(OB_INVALID_ID), resource_pool_id_(OB_INVALID_ID), server_() {}
    PoolOccupation(const uint64_t tenant_id, const uint64_t pool_id, const common::ObAddr &server)
      : tenant_id_(tenant_id), resource_pool_id_(pool_id), server_(server) {}
    TO_STRING_KV(K(tenant_id_), K(resource_pool_id_), K(server_));
    uint64_t tenant_id_;
    // When the resource pool does not have a grant, tenant_id_ is an invalid value
    uint64_t resource_pool_id_;
    common::ObAddr server_;
  };

  struct TenantGroupBalanceInfo
  {
    TenantGroupBalanceInfo() : tenant_id_matrix_(NULL),
                               unit_migrate_stat_matrix_(),
                               column_unit_num_array_(),
                               unitgroup_load_array_(),
                               server_load_array_(),
                               intra_weights_(),
                               inter_weights_() {}

    int get_trick_id(uint64_t &trick_id);
    bool is_stable() const;
    ServerLoad *get_server_load(const common::ObAddr &server);
    int get_all_unit_loads(common::ObIArray<ObUnitManager::ObUnitLoad> &unit_loads);
    TO_STRING_KV(KP(tenant_id_matrix_),
                 K(unit_migrate_stat_matrix_),
                 K(column_unit_num_array_),
                 K(unitgroup_load_array_),
                 K(server_load_array_));

    const Matrix<uint64_t> *tenant_id_matrix_;
    Matrix<UnitMigrateStat> unit_migrate_stat_matrix_;
    common::ObArray<int64_t> column_unit_num_array_;
    common::ObArray<UnitGroupLoad> unitgroup_load_array_;
    common::ObArray<ServerLoad> server_load_array_;
    double intra_weights_[RES_MAX];
    double inter_weights_[RES_MAX];
  };

  struct UnitLoadDiskCmp
  {
    UnitLoadDiskCmp(ObUnitStatManager &unit_stat_mgr)
        : unit_stat_mgr_(unit_stat_mgr),
          ret_(common::OB_SUCCESS) {}
    bool operator()(const ObUnitManager::ObUnitLoad *left, const ObUnitManager::ObUnitLoad *right);
    int get_ret() const { return ret_; }
    ObUnitStatManager &unit_stat_mgr_;
    int ret_;
  };

  struct ServerDiskStatistic
  {
    ServerDiskStatistic() : server_(),
                            disk_in_use_(0),
                            disk_total_(0),
                            wild_server_(false){}
    void reset() {
      server_.reset();
      disk_in_use_ = 0;
      disk_total_ = 0;
      wild_server_ = false;
    }

    double get_disk_used_percent() const {
      double percent = 0.0;
      if (disk_total_ <= 0) {
        percent = 1.0;
      } else if (disk_total_ < disk_in_use_) {
        percent = 1.0;
      } else {
        percent = static_cast<double>(disk_in_use_) / static_cast<double>(disk_total_);
      }
      return percent;
    }

    double get_disk_used_percent_if_add(int64_t add_size) const {
      double percent = 0.0;
      int64_t disk_if_add = add_size + disk_in_use_;
      if (disk_total_ <= 0) {
        percent = 1.0;
      } else if (disk_total_ < disk_if_add) {
        percent = 1.0;
      } else {
        percent = static_cast<double>(disk_if_add) / static_cast<double>(disk_total_);
      }
      return percent;
    }

    TO_STRING_KV(K(server_),
                 K(disk_in_use_),
                 K(disk_total_),
                 K(wild_server_));

    common::ObAddr server_;
    int64_t disk_in_use_;
    int64_t disk_total_;
    // The permanently offline server and the deleted server are called wild_server_
    bool wild_server_;
  };

  struct ServerDiskStatisticCmp
  {
    bool operator()(
         const ServerDiskStatistic &left,
         const ServerDiskStatistic &right);
  };

  struct ServerDiskPercentCmp
  {
    ServerDiskPercentCmp() : ret_(common::OB_SUCCESS) {}
    bool operator()(
         const ServerDiskStatistic *left,
         const ServerDiskStatistic *right);
    int get_ret() const { return ret_; }
    int ret_;
  };

  struct ZoneServerDiskStatistic
  {
    ZoneServerDiskStatistic() : zone_(),
                                server_disk_statistic_array_(),
                                over_disk_waterlevel_(false) {}
    void reset() {
      zone_.reset();
      server_disk_statistic_array_.reset();
      over_disk_waterlevel_ = false;
    }
    int append(
        ServerDiskStatistic &server_disk_statistic);
    int get_server_disk_statistic(
        const common::ObAddr &server,
        ServerDiskStatistic &server_disk_statistic);
    int raise_server_disk_use(
        const common::ObAddr &server,
        const int64_t unit_required_size);
    int reduce_server_disk_use(
        const common::ObAddr &server,
        const int64_t unit_required_size);
    int check_server_over_disk_waterlevel(
        const common::ObAddr &server,
        const double disk_waterlevel,
        bool &disk_over_waterlevel);
    bool over_disk_waterlevel() { return over_disk_waterlevel_; }
    int check_all_available_servers_over_disk_waterlevel(
        const double disk_waterlevel,
        bool &all_available_servers_over_disk_waterlevel);

    TO_STRING_KV(K(zone_), K(server_disk_statistic_array_), K(over_disk_waterlevel_));

    common::ObZone zone_;
    common::ObArray<ServerDiskStatistic> server_disk_statistic_array_;
    // If any server in the array exceeds the disk waterlevel, the following flags will be set
    bool over_disk_waterlevel_;
  };

  class InnerTenantGroupBalanceStrategy
  {
  public:
    InnerTenantGroupBalanceStrategy(ObServerBalancer &host) : host_(host) {}
    virtual ~InnerTenantGroupBalanceStrategy() {}
    virtual int do_balance_row_units(
        TenantGroupBalanceInfo &balance_info,
        Matrix<UnitMigrateStat> &unit_migrate_stat_matrix,
        common::ObIArray<UnitGroupLoad> &unitgroup_loads,
        common::ObIArray<ServerLoad> &server_loads) = 0;
    virtual int amend_ug_inter_ttg_server_load(
        ServerLoad *over_server_load,
        ServerLoad *under_server_load,
        TenantGroupBalanceInfo *balance_info,
        const double inter_ttg_upper_limit) = 0;
    int coordinate_all_column_unit_group(
        const common::ObIArray<int64_t> &column_unit_num_array,
        Matrix<UnitMigrateStat> &unit_migrate_stat_matrix,
        const common::ObIArray<UnitGroupLoad> &unitgroup_loads);
  protected:
    int coordinate_single_column_unit_group(
        const int64_t start_column_idx,
        const int64_t column_count,
        Matrix<UnitMigrateStat> &unit_migrate_stat_matrix,
        const common::ObIArray<UnitGroupLoad> &unitgroup_loads);
    int do_coordinate_single_column_unit_group(
        const int64_t start_column_idx,
        const int64_t column_count,
        Matrix<UnitMigrateStat> &unit_migrate_stat_matrix,
        const common::ObIArray<common::ObAddr> &dest_servers);
    int do_coordinate_single_unit_row(
        const int64_t row,
        const int64_t start_column_idx,
        const int64_t column_count,
        const common::ObIArray<common::ObAddr> &candidate_servers,
        Matrix<UnitMigrateStat> &unit_migrate_stat_matrix);
    int arrange_unit_migrate_stat_pos(
        const common::ObIArray<common::ObAddr> &candidate_servers,
        UnitMigrateStat &unit_migrate_stat,
        const common::ObIArray<common::ObAddr> &excluded_array);
    int move_ug_between_server_loads(
        ServerLoad &src_server_load,
        ServerLoad &dst_server_load,
        UnitGroupLoad &ug_load,
        const int64_t ug_idx);
    int get_ug_balance_excluded_dst_servers(
        const UnitGroupLoad &unitgroup_load,
        const common::ObIArray<UnitGroupLoad> &unitgroup_loads,
        common::ObIArray<common::ObAddr> &excluded_servers);
    int exchange_ug_between_server_loads(
        UnitGroupLoad &left_ug,
        const int64_t left_idx,
        ServerLoad &left_server,
        UnitGroupLoad &right_ug,
        const int64_t right_idx,
        ServerLoad &right_server);
  protected:
    ObServerBalancer &host_;
  };

  class CountBalanceStrategy : public InnerTenantGroupBalanceStrategy
  {
  public:
    CountBalanceStrategy(ObServerBalancer &host)
      : InnerTenantGroupBalanceStrategy(host) {}
    virtual ~CountBalanceStrategy() {}
    virtual int do_balance_row_units(
        TenantGroupBalanceInfo &balance_info,
        Matrix<UnitMigrateStat> &unit_migrate_stat_matrix,
        common::ObIArray<UnitGroupLoad> &unitgroup_loads,
        common::ObIArray<ServerLoad> &server_loads) override;
    virtual int amend_ug_inter_ttg_server_load(
        ServerLoad *over_server_load,
        ServerLoad *under_server_load,
        TenantGroupBalanceInfo *balance_info,
        const double inter_ttg_upper_limit) override;
  private:
    int make_count_balanced(
        TenantGroupBalanceInfo &balance_info,
        Matrix<UnitMigrateStat> &unit_migrate_stat_matrix,
        common::ObIArray<UnitGroupLoad> &unitgroup_loads,
        common::ObIArray<ServerLoad> &server_loads);
    int do_make_count_balanced(
        common::ObArray<ServerLoad *> &server_cnt_ptrs_sorted,
        common::ObIArray<UnitGroupLoad> &unitgroup_loads);
    int do_make_count_balanced_foreach(
        ServerLoad &src_server,
        ServerLoad &dst_server,
        common::ObIArray<UnitGroupLoad> &unitgroup_loads);
    int move_or_exchange_ug_balance_ttg_load(
        TenantGroupBalanceInfo &balance_info,
        common::ObIArray<UnitGroupLoad> &unitgroup_loads,
        common::ObIArray<ServerLoad> &server_loads);
    int do_move_or_exchange_ug_balance_ttg_load(
        common::ObArray<ServerLoad *> &server_load_ptrs_sorted,
        common::ObIArray<UnitGroupLoad> &unitgroup_loads,
        const double tolerance);
    int try_exchange_ug_balance_ttg_load_foreach(
        ServerLoad &src_server_load,
        ServerLoad &dst_server_load,
        common::ObIArray<UnitGroupLoad> &unitgroup_loads,
        bool &do_exchange);
    int try_move_ug_balance_ttg_load_foreach(
        ServerLoad &src_server_load,
        ServerLoad &dst_server_load,
        common::ObIArray<UnitGroupLoad> &unitgroup_loads,
        bool &do_move);
    int try_exchange_ug_balance_inter_ttg_load(
        ServerLoad &src_inter_load,
        ServerLoad &dst_inter_load,
        TenantGroupBalanceInfo *balance_info,
        common::ObIArray<UnitGroupLoad> &unitgroup_loads,
        const double inter_ttg_upper_lmt);
    int try_exchange_ug_balance_inter_ttg_load_foreach(
        ServerLoad &src_inter_load,
        ServerLoad &dst_inter_load,
        TenantGroupBalanceInfo *balance_info,
        common::ObIArray<UnitGroupLoad> &unitgroup_loads,
        const double inter_ttg_upper_lmt,
        bool &do_exchange);
    int try_move_single_ug_balance_inter_ttg_load(
        ServerLoad &src_inter_load,
        ServerLoad &dst_inter_load,
        TenantGroupBalanceInfo *balance_info,
        const double inter_ttg_upper_lmt);
    int check_can_move_ug_balance_intra_ttg_load(
        ServerLoad &left_server,
        UnitGroupLoad &ug_load,
        ServerLoad &right_server,
        bool &can_move);
    int check_can_exchange_ug_balance_intra_ttg_load(
        UnitGroupLoad &left_ug,
        ServerLoad &left_server,
        UnitGroupLoad &right_ug,
        ServerLoad &right_server,
        bool &can_exchange);
    int check_can_move_ug_balance_inter_ttg_load(
        ServerLoad &left_server,
        UnitGroupLoad &ug_load,
        ServerLoad &right_server,
        bool &can_move);
    int check_can_exchange_ug_balance_inter_ttg_load(
        UnitGroupLoad &left_ug,
        ServerLoad &left_server,
        UnitGroupLoad &right_ug,
        ServerLoad &right_server,
        bool &can_exchange);
  };

protected:
  int check_can_execute_rebalance(
      const common::ObZone &zone,
      bool &can_execute_rebalance);
  int rebalance_servers_v2(
      const common::ObZone &zone);
  int generate_single_tenant_group(
      Matrix<uint64_t> &tenant_id_matrix,
      const ObTenantGroupParser::TenantNameGroup &tenant_name_group);
  int generate_group_tenant_array(
      const common::ObIArray<ObTenantGroupParser::TenantNameGroup> &tenant_groups,
      common::ObIArray<Matrix<uint64_t> > &group_tenant_array);
  bool has_exist_in_group_tenant_array(
       const uint64_t tenant_id,
       const common::ObIArray<Matrix<uint64_t> > &group_tenant_array);
  int generate_standalone_tenant_array(
      const common::ObIArray<Matrix<uint64_t> > &group_tenant_array,
      common::ObIArray<uint64_t> &standalone_tenant_array);
  virtual int generate_tenant_array(
      const common::ObZone &zone,
      common::ObIArray<Matrix<uint64_t> > &group_tenant_array,
      common::ObIArray<uint64_t> &standalone_tenant_array);
  virtual int generate_available_servers(
      const common::ObZone &zone,
      const bool exclude_sys_unit_server,
      common::ObIArray<common::ObAddr> &available_servers);
  int fill_pool_units_in_zone(
      const common::ObZone &zone,
      share::ObResourcePool &pool,
      common::ObIArray<ObUnitManager::ObUnitLoad> &units);
  virtual int generate_not_grant_pool_units(
      const common::ObZone &zone,
      common::ObIArray<ObUnitManager::ObUnitLoad> &not_grant_units);
  virtual int generate_standalone_units(
      const common::ObZone &zone,
      const common::ObIArray<uint64_t> &standalone_tenant_array,
      common::ObIArray<ObUnitManager::ObUnitLoad> &standalone_units);
  int generate_tenant_balance_info_array(
      const common::ObZone &zone,
      const common::ObIArray<Matrix<uint64_t> > &group_tenant_array,
      common::ObIArray<Matrix<uint64_t> > &degraded_tenant_group_array,
      common::ObIArray<TenantGroupBalanceInfo> &balance_info_array);
  int try_generate_degraded_balance_info_array(
      const common::ObZone &zone,
      const common::ObIArray<const Matrix<uint64_t> *> &unit_num_not_match_array,
      common::ObIArray<Matrix<uint64_t> > &degraded_tenant_group_array,
      common::ObIArray<TenantGroupBalanceInfo> &balance_info_array);
  int do_degrade_tenant_group_matrix(
      const Matrix<uint64_t> &source_tenant_group,
      common::ObIArray<Matrix<uint64_t> > &degraded_tenant_group_array);
  int do_rebalance_servers_v2(
      const common::ObZone &zone,
      const common::ObIArray<Matrix<uint64_t> > &group_tenant_array,
      const common::ObIArray<ObUnitManager::ObUnitLoad> &standalone_units,
      const common::ObIArray<ObUnitManager::ObUnitLoad> &not_grant_units);
  int check_need_balance_sys_tenant_units(
      const common::ObIArray<ObUnitManager::ObUnitLoad> &sys_tenant_units,
      const common::ObIArray<common::ObAddr> &available_servers,
      bool &need_balance);
  int try_balance_sys_tenant_units(
      const common::ObZone &zone,
      bool &do_execute);
  int try_balance_non_sys_tenant_units(
      const common::ObZone &zone,
      const common::ObIArray<Matrix<uint64_t> > &group_tenant_array,
      const common::ObIArray<ObUnitManager::ObUnitLoad> &standalone_units,
      const common::ObIArray<ObUnitManager::ObUnitLoad> &not_grant_units);
  int do_balance_sys_tenant_single_unit(
      const ObUnitManager::ObUnitLoad &unit_load,
      common::ObArray<ServerTotalLoad *> &server_load_ptrs_sorted,
      common::ObIArray<UnitMigrateStat> &task_array,
      common::ObIArray<PoolOccupation> &pool_occupation);
  int do_update_src_server_load(
      const ObUnitManager::ObUnitLoad &unit_load,
      ServerTotalLoad &this_server_load,
      const share::ObUnitStat &unit_stat);
  int do_update_dst_server_load(
      const ObUnitManager::ObUnitLoad &unit_load,
      ServerTotalLoad &this_server_load,
      const share::ObUnitStat &unit_stat,
      common::ObIArray<PoolOccupation> &pool_occupation);
  int pick_server_load(
      const common::ObAddr &server,
      ObIArray<ServerTotalLoad *> &server_load_ptrs_sorted,
      ServerTotalLoad *&target_ptr);
  int try_balance_single_unit_by_disk(
      const ObUnitManager::ObUnitLoad &unit_load,
      common::ObArray<ServerTotalLoad *> &server_load_ptrs_sorted,
      common::ObIArray<UnitMigrateStat> &task_array,
      const common::ObIArray<common::ObAddr> &excluded_servers,
      common::ObIArray<PoolOccupation> &pool_occupation,
      bool &do_balance);
  int try_balance_single_unit_by_cm(
      const ObUnitManager::ObUnitLoad &unit_load,
      common::ObArray<ServerTotalLoad *> &server_load_ptrs_sorted,
      common::ObIArray<UnitMigrateStat> &task_array,
      const common::ObIArray<common::ObAddr> &excluded_servers,
      common::ObIArray<PoolOccupation> &pool_occupation,
      bool &do_balance);
  int do_balance_sys_tenant_units(
      const common::ObIArray<ObUnitManager::ObUnitLoad> &sys_tenant_units,
      const common::ObIArray<common::ObAddr> &available_servers,
      common::ObArray<ServerTotalLoad> &server_loads);
  int try_amend_and_execute_unit_balance_task(
      const common::ObZone &zone,
      common::ObIArray<TenantGroupBalanceInfo> &balance_info_array,
      const common::ObIArray<ObUnitManager::ObUnitLoad> &standalone_units,
      const common::ObIArray<ObUnitManager::ObUnitLoad> &not_grant_units,
      const common::ObIArray<common::ObAddr> &available_servers);
  int divide_tenantgroup_balance_info(
      common::ObIArray<TenantGroupBalanceInfo> &balance_info_array,
      common::ObIArray<TenantGroupBalanceInfo *> &stable_tenant_group,
      common::ObIArray<TenantGroupBalanceInfo *> &unstable_tenant_group);
  int choose_balance_info_amend(
      const common::ObIArray<TenantGroupBalanceInfo *> &tenant_group,
      const bool is_stable_tenantgroup,
      TenantGroupBalanceInfo *&info_may_amend,
      common::ObIArray<TenantGroupBalanceInfo *> &other_ttg);
  int try_amend_and_execute_stable_tenantgroup(
      const common::ObZone &zone,
      const common::ObIArray<TenantGroupBalanceInfo *> &unstable_tenant_group,
      const common::ObIArray<TenantGroupBalanceInfo *> &stable_tenant_group,
      const common::ObIArray<ObUnitManager::ObUnitLoad> &standalone_units,
      const common::ObIArray<ObUnitManager::ObUnitLoad> &not_grant_units,
      const common::ObIArray<common::ObAddr> &available_servers,
      bool &do_amend);
  int try_amend_and_execute_unstable_tenantgroup(
      const common::ObZone &zone,
      const common::ObIArray<TenantGroupBalanceInfo *> &unstable_tenant_group,
      const common::ObIArray<TenantGroupBalanceInfo *> &stable_tenant_group,
      const common::ObIArray<ObUnitManager::ObUnitLoad> &standalone_units,
      const common::ObIArray<ObUnitManager::ObUnitLoad> &not_grant_units,
      const common::ObIArray<common::ObAddr> &available_servers,
      bool &do_amend);
  int get_sys_tenant_unitgroup_loads(
      const common::ObZone &zone,
      common::ObIArray<UnitGroupLoad> &sys_tenant_ug_loads);
  int try_amend_and_execute_tenantgroup(
      const common::ObZone &zone,
      TenantGroupBalanceInfo *info_amend,
      const common::ObIArray<TenantGroupBalanceInfo *> &stable_tenant_group,
      const common::ObIArray<TenantGroupBalanceInfo *> &unstable_tenant_group,
      const common::ObIArray<ObUnitManager::ObUnitLoad> &standalone_units,
      const common::ObIArray<ObUnitManager::ObUnitLoad> &not_grant_units,
      const common::ObIArray<common::ObAddr> &available_servers,
      bool &do_amend);
  int try_execute_unit_balance_task(
      const common::ObZone &zone,
      TenantGroupBalanceInfo *info_need_amend,
      const common::ObIArray<ObUnitManager::ObUnitLoad> &standalone_units,
      const common::ObIArray<ObUnitManager::ObUnitLoad> &not_grant_units,
      const common::ObIArray<TenantGroupBalanceInfo *> &unstable_tenant_group,
      const common::ObIArray<common::ObAddr> &available_servers,
      bool &do_amend);
  int generate_unit_balance_task(
      TenantGroupBalanceInfo &balance_info,
      common::ObIArray<UnitMigrateStat *> &task_array);
  int check_and_do_migrate_unit_task_(
      const common::ObIArray<UnitMigrateStat> &task_array);
  virtual int do_migrate_unit_task(
      const common::ObIArray<UnitMigrateStat> &task_array);
  virtual int do_migrate_unit_task(
      const common::ObIArray<UnitMigrateStat *> &task_array);
  int vacate_space_for_ttg_balance(
      const common::ObZone &zone,
      TenantGroupBalanceInfo &info_need_amend,
      const common::ObIArray<ObUnitManager::ObUnitLoad> &standalone_units,
      const common::ObIArray<ObUnitManager::ObUnitLoad> &not_grant_units,
      const common::ObIArray<TenantGroupBalanceInfo *> &unstable_tenant_group,
      const common::ObIArray<common::ObAddr> &available_servers,
      bool &do_amend);
  int do_migrate_out_collide_unit(
      UnitMigrateStat &unit_migrate_stat,
      ObUnitManager::ObUnitLoad &collide_unit_load,
      common::ObArray<ServerTotalLoad> &server_loads,
      bool &do_amend);
  int do_vacate_space_for_ttg_balance(
      UnitMigrateStat &unit_migrate_stat,
      const common::ObIArray<ObUnitManager::ObUnitLoad> &standalone_units,
      const common::ObIArray<ObUnitManager::ObUnitLoad> &not_grant_units,
      const common::ObIArray<TenantGroupBalanceInfo *> &unstable_tenant_group,
      common::ObArray<ServerTotalLoad> &server_loads,
      bool &do_amend);
  int pick_vacate_server_load(
      common::ObIArray<ServerTotalLoad> &server_loads,
      common::ObArray<ServerTotalLoad *> &server_load_ptrs_sorted,
      UnitMigrateStat &unit_migrate_stat,
      ServerTotalLoad *&vacate_server_load);
  int generate_complete_server_loads(
      const common::ObZone &zone,
      const common::ObIArray<common::ObAddr> &avaliable_servers,
      double *const resource_weights,
      const int64_t weights_count,
      common::ObArray<ServerTotalLoad> &server_loads);
  int generate_pool_occupation_array(
      const common::ObIArray<ObUnitManager::ObUnitLoad> &units,
      common::ObIArray<PoolOccupation> &pool_occupation);
  int check_single_server_resource_enough(
      const LoadSum &this_load,
      const share::ObUnitStat &unit_stat,
      const ServerTotalLoad &server_load,
      bool &enough,
      const bool mind_disk_waterlevel = true);
  int vacate_space_by_tenantgroup(
      LoadSum &load_to_vacate,
      const ObUnitManager::ObUnitLoad &unit_load_to_vacate,
      ServerTotalLoad &server_load_to_vacate,
      common::ObIArray<UnitMigrateStat> &task_array,
      const common::ObIArray<TenantGroupBalanceInfo *> &unstable_tenant_group,
      common::ObArray<ServerTotalLoad *> &server_load_ptrs_sorted);
  int vacate_space_by_unit_array(
      LoadSum &load_to_vacate,
      const ObUnitManager::ObUnitLoad &unit_load_to_vacate,
      ServerTotalLoad &server_load_to_vacate,
      common::ObIArray<UnitMigrateStat> &task_array,
      const common::ObIArray<ObUnitManager::ObUnitLoad> &units,
      common::ObArray<ServerTotalLoad *> &server_load_ptrs_sorted);
  int vacate_space_by_single_unit(
      ServerTotalLoad &server_load_to_vacate,
      const ObUnitManager::ObUnitLoad &unit_load,
      common::ObIArray<UnitMigrateStat> &task_array,
      common::ObIArray<PoolOccupation> &pool_occupation,
      common::ObArray<ServerTotalLoad *> &server_load_ptrs_sorted);
  int get_pool_occupation_excluded_dst_servers(
      const ObUnitManager::ObUnitLoad &this_load,
      const common::ObIArray<PoolOccupation> &pool_occupation,
      common::ObIArray<common::ObAddr> &excluded_servers);
  int check_servers_resource_enough(
      const common::ObIArray<ServerLoadSum> &server_load_sums,
      bool &enough);
  int accumulate_balance_task_loadsum(
      const UnitMigrateStat &unit_migrate_stat,
      common::ObIArray<ServerLoadSum> &server_load_sums);
  int try_generate_square_task_from_ttg_matrix(
      Matrix<UnitMigrateStat> &unit_migrate_stat_task,
      common::ObIArray<UnitMigrateStat *> &task_array);
  int try_generate_line_task_from_ttg_matrix(
      Matrix<UnitMigrateStat> &unit_migrate_stat_task,
      common::ObIArray<UnitMigrateStat *> &task_array);
  int try_generate_dot_task_from_ttg_matrix(
      Matrix<UnitMigrateStat> &unit_migrate_stat_task,
      common::ObIArray<UnitMigrateStat *> &task_array);
  int try_generate_dot_task_from_migrate_stat(
      UnitMigrateStat &unit_migrate_stat,
      common::ObIArray<UnitMigrateStat *> &task_array);
  int check_unit_migrate_stat_unit_collide(
      UnitMigrateStat &unit_migrate_stat,
      ObUnitManager::ObUnitLoad &collide_unit,
      bool &tenant_unit_collide);
  int calc_inter_ttg_weights(
      TenantGroupBalanceInfo *info_need_amend,
      const common::ObIArray<TenantGroupBalanceInfo *> &stable_tenant_group,
      const common::ObIArray<UnitGroupLoad> &sys_tenant_ug_loads,
      const common::ObIArray<common::ObAddr> &avaliable_servers,
      double *const resource_weights,
      const int64_t weights_count);
  int generate_inter_ttg_server_loads(
      TenantGroupBalanceInfo *info_need_amend,
      const common::ObIArray<TenantGroupBalanceInfo *> &stable_tenant_group,
      common::ObIArray<UnitGroupLoad> &sys_tenant_ug_loads,
      const common::ObIArray<common::ObAddr> &available_servers);
  int append_alien_ug_loads(
      ServerLoad &server_load,
      const common::ObIArray<TenantGroupBalanceInfo *> &tenant_groups,
      common::ObIArray<UnitGroupLoad> &sys_tenant_ug_loads);
  int do_amend_inter_ttg_balance(
      TenantGroupBalanceInfo *balance_info,
      const common::ObIArray<TenantGroupBalanceInfo *> &stable_tenant_group,
      common::ObIArray<ServerLoad> &server_loads);
  int sort_server_loads_for_balance(
      common::ObIArray<ServerLoad *> &complete_server_loads,
      common::ObIArray<ServerLoad *> &over_server_loads,
      common::ObIArray<ServerLoad *> &under_server_loads,
      double &upper_lmt);
  int make_inter_ttg_server_under_load(
      TenantGroupBalanceInfo *balance_info,
      common::ObIArray<ServerLoad *> &over_server_loads,
      common::ObIArray<ServerLoad *> &under_server_loads,
      const double inter_ttg_upper_lmt);
  int amend_ug_inter_ttg_server_load(
      ServerLoad *over_server_load,
      ServerLoad *under_server_load,
      TenantGroupBalanceInfo *balance_info,
      const double inter_ttg_upper_limit);
  int generate_simple_ug_loads(
      common::ObIArray<SimpleUgLoad> &simple_ug_loads,
      TenantGroupBalanceInfo &balance_info);
  int get_ug_exchange_excluded_dst_servers(
      const int64_t ug_idx,
      const Matrix<UnitMigrateStat> &unit_migrate_stat_matrix,
      common::ObIArray<common::ObAddr> &excluded_servers);
  int check_cm_resource_enough(
      const LoadSum &this_load,
      const ServerTotalLoad &server_load,
      bool &enough);
  int check_exchange_ug_make_sense(
      common::ObIArray<ServerTotalLoad> &server_loads,
      const int64_t left_idx,
      const int64_t right_idx,
      common::ObIArray<SimpleUgLoad> &simple_ug_loads,
      Matrix<UnitMigrateStat> &unit_migrate_stat_matrix,
      bool &do_make_sense);
  int coordinate_unit_migrate_stat_matrix(
      const int64_t left_idx,
      const int64_t right_idx,
      common::ObIArray<SimpleUgLoad> &simple_ug_loads,
      Matrix<UnitMigrateStat> &unit_migrate_stat_matrix);
  int generate_exchange_ug_migrate_task(
      const int64_t left_idx,
      const int64_t right_idx,
      common::ObIArray<SimpleUgLoad> &simple_ug_loads,
      Matrix<UnitMigrateStat> &unit_migrate_stat_matrix,
      common::ObIArray<UnitMigrateStat> &task_array);
  int try_balance_server_disk_onebyone(
      common::ObIArray<ServerTotalLoad> &server_loads,
      TenantGroupBalanceInfo &balance_info,
      common::ObIArray<SimpleUgLoad> &simple_ug_loads,
      const int64_t ug_idx,
      const common::ObIArray<common::ObAddr> &available_servers,
      const common::ObIArray<common::ObAddr> &disk_over_servers,
      common::ObIArray<UnitMigrateStat> &task_array,
      bool &do_balance_disk);
  int try_balance_server_disk_by_ttg(
      common::ObIArray<ServerTotalLoad> &server_loads,
      const common::ObAddr &src_server,
      TenantGroupBalanceInfo &balance_info,
      const common::ObIArray<common::ObAddr> &available_servers,
      const common::ObIArray<common::ObAddr> &disk_over_servers,
      common::ObIArray<UnitMigrateStat> &task_array,
      bool &do_balance_disk);
  int try_balance_disk_by_stable_tenantgroup(
      const common::ObZone &zone,
      common::ObIArray<TenantGroupBalanceInfo *> &stable_ttg,
      const common::ObIArray<common::ObAddr> &available_servers,
      bool &do_balance_disk);
  int do_non_tenantgroup_unit_balance_task(
      const common::ObZone &zone,
      const common::ObIArray<ObUnitManager::ObUnitLoad> &standalone_units,
      const common::ObIArray<ObUnitManager::ObUnitLoad> &not_grant_units,
      const common::ObIArray<common::ObAddr> &available_servers);
  int divide_complete_server_loads_for_balance(
      const common::ObIArray<common::ObAddr> &available_servers,
      common::ObIArray<ServerTotalLoad> &server_loads,
      common::ObIArray<ServerTotalLoad *> &wild_server_loads,
      common::ObIArray<ServerTotalLoad *> &over_server_loads,
      common::ObIArray<ServerTotalLoad *> &under_server_loads,
      double &upper_lmt);
  int make_wild_servers_empty(
      const common::ObIArray<ObUnitManager::ObUnitLoad> &standalone_units,
      const common::ObIArray<ObUnitManager::ObUnitLoad> &not_grant_units,
      common::ObIArray<ServerTotalLoad *> &wild_server_loads,
      common::ObIArray<ServerTotalLoad *> &over_server_loads,
      common::ObIArray<ServerTotalLoad *> &under_server_loads);
  int make_wild_servers_empty_by_units(
      common::ObIArray<ServerTotalLoad *> &wild_server_loads,
      const common::ObIArray<ObUnitManager::ObUnitLoad> &units,
      common::ObArray<ServerTotalLoad *> &available_server_loads,
      common::ObIArray<UnitMigrateStat> &task_array);
  int make_single_wild_server_empty_by_units(
      common::ObIArray<PoolOccupation> &pool_occupation,
      ServerTotalLoad &wild_server_load,
      const common::ObIArray<ObUnitManager::ObUnitLoad> &units,
      common::ObArray<ServerTotalLoad *> &available_server_loads,
      common::ObIArray<UnitMigrateStat> &task_array);
  int generate_available_server_loads(
      ObIArray<ServerTotalLoad *> &over_server_loads,
      ObIArray<ServerTotalLoad *> &under_server_loads,
      ObIArray<ServerTotalLoad *> &available_server_loads);
  int get_disk_over_available_servers(
      common::ObIArray<common::ObAddr> &disk_over_servers);
  int make_non_ttg_balance_under_load_by_disk(
      common::ObIArray<PoolOccupation> &pool_occupation,
      common::ObIArray<UnitMigrateStat> &task_array,
      const ObUnitManager::ObUnitLoad &unit_load,
      ServerTotalLoad *src_server_load,
      const common::ObIArray<common::ObAddr> &disk_over_servers,
      common::ObIArray<ServerTotalLoad *> &avaliable_server_loads);
  int generate_sort_available_servers_disk_statistic(
      common::ObArray<ServerDiskStatistic> &server_disk_statistic_array_,
      common::ObArray<ServerDiskStatistic *> &available_servers_disk_statistic);
  int divide_available_servers_disk_statistic(
      common::ObIArray<ServerDiskStatistic *> &available_servers_disk_statistic,
      common::ObIArray<ServerDiskStatistic *> &over_disk_statistic,
      common::ObIArray<ServerDiskStatistic *> &under_disk_statistic,
      double &upper_limit);
  int make_server_disk_underload_by_unit(
      ServerTotalLoad *src_server_load,
      const ObUnitManager::ObUnitLoad &unit_load,
      const double upper_lmt,
      common::ObIArray<PoolOccupation> &pool_occupation,
      common::ObIArray<ServerTotalLoad *> &available_server_loads,
      common::ObArray<ServerDiskStatistic *> &under_disk_statistic,
      common::ObIArray<UnitMigrateStat> &task_array);
  int make_server_disk_underload(
      ServerDiskStatistic &src_disk_statistic,
      const common::ObIArray<ObUnitManager::ObUnitLoad> &standalone_units,
      const double upper_lmt,
      common::ObIArray<PoolOccupation> &pool_occupation,
      common::ObIArray<ServerTotalLoad *> &available_server_loads,
      common::ObArray<ServerDiskStatistic *> &under_disk_statistic,
      common::ObIArray<UnitMigrateStat> &task_array);
  int make_available_servers_disk_balance(
      const common::ObIArray<ObUnitManager::ObUnitLoad> &standalone_units,
      common::ObIArray<ServerTotalLoad *> &available_server_loads,
      int64_t &task_count);
  int make_available_servers_balance_by_disk(
      const common::ObIArray<ObUnitManager::ObUnitLoad> &standalone_units,
      common::ObIArray<ServerTotalLoad *> &available_server_loads,
      int64_t &task_count);
  int make_available_servers_balance_by_cm(
      const common::ObIArray<ObUnitManager::ObUnitLoad> &standalone_units,
      const common::ObIArray<ObUnitManager::ObUnitLoad> &not_grant_units,
      common::ObIArray<ServerTotalLoad *> &over_server_loads,
      common::ObIArray<ServerTotalLoad *> &under_server_loads,
      const double upper_lmt,
      double *const g_res_weights,
      const int64_t weights_count,
      int64_t &task_count);
  int make_available_servers_balance(
      const common::ObIArray<ObUnitManager::ObUnitLoad> &standalone_units,
      const common::ObIArray<ObUnitManager::ObUnitLoad> &not_grant_units,
      common::ObIArray<ServerTotalLoad *> &over_server_loads,
      common::ObIArray<ServerTotalLoad *> &under_server_loads,
      const double upper_lmt,
      double *const g_res_weights,
      const int64_t weights_count);
  int do_non_ttg_unit_balance_by_cm(
      common::ObIArray<UnitMigrateStat> &task_array,
      const common::ObIArray<ObUnitManager::ObUnitLoad> &units,
      common::ObIArray<ServerTotalLoad *> &over_server_loads,
      common::ObIArray<ServerTotalLoad *> &under_server_loads,
      const double upper_lmt,
      double *const g_res_weights,
      const int64_t weights_count);
  int make_non_ttg_balance_under_load_by_cm(
      common::ObIArray<PoolOccupation> &pool_occupation,
      common::ObIArray<UnitMigrateStat> &task_array,
      const common::ObIArray<ObUnitManager::ObUnitLoad> &units,
      ServerTotalLoad *over_server_loads,
      ServerTotalLoad *under_server_loads,
      const double upper_lmt,
      double *const g_res_weights,
      const int64_t weights_count);
  int calc_global_balance_resource_weights(
      const common::ObZone &zone,
      const common::ObIArray<common::ObAddr> &available_servers,
      double *const resource_weights,
      const int64_t weights_count);
  int check_and_get_tenant_matrix_unit_num(
      const Matrix<uint64_t> &tenant_id_matrix,
      const common::ObZone &zone,
      bool &unit_num_match,
      ObIArray<int64_t> &column_unit_num_array);
  int check_and_get_tenant_column_unit_num(
      const ObIArray<uint64_t> &tenant_id_column,
      const common::ObZone &zone,
      bool &unit_num_match,
      int64_t &unit_num);
  int generate_original_unit_matrix(
      const Matrix<uint64_t> &tenant_id_matrix,
      const common::ObZone &zone,
      const ObIArray<int64_t> &column_unit_num_array,
      Matrix<UnitMigrateStat> &unit_migrate_stat_matrix);
  int fill_unit_migrate_stat_matrix(
      Matrix<UnitMigrateStat> &unit_migrate_stat_matrix,
      const uint64_t tenant_id,
      const int64_t row,
      const int64_t start_column_id,
      const common::ObIArray<ObUnitManager::ObUnitLoad> &unit_loads);
  int single_unit_migrate_stat_matrix_balance(
      TenantGroupBalanceInfo &balance_info,
      const common::ObIArray<common::ObAddr> &available_servers);
  int balance_row_units(
      TenantGroupBalanceInfo &balance_info,
      const common::ObIArray<int64_t> &column_unit_num_array,
      Matrix<UnitMigrateStat> &unit_migrate_stat_matrix,
      common::ObIArray<UnitGroupLoad> &unitgroup_loads,
      common::ObIArray<ServerLoad> &server_loads,
      const common::ObIArray<common::ObAddr> &available_servers);
  int generate_unitgroup_and_server_load(
      const Matrix<UnitMigrateStat> &unit_migrate_stat_matrix,
      const common::ObIArray<int64_t> &column_unit_num_array,
      const common::ObIArray<common::ObAddr> &available_servers,
      common::ObIArray<UnitGroupLoad> &unitgroup_loads,
      common::ObIArray<ServerLoad> &server_loads);
  int generate_unitgroup_load(
      const Matrix<UnitMigrateStat> &unit_migrate_stat_matrix,
      const common::ObIArray<int64_t> &column_unit_num_array,
      common::ObIArray<UnitGroupLoad> &unitgroup_loads);
  int generate_column_unitgroup_load(
      const int64_t column,
      const Matrix<UnitMigrateStat> &unit_migrate_stat_matrix,
      const int64_t start_column_idx,
      const int64_t column_count,
      common::ObIArray<UnitGroupLoad> &unitgroup_loads);
  int try_regulate_intra_ttg_resource_info(
      const share::ObServerResourceInfo &this_resource_info,
      share::ObServerResourceInfo &intra_ttg_resource_info);
  int generate_server_load(
      const common::ObIArray<common::ObAddr> &available_servers,
      common::ObIArray<UnitGroupLoad> &unitgroup_loads,
      common::ObIArray<ServerLoad> &server_loads);
  int arrange_server_load_for_wild_unitgroup(
      UnitGroupLoad &unitgroup_load,
      common::ObIArray<ServerLoad> &server_loads);
  int calc_row_balance_resource_weights(
      const common::ObIArray<ServerLoad> &server_loads,
      const common::ObIArray<UnitGroupLoad> &unitgroup_loads,
      double *const resource_weights,
      const int64_t weights_count);
  int update_server_load_value(
      const double *const resource_weights,
      const int64_t weights_count,
      common::ObIArray<ServerLoad> &server_loads);
  int do_balance_row_units(
      TenantGroupBalanceInfo &balance_info,
      Matrix<UnitMigrateStat> &unit_migrate_stat_matrix,
      common::ObIArray<UnitGroupLoad> &unitgroup_loads,
      common::ObIArray<ServerLoad> &server_loads);

  int generate_zone_server_disk_statistic(
      const common::ObZone &zone);
  int get_server_balance_critical_disk_waterlevel(
      double &server_balance_critical_disk_waterlevel) const;
  int get_server_data_disk_usage_limit(
      double &data_disk_usage_limit) const;

protected:
  bool inited_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
  ObUnitManager *unit_mgr_;
  ObZoneManager *zone_mgr_;
  ObServerManager *server_mgr_;
  ObUnitStatManager unit_stat_mgr_;
  CountBalanceStrategy count_balance_strategy_;
  InnerTenantGroupBalanceStrategy &inner_ttg_balance_strategy_;
  // Each time the unit balance between servers is executed,
  // the disk information of each server in the zone is calculated
  ZoneServerDiskStatistic zone_disk_statistic_;

  DISALLOW_COPY_AND_ASSIGN(ObServerBalancer);
};

} // end namespace rootserver
} // end namespace oceanbase

#endif /* _OB_SERVER_BALANCER_H */
