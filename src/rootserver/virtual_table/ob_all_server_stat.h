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

#ifndef OCEANBASE_ROOTSERVER_OB_ALL_SERVER_STAT_H_
#define OCEANBASE_ROOTSERVER_OB_ALL_SERVER_STAT_H_

#include "share/ob_virtual_table_projector.h"
#include "share/ob_server_status.h"
#include "rootserver/ob_unit_manager.h"
#include "rootserver/ob_leader_coordinator.h"

namespace oceanbase {
namespace share {
namespace schema {
class ObMultiVersionSchemaService;
class ObTableSchema;
}  // namespace schema
}  // namespace share
namespace rootserver {
class ObServerManager;
class ObZoneManager;

class ObAllServerStat : public common::ObVirtualTableProjector {
public:
  ObAllServerStat();
  virtual ~ObAllServerStat();

  int init(share::schema::ObMultiVersionSchemaService& schema_service, ObUnitManager& unit_mgr,
      ObServerManager& server_mgr, ObILeaderCoordinator& leader_coordinator);
  virtual int inner_get_next_row(common::ObNewRow*& row);

private:
  static constexpr double EPSLISON = 0.000000001;
  static const int64_t INVALID_TOTAL_RESOURCE = 0;
  static const int64_t INVALID_ASSIGNED_PERCENT = -1;
  static const int64_t INVALID_INT_VALUE = -1;
  struct ServerStat {
    ObUnitManager::ObServerLoad server_load_;
    int64_t cpu_assigned_percent_;
    int64_t mem_assigned_percent_;
    int64_t disk_assigned_percent_;
    int64_t unit_num_;
    int64_t migrating_unit_num_;
    int64_t merged_version_;
    int64_t leader_count_;
    // the following fields are added at OB 1.4
    double load_;
    double cpu_weight_;
    double memory_weight_;
    double disk_weight_;

    ServerStat();

    void reset();
    // get methods
    inline double get_cpu_capacity() const
    {
      return server_load_.status_.resource_info_.cpu_;
    }
    inline int64_t get_mem_capacity() const
    {
      return server_load_.status_.resource_info_.mem_total_;
    }
    inline int64_t get_disk_total() const
    {
      return server_load_.status_.resource_info_.disk_total_;
    }
    inline double get_cpu_assigned() const
    {
      return server_load_.sum_load_.min_cpu_;
    }
    inline double get_cpu_max_assigned() const
    {
      return server_load_.sum_load_.max_cpu_;
    }
    inline int64_t get_mem_assigned() const
    {
      return server_load_.sum_load_.min_memory_;
    }
    inline int64_t get_mem_max_assigned() const
    {
      return server_load_.sum_load_.max_memory_;
    }
    inline int64_t get_disk_assigned(void) const
    {
      return server_load_.sum_load_.max_disk_size_;
    }
    inline int64_t get_cpu_assigned_percent(void) const
    {
      return cpu_assigned_percent_;
    }
    inline int64_t get_mem_assigned_percent(void) const
    {
      return mem_assigned_percent_;
    }
    inline int64_t get_disk_assigned_percent(void) const
    {
      return disk_assigned_percent_;
    }
    inline int64_t get_unit_num(void) const
    {
      return unit_num_;
    }
    inline int64_t get_migrating_unit_num(void) const
    {
      return migrating_unit_num_;
    }
    const common::ObZone& get_zone(void) const
    {
      return server_load_.status_.zone_;
    }

    // set methods
    inline void set_cpu_assigned_percent(const int64_t& percent)
    {
      cpu_assigned_percent_ = percent;
    }
    inline void set_mem_assigned_percent(const int64_t& percent)
    {
      mem_assigned_percent_ = percent;
    }
    inline void set_disk_assigned_percent(const int64_t& percent)
    {
      disk_assigned_percent_ = percent;
    }
    inline void set_unit_num(const int64_t& num)
    {
      unit_num_ = num;
    }
    inline void set_migrating_unit_num(const int64_t& num)
    {
      migrating_unit_num_ = num;
    }

    int assign(const ServerStat& other);

    TO_STRING_KV(K_(server_load), K_(cpu_assigned_percent), K_(disk_assigned_percent), K_(unit_num),
        K_(migrating_unit_num), K_(merged_version), K_(leader_count));

  private:
    DISALLOW_COPY_AND_ASSIGN(ServerStat);
  };

  int get_server_stats(common::ObIArray<ServerStat>& server_stat);
  int get_leader_count(
      const ObILeaderCoordinator::ServerLeaderStat& leader_stat, const common::ObAddr& server, int64_t& leader_count);
  int calc_server_usage(ServerStat& server_stat);
  int calc_server_unit_num(ServerStat& server_stat);
  int get_full_row(
      const share::schema::ObTableSchema* table, ServerStat& server_stat, common::ObIArray<Column>& columns);

private:
  bool inited_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  ObUnitManager* unit_mgr_;
  ObServerManager* server_mgr_;
  ObILeaderCoordinator* leader_coordinator_;
};
}  // end namespace rootserver
}  // end namespace oceanbase

#endif  // OCEANBASE_ROOTSERVER_OB_ALL_SERVER_STAT_H_
