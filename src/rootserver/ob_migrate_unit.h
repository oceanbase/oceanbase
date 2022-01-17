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

#ifndef _OB_MIGRATE_UNIT_H
#define _OB_MIGRATE_UNIT_H 1
#include "share/ob_define.h"
#include "ob_root_utils.h"
namespace oceanbase {
namespace rootserver {
class ObUnitManager;
class ObRebalanceTaskMgr;
class ObRebalanceTaskInfo;
class ObMigrateTaskInfo;
struct TenantBalanceStat;

class ObMigrateUnit {
public:
  ObMigrateUnit();
  virtual ~ObMigrateUnit()
  {}
  int init(ObUnitManager& unit_mgr, ObRebalanceTaskMgr& task_mgr, TenantBalanceStat& tenant_stat,
      share::ObCheckStopProvider& check_stop_provider);

  // if replica location and unit location differ, migrate to to unit location.
  int migrate_to_unit(int64_t& task_cnt);
  // check unit migrate finish
  int unit_migrate_finish(int64_t& task_cnt);

private:
  /* two return value:
   * 1 task_cnt: task count
   * 2 do_accumulated: try_accumulate_task_info has the semantic of trying to perform;
   *   if it is done, the set do_accumulated to true, else false
   */
  int try_accumulate_task_info(const bool small_tenant, common::ObIArray<ObMigrateTaskInfo>& task_info_array,
      const ObMigrateTaskInfo& task_info, const Partition* cur_partition, const Replica* cur_replica,
      const Partition*& first_migrate_p, const Replica*& first_migrate_r, int64_t& task_cnt, bool& do_accumulated);

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObMigrateUnit);
  // function members
  // return OB_CANCELED if stop, else return OB_SUCCESS
  int check_stop() const
  {
    return check_stop_provider_->check_stop();
  }

private:
  // data members
  bool inited_;
  ObUnitManager* unit_mgr_;
  ObRebalanceTaskMgr* task_mgr_;
  TenantBalanceStat* tenant_stat_;
  share::ObCheckStopProvider* check_stop_provider_;
};

}  // end namespace rootserver
}  // end namespace oceanbase

#endif /* _OB_MIGRATE_UNIT_H */
