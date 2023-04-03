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

#ifndef OCEANBASE_OBSERVER_OB_TENANT_META_TABLE_CHECKER
#define OCEANBASE_OBSERVER_OB_TENANT_META_TABLE_CHECKER

#include "lib/task/ob_timer.h" // ObTimerTask
#include "share/ls/ob_ls_info.h" // ObLSReplica
#include "share/tablet/ob_tablet_info.h" // ObTabletReplica
#include "share/ls/ob_ls_table.h" // ObLSTable::Mode

namespace oceanbase
{
namespace share
{
class ObLSTableOperator;
class ObTabletTableOperator;
}

namespace observer
{
class ObTenantMetaChecker;

class ObTenantLSMetaTableCheckTask : public common::ObTimerTask
{
public:
  explicit ObTenantLSMetaTableCheckTask(ObTenantMetaChecker &checker);
  virtual ~ObTenantLSMetaTableCheckTask() {}
  virtual void runTimerTask() override;
private:
  ObTenantMetaChecker &checker_;
};

class ObTenantTabletMetaTableCheckTask : public common::ObTimerTask
{
public:
  explicit ObTenantTabletMetaTableCheckTask(ObTenantMetaChecker &checker);
  virtual ~ObTenantTabletMetaTableCheckTask() {}
  virtual void runTimerTask() override;
private:
  ObTenantMetaChecker &checker_;
};

// ObTenantMetaChecker is used to check info in __all_tablet_meta_table and __all_ls_meta_table for tenant.
// It will supplement the missing ls/tablet and remove residual ls/tablet to meta table.
class ObTenantMetaChecker
{
public:
  ObTenantMetaChecker();
  virtual ~ObTenantMetaChecker() {}
  static int mtl_init(ObTenantMetaChecker *&checker);
  int init(
      const uint64_t tenant_id,
      share::ObLSTableOperator *lst_operator,
      share::ObTabletTableOperator *tt_operator);
  int start();
  void stop();
  void wait();
  void destroy();
  // check __all_ls_meta_table with local ls_service
  int check_ls_table();
  // check __all_tablet_meta_table with local ls_tablet_service
  int check_tablet_table();
  int schedule_ls_meta_check_task();
  int schedule_tablet_meta_check_task();
private:
  int check_ls_table_(const share::ObLSTable::Mode mode);
private:
  static const int64_t LS_REPLICA_MAP_BUCKET_NUM = 10;
  static const int64_t TABLET_REPLICA_MAP_BUCKET_NUM = 64 * 1024;
  typedef common::hash::ObHashMap<share::ObLSID, share::ObLSReplica> ObLSReplicaMap;
  typedef common::hash::ObHashMap<share::ObTabletLSPair, share::ObTabletReplica> ObTabletReplicaMap;

  int build_replica_map_(ObLSReplicaMap &replica_map, const share::ObLSTable::Mode mode);
  int build_replica_map_(ObTabletReplicaMap &replica_map);
  int check_dangling_replicas_(ObLSReplicaMap &replica_map, int64_t &dangling_count);
  int check_dangling_replicas_(ObTabletReplicaMap &replica_map, int64_t &dangling_count);
  int check_report_replicas_(ObLSReplicaMap &replica_map, int64_t &report_count);
  int check_report_replicas_(ObTabletReplicaMap &replica_map, int64_t &report_count);
  int check_tablet_not_exist_in_local_(
      const share::ObLSID &ls_id,
      const ObTabletID &tablet_id,
      bool &not_exist);

  bool inited_;
  bool stopped_;
  uint64_t tenant_id_;
  int ls_checker_tg_id_;
  int tablet_checker_tg_id_;
  share::ObLSTableOperator *lst_operator_; // operator to process __all_ls_meta_table
  share::ObTabletTableOperator *tt_operator_; // operator to process __all_tablet_meta_table
  ObTenantLSMetaTableCheckTask ls_meta_check_task_; // timer task to check ls meta
  ObTenantTabletMetaTableCheckTask tablet_meta_check_task_; // timer task to check tablet meta
};

} // end namespace observer
} // end namespace oceanbase
#endif
