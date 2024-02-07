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

#ifndef OCEANBASE_SHARE_OB_TABLET_LOCATION_REFRESH_SERVICE_H
#define OCEANBASE_SHARE_OB_TABLET_LOCATION_REFRESH_SERVICE_H

#include "lib/hash/ob_hashmap.h"
#include "lib/lock/ob_mutex.h"
#include "rootserver/ob_rs_reentrant_thread.h"
#include "rootserver/ob_thread_idling.h"
#include "share/transfer/ob_transfer_info.h"
namespace oceanbase
{
namespace share
{
namespace schema
{
class ObMultiVersionSchemaService;
}
class ObTabletLSService;

class ObTabletLocationRefreshMgr
{
public:
  ObTabletLocationRefreshMgr() = delete;
  ObTabletLocationRefreshMgr(const uint64_t tenant_id,
                             const ObTransferTaskID &base_task_id);
  ~ObTabletLocationRefreshMgr();

  void set_base_task_id(const ObTransferTaskID &base_task_id);
  void get_base_task_id(ObTransferTaskID &base_task_id);

  int set_tablet_ids(const common::ObIArray<ObTabletID> &tablet_ids);
  int get_tablet_ids(common::ObIArray<ObTabletID> &tablet_ids);

  // ensure inc_task_infos_ order by task_id asc
  int merge_inc_task_infos(common::ObArray<ObTransferRefreshInfo> &inc_task_infos_to_merge);
  int get_doing_task_ids(common::ObIArray<ObTransferTaskID> &doing_task_ids);
  int clear_task_infos(bool &has_doing_task);

  void dump_statistic();
public:
  static const int64_t BATCH_TASK_COUNT = 128;
private:
  lib::ObMutex mutex_;
  uint64_t tenant_id_;
  // base_task_id_ = 0 : valid, means transfer never occur.
  // base_task_id_ = OB_INVALID_ID, invalid, means should be run in compat mode
  ObTransferTaskID base_task_id_;
  // tablet_ids to reload cache for compatibility
  common::ObArray<ObTabletID> tablet_ids_;
  // transfer tasks to process, order by tablet_id asc
  common::ObArray<ObTransferRefreshInfo> inc_task_infos_;
  DISALLOW_COPY_AND_ASSIGN(ObTabletLocationRefreshMgr);
};

class ObTabletLocationRefreshServiceIdling : public rootserver::ObThreadIdling
{
public:
  explicit ObTabletLocationRefreshServiceIdling(volatile bool &stop)
    : ObThreadIdling(stop) {}
  virtual int64_t get_idle_interval_us() override;
  int fast_idle();
private:
  const static int64_t DEFAULT_TIMEOUT_US = 10 * 60 * 1000 * 1000L; // 10m
  const static int64_t FAST_TIMEOUT_US    =  1 * 60 * 1000 * 1000L; // 1m
};

// refresh all tenant's cached tablet-ls locations automatically
// design doc : ob/rootservice/di76sdhof1h97har#p34dp
class ObTabletLocationRefreshService : public rootserver::ObRsReentrantThread
{
public:
  ObTabletLocationRefreshService();
  virtual ~ObTabletLocationRefreshService();

  int init(ObTabletLSService &tablet_ls_service,
           share::schema::ObMultiVersionSchemaService &schema_service,
           common::ObMySQLProxy &sql_proxy);

  int try_init_base_point(const int64_t tenant_id);

  void destroy();

  virtual int start() override;
  virtual void stop() override;
  virtual void wait() override;
  virtual void run3() override;
  virtual int blocking_run() override { BLOCKING_RUN_IMPLEMENT(); }
  // don't use common::ObThreadFlags::set_rs_flag()
  virtual int before_blocking_run() override { return common::OB_SUCCESS; }
  virtual int after_blocking_run() override { return common::OB_SUCCESS; }
private:
  void idle_();
  int check_stop_();
  int check_tenant_can_refresh_(const uint64_t tenant_id);
  int get_base_task_id_(const uint64_t tenant_id, ObTransferTaskID &base_task_id);

  int inner_get_mgr_(const int64_t tenant_id,
                     ObTabletLocationRefreshMgr *&mgr);
  int get_tenant_ids_(common::ObIArray<uint64_t> &tenant_ids);
  int try_clear_mgr_(const uint64_t tenant_id, bool &clear);

  int try_init_base_point_(const int64_t tenant_id);

  int refresh_cache_();
  int refresh_cache_(const uint64_t tenant_id);

  int try_runs_for_compatibility_(const uint64_t tenant_id);
  int try_reload_tablet_cache_(const uint64_t tenant_id);

  int fetch_inc_task_infos_and_update_(const uint64_t tenant_id);
  int process_doing_task_infos_(const uint64_t tenant_id);
  int clear_task_infos_(const uint64_t tenant_id);
private:
  bool inited_;
  bool has_task_;
  mutable ObTabletLocationRefreshServiceIdling idling_;
  ObTabletLSService *tablet_ls_service_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
  common::ObMySQLProxy *sql_proxy_;
  common::ObArenaAllocator allocator_;
  // Wlock will be holded in the following scenes:
  // - Init/Destroy tenant's management struct.
  // - Init `base_task_id_` for compatibility scence.
  // - Destroy tenant's management struct.
  common::SpinRWLock rwlock_;
  // tenant_mgr_map_ won't be erased
  common::hash::ObHashMap<uint64_t, ObTabletLocationRefreshMgr*, common::hash::NoPthreadDefendMode> tenant_mgr_map_;
  DISALLOW_COPY_AND_ASSIGN(ObTabletLocationRefreshService);
};

} // end namespace share
} // end namespace oceanbase
#endif // OCEANBASE_SHARE_OB_TABLET_LOCATION_REFRESH_SERVICE_H
