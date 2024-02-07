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

#ifndef OCEANBASE_SHARE_OB_TABLET_LS_SERVICE
#define OCEANBASE_SHARE_OB_TABLET_LS_SERVICE

#include "share/location_cache/ob_location_struct.h"
#include "share/location_cache/ob_location_update_task.h"
#include "share/location_cache/ob_tablet_ls_map.h" // ObTabletLSMap
#include "share/location_cache/ob_tablet_location_refresh_service.h"
#include "share/location_cache/ob_tablet_location_broadcast.h" // ObTabletLocationSender, ObTabletLocationUpdater

namespace oceanbase
{
namespace common
{
class ObMySQLProxy;
class ObTimeoutCtx;
}
namespace share
{
class ObTabletLSService;
typedef observer::ObUniqTaskQueue<ObTabletLSUpdateTask,
    ObTabletLSService> ObTabletLSUpdateQueue;

// This class is used to process the mapping between tablet and ls by ObLocationService.
class ObTabletLSService
{
public:
  ObTabletLSService()
      : inited_(false),
        sql_proxy_(NULL),
        inner_cache_(),
        async_queue_(),
        clear_expired_cache_task_(*this),
        broadcast_sender_(),
        broadcast_updater_(),
        auto_refresh_service_() {}
  virtual ~ObTabletLSService() {}
  int init(share::schema::ObMultiVersionSchemaService &schema_service,
           common::ObMySQLProxy &sql_proxy,
           obrpc::ObSrvRpcProxy &srv_rpc_proxy);
  // Gets the mapping between the tablet and log stream synchronously.
  //
  // @param [in] tenant_id: target tenant which the tablet belongs to
  // @param [in] expire_renew_time: The oldest renew time of cache which the user can tolerate.
  //                                Setting it to 0 means that the cache will not be renewed.
  //                                Setting it to INT64_MAX means renewing cache synchronously.
  // @param [out] is_cache_hit: If hit in location cache.
  // @return OB_MAPPING_BETWEEN_TABLET_AND_LS_NOT_EXIST if no records in sys table.
  //         OB_GET_LOCATION_TIME_OUT if get location by inner sql timeout.
  int get(
      const uint64_t tenant_id,
      const ObTabletID &tablet_id,
      const int64_t expire_renew_time,
      bool &is_cache_hit,
      ObLSID &ls_id);
  // Noblock way to get the mapping between the tablet and log stream.
  //
  // @return OB_MAPPING_BETWEEN_TABLET_AND_LS_NOT_EXIST if no records in sys table.
  int nonblock_get(
      const uint64_t tenant_id,
      const ObTabletID &tablet_id,
      ObLSID &ls_id);
  // Nonblock way to renew the mapping between the tablet and log stream.
  int nonblock_renew(
      const uint64_t tenant_id,
      const ObTabletID &tablet_id);
  // Batch renew the mapping between the tablet and LS synchronously
  // @param [in] tenant_id: target tenant which the tablet belongs to
  // @param [in] tablet_ids: target tablet_id list
  // @param [out] ls_ids: array of ls_ids that tablet belongs to
  // @return OB_SUCCESS: process successfully
  //         other: inner_sql execution error
  int batch_renew_tablet_ls_cache(
      const uint64_t tenant_id,
      const ObList<common::ObTabletID, common::ObIAllocator> &tablet_list,
      common::ObIArray<ObTabletLSCache> &tablet_ls_caches);
  // Add update task into async_queue_.
  int add_update_task(const ObTabletLSUpdateTask &task);
  // Process update tasks.
  int batch_process_tasks(
      const common::ObIArray<ObTabletLSUpdateTask> &tasks,
      bool &stopped);
  // Unused.
  int process_barrier(const ObTabletLSUpdateTask &task, bool &stopped);
  int start();
  void stop();
  void wait();
  int destroy();
  int reload_config();

  int clear_expired_cache();
  int submit_broadcast_task(const ObTabletLocationBroadcastTask &task);
  int submit_update_task(const ObTabletLocationBroadcastTask &task);

  int update_cache(const ObTabletLSCache &tablet_cache, const bool update_only);

  int get_tablet_ids_from_cache(
      const uint64_t tenant_id,
      common::ObIArray<ObTabletID> &tablet_ids);
private:
  int get_from_cache_(
      const uint64_t tenant_id,
      const ObTabletID &tablet_id,
      ObTabletLSCache &tablet_cache);
  int renew_cache_(
      const uint64_t tenant_id,
      const ObTabletID &tablet_id,
      ObTabletLSCache &tablet_cache);
  int set_timeout_ctx_(common::ObTimeoutCtx &ctx);
  bool is_valid_key_(const uint64_t tenant_id, const ObTabletID &tablet_id) const;
  int erase_cache_(const uint64_t tenant_id, const ObTabletID &tablet_id);
  bool belong_to_sys_ls_(const uint64_t tenant_id, const ObTabletID &tablet_id) const;

private:
  class IsDroppedTenantCacheFunctor; // use to clear expired cache of dropped tenant

  const int64_t MINI_MODE_UPDATE_THREAD_CNT = 1;
  const int64_t USER_TASK_QUEUE_SIZE = 100 * 1000; // 10W partitions
  const int64_t MINI_MODE_USER_TASK_QUEUE_SIZE = 10 * 1000; // 1W partitions
  const int64_t CLEAR_EXPIRED_CACHE_INTERVAL_US = 6 * 3600 * 1000 * 1000L; // 6h

  bool inited_;
  bool stopped_;
  common::ObMySQLProxy *sql_proxy_;
  ObTabletLSMap inner_cache_; // Store the mapping between tablet and log stream in user tenant.
  ObTabletLSUpdateQueue async_queue_;
  ObClearTabletLSCacheTimerTask clear_expired_cache_task_; // timer task used to clear the expired cache
  ObTabletLocationSender broadcast_sender_; // broadcast updated tablet location to every server
  ObTabletLocationUpdater broadcast_updater_; // process received broadcast task
  //TODO: need more queue later
  ObTabletLocationRefreshService auto_refresh_service_;
};

} // end namespace share
} // end namespace oceanbase
#endif
