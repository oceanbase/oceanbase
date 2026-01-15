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

#ifndef OCEANBASE_OBSERVER_OB_TENANT_OFFLINE_TABLET_CLEANUP_SERVICE_H_
#define OCEANBASE_OBSERVER_OB_TENANT_OFFLINE_TABLET_CLEANUP_SERVICE_H_

#include "lib/task/ob_timer.h"
#include "lib/container/ob_array.h"
#include "share/ob_ls_id.h"

namespace oceanbase
{
namespace observer
{
class ObTenantTabletCleanupService;
class ObTenantOfflineTabletCleanupTask : public common::ObTimerTask
{
public:
  ObTenantOfflineTabletCleanupTask(ObTenantTabletCleanupService &service)
    : service_(service) {}
  virtual ~ObTenantOfflineTabletCleanupTask() = default;
  virtual void runTimerTask() override;

private:
  int cleanup_offline_tablet_meta_and_checksum_();
  int get_offline_servers_(common::ObArray<std::pair<share::ObLSID, common::ObAddr>> &offline_servers);
  int get_candidate_offline_pairs_(common::ObArray<std::pair<share::ObLSID, common::ObAddr>> &pairs);
  int cleanup_offline_tablet_table_(const share::ObLSID &ls_id, const common::ObAddr &server, const char *table_name);
  bool is_sys_ls_leader_();
private:
  ObTenantTabletCleanupService &service_;
};

class ObTenantTabletCleanupService
{
public:
  static int mtl_new(ObTenantTabletCleanupService *&service);
  static void mtl_destroy(ObTenantTabletCleanupService *&service);
public:
  ObTenantTabletCleanupService(uint64_t tenant_id);
  ~ObTenantTabletCleanupService();
  int init();
  int start();
  int stop();
  void wait();
  void destroy();
  bool is_stop() const { return is_stop_; }
  uint64_t get_tenant_id() const { return tenant_id_; }

private:
  static const int64_t CLEANUP_INTERVAL = 10 * 60 * 1000 * 1000; // 10 minutes

private:
  uint64_t tenant_id_;
  ObTenantOfflineTabletCleanupTask cleanup_task_;
  common::ObTimer timer_;
  volatile bool is_stop_;
  bool is_inited_;
};

} // end namespace observer
} // end namespace oceanbase

#endif // OCEANBASE_OBSERVER_OB_TENANT_OFFLINE_TABLET_CLEANUP_SERVICE_H_
