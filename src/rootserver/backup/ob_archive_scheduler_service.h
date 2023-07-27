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

#ifndef OCEANBASE_ROOTSERVER_OB_ARCHIVE_SCHEDULER_SERVICE_H_
#define OCEANBASE_ROOTSERVER_OB_ARCHIVE_SCHEDULER_SERVICE_H_

#include "ob_backup_base_service.h"
#include "lib/mysqlclient/ob_isql_client.h"
#include "lib/container/ob_iarray.h"
#include "share/backup/ob_backup_struct.h"

namespace oceanbase
{

namespace obrpc {
  class ObSrvRpcProxy;
}

namespace common {
  class ObMySQLProxy;
}

namespace rootserver
{

class ObArchiveSchedulerService final : public ObBackupBaseService
{
public:
  ObArchiveSchedulerService();
  virtual ~ObArchiveSchedulerService() {}

  const int64_t RESERVED_FETCH_US = 10 * 1000 * 1000; // 10s, used for fetch observer log archive status
  const int64_t MIN_IDLE_INTERVAL_US = 2 * 1000 * 1000; // 2s
  const int64_t FAST_IDLE_INTERVAL_US = 10 * 1000 * 1000; // 10s, used during BEGINNING or STOPPING
  //const int64_t MAX_IDLE_INTERVAL_US = 60 * 1000 * 1000; // 60s
  const int64_t MAX_IDLE_INTERVAL_US = 10 * 1000 * 1000; // 60s
  DEFINE_MTL_FUNC(ObArchiveSchedulerService);
  int init();
  void run2() override;
  // force cancel archive
  int force_cancel(const uint64_t tenant_id);

  int open_archive_mode(const uint64_t tenant_id, const common::ObIArray<uint64_t> &archive_tenant_ids);

  int close_archive_mode(const uint64_t tenant_id, const common::ObIArray<uint64_t> &archive_tenant_ids);

  // If input tenant is sys tenant and archive_tenant_ids is empty, then start archive for all tenants.
  // Or if input tenant is sys tenant but archive_tenant_ids is not empty, then start archive for tenants in archive_tenant_ids.
  // Otherwize, just start archive for input tenant.
  int start_archive(const uint64_t tenant_id, const common::ObIArray<uint64_t> &archive_tenant_ids);

  // If input tenant is sys tenant and archive_tenant_ids is empty, then stop all tenant archive.
  // Or if input tenant is sys tenant but archive_tenant_ids is not empty, then stop archive for tenants in archive_tenant_ids.
  // Otherwize, just stop archive for input tenant.
  int stop_archive(const uint64_t tenant_id, const common::ObIArray<uint64_t> &archive_tenant_ids);

private:
  int process_();
  int start_tenant_archive_(const uint64_t tenant_id);
  // Return the first error that failed to start archive if force_start is true. Otherwise,
  // ignore all error.
  int start_tenant_archive_(const common::ObIArray<uint64_t> &tenant_ids_array, const bool force_start);
  // Return the first error that failed to stop archive if force_stop is true. Otherwise,
  // ignore all error.
  int stop_tenant_archive_(const common::ObIArray<uint64_t> &tenant_ids_array, const bool force_stop);
  int stop_tenant_archive_(const uint64_t tenant_id);
  int get_all_tenant_ids_(common::ObIArray<uint64_t> &tenantid_array);

  void set_checkpoint_interval_(const int64_t interval_us);
  int open_tenant_archive_mode_(const common::ObIArray<uint64_t> &tenant_ids_array);
  int open_tenant_archive_mode_(const uint64_t tenant_id);
  int close_tenant_archive_mode_(const common::ObIArray<uint64_t> &tenant_ids_array);
  int close_tenant_archive_mode_(const uint64_t tenant_id);

  bool is_inited_;
  uint64_t tenant_id_;
  obrpc::ObSrvRpcProxy *rpc_proxy_;
  common::ObMySQLProxy *sql_proxy_;
  share::schema::ObMultiVersionSchemaService *schema_service_;

  DISALLOW_COPY_AND_ASSIGN(ObArchiveSchedulerService);
};

}
}

#endif
