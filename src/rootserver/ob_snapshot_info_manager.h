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

#ifndef OCEANBASE_ROOTSERVER_OB_SNAPSHOT_INFO_MANAGER_H_
#define OCEANBASE_ROOTSERVER_OB_SNAPSHOT_INFO_MANAGER_H_

#include "share/ob_snapshot_table_proxy.h"
#include "lib/net/ob_addr.h"
#include "share/scn.h"
namespace oceanbase
{
namespace common
{
class ObMySQLProxy;
}
namespace share
{
class ObSnapshotInfo;
  class SCN;
}
namespace rootserver
{
class ObZoneManager;
class ObSnapshotInfoManager
{
public:
  ObSnapshotInfoManager() : self_addr_() {}
  virtual ~ObSnapshotInfoManager() {}
  int init(const common::ObAddr &self_addr);
  int acquire_snapshot(common::ObMySQLTransaction &trans,
                       const uint64_t tenant_id,
                       const share::ObSnapshotInfo &snapshot);
  int release_snapshot(common::ObMySQLTransaction &trans,
                       const uint64_t tenant_id,
                       const share::ObSnapshotInfo &snapshot);
  int get_snapshot(common::ObMySQLProxy &proxy,
                   const uint64_t tenant_id,
                   share::ObSnapShotType snapshot_type,
                   const char *extra_info,
                   share::ObSnapshotInfo &snapshot_info);
  int get_snapshot(common::ObMySQLProxy &proxy,
                   const uint64_t tenant_id,
                   share::ObSnapShotType snapshot_type,
                   const share::SCN &snapshot_scn,
                   share::ObSnapshotInfo &snapshot_info);

  int check_restore_point(common::ObMySQLProxy &proxy,
                          const uint64_t tenant_id,
                          const int64_t table_id,
                          bool &is_exist);
  int get_snapshot_count(common::ObMySQLProxy &proxy,
                         const uint64_t tenant_id,
                         share::ObSnapShotType snapshot_type,
                         int64_t &count);
  int batch_acquire_snapshot(
      common::ObMySQLProxy &proxy,
      share::ObSnapShotType snapshot_type,
      const uint64_t tenant_id,
      const int64_t schema_version,
      const share::SCN &snapshot_scn,
      const char *comment,
      const common::ObIArray<ObTabletID> &tablet_ids);
  int batch_release_snapshot_in_trans(
      common::ObMySQLTransaction &trans,
      share::ObSnapShotType snapshot_type,
      const uint64_t tenant_id,
      const int64_t schema_version,
      const share::SCN &snapshot_scn,
      const common::ObIArray<ObTabletID> &tablet_ids);

private:
  common::ObAddr self_addr_;
  DISALLOW_COPY_AND_ASSIGN(ObSnapshotInfoManager);
};
} //end rootserver
} //end oceanbase
#endif
