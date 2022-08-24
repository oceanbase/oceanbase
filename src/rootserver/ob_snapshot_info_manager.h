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
namespace oceanbase {
namespace share {
class ObSnapshotInfo;
}
namespace rootserver {
class ObZoneManager;
class ObSnapshotInfoManager {
public:
  ObSnapshotInfoManager() : self_addr_()
  {}
  virtual ~ObSnapshotInfoManager()
  {}
  int init(const common::ObAddr& self_addr);
  int acquire_snapshot(common::ObMySQLTransaction& trans, const share::ObSnapshotInfo& snapshot);
  int release_snapshot(common::ObMySQLTransaction& trans, const share::ObSnapshotInfo& snapshot);
  int acquire_snapshot_for_building_index(
      common::ObMySQLTransaction &trans, const share::ObSnapshotInfo &snapshot, const int64_t index_table_id);
  int get_snapshot(common::ObMySQLProxy &proxy, const int64_t tenant_id, share::ObSnapShotType snapshot_type,
      const char *extra_info, share::ObSnapshotInfo &snapshot_info);
  int get_snapshot(common::ObISQLClient &proxy, const int64_t tenant_id, share::ObSnapShotType snapshot_type,
      const int64_t snapshot_ts, share::ObSnapshotInfo &snapshot_info);

  int check_restore_point(common::ObMySQLProxy& proxy, const int64_t tenant_id, const int64_t table_id, bool& is_exist);
  int get_snapshot_count(
      common::ObMySQLProxy &proxy, const int64_t tenant_id, share::ObSnapShotType snapshot_type, int64_t &count);
  int set_index_building_snapshot(common::ObMySQLProxy &proxy, const int64_t index_table_id, const int64_t snapshot_ts);

private:
  DISALLOW_COPY_AND_ASSIGN(ObSnapshotInfoManager);

private:
  common::ObAddr self_addr_;
};
}  // namespace rootserver
}  // namespace oceanbase
#endif
