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

#ifndef OCEANBASE_RS_OB_SNAPSHOT_TABLE_PROXY_H_
#define OCEANBASE_RS_OB_SNAPSHOT_TABLE_PROXY_H_

#include "lib/container/ob_iarray.h"
#include "lib/lock/ob_mutex.h"
#include "share/ob_define.h"
#include "share/ob_freeze_info_proxy.h"

namespace oceanbase {
namespace common {
class ObISQLClient;
}
namespace share {
struct ObSnapshotInfo {
public:
  share::ObSnapShotType snapshot_type_;
  int64_t snapshot_ts_;
  int64_t schema_version_;
  uint64_t tenant_id_;  // tenant_id=OB_INVALID_ID represent all tenants
  uint64_t table_id_;   // table_id=OB_INVALID_ID represent all tables of tenant
  const char* comment_;
  ObSnapshotInfo();
  ~ObSnapshotInfo()
  {}
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K_(snapshot_type), K_(snapshot_ts), K_(schema_version), K_(tenant_id), K_(table_id), KP_(comment));
};

struct TenantSnapshot {
public:
  uint64_t tenant_id_;
  int64_t snapshot_ts_;
  TenantSnapshot()
  {}
  TenantSnapshot(const uint64_t tenant_id, const int64_t snapshot_ts) : tenant_id_(tenant_id), snapshot_ts_(snapshot_ts)
  {}
  ~TenantSnapshot()
  {}
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K_(tenant_id), K_(snapshot_ts));
};

class ObSnapshotTableProxy {
public:
  ObSnapshotTableProxy() : lock_(), last_event_ts_(0)
  {}
  virtual ~ObSnapshotTableProxy()
  {}
  int add_snapshot(
      common::ObMySQLTransaction& trans, const ObSnapshotInfo& info, const bool& need_lock_gc_snapshot = true);
  int remove_snapshot(common::ObISQLClient& proxy, const ObSnapshotInfo& info);
  int get_all_snapshots(common::ObISQLClient& proxy, common::ObIArray<ObSnapshotInfo>& snapshots);
  // used to save multiversion of standby cluster
  // while tenant_id/snapshot of info exist, update it or do insert
  int insert_or_update_snapshot(common::ObISQLClient& proxy, const TenantSnapshot& info);
  int get_snapshot(common::ObISQLClient& proxy, const int64_t tenant_id, share::ObSnapShotType snapshot_type,
      ObSnapshotInfo& snapshot_info);
  int get_snapshot(common::ObISQLClient& proxy, const int64_t tenant_id, share::ObSnapShotType snapshot_type,
      const char* extra_info, ObSnapshotInfo& snapshot_info);
  int get_snapshot(common::ObISQLClient& proxy, const int64_t tenant_id, const share::ObSnapShotType snapshot_type,
      const int64_t snapshot_ts, ObSnapshotInfo& snapshot_info);

  int get_max_snapshot_info(common::ObISQLClient& proxy, ObSnapshotInfo& snapshot_info);
  int check_snapshot_exist(common::ObISQLClient& proxy, const int64_t tenant_id, const int64_t table_id,
      share::ObSnapShotType snapshot_type, bool& is_exist);
  int check_snapshot_exist(common::ObISQLClient& proxy, const share::ObSnapShotType snapshot_type, bool& is_exist);
  int get_snapshot_count(
      common::ObISQLClient& proxy, const int64_t tenant_id, share::ObSnapShotType snapshot_type, int64_t& count);

private:
  int inner_add_snapshot(common::ObMySQLTransaction& trans, const ObSnapshotInfo& info, const bool& insert_update);
  int gen_event_ts(int64_t& event_ts);
  int check_snapshot_valid(
      common::ObISQLClient& client, const ObSnapshotInfo& info, const bool& need_lock_gc_snapshot, bool& is_valid);

private:
  lib::ObMutex lock_;
  int64_t last_event_ts_;
};
}  // namespace share
}  // namespace oceanbase
#endif
