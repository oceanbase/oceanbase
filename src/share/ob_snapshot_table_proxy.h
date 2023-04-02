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

#ifndef OCEANBASE_SHARE_OB_SNAPSHOT_TABLE_PROXY_H_
#define OCEANBASE_SHARE_OB_SNAPSHOT_TABLE_PROXY_H_

#include "lib/container/ob_iarray.h"
#include "lib/lock/ob_mutex.h"
#include "share/ob_define.h"
#include "share/scn.h"

namespace oceanbase
{
namespace common
{
class ObISQLClient;
class ObMySQLTransaction;
}
namespace share
{
class ObDMLSqlSplicer;

enum ObSnapShotType
{
  SNAPSHOT_FOR_MAJOR = 0,
  SNAPSHOT_FOR_DDL = 1,
  SNAPSHOT_FOR_MULTI_VERSION = 2,
  SNAPSHOT_FOR_RESTORE_POINT = 3,
  SNAPSHOT_FOR_BACKUP_POINT = 4,
  MAX_SNAPSHOT_TYPE,
};

struct ObSnapshotInfo
{
public:
  ObSnapShotType snapshot_type_;
  SCN snapshot_scn_;
  int64_t schema_version_;
  uint64_t tenant_id_; //tenant_id=OB_INVALID_ID represent all tenants
  uint64_t tablet_id_; //tablet_id=OB_INVALID_ID represent all tablets of tenant
  const char* comment_;
  ObSnapshotInfo();
  ~ObSnapshotInfo() {}
  int init(const uint64_t tenant_id, const uint64_t tablet_id,
           const ObSnapShotType &snapshot_type, const SCN &snapshot_scn,
           const int64_t schema_version, const char* comment);
  void reset();
  bool is_valid() const;
  const char * get_snapshot_type_str() const;
  static const char *ObSnapShotTypeStr[];
  TO_STRING_KV(K_(snapshot_type),
               K_(snapshot_scn),
               K_(schema_version),
               K_(tenant_id),
               K_(tablet_id),
               KP_(comment));

};

struct TenantSnapshot
{
public:
  uint64_t tenant_id_;
  SCN snapshot_scn_;
  TenantSnapshot() {}
  TenantSnapshot(const uint64_t tenant_id, const SCN &snapshot_scn)
      : tenant_id_(tenant_id), snapshot_scn_(snapshot_scn) {}
  ~TenantSnapshot() {}
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K_(tenant_id), K_(snapshot_scn));
};

class ObSnapshotTableProxy
{
  static const int64_t BATCH_OP_SIZE = 256;
public:
  ObSnapshotTableProxy() : lock_(ObLatchIds::DEFAULT_MUTEX), last_event_ts_(0) {}
  virtual ~ObSnapshotTableProxy() {}

  int add_snapshot(
      common::ObMySQLTransaction &trans,
      const share::ObSnapshotInfo &snapshot);

  int batch_add_snapshot(
      common::ObMySQLTransaction &trans,
      const share::ObSnapShotType snapshot_type,
      const uint64_t tenant_id,
      const int64_t schema_version,
      const SCN &snapshot_scn,
      const char *comment,
      const common::ObIArray<ObTabletID> &tablet_id_array);

  int remove_snapshot(common::ObISQLClient &proxy,
                      const uint64_t tenant_id,
                      const ObSnapshotInfo &info);
  int remove_snapshot(common::ObISQLClient &proxy,
                      const uint64_t tenant_id,
                      share::ObSnapShotType snapshot_type);
  int batch_remove_snapshots(common::ObISQLClient &proxy,
                             share::ObSnapShotType snapshot_type,
                             const uint64_t tenant_id,
                             const int64_t schema_version,
                             const SCN &snapshot_scn,
                             const common::ObIArray<ObTabletID> &tablet_ids);
  int get_all_snapshots(common::ObISQLClient &proxy,
                        const uint64_t tenant_id,
                        common::ObIArray<ObSnapshotInfo> &snapshots);
  int get_snapshot(common::ObISQLClient &proxy,
                   const uint64_t tenant_id,
                   ObSnapShotType snapshot_type,
                   ObSnapshotInfo &snapshot_info);
  int get_snapshot(common::ObISQLClient &proxy,
                   const uint64_t tenant_id,
                   ObSnapShotType snapshot_type,
                   const char *extra_info,
                   ObSnapshotInfo &snapshot_info);
  int get_snapshot(common::ObISQLClient &proxy,
                   const uint64_t tenant_id,
                   const ObSnapShotType snapshot_type,
                   const SCN &snapshot_scn,
                   ObSnapshotInfo &snapshot_info);

  int get_max_snapshot_info(common::ObISQLClient &proxy,
                            const uint64_t tenant_id,
                            ObSnapshotInfo &snapshot_info);
  int check_snapshot_exist(common::ObISQLClient &proxy,
                           const uint64_t tenant_id,
                           const int64_t table_id,
                           ObSnapShotType snapshot_type,
                           bool &is_exist);
  int check_snapshot_exist(common::ObISQLClient &proxy,
                           const uint64_t tenant_id,
                           const share::ObSnapShotType snapshot_type,
                           bool &is_exist);
  int get_snapshot_count(common::ObISQLClient &proxy,
                         const uint64_t tenant_id,
                         ObSnapShotType snapshot_type,
                         int64_t &count);
private:
  int gen_event_ts(int64_t &event_ts);
  int check_snapshot_valid(const SCN &snapshot_gc_scn,
                           const ObSnapshotInfo &info,
                           bool &is_valid) const;
  int fill_snapshot_item(const ObSnapshotInfo &info,
      share::ObDMLSqlSplicer &dml);

private:
  lib::ObMutex lock_;
  int64_t last_event_ts_;
};
} //namespace share
} //namespace oceanbase

#endif // OCEANBASE_SHARE_OB_SNAPSHOT_TABLE_PROXY_H_
