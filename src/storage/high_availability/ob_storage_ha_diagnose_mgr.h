/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEABASE_STORAGE_HA_DIAGNOSE_
#define OCEABASE_STORAGE_HA_DIAGNOSE_

#include "lib/container/ob_array.h"
#include "lib/hash/ob_hashset.h"
#include "lib/lock/ob_spin_rwlock.h"
#include "share/ob_ls_id.h"
#include "share/ob_storage_ha_diagnose_struct.h"
#include "share/storage/ob_ha_inflight_diag.h"

namespace oceanbase
{
namespace storage
{

// Flush-time bundle handed from transfer handler to the per-tenant diag
// queue. Carries the atomic state snapshot plus the LS id. The retry_count
// now lives inside `snapshot_` (see ObHAInflightDiagState). The terminating
// result is folded into `snapshot_.last_err_code_` by the producer.
struct ObHADiagFlushItem final
{
  ObHADiagFlushItem();
  void reset();

  share::ObLSID ls_id_;
  share::ObHAInflightDiagState snapshot_;

  TO_STRING_KV(K_(ls_id), K_(snapshot));
};

// Tenant-level HA diagnose manager.
//   - Keeps the per-tenant inflight ls_id index used by the virtual tables.
//   - Buffers flush items produced by transfer handlers. The global
//     ObStorageHADiagService drains the queue each tick and writes rows
//     into the sys-tenant history tables.
class ObStorageHADiagMgr final
{
public:
  ObStorageHADiagMgr();
  ~ObStorageHADiagMgr() { destroy(); }
  static int mtl_init(ObStorageHADiagMgr *&storage_ha_diag_mgr);
  int init(const uint64_t tenant_id);
  void destroy();

  int add_inflight(const share::ObLSID &ls_id);
  int remove_inflight(const share::ObLSID &ls_id);
  int get_inflight_snapshot(common::ObIArray<share::ObLSID> &out) const;

  // Handler entry point — enqueue one flush item and return. Non-blocking:
  // the caller's thread never touches SQL. Drops with warn when the queue
  // is full (callers already treat flush errors as advisory).
  int submit_flush(const ObHADiagFlushItem &item);

  // Move all pending items into `out` and clear the queue. Called by the
  // global ObStorageHADiagService after switching into this tenant.
  int drain_pending(common::ObIArray<ObHADiagFlushItem> &out);

public:
  static constexpr int64_t INFLIGHT_BUCKET_NUM = 16;
  static constexpr int64_t MAX_PENDING = 1024;

private:
  bool is_inited_;
  mutable common::SpinRWLock inflight_lock_;
  common::hash::ObHashSet<share::ObLSID, common::hash::NoPthreadDefendMode> inflight_keys_;

  mutable common::SpinRWLock queue_lock_;
  common::ObArray<ObHADiagFlushItem> pending_;
  int64_t dropped_count_;

  DISALLOW_COPY_AND_ASSIGN(ObStorageHADiagMgr);
};

}
}
#endif
