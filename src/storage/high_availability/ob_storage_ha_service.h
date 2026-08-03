/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEABASE_STORAGE_HA_SERVICE_
#define OCEABASE_STORAGE_HA_SERVICE_

#include "lib/thread/thread_pool.h"
#include "lib/thread/ob_reentrant_thread.h"
#include "lib/thread/ob_thread_name.h"
#include "lib/lock/ob_thread_cond.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/container/ob_se_array.h"
#include "lib/container/ob_array.h"
#include "lib/net/ob_addr.h"
#include "storage/tx_storage/ob_ls_service.h"

namespace oceanbase
{
namespace storage
{

// Local src reservation book. Per-addr "heat" of in-flight migrations on THIS
// observer, so concurrent LS migrations on the same node don't pile onto the
// hottest source. Lives on ObStorageHAService.
class ObMigrationSrcBook final
{
public:
  struct ReservationSnapshot
  {
    ReservationSnapshot() : ls_id_(), version_(0) {}
    share::ObLSID ls_id_;
    uint64_t version_;
    TO_STRING_KV(K_(ls_id), K_(version));
  };

  ObMigrationSrcBook();
  ~ObMigrationSrcBook() = default;
  void reset();

  // Record or replace ls_id's fixed source reservation.
  int reserve(const share::ObLSID &ls_id, const common::ObAddr &src);
  // Drop ls_id's reservation; no-op if absent.
  int release(const share::ObLSID &ls_id);
  // Drop the snapshotted reservation only when it has not been replaced.
  int release_if_version_matches(const ReservationSnapshot &snapshot, bool &released);
  // Pick the candidate with the lowest reserved-count on this observer and
  // record the choice for ls_id under the same write lock, so concurrent
  // picks see each other immediately (no pick/reserve TOCTOU window).
  // Ties broken by rand. Returns OB_ENTRY_NOT_EXIST if candidates is empty.
  int pick_coolest(const share::ObLSID &ls_id,
                   const common::ObIArray<common::ObAddr> &candidates,
                   common::ObAddr &chosen);
  // Snapshot the reservations currently held by the book. Used by the
  // background self-heal in ObStorageHAService to find reservations whose
  // migration has already ended but whose release() never ran. The book
  // itself cannot judge liveness (that needs the migration handler, a higher
  // layer), so it exposes versioned keys and lets the service decide.
  int list_reservations(common::ObIArray<ReservationSnapshot> &reservations) const;

private:
  static const int64_t ENTRY_ARRAY_CNT = 8;
  struct Entry
  {
    Entry() : ls_id_(), src_(), version_(0) {}
    share::ObLSID ls_id_;
    common::ObAddr src_;
    uint64_t version_;
    TO_STRING_KV(K_(ls_id), K_(src), K_(version));
  };
  // Callers hold lock_.
  int64_t get_heat_(const common::ObAddr &addr,
                    const share::ObLSID &excluded_ls_id) const;
  int find_coolest_(const share::ObLSID &ls_id,
                    const common::ObIArray<common::ObAddr> &candidates,
                    common::ObAddr &chosen) const;
  // Record (or replace) ls_id's reservation. Always returns OB_SUCCESS: an
  // alloc failure is logged and dropped — the chosen src stays valid, only
  // balancing degrades. Callers hold lock_.
  int upsert_(const share::ObLSID &ls_id, const common::ObAddr &src);
  // Bounded by # concurrent migrations on this observer (small) — O(n) is fine.
  // mutable: list_reservations() is const but still takes the lock.
  mutable common::ObSpinLock lock_;
  // Keep increasing across reset() so an old snapshot cannot match a reinsert.
  uint64_t next_version_;
  common::ObSEArray<Entry, ENTRY_ARRAY_CNT> entries_;
  DISALLOW_COPY_AND_ASSIGN(ObMigrationSrcBook);
};

class ObStorageHAService : public lib::ThreadPool
{
public:
  ObStorageHAService();
  virtual ~ObStorageHAService();
  static int mtl_init(ObStorageHAService *&ha_service);

  int init(ObLSService *ls_service);
  void destroy();
  void run1() final;
  void wakeup();
  void stop();
  void wait();
  int start();

  ObMigrationSrcBook &get_migration_src_book() { return migration_src_book_; }

private:
  int get_ls_id_array_();
  int scheduler_ls_ha_handler_();
  int do_ha_handler_(const share::ObLSID &ls_id);
  // Background self-heal for the migration src book: for each reserved ls_id,
  // ask its migration handler whether a migration is still running; drop the
  // reservation when the handler confirms none exists, or when the LS itself
  // is already gone (its migration can no longer be running). Conservative —
  // any lookup error or uncertainty leaves the reservation in place, so a
  // running migration is never released early. Runs on the background tick,
  // throttled to MIGRATION_SRC_BOOK_SWEEP_INTERVAL_US.
  int sweep_leaked_src_reservations_();
  // True when ls_id has no in-flight migration task: the handler reports an
  // empty task list, or the LS itself no longer exists.
  int check_ls_migration_finished_(const share::ObLSID &ls_id, bool &finished);

#ifdef ERRSIM
  int errsim_set_ls_migration_status_hold_();
#endif

private:
  static const int64_t SCHEDULER_WAIT_TIME_MS = 1000L; // 1s
  static const int64_t MIGRATION_SRC_BOOK_SWEEP_INTERVAL_US = 10L * 60L * 1000L * 1000L; // 10min
  bool is_inited_;
  common::ObThreadCond thread_cond_;
  int64_t wakeup_cnt_;
  ObLSService *ls_service_;
  ObArray<share::ObLSID> ls_id_array_;
  ObMigrationSrcBook migration_src_book_;

  DISALLOW_COPY_AND_ASSIGN(ObStorageHAService);
};



}
}
#endif
