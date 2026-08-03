/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "ob_storage_ha_service.h"
#include "storage/meta_store/ob_server_storage_meta_service.h"
#include "storage/ls/ob_ls.h"
#include "ob_ls_migration_handler.h"
#ifdef ERRSIM
#include "observer/ob_server_event_history_table_operator.h"
#endif

namespace oceanbase
{
using namespace common;
namespace storage
{

ERRSIM_POINT_DEF(EN_STORAGE_HA_SERVICE_SET_LS_MIGRATION_STATUS_HOLD);
ObMigrationSrcBook::ObMigrationSrcBook()
  : lock_(ObLatchIds::OB_LS_MIGRATION_LOCK),
    next_version_(0),
    entries_()
{
  entries_.set_attr(ObMemAttr(MTL_ID(), "MigSrcBook"));
}

void ObMigrationSrcBook::reset()
{
  ObSpinLockGuard guard(lock_);
  entries_.reset();
}

int ObMigrationSrcBook::reserve(
    const share::ObLSID &ls_id, const common::ObAddr &src)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ls_id.is_valid() || !src.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id), K(src));
  } else {
    ObSpinLockGuard guard(lock_);
    if (OB_FAIL(upsert_(ls_id, src))) {
      LOG_WARN("failed to reserve migration src", K(ret), K(ls_id), K(src));
    }
  }
  return ret;
}

int ObMigrationSrcBook::release(const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls_id", K(ret), K(ls_id));
  } else {
    ObSpinLockGuard guard(lock_);
    for (int64_t i = 0; i < entries_.count(); ++i) {
      if (entries_.at(i).ls_id_ == ls_id) {
        const ObAddr released = entries_.at(i).src_;
        if (OB_FAIL(entries_.remove(i))) {
          LOG_WARN("remove failed", K(ret), K(ls_id));
        } else {
          LOG_TRACE("migration src released", K(ls_id), K(released),
                   "count", entries_.count());
        }
        break;
      }
    }
  }
  return ret;
}

int ObMigrationSrcBook::release_if_version_matches(
    const ReservationSnapshot &snapshot, bool &released)
{
  int ret = OB_SUCCESS;
  released = false;
  if (OB_UNLIKELY(!snapshot.ls_id_.is_valid() || 0 == snapshot.version_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid reservation snapshot", K(ret), K(snapshot));
  } else {
    ObSpinLockGuard guard(lock_);
    for (int64_t i = 0; i < entries_.count(); ++i) {
      if (entries_.at(i).ls_id_ == snapshot.ls_id_) {
        if (entries_.at(i).version_ == snapshot.version_) {
          if (OB_FAIL(entries_.remove(i))) {
            LOG_WARN("remove failed", K(ret), K(snapshot));
          } else {
            released = true;
          }
        }
        break;
      }
    }
  }
  return ret;
}

int ObMigrationSrcBook::pick_coolest(
    const share::ObLSID &ls_id,
    const ObIArray<ObAddr> &candidates,
    ObAddr &chosen)
{
  int ret = OB_SUCCESS;
#ifdef ERRSIM
  int64_t occupied_candidate_count_before = 0;
  int64_t chosen_heat_before = -1;
#endif
  chosen.reset();
  if (OB_UNLIKELY(!ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls_id", K(ret), K(ls_id));
  } else if (OB_UNLIKELY(candidates.empty())) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("no candidates", K(ret));
  } else {
    // The lock spans heat computation, pick AND the reservation update,
    // so concurrent picks immediately see each other's choice — otherwise
    // two concurrent migrations both observe the same src as coolest.
    {
      ObSpinLockGuard guard(lock_);
#ifdef ERRSIM
      for (int64_t i = 0; i < candidates.count(); ++i) {
        if (get_heat_(candidates.at(i), ls_id) > 0) {
          ++occupied_candidate_count_before;
        }
      }
#endif
      if (OB_FAIL(find_coolest_(ls_id, candidates, chosen))) {
        LOG_WARN("find coolest failed", K(ret), K(ls_id));
      } else {
#ifdef ERRSIM
        // Sample before upsert: this is the state that influenced this pick.
        chosen_heat_before = get_heat_(chosen, ls_id);
#endif
        if (OB_FAIL(upsert_(ls_id, chosen))) {
          LOG_WARN("upsert failed", K(ret), K(ls_id), K(chosen));
        } else {
          LOG_DEBUG("picked coolest src", K(ls_id), K(chosen),
              "candidate_count", candidates.count(), "count", entries_.count());
        }
      }
    }
#ifdef ERRSIM
    // Emit outside lock_: the event is diagnostic only and must neither extend
    // the critical section nor affect source selection.
    if (OB_SUCC(ret) && SERVER_EVENT_INSTANCE.is_inited()) {
      (void) SERVER_EVENT_ADD(
          "storage_ha", "migration_src_book_pick",
          "tenant_id", MTL_ID(),
          "ls_id", ls_id.id(),
          "chosen_src", chosen,
          "candidate_count", candidates.count(),
          "occupied_before", occupied_candidate_count_before,
          "chosen_heat_before", chosen_heat_before);
    }
#endif
  }
  return ret;
}

int64_t ObMigrationSrcBook::get_heat_(
    const ObAddr &addr, const share::ObLSID &excluded_ls_id) const
{
  int64_t heat = 0;
  for (int64_t i = 0; i < entries_.count(); ++i) {
    if (entries_.at(i).ls_id_ != excluded_ls_id
        && entries_.at(i).src_ == addr) {
      ++heat;
    }
  }
  return heat;
}

int ObMigrationSrcBook::find_coolest_(
    const share::ObLSID &ls_id,
    const ObIArray<ObAddr> &candidates,
    ObAddr &chosen) const
{
  int ret = OB_SUCCESS;
  // Single pass: track min heat, reservoir-sample among ties so each
  // coolest candidate is chosen with equal probability.
  int64_t min_heat = INT64_MAX;
  int64_t tied_cnt = 0;
  for (int64_t i = 0; i < candidates.count(); ++i) {
    // A repick replaces ls_id's old reservation, so its own entry must not
    // influence the heat comparison for the new choice.
    const int64_t heat = get_heat_(candidates.at(i), ls_id);
    if (heat < min_heat) {
      min_heat = heat;
      tied_cnt = 1;
      chosen = candidates.at(i);
    } else if (heat == min_heat && 0 == rand() % ++tied_cnt) {
      chosen = candidates.at(i);
    }
  }
  if (!chosen.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no coolest candidate found", K(ret), K(candidates));
  }
  return ret;
}

int ObMigrationSrcBook::upsert_(const share::ObLSID &ls_id, const ObAddr &src)
{
  int ret = OB_SUCCESS;
  bool found = false;
  const uint64_t version = ++next_version_;
  for (int64_t i = 0; !found && i < entries_.count(); ++i) {
    if (entries_.at(i).ls_id_ == ls_id) {
      entries_.at(i).src_ = src;
      entries_.at(i).version_ = version;
      found = true;
    }
  }
  if (!found) {
    Entry e;
    e.ls_id_ = ls_id;
    e.src_ = src;
    e.version_ = version;
    // Bookkeeping failure must not fail the caller — the chosen src is
    // still valid, only balancing degrades, so the error stops here.
    if (OB_FAIL(entries_.push_back(e))) {
      LOG_WARN("record migration src failed, balancing degrades", K(ret), K(ls_id), K(src));
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObMigrationSrcBook::list_reservations(
    ObIArray<ReservationSnapshot> &reservations) const
{
  int ret = OB_SUCCESS;
  reservations.reset();
  ObSpinLockGuard guard(lock_);
  for (int64_t i = 0; OB_SUCC(ret) && i < entries_.count(); ++i) {
    ReservationSnapshot snapshot;
    snapshot.ls_id_ = entries_.at(i).ls_id_;
    snapshot.version_ = entries_.at(i).version_;
    if (OB_FAIL(reservations.push_back(snapshot))) {
      LOG_WARN("failed to push back reservation snapshot", K(ret), K(snapshot));
    }
  }
  return ret;
}

ObStorageHAService::ObStorageHAService()
  : is_inited_(false),
    thread_cond_(),
    wakeup_cnt_(0),
    ls_service_(nullptr),
    migration_src_book_()
{
}

ObStorageHAService::~ObStorageHAService()
{
}

int ObStorageHAService::mtl_init(ObStorageHAService *&ha_service)
{
  int ret = OB_SUCCESS;
  ObLSService *ls_service = nullptr;

  ha_service->ls_id_array_.set_attr(ObMemAttr(MTL_ID(), "ls_id"));
  if (OB_ISNULL(ls_service =  (MTL(ObLSService *)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service should not be NULL", K(ret), KP(ls_service));
  } else if (OB_FAIL(ha_service->init(ls_service))) {
    LOG_WARN("failed to init ha service", K(ret), KP(ls_service));
  }
  return ret;
}

int ObStorageHAService::init(
    ObLSService *ls_service)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("storage ha service is aleady init", K(ret));
  } else if (OB_ISNULL(ls_service)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init high avaiable handler mgr get invalid argument", K(ret), KP(ls_service));
  } else if (OB_FAIL(thread_cond_.init(ObWaitEventIds::HA_SERVICE_COND_WAIT))) {
    LOG_WARN("failed to init ha service thread cond", K(ret));
  } else {
    lib::ThreadPool::set_run_wrapper(MTL_CTX());
    ls_service_ = ls_service;
    is_inited_ = true;
  }
  return ret;
}

void ObStorageHAService::wakeup()
{
  ObThreadCondGuard cond_guard(thread_cond_);
  wakeup_cnt_++;
  thread_cond_.signal();
}

void ObStorageHAService::destroy()
{
  if (is_inited_) {
    COMMON_LOG(INFO, "ObStorageHAService starts to destroy");
    thread_cond_.destroy();
    wakeup_cnt_ = 0;
    is_inited_ = false;
    migration_src_book_.reset();
    COMMON_LOG(INFO, "ObStorageHAService destroyed");
  }
}

void ObStorageHAService::stop()
{
  if (is_inited_) {
    COMMON_LOG(INFO, "ObStorageHAService starts to stop");
    ThreadPool::stop();
    wakeup();
    COMMON_LOG(INFO, "ObStorageHAService stopped");
  }
}

void ObStorageHAService::wait()
{
  if (is_inited_) {
    COMMON_LOG(INFO, "ObStorageHAService starts to wait");
    ThreadPool::wait();
    COMMON_LOG(INFO, "ObStorageHAService finish to wait");
  }
}

int ObStorageHAService::start()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage ha service do not init", K(ret));
  } else {
    if (OB_FAIL(lib::ThreadPool::start())) {
      COMMON_LOG(WARN, "ObStorageHAService start thread failed", K(ret));
    } else {
      COMMON_LOG(INFO, "ObStorageHAService start");
    }
  }
  return ret;
}

void ObStorageHAService::run1()
{
  int ret = OB_SUCCESS;
  lib::set_thread_name("HAService");

  while (!has_set_stop()) {
    ls_id_array_.reset();

    if (!SERVER_STORAGE_META_SERVICE.is_started()) {
      ret = OB_SERVER_IS_INIT;
      LOG_WARN("server is not serving", K(ret), K(GCTX.status_));
    } else if (OB_FAIL(get_ls_id_array_())) {
      LOG_WARN("failed to get ls id array", K(ret));
    } else if (OB_FAIL(scheduler_ls_ha_handler_())) {
      LOG_WARN("failed to do scheduler ls ha handler", K(ret));
    }

    // Backstop for leaked src reservations (see sweep_leaked_src_reservations_).
    // Independent of the handler result above: run it on its own ~10min cadence
    // regardless of whether this tick's scheduling succeeded.
    if (REACH_THREAD_TIME_INTERVAL(MIGRATION_SRC_BOOK_SWEEP_INTERVAL_US)) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(sweep_leaked_src_reservations_())) {
        LOG_WARN_RET(tmp_ret, "failed to sweep leaked migration src reservations", K(tmp_ret));
      }
    }

#ifdef ERRSIM
    if (FAILEDx(errsim_set_ls_migration_status_hold_())) {
      LOG_WARN("failed to errsim set ls migration status hold", K(ret));
    }
#endif

    ObThreadCondGuard guard(thread_cond_);
    if (has_set_stop() || wakeup_cnt_ > 0) {
      wakeup_cnt_ = 0;
    } else {
      ObBKGDSessInActiveGuard inactive_guard;
      thread_cond_.wait(SCHEDULER_WAIT_TIME_MS);
    }
  }
}

int ObStorageHAService::get_ls_id_array_()
{
  int ret = OB_SUCCESS;
  ls_id_array_.reset();
  ObSharedGuard<ObLSIterator> ls_iter_guard;
  ObLSIterator *ls_iter = nullptr;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage ha service do not init", K(ret));
  } else if (OB_FAIL(ls_service_->get_ls_iter(ls_iter_guard, ObLSGetMod::HA_MOD))) {
    LOG_WARN("failed to get ls iter", K(ret));
  } else if (OB_ISNULL(ls_iter = ls_iter_guard.get_ptr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls iter should not be NULL", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      ObLS *ls = nullptr;
      if (OB_FAIL(ls_iter->get_next(ls))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        }
      } else if (OB_ISNULL(ls)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("log stream should not be NULL", K(ret), KP(ls));
      } else if (OB_FAIL(ls_id_array_.push_back(ls->get_ls_id()))) {
        LOG_WARN("failed to push ls id into array", K(ret), KPC(ls));
      }
    }
  }
  return ret;
}

int ObStorageHAService::scheduler_ls_ha_handler_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage ha service do not init", K(ret));
  } else {
    std::random_shuffle(ls_id_array_.begin(), ls_id_array_.end());
    LOG_INFO("start do ls ha handler", K(ls_id_array_));

    for (int64_t i = 0; OB_SUCC(ret) && i < ls_id_array_.count(); ++i) {
      const share::ObLSID &ls_id = ls_id_array_.at(i);
      if (OB_SUCCESS != (tmp_ret = do_ha_handler_(ls_id))) {
        //The purpose of using tmp_ret here is to not block the scheduling of other ls afterward
        LOG_WARN("failed to do ha handler", K(tmp_ret), K(ls_id));
      }
    }
  }
  return ret;
}

int ObStorageHAService::do_ha_handler_(const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage ha service do not init", K(ret));
  } else if (!ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("do ha handler get invalid argument", K(ret), K(ls_id));
  } else if (OB_FAIL(ls_service_->get_ls(ls_id, ls_handle, ObLSGetMod::HA_MOD))) {
    LOG_WARN("failed to get ls", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), KP(ls), K(ls_id));
  } else {
    if (OB_SUCCESS != (tmp_ret = ls->get_ls_migration_handler()->process())) {
      LOG_WARN("failed to do ls migration handler process", K(tmp_ret), K(ls_id));
    }

    if (OB_SUCCESS != (tmp_ret = ls->get_ls_restore_handler()->process())) {
      LOG_WARN("failed to do ls restore handler process", K(tmp_ret), K(ls_id));
    }

    //ls->tablets transfer
  }
  return ret;
}

int ObStorageHAService::check_ls_migration_finished_(const share::ObLSID &ls_id, bool &finished)
{
  int ret = OB_SUCCESS;
  finished = false;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  ObLSMigrationTask task;
  ObLSMigrationHandlerStatus status = ObLSMigrationHandlerStatus::MAX_STATUS;
  if (OB_UNLIKELY(!ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls_id", K(ret), K(ls_id));
  } else if (OB_FAIL(ls_service_->get_ls(ls_id, ls_handle, ObLSGetMod::HA_MOD))) {
    if (OB_LS_NOT_EXIST == ret) {
      // the LS itself is gone (offline / removed) — its migration cannot be
      // running any more, so the reservation is definitely leaked.
      finished = true;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get ls", K(ret), K(ls_id));
    }
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), K(ls_id));
  } else if (OB_FAIL(ls->get_ls_migration_handler()->get_migration_task_and_handler_status(task, status))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      // handler has no task in its list — no migration is running for this LS.
      finished = true;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get migration task and handler status", K(ret), K(ls_id));
    }
  } else {
    // a task still exists — the migration is in flight, keep the reservation.
    finished = false;
  }
  return ret;
}

int ObStorageHAService::sweep_leaked_src_reservations_()
{
  int ret = OB_SUCCESS;
  ObSEArray<ObMigrationSrcBook::ReservationSnapshot, 8> reservations;
  reservations.set_attr(ObMemAttr(MTL_ID(), "LeakedSrcRes"));
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage ha service do not init", K(ret));
  } else if (OB_FAIL(migration_src_book_.list_reservations(reservations))) {
    LOG_WARN("failed to list reservations", K(ret));
  } else {
    for (int64_t i = 0; i < reservations.count(); ++i) {
      const ObMigrationSrcBook::ReservationSnapshot &snapshot = reservations.at(i);
      const share::ObLSID &ls_id = snapshot.ls_id_;
      bool finished = false;
      int tmp_ret = OB_SUCCESS;
      // Conservative: only release when the handler confirms no migration is
      // running. Any error leaves the reservation in place (never release a
      // live migration), and never blocks sweeping of the remaining ls_ids.
      if (OB_TMP_FAIL(check_ls_migration_finished_(ls_id, finished))) {
        LOG_WARN_RET(tmp_ret, "failed to check ls migration finished, keep reservation", K(tmp_ret), K(ls_id));
      } else if (finished) {
        bool released = false;
        if (OB_TMP_FAIL(migration_src_book_.release_if_version_matches(snapshot, released))) {
          LOG_WARN_RET(tmp_ret, "failed to release leaked migration src reservation",
              K(tmp_ret), K(snapshot));
        } else if (released) {
          LOG_INFO("released leaked migration src reservation", K(snapshot));
        }
      }
    }
  }
  return ret;
}

#ifdef ERRSIM
int ObStorageHAService::errsim_set_ls_migration_status_hold_()
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  const ObMigrationStatus migration_status = ObMigrationStatus::OB_MIGRATION_STATUS_HOLD;
  const bool write_slog = true;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage ha service do not init", K(ret));
  } else {
    ret = EN_STORAGE_HA_SERVICE_SET_LS_MIGRATION_STATUS_HOLD ? : OB_SUCCESS;
    const ObAddr &self = GCONF.self_addr_;
    if (OB_FAIL(ret)) {
      STORAGE_LOG(ERROR, "fake EN_STORAGE_HA_SERVICE_SET_LS_MIGRATION_STATUS_HOLD", K(ret));
      //overwrite ret
      ret = OB_SUCCESS;
      const ObString &errsim_server = GCONF.errsim_migration_src_server_addr.str();
      if (!errsim_server.empty()) {
        ObAddr tmp_errsim_addr;
        if (OB_FAIL(tmp_errsim_addr.parse_from_string(errsim_server))) {
          LOG_WARN("failed to parse from string", K(ret), K(errsim_server));
        } else if (self != tmp_errsim_addr) {
          //do nothing
        } else {
          const int64_t errsim_migration_ls_id = GCONF.errsim_migration_ls_id;
          const ObLSID ls_id(errsim_migration_ls_id);
          if (errsim_migration_ls_id <= 0 || !ls_id.is_valid()) {
            //do nothing
          } else if (OB_FAIL(ls_service_->get_ls(ls_id, ls_handle, ObLSGetMod::HA_MOD))) {
            LOG_WARN("failed to get ls", K(ret), K(ls_id));
          } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("ls should not be NULL", K(ret), KP(ls), K(ls_id));
          } else if (OB_FAIL(ls->set_migration_status(migration_status, ls->get_rebuild_seq(), write_slog))) {
            LOG_WARN("failed to set migration status", K(ret), KPC(ls), K(ls_id));
          }
        }
      }

    }
  }
  return ret;
}
#endif

}
}
