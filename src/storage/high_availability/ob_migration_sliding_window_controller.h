/**
* Copyright (c) 2021 OceanBase
* SPDX-License-Identifier: Apache-2.0
*/

#ifndef OCEANBASE_STORAGE_MIGRATION_SLIDING_WINDOW_CONTROLLER_H_
#define OCEANBASE_STORAGE_MIGRATION_SLIDING_WINDOW_CONTROLLER_H_

#include "lib/allocator/ob_malloc.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/lock/ob_spin_lock.h"
#include "storage/high_availability/ob_migration_tenant_window_mgr.h"

namespace oceanbase
{
namespace storage
{

class ObIMigrationWindowSlidCallback
{
public:
  virtual ~ObIMigrationWindowSlidCallback() = default;

  // granted_start_seq / granted_slot_count: start seq and count of *new* mgr-backed slots granted
  //   in the refill grow after this slide batch (not the internal slid_count of head advances).
  virtual void on_window_slid(const int64_t granted_start_seq,
                              const int64_t granted_slot_count) = 0;
};

class ObMigrationSlidingWindowSourceController;
class ObMigrationSlidingWindowDestController;

template <typename TController>
class ObVectorIndexMigrationHandleT;

class ObMigrationSlidingWindowController
{
public:
  static constexpr int64_t PER_APPLY_SLOT_COUNT           = 1;
  static constexpr int64_t SLOT_MAP_INIT_BUCKET_COUNT     = 128;
  // ---- wait / poll constants (microseconds) ----
  static constexpr int64_t DEFAULT_OP_TIMEOUT_US          = 60L * 1000L * 1000L;  // 60s, fill_slot_ / dest get_next_consume_data default total timeout
  static constexpr int64_t SLOT_ACQUIRE_RETRY_INTERVAL_US = 100000;               // 100ms, back-off before retrying slot apply from mgr
  static constexpr int64_t SLOT_POLL_INTERVAL_US          = 1000;                 // 1ms, poll interval for slot data ready / window full
  static constexpr int64_t SLIDE_HEAD_SUCCESS_LOG_INTERVAL_US = 10L * 1000L * 1000L;  // 10s

  virtual ~ObMigrationSlidingWindowController();

  int init(ObMigrationTenantWindowMgr *window_mgr,
           ObIMigrationWindowSlidCallback *on_window_slid,
           const share::ObDagPrio::ObDagPrioEnum dag_prio);
  void destroy();
  void stop();
  void wakeup_waiters();

  int set_total_task_count(const int64_t total_task_count);
  // Monotonically shrink total_task_count (only decreases, never increases).
  // Used when async RPC callbacks discover the source is exhausted at a given seq.
  int shrink_total_task_count(const int64_t total_task_count);

  bool is_inited() const { return ATOMIC_LOAD(&is_inited_); }
  int get_runtime_snapshot(int64_t &head_seq, int64_t &window_size);
  int get_controller_id(int64_t &controller_id) const;
  int get_slot_buf_size(int64_t &slot_buf_size);
  int get_role_type(ObMigrationControllerInfo::Role &role_type) const;

  int64_t get_ref_count() const { return ATOMIC_LOAD(&ref_count_); }

  TO_STRING_KV(K_(role_type), K_(controller_id), K_(window_size), K_(head_seq),
                K_(total_task_count), K_(stopped),
                "ref_count", get_ref_count());

protected:
  explicit ObMigrationSlidingWindowController(const ObMigrationControllerInfo::Role role_type);

  int fill_slot_(const int64_t seq_idx, const char *data, const int64_t data_len);
  // Single "try once" consume primitive shared by source and dest subclasses.
  // Contract:
  //   OB_SUCCESS           slot READY, data copied out, slot erased, mgr slot freed,
  //                        head slid if possible.
  //   OB_EAGAIN            slot not in map yet, or present but not READY. Caller
  //                        decides whether to retry (dest loop) or surface (source).
  //   OB_CANCELED          stopped_ is set.
  //   OB_ITER_END          seq_idx >= total_task_count_.
  //   OB_BUF_NOT_ENOUGH    out_buf_len < slot.data_len_.

  int try_consume_slot_(const int64_t seq_idx, char *out_buf,
                        const int64_t out_buf_len, int64_t &data_len);

  ObMigrationControllerInfo::Role role_type_;
  int64_t controller_id_;

  int try_grow_window_(int64_t &out_used_count, int64_t *out_granted_start_seq = nullptr);
  int check_seq_in_window_(const int64_t seq_idx, bool &slot_in_window);

  bool is_stopped_() const { return ATOMIC_LOAD(&stopped_); }
  ObMigrationTenantWindowMgr *get_window_mgr_() const { return ATOMIC_LOAD(&window_mgr_); }
  ObIMigrationWindowSlidCallback *get_on_window_slid_() const { return on_window_slid_; }
  virtual void on_after_slide_head_() { return; }

private:
  template <typename T> friend class ObVectorIndexMigrationHandleT;
  int64_t inc_ref_() { return ATOMIC_AAF(&ref_count_, 1); }
  int64_t dec_ref_();

  bool is_inited_;

  struct Slot
  {
    enum class State : int64_t
    {
      RESERVED = 0,   // buffer allocated, waiting for data
      FILLING = 1,    // data write in progress
      READY = 2,      // data filled, waiting to be consumed
      MAX
    };
    Slot();
    void reset();

    State state_;
    int64_t seq_;                          // logical sequence number
    int64_t data_len_;
    ObMigrationTenantWindowSlot mgr_slot_; // borrowed buffer-pool slot from window_mgr
  };

  typedef common::hash::ObHashMap<int64_t, Slot, common::hash::NoPthreadDefendMode> SlotMap;

  int try_slide_head_();
  // After apply_slots: insert granted slots into slot_map_, rollback on failure, free excess to mgr.
  int try_grow_commit_granted_slots_(ObMigrationTenantWindowMgr *mgr,
      const common::ObIArray<ObMigrationTenantWindowSlot> &granted,
      const int64_t granted_count,
      int64_t *out_granted_start_seq,
      int64_t &used_count);

  // After seq is in-window: RESERVED -> FILLING, memcpy payload, FILLING -> READY.
  int fill_slot_materialize_payload_(const int64_t seq_idx, const char *data, const int64_t data_len);

  bool stopped_;
  common::ObSpinLock window_lock_;  // protects head_seq_, window_size_, slot_map_
  int64_t window_size_;
  int64_t head_seq_;
  int64_t total_task_count_;
  SlotMap slot_map_;               // seq -> Slot, quick access for fill_slot_ / try_consume_slot_

  ObMigrationTenantWindowMgr *window_mgr_;
  ObIMigrationWindowSlidCallback *on_window_slid_;  // callback for window slid
  int64_t last_slide_head_log_us_;

  int64_t ref_count_;

  DISALLOW_COPY_AND_ASSIGN(ObMigrationSlidingWindowController);
};

// Refcounted handle template.
template <typename TController>
class ObVectorIndexMigrationHandleT
{
public:
  ObVectorIndexMigrationHandleT() : ctrl_(nullptr) {}
  explicit ObVectorIndexMigrationHandleT(TController *ctrl) : ctrl_(ctrl)
  {
    if (OB_NOT_NULL(ctrl_)) {
      ctrl_->inc_ref_();
    }
  }
  ObVectorIndexMigrationHandleT(const ObVectorIndexMigrationHandleT &other) : ctrl_(other.ctrl_)
  {
    if (OB_NOT_NULL(ctrl_)) {
      ctrl_->inc_ref_();
    }
  }
  ObVectorIndexMigrationHandleT &operator=(const ObVectorIndexMigrationHandleT &other)
  {
    if (this != &other) {
      reset();
      ctrl_ = other.ctrl_;
      if (OB_NOT_NULL(ctrl_)) {
        ctrl_->inc_ref_();
      }
    }
    return *this;
  }

  void reset()
  {
    if (OB_NOT_NULL(ctrl_)) {
      TController *ctrl = ctrl_;
      ctrl_ = nullptr;
      if (0 == ctrl->dec_ref_()) {
        OB_DELETE(TController, "MigVecIdxCtrl", ctrl);
      }
    }
  }
  ~ObVectorIndexMigrationHandleT() { reset(); }

  bool is_valid() const { return ctrl_ != nullptr; }
  TController *get() const { return ctrl_; }
  TController *operator->() const { return ctrl_; }
  TController &operator*() const { return *ctrl_; }

  TO_STRING_KV(KP_(ctrl));

private:
  TController *ctrl_;
};

typedef ObVectorIndexMigrationHandleT<ObMigrationSlidingWindowSourceController> ObMigrationSlidingWindowSourceHandle;
typedef ObVectorIndexMigrationHandleT<ObMigrationSlidingWindowDestController>   ObMigrationSlidingWindowDestHandle;

// Source: single DAG producer thread produces data in order; multiple RPC network threads consume data out of order.
class ObMigrationSlidingWindowSourceController : public ObMigrationSlidingWindowController
{
public:
  ObMigrationSlidingWindowSourceController();
  virtual ~ObMigrationSlidingWindowSourceController() = default;

  static int create(ObMigrationTenantWindowMgr *window_mgr,
                    ObIMigrationWindowSlidCallback *on_window_slid,
                    const share::ObDagPrio::ObDagPrioEnum dag_prio,
                    ObMigrationSlidingWindowSourceHandle &out_handle);

  int generate_next_data(const char *data, const int64_t data_len, const int64_t wait_timeout_us = 0);
  // Non-blocking: returns OB_EAGAIN when data is not yet produced for seq_idx.
  // Source-side callers are RPC handlers that must not block worker threads.
  int try_get_data(const int64_t seq_idx, char *out_buf, const int64_t out_buf_len,
                  int64_t &data_len);

  int64_t get_next_generate_seq() const { return ATOMIC_LOAD(&next_generate_seq_); }

private:
  int acquire_window_slot_(const int64_t seq_idx, const int64_t wait_timeout_us);

  int64_t next_generate_seq_;

  DISALLOW_COPY_AND_ASSIGN(ObMigrationSlidingWindowSourceController);
};

// Dest: multiple network IO threads fill out of order; single DAG consumer thread consumes strictly in order.
class ObMigrationSlidingWindowDestController : public ObMigrationSlidingWindowController
{
public:
  ObMigrationSlidingWindowDestController();
  virtual ~ObMigrationSlidingWindowDestController() = default;

  static int create(ObMigrationTenantWindowMgr *window_mgr,
                    ObIMigrationWindowSlidCallback *on_window_slid,
                    const share::ObDagPrio::ObDagPrioEnum dag_prio,
                    ObMigrationSlidingWindowDestHandle &out_handle);

  int fill_data(const int64_t seq_idx, const char *data, const int64_t data_len);
  int get_next_consume_data(char *out_buf, const int64_t out_buf_len,
                            int64_t &data_len, const int64_t wait_timeout_us = 0);

private:
  void on_after_slide_head_() override;  // Refill window slack and notify fetch driver to dispatch the next batch of RPCs.

  int64_t next_consume_seq_;

  DISALLOW_COPY_AND_ASSIGN(ObMigrationSlidingWindowDestController);
};

} // namespace storage
} // namespace oceanbase

#endif  // OCEANBASE_STORAGE_MIGRATION_SLIDING_WINDOW_CONTROLLER_H_
