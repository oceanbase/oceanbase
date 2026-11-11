/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef SRC_SHARE_SCHEDULER_OB_SHARED_COMPACTION_THREAD_LIMITER_H_
#define SRC_SHARE_SCHEDULER_OB_SHARED_COMPACTION_THREAD_LIMITER_H_

#include "lib/ob_define.h"
#include "lib/lock/ob_mutex.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace share
{

static const int64_t MERGE_DAG_PRIO_CNT = 5;

// Two 32-bit words, each read/written as a WHOLE word via volatile store/load: a scheduling decision
// reads all fields of a word in one snapshot (enable+limit consistent), lock-free. Split by owner --
// a tenant writes only its own half (user_state_ / meta_state_) and reads the sibling's; no
// cross-tenant write race and no cross-tenant lock on the hot path.
//
// Writes to a half MUST hold the owning tenant's prio_rwlock_. Several same-tenant threads write the
// same half as whole-word write/RMW (reload: set_*_state/set_user_limit; workers: set_*_running_count;
// window-adapt timer: set_user_limit); prio_rwlock_ serializes them, which is why a plain store needs
// no CAS. Hoisting a write outside the lock would let two RMWs interleave and silently lose an update.
// EXCEPTION: the reset path (clear_shared_thread_limiters -> disable_shared_limiter) writes without
// the lock -- safe only because workers are already stopped and reload no longer reaches the tenant.
//
// volatile guarantees the compiler does not cache the value in a register and issues a real 32-bit
// load/store each time. On x86 an aligned 32-bit volatile store/load is inherently atomic (TSO).
// On ARM64 volatile does not add DMB, so readers may transiently see a slightly stale value -- this
// is acceptable because the limit is a soft constraint (config changes propagate via heartbeat,
// ~2-3s delay), and a momentary inaccuracy (running_count transiently over limit by 1-2) is
// acceptable and self-corrects next round.
class ObSharedCompactionThreadLimiter
{
public:
  ObSharedCompactionThreadLimiter();
  ~ObSharedCompactionThreadLimiter() = default;
  // Reset both halves to a clean disabled state (user not opted in, meta not opted in, no running).
  // Used by the pool when (re)assigning a slot so a reused slot never carries a previous tenant's
  // state. NOTE: meta defaults to disabled, so a half-bound slot reads is_enabled()==false until the
  // meta tenant actually binds and reloads -- the AND in is_enabled() is the sole guard.
  void reset();
  bool is_enabled() const;
  int64_t get_limit() const;
  int64_t get_user_running_count() const;
  int64_t get_meta_running_count() const;
  bool get_meta_has_waiting_dag() const;
  void get_schedule_snapshot(
      bool &out_enable_shared,
      int64_t &out_user_running,
      int64_t &out_meta_running,
      int64_t &out_limit,
      bool &out_meta_has_waiting_dag) const;
  void check_can_schedule(const bool is_meta_tenant,
                          bool &out_enable_shared,
                          bool &out_can_schedule) const;
  void check_need_pause(bool &out_enable_shared, bool &out_need_pause) const;
  void set_meta_has_waiting_dag(const bool has_waiting_dag);
  int set_user_state(const bool enable, const int64_t limit, const int64_t user_running);
  int set_user_running_count(const int64_t user_running);
  int set_user_limit(const int64_t limit);
  int set_meta_state(const bool meta_use_shared_limit, const int64_t meta_running);
  int set_meta_running_count(const int64_t meta_running);

  TO_STRING_KV("enable_shared_by_type", (0 != user_state_.enable_shared_by_type_),
               "meta_use_shared_limit", (0 != meta_state_.meta_use_shared_limit_),
               "meta_has_waiting_dag",  (0 != meta_state_.meta_has_waiting_dag_),
               "user_running_count",    static_cast<int64_t>(user_state_.user_running_count_),
               "meta_running_count",    static_cast<int64_t>(meta_state_.meta_running_count_),
               "limit",                 static_cast<int64_t>(user_state_.limit_));

private:
  static constexpr int64_t PACKED_BOOL_FIELD_BITS = 1;
  static constexpr int64_t PACKED_COUNT_FIELD_BITS = 14;
  static constexpr int64_t USER_STATE_RESERVED_BITS =
      32 - PACKED_BOOL_FIELD_BITS - 2 * PACKED_COUNT_FIELD_BITS;
  static constexpr int64_t META_STATE_RESERVED_BITS =
      32 - 2 * PACKED_BOOL_FIELD_BITS - PACKED_COUNT_FIELD_BITS;
  static constexpr int64_t MAX_PACKED_FIELD = ((1LL << (PACKED_COUNT_FIELD_BITS - 1)) - 1);
  static bool is_packed_field_in_range(const int64_t value);
private:

  union UserState
  {
    UserState() : value_(0) {}
    int32_t value_;
    struct
    {
      int32_t enable_shared_by_type_ : PACKED_BOOL_FIELD_BITS;
      int32_t limit_ :                 PACKED_COUNT_FIELD_BITS;
      int32_t user_running_count_ :    PACKED_COUNT_FIELD_BITS;
      int32_t reserved_ :              USER_STATE_RESERVED_BITS;
    };
  };
  union MetaState
  {
    MetaState() : value_(0) {}
    int32_t value_;
    struct
    {
      int32_t meta_use_shared_limit_ : PACKED_BOOL_FIELD_BITS;
      int32_t meta_has_waiting_dag_  : PACKED_BOOL_FIELD_BITS;
      int32_t meta_running_count_    : PACKED_COUNT_FIELD_BITS;
      int32_t reserved_              : META_STATE_RESERVED_BITS;
    };
  };

  volatile UserState user_state_; // packed UserState: user writes the whole 32-bit word (volatile store), both read
  volatile MetaState meta_state_; // packed MetaState: meta writes the whole 32-bit word (volatile store), both read

  DISALLOW_COPY_AND_ASSIGN(ObSharedCompactionThreadLimiter);
};

// Process-resident pool of shared compaction-thread limiters.
// Each slot holds MERGE_DAG_PRIO_CNT limiters and is co-borrowed by a user tenant and its meta
// tenant, which resolve to the SAME slot via the normalized user tenant id (gen_user_tenant_id).
//
// MAX_SLOT_CNT slots are embedded in the singleton (no heap allocation). Their addresses never
// change, which is required because ObDagPrioScheduler holds a raw ObSharedCompactionThreadLimiter*
// into a slot. allocated_slot_cnt_ is the high-water mark of slots ever handed out; when it reaches
// MAX_SLOT_CNT and no free slot is available, bind() returns OB_SIZE_OVERFLOW and the tenant falls
// back to independent compaction limits (no shared limiter pointer is set).
//
// bind()/release() take a mutex but are only called on the tenant init/destroy cold path.
class ObSharedCompactionLimiterPool
{
public:
  static ObSharedCompactionLimiterPool &get_instance();
  int bind(const uint64_t tenant_id, ObSharedCompactionThreadLimiter *&out_limiters);
  void release(const uint64_t tenant_id);
private:
  static constexpr int64_t MAX_SLOT_CNT = 100;
  struct Slot
  {
    Slot()
      : user_tenant_id_(OB_INVALID_TENANT_ID),
        meta_tenant_id_(OB_INVALID_TENANT_ID)
    {}
    ~Slot() { reset(); };
    // Clears all limiters and marks the slot free for reuse.
    void reset();
    ObSharedCompactionThreadLimiter limiters_[MERGE_DAG_PRIO_CNT];
    uint64_t user_tenant_id_;
    uint64_t meta_tenant_id_;
    TO_STRING_KV(K_(user_tenant_id), K_(meta_tenant_id));
  };
  ObSharedCompactionLimiterPool();
  ~ObSharedCompactionLimiterPool() = default;
  static uint64_t get_slot_key_(const Slot &slot);
  int occupy_slot_side_(const uint64_t tenant_id, const bool is_user, Slot &slot);
  int vacate_slot_side_(const bool is_user, Slot &slot);
  void find_occupied_or_free_slot_(const uint64_t key, int64_t &occupied_idx, int64_t &free_idx) const;
  int allocate_new_slot_(const uint64_t key, int64_t &slot_idx);
  int acquire_slot_for_bind_locked_(const uint64_t key,
                                    int64_t &slot_idx,
                                    bool &occupied_by_sibling);
  Slot slots_[MAX_SLOT_CNT];   // embedded storage; addresses fixed for process lifetime
  int64_t allocated_slot_cnt_; // high-water mark; never decremented
  lib::ObMutex lock_;          // guards slot bookkeeping; only contended on tenant init/destroy
  DISALLOW_COPY_AND_ASSIGN(ObSharedCompactionLimiterPool);
};

} // namespace share
} // namespace oceanbase

#endif // SRC_SHARE_SCHEDULER_OB_SHARED_COMPACTION_THREAD_LIMITER_H_
