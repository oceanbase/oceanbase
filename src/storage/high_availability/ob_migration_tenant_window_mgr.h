/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_MIGRATION_TENANT_WINDOW_MGR_H_
#define OCEANBASE_STORAGE_MIGRATION_TENANT_WINDOW_MGR_H_

#include "lib/hash/ob_hashmap.h"
#include "lib/list/ob_dlink_node.h"
#include "lib/list/ob_dlist.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/lock/ob_thread_cond.h"
#include "lib/ob_define.h"
#include "lib/utility/ob_print_utils.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"

namespace oceanbase
{
namespace storage
{
struct ObMigrationControllerInfo
{
  enum class Role : int64_t
  {
    SOURCE = 0,
    DEST = 1,
    MAX_ROLE,
  };

  ObMigrationControllerInfo()
      : role_(Role::MAX_ROLE),
        dag_prio_(share::ObDagPrio::DAG_PRIO_MAX),
        hold_count_(0)
  {}
  ObMigrationControllerInfo(const Role role,
                            const share::ObDagPrio::ObDagPrioEnum dag_prio)
      : role_(role), dag_prio_(dag_prio), hold_count_(0)
  {}

  TO_STRING_KV(K_(role), K_(dag_prio), K_(hold_count));

  Role role_;
  share::ObDagPrio::ObDagPrioEnum dag_prio_;
  int64_t hold_count_;
};

class ObMigrationTenantWindowSlot
{
public:
  ObMigrationTenantWindowSlot();
  void reset();
  bool is_valid() const;

  char *buf() const { return buf_ptr_; }
  int64_t buf_cap() const { return buf_cap_; }

  TO_STRING_KV(KP_(buf_ptr), K_(buf_cap));

private:
  friend class ObMigrationTenantWindowMgr; // mgr can modify it but controller can't
  char *buf_ptr_;
  int64_t buf_cap_;
};

class ObMigrationTenantWindowMgr
{
public:
  ObMigrationTenantWindowMgr();
  ~ObMigrationTenantWindowMgr();

  int init(
      const uint64_t tenant_id,
      const int64_t max_slots,
      const int64_t slot_buf_size,
      const int64_t capacity);
  int mark_stopped();
  int safe_to_destroy(bool &is_safe);

  void destroy();


  bool is_inited() const;
  bool is_stopped() const;
  int get_tenant_id(uint64_t &tenant_id) const;
  int get_slot_buf_size(int64_t &slot_buf_size) const;
  int get_free_slot_count(int64_t &count) const;
  int get_total_slot_count(int64_t &count) const;

  int apply_slots(
      const int64_t request_count,
      const int64_t ctrl_id,
      common::ObIArray<ObMigrationTenantWindowSlot> &out_slots,
      int64_t &granted_count);
  int free_slots(const common::ObIArray<ObMigrationTenantWindowSlot> &slots, const int64_t ctrl_id);
  int resize(const int64_t new_max_slots, int64_t &actual_max_slots);


  int register_controller(const ObMigrationControllerInfo::Role role,
                          const share::ObDagPrio::ObDagPrioEnum dag_prio,
                          int64_t &ctrl_id);
  int unregister_controller(const int64_t ctrl_id);

  // ---------- synchronization ----------
  int wait_for_free_slot(const uint64_t timeout_us);
  void broadcast_slot_release();

  static int64_t calc_max_slots_by_memory(
      const int64_t mem_limit_byte,
      const int64_t slot_buf_size);

  // Test-only: resize upper bound (== capacity_, fixed at init).
  int64_t get_internal_capacity_for_test() const;
  int64_t get_node_count_for_test() const;
  static constexpr int64_t MIGRATION_WINDOW_DEFAULT_SLOT_BUF_SIZE = 2L * 1024L * 1024L;  // 2 MB
  int init_from_tenant_config(const uint64_t tenant_id);
  int reload_config_from_tenant(const uint64_t tenant_id);

private:

  struct SlotMeta : public common::ObDLinkBase<SlotMeta>
  {
    char *buf_ptr_;
    int64_t ctrl_id_;
    SlotMeta() : buf_ptr_(nullptr), ctrl_id_(common::OB_INVALID_INDEX_INT64) {}
    ~SlotMeta();
    TO_STRING_KV(KP_(buf_ptr), K_(ctrl_id));
  };

  typedef common::hash::ObHashMap<int64_t, ObMigrationControllerInfo, common::hash::NoPthreadDefendMode> CtrlInfoMap;

  static const int64_t DEFAULT_CTRL_INFO_MAP_SIZE = 64;
  static const int64_t DRAIN_SLICE_US = 100 * 1000;           // 100ms per drain poll
  static const int64_t DRAIN_WARN_US = 60L * 1000L * 1000L;   // 60s between drain warnings

  // -- node lifecycle helpers (caller holds lock_) --
  SlotMeta *new_slot_meta_();
  void delete_slot_meta_(SlotMeta *&node);
  int ensure_slot_buffer_(SlotMeta *node);
  SlotMeta *find_in_use_by_buf_(const char *buf_ptr);

  // -- resize helpers (caller holds lock_, caller already updated max_slots_) --
  int shrink_(const int64_t target_alive);
  int adjust_ctrl_hold_count_(const int64_t ctrl_id, const int64_t delta);
  void cleanup_resources_();

  // caller holds lock_
  int64_t total_node_count_() const
  {
    return free_list_.get_size() + in_use_list_.get_size();
  }

  int calc_dag_prio_concurrency_quota_(const ObMigrationControllerInfo &info, int64_t &quota) const;

  // ========== member variables ==========
  bool is_inited_;                         // true between init() success and destroy() teardown
  bool stopped_;                           // true after mark_stopped() until destroy() resets

  uint64_t tenant_id_;
  int64_t max_slots_;                      // logical cap on alive (in-use + idle) slots; mutated by resize()
  int64_t capacity_;                       // resize upper bound; fixed at init
  int64_t slot_buf_size_;

  // -- slot storage --
  common::ObDList<SlotMeta> free_list_;
  common::ObDList<SlotMeta> in_use_list_;

  // -- fair-share accounting --
  CtrlInfoMap ctrl_info_map_;
  int64_t next_ctrl_id_;

  // -- synchronization --
  mutable common::ObSpinLock lock_;
  common::ObThreadCond slot_release_cond_; // waiters block here for free slots

  DISALLOW_COPY_AND_ASSIGN(ObMigrationTenantWindowMgr);
};

} // namespace storage
} // namespace oceanbase

#endif  // OCEANBASE_STORAGE_MIGRATION_TENANT_WINDOW_MGR_H_
