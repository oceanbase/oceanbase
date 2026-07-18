/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "storage/high_availability/ob_migration_tenant_window_mgr.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/stat/ob_latch_define.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/utility.h"
#include "lib/wait_event/ob_wait_event.h"
#include "share/ob_errno.h"
#include "share/scheduler/ob_dag_scheduler_config.h"

namespace oceanbase
{
namespace storage
{

ObMigrationTenantWindowSlot::ObMigrationTenantWindowSlot()
    : buf_ptr_(nullptr), buf_cap_(0)
{
}

void ObMigrationTenantWindowSlot::reset()
{
  buf_ptr_ = nullptr;
  buf_cap_ = 0;
}

bool ObMigrationTenantWindowSlot::is_valid() const
{
  return OB_NOT_NULL(buf_ptr_) && buf_cap_ > 0;
}

ObMigrationTenantWindowMgr::SlotMeta::~SlotMeta()
{
  if (OB_NOT_NULL(buf_ptr_)) {
    ob_free(buf_ptr_);
    buf_ptr_ = nullptr;
  }
}

ObMigrationTenantWindowMgr::ObMigrationTenantWindowMgr()
    : is_inited_(false),
      stopped_(false),
      tenant_id_(OB_INVALID_TENANT_ID),
      max_slots_(0),
      capacity_(0),
      slot_buf_size_(0),
      free_list_(),
      in_use_list_(),
      ctrl_info_map_(),
      next_ctrl_id_(1),
      lock_(common::ObLatchIds::OB_STORAGE_HA_STRUCT_LOCK),
      slot_release_cond_()
{
}

ObMigrationTenantWindowMgr::~ObMigrationTenantWindowMgr()
{
  destroy();
}

int64_t ObMigrationTenantWindowMgr::calc_max_slots_by_memory(
    const int64_t mem_limit_byte,
    const int64_t slot_buf_size)
{
  return (mem_limit_byte <= 0 || slot_buf_size <= 0) ? 0 : mem_limit_byte / slot_buf_size;
}

int ObMigrationTenantWindowMgr::init(
    const uint64_t tenant_id,
    const int64_t max_slots,
    const int64_t slot_buf_size,
    const int64_t capacity)
{
  int ret = OB_SUCCESS;
  common::ObSpinLockGuard guard(lock_);
  if (is_inited()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tenant window mgr already inited", KR(ret), K(tenant_id));
  } else if (OB_INVALID_TENANT_ID == tenant_id
            || max_slots <= 0
            || slot_buf_size <= 0
            || capacity <= 0
            || max_slots > capacity) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid init args", KR(ret), K(tenant_id), K(max_slots),
            K(slot_buf_size), K(capacity));
  } else if (OB_FAIL(slot_release_cond_.init(static_cast<int32_t>(
          common::ObWaitEventIds::HA_SERVICE_COND_WAIT)))) {
    LOG_WARN("fail to init slot release cond", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ctrl_info_map_.create(DEFAULT_CTRL_INFO_MAP_SIZE,
                lib::ObMemAttr(tenant_id, "MigVecIdxCtlHd")))) {
    LOG_WARN("fail to create ctrl info map", KR(ret), K(tenant_id));
  } else {
    // Lazy pool: no slot nodes are allocated up front.
    // The last unregister_controller drops every node.
    tenant_id_ = tenant_id;
    slot_buf_size_ = slot_buf_size;
    capacity_ = capacity;
    max_slots_ = max_slots;
    ATOMIC_STORE(&next_ctrl_id_, 1);
    ATOMIC_STORE(&stopped_, false);
    ATOMIC_STORE(&is_inited_, true);
  }
  return ret;
}

int ObMigrationTenantWindowMgr::mark_stopped()
{
  int ret = OB_SUCCESS;
  if (ATOMIC_BCAS(&stopped_, false, true)) {
    // We are the thread that flipped stopped_; broadcast once so waiters wake.
    broadcast_slot_release();
    LOG_INFO("tenant window mgr stopped", K_(tenant_id));
  }
  return ret;
}

int ObMigrationTenantWindowMgr::safe_to_destroy(bool &is_safe)
{
  int ret = OB_SUCCESS;
  is_safe = false;
  if (!is_stopped()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("safe to destroy called before mark stopped", KR(ret), K_(tenant_id));
  } else {
    common::ObSpinLockGuard guard(lock_);
    // Every waiter is a registered controller, so ctrl_info_map_ empty
    // implies no thread is inside wait_for_free_slot.
    is_safe = (in_use_list_.is_empty() && 0 == ctrl_info_map_.size());
  }
  return ret;
}

void ObMigrationTenantWindowMgr::destroy()
{
  mark_stopped();
  const int64_t start_us = common::ObTimeUtility::current_time();
  int64_t last_warn_us = start_us;
  bool is_safe = false;
  while (!is_safe) {
    int ret = OB_SUCCESS;
    if (OB_FAIL(safe_to_destroy(is_safe))) {
      LOG_ERROR("fail to check safe to destroy during drain", KR(ret), K_(tenant_id));
      ob_usleep(DRAIN_SLICE_US);
    } else {
      broadcast_slot_release();
      const int64_t now_us = common::ObTimeUtility::current_time();
      if (now_us - last_warn_us >= DRAIN_WARN_US) {
        common::ObSpinLockGuard guard(lock_);
        LOG_ERROR_RET(OB_TIMEOUT, "tenant window mgr drain stuck, continue wait",
                      K_(tenant_id), "elapsed_us", now_us - start_us,
                      "in_use_size", in_use_list_.get_size(),
                      "registered_ctrl_count", ctrl_info_map_.size());
        last_warn_us = now_us;
      }
      ob_usleep(DRAIN_SLICE_US);
    }
  }

  {
    common::ObSpinLockGuard guard(lock_);
    cleanup_resources_();
  }
  ATOMIC_STORE(&is_inited_, false);

  LOG_INFO("tenant window mgr destroyed", K_(tenant_id));
}

bool ObMigrationTenantWindowMgr::is_inited() const
{
  return ATOMIC_LOAD(&is_inited_);
}

bool ObMigrationTenantWindowMgr::is_stopped() const
{
  return ATOMIC_LOAD(&stopped_);
}

void ObMigrationTenantWindowMgr::cleanup_resources_()
{
  while (!free_list_.is_empty()) {
    SlotMeta *node = free_list_.remove_first();
    delete_slot_meta_(node);
  }
  while (!in_use_list_.is_empty()) {
    SlotMeta *node = in_use_list_.remove_first();
    delete_slot_meta_(node);
  }
}

ObMigrationTenantWindowMgr::SlotMeta *ObMigrationTenantWindowMgr::new_slot_meta_()
{
  return OB_NEW(SlotMeta, ObMemAttr(tenant_id_, "MigVecIdxNode"));
}

void ObMigrationTenantWindowMgr::delete_slot_meta_(SlotMeta *&node)
{
  ob_delete(node);
}

ObMigrationTenantWindowMgr::SlotMeta *
ObMigrationTenantWindowMgr::find_in_use_by_buf_(const char *buf_ptr)
{
  SlotMeta *found = nullptr;
  if (OB_NOT_NULL(buf_ptr)) {
    DLIST_FOREACH_NORET(node, in_use_list_) {
      if (node->buf_ptr_ == buf_ptr) {
        found = node;
        break;
      }
    }
  }
  return found;
}

int ObMigrationTenantWindowMgr::ensure_slot_buffer_(SlotMeta *node)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_NOT_NULL(node->buf_ptr_)) {
  } else {
    const ObMemAttr buf_attr(tenant_id_, "MigVecIdxBufPl");
    char *buf = static_cast<char *>(ob_malloc(slot_buf_size_, buf_attr));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc slot buffer", KR(ret), K_(tenant_id), K_(slot_buf_size));
    } else {
      node->buf_ptr_ = buf;
    }
  }
  return ret;
}

int ObMigrationTenantWindowMgr::calc_dag_prio_concurrency_quota_(
    const ObMigrationControllerInfo &info,
    int64_t &quota) const
{
  int ret = OB_SUCCESS;
  const share::ObDagPrio::ObDagPrioEnum dag_prio = info.dag_prio_;
  quota = INT64_MAX;
  // Total concurrency budget (thread score) shared by all controllers of this dag priority.
  int64_t prio_concurrency_budget = share::OB_DAG_PRIOS[dag_prio].score_;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
  if (tenant_config.is_valid()) {
    int64_t config_score = 0;
    switch (dag_prio) {
    case share::ObDagPrio::DAG_PRIO_HA_HIGH:
      config_score = tenant_config->ha_high_thread_score;
      break;
    case share::ObDagPrio::DAG_PRIO_HA_MID:
      config_score = tenant_config->ha_mid_thread_score;
      break;
    default:
      break;
    }
    if (config_score > 0) {
      prio_concurrency_budget = config_score;
    }
  }

  int64_t same_prio_held_count = 0;
  int64_t same_prio_ctrl_count = 0;
  for (CtrlInfoMap::const_iterator it = ctrl_info_map_.begin();
       it != ctrl_info_map_.end(); ++it) {
    if (it->second.dag_prio_ == dag_prio) {
      same_prio_held_count += it->second.hold_count_;
      ++same_prio_ctrl_count;
    }
  }

  if (same_prio_ctrl_count <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no controller counted for current dag prio, ctrl_info_map inconsistent",
            KR(ret), K(dag_prio), K(same_prio_ctrl_count), K_(tenant_id));
  } else {
    const int64_t fair_share_per_ctrl =
        (prio_concurrency_budget + same_prio_ctrl_count - 1) / same_prio_ctrl_count;
    const int64_t ctrl_remaining_quota = fair_share_per_ctrl > info.hold_count_
                                         ? fair_share_per_ctrl - info.hold_count_ : 0;
    const int64_t prio_remaining_quota = prio_concurrency_budget > same_prio_held_count
                                         ? prio_concurrency_budget - same_prio_held_count : 0;
    quota = MIN(prio_remaining_quota, ctrl_remaining_quota);
  }
  return ret;
}

int ObMigrationTenantWindowMgr::apply_slots(
    const int64_t request_count,
    const int64_t ctrl_id,
    common::ObIArray<ObMigrationTenantWindowSlot> &out_slots,
    int64_t &granted_count)
{
  int ret = OB_SUCCESS;
  granted_count = 0;
  if (request_count <= 0 || common::OB_INVALID_INDEX_INT64 == ctrl_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args for apply_slots", KR(ret), K(request_count), K(ctrl_id), K_(tenant_id));
  } else {
    common::ObSpinLockGuard guard(lock_);
    if (!is_inited()) {
      ret = OB_NOT_INIT;
      LOG_WARN("tenant window mgr not inited", KR(ret), K_(tenant_id));
    } else if (is_stopped()) {
      ret = OB_NOT_RUNNING;
      LOG_WARN("tenant window mgr stopped", KR(ret), K_(tenant_id));
    } else {
      int64_t fair_share_remaining = 0;
      const int64_t ctrl_count = ctrl_info_map_.size();
      ObMigrationControllerInfo info;
      if (ctrl_count <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected empty registered ctrl map while applying slots", KR(ret), K(ctrl_count),
                K(ctrl_id), K_(tenant_id));
      } else {
        // Single lookup serves both as registration check (entry presence)
        // and current-hold fetch.
        const int hash_ret = ctrl_info_map_.get_refactored(ctrl_id, info);
        if (OB_HASH_NOT_EXIST == hash_ret) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("controller not registered, apply_slots contract violated",
                  KR(ret), K(ctrl_id), K(ctrl_count), K_(tenant_id));
        } else if (OB_SUCCESS != hash_ret) {
          ret = hash_ret;
          LOG_WARN("fail to get ctrl info map", KR(ret), K(ctrl_id), K_(tenant_id));
        } else {
          const int64_t fair_share = (max_slots_ + ctrl_count - 1) / ctrl_count;
          fair_share_remaining = fair_share > info.hold_count_ ? fair_share - info.hold_count_ : 0;
        }
      }

      // Only the DEST role is bound by the dag_prio concurrency quota
      const bool limited_by_concurrency = (ObMigrationControllerInfo::Role::DEST == info.role_);
      int64_t concurrency_quota = 0;
      if (OB_SUCC(ret) && limited_by_concurrency) {
        if (OB_FAIL(calc_dag_prio_concurrency_quota_(info, concurrency_quota))) {
          LOG_WARN("fail to calc dag_prio concurrency quota",
                  KR(ret), K_(tenant_id), K(ctrl_id));
        }
      }

      if (OB_SUCC(ret)) {
        const int64_t allocated_node_count = total_node_count_();
        // Slots the pool may still grow into: max_slots_ minus already allocated nodes.
        const int64_t growable_slot_count = max_slots_ > allocated_node_count
                                            ? max_slots_ - allocated_node_count : 0;
        // Slots grantable right now: idle nodes on the free list plus growable slots.
        const int64_t available_slot_count = free_list_.get_size() + growable_slot_count;

        // Start from the request, then clamp by each independent constraint.
        int64_t grant_count = request_count;
        grant_count = MIN(grant_count, available_slot_count); // limit by slot inventory
        grant_count = MIN(grant_count, fair_share_remaining); // limit by fair-share
        if (limited_by_concurrency) {
          grant_count = MIN(grant_count, concurrency_quota); // limit by dag_prio quota (DEST only)
        }
        if (grant_count <= 0) {
          ret = OB_EAGAIN;
          LOG_WARN("no slot available", KR(ret), K(request_count), K(available_slot_count), K(fair_share_remaining),
                  K(limited_by_concurrency), K(concurrency_quota), K_(tenant_id), K(ctrl_id));
        } else {
          out_slots.reuse();
          ObMigrationTenantWindowSlot slot;
          for (int64_t k = 0; OB_SUCC(ret) && k < grant_count; ++k) {
            SlotMeta *node = nullptr;
            if (!free_list_.is_empty()) {
              node = free_list_.remove_first();
            } else {
              node = new_slot_meta_();
              if (OB_ISNULL(node)) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                LOG_WARN("fail to alloc slot meta node", KR(ret), K_(tenant_id),
                        K_(max_slots), "total_nodes", total_node_count_());
              }
            }
            if (OB_SUCC(ret)) {
              if (OB_FAIL(ensure_slot_buffer_(node))) {
                LOG_WARN("fail to attach buffer to slot node", KR(ret), K_(tenant_id));
                free_list_.add_last(node);
              } else {
                node->ctrl_id_ = ctrl_id;
                in_use_list_.add_last(node);
                slot.reset();
                slot.buf_ptr_ = node->buf_ptr_;
                slot.buf_cap_ = slot_buf_size_;
                if (OB_FAIL(out_slots.push_back(slot))) {
                  LOG_WARN("fail to push back slot", KR(ret), K_(tenant_id));
                  in_use_list_.remove(node);
                  node->ctrl_id_ = common::OB_INVALID_INDEX_INT64;
                  free_list_.add_last(node);
                } else {
                  ++granted_count;
                }
              }
            }
          }
          // rollback all granted slots on mid-loop failure to guarantee atomicity
          if (OB_FAIL(ret) && granted_count > 0) {
            for (int64_t k = 0; k < granted_count; ++k) {
              SlotMeta *n = find_in_use_by_buf_(out_slots.at(k).buf_ptr_);
              if (OB_NOT_NULL(n)) {
                in_use_list_.remove(n);
                n->ctrl_id_ = common::OB_INVALID_INDEX_INT64;
                free_list_.add_last(n);
              } else {
                LOG_ERROR("rollback find slot meta failed in apply_slots, slot may leak in in_use_list",
                          KR(ret), K(k), K(granted_count), "buf_ptr", out_slots.at(k).buf_ptr_,
                          K_(tenant_id), K(ctrl_id));
              }
            }
            LOG_WARN("apply slots rolled back on mid-loop failure",
                    KR(ret), K(granted_count), K_(tenant_id), K(ctrl_id));
            granted_count = 0;
            out_slots.reuse();
          }
        }
      }
      if (OB_SUCC(ret) && granted_count > 0) {
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(adjust_ctrl_hold_count_(ctrl_id, granted_count))) {
          LOG_WARN_RET(tmp_ret, "fail to adjust ctrl hold count, fair share may be inaccurate",
                      K(ctrl_id), K(granted_count), K_(tenant_id));
        }
      }
    }
  }
  return ret;
}

int ObMigrationTenantWindowMgr::free_slots(
    const common::ObIArray<ObMigrationTenantWindowSlot> &slots,
    const int64_t ctrl_id)
{
  int ret = OB_SUCCESS;
  if (slots.count() <= 0 || common::OB_INVALID_INDEX_INT64 == ctrl_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args for free_slots", KR(ret), K(slots.count()),
            K(ctrl_id), K_(tenant_id));
  } else {
    // Best-effort: collect first error but keep freeing the rest.
    int first_err = OB_SUCCESS;
    int64_t freed_count = 0;
    bool need_broadcast = false;
    {
      common::ObSpinLockGuard guard(lock_);
      if (!is_inited()) {
        ret = OB_NOT_INIT;
        LOG_WARN("tenant window mgr not inited", KR(ret), K_(tenant_id));
      } else {
        for (int64_t i = 0; i < slots.count(); ++i) {
          SlotMeta *node = find_in_use_by_buf_(slots.at(i).buf_ptr_);
          if (OB_ISNULL(node)) {
            if (OB_SUCCESS == first_err) {
              first_err = OB_ERR_UNEXPECTED;
            }
            LOG_ERROR("slot not found in in_use_list, possible caller corruption or double free",
                      "buf_ptr", slots.at(i).buf_ptr_, K_(tenant_id), K(ctrl_id));
          } else {
            const int64_t prev_owner = node->ctrl_id_;
            if (prev_owner != ctrl_id) {
              if (OB_SUCCESS == first_err) {
                first_err = OB_ERR_UNEXPECTED;
              }
              LOG_ERROR("slot owned by different controller",
                      KP(node), K(prev_owner), K(ctrl_id), K_(tenant_id));
            } else {
              in_use_list_.remove(node);
              node->ctrl_id_ = common::OB_INVALID_INDEX_INT64;
              if (total_node_count_() >= max_slots_) {
                delete_slot_meta_(node);
              } else {
                free_list_.add_last(node);
              }
              ++freed_count;
              // use prev_owner (actual holder) for hold count adjustment, not the caller
              int tmp_ret = OB_SUCCESS;
              if (OB_TMP_FAIL(adjust_ctrl_hold_count_(prev_owner, -1))) {
                LOG_WARN_RET(tmp_ret,
                            "fail to adjust ctrl hold count, fair share may be inaccurate",
                            K(prev_owner), K_(tenant_id));
              }
            }
          }
        }
        if (freed_count > 0) {
          need_broadcast = true;
        }
      }
    }
    if (need_broadcast) {
      broadcast_slot_release();
    }
    if (OB_SUCC(ret) && OB_SUCCESS != first_err) {
      ret = first_err;
    }
  }
  return ret;
}

int ObMigrationTenantWindowMgr::get_free_slot_count(int64_t &count) const
{
  int ret = OB_SUCCESS;
  count = 0;
  common::ObSpinLockGuard guard(lock_);
  if (!is_inited()) {
    ret = OB_NOT_INIT;
  } else {
    const int64_t in_use = in_use_list_.get_size();
    count = max_slots_ > in_use ? max_slots_ - in_use : 0;
  }
  return ret;
}

int ObMigrationTenantWindowMgr::get_tenant_id(uint64_t &tenant_id) const
{
  int ret = OB_SUCCESS;
  tenant_id = OB_INVALID_TENANT_ID;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant window mgr not inited", KR(ret));
  } else {
    tenant_id = tenant_id_;
  }
  return ret;
}

int ObMigrationTenantWindowMgr::get_slot_buf_size(int64_t &slot_buf_size) const
{
  int ret = OB_SUCCESS;
  slot_buf_size = 0;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant window mgr not inited", KR(ret));
  } else {
    slot_buf_size = slot_buf_size_;
  }
  return ret;
}

int64_t ObMigrationTenantWindowMgr::get_internal_capacity_for_test() const
{
  common::ObSpinLockGuard guard(lock_);
  return capacity_;
}

int64_t ObMigrationTenantWindowMgr::get_node_count_for_test() const
{
  common::ObSpinLockGuard guard(lock_);
  return total_node_count_();
}

int ObMigrationTenantWindowMgr::get_total_slot_count(int64_t &count) const
{
  int ret = OB_SUCCESS;
  count = 0;
  common::ObSpinLockGuard guard(lock_);
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant window mgr not inited", KR(ret), K_(tenant_id));
  } else {
    count = max_slots_;
  }
  return ret;
}

int ObMigrationTenantWindowMgr::wait_for_free_slot(const uint64_t timeout_us)
{
  int ret = OB_SUCCESS;
  bool need_wait = false;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
  } else if (is_stopped()) {
    ret = OB_NOT_RUNNING;
  } else {
    common::ObSpinLockGuard guard(lock_);
    const int64_t in_use = in_use_list_.get_size();
    if (max_slots_ <= in_use) {
      need_wait = true;
    }
  }

  if (OB_SUCC(ret) && need_wait) {
    common::ObThreadCondGuard cond_guard(slot_release_cond_);
    if (OB_FAIL(cond_guard.get_ret())) {
      LOG_WARN("fail to lock slot release cond", KR(ret), K_(tenant_id));
    } else {
      slot_release_cond_.wait_us(timeout_us);
    }
  }
  return ret;
}

void ObMigrationTenantWindowMgr::broadcast_slot_release()
{
  // Caller contract guarantees no broadcaster is in flight when destroy()
  // tears the cond down; the atomic check is best-effort defense.
  if (is_inited()) {
    common::ObThreadCondGuard cond_guard(slot_release_cond_);
    if (OB_SUCCESS == cond_guard.get_ret()) {
      slot_release_cond_.broadcast();
    }
  }
}

int ObMigrationTenantWindowMgr::register_controller(
    const ObMigrationControllerInfo::Role role,
    const share::ObDagPrio::ObDagPrioEnum dag_prio,
    int64_t &ctrl_id)
{
  int ret = OB_SUCCESS;
  ctrl_id = common::OB_INVALID_INDEX_INT64;
  int64_t max_controllers = share::OB_DAG_PRIOS[share::ObDagPrio::DAG_PRIO_VECTOR_INDEX].score_;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
  if (tenant_config.is_valid()) {
    const int64_t score = tenant_config->ha_vector_index_thread_score;
    if (score > 0) {
      max_controllers = score;
    }
  }
  common::ObSpinLockGuard guard(lock_);
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant window mgr not inited", KR(ret), K_(tenant_id));
  } else if (is_stopped()) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("tenant window mgr stopped", KR(ret), K_(tenant_id));
  } else if (ObMigrationControllerInfo::Role::SOURCE == role) {
    // Cap check for source-side vector index serialization controllers.
    int64_t same_role_prio_count = 0;
    for (CtrlInfoMap::const_iterator it = ctrl_info_map_.begin();
         it != ctrl_info_map_.end(); ++it) {
      if (ObMigrationControllerInfo::Role::SOURCE == it->second.role_) {
        ++same_role_prio_count;
      }
    }
    if (same_role_prio_count >= max_controllers) {
      ret = OB_EAGAIN;
      LOG_WARN("vector index serialize controller count at limit, reject registration",
               KR(ret), K_(tenant_id), K(same_role_prio_count), K(max_controllers));
    }
  }
  if (OB_SUCC(ret)) {
    const int64_t new_id = next_ctrl_id_++;
    ObMigrationControllerInfo info(role, dag_prio);
    if (OB_FAIL(ctrl_info_map_.set_refactored(new_id, info, 0 /* no overwrite */))) {
      if (OB_HASH_EXIST == ret) {
        ret = OB_ENTRY_EXIST;
        LOG_WARN("controller already registered", KR(ret), K_(tenant_id), K(new_id), K(info),
                "registered_ctrl_count", ctrl_info_map_.size());
      } else {
        LOG_WARN("fail to insert into ctrl info map", KR(ret), K(new_id), K(info), K_(tenant_id));
      }
    } else {
      ctrl_id = new_id;
      LOG_INFO("A new controller registered", K_(tenant_id), K(ctrl_id), K(info),
              "registered_ctrl_count", ctrl_info_map_.size(), K_(max_slots));
    }
  }
  return ret;
}

int ObMigrationTenantWindowMgr::unregister_controller(const int64_t ctrl_id)
{
  int ret = OB_SUCCESS;
  bool need_broadcast = false;
  {
    common::ObSpinLockGuard guard(lock_);
    if (!is_inited()) {
      ret = OB_NOT_INIT;
      LOG_WARN("tenant window mgr not inited", KR(ret), K_(tenant_id));
    } else if (common::OB_INVALID_INDEX_INT64 == ctrl_id) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid controller id", KR(ret), K_(tenant_id));
    } else {
      const int erase_ret = ctrl_info_map_.erase_refactored(ctrl_id);
      if (OB_HASH_NOT_EXIST == erase_ret) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("controller not registered", KR(ret), K_(tenant_id), K(ctrl_id),
                "registered_ctrl_count", ctrl_info_map_.size());
      } else if (OB_SUCCESS != erase_ret) {
        ret = erase_ret;
        LOG_WARN("fail to erase ctrl info map", KR(ret), K_(tenant_id), K(ctrl_id));
      } else {
        DLIST_FOREACH_NORET(node, in_use_list_) {
          if (node->ctrl_id_ == ctrl_id) {
            LOG_ERROR("controller leaked slot on unregister", K_(tenant_id), K(ctrl_id), KP(node));
            OB_ASSERT(false);
          }
        }
        // Last controller unregistered: drop every cached idle node so an idle tenant pays
        // no buffer-pool memory when there are no migrations. Next migration grows the pool again from zero.
        if (0 == ctrl_info_map_.size()) {
          int64_t released = 0;
          while (!free_list_.is_empty()) {
            SlotMeta *node = free_list_.remove_first();
            delete_slot_meta_(node);
            ++released;
          }
          if (released > 0) {
            LOG_INFO("released cached slot nodes on last unregister",
                    K_(tenant_id), K(released));
          }
        }
        // Fair share grew; also nudges a destroy() polling safe_to_destroy.
        need_broadcast = true;
        LOG_INFO("controller unregistered", K_(tenant_id), K(ctrl_id),
                "registered_ctrl_count", ctrl_info_map_.size(),
                K_(max_slots));
      }
    }
  }
  if (need_broadcast) {
    broadcast_slot_release();
  }
  return ret;
}


int ObMigrationTenantWindowMgr::adjust_ctrl_hold_count_(
    const int64_t ctrl_id,
    const int64_t delta)
{
  int ret = OB_SUCCESS;
  if (common::OB_INVALID_INDEX_INT64 == ctrl_id || 0 == delta) {
  } else {
    ObMigrationControllerInfo info;
    if (OB_FAIL(ctrl_info_map_.get_refactored(ctrl_id, info))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("adjust hold count for unregistered controller",
                KR(ret), K(ctrl_id), K(delta), K_(tenant_id));
      } else {
        LOG_WARN("fail to get ctrl info map", KR(ret), K(ctrl_id), K_(tenant_id));
      }
    } else {
      const int64_t cur = info.hold_count_;
      int64_t new_val = cur + delta;
      if (new_val < 0) {
        LOG_ERROR("ctrl hold count would go negative, clamp to 0",
                  K(cur), K(delta), K(ctrl_id), K_(tenant_id));
        new_val = 0;
      }
      info.hold_count_ = new_val;
      if (OB_FAIL(ctrl_info_map_.set_refactored(
              ctrl_id, info, 1 /* overwrite */))) {
        LOG_WARN("fail to set ctrl info map", KR(ret),
                K(ctrl_id), K(new_val), K_(tenant_id));
      }
    }
  }
  return ret;
}


int ObMigrationTenantWindowMgr::shrink_(const int64_t target_alive)
{
  int ret = OB_SUCCESS;
  int64_t drained = 0;
  // Destroy idle nodes from free_list_ until alive count <= target_alive (frees buffer + node).
  // In-use surplus stays until controllers return slots; free_slots then drops the extras.
  while (total_node_count_() > target_alive && !free_list_.is_empty()) {
    SlotMeta *node = free_list_.remove_first();
    delete_slot_meta_(node);
    ++drained;
  }
  if (drained > 0) {
    LOG_INFO("tenant migration window mgr shrunk", K(target_alive), K(drained), K_(max_slots),
            K_(capacity), K_(tenant_id), "in_use", in_use_list_.get_size(),
            "free", free_list_.get_size());
  }
  return ret;
}

int ObMigrationTenantWindowMgr::resize(const int64_t new_max_slots, int64_t &actual_max_slots)
{
  int ret = OB_SUCCESS;
  actual_max_slots = 0;
  bool need_broadcast = false;
  {
    common::ObSpinLockGuard guard(lock_);
    if (!is_inited()) {
      ret = OB_NOT_INIT;
      LOG_WARN("tenant window mgr not inited", KR(ret), K_(tenant_id));
    } else if (is_stopped()) {
      ret = OB_NOT_RUNNING;
      LOG_WARN("tenant window mgr stopped", KR(ret), K_(tenant_id));
    } else if (new_max_slots <= 0 || new_max_slots > capacity_) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid new max slots", KR(ret), K(new_max_slots),
              K_(capacity), K_(tenant_id));
    } else {
      const int64_t old_max = max_slots_;
      max_slots_ = new_max_slots;
      if (new_max_slots < old_max) {
        if (OB_FAIL(shrink_(new_max_slots))) {
          LOG_WARN("shrink locked failed", KR(ret), K(new_max_slots), K(old_max), K_(tenant_id));
        }
      } else {
        need_broadcast = true;
      }
      actual_max_slots = max_slots_;
    }
  }
  if (need_broadcast) {
    broadcast_slot_release();
  }
  LOG_INFO("resize done", KR(ret), K(new_max_slots), K(actual_max_slots),
          K_(tenant_id), K_(max_slots), K_(capacity));
  return ret;
}

int ObMigrationTenantWindowMgr::init_from_tenant_config(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  if (!tenant_config.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get tenant config", KR(ret), K(tenant_id));
  } else {
    const int64_t mem_limit_byte = tenant_config->_migration_vector_index_window_buffer_size;
    const int64_t max_mem_limit = tenant_config->_migration_vector_index_window_buffer_size.get_max_value();
    const int64_t max_slots = calc_max_slots_by_memory(mem_limit_byte, MIGRATION_WINDOW_DEFAULT_SLOT_BUF_SIZE);
    const int64_t capacity = calc_max_slots_by_memory(max_mem_limit, MIGRATION_WINDOW_DEFAULT_SLOT_BUF_SIZE);
    if (max_slots <= 0 || capacity <= 0 || max_slots > capacity) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid max_slots / capacity, check config value",
              KR(ret), K(tenant_id), K(mem_limit_byte), K(max_mem_limit),
              K(max_slots), K(capacity));
    } else if (OB_FAIL(init(tenant_id, max_slots, MIGRATION_WINDOW_DEFAULT_SLOT_BUF_SIZE, capacity))) {
      LOG_WARN("failed to init tenant window mgr", KR(ret), K(tenant_id),
              K(max_slots), K(capacity));
    }
  }
  return ret;
}

int ObMigrationTenantWindowMgr::reload_config_from_tenant(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant window mgr not inited", KR(ret), K(tenant_id));
  } else {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
    if (!tenant_config.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get tenant config", KR(ret), K(tenant_id));
    } else {
      const int64_t mem_limit_byte = tenant_config->_migration_vector_index_window_buffer_size;
      const int64_t new_max_slots =
          calc_max_slots_by_memory(mem_limit_byte, MIGRATION_WINDOW_DEFAULT_SLOT_BUF_SIZE);
      if (new_max_slots <= 0) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("config invalid, skip resize", KR(ret), K(mem_limit_byte), K(tenant_id));
      } else {
        int64_t cur_total_slots = 0;
        int64_t actual_max_slots = 0;
        if (OB_FAIL(get_total_slot_count(cur_total_slots))) {
          LOG_WARN("failed to get total slot count", KR(ret), K(tenant_id));
        } else if (new_max_slots != cur_total_slots) {
          if (OB_FAIL(resize(new_max_slots, actual_max_slots))) {
            LOG_WARN("failed to resize tenant window mgr", KR(ret), K(new_max_slots), K(actual_max_slots));
          } else {
            LOG_INFO("tenant window mgr resized by config change", K(mem_limit_byte),
                    K(new_max_slots), K(actual_max_slots));
          }
        }
      }
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
