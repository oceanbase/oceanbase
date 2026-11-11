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

#include "ob_kvcache_hazard_version.h"


namespace oceanbase{
namespace common{

/*
 * -----------------------------------------------------------ObKVCacheHazardNode-----------------------------------------------------------
 */
ObKVCacheHazardNode::ObKVCacheHazardNode()
    : tenant_id_(OB_INVALID_TENANT_ID),
      hazard_next_(nullptr),
      version_(UINT64_MAX)
{
}

ObKVCacheHazardNode::~ObKVCacheHazardNode()
{
}

void ObKVCacheHazardNode::set_next(ObKVCacheHazardNode * const next)
{
  if (this != next) {
    hazard_next_ = next;
  }
}

/*
 * -----------------------------------------------------------ObKVCacheHazardSlot-----------------------------------------------------------
 */
ObKVCacheHazardSlot::ObKVCacheHazardSlot()
    : acquired_version_(UINT64_MAX),
      delete_list_(nullptr),
      waiting_nodes_count_(0),
      last_retire_version_(0),
      is_retiring_(false)
{
}

ObKVCacheHazardSlot::~ObKVCacheHazardSlot()
{
}

bool ObKVCacheHazardSlot::acquire(const uint64_t version)
{
  return ATOMIC_BCAS(&acquired_version_, UINT64_MAX, version);
}

void ObKVCacheHazardSlot::release()
{
  ATOMIC_STORE(&acquired_version_, UINT64_MAX);
}

void ObKVCacheHazardSlot::delete_node(ObKVCacheHazardNode &node)
{
  add_nodes(node);
  ATOMIC_AAF(&waiting_nodes_count_, 1);
}

void ObKVCacheHazardSlot::retire(const uint64_t version, const uint64_t tenant_id)
{
  if (version > ATOMIC_LOAD(&last_retire_version_) || tenant_id != OB_INVALID_TENANT_ID) {
    while(!ATOMIC_BCAS(&is_retiring_, false, true)) {
      // wait until get retiring
      PAUSE();
    }

    ObKVCacheHazardNode *head = ATOMIC_LOAD(&delete_list_);
    if (nullptr != head) {
      if (version > last_retire_version_) {
        (void) ATOMIC_SET(&last_retire_version_, version);
      }
      ObKVCacheHazardNode *temp_node = head;
      while (temp_node != (head = ATOMIC_VCAS(&delete_list_, temp_node, nullptr))) {
        temp_node = head;
      }

      int64_t retire_count = 0;
      ObKVCacheHazardNode *remain_list = nullptr;
      while (head != nullptr) {
        temp_node = head;
        head = head->get_next();
        if (temp_node->get_version() < version || tenant_id == temp_node->tenant_id_) {
          temp_node->retire();
          temp_node = nullptr;
          ++retire_count;
        } else {
          temp_node->set_next(remain_list);
          remain_list = temp_node;
        }
      }
      if (remain_list != nullptr) {
        add_nodes(*remain_list);
      }
      if (retire_count > 0) {
        ATOMIC_SAF(&waiting_nodes_count_, retire_count);
      }
    }
    ATOMIC_SET(&is_retiring_, false);  // return retiring
  }
}

void ObKVCacheHazardSlot::add_nodes(ObKVCacheHazardNode &list)
{
  // Remember to udapte waiting_nodes_count_ outside

  ObKVCacheHazardNode *tail = &list;
  while (nullptr != tail->get_next()) {
    tail = tail->get_next();
  }

  ObKVCacheHazardNode *curr = ATOMIC_LOAD(&delete_list_);
  ObKVCacheHazardNode *old = curr;
  tail->set_next(curr);
  while (old != (curr = ATOMIC_VCAS(&delete_list_, old, &list))) {
    old = curr;
    tail->set_next(old);
  }
}

/*
 * -----------------------------------------------------------ObKVCacheHazardStation-----------------------------------------------------------
 */

ObKVCacheHazardStation::ObKVCacheHazardStation()
    : inited_(false),
      waiting_node_threshold_(0),
      hazard_slots_(nullptr),
      group_min_version_(nullptr),
      slot_allocator_("KVCACHE_HAZARD", OB_MALLOC_MIDDLE_BLOCK_SIZE, OB_SERVER_TENANT_ID),
      version_(0),
      atomic_scan_cursor_(0)
{
}

ObKVCacheHazardStation::~ObKVCacheHazardStation()
{
  destroy();
}

int ObKVCacheHazardStation::init(const int64_t waiting_node_threshold)
{
  int ret = OB_SUCCESS;

  void *buf = nullptr;
  void *group_min_buf = nullptr;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "This hazard station has been inited", K(ret), K(inited_));
  } else if (OB_UNLIKELY(waiting_node_threshold <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument", K(ret), K(waiting_node_threshold));
  } else if (OB_ISNULL(buf = slot_allocator_.alloc_align(sizeof(ObKVCacheHazardSlot) * SLOT_NUM, alignof(ObKVCacheHazardSlot)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    COMMON_LOG(WARN, "Fail to allocate memory for hazard slots", K(ret));
  } else if (OB_ISNULL(group_min_buf = slot_allocator_.alloc_align(sizeof(GroupMinVersion) * GROUP_NUM, alignof(GroupMinVersion)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    COMMON_LOG(WARN, "Fail to allocate memory for group min", K(ret));
  } else {
    hazard_slots_ = new (buf) ObKVCacheHazardSlot[SLOT_NUM];
    group_min_version_ = new (group_min_buf) GroupMinVersion[GROUP_NUM];
    version_ = 0;
    atomic_scan_cursor_ = 0;
    waiting_node_threshold_ = waiting_node_threshold;
    inited_ = true;
  }
  COMMON_LOG(DEBUG, "Hazard station init details", K(ret), K(waiting_node_threshold_));

  return ret;
}

void ObKVCacheHazardStation::destroy()
{
  COMMON_LOG(INFO, "Hazard station begin to destroy");

  inited_ = false;
  if (OB_NOT_NULL(hazard_slots_)) {
    for (int64_t i = 0 ; i < SLOT_NUM ; ++i) {
      hazard_slots_[i].~ObKVCacheHazardSlot();
    }
    slot_allocator_.free_align(hazard_slots_);
    hazard_slots_ = nullptr;
  }
  if (OB_NOT_NULL(group_min_version_)) {
    slot_allocator_.free_align(group_min_version_);
    group_min_version_ = nullptr;
  }
  waiting_node_threshold_ = 0;
  version_ = 0;
  atomic_scan_cursor_ = 0;
  slot_allocator_.reset();
}

int ObKVCacheHazardStation::delete_node(const int64_t slot_id, ObKVCacheHazardNode *node)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "This hazard station is not inited", K(ret), K(inited_));
  } else if (OB_UNLIKELY(slot_id < 0 || slot_id >= SLOT_NUM || nullptr == node)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument", K(ret), K(SLOT_NUM), K(slot_id), KP(node));
  } else {
    node->set_version(ATOMIC_FAA(&version_, 1));
    if (OB_UNLIKELY(nullptr != node->get_next())) {
      COMMON_LOG(ERROR, "Unexpected hazard next", KPC(node));
      ob_abort();
    } else {
      hazard_slots_[slot_id].delete_node(*node);
    }
  }

  return ret;
}

int ObKVCacheHazardStation::acquire(int64_t &slot_id)
{
  int ret = OB_SUCCESS;

  slot_id = -1;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "This hazard station is not inited", K(ret), K(inited_));
  } else {
    int64_t retry_count = 0;
    int64_t curr = get_itid() % SLOT_NUM;
    const int64_t end = curr;
    while (OB_SUCC(ret) && !hazard_slots_[curr].acquire(version_)) {
      sched_yield();
      curr = (curr + 1) % SLOT_NUM;
      if (curr == end) {
        if (++retry_count >= MAX_ACQUIRE_RETRY_COUNT) {
          COMMON_LOG_RET(WARN, common::OB_ENTRY_NOT_EXIST, "hazard version slot maybe not enough");
          retry_count = 0;
        }
      }
    }
    if (OB_SUCC(ret)) {
      slot_id = curr;
    }
  }

  return ret;
}

void ObKVCacheHazardStation::release(const int64_t slot_id)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!inited_)) {
    ret  = OB_NOT_INIT;
    COMMON_LOG(WARN, "This hazard station is not inited", K(ret));
  } else if (OB_UNLIKELY(slot_id < 0 || slot_id >= SLOT_NUM)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument", K(ret), K(slot_id));
  } else {
    ObKVCacheHazardSlot &slot = hazard_slots_[slot_id];
    slot.release();
    if (slot.get_waiting_count() >= waiting_node_threshold_) {
      const uint64_t advanced_group_idx = ATOMIC_FAA(&atomic_scan_cursor_, 1) % GROUP_NUM;
      advance_group_min_version(advanced_group_idx);
      const uint64_t last_retire_version = slot.get_last_retire_version();
      const uint64_t min_version = get_min_version_for_retire(last_retire_version);
      if (min_version > last_retire_version) {
        slot.retire(min_version, OB_INVALID_TENANT_ID);
      }
    }
  }

  if (OB_FAIL(ret)) {
    COMMON_LOG(ERROR, "Fail to release version", K(ret));
  }
}

int ObKVCacheHazardStation::retire(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "This hazard station is not inited", K(ret), K(inited_));
  } else {
    advance_all_group_min_version();
    uint64_t min_version = get_min_version();
    for (int64_t i = 0 ; i < SLOT_NUM ; ++i) {
      hazard_slots_[i].retire(min_version, tenant_id);
    }
  }

  if (tenant_id != OB_INVALID_TENANT_ID) {
    COMMON_LOG(INFO, "erase tenant hazard map node details", K(ret), K(tenant_id));
  }

  return ret;
}

int ObKVCacheHazardStation::print_current_status() const
{
  int ret = OB_SUCCESS;

  static const int64_t BUFLEN = 1 << 18;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "This hazard station is not inited", K(ret), K(inited_));
  } else {
    lib::ContextParam param;
    param.set_mem_attr(common::OB_SERVER_TENANT_ID, ObModIds::OB_TEMP_VARIABLES);
    CREATE_WITH_TEMP_CONTEXT(param) {
      int64_t ctxpos = 0;
      int64_t total_nodes_num = 0;
      char *buf = nullptr;
      if (OB_ISNULL(buf = (char *)lib::ctxalp(BUFLEN))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        COMMON_LOG(ERROR, "[KVCACHE-HAZARD] no memory", K(ret));
      } else {
        for (int64_t i = 0 ; i < SLOT_NUM ; ++i) {
          ObKVCacheHazardSlot &slot = hazard_slots_[i];
          int64_t waiting_nodes_count = slot.get_waiting_count();
          int64_t acquired_version = slot.get_acquired_version();
          total_nodes_num += waiting_nodes_count;
          if (waiting_nodes_count == 0 && acquired_version == UINT64_MAX) {
          } else if (OB_FAIL(ret = databuff_printf(buf, BUFLEN, ctxpos,
                  "[KVCACHE-HAZARD] i=%8ld | acquire_version=%12lu | waiting_nodes_count=%8ld | last_retire_version=%8lu |\n",
                  i, acquired_version, waiting_nodes_count, slot.get_last_retire_version()))) {
            COMMON_LOG(WARN, "Fail to write data buf", K(ret), K(ctxpos), K(BUFLEN));
          }
        }
        if (OB_SUCC(ret)) {
          _OB_LOG(INFO, "[KVCACHE-HAZARD-VERSION] hazard version status info: current_version: %8ld | min_version=%8ld | total_nodes_count: %8ld |\n%s",
              version_, get_min_version(), total_nodes_num, buf);
        }
      }
    }
  }

  return ret;
}

void ObKVCacheHazardStation::advance_group_min_version(const uint64_t group_idx)
{
  // Release Compile will optimize %/* with bit operation
  const uint64_t slot_start = group_idx * GROUP_SIZE;
  const uint64_t slot_end = slot_start + GROUP_SIZE;
  const uint64_t old_group_min_version = ATOMIC_LOAD_RLX(&group_min_version_[group_idx].min_version_);

  uint64_t min_version = ATOMIC_LOAD(&version_);
  for (uint64_t i = slot_start; i < slot_end; i++) {
    const uint64_t slot_version = hazard_slots_[i].get_acquired_version_rlx();
    min_version = MIN(min_version, slot_version);
    if (min_version <= old_group_min_version) {
      break;
    }
  }

  if (min_version > old_group_min_version) {
    // Use the value read before scanning. If another scanner has advanced it,
    // leave its result in place and let a later scan make further progress.
    (void)ATOMIC_BCAS(&group_min_version_[group_idx].min_version_, old_group_min_version, min_version);
  }
}

void ObKVCacheHazardStation::advance_all_group_min_version()
{
  for (int64_t i = 0; i < GROUP_NUM; i++) {
    advance_group_min_version(i);
  }
}

uint64_t ObKVCacheHazardStation::get_min_version() const
{
  uint64_t min_version = ATOMIC_LOAD(&version_);

  for (int64_t i = 0; i < GROUP_NUM; i++) {
    uint64_t group_min_version = ATOMIC_LOAD_RLX(&group_min_version_[i].min_version_);
    if (group_min_version < min_version) {
      min_version = group_min_version;
    }
  }

  return min_version;
}

uint64_t ObKVCacheHazardStation::get_min_version_for_retire(const uint64_t last_retire_version) const
{
  uint64_t min_version = ATOMIC_LOAD(&version_);
  for (int64_t i = 0; i < GROUP_NUM; i++) {
    uint64_t group_min_version = ATOMIC_LOAD_RLX(&group_min_version_[i].min_version_);
    min_version = MIN(min_version, group_min_version);
    if (min_version <= last_retire_version) {
      break;
    }
  }

  return min_version;
}

/*
 * -----------------------------------------------------------ObKVCacheHazardGuard-----------------------------------------------------------
 */

ObKVCacheHazardGuard::ObKVCacheHazardGuard(ObKVCacheHazardStation &g_station)
    : global_hazard_station_(g_station),
      slot_id_(-1),
      ret_(OB_SUCCESS)
{
  if (OB_UNLIKELY( OB_SUCCESS != (ret_ = global_hazard_station_.acquire(slot_id_)) )) {
    COMMON_LOG_RET(WARN, ret_, "Fail to acquire hazard version", K(ret_));
  }
}

ObKVCacheHazardGuard::~ObKVCacheHazardGuard()
{
  if (slot_id_ != -1) {
    global_hazard_station_.release(slot_id_);
  }
}


}  // end namespace common
}  // end namespace oceanbase
