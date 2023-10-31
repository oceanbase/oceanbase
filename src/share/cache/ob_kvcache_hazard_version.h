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

#ifndef OCEANBASE_CACHE_OB_KVCACHE_HAZARD_VERSION_H_
#define OCEANBASE_CACHE_OB_KVCACHE_HAZARD_VERSION_H_

#include "share/cache/ob_cache_utils.h"

namespace oceanbase {
namespace common {

class ObKVCacheHazardNode {
public:
  ObKVCacheHazardNode();
  virtual ~ObKVCacheHazardNode();
  virtual void retire() = 0;  // Release resource of current node
  void set_next(ObKVCacheHazardNode * const next);
  OB_INLINE ObKVCacheHazardNode* get_next() const { return hazard_next_; }
  OB_INLINE void set_version(const uint64_t version) { version_ = version; }
  OB_INLINE uint64_t get_version() const { return version_; }
  VIRTUAL_TO_STRING_KV(K_(tenant_id), KP_(hazard_next), K_(version));
public:
  uint64_t tenant_id_;
private:
  ObKVCacheHazardNode *hazard_next_;
  uint64_t version_;  // version of node when delete
};


class ObKVCacheHazardSlot {
public:
  ObKVCacheHazardSlot();
  ~ObKVCacheHazardSlot();

  OB_INLINE int64_t get_waiting_count() const { return waiting_nodes_count_; }
  OB_INLINE uint64_t get_last_retire_version() const { return last_retire_version_; }
  OB_INLINE uint64_t get_acquired_version() const { return ATOMIC_LOAD(&acquired_version_); }
  OB_INLINE bool acquire(const uint64_t version);
  OB_INLINE void release();
  void delete_node(ObKVCacheHazardNode &node);

  // Local retire: Retire nodes in delete_list whose version is less than version
  void retire(const uint64_t version, const uint64_t tenant_id);
  TO_STRING_KV(K_(acquired_version), KP_(delete_list), K_(waiting_nodes_count), K_(last_retire_version));

private:
  void add_nodes(ObKVCacheHazardNode &list);

private:
  uint64_t acquired_version_;
  ObKVCacheHazardNode * delete_list_;  // nodes waiting to retire
  int64_t waiting_nodes_count_;  // length of delete_list_
  uint64_t last_retire_version_;  // last version of retire()
  bool is_retiring_;
};

class ObKVCacheHazardStation {
public:
  ObKVCacheHazardStation();
  virtual ~ObKVCacheHazardStation();
  int init(const int64_t thread_waiting_node_threshold, const int64_t sloc_num);
  void destroy();
  int delete_node(const int64_t slot_id, ObKVCacheHazardNode *node);
  int acquire(int64_t &slot_id);  // Thread start to access shared memory, acquire version to protect this version
  void release(const int64_t slot_id);  // Thread finish access process, release protected version.
  int retire(const uint64_t tenant_id = OB_INVALID_TENANT_ID);  // Global retire, call retire() of every slot
  int print_current_status() const ;

private:
  uint64_t get_min_version() const ;

private:
  static const int64_t MAX_ACQUIRE_RETRY_COUNT = 3;

  uint64_t version_;  // current global version
  int64_t waiting_node_threshold_;
  ObKVCacheHazardSlot *hazard_slots_;
  int64_t slot_num_;
  ObArenaAllocator slot_allocator_;
  bool inited_;
};

class ObKVCacheHazardGuard final
{
public:
  ObKVCacheHazardGuard(ObKVCacheHazardStation &g_version);
  ~ObKVCacheHazardGuard();
  OB_INLINE int64_t get_slot_id() const { return slot_id_; }
  OB_INLINE int get_ret() const { return ret_; }
private:
  ObKVCacheHazardStation &global_hazard_station_;
  int64_t slot_id_;
  int ret_;
};


}  // end namespace common
}  // end namespace oceanbase

#endif
