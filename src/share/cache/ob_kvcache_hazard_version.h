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

#include <pthread.h>
#include "share/ob_define.h"
#include "share/cache/ob_cache_utils.h"
#include "lib/lock/ob_mutex.h"
#include "lib/allocator/ob_concurrent_fifo_allocator.h"

namespace oceanbase {
namespace common {

class KVCacheHazardNode {
public:
  KVCacheHazardNode();
  virtual ~KVCacheHazardNode();
  virtual void retire() = 0;  // Release resource of current node
  void set_next(KVCacheHazardNode * const next);
  OB_INLINE KVCacheHazardNode* get_next() const { return hazard_next_; }
  OB_INLINE void set_version(const uint64_t version) { version_ = version; } 
  OB_INLINE uint64_t get_version() const { return version_; }
  VIRTUAL_TO_STRING_KV(K_(tenant_id), KP_(hazard_next), K_(version));
public:
  uint64_t tenant_id_;
private:
  KVCacheHazardNode *hazard_next_;
  uint64_t version_;  // version of node when delete
};


class KVCacheHazardThreadStore {
public:
  KVCacheHazardThreadStore();
  ~KVCacheHazardThreadStore();

  OB_INLINE bool is_inited() const { return ATOMIC_LOAD(&inited_); }
  int init(const int64_t thread_id);
  void set_exit();
  OB_INLINE int64_t get_thread_id() const { return thread_id_; }
  OB_INLINE int64_t get_waiting_count() const { return ATOMIC_LOAD(&waiting_nodes_count_); }
  OB_INLINE uint64_t get_last_retire_version() const { return ATOMIC_LOAD(&last_retire_version_); }
  OB_INLINE uint64_t get_acquired_version() const { return acquired_version_; }
  OB_INLINE void set_acquired_version(const uint64_t version) { acquired_version_ = version; }
  OB_INLINE KVCacheHazardThreadStore *get_next() const { return ATOMIC_LOAD(&next_); }
  OB_INLINE void set_next(KVCacheHazardThreadStore * const next) { ATOMIC_SET(&next_, next); }
  int delete_node(KVCacheHazardNode &node);  // Put node in delete_list and set its version

  // Local retire: Retire nodes in delete_list whose version is less than version and send rest nodes to node_receiver
  void retire(const uint64_t version, const uint64_t tenant_id);
  TO_STRING_KV(K_(thread_id), K_(inited), K_(last_retire_version), KP_(delete_list), K_(waiting_nodes_count), K_(acquired_version));

private:
  void add_nodes(KVCacheHazardNode &list);

private:
  uint64_t acquired_version_;
  KVCacheHazardNode * delete_list_;  // nodes waiting to retire
  int64_t waiting_nodes_count_;  // length of delete_list_
  uint64_t last_retire_version_;  // last version of retire()
  KVCacheHazardThreadStore *next_;
  int64_t thread_id_;  // thread id of relative thread
  bool is_retiring_;
  bool inited_;
};

class GlobalHazardVersion {
public:
  GlobalHazardVersion();
  virtual ~GlobalHazardVersion();
  int init(const int64_t thread_waiting_node_threshold);
  void destroy();
  int delete_node(KVCacheHazardNode *node);
  int acquire();  // Thread start to access shared memory, acquire version to protect this version
  void release();  // Thread finish access process, release protected version.
  int retire(const uint64_t tenant_id = OB_INVALID_TENANT_ID);  // Global retire, call retire() of every thread store
  int get_thread_store(KVCacheHazardThreadStore *&ts);
  int print_current_status() const;

private:
  static void deregister_thread(void *d_ts);
  int get_min_version(uint64_t &min_version) const;

private:
  static const int64_t MAX_PRINT_COUNT = 1000;

  uint64_t version_;  // current global version
  int64_t thread_waiting_node_threshold_;
  lib::ObMutex thread_store_lock_;
  KVCacheHazardThreadStore *thread_stores_;  // TODO: Implement clean function to clear unused thread store
  ObConcurrentFIFOAllocator thread_store_allocator_;
  pthread_key_t ts_key_;  // pthread_key for getting local KVCacheHazardThreadStore*
  bool inited_;
};

class GlobalHazardVersionGuard final
{
public:
  GlobalHazardVersionGuard(GlobalHazardVersion &g_version);
  ~GlobalHazardVersionGuard();
  OB_INLINE int get_ret() const { return ret_; } 
private:
  GlobalHazardVersion &global_hazard_version_;
  int ret_;
};


}  // end namespace common
}  // end namespace oceanbase

#endif
