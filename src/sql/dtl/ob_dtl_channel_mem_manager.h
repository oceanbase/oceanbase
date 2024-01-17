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

#ifndef OB_DTL_CHANNEL_MEM_MANEGER_H
#define OB_DTL_CHANNEL_MEM_MANEGER_H

#include "lib/queue/ob_lighty_queue.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/allocator/ob_fifo_allocator.h"
#include "sql/dtl/ob_dtl_linked_buffer.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/allocator/ob_mod_define.h"
#include "lib/alloc/alloc_func.h"
#include "share/config/ob_server_config.h"
#include "src/sql/dtl/ob_dtl_tenant_mem_manager.h"

namespace oceanbase {
namespace sql {
namespace dtl {

//class ObDtlLinkedBuffer;

class ObDtlTenantMemManager;
class ObDtlChannelMemManager
{
public:
  ObDtlChannelMemManager(uint64_t tenant_id, ObDtlTenantMemManager &tenant_mgr);
  virtual ~ObDtlChannelMemManager() { destroy(); }

  int init();
  void destroy();

public:
  ObDtlLinkedBuffer *alloc(int64_t chid, int64_t size);
  int free(ObDtlLinkedBuffer *buf, bool auto_free = true);

  void set_seqno(int64_t seqno) { seqno_ = seqno; }
  int64_t get_seqno() { return seqno_; }
  TO_STRING_KV(K_(size_per_buffer));

  OB_INLINE int64_t get_alloc_cnt() { return alloc_cnt_; }
  OB_INLINE int64_t get_free_cnt() { return free_cnt_; }
  OB_INLINE int64_t get_free_queue_length() { return free_queue_.size(); }

  OB_INLINE int64_t get_real_alloc_cnt() { return real_alloc_cnt_; }
  OB_INLINE int64_t get_real_free_cnt() { return real_free_cnt_; }

  OB_INLINE void increase_alloc_cnt() { ATOMIC_INC(&alloc_cnt_); }
  OB_INLINE void increase_free_cnt() { ATOMIC_INC(&free_cnt_); }


  int64_t get_total_memory_size() { return allocator_.used(); }

  int get_max_mem_percent();
  void update_max_memory_percent();
  int64_t get_buffer_size() { return size_per_buffer_; }
  int auto_free_on_time(int64_t cur_max_reserve_count);

  OB_INLINE int64_t queue_cnt() { return free_queue_.size(); }

private:
  bool out_of_memory();
  int64_t get_used_memory_size();
  int64_t get_max_dtl_memory_size();
  int64_t get_max_tenant_memory_limit_size();
  int get_memstore_limit_percentage_();
  void real_free(ObDtlLinkedBuffer *buf);
private:
  uint64_t tenant_id_;
  int64_t size_per_buffer_;
  int64_t seqno_;
  static const int64_t MAX_CAPACITY = 128;
  common::ObLightyQueue free_queue_;
  common::ObFIFOAllocator allocator_;

  int64_t pre_alloc_cnt_;
  double max_mem_percent_;
  int64_t memstore_limit_percent_;

  // some statistics
  int64_t alloc_cnt_;
  int64_t free_cnt_;

  int64_t real_alloc_cnt_;
  int64_t real_free_cnt_;
  ObDtlTenantMemManager &tenant_mgr_;
  int64_t mem_used_;
  int64_t last_update_memory_time_;
};

OB_INLINE int64_t ObDtlChannelMemManager::get_max_dtl_memory_size()
{
  if (0 == max_mem_percent_) {
    get_max_mem_percent();
  }
  return get_max_tenant_memory_limit_size() * max_mem_percent_ / 100;
}

OB_INLINE int64_t ObDtlChannelMemManager::get_max_tenant_memory_limit_size()
{
  int ret = OB_SUCCESS;
  if (0 == memstore_limit_percent_) {
    get_memstore_limit_percentage_();
  }
  int64_t percent_execpt_memstore = 100 - memstore_limit_percent_;
  return lib::get_tenant_memory_limit(tenant_id_) * percent_execpt_memstore / 100;
}

OB_INLINE bool ObDtlChannelMemManager::out_of_memory()
{
  bool oom = false;
  int64_t used = get_used_memory_size();
  int64_t max_dtl_memory_size = get_max_dtl_memory_size();
  if (used > max_dtl_memory_size) {
    oom = true;
  }
  return oom;
}

OB_INLINE void ObDtlChannelMemManager::update_max_memory_percent()
{
  get_memstore_limit_percentage_();
  get_max_mem_percent();
}

} // dtl
} // sql
} // oceanbase

#endif /* OB_DTL_CHANNEL_MEM_MANEGER_H */
