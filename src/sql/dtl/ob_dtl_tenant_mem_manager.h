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

#ifndef OB_DTL_TETANT_MEM_MANEGER_H
#define OB_DTL_TETANT_MEM_MANEGER_H

#include "lib/container/ob_se_array.h"
#include "sql/dtl/ob_dtl_channel_mem_manager.h"
#include "lib/hash_func/murmur_hash.h"

namespace oceanbase {
namespace sql {
namespace dtl {

// class ObDtlLinkedBuffer;

class ObDtlChannelMemManager;
class ObDtlTenantMemManager
{
public:
  ObDtlTenantMemManager(uint64_t tenant_id);
  virtual ~ObDtlTenantMemManager() { destroy(); }

  int init();
  void destroy();

  int auto_free_on_time();
public:
  ObDtlLinkedBuffer *alloc(int64_t chid, int64_t size);
  int free(ObDtlLinkedBuffer *buf);
  int64_t hash(int64_t chid);
  static int64_t hash(int64_t chid, int64_t ratio);

  int get_channel_mem_manager(int64_t idx, ObDtlChannelMemManager *&mgr);
  int64_t get_channel_mgr_count() { return mem_mgrs_.count(); }
  int64_t get_used_memory_size();
private:
  void buffer_status();

  int64_t get_min_buffer_size();

  int64_t variance_alloc_times();
  int64_t avg_alloc_times();

private:
  //通过128个散列处理并发
  static const int64_t HASH_CNT = 128;
  uint64_t tenant_id_;
  common::ObSEArray<ObDtlChannelMemManager*, HASH_CNT> mem_mgrs_;
private:
  common::ObSEArray<int64_t, HASH_CNT> times_;
  int64_t hash_cnt_;
};

OB_INLINE int64_t ObDtlTenantMemManager::hash(int64_t chid)
{
  uint64_t val = common::murmurhash(&chid, sizeof(chid), 0);
  return val % hash_cnt_;
}

OB_INLINE int64_t ObDtlTenantMemManager::hash(int64_t chid, int64_t ratio)
{
  uint64_t val = common::murmurhash(&chid, sizeof(chid), 0);
  return val % (HASH_CNT * ratio);
}

} // dtl
} // sql
} // oceanbase

#endif /* OB_DTL_TETANT_MEM_MANEGER_H */
