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

#ifndef SHARE_STORAGE_MULTI_DATA_SOURCE_MDS_TABLE_HIGH_PRIORITY_FLUSHER_H
#define SHARE_STORAGE_MULTI_DATA_SOURCE_MDS_TABLE_HIGH_PRIORITY_FLUSHER_H

#include "ob_tablet_id.h"
#include "share/scn.h"
#include "runtime_utility/common_define.h"
#include "deps/oblib/src/lib/allocator/page_arena.h"
#include "lib/ob_errno.h"
#include "lib/utility/ob_print_utils.h"
#include "meta_programming/ob_type_traits.h"
#include "lib/hash/ob_linear_hash_map.h"
#include "deps/oblib/src/lib/container/ob_array_iterator.h"
#include "mds_for_each_map_flush_operation.h"
#include <algorithm>
#include <exception>

namespace oceanbase
{
namespace storage
{
namespace mds
{

struct MdsFlusherModulePageAllocator : public ModulePageAllocator {
  // just forward to parent
  MdsFlusherModulePageAllocator(const lib::ObLabel &label = "MdsFlusherArray",
                                int64_t tenant_id = OB_SERVER_TENANT_ID,
                                int64_t ctx_id = 0)
  : ModulePageAllocator(ObMemAttr(tenant_id, label, ctx_id)) {}
  MdsFlusherModulePageAllocator(const lib::ObMemAttr &attr) : ModulePageAllocator("MdsFlusherArray") {}
  explicit MdsFlusherModulePageAllocator(ObIAllocator &allocator,
                                         const lib::ObLabel &label = "MdsFlusherArray")
  : ModulePageAllocator(allocator, label) {}
  virtual ~MdsFlusherModulePageAllocator() {}
  // just change ob_malloc to ob_tenant_malloc
  void *alloc(const int64_t size) { return ModulePageAllocator::alloc(size); }
  void *alloc(const int64_t size, const ObMemAttr &attr);
  void free(void *p) {
    (NULL == allocator_) ? share::mtl_free(p) : allocator_->free(p); p = NULL;
  }
};

struct FlushKey// 16 bytes
{
  FlushKey()
  : tablet_id_(),
  rec_scn_() {}
  FlushKey(common::ObTabletID tablet_id, share::SCN rec_scn)
  : tablet_id_(tablet_id), rec_scn_(rec_scn) {}
  bool operator<(const FlushKey &rhs) const { return rec_scn_ < rhs.rec_scn_; }
  bool operator==(const FlushKey &rhs) const { return rec_scn_ == rhs.rec_scn_; }
  bool is_valid() const { return tablet_id_.is_valid() && rec_scn_.is_valid(); }
  TO_STRING_KV(K_(tablet_id), K_(rec_scn));
  common::ObTabletID tablet_id_;
  share::SCN rec_scn_;
};

template <int64_t STACK_QUEUE_SIZE>
struct MdsTableHighPriorityFlusher {
  MdsTableHighPriorityFlusher() : size_(0) {}
  bool try_record_mds_table(FlushKey new_key, FlushKey &eliminated_key);
  void flush_by_order(MdsTableMap &map, share::SCN limit, share::SCN max_decided_scn);
  int64_t to_string(char *buf, const int64_t len) const;
  int64_t size_;
  FlushKey high_priority_mds_tables_[STACK_QUEUE_SIZE];
};

template <int64_t STACK_QUEUE_SIZE, bool ORDER_ALL>
struct MdsTableOrderFlusher;

// 1. if memory is enough, flush mds table by strict ASC rec_scn order.
// 2. if memory is not enough, flush at least STACK_QUEUE_SIZE smallest mds table by strict ASC rec_scn order.
template <int64_t STACK_QUEUE_SIZE>
struct MdsTableOrderFlusher<STACK_QUEUE_SIZE, true>
{
  MdsTableOrderFlusher()
  : array_err_(OB_SUCCESS), high_priority_flusher_(), extra_mds_tables_() {}
  void reserve_memory(int64_t mds_table_total_size_likely);
  void record_mds_table(FlushKey key);
  void flush_by_order(MdsTableMap &map, share::SCN limit, share::SCN max_decided_scn);
  int64_t count() const {  return high_priority_flusher_.size_ + extra_mds_tables_.count(); }
  bool incomplete() const { return array_err_ != OB_SUCCESS; }
  TO_STRING_KV(K_(array_err), K_(high_priority_flusher));
  int array_err_;
  MdsTableHighPriorityFlusher<STACK_QUEUE_SIZE> high_priority_flusher_;
  ObArray<FlushKey, MdsFlusherModulePageAllocator> extra_mds_tables_;
};

// just record stack number items, and won't failed
template <int64_t STACK_QUEUE_SIZE>
struct MdsTableOrderFlusher<STACK_QUEUE_SIZE, false>
{
  MdsTableOrderFlusher()
  : high_priority_flusher_() {}
  void record_mds_table(FlushKey key);
  void flush_by_order(MdsTableMap &map, share::SCN limit, share::SCN max_decided_scn);
  bool empty() const { return high_priority_flusher_.size_ == 0; }
  bool full() const { return high_priority_flusher_.size_ == STACK_QUEUE_SIZE; }
  FlushKey min_key() const { return high_priority_flusher_.high_priority_mds_tables_[0]; }
  FlushKey max_key() const { return high_priority_flusher_.high_priority_mds_tables_[high_priority_flusher_.size_ == 0 ?
                                                                                     0 :
                                                                                     high_priority_flusher_.size_ - 1]; }
  TO_STRING_KV(K_(high_priority_flusher));
  MdsTableHighPriorityFlusher<STACK_QUEUE_SIZE> high_priority_flusher_;
};

static constexpr int64_t FLUSH_FOR_ONE_SIZE = 1;
static constexpr int64_t FLUSH_FOR_SOME_SIZE = 32;
static constexpr int64_t FLUSH_FOR_ALL_SIZE = 128;
using FlusherForOne = MdsTableOrderFlusher<FLUSH_FOR_ONE_SIZE, false>;
using FlusherForSome = MdsTableOrderFlusher<FLUSH_FOR_SOME_SIZE, false>;
using FlusherForAll = MdsTableOrderFlusher<FLUSH_FOR_ALL_SIZE, true>;

}
}
}

#ifndef SHARE_STORAGE_MULTI_DATA_SOURCE_MDS_TABLE_HIGH_PRIORITY_FLUSHER_H_IPP
#define SHARE_STORAGE_MULTI_DATA_SOURCE_MDS_TABLE_HIGH_PRIORITY_FLUSHER_H_IPP
#include "mds_table_order_flusher.ipp"
#endif

#endif