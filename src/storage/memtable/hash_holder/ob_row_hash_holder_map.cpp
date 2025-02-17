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

#include "ob_row_hash_holder_map.h"
#include "lib/alloc/alloc_func.h"
#include "lib/list/ob_dlist.h"
#include "lib/literals/ob_literals.h"
#include "lib/lock/ob_small_spin_lock.h"
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "ob_clock_generator.h"
#include "share/deadlock/ob_deadlock_detector_mgr.h"
#include "storage/memtable/hash_holder/ob_row_hash_holder_allocator.h"
#include "storage/memtable/hash_holder/ob_row_hash_holder_info.h"
#include "storage/memtable/mvcc/ob_mvcc_row.h"

namespace oceanbase
{
namespace memtable
{

#ifdef UNITTEST_DEBUG
int64_t HashHolderAllocator::ALLOC_TIMES = 0;
int64_t HashHolderAllocator::FREE_TIMES = 0;
bool HashHolderAllocator::NO_MEMORY = false;
#endif

RowHolderBucketHead::~RowHolderBucketHead() {
  RowHolderList *iter = list_;
  while (OB_NOT_NULL(iter)) {
    RowHolderList *deleted_list = iter;
    iter = iter->next_list_;
    map_->destroy(deleted_list, deleted_list->hash_val_);
  }
  new (this) RowHolderBucketHead();
}

int64_t RowHolderBucketHead::to_string(char *buf, const int64_t buf_len) const {
  ObCStringHelper helper;
  int64_t pos = 0;
  if (OB_ISNULL(list_)) {
    common::databuff_printf(buf, buf_len, pos, "EMPTY");
  } else {
    common::databuff_printf(buf, buf_len, pos, "{stack_list:%s}", helper.convert(*list_));
    const RowHolderList *list = list_->next_list_;
    constexpr int64_t MAX_PRINT_ITEM = 3;
    int64_t iter_item_cnt = 1;// head list has been printed
    while (OB_NOT_NULL(list) && ++iter_item_cnt) {
      if (OB_LIKELY(iter_item_cnt <= MAX_PRINT_ITEM)) {
        common::databuff_printf(buf, buf_len, pos, "->{%lx:%s}", (unsigned long)list, helper.convert(*list));
      }
      list = list->next_list_;
    }
    if (iter_item_cnt > MAX_PRINT_ITEM) {
      common::databuff_printf(buf, buf_len, pos,
                              "->%ld has printed, but there are %ld more...",
                              MAX_PRINT_ITEM, iter_item_cnt - MAX_PRINT_ITEM);
    }
  }
  return pos;
}

int64_t RowHolderBucketHead::size() const {
  int64_t size = 0;
  ObByteLockGuard lg(lock_, ObWaitEventIds::DEADLOCK_MGR_BUCKET_LOCK);
  if (OB_NOT_NULL(list_)) {
    const RowHolderList *iter = list_;
    while (OB_NOT_NULL(iter)) {
      size += iter->size();
      iter = iter->next_list_;
    }
  }
  return size;
}

int RowHolderBucketHead::insert(const uint64_t hash,
                                const transaction::ObTransID tx_id,
                                const transaction::ObTxSEQ seq,
                                const share::SCN scn,
                                HashHolderGlocalMonitor &monitor) {
  #define PRINT_WRAPPER KR(ret), K(hash), K(tx_id), K(seq), KP(found_list), K(*this)
  int ret = OB_SUCCESS;
  RowHolderList *found_list = nullptr;
  RowHolderList **link_found_list_ptr = nullptr;// not used
  if (OB_FAIL(find_or_create_hash_list_(hash,
                                        true/*create_if_not_exist*/,
                                        found_list,
                                        link_found_list_ptr,
                                        monitor))) {
    DETECT_LOG(WARN, "fail to find or create list", PRINT_WRAPPER);
  } else if (OB_FAIL(found_list->insert(hash, tx_id, seq, scn))) {
    DETECT_LOG(WARN, "fail to append record to existed list", PRINT_WRAPPER);
    destroy_hash_list_if_not_valid_(hash, link_found_list_ptr, found_list, monitor);
  }
  return ret;
  #undef PRINT_WRAPPER
}

int RowHolderBucketHead::erase(const uint64_t hash,
                               const transaction::ObTransID tx_id,
                               const transaction::ObTxSEQ seq,
                               HashHolderGlocalMonitor &monitor) {
  #define PRINT_WRAPPER KR(ret), K(hash), K(tx_id), K(seq), KP(found_list), K(*this)
  int ret = OB_SUCCESS;
  RowHolderList *found_list = nullptr;
  RowHolderList **link_found_list_ptr = nullptr;// not used
  if (OB_FAIL(find_or_create_hash_list_(hash,
                                        false/*create_if_not_exist*/,
                                        found_list,
                                        link_found_list_ptr,
                                        monitor))) {
    DETECT_LOG(WARN, "fail to find list", PRINT_WRAPPER);
  } else if (OB_FAIL(found_list->erase(hash, tx_id, seq))) {
    DETECT_LOG(WARN, "fail to erase node in list", PRINT_WRAPPER);
  } else {
    destroy_hash_list_if_not_valid_(hash, link_found_list_ptr, found_list, monitor);
  }
  return ret;
  #undef PRINT_WRAPPER
}

int RowHolderBucketHead::get(const uint64_t hash, RowHolderInfo &holder_info, HashHolderGlocalMonitor &monitor) const {
  #define PRINT_WRAPPER KR(ret), K(hash), KP(found_list), K(*this)
  int ret = OB_SUCCESS;
  RowHolderList *found_list = nullptr;
  RowHolderList **link_found_list_ptr = nullptr;// not used
  if (OB_FAIL(const_cast<RowHolderBucketHead *>(this)->find_or_create_hash_list_(hash,
                                                                                 false/*create_if_not_exist*/,
                                                                                 found_list,
                                                                                 link_found_list_ptr,
                                                                                 monitor))) {
    DETECT_LOG(WARN, "fail to find list", PRINT_WRAPPER);
  } else {
    holder_info = found_list->list_tail_->holder_info_;
  }
  return ret;
  #undef PRINT_WRAPPER
}

int RowHolderBucketHead::find_or_create_hash_list_(const uint64_t hash,
                                                   const bool create_if_not_exist,
                                                   RowHolderList *&found_list,
                                                   RowHolderList **&link_found_list_ptr,
                                                   HashHolderGlocalMonitor &monitor) {
  #define PRINT_WRAPPER KR(ret), K(hash), K(create_if_not_exist), KP(iter), KP(found_list), K(*this)
  int ret = OB_SUCCESS;
  link_found_list_ptr = &list_;
  RowHolderList *iter = list_;
  found_list = nullptr;
  while (iter) {
    if (iter->hash_val_ == hash) {
      found_list = iter;
      break;
    } else {
      link_found_list_ptr = &(iter->next_list_);
      iter = iter->next_list_;
    }
  }
  if (!found_list) {// found existed list
    if (!create_if_not_exist) {
      ret = OB_ENTRY_NOT_EXIST;
      DETECT_LOG(WARN, "hash list not exist", PRINT_WRAPPER);
    } else if (OB_FAIL(map_->create(found_list, hash, map_))) {// non-empty list, alloc new heap object
      DETECT_LOG(WARN, "create list failed", PRINT_WRAPPER);
    } else {
      found_list->hash_val_ = hash;
      found_list->next_list_ = list_;
      list_ = found_list;
      link_found_list_ptr = &list_;
      DETECT_LOG(DEBUG, "create new list success", PRINT_WRAPPER);
    }
  }
  DETECT_LOG(DEBUG, "find list", KP(found_list), KR(ret), K(hash), K(create_if_not_exist));
  return ret;
  #undef PRINT_WRAPPER
}

void RowHolderBucketHead::destroy_hash_list_if_not_valid_(const uint64_t hash,
                                                          RowHolderList **link_deleted_list_ptr,
                                                          RowHolderList *deleted_list,
                                                          HashHolderGlocalMonitor &monitor) {
  if (deleted_list->is_empty()) {
    DETECT_LOG(DEBUG, "destroy empty list", K(*this));
    *link_deleted_list_ptr = deleted_list->next_list_;
    map_->destroy(deleted_list, hash);
  }
}

int RowHolderMapper::init(bool for_unit_test) {
  int ret = OB_SUCCESS;
  int64_t tenant_memory_limit = lib::get_tenant_memory_limit(MTL_ID());
  int64_t BUCKET_COST = MIN(tenant_memory_limit * 0.005, 3.9_GB);// 0.5%
  int64_t BUCKET_CNT_LIMIT = (BUCKET_COST / sizeof(RowHolderBucketHead));
  int64_t BUCKET_CNT = 1;
  while (BUCKET_CNT <= BUCKET_CNT_LIMIT) {
    BUCKET_CNT <<= 1;
  }
  BUCKET_CNT >>= 1;
  bucket_cnt_ = for_unit_test ? 1ULL << 16 : BUCKET_CNT;
  if (OB_ISNULL(bucket_head_ = (RowHolderBucketHead *)HashHolderAllocator::alloc(sizeof(RowHolderBucketHead) * bucket_cnt_, "HHBucket"))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    DETECT_LOG(WARN, "init bucket head failed", KR(ret));
  } else {
    for (int64_t idx = 0; idx < bucket_cnt_; ++idx) {
      new (&bucket_head_[idx]) RowHolderBucketHead(this);
    }
    if (OB_FAIL(factory_.init(for_unit_test))) {
      DETECT_LOG(WARN, "init factory failed", KR(ret));
    }
    if (OB_FAIL(ret)) {
      this->~RowHolderMapper();
    }
  }
  if (OB_SUCC(ret)) {
    DETECT_LOG(INFO, "RowHolderMapper init success", K(bucket_cnt_));
  }
  return ret;
}

void RowHolderBucketHead::clear()
{
  if (OB_NOT_NULL(list_)) {
    DETECT_LOG(TRACE, "clear not empty bucket", K(*this));
    RowHolderMapper *map = map_;
    this->~RowHolderBucketHead();
    new (this) RowHolderBucketHead(map);
  }
}

RowHolderMapper::~RowHolderMapper() {
  periodic_tasks();
  DETECT_LOG(INFO, "before RowHolderMapper destroyed", KP(this));
  if (OB_NOT_NULL(bucket_head_)) {
    for (int i = 0; i < bucket_cnt_; ++i) {
      bucket_head_[i].~RowHolderBucketHead();
    }
    HashHolderAllocator::free(bucket_head_);
  }
  periodic_tasks();
  DETECT_LOG(INFO, "after RowHolderMapper destroyed", KP(this));
  factory_.~HashHolderFactory();
  monitor_.~HashHolderGlocalMonitor();
  new (this) RowHolderMapper();
}

void RowHolderMapper::insert_or_replace_hash_holder(const uint64_t hash,
                                                    const transaction::ObTransID tx_id,
                                                    const transaction::ObTxSEQ seq,
                                                    const share::SCN scn) {
  int64_t bucket_mask = bucket_cnt_ - 1;
  RowHolderBucketHead &bucket = bucket_head_[hash & bucket_mask];
  bool empty_before = false;
  bool empty_after = false;
  {
    ObByteLockGuard lg(bucket.lock_, ObWaitEventIds::DEADLOCK_MGR_BUCKET_LOCK);
#ifdef __HASH_HOLDER_ENABLE_MONITOR__
    empty_before = bucket.is_empty();
#endif
    (void) bucket.insert(hash, tx_id, seq, scn, monitor_);
#ifdef __HASH_HOLDER_ENABLE_MONITOR__
    empty_after = bucket.is_empty();
#endif
  }
#ifdef __HASH_HOLDER_ENABLE_MONITOR__
  if (empty_before && !empty_after) {
    monitor_.inc_valid_bucket_cnt(hash);
  }
#endif
}

void RowHolderMapper::erase_hash_holder_record(const uint64_t hash,
                                               const transaction::ObTransID tx_id,
                                               const transaction::ObTxSEQ seq) {
  int64_t bucket_mask = bucket_cnt_ - 1;
  RowHolderBucketHead &bucket = bucket_head_[hash & bucket_mask];
  bool empty_before = false;
  bool empty_after = false;
  {
    ObByteLockGuard lg(bucket.lock_, ObWaitEventIds::DEADLOCK_MGR_BUCKET_LOCK);
#ifdef __HASH_HOLDER_ENABLE_MONITOR__
    empty_before = bucket.is_empty();
#endif
    (void) bucket_head_[hash & bucket_mask].erase(hash, tx_id, seq, monitor_);
#ifdef __HASH_HOLDER_ENABLE_MONITOR__
    empty_after = bucket.is_empty();
#endif
  }
#ifdef __HASH_HOLDER_ENABLE_MONITOR__
  if (!empty_before && empty_after) {
    monitor_.dec_valid_bucket_cnt(hash);
  }
#endif
}

int RowHolderMapper::get_hash_holder(uint64_t hash, RowHolderInfo &holder_info) const {
  int64_t bucket_mask = bucket_cnt_ - 1;
  RowHolderBucketHead &bucket = bucket_head_[hash & bucket_mask];
  ObByteLockGuard lg(bucket.lock_, ObWaitEventIds::DEADLOCK_MGR_BUCKET_LOCK);
  return bucket_head_[hash & bucket_mask].get(hash, holder_info, const_cast<HashHolderGlocalMonitor&>(monitor_));
}

template <typename T>
static void print_cache_statics_to_buffer(T &cache, char *buffer, int64_t &pos, const int64_t buffer_len) {
  ObCStringHelper helper;
  for (int64_t idx = 0; idx < cache.thread_size_; ++idx) {
    int64_t revert_cnt = ATOMIC_LOAD(&cache.thread_cache_[idx].revert_times_);
    int64_t fetch_cnt = ATOMIC_LOAD(&cache.thread_cache_[idx].fetch_times_);
    int64_t total_cnt = cache.thread_cache_[idx].pool_size_;
    int64_t free_cnt = total_cnt - (fetch_cnt - revert_cnt);
    ObSizeLiteralPrettyPrinter total_size = total_cnt * sizeof(typename T::cache_type);
    ObSizeLiteralPrettyPrinter free_size = free_cnt * sizeof(typename T::cache_type);
    databuff_printf(buffer, buffer_len, pos,
    "[DETECT.CACHE][T%ld][% '2ld] total_cnt=% '10ld history_fetch_cnt=% '16ld history_revert_cnt=% '16ld "
    "free_cnt=% '10ld, total_size=%s, free_size=%s\n",
    MTL_ID(), idx, total_cnt, fetch_cnt, revert_cnt,
    free_cnt, helper.convert(total_size), helper.convert(free_size));
  }
}

void RowHolderMapper::print_summary_info_to_buffer_(char *buffer, int64_t &pos, const int64_t buffer_len) const {
  ObCStringHelper helper;
  ObSizeLiteralPrettyPrinter tenant_memory_limit = lib::get_tenant_memory_limit(MTL_ID());
  ObSizeLiteralPrettyPrinter bucket_head_alloc_size = sizeof(RowHolderBucketHead) * bucket_cnt_;
  ObSizeLiteralPrettyPrinter cache_node_alloc_size_for_one_thread =
  sizeof(RowHolderNode) * factory_.node_cache_.cache_size_each_thread_;
  ObSizeLiteralPrettyPrinter cache_list_alloc_size_for_one_thread =
  sizeof(RowHolderList) * factory_.list_cache_.cache_size_each_thread_;
  int64_t thread_number = factory_.node_cache_.thread_size_;
  ObSizeLiteralPrettyPrinter total_size = bucket_head_alloc_size +
                                          (cache_node_alloc_size_for_one_thread +
                                            cache_list_alloc_size_for_one_thread) * thread_number;
  int64_t dynamic_alloc_node_cnt = monitor_.get_history_dynamic_alloc_node_cnt();
  int64_t dynamic_alloc_list_cnt = monitor_.get_history_dynamic_alloc_list_cnt();
  int64_t dynamic_free_node_cnt = monitor_.get_history_dynamic_free_node_cnt();
  int64_t dynamic_free_list_cnt = monitor_.get_history_dynamic_free_list_cnt();
  int64_t valid_bucket_cnt = monitor_.get_valid_bucket_cnt();
  int64_t valid_list_cnt = monitor_.get_valid_list_cnt();
  int64_t valid_node_cnt = monitor_.get_valid_node_cnt();
  databuff_printf(buffer, buffer_len, pos,
  "[T%ld]tenant_memory_limit:%s, total_used:%s, bucket_used:%s, "
  "valid_bucket:%ld, valid_list:%ld, valid_node:%ld, "
  "history_node_alloc_cnt:%ld, history_node_free_cnt:%ld, history_list_alloc_cnt:%ld, history_list_free_cnt:%ld",
  MTL_ID(), helper.convert(tenant_memory_limit), helper.convert(total_size), helper.convert(bucket_head_alloc_size),
  valid_bucket_cnt, valid_list_cnt, valid_node_cnt,
  dynamic_alloc_node_cnt, dynamic_free_node_cnt, dynamic_alloc_list_cnt, dynamic_free_list_cnt);
}

void RowHolderMapper::periodic_tasks() {
  if (!share::detector::ObDeadLockDetectorMgr::is_deadlock_enabled()) {
    int64_t count = this->count();
    if (count != 0) {
      clear();
      DETECT_LOG(INFO, "clear RowHolderMapper cause deadlock is disabled", K(count));
    }
  }
#ifdef __HASH_HOLDER_ENABLE_MONITOR__
  static constexpr int64_t BUFFER_SIZE = 3_KB;

  char buffer[BUFFER_SIZE] = {0};
  int64_t pos = 0;
  databuff_printf(buffer, BUFFER_SIZE, pos, "dump RowHolderMapper nodes info\n");
  print_cache_statics_to_buffer(HashHolderFactory::CacheTypeRouter<RowHolderNode>::get_cache(factory_), buffer, pos, BUFFER_SIZE);
  buffer[BUFFER_SIZE - 1] = '\0';
  OB_MOD_LOG_RET("[DETECT.CACHE.NODE]", INFO, OB_SUCCESS, buffer);

  pos = 0;
  databuff_printf(buffer, BUFFER_SIZE, pos, "dump RowHolderMapper lists info\n");
  print_cache_statics_to_buffer(HashHolderFactory::CacheTypeRouter<RowHolderList>::get_cache(factory_), buffer, pos, BUFFER_SIZE);
  buffer[BUFFER_SIZE - 1] = '\0';
  OB_MOD_LOG_RET("[DETECT.CACHE.LIST]", INFO, OB_SUCCESS, buffer);

  pos = 0;
  databuff_printf(buffer, BUFFER_SIZE, pos, "dump RowHolderMapper nodes info:");
  print_summary_info_to_buffer_(buffer, pos, BUFFER_SIZE);
  buffer[BUFFER_SIZE - 1] = '\0';
  OB_MOD_LOG_RET("[DETECT.CACHE.SUMMARY]", INFO, OB_SUCCESS, buffer);
#endif
}

void RowHolderMapper::clear()
{
  for (int64_t idx = 0; idx < bucket_cnt_; ++idx) {
    ObByteLockGuard lg(bucket_head_[idx].lock_, ObWaitEventIds::DEADLOCK_MGR_BUCKET_LOCK);
    bucket_head_[idx].clear();
  }
}

int64_t RowHolderMapper::count() const
{
  int64_t total_cnt = 0;
#ifdef __HASH_HOLDER_ENABLE_MONITOR__
  total_cnt = monitor_.get_valid_node_cnt();
#endif
  return total_cnt;
}

}
}
