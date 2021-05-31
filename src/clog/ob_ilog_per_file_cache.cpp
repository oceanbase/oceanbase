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

#include "ob_ilog_per_file_cache.h"
#include "ob_raw_entry_iterator.h"
#include "ob_file_id_cache.h"
#include "ob_ilog_storage.h"

namespace oceanbase {
using namespace common;
namespace clog {
int ObIlogPerFileCache::init(const file_id_t file_id, const int64_t array_size, PageArena<>* pf_allocator)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
  } else if (OB_UNLIKELY(OB_INVALID_FILE_ID == file_id) || OB_UNLIKELY(array_size <= 0) ||
             OB_UNLIKELY(NULL == pf_allocator)) {
    ret = OB_INVALID_ARGUMENT;
    CSR_LOG(ERROR, "ObIlogPerFileCache init error", K(ret), K(file_id), K(array_size), KP(pf_allocator));
  } else {
    pf_allocator_ = pf_allocator;
    char* buf = reinterpret_cast<char*>(pf_allocator_->alloc(sizeof(ObLogCursorExt) * array_size));
    if (OB_UNLIKELY(NULL == buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      cursor_arr_ = reinterpret_cast<ObLogCursorExt*>(buf);
      file_id_ = file_id;
      arr_size_ = array_size;
      tail_pos_ = 0;
      is_inited_ = true;
    }
  }
  if (OB_SUCCESS != ret && OB_INIT_TWICE != ret) {
    destroy();
  }
  return ret;
}

void ObIlogPerFileCache::destroy()
{
  if (is_inited_) {
    is_inited_ = false;
    file_id_ = OB_INVALID_FILE_ID;
    pf_allocator_->free();
    pf_allocator_ = NULL;
    cursor_arr_ = NULL;
    arr_size_ = 0;
    tail_pos_ = 0;
  }
}

int ObIlogPerFileCache::append_cursor(const ObLogCursorExt& cursor)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!cursor.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    CSR_LOG(ERROR, "append cursor error, invalid cursor", K(ret), K(cursor));
  } else if (OB_UNLIKELY(tail_pos_ >= arr_size_)) {
    ret = OB_ERR_UNEXPECTED;
    // inited arr_size_ error
    CSR_LOG(ERROR, "append too many cursors", K(ret), K(tail_pos_), K(arr_size_));
  } else {
    cursor_arr_[tail_pos_] = cursor;
    tail_pos_++;
  }
  return ret;
}

int ObIlogPerFileCache::query_cursor(const ObPartitionKey& pkey, const uint64_t query_log_id, const uint64_t min_log_id,
    const uint64_t max_log_id, const offset_t start_offset_index, ObGetCursorResult& result)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!pkey.is_valid()) || OB_UNLIKELY(OB_INVALID_ID == query_log_id) ||
      OB_UNLIKELY(OB_INVALID_ID == min_log_id) || OB_UNLIKELY(OB_INVALID_ID == max_log_id) ||
      OB_UNLIKELY(min_log_id > query_log_id) || OB_UNLIKELY(max_log_id < query_log_id) ||
      OB_UNLIKELY(start_offset_index < 0) || OB_UNLIKELY(start_offset_index >= tail_pos_) ||
      OB_UNLIKELY(!result.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    CSR_LOG(ERROR,
        "invald argument",
        K(ret),
        K(pkey),
        K(query_log_id),
        K(min_log_id),
        K(max_log_id),
        K(start_offset_index),
        K(tail_pos_));
  } else {
    const int64_t target_offset_index = static_cast<int64_t>(start_offset_index + (query_log_id - min_log_id));
    const int64_t ret_len = std::min(static_cast<int64_t>(max_log_id - query_log_id + 1), result.arr_len_);
    const ObLogCursorExt* start_cursor = cursor_arr_ + target_offset_index;
    memcpy(result.csr_arr_, start_cursor, sizeof(ObLogCursorExt) * ret_len);
    result.ret_len_ = ret_len;
    CSR_LOG(DEBUG,
        "[ILOG_PER_FILE_CACHE] query cursor success",
        K_(file_id),
        K(pkey),
        K(query_log_id),
        K(min_log_id),
        K(max_log_id),
        K(target_offset_index),
        K(result));
  }
  return ret;
}

// Return value
// 1. OB_SUCCESS        SUCCESS target_log is the target log
// 2. other error code  FAILURE target_log is invalid
int ObIlogPerFileCache::locate_by_timestamp(const common::ObPartitionKey& pkey, const int64_t start_ts,
    const uint64_t min_log_id, const uint64_t max_log_id, const offset_t start_offset_index, uint64_t& target_log_id,
    int64_t& target_log_timestamp)
{
  int ret = OB_SUCCESS;
  if (!pkey.is_valid() || OB_INVALID_TIMESTAMP == start_ts || OB_INVALID_ID == min_log_id || min_log_id <= 0 ||
      OB_INVALID_ID == max_log_id || max_log_id <= 0 || min_log_id > max_log_id || start_offset_index < 0 ||
      start_offset_index >= tail_pos_) {
    ret = OB_INVALID_ARGUMENT;
    CSR_LOG(
        ERROR, "invalid argument", K(ret), K(pkey), K(start_ts), K(min_log_id), K(max_log_id), K(start_offset_index));
  } else {
    const ObLogCursorExt* pkey_csr_arr = cursor_arr_ + start_offset_index;
    const int64_t array_len = max_log_id - min_log_id + 1;
    const ObLogCursorExt* target_csr = NULL;

    // Binary search timestamp from array, the target_csr is the first log which log_ts
    // is greater than start_ts
    if (OB_FAIL(binary_search_timestamp_from_cursor_array_(pkey_csr_arr, array_len, start_ts, target_csr))) {
      CSR_LOG(WARN,
          "binary_search_timestamp_from_cursor_array_ fail",
          K(ret),
          K(pkey),
          K(start_ts),
          K(min_log_id),
          K(max_log_id),
          K(start_offset_index));
    } else if (OB_ISNULL(target_csr)) {
      ret = OB_ERR_UNEXPECTED;
      CSR_LOG(ERROR,
          "binary_search_timestamp_from_cursor_array_ fail, target cursor is NULL",
          K(ret),
          K(pkey),
          K(start_ts),
          K(min_log_id),
          K(max_log_id),
          K(start_offset_index));
    } else {
      target_log_id = min_log_id + target_csr - pkey_csr_arr;
      target_log_timestamp = target_csr->get_submit_timestamp();
    }

    CSR_LOG(INFO,
        "[ILOG_PER_FILE_CACHE] locate_by_timestamp finish",
        K(ret),
        K(pkey),
        K(start_ts),
        K(min_log_id),
        K(max_log_id),
        K(start_offset_index),
        K(target_log_id),
        K(target_log_timestamp));
  }
  return ret;
}

// Binary search a cursor which log_ts is greater than target_ts
int ObIlogPerFileCache::binary_search_timestamp_from_cursor_array_(const ObLogCursorExt* csr_arr,
    const int64_t array_len, const int64_t start_ts, const ObLogCursorExt*& target_cursor)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(csr_arr) || OB_UNLIKELY(array_len <= 0) || OB_UNLIKELY(OB_INVALID_TIMESTAMP == start_ts)) {
    ret = OB_INVALID_ARGUMENT;
    CSR_LOG(ERROR, "invalid argument", K(ret), K(csr_arr), K(array_len));
  } else {
    int64_t begin = 0;
    int64_t end = array_len - 1;

    while (begin <= end) {
      int64_t middle = ((begin + end) / 2);
      if (csr_arr[middle].get_submit_timestamp() >= start_ts) {
        end = middle - 1;
      } else {
        begin = middle + 1;
      }
    }

    if (end + 1 < array_len) {
      target_cursor = csr_arr + end + 1;
    } else {
      ret = OB_ERR_OUT_OF_UPPER_BOUND;
    }
  }
  return ret;
}

int ObIlogPerFileCacheBuilder::init(ObIlogStorage* ilog_storage, ObFileIdCache* file_id_cache)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == ilog_storage) || OB_UNLIKELY(NULL == file_id_cache)) {
    ret = OB_INVALID_ARGUMENT;
    CSR_LOG(ERROR, "init error", K(ret), KP(ilog_storage), KP(file_id_cache));
  } else {
    ilog_storage_ = ilog_storage;
    file_id_cache_ = file_id_cache;
  }
  return ret;
}

// NOTE: this operation is time-consumed
int ObIlogPerFileCacheBuilder::build_cache(
    const file_id_t file_id, ObIlogPerFileCache* pf_cache, PageArena<>& pf_page_arena, ObIlogStorageQueryCost& csr_cost)
{
  int ret = OB_SUCCESS;
  CSR_LOG(TRACE, "[ILOG_PER_FILE_CACHE] start build", K(file_id), KP(pf_cache));
  if (OB_ISNULL(ilog_storage_)) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(OB_INVALID_FILE_ID == file_id) || OB_UNLIKELY(NULL == pf_cache)) {
    ret = OB_INVALID_ARGUMENT;
    CSR_LOG(ERROR, "build cache error", K(ret), K(file_id), KP(pf_cache));
  } else {
    ObIRawIndexIterator* raw_ilog_iter = NULL;
    RawArray raw_array;                                                    // to buffer raw ilog entry from ilog file
    ObArenaAllocator raw_entry_allocator(ObModIds::OB_INDEX_ITERATOR_ID);  // tmp_allocator
    const offset_t start_offset = 0;
    if (OB_UNLIKELY(
            NULL == (raw_ilog_iter = ilog_storage_->alloc_raw_index_iterator(file_id, file_id, start_offset)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      CSR_LOG(ERROR, "alloc raw ilog iter", K(ret), K(file_id), KP(ilog_storage_), KP(raw_ilog_iter));
    } else if (OB_FAIL(prepare_sorted_raw_array_(file_id, raw_ilog_iter, raw_entry_allocator, raw_array, csr_cost))) {
      CSR_LOG(WARN, "prepare sorted raw_array error", K(ret), K(file_id), K(raw_array));
    } else if (raw_array.count_ > 0) {
      if (OB_FAIL(pf_cache->init(file_id, raw_array.count_, &pf_page_arena))) {
        CSR_LOG(WARN, "init pf_cache error", K(ret), K(file_id), K(raw_array), KP(pf_cache));
      } else if (OB_FAIL(build_pf_cache_(file_id, raw_array, pf_cache))) {
        CSR_LOG(WARN, "build pf_cache error", K(ret), K(file_id), KP(pf_cache));
      } else {
        CSR_LOG(INFO, "[ILOG_PER_FILE_CACHE] build_cache success", K(ret), K(file_id), "entry_count", raw_array.count_);
      }
    } else {
      CSR_LOG(ERROR, "[ILOG_PER_FILE_CACHE] build_cache ilog raw_array count is 0", K(file_id), K(raw_array));
    }
    if (OB_LIKELY(NULL != raw_ilog_iter)) {
      ilog_storage_->revert_raw_index_iterator(raw_ilog_iter);
      raw_ilog_iter = NULL;
    }
    raw_entry_allocator.clear();
  }
  CSR_LOG(TRACE, "[ILOG_PER_FILE_CACHE] build cache finish", K(ret), K(file_id), KP(pf_cache));
  return ret;
}

// compare ret:
//   e1  < e2   =>   -1
//   e1  > e2   =>    1
//   e1 == e2   =>    0
int ilog_entry_comparator(const void* e1, const void* e2)
{
  int cmp_ret = 0;
  if (OB_ISNULL(e1) || OB_ISNULL(e2)) {
    int tmp_ret = common::OB_ERR_UNEXPECTED;
    CSR_LOG(ERROR, "ilog_entry_comparator error, null element", K(tmp_ret), KP(e1), KP(e2));
  } else {
    const ObIndexEntry* i1 = reinterpret_cast<const ObIndexEntry*>(e1);
    const ObIndexEntry* i2 = reinterpret_cast<const ObIndexEntry*>(e2);
    if (i1->get_partition_key() != i2->get_partition_key()) {
      if (i1->get_partition_key() < i2->get_partition_key()) {
        cmp_ret = -1;
      } else {
        cmp_ret = 1;
      }
    } else if (i1->get_log_id() < i2->get_log_id()) {
      cmp_ret = -1;
    } else if (i1->get_log_id() > i2->get_log_id()) {
      cmp_ret = 1;
    } else {
      cmp_ret = 0;
    }
  }
  return cmp_ret;
}

int ObIlogPerFileCacheBuilder::prepare_sorted_raw_array_(const file_id_t file_id, ObIRawIndexIterator* raw_ilog_iter,
    ObIAllocator& raw_entry_allocator, RawArray& raw_array, ObIlogStorageQueryCost& csr_cost)
{
  // alloc -> fill -> sort
  UNUSED(file_id);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(raw_ilog_iter)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    raw_array.arr_ =
        reinterpret_cast<ObIndexEntry*>(raw_entry_allocator.alloc(sizeof(ObIndexEntry) * ILOG_ENTRY_COUNT_PER_FILE));
    if (OB_UNLIKELY(NULL == raw_array.arr_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      CSR_LOG(ERROR, "alloc memory for raw_array error", K(ret), K(file_id));
    } else {
      ObIndexEntry ilog_entry;
      int64_t count = 0;
      ObReadParam param;
      int64_t persist_len = 0;
      while (OB_SUCC(ret) && OB_SUCC(raw_ilog_iter->next_entry(ilog_entry, param, persist_len)) &&
             file_id == param.file_id_) {
        ret = raw_array.arr_[count].shallow_copy(ilog_entry);
        count++;
      }
      // always update csr_cost no matter success or not
      csr_cost.read_ilog_disk_count_ += raw_ilog_iter->get_read_cost().read_disk_count_;
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
      if (OB_SUCC(ret)) {
        raw_array.count_ = count;
        CSR_LOG(TRACE, "raw_array size", K(count), K(file_id));
      } else {
        CSR_LOG(WARN, "iterate ilog file error", K(ret), K(file_id), K(raw_array));
      }
    }
    if (OB_SUCC(ret)) {
      qsort(raw_array.arr_, raw_array.count_, sizeof(ObIndexEntry), ilog_entry_comparator);
    }
  }
  CSR_LOG(TRACE, "prepare_sorted_raw_array finish", K(ret), K(file_id), K(raw_array), K(csr_cost));
  return ret;
}

ObLogCursorExt ObIlogPerFileCacheBuilder::trim_to_cursor_(const ObIndexEntry& entry)
{
  ObLogCursorExt ret_cursor;
  ret_cursor.reset(entry.get_file_id(),
      entry.get_offset(),
      entry.get_size(),
      entry.get_accum_checksum(),
      entry.get_submit_timestamp(),
      entry.is_batch_committed());
  return ret_cursor;
}

int ObIlogPerFileCacheBuilder::BackfillGenerator::init(const file_id_t file_id, ObFileIdCache* file_id_cache)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_FILE_ID == file_id) || OB_UNLIKELY(NULL == file_id_cache)) {
    ret = OB_INVALID_ARGUMENT;
    CSR_LOG(ERROR, "DamGenerator init error", K(ret), K(file_id), KP(file_id_cache));
  } else {
    file_id_ = file_id;
    file_id_cache_ = file_id_cache;
    cur_offset_ = OB_INVALID_OFFSET;

    cur_pkey_.reset();
    cur_min_log_id_ = OB_INVALID_ID;
    cur_max_log_id_ = OB_INVALID_ID;
    cur_start_offset_index_ = OB_INVALID_OFFSET;
    CSR_LOG(TRACE, "DamGenerator init success", K(file_id_), KP(file_id_cache_));
  }
  return ret;
}

void ObIlogPerFileCacheBuilder::BackfillGenerator::reset()
{
  file_id_ = OB_INVALID_FILE_ID;
  file_id_cache_ = NULL;
  cur_offset_ = OB_INVALID_OFFSET;

  cur_pkey_.reset();
  cur_min_log_id_ = OB_INVALID_ID;
  cur_max_log_id_ = OB_INVALID_ID;
  cur_start_offset_index_ = OB_INVALID_OFFSET;
}

int ObIlogPerFileCacheBuilder::BackfillGenerator::end_cur_pkey_()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(file_id_cache_)) {
    ret = OB_NOT_INIT;
    CSR_LOG(ERROR, "invalid file_id_cache", K(file_id_cache_));
  } else if (OB_FAIL(file_id_cache_->backfill(cur_pkey_, cur_min_log_id_, file_id_, cur_start_offset_index_))) {
    CSR_LOG(WARN,
        "backfill file id cache fail",
        K(ret),
        K(cur_pkey_),
        K(cur_min_log_id_),
        K(file_id_),
        K(cur_start_offset_index_));
  } else {
    CSR_LOG(TRACE,
        "[ILOG_PER_FILE_CACHE] backfill success",
        K(ret),
        K(cur_pkey_),
        K(cur_min_log_id_),
        K(file_id_),
        K(cur_start_offset_index_));
  }
  return ret;
}

int ObIlogPerFileCacheBuilder::BackfillGenerator::do_track_(
    const ObPartitionKey& pkey, const uint64_t log_id, const int32_t array_idx)
{
  int ret = OB_SUCCESS;

  // NOTE: travse sequentially
  if (OB_UNLIKELY(OB_INVALID_OFFSET == cur_offset_ && 0 != array_idx) || OB_UNLIKELY(array_idx != (cur_offset_ + 1))) {
    ret = OB_INVALID_ARGUMENT;
    CSR_LOG(ERROR, "array index is not expected", K(ret), K(array_idx), K(cur_offset_));
  } else if (OB_UNLIKELY(0 == array_idx)) {
    cur_pkey_ = pkey;
    cur_min_log_id_ = log_id;
    cur_max_log_id_ = log_id;
    cur_start_offset_index_ = array_idx;
  } else {
    const bool is_same_pkey = (cur_pkey_ == pkey);
    const bool is_next_log = (cur_max_log_id_ + 1 == log_id);

    if (is_same_pkey && is_next_log) {
      cur_max_log_id_ = log_id;
      // NOTE: logs in the same partition must be continous
    } else if (is_same_pkey && !is_next_log) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(ERROR,
          "is_same_pkey, but not is_next_log",
          K(ret),
          K(pkey),
          K(file_id_),
          K(cur_min_log_id_),
          K(cur_max_log_id_),
          K(log_id));
    } else if (OB_FAIL(end_cur_pkey_())) {
      CSR_LOG(WARN,
          "end_cur_pkey_ fail",
          K(ret),
          K(cur_pkey_),
          K(pkey),
          K(file_id_),
          K(cur_min_log_id_),
          K(cur_max_log_id_),
          K(log_id));
    } else {
      cur_pkey_ = pkey;
      cur_min_log_id_ = log_id;
      cur_max_log_id_ = log_id;
      cur_start_offset_index_ = array_idx;
    }
  }

  if (OB_SUCCESS == ret) {
    // update index
    cur_offset_ = array_idx;
  }
  return ret;
}

int ObIlogPerFileCacheBuilder::BackfillGenerator::track(const ObIndexEntry& ilog_entry, const int32_t array_idx)
{
  int ret = OB_SUCCESS;
  const ObPartitionKey& pkey = ilog_entry.get_partition_key();
  const uint64_t log_id = ilog_entry.get_log_id();
  if (OB_UNLIKELY(!pkey.is_valid()) || OB_UNLIKELY(OB_INVALID_ID == log_id) || OB_UNLIKELY(array_idx < 0)) {
    ret = OB_INVALID_ARGUMENT;
    CSR_LOG(WARN, "invalid ilog_entry", K(ret), K(pkey), K(log_id), K(ilog_entry), K(array_idx));
  } else if (OB_FAIL(do_track_(pkey, log_id, array_idx))) {
    CSR_LOG(WARN, "dam_generator do_track error", K(ret), K(pkey), K(log_id), K(array_idx));
  } else {
    // success, do nothing
  }
  return ret;
}

int ObIlogPerFileCacheBuilder::BackfillGenerator::close()
{
  int ret = OB_SUCCESS;

  if (cur_offset_ >= 0) {
    if (OB_FAIL(end_cur_pkey_())) {
      CSR_LOG(WARN,
          "BackfillGenerator end_cur_pkey_ fail",
          K(ret),
          K(file_id_),
          K(cur_offset_),
          K(cur_pkey_),
          K(cur_min_log_id_),
          K(cur_max_log_id_),
          K(cur_start_offset_index_));
    } else {
      CSR_LOG(TRACE,
          "DamGenerator close success",
          K(ret),
          K(file_id_),
          K(cur_offset_),
          K(cur_pkey_),
          K(cur_min_log_id_),
          K(cur_max_log_id_),
          K(cur_start_offset_index_));

      reset();
    }
  }

  return ret;
}

int ObIlogPerFileCacheBuilder::build_pf_cache_(
    const file_id_t file_id, const RawArray& raw_array, ObIlogPerFileCache* pf_cache)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(pf_cache)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(!raw_array.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    CSR_LOG(ERROR, "invalid raw_array", K(ret), K(raw_array));
  } else {
    BackfillGenerator backfill_generator;
    if (OB_FAIL(backfill_generator.init(file_id, file_id_cache_))) {
      CSR_LOG(WARN, "backfill_generator init error", K(ret), K(file_id), K(pf_cache));
    }

    for (int32_t idx = 0; OB_SUCC(ret) && (idx < raw_array.count_); idx++) {
      const ObIndexEntry& ilog_entry = raw_array.arr_[idx];
      if (OB_FAIL(backfill_generator.track(ilog_entry, idx))) {
        CSR_LOG(WARN, "backfill_generator track cur ilog_entry error", K(ret), K(ilog_entry), K(idx));
      } else if (OB_FAIL(pf_cache->append_cursor(trim_to_cursor_(ilog_entry)))) {
        CSR_LOG(WARN, "pf_cache append_cursor", K(ret), K(ilog_entry));
      }
    }  // end for

    // close the loop
    if (OB_SUCC(ret)) {
      if (OB_FAIL(backfill_generator.close())) {
        CSR_LOG(WARN, "backfill_generator close error", K(ret), K(file_id), KP(pf_cache));
      }
    }
  }
  CSR_LOG(TRACE, "[ILOG_PER_FILE_CACHE] build_pf_cache finish", K(ret), K(file_id), "entry_count", raw_array.count_);
  return ret;
}
}  // namespace clog
}  // namespace oceanbase
